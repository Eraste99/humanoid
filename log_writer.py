# -*- coding: utf-8 -*-
from __future__ import annotations
"""
LogWriter — writer SQLite avec rotation quotidienne + schéma étendu (tri-CEX).

Points clés:
- Colonnes supplémentaires trades: trade_mode, account_alias, sub_account_id, IDs, latences,
  scores, **route/buy_ex/sell_ex/leg_role** (tri-CEX).
- Tables additionnelles: opportunities (avec trade_mode/route_id), alerts, pair_history (+ latency_ms_avg),
  **latency** (pipeline scanner→engine→ack→fills).
- Idempotence via _ensure_columns (ALTER TABLE ADD COLUMN si manquants).
- Rotation quotidienne, WAL + busy_timeout pour concurrence, exports CSV filtrables.
- **Nouveaux utilitaires**: `wal_checkpoint(mode)` et `optimize()`; checkpoint avant backup/rotation.
"""

import sqlite3
import csv
import shutil
from datetime import datetime
from typing import Dict, Any, Iterable, Optional, List, Tuple
# logger_historique/LogWriter.py
import os, gzip, time, threading


import csv
import gzip
import logging
import os
import shutil
import sqlite3
import threading
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None

_REGION_TZ = {
        "EU": "Europe/Rome",
        "US": "America/New_York",
    }

def _derive_time_fields(ts_iso_or_ms, pod_region: str):
    # ts peut être epoch-ms (int/float) ou ISO (str) ; fallback: now UTC
    if isinstance(ts_iso_or_ms, (int, float)):
        dt_utc = datetime.fromtimestamp(float(ts_iso_or_ms)/1000.0, tz=timezone.utc)
    elif isinstance(ts_iso_or_ms, str):
        try:
            dt_utc = datetime.fromisoformat(ts_iso_or_ms.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            dt_utc = datetime.now(timezone.utc)
    else:
        dt_utc = datetime.now(timezone.utc)

    tz = _REGION_TZ.get((pod_region or "EU").upper(), "Europe/Rome")
    if ZoneInfo is not None:
        try:
            dt_loc = dt_utc.astimezone(ZoneInfo(tz))
        except Exception:
            dt_loc = dt_utc
    else:
        dt_loc = dt_utc

    return {
        "hour_utc": dt_utc.hour,
        "weekday_utc": dt_utc.weekday(),
        "hour_local": dt_loc.hour,
        "weekday_local": dt_loc.weekday(),
        "local_day": dt_loc.strftime("%Y-%m-%d"),
    }


logger = logging.getLogger("LogWriter")


class LogWriter:
    """
    - Rotation quotidienne par fichier: logs/trades_log_YYYY-MM-DD.db
    - Tables: trades, pair_history, opportunities, alerts, latency
    - Ajout de colonnes manquantes via ALTER TABLE (idempotent)
    - PRAGMA WAL + busy_timeout pour meilleure concurrence
    """

    # ---------------------- utils canon ----------------------
    @staticmethod
    def _norm_ex(x: Optional[str]) -> Optional[str]:
        if not x:
            return None
        s = str(x).strip().replace("-", "").replace("_", "").upper()
        if s.startswith("BINANCE"):
            return "BINANCE"
        if s.startswith("BYBIT"):
            return "BYBIT"
        if s.startswith("COINBASE") or s in {"CB", "COINBASEAT", "COINBASEADVANCEDTRADE"}:
            return "COINBASE"
        return s

    # ---------------------- lifecycle -----------------------
    def __init__(
        self,
        db_dir: str = "logs",
        *,
        rotate_bytes: int = 256 * 1024 * 1024,   # 256 MiB (borne taille)
        backup_count: int = 10,                  # nombre de rotations à conserver (compressées)
        compress_rotations: bool = True          # compresser les rotations
    ) -> None:
        self.db_dir = db_dir
        os.makedirs(self.db_dir, exist_ok=True)

        # seuils de rotation size/age (size traité par rotate_if_needed côté manager)
        self._rotate_bytes = int(max(1, rotate_bytes))
        self._backup_count = int(max(0, backup_count))
        self._compress_rot = bool(compress_rotations)

        self._current_date = datetime.utcnow().date()
        self.db_path = self._daily_db_path()
        self.log_count = 0
        self._lock = threading.Lock()

        # bootstrap initial
        with self._conn() as conn:
            self._bootstrap(conn)

        # Best effort maintenance au démarrage
        try:
            self.wal_checkpoint("TRUNCATE")
        except Exception:
            logger.exception("wal_checkpoint(TRUNCATE) at boot failed")
        try:
            # backup rapide de la DB neuve du jour (atomique)
            self.backup_database()
        except Exception:
            logger.exception("backup at boot failed")


    # ------------------------------------------------------------------
    # Rotation & maintenance
    # ------------------------------------------------------------------
    def purge_old_dbs(self, keep_days: int = 14, compress: bool = True) -> None:
        """
        Purge/compresse les anciennes DB (trades_log_*.db).
        Si compress=True, remplace .db par .db.gz.
        """
        cutoff = time.time() - int(keep_days) * 86400
        try:
            for name in os.listdir(self.db_dir):
                if not name.startswith("trades_log_") or not name.endswith(".db"):
                    continue
                p = os.path.join(self.db_dir, name)
                try:
                    if os.path.getmtime(p) < cutoff:
                        if compress:
                            gz = p + ".gz"
                            with open(p, "rb") as f_in, gzip.open(gz, "wb", compresslevel=5) as f_out:
                                shutil.copyfileobj(f_in, f_out)
                            os.remove(p)
                        else:
                            os.remove(p)
                except Exception:
                    logger.exception("purge_old_dbs failed for %s", p)
        except Exception:
            logger.exception("purge_old_dbs listing failed")


    def _daily_db_path(self) -> str:
        return os.path.join(self.db_dir, f"trades_log_{self._current_date.isoformat()}.db")


    def insert_trade_log(self, category: str, **kwargs) -> None:
        """
        Écrit une ligne dans trades. Filtrage strict des colonnes.
        Dérive les champs horaires et net_profit_sign si absents.
        """
        with self._lock:
            self._rotate_if_needed()
            with self._conn() as conn:
                cur = conn.cursor()
                cur.execute("PRAGMA table_info(trades);")
                cols = {row[1] for row in cur.fetchall()}

                # 1) Payload normalisé
                payload = {"category": category, **(kwargs or {})}

                # 2) Timestamp: si "timestamp" absent → ISO UTC maintenant
                if "timestamp" not in payload or not payload["timestamp"]:
                    from datetime import datetime, timezone
                    payload["timestamp"] = datetime.now(timezone.utc).isoformat()

                # 3) Dérivation horaires (UTC+locale) depuis ts_ms s’il existe, sinon ISO
                ts_ms = payload.get("ts_ms")
                pod_region = payload.get("pod_region") or payload.get("region") or "EU"
                tf = _derive_time_fields(ts_ms if ts_ms else payload.get("timestamp"), pod_region)
                for k, v in tf.items():
                    payload.setdefault(k, v)

                # 4) net_profit_sign si absent (on ne calcule PAS net_profit ici)
                if "net_profit_sign" not in payload or payload["net_profit_sign"] is None:
                    try:
                        np = payload.get("net_profit")
                        if isinstance(np, (int, float)) and np != 0:
                            payload["net_profit_sign"] = 1 if np > 0 else -1
                        else:
                            # fallback par bps si dispo
                            nbps = payload.get("net_bps") or payload.get("spread_net_final_bps")
                            if isinstance(nbps, (int, float)) and nbps != 0:
                                payload["net_profit_sign"] = 1 if nbps > 0 else -1
                    except Exception:
                        pass  # best-effort
                # 5) Filtrage colonnes connues
                payload = {k: v for k, v in payload.items() if k in cols}

                # 6) Insert
                keys = ",".join(payload.keys())
                qmarks = ",".join(["?"] * len(payload))
                cur.execute(f"INSERT INTO trades ({keys}) VALUES ({qmarks})", list(payload.values()))
                self.log_count += 1


    def _rotate_if_needed(self) -> None:
        """Rotation quand la date change (daily rollover)."""
        today = datetime.utcnow().date()
        if today == self._current_date:
            return

        # Checkpoint de l'ancienne DB avant de basculer de fichier
        try:
            self.wal_checkpoint("TRUNCATE")
        except Exception:
            logger.exception("wal_checkpoint before daily rotate failed")

        try:
            self.backup_database()
        except Exception:
            logger.exception("backup before daily rotate failed")

        self._current_date = today
        self.db_path = self._daily_db_path()

        # Re-crée le schéma sur la nouvelle DB du jour
        with self._conn() as conn:
            self._bootstrap(conn)

        try:
            self.optimize()
        except Exception:
            logger.exception("optimize after daily rotate failed")

    def rotate_now(self) -> None:
        """
        Force une rotation immédiate du fichier SQLite courant en
        le renommant avec un suffixe timestamp, puis recrée une DB du jour.
        """
        # Checkpoint WAL pour garantir la cohérence du fichier source
        try:
            self.wal_checkpoint("TRUNCATE")
        except Exception:
            logger.exception("wal_checkpoint before rotate_now failed")

        # Chemin courant -> nom horodaté
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        base = os.path.basename(self.db_path)
        root, ext = os.path.splitext(base)
        rotated = os.path.join(self.db_dir, f"{root}.{ts}{ext}")

        try:
            shutil.move(self.db_path, rotated)
            if self._compress_rot and os.path.exists(rotated):
                with open(rotated, "rb") as src, gzip.open(rotated + ".gz", "wb", compresslevel=5) as dst:
                    shutil.copyfileobj(src, dst)
                os.remove(rotated)
        except FileNotFoundError:
            # rien à déplacer (DB n'existait pas encore)
            pass
        except Exception:
            logger.exception("rotate_now move/compress failed")

        # Re-crée le fichier DB du jour
        with self._conn() as conn:
            self._bootstrap(conn)

        # Purge des rotations excédentaires (fichiers compressés)
        try:
            cand = sorted([p for p in os.listdir(self.db_dir) if p.startswith(root + ".")])
            excess = len(cand) - self._backup_count
            for i in range(max(0, excess)):
                try:
                    os.remove(os.path.join(self.db_dir, cand[i]))
                except Exception:
                    logger.exception("rotation retention purge failed")
        except Exception:
            logger.exception("rotation retention listing failed")

    def rotate_if_needed(self, max_bytes: int, max_age_s: int) -> bool:
        """
        Rotation size/age. Retourne True si une rotation a été faite.
        - max_bytes: taille seuil du fichier (.db)
        - max_age_s: âge seuil (mtime) en secondes
        """
        try:
            st = os.stat(self.db_path)
        except FileNotFoundError:
            return False
        except Exception:
            logger.exception("stat(db_path) failed in rotate_if_needed")
            return False

        do_rotate = False
        if max_bytes and st.st_size >= int(max_bytes):
            do_rotate = True
        if max_age_s and (time.time() - float(st.st_mtime)) >= float(max_age_s):
            do_rotate = True

        if do_rotate:
            self.rotate_now()
            return True
        return False

    # ------------------------------------------------------------------
    # Connexion courte
    # ------------------------------------------------------------------
    def _conn(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path, timeout=20, check_same_thread=False)
        try:
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute("PRAGMA synchronous=NORMAL;")
            con.execute("PRAGMA temp_store=MEMORY;")
            con.execute("PRAGMA busy_timeout=15000;")
        except Exception:
            logger.exception("PRAGMA setup failed")
        return con

    # ------------------------------------------------------------------
    # Bootstrap schéma
    # ------------------------------------------------------------------
    def _bootstrap(self, conn: sqlite3.Connection) -> None:
        cur = conn.cursor()

        # ---------------- trades ----------------
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT,
                timestamp TEXT,
                exchange TEXT,
                asset TEXT,
                volume REAL,
                price REAL,
                slippage REAL,
                fees REAL,
                net_profit REAL,
                trade_type TEXT,
                side TEXT,
                strategy TEXT,
                source TEXT,

                -- Modes/branches
                trade_mode TEXT,                 -- "TT" | "TM" | "REB"
                account_alias TEXT,
                sub_account_id TEXT,

                -- Identifiants
                trade_id TEXT,
                bundle_id TEXT,
                slice_id TEXT,
                client_id TEXT,
                order_id TEXT,
                maker_order_id TEXT,
                taker_order_id TEXT,
                opp_id TEXT,
                route_id TEXT,
                rebal_group_id TEXT,

                -- Latences & temps bruts
                latency REAL,
                engine_latency_ms REAL,
                ack_latency_ms REAL,
                fill_latency_ms REAL,
                end_to_end_ms REAL,
                t_sent_ms INTEGER,
                t_ack_ms INTEGER,
                t_filled_ms INTEGER,
                t_done_ms INTEGER,

                -- Exécution enrichie
                executed_volume_usdc REAL,
                vwap_buy REAL,
                vwap_sell REAL,
                spread_net_final REAL,

                -- Routing tri-CEX
                route TEXT,
                buy_ex TEXT,
                sell_ex TEXT,
                leg_role TEXT,

                -- Scoring/rotation
                pair_score REAL,
                pair_rank INTEGER,

                raw_json TEXT
            );
            """
        )
        # Index utiles
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_asset ON trades(asset);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(timestamp);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_trade_id ON trades(trade_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_route_id ON trades(route_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_mode ON trades(trade_mode);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_account ON trades(account_alias);")
        # Index tri-CEX
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_route ON trades(route);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_buy_ex ON trades(buy_ex);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_sell_ex ON trades(sell_ex);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_leg_role ON trades(leg_role);")

        # ---------------- pair_history ----------------
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pair_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                pair TEXT,
                spread REAL,
                volume REAL,
                slippage_est REAL,
                slippage_real REAL,
                volatility REAL,
                trade_result TEXT,
                net_profitability REAL,
                opportunity_freq REAL,
                score REAL,
                latency_ms_avg REAL
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pair_history_pair ON pair_history(pair);")

        # ---------------- opportunities ----------------
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS opportunities (
              id TEXT PRIMARY KEY,
              ts REAL,
              pair_key TEXT,
              buy_ex TEXT,
              sell_ex TEXT,
              buy_price REAL,
              sell_price REAL,
              spread_brut REAL,
              spread_net REAL,
              volume_possible_usdc REAL,
              score REAL,
              typ TEXT,
              trade_mode TEXT,     -- "TT" | "TM" | "REB"
              route_id TEXT,
              raw_json TEXT
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_opp_pair ON opportunities(pair_key);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_opp_ts ON opportunities(ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_opp_mode ON opportunities(trade_mode);")
        # tri-CEX (pour requêtes buy/sell rapide)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_opp_buy_ex ON opportunities(buy_ex);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_opp_sell_ex ON opportunities(sell_ex);")

        # ---------------- alerts ----------------
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT,
              module TEXT,
              level TEXT,
              pair_key TEXT,
              message TEXT,
              ctx TEXT
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(ts);")

        # ---------------- latency (pipeline) ----------------
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS latency (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              trace_id TEXT,
              pair_key TEXT,
              route TEXT,
              buy_ex TEXT,
              sell_ex TEXT,
              t_scanner_ms INTEGER,
              t_engine_submit_ms INTEGER,
              t_engine_ack_ms INTEGER,
              t_first_fill_ms INTEGER,
              t_all_filled_ms INTEGER,
              status TEXT,
              raw_json TEXT
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_latency_route ON latency(route);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_latency_ts ON latency(t_scanner_ms);")

        conn.commit()
        # [PATCH-SCHEMA] Tags SPLIT / profil / pacer (idempotent)
        self._ensure_columns(conn, "trades", {
            "deployment_mode": "TEXT",
            "pod_region": "TEXT",
            "capital_profile": "TEXT",
            "pacer_mode": "TEXT",
        })

        self._ensure_columns(conn, "opportunities", {
            "deployment_mode": "TEXT",
            "pod_region": "TEXT",
            "capital_profile": "TEXT",
            "pacer_mode": "TEXT",
        })

        # -------- Assure colonnes additionnelles (idempotent) ----------
        self._ensure_columns(
            conn,
            "trades",
            {
                # modes / comptes
                "trade_mode": "TEXT",
                "account_alias": "TEXT",
                "sub_account_id": "TEXT",
                # ids
                "maker_order_id": "TEXT",
                "taker_order_id": "TEXT",
                "rebal_group_id": "TEXT",
                # latences
                "engine_latency_ms": "REAL",
                "ack_latency_ms": "REAL",
                "fill_latency_ms": "REAL",
                "end_to_end_ms": "REAL",
                "t_sent_ms": "INTEGER",
                "t_ack_ms": "INTEGER",
                "t_filled_ms": "INTEGER",
                "t_done_ms": "INTEGER",
                # scoring
                "pair_score": "REAL",
                "pair_rank": "INTEGER",
                # tri-CEX
                "route": "TEXT",
                "buy_ex": "TEXT",
                "sell_ex": "TEXT",
                "leg_role": "TEXT",
                # --- MM (si présents dans record) ---
                "p_both_est": "REAL",
                "inventory_delta_usd": "REAL",
                "reason": "TEXT",

            },
        )
        self._ensure_columns(conn, "pair_history", {"latency_ms_avg": "REAL"})
        self._ensure_columns(conn, "opportunities", {"trade_mode": "TEXT", "route_id": "TEXT"})
        # trades: colonnes dérivées P0
        self._ensure_columns(conn, "trades", {
            "net_profit_sign": "INTEGER",
            "hour_utc": "INTEGER",
            "weekday_utc": "INTEGER",
            "hour_local": "INTEGER",
            "weekday_local": "INTEGER",
            "local_day": "TEXT",
        })

        # opportunities: même dérivés pour analytiques par heure
        self._ensure_columns(conn, "opportunities", {
            "hour_utc": "INTEGER",
            "weekday_utc": "INTEGER",
            "hour_local": "INTEGER",
            "weekday_local": "INTEGER",
            "local_day": "TEXT",
            "deployment_mode": "TEXT",
            "pod_region": "TEXT",
            "capital_profile": "TEXT",
            "pacer_mode": "TEXT",
        })

        conn.commit()

    def _ensure_columns(self, conn: sqlite3.Connection, table: str, cols: Dict[str, str]) -> None:
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table});")
        existing = {row[1] for row in cur.fetchall()}
        for name, decl in cols.items():
            if name not in existing:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl};")

    # ------------------------------------------------------------------
    # Inserts — trades / pairs / opps / alerts / latency
    # ------------------------------------------------------------------

    def insert_trades_bulk(self, rows: Iterable[Dict[str, Any]]) -> int:
        rows = list(rows or [])
        if not rows:
            return 0
        with self._lock:
            self._rotate_if_needed()
            with self._conn() as conn:
                cur = conn.cursor()
                cur.execute("PRAGMA table_info(trades);")
                cols = {row[1] for row in cur.fetchall()}
                count = 0
                for payload in rows:
                    p = {k: v for k, v in {**payload}.items() if k in cols}
                    if "timestamp" not in p:
                        p["timestamp"] = datetime.utcnow().isoformat()
                    keys = ", ".join(p.keys())
                    qmarks = ", ".join(["?"] * len(p))
                    cur.execute(f"INSERT INTO trades ({keys}) VALUES ({qmarks})", list(p.values()))
                    count += 1
                conn.commit()
                self.log_count += count
                return count

    def write_pair_history(self, pair_data_list: Iterable[Dict[str, Any]]) -> None:
        """Insère plusieurs entrées dans pair_history (batch)."""
        with self._lock:
            self._rotate_if_needed()
            pair_data_list = list(pair_data_list or [])
            if not pair_data_list:
                return
            try:
                with self._conn() as conn:
                    cur = conn.cursor()
                    rows: List[Tuple] = []
                    for d in pair_data_list:
                        rows.append(
                            (
                                d.get("timestamp") or datetime.utcnow().isoformat(),
                                d.get("pair"),
                                d.get("spread"),
                                d.get("volume"),
                                d.get("slippage_est"),
                                d.get("slippage_real"),
                                d.get("volatility"),
                                d.get("trade_result"),
                                d.get("net_profitability"),
                                d.get("opportunity_freq"),
                                d.get("score"),
                                d.get("latency_ms_avg"),
                            )
                        )
                    cur.executemany(
                        """
                        INSERT INTO pair_history (
                            timestamp, pair, spread, volume, slippage_est, slippage_real,
                            volatility, trade_result, net_profitability, opportunity_freq, score, latency_ms_avg
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        rows,
                    )
                    conn.commit()
            except Exception as e:
                print(f"[LogWriter] Erreur écriture pair_history : {e}")

    def fetch_records(self, kind: str, day: str | None = None, limit: int = 1000) -> list[dict]:
        """
        Lecture rapide des enregistrements depuis la DB.
        kind: 'trades' | 'opportunities' | 'latency' | 'alerts' (ou une table existante)
        day: 'YYYY-MM-DD' (filtre sur local_day si dispo, sinon sur date(timestamp))
        limit: max lignes renvoyées (ORDER BY ts_ms DESC si dispo, sinon ROWID DESC)

        Retourne: list[dict]
        """
        import sqlite3, time
        kind = str(kind).strip().lower()
        rows_out: list[dict] = []
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()

            # existence & colonnes
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (kind,))
            if cur.fetchone() is None:
                return []

            cur.execute(f"PRAGMA table_info({kind});")
            cols = {r[1] for r in cur.fetchall()}
            # tri : ts_ms si dispo sinon ROWID
            order_by = "ts_ms DESC" if "ts_ms" in cols else "ROWID DESC"

            if day:
                # filtre jour local si la colonne existe
                if "local_day" in cols:
                    sql = f"SELECT * FROM {kind} WHERE local_day = ? ORDER BY {order_by} LIMIT ?"
                    cur.execute(sql, (day, int(limit)))
                else:
                    # fallback: cast sur timestamp si présent
                    date_expr = None
                    if "timestamp" in cols:
                        date_expr = "substr(timestamp, 1, 10)"
                    elif "ts_ms" in cols:
                        # convertir epoch-ms en date UTC (approx rapide via unixepoch/1000)
                        date_expr = "date(ts_ms/1000,'unixepoch')"
                    if date_expr:
                        sql = f"SELECT * FROM {kind} WHERE {date_expr} = ? ORDER BY {order_by} LIMIT ?"
                        cur.execute(sql, (day, int(limit)))
                    else:
                        sql = f"SELECT * FROM {kind} ORDER BY {order_by} LIMIT ?"
                        cur.execute(sql, (int(limit),))
            else:
                sql = f"SELECT * FROM {kind} ORDER BY {order_by} LIMIT ?"
                cur.execute(sql, (int(limit),))

            for r in cur.fetchall():
                rows_out.append({k: r[k] for k in r.keys()})
        return rows_out

    def compute_funnel(self, since_ms: int | None = None, until_ms: int | None = None) -> dict:
        """
        Petits agrégats "entonnoir" sur la fenêtre [since_ms, until_ms]:
          opportunities -> admitted -> submitted -> ack -> fill (win/loss)
        Calcul best-effort selon colonnes présentes; ne casse jamais.

        Retourne:
        {
          "window": {"since_ms":..., "until_ms":...},
          "opportunities": {"total": N, "admitted": N?},
          "trades": {"total": N, "wins": N, "losses": N},
          "latency": {"by_stage": {"submit_to_ack_ms_avg": x, ...}}
        }
        """
        import sqlite3, time
        now_ms = int(time.time() * 1000)
        if since_ms is None: since_ms = now_ms - 24 * 3600 * 1000
        if until_ms is None: until_ms = now_ms

        out = {
            "window": {"since_ms": since_ms, "until_ms": until_ms},
            "opportunities": {"total": 0, "admitted": None},
            "trades": {"total": 0, "wins": 0, "losses": 0},
            "latency": {"by_stage": {}},
        }

        def _has_cols(conn, table, names: set[str]) -> set[str]:
            cur = conn.cursor()
            cur.execute(f"PRAGMA table_info({table});")
            cols = {r[1] for r in cur.fetchall()}
            return (names & cols)

        with sqlite3.connect(self.db_path, timeout=10) as conn:
            cur = conn.cursor()

            # ---- opportunities
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='opportunities';")
            if cur.fetchone():
                cols = _has_cols(conn, "opportunities", {"ts_ms", "admit", "decision", "status"})
                # total
                if "ts_ms" in cols:
                    cur.execute("SELECT COUNT(1) FROM opportunities WHERE ts_ms BETWEEN ? AND ?;", (since_ms, until_ms))
                else:
                    cur.execute("SELECT COUNT(1) FROM opportunities;")
                out["opportunities"]["total"] = int(cur.fetchone()[0])

                # admitted ~ best-effort (admit=1 OR decision='admit' OR status='ADMITTED')
                admitted = None
                if "ts_ms" in cols:
                    if "admit" in cols:
                        cur.execute("SELECT COUNT(1) FROM opportunities WHERE ts_ms BETWEEN ? AND ? AND admit=1;",
                                    (since_ms, until_ms))
                        admitted = int(cur.fetchone()[0])
                    elif "decision" in cols:
                        cur.execute(
                            "SELECT COUNT(1) FROM opportunities WHERE ts_ms BETWEEN ? AND ? AND LOWER(decision)='admit';",
                            (since_ms, until_ms))
                        admitted = int(cur.fetchone()[0])
                    elif "status" in cols:
                        cur.execute(
                            "SELECT COUNT(1) FROM opportunities WHERE ts_ms BETWEEN ? AND ? AND UPPER(status)='ADMITTED';",
                            (since_ms, until_ms))
                        admitted = int(cur.fetchone()[0])
                out["opportunities"]["admitted"] = admitted

            # ---- trades (wins/losses via net_profit OU signe)
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades';")
            if cur.fetchone():
                tcols = _has_cols(conn, "trades", {"ts_ms", "net_profit", "net_profit_sign"})
                if "ts_ms" in tcols:
                    cur.execute("SELECT COUNT(1) FROM trades WHERE ts_ms BETWEEN ? AND ?;", (since_ms, until_ms))
                else:
                    cur.execute("SELECT COUNT(1) FROM trades;")
                out["trades"]["total"] = int(cur.fetchone()[0])

                wins = losses = 0
                if "ts_ms" in tcols:
                    if "net_profit" in tcols:
                        # wins
                        cur.execute("SELECT COUNT(1) FROM trades WHERE ts_ms BETWEEN ? AND ? AND net_profit > 0;",
                                    (since_ms, until_ms))
                        wins = int(cur.fetchone()[0])
                        # losses
                        cur.execute("SELECT COUNT(1) FROM trades WHERE ts_ms BETWEEN ? AND ? AND net_profit < 0;",
                                    (since_ms, until_ms))
                        losses = int(cur.fetchone()[0])
                    elif "net_profit_sign" in tcols:
                        cur.execute("SELECT COUNT(1) FROM trades WHERE ts_ms BETWEEN ? AND ? AND net_profit_sign=1;",
                                    (since_ms, until_ms))
                        wins = int(cur.fetchone()[0])
                        cur.execute("SELECT COUNT(1) FROM trades WHERE ts_ms BETWEEN ? AND ? AND net_profit_sign=-1;",
                                    (since_ms, until_ms))
                        losses = int(cur.fetchone()[0])
                out["trades"]["wins"], out["trades"]["losses"] = wins, losses

            # ---- latency (dérivée des timestamps si table présente)
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='latency';")
            if cur.fetchone():
                # colonnes timestamp attendues (créées au bootstrap L526–542)
                cols_needed = {"t_scanner_ms", "t_engine_submit_ms", "t_engine_ack_ms", "t_first_fill_ms",
                               "t_all_filled_ms"}
                lcols = _has_cols(conn, "latency", cols_needed)
                if cols_needed.issubset(lcols):
                    # Fenêtre
                    if since_ms is not None and until_ms is not None:
                        where = "WHERE t_scanner_ms BETWEEN ? AND ?"
                        args = (since_ms, until_ms)
                    else:
                        where, args = "", tuple()

                    def _avg(expr: str) -> float:
                        sql = f"SELECT AVG({expr}) FROM latency {where};"
                        cur.execute(sql, args)
                        v = cur.fetchone()[0]
                        try:
                            return float(v or 0.0)
                        except Exception:
                            return 0.0

                    out["latency"]["by_stage"]["scan→submit"] = _avg("t_engine_submit_ms - t_scanner_ms")
                    out["latency"]["by_stage"]["submit→ack"] = _avg("t_engine_ack_ms - t_engine_submit_ms")
                    out["latency"]["by_stage"]["ack→first_fill"] = _avg("t_first_fill_ms - t_engine_ack_ms")
                    out["latency"]["by_stage"]["first→all_filled"] = _avg("t_all_filled_ms - t_first_fill_ms")
                    out["latency"]["by_stage"]["e2e"] = _avg("t_all_filled_ms - t_scanner_ms")

        return out

    def insert_opportunity(self, **opp):
        """
        Insert robuste:
          - dérive les champs horaires (UTC + local selon pod_region)
          - filtre dynamiquement par les colonnes réellement présentes
          - INSERT idempotent
        """
        opp = dict(opp or {})
        ts_ms = opp.get("ts_ms") or int(float(opp.get("timestamp") or time.time()) * 1000)
        tf = _derive_time_fields(ts_ms, opp.get("pod_region") or "EU")
        opp.update(tf)

        with sqlite3.connect(self.db_path, timeout=10) as conn:
            cur = conn.cursor()
            cur.execute("PRAGMA table_info(opportunities);")
            cols = {row[1] for row in cur.fetchall()}

            data = {k: v for k, v in opp.items() if k in cols}
            if not data:
                return 0

            keys = ",".join(data.keys())
            q = ",".join(["?"] * len(data))
            sql = f"INSERT OR REPLACE INTO opportunities ({keys}) VALUES ({q})"
            cur.execute(sql, tuple(data.values()))
            conn.commit()
            return 1

    def insert_opportunities_bulk(self, opps: Iterable[Dict[str, Any]]) -> int:
        opps = list(opps or [])
        if not opps:
            return 0
        self._rotate_if_needed()
        with self._conn() as conn:
            cur = conn.cursor()
            rows = []
            for o in opps:
                rows.append(
                    (
                        o.get("id"),
                        float(o.get("timestamp", 0.0) or 0.0),
                        (o.get("pair") or o.get("pair_key")),
                        self._norm_ex(o.get("buy_exchange") or o.get("buy_ex")),
                        self._norm_ex(o.get("sell_exchange") or o.get("sell_ex")),
                        o.get("buy_price"),
                        o.get("sell_price"),
                        o.get("spread_brut"),
                        o.get("spread_net"),
                        o.get("volume_possible_usdc"),
                        o.get("score"),
                        o.get("type"),
                        o.get("trade_mode"),
                        o.get("route_id"),
                        o.get("deployment_mode"),
                        o.get("pod_region"),
                        o.get("capital_profile"),
                        o.get("pacer_mode"),
                        str(o),
                    )
                )

                cur.executemany(
                    """
                    INSERT OR REPLACE INTO opportunities(
                        id, ts, pair_key, buy_ex, sell_ex, buy_price, sell_price, spread_brut,
                        spread_net, volume_possible_usdc, score, typ, trade_mode, route_id,
                        deployment_mode, pod_region, capital_profile, pacer_mode, raw_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    rows,
                )

            conn.commit()
            return len(rows)

    def insert_alert(self, module: str, level: str, pair_key: str | None, message: str, ctx: str | None = None) -> None:
        with self._lock:
            self._rotate_if_needed()
            with self._conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO alerts(ts, module, level, pair_key, message, ctx)
                    VALUES (?,?,?,?,?,?)
                    """,
                    (datetime.utcnow().isoformat(), module, level, pair_key, message, ctx),
                )
                conn.commit()

    # ------------- latency events (tri-CEX pipeline) -------------------
    def insert_latency(self, metrics: Dict[str, Any]) -> None:
        """
        Écrit un évènement dans `latency`.
        Attendu:
          {
            "trace_id": str,
            "pair_key": "ETHUSDC",
            "route": "BUY:BINANCE→SELL:BYBIT",
            "buy_ex": "BINANCE",
            "sell_ex": "BYBIT",
            "t_scanner_ms":  ... (epoch ms),
            "t_engine_submit_ms": ...,
            "t_engine_ack_ms": ...,
            "t_first_fill_ms": ...,
            "t_all_filled_ms": ...,
            "status": "ok|timeout|rejected|..."
            "raw_json": "...",
          }
        """
        with self._lock:
            self._rotate_if_needed()
            with self._conn() as conn:
                cur = conn.cursor()
                buy_ex = self._norm_ex(metrics.get("buy_ex"))
                sell_ex = self._norm_ex(metrics.get("sell_ex"))
                cur.execute(
                    """
                    INSERT INTO latency(
                      trace_id, pair_key, route, buy_ex, sell_ex,
                      t_scanner_ms, t_engine_submit_ms, t_engine_ack_ms, t_first_fill_ms, t_all_filled_ms,
                      status, raw_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        metrics.get("trace_id"),
                        metrics.get("pair_key"),
                        metrics.get("route"),
                        buy_ex,
                        sell_ex,
                        metrics.get("t_scanner_ms"),
                        metrics.get("t_engine_submit_ms"),
                        metrics.get("t_engine_ack_ms"),
                        metrics.get("t_first_fill_ms"),
                        metrics.get("t_all_filled_ms"),
                        metrics.get("status"),
                        str(metrics.get("raw_json") if metrics.get("raw_json") is not None else metrics),
                    ),
                )
                conn.commit()

    def insert_latencies_bulk(self, events: Iterable[Dict[str, Any]]) -> int:
        events = list(events or [])
        with self._lock:
            if not events:
                return 0
            self._rotate_if_needed()
            with self._conn() as conn:
                cur = conn.cursor()
                rows = []
                for e in events:
                    rows.append(
                        (
                            e.get("trace_id"),
                            e.get("pair_key"),
                            e.get("route"),
                            self._norm_ex(e.get("buy_ex")),
                            self._norm_ex(e.get("sell_ex")),
                            e.get("t_scanner_ms"),
                            e.get("t_engine_submit_ms"),
                            e.get("t_engine_ack_ms"),
                            e.get("t_first_fill_ms"),
                            e.get("t_all_filled_ms"),
                            e.get("status"),
                            str(e.get("raw_json") if e.get("raw_json") is not None else e),
                        )
                    )
                cur.executemany(
                    """
                    INSERT INTO latency(
                      trace_id, pair_key, route, buy_ex, sell_ex,
                      t_scanner_ms, t_engine_submit_ms, t_engine_ack_ms, t_first_fill_ms, t_all_filled_ms,
                      status, raw_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    rows,
                )
                conn.commit()
                return len(rows)

    # ------------------------------------------------------------------
    # Exports & backup
    # ------------------------------------------------------------------
    def export_trades_to_csv(
        self,
        output_path: str = "logs/trades_export.csv",
        *,
        pair: Optional[str] = None,
        trade_mode: Optional[str] = None,
        start_iso: Optional[str] = None,
        end_iso: Optional[str] = None,
        include_header: bool = True,
    ) -> str:
        """
        Exporte la table "trades" en CSV avec filtres optionnels.
        - pair: ex "ETHUSDC" (sans tiret)
        - trade_mode: "TT"|"TM"|"REB"
        - start_iso / end_iso: limites temporelles (ISO8601)
        """
        where: List[str] = []
        params: List[Any] = []
        if pair:
            where.append("asset=?")
            params.append(pair.replace("-", "").upper())
        if trade_mode:
            where.append("trade_mode=?")
            params.append(trade_mode)
        if start_iso:
            where.append("timestamp>=?")
            params.append(start_iso)
        if end_iso:
            where.append("timestamp<=?")
            params.append(end_iso)
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""

        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM trades{where_sql} ORDER BY timestamp ASC", tuple(params))
            rows = cur.fetchall()
            headers = [d[0] for d in cur.description]
            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if include_header:
                    w.writerow(headers)
                w.writerows(rows)
        return output_path

    def backup_database(self, backup_path: Optional[str] = None) -> str:
        """
        Backup SQLite atomique via l'API `backup` (évite les copies partielles sous WAL).
        Retourne le chemin du backup.
        """
        if backup_path is None:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            backup_path = os.path.join(self.db_dir, f"trades_log_backup_{timestamp}.db")

        # s'assurer que la DB source existe
        if not os.path.exists(self.db_path):
            open(self.db_path, "a").close()

        with self._conn() as src:
            dest = sqlite3.connect(backup_path, timeout=20, check_same_thread=False)
            try:
                src.backup(dest)
            finally:
                dest.close()
        return backup_path

    def wal_checkpoint(self, mode: str = "PASSIVE") -> None:
        mode = (mode or "PASSIVE").upper()
        with self._conn() as conn:
            try:
                conn.execute(f"PRAGMA wal_checkpoint({mode});")
            except Exception:
                logger.exception("wal_checkpoint(%s) failed", mode)

    def optimize(self) -> None:
        try:
            with self._conn() as conn:
                conn.execute("PRAGMA optimize;")
        except Exception:
            logger.exception("PRAGMA optimize failed")

    # ------------------------------------------------------------------
    # Utilitaires lecture simples / statut
    # ------------------------------------------------------------------
    def get_db_path(self) -> str:
        return self.db_path

    def get_status(self) -> Dict[str, Any]:
        try:
            st = os.stat(self.db_path)
            size = int(st.st_size)
            mtime = int(st.st_mtime)
        except Exception:
            size, mtime = None, None
        return {
            "db_path": self.db_path,
            "log_count": self.log_count,
            "current_date": self._current_date.isoformat(),
            "db_file_bytes": size,
            "db_mtime": mtime,
        }
