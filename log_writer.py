# -*- coding: utf-8 -*-
from __future__ import annotations
"""
LogWriter — writer SQLite avec rotation quotidienne + schéma étendu (tri-CEX).

Rôle dans la chaîne LHM (M5-B2-1):
- La base SQLite gérée par LogWriter est la **source de vérité PnL / comptable**.
  → Toutes les vues PnL online, funnels et dashboards doivent s’appuyer sur cette DB.
- Les fichiers JSONL gérés par LoggerHistoriqueManager sont complémentaires (forensic/replay),
  mais ne doivent pas être utilisés comme base de calcul PnL “officielle”.

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
import json
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
    LogWriter — writer SQLite avec rotation quotidienne + schéma étendu (tri-CEX).

    Points clés:
- Tous les agrégats PnL "sérieux" (jour, rolling, par dimension) doivent être calculés
    à partir de cette base, via des requêtes SQL centralisées dans ce module.
    → Les autres composants (RiskManager, dashboards, outils de reco) doivent consommer
    ces vues agrégées plutôt que recalculer un PnL ad hoc sur les JSONL ou sur des
    streams en mémoire.
-   Les colonnes PnL et qualité PnL de la table `trades` (net_profit, fees,
    net_profit_sign, pnl_missing, pnl_derived_from_bps, etc.) constituent le contrat
    canonique pour la ligne PnL : toute logique de PnL doit s'appuyer sur ces champs.
    - Colonnes supplémentaires trades: trade_mode, account_alias, sub_account_id, IDs, latences,
      scores, **route/buy_ex/sell_ex/leg_role** (tri-CEX).
    - Tables additionnelles: opportunities (avec trade_mode/route_id), alerts, pair_history (+ latency_ms_avg),
      **latency** (pipeline scanner→engine→ack→fills).
    - Idempotence via _ensure_columns (ALTER TABLE ADD COLUMN si manquants).
    - Rotation quotidienne par date (un fichier trades_log_YYYY-MM-DD.db par db_dir / région),
      complétée par une rotation size/age via rotate_if_needed(max_bytes,max_age_s) (M5-B2-2).
    - Les rotations sont atomiques: wal_checkpoint(TRUNCATE) + move + (optionnel) compression gzip,
      ce qui garantit qu’aucune ligne validée n’est perdue lors d’un rollover.
    - **Nouveaux utilitaires**: `wal_checkpoint(mode)` et `optimize()`; checkpoint avant backup/rotation.

    Points clés:
- Colonnes supplémentaires trades: trade_mode, account_alias, sub_account_id, IDs, latences,
  scores, **route/buy_ex/sell_ex/leg_role** (tri-CEX).
- Tables additionnelles: opportunities (avec trade_mode/route_id), alerts, pair_history (+ latency_ms_avg),
  **latency** (pipeline scanner→engine→ack→fills).
- Tous les agrégats PnL "sérieux" (jour, rolling, par dimension) doivent être calculés
  à partir de cette base, via des requêtes SQL centralisées dans ce module. Les autres
  composants (RiskManager, dashboards, outils de reco) consomment les vues agrégées
  exposées ici, plutôt que de recalculer du PnL ad hoc.
- Vues PnL multi-dimensions (M5-C v2-B) — contrat cible `pnl_multiview`:
  {
    "component": "pnl_multiview",
    "base_currency": "USDC" | "EUR",
    "window": {"since_ms": int, "until_ms": int},
    "as_of_ts": float,
    "pnl_global": PnlRow,
    "pnl_by_exchange": [PnlRow, ...],
    "pnl_by_account": [PnlRow, ...],
    "pnl_by_branch": [PnlRow, ...],
    "quality_global": {
      "trades": int,
      "pnl_missing": int,
      "pnl_missing_share": float,
      "derived_from_bps": int,
      "derived_share": float,
    },
  }

  Où chaque `PnlRow` a la forme:
  {
    "dimensions": {...},   # ex: {}, {"exchange": "BINANCE"}, {"exchange": "BINANCE","account_alias": "EU_TT_01"}, {"branch": "TT","exchange": "BYBIT"}
    "pnl": {
      "net_profit": float,      # somme net_profit sur la fenêtre (base_currency)
      "fees": float,            # somme fees
      "turnover_quote": float,  # somme abs(notional_quote) si dispo, sinon 0.0
      "trades": int,
      "wins": int,
      "losses": int,
      "win_rate": float,        # wins / max(1, wins+losses)
    },
    "quality": {
      "pnl_missing": int,       # trades sans PnL exploitable
      "pnl_missing_share": float,   # pnl_missing / trades si trades>0, sinon 0.0
      "derived_from_bps": int,      # trades avec pnl_derived_from_bps=1
      "derived_share": float,       # derived_from_bps / trades si trades>0, sinon 0.0
    },
  }

  Vues prévues:
  - pnl_global      → 1 ligne agrégée (dimensions = {}).
  - pnl_by_exchange → 1 ligne par exchange (dimensions = {"exchange": "<CEX>"}).
  - pnl_by_account  → 1 ligne par (exchange, account_alias).
  - pnl_by_branch   → 1 ligne par (branch, exchange), branch dérivé de trade_mode (TT/TM/MM/REB…).


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
        """
               Retourne le chemin DB “du jour” pour ce db_dir.

               Convention M5-B2-2:
               - db_dir est attendu spécifique à une région/pod (ex: /var/lib/bot/EU/loggerh),
               - le nom du fichier encode la date: trades_log_YYYY-MM-DD.db.
               Plusieurs rotations intra-journalières peuvent exister via rotate_now()
               (suffixe timestamp + .gz), mais ce chemin représente toujours la DB courante.
               """
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

                # 4) net_profit_sign : supposé déjà dérivé côté LHM
                #    (fallback centralisé dans LoggerHistoriqueManager._fallback_net_profit_sign)
                #    Ici on ne fait que passer la valeur telle quelle jusqu'à la DB.
                #    Si net_profit_sign est absent, le trade sera visible comme “PnL incomplet”
                #    dans les analyses, ce qui est voulu (pas de dérivation locale).


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
        # [PATCH-SCHEMA] Tags SPLIT / profil / pacer + qualité PnL (idempotent)
        self._ensure_columns(conn, "trades", {
            "deployment_mode": "TEXT",
            "pod_region": "TEXT",
            "capital_profile": "TEXT",
            "pacer_mode": "TEXT",
            # [M5-A3-5] Qualité PnL centralisée par le LHM
            "net_profit_sign": "INTEGER",
            "pnl_missing": "INTEGER",
            "pnl_derived_from_bps": "INTEGER",
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

    def compute_pnl_views(self, since_ms: int | None = None, until_ms: int | None = None) -> dict:
        """
        Agrégats PnL multi-vues sur la fenêtre [since_ms, until_ms].

        Implémentation B2-1/B2-2 :
          - construit un conteneur "pnl_multiview" complet,
          - renseigne la vue globale (`pnl_global`) et `quality_global`,
          - construit également les vues par exchange / account / branch lorsque
            les colonnes nécessaires sont disponibles dans la table `trades`.

        Logique :
          - filtre les lignes de `trades` sur ts_ms si la colonne est disponible,
          - agrège net_profit, fees, executed_volume_usdc, net_profit_sign,
            pnl_missing, pnl_derived_from_bps,
          - calcule wins / losses / win_rate et les ratios de qualité.
        """
        import time
        import sqlite3

        now_ms = int(time.time() * 1000)
        if since_ms is None:
            since_ms = now_ms - 24 * 3600 * 1000
        if until_ms is None:
            until_ms = now_ms

        # Conteneur de sortie "canonique" (M5-C v2-B)
        out: dict[str, Any] = {
            "component": "pnl_multiview",
            # Devise PnL canonique ; pourra être rendue configurable plus tard
            "base_currency": "USDC",
            "window": {"since_ms": int(since_ms), "until_ms": int(until_ms)},
            "as_of_ts": time.time(),
            "pnl_global": {
                "dimensions": {},
                "pnl": {
                    "net_profit": 0.0,
                    "fees": 0.0,
                    "turnover_quote": 0.0,
                    "trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "win_rate": 0.0,
                },
                "quality": {
                    "pnl_missing": 0,
                    "pnl_missing_share": 0.0,
                    "derived_from_bps": 0,
                    "derived_share": 0.0,
                },
            },
            "pnl_by_exchange": [],
            "pnl_by_account": [],
            "pnl_by_branch": [],
            "quality_global": {
                "trades": 0,
                "pnl_missing": 0,
                "pnl_missing_share": 0.0,
                "derived_from_bps": 0,
                "derived_share": 0.0,
            },
        }

        try:
            with sqlite3.connect(self.db_path, timeout=10) as conn:
                cur = conn.cursor()
                # Vérifie la présence de la table trades
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades';")
                if not cur.fetchone():
                    return out

                # Colonnes disponibles
                cur.execute("PRAGMA table_info(trades);")
                cols = {row[1] for row in cur.fetchall()}

                # Clause WHERE temporelle best-effort
                if "ts_ms" in cols:
                    where = "WHERE ts_ms BETWEEN ? AND ?"
                    args = (since_ms, until_ms)
                else:
                    where = ""
                    args = tuple()

                # Nombre total de trades
                cur.execute(f"SELECT COUNT(1) FROM trades {where};", args)
                total_trades_row = cur.fetchone()
                try:
                    total_trades = int(total_trades_row[0] if total_trades_row else 0)
                except Exception:
                    total_trades = 0

                pnl_global = out["pnl_global"]["pnl"]
                quality_global = out["quality_global"]

                pnl_global["trades"] = total_trades
                quality_global["trades"] = total_trades

                # Si aucun trade, on retourne le squelette
                if total_trades == 0:
                    return out

                metric_cols = [
                    "net_profit",
                    "fees",
                    "executed_volume_usdc",
                    "net_profit_sign",
                    "pnl_missing",
                    "pnl_derived_from_bps",
                ]
                dim_cols = ["exchange", "account_alias", "trade_mode"]

                available_metric_cols = [name for name in metric_cols if name in cols]
                if not available_metric_cols:
                    return out

                select_cols = list(available_metric_cols)
                for d in dim_cols:
                    if d in cols and d not in select_cols:
                        select_cols.append(d)

                select_clause = ", ".join(select_cols)
                cur.execute(f"SELECT {select_clause} FROM trades {where};", args)

                sum_net_profit = 0.0
                sum_fees = 0.0
                sum_turnover = 0.0
                wins = 0
                losses = 0
                pnl_missing_count = 0
                derived_from_bps_count = 0

                # Agrégats par dimension
                by_exchange: dict[str, dict] = {}
                by_account: dict[tuple[str, str], dict] = {}
                by_branch: dict[tuple[str, str], dict] = {}

                def _ensure_bucket(store: dict, key):
                    b = store.get(key)
                    if b is None:
                        b = {
                            "trades": 0,
                            "net_profit": 0.0,
                            "fees": 0.0,
                            "turnover": 0.0,
                            "wins": 0,
                            "losses": 0,
                            "pnl_missing": 0,
                            "derived_from_bps": 0,
                        }
                        store[key] = b
                    return b

                rows = cur.fetchall()
                for row in rows:
                    rec = dict(zip(select_cols, row))

                    # net_profit / fees / turnover
                    np_raw = rec.get("net_profit")
                    fee_raw = rec.get("fees")
                    vol_raw = rec.get("executed_volume_usdc")

                    np_val = None
                    if np_raw is not None:
                        try:
                            np_val = float(np_raw)
                            sum_net_profit += np_val
                        except Exception:
                            np_val = None

                    if fee_raw is not None:
                        try:
                            sum_fees += float(fee_raw)
                        except Exception:
                            pass

                    vol_val = None
                    if vol_raw is not None:
                        try:
                            vol_val = float(vol_raw)
                            sum_turnover += abs(vol_val)
                        except Exception:
                            vol_val = None

                    # wins / losses (priorité au net_profit_sign si présent)
                    sign_val = rec.get("net_profit_sign")
                    sign_int = None
                    if sign_val is not None:
                        try:
                            sign_int = int(sign_val)
                        except Exception:
                            sign_int = None

                    # Compteurs globaux wins/losses
                    if sign_int is not None:
                        if sign_int > 0:
                            wins += 1
                        elif sign_int < 0:
                            losses += 1
                    elif np_val is not None:
                        if np_val > 0:
                            wins += 1
                        elif np_val < 0:
                            losses += 1

                    # Qualité PnL globale
                    missing_flag = False
                    if "pnl_missing" in rec:
                        try:
                            if int(rec.get("pnl_missing") or 0) != 0:
                                pnl_missing_count += 1
                                missing_flag = True
                        except Exception:
                            pass

                    derived_flag = False
                    if "pnl_derived_from_bps" in rec:
                        try:
                            if int(rec.get("pnl_derived_from_bps") or 0) != 0:
                                derived_from_bps_count += 1
                                derived_flag = True
                        except Exception:
                            pass

                    # --- Agrégats par dimension (best-effort) ---
                    ex = rec.get("exchange")
                    alias = rec.get("account_alias")
                    branch = rec.get("trade_mode")

                    # Par exchange
                    if ex is not None:
                        b_ex = _ensure_bucket(by_exchange, str(ex))
                        b_ex["trades"] += 1
                        if np_val is not None:
                            b_ex["net_profit"] += np_val
                        if fee_raw is not None:
                            try:
                                b_ex["fees"] += float(fee_raw)
                            except Exception:
                                pass
                        if vol_val is not None:
                            b_ex["turnover"] += abs(vol_val)
                        if sign_int is not None:
                            if sign_int > 0:
                                b_ex["wins"] += 1
                            elif sign_int < 0:
                                b_ex["losses"] += 1
                        elif np_val is not None:
                            if np_val > 0:
                                b_ex["wins"] += 1
                            elif np_val < 0:
                                b_ex["losses"] += 1
                        if missing_flag:
                            b_ex["pnl_missing"] += 1
                        if derived_flag:
                            b_ex["derived_from_bps"] += 1

                    # Par (exchange, account_alias)
                    if ex is not None and alias is not None:
                        key_acc = (str(ex), str(alias))
                        b_acc = _ensure_bucket(by_account, key_acc)
                        b_acc["trades"] += 1
                        if np_val is not None:
                            b_acc["net_profit"] += np_val
                        if fee_raw is not None:
                            try:
                                b_acc["fees"] += float(fee_raw)
                            except Exception:
                                pass
                        if vol_val is not None:
                            b_acc["turnover"] += abs(vol_val)
                        if sign_int is not None:
                            if sign_int > 0:
                                b_acc["wins"] += 1
                            elif sign_int < 0:
                                b_acc["losses"] += 1
                        elif np_val is not None:
                            if np_val > 0:
                                b_acc["wins"] += 1
                            elif np_val < 0:
                                b_acc["losses"] += 1
                        if missing_flag:
                            b_acc["pnl_missing"] += 1
                        if derived_flag:
                            b_acc["derived_from_bps"] += 1

                    # Par (branch, exchange)
                    if branch is not None and ex is not None:
                        key_br = (str(branch), str(ex))
                        b_br = _ensure_bucket(by_branch, key_br)
                        b_br["trades"] += 1
                        if np_val is not None:
                            b_br["net_profit"] += np_val
                        if fee_raw is not None:
                            try:
                                b_br["fees"] += float(fee_raw)
                            except Exception:
                                pass
                        if vol_val is not None:
                            b_br["turnover"] += abs(vol_val)
                        if sign_int is not None:
                            if sign_int > 0:
                                b_br["wins"] += 1
                            elif sign_int < 0:
                                b_br["losses"] += 1
                        elif np_val is not None:
                            if np_val > 0:
                                b_br["wins"] += 1
                            elif np_val < 0:
                                b_br["losses"] += 1
                        if missing_flag:
                            b_br["pnl_missing"] += 1
                        if derived_flag:
                            b_br["derived_from_bps"] += 1

                # Remplissage des champs globaux
                pnl_global["net_profit"] = float(sum_net_profit)
                pnl_global["fees"] = float(sum_fees)
                pnl_global["turnover_quote"] = float(sum_turnover)
                pnl_global["wins"] = int(wins)
                pnl_global["losses"] = int(losses)

                denom = max(1, wins + losses)
                pnl_global["win_rate"] = float(wins) / float(denom)

                quality = out["pnl_global"]["quality"]
                quality["pnl_missing"] = int(pnl_missing_count)
                quality["derived_from_bps"] = int(derived_from_bps_count)

                if total_trades > 0:
                    quality["pnl_missing_share"] = float(pnl_missing_count) / float(total_trades)
                    quality["derived_share"] = float(derived_from_bps_count) / float(total_trades)
                    quality_global["pnl_missing_share"] = quality["pnl_missing_share"]
                    quality_global["derived_share"] = quality["derived_share"]
                else:
                    quality["pnl_missing_share"] = 0.0
                    quality["derived_share"] = 0.0
                    quality_global["pnl_missing_share"] = 0.0
                    quality_global["derived_share"] = 0.0

                quality_global["pnl_missing"] = int(pnl_missing_count)
                quality_global["derived_from_bps"] = int(derived_from_bps_count)

                # --- Construction des vues multi-dimensions à partir des buckets ---

                def _bucket_to_pnl_row(dimensions: dict, agg: dict) -> dict:
                    trades_b = int(agg.get("trades") or 0)
                    wins_b = int(agg.get("wins") or 0)
                    losses_b = int(agg.get("losses") or 0)
                    denom_b = max(1, wins_b + losses_b)
                    win_rate_b = float(wins_b) / float(denom_b)
                    pnl_missing_b = int(agg.get("pnl_missing") or 0)
                    derived_b = int(agg.get("derived_from_bps") or 0)
                    if trades_b > 0:
                        missing_share_b = float(pnl_missing_b) / float(trades_b)
                        derived_share_b = float(derived_b) / float(trades_b)
                    else:
                        missing_share_b = 0.0
                        derived_share_b = 0.0
                    return {
                        "dimensions": dimensions,
                        "pnl": {
                            "net_profit": float(agg.get("net_profit") or 0.0),
                            "fees": float(agg.get("fees") or 0.0),
                            "turnover_quote": float(agg.get("turnover") or 0.0),
                            "trades": trades_b,
                            "wins": wins_b,
                            "losses": losses_b,
                            "win_rate": win_rate_b,
                        },
                        "quality": {
                            "pnl_missing": pnl_missing_b,
                            "pnl_missing_share": missing_share_b,
                            "derived_from_bps": derived_b,
                            "derived_share": derived_share_b,
                        },
                    }

                # by_exchange
                rows_ex = []
                for ex, agg in by_exchange.items():
                    dims = {"exchange": ex}
                    rows_ex.append(_bucket_to_pnl_row(dims, agg))
                out["pnl_by_exchange"] = rows_ex

                # by_account (exchange, account_alias)
                rows_acc = []
                for (ex, alias), agg in by_account.items():
                    dims = {"exchange": ex, "account_alias": alias}
                    rows_acc.append(_bucket_to_pnl_row(dims, agg))
                out["pnl_by_account"] = rows_acc

                # by_branch (branch, exchange)
                rows_br = []
                for (branch, ex), agg in by_branch.items():
                    dims = {"branch": branch, "exchange": ex}
                    rows_br.append(_bucket_to_pnl_row(dims, agg))
                out["pnl_by_branch"] = rows_br

        except Exception:
            # Best-effort : ne doit jamais casser le pipeline appelant
            logger.exception("compute_pnl_views failed")
        return out

    def compute_pnl_views_for_local_day(
        self,
        local_day: str | None = None,
        pod_region: str | None = None,
    ) -> dict:
        """Agrégats PnL multi-vues pour une journée calendaire locale.

        Cette méthode résout une fenêtre [since_ms, until_ms] correspondant à un
        `local_day` donné dans la timezone associée à `pod_region` (EU/US, etc.),
        puis délègue le calcul des agrégats à :meth:`compute_pnl_views`.

        Paramètres
        ----------
        local_day:
            Jour local au format ``'YYYY-MM-DD'`` dans la timezone de
            ``pod_region``. Si None, utilise la date locale du jour courant.
        pod_region:
            Région logique du pod ("EU", "US", ...). Si None, on utilise le
            même fallback que `_derive_time_fields` (EU → Europe/Rome).

        Retour
        ------
        dict
            Structure "pnl_multiview" identique à :meth:`compute_pnl_views`,
            mais avec la fenêtre temporelle calée sur la journée locale.
        """  # noqa: E501
        from datetime import datetime, timezone, timedelta
        import time

        # Résolution robuste de la timezone locale à partir de pod_region
        region = (pod_region or "EU").upper()
        tz_name = _REGION_TZ.get(region, "Europe/Rome")

        # Choix du jour local à couvrir
        try:
            if local_day:
                local_day_str = str(local_day).strip()
            else:
                # Date locale "aujourd'hui" dans la timezone du pod
                if ZoneInfo is not None:
                    dt_loc_now = datetime.now(ZoneInfo(tz_name))
                else:
                    dt_loc_now = datetime.now()
                local_day_str = dt_loc_now.strftime("%Y-%m-%d")

            # Construction du début/fin de journée locale
            dt_start_loc = datetime.strptime(local_day_str, "%Y-%m-%d")
            if ZoneInfo is not None:
                dt_start_loc = dt_start_loc.replace(tzinfo=ZoneInfo(tz_name))
            else:
                # Fallback: on suppose que dt_start_loc est déjà en UTC
                dt_start_loc = dt_start_loc.replace(tzinfo=timezone.utc)

            dt_end_loc = dt_start_loc + timedelta(days=1)

            dt_start_utc = dt_start_loc.astimezone(timezone.utc)
            dt_end_utc = dt_end_loc.astimezone(timezone.utc)

            since_ms = int(dt_start_utc.timestamp() * 1000)
            # Fenêtre [start, next_day) → on enlève 1 ms pour rester inclusif
            until_ms = int(dt_end_utc.timestamp() * 1000) - 1
        except Exception:
            # Fallback best-effort : 24h glissantes
            now_ms = int(time.time() * 1000)
            since_ms = now_ms - 24 * 3600 * 1000
            until_ms = now_ms

        return self.compute_pnl_views(since_ms=since_ms, until_ms=until_ms)


    def compute_pnl_summary(self, since_ms: int | None = None, until_ms: int | None = None) -> dict:
        """
        Petits agrégats PnL "comptables" sur la fenêtre [since_ms, until_ms].

        Basé exclusivement sur la table `trades`:
        - somme net_profit (quote),
        - somme fees (quote),
        - nombre total de trades,
        - gains/pertes (wins/losses),
        - compteurs de qualité PnL (pnl_missing, pnl_derived_from_bps).

        Best-effort: si la table/colonnes n'existent pas, retourne des zéros sans lever d'exception.
        """
        import sqlite3, time

        now_ms = int(time.time() * 1000)
        if since_ms is None:
            since_ms = now_ms - 24 * 3600 * 1000
        if until_ms is None:
            until_ms = now_ms

        out = {
            "window": {"since_ms": since_ms, "until_ms": until_ms},
            "pnl": {
                "net_profit": 0.0,
                "fees": 0.0,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "pnl_missing": 0,
                "derived_from_bps": 0,
            },
        }

        # Si la DB n'existe pas encore, on retourne les valeurs par défaut.
        if not os.path.exists(self.db_path):
            return out

        def _has_cols(conn, table: str, names: set[str]) -> set[str]:
            cur = conn.cursor()
            cur.execute(f"PRAGMA table_info({table});")
            cols = {r[1] for r in cur.fetchall()}
            return names & cols

        with sqlite3.connect(self.db_path, timeout=10) as conn:
            cur = conn.cursor()

            needed = {"ts_ms", "net_profit", "fees", "net_profit_sign", "pnl_missing", "pnl_derived_from_bps"}
            cols = _has_cols(conn, "trades", needed)

            # Si la table trades n'existe pas ou ne contient aucune des colonnes attendues,
            # on retourne simplement la structure par défaut.
            if not cols:
                return out

            where = ""
            params: tuple = ()
            if "ts_ms" in cols:
                where = " WHERE ts_ms BETWEEN ? AND ?"
                params = (since_ms, until_ms)

            # net_profit + nombre total de trades
            try:
                if "net_profit" in cols:
                    cur.execute(f"SELECT COALESCE(SUM(net_profit), 0.0), COUNT(1) FROM trades{where};", params)
                else:
                    cur.execute(f"SELECT 0.0, COUNT(1) FROM trades{where};", params)
                row = cur.fetchone() or (0.0, 0)
                net_profit_sum = float(row[0] or 0.0)
                trades_count = int(row[1] or 0)
            except Exception:
                # best-effort: en cas d'erreur, on préfère retourner l'out partiel
                return out

            # fees
            fees_sum = 0.0
            if "fees" in cols:
                try:
                    cur.execute(f"SELECT COALESCE(SUM(fees), 0.0) FROM trades{where};", params)
                    fees_row = cur.fetchone() or (0.0,)
                    fees_sum = float(fees_row[0] or 0.0)
                except Exception:
                    fees_sum = 0.0

            # wins / losses
            wins = losses = 0
            try:
                if "net_profit_sign" in cols:
                    cur.execute(
                        f"""SELECT
                        SUM(CASE WHEN net_profit_sign > 0 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN net_profit_sign < 0 THEN 1 ELSE 0 END)
                        FROM trades{where};""",
                        params,
                    )
                elif "net_profit" in cols:
                    cur.execute(
                        f"""SELECT
                        SUM(CASE WHEN net_profit > 0 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN net_profit < 0 THEN 1 ELSE 0 END)
                        FROM trades{where};""",
                        params,
                    )
                row = cur.fetchone() or (0, 0)
                wins = int(row[0] or 0)
                losses = int(row[1] or 0)
            except Exception:
                wins = losses = 0

            # flags de qualité PnL
            pnl_missing = 0
            if "pnl_missing" in cols:
                try:
                    cur.execute(
                        f"SELECT SUM(CASE WHEN pnl_missing THEN 1 ELSE 0 END) FROM trades{where};",
                        params,
                    )
                    row = cur.fetchone() or (0,)
                    pnl_missing = int(row[0] or 0)
                except Exception:
                    pnl_missing = 0

            derived_from_bps = 0
            if "pnl_derived_from_bps" in cols:
                try:
                    cur.execute(
                        f"SELECT SUM(CASE WHEN pnl_derived_from_bps THEN 1 ELSE 0 END) FROM trades{where};",
                        params,
                    )
                    row = cur.fetchone() or (0,)
                    derived_from_bps = int(row[0] or 0)
                except Exception:
                    derived_from_bps = 0

        out["pnl"]["net_profit"] = net_profit_sum
        out["pnl"]["fees"] = fees_sum
        out["pnl"]["trades"] = trades_count
        out["pnl"]["wins"] = wins
        out["pnl"]["losses"] = losses
        out["pnl"]["pnl_missing"] = pnl_missing
        out["pnl"]["derived_from_bps"] = derived_from_bps

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

    def insert_alerts_bulk(self, alerts: Iterable[Dict[str, Any]]) -> int:
        rows = list(alerts or [])
        if not rows:
            return 0
        with self._lock:
            self._rotate_if_needed()
            with self._conn() as conn:
                cur = conn.cursor()
                payloads: List[Tuple] = []
                for alert in rows:
                    module = str(alert.get("module") or alert.get("source") or "LoggerHistorique")
                    level = str(alert.get("level") or alert.get("severity") or "INFO")
                    pair_key = alert.get("pair_key")
                    message = str(alert.get("message") or alert.get("msg") or alert.get("reason") or "alert")
                    ctx = alert.get("ctx")
                    if ctx is None:
                        ctx = json.dumps(alert, default=str)
                    payloads.append(
                        (
                            alert.get("ts") or datetime.utcnow().isoformat(),
                            module,
                            level,
                            pair_key,
                            message,
                            ctx,
                        )
                    )
                cur.executemany(
                    """
                    INSERT INTO alerts(ts, module, level, pair_key, message, ctx)
                    VALUES (?,?,?,?,?,?)
                    """,
                    payloads,
                )
                conn.commit()
                return len(payloads)


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

    def insert_generic(self, table: str, rows: Iterable[Dict[str, Any]]) -> int:
        table_name = str(table or "").strip()
        if not table_name:
            return 0
        if not all(c.isalnum() or c == "_" for c in table_name):
            logger.warning("insert_generic: invalid table %s", table)
            return 0
        payloads = list(rows or [])
        if not payloads:
            return 0
        with self._lock:
            self._rotate_if_needed()
            with self._conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                    (table_name,),
                )
                if cur.fetchone() is None:
                    logger.warning("insert_generic: table %s does not exist", table_name)
                    return 0
                cur.execute(f"PRAGMA table_info({table_name});")
                cols = {row[1] for row in cur.fetchall()}
                if not cols:
                    return 0
                inserted = 0
                for payload in payloads:
                    filtered = {k: v for k, v in (payload or {}).items() if k in cols}
                    if not filtered:
                        continue
                    if "ts" in cols and "ts" not in filtered:
                        filtered["ts"] = datetime.utcnow().isoformat()
                    keys = ", ".join(filtered.keys())
                    qmarks = ", ".join(["?"] * len(filtered))
                    cur.execute(
                        f"INSERT INTO {table_name} ({keys}) VALUES ({qmarks})",
                        list(filtered.values()),
                    )
                    inserted += 1
                conn.commit()
                return inserted

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
