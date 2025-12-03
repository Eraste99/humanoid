# -*- coding: utf-8 -*-
from __future__ import annotations
"""
LoggerHistoriqueManager — façade unique et async-safe (tri-CEX ready).

Sous-modules encapsulés:
- BaseTradeLogger      → batch passif + normalisation (IDs, latences, TT/TM/REB, route/buy_ex/sell_ex/leg_role)
- LogWriter            → I/O SQLite rotatif (schémas étendus + table latency)
- PairHistoryTracker   → scoring/rotation + moyennes de latences par paire (EMA)

Contrat DB vs JSONL (M5-B2-1):
- La **DB SQLite gérée par LogWriter est la source de vérité PnL / funnels / dashboards**.
  → PnLAggregator, les vues SQL et les dashboards doivent lire la DB, pas les JSONL.
- Les **streams JSONL.gz (_JsonlRotator) sont un flux append-only à vocation forensic/replay/audit**.
  → Utilisés pour rejouer une journée ou remonter un trace_id, mais pas comme base de calcul PnL officielle.
- En cas de divergence DB vs JSONL, c’est la DB qui fait foi; les JSONL servent à expliquer / enquêter.

Rôle PnL (M5-C) :
- LoggerHistoriqueManager est la façade officielle PnL côté "plane historique" :
  les modules amont (Engine, Scanner, RiskManager, Rebalancing, etc.) ne doivent
  pas interroger directement la DB SQLite, mais passer par des APIs de ce manager
  (get_funnel, futures vues PnL agrégées, etc.).
- Toute API PnL utilisée par le RiskManager (pnl_guard, drawdown, modes spéciaux)
  ou par les dashboards doit s'appuyer sur des agrégats fournis par LogWriter via
  cette façade, afin de garantir un contrat unique et audit-grade.

Nouveautés:
- record_latency(metrics): persiste dans table `latency` + (optionnel) métriques Prometheus.
- get_pair_history(...) expose aussi trade_mode/account_alias/sub_account_id/route/buy_ex/sell_ex/leg_role.
- APIs par branche (TT/TM/REB): get_active_pairs(mode), get_priority_pairs(mode, k), get_rank_map(mode), rotate_now(),
  set_max_active_by_mode(...). Fallback sécurisé si le tracker n’a pas encore les méthodes *_by_mode(...).
- **Flush immédiat**: `await flush_now(rotate=True)` vide les buffers et pousse les métriques paires.
Rôle PnL multi-vues (M5-C v2-B):
- LoggerHistoriqueManager expose une façade unique pour récupérer les vues PnL
  agrégées "pnl_multiview" produites par LogWriter. Ces vues sont destinées à
  être consommées par:
  * le RiskManager (via un provider de type pnl_guard),
  * les dashboards PnL,
  * les outils de diagnostic / audit.
- Le contrat cible "pnl_multiview" est le suivant:
  {
    "component": "pnl_multiview",
    "base_currency": "USDC" | "EUR",
    "window": {"since_ms": int, "until_ms": int},
    "as_of_ts": float,
    "pnl_global": PnlRow,
    "pnl_by_exchange": [PnlRow, ...],
    "pnl_by_account": [PnlRow, ...],
    "pnl_by_branch": [PnlRow, ...],
    "quality_global": {...},
  }
  avec `PnlRow` défini comme dans log_writer.LogWriter (dimensions + bloc `pnl`
  + bloc `quality`).
- Les méthodes de façade comme `get_pnl_multiview(...)` (à introduire) doivent
  simplement:
  * choisir une fenêtre [since_ms, until_ms] cohérente (ex: 24h glissante),
  * appeler LogWriter.compute_pnl_views(...),
  * renvoyer la structure telle quelle (éventuellement enrichie d'un tout petit
    bloc de flags pipeline), sans recalcul PnL côté LHM.

"""


import asyncio
import json
import logging
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional
from modules.logger_historique.log_writer import LogWriter
from modules.logger_historique.base_trade_logger import BaseTradeLogger
from modules.logger_historique.pair_history_tracker import PairHistoryTracker, TrackerConfig
from modules.obs_metrics import update_storage_metrics
import os, time
import hashlib, json, time
from pathlib import Path

try:
    from modules.obs_metrics import obs_is_ready
    _PROM_READY = bool(obs_is_ready())
except Exception:
    _PROM_READY = False

# --- JSONL/GZIP + stockage -----------------------------------------------
import gzip, io, shutil
from pathlib import Path
import contextlib

# --- JSONL rotator unifié (taille + âge), thread-safe ---
import os, time, threading, gzip
from pathlib import Path

class _JsonlRotator:
    """
    Rotateur thread-safe de fichiers .jsonl.gz append-only.

    Politique M5-B2-2:
    - Rotation par taille (max_bytes) ET par âge (max_age_s).
      Defaults (via LoggerHistoriqueManager): 256 MiB et 1h de TTL par stream.
    - Nommage des fichiers: <stream>-YYYYMMDD-HHMMSS.jsonl.gz, où `stream` inclut déjà
      éventuellement db_name/pod_region côté LoggerHistoriqueManager.
    - Conçu pour le forensic/replay; la source de vérité PnL reste la DB SQLite (LogWriter).
    """
    def __init__(self, base_dir: str, stream: str, *, max_bytes: int = 50 * (1 << 20), max_age_s: int = 3600):
        self.base = Path(base_dir)
        self.base.mkdir(parents=True, exist_ok=True)
        self.stream = str(stream)
        self.max_bytes = int(max_bytes)
        self.max_age_s = int(max_age_s)
        self._lock = threading.Lock()
        self._fh = None      # type: ignore
        self._gz = None      # type: ignore
        self._path: Path | None = None
        self._opened_epoch = 0.0
        self._bytes = 0
        self._open_new()

    # --- état courant ---
    @property
    def current_path(self) -> str | None:
        return str(self._path) if self._path else None

    @property
    def current_bytes(self) -> int:
        return int(self._bytes)

    # --- interne ---
    def _new_name(self) -> Path:
        ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
        return self.base / f"{self.stream}-{ts}.jsonl.gz"

    def _open_new(self) -> None:
        if self._gz:
            try: self._gz.close()
            finally: self._gz = None
        if self._fh:
            try: self._fh.close()
            finally: self._fh = None
        self._path = self._new_name()
        self._fh = open(self._path, "ab")
        self._gz = gzip.GzipFile(fileobj=self._fh, mode="ab", compresslevel=5)
        self._opened_epoch = time.time()
        self._bytes = 0

    def _rotate_if_needed(self) -> bool:
        need = False
        if (time.time() - self._opened_epoch) >= self.max_age_s:
            need = True
        elif self._bytes >= self.max_bytes:
            need = True
        if need:
            self._open_new()
        return need

    def rotate_if_needed(self) -> bool:
        with self._lock:
            return self._rotate_if_needed()

    # --- API ---
    def write_line(self, b: bytes) -> None:
        if not b:
            return
        with self._lock:
            if not self._gz:
                self._open_new()
            self._gz.write(b)
            self._bytes += len(b)
            # rotation taille immédiate; rotation âge testée à chaque tick worker
            if self._bytes >= self.max_bytes:
                self._open_new()

    def flush(self) -> None:
        with self._lock:
            if self._gz:
                self._gz.flush()

#python -m pip install -U pip setuptools wheel pour le package cryptography
#python -m pip install cryptography>=41#

# --- OBS/METRICS (no-op fallback si absentes) -----------------------------
try:
    from modules.obs_metrics import (
        LOGGERH_JSONL_BYTES,              # gauge{stream}
        LOGGERH_JSONL_ROTATIONS_TOTAL,    # counter{stream}
        STORAGE_USAGE_PCT,                # gauge{mount}
        STORAGE_ALERTS_TOTAL,
        loggerh_observe_write_ms, lhm_on_ingested, lhm_on_dropped, lhm_set_queue_size, lhm_on_rotation,
        LOGGERH_QUEUE_PLATEAU_TOTAL, LOGGERH_LAST_FLUSH_TS_SECONDS, LOGGERH_LAST_ROTATION_TS_SECONDS,LOGGERH_FILE_ROTATIONS_TOTAL,
        # Storage
        update_storage_metrics, STORAGE_USAGE_PCT, STORAGE_BYTES_FREE,
        # counter{level}
        loggerh_write_ms, lhm_jsonl_queue_size, lhm_jsonl_queue_cap,
        lhm_jsonl_ingested_total, lhm_jsonl_dropped_total, lhm_flush_batch_current,
        schema_violation_total, log_dedup_total, log_replay_total,
        forensic_chain_head, forensic_verify_ok_total,
        set_lhm_queue, mark_schema_violation, mark_drop, mark_ingested,
        set_flush_batch, set_forensic_head_numeric,
        # --- PnL reconciliation CEX ↔ DB (M5-D-2) ---
        PNL_RECO_LAST_RUN_TS_SECONDS,
        PNL_RECO_STATE,
        PNL_RECO_ABS_DIFF_QUOTE,
        PNL_RECO_MISMATCH_TOTAL,
        PNL_RECO_ERRORS_TOTAL,
    )
except Exception:
    class _NoopM:
        def labels(self, *a, **k): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
    LOGGERH_JSONL_BYTES = _NoopM()
    LOGGERH_JSONL_ROTATIONS_TOTAL = _NoopM()
    STORAGE_USAGE_PCT = _NoopM()
    STORAGE_ALERTS_TOTAL = _NoopM()
    # Fallbacks pour les métriques de reco PnL
    PNL_RECO_LAST_RUN_TS_SECONDS = _NoopM()
    PNL_RECO_STATE = _NoopM()
    PNL_RECO_ABS_DIFF_QUOTE = _NoopM()
    PNL_RECO_MISMATCH_TOTAL = _NoopM()
    PNL_RECO_ERRORS_TOTAL = _NoopM()

# Chemin optionnel sur lequel sonder le stockage (peut être un dossier JSONL)
LHM_STORAGE_PATH = os.getenv("LHM_STORAGE_PATH")  # ex: /var/lib/arbitrage-bot/data

def _update_storage_safe(candidate_path: str | None = None) -> None:
    """
    Best-effort: choisit un dossier pertinent et met à jour les jauges storage.
    - candidate_path: un chemin de fichier ou dossier (on prendra son dossier)
    - sinon: var d'env LHM_STORAGE_PATH
    - sinon: tente de déduire à partir des attributs connus du manager
    """
    try:
        path = candidate_path or LHM_STORAGE_PATH
        # fallback heuristique: on tente d'inspecter l'instance globale du manager si dispo
        if not path:
            mgr = globals().get("LOGGER_HISTORIQUE_MANAGER") or globals().get("logger_manager")  # optionnel
            for attr in ("jsonl_dir", "base_dir", "output_dir", "root_dir", "log_dir"):
                p = getattr(mgr, attr, None) if mgr else None
                if p:
                    path = p
                    break
        if not path:
            path = "."  # dernier recours: cwd

        # si on nous passe un fichier, on remonte au dossier
        if not os.path.isdir(path):
            path = os.path.dirname(path) or "."

        update_storage_metrics(path)
    except Exception:
        # observabilité best-effort seulement: jamais bloquant
        pass

# Chemin du dossier à monitorer (définissable via env)
LHM_STORAGE_PATH = os.getenv("LHM_STORAGE_PATH")  # ex: /var/lib/arbitrage-bot/data

_storage_task: asyncio.Task | None = None

def _resolve_storage_path(candidate_path: str | None = None) -> str:
    path = candidate_path or LHM_STORAGE_PATH
    if not path:
        # Dernier recours : cwd
        path = "."
    # Si on nous passe un fichier, prendre son dossier
    if not os.path.isdir(path):
        path = os.path.dirname(path) or "."
    return path

def _safe_int(val, default: int) -> int:
    if val in (None, "", "None"):
        return default
    try:
        return int(val)
    except Exception:
        return default


async def _storage_monitor(period_s: float, path: str) -> None:
    path = _resolve_storage_path(path)
    while True:
        try:
            update_storage_metrics(path)
        except Exception:
            logging.getLogger("lhm.storage").exception("update_storage_metrics failed")
        await asyncio.sleep(max(1.0, float(period_s)))

def start_storage_monitor(path: str | None = None, period_s: float = 60.0) -> None:
    """Démarre la tâche asyncio de monitoring du stockage."""
    global _storage_task
    if _storage_task and not _storage_task.done():
        return
    loop = asyncio.get_running_loop()
    _storage_task = loop.create_task(_storage_monitor(period_s, path or LHM_STORAGE_PATH))

async def stop_storage_monitor() -> None:
    """Arrête proprement la tâche asyncio (si active)."""
    global _storage_task
    if _storage_task:
        _storage_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _storage_task
        _storage_task = None


logger = logging.getLogger("LoggerHistoriqueManager")
# --- JSONL rotator minimal (append-only .jsonl.gz avec rotation taille/âge) ---
import os, gzip, time, threading

# ------------------------------------------------------------
# Observabilité centralisée (uniquement via modules.obs_metrics)
# ------------------------------------------------------------
try:
    from modules.obs_metrics import (
        # Latency histograms & counters (centralisés)
        LAT_ACK_MS, LAT_FILL_FIRST_MS, LAT_FILL_ALL_MS, LAT_E2E_MS,
        LAT_EVENTS_TOTAL, LAT_PIPELINE_EVENTS_TOTAL,
        # LHM metrics
        LOGGERH_WRITE_MS, LOGGERH_QUEUE_PLATEAU_TOTAL, LOGGERH_DB_STALLS_TOTAL,
        LOGGERH_TRADE_QUEUE_SIZE, LOGGERH_DB_FILE_BYTES,
        LOGGERH_LAST_ROTATION_TS_SECONDS, LOGGERH_LAST_FLUSH_TS_SECONDS,
        LOGGERH_JSONL_ROTATIONS_TOTAL,
        # JSONL streams metrics
        LHM_JSONL_INGESTED_TOTAL, LHM_JSONL_DROPPED_TOTAL, LHM_JSONL_QUEUE_SIZE,
    )
except Exception:
    # Fallback no-op (zéro crash si obs indisponible)
    class _Noop:
        def labels(self, *a, **k): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def observe(self, *a, **k): pass
    LAT_ACK_MS = LAT_FILL_FIRST_MS = LAT_FILL_ALL_MS = LAT_E2E_MS = _Noop()
    LAT_EVENTS_TOTAL = LAT_PIPELINE_EVENTS_TOTAL = _Noop()
    LOGGERH_WRITE_MS = LOGGERH_QUEUE_PLATEAU_TOTAL = LOGGERH_DB_STALLS_TOTAL = _Noop()
    LOGGERH_TRADE_QUEUE_SIZE = LOGGERH_DB_FILE_BYTES = _Noop()
    LOGGERH_LAST_ROTATION_TS_SECONDS = LOGGERH_LAST_FLUSH_TS_SECONDS = _Noop()
    LOGGERH_JSONL_ROTATIONS_TOTAL = _Noop()
    LHM_JSONL_INGESTED_TOTAL = LHM_JSONL_DROPPED_TOTAL = LHM_JSONL_QUEUE_SIZE = _Noop()


class LoggerHistoriqueManager:
    def __init__(
        self,
        cfg,
        out_dir: str,
        *,
        tracker_config=None,            # ex: TrackerConfig
        trade_filter=None,
        trade_batch_size: int = 30,
        trade_flush_interval: float = 0.35,
        rotate_every_s: float | None = None,  # défaut: tracker_config.rotation_interval
    ):
        """
        Logger Historique piloté par cfg.lhm + tags globaux cfg.g.*
        - ZÉRO os.getenv : tout vient de la config.
        - Streams JSONL.gz rotatifs + index + watchdogs + métriques.
        """

        self.cfg = cfg
        self.out_dir = out_dir
        # Cap coda JSONL con fallback robusto
        raw_cap = getattr(self.cfg, "LHM_JSONL_QUEUE_CAP", 5000)
        self._jsonl_queue_cap = _safe_int(raw_cap, 5000)

        # -------- Tags globaux (déploiement/profil/pacer) --------
        self._deployment_mode = str(getattr(self.cfg.g, "deployment_mode", "EU_ONLY"))
        self._pod_region      = str(getattr(self.cfg.g, "pod_region", "EU"))
        self._capital_profile = str(getattr(self.cfg.g, "capital_profile", "NANO")).upper()
        self._pacer_mode      = str(getattr(self.cfg.g, "pacer_mode", "NORMAL")).upper()

        # -------- Paramètres LHM (source de vérité) ---------------
        L = self.cfg.lhm
        # Bornes et batching des files streams
        self._q_stream_max   = int(getattr(L, "LHM_Q_STREAM_MAX", 20_000))
        self._stream_batch   = int(getattr(L, "LHM_STREAM_BATCH", 256))
        self._drop_when_full = bool(getattr(L, "LHM_DROP_WHEN_FULL", True))
        self._high_watermark = float(getattr(L, "LHM_HIGH_WATERMARK_RATIO", 0.80))
        self._plateau_s      = int(getattr(L, "LHM_MAX_QUEUE_PLATEAU_S", 60))

        # Sampling MM (cfg > défauts par profil capital)
        _def_q = {"NANO": 0.10, "MICRO": 0.20, "SMALL": 0.50, "MID": 0.75, "LARGE": 1.00}.get(self._capital_profile, 0.10)
        _def_c = {"NANO": 0.05, "MICRO": 0.10, "SMALL": 0.25, "MID": 0.50, "LARGE": 0.75}.get(self._capital_profile, 0.05)
        self._mm_sampling_quotes  = float(getattr(L, "LHM_MM_SAMPLING_QUOTES",  _def_q))
        self._mm_sampling_cancels = float(getattr(L, "LHM_MM_SAMPLING_CANCELS", _def_c))

        # Limites rotation/retention JSONL (M5-B2-2)
        # - LHM_JSONL_MAX_BYTES: taille max d'un fichier .jsonl.gz par stream (append-only).
        # - LHM_JSONL_MAX_AGE_S: TTL max avant rotation forcée (même si la taille est faible).
        #   Defaults: 256 MiB / 1h → dimensionnés pour 500 paires en charge sans fichiers géants.
        self._jsonl_max_bytes = int(getattr(L, "LHM_JSONL_MAX_BYTES", 256 * 1024 * 1024))  # 256 MiB
        self._jsonl_max_age_s = int(getattr(L, "LHM_JSONL_MAX_AGE_S", 3600))  # 1h

        # Seuils d’alertes stockage
        self._disk_alert_warn_pct = float(getattr(L, "LHM_DISK_WARN_PCT", 70.0))
        self._disk_alert_crit_pct = float(getattr(L, "LHM_DISK_CRIT_PCT", 85.0))
        self._last_storage_alert_level = None  # None|"WARN"|"ERROR"

        # Watchdogs/rotation DB (métriques globales + bornes taille/âge)
        # - LHM_DB_MAX_BYTES: garde-fou sur la taille maximale du fichier .db courant.
        # - LHM_DB_MAX_AGE_S: TTL max avant rotation forcée, même si la date ne change pas.
        #   En pratique: rotation quotidienne par date + rotation size/age si la DB gonfle trop vite.
        self._db_name = str(getattr(L, "LHM_DB_NAME", "loggerh"))
        self._max_db_bytes = int(getattr(L, "LHM_DB_MAX_BYTES", 256 * 1024 * 1024))
        self._max_db_age_s = int(getattr(L, "LHM_DB_MAX_AGE_S", 24 * 3600))

        self._wd_interval_s = float(getattr(L, "LHM_WD_INTERVAL_S", 2.0))
        self._wd_queue_plateau_window = int(getattr(L, "LHM_WD_PLATEAU_WINDOW", 15))
        self._wd_queue_min_size = int(getattr(L, "LHM_WD_QUEUE_MIN_SIZE", 50))
        self._wd_stall_threshold_s = float(getattr(L, "LHM_WD_STALL_THRESHOLD_S", 10.0))

        # -------- Réconciliation PnL CEX ↔ DB (M5-D-2) ----------
        # Ces paramètres pilotent uniquement les helpers de reco PnL.
        # Ils n'ont aucun effet direct sur le hot-path de trading.
        self._pnl_reco_enabled = bool(getattr(L, "LHM_PNL_RECO_ENABLED", False))

        # Tolérance absolue (en devise PnL canonique, ex. USDC)
        self._pnl_reco_tol_abs_quote = float(
            getattr(L, "LHM_PNL_RECO_TOL_ABS_QUOTE", 25.0)
        )

        # Tolérance relative (en fraction, ex. 0.01 = 1%)
        self._pnl_reco_tol_pct = float(
            getattr(L, "LHM_PNL_RECO_TOL_PCT", 0.01)
        )

        # Limite d'âge pour les journées éligibles à la reco (EOD / replay)
        self._pnl_reco_max_lookback_days = int(
            getattr(L, "LHM_PNL_RECO_MAX_LOOKBACK_DAYS", 3)
        )

        # Région par défaut utilisée si aucun pod_region n'est fourni à l'API de reco
        default_region = getattr(self.cfg.g, "pod_region", "EU")
        self._pnl_reco_default_region = str(
            getattr(L, "LHM_PNL_RECO_DEFAULT_REGION", default_region)
        ).upper()




        # -------- I/O & trackers ---------------------------------
        db_dir = str(Path(out_dir))
        self._writer = LogWriter(db_dir=db_dir)
        self._tracker = PairHistoryTracker(tracker_config)
        self._trade_logger = BaseTradeLogger(
            log_type="trades",
            trade_filter=trade_filter or (lambda _t: True),
            batch_size=int(getattr(L, "LHM_TRADE_BATCH_SIZE", trade_batch_size)),
            flush_interval=float(getattr(L, "LHM_TRADE_FLUSH_INTERVAL_S", trade_flush_interval)),
        )
        self._trade_logger.set_event_sink(self._on_event_sink)

        # -------- Pipelines JSONL.gz ------------------------------
        # -------- Pipelines JSONL.gz ------------------------------
        self._jsonl_dir = str(Path(db_dir) / "streams")
        # Nommage canon M5-B2-2:
        #   <db_name>-<pod_region>-<stream>-YYYYMMDD-HHMMSS.jsonl.gz
        # ex: loggerh-EU-fills_normalized-20251130-120000.jsonl.gz
        stream_prefix = f"{self._db_name}-{self._pod_region}".replace(" ", "_")

        self._jsonl: dict[str, _JsonlRotator] = {
            "rm_decisions": _JsonlRotator(self._jsonl_dir, f"{stream_prefix}-rm_decisions",
                                          max_bytes=self._jsonl_max_bytes, max_age_s=self._jsonl_max_age_s),
            "engine_submits": _JsonlRotator(self._jsonl_dir, f"{stream_prefix}-engine_submits",
                                            max_bytes=self._jsonl_max_bytes, max_age_s=self._jsonl_max_age_s),
            "engine_acks": _JsonlRotator(self._jsonl_dir, f"{stream_prefix}-engine_acks",
                                         max_bytes=self._jsonl_max_bytes, max_age_s=self._jsonl_max_age_s),
            "fills_normalized": _JsonlRotator(self._jsonl_dir, f"{stream_prefix}-fills_normalized",
                                              max_bytes=self._jsonl_max_bytes, max_age_s=self._jsonl_max_age_s),
            "privatews_events": _JsonlRotator(self._jsonl_dir, f"{stream_prefix}-privatews_events",
                                              max_bytes=self._jsonl_max_bytes, max_age_s=self._jsonl_max_age_s),
            # MM streams
            "mm_quotes": _JsonlRotator(self._jsonl_dir, f"{stream_prefix}-mm_quotes",
                                       max_bytes=self._jsonl_max_bytes, max_age_s=self._jsonl_max_age_s),
            "mm_cancels": _JsonlRotator(self._jsonl_dir, f"{stream_prefix}-mm_cancels",
                                        max_bytes=self._jsonl_max_bytes, max_age_s=self._jsonl_max_age_s),
            "mm_hedges": _JsonlRotator(self._jsonl_dir, f"{stream_prefix}-mm_hedges",
                                       max_bytes=self._jsonl_max_bytes, max_age_s=self._jsonl_max_age_s),
        }

        # -------- Index SQLite (ts_ns, type, id, path) ------------
        self._index_ready = False  # créé à la 1re écriture

        # -------- Contexte runtime / lifecycle --------------------
        self._started = False
        self._rotation_task = None  # type: asyncio.Task | None
        # rotate_every_s > tracker_config.rotation_interval > fallback 3600s
        _tracker_rot = getattr(getattr(self._tracker, "cfg", object()), "rotation_interval", 3600.0)
        self._rotate_interval = float(rotate_every_s if rotate_every_s is not None else _tracker_rot)

        # -------- Backpressure & priorité de streams --------------
        self._stream_priority = [
            "fills_normalized",   # P0 (critique: jamais droppé)
            "engine_acks",        # P1
            "engine_submits",     # P2
            "mm_hedges",          # P3
            "rm_decisions",       # P4
            "mm_quotes",          # P5 (verbeux)
            "mm_cancels",         # P6 (verbeux)
            "privatews_events",   # P7 (massif)
        ]
        self._stream_critical = {"fills_normalized": True}

        # Files bornées par stream + suivi plateau
        self._q_streams = {name: asyncio.Queue(maxsize=self._q_stream_max) for name in self._jsonl.keys()}
        self._plateau_since = {name: 0.0 for name in self._jsonl.keys()}
        self._streams_task = None

        # (Compat) structures simples héritées
        self._queue = []               # compat héritée (non utilisée par le pipeline JSONL)
        self._last_flush_ms = 0

        # Proxy metrics d’un sous-module vers LHM (si dispo)
        try:
            self._trade_logger._metrics_proxy = self  # noqa: attr-defined
        except Exception:
            pass

        # -------- Watchdog runtime --------------------------------
        self._wd_task = None
        self._queue_sizes: list[int] = []
        self._last_write_monotonic = None
        self._last_rotation_epoch = 0.0
        # -------- Flags santé pipeline PnL (M5-B4) ----------------
        # True si au moins un événement PnL-critique (trade / balance / décision RM…)
        # a été définitivement perdu dans la chaîne JSONL.
        self._critical_drop_seen = False

        # True si une erreur d'I/O (DB ou JSONL) a été observée.
        # Sert à marquer la journée comme "non audit-grade" côté PnL.
        self._storage_error_seen = False
        # Timestamps des derniers incidents détectés par le watchdog.
        # Utilisés pour enrichir le statut pnl_pipeline (OK/WARN/CRIT).
        self._last_queue_plateau_ts: Optional[float] = None
        self._last_db_stall_ts: Optional[float] = None



        # -------- Métriques (init à 0) ----------------------------
        try:
            # calcolata PRIMA di toccare le metriche (già fatto sopra ma ribadiamo il valore safe)
            self._jsonl_queue_cap = _safe_int(getattr(self.cfg, "LHM_JSONL_QUEUE_CAP", 5000), 5000)

            # helper compat che aggiorna size+cap della coda JSONL
            from modules.obs_metrics import set_lhm_queue, LOGGERH_DB_FILE_BYTES, LOGGERH_LAST_FLUSH_TS_SECONDS, \
                LOGGERH_LAST_ROTATION_TS_SECONDS
            set_lhm_queue(0, self._jsonl_queue_cap)  # size=0 + cap
            LOGGERH_DB_FILE_BYTES.set(0)  # gauge SENZA label
            LOGGERH_LAST_FLUSH_TS_SECONDS.set(0)  # gauge SENZA label
            LOGGERH_LAST_ROTATION_TS_SECONDS.set(0)  # gauge SENZA label
        except Exception:
            # no-op: nessun ricast pericoloso qui
            pass

            # Forensic chain
            self._forensic_head = ""  # head hex courante
            self._jsonl_path = Path(getattr(self, "jsonl_path", "logs/history.jsonl"))


    # ---------------- lifecycle ----------------
    # logger_historique_manager.py — dans class LoggerHistoriqueManager

    async def start(self) -> None:
        """
        Démarre le pipeline LHM : BaseTradeLogger (thread) + worker JSONL + garde-fous.
        Idempotent.
        """
        if getattr(self, "_running", False):
            return
        self._running = True
        self._flush_lock = getattr(self, "_flush_lock", asyncio.Lock())
        # 1) BaseTradeLogger -> event_sink (nous)
        btl = getattr(self, "_trade_logger", None)
        if btl:
            btl.set_event_sink(self._on_event_sink)  # (log_type, entries)
            await btl.start()
        # 2) Worker JSONL (best-effort si configuré)
        self._init_jsonl_streams()

        await self._spawn_streams_worker()
        # 3) Statut / métriques (optionnelles)
        try:
            from modules.obs_metrics import LOGGERH_LAST_FLUSH_TS_SECONDS
            LOGGERH_LAST_FLUSH_TS_SECONDS.set(int(time.time()))
        except Exception:
            pass

    async def stop(self) -> None:
        """
        Arrêt propre : flush + stop BTL + stop worker JSONL.
        Idempotent.
        """
        if not getattr(self, "_running", False):
            return
        self._running = False
        # 1) Flush fort (DB + JSONL)
        try:
            await self.flush_now(rotate=True)
        except Exception:
            logger.exception("flush_now at stop failed")
        # 2) Arrêt du BTL
        btl = getattr(self, "_trade_logger", None)
        if btl:
            try:
                await btl.stop()
            except Exception:
                logger.exception("BaseTradeLogger.stop failed")
        # 3) Arrêt worker JSONL
        t = getattr(self, "_streams_task", None)
        if t and not t.done():
            t.cancel()
            try:
                await t
            except Exception:
                pass
        self._streams_task = None

    def _init_jsonl_streams(self, *, base_dir: str | None = None, max_bytes: int = 50 * (1 << 20),
                            max_age_s: int = 3600) -> None:
        """
        Initialise deux streams JSONL: 'events' et 'errors' avec leurs queues.
        Idempotent.
        """
        if getattr(self, "_jsonl_streams", None):
            return
        import asyncio, queue
        base_dir = base_dir or os.path.join(os.getcwd(), "logs", "jsonl")
        os.makedirs(base_dir, exist_ok=True)
        self._jsonl_streams = {}
        for stream in ("events", "errors"):
            q: asyncio.Queue = asyncio.Queue(maxsize=10000)
            rot = _JsonlRotator(os.path.join(base_dir, stream), stream, max_bytes=max_bytes, max_age_s=max_age_s)
            self._jsonl_streams[stream] = (q, rot)

    def forensic_verify(self, limit_lines: int | None = None) -> bool:
        """
        Recalcule la rolling-hash depuis le début du JSONL et compare à self._forensic_head.
        - limit_lines: bornage optionnel (None = tout)
        """
        head = b""
        count = 0
        try:
            with self._jsonl_path.open("r", encoding="utf-8") as f:
                for line in f:
                    head = hashlib.sha256(head + line.encode("utf-8")).digest()
                    count += 1
                    if limit_lines and count >= int(limit_lines):
                        break
            recomputed = head.hex()
            ok = (not self._forensic_head) or (recomputed == self._forensic_head)
            if ok:
                forensic_verify_ok_total.inc()
            return ok
        except FileNotFoundError:
            # Pas encore de fichier: considéré OK
            forensic_verify_ok_total.inc()
            return True
        except Exception:
            return False

    async def _spawn_streams_worker(self) -> None:
        """
        Lance un worker unique qui draine les streams JSONL avec priorité.
        Safe si aucune stream n’est configurée.
        """
        if getattr(self, "_streams_task", None):
            return

        async def _runner():
            # ordre de priorité : events > errors (modifiable)
            prio = ["events", "errors"]
            while self._running:
                try:
                    drained = await self._drain_stream_once(prio)
                    if not drained:
                        await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    break
                except Exception:
                    logger.exception("streams worker tick failed")
                    await asyncio.sleep(0.05)

        self._streams_task = asyncio.create_task(_runner(), name="LHM-StreamsWorker")

    async def _drain_stream_once(self, order: list[str]) -> bool:
        """
        Draine chaque stream JSONL au plus N items par tick (backpressure).
        Retourne True si au moins un élément a été écrit.
        """
        streams = getattr(self, "_jsonl_streams", {}) or {}
        if not streams:
            return False
        wrote_any = False
        per_tick_budget = 500
        for name in order:
            q_rot = streams.get(name)
            if not q_rot:
                continue
            q, rot = q_rot
            n = 0
            while n < per_tick_budget:
                try:
                    item = q.get_nowait()
                except Exception:
                    break
                try:
                    line = (json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")

                    # Écriture via l’API du rotateur (thread-safe)
                    await asyncio.to_thread(rot.write_line, line)

                    wrote_any = True
                    n += 1

                    # === OBS (on conserve tes compteurs) ===
                    try:
                        from modules.obs_metrics import LHM_JSONL_INGESTED_TOTAL, LHM_JSONL_QUEUE_SIZE
                        LHM_JSONL_INGESTED_TOTAL.labels(stream=name).inc()
                        LHM_JSONL_QUEUE_SIZE.labels(stream=name).set(q.qsize())
                    except Exception:
                        pass


                except Exception:

                    # Drop contrôlé si l’écriture échoue (I/O JSONL)

                    try:

                        from modules.obs_metrics import LHM_JSONL_DROPPED_TOTAL

                        LHM_JSONL_DROPPED_TOTAL.labels(stream=name).inc()

                    except Exception:

                        pass

                    # Erreur de stockage → pipeline PnL potentiellement non audit-grade

                    try:

                        self._storage_error_seen = True

                    except Exception:

                        pass

            # Rotation par âge testée à chaque tick (et comptabilisée)
            try:
                rotated = await asyncio.to_thread(rot.rotate_if_needed)
                if rotated:
                    try:
                        from modules.obs_metrics import LOGGERH_JSONL_ROTATIONS_TOTAL
                        LOGGERH_JSONL_ROTATIONS_TOTAL.labels(stream=name).inc()
                    except Exception:
                        pass
            except Exception:
                logger.exception("jsonl age-rotation failed for %s", name)
                try:
                    self._storage_error_seen = True
                except Exception:
                    pass

        return wrote_any

    def _on_schema_violation(self, field: str) -> None:
        try:
            schema_violation_total.labels(field=str(field)).inc()
        except Exception:
            pass

    def _on_log_dedup(self, kind: str = "jsonl") -> None:
        try:
            log_dedup_total.labels(kind=kind).inc()
        except Exception:
            pass

    def _on_log_replay(self, kind: str = "jsonl") -> None:
        try:
            log_replay_total.labels(kind=kind).inc()
        except Exception:
            pass

    def _derive_time_fields(self, ts_ms_or_iso, pod_region: str) -> dict[str, int | str]:
        """
        Dérive hour_local/hour_utc/weekday_* et local_day (helper interne),
        en s'alignant sur les TZ utilisées par LogWriter.
        """
        from datetime import datetime, timezone
        try:
            from zoneinfo import ZoneInfo
        except Exception:
            ZoneInfo = None
        _REGION_TZ = {"EU": "Europe/Rome", "US": "America/New_York"}
        if isinstance(ts_ms_or_iso, (int, float)):
            dt_utc = datetime.fromtimestamp(float(ts_ms_or_iso) / 1000.0, tz=timezone.utc)
        elif isinstance(ts_ms_or_iso, str):
            try:
                dt_utc = datetime.fromisoformat(ts_ms_or_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                dt_utc = datetime.now(timezone.utc)
        else:
            dt_utc = datetime.now(timezone.utc)
        tz = _REGION_TZ.get(str(pod_region or "EU").upper(), "Europe/Rome")
        if ZoneInfo:
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

    def _fallback_net_profit_sign(self, e: dict) -> None:
        """
        [M5-A3-6/7] Règle de secours « signe seulement » + flags qualité.

        - Ne touche jamais e["net_profit"].
        - Pose e["net_profit_sign"] si on peut inférer un signe.
        - Ajoute :
            * e["pnl_derived_from_bps"] = True si le signe vient des bps,
            * e["pnl_missing"] = True si rien n'est inférable.
        - Compte les métriques DERIVED_NET_PROFIT_SIGN_TOTAL / MISSING_NET_PROFIT_TOTAL
          côté LHM uniquement (source de vérité PnL).
        """
        # Initialise les flags si absents (permet aussi de tagger les anciens enregistrements)
        if "pnl_derived_from_bps" not in e:
            e["pnl_derived_from_bps"] = False
        if "pnl_missing" not in e:
            e["pnl_missing"] = False

        # Si un signe explicite est déjà fourni, on ne touche à rien
        cur_sign = e.get("net_profit_sign")
        if cur_sign in (1, -1):
            # On s'assure juste que les flags reflètent un PnL non manquant
            e["pnl_missing"] = bool(e.get("pnl_missing", False)) and False
            return

        try:
            # 1) Source principale : net_profit
            np = e.get("net_profit")
            if isinstance(np, (int, float)) and np != 0:
                e["net_profit_sign"] = 1 if np > 0 else -1
                e["pnl_derived_from_bps"] = False
                e["pnl_missing"] = False
                return

            # 2) Fallback bps : net_bps / spread_net_final_bps / spread_net_final
            nbps = (
                    e.get("net_bps")
                    or e.get("spread_net_final_bps")
                    or e.get("spread_net_final")
            )
            if isinstance(nbps, (int, float)) and nbps != 0:
                e["net_profit_sign"] = 1 if nbps > 0 else -1
                e["pnl_derived_from_bps"] = True
                e["pnl_missing"] = False
                try:
                    from modules.obs_metrics import DERIVED_NET_PROFIT_SIGN_TOTAL
                    DERIVED_NET_PROFIT_SIGN_TOTAL.labels(
                        module="LHM",
                        branch=str(e.get("trade_mode") or ""),
                    ).inc()
                except Exception:
                    pass
                return

            # 3) Rien d'inférable → PnL manquant
            e["pnl_missing"] = True
            try:
                from modules.obs_metrics import MISSING_NET_PROFIT_TOTAL
                MISSING_NET_PROFIT_TOTAL.labels(
                    module="LHM",
                    branch=str(e.get("trade_mode") or ""),
                ).inc()
            except Exception:
                pass
        except Exception:
            # Jamais bloquant
            pass


    def _canonicalize(self, e: dict) -> dict:
        """
        Petites normalisations communes (exchanges, pair, champs clés).
        """

        def _norm_ex(x):
            if not x: return None
            s = str(x).strip().replace("-", "").replace("_", "").upper()
            if s.startswith("BINANCE"): return "BINANCE"
            if s.startswith("BYBIT"): return "BYBIT"
            if s.startswith("COINBASE") or s in {"CB", "COINBASEAT", "COINBASEADVANCEDTRADE"}: return "COINBASE"
            return s

        e["buy_ex"] = _norm_ex(e.get("buy_ex"))
        e["sell_ex"] = _norm_ex(e.get("sell_ex"))
        if "pair_key" in e and isinstance(e["pair_key"], str):
            e["pair_key"] = e["pair_key"].replace("-", "").upper()
        if "symbol" in e and isinstance(e["symbol"], str):
            e["symbol"] = e["symbol"].replace("_", "-").upper()
        return e

    def _validate_trade_schema(self, e: dict) -> None:
        """
        [M5-A3-1] Validation NON BLOQUANTE du contrat minimal pour les trades.

        - Marque e["_schema_invalid"] = True si des champs critiques manquent.
        - Expose la liste dans e["_schema_missing_fields"] pour audit / SQL.
        - Remonte une métrique schema_violation_total par champ manquant.

        On NE jette PAS le trade : il est loggué, mais flaggé comme incomplet
        pour la compta PnL stricte / les dashboards de qualité.
        """
        try:
            required_keys = [
                # Identité marché & mode
                "asset",                 # paire canonique (ex: BTCUSDC)
                "trade_mode",            # TT / TM / MM / REB
                "route",                 # libellé route BUY:...→SELL:...

                # Journal unifié par bundle
                "bundle_id",             # agrégat logique du trade

                # Notional pivot (aujourd'hui USDC)
                "executed_volume_usdc",  # notional en quote pivot

                # Contexte wallet / desk
                "account_alias",         # alias de sous-compte / wallet
                "deployment_mode",       # EU_ONLY / SPLIT / ...
                "pod_region",            # EU / US
                "capital_profile",       # NANO / MICRO / SMALL / ...

                # On pourrait ajouter plus tard: pacer_mode, branch, etc.
            ]

            missing: list[str] = []

            for key in required_keys:
                val = e.get(key)
                if val is None:
                    missing.append(key)
                elif isinstance(val, str) and not val.strip():
                    missing.append(key)

            # Vérification spécifique sur le notional (doit être > 0)
            try:
                notional_raw = e.get("executed_volume_usdc")
                notional = float(notional_raw) if notional_raw is not None else None
                if notional is None or notional <= 0:
                    if "executed_volume_usdc" not in missing:
                        missing.append("executed_volume_usdc")
            except Exception:
                if "executed_volume_usdc" not in missing:
                    missing.append("executed_volume_usdc")

            if missing:
                # Flag explicite pour consommation downstream (PnL strict, dashboards qualité)
                e["_schema_invalid"] = True
                e["_schema_missing_fields"] = missing

                # métriques par champ manquant (pour suivre la dérive)
                for field in missing:
                    self._on_schema_violation(field)
            else:
                # On rend l'état explicite pour requêtes SQL/analyses
                e.setdefault("_schema_invalid", False)

        except Exception:
            # Jamais bloquant : on ne casse JAMAIS la pipeline si la validation se plante
            pass

    def _normalize_event_contract(self, log_type: str, e: dict) -> dict:
        """
        [M5-B1-1] Contrat minimal d'événement pour le pipeline LHM (non bloquant).

        Objectif :
        - Donner à tous les events PnL-critiques quelques pivots communs :
          * ts_ms  : timestamp ms (fallback sur now si absent)
          * event_type : type canonique d'event (trade / reject / rm.decision / ...)
          * log_type / stream : nom de stream logique (trades / opportunities / alerts / ...)

        On NE jette jamais l'event ; en cas de problème, on renvoie l'event tel quel.
        """
        try:
            obj = dict(e or {})

            # --- ts_ms pivot ---
            ts = (
                obj.get("ts_ms")
                or obj.get("ts")
                or obj.get("timestamp")
            )
            if isinstance(ts, (int, float)):
                obj.setdefault("ts_ms", int(ts))
            elif ts is None:
                now_ms = int(time.time() * 1000)
                obj.setdefault("ts_ms", now_ms)
                ts = now_ms
            # si ts est une string ISO, on laisse _derive_time_fields la gérer

            # --- event_type canonique ---
            event_type = (
                obj.get("event_type")
                or obj.get("kind")
                or obj.get("_kind")
                or obj.get("_hist_kind")
                or log_type
            )
            obj.setdefault("event_type", str(event_type))

            # --- stream / log_type pivots (pour forensic / requêtes génériques) ---
            obj.setdefault("log_type", str(log_type))
            obj.setdefault("stream", str(log_type))

            return obj
        except Exception:
            # On ne casse jamais la pipeline : si la normalisation échoue, on renvoie l'event brut
            return e



    def _flush_jsonl_batch(self, batch: list[str], stream: str = "trades") -> None:
        """
        Flush d'un batch JSONL:
        - observe la latence d'écriture (ms)
        - met à jour la taille de file et le batch courant
        - incrémente les compteurs ingested/dropped quand approprié
        - met à jour la forensic chain head
        """
        if not batch:
            return

        # Panneau "batch courant"
        set_flush_batch(len(batch))

        t0 = time.perf_counter()
        # Ecriture append-only
        self._jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        with self._jsonl_path.open("a", encoding="utf-8") as f:
            for line in batch:
                f.write(line)
                # Maintien de la forensic chain (rolling hash: Hn = sha256(Hn-1 | line))
                try:
                    prev = bytes.fromhex(self._forensic_head) if self._forensic_head else b""
                    self._forensic_head = hashlib.sha256(prev + line.encode("utf-8")).hexdigest()
                except Exception:
                    # On ignore les erreurs ponctuelles de hash — pas bloquant
                    pass

        dt_ms = (time.perf_counter() - t0) * 1000.0
        try:
            loggerh_write_ms.observe(dt_ms)
        except Exception:
            pass

        # Taille de file après flush
        try:
            lhm_jsonl_queue_size.set(len(self._queue))
        except Exception:
            pass

        # Ingestion: on crédite le stream (par défaut "trades")
        try:
            lhm_jsonl_ingested_total.labels(stream=stream).inc(len(batch))
        except Exception:
            pass

        # Publie la nouvelle tête de forensic chain (projection numérique)
        set_forensic_head_numeric(self._forensic_head)

    def _enqueue_jsonl(self, stream: str, payload: dict) -> None:
        """
        Enqueue best-effort dans une stream JSONL si configurée.

        [M5-B4-1] Politique de drop :
        - toutes les écritures JSONL restent best-effort,
        - mais on marque explicitement les drops d'événements PnL-critiques
          (trades/balances/décisions RM…) pour que le pipeline PnL puisse
          se mettre en mode "à risque".
        """
        streams = getattr(self, "_jsonl_streams", {}) or {}
        q_rot = streams.get(stream)
        if not q_rot:
            return
        q, _rot = q_rot

        # Détection d'event PnL-critique en fonction du contenu (best-effort)
        try:
            log_type = str(payload.get("log_type") or "").lower()
            inner = payload.get("payload") or {}
            kind = str(inner.get("event_type") or inner.get("kind") or "").lower()
            is_critical = (
                    log_type in ("trades", "balances", "balance_snapshots")
                    or kind.startswith("rm.")
                    or kind.startswith("engine.")
                    or kind.startswith("balance.")
            )
        except Exception:
            is_critical = False

        try:
            q.put_nowait(payload)
        except Exception:
            # File pleine → on signale le drop, et si c’était critique on marque le flag.
            try:
                from modules.obs_metrics import LHM_JSONL_DROPPED_TOTAL
                LHM_JSONL_DROPPED_TOTAL.labels(stream=stream).inc()
            except Exception:
                pass
            if is_critical:
                try:
                    self._critical_drop_seen = True
                except Exception:
                    # on n'expose pas cette erreur, c'est un flag interne
                    pass


    async def _on_event_sink(self, log_type: str, entries: list[dict]) -> None:
        """
        Point d’entrée unique du BTL (batch). Ordre strict :
          enrichissements → fallback sign → écriture DB → tracker (+ JSONL en parallèle).
        """
        if not entries:
            return
        # enrichissements légers (in-place)
        enriched: list[dict] = []
        for e in entries:
            e = dict(e or {})

            # [M5-A3] tags de contexte globaux (deployment_mode/pod_region/capital_profile/pacer_mode)
            e = self._with_context_tags(e)

            # [M5-B1-1] Normalisation du contrat d'événement (ts_ms / event_type / stream/log_type)
            e = self._normalize_event_contract(log_type, e)

            pod = (e.get("pod_region") or "EU")
            ts = (
                    e.get("ts_ms")
                    or e.get("ts")
                    or e.get("timestamp")  # iso-8601 éventuel (BaseTradeLogger ou Engine)
                    or int(time.time() * 1000)
            )

            # Horaires (UTC/local) + local_day (en s'appuyant sur pod_region)
            e.update(self._derive_time_fields(ts, pod))

            # Canonicalisation légère (exchanges, pair, symbol, etc.)
            e = self._canonicalize(e)

            # [M5-A3-1] Contrat minimal TRADES (best-effort, non bloquant)
            if log_type == "trades":
                self._validate_trade_schema(e)

            # Fallback signe (sans inventer de montant)
            self._fallback_net_profit_sign(e)

            enriched.append(e)

            # JSONL parallèle “events” (inchangé : on loggue ce qui part en DB)
            self._enqueue_jsonl(
                "events",
                {
                    "stream": "events",
                    "log_type": log_type,
                    "payload": e,
                    "ingested_ms": int(time.time() * 1000),
                },
            )

        # Écriture DB (thread → pas de blocage loop)
        try:
            t0 = time.time()
            record_latency = True
            if log_type == "trades":
                await self._write_trades_async(enriched)
                record_latency = False  # déjà observé dans _write_trades_async
            elif log_type == "opportunities":
                await asyncio.to_thread(self._writer.insert_opportunities_bulk, enriched)
            elif log_type == "alerts":
                await asyncio.to_thread(self._writer.insert_alerts_bulk, enriched)
            else:
                # best-effort: on tente une table générique si elle existe
                await asyncio.to_thread(self._writer.insert_generic, log_type, enriched)
            if record_latency:
                try:
                    from modules.obs_metrics import LOGGERH_WRITE_MS
                    LOGGERH_WRITE_MS.observe((time.time() - t0) * 1000.0)
                except Exception:
                    pass

        except Exception:
            logger.exception("DB write failed for log_type=%s", log_type)
            # Erreur DB = I/O critique côté PnL (on ne sait pas si la ligne a été persistée)
            try:
                self._storage_error_seen = True
            except Exception:
                pass
            self._enqueue_jsonl(
                "errors",
                {
                    "stream": "errors",
                    "log_type": log_type,
                    "payload": enriched,
                    "err": "db_write_failed",
                },
            )
        # Ingestion tracker (best-effort, non bloquant)
        try:
            if log_type == "trades":
                self._tracker.ingest_trades(enriched)
            elif log_type == "opportunities":
                self._tracker.ingest_opportunities(enriched)
        except Exception:
            logger.exception("PairHistoryTracker ingestion failed for %s", log_type)

    async def record_latency(self, metrics: dict[str, any]) -> None:
        """
        Persiste un évènement de latence dans la table `latency` et pousse (optionnel) des métriques.
        (Architecture: métriques centralisées ici, pas dans les sous-modules.)
        """
        try:
            await asyncio.to_thread(self._writer.insert_latency, metrics)
        except Exception:
            logger.exception("record_latency failed (db)")
        # Prometheus optionnel (centralisé)
        try:
            from modules.obs_metrics import LAT_EVENTS_TOTAL
            route = str(metrics.get("route") or "NA")
            buy_ex = str(metrics.get("buy_ex") or "NA")
            sell_ex = str(metrics.get("sell_ex") or "NA")
            status = str(metrics.get("status") or "NA")
            LAT_EVENTS_TOTAL.labels(route=route, buy_ex=buy_ex, sell_ex=sell_ex, status=status).inc()
        except Exception:
            logger.exception("record_latency: prometheus emit failed")

    async def restart(self) -> None:
        await self.stop()
        await self.start()

    # logger_historique_manager.py — dans class LoggerHistoriqueManager

    async def run_eod_reconciliation(self, *, sign_priv_pem_path: str | None = None) -> dict:
        """
        Fin de journée: flush + finalize DB (checkpoint+vacuum+hash+signature optionnelle),
        et trace un marqueur JSONL 'eod' (best effort).
        """
        # Flush fort
        try:
            await self.flush_now(rotate=True)
        except Exception:
            logger.exception("EOD: flush_now failed")

        # Finalize DB (writer)
        try:
            meta = await asyncio.to_thread(self._writer.finalize_day, sign_priv_pem_path=sign_priv_pem_path)
        except Exception:
            logger.exception("EOD: finalize_day failed")
            meta = {"error": "finalize_failed"}


        # Marqueur JSONL
        try:
            self._enqueue_jsonl("events", {
                "stream": "events",
                "type": "eod",
                "meta": meta,
                "ts": int(time.time() * 1000),
            })
        except Exception:
            pass
        # Trace un marqueur JSONL EOD (best effort, non bloquant) — additionnel
        meta2 = {"pod_region": self._pod_region, "ts": int(time.time() * 1000)}
        try:
            self._enqueue_jsonl("events", {"kind": "eod", "meta": meta2})
        except Exception:
            logger.exception("EOD: enqueue jsonl failed")

        return meta

    async def replay_from_logs(self, *, day: str | None = None, kind: str = "trades", limit: int = 1000) -> list[dict]:
        """
        Relit rapidement la DB (jour optionnel) pour debug/QA.
        (Encapsulation: on passe par LogWriter, pas d'accès DB ailleurs.)
        """
        try:
            return await asyncio.to_thread(self._writer.fetch_records, kind, day, limit)
        except Exception:
            logger.exception("replay_from_logs failed")
            return []

    async def get_funnel(self, *, since_ms: int | None = None, until_ms: int | None = None) -> dict:
        """
        Petits agrégats “entonnoir” : opportunities → admitted → executed (win/loss) avec latences.
        S’adosse aux tables `opportunities`, `trades`, `latency`.
        """
        return await asyncio.to_thread(self._writer.compute_funnel, since_ms=since_ms, until_ms=until_ms)

    async def get_pnl_pipeline_status(self, *, window_s: int = 86400) -> Dict[str, Any]:
        """
        Synthèse santé + PnL du pipeline LHM/PnL sur une fenêtre glissante (par défaut 24h).

        S'appuie sur:
        - LogWriter.compute_pnl_summary (PnL "comptable"),
        - get_funnel (entonnoir opportunities→trades),
        - PairHistoryTracker.get_status_enriched (qualité PnL),
        - les flags internes du watchdog (queue/stall/stockage).
        """
        now_ts = time.time()
        now_ms = int(now_ts * 1000)

        if not window_s or window_s <= 0:
            window_s = 86400
        since_ms = now_ms - int(window_s * 1000)
        until_ms = now_ms

        # 1) PnL "comptable" via DB
        try:
            pnl_summary = await asyncio.to_thread(
                self._writer.compute_pnl_summary,
                since_ms=since_ms,
                until_ms=until_ms,
            )
        except Exception:
            logging.exception("get_pnl_pipeline_status: compute_pnl_summary failed")
            pnl_summary = {
                "window": {"since_ms": since_ms, "until_ms": until_ms},
                "pnl": {},
            }

        window_block = pnl_summary.get("window") or {"since_ms": since_ms, "until_ms": until_ms}
        pnl_block = pnl_summary.get("pnl") or {}

        # 2) Funnel opportunities → trades
        try:
            funnel = await self.get_funnel(since_ms=since_ms, until_ms=until_ms)
        except Exception:
            logging.exception("get_pnl_pipeline_status: get_funnel failed")
            funnel = {}

        # 3) Qualité PnL via PairHistoryTracker
        wr_ignored = 0
        try:
            tracker_status = getattr(self._tracker, "get_status_enriched", self._tracker.get_status)() or {}
            wr_ignored = int(tracker_status.get("wr_ignored_trades_total", 0) or 0)
        except Exception:
            wr_ignored = 0

        # 4) Flags PnL pipeline
        try:
            flags = dict(self.get_pnl_pipeline_flags() or {})
        except Exception:
            flags = {}
        critical_drop = bool(flags.get("critical_drop_seen"))
        storage_error = bool(flags.get("storage_error_seen"))
        storage_alert_level = getattr(self, "_last_storage_alert_level", None)

        # 5) Runtime (queue + incidents récents)
        queue_size = 0
        try:
            queue_size = int(getattr(self._trade_logger, "get_queue_size", lambda: 0)() or 0)
        except Exception:
            queue_size = 0

        last_queue_plateau_ts = getattr(self, "_last_queue_plateau_ts", None)
        last_db_stall_ts = getattr(self, "_last_db_stall_ts", None)

        # 6) Statut DB
        try:
            writer_status = getattr(self._writer, "get_status", lambda: {})() or {}
        except Exception:
            writer_status = {}
        db_path = writer_status.get("db_path") or self._writer.get_db_path()
        db_file_bytes = writer_status.get("db_file_bytes")
        db_mtime = writer_status.get("db_mtime")

        # 7) Décision niveau OK/WARN/CRIT
        level = "OK"
        reasons: list[str] = []

        if critical_drop:
            level = "CRIT"
            reasons.append("critical_drop_seen")
        if storage_error:
            if level != "CRIT":
                level = "CRIT"
            reasons.append("storage_error_seen")
        if storage_alert_level == "ERROR":
            if level != "CRIT":
                level = "CRIT"
            reasons.append("storage_alert_level_ERROR")

        if level != "CRIT":
            now_ts2 = time.time()
            recent_plateau = (
                last_queue_plateau_ts is not None
                and (now_ts2 - last_queue_plateau_ts) <= 10 * self._wd_interval_s
            )
            recent_stall = (
                last_db_stall_ts is not None
                and (now_ts2 - last_db_stall_ts) <= 10 * self._wd_interval_s
            )

            if storage_alert_level == "WARN":
                level = "WARN"
                reasons.append("storage_alert_level_WARN")
            if recent_plateau:
                if level == "OK":
                    level = "WARN"
                reasons.append("recent_queue_plateau")
            if recent_stall:
                if level == "OK":
                    level = "WARN"
                reasons.append("recent_db_stall")

        healthy = (level == "OK")

        # 8) Normalisation du bloc PnL
        pnl_out = {
            "net_profit": float(pnl_block.get("net_profit") or 0.0),
            "fees": float(pnl_block.get("fees") or 0.0),
            "trades": int(pnl_block.get("trades") or 0),
            "wins": int(pnl_block.get("wins") or 0),
            "losses": int(pnl_block.get("losses") or 0),
            "pnl_missing": int(pnl_block.get("pnl_missing") or 0),
            "derived_from_bps": int(pnl_block.get("derived_from_bps") or 0),
        }

        return {
            "component": "pnl_pipeline",
            "healthy": bool(healthy),
            "level": level,
            "reasons": reasons,
            "window": window_block,
            "flags": {
                "critical_drop_seen": critical_drop,
                "storage_error_seen": storage_error,
                "storage_alert_level": storage_alert_level,
            },
            "runtime": {
                "queue_size": queue_size,
                "last_queue_plateau_ts": last_queue_plateau_ts,
                "last_db_stall_ts": last_db_stall_ts,
            },
            "pnl": pnl_out,
            "funnel": funnel,
            "quality": {
                "wr_ignored_trades_total": wr_ignored,
            },
            "db": {
                "db_path": db_path,
                "db_file_bytes": db_file_bytes,
                "db_mtime": db_mtime,
            },
        }

    async def get_pnl_multiview(
        self,
        *,
        window_s: int = 86400,
        now_ms: int | None = None,
        include_flags: bool = True,
    ) -> dict:
        """
        Vue PnL multi-dimensions (M5-C v2-B) sur une fenêtre glissante.

        - window_s  : durée de la fenêtre en secondes (défaut: 24h glissante).
        - now_ms    : timestamp de référence en ms (epoch). Si None, utilise
                      l'heure courante.
        - include_flags : si True, ajoute un bloc "pipeline_flags" basé sur
                          get_pnl_pipeline_flags().

        Comportement:
        - Convertit (window_s, now_ms) en [since_ms, until_ms].
        - Délègue le calcul des agrégats PnL à LogWriter.compute_pnl_views(...)
          via asyncio.to_thread (aucune I/O bloquante dans la boucle async).
        - Retourne la structure "pnl_multiview" produite par LogWriter, avec
          éventuellement un petit bloc "pipeline_flags" ajoutée pour donner un
          aperçu de la santé du pipeline LHM/PnL.
        """
        import time

        if now_ms is None:
            now_ms = int(time.time() * 1000)
        try:
            since_ms = int(now_ms) - int(window_s) * 1000
        except Exception:
            # fallback minimal si window_s est pathologique
            since_ms = int(now_ms) - 86400 * 1000

        until_ms = int(now_ms)

        # Appel best-effort à compute_pnl_views côté LogWriter
        try:
            views = await asyncio.to_thread(
                self._writer.compute_pnl_views,
                since_ms=since_ms,
                until_ms=until_ms,
            )
        except Exception:
            logger.exception("get_pnl_multiview: compute_pnl_views failed")
            # Fallback minimal en cas d'erreur : squelette cohérent
            views = {
                "component": "pnl_multiview",
                "base_currency": "USDC",
                "window": {"since_ms": since_ms, "until_ms": until_ms},
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

        if not isinstance(views, dict):
            # Normalisation best-effort si jamais compute_pnl_views retourne
            # quelque chose d'inhabituel
            try:
                views = dict(views)  # type: ignore[arg-type]
            except Exception:
                views = {
                    "component": "pnl_multiview",
                    "base_currency": "USDC",
                    "window": {"since_ms": since_ms, "until_ms": until_ms},
                    "as_of_ts": time.time(),
                }

        # Injection optionnelle des flags pipeline (santé LHM/PnL)
        if include_flags:
            try:
                flags = self.get_pnl_pipeline_flags()
            except Exception:
                logger.exception("get_pnl_multiview: get_pnl_pipeline_flags failed")
                flags = None

            if isinstance(flags, dict):
                # On évite d'écraser un bloc existant par accident
                existing = views.get("pipeline_flags")
                if isinstance(existing, dict):
                    existing.update(flags)
                else:
                    views["pipeline_flags"] = dict(flags)

        return views

    async def get_pnl_daily_multiview(
        self,
        *,
        local_day: str | None = None,
        pod_region: str | None = None,
        include_flags: bool = True,
    ) -> dict:
        """
        Vue PnL multi-dimensions pour une journée calendaire locale (M5-D-1).

        Cette API est le pendant "jour calendaire" de :meth:`get_pnl_multiview`:
        - la résolution [since_ms, until_ms] est déléguée à LogWriter via
          compute_pnl_views_for_local_day(local_day, pod_region),
        - le contrat de sortie "pnl_multiview" est identique à compute_pnl_views.

        Paramètres
        ----------
        local_day:
            Jour local au format ``'YYYY-MM-DD'`` dans la timezone associée à
            ``pod_region``. Si None, on laisse LogWriter choisir "aujourd'hui"
            dans la timezone du pod.
        pod_region:
            Région logique du pod ("EU", "US", ...). Si None, LogWriter
            appliquera son fallback (EU → Europe/Rome, etc.).
        include_flags:
            Si True, ajoute un bloc "pipeline_flags" avec get_pnl_pipeline_flags().

        Retour
        ------
        dict
            Structure "pnl_multiview" (pnl_global, pnl_by_exchange, etc.) sur
            la journée calendaire locale.
        """
        # Normalisation légère des paramètres
        local_day_arg = None
        if local_day is not None:
            local_day_arg = str(local_day).strip() or None

        pod_region_arg = None
        if pod_region is not None:
            pod_region_arg = str(pod_region).strip() or None

        # Appel best-effort à compute_pnl_views_for_local_day côté LogWriter
        views: dict | None = None
        try:
            if hasattr(self._writer, "compute_pnl_views_for_local_day"):
                views = await asyncio.to_thread(
                    self._writer.compute_pnl_views_for_local_day,
                    local_day=local_day_arg,
                    pod_region=pod_region_arg,
                )
            else:
                # Fallback: on revient à une fenêtre glissante 24h en direct DB
                import time as _time

                now_ms = int(_time.time() * 1000)
                since_ms = now_ms - 86400 * 1000
                views = await asyncio.to_thread(
                    self._writer.compute_pnl_views,
                    since_ms=since_ms,
                    until_ms=now_ms,
                )
        except Exception:
            logger.exception("get_pnl_daily_multiview: compute_pnl_views_for_local_day failed")
            views = None

        # Fallback minimal en cas d'erreur : squelette cohérent, même contrat
        if not isinstance(views, dict):
            import time as _time

            views = {
                "component": "pnl_multiview",
                "base_currency": "USDC",
                "window": {},
                "as_of_ts": _time.time(),
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

        # Injection optionnelle des flags pipeline (santé LHM/PnL)
        if include_flags:
            try:
                flags = self.get_pnl_pipeline_flags()
            except Exception:
                logger.exception("get_pnl_daily_multiview: get_pnl_pipeline_flags failed")
                flags = None

            if isinstance(flags, dict):
                existing = views.get("pipeline_flags")
                if isinstance(existing, dict):
                    existing.update(flags)
                else:
                    views["pipeline_flags"] = dict(flags)

        return views

    async def run_pnl_reconciliation_for_day(
        self,
        *,
        local_day: str | None = None,
        pod_region: str | None = None,
        fetch_pnl_cex_fn=None,
        dry_run: bool = False,
    ) -> dict:
        """
        Reconciliation PnL CEX ↔ DB pour une journée calendaire locale (M5-D-2).

        Cette API:
        - lit les agrégats PnL "jour" côté DB (LogWriter.compute_pnl_views*),
        - appelle un callback `fetch_pnl_cex_fn(exchange, account_alias, local_day, region)`
          pour reconstruire le PnL jour côté CEX,
        - compare les deux et classe chaque (exchange,account_alias) en OK/WARN/CRIT,
        - met à jour les métriques Prometheus pnl_reco_* (sauf en dry_run).

        Paramètres
        ----------
        local_day:
            Jour local au format 'YYYY-MM-DD' dans la timezone de `pod_region`.
            Si None, on laisse la méthode dériver "aujourd'hui" pour la région.
        pod_region:
            Région logique du pod ("EU", "US", ...). Si None, on utilise
            _pnl_reco_default_region (initialisé depuis cfg.g.pod_region).
        fetch_pnl_cex_fn:
            Callback sync ou async de signature:
                fn(exchange: str, account_alias: str, local_day: str, region: str) -> Mapping
            Il doit retourner au minimum:
                {
                    "net_profit_quote": float,
                    "fees_quote": float,
                    "turnover_quote": float,
                    "trades": int,
                }
        dry_run:
            Si True, aucune métrique Prometheus n'est poussée; seule la structure
            de résultat est calculée et renvoyée.

        Retour
        ------
        dict
            {
              "component": "pnl_reconciliation",
              "enabled": bool,
              "local_day": str,
              "region": str,
              "global_state": int (0/1/2),
              "rows": [
                 {
                   "exchange": str,
                   "account_alias": str,
                   "pnl_db": {...},
                   "pnl_cex": {...} | None,
                   "delta": {...},
                   "state": int,
                   "level": "OK"|"WARN"|"CRIT",
                   "reason": str,
                   "quality_db": {...},
                   "error_kind": str | None,
                 },
                 ...
              ],
            }
        """
        import time
        from datetime import datetime, timezone, timedelta
        try:
            from zoneinfo import ZoneInfo  # type: ignore
        except Exception:
            ZoneInfo = None  # type: ignore

        # 0) Guard: feature désactivée
        if not bool(getattr(self, "_pnl_reco_enabled", False)):
            return {
                "component": "pnl_reconciliation",
                "enabled": False,
                "reason": "disabled",
                "local_day": local_day,
                "region": pod_region,
                "rows": [],
                "global_state": None,
            }

        # 1) Résolution région + jour local
        region = str(
            pod_region
            or getattr(self, "_pnl_reco_default_region", None)
            or getattr(getattr(self, "cfg", object()), "pod_region", "EU")
        ).upper()

        # Détermination du jour local cible
        if local_day is not None and str(local_day).strip():
            local_day_str = str(local_day).strip()
        else:
            # "Aujourd'hui" dans la timezone de la région
            tz_name = {"EU": "Europe/Rome", "US": "America/New_York"}.get(region, "Europe/Rome")
            if ZoneInfo is not None:
                try:
                    dt_now_loc = datetime.now(ZoneInfo(tz_name))
                except Exception:
                    dt_now_loc = datetime.now()
            else:
                dt_now_loc = datetime.now()
            local_day_str = dt_now_loc.strftime("%Y-%m-%d")

        # Limite d'âge (lookback) best-effort
        max_lookback_days = int(getattr(self, "_pnl_reco_max_lookback_days", 0) or 0)
        if max_lookback_days > 0:
            try:
                tz_name = {"EU": "Europe/Rome", "US": "America/New_York"}.get(region, "Europe/Rome")
                if ZoneInfo is not None:
                    dt_today_loc = datetime.now(ZoneInfo(tz_name)).date()
                else:
                    dt_today_loc = datetime.utcnow().date()
                dt_target = datetime.strptime(local_day_str, "%Y-%m-%d").date()
                delta_days = (dt_today_loc - dt_target).days
                if delta_days < 0 or delta_days > max_lookback_days:
                    return {
                        "component": "pnl_reconciliation",
                        "enabled": True,
                        "skipped": True,
                        "reason": f"outside_lookback({delta_days}d)",
                        "local_day": local_day_str,
                        "region": region,
                        "rows": [],
                        "global_state": None,
                    }
            except Exception:
                # En cas de problème de parsing, on continue sans gate
                pass

        # 2) Récupération PnL DB (pnl_multiview jour)
        writer = getattr(self, "_writer", None)
        if writer is None:
            logger.error("run_pnl_reconciliation_for_day: LogWriter not ready")
            return {
                "component": "pnl_reconciliation",
                "enabled": True,
                "local_day": local_day_str,
                "region": region,
                "rows": [],
                "global_state": None,
                "error": "logwriter_not_ready",
            }

        views_db: dict | None = None
        try:
            if hasattr(writer, "compute_pnl_views_for_local_day"):
                # Cas M5-D-1 complet: helper côté LogWriter
                views_db = await asyncio.to_thread(
                    writer.compute_pnl_views_for_local_day,
                    local_day=local_day_str,
                    pod_region=region,
                )
            else:
                # Fallback: reconstruit [since_ms, until_ms] pour le jour local
                tz_name = {"EU": "Europe/Rome", "US": "America/New_York"}.get(region, "Europe/Rome")
                dt_start_loc = datetime.strptime(local_day_str, "%Y-%m-%d")
                if ZoneInfo is not None:
                    dt_start_loc = dt_start_loc.replace(tzinfo=ZoneInfo(tz_name))
                else:
                    dt_start_loc = dt_start_loc.replace(tzinfo=timezone.utc)
                dt_end_loc = dt_start_loc + timedelta(days=1)
                dt_start_utc = dt_start_loc.astimezone(timezone.utc)
                dt_end_utc = dt_end_loc.astimezone(timezone.utc)
                since_ms = int(dt_start_utc.timestamp() * 1000)
                until_ms = int(dt_end_utc.timestamp() * 1000) - 1

                views_db = await asyncio.to_thread(
                    writer.compute_pnl_views,
                    since_ms=since_ms,
                    until_ms=until_ms,
                )
        except Exception:
            logger.exception("run_pnl_reconciliation_for_day: DB PnL retrieval failed")
            views_db = None

        if not isinstance(views_db, dict):
            return {
                "component": "pnl_reconciliation",
                "enabled": True,
                "local_day": local_day_str,
                "region": region,
                "rows": [],
                "global_state": None,
                "error": "db_pnl_unavailable",
            }

        # 3) Prépare la vue DB par (exchange, account_alias)
        rows_acc = views_db.get("pnl_by_account") or []
        db_map: dict[tuple[str, str], dict] = {}
        for row in rows_acc:
            try:
                dims = row.get("dimensions") or {}
                ex = str(dims.get("exchange") or "").strip()
                alias = str(dims.get("account_alias") or "").strip()
                if not ex or not alias:
                    continue
                pnl = row.get("pnl") or {}
                quality = row.get("quality") or {}
                db_map[(ex, alias)] = {
                    "net_profit_quote": float(pnl.get("net_profit") or 0.0),
                    "fees_quote": float(pnl.get("fees") or 0.0),
                    "turnover_quote": float(pnl.get("turnover_quote") or 0.0),
                    "trades": int(pnl.get("trades") or 0),
                    "quality": {
                        "pnl_missing": int(quality.get("pnl_missing") or 0),
                        "pnl_missing_share": float(quality.get("pnl_missing_share") or 0.0),
                        "derived_from_bps": int(quality.get("derived_from_bps") or 0),
                        "derived_share": float(quality.get("derived_share") or 0.0),
                    },
                }
            except Exception:
                logger.exception("run_pnl_reconciliation_for_day: invalid DB row")
                continue

        # 4) Guard: callback CEX obligatoire pour comparer
        if fetch_pnl_cex_fn is None:
            logger.warning("run_pnl_reconciliation_for_day: fetch_pnl_cex_fn is None; skipping CEX side")
            return {
                "component": "pnl_reconciliation",
                "enabled": True,
                "skipped": True,
                "reason": "no_fetch_pnl_cex_fn",
                "local_day": local_day_str,
                "region": region,
                "rows": [],
                "global_state": None,
            }

        # 5) Boucle de comparaison DB ↔ CEX
        rows_out: list[dict] = []
        global_state = 0  # max(0/1/2)

        for (ex, alias), db_info in db_map.items():
            error_kind: str | None = None
            pnl_cex: dict | None = None

            # 5.a) Fetch PnL côté CEX
            try:
                res = fetch_pnl_cex_fn(ex, alias, local_day_str, region)
                if asyncio.iscoroutine(res):  # type: ignore[attr-defined]
                    res = await res  # type: ignore[assignment]
                if isinstance(res, dict):
                    pnl_cex = {
                        "net_profit_quote": float(res.get("net_profit_quote") or 0.0),
                        "fees_quote": float(res.get("fees_quote") or 0.0),
                        "turnover_quote": float(res.get("turnover_quote") or 0.0),
                        "trades": int(res.get("trades") or 0),
                    }
                else:
                    error_kind = "cex_result_invalid"
            except Exception:
                logger.exception(
                    "run_pnl_reconciliation_for_day: fetch_pnl_cex_fn failed",
                    extra={"exchange": ex, "account_alias": alias},
                )
                error_kind = "cex_fetch_error"

            pnl_db = db_info
            quality_db = db_info.get("quality") or {}

            if pnl_cex is None:
                # Impossible de comparer: on marque CRIT best-effort
                state = 2
                level = "CRIT"
                reason = "cex_pnl_unavailable"
                abs_diff = 0.0
                trades_diff = 0
            else:
                # 5.b) Classification mismatch
                state, level, reason, abs_diff, trades_diff = self._classify_pnl_mismatch(
                    pnl_db=pnl_db,
                    pnl_cex=pnl_cex,
                    quality_db=quality_db,
                )

            global_state = max(global_state, int(state))

            # 5.c) Metrics (sauf en dry_run)
            if not dry_run:
                self._emit_pnl_reco_metrics(
                    region=region,
                    exchange=ex,
                    account_alias=alias,
                    state=int(state),
                    level=str(level),
                    abs_diff_quote=float(abs_diff),
                    error_kind=error_kind,
                )

            rows_out.append(
                {
                    "exchange": ex,
                    "account_alias": alias,
                    "local_day": local_day_str,
                    "region": region,
                    "pnl_db": {
                        "net_profit_quote": pnl_db.get("net_profit_quote", 0.0),
                        "fees_quote": pnl_db.get("fees_quote", 0.0),
                        "turnover_quote": pnl_db.get("turnover_quote", 0.0),
                        "trades": pnl_db.get("trades", 0),
                    },
                    "pnl_cex": pnl_cex,
                    "delta": {
                        "net_profit_abs_diff": float(abs_diff),
                        "net_profit_signed_diff": (
                            float(pnl_cex["net_profit_quote"]) - float(pnl_db["net_profit_quote"])
                        ) if pnl_cex is not None else 0.0,
                        "trades_diff": int(trades_diff),
                    },
                    "state": int(state),
                    "level": str(level),
                    "reason": str(reason),
                    "quality_db": dict(quality_db),
                    "error_kind": error_kind,
                }
            )

        # 6) Mise à jour du timestamp de run (par région)
        if not dry_run:
            try:
                now_s = time.time()
                PNL_RECO_LAST_RUN_TS_SECONDS.labels(region=region).set(now_s)
            except Exception:
                logger.exception("run_pnl_reconciliation_for_day: failed to push last_run metric")

        return {
            "component": "pnl_reconciliation",
            "enabled": True,
            "local_day": local_day_str,
            "region": region,
            "global_state": int(global_state) if rows_out else None,
            "rows": rows_out,
        }

    def _classify_pnl_mismatch(
        self,
        *,
        pnl_db: dict,
        pnl_cex: dict,
        quality_db: dict | None = None,
    ) -> tuple[int, str, str, float, int]:
        """
        Applique la tolérance PnL Reco pour classifier un écart DB ↔ CEX.

        Retourne:
        - state : 0=OK, 1=WARN, 2=CRIT
        - level : "OK" | "WARN" | "CRIT"
        - reason : motif synthétique ("within_tolerance", "moderate_diff", ...)
        - abs_diff_quote : |PnL_CEX - PnL_DB|
        - trades_diff : trades_cex - trades_db
        """
        # Tolérances config (avec fallback robuste)
        tol_abs = float(getattr(self, "_pnl_reco_tol_abs_quote", 25.0) or 0.0)
        tol_pct = float(getattr(self, "_pnl_reco_tol_pct", 0.01) or 0.0)

        np_db = float(pnl_db.get("net_profit_quote") or 0.0)
        np_cex = float(pnl_cex.get("net_profit_quote") or 0.0)
        abs_diff = abs(np_cex - np_db)

        trades_db = int(pnl_db.get("trades") or 0)
        trades_cex = int(pnl_cex.get("trades") or 0)
        trades_diff = trades_cex - trades_db

        denom = max(1.0, abs(np_cex))
        ratio = abs_diff / denom

        q = quality_db or {}
        missing_share = float(q.get("pnl_missing_share") or 0.0)
        derived_share = float(q.get("derived_share") or 0.0)

        # Base: dans la tolérance → OK
        if abs_diff <= tol_abs and ratio <= tol_pct:
            state = 0
            level = "OK"
            reason = "within_tolerance"
        else:
            # Déviation modérée → WARN
            if ratio <= tol_pct * 2.0 and abs_diff <= tol_abs * 3.0:
                state = 1
                level = "WARN"
                reason = "moderate_diff"
            else:
                state = 2
                level = "CRIT"
                reason = "large_diff"

            # Qualité DB faible : on le note dans le reason
            if missing_share > 0.05 or derived_share > 0.05:
                reason = f"{reason}_db_low_quality"

        # Mismatch sur les volumes de trades : au minimum WARN
        if trades_diff != 0 and state == 0:
            state = 1
            level = "WARN"
            reason = "trades_count_mismatch"

        return int(state), str(level), str(reason), float(abs_diff), int(trades_diff)

    def _emit_pnl_reco_metrics(
        self,
        *,
        region: str,
        exchange: str,
        account_alias: str,
        state: int,
        level: str,
        abs_diff_quote: float,
        error_kind: str | None = None,
    ) -> None:
        """
        Pousse les métriques PnL Reco pour un (region,exchange,account_alias).

        - state: 0/1/2
        - level: "OK"/"WARN"/"CRIT"
        - abs_diff_quote: |PnL_CEX - PnL_DB|
        - error_kind: type d'erreur éventuelle ("cex_fetch_error", ...)
        """
        try:
            PNL_RECO_STATE.labels(
                region=region,
                exchange=exchange,
                account_alias=account_alias,
            ).set(float(state))
        except Exception:
            logger.exception(
                "emit_pnl_reco_metrics: failed to push PNL_RECO_STATE",
                extra={"region": region, "exchange": exchange, "account_alias": account_alias},
            )

        try:
            PNL_RECO_ABS_DIFF_QUOTE.labels(
                region=region,
                exchange=exchange,
                account_alias=account_alias,
            ).set(float(abs_diff_quote))
        except Exception:
            logger.exception(
                "emit_pnl_reco_metrics: failed to push PNL_RECO_ABS_DIFF_QUOTE",
                extra={"region": region, "exchange": exchange, "account_alias": account_alias},
            )

        # Compteur de mismatches WARN/CRIT
        if level in ("WARN", "CRIT"):
            try:
                PNL_RECO_MISMATCH_TOTAL.labels(
                    region=region,
                    exchange=exchange,
                    account_alias=account_alias,
                    level=level,
                ).inc()
            except Exception:
                logger.exception(
                    "emit_pnl_reco_metrics: failed to inc PNL_RECO_MISMATCH_TOTAL",
                    extra={"region": region, "exchange": exchange, "account_alias": account_alias, "level": level},
                )

        # Compteur d'erreurs techniques éventuelles
        if error_kind:
            try:
                PNL_RECO_ERRORS_TOTAL.labels(
                    region=region,
                    exchange=exchange,
                    account_alias=account_alias,
                    kind=error_kind,
                ).inc()
            except Exception:
                logger.exception(
                    "emit_pnl_reco_metrics: failed to inc PNL_RECO_ERRORS_TOTAL",
                    extra={
                        "region": region,
                        "exchange": exchange,
                        "account_alias": account_alias,
                        "kind": error_kind,
                    },
                )


    # ---------------- index SQLite pour JSONL ----------------
    def _ensure_index_table(self) -> None:
        """
        Table `idx` = index minimal JSONL/forensic (M5-B2-3).

        Schéma logique:
        - ts_ns        INTEGER  → timestamp (ns) de l'évènement (langue LHM)
        - type         TEXT     → event_type / kind (ex: "engine.fill", "rm.decision")
        - id           TEXT     → identifiant libre (compat historique)
        - path         TEXT     → chemin du fichier .jsonl.gz concerné
        - offset_bytes INTEGER  → offset approx. dans le fichier (bytes écrits avant la ligne)
        - trace_id     TEXT     → trace_id stable si dispo
        - bundle_id    TEXT     → bundle_id stable si dispo

        Remarque: pour les outils récents, privilégier (trace_id,bundle_id,offset_bytes,path).
        """
        if self._index_ready:
            return
        try:
            path = self._writer.get_db_path()
            with sqlite3.connect(path, timeout=10) as conn:
                cur = conn.cursor()
                # table minimale, complétée ensuite par ALTER TABLE si besoin
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS idx ("
                    "ts_ns INTEGER, "
                    "type TEXT, "
                    "id TEXT, "
                    "path TEXT, "
                    "offset_bytes INTEGER, "
                    "trace_id TEXT, "
                    "bundle_id TEXT)"
                )
                # colonnes additionnelles pour compat schémas plus anciens
                try:
                    cur.execute("PRAGMA table_info(idx);")
                    existing = {row[1] for row in cur.fetchall()}
                    for name, decl in (
                            ("offset_bytes", "INTEGER"),
                            ("trace_id", "TEXT"),
                            ("bundle_id", "TEXT"),
                    ):
                        if name not in existing:
                            cur.execute(f"ALTER TABLE idx ADD COLUMN {name} {decl};")
                except Exception:
                    # best-effort, ne doit pas bloquer le pipeline LHM
                    logger.exception("_ensure_index_table: ensure columns failed")

                cur.execute("CREATE INDEX IF NOT EXISTS idx_ts ON idx(ts_ns)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_type ON idx(type)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_type_id ON idx(type,id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_trace_id ON idx(trace_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_bundle_id ON idx(bundle_id)")
                conn.commit()
            self._index_ready = True
        except Exception:
            logger.exception("_ensure_index_table failed")

    # --- Proxy “BTL → LHM” (centralisation des métriques) ---
    def on_btl_queue_depth(self, log_type: str, *, depth: int) -> None:
        try:
            from modules.obs_metrics import LOGGERH_TRADE_QUEUE_SIZE
            LOGGERH_TRADE_QUEUE_SIZE.set(max(0, int(depth)))
        except Exception:
            pass

    def on_btl_highwater(self, log_type: str, *, depth: int) -> None:
        try:
            from modules.obs_metrics import LOGGERH_QUEUE_PLATEAU_TOTAL
            LOGGERH_QUEUE_PLATEAU_TOTAL.inc()
        except Exception:
            pass

    def on_btl_drop_full(self, log_type: str) -> None:
        # Pas de métrique dédiée “drop_full” dans obs_metrics → on signale un plateau (saturation)
        try:
            from modules.obs_metrics import LOGGERH_QUEUE_PLATEAU_TOTAL
            LOGGERH_QUEUE_PLATEAU_TOTAL.inc()
        except Exception:
            pass

    def on_btl_flush_latency(self, log_type: str, *, ms: float) -> None:
        try:
            from modules.obs_metrics import LOGGERH_WRITE_MS
            LOGGERH_WRITE_MS.observe(float(ms))
        except Exception:
            pass

    def on_btl_emitted(self, log_type: str, *, n: int) -> None:
        # Optionnel : ici on pourrait pousser un compteur “batches” si on veut
        pass

    # -------- API santé PnL (M5-B4-3) ----------------------------
    def get_pnl_pipeline_flags(self) -> dict[str, bool]:
        """
        Expose des drapeaux de santé du pipeline LHM/PnL.

        - critical_drop_seen : au moins un event PnL-critique (trade/balance/decision RM…)
          a été droppé par la chaîne JSONL.
        - storage_error_seen : une erreur d'I/O (DB ou JSONL) a été observée.

        Cette API reste volontairement minimale : elle pourra être consommée
        plus tard par le Watchdog, le RM ou un outil externe.
        """
        return {
            "critical_drop_seen": bool(getattr(self, "_critical_drop_seen", False)),
            "storage_error_seen": bool(getattr(self, "_storage_error_seen", False)),
        }


    def _insert_index_event(
            self,
            ts_ns: int,
            typ: str,
            ev_id: str | None,
            path: str | None,
            *,
            offset_bytes: int | None = None,
            trace_id: str | None = None,
            bundle_id: str | None = None,
    ) -> None:
        """
        Insère une ligne d'index JSONL minimal (M5-B2-3) dans la table `idx`.

        Paramètres:
        - ts_ns:        timestamp de l'évènement en nanosecondes.
        - typ:          event_type / kind (ex: "engine.fill", "rm.decision").
        - ev_id:        identifiant libre (compat historique).
        - path:         chemin du fichier .jsonl.gz concerné.
        - offset_bytes: offset approx. dans le fichier (bytes écrits avant la ligne).
        - trace_id:     identifiant de trace stable si disponible.
        - bundle_id:    identifiant de bundle stable si disponible.

        Outils externes peuvent ensuite:
        - rejouer une journée en filtrant par ts_ns/path,
        - extraire tous les évènements d'un trace_id,
        - reconstruire un trade complet (via trace_id/bundle_id + path/offset_bytes).
        """
        try:
            self._ensure_index_table()
            dbp = self._writer.get_db_path()
            with sqlite3.connect(dbp, timeout=10) as conn:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO idx (ts_ns,type,id,path,offset_bytes,trace_id,bundle_id) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (
                        int(ts_ns),
                        str(typ),
                        None if ev_id is None else str(ev_id),
                        path or None,
                        None if offset_bytes is None else int(offset_bytes),
                        None if trace_id is None else str(trace_id),
                        None if bundle_id is None else str(bundle_id),
                    ),
                )
                conn.commit()
        except Exception:
            logger.exception("_insert_index_event failed")


    # ---------------- rotation JSONL & stockage ----------------
    async def _maybe_rotate_jsonl_files(self) -> None:
        try:
            for rot in self._jsonl.values():
                rotated = await asyncio.to_thread(rot.rotate_if_needed)
                # la métrique bytes est déjà mise à jour dans append/rotate
                _ = rotated
        except Exception:
            logger.exception("_maybe_rotate_jsonl_files failed")

    async def _check_storage_usage(self) -> None:
        """
        Sonde l'utilisation du disque hébergeant db_dir/streams et émet des alertes 70/85%.
        """
        try:
            mount = str(Path(self._jsonl_dir).resolve().anchor or "/")
            total, used, free = shutil.disk_usage(self._jsonl_dir)
            pct = (used / max(1, total)) * 100.0
            try:
                STORAGE_USAGE_PCT.labels(mount=mount).set(pct)
            except Exception:
                pass

            level = None
            if pct >= self._disk_alert_crit_pct:
                level = "ERROR"
            elif pct >= self._disk_alert_warn_pct:
                level = "WARN"

            if level and level != self._last_storage_alert_level:
                STORAGE_ALERTS_TOTAL.labels(level=level).inc()
                await self.record_alert(
                    "LoggerHistorique", level,
                    "storage_usage_threshold_crossed",
                    {"percent_used": round(pct, 2), "mount": mount,
                     "warn": self._disk_alert_warn_pct, "crit": self._disk_alert_crit_pct},
                    None
                )
                if level == "ERROR":
                    logger.error('{"loggerh":"storage_crit","pct":%.2f,"mount":"%s"}', pct, mount)
                else:
                    logger.warning('{"loggerh":"storage_warn","pct":%.2f,"mount":"%s"}', pct, mount)
                self._last_storage_alert_level = level
            if not level:
                self._last_storage_alert_level = None
        except Exception:
            logger.exception("_check_storage_usage failed")


    async def _watchdog_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._wd_interval_s)

                # 1) queue size
                qsize = 0
                try:
                    qsize = int(getattr(self._trade_logger, "get_queue_size", lambda: 0)() or 0)
                except Exception:
                    qsize = 0
                try:
                    path = self._writer.get_db_path()
                    db_label = f"{self._db_name}:{path.split('/')[-1]}"
                    LOGGERH_TRADE_QUEUE_SIZE.set(qsize)

                except Exception:
                    pass

                # 2) plateau detection (taille quasi constante et au-dessus d'un seuil)
                self._queue_sizes.append(qsize)
                if len(self._queue_sizes) > self._wd_queue_plateau_window:
                    self._queue_sizes.pop(0)
                    window = self._queue_sizes
                    if max(window) >= self._wd_queue_min_size and (max(window) - min(window)) <= int(
                            0.05 * max(1, max(window))):
                        # plateau détecté
                        self._last_queue_plateau_ts = time.time()
                        LOGGERH_QUEUE_PLATEAU_TOTAL.labels(streams="btl_trades").inc()
                        try:
                            await self.record_alert(
                                "LoggerHistorique", "WARN", "queue_plateau_detected",
                                {"window": window[-10:], "min": min(window), "max": max(window)}, None
                            )
                            logger.warning('{"loggerh":"queue_plateau","min":%d,"max":%d,"db":"%s"}',
                                           min(window), max(window), db_label)
                        except Exception:
                            logging.exception("Unhandled exception")

                # 3) stall detection (pas d’écriture depuis X secondes)
                if self._last_write_monotonic is not None:
                    idle_s = time.monotonic() - self._last_write_monotonic
                    if idle_s >= self._wd_stall_threshold_s and qsize >= self._wd_queue_min_size:
                        LOGGERH_DB_STALLS_TOTAL.labels(stream=db_label).inc()
                        self._last_db_stall_ts = time.time()
                        try:
                            await self.record_alert(
                                "LoggerHistorique", "ERROR", "db_stall_detected",
                                {"idle_seconds": round(idle_s, 3), "queue_size": qsize}, None
                            )
                            logger.error('{"loggerh":"db_stall","idle_s":%.3f,"queue":%d,"db":"%s"}',
                                         idle_s, qsize, db_label)
                        except Exception:
                            logging.exception("Unhandled exception")

                # 4) rotation opportuniste + MAJ taille DB
                await self._maybe_rotate_files()
                await self._update_db_size_gauge()
                # 5) rotation JSONL + storage usage
                await self._maybe_rotate_jsonl_files()
                await self._check_storage_usage()

        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("watchdog_loop error")

    async def _update_db_size_gauge(self) -> None:
        try:
            path = self._writer.get_db_path()
            if not path:
                return
            st = await asyncio.to_thread(os.stat, path)
            # gauge SENZA label
            LOGGERH_DB_FILE_BYTES.set(int(st.st_size))
        except Exception:
            logging.exception("Unhandled exception")

    async def _maybe_rotate_files(self) -> None:
        """
        Rotation taille/âge : utilise rotate_if_needed(max_bytes,max_age_s) si présent,
        sinon fallback basique sur la taille du fichier + appel rotate_now() si dispo.
        """
        try:
            path = self._writer.get_db_path()
            if not path:
                return
            db_label = f"{self._db_name}:{path.split('/')[-1]}"

            # chemin rapide : API du writer disponible
            fn = getattr(self._writer, "rotate_if_needed", None)
            if callable(fn):
                rotated = await asyncio.to_thread(fn, self._max_db_bytes, self._max_db_age_s)
                if rotated:
                    self._last_rotation_epoch = time.time()
                    # senza label
                    LOGGERH_FILE_ROTATIONS_TOTAL.inc()
                    LOGGERH_LAST_ROTATION_TS_SECONDS.set(int(self._last_rotation_epoch))

                    logger.info('{"loggerh":"rotated","db":"%s"}', db_label)
                    return

            # fallback : check taille
            st = await asyncio.to_thread(os.stat, path)
            if st.st_size >= self._max_db_bytes:
                rot = getattr(self._writer, "rotate_now", None)
                if callable(rot):
                    await asyncio.to_thread(rot)
                    self._last_rotation_epoch = time.time()
                    LOGGERH_FILE_ROTATIONS_TOTAL.inc()
                    LOGGERH_LAST_ROTATION_TS_SECONDS.set(int(self._last_rotation_epoch))
                    logger.info('{"loggerh":"rotated_fallback","db":"%s"}', db_label)
        except Exception:
            logger.exception("_maybe_rotate_files failed")

    def set_rotate_every_s(self, seconds: int) -> None:
        """Ajuste dynamiquement l'intervalle de rotation/flush des métriques paires."""
        try:
            s = float(seconds)
            if s > 0:
                self._rotate_interval = s
        except Exception:
            logger.debug("set_rotate_every_s: invalid value %s", seconds)

    # ---------------- flush immédiat ----------------
    async def flush_now(self, *, rotate: bool = False) -> None:
        """Vide les buffers trades et pousse (optionnel) les métriques paires en DB."""
        try:
            # vidage du buffer BaseTradeLogger
            await self._trade_logger.flush_now()
        except Exception:
            logging.exception("Unhandled exception")

        # faire une petite cession pour laisser _run() émettre
        await asyncio.sleep(0)  # yield au scheduler

        if rotate:
            try:
                self._tracker.rotate_pairs()
            except Exception:
                logger.exception("rotate_pairs failed")

        # export des métriques paires
        try:
            rows = self._tracker.collect_pair_metrics() or []
            if rows:
                import time
                t0 = time.perf_counter()
                await asyncio.to_thread(self._writer.write_pair_history, rows)
                dt_ms = (time.perf_counter() - t0) * 1000.0
                try:
                    from modules.obs_metrics import PAIR_HISTORY_ROWS_TOTAL, PAIR_HISTORY_COMPUTE_MS
                    PAIR_HISTORY_ROWS_TOTAL.inc(len(rows))
                    PAIR_HISTORY_COMPUTE_MS.observe(dt_ms)
                except Exception:
                    pass
        except Exception:
            logger.exception("flush_now: write_pair_history failed")
        try:
            path = self._writer.get_db_path()
            db_label = f"{self._db_name}:{path.split('/')[-1]}"
            LOGGERH_LAST_FLUSH_TS_SECONDS.set(int(datetime.utcnow().timestamp()))
        except Exception:
            pass
        await self._update_db_size_gauge()
        # rotation opportuniste des flux JSONL (limiter la dérive horaire)
        try:
            for rot in self._jsonl.values():
                await asyncio.to_thread(rot.rotate_if_needed)
        except Exception:
            logger.exception("flush_now: jsonl rotate failed")


    # ---------------- event sink (trades batch) ----------------

    async def _write_trades_async(self, entries: List[Dict[str, Any]]) -> None:
        start = time.perf_counter()
        try:
            def _write():
                for e in entries:
                    self._writer.insert_trade_log("trade", **e)

            await asyncio.to_thread(_write)
            dur_ms = (time.perf_counter() - start) * 1000.0
            LOGGERH_WRITE_MS.observe(dur_ms)
            # MAJ “dernier flush”
            ts = int(datetime.utcnow().timestamp())
            try:
                path = self._writer.get_db_path()
                db_label = f"{self._db_name}:{path.split('/')[-1]}"
                LOGGERH_LAST_FLUSH_TS_SECONDS.set(ts)
            except Exception:
                pass
            # MAJ DB size gauge
            await self._update_db_size_gauge()
            # marque le dernier write pour watchdog
            self._last_write_monotonic = time.monotonic()
        except Exception:
            logger.exception("_write_trades_async failed")

    async def _rotation_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._rotate_interval)
                self._tracker.rotate_pairs()
                rows = self._tracker.collect_pair_metrics() or []
                if rows:
                    # mapping de compat: rt_latency_ema_ms -> latency_ms_avg
                    mapped = []
                    for r in rows:
                        if "latency_ms_avg" not in r and "rt_latency_ema_ms" in r:
                            r = {**r, "latency_ms_avg": r.get("rt_latency_ema_ms")}
                        mapped.append(r)
                    await asyncio.to_thread(self._writer.write_pair_history, mapped)
                    try:
                        path = self._writer.get_db_path()
                        db_label = f"{self._db_name}:{path.split('/')[-1]}"
                        LOGGERH_LAST_FLUSH_TS_SECONDS.set(int(datetime.utcnow().timestamp()))
                    except Exception:
                        pass
                    await self._maybe_rotate_files()
                    await self._update_db_size_gauge()

        except asyncio.CancelledError:
            logging.exception("Unhandled exception")
        except Exception:
            logger.exception("rotation_loop error")


    # ---------------- write API ----------------
    def sink(self, payload: Dict[str, Any]) -> None:
        """
        [M5-B1-2] Sink synchrone pour ExecutionEngine.history_logger.

        - ExecutionEngine._hist appelle history_sink(rec) depuis un contexte non-async.
        - Ici on délègue simplement à BaseTradeLogger.ingest, qui bufferise et flush en arrière-plan.
        """
        try:
            self._trade_logger.ingest(payload)
        except Exception:
            logger.exception("LHM.sink failed")


    async def record_trade(self, payload: Dict[str, Any]) -> None:
        """Ingestion publique. BaseTradeLogger gère batch/IDs/latences/tri-CEX."""
        try:
            self._trade_logger.ingest(payload)
        except Exception:
            logger.exception("record_trade failed")

    async def record_opportunity(self, payload: Dict[str, Any]) -> None:
        try:
            # tracker en amont
            try:
                self._tracker.ingest_opportunity(payload)
            except Exception:
                logging.exception("Unhandled exception")
            # tagging + normalisation guard_reason
            obj = self._with_context_tags(dict(payload or {}))
            if "guard_reason" not in obj:
                gr = obj.get("reject_reason") or obj.get("blocked_reason") or obj.get("reason")
                if gr is not None:
                    obj["guard_reason"] = str(gr)
            await asyncio.to_thread(self._writer.insert_opportunity, obj)
        except Exception:
            logger.exception("record_opportunity failed")

    async def record_alert(
        self,
        module: str,
        level: str,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        pair_key: Optional[str] = None,
    ) -> None:
        try:
            ctx = json.dumps(context or {}, separators=(",", ":"))
            await asyncio.to_thread(self._writer.insert_alert, module, level, pair_key or None, message, ctx)
        except Exception:
            logger.exception("record_alert failed")

    # ---------------- JSONL pipelines (publiques) ----------------
    async def record_rm_decision(self, payload: Dict[str, Any]) -> None:
        await self._append_jsonl("rm_decisions", payload, id_key="opp_id")

    async def record_engine_submit(self, payload: Dict[str, Any]) -> None:
        await self._append_jsonl("engine_submits", payload, id_key="client_id")

    async def record_engine_ack(self, payload: Dict[str, Any]) -> None:
        await self._append_jsonl("engine_acks", payload, id_key="client_id")

    async def record_fill_normalized(self, payload: Dict[str, Any]) -> None:
        await self._append_jsonl("fills_normalized", payload, id_key="trade_id")

    async def record_privatews_event(self, payload: Dict[str, Any]) -> None:
        await self._append_jsonl("privatews_events", payload, id_key="event_id")

    # [PATCH-API-MM] Événements MM avec sampling déterministe
    async def record_mm_quote(self, payload: Dict[str, Any]) -> None:
        if not self._stable_sample(payload, getattr(self, "_mm_sampling_quotes", 1.0)):
            return
        await self._append_jsonl("mm_quotes", self._with_context_tags(payload), id_key="quote_id")

    async def record_mm_cancel(self, payload: Dict[str, Any]) -> None:
        if not self._stable_sample(payload, getattr(self, "_mm_sampling_cancels", 1.0)):
            return
        await self._append_jsonl("mm_cancels", self._with_context_tags(payload), id_key="cancel_id")

    async def record_mm_hedge(self, payload: Dict[str, Any]) -> None:
        await self._append_jsonl("mm_hedges", self._with_context_tags(payload), id_key="hedge_id")

    # ---- helper commun
    async def _append_jsonl(self, stream: str, payload: Dict[str, Any], *, id_key: str) -> None:
        try:
            if stream not in self._q_streams:
                return
            ts_ns = int(time.time_ns())
            obj = self._with_context_tags(dict(payload or {}))
            ev_id = str(obj.get(id_key) or obj.get("id") or obj.get("client_order_id") or ts_ns)
            obj.setdefault("ts_ns", ts_ns)
            obj.setdefault("type", stream)
            obj.setdefault("id", ev_id)

            q = self._q_streams[stream]
            try:
                q.put_nowait(obj)
                # NOTE: on n'incrémente PAS ici *INGESTED* (ça comptera au flush disque)
            except asyncio.QueueFull:
                if self._stream_critical.get(stream):
                    try:
                        await asyncio.wait_for(q.put(obj), timeout=0.05)
                    except Exception:
                        self._drop_stream_record(stream)
                else:
                    self._drop_stream_record(stream)
        except Exception:
            logger.exception("_append_jsonl enqueue failed (%s)", stream)

    # [PATCH-BP] incrément de drops + log throttle
    def _drop_stream_record(self, stream: str) -> None:
        LHM_JSONL_DROPPED_TOTAL.labels(stream=stream).inc()
        # pas de log spammy; laisse Prometheus raconter l'histoire

    async def _loop_streams(self) -> None:
        """
        Écrit les streams JSONL en *round-robin* prioritaire, par rafales.
        Surveille les tailles de files et détecte les plateaux (hi-watermark prolongé).
        """
        try:
            while True:
                idle = True
                # ordre de priorité strict
                for name in self._stream_priority:
                    q = self._q_streams.get(name)
                    if not q or q.empty():
                        continue
                    idle = False
                    await self._drain_stream_once(name, q, self._stream_batch)
                    # export gauge de taille
                    try:
                        LHM_JSONL_QUEUE_SIZE.labels(stream=name).set(q.qsize())
                    except Exception:
                        pass
                    # plateau ?
                    self._maybe_plateau(name, q)
                if idle:
                    await asyncio.sleep(0.02)
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("_loop_streams crashed; will exit")


    async def _drain_all_streams_once(self) -> None:
        """
        Vidage best-effort de toutes les files (utilisé au stop).
        """
        for name, q in self._q_streams.items():
            await self._drain_stream_once(name, q, q.qsize())

    def _maybe_plateau(self, name: str, q: "asyncio.Queue") -> None:
        """
        Détecte un plateau de haute eau (file >= 80% *durant* plateau_s).
        Incrémente un compteur si franchi.
        """
        cap = q.maxsize or self._q_stream_max
        if cap <= 0:
            return
        level = q.qsize() / float(cap)
        now = time.time()
        if level >= self._high_watermark:
            if not self._plateau_since.get(name):
                self._plateau_since[name] = now
            elif now - self._plateau_since[name] >= self._plateau_s:
                try:
                    LOGGERH_QUEUE_PLATEAU_TOTAL.labels(stream=name).inc()
                except Exception:
                    pass
                # réarme pour ne pas spammer
                self._plateau_since[name] = now
        else:
            self._plateau_since[name] = 0.0

    # [PATCH-HELPERS] Tagging universel + sampling stable (md5 tail)
    def _with_context_tags(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        obj = dict(payload or {})
        obj.setdefault("deployment_mode", getattr(self, "_deployment_mode", "EU_ONLY"))
        obj.setdefault("pod_region", getattr(self, "_pod_region", "EU"))
        obj.setdefault("capital_profile", getattr(self, "_capital_profile", "NANO"))
        obj.setdefault("pacer_mode", getattr(self, "_pacer_mode", "NORMAL"))
        return obj

    @staticmethod
    def _stable_sample(rec: Dict[str, Any], ratio: float) -> bool:
        if ratio >= 1.0: return True
        if ratio <= 0.0: return False
        try:
            import hashlib, json as _json
            s = _json.dumps(rec, sort_keys=True, separators=(",", ":")).encode("utf-8")
            h = int(hashlib.md5(s).hexdigest()[-4:], 16) / 0xFFFF  # 0..1
            return h <= ratio
        except Exception:
            return True

    # Compat pour record_error (redirigée vers alerts niveau ERROR)
    async def record_error(
        self, module: str, message: str, context: Optional[Dict[str, Any]] = None, pair_key: Optional[str] = None
    ) -> None:
        await self.record_alert(module, "ERROR", message, context, pair_key)

    # ---------------- latency pipeline API (tri-CEX) ----------------

    async def _record_latency_async(self, metrics: Dict[str, Any]) -> None:
        """
        Corps réel: écrit en DB (thread) + incrémente la métrique LHM.
        """
        # 1) écriture DB en thread pour ne pas bloquer l’event loop
        try:
            await asyncio.to_thread(self._writer.insert_latency, metrics)
        except Exception:
            logger.exception("_record_latency_async: DB insert failed")

        # 2) métriques CENTRALISÉES dans le LHM
        try:
            # APRÈS
            from modules.obs_metrics import LAT_PIPELINE_EVENTS_TOTAL
            stage = str(metrics.get("stage") or "unknown")
            status = str(metrics.get("status") or "NA")
            LAT_PIPELINE_EVENTS_TOTAL.labels(stage=stage, status=status).inc()
        except Exception:
            pass

    async def get_pair_history(self, pair: str, *, limit: int = 200) -> List[Dict[str, Any]]:
        pair = (pair or "?").replace("-", "").upper()
        sql = (
            "SELECT timestamp,exchange,asset,trade_type,trade_mode,side,price,executed_volume_usdc,net_profit,"
            "account_alias,sub_account_id,"
            "route,buy_ex,sell_ex,leg_role,"
            "trade_id,bundle_id,slice_id,client_id,order_id,opp_id,route_id,"
            "ack_latency_ms,fill_latency_ms,engine_latency_ms,end_to_end_ms,t_sent_ms,t_ack_ms,t_filled_ms,t_done_ms,"
            "pair_score,pair_rank,raw_json "
            "FROM trades WHERE asset=? ORDER BY timestamp DESC LIMIT ?"
        )

        def _fetch():
            out: List[Dict[str, Any]] = []
            with sqlite3.connect(self._writer.get_db_path(), timeout=10) as conn:
                cur = conn.cursor()
                cur.execute(sql, (pair, int(limit)))
                for r in cur.fetchall():
                    (
                        ts, ex, asset, ttype, tmode, side, price, vol_usdc, netp,
                        acc_alias, sub_id,
                        route, buy_ex, sell_ex, leg_role,
                        trade_id, bundle_id, slice_id, client_id, order_id, opp_id, route_id,
                        ack, fill, engine, e2e, t_sent, t_ack, t_filled, t_done,
                        pscore, prank, raw,
                    ) = r
                    try:
                        meta = json.loads(raw) if isinstance(raw, str) else {}
                    except Exception:
                        meta = {}
                    out.append(
                        {
                            "timestamp": ts,
                            "exchange": ex,
                            "asset": asset,
                            "trade_type": ttype,
                            "trade_mode": tmode,
                            "side": side,
                            "price": price,
                            "executed_volume_usdc": vol_usdc,
                            "net_profit": netp,
                            "account_alias": acc_alias,
                            "sub_account_id": sub_id,
                            "route": route,
                            "buy_ex": buy_ex,
                            "sell_ex": sell_ex,
                            "leg_role": leg_role,
                            "trade_id": trade_id,
                            "bundle_id": bundle_id,
                            "slice_id": slice_id,
                            "client_id": client_id,
                            "order_id": order_id,
                            "opp_id": opp_id,
                            "route_id": route_id,
                            "ack_latency_ms": ack,
                            "fill_latency_ms": fill,
                            "engine_latency_ms": engine,
                            "end_to_end_ms": e2e,
                            "t_sent_ms": t_sent,
                            "t_ack_ms": t_ack,
                            "t_filled_ms": t_filled,
                            "t_done_ms": t_done,
                            "pair_score": pscore,
                            "pair_rank": prank,
                            "meta": meta,
                        }
                    )
            return out

        return await asyncio.to_thread(_fetch)

    # ---------------- vues/quotas par branche (TT/TM/REB) ----------------
    def get_active_pairs(self, mode: str | None = None) -> List[str]:
        """Liste des paires actives; filtrée par branche si `mode` est fourni."""
        # Fallback si le tracker n'a pas get_active_pairs_by_mode
        if hasattr(self._tracker, "get_active_pairs_by_mode"):
            return self._tracker.get_active_pairs_by_mode(mode)
        # fallback global
        return list(self._tracker.get_active_pairs())

    def get_priority_pairs(self, mode: str, k: int | None = None) -> List[str]:
        """Top-k prioritaire pour un mode (TT/TM/REB)."""
        if hasattr(self._tracker, "get_priority_pairs_by_mode"):
            return self._tracker.get_priority_pairs_by_mode(mode, k)
        # fallback = tronquer la liste globale
        base = list(self._tracker.get_active_pairs())
        return base[: (k or len(base))]

    def get_ranked_pairs(self) -> List[str]:
        """Alias attendu par certains scanners; ordre croissant de rank global."""
        try:
            rm = self.get_rank_map(None)
            return [p for p, _r in sorted(rm.items(), key=lambda kv: kv[1])]
        except Exception:
            return list(self._tracker.get_active_pairs())

    def get_rank_map(self, mode: str | None = None) -> Dict[str, int]:
        """Map {pair -> rang} pour un mode donné (ou global si None)."""
        if hasattr(self._tracker, "get_rank_map_by_mode"):
            return self._tracker.get_rank_map_by_mode(mode)
        # fallback global
        return self._tracker.get_rank_map()

    def rotate_now(self) -> None:
        """Force une rotation immédiate (le tracker applique ses quotas par branche)."""
        self._tracker.rotate_pairs()

    def set_max_active_by_mode(self, **kw) -> None:
        """
        Configure des quotas par branche (ex: set_max_active_by_mode(TT=10, TM=6, REB=2)).
        Fallback: si non supporté, applique TT+TM+REB → max_active global = somme.
        """
        if hasattr(self._tracker, "set_max_active_by_mode"):
            try:
                self._tracker.set_max_active_by_mode(**kw)
                return
            except Exception:
                logger.exception("set_max_active_by_mode failed on tracker")
        # Fallback: somme → max_active global
        try:
            total = sum(int(v) for v in kw.values() if v is not None)
            if total > 0 and hasattr(self._tracker, "set_max_active"):
                self._tracker.set_max_active(total)
        except Exception:
            logging.exception("Unhandled exception")

    # ---------------- exports / backup / rétention ----------------
    async def export_trades_csv(self, output_path: str = "logs/trades_export.csv") -> str:
        await asyncio.to_thread(self._writer.export_trades_to_csv, output_path)
        return output_path

    async def backup_now(self, out_path: Optional[str] = None) -> str:
        def _run():
            self._writer.backup_database(out_path)
            return out_path or "<auto>"
        return await asyncio.to_thread(_run)

    async def enforce_retention(self, *, keep_days: int, compress: bool = True) -> None:
        """Purge/compresse les DB journalières (via LogWriter.purge_old_dbs)."""
        def _run():
            self._writer.purge_old_dbs(keep_days=keep_days, compress=compress)
        await asyncio.to_thread(_run)

    # ---------------- status ----------------
    def get_status(self) -> Dict[str, Any]:
        st = {
            "module": "LoggerHistoriqueManager",
            "started": self._started,
            "db_path": self._writer.get_db_path(),
            "tracker": getattr(self._tracker, "get_status_enriched", self._tracker.get_status)(),
            "rotate_every_s": self._rotate_interval,
            "prometheus_enabled": bool(_PROM_READY),
            # extras utiles
            "trade_queue_size": getattr(self._trade_logger, "get_queue_size", lambda: None)(),
            "writer": getattr(self._writer, "get_status", lambda: {} )(),

        }
        # flags santé pipeline PnL (M5-C3) — version légère
        try:
            st["pnl_pipeline_flags"] = self.get_pnl_pipeline_flags()
        except Exception:
            st["pnl_pipeline_flags"] = {}

        # expose en plus les listes actives par mode si l’API existe
        try:
            st["active_pairs"] = {
                "ALL": self.get_active_pairs(None),
                "TT": self.get_active_pairs("TT"),
                "TM": self.get_active_pairs("TM"),
                "REB": self.get_active_pairs("REB"),
            }
        except Exception:
            logging.exception("Unhandled exception")
        try:
            st["streams"] = {
                name: {
                    "path": rot.current_path,
                    "bytes": rot.current_bytes,
                    "max_bytes": self._jsonl_max_bytes,
                    "max_age_s": self._jsonl_max_age_s,
                }
                for name, rot in self._jsonl.items()
            }
        except Exception:
            logging.exception("Unhandled exception")

        return st

    # ---------------- internes ----------------
    def _safe_rank_map(self) -> Dict[str, int]:
        """Utilise un rank map par mode si dispo, sinon global."""
        try:
            # global (évite de surcharger les écritures avec des rangs multibranches)
            return self.get_rank_map(None)
        except Exception:
            return {}


# -------------------- small locals --------------------
def _to_int(x) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None
