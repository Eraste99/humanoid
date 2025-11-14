# -*- coding: utf-8 -*-
from __future__ import annotations
"""
LoggerHistoriqueManager — façade unique et async-safe (tri-CEX ready).

Sous-modules encapsulés:
- BaseTradeLogger      → batch passif + normalisation (IDs, latences, TT/TM/REB, route/buy_ex/sell_ex/leg_role)
- LogWriter            → I/O SQLite rotatif (schémas étendus + table latency)
- PairHistoryTracker   → scoring/rotation + moyennes de latences par paire (EMA)

Nouveautés:
- record_latency(metrics): persiste dans table `latency` + (optionnel) métriques Prometheus.
- get_pair_history(...) expose aussi trade_mode/account_alias/sub_account_id/route/buy_ex/sell_ex/leg_role.
- APIs par branche (TT/TM/REB): get_active_pairs(mode), get_priority_pairs(mode, k), get_rank_map(mode), rotate_now(),
  set_max_active_by_mode(...). Fallback sécurisé si le tracker n’a pas encore les méthodes *_by_mode(...).
- **Flush immédiat**: `await flush_now(rotate=True)` vide les buffers et pousse les métriques paires.
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
        set_flush_batch, set_forensic_head_numeric
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

        # Limites rotation/retention JSONL
        self._jsonl_max_bytes = int(getattr(L, "LHM_JSONL_MAX_BYTES", 256 * 1024 * 1024))  # 256 MiB
        self._jsonl_max_age_s = int(getattr(L, "LHM_JSONL_MAX_AGE_S", 3600))               # 1h

        # Seuils d’alertes stockage
        self._disk_alert_warn_pct = float(getattr(L, "LHM_DISK_WARN_PCT", 70.0))
        self._disk_alert_crit_pct = float(getattr(L, "LHM_DISK_CRIT_PCT", 85.0))
        self._last_storage_alert_level = None  # None|"WARN"|"ERROR"

        # Watchdogs/rotation DB (métriques globales)
        self._db_name = str(getattr(L, "LHM_DB_NAME", "loggerh"))
        self._max_db_bytes = int(getattr(L, "LHM_DB_MAX_BYTES", 256 * 1024 * 1024))
        self._max_db_age_s = int(getattr(L, "LHM_DB_MAX_AGE_S", 24 * 3600))
        self._wd_interval_s = float(getattr(L, "LHM_WD_INTERVAL_S", 2.0))
        self._wd_queue_plateau_window = int(getattr(L, "LHM_WD_PLATEAU_WINDOW", 15))
        self._wd_queue_min_size = int(getattr(L, "LHM_WD_QUEUE_MIN_SIZE", 50))
        self._wd_stall_threshold_s = float(getattr(L, "LHM_WD_STALL_THRESHOLD_S", 10.0))

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
        self._jsonl_dir = str(Path(db_dir) / "streams")
        self._jsonl: dict[str, _JsonlRotator] = {
            "rm_decisions":     _JsonlRotator(self._jsonl_dir, "rm_decisions",     max_bytes=self._jsonl_max_bytes, max_age_s=self._jsonl_max_age_s),
            "engine_submits":   _JsonlRotator(self._jsonl_dir, "engine_submits",   max_bytes=self._jsonl_max_bytes, max_age_s=self._jsonl_max_age_s),
            "engine_acks":      _JsonlRotator(self._jsonl_dir, "engine_acks",      max_bytes=self._jsonl_max_bytes, max_age_s=self._jsonl_max_age_s),
            "fills_normalized": _JsonlRotator(self._jsonl_dir, "fills_normalized", max_bytes=self._jsonl_max_bytes, max_age_s=self._jsonl_max_age_s),
            "privatews_events": _JsonlRotator(self._jsonl_dir, "privatews_events", max_bytes=self._jsonl_max_bytes, max_age_s=self._jsonl_max_age_s),
            # MM streams
            "mm_quotes":        _JsonlRotator(self._jsonl_dir, "mm_quotes",        max_bytes=self._jsonl_max_bytes, max_age_s=self._jsonl_max_age_s),
            "mm_cancels":       _JsonlRotator(self._jsonl_dir, "mm_cancels",       max_bytes=self._jsonl_max_bytes, max_age_s=self._jsonl_max_age_s),
            "mm_hedges":        _JsonlRotator(self._jsonl_dir, "mm_hedges",        max_bytes=self._jsonl_max_bytes, max_age_s=self._jsonl_max_age_s),
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
                    # Drop contrôlé si l’écriture échoue
                    try:
                        from modules.obs_metrics import LHM_JSONL_DROPPED_TOTAL
                        LHM_JSONL_DROPPED_TOTAL.labels(stream=name).inc()
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
        Règle de secours « signe seulement » (sans inventer de montant).
        Ne touche pas e['net_profit'] ; ne fait que poser e['net_profit_sign'] si évident.
        Compte une métrique 'MISSING_NET_PROFIT_TOTAL' si rien d'inférable.
        """
        if e.get("net_profit_sign") is not None:
            return
        try:
            np = e.get("net_profit")
            if isinstance(np, (int, float)) and np != 0:
                e["net_profit_sign"] = 1 if np > 0 else -1
                return
            nbps = e.get("net_bps") or e.get("spread_net_final_bps")
            if isinstance(nbps, (int, float)) and nbps != 0:
                e["net_profit_sign"] = 1 if nbps > 0 else -1
                try:
                    from modules.obs_metrics import DERIVED_NET_PROFIT_SIGN_TOTAL
                    DERIVED_NET_PROFIT_SIGN_TOTAL.labels(module="LHM", branch=str(e.get("trade_mode") or "")).inc()
                except Exception:
                    pass
            else:
                try:
                    from modules.obs_metrics import MISSING_NET_PROFIT_TOTAL
                    MISSING_NET_PROFIT_TOTAL.labels(module="LHM", branch=str(e.get("trade_mode") or "")).inc()
                except Exception:
                    pass
        except Exception:
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
        """
        streams = getattr(self, "_jsonl_streams", {}) or {}
        q_rot = streams.get(stream)
        if not q_rot:
            return
        q, _rot = q_rot
        try:
            q.put_nowait(payload)
        except Exception:
            try:
                from modules.obs_metrics import LHM_JSONL_DROPPED_TOTAL
                LHM_JSONL_DROPPED_TOTAL.labels(stream=stream).inc()
            except Exception:
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
            pod = (e.get("pod_region") or "EU")
            ts = e.get("ts_ms") or e.get("ts") or int(time.time() * 1000)
            # Horaires (UTC/local) + local_day
            e.update(self._derive_time_fields(ts, pod))
            # Canonicalisation
            e = self._canonicalize(e)
            # Fallback signe
            self._fallback_net_profit_sign(e)
            enriched.append(e)
            # JSONL parallèle “events”
            self._enqueue_jsonl("events", {"stream": "events", "log_type": log_type, "payload": e,
                                           "ingested_ms": int(time.time() * 1000)})

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
            self._enqueue_jsonl("errors", {"stream": "errors", "log_type": log_type, "payload": enriched,
                                           "err": "db_write_failed"})

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

    # ---------------- index SQLite pour JSONL ----------------
    def _ensure_index_table(self) -> None:
        if self._index_ready:
            return
        try:
            path = self._writer.get_db_path()
            with sqlite3.connect(path, timeout=10) as conn:
                cur = conn.cursor()
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS idx ("
                    "ts_ns INTEGER, "
                    "type TEXT, "
                    "id TEXT, "
                    "path TEXT)"
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ts ON idx(ts_ns)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_type ON idx(type)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_type_id ON idx(type,id)")
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

    def _insert_index_event(self, ts_ns: int, typ: str, ev_id: str, path: str | None) -> None:
        try:
            self._ensure_index_table()
            dbp = self._writer.get_db_path()
            with sqlite3.connect(dbp, timeout=10) as conn:
                cur = conn.cursor()
                cur.execute("INSERT INTO idx (ts_ns,type,id,path) VALUES (?,?,?,?)",
                            (int(ts_ns), str(typ), str(ev_id), path or None))
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
            "tracker": self._tracker.get_status(),
            "rotate_every_s": self._rotate_interval,
            "prometheus_enabled": bool(_PROM_READY),
            # extras utiles
            "trade_queue_size": getattr(self._trade_logger, "get_queue_size", lambda: None)(),
            "writer": getattr(self._writer, "get_status", lambda: {} )(),
        }
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
