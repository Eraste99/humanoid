# -*- coding: utf-8 -*-
from __future__ import annotations
"""
MarketDataRouter — alignement multi-CEX (tri-CEX) + fan-out 12 routes (low-latency)

Structure des routes :
- 3 combos alignés (→ Scanner uniquement) :
  - combo:BINANCE/COINBASE  -> {"scanner": Queue}
  - combo:BINANCE/BYBIT     -> {"scanner": Queue}
  - combo:BYBIT/COINBASE    -> {"scanner": Queue}

- 9 per-CEX continues (→ VolatilityMonitor, SlippageHandler, Health/Observability) :
  - cex:BINANCE  -> {"vol": Queue, "slip": Queue, "health": Queue}
  - cex:COINBASE -> {"vol": Queue, "slip": Queue, "health": Queue}
  - cex:BYBIT    -> {"vol": Queue, "slip": Queue, "health": Queue}

Politique per-CEX :
- Vol : “on-change” (>~2–3 bps sur EMA(250–500ms)), cap 3–5 Hz, heartbeat 1 Hz
- Slip : “on-change” (>~5–10 bps, proxy simple), cap 3–5 Hz, heartbeat 1–1.5 Hz

Flags low-latency :
- push_to_scanner (def True)       : push direct (deux L1) → Scanner
- publish_combo_to_bus (def True)  : publication payload combo:... (debug/QA)

API d’entrée/sortie inchangée côté snapshots ; clés out_queues explicites : "combo:..." et "cex:...".
"""


from _collections_abc import  Mapping
from modules.utils import json_utils as json
import asyncio, contextlib, inspect, logging, math, time
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Deque, List, Optional, Tuple, Callable, Protocol, runtime_checkable
# from contracts.payloads import HealthEvent, MarketEvent, SlipEvent, ValidationError, VolEvent # Removed for CPU optimization
# On les ré-importe localement ou on les laisse commentés ici pour éviter l'overhead import global?
# L'issue demande de les utiliser pour le sampling.
try:
    from contracts.payloads import MarketEvent, ValidationError
except ImportError:
    MarketEvent = None
    ValidationError = Exception

logger = logging.getLogger("MarketDataRouter")

# Observabilité (no-op fallback)
try:
    from modules.obs_metrics import router_on_combo_event, report_nonfatal  # type: ignore
except Exception:  # pragma: no cover
    router_on_combo_event = None  # type: ignore

    def report_nonfatal(*a, **k):
        return None

# --- Observability (canonique) ----------------------------------------------
# On importe TOUT depuis modules.obs_metrics ; si non dispo, on utilise le fallback centralisé.
try:
    from modules.obs_metrics import (
        mark_router_to_scanner_ts,
        ROUTER_TO_SCANNER_ERRORS_TOTAL,
        ROUTER_QUEUE_DEPTH,
        ROUTER_PAIR_QUEUE_DEPTH,
        ROUTER_QUEUE_DEPTH_BY_EX,
        ROUTER_QUEUE_HIGH_WATERMARK_TOTAL,
        ROUTER_DROPPED_TOTAL,
        ROUTER_SLO_MISSING_TOTAL,
        ROUTER_DEPLOYMENT_MODE_INVALID_TOTAL,
        ROUTER_CLOCK_OFFSET_MS,
        WS_RECONNECTS_TOTAL,
        note_router_cfg,
        safe_inc,
        safe_set,
        record_pipeline_latency,
        set_pipeline_backlog,
        should_trace_latency,
    )
except ImportError:  # pragma: no cover
    # On laisse obs_metrics gérer ses propres fallbacks si possible, 
    # sinon on définit le minimum vital pour ne pas crasher.
    def mark_router_to_scanner_ts(*a, **k): return None
    def note_router_cfg(*a, **k): return None
    def safe_inc(*a, **k): return None
    def safe_set(*a, **k): return None
    
    ROUTER_TO_SCANNER_ERRORS_TOTAL = None
    ROUTER_QUEUE_DEPTH = None
    ROUTER_PAIR_QUEUE_DEPTH = None
    ROUTER_QUEUE_DEPTH_BY_EX = None
    ROUTER_QUEUE_HIGH_WATERMARK_TOTAL = None
    ROUTER_DROPPED_TOTAL = None
    WS_RECONNECTS_TOTAL = None
    ROUTER_SLO_MISSING_TOTAL = None
    ROUTER_DEPLOYMENT_MODE_INVALID_TOTAL = None
    ROUTER_CLOCK_OFFSET_MS = None


# PATCH 1 — Gauge pair-level: router_queue_depths{exchange, pair}
# À coller près des autres imports Prometheus (au-dessus de MarketDataRouter)

# ------------------------- Scanner contract -------------------------
@runtime_checkable
class ScannerProtocol(Protocol):
    def update_orderbook(self, event: Dict[str, Any]) -> Any: ...  # may be sync or async


# ------------------------- Utils temps & L2 -------------------------
def _now_dt() -> datetime:
    return datetime.utcnow()  # UTC naive


def _to_dt(ts_like) -> Optional[datetime]:
    """Accepte s / ms / µs / ns ; heuristique décroissante pour ms>=1e11."""
    if ts_like is None:
        return None
    try:
        ts = float(ts_like)
        if ts >= 1e18:      # ns
            ts = ts / 1e9
        elif ts >= 1e15:    # µs
            ts = ts / 1e6
        elif ts >= 1e11:    # ms (ex: 1.7e12)
            ts = ts / 1e3
        return datetime.utcfromtimestamp(ts)
    except Exception as e:
        logger.error(f"[Timestamp] Conversion échouée: {ts_like} -> {e}")
        return None


def _percentile(values: List[float], p: float) -> Optional[float]:
    if not values:
        return None
    xs = sorted(values)
    k = max(0, min(len(xs) - 1, int(round((p / 100.0) * (len(xs) - 1)))))
    return xs[k]


def _sanitize_orderbook(ob: Dict[str, Any], max_levels: int = 50) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
    bids = ob.get("bids") or []
    asks = ob.get("asks") or []
    out_bids: List[Tuple[float, float]] = []
    out_asks: List[Tuple[float, float]] = []
    try:
        for px, qty, *_ in bids[:max_levels]:
            out_bids.append((float(px), float(qty)))
    except Exception:
        out_bids = []
    try:
        for px, qty, *_ in asks[:max_levels]:
            out_asks.append((float(px), float(qty)))
    except Exception:
        out_asks = []
    out_bids.sort(key=lambda x: x[0], reverse=True)
    out_asks.sort(key=lambda x: x[0])
    return out_bids, out_asks


# ----------------------------- Router ------------------------------
class MarketDataRouter:
    _STOP_SENTINEL_KEY = "__stop__"

    def __init__(
        self,
        in_queue: asyncio.Queue,
        *,
        router_cfg=None,
        bot_cfg=None,
        pair_mapping: Optional[Dict[str, Dict[str, str]]] = None,
        out_queues: Optional[Dict[str, Dict[str, asyncio.Queue]]] = None,
        combos: Optional[List[Tuple[str, str]]] = None,
        scanner: ScannerProtocol = None,             # ← obligatoire
        anchors_sink: Optional[Callable[[Dict[str, Any]], None]] = None,  # push anchor vers un hub externe
        # Fenêtres (histogramme de skew → delta adaptatif)
        base_max_delta_ms: int = 160,
        min_delta_ms: int = 100,
        max_delta_cap_ms: int = 200,
        skew_margin_ms: int = 20,
        stale_source_ms: int = 1000,
        purge_threshold_s: int = 2,
        purge_interval_s: float = 1.0,
        history_len: int = 256,
        verbose: bool = False,
        # Cadence per-CEX (vol/slip)
        vol_onchange_bps: float = 2.5,
        vol_ema_window_ms: int = 400,
        vol_max_hz: float = 5.0,
        vol_heartbeat_s: float = 1.0,
        slip_onchange_bps: float = 7.0,      # proxy simple
        slip_max_hz: float = 5.0,
        hb_onchange_bps: float = 5.0,
        slip_heartbeat_s: float = 1.5,
        health_heartbeat_s: float = 1.0,
        shard_id: str = "S0",
        # Flags low-latency
        push_to_scanner: bool = True,
        publish_combo_to_bus: bool = True,
        # Gating : exiger un premier L2 avant publication (par clé EX/PAIR)
        require_l2_first: bool = False,
            # Coalescing (par paire)
            coalesce_window_ms: int = 15,
            coalesce_maxlen: int = 3,
            ws_source_backpressure_cooldown_s: float = 5.0,
    ) -> None:
        if in_queue is None:
            raise ValueError("MarketDataRouter: in_queue requis")
        self.in_queue = in_queue
        # Validation sampling (P0)
        self._val_counter = 0
        self._val_sample_rate = 100  # 1/100 events validés par Pydantic
        # Config injectée dès la construction (legacy : bot_cfg peut subsister)
        self.bot_cfg = bot_cfg
        self.cfg = bot_cfg # P0: Alias pour éviter les AttributeError sur .cfg
        self.router_cfg = router_cfg or (bot_cfg.router if bot_cfg is not None else None)
        self.pair_mapping = pair_mapping or {} # P0: Mapping des symboles pour la validation
        self.shard = str(shard_id).upper()  # ➌ NOUVEAU
        self._deployment_mode_logged = False
        self._missing_slo_warned: set[tuple[str, str]] = set()

        # scanner obligatoire (fail-fast)
        if scanner is None or not hasattr(scanner, "update_orderbook"):
            raise ValueError("MarketDataRouter: 'scanner' est requis et doit exposer update_orderbook(event).")
        self.scanner = scanner

        self.anchors_sink = anchors_sink

        self.combos: List[Tuple[str, str]] = [
            (a.upper(), b.upper())
            for (a, b) in (combos or [("BINANCE", "COINBASE"), ("BINANCE", "BYBIT"), ("BYBIT", "COINBASE")])
        ]

        if self.router_cfg is not None:
            stale_source_ms = int(getattr(self.router_cfg, "stale_ms", stale_source_ms))
            coalesce_window_ms = int(getattr(self.router_cfg, "coalesce_window_ms", coalesce_window_ms))
            require_l2_first = bool(getattr(self.router_cfg, "require_l2_first", require_l2_first))
        self.stale_cfg_ms = int(stale_source_ms or 1000)
        stale_source_ms = self.stale_cfg_ms

        vol_onchange_bps = float(getattr(self.router_cfg, "vol_onchange_bps", vol_onchange_bps))
        slip_onchange_bps = float(getattr(self.router_cfg, "slip_onchange_bps", slip_onchange_bps))
        hb_onchange_bps = float(getattr(self.router_cfg, "hb_onchange_bps", hb_onchange_bps))
        coalesce_maxlen = int(getattr(self.router_cfg, "coalesce_maxlen", coalesce_maxlen))
        ws_source_backpressure_cooldown_s = float(getattr(
            self.router_cfg,
            "ws_source_backpressure_cooldown_s",
            ws_source_backpressure_cooldown_s,
        ))

        # --- Queues de sortie vers Scanner / Vol / Slip / Health -------------------
        queue_max_spec: int | Dict[str, int] | None = None
        if out_queues is not None:
            # Boot peut injecter un mapping explicite pré-construit
            self.out_queues = out_queues
            if self.router_cfg is not None:
                queue_max_spec = getattr(self.router_cfg, "out_queues_maxlen_by_kind", None) or getattr(
                    self.router_cfg, "out_queues_maxlen", None
                )
        else:
            # P0 Marché Public — tailles de queues pilotées par BotConfig.router
            # 1) base globale (ROUTER_OUT_QUEUES_MAXLEN)
            base_max = 2000  # fallback legacy
            router_cfg = self.router_cfg
            try:
                v = getattr(router_cfg, "out_queues_maxlen", None) if router_cfg is not None else None
                if v is not None:
                    base_max = int(v)
            except Exception:
                # on garde le fallback 2000 si la cfg n'est pas disponible
                base_max = 2000

            if base_max <= 0:
                base_max = 2000

            # 2) Priorité à la map stream-centrics si fournie (ROUTER_OUT_QUEUES_MAXLEN_BY_KIND)
            custom_by_kind: Dict[str, int] = {}
            if router_cfg is not None:
                try:
                    raw = getattr(router_cfg, "out_queues_maxlen_by_kind", {}) or {}
                    if isinstance(raw, dict):
                        # on force en int, on filtre les valeurs invalides
                        custom_by_kind = {
                            str(k): int(v)
                            for k, v in raw.items()
                            if v is not None
                        }
                except Exception:
                    custom_by_kind = {}

            if custom_by_kind:
                # On s'assure que tous les streams critiques ont une capacité définie
                other_default = max(1000, base_max // 2)
                maxsize_spec: int | Dict[str, int] = dict(custom_by_kind)
                # combo = flux critique (Router → Scanner)
                maxsize_spec.setdefault("combo", int(base_max))
                # vol/slip/health = au moins un plancher raisonnable
                maxsize_spec.setdefault("vol", int(other_default))
                maxsize_spec.setdefault("slip", int(other_default))
                maxsize_spec.setdefault("health", int(other_default))
            else:
                # Fallback P0 documenté :
                #   - la file "combo" prend la capacité complète (base_max)
                #   - les files vol/slip/health prennent une fraction (base_max // 2, plancher 1000)
                #   ex: base_max=5000  => combo=5000, vol/slip/health=2500
                other_max = max(1000, base_max // 2)
                maxsize_spec: int | Dict[str, int] = {
                    "combo": int(base_max),
                    "vol": int(other_max),
                    "slip": int(other_max),
                    "health": int(other_max),
                }

            self.out_queues = self.build_default_out_queues(
                combos=self.combos,
                maxsize=maxsize_spec,
            )
            queue_max_spec = maxsize_spec


        # vues latest (full & light) + lock
        self._latest_lock = asyncio.Lock()
        self._latest_books: Dict[str, Dict[str, Dict[str, Any]]] = {}  # full: bids/asks/ts/etc.
        self._latest_light: Dict[str, Dict[str, Dict[str, Any]]] = {}  # light: best_bid/best_ask/ts
        # rétro-compat si d’autres parties lisent self.latest
        self.latest = self._latest_books

        self.base_max_delta = timedelta(milliseconds=base_max_delta_ms)
        self.min_delta = timedelta(milliseconds=min_delta_ms)
        self.max_delta_cap = timedelta(milliseconds=max_delta_cap_ms)
        self.skew_margin_ms = int(skew_margin_ms)
        # TTL brut pour staleness Router (ms) + version timedelta
        self.stale_source_ms = int(stale_source_ms)
        self.stale_source = timedelta(milliseconds=self.stale_source_ms)

        # housekeeping
        self.purge_threshold = timedelta(seconds=purge_threshold_s)
        self.purge_interval = purge_interval_s
        self.verbose = verbose

        # historiques & fenêtres
        self._skew_ms_history: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=history_len))
        self._pair_max_delta: Dict[str, timedelta] = defaultdict(lambda: self.base_max_delta)

        # --- Per-CEX cadence & state (on-change + cap Hz + heartbeat)
        self.vol_onchange_bps = float(vol_onchange_bps)
        self.vol_ema_window_ms = int(max(50, vol_ema_window_ms))
        self.vol_max_hz = float(max(0.5, vol_max_hz))
        self.vol_heartbeat_s = float(max(0.5, vol_heartbeat_s))

        self.slip_onchange_bps = float(slip_onchange_bps)
        self.slip_max_hz = float(max(0.5, slip_max_hz))
        self.slip_heartbeat_s = float(max(0.5, slip_heartbeat_s))
        self.health_heartbeat_s = float(max(0.5, health_heartbeat_s))
        self.hb_onchange_bps = float(hb_onchange_bps)

        # flags
        self.push_to_scanner = bool(push_to_scanner)
        self.publish_combo_to_bus = bool(publish_combo_to_bus)
        self.require_l2_first = bool(require_l2_first)
        self._l2_seen: Dict[Tuple[str, str], bool] = defaultdict(lambda: False)  # (EX,PAIR) -> bool
        # Cache de tier par pair (PRIMARY/AUDITION/UNKNOWN) pour métriques et purge
        self._pair_tier: Dict[str, str] = {}

        # --- Coalescing par paire ---
        self.coalesce_window_ms = int(max(1, coalesce_window_ms))
        self.coalesce_maxlen = int(max(1, coalesce_maxlen))
        # Buckets: pair -> { EX -> deque([...events...], maxlen=N) }
        self._coalesce_buckets: Dict[str, Dict[str, Deque[Dict[str, Any]]]] = defaultdict(dict)
        # Tâches de flush programmées par paire
        self._coalesce_tasks: Dict[str, asyncio.Task] = {}

        try:
            note_router_cfg(
                self.stale_source_ms,
                self.coalesce_window_ms,
                self.require_l2_first,
                queue_max_spec,
            )
        except Exception:
            pass

        # state per (EX,PAIR)
        self._vol_ema: Dict[Tuple[str, str], float] = {}
        self._vol_last_ts: Dict[Tuple[str, str], float] = {}
        self._vol_last_pub_val: Dict[Tuple[str, str], float] = {}
        self._vol_last_pub_ts: Dict[Tuple[str, str], float] = defaultdict(float)

        self._slip_last_metric: Dict[Tuple[str, str], float] = {}
        self._slip_last_pub_ts: Dict[Tuple[str, str], float] = defaultdict(float)

        # state per EX
        self._health_last_pub_ts: Dict[str, float] = defaultdict(float)

        # throttles legacy feeds (compat, indépendants des out_queues)
        self._last_vol_feed = defaultdict(lambda: 0.0)
        self._last_slip_feed = defaultdict(lambda: 0.0)
        self._vol_feed_min_interval_ms = 75
        # scanner lane décorrélée pour éviter de bloquer l'ingestion
        scanner_max = int(getattr(self.router_cfg, "scanner_queue_maxlen", 512)) if self.router_cfg else 512
        self._scanner_queue: asyncio.Queue = asyncio.Queue(
            maxsize=max(1, scanner_max)
        ) if self.push_to_scanner else asyncio.Queue(maxsize=1)
        self._scanner_task: Optional[asyncio.Task] = None
        self._slip_feed_min_interval_ms = 100

        # buffers / métriques
        self.buffer: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.successful_syncs = 0
        self._running = False
        self._purge_task: Optional[asyncio.Task] = None
        self.last_log_time = defaultdict(lambda: 0.0)
        self.restart_count = 0

        # compteurs
        self._events_in = 0
        self._events_ignored_stale = 0
        self._events_schema_errors = 0
        self._route_published = defaultdict(int)
        self._route_drops = defaultdict(int)
        # Baseline gauges pour chaque CEX (0 au démarrage)
        try:
            self._update_queue_depth_metrics_all()
        except Exception:
            logging.exception("Unhandled exception")
        # P0 Observabilité: créer des séries 0 pour éviter les panneaux Grafana 'No data'
        # (Prometheus n'expose pas un Counter tant qu'il n'a jamais été incrémenté)
        try:
            ROUTER_TO_SCANNER_ERRORS_TOTAL.labels(route="tri_cex", reason="exception").inc(0)
            ROUTER_TO_SCANNER_ERRORS_TOTAL.labels(route="tri_cex", reason="queue_full").inc(0)
        except Exception:
            pass

        # PATCH 3 — State & paramètres backpressure
        # À ajouter dans __init__ de MarketDataRouter (section 'housekeeping' / 'compteurs')

        self._event_sink: Optional[Callable[[Dict[str, Any]], Any]] = None
        # Fenêtre & seuil d’alerte backpressure (drop coalescing/overflow)
        self.drop_window_s: float = 3.0
        self.drop_alert_threshold: int = 64
        self._bp_drops: Dict[str, deque] = {}
        self.ws_source_backpressure_cooldown_s = float(max(0.5, ws_source_backpressure_cooldown_s))
        self._ws_backpressure_until: float = 0.0
        self._ws_backpressure_state: Dict[str, Dict[str, Any]] = {}

    # ----------------------- Keys / Queues builders -----------------------
    @staticmethod
    def _combo_key(a: str, b: str) -> str:
        return f"combo:{a.upper()}/{b.upper()}"

    @staticmethod
    def _cex_key(ex: str) -> str:
        return f"cex:{ex.upper()}"

    @classmethod
    def build_default_out_queues(
            cls,
            *,
            combos: List[Tuple[str, str]],
            maxsize: int | Dict[str, int] = 2000,
    ) -> Dict[str, Dict[str, asyncio.Queue]]:
        """
        Construit les out_queues avec tailles différenciées si un dict est fourni:
          maxsize = {"combo":5000, "vol":2000, "slip":2000, "health":2000}
        Fallback rétro-compatible: si maxsize est un int, on l'utilise pour tout.
        """
        if isinstance(maxsize, dict):
            combo_sz = int(maxsize.get("combo", 2000))
            vol_sz = int(maxsize.get("vol", 2000))
            slip_sz = int(maxsize.get("slip", 2000))
            health_sz = int(maxsize.get("health", 2000))
        else:
            combo_sz = vol_sz = slip_sz = health_sz = int(maxsize)

        out: Dict[str, Dict[str, asyncio.Queue]] = {}

        # combos -> scanner
        for a, b in combos:
            out[cls._combo_key(a, b)] = {"scanner": asyncio.Queue(maxsize=combo_sz)}

        # per-CEX -> vol/slip/health
        exs = set([a for a, _ in combos] + [b for _, b in combos])
        for ex in sorted(exs):
            out[cls._cex_key(ex)] = {
                "vol": asyncio.Queue(maxsize=vol_sz),
                "slip": asyncio.Queue(maxsize=slip_sz),
                "health": asyncio.Queue(maxsize=health_sz),
            }

        return out

    # --- PATCH A: propriété pour _configure_topic_rates() ---
    @property
    def exchanges(self) -> List[str]:
        # dérive la liste des CEX depuis self.combos
        return sorted({a for (a, _) in self.combos} | {b for (_, b) in self.combos})

    # --- /PATCH A ---

    # --- Métriques: helpers queue depth ---
    @staticmethod
    def _qsize(q: Optional[asyncio.Queue]) -> int:
        try:
            return int(q.qsize()) if q else 0
        except Exception:
            return 0

    @staticmethod
    def _queue_label(route_name: str) -> str:
        if route_name.startswith("combo:"):
            return "combo"
        if route_name.startswith("cex:"):
            if route_name.endswith(".vol"):
                return "vol"
            if route_name.endswith(".slip"):
                return "slip"
            if route_name.endswith(".health"):
                return "health"
        if "scanner" in route_name:
            return "scanner"
        return "combo"

    def _update_queue_depth_metrics_for(self, ex: str) -> None:
        key = self._cex_key(ex)
        bucket = self.out_queues.get(key) or {}
        v = self._qsize(bucket.get("vol"))
        s = self._qsize(bucket.get("slip"))
        h = self._qsize(bucket.get("health"))
        t = v + s + h

        safe_set(
            ROUTER_QUEUE_DEPTH_BY_EX,
            "router_queue_depth_by_ex",
            "_update_queue_depth_metrics_for",
            v,
            exchange=str(ex).upper(),
            queue="vol",
            shard=self.shard,
        )
        safe_set(
            ROUTER_QUEUE_DEPTH_BY_EX,
            "router_queue_depth_by_ex",
            "_update_queue_depth_metrics_for",
            s,
            exchange=str(ex).upper(),
            queue="slip",
            shard=self.shard,
        )
        safe_set(
            ROUTER_QUEUE_DEPTH_BY_EX,
            "router_queue_depth_by_ex",
            "_update_queue_depth_metrics_for",
            h,
            exchange=str(ex).upper(),
            queue="health",
            shard=self.shard,
        )
        safe_set(
            ROUTER_QUEUE_DEPTH_BY_EX,
            "router_queue_depth_by_ex",
            "_update_queue_depth_metrics_for",
            t,
            exchange=str(ex).upper(),
            queue="total",
            shard=self.shard,
        )

    def _update_queue_depth_metrics_all(self) -> None:
        for key in list(self.out_queues.keys()):
            if key.startswith("cex:"):
                ex = key.split(":", 1)[1]
                self._update_queue_depth_metrics_for(ex)

    # PATCH 2 — Helpers métriques + event sink backpressure
    # À coller dans la classe MarketDataRouter (méthodes)

    def set_event_sink(self, sink: Optional[Callable[[Dict[str, Any]], Any]]) -> None:
        """Optionnel: réception des alertes (backpressure, etc.)."""
        self._event_sink = sink

    def _update_pair_queue_depth(self, pair: str, tier: str, depth: int) -> None:
        """Gauge: profondeur de coalescing par pair.
        IMPORTANT (P0):
          - Le dashboard D6 attend router_pair_queue_depth{pair,tier}.
          - Ici, 'tier' est utilisé comme dimension de découpage (dans ce fichier, on lui passe l'exchange).
        """
        try:
            pk = str(pair or "UNKNOWN")
            tr = str(tier or "UNKNOWN").strip().upper() or "UNKNOWN"
            self._pair_tier[pk] = tr
            safe_set(
                ROUTER_PAIR_QUEUE_DEPTH,
                "router_pair_queue_depth",
                "router",
                pair=pk,
                tier=tr,
                value=float(depth),
            )
        except Exception:
            pass

    def _bp_note_drop(self, route: str, *, reason: str) -> None:
        """Accumule des drops pour détection backpressure soft."""
        try:
            dq = self._bp_drops.setdefault(route, deque())
            dq.append(time.monotonic())
            # GC local pour contenir la fenêtre
            horizon = time.monotonic() - float(self.drop_window_s)
            while dq and dq[0] < horizon:
                dq.popleft()
            # seuil → alerte
            if len(dq) >= int(self.drop_alert_threshold):
                if self._event_sink:
                    self._event_sink({
                        "module": "MarketDataRouter",
                        "level": "WARNING",
                        "type": "backpressure",
                        "route": route,
                        "reason": reason,
                        "drops_in_window": len(dq),
                        "window_s": float(self.drop_window_s),
                    })
                # évite le spam: on purge une partie de la fenêtre
                for _ in range(len(dq) // 2):
                    if dq: dq.popleft()
        except Exception:
            logger.exception("[Router] backpressure note_drop failed")

    def _extract_pair_for_drop(self, payload: dict) -> Optional[str]:
        """Best-effort: retourne la paire si elle est déjà présente dans le payload."""
        try:
            if isinstance(payload, Mapping):
                for key in ("pair_key", "pair"):
                    value = payload.get(key)
                    if value:
                        return str(value).upper()
        except Exception:
            return None
        return None

    def _emit_drop_event(self, *, queue_label: str, payload: dict) -> None:
        if not self._event_sink:
            return
        pair = self._extract_pair_for_drop(payload)
        evt = {
            "type": "router_drop",
            "reason": "queue_full",
            "pair": pair,
            "queue_label": queue_label,
            "ts_ms": int(time.time() * 1000),
        }
        try:
            self._event_sink(evt)
        except Exception:
            logger.exception("[Router] event_sink drop dispatch failed")

    # ----------------------- Latest store (full & light) -----------------------
    def _light_from_full(self, b: Dict[str, Any]) -> Dict[str, Any]:
        """Vue optimisée (Flat Dict) sans overhead de conversion."""
        return {
            "best_bid": b.get("best_bid", 0.0),
            "best_ask": b.get("best_ask", 0.0),
            "exchange_ts_ms": b.get("exchange_ts_ms", 0),
            "recv_ts_ms": b.get("recv_ts_ms", 0),
            "active": b.get("active", True),
        }



    async def _store_latest(self, data: Dict[str, Any]) -> None:
        """
        Mise à jour ultra-performante du cache latest_books.
        Assume que data a été normalisé par _validate_and_enrich (Fast Path).
        """
        ex = data.get("exchange")
        pair = data.get("pair_key")
        if not ex or not pair:
            return

        # Accès direct au dictionnaire pour minimiser l'overhead
        if ex not in self._latest_books:
            self._latest_books[ex] = {}
        ex_books = self._latest_books[ex]

        if pair not in ex_books:
            ex_books[pair] = {}
        full = ex_books[pair]
        
        # Mise à jour atomique (CPython)
        full.update(data)

        # Remplacement des fallbacks lents par des accès directs
        # best_bid/best_ask/timestamps sont garantis par _validate_and_enrich
        if "exchange_ts_ms" not in full:
            full["exchange_ts_ms"] = int(full.get("ts_ex_ms") or 0)

        # Mise à jour de la vue légère
        if ex not in self._latest_light:
            self._latest_light[ex] = {}
        self._latest_light[ex][pair] = self._light_from_full(full)

    # --- Accesseurs Engine ---
    def get_latest_orderbooks(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Vue FULL (bids/asks/L1/ts)."""
        return self._latest_books

    def get_latest_orderbooks_light(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Vue LIGHT (L1/ts)."""
        return self._latest_light

    def get_top_of_book(self, exchange: str, pair: str) -> tuple[float, float]:
        ex = (exchange or "").upper()
        pk = (pair or "").replace("-", "").upper()
        d = (self._latest_light.get(ex, {}) or {}).get(pk, {}) or {}
        return float(d.get("best_bid") or 0.0), float(d.get("best_ask") or 0.0)

    def get_orderbook_depth(self, exchange: str, pair: str, depth: int = 10):
        ex = (exchange or "").upper()
        pk = (pair or "").replace("-", "").upper()
        d = (self._latest_books.get(ex, {}) or {}).get(pk, {}) or {}
        asks = list(d.get("asks") or (d.get("orderbook") or {}).get("asks") or [])[: int(depth)]
        bids = list(d.get("bids") or (d.get("orderbook") or {}).get("bids") or [])[: int(depth)]
        return asks, bids

    def get_anchor(self, exchange: str, pair: str, side: str) -> tuple[float, int]:
        """Prix d’ancre et timestamp pour staleness guard (ms). BUY -> best_ask ; SELL -> best_bid"""
        ex = (exchange or "").upper()
        pk = (pair or "").replace("-", "").upper()
        d = (self._latest_light.get(ex, {}) or {}).get(pk, {}) or {}
        if str(side or "").upper() == "BUY":
            price = float(d.get("best_ask") or 0.0)
        else:
            price = float(d.get("best_bid") or 0.0)
        ts = int(d.get("exchange_ts_ms") or d.get("recv_ts_ms") or 0)
        return price, ts

    def _new_deque_for(self, ex: str, *, key: str) -> deque:
        """
        Crée un deque pour le cache par exchange, dimensionné via RouterCfg.deque_maxlen_per_ex.

        Gouvernance P0 :
          - BotConfig.router.deque_maxlen_per_ex est la source unique.
          - DEFAULT sert de base pour tous les exchanges non explicitement configurés.
        """
        exu = ex.upper()
        per_ex: Mapping[str, int] = {}

        cfg = self.router_cfg
        if cfg is not None:
            try:
                per_ex = getattr(cfg, "deque_maxlen_per_ex", {}) or {}
            except Exception:
                per_ex = {}

        default = int(per_ex.get("DEFAULT", getattr(self, "coalesce_maxlen", 8)))
        mx = int(per_ex.get(exu, default))
        return deque(maxlen=mx)

    def get_vol_queues_by_exchange(self) -> Dict[str, asyncio.Queue]:
        """Retourne {'BINANCE': q, 'COINBASE': q, 'BYBIT': q} pour le flux vol."""
        out = {}
        for key, bucket in self.out_queues.items():
            if not key.startswith("cex:"):
                continue
            q = bucket.get("vol")
            if q:
                out[key.split(":", 1)[1]] = q
        return out

    def get_slip_queues_by_exchange(self) -> Dict[str, asyncio.Queue]:
        """Retourne {'BINANCE': q, 'COINBASE': q, 'BYBIT': q} pour le flux slippage."""
        out = {}
        for key, bucket in self.out_queues.items():
            if not key.startswith("cex:"):
                continue
            q = bucket.get("slip")
            if q:
                out[key.split(":", 1)[1]] = q
        return out

    # Hook optionnel, callable depuis les clients WS
    def note_ws_reconnect(self, exchange: str, *, reason: str = "listener_reconnect") -> None:
        ex = str(exchange).upper()
        safe_inc(
            WS_RECONNECTS_TOTAL,
            "ws_reconnects_total",
            "note_ws_reconnect",
            exchange=ex,
            reason=str(reason),
        )

    def note_ws_backpressure(self, exchange: str, *, reason: str, drops: int = 1, last_error: Optional[str] = None) -> None:
        ex = str(exchange).upper()
        self._ws_backpressure_state[ex] = {
            "ts": time.time(),
            "reason": reason,
            "drops": int(drops),
            "last_error": last_error,
        }
        self._ws_backpressure_until = time.monotonic() + self.ws_source_backpressure_cooldown_s
        for _ in range(max(1, int(drops))):
            safe_inc(
                ROUTER_DROPPED_TOTAL,
                "router_dropped_total",
                "note_ws_backpressure",
                queue="health",
                reason="queue_full",
            )
        self._bp_note_drop("health", reason="queue_full")

     # ----------------------------- Purge -----------------------------
    def _purge_old_snapshots(self) -> None:
        now_ms = int(time.time() * 1000)
        purge_threshold_ms = int(self.purge_threshold.total_seconds() * 1000)
        
        for pair in list(self.buffer.keys()):
            for ex in list(self.buffer[pair].keys()):
                ts_ex_ms = self.buffer[pair][ex]["ts_ex_ms"]
                if now_ms - ts_ex_ms > purge_threshold_ms:
                    del self.buffer[pair][ex]
                self._update_pair_queue_depth(pair, ex, 0)
            if not self.buffer.get(pair):
                self.buffer.pop(pair, None)

    # --- PATCH B1: alias attendu par _purge_loop() ---
    def _purge_stale_pairs(self) -> None:
        # P1: Purge globale des caches latest si trop vieux
        mono_now_ns = time.monotonic_ns()
        
        # On purge tout ce qui a plus de 3 secondes du cache Router
        # (Seuil réduit de 5s à 3s pour limiter les rejets book_stale inutiles au Scanner)
        for ex in list(self._latest_books.keys()):
            for pair in list(self._latest_books[ex].keys()):
                snap = self._latest_books[ex][pair]
                
                # Priorité au monotonic clock pour le nettoyage du cache
                recv_mono_ns = snap.get("recv_mono_ns")
                if recv_mono_ns is not None:
                    age_ms = int((mono_now_ns - int(recv_mono_ns)) / 1_000_000)
                else:
                    age_ms = 0
                
                if age_ms > 3000:
                    del self._latest_books[ex][pair]
                    if pair in self._latest_light.get(ex, {}):
                        del self._latest_light[ex][pair]

    # --- /PATCH B1 ---

    async def _purge_loop(self) -> None:
        flush_every_ms = max(5, int(getattr(self, "coalesce_window_ms", 20)))
        hb_every_ms = 1000
        last_flush_ms = last_hb_ms = int(time.monotonic() * 1000)

        # P0 Marché Public — backpressure piloté par BotConfig.router.backpressure
        bp_cfg = self.router_cfg
        bp: Dict[str, Any] = {}
        if bp_cfg is not None:
            # RouterCfg.backpressure est la source unique
            try:
                bp = getattr(bp_cfg, "backpressure", {}) or {}
            except Exception:
                bp = {}


        wm_ratio = float(bp.get("HIGH_WM_RATIO", 0.70))
        bump_ms = int(bp.get("BP_COALESCE_BUMP_MS", 8))
        grow = int(bp.get("BP_DEQUE_GROW", 8))
        cooldown = float(bp.get("COOLDOWN_S", 5.0))
        bp_until = 0.0

        # Valeurs de base de coalescing / profondeur, pilotées par la cfg Router
        base_coalesce_ms = int(getattr(self, "coalesce_window_ms", 20))
        base_maxlen = int(getattr(self, "coalesce_maxlen", 8))

        while getattr(self, "_running", False):
            now_ms = int(time.monotonic() * 1000)

            # FLUSH
            if now_ms - last_flush_ms >= flush_every_ms:
                last_flush_ms = now_ms
                deadline = time.monotonic() + 0.002
                for pair in list(self._coalesce_buckets.keys()):
                    await self._flush_pair(pair)
                    if time.monotonic() > deadline:
                        break

            # --- PATCH B2: dans _purge_loop(), remplacer le bloc HEARTBEAT ---
            # HEARTBEAT (optionnel)
            if now_ms - last_hb_ms >= hb_every_ms:
                last_hb_ms = now_ms
                try:
                    self._maybe_emit_heartbeats()
                except Exception:
                    pass

            # PURGE STALES
            try:
                self._purge_stale_pairs()
            except Exception:
                pass

            # --- Backpressure: auto-relax ---
            # Si au moins une route ≥ HIGH_WM_RATIO, on élargit coalescing & deques temporairement
            high = False
            for route, qmap in (self.out_queues or {}).items():
                if isinstance(qmap, dict):
                    for name, q in qmap.items():
                        if isinstance(q, asyncio.Queue) and q.qsize() >= int(q.maxsize * wm_ratio):
                            high = True
                            break
                if high: break
            deterministic = bool(
                getattr(self.router_cfg, "deterministic_backpressure", False)
            ) if self.router_cfg is not None else False

            ws_bp_active = bool(self._ws_backpressure_until and time.monotonic() < self._ws_backpressure_until)
            if not deterministic:
                if high or ws_bp_active:
                    self.coalesce_window_ms = base_coalesce_ms + bump_ms
                    self.coalesce_maxlen = base_maxlen + grow
                    bp_until = max(time.monotonic() + cooldown, self._ws_backpressure_until or 0.0)
                elif bp_until and time.monotonic() > bp_until:
                    # retour normal
                    self.coalesce_window_ms = base_coalesce_ms
                    self.coalesce_maxlen = base_maxlen
                    bp_until = 0.0
                    if self._ws_backpressure_until and time.monotonic() >= self._ws_backpressure_until:
                        self._ws_backpressure_until = 0.0

            await asyncio.sleep(0.001)

    # ------------------------- Coalescing par paire -------------------------
    # PATCH A — _coalesce_enqueue : éviter le double comptage + noter backpressure avant append
    # => remplace entièrement la méthode existante
    def _coalesce_enqueue(self, ev: Dict[str, Any]) -> None:
        """
        P2: Enqueue coalescing — laisse _flush_pair_after recalculer dynamiquement la fenêtre.
        """
        pair = ev["pair_key"]
        ex = ev["exchange"]
        bucket = self._coalesce_buckets.setdefault(pair, {})
        dq = bucket.get(ex)
        if dq is None:
            dq = deque(maxlen=self.coalesce_maxlen)
            bucket[ex] = dq

        # Nom de queue explicite pour la métrique
        qname = "combo"

        # backpressure counters
        if len(dq) == dq.maxlen:
            safe_inc(
                ROUTER_DROPPED_TOTAL,
                "router_dropped_total",
                "_coalesce_enqueue",
                queue=qname,
                reason="queue_full",
            )
            self._bp_note_drop(qname, reason="queue_full")

        elif len(dq) >= 1:
            safe_inc(
                ROUTER_DROPPED_TOTAL,
                "router_dropped_total",
                "_coalesce_enqueue",
                queue=qname,
                reason="dedup_coalesce",
            )
            self._bp_note_drop(qname, reason="dedup_coalesce")

        dq.append(ev)
        self._update_pair_queue_depth(pair, ex, len(dq))

        # planifie un flush si non déjà programmé
        if pair not in self._coalesce_tasks or self._coalesce_tasks[pair].done():
            self._coalesce_tasks[pair] = asyncio.create_task(
                self._flush_pair_after(pair, self.coalesce_window_ms / 1000.0)
            )

    def _deployment_mode(self) -> str:
        g_cfg = getattr(self.bot_cfg, "g", None) if self.bot_cfg is not None else None
        mode = getattr(g_cfg, "deployment_mode", None) if g_cfg else None
        mode_norm = str(mode).strip().upper() if mode else ""
        if not mode_norm:
            mode_norm = "EU_ONLY"
        if mode_norm not in {"EU_ONLY", "SPLIT", "JP_ONLY"}:
            safe_inc(
                ROUTER_DEPLOYMENT_MODE_INVALID_TOTAL,
                "router_deployment_mode_invalid_total",
                "_deployment_mode",
                mode=mode_norm or "EMPTY",
            )
            mode_norm = "EU_ONLY"
        if not self._deployment_mode_logged:
            self._deployment_mode_logged = True
        return mode_norm

    def _resolve_stale_limit_ms(self, exchange: str) -> int:
        stale_cfg_ms = int(getattr(self, "stale_cfg_ms", 1000) or 1000)
        exu = str(exchange or "").upper()
        mode_key = self._deployment_mode()
        slo_limit_ms = None
        try:
            cfg = getattr(self, "bot_cfg", None)
            slo_map = getattr(cfg, "slo", None) if cfg is not None else None
            if slo_map:
                per_ex = slo_map.get(mode_key) or {}
                path_slo = per_ex.get(exu)
                if path_slo is not None and getattr(path_slo, "public", None) is not None:
                    l2_fresh_max_s = float(getattr(path_slo.public, "l2_fresh_max_s", 0.0) or 0.0)
                    if l2_fresh_max_s > 0.0:
                        slo_limit_ms = int(l2_fresh_max_s * 1000.0)
        except Exception:
            return stale_cfg_ms

        if slo_limit_ms is None:
            warn_key = (mode_key, exu)
            if warn_key not in self._missing_slo_warned:
                self._missing_slo_warned.add(warn_key)
                safe_inc(
                    ROUTER_SLO_MISSING_TOTAL,
                    "router_slo_missing_total",
                    "_resolve_stale_limit_ms",
                    mode=mode_key,
                    exchange=exu,
                )
            return stale_cfg_ms

        resolved = min(stale_cfg_ms, slo_limit_ms)

    # ------------------------- Coalescing par paire -------------------------

    async def _flush_pair_after(self, pair: str, delay_s: float) -> None:
        """
        P2: Coalescing pair-aware:
          - bonus de coalescing pour COINBASE en mode SPLIT (+5..15ms),
          - fenêtre recalculée au moment du flush,
          - piloté par BotConfig (router.* + g.deployment_mode).
        """
        task = asyncio.current_task()
        try:
            # Fenêtre de base
            base_ms = int(getattr(self, "coalesce_window_ms", 20))

            # --- Lecture robuste du mode de déploiement -------------------------
            dep_mode = self._deployment_mode()
            split = dep_mode == "SPLIT"
            # --- Bonus CB (5..15ms) si SPLIT et si COINBASE présent -------------
            cb_bump_ms = 0
            if split:
                bump_src = (
                               getattr(self.router_cfg, "cb_coalesce_bump_ms",
                                       None) if self.router_cfg is not None else None
                           ) or 10
                try:
                    cb_bump_ms = max(5, min(15, int(bump_src)))
                except Exception:
                    cb_bump_ms = 10

                # Bump seulement si la paire agrège des évènements COINBASE
                bucket = self._coalesce_buckets.get(pair) or {}
                has_cb = any(str(ex).upper() == "COINBASE" for ex in bucket.keys())
                if not has_cb:
                    cb_bump_ms = 0

            eff_ms = max(1, int(base_ms + cb_bump_ms))
            await asyncio.sleep(eff_ms / 1000.0)
            await self._flush_pair(pair)

        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("[Router] _flush_pair_after error")
        finally:
            if self._coalesce_tasks.get(pair) is task:
                self._coalesce_tasks.pop(pair, None)

    # === MarketDataRouter ===
    async def _flush_pair(self, pair_key: str) -> None:
        """
        Coalesce window -> take last by CEX -> fan-out per-CEX (vol/slip/health)
        + push (optionnel) vers le Scanner.
        """
        try:
            buckets = self._coalesce_buckets.get(pair_key) or {}
            if not buckets:
                return

            # 1) dernier snapshot par CEX
            latest_by_ex: Dict[str, dict] = {}
            for ex, dq in buckets.items():
                if not dq:
                    continue
                ev = dq[-1]
                # normalisation minimaliste
                ev["exchange"] = str(ex).upper()
                ev["pair_key"] = pair_key
                latest_by_ex[ev["exchange"]] = ev

            # 2) fan-out per-CEX (rate-limited par pair)
            # IMPORTANT: on évite de publier à chaque flush (coalesce window), sinon les queues vol/slip saturent.
            # Les helpers _per_cex_* appliquent capHz + heartbeat + on-change par (exchange,pair).
            for ex_up, ev in latest_by_ex.items():
                try:
                    self._per_cex_vol(ev)
                except Exception:
                    logger.exception("[Router] fanout vol")
                try:
                    self._per_cex_slip(ev)
                except Exception:
                    logger.exception("[Router] fanout slip")
                try:
                    self._per_cex_health(ev)
                except Exception:
                    logger.exception("[Router] fanout health")

            # 3) Push vers Scanner (optionnel, via queue dédiée)
            if self.push_to_scanner:
                for ev in latest_by_ex.values():
                    self._enqueue_scanner(ev)

            # 4) reset fenêtre
            self._coalesce_buckets[pair_key] = {
                ex: deque(maxlen=self.coalesce_maxlen) for ex in latest_by_ex.keys()
            }
        except Exception:
            logger.exception("[Router] _flush_pair error")

    def _configure_topic_rates(self, cfg=None) -> None:
        """
        Charge les max_hz par CEX/topic depuis BotConfig et prépare les intervalles min (ms).
        """
        cfg = cfg or self.router_cfg
        spec = (getattr(cfg, "topic_max_hz", None) or {}) if cfg is not None else {}

        self._topic_min_interval_ms = {}
        for ex in self.exchanges:  # liste des CEX actifs
            exu = str(ex).upper()
            m = dict(spec.get("DEFAULT", {}))
            m.update(dict(spec.get(exu, {})))  # override par CEX
            self._topic_min_interval_ms[exu] = {
                t: int(1000.0 / max(0.1, float(hz))) for t, hz in m.items()
            }

        # registres de dernier publish (ms)
        self._last_pub_ms = {exu: {"vol": 0, "slip": 0, "health": 0} for exu in self._topic_min_interval_ms.keys()}

    def _can_publish_topic(self, ex: str, topic: str, now_ms: int) -> bool:
        exu = str(ex).upper()
        min_iv = self._topic_min_interval_ms.get(exu, {}).get(topic)
        if not min_iv:
            return True
        last = self._last_pub_ms.get(exu, {}).get(topic, 0)
        return (now_ms - last) >= min_iv

    def _publish_to_bus(self, ex: str, ev: dict) -> None:
        """
        P2: publication per-CEX avec respect des cadences max_hz par topic et backpressure-aware.
        """
        exu = str(ex).upper()
        now_ms = int(time.monotonic() * 1000)
        per_cex_key = self._cex_key(exu)
        qmap = self.out_queues.get(per_cex_key, {})

        # VOL
        if self._can_publish_topic(exu, "vol", now_ms):
            payload_vol = {
                "exchange": exu,
                "pair_key": ev.get("pair_key"),
                "best_bid": ev.get("best_bid"),
                "best_ask": ev.get("best_ask"),
                "mid": (ev.get("best_bid") + ev.get("best_ask")) / 2.0 if ev.get("best_bid") and ev.get(
                    "best_ask") else None,
                "exchange_ts_ms": ev.get("exchange_ts_ms"),
                "recv_ts_ms": ev.get("recv_ts_ms"),
                "active": ev.get("active", True),
            }
            self._try_put(qmap.get("vol"), f"{per_cex_key}.vol", payload_vol)
            self._note_publish_topic(exu, "vol", now_ms)

        # SLIP
        if self._can_publish_topic(exu, "slip", now_ms):
            payload_slip = {
                "exchange": exu,
                "pair_key": ev.get("pair_key"),
                "orderbook": ev.get("orderbook"),
                "top_bid_vol": ev.get("top_bid_vol"),
                "top_ask_vol": ev.get("top_ask_vol"),
                "exchange_ts_ms": ev.get("exchange_ts_ms"),
                "recv_ts_ms": ev.get("recv_ts_ms"),
                "active": ev.get("active", True),
            }
            self._try_put(qmap.get("slip"), f"{per_cex_key}.slip", payload_slip)
            self._note_publish_topic(exu, "slip", now_ms)

        # HEALTH (léger, heartbeat)
        if self._can_publish_topic(exu, "health", now_ms):
            payload_health = {
                "exchange": exu,
                "pair_key": ev.get("pair_key"),
                "seq": ev.get("seq"),
                "recv_ts_ms": ev.get("recv_ts_ms"),
                "exchange_ts_ms": ev.get("exchange_ts_ms"),
            }
            self._try_put(qmap.get("health"), f"{per_cex_key}.health", payload_health)
            self._note_publish_topic(exu, "health", now_ms)

    def _note_publish_topic(self, ex: str, topic: str, now_ms: int) -> None:
        exu = str(ex).upper()
        self._last_pub_ms.setdefault(exu, {})
        self._last_pub_ms[exu][topic] = now_ms

    async def _flush_all(self) -> None:
        """
        Flush final (sync) de tous les buckets au stop.
        """
        for pair in list(self._coalesce_buckets.keys()):
            try:
                await self._flush_pair(pair)
            except Exception:
                logger.exception("[Router] flush_all error on %s", pair)
        self._coalesce_buckets.clear()

    # ------------------------- Validation event -------------------------
    def _validate_and_enrich(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Validation hybride (P0) : Fast-Path (types natifs) + Slow-Path (Pydantic sampling).
        Élimine l'overhead Pydantic sur 99% des events.
        """
        try:
            if not isinstance(data, dict):
                return None

            if data.get(self._STOP_SENTINEL_KEY):
                return {self._STOP_SENTINEL_KEY: True}

            # 1. FAST PATH : Extraction et validation minimale (types natifs)
            ex = str(data.get("exchange") or "").upper()
            pair = str(data.get("pair_key") or data.get("symbol") or "").replace("-", "").upper()
            
            # Top-of-book (float natif)
            try:
                bid = float(data.get("best_bid") or 0.0)
                ask = float(data.get("best_ask") or 0.0)
            except (ValueError, TypeError):
                bid, ask = 0.0, 0.0

            if not ex or not pair or bid <= 0 or ask <= 0 or ask < bid:
                # Anomalie structurelle détectée -> on laisse Pydantic diagnostiquer ou on drop
                return None

            # Normalisation directe (In-place)
            data["exchange"] = ex
            data["pair_key"] = pair
            
            now_ms = int(time.time() * 1000)
            
            # Timestamps uniformes (int ms) - Pas de datetime ici
            ex_ts = int(data.get("exchange_ts_ms") or 0)
            recv_ts = int(data.get("recv_ts_ms") or now_ms)
            data["recv_ts_ms"] = recv_ts
            
            # 2. SLOW PATH (Sampling 1/N, Snapshots, Anomalies)
            self._val_counter += 1
            
            # Heuristique snapshot: bcp de niveaux
            ob = data.get("orderbook") or {}
            is_large_ob = len(ob.get("bids", [])) > 20
            
            do_slow_val = (
                (self._val_counter % self._val_sample_rate == 0) or 
                is_large_ob or 
                data.get("is_snapshot")
            )

            if MarketEvent and do_slow_val:
                try:
                    # On valide mais on ne garde pas l'objet Pydantic (trop cher de model_dump)
                    MarketEvent(**data)
                except ValidationError:
                    # Si la validation échoue, on drop l'event
                    safe_inc(
                        ROUTER_DROPPED_TOTAL,
                        "router_dropped_total",
                        "validate",
                        queue="validate",
                        reason="pydantic_fail",
                    )
                    return None
            
            # Détection Clock Drift
            if ex_ts > 0:
                diff = ex_ts - now_ms
                if abs(diff) > 1000:
                    if not hasattr(self, "_clock_offset_ms"):
                        self._clock_offset_ms = diff
                        try:
                            if ROUTER_CLOCK_OFFSET_MS is not None:
                                ROUTER_CLOCK_OFFSET_MS.labels(exchange=ex).set(float(diff))
                        except Exception:
                            pass
            
            # Mode DRY_RUN
            is_dry = False
            if self.bot_cfg is not None:
                is_dry = str(getattr(self.bot_cfg, "MODE", "")).upper() == "DRY_RUN"
            
            mono_now_ns = time.monotonic_ns()
            recv_mono = data.get("recv_mono_ns")
            if recv_mono is None:
                data["recv_mono_ns"] = recv_mono = mono_now_ns

            if is_dry:
                data["active"] = True
                data["recv_ts_ms"] = now_ms
            
            # Validation de Fraîcheur (P0)
            stale_limit = self._resolve_stale_limit_ms(ex)
            age = (mono_now_ns - int(recv_mono)) // 1_000_000
            data["age_ms"] = age # On l'injecte pour start()

            # P0 Observabilité
            try:
                from modules.obs_metrics import ROUTER_EVENT_AGE_MS
                if ROUTER_EVENT_AGE_MS:
                    ROUTER_EVENT_AGE_MS.labels(exchange=ex).observe(age)
            except Exception:
                pass
            
            # Startup Grace Period (P0)
            startup_age = (time.monotonic() - getattr(self, "_start_mono", 0))
            is_booting = startup_age < 5.0
            
            if not is_booting and age > stale_limit:
                return None

            return data

        except Exception as e:
            safe_inc(
                ROUTER_DROPPED_TOTAL,
                "router_dropped_total",
                "_validate_and_enrich",
                exchange="UNKNOWN",
                reason="validate_exception",
            )
            return None


    # -------------------------- Fan-out helpers --------------------------
    def _try_put(self, q: asyncio.Queue | None, route_name: str, payload: dict) -> None:
        if not isinstance(q, asyncio.Queue):
            return

        queue_label = self._queue_label(route_name)

        # default HIGH_WM_RATIO (can be overridden by cfg.router.backpressure.HIGH_WM_RATIO)
        wm_ratio = 0.70
        try:
            cfg = self.router_cfg
            if cfg is not None:
                bp = getattr(cfg, "backpressure", {}) or {}
                wm_ratio = float(bp.get("HIGH_WM_RATIO", wm_ratio))
        except Exception:
            wm_ratio = 0.70

        # Drop si au-dessus du watermark (ex: 70%) pour éviter la saturation totale.
        # P0: au lieu de "drop + return" (qui fige la queue haut et rend le flux stale),
        # on drop l'OLDEST puis on tente de push le LATEST (best-effort).
        if q.maxsize and q.qsize() >= int(q.maxsize * wm_ratio):
            safe_inc(
                ROUTER_QUEUE_HIGH_WATERMARK_TOTAL,
                "router_queue_high_watermark_total",
                "_try_put",
                queue=queue_label,
            )
            # P0: On utilise maintenant les compteurs consolidés
            try:
                from modules.obs_metrics import ROUTER_DROPPED_TOTAL
                if ROUTER_DROPPED_TOTAL:
                    ROUTER_DROPPED_TOTAL.labels(queue=queue_label, reason="queue_full").inc()
            except Exception:
                pass

            try:
                self._emit_drop_event(queue_label=queue_label, payload=payload)
            except Exception:
                pass
            try:
                self._bp_note_drop(queue_label, reason="queue_full")
            except Exception:
                pass

            # drop oldest
            try:
                _ = q.get_nowait()
                try:
                    q.task_done()
                except Exception:
                    pass
            except Exception:
                pass

            # keep latest (best-effort)
            try:
                q.put_nowait(payload)
            except Exception:
                pass
            return

        # Normal path: try to enqueue. If full, drop oldest then keep latest.
        try:
            q.put_nowait(payload)
            return
        except asyncio.QueueFull:
            pass
        except Exception:
            # Unknown error -> count as drop and bail
            try:
                from modules.obs_metrics import ROUTER_DROPPED_TOTAL
                if ROUTER_DROPPED_TOTAL:
                    ROUTER_DROPPED_TOTAL.labels(queue=queue_label, reason="exception").inc()
            except Exception:
                pass
            try:
                self._emit_drop_event(queue_label=queue_label, payload=payload)
            except Exception:
                pass
            return

        # QueueFull: drop oldest then retry put latest
        try:
            _ = q.get_nowait()
            try:
                q.task_done()
            except Exception:
                pass
        except Exception:
            pass

        try:
            q.put_nowait(payload)
            try:
                from modules.obs_metrics import ROUTER_DROPPED_TOTAL
                if ROUTER_DROPPED_TOTAL:
                    ROUTER_DROPPED_TOTAL.labels(queue=queue_label, reason="queue_full").inc()
            except Exception:
                pass
            try:
                self._emit_drop_event(queue_label=queue_label, payload=payload)
            except Exception:
                pass
        except asyncio.QueueFull:
            try:
                from modules.obs_metrics import ROUTER_DROPPED_TOTAL
                if ROUTER_DROPPED_TOTAL:
                    ROUTER_DROPPED_TOTAL.labels(queue=queue_label, reason="queue_full").inc()
            except Exception:
                pass
            try:
                self._emit_drop_event(queue_label=queue_label, payload=payload)
            except Exception:
                pass
        except Exception:
            try:
                from modules.obs_metrics import ROUTER_DROPPED_TOTAL
                if ROUTER_DROPPED_TOTAL:
                    ROUTER_DROPPED_TOTAL.labels(queue=queue_label, reason="exception").inc()
            except Exception:
                pass
            try:
                self._emit_drop_event(queue_label=queue_label, payload=payload)
            except Exception:
                pass

    def _publish_combo(self, a: str, b: str, payload: Dict[str, Any]) -> None:
        if not self.publish_combo_to_bus:
            return
        required = ("exchange", "pair_key", "best_bid", "best_ask", "exchange_ts_ms", "recv_ts_ms", "age_ms", "active")
        missing = [k for k in required if payload.get(k) is None]
        if missing:
            safe_inc(
                ROUTER_DROPPED_TOTAL,
                "router_dropped_total",
                "_publish_combo",
                queue="combo",
                reason="schema_missing_field",
            )
            return

        combo_key = self._combo_key(a, b)
        qs = self.out_queues.get(combo_key) or {}
        self._try_put(qs.get("scanner"), f"{combo_key}.scanner", payload)

        # Hook observabilité optionnel
        if callable(router_on_combo_event):
            try:
                router_on_combo_event(combo_key, payload)  # type: ignore
            except Exception as e:
                report_nonfatal("MarketDataRouter", "router_on_combo_event_failed", e)

    def _publish_cex(self, ex: str, kind: str, payload: Dict[str, Any]) -> None:
        k = self._cex_key(ex)
        qs = self.out_queues.get(k) or {}
        self._try_put(qs.get(kind), f"{k}.{kind}", payload)
        # MAJ métriques pour ce CEX
        self._update_queue_depth_metrics_for(ex)

    def _enqueue_scanner(self, payload: Dict[str, Any]) -> None:
        if not self.push_to_scanner:
            return
        policy = "DROP_OLDEST"
        if self.router_cfg is not None:
            policy = str(
                getattr(self.router_cfg, "scanner_queue_drop_policy", "DROP_OLDEST") or "DROP_OLDEST"
            ).upper()
        try:
            if getattr(self.bot_cfg.obs, "enable_latency_tracing", False):
                payload["router_exit_ns"] = time.perf_counter_ns()
            self._scanner_queue.put_nowait(payload)
            set_pipeline_backlog("router_to_scanner", self._scanner_queue.qsize())
        except asyncio.QueueFull:
            if policy == "DROP_NEW":
                safe_inc(
                    ROUTER_DROPPED_TOTAL,
                    "router_dropped_total",
                    "_enqueue_scanner",
                    queue="scanner",
                    reason="queue_full",
                )
                return
            try:
                _ = self._scanner_queue.get_nowait()
            except Exception:
                pass
            try:
                self._scanner_queue.put_nowait(payload)
                safe_inc(
                    ROUTER_DROPPED_TOTAL,
                    "router_dropped_total",
                    "_enqueue_scanner",
                    queue="scanner",
                    reason="queue_full",
                )
            except asyncio.QueueFull:
                safe_inc(
                    ROUTER_DROPPED_TOTAL,
                    "router_dropped_total",
                    "_enqueue_scanner",
                    queue="scanner",
                    reason="queue_full",
                )

    async def _scanner_worker(self) -> None:
        while self._running:
            try:
                ev = await self._scanner_queue.get()
                set_pipeline_backlog("router_to_scanner", self._scanner_queue.qsize())
                
                # Mesure latence queue (P1)
                tracing_enabled = getattr(self.bot_cfg.obs, "enable_latency_tracing", False)
                if tracing_enabled:
                    now_ns = time.perf_counter_ns()
                    ev["scanner_entry_ns"] = now_ns
                    exit_ns = ev.get("router_exit_ns")
                    if exit_ns:
                        queue_ms = (now_ns - int(exit_ns)) / 1e6
                        if getattr(self.bot_cfg.obs, "enable_segment_metrics_router", True):
                            record_pipeline_latency("router_to_scanner_queue", queue_ms, exchange=ev.get("exchange", "none"))

                # P1: Freshness check immédiat dans le worker scanner
                # Si le backlog a gonflé, on skip les vieux events
                now_mono_ns = time.monotonic_ns()
                recv_mono_ns = ev.get("recv_mono_ns")
                if recv_mono_ns is None:
                    ev["recv_mono_ns"] = recv_mono_ns = now_mono_ns
                age_ms = int((now_mono_ns - int(recv_mono_ns)) / 1_000_000)

                if age_ms > 1500:
                    safe_inc(
                        ROUTER_DROPPED_TOTAL,
                        "router_dropped_total",
                        "_scanner_worker",
                        queue="scanner",
                        reason="stale_backlog",
                    )
                    self._scanner_queue.task_done()
                    continue
            except Exception:
                continue
            ts0 = time.perf_counter_ns()
            ok = False
            reason = "ok"
            try:
                res = self.scanner.update_orderbook(ev)
                if inspect.iscoroutine(res):
                    await res
                ok = True
            except Exception as e:
                reason = "exception"
                report_nonfatal("MarketDataRouter", "scanner_update_failed", e)
            finally:
                mark_router_to_scanner_ts(ts0, route="tri_cex", ok=ok, reason=reason)
                self._scanner_queue.task_done()

    # -------------------------- Per-CEX signals --------------------------
    @staticmethod
    def _ema_update(prev: Optional[float], value: float, dt_ms: float, window_ms: float) -> float:
        """EMA avec alpha = 1 - exp(-dt/window)."""
        if prev is None or prev != prev:  # NaN check
            return float(value)
        try:
            alpha = 1.0 - math.exp(-max(1.0, dt_ms) / max(1.0, window_ms))
        except Exception:
            alpha = 0.2
        return float(prev + alpha * (value - prev))

    def _per_cex_vol(self, ev: Dict[str, Any]) -> None:
        """Émet VolEvent (bps): l1_spread_bps/ema_vol_bps sont exprimés en bps."""
        ex = ev["exchange"]; pair = ev["pair_key"]
        key = (ex, pair)
        now_mono = time.monotonic()
        now_ms = int(time.time() * 1000)

        # --- AJOUT P1 : Throttling (Budget CPU) ---
        # On limite le calcul de volatilité à 5Hz max par paire.
        # Sur 120 paires, cela évite des milliers de calculs inutiles par seconde.
        last_calc = getattr(self, "_vol_last_calc_ts", {}).get(key, 0.0)
        if not hasattr(self, "_vol_last_calc_ts"): self._vol_last_calc_ts = {}
        if (now_mono - last_calc) < (1.0 / self.vol_max_hz):
            return
        self._vol_last_calc_ts[key] = now_mono

        bid, ask = float(ev.get("best_bid", 0.0)), float(ev.get("best_ask", 0.0))
        if bid <= 0 or ask <= 0: return
        val = ((ask - bid) / bid) * 10000.0  # en bps

        ema_prev = self._vol_ema.get(key)
        ema_curr = self._ema_update(ema_prev, val, dt_ms=(now_mono - last_calc)*1000.0, window_ms=self.vol_ema_window_ms)
        self._vol_ema[key] = ema_curr

        # rate limit & on-change
        last_pub_ts = self._vol_last_pub_ts.get(key, 0.0)
        do_publish = False
        changed = False

        last_pub_val = self._vol_last_pub_val.get(key, ema_curr)
        if abs(ema_curr - last_pub_val) >= self.vol_onchange_bps:
            changed = True

        min_interval = 0.1 # 10Hz max pub
        if changed and (now_mono - last_pub_ts) >= min_interval:
            do_publish = True
        elif (now_mono - last_pub_ts) >= self.vol_heartbeat_s:
            do_publish = True
            changed = False  # heartbeat

        if do_publish:
            # P0: Fast-path pour VolEvent sans Pydantic.

            ex_ts_ms = int(ev.get("exchange_ts_ms") or 0)
            
            payload = {
                "exchange": ex,
                "pair_key": pair,
                "symbol": ev.get("symbol"),
                "ema_vol_bps": round(ema_curr, 4),
                "l1_spread_bps": round(val, 4),
                "best_bid": bid,
                "best_ask": ask,
                "mid": (bid + ask) / 2.0,
                "changed": changed,
                "exchange_ts_ms": ex_ts_ms,
                "recv_ts_ms": int(ev.get("recv_ts_ms") or now_ms),
                "age_ms": float(ev.get("age_ms") or 0.0),
                "seq_no": int(now_ms % 2_147_483_647),
                "kind": "vol"
            }
            self._publish_cex(ex, "vol", payload)
            self._vol_last_pub_ts[key] = now_mono
            self._vol_last_pub_val[key] = ema_curr

    def _per_cex_slip(self, ev: Dict[str, Any]) -> None:
        """Proxy slippage très léger (on-change/heartbeat/capHz). Unité: bps (pas fraction)."""
        ex = ev["exchange"]; pair = ev["pair_key"]
        key = (ex, pair)
        now_mono = time.monotonic()
        now_ms = int(time.time() * 1000)

        # --- AJOUT P1 : Throttling (Budget CPU) ---
        # On limite le calcul de slippage à 5Hz max par paire.
        last_calc = getattr(self, "_slip_last_calc_ts", {}).get(key, 0.0)
        if not hasattr(self, "_slip_last_calc_ts"): self._slip_last_calc_ts = {}
        if (now_mono - last_calc) < (1.0 / self.slip_max_hz):
            return
        self._slip_last_calc_ts[key] = now_mono

        # proxy : on réutilise l1_spread_bps comme métrique simple
        metric = float(ev.get("l1_spread_bps") or 0.0)
        if metric < 0.0 or metric > 100000.0:
            safe_inc(
                ROUTER_DROPPED_TOTAL,
                "router_dropped_total",
                "_per_cex_slip",
                queue="slip",
                reason="unit_mismatch",
            )
            return
        last_metric = self._slip_last_metric.get(key, metric)
        last_pub_ts = self._slip_last_pub_ts.get(key, 0.0)
        
        changed = abs(metric - last_metric) >= self.slip_onchange_bps
        do_publish = False

        min_interval = 0.1 # 10Hz max pub
        if changed and (now_mono - last_pub_ts) >= min_interval:
            do_publish = True
        elif (now_mono - last_pub_ts) >= self.slip_heartbeat_s:
            do_publish = True
            changed = False  # heartbeat

        if do_publish:
            # P0: Fast-path pour SlipEvent sans Pydantic.
            bids, asks = (ev.get("orderbook") or {}).get("bids") or [], (ev.get("orderbook") or {}).get("asks") or []
            payload = {
                "exchange": ex,
                "pair_key": pair,
                "symbol": ev.get("symbol"),
                "slip_metric_bps": round(metric, 4),
                "changed": changed,
                "notional_hint": None,
                "orderbook": {"bids": bids, "asks": asks} if (bids or asks) else {},
                "top_bid_vol": float(ev.get("bid_volume") or 0.0),
                "top_ask_vol": float(ev.get("ask_volume") or 0.0),
                "exchange_ts_ms": int(ev.get("exchange_ts_ms") or 0),
                "recv_ts_ms": int(ev.get("recv_ts_ms") or now_ms),
                "kind": "slip"
            }
            self._publish_cex(ex, "slip", payload)
            self._slip_last_pub_ts[key] = now_mono
            self._slip_last_metric[key] = metric


    def _per_cex_health(self, ev: Dict[str, Any]) -> None:
        ex = ev["exchange"]
        now_mono = time.monotonic()
        now_ms = int(time.time() * 1000)
        last = self._health_last_pub_ts.get(ex, 0.0)
        if (now_mono - last) < self.health_heartbeat_s:
            return
            
        # P0: Fast-path pour HealthEvent sans Pydantic.
        payload = {
            "exchange": ex,
            "last_ex_ts_ms": int(ev.get("exchange_ts_ms") or 0),
            "last_recv_ts_ms": int(ev.get("recv_ts_ms") or now_ms),
            "age_ms": float(ev.get("age_ms") or 0.0),
            "pairs_seen": list((self._latest_light.get(ex, {}) or {}).keys()),
            "changed": False,
            "kind": "health"
        }
        self._publish_cex(ex, "health", payload)
        self._health_last_pub_ts[ex] = now_mono

    def _maybe_emit_heartbeats(self) -> None:
        """Émet des heartbeats per-CEX en l’absence de changements notables et met à jour les gauges."""
        now_mono = time.monotonic()
        for ex, pairs in (self._latest_light or {}).items():
            if (now_mono - self._health_last_pub_ts.get(ex, 0.0)) >= self.health_heartbeat_s:
                # P0: Fast-path pour HealthEvent heartbeat sans Pydantic.
                payload = {
                    "exchange": ex,
                    "last_ex_ts_ms": max([int(v.get("exchange_ts_ms") or 0) for v in pairs.values()] or [0]),
                    "last_recv_ts_ms": max([int(v.get("recv_ts_ms") or 0) for v in pairs.values()] or [0]),
                    "age_ms": 0.0,
                    "pairs_seen": list(pairs.keys()),
                    "changed": False,
                    "kind": "health"
                }

                self._publish_cex(ex, "health", payload)
                self._health_last_pub_ts[ex] = now_mono
            # MAJ gauges même sans publication (ex: calme plat)
            self._update_queue_depth_metrics_for(ex)

    # ----------------------------- Push vers Scanner -----------------------------
    async def _push_to_scanner(self, ev_a: Dict[str, Any], ev_b: Dict[str, Any]) -> None:
        """Push ultra-low-latency de deux L1 vers le scanner (sync/async)."""
        ts0 = time.perf_counter_ns()
        ok = False
        reason = "ok"
        try:
            upd = getattr(self.scanner, "update_orderbook", None)
            if inspect.iscoroutinefunction(upd):
                await upd(ev_a)
                await upd(ev_b)
            else:
                upd(ev_a)  # type: ignore
                upd(ev_b)  # type: ignore
            ok = True
        except Exception as e:
            reason = "exception"
            report_nonfatal("MarketDataRouter", "scanner_update_failed", e)
            logger.exception("[Router] update_orderbook error")
        finally:
            # route logique : tri_cex (aligné avec obs)
            mark_router_to_scanner_ts(ts0, route="tri_cex", ok=ok, reason=reason)

    # ----------------------------- Main loop -----------------------------
    # PATCH D — WSFanInMux.start : sources dynamiques, pas de snapshot de la liste des clés
    # => remplace la méthode 'start' du mux si tu veux pouvoir register() après start()


    async def start(self) -> None:
        """
        P0/P1: boucle principale du Router
          - lit self.in_queue (events WS normalisés)
          - _validate_and_enrich → _coalesce_enqueue
          - purge/flush dans une tâche dédiée
        """
        if getattr(self, "_running", False):
            return
        self._running = True
        if self.push_to_scanner and (self._scanner_task is None or self._scanner_task.done()):
            self._scanner_task = asyncio.create_task(self._scanner_worker(), name="router-scanner")
        # maintenance loop (flush/heartbeats/purge)
        if not getattr(self, "_purge_task", None) or self._purge_task.done():
            self._purge_task = asyncio.create_task(self._purge_loop(), name="router-purge")

        try:
            while self._running:
                item = await self.in_queue.get()
                self._events_in += 1
                try:
                    if isinstance(item, dict) and item.get("__stop__"):
                        break
                    if isinstance(item, dict) and item.get("__ws_reconnect__"):
                        payload = item
                        ex = payload.get("exchange") or payload.get("ex") or "UNKNOWN"
                        self.note_ws_reconnect(ex, reason=str(payload.get("reason", "listener_reconnect")))
                        continue
                    if isinstance(item, dict) and item.get("__ws_backpressure__"):
                        payload = item
                        ex = payload.get("exchange") or payload.get("ex") or "UNKNOWN"
                        self.note_ws_backpressure(
                            ex,
                            reason=str(payload.get("reason", "ws_source")),
                            drops=int(payload.get("drops") or 1),
                            last_error=payload.get("error"),
                        )
                        continue
                    if isinstance(item, dict) and item.get("__ws_error__"):
                        payload = item
                        ex = payload.get("exchange") or payload.get("ex") or "UNKNOWN"
                        safe_inc(
                            ROUTER_DROPPED_TOTAL,
                            "router_dropped_total",
                            "start",
                            queue="health",
                            reason="exception",
                        )
                        continue



                    start_proc = time.perf_counter_ns()
                    ev = self._validate_and_enrich(item)
                    if not ev:
                        continue

                    # Instrumentation latence (P1)
                    tracing_enabled = should_trace_latency(self.bot_cfg)

                    if tracing_enabled:
                        ev["router_entry_ns"] = start_proc

                    # --- compensation de Skew (Ticket P1-1) ---
                    stale_limit_ms = self._resolve_stale_limit_ms(ev.get("exchange"))
                    # On utilise age_ms calculé par _validate_and_enrich car il compense le skew
                    age_ms = float(ev.get("age_ms") or 0.0)
                    is_stale = age_ms > float(stale_limit_ms)

                    # P0: Grâce initiale au démarrage du Router (pendant les 500 premiers messages)
                    # pour éviter les faux-positifs staleness avant calibration complète.
                    if getattr(self, "_events_in", 0) < 500:
                        is_stale = False

                    if is_stale:
                        safe_inc(
                            ROUTER_DROPPED_TOTAL,
                            "router_dropped_total",
                            "start",
                            queue="combo",
                            reason="stale_source",
                        )
                        self._events_ignored_stale += 1
                        continue
                    # --- /PATCH C2 ---

                    # P0: Mise à jour du cache latest (indispensable pour RM et Dashboard)
                    # On le fait APRÈS le guard staleness pour éviter de polluer le cache.
                    await self._store_latest(ev)
                    
                    # Marquer le temps de sortie pour la mesure de latence inter-module
                    ev["publish_ts_ms"] = int(time.time() * 1000)

                    # Mesure de la latence interne du Router (en ms)
                    now_ns = time.perf_counter_ns()
                    proc_time_ms = (now_ns - start_proc) / 1e6

                    if tracing_enabled and getattr(self.bot_cfg.obs, "enable_segment_metrics_router", True):
                        record_pipeline_latency("router_proc", proc_time_ms, exchange=ev.get("exchange", "none"))

                    self._coalesce_enqueue(ev)

                except Exception:
                    safe_inc(
                        ROUTER_DROPPED_TOTAL,
                        "router_dropped_total",
                        "start",
                        queue="combo",
                        reason="exception",
                    )
                    logger.exception("[Router] start: event error")
                    if self.router_cfg and getattr(self.router_cfg, "fail_fast_on_event_exception", False):
                        raise
                finally:
                    try:
                        self.in_queue.task_done()
                    except Exception:
                        pass

        finally:
            self._running = False
            try:
                await self._flush_all()
            except Exception:
                logger.exception("[Router] stop: flush_all failed")
            if getattr(self, "_purge_task", None):
                self._purge_task.cancel()
                try:
                    await self._purge_task
                except asyncio.CancelledError:
                    pass
            self._purge_task = None
            if self._scanner_task:
                self._scanner_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._scanner_task
            self._scanner_task = None

    # --- market_data_router.py (dans class MarketDataRouter) ---
    def get_orderbooks(self):
        """
        Alias tolérant: renvoie le dernier snapshot d'orderbooks.
        Compat avec les modules qui attendent get_orderbooks().
        """
        # P1: On force un nettoyage rapide des snapshots avant de les servir en mode pull
        # pour éviter que le scanner pull du vieux cache sans s'en rendre compte.
        self._purge_stale_pairs()
        
        fn = getattr(self, "get_latest_orderbooks", None)
        return fn() if callable(fn) else {}

    # --- market_data_router.py (dans class MarketDataRouter) ---


    async def stop(self, timeout_s: float = 5.0) -> None:
        """
        Arrêt propre du Router:
          - pousse une sentinelle dans la file d'entrée (si présente),
          - annule/attend la ou les tasks de travail,
          - libère les ressources.
        Idempotent, best-effort.
        """
        # 1) Sentinelle de fin (si file connue)
        try:
            q = getattr(self, "in_queue", None) or getattr(self, "in_q", None) or getattr(self, "_in_q", None)

            stop_key = getattr(self, "_STOP_SENTINEL_KEY", "__STOP__")
            if q:
                try:
                    q.put_nowait({stop_key: True})
                except Exception:
                    pass
        except Exception:
            pass

        # 2) Attendre/annuler les workers
        tasks = []
        for name in ("_task", "task", "_worker_task", "worker_task"):
            t = getattr(self, name, None)
            if t: tasks.append(t)
        for t in tasks:
            with contextlib.suppress(Exception):
                if not t.done():
                    t.cancel()
                    await asyncio.wait_for(t, timeout=timeout_s)

        # 3) Drapeaux
        with contextlib.suppress(Exception):
            setattr(self, "_running", False)

    async def restart(self) -> None:
        await self.stop()
        self.restart_count += 1

    # ----------------------------- Santé & métriques -----------------------------
    def _is_healthy(self) -> bool:
        now_ms = int(time.time() * 1000)
        ls = self._last_sync_time_ms
        return self.successful_syncs > 0 and ls > 0 and (now_ms - ls) < 60000

    @property
    def last_sync_time(self) -> datetime:
        return datetime.fromtimestamp(self._last_sync_time_ms / 1000.0, tz=timezone.utc)

    @property
    def _last_sync_time_ms(self) -> int:
        times = [v["ts_ex_ms"] for exs in self.buffer.values() for v in exs.values()]
        return max(times) if times else 0

    @property
    def desync_count(self) -> int:
        count = 0
        for pair, exs in self.buffer.items():
            ts_list = [v["ts_ex_ms"] for v in exs.values()]
            if len(ts_list) > 1:
                delta = max(ts_list) - min(ts_list)
                max_delta = int(self._pair_max_delta.get(pair, self.base_max_delta).total_seconds() * 1000)
                if delta > max_delta:
                    count += 1
        return count

    @property
    def avg_skew_seconds(self) -> float:
        gaps_ms: List[int] = []
        for _, exs in self.buffer.items():
            ts_list = [v["ts_ex_ms"] for v in exs.values()]
            if len(ts_list) > 1:
                gaps_ms.append(max(ts_list) - min(ts_list))
        return (sum(gaps_ms) / len(gaps_ms) / 1000.0) if gaps_ms else 0.0

    def set_risk_manager(self, rm: Any) -> None:
        self.risk_manager = rm

    def get_status(self) -> Dict[str, Any]:
        st: Dict[str, Any] = {
            "module": "MarketDataRouter",
            "healthy": self._is_healthy(),
            "details": f"syncs={self.successful_syncs}, desync_pairs={self.desync_count}",
            "metrics": {
                "events_in": self._events_in,
                "events_ignored_stale": self._events_ignored_stale,
                "schema_errors": self._events_schema_errors,
                "route_published": dict(self._route_published),
                "route_drops": dict(self._route_drops),
                "avg_skew_seconds": self.avg_skew_seconds,
                "max_delta_cap_ms": int(self.max_delta_cap.total_seconds() * 1000),
                "restart_count": self.restart_count,
            },
            "submodules": {},
            "ws_source_backpressure": self._ws_backpressure_state,
        }
        rm = getattr(self, "risk_manager", None)
        if rm is not None:
            try:
                modes = {
                    "rm_mode": str(getattr(rm, "rm_mode", "UNKNOWN")),
                    "trade_mode": str(getattr(rm, "trade_mode", "UNKNOWN")),
                }
                if hasattr(rm, "private_plane_state"):
                    modes["private_plane_state"] = str(rm.private_plane_state)
                st["modes"] = modes
            except Exception:
                pass
        return st

# PATCH 6 — Mux WS sharding (fan-in 2–3 sockets/venue → in_queue du Router)
# À coller n’importe où dans le module (niveau top), usage optionnel:
#   mux = WSFanInMux(router_in_queue)
#   mux.register("BINANCE", "A", q_binance_A)
#   mux.register("BINANCE", "B", q_binance_B)
#   asyncio.create_task(mux.start(router.note_ws_reconnect))

class WSFanInMux:
    """
    Fan-in équitable de files shardées par venue → in_queue du Router.
    Non-bloquant, tolérant aux temps morts d’un shard.
    """
    def __init__(self, router_in_queue: asyncio.Queue, *, poll_ms: int = 5):
        self.router_in_queue = router_in_queue
        self.poll_s = max(0.001, float(poll_ms) / 1000.0)
        self._sources: Dict[Tuple[str, str], asyncio.Queue] = {}
        self._running = False


    def register(self, exchange: str, shard: str, source_queue: asyncio.Queue) -> None:
        self._sources[(str(exchange).upper(), str(shard).upper())] = source_queue

    async def start(self, note_reconnect: Optional[Callable[[str], None]] = None) -> None:
        self._running = True
        idx = 0
        while self._running:
            keys = list(self._sources.keys())
            if not keys:
                await asyncio.sleep(self.poll_s); continue
            ex, sh = keys[idx % len(keys)]
            idx += 1
            q = self._sources.get((ex, sh))
            if not q:
                continue
            try:
                item = q.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(self.poll_s)
                continue
            try:
                if isinstance(item, dict) and item.get("__ws_reconnect__") and callable(note_reconnect):
                    note_reconnect(item.get("exchange") or ex)
                self.router_in_queue.put_nowait(item)
            except asyncio.QueueFull:
        # backpressure jusqu’au Router: on laisse l’item en tête du shard (pas de lose)
                try:
                    q.put_nowait(item)  # requeue best effort
                except Exception:
                    pass
                await asyncio.sleep(self.poll_s)

    def stop(self) -> None:
        self._running = False
