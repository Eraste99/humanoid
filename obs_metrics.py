from __future__ import annotations
import asyncio
import json
import logging
import threading
import time
import sys
import types

from typing import Any, Dict,List, Optional
from dataclasses import dataclass, asdict
log = logging.getLogger('obs_metrics')
_obs_shim_log = logging.getLogger('observability_shim')

def _register_singleton_aliases() -> None:
    """Enforce a single obs_metrics module reference across import paths."""
    this_module = sys.modules[__name__]
    canonical = "modules.obs_metrics"
    alt = "obs_metrics"

    existing_canonical = sys.modules.get(canonical)
    if existing_canonical is not None and existing_canonical is not this_module:
        raise RuntimeError(
            f"obs_metrics double import detected: {existing_canonical.__name__} vs {__name__}"
        )
    sys.modules[canonical] = this_module

    pkg = sys.modules.setdefault("modules", types.ModuleType("modules"))
    setattr(pkg, "obs_metrics", this_module)

    existing_alt = sys.modules.get(alt)
    if existing_alt is not None and existing_alt is not this_module:
        raise RuntimeError(
            f"obs_metrics alias mismatch: {existing_alt.__name__} vs {__name__}"
        )
    sys.modules[alt] = this_module
    log.info("obs_metrics loaded via %s", __name__)


_register_singleton_aliases()

class _AlertCounter:
    def __init__(self):
        self.count = 0
        self.kwargs = []
        self.last_value = None

    def labels(self, **kwargs):
        self.kwargs.append(kwargs)
        return self

    def inc(self, *_args, **_kwargs):
        self.count += 1
        return None

    def set(self, value):
        self.last_value = value
        self.count += 1
        return None

# --- PROMETHEUS FALLBACK ---
try:
    from prometheus_client import Counter, Gauge, Histogram, REGISTRY, start_http_server, CollectorRegistry, CONTENT_TYPE_LATEST, generate_latest
    _PROM_READY = True
except Exception:
    _PROM_READY = False
    REGISTRY = None
    CollectorRegistry = None
    CONTENT_TYPE_LATEST = 'text/plain; version=0.0.4; charset=utf-8'

    class _NoopMetric:
        def __init__(self, *_a, **_k): pass
        def labels(self, **_k): return self
        def inc(self, *_a, **_k): pass
        def set(self, *_a, **_k): pass
        def observe(self, *_a, **_k): pass

    Counter = Gauge = Histogram = _NoopMetric

    def start_http_server(*_a, **_k):
        log.warning('prometheus_client absent: /metrics non démarré (no-op)')

    def generate_latest(_=None):
        return b'# prometheus_client indisponible (Cas 1: import fail)\n'

def _metric(klass, name: str, doc: str, *args, **kwargs):
    """
    Optimisation HFT : Centralisation de la création de métriques.
    Si prometheus_client est absent, retourne un _NoopMetric silencieux.
    """
    if not _PROM_READY:
        return _NoopMetric()

    reg = REGISTRY
    if reg is not None:
        try:
            existing = getattr(reg, '_names_to_collectors', {}).get(name)
            if existing is not None:
                return existing
        except Exception:
            pass
    return klass(name, doc, *args, **kwargs)

# --- FONDATIONS OBSERVABILITÉ ---
OBS_NOOP_TOTAL = _metric(Counter, "obs_noop_total", "Total des appels metrics no-op", ["metric", "where"])
OBS_INIT_ERROR_TOTAL = _metric(Counter, "obs_init_error_total", "Echecs d'initialisation de métriques", ["metric", "kind"])
OBS_LABEL_MISMATCH_TOTAL = _metric(Counter, "obs_label_mismatch_total", "Labels fournis non conformes", ["metric"])

# --- MANDATORY OBSERVABILITY (P0) ---
ROUTER_EVENT_AGE_MS = _metric(Histogram, "router_event_age_ms", "Age des events à l'entrée du Router (ms)", ["exchange"], buckets=[10, 50, 100, 250, 500, 1000, 2000, 5000])
SCANNER_EVENT_AGE_MS = _metric(Histogram, "scanner_event_age_ms", "Age des events à l'évaluation Scanner (ms)", ["exchange", "pair"], buckets=[50, 100, 250, 500, 1000, 1500, 2000, 5000])

# --- Payload Consistency (Audit End-to-End) ---
PAYLOAD_COERCED_TOTAL = _metric(Counter, "payload_coerced_total", "Nombre de champs de payload forcés vers un type canonique", ["kind", "field", "from_type", "to_type"])
PAYLOAD_NORMALIZED_TOTAL = _metric(Counter, "payload_normalized_total", "Nombre de champs de payload normalisés vers un format canonique", ["kind", "field", "from_format", "to_format"])
PAYLOAD_INVALID_TOTAL = _metric(Counter, "payload_invalid_total", "Nombre de payloads rejetés car invalides", ["kind", "reason"])
PAYLOAD_SCHEMA_VERSION = _metric(Gauge, "payload_schema_version", "Version actuelle du schéma de payload canonique", ["version"])
ROUTER_SLO_MISSING_TOTAL = _metric(Counter, "router_slo_missing_total", "SLO manquante lors de la résolution du stale_limit (Router)", ["mode", "exchange"])
ROUTER_DEPLOYMENT_MODE_INVALID_TOTAL = _metric(Counter, "router_deployment_mode_invalid_total", "Deployment mode invalide (Router)", ["mode"])
ROUTER_CLOCK_OFFSET_MS = _metric(Gauge, "router_clock_offset_ms", "Offset d'horloge détecté (Router) en ms", ["exchange"])
WS_PUBLIC_CLOCK_OFFSET_MS = _metric(Gauge, "ws_public_clock_offset_ms", "Offset d'horloge détecté (WS public) en ms", ["exchange", "region", "deployment_mode"])
PWS_HUB_DROPPED_TOTAL = _metric(Counter, "pws_hub_dropped_total", "Total des events drop par le PrivateWSHub", ["exchange", "reason"])
PWS_LATENCY_DRAIN_MS = _metric(Histogram, "pws_latency_drain_ms", "Latence de vidage des queues privées (ms)", ["exchange"], buckets=[5, 10, 25, 50, 100, 250, 500])
LHM_EMIT_ERROR_TOTAL = _metric(Counter, "lhm_emit_error_total", "Total des erreurs d'émission LHM (rupture de logging)", ["log_type"])
RM_CLOCK_SKEW_UNKNOWN_TOTAL = _metric(
    Counter,
    "rm_clock_skew_unknown_total",
    "RM preflight rejected because clock skew could not be measured",
)
# --- BEGIN OM-0: STRICT + PROM READY + Noop ---
try:
    from modules.bot_config import BotConfig as _ObsBotConfig
except Exception:
    _ObsBotConfig = None

STRICT_OBS = 0

def _load_obs_defaults() -> dict[str, object]:
    global STRICT_OBS
    defaults = {
        "status_port": 9110,
        "expose_metrics_on_status": True,
        "enable_obs_port": False,
        "obs_port": 9108,
    }
    if _ObsBotConfig is None:
        return defaults
    try:
        cfg = _ObsBotConfig.from_env()
    except Exception:
        return defaults
    obs_cfg = getattr(cfg, "obs", None)
    if obs_cfg is None:
        return defaults
    STRICT_OBS = 1 if getattr(obs_cfg, "strict_obs", False) else 0
    return {
        "status_port": getattr(obs_cfg, "status_port", defaults["status_port"]),
        "expose_metrics_on_status": getattr(obs_cfg, "expose_metrics_on_status", defaults["expose_metrics_on_status"]),
        "enable_obs_port": getattr(obs_cfg, "enable_obs_port", defaults["enable_obs_port"]),
        "obs_port": getattr(obs_cfg, "obs_port", defaults["obs_port"]),
    }

_OBS_DEFAULTS = _load_obs_defaults()

def prom_ready() -> bool:
    """Expose readiness des métriques Prometheus pour les modules clients (ex: LHM.get_status)."""
    return bool(_PROM_READY)

# Pilotage runtime du mode strict (config-driven)
def set_strict_obs(strict: bool | int | None) -> None:
    global STRICT_OBS
    if strict is None:
        return
    STRICT_OBS = 1 if bool(strict) else 0

# --- END OM-1 ---
# --- BEGIN OM-2: métriques utilisées par Engine/Scanner/LHM ---
# Scanner – hint top_qty manquant (utilisé dans Patch S4)
SCANNER_HINT_TOPQTY_MISSING_TOTAL = _metric(Counter,
    "scanner_hint_topqty_missing_total",
    "Hints L1 (top_qty) manquants (non bloquant)",
    ["pair", "ex", "side"]
)
SCANNER_MM_LEG_RESTRICTED_TOTAL = _metric(
    Counter,
    "scanner_mm_leg_restricted_total",
    "MM legs restricted by inventory drift or safety gating",
    ["exchange", "side", "reason"],
)

SCANNER_ROTATION_ERRORS_TOTAL = _metric(
    Counter,
    "scanner_rotation_errors_total",
    "Errors during scanner rotation decisions",
    ["stage"],
)
# Router – erreurs d'émission d'event sink (backpressure/telemetry)
ROUTER_EVENT_SINK_ERRORS_TOTAL = _metric(
    Counter,
    "router_event_sink_errors_total",
    "Errors while emitting router events to event sink",
    ["reason"],
)

# Simulator priming
SIM_PRIME_TOTAL = _metric(Counter,
    "sim_prime_total",
    "Prime requests enqueued",
    ["branch"],
)
SIM_PRIME_ERROR_TOTAL = _metric(Counter,
    "sim_prime_error_total",
    "Prime errors",
    ["branch"],
)
# LHM – rotations fichiers (demandée précédemment)
LOGGERH_FILE_ROTATIONS_TOTAL = _metric(Counter,
    "loggerh_file_rotations_total",
    "Compteur de rotations de fichiers du LoggerHistorique",
    ["reason"]  # ex: size, time, manual
)

# Engine – erreurs/visibilité supplémentaires (si déjà définies, garde celles existantes)

AC_KV_ERRORS_TOTAL = _metric(Counter,
        "ac_kv_errors_total",
        "Erreurs KV (anti-crossing multi-pod)",
        ["kind"]  # set/get/eval_del
    )


AC_RESERVE_CONFLICT_TOTAL = _metric(Counter,
        "ac_reserve_conflict_total",
        "Conflits de réservation (anti-crossing)",
        ["branch", "ex"]  # TM/MM, EX
    )


AC_RELEASE_ERRORS_TOTAL = _metric(Counter,
        "ac_release_errors_total",
        "Erreurs de release (anti-crossing)",
        ["kind"]  # kv_delete/memory
    )


ENGINE_WORKER_ERRORS_TOTAL = _metric(Counter,
        "engine_worker_errors_total",
        "Erreurs dans les workers de l'Engine",
        ["phase"]  # W1/W2...
    )


ENGINE_INFLIGHT_GAUGE_SET_ERRORS_TOTAL = _metric(Counter,
        "engine_inflight_gauge_set_errors_total",
        "Echecs de set() du gauge in-flight",
        ["exchange"]
    )


ENGINE_BEST_PRICE_MISSING_TOTAL = _metric(Counter,
        "engine_best_price_missing_total",
        "Prix de référence manquant côté Engine",
        ["exchange", "pair", "side"]
    )
# LHM – EOD orchestration (EOD-01)
LHM_EOD_RUNS_TOTAL = _metric(Counter,
    "lhm_eod_runs_total",
    "Total des exécutions EOD (logger historique)",
    labelnames=["status"],
)

LHM_EOD_ERRORS_TOTAL = _metric(Counter,
    "lhm_eod_errors_total",
    "Erreurs rencontrées pendant l'EOD (par étape)",
    labelnames=["stage"],
)

LHM_EOD_DURATION_MS = _metric(Histogram,
    "lhm_eod_duration_ms",
    "Durée des runs EOD en millisecondes",
)

LHM_EOD_LAST_SUCCESS_TS_MS = _metric(Gauge,
    "lhm_eod_last_success_ts_ms",
    "Horodatage (ms epoch) du dernier EOD réussi",
)

# --- END OM-2 ---
# --- BEGIN OM-3: wrappers safe pour inc/set/observe ---
def _safe_labels(metric, name: str, where: str, **labels):
    try:
        return metric.labels(**labels)
    except Exception:
        OBS_LABEL_MISMATCH_TOTAL.labels(metric=name).inc()
        OBS_NOOP_TOTAL.labels(metric=name, where=where).inc()
        return metric  # no-op si Counter/Gauge est Noop, sinon évite de casser

def safe_inc(metric, name: str, where: str, **labels):
    try:
        _safe_labels(metric, name, where, **labels).inc()
    except Exception:
        OBS_NOOP_TOTAL.labels(metric=name, where=where).inc()

def safe_set(metric, name: str, where: str, value: float, **labels):
    try:
        _safe_labels(metric, name, where, **labels).set(value)
    except Exception:
        OBS_NOOP_TOTAL.labels(metric=name, where=where).inc()

def safe_observe(metric, name: str, where: str, value: float, **labels):
    try:
        _safe_labels(metric, name, where, **labels).observe(value)
    except Exception:
        OBS_NOOP_TOTAL.labels(metric=name, where=where).inc()
# --- END OM-3 ---


# --- BEGIN CARDINALITY GATING ---
# Liste des métriques autorisées à conserver le label 'pair' (Haute Cardinalité)
# Les autres métriques verront leur label 'pair' agrégé en 'ALL' ou supprimé.
CRITICAL_METRICS_WITH_PAIR = {
    "inventory_usd",
    "rm_decision_ms",
    "rm_fragment_profit_ms",
    "pair_health_penalty_total",
    "pnl_quote_total",
    "active_bundles_count",
    "inflight_orders_count",
    "spread_bps",
    "vol_bps",
    "vol_ewma_bps",
    "vol_p95_bps",
    "dynamic_min_bps",
    "mm_fills_both_total",
    "mm_single_fill_hedged_total",
    "mm_panic_hedge_total",
    "engine_inflight",
    "pnl_live_day_usd",
    "mm_throttled_total",
    "mm_makers_expired_ttl_total",
    "mm_makers_canceled_total",
    "engine_cancellations_total",
    "engine_retries_total",
    "engine_queuepos_blocked_total",
    "router_pair_queue_depth",
    "scanner_evaluate_ms",
}

def _filter_labels(name: str, labelnames: tuple | list) -> list:
    """Filtre les labels pour limiter la cardinalité si nécessaire."""
    if "pair" not in labelnames:
        return list(labelnames)
    
    # Si la métrique est critique, on garde le label 'pair'
    if name in CRITICAL_METRICS_WITH_PAIR:
        return list(labelnames)
    
    # Sinon, on retire 'pair' de la définition de la métrique pour agréger
    return [l for l in labelnames if l != "pair"]

def get_counter(name, documentation, labelnames=(), **kwargs):
    filtered_labels = _filter_labels(name, labelnames)
    return _metric(Counter, name, documentation, labelnames=filtered_labels, **kwargs)

def get_gauge(name, documentation, labelnames=(), **kwargs):
    filtered_labels = _filter_labels(name, labelnames)
    return _metric(Gauge, name, documentation, labelnames=filtered_labels, **kwargs)

def get_histogram(name, documentation, labelnames=(), **kwargs):
    filtered_labels = _filter_labels(name, labelnames)
    return _metric(Histogram, name, documentation, labelnames=filtered_labels, **kwargs)
# --- END CARDINALITY GATING ---
BUCKETS_MS = (1, 2, 5, 10, 20, 50, 75, 100, 150, 250, 400, 600, 1000, 1500, 2000, 3000, 5000, 8000, 12000)
BUCKETS_LATENCY_HFT = (0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0)

# --- LATENCY PIPELINE (END-TO-END) ---
LATENCY_PIPELINE_MS = _metric(
    Histogram,
    'latency_pipeline_ms',
    'End-to-end pipeline latency by segment (ms)',
    ['segment', 'route', 'exchange'],
    buckets=BUCKETS_LATENCY_HFT
)

PIPELINE_BACKLOG = _metric(
    Gauge,
    'pipeline_backlog',
    'Number of pending events in pipeline stages',
    ['stage']
)

def record_pipeline_latency(segment: str, dt_ms: float, route: str = 'none', exchange: str = 'none') -> None:
    try:
        LATENCY_PIPELINE_MS.labels(segment=_norm(segment), route=_norm(route), exchange=_norm(exchange)).observe(max(0.0, float(dt_ms)))
    except Exception:
        pass

def set_pipeline_backlog(stage: str, count: int) -> None:
    try:
        PIPELINE_BACKLOG.labels(stage=_norm(stage)).set(float(count))
    except Exception:
        pass

# --- END LATENCY PIPELINE ---

def should_trace_latency(cfg: Any) -> bool:
    """Check if latency tracing is enabled and should be sampled."""
    try:
        if not cfg or not hasattr(cfg, "obs"):
            return False
        obs = cfg.obs
        if not getattr(obs, "enable_latency_tracing", False):
            return False
        rate = getattr(obs, "latency_sampling_rate", 1.0)
        if rate >= 1.0:
            return True
        import random
        return random.random() <= rate
    except Exception:
        return False

def _norm(v: Any) -> str:
    if v is None:
        return 'none'
    s = str(v)
    return s if s else 'empty'

def lbl_exchange(ex: str) -> str:
    ex = _norm(ex).upper()
    return ex if ex in ('BINANCE', 'BYBIT', 'COINBASE') else ex

def lbl_alias(alias: str) -> str:
    return _norm(alias).upper()

def lbl_region(r: str) -> str:
    r = _norm(r).upper()
    return r if r in ('EU', 'US', 'EU-CB') else r

def lbl_mode(m: str) -> str:
    m = _norm(m).upper()
    return m if m in ('EU_ONLY', 'SPLIT', 'JP_ONLY') else m


DEPLOYMENT_REGION = _metric(Gauge, 'deployment_region_info', "Region info gauge (label 'region')", ['region'])
DEPLOYMENT_MODE = _metric(Gauge, 'deployment_mode_info', "Mode info gauge (label 'mode')", ['mode'])

def set_region(region: str) -> None:
    try:
        DEPLOYMENT_REGION.labels(lbl_region(region)).set(1)
    except Exception:
        pass

def set_deployment_mode(mode: str) -> None:
    try:
        DEPLOYMENT_MODE.labels(lbl_mode(mode)).set(1)
    except Exception:
        pass
TIME_SKEW_MS = _metric(Gauge, 'time_skew_ms', 'Estimated system time skew (ms)')
TIME_SKEW_STATUS = _metric(Gauge, 'time_skew_status', 'Time sync status (1=OK,0=unknown/-1=bad)')
EVENT_LOOP_LAG_MS = _metric(Gauge, 'event_loop_lag_ms', 'Event loop / scheduler lag (ms)')
_loop_lag_thread: Optional[threading.Thread] = None
_time_skew_thread: Optional[threading.Thread] = None

_CURRENT_TIME_SKEW_MS = 0.0

def start_time_skew_probe(period_s: float = 10.0) -> None:
    """
    Publie TIME_SKEW_MS/TIME_SKEW_STATUS.
    Best-effort: lit chrony via `chronyc tracking`.
      - status=1 : mesure OK
      - status=0 : indisponible (chrony absent/erreur)
    """
    global _time_skew_thread
    if _time_skew_thread and _time_skew_thread.is_alive():
        return

    def _runner():
        import re
        import subprocess

        while True:
            skew_ms = 0.0
            status = 0
            try:
                p = subprocess.run(
                    ["chronyc", "tracking"],
                    capture_output=True,
                    text=True,
                    timeout=0.8,
                )
                if p.returncode == 0 and p.stdout:
                    lines = p.stdout.splitlines()

                    # 1) "System time     : 0.000123 seconds slow/fast of NTP time"
                    for ln in lines:
                        if "System time" in ln:
                            m = re.search(r":\s*([+-]?[0-9]*\.?[0-9]+)\s*seconds", ln)
                            if m:
                                sec = float(m.group(1))
                                ll = ln.lower()
                                if "slow" in ll:
                                    sec = -abs(sec)
                                elif "fast" in ll:
                                    sec = abs(sec)
                                skew_ms = sec * 1000.0
                                status = 1
                                break

                    # 2) fallback "Last offset   : +0.000001 seconds"
                    if status == 0:
                        for ln in lines:
                            if "Last offset" in ln:
                                m = re.search(r":\s*([+-]?[0-9]*\.?[0-9]+)\s*seconds", ln)
                                if m:
                                    skew_ms = float(m.group(1)) * 1000.0
                                    status = 1
                                    break
            except Exception:
                pass

            try:
                update_time_skew(skew_ms, status)
            except Exception:
                pass

            time.sleep(max(1.0, float(period_s)))

    _time_skew_thread = threading.Thread(target=_runner, name="time_skew_probe", daemon=True)
    _time_skew_thread.start()

def update_time_skew(ms: float, status: int=1) -> None:
    global _CURRENT_TIME_SKEW_MS
    try:
        _CURRENT_TIME_SKEW_MS = float(ms)
        TIME_SKEW_MS.set(float(ms))
        TIME_SKEW_STATUS.set(float(status))
    except Exception:
        log.exception('update_time_skew failed')

def get_time_skew_ms() -> float:
    return _CURRENT_TIME_SKEW_MS

async def start_loop_lag_probe(period_s: float = 0.5) -> None:
    """
    Mesure *réelle* du lag de scheduling de l'event-loop (ms).

    Définition:
      lag_ms = (time_of_callback_execution - scheduled_time) * 1000

    Pourquoi pas asyncio.sleep?
      - sleep mesure "retard de réveil", mais peut être quasi-nul (arrondi / jitter),
        et se prête mal à une interprétation HFT.
      - call_at mesure directement le retard d'exécution d'un callback planifié,
        ce qu'on veut pour détecter un event-loop bloqué.

    Valeur publiée:
      EVENT_LOOP_LAG_MS (Gauge) en millisecondes (>= 0).
    """
    loop = asyncio.get_running_loop()

    # Garde-fous: éviter des périodes trop petites qui amplifient le bruit flottant.
    try:
        period_s = float(period_s)
    except Exception:
        period_s = 0.5
    if period_s < 0.05:
        period_s = 0.05

    while True:
        try:
            scheduled_at = loop.time() + period_s
            fut: asyncio.Future = loop.create_future()

            def _cb() -> None:
                try:
                    now = loop.time()
                    lag_ms = (now - scheduled_at) * 1000.0
                    if lag_ms < 0.0:
                        lag_ms = 0.0
                    try:
                        EVENT_LOOP_LAG_MS.set(float(lag_ms))
                    except Exception:
                        pass
                finally:
                    if not fut.done():
                        try:
                            fut.set_result(True)
                        except Exception:
                            pass

            loop.call_at(scheduled_at, _cb)
            await fut

        except asyncio.CancelledError:
            raise
        except Exception:
            # Best-effort: ne jamais casser le bot à cause de l'observabilité.
            try:
                await asyncio.sleep(1.0)
            except Exception:
                pass


# --- Optimisation GC ---
def optimize_gc():
    """Ajuste le Garbage Collector pour réduire les pauses en HFT."""
    try:
        import gc
        # Augmente les seuils de déclenchement du GC (réduit la fréquence des pauses)
        # Par défaut: (700, 10, 10) -> On passe à (50000, 10, 10)
        gc.set_threshold(50000, 10, 10)
        log.info("[Obs] GC Optimized: thresholds set to (50000, 10, 10)")
    except Exception:
        pass
NONFATAL_ERRORS_TOTAL = _metric(Counter, 'nonfatal_errors_total', 'Non-fatal errors reported', ['module', 'kind'])
BLOCKED_TOTAL = _metric(Counter, 'blocked_total', 'Blocked events by guard/gate', ['module', 'reason', 'pair'])

def report_nonfatal(module: str, kind: str, err: Optional[BaseException]=None, **labels: Any) -> None:
    try:
        NONFATAL_ERRORS_TOTAL.labels(_norm(module), _norm(kind)).inc()
    except Exception:
        pass
    if err:
        log.debug('[nonfatal][%s:%s] %s | labels=%r', module, kind, err, labels)

def inc_blocked(module: str, reason: str, pair: Optional[str]=None) -> None:
    try:
        BLOCKED_TOTAL.labels(_norm(module), _norm(reason), _norm(pair)).inc()
    except Exception:
        pass
BF_API_ERRORS_TOTAL = _metric(
    Counter,
    'bf_api_errors_total',
    'BalanceFetcher API errors',
    ['exchange', 'alias', 'endpoint', 'reason'],
)
MBF_WS_DELTA_APPLIED_TOTAL = _metric(Counter, 'mbf_ws_delta_applied_total', 'Total WS deltas applied to MBF', ['exchange', 'alias'])
MBF_WS_DELTA_REJECTED_TOTAL = _metric(Counter, 'mbf_ws_delta_rejected_total', 'Total WS deltas rejected by MBF', ['exchange', 'alias', 'reason'])
MBF_WALLET_ASSET_COUNT = _metric(Gauge, 'mbf_wallet_asset_count', 'Number of assets per wallet in MBF', ['exchange', 'alias', 'wallet'])
BF_API_LATENCY_MS = _metric(
    Histogram,
    'bf_api_latency_ms',
    'BalanceFetcher API latency (ms)',
    ['exchange', 'alias', 'endpoint'],
    buckets=BUCKETS_MS,
)
BF_CACHE_AGE_SECONDS = _metric(Gauge, 'bf_cache_age_seconds', 'Age of balances cache (seconds)', ['exchange', 'alias'])
BF_LAST_SUCCESS_TS = _metric(Gauge, 'bf_last_success_ts_seconds', 'Last successful BF ts (epoch seconds)', ['exchange', 'alias'])
FEE_TOKEN_BALANCE = _metric(Gauge, 'fee_token_balance', 'Fee token balance', ['exchange', 'alias', 'token'])
FEE_TOKEN_CHECK_ERRORS_TOTAL = _metric(
    Counter,
    'fee_token_check_errors_total',
    'Fee token check errors',
    ['reason'],
)
FEE_TOKEN_TOPUP_REQUESTED_TOTAL = _metric(
    Counter,
    'fee_token_topup_requested_total',
    'Fee token top-up requests',
    ['symbol'],
)

BF_BALANCES_TTL_NORMAL_SECONDS = _metric(
    Gauge,
    'bf_balances_ttl_normal_seconds',
    'SLO TTL for balances (normal) per exchange',
    ['exchange'],
)
BF_BALANCES_TTL_DEGRADED_SECONDS = _metric(
    Gauge,
    'bf_balances_ttl_degraded_seconds',
    'SLO TTL for balances (degraded) per exchange',
    ['exchange'],
)
BF_BALANCES_TTL_BLOCK_SECONDS = _metric(
    Gauge,
    'bf_balances_ttl_block_seconds',
    'SLO TTL for balances (block) per exchange',
    ['exchange'],
)
BF_BALANCES_HEALTH_STATE = _metric(
    Gauge,
    'bf_balances_health_state',
    'Balance fetcher health state (0=NORMAL,1=DEGRADED,2=BLOCK)',
    ['exchange', 'alias'],
)

CONTRACTS_HELPERS_CALLS_TOTAL = _metric(
    Counter,
    'contracts_helpers_calls_total',
    'Number of contract conversion helpers calls',
    ['func'],
)
CONTRACTS_VALIDATION_ERRORS_TOTAL = _metric(
    Counter,
    'contracts_validation_errors_total',
    'Number of contract validation errors',
    ['model'],
)
BF_HTTP_LATENCY_SECONDS = _metric(
    Histogram,
    'bf_http_latency_seconds',
    'BalanceFetcher HTTP latency (seconds)',
    ['exchange', 'alias', 'endpoint'],
)
BF_429_TOTAL = _metric(
    Counter,
    'balance_fetcher_429_total',
    'Balance fetcher 429 responses',
    ['exchange', 'alias', 'endpoint'],
)
BF_HTTP_ERRORS_TOTAL = _metric(
    Counter,
    'bf_http_errors_total',
    'BalanceFetcher HTTP errors',
    ['exchange', 'alias', 'endpoint'],
)
BF_FEE_TOKEN_LEVEL = _metric(
    Gauge,
    'bf_fee_token_level',
    "Niveau courant d'un token de frais",
    ['exchange', 'alias', 'token'],
)
BF_FEE_TOKEN_LOW_TOTAL = _metric(
    Counter,
    'bf_fee_token_low_total',
    'Alertes de niveau bas sur token de frais',
    ['exchange', 'alias', 'token'],
)

def mark_bf_latency(exchange: str, alias: str, seconds: float, ok: bool, endpoint: str='generic', reason: str='ok') -> None:
    try:
        BF_API_LATENCY_MS.labels(lbl_exchange(exchange), lbl_alias(alias), _norm(endpoint)).observe(
            max(0.0, float(seconds * 1000.0))
        )
        if not ok:
            BF_API_ERRORS_TOTAL.labels(
                lbl_exchange(exchange),
                lbl_alias(alias),
                _norm(endpoint),
                _norm(reason or 'error'),
            ).inc()
        else:
            BF_LAST_SUCCESS_TS.labels(lbl_exchange(exchange), lbl_alias(alias)).set(time.time())
    except Exception:
        pass
RPC_LATENCY_MS = _metric(Histogram, 'rpc_latency_ms', 'RPC latency (ms)', ['method', 'region'], buckets=BUCKETS_MS)
RPC_ERR_TOTAL = _metric(Counter, 'rpc_err_total', 'RPC errors', ['code', 'method', 'region'])
RPC_RETRIES_TOTAL = _metric(Counter, 'rpc_retries_total', 'RPC retries', ['method', 'region'])
RPC_PAYLOAD_REJECTED_TOTAL = _metric(Counter, 'rpc_payload_rejected_total', 'Rejected RPC payloads', ['model'])
RPC_IDEMPOTENCY_HIT_TOTAL = _metric(
    Counter,
    'rpc_idempotency_hit_total',
    'Idempotency cache hits',
    ['method', 'region'],
)
ENGINE_IDEMPOTENCY_MISSING_TOTAL = _metric(
    Counter,
    "engine_idempotency_missing_total",
    "Engine submissions rejected due to missing idempotency key",
    ["branch", "profile"],
)
ROUTER_QUEUE_DEPTH = _metric(Gauge, 'router_queue_depth', 'Router queue depth', ['queue'])
ROUTER_PAIR_QUEUE_DEPTH = _metric(Gauge, 'router_pair_queue_depth', 'Router per-pair queue depth', ['pair', 'tier'])
ROUTER_QUEUE_HIGH_WATERMARK_TOTAL = _metric(Counter, 'router_queue_high_watermark_total', 'Router queue high watermark hits', ['queue'])
ROUTER_QUEUE_DEPTH_BY_EX = _metric(Gauge, 'router_queue_depth_by_ex', 'Router queue depth by exchange/shard', ['exchange', 'queue', 'shard'])
ROUTER_DROPPED_TOTAL = _metric(Counter, 'router_dropped_total', 'Router dropped events', ['queue', 'reason'])
ROUTER_COMBO_SKEW_MS = _metric(Histogram, 'router_combo_skew_ms', 'Router combo skew (ms)', ['route'], buckets=BUCKETS_MS)
ROUTER_TO_SCANNER_MS = _metric(Histogram, 'router_to_scanner_ms', 'Latency Router→Scanner (ms)', ['route'], buckets=BUCKETS_MS)
ROUTER_TO_SCANNER_ERRORS_TOTAL = _metric(Counter, 'router_to_scanner_errors_total', 'Errors Router→Scanner', ['route', 'reason'])
ROUTER_CFG_STALE_MS = _metric(Gauge, 'router_cfg_stale_ms', 'Configured Router stale threshold (ms)')
ROUTER_CFG_COALESCE_WINDOW_MS = _metric(Gauge, 'router_cfg_coalesce_window_ms', 'Configured Router coalesce window (ms)')
ROUTER_CFG_REQUIRE_L2_FIRST = _metric(Gauge, 'router_cfg_require_l2_first', 'Router require_l2_first flag (1/0)')
ROUTER_CFG_OUT_QUEUE_MAXLEN = _metric(Gauge, 'router_cfg_out_queue_maxlen', 'Router out queue maxlen by kind', ['kind'])
WS_SYMBOL_UNMAPPED_TOTAL = _metric(Counter, 'ws_symbol_unmapped_total', 'WS symbol mapping failures', ['exchange'])


def mark_router_to_scanner(route: str, ok: bool, dt_ms: float, reason: str = 'ok') -> None:
    try:
        ROUTER_TO_SCANNER_MS.labels(_norm(route)).observe(max(0.0, float(dt_ms)))
        if not ok:
            ROUTER_TO_SCANNER_ERRORS_TOTAL.labels(_norm(route), _norm(reason)).inc()
    except Exception:
        pass


def mark_router_to_scanner_ts(
    ts_start_ns: int,
    *,
    route: str = "tri_cex",
    ok: bool = True,
    reason: str = "ok",
    **_labels: Any,
) -> None:
    """
    Wrapper basé sur un timestamp perf_counter_ns() pour Router→Scanner.

    - ts_start_ns: timestamp de départ en ns (perf_counter_ns()).
    - route: label logique de la route (par défaut "tri_cex").
    - ok: True si le push s'est bien passé, False sinon.
    - reason: raison de l'échec ("queue_full", "exception", ...).
    """
    try:
        now_ns = time.perf_counter_ns()
        dt_ms = max(0.0, (float(now_ns - int(ts_start_ns)) / 1e6))
        # On passe par la fonction canonique existante
        mark_router_to_scanner(route, ok, dt_ms, reason=reason)
    except Exception:
        # Observabilité best-effort, jamais bloquante
        pass

def note_router_cfg(stale_ms: float, coalesce_window_ms: float, require_l2_first: bool, out_queue_maxlen: Dict[str, float] | float | None) -> None:
    try:
        safe_set(ROUTER_CFG_STALE_MS, 'router_cfg_stale_ms', 'note_router_cfg', float(stale_ms))
        safe_set(ROUTER_CFG_COALESCE_WINDOW_MS, 'router_cfg_coalesce_window_ms', 'note_router_cfg', float(coalesce_window_ms))
        safe_set(ROUTER_CFG_REQUIRE_L2_FIRST, 'router_cfg_require_l2_first', 'note_router_cfg', 1.0 if require_l2_first else 0.0)
        if out_queue_maxlen is not None:
            items = out_queue_maxlen.items() if isinstance(out_queue_maxlen, dict) else [("combo", float(out_queue_maxlen))]
            for kind, size in items:
                try:
                    safe_set(
                        ROUTER_CFG_OUT_QUEUE_MAXLEN,
                        'router_cfg_out_queue_maxlen',
                        'note_router_cfg',
                        float(size),
                        kind=_norm(kind),
                    )
                except Exception:
                    OBS_NOOP_TOTAL.labels(metric='router_cfg_out_queue_maxlen', where='note_router_cfg').inc()
    except Exception:
        OBS_NOOP_TOTAL.labels(metric='note_router_cfg', where='note_router_cfg').inc()


def note_ws_public_cfg(
    *,
    ping_interval_s: float,
    pong_timeout_s: float,
    connect_timeout_s: float,
    read_timeout_s: float,
    out_queue_put_timeout_s: float | None = None,
    chunk_size_by_exchange: Dict[str, float] | None = None,
) -> None:
    try:
        safe_set(WS_PUBLIC_PING_INTERVAL_SECONDS_CONFIG, 'ws_public_ping_interval_seconds_config', 'note_ws_public_cfg', float(ping_interval_s))
        safe_set(WS_PUBLIC_PONG_TIMEOUT_SECONDS_CONFIG, 'ws_public_pong_timeout_seconds_config', 'note_ws_public_cfg', float(pong_timeout_s))
        safe_set(WS_PUBLIC_CONNECT_TIMEOUT_SECONDS_CONFIG, 'ws_public_connect_timeout_seconds_config', 'note_ws_public_cfg', float(connect_timeout_s))
        safe_set(WS_PUBLIC_READ_TIMEOUT_SECONDS_CONFIG, 'ws_public_read_timeout_seconds_config', 'note_ws_public_cfg', float(read_timeout_s))
        if out_queue_put_timeout_s is not None:
            safe_set(
                WS_PUBLIC_OUT_QUEUE_PUT_TIMEOUT_SECONDS_CONFIG,
                'ws_public_out_queue_put_timeout_seconds_config',
                'note_ws_public_cfg',
                float(out_queue_put_timeout_s),
            )
        if chunk_size_by_exchange:
            for ex, size in chunk_size_by_exchange.items():
                try:
                    safe_set(
                        WS_PUBLIC_CHUNK_SIZE_CONFIG,
                        'ws_public_chunk_size_config',
                        'note_ws_public_cfg',
                        float(size),
                        exchange=_norm(ex),
                    )
                except Exception:
                    OBS_NOOP_TOTAL.labels(metric='ws_public_chunk_size_config', where='note_ws_public_cfg').inc()
    except Exception:
        OBS_NOOP_TOTAL.labels(metric='note_ws_public_cfg', where='note_ws_public_cfg').inc()


def note_vol_ttl_seconds(ttl_s: float) -> None:
    try:
        safe_set(VOL_TTL_SECONDS_CONFIG, 'vol_ttl_seconds_config', 'note_vol_ttl_seconds', float(ttl_s))
    except Exception:
        OBS_NOOP_TOTAL.labels(metric='vol_ttl_seconds_config', where='note_vol_ttl_seconds').inc()


def note_slip_ttl_seconds(ttl_s: float) -> None:
    try:
        safe_set(SLIP_TTL_SECONDS_CONFIG, 'slip_ttl_seconds_config', 'note_slip_ttl_seconds', float(ttl_s))
    except Exception:
        OBS_NOOP_TOTAL.labels(metric='slip_ttl_seconds_config', where='note_slip_ttl_seconds').inc()

def note_slip_drop(reason: str, exchange: str) -> None:
    try:
        SLIP_DROP_TOTAL.labels(lbl_exchange(exchange), _norm(reason)).inc()
    except Exception:
        OBS_NOOP_TOTAL.labels(metric='slip_drop_total', where='note_slip_drop').inc()

def note_scanner_cfg(scan_interval_s: float, min_required_bps: float, max_pairs_per_tick: float) -> None:
    try:
        safe_set(SCANNER_CFG_SCAN_INTERVAL_S, 'scanner_cfg_scan_interval_s', 'note_scanner_cfg', float(scan_interval_s))
        safe_set(SCANNER_CFG_MIN_REQUIRED_BPS, 'scanner_cfg_min_required_bps', 'note_scanner_cfg', float(min_required_bps))
        safe_set(SCANNER_CFG_MAX_PAIRS_PER_TICK, 'scanner_cfg_max_pairs_per_tick', 'note_scanner_cfg', float(max_pairs_per_tick))
    except Exception:
        OBS_NOOP_TOTAL.labels(metric='note_scanner_cfg', where='note_scanner_cfg').inc()


def discovery_note_stage(stage: str, count: int) -> None:
    try:
        DISCOVERY_PAIRS_TOTAL.labels(stage=_norm(stage)).inc(float(max(0, int(count))))
    except Exception:
        OBS_NOOP_TOTAL.labels(metric='discovery_pairs_total', where='discovery_note_stage').inc()


def discovery_note_filtered(reason: str, count: int) -> None:
    try:
        DISCOVERY_FILTERED_TOTAL.labels(reason=_norm(reason)).inc(float(max(0, int(count))))
    except Exception:
        OBS_NOOP_TOTAL.labels(metric='discovery_filtered_total', where='discovery_note_filtered').inc()

DISCOVERY_FILTER_REASONS = {
    "api_error",
    "combo_not_eligible",
    "denylist",
    "min_volume",
    "missing_base_quote",
    "missing_quote_field",
    "no_enabled_exchanges",
    "unknown_region_hint",
    "parse_error",
    "pk_collision",
    "quote_not_allowed",
    "rank_cutoff",
    "region_disabled_jp",
    "whitelist",
}

WS_PUBLIC_DROP_REASONS = {
    "queue_full",
    "parse_error",
    "schema_mismatch",
    "unknown_pair",
    "region_disabled_jp",
    "disabled_by_flag",
    "stale",
}

ROUTER_DROP_REASONS = {
    "bad_ts_negative_age",
    "dedup_coalesce",
    "exception",
    "inactive",
    "l2_missing",
    "missing_quote",
    "pair_unmapped",
    "queue_full",
    "schema_missing_field",
    "schema_mismatch",
    "stale_source",
    "unit_mismatch",
}

def discovery_note_api_error(exchange: str) -> None:
    try:
        DISCOVERY_API_ERRORS_TOTAL.labels(exchange=_norm(exchange)).inc()
    except Exception:
        OBS_NOOP_TOTAL.labels(metric='discovery_api_errors_total', where='discovery_note_api_error').inc()

def discovery_note_rate_limit(exchange: str) -> None:
    try:
        DISCOVERY_429_TOTAL.labels(exchange=_norm(exchange)).inc()
    except Exception:
        OBS_NOOP_TOTAL.labels(metric='discovery_429_total', where='discovery_note_rate_limit').inc()

def discovery_observe_run_ms(ms: float) -> None:
    try:
        safe_observe(DISCOVERY_RUN_MS, 'discovery_run_ms', 'discovery_observe_run_ms', float(ms))
    except Exception:
        OBS_NOOP_TOTAL.labels(metric='discovery_run_ms', where='discovery_observe_run_ms').inc()


SCANNER_DECISION_MS = _metric(Histogram, 'scanner_decision_ms', 'Scanner decision latency (ms)', buckets=BUCKETS_MS)
SCANNER_EVAL_MS = _metric(
    Histogram,
    'scanner_evaluate_ms',
    'Time to evaluate an opportunity',
    ['pair', 'route'],
    buckets=BUCKETS_MS,
)
SCANNER_REJECT_REASONS = {
    "fee_unknown",
    "rm_ack_timeout",
    "region_disabled_jp",
    "slip_unknown_or_stale",
    "vol_unknown_or_stale",
    "bad_ts_negative_age",
}

SLIP_REASONS = {
    "slip_unknown_or_stale",
}
WATCHDOG_SNAPSHOT_TOTAL = _metric(
    Counter,
    "watchdog_snapshot_total",
    "Watchdog health snapshots emitted",
    ["watchdog", "severity"],
)
WATCHDOG_DEGRADED_TOTAL = _metric(
    Counter,
    "watchdog_degraded_total",
    "Watchdog degraded reasons observed",
    ["watchdog", "reason"],
)
WATCHDOG_RESTART_INTENT_TOTAL = _metric(
    Counter,
    "watchdog_restart_intent_total",
    "Watchdog restart intents emitted",
    ["reason"],
)
VOL_REASONS = {
    "vol_unknown_or_stale",
}
WATCHDOG_FALLBACK_USED_TOTAL = _metric(
    Counter,
    "watchdog_fallback_used_total",
    "Watchdog fallback thresholds used",
    ["watchdog", "key"],
)
OBS_REASON_REGISTRY = {
    "router": ROUTER_DROP_REASONS,
    "ws_public": WS_PUBLIC_DROP_REASONS,
    "scanner": SCANNER_REJECT_REASONS,
    "slip": SLIP_REASONS,
    "vol": VOL_REASONS,
}
def watchdog_snapshot_total(watchdog: str, severity: str) -> None:
    try:
        WATCHDOG_SNAPSHOT_TOTAL.labels(_norm(watchdog), _norm(severity)).inc()
    except Exception:
        pass

def watchdog_degraded_total(watchdog: str, reason: str) -> None:
    try:
        WATCHDOG_DEGRADED_TOTAL.labels(_norm(watchdog), _norm(reason)).inc()
    except Exception:
        pass

def watchdog_restart_intent_total(reason: str) -> None:
    try:
        WATCHDOG_RESTART_INTENT_TOTAL.labels(_norm(reason)).inc()
    except Exception:
        pass
def watchdog_fallback_used(watchdog: str, key: str) -> None:
    try:
        WATCHDOG_FALLBACK_USED_TOTAL.labels(_norm(watchdog), _norm(key)).inc()
    except Exception:
        pass
SCANNER_GLOBAL_LOAD = _metric(Gauge, 'scanner_global_load', 'Scanner global load (0..1)')
SCANNER_RATE_LIMITED_TOTAL = _metric(Counter, 'scanner_rate_limited_total', 'Scanner rate limited hits', ['kind', 'cohort'])
SCANNER_EMITTED_TOTAL = _metric(Counter, 'scanner_emitted_total', 'Opportunities emitted')
SCANNER_CFG_SCAN_INTERVAL_S = _metric(Gauge, 'scanner_cfg_scan_interval_s', 'Configured scanner scan interval (seconds)')
SCANNER_CFG_MIN_REQUIRED_BPS = _metric(Gauge, 'scanner_cfg_min_required_bps', 'Configured scanner min_required_bps (bps)')
SCANNER_CFG_MAX_PAIRS_PER_TICK = _metric(Gauge, 'scanner_cfg_max_pairs_per_tick', 'Configured scanner max pairs per tick')
DISCOVERY_PAIRS_TOTAL = _metric(Counter, 'discovery_pairs_total', 'Pairs observed during discovery', ['stage'])
DISCOVERY_FILTERED_TOTAL = _metric(Counter, 'discovery_filtered_total', 'Pairs filtered during discovery', ['reason'])
DISCOVERY_RUN_MS = _metric(Histogram, 'discovery_run_ms', 'Discovery run duration (ms)', buckets=BUCKETS_MS)
DISCOVERY_API_ERRORS_TOTAL = _metric(Counter, 'discovery_api_errors_total', 'Discovery API errors', ['exchange'])
VOL_TTL_SECONDS_CONFIG = _metric(Gauge, 'vol_ttl_seconds_config', 'Configured volatility TTL (seconds)')
DISCOVERY_429_TOTAL = _metric(Counter, 'discovery_429_total', 'Discovery 429 responses', ['exchange'])
SLIP_TTL_SECONDS_CONFIG = _metric(Gauge, 'slip_ttl_seconds_config', 'Configured slippage TTL (seconds)')
SLIP_SAMPLE_TOTAL = _metric(Counter, 'slip_sample_total', 'Slippage samples ingested')
SLIP_DECISION_TOTAL = _metric(Counter, 'slip_decision_total', 'Slippage decisions recorded')
SLIP_P95_BPS = _metric(Gauge, 'slip_p95_bps', 'Slippage p95 (bps)')
SLIP_P99_BPS = _metric(Gauge, 'slip_p99_bps', 'Slippage p99 (bps)')
SCANNER_REJECTIONS_TOTAL = _metric(Counter, 'scanner_rejections_total', 'Opportunities rejected', ['reason'])
SCANNER_QUEUE_DROPPED_TOTAL = _metric(
    Counter,
    'scanner_queue_dropped_total',
    'Scanner queue drops due to backpressure',
    ['reason'],
)
SC_STRATEGY_SCORE = _metric(Gauge, 'sc_strategy_score', 'Strategy score', ['pair', 'route', 'branch'])
SC_ELIGIBLE = _metric(Gauge, 'sc_eligible', 'Pair eligibility flag', ['pair'])
SLIP_DROP_TOTAL = _metric(
    Counter,
    'slip_drop_total',
    'Slippage bus events dropped',
    ['exchange', 'reason'],
)
SC_BANNED = _metric(Gauge, 'sc_banned', 'Pair banned flag', ['pair'])
SC_PROMOTED_PRIMARY = _metric(Counter, 'sc_promoted_primary_total', 'Promotions to PRIMARY', ['pair'])
SC_ROTATION_PRIMARY_SIZE = _metric(Gauge, 'sc_rotation_primary_size', 'Rotation PRIMARY size')
SC_ROTATION_AUDITION_SIZE = _metric(Gauge, 'sc_rotation_audition_size', 'Rotation AUDITION size')
RM_DECISION_MS = _metric(Histogram, 'rm_decision_ms', 'RiskManager decision latency (ms)', ['cohort'], buckets=BUCKETS_MS)
RM_REVALIDATE_MS = _metric(Histogram, 'rm_revalidate_ms', 'revalidate_arbitrage duration (ms)')
RM_FRAGMENT_PROFIT_MS = _metric(Histogram, 'rm_fragment_profit_ms', 'is_fragment_profitable duration (ms)')
RM_PREFLIGHT_MS = _metric(Histogram, 'rm_preflight_ms', 'Pre-flight gate duration (ms)')
RM_DECISIONS_TOTAL = _metric(Counter, 'rm_decisions_total', 'RM decisions', ['status'])
RM_SKIPS_TOTAL = _metric(Counter, 'rm_skips_total', 'RM skipped reasons', ['reason'])
RM_QUEUE_DEPTH = _metric(Gauge, 'rm_queue_depth', 'Queue depth per stage', ['stage'])
RM_FINAL_DECISIONS_TOTAL = _metric(Counter, 'rm_final_decisions_total', 'Final decisions emitted by RM, per route', ['route'])
RM_ADMITTED_TOTAL = _metric(Counter, 'rm_admitted_total', 'Opportunities admitted by the RM', labelnames=('cohort',))
RM_REB_LOCK_SET_TOTAL = _metric(Counter, 'rm_reb_lock_set_total', 'Total number of REB locks set', ['pair', 'route', 'source'])
RM_REB_LOCK_ACTIVE = _metric(Gauge, 'rm_reb_lock_active', 'Number of active REB locks')
RM_DROPPED_TOTAL = _metric(
    Counter,
    'rm_dropped_total',
    'Opportunities dropped by the RM',
    labelnames=('reason',),
)
RM_RESERVED_FUNDS_ACTIVE = _metric(Gauge, 'rm_reserved_funds_active', 'Active reserved funds in RM', ['exchange', 'alias', 'ccy'])
RM_INVALID_PRIVATE_EVENT_TOTAL = _metric(
    Counter,
    'rm_invalid_private_event_total',
    'Invalid private events observed by RM',
    ['exchange', 'alias', 'reason'],
)
RM_SHUTDOWN_SECONDS = _metric(
    Histogram,
    'rm_shutdown_seconds',
    'RiskManager shutdown duration (seconds)',
)
RM_SHUTDOWN_TIMEOUT_TOTAL = _metric(
    Counter,
    'rm_shutdown_timeout_total',
    'RiskManager shutdowns that hit timeout with pending tasks',
)
RM_SHUTDOWN_PENDING_TASKS = _metric(
    Gauge,
    'rm_shutdown_pending_tasks',
    'Number of RiskManager tasks still pending after shutdown timeout',
)

RM_TRADING_READY = _metric(
    Gauge,
    'rm_trading_ready',
    'RiskManager trading readiness (1=ready)',
)
RM_PIPELINE_READY = _metric(
    Gauge,
    'rm_pipeline_ready',
    'RiskManager pipeline readiness (deps OK; safe to run in DRY_RUN or PROD with kill-switch)',
)

RM_DEP_READY = _metric(
    Gauge,
    'rm_dep_ready',
    'RiskManager dependency readiness',
    labelnames=('dep',),
)
RM_CALLBACK_LATENCY_MS = _metric(
    Histogram,
    'rm_callback_latency_ms',
    'Latency of RM callbacks',
    buckets=(0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000),
    labelnames=('cb_name',),
)
RM_CALLBACK_DROPS_TOTAL = _metric(
    Counter,
    'rm_callback_drops_total',
    'Dropped RM callbacks',
    labelnames=('cb_name', 'reason'),
)
RM_CALLBACK_INFLIGHT = _metric(
    Gauge,
    'rm_callback_inflight',
    'Inflight RM callbacks',
    labelnames=('cb_name',),
)

RM_MODE_CURRENT = _metric(
    Gauge,
    'rm_mode_current',
    'Current RM mode (NORMAL=0, OPP_VOLUME=1, OPP_VOL=2, SEVERE=3)',
    ['mode'],
)
RM_TRADE_MODE_CURRENT = _metric(
    Gauge,
    'rm_trade_mode_current',
    'Current trade mode exposed to Engine (0=NORMAL,1=CONSTRAINED,2=SEVERE,3=OPPORTUNISTE)',
    ['mode'],
)
RM_MODE_ENTRIES_TOTAL = _metric(
    Counter,
    'rm_mode_entries_total',
    'RM mode entries',
    ['mode', 'reason'],
)
RM_MODE_EXITS_TOTAL = _metric(
    Counter,
    'rm_mode_exits_total',
    'RM mode exits with reason',
    ['mode', 'reason'],
)
RM_MODE_ABORT_TOTAL = _metric(
    Counter,
    'rm_mode_abort_total',
    'RM mode veto/abort by guard',
    ['mode', 'guard'],
)


def rm_update_mode_gauges(rm_mode: str, trade_mode: str) -> None:
    """
    Helper RM pour exposer rm_mode / trade_mode sur les métriques de mode.

    - rm_mode_current{mode="..."} = 1 pour le mode courant, 0 pour les autres.
    - rm_trade_mode_current{mode="..."} = 1 pour le mode courant, 0 pour les autres.

    Fallback: en cas de mismatch de labels (legacy), on expose un entier encodé.
    """
    try:
        rm_mode_u = str(rm_mode or "NORMAL").upper()
        trade_mode_u = str(trade_mode or "NORMAL").upper()

        rm_rank = {"NORMAL": 0, "OPP_VOLUME": 1, "OPP_VOL": 2, "SEVERE": 3}
        trade_rank = {"NORMAL": 0, "CONSTRAINED": 1, "SEVERE": 2, "OPPORTUNISTE": 3}

        # One-hot sur label "mode" pour rm_mode
        try:
            for m in ("NORMAL", "OPP_VOLUME", "OPP_VOL", "SEVERE"):
                RM_MODE_CURRENT.labels(mode=m).set(1.0 if m == rm_mode_u else 0.0)
        except Exception:
            try:
                RM_MODE_CURRENT.set(float(rm_rank.get(rm_mode_u, -1)))
            except Exception:
                pass

        # One-hot sur label "mode" pour trade_mode
        try:
            for m in ("NORMAL", "CONSTRAINED", "SEVERE", "OPPORTUNISTE"):
                RM_TRADE_MODE_CURRENT.labels(mode=m).set(
                    1.0 if m == trade_mode_u else 0.0
                )
        except Exception:
            try:
                RM_TRADE_MODE_CURRENT.set(float(trade_rank.get(trade_mode_u, -1)))
            except Exception:
                pass
    except Exception:
        _obs_shim_log.exception("rm_update_mode_gauges failed")


STALE_OPPORTUNITY_DROPPED_TOTAL = _metric(Counter, 'stale_opportunity_dropped_total', 'Opportunities dropped due to stale/invalid orderbooks')
PAIR_HEALTH_PENALTY_TOTAL = _metric(Counter, 'pair_health_penalty_total', 'Pair-level penalties applied (circuit-breakers)', labelnames=('pair', 'reason'))
POOL_GATE_THROTTLES_TOTAL = _metric(Counter, 'pool_gate_throttles_total', 'Pool gate throttles fired (insufficient quote buffer)', labelnames=('quote',))
RM_BALANCES_TTL_BREACH = _metric(
    Counter,
    'rm_balances_ttl_breach_total',
    'TTL balance breaches detected by RM',
    ['exchange', 'alias', 'status'],
)
RM_BALANCES_STALE_TOTAL = _metric(
    Counter,
    'rm_balances_stale_total',
    'Stale balance occurrences detected by RM',
    ['exchange', 'alias', 'status'],
)
RM_TM_EXPOSURE_TTL_MS = _metric(
    Gauge,
    'rm_tm_exposure_ttl_ms',
    'Configured TM exposure TTL in ms per exchange (SLO private)',
    ['exchange'],
)
RM_TM_EXPOSURE_TTL_BREACH_TOTAL = _metric(
    Counter,
    'rm_tm_exposure_ttl_breach_total',
    'Number of times TM exposure TTL was breached',
    ['exchange', 'asset', 'level'],
)
RM_CAPITAL_MOVE_VISIBILITY_LATENCY_S = _metric(
    Histogram,
    'rm_capital_move_visibility_latency_seconds',
    'Latency for capital move visibility (seconds)',
    ['exchange', 'alias', 'status'],
)
RM_CAPITAL_MOVE_VISIBILITY_TOTAL = _metric(
    Counter,
    'rm_capital_move_visibility_total',
    'Capital move visibility events observed by RM',
    ['exchange', 'alias', 'status'],
)
RM_CAPITAL_MOVE_TOTAL = _metric(
    Counter,
    'rm_capital_move_total',
    'Capital move events emitted by RM',
    ['exchange', 'subtype', 'source', 'status'],
)
RM_CAPITAL_MOVE_NOTIONAL_USD = _metric(
    Histogram,
    'rm_capital_move_notional_usd',
    'Capital move notional (USD)',
    ['exchange', 'subtype', 'source'],
)

def mark_scanner_to_rm(ok: bool, dt_ms: float, **labels: Any) -> None:
    try:
        SCANNER_DECISION_MS.observe(max(0.0, float(dt_ms)))
        if not ok:
            inc_blocked('scanner_to_rm', labels.get('reason', 'unknown'), labels.get('pair'))
    except Exception:
        pass


def mark_scanner_to_rm_ts(
    ts_start_ns: int,
    *,
    ok: bool = True,
    reason: str = "ok",
    **labels: Any,
) -> None:
    """
    Wrapper basé sur un timestamp perf_counter_ns() pour Scanner→RM.

    - ts_start_ns: timestamp de départ en ns (perf_counter_ns()).
    - ok: True si l'appel RM a abouti sans exception, False sinon.
    - reason: code raison ("ok", "async_task", "no_callback", "exception", ...).
    - labels: labels additionnels (pair, route, ...).
    """
    try:
        now_ns = time.perf_counter_ns()
        dt_ms = max(0.0, (float(now_ns - int(ts_start_ns)) / 1e6))
        enriched = {
            "region": labels.get("region"),
            "strategy": labels.get("strategy"),
            "pair": labels.get("pair"),
            "reason": reason,
        }
        mark_scanner_to_rm(ok, dt_ms, **enriched)
    except Exception:
        # Best-effort
        pass

INVENTORY_USD = _metric(Gauge, 'inventory_usd', 'Inventory (quote units) by exchange/quote', ['exchange', 'quote'])
RM_REJECT_TOTAL = _metric(Counter, 'rm_reject_total', 'RM rejections', ['reason'])
PAIR_HEALTH_PENALTY_TOTAL = _metric(Counter, 'pair_health_penalty_total', 'Pair health penalties', ['pair', 'reason'])
VOL_EWMA_BPS = _metric(Gauge, 'vol_ewma_bps', 'EWMA volatility (bps)', ['pair'])
VOL_P95_BPS = _metric(Gauge, 'vol_p95_bps', 'P95 volatility (bps)', ['pair'])
VOL_BAND_TOTAL = _metric(Counter, 'vol_band_total', 'Volatility band counts', ['band'])
FEE_SNAPSHOT_AGE_SECONDS = _metric(Gauge, 'fee_snapshot_age_seconds', 'Fee snapshot age (seconds)', ['exchange', 'alias'])
TOTAL_COST_BPS = _metric(Gauge, 'total_cost_bps', 'Total cost (fees+slip) in bps', ['route', 'side'])
FEE_MISMATCH_TOTAL = _metric(Counter, 'fee_mismatch_total', 'Fee reality-check mismatches', ['exchange', 'alias', 'side'])
FEES_EXPECTED_BPS = _metric(Gauge, 'fees_expected_bps', 'Expected fees (bps)', ['exchange', 'alias'])
FEES_REALIZED_BPS = _metric(Gauge, 'fees_realized_bps', 'Realized fees (bps)', ['exchange', 'alias'])
FEESYNC_LAST_TS = _metric(Gauge, 'feesync_last_ts_seconds', 'Last fee sync ts (epoch s)', ['exchange', 'alias'])
FEESYNC_ERRORS = _metric(Counter, 'feesync_errors_total', 'Fee sync errors', ['exchange', 'alias'])
REBAL_DETECTED_TOTAL = _metric(Counter, 'rebal_detected_total', 'Rebalancing detected', ['status'])
REBAL_PLAN_QUANTUM_QUOTE = _metric(Gauge, 'rebal_plan_quantum_quote', 'Rebalancing plan quantum (quote units)', ['quote'])
REBAL_CROSS_TOO_EXPENSIVE_TOTAL = _metric(
    Counter,
    'rebal_cross_too_expensive_total',
    'Cross-CEX opportunities rejected because estimated net bps is below the allowed loss',
    ['stage']
)

RM_PAUSED_COUNT = _metric(Gauge, 'rm_paused_count', 'Number of paused routes/pairs')
LAST_BOOKS_FRESH_TS = _metric(Gauge, 'last_books_fresh_ts_seconds', 'Last books fresh ts', ['pair'])
LAST_BALANCES_FRESH_TS = _metric(Gauge, 'last_balances_fresh_ts_seconds', 'Last balances fresh ts', ['exchange', 'alias'])
DYNAMIC_MIN_BPS = _metric(Gauge, 'dynamic_min_bps', 'Dynamic min required bps', ['pair', 'side'])

def mark_books_fresh(pair: str) -> None:
    try:
        LAST_BOOKS_FRESH_TS.labels(_norm(pair)).set(time.time())
    except Exception:
        pass

def mark_balances_fresh(exchange: str, alias: str) -> None:
    try:
        LAST_BALANCES_FRESH_TS.labels(lbl_exchange(exchange), lbl_alias(alias)).set(time.time())
    except Exception:
        pass

def set_rm_paused_count(n: int) -> None:
    try:
        RM_PAUSED_COUNT.set(max(0, int(n)))
    except Exception:
        pass

def set_dynamic_min(pair: str, side: str, bps: float) -> None:
    try:
        DYNAMIC_MIN_BPS.labels(_norm(pair), _norm(side)).set(float(bps))
    except Exception:
        pass

def inc_rm_reject(reason: str) -> None:
    try:
        RM_REJECT_TOTAL.labels(_norm(reason)).inc()
        # Ticket D4: Unified Robustness
        if 'REJECT_TOTAL_COUNTER' in globals():
            REJECT_TOTAL_COUNTER.labels(layer="RM", reason=_norm(reason)).inc()
    except Exception:
        pass
def inc_rm_skip(reason: str) -> None:
    try:
        RM_SKIPS_TOTAL.labels(_norm(reason)).inc()
    except Exception:
        pass

def mark_rm_to_engine(ok: bool, dt_ms: float, **labels: Any) -> None:
    try:
        RM_DECISION_MS.observe(max(0.0, float(dt_ms)))
        if not ok:
            inc_blocked('rm_to_engine', labels.get('reason', 'unknown'), labels.get('pair'))
    except Exception:
        pass
MM_FILLS_BOTH = _metric(Counter, 'mm_fills_both_total', 'Both maker orders filled (MM) before TTL; no hedge needed', ['pair', 'variant'])
MM_SINGLE_FILL_HEDGED = _metric(Counter, 'mm_single_fill_hedged_total', 'Single maker order filled (MM) then hedged with a taker', ['pair', 'variant'])
MM_PANIC_HEDGE_TOTAL = _metric(Counter, 'mm_panic_hedge_total', 'Panic hedge triggered due to exception/timeout during MM', ['pair', 'variant'])
MM_THROTTLED_TOTAL = _metric(
    Counter,
    'mm_throttled_total',
    'MM throttle hits by action',
    ['reason', 'exchange', 'pair', 'variant'],
)
MM_MAKERS_EXPIRED_TTL_TOTAL = _metric(
    Counter,
    'mm_makers_expired_ttl_total',
    'MM makers expired on TTL enforcement',
    ['exchange', 'pair', 'variant'],
)
MM_MAKERS_CANCELED_TOTAL = _metric(
    Counter,
    'mm_makers_canceled_total',
    'MM makers canceled proactively',
    ['reason', 'exchange', 'pair', 'variant'],
)
MM_EXPECTED_CAPTURE_BPS = _metric(
    Histogram,
    'mm_expected_capture_bps',
    'Capture attendue en bps (hints)',
    ['pair', 'variant'],
    buckets=(-10, -5, 0, 2, 5, 10, 20, 50, 100)
)
MM_REALIZED_BPS = _metric(
    Histogram,
    'mm_realized_bps',
    'Capture réalisée en bps (post-trade)',
    ['pair', 'variant'],
    buckets=(-100, -50, -20, -10, -5, 0, 2, 5, 10, 20, 50, 100)
)
MM_CANCEL_BUDGET_EXHAUSTED_TOTAL = _metric(
    Counter,
    'mm_cancel_budget_exhausted_total',
    'Nombre de fois où le budget de cancel MM est épuisé',
    ['exchange', 'pair']
)
MM_INVENTORY_DRIFT_USD = _metric(
    Gauge,
    'mm_inventory_drift_usd',
    'Drift inventaire courant en USD',
    ['asset']
)
MM_INVENTORY_MODE_TRANSITIONS_TOTAL = _metric(
    Counter,
    'mm_inventory_mode_transitions_total',
    'Nombre de transitions DUAL <-> SINGLE',
    ['old', 'new', 'asset']
)
MM_SINGLE_TIME_SECONDS = _metric(
    Gauge,
    'mm_single_time_seconds',
    'Temps passe en mode SINGLE par asset',
    ['asset']
)
MM_SINGLE_ESCALATIONS_TOTAL = _metric(
    Counter,
    'mm_single_escalations_total',
    'Nombre d escalades en mode SINGLE',
    ['asset', 'phase']
)
MM_SINGLE_BLOCKED_TOTAL = _metric(
    Counter,
    'mm_single_blocked_total',
    'Nombre de blocages en mode SINGLE par raison',
    ['asset', 'reason']
)
MM_SINGLE_PLACES_TOTAL = _metric(
    Counter,
    'mm_single_places_total',
    'Nombre de placements en mode SINGLE',
    ['exchange', 'pair', 'variant']
)
MM_SINGLE_CANCELS_TOTAL = _metric(
    Counter,
    'mm_single_cancels_total',
    'Nombre d annulations en mode SINGLE',
    ['exchange', 'pair', 'variant']
)
MM_CROSS_BLOCKED_TOTAL = _metric(
    Counter,
    'mm_cross_blocked_total',
    'Number of cross-cex MM opportunities blocked',
    ['reason', 'pair']
)
MM_POST_FILL_MOVE_BPS = _metric(
    Histogram,
    'mm_post_fill_move_bps',
    'Mouvement de prix post-fill en bps (adverse selection proxy)',
    ['pair', 'variant'],
    buckets=(-50, -20, -10, -5, -2, 0, 2, 5, 10, 20, 50)
)
MM_CHURN_RATE = _metric(
    Gauge,
    'mm_churn_rate',
    'Taux de changement du carnet (updates/s)',
    ['exchange', 'pair']
)
MM_LIFETIME_MS = _metric(
    Histogram,
    'mm_lifetime_ms',
    'Quote lifetime before cancel/fill (ms)',
    ['exchange', 'pair', 'variant'],
    buckets=(100, 250, 500, 1000, 2500, 5000, 10000)
)
MM_DISTANCE_TO_BEST_BPS = _metric(
    Histogram,
    'mm_distance_to_best_bps',
    'Distance to best bid/ask in bps',
    ['exchange', 'pair', 'side'],
    buckets=(0, 1, 2, 5, 10, 20, 50)
)
MM_CLAMP_TOTAL = _metric(
    Counter,
    'mm_clamp_total',
    'Total number of hard price clamps applied',
    ['exchange', 'pair', 'side']
)
MM_BOOK_STALE_TOTAL = _metric(
    Counter,
    'mm_book_stale_total',
    'Total number of MM orders rejected due to stale orderbook',
    ['exchange', 'pair']
)
MM_JUMP_GUARD_TOTAL = _metric(
    Counter,
    'mm_jump_guard_total',
    'Total number of MM jump guard freezes',
    ['exchange', 'pair']
)
MM_STICKY_SKIPS_TOTAL = _metric(
    Counter,
    'mm_sticky_skips_total',
    'Total number of MM quote replaces skipped due to sticky band or cooldown',
    ['exchange', 'pair', 'side', 'reason']
)
MM_POSTONLY_REJECT_TOTAL = _metric(
    Counter,
    'mm_postonly_reject_total',
    'Total number of MM orders rejected by exchange as post-only',
    ['exchange', 'pair', 'side']
)
MM_LIVE_QUOTES = _metric(
    Gauge,
    'mm_live_quotes',
    'Current number of active MM quotes',
    ['exchange', 'pair', 'side']
)
MM_LADDER_LEVELS_USED = _metric(
    Gauge,
    'mm_ladder_levels_used',
    'Number of ladder levels actually being quoted',
    ['exchange', 'pair']
)
MM_DISTANCE_TO_BEST_TICKS = _metric(
    Histogram,
    'mm_distance_to_best_ticks',
    'Distance to best bid/ask in ticks',
    ['exchange', 'pair', 'side'],
    buckets=(0, 0.5, 1, 2, 5, 10, 20)
)
MM_PLACE_TOTAL = _metric(
    Counter,
    'mm_place_total',
    'Total MM place orders',
    ['exchange', 'pair', 'side']
)
MM_CANCEL_TOTAL = _metric(
    Counter,
    'mm_cancel_total',
    'Total MM cancel orders',
    ['exchange', 'pair', 'side']
)
MM_REPLACE_TOTAL = _metric(
    Counter,
    'mm_replace_total',
    'Total MM replace (cancel+place) cycles',
    ['exchange', 'pair', 'side']
)
MM_TOXICITY_SCORE = _metric(
    Gauge,
    'mm_toxicity_score',
    'Current estimated toxicity score (0..1)',
    ['exchange', 'pair']
)
MM_DEPTH_PROFILE_RATIO = _metric(
    Gauge,
    'mm_depth_profile_ratio',
    'Ratio volume L1 / top 5 (fragilité)',
    ['exchange', 'pair']
)
MM_CROSS_SINGLE_FILL_TOTAL = _metric(
    Counter,
    'mm_cross_single_fill_total',
    'MM Cross single leg filled',
    ['pair']
)
MM_CROSS_BOTH_FILL_TOTAL = _metric(
    Counter,
    'mm_cross_both_fill_total',
    'MM Cross both legs filled',
    ['pair']
)
MM_CROSS_DEFENSIVE_MODE = _metric(
    Gauge,
    'mm_cross_defensive_mode',
    'MM Cross defensive mode state',
    ['pair']
)
MM_CROSS_HEDGE_SLIPPAGE_BPS = _metric(
    Histogram,
    'mm_cross_hedge_slippage_bps',
    'MM Cross hedge slippage in bps',
    ['pair'],
    buckets=(-100, -50, -20, -10, -5, 0, 5, 10, 20, 50, 100)
)
ENGINE_SUBMIT_TO_ACK_MS = _metric(Histogram, 'engine_submit_to_ack_ms', 'Engine submit→ack latency (ms)', buckets=BUCKETS_MS)
ENGINE_ACK_TO_FILL_MS = _metric(Histogram, 'engine_ack_to_fill_ms', 'Engine ack→fill latency (ms)', buckets=BUCKETS_MS)
ENGINE_CANCELLATIONS_TOTAL = _metric(Counter, 'engine_cancellations_total', 'Engine cancellations', ['exchange', 'pair', 'reason'])
ENGINE_RETRIES_TOTAL = _metric(Counter, 'engine_retries_total', 'Engine retries', ['exchange', 'pair', 'reason'])
ENGINE_QUEUEPOS_BLOCKED_TOTAL = _metric(Counter, 'engine_queuepos_blocked_total', 'Engine TM queuepos blocked', ['exchange', 'pair'])
ENGINE_SUBMIT_QUEUE_DEPTH = _metric(Gauge, 'engine_submit_queue_depth', 'Engine submit queue depth')
INFLIGHT_GAUGE = _metric(Gauge, 'engine_inflight', 'Engine inflight orders')
INFLIGHT_LANE_INUSE = _metric(
    Gauge,
    'engine_inflight_lane_inuse',
    'Engine inflight orders by lane',
    ['exchange', 'lane'],
)
INFLIGHT_LANE_WAIT_MS = _metric(
    Histogram,
    'engine_inflight_lane_wait_ms',
    'Engine inflight wait time by lane (ms)',
    ['exchange', 'lane'],
    buckets=BUCKETS_MS if 'BUCKETS_MS' in globals() else (1, 2, 5, 10, 25, 50, 100, 250, 500, 1000),
)
ENGINE_DEDUP_HITS_TOTAL = _metric(Counter, 'engine_dedup_hits_total', 'Engine dedupe hits', ['source'])
# --- PnL live (Engine → Prometheus) ---
# PNL_LIVE_DAY_USD :
#   - Somme du net_profit réalisé pour la journée locale courante,
#     exprimée dans la devise PnL canonique (USDC/EUR) définie côté config.
#   - La "journée" est définie par région (EU/US/UTC) via la logique
#     de PnLAggregator (_now_local_day / reset par région).
#   - Vue live / best-effort de monitoring : la vérité PnL "comptable"
#     reste la DB SQLite gérée par LogWriter.

PNL_LIVE_DAY_USD = _metric(Gauge, 'pnl_live_day_usd', 'Live PnL for the current local day (USD)', ['region', 'branch', 'mode'])

# TRADES_LIVE_DAY_TOTAL :
#   - Compteur de trades du jour classés par résultat "win" / "loss" / "flat",
#     selon le signe de net_profit ou, à défaut, de net_profit_sign.

TRADES_LIVE_DAY_TOTAL = _metric(Counter, 'trades_live_day_total', 'Trades live day total', ['result', 'region', 'branch', 'mode'])
# DERIVED_NET_PROFIT_SIGN_TOTAL :
#   - Compteur de trades pour lesquels seul le signe net_profit_sign a été utilisé
#     (net_profit manquant ou jugé peu exploitable) pour classer win / loss / flat.

DERIVED_NET_PROFIT_SIGN_TOTAL = _metric(Counter, 'derived_net_profit_sign_total', 'Derived net profit sign (fallback)', ['reason', 'region', 'branch', 'mode'])

MISSING_NET_PROFIT_TOTAL = _metric(Counter, 'missing_net_profit_total', 'Missing net profit values', ['stage', 'region', 'branch', 'mode'])
# MISSING_NET_PROFIT_TOTAL :
#   - Compteur de trades pour lesquels aucun PnL exploitable n'était disponible
#     au moment de l'agrégation live (ni net_profit ni net_profit_sign utilisable).

# --- PnL reconciliation CEX ↔ DB (M5-D-2) ----------------------
# Ces métriques sont alimentées par LoggerHistoriqueManager lorsqu'il
# exécute une reco PnL pour une journée donnée:
#   - PNL_RECO_LAST_RUN_TS_SECONDS:
#       timestamp (epoch seconds) du dernier run réussi pour la région.
#   - PNL_RECO_STATE:
#       état de la reco pour (region, exchange, account_alias):
#       0 = OK, 1 = WARN, 2 = CRIT.
#   - PNL_RECO_ABS_DIFF_QUOTE:
#       |PnL_CEX - PnL_DB| exprimé dans la devise PnL canonique (ex. USDC).
#   - PNL_RECO_MISMATCH_TOTAL:
#       compteur de mismatches classés par niveau ("WARN"/"CRIT").
#   - PNL_RECO_ERRORS_TOTAL:
#       erreurs rencontrées pendant la reco (CEX unreachable, DB error, ...).

PNL_RECO_LAST_RUN_TS_SECONDS = _metric(
    Gauge,
    'pnl_reco_last_run_ts_seconds',
    'Last PnL reconciliation run (epoch seconds)',
    ['region'],
)

PNL_RECO_STATE = _metric(
    Gauge,
    'pnl_reco_state',
    'PnL reconciliation state (OK/MISMATCH/ERROR/SKIPPED)',
    ['region', 'exchange', 'account_alias', 'state'],
)

PNL_RECO_ABS_DIFF_QUOTE = _metric(
    Gauge,
    'pnl_reco_abs_diff_quote',
    'Absolute PnL diff between CEX and DB (quote currency)',
    ['region', 'exchange', 'account_alias'],
)

PNL_RECO_MISMATCH_TOTAL = _metric(
    Counter,
    'pnl_reco_mismatch_total',
    'PnL reconciliation mismatches by reason',
    ['region', 'exchange', 'account_alias', 'reason'],
)

PNL_RECO_ERRORS_TOTAL = _metric(
    Counter,
    'pnl_reco_errors_total',
    'PnL reconciliation errors',
    ['region', 'exchange', 'account_alias', 'stage'],
)

PNL_DERIVED_FROM_BPS_TOTAL = _metric(
    Counter,
    'pnl_derived_from_bps_total',
    'PnL derived from bps (approximate accounting)',
    ['route'],
)

PNL_VIEW_BUILD_MS = _metric(
    Histogram,
    'pnl_view_build_ms',
    'PnL view build latency (ms)',
    ['view'],
    buckets=BUCKETS_MS if 'BUCKETS_MS' in globals() else (1, 2, 5, 10, 25, 50, 100, 250, 500, 1000),
)

ENGINE_PACER_DELAY_MS = _metric(
    Gauge,
    'engine_pacer_delay_ms',
    'Engine pacer delay (ms)',
    ['region', 'profile', 'mode'],
)
ENGINE_RL_WAIT_MS = _metric(
    Histogram,
    'engine_rl_wait_ms',
    'Engine internal rate-limit wait time (ms)',
    ['exchange', 'lane'],
    buckets=BUCKETS_MS if 'BUCKETS_MS' in globals() else (1, 2, 5, 10, 25, 50, 100, 250, 500, 1000),
)
ENGINE_ORDER_REJECTED_TOTAL = _metric(
    Counter,
    'engine_order_rejected_total',
    'Engine orders rejected by exchange',
    ['exchange', 'reason'],
)
ENGINE_PACER_INFLIGHT_MAX = _metric(
    Gauge,
    'engine_pacer_inflight_max',
    'Engine pacer inflight cap',
    ['region', 'profile', 'mode'],
)
ENGINE_PACER_MODE = _metric(
    Gauge,
    'engine_pacer_mode',
    'Engine pacer mode (0=NORMAL,1=CONSTRAINED,2=SEVERE)',
    ['region', 'profile'],
)
ENGINE_PACER_UNAVAILABLE_TOTAL = _metric(
    Counter,
    'engine_pacer_unavailable_total',
    'Engine pacer unavailable events',
    ['reason'],
)
ENGINE_RM_OVERRIDES_TOTAL = _metric(
    Counter,
    'engine_rm_overrides_total',
    'Engine overrides applied for RM decisions',
)
ENGINE_RL_TIMEOUT_TOTAL = _metric(
    Counter,
    'engine_rate_limit_timeout_total',
    'Rate limiter timeouts caught by the engine',
    ['exchange'],
)
PACER_ACK_TARGET_MS = _metric(
    Gauge,
    'engine_pacer_ack_target_ms',
    'Pacer ack target SLO (ms)',
    ['region', 'profile'],
)
PACER_ACK_HI_MS = _metric(
    Gauge,
    'engine_pacer_ack_hi_ms',
    'Pacer ack hi SLO (ms)',
    ['region', 'profile'],
)
PACER_ACK_SEV_MS = _metric(
    Gauge,
    'engine_pacer_ack_sev_ms',
    'Pacer ack severe SLO (ms)',
    ['region', 'profile'],
)
ENGINE_DRAIN_LATENCY_MS = _metric(
    Histogram,
    'engine_drain_latency_ms',
    'Engine drain latency (ms)',
    buckets=BUCKETS_MS,
)

ENGINE_PACING_BACKPRESSURE_TOTAL = _metric(Counter, 'engine_pacing_backpressure_total', 'Engine pacing backpressure', ['reason'])
ENGINE_ACK_TIMEOUT_TOTAL = _metric(Counter, 'engine_ack_timeout_total', 'Engine ack timeouts')

# --- TT (Ticket D1, D2, D3, D4) ---
TT_PLAN_LATENCY_MS = _metric(Histogram, "tt_plan_latency_ms", "Latency of TT plan decision (ms)", buckets=[0.5, 1, 2, 5, 10, 20])
TT_PLAN_FRAGMENTS = _metric(Gauge, "tt_plan_fragments", "Number of fragments in last TT plan")
TT_PLAN_SUBMIT_MODE_TOTAL = _metric(Counter, "tt_plan_submit_mode_total", "Counter of TT plan submit modes", ["mode"])
TT_REVALIDATE_FAIL_TOTAL = _metric(Counter, "tt_revalidate_fail_total", "Counter of TT revalidation failures", ["reason"])
TT_EDGE_NET_AT_DECISION_BPS = _metric(Histogram, "tt_edge_net_at_decision_bps", "Edge net at decision time (bps)", buckets=[-50, -20, -10, 0, 5, 10, 20, 50, 100])
TT_EDGE_NET_AT_SUBMIT_BPS = _metric(Histogram, "tt_edge_net_at_submit_bps", "Edge net at submit time (bps)", buckets=[-50, -20, -10, 0, 5, 10, 20, 50, 100])
RM_LATENCY_PENALTY_BPS = _metric(Gauge, "rm_latency_penalty_bps", "Latency penalty applied by RM (bps)")
RM_STALENESS_PENALTY_BPS = _metric(Gauge, "rm_staleness_penalty_bps", "Staleness penalty applied by RM (bps)")
TT_ABORT_TOTAL = _metric(Counter, "tt_abort_total", "Total TT pipeline aborts", ["reason"])
TT_TIME_BUDGET_EXCEEDED_TOTAL = _metric(Counter, "tt_time_budget_exceeded_total", "Total TT time budget exceeded")
TT_HEALTH_BLOCK_TOTAL = _metric(Counter, "tt_health_block_total", "Total TT health blocks", ["reason", "exchange"])

# --- TM (Ticket D1, D2, D3, D4) ---
TM_PLAN_LATENCY_MS = _metric(Histogram, "tm_plan_latency_ms", "Latency of TM plan decision (ms)", buckets=[0.5, 1, 2, 5, 10, 20])
TM_PLAN_FRAGMENTS = _metric(Gauge, "tm_plan_fragments", "Number of fragments in last TM plan")
TM_PLAN_LEVELS = _metric(Gauge, "tm_plan_levels", "Number of ladder levels in TM plan")
TM_REVALIDATE_FAIL_TOTAL = _metric(Counter, "tm_revalidate_fail_total", "Counter of TM revalidation failures", ["reason"])
TM_ABORT_TOTAL = _metric(Counter, "tm_abort_total", "Total TM pipeline aborts", ["reason"])
TM_EDGE_NET_BPS_AT_PLACE = _metric(Histogram, "tm_edge_net_bps_at_place", "Edge net BPS at maker placement", buckets=[-20, -10, -5, 0, 5, 10, 20, 50, 100])
TM_QPOS_BLOCK_TOTAL = _metric(Counter, "tm_qpos_block_total", "TM Queue Position blocks", ["reason", "exchange", "symbol"])
TM_QPOS_AHEAD_QUOTE = _metric(Gauge, "tm_qpos_ahead_quote", "Ahead quote amount", ["exchange", "symbol"])
TM_ETA_S = _metric(Gauge, "tm_eta_s", "Estimated time to fill in seconds", ["exchange", "symbol"])

MM_QPOS_BLOCK_TOTAL = _metric(Counter, "mm_qpos_block_total", "MM Queue Position blocks", ["reason", "exchange", "symbol"])
MM_QPOS_AHEAD_QUOTE = _metric(Gauge, "mm_qpos_ahead_quote", "Ahead quote amount", ["exchange", "symbol"])
MM_ETA_S = _metric(Gauge, "mm_eta_s", "Estimated time to fill in seconds", ["exchange", "symbol"])
MM_QUOTES_TOTAL = _metric(Counter, "mm_quotes_total", "Total MM quotes submitted", ["exchange", "pair", "variant"])
MM_REQUOTES_TOTAL = _metric(Counter, "mm_requotes_total", "Total MM quotes updated/replaced", ["exchange", "pair"])
MM_CANCELS_TOTAL = _metric(Counter, "mm_cancels_total", "Total MM quotes cancelled", ["exchange", "pair", "reason"])
MM_FILLS_TOTAL = _metric(Counter, "mm_fills_total", "Total MM fills", ["exchange", "pair", "variant"])
MM_REJECT_TOTAL = _metric(Counter, "mm_reject_total", "Total MM order rejects", ["reason"])
MM_ABORT_TOTAL = _metric(Counter, "mm_abort_total", "Total MM pipeline aborts", ["reason"])
MM_PAUSE_TOTAL = _metric(Counter, "mm_pause_total", "Total MM pauses (circuit breakers)", ["reason"])
MM_LADDER_PLACES_TOTAL = _metric(
    Counter,
    "mm_ladder_places_total",
    "Total MM ladder placements",
    ["exchange", "side", "variant", "profile", "level_idx"],
)
MM_LADDER_LEVELS_USED = _metric(
    Gauge,
    "mm_ladder_levels_used",
    "MM ladder levels used (latest)",
    ["variant", "profile"],
)

MM_POSTFILL_MOVE_BPS = _metric(Histogram, "mm_postfill_move_bps", "Price move after MM fill (bps)", ["exchange", "pair"], buckets=[-50, -20, -10, -5, 0, 5, 10, 20, 50])
MM_EXPECTED_CAPTURE_BPS = _metric(Histogram, "mm_expected_capture_bps", "Expected capture at MM placement", ["pair"], buckets=[0, 2, 5, 10, 20, 50, 100])
MM_INVENTORY_DRIFT_BPS = _metric(Gauge, "mm_inventory_drift_bps", "MM inventory drift from target", ["pair"])
TM_QUOTES_ALIVE = _metric(Gauge, "tm_quotes_alive", "Number of active TM quotes", ["pair"])
TM_EXPOSURE_QUOTE = _metric(Gauge, "tm_exposure_quote", "TM exposure in quote currency", ["pair"])
TM_EXPOSURE_USD = _metric(Gauge, "tm_exposure_usd", "TM exposure in USD", ["pair"])
TM_HEDGE_SUBMIT_TO_ACK_MS = _metric(Histogram, "tm_hedge_submit_to_ack_ms", "TM hedge submit to ack latency", buckets=[5, 10, 20, 50, 100, 200, 500])
TM_HEDGE_FAIL_TOTAL = _metric(Counter, "tm_hedge_fail_total", "TM hedge submission failures", ["reason"])
TM_FALLBACK_TOTAL = _metric(Counter, "tm_fallback_total", "TM fallback mode activations", ["mode"])


# --- UNIFIED FRAGMENTATION & EXECUTION (Ticket Slicing/Fragmentation) ---
FRAG_PLAN_LATENCY_MS = _metric(Histogram, 'frag_plan_latency_ms', 'Fragmentation plan latency (ms)', ['branch'], buckets=BUCKETS_MS)
FRAG_FRAGMENTS_COUNT = _metric(Histogram, 'frag_fragments_count', 'Number of fragments in plan', ['branch'], buckets=(1, 2, 3, 5, 8, 12, 16, 32))
FRAG_FRAGMENT_QUOTE_HIST = _metric(Histogram, 'frag_fragment_quote_hist', 'Fragment size (quote ccy)', ['branch'], buckets=(10, 50, 100, 200, 500, 1000, 2500, 5000, 10000))
FRAG_ABORT_REASON_TOTAL = _metric(Counter, 'frag_abort_reason_total', 'Total plan aborts', ['branch', 'reason'])

# Latences
RM_DECISION_MS_HIST = _metric(Histogram, 'rm_decision_ms', 'RM decision latency (ms)', buckets=BUCKETS_MS)
ENGINE_QUEUE_DELAY_MS = _metric(Histogram, 'engine_queue_delay_ms', 'Engine queue delay (ms)', buckets=BUCKETS_MS)
ENGINE_SUBMIT_TO_ACK_MS_HIST = _metric(Histogram, 'engine_submit_to_ack_ms', 'Submit to ack latency (ms)', ['exchange'], buckets=BUCKETS_MS)
PRIVATE_WS_LAG_MS = _metric(Histogram, 'private_ws_lag_ms', 'Private WS lag (ms)', ['exchange'], buckets=BUCKETS_MS)
RPC_LATENCY_MS_HIST = _metric(Histogram, 'rpc_latency_ms', 'RPC gateway latency (ms)', buckets=BUCKETS_MS)

# Qualité
EDGE_NET_BPS_HIST = _metric(Histogram, 'edge_net_bps_hist', 'Realized edge net bps', ['branch'], buckets=(-100, -50, -20, -10, 0, 5, 10, 20, 50, 100))
FILL_RATIO_HIST = _metric(Histogram, 'fill_ratio', 'Fill ratio (filled/planned)', ['branch'], buckets=(0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0, 1.1))
PANIC_HEDGE_TOTAL_COUNTER = _metric(Counter, 'panic_hedge_total', 'Total panic hedges', ['branch', 'reason'])
QPOS_BLOCK_TOTAL_COUNTER = _metric(Counter, 'qpos_block_total', 'Total queue position blocks', ['exchange', 'reason'])

# Robustesse
REJECT_TOTAL_COUNTER = _metric(Counter, 'reject_total', 'Total rejects', ['layer', 'reason'])
ABORT_TOTAL_COUNTER = _metric(Counter, 'abort_total', 'Total execution aborts', ['branch', 'reason'])
RETRY_TOTAL_COUNTER = _metric(Counter, 'retry_total', 'Total execution retries', ['exchange', 'code'])
ENGINE_429_TOTAL_COUNTER = _metric(Counter, 'engine_429_total', 'Total 429 received', ['exchange'])


# === OBS READINESS (Lot B) — strict + stubs + métriques Lot B ===


# === WS publics ===
WS_RECONNECTS_TOTAL = _metric(Counter, 'ws_reconnects_total', 'Reconnects WS publics', ['exchange', 'reason'])
WS_BACKOFF_SECONDS = _metric(Gauge, 'ws_backoff_seconds', 'Backoff courant (secondes) WS publics', ['exchange'])
WS_PUBLIC_DROPPED_TOTAL = _metric(Counter, 'ws_public_dropped_total', 'Evénements WS publics rejetés', ['exchange', 'reason'])
WS_PUBLIC_PING_INTERVAL_SECONDS_CONFIG = _metric(
    Gauge,
    'ws_public_ping_interval_seconds_config',
    'Configured ping interval (seconds) for public WS',
)
WS_PUBLIC_PONG_TIMEOUT_SECONDS_CONFIG = _metric(
    Gauge,
    'ws_public_pong_timeout_seconds_config',
    'Configured pong timeout (seconds) for public WS',
)
WS_PUBLIC_CONNECT_TIMEOUT_SECONDS_CONFIG = _metric(
    Gauge,
    'ws_public_connect_timeout_seconds_config',
    'Configured connect/open timeout (seconds) for public WS',
)
WS_PUBLIC_READ_TIMEOUT_SECONDS_CONFIG = _metric(
    Gauge,
    'ws_public_read_timeout_seconds_config',
    'Configured read/close timeout (seconds) for public WS',
)
WS_PUBLIC_OUT_QUEUE_PUT_TIMEOUT_SECONDS_CONFIG = _metric(
    Gauge,
    'ws_public_out_queue_put_timeout_seconds_config',
    'Configured out_queue put timeout (seconds) for public WS',
)
WS_PUBLIC_CHUNK_SIZE_CONFIG = _metric(
    Gauge,
    'ws_public_chunk_size_config',
    'Configured chunk size by exchange for public WS',
    ['exchange'],
)
# === WS publics v2 (exchange / region / deployment_mode) ===
WS_PUBLIC_EVENTS_TOTAL_V2 = _metric(
    Counter,
    'ws_public_events_total_v2',
    'Evénements WS publics reçus (v2, taggés par exchange/region/deployment_mode/stream)',
    ['exchange', 'region', 'deployment_mode', 'stream'],
)
WS_PUBLIC_ERRORS_TOTAL_V2 = _metric(
    Counter,
    'ws_public_errors_total_v2',
    'Erreurs WS publics (v2, taggées par exchange/region/deployment_mode/raison)',
    ['exchange', 'region', 'deployment_mode', 'reason'],
)

WS_PUBLIC_RECONNECTS_TOTAL_V2 = _metric(
    Counter,
    'ws_public_reconnects_total_v2',
    'Reconnects WS publics (v2, taggées par exchange/region/deployment_mode/raison)',
    ['exchange', 'region', 'deployment_mode', 'reason'],
)

WS_PUBLIC_BACKOFF_SECONDS_V2 = _metric(
    Gauge,
    'ws_public_backoff_seconds_v2',
    'Backoff courant (secondes) WS publics (v2)',
    ['exchange', 'region', 'deployment_mode'],
)

WS_PUBLIC_CONNECTIONS_OPEN_V2 = _metric(
    Gauge,
    'ws_public_connections_open_v2',
    'Connexions WS publiques ouvertes (v2)',
    ['exchange', 'region', 'deployment_mode'],
)

WS_PUBLIC_DROPPED_TOTAL_V2 = _metric(
    Counter,
    'ws_public_dropped_total_v2',
    'Evénements WS publics rejetés (v2)',
    ['exchange', 'region', 'deployment_mode', 'reason'],
)



WS_PUBLIC_STALENESS_SECONDS = _metric(
    Gauge,
    'ws_public_staleness_seconds',
    'Staleness estimée des flux WS publics (secondes, v2)',
    ['exchange', 'region', 'deployment_mode'],
)

# Log “strict” one-shot (optionnel)
_log_once = set()
def strict_warn_once(msg: str):
    if not STRICT_OBS: return
    if msg in _log_once: return
    _log_once.add(msg)
    logging.getLogger("obs_metrics").warning(msg)
# === FIN OBS READINESS ===

def inc_engine_pacing_backpressure(reason: str) -> None:
    try:
        ENGINE_PACING_BACKPRESSURE_TOTAL.labels(_norm(reason)).inc()
    except Exception:
        pass
WS_CONNECTIONS_OPEN = _metric(Gauge, 'ws_connections_open', 'Active WS public connections', ['exchange'])
PACER_STATE = _metric(Gauge, 'pacer_state', 'Pacer state (0=NORMAL,1=CONSTRAINED,2=SEVERE)')
PACER_CLAMP_SECONDS = _metric(Gauge, 'pacer_clamp_seconds', 'Seconds under clamp by kind', ['kind'])
ENGINE_MUTE_TOTAL = _metric(Counter, 'engine_mute_total', 'Mute/abandon events (by branch/pair/reason)', ['branch', 'pair', 'reason'])
FEE_TOKEN_LEVEL = _metric(Gauge, 'fee_token_level', 'Fee token level (0..1 of target)', ['cex', 'token'])
FEE_TOKEN_TARGET_PERCENT = _metric(Gauge, 'fee_token_target_percent', 'Fee token target (0..1)', ['cex', 'token'])
PWS_DEDUP_HITS_TOTAL = _metric(Counter, 'pws_dedup_hits_total', 'PrivateWS LRU dedup hits', ['exchange', 'alias'])
PWS_RECONNECTS_TOTAL = _metric(Counter, 'pws_reconnects_total', 'PrivateWS reconnects', ['exchange'])
PWS_EVENT_LAG_MS = _metric(Histogram, 'pws_event_lag_ms', 'PrivateWS event lag (ms)', ['exchange', 'alias'], buckets=BUCKETS_MS)
PWS_TRANSFERS_TOTAL = _metric(Counter, 'pws_transfers_total', 'PrivateWS internal transfers', ['exchange'])
PWS_EVENTS_TOTAL = _metric(Counter, 'pws_events_total', 'PrivateWS events received', ['exchange', 'alias', 'status', 'source'])
PWS_BALANCE_EVENTS_TOTAL = _metric(Counter, 'pws_balance_events_total', 'PrivateWS balance events received', ['exchange', 'alias', 'type'])
PWS_BACKOFF_SECONDS = _metric(Gauge, 'pws_backoff_seconds', 'PrivateWS backoff seconds', ['exchange', 'alias'])
PWS_HEARTBEAT_GAP_SECONDS = _metric(
    Gauge,
    'pws_heartbeat_gap_seconds',
    'PrivateWS heartbeat gap seconds',
    ['exchange', 'alias'],
)
PWS_HEARTBEAT_GAP_SLO_SECONDS = _metric(
    Gauge,
    'pws_heartbeat_gap_slo_seconds',
    'PrivateWS heartbeat gap SLO (seconds)',
    ['exchange', 'alias'],
)
PWS_HEARTBEAT_GAP_BREACH_TOTAL = _metric(
    Counter,
    'pws_heartbeat_gap_breach_total',
    'PrivateWS heartbeat gap SLO breaches',
    ['exchange', 'alias'],
)
PWS_HEALTH_STATE = _metric(
    Gauge,
    'pws_health_state',
    'PrivateWS health state (0=HEALTHY,1=WARN,2=CRITICAL)',
    ['exchange', 'alias'],
)
PWS_DROPPED_TOTAL = _metric(Counter, 'pws_dropped_total', 'PrivateWS dropped events', ['exchange', 'alias', 'reason'])
PWS_ACK_LATENCY_MS = _metric(Histogram, 'pws_ack_latency_ms', 'Ack latency from private WS (ms)', ['exchange'], buckets=BUCKETS_MS)
PWS_FILL_LATENCY_MS = _metric(Histogram, 'pws_fill_latency_ms', 'Fill latency from private WS (ms)', ['exchange'], buckets=BUCKETS_MS)
WS_FAILOVER_TOTAL = _metric(Counter, 'ws_failover_total', 'PrivateWS failovers', ['exchange', 'reason'])
PWS_POOL_SIZE = _metric(Gauge, 'pws_pool_size', 'Private WS connection pool size', ['exchange'])
PWS_QUEUE_DEPTH = _metric(Gauge, 'pws_queue_depth', 'PrivateWS submit queue depth', ['exchange', 'alias', 'kind'])
PWS_QUEUE_CAP = _metric(Gauge, 'pws_queue_cap', 'PrivateWS submit queue capacity', ['exchange', 'alias', 'kind'])
PWS_QUEUE_FILL_RATIO = _metric(
    Gauge,
    'pws_queue_fill_ratio',
    'PrivateWS queue fill ratio',
    ['exchange', 'alias', 'kind'],
)
PWS_QUEUE_SATURATION_TOTAL = _metric(
    Counter,
    'pws_queue_saturation_total',
    'PrivateWS queue saturation events',
    ['exchange', 'alias', 'kind'],
)
PWS_ALERT_TOTAL = _metric(
    Counter,
    'pws_alert_total',
    'PrivateWS alerts emitted',
    ['severity', 'reason', 'exchange', 'alias', 'kind'],
)
WS_RECO_RUN_MS = _metric(Histogram, 'ws_reco_run_ms', 'Private WS reconciler run duration (ms)', ['exchange'], buckets=BUCKETS_MS)
WS_RECO_ERRORS_TOTAL = _metric(Counter, 'ws_reco_errors_total', 'Errors in private WS reconciler', ['exchange'])
WS_RECO_MISS_PER_MINUTE = _metric(
    Gauge,
    'ws_reco_miss_per_minute',
    'Miss détectés par minute (fenêtre glissante ~60s)',
    ['exchange', 'alias'],
)
TRANSFER_FSM_EVENTS_TOTAL = _metric(
    Counter,
    'transfer_fsm_events_total',
    'Transfer FSM events by status and reason',
    ['status', 'reason'],
)
TRANSFER_INFLIGHT_COUNT = _metric(
    Gauge,
    'transfer_inflight_count',
    'Durable inflight transfers (in-progress states)',
)
TRANSFER_RECOVERY_RECONCILED_TOTAL = _metric(
    Counter,
    'transfer_recovery_reconciled_total',
    'Transfer recovery reconciled events',
)


def inc_transfer_fsm_event(status: str, reason: Optional[str] = None) -> None:
    try:
        TRANSFER_FSM_EVENTS_TOTAL.labels(_norm(status), _norm(reason)).inc()
    except Exception:
        pass


def set_transfer_inflight(count: int) -> None:
    try:
        TRANSFER_INFLIGHT_COUNT.set(int(count))
    except Exception:
        pass


def inc_transfer_recovery_reconciled() -> None:
    try:
        TRANSFER_RECOVERY_RECONCILED_TOTAL.inc()
    except Exception:
        pass
PWS_CALLBACK_ERRORS_TOTAL = _metric(
    Counter,
    'pws_callback_errors_total',
    'Erreurs surfaced par les callbacks PrivateWSHub',
    ['label'],
)
WS_RECO_MISS_BURST_TOTAL = _metric(
    Counter,
    'ws_reco_miss_burst_total',
    'Bursts de miss > seuil par minute',
    ['exchange', 'alias'],
)
RECONCILE_MISS_TOTAL = _metric(Counter, 'reconcile_miss_total', 'Reconciler misses', ['exchange', 'alias', 'reason'])
RECONCILE_RESYNC_TOTAL = _metric(Counter, 'reconcile_resync_total', 'Resyncs requested by reconciler', ['exchange', 'alias', 'scope'])
RECONCILE_RESYNC_FAILED_TOTAL = _metric(
    Counter,
    'reconcile_resync_failed_total',
    'Failed resync attempts triggered by reconciler',
    ['exchange', 'alias', 'scope'],
)
RECONCILE_RESYNC_LATENCY_MS = _metric(
    Histogram,
    'reconcile_resync_latency_ms',
    'Resync rebuild latency (ms)',
    ['exchange', 'alias', 'scope'],
    buckets=BUCKETS_MS,
)

RM_SC_RL_REJECT_TOTAL = _metric(Counter,
    "rm_sc_rl_reject_total",
    "Rejets RM pour dépassement du soft rate-limit SC (sub-account)",
    labelnames=("exchange", "alias", "branch"),
)

RM_SC_RL_TOKENS = _metric(Gauge,
    "rm_sc_rl_tokens",
    "Tokens restants dans le bucket soft RL par SC",
    labelnames=("exchange", "alias", "branch"),
)

COLD_RESYNC_TOTAL = _metric(Counter, 'cold_resync_total', 'Cold resyncs', ['exchange', 'result'])
COLD_RESYNC_RUN_MS = _metric(Histogram, 'cold_resync_run_ms', 'Cold resync duration (ms)', ['exchange'], buckets=BUCKETS_MS)

def recon_run_ms(dt_ms: float) -> None:
    try:
        WS_RECO_RUN_MS.observe(max(0.0, float(dt_ms)))
    except Exception:
        pass

def recon_error(exchange: str) -> None:
    try:
        WS_RECO_ERRORS_TOTAL.labels(lbl_exchange(exchange)).inc()
    except Exception:
        pass

def recon_on_resync(exchange: str, alias: str, scope: str='unknown') -> None:
    try:
        RECONCILE_RESYNC_TOTAL.labels(lbl_exchange(exchange), lbl_alias(alias), _norm(scope)).inc()
    except Exception:
        pass

def recon_observe_latency(exchange: str, alias: str, scope: str, dt_ms: float) -> None:
    try:
        RECONCILE_RESYNC_LATENCY_MS.labels(
            lbl_exchange(exchange),
            lbl_alias(alias),
            _norm(scope),
        ).observe(max(0.0, float(dt_ms)))
    except Exception:
        pass

def pws_on_failover(exchange: str, reason: str='unknown') -> None:
    try:
        WS_FAILOVER_TOTAL.labels(lbl_exchange(exchange), _norm(reason)).inc()
    except Exception:
        pass

def pws_set_pool_size(exchange: str, size: int) -> None:
    try:
        PWS_POOL_SIZE.labels(lbl_exchange(exchange)).set(max(0, int(size)))
    except Exception:
        pass
LOGGERH_WRITE_MS = _metric(Histogram, 'loggerh_write_ms', 'Write latency per JSONL batch (ms)', buckets=BUCKETS_MS if 'BUCKETS_MS' in globals() else (1, 2, 5, 8, 12, 18, 25, 35, 50, 75, 100, 150, 200, 300, 500, 800, 1600))
LOGGERH_DB_WRITE_MS = _metric(
    Histogram,
    'loggerh_db_write_ms',
    'DB write latency (ms) by operation',
    ['op'],
    buckets=BUCKETS_MS if 'BUCKETS_MS' in globals() else (1, 2, 5, 8, 12, 18, 25, 35, 50, 75, 100, 150, 200, 300, 500, 800, 1600),
)
LOGGERH_DB_LOCKED_RETRIES_TOTAL = _metric(
    Counter,
    'loggerh_db_locked_retries_total',
    'SQLite locked/busy retries',
    ['op'],
)

LHM_JSONL_INGESTED_TOTAL = _metric(Counter, 'lhm_jsonl_ingested_total', 'JSONL records ingested', ['stream'])
LHM_JSONL_DROPPED_TOTAL = _metric(Counter, 'lhm_jsonl_dropped_total', 'JSONL records dropped (non-critical/backpressure)', ['stream', 'reason'])
LHM_JSONL_QUEUE_SIZE = _metric(Gauge, 'lhm_jsonl_queue_size', 'Current queue size per JSONL stream', ['stream'])
LHM_FLUSH_BATCH_CURRENT = _metric(Gauge, 'lhm_flush_batch_current', 'Current flush batch size (records)')
LOGGERH_JSONL_ROTATIONS_TOTAL = _metric(Counter, 'loggerh_jsonl_rotations_total', 'JSONL file rotations', ['stream'])
LOGGERH_LAST_FLUSH_TS_SECONDS = _metric(Gauge, 'loggerh_last_flush_ts_seconds', 'Last JSONL flush ts (epoch seconds)')
LOGGERH_LAST_ROTATION_TS_SECONDS = _metric(Gauge, 'loggerh_last_rotation_ts_seconds', 'Last JSONL rotation ts (epoch seconds)')
LOGGERH_TRADE_QUEUE_SIZE = LHM_JSONL_QUEUE_SIZE
SCHEMA_VIOLATION_TOTAL = _metric(Counter, 'schema_violation_total', 'Schema violations encountered', ['field'])
LOG_DEDUP_TOTAL = _metric(Counter, 'log_dedup_total', 'Log deduplications', ['kind'])
LOG_REPLAY_TOTAL = _metric(Counter, 'log_replay_total', 'Log replays', ['kind'])
FORENSIC_CHAIN_HEAD = _metric(Gauge, 'forensic_chain_head', 'Rolling hash head (numeric projection)')
FORENSIC_VERIFY_OK_TOTAL = _metric(Counter, 'forensic_verify_ok_total', 'Successful forensic chain verifications')
STORAGE_USAGE_PCT = _metric(Gauge, 'storage_usage_pct', 'Filesystem usage percentage for a mount', ['mount'])
STORAGE_BYTES_FREE = _metric(Gauge, 'storage_bytes_free', 'Free bytes on the filesystem for a mount', ['mount'])
STORAGE_ALERTS_TOTAL = _metric(Counter, 'storage_alerts_total', 'Storage alerts (e.g., low_free, io_error, rotate_fail)', ['kind'])
LOGGERH_JSONL_BYTES = _metric(Gauge, 'loggerh_jsonl_bytes', 'Total size of JSONL files (bytes)')
LOGGERH_DB_STALLS_TOTAL = _metric(Counter, 'loggerh_db_stalls_total', 'DB stalls detected (write delays/backpressure)')
LOGGERH_DB_FILE_BYTES = _metric(Gauge, 'loggerh_db_file_bytes', 'LoggerHistorique DB file size (bytes)')
LOGGERH_DB_LANE_QUEUE_DEPTH = _metric(Gauge, 'loggerh_db_lane_queue_depth', 'DB lane queue depth (pending write batches)')
LOGGERH_DB_LANE_DROPS_TOTAL = _metric(Counter, 'loggerh_db_lane_drops_total', 'DB lane drops when queue is full', ['op'])
LHM_DB_LANE_QUEUE_DEPTH = _metric(Gauge, 'lhm_db_lane_queue_depth', 'DB lane queue depth (pending write batches)')
LHM_TRADE_CONTRACT_INVALID_TOTAL = _metric(
    Counter,
    'lhm_trade_contract_invalid_total',
    'Invalid trade contracts observed by LoggerHistorique',
    ['reason'],
)
LHM_JSONL_QUEUE_CAP = _metric(Gauge, 'lhm_jsonl_queue_cap', 'Configured JSONL queue capacity (records)')

LOGGERH_DB_LANE_DROPPED_TOTAL = _metric(
    Counter,
    'loggerh_db_lane_dropped_total',
    'DB lane drops (queue full/timeouts)',
    ['lane', 'reason'],
)

# --- [LHM SLO] Cibles & lag pipeline LHM (M5-B3) ------------------------------

LHM_PIPELINE_LAG_SECONDS = _metric(
    Gauge,
    'lhm_pipeline_lag_seconds',
    'Approximate end-to-end lag of LHM pipeline (seconds, event→JSONL/DB)',
)

LHM_SLO_WRITE_MS_P95_TARGET = _metric(
    Gauge,
    'lhm_slo_write_ms_p95_target',
    'Target p95 write latency (ms) for LHM JSONL pipeline',
)

LHM_SLO_QUEUE_DEPTH_MAX_TARGET = _metric(
    Gauge,
    'lhm_slo_queue_depth_max_target',
    'Target max JSONL queue depth (records) for CRIT/WARN SLO',
)

# PairHistory rotation freshness
PAIR_ROTATION_LAST_TS = _metric(
    Gauge,
    'pair_rotation_last_ts_seconds',
    'Timestamp of the last PairHistory rotation (epoch seconds)',
)
PAIR_ROTATION_AGE_SECONDS = _metric(
    Gauge,
    'pair_rotation_age_seconds',
    'Age in seconds since the last PairHistory rotation',
)

LHM_SLO_PIPELINE_LAG_MAX_SECONDS_TARGET = _metric(
    Gauge,
    'lhm_slo_pipeline_lag_max_seconds_target',
    'Target max allowed LHM pipeline lag (seconds)',
)

LHM_SLO_DROPPED_TRADES_BUDGET = _metric(
    Gauge,
    'lhm_slo_dropped_trades_budget',
    'Budget of dropped JSONL trade records per observation window',
)

LHM_CRITICAL_DROP_SEEN = _metric(
    Gauge,
    'lhm_critical_drop_seen',
    'LHM critical drop seen flag (1=yes)',
)

LHM_STORAGE_ERROR_SEEN = _metric(
    Gauge,
    'lhm_storage_error_seen',
    'LHM storage error seen flag (1=yes)',
)

TRADE_FSM_OPEN_BUNDLES = _metric(
    Gauge,
    'trade_fsm_open_bundles',
    'Open trade FSM bundles pending reconcile',
)

RECONCILE_BACKLOG = _metric(
    Gauge,
    'reconcile_backlog',
    'Reconcile backlog (open bundles/events)',
    labelnames=('scope',),
)

BUNDLE_TRUTH_DIVERGENCE_TOTAL = _metric(
    Counter,
    'bundle_truth_divergence_total',
    'Bundle truth divergence events',
    labelnames=('reason',),
)

def lhm_set_pipeline_lag(seconds: float) -> None:
    """
    Helper best-effort pour mettre à jour le lag pipeline LHM (event→JSONL/DB).

    Appelé côté LoggerHistoriqueManager quand on connaît un lag end-to-end
    approximatif (ex: now - last_db_flush_ts ou similar).
    """
    try:
        LHM_PIPELINE_LAG_SECONDS.set(max(0.0, float(seconds)))
    except Exception:
        # Observabilité best-effort, ne casse jamais le flux métier
        pass


def init_lhm_slo_targets(
    *,
    write_ms: float | None = None,
    queue_max: float | None = None,
    lag_max: float | None = None,
    dropped_budget: float | None = None,
) -> None:
    """Initialise les cibles SLO LHM depuis la config (aucun accès ENV).
    """
    try:
        if write_ms is not None:
            LHM_SLO_WRITE_MS_P95_TARGET.set(float(write_ms))
        if queue_max is not None:
            LHM_SLO_QUEUE_DEPTH_MAX_TARGET.set(float(queue_max))
        if lag_max is not None:
            LHM_SLO_PIPELINE_LAG_MAX_SECONDS_TARGET.set(float(lag_max))
        if dropped_budget is not None:
            LHM_SLO_DROPPED_TRADES_BUDGET.set(float(dropped_budget))
    except Exception:
        # Best-effort
        pass



def loggerh_observe_write_ms(dt_ms: float) -> None:
    try:
        LOGGERH_WRITE_MS.observe(max(0.0, float(dt_ms)))
    except Exception:
        pass

def lhm_on_ingested(stream: str, n: int=1) -> None:
    try:
        LHM_JSONL_INGESTED_TOTAL.labels(str(stream)).inc(max(1, int(n)))
    except Exception:
        pass

def lhm_on_dropped(stream: str, reason: str='noncritical', n: int=1) -> None:
    try:
        LHM_JSONL_DROPPED_TOTAL.labels(str(stream), str(reason)).inc(max(1, int(n)))
    except Exception:
        pass

def lhm_set_queue_size(stream: str, size: int) -> None:
    try:
        LHM_JSONL_QUEUE_SIZE.labels(str(stream)).set(max(0, int(size)))
    except Exception:
        pass

def lhm_on_rotation(stream: str) -> None:
    try:
        LOGGERH_JSONL_ROTATIONS_TOTAL.labels(str(stream)).inc()
    except Exception:
        pass

def loggerh_set_last_flush_now() -> None:
    try:
        LOGGERH_LAST_FLUSH_TS_SECONDS.set(time.time())
    except Exception:
        pass

def loggerh_set_last_rotation_now() -> None:
    try:
        LOGGERH_LAST_ROTATION_TS_SECONDS.set(time.time())
    except Exception:
        pass

def set_lhm_pipeline_flags(*, critical_drop_seen: bool, storage_error_seen: bool) -> None:
    try:
        LHM_CRITICAL_DROP_SEEN.set(1.0 if critical_drop_seen else 0.0)
        LHM_STORAGE_ERROR_SEEN.set(1.0 if storage_error_seen else 0.0)
    except Exception:
        pass

def set_trade_fsm_open_bundles(count: int) -> None:
    try:
        TRADE_FSM_OPEN_BUNDLES.set(max(0, int(count)))
    except Exception:
        pass

def set_reconcile_backlog(scope: str, count: int) -> None:
    try:
        RECONCILE_BACKLOG.labels(str(scope)).set(max(0, int(count)))
    except Exception:
        pass

def note_bundle_truth_divergence(reason: str, n: int = 1) -> None:
    try:
        BUNDLE_TRUTH_DIVERGENCE_TOTAL.labels(str(reason)).inc(max(1, int(n)))
    except Exception:
        pass

def set_forensic_head_numeric(head_hex: str) -> None:
    """Convertit un hash hex en entier (projection) pour FORENSIC_CHAIN_HEAD."""
    try:
        val = int(head_hex[:16], 16)
        FORENSIC_CHAIN_HEAD.set(val)
    except Exception:
        pass

def update_storage_metrics(mount: str) -> None:
    """Observe l'état d'un filesystem (portable Linux/macOS/Windows)."""
    try:
        import os
        import shutil
        path = mount
        if not os.path.isdir(path):
            path = os.path.dirname(path) or '.'
        if hasattr(os, 'statvfs'):
            st = os.statvfs(path)
            total = float(st.f_frsize) * float(st.f_blocks)
            free = float(st.f_frsize) * float(st.f_bavail)
        else:
            total, used, free = shutil.disk_usage(path)
        pct = 0.0 if total <= 0 else 100.0 * (1.0 - free / total)
        STORAGE_BYTES_FREE.labels(path).set(free)
        STORAGE_USAGE_PCT.labels(path).set(pct)
    except Exception:
        pass
LOGGERH_QUEUE_PLATEAU_TOTAL = _metric(Counter, 'loggerh_queue_plateau_total', 'Queue plateau detected', ['stream'])

def loggerh_on_queue_plateau(stream: str) -> None:
    try:
        LOGGERH_QUEUE_PLATEAU_TOTAL.labels(str(stream)).inc()
    except Exception:
        pass
loggerh_write_ms = LOGGERH_WRITE_MS
lhm_jsonl_ingested_total = LHM_JSONL_INGESTED_TOTAL
lhm_jsonl_dropped_total = LHM_JSONL_DROPPED_TOTAL
lhm_jsonl_queue_size = LHM_JSONL_QUEUE_SIZE
lhm_jsonl_queue_cap = LHM_JSONL_QUEUE_CAP
lhm_flush_batch_current = LHM_FLUSH_BATCH_CURRENT
schema_violation_total = SCHEMA_VIOLATION_TOTAL
log_dedup_total = LOG_DEDUP_TOTAL
log_replay_total = LOG_REPLAY_TOTAL
forensic_chain_head = FORENSIC_CHAIN_HEAD
forensic_verify_ok_total = FORENSIC_VERIFY_OK_TOTAL
loggerh_queue_plateau_total = LOGGERH_QUEUE_PLATEAU_TOTAL

def mark_ingested(stream: str, n: int=1) -> None:
    lhm_on_ingested(stream, n)

def mark_drop(stream: str, reason: str, n: int=1) -> None:
    lhm_on_dropped(stream, reason, n)
def mark_schema_violation(field: str) -> None:
    try:
        SCHEMA_VIOLATION_TOTAL.labels(field=str(field)).inc()
    except Exception:
        pass

def set_flush_batch(n: int) -> None:
    try:
        LHM_FLUSH_BATCH_CURRENT.set(int(n))
    except Exception:
        pass

def set_lhm_queue(size: int, cap: int | None=None) -> None:
    """Compat: vue agrégée sur '_all_' + capacité globale optionnelle."""
    try:
        LHM_JSONL_QUEUE_SIZE.labels('_all_').set(int(size))
        if cap is not None:
            LHM_JSONL_QUEUE_CAP.set(int(cap))
    except Exception:
        pass
VOL_PRICE_VOL_MICRO = _metric(Gauge, 'vol_price_vol_micro', 'Micro price volatility', ['pair'])
VOL_SPREAD_VOL_MICRO = _metric(Gauge, 'vol_spread_vol_micro', 'Micro spread volatility', ['pair'])
VOL_PRICE_PCTL = _metric(Gauge, 'vol_price_pctl', 'Price percentile', ['pair', 'pctl'])
VOL_SPREAD_PCTL = _metric(Gauge, 'vol_spread_pctl', 'Spread percentile', ['pair', 'pctl'])
VOL_ANOMALY_TOTAL = _metric(Counter, 'vol_anomaly_total', 'Volatility anomalies', ['pair', 'kind'])
VOL_SIGNAL_STATE = _metric(Gauge, 'vol_signal_state', 'Volatility signal state', ['pair'])
SIM_DECISION_MS = _metric(Histogram, 'sim_decision_ms', 'Simulator decision ms', buckets=BUCKETS_MS)
SIMULATED_VWAP_DEVIATION_BPS = _metric(Histogram, 'simulated_vwap_deviation_bps', 'Simulated VWAP deviation (bps)', buckets=(0.1, 0.2, 0.5, 1, 2, 3, 5, 8, 13, 21))

# Age des snapshots de volatilité (VM) par paire
VOL_AGE_SECONDS = _metric(
    Gauge,
    "vol_age_seconds",
    "Âge du dernier snapshot de volatilité (secondes)",
    ["pair"],
)

# Age des points de slippage par pair/exchange/side
SLIP_AGE_SECONDS = _metric(
    Gauge,
    "slip_age_seconds",
    "Âge du dernier point de slippage observé (secondes)",
    ["pair", "exchange", "side"],
)


def set_vol_age_seconds(pair: str, age_seconds: float) -> None:
    """
    Met à jour l'âge (en secondes) du dernier snapshot de volatilité pour une paire.
    """
    try:
        VOL_AGE_SECONDS.labels(_norm(pair)).set(max(0.0, float(age_seconds)))
    except Exception:
        # Observabilité best-effort
        pass


def set_slip_age_seconds(
    pair: str,
    exchange: str,
    side: str,
    age_seconds: float,
) -> None:
    """
    Met à jour l'âge (en secondes) du dernier point de slippage pour pair/exchange/side.
    """
    try:
        SLIP_AGE_SECONDS.labels(
            _norm(pair),
            lbl_exchange(exchange),
            _norm(side),
        ).set(max(0.0, float(age_seconds)))
    except Exception:
        # Observabilité best-effort
        pass


def sim_on_run(mode: str, vwap_dev: Optional[float]=None, fragments: int=0, blocked: bool=False) -> None:
    try:
        if vwap_dev is not None:
            SIMULATED_VWAP_DEVIATION_BPS.observe(abs(float(vwap_dev)))
        if blocked:
            inc_blocked('simulator', f'mode:{_norm(mode)}', None)
    except Exception:
        pass
PAYLOAD_REJECTED_TOTAL = _metric(Counter, 'payload_rejected_total', 'Payloads rejected by validation', ['field'])
_metrics_http_started = False


class ObsServer:
    """HTTP helper exposing /metrics plus optional JSON endpoints."""

    def __init__(self, *, host: str = '0.0.0.0', port: int = 9108) -> None:
        self.host = host
        self.port = int(port)
        self._runner: Optional['web.AppRunner'] = None
        self._site: Optional['web.TCPSite'] = None
        self._modules: Dict[str, Any] = {}
        self._started_prometheus_only = False

    def register_modules(self, **modules: Any) -> None:
        self._modules.update({k: v for k, v in modules.items() if v is not None})

    async def start(self) -> None:
        global _metrics_http_started
        if web is None:
            if self._started_prometheus_only or _metrics_http_started:
                return
            start_http_server(self.port)
            self._started_prometheus_only = True
            _metrics_http_started = True
            _obs_shim_log.info('ObsServer: prometheus_client /metrics on :%d (aiohttp absent)', self.port)
            return

        if self._runner or self._site:
            return
        if _metrics_http_started:
            _obs_shim_log.info('ObsServer already started globally; skipping second instance')
            return

        app = web.Application()

        @web.middleware
        async def _cors_mw(request, handler):
            resp = await handler(request)
            resp.headers['Access-Control-Allow-Origin'] = '*'
            return resp

        app.middlewares.append(_cors_mw)
        app.add_routes([
            web.get('/health', self._health),
            web.get('/metrics', self._metrics),
            web.get('/api/engine/status', self._engine_status),
            web.get('/engine/status', self._engine_status),
            web.get('/api/risk/snapshot', self._risk_snapshot),
            web.get('/risk/snapshot', self._risk_snapshot),
            web.get('/api/scanner/snapshot', self._scanner_snapshot),
            web.get('/scanner/snapshot', self._scanner_snapshot),
            web.get('/api/router/health', self._router_health),
            web.get('/router/health', self._router_health),
            web.get('/central/last_event', self._central_last_event),
            web.get('/central/status', self._central_status),
        ])
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()
        self._started_prometheus_only = True
        _metrics_http_started = True
        _obs_shim_log.info('ObsServer started on %s:%d (aiohttp)', self.host, self.port)

    async def stop(self) -> None:
        global _metrics_http_started
        if self._site:
            await self._site.stop()
        if self._runner:
            await self._runner.cleanup()
        self._site = None
        self._runner = None
        if self._started_prometheus_only:
            _metrics_http_started = False
            self._started_prometheus_only = False

    async def _health(self, request):
        statuses: Dict[str, Any] = {}
        healthy = True
        for name, mod in self._modules.items():
            try:
                st = mod.get_status() if hasattr(mod, 'get_status') else {'healthy': True}
            except Exception:
                st = {'healthy': False, 'details': 'get_status failed'}
            statuses[name] = st
            healthy = healthy and bool(st.get('healthy', True))
        return web.json_response({'healthy': healthy, 'modules': statuses})

    async def _metrics(self, request):
        data = generate_latest(REGISTRY)
        return web.Response(body=data, content_type=CONTENT_TYPE_LATEST)

    async def _engine_status(self, request):
        eng = self._modules.get('engine')
        out: Dict[str, Any] = {}
        if eng:
            snap = None
            for name in ('export_status', 'snapshot', 'status', 'to_dict', 'export'):
                if hasattr(eng, name):
                    res = getattr(eng, name)()
                    if asyncio.iscoroutine(res):
                        res = await res
                    snap = res
                    break
            if isinstance(snap, dict):
                out = snap
            else:
                out = {
                    'status': getattr(eng, 'state', 'running'),
                    'latency_ms': getattr(eng, 'latency_ms', {'ack_p50': None, 'fill_p50': None}),
                }
        return web.json_response(out or {'status': 'unknown'})

    async def _risk_snapshot(self, request):
        rm = self._modules.get('risk')
        out: Dict[str, Any] = {}
        if rm:
            snap = None
            for name in ('export_snapshot', 'snapshot', 'to_dict', 'export'):
                if hasattr(rm, name):
                    res = getattr(rm, name)()
                    if asyncio.iscoroutine(res):
                        res = await res
                    snap = res
                    break
            if isinstance(snap, dict):
                out = snap
            else:
                fees = None
                for n in ('export_fees_snapshot', 'get_all_fees'):
                    if hasattr(rm, n):
                        fees = getattr(rm, n)()
                        break
                prud = 'unknown'
                vm = getattr(rm, 'volatility_monitor', None)
                if vm is not None:
                    prud = getattr(vm, 'prudence', None) or getattr(vm, 'get_prudence_signal', lambda *_: 'unknown')('ALL')
                out = {
                    'fees': fees or {},
                    'slippage': getattr(rm, '_ext_slip', {}),
                    'volatility_bps': getattr(rm, '_ext_vol_bps', {}),
                    'prudence': prud,
                }
        return web.json_response(out or {})

    async def _scanner_snapshot(self, request):
        sc = self._modules.get('scanner')
        out: Dict[str, Any] = {}
        if sc:
            snap = None
            for name in ('export_snapshot', 'snapshot', 'to_dict', 'export'):
                if hasattr(sc, name):
                    res = getattr(sc, name)()
                    if asyncio.iscoroutine(res):
                        res = await res
                    snap = res
                    break
            if isinstance(snap, dict):
                out = snap
            else:
                opps = []
                for name in ('get_recent_opportunities', 'recent_opportunities', 'opportunities'):
                    if hasattr(sc, name):
                        data = getattr(sc, name)
                        data = data() if callable(data) else data
                        if isinstance(data, list):
                            opps = data
                            break
                out = {'opportunities': opps, 'summary': {'count': len(opps)}}
        return web.json_response(out or {})

    async def _router_health(self, request):
        try:
            return web.json_response({'exchanges': _ROUTER_HEALTH_CACHE})
        except Exception:
            return web.json_response({'error': 'failed to read router health'}, status=500)

    async def _central_last_event(self, request):
        cw = self._modules.get('central_watchdog')
        if not cw or not hasattr(cw, 'get_status'):
            return web.json_response({'error': 'central_watchdog not registered'}, status=404)
        try:
            st = cw.get_status() or {}
            return web.json_response({'last_event': st.get('last_event')})
        except Exception:
            return web.json_response({'error': 'failed to read central last event'}, status=500)

    async def _central_status(self, request):
        cw = self._modules.get('central_watchdog')
        if not cw or not hasattr(cw, 'get_status'):
            return web.json_response({'error': 'central_watchdog not registered'}, status=404)
        try:
            return web.json_response(cw.get_status())
        except Exception:
            return web.json_response({'error': 'failed to read central status'}, status=500)

try:
    from aiohttp import web
except Exception:
    web = None

def _bool_event(e: Any) -> Optional[bool]:
    """True si asyncio.Event() est set, False si présent mais non set, None sinon."""
    try:
        return bool(getattr(e, 'is_set', lambda: None)())
    except Exception:
        return None

def _extract_status_from_boot(boot: Any) -> Dict[str, Any]:
    """Lit l'état depuis l'instance Boot (souple: champs optionnels).
    Préfère boot.get_status() si disponible; sinon dérive depuis .state et .ready_*.
    """
    now = time.time()
    if boot is None:
        return {'ts': now, 'ready_all': False, 'degraded': True, 'stage': 'unknown', 'reasons': ['boot=None']}
    try:
        if hasattr(boot, 'get_status'):
            st = boot.get_status()
            if isinstance(st, dict):
                return {'ts': st.get('ts', now), 'ready_all': bool(st.get('ready_all', st.get('ready', False))), 'degraded': bool(st.get('degraded', False)), 'stage': st.get('stage', 'unknown'), 'reasons': list(st.get('reasons', [])), **st}
    except Exception:
        log.exception('get_status() a levé une exception; fallback champs bruts')
    stage = getattr(getattr(boot, 'state', None), 'get', lambda *_: 'unknown')('stage')
    degraded = bool(getattr(getattr(boot, 'state', None), 'get', lambda *_: False)('degraded'))
    reasons = getattr(getattr(boot, 'state', None), 'get', lambda *_: [])('reasons') or []
    ready_all = _bool_event(getattr(boot, 'ready_all', None))
    if ready_all is None:
        flags = []
        for name in ('ready_ws', 'ready_router', 'ready_scanner', 'ready_rm', 'ready_engine', 'ready_private', 'ready_balances', 'ready_rpc'):
            v = _bool_event(getattr(boot, name, None))
            if v is not None:
                flags.append(v)
        ready_all = all(flags) if flags else False
    return {'ts': now, 'ready_all': bool(ready_all), 'degraded': degraded, 'stage': stage or 'unknown', 'reasons': reasons if isinstance(reasons, list) else [str(reasons)]}

class StatusHTTPServer:
    """Serveur HTTP minimaliste (aiohttp):
      • GET /ready  → 200 si ready_all & non dégradé, sinon 503 avec raisons
      • GET /status → 200 (JSON d’état complet)
      • GET /metrics → exposition Prometheus (facultatif)

    Par défaut, include_metrics=False pour éviter la double écoute avec ObsServer.
    """

    def __init__(self, host: str='0.0.0.0', port: int=9110, registry=None, include_metrics: bool=True):
        self.host = host
        self.port = int(port)
        self.registry = registry or REGISTRY
        self.include_metrics = bool(include_metrics)
        self._boot: Any = None
        self._runner: Optional['web.AppRunner'] = None
        self._site: Optional['web.TCPSite'] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None

    def set_boot(self, boot: Any) -> None:
        self._boot = boot

    async def _handle_status(self, request):
        st = _extract_status_from_boot(self._boot)
        return web.json_response(st, status=200)

    async def _handle_ready(self, request):
        st = _extract_status_from_boot(self._boot)

        # Rollout: cfg.g.ready_strict si présent.
        # Fallback safe: auto-strict en PROD si cfg.g.mode == "PROD", sinon legacy.
        strict = False
        try:
            cfg = getattr(self._boot, "cfg", None)
            g = getattr(cfg, "g", None)
            if g is not None and hasattr(g, "ready_strict"):
                strict = bool(getattr(g, "ready_strict"))
            elif g is not None and str(getattr(g, "mode", "DRY_RUN")).upper() == "PROD":
                strict = True
        except Exception:
            strict = False

        st["ready_strict"] = bool(strict)

        if strict:
            # TRADING_READY strict: fail-safe (si champs manquent => 503)
            ok = False
            fail_reasons = []

            # 1) Prefer explicit trading_ready if boot exposes it
            if "trading_ready" in st:
                ok = bool(st.get("trading_ready"))
                if not ok:
                    fail_reasons.append("trading_ready=false")
            else:
                eng_ok = bool(st.get("engine_ready"))
                rm_ok = bool(st.get("rm_trading_ready"))
                tstate = str(st.get("trading_state", "READY")).upper()

                if not eng_ok:
                    fail_reasons.append("engine_not_ready")
                if not rm_ok:
                    fail_reasons.append("rm_not_trading_ready")
                if tstate != "READY":
                    fail_reasons.append(f"trading_state={tstate}")

                ok = bool(eng_ok and rm_ok and tstate == "READY")

            # 2) Safety: if boot is degraded, trading is not ready
            if bool(st.get("degraded")):
                reasons = st.get("reasons") or []
                if isinstance(reasons, list) and reasons:
                    metrics_only = all(str(r) == "OBS_METRICS_UNAVAILABLE" for r in reasons)
                else:
                    metrics_only = False
                if not metrics_only:
                    ok = False
                    fail_reasons.append("boot_degraded")
                else:
                    st["boot_degraded_metrics_only"] = True

            # 3) Micro-durcissement: private_ws wiring check ONLY if private_ws feature is enabled
            private_ws_enabled = False
            try:
                cfg = getattr(self._boot, "cfg", None)
                g = getattr(cfg, "g", None)
                fs = getattr(g, "feature_switches", None) if g is not None else None
                if isinstance(fs, dict):
                    private_ws_enabled = bool(fs.get("private_ws", False))
            except Exception:
                private_ws_enabled = False

            st["private_ws_enabled"] = bool(private_ws_enabled)

            if private_ws_enabled:
                pws = st.get("private_ws") or {}
                rm_pws = (pws.get("rm") or {}) if isinstance(pws, dict) else {}
                eng_pws = (pws.get("engine") or {}) if isinstance(pws, dict) else {}

                # Fail-safe si status absent
                if not isinstance(pws, dict) or (not rm_pws and not eng_pws):
                    ok = False
                    fail_reasons.append("pws_status_missing")
                else:
                    rm_hub_ok = bool(rm_pws.get("hub_wiring_ok", False))
                    rm_rec_ok = bool(rm_pws.get("reconciler_wiring_ok", False))
                    eng_hub_ok = bool(eng_pws.get("hub_wiring_ok", False))

                    st["ready_private_ws_wiring_ok"] = bool(rm_hub_ok and rm_rec_ok and eng_hub_ok)

                    if not rm_hub_ok:
                        ok = False
                        fail_reasons.append("pws_rm_hub_wiring_not_ok")
                    if not rm_rec_ok:
                        ok = False
                        fail_reasons.append("pws_rm_reconciler_wiring_not_ok")
                    if not eng_hub_ok:
                        ok = False
                        fail_reasons.append("pws_engine_hub_wiring_not_ok")

            st["ready_semantics"] = "TRADING_READY"
            st["ready_trading"] = bool(ok)
            if not ok and fail_reasons:
                # Reasons endpoint-only (ne pas confondre avec reason codes canoniques trading)
                st["ready_fail_reasons"] = fail_reasons

            return web.json_response(st, status=(200 if ok else 503))

        # Legacy service-ready behavior
        ok = bool(st.get("ready_all")) and (not bool(st.get("degraded")))
        st["ready_semantics"] = "SERVICE_READY"
        st["ready_service"] = bool(ok)
        return web.json_response(st, status=(200 if ok else 503))

    async def _handle_healthz(self, request):
        # /healthz conserve l'ancien critère: service-ready (boot ready_all et non dégradé)
        st = _extract_status_from_boot(self._boot)
        ok = bool(st.get("ready_all")) and (not bool(st.get("degraded")))
        st["ready_semantics"] = "SERVICE_READY"
        st["ready_service"] = bool(ok)
        return web.json_response(st, status=(200 if ok else 503))

    async def _handle_metrics(self, request):
        if not self.include_metrics or self.registry is None:
            return web.Response(text='# metrics disabled on this endpoint\n', status=200)
        try:
            output = generate_latest(self.registry)
            if not output or output.strip() == b"":
                log.warning("/metrics called but REGISTRY seems empty or generate_latest returned nothing")
            return web.Response(body=output, headers={'Content-Type': CONTENT_TYPE_LATEST})
        except Exception:
            log.exception('generate_latest() a échoué')
            return web.Response(text='# error generating metrics\n', status=500)

    async def start(self) -> None:
        if web is None:
            log.warning('aiohttp non installé — /ready|/status|/metrics (optionnel) désactivés')
            return
        if self._runner:
            return
        app = web.Application()
        app.add_routes([
            web.get('/ready', self._handle_ready),
            web.get('/status', self._handle_status),
            web.get('/healthz', self._handle_healthz),
            web.get('/metrics', self._handle_metrics)
        ])
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, host=self.host, port=self.port)
        await self._site.start()
        log.info('StatusHTTPServer started on http://%s:%d', self.host, self.port)

    def start_in_thread(self) -> None:
        """Démarre le serveur dans un thread dédié pour ne pas dépendre de la loop principale."""
        if self._thread is not None:
            return

        def _run_loop():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            try:
                self._loop.run_until_complete(self.start())
                log.info("status_http_loop_running=1")
                self._loop.run_forever()
            except Exception:
                log.exception("StatusHTTPServer thread died")
            finally:
                self._loop.close()

        self._thread = threading.Thread(target=_run_loop, name="StatusHTTPThread", daemon=True)
        self._thread.start()

    async def stop(self) -> None:
        if self._loop and self._loop.is_running():
            # Arrêt propre si on est en mode thread
            fut = asyncio.run_coroutine_threadsafe(self._stop_internal(), self._loop)
            try:
                fut.result(timeout=5)
            except Exception:
                log.warning("StatusHTTPServer stop timed out")
            self._loop.call_soon_threadsafe(self._loop.stop)
        else:
            await self._stop_internal()

    async def _stop_internal(self) -> None:
        try:
            if self._site:
                await self._site.stop()
            if self._runner:
                await self._runner.cleanup()
        finally:
            self._site = None
            self._runner = None
            log.info('StatusHTTPServer stopped')
BOT_STARTUPS_TOTAL = _metric(Counter, 'bot_startups_total', 'Number of bot startups')
BOT_STATE = _metric(Gauge, 'bot_state', 'Bot state (0=STOPPED,1=STARTING,2=RUNNING,3=STOPPING)')

class MainMetrics:
    """Back-compat shim. Préférence: utiliser directement les métriques exposées ci-dessus."""

    def __init__(self):
        self.bot_startups_total = BOT_STARTUPS_TOTAL
        self.bot_state = BOT_STATE

    def inc(self, *_, **__):
        pass

    def set(self, *_, **__):
        pass

    def observe(self, *_, **__):
        pass

    def register(self, boot=None) -> None:
        """Ajout superset: incrémente startups + publie bot_state depuis boot (si dispo)."""
        try:
            BOT_STARTUPS_TOTAL.inc()
        except Exception:
            pass
        try:
            if boot is not None and hasattr(boot, 'get_status'):
                st = boot.get_status() or {}
                stage = str(st.get('stage', 'RUNNING')).upper()
            else:
                stage = 'RUNNING'
            BOT_STATE.set({'STOPPED': 0, 'STARTING': 1, 'RUNNING': 2, 'STOPPING': 3}.get(stage, 2))
        except Exception:
            pass

def _init_default_labels() -> None:
    """Force l'initialisation des métriques critiques avec des labels par défaut pour exposition immédiate."""
    if not _PROM_READY:
        return
    try:
        # 1) Modes RiskManager (Gauges 0/1)
        # On synchronise avec les labels réels utilisés dans rm_update_mode_gauges
        for m in ("NORMAL", "OPP_VOLUME", "OPP_VOL", "CAREFUL", "ALERTE", "SHUTDOWN"):
            safe_set(RM_MODE_CURRENT, "rm_mode_current", "boot.init", 0.0, mode=m)
        safe_set(RM_MODE_CURRENT, "rm_mode_current", "boot.init", 1.0, mode="NORMAL")

        for m in ("NORMAL", "CONSTRAINED", "SEVERE", "QUICK", "HEDGE_ONLY", "SANDBOX", "OPPORTUNISTE"):
            safe_set(RM_TRADE_MODE_CURRENT, "rm_trade_mode_current", "boot.init", 0.0, mode=m)
        safe_set(RM_TRADE_MODE_CURRENT, "rm_trade_mode_current", "boot.init", 1.0, mode="NORMAL")

        # 2) Compteurs de transitions (Entries/Exits)
        for m in ("NORMAL", "OPP_VOLUME", "OPP_VOL", "CAREFUL", "ALERTE", "SHUTDOWN"):
            for r in ("startup", "volatility", "slippage", "manual", "auto_recovery"):
                safe_inc(RM_MODE_ENTRIES_TOTAL, "rm_mode_entries_total", "boot.init", mode=m, reason=r, value=0)
                safe_inc(RM_MODE_EXITS_TOTAL, "rm_mode_exits_total", "boot.init", mode=m, reason=r, value=0)

        # 2b) État de trading RM
        safe_set(RM_TRADING_READY, "rm_trading_ready", "rm", 0.0)
        for dep in ("engine", "books", "balances", "scanner"):
            safe_set(RM_DEP_READY, "rm_dep_ready", "rm", 0.0, dep=dep)

        # 2c) Métriques de démarrage
        safe_inc(BOT_STARTUPS_TOTAL, "bot_startups_total", "boot.init", value=0)
        safe_set(BOT_STATE, "bot_state", "boot.init", 1.0) # STARTING

        # 3) Volatilité & Staleness (Paires seed)
        for p in ("BTCUSDC", "ETHUSDC"):
            safe_set(VOL_P95_BPS, "vol_p95_bps", "boot.init", 0.0, pair=p)
            for ex in ("BINANCE", "BYBIT", "COINBASE"):
                for side in ("buy", "sell"):
                    safe_set(SLIP_AGE_SECONDS, "slip_age_seconds", "boot.init", 0.0, pair=p, exchange=ex, side=side)

        for ex in ("BINANCE", "BYBIT", "COINBASE"):
            safe_set(WS_PUBLIC_STALENESS_SECONDS, "ws_public_staleness_seconds", "boot.init", 0.0, deployment_mode="EU_ONLY", exchange=ex, region="EU")
            safe_set(WS_PUBLIC_STALENESS_SECONDS, "ws_public_staleness_seconds", "boot.init", 0.0, deployment_mode="SPLIT", exchange=ex, region="EU")

        # 4) Latences (Histogrammes -> initialisent les _count/_sum à 0)
        safe_observe(ROUTER_TO_SCANNER_MS, "router_to_scanner_ms", "boot.init", 0.0, route="tri_cex")
        safe_observe(SIM_DECISION_MS, "sim_decision_ms", "boot.init", 0.0)
        safe_observe(RM_DECISION_MS, "rm_decision_ms", "boot.init", 0.0, cohort="PRIMARY")

        log.info("Métriques critiques initialisées avec labels par défaut (enrichi)")
    except Exception as e:
        log.warning("Échec initialisation metrics par défaut: %s", e)


def start_servers(boot=None, cfg=None) -> dict[str, object]:
    """
    Démarre StatusHTTPServer avec /metrics activable par configuration,
    et ObsServer optionnel. Retourne {"status_server": .., "obs_server": ..}.
    """
    _init_default_labels()
    servers: dict[str, object] = {}
    obs_cfg = getattr(cfg, "obs", None)
    defaults = dict(_OBS_DEFAULTS)
    status_port = int(getattr(obs_cfg, "status_port", defaults["status_port"]))
    expose_9110 = bool(getattr(obs_cfg, "expose_metrics_on_status", defaults["expose_metrics_on_status"]))
    enable_obs_port = bool(getattr(obs_cfg, "enable_obs_port", defaults["enable_obs_port"]))
    obs_port = int(getattr(obs_cfg, "obs_port", defaults["obs_port"]))
    try:

        st = StatusHTTPServer(port=status_port, include_metrics=expose_9110)
        if boot is not None:
            st.set_boot(boot)
        servers['status_server'] = st
    except Exception:
        log.exception('start_servers: StatusHTTPServer init a échoué')
    try:
        if enable_obs_port:
            servers['obs_server'] = ObsServer(port=obs_port)
    except Exception:
        log.exception('start_servers: ObsServer init a échoué')
    return servers

# Compatibilité: signature historique
def start_servers_from_env(boot=None) -> dict[str, object]:
    return start_servers(boot=boot)
# --- [LHM/LAT] Métriques latence & pipeline centralisées ----------------------

# Histos latence (submit→ack / submit→first_fill / submit→all_filled / e2e)
LAT_ACK_MS = get_histogram(
    "latency_ack_ms",
    "Submit→Ack latency (ms) observed from latency pipeline",
    buckets=BUCKETS_MS,
)
LAT_FILL_FIRST_MS = get_histogram(
    "latency_first_fill_ms",
    "Submit→First fill latency (ms)",
    buckets=BUCKETS_MS,
)
LAT_FILL_ALL_MS = get_histogram(
    "latency_all_filled_ms",
    "Submit→All filled latency (ms)",
    buckets=BUCKETS_MS,
)
LAT_E2E_MS = get_histogram(
    "latency_e2e_ms",
    "End-to-end latency (ms) observed from latency pipeline",
    buckets=BUCKETS_MS,
)

# Compteurs d'événements de latence
# 1) Par route (utilisé lors de l'insert DB latence)
LAT_EVENTS_TOTAL = get_counter(
    "latency_events_total",
    "Number of latency events ingested (by route)",
    labelnames=("route", "buy_ex", "sell_ex", "status"),
)

# 2) Par étape pipeline (utilisé lors des increments 'stage/status')
LAT_PIPELINE_EVENTS_TOTAL = get_counter(
    "latency_pipeline_events_total",
    "Number of latency events by pipeline stage",
    labelnames=("stage", "status"),
)

# PairHistory (piloté par LHM)
PAIR_HISTORY_ROWS_TOTAL = get_counter(
    "pair_history_rows_total",
    "Rows written to pair_history table",
)
PAIR_HISTORY_COMPUTE_MS = get_histogram(
    "pair_history_compute_ms",
    "Compute+write time for pair history (ms)",
    buckets=BUCKETS_MS,
)
LOGGERH_FILE_ROTATIONS_TOTAL = get_counter(
    "loggerh_file_rotations_total",
    "Nombre de rotations de fichiers effectuées par le LoggerHistorique",
    labelnames=("kind", "reason"),
)

OBS_READY = get_gauge(
    "obs_ready",
    "1 si l'observabilité Prometheus est pleinement disponible, sinon 0",
)

try:
    # Si prometheus_client est importable, on considère l'obs "prête"
    import prometheus_client  # type: ignore
    _OBS_PROM_AVAILABLE = True
except Exception:
    _OBS_PROM_AVAILABLE = False

def obs_is_ready() -> bool:
    """Indique si l'empilement Prometheus est opérationnel (client importable)."""
    return bool(_OBS_PROM_AVAILABLE)

try:
    OBS_READY.set(1.0 if _OBS_PROM_AVAILABLE else 0.0)
except Exception:
    pass

# --- Legacy observability helpers (migrated from observability.py) ---
try:
    from aiohttp import web  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    web = None  # type: ignore

_ROUTER_HEALTH_CACHE: Dict[str, Dict[str, Any]] = {}

def router_health_on_beat(
    exchange: str,
    *,
    last_ex_ts_ms: int,
    last_recv_ts_ms: int,
    age_ms: float,
    pairs_seen: int | List[str] | None = None,
) -> None:
    """Backfills the /api/router/health cache used by the HTTP shim."""

    try:
        exu = str(exchange).upper()
        n_pairs = len(pairs_seen) if isinstance(pairs_seen, (list, tuple)) else int(pairs_seen or 0)
        _ROUTER_HEALTH_CACHE[exu] = {
            'exchange': exu,
            'last_ex_ts_ms': int(last_ex_ts_ms),
            'last_recv_ts_ms': int(last_recv_ts_ms),
            'age_ms': float(age_ms),
            'pairs_seen': n_pairs,
            'updated_ts': time.time(),
        }
    except Exception:  # pragma: no cover - defensive
        _obs_shim_log.exception('router_health_on_beat failed')


# --- Helpers high-level WS publics (v2) ---

def ws_public_event(
    exchange: str,
    *,
    region: str,
    deployment_mode: str,
    stream: str = "generic",
) -> None:
    """Incrémente le compteur d'événements WS publics (v2).

    Labels:
      - exchange: nom de la plateforme (BINANCE, BYBIT, COINBASE…)
      - region: région logique (EU, US, EU_CB…)
      - deployment_mode: EU_ONLY, SPLIT, …
      - stream: type de flux (l2, trades, ticker, all…)
    """
    try:
        safe_inc(
            WS_PUBLIC_EVENTS_TOTAL_V2,
            "ws_public_events_total_v2",
            "ws_public_event",
            exchange=(exchange or "UNKNOWN").upper(),
            region=region or "UNKNOWN",
            deployment_mode=deployment_mode or "UNKNOWN",
            stream=stream or "generic",
        )
    except Exception:  # pragma: no cover - observabilité optionnelle
        _obs_shim_log.exception("ws_public_event failed")


def ws_public_reconnect(
    exchange: str,
    *,
    region: str,
    deployment_mode: str,
    reason: str = "unknown",
) -> None:
    """Incrémente le compteur de reconnexions WS publics (v2)."""
    try:
        safe_inc(
            WS_PUBLIC_RECONNECTS_TOTAL_V2,
            "ws_public_reconnects_total_v2",
            "ws_public_reconnect",
            exchange=(exchange or "UNKNOWN").upper(),
            region=region or "UNKNOWN",
            deployment_mode=deployment_mode or "UNKNOWN",
            reason=_norm(reason),
        )
    except Exception:  # pragma: no cover
        _obs_shim_log.exception("ws_public_reconnect failed")


def ws_public_error(
    exchange: str,
    *,
    region: str,
    deployment_mode: str,
    reason: str = "unknown",
) -> None:
    """Incrémente le compteur d'erreurs WS publics (v2)."""
    try:
        safe_inc(
            WS_PUBLIC_ERRORS_TOTAL_V2,
            "ws_public_errors_total_v2",
            "ws_public_error",
            exchange=(exchange or "UNKNOWN").upper(),
            region=region or "UNKNOWN",
            deployment_mode=deployment_mode or "UNKNOWN",
            reason=_norm(reason),
        )
    except Exception:  # pragma: no cover
        _obs_shim_log.exception("ws_public_error failed")


def ws_public_backoff(
    exchange: str,
    *,
    region: str,
    deployment_mode: str,
    seconds: float,
) -> None:
    """Met à jour le backoff courant WS publics (v2)."""
    try:
        safe_set(
            WS_PUBLIC_BACKOFF_SECONDS_V2,
            "ws_public_backoff_seconds_v2",
            "ws_public_backoff",
            max(0.0, float(seconds)),
            exchange=(exchange or "UNKNOWN").upper(),
            region=region or "UNKNOWN",
            deployment_mode=deployment_mode or "UNKNOWN",
        )
    except Exception:  # pragma: no cover
        _obs_shim_log.exception("ws_public_backoff failed")


def ws_public_connections_open(
    exchange: str,
    *,
    region: str,
    deployment_mode: str,
    value: int,
) -> None:
    """Met à jour le nombre de connexions WS publiques ouvertes (v2)."""
    try:
        safe_set(
            WS_PUBLIC_CONNECTIONS_OPEN_V2,
            "ws_public_connections_open_v2",
            "ws_public_connections_open",
            max(0, int(value)),
            exchange=(exchange or "UNKNOWN").upper(),
            region=region or "UNKNOWN",
            deployment_mode=deployment_mode or "UNKNOWN",
        )
    except Exception:  # pragma: no cover
        _obs_shim_log.exception("ws_public_connections_open failed")


def ws_public_drop(
    exchange: str,
    *,
    region: str,
    deployment_mode: str,
    reason: str = "unknown",
) -> None:
    """Incrémente le compteur d'événements WS publics droppés (v2)."""
    try:
        safe_inc(
            WS_PUBLIC_DROPPED_TOTAL_V2,
            "ws_public_dropped_total_v2",
            "ws_public_drop",
            exchange=(exchange or "UNKNOWN").upper(),
            region=region or "UNKNOWN",
            deployment_mode=deployment_mode or "UNKNOWN",
            reason=_norm(reason),
        )
    except Exception:  # pragma: no cover
        _obs_shim_log.exception("ws_public_drop failed")


def ws_public_staleness(
    exchange: str,
    *,
    region: str,
    deployment_mode: str,
    seconds: float,
) -> None:
    """Met à jour la staleness estimée d'un flux WS public (v2)."""
    try:
        safe_set(
            WS_PUBLIC_STALENESS_SECONDS,
            "ws_public_staleness_seconds",
            "ws_public_staleness",
            max(0.0, float(seconds)),
            exchange=(exchange or "UNKNOWN").upper(),
            region=region or "UNKNOWN",
            deployment_mode=deployment_mode or "UNKNOWN",
        )
    except Exception:  # pragma: no cover
        _obs_shim_log.exception("ws_public_staleness failed")


# --- Wrappers legacy (compat v1) ---




# --- Wrappers “note_*” consommés par websockets_clients.py -----------------


def ws_public_note_connection_open(
    exchange: str,
    region: str,
    deployment_mode: str,
    *,
    open_count: int,
) -> None:
    """
    Wrapper appelé par websockets_clients.py quand une connexion WS publique
    est (re)ouverte.

    Effet:
      - met à jour WS_PUBLIC_CONNECTIONS_OPEN_V2 avec exchange/region/mode.
    """
    try:
        ws_public_connections_open(
            exchange=exchange,
            region=region,
            deployment_mode=deployment_mode,
            value=open_count,
        )
    except Exception:  # best effort, jamais de propagation vers le flux métier
        return


def ws_public_note_connection_closed(
    exchange: str,
    region: str,
    deployment_mode: str,
    *,
    open_count: int,
) -> None:
    """
    Wrapper appelé par websockets_clients.py quand une connexion WS publique
    est fermée.

    Effet:
      - met à jour WS_PUBLIC_CONNECTIONS_OPEN_V2 avec le nouveau compteur.
    """
    try:
        ws_public_connections_open(
            exchange=exchange,
            region=region,
            deployment_mode=deployment_mode,
            value=open_count,
        )
    except Exception:
        return


def ws_public_note_reconnect(
    exchange: str,
    region: str,
    deployment_mode: str,
    *,
    reason: str,
    delay_s: float | int | None = None,
) -> None:
    """
    Wrapper pour un reconnect.

    Effets:
      - incrémente WS_RECONNECTS_TOTAL_V2 avec exchange/region/mode + reason,
      - met à jour WS_PUBLIC_BACKOFF_SECONDS_V2 si delay_s est fourni.
    """
    try:
        ws_public_reconnect(
            exchange=exchange,
            region=region,
            deployment_mode=deployment_mode,
            reason=reason,
        )
        if delay_s is not None:
            ws_public_backoff(
                exchange=exchange,
                region=region,
                deployment_mode=deployment_mode,
                delay_s=float(delay_s),
            )
    except Exception:
        return


def ws_public_note_event_ok(
    exchange: str,
    region: str,
    deployment_mode: str,
    *,
    kind: str,
) -> None:
    """
    Wrapper pour un message WS consommé correctement.

    kind = type logique du flux ("combo", "l2", "trades", ...).
    """
    try:
        ws_public_event(
            exchange=exchange,
            region=region,
            deployment_mode=deployment_mode,
            stream=kind,
            ok=True,
        )
    except Exception:
        return

def ws_public_note_event_received(
    exchange: str,
    region: str,
    deployment_mode: str,
    *,
    latency_ms: float,
) -> None:
    """
    Publie une estimation de staleness (secondes) des events WS publics.
    """
    try:
        safe_set(
            WS_PUBLIC_STALENESS_SECONDS,
            "ws_public_staleness_seconds",
            "ws_public_note_event_received",
            max(0.0, float(latency_ms)) / 1000.0,
            exchange=(exchange or "UNKNOWN").upper(),
            region=region or "UNKNOWN",
            deployment_mode=deployment_mode or "UNKNOWN",
        )
    except Exception:
        return

def ws_public_note_event_dropped(
    exchange: str,
    region: str,
    deployment_mode: str,
    *,
    reason: str,
    kind: str | None = None,
) -> None:
    """
    Wrapper pour un message WS droppé.

    Effets:
      - incrémente WS_PUBLIC_DROPPED_TOTAL_V2 (taggé par reason),
      - optionnel: logge aussi un event WS_PUBLIC_EVENTS_TOTAL_V2 avec ok=False
        et stream=kind si fourni.
    """
    try:
        # compat legacy: compteur exchange + reason
        WS_PUBLIC_DROPPED_TOTAL.labels(exchange=exchange, reason=reason).inc()
        # compteur “dropped” taggé par raison
        ws_public_drop(
            exchange=exchange,
            region=region,
            deployment_mode=deployment_mode,
            reason=reason,
        )

        # trace dans le flux d’events (avec ok=False) si on connaît le type
        if kind:
            ws_public_event(
                exchange=exchange,
                region=region,
                deployment_mode=deployment_mode,
                stream=kind,
                ok=False,
            )
    except Exception:
        return




def mark_books_fresh_by_exchange(exchange: str) -> None:
    """Marks the synthetic pair "{EX}:ALL" as fresh to retain per-exchange granularity."""

    try:
        ex = (exchange or 'ALL').upper()
        mark_books_fresh(f'{ex}:ALL')
    except Exception:
        _obs_shim_log.exception('mark_books_fresh_by_exchange failed')


def router_on_combo_event(
    combo: str,
    skew_ms: float | None = None,
    ages_ms_by_exchange: dict[str, float] | None = None,
) -> None:
    try:
        if isinstance(skew_ms, (int, float)):
            ROUTER_COMBO_SKEW_MS.labels(str(combo)).observe(max(0.0, float(skew_ms)))
    except Exception:  # pragma: no cover - metrics optional
        _obs_shim_log.exception('router_on_combo_event failed')



def bump_scanner(
    spread_pct: float | None = None,
    active_pairs: int | None = None,
    *,
    load: float | None = None,
    queue_drop: int | None = None,
) -> None:
    """
    Scanner heartbeat (D7):

    - load (0..1) -> SCANNER_GLOBAL_LOAD gauge
    - queue_drop est accepté pour compat avec les call-sites (ne doit JAMAIS lever TypeError)
    - spread_pct/active_pairs : gardés pour compat (pas exportés ici pour l’instant)

    Objectif: éviter le "silence" Grafana (load=0 permanent car bump_scanner no-op)
    + supprimer les exceptions TypeError silencieuses dues à bump_scanner(queue_drop=1).
    """
    try:
        if load is not None:
            v = float(load)
            # clamp + NaN guard
            if v != v:  # NaN
                v = 0.0
            if v < 0.0:
                v = 0.0
            elif v > 1.0:
                v = 1.0
            safe_set(SCANNER_GLOBAL_LOAD, "scanner_global_load", "bump_scanner", v)
    except Exception:
        try:
            OBS_NOOP_TOTAL.labels(metric="scanner_global_load", where="bump_scanner").inc()
        except Exception:
            pass

    # queue_drop/spread_pct/active_pairs: best-effort (compat), volontairement no-op côté métriques
    # (on préfère ne pas inventer des métriques sans dashboard associé).
    try:
        if queue_drop:
            SCANNER_QUEUE_DROPPED_TOTAL.labels(reason="queue_full").inc(float(queue_drop))
    except Exception:
        try:
            OBS_NOOP_TOTAL.labels(metric="scanner_queue_dropped_total", where="bump_scanner").inc()
        except Exception:
            pass
    return None


def set_engine_queue(n: int) -> None:
    try:
        ENGINE_SUBMIT_QUEUE_DEPTH.set(max(0, int(n)))
    except Exception:
        _obs_shim_log.exception('set_engine_queue failed')


def inc_ack_timeout() -> None:
    try:
        ENGINE_ACK_TIMEOUT_TOTAL.inc()
    except Exception:
        _obs_shim_log.exception('inc_ack_timeout failed')


def _norm_shim(v: Any) -> str:
    if v is None:
        return 'none'
    s = str(v)
    return s if s else 'empty'


def inc_engine_trade(result: str, kind: str, mode: str = 'standard') -> None:
    try:
        TRADES_LIVE_DAY_TOTAL.labels(_norm_shim(result)).inc()
    except Exception:
        _obs_shim_log.exception('inc_engine_trade failed')


def observe_engine_latency(seconds: float) -> None:
    try:
        ENGINE_DRAIN_LATENCY_MS.observe(max(0.0, float(seconds) * 1000.0))
    except Exception:
        _obs_shim_log.exception('observe_engine_latency failed')


def mark_engine_ack(submit_ts_ns: int) -> None:
    """Legacy shim mapping Engine submit→ack timings to Prometheus."""

    try:
        dt_ms = max(0.0, (time.perf_counter_ns() - int(submit_ts_ns)) / 1_000_000.0)
        ENGINE_SUBMIT_TO_ACK_MS.observe(dt_ms)
    except Exception:
        _obs_shim_log.exception('mark_engine_ack failed')


def record_pipeline_timings(trace: Dict[str, Any]) -> None:
    """Best-effort mapper for legacy dict payloads into Engine histograms."""

    try:
        t_sub = trace.get('t_engine_submit_ms')
        t_ack = trace.get('t_engine_ack_ms')
        t_ff = trace.get('t_first_fill_ms')
        if isinstance(t_sub, (int, float)) and isinstance(t_ack, (int, float)) and (t_ack >= t_sub):
            ENGINE_SUBMIT_TO_ACK_MS.observe(float(t_ack - t_sub))
        if isinstance(t_sub, (int, float)) and isinstance(t_ff, (int, float)) and (t_ff >= t_sub):
            ENGINE_ACK_TO_FILL_MS.observe(float(t_ff - t_sub))
    except Exception:
        _obs_shim_log.exception('record_pipeline_timings failed')


def inc_scanner_rejection(reason: str, route: str = 'n/a', pair: str = 'n/a', strategy: str = 'TT') -> None:
    try:
        if SCANNER_REJECTED_TOTAL is not None and not isinstance(SCANNER_REJECTED_TOTAL, _MetricNoOp):
            SCANNER_REJECTED_TOTAL.labels(
                reason=str(reason),
                route=str(route),
                pair=str(pair).upper(),
                strategy=str(strategy).upper()
            ).inc()
        # Ticket D4: Unified Robustness
        if 'REJECT_TOTAL_COUNTER' in globals():
            REJECT_TOTAL_COUNTER.labels(layer="SCANNER", reason=str(reason)).inc()
    except Exception:
        _obs_shim_log.exception('inc_scanner_rejection failed')


def inc_scanner_emitted(route: str = 'n/a', pair: str = 'n/a', strategy: str = 'TT') -> None:
    try:
        if OPPORTUNITIES_EMITTED_TOTAL is not None and not isinstance(OPPORTUNITIES_EMITTED_TOTAL, _MetricNoOp):
            OPPORTUNITIES_EMITTED_TOTAL.labels(
                strategy=str(strategy).upper(),
                pair=str(pair).upper()
            ).inc()
    except Exception:
        _obs_shim_log.exception('inc_scanner_emitted failed')


def inc_scanner_decision(pair: str = 'n/a', strategy: str = 'TT') -> None:
    try:
        if FINAL_DECISIONS_TOTAL is not None and not isinstance(FINAL_DECISIONS_TOTAL, _MetricNoOp):
            FINAL_DECISIONS_TOTAL.labels(
                strategy=str(strategy).upper(),
                pair=str(pair).upper()
            ).inc()
    except Exception:
        _obs_shim_log.exception('inc_scanner_decision failed')


def observe_scanner_latency(route: str, seconds: float) -> None:
    try:
        ROUTER_TO_SCANNER_MS.labels(str(route)).observe(max(0.0, float(seconds) * 1000.0))
    except Exception:
        _obs_shim_log.exception('observe_scanner_latency failed')





__all__ = ['BUCKETS_MS',"LAT_ACK_MS", "LAT_FILL_FIRST_MS", "LAT_FILL_ALL_MS", "LAT_E2E_MS","LOGGERH_FILE_ROTATIONS_TOTAL",
    "LAT_EVENTS_TOTAL", "LAT_PIPELINE_EVENTS_TOTAL","OBS_READY", "obs_is_ready",
           "PAIR_HISTORY_ROWS_TOTAL", "PAIR_HISTORY_COMPUTE_MS", 'set_region', 'set_deployment_mode', 'lbl_exchange',
           'lbl_region', 'lbl_mode', 'start_time_skew_probe', 'start_loop_lag_probe', 'update_time_skew',
           'TIME_SKEW_MS', 'TIME_SKEW_STATUS', 'EVENT_LOOP_LAG_MS', 'report_nonfatal', 'inc_blocked',
           'NONFATAL_ERRORS_TOTAL', 'BLOCKED_TOTAL', 'BF_API_ERRORS_TOTAL', 'BF_API_LATENCY_MS', 'BF_CACHE_AGE_SECONDS',
           'BF_LAST_SUCCESS_TS', 'FEE_TOKEN_BALANCE', 'BF_BALANCES_TTL_NORMAL_SECONDS',
           'BF_BALANCES_TTL_DEGRADED_SECONDS', 'BF_BALANCES_TTL_BLOCK_SECONDS', 'BF_BALANCES_HEALTH_STATE',
           'BF_429_TOTAL', 'DISCOVERY_429_TOTAL', 'discovery_note_rate_limit', 'mark_bf_latency', 'RPC_LATENCY_MS',
           'RPC_ERR_TOTAL', 'RPC_RETRIES_TOTAL', 'RPC_PAYLOAD_REJECTED_TOTAL', 'ROUTER_QUEUE_DEPTH',
           'ROUTER_PAIR_QUEUE_DEPTH', 'ROUTER_QUEUE_HIGH_WATERMARK_TOTAL', 'ROUTER_QUEUE_DEPTH_BY_EX',
           'ROUTER_DROPPED_TOTAL', 'ROUTER_COMBO_SKEW_MS', 'ROUTER_TO_SCANNER_MS', 'ROUTER_TO_SCANNER_ERRORS_TOTAL',
           'mark_router_to_scanner', 'SCANNER_DECISION_MS', 'SCANNER_GLOBAL_LOAD', 'SCANNER_RATE_LIMITED_TOTAL',
           'SCANNER_EMITTED_TOTAL', 'SCANNER_REJECTIONS_TOTAL', 'SCANNER_MM_LEG_RESTRICTED_TOTAL', 'SC_STRATEGY_SCORE',
           'SC_ELIGIBLE', 'SC_BANNED',
           'SC_PROMOTED_PRIMARY', 'SC_ROTATION_PRIMARY_SIZE', 'SC_ROTATION_AUDITION_SIZE', 'RM_DECISION_MS',
           'mark_scanner_to_rm', 'INVENTORY_USD', 'RM_REJECT_TOTAL', 'PAIR_HEALTH_PENALTY_TOTAL', 'VOL_EWMA_BPS',
           'VOL_P95_BPS', 'FEE_MISMATCH_TOTAL', 'FEES_EXPECTED_BPS', 'FEES_REALIZED_BPS', 'FEESYNC_LAST_TS',
           'FEESYNC_ERRORS', 'REBAL_DETECTED_TOTAL', 'REBAL_PLAN_QUANTUM_QUOTE', 'RM_PAUSED_COUNT',
           'LAST_BOOKS_FRESH_TS', 'LAST_BALANCES_FRESH_TS', 'DYNAMIC_MIN_BPS', 'mark_books_fresh',
           'mark_balances_fresh', 'set_rm_paused_count', 'set_dynamic_min', 'inc_rm_reject', 'inc_rm_skip',
           'mark_rm_to_engine', 'MM_FILLS_BOTH', 'MM_SINGLE_FILL_HEDGED', 'MM_PANIC_HEDGE_TOTAL',
           'ENGINE_SUBMIT_TO_ACK_MS', 'ENGINE_ACK_TO_FILL_MS', 'ENGINE_CANCELLATIONS_TOTAL', 'ENGINE_RETRIES_TOTAL',
           'ENGINE_QUEUEPOS_BLOCKED_TOTAL', 'ENGINE_SUBMIT_QUEUE_DEPTH', 'INFLIGHT_GAUGE', 'ENGINE_DEDUP_HITS_TOTAL',
           'PNL_LIVE_DAY_USD', 'TRADES_LIVE_DAY_TOTAL', 'DERIVED_NET_PROFIT_SIGN_TOTAL', 'MISSING_NET_PROFIT_TOTAL',
           'ENGINE_PACER_DELAY_MS', 'ENGINE_PACER_INFLIGHT_MAX', 'ENGINE_PACER_MODE', 'PACER_ACK_TARGET_MS',
           'PACER_ACK_HI_MS', 'PACER_ACK_SEV_MS', 'ENGINE_DRAIN_LATENCY_MS', 'ENGINE_PACING_BACKPRESSURE_TOTAL',
           'inc_engine_pacing_backpressure', 'WS_RECONNECTS_TOTAL', 'WS_BACKOFF_SECONDS', 'WS_CONNECTIONS_OPEN',
           'PACER_STATE', 'PACER_CLAMP_SECONDS', 'ENGINE_MUTE_TOTAL', 'FEE_TOKEN_LEVEL', 'FEE_TOKEN_TARGET_PERCENT',
           'PWS_DEDUP_HITS_TOTAL', 'PWS_RECONNECTS_TOTAL', 'PWS_EVENT_LAG_MS', 'PWS_TRANSFERS_TOTAL',
           'PWS_EVENTS_TOTAL', 'PWS_BACKOFF_SECONDS', 'PWS_HEARTBEAT_GAP_SECONDS', 'PWS_DROPPED_TOTAL',
           'PWS_ACK_LATENCY_MS', 'PWS_FILL_LATENCY_MS', 'WS_FAILOVER_TOTAL', 'PWS_POOL_SIZE', 'PWS_QUEUE_DEPTH',
           'PWS_QUEUE_CAP', 'WS_RECO_RUN_MS', 'WS_RECO_ERRORS_TOTAL', 'RECONCILE_MISS_TOTAL', 'RECONCILE_RESYNC_TOTAL',
           'RECONCILE_RESYNC_LATENCY_MS', 'COLD_RESYNC_TOTAL', 'COLD_RESYNC_RUN_MS', 'recon_run_ms', 'recon_error',
           'recon_on_resync', 'recon_observe_latency', 'pws_on_failover', 'pws_set_pool_size', 'LOGGERH_WRITE_MS',
           'LOGGERH_QUEUE_PLATEAU_TOTAL', 'LHM_JSONL_INGESTED_TOTAL', 'LHM_JSONL_DROPPED_TOTAL', 'LHM_JSONL_QUEUE_SIZE',
           'LOGGERH_TRADE_QUEUE_SIZE', 'LOGGERH_JSONL_ROTATIONS_TOTAL', 'LOGGERH_LAST_FLUSH_TS_SECONDS',
           'LOGGERH_LAST_ROTATION_TS_SECONDS', 'loggerh_observe_write_ms', 'lhm_on_ingested', 'lhm_on_dropped',
           'lhm_set_queue_size', 'lhm_on_rotation', 'loggerh_set_last_flush_now', 'loggerh_set_last_rotation_now',
           'STORAGE_USAGE_PCT', 'STORAGE_BYTES_FREE', 'STORAGE_ALERTS_TOTAL', 'LOGGERH_JSONL_BYTES',
           'LOGGERH_DB_STALLS_TOTAL', 'LOGGERH_DB_FILE_BYTES', 'update_storage_metrics', 'SIM_DECISION_MS',
           'SIMULATED_VWAP_DEVIATION_BPS', 'sim_on_run', 'PAYLOAD_REJECTED_TOTAL', 'ObsServer', 'StatusHTTPServer',
           'MainMetrics', 'BOT_STARTUPS_TOTAL', 'BOT_STATE', 'start_servers_from_env', 'WS_RECONNECTS_TOTAL',
           'RM_DECISION_MS', 'RM_PREFLIGHT_MS', 'RM_DECISIONS_TOTAL', 'RM_SKIPS_TOTAL', 'RM_QUEUE_DEPTH',
           'RM_REVALIDATE_MS', 'RM_FRAGMENT_PROFIT_MS', 'PAIR_HEALTH_PENALTY_TOTAL', 'POOL_GATE_THROTTLES_TOTAL',
           'RM_FINAL_DECISIONS_TOTAL', 'RM_ADMITTED_TOTAL', 'RM_DROPPED_TOTAL', "VOL_PRICE_VOL_MICRO",                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              "VOL_PRICE_VOL_MICRO",
           "VOL_SPREAD_VOL_MICRO",
           "VOL_PRICE_PCTL",
           "VOL_SPREAD_PCTL",
           "VOL_ANOMALY_TOTAL",
           "VOL_SIGNAL_STATE",
           "VOL_BAND_TOTAL",
           "VOL_AGE_SECONDS",
           "SLIP_AGE_SECONDS",
           "set_vol_age_seconds",
           "set_slip_age_seconds",
           "FEE_SNAPSHOT_AGE_SECONDS",
           "TOTAL_COST_BPS",
           ]
__all__ += [
    'BF_HTTP_LATENCY_SECONDS',
    'BF_HTTP_ERRORS_TOTAL',
    'BF_FEE_TOKEN_LEVEL',
    'BF_FEE_TOKEN_LOW_TOTAL',
    'CONTRACTS_HELPERS_CALLS_TOTAL',
    'CONTRACTS_VALIDATION_ERRORS_TOTAL',
    'SCANNER_EVAL_MS',
    'PWS_QUEUE_FILL_RATIO',
    'PWS_QUEUE_SATURATION_TOTAL',
    'PWS_HEARTBEAT_GAP_BREACH_TOTAL',
    'PWS_ALERT_TOTAL',
    'WS_RECO_MISS_PER_MINUTE',
    'WS_RECO_MISS_BURST_TOTAL',
]
__all__ += [
    'PNL_DERIVED_FROM_BPS_TOTAL',
    'PNL_VIEW_BUILD_MS',
]
__all__ += [
    'ObsServer',
    'router_health_on_beat',
    'router_on_combo_event',
    'mark_books_fresh_by_exchange',
    'set_engine_queue',
    'inc_ack_timeout',
    'inc_engine_trade',
    'observe_engine_latency',
    'mark_engine_ack',
    'record_pipeline_timings',
    'inc_scanner_rejection',
    'inc_scanner_emitted',
    'observe_scanner_latency',
    'set_engine_running',
    'mark_router_to_scanner_ts',
    'mark_scanner_to_rm_ts',

]
__all__ += [
    'lbl_alias',
    'RM_BALANCES_TTL_BREACH',
    'RM_BALANCES_STALE_TOTAL',
    'RM_CAPITAL_MOVE_VISIBILITY_LATENCY_S',
    'RM_CAPITAL_MOVE_VISIBILITY_TOTAL',
    'RM_CAPITAL_MOVE_TOTAL',
    'RM_CAPITAL_MOVE_NOTIONAL_USD',
# --- nouveaux exports M5-B3 (LHM SLO) ---
    'LHM_PIPELINE_LAG_SECONDS',
    'LHM_SLO_WRITE_MS_P95_TARGET',
    'LHM_SLO_QUEUE_DEPTH_MAX_TARGET',
    'LHM_SLO_PIPELINE_LAG_MAX_SECONDS_TARGET',
    'LHM_SLO_DROPPED_TRADES_BUDGET',
    'lhm_set_pipeline_lag',
    'LHM_CRITICAL_DROP_SEEN',
    'LHM_STORAGE_ERROR_SEEN',
    'TRADE_FSM_OPEN_BUNDLES',
    'RECONCILE_BACKLOG',
    'BUNDLE_TRUTH_DIVERGENCE_TOTAL',
    'set_lhm_pipeline_flags',
    'set_trade_fsm_open_bundles',
    'set_reconcile_backlog',
    'note_bundle_truth_divergence',
    "VOL_PRICE_VOL_MICRO",
    "VOL_SPREAD_VOL_MICRO",
    "VOL_PRICE_PCTL",
    "VOL_SPREAD_PCTL",
    "VOL_ANOMALY_TOTAL",
    "VOL_SIGNAL_STATE",
    "VOL_BAND_TOTAL",
    "VOL_AGE_SECONDS",
    "SLIP_AGE_SECONDS",
    "set_vol_age_seconds",
    "set_slip_age_seconds",
    "FEE_SNAPSHOT_AGE_SECONDS",
    "TOTAL_COST_BPS",
    'BF_BALANCES_TTL_NORMAL_SECONDS',
    'BF_BALANCES_TTL_DEGRADED_SECONDS',
    'BF_BALANCES_TTL_BLOCK_SECONDS',
    'BF_BALANCES_HEALTH_STATE',
    "ENGINE_ROUTER_PROC_US",
    "ENGINE_ROUTER_TO_SCANNER_LAT_MS",
    "SCANNER_DECISION_MS",
    "SCANNER_EVAL_MS",
    "RM_DECISION_MS",
    "LOGGERH_WRITE_MS",
    "LOGGERH_DB_LANE_DROPS_TOTAL",
    "LOGGERH_DB_LANE_QUEUE_DEPTH",
    "SCANNER_REJECTED_TOTAL",
    "SCANNER_RATE_LIMITED_TOTAL",
    "ROUTER_QUEUE_DEPTH",
    "ROUTER_PAIR_QUEUE_DEPTH",
    "ROUTER_DROPPED_TOTAL",
    "OPPORTUNITIES_EMITTED_TOTAL",
    "RM_STATUS_INFO",
    "RM_QUEUE_DEPTH",
    "FINAL_DECISIONS_TOTAL",
    "PACER_DELAY_MS",
    "PACING_BACKPRESSURE_TOTAL",
    "MM_PANIC_HEDGE_TOTAL",
    "QUEUEPOS_BLOCKED_TOTAL",
    "WS_PUBLIC_FRAME_RECEIVED_TOTAL",
    "WS_PUBLIC_DROPPED_TOTAL",
    "WS_PUBLIC_BACKOFF_SECONDS",
    "WS_PUBLIC_CONN_OPEN_TOTAL",
    "WS_PUBLIC_STALENESS_SECONDS",
    "TIME_SKEW_MS",
    "OBS_INIT_ERRORS_TOTAL",
]

# --- Classes et Fonctions de support ---
class _MetricNoOp:
    def labels(self, *args, **kwargs): return self
    def inc(self, *args, **kwargs): pass
    def set(self, *args, **kwargs): pass
    def observe(self, *args, **kwargs): pass

def lbl_exchange(ex: str) -> str:
    return str(ex or "UNKNOWN").upper()

# --- PUBLIC WS: Métriques V2 (Pipeline Standardisé) ---
WS_PUBLIC_FRAME_RECEIVED_TOTAL = _metric(
    Counter,
    "ws_public_frame_received_total",
    "Nombre total de frames reçues via WS public (V2)",
    ["exchange", "region", "deployment_mode"]
)

WS_PUBLIC_DROPPED_TOTAL = _metric(
    Counter,
    "ws_public_dropped_total",
    "Nombre total d'événements jetés (V2)",
    ["exchange", "region", "deployment_mode", "reason", "kind"]
)

WS_PUBLIC_BACKOFF_SECONDS = _metric(
    Counter,
    "ws_public_backoff_seconds_total",
    "Temps total passé en backoff (V2)",
    ["exchange", "region", "deployment_mode", "reason"]
)

WS_PUBLIC_CONN_OPEN_TOTAL = _metric(
    Counter,
    "ws_public_reconnects_total",
    "Nombre total de reconnexions WS (V2)",
    ["exchange", "region", "deployment_mode"]
)

WS_PUBLIC_STALENESS_SECONDS = _metric(
    Gauge,
    "ws_public_staleness_seconds",
    "Temps écoulé depuis le dernier message reçu (s)",
    ["exchange", "region", "deployment_mode"]
)

TIME_SKEW_MS = _metric(
    Gauge,
    "time_skew_ms",
    "Décalage d'horloge détecté entre le bot et l'exchange (ms)",
    ["exchange"]
)

def ws_public_note_frame_received(exchange: str, region: str, deployment_mode: str):
    """V2: Note la réception d'une frame brute."""
    try:
        if WS_PUBLIC_FRAME_RECEIVED_TOTAL is not None and not isinstance(WS_PUBLIC_FRAME_RECEIVED_TOTAL, _MetricNoOp):
            WS_PUBLIC_FRAME_RECEIVED_TOTAL.labels(
                exchange=exchange.upper(), 
                region=region.upper(), 
                deployment_mode=deployment_mode.upper()
            ).inc()
    except Exception: pass

def ws_public_note_backoff(exchange: str, region: str, deployment_mode: str, reason: str, duration_s: float):
    """V2: Enregistre une pénalité de backoff."""
    try:
        if WS_PUBLIC_BACKOFF_SECONDS is not None and not isinstance(WS_PUBLIC_BACKOFF_SECONDS, _MetricNoOp):
            WS_PUBLIC_BACKOFF_SECONDS.labels(
                exchange=exchange.upper(), 
                region=region.upper(), 
                deployment_mode=deployment_mode.upper(),
                reason=str(reason)
            ).inc(float(duration_s))
    except Exception: pass

# --- RiskManager: Status et Santé ---
RM_STATUS_INFO = _metric(
    Gauge,
    "rm_status_info",
    "Statut de santé des composants du RiskManager (1=OK, 0=KO)",
    ["exchange", "component", "shard"]
)

RM_QUEUE_DEPTH = _metric(
    Gauge,
    "rm_queue_depth",
    "Nombre d'événements en attente dans le RiskManager",
    ["exchange"]
)

FINAL_DECISIONS_TOTAL = _metric(
    Counter,
    "final_decisions_total",
    "Nombre total de décisions finales prises (Autorisées)",
    ["strategy", "pair"]
)

def update_rm_status(exchange: str, component: str, status: float, shard: str = "S0"):
    """Met à jour le statut d'un composant RM pour Prometheus."""
    try:
        if RM_STATUS_INFO is not None and not isinstance(RM_STATUS_INFO, _MetricNoOp):
            RM_STATUS_INFO.labels(
                exchange=exchange.upper(), 
                component=component.lower(), 
                shard=shard.upper()
            ).set(status)
    except Exception:
        pass

# --- Logger: Performance écriture ---
LOGGERH_WRITE_MS = _metric(
    Histogram,
    "loggerh_write_ms",
    "Temps d'écriture des logs sur disque (ms)",
    buckets=(0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0)
)

LOGGERH_DB_LANE_DROPS_TOTAL = _metric(
    Counter,
    "loggerh_db_lane_drops_total",
    "Nombre de logs jetés car la DB lane est saturée",
    ["stream"]
)

LOGGERH_DB_LANE_QUEUE_DEPTH = _metric(
    Gauge,
    "loggerh_db_lane_queue_depth",
    "Profondeur de la file d'attente DB Lane",
    ["stream"]
)

# --- Scanner: Décisions et évaluations ---
SCANNER_DECISION_MS = _metric(
    Histogram,
    "scanner_decision_ms",
    "Temps de décision globale du scanner (ms)",
    buckets=(0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0)
)

SCANNER_REJECTED_TOTAL = _metric(
    Counter,
    "scanner_rejected_total",
    "Nombre total d'opportunités rejetées par le scanner",
    ["reason", "route", "pair", "strategy"]
)

SCANNER_EVAL_MS = _metric(
    Histogram,
    "scanner_eval_ms",
    "Temps d'évaluation par paire/route (ms)",
    ["pair", "route"],
    buckets=(0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0)
)

REBAL_PLAN_BUILD_MS = _metric(
    Histogram,
    "rebal_plan_build_ms",
    "Temps de construction du plan de rebalancing (ms)",
    buckets=(1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0)
)

REBAL_OPERATIONS_TOTAL = _metric(
    Counter,
    "rebal_operations_total",
    "Nombre total d'opérations de rebalancing générées",
    ["type", "exchange", "status"]
)

REBAL_TRANSFER_LATENCY_MS = _metric(
    Histogram,
    "rebal_transfer_latency_ms",
    "Latence des transferts internes (ms)",
    ["exchange", "type"],
    buckets=(10, 50, 100, 250, 500, 1000, 2500, 5000, 10000),
)

REBAL_TRANSFER_RL_WAIT_MS = _metric(
    Histogram,
    "rebal_transfer_rl_wait_ms",
    "Temps d'attente rate-limit transferts (ms)",
    ["exchange"],
    buckets=(1, 5, 10, 50, 100, 500, 1000, 5000),
)

REBAL_SNAPSHOTS_AGE_S = _metric(
    Gauge,
    "rebal_snapshots_age_s",
    "Âge des snapshots utilisés par le rebalancing",
    ["kind"]
)

REBAL_IMBALANCE_GAUGE = _metric(
    Gauge,
    "rebal_imbalance_gauge",
    "Déséquilibre détecté par actif et exchange (valeur quote)",
    ["exchange", "asset"]
)

REBAL_LOCK_CONFLICT_TOTAL = _metric(
    Counter,
    'rebal_lock_conflict_total',
    'Nombre de fois où une opération REB est bloquée par un verrou actif',
    ['pair', 'route']
)

REBAL_SUCCESS_TOTAL = _metric(
    Counter,
    'rebal_success_total',
    'Nombre total de rebalancements réussis',
    ['branch', 'pair']
)

REBAL_ABORT_TOTAL = _metric(
    Counter,
    'rebal_abort_total',
    'Nombre total de rebalancements interrompus',
    ['branch', 'pair', 'reason']
)

SCANNER_RATE_LIMITED_TOTAL = _metric(
    Counter,
    "scanner_rate_limited_total",
    "Nombre d'événements rejetés par le rate limiter",
    ["kind"]
)

# --- Observabilité: Erreurs d'initialisation ---
OBS_INIT_ERRORS_TOTAL = _metric(
    Counter,
    "obs_init_errors_total",
    "Nombre d'erreurs lors de l'initialisation de l'observabilité",
    ["module"]
)

OPPORTUNITIES_EMITTED_TOTAL = _metric(
    Counter,
    "opportunities_emitted_total",
    "Nombre total d'opportunités envoyées à l'Engine",
    ["strategy", "pair"]
)

RM_DECISION_MS = _metric(
    Histogram,
    "rm_decision_ms",
    "Temps de décision du RiskManager par cohorte (ms)",
    ["cohort"],
    buckets=(0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0)
)

# --- Engine: Latence d'exécution (Tick-to-Trade) ---
ENGINE_EXECUTION_LATENCY_MS = _metric(
    Histogram,
    "engine_execution_latency_ms",
    "Latence entre réception opportunité et envoi de l'ordre (ms)",
    ["exchange", "strategy", "pair"],
    buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0)
)

PACER_DELAY_MS = _metric(
    Gauge,
    "pacer_delay_ms",
    "Délai imposé par l'EnginePacer (ms)",
    ["exchange"]
)

PACING_BACKPRESSURE_TOTAL = _metric(
    Counter,
    "pacing_backpressure_total",
    "Nombre d'opportunités bloquées par le pacing",
    ["exchange", "strategy"]
)

MM_PANIC_HEDGE_TOTAL = _metric(
    Counter,
    "mm_panic_hedge_total",
    "Nombre de panic hedges déclenchés (MM)",
    ["exchange", "reason"]
)

QUEUEPOS_BLOCKED_TOTAL = _metric(
    Counter,
    "queuepos_blocked_total",
    "Nombre d'ordres bloqués par QueuePosition",
    ["exchange", "pair"]
)

ENGINE_ROUTER_PROC_US = _metric(
    Histogram,
    "engine_router_proc_us",
    "Temps de traitement interne du Router (μs)",
    ["exchange"],
    buckets=(1.0, 5.0, 10.0, 20.0, 50.0, 100.0, 250.0, 500.0, 1000.0)
)

ENGINE_ROUTER_TO_SCANNER_LAT_MS = _metric(
    Histogram,
    "engine_router_to_scanner_lat_ms",
    "Temps de transfert Router -> Scanner (ms)",
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 50.0)
)

def record_execution_latency(exchange: str, strategy: str, pair: str, latency_ms: float):
    """Enregistre la latence d'exécution (simulée ou réelle) dans Prometheus."""
    try:
        m = ENGINE_EXECUTION_LATENCY_MS
        if m is not None and not isinstance(m, _MetricNoOp):
            m.labels(
                exchange=lbl_exchange(exchange), 
                strategy=strategy.upper(), 
                pair=pair.upper()
            ).observe(max(0.0, float(latency_ms)))
    except Exception:
        pass

def record_router_proc_time(exchange: str, proc_time_us: float):
    """Enregistre le temps de traitement du Router."""
    try:
        m = ENGINE_ROUTER_PROC_US
        if m is not None and not isinstance(m, _MetricNoOp):
            m.labels(exchange=lbl_exchange(exchange)).observe(max(0.0, float(proc_time_us)))
    except Exception:
        pass
