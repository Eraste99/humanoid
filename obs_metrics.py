from __future__ import annotations
import asyncio
import json
import logging
import threading
import time
import os as _os
from typing import Any, Dict,List, Optional
from dataclasses import dataclass, asdict
log = logging.getLogger('obs_metrics')
_obs_shim_log = logging.getLogger('observability_shim')

from prometheus_client import Counter, Gauge, Histogram, REGISTRY

# --- BEGIN OM-0: STRICT + PROM READY + Noop ---
import os
STRICT_OBS = int(os.getenv("STRICT_OBS", "0"))  # 0 par défaut en dry-run

try:
    from prometheus_client import Counter, Gauge, Histogram  # déjà présent en général
    _PROM_READY = True
except Exception:
    # fallback: on exporte des classes no-op
    _PROM_READY = False
    class _NoopMetric:
        def labels(self, **kwargs): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def observe(self, *a, **k): pass
    Counter = Gauge = Histogram = _NoopMetric  # type: ignore
# --- END OM-0 ---
# --- BEGIN OM-1: fondations observabilité ---
# 1) No-op/erreurs d'init metrics
OBS_NOOP_TOTAL = Counter(  # increments quand un call obs échoue / est no-op
    "obs_noop_total",
    "Total des appels metrics no-op (init manquante, label mismatch, etc.)",
    ["metric", "where"]
)
OBS_INIT_ERROR_TOTAL = Counter(  # increments quand une metric ne peut pas être créée
    "obs_init_error_total",
    "Echecs d'initialisation de métriques",
    ["metric", "kind"]
)
OBS_LABEL_MISMATCH_TOTAL = Counter(  # increments quand labels() ne matchent pas la définition
    "obs_label_mismatch_total",
    "Labels fournis non conformes à la métrique déclarée",
    ["metric"]
)

def prom_ready() -> bool:
    """Expose readiness des métriques Prometheus pour les modules clients (ex: LHM.get_status)."""
    return bool(_PROM_READY)
# --- END OM-1 ---
# --- BEGIN OM-2: métriques utilisées par Engine/Scanner/LHM ---
# Scanner – hint top_qty manquant (utilisé dans Patch S4)
SCANNER_HINT_TOPQTY_MISSING_TOTAL = Counter(
    "scanner_hint_topqty_missing_total",
    "Hints L1 (top_qty) manquants (non bloquant)",
    ["pair", "ex", "side"]
)

# LHM – rotations fichiers (demandée précédemment)
LOGGERH_FILE_ROTATIONS_TOTAL = Counter(
    "loggerh_file_rotations_total",
    "Compteur de rotations de fichiers du LoggerHistorique",
    ["reason"]  # ex: size, time, manual
)

# Engine – erreurs/visibilité supplémentaires (si déjà définies, garde celles existantes)

AC_KV_ERRORS_TOTAL = Counter(
        "ac_kv_errors_total",
        "Erreurs KV (anti-crossing multi-pod)",
        ["kind"]  # set/get/eval_del
    )


AC_RESERVE_CONFLICT_TOTAL = Counter(
        "ac_reserve_conflict_total",
        "Conflits de réservation (anti-crossing)",
        ["branch", "ex"]  # TM/MM, EX
    )


AC_RELEASE_ERRORS_TOTAL = Counter(
        "ac_release_errors_total",
        "Erreurs de release (anti-crossing)",
        ["kind"]  # kv_delete/memory
    )


ENGINE_WORKER_ERRORS_TOTAL = Counter(
        "engine_worker_errors_total",
        "Erreurs dans les workers de l'Engine",
        ["phase"]  # W1/W2...
    )


ENGINE_INFLIGHT_GAUGE_SET_ERRORS_TOTAL = Counter(
        "engine_inflight_gauge_set_errors_total",
        "Echecs de set() du gauge in-flight",
        ["exchange"]
    )


ENGINE_BEST_PRICE_MISSING_TOTAL = Counter(
        "engine_best_price_missing_total",
        "Prix de référence manquant côté Engine",
        ["exchange", "pair", "side"]
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


def get_counter(name, documentation, labelnames=(), **kwargs):
    try:
        return Counter(name, documentation, labelnames=labelnames, **kwargs)
    except ValueError:
        return REGISTRY._names_to_collectors[name]

def get_gauge(name, documentation, labelnames=(), **kwargs):
    try:
        return Gauge(name, documentation, labelnames=labelnames, **kwargs)
    except ValueError:
        return REGISTRY._names_to_collectors[name]

def get_histogram(name, documentation, labelnames=(), **kwargs):
    try:
        return Histogram(name, documentation, labelnames=labelnames, **kwargs)
    except ValueError:
        return REGISTRY._names_to_collectors[name]
try:
    from prometheus_client import Counter, Gauge, Histogram, REGISTRY, start_http_server, CollectorRegistry, CONTENT_TYPE_LATEST, generate_latest
except Exception:
    REGISTRY = None
    CollectorRegistry = None
    CONTENT_TYPE_LATEST = 'text/plain; version=0.0.4; charset=utf-8'

    class _NoopMetric:

        def __init__(self, *_a, **_k):
            ...

        def labels(self, *_a, **_k):
            return self

        def inc(self, *_a, **_k):
            ...

        def set(self, *_a, **_k):
            ...

        def observe(self, *_a, **_k):
            ...
    Counter = Gauge = Histogram = _NoopMetric

    def start_http_server(*_a, **_k):
        logging.getLogger('obs_metrics').warning('prometheus_client absent: /metrics non démarré (no-op)')

    def generate_latest(_=None):
        return b'# prometheus_client indisponible\n'

def _metric(klass, name: str, doc: str, *args, **kwargs):
    """Retourne un collector existant s'il est déjà enregistré dans REGISTRY,
    sinon en crée un nouveau. Évite les ValueError en cas de double import."""
    reg = REGISTRY if 'REGISTRY' in globals() else None
    if reg is not None:
        try:
            existing = getattr(reg, '_names_to_collectors', {}).get(name)
            if existing is not None:
                return existing
        except Exception:
            pass
    return klass(name, doc, *args, **kwargs)
BUCKETS_MS = (1, 2, 5, 10, 20, 50, 75, 100, 150, 250, 400, 600, 1000, 1500, 2000, 3000, 5000, 8000, 12000)

def _norm(v: Any) -> str:
    if v is None:
        return 'none'
    s = str(v)
    return s if s else 'empty'

def lbl_exchange(ex: str) -> str:
    ex = _norm(ex).upper()
    return ex if ex in ('BINANCE', 'BYBIT', 'COINBASE') else ex

def lbl_region(r: str) -> str:
    r = _norm(r).upper()
    return r if r in ('EU', 'US', 'EU-CB') else r

def lbl_mode(m: str) -> str:
    m = _norm(m).upper()
    return m if m in ('EU_ONLY', 'SPLIT') else m

def _env_bool(name: str, default: bool) -> bool:
    v = str(_os.getenv(name, '1' if default else '0')).strip().lower()
    return v in ('1', 'true', 'yes', 'on')
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

def start_time_skew_probe(period_s: float=10.0) -> None:
    """Probe minimaliste: publie 0 ms (OK) périodiquement.
    Si tu as une source d'offset, appelle update_time_skew(ms) ailleurs.
    """
    global _time_skew_thread
    if _time_skew_thread and _time_skew_thread.is_alive():
        return

    def _runner():
        while True:
            try:
                TIME_SKEW_MS.set(0.0)
                TIME_SKEW_STATUS.set(1.0)
            except Exception:
                pass
            time.sleep(max(1.0, float(period_s)))
    _time_skew_thread = threading.Thread(target=_runner, name='time_skew_probe', daemon=True)
    _time_skew_thread.start()

def update_time_skew(ms: float, status: int=1) -> None:
    try:
        TIME_SKEW_MS.set(float(ms))
        TIME_SKEW_STATUS.set(float(status))
    except Exception:
        log.exception('update_time_skew failed')

def start_loop_lag_probe(period_s: float=0.5) -> None:
    """Mesure simple du lag: drift d'un sleep périodique."""
    global _loop_lag_thread
    if _loop_lag_thread and _loop_lag_thread.is_alive():
        return

    def _runner():
        next_t = time.perf_counter() + period_s
        while True:
            time.sleep(period_s)
            now = time.perf_counter()
            drift = (now - next_t) * 1000.0
            next_t = now + period_s
            try:
                EVENT_LOOP_LAG_MS.set(max(0.0, float(drift)))
            except Exception:
                pass
    _loop_lag_thread = threading.Thread(target=_runner, name='loop_lag_probe', daemon=True)
    _loop_lag_thread.start()
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
        BF_API_LATENCY_MS.labels(lbl_exchange(exchange), _norm(alias), _norm(endpoint)).observe(
            max(0.0, float(seconds * 1000.0))
        )
        if not ok:
            BF_API_ERRORS_TOTAL.labels(
                lbl_exchange(exchange),
                _norm(alias),
                _norm(endpoint),
                _norm(reason or 'error'),
            ).inc()
        else:
            BF_LAST_SUCCESS_TS.labels(lbl_exchange(exchange), _norm(alias)).set(time.time())
    except Exception:
        pass
RPC_LATENCY_MS = _metric(Histogram, 'rpc_latency_ms', 'RPC latency (ms)', ['method', 'region'], buckets=BUCKETS_MS)
RPC_ERR_TOTAL = _metric(Counter, 'rpc_err_total', 'RPC errors', ['code', 'method', 'region'])
RPC_RETRIES_TOTAL = _metric(Counter, 'rpc_retries_total', 'RPC retries', ['method', 'region'])
RPC_PAYLOAD_REJECTED_TOTAL = _metric(Counter, 'rpc_payload_rejected_total', 'Rejected RPC payloads', ['model'])
ROUTER_QUEUE_DEPTH = _metric(Gauge, 'router_queue_depth', 'Router queue depth', ['queue'])
ROUTER_PAIR_QUEUE_DEPTH = _metric(Gauge, 'router_pair_queue_depth', 'Router per-pair queue depth', ['pair', 'tier'])
ROUTER_QUEUE_HIGH_WATERMARK_TOTAL = _metric(Counter, 'router_queue_high_watermark_total', 'Router queue high watermark hits', ['queue'])
ROUTER_QUEUE_DEPTH_BY_EX = _metric(Gauge, 'router_queue_depth_by_ex', 'Router queue depth by exchange/shard', ['exchange', 'queue', 'shard'])
ROUTER_DROPPED_TOTAL = _metric(Counter, 'router_dropped_total', 'Router dropped events', ['queue', 'reason'])
ROUTER_COMBO_SKEW_MS = _metric(Histogram, 'router_combo_skew_ms', 'Router combo skew (ms)', ['route'], buckets=BUCKETS_MS)
ROUTER_TO_SCANNER_MS = _metric(Histogram, 'router_to_scanner_ms', 'Latency Router→Scanner (ms)', ['route'], buckets=BUCKETS_MS)
ROUTER_TO_SCANNER_ERRORS_TOTAL = _metric(Counter, 'router_to_scanner_errors_total', 'Errors Router→Scanner', ['route', 'reason'])

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


SCANNER_DECISION_MS = _metric(Histogram, 'scanner_decision_ms', 'Scanner decision latency (ms)', buckets=BUCKETS_MS)
SCANNER_EVAL_MS = _metric(
    Histogram,
    'scanner_evaluate_ms',
    'Time to evaluate an opportunity',
    ['pair', 'route'],
    buckets=BUCKETS_MS,
)
SCANNER_GLOBAL_LOAD = _metric(Gauge, 'scanner_global_load', 'Scanner global load (0..1)')
SCANNER_RATE_LIMITED_TOTAL = _metric(Counter, 'scanner_rate_limited_total', 'Scanner rate limited hits', ['kind', 'cohort'])
SCANNER_EMITTED_TOTAL = _metric(Counter, 'scanner_emitted_total', 'Opportunities emitted')
SCANNER_REJECTIONS_TOTAL = _metric(Counter, 'scanner_rejections_total', 'Opportunities rejected', ['reason'])
SC_STRATEGY_SCORE = _metric(Gauge, 'sc_strategy_score', 'Strategy score', ['pair', 'route', 'branch'])
SC_ELIGIBLE = _metric(Gauge, 'sc_eligible', 'Pair eligibility flag', ['pair'])
SC_BANNED = _metric(Gauge, 'sc_banned', 'Pair banned flag', ['pair'])
SC_PROMOTED_PRIMARY = _metric(Counter, 'sc_promoted_primary_total', 'Promotions to PRIMARY', ['pair'])
SC_ROTATION_PRIMARY_SIZE = _metric(Gauge, 'sc_rotation_primary_size', 'Rotation PRIMARY size')
SC_ROTATION_AUDITION_SIZE = _metric(Gauge, 'sc_rotation_audition_size', 'Rotation AUDITION size')
RM_DECISION_MS = _metric(Histogram, 'rm_decision_ms', 'RiskManager decision latency (ms)', buckets=BUCKETS_MS)
RM_REVALIDATE_MS = _metric(Histogram, 'rm_revalidate_ms', 'revalidate_arbitrage duration (ms)')
RM_FRAGMENT_PROFIT_MS = _metric(Histogram, 'rm_fragment_profit_ms', 'is_fragment_profitable duration (ms)')
RM_PREFLIGHT_MS = _metric(Histogram, 'rm_preflight_ms', 'Pre-flight gate duration (ms)')
RM_DECISIONS_TOTAL = _metric(Counter, 'rm_decisions_total', 'RM decisions', ['status'])
RM_SKIPS_TOTAL = _metric(Counter, 'rm_skips_total', 'RM skipped reasons', ['reason'])
RM_QUEUE_DEPTH = _metric(Gauge, 'rm_queue_depth', 'Queue depth per stage', ['stage'])
RM_FINAL_DECISIONS_TOTAL = _metric(Counter, 'rm_final_decisions_total', 'Final decisions emitted by RM, per route', ['route'])
RM_ADMITTED_TOTAL = _metric(Counter, 'rm_admitted_total', 'Opportunities admitted by the RM', labelnames=('cohort',))
RM_DROPPED_TOTAL = _metric(Counter, 'rm_dropped_total', 'Opportunities dropped by the RM', labelnames=('reason',))
STALE_OPPORTUNITY_DROPPED_TOTAL = _metric(Counter, 'stale_opportunity_dropped_total', 'Opportunities dropped due to stale/invalid orderbooks')
PAIR_HEALTH_PENALTY_TOTAL = _metric(Counter, 'pair_health_penalty_total', 'Pair-level penalties applied (circuit-breakers)', labelnames=('pair', 'reason'))
POOL_GATE_THROTTLES_TOTAL = _metric(Counter, 'pool_gate_throttles_total', 'Pool gate throttles fired (insufficient quote buffer)', labelnames=('quote',))

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
        enriched = dict(labels) if labels else {}
        # On force la présence d'un reason dans les labels pour inc_blocked
        enriched.setdefault("reason", reason)
        mark_scanner_to_rm(ok, dt_ms, **enriched)
    except Exception:
        # Best-effort
        pass

INVENTORY_USD = _metric(Gauge, 'inventory_usd', 'Inventory in USD', ['exchange', 'alias'])
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
        LAST_BALANCES_FRESH_TS.labels(lbl_exchange(exchange), _norm(alias)).set(time.time())
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
    except Exception:
        pass

def mark_rm_to_engine(ok: bool, dt_ms: float, **labels: Any) -> None:
    try:
        RM_DECISION_MS.observe(max(0.0, float(dt_ms)))
        if not ok:
            inc_blocked('rm_to_engine', labels.get('reason', 'unknown'), labels.get('pair'))
    except Exception:
        pass
MM_FILLS_BOTH = _metric(Counter, 'mm_fills_both_total', 'Both maker orders filled (MM) before TTL; no hedge needed', ['pair'])
MM_SINGLE_FILL_HEDGED = _metric(Counter, 'mm_single_fill_hedged_total', 'Single maker order filled (MM) then hedged with a taker', ['pair'])
MM_PANIC_HEDGE_TOTAL = _metric(Counter, 'mm_panic_hedge_total', 'Panic hedge triggered due to exception/timeout during MM', ['pair'])
ENGINE_SUBMIT_TO_ACK_MS = _metric(Histogram, 'engine_submit_to_ack_ms', 'Engine submit→ack latency (ms)', buckets=BUCKETS_MS)
ENGINE_ACK_TO_FILL_MS = _metric(Histogram, 'engine_ack_to_fill_ms', 'Engine ack→fill latency (ms)', buckets=BUCKETS_MS)
ENGINE_CANCELLATIONS_TOTAL = _metric(Counter, 'engine_cancellations_total', 'Engine cancellations', ['exchange', 'pair', 'reason'])
ENGINE_RETRIES_TOTAL = _metric(Counter, 'engine_retries_total', 'Engine retries', ['exchange', 'pair', 'reason'])
ENGINE_QUEUEPOS_BLOCKED_TOTAL = _metric(Counter, 'engine_queuepos_blocked_total', 'Engine TM queuepos blocked', ['exchange', 'pair'])
ENGINE_SUBMIT_QUEUE_DEPTH = _metric(Gauge, 'engine_submit_queue_depth', 'Engine submit queue depth')
INFLIGHT_GAUGE = _metric(Gauge, 'engine_inflight', 'Engine inflight orders')
PNL_LIVE_DAY_USD = _metric(Gauge, 'pnl_live_day_usd', 'Live PnL for the current local day (USD)', ['region', 'branch', 'mode'])
TRADES_LIVE_DAY_TOTAL = _metric(Counter, 'trades_live_day_total', 'Trades live day total', ['result'])
DERIVED_NET_PROFIT_SIGN_TOTAL = _metric(Counter, 'derived_net_profit_sign_total', 'Derived net profit sign (fallback)', ['reason'])
MISSING_NET_PROFIT_TOTAL = _metric(Counter, 'missing_net_profit_total', 'Missing net profit values', ['stage'])
ENGINE_PACER_DELAY_MS = _metric(Gauge, 'engine_pacer_delay_ms', 'Engine pacer delay (ms)')
ENGINE_PACER_INFLIGHT_MAX = _metric(Gauge, 'engine_pacer_inflight_max', 'Engine pacer inflight cap')
ENGINE_PACER_MODE = _metric(Gauge, 'engine_pacer_mode', 'Engine pacer mode (0=NORMAL,1=CONSTRAINED,2=SEVERE)')
ENGINE_DRAIN_LATENCY_MS = _metric(Histogram, 'engine_drain_latency_ms', 'Engine drain latency (ms)', buckets=BUCKETS_MS)
ENGINE_PACING_BACKPRESSURE_TOTAL = _metric(Counter, 'engine_pacing_backpressure_total', 'Engine pacing backpressure', ['reason'])
ENGINE_ACK_TIMEOUT_TOTAL = _metric(Counter, 'engine_ack_timeout_total', 'Engine ack timeouts')


# === OBS READINESS (Lot B) — strict + stubs + métriques Lot B ===
import os, logging

# Mode strict (bruit contrôlé en prod)
STRICT_OBS = int(os.getenv("STRICT_OBS", "0"))

# Prometheus ou no-op
try:
    from prometheus_client import Counter, Gauge, Histogram
    _PROM_READY = True
except Exception:
    _PROM_READY = False
    class _NoopMetric:
        def labels(self, **kw): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def observe(self, *a, **k): pass
    Counter = Gauge = Histogram = _NoopMetric  # type: ignore

def prom_ready() -> bool:
    return bool(_PROM_READY)

# --- Wrappers sûrs pour éviter les try/except:pass dans les modules ---
if "OBS_NOOP_TOTAL" not in globals():
    OBS_NOOP_TOTAL = Counter("obs_noop_total", "Appels metrics no-op (init manquante, labels, etc.)", ["metric","where"])
if "OBS_INIT_ERROR_TOTAL" not in globals():
    OBS_INIT_ERROR_TOTAL = Counter("obs_init_error_total", "Echecs d'initialisation de métriques", ["metric","kind"])
if "OBS_LABEL_MISMATCH_TOTAL" not in globals():
    OBS_LABEL_MISMATCH_TOTAL = Counter("obs_label_mismatch_total", "Labels fournis non conformes", ["metric"])

def _safe_labels(metric, name: str, where: str, **labels):
    try:
        return metric.labels(**labels)
    except Exception:
        OBS_LABEL_MISMATCH_TOTAL.labels(metric=name).inc()
        OBS_NOOP_TOTAL.labels(metric=name, where=where).inc()
        return metric

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

# === Router (public) ===
if "ROUTER_DROPPED_TOTAL" not in globals():
    ROUTER_DROPPED_TOTAL = Counter(
        "router_dropped_total", "Evénements rejetés côté Router", ["reason","topic"]
    )
if "ROUTER_TO_SCANNER_MS" not in globals():
    ROUTER_TO_SCANNER_MS = Histogram(
        "router_to_scanner_ms", "Latency Router→Scanner (ms)", ["topic"]
    )
if "ROUTER_TO_SCANNER_ERRORS_TOTAL" not in globals():
    ROUTER_TO_SCANNER_ERRORS_TOTAL = Counter(
        "router_to_scanner_errors_total", "Erreurs Router→Scanner", ["reason","topic"]
    )
if "ROUTER_ROUTE_MUTED_TOTAL" not in globals():
    ROUTER_ROUTE_MUTED_TOTAL = Counter(
        "router_route_muted_total", "Routes mises en mute par le Router", ["reason","exchange","pair"]
    )

# === WS publics ===
if "WS_RECONNECTS_TOTAL" not in globals():
    WS_RECONNECTS_TOTAL = Counter(
        "ws_reconnects_total", "Reconnects WS publics", ["exchange","reason"]
    )
if "WS_BACKOFF_SECONDS" not in globals():
    WS_BACKOFF_SECONDS = Gauge(
        "ws_backoff_seconds", "Backoff courant (secondes) WS publics", ["exchange"]
    )
if "WS_CONNECTIONS_OPEN" not in globals():
    WS_CONNECTIONS_OPEN = Gauge(
        "ws_connections_open", "Connexions WS publiques ouvertes", ["exchange"]
    )
if "WS_PUBLIC_DROPPED_TOTAL" not in globals():
    WS_PUBLIC_DROPPED_TOTAL = Counter(
        "ws_public_dropped_total", "Evénements WS publics rejetés", ["exchange", "reason"]
    )

# === WS publics v2 (exchange / region / deployment_mode) ===
if "WS_PUBLIC_EVENTS_TOTAL_V2" not in globals():
    WS_PUBLIC_EVENTS_TOTAL_V2 = Counter(
        "ws_public_events_total_v2",
        "Evénements WS publics reçus (v2, taggés par exchange/region/deployment_mode/stream)",
        ["exchange", "region", "deployment_mode", "stream"],
    )

if "WS_PUBLIC_ERRORS_TOTAL_V2" not in globals():
    WS_PUBLIC_ERRORS_TOTAL_V2 = Counter(
        "ws_public_errors_total_v2",
        "Erreurs WS publics (v2, taggées par exchange/region/deployment_mode/raison)",
        ["exchange", "region", "deployment_mode", "reason"],
    )

if "WS_PUBLIC_RECONNECTS_TOTAL_V2" not in globals():
    WS_PUBLIC_RECONNECTS_TOTAL_V2 = Counter(
        "ws_public_reconnects_total_v2",
        "Reconnects WS publics (v2, taggées par exchange/region/deployment_mode/raison)",
        ["exchange", "region", "deployment_mode", "reason"],
    )

if "WS_PUBLIC_BACKOFF_SECONDS_V2" not in globals():
    WS_PUBLIC_BACKOFF_SECONDS_V2 = Gauge(
        "ws_public_backoff_seconds_v2",
        "Backoff courant (secondes) WS publics (v2)",
        ["exchange", "region", "deployment_mode"],
    )

if "WS_PUBLIC_CONNECTIONS_OPEN_V2" not in globals():
    WS_PUBLIC_CONNECTIONS_OPEN_V2 = Gauge(
        "ws_public_connections_open_v2",
        "Connexions WS publiques ouvertes (v2)",
        ["exchange", "region", "deployment_mode"],
    )

if "WS_PUBLIC_DROPPED_TOTAL_V2" not in globals():
    WS_PUBLIC_DROPPED_TOTAL_V2 = Counter(
        "ws_public_dropped_total_v2",
        "Evénements WS publics rejetés (v2)",
        ["exchange", "region", "deployment_mode", "reason"],
    )

if "WS_PUBLIC_STALENESS_SECONDS" not in globals():
    WS_PUBLIC_STALENESS_SECONDS = Gauge(
        "ws_public_staleness_seconds",
        "Staleness estimée des flux WS publics (secondes, v2)",
        ["exchange", "region", "deployment_mode"],
    )


# === Hub WS privés (PWS) ===
if "PWS_DROPPED_TOTAL" not in globals():
    PWS_DROPPED_TOTAL = Counter(
        "pws_dropped_total", "Evénements PWS rejetés", ["exchange","alias","reason"]
    )
if "PWS_RECONNECTS_TOTAL" not in globals():
    PWS_RECONNECTS_TOTAL = Counter(
        "pws_reconnects_total", "Reconnects PWS", ["exchange","alias"]
    )
if "PWS_BACKOFF_SECONDS" not in globals():
    PWS_BACKOFF_SECONDS = Gauge(
        "pws_backoff_seconds", "Backoff courant (secondes) PWS", ["exchange","alias"]
    )
if "PWS_ALERT_TOTAL" not in globals():
    PWS_ALERT_TOTAL = Counter(
        "pws_alert_total",
        "Alertes PWS (heartbeat/ordre/lag…)",
        ["severity", "reason", "exchange", "alias", "kind"],
    )
if "PWS_EVENT_LAG_MS" not in globals():
    PWS_EVENT_LAG_MS = Histogram(
        "pws_event_lag_ms", "Lag événement PWS (ms)", ["exchange","alias"]
    )
if "PWS_QUEUE_FILL_RATIO" not in globals():
    PWS_QUEUE_FILL_RATIO = Gauge(
        "pws_queue_fill_ratio",
        "Remplissage des queues internes PWS (0..1)",
        ["exchange", "alias", "kind"],
    )
if "PWS_QUEUE_SATURATION_TOTAL" not in globals():
    PWS_QUEUE_SATURATION_TOTAL = Counter(
        "pws_queue_saturation_total",
        "Saturations de queues PWS",
        ["exchange", "alias", "kind"],
    )
if "PWS_HEARTBEAT_GAP_BREACH_TOTAL" not in globals():
    PWS_HEARTBEAT_GAP_BREACH_TOTAL = Counter(
        "pws_heartbeat_gap_breach_total",
        "Nombre de fois où le gap heartbeat hub a dépassé le seuil",
        ["exchange", "alias"],
    )

# === Déjà utilisés ailleurs (cohérence Lot A & B) ===
if "SCANNER_HINT_TOPQTY_MISSING_TOTAL" not in globals():
    SCANNER_HINT_TOPQTY_MISSING_TOTAL = Counter(
        "scanner_hint_topqty_missing_total", "Hints L1 (top_qty) manquants", ["pair","ex","side"]
    )
if "LOGGERH_FILE_ROTATIONS_TOTAL" not in globals():
    LOGGERH_FILE_ROTATIONS_TOTAL = Counter(
        "loggerh_file_rotations_total", "Rotations LoggerHistorique", ["reason"]
    )
# Engine/AC stubs si absents
if "AC_KV_ERRORS_TOTAL" not in globals():
    AC_KV_ERRORS_TOTAL = Counter("ac_kv_errors_total", "Erreurs KV (anti-crossing)", ["kind"])
if "AC_RESERVE_CONFLICT_TOTAL" not in globals():
    AC_RESERVE_CONFLICT_TOTAL = Counter("ac_reserve_conflict_total", "Conflits de réservation AC", ["branch","ex"])
if "AC_RELEASE_ERRORS_TOTAL" not in globals():
    AC_RELEASE_ERRORS_TOTAL = Counter("ac_release_errors_total", "Erreurs release AC", ["kind"])
if "ENGINE_WORKER_ERRORS_TOTAL" not in globals():
    ENGINE_WORKER_ERRORS_TOTAL = Counter("engine_worker_errors_total", "Erreurs worker Engine", ["phase"])
if "ENGINE_INFLIGHT_GAUGE_SET_ERRORS_TOTAL" not in globals():
    ENGINE_INFLIGHT_GAUGE_SET_ERRORS_TOTAL = Counter(
        "engine_inflight_gauge_set_errors_total", "Echecs INFLIGHT_GAUGE.set", ["exchange"]
    )
if "ENGINE_BEST_PRICE_MISSING_TOTAL" not in globals():
    ENGINE_BEST_PRICE_MISSING_TOTAL = Counter(
        "engine_best_price_missing_total", "Best price manquant côté Engine", ["exchange","pair","side"]
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
PWS_EVENT_LAG_MS = _metric(Histogram, 'pws_event_lag_ms', 'PrivateWS event lag (ms)', ['exchange'], buckets=BUCKETS_MS)
PWS_TRANSFERS_TOTAL = _metric(Counter, 'pws_transfers_total', 'PrivateWS internal transfers', ['exchange'])
PWS_EVENTS_TOTAL = _metric(Counter, 'pws_events_total', 'PrivateWS events received', ['exchange', 'kind'])
PWS_BACKOFF_SECONDS = _metric(Gauge, 'pws_backoff_seconds', 'PrivateWS backoff seconds', ['exchange'])
PWS_HEARTBEAT_GAP_SECONDS = _metric(Gauge, 'pws_heartbeat_gap_seconds', 'PrivateWS heartbeat gap seconds', ['exchange'])
PWS_DROPPED_TOTAL = _metric(Counter, 'pws_dropped_total', 'PrivateWS dropped events', ['exchange', 'reason'])
PWS_ACK_LATENCY_MS = _metric(Histogram, 'pws_ack_latency_ms', 'Ack latency from private WS (ms)', ['exchange'], buckets=BUCKETS_MS)
PWS_FILL_LATENCY_MS = _metric(Histogram, 'pws_fill_latency_ms', 'Fill latency from private WS (ms)', ['exchange'], buckets=BUCKETS_MS)
WS_FAILOVER_TOTAL = _metric(Counter, 'ws_failover_total', 'PrivateWS failovers', ['exchange', 'reason'])
PWS_POOL_SIZE = _metric(Gauge, 'pws_pool_size', 'Private WS connection pool size', ['exchange'])
PWS_QUEUE_DEPTH = _metric(Gauge, 'pws_queue_depth', 'PrivateWS submit queue depth', ['exchange', 'alias', 'kind'])
PWS_QUEUE_CAP = _metric(Gauge, 'pws_queue_cap', 'PrivateWS submit queue capacity', ['exchange', 'alias', 'kind'])
WS_RECO_RUN_MS = _metric(Histogram, 'ws_reco_run_ms', 'Private WS reconciler run duration (ms)', buckets=BUCKETS_MS)
WS_RECO_ERRORS_TOTAL = _metric(Counter, 'ws_reco_errors_total', 'Errors in private WS reconciler', ['exchange'])
WS_RECO_MISS_PER_MINUTE = _metric(
    Gauge,
    'ws_reco_miss_per_minute',
    'Miss détectés par minute (fenêtre glissante ~60s)',
    ['exchange', 'alias'],
)
WS_RECO_MISS_BURST_TOTAL = _metric(
    Counter,
    'ws_reco_miss_burst_total',
    'Bursts de miss > seuil par minute',
    ['exchange', 'alias'],
)
RECONCILE_MISS_TOTAL = _metric(Counter, 'reconcile_miss_total', 'Reconciler misses', ['exchange', 'kind'])
RECONCILE_RESYNC_TOTAL = _metric(Counter, 'reconcile_resync_total', 'Resyncs requested by reconciler', ['exchange', 'reason'])
RECONCILE_RESYNC_FAILED_TOTAL = _metric(
    Counter,
    'reconcile_resync_failed_total',
    'Failed resync attempts triggered by reconciler',
    ['exchange', 'reason'],
)
RECONCILE_RESYNC_LATENCY_MS = _metric(Histogram, 'reconcile_resync_latency_ms', 'Resync rebuild latency (ms)', ['exchange'], buckets=BUCKETS_MS)
COLD_RESYNC_TOTAL = _metric(Counter, 'cold_resync_total', 'Cold resyncs', ['exchange'])
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

def recon_on_resync(exchange: str, reason: str='unknown') -> None:
    try:
        RECONCILE_RESYNC_TOTAL.labels(lbl_exchange(exchange), _norm(reason)).inc()
    except Exception:
        pass

def recon_observe_latency(exchange: str, dt_ms: float) -> None:
    try:
        RECONCILE_RESYNC_LATENCY_MS.labels(lbl_exchange(exchange)).observe(max(0.0, float(dt_ms)))
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
LHM_JSONL_QUEUE_CAP = _metric(Gauge, 'lhm_jsonl_queue_cap', 'Configured JSONL queue capacity (records)')

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

    def __init__(self, host: str='0.0.0.0', port: int=9110, registry=None, include_metrics: bool=False):
        self.host = host
        self.port = int(port)
        self.registry = registry or REGISTRY
        self.include_metrics = bool(include_metrics)
        self._boot: Any = None
        self._runner: Optional['web.AppRunner'] = None
        self._site: Optional['web.TCPSite'] = None

    def set_boot(self, boot: Any) -> None:
        self._boot = boot

    async def _handle_status(self, request):
        st = _extract_status_from_boot(self._boot)
        return web.json_response(st, status=200)

    async def _handle_ready(self, request):
        st = _extract_status_from_boot(self._boot)
        ok = bool(st.get('ready_all')) and (not bool(st.get('degraded')))
        code = 200 if ok else 503
        return web.json_response(st, status=code)

    async def _handle_metrics(self, request):
        if not self.include_metrics or self.registry is None:
            return web.Response(text='# metrics disabled on this endpoint\n', status=200)
        try:
            output = generate_latest(self.registry)
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
        app.add_routes([web.get('/ready', self._handle_ready), web.get('/status', self._handle_status), web.get('/healthz', self._handle_ready)])
        if self.include_metrics:
            app.add_routes([web.get('/metrics', self._handle_metrics)])
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, host=self.host, port=self.port)
        await self._site.start()
        log.info('StatusHTTPServer started on http://%s:%d (ready/status%s)', self.host, self.port, '/metrics' if self.include_metrics else '')

    async def stop(self) -> None:
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

def start_servers_from_env(boot=None) -> dict[str, object]:
    """
    Démarre StatusHTTPServer(9110) avec /metrics activable par env,
    et ObsServer(9108) optionnel. Retourne {"status_server": .., "obs_server": ..}.

    ENV:
      - STATUS_PORT (def 9110)
      - EXPOSE_METRICS_ON_9110 (def 1 → expose /metrics sur 9110)
      - OBS_ENABLE_9108 (def 0 → pas d’ObsServer séparé)
      - OBS_PORT (def 9108)
    """
    servers: dict[str, object] = {}
    try:
        status_port = int(_os.getenv('STATUS_PORT', '9110'))
        expose_9110 = _env_bool('EXPOSE_METRICS_ON_9110', True)
        st = StatusHTTPServer(port=status_port, include_metrics=expose_9110)
        if boot is not None:
            st.set_boot(boot)
        servers['status_server'] = st
    except Exception:
        log.exception('start_servers_from_env: StatusHTTPServer init a échoué')
    try:
        if _env_bool('OBS_ENABLE_9108', False):
            obs_port = int(_os.getenv('OBS_PORT', '9108'))
            servers['obs_server'] = ObsServer(port=obs_port)
    except Exception:
        log.exception('start_servers_from_env: ObsServer init a échoué')
    return servers

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


def ws_public_on_frame(exchange: str) -> None:
    """Compatibilité v1: utilisé par d'anciens modules.

    Par défaut, taggue l'évènement avec region/deployment_mode = "UNKNOWN".
    Les nouveaux appels devraient passer par ws_public_event().
    """
    try:
        ws_public_event(exchange, region="UNKNOWN", deployment_mode="UNKNOWN", stream="legacy")
    except Exception:  # pragma: no cover
        _obs_shim_log.debug("ws_public_on_frame(%s) [legacy]", exchange)


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


def ws_public_set_staleness(exchange: str, seconds: float) -> None:
    """Compatibilité v1 pour la staleness WS publics."""
    try:
        ws_public_staleness(
            exchange,
            region="UNKNOWN",
            deployment_mode="UNKNOWN",
            seconds=seconds,
        )
    except Exception:  # pragma: no cover
        _obs_shim_log.debug(
            "ws_public_set_staleness(%s, %.3f) [legacy]", exchange, seconds
        )


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


def feesync_on_refresh(ok: bool) -> None:
    _obs_shim_log.debug('feesync_on_refresh(%s) [legacy no-op]', ok)


def feesync_on_apply(exchange: str, alias: str) -> None:
    _obs_shim_log.debug('feesync_on_apply(%s,%s) [legacy no-op]', exchange, alias)


def feesync_on_error() -> None:
    _obs_shim_log.debug('feesync_on_error() [legacy no-op]')


def discovery_on_run(enabled_exchanges: list[str], pairs_per_exchange: dict[str, int]) -> None:
    _obs_shim_log.debug('discovery_on_run(%d exchanges) [legacy no-op]', len(enabled_exchanges or []))


def private_on_poller_tick(exchange: str) -> None:
    _obs_shim_log.debug('private_on_poller_tick(%s) [legacy no-op]', exchange)


def private_on_event(exchange: str, typ: str) -> None:
    _obs_shim_log.debug('private_on_event(%s,%s) [legacy no-op]', exchange, typ)


def snapshot_inventory(balances: dict[str, dict[str, float]], mids: dict[str, float]) -> None:
    _obs_shim_log.debug('snapshot_inventory(...) [legacy no-op]')


def bump_scanner(spread_pct: float | None, active_pairs: int | None) -> None:
    _obs_shim_log.debug('bump_scanner(spread=%s, active=%s) [legacy no-op]', spread_pct, active_pairs)


def set_tm_open_makers(n: int) -> None:
    _obs_shim_log.debug('set_tm_open_makers(%d) [legacy no-op]', n)


def tm_on_maker_placed(exchange: str, symbol: str, side: str) -> None:
    _obs_shim_log.debug('tm_on_maker_placed(%s,%s,%s) [legacy no-op]', exchange, symbol, side)


def tm_on_maker_canceled(exchange: str, symbol: str) -> None:
    _obs_shim_log.debug('tm_on_maker_canceled(%s,%s) [legacy no-op]', exchange, symbol)


def tm_on_hedge_sent(exchange: str, symbol: str, side: str, lag_seconds: float | None = None) -> None:
    _obs_shim_log.debug('tm_on_hedge_sent(%s,%s,%s,lag=%s) [legacy no-op]', exchange, symbol, side, lag_seconds)


def tm_on_maker_fill_ratio(ratio: float) -> None:
    _obs_shim_log.debug('tm_on_maker_fill_ratio(%.3f) [legacy no-op]', ratio)


def mm_on_opp(pair: str) -> None:
    _obs_shim_log.debug('mm_on_opp(%s) [legacy no-op]', pair)


def mm_on_both_filled(pair: str) -> None:
    _obs_shim_log.debug('mm_on_both_filled(%s) [legacy no-op]', pair)


def mm_on_single_fill_hedged(pair: str) -> None:
    _obs_shim_log.debug('mm_on_single_fill_hedged(%s) [legacy no-op]', pair)


def mm_on_panic_hedge(pair: str) -> None:
    _obs_shim_log.debug('mm_on_panic_hedge(%s) [legacy no-op]', pair)


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


def inc_scanner_rejection(reason: str, route: str = 'n/a', pair: str = 'n/a') -> None:
    try:
        SCANNER_REJECTIONS_TOTAL.labels(str(reason)).inc()
    except Exception:
        _obs_shim_log.exception('inc_scanner_rejection failed')


def inc_scanner_emitted(route: str = 'n/a', pair: str = 'n/a') -> None:
    try:
        SCANNER_EMITTED_TOTAL.inc()
    except Exception:
        _obs_shim_log.exception('inc_scanner_emitted failed')


def observe_scanner_latency(route: str, seconds: float) -> None:
    try:
        ROUTER_TO_SCANNER_MS.labels(str(route)).observe(max(0.0, float(seconds) * 1000.0))
    except Exception:
        _obs_shim_log.exception('observe_scanner_latency failed')


def set_engine_running(flag: bool) -> None:
    _obs_shim_log.debug('set_engine_running(%s) [legacy no-op]', flag)



__all__ = ['BUCKETS_MS',"LAT_ACK_MS", "LAT_FILL_FIRST_MS", "LAT_FILL_ALL_MS", "LAT_E2E_MS","LOGGERH_FILE_ROTATIONS_TOTAL",
    "LAT_EVENTS_TOTAL", "LAT_PIPELINE_EVENTS_TOTAL","OBS_READY", "obs_is_ready",
    "PAIR_HISTORY_ROWS_TOTAL", "PAIR_HISTORY_COMPUTE_MS", 'set_region', 'set_deployment_mode', 'lbl_exchange', 'lbl_region', 'lbl_mode', 'start_time_skew_probe', 'start_loop_lag_probe', 'update_time_skew', 'TIME_SKEW_MS', 'TIME_SKEW_STATUS', 'EVENT_LOOP_LAG_MS', 'report_nonfatal', 'inc_blocked', 'NONFATAL_ERRORS_TOTAL', 'BLOCKED_TOTAL', 'BF_API_ERRORS_TOTAL', 'BF_API_LATENCY_MS', 'BF_CACHE_AGE_SECONDS', 'BF_LAST_SUCCESS_TS', 'FEE_TOKEN_BALANCE', 'mark_bf_latency', 'RPC_LATENCY_MS', 'RPC_ERR_TOTAL', 'RPC_RETRIES_TOTAL', 'RPC_PAYLOAD_REJECTED_TOTAL', 'ROUTER_QUEUE_DEPTH', 'ROUTER_PAIR_QUEUE_DEPTH', 'ROUTER_QUEUE_HIGH_WATERMARK_TOTAL', 'ROUTER_QUEUE_DEPTH_BY_EX', 'ROUTER_DROPPED_TOTAL', 'ROUTER_COMBO_SKEW_MS', 'ROUTER_TO_SCANNER_MS', 'ROUTER_TO_SCANNER_ERRORS_TOTAL', 'mark_router_to_scanner', 'SCANNER_DECISION_MS', 'SCANNER_GLOBAL_LOAD', 'SCANNER_RATE_LIMITED_TOTAL', 'SCANNER_EMITTED_TOTAL', 'SCANNER_REJECTIONS_TOTAL', 'SC_STRATEGY_SCORE', 'SC_ELIGIBLE', 'SC_BANNED', 'SC_PROMOTED_PRIMARY', 'SC_ROTATION_PRIMARY_SIZE', 'SC_ROTATION_AUDITION_SIZE', 'RM_DECISION_MS', 'mark_scanner_to_rm', 'INVENTORY_USD', 'RM_REJECT_TOTAL', 'PAIR_HEALTH_PENALTY_TOTAL', 'VOL_EWMA_BPS', 'VOL_P95_BPS', 'FEE_MISMATCH_TOTAL', 'FEES_EXPECTED_BPS', 'FEES_REALIZED_BPS', 'FEESYNC_LAST_TS', 'FEESYNC_ERRORS', 'REBAL_DETECTED_TOTAL', 'REBAL_PLAN_QUANTUM_QUOTE', 'RM_PAUSED_COUNT', 'LAST_BOOKS_FRESH_TS', 'LAST_BALANCES_FRESH_TS', 'DYNAMIC_MIN_BPS', 'mark_books_fresh', 'mark_balances_fresh', 'set_rm_paused_count', 'set_dynamic_min', 'inc_rm_reject', 'mark_rm_to_engine', 'MM_FILLS_BOTH', 'MM_SINGLE_FILL_HEDGED', 'MM_PANIC_HEDGE_TOTAL', 'ENGINE_SUBMIT_TO_ACK_MS', 'ENGINE_ACK_TO_FILL_MS', 'ENGINE_CANCELLATIONS_TOTAL', 'ENGINE_RETRIES_TOTAL', 'ENGINE_QUEUEPOS_BLOCKED_TOTAL', 'ENGINE_SUBMIT_QUEUE_DEPTH', 'INFLIGHT_GAUGE', 'PNL_LIVE_DAY_USD', 'TRADES_LIVE_DAY_TOTAL', 'DERIVED_NET_PROFIT_SIGN_TOTAL', 'MISSING_NET_PROFIT_TOTAL', 'ENGINE_PACER_DELAY_MS', 'ENGINE_PACER_INFLIGHT_MAX', 'ENGINE_PACER_MODE', 'ENGINE_DRAIN_LATENCY_MS', 'ENGINE_PACING_BACKPRESSURE_TOTAL', 'inc_engine_pacing_backpressure', 'WS_RECONNECTS_TOTAL', 'WS_BACKOFF_SECONDS', 'WS_CONNECTIONS_OPEN', 'PACER_STATE', 'PACER_CLAMP_SECONDS', 'ENGINE_MUTE_TOTAL', 'FEE_TOKEN_LEVEL', 'FEE_TOKEN_TARGET_PERCENT', 'PWS_DEDUP_HITS_TOTAL', 'PWS_RECONNECTS_TOTAL', 'PWS_EVENT_LAG_MS', 'PWS_TRANSFERS_TOTAL', 'PWS_EVENTS_TOTAL', 'PWS_BACKOFF_SECONDS', 'PWS_HEARTBEAT_GAP_SECONDS', 'PWS_DROPPED_TOTAL', 'PWS_ACK_LATENCY_MS', 'PWS_FILL_LATENCY_MS', 'WS_FAILOVER_TOTAL', 'PWS_POOL_SIZE', 'PWS_QUEUE_DEPTH', 'PWS_QUEUE_CAP', 'WS_RECO_RUN_MS', 'WS_RECO_ERRORS_TOTAL', 'RECONCILE_MISS_TOTAL', 'RECONCILE_RESYNC_TOTAL', 'RECONCILE_RESYNC_LATENCY_MS', 'COLD_RESYNC_TOTAL', 'COLD_RESYNC_RUN_MS', 'recon_run_ms', 'recon_error', 'recon_on_resync', 'recon_observe_latency', 'pws_on_failover', 'pws_set_pool_size', 'LOGGERH_WRITE_MS', 'LOGGERH_QUEUE_PLATEAU_TOTAL', 'LHM_JSONL_INGESTED_TOTAL', 'LHM_JSONL_DROPPED_TOTAL', 'LHM_JSONL_QUEUE_SIZE', 'LOGGERH_TRADE_QUEUE_SIZE', 'LOGGERH_JSONL_ROTATIONS_TOTAL', 'LOGGERH_LAST_FLUSH_TS_SECONDS', 'LOGGERH_LAST_ROTATION_TS_SECONDS', 'loggerh_observe_write_ms', 'lhm_on_ingested', 'lhm_on_dropped', 'lhm_set_queue_size', 'lhm_on_rotation', 'loggerh_set_last_flush_now', 'loggerh_set_last_rotation_now', 'STORAGE_USAGE_PCT', 'STORAGE_BYTES_FREE', 'STORAGE_ALERTS_TOTAL', 'LOGGERH_JSONL_BYTES', 'LOGGERH_DB_STALLS_TOTAL', 'LOGGERH_DB_FILE_BYTES', 'update_storage_metrics',  'SIM_DECISION_MS', 'SIMULATED_VWAP_DEVIATION_BPS', 'sim_on_run', 'PAYLOAD_REJECTED_TOTAL', 'ObsServer', 'StatusHTTPServer', 'MainMetrics', 'BOT_STARTUPS_TOTAL', 'BOT_STATE', 'start_servers_from_env', 'WS_RECONNECTS_TOTAL', 'RM_DECISION_MS', 'RM_PREFLIGHT_MS', 'RM_DECISIONS_TOTAL', 'RM_SKIPS_TOTAL', 'RM_QUEUE_DEPTH', 'RM_REVALIDATE_MS', 'RM_FRAGMENT_PROFIT_MS', 'PAIR_HEALTH_PENALTY_TOTAL', 'POOL_GATE_THROTTLES_TOTAL', 'RM_FINAL_DECISIONS_TOTAL', 'RM_ADMITTED_TOTAL', 'RM_DROPPED_TOTAL',
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