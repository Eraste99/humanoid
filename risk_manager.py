# -*- coding: utf-8 -*-
from __future__ import annotations



"""
    Chef d’orchestre strict — **multi-comptes (TT/TM) par CEX**

    - Seul module autorisé à dialoguer avec :
        * MarketDataRouter (orderbooks via callback)
        * BalanceFetcher (ou soldes virtuels en dry-run)
        * Simulator (ajustements d'exécution + plan de fragmentation)
        * Système d’alerting (callback)
    - Sous-modules PASSIFS gérés par le RiskManager :
        * VolatilityManager, SlippageAndFeesCollector, RebalancingManager
    - Boucles internes :
        * _loop_orderbooks / _loop_balances / _loop_rebalancing / _loop_volatility / _loop_fee_sync

    Mises à niveau clés :
        • **Multi-comptes**: snapshots de soldes structurés par *exchange -> alias -> assets*.
        • **Rebalancing intra-CEX** (alias→alias) & **cross-CEX** (CEX→CEX).
        • **Validation inventaire** “alias-aware”.
        • **Owner économique**: seul module autorisé à décider GO/NO-GO basé sur `net_bps` / `min_required_bps`
          pour les arbitrages TT/TM/REB. L’ExecutionEngine ne peut refuser qu’en raison de contraintes
          **techniques** (book, queuepos, timeouts, 429, etc.).
        • **Seuil dynamique** min_required_bps basé sur la vol (télémétrie set_dynamic_min).
        • **Fast-path** (spread, frais+slip, fraîcheur OB) et modes TT/TM.
        • **TM**: helpers maker/taker, profondeur, bascule NON_NEUTRAL.
        • **Simu dual parallèle** si supportée: `simulate_both_parallel()`.
        • **Verrou d’exécution** par (exchange, alias, pair) côté Engine.
        • **Pipeline transferts internes** (wallet / sous-comptes).

    NOTE: toutes les quantités « volume_usdc » sont interprétées comme **notionnels en devise de cotation**
          (USDC *ou* EUR). Les noms legacy sont conservés pour compat.
    """
import asyncio, time, inspect
import uuid, random
import hashlib, json
import threading
from concurrent.futures import ThreadPoolExecutor
import contextlib
from modules.bot_config import RmSwitchKnobs
from dataclasses import fields
from typing import Dict, Any, List, Optional, Tuple
from modules.obs_metrics import (
    inc_blocked,
    set_transfer_inflight,
    inc_transfer_fsm_event,
    RM_RESERVED_FUNDS_ACTIVE,
    record_pipeline_latency,
    set_pipeline_backlog,
    should_trace_latency,
)
import logging
from collections import defaultdict, deque
from modules.obs_metrics import TIME_SKEW_MS
from contracts.payloads import (
    make_submit_bundle,
    submit_leg_from_intent,
    normalize_leg_dict,
    normalize_reason_code,
    ReasonCodes,
    canonical_transfer_id,
)
from modules.retry_policy import BackoffPolicy, awith_retry


from dataclasses import dataclass, asdict
import json
from contracts import payloads as fraglib
from modules.dynamic_execution_simulator import FragmentationPolicy

from typing import Any, Dict, List, Optional, Callable, Tuple, Set, Iterable
from types import SimpleNamespace
import math, random
import unittest

# RM_* : décisions métier / risque prises par le RiskManager.
# ENGINE_* : rejets techniques ou incapacité du moteur / CEX (backpressure, erreurs réseau, etc.).
RM_STALE_VOL = "RM_STALE_VOL"
RM_SFC_UNAVAILABLE = "RM_SFC_UNAVAILABLE"
RM_COST_COMPUTE_ERROR = "RM_COST_COMPUTE_ERROR"
RM_BELOW_MIN_BPS = "RM_BELOW_MIN_BPS"
RM_BELOW_MIN_NOTIONAL = "RM_BELOW_MIN_NOTIONAL"
RM_BALANCE_TTL_BLOCK = "RM_BALANCE_TTL_BLOCK"
RM_ENGINE_NOT_READY = "RM_ENGINE_NOT_READY"

RM_ALIAS_COLLAT_CRITICAL = "RM_ALIAS_COLLAT_CRITICAL"
RM_ALIAS_COLLAT_LOW = "RM_ALIAS_COLLAT_LOW"
BUNDLE_ILLEGAL = "BUNDLE_ILLEGAL"
REB_LOCK = "REB_LOCK"
REB_LOCK_CHECK_FAILED = "REB_LOCK_CHECK_FAILED"
TT_CONTRACT_INVALID = "TT_CONTRACT_INVALID"
TM_CONTRACT_INVALID = "TM_CONTRACT_INVALID"
REB_CONTRACT_INVALID = "REB_CONTRACT_INVALID"
REB_DISABLED = "REB_DISABLED"
# =========================
# RM Reason taxonomy (closed set)
# =========================
# Règles:
# 1) 1 invariant -> 1 reason unique (pas d’alias).
# 2) Si plusieurs invariants tombent: choisir le reason le plus prioritaire
#    selon RM_REASON_PRIORITY (index le plus petit).
# 3) "metric dédiée ⇄ reason dédié": toute cause structurante doit avoir son reason.



# Liste fermée (à maintenir) + ordre de priorité global.
# (On met des strings pour éviter toute dépendance à l’ordre de définition des constantes.)
RM_REASON_PRIORITY = (
    # A) Hard-safety / invariants (bloquants)
    "RM_CAPS_INVALID",
    "RM_COST_COMPUTE_ERROR",
    "RM_CAPS_BROKEN",
    "RM_SFC_UNAVAILABLE",
    "RM_BUNDLE_EMPTY_PARAMS",
    "RM_REB_FACTORY_REJECT",
    "RM_OPP_BAD_INPUTS",
    "RM_REGION_UNSUPPORTED",
    "TT_CONTRACT_INVALID",
    "TM_CONTRACT_INVALID",
    "REB_CONTRACT_INVALID",
    "RM_REGION_UNKNOWN",

    # B) Readiness
    "RM_ENGINE_NOT_READY",

    # C) Freshness / TTL strict
    "RM_BALANCE_TTL_BLOCK",
    "RM_STALE_VOL",
    "RM_MARKETDATA_STALE",

    # D) Guards (risque / intégrité trade)
    "RM_TTTM_DELTA_HARD_LIMIT",
    "RM_MM_DELTA_HARD_LIMIT",

    # E) Pacer / RL / Backpressure / NACK Engine (tech)
    "RM_ENGINE_NACK_429",
    "RM_ENGINE_NACK_TIMEOUT",
    "RM_ENGINE_NACK_5XX",
    "RM_ENGINE_NACK_REJECT",

    "ENGINE_BACKPRESSURE_QUEUE_FULL",
    "ENGINE_BACKPRESSURE_CAP_BRANCH",
    "ENGINE_BACKPRESSURE_HIGH_WM",

    "ENGINE_MM_DISABLED_BY_CAPITAL",
    "ENGINE_NACK_429",
    "ENGINE_NACK_5XX",
    "ENGINE_REJECT",
    "ENGINE_SUBMIT_TIMEOUT",
    "ENGINE_PRICE_GUARD",
    "ENGINE_SHALLOW_BOOK",

    # F) Locks
    "REB_LOCK",
    "REB_LOCK_CHECK_FAILED",
    "BUNDLE_ILLEGAL",

    # G) Caps / Budgets
    "RM_CAP_PROFILE_DISABLED",
    "RM_CAP_BRANCH_DISABLED",
    "RM_CAP_COMBO_EXCEEDED",
    "RM_CAPS_ZERO",
    "CAPS_PREEMPT",

    "RM_MM_BUDGET_EXHAUSTED",      # reason canonique RM (Partie 2: mapping strict)
    "MM_BUDGET_EXHAUSTED",       # legacy (Partie 2: à mapper vers RM_MM_BUDGET_EXHAUSTED)
    "RM_BUDGET_EXHAUSTED",

    "REB_REJECT_CAP_EXCEEDED",

    # H) Soft-gates / éligibilité (non “hard-safety”)
    "RM_ALIAS_COLLAT_CRITICAL",
    "RM_ALIAS_COLLAT_LOW",
    "RM_BELOW_MIN_NOTIONAL",
    "RM_BELOW_MIN_BPS",
    "RM_INTERNAL_ERROR",
    "RM_INTERNAL_SKIP",
    "RM_NOTIONAL_QUOTE_INVALID",
    "RM_QUOTE_MISMATCH",

    "GLOBAL_KILL_SWITCH",
    "RM_MODE_SEVERE_MM_OFF",
    "MM_DISABLED",

    "MM_MODE_MONO_REQUIRED",
    "MM_MODE_CROSS_UNSUPPORTED",

    "MM_HINTS_GUARD_VOL",
    "MM_HINTS_GUARD_NET_BPS",
    "MM_HINTS_GUARD_QPOS",
    "MM_HINTS_GUARD_DEPTH",
    "MM_HINTS_GUARD_P_BOTH",
    "MM_STALE_BOOK_DUAL",

    "MM_COLLAT_CRIT",
    "MM_REB_CRITICAL",
    "MM_HIGH_WM",
    "MM_DELTA_HEDGE",

    "MM_DUAL_SPREAD_TOO_SMALL",
    "MM_DUAL_DEPTH_TOO_SHALLOW",
    "MM_DUAL_INVENTORY_SKEW_TOO_HIGH",

    "REB_HINT_IGNORED_BAD_CONTEXT",
    "REB_HINT_IGNORED_SMALL_SIZE",
    "REB_HINT_IGNORED_UNIMPLEMENTED",

    "REB_SIM_REJECT_GUARD",
    "REB_SIM_REJECT_LATENCY",
    "REB_SIM_REJECT_BPS",

    "REB_HINT_EXEC_INTERNAL_TRANSFER",
    "REB_HINT_EXEC_REB_TM_NEUTRAL",
    "REB_TM_NEUTRAL_TRADE",
)

RM_REASON_RANK = {r: i for i, r in enumerate(RM_REASON_PRIORITY)}

def _rm_pick_reason(*candidates: str) -> str:
    """Choisit un reason unique selon RM_REASON_PRIORITY."""
    best = ""
    best_rank = 10**9
    for c in candidates:
        if not c:
            continue
        r = str(c)
        rank = RM_REASON_RANK.get(r, 10**8)
        if rank < best_rank:
            best, best_rank = r, rank
    return best or (str(candidates[0]) if candidates else "")

RM_MM_DELTA_HARD_LIMIT = "RM_MM_DELTA_HARD_LIMIT"
RM_MM_BUDGET_EXHAUSTED = "RM_MM_BUDGET_EXHAUSTED"
RM_TTTM_DELTA_HARD_LIMIT = "RM_TTTM_DELTA_HARD_LIMIT"

ENGINE_BACKPRESSURE_QUEUE_FULL = "ENGINE_BACKPRESSURE_QUEUE_FULL"
ENGINE_BACKPRESSURE_CAP_BRANCH = "ENGINE_BACKPRESSURE_CAP_BRANCH"
ENGINE_BACKPRESSURE_HIGH_WM = "ENGINE_BACKPRESSURE_HIGH_WM"
ENGINE_PRICE_GUARD = "ENGINE_PRICE_GUARD"
ENGINE_SHALLOW_BOOK = "ENGINE_SHALLOW_BOOK"
ENGINE_SUBMIT_TIMEOUT = "ENGINE_SUBMIT_TIMEOUT"
ENGINE_NACK_429 = "ENGINE_NACK_429"
ENGINE_NACK_5XX = "ENGINE_NACK_5XX"


from modules.risk_manager.rebalancing_manager import RebalancingManager
from modules.risk_manager.volatility_manager import VolatilityManager
from modules.risk_manager.slippage_and_fees_collector import SlippageAndFeesCollector
from modules.private_ws_reconciler import PrivateWSReconciler
from contracts.errors import (
    RMError, NotReadyError, DataStaleError, InconsistentStateError, ExternalServiceError, EngineSubmitError
)
import time

# risk_manager.py



# --- MM / Obs (ajouts légers) ---
try:
    from modules.obs_metrics import (INVENTORY_USD,
                                     RM_DECISION_MS,
                                     RM_FRAGMENT_PROFIT_MS,
                                     RM_REVALIDATE_MS,
                                     RM_PREFLIGHT_MS,
                                     RM_DECISIONS_TOTAL,
                                     RM_SKIPS_TOTAL,
                                     RM_QUEUE_DEPTH,
                                     RM_FINAL_DECISIONS_TOTAL,
                                     RM_ADMITTED_TOTAL,
                                     RM_DROPPED_TOTAL,
                                     STALE_OPPORTUNITY_DROPPED_TOTAL,
                                     PAIR_HEALTH_PENALTY_TOTAL,
                                     POOL_GATE_THROTTLES_TOTAL, FEE_TOKEN_CHECK_ERRORS_TOTAL,
                                     FEE_TOKEN_TOPUP_REQUESTED_TOTAL, )

    # gauge inventaire par ex/quote
except Exception:
    INVENTORY_USD = None  # tolérant si obs pas encore patché


from modules.obs_metrics import (
    mark_books_fresh, mark_balances_fresh, inc_rm_reject, inc_rm_skip,
    set_rm_paused_count, set_dynamic_min, REBAL_CROSS_TOO_EXPENSIVE_TOTAL,
    get_counter, get_gauge, safe_inc, safe_set, safe_observe,
    RM_SHUTDOWN_SECONDS, RM_SHUTDOWN_TIMEOUT_TOTAL, RM_SHUTDOWN_PENDING_TASKS,
    RM_PIPELINE_READY, RM_TRADING_READY, RM_DEP_READY, RM_CALLBACK_LATENCY_MS,
    RM_CALLBACK_DROPS_TOTAL, RM_CALLBACK_INFLIGHT,
)

from modules.bot_config import ALLOWED_BRANCHES, ALLOWED_CAPITAL_PROFILES


# --- Taxonomie commune des raisons (Ticket 12) -----------------------------
# NB: Ces codes doivent rester synchrones avec ceux d'execution_engine.py
# et, à terme, pourront être extraits dans contracts/reasons.py.

# Famille RM_* : rejets économiques / guards métier
RM_STALE_VOL = "RM_STALE_VOL"
RM_SFC_UNAVAILABLE = "RM_SFC_UNAVAILABLE"
RM_COST_COMPUTE_ERROR = "RM_COST_COMPUTE_ERROR"
RM_BUNDLE_EMPTY_PARAMS = "RM_BUNDLE_EMPTY_PARAMS"
RM_OPP_BAD_INPUTS = "RM_OPP_BAD_INPUTS"
RM_MARKETDATA_STALE = "RM_MARKETDATA_STALE"
RM_REGION_UNSUPPORTED = "RM_REGION_UNSUPPORTED"
RM_REGION_UNKNOWN = "RM_REGION_UNKNOWN"
RM_INTERNAL_ERROR = "RM_INTERNAL_ERROR"
RM_INTERNAL_SKIP = "RM_INTERNAL_SKIP"
RM_NOTIONAL_QUOTE_INVALID = "RM_NOTIONAL_QUOTE_INVALID"
RM_QUOTE_MISMATCH = "RM_QUOTE_MISMATCH"

# Famille RM_* mais pour causes techniques côté Engine vues par le RM
RM_ENGINE_NOT_READY = "RM_ENGINE_NOT_READY"
RM_ENGINE_NACK_TIMEOUT = "RM_ENGINE_NACK_TIMEOUT"
RM_ENGINE_NACK_429 = "RM_ENGINE_NACK_429"
RM_ENGINE_NACK_5XX = "RM_ENGINE_NACK_5XX"
RM_ENGINE_NACK_REJECT = "RM_ENGINE_NACK_REJECT"

# Famille RM_CAP_* : rejets caps métier (profil/branche/combo/disable)
RM_CAPS_INVALID = "RM_CAPS_INVALID"
RM_CAPS_BROKEN = "RM_CAPS_BROKEN"
RM_CAPS_ZERO = "RM_CAPS_ZERO"
RM_CAP_PROFILE_DISABLED = "RM_CAP_PROFILE_DISABLED"
RM_CAP_BRANCH_DISABLED = "RM_CAP_BRANCH_DISABLED"
RM_CAP_COMBO_EXCEEDED = "RM_CAP_COMBO_EXCEEDED"

# --- Metrics (caps path hygiene) -------------------------------------------
RM_CAPS_BUNDLE_CALLS_TOTAL = get_counter(
    "rm_caps_bundle_calls_total",
    "Bundle-level caps/preemption path invocations",
)

# --- Collat health ----------------------------------------------------------
RM_ALIAS_COLLAT_RATIO = get_gauge(
    "rm_alias_collat_ratio",
    "Alias-level collateral ratio (USD-like holdings / min_usd)",
    labelnames=("exchange", "alias"),
)
RM_ALIAS_COLLAT_STATE = get_gauge(
    "rm_alias_collat_state",
    "Alias-level collateral state (0=OK,1=LOW,2=CRITICAL)",
    labelnames=("exchange", "alias"),
)
RM_ALIAS_COLLAT_LOW_TOTAL = get_counter(
    "rm_alias_collat_low_total",
    "Alias collateral LOW state occurrences during gating",
    labelnames=("exchange", "alias", "branch"),
)
RM_MM_COLLAT_CRIT_DROP_TOTAL = get_counter(
    "rm_mm_collat_crit_drop_total",
    "MM bundles dropped because alias collateral state is CRIT",
    labelnames=("exchange", "alias"),
)


# --- MM obs: budgets, preemptions, wallet inventory ------------------------
RM_MM_BUDGET_SPENT_QUOTE = get_counter(
    "rm_mm_budget_spent_quote",
    "Quote currency spent from MM virtual wallet",
    labelnames=("profile", "exchange", "quote"),
)
RM_MM_BUDGET_EXHAUSTED_TOTAL = get_counter(
    "rm_mm_budget_exhausted_total",
    "MM opportunities blocked due to exhausted MM virtual wallet",
    labelnames=("profile", "exchange", "quote"),
)
RM_MM_PREEMPTED_TOTAL = get_counter(
    "rm_mm_preempted_total",
    "MM liquidity preempted by higher-priority branches",
    labelnames=("by",),
)
RM_MM_DELTA_USD = get_gauge(
    "rm_mm_delta_usd",
    "Per-asset MM delta expressed in USD-like terms",
    labelnames=("asset",),
)
RM_MM_DELTA_STATE = get_gauge(
    "rm_mm_delta_state",
    "Per-asset MM delta state (0=OK,1=SOFT,2=HARD)",
    labelnames=("asset",),
)
RM_MM_DELTA_SOFT_HIT = get_counter(
    "rm_mm_delta_soft_hit_total",
    "Soft-limit MM delta hits observed during gating",
    labelnames=("asset",),
)

RM_TTTM_DELTA_USD = get_gauge(
    "rm_tttm_delta_usd",
    "Per-asset TT/TM delta expressed in USD-like terms",
    labelnames=("asset",),
)
RM_TTTM_DELTA_STATE = get_gauge(
    "rm_tttm_delta_state",
    "Per-asset TT/TM delta state (0=OK,1=SOFT,2=HARD)",
    labelnames=("asset",),
)
RM_TTTM_DELTA_SOFT_HIT = get_counter(
    "rm_tttm_delta_soft_hit_total",
    "Soft-limit TT/TM delta hits observed during gating",
    labelnames=("asset", "branch"),
)

# Reason codes spécifiques REB
RM_REB_FACTORY_REJECT = "RM_REB_FACTORY_REJECT"
REB_HINT_IGNORED_SMALL_SIZE = "REB_HINT_IGNORED_SMALL_SIZE"
REB_HINT_IGNORED_BAD_CONTEXT = "REB_HINT_IGNORED_BAD_CONTEXT"
REB_HINT_IGNORED_UNIMPLEMENTED = "REB_HINT_IGNORED_UNIMPLEMENTED"
REB_HINT_EXEC_INTERNAL_TRANSFER = "REB_HINT_EXEC_INTERNAL_TRANSFER"
REB_HINT_EXEC_REB_TM_NEUTRAL = "REB_HINT_EXEC_REB_TM_NEUTRAL"
REB_REJECT_CAP_EXCEEDED = "REB_REJECT_CAP_EXCEEDED"
REB_SIM_REJECT_BPS = "REB_SIM_REJECT_BPS"
REB_SIM_REJECT_LATENCY = "REB_SIM_REJECT_LATENCY"
REB_SIM_REJECT_GUARD = "REB_SIM_REJECT_GUARD"

RM_REB_HINTS_TOTAL = get_counter(
    "rm_reb_hints_total",
    "Total REB hints handled by type and action",
    labelnames=("type", "action"),
)

RM_REB_FROM_HINTS_NOTIONAL_QUOTE_TOTAL = get_counter(
    "rm_reb_from_hints_notional_quote_total",
    "Total REB notional in quote currency submitted from hints",
    labelnames=("profile", "quote"),
)

RM_MM_HEDGE_TOTAL = get_counter(
    "rm_mm_hedge_total",
    "MM delta hedge attempts",
    labelnames=("asset",),
)
RM_MM_HEDGE_USD = get_counter(
    "rm_mm_hedge_usd",
    "Notional hedged for MM delta",
    labelnames=("asset",),
)
RM_TT_HEDGE_TOTAL = get_counter(
    "rm_tt_hedge_total",
    "TT stuck hedge attempts",
    labelnames=("asset",),
)
RM_TT_HEDGE_USD = get_counter(
    "rm_tt_hedge_usd",
    "Notional hedged for TT stuck legs",
    labelnames=("asset",),
)
RM_MM_HEDGE_FAILED_TOTAL = get_counter(
    "rm_mm_hedge_failed_total",
    "MM delta hedge failures",
    labelnames=("asset",),
)
RM_TT_HEDGE_FAILED_TOTAL = get_counter(
    "rm_tt_hedge_failed_total",
    "TT stuck hedge failures",
    labelnames=("asset",),
)

# --- Helpers robusti per cast da ENV/config --------------------------------
# --- Helpers robusti per cast da ENV/config (module-scope, no decorator) ---

def _cfg_root(self): return self.cfg
def _cfg_g(self):    return getattr(self.cfg, "g", None)
def _cfg_rm(self):   return getattr(self.cfg, "rm", None)

@staticmethod
def _as_float_or(val, default: float) -> float:
    if val in (None, "", "None"):
        return float(default)
    try:
        return float(val)
    except Exception:
        return float(default)

@staticmethod
def _as_int_or(val, default: int) -> int:
    if val in (None, "", "None"):
        return int(default)
    try:
        return int(val)
    except Exception:
        return int(default)

@staticmethod
def _as_str_or(val, default: str) -> str:
    if val in (None, "None"):
        return default
    try:
        s = str(val)
        return default if s == "" else s
    except Exception:
        return default

@staticmethod
def _as_dict_or_empty(v):
    try:
        return dict(v) if isinstance(v, dict) else {}
    except Exception:
        return {}

@staticmethod
def _as_list_upper(val, default):
    if val is None:
        return list(default)
    if isinstance(val, str):
        s = val.strip()
        if not s or s.lower() == "none":
            return list(default)
        # JSON list ?
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return [str(e).upper() for e in parsed]
        except Exception:
            # CSV
            return [t.strip().upper() for t in s.split(",") if t.strip()]
    if isinstance(val, (list, tuple, set)):
        return [str(e).upper() for e in val]
    return list(default)

logger = logging.getLogger("RiskManager")


# ---- Token bucket simple (module-level) ----
class _Bucket:
    __slots__ = ("r", "b", "tokens", "t0")
    def __init__(self, rate_per_s: float, burst: float):
        import time
        self.r = max(0.0, float(rate_per_s))
        self.b = max(1.0, float(burst))
        self.tokens = self.b
        self.t0 = time.time()

    def try_acquire(self) -> bool:
        import time
        now = time.time()
        dt = now - self.t0
        # refill
        self.tokens = min(self.b, self.tokens + dt * self.r)
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            self.t0 = now
            return True
        self.t0 = now
        return False

    # utile si tu fais de l’auto-tuning dynamiquement
    def update(self, rate_per_s: float = None, burst: float = None):
        if rate_per_s is not None:
            self.r = max(0.0, float(rate_per_s))
        if burst is not None:
            self.b = max(1.0, float(burst))
            self.tokens = min(self.tokens, self.b)

@dataclass
class DecisionRecord:
    ts_ns: int
    status: str          # "admitted" | "skipped" | ...
    reason: str          # code RM_* ou autre
    pair: str
    buy_exchange: str
    sell_exchange: str
    prudence: Optional[float] = None
    slippage_kind: Optional[str] = None
    # Ajouts M1-4 pour funnel GO/NO-GO
    strategy: Optional[str] = None   # TT | TM | MM | REB | UNKNOWN
    branch: Optional[str] = None     # pour l’instant = strategy (mais extensible)
    profile: Optional[str] = None    # NANO | MICRO | SMALL | MID | LARGE | UNKNOWN
    explain: Optional[Dict[str, Any]] = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":"))
# =============================================================================

# -----------------------------
# Quote handling (USDC & EUR)
# -----------------------------
QUOTES_ALLOWED = ("USDC", "EUR")


def _pair_quote(pair_key: str) -> str:
    pk = (pair_key or "").replace("-", "").upper()
    for q in QUOTES_ALLOWED:
        if pk.endswith(q):
            return q
    return "USDC"


def _strip_quote(pair_key: str) -> str:
    pk = (pair_key or "").replace("-", "").upper()
    q = _pair_quote(pk)
    return pk[:-len(q)] if pk.endswith(q) else pk

# === RM: Fee Token Buyer (IOC/Market + fallback LIMIT IOC) ===
class FeeTokenBuyer:
    """
    Acheteur de tokens de fees (BNB/MNT...) avec sélection de symbole (USDT/USDC/EUR),
    MARKET (quote_amount) si dispo, sinon LIMIT IOC avec buffer de slippage.
    """
    def __init__(self, config, logger=None):
        self.cfg = config
        self.log = logger if logger is not None else getattr(config, "logger", None)
        self.max_slip_bps = _cfg_int(self.cfg, "fee_topup_max_slip_bps", 30)  # 30 bps par défaut
        # préférence des quotes par exchange (override par BotConfig si donné)
        self.quote_pref = getattr(
            config, "fee_token_quote_preference",
            {"BINANCE": ["USDT", "USDC", "EUR"], "BYBIT": ["USDT", "USDC"], "COINBASE": ["USDT", "USD", "USDC"]}
        )
        # gateway RPC pour envoyer des ordres spot
        try:
            # risk_manager.py
            from modules.rpc_gateway import RPCClient  # client réel

            region = getattr(config, "pod_region", getattr(config, "region", "EU"))
            self.rpc = RPCClient(config, region=region)

        except Exception:
            self.rpc = None
            if self.log:
                self.log.warning("FeeTokenBuyer: RpcGateway indisponible, fallback limité.")

        # market data router (facultatif)
        try:
            from modules.market_data_router import MarketDataRouter as mdr
            self.mdr = mdr
        except Exception:
            self.mdr = None

    # ---- symbol & price helpers ----
    def _choose_symbol(self, ex_key: str, token: str) -> (str, str):
        prefs = self.quote_pref.get(ex_key.upper(), ["USDT", "USDC", "EUR"])
        for q in prefs:
            sym = f"{token}{q}"
            if self._symbol_exists(ex_key, sym):
                return sym, q
        # fallback “suffisamment bon”
        return f"{token}USDT", "USDT"

    def _symbol_exists(self, ex_key: str, symbol: str) -> bool:
        # si tu as un listing des symbols par exchange, vérifie-le ici
        # sinon, tente un ticker; si ça répond, on considère qu’il existe
        try:
            if self.rpc and hasattr(self.rpc, "get_ticker_price"):
                px = self.rpc.get_ticker_price(ex_key, symbol)
                return px is not None and float(px) > 0
        except Exception:
            pass
        return True  # on assume vrai si on ne peut pas vérifier

    def _get_price(self, ex_key: str, symbol: str) -> Optional[float]:
        # 1) market_data_router si dispo
        try:
            if self.mdr and hasattr(self.mdr, "get_last_price"):
                px = self.mdr.get_last_price(ex_key, symbol)
                if px: return float(px)
        except Exception:
            pass
        # 2) RPC (ticker)
        try:
            if self.rpc and hasattr(self.rpc, "get_ticker_price"):
                px = self.rpc.get_ticker_price(ex_key, symbol)
                if px: return float(px)
        except Exception:
            pass
        return None

    # ---- execution primitives ----
    def _place_market_quote(self, ex_key: str, symbol: str, quote_amount: float) -> Dict[str, Any]:
        """
        MARKET avec montant en quote (quoteOrderQty). Tous les RPC ne supportent pas;
        on tente puis on laisse l’exception conduire au fallback LIMIT IOC.
        """
        if not self.rpc or not hasattr(self.rpc, "place_spot_order"):
            raise RuntimeError("rpc_gateway.place_spot_order indisponible")
        return self.rpc.place_spot_order(
            exchange=ex_key,
            symbol=symbol,
            side="BUY",
            order_type="MARKET",
            quote_amount=float(quote_amount)
        )

    def _place_limit_ioc(self, ex_key: str, symbol: str, price: float, quote_amount: float) -> Dict[str, Any]:
        """ LIMIT IOC agressif (BUY) avec buffer de slippage. """
        if not self.rpc or not hasattr(self.rpc, "place_spot_order"):
            raise RuntimeError("rpc_gateway.place_spot_order indisponible")
        # buffer de slippage (prix “au-dessus” pour BUY)
        buf = (1.0 + (self.max_slip_bps / 10000.0))
        limit_price = round(float(price) * buf, 8)
        size = max(1e-8, float(quote_amount) / limit_price)
        # certaines venues exigent des incréments de tick / stepSize -> idéalement snap au lot
        return self.rpc.place_spot_order(
            exchange=ex_key,
            symbol=symbol,
            side="BUY",
            order_type="LIMIT",
            timeInForce="IOC",
            price=limit_price,
            size=size
        )

    # ---- public top-up ----
    def topup_for_key(self, key: str, quote_amount: float) -> Dict[str, Any]:
        """
        key = "BINANCE:BNB" par ex. | quote_amount = 20.0 (USDT/USDC/EUR selon symbole choisi)
        """
        ex_key, token = key.split(":")
        ex_key = ex_key.upper()
        symbol, quote = self._choose_symbol(ex_key, token)

        # 1) essaie MARKET (quote qty)
        try:
            return self._place_market_quote(ex_key, symbol, quote_amount)
        except Exception as e:
            if self.log:
                self.log.info("TopUp %s via MARKET échoué (%s), fallback LIMIT IOC...", key, e)

        # 2) fallback LIMIT IOC si on a un prix
        px = self._get_price(ex_key, symbol)
        if px and px > 0:
            try:
                return self._place_limit_ioc(ex_key, symbol, px, quote_amount)
            except Exception as e:
                if self.log:
                    self.log.warning("TopUp %s LIMIT IOC échoué: %s", key, e)

        # 3) échec : log & retourne un statut d’erreur
        return {"status": "ERROR", "exchange": ex_key, "symbol": symbol, "q": quote_amount, "reason": "topup_failed"}
# === /RM: Fee Token Buyer ===

# === RM: Fee Token Reserves Policy (BNB/MNT) ===
class FeeTokenReservesPolicy:
    def __init__(self, config):
        self.cfg = config
        # seuils bas pour relancer un top-up (NANO par défaut)
        self.low_watermarks = {"BINANCE:BNB": 40.0, "BYBIT:MNT": 15.0}
        self.topup_step = 20.0  # incrément d'achat (€/USD équivalent)

    def targets_quote(self) -> dict:
        # ex: {"BINANCE:BNB": 100.0, "BYBIT:MNT": 100.0}
        return dict(getattr(self.cfg, "fee_token_reserve_quote", {}) or {})

    def check_and_topup(self, fee_meta, place_order_cb):
        """
        Consomme la structure meta_fee produite par le MultiBalanceFetcher:

            fee_meta = {
                "<TOKEN>": {
                    "<EXCHANGE>.<ALIAS>": {
                        "balance": float,
                        "level": "low" | "ok" | "high",
                        "low_watermark": float,
                        "high_watermark": float,
                    },
                },
            }

        Retourne une liste d'actions (dict) décrivant les top-ups à effectuer.
        Cette méthode ne place aucun ordre ; elle produit uniquement des
        instructions structurées pour une pipeline séparée.
        """
        actions = []

        # Si aucune politique de cible ni step n'est définie, on ne fait rien.
        targets = self.targets_quote()
        if not targets and self.topup_step <= 0.0:
            return actions

        if not isinstance(fee_meta, dict):
            return actions

        for token, per_alias in fee_meta.items():
            if not isinstance(per_alias, dict):
                continue

            for exal, info in per_alias.items():
                info = info or {}
                level = str(info.get("level") or "").lower() or "unknown"
                balance = float(info.get("balance", info.get("bal", 0.0)) or 0.0)

                # On ne top-up que les niveaux "low", pour rester aligné avec MBF.
                if level != "low":
                    continue

                # Clé symbolique : "<EXCHANGE>.<ALIAS>:<TOKEN>"
                symbol_key = f"{exal}:{token}"

                # Politique de cible en quote :
                # 1) clé complète exal:token
                # 2) fallback sur le token seul
                target_quote = float(targets.get(symbol_key, 0.0) or 0.0)
                if target_quote <= 0.0:
                    target_quote = float(targets.get(token, 0.0) or 0.0)

                amount_quote = target_quote if target_quote > 0.0 else float(self.topup_step or 0.0)
                if amount_quote <= 0.0:
                    # Pas de politique de top-up pour ce token
                    continue

                self.log.warning(
                    "Fee token LOW (RM policy): %s level=%s balance=%.4f target_quote=%.4f",
                    symbol_key,
                    level,
                    balance,
                    amount_quote,
                )
                actions.append(place_order_cb(symbol_key, amount_quote))

        return actions


# === /RM: Fee Token Reserves Policy ===

# === RM: Upgrade/Downgrade controller (par région) ===
class ProfileController:
    def __init__(self, pacer, config):
        self.pacer = pacer
        self.cfg = config
        self.cooldown_min = int(getattr(config, "pacer_cooldown_min", 30))
        self._last_change_ts = {}

    def _cooldown_ok(self, region: str) -> bool:
        import time
        last = self._last_change_ts.get(region, 0.0)
        return (time.time() - last) >= self.cooldown_min * 60

    def _mark_change(self, region: str) -> None:
        import time
        self._last_change_ts[region] = time.time()

    def _get_rm_cfg(self):
        cfg = getattr(self, "cfg", None)
        if cfg is None:
            return None
        rm_cfg = getattr(cfg, "rm", None)
        return rm_cfg or cfg

    def _get_capital_ladder_cfg(self) -> Dict[str, Dict[str, Any]]:
        rm_cfg = self._get_rm_cfg()
        try:
            ladder_cfg = getattr(rm_cfg, "capital_ladder_cfg", {}) if rm_cfg is not None else {}
        except Exception:
            ladder_cfg = {}
        if isinstance(ladder_cfg, dict):
            return ladder_cfg
        return {}

    def _get_ladder_order(self) -> List[str]:
        ladder_cfg = self._get_capital_ladder_cfg()
        if ladder_cfg:
            # On respecte l'ordre défini dans BotConfig.capital_ladder_cfg
            return list(ladder_cfg.keys())
        # Fallback statique si la policy est absente
        return ["NANO", "MICRO", "SMALL", "MID", "LARGE"]

    def maybe_upgrade(
            self,
            region: str,
            net_per_sc: float,
            slo_ok: bool,
            pnl_ok: bool,
            dd_ok: bool,
            inflight_util_ok: bool,
    ) -> Optional[str]:
        if not self._cooldown_ok(region):
            return None
            # Si le Pacer n'est pas encore branché, on ne fait rien.
        if not getattr(self, "pacer", None):
            return None

        cur = getattr(self.pacer, "_profile", "NANO")
        ladder = self._get_ladder_order()
        try:
            idx = ladder.index(cur)
        except ValueError:
            idx = 0
        if idx >= len(ladder) - 1:
            return None  # déjà au sommet de la ladder

        next_prof = ladder[idx + 1]
        ladder_cfg = self._get_capital_ladder_cfg()
        policy_next = (ladder_cfg or {}).get(next_prof, {}) or {}
        min_cap = float(policy_next.get("min_capital_per_sc", 0.0) or 0.0)
        allow_upgrade = bool(policy_next.get("allow_auto_upgrade", True))

        # Gate capital (policy) + gates SLO/risque
        gate_ok = allow_upgrade and (net_per_sc >= min_cap)

        if gate_ok and slo_ok and pnl_ok and dd_ok and inflight_util_ok:
            self.pacer.set_capital_profile(next_prof)
            self._mark_change(region)
            return next_prof
        return None

    def maybe_downgrade(
            self,
            region: str,
            severe_evt: bool,
            err_spike: bool,
            lag_spike: bool,
            drain_spike: bool,
            dd_breach: bool,
    ) -> Optional[str]:
        if not self._cooldown_ok(region):
            return None
            # Si le Pacer n'est pas encore branché, on ne fait rien.
        if not getattr(self, "pacer", None):
            return None
        cur = getattr(self.pacer, "_profile", "NANO")
        ladder = self._get_ladder_order()
        try:
            idx = ladder.index(cur)
        except ValueError:
            idx = 0
        if idx <= 0:
            return None  # déjà au profil le plus bas

        ladder_cfg = self._get_capital_ladder_cfg()
        policy_cur = (ladder_cfg or {}).get(cur, {}) or {}
        allow_downgrade = bool(policy_cur.get("allow_auto_downgrade", True))
        if not allow_downgrade:
            return None

        if severe_evt or err_spike or lag_spike or drain_spike or dd_breach:
            next_prof = ladder[idx - 1]
            self.pacer.set_capital_profile(next_prof)
            self._mark_change(region)
            return next_prof
        return None


# === /RM: Upgrade/Downgrade controller ===
@staticmethod
def _cfg_float(cfg, name: str, default: float) -> float:
    try:
        v = getattr(cfg, name, None)
        return float(default if v is None else v)
    except Exception:
        return float(default)

@staticmethod
def _cfg_int(cfg, name: str, default: int) -> int:
    try:
        v = getattr(cfg, name, None)
        return int(default if v is None else v)
    except Exception:
        return int(default)

@staticmethod
def _cfg_str(cfg, name: str, default: str) -> str:
    try:
        v = getattr(cfg, name, None)
        s = default if v is None else str(v)
        return s
    except Exception:
        return default

@staticmethod
def _cfg_dict(cfg, name: str, default: dict) -> dict:
    try:
        v = getattr(cfg, name, None)
        return dict(default if v is None else v)
    except Exception:
        return dict(default)

@staticmethod
def _cfg_list_upper(cfg, name: str, default: list) -> list:
    try:
        v = getattr(cfg, name, None)
        seq = default if v is None else list(v)
        return [str(x).upper() for x in seq]
    except Exception:
        return [str(x).upper() for x in default]

class TransferController:
    def __init__(
            self,
            *,
            policy: Optional[BackoffPolicy] = None,
            logger_historique_manager=None,
            submitted_timeout_s: float = 300.0,
            rate_limiter: Optional[Any] = None,
    ) -> None:
        self._policy = policy or BackoffPolicy()
        self._states: Dict[str, Dict[str, Any]] = {}
        self._lhm = logger_historique_manager
        self._submitted_timeout_s = float(submitted_timeout_s or 300.0)
        self._rate_limiter = rate_limiter
        self._order = {
            "REQUESTED": 0,
            "PREPARED": 1,
            "SUBMITTED": 2,
            "SETTLED": 3,
            "FAILED": 3,
        }

    @staticmethod
    def canonical_transfer_id(payload: Dict[str, Any]) -> str:
        return canonical_transfer_id(payload)

    @staticmethod
    def _op_id(transfer_id: str) -> str:
        return f"XFER/{transfer_id}"

    def rehydrate(self, inflight: list[dict[str, Any]], now_ms: Optional[int] = None, now_mono: Optional[float] = None) -> None:
        now_ms = now_ms or int(time.time() * 1000)
        now_mono = now_mono or time.monotonic()
        for row in inflight or []:
            op_id = str(row.get("op_id") or "")
            if not op_id.startswith("XFER/"):
                continue
            transfer_id = op_id.split("/", 1)[-1]
            self._set_state_from_row(row=row, transfer_id=transfer_id, now_ms=now_ms, now_mono=now_mono)

    def restore_from_journal(self, *, now_ms: Optional[int] = None, now_mono: Optional[float] = None) -> None:
        now_ms = now_ms or int(time.time() * 1000)
        now_mono = now_mono or time.monotonic()
        if not self._lhm:
            return
        try:
            inflight = self._lhm.list_inflight_transfers(include_expired=True)
        except Exception:
            logger.exception("[TransferController] restore_from_journal failed")
            return
        self.rehydrate(inflight, now_ms=now_ms, now_mono=now_mono)
        try:
            set_transfer_inflight(self.inflight_count())
        except Exception:
            pass

    def _set_state_from_row(self, *, row: dict[str, Any], transfer_id: str, now_ms: int, now_mono: float) -> None:
        expires = row.get("expires_ts_ms")
        state = str(row.get("status") or "SUBMITTED").upper()
        last_ts = float(row.get("updated_ts_ms") or row.get("created_ts_ms") or now_ms) / 1000.0
        payload = row.get("payload")
        attempts = row.get("attempt_count")
        expires_mono = None
        if expires is not None:
            try:
                remaining_s = max(0.0, (float(expires) - float(now_ms)) / 1000.0)
                expires_mono = now_mono + remaining_s
            except Exception:
                expires_mono = None

        self._states[transfer_id] = {
            "state": state,
            "last_ts": last_ts,
            "expires_ts_ms": expires,
            "expires_ts_mono": expires_mono,
            "payload": payload,
            "attempts": attempts,
        }
        if state in {"SUBMITTED", "REQUESTED", "PREPARED"} and expires is not None and int(expires) <= now_ms:
            op_id = self._op_id(transfer_id)
            self._fail_stuck(transfer_id=transfer_id, op_id=op_id, payload=payload, now_ms=now_ms)

    def _fail_stuck(
            self,
            *,
            transfer_id: str,
            op_id: str,
            payload: Optional[dict[str, Any]],
            now_ms: Optional[int] = None,
            reason: str = "XFER_STUCK_SUBMITTED_FAIL_CLOSED",
    ) -> None:
        try:
            self._transition(
                transfer_id=transfer_id,
                new_state="FAILED",
                payload=payload,
                error="stuck_submitted_timeout",
                reason=reason,
                expires_ts_ms=now_ms,
            )
        except Exception:
            logger.exception("[TransferController] failed to transition stuck transfer")
        if not self._lhm:
            return
        try:
            self._lhm.activate_fail_closed_latch(
                op_id="TRANSFERS/XFER_STUCK_SUBMITTED_FAIL_CLOSED",
                reason=reason,
                payload={"op_id": op_id, "transfer_id": transfer_id},
            )
        except Exception:
            logger.exception("[TransferController] failed to mark stuck transfer")

    def check_timeouts(self) -> None:
        if not self._states:
            return
        now_ms = int(time.time() * 1000)
        now_mono = time.monotonic()
        for transfer_id, state in list(self._states.items()):
            cur_state = state.get("state")
            if cur_state not in {"SUBMITTED", "REQUESTED", "PREPARED"}:
                continue

            expires_mono = state.get("expires_ts_mono")
            expires_ms = state.get("expires_ts_ms")
            is_expired = False
            if expires_mono is not None and float(expires_mono) <= now_mono:
                is_expired = True
            elif expires_ms is not None and int(expires_ms) <= now_ms:
                is_expired = True
            if is_expired:
                # AJOUT P0: Avant de fail-close, on pourrait tenter un refresh REST final
                # Mais ici TransferController est passif (data class). 
                # On délègue la décision de polling à RiskManager.
                try:
                    op_id = self._op_id(transfer_id)
                    self._fail_stuck(
                        transfer_id=transfer_id,
                        op_id=op_id,
                        payload=state.get("payload"),
                        now_ms=now_ms,
                    )
                except Exception:
                    logger.exception("[TransferController] failed to fail-close stuck transfer")

    def list_pending_transfer_ids(self) -> List[str]:
        return [tid for tid, st in (self._states or {}).items() if st.get("state") == "SUBMITTED"]

    def get_transfer_payload(self, transfer_id: str) -> Optional[Dict[str, Any]]:
        return self._states.get(transfer_id, {}).get("payload")

    def _persist_state(
        self,
        *,
        transfer_id: str,
        state: str,
        payload: Optional[dict[str, Any]] = None,
        expires_ts_ms: Optional[int] = None,
        error: Optional[str] = None,
        attempts: Optional[int] = None,
        reason: Optional[str] = None,
    ) -> None:
        op_id = self._op_id(transfer_id)
        if not self._lhm:
            return
        try:
            if state == "REQUESTED":
                self._lhm.mark_transfer_requested(op_id=op_id, payload=payload)
            elif state == "PREPARED":
                self._lhm.mark_transfer_prepared(
                    op_id=op_id,
                    payload=payload,
                    expires_ts_ms=expires_ts_ms,
                    attempt_count=attempts,
                )
            elif state == "SUBMITTED":
                self._lhm.mark_transfer_submitted(
                    op_id=op_id,
                    payload=payload,
                    expires_ts_ms=expires_ts_ms,
                    attempt_count=attempts,
                )
            elif state == "SETTLED":
                self._lhm.mark_transfer_settled(op_id=op_id, payload=payload)
            elif state == "FAILED":
                self._lhm.mark_transfer_failed(
                    op_id=op_id, payload=payload, last_error=error, reason=reason
                )
        except Exception:
            logger.exception("[TransferController] persist state failed")

    def _state_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "exchange": str(payload.get("exchange") or ""),
            "from_alias": str(payload.get("from_alias") or payload.get("from") or ""),
            "to_alias": str(payload.get("to_alias") or payload.get("to") or ""),
            "from_wallet": str(payload.get("from_wallet") or ""),
            "to_wallet": str(payload.get("to_wallet") or ""),
            "ccy": str(payload.get("ccy") or payload.get("currency") or ""),
            "amount": float(payload.get("amount") or payload.get("amount_quote") or payload.get("amount_usdc") or 0.0),
            "type": str(payload.get("type") or payload.get("kind") or "transfer"),
        }

    def _state_is_in_progress(self, state: Optional[dict[str, Any]]) -> bool:
        if not state:
            return False
        return str(state.get("state") or "").upper() in {"REQUESTED", "PREPARED", "SUBMITTED"}

    def inflight_count(self) -> int:
        return sum(1 for st in (self._states or {}).values() if self._state_is_in_progress(st))

    def _transition(
            self,
            *,
            transfer_id: str,
            new_state: str,
            payload: Optional[dict[str, Any]] = None,
            expires_ts_ms: Optional[int] = None,
            expires_ts_mono: Optional[float] = None,
            error: Optional[str] = None,
            attempts: Optional[int] = None,
            reason: Optional[str] = None,
    ) -> dict:
        now = time.time()
        current = self._states.get(transfer_id)
        cur_state = str((current or {}).get("state") or "").upper()
        new_state_up = str(new_state or "").upper()
        if cur_state in {"FAILED", "SETTLED"}:
            return current or {}
        if cur_state and self._order.get(new_state_up, -1) < self._order.get(cur_state, -1):
            return current or {}
        if cur_state == new_state_up and attempts is None and reason is None and error is None:
            return current or {}
        state_entry = {
            "state": new_state_up,
            "last_ts": now,
            "payload": payload,
            "expires_ts_ms": expires_ts_ms,
        }
        if expires_ts_mono is not None:
            state_entry["expires_ts_mono"] = expires_ts_mono
        if error is not None:
            state_entry["error"] = error
        if attempts is not None:
            state_entry["attempts"] = attempts
        self._states[transfer_id] = state_entry
        try:
            self._persist_state(
                transfer_id=transfer_id,
                state=new_state_up,
                payload=payload,
                expires_ts_ms=expires_ts_ms,
                error=error,
                attempts=attempts,
                reason=reason,
            )
        except Exception:
            logger.exception("[TransferController] persist state failed")
        try:
            set_transfer_inflight(self.inflight_count())
        except Exception:
            pass
        try:
            inc_transfer_fsm_event(new_state_up, reason or error)
        except Exception:
            pass
        return state_entry

    async def submit(self, *, payload: Dict[str, Any], submit_fn: Callable[[], Any], venue: str) -> Dict[str, Any]:
        transfer_id = payload.get("transfer_id") or self.canonical_transfer_id(payload)
        payload["transfer_id"] = transfer_id
        op_id = self._op_id(transfer_id)
        state = self._states.get(transfer_id)
        if state and str(state.get("state") or "").upper() == "SETTLED":
            try:
                inc_transfer_fsm_event("SETTLED", "TRANSFER_FSM_ALREADY_SETTLED")
            except Exception:
                pass
            return {
                "status": "SETTLED",
                "transfer_id": transfer_id,
                "replayed": True,
                "reason": "TRANSFER_FSM_ALREADY_SETTLED",
            }
        if self._state_is_in_progress(state):
            try:
                inc_transfer_fsm_event(state.get("state"), "TRANSFER_FSM_IN_PROGRESS_SKIP")
            except Exception:
                pass
            return {
                "status": state.get("state"),
                "transfer_id": transfer_id,
                "replayed": True,
                "reason": "TRANSFER_FSM_IN_PROGRESS_SKIP",
            }
        if state is None and self._lhm:
            existing = self._lhm.get_transfer_state(op_id)
            if existing:
                self._set_state_from_row(row=existing, transfer_id=transfer_id, now_ms=int(time.time() * 1000))
                state = self._states.get(transfer_id)
            if state and str(state.get("state") or "").upper() == "SETTLED":
                try:
                    inc_transfer_fsm_event("SETTLED", "TRANSFER_FSM_ALREADY_SETTLED")
                except Exception:
                    pass
                return {
                    "status": "SETTLED",
                    "transfer_id": transfer_id,
                    "replayed": True,
                    "reason": "TRANSFER_FSM_ALREADY_SETTLED",
                }
            if self._state_is_in_progress(state):
                try:
                    inc_transfer_fsm_event(state.get("state"), "TRANSFER_FSM_IN_PROGRESS_SKIP")
                except Exception:
                    pass
                return {
                    "status": state.get("state"),
                    "transfer_id": transfer_id,
                    "replayed": True,
                    "reason": "TRANSFER_FSM_IN_PROGRESS_SKIP",
                }

        state_payload = self._state_payload(payload)
        expires_ts_ms = int(time.time() * 1000 + (self._submitted_timeout_s * 1000.0))
        expires_ts_mono = time.monotonic() + float(self._submitted_timeout_s or 0.0)
        ex = str(payload.get("exchange") or venue.split(":")[0]).upper()

        self._transition(
            transfer_id=transfer_id,
            new_state="REQUESTED",
            payload=state_payload,
            expires_ts_ms=expires_ts_ms,
            expires_ts_mono=expires_ts_mono,
        )
        self._transition(
            transfer_id=transfer_id,
            new_state="PREPARED",
            payload=state_payload,
            expires_ts_ms=expires_ts_ms,
            expires_ts_mono=expires_ts_mono,
        )

        # Rate limiting (internal transfers often have strict quotas)
        start_wait = time.perf_counter()
        if self._rate_limiter:
            try:
                # "transfer" kind used by default for internal moves
                await self._rate_limiter.acquire(ex, "transfer")
            except Exception:
                pass

        rl_wait_ms = (time.perf_counter() - start_wait) * 1000.0
        if rl_wait_ms > 0:
            try:
                from modules.obs_metrics import REBAL_TRANSFER_RL_WAIT_MS
                if REBAL_TRANSFER_RL_WAIT_MS:
                    REBAL_TRANSFER_RL_WAIT_MS.labels(exchange=ex).observe(rl_wait_ms)
            except Exception:
                pass

        t_start_xfer = time.perf_counter()
        outcome = await awith_retry(submit_fn, venue=venue, policy=self._policy)
        t_lat_ms = (time.perf_counter() - t_start_xfer) * 1000.0

        # Observabilité fine
        try:
            from modules.obs_metrics import REBAL_OPERATIONS_TOTAL, REBAL_TRANSFER_LATENCY_MS
            op_type = str(payload.get("type") or "unknown")
            status_label = "SUCCESS" if outcome.ok else "FAILED"
            if REBAL_OPERATIONS_TOTAL:
                REBAL_OPERATIONS_TOTAL.labels(type=op_type, exchange=ex, status=status_label).inc()
            if REBAL_TRANSFER_LATENCY_MS:
                REBAL_TRANSFER_LATENCY_MS.labels(exchange=ex, type=op_type).observe(t_lat_ms)
        except Exception:
            pass

        if outcome.ok:

            self._transition(
                transfer_id=transfer_id,
                new_state="SUBMITTED",
                payload=state_payload,
                expires_ts_ms=expires_ts_ms,
                expires_ts_mono=expires_ts_mono,
                attempts=outcome.attempts,
            )
            return {"status": "SUBMITTED", "transfer_id": transfer_id, "attempts": outcome.attempts}

        self._transition(
            transfer_id=transfer_id,
            new_state="FAILED",
            payload=state_payload,
            error=str(outcome.last_exception) if outcome.last_exception else "unknown",
            attempts=outcome.attempts,
        )
        return {"status": "FAILED", "transfer_id": transfer_id, "attempts": outcome.attempts}

    def mark_settled(self, transfer_id: Optional[str], *, payload: Optional[dict[str, Any]] = None) -> None:
        if not transfer_id:
            return
        self._transition(transfer_id=transfer_id, new_state="SETTLED", payload=payload)

    def mark_submitted(
            self,
            transfer_id: Optional[str],
            *,
            payload: Optional[dict[str, Any]] = None,
            expires_ts_ms: Optional[int] = None,
            attempts: Optional[int] = None,
    ) -> None:
        if not transfer_id:
            return
        expires_ts_mono = None
        if expires_ts_ms is not None:
            try:
                now_ms = int(time.time() * 1000)
                remaining_s = max(0.0, (float(expires_ts_ms) - float(now_ms)) / 1000.0)
                expires_ts_mono = time.monotonic() + remaining_s
            except Exception:
                expires_ts_mono = None
        self._transition(
            transfer_id=transfer_id,
            new_state="SUBMITTED",
            payload=payload,
            expires_ts_ms=expires_ts_ms,
            expires_ts_mono=expires_ts_mono,
            attempts=attempts,
        )

    def mark_failed(
            self,
            transfer_id: Optional[str],
            *,
            payload: Optional[dict[str, Any]] = None,
            error: Optional[str] = None,
    ) -> None:
        if not transfer_id:
            return
        self._transition(transfer_id=transfer_id, new_state="FAILED", payload=payload, error=error)

class RiskManager:
    """
    Chef d’orchestre strict — **multi-comptes (TT/TM) par CEX**

    - Seul module autorisé à dialoguer avec :
        * MarketDataRouter (orderbooks via callback)
        * BalanceFetcher (ou soldes virtuels en dry-run)
        * Simulator (ajustements d'exécution + plan de fragmentation)
        * Système d’alerting (callback)
    - Sous-modules PASSIFS gérés par le RiskManager :
        * VolatilityManager, SlippageAndFeesCollector, RebalancingManager
    - Boucles internes :
        * _loop_orderbooks / _loop_balances / _loop_rebalancing / _loop_volatility / _loop_fee_sync

    Mises à niveau clés :
        • **Multi-comptes**: snapshots de soldes structurés par *exchange -> alias -> assets*.
        • **Rebalancing intra-CEX** (alias→alias) & **cross-CEX** (CEX→CEX).
        • **Validation inventaire** “alias-aware”.
        • **Seuil dynamique** min_required_bps basé sur la vol (télémétrie set_dynamic_min).
        • **Fast-path** (spread, frais+slip, fraîcheur OB) et modes TT/TM.
        • **TM**: helpers maker/taker, profondeur, bascule NON_NEUTRAL.
        •         • **Simu dual parallèle** si supportée: `simulate_both_parallel()`.
        • **Verrou d’exécution** par (exchange, alias, pair) côté Engine.
        • **Pacer infra partagé**: EnginePacer comme source unique des signaux infra
          (pacer_mode, pacing_ms, inflight_max), consommés par le RM via set_pacer_mode()
          pour dériver trade_mode (rm_mode × pacer_mode).
        • **Pipeline transferts internes** (wallet / sous-comptes).

    NOTE: toutes les quantités « volume_usdc » sont interprétées comme **notionnels en devise de cotation**
          (USDC *ou* EUR). Les noms legacy sont conservés pour compat.
    """

    # ------------------------------------------------------------------
    # Lifecycle / init
    # ------------------------------------------------------------------
    # Bridge: esponi gli helper di modulo anche come metodi di classe
    _as_float_or = staticmethod(_as_float_or)
    _as_int_or = staticmethod(_as_int_or)
    _as_str_or = staticmethod(_as_str_or)
    _as_dict_or_empty = staticmethod(_as_dict_or_empty)
    _as_list_upper=staticmethod(_as_list_upper)
    _pair_quote = staticmethod(_pair_quote)  # si tu veux pouvoir l’appeler en self._pair_quote(...)

    def _rm_cfg(self):
        try:
            return getattr(self, "_rm_cfg_obj", None) or getattr(self, "_cfg_root", None) or getattr(self.cfg, "rm",
                                                                                                     None)
        except Exception:
            return getattr(self, "cfg", None)

    def _rm_strict_config(self) -> bool:
        rm_cfg = self._rm_cfg()
        return bool(getattr(rm_cfg, "strict_config", False))

    def _rm_live_armed(self) -> bool:
        cfg_root = getattr(self, "_cfg_root", None) or getattr(self, "cfg", None)
        g_cfg = getattr(cfg_root, "g", None)
        fs = getattr(g_cfg, "feature_switches", {}) if g_cfg else {}
        live_mode = str(getattr(g_cfg, "mode", "DRY_RUN")).upper() == "PROD" or bool(
            fs.get("engine_real", False)
        )
        return live_mode and bool(getattr(g_cfg, "live_trading_armed", False))

    def _transfer_region_allowed(self, exchange: str) -> bool:
        cfg_root = getattr(self, "_cfg_root", None) or getattr(self, "cfg", None)
        g_cfg = getattr(cfg_root, "g", None)
        if g_cfg is None:
            return True
        dm = str(getattr(g_cfg, "deployment_mode", "SPLIT")).upper()
        if dm == "SPLIT":
            return True
        expected_region = "EU" if dm == "EU_ONLY" else "JP" if dm == "JP_ONLY" else None
        if expected_region is None:
            return True
        region_map = getattr(g_cfg, "exchange_region_map", {}) or {}
        ex_region = str(region_map.get(str(exchange).upper(), "") or "").upper()
        if not ex_region:
            return False
        return ex_region == expected_region
    def _is_mm_family(self, branch: str, kind: str = "") -> bool:
        br = str(branch or "").upper()
        k = str(kind or "").upper()
        # MM_MONO, MM_CROSS commencent par MM_
        return br == "MM" or br.startswith("MM_") or k == "MAKER_MM"

    def _should_raise_config_conflict(self) -> bool:
        return self._rm_strict_config() and self._rm_live_armed()

    def _warn_rm_alias(self, key: str, msg: str) -> None:
        if not hasattr(self, "_rm_alias_warnings"):
            self._rm_alias_warnings = set()
        if key not in self._rm_alias_warnings:
            self._rm_alias_warnings.add(key)
            logging.getLogger(__name__).warning("[RiskManager] %s", msg)


    def _resolve_rm_param(self, names: tuple | str, default=None):
        if isinstance(names, str):
            names = (names,)

        rm_cfg = self._rm_cfg()
        cfg_obj = getattr(self, "_cfg_root", None) or getattr(self, "cfg", None)

        for name in names:
            val = getattr(rm_cfg, name, None) if rm_cfg is not None else None
            if val is not None:
                legacy_vals = []
                if cfg_obj is not None:
                    legacy_val = getattr(cfg_obj, name, None)
                    if legacy_val is not None and legacy_val != val:
                        legacy_vals.append((type(cfg_obj).__name__, legacy_val))
                if legacy_vals:
                    try:
                        logger.warning(
                            "[RiskManager] config divergence for %s: rm=%s legacy=%s",
                            name,
                            val,
                            legacy_vals,
                        )
                        inc_rm_reject(reason="FALLBACK_THRESHOLD_USED")
                    except Exception:
                        pass
                return val
            if name.isupper() and cfg_obj is not None:
                legacy_val = getattr(cfg_obj, name, None)
                if legacy_val is not None:
                    msg = f"legacy config {name} is deprecated; use rm.* instead"
                    if self._should_raise_config_conflict():
                        raise RuntimeError(msg)
                    self._warn_rm_alias(f"{name}_legacy", msg)
                    return legacy_val

        return default

    def _get_rm_knob(self, name: str, legacy: str = None, default: Any = None) -> Any:
        try:
            rm_cfg = self._rm_cfg()
            if rm_cfg is not None:
                val = getattr(rm_cfg, name, None)
                if val is not None:
                    return val
                if legacy:
                    val = getattr(rm_cfg, legacy, None)
                    if val is not None:
                        return val

            if hasattr(self, "_switch_knobs") and self._switch_knobs:
                val = getattr(self._switch_knobs, name, None)
                if val is not None:
                    return val

            cfg_obj = getattr(self, "_cfg_root", None) or getattr(self, "cfg", None)
            if cfg_obj is not None:
                if legacy:
                    val = getattr(cfg_obj, legacy, None)
                    if val is not None:
                        return val
                val = getattr(cfg_obj, name, None)
                if val is not None:
                    return val
        except Exception:
            pass
        return default

    def _load_rm_runtime_policy(self) -> None:
        raw_budgets = self._resolve_rm_param("daily_strategy_budget_quote", {}) or {}
        self.daily_strategy_budget_quote = {
            str(k).upper(): float(v)
            for k, v in (raw_budgets or {}).items()
        }

        self._spent_today_quote = {"TT": 0.0, "TM": 0.0, "MM": 0.0}
        for strat in self.daily_strategy_budget_quote.keys():
            self._spent_today_quote.setdefault(strat, 0.0)

        self._budget_reset_ts = time.time()
        self._budget_reset_interval_s = float(
            self._resolve_rm_param("daily_budget_reset_interval_s", 86400.0)
        )
        self.global_kill_switch = bool(
            self._resolve_rm_param("global_kill_switch", False)
        )

    def _load_inventory_limits(self) -> None:
        inv_cap = self._resolve_rm_param(("inventory_cap_quote", "inventory_cap_usd"), 1500.0)
        self.inventory_cap_quote = float(inv_cap)
        self.inventory_cap_usd = self.inventory_cap_quote
        self.min_buffer_quote = float(self._resolve_rm_param("min_buffer_quote", 0.0))

        # P0: Coordonner les seuils de rééquilibrage pour éviter les oscillations
        # RiskManager (Passif) : On désactive un leg dès que le drift dépasse SOFT (ex: 1%)
        # RebalancingManager (Actif) : On déclenche un transfert dès que le drift dépasse HARD (ex: 5%)
        # Ces valeurs sont chargées depuis BotConfig.rm.inventory_limits si dispo
        self.inv_soft_drift_pct = float(self._resolve_rm_param(("inv_soft_drift_pct", "INV_SOFT_DRIFT_PCT"), 1.5))
        self.inv_hard_drift_pct = float(self._resolve_rm_param(("inv_hard_drift_pct", "INV_HARD_DRIFT_PCT"), 5.0))
        
        # On injecte ces seuils dans le RebalancingManager s'il est branché
        if hasattr(self, "rebalancing") and self.rebalancing:
            try:
                self.rebalancing.set_drift_thresholds(soft=self.inv_soft_drift_pct, hard=self.inv_hard_drift_pct)
            except Exception:
                pass

        # Hystérésis inventaire (DUAL <-> SINGLE)
        self.inv_band_hi_usd = float(self._resolve_rm_param("inv_band_hi_usd", 250.0))
        self.inv_band_lo_usd = float(self._resolve_rm_param("inv_band_lo_usd", 100.0))
        self.inv_state_min_seconds = float(self._resolve_rm_param("inv_state_min_seconds", 10.0))

        # Paramètres Anti-stuck (SINGLE mode)
        self.single_stuck_t1_s = float(self._resolve_rm_param("single_stuck_t1_s", 30.0))
        self.single_stuck_t2_s = float(self._resolve_rm_param("single_stuck_t2_s", 60.0))
        self.single_max_time_s = float(self._resolve_rm_param("single_max_time_s", 120.0))

        # Paramètres agressivité Phase 2
        self.mm_single_aggressive_pad_ticks = float(self._resolve_rm_param("mm_single_aggressive_pad_ticks", 0.0))
        self.mm_single_aggressive_size_factor = float(self._resolve_rm_param("mm_single_aggressive_size_factor", 1.2))

        # Budget cancel minimal (SINGLE mode)
        self.mm_single_min_quote_lifetime_ms = int(self._resolve_rm_param("mm_single_min_quote_lifetime_ms", 1000))
        self.mm_single_cancel_budget = int(self._resolve_rm_param("mm_single_cancel_budget", 30))

        # Seuil imbalance adverse
        self.mm_single_imbalance_threshold = float(self._resolve_rm_param("mm_single_imbalance_threshold", 0.6))

        # MM Cross strict p_both
        self.mm_p_both_min_cross = float(self._resolve_rm_param("mm_p_both_min_cross", 0.8))

        # État courant : (ex, alias, asset) -> {"mode": "DUAL"|"SINGLE", "last_transition": ts}
        self.mm_inventory_mode_state: Dict[tuple[str, str, str], Dict[str, Any]] = {}

    def __init__(
            self,
            *,
            bot_cfg,
            config,
            exchanges: List[str],
            symbols: List[str],
            balance_fetcher,
            volatility_monitor,
            simulator,
            get_orderbooks_callback: Callable[[], Any],
            alert_callback: Optional[Callable[[str, str, Optional[str], str], Any]] = None,
            # Sous-modules passifs (déjà instanciés)
            volatility_manager=None,
            slippage_collector=None,
            rebalancing=None,
            slippage_handler=None,
            loops_config: Optional[Dict[str, float]] = None,
            # Pipeline unifié en rebalancing (optionnel)
            rebalancing_callback: Optional[Callable[[Dict[str, Any]], Any]] = None,
            # --- AJOUTS ---
            execution_engine=None,
            scanner_consumer: Optional[Callable] = None,
            fee_sync_interval: Optional[float] = None,
            # Clients de transfert (par CEX)
            transfer_clients: Optional[Dict[str, Any]] = None,
            rate_limiter: Optional[Any] = None,
            # Readiness (nouveau)
            ready_event: asyncio.Event | None = None,
            history_logger: Optional[Callable[[Dict[str, Any]], Any]] = None,
    ) -> None:
        def _cfg_signature(cfg_obj):
            g_cfg = getattr(cfg_obj, "g", None)
            rm_cfg = getattr(cfg_obj, "rm", None)
            snapshot_hash = getattr(cfg_obj, "snapshot_hash", None)
            snapshot_val = snapshot_hash() if callable(snapshot_hash) else None
            return {
                "snapshot_hash": snapshot_val,
                "g.mode": getattr(g_cfg, "mode", None),
                "g.deployment_mode": getattr(g_cfg, "deployment_mode", None),
                "rm.strict_config": getattr(rm_cfg, "strict_config", None),
                "rm.switch_knobs": getattr(rm_cfg, "switch_knobs", None),
            }

        def _cfg_live_armed(cfg_obj) -> bool:
            g_cfg = getattr(cfg_obj, "g", None)
            fs = getattr(g_cfg, "feature_switches", {}) if g_cfg else {}
            live_mode = str(getattr(g_cfg, "mode", "DRY_RUN")).upper() == "PROD" or bool(
                fs.get("engine_real", False)
            )
            return live_mode and bool(getattr(g_cfg, "live_trading_armed", False))

        def _cfg_strict(cfg_obj) -> bool:
            rm_cfg = getattr(cfg_obj, "rm", None)
            return bool(getattr(rm_cfg, "strict_config", False))
        base_cfg = bot_cfg or config
        if bot_cfg is not None and config is not None and bot_cfg is not config:
            bot_sig = _cfg_signature(bot_cfg)
            cfg_sig = _cfg_signature(config)
            conflict = False
            if bot_sig.get("snapshot_hash") and cfg_sig.get("snapshot_hash"):
                conflict = bot_sig["snapshot_hash"] != cfg_sig["snapshot_hash"]
            else:
                for key in ("g.mode", "g.deployment_mode", "rm.strict_config", "rm.switch_knobs"):
                    if bot_sig.get(key) != cfg_sig.get(key):
                        conflict = True
                        break
            if conflict:
                msg = "config divergence between bot_cfg and cfg; using bot_cfg as canonical"
                strict = _cfg_strict(bot_cfg) or _cfg_strict(config)
                live_armed = _cfg_live_armed(bot_cfg) or _cfg_live_armed(config)
                if strict and live_armed:
                    raise RuntimeError(msg)
                logger.warning("[RiskManager] %s", msg)

        self._cfg_root = base_cfg
        self.bot_cfg = base_cfg
        self.config = base_cfg  # alias explicite conservé
        self.cfg = base_cfg
        self._rm_cfg_obj = getattr(self._cfg_root, "rm", None)
        self._rm_alias_warnings: set[str] = set()
        self.fee_reserves = FeeTokenReservesPolicy(self.config)
        self.history_logger = history_logger
        self._history_fsm = None
        self._lhm_manager = None
        if history_logger is not None and not callable(history_logger):
            if hasattr(history_logger, "sink") and callable(getattr(history_logger, "sink")):
                self.history_logger = history_logger.sink
            if hasattr(history_logger, "record_trade_fsm_event"):
                self._history_fsm = history_logger
            if hasattr(history_logger, "get_status") or hasattr(history_logger, "get_pnl_pipeline_flags"):
                self._lhm_manager = history_logger
        elif history_logger is not None and hasattr(history_logger, "record_trade_fsm_event"):
            self._history_fsm = history_logger
            if hasattr(history_logger, "get_status") or hasattr(history_logger, "get_pnl_pipeline_flags"):
                self._lhm_manager = history_logger
        # Hooks optionnels : observabilité (obs_inc) et mute de routes.
        # Le Boot / orchestrateur peut les remplir via set_obs_inc_callback /
        # set_mute_route_callback ou en assignant directement _obs_inc_cb/_mute_route_cb.
        self._obs_inc_cb: Optional[Callable[[str], Any]] = None
        self._mute_route_cb: Optional[Callable[..., Any]] = None

        self._combo_cap_window_s = float(
            getattr(getattr(self.cfg, "rm", None), "combo_cap_window_s", 120.0) or 120.0
        )
        self._combo_inflight_notional: Dict[str, List[Tuple[float, float]]] = {}
        # Ticket P0-RM-CAPMOVE-01: Réservations de balance pour éliminer les ghost funds (keyed by transfer_id)
        self._reserved_balances: Dict[str, Dict[str, Any]] = {}
        # Ticket P0-RM-RESYNC-01: Historique pour plafond global de resync
        self._resync_history: Dict[str, deque] = {}
        self._boot_ts = time.time()

        # EWMA Imbalance (exchange, pair) -> float
        self._mm_imbalance_ewma: Dict[tuple[str, str], float] = {}
        self._mm_imbalance_alpha = float(self._resolve_rm_param("mm_imbalance_alpha", 0.3))

        # Optimisation P1: Cache de snapshot balances pour le hot path (Scanner)
        self._balances_cache: Dict[str, Dict[str, Any]] = {}
        self._last_balances_ts: float = 0.0
        # Intervalle de rafraîchissement du cache balances interne (RM)
        self._balances_cache_interval_s = 0.200 # 200ms

        g_cfg = _cfg_g(self)
        self.capital_profile = str(getattr(g_cfg, "capital_profile", "SMALL") or "SMALL").upper()
        # --- Résumé des caps globaux par profil (Macro 3 / M3-A) ---
        # Best-effort uniquement, pour debug / observabilité. Aucune logique RM ne s'appuie
        # sur cette structure (les décisions restent basées sur caps_trading_by_profile,
        # inflight_rebal_by_profile et combo_cap_usd_by_profile).
        self._profile_caps_summary: Dict[str, Dict[str, float | int]] = {}
        try:
            rm_cfg = self._rm_cfg()
            if rm_cfg is not None:
                inflight = getattr(rm_cfg, "inflight_trading_by_profile", {}) or {}
                caps = getattr(rm_cfg, "caps_trading_by_profile", {}) or {}
                reb_inflight = getattr(rm_cfg, "inflight_rebal_by_profile", {}) or {}
                combo_caps = getattr(rm_cfg, "combo_cap_usd_by_profile", {}) or {}

                for prof, inflight_total in (inflight or {}).items():
                    prof_u = str(prof).upper()
                    prof_caps = (
                            caps.get(prof_u)
                            or caps.get(prof)
                            or {}
                    )
                    tt = int(prof_caps.get("TT") or 0)
                    tm = int(prof_caps.get("TM") or 0)
                    mm = int(prof_caps.get("MM") or 0)
                    reb = int(reb_inflight.get(prof_u) or reb_inflight.get(prof) or 0)
                    combo = float(combo_caps.get(prof_u) or combo_caps.get(prof) or 0.0)

                    self._profile_caps_summary[prof_u] = {
                        "inflight_trading": int(inflight_total),
                        "caps_tt": tt,
                        "caps_tm": tm,
                        "caps_mm": mm,
                        "caps_trading_sum": tt + tm + mm,
                        "inflight_rebal": reb,
                        "combo_cap_usd": combo,
                    }
        except Exception:
            # Purement décoratif : ne doit jamais casser l'init du RM
            self._profile_caps_summary = {}

        self._load_inventory_limits()
        # --- MM toggles & budgets ---
        self.mm_mode = str(getattr(self.bot_cfg, "mm_mode", "MONO") or "MONO").upper()
        self.enable_mm = bool(getattr(self.bot_cfg, "enable_maker_maker", False))
        self.mm_ttl_ms = _cfg_int(self.bot_cfg, "mm_ttl_ms", 2200)
        self.mm_alias = _cfg_str(self.bot_cfg, "mm_alias_name", "MM").upper()
        if self.mm_mode == "OFF":
            self.enable_mm = False

        # Delta MM (mesure + garde-fou P0)
        self.mm_delta_state: dict[str, dict] = {}
        self.mm_pair_spent_usdc: dict[tuple[str, str, str], float] = {}
        self.mm_last_hedge_ts: dict[str, float] = {}

        # Expo globale TT/TM par asset (VaR-lite).
        # Clé: asset UPPER -> {
        #   "tt_delta_usd": float,
        #   "tm_delta_usd": float,
        #   "delta_usd": float,
        #   "soft_usd": float,
        #   "hard_usd": float,
        #   "state": "OK" | "SOFT" | "HARD",
        #   "last_update_ts": float,
        # }
        self.tttm_exposure_state: Dict[str, Dict[str, Any]] = {}

        # Legs TT coincés ("stuck") agrégés par asset.
        # Clé: asset UPPER -> {
        #   "delta_usd": float,
        #   "legs": List[Dict[str, Any]],
        #   "last_update_ts": float,
        # }
        self.tt_stuck_state: Dict[str, Dict[str, Any]] = {}
        self.tt_last_hedge_ts: dict[str, float] = {}

        # Expositions TM "inflight" (P0: peut rester vide).
        # Clé: asset UPPER -> List[{"notional_usd": float, "side": "LONG"|"SHORT", "created_ts": float, "max_exposure_s": float}]
        self.tm_inflight_exposures: Dict[str, List[Dict[str, Any]]] = {}
        self.tm_exposure_ttl_ms_by_exchange: Dict[str, int] = {}
        self._tm_ttl_slo_warned = False

        # Tailles/slots MM par profil (définies dans BotConfig.rm)
        rm_cfg = _cfg_rm(self)
        try:
            from modules.bot_config import RiskManagerCfg
            rm_defaults = RiskManagerCfg()
        except Exception:
            rm_defaults = None

        slot_cfg_raw = getattr(rm_cfg, "mm_slot_notional_usdc_by_profile", None)
        if not isinstance(slot_cfg_raw, dict) or not slot_cfg_raw:
            slot_cfg_raw = getattr(rm_defaults, "mm_slot_notional_usdc_by_profile", {}) if rm_defaults else {}
        slot_cfg = slot_cfg_raw or {}

        self.mm_slot_notional_usdc_by_profile: Dict[str, float] = {
            str(k).upper(): self._as_float_or(v, 0.0)
            for k, v in slot_cfg.items()
        }
        pair_ratio_cfg_raw = getattr(rm_cfg, "mm_pair_cap_ratio_by_profile", None)
        if not isinstance(pair_ratio_cfg_raw, dict) or not pair_ratio_cfg_raw:
            pair_ratio_cfg_raw = getattr(rm_defaults, "mm_pair_cap_ratio_by_profile", {}) if rm_defaults else {}
        pair_ratio_cfg = pair_ratio_cfg_raw or {}

        self.mm_pair_cap_ratio_by_profile: Dict[str, float] = {
            str(k).upper(): self._as_float_or(v, 0.0)
            for k, v in pair_ratio_cfg.items()
        }
        slots_per_pair_cfg_raw = getattr(rm_cfg, "mm_slots_per_pair_by_profile", None)
        if not isinstance(slots_per_pair_cfg_raw, dict) or not slots_per_pair_cfg_raw:
            slots_per_pair_cfg_raw = getattr(rm_defaults, "mm_slots_per_pair_by_profile", {}) if rm_defaults else {}
        slots_per_pair_cfg = slots_per_pair_cfg_raw or {}

        self.mm_slots_per_pair_by_profile: Dict[str, int] = {
            str(k).upper(): self._as_int_or(v, 0)
            for k, v in slots_per_pair_cfg.items()
        }

        # --- MM rebalancing ladder (mono-CEX + intra + cross) ---
        self.mm_mono_alias_name = str(getattr(self.bot_cfg, "mm_mono_alias_name", "MM_MONO") or "MM_MONO").upper()
        self.mm_cross_alias_name = str(getattr(self.bot_cfg, "mm_cross_alias_name", "MM_CROSS") or "MM_CROSS").upper()

        self.mm_reb_inventory_soft_pct = _cfg_float(self.bot_cfg, "mm_reb_inventory_soft_pct", 5.0)
        self.mm_reb_inventory_hard_pct = _cfg_float(self.bot_cfg, "mm_reb_inventory_hard_pct", 15.0)
        self.mm_reb_inventory_critical_pct = _cfg_float(self.bot_cfg, "mm_reb_inventory_critical_pct", 25.0)

        # Seuils spécifiques Mono
        self.mm_mono_reb_soft = _cfg_float(self.bot_cfg, "mm_mono_reb_inventory_soft_pct", self.mm_reb_inventory_soft_pct)
        self.mm_mono_reb_hard = _cfg_float(self.bot_cfg, "mm_mono_reb_inventory_hard_pct", self.mm_reb_inventory_hard_pct)
        self.mm_mono_reb_critical = _cfg_float(self.bot_cfg, "mm_mono_reb_inventory_critical_pct", self.mm_reb_inventory_critical_pct)

        # Seuils spécifiques Cross
        self.mm_cross_reb_soft = _cfg_float(self.bot_cfg, "mm_cross_reb_inventory_soft_pct", self.mm_reb_inventory_soft_pct)
        self.mm_cross_reb_hard = _cfg_float(self.bot_cfg, "mm_cross_reb_inventory_hard_pct", self.mm_reb_inventory_hard_pct)
        self.mm_cross_reb_critical = _cfg_float(self.bot_cfg, "mm_cross_reb_inventory_critical_pct", self.mm_reb_inventory_critical_pct)

        self.mm_reb_inventory_min_notional_usd = _cfg_float(
            self.bot_cfg, "mm_reb_inventory_min_notional_usd", 200.0
        )
        self.mm_reb_collat_target_low_ratio = _cfg_float(
            self.bot_cfg, "mm_reb_collat_target_low_ratio", 1.2
        )
        self.mm_reb_collat_min_safe_ratio = _cfg_float(
            self.bot_cfg, "mm_reb_collat_min_safe_ratio", 1.05
        )
        # Placeholders P0 : ratios delta/collat non utilisés dans ce ticket
        self.mm_reb_delta_soft_usd = _cfg_float(self.bot_cfg, "mm_reb_delta_soft_usd", 0.0)
        self.mm_reb_delta_hard_usd = _cfg_float(self.bot_cfg, "mm_reb_delta_hard_usd", 0.0)
        self.mm_reb_allow_loss_bps = _cfg_float(self.bot_cfg, "mm_reb_allow_loss_bps", 5.0)
        self.mm_reb_state: Dict[tuple[str, str, str], Dict[str, Any]] = {}



        # --- MM MONO : conditions pour 2 côtés (BUY+SELL) ---
        self.mm_dual_min_net_bps = _cfg_float(self.bot_cfg, "mm_dual_min_net_bps", 3.0)
        self.mm_dual_min_depth_quote = _cfg_float(self.bot_cfg, "mm_dual_min_depth_quote", 0.0)
        self.mm_dual_max_skew_pct = _cfg_float(self.bot_cfg, "mm_dual_max_skew_pct", 10.0)
        self.mm_dual_guard_enabled = bool(getattr(self.bot_cfg, "mm_dual_guard_enabled", True))

        # --- MM inventaire (single maker) ---
        self.mm_inventory_enabled = bool(
            getattr(self.bot_cfg, "mm_inventory_enabled", False)
        )
        # seuils très simples pour commencer
        self.mm_inventory_max_skew_pct = float(
            getattr(self.bot_cfg, "mm_inventory_max_skew_pct", 15.0)
        )
        self.mm_inventory_notional_usd = float(
            getattr(self.bot_cfg, "mm_inventory_notional_usd", 500.0)
        )


        # --- TM TTL & hedge policy (source unique pour l'Engine) ---
        # Ces champs sont les SEULES sources de vérité pour TM :
        # - tm_exposure_ttl_ms : TTL d'exposition TM (ms)
        # - tm_exposure_ttl_hedge_ratio : ratio hedge en mode NEUTRAL
        # - tm_nn_hedge_ratio : ratio hedge cible en mode NON_NEUTRAL
        self.tm_exposure_ttl_ms = _cfg_int(self.cfg, "tm_exposure_ttl_ms", 2500)
        self.tm_exposure_ttl_hedge_ratio = _cfg_float(self.cfg, "tm_exposure_ttl_hedge_ratio", 0.50)
        self.tm_nn_hedge_ratio = _cfg_float(self.cfg, "tm_nn_hedge_ratio", 0.65)
        self._refresh_tm_exposure_ttl_from_slo()


        # Sink optionnel pour les drops de bundles (shadow, simu, recorder…)
        # Horizon métier maximum d'exposition TM NON_NEUTRAL (secondes).
        # Utilisé pour alimenter meta["tm"]["max_exposure_s"] sur les bundles TM/REB.
        self.tm_nn_max_exposure_s = _cfg_float(self.cfg, "tm_nn_max_exposure_s", 3.0)

        # Queue-position TM (ahead en QUOTE/USD) + ETA max (ms)
        # Ces valeurs servent de fallback canonique pour tm_controls envoyés à l'Engine.
        rm_cfg = self._rm_cfg()
        self.tm_queuepos_max_ahead_usd = _cfg_float(rm_cfg, "tm_queuepos_max_ahead_usd", 25000.0)
        self.tm_queuepos_max_eta_ms = _cfg_int(rm_cfg, "tm_queuepos_max_eta_ms", 0)
        # Alias "absolu" (aujourd'hui = ahead) pour compat tm_controls["queuepos_max_usd"].
        self.tm_queuepos_max_usd = self.tm_queuepos_max_ahead_usd

        self._shadow = None

        # ==== [ADD INSIDE __init__ RIGHT AFTER "MM toggles & budgets" BLOCK] =========
        # Kill switch global + budgets (en mémoire, resetés par ton scheduler quotidien)

        self._load_rm_runtime_policy()
        # Optionnel: fichier JSONL de décision (audit)

        self.decision_log_path = _cfg_str(SimpleNamespace(decision_log_path=self._resolve_rm_param("decision_log_path", "")), "decision_log_path","")  # vide = pas de fichier
        # Politique pré-filtre: source slippage et seuils de fraicheur (utilise déjà tes cfg si présents)
        self.slippage_source = _cfg_str(self.cfg, "sfc_slippage_source", "ewma")
        self.max_book_age_s    = _cfg_float(self.cfg, "max_book_age_s", 1.0)
        self.max_book_age_s    = _cfg_float(self.cfg, "max_book_age_s", 1.0)
        # =============================================================================

        # Préemption & caps notionnels par stratégie/CEX (devise de cotation)
        self.preempt_mm_for_tt_tm = bool(getattr(self.bot_cfg, "preempt_mm_for_tt_tm", True))
        self.ff_enforce_preemption = bool(getattr(self.bot_cfg, "ff_enforce_preemption", False))
        self.ff_tm_enabled = bool(getattr(self.bot_cfg, "ff_tm_enabled", False))
        self.ff_mm_enabled = bool(getattr(self.bot_cfg, "ff_mm_enabled", False))
        self.ff_mm_opportunistic_gating_enforced = bool(
            getattr(self.bot_cfg, "ff_mm_opportunistic_gating_enforced", False)
        )
        self.ff_reb_enabled = bool(getattr(self.bot_cfg, "ff_reb_enabled", False))
        self.mm_preempt_cooldown_s = float(getattr(self.bot_cfg, "mm_preempt_cooldown_s", 1.0))
        self._mm_last_preempt_ts: Dict[tuple[str, str, str], float] = {}
        self.per_strategy_notional_cap   = _cfg_dict(self.bot_cfg, "per_strategy_notional_cap", {})
        # --- budgets virtuels par quote (USDC/EUR) ---
        # ex: {"BINANCE":{"USDC": 25000.0, "EUR": 0.0}, ...}
        self.virt_balances: dict[str, dict[str, float]] = {}
        self.set_mm_budgets(_cfg_dict(self.bot_cfg, "mm_budget_by_exchange_quote", {}))

        # alias pratique vers la config si pas déjà présent
        self.cfg = getattr(self, "cfg", None) or config

        # paramètres dépendants de cfg
        self.rebal_allow_loss_bps   = _cfg_float(self.bot_cfg, "rebal_allow_loss_bps", 0.0)   # 0.0 = no-loss
        self.rebal_volume_haircut   = _cfg_float(self.bot_cfg, "rebal_volume_haircut", 0.80)  # 80% par défaut

        self.exchanges = [str(e).upper() for e in (exchanges or [])]
        self.symbols = [self._norm_pair(s) for s in (symbols or [])]
        self.balance_fetcher = balance_fetcher
        self.vol_monitor = volatility_monitor
        self.simulator = simulator
        self._get_orderbooks = get_orderbooks_callback
        # alias pour les getters ponctuels
        self.get_orderbooks_callback = get_orderbooks_callback
        self.alert_cb = alert_callback
        self._last_balances: Dict[str, Dict[str, Dict[str, float]]] = {}


        # Sous-modules passifs encapsulés
        self.vol_manager = volatility_manager
        self.slip_collector = slippage_collector
        self.slippage_collector = self.slip_collector  # compat avec _get_slippage()

        self.rebalancing = rebalancing
        self.slippage_handler = slippage_handler
        self.bot_cfg = bot_cfg

        self.engine = execution_engine

        # Hub WS privé + santé + wiring
        self.private_ws_hub = None
        self.private_ws_healthy: bool = True
        self._private_ws_status: Dict[str, Any] = {}
        self._pws_critical_drop_seen: bool = False
        self._pws_critical_drop_reason: Optional[str] = None
        self._pws_blocked_emitted: bool = False
        self.trading_state: str = "READY"
        self.trading_state_reason: Optional[str] = None
        # Flags de wiring (Hub / Reconciler) mis à jour par bind_* et _wire_*
        self.private_ws_wiring_ok: bool = False
        self.reconciler_wiring_ok: bool = False

        # Reconciler par défaut (peut être remplacé par bind_reconciler)
        self.reconciler = PrivateWSReconciler()
        self.reconciler._lookup = getattr(self, "lookup_inflight", None)
        self.reconciler._resync_order = getattr(self.engine, "resync_order", None)
        self.reconciler._resync_alias = getattr(self.engine, "resync_alias", None)

        # --- Instanciation lazy des sous-modules internes (si non injectés) ---

        if self.vol_manager is None:
            self.vol_manager = VolatilityManager(cfg_root=self.bot_cfg)
        else:
            logger.debug("[RiskManager] using injected VolatilityManager: %s", type(self.vol_manager).__name__)

        if self.slip_collector is None:
            self.slip_collector = SlippageAndFeesCollector(cfg=self.bot_cfg)
        else:
            logger.debug("[RiskManager] using injected SlippageAndFeesCollector: %s",
                         type(self.slip_collector).__name__)

        # Contrat fort pour le collector de slippage
        if self.slip_collector is None:
            raise RuntimeError("slip_collector manquant (contractuel)")

        fn = getattr(self.slip_collector, "ingest_slippage_bps", None)
        if not callable(fn):
            raise TypeError(
                "slip_collector doit exposer ingest_slippage_bps(pair, exchange, side, qty, slip_bps, ts_ns=None)"
            )

        if self.rebalancing is None:
            self.rebalancing = RebalancingManager(rm=self)
        else:
            logger.debug("[RiskManager] using injected RebalancingManager: %s", type(self.rebalancing).__name__)

        # Harmonise les alias legacy
        self.slippage_collector = self.slip_collector


        # Expose aussi "rebal_mgr" (utilisé ailleurs)
        self.rebal_mgr = self.rebalancing
        # --- fin instanciation lazy ---

        # Raccorder un "event sink" unique
        for sm in (self.vol_manager, self.slip_collector, self.rebalancing):
            if sm and getattr(sm, "set_event_sink", None):
                sm.set_event_sink(self._submodule_event)
        if self.rebalancing:
            cost_fn = getattr(self, "_reb_cost_fn", None)
            setter = getattr(self.rebalancing, "set_cost_function", None)
            if callable(setter):
                setter(cost_fn)
            else:
                setattr(self.rebalancing, "_reb_cost_fn", cost_fn)

        if self.simulator and getattr(self.simulator, "set_event_sink", None):
            try:
                self.simulator.set_event_sink(self._submodule_event)
            except Exception:
                logger.debug("[RiskManager] simulator.set_event_sink failed", exc_info=False)

        # Cadences
        lc = loops_config or {}
        self.t_vol = float(lc.get("volatility_interval", 0.5))
        self.t_books = float(lc.get("orderbooks_interval", 0.5))
        self.t_bal = float(lc.get("balances_interval", 5.0))
        self.t_rebal = float(lc.get("rebal_interval", 2.0))
        self.t_fee = float(lc.get("fee_sync_interval", fee_sync_interval or 600.0))
        self.t_mode = float(
            lc.get(
                "mode_interval",
                getattr(getattr(self.cfg, "rm", None), "mode_tick_interval_s", 1.0),
            )
        )
        self.ready_event = ready_event or asyncio.Event()
        self._last_fee_sync = 0.0

        # Rebalancing orchestration (TTL/cooldown)
        self.rebal_emit_cooldown_s = float(
            getattr(config, "rebal_emit_cooldown_s", lc.get("rebal_emit_cooldown_s", 30.0))
        )
        self.rebal_active_ttl_s = float(
            getattr(config, "rebal_active_ttl_s", lc.get("rebal_active_ttl_s", 20.0))
        )
        self._rebal_emit_next_allowed = 0.0
        self._rebalancing_until = 0.0
        self._rebalancing_cb = rebalancing_callback

        # Exécution : modes TT/TM activables
        self.enable_tt = bool(getattr(config, "enable_taker_taker", True))
        self.enable_tm = bool(getattr(config, "enable_taker_maker", False))

        # --- AJOUTS ---
        self.engine = execution_engine
        self._scanner_consumer = scanner_consumer
        self.transfer_clients: Dict[str, Any] = {str(k).upper(): v for k, v in (transfer_clients or {}).items()}
        retry_cfg = getattr(getattr(self.cfg, "rm", None), "transfer_retry_policy", None)
        retry_cfg = retry_cfg or getattr(self.cfg, "transfer_retry_policy", None)
        transfer_timeout_s = float(self._resolve_rm_param("transfer_submitted_timeout_s", 300.0) or 300.0)
        self._transfer_controller = TransferController(
            policy=BackoffPolicy.from_cfg(retry_cfg),
            logger_historique_manager=self._lhm_manager,
            submitted_timeout_s=transfer_timeout_s,
            rate_limiter=rate_limiter,
        )
        try:
            now_ms = int(time.time() * 1000)
            self._transfer_controller.restore_from_journal(now_ms=now_ms, now_mono=time.monotonic())
            if getattr(self, "rebalancing", None) and hasattr(
                    self.rebalancing, "restore_inflight_from_journal"
            ):
                self.rebalancing.restore_inflight_from_journal(now_ms=now_ms)
        except Exception:
            logger.exception("[RiskManager] failed to rehydrate transfer state")
        self._decision_id_cache: Dict[str, Dict[str, str]] = {}

        # Routes autorisées tri-CEX (configurable)
        raw_allowed_routes = (
                getattr(getattr(self.cfg, "g", None), "allowed_routes", None)
                or getattr(config, "allowed_routes", None)
                or {
                ("BINANCE", "BYBIT"), ("BYBIT", "BINANCE"),
                ("BINANCE", "COINBASE"), ("COINBASE", "BINANCE"),
                ("BYBIT", "COINBASE"), ("COINBASE", "BYBIT"),
            }
        )
        self.allowed_routes: Set[Tuple[str, str]] = {
            (str(a).upper(), str(b).upper())
            for (a, b) in (raw_allowed_routes or [])
        }
        self._sync_simulator_allowed_routes()
        # 1) s'assurer qu'on a un rebal_mgr dispo
        _mk_rebal_mgr_if_missing(self)

        # 2) coller le glue RM×MBF (conserve self.rebal_mgr si déjà présent)
        self._mbf_glue = _RM_MBFGlue(self, balance_fetcher, getattr(self, "rebal_mgr", None))
        # Santé pair-level (circuit-breakers courts)
        self._pair_penalties: Dict[str, float] = {}  # pk -> penalty_until_ts
        self.pair_penalty_default_ttl_s = _cfg_float(self.cfg,"pair_penalty_default_ttl_s", 15.0)

        # État runtime
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self.last_update = time.time()
        self._last_books: Dict[str, Dict[str, dict]] = {}
        self._orderbooks: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._last_sim_fee_sync = 0.0

        self._paused: Dict[str, bool] = {}
        self._loop_health: Dict[str, Dict[str, float]] = {
            name: {"last_success": 0.0, "consecutive_errors": 0}
            for name in ("orderbooks", "balances", "rebalancing", "volatility", "fee_sync")
        }
        self._loop_error_budget = max(1, int(getattr(self.cfg, "loop_error_budget", 5)))


        # Verrous par (exchange, alias, pair)
        self._pair_locks: Dict[Tuple[str, str, str], asyncio.Lock] = {}
        self._inflight: Set[Tuple[str, str, str]] = set()

        # Seuil dynamique min_required_bps
        rm_cfg = _cfg_rm(self)
        self.dynamic_min_required = bool(getattr(rm_cfg, "dynamic_min_required", True))
        self.base_min_bps = _cfg_float(rm_cfg, "base_min_bps", 20.0)
        self.dynamic_K = _cfg_float(rm_cfg, "dynamic_k", 0.3)
        self.min_bps_floor = _cfg_float(rm_cfg, "min_bps_floor", 10.0)
        self.min_bps_cap = _cfg_float(rm_cfg, "min_bps_cap", 60.0)

        # Caps d’inventaire / skew guard (cap interprété en **devise de cotation**)
        self.inventory_cap_usd = _cfg_float(config, "inventory_cap_usd", 1500.0)
        self.inventory_skew_max_pct = _cfg_float(config, "inventory_skew_max_pct", 25.0)
        self.inventory_rebal_exempt = bool(getattr(config, "inventory_rebal_exempt", True))

        self.max_book_age_s = _cfg_float(self.cfg, "max_book_age_s", 1.0)
        self.max_clock_skew_ms = _cfg_float(self.cfg,"max_clock_skew_ms", 250.0)

        # Dry-run : soldes virtuels (USDC & EUR / exchange / alias)
        self._virtual_balances: Dict[str, Dict[str, Dict[str, float]]] = {}
        if bool(getattr(self.cfg, "dry_run", False)):
            # P0: Augmenter radicalement les fonds virtuels pour ne pas brider les tests
            # On passe de 2000 à 1,000,000 USDC/EUR/USDT
            default_usdc = _cfg_float(self.cfg,"dry_usdc_per_account", 1000000.0)
            default_eur = _cfg_float(self.cfg,"dry_eur_per_account", default_usdc)
            default_usdt = _cfg_float(self.cfg,"dry_usdt_per_account", default_usdc)

            for ex in self.exchanges:
                self._virtual_balances[ex] = {
                    "TT": {"USDC": default_usdc, "EUR": default_eur, "USDT": default_usdt},
                    "TM": {"USDC": default_usdc, "EUR": default_eur, "USDT": default_usdt},
                }
            # 👉 normalise/merge une seule fois
            self.seed_virtual_balances(self._virtual_balances, overwrite=True)

        # Fragmentation (alignement Engine)
        self.min_fragment_usdc = _cfg_float(self.cfg, "min_fragment_usdc", 200.0)
        self.max_fragments = _cfg_int(self.cfg, "max_fragments", 5)
        self.fragment_safety_pad = _cfg_float(self.cfg, "fragment_safety_pad", 0.9)
        self.target_ladder_participation = _cfg_float(self.cfg, "target_ladder_participation", 0.25)
        self.frontload_weights: List[float] = _cfg_list_upper(self.cfg, "frontload_weights", [0.5, 0.35, 0.15])
        self.frontload_group_size = _cfg_int(self.cfg, "frontload_group_size", 3)

        # PATCH (bridge Scanner)
        self._scanner_ref = None  # référence optionnelle au Scanner
        self.get_orderbooks_callback = None  # sera branché par le boot

        # Readiness
        # -------------------- Readiness (PIPELINE_READY vs TRADING_READY) --------------------
        # ready_event           : "loops started" (orchestrateur vivant)
        # pipeline_ready_event  : dépendances OK (engine/books/balances/slip/vol/scanner) => bot opérationnel infra
        # trading_ready_event   : pipeline_ready + autorisation live (armed + kill_switch off + dry_run off) + trading_state READY
        self.ready_event: asyncio.Event = ready_event or asyncio.Event()
        self.pipeline_ready_event: asyncio.Event = asyncio.Event()
        self.trading_ready_event: asyncio.Event = asyncio.Event()

        self._readiness: Dict[str, Any] = {
            "engine": False,
            "books": False,
            "balances": False,
            "scanner": True,
            "slippage": False,
            "volatility": False,
            "pipeline_ready": False,
            "trading_allowed": False,
            "trading_ready": False,
            "reasons": [],
        }
        self._last_books_snapshot_ts: float = 0.0
        self._last_balances_ts: float = 0.0


        # Callback dispatch
        self._cb_tasks: Set[asyncio.Task] = set()
        self._cb_executor = None
        self.fee_buyer = FeeTokenBuyer(self.config, logger=getattr(self, "logger", None))

        # --- RM Mode Overlay (FSM P0) ---
        # rm_mode      : état "business" interne (OPP_VOLUME / OPP_VOL / SEVERE…)
        # trade_mode   : mode consolidé exposé à l'Engine (rm_mode × pacer_mode)
        # pacer_mode   : vue consolidée du Pacer (NORMAL / CONSTRAINED / SEVERE)
        rm_cfg = self._rm_cfg()
        self._switch_knobs = getattr(rm_cfg, "switch_knobs", None)
        self._signal_policy = getattr(rm_cfg, "signal_policy", None)
        if not isinstance(self._switch_knobs, RmSwitchKnobs):
            msg = "rm.switch_knobs missing or invalid; using defaults"
            if self._should_raise_config_conflict():
                raise RuntimeError(msg)
            self._warn_rm_alias("switch_knobs_default", msg)
            self._switch_knobs = RmSwitchKnobs()
        self.rm_mode = "NORMAL"  # NORMAL | OPP_VOLUME | OPP_VOL | SEVERE
        self.trade_mode = "NORMAL"  # NORMAL | CONSTRAINED | SEVERE | OPPORTUNISTE
        self.pacer_mode = "NORMAL"  # NORMAL | CONSTRAINED | SEVERE (injecté par watcher/Pacer)
        self._mode_since = 0.0
        self._mode_timeout_s = float(self._switch_knobs.mode_timeout_s)
        self._opp_volume_timeout_s = float(self._switch_knobs.opp_volume_timeout_s)
        self._enter_hyst_s = int(self._switch_knobs.enter_hyst_s)
        self._exit_hyst_s = int(self._switch_knobs.exit_hyst_s)
        self._last_rm_mode_obs = self.rm_mode
        self._last_trade_mode_obs = self.trade_mode

        # --- Capital ladder / ProfileController (7-RM-2a) ---
        try:
            self._profile_ctrl = ProfileController(
                getattr(self, "pacer", None),
                getattr(self, "cfg", None),
            )
        except Exception:
            # On ne bloque jamais l'init du RM sur la ladder.
            self._profile_ctrl = None

        #

        # Contrat Macro 5 — rm_mode × pacer_mode → trade_mode :
        #   rm_mode     pacer_mode    → trade_mode
        #   ---------------------------------------
        #   SEVERE      *             → SEVERE  (PnL-guard / incidents / infra)
        #   *           SEVERE        → SEVERE  (infra en crise)
        #   *           CONSTRAINED   → CONSTRAINED (infra sous pression)
        #   OPP_VOLUME  NORMAL        → OPPORTUNISTE (volume ↑ sur marché "vert")
        #   OPP_VOL     NORMAL        → OPPORTUNISTE (volatilité exploitée)
        #   sinon       NORMAL        → NORMAL

        # Etat consolidé exposé au moteur (FSM centrale)
        # Domaines : NORMAL / CONSTRAINED / SEVERE / OPPORTUNISTE
        self.trade_mode = "NORMAL"


        # Plancher net (empêche “volume toxique”)
        self._net_floor_bps = float(self._switch_knobs.net_floor_bps)

        # Deltas par mode (défauts sûrs ; profile-aware clamp appliqué plus bas)
        self._overlay = {
            "OPP_VOLUME": {
                "tt_min_bps_delta": float(self._switch_knobs.opp_volume_tt_min_bps_delta),
                "tm_min_bps_delta": float(self._switch_knobs.opp_volume_tm_min_bps_delta),
                "cap_factor": float(self._switch_knobs.opp_volume_cap_factor),
                "mm_enable": bool(self._switch_knobs.opp_volume_mm_enable),
            },
            "OPP_VOL": {
                "tt_min_bps_delta": float(self._switch_knobs.opp_vol_tt_min_bps_delta),
                "tm_min_bps_delta": float(self._switch_knobs.opp_vol_tm_min_bps_delta),
                "cap_factor": float(self._switch_knobs.opp_vol_cap_factor),
                "mm_enable": bool(self._switch_knobs.opp_vol_mm_enable),
            },
            "SEVERE": {
                "tt_min_bps_delta": float(self._switch_knobs.severe_tt_min_bps_delta),
                "tm_min_bps_delta": float(self._switch_knobs.severe_tm_min_bps_delta),
                "cap_factor": float(self._switch_knobs.severe_cap_factor),
                "mm_enable": bool(self._switch_knobs.severe_mm_enable),
                "ioc_only": bool(self._switch_knobs.severe_ioc_only),
            },
        }
        # Overlay consolidé par trade_mode (normalisé, clamp down uniquement)
        self._overlay_by_trade_mode = {
            "NORMAL": {
                "tt_min_bps_delta": float(self._switch_knobs.normal_tt_min_bps_delta),
                "tm_min_bps_delta": float(self._switch_knobs.normal_tm_min_bps_delta),
                "cap_factor": float(self._switch_knobs.normal_cap_factor),
                "ioc_only": bool(self._switch_knobs.normal_ioc_only),
            },
            "CONSTRAINED": {
                "tt_min_bps_delta": float(self._switch_knobs.constr_tt_min_bps_delta),
                "tm_min_bps_delta": float(self._switch_knobs.constr_tm_min_bps_delta),
                "cap_factor": float(self._switch_knobs.constr_cap_factor),
                "mm_enable": bool(self._switch_knobs.constr_mm_enable),
                "ioc_only": bool(self._switch_knobs.constr_ioc_only),
            },
            "SEVERE": dict(self._overlay.get("SEVERE", {})),
            "OPPORTUNISTE": {},  # fusionné avec le sous-mode opportuniste (OPP_VOLUME / OPP_VOL)
        }

        # --- PnL guard (config & état) ---
        self._pnl_guard_lvl1 = float(self._switch_knobs.pnl_guard_day_lvl1_pct)  # %
        self._pnl_guard_lvl2 = float(self._switch_knobs.pnl_guard_day_lvl2_pct)  # %
        self._pnl_cooldown_s = int(self._switch_knobs.pnl_cooldown_s)  # 30 min
        self._last_bad_ts = 0.0
        # --- Balances TTL (MBF → RM) ----------------------------------------
        # Paramètres RM côté config (en secondes). Defaults à ajuster dans BotConfig
        # mais on met des valeurs safe par défaut ici.
        self._balance_ttl_s_normal = float(self._resolve_rm_param(
            ("balance_ttl_s_normal", "RM_BALANCE_TTL_S_NORMAL"), 60.0
        ))
        self._balance_ttl_s_degraded = float(self._resolve_rm_param(
            ("balance_ttl_s_degraded", "RM_BALANCE_TTL_S_DEGRADED"), 180.0
        ))
        self._balance_ttl_s_block = float(self._resolve_rm_param(
            ("balance_ttl_s_block", "RM_BALANCE_TTL_S_BLOCK"), 600.0
        ))

        # Cache local par (exchange, alias) pour l’âge et le statut TTL.
        # Clés toujours en UPPER pour être robustes.
        self._alias_balance_age_s: Dict[Tuple[str, str], float] = {}
        self._alias_balance_status: Dict[Tuple[str, str], str] = {}
        # Vue consolidée (balances SLO + WS) projetée depuis MBF.meta[].
        self._alias_private_health: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self.alias_collat_state: Dict[Tuple[str, str], Dict[str, Any]] = {}

        # Cache local des statuts comptes WS (Hub + Reconciler) vus via MBF.as_rm_snapshot().
        # Clés: (EX, ALIAS) en UPPER. Valeur: dict(meta_ws) incluant capital_at_risk/hub_status/reco_status...
        self._alias_ws_accounts_status: Dict[Tuple[str, str], Dict[str, Any]] = {}

        # Throttle pour les demandes de resync balances ciblées (par alias)
        self._alias_last_resync_request_ts: Dict[Tuple[str, str], float] = {}
        # Coalescence : une seule task de resync en vol par alias
        self._alias_resync_inflight: Set[Tuple[str, str]] = set()

        # Suivi de dérive entre capital virtuel (RM) et réel (MBF)
        self._last_capital_drift_pct: float = 0.0
        # --- Capital move overlay (transferts internes) --------------------
        self._capital_move_threshold_usdc = float(
            getattr(self.cfg, "RM_CAPITAL_MOVE_THRESHOLD_USDC", 0.0)
        )
        self._capital_move_refresh_max_delay_s = float(
            getattr(self.cfg, "RM_CAPITAL_MOVE_REFRESH_MAX_DELAY_S", 15.0)
        )
        self._capital_move_refresh_mode = str(
            getattr(self.cfg, "RM_CAPITAL_MOVE_REFRESH_MODE", "alias_only")
        ).upper()

        # Cache local des fenêtres "capital en mouvement" par (EXCHANGE, ALIAS)
        # Clés en UPPER, valeurs = dict(state) avec start_ts / deadline_ts / last_notional_usdc.
        self._alias_capital_move_state: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._transfer_alias_index: Dict[str, Set[Tuple[str, str]]] = {}

    # ------------------------------------------------------------------
    # Hooks d'observabilité / mute de routes (callbacks injectés par Boot)
    # ------------------------------------------------------------------

    def set_obs_inc_callback(self, cb: Optional[Callable[[str, Any], Any]]) -> None:
        """
        Enregistre un callback pour l'incrément de métriques (obs_inc).

        Exemple typique dans le Boot :
            rm.set_obs_inc_callback(obs.inc)
        """
        self._obs_inc_cb = cb

    def obs_inc(self, metric: str, **labels: Any) -> None:
        """
        Incrémente une métrique d'observabilité.

        Délègue à `_obs_inc_cb` si présent et ne laisse jamais remonter d'exception.
        Si aucun callback n'est configuré, c'est un no-op.
        """
        cb = getattr(self, "_obs_inc_cb", None)
        if cb is None:
            return
        try:
            cb(metric, **labels)
        except Exception:
            # Pure obs : jamais bloquant
            return

    def set_mute_route_callback(self, cb: Optional[Callable[..., Any]]) -> None:
        """
        Enregistre un callback pour muter une route (buy_ex, sell_ex, pair).

        Exemple dans le Boot :
            rm.set_mute_route_callback(router.mute_route_for)
        """
        self._mute_route_cb = cb

    def mute_route_for(
        self,
        buy_ex: str,
        sell_ex: str,
        pair: str,
        *,
        ttl_s: float | int,
        reason: Optional[str] = None,
    ) -> None:
        """
        Mute temporairement une route (buy_ex, sell_ex, pair) en déléguant à `_mute_route_cb`
        si celui-ci est défini. Sinon, no-op.
        """
        cb = getattr(self, "_mute_route_cb", None)
        if cb is None:
            return
        try:
            cb(buy_ex, sell_ex, pair, ttl_s=ttl_s, reason=reason)
        except Exception:
            # Ne jamais bloquer le RM à cause d'un problème de mute côté Router.
            return


    @property
    def effective_inventory_cap_usd(self) -> float:
        """
        Cap global d'inventaire aligné sur les buffers MBF.

        - self.inventory_cap_usd reste la valeur de configuration (profil capital).
        - On ne laisse jamais ce cap dépasser le capital observable dans les
          poches de quote (USDC/USD/USDT) retournées par MBF.as_buffers_snapshot().
        """
        base_cap = float(getattr(self, "inventory_cap_usd", 0.0) or 0.0)

        try:
            buffers_cap = float(self._compute_capital_available_usdc_from_buffers())
        except Exception:
            buffers_cap = 0.0

        if buffers_cap > 0.0:
            # Cap effectif borné par le capital réellement observable.
            return min(base_cap, buffers_cap)
        return base_cap

    def _sync_reserved_metrics(self) -> None:
        """Met à jour la jauge RM_RESERVED_FUNDS_ACTIVE à partir des réservations en cours."""
        if not getattr(self, "_reserved_balances", None):
            return

        totals: Dict[Tuple[str, str, str], float] = {}
        for res in self._reserved_balances.values():
            key = (res["exchange"], res["alias"], res["ccy"])
            totals[key] = totals.get(key, 0.0) + res["amount"]

        # On devrait idéalement pouvoir faire un reset() mais Prometheus ne le permet pas facilement par label.
        # On va mettre à 0 les clés connues qui ne sont plus dans totals si on avait un cache, 
        # mais ici on va juste mettre à jour les actifs. 
        # Pour faire propre, on peut garder la liste des labels déjà vus.
        if not hasattr(self, "_reserved_metrics_seen"):
            self._reserved_metrics_seen: Set[Tuple[str, str, str]] = set()

        for key, val in totals.items():
            try:
                RM_RESERVED_FUNDS_ACTIVE.labels(exchange=key[0], alias=key[1], ccy=key[2]).set(val)
                self._reserved_metrics_seen.add(key)
            except Exception:
                pass

        # Clean up des anciens qui sont tombés à 0
        for key in list(self._reserved_metrics_seen):
            if key not in totals:
                try:
                    RM_RESERVED_FUNDS_ACTIVE.labels(exchange=key[0], alias=key[1], ccy=key[2]).set(0.0)
                    self._reserved_metrics_seen.remove(key)
                except Exception:
                    pass

    def _apply_reservations_to_pockets(self, pockets: Dict[str, Any]) -> Dict[str, Any]:
        """
        Applique les réservations de balance (Ticket P0-RM-CAPMOVE-01) sur un dictionnaire 'pockets_by_quote'.
        Utilise effective_available = max(0, available - reserved).
        """
        if not pockets or not getattr(self, "_reserved_balances", None):
            return pockets

        now = time.time()
        # On travaille sur une copie profonde pour éviter de polluer le cache MBF si passé par ref
        import copy
        pockets = copy.deepcopy(pockets)

        for tid, res in list(self._reserved_balances.items()):
            # Nettoyage auto des expirés au passage
            if now > res.get("deadline", 0):
                self._reserved_balances.pop(tid, None)
                continue
        
        self._sync_reserved_metrics()

        for tid, res in self._reserved_balances.items():
            ex = res["exchange"]
            alias = res["alias"]
            ccy = res["ccy"]
            amount = res["amount"]

            if ex in pockets and alias in pockets[ex] and ccy in pockets[ex][alias]:
                node = pockets[ex][alias][ccy]
                if isinstance(node, dict):
                    orig = float(node.get("available", node.get("total", 0.0)))
                    node["available"] = max(0.0, orig - amount)
                else:
                    orig = float(node or 0.0)
                    pockets[ex][alias][ccy] = max(0.0, orig - amount)
        return pockets

    def _refresh_capital_from_buffers(self, *, mode: str = "merged") -> None:
        """
        Helper interne : rafraîchit la vue capital à partir du MultiBalanceFetcher.

        Source unique : MultiBalanceFetcher.as_buffers_snapshot()
        - Remplit self.capital_buffers[exchange][alias][quote] = available
        - Met à jour self.capital_available_usdc (somme des quotes USDC)
        - Met à jour last_capital_update_ts
        """
        try:
            snapshot = self.mbf.as_buffers_snapshot(mode=mode)
        except Exception as exc:
            self.logger.warning(f"[RM] _refresh_capital_from_buffers: échec snapshot MBF: {exc}")
            return

        pockets = snapshot.get("pockets_by_quote", {}) or {}
        # Ticket P0-RM-CAPMOVE-01: Appliquer les réservations (virtual debit)
        pockets = self._apply_reservations_to_pockets(pockets)

        capital_buffers: Dict[str, Dict[str, Dict[str, float]]] = {}
        total_usdc = 0.0

        for ex, aliases in pockets.items():
            ex_map = capital_buffers.setdefault(ex, {})
            for alias, quotes in (aliases or {}).items():
                alias_map = ex_map.setdefault(alias, {})
                for quote, qdata in (quotes or {}).items():
                    if isinstance(qdata, dict):
                        available = float((qdata or {}).get("available", qdata.get("total", 0.0)) or 0.0)
                    else:
                        available = float(qdata or 0.0)
                    alias_map[quote] = available
                    if str(quote).upper() == "USDC":
                        total_usdc += available

        self.capital_buffers = capital_buffers
        self.capital_available_usdc = total_usdc
        self.last_capital_update_ts = time.time()
        self.logger.debug(
            "[RM] Capital buffers mis à jour via MBF: "
            f"mode={mode}, total_usdc={total_usdc:.2f}"
        )

        # Met à jour également la vue collat/marge par alias.
        try:
            self._refresh_alias_collat_from_buffers(snapshot)
        except Exception:
            self.logger.exception("[RM] _refresh_alias_collat_from_buffers failed")
        try:
            self._refresh_alias_collat_from_mbf(now=self.last_capital_update_ts)
        except Exception:
            self.logger.debug("[RM] _refresh_alias_collat_from_mbf failed", exc_info=False)

    def _refresh_alias_collat_from_buffers(self, snapshot: Dict[str, Any]) -> None:
        """
        Construit une vue collat/marge par (exchange, alias) à partir de as_buffers_snapshot().

        - collat_usd : somme des quotes considérées comme collat (cfg.rm.collat_quotes)
        - min_usd    : minimum de sécurité (collat_default_min_usd ou override par alias)
        - ratio      : collat_usd / min_usd
        - state      : "OK" | "LOW" | "CRIT" selon les thresholds de config
        """
        import time as _time

        rm_cfg = getattr(self.cfg, "rm", None)
        if rm_cfg is None:
            return

        collat_quotes = getattr(rm_cfg, "collat_quotes", None) or ["USDC", "USDT", "USD", "EUR"]
        collat_quotes = [str(q).upper() for q in collat_quotes if q]

        default_min = float(getattr(rm_cfg, "collat_default_min_usd", 500.0) or 0.0)
        ratio_low = float(getattr(rm_cfg, "collat_ratio_low", 1.1) or 1.1)
        ratio_crit = float(getattr(rm_cfg, "collat_ratio_crit", 1.0) or 1.0)

        alias_overrides = getattr(rm_cfg, "collat_alias_overrides", {}) or {}

        pockets = snapshot.get("pockets_by_quote", {}) or {}
        as_of_ts = float(snapshot.get("as_of_ts") or _time.time())

        out: Dict[Tuple[str, str], Dict[str, Any]] = {}

        for ex, aliases in pockets.items():
            ex_u = str(ex or "").upper()
            if not ex_u:
                continue
            for alias, quotes in (aliases or {}).items():
                alias_u = str(alias or "").upper()
                if not alias_u:
                    continue

                collat_usd = 0.0
                for quote, qdata in (quotes or {}).items():
                    q_u = str(quote or "").upper()
                    if q_u not in collat_quotes:
                        continue
                    try:
                        available = float((qdata or {}).get("available", 0.0) or 0.0)
                    except Exception:
                        available = 0.0
                    if available > 0.0:
                        collat_usd += available

                key = f"{ex_u}.{alias_u}"
                ov = alias_overrides.get(key) or {}
                min_usd = float(ov.get("min_usd", default_min) or 0.0)

                if min_usd <= 0.0:
                    ratio = float("inf")
                    state = "OK"
                else:
                    ratio = collat_usd / min_usd
                    if ratio < ratio_crit:
                        state = "CRIT"
                    elif ratio < ratio_low:
                        state = "LOW"
                    else:
                        state = "OK"

                out[(ex_u, alias_u)] = {
                    "collat_usd": collat_usd,
                    "min_usd": min_usd,
                    "ratio": ratio,
                    "state": state,
                    "last_update_ts": as_of_ts,
                }

        self.alias_collat_state = out


    def update_capital_from_mbf(self, *, mode: str = "merged") -> None:
        """
        Wrapper public pour compatibilité ascendante.
        Préférer l'usage interne de _refresh_capital_from_buffers().
        """
        self._refresh_capital_from_buffers(mode=mode)

    def _refresh_alias_collat_from_mbf(self, now: float | None = None) -> None:
        """Rafraîchit la vue collat/marge par alias depuis le MBF."""
        glue = getattr(self, "_mbf_glue", None) or getattr(self, "mbf", None)
        if not glue or not hasattr(glue, "as_buffers_snapshot"):
            return

        try:
            snapshot = glue.as_buffers_snapshot(mode="merged", cached_only=True)
        except Exception as exc:
            self.logger.debug("[RM] collat snapshot MBF failed: %s", exc, exc_info=False)
            return

        pockets = snapshot.get("pockets_by_quote", {}) or {}
        meta = snapshot.get("meta") or {}
        age_s = None
        try:
            age_s = float(meta.get("age_s")) if meta.get("age_s") is not None else None
        except Exception:
            age_s = None

        cfg_rm = getattr(self.cfg, "rm", None)
        min_default = float(getattr(cfg_rm, "collat_default_min_usd", 0.0) or 0.0)
        ratio_warn = float(getattr(cfg_rm, "collat_ratio_warn", 1.1) or 1.1)
        ratio_crit = float(getattr(cfg_rm, "collat_ratio_crit", 1.0) or 1.0)
        overrides = getattr(cfg_rm, "collat_alias_overrides", {}) or {}
        collat_quotes = [str(q).upper() for q in (getattr(cfg_rm, "collat_quotes", []) or [])]
        now_ts = float(now) if now is not None else time.time()

        for ex, aliases in pockets.items():
            for alias, quotes in (aliases or {}).items():
                ex_u = str(ex).upper()
                alias_u = str(alias).upper()
                collat_usd = 0.0
                for quote, value in (quotes or {}).items():
                    if str(quote).upper() in collat_quotes:
                        try:
                            collat_usd += float(value or 0.0)
                        except Exception:
                            collat_usd += 0.0

                key = f"{ex_u}.{alias_u}"
                override = overrides.get(key, {}) if isinstance(overrides, dict) else {}
                min_usd = float(override.get("min_usd", min_default) or 0.0)

                if min_usd <= 0:
                    ratio = float("inf")
                    state = "OK"
                else:
                    ratio = collat_usd / min_usd if min_usd else float("inf")
                    if ratio < ratio_crit:
                        state = "CRITICAL"
                    elif ratio < ratio_warn:
                        state = "LOW"
                    else:
                        state = "OK"

                state_val = {"OK": 0, "LOW": 1, "CRITICAL": 2}.get(state, 0)

                self.alias_collat_state[(ex_u, alias_u)] = {
                    "collat_usd": collat_usd,
                    "min_usd": min_usd,
                    "ratio": ratio,
                    "state": state,
                    "age_s": age_s,
                    "last_update_ts": now_ts,
                }

                try:
                    RM_ALIAS_COLLAT_RATIO.labels(exchange=ex_u, alias=alias_u).set(ratio)
                    RM_ALIAS_COLLAT_STATE.labels(exchange=ex_u, alias=alias_u).set(state_val)
                except Exception:
                    pass

    def _refresh_mm_delta_from_balances(
            self,
            now: float | None = None,
            snapshot_rm: Optional[dict] = None,
    ) -> None:
        """
        Mesure le delta global MM par asset en USD-like à partir du snapshot MBF.
        """
        raw = snapshot_rm
        glue = getattr(self, "_mbf_glue", None) or getattr(self, "mbf", None)
        if raw is None and glue is None:
            return

        if raw is None:
            try:
                if hasattr(glue, "as_rm_snapshot"):
                    try:
                        raw = glue.as_rm_snapshot(mode="merged", cached_only=True)
                    except TypeError:
                        raw = glue.as_rm_snapshot(cached_only=True)
                elif hasattr(glue, "snapshot"):
                    raw = glue.snapshot(mode="merged", cached_only=True)
                else:
                    return
            except Exception as exc:
                self.logger.debug("[RM] mm_delta snapshot MBF failed: %s", exc, exc_info=False)
                return


        if not isinstance(raw, dict) or not raw:
            return

        positions = raw.get("positions") if isinstance(raw.get("positions"), dict) else None
        if positions is None:
            balances = raw.get("balances") if isinstance(raw.get("balances"), dict) else None
            if balances:
                positions = {
                    (ex, alias, asset): {"net": qty}
                    for ex, per_alias in balances.items()
                    for alias, assets in (per_alias or {}).items()
                    for asset, qty in (assets or {}).items()
                }
        if not positions:
            return

        cfg_rm = getattr(self.cfg, "rm", None)
        delta_usd_by_asset: dict[str, float] = defaultdict(float)

        for key, data in positions.items():
            if not isinstance(data, dict):
                continue
            if isinstance(key, tuple) and len(key) >= 3:
                ex, alias, asset = key[0], key[1], key[2]
            else:
                ex = data.get("exchange")
                alias = data.get("alias")
                asset = data.get("asset") or (key if isinstance(key, str) else None)

            if not self._is_mm_alias(ex, alias):
                continue

            asset_u = str(asset or "").upper()
            if not asset_u:
                continue

            try:
                delta_usd = float(data.get("net_usd"))
            except Exception:
                try:
                    delta_usd = float(data.get("net"))
                except Exception:
                    continue

            delta_usd_by_asset[asset_u] += delta_usd

        if not delta_usd_by_asset:
            return

        now_ts = float(now) if now is not None else float(raw.get("as_of_ts") or time.time())
        default_soft = float(getattr(cfg_rm, "mm_delta_soft_usd", 0.0) or 0.0)
        default_hard = float(getattr(cfg_rm, "mm_delta_hard_usd", 0.0) or 0.0)
        overrides = getattr(cfg_rm, "mm_delta_by_asset", {}) or {}

        mm_state: dict[str, dict] = {}
        for asset, d_usd in delta_usd_by_asset.items():
            override = overrides.get(asset) or overrides.get(asset.upper()) or {}
            soft_limit = float(override.get("soft_usd", default_soft) or default_soft)
            hard_limit = float(override.get("hard_usd", default_hard) or default_hard)
            abs_delta = abs(d_usd)

            state = "OK"
            if hard_limit > 0 and abs_delta > hard_limit:
                state = "HARD"
            elif soft_limit > 0 and abs_delta > soft_limit:
                state = "SOFT"

            state_val = {"OK": 0, "SOFT": 1, "HARD": 2}.get(state, 0)

            mm_state[asset] = {
                "delta_usd": d_usd,
                "soft_usd": soft_limit,
                "hard_usd": hard_limit,
                "state": state,
                "last_update_ts": now_ts,
            }

            try:
                RM_MM_DELTA_USD.labels(asset=asset).set(d_usd)
                RM_MM_DELTA_STATE.labels(asset=asset).set(state_val)
            except Exception:
                pass

        self.mm_delta_state = mm_state

    def _is_mm_alias(self, exchange: Any, alias: Any) -> bool:
        alias_u = str(alias or "").upper()
        if not alias_u:
            return False
        cfg_mm_alias = str(getattr(self, "mm_alias", "MM") or "MM").upper()
        cfg_mm_mono = str(getattr(self, "mm_mono_alias_name", "MM_MONO") or "MM_MONO").upper()
        cfg_mm_cross = str(getattr(self, "mm_cross_alias_name", "MM_CROSS") or "MM_CROSS").upper()
        return "MM" in alias_u or alias_u in {cfg_mm_alias, cfg_mm_mono, cfg_mm_cross}

    def _rm_mm_delta_status_for_asset(self, asset: str) -> Optional[Dict[str, Any]]:
        """
        Retourne l'état mm_delta pour un asset (upper) ou None si absent.
        """
        if not asset:
            return None
        a = str(asset).upper()
        return self.mm_delta_state.get(a)

    def _update_mm_delta_from_event(self, event: Dict[str, Any]) -> None:
        try:
            meta = event.get("meta") or {}
            branch = str(meta.get("branch") or meta.get("strategy") or meta.get("kind") or "").upper()
            kind = str(meta.get("kind") or "").upper()
            if not self._is_mm_family(branch, kind):
                return

            symbol = event.get("symbol") or meta.get("pair") or meta.get("symbol")
            pk = self._norm_pair(symbol or "")
            if not pk:
                return
            base_asset = self._pair_base(pk)
            if not base_asset:
                return

            try:
                fill_px = float(event.get("fill_px") or meta.get("fill_px") or 0.0)
                base_qty = float(event.get("base_qty") or meta.get("base_qty") or 0.0)
            except Exception:
                fill_px = 0.0
                base_qty = 0.0
            if fill_px <= 0 or base_qty == 0:
                return

            side = str(event.get("side") or meta.get("side") or "").upper()
            signed_delta = base_qty * fill_px
            if side == "SELL":
                signed_delta *= -1.0

            prev_state = self.mm_delta_state.get(base_asset, {}) if isinstance(self.mm_delta_state, dict) else {}
            delta_usd = float(prev_state.get("delta_usd") or 0.0) + signed_delta
            delta_sq = float(prev_state.get("delta_sq_usd2") or 0.0) + signed_delta ** 2

            cfg_rm = getattr(self.cfg, "rm", None)
            default_soft = float(getattr(cfg_rm, "mm_delta_soft_usd", 0.0) or 0.0)
            default_hard = float(getattr(cfg_rm, "mm_delta_hard_usd", 0.0) or 0.0)
            overrides = getattr(cfg_rm, "mm_delta_by_asset", {}) or {}
            override = overrides.get(base_asset) or overrides.get(base_asset.upper()) or {}
            soft_limit = float(override.get("soft_usd", default_soft) or default_soft)
            hard_limit = float(override.get("hard_usd", default_hard) or default_hard)

            abs_delta = abs(delta_usd)
            state_simple = "OK"
            status = "FLAT"
            if hard_limit > 0 and abs_delta > hard_limit:
                state_simple = "HARD"
                status = "LONG_HARD" if delta_usd > 0 else "SHORT_HARD"
            elif soft_limit > 0 and abs_delta > soft_limit:
                state_simple = "SOFT"
                status = "LONG_SOFT" if delta_usd > 0 else "SHORT_SOFT"
            elif delta_usd != 0:
                status = "LONG_SOFT" if delta_usd > 0 else "SHORT_SOFT"

            state_val = {"OK": 0, "SOFT": 1, "HARD": 2}.get(state_simple, 0)
            now_ts = float(event.get("ts") or event.get("ts_local") or time.time())
            self.mm_delta_state[base_asset] = {
                "delta_usd": delta_usd,
                "delta_sq_usd2": delta_sq,
                "soft_usd": soft_limit,
                "hard_usd": hard_limit,
                "status": status,
                "state": state_simple,
                "last_update_ts": now_ts,
            }

            try:
                RM_MM_DELTA_USD.labels(asset=base_asset).set(delta_usd)
                RM_MM_DELTA_STATE.labels(asset=base_asset).set(state_val)
            except Exception:
                pass
        except Exception:
            try:
                logger.debug("[RM] mm_delta_from_event failed", exc_info=False)
            except Exception:
                pass

    def _register_tt_stuck_leg(self, leg_info: Dict[str, Any]) -> None:
        """
        Enregistre un leg TT coincé dans tt_stuck_state.

        leg_info attendu:
          - asset: str           (ex: "ETH")
          - side: "BUY"|"SELL"
          - notional_usd: float  (exposition de ce leg en USD-like)
          - exchange: str
          - alias: str
          - pair: str            (ex: "ETHUSDC")
          - created_ts: float    (epoch seconds)
        """
        asset = str(leg_info.get("asset") or "").upper()
        if not asset:
            return

        side = str(leg_info.get("side") or "").upper()
        notional_usd = float(leg_info.get("notional_usd") or 0.0)
        if notional_usd <= 0.0:
            return

        # Sign convention: BUY = +, SELL = -
        signed = notional_usd if side == "BUY" else -notional_usd
        state = self.tt_stuck_state.get(asset)
        if not state:
            state = {"delta_usd": 0.0, "legs": [], "last_update_ts": 0.0}
            self.tt_stuck_state[asset] = state

        state["legs"].append(dict(leg_info))
        state["delta_usd"] += signed
        state["last_update_ts"] = float(leg_info.get("created_ts") or time.time())

    def _gc_tt_stuck_legs(self, now: float) -> None:
        """
        GC des legs TT coincés:
          - supprime ceux dont l'âge > cfg.rm.tt_stuck_max_age_s,
          - recalcule delta_usd par asset.
        """
        max_age = float(getattr(getattr(self.cfg, "rm", None), "tt_stuck_max_age_s", 0.0) or 0.0)
        rm_cfg = getattr(self, "cfg", None)
        rm_section = getattr(rm_cfg, "rm", None)
        max_age = float(getattr(rm_section, "tt_stuck_max_age_s", 10.0) or 0.0)
        if max_age <= 0.0:
            return

        for asset, state in list(self.tt_stuck_state.items()):
            legs = state.get("legs") or []
            kept: List[Dict[str, Any]] = []
            delta_usd = 0.0

        for leg in legs:
            created_ts = float(leg.get("created_ts") or 0.0)
            age_s = max(0.0, now - created_ts)
            if age_s > max_age:
                continue

            side = str(leg.get("side") or "").upper()
            notional_usd = float(leg.get("notional_usd") or 0.0)
            if notional_usd <= 0.0:
                continue
            signed = notional_usd if side == "BUY" else -notional_usd

            kept.append(leg)
            delta_usd += signed

        if not kept:
            # plus de legs pertinents -> on purge l'asset
            self.tt_stuck_state.pop(asset, None)
        else:
            state["legs"] = kept
            state["delta_usd"] = delta_usd
            state["last_update_ts"] = now

    def _refresh_tm_exposure_ttl_from_slo(self) -> None:
        cfg = getattr(self, "cfg", None)
        slo_map = getattr(cfg, "slo", None)
        self.tm_exposure_ttl_ms_by_exchange = {}

        if slo_map is None:
            return

        mode_key = str(getattr(getattr(cfg, "g", None), "deployment_mode", "SPLIT") or "SPLIT").upper()
        per_ex = slo_map.get(mode_key) or {}

        for ex, path_slo in per_ex.items():
            pvt = getattr(path_slo, "private", None)
            if pvt is None:
                continue
            ttl_ms = int(getattr(pvt, "tm_exposure_ttl_ms", 0) or 0)
            if ttl_ms <= 0:
                continue
            self.tm_exposure_ttl_ms_by_exchange[str(ex).upper()] = ttl_ms

        if not self.tm_exposure_ttl_ms_by_exchange and not self._tm_ttl_slo_warned:
            self._tm_ttl_slo_warned = True
            try:
                logger.warning(
                    "[RiskManager] tm_exposure_ttl_ms SLO absent pour mode=%s, fallback sur config globale",
                    mode_key,
                )
            except Exception:
                pass

        self._push_tm_exposure_ttl_metrics()

    def _push_tm_exposure_ttl_metrics(self) -> None:
        try:
            from . import obs_metrics  # type: ignore
        except Exception:
            return

        metric = getattr(obs_metrics, "RM_TM_EXPOSURE_TTL_MS", None)
        if metric is None:
            return

        ttl_default = int(getattr(self, "tm_exposure_ttl_ms", 0) or 0)

        for ex_u, ttl_ms in (self.tm_exposure_ttl_ms_by_exchange or {}).items():
            try:
                metric.labels(exchange=ex_u).set(float(ttl_ms))
            except Exception:
                continue

        try:
            metric.labels(exchange="DEFAULT").set(float(ttl_default))
        except Exception:
            pass

    def _get_tm_exposure_ttl_ms_for_exchange(self, exchange: str) -> int:
        ex_u = str(exchange or "").upper()
        try:
            return int(self.tm_exposure_ttl_ms_by_exchange.get(ex_u, self.tm_exposure_ttl_ms))
        except Exception:
            return int(getattr(self, "tm_exposure_ttl_ms", 0) or 0)

    def _refresh_tm_inflight_exposures(self, now: float) -> None:
        """Met à jour l'état des expositions TM inflight et leur fraîcheur SLO."""

        for asset, entries in list(self.tm_inflight_exposures.items()):
            kept: list[Dict[str, Any]] = []
            for entry in entries or []:
                opened_ts = float(entry.get("opened_ts") or entry.get("created_ts") or 0.0)
                max_expo_s = float(entry.get("max_exposure_s") or 0.0)
                age_s = max(0.0, now - opened_ts) if opened_ts > 0 else 0.0
                entry["age_s"] = age_s

                ttl_ms = self._get_tm_exposure_ttl_ms_for_exchange(entry.get("exchange"))
                ttl_s = max(0.0, float(ttl_ms) / 1000.0)
                entry["ttl_s"] = ttl_s
                entry["stale"] = bool(ttl_s > 0.0 and age_s > ttl_s)

                if max_expo_s > 0.0 and age_s > max_expo_s:
                    continue
                kept.append(entry)

            if kept:
                self.tm_inflight_exposures[asset] = kept
            else:
                self.tm_inflight_exposures.pop(asset, None)

    def _refresh_tttm_exposure_state(self, now: float) -> None:
        """
                Reconstruit tttm_exposure_state[asset] à partir de:
                  - tt_stuck_state (legs TT coincés)
                  - tm_inflight_exposures (P0: peut rester vide)

                Classe chaque asset en OK/SOFT/HARD selon les seuils de config.
                """
        rm_cfg = getattr(self.cfg, "rm", None)
        if rm_cfg is None:
            return

        # GC des stuck legs d'abord
        self._gc_tt_stuck_legs(now)
        try:
            self._refresh_tm_inflight_exposures(now)
        except Exception:
            if getattr(self, "logger", None):
                self.logger.debug("[RM] refresh_tm_inflight_exposures failed", exc_info=False)

        result: Dict[str, Dict[str, Any]] = {}
        # 1) Atomes TT
        assets = set(self.tt_stuck_state.keys()) | set(self.tm_inflight_exposures.keys())


        for asset in assets:
            asset_u = str(asset).upper()
            prev_state = self.tttm_exposure_state.get(asset_u) or {}
            prev_soft_breach = bool(prev_state.get("soft_breach"))
            prev_hard_breach = bool(prev_state.get("hard_breach"))

            # TT component
            tt_state = self.tt_stuck_state.get(asset_u) or {}
            tt_delta_usd = float(tt_state.get("delta_usd") or 0.0)

            # TM component (P0: somme notional_usd * signe)
            tm_list = self.tm_inflight_exposures.get(asset_u) or []
            tm_delta_usd = 0.0
            stale_notional_usd = 0.0
            stale_exposures: list[Dict[str, Any]] = []
            breach_by_exchange: Dict[str, float] = {}
            for entry in tm_list:
                notional_usd = float(entry.get("notional_usd") or 0.0)
                side = str(entry.get("side") or "").upper()
                if notional_usd <= 0.0:
                    continue
                signed = notional_usd if side in ("LONG", "BUY") else -notional_usd
                tm_delta_usd += signed
                if bool(entry.get("stale")):
                    stale_notional_usd += abs(notional_usd)
                    ex_u = str(entry.get("exchange") or "").upper()
                    if ex_u:
                        breach_by_exchange[ex_u] = breach_by_exchange.get(ex_u, 0.0) + abs(notional_usd)
                    stale_exposures.append(entry)

            delta_usd = tt_delta_usd + tm_delta_usd
            # Seuils de config
            overrides = getattr(rm_cfg, "tttm_exposure_by_asset", {}) or {}
            ov = overrides.get(asset_u) or {}
            soft_default = float(getattr(rm_cfg, "tttm_exposure_soft_usd", 2000.0) or 0.0)
            hard_default = float(getattr(rm_cfg, "tttm_exposure_hard_usd", 5000.0) or 0.0)
            soft = float(ov.get("soft_usd", soft_default) or 0.0)
            hard = float(ov.get("hard_usd", hard_default) or 0.0)

            soft_breach = soft > 0.0 and stale_notional_usd > soft
            hard_breach = hard > 0.0 and stale_notional_usd > hard

            abs_d = abs(delta_usd)
            if hard_breach or (hard > 0.0 and abs_d > hard):
                state = "HARD"
            elif soft_breach or (soft > 0.0 and abs_d > soft):
                state = "SOFT"
            else:
                state = "OK"

            result[asset_u] = {
                "tt_delta_usd": tt_delta_usd,
                "tm_delta_usd": tm_delta_usd,
                "delta_usd": delta_usd,
                "soft_usd": soft,
                "hard_usd": hard,
                "state": state,
                "soft_breach": soft_breach,
                "hard_breach": hard_breach,
                "stale_notional_usd": stale_notional_usd,
                "stale_exposures": stale_exposures,
                "last_update_ts": now,
            }
            # Metrics optionnelles (si infra metrics déjà en place)
            try:
                self.metrics.gauge(
                    "RM_TTTM_DELTA_USD",
                    delta_usd,
                    tags={"asset": asset_u},
                )
                state_int = 0 if state == "OK" else (1 if state == "SOFT" else 2)
                self.metrics.gauge(
                    "RM_TTTM_DELTA_STATE",
                    state_int,
                    tags={"asset": asset_u},
                )
            except Exception:
                pass
            try:
                from . import obs_metrics  # type: ignore
                ttl_breach_metric = getattr(obs_metrics, "RM_TM_EXPOSURE_TTL_BREACH_TOTAL", None)
            except Exception:
                ttl_breach_metric = None

            if ttl_breach_metric:
                if hard_breach and not prev_hard_breach:
                    for ex_u in breach_by_exchange or {}:
                        ttl_breach_metric.labels(exchange=ex_u, asset=asset_u, level="hard").inc()
                elif soft_breach and not prev_soft_breach:
                    for ex_u in breach_by_exchange or {}:
                        ttl_breach_metric.labels(exchange=ex_u, asset=asset_u, level="soft").inc()

            try:
                RM_TTTM_DELTA_USD.labels(asset=asset_u).set(delta_usd)
                state_int = 0 if state == "OK" else (1 if state == "SOFT" else 2)
                RM_TTTM_DELTA_STATE.labels(asset=asset_u).set(state_int)
            except Exception:
                pass
        self.tttm_exposure_state = result

    def _tttm_state_for_asset(self, asset: str) -> Optional[Dict[str, Any]]:
        if not asset:
            return None
        return self.tttm_exposure_state.get(str(asset).upper())

    def _pick_mm_hedge_venue(self, asset: str) -> tuple[str, str, str] | None:
        """
        Retourne (exchange, alias, symbol) pour exécuter un hedge MM sur 'asset'.
        Filtre les alias en CRITICAL, applique mm_hedge_allowed_exchanges s'il est rempli,
        et privilégie la plus grosse position (sinon le plus de collat_usd).
        """
        asset_u = str(asset or "").upper()
        if not asset_u:
            return None

        balances = getattr(self, "_last_balances", {}) or {}
        collat_map = dict(getattr(self, "alias_collat_state", {}) or {})
        if not collat_map:
            for ex, per_alias in balances.items():
                for alias in (per_alias or {}).keys():
                    collat_map[(str(ex).upper(), str(alias).upper())] = {"state": "OK", "collat_usd": 0.0}

        mm_alias = str(getattr(self, "mm_alias", "MM") or "MM").upper()
        rm_cfg = getattr(self.cfg, "rm", None)
        allowed = {str(ex).upper() for ex in (getattr(rm_cfg, "mm_hedge_allowed_exchanges", []) or []) if ex}

        candidates: list[tuple[float, float, str, str, str]] = []
        for (ex, alias), meta in collat_map.items():
            if mm_alias and mm_alias not in alias:
                continue
            if allowed and ex not in allowed:
                continue
            if str(meta.get("state") or "").upper() == "CRITICAL":
                continue

            assets = (balances.get(ex, {}) or {}).get(alias, {}) or {}
            try:
                position = float(assets.get(asset_u) or 0.0)
            except Exception:
                position = 0.0
            try:
                collat_usd = float(meta.get("collat_usd") or 0.0)
            except Exception:
                collat_usd = 0.0

            symbol = None
            for sym in getattr(self, "symbols", []) or []:
                sym_u = str(sym).replace("-", "").upper()
                if sym_u.startswith(asset_u):
                    symbol = sym_u
                    break
            if not symbol:
                quote = str(
                    getattr(getattr(self.cfg, "g", None), "primary_quote", getattr(self.cfg, "primary_quote", "USDC"))
                    or "USDC"
                ).upper()
                symbol = f"{asset_u}{quote}"

            candidates.append((abs(position), collat_usd, ex, alias, symbol))

        if not candidates:
            return None

        candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
        _, _, ex, alias, symbol = candidates[0]
        return ex, alias, symbol

    def _compute_mm_hedge_step_usd(self, asset: str) -> float:
        state = self.mm_delta_state.get(asset, {}) if isinstance(self.mm_delta_state, dict) else {}
        rm_cfg = getattr(self.cfg, "rm", None)
        max_step = float(getattr(rm_cfg, "mm_hedge_max_step_usd", 0.0) or 0.0)
        try:
            delta = abs(float(state.get("delta_usd") or 0.0))
        except Exception:
            delta = 0.0
        try:
            hard_limit = float(state.get("hard_usd") or state.get("hard_limit_usd") or 0.0)
        except Exception:
            hard_limit = 0.0

        over = max(delta - hard_limit, 0.0)
        if over <= 0:
            return 0.0
        return min(over, max_step) if max_step > 0 else over

    async def _mm_hedge_tick(self, now: float) -> None:
        rm_cfg = getattr(self.cfg, "rm", None)
        if not getattr(rm_cfg, "mm_hedge_enabled", False):
            return
        if not getattr(self, "engine", None):
            return

        cooldown = float(getattr(rm_cfg, "mm_hedge_cooldown_s", 0.0) or 0.0)
        for asset, state in (self.mm_delta_state or {}).items():
            if str(state.get("state") or "").upper() != "HARD":
                continue

            last_ts = float(self.mm_last_hedge_ts.get(asset, 0.0) or 0.0)
            if (now - last_ts) < cooldown:
                continue

            step_usd = self._compute_mm_hedge_step_usd(asset)
            if step_usd <= 0:
                continue

            delta_usd = float(state.get("delta_usd") or 0.0)
            side = "SELL" if delta_usd > 0 else "BUY"
            venue = self._pick_mm_hedge_venue(asset)
            if venue is None:
                self.logger.warning("[RM] mm hedge venue unavailable for %s", asset)
                continue
            exchange, alias, symbol = venue
            req = {
                "exchange": exchange,
                "alias": alias,
                "symbol": symbol,
                "side": side,
                "notional_usd": step_usd,
                "max_slippage_bps": None,
                "tag": "MM_DELTA_HEDGE",
            }

            try:
                RM_MM_HEDGE_TOTAL.labels(asset=asset).inc()
                RM_MM_HEDGE_USD.labels(asset=asset).inc(step_usd)
            except Exception:
                pass

            try:
                result = await self.engine.hedge_delta_single(req)
            except Exception as exc:
                self.logger.warning("[RM] engine hedge MM failed for %s: %s", asset, exc, exc_info=False)
                try:
                    RM_MM_HEDGE_FAILED_TOTAL.labels(asset=asset).inc()
                except Exception:
                    pass
                self.mm_last_hedge_ts[asset] = now
                continue

            self.mm_last_hedge_ts[asset] = now
            if not (result or {}).get("ok"):
                try:
                    RM_MM_HEDGE_FAILED_TOTAL.labels(asset=asset).inc()
                except Exception:
                    pass

    async def _tt_hedge_tick(self, now: float) -> None:
        rm_cfg = getattr(self.cfg, "rm", None)
        if not getattr(rm_cfg, "tt_hedge_enabled", False):
            return
        if not getattr(self, "engine", None):
            return

        hard_threshold = float(getattr(rm_cfg, "tt_stuck_hard_usd", 0.0) or 0.0)
        cooldown = float(getattr(rm_cfg, "tt_hedge_cooldown_s", 0.0) or 0.0)
        fraction = float(getattr(rm_cfg, "tt_hedge_fraction_of_expo", 0.0) or 0.0)
        max_step = float(getattr(rm_cfg, "tt_hedge_max_step_usd", 0.0) or 0.0)

        for asset, stuck_info in (self.tt_stuck_state or {}).items():
            delta_usd = float(stuck_info.get("delta_usd", 0.0) or 0.0)
            if abs(delta_usd) < hard_threshold:
                continue

            last_ts = float(self.tt_last_hedge_ts.get(asset, 0.0) or 0.0)
            if (now - last_ts) < cooldown:
                continue

            step_usd = abs(delta_usd) * (fraction if fraction > 0 else 0.0)
            if max_step > 0:
                step_usd = min(step_usd, max_step)
            if step_usd <= 0:
                continue

            side = "SELL" if delta_usd > 0 else "BUY"
            venue = self._pick_mm_hedge_venue(asset)
            if venue is None:
                self.logger.warning("[RM] tt hedge venue unavailable for %s", asset)
                continue
            exchange, alias, symbol = venue
            req = {
                "exchange": exchange,
                "alias": alias,
                "symbol": symbol,
                "side": side,
                "notional_usd": step_usd,
                "max_slippage_bps": None,
                "tag": "TT_STUCK_HEDGE",
            }

            try:
                RM_TT_HEDGE_TOTAL.labels(asset=asset).inc()
                RM_TT_HEDGE_USD.labels(asset=asset).inc(step_usd)
            except Exception:
                pass

            try:
                result = await self.engine.hedge_delta_single(req)
            except Exception as exc:
                self.logger.warning("[RM] engine hedge TT failed for %s: %s", asset, exc, exc_info=False)
                try:
                    RM_TT_HEDGE_FAILED_TOTAL.labels(asset=asset).inc()
                except Exception:
                    pass
                self.tt_last_hedge_ts[asset] = now
                continue

            self.tt_last_hedge_ts[asset] = now
            # Même si l'appel ne lance pas d'exception, on compte les cas ok=False
            if not (result or {}).get("ok"):
                try:
                    RM_TT_HEDGE_FAILED_TOTAL.labels(asset=asset).inc()
                except Exception:
                    pass

    def check_capital_drift(self, threshold_pct: float | None = None) -> None:
        """
        Compare le capital « virtuel » du RM (inventory_cap_usd) au capital réel
        observable via MBF (pockets de quote) et loggue une alerte si la dérive
        relative dépasse un seuil.

        - Capital virtuel  : inventory_cap_usd (profil capital / config).
        - Capital réel     : somme des poches de quote (USDC/USD/USDT) retournées
                             par MBF.as_buffers_snapshot() via
                             _compute_capital_available_usdc_from_buffers().
        """
        virt_cap = float(getattr(self, "inventory_cap_usd", 0.0) or 0.0)

        try:
            real_cap = float(self._compute_capital_available_usdc_from_buffers())
        except Exception as exc:
            self.logger.warning(
                "[RM] check_capital_drift: échec lecture capital MBF",
                exc_info=True,
            )
            return

        # Si on ne voit aucun capital réel côté MBF, on ne mesure pas la dérive.
        if real_cap <= 0.0:
            return

        drift_abs = virt_cap - real_cap
        drift_pct = (drift_abs / max(real_cap, 1e-9)) * 100.0
        self._last_capital_drift_pct = drift_pct

        # Seuil : paramètre RM_CAPITAL_DRIFT_THRESHOLD_PCT si présent, sinon défaut.
        if threshold_pct is None:
            threshold_pct = float(
                getattr(self.cfg, "RM_CAPITAL_DRIFT_THRESHOLD_PCT", 2.0)
            )

        if abs(drift_pct) >= threshold_pct:
            self.logger.warning(
                "[RM] Dérive capital virtuel / réel détectée: "
                f"virtuel={virt_cap:.2f} real={real_cap:.2f} "
                f"drift={drift_pct:.2f}% (seuil={threshold_pct:.2f}%)"
            )


    def _rm_check_fee_reserves(self, context) -> tuple:
        """
        Check que les réserves de tokens de fees (BNB, MNT, etc.) sont au-dessus
        des seuils bas, en utilisant UNIQUEMENT le snapshot MBF (source unique).

        Retourne (ok, reason).
        """
        if not hasattr(self, "fee_token_policy"):
            return True, "no_fee_token_policy"

        if (
                getattr(self.cfg.rm, "enable_fee_token_reserves_check", False)
                is False
        ):
            return True, "disabled"

        # Source unique : MultiBalanceFetcher via glue RM_MBFGlue
        try:
            bf_snapshot = self._mbf_glue.snapshot(cached_only=True)
        except Exception as exc:
            self.log.error("Fee reserves MBF snapshot failed: %s", exc, exc_info=True)
            try:
                FEE_TOKEN_CHECK_ERRORS_TOTAL.labels("mbf_snapshot_failed").inc()
            except Exception:
                pass
            return False, "fee_reserves_mbf_snapshot_failed"

        meta = bf_snapshot.get("meta") or {}
        fee_meta = meta.get("fee_tokens") or {}

        # On garde en cache pour éventuellement impacter les caps par exchange/alias.
        self._last_fee_tokens_meta = fee_meta

        if not fee_meta:
            # Rien à vérifier explicitement, on laisse passer.
            return True, "no_fee_tokens_meta"

        def place_order_cb(symbol_key, amount_quote):
            # Ne place PAS l'ordre ici, seulement une intention structurée.
            return {
                "kind": "fee_token_topup",
                "symbol": symbol_key,
                "amount_quote": float(amount_quote or 0.0),
            }

        try:
            actions = self.fee_token_policy.check_and_topup(fee_meta, place_order_cb)
        except Exception as exc:
            self.log.error("Fee reserves check failed: %s", exc, exc_info=True)
            try:
                FEE_TOKEN_CHECK_ERRORS_TOTAL.labels("policy_failed").inc()
            except Exception:
                pass
            return False, "fee_reserves_policy_failed"

        if actions:
            # On ne bloque pas le desk, mais on signale un état dégradé + on compte les top-ups.
            for act in actions:
                sym = act.get("symbol", "unknown")
                amt = float(act.get("amount_quote", 0.0) or 0.0)
                self.log.warning("Fee token TOPUP requested: %s amount_quote=%.4f", sym, amt)
                try:
                    FEE_TOKEN_TOPUP_REQUESTED_TOTAL.labels(sym).inc()
                except Exception:
                    pass

            return True, "fee_reserves_low_topup_requested"

        return True, "ok"


    def _get_pnl_day_pct(self) -> float:
        """
               Retourne le PnL réalisé du jour sous forme de fraction du capital
               de référence (ex: 0.01 = +1 %, -0.005 = -0.5 %).

               Contrat :
                 - Si `self.pnl_guard_provider` est défini et callable, il est utilisé
                   comme source principale. Il doit être de la forme Callable[[], float]
                   et renvoyer un PnL jour en pourcentage, idéalement calculé à partir
                   des agrégats PnL de la DB (LogWriter / LoggerHistoriqueManager).
                   Le provider doit être non bloquant (pas d'I/O lourde) et ne pas lever
                   d'exception ; en cas de problème, il doit retourner une valeur neutre.
                 - Sinon, on se rabat sur `self.pnl_day_pct` si cet attribut est exposé
                   ailleurs, avec la même convention de signe.
                 - En dernier recours, cette méthode retourne 0.0 (guard inactif) en cas
                   d'absence de source ou d'erreur.

               NB : cette méthode ne calcule pas elle-même le PnL ; elle délègue à un
               provider externe ou à un champ déjà mis à jour par une autre boucle.
               """
        try:
            prov = getattr(self, "pnl_guard_provider", None)
            if callable(prov):
                return float(prov() or 0.0)
            return float(getattr(self, "pnl_day_pct", 0.0) or 0.0)
        except Exception:
            return 0.0

    def _pnl_guard_tick(self) -> None:
        day = float(self._get_pnl_day_pct())
        now = time.time()

        # Entrées (paliers)
        if day <= self._pnl_guard_lvl2:
            self.rm_mode = "SEVERE"
            self._mode_since = now
            self._last_bad_ts = now
        elif day <= self._pnl_guard_lvl1 and self.rm_mode == "NORMAL":
            self.rm_mode = "OPP_VOL"
            self._mode_since = now
            self._last_bad_ts = now

        # Sortie (cooldown)
        recover_floor = float(self._switch_knobs.pnl_guard_recover_floor_pct)
        if self.rm_mode in ("OPP_VOL", "SEVERE") and day > min(self._pnl_guard_lvl1, recover_floor):
            if (now - max(self._last_bad_ts, self._mode_since)) >= self._pnl_cooldown_s:
                self.rm_mode = "NORMAL"
                self._mode_since = now

    # ------------------------------------------------------------------
    # Readiness / Guards
    # ------------------------------------------------------------------
    # === Helpers MM / budgets / réservations (USDC/EUR) ===================


    def _profile_cap_notional(self, *, profile: str, slip_tail_bps: float = 25.0) -> float:
        # borne “tail-risk” (budget perte € / tail-slip)
        loss_budget = dict(NANO=0.5, MICRO=1.0, SMALL=2.0, MID=4.0, LARGE=8.0).get(profile, 1.0)
        cap_tail = float(loss_budget) / (float(slip_tail_bps) / 1e4)  # ex: LARGE: 8€/0.0025 = 3200€
        # borne “profil” (budget branches / concurrency)
        branches = int(getattr(self, "max_fragments", 1) or 1)
        budget_branches = float(getattr(self, "default_notional", 100.0)) * float(branches)
        cap_profile = (budget_branches / max(1, branches)) * 0.6
        return max(10.0, min(cap_tail, cap_profile))

    def _mm_wallet_key(self, exchange: str, quote: str) -> tuple[str, str]:
        return str(exchange or "").upper(), str(quote or "").upper()

    def _mm_wallet_remaining(self, exchange: str, quote: str) -> float:
        exu, q = self._mm_wallet_key(exchange, quote)
        return max(0.0, float(self.virt_balances.get(exu, {}).get(q, 0.0)))

    def _mm_wallet_reserve(
            self,
            exchange: str,
            quote: str,
            amt: float,
            *,
            dry_run: bool = False,
            reason: str = "",
            profile: str | None = None,
    ) -> bool:
        exu, q = self._mm_wallet_key(exchange, quote)
        need = float(amt or 0.0)
        cur = self._mm_wallet_remaining(exu, q)
        if cur < need or need <= 0.0:
            return False
        if dry_run:
            return True

        self.virt_balances.setdefault(exu, {})[q] = cur - need
        try:
            self._maybe_reset_daily_budget()
            strat = "MM"
            self._spent_today_quote[strat] = self._spent_today_quote.get(strat, 0.0) + need
        except Exception:
            pass

        prof_label = str(profile or getattr(self, "capital_profile", "UNKNOWN") or "UNKNOWN").upper()
        try:
            RM_MM_BUDGET_SPENT_QUOTE.labels(prof_label, exu or "UNKNOWN", q or "UNKNOWN").inc(need)
        except Exception:
            pass
        if INVENTORY_USD:
            try:
                INVENTORY_USD.labels(exchange=exu, quote=q).set(self.virt_balances[exu][q])
            except Exception:
                logging.exception("Unhandled exception")
        return True

    def _mm_wallet_release(self, exchange: str, quote: str, amt: float, *, reason: str = "") -> None:
        exu, q = self._mm_wallet_key(exchange, quote)
        add = float(amt or 0.0)
        cur = self._mm_wallet_remaining(exu, q)
        new_val = max(cur + add, 0.0)
        self.virt_balances.setdefault(exu, {})[q] = new_val
        try:
            if INVENTORY_USD:
                INVENTORY_USD.labels(exchange=exu, quote=q).set(new_val)
        except Exception:
            logging.exception("Unhandled exception")

    def _mm_pair_headroom(self, profile: str, exchange: str, pair_key: str, quote: str) -> float:
        prof = str(profile or "LARGE").upper()
        exu, pk, q = str(exchange or "").upper(), self._norm_pair(pair_key), str(quote or "USDC").upper()
        spent = float(self.mm_pair_spent_usdc.get((prof, exu, pk), 0.0))

        params = self.get_mm_slot_params(prof)
        slot_notional = float(params.get("slot_notional_usdc") or 0.0)
        slots_per_pair = int(params.get("slots_per_pair") or 0)
        cap_ratio = float(params.get("pair_cap_ratio") or self.mm_pair_cap_ratio_by_profile.get(prof, 0.0))

        profile_cap = float(self._profile_cap_notional(profile=prof))
        cap_from_ratio = profile_cap * max(0.0, min(1.0, cap_ratio)) if cap_ratio > 0 else 0.0
        cap_from_slots = slot_notional * float(slots_per_pair) if slot_notional > 0 and slots_per_pair > 0 else 0.0

        cap_pair = cap_from_ratio if cap_from_ratio > 0 else cap_from_slots
        remaining = max(cap_pair - spent, 0.0)
        return remaining

    def _handle_mm_budget_event(self, ev: Dict[str, Any]) -> None:
        meta = ev.get("meta") or {}
        branch = str(meta.get("branch") or meta.get("strategy") or meta.get("kind") or "").upper()
        kind = str(meta.get("kind") or "").upper()
        if not self._is_mm_family(branch, kind):
            return

        status = str(ev.get("status") or meta.get("status") or meta.get("state") or "").upper()
        if status not in {"FILL", "FILLED", "PARTIAL", "PARTIAL_FILL", "CANCEL", "CANCELED", "CANCELLED"}:
            return

        exchange = str(ev.get("exchange") or meta.get("exchange") or "").upper()
        quote = str(meta.get("quote") or ev.get("quote") or "").upper()
        profile = str(meta.get("capital_profile") or meta.get("profile") or getattr(self, "capital_profile",
                                                                                    "LARGE") or "LARGE").upper()
        pair_key = meta.get("pair") or ev.get("symbol") or meta.get("symbol")
        pk = self._norm_pair(pair_key or "")

        if not exchange or not quote or not pk:
            try:
                logger.warning("[RM] MM budget release skipped missing data ex=%s quote=%s pair=%s", exchange or "?",
                               quote or "?", pk or "?")
            except Exception:
                pass
            return

        def _as_float(val: Any) -> Optional[float]:
            try:
                return float(val)
            except Exception:
                return None

        meta_open_keys = ["prev_open_notional_usdc", "open_notional_prev_usdc", "open_notional_before_usdc"]
        prev_open = None
        for key in meta_open_keys:
            prev_open = _as_float(meta.get(key))
            if prev_open is not None:
                break

        new_open = _as_float(meta.get("open_notional_usdc"))
        if new_open is None:
            new_open = _as_float(meta.get("remaining_notional_usdc"))
        if new_open is None:
            new_open = 0.0 if status in {"FILL", "FILLED", "CANCEL", "CANCELED", "CANCELLED"} else None

        if prev_open is None:
            try:
                logger.warning(
                    "[RM] MM budget release missing prev_open ex=%s pair=%s status=%s", exchange, pk, status
                )
            except Exception:
                pass
            return
        if new_open is None:
            try:
                logger.warning(
                    "[RM] MM budget release missing new_open ex=%s pair=%s status=%s", exchange, pk, status
                )
            except Exception:
                pass
            return

        delta_release = max(prev_open - new_open, 0.0)
        if delta_release <= 0.0:
            return

        self._mm_wallet_release(exchange, quote, delta_release, reason=status.lower())
        key = (profile, exchange, pk)
        before = float(self.mm_pair_spent_usdc.get(key, 0.0))
        self.mm_pair_spent_usdc[key] = max(before - delta_release, 0.0)

        try:
            logger.debug(
                "[RM] MM budget release ex=%s pair=%s profile=%s quote=%s delta=%.4f spent_before=%.4f spent_after=%.4f",
                exchange,
                pk,
                profile,
                quote,
                delta_release,
                before,
                self.mm_pair_spent_usdc[key],
            )
        except Exception:
            pass

    def rm_is_asset_under_rebalancing(self, exchange: str, asset: str) -> bool:
        reb = getattr(self, "rebalancer", None)
        if not reb or not hasattr(reb, "is_asset_under_rebalancing"):
            return False
        try:
            return bool(reb.is_asset_under_rebalancing(exchange, asset))
        except Exception:
            return False

    def _norm_route_for_sfc(self, route: dict | None, *, pair_key: str | None = None) -> dict:
        """
        Normalise un dict route pour l'API SFC Forme A.
        Garantit: buy_ex/sell_ex, pair_key, base, quote (+ conserve les autres champs).
        Ne casse rien: copie défensive, jamais d'exception.
        """
        r = dict(route or {})

        try:
            buy_ex = (r.get("buy_ex") or r.get("buy_exchange") or r.get("buy") or "").upper()
            sell_ex = (r.get("sell_ex") or r.get("sell_exchange") or r.get("sell") or "").upper()

            # pair
            pk = (r.get("pair_key") or r.get("pair") or pair_key or "")
            pk = self._norm_pair(pk) if pk else ""

            # base/quote (SFC Forme A construit pair = base+quote)
            base = (r.get("base") or "").upper()
            quote = (r.get("quote") or "").upper()

            if not pk and base and quote:
                pk = (base + quote).replace("-", "").upper()
            if pk and (not base or not quote):
                try:
                    quote = _pair_quote(pk)
                except Exception:
                    quote = "USDC"
                try:
                    base = _strip_quote(pk)
                except Exception:
                    base = pk

            if buy_ex:
                r["buy_ex"] = buy_ex
            if sell_ex:
                r["sell_ex"] = sell_ex
            if pk:
                r["pair_key"] = pk
                r["pair"] = pk
            if base:
                r["base"] = base
            if quote:
                r["quote"] = quote

        except Exception:
            # fallback ultra-safe: on renvoie la copie originale
            pass

        return r

    def _reb_cost_fn(self, route: dict) -> float:
        """
        Coût net en bps pour REB (pur, sans I/O).
        Utilise SFC.get_total_cost_pct(..., side="TM") car le bridge se fait en maker côté destination.
        """
        try:
            route_sfc = self._norm_route_for_sfc(route, pair_key=(route.get("pair") or route.get("pair_key")))
            slip_kind = str(getattr(self.cfg, "sfc_slippage_source", "ewma"))

            pct = float(self.slip_collector.get_total_cost_pct(
                route_sfc,
                side="TM",
                size_quote=float(getattr(self.cfg, "rebal_size_quote", 2000.0)),
                slippage_kind=("p95" if str(slip_kind).lower() == "p95" else "ewma"),
                prudence_key="NORMAL",
            ))
            bps = pct * 1e4
            try:
                from modules.obs_metrics import TOTAL_COST_BPS
                r = f"{route.get('buy_ex')}->{route.get('sell_ex')}"
                TOTAL_COST_BPS.labels(r, "TM").set(bps)
            except Exception:
                pass
            return bps

        except Exception:
            return 0.0

    def _publish_fee_rc_obs(self, rc: dict, with_token: bool = False) -> None:
        """
        Publie les métriques d'obs pour le reality-check fees (labels cohérents).
        - rc: dict retourné par SFC.on_fill_fee_reality_check(...)
              attendu: exchange, alias, side, notional, expected_fee, paid_fee, exceeded
        """
        try:
            from modules.obs_metrics import FEES_EXPECTED_BPS, FEES_REALIZED_BPS, FEE_MISMATCH_TOTAL
        except Exception:
            return  # obs indisponible -> no-op

        ex = str(rc.get("exchange", "NA"))
        alias = str(rc.get("alias", "NA"))
        side = str(rc.get("side", "NA")).upper()

        denom = float(rc.get("notional") or 0.0)
        denom = denom if denom > 0 else 1e-12
        exp_bps = float(rc.get("expected_fee", 0.0)) / denom * 1e4
        real_bps = float(rc.get("paid_fee", 0.0)) / denom * 1e4

        try:
            FEES_EXPECTED_BPS.labels(ex, "SPOT", alias, str(with_token)).observe(exp_bps)
        except Exception:
            pass
        try:
            FEES_REALIZED_BPS.labels(ex, "SPOT", alias, str(with_token)).observe(real_bps)
        except Exception:
            pass
        if rc.get("exceeded"):
            try:
                FEE_MISMATCH_TOTAL.labels("reality_check_exceeded").inc()
            except Exception:
                pass

    @staticmethod
    def _normalize_notional(self, opp: Dict[str, Any]) -> float:
        """
        Retourne le notionnel dans la devise de cotation (USDC/EUR), tolérant aux alias legacy.
        Accepte aussi le format moderne {"notional_quote":{"quote": "...", "amount": ...}}.
        """
        # format moderne dict
        nq = opp.get("notional_quote")
        if isinstance(nq, dict):
            try:
                return float(nq.get("amount", 0.0) or 0.0)
            except Exception:
                pass

        # formats legacy / alias
        for k in ("notional", "volume_selected_quote", "volume_quote",
                  "volume_possible_quote", "volume_selected_usdc",
                  "volume_usdc", "volume_possible_usdc"):
            if k in (opp or {}):
                try:
                    v = float(opp.get(k) or 0.0)
                    if v > 0:
                        return v
                except Exception:
                    logger.debug("normalize_notional parse error for %s", k, exc_info=False)
        return 0.0

    def _normalize_notional_tuple(self, opportunity: dict) -> tuple[str, float]:
        """
        Retourne (quote, notional) avec priorité à 'notional_quote' sinon 'notional'.
        Compat descendante: si rien, essaye volume_selected_quote.
        """
        # 1) format moderne {"notional_quote":{"quote":"USDC","amount":...}}
        nq = opportunity.get("notional_quote")
        if isinstance(nq, dict) and "quote" in nq and "amount" in nq:
            return str(nq["quote"]).upper(), float(nq["amount"])

        # 2) compat: champ 'notional' (devise implicite = quote de la paire)
        if "notional" in opportunity:
            q = _pair_quote(str(opportunity.get("pair") or opportunity.get("symbol") or ""))
            return q, float(opportunity["notional"])

        # 3) compat legacy: {"volume_selected_quote":{"quote":"USDC","amount":...}}
        vsq = opportunity.get("volume_selected_quote")
        if isinstance(vsq, dict) and "quote" in vsq and "amount" in vsq:
            return str(vsq["quote"]).upper(), float(vsq["amount"])

        # 4) fallback final: default_notional
        q = _pair_quote(str(opportunity.get("pair") or opportunity.get("symbol") or ""))
        return q, _cfg_float(self.cfg, "default_notional", 0.0)

    def _choose_strategy(self, opp: dict) -> str:
        """Deprecated: kept for compat, scheduler uses on_scanner_opportunity order."""
        exp = (opp.get("expected_net_bps") or {})
        # Garde-fous MM (hints calculés par le Scanner)
        mm = float(exp.get("MM", 0.0) or 0.0)
        if bool(getattr(self.cfg, "enable_mm", getattr(self, "enable_mm", False))) and mm > 0.0:
            hints = (opp.get("hints") or {}).get("MM") or {}
            if all([
                float(hints.get("depth_A_usd", 0.0)) >= float(getattr(self.cfg, "mm_depth_min_usd", 0.0)),
                float(hints.get("depth_B_usd", 0.0)) >= float(getattr(self.cfg, "mm_depth_min_usd", 0.0)),
                float(hints.get("qpos_A_usd", 0.0)) <= float(getattr(self.cfg, "mm_qpos_max_ahead_usd", 1e12)),
                float(hints.get("qpos_B_usd", 0.0)) <= float(getattr(self.cfg, "mm_qpos_max_ahead_usd", 1e12)),
                float(hints.get("p_both", 0.0)) >= float(getattr(self.cfg, "mm_min_p_both", 0.0)),
            ]):
                return "MM"
        if bool(getattr(self.cfg, "enable_tm", getattr(self, "enable_tm", False))) and float(
                exp.get("TM", 0.0) or 0.0) > 0.0:
            return "TM"
        if bool(getattr(self.cfg, "enable_tt", getattr(self, "enable_tt", True))) and float(
                exp.get("TT", 0.0) or 0.0) > 0.0:
            return "TT"
        return "NONE"

    async def _cancel_open_mm_quotes_on_exchange(
        self,
        exchange: str,
        pair: str | None = None,
        account_alias: str | None = None,
        *,
        reason: str = "",
    ) -> int:
        """Préempte la liquidité MM via l'executor, avec cooldown fail-soft."""

        cooldown = float(getattr(self, "mm_preempt_cooldown_s", 1.0))
        pair_key = self._norm_pair(pair) if pair else None
        alias_u = str(account_alias or "").upper()
        key = (str(exchange or "").upper(), pair_key or "*", alias_u or "*")
        now = time.time()
        last = getattr(self, "_mm_last_preempt_ts", {}).get(key, 0.0)
        if now - last < cooldown:
            return 0
        self._mm_last_preempt_ts[key] = now


        executor = getattr(self, "executor", None) or getattr(self, "engine", None)
        if executor is None or not hasattr(executor, "cancel_mm_quotes_on_exchange"):
            logging.warning("[RiskManager] executor without MM cancel API, skip preempt")
            return 0

        try:
            res = await executor.cancel_mm_quotes_on_exchange(
                exchange=str(exchange).upper(),
                pair=pair_key,
                account_alias=alias_u or None,
                reason=reason,
            )
            count = int(res) if res is not None else -1
        except Exception:
            logging.exception("[RiskManager] MM preempt cancel failed")
            count = -1

        if count >= 0:
            try:
                reason_code = normalize_reason_code(reason or "MM_PREEMPTED") or "MM_PREEMPTED"
                RM_MM_PREEMPTED_TOTAL.labels(by=reason_code).inc(count)
            except Exception:
                pass
        return count
    # === RM: capital net & profil ===
    def _mm_preempt_reason(self, by: str) -> str:
        by_u = str(by or "").upper()
        mapping = {
            "TT": "PREEMPT_TT",
            "TM": "PREEMPT_TM",
            "REB": "PREEMPT_REB",
            "HEDGE": "PREEMPT_HEDGE",
        }
        reason = mapping.get(by_u, "MM_PREEMPTED")
        return normalize_reason_code(reason) or reason

    def compute_capital_net_per_subaccount(self, gross_equity: float, fee_reserve_total: float) -> float:
        """
        Capital net exploitable = equity - réserves tokens fees (BNB/MNT...).
        gross_equity et fee_reserve_total sont par sous-compte.
        """
        return max(0.0, float(gross_equity) - float(fee_reserve_total))

    def decide_capital_profile(self, net_per_sc: float) -> str:
        """
        Détermine le profil capital à partir du capital net moyen par sous-compte,
        en se basant exclusivement sur la policy capital_ladder_cfg de BotConfig.
        """
        cfg = getattr(self, "cfg", None)

        # Récupère la policy RM.capital_ladder_cfg
        rm_cfg = getattr(cfg, "rm", None) if cfg is not None else None
        ladder_cfg = {}
        if rm_cfg is not None:
            try:
                ladder_cfg = dict(getattr(rm_cfg, "capital_ladder_cfg", {}) or {})
            except Exception:
                ladder_cfg = {}

        if not ladder_cfg:
            # Fallback conservateur : on renvoie simplement le profil global configuré.
            g = getattr(cfg, "g", None) if cfg is not None else None
            prof = getattr(g, "capital_profile", "LARGE") if g is not None else "LARGE"
            return str(prof).upper()

        best_prof = None
        best_min_cap = None
        for prof, policy in ladder_cfg.items():
            policy = policy or {}
            try:
                min_cap = float(policy.get("min_capital_per_sc", 0.0) or 0.0)
            except Exception:
                continue
            if net_per_sc >= min_cap and (best_min_cap is None or min_cap >= best_min_cap):
                best_prof = prof
                best_min_cap = min_cap

        if best_prof is None:
            # net_per_sc en-dessous de toutes les gates : on prend le premier profil de la ladder.
            best_prof = next(iter(ladder_cfg.keys()))

        return str(best_prof).upper()


    # === /RM: capital net & profil ===

    def _apply_caps_and_preempt_cex(self, strategy: str, ex: str, desired_notional: float) -> float:
        """
        Applique le cap notionnel par (stratégie,CEX). Si dépassement:
          - préempte MM sur ce CEX uniquement pour hedge (si autorisé), puis tronque à cap.
        """
        cap = float(((self.per_strategy_notional_cap or {}).get(strategy, {}) or {}).get(str(ex).upper(), float("inf")))
        if desired_notional <= cap:
            return max(0.0, desired_notional)
        if str(strategy or "").upper() in ("HEDGE", "PANIC_HEDGE") and (
                self.preempt_mm_for_tt_tm or getattr(self, "ff_enforce_preemption", False)
        ):
            try:
                reason = self._mm_preempt_reason("HEDGE")
                asyncio.create_task(
                    self._cancel_open_mm_quotes_on_exchange(ex, reason=reason)
                )
            except Exception:
                logging.exception("Unhandled while scheduling MM preempt")
        return max(0.0, cap)

    def _get_caps_for_bundle(
            self,
            bundle: Dict[str, Any],
            branch: str,
            profile: str,
            quote: str,
            meta: dict,
    ) -> dict:
        # SAFE DEFAULT: must always be defined (prevents UnboundLocalError)


        """
        Calcule/normalise les caps locaux pour ce bundle (Ticket 10).

        Règles :
        - Source de vérité business = BotConfig.RiskManagerCfg
          (caps_trading_by_profile, inflight_rebal_by_profile).
        - On part des caps déjà packés dans le bundle (bundle["caps"])
          pour respecter les décisions prises lors de la construction
          (_build_bundle + degraded["caps"]).
        - On ne complète que les champs manquants :
          inflight_cap, bundle_concurrency, headroom_min.
        """
        rm_cfg = getattr(getattr(self, "cfg", None), "rm", None)
        branch_u = str(branch or meta.get("branch") or "").upper()
        profile_u = str(profile or getattr(self, "capital_profile", "LARGE")).upper()

        # Facteur TTL alias (balances_ttl) injecté par engine_enqueue_bundle.
        # Par défaut, on est neutre (=1.0) si aucun overlay n'est présent.
        alias_cap_factor = 1.0
        try:
            overlays = (meta or {}).get("overlays") or {}
            ttl_overlay = overlays.get("balances_ttl") or {}
            alias_cap_factor = float(ttl_overlay.get("alias_cap_factor", 1.0))
        except Exception:
            alias_cap_factor = 1.0
        if alias_cap_factor > 1.0:
            alias_cap_factor = 1.0
        if alias_cap_factor < 0.0:
            alias_cap_factor = 0.0


        # 0) Point de départ = ce que le builder a déjà packé
        caps_local: dict = {}
        try:
            initial = bundle.get("caps") or {}
            if isinstance(initial, dict):
                caps_local.update(initial)
        except Exception:
            caps_local = {}

        # 1) inflight_cap : si absent, on reprend ta logique actuelle (profil × branche)
        #    et on la claque à 0 si la branche est désactivée pour ce profil.
        if "inflight_cap" not in caps_local:
            inflight_cap = None
            try:
                branch_enabled = True
                try:
                    if branch_u == "TT":
                        branch_enabled = bool(getattr(self.cfg, "enable_tt", getattr(self, "enable_tt", True)))
                    elif branch_u == "TM":
                        branch_enabled = bool(getattr(self.cfg, "enable_tm", getattr(self, "enable_tm", True)))
                    elif self._is_mm_family(branch_u):
                        branch_enabled = bool(getattr(self, "enable_mm", False))
                    elif branch_u == "REB":
                        branch_enabled = bool(getattr(self.cfg, "enable_reb", getattr(self, "enable_reb", True)))
                except Exception:
                    branch_enabled = True

                if branch_u == "REB":
                    caps_reb = getattr(rm_cfg, "inflight_rebal_by_profile", {}) or {}
                    inflight_cap = int((caps_reb.get(profile_u) or caps_reb.get("LARGE") or 0))
                else:
                    caps_by_profile = getattr(rm_cfg, "caps_trading_by_profile", {}) or {}
                    prof_caps = caps_by_profile.get(profile_u) or caps_by_profile.get("LARGE", {})
                    # Normalisation MM pour l'accès aux caps de profil
                    lookup_branch = "MM" if self._is_mm_family(branch_u) else branch_u
                    inflight_cap = int(prof_caps.get(lookup_branch, 0) or 0)
                if not branch_enabled:
                    inflight_cap = 0
            except Exception:
                inflight_cap = None
            caps_local["inflight_cap"] = inflight_cap

        inflight_cap_eff = caps_local.get("inflight_cap") or 0

        # 2) bundle_concurrency : si absent, on remet ta logique pacer_factor (down-clamp only)
        if "bundle_concurrency" not in caps_local:
            pacer = getattr(self, "pacer", None)
            pacer_factor = 1.0
            if pacer and hasattr(pacer, "factor_for_branch"):
                try:
                    pacer_factor = float(pacer.factor_for_branch(branch_u))
                except Exception:
                    pacer_factor = 1.0

            # Clamp explicite pour garantir le "down-clamp only"
            if pacer_factor > 1.0:
                pacer_factor = 1.0
            if pacer_factor < 0.0:
                pacer_factor = 0.0

            # Formula: inflight_cap × pacer_factor(branch) × alias_cap_factor (tous ≤ 1.0)
            bundle_concurrency = 0
            try:
                bundle_concurrency = max(
                    0,
                    int(round(float(inflight_cap_eff) * pacer_factor * alias_cap_factor)),
                )
            except Exception:
                try:

                    bundle_concurrency = max(
                        0,
                        int(float(inflight_cap_eff or 0) * alias_cap_factor),
                    )
                except Exception:
                    bundle_concurrency = 0

            # Ensure always valid and >= 0 (0 means "disabled / no concurrency")
            try:
                bundle_concurrency = int(bundle_concurrency) if bundle_concurrency is not None else 0
            except Exception:
                bundle_concurrency = 0
            if bundle_concurrency < 0:
                bundle_concurrency = 0

            # If inflight cap is 0, concurrency must be 0 (do not resurrect the branch)
            if int(inflight_cap_eff or 0) <= 0:
                bundle_concurrency = 0

            # Do not override if already set earlier in caps_local (keeps existing semantics)
            caps_local.setdefault("bundle_concurrency", bundle_concurrency)

        # 3) headroom_min : si absent, on garde ta valeur par défaut config
        if "headroom_min" not in caps_local:
            caps_local["headroom_min"] = int(getattr(self, "inflight_headroom_min", 1) or 0)

        # 4) Exposer le facteur TTL alias appliqué pour ce bundle (observabilité / debug).
        caps_local["alias_cap_factor"] = alias_cap_factor

        return caps_local

    # === /RM: capital net & profil ===

    def _apply_caps_and_preempt_legacy(self, strategy: str, ex: str, desired_notional: float) -> float:
        """
        Adaptateur legacy (Opp-level) pour compatibilité ascendante.
        Utilise la logique historique _apply_caps_and_preempt_cex.
        """
        return self._apply_caps_and_preempt_cex(strategy, ex, desired_notional)

    def _maybe_fire_mm_inventory_single(self, opp: Dict[str, Any], reason: str, branch: str = "MM") -> None:
        """Déclenche un maker inventaire vers l'Engine via la file standard."""
        if not getattr(self, "mm_inventory_enabled", False):
            return

        pair = opp.get("pair") or opp.get("symbol")
        if not pair:
            return
        pk = self._norm_pair(pair)

        ex = (opp.get("buy_ex") or opp.get("sell_ex") or "").upper()
        if not ex:
            return

        snap = getattr(self, "balances", None) or {}
        base = self._pair_base(pk)

        # On utilise le branch pour trouver l'alias correct si non fourni dans opp
        alias = opp.get("alias") or branch

        try:
            base_pos = float((snap.get(ex, {}).get(alias) or {}).get(base) or {}).get("free", 0.0)
        except Exception:
            base_pos = 0.0

        bid, ask = getattr(self, "get_top_of_book", lambda *a: (0.0, 0.0))(ex, pk)
        mid = (float(bid) + float(ask)) / 2.0 if (bid and ask) else 0.0
        inv_usd = base_pos * mid

        notional_target = float(getattr(self, "notional_usd", 0.0) or opp.get("notional_usdc") or 0.0)
        if notional_target <= 0:
            notional_target = abs(inv_usd)
        
        skew_pct = 100.0 * inv_usd / notional_target if notional_target else 0.0
        max_skew = float(getattr(self, "mm_inventory_max_skew_pct", 15.0) or 0.0)
        min_notional = float(getattr(self, "mm_inventory_notional_usd", 0.0) or 0.0)

        if abs(skew_pct) < max_skew or abs(inv_usd) < min_notional:
            return

        maker_side = "SELL" if inv_usd > 0 else "BUY"
        amount_quote = max(abs(inv_usd), 0.0)

        try:
            ttl_ms = int(getattr(self, "mm_ttl_ms", 2300))
        except Exception:
            ttl_ms = 2300

        payload = {
            "type": "mm_single_inventory",
            "pair": pk,
            "exchange": ex,
            "side": maker_side,
            "amount_quote": amount_quote,
            "ttl_ms": ttl_ms,
            "meta": {"branch": branch, "mm_mode": "SINGLE", "reason": reason},
        }

        try:
            self.engine._spawn(self.engine.execute(payload), name=f"mm-inv-{pk}-{ex}")
        except Exception:
            if getattr(self, "log", None):
                self.log.exception("RM._maybe_fire_mm_inventory_single: failed")



    def _apply_caps_and_preempt(
            self,
            bundle: Dict[str, Any],
            caps_local: Dict[str, Any],
            profile: Optional[str] = None,
            eligible: Any = None,
    ) -> tuple[bool, Dict[str, Any], str]:
        """
        Contrat bundle-centric (Ticket 10) — utilisé par engine_enqueue_bundle.

        Rôle :
        - valider / normaliser caps_local,
        - appliquer les caps globaux MM (virtual wallet + headroom par paire),
        - renvoyer (ok, caps_local, trade_mode).

        NB : la limitation réelle de concurrence par branche reste appliquée
        côté Engine via bundle_concurrency/headroom_min ; ici on fait un
        pré-check léger mais bloquant pour MM.
        """
        meta = bundle.get("meta") or {}
        trade_mode = str(meta.get("mode") or getattr(self, "trade_mode", "NORMAL") or "NORMAL").upper()

        # --- 0) Hygiène de base sur caps_local ---------------------------------
        if not isinstance(caps_local, dict):
            logging.warning(
                "[RM] caps_local invalid for bundle (type=%s) trace_id=%s",
                type(caps_local),
                meta.get("trace_id") or "NA",
            )
            # On drop le bundle : caps non interprétables.
            return False, caps_local, trade_mode

        inflight_cap = caps_local.get("inflight_cap")
        try:
            if isinstance(inflight_cap, (int, float)) and inflight_cap <= 0:
                # Branche désactivée ou aucun headroom configuré pour ce profil/branche.
                logging.info(
                    "[RM] branch disabled by inflight_cap<=0: branch=%s profile=%s inflight_cap=%s",
                    str(meta.get("branch") or bundle.get("branch") or "NA"),
                    str(profile or meta.get("profile") or getattr(self, "capital_profile", "LARGE")),
                    inflight_cap,
                )
                return False, caps_local, trade_mode
        except Exception:
            # On ne bloque pas si on ne sait pas interpréter le cap.
            pass

        # --- 1) Gating spécifique MM : headroom par paire + virtual wallet -----
        branch_u = str(meta.get("branch") or bundle.get("branch") or bundle.get("strategy") or "").upper()
        kind_u = str(meta.get("kind") or bundle.get("kind") or "").upper()

        if self._is_mm_family(branch_u, kind_u):
            profile_u = str(
                profile
                or meta.get("profile")
                or getattr(self, "capital_profile", "LARGE")
                or "LARGE"
            ).upper()

            exchange = str(
                meta.get("exchange")
                or bundle.get("exchange")
                or meta.get("ex")
                or bundle.get("ex")
                or ""
            ).upper()
            quote = str(meta.get("quote") or bundle.get("quote") or "USDC").upper()
            pair_key = (
                    meta.get("pair")
                    or meta.get("symbol")
                    or bundle.get("pair")
                    or bundle.get("symbol")
            )
            pair_norm = self._norm_pair(pair_key or "")

            # Si on n'a pas assez d'infos, on reste fail-soft (on ne casse pas le flux MM).
            if not (exchange and quote and pair_norm and profile_u):
                logging.warning(
                    "[RM] MM bundle without full context for wallet gating; skip M3-B checks: "
                    "profile=%s ex=%s pair=%s quote=%s trace_id=%s",
                    profile_u,
                    exchange,
                    pair_norm,
                    quote,
                    meta.get("trace_id") or "NA",
                )
                return True, caps_local, trade_mode

            # Notional du bundle en USD-like.
            try:
                bundle_notional = float(self._estimate_bundle_notional_usd(bundle) or 0.0)
            except Exception:
                bundle_notional = 0.0

            if bundle_notional <= 0.0:
                # Rien à réserver → on laisse passer.
                return True, caps_local, trade_mode

            # 1.a) Cap par paire via mm_pair_headroom
            try:
                headroom_pair = float(self._mm_pair_headroom(profile_u, exchange, pair_norm, quote) or 0.0)
            except Exception:
                headroom_pair = 0.0

            if bundle_notional > headroom_pair:
                logging.info(
                    "[RM] MM_PAIR_CAP_EXHAUSTED: profile=%s ex=%s pair=%s quote=%s "
                    "notional=%.4f headroom=%.4f",
                    profile_u,
                    exchange,
                    pair_norm,
                    quote,
                    bundle_notional,
                    headroom_pair,
                )
                try:
                    if RM_MM_BUDGET_EXHAUSTED_TOTAL is not None:
                        RM_MM_BUDGET_EXHAUSTED_TOTAL.labels(profile_u, exchange, quote).inc()
                except Exception:
                    pass
                try:
                    if not isinstance(bundle.get("meta"), dict):
                        bundle["meta"] = meta
                    meta["rm_drop_reason"] = RM_MM_BUDGET_EXHAUSTED
                except Exception:
                    pass
                inc_rm_reject(reason=RM_MM_BUDGET_EXHAUSTED)
                return False, caps_local, trade_mode

            # 1.b) Réservation dans le wallet global MM
            wallet_ok = False
            try:
                wallet_ok = bool(
                    self._mm_wallet_reserve(
                        exchange,
                        quote,
                        bundle_notional,
                        dry_run=False,
                        reason="bundle_admission",
                        profile=profile_u,
                    )
                )
            except Exception:
                wallet_ok = False

            if not wallet_ok:
                logging.info(
                    "[RM] MM_GLOBAL_BUDGET_EXHAUSTED: profile=%s ex=%s quote=%s pair=%s "
                    "notional=%.4f remaining_wallet=%.4f",
                    profile_u,
                    exchange,
                    quote,
                    pair_norm,
                    bundle_notional,
                    float(self._mm_wallet_remaining(exchange, quote)),
                )
                # Budget virtuel MM épuisé (virtual wallet)
                try:
                    if RM_MM_BUDGET_EXHAUSTED_TOTAL is not None:
                        RM_MM_BUDGET_EXHAUSTED_TOTAL.labels(profile_u, exchange, quote).inc()
                except Exception:
                    pass

                # Expose la cause au reste du pipeline (shadow/logs) sans changer le contrat public
                try:
                    if not isinstance(bundle.get("meta"), dict):
                        bundle["meta"] = meta
                    meta["rm_drop_reason"] = RM_MM_BUDGET_EXHAUSTED
                except Exception:
                    pass
                inc_rm_reject(reason=RM_MM_BUDGET_EXHAUSTED)

                return False, caps_local, trade_mode
            # 1.c) Mise à jour du registre mm_pair_spent_usdc (notional inflight par paire)
            key = (profile_u, exchange, pair_norm)
            prev_spent = float(self.mm_pair_spent_usdc.get(key, 0.0))
            self.mm_pair_spent_usdc[key] = prev_spent + bundle_notional
            logging.debug(
                "[RM] MM wallet reserve: profile=%s ex=%s pair=%s quote=%s bundle_notional=%.4f "
                "spent_before=%.4f spent_after=%.4f wallet_remaining=%.4f",
                profile_u,
                exchange,
                pair_norm,
                quote,
                bundle_notional,
                prev_spent,
                self.mm_pair_spent_usdc[key],
                float(self._mm_wallet_remaining(exchange, quote)),
            )

        # --- 2) Pour les autres branches : on laisse caps_local inchangé -------
        return True, caps_local, trade_mode

    def _record_mm_disabled(
        self,
        by: str,
        *,
        branch: str | None = None,
        profile: str | None = None,
    ) -> None:
        """
        Observabilité RM — savoir qui a coupé MM (RM / PACER / CAPITAL).

        `by` doit être l'un de "RM", "PACER", "CAPITAL" (fallback "UNKNOWN").
        """
        try:
            by_label = str(by or "UNKNOWN").upper()
            if by_label not in ("RM", "PACER", "CAPITAL"):
                by_label = "UNKNOWN"

            branch_label = str(branch or "MM").upper() or "MM"
            profile_label = str(profile or "UNKNOWN").upper() or "UNKNOWN"

            if hasattr(self, "obs_inc"):
                self.obs_inc(
                    "rm_mm_disabled_total",
                    by=by_label,
                    branch=branch_label,
                    profile=profile_label,
                )
        except Exception:
            # Pure obs : jamais bloquant
            pass


    def _record_engine_backpressure(
            self,
            reason: str,
            branch: str | None = None,
            profile: str | None = None,
    ) -> None:
        """
        Compteur dédié aux rejets Engine (backpressure) + hook Pacer.

        - Normalise les raisons (alias courts → constantes ENGINE_BACKPRESSURE_*).
        - Compte les occurrences par code interne (ENGINE_BACKPRESSURE_*).
        - Dégrade le Pacer si des BACKPRESSURE Queue/Cap se répètent.
        - Publie un compteur spécifique rm_engine_backpressure_total{type, branch, profile}
          + un compteur générique via inc_blocked (obs légère).
        """
        # Normalisation basique des alias (robustesse aux sources hétérogènes)
        raw = str(reason or "").upper().strip()
        alias_map = {
            # Alias courts éventuels
            "QUEUE_FULL": ENGINE_BACKPRESSURE_QUEUE_FULL,
            "CAP_BRANCH": ENGINE_BACKPRESSURE_CAP_BRANCH,
            "HIGH_WM": ENGINE_BACKPRESSURE_HIGH_WM,
            "MM_HIGH_WM": ENGINE_BACKPRESSURE_HIGH_WM,
            # Codes Engine canoniques
            ENGINE_BACKPRESSURE_QUEUE_FULL: ENGINE_BACKPRESSURE_QUEUE_FULL,
            ENGINE_BACKPRESSURE_CAP_BRANCH: ENGINE_BACKPRESSURE_CAP_BRANCH,
            ENGINE_BACKPRESSURE_HIGH_WM: ENGINE_BACKPRESSURE_HIGH_WM,
        }
        code = alias_map.get(raw, raw)

        # Type canonique pour les métriques RM (vue agrégée)
        # NOTE: ENGINE_BACKPRESSURE_HIGH_WM est utilisé pour les rejets MM_HIGH_WM
        # dans l'Engine (branche MM), on le mappe donc sur "MM_HIGH_WM".
        bp_type = "OTHER"
        if code == ENGINE_BACKPRESSURE_QUEUE_FULL or raw == "QUEUE_FULL":
            bp_type = "QUEUE_FULL"
        elif code == ENGINE_BACKPRESSURE_CAP_BRANCH or raw == "CAP_BRANCH":
            bp_type = "CAP_BRANCH"
        elif code == ENGINE_BACKPRESSURE_HIGH_WM:
            bp_type = "MM_HIGH_WM"

        # Compteur interne par code (best-effort)
        try:
            if not hasattr(self, "_engine_backpressure_counts"):
                self._engine_backpressure_counts = defaultdict(int)
            self._engine_backpressure_counts[code] += 1
        except Exception:
            # Si on ne peut pas compter proprement, on ne fait rien de plus.
            return

        # Observabilité dédiée côté RM: rm_engine_backpressure_total{type, branch, profile}
        try:
            branch_label = (str(branch or "") or "UNKNOWN").upper()
            profile_label = (str(profile or "") or "UNKNOWN").upper()
            if hasattr(self, "obs_inc"):
                self.obs_inc(
                    "rm_engine_backpressure_total",
                    type=bp_type,
                    branch=branch_label,
                    profile=profile_label,
                )
        except Exception:
            # L'obs ne doit jamais casser la logique de backpressure
            pass

        # Observabilité légère via inc_blocked (ne casse jamais)
        try:
            # category="rm", reason="engine_backpressure", detail=code
            inc_blocked("rm", "engine_backpressure", code)
        except Exception:
            pass

        # Dégradation du Pacer si backpressure dur récurrent
        threshold = int(getattr(self, "engine_backpressure_degrade_threshold", 3) or 3)
        if code in {ENGINE_BACKPRESSURE_QUEUE_FULL, ENGINE_BACKPRESSURE_CAP_BRANCH} and \
                self._engine_backpressure_counts[code] >= threshold:
            try:
                logger.warning(
                    "[RiskManager] pacer degrade source=engine_backpressure reason=%s count=%s",
                    code,
                    self._engine_backpressure_counts[code],
                )
            except Exception:
                pass
            # Macro 7-D : observabilité des dégradations Pacer déclenchées par le RM
            try:
                if hasattr(self, "obs_inc"):
                    self.obs_inc(
                        "rm_pacer_degrade_total",
                        reason=str(code),
                        source="engine_backpressure",
                    )
            except Exception:
                # L'observabilité ne doit pas empêcher la dégradation Pacer
                pass

            try:
                if hasattr(self, "pacer") and hasattr(self.pacer, "degrade"):
                    self.pacer.degrade(source="engine_backpressure")
            except Exception:
                pass
            # Reset du compteur pour ce code après action
            self._engine_backpressure_counts[code] = 0


    def _can_reserve_quote(self, ex: str, alias: str, quote: str, usd: float) -> bool:
        """
        Vérifie qu'on a assez de cash en devise de cotation (USDC/EUR) sur (exchange, alias).
        Pas d'écriture: réservation 'logique' pour gating risque.
        """
        try:
            have = float(self._available_quote(ex, alias, quote))
            return have >= float(usd or 0.0)
        except Exception:
            return False

    def _is_mm_admissible_from_hints(self, opp: Dict[str, Any]) -> tuple[bool, str]:
        """
        Lit les hints MM produits par le Scanner et applique les garde-fous RM.

        Expects opp["hints"]["MM"] with depth/qpos/net_bps/p_both/vol/ttl and
        opp["hints"]["expected_net_bps"]["MM"] (fallback opp["expected_net_bps"]["MM"]).
        """
        hints_mm = ((opp.get("hints") or {}).get("MM") or {})
        exp_net = ((opp.get("hints") or {}).get("expected_net_bps") or {}).get("MM")
        if exp_net is None:
            exp_net = (opp.get("expected_net_bps") or {}).get("MM")

        try:
            net_bps = float(exp_net or hints_mm.get("net_bps") or 0.0)
        except Exception:
            net_bps = 0.0

        # Paramètres de garde-fous (QUOTE)
        cfg_root = getattr(self, "cfg", None)
        mm_depth_min = float(getattr(cfg_root, "mm_depth_min_quote", getattr(cfg_root, "mm_depth_min_usd", 0.0)) or 0.0)
        mm_qpos_max = float(
            getattr(cfg_root, "mm_qpos_max_quote", getattr(cfg_root, "mm_qpos_max_ahead_usd", 1e12)) or 1e12)
        mm_min_p_both = float(getattr(cfg_root, "mm_min_p_both", 0.0) or 0.0)
        mm_vol_max = float(getattr(cfg_root, "mm_vol_max_bps_ema", 1e9) or 1e9)
        mm_min_net_bps = float(getattr(cfg_root, "mm_min_net_bps", 0.0) or 0.0)

        try:
            depth = hints_mm.get("depth") or {}
            depth_a = float(depth.get("A", 0.0))
            depth_b = float(depth.get("B", 0.0))
        except Exception:
            depth_a = depth_b = 0.0

        try:
            qpos = hints_mm.get("qpos") or {}
            qpos_a = float(qpos.get("A", 0.0))
            qpos_b = float(qpos.get("B", 0.0))
        except Exception:
            qpos_a = qpos_b = 0.0

        try:
            p_both = float(hints_mm.get("p_both", 0.0))
        except Exception:
            p_both = 0.0

        try:
            vol_bps = float(hints_mm.get("vol_bps_ema", 0.0))
        except Exception:
            vol_bps = 0.0

        if net_bps < mm_min_net_bps:
            return False, "MM_HINTS_GUARD_NET_BPS"
        if depth_a < mm_depth_min or depth_b < mm_depth_min:
            return False, "MM_HINTS_GUARD_DEPTH"
        if qpos_a > mm_qpos_max or qpos_b > mm_qpos_max:
            return False, "MM_HINTS_GUARD_QPOS"
        if vol_bps > mm_vol_max:
            return False, "MM_HINTS_GUARD_VOL"

        # Gate Microstructure (Imbalance, Churn, Depth Profile)
        try:
            imbalance = hints_mm.get("imbalance") or {}
            churn = hints_mm.get("churn_rate") or {}
            dp = hints_mm.get("depth_profile") or {}

            imb_a = float(imbalance.get("A", 0.0))
            imb_b = float(imbalance.get("B", 0.0))
            churn_a = float(churn.get("A", 0.0))
            churn_b = float(churn.get("B", 0.0))
            dp_a = float(dp.get("A", 0.0))
            dp_b = float(dp.get("B", 0.0))

            mm_churn_max = float(self._get_rm_knob("mm_churn_max", default=100.0))
            mm_dp_min = float(self._get_rm_knob("mm_dp_min", default=0.01))
            mm_imb_max = float(self._get_rm_knob("mm_imb_max", default=0.95))

            if max(churn_a, churn_b) > mm_churn_max:
                return False, "MM_CHURN_TOO_HIGH"
            if min(dp_a, dp_b) < mm_dp_min:
                return False, "MM_DEPTH_PROFILE_FRAGILE"
            if abs(imb_a) > mm_imb_max or abs(imb_b) > mm_imb_max:
                return False, "MM_MICROPRICE_ADVERSE"
        except Exception:
            pass

        # Fee/Rebate Optimized
        fee_net_min_bps = float(self._get_rm_knob("mm_fee_net_min_bps", default=-50.0))
        if net_bps < fee_net_min_bps:
            return False, "MM_FEE_NET_NEGATIVE"

        # Gate Cross-CEX : staleness + “both healthy” (public + private)
        strat = str(opp.get("strategy") or "").upper()
        is_cross = (strat == "MM_CROSS")

        if is_cross:
            # Stricter p_both pour le cross
            p_both_min_cross = float(getattr(self, "mm_p_both_min_cross", 0.8))
            if p_both < p_both_min_cross:
                if hasattr(self, "obs_inc"):
                    self.obs_inc("mm_cross_blocked_total", reason="MM_CROSS_P_BOTH_TOO_LOW")
                return False, "MM_CROSS_P_BOTH_TOO_LOW"

            # Private WS health pour les DEUX exchanges
            buy_ex = str(opp.get("buy_ex") or "").upper()
            sell_ex = str(opp.get("sell_ex") or "").upper()

            if not self._is_exchange_private_ws_healthy(buy_ex) or \
                    not self._is_exchange_private_ws_healthy(sell_ex):
                if hasattr(self, "obs_inc"):
                    self.obs_inc("mm_cross_blocked_total", reason="MM_CROSS_EX_UNHEALTHY_PRIVATE")
                return False, "MM_CROSS_EX_UNHEALTHY_PRIVATE"

        if p_both < mm_min_p_both:
            return False, "MM_HINTS_GUARD_P_BOTH"

        if not getattr(self, "mm_dual_guard_enabled", True):
            return True, ""

        # Spread un peu plus strict pour autoriser BID+ASK simultanés
        dual_min_net = float(getattr(self, "mm_dual_min_net_bps", mm_min_net_bps) or 0.0)
        if net_bps < dual_min_net:
            return False, "MM_DUAL_SPREAD_TOO_SMALL"

        # Profondeur mini sur les deux côtés (en devise de cotation)
        try:
            depth = hints_mm.get("depth") or {}
            depth_a = float(depth.get("A", 0.0))
            depth_b = float(depth.get("B", 0.0))
        except Exception:
            depth_a = depth_b = 0.0

        min_depth_dual = float(
            getattr(self, "mm_dual_min_depth_quote", mm_depth_min) or 0.0
        )
        if min(depth_a, depth_b) < min_depth_dual:
            return False, "MM_DUAL_DEPTH_TOO_SHALLOW"

        # Inventaire : ne faire du 2-côtés que si le skew reste raisonnable
        try:
            snap = self._balances_snapshot()
            base = self._pair_base(self._norm_pair(opp.get("pair") or opp.get("symbol") or ""))
            skew_pct = self._skew_pct(snap, base, self._norm_pair(opp.get("pair") or opp.get("symbol") or ""))
        except Exception:
            skew_pct = 0.0

        max_skew = float(getattr(self, "mm_dual_max_skew_pct", self.inventory_skew_max_pct) or 0.0)
        if abs(skew_pct) > max_skew:
            return False, "MM_DUAL_INVENTORY_SKEW_TOO_HIGH"

        # Gate 1 — Staleness strict + fail-closed (spécifique MM DUAL)
        # Raison : en DUAL, un stale quote = adverse selection quasi garantie.
        mm_stale_ms_dual = float(getattr(self, "mm_stale_ms_dual", 350.0))
        mm_mode = (opp.get("meta") or {}).get("mm_mode", "DUAL")
        if mm_mode == "DUAL":
            book_age_ms = float(opp.get("book_age_ms") or 0.0)
            # Vérification par rapport au temps local (fail-closed)
            import time
            now_ms = time.time() * 1000.0
            ts_buy = float(opp.get("ts_buy_ex_ms") or 0.0)
            ts_sell = float(opp.get("ts_sell_ex_ms") or 0.0)
            
            # On prend le max des âges des deux côtés par rapport au temps local
            age_buy = now_ms - ts_buy if ts_buy > 0 else 0.0
            age_sell = now_ms - ts_sell if ts_sell > 0 else 0.0
            
            if book_age_ms > mm_stale_ms_dual or max(age_buy, age_sell) > mm_stale_ms_dual:
                if hasattr(self, "obs_inc"):
                    self.obs_inc("mm_reject_total", reason="MM_STALE_BOOK_DUAL")
                return False, "MM_STALE_BOOK_DUAL"

        return True, ""

    def _mm_profile_params(self, capital_profile: str | None = None) -> Dict[str, Any]:
        p_requested = str(capital_profile or self.capital_profile or "SMALL").upper()
        slot_map = self.mm_slot_notional_usdc_by_profile or {}
        pair_ratio_map = self.mm_pair_cap_ratio_by_profile or {}
        slots_per_pair_map = self.mm_slots_per_pair_by_profile or {}

        def _resolve(map_obj: Dict[str, Any], default: Any) -> tuple[str, Any]:
            if not map_obj:
                return p_requested, default
            if p_requested in map_obj:
                return p_requested, map_obj[p_requested]
            if "SMALL" in map_obj:
                return "SMALL", map_obj["SMALL"]
            key, value = next(iter(map_obj.items()))
            return str(key).upper(), value

        slot_profile, slot_value = _resolve(slot_map, 0.0)
        pair_profile, pair_value = _resolve(pair_ratio_map, 0.0)
        slots_profile, slots_value = _resolve(slots_per_pair_map, 0)
        effective_profile = slot_profile or pair_profile or slots_profile or p_requested

        return {
            "profile": effective_profile,
            "slot_notional_usdc": float(self._as_float_or(slot_value, 0.0)),
            "pair_cap_ratio": float(self._as_float_or(pair_value, 0.0)),
            "slots_per_pair": int(self._as_int_or(slots_value, 0)),
        }

    def get_mm_slot_params(self, capital_profile: str | None = None) -> Dict[str, Any]:
        return self._mm_profile_params(capital_profile)

    def _mm_slot_notional_for_profile(self, profile: str, *, min_trade_usdc: float) -> float:
        """Renvoie la taille cible d'un slot MM pour le profil capital donné."""
        params = self._mm_profile_params(profile)
        slot = float(params.get("slot_notional_usdc", 0.0))
        slot = max(slot, float(min_trade_usdc))
        return slot

    # modules/risk_manager.py — class RiskManager

    # risk_manager.py — class RiskManager

    # modules/risk_manager.py — class RiskManager

    # risk_manager.py — class RiskManager

    def _is_branch_eligible(self, branch: str, profile: str) -> tuple[bool, str]:
        """
        Gate haut niveau avant allocation de capital sur un bundle.

        Règles métier Macro 4 / Macro 5 (pré-Engine) :
        - Kill switch global coupe toutes les branches.
        - Branch MM désactivée si enable_mm=False.
        - En mode SEVERE (PnL guard), la branche MM est coupée (MM=0),
          TT/TM/REB restent autorisées mais sous caps/pacer/TTL renforcés.
        - Les autres gardes (budget quotidien, net floor, etc.) sont appliquées
          plus loin dans la chaîne, au plus près du sizing.

        Retourne (True, "") si la branche est éligible, sinon (False, REASON).
        """
        try:
            b = str(branch or "TT").upper()
            p = str(profile or "LARGE").upper()
        except Exception:
            b = "TT"
            p = "LARGE"

        # 1) Kill switch global (ops) : coupe toutes les branches.
        if getattr(self, "global_kill_switch", False):
            return False, "GLOBAL_KILL_SWITCH"

        # 2) Branch MM désactivée par config (enable_mm=False ou mm_mode=OFF).
        if b == "MM" and not bool(getattr(self, "enable_mm", False)):

            try:
                self._record_mm_disabled("RM", branch=b, profile=p)
            except Exception:
                pass
            return False, "MM_DISABLED"

        # 3) Mode SEVERE (PnL guard) : MM = 0, les autres branches passent encore.
        mode = str(getattr(self, "rm_mode", "NORMAL") or "NORMAL").upper()
        if mode == "SEVERE" and b == "MM":
            # MM coupé par le mode SEVERE (PnL-guard)
            try:
                self._record_mm_disabled("RM", branch=b, profile=p)
            except Exception:
                pass
            return False, "RM_MODE_SEVERE_MM_OFF"

        # Hooks futurs : mute par branche/profil, netfloor global, etc.
        return True, ""


    def engine_enqueue_bundle(self, bundle: Dict[str, Any], decision_ctx: Optional[Dict[str, Any]] = None) -> bool:
        """
                Point central pour envoyer un bundle vers l'Engine en appliquant:
                - éligibilité RM (branch/profile/pacer/drawdowns),
                - TTL balances par alias (MBF → RM),
                - dérivation d'un capital_mode consolidé pour le bundle
                  ("OK" / "CONSTRAINED" / "BLOCKED"),
                - caps notionnels (TT/TM/MM/REB) + préemption MM,
                - intégration shadow (simulateur) si actif.

                Retourne True si le bundle a été accepté par l'Engine.
                """

        def _record_decision(ok: bool, reason: str = "") -> None:
            if decision_ctx is None:
                return
            decision_ctx["attempted"] = decision_ctx.get("attempted") or True
            if reason:
                decision_ctx.setdefault("reasons", []).append(str(reason))
            if ok:
                decision_ctx["submitted"] = True

        if not self.engine:
            logging.warning("RM : engine indisponible, drop bundle")
            _record_decision(False, RM_ENGINE_NOT_READY)
            return False

        # 1) BLOCKED/DEGRADED trading_state (private plane / truth) => jamais de bundle Engine
        if str(getattr(self, "trading_state", "READY")).upper() != "READY":
            reason = normalize_reason_code(
                getattr(self, "trading_state_reason", None) or "PWS_QUEUE_BACKPRESSURE_TIMEOUT"
            ) or "PWS_QUEUE_BACKPRESSURE_TIMEOUT"
            logging.warning("RM : trading blocked (%s), drop bundle (trace_id=%s)", reason, bundle.get("trace_id") or "NA")
            _record_decision(False, reason)
            return False

        # 2) PIPELINE_READY gate (permet DRY_RUN infra verte)
        ev_pipe = getattr(self, "pipeline_ready_event", None) or getattr(self, "trading_ready_event", None)
        if ev_pipe is not None and (not ev_pipe.is_set()):
            reason = normalize_reason_code("PIPELINE_NOT_READY") or "PIPELINE_NOT_READY"
            logging.warning("RM : pipeline not ready, drop bundle (trace_id=%s)", bundle.get("trace_id") or "NA")
            _record_decision(False, reason)
            return False

        # 3) TRADING gate (respect strict des 3 modes)
        # - DRY_RUN           : on laisse passer (Engine ne doit pas trader réellement)
        # - PROD + KILL_SWITCH: on bloque net
        # - FULL PROD (armed) : on laisse passer
        cfg = getattr(self, "cfg", None)
        rm_cfg = getattr(cfg, "rm", cfg)
        g_cfg = getattr(cfg, "g", None)

        dry_run = bool(getattr(rm_cfg, "dry_run", False))
        kill = bool(getattr(rm_cfg, "global_kill_switch", False)) or bool(getattr(self, "global_kill_switch", False))
        armed = bool(getattr(g_cfg, "live_trading_armed", False))

        if not dry_run:
            if kill:
                reason = normalize_reason_code("GLOBAL_KILL_SWITCH") or "GLOBAL_KILL_SWITCH"
                logging.warning("RM : kill switch ON, drop bundle (trace_id=%s)", bundle.get("trace_id") or "NA")
                _record_decision(False, reason)
                return False
            if not armed:
                reason = normalize_reason_code("TRADING_NOT_ARMED") or "TRADING_NOT_ARMED"
                logging.warning("RM : not armed, drop bundle (trace_id=%s)", bundle.get("trace_id") or "NA")
                _record_decision(False, reason)
                return False


        # Rafraîchit le mode consolidé et ses overlays avant d'exposer au moteur
        try:
            self._update_trade_mode()
        except Exception:
            # Défensif : ne jamais casser le flux si la consolidation échoue
            self.trade_mode = getattr(self, "trade_mode", "NORMAL") or "NORMAL"
        self._apply_mode_overrides()


        meta = bundle.get("meta") or {}
        meta.setdefault("mode", self.trade_mode)
        meta.setdefault("mode_overrides", dict(getattr(self, "_current_mode_overrides", {}) or {}))
        if self.trade_mode == "OPPORTUNISTE" and self.rm_mode in ("OPP_VOLUME", "OPP_VOL"):
            meta.setdefault("mode_overrides", {}).setdefault("submode", self.rm_mode)

        # Macro 4 — capital_mode : vue consolidée du plane capital/TTL pour ce bundle.
        # Par défaut on reste "UNKNOWN" tant que les règles TTL n'ont pas encore été appliquées.
        meta.setdefault("capital_mode", "UNKNOWN")

        bundle["meta"] = meta

        # Branche métier (TT/TM/MM/REB) : toujours dérivée du payload, jamais d'état global.
        raw_branch = (
                meta.get("branch")
                or bundle.get("strategy")
                or bundle.get("branch")
                or "TT"
        )
        branch = str(raw_branch or "TT").upper()

        # Profil capital : payload → attribut RM → config, avec fallback conservateur.
        raw_profile = (
                meta.get("profile")
                or bundle.get("profile")
                or getattr(self, "capital_profile", None)
                or getattr(getattr(self, "cfg", None), "capital_profile", None)
                or getattr(getattr(self, "cfg", None), "capital_profile_name", None)
                or "LARGE"
        )
        profile = str(raw_profile or "LARGE").upper()

        priority = int(meta.get("priority") or 0)
        trace_id = meta.get("trace_id") or bundle.get("trace_id") or "NA"
        quote = str(meta.get("quote") or "USDC").upper()
        if not self._is_flow_allowed_under_current_mode(meta):
            flow_kind = str(meta.get("flow_kind") or "core").lower()
            risk_effect = str(meta.get("risk_effect") or "risk_increasing").lower()
            logging.getLogger(__name__).warning(
                "[RM][FILTER] flow_dropped_by_mode trade_mode=%s flow_kind=%s risk_effect=%s trace_id=%s",
                getattr(self, "trade_mode", "NORMAL"),
                flow_kind,
                risk_effect,
                trace_id,
            )
            if self._shadow:
                try:
                    self._shadow.on_bundle_drop(bundle, "FLOW_FILTERED_BY_MODE")
                except Exception:
                    pass
            _record_decision(False, "FLOW_FILTERED_BY_MODE")

            return False


        # Macro 7-C — Contexte RM / Engine / Pacer attaché au bundle pour debug
        try:
            rm_mode = str(getattr(self, "rm_mode", "NORMAL") or "NORMAL").upper()
        except Exception:
            rm_mode = "NORMAL"

        try:
            pacer_mode = str(getattr(self, "pacer_mode", "UNKNOWN") or "UNKNOWN").upper()
        except Exception:
            pacer_mode = "UNKNOWN"

        try:
            trade_mode = str(getattr(self, "trade_mode", "NORMAL") or "NORMAL").upper()
        except Exception:
            trade_mode = "NORMAL"

        ctx = meta.get("rm_engine_pacer_ctx") or {}
        if not isinstance(ctx, dict):
            ctx = {}

        # On ne surcharge jamais ce que l'appelant aurait éventuellement posé.
        ctx.setdefault("rm_mode", rm_mode)
        ctx.setdefault("trade_mode", trade_mode)
        ctx.setdefault("pacer_mode", pacer_mode)
        ctx.setdefault("capital_mode", meta.get("capital_mode", "UNKNOWN"))
        ctx.setdefault("branch", branch)
        ctx.setdefault("profile", profile)
        ctx.setdefault("quote", quote)

        meta["rm_engine_pacer_ctx"] = ctx
        bundle["meta"] = meta

        # 1) Éligibilités de base (branch / profile / pacer / draws / netfloor…)
        eligible, reason = self._is_branch_eligible(branch, profile)
        if not eligible:
            if self._shadow:
                self._shadow.on_bundle_drop(bundle, reason or "INELIGIBLE")
            _record_decision(False, reason or "INELIGIBLE")
            return False

        # 1.b) Gate TTL balances (MBF → RM) avant d'autoriser du capital
        ttl_info = self._check_balance_ttl_for_bundle(bundle)

        # Par défaut : aucun signal TTL exploitable.
        status: Optional[str] = None
        ex_ttl: Optional[str] = None
        alias_ttl: Optional[str] = None
        age_s: float = 0.0
        capital_at_risk: bool = False
        alias_cap_factor: float = 1.0
        ws_info: Dict[str, Any] = {}

        if ttl_info:
            status = ttl_info["status"]
            ex_ttl = ttl_info["exchange"]
            alias_ttl = ttl_info["alias"]
            age_s = float(ttl_info.get("age_s") or 0.0)

            # Macro 4 — dérive un capital_mode consolidé depuis le statut TTL effectif.
            if status == "BLOCKED":
                meta["capital_mode"] = "BLOCKED"
            elif status == "DEGRADED":
                # Alias dégradé : capital contraint (caps down-clampés, MM coupé plus bas).
                meta["capital_mode"] = "CONSTRAINED"
            else:
                # Par construction _check_balance_ttl_for_bundle ne renvoie que DEGRADED/BLOCKED,
                # mais on reste défensif pour les évolutions futures.
                meta.setdefault("capital_mode", "OK")
            bundle["meta"] = meta

            ws_info = ttl_info.get("ws_accounts") or {}
            capital_at_risk = bool(ws_info.get("capital_at_risk"))

            # Facteur de down-clamp de caps appliqué sur cet alias (0.0–1.0, défensif).
            try:
                alias_cap_factor = float(ttl_info.get("alias_cap_factor", 1.0))
                if alias_cap_factor < 0.0:
                    alias_cap_factor = 0.0
                elif alias_cap_factor > 1.0:
                    alias_cap_factor = 1.0
            except Exception:
                alias_cap_factor = 1.0

            # Si l'alias est marqué "capital_at_risk" par la chaîne WS,
            # on déclenche/priorise un resync balances ciblé (throttlé).
            if capital_at_risk:
                self._schedule_balance_resync_for_alias(ex_ttl, alias_ttl)

            if status == "BLOCKED":
                reason_ttl = f"BALANCE_STALE:{ex_ttl}.{alias_ttl}:{age_s:.1f}s"
                self._obs_balance_ttl_breach(ex_ttl, alias_ttl, status)
                logging.warning(
                    "RM: drop bundle %s (branch=%s, profile=%s) pour alias %s.%s "
                    "stale (age=%.1fs, capital_at_risk=%s)",
                    trace_id,
                    branch,
                    profile,
                    ex_ttl,
                    alias_ttl,
                    age_s,
                    capital_at_risk,
                )
                if self._shadow:
                    self._shadow.on_bundle_drop(bundle, reason_ttl)
                _record_decision(False, reason_ttl)
                return False

            if status == "DEGRADED":
                # Politique Ticket 2 + Ticket 7:
                # - On coupe les branches non critiques (MM) sur alias dégradé.
                # - TT / TM / REB passent encore mais marqués en overlay pour dashboard.
                self._obs_balance_ttl_breach(ex_ttl, alias_ttl, status)
                try:
                    ttl_cap_factor = float(self._get_alias_ttl_cap_factor(status, branch))
                except Exception:
                    ttl_cap_factor = 1.0
                if ttl_cap_factor < 0.0:
                    ttl_cap_factor = 0.0
                if ttl_cap_factor > 1.0:
                    ttl_cap_factor = 1.0
                if ttl_cap_factor < alias_cap_factor:
                    alias_cap_factor = ttl_cap_factor

                if branch == "MM":
                    reason_ttl = f"BALANCE_TTL_DEGRADED:{ex_ttl}.{alias_ttl}"
                    logging.info(
                        "RM: drop bundle %s branch=MM en mode DEGRADED pour alias %s.%s "
                        "(age=%.1fs, capital_at_risk=%s)",
                        trace_id,
                        ex_ttl,
                        alias_ttl,
                        age_s,
                        capital_at_risk,
                    )
                    if self._shadow:
                        self._shadow.on_bundle_drop(bundle, reason_ttl)
                    return False

            # Pour les branches critiques, on laisse passer mais on taggue l'overlay.
        overlays = meta.setdefault("overlays", {})
        overlays_ttl = {
            # Statut TTL effectif pour ce bundle (après surcouche WS/capital_move).
            "status": status,
            "effective_status": status,
            "exchange": ex_ttl,
            "alias": alias_ttl,
            "age_s": age_s,
            "capital_at_risk": capital_at_risk,
            "alias_cap_factor": alias_cap_factor,
            # Copie du capital_mode consolidé exposé au moteur.
            "capital_mode": str(meta.get("capital_mode") or "UNKNOWN").upper(),
        }
        if ws_info:
            overlays_ttl["ws_accounts"] = dict(ws_info)
        overlays["balances_ttl"] = overlays_ttl
        bundle["meta"] = meta

        # 1.c) Si aucune info TTL exploitable n'est disponible,
        # on considère le capital_mode comme "OK" (aucune contrainte TTL détectée).
        if not meta.get("capital_mode") or meta.get("capital_mode") == "UNKNOWN":
            meta["capital_mode"] = "OK"
            bundle["meta"] = meta

        # 1-bis) Collat / marge alias-aware pour MM : on coupe si collat CRIT sur un alias du bundle.
        if branch == "MM":
            try:
                aliases = self._iter_bundle_aliases(bundle)
            except Exception:
                aliases = []

            worst_state = "OK"
            worst_alias: Optional[Tuple[str, str]] = None

            coll_state = getattr(self, "alias_collat_state", {}) or {}
            for ex, alias in aliases or []:
                key = (str(ex).upper(), str(alias).upper())
                info = coll_state.get(key)
                if not info:
                    continue
                st = str(info.get("state") or "OK").upper()

                # Observabilité P0 : compter les cas LOW rencontrés pendant le gating.
                if st == "LOW":
                    try:
                        RM_ALIAS_COLLAT_LOW_TOTAL.labels(
                            exchange=key[0],
                            alias=key[1],
                            branch=branch,
                        ).inc()
                    except Exception:
                        pass

                if st == "CRIT":
                    worst_state = "CRIT"
                    worst_alias = key
                    break
                if st == "LOW" and worst_state == "OK":
                    worst_state = "LOW"
                    worst_alias = key

            if worst_state == "CRIT":
                reason = (
                    f"MM_COLLAT_CRIT:{worst_alias[0]}.{worst_alias[1]}"
                    if worst_alias else
                    "MM_COLLAT_CRIT"
                )
                logging.info(
                    "RM: drop bundle %s branch=MM pour collat CRIT sur alias %s.%s",
                    trace_id,
                    worst_alias[0] if worst_alias else "NA",
                    worst_alias[1] if worst_alias else "NA",
                )

                # Compteur dédié pour les drops MM liés au collat CRIT.
                if worst_alias:
                    try:
                        RM_MM_COLLAT_CRIT_DROP_TOTAL.labels(
                            exchange=str(worst_alias[0]).upper(),
                            alias=str(worst_alias[1]).upper(),
                        ).inc()
                    except Exception:
                        pass

                if self._shadow:
                    self._shadow.on_bundle_drop(bundle, reason)
                _record_decision(False, reason)
                return False

        # 2) Légalité & REB lock
        legal, legal_reason = self._bundle_legality_decision(bundle)
        if not legal:
            if self._shadow:
                self._shadow.on_bundle_drop(bundle, legal_reason)
            _record_decision(False, legal_reason)
            return False
        if legal_reason:
            try:
                logging.info(
                    "RM: bundle legality warn-only (%s) trace_id=%s",
                    legal_reason,
                    trace_id,
                )
            except Exception:
                pass

        lock_active, lock_reason = self._is_rebal_lock_active(bundle)
        if lock_active:
            meta = bundle.get("meta") or {}
            if not isinstance(bundle.get("meta"), dict):
                bundle["meta"] = meta
            meta["rm_drop_reason"] = lock_reason
            if lock_reason == REB_LOCK_CHECK_FAILED:
                meta["capital_mode"] = "BLOCKED"
            bundle["meta"] = meta
            if self._shadow:
                self._shadow.on_bundle_drop(bundle, lock_reason)
            _record_decision(False, lock_reason)
            return False

        # 3) Caps notionnels par CEX / profil / branche (+ préemption MM)
        try:
            caps_local = self._get_caps_for_bundle(bundle, branch, profile, quote, meta)
        except Exception:
            reason_caps = RM_CAPS_BROKEN
            pair = (
                    meta.get("pair")
                    or meta.get("symbol")
                    or bundle.get("pair")
                    or bundle.get("symbol")
                    or None
            )
            logging.exception(
                "[RM] caps calculation failed; trace_id=%s branch=%s profile=%s pair=%s",
                trace_id,
                branch,
                profile,
                pair or "NA",
            )
            try:
                inc_blocked("rm", reason_caps, pair)
            except Exception:
                pass
            if not isinstance(bundle.get("meta"), dict):
                bundle["meta"] = meta
            meta["rm_drop_reason"] = reason_caps
            if bool(getattr(getattr(self.cfg, "rm", None), "ff_fail_closed_caps", False)):
                meta["capital_mode"] = "BLOCKED"
                bundle["meta"] = meta
                if self._shadow:
                    self._shadow.on_bundle_drop(bundle, reason_caps)
                _record_decision(False, reason_caps)
                return False
            meta["capital_mode"] = "CONSTRAINED"
            bundle["meta"] = meta
            caps_local = {
                "inflight_cap": None,
                "bundle_concurrency": 1,
                "headroom_min": int(getattr(self, "inflight_headroom_min", 1) or 0),
                "alias_cap_factor": 1.0,
            }

        ok, caps_local, trade_mode = self._apply_caps_and_preempt(
            bundle=bundle,
            caps_local=caps_local,
            profile=profile,
        )
        if not ok:
            if self._shadow:
                drop_reason = "CAPS_PREEMPT"
                try:
                    drop_reason = str((bundle.get("meta") or {}).get("rm_drop_reason") or drop_reason)
                except Exception:
                    pass
                self._shadow.on_bundle_drop(bundle, drop_reason)
            _record_decision(False, "REB_LOCK")
            return False


        # 3.a) Cap global par combo (TT+TM+REB) — Ticket 6-RM-2
        combo_cap_usd = self._compute_combo_cap_for_bundle(
            bundle=bundle,
            branch=branch,
            profile=profile,
            quote=quote,
            caps_local=caps_local,
        )
        if combo_cap_usd is not None and combo_cap_usd > 0.0:
            notional_usd = self._estimate_bundle_notional_usd(bundle)
            combo_key = self._get_combo_key_from_bundle(bundle) or ""
            now_ts = time.time()
            inflight_combo = self._get_combo_inflight_notional(combo_key, now_ts) if combo_key else 0.0
            prospective_total = notional_usd + inflight_combo
            if prospective_total > combo_cap_usd:
                ttl_status = ""
                try:
                    meta = bundle.get("meta") or {}
                    overlays = meta.get("overlays") or {}
                    ttl_overlay = overlays.get("balances_ttl") or {}
                    if isinstance(ttl_overlay, dict):
                        ttl_status = str(ttl_overlay.get("status") or "").upper()
                except Exception:
                    ttl_status = ""
                reason_combo = RM_CAP_COMBO_EXCEEDED
                try:
                    self._obs_combo_cap_reject(
                        combo_key=combo_key or "NA",
                        branch=branch,
                        profile=profile,
                        ttl_status=ttl_status,
                    )
                except Exception:
                    pass

                try:
                    logging.info(
                        "RM: drop bundle for combo cap %s (branch=%s, profile=%s, quote=%s, trace_id=%s, ttl_status=%s, inflight_usd=%.4f, notional_usd=%.4f, combo_cap_usd=%.4f)",
                        combo_key or "NA",
                        branch,
                        profile,
                        quote,
                        trace_id,
                        ttl_status,
                        float(inflight_combo or 0.0),
                        float(notional_usd or 0.0),
                        float(combo_cap_usd or 0.0),
                    )
                except Exception:
                    pass

                if self._shadow:
                    self._shadow.on_bundle_drop(bundle, reason_combo)
                _record_decision(False, reason_combo)
                return False

            if combo_key and notional_usd > 0.0:
                self._register_combo_inflight_notional(combo_key, notional_usd, now_ts)


        # 3.a bis) Soft caps par sub-compte (SC) — Rate limiting SC (Macro 10)
        if not self._should_bypass_sc_rl(bundle, branch):
            ok_rl, rl_reason = self._check_sc_softcap_for_bundle(bundle, branch, profile)
            if not ok_rl:
                reason_sc = rl_reason or "RL_SC_SOFTCAP"
                try:
                    logging.info(
                        "RM: drop bundle %s (branch=%s, profile=%s) pour RL SC (%s)",
                        trace_id,
                        branch,
                        profile,
                        reason_sc,
                    )
                except Exception:
                    pass
                if self._shadow:
                    self._shadow.on_bundle_drop(bundle, reason_sc)
                _record_decision(False, reason_sc)
                return False


        # 3.b) Mode consolidé & overrides → Engine (Ticket 11)
        try:
            trade_mode = str(getattr(self, "trade_mode", "NORMAL")).upper()
        except Exception:
            trade_mode = "NORMAL"

        # Overrides exposés à l’Engine pour ce bundle
        ioc_only = bool(getattr(self, "_ioc_only", False))
        mm_enabled = bool(getattr(self, "enable_mm", False))

        mode_overrides = dict(meta.get("mode_overrides") or {})
        mode_overrides.update(
            {
                "ioc_only": ioc_only,
                "mm_enabled": mm_enabled,
                # Optionnel : on expose aussi le sous-mode interne pour debug
                "rm_mode": str(getattr(self, "rm_mode", "NORMAL")).upper(),
            }
        )
        meta["mode"] = trade_mode
        meta["mode_overrides"] = mode_overrides
        bundle["meta"] = meta

        # 4) Passage au moteur
        try:
            accepted = None
            submitter = None
            route = bundle.get("route") or {}
            try:
                ex_key = str(route.get("buy_ex") or route.get("sell_ex") or meta.get("exchange") or "").upper()
            except Exception:
                ex_key = ""
            if hasattr(self, "exec_submitters"):
                submitter = (self.exec_submitters or {}).get(ex_key)
            if submitter is not None:
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # P0-3: launch task if loop is running to avoid RuntimeError
                        def _done_cb(fut: asyncio.Task):
                            try:
                                res = fut.result()
                                # On peut logger ici si res indique un échec différé
                                if isinstance(res, dict) and str(res.get("state")).upper() != "ENGINE_ACCEPTED":
                                    logging.warning("[RM] Async submit failed: %s", res.get("reason_code"))
                                    self.obs_inc("rm_engine_reject_total", reason=res.get("reason_code") or "ASYNC_REJECT")
                            except Exception as e:
                                logging.error("[RM] Async submit crash: %s", e)
                                self.obs_inc("rm_engine_reject_total", reason="ASYNC_CRASH")

                        task = asyncio.create_task(submitter.submit_bundle(bundle))
                        task.add_done_callback(_done_cb)
                        accepted = True  # Handed over to Engine
                    else:
                        accepted = loop.run_until_complete(submitter.submit_bundle(bundle))  # type: ignore
                except Exception:
                    accepted = None
            if accepted is None:
                accepted = self.engine.execute_bundle(bundle)
        except EngineSubmitError as exc:
            raw_reason = getattr(exc, "reason", None) or str(exc)
            reason = normalize_reason_code(raw_reason) or str(raw_reason)
            try:
                if hasattr(self, "obs_inc"):
                    self.obs_inc("rm_engine_reject_total", reason=reason)
            except Exception:
                pass

            # MM coupé par le plane capital (vu côté Engine)
            try:
                reason_u = reason.upper()
            except Exception:
                reason_u = str(reason or "").upper()

            if "ENGINE_MM_DISABLED_BY_CAPITAL" in reason_u:
                try:
                    self._record_mm_disabled("CAPITAL", branch=branch, profile=profile)
                except Exception:
                    pass

            # Backpressure Engine (Queue / Caps / High WM...)
            # (dépend du patch M7-A : _record_engine_backpressure(reason, branch, profile))
            self._record_engine_backpressure(reason, branch=branch, profile=profile)

            if self._shadow:
                self._shadow.on_bundle_drop(bundle, "ENGINE_REJECT")
            _record_decision(False, reason or "ENGINE_REJECT")
            return False
        if isinstance(accepted, dict):
            state = str(accepted.get("state") or "").upper()
            reason = str(accepted.get("reason_code") or "")
            if state and state != "ENGINE_ACCEPTED":
                _record_decision(False, reason or "ENGINE_REJECT")
                return False
            _record_decision(True, "")
            return True

        if not accepted and self._shadow:
            self._shadow.on_bundle_drop(bundle, "ENGINE_REJECT")
        _record_decision(bool(accepted), "ENGINE_REJECT" if not accepted else "")
        return accepted


    # ------------------------------------------------------------------
    # Légalité bundle & REB lock
    # ------------------------------------------------------------------
    def _bundle_legality_decision(self, bundle: Dict[str, Any]) -> Tuple[bool, str]:
        ok, reason = self._is_bundle_legal(bundle)
        if ok:
            return True, ""
        reason_code = normalize_reason_code(reason or BUNDLE_ILLEGAL) or BUNDLE_ILLEGAL
        enforce = bool(getattr(getattr(self.cfg, "rm", None), "ff_enforce_bundle_legality", False))
        if enforce:
            return False, reason_code
        return True, reason_code

    def _parse_combo_signature(self, raw: Any) -> Optional[Tuple[str, str, str]]:
        if isinstance(raw, (tuple, list)) and len(raw) == 3:
            pair_raw, buy_raw, sell_raw = raw
            return (
                self._norm_pair(str(pair_raw)) if hasattr(self, "_norm_pair") else str(pair_raw).replace("-", "").upper(),
                str(buy_raw or "").upper(),
                str(sell_raw or "").upper(),
            )
        if not isinstance(raw, str):
            return None
        cleaned = raw.strip()
        if not cleaned:
            return None
        parts = cleaned.split("|")
        if len(parts) < 2:
            return None
        pair_raw = parts[0]
        route_part = parts[-1]
        if "->" not in route_part:
            return None
        buy_raw, sell_raw = route_part.split("->", 1)
        return (
            self._norm_pair(str(pair_raw)) if hasattr(self, "_norm_pair") else str(pair_raw).replace("-", "").upper(),
            str(buy_raw or "").upper(),
            str(sell_raw or "").upper(),
        )

    def _bundle_combo_signature(self, bundle: Dict[str, Any]) -> Optional[Tuple[str, str, str]]:
        if not isinstance(bundle, dict):
            return None
        meta = bundle.get("meta") or {}
        route = bundle.get("route") or {}
        combo_sig = meta.get("combo_signature")
        parsed = self._parse_combo_signature(combo_sig)
        if parsed:
            return parsed
        pair = (
                meta.get("pair")
                or meta.get("symbol")
                or bundle.get("pair")
                or bundle.get("symbol")
                or route.get("pair")
        )
        buy_ex = (
                meta.get("buy_ex")
                or route.get("buy_ex")
                or meta.get("to_exchange")
                or route.get("to_exchange")
        )
        sell_ex = (
                meta.get("sell_ex")
                or route.get("sell_ex")
                or meta.get("from_exchange")
                or route.get("from_exchange")
        )
        if not (pair and buy_ex and sell_ex):
            return None
        pair_key = (
            self._norm_pair(str(pair))
            if hasattr(self, "_norm_pair")
            else str(pair).replace("-", "").upper()
        )
        return (pair_key, str(buy_ex).upper(), str(sell_ex).upper())

    def _format_combo_signature(self, combo: Tuple[str, str, str]) -> str:
        pair, buy_ex, sell_ex = combo
        return f"{pair}|{buy_ex}->{sell_ex}"

    def _is_bundle_legal(self, bundle: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Vérifie la « légalité » minimale d'un bundle.
        """
        if not isinstance(bundle, dict):

            return False, BUNDLE_ILLEGAL

        meta = bundle.get("meta") or {}
        branch = ""
        try:
            branch = self._branch_of(meta, bundle)
        except Exception:
            branch = str(meta.get("branch") or bundle.get("branch") or "").upper()

        notional = bundle.get("notional_quote") or {}
        amount = None
        try:
            amount = float(notional.get("amount"))
        except Exception:
            amount = None
        if amount is None or amount <= 0.0:
            return False, "RM_NOTIONAL_QUOTE_INVALID"

        legs = bundle.get("legs") or bundle.get("orders") or []
        if not isinstance(legs, list) or not legs:
            return False, BUNDLE_ILLEGAL

        combo = self._bundle_combo_signature(bundle)
        if not combo:
            return False, BUNDLE_ILLEGAL

        if branch == "REB" and not self._parse_combo_signature(meta.get("combo_signature")):
            return False, BUNDLE_ILLEGAL

        allowed_routes = getattr(self, "allowed_routes", set()) or set()
        if allowed_routes:
            buy_ex, sell_ex = combo[1], combo[2]
            if buy_ex and sell_ex and (buy_ex, sell_ex) not in allowed_routes:
                return False, BUNDLE_ILLEGAL

        return True, ""


    def _is_rebal_lock_active(self, bundle: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Lock combo REB partagé avec TT/TM/MM.

        Principe business :
        - un REB qui démarre sur un combo (pair, buy_ex, sell_ex) pose un lock TTL
          via _reb_locks / is_rebalancing_locked(...),
        - pendant ce lock, on gèle TT/TM/MM sur ce même combo pour éviter
          sur-exposition et “course poursuite” REB vs TT/TM.

        Détails d’implémentation :
        - on ne bloque que les branches TT/TM/MM (pas REB, pas HEDGE isolé),
        - on derive le combo depuis bundle['route'] ou bundle['meta'],
        - on délègue la décision finale à is_rebalancing_locked(...)
          qui s’appuie sur _reb_locks et nettoie les expirations.
        """
        # 1) Sanity minimal sur la structure
        if not isinstance(bundle, dict):
            return False, ""

        meta = bundle.get("meta") or {}

        # 2) Branch du bundle (TT/TM/MM uniquement)
        try:
            branch = self._branch_of(meta, bundle)
        except Exception:
            branch = "UNKNOWN"

        # On ne gèle que les branches de trading “classiques”
        if branch not in ("TT", "TM", "MM"):
            # REB lui-même, HEDGE internes, etc. ne sont pas bloqués par ce hook.
            return False, ""

        combo = self._bundle_combo_signature(bundle)
        if not combo:
            if bool(getattr(getattr(self.cfg, "rm", None), "ff_fail_closed_reb_lock", False)):
                return True, REB_LOCK_CHECK_FAILED
            return False, ""

        # 4) Délégation à la fonction canonique de lock REB
        lock_fn = getattr(self, "is_rebalancing_locked", None)
        if not callable(lock_fn):
            if bool(getattr(getattr(self.cfg, "rm", None), "ff_fail_closed_reb_lock", False)):
                return True, REB_LOCK_CHECK_FAILED
            return False, ""

        try:
            locked = lock_fn(combo[0], combo[1], combo[2])
            if locked is None:
                raise RuntimeError("reb_lock_check_none")
            if bool(locked):
                return True, REB_LOCK
            return False, ""

        except Exception:
            # En cas de problème de lock, on préfère bloquer si le flag est actif.
            if bool(getattr(getattr(self.cfg, "rm", None), "ff_fail_closed_reb_lock", False)):
                return True, REB_LOCK_CHECK_FAILED
            return False, ""

    def _get_combo_key_from_bundle(self, bundle: Dict[str, Any]) -> Optional[str]:
        """
        Extrait un identifiant de combo « PAIR|EX_FROM->EX_TO » à partir d'un bundle.

        Utilisé pour appliquer un cap global par combo (TT+TM+REB), quelle que soit
        la branche qui porte le trade.
        """
        try:
            meta = bundle.get("meta") or {}
        except Exception:
            meta = {}
        try:
            route = bundle.get("route") or {}
        except Exception:
            route = {}

        pair = meta.get("pair") or route.get("pair") or bundle.get("pair") or bundle.get("symbol")
        buy_ex = meta.get("buy_ex") or route.get("buy_ex")
        sell_ex = meta.get("sell_ex") or route.get("sell_ex")

        if not pair or not buy_ex or not sell_ex:
            return None

        try:
            pair_key = self._norm_pair(str(pair))
        except Exception:
            pair_key = (str(pair) or "").replace("-", "").upper()

        buy_u = str(buy_ex).upper()
        sell_u = str(sell_ex).upper()

        if not pair_key or not buy_u or not sell_u:
            return None

        quote = _pair_quote(pair_key)
        return f"{pair_key}|{quote}|{buy_u}->{sell_u}"

    def _get_combo_inflight_notional(self, combo_key: str, now: float) -> float:
        entries = self._combo_inflight_notional.get(combo_key, [])
        if not entries:
            return 0.0
        kept: List[Tuple[float, float]] = []
        total = 0.0
        for expiry, amt in entries:
            if expiry > now:
                kept.append((expiry, amt))
                total += float(amt or 0.0)
        if kept:
            self._combo_inflight_notional[combo_key] = kept
        else:
            self._combo_inflight_notional.pop(combo_key, None)
        return total

    def _register_combo_inflight_notional(self, combo_key: str, amount: float, now: float) -> None:
        if not combo_key:
            return
        if amount <= 0.0:
            return
        expiry = now + float(self._combo_cap_window_s or 0.0)
        entries = self._combo_inflight_notional.get(combo_key, [])
        entries.append((expiry, float(amount)))
        self._combo_inflight_notional[combo_key] = entries

    def _estimate_bundle_notional_usd(self, bundle: Dict[str, Any]) -> float:
        """
        Estimation conservative du notionnel du bundle en « USD-like ».

        Ordre de priorité:
          1) meta["notional_quote"]["amount"] si présent,
          2) champs numériques connus (notional_usdc, notional_usd, volume_*),
          3) fallback: max(qty * px) sur les legs.
        """
        try:
            meta = bundle.get("meta") or {}
        except Exception:
            meta = {}

        # 1) notional_quote structuré
        try:
            nq = meta.get("notional_quote") or bundle.get("notional_quote")
        except Exception:
            nq = None
        if isinstance(nq, dict):
            try:
                amt = float(nq.get("amount") or nq.get("volume") or 0.0)
                if amt > 0.0:
                    return amt
            except Exception:
                pass

        # 2) Champs numériques simples
        for container in (meta, bundle):
            if not isinstance(container, dict):
                continue
            for key in (
                "notional_usdc",
                "notional_usd",
                "notional",
                "volume_selected_quote",
                "volume_quote",
                "volume_usdc",
                "volume_selected_usdc",
            ):
                if key in container:
                    try:
                        v = float(container.get(key) or 0.0)
                        if v > 0.0:
                            return v
                    except Exception:
                        continue

        # 3) Fallback: derive depuis les legs
        legs = bundle.get("legs") or []
        best = 0.0
        if isinstance(legs, (list, tuple)):
            for leg in legs:
                if not isinstance(leg, dict):
                    continue
                try:
                    qty = float(leg.get("qty") or 0.0)
                    px = float(leg.get("px_limit") or leg.get("px") or 0.0)
                    if qty > 0.0 and px > 0.0:
                        notional = abs(qty * px)
                        if notional > best:
                            best = notional
                except Exception:
                    continue
        return best

    def _compute_combo_cap_for_bundle(
        self,
        bundle: Dict[str, Any],
        branch: str,
        profile: str,
        quote: str,
        caps_local: Dict[str, Any],
    ) -> Optional[float]:
        """
        Cap global par combo (TT+TM+REB) — Ticket 6-RM-2.

        Objectif:
          - bornes notionnelles par route (PAIR|EX_FROM->EX_TO) pour TT/TM/REB,
            cohérentes avec les caps par profil,
          - sur-correction prudente en mode DEGRADED (TTL balances) via un facteur
            multiplicatif, sans jamais dépasser les caps de profil.

        Retourne None si aucun cap spécifique n'est applicable.
        """
        branch_u = (branch or "").upper()
        if branch_u not in ("TT", "TM", "REB"):
            # Cap global par combo ciblé uniquement sur les branches de trading.
            return None

        combo_key = self._get_combo_key_from_bundle(bundle)
        if not combo_key:
            return None

        rm_cfg = getattr(getattr(self, "cfg", None), "rm", None)
        profile_u = (profile or "").upper() or "LARGE"

        base_cap: Optional[float] = None

        # 1) Config directe combo_cap_usd_by_profile si disponible.
        try:
            if rm_cfg is not None:
                combo_caps = getattr(rm_cfg, "combo_cap_usd_by_profile", None)
                if isinstance(combo_caps, dict):
                    raw = combo_caps.get(profile_u) or combo_caps.get("LARGE") or 0.0
                    raw_f = float(raw or 0.0)
                    if raw_f > 0.0:
                        base_cap = raw_f
        except Exception:
            base_cap = None

        # 2) Fallback: dériver un cap combo à partir des caps de profil.
        if base_cap is None:
            try:
                inflight_cap_branch = float(caps_local.get("inflight_cap") or 0.0)
            except Exception:
                inflight_cap_branch = 0.0

            tt_cap = tm_cap = reb_cap = 0.0
            try:
                if rm_cfg is not None:
                    caps_trading = getattr(rm_cfg, "caps_trading_by_profile", {}) or {}
                    prof_caps = caps_trading.get(profile_u) or caps_trading.get("LARGE") or {}
                    if isinstance(prof_caps, dict):
                        tt_cap = float(prof_caps.get("TT") or 0.0)
                        tm_cap = float(prof_caps.get("TM") or 0.0)
                    rebal_caps = getattr(rm_cfg, "inflight_rebal_by_profile", {}) or {}
                    reb_cap = float(rebal_caps.get(profile_u) or rebal_caps.get("LARGE") or 0.0)
            except Exception:
                tt_cap = tm_cap = reb_cap = 0.0

            positive_caps = [c for c in (tt_cap, tm_cap, reb_cap) if c > 0.0]
            if positive_caps:
                # Cap combo <= min(cap_TT, cap_TM, cap_REB) pour ne jamais dépasser
                # le budget le plus conservateur.
                base_cap = min(positive_caps)
            else:
                base_cap = inflight_cap_branch

        if not base_cap or base_cap <= 0.0:
            return None

        # 3) Ajustement TTL balances (vue enrichie via overlay balances_ttl)
        ttl_status = ""
        try:
            meta = bundle.get("meta") or {}
            overlays = meta.get("overlays") or {}
            ttl_overlay = overlays.get("balances_ttl") or {}
            if isinstance(ttl_overlay, dict):
                ttl_status = str(ttl_overlay.get("status") or "").upper()
        except Exception:
            ttl_status = ""

        degraded_factor = 1.0
        if ttl_status == "DEGRADED":
            # Facteur configurable (0.0–1.0) pour mode DEGRADED.
            factor_cfg = None
            try:
                if rm_cfg is not None:
                    factor_cfg = getattr(rm_cfg, "combo_ttl_degraded_factor", None)
            except Exception:
                factor_cfg = None
            try:
                degraded_factor = float(factor_cfg)
            except Exception:
                degraded_factor = 0.5
            if degraded_factor < 0.0:
                degraded_factor = 0.0
            if degraded_factor > 1.0:
                degraded_factor = 1.0

        combo_cap = float(base_cap) * float(degraded_factor or 1.0)
        if combo_cap <= 0.0:
            return None

        return combo_cap


    # ---- RiskManager helpers (remplacement complet) ----
    def _cfg(self, key: str, default):
        cfg = getattr(self, "config", None)
        if cfg and hasattr(cfg, key.replace(".", "_")):
            return getattr(cfg, key.replace(".", "_"))
        if isinstance(cfg, dict):
            return cfg.get(key, default)
        return default

    def _ensure_sc_softcap(self):
        if hasattr(self, "_sc_buckets"):
            return
        self._sc_buckets = {}  # (ex, alias, branch) -> _Bucket
        self._sc_default_rps = float(self._cfg("rl.sc.default.rate_per_s", 3.0))
        self._sc_default_burst = float(self._cfg("rl.sc.default.burst", 6.0))

    def _get_sc_bucket(self, key):
        b = self._sc_buckets.get(key)
        if b:
            return b
        ex, alias, branch = key
        rate = self._cfg(f"rl.sc.{ex}.{branch}.rate_per_s", self._sc_default_rps)
        burst = self._cfg(f"rl.sc.{ex}.{branch}.burst", self._sc_default_burst)
        self._sc_buckets[key] = _Bucket(rate, burst)
        return self._sc_buckets[key]

    def _should_bypass_sc_rl(self, bundle: Dict[str, Any], branch: str) -> bool:
        """
        Détermine si le rate limiting SC doit être bypassé pour ce bundle.

        On ne rate-limit pas les flux de secours:
        - HEDGE (y compris PANIC_HEDGE),
        - achats de tokens de frais (fee_token_topup),
        - transferts internes / refresh capital explicite.
        """
        try:
            meta = bundle.get("meta") or {}
        except Exception:
            meta = {}

        kind = str(meta.get("kind") or bundle.get("kind") or "").upper()
        if kind in (
            "HEDGE",
            "PANIC_HEDGE",
            "FEE_TOKEN_TOPUP",
            "FEE_TOPUP",
            "INTERNAL_TRANSFER",
            "INTERNAL",
            "CAPITAL_REFRESH",
        ):
            return True

        br = str(branch or meta.get("branch") or "").upper()
        if br in ("HEDGE", "INTERNAL") or self._is_mm_family(br):
            return True

        return False

    def _check_sc_softcap_for_bundle(
        self,
        bundle: Dict[str, Any],
        branch: str,
        profile: str,
    ):
        """
        Applique le rate limiting « soft cap » par sub-compte
        (exchange, alias, branch) pour un bundle donné.

        Retourne (ok, reason) où:
        - ok = True  : RL SC laisse passer le bundle,
        - ok = False : RL SC rejette le bundle avec reason non nul.
        """
        # Sécurité: si la config RL est absente/cassée, on ne casse pas le flux.
        try:
            self._ensure_sc_softcap()
        except Exception:
            return True, None

        try:
            aliases = self._iter_bundle_aliases(bundle)
        except Exception:
            aliases = []

        if not aliases:
            # Pas d’alias exploitable → pas de RL SC ici.
            return True, None

        branch_u = str(branch or "").upper() or "UNKNOWN"
        if self._is_mm_family(branch_u):
            branch_u = "MM"

        for ex, alias in aliases:
            ex_u = str(ex or "").upper()
            alias_u = str(alias or "").upper()
            key = (ex_u, alias_u, branch_u)

            bucket = self._get_sc_bucket(key)
            ok = bool(bucket.try_acquire())

            if ok:
                # Admission SC
                try:
                    self._obs_sc_counter("rm_rl_sc_admit_total", ex_u, alias_u, branch_u)
                except Exception:
                    pass
                continue

            # Rejet SC pour ce bundle (on logge + métriques)
            try:
                self._obs_sc_counter("rm_rl_sc_reject_total", ex_u, alias_u, branch_u)
            except Exception:
                pass

            reason = f"RL_SC_SOFTCAP:{ex_u}.{alias_u}.{branch_u}"
            return False, reason

        # Tous les SC concernés ont laissé passer
        return True, None


    def _branch_of(self, meta: dict, bundle: dict) -> str:
        b = (meta.get("branch") or bundle.get("branch") or "").upper()
        k = (meta.get("kind") or bundle.get("kind") or "").upper()
        if b == "TT" or k in ("HEDGE", "TAKER"):
            return "TT"
        if b == "TM" or k in ("MAKER_TM", "TM"):
            return "TM"
        if self._is_mm_family(b, k):
            return "MM"
        return "UNKNOWN"

    def _is_flow_allowed_under_current_mode(self, meta: dict) -> bool:
        mode = str(getattr(self, "trade_mode", "NORMAL") or "NORMAL").upper()
        flow_kind = str(meta.get("flow_kind") or "").lower() or "core"
        risk_effect = str(meta.get("risk_effect") or "risk_increasing").lower()

        if mode == "SEVERE":
            if risk_effect == "risk_increasing":
                return False
            return True

        if mode == "CONSTRAINED":
            if flow_kind == "opportunistic":
                return False
            return True

        return True

    def _dispatch_bundle(self, bundle: dict) -> None:
        async def _runner():
            await self._submit_with_pairlocks(bundle)

        try:
            asyncio.get_running_loop().create_task(_runner())
        except RuntimeError:
            threading.Thread(
                target=lambda: asyncio.run(_runner()),
                name="rm-submit-offloop",
                daemon=True,
            ).start()

    def _schedule_delay(self, engine, bundle, defer_ms: float):
        async def _delay_then_submit():
            await asyncio.sleep(max(0.0, defer_ms / 1000.0))
            self._dispatch_bundle(bundle)

        try:
            asyncio.get_running_loop().create_task(_delay_then_submit())
        except RuntimeError:
            threading.Thread(
                target=lambda: asyncio.run(_delay_then_submit()),
                name="rm-delay-enqueue",
                daemon=True,
            ).start()
    def _obs_sc_counter(self, name: str, ex: str, alias: str, branch: str):
        c = getattr(self, "obs_counter", None)
        if callable(c):
            try:
                c(name, 1, exchange=ex, alias=alias, branch=branch)
            except Exception:
                pass


    def shadow_simulate(self, bundle: dict, l2_cache=None) -> None:
        """
        Lance la simulation 'shadow' non-bloquante sur le bundle fourni.
        - Récupère L2 (top-of-book) buy/sell depuis l2_cache ou fallback interne.
        - Passe un budget (capital_available_usdc) si disponible.
        - Appelle simulator.simulate_both_parallel(...) en tâche détachée.
        - Sampling contrôlé par self.sim_shadow_sample_pct (0..1, défaut 0.15).
        """
        # Sampling
        import random
        sample_pct = float(getattr(self, "sim_shadow_sample_pct", 0.15))
        if random.random() > max(0.0, min(1.0, sample_pct)):
            return  # ne simule pas cette fois

        simulator = getattr(self, "simulator", None)
        if not simulator or not hasattr(simulator, "simulate_both_parallel"):
            return

        # Utilitaires locaux pour extraire les L2
        def _best(levels):
            # levels: list[[price, qty], ...] — retourne (px, qty) ou (None, None)
            try:
                if levels and len(levels[0]) >= 2:
                    return float(levels[0][0]), float(levels[0][1])
            except Exception:
                pass
            return None, None

        def _get_l2(exchange: str, pair: str):
            bids = asks = None
            # 1) Via l2_cache fourni
            if l2_cache:
                # Essaye une API de type l2_cache.get_tob(exchange, pair) -> {"bids":[...], "asks":[...]}
                getter = getattr(l2_cache, "get_tob", None) or getattr(l2_cache, "get_l2", None)
                if callable(getter):
                    try:
                        book = getter(exchange, pair)  # dict attendu
                        if book:
                            bids = book.get("bids")
                            asks = book.get("asks")
                    except Exception:
                        pass
            # 2) Via un cache interne éventuel (self.last_l2)
            if (bids is None or asks is None) and hasattr(self, "last_l2"):
                try:
                    book = self.last_l2.get(exchange, {}).get(pair)
                    if book:
                        bids = bids or book.get("bids")
                        asks = asks or book.get("asks")
                except Exception:
                    pass
            # 3) As a service: None si introuvable (le simulateur sait dégrader)
            return bids, asks

        route = bundle.get("route", {})
        pair = route.get("pair")
        buy_ex = route.get("buy_ex")
        sell_ex = route.get("sell_ex")

        buy_bids, buy_asks = _get_l2(buy_ex, pair)
        sell_bids, sell_asks = _get_l2(sell_ex, pair)

        # Top of book (si disponible)
        buy_best_bid, buy_best_bid_qty = _best(buy_bids or [])
        buy_best_ask, buy_best_ask_qty = _best(buy_asks or [])
        sell_best_bid, sell_best_bid_qty = _best(sell_bids or [])
        sell_best_ask, sell_best_ask_qty = _best(sell_asks or [])

        # Budget capital si disponible
        capital_usdc = None
        try:
            # balance_fetcher ou un attribut local
            capital_usdc = float(getattr(self, "capital_available_usdc", None) or 0)
        except Exception:
            capital_usdc = None

        # Compose l'input shadow — le simulateur tolère les champs manquants
        shadow_payload = {
            "bundle": bundle,
            "pair": pair,
            "buy_ex": buy_ex,
            "sell_ex": sell_ex,
            "buy_tob": {
                "bid": {"px": buy_best_bid, "qty": buy_best_bid_qty},
                "ask": {"px": buy_best_ask, "qty": buy_best_ask_qty},
            },
            "sell_tob": {
                "bid": {"px": sell_best_bid, "qty": sell_best_bid_qty},
                "ask": {"px": sell_best_ask, "qty": sell_best_ask_qty},
            },
            "capital_available_usdc": capital_usdc,
            "shadow": True,
        }

        # Dispatch non-bloquant
        try:
            import asyncio
            loop = None
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop and loop.is_running():
                loop.create_task(simulator.simulate_both_parallel(shadow_payload))
            else:
                import threading
                def _runner():
                    try:
                        asyncio.run(simulator.simulate_both_parallel(shadow_payload))
                    except Exception as exc:
                        try:
                            logger.exception("[RiskManager] shadow_simulate failed", exc_info=exc)
                            inc_rm_reject(reason="SIM_SHADOW_EXCEPTION")
                        except Exception:
                            pass
                        rm_cfg = getattr(self, "_rm_cfg_obj", None) or getattr(self, "cfg", None)
                        if rm_cfg is not None and bool(getattr(rm_cfg, "strict_config", False)):
                            raise

                threading.Thread(target=_runner, daemon=True).start()
        except Exception as e:
            if getattr(self, "log", None): self.log.exception("shadow_simulate launch failed", exc_info=e)

    def _reserve_quote(self, ex: str, quote: str, amount: float) -> bool:
        """
        Réserve 'amount' dans self.virt_balances[ex][quote] si disponible.
        Met à jour la métrique INVENTORY_USD si dispo.
        """
        exu = str(ex).upper()
        q = str(quote).upper()
        cur = float(self.virt_balances.setdefault(exu, {}).get(q, 0.0))
        need = float(amount or 0.0)
        if cur < need:
            return False
        self.virt_balances[exu][q] = cur - need
        try:
            if INVENTORY_USD:
                INVENTORY_USD.labels(exchange=exu, quote=q).set(self.virt_balances[exu][q])
        except Exception:
            logging.exception("Unhandled exception")
        return True

    # --- risk_manager.py (dans class RiskManager) ---

    def bind_reconciler(self, reconciler) -> None:
        """
        Injection tardive d'un PrivateWSReconciler unique (géré par le Boot).

        Idempotent. N'échoue jamais.
        Met en place les hooks RM -> Reconciler (lookup/is_inflight) puis
        délègue à _wire_reconciler_engine_hooks() pour la partie Engine.
        """
        try:
            previous = getattr(self, "reconciler", None)
            self.reconciler = reconciler
            # Par défaut, wiring KO tant que _wire_reconciler_engine_hooks n'a pas validé
            if reconciler is not previous:
                self.reconciler_wiring_ok = False

            if self.reconciler is None:
                return

            if hasattr(self.reconciler, "set_event_sink"):
                self.reconciler.set_event_sink(self._submodule_event)

            # Lookup inflight côté RM (si disponible)
            if hasattr(self, "lookup_inflight"):
                self.reconciler._lookup = self.lookup_inflight
            if hasattr(self, "is_inflight"):
                self.reconciler._is_inflight = self.is_inflight

            # Partie Engine (resync_order/resync_alias)
            self._wire_reconciler_engine_hooks()
        except Exception as exc:
            logger.exception("[RiskManager] bind_reconciler failed")
            self._emit_private_plane_event("bind_reconciler_failed", error=str(exc))


    def bind_private_ws_hub(self, hub) -> None:
        """
        Injection tardive du Hub privé (registre les callbacks RM).

        Rôle:
          - enregistrer on_private_event comme callback "risk" sur le Hub,
          - récupérer le status du Hub (santé + wiring),
          - mettre à jour:
              * private_ws_healthy (via set_private_ws_health),
              * private_ws_wiring_ok (via status["wiring"]).
        """
        previous = getattr(self, "private_ws_hub", None)
        self.private_ws_hub = hub
        # Par défaut: wiring considéré comme KO tant qu'on n'a pas un status exploitable
        if hub is not previous:
            self.private_ws_wiring_ok = False
            self._pws_callback_registered = False

        if hub is None:
            self.private_ws_wiring_ok = False
            self.set_private_ws_health(None)
            return

        # 1) Enregistement du callback RM auprès du Hub
        if hasattr(hub, "register_callback") and hasattr(self, "on_private_event"):
            try:
                if not getattr(self, "_pws_callback_registered", False) or hub is not previous:
                    hub.register_callback(self.on_private_event, role="risk")
                    self._pws_callback_registered = True
            except Exception as exc:
                logger.exception("[RiskManager] unable to register RM callback on hub")
                self._emit_private_plane_event("pws_register_failed", error=str(exc))

        # 2) Récupérer le status complet du Hub
        status: Optional[Dict[str, Any]] = None
        if hasattr(hub, "get_status"):
            try:
                status = hub.get_status()
            except Exception as exc:
                logger.exception("[RiskManager] private WS status fetch failed")
                self._emit_private_plane_event("pws_status_failed", error=str(exc))

        # 3) Mettre à jour la santé (héritage)
        self.set_private_ws_health(status)

        # 4) Interpréter le wiring du Hub si exposé
        wiring_info: Dict[str, Any] = {}
        if isinstance(status, dict):
            raw_wiring = status.get("wiring")
            if isinstance(raw_wiring, dict):
                wiring_info = dict(raw_wiring)

        has_engine_cb = bool(wiring_info.get("has_engine_callback"))
        has_rm_cb = bool(wiring_info.get("has_rm_callback"))
        auto_rm = bool(wiring_info.get("auto_rm_from_engine"))

        # Wiring OK = les deux callbacks câblés, sans auto-wiring implicite RM<-engine
        self.private_ws_wiring_ok = bool(has_engine_cb and has_rm_cb and not auto_rm)

        # Evénements private-plane pour visibilité
        try:
            if auto_rm:
                self._emit_private_plane_event(
                    "pws_wiring_auto_rm_from_engine",
                    has_engine_callback=has_engine_cb,
                    has_rm_callback=has_rm_cb,
                    auto_rm_from_engine=True,
                )
            if not self.private_ws_wiring_ok:
                self._emit_private_plane_event(
                    "pws_wiring_incomplete",
                    has_engine_callback=has_engine_cb,
                    has_rm_callback=has_rm_cb,
                    auto_rm_from_engine=auto_rm,
                )
        except Exception:
            # Pas bloquant, on garde le flag private_ws_wiring_ok pour le Boot
            pass


    @staticmethod
    def _derive_private_ws_health(status: Optional[Dict[str, Any]]) -> bool:
        if not status:
            return False
        healthy = bool(status.get("healthy", False))
        subs = status.get("submodules")
        if subs and isinstance(subs, dict):
            any_client = False
            any_ok = False
            for per_exchange in subs.values():
                if not isinstance(per_exchange, dict):
                    continue
                for sub_status in per_exchange.values():
                    any_client = True
                    if bool((sub_status or {}).get("healthy", False)):
                        any_ok = True
            if any_client:
                return any_ok
        return healthy

    def _is_exchange_private_ws_healthy(self, exchange: str) -> bool:
        """
        Vérifie la santé du WS privé pour un exchange spécifique.
        """
        if not hasattr(self, "_private_ws_status") or not self._private_ws_status:
            return False
        
        subs = self._private_ws_status.get("submodules")
        if subs and isinstance(subs, dict):
            ex_status = subs.get(str(exchange).upper())
            if ex_status and isinstance(ex_status, dict):
                # Un exchange est sain si au moins un de ses clients (comptes) est sain
                for sub_status in ex_status.values():
                    if bool((sub_status or {}).get("healthy", False)):
                        return True
        return False

    def set_private_ws_health(self, status: Optional[Dict[str, Any]]) -> None:
        healthy = self._derive_private_ws_health(status)
        prev = getattr(self, "private_ws_healthy", None)
        self.private_ws_healthy = healthy
        self._private_ws_status = dict(status or {})
        self._update_pws_critical_drop_from_status(status)
        if prev is None or bool(prev) != bool(healthy):
            event = "private_ws_recovered" if healthy else "private_ws_degraded"
            self._emit_private_plane_event(event, healthy=bool(healthy))

    def _handle_pws_unsafe_dedup(self, payload: Optional[Dict[str, Any]] = None) -> None:
        if getattr(self, "_pws_unsafe_dedup_seen", False):
            return
        try:
            self._pws_unsafe_dedup_seen = True
        except Exception:
            pass
        reason = normalize_reason_code("PWS_UNSAFE_DEDUP") or "PWS_UNSAFE_DEDUP"
        try:
            inc_blocked("rm", reason, None)
        except Exception:
            pass
        try:
            mgr = getattr(self, "_lhm_manager", None)
            if mgr and hasattr(mgr, "activate_fail_closed_latch"):
                mgr.activate_fail_closed_latch(
                    op_id="PWS/PWS_UNSAFE_DEDUP",
                    reason=reason,
                    payload=payload if isinstance(payload, dict) else None,
                )
        except Exception:
            logger.exception("[RiskManager] activate_fail_closed_latch failed")
        self.set_logging_fail_closed(reason)
    def _set_trading_state(self, state: str, *, reason: Optional[str] = None) -> None:
        new_state = str(state or "READY").upper()
        prev_state = str(getattr(self, "trading_state", "READY") or "READY").upper()
        if new_state == prev_state and (not reason or reason == getattr(self, "trading_state_reason", None)):
            return
        self.trading_state = new_state
        self.trading_state_reason = reason
        try:
            if getattr(self, "trading_ready_event", None):
                if new_state == "READY":
                    self.trading_ready_event.set()
                else:
                    self.trading_ready_event.clear()
        except Exception:
            pass
        eng = getattr(self, "engine", None)
        if eng and hasattr(eng, "set_trading_state"):
            try:
                eng.set_trading_state(new_state, reason=reason)
            except Exception:
                pass

    def set_logging_fail_closed(self, reason: str) -> None:
        reason_code = normalize_reason_code(reason) or str(reason or "TRUTH_PERSISTENCE_FAILED")
        self._set_trading_state("BLOCKED", reason=reason_code)

    def get_trading_state(self) -> str:
        return str(getattr(self, "trading_state", "READY") or "READY").upper()

    def get_block_reason(self) -> Optional[str]:
        return getattr(self, "trading_state_reason", None)

    def _collect_logging_health(self) -> dict:
        manager = getattr(self, "_lhm_manager", None)
        if manager is None:
            return {}
        try:
            if hasattr(manager, "get_status"):
                status = manager.get_status() or {}
                if status.get("logging_broken"):
                    self.set_logging_fail_closed("LHM_LOGGING_BROKEN")
                
                lhm_health = status.get("lhm_health") or {}
                if isinstance(lhm_health, dict):
                    return lhm_health
        except Exception:
            pass
        try:
            if hasattr(manager, "get_pnl_pipeline_flags"):
                flags = manager.get_pnl_pipeline_flags() or {}
                return {
                    "critical_drop_seen": bool(flags.get("critical_drop_seen")),
                    "last_critical_drop_reason": None,
                    "logging_persistence_ok": not bool(flags.get("critical_drop_seen")),
                }
        except Exception:
            pass
        return {}

    def _update_pws_critical_drop_from_status(self, status: Optional[Dict[str, Any]]) -> None:
        cfg = getattr(self, "cfg", None)
        pws_cfg = getattr(cfg, "pws", None)
        enforce = bool(getattr(pws_cfg, "ff_pws_no_drop_critical_enforced", False))
        if not status:
            return
        pws_health = status.get("pws_health") if isinstance(status, dict) else None
        critical_drop_seen = None
        last_reason = None
        unsafe_dedup = False
        unsafe_payload = None
        if isinstance(pws_health, dict):
            critical_drop_seen = pws_health.get("critical_drop_seen")
            last_reason = pws_health.get("last_critical_drop_reason")
            unsafe_dedup = bool(pws_health.get("unsafe_dedup_seen"))
            unsafe_payload = pws_health.get("last_unsafe_dedup")
        if critical_drop_seen is None and isinstance(status, dict):
            critical_drop_seen = status.get("critical_drop_seen")
        
        if critical_drop_seen:
            reason = normalize_reason_code(last_reason or "PWS_CRITICAL_DROP") \
                     or "PWS_CRITICAL_DROP"
            self._pws_critical_drop_seen = True
            self._pws_critical_drop_reason = reason
            # P0: On force le blocage si DROP critique détecté (sécurité Private Plane)
            # On ne dépend plus du feature flag pour le blocage si c'est vraiment critique
            if not self._pws_blocked_emitted:
                try:
                    inc_blocked("rm", reason, None)
                except Exception:
                    pass
                self._pws_blocked_emitted = True
            self._set_trading_state("BLOCKED", reason=reason)

        if unsafe_dedup:
            try:
                self._handle_pws_unsafe_dedup(unsafe_payload)
            except Exception:
                logger.exception("[RiskManager] handle_pws_unsafe_dedup failed")
    def set_engine(self, engine) -> None:
        """
        Injection tardive de l'Engine (après création réelle dans le Boot).
        Repose les callbacks pour la resync (si reconciler présent).

        Idempotent. N'échoue jamais.
        """
        try:
            self.engine = engine
            # Si pas d'engine => wiring Reconciler forcément KO
            if self.engine is None:
                self.reconciler_wiring_ok = False
                return
            self._wire_reconciler_engine_hooks()
            self._maybe_update_trading_ready()
        except Exception as exc:
            logger.exception("[RiskManager] set_engine failed")
            self._emit_private_plane_event("set_engine_failed", error=str(exc))

    def set_exec_submitters(self, submitters: Dict[str, Any]) -> None:
        try:
            self.exec_submitters = {str(k).upper(): v for k, v in (submitters or {}).items()}
        except Exception:
            self.exec_submitters = {}

    def _wire_reconciler_engine_hooks(self) -> None:
        """
        Branche les hooks Engine <- Reconciler et met à jour reconciler_wiring_ok.

        Hooks attendus côté Reconciler:
          - _lookup (RM)
          - _is_inflight (RM)
          - _resync_order (Engine)
          - _resync_alias (Engine)
        """
        rec = getattr(self, "reconciler", None)
        eng = getattr(self, "engine", None)

        if not rec or not eng:
            # Sans Engine ou sans Reconciler, wiring forcément incomplet
            self.reconciler_wiring_ok = False
            return

        # Branche resync_* depuis l'Engine et resync_balances depuis le RM
        rec._resync_order = getattr(eng, "resync_order", None)
        rec._resync_alias = getattr(eng, "resync_alias", None)
        rec._resync_balances = self._schedule_balance_resync_for_alias

        missing: List[str] = []

        if not callable(getattr(rec, "_lookup", None)):
            missing.append("lookup")
        if not callable(getattr(rec, "_is_inflight", None)):
            missing.append("is_inflight")
        if not callable(getattr(rec, "_resync_order", None)):
            missing.append("resync_order")
        if not callable(getattr(rec, "_resync_alias", None)):
            missing.append("resync_alias")
        if not callable(getattr(rec, "_resync_balances", None)):
            missing.append("resync_balances")

        self.reconciler_wiring_ok = not missing

        if missing:
            # Evénement private-plane unique pour diagnostic
            self._emit_private_plane_event(
                "reconciler_missing_hooks",
                missing=missing,
            )


    def _credit_quote(self, ex: str, quote: str, amount: float) -> None:
        """
        Crédite 'amount' dans self.virt_balances[ex][quote].
        Met à jour la métrique INVENTORY_USD si dispo.
        """
        exu = str(ex).upper()
        q = str(quote).upper()
        add = float(amount or 0.0)
        cur = float(self.virt_balances.setdefault(exu, {}).get(q, 0.0))
        self.virt_balances[exu][q] = cur + add
        try:
            if INVENTORY_USD:
                INVENTORY_USD.labels(exchange=exu, quote=q).set(self.virt_balances[exu][q])
        except Exception:
            logging.exception("Unhandled exception")

    def _mm_global_budget_usdc(self, profile: str, ex: str, quote: str) -> float:
        """
        Budget MM global encore disponible côté wallet virtuel.

        P0 : le profil est accepté pour compat future mais n'est pas différencié ;
        on retourne simplement le solde courant virtuel.
        """
        exu = str(ex or "").upper()
        q = str(quote or "").upper()
        cur = float(self.virt_balances.get(exu, {}).get(q, 0.0))
        return max(0.0, cur)

    def _mm_pair_budget_remaining(self, profile: str, ex: str, pair: str, quote: str) -> float:
        """
        P0 : cap soft par paire, basé sur le budget disponible courant et le ratio
        par profil, sans tracking cumulatif par paire. Une version future ajoutera
        un suivi fin par paire.
        """
        prof = str(profile or "LARGE").upper()
        exu = str(ex or "").upper()
        pair_key = self._norm_pair(pair)

        global_budget = self._mm_global_budget_usdc(prof, exu, quote)
        ratio = self.mm_pair_cap_ratio_by_profile.get(
            prof, self.mm_pair_cap_ratio_by_profile.get("LARGE", 1.0)
        )
        pair_cap = global_budget * max(0.0, min(1.0, ratio))

        remaining = min(pair_cap, global_budget)
        return max(0.0, remaining)

    def _update_inventory_metrics(self) -> None:
        """
        Pousse INVENTORY_USD(exchange,quote) = somme des montants quote (USDC/EUR)
        agrégés sur tous les alias.

        NOTE: Les labels Prometheus doivent matcher exactement obs_metrics.INVENTORY_USD.
        """
        if INVENTORY_USD is None:
            return

        try:
            snap = self._balances_snapshot()
            by_ex_quote: dict[str, dict[str, float]] = {}  # ex -> quote -> total

            for ex, accounts in (snap or {}).items():
                exu = str(ex or "UNKNOWN").upper()
                for _alias, assets in (accounts or {}).items():
                    if not assets:
                        continue
                    for q in ("USDC", "EUR"):
                        if q in assets:
                            by_ex_quote.setdefault(exu, {}).setdefault(q, 0.0)
                            by_ex_quote[exu][q] += float(assets.get(q, 0.0))

            for exu, m in by_ex_quote.items():
                for q, v in (m or {}).items():
                    # ✅ labels EXACTS : exchange, quote
                    INVENTORY_USD.labels(exchange=exu, quote=q).set(float(v))

        except Exception:
            logging.exception("Unhandled exception in _update_inventory_metrics")

        # keep existing behavior
        self.check_capital_drift()

    # ------------------------------------------------------------------ #
    # Balances TTL (MBF → RM)                                            #
    # ------------------------------------------------------------------ #

    def _balance_unknown_policy(self) -> str:
        rm_cfg = getattr(getattr(self, "cfg", None), "rm", None)
        policy = str(getattr(rm_cfg, "balance_unknown_policy", "DEGRADED")).upper()
        if policy not in {"NEUTRAL", "DEGRADED", "BLOCKED"}:
            return "DEGRADED"
        return policy

    def _classify_balance_age(self, age_s: float) -> str:
        """
        Classe un âge de balance en statut métier.

        - OK        : age <= RM_BALANCE_TTL_S_NORMAL
        - DEGRADED  : RM_BALANCE_TTL_S_NORMAL < age <= RM_BALANCE_TTL_S_BLOCK
        - BLOCKED   : age > RM_BALANCE_TTL_S_BLOCK
        Policy UNKNOWN (age<=0) configurable via cfg.rm.balance_unknown_policy :
        - BLOCKED  : retour BLOCKED (fail-closed)
        - DEGRADED : retour DEGRADED (down-clamp)
        - NEUTRAL  : retour UNKNOWN (compat)

        NB: RM_BALANCE_TTL_S_DEGRADED est là pour affiner la zone "DEGRADED"
        si tu veux plus tard (par ex. modes intermédiaires).
        """
        if age_s <= 0:
            policy = self._balance_unknown_policy()
            if policy == "BLOCKED":
                return "BLOCKED"
            if policy == "DEGRADED":
                return "DEGRADED"
            return "UNKNOWN"

        if age_s <= self._balance_ttl_s_normal:
            return "OK"

        if age_s <= self._balance_ttl_s_block:
            return "DEGRADED"

        return "BLOCKED"

    def _get_alias_ttl_cap_factor(self, ttl_status: str, branch: str) -> float:
        """
               Calcule un facteur de cap par alias en fonction du statut TTL et de la branche.

               Règles métier Macro 4 (alias_state × branch) :
               - UNKNOWN / OK  -> 1.0 (aucun impact sur les caps, toutes branches)
               - BLOCKED       -> 0.0 théorique (les alias BLOCKED sont déjà hard-gatés par
                                 engine_enqueue_bundle, aucun bundle ne sort du RM)
               - DEGRADED      -> down-clamp piloté par branche via cfg.rm.ttl_factor_*_degraded :

                   | alias_state | TT            | TM            | REB           | MM              |
                   |-------------|---------------|---------------|---------------|-----------------|
                   | OK/UNKNOWN  | 1.0           | 1.0           | 1.0           | 1.0             |
                   | DEGRADED    | f_TT (≤1.0)   | f_TM (≤1.0)   | f_REB (≤1.0)  | f_MM (=0.0 par défaut + DROP RM) |
                   | BLOCKED     | 0.0 + DROP RM | 0.0 + DROP RM | 0.0 + DROP RM | 0.0 + DROP RM   |

               Ici, f_TT/f_TM/f_REB/f_MM sont lus dans cfg.rm.ttl_factor_*_degraded, avec des
               valeurs par défaut conservatrices. Le DROP MM sur alias DEGRADED/BLOCKED est
               appliqué dans engine_enqueue_bundle, cette fonction ne fait que calculer un
               facteur de down-clamp.

               Les facteurs sont toujours clampés dans [0.0, 1.0] pour garantir le "down-clamp only".
               """
        status = str(ttl_status or "UNKNOWN").upper()
        b = str(branch or "").upper()

        # Cas neutres
        if status in ("UNKNOWN", "OK"):
            policy = self._balance_unknown_policy()
            if status == "UNKNOWN":
                if policy == "BLOCKED":
                    return 0.0
                if policy == "DEGRADED":
                    status = "DEGRADED"
                else:
                    return 1.0
            else:
                return 1.0
        if status == "BLOCKED":
            return 0.0

        # DEGRADED : lookup config, sinon fallback conservateur.
        rm_cfg = getattr(getattr(self, "cfg", None), "rm", None)

        default_map = {
            "TT": 0.5,
            "TM": 0.4,
            "REB": 0.3,
            "MM": 0.0,
        }

        if b == "TT":
            val = getattr(rm_cfg, "ttl_factor_tt_degraded", default_map["TT"])
        elif b == "TM":
            val = getattr(rm_cfg, "ttl_factor_tm_degraded", default_map["TM"])
        elif b == "REB":
            val = getattr(rm_cfg, "ttl_factor_reb_degraded", default_map["REB"])
        elif b == "MM":
            val = getattr(rm_cfg, "ttl_factor_mm_degraded", default_map["MM"])
        else:
            val = getattr(rm_cfg, "ttl_factor_default_degraded", 1.0)

        try:
            f = float(val)
        except Exception:
            f = 1.0

        if f < 0.0:
            f = 0.0
        if f > 1.0:
            f = 1.0
        return f


    def _is_critical_alias(self, exchange: str, alias: str) -> bool:
        """
        Retourne True si (exchange, alias) doit contribuer aux escalades globales
        de mode (TTL / WS). Permet de ne pas faire basculer toute la plateforme
        sur un alias purement sandbox/test.

        Règles:
        - si cfg.RM_CRITICAL_ALIASES est défini: liste de patterns "EX.ALIAS"
          ou "*.ALIAS" (EX ou ALIAS peuvent être "*" ou vides pour matcher tout).
        - sinon: on considère comme NON critiques uniquement les alias dont le
          nom contient "SANDBOX" ou "TEST" (en insensible à la casse).
        """
        ex_u = str(exchange or "").upper()
        alias_u = str(alias or "").upper()

        patterns = getattr(self.cfg, "RM_CRITICAL_ALIASES", None)
        if isinstance(patterns, (list, tuple, set)):
            for pat in patterns:
                try:
                    ex_pat, alias_pat = str(pat).upper().split(".", 1)
                except ValueError:
                    # Pattern inattendu, on l'ignore.
                    continue

                if ex_pat not in ("", "*") and ex_pat != ex_u:
                    continue
                if alias_pat not in ("", "*") and alias_pat != alias_u:
                    continue
                return True

            # Une liste explicite existe mais aucun pattern ne matche :
            # on considère l'alias comme non critique.
            return False

        # Fallback heuristique : tout est critique sauf les alias explicitement
        # marqués sandbox/test.
        if "SANDBOX" in alias_u or "TEST" in alias_u:
            return False

        return True


    def _refresh_balances_ttl_cache(self) -> None:
        """
        Récupère le snapshot MBF (cached_only) et met à jour :
        - self._alias_balance_age_s[(EX, ALIAS)]
        - self._alias_balance_status[(EX, ALIAS)] = OK/DEGRADED/BLOCKED

        Aucune I/O réseau : on ne fait que lire le cache MBF.
        """
        if not getattr(self, "_mbf_glue", None):
            return

        try:
            raw = self._mbf_glue.snapshot(cached_only=True) or {}
        except Exception:
            logging.exception("RM: échec snapshot MBF pour TTL balances")
            return

        meta = (raw.get("meta") or {}) if isinstance(raw, dict) else {}
        age_map = meta.get("age_s") or {}
        balances_health = meta.get("balances_health") or {}

        if not isinstance(age_map, dict):
            return

        now = time.time()
        self._alias_balance_age_s.clear()
        self._alias_balance_status.clear()
        self._alias_private_health = {}

        for key, age in age_map.items():
            try:
                ex, alias = str(key).split(".", 1)
            except ValueError:
                # Clé inattendue, on ignore.
                continue

            ex_u = ex.upper().strip()
            alias_u = alias.upper().strip()
            try:
                age_s = float(age or 0.0)
            except (TypeError, ValueError):
                age_s = 0.0

            self._alias_balance_age_s[(ex_u, alias_u)] = age_s
            bh_raw = balances_health.get(f"{ex_u}.{alias_u}") if isinstance(balances_health, dict) else None
            bh_state = str((bh_raw or {}).get("state") or "").upper() if isinstance(bh_raw, dict) else ""
            if bh_state == "NORMAL":
                status = "OK"
            elif bh_state == "DEGRADED":
                status = "DEGRADED"
            elif bh_state == "BLOCK":
                status = "BLOCKED"
            else:
                status = self._classify_balance_age(age_s)
            self._alias_balance_status[(ex_u, alias_u)] = status

            # On loggue les cas non-OK pour dashboards / alertes.
            if status in ("DEGRADED", "BLOCKED"):
                self._obs_balance_ttl_stale(ex_u, alias_u, age_s, status)

        # Projection des statuts comptes WS (Hub + Reconciler) depuis MBF.meta["ws_accounts"].
        # On conserve un cache local par (exchange, alias) pour les décisions RM.
        try:

            ws_accounts = meta.get("ws_accounts") or {}
        except Exception:
            ws_accounts = {}

        self._alias_ws_accounts_status = {}

        for key_str, ws_meta in (ws_accounts or {}).items():
            if not isinstance(key_str, str):
                continue
            try:
                ex_part, alias_part = key_str.split(".", 1)
            except ValueError:
                continue

            ex_u = ex_part.upper().strip()
            alias_u = alias_part.upper().strip()
            if not ex_u or not alias_u:
                continue

            try:
                self._alias_ws_accounts_status[(ex_u, alias_u)] = dict(ws_meta or {})
            except Exception:
                # En cas de format inattendu, on n'expose pas ce statut mais on
                # laisse la TTL classique jouer son rôle.
                continue
                # Vue consolidée balances/WS pour RM et debug.
        keys = set(self._alias_balance_age_s.keys()) | set(self._alias_ws_accounts_status.keys())
        for ex_u, alias_u in keys:
            key_str = f"{ex_u}.{alias_u}"
            bh_raw = balances_health.get(key_str) if isinstance(balances_health, dict) else None
            ws_meta = self._alias_ws_accounts_status.get((ex_u, alias_u)) or {}
            status_rm = self._alias_balance_status.get((ex_u, alias_u), "UNKNOWN")
            try:
                age_s = float(self._alias_balance_age_s.get((ex_u, alias_u), 0.0) or 0.0)
            except Exception:
                age_s = 0.0

            self._alias_private_health[(ex_u, alias_u)] = {
                "balance_status": status_rm,
                "balance_age_s": age_s,
                "bf_balances_state": str((bh_raw or {}).get("state") or "").upper() if isinstance(bh_raw,
                                                                                                  dict) else None,
                "bf_ttl_normal_s": (bh_raw or {}).get("ttl_normal_s") if isinstance(bh_raw, dict) else None,
                "bf_ttl_degraded_s": (bh_raw or {}).get("ttl_degraded_s") if isinstance(bh_raw, dict) else None,
                "bf_ttl_block_s": (bh_raw or {}).get("ttl_block_s") if isinstance(bh_raw, dict) else None,
                "ws_accounts": dict(ws_meta) if isinstance(ws_meta, dict) else {},
                "last_update_ts": now,
            }

        # Après mise à jour des caches TTL + comptes WS, on propage
        # cette information vers la surcouche "capital en mouvement".
        self._update_capital_move_state_from_ttl()

    def _prune_transfer_tracking(self, transfer_id: Optional[str]) -> None:
        if not transfer_id:
            return
        tid = str(transfer_id)

        # Ticket P0-RM-CAPMOVE-01: Nettoyage immédiat de la réservation de balance associée
        if hasattr(self, "_reserved_balances"):
            if tid in self._reserved_balances:
                self._reserved_balances.pop(tid, None)
                self._sync_reserved_metrics()

        keys = self._transfer_alias_index.pop(tid, set()) if hasattr(self, "_transfer_alias_index") else set()
        for key in list(keys):
            state = self._alias_capital_move_state.get(key)
            if not isinstance(state, dict):
                continue
            transfers = set(state.get("transfer_ids") or [])
            transfers.discard(tid)
            payloads = state.get("transfer_payloads") or {}
            if isinstance(payloads, dict):
                payloads.pop(tid, None)
            if transfers:
                state["transfer_ids"] = list(transfers)
            else:
                state.pop("transfer_ids", None)
            if isinstance(payloads, dict) and payloads:
                state["transfer_payloads"] = payloads
            else:
                state.pop("transfer_payloads", None)
            if state:
                self._alias_capital_move_state[key] = state
            else:
                self._alias_capital_move_state.pop(key, None)
    def _update_capital_move_state_from_ttl(self) -> None:
        """Met à jour l'état "capital en mouvement" à partir du cache TTL MBF.

        - Si une balance (EX, ALIAS) est fraîche (age_s <= RM_BALANCE_TTL_S_NORMAL),
          on considère que le transfert correspondant est visible côté MBF/RM et
          on purge l'état local.
        - Si la fenêtre maximale de rafraîchissement est dépassée sans balance
          fraîche, on purge également l'état après avoir signalé un potentiel
          dépassement de SLO via les hooks d'observabilité.
        """
        if not getattr(self, "_alias_capital_move_state", None):
            return

        now = time.time()
        done: list[tuple[str, str]] = []

        for key, state in list(self._alias_capital_move_state.items()):
            ex_u, alias_u = key
            try:
                age_s = float(self._alias_balance_age_s.get(key, 0.0))
            except Exception:
                age_s = 0.0

            start_ts = float(state.get("start_ts") or 0.0)
            deadline_ts = float(state.get("deadline_ts") or 0.0)
            transfer_ids = state.get("transfer_ids") or []
            payloads = state.get("transfer_payloads") or {}

            # Condition 1: balance fraîche -> transfert visible.
            if age_s > 0.0 and age_s <= self._balance_ttl_s_normal:
                if transfer_ids and isinstance(payloads, dict):
                    pending_wallet = False
                    for transfer_id in transfer_ids:
                        payload = payloads.get(str(transfer_id)) if isinstance(payloads, dict) else None
                        if str((payload or {}).get("type") or "").lower() in (
                                "internal_wallet_transfer",
                                "wallet_transfer",
                        ):
                            pending_wallet = True
                            break
                    if pending_wallet:
                        continue
                # On mesure la latence observée entre l'event de transfert et la
                # première balance fraîche.
                latency_s = max(0.0, now - start_ts) if start_ts > 0.0 else 0.0
                self._obs_capital_move_visibility(ex_u, alias_u, latency_s, status="OK")
                for transfer_id in transfer_ids:
                    try:
                        payload = payloads.get(str(transfer_id)) if isinstance(payloads, dict) else None
                        self._transfer_controller.mark_settled(transfer_id, payload=payload)
                        if self.rebalancing and hasattr(self.rebalancing, "mark_transfer_status"):
                            self.rebalancing.mark_transfer_status(transfer_id, "SETTLED")
                    except Exception:
                        logger.exception("[RiskManager] failed to mark transfer settled for %s", transfer_id)
                    self._prune_transfer_tracking(transfer_id)
                done.append(key)
                continue

            # Condition 2: fenêtre maximale dépassée sans balance fraîche.
            if deadline_ts > 0.0 and now >= deadline_ts and (age_s <= 0.0 or age_s > self._balance_ttl_s_normal):
                latency_s = max(0.0, now - start_ts) if start_ts > 0.0 else 0.0
                self._obs_capital_move_visibility(
                    ex_u,
                    alias_u,
                    latency_s,
                    status="SLO_BREACH",
                )
                done.append(key)

        for key in done:
            state = self._alias_capital_move_state.pop(key, None)
            transfer_ids = set((state or {}).get("transfer_ids") or [])
            for transfer_id in transfer_ids:
                aliases = self._transfer_alias_index.get(str(transfer_id))
                if not aliases:
                    continue
                aliases.discard(key)
                if not aliases:
                    self._transfer_alias_index.pop(str(transfer_id), None)

    def _get_private_path_health(self, exchange: str, alias: str) -> Dict[str, Any]:
        ex_u = str(exchange or "").upper()
        alias_u = str(alias or "").upper()
        return self._alias_private_health.get((ex_u, alias_u), {})

    def _classify_private_path_severity(self, priv: Dict[str, Any]) -> Tuple[int, Set[str]]:
        """Classe la sévérité privée (balances + WS) pour un chemin (ex, alias).

        Retourne (severity_level, reason_flags) où:
        - severity_level: 0 = OK, 1 = SOFT_CLAMP, 2 = HARD_BLOCK
        - reason_flags: ensemble de codes (BAL_DEGRADED, BAL_BLOCKED, PWS_WARN, ...)
        """

        severity = 0
        flags: Set[str] = set()

        status = str(priv.get("balance_status") or "").upper()
        if status == "BLOCKED":
            severity = 2
            flags.add("BAL_BLOCKED")
        elif status == "DEGRADED":
            severity = 1
            flags.add("BAL_DEGRADED")

        ws_meta = priv.get("ws_accounts") or {}
        if isinstance(ws_meta, dict):
            hub_status = str(ws_meta.get("hub_status") or "").upper()
            reco_status = str(ws_meta.get("reco_status") or "").upper()
            capital_at_risk = bool(ws_meta.get("capital_at_risk"))

            if capital_at_risk:
                severity = 2 if severity == 1 else max(severity, 1)
                flags.add("CAPITAL_AT_RISK")

            if "CRITICAL" in {hub_status, reco_status}:
                severity = max(severity, 2)
                flags.add("PWS_CRITICAL")
            elif "WARN" in {hub_status, reco_status}:
                severity = max(severity, 1)
                flags.add("PWS_WARN")

        return severity, flags

    def get_private_alias_status_snapshot(self) -> Dict[str, Dict[str, Any]]:
        """Retourne un snapshot alias-centric du marché privé pour obs/dashboard.

        Structure de retour (exemple) ::

            {
                "BINANCE": {
                    "TT": {
                        "age_s": 12.3,
                        "ttl_status": "OK",
                        "effective_status": "OK",  # TTL + WS + capital_move
                        "capital_move": {
                            "active": false,
                            "start_ts": 0.0,
                            "deadline_ts": 0.0,
                            "last_notional_usdc": 0.0,
                            "source": "",
                            "subtype": "",
                        },
                        "ws_accounts": {...},      # hub_status / reco_status / last_resync_ts / capital_at_risk
                        "capital_at_risk": false,
                    },
                    ...
                },
                ...
                "_meta": {
                    "rm_mode": "NORMAL",
                    "trade_mode": "NORMAL",
                },
            }

        Cette vue est strictement read-only et destinée à la couche
        observabilité / dashboard (Ticket 13).
        """
        snapshot: Dict[str, Dict[str, Any]] = {}

        # Caches internes mis à jour par _refresh_balances_ttl_cache().
        try:
            alias_age = dict(getattr(self, "_alias_balance_age_s", {}) or {})
        except Exception:
            alias_age = {}
        try:
            alias_status = dict(getattr(self, "_alias_balance_status", {}) or {})
        except Exception:
            alias_status = {}
        try:
            ws_cache = dict(getattr(self, "_alias_ws_accounts_status", {}) or {})
        except Exception:
            ws_cache = {}
        try:
            move_cache = dict(getattr(self, "_alias_capital_move_state", {}) or {})
        except Exception:
            move_cache = {}

        try:
            private_cache = dict(getattr(self, "_alias_private_health", {}) or {})
        except Exception:
            private_cache = {}

        if not alias_age and not ws_cache and not move_cache:
            # Rien à exposer pour l'instant : on renvoie uniquement le contexte global.
            return {
                "_meta": {
                    "rm_mode": str(getattr(self, "rm_mode", "NORMAL")).upper(),
                    "trade_mode": str(getattr(self, "trade_mode", "NORMAL")).upper(),
                }
            }

        # Union de toutes les clés vues côté balances/WS/capital_move.
        all_keys: Set[Tuple[str, str]] = set()
        for d in (alias_age, alias_status, ws_cache, move_cache, private_cache):
            try:
                all_keys.update(d.keys())
            except Exception:
                continue

        now = time.time()
        private_health_snapshot: Dict[str, Any] = {}

        for raw_key in all_keys:
            try:
                ex_u, alias_u = raw_key
            except Exception:
                # Clé inattendue (pas un tuple (ex, alias)), on ignore.
                continue

            ex = str(ex_u).upper()
            alias = str(alias_u).upper()
            key = (ex_u, alias_u)

            # 1) TTL balances (âge + statut brut)
            try:
                age_raw = alias_age.get(key, None)
                age_s = float(age_raw) if age_raw is not None else None
            except Exception:
                age_s = None

            ttl_status = str(alias_status.get(key, "UNKNOWN")).upper()

            # 2) Surcouche WS + capital_at_risk
            ws_meta_raw = ws_cache.get(key) or {}
            ws_meta = dict(ws_meta_raw) if isinstance(ws_meta_raw, dict) else {}
            capital_at_risk = bool(ws_meta.get("capital_at_risk"))

            # 3) Surcouche "capital en mouvement"
            move_state_raw = move_cache.get(key) or {}
            move_state = dict(move_state_raw) if isinstance(move_state_raw, dict) else {}
            deadline_ts = float(move_state.get("deadline_ts") or 0.0)
            start_ts = float(move_state.get("start_ts") or 0.0)
            last_notional = float(move_state.get("last_notional_usdc") or 0.0)
            capital_move_active = bool(deadline_ts > 0.0 and now < deadline_ts)

            # 4) Statut effectif aligné sur _check_balance_ttl_for_bundle():
            effective_status = ttl_status
            if capital_move_active and effective_status in ("UNKNOWN", "OK"):
                effective_status = "DEGRADED"

            if capital_at_risk:
                if effective_status in ("UNKNOWN", "OK"):
                    effective_status = "DEGRADED"
                elif effective_status == "DEGRADED":
                    effective_status = "BLOCKED"

            # 5) SLO balances côté MBF (si dispo)
            private_meta = private_cache.get((ex_u, alias_u)) or {}

            # 6) Construction de la vue par (exchange, alias)
            per_ex = snapshot.setdefault(ex, {})
            per_ex[alias] = {
                "age_s": age_s,
                "ttl_status": ttl_status,
                "effective_status": effective_status,
                "capital_move": {
                    "active": capital_move_active,
                    "start_ts": start_ts,
                    "deadline_ts": deadline_ts,
                    "last_notional_usdc": last_notional,
                    "source": str(move_state.get("source") or ""),
                    "subtype": str(move_state.get("subtype") or ""),
                } if move_state else {
                    "active": False,
                },
                "ws_accounts": ws_meta,
                "capital_at_risk": capital_at_risk,
            }

        private_health_snapshot[f"{ex}.{alias}"] = {
            "balance_status": private_meta.get("balance_status", ttl_status),
            "balance_age_s": private_meta.get("balance_age_s", age_s),
            "bf_balances_state": private_meta.get("bf_balances_state"),
            "bf_ttl_normal_s": private_meta.get("bf_ttl_normal_s"),
            "bf_ttl_degraded_s": private_meta.get("bf_ttl_degraded_s"),
            "bf_ttl_block_s": private_meta.get("bf_ttl_block_s"),
            "ws_accounts": private_meta.get("ws_accounts") or ws_meta,
        }

        if private_health_snapshot:
            snapshot["private_health_snapshot"] = private_health_snapshot

        # 7) Ajout du contexte global RM/Trade mode.
        snapshot["_meta"] = {
            "rm_mode": str(getattr(self, "rm_mode", "NORMAL")).upper(),
            "trade_mode": str(getattr(self, "trade_mode", "NORMAL")).upper(),
        }
        return snapshot


    def _iter_bundle_aliases(self, bundle: Dict[str, Any]) -> List[Tuple[str, str]]:
        """
        Extrait la liste (exchange, alias) impliqués par un bundle.

        - D’abord via bundle['meta'] (cas REB / TM global)
        - Puis via bundle['legs'] si dispo (cas TT/TM multi-legs)

        On normalise toujours en UPPER.
        """
        aliases: set[Tuple[str, str]] = set()

        meta = bundle.get("meta") or {}
        ex_meta = str(meta.get("exchange") or meta.get("venue") or "").upper()
        alias_meta = str(
            meta.get("account_alias") or meta.get("alias") or ""
        ).upper()
        if ex_meta and alias_meta:
            aliases.add((ex_meta, alias_meta))

        for leg in bundle.get("legs") or []:
            if not isinstance(leg, dict):
                continue
            ex_l = str(leg.get("exchange") or leg.get("venue") or "").upper()
            alias_l = str(
                leg.get("account_alias")
                or leg.get("alias")
                or alias_meta  # fallback sur meta si besoin
            ).upper()
            if ex_l and alias_l:
                aliases.add((ex_l, alias_l))

        return list(aliases)

    def _check_balance_ttl_for_bundle(self, bundle: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Calcule le "pire" statut TTL pour les alias impliqués dans le bundle,
        en combinant:
        - la fraîcheur MBF (TTL balances),
        - le statut comptes WS (Hub + Reconciler) projeté par MBF.meta["ws_accounts"].

        Retourne:
            None si aucune info TTL exploitable OU tout est OK/UNKNOWN,
            sinon un dict:
                {
                    "status": "DEGRADED" | "BLOCKED",
                    "severity_level": 0 | 1 | 2,
                    "reason_flags": List[str],
                    "exchange": "BINANCE",
                    "alias": "TT",
                    "age_s": 123.4,
                    "ws_accounts": {
                        "capital_at_risk": bool,
                        "hub_status": str | None,
                        "reco_status": str | None,
                        "last_resync_ts": float | None,
                    },
                }
        """
        if not getattr(self, "_mbf_glue", None):
            return None

        meta = bundle.get("meta") or {}
        branch = self._branch_of(meta, bundle)


        # Rafraîchit le cache TTL + statuts WS depuis MBF.as_rm_snapshot()
        self._refresh_balances_ttl_cache()

        aliases = self._iter_bundle_aliases(bundle)
        if not aliases:
            return None

        worst: Optional[Dict[str, Any]] = None
        worst_rank: int = -1

        now = time.time()

        ws_cache = getattr(self, "_alias_ws_accounts_status", {}) or {}

        for ex, alias in aliases:
            key = (ex.upper(), alias.upper())
            priv = self._get_private_path_health(ex, alias)
            ws_meta = ws_cache.get(key) or {}

            if priv:
                age_s = priv.get("balance_age_s")
                status = str(priv.get("balance_status") or "UNKNOWN").upper()
                ws_meta = priv.get("ws_accounts") or ws_meta
            else:
                age_s = self._alias_balance_age_s.get(key)
                status = str(self._alias_balance_status.get(key, "UNKNOWN")).upper()
                priv = {
                    "balance_status": status,
                    "balance_age_s": age_s,
                    "ws_accounts": ws_meta,
                }
            if age_s is None:
                continue

            severity_level, reason_flags = self._classify_private_path_severity(priv)


            capital_move = False
            move_state = getattr(self, "_alias_capital_move_state", {}).get(key, None)
            if move_state:
                deadline_ts = float(move_state.get("deadline_ts") or 0.0)
                if deadline_ts > 0.0 and now < deadline_ts:
                    capital_move = True

            if capital_move:
                severity_level = max(severity_level, 1)
                reason_flags.add("CAPITAL_MOVE")

            if severity_level <= 0:
                continue
            derived_status = status
            if severity_level == 2:
                derived_status = "BLOCKED"
            elif severity_level == 1:
                derived_status = "DEGRADED"

            ws_info = {
                "capital_at_risk": bool((ws_meta or {}).get("capital_at_risk")),
                "hub_status": (ws_meta or {}).get("hub_status"),
                "reco_status": (ws_meta or {}).get("reco_status"),
                "last_resync_ts": (ws_meta or {}).get("last_resync_ts"),
            }

            if severity_level > worst_rank:
                worst_rank = severity_level
                alias_cap_factor = self._get_alias_ttl_cap_factor(derived_status, branch)
                try:
                    alias_cap_factor = float(alias_cap_factor)
                except Exception:
                    alias_cap_factor = 1.0
                if alias_cap_factor < 0.0:
                    alias_cap_factor = 0.0
                if alias_cap_factor > 1.0:
                    alias_cap_factor = 1.0

                worst = {
                    "status": derived_status,
                    "severity_level": severity_level,
                    "reason_flags": sorted(reason_flags),
                    "exchange": ex.upper(),
                    "alias": alias.upper(),
                    "age_s": float(age_s),
                    "ws_accounts": ws_info,
                    "capital_at_risk": bool(ws_info.get("capital_at_risk")),
                    "alias_cap_factor": alias_cap_factor,
                }


        if not worst:
            return None

        if worst["status"] in ("DEGRADED", "BLOCKED"):
            return worst

        return None


    def _schedule_balance_resync_for_alias(self, exchange: str, alias: str) -> None:
        """
        Demande asynchrone (throttlée) de resync balances ciblé pour (exchange, alias).

        S'appuie sur MultiBalanceFetcher.resync_balances_for_alias quand disponible.
        Ne fait rien si:
        - MBF absent,
        - API resync_balances_for_alias manquante,
        - pas de boucle asyncio active,
        - appel trop fréquent pour le même alias (throttle par RM_WS_BALANCE_RESYNC_MIN_INTERVAL_S).
        """
        mbf = getattr(self, "mbf", None) or getattr(self, "balance_fetcher", None)
        if mbf is None or not hasattr(mbf, "resync_balances_for_alias"):
            return

        ex_u = str(exchange or "").upper()
        alias_u = str(alias or "").upper()
        if not ex_u or not alias_u:
            return

        key = (ex_u, alias_u)
        now = time.time()

        # Ticket P0-RM-RESYNC-01: Throttle adaptatif (2 vitesses)
        move_state = getattr(self, "_alias_capital_move_state", {}).get(key)
        is_moving = move_state and now < float(move_state.get("deadline_ts", 0.0))

        if is_moving:
            # Mode rapide pendant un capital move (Ticket P0-RM-RESYNC-01)
            min_interval = float(getattr(self.cfg, "RM_CAPITAL_MOVE_RESYNC_INTERVAL_S", 3.0))
        else:
            # Mode normal
            min_interval = float(getattr(self.cfg, "RM_WS_BALANCE_RESYNC_MIN_INTERVAL_S", 30.0))

        last_ts = self._alias_last_resync_request_ts.get(key, 0.0)
        if now - last_ts < max(1.0, min_interval):
            return

        # Plafond global (Ticket P0-RM-RESYNC-01) - max N resyncs par minute par exchange pour éviter 429
        hist = self._resync_history.setdefault(ex_u, deque(maxlen=100))
        while hist and now - hist[0] > 60.0:
            hist.popleft()

        max_per_min = int(getattr(self.cfg, "RM_MAX_RESYNC_PER_MIN", 15))
        if len(hist) >= max_per_min:
            return

        hist.append(now)
        self._alias_last_resync_request_ts[key] = now

        # Ticket P0-RM-RESYNC-01: Coalescence (une seule task en vol)
        if key in self._alias_resync_inflight:
            return

        async def _do_resync() -> None:
            self._alias_resync_inflight.add(key)
            try:
                await mbf.resync_balances_for_alias(ex_u, alias_u)
            except Exception:
                logger.warning(
                    "RM: échec resync balances ciblé pour %s.%s",
                    ex_u,
                    alias_u,
                    exc_info=True,
                )
            finally:
                self._alias_resync_inflight.discard(key)

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # Pas de boucle asyncio active: on ne tente pas de resync.
            return

        try:
            asyncio.create_task(
                _do_resync(),
                name=f"rm-resync-balances-{ex_u}-{alias_u}",
            )
        except Exception:
            # Pas bloquant pour la prise de décision RM.
            return

    # ------------------------------------------------------------------ #
    # Observabilité TTL balances (hooks, no-ops si obs_metrics absent)   #
    # ------------------------------------------------------------------ #

    def _obs_balance_ttl_breach(self, exchange: str, alias: str, status: str) -> None:
        """
        Hook pour compter les rejets RM liés au TTL balances.

        Câblage attendu côté obs_metrics:
            RM_BALANCES_TTL_BREACH.labels(exchange=..., alias=..., status=...).inc()
        """
        try:
            from . import obs_metrics  # type: ignore
        except Exception:  # pragma: no cover
            return

        metric = getattr(obs_metrics, "RM_BALANCES_TTL_BREACH", None)
        if metric is None:
            return

        ex_u = str(exchange).upper()
        alias_u = str(alias).upper()
        try:
            metric.labels(exchange=ex_u, alias=alias_u, status=str(status).upper()).inc()
        except Exception:
            # On ne casse jamais la décision RM pour un problème de métriques.
            logging.exception("RM: erreur métrique RM_BALANCES_TTL_BREACH")

    def _obs_balance_ttl_stale(
        self, exchange: str, alias: str, age_s: float, status: str
    ) -> None:
        """
        Hook pour traquer les alias dont la balance est vieillissante/stale.

        Câblage attendu côté obs_metrics:
            RM_BALANCES_STALE_TOTAL.labels(exchange=..., alias=..., status=...).inc()
        """
        try:
            from . import obs_metrics  # type: ignore
        except Exception:  # pragma: no cover
            return

        metric = getattr(obs_metrics, "RM_BALANCES_STALE_TOTAL", None)
        if metric is None:
            return

        ex_u = str(exchange).upper()
        alias_u = str(alias).upper()

        try:
            metric.labels(exchange=ex_u, alias=alias_u, status=str(status).upper()).inc()
        except Exception:
            logging.exception("RM: erreur métrique RM_BALANCES_STALE_TOTAL")

    def _obs_combo_cap_reject(
        self,
        *,
        combo_key: str,
        branch: str,
        profile: str,
        ttl_status: str,
    ) -> None:
        """
        Observabilité: compteur de rejets liés au cap global par combo.
        """
        try:
            from . import obs_metrics  # type: ignore
        except Exception:  # pragma: no cover
            return

        metric = getattr(obs_metrics, "RM_COMBO_CAP_REJECT_TOTAL", None)
        if metric is None:
            return

        try:
            metric.labels(
                combo=str(combo_key),
                branch=str(branch).upper(),
                profile=str(profile).upper(),
                ttl_status=str(ttl_status).upper() or "NA",
            ).inc()
        except Exception:
            logging.exception("RM: erreur métrique RM_COMBO_CAP_REJECT_TOTAL")


    def _obs_capital_move_visibility(
        self,
        exchange: str,
        alias: str,
        latency_s: float,
        status: str,
    ) -> None:
        """Hook d'observabilité pour la latence de visibilité des transferts internes.

        Deux usages principaux:
          - status="OK"        -> latence observée jusqu'à première balance fraîche.
          - status="SLO_BREACH" -> fenêtre max dépassée sans balance fraîche.
        """
        try:
            from . import obs_metrics  # type: ignore
        except Exception:  # pragma: no cover
            return

        # Histogramme facultatif de latence (secondes).
        hist = getattr(obs_metrics, "RM_CAPITAL_MOVE_VISIBILITY_LATENCY_S", None)
        if hist is not None:
            try:
                hist.labels(
                    exchange=str(exchange).upper(),
                    alias=str(alias).upper(),
                    status=str(status).upper(),
                ).observe(float(max(0.0, latency_s)))
            except Exception:
                logging.exception("RM: erreur métrique RM_CAPITAL_MOVE_VISIBILITY_LATENCY_S")

        # Compteur d'évènements, notamment pour les SLO breaches.
        counter = getattr(obs_metrics, "RM_CAPITAL_MOVE_VISIBILITY_TOTAL", None)
        if counter is not None:
            try:
                counter.labels(
                    exchange=str(exchange).upper(),
                    alias=str(alias).upper(),
                    status=str(status).upper(),
                ).inc()
            except Exception:
                logging.exception("RM: erreur métrique RM_CAPITAL_MOVE_VISIBILITY_TOTAL")

    def _obs_capital_move_event(
        self,
        exchange: str,
        aliases: list[str],
        notional_usdc: float,
        subtype: str,
        source: str,
        status: str = "EMITTED",
    ) -> None:
        """Hook pour tracer les évènements de transferts internes > seuil.

        Câblage attendu côté obs_metrics (facultatif):
          - RM_CAPITAL_MOVE_TOTAL(exchange, subtype, source, status).inc()
          - RM_CAPITAL_MOVE_NOTIONAL_USD.observe(notional_usdc)
        """
        try:
            from . import obs_metrics  # type: ignore
        except Exception:  # pragma: no cover
            return

        counter = getattr(obs_metrics, "RM_CAPITAL_MOVE_TOTAL", None)
        if counter is not None:
            try:
                counter.labels(
                    exchange=str(exchange).upper(),
                    subtype=subtype,
                    source=source,
                    status=str(status).upper(),
                ).inc()
            except Exception:
                logging.exception("RM: erreur métrique RM_CAPITAL_MOVE_TOTAL")

        hist = getattr(obs_metrics, "RM_CAPITAL_MOVE_NOTIONAL_USD", None)
        if hist is not None:
            try:
                hist.labels(
                    exchange=str(exchange).upper(),
                    subtype=subtype,
                    source=source,
                ).observe(float(max(0.0, notional_usdc)))
            except Exception:
                logging.exception("RM: erreur métrique RM_CAPITAL_MOVE_NOTIONAL_USD")

    def _obs_set_mode_gauges(self, rm_mode: str, trade_mode: str) -> None:
        """
        Bridge RM → Prometheus pour les modes.

        5-OBS-1 :
        - expose rm_mode et trade_mode via RM_MODE_CURRENT / RM_TRADE_MODE_CURRENT
        - un seul mode actif à 1 par métrique (one-hot sur label "mode").
        """
        try:
            from . import obs_metrics  # type: ignore
        except Exception:  # pragma: no cover
            return

        # Préférence : déléguer à obs_metrics si le helper existe
        helper = getattr(obs_metrics, "rm_update_mode_gauges", None)
        if callable(helper):
            try:
                helper(rm_mode, trade_mode)
                return
            except Exception:
                # On retombe sur le fallback local pour ne pas casser le RM
                pass

        rm_gauge = getattr(obs_metrics, "RM_MODE_CURRENT", None)
        trade_gauge = getattr(obs_metrics, "RM_TRADE_MODE_CURRENT", None)

        rm_mode_u = str(rm_mode or "").upper()
        trade_mode_u = str(trade_mode or "").upper()
        rm_value = {"NORMAL": 0, "OPP_VOLUME": 1, "OPP_VOL": 2, "SEVERE": 3}.get(rm_mode_u, -1)
        trade_value = {"NORMAL": 0, "CONSTRAINED": 1, "SEVERE": 2, "OPPORTUNISTE": 3}.get(trade_mode_u, -1)

        # Fallback "one-hot" local si rm_update_mode_gauges n'est pas disponible
        if rm_gauge is not None:
            try:
                for m in ("NORMAL", "OPP_VOLUME", "OPP_VOL", "SEVERE"):
                    rm_gauge.labels(mode=m).set(1.0 if m == rm_mode_u else 0.0)
            except Exception:
                try:
                    rm_gauge.set(float(rm_value))
                except Exception:
                    pass

        if trade_gauge is not None:
            try:
                for m in ("NORMAL", "CONSTRAINED", "SEVERE", "OPPORTUNISTE"):
                    trade_gauge.labels(mode=m).set(1.0 if m == trade_mode_u else 0.0)
            except Exception:
                try:
                    trade_gauge.set(float(trade_value))
                except Exception:
                    pass


    def set_orderbooks_source(self, fn):
        """Définit la source d’orderbooks pour les accès ponctuels et la boucle interne."""
        self.get_orderbooks_callback = fn
        self._get_orderbooks = fn

    # -- dans RiskManager (méthodes utilitaires OB) --
    def _ob_snapshot(self) -> dict:
        try:
            if callable(self.get_orderbooks_callback):
                return self.get_orderbooks_callback() or {}
        except Exception:
            logging.exception("Unhandled exception")
        return {}

    def _best_bid_ask(self, exchange: str, pair_key: str) -> tuple[float, float, int]:
        """
        Retourne (bid, ask, ts_ms) depuis le snapshot Router si disponible.
        """
        ob = self._ob_snapshot()
        ex = str(exchange).upper()
        pk = str(pair_key).replace("-", "").upper()
        d = ((ob.get(ex) or {}).get(pk) or {})
        try:
            bid = float(d.get("best_bid") or d.get("bid") or 0.0)
            ask = float(d.get("best_ask") or d.get("ask") or 0.0)
            ts = int(d.get("exchange_ts_ms") or d.get("recv_ts_ms") or d.get("ts_ms") or 0)
            return bid, ask, ts
        except Exception:
            return 0.0, 0.0, 0

    def _ensure_ready(self) -> None:
        if not self.ready_event.is_set():
            logger.warning("[RM] Appel avant readiness — action refusée")
            raise NotReadyError("RiskManager not ready")
        # Vérifie l’Engine si présent
        eng = getattr(self, "engine", None)
        if eng is None:
            # Autorisé : certains usages hors Engine (dry-route) — mais on loggue
            logger.warning("[RM] Engine absent — certaines actions seront dry-routées")
            return
        if not getattr(eng, "ready_event", None) or not eng.ready_event.is_set():
            logger.warning("[RM] Engine pas prêt — action refusée")
            raise NotReadyError("Engine not ready (from RM)")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    # --- à mettre dans RiskManager ------------------------------------------------
    import asyncio, logging
    logger = logging.getLogger("rm")

    async def start(self) -> None:
        """
        Démarrage idempotent :
        - crée les boucles internes en tâches
        - branche le consumer du Scanner si fourni
        - marque ready, puis démarre la glue RM<->MBF (si présente)
        """
        if getattr(self, "_running", False):
            return
        self._running = True

        logger.info(
            "[RiskManager] ✅ Orchestrateur démarré (TT=%s, TM=%s, dry_run=%s)",
            getattr(self, "enable_tt", True),
            getattr(self, "enable_tm", True),
            getattr(getattr(self, "cfg", None), "dry_run", False),
        )
        try:
            rm_cfg = getattr(getattr(self, "cfg", None), "rm", None)
            mode = str(
                getattr(self, "split_mode", None) or getattr(getattr(self, "cfg", None), "DEPLOYMENT_MODE", "")).upper()
            base_delta_ms, skew_ms, stale_ms = self._split_latency_defaults(mode)
            thresholds = {
                "base_ms": float(getattr(rm_cfg, "split_breach_thr_base_ms", 180.0)),
                "skew_ms": float(getattr(rm_cfg, "split_breach_thr_skew_ms", 40.0)),
                "stale_ms": float(getattr(rm_cfg, "split_breach_thr_stale_ms", 1300.0)),
            }
            cap_bps = float(getattr(rm_cfg, "split_penalty_bps_max", 6.0))
            logger.info(
                "[RiskManager] region_mode=%s latency=%s thresholds=%s penalty_cap_bps=%s",
                mode,
                {"base_delta_ms": base_delta_ms, "skew_ms": skew_ms, "stale_ms": stale_ms},
                thresholds,
                cap_bps,
            )
        except Exception:
            logger.exception("[RiskManager] region snapshot log failed", exc_info=False)

        self._tasks = [
            asyncio.create_task(self._loop_orderbooks(), name="rm-orderbooks"),
            asyncio.create_task(self._loop_balances(), name="rm-balances"),
            asyncio.create_task(self._loop_rebalancing(), name="rm-rebalancing"),
            asyncio.create_task(self._loop_volatility(), name="rm-volatility"),
            asyncio.create_task(self._loop_mode(), name="rm-mode"),
            asyncio.create_task(self._loop_fee_sync(), name="rm-fee-sync"),

        ]

        # Hook scanner -> RM
        if callable(getattr(self, "_scanner_consumer", None)):
            try:
                self._scanner_consumer(self.on_scanner_opportunity)
            except Exception:
                logger.exception("[RiskManager] scanner_consumer hookup failed")

        # Le RM est prêt dès que ses boucles sont lancées
        if hasattr(self, "ready_event"):
            try:
                self.ready_event.set()
            except Exception:
                pass

        self._maybe_update_trading_ready()

        # Démarre la colle RM<->MBF a posteriori (pour ne pas bloquer start())
        if hasattr(self, "_mbf_glue"):
            try:
                await self._mbf_glue.start()
            except Exception:
                logger.exception("[RiskManager] mbf_glue.start() failed")

            # Audit de config (best-effort, option strict)
            rm_cfg = getattr(getattr(self, "cfg", None), "rm", getattr(self, "cfg", None))
            if bool(getattr(rm_cfg, "audit_config_on_start", True)):
                try:
                    payload = self._audit_effective_config(strict=bool(getattr(rm_cfg, "strict_config", False)))
                    self._hist_rm_event("rm.config_audit", payload)
                except Exception:
                    logger.exception("[RiskManager] config audit failed", exc_info=False)

    async def _cancel_and_join(self, tasks: List[asyncio.Task], *, timeout_s: float, label: str) -> List[
        asyncio.Task]:
        remaining: List[asyncio.Task] = [t for t in tasks if t and not t.done()]
        for t in remaining:
            with contextlib.suppress(Exception):
                t.cancel()

        if not remaining:
            return []

        done: Set[asyncio.Task]
        pending: Set[asyncio.Task]
        try:
            done, pending = await asyncio.wait(remaining, timeout=timeout_s)
        except asyncio.CancelledError:
            # stop() must be cancellation-safe
            return remaining

        if pending:
            try:
                names = [getattr(t, "get_name", lambda: "")() or str(t) for t in pending]
                logger.warning("[RiskManager] shutdown pending tasks (label=%s): %s", label, names)
            except Exception:
                logger.warning("[RiskManager] shutdown pending tasks (label=%s)", label)
        return list(pending)

    async def stop(self) -> None:
        """Arrêt idempotent, join propre des boucles et de la glue."""
        if not getattr(self, "_running", False) and not getattr(self, "_tasks", None):
            with contextlib.suppress(Exception):
                if hasattr(self, "ready_event"):
                    self.ready_event.clear()
            return

        self._running = False
        shutdown_start = time.perf_counter()

        cfg = getattr(self, "cfg", None)
        rm_cfg = getattr(cfg, "rm", cfg)
        join_timeout_s = float(getattr(rm_cfg, "rm_stop_join_timeout_s", 2.0))
        glue_timeout_s = float(getattr(rm_cfg, "mbf_glue_stop_timeout_s", 1.0))
        dump_stacks = bool(getattr(rm_cfg, "shutdown_dump_task_stacks", True))
        stack_limit = int(getattr(rm_cfg, "shutdown_stack_limit", 10))
        pending: List[asyncio.Task] = []
        # Arrêt glue d'abord (évite push tardifs)
        if hasattr(self, "_mbf_glue"):
            try:
                await asyncio.wait_for(self._mbf_glue.stop(), timeout=glue_timeout_s)
            except asyncio.TimeoutError:
                logger.warning("[RiskManager] mbf_glue.stop() timeout (%.2fs)", glue_timeout_s)
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("[RiskManager] mbf_glue.stop() failed")

        tasks = list(getattr(self, "_tasks", []))
        try:
            pending = await self._cancel_and_join(tasks, timeout_s=join_timeout_s, label="rm_loops")
        except Exception:
            pending = []
        self._tasks = []

        if pending and dump_stacks:
            for t in pending:
                try:
                    stack = t.get_stack(limit=stack_limit)
                    if stack:
                        logger.warning("[RiskManager] pending task stack: %s", stack)
                except Exception:
                    pass

        if hasattr(self, "ready_event"):
            with contextlib.suppress(Exception):
                self.ready_event.clear()
                with contextlib.suppress(Exception):
                    self.trading_ready_event.clear()
                try:
                    for t in list(self._cb_tasks):
                        t.cancel()
                    self._cleanup_cb_tasks()
                except Exception:
                    pass

                duration = max(0.0, time.perf_counter() - shutdown_start)
                safe_observe(RM_SHUTDOWN_SECONDS, "rm_shutdown_seconds", "rm.stop", duration)
                safe_set(RM_SHUTDOWN_PENDING_TASKS, "rm_shutdown_pending_tasks", "rm.stop", float(len(pending)))
                if pending:
                    safe_inc(RM_SHUTDOWN_TIMEOUT_TOTAL, "rm_shutdown_timeout_total", "rm.stop")

        logger.info("[RiskManager] 🛑 Orchestrateur arrêté en %.3fs (pending=%d)", duration, len(pending))

    # ------------------------------------------------------------------
    # API externes protégées par readiness (patch demandé)
    # ------------------------------------------------------------------

    # ==== [ADD THESE METHODS INSIDE class RiskManager (helpers section)] =========

    def _hash_decision_id(self, pair: str, buy: str, sell: str, ts_ns: int) -> str:
        base = f"{pair}|{buy}|{sell}|{ts_ns // 1_000_000}"  # tranche à la ms
        return hashlib.sha1(base.encode("utf-8")).hexdigest()

    def _build_idempotency_key(
            self,
            *,
            decision_id: str,
            bundle_id: str,
            opp_id: Optional[str],
            notional: Dict[str, Any],
            legs: List[Dict[str, Any]],
    ) -> str:
        leg_fingerprint = [
            {
                "exchange": leg.get("exchange"),
                "symbol": leg.get("symbol"),
                "side": leg.get("side"),
                "qty": leg.get("qty"),
                "price": leg.get("price") or leg.get("px_limit"),
            }
            for leg in (legs or [])
        ]
        base = {
            "decision_id": decision_id,
            "bundle_id": bundle_id,
            "opp_id": opp_id,
            "notional": notional or {},
            "legs": leg_fingerprint,
        }
        data = json.dumps(base, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha1(data.encode("utf-8")).hexdigest()

    def _ensure_canonical_ids(
            self,
            *,
            opp: Dict[str, Any],
            pair: str,
            buy_ex: str,
            sell_ex: str,
            notional: Dict[str, Any],
            legs: List[Dict[str, Any]],
            frag_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        opp_id = (
                opp.get("opp_id")
                or opp.get("opportunity_id")
                or (opp.get("meta") or {}).get("opportunity_id")
        )
        cache_key = str(opp_id or "")
        cached = self._decision_id_cache.get(cache_key) if cache_key else None

        meta_root = opp.setdefault("meta", {}) if isinstance(opp, dict) else {}
        decision_id = (
                opp.get("decision_id")
                or meta_root.get("decision_id")
                or (cached or {}).get("decision_id")
        )
        if decision_id is None:
            decision_ts_ms = int(
                opp.get("decision_ts_ms")
                or (float(opp.get("t_detect") or opp.get("timestamp") or time.time()) * 1000.0)
            )
            decision_id = self._hash_decision_id(pair, buy_ex or "", sell_ex or "", int(decision_ts_ms * 1_000_000))

        trace_id = (
                opp.get("trace_id")
                or meta_root.get("trace_id")
                or (cached or {}).get("trace_id")
                or decision_id
        )
        bundle_base = (
                opp.get("bundle_id")
                or meta_root.get("bundle_id")
                or (cached or {}).get("bundle_id")
                or decision_id
        )
        bundle_id = str(bundle_base)
        frag_idx = (frag_meta or {}).get("idx") if isinstance(frag_meta, dict) else None
        frag_total = (frag_meta or {}).get("total") if isinstance(frag_meta, dict) else None
        if frag_idx is not None and frag_total and int(frag_total) > 1:
            bundle_id = f"{bundle_id}:{int(frag_idx)}"

        idempotency_key = (
                opp.get("idempotency_key")
                or meta_root.get("idempotency_key")
                or (cached or {}).get("idempotency_key")
        )
        if not idempotency_key:
            idempotency_key = self._build_idempotency_key(
                decision_id=str(decision_id),
                bundle_id=str(bundle_id),
                opp_id=str(opp_id) if opp_id is not None else None,
                notional=notional or {},
                legs=legs or [],
            )

        meta_root.setdefault("decision_id", str(decision_id))
        meta_root.setdefault("trace_id", str(trace_id))
        meta_root.setdefault("bundle_id", str(bundle_id))
        meta_root.setdefault("idempotency_key", str(idempotency_key))
        if opp_id:
            meta_root.setdefault("opportunity_id", opp_id)
        opp["meta"] = meta_root

        if cache_key and cache_key not in self._decision_id_cache:
            self._decision_id_cache[cache_key] = {
                "decision_id": str(decision_id),
                "trace_id": str(trace_id),
                "bundle_id": str(bundle_id),
                "idempotency_key": str(idempotency_key),
            }

        return {
            "decision_id": str(decision_id),
            "trace_id": str(trace_id),
            "bundle_id": str(bundle_id),
            "idempotency_key": str(idempotency_key),
        }

    def _current_prudence(self, pair: str) -> str:
        """
        Vue RiskManager de la prudence VM.

        Retourne la clé normalisée "NORMAL" / "CAREFUL" / "ALERT" pour la paire,
        en s'appuyant sur VolatilityManager si présent. En cas d'erreur ou de VM
        absent, retourne toujours "NORMAL".
        """
        vm = getattr(self, "vol_manager", None)
        if vm is None:
            return "NORMAL"

        try:
            # API moderne : VolatilityManager vP1+
            if hasattr(vm, "get_prudence_key"):
                key = vm.get_prudence_key(pair)
            else:
                # Compat : anciennes signatures renvoyant l'état FR.
                if hasattr(vm, "get_prudence_for_pair"):
                    fr_state = vm.get_prudence_for_pair(pair)
                elif hasattr(vm, "get_prudence"):
                    fr_state = vm.get_prudence(pair)
                else:
                    return "NORMAL"

                # Si le VM expose le helper interne, on l'utilise,
                # sinon on fait un best-effort.
                if hasattr(vm, "_prudence_key_for_cfg"):
                    key = vm._prudence_key_for_cfg(fr_state)
                else:
                    key = str(fr_state).upper()

            key = str(key or "").upper()
            if key not in ("NORMAL", "CAREFUL", "ALERT"):
                return "NORMAL"
            return key

        except Exception:
            logging.debug("RM: unable to fetch prudence", exc_info=False)
            return "NORMAL"


    def _maybe_reset_daily_budget(self) -> None:
        interval = max(1.0, float(getattr(self, "_budget_reset_interval_s", 86400.0)))
        now = time.time()
        if (now - getattr(self, "_budget_reset_ts", 0.0)) < interval:
            return
        for strat in list(self._spent_today_quote.keys()):
            self._spent_today_quote[strat] = 0.0
        self._budget_reset_ts = now

    def _check_and_reserve_daily_budget(self, strategy: str, notional_quote: float) -> tuple[bool, str]:
        """Vérifie et réserve le budget journalier pour une stratégie donnée.

               - Réinitialise les compteurs si l'intervalle de reset est dépassé.
               - Bloque si la réservation dépasserait le budget configuré.
               - Incrémente _spent_today_quote en cas d'acceptation.
               """

        self._maybe_reset_daily_budget()
        strat = str(strategy or "TT").upper()
        amount = float(notional_quote or 0.0)
        cap = float(self.daily_strategy_budget_quote.get(strat, 0.0))
        if cap <= 0.0:
            return True, "ok"
        spent = float(self._spent_today_quote.get(strat, 0.0))
        if spent + amount > cap:
            try:
                RM_MM_BUDGET_EXHAUSTED_TOTAL.labels(strat, "GLOBAL", "QUOTE").inc()
            except Exception:
                pass
            return False, "RM_BUDGET_EXHAUSTED"

        self._spent_today_quote[strat] = spent + amount
        try:
            RM_MM_BUDGET_SPENT_QUOTE.labels(strat, "GLOBAL", "QUOTE").inc(amount)
        except Exception:
            pass
        return True, "ok"

    def _budget_allows(self, strategy: str, notional_quote: float) -> tuple[bool, str]:
        return self._check_and_reserve_daily_budget(strategy, notional_quote)


    def _record_budget_spend(self, strategy: Optional[str], bundle: Dict[str, Any]) -> None:
        try:
            self._maybe_reset_daily_budget()
            strat = str(strategy or bundle.get("branch") or "TT").upper()
            notional = bundle.get("notional_quote") or {}
            amount = float(notional.get("amount") or 0.0)
            if amount <= 0.0:
                return
            self._spent_today_quote[strat] = self._spent_today_quote.get(strat, 0.0) + amount
        except Exception:
            logger.debug("[RiskManager] record budget spend failed", exc_info=False)

    def _record_exposure_for_bundle(self, branch: Optional[str], bundle: Dict[str, Any]) -> None:
        """
        Observabilité P0 (M3-4) — enregistre la taille d'exposition notionnelle par branche/profil/combo.

        On se place côté RM, au moment où le bundle est effectivement admis et
        envoyé vers l’Engine/Simu. On logue ici le notional "planifié" du bundle
        (pré-trade), avec les labels:
          - branch  ∈ {"TM", "MM", "REB"} (les autres branches sont ignorées),
          - profile (profil capital RM),
          - combo   (clé combo RM),
          - quote   (devise de quote).

        Cela fournit la métrique rm_exposure_usd{branch,profile,combo,quote}
        demandée par M3-4, côté RM.
        """
        # 0) Branche + filtre sur les branches pertinentes
        branch_u = str(branch or bundle.get("branch") or "").upper()
        if branch_u not in ("TM", "MM", "REB"):
            return

        # 1) Import lazy des métriques pour ne jamais casser la décision
        try:
            from . import obs_metrics  # type: ignore
        except Exception:
            return

        metric = getattr(obs_metrics, "RM_EXPOSURE_USD", None)
        if metric is None:
            # Compat si la métrique n'est pas encore définie dans obs_metrics
            return

        route = bundle.get("route") or {}
        meta = bundle.get("meta") or {}

        # 2) Profil capital et quote
        profile = (
            str(bundle.get("profile")
                or meta.get("profile")
                or getattr(self, "capital_profile", "")
                or "LARGE")
            .upper()
        )
        quote = str(route.get("quote") or meta.get("quote") or "NA").upper()

        # 3) Combo key RM (helper existant)
        combo = ""
        try:
            combo = self._get_combo_key_from_bundle(bundle) or ""
        except Exception:
            combo = ""

        # 4) Notional : on privilégie notional_quote si présent,
        #    sinon on retombe sur _estimate_bundle_notional_usd(...)
        notional = 0.0
        notional_quote = route.get("notional_quote")

        try:
            if isinstance(notional_quote, dict):
                notional = float(notional_quote.get("amount") or 0.0)
            elif notional_quote is not None:
                notional = float(notional_quote)
        except Exception:
            notional = 0.0

        if notional <= 0.0:
            try:
                notional = float(self._estimate_bundle_notional_usd(bundle))
            except Exception:
                notional = 0.0

        if notional <= 0.0:
            # Rien d’exploitable pour l’observabilité, on sort proprement.
            return

        # 5) Publication de la métrique (histogramme ou gauge côté obs_metrics)
        try:
            metric.labels(
                branch=branch_u,
                profile=profile,
                combo=str(combo),
                quote=quote,
            ).observe(notional)
        except Exception:
            # On ne casse jamais le chemin critique pour une simple métrique.
            logging.debug(
                "[RiskManager] _record_exposure_for_bundle: erreur métrique",
                exc_info=False,
            )

            if branch_u in ("TM", "REB"):
                try:
                    pk = self._norm_pair(
                        route.get("pair") or meta.get("pair") or bundle.get("pair") or bundle.get("symbol") or "")
                    asset = self._pair_base(pk)
                    maker_leg = None
                    for leg in bundle.get("legs") or []:
                        if not isinstance(leg, dict):
                            continue
                        if leg.get("meta", {}).get("maker"):
                            maker_leg = leg
                            break

                    if maker_leg is None:
                        for leg in bundle.get("legs") or []:
                            if isinstance(leg, dict):
                                maker_leg = leg
                                break

                    exchange = str(
                        (maker_leg or {}).get("exchange")
                        or meta.get("exchange")
                        or meta.get("venue")
                        or route.get("buy_ex")
                        or route.get("exchange")
                        or ""
                    ).upper()
                    alias = str(
                        (maker_leg or {}).get("account_alias")
                        or meta.get("account_alias")
                        or meta.get("alias")
                        or ""
                    ).upper()
                    side = str((maker_leg or {}).get("side") or meta.get("side") or "").upper()
                    max_expo_s = float(
                        (meta.get("tm") or {}).get("max_exposure_s")
                        or getattr(self, "tm_nn_max_exposure_s", 3.0)
                    )

                    notional_usd = float(notional)
                    asset_u = str(asset or "").upper()

                    if asset_u and exchange and notional_usd > 0.0:
                        entry = {
                            "asset": asset_u,
                            "exchange": exchange,
                            "alias": alias,
                            "side": side,
                            "notional_usd": notional_usd,
                            "opened_ts": time.time(),
                            "max_exposure_s": max_expo_s,
                        }
                        self.tm_inflight_exposures.setdefault(asset_u, []).append(entry)
                except Exception:
                    logger.debug("[RiskManager] tm inflight exposure record failed", exc_info=False)


    def _preflight_gate(self, opp: dict) -> tuple[bool, str, dict]:
        """
        Garde-fous avant stratégie: fraicheur données, skew horloge, prudence, budgets.
        Ne modifie pas opp. Retourne (admit, reason, ctx).
        """
        t0 = time.perf_counter()
        ctx = {}
        try:
            pair = str(opp.get("pair") or opp.get("symbol") or "")
            buy = str(opp.get("buy_exchange") or "").upper()
            sell = str(opp.get("sell_exchange") or "").upper()
            ctx.update({"pair": pair, "buy": buy, "sell": sell})
            missing_fields = [k for k, v in (("pair", pair), ("buy_exchange", buy), ("sell_exchange", sell)) if not v]
            if missing_fields:
                ctx["missing_fields"] = missing_fields
                return (False, RM_OPP_BAD_INPUTS, ctx)

            buy_region = self._resolve_exchange_region(buy)
            sell_region = self._resolve_exchange_region(sell)
            ctx["buy_region"] = buy_region
            ctx["sell_region"] = sell_region
            if buy_region in ("JP",) or sell_region in ("JP",):
                return (
                    False,
                    RM_REGION_UNSUPPORTED,
                    {"exchange": buy or sell, "region": buy_region or sell_region, "pair": pair, **ctx},
                )
            if buy_region in (None, "UNKNOWN") or sell_region in (None, "UNKNOWN"):
                return (False, RM_REGION_UNKNOWN, ctx)

            # Horloge
            try:
                from modules.obs_metrics import get_time_skew_ms
                ctx["time_skew_ms"] = float(get_time_skew_ms())
            except Exception:
                # Fail-closed: si on ne peut pas vérifier le skew, on rejette l'opportunité
                try:
                    from modules.obs_metrics import RM_CLOCK_SKEW_UNKNOWN_TOTAL
                    RM_CLOCK_SKEW_UNKNOWN_TOTAL.inc()
                except Exception:
                    pass
                return (False, "RM_CLOCK_SKEW_UNKNOWN", ctx)
            if ctx["time_skew_ms"] > float(self.max_clock_skew_ms):
                try:
                    TIME_SKEW_MS.set(float(ctx["time_skew_ms"]))
                except Exception:
                    pass
                return (False, RM_MARKETDATA_STALE, ctx)

            # Fraîcheur OB (inchangé côté alimentation : volatility monitor -> RM -> VM ; ici on lit juste les OB du RM)
            b = self.get_book_snapshot(buy, pair)
            s = self.get_book_snapshot(sell, pair)
            if not (self._fresh_enough(b) and self._fresh_enough(s)):
                try:
                    STALE_OPPORTUNITY_DROPPED_TOTAL.inc()
                except Exception:
                    pass
                return (False, RM_MARKETDATA_STALE, ctx)

            # Prudence (VM) — on ne bloque que si ALERT
            prudence = self._current_prudence(pair)
            ctx["prudence"] = prudence
            if prudence == "ALERT":
                return (False, "PRUDENCE_ALERT", ctx)

            # Budget par stratégie (on lit le choix provisoire si fourni par le scanner sinon on re-choisira plus tard)
            exp = opp.get("expected_net_bps")
            strat_hint = "TT"
            if isinstance(exp, dict):
                best_field = exp.get("best")
                if isinstance(best_field, str):
                    strat_hint = best_field
                elif isinstance(best_field, dict):
                    strat_hint = best_field.get("strategy") or strat_hint
            elif isinstance(exp, str):
                strat_hint = exp
            strat = str(strat_hint).upper() or "TT"
            branch = str(opp.get("branch") or strat).upper()
            ctx["branch"] = branch
            base_asset = self._pair_base(pair)
            if base_asset:
                ctx["base_asset"] = base_asset
            q, amt = self._normalize_notional_tuple(opp)  # ta fonction déjà existante
            ctx["notional_quote"] = {"quote": q, "amount": float(amt)}
            ok, why = self._budget_allows(strat, amt)

            if not ok:
                return (False, why, ctx)

            # Gating VaR-lite TT/TM global par asset.
            if branch in ("TT", "TM"):
                base_asset = ctx.get("base_asset") or ctx.get("asset")
                tttm = self._tttm_state_for_asset(base_asset)
                if tttm:
                    state = str(tttm.get("state") or "OK").upper()
                    delta_usd = float(tttm.get("delta_usd") or 0.0)

                    if state == "HARD":
                        try:
                            self.metrics.increment(
                                "RM_TTTM_DELTA_HARD_LIMIT",
                                tags={"asset": str(base_asset).upper(), "branch": branch.lower()},
                            )
                        except Exception:
                            pass
                        ctx["tttm_delta_state"] = "HARD"
                        ctx["tttm_delta_usd"] = delta_usd
                        return False, "RM_TTTM_DELTA_HARD_LIMIT", ctx

                    if state == "SOFT":
                        try:
                            self.metrics.increment(
                                "RM_TTTM_DELTA_SOFT_LIMIT",
                                tags={"asset": str(base_asset).upper(), "branch": branch.lower()},
                            )
                        except Exception:
                            pass
                        ctx["tttm_delta_state"] = "SOFT"
                        ctx["tttm_delta_usd"] = delta_usd

            if branch == "MM":
                base_asset = ctx.get("base_asset") or self._pair_base(pair)
                ctx["base_asset"] = base_asset
                mm_state = self._rm_mm_delta_status_for_asset(base_asset)
                if mm_state:
                    st = str(mm_state.get("state") or "").upper()
                    delta_usd = float(mm_state.get("delta_usd") or 0.0)
                    soft = float(mm_state.get("soft_usd") or mm_state.get("soft_limit_usd") or 0.0)
                    hard = float(mm_state.get("hard_usd") or mm_state.get("hard_limit_usd") or 0.0)

                    if st == "HARD":
                        try:
                            self.metrics.increment("RM_MM_DELTA_HARD_LIMIT", tags={"asset": base_asset})
                        except Exception:
                            pass
                        return (False, "RM_MM_DELTA_HARD_LIMIT", ctx)

                    if st == "SOFT":
                        ctx["mm_delta_state"] = "SOFT"
                        ctx["mm_delta_usd"] = delta_usd
                        try:
                            self.metrics.increment("RM_MM_DELTA_SOFT_LIMIT", tags={"asset": base_asset})
                        except Exception:
                            pass

            return (True, "", ctx)
        finally:
            try:
                RM_PREFLIGHT_MS.observe((time.perf_counter() - t0) * 1000.0)
            except Exception:
                pass

    def _emit_decision_record(
            self,
            status: str,
            reason: str,
            opp: Dict[str, Any],
            ctx: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Enregistre une décision RM (GO/NO-GO) avec vue funnel.

        - status: "admitted" | "skipped" | ...
        - reason: code RM_* ou autre
        - opp: opportunité brute (Scanner)
        - ctx: contexte enrichi (préflight / décision interne)
        """
        try:
            now_ns = time.time_ns()
            ctx = dict(ctx or {})

            # --- Extraction pair / venues ---
            pair = str(opp.get("pair") or opp.get("symbol") or "").upper()
            buy_ex = str(
                opp.get("buy_exchange") or
                (opp.get("legs") or [{}])[0].get("buy_exchange") or
                ctx.get("buy_exchange") or
                ""
            ).upper()
            sell_ex = str(
                opp.get("sell_exchange") or
                (opp.get("legs") or [{}])[0].get("sell_exchange") or
                ctx.get("sell_exchange") or
                ""
            ).upper()

            # --- Prudence + source slippage (si exposés) ---
            prudence = None
            try:
                if hasattr(self, "_current_prudence"):
                    prudence = float(self._current_prudence(pair))
            except Exception:
                prudence = None

            slippage_kind = None
            try:
                slippage_kind = str(getattr(self, "slippage_source", "") or "") or None
            except Exception:
                slippage_kind = None

            # --- Strategy / branch / profile pour le funnel ---
            # Strategy : TT / TM / MM / REB (fallback TT)
            raw_strategy = (
                    ctx.get("strategy")
                    or opp.get("strategy")
                    or opp.get("kind")
                    or (opp.get("route") or {}).get("strategy")
                    or (opp.get("meta") or {}).get("strategy")
                    or ""
            )
            strategy = str(raw_strategy or "TT").upper()
            if strategy not in ("TT", "TM", "MM", "REB"):
                strategy = "TT"

            # Branch : pour l’instant = strategy (extensible si besoin)
            raw_branch = ctx.get("branch") or (opp.get("route") or {}).get("branch") or strategy
            branch = str(raw_branch or strategy).upper()

            # Profile capital : tiré du RM / config
            raw_profile = (
                    getattr(self, "capital_profile", None)
                    or getattr(getattr(self, "cfg", None), "capital_profile", None)
                    or getattr(getattr(self, "cfg", None), "capital_profile_name", None)
                    or "LARGE"
            )
            profile = str(raw_profile or "LARGE").upper()

            # On pousse ces valeurs aussi dans le ctx pour les logs JSONL
            ctx.setdefault("strategy", strategy)
            ctx.setdefault("branch", branch)
            ctx.setdefault("profile", profile)

            # --- Construction de l’enregistrement structuré ---
            rec = DecisionRecord(
                ts_ns=now_ns,
                status=str(status or "").upper(),
                reason=str(reason or ""),
                pair=pair,
                buy_exchange=buy_ex,
                sell_exchange=sell_ex,
                prudence=prudence,
                slippage_kind=slippage_kind,
                strategy=strategy,
                branch=branch,
                profile=profile,
                explain=ctx or None,
            )

            # --- JSONL local (fichier RM) ---
            path = getattr(self, "decision_log_path", "") or ""
            if path:
                try:
                    with open(path, "a", encoding="utf-8") as f:
                        f.write(
                            json.dumps(asdict(rec), ensure_ascii=False, separators=(",", ":"))
                            + "\n"
                        )
                except Exception:
                    # On ne casse jamais le RM pour la traçabilité
                    logger.exception("[RM] Impossible d'écrire dans decision_log_path", exc_info=False)

            # --- Compteurs RM hérités (par status / reason) ---
            try:
                if RM_DECISIONS_TOTAL is not None:
                    RM_DECISIONS_TOTAL.labels(str(status or "").upper()).inc()
            except Exception:
                pass

            if str(status or "").lower() == "skipped":
                try:
                    inc_rm_skip(str(reason or ""))
                except Exception:
                    pass

            # --- Nouveau funnel métrique rm_decision_total (Macro M1-4) ---
            try:
                if hasattr(self, "obs_inc"):
                    self.obs_inc(
                        "rm_decision_total",
                        strategy=strategy,
                        branch=branch,
                        profile=profile,
                        status=str(status or "").upper(),
                        reason=str(reason or ""),
                        reason_kind="RM",
                    )
            except Exception:
                # Never break decision path for obs
                pass
            # --- Emission LHM: RM → historique unifié (M5-B1-3-A) ---
            try:
                self._hist_rm_event(
                    "rm.decision",
                    {
                        "ts_ns": now_ns,
                        "status": str(status or "").upper(),
                        "reason": str(reason or ""),
                        "pair": pair,
                        "buy_exchange": buy_ex,
                        "sell_exchange": sell_ex,
                        "prudence": prudence,
                        "slippage_kind": slippage_kind,
                        "strategy": strategy,
                        "branch": branch,
                        "profile": profile,
                        # Tags de contexte utiles pour PnL/audit
                        "rm_mode": getattr(self, "mode", None),
                        "pacer_mode": getattr(self, "pacer_mode", None),
                        "ctx": ctx or None,
                    },
                )
            except Exception:
                # Jamais de blocage décisionnel à cause de l'historique
                pass

        except Exception:
            # Sécurité maximale : jamais d'exception qui remonte
            logger.exception("[RM] _emit_decision_record failed", exc_info=False)

    def _cleanup_cb_tasks(self) -> None:
        try:
            done = {t for t in self._cb_tasks if t.done()}
            self._cb_tasks.difference_update(done)
        except Exception:
            self._cb_tasks = {t for t in self._cb_tasks if not t.done()}

    def _dispatch_best_effort(self, cb: Callable[[Any], Any], *, name: str, payload: Any) -> None:
        if not callable(cb):
            return

        loop = asyncio.get_running_loop()
        cfg = getattr(self, "cfg", None)
        rm_cfg = getattr(cfg, "rm", cfg)
        timeout_s = float(getattr(rm_cfg, "cb_timeout_s", 0.25) or 0.25)
        max_inflight = int(getattr(rm_cfg, "cb_max_inflight", 200) or 200)
        executor_workers = int(getattr(rm_cfg, "cb_executor_workers", 2) or 2)

        self._cleanup_cb_tasks()
        if len(self._cb_tasks) >= max_inflight:
            safe_inc(RM_CALLBACK_DROPS_TOTAL, "rm_callback_drops_total", name, cb_name=name, reason="overflow")
            return

        if self._cb_executor is None and executor_workers > 0:
            try:
                self._cb_executor = ThreadPoolExecutor(max_workers=executor_workers)
            except Exception:
                self._cb_executor = None

        async def _run() -> None:
            start = time.perf_counter()
            try:
                res = cb(payload)
                if inspect.isawaitable(res):
                    await asyncio.wait_for(res, timeout=timeout_s)
                elif self._cb_executor:
                    fut = loop.run_in_executor(self._cb_executor, cb, payload)
                    await asyncio.wait_for(fut, timeout=timeout_s)
            except asyncio.TimeoutError:
                safe_inc(RM_CALLBACK_DROPS_TOTAL, "rm_callback_drops_total", name, cb_name=name, reason="timeout")
            except asyncio.CancelledError:
                safe_inc(RM_CALLBACK_DROPS_TOTAL, "rm_callback_drops_total", name, cb_name=name, reason="cancelled")
            except Exception:
                safe_inc(RM_CALLBACK_DROPS_TOTAL, "rm_callback_drops_total", name, cb_name=name, reason="error")
            else:
                duration_ms = (time.perf_counter() - start) * 1000.0
                safe_observe(RM_CALLBACK_LATENCY_MS, "rm_callback_latency_ms", name, duration_ms, cb_name=name)
            finally:
                safe_set(RM_CALLBACK_INFLIGHT, "rm_callback_inflight", name, float(max(0, len(self._cb_tasks) - 1)),
                         cb_name=name)

        task = asyncio.create_task(_run(), name=f"rm-cb-{name}")
        self._cb_tasks.add(task)
        safe_set(RM_CALLBACK_INFLIGHT, "rm_callback_inflight", name, float(len(self._cb_tasks)), cb_name=name)

    def _hist_rm_event(self, kind: str, payload: Dict[str, Any]) -> None:
        """
        [M5-B1-3-A] Émission best-effort d'un event RiskManager vers l'historique (LHM).

        - kind: étiquette logique (ex: "rm.decision", "reb.detected", "fees.reality_check").
        - payload: dict déjà structuré par l'appelant.

        Invariants:
        - Jamais bloquant: si history_logger est absent ou plante, on ignore.
        - N'ajoute que quelques pivots (module/_kind/ts_ms) si possible.
        """
        try:
            sink = getattr(self, "history_logger", None)
            if not callable(sink):
                return

            event = dict(payload or {})
            # source/module par défaut
            event.setdefault("module", "RM")
            event.setdefault("_kind", str(kind))

            # ts_ms: si on a ts_ns, on dérive un pivot ms
            ts_ns = event.get("ts_ns")
            if ts_ns is not None and "ts_ms" not in event:
                try:
                    event["ts_ms"] = int(int(ts_ns) / 1_000_000)
                except Exception:
                    pass

            # stream/log_type pivots génériques — seront recanonisés côté LHM
            event.setdefault("log_type", "rm")
            event.setdefault("stream", "rm")

            self._dispatch_best_effort(sink, name="history_sink", payload=event)
        except Exception:
            # Jamais d'escalade depuis la voie historique
            try:
                logger.exception("[RM] _hist_rm_event failed", exc_info=False)
            except Exception:
                pass

    def _emit_trade_fsm_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        mgr = getattr(self, "_history_fsm", None)
        if not mgr or not hasattr(mgr, "record_trade_fsm_event"):
            return
        try:
            result = mgr.record_trade_fsm_event(payload, event_type=event_type, stream="trade_fsm")
            if inspect.isawaitable(result):
                asyncio.create_task(result)
        except Exception:
            logger.exception("[RM] _emit_trade_fsm_event failed", exc_info=False)
    # ==== [ADD THIS METHOD INSIDE class RiskManager] =============================
    def _emit_final_decision(self, opp: dict, choice: dict, outcome: str = "submitted") -> None:
        """
        Émet un enregistrement 'final' au moment où la route est choisie (TT/TM/MM).
        'choice' est un dict léger construit juste après ta sélection (voir bloc 3).
        """
        try:
            pair = str(opp.get("pair") or opp.get("symbol") or "")
            route = str(choice.get("route", "")).upper() or "TT"
            buy_ex = str(choice.get("buy_exchange") or opp.get("buy_exchange") or "").upper()
            sell_ex = str(choice.get("sell_exchange") or opp.get("sell_exchange") or "").upper()

            # tailles & coûts — on prend ce que ta logique a déjà calculé, sinon fallback 0
            size_quote = float(choice.get("size_quote", 0.0))
            size_base = float(choice.get("size_base", 0.0))
            expected_edge_bps = float(choice.get("expected_edge_bps", 0.0))
            total_cost_pct = float(choice.get("total_cost_pct", 0.0))
            fees_pct = float(choice.get("fees_pct", 0.0))
            slippage_pct = float(choice.get("slippage_pct", 0.0))

            rec = {
                "type": "final",
                "ts_ns": time.time_ns(),
                "pair": pair,
                "route": route,  # TT | TM | MM
                "buy_exchange": buy_ex,
                "sell_exchange": sell_ex,
                "size_quote": size_quote,  # notional en quote
                "size_base": size_base,  # quantité base si dispo
                "expected_edge_bps": expected_edge_bps,
                "slippage_kind": getattr(self, "slippage_source", "ewma"),
                "costs": {
                    "total_pct": total_cost_pct,
                    "fees_pct": fees_pct,
                    "slippage_pct": slippage_pct,
                },
                "prudence": str(
                    getattr(self, "_current_prudence")(pair) if hasattr(self, "_current_prudence") else "UNKNOWN"),
                "outcome": outcome,  # "submitted" | "simulated" | "skipped"...
            }

            # métrique par route
            RM_FINAL_DECISIONS_TOTAL.labels(rec["route"]).inc()

            # JSONL optionnel (même fichier que DecisionRecord si tu veux)
            path = getattr(self, "decision_log_path", "")
            if path:
                import json
                with open(path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
        except Exception:
            pass

    # =============================================================================
    def _canonize_opportunity(self, opp: Dict[str, Any]) -> None:
        """
        TICKET P0-1: Adapte l'opportunité (ex-Scanner) pour assurer la compatibilité avec le RM.
        Unwrap le payload si présent et synchronise les clés critiques (buy_ex, notional_quote).
        """
        payload = opp.get("payload")
        if isinstance(payload, dict):
            # A) Map payload fields to top-level if missing (Invariant B)
            for k in [
                "buy_ex", "sell_ex", "notional_quote", "legs", "quote",
                "pair_key", "tier", "strategy",
                # P0: rendre visibles au RM (sinon rejects SIGNAL_MISSING_BOOK_AGE)
                "book_age_ms", "book_ts_ms", "decision_ts_ms",
            ]:
                if k in payload and opp.get(k) is None:
                    opp[k] = payload[k]

            # P0: le Scanner emballe l'opportunité dans opp{payload=...}.
            # Le RM attend certains champs au top-level.
            pk = payload.get("pair_key")
            if pk and (not opp.get("pair") or str(opp.get("pair") or "").upper() in ("UNKNOWN", "")):
                opp["pair"] = pk
            if pk and not opp.get("symbol"):
                opp["symbol"] = pk

        # B) Alignement échanges (Invariant A)
        if not opp.get("buy_ex") and opp.get("buy_exchange"):
            opp["buy_ex"] = opp.get("buy_exchange")
        if not opp.get("sell_ex") and opp.get("sell_exchange"):
            opp["sell_ex"] = opp.get("sell_exchange")

        if not opp.get("buy_exchange") and opp.get("buy_ex"):
            opp["buy_exchange"] = opp.get("buy_ex")
        if not opp.get("sell_exchange") and opp.get("sell_ex"):
            opp["sell_exchange"] = opp.get("sell_ex")

        # P0: Dérivation de 'branch' si absent
        if not opp.get("branch"):
            candidates = opp.get("branch_candidates") or []
            strategy = str(opp.get("strategy") or "").upper()
            if strategy in ("TT", "TM", "MM", "REB"):
                opp["branch"] = strategy
            elif candidates:
                # Priorité TT > TM > MM
                if "TT" in candidates: opp["branch"] = "TT"
                elif "TM" in candidates: opp["branch"] = "TM"
                elif "MM" in candidates: opp["branch"] = "MM"
                else: opp["branch"] = candidates[0]
            else:
                # Fallback par défaut
                opp["branch"] = "TT"

        # C) Garantie du format notional_quote (Invariant A + P0-2 compat)
        nq = opp.get("notional_quote")
        if not isinstance(nq, dict) or "ccy" not in nq or "amount" not in nq:
            q, amt = self._normalize_notional_tuple(opp)
            opp["notional_quote"] = {"ccy": q, "amount": float(amt)}

    # =============================================================================

    # ==== [REPLACE WHOLE METHOD handle_opportunity WITH THIS ONE] ================
    async def handle_opportunity(self, opp: Dict[str, Any]) -> None:
        """
        Entrée stratégie (ex-Scanner) — pré-filtre industry-like puis délégation.
        Ne modifie pas la logique de décision interne : on garde exactement le pipeline existant.
        """
        # Canonisation des entrées (Ticket P0-1)
        try:
            self._canonize_opportunity(opp)
        except Exception:
            logging.exception("[RiskManager] Opportunity canonization failed")

        # Readiness (comme avant)
        if getattr(self, "trading_ready_event", None) and not self.trading_ready_event.is_set():
            reason = normalize_reason_code("TRADING_NOT_READY") or "TRADING_NOT_READY"
            try:
                self._emit_decision_record("skipped", reason, opp, {})
            except Exception:
                pass
            return
        self._ensure_ready()

        # Pré-filtre
        admit, reason, ctx = self._preflight_gate(opp)
        if not admit:
            # audit + métriques puis sortie
            try:
                self._emit_decision_record("skipped", reason, opp, ctx)
            except Exception:
                pass
            return

        # audit "admitted" minimal (le détail final peut être émis plus loin si tu veux)
        self._emit_decision_record("admitted", "", opp, ctx)
        decision_ctx: Dict[str, Any] = {"submitted": False, "attempted": False, "reasons": []}

        # Délégation au pipeline existant (inchangé)
        try:
            # si on_scanner_opportunity est synchrone dans ton code actuel, enlève "await" (garde une seule variante)
            res = self.on_scanner_opportunity(opp, decision_ctx=decision_ctx)
            if inspect.iscoroutine(res):
                res = await res
        except Exception as exc:
            logging.exception("Unhandled exception during on_scanner_opportunity")
            try:
                if RM_DROPPED_TOTAL is not None:
                    RM_DROPPED_TOTAL.labels(RM_INTERNAL_ERROR).inc()
            except Exception:
                pass
            try:
                self._hist_rm_event(
                    "rm.decision",
                    {
                        "ts_ns": time.time_ns(),
                        "status": "SKIPPED",
                        "reason": RM_INTERNAL_ERROR,
                        "pair": str(opp.get("pair") or opp.get("symbol") or ""),
                        "exchange": str(opp.get("buy_exchange") or ""),
                        "exc_class": type(exc).__name__,
                        "stage": "handle_opportunity",
                    },
                )
            except Exception:
                pass
            decision_ctx.setdefault("reasons", []).append(RM_INTERNAL_ERROR)
            res = decision_ctx

        summary = res if isinstance(res, dict) else decision_ctx
        reasons = summary.get("reasons") or []
        primary_reason = _rm_pick_reason(*reasons) if reasons else ""
        status_final = "submitted" if summary.get("submitted") else "skipped"
        primary_reason = (
            _rm_pick_reason(primary_reason or "", RM_INTERNAL_SKIP)
            if not summary.get("submitted")
            else primary_reason
        )
        self._emit_decision_record(status_final, primary_reason, opp, ctx)

    # =============================================================================
    async def submit_bundle_from_rpc(self, payload: dict) -> dict:
        def _bundle_ack(ok: bool, reason: str = "") -> dict:
            meta = bundle.get("meta") or {}
            return {
                "trace_id": meta.get("trace_id") or bundle.get("trace_id"),
                "decision_id": meta.get("decision_id") or bundle.get("decision_id"),
                "bundle_id": meta.get("bundle_id") or bundle.get("bundle_id"),
                "idempotency_key": meta.get("idempotency_key") or bundle.get("idempotency_key"),
                "state": "ENGINE_ACCEPTED" if ok else "ENGINE_REJECTED",
                "reason_code": reason or None,
            }

        if not isinstance(payload, dict):
            bundle = {}
            return _bundle_ack(False, BUNDLE_ILLEGAL)

        bundle = dict(payload)
        meta = bundle.get("meta") or {}
        if not isinstance(meta, dict):
            meta = {}
        route = bundle.get("route") or {}
        if not isinstance(route, dict):
            route = {}
        if not route:
            route = {
                "pair": bundle.get("pair") or bundle.get("symbol") or meta.get("pair") or meta.get("symbol"),
                "buy_ex": bundle.get("buy_ex") or bundle.get("buy_exchange") or meta.get("buy_ex"),
                "sell_ex": bundle.get("sell_ex") or bundle.get("sell_exchange") or meta.get("sell_ex"),
            }
        bundle["route"] = route

        branch = str(bundle.get("branch") or meta.get("branch") or meta.get("strategy") or "TT").upper()
        profile = str(
            bundle.get("profile") or meta.get("profile") or getattr(self, "capital_profile", "LARGE") or "LARGE"
        ).upper()
        meta.setdefault("branch", branch)
        meta.setdefault("profile", profile)
        if route.get("pair"):
            meta.setdefault("pair", route.get("pair"))
        if route.get("buy_ex"):
            meta.setdefault("buy_ex", route.get("buy_ex"))
        if route.get("sell_ex"):
            meta.setdefault("sell_ex", route.get("sell_ex"))
        bundle["meta"] = meta

        if getattr(self, "trading_ready_event", None) and not self.trading_ready_event.is_set():
            reason = normalize_reason_code("TRADING_NOT_READY") or "TRADING_NOT_READY"
            return _bundle_ack(False, reason)

        try:
            self._ensure_ready()
        except NotReadyError:
            return _bundle_ack(False, RM_ENGINE_NOT_READY)

        if branch == "TT" and not bool(getattr(self.cfg, "enable_tt", getattr(self, "enable_tt", True))):
            return _bundle_ack(False, RM_CAP_BRANCH_DISABLED)
        if branch == "TM" and not bool(getattr(self.cfg, "enable_tm", getattr(self, "enable_tm", True))):
            return _bundle_ack(False, RM_CAP_BRANCH_DISABLED)
        if branch == "MM" and not bool(getattr(self.cfg, "enable_mm", getattr(self, "enable_mm", False))):
            return _bundle_ack(False, "MM_DISABLED")
        if branch == "REB" and not bool(getattr(self.cfg, "enable_reb", getattr(self, "enable_reb", True))):
            return _bundle_ack(False, RM_CAP_BRANCH_DISABLED)

        legal, legal_reason = self._is_bundle_legal(bundle)
        if not legal:
            return _bundle_ack(False, legal_reason or BUNDLE_ILLEGAL)

        notional_quote = bundle.get("notional_quote") or {}
        if isinstance(notional_quote, dict) and "ccy" in notional_quote and "quote" not in notional_quote:
            notional_quote = dict(notional_quote)
            notional_quote["quote"] = notional_quote.get("ccy")

        opp = {
            "pair": route.get("pair") or meta.get("pair"),
            "buy_exchange": route.get("buy_ex") or meta.get("buy_ex"),
            "sell_exchange": route.get("sell_ex") or meta.get("sell_ex"),
            "notional_quote": notional_quote,
            "branch": branch,
            "expected_net_bps": {"best": branch},
        }
        admit, reason, _ctx = self._preflight_gate(opp)
        if not admit:
            return _bundle_ack(False, reason)

        decision_ctx: Dict[str, Any] = {"submitted": False, "attempted": False, "reasons": []}
        ok = self.engine_enqueue_bundle(bundle, decision_ctx=decision_ctx)
        reasons = decision_ctx.get("reasons") or []
        primary_reason = _rm_pick_reason(*reasons) if reasons else ""
        return _bundle_ack(bool(ok), primary_reason)

    # =============================================================================
    async def rebalance_tick(self) -> None:
        """
        Tick manuel de la boucle rebalancing, utile en mode “piloté”.
        """
        self._ensure_ready()
        try:
            if not self.rebalancing:
                return

            imb = self.rebalancing.detect_imbalance()
            # Obs: un déséquilibre détecté
            try:
                from modules.obs_metrics import REBAL_DETECTED_TOTAL
                REBAL_DETECTED_TOTAL.labels("detected").inc()
            except Exception:
                pass

            plan = None
            if hasattr(self.rebalancing, "build_plan"):
                try:
                    plan = self.rebalancing.build_plan(imb)
                except Exception:
                    plan = None
                try:
                    if plan and isinstance(plan, dict):
                        from modules.obs_metrics import REBAL_PLAN_QUANTUM_QUOTE
                        qmap = plan.get("quantum_quote") or {}
                        if isinstance(qmap, dict) and qmap:
                            for quote, qval in qmap.items():
                                try:
                                    REBAL_PLAN_QUANTUM_QUOTE.labels(str(quote).upper()).set(float(qval or 0.0))
                                except Exception:
                                    # On ne casse jamais le tick pour une simple métrique.
                                    pass
                except Exception:
                    pass

            if hasattr(self.rebalancing, "push_history"):
                try:
                    self.rebalancing.push_history(imb, plan or {})
                except Exception:
                    logging.exception("Unhandled exception")

            self._alert(
                "RiskManager",
                f"Rebalancing détecté: {imb}" + (f" | plan={plan}" if plan else ""),
                alert_type="WARNING"
            )

            now = time.time()
            self._rebalancing_until = max(self._rebalancing_until, now + self.rebal_active_ttl_s)

            # Construire les opérations et les router (cooldown respecté)
            ops: List[Dict[str, Any]] = []
            try:
                if plan is not None and hasattr(self.rebalancing, "plan_to_operations"):
                    ops = list(self.rebalancing.plan_to_operations(plan) or [])
                else:
                    ops = list(self._make_rebalancing_actions(plan or {}))
            except Exception:
                logger.exception("[RiskManager] plan_to_operations failed, fallback to legacy")
                ops = list(self._make_rebalancing_actions(plan or {}))

            if now >= self._rebal_emit_next_allowed:
                for op in ops:
                    try:
                        await self._handle_rebalancing_op(op)
                    except Exception:
                        logger.exception("[RiskManager] handle rebal op failed")
                self._rebal_emit_next_allowed = now + self.rebal_emit_cooldown_s

        except Exception as e:
            logger.exception(f"[RiskManager] rebalance_tick: {e}")

    # ------------------------------------------------------------------
    # Internal event hub (submodules -> RiskManager -> outside)
    # ------------------------------------------------------------------
    def on_mbf_event(self, event: Dict[str, Any]) -> None:
        """
        [M5-B1-3-B] Point d'entrée dédié pour les évènements MultiBalanceFetcher.

        Hypothèse:
        - Boot appelle MultiBalanceFetcher.set_event_sink(self.on_mbf_event)
          via _wire_mbf_event_sink().

        Event attendu (capital_refresh):
          {
            "type": "capital_refresh",
            "exchange": "...",
            "alias": "...",
            "source": "...",
            "ts": <float seconds>,
          }
        """
        try:
            ev_type = str(event.get("type") or "").lower()
            if ev_type != "capital_refresh":
                # Pour l'instant on ignore les autres évènements MBF
                return

            ex = str(event.get("exchange") or "NA").upper()
            alias = str(event.get("alias") or "NA").upper()
            source = str(event.get("source") or "unknown")
            ts = float(event.get("ts") or time.time())
            ts_ns = int(ts * 1e9)

            payload = {
                "ts_ns": ts_ns,
                "module": "MBF",
                "event": ev_type,
                "exchange": ex,
                "alias": alias,
                "source": source,
                # On garde la forme brute pour forensic
                "mbf_event": dict(event or {}),
            }

            # Event LHM canonicalisé : balance.capital_refresh
            self._hist_rm_event("balance.capital_refresh", payload)
        except Exception:
            try:
                logger.exception("[RM] on_mbf_event failed", exc_info=False)
            except Exception:
                pass


    def _submodule_event(self, event: Dict[str, Any]) -> None:
        """
        Point d’entrée unique des sous-modules (VOL/SFC/REB/Sim).
        - Zéro import Prometheus dans les sous-modules.
        - On importe obs_metrics ici à la volée.
        """
        try:
            module = str(event.get("module") or "RM").upper()
            ev = str(event.get("event") or "").lower()
            level = str(event.get("level") or "INFO").upper()
            pair = event.get("pair")
            event_id = str(event.get("event_id") or event.get("id") or event.get("event") or "")

            # 1) Alerte centralisée (pager/digest)
            try:
                self._alert(module, event.get("message", ""), pair=pair, alert_type=level)
            except Exception:
                pass

            # 2) Publication métriques centralisée
            try:
                from modules.obs_metrics import (
                    REBAL_DETECTED_TOTAL, REBAL_PLAN_QUANTUM_QUOTE,
                    FEESYNC_LAST_TS, FEE_MISMATCH_TOTAL,
                    FEES_EXPECTED_BPS, FEES_REALIZED_BPS,  # déjà utilisés par _publish_fee_rc_obs
                    TOTAL_COST_BPS,
                )
            except Exception:
                REBAL_DETECTED_TOTAL = REBAL_PLAN_QUANTUM_QUOTE = None
                FEESYNC_LAST_TS = FEE_MISMATCH_TOTAL = FEES_EXPECTED_BPS = FEES_REALIZED_BPS = TOTAL_COST_BPS = None

            if module == "REB":
                if ev == "detected":
                    try:
                        REBAL_DETECTED_TOTAL.labels("detected").inc()
                    except Exception:
                        pass
                    # [M5-B1-3-A] Mirror REB.detected vers LHM (reb.detected)
                    try:
                        self._hist_rm_event(
                            "reb.detected",
                            {
                                "ts_ns": event.get("ts_ns"),
                                "pair": pair,
                                "quote": str(event.get("quote") or "NA").upper(),
                                "status": "DETECTED",
                            },
                        )
                    except Exception:
                        pass

                elif ev == "planned":
                    q = str(event.get("quote") or "NA").upper()
                    quantum = float(event.get("quantum_quote") or 0.0)
                    try:
                        REBAL_PLAN_QUANTUM_QUOTE.labels(q).set(quantum)
                    except Exception:
                        pass
                    # [M5-B1-3-A] Mirror REB.planned vers LHM (reb.planned)
                    try:
                        self._hist_rm_event(
                            "reb.planned",
                            {
                                "ts_ns": event.get("ts_ns"),
                                "pair": pair,
                                "quote": q,
                                "quantum_quote": quantum,
                                "status": "PLANNED",
                            },
                        )
                    except Exception:
                        pass

            elif module == "SFC":
                if ev == "fee_sync_done":
                    ex = str(event.get("exchange") or "NA").upper()
                    alias = str(event.get("alias") or "NA").upper()
                    ts = float(event.get("last_refresh_ts") or time.time())
                    try:
                        FEESYNC_LAST_TS.labels(ex, alias).set(ts)
                    except Exception:
                        pass
                elif ev == "reality_check_exceeded":
                    ex = str(event.get("exchange") or "NA").upper()
                    alias = str(event.get("alias") or "NA").upper()
                    side = str(event.get("side") or "NA").upper()
                    try:
                        FEE_MISMATCH_TOTAL.labels(ex, alias, side).inc()
                    except Exception:
                        pass
            # --- Emission LHM pour les évènements SFC (M5-B1-3) ---
            try:
                ts_ns = int(time.time_ns())
                base_payload = {
                    "ts_ns": ts_ns,
                    "module": "SFC",
                    "event": ev,
                    # event brut pour forensic / audit
                    "sfc_event": dict(event or {}),
                }
                if ev == "fee_sync_done":
                    base_payload.update(
                        {
                            "exchange": str(event.get("exchange") or "NA").upper(),
                            "alias": str(event.get("alias") or "NA").upper(),
                            "last_refresh_ts": float(event.get("last_refresh_ts") or time.time()),
                        }
                    )
                    self._hist_rm_event("feesync.done", base_payload)
                elif ev == "reality_check_exceeded":
                    base_payload.update(
                        {
                            "exchange": str(event.get("exchange") or "NA").upper(),
                            "alias": str(event.get("alias") or "NA").upper(),
                            "side": str(event.get("side") or "NA").upper(),
                        }
                    )
                    self._hist_rm_event("fees.reality_check_exceeded", base_payload)
            except Exception:
                # Historique = best-effort, jamais bloquant
                pass
            if module == "PWS":
                try:
                    if not event_id:
                        event_id = hashlib.sha1(
                            json.dumps(event or {}, sort_keys=True, default=str).encode("utf-8")
                        ).hexdigest()
                except Exception:
                    event_id = event_id or ""
                payload = dict(event or {})
                payload.setdefault("event_id", event_id)
                payload.setdefault("ts_ns", int(time.time_ns()))
                if hasattr(self._lhm_manager, "record_privatews_event"):
                    result = self._lhm_manager.record_privatews_event(payload)
                    if inspect.isawaitable(result):
                        asyncio.create_task(result)
                else:
                    self._hist_rm_event("privatews.event", payload)

            # VOL: la boucle _loop_volatility publie déjà VOL_* → no-op ici.

        except Exception:
            try:
                logger.debug("[RiskManager] _submodule_event fallback", exc_info=False)
            except Exception:
                pass

    def _emit_private_plane_event(self, event: str, **extra: Any) -> None:
        payload = {"module": "PWS", "event": event}
        if extra:
            payload.update(extra)
        try:
            self._submodule_event(payload)
        except Exception:
            logger.debug("[RiskManager] private plane event drop (%s)", event, exc_info=False)

    def _mark_books_observability(self, orderbooks: Dict[str, Dict[str, Any]]) -> None:
        """Met à jour les métriques de fraîcheur orderbook (best-effort)."""
        try:
            for ex, pairs in (orderbooks or {}).items():
                for pk in (pairs or {}).keys():
                    mark_books_fresh(self._norm_pair(pk))
        except Exception:
            try:
                mark_books_fresh("ALL")
            except Exception:
                pass

    def _mark_balances_observability(self, balances: Dict[str, Dict[str, Any]]) -> None:
        """Met à jour les métriques de fraîcheur balances (best-effort)."""
        try:
            for ex, per_alias in (balances or {}).items():
                for alias in (per_alias or {}).keys():
                    mark_balances_fresh(ex, alias)
        except Exception:
            try:
                mark_balances_fresh("ALL", "ALL")
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Loops
    # ------------------------------------------------------------------
    async def _loop_orderbooks(self):
        while self._running:
            try:
                res = self._get_orderbooks()
                books = await res if inspect.isawaitable(res) else res

                if not isinstance(books, dict):
                    await asyncio.sleep(self.t_books)
                    continue

                self._last_books = books or {}
                ob_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
                now_s = time.time()
                for ex, pairs in (self._last_books or {}).items():
                    exu = str(ex).upper()
                    for pk, snap in (pairs or {}).items():
                        norm_pk = self._norm_pair(pk)
                        bid = float((snap or {}).get("best_bid") or 0.0)
                        ask = float((snap or {}).get("best_ask") or 0.0)
                        
                        # Update EWMA Imbalance
                        self._update_imbalance_ewma(exu, norm_pk, snap)

                        ts_ms = (snap or {}).get("exchange_ts_ms") or (snap or {}).get("recv_ts_ms") or 0
                        ts = float(ts_ms) / 1000.0 if ts_ms else now_s
                        ob_cache[(exu, norm_pk)] = {
                            "best_bid": bid,
                            "best_ask": ask,
                            "ts": ts,
                        }
                self._orderbooks = ob_cache
                self._mark_books_observability(self._last_books)
                
                # P0: Mise à jour des statuts RM pour Grafana (health/slip/vol/total)
                try:
                    from modules.obs_metrics import update_rm_status, RM_QUEUE_DEPTH
                    # Update queue depth (si on utilise une file d'attente interne)
                    if hasattr(self, "_event_queue") and RM_QUEUE_DEPTH is not None:
                        try:
                            q_size = self._event_queue.qsize() if hasattr(self._event_queue, "qsize") else 0
                            RM_QUEUE_DEPTH.labels(exchange="ALL").set(float(q_size))
                        except Exception: pass

                    for ex in self.exchanges:
                        exu = ex.upper()
                        # Santé globale (1 si on a des books)
                        update_rm_status(exu, "health", 1.0)
                        # Statut Slippage (1 si alimenté)
                        update_rm_status(exu, "slip", 1.0 if self.slip_collector else 0.0)
                        # Statut Volatilité (1 si monitor actif)
                        update_rm_status(exu, "vol", 1.0 if self.volatility else 0.0)
                        # Statut Total (Toujours 1 si la boucle tourne)
                        update_rm_status(exu, "total", 1.0)
                except Exception:
                    pass

                # Slippage & fees collector (passif)
                if self.slip_collector and hasattr(self.slip_collector, "collect_from_orderbooks"):
                    try:
                        self.slip_collector.collect_from_orderbooks(self._last_books)
                    except Exception:
                        logger.debug("[RiskManager] slippage collector collect failed", exc_info=False)
                self._sync_simulator_fee_map()

                # Pousser le slippage courant vers le simulateur
                if self.simulator and hasattr(self.simulator, "update_slippage"):
                    try:
                        for ex, pairs in (self._last_books or {}).items():
                            for pk in (pairs or {}).keys():
                                sb = ss = None
                                if self.slippage_handler and hasattr(self.slippage_handler, "get_slippage"):
                                    try:
                                        sb = float(self.slippage_handler.get_slippage(ex, pk, "buy"))
                                        ss = float(self.slippage_handler.get_slippage(ex, pk, "sell"))
                                    except Exception:
                                        sb = ss = None
                                if (sb is None or ss is None) and self.slip_collector:
                                    try:
                                        recent = float(self.slip_collector.get_recent_slippage(pk))
                                        if sb is None:

                                            sb = recent
                                        if ss is None:
                                            ss = recent
                                    except Exception:
                                        logging.exception("Unhandled exception")
                                if sb is not None:
                                    self.simulator.update_slippage(ex, pk, "BUY", sb)
                                if ss is not None:
                                    self.simulator.update_slippage(ex, pk, "SELL", ss)
                    except Exception:
                        logger.debug("[RiskManager] push slip to simulator failed", exc_info=False)
                try:
                    self._emit_slippage_to_scanner()
                except Exception:
                    logger.debug("[RiskManager] emit slippage -> scanner failed", exc_info=False)

                # Rebalancing snapshots
                if self.rebalancing:
                    for ex, pairs in (self._last_books or {}).items():
                        for pair_key, d in (pairs or {}).items():
                            try:
                                if hasattr(self.rebalancing, "ingest_snapshot"):
                                    self.rebalancing.ingest_snapshot({
                                        "exchange": ex,
                                        "pair_key": pair_key,
                                        "best_bid": d.get("best_bid"),
                                        "best_ask": d.get("best_ask"),
                                        "active": d.get("active", True),
                                    })
                            except Exception:
                                logging.exception("Unhandled exception")

                # PATCH HOOK: pousser les métriques slippage vers le Scanner
                #self._emit_slippage_to_scanner()

                self.last_update = time.time()
                self._mark_loop_success("orderbooks")
            except Exception as e:
                logger.exception(f"[RiskManager] orderbooks loop: {e}")
                self._mark_loop_error("orderbooks", e)
            await asyncio.sleep(self.t_books)

    def _get_ladder_region(self) -> str:
        cfg = getattr(self, "cfg", None)
        if cfg is None:
            return "DEFAULT"
        region = getattr(cfg, "pod_region", getattr(cfg, "region", "DEFAULT"))
        try:
            return str(region or "DEFAULT").upper()
        except Exception:
            return "DEFAULT"

    def _run_capital_ladder_tick(self) -> None:
        """Tick léger de la ladder capital (v1 — placeholders).

        7-RM-2a : structure uniquement, les signaux riches seront câblés en 7-RM-2b.
        """
        ctrl = getattr(self, "_profile_ctrl", None)
        if not ctrl:
            return

        # Si le Pacer n'est pas encore branché, on ne fait rien.
        if not getattr(ctrl, "pacer", None):
            return

        region = self._get_ladder_region()

        try:
            # Placeholders : conditions volontairement non favorables pour éviter
            # tout changement de profil tant que 7-RM-2b n'est pas en place.
            ctrl.maybe_upgrade(
                region=region,
                net_per_sc=0.0,
                slo_ok=False,
                pnl_ok=False,
                dd_ok=True,
                inflight_util_ok=False,
            )
            ctrl.maybe_downgrade(
                region=region,
                severe_evt=False,
                err_spike=False,
                lag_spike=False,
                drain_spike=False,
                dd_breach=False,
            )
        except Exception:
            logger.debug("[RiskManager] ladder_tick placeholder failed", exc_info=False)


    async def _reconcile_pending_transfers(self) -> None:
        """
        P0: Boucle de réconciliation REST pour les transferts bloqués en SUBMITTED.
        """
        if not self._transfer_controller:
            return
        
        pending_ids = self._transfer_controller.list_pending_transfer_ids()
        if not pending_ids:
            return
            
        mbf = getattr(self, "balance_fetcher", None)
        if not mbf or not hasattr(mbf, "verify_transfer_status_rest"):
            return
            
        for tid in pending_ids:
            payload = self._transfer_controller.get_transfer_payload(tid)
            if not payload: continue
            
            ex = str(payload.get("exchange") or "").upper()
            # On prend le premier alias pour l'authentification (souvent SUB_TT ou SUB_TM)
            alias = str(payload.get("from_alias") or payload.get("to_alias") or "")
            if not ex or not alias: continue
            
            try:
                # Appel REST pour vérifier le statut réel
                new_status = await mbf.verify_transfer_status_rest(ex, alias, tid)
                if new_status == "SETTLED":
                    logger.info("[RiskManager] P0: REST Reconciled transfer %s -> SETTLED", tid)
                    self._transfer_controller.mark_settled(tid, payload=payload)
                elif new_status == "FAILED":
                    logger.warning("[RiskManager] P0: REST Reconciled transfer %s -> FAILED", tid)
                    self._transfer_controller.mark_failed(tid, payload=payload, error="reconciled_failed_rest")
            except Exception as e:
                logger.debug("[RiskManager] REST reconciliation failed for %s: %s", tid, e)

    async def _loop_balances(self):
        while self._running:
            try:
                try:
                    self._transfer_controller.check_timeouts()
                except Exception:
                    logger.exception("[RiskManager] transfer timeout check failed")
                
                # AJOUT P0: Réconciliation REST périodique
                try:
                    await self._reconcile_pending_transfers()
                except Exception:
                    logger.debug("[RiskManager] _reconcile_pending_transfers failed", exc_info=True)
                if bool(getattr(self.cfg, "dry_run", False)):
                    bals = self._virtual_balances.copy()
                    self._last_balances = bals
                    # pousser l'inventaire par ex/quote vers Prometheus
                    self._update_inventory_metrics()
                    # Rafraîchit la vue capital (buffers MBF) si MBF est disponible
                    try:
                        if getattr(self, "mbf", None) is not None:
                            self.update_capital_from_mbf()
                    except Exception:
                        logger.debug("[RiskManager] capital refresh from MBF failed (dry_run)", exc_info=False)
                    try:
                        self._refresh_mm_delta_from_balances()
                    except Exception:
                        logger.debug("[RiskManager] mm_delta refresh failed (dry_run)", exc_info=False)
                    try:
                        now = time.time()
                        self._refresh_tttm_exposure_state(now)
                    except Exception:
                        logger.debug("[RiskManager] tttm exposure refresh failed (dry_run)", exc_info=False)
                    try:
                        self._refresh_alias_collat_from_mbf(now=self.last_capital_update_ts)
                    except Exception:
                        self.logger.debug("[RM] _refresh_alias_collat_from_mbf failed", exc_info=False)


                else:
                    # BalanceFetcher multi-comptes
                    bals = await self.balance_fetcher.get_all_balances(force_refresh=True)
                    self._last_balances = bals
                    # pousser l'inventaire par ex/quote vers Prometheus
                    self._update_inventory_metrics()
                    # Rafraîchit la vue capital (buffers MBF) si MBF est disponible
                    try:
                        if getattr(self, "mbf", None) is not None:
                            self.update_capital_from_mbf()
                    except Exception:
                        logger.debug("[RiskManager] capital refresh from MBF failed (dry_run)", exc_info=False)
                    try:
                        self._refresh_mm_delta_from_balances()
                    except Exception:
                        logger.debug("[RiskManager] mm_delta refresh failed", exc_info=False)

                    try:
                        now = time.time()
                        self._refresh_tttm_exposure_state(now)
                    except Exception:
                        logger.debug("[RiskManager] tttm exposure refresh failed", exc_info=False)

                    # Publie les niveaux de tokens de fees si le collector expose l’API
                    try:
                        if self.slip_collector and hasattr(self.slip_collector, "update_fee_token_level"):
                            for ex, per_alias in (self._last_balances or {}).items():
                                # agrège BNB/MNT en unités (simple, sans valorisation)
                                agg = {}
                                for _alias, assets in (per_alias or {}).items():
                                    for token in ("BNB", "MNT"):
                                        agg[token] = agg.get(token, 0.0) + float((assets or {}).get(token, 0.0))
                                for tok, units in agg.items():
                                    self.slip_collector.update_fee_token_level(ex, tok, units)
                            # cibles % (si configurées)
                            targets = getattr(self.cfg, "fee_token_target_percent",
                                              {}) or {}  # ex: {"BINANCE":{"BNB":1.0}}
                            if hasattr(self.slip_collector, "set_fee_token_target_percent"):
                                for ex, m in targets.items():
                                    for tok, pct in (m or {}).items():
                                        self.slip_collector.set_fee_token_target_percent(ex, tok, float(pct))

                    except Exception:
                        logger.debug("[RiskManager] fee-token levels push failed", exc_info=False)
                        # Tick ladder capital (v1 — placeholders; signaux riches en 7-RM-2b)
                try:
                    self._run_capital_ladder_tick()
                except Exception:
                    logger.debug("[RiskManager] ladder_tick from balances loop failed", exc_info=False)
                try:
                    now_ts = time.time()
                    await self._mm_hedge_tick(now_ts)
                    await self._tt_hedge_tick(now_ts)
                except Exception:
                    logger.debug("[RiskManager] hedge ticks failed", exc_info=False)

                if self.rebalancing and hasattr(self.rebalancing, "update_balances"):
                    self.rebalancing.update_balances(bals)
                    self._mark_balances_observability(self._last_balances)

                self.last_update = time.time()
                self._mark_loop_success("balances")
            except Exception as e:
                logger.exception(f"[RiskManager] balances loop: {e}")
                self._mark_loop_error("balances", e)
            await asyncio.sleep(self.t_bal)


    async def _loop_rebalancing(self):
        while self._running:
            try:
                if not self.rebalancing:
                    await asyncio.sleep(self.t_rebal)
                    continue
                try:
                    await self._mm_rebalancing_step()
                except Exception:
                    logger.exception("[RiskManager] mm_rebalancing_step failed")

                imb = self.rebalancing.detect_imbalance()
                if imb:
                    plan = None
                    if hasattr(self.rebalancing, "build_plan"):
                        try:
                            plan = self.rebalancing.build_plan(imb)
                        except Exception:
                            plan = None
                    if hasattr(self.rebalancing, "push_history"):
                        try:
                            self.rebalancing.push_history(imb, plan or {})
                        except Exception:
                            logging.exception("Unhandled exception")

                    self._alert(
                        "RiskManager",
                        f"Rebalancing détecté: {imb}" + (f" | plan={plan}" if plan else ""),
                        alert_type="WARNING"
                    )

                    now = time.time()
                    self._rebalancing_until = max(self._rebalancing_until, now + self.rebal_active_ttl_s)

                    ops: List[Dict[str, Any]] = []
                    try:
                        if plan is not None and hasattr(self.rebalancing, "plan_to_operations"):
                            ops = list(self.rebalancing.plan_to_operations(plan) or [])
                        else:
                            ops = list(self._make_rebalancing_actions(plan or {}))
                    except Exception:
                        logger.exception("[RiskManager] plan_to_operations failed, fallback to legacy")
                        ops = list(self._make_rebalancing_actions(plan or {}))

                    if now >= self._rebal_emit_next_allowed:
                        for op in ops:
                            try:
                                await self._handle_rebalancing_op(op)
                            except Exception:
                                logger.exception("[RiskManager] handle rebal op failed")
                        self._rebal_emit_next_allowed = now + self.rebal_emit_cooldown_s

                self.last_update = time.time()
                self._mark_loop_success("rebalancing")
            except Exception as e:
                logger.exception(f"[RiskManager] rebalancing loop: {e}")
                self._mark_loop_error("rebalancing", e)
            await asyncio.sleep(self.t_rebal)

    async def _mm_rebalancing_step(self) -> None:
        if not bool(getattr(self, "enable_mm", False)):
            return

        balances = self._balances_snapshot()
        now = time.time()
        status_cache = getattr(self, "_alias_balance_status", {}) or {}

        # 1. Groupement des drifts par (alias, asset) pour une vision globale (notamment pour Cross-CEX)
        all_drifts = {}

        for ex, per_alias in (balances or {}).items():
            exu = str(ex).upper()
            for alias, assets in (per_alias or {}).items():
                if not self._is_mm_alias(exu, alias):
                    continue
                
                if status_cache.get((exu, alias)) != "OK":
                    continue

                for asset in (assets or {}).keys():
                    asset_u = str(asset).upper()
                    drift_usd, drift_pct = self._mm_compute_inventory_drift(exu, alias, asset_u, balances)

                    # Métriques drift
                    try:
                        from modules.obs_metrics import MM_INVENTORY_DRIFT_USD
                        if MM_INVENTORY_DRIFT_USD:
                            MM_INVENTORY_DRIFT_USD.labels(asset=asset_u).set(drift_usd)
                    except Exception:
                        pass

                    key = (alias, asset_u)
                    if key not in all_drifts:
                        all_drifts[key] = []
                    all_drifts[key].append({
                        "ex": exu,
                        "drift_usd": drift_usd,
                        "drift_pct": drift_pct
                    })

        # 2. Traitement par stratégie et asset
        for (alias, asset), drifts in all_drifts.items():
            is_cross = (str(alias).upper() == self.mm_cross_alias_name)
            
            if not is_cross:
                # MM_MONO ou MM par défaut : gestion indépendante par exchange
                for d in drifts:
                    ex = d["ex"]
                    drift_usd = d["drift_usd"]
                    drift_pct = d["drift_pct"]
                    state = self._mm_classify_inventory_state(drift_usd, drift_pct, alias_mm=alias)
                    
                    self.mm_reb_state[(ex, alias, asset)] = {
                        "state": state,
                        "drift_usd": drift_usd,
                        "drift_pct": drift_pct,
                        "last_update_ts": now,
                    }
                    await self._mm_dispatch_actions_for_state(ex, alias, asset, state, drift_usd)
            else:
                # MM_CROSS : gestion intelligente du delta global
                total_drift_usd = sum(d["drift_usd"] for d in drifts)
                global_state = self._mm_classify_inventory_state(total_drift_usd, 0.0, alias_mm=alias)
                
                # Enregistrement de l'état global pour le monitoring
                for d in drifts:
                    self.mm_reb_state[(d["ex"], alias, asset)] = {
                        "state": global_state,
                        "drift_usd": d["drift_usd"],
                        "global_drift_usd": total_drift_usd,
                        "last_update_ts": now,
                    }

                if global_state in ["TENSION", "CRITICAL"]:
                    # Le delta global est déséquilibré -> on réduit la jambe la plus lourde via un trade simple (Self-rebal)
                    worst = max(drifts, key=lambda x: abs(x["drift_usd"]))
                    logger.info(f"[RiskManager] MM_CROSS global drift {global_state} for {asset}: {total_drift_usd:.1f}$ -> reducing on {worst['ex']}")
                    # On force une action simple pour réduire le delta global
                    self._mm_trigger_self_rebal(worst["ex"], alias, asset, total_drift_usd)
                else:
                    # Delta global OK. On vérifie s'il y a des déséquilibres locaux (offsetting)
                    for d in drifts:
                        ex = d["ex"]
                        local_drift = d["drift_usd"]
                        local_state = self._mm_classify_inventory_state(local_drift, 0.0, alias_mm=alias)
                        
                        if local_state in ["TENSION", "CRITICAL"]:
                            # On a un déséquilibre interne (ex: long sur A, short sur B).
                            # On utilise le Cross-CEX rebal (trade d'arbitrage ou transfert)
                            logger.info(f"[RiskManager] MM_CROSS local imbalance on {ex}:{asset}: {local_drift:.1f}$ -> cleaning up")
                            await self._mm_dispatch_actions_for_state(ex, alias, asset, local_state, local_drift)

    async def _mm_dispatch_actions_for_state(self, ex: str, alias_mm: str, asset: str, state: str,
                                             drift_usd: float) -> None:
        st = str(state or "").upper()
        if st == "NORMAL":
            return

        status_cache = getattr(self, "_alias_balance_status", {}) or {}
        if status_cache.get((ex, alias_mm)) != "OK":
            return

        is_mono = (str(alias_mm).upper() == self.mm_mono_alias_name)

        if st == "ALERT":
            self._mm_trigger_self_rebal(ex, alias_mm, asset, drift_usd)
        elif st == "TENSION":
            await self._mm_plan_intra_cex_transfers(ex, alias_mm, asset, drift_usd)
        elif st == "CRITICAL":
            if is_mono:
                # Pour Mono-CEX, on évite le cross-cex, on fait un intra-cex aggressif ou on renforce le self-rebal
                logger.info(f"[RiskManager] MM_MONO Critical drift on {ex}:{asset} ({drift_usd:.1f}$) -> using intra-cex rebalancing")
                await self._mm_plan_intra_cex_transfers(ex, alias_mm, asset, drift_usd)
            else:
                await self._mm_trigger_cross_cex_reb(ex, alias_mm, asset, drift_usd)

    def _mm_trigger_self_rebal(self, ex: str, alias_mm: str, asset: str, drift_usd: float) -> None:
        if not getattr(self, "mm_inventory_enabled", False):
            return

        pair_key = self._norm_pair(f"{asset}USDT")
        opp = {
            "pair": pair_key,
            "buy_ex": ex,
            "sell_ex": ex,
            "alias": alias_mm,
            "notional_usdc": abs(float(drift_usd)),
        }
        self._maybe_fire_mm_inventory_single(opp, reason="mm_reb_alert", branch=alias_mm)

    async def _mm_plan_intra_cex_transfers(self, ex: str, alias_mm: str, asset: str, drift_usd: float) -> None:
        if self.rebalancing is None:
            return

        now = time.time()
        if now < getattr(self, "_rebal_emit_next_allowed", 0.0):
            return

        imbalance = {
            "CRYPTO": {ex: {alias_mm: {asset: drift_usd}}},
            "CASH": {},
            "OVERLAY": {},
        }

        try:
            plan = self.rebalancing.build_plan(imbalance)
        except Exception:
            logger.exception("[RiskManager] mm_reb build_plan failed")
            return

        try:
            ops = list(self.rebalancing.plan_to_operations(plan) or [])
        except Exception:
            logger.exception("[RiskManager] mm_reb plan_to_operations failed")
            return

        allowed_types = {"internal_subaccount_transfer", "internal_wallet_transfer"}
        ops = [op for op in ops if (op or {}).get("type") in allowed_types]
        if not ops:
            return

        for op in ops:
            try:
                await self._handle_rebalancing_op(op)
            except Exception:
                logger.exception("[RiskManager] mm_reb handle op failed")

        self._rebal_emit_next_allowed = now + self.rebal_emit_cooldown_s

    def _mm_pick_reb_counterparty_exchange(self, ex_mm: str, asset: str) -> Optional[str]:
        # P0 : sélection naive, à raffiner (capital dispo, slippage, etc.)
        candidates = ["BINANCE", "BYBIT", "COINBASE"]
        exu = str(ex_mm).upper()
        for cand in candidates:
            if cand != exu:
                return cand
        return None

    async def _mm_trigger_cross_cex_reb(self, ex: str, alias_mm: str, asset: str, drift_usd: float) -> None:
        counterparty = self._mm_pick_reb_counterparty_exchange(ex, asset)
        if not counterparty:
            return

        pair_key = self._norm_pair(f"{asset}USDT")
        try:
            bid, ask = self.get_top_of_book(ex, pair_key, enforce_fresh=False)
        except Exception:
            bid, ask = 0.0, 0.0
        mid = (float(bid) + float(ask)) / 2.0 if (bid and ask) else 0.0
        if mid <= 0:
            return

        qty = abs(float(drift_usd)) / mid
        if qty <= 0:
            return

        if drift_usd > 0:
            buy_ex, sell_ex = counterparty, ex
        else:
            buy_ex, sell_ex = ex, counterparty

        opp = {
            "pair": pair_key,
            "buy_ex": buy_ex,
            "sell_ex": sell_ex,
            "qty": qty,
            "notional_usdc": abs(float(drift_usd)),
            "meta": {
                "branch": "REB",
                "strategy": "REB",
                "type": "rebalancing",
                "source": "MM_REB_CRITICAL",
                "allow_final_loss_bps": float(self.mm_reb_allow_loss_bps),
                "allow_loss_bps": float(self.mm_reb_allow_loss_bps),
            },
        }

        bundle = self._build_bundle(opp, strategy="REB")
        if not bundle:
            return

        meta = bundle.setdefault("meta", {}) or {}
        meta.setdefault("source", "MM_REB_CRITICAL")
        meta.setdefault("type", "rebalancing")
        meta.setdefault("branch", "REB")
        meta["allow_final_loss_bps"] = float(self.mm_reb_allow_loss_bps)
        meta["allow_loss_bps"] = float(self.mm_reb_allow_loss_bps)

        try:
            await self.engine.execute(bundle)
        except Exception:
            logger.exception("[RiskManager] mm_reb cross-cex execution failed")



    async def _loop_volatility(self):
        """
        Boucle volatilité centralisée (passive) :
        - Lit les métriques & seuils depuis le VolatilityMonitor (router-driven)
        - Demande au VolatilityManager des ajustements (step)
        - Publie les métriques vol_* (ewma/p95/band) via obs_metrics (centralisé RM)
        - Met à jour l’état paused par pair (modere/eleve)
        """
        # état local pour éviter d'incrémenter le counter à chaque tour
        if not hasattr(self, "_last_band"):
            self._last_band = {}

        try:
            from modules.obs_metrics import VOL_EWMA_BPS, VOL_P95_BPS, VOL_BAND_TOTAL
        except Exception:
            VOL_EWMA_BPS = VOL_P95_BPS = VOL_BAND_TOTAL = None  # no-op si obs indispo

        while self._running:
            try:
                vol_getter = getattr(self, "vol_monitor", None)

                for pair in self.symbols:
                    # --- SAFE: vol_monitor peut être None ---
                    if vol_getter is not None:
                        metrics = vol_getter.get_current_metrics(pair) or {}
                        thresholds = vol_getter.get_current_thresholds(pair) or {}
                        sig = metrics.get("prudence_signal") or vol_getter.get_prudence_signal(pair)
                    else:
                        metrics = {}
                        thresholds = {}
                        sig = "normal"

                    # 2) Manager (ajustements non bloquants)
                    if self.vol_manager and hasattr(self.vol_manager, "step"):
                        adj = self.vol_manager.step(pair, metrics, thresholds)
                        await self._apply_adjustments(pair, adj)

                    # 3) Publication des métriques consolidées (depuis le Manager)
                    vm = None
                    try:
                        if self.vol_manager and hasattr(self.vol_manager, "get_current_metrics"):
                            vm = self.vol_manager.get_current_metrics(pair) or {}
                    except Exception:
                        vm = {}

                    if vm:
                        if VOL_EWMA_BPS:
                            try:
                                VOL_EWMA_BPS.labels(vm.get("pair", pair)).set(float(vm.get("ewma_vol_bps", 0.0)))
                            except Exception:
                                pass
                        if VOL_P95_BPS:
                            try:
                                VOL_P95_BPS.labels(vm.get("pair", pair)).set(float(vm.get("p95_vol_bps", 0.0)))
                            except Exception:
                                pass
                        # band transitions (counter) — incrément seulement si changement
                        band = str(vm.get("band", "normal")).lower()
                        last = self._last_band.get(pair)
                        if band and band != last and VOL_BAND_TOTAL:
                            try:
                                VOL_BAND_TOTAL.labels(band).inc()
                            except Exception:
                                pass
                        self._last_band[pair] = band

                    # 4) Pause pair en prudence élevée/modérée (affecte scanning/decision)
                    sig = "NORMAL"
                    if self.vol_monitor and hasattr(self.vol_monitor, "get_prudence_signal"):
                        sig = metrics.get("prudence_signal") or self.vol_monitor.get_prudence_signal(pair)
                    elif metrics.get("prudence_signal"):
                        sig = metrics["prudence_signal"]

                    self._paused[pair] = (str(sig).lower() in ("modéré", "modere", "élevé", "eleve"))

                set_rm_paused_count(sum(1 for v in self._paused.values() if v))
                self.last_update = time.time()
                self._mark_loop_success("volatility")

            except Exception as e:
                logger.exception(f"[RiskManager] volatility loop: {e}")
                self._mark_loop_error("volatility", e)
            await asyncio.sleep(self.t_vol)

    async def _loop_mode(self) -> None:
        interval_s = float(getattr(self, "t_mode", 1.0) or 1.0)
        if interval_s <= 0.0:
            interval_s = 1.0
        while self._running:
            try:
                self._tick_mode()
            except Exception:
                logger.exception("[RiskManager] _tick_mode failed", exc_info=True)
            await asyncio.sleep(interval_s)
    # ===================== Revalidation & Profitability API =====================
    def _apply_mode_overrides(self) -> None:
        """
        Applique l'overlay du mode courant (sans jamais dépasser les caps du profil).
        - Ajuste min_bps TT (base_min_bps) et min_bps TM (tm_min_required_bps)
        - Recalcule les caps notionnels TT/TM/MM pour USDC/USDT/EUR (profile-aware × cap_factor)
        - Active/désactive MM et communique un hint IOC-only au moteur si nécessaire

        Scénarios de validation (Ticket 11.C):
          • NORMAL: trade_mode=NORMAL, caps profil standard, ioc_only=False, mm_enabled=True.
          • CONSTRAINED: pacer en CONSTRAINED → caps clampés (downscale), mm_enabled=True, ioc_only=False.
          • SEVERE: PnL-guard ou pacer SEVERE → caps réduits, ioc_only=True, mm_enabled=False.
          • OPPORTUNISTE: rm_mode opportuniste & pacer NORMAL → min_bps ajustés selon sous-mode, caps ≤ profil, mm_enabled policy.
        Côté Engine, TIF doit respecter mode_overrides["ioc_only"] et les jambes maker ne sont créées que si mode_overrides["mm_enabled"] est True.
                    Structure exposée dans bundle.meta.mode_overrides (snapshot RM "brut") :

              {
                  "ioc_only": bool,
                  "mm_enabled": bool,
                  "rm_mode": rm_mode courant (NORMAL / OPP_VOLUME / OPP_VOL / SEVERE),
                  "trade_mode": trade_mode courant (NORMAL / CONSTRAINED / SEVERE / OPPORTUNISTE),
                  "stage": "rm_raw",
                  # optionnel : "submode" pour distinguer OPP_VOLUME vs OPP_VOL
              }

            L'Engine lira cette structure, appliquera les flags du PACER (mm_frozen,
            ioc_only infra) et pourra écrire une vue fusionnée avec stage="engine_fused"
            à des fins d'observabilité.

        """
        # Profil capital (fallback safe)
        prof = getattr(self, "capital_profile", None) \
               or getattr(getattr(self, "pacer", None), "_profile", None) \
               or "NANO"
        trade_ov = dict(self._overlay_by_trade_mode.get(self.trade_mode, {}) or {})
        if self.trade_mode == "OPPORTUNISTE" and self.rm_mode in self._overlay:
            trade_ov.update(self._overlay.get(self.rm_mode, {}))
        elif self.trade_mode == "SEVERE" and not trade_ov:
            trade_ov = dict(self._overlay.get("SEVERE", {}))

        # 1) Min bps dynamiques
        rm_cfg = getattr(self.cfg, "rm", None)
        base_tt = float(getattr(self, "base_min_bps", getattr(rm_cfg, "base_min_bps", 6.5)))
        delta_tt = float(trade_ov.get("tt_min_bps_delta", 0.0))
        self.base_min_bps = max(0.0, base_tt + delta_tt)

        base_tm = float(getattr(self, "tm_min_required_bps", getattr(self.cfg, "tm_min_required_bps_base", 11.0)))
        delta_tm = float(trade_ov.get("tm_min_bps_delta", 0.0))
        self.tm_min_required_bps = max(0.0, base_tm + delta_tm)

        # 2) Caps notionnels profile-aware × cap_factor (down-clamp only)
        cap_eff = float(self._profile_cap_notional(profile=prof))  # méthode existante
        factor = min(1.0, float(trade_ov.get("cap_factor", 1.0)))

        def _copy_caps(src: dict) -> dict:
            return {k: dict(v) for k, v in (src or {}).items()}

        base_caps_src = getattr(self, "_per_strategy_notional_cap_base", None)
        if base_caps_src is None:
            self._per_strategy_notional_cap_base = _copy_caps(getattr(self, "per_strategy_notional_cap", {}))
            base_caps_src = self._per_strategy_notional_cap_base

        caps = _copy_caps(base_caps_src)  # {'TT': {'USDC': ...}, ...}
        for strat in ("TT", "TM", "MM"):
            for q in ("USDC", "USDT", "EUR"):
                cur = float(((caps.get(strat) or {}).get(q) or 0.0) or 0.0)
                target = min(cur or cap_eff, cap_eff) * factor  # JAMAIS d'upscale > cap_eff
                caps.setdefault(strat, {})[q] = max(10.0, float(target))
        self.per_strategy_notional_cap = caps

        # 3) MM & IOC-only
        mm_override = trade_ov.get("mm_enable")
        if mm_override is None:
            self.enable_mm = bool(getattr(self, "enable_mm", False))
        else:
            self.enable_mm = bool(mm_override)
        if not self.enable_mm:
            try:
                # best-effort : adapte la venue si besoin
                asyncio.create_task(
                    self._cancel_open_mm_quotes_on_exchange(
                        "BINANCE", reason=f"rm_overlay:{self.rm_mode}"
                    )
                )
            except Exception:
                pass

        # Hint pour l'Engine (TM en IOC si nécessaire)
        # Hint pour l'Engine (TM en IOC si nécessaire)
        self._ioc_only = bool(trade_ov.get("ioc_only", False))
        self._current_mode_overrides = {
            "ioc_only": self._ioc_only,
            "mm_enabled": bool(self.enable_mm),
            "rm_mode": str(self.rm_mode or "NORMAL").upper(),
            "trade_mode": str(self.trade_mode or "NORMAL").upper(),
            "stage": "rm_raw",
        }
        # --- Down-clamp only avec policy PACER (flags hold-time) ---
        try:
            pol = dict(getattr(self, "_last_engine_pacer_policy", {}) or {})
            flags = pol.get("flags") or {}
            if isinstance(flags, dict):
                # PACER ne peut que SERRER
                if bool(flags.get("mm_frozen")):
                    self.enable_mm = False
                    self._current_mode_overrides["mm_enabled"] = False
                    self._current_mode_overrides["pacer_mm_frozen"] = True
                if bool(flags.get("ioc_only")):
                    self._ioc_only = True
                    self._current_mode_overrides["ioc_only"] = True
                    self._current_mode_overrides["pacer_ioc_only"] = True
        except Exception:
            pass

        if self.trade_mode == "OPPORTUNISTE" and self.rm_mode in ("OPP_VOLUME", "OPP_VOL"):
            self._current_mode_overrides["submode"] = self.rm_mode

        self._last_applied_trade_mode = self.trade_mode

    def _latest_book_age_s(self) -> float:
        """
        Âge réel du dernier orderbook reçu côté RM/Router.
        Essaie plusieurs sources; fallback raisonnable si indispo.
        """
        import time
        now_ms = int(time.time() * 1000)
        # Essais de timestamps (mets ici les champs que tu exposes côté Router/Hub)
        ts_ms = (
                getattr(self, "last_orderbook_recv_ts_ms", None)
                or getattr(self, "router_last_ob_recv_ts_ms", None)
                or getattr(self, "books_last_recv_ts_ms", None)
        )
        if ts_ms is not None:
            try:
                return max(0.0, (now_ms - int(ts_ms)) / 1000.0)
            except Exception:
                pass
        # Fallback "pire cas" : on renvoie un âge élevé pour forcer prudence
        return float(self._switch_knobs.opp_age_fallback_s)

    def _latest_book_age_s_with_missing(self) -> tuple[float, bool]:
        import time
        now_ms = int(time.time() * 1000)
        ts_ms = (
                getattr(self, "last_orderbook_recv_ts_ms", None)
                or getattr(self, "router_last_ob_recv_ts_ms", None)
                or getattr(self, "books_last_recv_ts_ms", None)
        )
        if ts_ms is None:
            return float(self._switch_knobs.opp_age_fallback_s), True
        try:
            return max(0.0, (now_ms - int(ts_ms)) / 1000.0), False
        except Exception:
            return float(self._switch_knobs.opp_age_fallback_s), True

    def _signal_missing(self, name: str) -> None:
        reason = normalize_reason_code(ReasonCodes.signal_missing(name)) or ReasonCodes.signal_missing(name)
        try:
            inc_rm_reject(reason=reason)
        except Exception:
            pass

    def _green_calme(self) -> bool:
        """
        Vrai si le marché est "très favorable" pour OPP_VOLUME.
        Conditions (toutes vraies) :
          - books frais (age ≤ seuil),
          - vol_p95_bps ≤ seuil,
          - rate-limits OK (429 bas),
          - simulateur aligné (shadow_error_bps_p50 ≤ seuil),
          - pacer en NORMAL.
        """
        policy = self._signal_policy
        # 1) Fraîcheur du book
        age_s, age_missing = self._latest_book_age_s_with_missing()
        if age_missing and bool(getattr(policy, "require_book_age_for_opp", True)):
            self._signal_missing("BOOK_AGE")
            return False
        age_ok = bool(age_s <= float(self._switch_knobs.opp_vol_slip_age_s_max))

        # 2) Volatilité p95 (bps)
        try:
            vm = self.vol_manager.get_current_metrics(None) if hasattr(self, "vol_manager") else None
            vol_p95 = None if not vm else (vm or {}).get("p95_bps")
        except Exception:
            vol_p95 = None
            if vol_p95 is None and bool(getattr(policy, "require_vol_signal_for_opp", True)):
                self._signal_missing("VOL_P95")
                return False
            vol_ok = bool(float(vol_p95 or 0.0) <= float(self._switch_knobs.opp_vol_p95_bps_max))

        # 3) Rate-limits / 429
        rl_health = getattr(self, "rate_limits_healthy", None)
        if rl_health is None and bool(getattr(policy, "require_rl_health_for_opp", True)):
            self._signal_missing("RATE_LIMITS")
            return False
        rl_ok = bool(rl_health)

        # 4) Simulateur (shadow)
        try:
            shadow_p50 = getattr(self, "shadow_error_bps_p50", None)
        except Exception:
            shadow_p50 = None
            if shadow_p50 is None and bool(getattr(policy, "require_shadow_for_opp", True)):
                self._signal_missing("SHADOW_P50")
                return False
            shadow_ok = bool(float(shadow_p50 or 0.0) <= float(self._switch_knobs.opp_shadow_p50_bps_max))

        # 5) Pacer
        pacer_ok = (str(getattr(self, "pacer_mode", "NORMAL")).upper() == "NORMAL")

        return all((age_ok, vol_ok, rl_ok, shadow_ok, pacer_ok))

    def _red_tempete(self) -> bool:
        """
        Vrai si le marché est "dégradé" (activer OPP_VOL).
        Conditions (au moins une vraie) :
          - vol_p95_bps ≥ seuil_min,
          - books trop vieux (age > seuil),
          - private WS/acks dégradé si exposé.
        """
        vol_bad = False
        stale_bad = False
        pws_bad = False

        policy = self._signal_policy
        age_s, age_missing = self._latest_book_age_s_with_missing()
        
        # Warmup grace period (e.g. 5s) to avoid TEMPETE at boot
        import time
        is_warmup = bool((time.time() - getattr(self, "_boot_ts", 0)) < 5.0)
        
        stale_bad = bool(age_s > float(self._switch_knobs.opp_vol_exit_slip_age_s_max))
        if age_missing and bool(getattr(policy, "require_book_age_for_opp", True)):
            if not is_warmup:
                self._signal_missing("BOOK_AGE")
                stale_bad = True
            else:
                stale_bad = False
        
        vol_bad = False
        try:
            vm = self.vol_manager.get_current_metrics(None) if hasattr(self, "vol_manager") else None
            vol_p95 = None if not vm else (vm or {}).get("p95_bps")
        except Exception:
            vol_p95 = None

        if vol_p95 is None and bool(getattr(policy, "require_vol_signal_for_opp", True)):
            self._signal_missing("VOL_P95")
            vol_bad = True
        else:
            vol_bad = bool(float(vol_p95 or 0.0) >= float(self._switch_knobs.oppvol_p95_bps_min))

        pws_health = getattr(self, "private_ws_healthy", None)
        # En DRY_RUN, on est souvent moins strict sur le PWS s'il n'est pas explicitement requis
        is_live = self._rm_live_armed()
        pws_required = bool(getattr(policy, "require_pws_health_for_opp", is_live))
        
        if pws_health is None and pws_required:
            self._signal_missing("PWS_HEALTH")
            pws_bad = True
        elif pws_health is not None:
            pws_bad = not bool(pws_health)
        else:
            pws_bad = False

        if vol_bad or stale_bad or pws_bad:
            # Log détaillé pour diagnostic (Macro 7-Diagnostic)
            if self.rm_mode == "NORMAL":
                try:
                    # Signal details in tags for metrics if possible
                    self.obs_inc("rm_tempete_trigger_total", 
                                 stale=str(stale_bad), 
                                 vol=str(vol_bad), 
                                 pws=str(pws_bad),
                                 is_live=str(is_live))

                    logging.getLogger(__name__).info(
                        "[RM][TEMPETE] Trigger details: stale=%s(age=%.3fs) vol=%s(p95=%s) pws=%s(health=%s) is_live=%s",
                        stale_bad, age_s, vol_bad, vol_p95, pws_bad, pws_health, is_live
                    )
                except Exception:
                    pass
            return True

        return False

    def set_pacer_mode(self, mode: str, *, source: str = "engine_pacer") -> None:
        """
        Bridge explicite PACER -> RM (Macro 5 / Macro 7-D).

        - Appelé par l'ExecutionEngine / EnginePacer pour pousser un pacer_mode canonique.
        - N'accepte que: "NORMAL", "CONSTRAINED", "SEVERE" (tout le reste est ramené à "NORMAL").
        - Ne déclenche PAS la FSM complète ici : _update_trade_mode() reste le point
          unique de consolidation rm_mode × pacer_mode, appelé dans le tick ou juste
          avant l'émission d'un bundle.

        Ce contrat évite les setattr sauvages sur le RM et documente clairement le flux
        PACER → RM.
        """
        try:
            raw = str(mode or "NORMAL").upper()
        except Exception:
            raw = "NORMAL"

        if raw not in ("NORMAL", "CONSTRAINED", "SEVERE"):
            raw = "NORMAL"

        # Valeur précédente pour l'observabilité
        try:
            prev = str(getattr(self, "pacer_mode", "NORMAL") or "NORMAL").upper()
        except Exception:
            prev = "NORMAL"

        # Mise à jour du pacer_mode consommé par _compute_trade_mode()
        self.pacer_mode = raw

        # Macro 7-D : observabilité des changements de mode Pacer
        if prev != raw:
            try:
                logging.getLogger(__name__).info(
                    "[RM][MODE] pacer_mode_transition %s->%s source=%s",
                    prev,
                    raw,
                    str(source or "engine_pacer"),
                )
            except Exception:
                pass

            try:
                if hasattr(self, "obs_inc"):
                    self.obs_inc(
                        "rm_mode_transitions_total",
                        kind="pacer_mode",
                        old=prev,
                        new=raw,
                    )
            except Exception:
                pass
            try:
                if hasattr(self, "obs_inc"):
                    self.obs_inc(
                        "rm_pacer_mode_changes_total",
                        prev=prev,
                        new=raw,
                        source=str(source or "engine_pacer"),
                    )
            except Exception:
                # L'observabilité ne doit jamais casser le flux RM
                pass

        # Best-effort de traçabilité pour le debug/obs
        try:
            import time
            self._last_pacer_mode_ts = float(time.time())
            self._last_pacer_mode_source = str(source or "engine_pacer")
        except Exception:
            pass

    def set_engine_metrics(self, metrics: dict, *, source: str = "engine") -> None:
        """
        Snapshot des métriques de l'Engine (latence, RPS, etc.).
        Utilisé pour les gates de performance MM.
        """
        try:
            if not isinstance(metrics, dict):
                return
            self._last_engine_metrics = dict(metrics)
            import time
            self._last_engine_metrics_ts = float(time.time())

            # Mise à jour des gauges d'observabilité demandées
            ack_p95 = float(metrics.get("p95_ack_ms") or 0.0)
            if hasattr(self, "obs_set"):
                self.obs_set("mm_ack_p95_ms", ack_p95)

            # Logique de Gate 2 (Latency/Ack) : calcul du niveau de pénalité
            soft_thresh = float(getattr(self, "mm_ack_p95_ms_soft", 40.0))
            hard_thresh = float(getattr(self, "mm_ack_p95_ms_hard", 100.0))

            penalty_level = 0  # none
            if ack_p95 >= hard_thresh:
                penalty_level = 2  # hard
                # Hard stop via mm_enabled dans mode_overrides
                if hasattr(self, "_current_mode_overrides"):
                    self._current_mode_overrides["mm_enabled"] = False
                    self.enable_mm = False
            elif ack_p95 >= soft_thresh:
                penalty_level = 1  # soft

            if hasattr(self, "obs_set"):
                self.obs_set("mm_latency_penalty_state", penalty_level)

            self._mm_latency_penalty_level = penalty_level

        except Exception:
            pass

    def set_engine_pacer_policy(self, policy: dict, *, source: str = "engine_pacer") -> None:
        """
        Snapshot best-effort de la policy du PACER (incluant flags hold-time).
        Sert uniquement à down-clamp les mode_overrides (IOC_ONLY / MM freeze).
        """
        if not hasattr(self, "_clock_offset_ms"):
            # P0: Forcer une tentative de recalage au premier contact avec le Pacer
            # Cela aide à synchroniser les horloges si le Pacer est à l'heure réelle.
            try:
                import time
                now = time.time()
                p_ts = policy.get("ts", now)
                diff = p_ts - now
                if abs(diff) > 1.0:
                    self._clock_offset_ms = diff * 1000.0
                    logging.getLogger(__name__).info("[RM] Calibration temporelle via Pacer: Offset=%dms", self._clock_offset_ms)
            except Exception: pass

        try:
            if not isinstance(policy, dict):
                return
            self._last_engine_pacer_policy = dict(policy)
            try:
                import time
                self._last_engine_pacer_policy_ts = float(time.time())
                self._last_engine_pacer_policy_source = str(source or "engine_pacer")
            except Exception:
                pass
        except Exception:
            pass

    def _compute_trade_mode(self) -> str:
        """
        Consolidation rm_mode × pacer_mode en mode unique exposé à l'Engine.

        Contrat Macro 5 (RM = owner métier, Pacer = overlay infra) :

          rm_mode      pacer_mode     → trade_mode
          ----------------------------------------
          SEVERE       *              → SEVERE
          *            SEVERE         → SEVERE
          CONSTRAINED  *              → CONSTRAINED
          *            CONSTRAINED    → CONSTRAINED
          OPP_VOLUME   NORMAL         → OPPORTUNISTE
          OPP_VOL      NORMAL         → OPPORTUNISTE
          (autres)     NORMAL         → NORMAL

        - rm_mode est piloté par PnL-guard, incidents TTL/WS/fees, OPP_VOLUME/OPP_VOL.
        - pacer_mode est injecté depuis l'EnginePacer (NORMAL / CONSTRAINED / SEVERE).
        - trade_mode ne peut jamais être plus "ouvert" que ce que dictent rm_mode ou pacer_mode.
        """
        rm_mode = str(getattr(self, "rm_mode", "NORMAL")).upper()
        pacer_mode = str(getattr(self, "pacer_mode", "NORMAL")).upper()

        if rm_mode == "SEVERE" or pacer_mode == "SEVERE":
            return "SEVERE"
        if rm_mode == "CONSTRAINED" or pacer_mode == "CONSTRAINED":
            return "CONSTRAINED"
        if pacer_mode == "NORMAL" and rm_mode in ("OPP_VOLUME", "OPP_VOL"):
            return "OPPORTUNISTE"
        return "NORMAL"

    @property
    def private_plane_state(self) -> str:
        """
        Vue simplifiée de la santé du plan privé.

        - GREEN        : rm_mode in {NORMAL, OPP_VOLUME, OPP_VOL}
        - CONSTRAINED  : rm_mode == CONSTRAINED
        - SEVERE       : rm_mode == SEVERE
        """
        rm_mode = str(getattr(self, "rm_mode", "NORMAL")).upper()
        if rm_mode == "SEVERE":
            return "SEVERE"
        if rm_mode == "CONSTRAINED":
            return "CONSTRAINED"
        return "GREEN"

    def _check_private_public_mode_invariant(self) -> None:
        """
        Invariant ZERO_PUBLIC_GREEN_PRIVATE_ON_FIRE (runtime, best-effort).

        - Si le plan privé est en alerte (rm_mode CONSTRAINED/SEVERE),
          le plan public (trade_mode) ne doit PAS être NORMAL / OPPORTUNISTE.
        """
        rm_mode = str(getattr(self, "rm_mode", "NORMAL")).upper()
        trade_mode = str(getattr(self, "trade_mode", "NORMAL")).upper()

        if rm_mode in ("CONSTRAINED", "SEVERE") and trade_mode in ("NORMAL", "OPPORTUNISTE"):
            msg = (
                f"[RM][INVARIANT] private_public_mode_violation "
                f"rm_mode={rm_mode} trade_mode={trade_mode}"
            )

            try:
                import logging

                logging.getLogger(__name__).error(msg)
            except Exception:
                pass

            try:
                if hasattr(self, "obs_inc"):
                    self.obs_inc(
                        "rm_invariant_private_public_mode_violations_total",
                        rm_mode=rm_mode,
                        trade_mode=trade_mode,
                    )
            except Exception:
                pass

            try:
                strict = bool(self._switch_knobs.rm_invariant_strict)
            except Exception:
                strict = False

            if strict:
                from contracts.errors import InconsistentStateError

                raise InconsistentStateError(msg)

    def _evaluate_incident_triggers(self) -> tuple[Optional[str], Optional[str]]:
        """
        Analyse les signaux incident (TTL balances, WS, fees) pour forcer
        rm_mode vers CONSTRAINED/SEVERE.

        Retourne un tuple (mode_cible | None, raison) pour documenter l'escalade.
        Cette version applique les règles suivantes (Ticket 14):
          - TTL balances / WS: on ne regarde que les alias "critiques".
          - Reconciler miss rate et latences WS: escalade globale.
          - Fee tokens "LOW" : n'escalade que si l'état dégradé est prolongé.
        """
        target_mode: Optional[str] = None
        target_reason: Optional[str] = None
        rank = {"NORMAL": 0, "CONSTRAINED": 1, "SEVERE": 2}
        now = time.time()

        # --- 1) TTL balances (MBF → RM) -----------------------------------
        # DEGRADED -> CONSTRAINED, BLOCKED -> SEVERE, mais uniquement sur les
        # alias jugés "critiques" par _is_critical_alias().
        try:
            self._refresh_balances_ttl_cache()
        except Exception:
            # On ne casse jamais la boucle de mode sur une erreur MBF.
            pass

        ttl_rank = {"BLOCKED": 2, "DEGRADED": 1}
        worst_ttl = 0
        ttl_cache = getattr(self, "_alias_balance_status", {}) or {}
        for (ex, alias), status in ttl_cache.items():
            try:
                if not self._is_critical_alias(ex, alias):
                    continue
            except Exception:
                # En cas de bug de config/parse, on ne bloque pas le mode.
                continue
            worst_ttl = max(worst_ttl, ttl_rank.get(status, 0))

        if worst_ttl == 2:
            target_mode, target_reason = "SEVERE", "ttl_blocked"
        elif worst_ttl == 1 and rank.get(target_mode or "NORMAL", 0) < 1:
            target_mode, target_reason = "CONSTRAINED", "ttl_degraded"

        # --- 2) Statut comptes WS (Hub + Reconciler) ----------------------
        # Si un alias critique est en statut à risque, on force au minimum
        # CONSTRAINED, mais on laisse les autres signaux prendre la main
        # pour aller jusqu'à SEVERE.
        ws_cache = getattr(self, "_alias_ws_accounts_status", {}) or {}
        for (ex, alias), meta in ws_cache.items():
            try:
                if not self._is_critical_alias(ex, alias):
                    continue
            except Exception:
                continue

            hub = str((meta or {}).get("hub_status") or "").upper()
            reco = str((meta or {}).get("reco_status") or "").upper()
            capital_at_risk = bool((meta or {}).get("capital_at_risk"))

            if capital_at_risk or hub not in ("", "OK") or reco not in ("", "OK"):
                if rank.get(target_mode or "NORMAL", 0) < 1:
                    target_mode, target_reason = "CONSTRAINED", "ws_accounts_degraded"
                break

        # --- 3) Reconciler miss rate (bursts) -----------------------------
        miss_rate = float(
            getattr(self, "ws_reco_miss_rate_per_min", getattr(self, "ws_reco_miss_per_minute", 0.0))
            or 0.0
        )
        thr_con = int(self._switch_knobs.rm_reco_miss_per_minute_constrained)
        thr_sev = int(self._switch_knobs.rm_reco_miss_per_minute_severe)

        if miss_rate >= thr_sev:
            target_mode, target_reason = "SEVERE", "ws_reco_miss_burst"
        elif miss_rate >= thr_con and rank.get(target_mode or "NORMAL", 0) < 1:
            target_mode, target_reason = "CONSTRAINED", "ws_reco_miss_rate"

        # --- 4) Latences WS (heartbeat/event/ack/fill) --------------------
        latency_checks = [
            ("pws_heartbeat_gap_seconds",
             float(self._switch_knobs.rm_pws_heartbeat_gap_severe_s),
             float(self._switch_knobs.rm_pws_heartbeat_gap_constrained_s)),
            ("pws_event_lag_ms",
             float(self._switch_knobs.rm_pws_event_lag_severe_ms),
             float(self._switch_knobs.rm_pws_event_lag_constrained_ms)),
            ("pws_ack_latency_ms",
             float(self._switch_knobs.rm_pws_ack_latency_severe_ms),
             float(self._switch_knobs.rm_pws_ack_latency_constrained_ms)),
            ("pws_fill_latency_ms",
             float(self._switch_knobs.rm_pws_fill_latency_severe_ms),
             float(self._switch_knobs.rm_pws_fill_latency_constrained_ms)),
        ]
        for name, sev_thr, con_thr in latency_checks:
            try:
                val = float(
                    getattr(self, f"{name}_p95", getattr(self, name, 0.0))
                    or 0.0
                )
            except Exception:
                continue

            if val >= sev_thr:
                target_mode, target_reason = "SEVERE", name
                break
            if val >= con_thr and rank.get(target_mode or "NORMAL", 0) < 1:
                target_mode, target_reason = "CONSTRAINED", name

        # --- 5) Fee tokens "LOW" prolongés -------------------------------
        fee_meta = getattr(self, "_last_fee_tokens_meta", {}) or {}
        fee_low_now = False
        min_pct = float(self._switch_knobs.rm_fee_token_min_pct)

        for token_meta in fee_meta.values():
            if not isinstance(token_meta, dict):
                continue
            for tok_meta in (token_meta or {}).values():
                status = str((tok_meta or {}).get("status") or (tok_meta or {}).get("level") or "").upper()
                pct = float((tok_meta or {}).get("percent") or (tok_meta or {}).get("pct", 100.0) or 100.0)
                if status in ("LOW", "CRITICAL") or pct < min_pct:
                    fee_low_now = True
                    break
            if fee_low_now:
                break

        low_since = getattr(self, "_fee_low_since_ts", None)
        if fee_low_now:
            if low_since is None:
                low_since = now
        else:
            low_since = None
        try:
            self._fee_low_since_ts = low_since
        except Exception:
            # AttributeError possible si __slots__, on ignore.
            pass

        min_low_s = float(self._switch_knobs.rm_fee_low_min_seconds)
        if (
                fee_low_now
                and low_since is not None
                and (now - low_since) >= max(0.0, min_low_s)
                and rank.get(target_mode or "NORMAL", 0) < 1
        ):
            target_mode, target_reason = "CONSTRAINED", "fees_low_prolonged"

        return target_mode, target_reason

    def _net_bps_with_split_penalty(self, net_bps_raw: float, buy_ex: str, sell_ex: str) -> tuple[float, float]:
        """
        Convention RM:
        - net_bps_raw = net_bps calculé "marché - fees/slip" (sans SPLIT penalty)
        - net_bps_eff = net_bps_raw - split_penalty_bps
        Retourne (net_bps_eff, split_penalty_bps).
        """
        try:
            pen = float(self._split_penalty_bps(str(buy_ex).upper(), str(sell_ex).upper()) or 0.0)
            if pen < 0 or math.isnan(pen):
                pen = 0.0
        except Exception:
            pen = 0.0
        try:
            nb = float(net_bps_raw)
            if math.isnan(nb):
                nb = 0.0
        except Exception:
            nb = 0.0
        return (nb - pen, pen)

    def _resolve_exchange_region(self, ex: str) -> str | None:
        """
        Résout la région (EU/US/JP/...) d’un exchange via cfg/botconfig.
        - Si aucun mapping n’est dispo -> None (fallback comportement historique).
        """
        exu = (ex or "").upper()
        if not exu:
            return None

        cfg = getattr(self, "cfg", None) or getattr(self, "config", None) or self
        g = getattr(cfg, "g", None)

        # 1) méthode dédiée si dispo (préférée)
        for obj in (cfg, g):
            if obj is None:
                continue
            for fn_name in ("exchange_region", "get_exchange_region", "cex_region", "get_cex_region"):
                fn = getattr(obj, fn_name, None)
                if callable(fn):
                    try:
                        r = fn(exu)
                        r = (str(r).upper() if r is not None else None)
                        return r or None
                    except Exception:
                        pass

        # 2) mapping dict si dispo
        for obj in (cfg, g):
            if obj is None:
                continue
            for attr in ("exchange_region_map", "cex_region_map", "engine_region_map", "engine_pod_map"):
                mp = getattr(obj, attr, None)
                if isinstance(mp, dict):
                    try:
                        # tolère clés non upper dans la config
                        if exu in mp:
                            return str(mp[exu]).upper()
                        for k, v in mp.items():
                            if str(k).upper() == exu:
                                return str(v).upper()
                    except Exception:
                        pass

        return None

    def _split_edge_key(self, r1: str, r2: str) -> str:
        a = str(r1).upper()
        b = str(r2).upper()
        return f"{a}-{b}" if a <= b else f"{b}-{a}"

    def _split_rm_cfg(self) -> Any:
        cfg = getattr(self, "cfg", None) or getattr(self, "config", None) or self
        return getattr(cfg, "rm", None)

    def _split_latency_defaults(self, mode: Optional[str] = None) -> tuple[float, float, float]:
        cfg = getattr(self, "cfg", None) or getattr(self, "config", None) or self
        g = getattr(cfg, "g", None)
        mode_key = str(mode or getattr(self, "split_mode", "") or getattr(g, "deployment_mode", "")).upper()
        if g is not None:
            latency = None
            if mode_key == "JP_ONLY":
                latency = getattr(g, "jp_latency", None)
            elif mode_key == "EU_ONLY":
                latency = getattr(g, "eu_latency", None)
            else:
                latency = getattr(g, "split_latency", None)
            if isinstance(latency, dict):
                base = float(latency.get("base_delta_ms", 0.0) or 0.0)
                skew = float(latency.get("skew_ms", 0.0) or 0.0)
                stale = float(latency.get("stale_ms", 0.0) or 0.0)
                return base, skew, stale
        return 0.0, 0.0, 0.0
    def _split_metrics_for_edge(self, edge_key: str) -> tuple[float, float, float]:
        """
        Retourne (base_delta_ms, skew_ms, stale_ms) pour une paire de régions.
        - Si le pacer/boot injecte self.split_edge_metrics[edge_key], on l’utilise.
        - Sinon fallback sur les métriques globales de config (cfg.g.split_latency).
        """
        try:
            em = getattr(self, "split_edge_metrics", None)
            if isinstance(em, dict):
                d = em.get(edge_key)
                if isinstance(d, dict):
                    base = float(d.get("base_delta_ms", d.get("base", 0.0)) or 0.0)
                    skew = float(d.get("skew_ms", d.get("skew", 0.0)) or 0.0)
                    stale = float(d.get("stale_ms", d.get("stale", 0.0)) or 0.0)
                    return base, skew, stale
        except Exception:
            pass

        return self._split_latency_defaults()


    def _split_penalty_bps(self, buy_ex: str, sell_ex: str) -> float:
        """
        Penalty deterministica basata su metriche SPLIT correnti e soglie cfg.
        """
        mode = str(getattr(self, "split_mode", "EU_ONLY")).upper()
        if mode not in {"SPLIT", "JP_ONLY"}:
            return 0.0

        buy_region = self._resolve_exchange_region(buy_ex)
        sell_region = self._resolve_exchange_region(sell_ex)
        if buy_region and sell_region:
            edge_key = self._split_edge_key(buy_region, sell_region)
            base, skew, stale = self._split_metrics_for_edge(edge_key)
        else:
            base, skew, stale = self._split_latency_defaults(mode)

        rm_cfg = self._split_rm_cfg()
        thr_base = float(getattr(rm_cfg, "split_breach_thr_base_ms", 180.0))
        thr_skew = float(getattr(rm_cfg, "split_breach_thr_skew_ms", 40.0))
        thr_stal = float(getattr(rm_cfg, "split_breach_thr_stale_ms", 1300.0))

        ratio = max(
            base / max(thr_base, 1e-9),
            skew / max(thr_skew, 1e-9),
            stale / max(thr_stal, 1e-9),
            0.0
        )
        cap = float(getattr(rm_cfg, "split_penalty_bps_max", 6.0))
        return float(min(cap, cap * ratio))

    def _compute_tt_revalidation_overlays(self, buy_ex: str, sell_ex: str, pair_key: str) -> Dict[str, float]:
        """
        Calcule les overlays RM-owned pour TT (Ticket D2-2).
        - latency_penalty_bps: dérivé du p95 submit->ack reporté par l'Engine.
        - staleness_penalty_bps: dérivé de l'âge du book vs TTL.
        """
        overlays = {
            "latency_penalty_bps": 0.0,
            "staleness_penalty_bps": 0.0
        }

        # 1. Latency penalty
        if self._get_rm_knob("latency_penalty_enabled", default=True):
            engine_metrics = getattr(self, "engine_metrics", {})
            p95_ack = float(engine_metrics.get("p95_ack_ms", 0.0))
            
            # Logique piecewise: ex 40ms -> 0bps, 100ms -> 5bps, 200ms -> 10bps
            if p95_ack > 40.0:
                # 0.1 bps par ms au dessus de 40ms, capé à 15bps
                overlays["latency_penalty_bps"] = min(15.0, (p95_ack - 40.0) * 0.1)

        # 2. Staleness penalty
        if self._get_rm_knob("staleness_penalty_enabled", default=True):
            # On récupère l'âge le plus élevé entre les deux jambes
            book_age_s = 0.0
            try:
                books = self._last_books or {}
                pk = self._norm_pair(pair_key)
                buy_book = books.get(buy_ex, {}).get(pk, {}) or {}
                sell_book = books.get(sell_ex, {}).get(pk, {}) or {}
                
                age_buy = time.time() - float(buy_book.get("ts_exchange_ms", 0) / 1000.0) if buy_book.get("ts_exchange_ms") else 0
                age_sell = time.time() - float(sell_book.get("ts_exchange_ms", 0) / 1000.0) if sell_book.get("ts_exchange_ms") else 0
                book_age_s = max(age_buy, age_sell)
            except Exception:
                pass

            if book_age_s > 0.4: # 400ms
                # 5 bps par seconde au dessus de 400ms, capé à 10bps
                overlays["staleness_penalty_bps"] = min(10.0, (book_age_s - 0.4) * 10.0)

        # Metrics pour debug
        if overlays["latency_penalty_bps"] > 0 or overlays["staleness_penalty_bps"] > 0:
             try:
                 if hasattr(self, "obs_inc"):
                     self.obs_inc("rm_latency_penalty_bps", value=overlays["latency_penalty_bps"])
                     self.obs_inc("rm_staleness_penalty_bps", value=overlays["staleness_penalty_bps"])
             except Exception:
                 pass

        return overlays

    def _total_cost_bps(self, buy_ex: str, sell_ex: str, pair_key: str) -> float:
        """
        bps = 1e4 * total_cost_pct + split_penalty_bps
        Source de vérité: get_total_cost_pct() (déjà routé vers SFC), puis pénalité SPLIT.
        """
        try:
            pct = float(self.get_total_cost_pct(buy_ex, sell_ex, pair_key) or 0.0)
        except Exception:
            pct = 0.0

        try:
            split_pen = float(self._split_penalty_bps(str(buy_ex).upper(), str(sell_ex).upper()))
        except Exception:
            split_pen = 0.0

        return float(1e4 * max(0.0, pct) + split_pen)


    def _update_trade_mode(self) -> None:
        """
        Applique trade_mode (Macro 5) à partir de rm_mode + pacer_mode
        en s'appuyant sur _compute_trade_mode(), puis logge les transitions.

        Domaines :
        - NORMAL
        - CONSTRAINED
        - SEVERE
        - OPPORTUNISTE (marché "vert" + pacer NORMAL)
        """
        prev = getattr(self, "trade_mode", "NORMAL")
        mode = self._compute_trade_mode()

        if mode != prev:
            try:
                logging.getLogger(__name__).info(
                    "[RM][MODE] trade_mode_transition %s->%s rm_mode=%s pacer_mode=%s",
                    prev,
                    mode,
                    str(getattr(self, "rm_mode", "NORMAL")).upper(),
                    str(getattr(self, "pacer_mode", "NORMAL")).upper(),
                )
            except Exception:
                pass

            try:
                if hasattr(self, "obs_inc"):
                    self.obs_inc(
                        "rm_mode_transitions_total",
                        kind="trade_mode",
                        old=prev,
                        new=mode,
                    )
            except Exception:
                pass

        self.trade_mode = mode
        self._check_private_public_mode_invariant()


    def _tick_mode(self) -> None:
        # 1) PnL-guard (peut forcer OPP_VOL / SEVERE)
        self._pnl_guard_tick()

        now = time.time()
        cur = self.rm_mode
        prev_rm_mode = getattr(self, "_last_rm_mode_obs", cur)
        old_rm_mode = cur
        prev_trade_mode = getattr(self, "_last_trade_mode_obs", getattr(self, "trade_mode", "NORMAL"))
        exit_reason = "transition"

        # 2) Triggers incident (TTL/WS/fees)
        target_mode, target_reason = self._evaluate_incident_triggers()
        if target_mode is not None:
            rank = {"NORMAL": 0, "OPP_VOLUME": 1, "OPP_VOL": 1, "CONSTRAINED": 2, "SEVERE": 3}
            if rank.get(target_mode, 0) > rank.get(self.rm_mode, 0):
                self.rm_mode, self._mode_since = target_mode, now
                exit_reason = target_reason or "incident"

        # ---- Sorties ----
        if cur == "OPP_VOLUME":
            # Plancher net effectif (si métrique dispo)
            net_floor_hit = False
            try:
                p50 = float(getattr(self, "net_bps_effective_p50", float("inf")))
                net_floor_hit = bool(p50 < float(self._net_floor_bps))
            except Exception:
                pass

            # Timeout (robuste) + conditions "calme" + net floor
            timeout_s = int(self._opp_volume_timeout_s or self._mode_timeout_s)
            if (now - self._mode_since > timeout_s) or (not self._green_calme()) or net_floor_hit:
                self.rm_mode, self._mode_since = "NORMAL", now
                exit_reason = "opp_volume_exit"

        elif cur in ("OPP_VOL", "SEVERE"):
            # Sortie sous conditions vertes + hystérésis de sortie
            exit_hyst = int(getattr(self, "_exit_hyst_s", 120))
            if self._green_calme():
                if (now - getattr(self, "_last_all_green_ts", now)) >= exit_hyst:
                    self.rm_mode, self._mode_since = "NORMAL", now
                    exit_reason = "calm_recovered"
            else:
                # Marqueur : dernier "pas calme" (sert pour l'hystérésis de sortie)
                self._last_all_green_ts = now

        # ---- Entrées opportunistes ----
        if self.rm_mode == "NORMAL":
            if self._red_tempete():
                self.rm_mode, self._mode_since = "OPP_VOL", now
                exit_reason = normalize_reason_code(ReasonCodes.TEMPETE) or ReasonCodes.TEMPETE
            elif self._green_calme():
                # Hystérésis d'entrée : temps écoulé depuis le dernier "pas calme"
                enter_hyst = int(getattr(self, "_enter_hyst_s", 180))
                if (now - getattr(self, "_last_all_green_ts", now)) >= enter_hyst:
                    self.rm_mode, self._mode_since = "OPP_VOLUME", now
                    exit_reason = normalize_reason_code(ReasonCodes.CALM_ENTRY) or ReasonCodes.CALM_ENTRY
            else:
                # Marqueur : dernier "pas calme" (reset la fenêtre d'hystérésis)
                self._last_all_green_ts = now

        # Consolidation du mode pour l'Engine (toujours mise à jour)
        try:
            self._update_trade_mode()
        except Exception:
            # Fallback : ne jamais casser la FSM pour un problème de consolidation
            try:
                rm_cfg = getattr(getattr(self, "cfg", None), "rm", None)
                fallback_mode = str(
                    getattr(rm_cfg, "fallback_on_tick_exception", "CONSTRAINED")
                ).upper()
            except Exception:
                fallback_mode = "CONSTRAINED"
            if fallback_mode not in ("CONSTRAINED", "SEVERE"):
                fallback_mode = "CONSTRAINED"
            self.trade_mode = fallback_mode
            self.rm_mode = fallback_mode
            self._mode_since = now

        new_rm_mode = getattr(self, "rm_mode", "NORMAL")
        if old_rm_mode != new_rm_mode:
            try:
                logging.getLogger(__name__).info(
                    "[RM][MODE] rm_mode_transition %s->%s reason=%s",
                    old_rm_mode,
                    new_rm_mode,
                    exit_reason,
                )
            except Exception:
                pass

            try:
                if hasattr(self, "obs_inc"):
                    self.obs_inc(
                        "rm_mode_transitions_total",
                        kind="rm_mode",
                        old=old_rm_mode,
                        new=new_rm_mode,
                    )
            except Exception:
                pass

        try:
            from . import obs_metrics  # type: ignore
        except Exception:  # pragma: no cover
            obs_metrics = None

        if obs_metrics is not None:
            if self.rm_mode != prev_rm_mode:
                entries = getattr(obs_metrics, "RM_MODE_ENTRIES_TOTAL", None)
                if entries is not None:
                    try:
                        entries.labels(mode=str(self.rm_mode).upper()).inc()
                    except Exception:
                        pass

                exits = getattr(obs_metrics, "RM_MODE_EXITS_TOTAL", None)
                if exits is not None and prev_rm_mode is not None:
                    try:
                        exits.labels(
                            mode=str(prev_rm_mode).upper(),
                            reason=str(exit_reason),
                        ).inc()
                    except Exception:
                        pass
                self._last_rm_mode_obs = self.rm_mode

            if self.trade_mode != prev_trade_mode:
                self._last_trade_mode_obs = self.trade_mode

            self._obs_set_mode_gauges(self.rm_mode, self.trade_mode)

        # ---- Appliquer les overlays du mode courant ----
        self._apply_mode_overrides()

        # risk_manager.py — dans _tick_mode(), juste avant de sortir de la méthode
        self._split_auto_fallback_tick()



    def get_orderbook_depth(self, exchange: str, pair_key: str, depth: int = 10):
        """
        Retourne (asks, bids) au format [(px, qty), ...]
        """
        try:
            get = getattr(self, "get_orderbooks_callback", None)
            if callable(get):
                ob = get()
                ex = (exchange or "").upper()
                pk = (pair_key or "").replace("-", "").upper()
                d = (ob.get(ex, {}) or {}).get(pk, {}) or {}
                asks = list(d.get("asks") or [])[: int(depth)]
                bids = list(d.get("bids") or [])[: int(depth)]
                return asks, bids
        except Exception:
            logging.exception("Unhandled exception")
        return [], []

    def _fee_pct(self, exchange: str, role: str) -> float:
        """
        Récupère un fee % en fraction (0.001 = 10 bps).
        """
        try:
            fn = getattr(self, "get_fee_pct", None)
            if callable(fn):
                try:
                    return float(fn(exchange, role))
                except TypeError:
                    return float(fn(exchange, None, role))
        except Exception:
            logging.exception("Unhandled exception")
        # fallback env/policy
        mp = getattr(self, "fee_map_pct", {}) or {}
        ex = (exchange or "").upper()
        if isinstance(mp.get(ex), dict):
            v = mp[ex].get(role.lower())
            if v is not None:
                return float(v)
        return 0.0



    def _slip_pct(self, exchange: str, pair_key: str, side: str) -> float:
        """
        Slippage modèle (fraction). Fallback 0 si non dispo.
        """
        try:
            model = getattr(self, "current_slippage_model", None) or getattr(self, "slippage_model", None)
            if isinstance(model, dict):
                ex = (exchange or "").upper()
                pk = (pair_key or "").replace("-", "").upper()
                sd = (side or "").lower()
                v = (
                        ((model.get(ex, {}) or {}).get(pk, {}) or {}).get(sd)
                        or ((model.get("pairs", {}) or {}).get(pk, {}) or {}).get(sd)
                )
                if v is not None:
                    v = float(v)
                    return v / 1e4 if v > 1.0 else v  # bps->ratio si besoin
        except Exception:
            logging.exception("Unhandled exception")
        return 0.0
    @staticmethod
    def _vwap_from_depth(self, levels: list[tuple[float, float]], side: str, notional: float) -> float:
        """
        VWAP côté depth. side='BUY' consomme asks, 'SELL' consomme bids. Notional en quote.
        Retourne le prix moyen payé/obtenu (0 si depth insuffisante ou notional<=0).
        """
        need = float(notional or 0.0)
        if need <= 0.0 or not levels:
            return 0.0
        filled_quote = 0.0
        filled_base = 0.0
        if side.upper() == "BUY":
            # asks croissants
            for px, qty in levels:
                px = float(px);
                qty = float(qty)
                if px <= 0 or qty <= 0:
                    continue
                lot_quote = px * qty
                take_quote = min(need - filled_quote, lot_quote)
                if take_quote <= 0:
                    break
                take_base = take_quote / px
                filled_quote += take_quote
                filled_base += take_base
                if filled_quote >= need - 1e-9:
                    break
        else:
            # bids décroissants
            for px, qty in levels:
                px = float(px);
                qty = float(qty)
                if px <= 0 or qty <= 0:
                    continue
                lot_quote = px * qty
                take_quote = min(need - filled_quote, lot_quote)
                if take_quote <= 0:
                    break
                take_base = take_quote / px
                filled_quote += take_quote
                filled_base += take_base
                if filled_quote >= need - 1e-9:
                    break
        if filled_base <= 0:
            return 0.0
        return filled_quote / filled_base

    async def is_fragment_profitable(
            self,
            *,
            pair_key: str,
            buy_ex: str,
            sell_ex: str,
            usdc_amt: float,
            strategy: str = "TT",
            timeout_s: Optional[float] = None,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Fusion analytique + simulation.
        1) Pré-check analytique: VWAP(asks pour BUY, bids pour SELL) + fees/slip → net_bps_est.
           - Si très au-dessus du seuil: accepte sans sim.
           - Si très au-dessous du seuil:
             rejette sans sim.
           - Entre les deux: renvoie "zone_grise" pour sim.
        2) La simulation (si appelée) utilise le même contrat de bundle que l’Engine.
        """
        try:
            pk = self._norm_pair(pair_key)

            if usdc_amt <= 0:
                return (False, None)

            # VWAP côté acheteur (asks) et vendeur (bids)
            buy_asks, _ = self._levels_for(buy_ex, pk)
            _, sell_bids = self._levels_for(sell_ex, pk)
            if not buy_asks or not sell_bids:
                return (False, {"status": "rejete", "reason": "depth_insufficient"})

            vwap_buy = self._vwap_from_depth(buy_asks, side="BUY", notional=usdc_amt)
            vwap_sell = self._vwap_from_depth(sell_bids, side="SELL", notional=usdc_amt)

            if vwap_buy <= 0 or vwap_sell <= 0 or vwap_sell <= vwap_buy:
                return (False, {"status": "rejete", "reason": "depth_insufficient"})

            # Frais & slippage "robust" (fallbacks intégrés)
            fee_buy = max(0.0, self._fee_pct(buy_ex, "taker"))
            fee_sell = max(0.0, self._fee_pct(sell_ex, "taker"))
            slip_buy = max(0.0, self._slip_pct(buy_ex, pk, "buy"))
            slip_sell = max(0.0, self._slip_pct(sell_ex, pk, "sell"))

            buy_cost = vwap_buy * (1.0 + fee_buy + slip_buy)
            sell_take = vwap_sell * (1.0 - fee_sell - slip_sell)
            net_bps_est = 1e4 * (sell_take - buy_cost) / max(buy_cost, 1e-12)

            net_bps_est_raw = float(net_bps_est)
            net_bps_est, split_penalty_bps = self._net_bps_with_split_penalty(net_bps_est_raw, buy_ex, sell_ex)

            # Seuils unifiés via politique RM (TT/TM dynamiques)
            strat = str(strategy or "TT").upper()
            thr_bps = float(self._min_required_bps_for(pk, strat))

            # Bande de décision "auto" pour savoir si l'on simule (zone grise)
            margin_bps = float(getattr(self.cfg, "fragment_sim_margin_bps", 5.0))

            analytic_block = {
                "method": "analytic",
                "vwap_buy": float(vwap_buy),
                "vwap_sell": float(vwap_sell),
                "fee_buy": float(fee_buy),
                "fee_sell": float(fee_sell),
                "slip_buy": float(slip_buy),
                "slip_sell": float(slip_sell),
                "net_bps_est_raw": float(net_bps_est_raw),
                "split_penalty_bps": float(split_penalty_bps),
                 "net_bps_est": float(net_bps_est),  # net effectif (après pénalité SPLIT)

                "min_required_bps": float(thr_bps),
                "margin_bps": float(margin_bps),
            }

            # Cas 1: largement gagnant → OK sans sim
            if net_bps_est >= (thr_bps + margin_bps):
                res = {
                    "status": "rentable",
                    "decision": "analytic_confident_win",
                    **analytic_block,
                }
                return (True, res)

            # Cas 2: largement perdant → rejet sans sim
            if net_bps_est <= (thr_bps - margin_bps):
                res = {
                    "status": "rejete",
                    "reason": "analytic_confident_loss",
                    **analytic_block,
                }
                return (False, res)

            # Cas 3: zone grise → on autorise mais on demande une sim
            res = {
                "status": "zone_grise",
                "decision": "simulate",
                **analytic_block,
            }
            return (True, res)

        except Exception:
            logging.exception("is_fragment_profitable failed")
            return (False, None)



    async def _loop_fee_sync(self) -> None:
        """
        Boucle passive pilotée par le RM : cadence le refresh SFC et publie l’âge des snapshots.
        - ZERO scheduling interne côté SFC (respect de la passivité).
        - Publie FEESYNC_LAST_TS et FEE_SNAPSHOT_AGE_SECONDS{ex,alias}.
        - Intervalle clampé entre 30 et 120 secondes (lisible depuis plusieurs sources).
        """
        sfc = getattr(self, "slip_collector", None)
        if not sfc or not hasattr(sfc, "fee_sync_refresh_once"):
            return

        import asyncio, time, logging
        log = getattr(self, "logger", None) or logging.getLogger("RM.feesync")

        # métriques (no-op si indispo)
        try:
            from modules.obs_metrics import FEESYNC_LAST_TS, FEE_SNAPSHOT_AGE_SECONDS, FEESYNC_ERRORS
        except Exception:
            FEESYNC_LAST_TS = FEE_SNAPSHOT_AGE_SECONDS = FEESYNC_ERRORS = None  # type: ignore

        def _read_interval() -> float:
            # priorité: attribut direct -> cfg -> bot_cfg -> défaut
            try:
                itv = float(getattr(self, "fee_sync_interval"))
            except Exception:
                try:
                    itv = float(getattr(getattr(self, "cfg", None), "fee_sync_interval_s", 60.0))
                except Exception:
                    try:
                        itv = float(getattr(getattr(self, "bot_cfg", None), "fee_sync_interval", 60.0))
                    except Exception:
                        itv = 60.0
            if itv <= 0:
                itv = 60.0
            # clamp 30–120 s
            return max(30.0, min(120.0, itv))

        while True:
            try:
                await sfc.fee_sync_refresh_once()

                # marquer le ts last-refresh
                if FEESYNC_LAST_TS:
                    try:
                        FEESYNC_LAST_TS.set(time.time())
                    except Exception:
                        pass

                # publier l’âge des snapshots (si exposés par le SFC)
                get_clients = getattr(sfc, "get_fee_clients", None)
                get_snap = getattr(sfc, "get_snapshot", None)
                if callable(get_clients) and callable(get_snap) and FEE_SNAPSHOT_AGE_SECONDS:
                    try:
                        for ex, by_alias in (get_clients() or {}).items():
                            for alias in (by_alias or {}).keys():
                                snap = get_snap(ex, alias) or {}
                                last = float(snap.get("last_refresh_ts", 0.0))
                                age = max(0.0, time.time() - last) if last > 0 else 1e9
                                try:
                                    FEE_SNAPSHOT_AGE_SECONDS.labels(ex, alias).set(age)
                                except Exception:
                                    pass
                    except Exception:
                        pass

                self._mark_loop_success("fee_sync")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if FEESYNC_ERRORS:
                    try:
                        FEESYNC_ERRORS.inc()
                    except Exception:
                        pass
                log.debug("fee_sync_refresh_once failed", exc_info=False)
                self._mark_loop_error("fee_sync", exc)
            try:
                await asyncio.sleep(_read_interval())
            except asyncio.CancelledError:
                raise

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    # À ajouter dans la classe RiskManager, à côté des helpers balances
    def get_balance(self, exchange: str, token: str, account_alias: str | None = None,
                    *, mode: str | None = None, cached_only: bool = True) -> float:
        """
        Lecture stricte et déterministe des soldes depuis le snapshot RM/MBF.
        - exchange: 'BINANCE'/'BYBIT'/'COINBASE'
        - token: e.g. 'BNB', 'MNT', 'USDC', ...
        - account_alias: None => somme tous alias, sinon alias précis (ex: 'TT', 'TM', 'MM')
        - mode: 'real'/'virtual'/'merged' (par défaut, il reprend la vue RM)
        """
        ex = (exchange or "").upper()
        tok = (token or "").upper()
        snap = self.get_balance_snapshot_for_rebal(mode=mode, cached_only=cached_only)

        # Nouveau contrat : snap = {"mode": ..., "balances": {...}, "meta": {...}}
        if isinstance(snap, dict) and "balances" in snap:
            balances = snap.get("balances") or {}
        else:
            # Compat legacy en cas d'appel direct à une ancienne version.
            balances = snap or {}

        accounts = (balances.get(ex, {}) or {}) if isinstance(balances, dict) else {}

        if account_alias:
            assets = accounts.get(account_alias, {}) or {}
            try:
                return float(assets.get(tok, 0.0))
            except Exception:
                return 0.0

        # somme cross-alias
        total = 0.0
        for assets in (accounts or {}).values():
            try:
                total += float((assets or {}).get(tok, 0.0))
            except Exception:
                continue
        return total


    async def _apply_adjustments(self, pair: str, adjustments: Dict[str, Any]):
        try:
            if hasattr(self.simulator, "update_volatility_metrics"):
                self.simulator.update_volatility_metrics(pair, self.vol_monitor.get_current_metrics(pair))
            if hasattr(self.simulator, "update_trade_parameters"):
                self.simulator.update_trade_parameters(pair, adjustments)
        except Exception:
            logger.debug(f"[RiskManager] simulator update failed for {pair}", exc_info=False)

    def _alert(self, module: str, message: str, pair: Optional[str] = None, alert_type: str = "INFO"):
        if not self.alert_cb:
            return
        try:
            self._dispatch_best_effort(
                self.alert_cb,
                name="alert_cb",
                payload={"module": module, "message": message, "pair": pair, "alert_type": alert_type},
            )
        except Exception:
            logging.exception("Unhandled exception")

    # ---------- API publique d'ingestion (appelée par les services externes) ----------

    def ingest_volatility_bps(self, pair: str, exchange: str, vol_bps: float, ts_ns: int | None = None) -> None:
        """
        STRICT: inoltra direttamente al VolatilityManager (interfaccia ufficiale).
        Niente hasattr, niente alias dinamici: l’API richiesta è ingest_spread_bps().
        """
        if self.vol_manager is None:
            raise RuntimeError("VolatilityManager non inizializzato")

        pk = self._norm_pair(pair)
        ex = str(exchange).upper().strip()
        vb = float(vol_bps)

        # Conversione del timestamp: il RM riceve ts_ns (nanosecondi),
        # il VolatilityManager lavora in secondi (time.time()).
        ts: float | None = None
        if ts_ns is not None:
            try:
                ts = float(ts_ns) / 1e9
            except Exception:
                # En cas de valeur bizarroïde, on laisse ts=None → VM utilisera _now()
                ts = None

        # Interfaccia hard del VM (già presente nel modulo): ingest_spread_bps(exchange, pair, spread_bps, ts)
        # vedi definizione in volatility_manager.py
        self.vol_manager.ingest_spread_bps(ex, pk, vb, ts=ts)

    def ingest_slippage_bps(self, pair: str, exchange: str, side: str,
                            qty: float, slip_bps: float, ts_ns: int | None = None) -> None:
        """
        STRICT: il RM inoltra al solo SlippageAndFeesCollector.
        Nessun alias, nessun calcolo surrogato.
        """
        import math
        if self.slip_collector is None:
            raise RuntimeError("SlippageAndFeesCollector non inizializzato")

        pk = self._norm_pair(pair)
        ex = str(exchange).upper().strip()
        sd0 = str(side).upper().strip()
        sd = "BUY" if sd0.startswith("B") else ("SELL" if sd0.startswith("S") else None)
        if sd is None:
            raise ValueError(f"side invalide: {side!r}")

        q = float(qty)
        bps = float(slip_bps)
        if not math.isfinite(bps) or abs(bps) > 5000:
            raise ValueError(f"slip_bps invalide: {slip_bps!r}")

        # Interfaccia hard del Collector
        self.slip_collector.ingest_slippage_bps(pair=pk, exchange=ex, side=sd, qty=q, slip_bps=bps, ts_ns=ts_ns)

    # ------------------------------------------------------------------
    # Rebalancing helpers / builders / routing (suite en part 2)
    # ------------------------------------------------------------------
    # (les méthodes _handle_rebalancing_op, _legacy_convert, _forward_rebalancing_trade,
    #  _exec_internal_wallet_transfer, _exec_internal_subaccount_transfer,
    #  _make_internal_transfer, _make_cross_cex_opportunity, _make_rebalancing_actions,
    #  _emit_rebalancing_opportunity arrivent en Part 2)
    # -*- coding: utf-8 -*-
    # Exemple d’API côté RM :

    """
    RiskManager — OFFICIEL (V2.1) — Fusion ciblée (part 2/2)
    =========================================================
    Suite et fin de la classe `RiskManager` (validations, stratégies TM, hooks
    Scanner/Engine, fragmentation, status, etc.).

    ⚠️ Cette partie complète **Part 1/2**. Concaténées, elles forment le fichier unique.
    """


    def _validate_rebalancing_cross(self, cross: Dict[str, Any]) -> bool:
        """
        Validation finale no-loss pour un rebalancing cross-CEX.

        - Utilise la lecture d'orderbooks courants + fees/slip dynamiques du RiskManager.
        - Applique la politique métier `rebal_allow_loss_bps` pour accepter / rejeter un REB en fonction
          de son coût attendu en bps (via RebalancingManager.estimate_cross_cex_net_bps).
        - Cette méthode, couplée à `revalidate_arbitrage(..., min_required_bps=0.0, is_rebalancing=True,
          allow_final_loss_bps=...)`, est l’unique arbitre **économique** des REB cross-CEX.
          L’ExecutionEngine ne doit pas recalculer un second "rebalancing_cost" en bps en parallèle.
        """
        try:
            buy_ex = str(cross.get("to_exchange") or cross.get("buy_exchange") or "").upper()
            sell_ex = str(cross.get("from_exchange") or cross.get("sell_exchange") or "").upper()
            pair = (cross.get("pair_key") or cross.get("pair") or "").replace("-", "").upper()
            if not (buy_ex and sell_ex and pair):
                return False

            # Extraction de la taille pour l'estimation (size-aware)
            size_quote = float(cross.get("amount") or cross.get("amount_usdc") or 0.0)

            allow_loss = float(self.rebal_allow_loss_bps)
            estimator = getattr(self, "rebal_mgr", None)
            if estimator and hasattr(estimator, "estimate_cross_cex_net_bps"):
                try:
                    est_bps = float(estimator.estimate_cross_cex_net_bps(
                        pair_key=pair,
                        from_exchange=sell_ex,
                        to_exchange=buy_ex,
                        size_quote=size_quote,
                    ))
                except Exception:
                    est_bps = None
                else:
                    if est_bps is not None and est_bps < -allow_loss:
                        logger.info(
                            "[RiskManager] rebal cross %s→%s rejeté (net=%.1f bps < -%.1f, size=%.0f)",
                            sell_ex,
                            buy_ex,
                            est_bps,
                            allow_loss,
                            size_quote
                        )
                        try:
                            REBAL_CROSS_TOO_EXPENSIVE_TOTAL.labels("rm_validate").inc()
                        except Exception:
                            pass
                        return False

            # Validation "prix": net >= -allow_loss_bps (par défaut 0.0)
            ok = self.revalidate_arbitrage(
                buy_ex=buy_ex,
                sell_ex=sell_ex,
                pair_key=pair,
                expected_net=None,
                max_drift_bps=10.0,
                min_required_bps=0.0,  # rebalancing => pas d'exigence de spread minimal
                is_rebalancing=True,
                allow_final_loss_bps=allow_loss
            )
            return bool(ok)
        except Exception:
            return False


    def set_rebalancing_callback(self, cb: Optional[Callable[[Dict[str, Any]], Any]]) -> None:
        self._rebalancing_cb = cb

    def is_rebalancing_active(self) -> bool:
        return time.time() < float(self._rebalancing_until)

    def get_balance_snapshot_for_rebal(
            self,
            *,
            mode: Optional[str] = None,
            cached_only: bool = True,
    ) -> Dict[str, Any]:
        """
        Vue capitale officielle pour Rebalancing (et plus largement le desk de risque).

        - Si `_mbf_glue` est actif : snapshot complet MBF (data-plane) + méta enrichie RM.
        - Sinon (tests / smoke) : snapshot minimal à partir de `rebalancing.latest_balances`.

        Retourne toujours un dict de la forme :
        {
            "mode": str,
            "balances": {exchange: {alias: {asset: amount}}},
            "meta": {
                ...,
                "rm_ttl_balances": {
                    "status": {"EX.ALIAS": "OK"/"DEGRADED"/"BLOCKED"/"UNKNOWN", ...},
                    "age_s": {"EX.ALIAS": float, ...},
                    "capital_buckets": {
                        "by_status": {...},
                        "by_status_quote": {...},
                        "by_alias": {...}
                    }
                }
            }
        }
        """

        # Chemin nominal : MBF = source de vérité
        # Chemin nominal : MBF = source de vérité
        if getattr(self, "_mbf_glue", None) is not None:
            try:
                snap = self._mbf_glue.snapshot(mode=mode, cached_only=cached_only)
            except Exception:
                self._log.exception("RM×MBF: erreur lors de la récupération du snapshot MBF")
                snap = None

            if isinstance(snap, dict) and snap:
                balances = snap.get("balances") or {}
                meta_in = snap.get("meta") or {}
                mode_val = snap.get("mode", mode or "mbf")

                # Vue TTL + capital par bucket, construite à partir des buffers MBF.
                try:
                    ttl_capital_view = self._compute_capital_by_ttl_from_buffers(mode=mode)
                except Exception:
                    ttl_capital_view = None

                # Maps TTL par alias (statut + age_s) depuis le cache RM.
                ttl_status_map: Dict[str, str] = {}
                ttl_age_map: Dict[str, float] = {}

                try:
                    status_cache = getattr(self, "_alias_balance_status", {}) or {}
                    age_cache = getattr(self, "_alias_balance_age_s", {}) or {}

                    for (ex_u, alias_u), st in status_cache.items():
                        key_str = f"{str(ex_u).upper()}.{str(alias_u).upper()}"
                        ttl_status_map[key_str] = str(st or "UNKNOWN").upper()

                    for (ex_u, alias_u), age in age_cache.items():
                        key_str = f"{str(ex_u).upper()}.{str(alias_u).upper()}"
                        try:
                            ttl_age_map[key_str] = float(age or 0.0)
                        except Exception:
                            ttl_age_map[key_str] = 0.0
                except Exception:
                    ttl_status_map = {}
                    ttl_age_map = {}

                meta_out = dict(meta_in) if isinstance(meta_in, dict) else {}

                ttl_meta: Dict[str, Any] = {}
                if ttl_status_map:
                    ttl_meta["status"] = ttl_status_map
                if ttl_age_map:
                    ttl_meta["age_s"] = ttl_age_map
                if isinstance(ttl_capital_view, dict) and ttl_capital_view:
                    ttl_meta["capital_buckets"] = ttl_capital_view

                if ttl_meta:
                    rm_ttl = meta_out.get("rm_ttl_balances") or {}
                    if isinstance(rm_ttl, dict):
                        rm_ttl.update(ttl_meta)
                    else:
                        rm_ttl = ttl_meta
                    meta_out["rm_ttl_balances"] = rm_ttl

                return {
                    "mode": mode_val,
                    "balances": balances,
                    "meta": meta_out,
                }

        # Fallback legacy (sans MBF) : miroir brut de RebalancingManager
        raw = getattr(self.rebalancing, "latest_balances", {}) or {}
        return {
            "mode": "legacy",
            "balances": raw,
            "meta": {},
        }

    async def _handle_rebalancing_op(self, op: Dict[str, Any]) -> None:
        mode = str(getattr(self, "rm_mode", "NORMAL") or "NORMAL").upper()
        if mode == "SEVERE":
            try:
                inc_rm_reject(reason=REB_DISABLED)
            except Exception:
                pass
            logger.info(
                "[RiskManager] rebalancing op skipped in SEVERE mode",
                extra={"op_type": op.get("type"), "reason": REB_DISABLED},
            )
            return
        t = str(op.get("type", "")).lower()
        if self._rebalancing_cb:
            try:
                r = self._rebalancing_cb(op)
                if inspect.isawaitable(r):
                    await r
            except Exception:
                logger.debug("[RiskManager] rebalancing_cb failed", exc_info=False)

        if t == "internal_wallet_transfer":
            await self._exec_internal_wallet_transfer(op)
        elif t in ("internal_subaccount_transfer", "internal_alias_transfer"):
            await self._exec_internal_subaccount_transfer(op)
        elif t == "rebalancing_trade":
            await self._forward_rebalancing_trade(op)
        elif t == "overlay_compensation":
            await self._handle_overlay_compensation(op)
        elif t == "crypto_topup_hint":
            await self._handle_crypto_topup_hint(op)
        elif t == "bridge_pre_hint":
            await self._handle_bridge_pre_hint(op)
        else:
            legacy = self._legacy_convert(op)
            if legacy:
                lt = str(legacy.get("type") or "").lower()
                if lt == "rebalancing_trade":
                    await self._forward_rebalancing_trade(legacy)
                elif lt.startswith("internal_"):
                    try:
                        if lt == "internal_wallet_transfer":
                            await self._exec_internal_wallet_transfer(legacy)
                        elif lt == "internal_subaccount_transfer":
                            await self._exec_internal_subaccount_transfer(legacy)
                    except Exception:
                        logger.exception("[RiskManager] legacy internal transfer failed")

    def _record_reb_hint_metric(self, *, hint_type: str, action: str) -> None:
        try:
            safe_inc(
                RM_REB_HINTS_TOTAL,
                "rm_reb_hints_total",
                "risk_manager.reb_hint",
                type=str(hint_type or "").lower(),
                action=str(action or "").upper(),
            )
        except Exception:
            pass

    def _reb_hint_event(self, reason: str, *, status: str, hint: Optional[Dict[str, Any]] = None) -> None:
        payload = {
            "reason": str(reason or ""),
            "status": str(status or "").lower(),
            "hint_type": str((hint or {}).get("type") or "").lower(),
        }

        for key in ("exchange", "from_exchange", "to_exchange"):
            val = (hint or {}).get(key)
            if val:
                payload.setdefault(key, str(val).upper())

        try:
            self._hist_rm_event("reb.hint", payload)
        except Exception:
            pass

    async def _handle_overlay_compensation(self, op: Dict[str, Any]) -> None:
        from_ex = str(
            (op.get("from_exchange")
             or (op.get("from") or {}).get("exchange")
             or "")
        ).upper()
        to_ex = str(
            (op.get("to_exchange")
             or (op.get("to") or {}).get("exchange")
             or "")
        ).upper()

        asset = str(
            op.get("asset")
            or op.get("valuation_quote")
            or op.get("ccy")
            or ""
        ).upper()

        exposure_quote = float(
            op.get("exposure_quote")
            or op.get("amount_quote")
            or op.get("amount_usdc")
            or op.get("notional")
            or 0.0
        )
        qty = float(op.get("qty") or op.get("amount") or 0.0)
        notional = exposure_quote if exposure_quote else qty

        if notional == 0.0 or not asset:
            try:
                inc_rm_reject(reason=REB_HINT_IGNORED_BAD_CONTEXT)
            except Exception:
                pass
            self._record_reb_hint_metric(hint_type="overlay", action="IGNORED")
            self._reb_hint_event(REB_HINT_IGNORED_BAD_CONTEXT, status="ignored", hint=op)
            logger.info(
                "[RiskManager] overlay_compensation ignoré (missing_size_or_asset): %s",
                op,
            )
            return

        min_q = float(getattr(self, "rebal_fragment_min_quote", 0.0) or 0.0)
        if abs(notional) < min_q:
            try:
                inc_rm_reject(reason=REB_HINT_IGNORED_SMALL_SIZE)
            except Exception:
                pass
            self._record_reb_hint_metric(hint_type="overlay", action="IGNORED")
            self._reb_hint_event(REB_HINT_IGNORED_SMALL_SIZE, status="ignored", hint=op)
            logger.info(
                "[RiskManager] overlay_compensation ignoré (too_small %.2f < %.2f): %s",
                notional,
                min_q,
                op,
            )
            return

        from_alias = str(
            op.get("from_alias")
            or (op.get("from") or {}).get("alias")
            or op.get("alias_from")
            or op.get("alias")

        ).upper()
        to_alias = str(
            op.get("to_alias")
            or (op.get("to") or {}).get("alias")
            or op.get("alias_to")

        ).upper()
        if not from_alias or not to_alias:
            logger.warning("[RiskManager] overlay internal transfer missing aliases: %s", op)
            return

        if from_ex and to_ex and from_ex == to_ex and asset:
            transfer_op = {
                "exchange": from_ex,
                "from_alias": from_alias,
                "to_alias": to_alias,
                "ccy": asset,
                "amount": abs(notional),
            }
            transfer_op["transfer_id"] = TransferController.canonical_transfer_id(transfer_op)

            from_wallet = (op.get("from") or {}).get("wallet") or op.get("from_wallet")
            to_wallet = (op.get("to") or {}).get("wallet") or op.get("to_wallet")
            if from_wallet or to_wallet:
                if not from_alias or not to_alias or from_alias != to_alias:
                    logger.warning(
                        "[RiskManager] overlay wallet transfer refused (alias mismatch): %s -> %s",
                        from_alias,
                        to_alias,
                    )
                    return
                transfer_op.update({
                    "alias": from_alias,
                    "from_wallet": str(from_wallet or "").upper() or "SPOT",
                    "to_wallet": str(to_wallet or "").upper() or "FUNDING",
                })
                self._record_reb_hint_metric(hint_type="overlay", action="INTERNAL_TRANSFER")
                self._reb_hint_event(
                    REB_HINT_EXEC_INTERNAL_TRANSFER,
                    status="internal_transfer",
                    hint=transfer_op,
                )
                await self._exec_internal_wallet_transfer(transfer_op)
            else:
                self._record_reb_hint_metric(hint_type="overlay", action="INTERNAL_TRANSFER")
                self._reb_hint_event(
                    REB_HINT_EXEC_INTERNAL_TRANSFER,
                    status="internal_transfer",
                    hint=transfer_op,
                )
                await self._exec_internal_subaccount_transfer(transfer_op)
            return

        try:
            self._record_reb_hint_metric(hint_type="overlay", action="REB_TM_NEUTRAL_TRADE")
            self._reb_hint_event(REB_HINT_EXEC_REB_TM_NEUTRAL, status="reb_trade", hint=op)
            self._submit_reb_bundle_from_hint(op, hint_type="overlay")
        except Exception:
            logger.exception("[RiskManager] overlay_compensation bundle dispatch failed")

    async def _handle_crypto_topup_hint(self, op: Dict[str, Any]) -> None:
        mode = str(getattr(self, "rm_mode", "NORMAL") or "NORMAL").upper()
        if mode == "SEVERE":
            try:
                inc_rm_reject(reason=REB_DISABLED)
            except Exception:
                pass
            logger.info(
                "[RiskManager] crypto_topup_hint skipped in SEVERE mode",
                extra={"reason": REB_DISABLED, "exchange": op.get("exchange")},
            )
            return
        ex = str(
            op.get("exchange")
            or (op.get("to") or {}).get("exchange")
            or ""
        ).upper()
        asset = str(
            op.get("asset")
            or op.get("ccy")
            or op.get("quote")
            or op.get("valuation_quote")
            or ""
        ).upper()

        target_alias = str(
            op.get("alias")
            or op.get("to_alias")
            or (op.get("to") or {}).get("alias")
            or "TT"
        ).upper()

        target_wallet = str(
            op.get("wallet")
            or op.get("to_wallet")
            or (op.get("to") or {}).get("wallet")
            or ""
        ).upper()

        current_balance = float(
            op.get("current_balance")
            or op.get("available")
            or op.get("balance")
            or 0.0
        )
        target_min = float(
            op.get("target_min")
            or op.get("min_balance")
            or op.get("target")
            or 0.0
        )
        suggested_topup = float(
            op.get("suggested_topup")
            or op.get("suggested")
            or op.get("amount_usdc")
            or op.get("amount")
            or 0.0
        )

        missing = max(target_min - current_balance, 0.0)
        needed = suggested_topup if suggested_topup > 0.0 else missing

        if not ex or not asset:
            try:
                inc_rm_reject(reason=REB_HINT_IGNORED_BAD_CONTEXT)
            except Exception:
                pass
            self._record_reb_hint_metric(hint_type="crypto_topup", action="IGNORED")
            self._reb_hint_event(REB_HINT_IGNORED_BAD_CONTEXT, status="ignored", hint=op)
            logger.info(
                "[RiskManager] crypto_topup_hint ignoré (missing_ex_or_asset): %s",
                op,
            )
            return

        if needed <= 0.0:
            try:
                inc_rm_reject(reason=REB_HINT_IGNORED_BAD_CONTEXT)
            except Exception:
                pass
            self._record_reb_hint_metric(hint_type="crypto_topup", action="IGNORED")
            self._reb_hint_event(REB_HINT_IGNORED_BAD_CONTEXT, status="ignored", hint=op)
            logger.info(
                "[RiskManager] crypto_topup_hint ignoré (no_deficit): %s",
                op,
            )
            return

        min_q = float(getattr(self, "rebal_fragment_min_quote", 0.0) or 0.0)
        if needed < min_q:
            try:
                inc_rm_reject(reason=REB_HINT_IGNORED_SMALL_SIZE)
            except Exception:
                pass
            self._record_reb_hint_metric(hint_type="crypto_topup", action="IGNORED")
            self._reb_hint_event(REB_HINT_IGNORED_SMALL_SIZE, status="ignored", hint=op)
            logger.info(
                "[RiskManager] crypto_topup_hint ignoré (too_small %.2f < %.2f): %s",
                needed,
                min_q,
                op,
            )
            return

        snap = self._balances_snapshot()
        ex_balances = snap.get(ex, {}) or {}

        best_alias = None
        best_available = 0.0
        for al, assets in ex_balances.items():
            if str(al).upper() == target_alias:
                continue
            try:
                avail = float((assets or {}).get(asset, 0.0) or 0.0)
            except Exception:
                avail = 0.0
            if avail > best_available:
                best_available = avail
                best_alias = str(al).upper()

        if best_alias and best_available > 0.0:
            amt = min(needed, best_available)
            transfer_op = {
                "exchange": ex,
                "from_alias": best_alias,
                "to_alias": target_alias,
                "ccy": asset,
                "amount": amt,
            }

            from_wallet = str(
                op.get("from_wallet")
                or (op.get("from") or {}).get("wallet")
                or target_wallet
                or ""
            ).upper()

            if target_wallet or from_wallet:
                transfer_op.update({
                    "alias": target_alias,
                    "from_wallet": from_wallet or "SPOT",
                    "to_wallet": target_wallet or "FUNDING",
                })
                self._record_reb_hint_metric(hint_type="crypto_topup", action="INTERNAL_TRANSFER")
                self._reb_hint_event(
                    REB_HINT_EXEC_INTERNAL_TRANSFER,
                    status="internal_transfer",
                    hint=transfer_op,
                )
                await self._exec_internal_wallet_transfer(transfer_op)
            else:
                self._record_reb_hint_metric(hint_type="crypto_topup", action="INTERNAL_TRANSFER")
                self._reb_hint_event(
                    REB_HINT_EXEC_INTERNAL_TRANSFER,
                    status="internal_transfer",
                    hint=transfer_op,
                )
                await self._exec_internal_subaccount_transfer(transfer_op)
            return

        op = dict(op)
        op.setdefault("topup_amount", needed)
        try:
            self._record_reb_hint_metric(hint_type="crypto_topup", action="REB_TM_NEUTRAL_TRADE")
            self._reb_hint_event(REB_HINT_EXEC_REB_TM_NEUTRAL, status="reb_trade", hint=op)
            self._submit_reb_bundle_from_hint(op, hint_type="crypto_topup")
        except Exception:
            logger.exception("[RiskManager] crypto_topup_hint bundle dispatch failed")

    async def _handle_bridge_pre_hint(self, op: Dict[str, Any]) -> None:
        try:
            inc_rm_reject(reason=REB_HINT_IGNORED_UNIMPLEMENTED)
        except Exception:
            pass
        self._record_reb_hint_metric(hint_type="bridge_pre", action="IGNORED")
        self._reb_hint_event(REB_HINT_IGNORED_UNIMPLEMENTED, status="ignored", hint=op)
        logger.info("[RiskManager] bridge_pre_hint ignoré (unimplemented): %s", op)

    def _mark_loop_success(self, loop_name: str) -> None:
        state = self._loop_health.setdefault(loop_name, {"last_success": 0.0, "consecutive_errors": 0})
        state["last_success"] = time.time()
        state["consecutive_errors"] = 0

        if loop_name == "orderbooks":
            self._last_books_snapshot_ts = state["last_success"]

            # P0A: fournir un timestamp "dernier book reçu" en ms pour éviter
            #      SIGNAL_MISSING_BOOK_AGE dans _latest_book_age_s_with_missing().
            try:
                ts_ms = int(state["last_success"] * 1000.0)
                self.books_last_recv_ts_ms = ts_ms
                self.last_orderbook_recv_ts_ms = ts_ms
            except Exception:
                pass

            self._maybe_update_trading_ready()

        elif loop_name == "balances":
            self._last_balances_ts = state["last_success"]
            self._maybe_update_trading_ready()

    def _mark_loop_error(self, loop_name: str, exc: Exception) -> None:
        state = self._loop_health.setdefault(loop_name, {"last_success": 0.0, "consecutive_errors": 0})
        state["consecutive_errors"] = state.get("consecutive_errors", 0) + 1
        if state["consecutive_errors"] >= self._loop_error_budget:
            msg = f"loop {loop_name} failed {self._loop_error_budget}x: {exc}"
            logger.warning(msg)
            self._submodule_event({
                "module": "RM",
                "level": "WARNING",
                "event": "loop_stalled",
                "loop": loop_name,
                "message": msg,
            })
            state["consecutive_errors"] = 0

    def _maybe_update_trading_ready(self) -> None:
        cfg = getattr(self, "cfg", None)
        rm_cfg = getattr(cfg, "rm", cfg)
        g_cfg = getattr(cfg, "g", None)

        now = time.time()

        # -------------------- deps readiness --------------------
        engine_ready = False
        eng = getattr(self, "engine", None)
        if eng and getattr(eng, "ready_event", None):
            try:
                engine_ready = eng.ready_event.is_set()
            except Exception:
                engine_ready = False

        book_age = now - self._last_books_snapshot_ts if self._last_books_snapshot_ts else 1e9
        bal_age = now - self._last_balances_ts if self._last_balances_ts else 1e9
        max_book_age = float(getattr(self, "max_book_age_s", 1.0) or 1.0)
        bal_ttl = float(getattr(self, "_balance_ttl_s_normal", 60.0) or 60.0)

        books_ready = book_age <= max_book_age
        balances_ready = bal_age <= bal_ttl

        balances_status = None
        balances_reason_code = None
        mbf = getattr(self, "balance_fetcher", None)
        if mbf and hasattr(mbf, "get_balances_freshness_status"):
            try:
                freshness = mbf.get_balances_freshness_status() or {}
                balances_status = str(freshness.get("status") or "").upper()
                balances_reason_code = freshness.get("reason_code")
                if balances_status in ("UNKNOWN", "BLOCK"):
                    balances_ready = False
            except Exception:
                balances_ready = False

        scanner_required = bool(getattr(rm_cfg, "trading_ready_require_scanner_hook", False))
        scanner_ready = True
        if scanner_required:
            scanner_ready = callable(getattr(self, "_scanner_consumer", None))

        # Slippage readiness (fail-closed)
        slip_ready = True
        slip_reason = "slip_unknown_or_stale"
        try:
            slip_ttl_s = float(getattr(getattr(self.bot_cfg, "slip", None), "ttl_s",
                                       float(getattr(self.cfg, "SLIP_SNAPSHOT_TTL_S", 2.0))))
        except Exception:
            slip_ttl_s = float(float(getattr(self.cfg, "SLIP_SNAPSHOT_TTL_S", 2.0)))

        slip = getattr(self, "slippage_handler", None)
        if slip and hasattr(slip, "get_status"):
            try:
                slip_status = slip.get_status() or {}
                slip_age = slip_status.get("age_s")
                if slip_age is None or (isinstance(slip_age, float) and not math.isfinite(slip_age)):
                    slip_ready = False
                else:
                    slip_ready = float(slip_age) <= float(slip_ttl_s)
            except Exception:
                slip_ready = False
        else:
            slip_ready = False

        # Volatility readiness (fail-closed)
        vol_ready = True
        vol_reason = "vol_unknown_or_stale"
        try:
            vol_ttl_s = float(
                getattr(getattr(self.bot_cfg, "vol", None), "ttl_s", None)
                or self._get_rm_knob("vol_snapshot_ttl_s", legacy="VOL_SNAPSHOT_TTL_S", default=5.0)
            )
        except Exception:
            vol_ttl_s = float(self._get_rm_knob("vol_snapshot_ttl_s", legacy="VOL_SNAPSHOT_TTL_S", default=5.0))

        vm = getattr(self, "vol_manager", None)
        if vm and hasattr(vm, "get_current_metrics"):
            try:
                vol_meta = vm.get_current_metrics(None) or {}
                vol_age = vol_meta.get("age_s", vol_meta.get("last_age_s"))
                if vol_age is None or (isinstance(vol_age, float) and not math.isfinite(vol_age)):
                    vol_ready = False
                else:
                    vol_ready = float(vol_age) <= float(vol_ttl_s)
            except Exception:
                vol_ready = False
        else:
            vol_ready = False

        readiness_reasons: List[str] = []
        if not engine_ready:
            readiness_reasons.append("engine_not_ready")
        if not books_ready:
            readiness_reasons.append("books_stale")
        if not balances_ready:
            readiness_reasons.append("balances_stale")
        if balances_status in ("UNKNOWN", "BLOCK"):
            reason_code = normalize_reason_code(balances_reason_code or "BALANCE_STALE") or "BALANCE_STALE"
            if reason_code not in readiness_reasons:
                readiness_reasons.append(reason_code)
                inc_blocked("rm", reason_code, None)
        if not scanner_ready:
            readiness_reasons.append("scanner_hook_missing")
        if not slip_ready:
            if slip_reason not in readiness_reasons:
                readiness_reasons.append(slip_reason)
                inc_blocked("rm", slip_reason, None)
        if not vol_ready:
            if vol_reason not in readiness_reasons:
                readiness_reasons.append(vol_reason)
                inc_blocked("rm", vol_reason, None)

        # -------------------- unified trading_state integration --------------------
        unified = bool(getattr(rm_cfg, "ff_trading_state_unified", False))
        blocked_reason = None
        degraded_reason = None

        if unified:
            # BLOCKED si private plane critique ou logging critique
            pws_status = getattr(self, "_private_ws_status", {}) or {}
            pws_health = pws_status.get("pws_health") if isinstance(pws_status, dict) else {}
            pws_critical = bool((pws_health or {}).get("critical_drop_seen") or getattr(self, "_pws_critical_drop_seen", False))
            if pws_critical:
                blocked_reason = normalize_reason_code(
                    (pws_health or {}).get("last_critical_drop_reason")
                    or getattr(self, "_pws_critical_drop_reason", None)
                    or "PWS_QUEUE_BACKPRESSURE_TIMEOUT"
                ) or "PWS_QUEUE_BACKPRESSURE_TIMEOUT"

            lhm_health = self._collect_logging_health() or {}
            lhm_critical = bool(lhm_health.get("critical_drop_seen"))
            if not blocked_reason and lhm_critical:
                blocked_reason = normalize_reason_code(
                    lhm_health.get("last_critical_drop_reason") or "LOGGERH_JSONL_QUEUE_FULL"
                ) or "LOGGERH_JSONL_QUEUE_FULL"

            lhm_cfg = getattr(cfg, "lhm", None)
            truth_fail_closed = bool(getattr(lhm_cfg, "ff_truth_fail_closed", False))
            if not blocked_reason and truth_fail_closed and not bool(lhm_health.get("logging_persistence_ok", True)):
                blocked_reason = normalize_reason_code("TRUTH_PERSISTENCE_FAILED") or "TRUTH_PERSISTENCE_FAILED"

            if not blocked_reason and readiness_reasons:
                degraded_reason = readiness_reasons[0]

            if blocked_reason:
                self._set_trading_state("BLOCKED", reason=blocked_reason)
            elif degraded_reason:
                self._set_trading_state("DEGRADED", reason=degraded_reason)
            else:
                self._set_trading_state("READY", reason=None)

        # -------------------- PIPELINE_READY vs TRADING_ALLOWED --------------------
        pipeline_ready = (not readiness_reasons)

        # Si BLOCKED (private plane / truth), pipeline doit être considéré non prêt.
        try:
            tstate = str(getattr(self, "trading_state", "READY") or "READY").upper()
        except Exception:
            tstate = "READY"
        if tstate == "BLOCKED":
            pipeline_ready = False
            if "blocked" not in readiness_reasons:
                readiness_reasons.insert(0, "blocked")

        # Trading allowed (live) = pipeline_ready + flags (NO dry_run, NO kill, armed)
        dry_run = bool(getattr(rm_cfg, "dry_run", False))
        kill_switch = bool(getattr(rm_cfg, "global_kill_switch", False)) or bool(getattr(self, "global_kill_switch", False))

        fs = getattr(g_cfg, "feature_switches", {}) if g_cfg else {}
        live_mode = (str(getattr(g_cfg, "mode", "DRY_RUN")).upper() == "PROD") or bool(fs.get("engine_real", False))
        armed = bool(getattr(g_cfg, "live_trading_armed", False))
        armed_effective = bool(live_mode and armed)

        trading_allowed = (not dry_run) and (not kill_switch) and armed_effective
        trading_ready = bool(pipeline_ready and trading_allowed and tstate == "READY")

        # -------------------- publish events --------------------
        if pipeline_ready:
            self.pipeline_ready_event.set()
        else:
            self.pipeline_ready_event.clear()

        if trading_ready:
            self.trading_ready_event.set()
        else:
            self.trading_ready_event.clear()

        # -------------------- store & metrics --------------------
        self._readiness.update({
            "engine": engine_ready,
            "books": books_ready,
            "balances": balances_ready,
            "scanner": scanner_ready,
            "slippage": slip_ready,
            "volatility": vol_ready,
            "pipeline_ready": bool(pipeline_ready),
            "trading_allowed": bool(trading_allowed),
            "trading_ready": bool(trading_ready),
            "reasons": list(readiness_reasons),
        })

        # Dep readiness metrics (extended)
        safe_set(RM_DEP_READY, "rm_dep_ready", "rm", 1.0 if engine_ready else 0.0, dep="engine")
        safe_set(RM_DEP_READY, "rm_dep_ready", "rm", 1.0 if books_ready else 0.0, dep="books")
        safe_set(RM_DEP_READY, "rm_dep_ready", "rm", 1.0 if balances_ready else 0.0, dep="balances")
        safe_set(RM_DEP_READY, "rm_dep_ready", "rm", 1.0 if scanner_ready else 0.0, dep="scanner")
        safe_set(RM_DEP_READY, "rm_dep_ready", "rm", 1.0 if slip_ready else 0.0, dep="slippage")
        safe_set(RM_DEP_READY, "rm_dep_ready", "rm", 1.0 if vol_ready else 0.0, dep="volatility")

        safe_set(RM_PIPELINE_READY, "rm_pipeline_ready", "rm", 1.0 if pipeline_ready else 0.0)
        safe_set(RM_TRADING_READY, "rm_trading_ready", "rm", 1.0 if trading_ready else 0.0)

    def _audit_effective_config(self, *, strict: bool = False) -> Dict[str, Any]:
        cfg = getattr(self, "cfg", None)
        rm_cfg = getattr(cfg, "rm", cfg)

        entries: List[Dict[str, Any]] = []

        def _add(key: str, value: Any, default: Any) -> None:
            present = hasattr(rm_cfg, key)
            if strict and not present:
                raise RuntimeError(f"missing config key {key}")
            entries.append({
                "key": key,
                "value": value,
                "default": default,
                "used_default": (not present) or value == default,
                "present": present,
            })

        _add("max_book_age_s", getattr(self, "max_book_age_s", 1.0), 1.0)
        _add("balance_ttl_s_normal", getattr(self, "_balance_ttl_s_normal", 60.0), 60.0)
        _add("cb_timeout_s", float(getattr(rm_cfg, "cb_timeout_s", 0.25) or 0.25), 0.25)
        _add("cb_max_inflight", int(getattr(rm_cfg, "cb_max_inflight", 200) or 200), 200)
        _add("cb_executor_workers", int(getattr(rm_cfg, "cb_executor_workers", 2) or 2), 2)
        _add("trading_ready_require_scanner_hook", bool(getattr(rm_cfg, "trading_ready_require_scanner_hook", False)),
             False)
        _add(
            "fallback_on_tick_exception",
            str(getattr(rm_cfg, "fallback_on_tick_exception", "CONSTRAINED")).upper(),
            "CONSTRAINED",
        )
        knobs = getattr(self, "_switch_knobs", None)
        if knobs is not None:
            default_knobs = type(knobs)()
            for field in fields(default_knobs):
                key = f"switch_knobs.{field.name}"
                _add(key, getattr(knobs, field.name), getattr(default_knobs, field.name))
        policy = getattr(self, "_signal_policy", None)
        if policy is not None:
            default_policy = type(policy)()
            for field in fields(default_policy):
                key = f"signal_policy.{field.name}"
                _add(key, getattr(policy, field.name), getattr(default_policy, field.name))

        payload= {
            "module": "RM",
            "event": "config_audit",
            "ts_ms": int(time.time() * 1000),
            "entries": entries,
            "strict": bool(strict),
        }
        try:
            payload["snapshot_hash"] = hashlib.sha1(
                json.dumps(entries, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()
        except Exception:
            payload["snapshot_hash"] = None
        return payload




    async def _forward_rebalancing_trade(self, op: Dict[str, Any]) -> None:
        cross = op.get("cross") or op  # accepte soit l'op brut Engine, soit le champ "cross"
        # Routes autorisées
        buy_ex = str((cross.get("to_exchange") or cross.get("buy_exchange") or "")).upper()
        sell_ex = str((cross.get("from_exchange") or cross.get("sell_exchange") or "")).upper()
        if buy_ex and sell_ex and (buy_ex, sell_ex) not in self.allowed_routes:
            logger.info("[RiskManager] rebalancing_trade %s→%s bloqué par allowed_routes", sell_ex, buy_ex)
            return

        # Validation no-loss (net >= -allow_loss_bps)
        if not self._validate_rebalancing_cross({
            "to_exchange": buy_ex,
            "from_exchange": sell_ex,
            "pair_key": (cross.get("pair_key") or cross.get("pair"))
        }):
            logger.info("[RiskManager] rebalancing_trade rejeté (no-loss check)")
            return

        # Route vers l'Engine
        if self.engine is None:
            logger.info("[RiskManager] (dry-route rebalancing_trade) %s", op)
            return
        try:
            r = self.engine_enqueue_bundle(
                op if op.get("type") == "rebalancing_trade" else {"type": "rebalancing_trade", "cross": cross})
            if inspect.isawaitable(r):
                await r
        except Exception:
            logger.exception("[RiskManager] engine.submit(rebalancing_trade) failed")

    def _on_internal_transfer_event(self, ev: Dict[str, Any]) -> None:
        """Traite un event de transfert interne provenant du PrivateWSHub.

        Contrat minimal attendu (cf. private_ws_hub._emit_transfer_event):
          - type: "transfer" (ou "pws_transfer")
          - subtype: "wallet" | "subaccount"
          - exchange: CEX
          - alias: alias focus (souvent alias destination)
          - status: "OK" | "ERROR"
          - payload: dict incluant au minimum:
                - ccy: devise transférée
                - amount: montant
                - alias / from_alias / to_alias (suivant subtype)

        Rôle côté RM:
          - pour les transferts > RM_CAPITAL_MOVE_THRESHOLD_USDC (en notional
            USDC approximatif), marquer les alias impactés comme "capital en
            mouvement" et demander un resync MBF ciblé sur ces alias.
          - exposer des hooks d'observabilité pour mesurer la latence de
            visibilité (event -> première balance fraîche).
        """
        if not ev:
            return

        try:
            status = str(ev.get("status") or "").upper()
            etype = str(ev.get("type") or "").lower()
            subtype = str(ev.get("subtype") or "").lower()
        except Exception:
            return


        if etype not in ("transfer", "pws_transfer"):
            return

        exchange = str(ev.get("exchange") or ev.get("ex") or "").upper()
        if not exchange:
            return

        payload = ev.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {}
        try:
            transfer_id = payload.get("transfer_id") or ev.get("transfer_id")
            if not transfer_id:
                logger.warning("[RiskManager] transfer event missing transfer_id: %s", ev)
                return
            existing_state = {}
            try:
                existing_state = self._transfer_controller._states.get(transfer_id) or {}
            except Exception:
                existing_state = {}
            expires_ts_ms = existing_state.get("expires_ts_ms")
            if expires_ts_ms is None:
                try:
                    expires_ts_ms = int(time.time() * 1000 + (self._transfer_controller._submitted_timeout_s * 1000.0))
                except Exception:
                    expires_ts_ms = None
            if status in ("SETTLED", "COMPLETED") and bool(payload.get("pws_transfer")):
                self._transfer_controller.mark_settled(transfer_id, payload=payload)
                if self.rebalancing and hasattr(self.rebalancing, "mark_transfer_status"):
                    self.rebalancing.mark_transfer_status(transfer_id, "SETTLED")
                self._prune_transfer_tracking(transfer_id)
            elif status in ("OK", "SUCCESS"):
                self._transfer_controller.mark_submitted(
                    transfer_id,
                    payload=payload,
                    expires_ts_ms=expires_ts_ms,
                )
                if self.rebalancing and hasattr(self.rebalancing, "mark_transfer_status"):
                    self.rebalancing.mark_transfer_status(transfer_id, "SUBMITTED")
                self._prune_transfer_tracking(transfer_id)
            elif status in ("ERROR", "FAILED", "REJECTED"):
                self._transfer_controller.mark_failed(transfer_id, payload=payload, error=str(ev.get("error")))
                if self.rebalancing and hasattr(self.rebalancing, "mark_transfer_status"):
                    self.rebalancing.mark_transfer_status(transfer_id, status)
        except Exception:
            pass

        # On ne traite que les transferts effectivement exécutés côté CEX.
        if status not in ("OK", "SUCCESS", "SETTLED", "COMPLETED"):
            return

        try:
            ccy = str(payload.get("ccy") or "USDC").upper()
        except Exception:
            ccy = "USDC"

        try:
            amount = float(payload.get("amount") or 0.0)
        except Exception:
            amount = 0.0

        if amount <= 0.0:
            return

        impacted_aliases: list[str] = []

        if subtype == "wallet":
            alias = str(payload.get("alias") or ev.get("alias") or "").upper()
            if alias:
                impacted_aliases.append(alias)
        elif subtype == "subaccount":
            from_alias = str(payload.get("from_alias") or "").upper()
            to_alias = str(payload.get("to_alias") or "").upper()
            if not from_alias or not to_alias:
                logger.warning("[RiskManager] transfer event missing aliases: %s", ev)
                return
            if from_alias:
                impacted_aliases.append(from_alias)
            if to_alias and to_alias != from_alias:
                impacted_aliases.append(to_alias)
        else:
            alias = str(ev.get("alias") or "").upper()
            if alias:
                impacted_aliases.append(alias)

        if not impacted_aliases:
            return

        # Approximation simple du notional en USDC:
        # - si ccy=USDC: 1:1
        # - sinon: on garde amount tel quel (les cross seront affinés via un
        #   prix de marché dans une évolution ultérieure).
        if ccy == "USDC":
            notional_usdc = float(amount)
        else:
            notional_usdc = float(amount)

        threshold = float(getattr(self, "_capital_move_threshold_usdc", 0.0) or 0.0)
        if threshold > 0.0 and notional_usdc < threshold:
            # En-dessous du seuil, on ne déclenche pas de surcouche "capital en
            # mouvement" (mais le prochain refresh MBF mettra quand même à jour
            # les balances).
            return

        now = time.time()
        max_delay_s = float(
            getattr(self, "_capital_move_refresh_max_delay_s", 15.0) or 15.0
        )

        # Normalise la liste d'alias (UPPER, uniques).
        aliases_norm: list[str] = []
        seen: set[str] = set()
        for alias in impacted_aliases:
            a = str(alias or "").upper()
            if not a or a in seen:
                continue
            seen.add(a)
            aliases_norm.append(a)

        if not aliases_norm:
            return

        # Ticket P0-RM-CAPMOVE-01: Ajout d'une réservation si c'est un débit potentiel pour un alias source
        if transfer_id:
            res_key = str(transfer_id)
            if subtype == "subaccount":
                # Pour un transfert inter-sous-comptes, on réserve sur l'alias source
                src_alias = str(payload.get("from_alias") or "").upper()
                if src_alias:
                    self._reserved_balances[res_key] = {
                        "exchange": exchange,
                        "alias": src_alias,
                        "ccy": ccy,
                        "amount": amount,
                        "deadline": now + max_delay_s,
                        "start_ts": now,
                    }
                    self._sync_reserved_metrics()
                    logger.info("[RiskManager] Réservation balance (subaccount) active: %s.%s %f %s (tid=%s)",
                                exchange, src_alias, amount, ccy, transfer_id)
            elif subtype == "wallet":
                # Pour un transfert inter-wallets, on réserve si on sort du wallet préféré (généralement SPOT)
                alias = str(payload.get("alias") or ev.get("alias") or "").upper()
                from_w = str(payload.get("from_wallet") or "").upper()
                # On assume que SPOT est le wallet de trading principal
                if alias and from_w in ("SPOT", "TRADE", "TRADING", "MAIN"):
                    self._reserved_balances[res_key] = {
                        "exchange": exchange,
                        "alias": alias,
                        "ccy": ccy,
                        "amount": amount,
                        "deadline": now + max_delay_s,
                        "start_ts": now,
                    }
                    logger.info("[RiskManager] Réservation balance (wallet) active: %s.%s %f %s (tid=%s)",
                                exchange, alias, amount, ccy, transfer_id)

        # Enregistre l'état local + demande de resync ciblé via MBF.
        for alias_u in aliases_norm:
            key = (exchange, alias_u)
            try:
                state = self._alias_capital_move_state.get(key, {})  # type: ignore[attr-defined]
            except Exception:
                state = {}

            state.update(
                {
                    "start_ts": now,
                    "deadline_ts": now + max_delay_s,
                    "last_notional_usdc": float(notional_usdc),
                    "subtype": subtype or "unknown",
                    "source": "pws_transfer",
                }
            )
            transfer_ids = set(state.get("transfer_ids") or [])
            if transfer_id:
                transfer_ids.add(str(transfer_id))
            if transfer_ids:
                state["transfer_ids"] = list(transfer_ids)
            payloads = state.get("transfer_payloads") or {}
            if transfer_id:
                payloads[str(transfer_id)] = payload
            if payloads:
                state["transfer_payloads"] = payloads
            try:
                # Enregistre/écrase l'état pour cet alias.
                self._alias_capital_move_state[key] = state  # type: ignore[attr-defined]
            except Exception:
                # En cas d'erreur inattendue sur le cache interne, on ne casse
                # pas le flux RM mais on loggue.
                logging.exception("RM: échec maj _alias_capital_move_state")
            if transfer_id:
                try:
                    self._transfer_alias_index.setdefault(str(transfer_id), set()).add(key)
                except Exception:
                    logging.exception("RM: échec maj _transfer_alias_index")

            # Demande explicite de resync MBF sur (exchange, alias).
            try:
                self._schedule_balance_resync_for_alias(exchange, alias_u)
            except Exception:
                logging.exception(
                    "RM: échec schedule_balance_resync_for_alias ex=%s alias=%s",
                    exchange,
                    alias_u,
                )

        # Hook d'observabilité (no-op si non câblé côté obs_metrics).
        try:
            self._obs_capital_move_event(
                exchange=exchange,
                aliases=aliases_norm,
                notional_usdc=float(notional_usdc),
                subtype=subtype or "unknown",
                source="pws_transfer",
                status="EMITTED",
            )
        except Exception:
            # On n'interrompt jamais la logique RM pour un problème de métriques.
            logging.exception("RM: erreur _obs_capital_move_event")


    async def _exec_internal_wallet_transfer(self, op: Dict[str, Any]) -> None:
        ex = str(op.get("exchange")).upper()
        if not self._transfer_region_allowed(ex):
            logger.warning("[RiskManager] wallet transfer blocked by region policy for %s", ex)
            return
        alias = str(op.get("alias") or op.get("account_alias") or "").upper()
        from_alias = str(op.get("from_alias") or "").upper()
        to_alias = str(op.get("to_alias") or "").upper()
        if from_alias and to_alias and from_alias != to_alias:
            logger.warning(
                "[RiskManager] wallet transfer alias mismatch (%s != %s) for %s",
                from_alias,
                to_alias,
                ex,
            )
            return
        if not alias:
            alias = from_alias or to_alias
        if not alias:
            logger.warning("[RiskManager] wallet transfer missing alias for %s", ex)
            return
        from_wallet = str(op.get("from_wallet") or "SPOT").upper()
        to_wallet = str(op.get("to_wallet") or "FUNDING").upper()
        ccy = str(op.get("ccy") or "USDC").upper()
        amount = float(op.get("amount") or op.get("amount_usdc") or 0.0)
        client = self.transfer_clients.get(ex)
        if not client:
            logger.warning("[RiskManager] no transfer client for %s", ex)
            return
        fn = None
        for name in ("transfer_wallet", "internal_wallet_transfer", "wallet_transfer"):
            if hasattr(client, name):
                fn = getattr(client, name)
                break
        if not fn:
            logger.warning("[RiskManager] client %s missing transfer_wallet(..)", ex)
            return
        try:
            async def _submit():
                res = fn(ex=ex, alias=alias, from_wallet=from_wallet, to_wallet=to_wallet, ccy=ccy, amount=amount)
                if inspect.isawaitable(res):
                    return await res
                return res

            payload = {
                "type": "internal_wallet_transfer",
                "exchange": ex,
                "alias": alias,
                "from_alias": alias,
                "to_alias": alias,
                "from_wallet": from_wallet,
                "to_wallet": to_wallet,
                "ccy": ccy,
                "amount": amount,

            }
            if op.get("transfer_bucket") is not None:
                payload["transfer_bucket"] = op.get("transfer_bucket")
            expected_id = TransferController.canonical_transfer_id(payload)
            transfer_id = op.get("transfer_id")
            if not transfer_id:
                logger.warning("[RiskManager] wallet transfer missing transfer_id for %s", ex)
                return
            if transfer_id != expected_id:
                logger.warning(
                    "[RiskManager] wallet transfer_id mismatch (%s != %s) for %s",
                    transfer_id,
                    expected_id,
                    ex,
                )
                return
            payload["transfer_id"] = transfer_id
            outcome = await self._transfer_controller.submit(
                payload=payload,
                submit_fn=_submit,
                venue=f"{ex}:wallet_transfer",
            )
            if outcome.get("status") == "SUBMITTED":
                self._alert(
                    "RiskManager",
                    f"✅ Internal WALLET transfer {ex}[{alias}] {from_wallet}→{to_wallet} {amount} {ccy} "
                    f"(transfer_id={transfer_id})",
                )
        except Exception:
            logger.exception("[RiskManager] internal wallet transfer failed")

    async def _exec_internal_subaccount_transfer(self, op: Dict[str, Any]) -> None:
        ex = str(op.get("exchange")).upper()
        if not self._transfer_region_allowed(ex):
            logger.warning("[RiskManager] subaccount transfer blocked by region policy for %s", ex)
            return
        from_alias = str(op.get("from_alias") or "").upper()
        to_alias = str(op.get("to_alias") or "").upper()
        if not from_alias or not to_alias:
            logger.warning("[RiskManager] subaccount transfer missing aliases for %s", ex)
            return
        ccy = str(op.get("ccy") or "USDC").upper()
        amount = float(op.get("amount") or op.get("amount_usdc") or 0.0)
        transfer_id = op.get("transfer_id")
        if not transfer_id:
            logger.warning("[RiskManager] subaccount transfer missing transfer_id for %s", ex)
            return
        client = self.transfer_clients.get(ex)
        if not client:
            logger.warning("[RiskManager] no transfer client for %s", ex)
            return
        fn = None
        for name in ("transfer_subaccount", "internal_subaccount_transfer", "subaccount_transfer", "universal_sub_transfer"):
            if hasattr(client, name):
                fn = getattr(client, name)
                break
        if not fn:
            logger.warning("[RiskManager] client %s missing transfer_subaccount(..)", ex)
            return
        try:
            async def _submit():
                res = fn(ex=ex, from_alias=from_alias, to_alias=to_alias, ccy=ccy, amount=amount)
                if inspect.isawaitable(res):
                    return await res
                return res

            payload = {
                "type": "internal_subaccount_transfer",
                "exchange": ex,
                "from_alias": from_alias,
                "to_alias": to_alias,
                "ccy": ccy,
                "amount": amount,
                "transfer_id": transfer_id,
            }
            if op.get("transfer_bucket") is not None:
                payload["transfer_bucket"] = op.get("transfer_bucket")
            outcome = await self._transfer_controller.submit(
                payload=payload,
                submit_fn=_submit,
                venue=f"{ex}:subaccount_transfer",
            )
            if outcome.get("status") == "SUBMITTED":
                self._alert(
                    "RiskManager",
                    f"✅ Internal SUBACCOUNT transfer {ex} {from_alias}→{to_alias} {amount} {ccy} "
                    f"(transfer_id={transfer_id})",
                )
                if bool(getattr(self.cfg, "dry_run", False)):
                    self.adjust_virtual_balance(ex, from_alias, ccy, -amount)
                    self.adjust_virtual_balance(ex, to_alias, ccy, +amount)
        except Exception:
            logger.exception("[RiskManager] internal subaccount transfer failed")

    def _make_internal_transfer(self, ex: str, from_alias: str, to_alias: str, amount: float, ccy: str = "USDC") -> Dict[str, Any]:
        now_ms = int(time.time() * 1000)
        payload = {
            "type": "internal_transfer",
            "ts_ms": now_ms,
            "exchange": str(ex).upper(),
            "from_alias": from_alias,
            "to_alias": to_alias,
            "ccy": str(ccy).upper(),
            "amount": float(amount),
            "amount_usdc": float(amount) if str(ccy).upper() == "USDC" else 0.0, # Compat legacy
            "meta": {"source": "RebalancingManager", "kind": "REB_TRANSFER"},
        }
        payload["transfer_id"] = TransferController.canonical_transfer_id(payload)
        return payload

    def _make_cross_cex_opportunity(self, plan: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        x = plan.get("cross_cex")
        if not x:
            return None

        now_ms = int(time.time() * 1000)
        pair_key = self._norm_pair(x.get("pair_key") or (self.symbols[0] if self.symbols else "ETHUSDC"))
        quote = _pair_quote(pair_key)

        buy_ex = str(x.get("to_exchange") or "").upper()
        sell_ex = str(x.get("from_exchange") or "").upper()
        buy_alias = str(x.get("buy_alias") or "TT").upper()
        sell_alias = str(x.get("sell_alias") or "TT").upper()

        vol_usdc = float(x.get("amount_usdc") or 0.0)
        if vol_usdc <= 0:
            return None

        # Borne par cash acheteur (sécurité) avec un pad de 95%
        cash_buy = float(self._available_quote(buy_ex, buy_alias, quote))
        cap = max(0.0, cash_buy * 0.95)
        vol_usdc = min(vol_usdc, cap)

        # Borne par minNotional de l'exchange acheteur (devise de cotation)
        min_notional = float(self.compute_cex_min_notional_usdc(buy_ex, pair_key) or 0.0)
        if min_notional > 0 and vol_usdc < min_notional:
            vol_usdc = min_notional

        if vol_usdc <= 0:
            return None

        return {
            "type": "rebalancing_trade",
            "ts_ms": now_ms,
            "pair": pair_key,
            "buy_exchange": buy_ex,
            "sell_exchange": sell_ex,
            "buy_alias": buy_alias,
            "sell_alias": sell_alias,
            "quote": quote,
            "payload": {
                "legs": [
                    {"exchange": buy_ex, "account_alias": buy_alias, "side": "BUY", "symbol": pair_key,
                     "volume_usdc": float(vol_usdc)},
                    {"exchange": sell_ex, "account_alias": sell_alias, "side": "SELL", "symbol": pair_key,
                     "volume_usdc": float(vol_usdc)},
                ]
            },
            "meta": {
                "source": "RebalancingManager",
                "strategy": "TM",  # neutre 100% (ou TT fallback Engine, voir patch Engine)
                "tm_mode": "NEUTRAL",
                "hedge_ratio": 1.0,
                "allow_loss_bps": float(self.rebal_allow_loss_bps),
            },
        }

    def _make_rebalancing_actions(self, plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        actions: List[Dict[str, Any]] = []
        for tr in plan.get("internal_transfers", []) or []:
            amt = float(tr.get("amount") or tr.get("amount_usdc") or 0.0)
            ccy = str(tr.get("ccy") or "USDC").upper()
            actions.append(
                self._make_internal_transfer(
                    tr.get("exchange"), 
                    str(tr.get("from_alias")), 
                    str(tr.get("to_alias")),
                    amt,
                    ccy=ccy
                )
            )
        x = self._make_cross_cex_opportunity(plan)
        if x:
            actions.append(x)
        return actions

    def is_rebalancing_locked(self, pair_key: str, buy_exchange: str, sell_exchange: str) -> bool:
        if not hasattr(self, "_reb_locks"):
            self._reb_locks = {}
        pk = self._norm_pair(pair_key)
        quote = _pair_quote(pk)
        combo_key = f"{pk}|{quote}|{str(buy_exchange).upper()}->{str(sell_exchange).upper()}"
        expiry = float(self._reb_locks.get(combo_key, 0.0) or 0.0)
        now = time.monotonic()
        if expiry <= now:
            if combo_key in self._reb_locks:
                self._reb_locks.pop(combo_key, None)
            return False
        return True

    async def _emit_rebalancing_opportunity(self, opp: Dict[str, Any]) -> None:
        cb = self._rebalancing_cb
        if not cb:
            return
        try:
            r = cb(opp)
            if inspect.isawaitable(r):
                await r
        except Exception:
            logger.exception("[RiskManager] rebalancing callback failed")

    # ------------------------------------------------------------------
    # Inventaire / skew / temps (helpers) — **multi-comptes**
    # ------------------------------------------------------------------

    def _norm_pair(self, s: str) -> str:
        return (s or "").replace("-", "").upper()

    @staticmethod
    def _pair_base(pair_key: str) -> str:
        return _strip_quote(pair_key)

    def _mid_price_usdc(self, pair_key: str) -> float:
        # NOTE: retourne le mid **dans la devise de cotation** (USDC ou EUR)
        pk = self._norm_pair(pair_key)
        books = self._last_books or {}
        for ex in books.keys():
            b = books.get(ex, {}).get(pk, {}) or {}
            bid = float(b.get("best_bid") or 0.0)
            ask = float(b.get("best_ask") or 0.0)
            if bid > 0 and ask > 0:
                return (bid + ask) / 2.0
        return 0.0

    def _balances_snapshot(self) -> Dict[str, Dict[str, Dict[str, float]]]:
        """
        Vue capitale unifiée RM/Rebal pour les décisions d’inventaire/caps.

        Optimisation P1 (HFT) : 
        - Utilise un cache read-only rafraîchi toutes les 200ms.
        - Supprime les normalisations UPPER redondantes (déjà gérées par MBF).
        - Intègre les débits virtuels (réservations) de manière efficace.
        """
        now = time.time()
        # P1: Fast-path cache (200ms)
        if (now - self._last_balances_ts) < self._balances_cache_interval_s and self._balances_cache:
            return self._balances_cache

        balances: Dict[str, Dict[str, Dict[str, float]]] = {}

        # 1) Source principale : MBF (owner data-plane)
        glue = getattr(self, "_mbf_glue", None)
        if glue is not None:
            try:
                # Utilise la vue optimisée du MBF
                raw = glue.snapshot(cached_only=True)
            except Exception:
                try:
                    logger.exception("RM×MBF: snapshot() failed", exc_info=True)
                except Exception:
                    pass
                raw = {}

            if isinstance(raw, dict) and raw:
                # format as_rm_snapshot : {"balances":{...},"meta":{...}}
                balances = raw.get("balances") or {}
                if not balances:
                    # fallback legacy
                    balances = {
                        ex: per for ex, per in raw.items()
                        if ex != "meta" and isinstance(per, dict)
                    }

        # 2) DRY-RUN pur : overlay virtuel RM si MBF ne fournit rien
        if not balances and bool(getattr(self.cfg, "dry_run", False)) and getattr(self, "_virtual_balances", None):
            balances = getattr(self, "_virtual_balances", {}) or {}

        # 3) Fallback legacy : cache RebalancingManager
        if not balances:
            balances = getattr(self.rebalancing, "latest_balances", {}) or {}

        # Normalisation légère (MBF est déjà en UPPER, on assure juste le type float)
        # On évite de recréer TOUT le dictionnaire si possible, mais on a besoin d'une copie
        # pour appliquer les réservations sans polluer la source.
        normalized: Dict[str, Dict[str, Dict[str, float]]] = {}
        for ex, per_alias in balances.items():
            exu = str(ex).upper()
            dst_ex = normalized.setdefault(exu, {})
            for alias, assets in per_alias.items():
                alu = str(alias).upper()
                dst_ex[alu] = {str(ccy).upper(): float(v or 0.0) for ccy, v in assets.items()}

        # Ticket P0-RM-CAPMOVE-01: Déduction des réservations actives (virtual debit)
        reserved_changed = False
        for tid, res in list(self._reserved_balances.items()):
            # 1) Expiration TTL
            if now > res.get("deadline", 0):
                del self._reserved_balances[tid]
                reserved_changed = True
                continue

            # 2) Libération si balance rafraîchie
            ex = res["exchange"]
            alias = res["alias"]
            age = self._alias_balance_age_s.get((ex, alias))
            if age is not None:
                snap_ts = now - age
                if snap_ts > res.get("start_ts", 0):
                    del self._reserved_balances[tid]
                    reserved_changed = True
                    continue
            
            # 3) Application du débit virtuel
            ccy = res["ccy"]
            if ex in normalized and alias in normalized[ex]:
                current = normalized[ex][alias].get(ccy, 0.0)
                normalized[ex][alias][ccy] = max(0.0, current - float(res["amount"]))

        if reserved_changed:
            self._sync_reserved_metrics()

        # Mise à jour du cache
        self._balances_cache = normalized
        self._last_balances_ts = now

        return normalized

    def _mm_compute_inventory_drift(
            self,
            ex: str,
            alias_mm: str,
            asset: str,
            balances_snapshot: Dict[str, Dict[str, Dict[str, float]]],
    ) -> tuple[float, float]:
        """Calcule la dérive inventaire MM pour (ex, alias, asset).

        P0 : cible d'inventaire = 0 → drift_pct reste à 0.0 tant que l'on n'a pas
        de référence plus riche (à étendre dans les tickets delta/collat). L'état
        est donc principalement piloté par drift_usd + paliers.
        """
        try:
            pos_units = float(((balances_snapshot.get(ex, {}) or {}).get(alias_mm, {}) or {}).get(asset, 0.0))
        except Exception:
            pos_units = 0.0

        pair_key = self._norm_pair(f"{asset}USDT")
        try:
            bid, ask = self.get_top_of_book(ex, pair_key, enforce_fresh=False)
        except Exception:
            bid, ask = 0.0, 0.0
        mid = (float(bid) + float(ask)) / 2.0 if (bid and ask) else 0.0

        drift_usd = float(pos_units) * float(mid)
        # P0 : cible neutre (0) → drift_pct = 0.0 en attendant une cible enrichie
        drift_pct = 0.0
        return drift_usd, drift_pct

    def _mm_classify_inventory_state(self, drift_usd: float, drift_pct: float, alias_mm: str = "MM") -> str:
        """Classe l'état MM en fonction des seuils drift_usd/pct.

        P0 : si drift_pct est nul (cible = 0), on utilise des paliers USD basés
        sur la magnitude du drift vs mm_reb_inventory_min_notional_usd.
        """
        abs_usd = abs(float(drift_usd))
        abs_pct = abs(float(drift_pct))

        if abs_usd < float(self.mm_reb_inventory_min_notional_usd):
            return "NORMAL"

        al_u = str(alias_mm).upper()
        if al_u == self.mm_mono_alias_name:
            soft = float(self.mm_mono_reb_soft)
            hard = float(self.mm_mono_reb_hard)
            critical = float(self.mm_mono_reb_critical)
        elif al_u == self.mm_cross_alias_name:
            soft = float(self.mm_cross_reb_soft)
            hard = float(self.mm_cross_reb_hard)
            critical = float(self.mm_cross_reb_critical)
        else:
            soft = float(self.mm_reb_inventory_soft_pct)
            hard = float(self.mm_reb_inventory_hard_pct)
            critical = float(self.mm_reb_inventory_critical_pct)

        if abs_pct > 0:
            if abs_pct <= soft:
                return "NORMAL"
            if abs_pct <= hard:
                return "ALERT"
            if abs_pct <= critical:
                return "TENSION"
            return "CRITICAL"

        # Fallback cible neutre : on transpose les seuils pct en paliers USD au-dessus de la base
        base = max(float(self.mm_reb_inventory_min_notional_usd), 1.0)
        soft_usd = base * (1.0 + soft / 100.0)
        hard_usd = base * (1.0 + hard / 100.0)
        critical_usd = base * (1.0 + critical / 100.0)

        if abs_usd <= soft_usd:
            return "NORMAL"
        if abs_usd <= hard_usd:
            return "ALERT"
        if abs_usd <= critical_usd:
            return "TENSION"
        return "CRITICAL"

    def _update_imbalance_ewma(self, exchange: str, pair_key: str, snap: Dict[str, Any]) -> None:
        try:
            asks = snap.get("asks", [])
            bids = snap.get("bids", [])
            if not asks or not bids:
                return

            # On prend le top 3 pour calculer l imbalance
            sum_ask = sum(float(q) for p, q in asks[:3])
            sum_bid = sum(float(q) for p, q in bids[:3])

            if (sum_ask + sum_bid) <= 0:
                return

            imbalance = (sum_bid - sum_ask) / (sum_bid + sum_ask)

            key = (str(exchange).upper(), self._norm_pair(pair_key))
            prev = self._mm_imbalance_ewma.get(key, imbalance)
            alpha = getattr(self, "_mm_imbalance_alpha", 0.3)
            self._mm_imbalance_ewma[key] = (1 - alpha) * prev + alpha * imbalance
        except Exception:
            pass

    def _is_imbalance_adverse(self, exchange: str, pair_key: str, leg_to_disable: str) -> bool:
        """
        Détermine si l imbalance du carnet est défavorable à la cotation unidirectionnelle.
        """
        try:
            key = (str(exchange).upper(), self._norm_pair(pair_key))
            ewma = self._mm_imbalance_ewma.get(key, 0.0)
            thresh = getattr(self, "mm_single_imbalance_threshold", 0.6)

            if leg_to_disable == "BUY":  # On ne cote que SELL
                # Si forte pression BUY, le prix risque de monter -> adverse pour un maker SELL (adverse selection)
                return ewma > thresh
            elif leg_to_disable == "SELL":  # On ne cote que BUY
                # Si forte pression SELL, le prix risque de baisser -> adverse pour un maker BUY
                return ewma < -thresh
        except Exception:
            pass
        return False

    def _compute_mm_inventory_skews(self, ex: str, pair: str, drift_usd: float) -> tuple[float, float]:
        """
        Calcule les skews de prix et de taille basés sur le drift d'inventaire.
        drift_usd > 0 : Trop de base (LONG), on veut vendre.
        drift_usd < 0 : Pas assez de base (SHORT), on veut acheter.
        """
        # Paramètres résolus via capital profile
        target = float(self._get_rm_knob("mm_inv_target_usd", default=0.0))
        band_lo = float(self._get_rm_knob("mm_inv_band_lo_usd", default=500.0))
        band_hi = float(self._get_rm_knob("mm_inv_band_hi_usd", default=2000.0))
        skew_strength_price = float(self._get_rm_knob("mm_skew_strength_price", default=1.0))
        skew_strength_size = float(self._get_rm_knob("mm_skew_strength_size", default=1.0))

        relative_drift = drift_usd - target
        abs_drift = abs(relative_drift)

        if abs_drift < band_lo:
            return 0.0, 1.0  # Neutre

        # Skew linéaire entre band_lo et band_hi
        factor = min(1.0, (abs_drift - band_lo) / (max(1.0, band_hi - band_lo)))

        # Skew prix : relative_drift > 0 (LONG) => skew > 0 => prix plus bas (vendre plus facile)
        price_skew = (relative_drift / band_hi) * factor * skew_strength_price

        # Skew taille : relative_drift > 0 (LONG) => size_skew > 1 => vendre plus gros, acheter plus petit
        size_skew = 1.0 + (relative_drift / band_hi) * factor * skew_strength_size

        return price_skew, size_skew

    def get_mm_leg_restriction(self, ex: str, alias: str, asset: str, pair_key: str = "") -> Optional[str]:
        """
        Renvoie 'BUY' si le leg BUY doit être désactivé (trop Long).
        Renvoie 'SELL' si le leg SELL doit être désactivé (trop Short).
        Renvoie 'BOTH' si la cotation doit être totalement arrêtée (stuck/adverse).
        Utilisé par le Scanner pour le MM bidirectionnel (Mono-CEX ou Cross-CEX).

        Implémente :
        - Hystérésis (inv_band_hi/lo)
        - Escalade temporelle (anti-stuck)
        - Gate imbalance adverse
        """
        ex_u = str(ex).upper()
        al_u = str(alias).upper()
        asset_u = str(asset).upper()
        key = (ex_u, al_u, asset_u)

        # On consulte l'état d'inventaire calculé par la boucle de rebalancing
        state_info = self.mm_reb_state.get(key)
        if not state_info:
            return None

        drift_usd = float(state_info.get("drift_usd", 0.0))
        abs_drift = abs(drift_usd)
        now = time.time()

        # État précédent
        current = self.mm_inventory_mode_state.get(key)
        if not current:
            # Init par défaut
            current = {"mode": "DUAL", "last_transition": now, "phase": 1}
            self.mm_inventory_mode_state[key] = current

        # --- Transitions DUAL <-> SINGLE ---
        if current["mode"] == "DUAL":
            if abs_drift > getattr(self, "inv_band_hi_usd", 250.0):
                if (now - current["last_transition"]) >= getattr(self, "inv_state_min_seconds", 10.0):
                    current["mode"] = "SINGLE"
                    current["last_transition"] = now
                    current["start_drift_usd"] = drift_usd
                    current["max_drift_usd"] = abs_drift
                    current["phase"] = 1
                    current["last_phase_transition"] = now
                    # Metrics transition
                    if hasattr(self, "obs_inc"):
                        self.obs_inc("mm_inventory_mode_transitions_total", old="DUAL", new="SINGLE", asset=asset_u)

        elif current["mode"] == "SINGLE":
            if abs_drift < getattr(self, "inv_band_lo_usd", 100.0):
                if (now - current["last_transition"]) >= getattr(self, "inv_state_min_seconds", 10.0):
                    current["mode"] = "DUAL"
                    current["last_transition"] = now
                    current.pop("start_drift_usd", None)
                    current.pop("phase", None)
                    if hasattr(self, "obs_inc"):
                        self.obs_inc("mm_inventory_mode_transitions_total", old="SINGLE", new="DUAL", asset=asset_u)

        # --- Logique SINGLE active ---
        if current["mode"] == "SINGLE":
            time_in_single = now - current["last_transition"]
            try:
                from modules.obs_metrics import MM_SINGLE_TIME_SECONDS
                if MM_SINGLE_TIME_SECONDS:
                    MM_SINGLE_TIME_SECONDS.labels(asset=asset_u).set(time_in_single)
            except Exception: pass

            leg_to_disable = "BUY" if drift_usd > 0 else "SELL"
            
            # A) Gate Imbalance adverse
            if pair_key and self._is_imbalance_adverse(ex_u, pair_key, leg_to_disable):
                try:
                    if hasattr(self, "obs_inc"):
                        self.obs_inc("mm_single_blocked_total", asset=asset_u, reason="MM_SINGLE_IMB_ADVERSE")
                except Exception: pass
                return "BOTH"

            # B) Escalade Anti-stuck
            current["max_drift_usd"] = max(current.get("max_drift_usd", 0.0), abs_drift)
            
            # Phase 1 -> 2 : Stuck
            if current["phase"] == 1 and time_in_single >= getattr(self, "single_stuck_t1_s", 30.0):
                # Si le drift ne baisse pas significativement (>= 90% du départ)
                if abs_drift >= 0.9 * abs(current.get("start_drift_usd", 0.0)):
                    current["phase"] = 2
                    current["last_phase_transition"] = now
                    if hasattr(self, "obs_inc"):
                        self.obs_inc("mm_single_escalations_total", asset=asset_u, phase="2")

            # Phase 2 -> 3 : Worsening or Max Time
            if current["phase"] == 2 and time_in_single >= getattr(self, "single_stuck_t2_s", 60.0):
                # Si le drift empire (>= 95% du max vu)
                if abs_drift >= 0.95 * current.get("max_drift_usd", 0.0):
                    current["phase"] = 3
                    current["last_phase_transition"] = now
                    if hasattr(self, "obs_inc"):
                        self.obs_inc("mm_single_escalations_total", asset=asset_u, phase="3")
            
            # Phase 3 Hard Stop
            if time_in_single >= getattr(self, "single_max_time_s", 120.0):
                current["phase"] = 3

            if current["phase"] == 3:
                # Trigger REB (self-rebalance agressif)
                self._mm_trigger_self_rebal(ex_u, al_u, asset_u, drift_usd)
                return "BOTH"

            return leg_to_disable

        return None

    @staticmethod
    def _aggregate_by_exchange(snapshot: Dict[str, Dict[str, Dict[str, float]]], asset: str) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for ex, accounts in (snapshot or {}).items():
            out[ex] = sum(float((assets or {}).get(asset, 0.0)) for assets in (accounts or {}).values())
        return out

    def _project_single(self, snapshot: Dict[str, Dict[str, Dict[str, float]]],
                        exchange: str, pair_key: str, side: str, volume_usdc: float,
                        account_alias: Optional[str] = None) -> None:
        # NOTE: volume_usdc == notional **quote** (USDC/EUR)
        base = self._pair_base(pair_key)
        quote = _pair_quote(pair_key)
        mid = self._mid_price_usdc(pair_key) or 0.0
        if mid <= 0.0:
            return
        qty_base = float(volume_usdc) / mid
        ex = str(exchange).upper()
        al = account_alias or "TT"
        snap_ex = snapshot.setdefault(ex, {})
        snap = snap_ex.setdefault(al, {})
        quote_cash = float(snap.get(quote, 0.0))
        base_qty = float(snap.get(base, 0.0))
        if str(side).upper() == "BUY":
            quote_cash -= volume_usdc
            base_qty += qty_base
        else:
            quote_cash += volume_usdc
            base_qty -= qty_base
        snap[quote] = quote_cash
        snap[base] = base_qty

    def _cap_breached(self, snapshot: Dict[str, Dict[str, Dict[str, float]]], base: str, pair_key: str,
                      cap_limit_usd: Optional[float] = None) -> bool:
        # NOTE: cap_limit_usd est traité comme un cap **en devise de cotation**
        mid = self._mid_price_usdc(pair_key) or 0.0
        if mid <= 0.0:
            return False
        cap = float(cap_limit_usd if cap_limit_usd is not None else self.inventory_cap_usd)
        by_ex = self._aggregate_by_exchange(snapshot, base)
        for _ex, qty in by_ex.items():
            if (abs(qty) * mid) > cap:
                return True
        return False

    def _skew_pct(self, snapshot: Dict[str, Dict[str, Dict[str, float]]], base: str, pair_key: str) -> float:
        mid = self._mid_price_usdc(pair_key) or 0.0
        if mid <= 0.0:
            return 0.0
        vals = []
        by_ex = self._aggregate_by_exchange(snapshot, base)
        for _ex, qty in by_ex.items():
            vals.append(abs(qty) * mid)
        if not vals:
            return 0.0
        avg = sum(vals) / max(1, len(vals))
        worst = max(abs(v - avg) for v in vals)
        return (worst / (avg + 1e-9)) * 100.0

    def _strict_share_guard(
        self,
        snapshot: Dict[str, Dict[str, Dict[str, float]]],
        base: str,
        pair_key: str,
        focus_exchange: str,
        skew_limit_pct: Optional[float] = None,
    ) -> bool:
        mid = self._mid_price_usdc(pair_key) or 0.0
        if mid <= 0.0:
            return True
        by_ex = self._aggregate_by_exchange(snapshot, base)
        total = sum(abs(qty) * mid for qty in by_ex.values())
        if total <= 0:
            return True
        vals = {ex: abs(qty) * mid for ex, qty in by_ex.items()}
        share = vals.get(focus_exchange, 0.0) / total
        eq_share = 1.0 / max(1, len(vals))
        limit = self.inventory_skew_max_pct if skew_limit_pct is None else float(skew_limit_pct)
        max_share = eq_share * (1.0 + limit / 100.0)
        return share <= max_share + 1e-9

    def _fresh_enough(self, book: dict) -> bool:
        # P0 : Bypass total en DRY_RUN pour ne pas bloquer les calculs de slippage
        if bool(getattr(self.cfg, "dry_run", False)):
            return True
            
        try:
            now_ms = time.time() * 1000.0
            ex_ms = float(book.get("exchange_ts_ms") or 0.0)
            rc_ms = float(book.get("recv_ts_ms") or 0.0)
            if ex_ms > 0:
                if (now_ms - ex_ms) > (self.max_book_age_s * 1000.0):
                    return False
            if ex_ms > 0 and rc_ms > 0:
                if abs(ex_ms - rc_ms) > self.max_clock_skew_ms:
                    return False
        except Exception:
            return True
        return True

    def _now(self) -> float:
        return time.time()

    def penalize_pair(self, pair_key: str, *, reason: str = "transient", ttl_s: Optional[float] = None) -> None:
        """Applique un circuit-breaker temporaire sur la paire (escalade douce).
        - Escalade si pénalisation répétée dans une fenêtre courte.
        - TTL borné par pair_penalty_min_s / pair_penalty_max_s.
        """
        try:
            pk = self._norm_pair(pair_key)
            now = self._now()
            base = float(ttl_s if ttl_s is not None else getattr(self, "pair_penalty_default_ttl_s", 60.0))
            # états internes
            if not hasattr(self, "_pair_penalties"):
                self._pair_penalties = {}
            if not hasattr(self, "_pair_penalty_counts"):
                self._pair_penalty_counts = {}
            if not hasattr(self, "_pair_penalty_last"):
                self._pair_penalty_last = {}
            last = float(self._pair_penalty_last.get(pk, 0.0))
            cnt = int(self._pair_penalty_counts.get(pk, 0))
            window = float(getattr(self.cfg, "pair_penalty_escalation_window_s", 90.0))
            if (now - last) <= window:
                cnt += 1
            else:
                cnt = 1
            self._pair_penalty_counts[pk] = cnt
            self._pair_penalty_last[pk] = now
            factor = _cfg_float(self.cfg, "pair_penalty_escalation", 1.6) * max(0, cnt - 1)
            ttl = base * factor
            ttl = max(ttl, _cfg_float(self.cfg, "pair_penalty_min_s", 15.0))
            ttl = min(ttl, _cfg_float(self.cfg, "pair_penalty_max_s", 900.0))
            self._pair_penalties[pk] = max(self._pair_penalties.get(pk, 0.0), now + ttl)
            try:
                from modules.obs_metrics import PAIR_HEALTH_PENALTY_TOTAL
                PAIR_HEALTH_PENALTY_TOTAL.labels(pk, reason).inc()
            except Exception:
                pass
        except Exception:
            import logging
            logging.exception("RiskManager.penalize_pair failed")

    def _is_pair_penalized(self, pair_key: str) -> bool:
        try:
            pk = self._norm_pair(pair_key)
            return self._now() < float(self._pair_penalties.get(pk, 0.0) or 0.0)
        except Exception:
            return False

    # --------------------------- Execution helpers ---------------------------

    def _get_capital_view_from_mbf(self, *, mode: Optional[str] = None) -> Dict[str, Any]:
        """
        Récupère la vue « buffers » depuis le MultiBalanceFetcher.

        Retourne toujours un dict avec au moins :
          - "pockets_by_quote"
          - "fee_token_levels"
        même en cas d'erreur (valeurs vides).
        """
        view_mode = mode or ("merged" if getattr(self.cfg, "dry_run", False) else "real")

        glue = getattr(self, "_mbf_glue", None)
        if glue is None or not hasattr(glue, "buffers_snapshot"):
            return {"pockets_by_quote": {}, "fee_token_levels": {}}

        try:
            snap = glue.buffers_snapshot(mode=view_mode) or {}
        except Exception:
            logger.debug(
                "RM: erreur lors de _get_capital_view_from_mbf(mode=%s)",
                view_mode,
                exc_info=True,
            )
            return {"pockets_by_quote": {}, "fee_token_levels": {}}

        pockets = snap.get("pockets_by_quote") or {}
        fee_tokens = snap.get("fee_token_levels") or {}
        return {
            "pockets_by_quote": pockets,
            "fee_token_levels": fee_tokens,
        }

    def _compute_capital_available_usdc_from_buffers(
        self,
        *,
        quotes: Tuple[str, ...] = ("USDC", "USD", "USDT"),
        exchanges: Optional[Iterable[str]] = None,
        aliases: Optional[Iterable[str]] = None,
        mode: Optional[str] = None,
    ) -> float:
        """
        Agrège un budget capital global à partir des poches MBF.

        Utilisé pour :
          - alimenter payload["capital_available_usdc"] du simulateur,
          - borner effective_inventory_cap_usd.

        Le budget est exprimé dans les mêmes unités que les poches (quote).
        """
        view = self._get_capital_view_from_mbf(mode=mode)
        pockets = view.get("pockets_by_quote") or {}
        # Ticket P0-RM-CAPMOVE-01: Appliquer les réservations
        pockets = self._apply_reservations_to_pockets(pockets)

        if not pockets:
            return 0.0

        ex_filter = {str(e).upper() for e in exchanges} if exchanges else None
        alias_filter = {str(a).upper() for a in aliases} if aliases else None
        qset = {str(q).upper() for q in (quotes or ())}

        total = 0.0
        for ex, per_alias in pockets.items():
            exu = str(ex).upper()
            if ex_filter and exu not in ex_filter:
                continue

            for alias, per_quote in (per_alias or {}).items():
                alu = str(alias).upper()
                if alias_filter and alu not in alias_filter:
                    continue

                for q, amt in (per_quote or {}).items():
                    qu = str(q).upper()
                    if qset and qu not in qset:
                        continue
                    try:
                        total += float(amt or 0.0)
                    except Exception:
                        continue

        return float(total)

    def _compute_capital_by_ttl_from_buffers(
        self,
        *,
        mode: Optional[str] = None,
        quotes: Tuple[str, ...] = ("USDC", "USD", "USDT"),
    ) -> Dict[str, Any]:
        """
        Agrège le capital disponible par statut TTL (OK/DEGRADED/BLOCKED/UNKNOWN),
        à partir des poches MBF (pockets_by_quote).

        Hypothèses :
        - Les quotes USDC/USD/USDT sont considérées comme "USD-like" et sommées telles quelles.
        - Les statuts TTL (OK/DEGRADED/BLOCKED/UNKNOWN) proviennent du cache local
          self._alias_balance_status[(EX,ALIAS)] + self._alias_balance_age_s[(EX,ALIAS)].

        Retourne un dict de la forme :
        {
          "by_status": { "OK": float, "DEGRADED": float, "BLOCKED": float, "UNKNOWN": float },
          "by_status_quote": { "OK": {"USDC": float, ...}, ... },
          "by_alias": {
            "BINANCE.TT": {
               "status": "OK",
               "age_s": 12.3,
               "by_quote": {"USDC": 123.0, "USDT": 45.0}
            },
            ...
          },
        }
        """
        # On s'assure que le cache TTL est rafraîchi à partir de MBF.as_rm_snapshot()
        try:
            self._refresh_balances_ttl_cache()
        except Exception:
            # En cas d'erreur, on retombe sur les valeurs déjà en cache
            pass

        view = self._get_capital_view_from_mbf(mode=mode)
        pockets = view.get("pockets_by_quote") or {}
        # Ticket P0-RM-CAPMOVE-01: Appliquer les réservations
        pockets = self._apply_reservations_to_pockets(pockets)

        statuses = ("OK", "DEGRADED", "BLOCKED", "UNKNOWN")
        by_status: Dict[str, float] = {s: 0.0 for s in statuses}
        by_status_quote: Dict[str, Dict[str, float]] = {s: {} for s in statuses}
        by_alias: Dict[str, Dict[str, Any]] = {}

        qset = {str(q).upper() for q in (quotes or ())}

        age_cache = getattr(self, "_alias_balance_age_s", {}) or {}
        status_cache = getattr(self, "_alias_balance_status", {}) or {}

        for ex, per_alias in (pockets or {}).items():
            exu = str(ex).upper()
            for alias, per_quote in (per_alias or {}).items():
                alu = str(alias).upper()
                key = (exu, alu)
                key_str = f"{exu}.{alu}"

                raw_status = status_cache.get(key, "UNKNOWN") or "UNKNOWN"
                status_u = str(raw_status).upper()
                if status_u not in by_status:
                    status_u = "UNKNOWN"

                try:
                    age_s = float(age_cache.get(key, 0.0) or 0.0)
                except Exception:
                    age_s = 0.0

                alias_info = by_alias.setdefault(
                    key_str,
                    {"status": status_u, "age_s": age_s, "by_quote": {}},
                )
                alias_info["status"] = status_u
                alias_info["age_s"] = age_s
                alias_quotes = alias_info["by_quote"]

                for q, qdata in (per_quote or {}).items():
                    qu = str(q).upper()
                    if qset and qu not in qset:
                        continue
                    try:
                        avail = float((qdata or {}).get("available", 0.0) or 0.0)
                    except Exception:
                        avail = 0.0
                    if avail <= 0.0:
                        continue

                    alias_quotes[qu] = alias_quotes.get(qu, 0.0) + avail
                    by_status[status_u] += avail
                    bsq = by_status_quote[status_u]
                    bsq[qu] = bsq.get(qu, 0.0) + avail

        return {
            "by_status": by_status,
            "by_status_quote": by_status_quote,
            "by_alias": by_alias,
        }


    def _available_quote(self, exchange: str, alias: str, quote: str) -> float:
        """
        Lecture unique des soldes quote depuis la vue capitale unifiée.

        Utilise toujours _balances_snapshot() (MBF owner → éventuel overlay dry-run → cache Rebal).
        """
        snap = self._balances_snapshot()
        ex = str(exchange).upper()
        al = str(alias)
        q = str(quote).upper()
        return float(((snap.get(ex, {}) or {}).get(al, {}) or {}).get(q, 0.0))

    def _available_usdc(self, exchange: str, alias: str) -> float:
        # compat legacy
        return self._available_quote(exchange, alias, "USDC")

    def _fees_for_branch(self, buy_ex: str, sell_ex: str, pair: str, branch: str) -> Tuple[float, float]:
        if str(branch).upper() == "TM":
            return (self.get_fee_pct(buy_ex, pair, "taker"), self.get_fee_pct(sell_ex, pair, "maker"))
        return (self.get_fee_pct(buy_ex, pair, "taker"), self.get_fee_pct(sell_ex, pair, "taker"))

    def _levels_for(self, exchange: str, pair: str) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        asks, bids = self.get_orderbook_depth(exchange, pair)
        try:
            asks = sorted([(float(p), float(q)) for p, q in asks], key=lambda x: x[0])
            bids = sorted([(float(p), float(q)) for p, q in bids], key=lambda x: x[0], reverse=True)
        except Exception:
            logging.exception("Unhandled exception")
        return asks, bids

    # ------------------------------------------------------------------
    # Market data accessors (public API)
    # ------------------------------------------------------------------
    def get_book_snapshot(self, exchange: str, pair_key: str) -> Dict[str, Any]:
        ex = str(exchange).upper()
        pk = self._norm_pair(pair_key)
        try:
            return dict(((self._last_books.get(ex, {}) or {}).get(pk, {}) or {}))
        except Exception:
            return {}

    def _rm_guard_or_raise(self, *, pair_key: str, buy_ex: str, sell_ex: str) -> dict | None:
        """
        Valida readiness e freschezza. Ritorna un dict 'degraded_ctx' (clamps/penalità) se 'giallo',
        None se 'verde'. Se 'rosso' → alza DataStaleError/NotReadyError. Nessun default.
        """
        now = time.time()
        cfg = getattr(self, "_cfg_root", None) or getattr(self, "bot_cfg", self.cfg)
        rm_cfg = self._rm_cfg() or cfg

        # Fees freshness (SFC = sorgente primaria)
        if not self.slip_collector:
            raise NotReadyError("SFC not attached")
        fee_age = now - float(getattr(self.slip_collector, "last_fee_sync_ts", 0.0))
        fee_ttl_strict = float(
            self._get_rm_knob("fee_snapshot_ttl_s", legacy="FEE_SNAPSHOT_TTL_S", default=900.0)
        )
        fee_ttl_tol = float(
            self._get_rm_knob("fee_tolerated_age_s", legacy="FEE_TOLERATED_AGE_S", default=3600.0)
        )

        # Volatilità (VM)
        if not self.vol_manager:
            raise NotReadyError("VM not attached")
        vm = self.vol_manager.get_current_metrics(pair_key) or {}
        vol_age = float(vm.get("age_s", float("inf")))
        vol_ttl_strict = float(
            self._get_rm_knob("vol_snapshot_ttl_s", legacy="VOL_SNAPSHOT_TTL_S", default=5.0)
        )
        vol_ttl_tol = float(
            self._get_rm_knob("degraded_vol_ttl_s", legacy="DEGRADED_VOL_TTL_S", default=12.0)
        )

        # TOB (hard strict)
        tob_max_age_s = float(
            self._get_rm_knob("tob_max_age_s", legacy="TOB_MAX_AGE_S", default=1.0)
        )
        b_bid, b_ask = self.get_top_of_book(buy_ex, pair_key, max_age_s=tob_max_age_s)
        s_bid, s_ask = self.get_top_of_book(sell_ex, pair_key, max_age_s=tob_max_age_s)
        
        # On ne vérifie l edge que si on est en cross-CEX (arbitrage classique)
        if str(sell_ex).upper() != str(buy_ex).upper():
            if s_bid <= b_ask:
                raise InconsistentStateError(f"No edge: {sell_ex}->{buy_ex} bid<=ask")

        # Strict verde
        if fee_age <= fee_ttl_strict and vol_age <= vol_ttl_strict:
            return None

        # Giallo (attività degradata esplicita, nessun valore inventato)
        if fee_age <= fee_ttl_tol and vol_age <= vol_ttl_tol:
            qpos_usd = int(
                self._get_rm_knob(
                    "tm_queuepos_max_ahead_usd",
                    legacy="TM_QUEUEPOS_MAX_AHEAD_USD",
                    default=25000,
                )
            )
            qpos_eta_ms = int(
                self._get_rm_knob(
                    "tm_queuepos_max_eta_ms",
                    legacy="TM_QUEUEPOS_MAX_ETA_MS",
                    default=0,
                )
            )

            return {
                "reason": "JAUNE_FEE" if fee_age > fee_ttl_strict else "JAUNE_VOL",
                "tm_controls": {
                    "hedge_ratio": float(
                        self._get_rm_knob(
                            "degraded_hedge_ratio",
                            legacy="DEGRADED_HEDGE_RATIO",
                            default=0.75,
                        )
                    ) if vol_age > fee_ttl_strict else None,
                    "ttl_ms": int(
                        self._get_rm_knob(
                            "tm_exposure_ttl_ms",
                            legacy="TM_EXPOSURE_TTL_MS",
                            default=2500,
                        )
                    ),
                    # Canon : ahead_usd ; alias queuepos_max_usd pour compat Engine
                    "queuepos_max_ahead_usd": qpos_usd,
                    "queuepos_max_usd": qpos_usd,
                    "queuepos_max_eta_ms": qpos_eta_ms,
                    "ioc_only": bool(vol_age > vol_ttl_strict),
                },
                "caps": {
                    "size_factor": float(
                        self._get_rm_knob(
                            "degraded_size_factor",
                            legacy="DEGRADED_SIZE_FACTOR",
                            default=0.7,
                        )
                    )
                },
                "min_bps_lift_bps": float(
                    self._get_rm_knob(
                        "degraded_min_bps_lift",
                        legacy="DEGRADED_MIN_BPS_LIFT",
                        default=4.0,
                    )) if vol_age > vol_ttl_strict else 0.0,
                "cost_penalty_bps": float(
                    self._get_rm_knob(
                        "degraded_fee_penalty_bps",
                        legacy="DEGRADED_FEE_PENALTY_BPS",
                        default=3.0,
                    )) if fee_age > fee_ttl_strict else 0.0,
                "bundle_concurrency_delta": int(
                    self._get_rm_knob(
                        "degraded_concurrency_delta",
                        legacy="DEGRADED_CONCURRENCY_DELTA",
                        default=-1,
                    )
                ),
            }

        # Rosso
        if fee_age > fee_ttl_tol:
            raise DataStaleError(f"Fees stale: {fee_age:.0f}s")
        if vol_age > vol_ttl_tol:
            raise DataStaleError(f"Vol stale: {vol_age:.1f}s")
        raise NotReadyError("Unspecified stale")

    def get_top_of_book(
            self,
            exchange: str,
            pair_key: str,
            *,
            max_age_s: float | None = 1.0,
            enforce_fresh: bool | None = None,  # compat legacy ; si False ⇒ on ne checke pas l’âge
    ) -> tuple[float, float]:
        """
        Retourne (best_bid, best_ask) pour (exchange, pair).
        - Si enforce_fresh is False: on ne vérifie pas l’âge (équivalent max_age_s=None).
        - Sinon: si données manquantes/invalides/stales ⇒ DataStaleError.
        """
        if enforce_fresh is False:
            max_age_s = None
        ex_key = str(exchange).upper()
        pk = self._norm_pair(pair_key)
        ob = getattr(self, "_orderbooks", {}).get((ex_key, pk))
        if not ob:
            raise DataStaleError(f"TOB missing: {exchange} {pair_key}")
        ts = float(ob.get("ts", 0.0))
        if max_age_s is not None and (time.time() - ts) > max_age_s:
            raise DataStaleError(f"TOB stale: {exchange} {pair_key}")
        bid = float(ob.get("best_bid", 0.0))
        ask = float(ob.get("best_ask", 0.0))
        if bid <= 0.0 or ask <= 0.0 or bid >= ask:
            raise InconsistentStateError(f"TOB invalid: bid={bid} ask={ask} {exchange} {pair_key}")
        return bid, ask

    def get_mid_price_usdc(self, pair_key: str, *, prefer_exchange: Optional[str] = None) -> float:
        pk = self._norm_pair(pair_key)
        if prefer_exchange:
            bid, ask = self.get_top_of_book(prefer_exchange, pk, enforce_fresh=False)
            if bid > 0 and ask > 0:
                return (bid + ask) / 2.0
        return self._mid_price_usdc(pk)


    # ------------------------------------------------------------------
    # Validations inventaire / skew (Engine → RM) — **alias-aware**
    # ------------------------------------------------------------------
    def validate_inventory_single(self, exchange: str, pair_key: str, side: str,
                                  volume_usdc: float, *, account_alias: Optional[str] = None,
                                  is_rebalancing: bool = False) -> bool:
        if volume_usdc <= 0:
            return False
        snap = self._balances_snapshot()
        base = self._pair_base(pair_key)
        self._project_single(snap, exchange, pair_key, side, volume_usdc, account_alias=account_alias)
        cap_limit = self.inventory_cap_usd * (1.5 if (is_rebalancing and self.inventory_rebal_exempt) else 1.0)
        skew_limit = self.inventory_skew_max_pct * (1.5 if (is_rebalancing and self.inventory_rebal_exempt) else 1.0)
        if self._cap_breached(snap, base, pair_key, cap_limit_usd=cap_limit):
            return False
        if self._skew_pct(snap, base, pair_key) > skew_limit:
            return False
        if not self._strict_share_guard(snap, base, pair_key, focus_exchange=str(exchange).upper(), skew_limit_pct=skew_limit):
            return False
        return True

    def validate_inventory_bundle(self, buy_ex: str, sell_ex: str, pair_key: str,
                                  vol_usdc_buy: float, vol_usdc_sell: float,
                                  *, buy_alias: Optional[str] = None, sell_alias: Optional[str] = None,
                                  is_rebalancing: bool = False) -> bool:
        if vol_usdc_buy <= 0 or vol_usdc_sell <= 0:
            return False
        snap = self._balances_snapshot()
        base = self._pair_base(pair_key)
        self._project_single(snap, buy_ex, pair_key, "BUY", vol_usdc_buy, account_alias=buy_alias)
        self._project_single(snap, sell_ex, pair_key, "SELL", vol_usdc_sell, account_alias=sell_alias)
        skew_limit = self.inventory_skew_max_pct * (1.5 if (is_rebalancing and self.inventory_rebal_exempt) else 1.0)
        cap_limit = self.inventory_cap_usd * (1.5 if (is_rebalancing and self.inventory_rebal_exempt) else 1.0)
        if self._cap_breached(snap, base, pair_key, cap_limit_usd=cap_limit):
            return False
        if self._skew_pct(snap, base, pair_key) > skew_limit:
            return False
        if not self._strict_share_guard(snap, base, pair_key, focus_exchange=str(buy_ex).upper(), skew_limit_pct=skew_limit):
            return False
        # --- Pool gate : min buffer en devise de cotation sur l'acheteur ---
        if float(self.min_buffer_quote) > 0.0:
            if self._available_quote(buy_ex, buy_alias or "TT", _pair_quote(pair_key)) < float(self.min_buffer_quote):
                try:
                    POOL_GATE_THROTTLES_TOTAL.labels(_pair_quote(pair_key)).inc()
                except Exception:
                    pass
                return False

        return True

    # ------------------------------------------------------------------
    # Validations de base
    # ------------------------------------------------------------------
    def validate_trade(self, exchange: str, pair_key: str, volume_usdc: float) -> bool:
        pk = self._norm_pair(pair_key)

        if self._paused.get(pk, False):
            logger.warning(f"[RiskManager] ⛔ {pk} en pause (prudence).")
            inc_blocked("rm", "prudence_pause", pk)
            return False
        if self._is_pair_penalized(pk):
            inc_blocked("rm", "pair_penalty", pk)
            try:
                RM_DROPPED_TOTAL.labels("pair_penalty").inc()
            except Exception:
                pass
            return False

        try:
            min_notional = self.get_minimum_volume_required(exchange, pk)
            if float(volume_usdc) < float(max(1e-9, min_notional)):
                logger.warning(f"[RiskManager] ⛔ Volume insuffisant {volume_usdc} < {min_notional} ({pk})")
                inc_blocked("rm", "min_notional", pk)
                return False
        except Exception:
            return False

        try:
            slip = float(self.get_slippage(exchange, pk))
            slip_thr = getattr(self.slip_collector, "slippage_threshold", 0.01) if self.slip_collector else 0.01
            if slip > slip_thr:
                logger.warning(f"[RiskManager] ⛔ Slippage élevé {pk}={slip:.4f} > thr={slip_thr:.4f}")
                inc_blocked("rm", "slippage_excess", pk)
                return False
        except Exception:
            logging.exception("Unhandled exception")

        return True

    def revalidate_trade(self, exchange: str, pair_key: str, volume_usdc: float) -> bool:
        return self.validate_trade(exchange, pair_key, volume_usdc)

    # ------------------------ Seuil dynamique (helper) ------------------------
    def _dynamic_min_required_bps(self, pair_key: str) -> float:
        try:
            th = self.vol_monitor.get_current_thresholds(pair_key) or {}
            p95_spread = float(th.get("spread_vol_threshold", 0.0))
        except Exception:
            p95_spread = 0.0
        dyn = self.base_min_bps + self.dynamic_K * (p95_spread * 1e4)
        dyn = float(max(self.min_bps_floor, min(dyn, self.min_bps_cap)))
        try:
            set_dynamic_min(self._norm_pair(pair_key), dyn)
        except Exception:
            logging.exception("Unhandled exception")
        return dyn

    def _min_required_bps_for(self, pair_key: str, strategy: str) -> float:
        """
        Helper interne: calcule le seuil min_required_bps (en bps) pour une paire et une stratégie.
        - TT : base_min_bps dynamiques (dynamic_min_required + boost VM si disponible)
        - TM : tm_min_required_bps + boost VM
        - REB : 0.0 (le contrôle de perte finale est géré via allow_final_loss_bps)
        """
        pk = self._norm_pair(pair_key)
        strat = str(strategy or "TT").upper()

        # REB: on laisse min_required_bps à 0, la politique de perte finale est gérée ailleurs
        if strat == "REB":
            return 0.0
        rm_cfg = getattr(self.cfg, "rm", None)
        base_tt = float(getattr(self, "base_min_bps",
                                getattr(rm_cfg, "base_min_bps", 20.0)))
        base_tm = float(getattr(self, "tm_min_required_bps",
                                getattr(self.cfg, "tm_min_required_bps_base", 11.0)))

        thr = base_tt
        if strat == "TM":
            thr = base_tm
        else:  # TT
            # Dynamic min optionnel
            if getattr(self, "dynamic_min_required", False):
                try:
                    thr = float(self._dynamic_min_required_bps(pk))
                except Exception:
                    thr = base_tt

        # VM adjustments: boost en fonction de la volatilité si disponible
        try:
            boost_bps, _, _ = self._vm_adjustments(pk)
            if boost_bps > 0.0:
                thr = float(thr) + float(boost_bps)
        except Exception:
            # best-effort: en cas d'erreur, on reste sur thr
            pass

        return float(thr)


    # --- VM integration (PASSIF) -------------------------------------------------
    # --- VM integration (PASSIF) -------------------------------------------------
    def _vm_adjustments(self, pair_key: str) -> tuple[float, float, float]:
        """
        STRICT: richiede VolatilityManager; nessun fallback.
        Ritorna (boost_min_required_bps, size_factor, neutral_hedge_ratio_dyn).

        - boost_min_required_bps in bps (float)
        - size_factor in [0.30, 1.00]
        - neutral_hedge_ratio_dyn:
            override dynamique de la clé canonique tm_exposure_ttl_hedge_ratio.
            Si la VM ne fournit rien, on retombe sur cfg.tm_exposure_ttl_hedge_ratio.
        """
        if self.vol_manager is None:
            raise RuntimeError("VolatilityManager non inizializzato")

        pk = self._norm_pair(pair_key)
        adj = self.vol_manager.step(pk)  # deve esistere e tornare il dict atteso

        boost_bps = float(adj.get("min_bps_boost", 0.0)) * 1e4
        size_factor = float(adj.get("size_factor", 1.0))

        # NB: la clé canonique reste tm_exposure_ttl_hedge_ratio.
        # Le champ "tm_neutral_hedge_ratio" dans l'output de la VM est interprété
        # comme un override dynamique de cette valeur canonique (neutral hedge ratio).
        hedge_ratio = float(
            adj.get("tm_neutral_hedge_ratio", getattr(self.cfg, "tm_exposure_ttl_hedge_ratio", 0.50))
        )

        # clamp industry-like
        size_factor = max(0.30, min(size_factor, 1.00))
        return boost_bps, size_factor, hedge_ratio

    def get_vm_adjustments(self, pair_key: str) -> Dict[str, Any]:
        """
        Helper PUBLIC (read-only) pour exposer le "desk vol" canonique basé sur le VolatilityManager.

        Retourne un dict lisible, destiné aux consommateurs externes (Scanner, Obs, dashboards, watchdogs):

            {
                "pair": <pair normalisée>,
                "boost_min_required_bps": <float>,   # bps à ajouter au min_required_bps
                "size_factor": <float>,              # multiplicateur de taille (soft)
                "neutral_hedge_ratio": <float>,      # hedge ratio neutre dynamique (0.0..1.0)
                "prudence": "normal|modere|eleve",   # bande de prudence VM
                "p95_vol_bps": <float>,              # vol micro p95 VM, en bps
                "ewma_vol_bps": <float>,             # vol micro EWMA VM, en bps
                "age_s": <float>,                    # âge des données de vol
            }

        En cas de problème, on retourne une vue neutre:
            boost_min_required_bps = 0.0, size_factor = 1.0, neutral_hedge_ratio = cfg.tm_exposure_ttl_hedge_ratio.
        """
        pk = self._norm_pair(pair_key)

        # Defaults "neutres"
        boost_bps: float = 0.0
        size_factor: float = 1.0
        neutral_hr: float = float(getattr(self.cfg, "tm_exposure_ttl_hedge_ratio", 0.50))
        prudence: str = "normal"
        p95_vol_bps: float = 0.0
        ewma_vol_bps: float = 0.0
        age_s: float = float("inf")

        # 1) Ajustements VM (boost / size / hedge)
        try:
            bps_boost, sf, hr = self._vm_adjustments(pk)
            boost_bps = float(bps_boost)
            size_factor = float(sf)
            neutral_hr = float(hr)
        except Exception:
            # best-effort: on garde les defaults
            pass

        # 2) Snapshot des métriques de vol issues du VM (si disponible)
        vm = getattr(self, "vol_manager", None)
        if vm is not None and hasattr(vm, "get_current_metrics"):
            try:
                met = vm.get_current_metrics(pk) or {}
                prudence = str(met.get("band", prudence) or prudence)
                p95_vol_bps = float(met.get("p95_vol_bps", met.get("p95_bps", p95_vol_bps)))
                ewma_vol_bps = float(met.get("ewma_vol_bps", met.get("ewma_bps", ewma_vol_bps)))
                age_s = float(met.get("age_s", met.get("last_age_s", age_s)))
            except Exception:
                # on ne fait pas échouer l'appel pour des métriques
                pass

        return {
            "pair": pk,
            "boost_min_required_bps": boost_bps,
            "size_factor": size_factor,
            "neutral_hedge_ratio": neutral_hr,
            "prudence": prudence,
            "p95_vol_bps": p95_vol_bps,
            "ewma_vol_bps": ewma_vol_bps,
            "age_s": age_s,
        }

    def get_volatility_bps(self, pair_key: str) -> float:
        """
        Helper PUBLIC minimal pour exposer une vol micro canonique en bps depuis le VolatilityManager.

        Pensé comme hook léger pour des hints Scanner / Obs:

            - retourne p95_vol_bps (ou p95_bps) telle que vue par le VM,
            - en cas de problème ou si VM absent => 0.0.

        NB: cette vol est en **bps micro (spread L1)**, pas la vol "statistique" du VolatilityMonitor.
        """
        pk = self._norm_pair(pair_key)
        vm = getattr(self, "vol_manager", None)
        if vm is None or not hasattr(vm, "get_current_metrics"):
            return 0.0

        try:
            met = vm.get_current_metrics(pk) or {}
            return float(met.get("p95_vol_bps", met.get("p95_bps", 0.0)))
        except Exception:
            return 0.0


    def revalidate_arbitrage(
            self,
            *,
            buy_ex: str,
            sell_ex: str,
            pair_key: str,
            expected_net: Optional[float] = None,
            max_drift_bps: float = 10.0,
            min_required_bps: Optional[float] = None,
            is_rebalancing: bool = False,
            allow_final_loss_bps: float = 0.0,
            enforce_min_required: bool = True,
            # overrides optionnels (compat)
            buy_px_override: Optional[float] = None,
            sell_px_override: Optional[float] = None,
            price_overrides: Optional[dict] = None,
            meta_to_fill: Optional[dict] = None, # Ticket D5
    ) -> bool:
        """
        Revalidation "last-mile" unifiée et robuste.

        Contrat métier (Ticket 9 – owner min_required_bps) :
        - Le RiskManager est l’unique owner des décisions GO/NO-GO **économiques** sur les arbitrages
          TT/TM/REB.
        - Les appelants (Scanner, ExecutionEngine, simulateur, etc.) ne doivent jamais recalculer eux-mêmes
          une rentabilité nette ni implémenter un second seuil bps en dehors de cette méthode.
        - L’ExecutionEngine peut uniquement ajouter des gardes **techniques** (qualité book, profondeur,
          queue-position, timeouts, 429, etc.), mais pas de décision business basée sur des bps locaux.
        - Le paramètre `min_required_bps` est un override optionnel **piloté par la politique de risque**
          (RiskManager + modes spéciaux). Il ne doit pas être recalculé de manière autonome au niveau Engine.

        Détails d’implémentation :
        - Essaie d'utiliser TOB strict via get_top_of_book (raise DataStaleError/InconsistentStateError si
          stale/invalide).
        - Fallback tolérant sur snapshot _last_books si le TOB strict n'est pas disponible.
        - Supporte overrides explicites (buy_px_override / sell_px_override / price_overrides).
        - Calcul du coût net en préférant :
            1) l'API SFC.get_total_cost_pct si disponible (renvoie fraction, ex. 0.0012)
            2) sinon, composition fees + slippage via get_fee_pct / get_slippage / _slip_pct fallback.
        - Applique boost VM (prudence) sur min_required_bps si applicable.
        - Vérifie expected_net drift, tolérance de perte finale et retourne True si net_bps >= min_required_bps.
        - Penalise la paire (penalize_pair) si incohérences / dérives observées.
        """
        try:
            pk = self._norm_pair(pair_key)
            bo = price_overrides or {}
            enforce_min_required = bool(enforce_min_required)
            # ===== min_required_bps resolution (param -> dynamic -> base) =====
            # ===== min_required_bps resolution (param -> dynamic -> base) =====
            # enforce_min_required=False => mode "drift-only" (sonde Engine) :
            # on ne recalcule pas min_required_bps ici pour éviter un 2e GO/NO-GO économique.
            if enforce_min_required:
                # Overlays de pénalité (Ticket D2-2)
                tt_overlays = self._compute_tt_revalidation_overlays(buy_ex, sell_ex, pk)
                tt_penalty = tt_overlays["latency_penalty_bps"] + tt_overlays["staleness_penalty_bps"]

                if min_required_bps is None:
                    if getattr(self, "dynamic_min_required", False) and not is_rebalancing:
                        try:
                            min_required_bps = self._dynamic_min_required_bps(pk)
                        except Exception:
                            rm_cfg = getattr(self.cfg, "rm", None)
                            min_required_bps = float(
                                getattr(self, "base_min_bps", getattr(rm_cfg, "base_min_bps", 20.0))
                            )
                    else:
                        min_required_bps = 0.0 if is_rebalancing else float(
                            getattr(self, "base_min_bps", getattr(getattr(self.cfg, "rm", None), "base_min_bps", 20.0))
                        )

                # Ajout des pénalités au seuil
                if tt_penalty > 0 and not is_rebalancing:
                    min_required_bps = float(min_required_bps) + tt_penalty

                # VM adjustments (boost threshold)
                try:
                    vm_boost_bps, _, _ = self._vm_adjustments(pk)
                    if vm_boost_bps > 0.0 and not is_rebalancing:
                        min_required_bps = float(min_required_bps) + float(vm_boost_bps)
                except Exception:
                    # best-effort: ignore VM failure
                    pass


            # ===== 1) read TOP-OF-BOOK (strict preferred) with graceful fallback =====
            buy_ask = None
            sell_bid = None
            try:
                # prefer strict TOB (may raise DataStaleError / InconsistentStateError)
                b_bid, b_ask = self.get_top_of_book(buy_ex, pk)
                s_bid, s_ask = self.get_top_of_book(sell_ex, pk)
                buy_ask = float(
                    buy_px_override if buy_px_override is not None else (bo.get("buy_ask") if bo else b_ask))
                sell_bid = float(
                    sell_px_override if sell_px_override is not None else (bo.get("sell_bid") if bo else s_bid))
            except (DataStaleError, InconsistentStateError):
                # Fallback: use last snapshot _last_books if available, but require freshness check
                books = self._last_books or {}
                buy_book = books.get(buy_ex, {}).get(pk, {}) or {}
                sell_book = books.get(sell_ex, {}).get(pk, {}) or {}
                if not (self._fresh_enough(buy_book) and self._fresh_enough(sell_book)):
                    # trop vieux -> penaliser et rejeter
                    try:
                        self.penalize_pair(pk, reason="revalidate_stale")
                    except Exception:
                        pass
                    return False
                buy_ask = float(buy_px_override if buy_px_override is not None else (
                    bo.get("buy_ask") if bo else buy_book.get("best_ask", 0)))
                sell_bid = float(sell_px_override if sell_px_override is not None else (
                    bo.get("sell_bid") if bo else sell_book.get("best_bid", 0)))

            except Exception:
                # lecture TOB totalement ratée -> safer reject (increment metric)
                try:
                    self.penalize_pair(pk, reason="revalidate_tob_err")
                except Exception:
                    pass
                return False

            # Price sanity
            if buy_ask <= 0 or sell_bid <= 0 or sell_bid <= buy_ask:
                try:
                    self.penalize_pair(pk, reason="revalidate_price_invalid")
                except Exception:
                    pass
                return False

            # ===== 2) compute costs: prefer SFC total_cost_pct else compose from fees+slip =====
            total_cost_frac = None  # fraction (ex: 0.0012)
            # Prefer slip_collector.get_total_cost_pct if exposed (SFC)
            sfc = getattr(self, "slip_collector", None) or getattr(self, "sfc", None)
            if sfc and hasattr(sfc, "get_total_cost_pct"):
                try:
                    # try to call with flexible signature; some implementations accept route+kwargs, others simpler
                    try:
                        total_cost_frac = float(
                            sfc.get_total_cost_pct(route={"buy_ex": buy_ex, "sell_ex": sell_ex, "pair": pk},
                                                   side="TM" if is_rebalancing else "TT",
                                                   size_quote=None,
                                                   slippage_kind=getattr(self, "slippage_source", "ewma"),
                                                   prudence_key=self._current_prudence(pk)))
                    except TypeError:
                        # fallback simple signature
                        total_cost_frac = float(sfc.get_total_cost_pct(buy_ex, sell_ex, pk))
                except Exception:
                    total_cost_frac = None

            # If SFC not available or failed, compute per-side fees+slip
            if total_cost_frac is None:
                # helper: robust fee getter (fraction)
                def _fee(ex: str, role: str) -> float:
                    try:
                        # try rm helpers / public API
                        return float(self._fee_pct(ex, role))
                    except Exception:
                        try:
                            return float(self.get_fee_pct(ex, pk, role))
                        except Exception:
                            # best-effort fallback 0.0
                            return 0.0

                def _slip(ex: str, pair: str, side: str) -> float:
                    try:
                        # prefer consolidated SFC API
                        return float(self._slip_pct(ex, pair, side))
                    except Exception:
                        try:
                            return float(self.get_slippage(ex, pair, side))
                        except Exception:
                            return 0.0

                fee_buy = max(0.0, _fee(buy_ex, "taker"))
                fee_sell = max(0.0, _fee(sell_ex, "taker"))
                slip_buy = max(0.0, _slip(buy_ex, pk, "buy"))
                slip_sell = max(0.0, _slip(sell_ex, pk, "sell"))

                # total cost as fraction sum of per-side contributions (conservative)
                total_cost_frac = max(0.0, fee_buy + fee_sell + slip_buy + slip_sell)

            # Defensive clamp
            try:
                total_cost_frac = float(total_cost_frac)
                if math.isnan(total_cost_frac) or total_cost_frac < 0:
                    total_cost_frac = 0.0
            except Exception:
                total_cost_frac = 0.0

            # ===== 3) compute net (fraction) and net_bps =====
            # Use multiplicative exact formulation when we have per-side fee/slip info; otherwise approximate via spread-mid minus total_cost_frac
            net_frac = None
            # if we have per-side components available in locals, use multiplicative formula
            try:
                # If we earlier computed fee_buy/fee_sell/slip_buy/slip_sell then use multiplicative formula
                if "fee_buy" in locals() and "fee_sell" in locals() and "slip_buy" in locals() and "slip_sell" in locals():
                    buy_cost = buy_ask * (1.0 + fee_buy + slip_buy)
                    sell_take = sell_bid * (1.0 - fee_sell - slip_sell)
                    net_frac = (sell_take - buy_cost) / max(buy_cost, 1e-12)
                else:
                    # fallback to spread-mid minus total_cost_frac
                    mid = (buy_ask + sell_bid) / 2.0
                    spread_norm = (sell_bid - buy_ask) / max(mid, 1e-12)
                    net_frac = spread_norm - total_cost_frac
            except Exception:
                # worst-case: compute via spread-mid - total_cost_frac
                mid = (buy_ask + sell_bid) / 2.0
                spread_norm = (sell_bid - buy_ask) / max(mid, 1e-12)
                net_frac = spread_norm - total_cost_frac

            net_bps_raw = 1e4 * float(net_frac)
            net_bps, split_penalty_bps = self._net_bps_with_split_penalty(net_bps_raw, buy_ex, sell_ex)

            # ===== 4) final checks: allow_final_loss, expected_net drift, threshold compare =====
            # final loss guard (absolute)
            if net_bps < -float(allow_final_loss_bps or 0.0):
                # allow_final_loss violated
                return False

            # expected_net coherence (expected_net is ratio)
            if expected_net is not None:
                try:
                    exp_bps = 1e4 * float(expected_net)
                    if (exp_bps - net_bps) > float(max_drift_bps or 0.0):
                        # drift too large
                        try:
                            self.penalize_pair(pk, reason="revalidate_drift")
                        except Exception:
                            pass
                        return False
                except Exception:
                    # if expected_net parsing fail -> treat as non-fatal

                    pass

            # compare to minimum required (both are in bps)
            if enforce_min_required:
                if net_bps < float(min_required_bps):
                    # special-case rebalancing allow_final_loss path déjà gérée au-dessus
                    try:
                        if is_rebalancing and float(allow_final_loss_bps or 0.0) > 0.0:
                            # allow a loss up to allow_final_loss_bps
                            if net_bps >= -(float(allow_final_loss_bps) or 0.0):
                                return True
                        # otherwise penalize and reject
                        self.penalize_pair(pk, reason="revalidate_below_min")
                    except Exception:
                        pass
                    return False

            # OK
            # Ticket D5: Reality check components
            if meta_to_fill is not None:
                try:
                    mid = (buy_ask + sell_bid) / 2.0
                    gross_bps = ((sell_bid / buy_ask) - 1.0) * 10000.0 if buy_ask > 0 else 0.0
                    
                    # Penalties components
                    lat_pen = 0.0
                    stale_pen = 0.0
                    if enforce_min_required:
                        try:
                            ovl = self._compute_tt_revalidation_overlays(buy_ex, sell_ex, pk)
                            lat_pen = ovl.get("latency_penalty_bps", 0.0)
                            stale_pen = ovl.get("staleness_penalty_bps", 0.0)
                        except Exception: pass
                    
                    f_buy = locals().get("fee_buy", 0.0)
                    f_sell = locals().get("fee_sell", 0.0)
                    s_buy = locals().get("slip_buy", 0.0)
                    s_sell = locals().get("slip_sell", 0.0)

                    meta_to_fill["edge_components"] = {
                        "gross_bps": round(gross_bps, 2),
                        "fees_bps": round((f_buy + f_sell) * 10000.0, 2),
                        "slip_bps": round((s_buy + s_sell) * 10000.0, 2),
                        "latency_penalty_bps": round(lat_pen, 2),
                        "staleness_penalty_bps": round(stale_pen, 2),
                        "net_bps": round(net_bps, 2),
                        "split_penalty_bps": round(split_penalty_bps, 2)
                    }
                except Exception:
                    logging.debug("Failed to fill edge_components")

            return True

        except Exception:
            # Harden: count refusals and return False
            try:
                self.metrics_refusals = getattr(self, "metrics_refusals", 0) + 1
            except Exception:
                logging.exception("Unhandled exception")
            return False


    # ------------------------------------------------------------------
    # API exposées à l’extérieur (alias capable)
    # ------------------------------------------------------------------
    def get_minimum_volume_required(self, exchange: str, pair_key: str, *,
                                    account_alias: Optional[str] = None) -> float:
        """
        Min notional pragmatique (en devise de cotation).

        Dry-run : cfg.min_usdc (ou règle CEX si plus élevée).
        Prod    : max(cfg.min_usdc, min_notional_cex) borné par le cash disponible (alias si fourni),
                  évalué sur la vue capitale unifiée (_balances_snapshot).
        """
        try:
            base_min = float(getattr(self.cfg, "min_usdc", 1000.0))
            # si règles CEX présentes, on les prend en compte
            rule_min = float(self.compute_cex_min_notional_usdc(exchange, pair_key) or 0.0)

            # DRY-RUN : on ne borne pas par le cash, on applique seulement les règles de taille
            if bool(getattr(self.cfg, "dry_run", False)):
                return max(base_min, rule_min)

            # PROD : borne par cash disponible (quote) sur la vue unifiée
            snap = self._balances_snapshot()
            ex = str(exchange).upper()
            quote = _pair_quote(pair_key)
            if account_alias:
                cash = float(((snap.get(ex, {}) or {}).get(account_alias, {}) or {}).get(quote, 0.0))
            else:
                cash = sum(
                    float((assets or {}).get(quote, 0.0))
                    for assets in (snap.get(ex, {}) or {}).values()
                )

            safety = 0.95
            cap = max(0.0, cash * safety)
            need = max(base_min, rule_min)
            return float(min(need, cap)) if cap > 0 else need
        except Exception:
            return float(getattr(self.cfg, "min_usdc", 1000.0))


    # ---------- Fees & slippage dynamiques ----------

    def get_fees(self, exchange: str, pair_key: str) -> float:
        return self.get_fee_pct(exchange, pair_key, "taker")

    # --- Remplacer l'implémentation actuelle de _mm_cost_bps par :
    def _mm_cost_bps(self, route: dict, *, size_quote: float, prudence_key: str = "NORMAL") -> float:
        """
        Coût MM (maker/maker) en bps.
        Patch: utilise SFC Forme A (route) avec side="MM" + size_quote + prudence_key.
        Fallback conservateur: TT taker/taker (via get_total_cost_pct) si SFC indisponible.
        """
        try:
            if not isinstance(route, dict):
                return 0.0

            # Route normalisée (helper ajouté au patch précédent)
            route_sfc = self._norm_route_for_sfc(route, pair_key=(route.get("pair") or route.get("pair_key")))

            sfc = getattr(self, "slip_collector", None)
            if sfc is not None and hasattr(sfc, "get_total_cost_pct"):
                kind = str(getattr(self.cfg, "sfc_slippage_source", "ewma")).lower()
                pct = float(sfc.get_total_cost_pct(
                    route_sfc,
                    side="MM",
                    size_quote=float(size_quote or 0.0),
                    slippage_kind=("p95" if kind == "p95" else "ewma"),
                    prudence_key=str(prudence_key or "NORMAL"),
                ) or 0.0)
                return float(max(0.0, pct) * 1e4)

            # Fallback conservateur (évite sous-estimation si SFC absent)
            buy_ex = (route_sfc.get("buy_ex") or "").upper()
            sell_ex = (route_sfc.get("sell_ex") or "").upper()
            pair = (route_sfc.get("pair_key") or route_sfc.get("pair") or "")
            pct = float(self.get_total_cost_pct(buy_ex, sell_ex, pair) or 0.0)
            return float(max(0.0, pct) * 1e4)

        except Exception:
            return 0.0


    def get_prudence_key(self, pair: str) -> str:
        """
        API publique renvoyant le régime de prudence actuel (NORMAL/CAREFUL/ALERT/...).
        """
        if hasattr(self, "_current_prudence"):
            return str(self._current_prudence(pair))
        return "NORMAL"

    def get_fee_pct(self, exchange: str, pair_key: str, mode: str = "taker", prudence_key: Optional[str] = None) -> float:
        if self.slip_collector is None or not hasattr(self.slip_collector, "get_fee_pct"):
            raise RuntimeError("SlippageAndFeesCollector non pronto (get_fee_pct)")
        return float(self.slip_collector.get_fee_pct(exchange, self._norm_pair(pair_key), mode, prudence_key=prudence_key))

    def get_slippage(self, exchange: str, pair_key: str, side: str = "buy") -> float:
        """
        API publique slippage (fraction).
        Alignée sur cfg.sfc_slippage_source via _get_slippage() (ewma/p95) + side.
        Fallbacks conservés: recent_slippage puis slippage_handler puis 0.0.
        """
        ex = (exchange or "").upper()
        pk = self._norm_pair(pair_key)
        sd = (side or "buy").lower()

        # 1) Chemin canonique (respect cfg.sfc_slippage_source)
        try:
            return float(self._get_slippage(ex, pk, sd))
        except Exception:
            pass

        # 2) Compat: ancien "recent" consolidé
        try:
            if self.slip_collector is not None and hasattr(self.slip_collector, "get_recent_slippage"):
                return float(self.slip_collector.get_recent_slippage(pk))
        except Exception:
            pass

        # 3) Compat: handler legacy si présent
        try:
            if self.slippage_handler is not None and hasattr(self.slippage_handler, "get_slippage"):
                return float(self.slippage_handler.get_slippage(ex, pk, sd))
        except Exception:
            pass

        return 0.0

    def get_slippage_for_rebal(self, exchange: str, pair_key: str, side: str = "buy") -> float:
        """
        Slippage spécifique au rééquilibrage : plus conservateur (P95).
        """
        ex = (exchange or "").upper()
        pk = self._norm_pair(pair_key)
        sd = (side or "buy").lower()

        if self.slip_collector is not None and hasattr(self.slip_collector, "get_slippage"):
            try:
                # On force le P95 pour le rééquilibrage pour être conservateur
                return float(self.slip_collector.get_slippage(ex, pk, sd, kind="p95"))
            except Exception:
                pass

        return self.get_slippage(exchange, pair_key, side)

    def _compute_cost_breakdown_for_route(self, buy_ex: str, sell_ex: str, pair_key: str) -> Dict[str, float]:
        """
        Desk « coût total » : fees, slippage, pénalité SPLIT.
        Patch: total_cost_pct provient de SFC.get_total_cost_pct() (Forme B legs),
        pour éviter toute divergence entre branches.
        """
        pk = self._norm_pair(pair_key)
        be = str(buy_ex or "").upper()
        se = str(sell_ex or "").upper()

        # Fees acheteur / vendeur (fractions) — toujours utile pour debug
        try:
            fb = float(max(0.0, self.get_fee_pct(be, pk, "taker")))
            fs = float(max(0.0, self.get_fee_pct(se, pk, "taker")))
        except Exception:
            fb = fs = 0.0

        # Slippage acheteur / vendeur (fractions) — aligné (get_slippage -> _get_slippage)
        try:
            sb = float(max(0.0, self.get_slippage(be, pk, "buy")))
            ss = float(max(0.0, self.get_slippage(se, pk, "sell")))
        except Exception:
            sb = ss = 0.0

        fees_pct = fb + fs
        slippage_pct = sb + ss
        dbg_total_cost_pct = max(0.0, fees_pct + slippage_pct)

        # ---- Source unique (SFC) pour total_cost_pct ----
        total_cost_pct = dbg_total_cost_pct
        try:
            sfc = getattr(self, "slip_collector", None)
            if sfc is not None and hasattr(sfc, "get_total_cost_pct"):
                kind = str(getattr(self.cfg, "sfc_slippage_source", "ewma")).lower()
                buy_leg = {"ex": be, "alias": "TT", "role": "taker"}
                sell_leg = {"ex": se, "alias": "TT", "role": "taker"}
                total_cost_pct = float(sfc.get_total_cost_pct(
                    pk,
                    buy_leg=buy_leg,
                    sell_leg=sell_leg,
                    size_quote=0.0,  # volontairement neutre (comportement proche de l'ancien)
                    slippage_kind=("p95" if kind == "p95" else "ewma"),
                    prudence_key="NORMAL",
                ) or 0.0)
                total_cost_pct = max(0.0, total_cost_pct)
        except Exception:
            total_cost_pct = dbg_total_cost_pct

        # Pénalité SPLIT (en bps)
        try:
            split_penalty_bps = float(self._split_penalty_bps(be, se))
        except Exception:
            split_penalty_bps = 0.0

        return {
            "fees_pct": fees_pct,
            "slippage_pct": slippage_pct,
            "total_cost_pct": total_cost_pct,  # ✅ canonique SFC
            "fees_bps": fees_pct * 1e4,
            "slippage_bps": slippage_pct * 1e4,
            "split_penalty_bps": split_penalty_bps,
            "total_cost_bps": total_cost_pct * 1e4 + split_penalty_bps,

            # debug non-cassant (additif)
            "dbg_total_cost_pct_components": dbg_total_cost_pct,
            "dbg_total_cost_bps_components": dbg_total_cost_pct * 1e4 + split_penalty_bps,
        }


    def get_total_cost_pct(self, buy_ex: str, sell_ex: str, pair_key: str) -> float:
        """
        Strict: coût % = fee_buy + fee_sell + slip_buy + slip_sell.

        Patch: route prioritairement vers SlippageAndFeesCollector.get_total_cost_pct()
        en **Forme B (legacy)** afin d'avoir une source unique et ordonnée (RM → SFC)
        pour fees+slippage.

        Fallback: ancien chemin (_compute_cost_breakdown_for_route) si SFC indisponible
        ou erreur inattendue, sans casser les signatures ni les appels existants.
        """
        pk = self._norm_pair(pair_key)
        be = str(buy_ex or "").upper()
        se = str(sell_ex or "").upper()

        # --- Fast path: SFC Forme B ---
        try:
            sfc = getattr(self, "slip_collector", None)
            if sfc is not None and hasattr(sfc, "get_total_cost_pct"):
                buy_leg = {"ex": be, "alias": "TT", "role": "taker"}
                sell_leg = {"ex": se, "alias": "TT", "role": "taker"}
                v = float(sfc.get_total_cost_pct(pk, buy_leg=buy_leg, sell_leg=sell_leg))
                # v peut légitimement être 0.0 si pas de snapshot / pas de mesures; on l’accepte.
                return v
        except Exception:
            pass

        # --- Fallback legacy (inchangé) ---
        try:
            breakdown = self._compute_cost_breakdown_for_route(be, se, pk)
            return float(breakdown.get("total_cost_pct", 0.0))
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    # Fast-path & mode strategy
    # ------------------------------------------------------------------
    def get_regime(self, pair_key: str) -> str:
        pk = self._norm_pair(pair_key)
        try:
            sig = (self.vol_monitor.get_prudence_signal(pk) or "normal").lower()
        except Exception:
            sig = "normal"
        if sig in ("élevé", "eleve"):
            return "eleve"
        if sig in ("modéré", "modere"):
            return "modere"
        return "normal"

    # --- PATCH: dans class RiskManager ---

    def _get_slippage(self, exchange: str, pair_key: str, side: str) -> float:
        """
        Retourne un slippage (fraction) cohérent avec la source choisie dans BotConfig.
        Priorité: SlippageAndFeesCollector (kind="ewma"|"p95"), sinon slippage_handler legacy.
        Fallback = 0.0.
        """
        # dans class RiskManager, méthode _get_slippage(...)
        kind = str(getattr(self.cfg, "sfc_slippage_source", "ewma")).lower()
        try:
            if self.slip_collector and hasattr(self.slip_collector, "get_slippage"):
                return float(self.slip_collector.get_slippage(
                    exchange.upper(), pair_key.replace("-", "").upper(), side.lower(),
                    kind=("p95" if kind == "p95" else "ewma"), default=0.0
                ))
            if self.slippage_handler and hasattr(self.slippage_handler, "get_slippage"):
                return float(self.slippage_handler.get_slippage(exchange, pair_key, side))
        except Exception:
            pass
        return 0.0

    def is_fastpath_ok(self, buy_ex: str, sell_ex: str, pair_key: str) -> bool:
        """
        Filtre rapide (fast-path) unifié:
        - régime "normal"
        - orderbooks frais
        - seuil dynamique (dyn_min_required_bps) sous plafond cfg
        - slippage (buy/sell) < cfg.fastpath_slip_bps_max (fraction)
        """
        pk = self._norm_pair(pair_key)
        if self.get_regime(pk) != "normal":
            return False

        b1 = (self._last_books.get(buy_ex, {}) or {}).get(pk, {}) or {}
        b2 = (self._last_books.get(sell_ex, {}) or {}).get(pk, {}) or {}
        if not (self._fresh_enough(b1) and self._fresh_enough(b2)):
            return False

        # --- plafond dynamique en bps (cfg exprimé en fraction: 30 bps = 0.003)
        try:
            dyn_bps = float(self._dynamic_min_required_bps(pk))  # bps (ex: 20.0)
        except Exception:
            dyn_bps = float(getattr(self, "base_min_bps", 20.0))

        # Convertit la ceiling fraction → bps
        ceiling_frac = float(getattr(self.cfg, "dyn_min_required_bps_ceiling_pct", 0.003))  # ex: 0.003
        ceiling_bps = max(float(getattr(self, "base_min_bps", 20.0)) + ceiling_frac * 10_000.0,
                          ceiling_frac * 10_000.0)
        if dyn_bps > ceiling_bps:
            return False

        # --- slippage buy/sell avec source contrôlée par cfg
        max_slip = float(getattr(self.cfg, "fastpath_slip_bps_max", 0.003))  # fraction
        try:
            s_buy = self._get_slippage(buy_ex, pk, "buy")
            s_sell = self._get_slippage(sell_ex, pk, "sell")
        except Exception:
            s_buy = s_sell = 0.0

        if s_buy > max_slip or s_sell > max_slip:
            return False

        return True

    def set_trade_modes(self, *, enable_tt: Optional[bool] = None, enable_tm: Optional[bool] = None) -> None:
        if enable_tt is not None:
            self.enable_tt = bool(enable_tt)
        if enable_tm is not None:
            self.enable_tm = bool(enable_tm)

    def _tm_edge_bps(self, *, maker_side: str, maker_ex: str, taker_ex: str, pair_key: str) -> float:
        maker_side = str(maker_side).upper()
        try:
            # lire TOB (attention: get_top_of_book lève si stale)
            mbid, mask = self.get_top_of_book(maker_ex, pair_key)
            tbid, task = self.get_top_of_book(taker_ex, pair_key)
        except Exception:
            return -1e9
        if min(mbid, mask, tbid, task) <= 0:
            return -1e9

        # calcul brut dépendant du côté maker
        if maker_side == "SELL":
            brut = (mask - task) / max(task, 1e-12)
            # coûts : taker fee @ taker_ex (taker), maker fee @ maker_ex (maker)
            total_cost = (
                    self.get_fee_pct(taker_ex, pair_key, "taker")
                    + self.get_fee_pct(maker_ex, pair_key, "maker")
                    + self.get_slippage(taker_ex, pair_key, "buy")
                    + self.get_slippage(maker_ex, pair_key, "sell")
            )
        else:
            brut = (tbid - mbid) / max(mbid, 1e-12)
            total_cost = (
                    self.get_fee_pct(maker_ex, pair_key, "maker")
                    + self.get_fee_pct(taker_ex, pair_key, "taker")
                    + self.get_slippage(maker_ex, pair_key, "buy")
                    + self.get_slippage(taker_ex, pair_key, "sell")
            )

        return (brut - total_cost) * 1e4

    def _depth_ratio_ok(self, ex: str, pair_key: str, usdc_amt: float, min_ratio: float) -> bool:
        asks, bids = self.get_orderbook_depth(ex, pair_key)
        limit = int(getattr(self.cfg, "binance_depth_level", 10) or 10)

        def _cum(levels):
            tot = 0.0
            for p, q in (levels or [])[:limit]:
                try:
                    tot += float(p) * float(q)
                except Exception:
                    logging.exception("Unhandled exception")
            return tot

        avail = max(_cum(asks), _cum(bids))
        need = max(1.0, float(usdc_amt))
        ratio_needed = max(1.0, float(
            min_ratio if min_ratio is not None else getattr(self.cfg, "tm_nn_min_depth_ratio", 1.4)))
        return (avail / need) >= ratio_needed

    def should_tm_non_neutral(
            self,
            *,
            pair_key: str,
            maker_ex: str,
            taker_ex: str,
            usdc_amt: float,
            edge_sell_bps: Optional[float] = None,
            edge_buy_bps: Optional[float] = None,
            profile: Optional[str] = None,
    ) -> Tuple[bool, str, float]:
        """
        Décide si TM peut basculer en NON_NEUTRAL.

        Patch (P1) : fail-closed.
        - Si vol/slip indisponibles, non finies, ou stale TTL ⇒ NN = OFF.
        - Signature compatible avec l'appel existant (edge_* + profile).
        """
        import math

        # 0) Edges : utiliser celles déjà calculées si fournies, sinon recalculer.
        try:
            if edge_sell_bps is None or not math.isfinite(float(edge_sell_bps)):
                e_sell = float(self._tm_edge_bps(pair_key, maker_ex=maker_ex, taker_ex=taker_ex,
                                                 usdc_amt=usdc_amt, side="SELL"))
            else:
                e_sell = float(edge_sell_bps)

            if edge_buy_bps is None or not math.isfinite(float(edge_buy_bps)):
                e_buy = float(self._tm_edge_bps(pair_key, maker_ex=maker_ex, taker_ex=taker_ex,
                                                usdc_amt=usdc_amt, side="BUY"))
            else:
                e_buy = float(edge_buy_bps)
        except Exception:
            logger.exception("[RiskManager] should_tm_non_neutral: edge calc failed → NN=OFF")
            # best-effort return
            return False, "SELL", 0.0

        best_side = "SELL" if e_sell >= e_buy else "BUY"
        best_edge = float(max(e_sell, e_buy))

        # Seuils NN (config)
        nn_min_edge_bps = float(getattr(self.cfg, "tm_nn_min_edge_bps", 9.0))
        nn_max_vol_bps = float(getattr(self.cfg, "tm_nn_max_vol_bps", 18.0))
        nn_max_slip_bps = float(getattr(self.cfg, "tm_nn_max_slip_bps", 7.0))

        # TTLs (contrat BotConfig si dispo, sinon fallback cfg)
        try:
            vol_ttl_s = float(
                getattr(getattr(self.bot_cfg, "vol", None), "ttl_s", None)
                or self._get_rm_knob(
                    "vol_snapshot_ttl_s",
                    legacy="VOL_SNAPSHOT_TTL_S",
                    default=5.0,
                )
            )
        except Exception:
            vol_ttl_s = float(
                self._get_rm_knob(
                    "vol_snapshot_ttl_s",
                    legacy="VOL_SNAPSHOT_TTL_S",
                    default=5.0,
                )
            )

        try:
            slip_ttl_s = float(getattr(getattr(self.bot_cfg, "slip", None), "ttl_s",
                                       float(getattr(self.cfg, "SLIP_SNAPSHOT_TTL_S", 2.0))))
        except Exception:
            slip_ttl_s = float(getattr(self.cfg, "SLIP_SNAPSHOT_TTL_S", 2.0))

        # 1) Volatilité : doit être dispo + finite + fraîche (TTL) + avec samples.
        vm = getattr(self, "vol_manager", None)
        if not vm or not hasattr(vm, "get_current_metrics"):
            logger.debug("[RiskManager] TM_NN fail-closed: vol_manager absent → NN=OFF")
            return False, best_side, best_edge

        met = None
        try:
            met = vm.get_current_metrics(pair_key)
        except Exception:
            met = None

        if not met:
            logger.debug("[RiskManager] TM_NN fail-closed: vol metrics absentes (%s) → NN=OFF", pair_key)
            return False, best_side, best_edge

        try:
            vol_age_s = float(met.get("last_age_s", met.get("age_s", float("inf"))))
        except Exception:
            vol_age_s = float("inf")

        try:
            vol_samples = int(met.get("samples", met.get("n_samples", 0)) or 0)
        except Exception:
            vol_samples = 0

        # p95_bps: on prend d'abord le snapshot (plus fiable), sinon fallback get_p95_bps()
        try:
            vol_bps = float(met.get("p95_bps", met.get("p95_vol_bps", 0.0)))
        except Exception:
            vol_bps = 0.0

        if (not math.isfinite(vol_age_s)) or vol_age_s >= float("inf") or vol_samples <= 0:
            logger.debug("[RiskManager] TM_NN fail-closed: vol age/samples invalides (age=%s, n=%s) → NN=OFF",
                         vol_age_s, vol_samples)
            return False, best_side, best_edge

        if vol_age_s > float(vol_ttl_s):
            logger.debug("[RiskManager] TM_NN fail-closed: vol stale (age=%.3fs > ttl=%.3fs) → NN=OFF",
                         vol_age_s, float(vol_ttl_s))
            return False, best_side, best_edge

        # 2) Slippage : doit être calculable + (si possible) fraîche TTL.
        sfc = getattr(self, "slip_collector", None) or getattr(self, "slippage_collector", None) or getattr(self, "sfc",
                                                                                                            None)
        if not sfc or not hasattr(sfc, "get_recent_slippage"):
            logger.debug("[RiskManager] TM_NN fail-closed: SFC/collector absent → NN=OFF")
            return False, best_side, best_edge

        try:
            slip_frac = float(sfc.get_recent_slippage(pair_key))
            slip_bps = slip_frac * 10_000.0
        except Exception:
            logger.exception("[RiskManager] TM_NN fail-closed: slippage compute failed (%s) → NN=OFF", pair_key)
            return False, best_side, best_edge

        if not math.isfinite(slip_bps) or slip_bps < 0:
            logger.debug("[RiskManager] TM_NN fail-closed: slip invalide (slip_bps=%s) → NN=OFF", slip_bps)
            return False, best_side, best_edge

        if hasattr(sfc, "last_age_seconds"):
            try:
                # NB: certains collectors ignorent l'arg → ok, c'est volontairement conservateur.
                slip_age_s = float(sfc.last_age_seconds(pair_key) or float("inf"))
            except Exception:
                slip_age_s = float("inf")

            if (not math.isfinite(slip_age_s)) or slip_age_s >= float("inf") or slip_age_s > float(slip_ttl_s):
                logger.debug("[RiskManager] TM_NN fail-closed: slip stale/unknown (age=%s ttl=%s) → NN=OFF",
                             slip_age_s, float(slip_ttl_s))
                return False, best_side, best_edge

        # 3) Profondeur (existant)
        try:
            depth_ok, _depth_ratio = self._depth_ratio_ok(pair_key, maker_ex=maker_ex, taker_ex=taker_ex)
        except Exception:
            logger.exception("[RiskManager] TM_NN fail-closed: depth check failed → NN=OFF")
            return False, best_side, best_edge

        # 4) Règles NN (inchangées sur le fond, mais plus de fail-open)
        ok = (
                (best_edge >= nn_min_edge_bps) and
                (vol_bps <= nn_max_vol_bps) and
                (slip_bps <= nn_max_slip_bps) and
                bool(depth_ok)
        )

        return bool(ok), best_side, best_edge


    def decide_tm_mode(
            self,
            *,
            pair_key: str,
            maker_ex: str,
            taker_ex: str,
            usdc_amt: float,
    ) -> Dict[str, Any]:
        """
        Décide NEUTRAL vs NON_NEUTRAL (NN) pour un TM, en étant profile-aware.

        Contrat M3-A / meta["tm"] (côté RM) :
        - mode: "NEUTRAL" ou "NON_NEUTRAL" (normalisé ensuite en {"NEUTRAL", "NN"}).
        - maker_side: "BUY" ou "SELL" (jambe maker).
        - edge_bps: edge TM (en bps) sur la jambe maker.
        - hedge_ratio: ratio initial de couverture (0.0–1.0).
        - max_exposure_s: horizon métier maximum d'exposition TM_NN, en secondes.

        Points clés :
        - Le RM reste l'unique owner économique de ce choix (mode + hedge + horizon).
        - L'Engine consommera ce bloc via payload.meta["tm"] et appliquera uniquement
          des gardes techniques (TTL, panic-hedge, backpressure).
        """
        cfg = self.cfg

        # Gardé pour tuning ultérieur (NEUTRAL vs NON_NEUTRAL par défaut)
        default_mode = str(getattr(cfg, "tm_default_mode", "NEUTRAL")).upper()

        # Clés canoniques pour le hedging et l'horizon d'exposition
        neutral_hr = float(getattr(
            cfg,
            "tm_exposure_ttl_hedge_ratio",
            getattr(self, "tm_exposure_ttl_hedge_ratio", 0.50),
        ))
        nn_hr = float(getattr(
            cfg,
            "tm_nn_hedge_ratio",
            getattr(self, "tm_nn_hedge_ratio", 0.65),
        ))
        # Horizon métier d'exposition NN (secondes) : config ou fallback local.
        nn_max_exposure_s = float(getattr(
            cfg,
            "tm_nn_max_exposure_s",
            getattr(self, "tm_nn_max_exposure_s", 3.0),
        ))
        # Pour NEUTRAL, on utilise le TTL canonique comme horizon (ms → s).
        try:
            neutral_max_exposure_s = float(
                getattr(self, "tm_exposure_ttl_ms", 2500)
            ) / 1000.0
        except Exception:
            neutral_max_exposure_s = nn_max_exposure_s

        # M4-A : override éventuel du hedge NEUTRAL par le VolatilityManager.
        # On ne fait que serrer/adapter le ratio neutre en fonction de la prudence
        # (NORMAL/CAREFUL/ALERT). En cas de problème ou si le VM est absent,
        # on reste sur la valeur canonique.
        try:
            _, _, neutral_hr_dyn = self._vm_adjustments(pair_key)
            if neutral_hr_dyn is not None:
                neutral_hr = float(neutral_hr_dyn)
        except Exception:
            # Best-effort uniquement : pas de hard-fail sur le VM.
            pass


        # Edge brut en bps, tel qu'évalué lors du scan / pricing.
        e_sell = float(getattr(self, "last_edge_bps_sell", 0.0))
        e_buy = float(getattr(self, "last_edge_bps_buy", 0.0))

        # Décide la jambe maker par défaut : celle qui a le meilleur edge.
        maker_side = "SELL" if e_sell >= e_buy else "BUY"
        edge = e_sell if maker_side == "SELL" else e_buy

        profile = str(getattr(self, "capital_profile", "LARGE") or "LARGE").upper()

        # Config globale / par profil pour le NN
        nn_enabled_global = bool(getattr(cfg, "allow_tm_non_neutral", True))
        allowed_profiles_raw = str(getattr(cfg, "tm_nn_allowed_profiles", "MICRO,SMALL,MID,LARGE"))

        if allowed_profiles_raw:
            allowed_profiles = {
                p.strip().upper()
                for p in str(allowed_profiles_raw or "").split(",")
                if p.strip()
            }
        else:
            # Fallback : tous les profils sauf NANO
            allowed_profiles = {"MICRO", "SMALL", "MID", "LARGE"}

        profile_allows_nn = profile in allowed_profiles
        nn_allowed = bool(nn_enabled_global and profile_allows_nn)

        # Expose la vue "autorisé NN" pour l'Engine (lecture seule).
        try:
            setattr(self, "allow_tm_non_neutral", nn_allowed)
        except Exception:
            pass

        # Si NN non autorisé (profil ou config), on fige NEUTRAL.
        if not nn_allowed:
            return {
                "mode": "NEUTRAL",
                "maker_side": maker_side,
                "edge_bps": edge,
                "hedge_ratio": neutral_hr,
                "max_exposure_s": neutral_max_exposure_s,
            }

        # Décision opportuniste : laisser un TM NON_NEUTRAL si le contexte le justifie.
        ok, best_side, best_edge = self.should_tm_non_neutral(
            pair_key=pair_key,
            maker_ex=maker_ex,
            taker_ex=taker_ex,
            usdc_amt=usdc_amt,
            edge_sell_bps=e_sell,
            edge_buy_bps=e_buy,
            profile=profile,
        )

        if ok:
            return {
                "mode": "NON_NEUTRAL",
                "maker_side": best_side,
                "edge_bps": best_edge,
                "hedge_ratio": nn_hr,
                "max_exposure_s": nn_max_exposure_s,
            }

        # Fallback : TM NEUTRAL avec hedge ratio canonique et horizon neutre.
        return {
            "mode": "NEUTRAL",
            "maker_side": maker_side,
            "edge_bps": edge,
            "hedge_ratio": neutral_hr,
            "max_exposure_s": neutral_max_exposure_s,
        }


    # ------------------------------------------------------------------
    # Admin helpers (watchdogs / discovery)
    # ------------------------------------------------------------------
    def set_allowed_routes(self, routes):
        self.allowed_routes = {(str(a).upper(), str(b).upper()) for (a, b) in (routes or [])}
        self._sync_simulator_allowed_routes()

    def enable_route(self, a, b):
        self.allowed_routes.add((str(a).upper(), str(b).upper()))
        self._sync_simulator_allowed_routes()

    def disable_route(self, a, b):
        self.allowed_routes.discard((str(a).upper(), str(b).upper()))
        self._sync_simulator_allowed_routes()

    def disable_exchange(self, ex):
        exu = str(ex).upper()
        self.allowed_routes = {(a, b) for (a, b) in self.allowed_routes if a != exu and b != exu}
        self._sync_simulator_allowed_routes()

    def _sync_simulator_allowed_routes(self) -> None:
        simulator = getattr(self, "simulator", None)
        if not simulator or not hasattr(simulator, "update_allowed_routes"):
            return
        try:
            simulator.update_allowed_routes(list(self.allowed_routes))
        except Exception:
            logger.debug("[RiskManager] sync allowed routes -> simulator failed", exc_info=False)

    def _sync_simulator_fee_map(self) -> None:
        simulator = getattr(self, "simulator", None)
        collector = getattr(self, "slip_collector", None)
        if not simulator or not hasattr(simulator, "set_fee_map_pct"):
            return
        if collector is None or not hasattr(collector, "export_effective_fee_map"):
            return
        now = time.time()
        interval = float(getattr(self, "simulator_fee_sync_interval_s", 30.0))
        if now - getattr(self, "_last_sim_fee_sync", 0.0) < interval:
            return
        fee_map = collector.export_effective_fee_map()
        if not fee_map:
            return
        try:
            simulator.set_fee_map_pct(fee_map)
            self._last_sim_fee_sync = now
        except Exception:
            logger.debug("[RiskManager] sync fee map -> simulator failed", exc_info=False)
    # ------------------------------------------------------------------
    def pause_all_symbols(self, reason: str = "") -> None:
        for s in self.symbols:
            self._paused[self._norm_pair(s)] = True
        set_rm_paused_count(len(self.symbols))
        if reason:
            logger.warning("[RiskManager] Pause ALL (%s)", reason)

    def resume_all_symbols(self) -> None:
        for s in self.symbols:
            self._paused[self._norm_pair(s)] = False
        set_rm_paused_count(0)
        logger.info("[RiskManager] Resume ALL")

    def pause_symbols(self, symbols: List[str]) -> None:
        for s in symbols or []:
            self._paused[self._norm_pair(s)] = True
        set_rm_paused_count(sum(1 for v in self._paused.values() if v))

    def resume_symbols(self, symbols: List[str]) -> None:
        for s in symbols or []:
            self._paused[self._norm_pair(s)] = False
        set_rm_paused_count(sum(1 for v in self._paused.values() if v))

    def update_pairs(self, pairs: List[str]) -> None:
        self.symbols = [self._norm_pair(s) for s in (pairs or [])]

    def connect_transfer_clients(self, clients_by_exchange: Dict[str, Any]) -> None:
        self.transfer_clients.update({str(k).upper(): v for k, v in (clients_by_exchange or {}).items()})

    def set_virtual_balances(self, balances: Dict[str, Dict[str, Dict[str, float]]]) -> None:
        if not bool(getattr(self.cfg, "dry_run", False)):
            return
        self._virtual_balances = {
            str(ex).upper(): {al: dict(assets or {}) for al, assets in (accounts or {}).items()}
            for ex, accounts in (balances or {}).items()
        }

    def adjust_virtual_balance(self, exchange: str, account_alias: str, asset: str, delta: float) -> None:
        if not bool(getattr(self.cfg, "dry_run", False)):
            return
        ex = str(exchange).upper()
        al = str(account_alias)
        self._virtual_balances.setdefault(ex, {}).setdefault(al, {})
        self._virtual_balances[ex][al][asset] = float(self._virtual_balances[ex][al].get(asset, 0.0) + float(delta))

    # === DRY-RUN: seed/merge de soldes virtuels (alias-aware) =====================
    def seed_virtual_balances(
            self,
            balances_by_ex_alias: Dict[str, Dict[str, Dict[str, float]]],
            *,
            overwrite: bool = True,
    ) -> None:
        """
        Initialise/merge des soldes virtuels comme si MBF avait renvoyé un snapshot.
        Format:
          {"BINANCE":{"TT":{"USDC":2000,"EUR":2000},"TM":{...}}, "BYBIT":{...}, "COINBASE":{...}}
        """
        if not bool(getattr(self.cfg, "dry_run", False)):
            return
        if overwrite or not hasattr(self, "_virtual_balances"):
            self._virtual_balances = {}
        for ex, per_alias in (balances_by_ex_alias or {}).items():
            exu = str(ex).upper()
            dst = self._virtual_balances.setdefault(exu, {})
            for alias, ccy_map in (per_alias or {}).items():
                a = str(alias).upper()
                dst[a] = {str(ccy).upper(): float(v) for ccy, v in (ccy_map or {}).items()}

    def set_mm_budgets(self, budgets_by_ex: Dict[str, Dict[str, float]]) -> None:
        """
        Initialise/merge les budgets de liquidité MM par exchange et par quote.
        Format: {"BINANCE":{"USDC": 25000.0, "EUR": 0.0}, "BYBIT":{"USDC": 15000.0}}
        Utilisés par _build_bundle_for_mm() pour réserver 50/50.
        """
        for ex, qmap in (budgets_by_ex or {}).items():
            exu = str(ex).upper()
            dst = self.virt_balances.setdefault(exu, {})
            for q, v in (qmap or {}).items():
                dst[str(q).upper()] = float(v or 0.0)
            # observabilité si gauge branchée
            try:
                if INVENTORY_USD:
                    for q, v in dst.items():
                        INVENTORY_USD.labels(exchange=exu, quote=q).set(v)
            except Exception:
                logger.debug("INVENTORY_USD gauge update failed", exc_info=False)


    # === Métadonnées symbole & micro-règles CEX (tick/step/minQty/minNotional) ====
    def set_symbol_meta(self, meta_by_ex: Dict[str, Dict[str, Dict[str, float]]]) -> None:
        """
        meta_by_ex[EX][PAIR] = {"tickSize":0.01,"stepSize":0.0001,"minQty":0.001,"minNotional":10.0}
        Appelle ça depuis ta discovery/router si tu as ces infos.
        """
        self._symbol_meta = {}
        for ex, per_pk in (meta_by_ex or {}).items():
            exu = str(ex).upper()
            for pk, d in (per_pk or {}).items():
                self._symbol_meta.setdefault(exu, {})[str(pk).replace("-", "").upper()] = {
                    "tickSize": float((d or {}).get("tickSize", 0.0) or 0.0),
                    "stepSize": float((d or {}).get("stepSize", 0.0) or 0.0),
                    "minQty": float((d or {}).get("minQty", 0.0) or 0.0),
                    "minNotional": float((d or {}).get("minNotional", 0.0) or 0.0),
                }

    @staticmethod
    def _quantize(self, x: float, step: float, *, up: bool = False) -> float:
        if step <= 0:
            return float(x)
        k = x / step
        return (math.ceil(k) if up else math.floor(k)) * step

    def apply_symbol_rules(
            self, exchange: str, pair_key: str, *, side: str, price: float, qty: float
    ) -> Dict[str, Any]:
        """
        Applique tickSize/stepSize, puis vérifie minQty & minNotional.
        Retour: {"ok":bool,"price":float,"qty":float,"reason":str|None}
        """
        ex = str(exchange).upper()
        pk = self._norm_pair(pair_key)
        meta = (getattr(self, "_symbol_meta", {}) or {}).get(ex, {}).get(pk, {})
        tick = float(meta.get("tickSize", 0.0) or 0.0)
        step = float(meta.get("stepSize", 0.0) or 0.0)
        min_qty = float(meta.get("minQty", 0.0) or 0.0)
        min_notional = float(meta.get("minNotional", 0.0) or 0.0)

        p = float(price);
        q = float(qty)
        if tick > 0:
            p = self._quantize(p, tick, up=(str(side).upper() == "BUY"))
        if step > 0:
            q = max(self._quantize(q, step, up=True), step)

        if min_qty > 0 and q + 1e-15 < min_qty:
            return {"ok": False, "price": p, "qty": q, "reason": f"minQty {min_qty} > {q}"}

        if min_notional > 0 and (p * q) + 1e-9 < min_notional:
            need_q = min_notional / max(p, 1e-12)
            if step > 0:
                need_q = self._quantize(need_q, step, up=True)
            if (p * need_q) + 1e-9 < min_notional:
                return {"ok": False, "price": p, "qty": need_q,
                        "reason": f"minNotional {min_notional} > {p * need_q:.4f}"}
            q = need_q

        return {"ok": True, "price": p, "qty": q, "reason": None}

    def compute_cex_min_notional_usdc(self, exchange: str, pair_key: str) -> float:
        """
        Renvoie le min notional **dans la devise de cotation** (USDC/EUR) en tenant compte:
        - minNotional si disponible
        - minQty * meilleur prix sinon
        - sinon 0.0 (pas d’info)
        """
        ex = str(exchange).upper()
        pk = self._norm_pair(pair_key)
        meta = (getattr(self, "_symbol_meta", {}) or {}).get(ex, {}).get(pk, {})
        min_notional = float(meta.get("minNotional", 0.0) or 0.0)
        min_qty = float(meta.get("minQty", 0.0) or 0.0)

        if min_notional > 0:
            return float(min_notional)

        if min_qty > 0:
            bid, ask = self.get_top_of_book(ex, pk, enforce_fresh=False)
            px = ask if ask > 0 else (bid if bid > 0 else 0.0)
            if px > 0:
                return float(min_qty * px)

        return 0.0

    # === Jitter réseau (simulateur/engine) ========================================
    def simulate_network_jitter_ms(self) -> Dict[str, int]:
        """
        Latences 'ack' et 'fill' simulées (ms) — lis depuis cfg si présent:
          jitter_ack_ms_min/max, jitter_fill_ms_min/max
        """
        try:
            j_ack_min = int(getattr(self.cfg, "jitter_ack_ms_min", 8))
            j_ack_max = int(getattr(self.cfg, "jitter_ack_ms_max", 25))
            j_fill_min = int(getattr(self.cfg, "jitter_fill_ms_min", 25))
            j_fill_max = int(getattr(self.cfg, "jitter_fill_ms_max", 120))
        except Exception:
            j_ack_min, j_ack_max, j_fill_min, j_fill_max = 8, 25, 25, 120
        if j_ack_min > j_ack_max: j_ack_min, j_ack_max = j_ack_max, j_ack_min
        if j_fill_min > j_fill_max: j_fill_min, j_fill_max = j_fill_max, j_fill_min
        return {"ack_ms": random.randint(j_ack_min, j_ack_max),
                "fill_ms": random.randint(j_fill_min, j_fill_max)}

    # === Booking virtuel (TT) avec frais ==========================================
    def book_virtual_fees_and_balances_after_fill(
            self,
            *,
            buy_ex: str, sell_ex: str,
            alias_buy: str, alias_sell: str,
            base_ccy: str, quote_ccy: str,
            qty_base: float, buy_price: float, sell_price: float,
    ) -> None:
        """
        Impacter les soldes virtuels après un TT (avec frais taker).
        """
        if not bool(getattr(self.cfg, "dry_run", False)):
            return
        tb = float(self.get_fee_pct(buy_ex, f"{base_ccy}{quote_ccy}", "taker"))
        ts = float(self.get_fee_pct(sell_ex, f"{base_ccy}{quote_ccy}", "taker"))
        usdc_in = sell_price * qty_base * (1.0 - ts)
        usdc_out = buy_price * qty_base * (1.0 + tb)
        self.adjust_virtual_balance(buy_ex, alias_buy, quote_ccy, -usdc_out)
        self.adjust_virtual_balance(buy_ex, alias_buy, base_ccy, +qty_base)
        self.adjust_virtual_balance(sell_ex, alias_sell, quote_ccy, +usdc_in)
        self.adjust_virtual_balance(sell_ex, alias_sell, base_ccy, -qty_base)

    # ------------------------------------------------------------------
    # Scanner / Engine / Fee-sync hooks (+ PATCH bridge Scanner)
    # ------------------------------------------------------------------
    def connect_fee_sync_clients(self, clients_by_ex_alias: Dict[str, Dict[str, Any]]) -> None:
        if self.slip_collector and hasattr(self.slip_collector, "set_fee_sync_clients"):
            self.slip_collector.set_fee_sync_clients(clients_by_ex_alias)

    # --- PATCH: pont Scanner ---
    def set_scanner(self, scanner) -> None:
        """Appelée depuis le main après instanciation."""
        self._scanner_ref = scanner

    def _emit_slippage_to_scanner(self) -> None:
        """Construit un payload bulk et le pousse au Scanner si présent."""

        if not self._scanner_ref:
            return
        payload = {}  # {EX: {PAIR: {buy, sell, recent}}}
        thr_by_pair = {}  # {PAIR: threshold_bps}

        # P0: On boucle sur TOUS les exchanges et TOUTES les paires configurées
        # pour garantir que le Scanner reçoit du slippage (heartbeats) même pour les paires stables.
        for ex in self.exchanges:
            for pk in self.symbols:
                sb = ss = recent = None
                # 1) priorité SlippageHandler s'il est branché (TTL déjà appliqué)
                if self.slippage_handler and hasattr(self.slippage_handler, "get_slippage_bps"):
                    try:
                        sb_bps = self.slippage_handler.get_slippage_bps(ex, pk, "buy")
                        ss_bps = self.slippage_handler.get_slippage_bps(ex, pk, "sell")
                        if sb_bps is not None and ss_bps is not None:
                            sb = float(sb_bps) / 1e4
                            ss = float(ss_bps) / 1e4
                    except Exception:
                        logging.exception("Unhandled exception")
                # 2) fallback collector “recent” uniquement si handler absent
                elif self.slip_collector:
                    try:
                        recent = float(self.slip_collector.get_recent_slippage(pk))
                    except Exception:
                        recent = None
                    if recent is not None:
                        sb = ss = recent

                if sb is None and ss is None and recent is None:
                    continue

                payload.setdefault(ex, {})[pk] = {
                    "buy": sb if sb is not None else None,
                    "sell": ss if ss is not None else None,
                    "recent": recent if recent is not None else None,
                }

                # seuil préfiltre slippage (en bps)
                thr_bps = float(getattr(self.cfg, "prefilter_slip_bps", 30.0))
                thr_by_pair[pk] = thr_bps

        try:
            if payload:
                # push en bulk (non bloquant)
                self._scanner_ref.ingest_slippage_bulk(payload, threshold_by_pair_bps=thr_by_pair)
        except Exception:
            logger.debug("[RiskManager] push slippage → Scanner failed", exc_info=False)

    # === risk_manager.py ===
    # Dans la classe RiskManager


    # ---------------------------------------------------------------------
    # REMPLACEMENT 1/3
    # ---------------------------------------------------------------------
    # ---------------------------------------------------------------------
    def on_scanner_opportunity(self, opp: Dict[str, Any], decision_ctx: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        start_rm_ns = time.perf_counter_ns()
        # Instrumentation latence queue Scanner -> RM (P1)
        tracing_enabled = should_trace_latency(self.bot_cfg)

        if tracing_enabled:
            scanner_exit_ns = opp.get("scanner_exit_ns")
            if scanner_exit_ns:
                queue_ms = (start_rm_ns - int(scanner_exit_ns)) / 1e6
                if getattr(self.bot_cfg.obs, "enable_segment_metrics_scanner", True):
                    record_pipeline_latency("scanner_to_rm_queue", queue_ms, exchange=opp.get("buy_exchange", "none"))

        """
        P2: scheduler multi-branches TT/TM/MM simultanés + caps notionnels et préemption MM,
            REB lock par combo, shadow non-bloquant.
        - TTL-strict slip/vol (2s/5s)
        - Caps par (branche,CEX) via _apply_caps_and_preempt (legacy opp-level, préemption MM réservée au hedge)
        - REB: lock combo et exécute TM neutral
        P1: Validation de schéma à la frontière Scanner -> RM.
        """
        if not opp:
            return None
        # Canonisation défensive pour compat scanner_consumer (payload encapsulé).
        try:
            self._canonize_opportunity(opp)
        except Exception:
            pass
        required = ("branch", "pair", "legs")
        for f in required:
            if opp.get(f) is None:
                logger.warning("[RiskManager] P1: Schema violation from scanner, missing field: %s", f)
                return None
        
        legs = opp.get("legs", [])
        if not isinstance(legs, (list, tuple)) or not legs:
            logger.warning("[RiskManager] P1: Schema violation from scanner, invalid or empty legs")
            return None

        now = time.monotonic()
        decision_ctx = decision_ctx if isinstance(decision_ctx, dict) else {"submitted": False, "attempted": False,
                                                                    "reasons": []}
        # P0A (fallback): si le Scanner fournit book_ts_ms / book_age_ms, on met à jour
        # last_orderbook_recv_ts_ms pour garder book_age disponible même si la loop OB stalle.
        try:
            book_ts_ms = int(opp.get("book_ts_ms") or opp.get("book_ts") or 0)
            if not book_ts_ms:
                decision_ts_ms = int(opp.get("decision_ts_ms") or opp.get("ts") or 0)
                book_age_ms = opp.get("book_age_ms")
                if decision_ts_ms and book_age_ms is not None:
                    book_ts_ms = decision_ts_ms - int(book_age_ms)
            if book_ts_ms > 0:
                self.last_orderbook_recv_ts_ms = int(book_ts_ms)
                self.books_last_recv_ts_ms = int(book_ts_ms)
        except Exception:
            pass


        def _record_reason(reason: str) -> None:
            if not reason:
                return
            decision_ctx.setdefault("reasons", []).append(str(reason))

        pair = opp.get("pair") or opp.get("symbol") or "UNKNOWN"
        notional_quote = opp.get("notional_quote")
        if (
                not isinstance(notional_quote, dict)
                or not str(notional_quote.get("ccy") or "").strip()
                or float(notional_quote.get("amount") or 0.0) <= 0.0
        ):
            _record_reason(RM_NOTIONAL_QUOTE_INVALID)
            try:
                if RM_DROPPED_TOTAL is not None:
                    RM_DROPPED_TOTAL.labels(RM_NOTIONAL_QUOTE_INVALID).inc()
            except Exception:
                pass
            try:
                inc_rm_reject(reason=RM_NOTIONAL_QUOTE_INVALID, pair=pair)
            except Exception:
                pass
            return decision_ctx

        pair_quote = _pair_quote(pair)
        notional_ccy = str(notional_quote.get("ccy") or "").upper()
        if notional_ccy != pair_quote:
            _record_reason(RM_QUOTE_MISMATCH)
            try:
                if RM_DROPPED_TOTAL is not None:
                    RM_DROPPED_TOTAL.labels(RM_QUOTE_MISMATCH).inc()
            except Exception:
                pass
            try:
                inc_rm_reject(reason=RM_QUOTE_MISMATCH, pair=pair)
            except Exception:
                pass
            return decision_ctx

        # --- 0) Freshness guards (TTL strict) ------------------------------------
        # On aligne le TTL sur les briques réellement utilisées :
        # - Slippage : priorité au SlippageAndFeesCollector (slip_collector) si dispo,
        #   sinon slippage_handler legacy.
        # - Volatilité : priorité au vol_monitor, fallback éventuel sur vol_manager.
        # Le contrat TTL "officiel" vient de BotConfig.slo[mode][exchange].public.
        slip_age_ok = True
        vol_age_ok = True
        slip_age_s = None
        vol_age_s = None
        slip_src = "none"
        vol_src = "none"

        # Tier hint (optionnel) : si absent, on considère PRIMARY => strict
        opp_tier = str(
            opp.get("tier")
            or opp.get("lhm_tier")
            or opp.get("cohort")
            or opp.get("bucket")
            or "PRIMARY"
        ).upper()

        # TTL strict sur les tiers critiques (CORE/PRIMARY). Sur les autres tiers, on peut tolérer "unknown"
        ttl_strict = opp_tier in ("CORE", "PRIMARY")

        # --- TTL de base (fallback legacy) : BotConfig.slip/vol.ttl_s ------------
        try:
            slip_ttl_s = float(getattr(getattr(self.bot_cfg, "slip", None), "ttl_s", 2.0))
        except Exception:
            slip_ttl_s = 2.0
        try:
            vol_ttl_s = float(getattr(getattr(self.bot_cfg, "vol", None), "ttl_s", 5.0))
        except Exception:
            vol_ttl_s = 5.0

        # --- Override TTL via contrat SLO, si disponible -------------------------
        # On prend le min des TTL publics sur les 2 CEX de l'opportunité
        try:
            slo_map = getattr(self.bot_cfg, "slo", None)
            if slo_map:
                g_cfg = _cfg_g(self)
                mode_key = str(getattr(g_cfg, "deployment_mode", "SPLIT")).upper()
                per_ex = slo_map.get(mode_key) or {}

                route = opp.get("route") or {}
                buy_ex = (opp.get("buy_ex") or route.get("buy_ex") or "").upper()
                sell_ex = (opp.get("sell_ex") or route.get("sell_ex") or "").upper()

                public_slos = []
                for ex in (buy_ex, sell_ex):
                    if not ex:
                        continue
                    path_slo = per_ex.get(ex)
                    if path_slo is not None and getattr(path_slo, "public", None) is not None:
                        public_slos.append(path_slo.public)

                if public_slos:
                    slip_ttl_s = min(
                        float(getattr(ps, "slip_ttl_s", slip_ttl_s))
                        for ps in public_slos
                    )
                    vol_ttl_s = min(
                        float(getattr(ps, "vol_ttl_s", vol_ttl_s))
                        for ps in public_slos
                    )
        except Exception:
            # En cas de souci sur slo, on garde les TTL legacy BotConfig.slip/vol
            if getattr(self, "log", None):
                self.log.warning(
                    "RM: unable to resolve SLO-based TTL for slip/vol, falling back "
                    "to BotConfig.slip/vol.ttl_s",
                    exc_info=False,
                )

        # Slippage TTL (contrat public)
        slip_col = getattr(self, "slip_collector", None)
        if slip_col and hasattr(slip_col, "last_age_seconds"):
            try:
                slip_age_s = float(slip_col.last_age_seconds(opp) or 0.0)
                slip_src = "collector"
            except Exception:
                slip_age_s = None

        if slip_age_s is None:
            slip_getter = getattr(self, "slippage_handler", None)
            if slip_getter and hasattr(slip_getter, "last_age_seconds"):
                try:
                    slip_age_s = float(slip_getter.last_age_seconds(opp) or 0.0)
                    slip_src = "handler"
                except Exception:
                    slip_age_s = None

        if slip_age_s is not None:
            slip_age_ok = float(slip_age_s) <= float(slip_ttl_s)
        else:
            # Unknown => stale sur CORE/PRIMARY (fail-closed). Sur tiers non-critiques, on tolère.
            slip_age_ok = (not ttl_strict)

        # Volatilité TTL (contrat public)
        vol_getter = getattr(self, "vol_monitor", None)
        if vol_getter and hasattr(vol_getter, "last_age_seconds"):
            try:
                vol_age_s = float(vol_getter.last_age_seconds(opp) or 0.0)
                vol_src = "monitor"
            except Exception:
                vol_age_s = None

        if vol_age_s is None:
            vm = getattr(self, "vol_manager", None)
            if vm and hasattr(vm, "last_age_seconds"):
                try:
                    vol_age_s = float(vm.last_age_seconds(opp) or 0.0)
                    vol_src = "manager"
                except Exception:
                    vol_age_s = None

        if vol_age_s is not None:
            vol_age_ok = float(vol_age_s) <= float(vol_ttl_s)
        else:
            # TTL strict : si on ne sait pas dater la vol, on considère stale.
            vol_age_ok = False

        if not (slip_age_ok and vol_age_ok):
            if getattr(self, "log", None):
                self.log.debug(
                    "RM.SKIP: stale slip/vol (TTL P2) slip_age_s=%.3f vol_age_s=%.3f "
                    "slip_src=%s vol_src=%s slip_ttl_s=%.3f vol_ttl_s=%.3f",
                    float(slip_age_s or -1.0),
                    float(vol_age_s or -1.0),
                    slip_src,
                    vol_src,
                    float(slip_ttl_s),
                    float(vol_ttl_s),
                )
            return

        # --- 1) Contexte & combo --------------------------------------------------
        buy_ex = (opp.get("buy_ex") or opp.get("route", {}).get("buy_ex") or "").upper()
        sell_ex = (opp.get("sell_ex") or opp.get("route", {}).get("sell_ex") or "").upper()
        combo_key = f"{self._norm_pair(pair)}|{pair_quote}|{buy_ex}->{sell_ex}"

        if not hasattr(self, "_reb_locks"): self._reb_locks = {}

        # --- 2) REB lock ----------------------------------------------------------
        if self._reb_locks.get(combo_key, 0) > now:
            if getattr(self, "log", None): self.log.debug(f"RM.REB_LOCK active for {combo_key}")
            _record_reason(REB_LOCK)
            try:
                from modules.obs_metrics import REBAL_LOCK_CONFLICT_TOTAL
                REBAL_LOCK_CONFLICT_TOTAL.labels(pair=str(pair).upper(), route=f"{buy_ex}->{sell_ex}").inc()
                inc_rm_reject(reason=REB_LOCK, pair=pair)
            except Exception:
                pass
            return

        needs_reb = False
        try:
            needs_reb = bool(getattr(self, "needs_rebalance_for_combo")(combo_key))
        except Exception:
            needs_reb = False

        if needs_reb:
            lock_ttl = float(getattr(self.cfg.rm, "rebal_lock_ttl_s", 15.0))
            self._reb_locks[combo_key] = now + lock_ttl


        # --- 3) Budgets d’in-flight (branche×profil) & pacer ----------------------
        # Hiérarchie des caps:
        # 1) caps_trading_by_profile (cfg.rm) si présent
        # 2) fallback legacy (NANO→LARGE) + pacer (NORMAL/CONSTRAINED/SEVERE)
        rm_cfg = getattr(getattr(self, "cfg", None), "rm", None)
        profile_name = (getattr(self, "capital_profile", None) or "LARGE").upper()
        pacer = getattr(self, "pacer", None)

        def _base_cap_for_branch(branch: str) -> int:
            """
            Cap brut (avant pacer) pour la branche demandée.
            Lit d'abord cfg.rm.caps_trading_by_profile si disponible,
            sinon fallback legacy.
            """
            try:
                if rm_cfg and getattr(rm_cfg, "caps_trading_by_profile", None):
                    caps_by_profile = rm_cfg.caps_trading_by_profile or {}
                    prof_caps = caps_by_profile.get(profile_name) or caps_by_profile.get("LARGE", {})
                    return int(prof_caps.get(branch.upper(), 0) or 0)
            except Exception:
                pass

            # Fallback legacy si cfg.rm n'est pas encore patché
            legacy_caps = {
                "NANO": {"TT": 2, "TM": 1, "MM": 1},
                "MICRO": {"TT": 3, "TM": 2, "MM": 1},
                "SMALL": {"TT": 4, "TM": 3, "MM": 2},
                "MID": {"TT": 6, "TM": 4, "MM": 3},
                "LARGE": {"TT": 8, "TM": 6, "MM": 4},
            }

            # Visibiliser l'usage du fallback legacy (log une seule fois par profil)
            try:
                used = getattr(self, "_rm_caps_legacy_profiles", None)
                if used is None:
                    used = set()
                    setattr(self, "_rm_caps_legacy_profiles", used)

                if profile_name not in used:
                    used.add(profile_name)
                    logger = getattr(self, "logger", None)
                    if logger is not None:
                        try:
                            logger.warning(
                                "[RM] caps_trading_by_profile absent ou vide — "
                                "fallback legacy caps pour profil %s",
                                profile_name,
                            )
                        except Exception:
                            pass
            except Exception:
                # On ne casse jamais sur de la télémétrie
                pass

            prof_caps = legacy_caps.get(profile_name, legacy_caps["LARGE"])
            return int(prof_caps.get(branch.upper(), 0) or 0)

        def _cap(branch: str) -> int:
            """
            Cap effectif (après pacer et overlays de sécurité).
            factor_for_branch ne doit JAMAIS augmenter le cap au-delà du base.
            """
            base = _base_cap_for_branch(branch)
            factor = 1.0
            if pacer and hasattr(pacer, "factor_for_branch"):
                try:
                    factor = float(pacer.factor_for_branch(branch))
                except Exception:
                    factor = 1.0

            # 2) Overlay de sécurité (AT_RISK / DEGRADED)
            if factor > 0.0:
                try:
                    # On applique une réduction agressive si le statut global RM est dégradé.
                    if getattr(self, "trading_state", "READY") in ("DEGRADED", "AT_RISK"):
                        # On réduit de 75% supplémentaires
                        factor *= 0.25
                except Exception:
                    pass

            # Clamp dur pour éviter tout upscale accidentel
            if factor > 1.0:
                factor = 1.0
            if factor < 0.0:
                factor = 0.0

            return max(0, int(round(base * factor)))

        # Caps TRADING par branche pour CETTE opportunité (TT/TM/MM uniquement)
        caps = {"TT": _cap("TT"), "TM": _cap("TM"), "MM": _cap("MM")}

        # Observabilité des caps RM (profil×branche) — Macro M2-4
        # On exporte à la fois le cap "base" (théorique) et le cap "effectif" (après pacer / overlays)
        try:
            try:
                from . import obs_metrics  # type: ignore
            except Exception:  # pragma: no cover
                obs_metrics = None

            if obs_metrics is not None:
                base_g = getattr(obs_metrics, "RM_CAP_BASE_INFLIGHT_USD", None)
                eff_g = getattr(obs_metrics, "RM_CAP_EFFECTIVE_INFLIGHT_USD", None)
                if base_g is not None or eff_g is not None:
                    for _branch in ("TT", "TM", "MM"):
                        try:
                            _base_val = float(_base_cap_for_branch(_branch))
                        except Exception:
                            _base_val = 0.0
                        try:
                            _eff_val = float(caps.get(_branch, 0) or 0)
                        except Exception:
                            _eff_val = 0.0

                        if base_g is not None:
                            try:
                                base_g.labels(
                                    profile=profile_name,
                                    branch=_branch,
                                ).set(_base_val)
                            except Exception:
                                pass

                        if eff_g is not None:
                            try:
                                eff_g.labels(
                                    profile=profile_name,
                                    branch=_branch,
                                ).set(_eff_val)
                            except Exception:
                                pass
        except Exception:
            # On ne casse jamais la décision pour un problème de télémétrie
            pass

        # --- 4) Éligibilités de base ----------------------------------------------
        # On utilise la stratégie choisie par le Scanner (TT, TM, MM_MONO, MM_CROSS, REB)
        opp_strategy = str(opp.get("strategy") or opp.get("branch") or "TT").upper()
        strategies: List[str] = [opp_strategy]

        # Mapping pour les caps et les gardes MM
        is_mm = self._is_mm_family(opp_strategy)
        
        if is_mm:
            if not getattr(self, "enable_mm", False):
                strategies = []
            else:
                # Support complet MM_MONO et MM_CROSS
                ok, why = self._is_mm_admissible_from_hints(opp)
                if not ok:
                    strategies = []
                    _record_reason(why)
                    try:
                        inc_rm_reject(reason=why, pair=pair)
                    except Exception:
                        pass

        # --- 5) Caps notionnels par CEX & préemption MM ---------------------------
        def _desired_notional(opp: Dict[str, Any]) -> float:
            v = opp.get("notional_usdc") or (opp.get("notional_quote") or {}).get("amount")
            try:
                return float(v or 0.0)
            except Exception:
                return 0.0

        desired = _desired_notional(opp)
        min_trade_usdc = float(getattr(self, "min_trade_usdc", 50.0))

        engine = getattr(self, "engine", None)
        sent_any = False
        mm_dual_attempted = False
        mm_dual_enqueued = False

        if not engine:
            if getattr(self, "log", None):
                self.log.error("RM.on_scanner_opportunity sans engine attaché")
            _record_reason(RM_ENGINE_NOT_READY)
            return decision_ctx

        # 5.a) TT/TM: utilisation directe des caps TT/TM par profil
        if opp_strategy in ("TT", "TM") and opp_strategy in strategies:
            cap_branch = caps.get(opp_strategy, 0)
            if cap_branch > 0 and desired >= min_trade_usdc:
                try:
                    bundle = self._build_bundle(opp, strategy=opp_strategy, decision_ctx=decision_ctx)
                    if bundle:
                        res_engine = self._multicast_shadow(bundle, decision_ctx=decision_ctx)
                        decision_ctx["attempted"] = True
                        decision_ctx["submitted"] = decision_ctx.get("submitted") or bool(res_engine)
                        sent_any = True
                except Exception:
                    if getattr(self, "log", None):
                        self.log.exception(f"RM.on_scanner_opportunity: erreur sur {opp_strategy}")

        # 5.b) MM dual : support MM_MONO / MM_CROSS (utilise caps "MM")
        if is_mm and "MM" in caps and caps["MM"] > 0 and not sent_any and strategies:
            mm_dual_attempted = True
            try:
                slot_notional = self._mm_slot_notional_for_profile(profile_name, min_trade_usdc=min_trade_usdc)
                opp_copy = dict(opp)
                opp_copy.setdefault("notional_usdc", slot_notional)
                opp_copy.setdefault("notional_quote", {"ccy": pair_quote, "amount": slot_notional})
                
                # On préserve le label spécifique (MM_MONO/MM_CROSS) pour le routage alias
                bundle = self._build_bundle(opp_copy, strategy=opp_strategy, decision_ctx=decision_ctx)
                if bundle:
                    res_engine = self._multicast_shadow(bundle, decision_ctx=decision_ctx)
                    decision_ctx["attempted"] = True
                    decision_ctx["submitted"] = decision_ctx.get("submitted") or bool(res_engine)
                    mm_dual_enqueued = True
                    sent_any = True
            except Exception:
                if getattr(self, "log", None):
                    self.log.exception(f"RM.on_scanner_opportunity: erreur sur {opp_strategy}")

        # 5.c) Fallback MM inventaire (single-maker) :
        # - on NE le déclenche que si MM dual a été tenté mais n'a pas fourni de bundle
        # - si MM n'est pas éligible (pas dans strategies / caps=0 / mode=CROSS / hints KO), rien ne se passe
        try:
            if (
                    getattr(self, "mm_inventory_enabled", False)
                    and mm_dual_attempted
                    and not mm_dual_enqueued
            ):
                self._maybe_fire_mm_inventory_single(opp, reason="dual_fallback")
        except Exception:
            if getattr(self, "log", None):
                self.log.exception("RM.on_scanner_opportunity: mm_inventory_single failed")
            # --- 6) REB (après TT/TM) : non-interférence -----------------------------
        if needs_reb and not sent_any:
            reb_bundle = self._build_bundle(opp, strategy="REB", decision_ctx=decision_ctx)
            if reb_bundle:
                self._multicast_shadow(reb_bundle)
                decision_ctx["attempted"] = True
                decision_ctx["submitted"] = decision_ctx.get("submitted") or True
        
        # Instrumentation latence RM (P1)
        if tracing_enabled and getattr(self.bot_cfg.obs, "enable_segment_metrics_rm", True):
            rm_ms = (time.perf_counter_ns() - start_rm_ns) / 1e6
            record_pipeline_latency("rm_proc", rm_ms, exchange=opp.get("buy_exchange", "none"))

        return decision_ctx


    # ---------------------------------------------------------------------
    # REMPLACEMENT 2/3
    # ---------------------------------------------------------------------
    def _build_reb_bundle_from_hint(
            self,
            hint: Dict[str, Any],
            *,
            hint_type: str,
    ) -> Optional[Dict[str, Any]]:
        hint_type_u = str(hint_type or hint.get("hint_type") or "").lower()
        h = hint or {}

        # Cap REB : inflight_rebal_by_profile (option pacer down-clamp)
        profile = str(getattr(self, "capital_profile", "LARGE") or "LARGE").upper()
        try:
            rm_cfg = getattr(getattr(self, "cfg", None), "rm", None)
            caps_reb = getattr(rm_cfg, "inflight_rebal_by_profile", {}) or {}
            inflight_cap = int(caps_reb.get(profile) or caps_reb.get(profile.title()) or caps_reb.get("LARGE") or 0)
        except Exception:
            inflight_cap = 0

        pacer_factor = 1.0
        pacer = getattr(self, "pacer", None)
        if pacer and hasattr(pacer, "factor_for_branch"):
            try:
                pacer_factor = float(pacer.factor_for_branch("REB"))
            except Exception:
                pacer_factor = 1.0
            if pacer_factor > 1.0:
                pacer_factor = 1.0
            if pacer_factor < 0.0:
                pacer_factor = 0.0
        try:
            inflight_cap = int(round(inflight_cap * pacer_factor))
        except Exception:
            inflight_cap = max(0, int(inflight_cap))

        if inflight_cap <= 0:
            raise RMError(REB_REJECT_CAP_EXCEEDED)

        buy_ex = str(
            h.get("buy_ex")
            or h.get("to_exchange")
            or (h.get("to") or {}).get("exchange")
            or h.get("exchange")
            or ""
        ).upper()
        sell_ex = str(
            h.get("sell_ex")
            or h.get("from_exchange")
            or (h.get("from") or {}).get("exchange")
            or ""
        ).upper()

        if hint_type_u == "crypto_topup" and not sell_ex:
            sell_ex = buy_ex

        asset = str(
            h.get("asset")
            or h.get("base")
            or h.get("ccy")
            or ""
        ).upper()
        quote = str(
            h.get("valuation_quote")
            or h.get("quote")
            or _pair_quote(h.get("pair") or h.get("symbol") or "")
            or "USDC"
        ).upper()

        pair = h.get("pair") or h.get("symbol")
        if not pair and asset:
            pair = f"{asset}{quote}"

        if not pair or not buy_ex:
            logger.info(
                "[RiskManager] _build_reb_bundle_from_hint ignoré (missing_pair_or_route): %s",
                hint,
            )
            return None

        try:
            pk = self._norm_pair(pair)
        except Exception:
            pk = pair

        def _top_prices(exchange: str, default_bid: float = 0.0, default_ask: float = 0.0) -> Tuple[float, float]:
            bid, ask = default_bid, default_ask
            try:
                b, a = self.get_top_of_book(exchange, pk)
                bid = float(b or bid)
                ask = float(a or ask)
            except Exception:
                books = getattr(self, "_last_books", {}) or {}
                book = books.get(exchange, {}).get(pk, {}) or {}
                try:
                    bid = float(book.get("best_bid", bid) or bid)
                    ask = float(book.get("best_ask", ask) or ask)
                except Exception:
                    pass
            return bid, ask

        sell_bid, _ = _top_prices(sell_ex)
        _, buy_ask = _top_prices(buy_ex)

        amount_quote = float(
            h.get("notional_usdc")
            or h.get("notional")
            or h.get("exposure_quote")
            or h.get("amount_quote")
            or 0.0
        )

        qty = float(h.get("qty") or h.get("amount") or 0.0)

        if hint_type_u == "crypto_topup":
            try:
                topup_qty = float(h.get("topup_amount") or 0.0)
            except Exception:
                topup_qty = 0.0
            if topup_qty > 0.0:
                qty = topup_qty
            if amount_quote <= 0.0 and buy_ask > 0.0:
                amount_quote = abs(qty) * buy_ask

        if amount_quote <= 0.0 and sell_bid > 0.0 and buy_ask > 0.0 and qty != 0.0:
            # Fallback: approx notional à partir de la quantité et du meilleur px disponible
            amount_quote = abs(qty) * max(buy_ask, sell_bid)

        if qty <= 0.0 and buy_ask > 0.0:
            qty = abs(amount_quote) / buy_ask if amount_quote else 0.0

        if qty <= 0.0 or buy_ask <= 0.0:
            logger.info(
                "[RiskManager] _build_reb_bundle_from_hint ignoré (missing_qty_or_price): %s",
                hint,
            )
            return None

        opp = {
            "type": "rebalancing",
            "pair": pair,
            "buy_ex": buy_ex,
            "sell_ex": sell_ex or buy_ex,
            "qty": qty,
            "notional_usdc": abs(amount_quote) if amount_quote else abs(qty) * buy_ask,
            "buy_px": buy_ask,
            "sell_px": sell_bid or buy_ask,
        }

        meta = dict(h.get("meta") or {})
        meta.setdefault("branch", "REB")
        meta.setdefault("strategy", "REB")
        meta.setdefault("type", "rebalancing")
        meta.setdefault("hint_type", hint_type_u)
        meta.setdefault("allow_final_loss_bps", float(getattr(self, "rebal_allow_loss_bps", 0.0)))
        meta.setdefault("allow_loss_bps", float(getattr(self, "rebal_allow_loss_bps", 0.0)))
        opp["meta"] = meta

        bundle = self._build_bundle(opp, strategy="REB")
        if not bundle:
            return None

        meta = bundle.setdefault("meta", {}) or {}
        meta.setdefault("branch", "REB")
        meta.setdefault("strategy", "REB")
        meta.setdefault("type", "rebalancing")
        meta.setdefault("hint_type", hint_type_u)
        meta.setdefault("allow_final_loss_bps", float(getattr(self, "rebal_allow_loss_bps", 0.0)))
        meta.setdefault("allow_loss_bps", float(getattr(self, "rebal_allow_loss_bps", 0.0)))
        bundle["meta"] = meta

        tm_meta = bundle.setdefault("tm", {}) or {}
        tm_meta.setdefault("mode", "NEUTRAL")
        tm_meta.setdefault("hedge_ratio", 1.0)
        bundle["tm"] = tm_meta

        return bundle

    def _map_reb_sim_reason(self, reason: str) -> str:
        r = str(reason or "").upper()
        if "BPS" in r or "EDGE" in r:
            return REB_SIM_REJECT_BPS
        if "LATENCY" in r or "QUEUEPOS" in r or "TTL" in r:
            return REB_SIM_REJECT_LATENCY
        if "CAP" in r:
            return REB_REJECT_CAP_EXCEEDED
        return REB_SIM_REJECT_GUARD

    def _record_reb_notional_from_hint(self, bundle: Dict[str, Any]) -> None:
        try:
            route = bundle.get("route") or {}
            meta = bundle.get("meta") or {}
            profile = str(
                bundle.get("profile")
                or meta.get("profile")
                or getattr(self, "capital_profile", "LARGE")
                or "LARGE"
            ).upper()
            quote = str(route.get("quote") or meta.get("quote") or "NA").upper()

            notional = 0.0
            notional_quote = route.get("notional_quote")
            if isinstance(notional_quote, dict):
                notional = float(notional_quote.get("amount") or 0.0)
            elif notional_quote is not None:
                notional = float(notional_quote)

            if notional <= 0.0:
                try:
                    notional = float(self._estimate_bundle_notional_usd(bundle))
                except Exception:
                    notional = 0.0

            if notional <= 0.0:
                return

            RM_REB_FROM_HINTS_NOTIONAL_QUOTE_TOTAL.labels(
                profile=profile,
                quote=quote,
            ).inc(abs(notional))
        except Exception:
            pass

    def _submit_reb_bundle_from_hint(self, hint: Dict[str, Any], *, hint_type: str) -> bool:
        """Fabrique puis soumet un bundle REB via la pipeline standard.

        Retourne True si un bundle a été construit et soumis, False sinon.
        """

        builder = getattr(self, "_build_reb_bundle_from_hint", None)
        if not callable(builder):
            logger.info(
                "[RiskManager] %s laissé en attente de bundle: %s",
                f"{hint_type}_hint",
                hint,
            )
            return False

        mapped_reason: Optional[str] = None
        try:
            bundle = builder(dict(hint), hint_type=hint_type)
        except RMError as exc:
            mapped_reason = self._map_reb_sim_reason(getattr(exc, "reason", "") or str(exc))
            bundle = None
        except Exception:
            mapped_reason = REB_SIM_REJECT_GUARD
            bundle = None

        if not bundle:
            mapped_reason = mapped_reason or self._map_reb_sim_reason(RM_REB_FACTORY_REJECT)
            try:
                inc_rm_reject(reason=mapped_reason)
                inc_rm_reject(reason=RM_REB_FACTORY_REJECT)
            except Exception:
                pass
            self._reb_hint_event(mapped_reason, status="factory_reject", hint=hint)
            logger.info(
                "[RiskManager] %s rejeté par la fabrique: %s",
                f"{hint_type}_hint",
                hint,
            )
            return False

        bundle.setdefault("branch", bundle.get("meta", {}).get("branch", "REB"))
        self._record_reb_notional_from_hint(bundle)
        self._reb_hint_event(REB_HINT_EXEC_REB_TM_NEUTRAL, status="bundle_submitted", hint=hint)
        self._multicast_shadow(bundle)
        return True

    def _build_bundle(
            self,
            opp: Dict[str, Any],
            strategy: str,
            decision_ctx: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        P0: construit un bundle exécutable standard (dict) en s'appuyant sur payloads.make_submit_bundle
        et payloads.submit_leg_from_intent. La fragmentation est suggérée par le Simulateur (suggest_slices)
        et embarquée en meta (cohortes G1/G2/G3).
        """
        meta = dict(opp.get("meta") or {})

        def _record_decision_reason(reason: str) -> None:

            if decision_ctx is None:
                return
            if reason:
                decision_ctx.setdefault("reasons", []).append(str(reason))

        pair = opp.get("pair") or opp.get("symbol")
        buy_ex = opp.get("buy_ex") or opp.get("route", {}).get("buy_ex")
        sell_ex = opp.get("sell_ex") or opp.get("route", {}).get("sell_ex")
        pk = self._norm_pair(pair or "")

        route = {"buy_ex": buy_ex, "sell_ex": sell_ex, "pair": pair}
        profile = str(getattr(self, "capital_profile", "LARGE") or "LARGE").upper()
        tif = "IOC" if strategy in ("TT", "TM") else "GTC"
        client_id = getattr(self, "client_id", "default")
        notional = opp.get("notional_quote")
        if (
                not isinstance(notional, dict)
                or not str(notional.get("ccy") or "").strip()
                or float(notional.get("amount") or 0.0) <= 0.0
        ):
            _record_decision_reason(RM_NOTIONAL_QUOTE_INVALID)
            try:
                inc_rm_reject(reason=RM_NOTIONAL_QUOTE_INVALID, pair=pair, route=f"{buy_ex}->{sell_ex}")
            except Exception:
                pass
            return None
        pair_quote = _pair_quote(pair or "")
        if str(notional.get("ccy") or "").upper() != pair_quote:
            _record_decision_reason(RM_QUOTE_MISMATCH)
            try:
                inc_rm_reject(reason=RM_QUOTE_MISMATCH, pair=pair, route=f"{buy_ex}->{sell_ex}")
            except Exception:
                pass
            return None

        strategy_u = str(strategy or "").upper()
        if strategy_u in ("TT", "TM"):
            base_asset = self._pair_base(pk)
            tttm_entry = self.tttm_exposure_state.get(base_asset, {}) or {}
            tttm_state = str(tttm_entry.get("state", "OK") or "OK").upper()
            if tttm_entry.get("hard_breach"):
                tttm_state = "HARD"
            elif tttm_entry.get("soft_breach") and tttm_state == "OK":
                tttm_state = "SOFT"

            if tttm_entry.get("hard_breach"):
                meta.setdefault("tm_exposure_mode", "HARD_BREACH")
            elif tttm_entry.get("soft_breach"):
                meta.setdefault("tm_exposure_mode", "SOFT_BREACH")
            if tttm_state == "HARD":
                inc_rm_reject(reason=RM_TTTM_DELTA_HARD_LIMIT)
                return None
            if tttm_state == "SOFT":
                try:
                    RM_TTTM_DELTA_SOFT_HIT.labels(asset=base_asset, branch=strategy_u).inc()
                except Exception:
                    pass

            is_mm_branch = strategy_u in ("MM", "MM_MONO", "MM_CROSS")
            if is_mm_branch:
                # Injection de la variante par défaut "neutral" si absente
                if "mm_variant" not in meta:
                    meta.setdefault("mm_variant", "neutral")

                base_asset = self._pair_base(pk)
                # Calcul du drift d'inventaire pour injection de skews (Inventory Target / Mean-Revert)
                try:
                    snap = self._balances_snapshot()
                    drift_usd = self._mm_compute_inventory_drift(buy_ex, "MM", base_asset, snap)
                    p_skew, s_skew = self._compute_mm_inventory_skews(buy_ex, pk, drift_usd)
                    meta["inv_price_skew"] = float(p_skew)
                    meta["inv_size_skew"] = float(s_skew)
                    meta["inv_drift_usd"] = float(drift_usd)
                except Exception:
                    pass
                delta_state = self.mm_delta_state.get(base_asset, {}).get("state", "OK")
                if delta_state == "HARD":
                    inc_rm_reject(reason=RM_MM_DELTA_HARD_LIMIT)
                    return None
                if delta_state == "SOFT":
                    try:
                        RM_MM_DELTA_SOFT_HIT.labels(asset=base_asset).inc()
                    except Exception:
                        pass

                # Gate 2 — Latency/ack penalty application (Ticket Gate 2)
                latency_penalty = getattr(self, "_mm_latency_penalty_level", 0)
                if latency_penalty == 1:  # SOFT
                    pad_on_bad_ack = float(getattr(self, "mm_pad_on_bad_ack", 1.0))
                    size_on_bad_ack = float(getattr(self, "mm_size_on_bad_ack", 0.7))
                    lifetime_mult_on_bad_ack = float(getattr(self, "mm_lifetime_mult_on_bad_ack", 2.0))

                    meta.setdefault("mm_latency_pad_ticks", pad_on_bad_ack)
                    meta.setdefault("mm_latency_size_factor", size_on_bad_ack)
                    meta.setdefault("mm_latency_lifetime_mult", lifetime_mult_on_bad_ack)

                    # Réduction directe du notionnel pour le bundle
                    if notional and "amount" in notional:
                        try:
                            old_amt = float(notional["amount"])
                            notional["amount"] = old_amt * size_on_bad_ack
                        except Exception:
                            pass

            ex_for_mm = (buy_ex or sell_ex or "").upper()
            if is_mm_branch and buy_ex and sell_ex and str(buy_ex).upper() != str(sell_ex).upper() and strategy_u == "MM_MONO":
                inc_rm_reject(reason="MM_MODE_MONO_REQUIRED")
                return None
            mm_quote = str(
                (notional or {}).get("ccy") or (notional or {}).get("quote") or _pair_quote(pair or "")).upper()
            try:
                mm_notional = float((notional or {}).get("amount") or opp.get("notional_usdc") or 0.0)
            except Exception:
                mm_notional = float(opp.get("notional_usdc") or 0.0)
            if is_mm_branch and mm_notional <= 0.0:
                try:
                    if RM_MM_BUDGET_EXHAUSTED_TOTAL is not None:
                        RM_MM_BUDGET_EXHAUSTED_TOTAL.labels(profile, ex_for_mm or "UNKNOWN", mm_quote).inc()
                except Exception:
                    pass
                _record_decision_reason(RM_MM_BUDGET_EXHAUSTED)
                inc_rm_reject(reason=RM_MM_BUDGET_EXHAUSTED)
                return None


        try:
            total = float((notional or {}).get("amount") or 0.0)
        except Exception:
            total = 0.0


        degraded = self._rm_guard_or_raise(pair_key=pk, buy_ex=buy_ex, sell_ex=sell_ex)



        # Intent(s) de base (côté RM/Scanner) -> legs Engine via helper
        intents: List[Dict[str, Any]] = opp.get("intents") or []
        legs: List[Dict[str, Any]] = [submit_leg_from_intent(it) for it in intents]

        # Si pas d’intents fournis : fallback TT/TM/MM simple (couvre P0)
        if not legs:
            side_buy, side_sell = ("BUY", "SELL")
            qty = float(opp.get("qty") or opp.get("size") or 0)
            pxb = float(opp.get("buy_px") or opp.get("px_buy") or 0)
            pxs = float(opp.get("sell_px") or opp.get("px_sell") or 0)
            if qty <= 0.0:
                buy_book = self.get_book_snapshot(buy_ex, pk)
                sell_book = self.get_book_snapshot(sell_ex, pk)
                if not (self._fresh_enough(buy_book) and self._fresh_enough(sell_book)):
                    _record_decision_reason(RM_MARKETDATA_STALE)
                    try:
                        inc_rm_reject(reason=RM_MARKETDATA_STALE)
                    except Exception:
                        pass
                    return None
                if pxb <= 0.0:
                    pxb = float(buy_book.get("best_ask") or 0.0)
                if pxs <= 0.0:
                    pxs = float(sell_book.get("best_bid") or 0.0)
                ref_px = pxb if pxb > 0 else pxs
                if ref_px > 0.0:
                    qty = float(notional.get("amount") or 0.0) / ref_px
                if qty <= 0.0:
                    _record_decision_reason(RM_OPP_BAD_INPUTS)
                    try:
                        inc_rm_reject(reason=RM_OPP_BAD_INPUTS)
                    except Exception:
                        pass
                    return None
            if strategy in ("TT", "REB"):
                # Deux takers (ou TM NEUTRAL pour REB plus bas)
                legs = [
                    {"exchange": buy_ex, "account_alias": None, "side": side_buy, "symbol": pair, "qty": qty,
                     "px_limit": pxb, "tif": "IOC", "meta": {}},
                    {"exchange": sell_ex, "account_alias": None, "side": side_sell, "symbol": pair, "qty": qty,
                     "px_limit": pxs, "tif": "IOC", "meta": {}},
                ]
            elif strategy == "TM":
                # Maker côté sell_ex, taker côté buy_ex (exemple)
                legs = [
                    {"exchange": sell_ex, "account_alias": None, "side": side_sell, "symbol": pair, "qty": qty,
                     "px_limit": pxs, "tif": "GTC", "meta": {"maker": True}},
                    {"exchange": buy_ex, "account_alias": None, "side": side_buy, "symbol": pair, "qty": qty,
                     "px_limit": pxb, "tif": "IOC", "meta": {"hedge": True}},
                ]
            elif strategy in ("MM", "MM_MONO", "MM_CROSS"):
                # Deux makers (pose/annul), Engine gère cancel budget & hystérésis
                legs = [
                    {"exchange": buy_ex, "account_alias": None, "side": "BUY", "symbol": pair, "qty": qty,
                     "px_limit": pxb, "tif": "GTC", "meta": {"maker": True}},
                    {"exchange": sell_ex, "account_alias": None, "side": "SELL", "symbol": pair, "qty": qty,
                     "px_limit": pxs, "tif": "GTC", "meta": {"maker": True}},
                ]

        # REB → converti en TM NEUTRAL (atomique par combo)
        mode = strategy
        if strategy == "REB":
            mode = "TM"  # Engine exécute TM NEUTRAL, lock combo géré par RM

        # Décision TM (mode + hedge) calculée côté RM, consommée par l'Engine.
        # Contrat canonique meta["tm"] (Ticket M3-A) :
        #   - mode ∈ {"NEUTRAL", "NN"} (libellé interne "NON_NEUTRAL" normalisé plus bas),
        #   - maker_side ∈ {"BUY", "SELL"},
        #   - hedge_ratio ∈ [0.0, 1.0],
        #   - max_exposure_s : horizon métier maximum d'exposition TM_NN (secondes).
        tm_meta: Optional[Dict[str, Any]] = None

        # Paramètres d'horizon TM utilisés comme fallback si le RM n'a pas encore posé max_exposure_s.
        # TTL canonique configuré, éventuellement modulé par prudence VM (down-clamp only).
        ttl_ms_cfg = int(self._get_tm_exposure_ttl_ms_for_exchange(buy_ex or sell_ex))
        ttl_ms_effective = ttl_ms_cfg
        try:
            prud = self._current_prudence(pk)
        except Exception:
            prud = "NORMAL"
        prud = str(prud or "NORMAL").upper()
        if prud == "CAREFUL":
            ttl_ms_effective = max(400, int(ttl_ms_cfg * 0.75))
        elif prud == "ALERT":
            ttl_ms_effective = max(300, int(ttl_ms_cfg * 0.50))

        tm_nn_max_exposure_s = float(
            getattr(
                self,
                "tm_nn_max_exposure_s",
                getattr(self.cfg, "tm_nn_max_exposure_s", 3.0),
            )
        )
        try:
            tm_neutral_max_exposure_s = float(ttl_ms_effective) / 1000.0
        except Exception:
            tm_neutral_max_exposure_s = tm_nn_max_exposure_s

        if mode == "TM":
            if strategy == "TM":
                # Notionnel en devise de cotation pour la décision TM
                try:
                    usdc_amt = float((notional or {}).get("amount") or opp.get("notional_usdc") or 0.0)
                except Exception:
                    usdc_amt = float(opp.get("notional_usdc") or 0.0)

                try:
                    tm_meta = self.decide_tm_mode(
                        pair_key=pk,
                        maker_ex=sell_ex,  # TM = maker sur la jambe SELL, hedge sur BUY
                        taker_ex=buy_ex,
                        usdc_amt=usdc_amt,
                    ) or {}
                except Exception:
                    # En cas d'erreur, on retombe sur un TM NEUTRAL standard.
                    tm_meta = {}

            elif strategy == "REB":
                # REB traité comme TM NEUTRAL "full hedge" avec horizon borné.
                max_expo_reb_s = min(tm_neutral_max_exposure_s, tm_nn_max_exposure_s)
                tm_meta = {
                    "mode": "NEUTRAL",
                    "hedge_ratio": 1.0,
                    "max_exposure_s": max_expo_reb_s,
                    "source": "RM_REB",
                }

            # Normalisation minimale pour l'Engine
            if tm_meta is None:
                tm_meta = {}

            mode_val = str(tm_meta.get("mode") or "").upper()
            if mode_val == "NON_NEUTRAL":
                tm_meta["mode"] = "NN"
            elif mode_val not in {"NEUTRAL", "NN"}:
                tm_meta["mode"] = "NEUTRAL"

            # Hedge ratio par défaut si absent : TTL hedge neutre (clé canonique).
            if "hedge_ratio" not in tm_meta:
                tm_meta["hedge_ratio"] = float(
                    getattr(
                        self,
                        "tm_exposure_ttl_hedge_ratio",
                        getattr(self, "tm_neutral_hedge_ratio", 0.50),
                    )
                )

            # max_exposure_s par défaut si absent : dépend du mode effectif.
            if "max_exposure_s" not in tm_meta:
                mode_eff = str(tm_meta.get("mode") or "NEUTRAL").upper()
                if mode_eff == "NN":
                    tm_meta["max_exposure_s"] = tm_nn_max_exposure_s
                else:
                    tm_meta["max_exposure_s"] = tm_neutral_max_exposure_s

            # maker_side par défaut si absent (utile pour audit, Engine n'en dépend pas).
            if "maker_side" not in tm_meta:
                tm_meta["maker_side"] = "SELL"
            else:
                tm_meta["maker_side"] = str(tm_meta["maker_side"]).upper()
            # NB: on ne touche PAS à hedge_ratio ici : il reste celui décidé par decide_tm_mode
            #     ou par la VM (override neutre). Le fallback canonique est déjà géré plus haut.


        # Fragmentation suggérée par le Simulateur (industry-grade)

        frag_meta = None
        simulator = getattr(self, "simulator", None)
        sim_cfg = getattr(getattr(self, "bot_cfg", None), "sim", None)
        sim_mode = str(getattr(sim_cfg, "simulator_mode", "ON")).upper()
        sim_wait_ms = int(getattr(sim_cfg, "sim_max_wait_ms_rm", 3) or 3)
        sim_bypass_live = bool(getattr(sim_cfg, "simulator_bypass_allowed_in_live", False))
        sim_plan = None
        sim_plan_age_ms = None
        sim_key = None
        if simulator and hasattr(simulator, "make_plan_key_from_payload"):
            try:
                sim_key = simulator.make_plan_key_from_payload(opp, strategy)
                if sim_key:
                    sim_plan = simulator.peek_plan(sim_key)
                    if sim_plan is None and hasattr(simulator, "wait_plan"):
                        try:
                            loop = asyncio.get_event_loop()
                            if loop.is_running():
                                # P1-1: No blocking wait if loop is running, rely on peek_plan already done
                                sim_plan = simulator.peek_plan(sim_key)
                            else:
                                sim_plan = loop.run_until_complete(simulator.wait_plan(sim_key, sim_wait_ms))
                        except Exception:
                            sim_plan = None
                    if sim_plan:
                        try:
                            sim_plan_age_ms = simulator.plan_age_ms(sim_key)
                        except Exception:
                            sim_plan_age_ms = None
            except Exception:

                sim_plan = None
        quote_ccy = str((notional or {}).get("ccy") or "USDC").upper()
        min_frag_map = getattr(self, "min_fragment_quote", {}) or {}
        eff_min_frag = float(min_frag_map.get(quote_ccy, getattr(self, "min_fragment_usdc", 200.0)))
        try:
            self.min_fragment_usdc = eff_min_frag
        except Exception:
            pass

        frag_plan = None
        if strategy in {"TT", "TM", "REB"}:
            frag_plan = sim_plan
            if frag_plan is None:
                if sim_mode == "ON":
                    _record_decision_reason("SIM_CACHE_MISS")
                    try:
                        inc_rm_reject(reason="SIM_CACHE_MISS", pair=pair)
                    except Exception:
                        pass
                    if decision_ctx is not None:
                        decision_ctx["simulator_cache_miss"] = True
                    return None

                # Fallback sur FragmentationPolicy si sim absent/bypass
                frag_plan = FragmentationPolicy.build(
                    total_quote=total,
                    quote=quote_ccy,
                    branch=strategy,
                    mode=str(getattr(self, "trade_mode", "NORMAL")),
                    config=self.cfg
                )
                if sim_mode == "BYPASS_SAFE":
                    decision_ctx["simulator_bypassed"] = True

        if frag_plan and (frag_plan.get("amounts") or []):
            amounts = frag_plan.get("amounts", [])
            groups = frag_plan.get("groups", []) or ["G1"]
            total_quote = float(frag_plan.get("total_quote", total) or total)
            first = amounts[0] if amounts else total
            
            # Enrichissement avec les champs Legacy pour compatibilité Engine
            frag_plan.update({
                "group": groups[0] if groups else "G1",
                "cohort": groups[0] if groups else "G1",
                "idx": 0,
                "total": len(amounts),
                "weight": (float(first) / total_quote) if total_quote > 0 and first else 1.0,
                "planned_notional_quote": float(first or total),
                "source": frag_plan.get("source", "STATIC"),
            })
            frag_meta = frag_plan


        # Contrôles TM (queuepos/TTL/hedge) en meta additifs (facultatifs)
        # Contrôles TM (queuepos/TTL/hedge) en meta additifs (facultatifs)
        tm_controls = None
        if mode == "TM":
            # Hedge ratio décisionnel = celui du RM dans tm_meta, sinon fallback neutre
            base_hr: Optional[float] = None
            if tm_meta:
                try:
                    base_hr = float(tm_meta.get("hedge_ratio"))
                except (TypeError, ValueError):
                    base_hr = None
            if base_hr is None:
                base_hr = float(
                    getattr(
                        self,
                        "tm_exposure_ttl_hedge_ratio",
                        getattr(self, "tm_neutral_hedge_ratio", 0.50),
                    )
                )

            # Canon : queuepos_max_ahead_usd + ttl_ms
            # On expose aussi queuepos_max_usd pour compat avec l'Engine existant.
            qpos = float(getattr(self, "tm_queuepos_max_ahead_usd", 25_000.0))
            ttl_ms = int(getattr(self, "tm_exposure_ttl_ms", 2500))
            eta_ms = int(getattr(self, "tm_queuepos_max_eta_ms", 0))

            tm_controls = {
                "queuepos_max_ahead_usd": qpos,
                "queuepos_max_usd": qpos,  # alias legacy
                "queuepos_max_eta_ms": eta_ms,
                "ttl_ms": ttl_ms,
                "hedge_ratio": base_hr,
            }


        # Merge overrides degraded (si présents)
        if degraded and isinstance(degraded.get("tm_controls"), dict):
            tm_controls = (tm_controls or {}) | degraded["tm_controls"]

        # SPLIT/EU_ONLY en meta additif (facultatif)
        split_mode = getattr(self, "split_mode", "EU_ONLY")  # "EU_ONLY"|"SPLIT"|"JP_ONLY"
        if split_mode in ("EU_ONLY", "SPLIT", "JP_ONLY"):
            base_delta_ms, skew_ms, stale_ms = self._split_latency_defaults(split_mode)
        if split_mode in ("EU_ONLY", "SPLIT"):
            split_meta = {
                "mode": split_mode,
                "skew_ms": skew_ms,
                "base_delta_ms": base_delta_ms,
                "stale_ms": stale_ms,
            }
        # [INSÉRER ICI — juste après 'caps_local |= degraded["caps"]' et avant 'if not legs:']
        # --- Guard pré-bundle: vol fraîche + coût total SFC (no fallback implicite)
        _pre_cost = self._prebundle_guard(
            pair=pair,
            route=route,
            side=strategy,  # "TT" | "TM" | "MM" (selon ta logique appelante)
            notional_quote=float(notional.get("amount") or 0.0),
        )
        # Optionnel: exposer _pre_cost dans le contexte/trace si tu le journalises

        # Caps par défaut (Ticket 10 : pilotés par BotConfig.RiskManagerCfg)
        rm_cfg = getattr(getattr(self, "cfg", None), "rm", None)
        strategy_u = str(strategy or "").upper()

        if strategy_u == "MM":
            if not bool(getattr(self, "ff_mm_enabled", False)):
                if getattr(self, "log", None):
                    self.log.info(
                        "[RiskManager] _build_bundle: MM disabled by ff_mm_enabled",
                    )
                return None
            trade_mode = str(getattr(self, "trade_mode", "NORMAL") or "NORMAL").upper()
            if trade_mode != "NORMAL" and bool(getattr(self, "ff_mm_opportunistic_gating_enforced", False)):
                if getattr(self, "log", None):
                    self.log.info(
                        "[RiskManager] _build_bundle: MM blocked by trade_mode=%s",
                        trade_mode,
                    )
                return None
        if strategy_u == "REB":
            if not bool(getattr(self, "ff_reb_enabled", False)):
                if getattr(self, "log", None):
                    self.log.info(
                        "[RiskManager] _build_bundle: REB disabled by ff_reb_enabled",
                    )
                return None
            trade_mode = str(getattr(self, "trade_mode", "NORMAL") or "NORMAL").upper()
            if trade_mode != "NORMAL":
                if getattr(self, "log", None):
                    self.log.info(
                        "[RiskManager] _build_bundle: REB blocked by trade_mode=%s",
                        trade_mode,
                    )
                return None

        # Validation stricte du contrat branch/profile (Macro 6-B-1)
        if strategy_u not in ALLOWED_BRANCHES:
            if getattr(self, "log", None):
                self.log.error(
                    "[RiskManager] _build_bundle: branche invalide %s (pair=%s route=%s)",
                    strategy_u,
                    pair,
                    f"{buy_ex}->{sell_ex}",
                )
            return None

        if profile not in ALLOWED_CAPITAL_PROFILES:
            if getattr(self, "log", None):
                self.log.error(
                    "[RiskManager] _build_bundle: capital_profile invalide %s (pair=%s route=%s)",
                    profile,
                    pair,
                    f"{buy_ex}->{sell_ex}",
                )
            return None

        profile_name = profile

        # 1) Point d'override éventuel (cap_hint_tt/tm/mm/reb si fixé par Boot)
        inflight_cap = getattr(self, "cap_hint_" + strategy.lower(), None)

        # 2) Sinon, on dérive des grilles BotConfig
        if inflight_cap is None and rm_cfg:
            try:
                # TRADING pur : TT / TM / MM variants
                if strategy_u in ("TT", "TM", "MM", "MM_MONO", "MM_CROSS"):
                    caps_by_profile = getattr(rm_cfg, "caps_trading_by_profile", {}) or {}
                    prof_caps = caps_by_profile.get(profile_name) or caps_by_profile.get("LARGE", {})
                    
                    # Fallback mapping pour les variants MM vers le cap MM générique
                    cap_key = strategy_u
                    if strategy_u in ("MM_MONO", "MM_CROSS") and strategy_u not in prof_caps:
                        cap_key = "MM"
                        
                    inflight_cap = int(prof_caps.get(cap_key, 0) or 0)

                # REB : budget séparé, exclusif par design
                elif strategy_u == "REB":
                    rebal_caps = getattr(rm_cfg, "inflight_rebal_by_profile", {}) or {}
                    inflight_cap = int((rebal_caps.get(profile_name) or rebal_caps.get("LARGE") or 0))
            except Exception:
                # On laisse inflight_cap tel quel (None) en cas de souci de config
                pass

        caps_local = {
            "inflight_cap": inflight_cap,
            "bundle_concurrency": getattr(self, "tt_bundle_concurrency_max", 3) if strategy_u == "TT" else None,
            "headroom_min": getattr(self, "inflight_headroom_min", 1),
        }

        # Merge overrides degraded si fournis
        if degraded and isinstance(degraded.get("caps"), dict):
            caps_local |= degraded["caps"]

        if not legs:
            raise RMError(RM_BUNDLE_EMPTY_PARAMS)


        for i, leg in enumerate(legs):
            q = float(leg.get("qty") or leg.get("quantity") or leg.get("size") or 0.0)
            px = float(leg.get("px_limit") or leg.get("price") or 0.0)
            if q <= 0.0 or px <= 0.0:
                inc_rm_reject(reason=RM_BUNDLE_EMPTY_PARAMS,
                              pair=pair,
                              route=f"{buy_ex}->{sell_ex}")
                raise RMError(f"{RM_BUNDLE_EMPTY_PARAMS} leg={i} qty={q} px={px}")

        leg_kind_default = "TAKER" if strategy_u in ("TT", "REB") else "MAKER_TM" if strategy_u == "TM" else "MAKER_MM"
        for leg in legs:
            meta_leg = leg.setdefault("meta", {}) or {}
            if not meta_leg.get("kind"):
                if meta_leg.get("maker") and strategy_u == "TM":
                    meta_leg["kind"] = "MAKER_TM"
                elif meta_leg.get("maker") and strategy_u in ("MM", "MM_MONO", "MM_CROSS"):
                    meta_leg["kind"] = "MAKER_MM"
                else:
                    meta_leg["kind"] = leg_kind_default
            leg["meta"] = meta_leg

        ids = self._ensure_canonical_ids(
            opp=opp,
            pair=pk,
            buy_ex=str(buy_ex or ""),
            sell_ex=str(sell_ex or ""),
            notional=notional or {},
            legs=legs,
            frag_meta=frag_meta,
        )
        decision_ts_ns = (
                opp.get("decision_ts_ns")
                or (int(opp.get("decision_ts_ms") or 0) * 1_000_000)
                or int(time.time_ns())
        )

        bundle = make_submit_bundle(
            legs=legs,
            route=route,
            mode=mode,
            tif=tif,
            notional_quote=notional,
            branch=strategy,
            profile=profile,
            frag=frag_meta,
            caps=caps_local,

            tm_controls=tm_controls,
            split=split_meta,
            shadow=False,
            client_id=client_id,
            trace_id=ids.get("trace_id"),
            decision_id=ids.get("decision_id"),
            bundle_id=ids.get("bundle_id"),
            idempotency_key=ids.get("idempotency_key"),
            ts_ns=decision_ts_ns,
        )
        base_for_meta = self._pair_base(pk)
        ex_for_state = ex_for_mm if strategy_u == "MM" else (buy_ex or sell_ex)
        if base_for_meta:
            meta_root = bundle.setdefault("meta", {}) or {}
            mm_state = self.mm_delta_state.get(base_for_meta, {}) if isinstance(self.mm_delta_state, dict) else {}
            meta_root.setdefault(
                "mm_delta_state",
                {
                    "asset": base_for_meta,
                    "status": mm_state.get("status", "UNKNOWN"),
                    "delta_usd": float(mm_state.get("delta_usd") or 0.0),
                },
            )
            try:
                reb_flag = bool(self.rm_is_asset_under_rebalancing(ex_for_state, base_for_meta))
            except Exception:
                reb_flag = False
            meta_root.setdefault("rebalancing_active", reb_flag)
            
            # Injection des paramètres d escalade SINGLE mode (Ticket 2)
            if strategy_u in ("MM", "MM_MONO", "MM_CROSS"):
                mm_mode = meta.get("mm_mode") or "DUAL"
                if mm_mode == "SINGLE":
                    # On cherche l état d inventaire pour cet exchange/alias/asset
                    inv_key = (str(ex_for_state).upper(), strategy_u, base_for_meta)
                    state = self.mm_inventory_mode_state.get(inv_key)
                    if state and state.get("mode") == "SINGLE":
                        phase = state.get("phase", 1)
                        meta_root["mm_single_phase"] = phase
                        if phase == 2:
                            meta_root["mm_single_pad_ticks"] = getattr(self, "mm_single_aggressive_pad_ticks", 0.0)
                            meta_root["mm_single_size_factor"] = getattr(self, "mm_single_aggressive_size_factor", 1.2)
                        
                        # Budget cancel & lifetime pour SINGLE
                        meta_root["mm_single_min_quote_lifetime_ms"] = getattr(self, "mm_single_min_quote_lifetime_ms", 1000)
                        meta_root["mm_single_cancel_budget"] = getattr(self, "mm_single_cancel_budget", 30)

            bundle["meta"] = meta_root

        # Injection explicite de la décision TM dans le payload pour l'Engine
        if tm_meta and isinstance(bundle, dict):
            bundle.setdefault("tm", tm_meta)

        if isinstance(bundle, dict):
            meta_root = bundle.setdefault("meta", {}) or {}
            meta_root.setdefault("kind", leg_kind_default)
            meta_root.setdefault("buy_ex", buy_ex)
            meta_root.setdefault("sell_ex", sell_ex)
            meta_root.setdefault("quote", pair_quote)
            opportunity_id = (
                    opp.get("opportunity_id")
                    or opp.get("opp_id")
                    or (opp.get("meta") or {}).get("opportunity_id")
            )
            if opportunity_id:
                meta_root.setdefault("opportunity_id", opportunity_id)
            meta_root.setdefault("decision_id", ids.get("decision_id"))
            meta_root.setdefault("trace_id", ids.get("trace_id"))
            meta_root.setdefault("bundle_id", ids.get("bundle_id"))
            meta_root.setdefault("idempotency_key", ids.get("idempotency_key"))
            meta_root.setdefault("strategy_tag", strategy_u)
            if strategy_u in ("TM", "REB"):
                meta_root.setdefault("route_id", ids.get("route_id") or route.get("route_id") or route.get("id"))
            combo = self._bundle_combo_signature(bundle)
            if combo:
                meta_root.setdefault("combo_signature", self._format_combo_signature(combo))
            fx_rate_used = (
                    opp.get("fx_rate_used")
                    or (opp.get("meta") or {}).get("fx_rate_used")
            )
            if strategy_u == "REB":
                meta_root.setdefault("kind", "REB_OP")
            if fx_rate_used is not None:
                meta_root.setdefault("fx_rate_used", fx_rate_used)

            # Overlays et versioning TT (Ticket D1 & D2)
            if strategy_u == "TT":
                meta_root.setdefault("tt_policy_version", getattr(self, "tt_policy_version", "v1"))
                overlays = self._compute_tt_revalidation_overlays(buy_ex, sell_ex, pk)
                meta_root.setdefault("overlays", {}).update(overlays)

            bundle["meta"] = meta_root
            if strategy_u == "TT" and isinstance(bundle, dict):
                missing = fraglib.tt_contract_missing_fields(bundle)
                if missing:
                    if getattr(self, "log", None):
                        self.log.error(
                            "[RiskManager] _build_bundle: TT contract missing fields=%s trace_id=%s",
                            ",".join(missing),
                            ids.get("trace_id"),
                        )
                    meta_root = bundle.setdefault("meta", {}) or {}
                    meta_root.setdefault("rm_drop_reason", "TT_CONTRACT_INVALID")
                    bundle["meta"] = meta_root
                    return None
            if strategy_u == "TM" and isinstance(bundle, dict):
                missing = fraglib.tm_contract_missing_fields(bundle)
                if missing:
                    if getattr(self, "log", None):
                        self.log.error(
                            "[RiskManager] _build_bundle: TM contract missing fields=%s trace_id=%s",
                            ",".join(missing),
                            ids.get("trace_id"),
                        )
                    meta_root = bundle.setdefault("meta", {}) or {}
                    meta_root.setdefault("rm_drop_reason", "TM_CONTRACT_INVALID")
                    bundle["meta"] = meta_root
                    return None
            if strategy_u == "REB" and isinstance(bundle, dict):
                missing = fraglib.reb_contract_missing_fields(bundle)
                if missing:
                    if getattr(self, "log", None):
                        self.log.error(
                            "[RiskManager] _build_bundle: REB contract missing fields=%s trace_id=%s",
                            ",".join(missing),
                            ids.get("trace_id"),
                        )
                    meta_root = bundle.setdefault("meta", {}) or {}
                    meta_root.setdefault("rm_drop_reason", "REB_CONTRACT_INVALID")
                    bundle["meta"] = meta_root
                    return None
        try:
            fsm_payload = {
                "event_type": "DECISION",
                "trace_id": ids.get("trace_id"),
                "decision_id": ids.get("decision_id"),
                "bundle_id": ids.get("bundle_id"),
                "idempotency_key": ids.get("idempotency_key"),
                "client_oid": ids.get("idempotency_key"),
                "strategy_tag": str(strategy_u),
                "kind": leg_kind_default,
                "meta": bundle.get("meta") if isinstance(bundle, dict) else {},
            }
            self._emit_trade_fsm_event("DECISION", fsm_payload)
        except Exception:
            pass
        if strategy_u == "MM" and isinstance(bundle, dict):
            meta_mm = bundle.setdefault("meta", {}) or {}
            meta_mm.setdefault("branch", "MM")
            meta_mm.setdefault("strategy", "MM")
            meta_mm.setdefault("capital_profile", "MM")
            meta_mm.setdefault("mm_mode", "DUAL")

        # --- Contrat M3-3 : TTL MM owner = RM (per-bundle) -----------------
        if strategy_u == "MM":
            # TTL d'exposition pour MM : calculé côté RM, envoyé dans le bundle.
            # Priorité :
            hints_mm = ((opp.get("hints") or {}).get("MM") or {})
            try:
                ttl_hint = int(hints_mm.get("ttl_ms") or 0)
            except Exception:
                ttl_hint = 0
            ttl_ms = None
            try:
                cfg_root = getattr(self, "cfg", None)
                rm_cfg = getattr(cfg_root, "rm", None) if cfg_root else None
            except Exception:
                rm_cfg = None

            ttl_min = int(getattr(rm_cfg, "mm_ttl_min_ms", 300)) if rm_cfg else 300
            ttl_max = int(getattr(rm_cfg, "mm_ttl_max_ms", 5000)) if rm_cfg else 5000
            ttl_cfg = getattr(rm_cfg, "mm_exposure_ttl_ms", None) if rm_cfg else None
            if ttl_cfg:
                ttl_ms = ttl_cfg
            elif ttl_hint > 0:
                ttl_ms = ttl_hint
            elif rm_cfg is not None:
                ttl_ms = getattr(rm_cfg, "tm_exposure_ttl_ms", None)

            try:
                ttl_ms = int(ttl_ms or 2500)
            except Exception:
                ttl_ms = 2500
            ttl_ms = max(ttl_min, min(ttl_ms, ttl_max))

            # On pose le TTL directement au niveau bundle, pour que l'Engine
            # puisse l'utiliser sans reconsulter la config.
            bundle["ttl_ms"] = ttl_ms


        return bundle

    # ---------------------------------------------------------------------
    # REMPLACEMENT 3/3
    # ---------------------------------------------------------------------
    def _multicast_shadow(self, bundle: Dict[str, Any], decision_ctx: Optional[Dict[str, Any]] = None) -> bool:
        # 1) Envoi Engine (non-bloquant, avec fallbacks gérés)

        engine_ok = self.engine_enqueue_bundle(bundle, decision_ctx=decision_ctx)
        # 2) Shadow Simu (non-bloquant, sampling interne)
        self.shadow_simulate(bundle, l2_cache=getattr(self, "l2_cache", None))
        # 3) Comptage des budgets (branch × profil × combo)
        if engine_ok:
            self._record_budget_spend(bundle.get("branch"), bundle)

        # 4) Observabilité expo (M3-4) : notional par branche/profil/combo
        #    On ne logue que TM/MM/REB, via la helper dédiée.
        try:
            self._record_exposure_for_bundle(bundle.get("branch"), bundle)
        except Exception:
            # Observabilité best-effort, jamais bloquante.
            logging.debug(
                "[RiskManager] _multicast_shadow: échec _record_exposure_for_bundle",
                exc_info=False,
            )
        return bool(engine_ok)


    # ------------------------------------------------------------------
    # Engine routing avec verrous (exchange, alias, pair)
    # ------------------------------------------------------------------
    def _pair_key(self, ex: str, alias: str, pair: str) -> Tuple[str, str, str]:
        return (ex.upper(), alias.upper(), self._norm_pair(pair))

    # === risk_manager.py ===
    # Dans class RiskManager


    async def _submit_with_pairlocks(self, order_bundle: dict):
        """
        P1: Envoi sous verrous par combo (pair|buy->sell|alias), avec:
          - Readiness Engine obligatoire (sinon defer/skip soft),
          - Timeout d'ack borné + retentative unique si réseau,
          - Pacer/backpressure: dégrade si 429/5xx,
          - Mute de route temporaire si incidents répétés,
          - Journalisation de reason codes (metrics-friendly).
        """
        # --- 0) Clé de verrou & init ---------------------------------------------
        route = order_bundle.get("route", {})
        pair = route.get("pair")
        buy_ex = route.get("buy_ex")
        sell_ex = route.get("sell_ex")
        alias = (order_bundle.get("legs") or [{}])[0].get("account_alias")
        combo_key = f"{pair}|{buy_ex}->{sell_ex}|{alias or 'NA'}"

        if not hasattr(self, "_pairlocks"):
            self._pairlocks = {}
        if combo_key not in self._pairlocks:
            self._pairlocks[combo_key] = asyncio.Lock()

        # --- 1) Guards de readiness ---------------------------------------------
        engine = getattr(self, "engine", None)
        is_ready = True
        if engine and hasattr(engine, "is_ready"):
            try:
                is_ready = bool(engine.is_ready())
            except Exception:
                is_ready = True
        if not is_ready:
            # Skip soft: on ne casse pas le lock mais on n’envoie pas tant que l’Engine n’est pas ready
            if getattr(self, "log", None):
                self.log.warning("RM.SKIP_READY: engine not ready")
            try:
                inc_rm_skip(RM_ENGINE_NOT_READY)
            except Exception:
                pass
            return


        # --- 2) Envoi sérialisé sous verrou -------------------------------------
        async with self._pairlocks[combo_key]:

            submit_timeout = float(getattr(self, "engine_submit_timeout_s", 3.0))
            max_retries_net = int(getattr(self, "engine_net_retry", 1))  # une retentative réseau max
            attempt = 0
            last_exc = None

            while attempt <= max_retries_net:
                attempt += 1
                try:
                    r = None
                    # Instrumentation latence RM exit (P1)
                    if should_trace_latency(self.bot_cfg):
                        order_bundle["rm_exit_ns"] = time.perf_counter_ns()

                    # Appel Engine *bloquant* (on respecte les verrous ici)
                    if engine and hasattr(engine, "submit"):
                        r = engine.submit(order_bundle)
                    elif engine and hasattr(engine, "place_bundle"):
                        # Compat ancien chemin sync
                        r = engine.place_bundle(order_bundle)

                    if inspect.isawaitable(r):
                        await asyncio.wait_for(r, timeout=submit_timeout)

                    # Succès -> métriques
                    if hasattr(self, "obs_inc"): self.obs_inc("rm_submit_ok_total", branch=order_bundle.get("branch"),
                                                              combo=combo_key)
                    return
                except asyncio.TimeoutError as e:
                    last_exc = e
                    reason = ENGINE_SUBMIT_TIMEOUT
                    if getattr(self, "log", None):
                        self.log.warning(f"RM.{reason} combo={combo_key}")
                    try:
                        if hasattr(self, "obs_inc"):
                            self.obs_inc("engine_reject_total", reason=reason)
                    except Exception:
                        pass
                    # P1: on ne boucle pas indéfiniment -> on tente une seule fois de plus si autorisé
                except Exception as e:
                    last_exc = e
                    reason = getattr(e, "reason", None) or str(e)
                    degrade = False
                    if isinstance(e, Exception) and not isinstance(reason, str):
                        reason = str(reason)
                    if str(reason).upper() in {ENGINE_NACK_429, ENGINE_NACK_5XX}:
                        degrade = True
                    if getattr(self, "log", None):
                        self.log.exception(f"RM.ENGINE_REJECT combo={combo_key} reason={reason}")
                    try:
                        if hasattr(self, "obs_inc"):
                            self.obs_inc("engine_reject_total", reason=str(reason))
                    except Exception:
                        pass

                    # Dégrader le pacer et/ou muter temporairement la route si itérations
                    if degrade:
                        try:
                            if hasattr(self, "pacer") and hasattr(self.pacer, "degrade"):
                                self.pacer.degrade(source="engine_error")
                        except Exception:
                            pass


                # Retentative si possible
                if attempt <= max_retries_net:
                    await asyncio.sleep(min(0.250 * attempt, 1.0))  # petit backoff

            # Echec final -> mute court de la route pour éviter le martelage
            try:
                mute_s = int(getattr(self, "route_mute_after_fail_s", 30))
                if hasattr(self, "mute_route_for"):
                    self.mute_route_for(
                        buy_ex,
                        sell_ex,
                        pair,
                        ttl_s=mute_s,
                        reason=normalize_reason_code(ReasonCodes.ENGINE_ERRORS) or ReasonCodes.ENGINE_ERRORS,
                    )
            except Exception:
                pass
            if getattr(self, "log", None): self.log.error(f"RM.FAIL combo={combo_key} (final)")
            return

    async def _route_to_engine(self, order_bundle: Dict[str, Any]) -> None:
        """
        order_bundle structure:
          { "type":"arbitrage_bundle", "pair":"ETHUSDC",
            "legs":[{exchange, alias, side, symbol, volume_usdc, volume_quote?, quote?}, ...],
            "strategy":"TT"|"TM", "tm":{...}?, "metadata":{...} }
        """
        try:
            meta = order_bundle.get("meta") or {}
            fsm_payload = {
                "event_type": "ENQUEUE",
                "trace_id": meta.get("trace_id") or order_bundle.get("trace_id"),
                "decision_id": meta.get("decision_id") or order_bundle.get("decision_id"),
                "bundle_id": meta.get("bundle_id") or order_bundle.get("bundle_id"),
                "idempotency_key": meta.get("idempotency_key") or order_bundle.get("idempotency_key"),
                "client_oid": meta.get("client_id") or meta.get("idempotency_key"),
                "strategy_tag": meta.get("strategy") or meta.get("branch") or order_bundle.get("branch"),
                "kind": meta.get("kind"),
                "meta": meta,
            }
            self._emit_trade_fsm_event("ENQUEUE", fsm_payload)
        except Exception:
            pass
        engine = getattr(self, "engine", None)
        if engine is None:
            try:
                if self.simulator and hasattr(self.simulator, "before_submit"):
                    order_bundle = self.simulator.before_submit(order_bundle) or order_bundle
            except Exception:
                logging.exception("Unhandled exception")
            logger.info("[RiskManager] (dry-route) %s", order_bundle)
            return

        try:
            if self.simulator and hasattr(self.simulator, "before_submit"):
                order_bundle = self.simulator.before_submit(order_bundle) or order_bundle
                self.shadow_simulate(order_bundle, l2_cache=getattr(self, "l2_cache", None))
            await self._submit_with_pairlocks(order_bundle)
        except Exception:
            logger.exception("[RiskManager] engine.submit failed")

    async def on_private_event(self, evt: dict) -> None:
        """
        RM consomme les FILL/PARTIAL (type="fill") provenant du Hub / Engine.

        Contrat minimal attendu pour evt:
          - type="fill"
          - status in {"FILL", "PARTIAL"}
          - exchange, alias (normalisés en majuscules ici)
          - symbol, side
          - au moins un identifiant: client_id (clé RM/Engine) ou exchange_order_id
          - fill_px, base_qty, quote, quote_qty (quote = devise de cotation)
          - ts_exchange, ts_local
          - meta.source ("ws" | "poller" | "resync_*")

        Rôle:
          - reality-check fees (BF) -> peut marquer vip_stale pour refresh,
          - reconciler : observe + resync async (ordre -> alias),
          - slippage : observation facultative.
        """
        if not evt:
            return

        ev = dict(evt or {})
        if str(ev.get("type") or "").lower() == "pws_health":
            reason = normalize_reason_code(ev.get("reason")) if ev.get("reason") else None
            if reason == "PWS_UNSAFE_DEDUP":
                try:
                    self._handle_pws_unsafe_dedup({
                        "reason": reason,
                        "exchange": ev.get("exchange"),
                        "alias": ev.get("alias"),
                        "kind": ev.get("kind"),
                    })
                except Exception:
                    logger.exception("[RiskManager] pws_health unsafe_dedup handling failed")
                return
            status = {
                "pws_health": {
                    "critical_drop_seen": True,
                    "last_critical_drop_reason": ev.get("reason"),
                },
            }
            self._update_pws_critical_drop_from_status(status)
            return
        status = str(ev.get("status") or ev.get("type") or "").upper()
        etype = str(ev.get("type") or "").lower()
        rec = getattr(self, "reconciler", None)
        if rec and hasattr(rec, "mark_ws_activity"):
            try:
                rec.mark_ws_activity()
            except Exception:
                pass
        if rec and hasattr(rec, "note_seen_idempotency_key"):
            try:
                meta = ev.get("meta") or {}
                idk = meta.get("idempotency_key") or ev.get("idempotency_key")
                rec.note_seen_idempotency_key(idk)
            except Exception:
                pass
        # Branche dédiée pour les transferts internes (PrivateWSHub).
        if etype in ("transfer", "pws_transfer"):
            try:
                self._on_internal_transfer_event(ev)
            except Exception:
                logging.exception("RM: on_private_event transfer handling failed")
                try:
                    self._emit_private_plane_event(
                        "transfer_event_failed",
                        exchange=str(ev.get("exchange") or "NA").upper(),
                        alias=str(ev.get("alias") or "NA").upper(),
                        error="on_private_event transfer handling failed",
                    )
                except Exception:
                    # On ne casse jamais la boucle RM pour un problème d'event.
                    pass
            return

        if status not in ("FILL", "FILLED", "PARTIAL", "PARTIAL_FILL", "CANCEL", "CANCELED", "CANCELLED"):
            return


        exchange = str(ev.get("exchange") or "NA").upper()
        alias = str(ev.get("alias") or "NA").upper()
        ev["exchange"] = exchange
        ev["alias"] = alias
        try:
            meta = ev.get("meta") or {}
            fsm_payload = {
                "event_type": "FILL" if status in ("FILL", "FILLED", "PARTIAL", "PARTIAL_FILL") else "CANCEL",
                "trace_id": meta.get("trace_id") or ev.get("trace_id"),
                "decision_id": meta.get("decision_id") or ev.get("decision_id"),
                "bundle_id": meta.get("bundle_id") or ev.get("bundle_id"),
                "idempotency_key": meta.get("idempotency_key") or ev.get("idempotency_key"),
                "client_oid": ev.get("client_id") or meta.get("client_id") or meta.get("idempotency_key"),
                "strategy_tag": meta.get("strategy") or meta.get("branch"),
                "kind": meta.get("kind") or etype,
                "meta": meta,
                "status": status,
            }
            self._emit_trade_fsm_event(fsm_payload["event_type"], fsm_payload)
        except Exception:
            pass
        def _handle_error(reason: str, exc: Exception) -> None:
            logger.exception("[RiskManager] %s", reason)
            self._emit_private_plane_event(
                reason,
                exchange=exchange,
                alias=alias,
                error=str(exc),
            )

        # Validation contrat FILL/PARTIAL pour les events type="fill"
        if etype == "fill":
            from contracts.payloads import normalize_private_fill_event, validate_private_fill_event_lite  # type: ignore

            ev = normalize_private_fill_event(ev)
            ev["exchange"] = exchange
            ev["alias"] = alias
            required = ("symbol", "side", "fill_px", "base_qty", "quote", "quote_qty")
            missing = [name for name in required if ev.get(name) in (None, "")]
            has_id = bool(ev.get("client_id") or ev.get("exchange_order_id"))

            if missing or not has_id:
                reason = "missing_fields" if missing else "missing_id"
                # Métrique optionnelle (no-op si non définie dans obs_metrics)
                try:
                    from modules.obs_metrics import RM_INVALID_PRIVATE_EVENT_TOTAL  # type: ignore
                except Exception:
                    RM_INVALID_PRIVATE_EVENT_TOTAL = None  # type: ignore

                try:
                    if RM_INVALID_PRIVATE_EVENT_TOTAL is not None:
                        RM_INVALID_PRIVATE_EVENT_TOTAL.labels(
                            exchange=exchange,
                            alias=alias,
                            reason=reason,
                        ).inc()
                except Exception:
                    pass

                try:
                    logger.warning(
                        "[RiskManager] on_private_event: drop invalid fill "
                        "reason=%s missing=%s has_id=%s head=%s",
                        reason,
                        missing,
                        has_id,
                        {
                            "exchange": exchange,
                            "alias": alias,
                            "symbol": ev.get("symbol"),
                            "side": ev.get("side"),
                            "status": ev.get("status"),
                            "type": ev.get("type"),
                            "client_id": ev.get("client_id"),
                            "exchange_order_id": ev.get("exchange_order_id"),
                        },
                    )
                except Exception:
                    pass
                return
            ok, fill_model = validate_private_fill_event_lite(ev)
            if not ok or fill_model is None:
                try:
                    logger.warning(
                        "[RiskManager] on_private_event: drop invalid fill after validation head=%s",
                        {
                            "exchange": exchange,
                            "alias": alias,
                            "symbol": ev.get("symbol"),
                            "side": ev.get("side"),
                            "status": ev.get("status"),
                            "type": ev.get("type"),
                            "client_id": ev.get("client_id"),
                            "exchange_order_id": ev.get("exchange_order_id"),
                        },
                    )
                except Exception:
                    pass
                return

            ev = fill_model.model_dump()

        # reality-check fees (passif, à partir d'un event déjà validé)
        bf = getattr(self, "balance_fetcher", None)
        if bf and hasattr(bf, "observe_fill_fee_reality_check"):
            try:
                bf.observe_fill_fee_reality_check(ev)
            except Exception as exc:
                _handle_error("bf_reality_check_failed", exc)

                # Reconciler: observe + resync async pour les fills valides
        if etype == "fill" and rec:
            try:
                rec.observe_fill_event(ev)
            except Exception as exc:
                _handle_error("reconciler_observe_failed", exc)
            else:
                try:
                    import asyncio
                    oid = (
                            ev.get("exchange_order_id")
                            or ev.get("client_id")
                            or ev.get("order_id")
                    )
                    id_kind = "exchange_order_id" if ev.get("exchange_order_id") else (
                        "client_id" if ev.get("client_id") else "order_id"
                    )
                    task = rec.correlate_and_maybe_resync(exchange, alias, oid, id_kind=id_kind)
                    if asyncio.iscoroutine(task):
                        asyncio.create_task(task)
                except Exception as exc:
                    _handle_error("reconciler_resync_failed", exc)

        # slippage observer (si dispo)
        sh = getattr(self, "slippage_handler", None)
        if sh and hasattr(sh, "observe_slippage"):
            try:
                sh.observe_slippage(ev)
            except Exception as exc:
                _handle_error("slippage_observer_failed", exc)
        try:
            self._handle_mm_budget_event(ev)
        except Exception:
            logger.debug("[RM] mm budget release handling failed", exc_info=False)

        try:
            self._update_mm_delta_from_event(ev)
        except Exception:
            logger.debug("[RM] mm delta update failed", exc_info=False)


    # risk_manager.py — dans class RiskManager
    def _split_auto_fallback_tick(self) -> None:
        """
        Auto-fallback SPLIT -> EU_ONLY si dégradation inter-région persistante,
        puis auto-restore EU_ONLY -> SPLIT après stabilisation.
        S'appuie sur les métriques configurées (cfg.g.split_latency)
        et sur la config RM:

          - split_breach_thr_base_ms / split_breach_thr_skew_ms / split_breach_thr_stale_ms
          - split_breach_min_s (durée de dépassement avant fallback)
          - split_fallback_cooldown_s (durée minimale en EU_ONLY)
          - split_restore_stable_s (fenêtre stable pour restaurer SPLIT)
        """
        now = time.time()

        mode = str(getattr(self, "split_mode", "EU_ONLY")).upper()
        if mode not in {"SPLIT", "EU_ONLY"}:
            return

        base, skew, stale = self._split_latency_defaults(mode)

        rm_cfg = self._split_rm_cfg()
        thr_base = float(getattr(rm_cfg, "split_breach_thr_base_ms", 180.0))  # ~EU<->US ack-delta
        thr_skew = float(getattr(rm_cfg, "split_breach_thr_skew_ms", 40.0))  # skew clock inter-pods
        thr_stal = float(getattr(rm_cfg, "split_breach_thr_stale_ms", 1300.0))  # obsolescence books cross
        breach = (base >= thr_base) or (skew >= thr_skew) or (stale >= thr_stal)

        min_breach_s = float(getattr(rm_cfg, "split_breach_min_s", 3.0))
        cd_s = float(getattr(rm_cfg, "split_fallback_cooldown_s", 60.0))
        restore_s = float(getattr(rm_cfg, "split_restore_stable_s", 20.0))

        # state locals
        if not hasattr(self, "_split_breach_since"):
            self._split_breach_since = 0.0
        if not hasattr(self, "_split_fallback_until"):
            self._split_fallback_until = 0.0
        if not hasattr(self, "_split_stable_since"):
            self._split_stable_since = 0.0

        # 1) En SPLIT : si breach persiste assez longtemps -> fallback EU_ONLY
        if mode == "SPLIT":
            if breach:
                self._split_breach_since = self._split_breach_since or now
                if (now - self._split_breach_since) >= min_breach_s:
                    setattr(self, "split_mode", "EU_ONLY")
                    self._split_fallback_until = now + cd_s
                    self._split_stable_since = 0.0
                    # (optionnel) métrique/alerte
                    try:
                        self._alert("RiskManager", "SPLIT→EU_ONLY (auto-fallback inter-région)")
                    except Exception:
                        pass
            else:
                self._split_breach_since = 0.0

        # 2) En EU_ONLY : si cooldown écoulé et stabilité confirmée -> restore SPLIT
        else:  # EU_ONLY
            if breach:
                # encore dégradé -> on prolonge la stabilité à zéro
                self._split_stable_since = 0.0
            else:
                # métriques OK
                self._split_stable_since = self._split_stable_since or now
                if (now >= self._split_fallback_until) and ((now - self._split_stable_since) >= restore_s):
                    setattr(self, "split_mode", "SPLIT")
                    self._split_breach_since = 0.0
                    # (optionnel) métrique/alerte
                    try:
                        self._alert("RiskManager", "EU_ONLY→SPLIT (auto-restore inter-région)")
                    except Exception:
                        pass



    # --------------------
    # Fragmentation (alignée Engine)
    # --------------------
    def plan_fragments(
        self,
        pair_key: str,
        buy_ex: str,
        sell_ex: str,
        total_usdc: float,
        *,
        strategy: str = "TT",
        regime: Optional[str] = None,
        desired_count: Optional[int] = None,
        avg_fragment_usdc: Optional[float] = None,
        source: str = "STATIC",
    ) -> Dict[str, Any]:
        """
        Renvoie un plan de fragments front-loaded, basé sur la profondeur actuelle.
        `total_usdc` = notional **quote** (USDC/EUR).
        """
        pk = self._norm_pair(pair_key)
        total = float(max(0.0, total_usdc))
        source = "SIM"

        # Ajustements conservateurs en fonction du trade_mode consolidé.
        trade_mode = str(getattr(self, "trade_mode", "NORMAL")).upper()
        max_frags = int(self.max_fragments)
        min_frag_usdc = float(self.min_fragment_usdc)
        if trade_mode == "CONSTRAINED":
            max_frags = max(1, int(max_frags * 0.5))
            min_frag_usdc = float(min_frag_usdc * 1.5)
        elif trade_mode == "SEVERE":
            max_frags = max(1, min(2, int(max_frags)))
            min_frag_usdc = float(min_frag_usdc * 2.0)

        if total <= 0:
            return {
                "amounts": [],
                "groups": [],
                "avg_fragment_quote": 0.0,
                "auto": True,
                "source": source,
            }

        # ---- VM : prudence -> réduction douce des tailles ---------------------------
        try:
            _, size_factor, _ = self._vm_adjustments(pk)
            if size_factor < 1.0:
                total = max(float(getattr(self, "min_fragment_usdc", 200.0)), total * size_factor)
        except Exception:
            pass

        asks, bids = self.get_orderbook_depth(buy_ex, pk)
        if not asks or not bids:
            validated = fraglib.validate_fragment_plan(
                {"amounts": [total], "groups": ["G1"], "source": "FALLBACK"},
                total_quote=total,
                min_fragment_quote=min_frag_usdc,
            )
            validated["auto"] = False
            validated["source"] = "FALLBACK"
            return validated

        cnt: Optional[int] = None
        avg: Optional[float] = None
        auto = True
        try:
            plan = self.simulator.suggest_slices(
                budget_usdc=total,
                asks=asks,
                bids=bids,
                target_participation=float(self.target_ladder_participation),
                max_frags=int(max_frags),
                min_frag_usdc=min_frag_usdc,
                safety_pad=float(self.fragment_safety_pad),
            )
            cnt = int(max(1, plan.get("count", 1))) if plan else None
            avg = float(plan.get("fragment_usdc", total)) if cnt else None
            auto = bool(plan.get("auto", True)) if plan is not None else auto
            source = "SIM"
        except Exception:
            cnt = None
            avg = None
            auto = False
            source = "FALLBACK"

        weights = fraglib.normalize_frontload_weights(
            getattr(self, "frontload_weights", None), max_fragments=int(max_frags)        )
        group_size = int(getattr(self, "frontload_group_size", 3) or 3)
        plan = fraglib.build_fragment_plan(
            total_quote=total,
            desired_count=cnt,
            weights=weights,
            min_fragment_quote=min_frag_usdc,
            max_fragments=int(max_frags),
            group_size=group_size,
            source=source,
            avg_fragment_quote=avg,
        )
        validated = fraglib.validate_fragment_plan(
            plan,
            total_quote=total,
            min_fragment_quote=min_frag_usdc,
            max_fragments=int(max_frags),
        )
        if not validated.get("valid", True):

            validated = fraglib.validate_fragment_plan(
                {
                    "amounts": [total],
                    "groups": ["G1"],
                    "source": "FALLBACK",
                },
                total_quote=total,
                min_fragment_quote=min_frag_usdc,
                max_fragments=int(max_frags),
            )
        validated["auto"] = auto
        validated["source"] = validated.get("source", source)
        return validated

    # --- [ADD INSIDE class RiskManager] -----------------------------------------
    def _prebundle_guard(self, *, pair: str, route: dict, side: str, notional_quote: float) -> float:
        """
        Vérifie la fraîcheur vol et calcule le coût total (fees+slippage) via SFC.
        Retourne le coût total en fraction (ex: 0.0012 pour 12 bps).
        Raise RMError si non-ok. Compte un rejet reason-coded.
        """
        # 1) Fraîcheur volatilité (VM)
        vm = getattr(self, "vol_manager", None)
        met = vm.get_current_metrics(pair) if vm and hasattr(vm, "get_current_metrics") else None
        last_age_s = float((met or {}).get("last_age_s", float("inf")))

        if not met or not (last_age_s < float("inf")):
            inc_rm_reject(reason=RM_STALE_VOL)
            raise RMError(RM_STALE_VOL)

        # --- TTL strict volatilité : si last_age_s > vol_ttl_s => reject prebundle ---
        # Défaut: BotConfig.vol.ttl_s (fallback 5s)
        try:
            vol_ttl_s = float(getattr(getattr(self.bot_cfg, "vol", None), "ttl_s", 5.0))
        except Exception:
            vol_ttl_s = 5.0

        # Override via SLO (si dispo) : on prend le MIN des TTL publics sur buy/sell exchanges
        try:
            slo_map = getattr(self.bot_cfg, "slo", None)
            if slo_map:
                g_cfg = _cfg_g(self)
                mode_key = str(getattr(g_cfg, "deployment_mode", "SPLIT")).upper()
                per_ex = slo_map.get(mode_key) or {}

                buy_ex_u = str(route.get("buy_ex") or route.get("buy_exchange") or "").upper()
                sell_ex_u = str(route.get("sell_ex") or route.get("sell_exchange") or "").upper()

                public_slos = []
                for ex in (buy_ex_u, sell_ex_u):
                    if not ex:
                        continue
                    path_slo = per_ex.get(ex)
                    if path_slo is not None and getattr(path_slo, "public", None) is not None:
                        public_slos.append(path_slo.public)

                if public_slos:
                    vol_ttl_s = min(
                        float(getattr(ps, "vol_ttl_s", vol_ttl_s))
                        for ps in public_slos
                    )
        except Exception:
            # Best-effort : si le contrat SLO n'est pas dispo/bug, on garde BotConfig.vol.ttl_s
            pass

        if float(last_age_s) > float(vol_ttl_s):
            route_str = f"{(route.get('buy_ex') or route.get('buy_exchange') or '?')}->" \
                        f"{(route.get('sell_ex') or route.get('sell_exchange') or '?')}"
            logger.info(
                "[RiskManager] reject prebundle (RM_STALE_VOL): vol_age_s=%.3f > vol_ttl_s=%.3f pair=%s route=%s",
                float(last_age_s), float(vol_ttl_s), pair, route_str
            )
            inc_rm_reject(reason=RM_STALE_VOL)
            raise RMError(RM_STALE_VOL)


        # 2) Prudence (clé) et source de slippage
        prudence = self._current_prudence(pair)
        slip_kind = getattr(self, "slippage_source", "ewma")

        # 3) Coût total via collecteur unique (SFC)
        sfc = getattr(self, "slip_collector", None) or getattr(self, "sfc", None)

        if not sfc or not hasattr(sfc, "get_total_cost_pct"):
            inc_rm_reject(reason=RM_SFC_UNAVAILABLE)
            raise RMError(RM_SFC_UNAVAILABLE)

        route_sfc = self._norm_route_for_sfc(route, pair_key=pair)
        slip_kind = str(getattr(self.cfg, "sfc_slippage_source", getattr(self, "slippage_source", "ewma")))

        cost_frac = float(sfc.get_total_cost_pct(
            route_sfc,
            side=side,
            size_quote=float(notional_quote or 0.0),
            slippage_kind=("p95" if str(slip_kind).lower() == "p95" else "ewma"),
            prudence_key=prudence,
        ))
        # cost_frac doit être numérique et >= 0

        if not (cost_frac >= 0.0):
            inc_rm_reject(reason=RM_COST_COMPUTE_ERROR)
            raise RMError(RM_COST_COMPUTE_ERROR)

    # ------------------------------------------------------------------
    # Statut
    # ------------------------------------------------------------------
    def get_status(self) -> Dict[str, Any]:
        # Vue wiring marché privé (Hub / Reconciler / Engine)
        private_ws_status = {
            "hub_attached": getattr(self, "private_ws_hub", None) is not None,
            "hub_healthy": bool(getattr(self, "private_ws_healthy", False)),
            "hub_wiring_ok": bool(getattr(self, "private_ws_wiring_ok", False)),
            "reconciler_attached": getattr(self, "reconciler", None) is not None,
            "reconciler_wiring_ok": bool(getattr(self, "reconciler_wiring_ok", False)),
            "engine_attached": getattr(self, "engine", None) is not None,
            "critical_drop_seen": bool(getattr(self, "_pws_critical_drop_seen", False)),
            "last_critical_drop_reason": getattr(self, "_pws_critical_drop_reason", None),
        }

        rebal_status = getattr(self.rebalancing, "get_status", lambda: {})()

        return {
            "module": "RiskManager",
            "healthy": self._running,
            "rm_pipeline_ready": bool(getattr(self, "pipeline_ready_event", asyncio.Event()).is_set()),
            "rm_trading_ready": bool(getattr(self, "trading_ready_event", asyncio.Event()).is_set()),
            "rm_loops_ready": bool(getattr(self, "ready_event", asyncio.Event()).is_set()),
            "trading_state": str(getattr(self, "trading_state", "READY")),
            "trading_state_reason": getattr(self, "trading_state_reason", None),
            "readiness": dict(self._readiness),
            "last_update": self.last_update,
            "private_ws_healthy": bool(getattr(self, "private_ws_healthy", True)),
            "private_ws": private_ws_status,
            "details": "Orchestrateur central actif (multi-comptes) — quote-agnostic USDC/EUR",
            "metrics": {
                "vol_interval_s": self.t_vol,
                "books_interval_s": self.t_books,
                "balances_interval_s": self.t_bal,
                "rebal_interval_s": self.t_rebal,
                "fee_sync_interval_s": self.t_fee,
                "last_fee_sync_age_s": (time.time() - self._last_fee_sync) if self._last_fee_sync else None,
                "paused_symbols": [k for k, v in self._paused.items() if v],
                "dynamic_min_required": self.dynamic_min_required,
                "rm_mode": getattr(self, "rm_mode", "NORMAL"),
                "private_plane_state": self.private_plane_state,
                "rm_mode_since": getattr(self, "_mode_since", 0.0),
                "rm_mode_timeout_s": float(getattr(self, "_mode_timeout_s", 0.0)),
                "rm_daily_budgets": getattr(self, "daily_strategy_budget_quote", {}),
                "rm_spent_today_quote": getattr(self, "_spent_today_quote", {}),
                "rm_balance_ttl_s_normal": getattr(self, "_balance_ttl_s_normal", None),
                "rm_balance_ttl_s_degraded": getattr(self, "_balance_ttl_s_degraded", None),
                "rm_balance_ttl_s_block": getattr(self, "_balance_ttl_s_block", None),
                "rm_last_capital_drift_pct": getattr(self, "_last_capital_drift_pct", 0.0),
                "min_required_bps": self.min_required_bps,
                "min_required_bps_tt": self.min_required_bps_tt,
                "min_required_bps_tm": self.min_required_bps_tm,
                "max_vol_bps": self.max_vol_bps,
                "max_slip_bps": self.max_slip_bps,
                "min_bps_cap": self.min_bps_cap,
                # Cap s'interprète dans la devise de cotation de la paire (USDC/EUR)
                "inventory_cap_quote": self.inventory_cap_usd,
                "inventory_skew_max_pct": self.inventory_skew_max_pct,
                "inventory_rebal_exempt": self.inventory_rebal_exempt,
                "max_book_age_s": self.max_book_age_s,
                "max_clock_skew_ms": self.max_clock_skew_ms,
                "trade_modes": {"TT": self.enable_tt, "TM": self.enable_tm},
                 "rebalancing_active": self.is_rebalancing_active(),
                "rebal_active_ttl_s": self.rebal_active_ttl_s,
                "rebal_emit_cooldown_s": self.rebal_emit_cooldown_s,
                "rebal_emit_next_allowed_in_s": max(0.0, self._rebal_emit_next_allowed - time.time()),
                "rebal_caps": {
                    "inflight_rebal_current": rebal_status.get("rebal_caps", {}).get("inflight_current"),
                    "inflight_rebal_cap": rebal_status.get("rebal_caps", {}).get("inflight_cap"),
                    "rebal_ops_emitted_last_min": rebal_status.get("rebal_caps", {}).get("ops_emitted_last_min"),
                    "rebal_ops_blocked_by_caps": rebal_status.get("rebal_caps", {}).get("ops_blocked_by_caps"),
                },
                "tm_policy": {
                    "default_mode": getattr(self.cfg, "tm_default_mode", "NEUTRAL"),
                    # Clé canonique pour la hedge NEUTRAL
                    "neutral_hedge_ratio": getattr(self.cfg, "tm_exposure_ttl_hedge_ratio", 0.50),
                    # Alias éventuel pour compat legacy (peut être None si non configuré)
                    "legacy_neutral_hedge_ratio": getattr(self.cfg, "tm_neutral_hedge_ratio", None),
                    "nn_min_edge_bps": getattr(self.cfg, "tm_nn_min_edge_bps", 3.0),
                    "nn_max_vol_bps": getattr(self.cfg, "tm_nn_max_vol_bps", 60.0),
                    "nn_max_slip_bps": getattr(self.cfg, "tm_nn_max_slip_bps", 25.0),
                    "nn_min_depth_ratio": getattr(self.cfg, "tm_nn_min_depth_ratio", 1.4),
                    "nn_hedge_ratio": getattr(self.cfg, "tm_nn_hedge_ratio", 0.65),
                    "nn_max_exposure_s": getattr(self.cfg, "tm_nn_max_exposure_s", 3.0),
                },

                 "fragmentation": {
                    "min_fragment_usdc": self.min_fragment_usdc,
                    "max_fragments": self.max_fragments,
                    "fragment_sim_margin_bps": getattr(self, "fragment_sim_margin_bps", None),
                    "fragment_safety_pad": self.fragment_safety_pad,
                    "target_ladder_participation": self.target_ladder_participation,
                    "frontload_weights": list(self.frontload_weights or []),
                    "frontload_group_size": getattr(self, "frontload_group_size", 3),
                },
            },
            "submodules": {
                "VolatilityManager": getattr(self.vol_manager, "get_status", lambda: {})(),
                "SlippageAndFeesCollector": getattr(self.slip_collector, "get_status", lambda: {})(),
                "RebalancingManager": rebal_status,
            },
        }

    # ------------------------------------------------------------------
    # Utilitaire interne (estimation net bps spot sur best levels)
    # ------------------------------------------------------------------
    def _price_bundle_net_bps(self, *, buy_ex: str, sell_ex: str, pair: str) -> float:
        bid_s, ask_s = self.get_top_of_book(sell_ex, pair)
        bid_b, ask_b = self.get_top_of_book(buy_ex, pair)
        if min(bid_s, ask_s, bid_b, ask_b) <= 0:
            return -1e9
        spread = (bid_s - ask_b) / max((bid_s + ask_b) / 2.0, 1e-12)
        total_cost = self.get_total_cost_pct(buy_ex, sell_ex, pair)
        return (spread - total_cost) * 1e4

# === BEGIN LATENCY INSTRUMENTATION (RM→Engine) ===
try:
    import time, inspect
    from modules.obs_metrics import mark_rm_to_engine

    RMClass = None
    for _n, _o in list(globals().items()):  # ← snapshot pour éviter la mutation en cours d’itération
        if _n in ("RiskManager", "RiskManagerV2", "RiskManager21") and isinstance(_o, type):
            RMClass = _o
            break

    if RMClass and not getattr(RMClass, "_rm2engine_wrapped", False):
        for meth_name in ("_execute_bundle", "execute_bundle", "_dispatch_to_engine", "dispatch_to_engine",
                          "_route_to_engine", "_submit_with_pairlocks"):
            if hasattr(RMClass, meth_name):
                _orig = getattr(RMClass, meth_name)


                def _labels_from_args(_args: Tuple[Any, ...], _kwargs: Dict[str, Any]) -> Dict[str, str]:
                    try:
                        bundle = _kwargs.get("order_bundle") if "order_bundle" in _kwargs else (
                            _args[0] if _args else None)
                        meta = bundle.get("meta") if isinstance(bundle, dict) else None
                        route = bundle.get("route") if isinstance(bundle, dict) else None
                        branch = None
                        if isinstance(meta, dict):
                            branch = meta.get("branch") or meta.get("strategy")
                        if branch is None and isinstance(bundle, dict):
                            branch = bundle.get("branch") or bundle.get("strategy")
                        route_label = None
                        if isinstance(route, dict):
                            route_label = route.get("strategy") or route.get("branch") or route.get(
                                "kind") or route.get("route")
                        return {"route": route_label or "", "branch": branch or ""}
                    except Exception:
                        return {"route": "", "branch": ""}
                if inspect.iscoroutinefunction(_orig):
                    async def _wrapped(self, *args, __orig=_orig, **kwargs):
                        ts = time.perf_counter_ns()
                        ok = True
                        try:
                            return await __orig(self, *args, **kwargs)
                        except Exception:
                            ok = False
                            raise
                        finally:
                            try:
                                dt_ms = max(0.0, (time.perf_counter_ns() - ts) / 1_000_000.0)
                                mark_rm_to_engine(ok, dt_ms, **_labels_from_args(args, kwargs))
                            except Exception:
                                logging.exception("Unhandled exception")
                else:
                    def _wrapped(self, *args, __orig=_orig, **kwargs):
                        ts = time.perf_counter_ns()
                        ok = True
                        try:
                            return __orig(self, *args, **kwargs)
                        except Exception:
                            ok = False
                            raise
                        finally:
                            try:
                                dt_ms = max(0.0, (time.perf_counter_ns() - ts) / 1_000_000.0)
                                mark_rm_to_engine(ok, dt_ms, **_labels_from_args(args, kwargs))
                            except Exception:
                                logging.exception("Unhandled exception")
                setattr(RMClass, meth_name, _wrapped)
        setattr(RMClass, "_rm2engine_wrapped", True)
except Exception:
    logging.exception("Unhandled exception")
# === END LATENCY INSTRUMENTATION (RM→Engine) ===
# === RM×MBF — Mini glue (balances "real|virtual|merged" + push vers RebalancingManager) ===


log_rm_mbf = logging.getLogger("RM×MBF")

class _RM_MBFGlue:
    """
    Colle RM ↔ MBF:
      - Choix de vue: real|virtual|merged (prod=real, dry-run=merged par défaut)
      - Boucle légère qui pousse balances (+wallets si dispo) vers rebal_mgr
      - Snapshot sync pour toute la logique rebal

    Usage côté RM:
        self._mbf_glue = _RM_MBFGlue(self, self.balance_fetcher, self.rebal_mgr)
        ...
        await self._mbf_glue.start()
        ...
        await self._mbf_glue.stop()
    """

    def __init__(self, rm, mbf, rebal_mgr=None):
        self.rm = rm
        self.mbf = mbf
        self.rebal_mgr = rebal_mgr
        cfg = getattr(rm, "cfg", None)
        rm_cfg = getattr(cfg, "rm", cfg)

        # vue par défaut: prod → real ; dry-run → merged
        default_mode = "merged" if bool(getattr(cfg, "dry_run", False)) else "real"
        self.mode = str(getattr(cfg, "rm_balance_view_mode", default_mode)).lower()

        # fréquence de push vers rebal manager
        self.poll_s = float(getattr(cfg, "rm_balance_poll_s", 3.0))
        self.stop_timeout_s = float(getattr(rm_cfg, "mbf_glue_stop_timeout_s", 1.0))
        self.task = None

    # --- lectures ---
    def snapshot(self, mode: str | None = None, *, cached_only: bool = False):
        m = (mode or self.mode).lower()
        # MBF moderne avec vue optimisée fast-path
        if cached_only and hasattr(self.mbf, "get_full_balances_view"):
            return self.mbf.get_full_balances_view(mode=m)

        # MBF moderne avec overlay intégré
        if hasattr(self.mbf, "as_rm_snapshot"):
            # mappe les modes
            mm = "real" if m == "real" else ("virtual" if m == "virtual" else "merged")
            try:
                return self.mbf.as_rm_snapshot(cached_only=cached_only, mode=mm)
            except TypeError:
                # rétro-compat (anciens MBF sans param mode)
                return self.mbf.as_rm_snapshot(cached_only=cached_only)
        # fallback ultime
        if hasattr(self.mbf, "get_balances_snapshot"):
            return self.mbf.get_balances_snapshot(m)
        return {}

    async def refresh_now(self):
        try:
            await self.mbf.get_all_balances(force_refresh=True)
        except Exception:
            log_rm_mbf.debug("MBF refresh_now failed", exc_info=False)

    # --- boucle push → rebal_mgr ---
    # --- à mettre dans la classe glue RM<->MBF -----------------------------------
    import asyncio, logging
    log_rm_mbf = logging.getLogger("rm-mbf")
    def buffers_snapshot(self, *, mode: str = "real") -> Dict[str, Any]:
        """
        Proxy léger vers MultiBalanceFetcher.as_buffers_snapshot().

        Utilisé comme base unique des capacités de capital
        (pockets par quote / alias / exchange) côté RiskManager et simulateur.
        """
        mbf = self._mbf
        if mbf is None:
            return {"pockets_by_quote": {}, "fee_token_levels": {}}

        try:
            return mbf.as_buffers_snapshot(mode=mode)
        except Exception:
            logger.debug(
                "RM_MBFGlue: erreur lors de as_buffers_snapshot(mode=%s)",
                mode,
                exc_info=True,
            )
            return {"pockets_by_quote": {}, "fee_token_levels": {}}

    async def start(self) -> None:
        """
        Boucle périodique non bloquante :
        - pousse les balances snapshots vers rebal_mgr
        - récupère wallets si MBF les expose
        """
        if getattr(self, "task", None) or float(getattr(self, "poll_s", 0)) <= 0:
            return

        # Récupère rebal_mgr si absent
        if getattr(self, "rebal_mgr", None) is None:
            self.rebal_mgr = getattr(self.rm, "rebal_mgr", None)

        async def _loop():
            try:
                while True:
                    try:
                        snap = self.snapshot()  # vue choisie (real/merged)
                        if self.rebal_mgr and hasattr(self.rebal_mgr, "update_balances"):
                            self.rebal_mgr.update_balances(snap)

                        # wallets optionnels
                        if hasattr(self.mbf, "get_wallets_snapshot"):
                            try:
                                wallets = await self.mbf.get_wallets_snapshot()
                                if wallets and self.rebal_mgr and hasattr(self.rebal_mgr, "update_wallets"):
                                    self.rebal_mgr.update_wallets(wallets)
                            except Exception:
                                log_rm_mbf.exception("wallets snapshot failed")
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        log_rm_mbf.exception("bridge loop error")

                    await asyncio.sleep(float(self.poll_s))
            except asyncio.CancelledError:
                # sortie clean
                pass

        self.task = asyncio.create_task(_loop(), name="rm-mbf-bridge")

    async def stop(self) -> None:
        t = getattr(self, "task", None)
        if t:
            t.cancel()
            try:
                await asyncio.wait_for(t, timeout=float(self.stop_timeout_s or 1.0))
            except asyncio.TimeoutError:
                log_rm_mbf.warning("RM×MBF glue stop timeout (%.2fs)", float(self.stop_timeout_s or 1.0))
            except asyncio.CancelledError:
                pass
            except Exception:
                log_rm_mbf.exception("RM×MBF glue stop failed")
        self.task = None


def _mk_rebal_mgr_if_missing(rm):
    """
    Fabrique et attache un RebalancingManager si le RM n'en a pas.
    Préfère réutiliser rm.rebalancing existant. Lit les valeurs depuis rm.cfg.
    """
    # si déjà présent
    if getattr(rm, "rebal_mgr", None):
        return rm.rebal_mgr

    # si un rebalancing existe déjà, on s’aligne dessus
    if getattr(rm, "rebalancing", None) is not None:
        rm.rebal_mgr = rm.rebalancing
        return rm.rebal_mgr

    # sinon, création
    if RebalancingManager is None:
        return None


    # >>> CHANGEMENT CLÉ : on passe rm=rm <<<
    rm.rebal_mgr = RebalancingManager(
        rm=rm,

    )

    # brancher un event sink si dispo
    for name in ("alert_callback", "_emit_alert", "_event_sink"):
        sink = getattr(rm, name, None)
        if callable(sink):
            try:
                rm.rebal_mgr.set_event_sink(sink)
                break
            except Exception:
                logging.exception("Unhandled exception")

    return rm.rebal_mgr

class _DummyMetrics:
    def gauge(self, *_args, **_kwargs):
        return None

    def increment(self, *_args, **_kwargs):
        return None


class RiskManagerTTTMTests(unittest.TestCase):
    def _mk_rm(self, soft=100.0, hard=200.0) -> RiskManager:
        rm = RiskManager.__new__(RiskManager)
        rm.tttm_exposure_state = {}
        rm.tt_stuck_state = {}
        rm.tm_inflight_exposures = {}
        rm.metrics = _DummyMetrics()
        rm.logger = logging.getLogger("rm_test")
        rm.cfg = SimpleNamespace(
            rm=SimpleNamespace(
                tttm_exposure_soft_usd=soft,
                tttm_exposure_hard_usd=hard,
                tttm_exposure_by_asset={},
                tt_stuck_soft_usd=0.0,
                tt_stuck_hard_usd=0.0,
                tt_stuck_max_age_s=10.0,
            )
        )
        rm.max_clock_skew_ms = 10_000.0
        rm._fresh_enough = lambda *_args, **_kwargs: True
        rm.get_book_snapshot = lambda *_args, **_kwargs: {}
        rm._current_prudence = lambda *_args, **_kwargs: "OK"
        rm._normalize_notional_tuple = (
            lambda opp: (opp.get("quote") or "USDC", float(opp.get("notional_usdc") or 0.0))
        )
        rm._budget_allows = lambda *_args, **_kwargs: (True, "")
        rm._pair_base = RiskManager._pair_base
        return rm

    def test_tt_stuck_basic(self):
        rm = self._mk_rm(soft=100.0, hard=200.0)
        now = time.time()
        rm._register_tt_stuck_leg(
            {
                "asset": "ETH",
                "side": "BUY",
                "notional_usd": 100.0,
                "exchange": "EX1",
                "alias": "A1",
                "pair": "ETHUSDC",
                "created_ts": now,
            }
        )
        rm._register_tt_stuck_leg(
            {
                "asset": "ETH",
                "side": "BUY",
                "notional_usd": 50.0,
                "exchange": "EX1",
                "alias": "A1",
                "pair": "ETHUSDC",
                "created_ts": now,
            }
        )

        rm._refresh_tttm_exposure_state(now)
        state = rm.tttm_exposure_state.get("ETH")
        self.assertIsNotNone(state)
        self.assertAlmostEqual(state.get("delta_usd"), 150.0)
        self.assertEqual(state.get("state"), "HARD")

    def test_tttm_gating_hard(self):
        rm = self._mk_rm()
        rm.tttm_exposure_state["BTC"] = {
            "delta_usd": 300.0,
            "soft_usd": 100.0,
            "hard_usd": 200.0,
            "state": "HARD",
        }
        opp = {
            "pair": "BTCUSDC",
            "branch": "tt",
            "notional_usdc": 10.0,
            "buy_exchange": "B1",
            "sell_exchange": "B2",
        }
        admit, reason, ctx = rm._preflight_gate(opp)
        self.assertFalse(admit)
        self.assertEqual(reason, "RM_TTTM_DELTA_HARD_LIMIT")
        self.assertEqual(ctx.get("tttm_delta_state"), "HARD")

    def test_tttm_gating_soft(self):
        rm = self._mk_rm()
        rm.tttm_exposure_state["BTC"] = {
            "delta_usd": 150.0,
            "soft_usd": 100.0,
            "hard_usd": 200.0,
            "state": "SOFT",
        }
        opp = {
            "pair": "BTCUSDC",
            "branch": "tt",
            "notional_usdc": 10.0,
            "buy_exchange": "B1",
            "sell_exchange": "B2",
        }
        admit, reason, ctx = rm._preflight_gate(opp)
        self.assertTrue(admit)
        self.assertEqual(reason, "")
        self.assertEqual(ctx.get("tttm_delta_state"), "SOFT")
