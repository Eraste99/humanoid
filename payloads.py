# -*- coding: utf-8 -*-
"""
contracts.payloads
==================
Source de vérité **unique** des contrats (Prod + DRY/DEV), compatible Pydantic v2 **et** v1.

Objectifs
---------
- Un seul module pour tout (prod & dev "miroir").
- Compat Pydantic v2/v1 (auto-détection).
- Normalisation des symboles: "BTC-USDC" / "BTCUSDC" acceptés, clé compacte via pair_key.
- Helpers de conversion (Scanner→Opportunity, RM→Decision, Decision/Intent→EngineAction).
- "Lite validators" tolérants (DRY/DEV), sans masquer les erreurs en OFFICIAL.
- Métriques Prometheus optionnelles (no-op si prometheus_client absent).
- Champs additifs rétro-compatibles pour bundles: frag/caps/tm_controls/split/shadow/notional_quote.
- SimResult minimal pour RM⇄Sim.
- Conventions meta utilisées par le RM :
  - meta["flow_kind"] ∈ {"core", "opportunistic", "hedge", "rebalance", "unwind", "maintenance"}
  - meta["risk_effect"] ∈ {"risk_increasing", "risk_neutral", "risk_reducing"}

# -----------------------------------------------------------------------------
# Convention meta (utilisée par RM/Engine)
# -----------------------------------------------------------------------------
# - meta["flow_kind"]:
#     "core"         : flux de base du tri-CEX
#     "opportunistic": flux alpha "luxe"
#     "hedge"        : legs de hedge
#     "rebalance"    : rééquilibrage de comptes
#     "unwind"       : désarmement de positions
#     "maintenance"  : flux techniques
#
# - meta["risk_effect"]:
#     "risk_increasing" : augmente le risque net (par défaut)
#     "risk_neutral"    : neutre ou quasi-neutre
#     "risk_reducing"   : réduit le risque net (hedge/unwind)

# Alias Policy (canon de sortie)
# ------------------------------
# - symbol est canonique en sortie; pair est un alias d'entrée (normalisé vers symbol).
# - alias est canonique en sortie; account_alias est un alias d'entrée.
#   Si alias et account_alias sont fournis et divergent -> ValueError.
# - exchange_ts_ms est canonique en sortie; ts_ex_ms est un alias d'entrée.
#
# Error → Reason Policy
# ---------------------
# - error_kind : stable interne (lowercase), utile pour observabilité non fatale.
# - reason_code : stable décisionnel (uppercase), utilisé côté RM/Engine,
#   BundleAck.reason_code, etc. et normalisé via KNOWN_REASON_CODES.

Patchs demandés (inclus)
------------------------
1) Tolérance `side` (B/S/BUY/SELL/buy/sell → buy/sell) dans OrderIntent, SubmitLeg, EngineAction, OrderModel.
2) make_submit_bundle : paramètre `route` optionnel (None accepté).
3) Exports utilitaires `norm_symbol` et `pair_key` dans __all__.
"""

from __future__ import annotations
import enum
import hashlib
import json
import math
import time
import re
import uuid
import logging
from typing import Any, Dict, List, Optional,Tuple

# -----------------------------------------------------------------------------␊
# Prometheus (centralisé via modules.obs_metrics)
# -----------------------------------------------------------------------------␊
try:  # pragma: no cover␊
    from modules.obs_metrics import (
        CONTRACTS_HELPERS_CALLS_TOTAL,
        CONTRACTS_VALIDATION_ERRORS_TOTAL,
    )
except Exception:  # pragma: no cover
    class _Noop:
        def labels(self, *_, **__):
            return self

        def inc(self, *_, **__):
            return None

    CONTRACTS_HELPERS_CALLS_TOTAL = _Noop()
    CONTRACTS_VALIDATION_ERRORS_TOTAL = _Noop()

_METRIC_CONVERT = CONTRACTS_HELPERS_CALLS_TOTAL
_METRIC_ERRORS = CONTRACTS_VALIDATION_ERRORS_TOTAL
_LOG = logging.getLogger("contracts.payloads")

def _inc(counter, *labels):
    try:
        counter.labels(*labels).inc()
    except Exception:
        return None

# -----------------------------------------------------------------------------
# Pydantic compat v2/v1 (avec fallback no-op)
# -----------------------------------------------------------------------------
try:
    from pydantic import field_validator as _pd_field_validator, model_validator as _pd_model_validator  # v2

    _PYDANTIC_V2 = True
    try:
        from pydantic import field_validator, model_validator  # v2
    except Exception:  # pragma: no cover
        from pydantic import validator as _pd_field_validator  # type: ignore
        from pydantic import root_validator as _pd_model_validator  # type: ignore

        _PYDANTIC_V2 = False
    try:
        from pydantic import ConfigDict  # v2 only
    except Exception:
        ConfigDict = None
except Exception:
    class _BaseModel:
        def __init__(self, **data: Any):
            for k, v in data.items():
                setattr(self, k, v)

        def model_dump(self, *_, **__):  # type: ignore[override]
            return dict(self.__dict__)

    BaseModel = _BaseModel  # type: ignore

    def Field(*args, **kwargs):  # type: ignore
        return None

    class ValidationError(Exception):
        pass

    def _noop_validator(*args, **kwargs):
        def decorator(fn):
            return fn

        return decorator

    field_validator = model_validator = _noop_validator  # type: ignore
    ConfigDict = None

    _PYDANTIC_V2 = False

if "_pd_field_validator" in globals():
    def field_validator(*fields, mode="after", **kwargs):  # type: ignore
        if _PYDANTIC_V2:
            return _pd_field_validator(*fields, mode=mode, **kwargs)
        pre = kwargs.pop("pre", mode == "before")
        return _pd_field_validator(*fields, pre=pre, **kwargs)

    def model_validator(*fields, mode="after", **kwargs):  # type: ignore
        if _PYDANTIC_V2:
            return _pd_model_validator(*fields, mode=mode, **kwargs)
        pre = kwargs.pop("pre", mode == "before")
        return _pd_model_validator(*fields, pre=pre, **kwargs)

class _Cfg(BaseModel):
    """Base config tolérante (utile en DRY/DEV & smoke)."""
    if ConfigDict is not None:
        model_config = ConfigDict(extra='allow')

    else:
        class Config:
            extra = "allow"

# -----------------------------------------------------------------------------
# Enums & versioning
# -----------------------------------------------------------------------------
class Side(str, enum.Enum):
    buy = "buy"
    sell = "sell"

class Action(str, enum.Enum):
    PASS = "PASS"
    SUBMIT = "SUBMIT"
    HEDGE = "HEDGE"

class Liquidity(str, enum.Enum):
    MAKER = "MAKER"
    TAKER = "TAKER"
    UNKNOWN = "UNKNOWN"

class PortfolioRiskMetricsFields(str, enum.Enum):
    PNL = "pnl"
    PNL_PCT = "pnl_pct"
    NET_EXPOSURE = "net_exposure"
    GROSS_EXPOSURE = "gross_exposure"
    LEVERAGE = "leverage"
    VAR_95 = "var_95"
    VAR_99 = "var_99"


SCHEMA_VERSION = "1.1.0"
SCHEMA_VERSION_MAJOR = 1

# -----------------------------------------------------------------------------
# Utils & normalisation
# -----------------------------------------------------------------------------
def _now_s() -> float:
    return float(time.time())

def _uuid() -> str:
    return uuid.uuid4().hex

def _norm_symbol(sym: str, keep_dash: bool = True) -> str:
    """
    Normalise un symbole:
    - uppercase
    - remplace '/' par '-'
    - `keep_dash=False` pour une clé compacte (ex: 'BTCUSDC')
    """
    if sym is None:
        try:
            if not getattr(_LOG, "_warned_empty_symbol", False):
                setattr(_LOG, "_warned_empty_symbol", True)
                _LOG.warning("[payloads] normalize symbol received None/empty; returning ''", exc_info=False)
        except Exception:
            pass
        return ""
    s = str(sym).strip().upper().replace("/", "-")
    if not s:
        try:
            if not getattr(_LOG, "_warned_empty_symbol", False):
                setattr(_LOG, "_warned_empty_symbol", True)
                _LOG.warning("[payloads] normalize symbol empty after stripping; returning ''", exc_info=False)
        except Exception:
            pass
    return s if keep_dash else s.replace("-", "")

def _norm_pair_key(sym: str) -> str:
    """Clé de paire compacte 'BTCUSDC' (sans tirets)."""
    return _norm_symbol(sym, keep_dash=False)


def _norm_exchange(sym: str) -> str:
    return (sym or "").strip().upper()
def _asset_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(v) for v in value if v is not None]
    return [str(value)]

def _normalize_metric_key(key: Any) -> str:
    text = str(key or "").strip().lower()
    if not text:
        return ""
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")

PORTFOLIO_RISK_METRICS_ALIASES = {
    "pnl": "pnl",
    "profit_loss": "pnl",
    "pnl_pct": "pnl_pct",
    "pnl_percent": "pnl_pct",
    "net_exposure": "net_exposure",
    "gross_exposure": "gross_exposure",
    "leverage": "leverage",
    "var95": "var_95",
    "var_95": "var_95",
    "var99": "var_99",
    "var_99": "var_99",
}

PORTFOLIO_RISK_METRICS_KEYS = {
    _normalize_metric_key(v.value) for v in PortfolioRiskMetricsFields
}.union({_normalize_metric_key(k) for k in PORTFOLIO_RISK_METRICS_ALIASES})

def encode_flow_meta(flow_kind: str | None = None, risk_effect: str | None = None) -> Dict[str, str]:
    """Helper best-effort pour encoder un meta (flow_kind, risk_effect) canonique."""
    fk = (flow_kind or "core").lower()
    reff = (risk_effect or "risk_increasing").lower()
    return {"flow_kind": fk, "risk_effect": reff}

# >>> PATCH #3 — Helpers publics exposés
def norm_symbol(sym: str, keep_dash: bool = True) -> str:
    """Wrapper public vers _norm_symbol (exposé dans __all__)."""
    return _norm_symbol(sym, keep_dash=keep_dash)

def pair_key(sym: str) -> str:
    """Wrapper public vers _norm_pair_key (exposé dans __all__)."""
    return _norm_pair_key(sym)


def _normalize_symbol_and_pair(symbol: Optional[str], pair_key_value: Optional[str]) -> tuple[str, str]:
    sym_norm = _norm_symbol(symbol) if symbol else ""
    pk_norm = _norm_pair_key(pair_key_value) if pair_key_value else ""
    if sym_norm and not pk_norm:
        pk_norm = _norm_pair_key(sym_norm)

    return sym_norm, pk_norm


def _side_ok(x: Any) -> Side:
    """Mappe des variantes courantes vers Side Enum."""
    if isinstance(x, Side):
        return x
    s = str(x or "").strip().lower()
    if s in ("b", "buy", "buyer"):
        return Side.buy
    if s in ("s", "sell", "seller"):
        return Side.sell
    raise ValueError(f"Invalid side: {x!r}")

def _pos(v: Any, name: str) -> float:
    f = float(v)
    if f <= 0:
        raise ValueError(f"{name} must be > 0")
    return f

def normalize_leg_dict(leg: Dict[str, Any]) -> Dict[str, Any]:
    """Normalise un dictionnaire de leg pour le contrat SubmitLeg.

    - Mappe price ↔ px_limit pour compat.
    - - Mappe alias ↔ account_alias pour compat (alias est canonique en sortie).
    - Normalise exchange/symbol/side/qty (best-effort).
    """
    if not isinstance(leg, dict):
        return leg  # pragma: no cover - tolérance legacy

    normalized: Dict[str, Any] = dict(leg)

    # price / px_limit compat
    price = normalized.get("price", normalized.get("px"))
    px_limit = normalized.get("px_limit")
    if price is not None and px_limit is None:
        px_limit = price
    if px_limit is not None and price is None:
        price = px_limit
    normalized["price"] = float(price) if price is not None else None
    normalized["px_limit"] = float(px_limit) if px_limit is not None else None

    # alias compat
    alias = normalized.get("alias")
    account_alias = normalized.get("account_alias")
    if alias and account_alias and str(alias) != str(account_alias):
        raise ValueError("alias/account_alias mismatch in leg payload")
    if alias and not account_alias:
        account_alias = alias
    if account_alias and not alias:
        alias = account_alias
    normalized["alias"] = alias
    normalized["account_alias"] = account_alias

    # Normalisation échange / symbole / sens / qty
    if "exchange" in normalized:
        normalized["exchange"] = _norm_exchange(normalized.get("exchange"))
    symbol = normalized.get("symbol") or normalized.get("pair")
    if symbol is not None:
        normalized["symbol"] = _norm_symbol(symbol, keep_dash=True)
        normalized.pop("pair", None)
    try:
        normalized["qty"] = float(normalized["qty"]) if normalized.get("qty") is not None else None
    except Exception:
        normalized["qty"] = normalized.get("qty")
    try:
        normalized_side = _side_ok(normalized.get("side"))
        normalized["side"] = normalized_side.value
    except Exception:
        pass

    return normalized

# -----------------------------------------------------------------------------
# Contrats marché public (WS → Router → Vol/Slip/Health)
# -----------------------------------------------------------------------------
class MarketEvent(_Cfg, BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    exchange: str
    symbol: Optional[str] = None
    pair_key: Optional[str] = None
    ex_symbol: Optional[str] = None
    best_bid: float = Field(..., gt=0)
    best_ask: float = Field(..., gt=0)
    bid_volume: Optional[float] = Field(default=None, ge=0)
    ask_volume: Optional[float] = Field(default=None, ge=0)
    orderbook: Dict[str, Any] = Field(default_factory=dict)
    exchange_ts_ms: Optional[int] = Field(default=None, validation_alias="ts_ex_ms", serialization_alias="ts_ex_ms")
    recv_ts_ms: Optional[int] = None
    latency_ms: Optional[float] = None
    active: bool = True

    @field_validator("exchange", mode="before")
    def _norm_exchange_field(cls, v: Any) -> str:
        return _norm_exchange(v)

    @field_validator("symbol", mode="before")
    def _norm_symbol_field(cls, v: Any) -> Optional[str]:
        return _norm_symbol(v) if v else v

    @field_validator("pair_key", mode="before")
    def _norm_pair_field(cls, v: Any) -> Optional[str]:
        return _norm_pair_key(v) if v else v

    @field_validator("exchange_ts_ms", "recv_ts_ms", mode="before")
    def _norm_ts(cls, v: Any) -> Optional[int]:
        return int(v) if v not in (None, "") else None

    @field_validator("orderbook", mode="before")
    def _fallback_orderbook(cls, v: Any) -> Dict[str, Any]:
        return v or {}

    @model_validator(mode="after")
    def _set_symbol_and_pair(self):
        sym, pk = _normalize_symbol_and_pair(self.symbol, self.pair_key)
        object.__setattr__(self, "symbol", sym or None)
        object.__setattr__(self, "pair_key", pk or None)
        return self

    def __init__(self, **data: Any):  # type: ignore[override]
        super().__init__(**data)
        if ConfigDict is None:
            self.schema_version = getattr(self, "schema_version", None) or SCHEMA_VERSION
            self.exchange = _norm_exchange(getattr(self, "exchange", None))
            sym, pk = _normalize_symbol_and_pair(getattr(self, "symbol", None), getattr(self, "pair_key", None))
            self.symbol = sym or None
            self.pair_key = pk or None
            ts_alias = getattr(self, "ts_ex_ms", None)
            if ts_alias is not None and getattr(self, "exchange_ts_ms", None) in (None, ""):
                self.exchange_ts_ms = int(ts_alias) if ts_alias not in (None, "") else None
            if getattr(self, "orderbook", None) is None:
                self.orderbook = {}
            if getattr(self, "exchange_ts_ms", None) is not None:
                self.ts_ex_ms = int(getattr(self, "exchange_ts_ms"))  # type: ignore[attr-defined]
            try:
                self.best_bid = float(getattr(self, "best_bid", 0))
                self.best_ask = float(getattr(self, "best_ask", 0))
                if getattr(self, "bid_volume", None) is not None:
                    self.bid_volume = float(getattr(self, "bid_volume"))
                if getattr(self, "ask_volume", None) is not None:
                    self.ask_volume = float(getattr(self, "ask_volume"))
            except Exception:
                pass
            if getattr(self, "active", None) is None:
                self.active = True
            if getattr(self, "orderbook", None) is None:
                self.orderbook = {}


class VolEvent(_Cfg, BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    exchange: str
    symbol: Optional[str] = None
    pair_key: Optional[str] = None
    best_bid: Optional[float] = Field(default=None, gt=0)
    best_ask: Optional[float] = Field(default=None, gt=0)
    mid: Optional[float] = Field(default=None, gt=0)
    ema_vol_bps: Optional[float] = None
    l1_spread_bps: Optional[float] = None
    changed: bool = False
    exchange_ts_ms: Optional[int] = Field(default=None, validation_alias="ts_ex_ms", serialization_alias="ts_ex_ms")
    recv_ts_ms: Optional[int] = None
    age_ms: Optional[float] = None
    seq_no: Optional[int] = None
    active: bool = True

    @field_validator("exchange", mode="before")
    def _norm_exchange_field(cls, v: Any) -> str:
        return _norm_exchange(v)

    @field_validator("symbol", mode="before")
    def _norm_symbol_field(cls, v: Any) -> Optional[str]:
        return _norm_symbol(v) if v else v

    @field_validator("pair_key", mode="before")
    def _norm_pair_field(cls, v: Any) -> Optional[str]:
        return _norm_pair_key(v) if v else v

    @field_validator("exchange_ts_ms", "recv_ts_ms", mode="before")
    def _norm_ts(cls, v: Any) -> Optional[int]:
        return int(v) if v not in (None, "") else None

    @model_validator(mode="after")
    def _set_symbol_and_pair(self):
        sym, pk = _normalize_symbol_and_pair(self.symbol, self.pair_key)
        object.__setattr__(self, "symbol", sym or None)
        object.__setattr__(self, "pair_key", pk or None)
        return self

    def __init__(self, **data: Any):  # type: ignore[override]
        super().__init__(**data)
        if ConfigDict is None:
            self.schema_version = getattr(self, "schema_version", None) or SCHEMA_VERSION
            self.exchange = _norm_exchange(getattr(self, "exchange", None))
            sym, pk = _normalize_symbol_and_pair(getattr(self, "symbol", None), getattr(self, "pair_key", None))
            self.symbol = sym or None
            self.pair_key = pk or None
            ts_alias = getattr(self, "ts_ex_ms", None)
            if ts_alias is not None and getattr(self, "exchange_ts_ms", None) in (None, ""):
                self.exchange_ts_ms = int(ts_alias) if ts_alias not in (None, "") else None
            if getattr(self, "exchange_ts_ms", None) is not None:
                self.ts_ex_ms = int(getattr(self, "exchange_ts_ms"))  # type: ignore[attr-defined]


class SlipEvent(_Cfg, BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    exchange: str
    symbol: Optional[str] = None
    pair_key: Optional[str] = None
    slip_metric_bps: Optional[float] = None
    changed: bool = False
    notional_hint: Optional[Any] = None
    orderbook: Dict[str, Any] = Field(default_factory=dict)
    top_bid_vol: Optional[float] = Field(default=None, ge=0)
    top_ask_vol: Optional[float] = Field(default=None, ge=0)
    exchange_ts_ms: Optional[int] = Field(default=None, validation_alias="ts_ex_ms", serialization_alias="ts_ex_ms")
    recv_ts_ms: Optional[int] = None
    active: bool = True

    @field_validator("exchange", mode="before")
    def _norm_exchange_field(cls, v: Any) -> str:
        return _norm_exchange(v)

    @field_validator("symbol", mode="before")
    def _norm_symbol_field(cls, v: Any) -> Optional[str]:
        return _norm_symbol(v) if v else v

    @field_validator("pair_key", mode="before")
    def _norm_pair_field(cls, v: Any) -> Optional[str]:
        return _norm_pair_key(v) if v else v

    @field_validator("exchange_ts_ms", "recv_ts_ms", mode="before")
    def _norm_ts(cls, v: Any) -> Optional[int]:
        return int(v) if v not in (None, "") else None

    @field_validator("orderbook", mode="before")
    def _fallback_orderbook(cls, v: Any) -> Dict[str, Any]:
        return v or {}

    @model_validator(mode="after")
    def _set_symbol_and_pair(self):
        sym, pk = _normalize_symbol_and_pair(self.symbol, self.pair_key)
        object.__setattr__(self, "symbol", sym or None)
        object.__setattr__(self, "pair_key", pk or None)
        return self

    def __init__(self, **data: Any):  # type: ignore[override]
        super().__init__(**data)
        if ConfigDict is None:
            self.schema_version = getattr(self, "schema_version", None) or SCHEMA_VERSION
            self.exchange = _norm_exchange(getattr(self, "exchange", None))
            sym, pk = _normalize_symbol_and_pair(getattr(self, "symbol", None), getattr(self, "pair_key", None))
            self.symbol = sym or None
            self.pair_key = pk or None
            ts_alias = getattr(self, "ts_ex_ms", None)
            if ts_alias is not None and getattr(self, "exchange_ts_ms", None) in (None, ""):
                self.exchange_ts_ms = int(ts_alias) if ts_alias not in (None, "") else None
            if getattr(self, "orderbook", None) is None:
                self.orderbook = {}
            if getattr(self, "exchange_ts_ms", None) is not None:
                self.ts_ex_ms = int(getattr(self, "exchange_ts_ms"))  # type: ignore[attr-defined]


class HealthEvent(_Cfg, BaseModel):
    schema_version: str = Field(default=SCHEMA_VERSION)
    exchange: str
    last_ex_ts_ms: int = 0
    last_recv_ts_ms: int = 0
    age_ms: float = 0.0
    pairs_seen: List[str] = Field(default_factory=list)
    changed: bool = False
    seq: Optional[int] = None

    @field_validator("exchange", mode="before")
    def _norm_exchange_field(cls, v: Any) -> str:
        return _norm_exchange(v)

    @field_validator("last_ex_ts_ms", "last_recv_ts_ms", mode="before")
    def _norm_ts(cls, v: Any) -> int:
        return int(v or 0)

    def __init__(self, **data: Any):  # type: ignore[override]
        super().__init__(**data)
        if ConfigDict is None:
            self.schema_version = getattr(self, "schema_version", None) or SCHEMA_VERSION
            self.exchange = _norm_exchange(getattr(self, "exchange", None))
            self.last_ex_ts_ms = int(getattr(self, "last_ex_ts_ms", 0) or 0)
            self.last_recv_ts_ms = int(getattr(self, "last_recv_ts_ms", 0) or 0)


# -----------------------------------------------------------------------------
# Fragmentation helpers (front-load canonique G1/G2/G3)
# -----------------------------------------------------------------------------
FRAGMENT_GROUPS = ("G1", "G2", "G3")


def normalize_frontload_weights(weights: Optional[List[float]], max_fragments: Optional[int] = None) -> List[float]:
    """Normalise les poids front-load (liste bornée aux cohortes G1/G2/G3).

    - fallback : [0.50, 0.35, 0.15]
    - clamp chaque poids à >= 0
    - trim/pad à 3 entrées puis tronque au besoin si max_fragments < 3
    - somme ≈ 1.0
    """
    base = list(weights or []) or [0.50, 0.35, 0.15]
    # pad / trim à 3
    if len(base) < 3:
        base = (base + [0.0, 0.0, 0.0])[:3]
    else:
        base = base[:3]

    safe = []
    for w in base:
        try:
            safe.append(max(0.0, float(w)))
        except Exception:
            safe.append(0.0)
    s = sum(safe)
    if s <= 0:
        safe = [1.0, 0.0, 0.0]
        s = 1.0
    normed = [w / s for w in safe]

    if max_fragments is not None and max_fragments < len(FRAGMENT_GROUPS):
        max_groups = max(1, int(max_fragments))
        normed = normed[:max_groups]
        tail = sum(normed)
        normed = [w / tail for w in normed] if tail > 0 else normed

    return normed


def build_fragment_plan(
    total_quote: float,
    desired_count: Optional[int],
    weights: Optional[List[float]],
    min_fragment_quote: float,
    max_fragments: Optional[int],
    group_size: int,
    *,
    source: str,
    avg_fragment_quote: Optional[float] = None,
    branch: str = "TT",
) -> Dict[str, Any]:
    """Construit un plan de fragmentation canonique (amounts + groups)."""
    total = max(0.0, float(total_quote))
    min_frag = float(min_fragment_quote or 0.0)
    
    if total <= 0:
        return {
            "version": 1,
            "source": source,
            "branch": branch,
            "total_quote": total,
            "min_fragment_quote": min_frag,
            "max_fragments": max_fragments or 1,
            "amounts": [],
            "groups": [],
            "group_size": group_size,
            "valid": False,
        }

    group_size = max(1, int(group_size))
    if desired_count and desired_count > 0:
        count = int(desired_count)
    elif avg_fragment_quote and avg_fragment_quote > 0:
        count = max(1, int(round(total / float(avg_fragment_quote))))
    else:
        count = max(1, int(round(total / max(min_frag, 1.0))))

    if max_fragments:
        try:
            count = min(count, int(max_fragments))
        except Exception:
            pass
    count = max(1, min(64, count))

    g1 = min(group_size, count)
    rem_after_g1 = count - g1
    g2 = min(group_size, rem_after_g1) if rem_after_g1 > 0 else 0
    rem_after_g2 = rem_after_g1 - g2
    g3 = max(0, rem_after_g2)

    norm_weights = normalize_frontload_weights(weights, max_fragments=count)
    cohorts = []
    if g1 > 0:
        cohorts.append(("G1", g1, norm_weights[0]))
    if g2 > 0 and len(norm_weights) > 1:
        cohorts.append(("G2", g2, norm_weights[1]))
    if g3 > 0 and len(norm_weights) > 2:
        cohorts.append(("G3", g3, norm_weights[2]))
    s = sum(w for _, _, w in cohorts) or 1.0
    cohorts = [(label, size, w / s) for (label, size, w) in cohorts]

    amounts: List[float] = []
    groups: List[str] = []
    for label, size, weight in cohorts:
        budget = total * weight
        if label == cohorts[-1][0]:
            budget = max(0.0, total - sum(amounts))
        if size <= 0 or budget <= 0:
            continue
        per_slice = budget / size
        if per_slice < min_frag and size > 1:
            size = max(1, int(min(size, math.ceil(budget / max(min_frag, 1.0)))))
            per_slice = budget / size
        for _i in range(size):
            amounts.append(per_slice)
            groups.append(label)

    diff = total - sum(amounts)
    if amounts and abs(diff) > 1e-6:
        amounts[-1] = max(0.0, amounts[-1] + diff)

    if len(amounts) >= 2 and amounts[-1] < (min_frag * 0.5):
        merged = amounts[-1] + amounts[-2]
        amounts[-2] = merged
        amounts.pop()
        if groups:
            groups.pop()

    return {
        "version": 1,
        "source": source,
        "branch": branch,
        "total_quote": total,
        "min_fragment_quote": min_frag,
        "max_fragments": max_fragments or 64,
        "amounts": [round(a, 6) for a in amounts],
        "groups": groups,
        "group_size": group_size,
        "stop_rules": {
            "time_budget_ms": 5000,
            "min_edge_net_bps": -100,
            "abort_on_health": True
        },
        "valid": True,
    }


def validate_fragment_plan(
    plan: Dict[str, Any],
    total_quote: float,
    min_fragment_quote: float,
    max_fragments: Optional[int] = None,
    *,
    tol: float = 1e-3,
) -> Dict[str, Any]:
    """Valide/normalise un plan de fragments (fallback mono-fragment G1)."""
    if not plan or not isinstance(plan, dict):
        return build_fragment_plan(total_quote, 1, None, min_fragment_quote, max_fragments, 3, source="VAL_MISSING")

    amounts = plan.get("amounts") or []
    groups = plan.get("groups") or []
    
    if not amounts or len(amounts) != len(groups):
        return build_fragment_plan(total_quote, 1, None, min_fragment_quote, max_fragments, 3, source="VAL_INCONSISTENT")

    total_plan = sum(amounts)
    if abs(total_plan - total_quote) > (total_quote * tol + 1e-3):
        return build_fragment_plan(total_quote, 1, None, min_fragment_quote, max_fragments, 3, source="VAL_TOTAL_MISMATCH")

    if max_fragments and len(amounts) > max_fragments:
        return build_fragment_plan(total_quote, max_fragments, None, min_fragment_quote, max_fragments, 3, source="VAL_MAX_FRAG")

    if any(a < (min_fragment_quote * 0.4) for a in amounts):
        return build_fragment_plan(total_quote, 1, None, min_fragment_quote, max_fragments, 3, source="VAL_MIN_FRAG")

    return plan


# -----------------------------------------------------------------------------
# Sous-modèles typés (riches)
# -----------------------------------------------------------------------------
class Frag(_Cfg, BaseModel):
    """Slicing/Fragmentation (TT + TM) — spécification unique."""
    version: int = 1
    source: str = "UNKNOWN"
    branch: str = "TT" # TT|TM
    total_quote: float = 0.0
    min_fragment_quote: float = 0.0
    max_fragments: int = 1
    amounts: List[float] = Field(default_factory=list)
    groups: List[str] = Field(default_factory=list)
    group_size: int = 3
    stop_rules: Dict[str, Any] = Field(default_factory=lambda: {
        "time_budget_ms": 5000,
        "min_edge_net_bps": -100,
        "abort_on_health": True
    })
    
    # Legacy / compat
    group: Optional[str] = None
    idx: Optional[int] = None
    total: Optional[int] = None
    weight: Optional[float] = None
    planned_notional_quote: Optional[float] = None
    plan: Optional[Dict[str, Any]] = None


class Caps(_Cfg, BaseModel):
    """Caps d'exécution / concurrence / headroom."""
    inflight_cap: Optional[int] = Field(default=None, ge=0)
    bundle_concurrency: Optional[int] = Field(default=None, ge=1)
    headroom_min: Optional[int] = Field(default=None, ge=0)

class TMControls(_Cfg, BaseModel):
    """Contrôles TM (Queue-pos, TTL, Hedge ratio)."""
    queuepos_max_usd: Optional[float] = Field(default=None, ge=0)
    ttl_ms: Optional[int] = Field(default=None, ge=0)
    hedge_ratio: Optional[float] = Field(default=None, ge=0)

class SplitControls(_Cfg, BaseModel):
    """Paramètres spécifiques au mode SPLIT."""
    mode: Optional[str] = None  # "EU_ONLY" / "SPLIT" / ...
    skew_ms: Optional[int] = Field(default=None, ge=0)
    base_delta_ms: Optional[int] = Field(default=None, ge=0)

class NotionalQuote(_Cfg, BaseModel):
    """Notional en devise de cotation (ex. USDC/EUR)."""
    ccy: str = "USDC"
    amount: float = Field(default=0.0, ge=0)

class Shadow(_Cfg, BaseModel):
    """Shadow/sampling booléen."""
    enabled: bool = False

# -----------------------------------------------------------------------------
# Modèles principaux
# -----------------------------------------------------------------------------
class Opportunity(_Cfg, BaseModel):
    symbol: str
    best_px_buy: Optional[float] = Field(default=None, gt=0)
    best_px_sell: Optional[float] = Field(default=None, gt=0)
    ts: float = Field(default_factory=_now_s)
    meta: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _alias_pair(cls, values: Dict[str, Any]):
        if isinstance(values, dict) and "symbol" not in values and "pair" in values:
            values = dict(values)
            values["symbol"] = values.get("pair")
        return values

    @field_validator("symbol")
    @classmethod
    def _v_symbol(cls, v: str) -> str:
        return _norm_symbol(v, keep_dash=True)


class RiskDecision(_Cfg, BaseModel):
    symbol: str
    action: Action
    side: Optional[Side] = None
    px: Optional[float] = Field(default=None, gt=0)
    qty: Optional[float] = Field(default=None, gt=0)
    route_id: Optional[str] = None
    opp_id: Optional[str] = None
    reason: Optional[str] = None
    meta: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def _v_symbol(cls, v: str) -> str:
        return _norm_symbol(v, keep_dash=True)

    @field_validator("side", mode="before")
    @classmethod
    def _v_side_anycase(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            s = v.strip().upper()
            if s in ("B", "BUY", "BUYER"):
                return "buy"
            if s in ("S", "SELL", "SELLER"):
                return "sell"
        return v

class EngineAction(_Cfg, BaseModel):
    symbol: str
    side: Side
    px: float = Field(..., gt=0)
    qty: float = Field(..., gt=0)
    exchange: Optional[str] = None
    opp_id: Optional[str] = None
    route_id: Optional[str] = None
    meta: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def _v_symbol(cls, v: str) -> str:
        return _norm_symbol(v, keep_dash=True)

    # >>> PATCH #1 — tolérance side (B/S/BUY/SELL)
    @field_validator("side", mode="before")
    @classmethod
    def _v_side_anycase(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            s = v.strip().upper()
            if s in ("B", "BUY", "BUYER"): return "buy"
            if s in ("S", "SELL", "SELLER"): return "sell"
        return v

class SubmitLeg(_Cfg, BaseModel):
    exchange: str
    alias: Optional[str] = None
    side: Side
    symbol: str
    price: Optional[float] = Field(default=None, gt=0)
    qty: Optional[float] = Field(default=None, gt=0)
    tif: Optional[str] = None
    meta: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("exchange", mode="before")
    @classmethod
    def _v_ex(cls, v: Any) -> str:
        return str(v or "").upper()

    @field_validator("symbol")
    @classmethod
    def _v_symbol(cls, v: str) -> str:
        return _norm_symbol(v, keep_dash=True)

    # >>> PATCH #1 — tolérance side
    @field_validator("side", mode="before")
    @classmethod
    def _v_side_anycase(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            s = v.strip().upper()
            if s in ("B", "BUY", "BUYER"): return "buy"
            if s in ("S", "SELL", "SELLER"): return "sell"
        return v

class OrderIntent(_Cfg, BaseModel):
    symbol: str
    side: Side
    qty: Optional[float] = Field(default=None, gt=0)
    price: Optional[float] = Field(default=None, gt=0)
    volume_quote: Optional[float] = Field(default=None, gt=0)
    exchange: Optional[str] = None
    route_id: Optional[str] = None
    opp_id: Optional[str] = None
    tif: Optional[str] = None
    meta: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def _v_symbol(cls, v: str) -> str:
        return _norm_symbol(v, keep_dash=True)

    # >>> PATCH #1 — tolérance side
    @field_validator("side", mode="before")
    @classmethod
    def _v_side_anycase(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            s = v.strip().upper()
            if s in ("B", "BUY", "BUYER"): return "buy"
            if s in ("S", "SELL", "SELLER"): return "sell"
        return v

class SubmitBundleRequest(_Cfg, BaseModel):
    """
    Requête d'envoi d'un bundle exécutable (Engine).
    - `legs`: liste de SubmitLeg (ou dictionnaires équivalents).
    - `notional_quote`: NotionalQuote (ccy/amount).
    - Champs additifs rétro-compatibles: frag/caps/tm_controls/split/shadow.
    """
    route_id: Optional[str] = None
    mode: str
    tif: str
    legs: List[SubmitLeg]
    notional_quote: NotionalQuote
    branch: Optional[str] = None
    profile: Optional[str] = None
    frag: Optional[Frag] = None
    caps: Optional[Caps] = None
    tm_controls: Optional[TMControls] = None
    split: Optional[SplitControls] = None
    shadow: Optional[bool] = False
    client_id: Optional[str] = None
    trace_id: Optional[str] = None
    decision_id: Optional[str] = None
    bundle_id: Optional[str] = None
    idempotency_key: Optional[str] = None
    ts_ns: Optional[int] = None

SubmitBundle = SubmitBundleRequest  # alias public

class CancelRequest(_Cfg, BaseModel):
    order_id: str
    exchange: Optional[str] = None
    route_id: Optional[str] = None
    meta: Dict[str, Any] = Field(default_factory=dict)
    idempotency_key: Optional[str] = None


class BundleAck(_Cfg, BaseModel):
    trace_id: Optional[str] = None
    decision_id: Optional[str] = None
    bundle_id: Optional[str] = None
    idempotency_key: Optional[str] = None
    state: str
    reason_code: Optional[str] = None
    per_order: Dict[str, Any] = Field(default_factory=dict)
    deadlines: Dict[str, Any] = Field(default_factory=dict)
class OrderModel(_Cfg, BaseModel):
    order_id: str
    side: Side
    px: float = Field(..., gt=0)
    qty: float = Field(..., gt=0)

    # >>> PATCH #1 — tolérance side
    @field_validator("side", mode="before")
    @classmethod
    def _v_side_anycase(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            s = v.strip().upper()
            if s in ("B", "BUY", "BUYER"): return "buy"
            if s in ("S", "SELL", "SELLER"): return "sell"
        return v

class FillModel(_Cfg, BaseModel):
    order_id: str
    qty: float = Field(..., gt=0)
    px: float = Field(..., gt=0)
    partial: bool

class FillNormalized(_Cfg, BaseModel):
    order_id: str
    exchange: Optional[str] = None
    alias: Optional[str] = None
    qty: float = Field(..., ge=0)
    px: float = Field(..., ge=0)
    fee_pct: Optional[float] = Field(default=None, ge=0)
    status: str = Field(default="FILL")  # FILL|PARTIAL

class PrivateFillEvent(_Cfg, BaseModel):
    type: str
    status: str
    exchange: str
    alias: str
    symbol: str
    side: str
    fill_px: float
    base_qty: float
    quote: str
    quote_qty: float
    client_id: Optional[str] = None
    exchange_order_id: Optional[str] = None

    @model_validator(mode="after")
    def _ensure_ids(cls, data):  # type: ignore[override]
        has_id = bool(data.client_id or data.exchange_order_id)
        if not has_id:
            raise ValueError("Missing client_id/exchange_order_id")
        return data


class WSAliasStatusSnapshot(_Cfg, BaseModel):
    exchange: str
    alias: str
    status: str = Field(default="WS_UNKNOWN")
    healthy: Optional[bool] = None
    heartbeat_gap_s: Optional[float] = None
    last_event_ts: Optional[float] = None
    errors_total: Optional[int] = Field(default=0, ge=0)
    reconnects_total: Optional[int] = Field(default=0, ge=0)
    clients: Dict[str, Any] = Field(default_factory=dict)


class RecoAliasStatusSnapshot(_Cfg, BaseModel):
    exchange: str
    alias: str
    status: str = Field(default="UNKNOWN")
    misses_recent: Optional[int] = Field(default=0, ge=0)
    miss_rate_per_min: Optional[float] = Field(default=0.0, ge=0)
    last_alias_resync_ts: Optional[float] = None
    age_since_last_alias_resync_s: Optional[float] = None
    wiring_ok: Optional[bool] = None
    wiring: Dict[str, Any] = Field(default_factory=dict)


class BalanceSnapshotRM(_Cfg, BaseModel):
    mode: str
    balances: Dict[str, Any]
    meta: Dict[str, Any]

class CMExposure(_Cfg, BaseModel):
    asset: List[str] = Field(default_factory=list)
    exposure: Optional[float] = None
    meta: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("asset", mode="before")
    @classmethod
    def _v_asset_list(cls, v: Any) -> List[str]:
        return _asset_list(v)

    def __init__(self, **data: Any):  # type: ignore[override]
        super().__init__(**data)
        if ConfigDict is None:
            self.asset = _asset_list(getattr(self, "asset", None))

class CMSnapshots(_Cfg, BaseModel):
    asset: List[str] = Field(default_factory=list)
    snapshots: List[CMExposure] = Field(default_factory=list)
    ts: float = Field(default_factory=_now_s)
    meta: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("asset", mode="before")
    @classmethod
    def _v_asset_list(cls, v: Any) -> List[str]:
        return _asset_list(v)

    def __init__(self, **data: Any):  # type: ignore[override]
        super().__init__(**data)
        if ConfigDict is None:
            self.asset = _asset_list(getattr(self, "asset", None))

class PortfolioSnapshot(_Cfg, BaseModel):
    risk_metrics: Dict[str, Any] = Field(default_factory=dict)
    exposure_dict: Dict[str, Any] = Field(default_factory=dict)
    ts: float = Field(default_factory=_now_s)
    meta: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _validate_risk_metrics(cls, values: Dict[str, Any]):
        if not isinstance(values, dict):
            return values
        risk_metrics = values.get("risk_metrics") or {}
        if not isinstance(risk_metrics, dict):
            return values
        unknown = []
        for key in risk_metrics.keys():
            if _normalize_metric_key(key) not in PORTFOLIO_RISK_METRICS_KEYS:
                unknown.append(key)
        if unknown:
            joined = ", ".join(str(k) for k in unknown)
            raise ValueError(f"Unknown risk_metrics key(s): {joined}")
        return values

class FeesSnapshot(_Cfg, BaseModel):
    exchange: str
    symbol: Optional[str] = None
    maker_pct: Optional[float] = Field(default=None, ge=0)
    taker_pct: Optional[float] = Field(default=None, ge=0)
    ts: float = Field(default_factory=_now_s)

class SlippageSnapshot(_Cfg, BaseModel):
    exchange: str
    symbol: str
    side: Optional[Side] = None
    slip_bps: Optional[float] = Field(default=None, ge=0)
    buy_bps: Optional[float] = Field(default=None, ge=0)
    sell_bps: Optional[float] = Field(default=None, ge=0)
    notional_quote: Optional[Dict[str, Any]] = None
    ts: float = Field(default_factory=_now_s)

    @field_validator("exchange", mode="before")
    def _norm_exchange_field(cls, v: Any) -> str:
        return _norm_exchange(v)

    @field_validator("symbol", mode="before")
    def _norm_symbol_field(cls, v: Any) -> str:
        return _norm_symbol(v)

class DiscoveryResult(_Cfg, BaseModel):
    """Synthèse d'une exécution de discovery (audit + métriques)."""

    schema_version: str = Field(default=SCHEMA_VERSION)
    stage_counts: Dict[str, int] = Field(default_factory=dict)
    filtered_counts: Dict[str, int] = Field(default_factory=dict)
    api_errors: Dict[str, int] = Field(default_factory=dict)
    run_ms: Optional[float] = Field(default=None, ge=0)
    top_pairs: List[str] = Field(default_factory=list)
    fx_applied: Optional[bool] = None
    ranking_mode: Optional[str] = None

    def __init__(self, **data: Any):  # type: ignore[override]
        super().__init__(**data)
        try:
            # Si pydantic n'est pas présent, les validators ne s'exécutent pas.
            # On applique la normalisation manuellement pour garder un contrat propre.
            self._clean_nulls(self)
        except Exception:
            pass

    @model_validator(mode="after")
    def _clean_nulls(cls, data):  # type: ignore[override]
        """Normalisation compatible pydantic v1 (root_validator) et v2 (model_validator)."""

        def _normalize_counts(raw):
            out = {}
            for k, v in (raw or {}).items():
                try:
                    out[str(k)] = max(0, int(v))
                except Exception:
                    continue
            return out

        def _normalize_filtered(raw):
            out = {}
            for k, v in (raw or {}).items():
                try:
                    val = max(0, int(v))
                except Exception:
                    continue
                if val:
                    out[str(k)] = val
            return out

        def _normalize_errors(raw):
            out = {}
            for k, v in (raw or {}).items():
                try:
                    out[_norm_exchange(k)] = max(0, int(v))
                except Exception:
                    continue
            return out

        def _normalize_pairs(raw):
            out = []
            for p in raw or []:
                if not p:
                    continue
                out.append(_norm_symbol(p))
            return out

        # pydantic v2 → data est l'instance; v1 → dict values
        if isinstance(data, DiscoveryResult):
            obj = data
            object.__setattr__(obj, "stage_counts", _normalize_counts(obj.stage_counts))
            object.__setattr__(obj, "filtered_counts", _normalize_filtered(obj.filtered_counts))
            object.__setattr__(obj, "api_errors", _normalize_errors(obj.api_errors))
            object.__setattr__(obj, "top_pairs", _normalize_pairs(obj.top_pairs))
            return obj

        values = dict(data or {})
        values["stage_counts"] = _normalize_counts(values.get("stage_counts"))
        values["filtered_counts"] = _normalize_filtered(values.get("filtered_counts"))
        values["api_errors"] = _normalize_errors(values.get("api_errors"))
        values["top_pairs"] = _normalize_pairs(values.get("top_pairs"))
        return values

class VolatilitySnapshot(_Cfg, BaseModel):
    exchange: str
    symbol: str
    vol_ema_bps: Optional[float] = None
    vol_p95_bps: Optional[float] = None
    vm_band: Optional[str] = None
    ts: float = Field(default_factory=_now_s)

    @field_validator("exchange", mode="before")
    def _norm_exchange_field(cls, v: Any) -> str:
        return _norm_exchange(v)

    @field_validator("symbol", mode="before")
    def _norm_symbol_field(cls, v: Any) -> str:
        return _norm_symbol(v)


class SimResult(_Cfg, BaseModel):
    ok: bool
    reason: Optional[str] = None
    sim_vwap_dev_bps: Optional[float] = None
    fills_expected_ratio: Optional[float] = None
    sim_latency_ms: Optional[float] = None
    guards: Dict[str, Any] = Field(default_factory=dict)

# -----------------------------------------------------------------------------
# Helpers de conversion
# -----------------------------------------------------------------------------
def opportunity_from_scanner(payload: Dict[str, Any]) -> Opportunity:
    _inc(_METRIC_CONVERT, "opportunity_from_scanner")
    try:
        return Opportunity(**payload)
    except ValidationError:
        _inc(_METRIC_ERRORS, "Opportunity")
        raise

def decision_submit_from_rm(payload: Dict[str, Any]) -> RiskDecision:
    _inc(_METRIC_CONVERT, "decision_submit_from_rm")
    try:
        return RiskDecision(**payload)
    except ValidationError:
        _inc(_METRIC_ERRORS, "RiskDecision")
        raise

def engine_action_from_decision(dec: RiskDecision, *,
                                symbol: Optional[str] = None,
                                exchange: Optional[str] = None) -> EngineAction:
    _inc(_METRIC_CONVERT, "engine_action_from_decision")
    if dec.action not in (Action.SUBMIT, Action.HEDGE):
        raise ValueError("EngineAction needs SUBMIT or HEDGE")
    if dec.side is None or dec.px is None or dec.qty is None:
        raise ValueError("Missing side/px/qty in RiskDecision")
    side = _side_ok(dec.side)
    return EngineAction(
        symbol=_norm_symbol(symbol or dec.symbol or ""),
        side=side,
        px=float(dec.px),
        qty=float(dec.qty),
        exchange=exchange,
        opp_id=dec.opp_id,
        route_id=dec.route_id,
        meta=dec.meta or {},
    )

def submit_leg_from_intent(intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convertit un intent (dict) en leg submit Engine (dict).
    Tolère: ex/exchange, pair/symbol, px/price, qty, side (mapping B/S/BUY/SELL).
    """
    _inc(_METRIC_CONVERT, "submit_leg_from_intent")
    ex = intent.get("exchange") or intent.get("ex")
    symbol = intent.get("symbol") or intent.get("pair")
    side = _side_ok(intent.get("side"))
    qty = intent.get("qty")
    price = intent.get("price", intent.get("px"))
    tif = intent.get("tif")

    return {
        "exchange": (str(ex or "")).upper(),
        "alias": intent.get("alias"),
        "side": side.value,
        "symbol": _norm_symbol(symbol, keep_dash=True),
        "price": float(price) if price is not None else None,
        "qty": float(qty) if qty is not None else None,
        "tif": tif,
        "meta": intent.get("meta") or {},
    }

def engine_action_from_intent(intent: Dict[str, Any]) -> EngineAction:
    _inc(_METRIC_CONVERT, "engine_action_from_intent")
    side = _side_ok(intent.get("side"))
    px = float(intent.get("price", intent.get("px")))
    qty = float(intent.get("qty"))
    sym = intent.get("symbol") or intent.get("pair")
    if not sym:
        raise ValueError("Missing symbol/pair in intent")
    return EngineAction(
        symbol=_norm_symbol(sym, keep_dash=True),
        side=side, px=px, qty=qty,
        exchange=intent.get("exchange"),
        opp_id=intent.get("opp_id"),
        route_id=intent.get("route_id"),
        meta=intent.get("meta") or {},
    )

# -----------------------------------------------------------------------------
# Validators "lite" (DRY/DEV)
# -----------------------------------------------------------------------------
def _validate_lite(model, payload, model_name: str):
    try:
        return model(**payload)
    except ValidationError:
        _inc(_METRIC_ERRORS, model_name)
        raise

def try_validate_lite(model, payload, model_name: str):
    try:
        return model(**payload), None
    except ValidationError as exc:
        _inc(_METRIC_ERRORS, model_name)
        return None, exc
    except Exception as exc:  # pragma: no cover
        return None, exc

def validate_payload_lite(payload: Dict[str, Any]) -> Opportunity:
    return _validate_lite(Opportunity, payload, "Opportunity")

def validate_opportunity_lite(payload: Dict[str, Any]) -> Opportunity:
    return _validate_lite(Opportunity, payload, "Opportunity")

def validate_decision_lite(payload: Dict[str, Any]) -> RiskDecision:
    return _validate_lite(RiskDecision, payload, "RiskDecision")

def validate_engine_action_lite(payload: Dict[str, Any]) -> EngineAction:
    return _validate_lite(EngineAction, payload, "EngineAction")

def validate_order_lite(payload: Dict[str, Any]) -> OrderModel:
    return _validate_lite(OrderModel, payload, "OrderModel")

def validate_fill_lite(payload: Dict[str, Any]) -> FillModel:
    return _validate_lite(FillModel, payload, "FillModel")

def normalize_private_fill_event(ev: dict) -> dict:
    if not isinstance(ev, dict):
      return {}

    out = dict(ev)
    if out.get("client_id") in (None, ""):
        for k in ("clientOrderId", "client_order_id"):
            v = out.get(k)
            if v not in (None, ""):
                out["client_id"] = v
                break

    if out.get("exchange_order_id") in (None, ""):
        for k in ("orderId", "order_id"):
            v = out.get(k)
            if v not in (None, ""):
                out["exchange_order_id"] = v
                break

    if "exchange" in out:
        out["exchange"] = _norm_exchange(out.get("exchange"))
    if "alias" in out:
        out["alias"] = _norm_exchange(out.get("alias"))
    if "symbol" in out:
        out["symbol"] = _norm_symbol(out.get("symbol"))
    return out


def validate_private_fill_event_lite(ev: dict) -> tuple[bool, Optional[PrivateFillEvent]]:
    has_id = isinstance(ev, dict) and bool(ev.get("client_id") or ev.get("exchange_order_id"))
    if not has_id:
        return False, None
    try:
        return True, _validate_lite(PrivateFillEvent, ev, "PrivateFillEvent")
    except Exception:
        return False, None


def validate_ws_alias_status_snapshot_lite(payload: Dict[str, Any]) -> WSAliasStatusSnapshot:
    return _validate_lite(WSAliasStatusSnapshot, payload, "WSAliasStatusSnapshot")


def validate_reco_alias_status_snapshot_lite(payload: Dict[str, Any]) -> RecoAliasStatusSnapshot:
    return _validate_lite(RecoAliasStatusSnapshot, payload, "RecoAliasStatusSnapshot")


def validate_balance_snapshot_rm_lite(payload: Dict[str, Any]) -> BalanceSnapshotRM:
    return _validate_lite(BalanceSnapshotRM, payload, "BalanceSnapshotRM")

def validate_idempotency_key_lite(payload: Dict[str, Any], *, prod: bool = False) -> Optional[str]:
    if not prod:
        return None
    if not isinstance(payload, dict):
        return normalize_reason_code("RPC_MISSING_IDEMPOTENCY_KEY") or "RPC_MISSING_IDEMPOTENCY_KEY"
    if payload.get("idempotency_key") or payload.get("idempotence_key"):
        return None
    return normalize_reason_code("RPC_MISSING_IDEMPOTENCY_KEY") or "RPC_MISSING_IDEMPOTENCY_KEY"

def _mark_idempotency_missing(payload: Dict[str, Any], *, prod: bool) -> Dict[str, Any]:
    reason_code = validate_idempotency_key_lite(payload, prod=prod)
    if not reason_code:
        return payload
    obj = dict(payload or {})
    obj.setdefault("_schema_invalid", True)
    obj.setdefault("_schema_reason", reason_code)
    obj.setdefault("reason_code", reason_code)
    return obj

def validate_submit_bundle_lite(payload: Dict[str, Any], *, prod: bool = False) -> SubmitBundleRequest:
    checked = _mark_idempotency_missing(payload, prod=prod)
    return _validate_lite(SubmitBundleRequest, checked, "SubmitBundleRequest")

def validate_cancel_lite(payload: Dict[str, Any], *, prod: bool = False) -> CancelRequest:
    checked = _mark_idempotency_missing(payload, prod=prod)
    return _validate_lite(CancelRequest, checked, "CancelRequest")

def validate_fill_normalized_lite(payload: Dict[str, Any]) -> FillNormalized:
    return _validate_lite(FillNormalized, payload, "FillNormalized")

def validate_order_intent_lite(payload: Dict[str, Any]) -> OrderIntent:
    return _validate_lite(OrderIntent, payload, "OrderIntent")

def validate_bundle_ack_lite(payload: Dict[str, Any]) -> BundleAck:
    return _validate_lite(BundleAck, payload, "BundleAck")

# -----------------------------------------------------------------------------
# make_submit_bundle — >>> PATCH #2 route optionnel
# -----------------------------------------------------------------------------
def make_submit_bundle(*,
                       legs: List[Dict[str, Any]],
                       route: Optional[Dict[str, Any]] = None,
                       mode: str,
                       tif: str,
                       notional_quote: Dict[str, Any],
                       branch: Optional[str] = None,
                       profile: Optional[str] = None,
                       frag: Optional[Dict[str, Any]] = None,
                       caps: Optional[Dict[str, Any]] = None,
                       tm_controls: Optional[Dict[str, Any]] = None,
                       split: Optional[Dict[str, Any]] = None,
                       shadow: Optional[bool] = False,
                       client_id: Optional[str] = None,
                       trace_id: Optional[str] = None,
                       decision_id: Optional[str] = None,
                       bundle_id: Optional[str] = None,
                       idempotency_key: Optional[str] = None,
                       ts_ns: Optional[int] = None) -> Dict[str, Any]:
    """
    Construit un bundle exécutable standard (dict).
    - `legs` : liste de legs dict (peuvent contenir "order": {...} ou être plats).
    - Rétro-compat: ajoute toujours "orders" miroir.
    """
    legs_normalized: List[Dict[str, Any]] = []
    orders: List[Dict[str, Any]] = []
    for l in legs:
        if isinstance(l, dict):
            base_leg = l.get("order", l)
            norm_leg = normalize_leg_dict(base_leg)
            legs_normalized.append(norm_leg)
            orders.append(norm_leg)
        else:
            legs_normalized.append(l)
            orders.append(l)
    r = route or {}
    bundle_id_final = bundle_id or str(uuid.uuid4())
    return {
        "bundle_id": bundle_id_final,
        "client_id": client_id or "default",
        "mode": mode,                    # "TT" | "TM" | "MM" | "REB-as-TM"
        "tif": tif,
        "trace_id": trace_id,
        "decision_id": decision_id,
        "idempotency_key": idempotency_key,
        "ts_ns": ts_ns,
        "route": {                       # champ existant, clés tolérantes
            "buy_ex":  r.get("buy_ex"),
            "sell_ex": r.get("sell_ex"),
            "pair":    r.get("pair"),
        },
        "legs": legs_normalized,
        "orders": orders,                # rétro-compat
        "notional_quote": {
            "ccy":    (notional_quote.get("ccy") or "USDC"),
            "amount": float(notional_quote.get("amount") or 0.0),
        },
        "ts": time.time(),
        "guards": {},

        # additifs (compat)
        "branch":   branch or mode,
        "profile":  profile,
        "frag":     frag,
        "caps":     caps,
        "tm_controls": tm_controls,
        "split":    split,
        "shadow":   bool(shadow),

        # meta informative centrale
        "meta": {
            "pair": (orders[0] or {}).get("symbol"),
            "symbol": (orders[0] or {}).get("symbol"),
            "side": (orders[0] or {}).get("side"),
            "tif": tif,
            "client_id": client_id,
            "branch": (branch or mode),
            "capital_profile": profile,
            "profile": profile,
            "route": r,
            "notional_quote": notional_quote,
            "frag": frag,
            "caps":   caps,
            "tm_controls": tm_controls,
            "split":  split,
            "trace_id": trace_id,
            "decision_id": decision_id,
            "bundle_id": bundle_id_final,
            "idempotency_key": idempotency_key,
        },
    }

def make_rm_shutdown_event(*,
                           ts_ms: Optional[int] = None,
                           duration_ms: Optional[float] = None,
                           pending_tasks: int = 0,
                           timed_out: bool = False) -> Dict[str, Any]:
    """Envelope minimale pour tracer un arrêt RiskManager (best-effort)."""
    now_ms = int(ts_ms if ts_ms is not None else time.time() * 1000)
    return {
        "event_type": "rm.shutdown",
        "schema_version": SCHEMA_VERSION,
        "ts_ms": now_ms,
        "duration_ms": float(duration_ms) if duration_ms is not None else None,
        "pending_tasks": int(pending_tasks or 0),
        "timed_out": bool(timed_out),
    }

class ReasonCodes:
    # Scanner (SC_*)
    SC_BOOK_STALE = "SC_BOOK_STALE"
    SC_FEES_UNKNOWN = "SC_FEES_UNKNOWN"
    SC_FEES_STALE = "SC_FEES_STALE"
    SC_SLIP_UNKNOWN = "SC_SLIP_UNKNOWN"
    SC_SLIP_STALE = "SC_SLIP_STALE"
    SC_VOL_UNKNOWN_OR_STALE = "SC_VOL_UNKNOWN_OR_STALE"
    SC_BELOW_MIN_BPS = "SC_BELOW_MIN_BPS"
    SC_SHALLOW_BOOK = "SC_SHALLOW_BOOK"

    # RM (RM_*)
    RM_MARKETDATA_STALE = "RM_MARKETDATA_STALE"
    RM_PRUDENCE_BLOCK = "RM_PRUDENCE_BLOCK"
    RM_BUDGET_EXHAUSTED = "RM_BUDGET_EXHAUSTED"
    RM_CAP_EXCEEDED_BUNDLE = "RM_CAP_EXCEEDED_BUNDLE"
    RM_CAP_EXCEEDED_BRANCH = "RM_CAP_EXCEEDED_BRANCH"
    RM_BELOW_MIN_NET_BPS = "RM_BELOW_MIN_NET_BPS"
    RM_CLOCK_SKEW = "RM_CLOCK_SKEW"
    RM_BALANCE_TTL_BLOCK = "RM_BALANCE_TTL_BLOCK"
    RM_BALANCE_TTL_DEGRADED = "RM_BALANCE_TTL_DEGRADED"
    RM_MODE_SEVERE_IOC_ONLY = "RM_MODE_SEVERE_IOC_ONLY"

    # Engine (ENGINE_*)
    # TT
    ENGINE_TT_REVALIDATE_FAIL = "ENGINE_TT_REVALIDATE_FAIL"
    ENGINE_TT_STOP_TIME_BUDGET = "ENGINE_TT_STOP_TIME_BUDGET"
    ENGINE_TT_STOP_EDGE_FLOOR = "ENGINE_TT_STOP_EDGE_FLOOR"
    ENGINE_TT_STOP_ACK_SLO = "ENGINE_TT_STOP_ACK_SLO"
    ENGINE_TT_DUALSUBMIT_FAIL = "ENGINE_TT_DUALSUBMIT_FAIL"
    ENGINE_TT_PANIC_HEDGE = "ENGINE_TT_PANIC_HEDGE"
    
    # TM
    ENGINE_TMGUARD_QPOS_AHEAD_QUOTE = "ENGINE_TMGUARD_QPOS_AHEAD_QUOTE"
    ENGINE_TMGUARD_QPOS_ETA = "ENGINE_TMGUARD_QPOS_ETA"
    ENGINE_TM_WATCHDOG_TIMEOUT = "ENGINE_TM_WATCHDOG_TIMEOUT"
    ENGINE_TM_HEDGE_TTL_BREACH = "ENGINE_TM_HEDGE_TTL_BREACH"
    ENGINE_TM_REVALIDATE_FAIL = "ENGINE_TM_REVALIDATE_FAIL"
    ENGINE_TM_PANIC_MODE = "ENGINE_TM_PANIC_MODE"

    TEMPETE = "TEMPETE"
    CALM_ENTRY = "CALM_ENTRY"
    ENGINE_ERRORS = "ENGINE_ERRORS"
    RM_OVERRIDE_EXCEPTION = "RM_OVERRIDE_EXCEPTION"
    SIGNAL_MISSING_PREFIX = "SIGNAL_MISSING_"

    @staticmethod
    def signal_missing(name: str) -> str:
        return f"{ReasonCodes.SIGNAL_MISSING_PREFIX}{name}"

KNOWN_REASON_CODES = {
    # Scanner
    "SC_BOOK_STALE", "SC_FEES_UNKNOWN", "SC_FEES_STALE", "SC_SLIP_UNKNOWN",
    "SC_SLIP_STALE", "SC_VOL_UNKNOWN_OR_STALE", "SC_BELOW_MIN_BPS", "SC_SHALLOW_BOOK",

    # RM
    "RM_MARKETDATA_STALE", "RM_PRUDENCE_BLOCK", "RM_BUDGET_EXHAUSTED",
    "RM_CAP_EXCEEDED_BUNDLE", "RM_CAP_EXCEEDED_BRANCH", "RM_BELOW_MIN_NET_BPS",
    "RM_CLOCK_SKEW", "RM_BALANCE_TTL_BLOCK", "RM_BALANCE_TTL_DEGRADED",
    "RM_MODE_SEVERE_IOC_ONLY", "RM_ENGINE_NOT_READY", "RM_STALE_VOL",

    # Engine TT
    "ENGINE_TT_REVALIDATE_FAIL", "ENGINE_TT_STOP_TIME_BUDGET", "ENGINE_TT_STOP_EDGE_FLOOR",
    "ENGINE_TT_STOP_ACK_SLO", "ENGINE_TT_DUALSUBMIT_FAIL", "ENGINE_TT_PANIC_HEDGE",

    # Engine TM
    "ENGINE_TMGUARD_QPOS_AHEAD_QUOTE", "ENGINE_TMGUARD_QPOS_ETA", "ENGINE_TM_WATCHDOG_TIMEOUT",
    "ENGINE_TM_HEDGE_TTL_BREACH", "ENGINE_TM_REVALIDATE_FAIL", "ENGINE_TM_PANIC_MODE",
    "RM_CAPS_INVALID",
    "ENGINE_SUBMIT_TIMEOUT",
    "RM_CAPS_BROKEN",
    "RATE_LIMIT_TIMEOUT",
    "MM_THROTTLED_PLACE",
    "ENGINE_REJECT",
    "ENGINE_BACKPRESSURE_QUEUE_FULL",
    "ENGINE_BACKPRESSURE_HIGH_WM",
    "LOGGERH_BTL_QUEUE_FULL_DROP",
    "LOGGERH_BTL_PUT_TIMEOUT_DROP",
    "LOGGERH_BTL_FLUSH_EMIT_FAILED",
    "LOGGERH_BTL_FILTER_EXCEPTION",
    "LOGGERH_SCHEMA_MISSING_ID",
    "LOGGERH_SCHEMA_MISSING_FIELD",
    "LOGGERH_JSONL_QUEUE_FULL",
    "LOGGERH_JSONL_SERIALIZE_ERROR",
    "LOGGERH_JSONL_WRITE_ERROR",
    "LOGGERH_JSONL_DRAIN_TIMEOUT",
    "LOGGERH_DB_LANE_QUEUE_FULL",
    "REGION_UNSUPPORTED_JP",
    "PNL_RECO_SKIPPED_ADAPTER_MISSING",
    "PNL_RECO_SKIPPED_FLAG",
    "PNL_RECO_MISMATCH_ABS_DIFF_QUOTE",
    "PNL_RECO_MISMATCH_TRADES_COUNT",
    "PNL_RECO_EXCEPTION",
    "DB_UNIQUE_DUPLICATE_IGNORED",
    "DB_SCHEMA_MISSING_TS_MS",
    "DB_ROTATE_FAILED",
    "PNL_INCOMPLETE_MISSING_NET_PROFIT",
    "PNL_DERIVED_FROM_BPS",
    "PNL_FX_MISSING_FOR_NON_USDC",
    "BOOT_DEP_MISSING",
    "BOOT_DEP_START_FAIL",
    "BOOT_READY_BLOCKED",
    "BOOT_STOP_TIMEOUT",
    "BOOT_SCANNER_PROXY_UNBOUND",
    "BOOT_SCANNER_PROXY_BUFFER_FULL",
    "BOOT_SCANNER_PROXY_BUFFER_DISABLED",
    "BOOT_SCANNER_PROXY_FLUSH_FAILED",
    "OBS_METRICS_UNAVAILABLE",
    "SCANNER_RM_CALLBACK_MISSING",
    "SCANNER_HISTORY_SINK_MISSING",
    "SCANNER_PAIR_NOT_IN_UNIVERSE",
    "SCANNER_PAIR_NOT_IN_PRIORITY",
    "SCANNER_PAIR_PAUSED",
    "SCHEMA_BAD_TS",
    "schema_mismatch",
    "SIM_SHADOW_EXCEPTION",
    "DUPLICATE_BUNDLE",
    "PWS_CALLBACK_NO_LOOP",
    "ROUTER_HEALTH_UNCONSUMED",
    "REGION_JP_DISABLED_BY_DEFAULT",
    "BOOT_MODE_FALLBACK",
    "RPC_MISSING_IDEMPOTENCY_KEY",
    "TRADING_NOT_READY",
    "RPC_IDEMPOTENCY",
    "RPC_IDEMPOTENCY_DEGRADED",
    "PWS_QUEUE_BACKPRESSURE_TIMEOUT",
    "TRUTH_PERSISTENCE_FAILED",
    "ENGINE_DEDUP_BROKEN",
    "IDEMPOTENCY_MISSING",
    "REB_LOCK",
    "ENGINE_REBAL_LOCKED",
    "BUNDLE_ILLEGAL",
    "DEDUP",
    "REB_LOCK_CHECK_FAILED",
    "MM_PREEMPTED",
    "PREEMPT_TT",
    "PREEMPT_TM",
    "PREEMPT_REB",
    "PREEMPT_HEDGE",
    "TT_CONTRACT_INVALID",
    "TM_CONTRACT_INVALID",
    "TM_DISABLED",
    "MM_OFF_NOT_READY",
    "REB_CONTRACT_INVALID",
    "REB_DISABLED",
    "WD_MODULE_DEAD",
    "WD_LOOP_STOPPED",
    "WD_QUEUE_BACKLOG",
    "WD_STALE",
    "PUBLIC_FEED_STALE",
    "PUBLIC_FEED_COVERAGE_LOW",
    "PRIVATE_WS_STALE",
    "PWS_UNSAFE_DEDUP",
    "ENGINE_BLOCKED",
    "BALANCE_STALE",
    "MISSING_FIELD",
    "FALLBACK_THRESHOLD_USED",
    "HEALTH_QUEUE_BACKLOG",
    "ROUTER_QUEUE_FULL_PAIR_FROZEN",
    "TRANSFER_FSM_IN_PROGRESS_SKIP",
    "TRANSFER_FSM",
    "ENGINE_TT_POLICY_NO_PLAN",
    "ENGINE_TT_POLICY_DEGRADED_STAGGERED",
    "ENGINE_TT_POLICY_IOC_ONLY",
    "ENGINE_TT_REVALIDATE_FAIL",
    "ENGINE_TT_FRAGMENT_NOT_PROFITABLE",
    "RM_MIN_EDGE_WITH_LAT_PENALTY",
    "ENGINE_TT_STOP_EDGE_FLOOR",
    "ENGINE_TT_STOP_ACK_SLO",
    "ENGINE_TT_STOP_TIME_BUDGET",
    "ENGINE_TT_STOP_PANIC_HEDGE",
    "ENGINE_TT_PWS_UNHEALTHY",
    "ENGINE_TT_BOOK_STALE",
    "ENGINE_TT_ROUTER_STALE",
    "TRANSFER_FAILED",
    "XFER_STUCK_SUBMITTED_FAIL_CLOSED",
    ReasonCodes.TEMPETE,
    ReasonCodes.CALM_ENTRY,
    ReasonCodes.ENGINE_ERRORS,
    ReasonCodes.RM_OVERRIDE_EXCEPTION,
}
def canonical_transfer_id(payload: Dict[str, Any]) -> str:
    base = {
        "exchange": str(payload.get("exchange") or ""),
        "from_alias": str(payload.get("from_alias") or payload.get("from") or ""),
        "to_alias": str(payload.get("to_alias") or payload.get("to") or ""),
        "from_wallet": str(payload.get("from_wallet") or ""),
        "to_wallet": str(payload.get("to_wallet") or ""),
        "ccy": str(payload.get("ccy") or payload.get("currency") or ""),
        "amount": float(payload.get("amount") or payload.get("amount_quote") or payload.get("amount_usdc") or 0.0),
        "type": str(payload.get("type") or payload.get("kind") or "transfer"),
    }
    data = json.dumps(base, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha1(data.encode("utf-8")).hexdigest()

def normalize_reason_code(reason: Optional[str]) -> Optional[str]:
    """Normalise un code de reason (safe, best-effort)."""
    if reason is None:
        return None
    try:
        r = str(reason).strip()
    except Exception:
        return None
    if not r:
        return None
    canon = r.upper()
    if canon in KNOWN_REASON_CODES or canon.startswith(ReasonCodes.SIGNAL_MISSING_PREFIX):
        return canon
    return canon
def reason_from_exc(exc: BaseException) -> str:
    """Retourne un reason_code canonique à partir d'une exception (best-effort)."""
    reason = getattr(exc, "reason", None)
    if reason:
        return normalize_reason_code(reason) or str(reason)
    kind = getattr(exc, "kind", None)
    if kind:
        normalized = normalize_reason_code(kind)
        return normalized or str(kind).strip().upper()
    exc_name = type(exc).__name__
    return normalize_reason_code(exc_name) or exc_name.strip().upper()
def tt_contract_missing_fields(payload: Dict[str, Any]) -> List[str]:
    meta = payload.get("meta") if isinstance(payload, dict) else {}
    meta = meta if isinstance(meta, dict) else {}
    missing: List[str] = []

    def _missing(field: str, value: Any) -> None:
        if value in (None, ""):
            missing.append(field)

    _missing("trace_id", meta.get("trace_id") or payload.get("trace_id"))
    _missing("decision_id", meta.get("decision_id") or payload.get("decision_id"))
    _missing("bundle_id", meta.get("bundle_id") or payload.get("bundle_id"))
    _missing("idempotency_key", meta.get("idempotency_key") or payload.get("idempotency_key"))

    notional = payload.get("notional_quote") or {}
    if not isinstance(notional, dict):
        missing.append("notional_quote")
    else:
        try:
            amount = float(notional.get("amount"))
        except Exception:
            amount = None
        if amount is None or amount <= 0.0:
            missing.append("notional_quote.amount")
        if not notional.get("ccy"):
            missing.append("notional_quote.ccy")

    strategy_tag = meta.get("strategy_tag") or meta.get("strategy") or meta.get("branch")
    if str(strategy_tag or "").upper() != "TT":
        missing.append("strategy_tag")

    kind = meta.get("kind")
    _missing("kind", kind)
    return missing


def tm_contract_missing_fields(payload: Dict[str, Any]) -> List[str]:
    meta = payload.get("meta") if isinstance(payload, dict) else {}
    meta = meta if isinstance(meta, dict) else {}
    missing: List[str] = []

    def _missing(field: str, value: Any) -> None:
        if value in (None, ""):
            missing.append(field)

    _missing("trace_id", meta.get("trace_id") or payload.get("trace_id"))
    _missing("decision_id", meta.get("decision_id") or payload.get("decision_id"))
    _missing("bundle_id", meta.get("bundle_id") or payload.get("bundle_id"))
    _missing("idempotency_key", meta.get("idempotency_key") or payload.get("idempotency_key"))

    notional = payload.get("notional_quote") or {}
    if not isinstance(notional, dict):
        missing.append("notional_quote")
    else:
        try:
            amount = float(notional.get("amount"))
        except Exception:
            amount = None
        if amount is None or amount <= 0.0:
            missing.append("notional_quote.amount")
        if not notional.get("ccy"):
            missing.append("notional_quote.ccy")

    strategy_tag = meta.get("strategy_tag") or meta.get("strategy") or meta.get("branch")
    if str(strategy_tag or "").upper() != "TM":
        missing.append("strategy_tag")

    kind = meta.get("kind")
    _missing("kind", kind)

    route_id = meta.get("route_id")
    _missing("route_id", route_id)
    return missing


def reb_contract_missing_fields(payload: Dict[str, Any]) -> List[str]:
    meta = payload.get("meta") if isinstance(payload, dict) else {}
    meta = meta if isinstance(meta, dict) else {}
    missing: List[str] = []

    def _missing(field: str, value: Any) -> None:
        if value in (None, ""):
            missing.append(field)

    _missing("trace_id", meta.get("trace_id") or payload.get("trace_id"))
    _missing("decision_id", meta.get("decision_id") or payload.get("decision_id"))
    _missing("bundle_id", meta.get("bundle_id") or payload.get("bundle_id"))
    _missing("idempotency_key", meta.get("idempotency_key") or payload.get("idempotency_key"))

    notional = payload.get("notional_quote") or {}
    if not isinstance(notional, dict):
        missing.append("notional_quote")
    else:
        try:
            amount = float(notional.get("amount"))
        except Exception:
            amount = None
        if amount is None or amount <= 0.0:
            missing.append("notional_quote.amount")
        if not notional.get("ccy"):
            missing.append("notional_quote.ccy")

    strategy_tag = meta.get("strategy_tag") or meta.get("strategy") or meta.get("branch")
    if str(strategy_tag or "").upper() != "REB":
        missing.append("strategy_tag")

    combo_signature = meta.get("combo_signature")
    _missing("combo_signature", combo_signature)

    kind = meta.get("kind")
    _missing("kind", kind)

    route_id = meta.get("route_id")
    _missing("route_id", route_id)
    return missing

# -----------------------------------------------------------------------------
# Exports publics
# -----------------------------------------------------------------------------
__all__ = [
    "SCHEMA_VERSION", "SCHEMA_VERSION_MAJOR", "ReasonCodes",
    "Side", "Action", "Liquidity", "PortfolioRiskMetricsFields",
    "MarketEvent", "VolEvent", "SlipEvent", "HealthEvent",
    "Opportunity", "RiskDecision", "EngineAction",
    "SubmitLeg", "OrderIntent", "SubmitBundleRequest", "SubmitBundle", "CancelRequest", "BundleAck",
    "OrderModel", "FillModel", "FillNormalized",
    "PrivateFillEvent", "WSAliasStatusSnapshot", "RecoAliasStatusSnapshot", "BalanceSnapshotRM",
    "CMExposure", "CMSnapshots", "PortfolioSnapshot",
    "FeesSnapshot", "SlippageSnapshot", "VolatilitySnapshot",
    "DiscoveryResult", "SimResult",  "opportunity_from_scanner", "decision_submit_from_rm", "engine_action_from_decision",
    "submit_leg_from_intent", "engine_action_from_intent",
    "validate_payload_lite", "validate_opportunity_lite", "validate_decision_lite",
    "validate_engine_action_lite", "validate_order_lite", "validate_fill_lite",
    "try_validate_lite",
    "normalize_private_fill_event", "validate_private_fill_event_lite",
    "validate_submit_bundle_lite", "validate_cancel_lite", "validate_bundle_ack_lite",
    "validate_fill_normalized_lite", "validate_order_intent_lite", "make_submit_bundle",
    "make_rm_shutdown_event", "normalize_reason_code", "validate_idempotency_key_lite",
    "validate_ws_alias_status_snapshot_lite", "validate_reco_alias_status_snapshot_lite",
    "reason_from_exc",
    "tt_contract_missing_fields", "tm_contract_missing_fields", "reb_contract_missing_fields",
    "validate_balance_snapshot_rm_lite", "normalize_leg_dict",
    # >>> PATCH #3 — helpers publics
    "norm_symbol", "pair_key","encode_flow_meta",
    # Fragmentation helpers
    "normalize_frontload_weights", "build_fragment_plan", "validate_fragment_plan",
]