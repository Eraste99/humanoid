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

Patchs demandés (inclus)
------------------------
1) Tolérance `side` (B/S/BUY/SELL/buy/sell → buy/sell) dans OrderIntent, SubmitLeg, EngineAction, OrderModel.
2) make_submit_bundle : paramètre `route` optionnel (None accepté).
3) Exports utilitaires `norm_symbol` et `pair_key` dans __all__.
"""

from __future__ import annotations
import enum
import math
import time
import uuid
from typing import Any, Dict, List, Optional

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

def _inc(counter, *labels):
    try:
        counter.labels(*labels).inc()
    except Exception:
        return None

# -----------------------------------------------------------------------------
# Pydantic compat v2/v1 (avec fallback no-op)
# -----------------------------------------------------------------------------
try:
    from pydantic import BaseModel, Field, ValidationError
    try:
        from pydantic import field_validator, model_validator  # v2
    except Exception:  # pragma: no cover
        from pydantic import validator as field_validator      # type: ignore
        from pydantic import root_validator as model_validator  # type: ignore
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
        return ""
    s = str(sym).strip().upper().replace("/", "-")
    return s if keep_dash else s.replace("-", "")

def _norm_pair_key(sym: str) -> str:
    """Clé de paire compacte 'BTCUSDC' (sans tirets)."""
    return _norm_symbol(sym, keep_dash=False)


def _norm_exchange(sym: str) -> str:
    return (sym or "").strip().upper()

# >>> PATCH #3 — Helpers publics exposés
def norm_symbol(sym: str, keep_dash: bool = True) -> str:
    """Wrapper public vers _norm_symbol (exposé dans __all__)."""
    return _norm_symbol(sym, keep_dash=keep_dash)

def pair_key(sym: str) -> str:
    """Wrapper public vers _norm_pair_key (exposé dans __all__)."""
    return _norm_pair_key(sym)

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
) -> Dict[str, Any]:
    """Construit un plan de fragmentation canonique (amounts + groups).

    La planification reste pure : aucun accès au RiskManager/Engine, uniquement
    des calculs déterministes basés sur le notional cible et la config.
    """
    total = max(0.0, float(total_quote))
    if total <= 0:
        return {
            "amounts": [],
            "groups": [],
            "avg_fragment_quote": 0.0,
            "total_quote": total,
            "source": source,
            "valid": False,
        }

    group_size = max(1, int(group_size))
    if desired_count and desired_count > 0:
        count = int(desired_count)
    elif avg_fragment_quote and avg_fragment_quote > 0:
        count = max(1, int(round(total / float(avg_fragment_quote))))
    else:
        count = max(1, int(round(total / max(min_fragment_quote, 1.0))))

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
        if per_slice < min_fragment_quote and size > 1:
            size = max(1, int(min(size, math.ceil(budget / max(min_fragment_quote, 1.0)))))
            per_slice = budget / size
        for _i in range(size):
            amt = per_slice
            amounts.append(amt)
            groups.append(label)

    diff = total - sum(amounts)
    if amounts and abs(diff) > 1e-6:
        amounts[-1] = max(0.0, amounts[-1] + diff)

    # Fusionner une queue trop petite avec l'avant-dernière tranche
    if len(amounts) >= 2 and amounts[-1] < (min_fragment_quote * 0.5):
        merged = amounts[-1] + amounts[-2]
        amounts[-2] = merged
        amounts.pop()
        if groups:
            groups.pop()

    avg = (sum(amounts) / max(1, len(amounts))) if amounts else 0.0
    return {
        "amounts": amounts,
        "groups": groups,
        "avg_fragment_quote": avg,
        "total_quote": total,
        "desired_count": desired_count,
        "weights": normalize_frontload_weights(weights, max_fragments=count),
        "group_size": group_size,
        "source": source,
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
    total = max(0.0, float(total_quote))
    amts = []
    groups = []
    try:
        amts = [float(a) for a in (plan or {}).get("amounts", []) if float(a) > 0]
    except Exception:
        amts = []
    try:
        groups = list((plan or {}).get("groups", []))
    except Exception:
        groups = []

    valid = True

    if max_fragments and max_fragments > 0:
        amts = amts[: max_fragments]
        groups = groups[: len(amts)]

    if len(amts) != len(groups) or not amts:
        valid = False
    if any(a < min_fragment_quote for a in amts):
        valid = False
    if any(g not in FRAGMENT_GROUPS for g in groups):
        valid = False
    if total > 0:
        if abs(sum(amts) - total) > max(tol, tol * max(1.0, total)):
            valid = False

    if not valid:
        amts = [total] if total > 0 else []
        groups = ["G1"] if total > 0 else []

    avg = (sum(amts) / max(1, len(amts))) if amts else 0.0
    out = {
        "amounts": amts,
        "groups": groups,
        "avg_fragment_quote": avg,
        "total_quote": total,
        "valid": valid,
    }
    out.update({k: v for k, v in (plan or {}).items() if k not in out})
    return out

# -----------------------------------------------------------------------------
# Sous-modèles typés (riches)
# -----------------------------------------------------------------------------
class Frag(_Cfg, BaseModel):
    """Fragmentation front-load canonique (G1/G2/G3, plan embarqué)."""

    group: Optional[str] = Field(default=None)
    idx: Optional[int] = Field(default=None, ge=0)
    total: Optional[int] = Field(default=None, ge=1)
    weight: Optional[float] = Field(default=None, ge=0)
    planned_notional_quote: Optional[float] = Field(default=None, ge=0)
    plan: Optional[Dict[str, Any]] = None
    # Legacy / compat
    cohort: Optional[str] = None
    weights: Optional[List[float]] = None  # ex: [0.50, 0.35, 0.15]


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

SubmitBundle = SubmitBundleRequest  # alias public

class CancelRequest(_Cfg, BaseModel):
    order_id: str
    exchange: Optional[str] = None
    route_id: Optional[str] = None
    meta: Dict[str, Any] = Field(default_factory=dict)

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
    return EngineAction(
        symbol=_norm_symbol(symbol or dec.symbol or ""),
        side=dec.side,
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

def validate_submit_bundle_lite(payload: Dict[str, Any]) -> SubmitBundleRequest:
    return _validate_lite(SubmitBundleRequest, payload, "SubmitBundleRequest")

def validate_cancel_lite(payload: Dict[str, Any]) -> CancelRequest:
    return _validate_lite(CancelRequest, payload, "CancelRequest")

def validate_fill_normalized_lite(payload: Dict[str, Any]) -> FillNormalized:
    return _validate_lite(FillNormalized, payload, "FillNormalized")

def validate_order_intent_lite(payload: Dict[str, Any]) -> OrderIntent:
    return _validate_lite(OrderIntent, payload, "OrderIntent")

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
                       client_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Construit un bundle exécutable standard (dict).
    - `legs` : liste de legs dict (peuvent contenir "order": {...} ou être plats).
    - Rétro-compat: ajoute toujours "orders" miroir.
    """
    r = route or {}
    orders = []
    for l in legs:
        if isinstance(l, dict):
            orders.append(l.get("order", l))
        else:
            orders.append(l)

    return {
        "bundle_id": str(uuid.uuid4()),
        "client_id": client_id or "default",
        "mode": mode,                    # "TT" | "TM" | "MM" | "REB-as-TM"
        "tif": tif,
        "route": {                       # champ existant, clés tolérantes
            "buy_ex":  r.get("buy_ex"),
            "sell_ex": r.get("sell_ex"),
            "pair":    r.get("pair"),
        },
        "legs": legs,
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
        },
    }

# -----------------------------------------------------------------------------
# Exports publics
# -----------------------------------------------------------------------------
__all__ = [
    "SCHEMA_VERSION", "SCHEMA_VERSION_MAJOR",
    "Side", "Action", "Liquidity",
    "Opportunity", "RiskDecision", "EngineAction",
    "SubmitLeg", "OrderIntent", "SubmitBundleRequest", "SubmitBundle", "CancelRequest",
    "OrderModel", "FillModel", "FillNormalized",
    "FeesSnapshot", "SlippageSnapshot", "VolatilitySnapshot",
    "SimResult",
    "opportunity_from_scanner", "decision_submit_from_rm", "engine_action_from_decision",
    "submit_leg_from_intent", "engine_action_from_intent",
    "validate_payload_lite", "validate_opportunity_lite", "validate_decision_lite",
    "validate_engine_action_lite", "validate_order_lite", "validate_fill_lite",
    "validate_submit_bundle_lite", "validate_cancel_lite",
    "validate_fill_normalized_lite", "validate_order_intent_lite", "make_submit_bundle",
    # >>> PATCH #3 — helpers publics
    "norm_symbol", "pair_key",
    # Fragmentation helpers
    "normalize_frontload_weights", "build_fragment_plan", "validate_fragment_plan",
]