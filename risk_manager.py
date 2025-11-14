# -*- coding: utf-8 -*-
from __future__ import annotations
"""
RiskManager — OFFICIEL (V2.1) — Fusion ciblée (part 1/2)
=========================================================
- Quote-agnostic **USDC/EUR** (détection automatique de la devise de cotation).
- Multi-comptes TT/TM par CEX, routes autorisées, verrous par (exchange, alias, pair).
- Boucles internes : orderbooks / balances / rebalancing / volatility / fee-sync.
- Seuil **min_required_bps** dynamique via volatilité (Prometheus: set_dynamic_min).
- **TT/TM** avec simulateur (option `simulate_both_parallel`), front-loading + fragmentation.
- **TM NON_NEUTRAL** auto (edge/depth/vol/slip), hedge ratio configurable.
- Rebalancing intra-CEX / cross-CEX (internal transfers + rebalancing_trade ➜ Engine).
- Dry-run : soldes virtuels (USDC & EUR) + ajustements sur fills privés.
- Compat scanner : accepte `notional_quote` / `notional` et alias `volume_*` legacy.
- 🔌 Patchs fusionnés:
    • Pont Scanner (set_scanner/_emit_slippage_to_scanner + hook dans _loop_orderbooks)
    • Parsing notionnel
    • Robustesse _submodule_event & on_private_event (priorité quote_qty)
- 🟢 Readiness guard:
    • ready_event + NotReadyError
    • _ensure_ready() (RM prêt + Engine prêt)
    • handle_opportunity() / rebalance_tick() protègent les appels externes

⚠️ Ce fichier est la **partie 1/2** du module complet. La **partie 2/2** contient le reste de la classe
   (validations inventaire, stratégie TM, hooks Scanner/Engine, fragmentation, status, etc.).
"""

import asyncio, time, inspect
import time, uuid, random
from typing import Dict, Any, List, Optional
from modules.obs_metrics import inc_blocked
import logging
from modules.obs_metrics import TIME_SKEW_MS
from contracts.payloads import make_submit_bundle, submit_leg_from_intent
from dataclasses import dataclass, asdict
import json
from typing import Any, Dict, List, Optional, Callable, Tuple, Set
import math, random


from modules.rm_compat import getattr_int, getattr_float, getattr_str, getattr_bool, getattr_dict, getattr_list
from modules.risk_manager.rebalancing_manager import RebalancingManager
from modules.risk_manager.volatility_manager import VolatilityManager
from modules.risk_manager.slippage_and_fees_collector import SlippageAndFeesCollector
from modules.private_ws_reconciler import PrivateWSReconciler
from contracts.errors import (
    RMError, NotReadyError, DataStaleError, InconsistentStateError, ExternalServiceError
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
                                     POOL_GATE_THROTTLES_TOTAL)
                                     # gauge inventaire par ex/quote
except Exception:
    INVENTORY_USD = None  # tolérant si obs pas encore patché


from modules.obs_metrics import (
    mark_books_fresh, mark_balances_fresh, inc_rm_reject,
    set_rm_paused_count, set_dynamic_min
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

@dataclass(frozen=True)
class DecisionRecord:
    decision_id: str
    status: str               # "admitted" | "skipped"
    reason: str               # "" si admitted
    pair: str
    buy_exchange: str
    sell_exchange: str
    prudence: str
    slippage_kind: str
    ts_ns: int
    explain: dict

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":"))
# =============================================================================

# -----------------------------
# Readiness / erreurs
# -----------------------------
class NotReadyError(RuntimeError):
    """Levée quand le RiskManager (ou son Engine) n’est pas prêt."""
    pass


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

    def check_and_topup(self, balances: dict, place_order_cb) -> list:
        """
        balances: ex {"BINANCE:BNB": 55.0, "BYBIT:MNT": 10.0}
        place_order_cb(ex_key: str, symbol: str, quote_amount: float) -> dict
        """
        actions = []
        tgt = self.targets_quote()
        for k, target_amt in tgt.items():
            cur = float(balances.get(k, 0.0))
            low = self.low_watermarks.get(k, 0.0)
            if cur < low:
                step = min(self.topup_step, target_amt - cur) if target_amt > cur else self.topup_step
                ex, token = k.split(":")
                symbol = f"{token}USDT"  # adapte au symbol de cotation / marché base-quote
                resp = place_order_cb(ex, symbol, step)
                actions.append({"what":"fee_token_topup", "where":k, "step":step, "resp":resp})
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

    def maybe_upgrade(self, region: str, net_per_sc: float, slo_ok: bool, pnl_ok: bool, dd_ok: bool, inflight_util_ok: bool) -> Optional[str]:
        if not self._cooldown_ok(region): return None
        cur = getattr(self.pacer, "_profile", "NANO")
        ladder = ["NANO","MICRO","SMALL","MID","LARGE"]
        idx = ladder.index(cur) if cur in ladder else 0
        if idx >= len(ladder)-1: return None  # déjà LARGE
        # capital gates
        next_prof = ladder[idx+1]
        # évalue la gate capital (par sous-compte)
        gates = {
            "MICRO": 2000, "SMALL": 5000, "MID": 30000, "LARGE": 100000
        }
        gate_ok = (net_per_sc >= gates.get(next_prof, 1e12))
        # autres gates (SLO/risque/pression)
        if gate_ok and slo_ok and pnl_ok and dd_ok and inflight_util_ok:
            self.pacer.set_capital_profile(next_prof)
            self._mark_change(region)
            return next_prof
        return None

    def maybe_downgrade(self, region: str, severe_evt: bool, err_spike: bool, lag_spike: bool, drain_spike: bool, dd_breach: bool) -> Optional[str]:
        if not self._cooldown_ok(region): return None
        cur = getattr(self.pacer, "_profile", "NANO")
        ladder = ["NANO","MICRO","SMALL","MID","LARGE"]
        idx = ladder.index(cur) if cur in ladder else 0
        if idx <= 0: return None  # déjà NANO
        if severe_evt or err_spike or lag_spike or drain_spike or dd_breach:
            next_prof = ladder[idx-1]
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
        • **Simu dual parallèle** si supportée: `simulate_both_parallel()`.
        • **Verrou d’exécution** par (exchange, alias, pair) côté Engine.
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
            # Readiness (nouveau)
            ready_event: asyncio.Event | None = None,

    ) -> None:

        self.config = config  # alias explicite conservé
        self.bot_cfg = config  # cohérence avec le reste du code
        self.cfg = config
        self.fee_reserves = FeeTokenReservesPolicy(self.config)

        self.inventory_cap_quote = _cfg_float(
            self.cfg, "inventory_cap_quote",
            _cfg_float(self.cfg, "inventory_cap_usd", 1500.0)
        )
        self.inventory_cap_quote = _cfg_float(
            self.cfg, "inventory_cap_quote",
            _cfg_float(self.cfg, "inventory_cap_usd", 1500.0)
        )
        # conservez les attributs existants pour limiter le diff interne
        self.inventory_cap_usd = self.inventory_cap_quote  # compat interne



        # --- MM toggles & budgets ---
        self.enable_mm = bool(getattr(self.bot_cfg, "enable_maker_maker", False))
        self.mm_ttl_ms = _cfg_int(self.bot_cfg, "mm_ttl_ms", 2200)
        self.mm_alias = _cfg_str(self.bot_cfg, "mm_alias_name", "MM").upper()
        # ==== [ADD INSIDE __init__ RIGHT AFTER "MM toggles & budgets" BLOCK] =========
        # Kill switch global + budgets (en mémoire, resetés par ton scheduler quotidien)


        self.global_kill_switch = bool(getattr(self.cfg, "global_kill_switch", False))
        self.daily_strategy_budget_quote = _cfg_dict(self.bot_cfg, "daily_strategy_budget_quote", {})
        self._spent_today_quote = {"TT": 0.0, "TM": 0.0, "MM": 0.0}

        # Optionnel: fichier JSONL de décision (audit)

        self.decision_log_path = _cfg_str(self.cfg, "decision_log_path","")  # vide = pas de fichier
        # Politique pré-filtre: source slippage et seuils de fraicheur (utilise déjà tes cfg si présents)
        self.slippage_source = _cfg_str(self.cfg, "sfc_slippage_source", "ewma")
        self.max_book_age_s    = _cfg_float(self.cfg, "max_book_age_s", 1.0)
        self.max_book_age_s    = _cfg_float(self.cfg, "max_book_age_s", 1.0)
        # =============================================================================

        # Préemption & caps notionnels par stratégie/CEX (devise de cotation)
        self.preempt_mm_for_tt_tm = bool(getattr(self.bot_cfg, "preempt_mm_for_tt_tm", True))
        self.per_strategy_notional_cap   = _cfg_dict(self.bot_cfg, "per_strategy_notional_cap", {})
        # --- budgets virtuels par quote (USDC/EUR) ---
        # ex: {"BINANCE":{"USDC": 25000.0, "EUR": 0.0}, ...}
        self.virt_balances: dict[str, dict[str, float]] = {}

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
        self.reconciler = PrivateWSReconciler()
        self.reconciler._lookup = getattr(self, "lookup_inflight", None)
        self.reconciler._resync_order = getattr(self.engine, "resync_order", None)
        self.reconciler._resync_alias = getattr(self.engine, "resync_alias", None)

        # --- Instanciation lazy des sous-modules internes (si non injectés) ---

        if self.vol_manager is None:
            self.vol_manager = VolatilityManager(cfg=self.bot_cfg)
        else:
            logger.debug("[RiskManager] using injected VolatilityManager: %s", type(self.vol_manager).__name__)

        if self.slip_collector is None:
            self.slip_collector = SlippageAndFeesCollector(cfg=self.bot_cfg)
        else:
            logger.debug("[RiskManager] using injected SlippageAndFeesCollector: %s", type(self.slip_collector).__name__)

        # Contrat fort pour le collector de slippage
        if self.slip_collector is None:
            raise RuntimeError("slip_collector manquant (contractuel)")

        fn = getattr(self.slip_collector, "ingest_slippage_bps", None)
        if not callable(fn):
            raise TypeError(
                "slip_collector doit exposer ingest_slippage_bps(pair, exchange, side, qty, slip_bps, ts_ns=None)"
            )

        if self.rebalancing is None:
            self.rebalancing = RebalancingManager(rm=self, enabled_exchanges=self.exchanges)
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

        # Routes autorisées tri-CEX (configurable)
        self.allowed_routes: Set[Tuple[str, str]] = set(getattr(
            config, "allowed_routes",
            {
                ("BINANCE", "BYBIT"), ("BYBIT", "BINANCE"),
                ("BINANCE", "COINBASE"), ("COINBASE", "BINANCE"),
                ("BYBIT", "COINBASE"), ("COINBASE", "BYBIT"),
            }
        ))
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
        self.dynamic_min_required = bool(getattr(config, "dynamic_min_required", True))
        self.base_min_bps = _cfg_float(config, "base_min_bps", 20.0)
        self.dynamic_K = _cfg_float(config, "dynamic_K", 0.3)
        self.min_bps_floor = _cfg_float(config, "min_bps_floor", 10.0)
        self.min_bps_cap = _cfg_float(config, "min_bps_cap", 60.0)

        # Caps d’inventaire / skew guard (cap interprété en **devise de cotation**)
        self.inventory_cap_usd = _cfg_float(config, "inventory_cap_usd", 1500.0)
        self.inventory_skew_max_pct = _cfg_float(config, "inventory_skew_max_pct", 25.0)
        self.inventory_rebal_exempt = bool(getattr(config, "inventory_rebal_exempt", True))

        self.max_book_age_s = _cfg_float(self.cfg, "max_book_age_s", 1.0)
        self.max_clock_skew_ms = _cfg_float(self.cfg,"max_clock_skew_ms", 250.0)

        # Dry-run : soldes virtuels (USDC & EUR / exchange / alias)
        self._virtual_balances: Dict[str, Dict[str, Dict[str, float]]] = {}
        if bool(getattr(self.cfg, "dry_run", False)):
            default_usdc = _cfg_float(self.cfg,"dry_usdc_per_account", 2000.0)
            default_eur = _cfg_float(self.cfg,"dry_eur_per_account", default_usdc)

            for ex in self.exchanges:
                self._virtual_balances[ex] = {
                    "TT": {"USDC": default_usdc, "EUR": default_eur},
                    "TM": {"USDC": default_usdc, "EUR": default_eur},
                }
            # 👉 normalise/merge une seule fois
            self.seed_virtual_balances(self._virtual_balances, overwrite=True)

        # Fragmentation (alignement Engine)
        self.min_fragment_usdc = _cfg_float(self.cfg,"min_fragment_usdc", 200.0)
        self.max_fragments = _cfg_int(self.cfg, "max_fragments", 5)
        self.fragment_safety_pad = _cfg_float(self.cfg,"fragment_safety_pad", 0.9)
        self.target_ladder_participation = _cfg_float(self.cfg,"target_ladder_participation", 0.25)
        self.frontload_weights: List[float] = _cfg_list_upper(self.cfg, "frontload_weights", [0.5, 0.35, 0.15])

        # PATCH (bridge Scanner)
        self._scanner_ref = None  # référence optionnelle au Scanner
        self.get_orderbooks_callback = None  # sera branché par le boot

        # Readiness
        self.ready_event: asyncio.Event = ready_event or asyncio.Event()
        self.fee_reserves = FeeTokenReservesPolicy(self.config)
        self.fee_buyer = FeeTokenBuyer(self.config, logger=getattr(self, "logger", None))
        # --- RM Mode Overlay (FSM P0) ---
        self.rm_mode = "NORMAL"  # NORMAL | OPP_VOLUME | OPP_VOL | SEVERE
        self._mode_since = 0.0
        self._mode_timeout_s = 30 * 60  # 30 min fenêtre opportuniste
        self._enter_hyst_s = 180  # 3 min verts
        self._exit_hyst_s = 120  # 2 min verts

        # Plancher net (empêche “volume toxique”)
        self._net_floor_bps =_cfg_float(self.cfg, "net_floor_bps", 4.5)

        # Deltas par mode (défauts sûrs ; profile-aware clamp appliqué plus bas)
        self._overlay = {
            "OPP_VOLUME": {"tt_min_bps_delta": -2.0, "tm_min_bps_delta": -2.0, "cap_factor": 1.0, "mm_enable": True},
            "OPP_VOL": {"tt_min_bps_delta": +3.0, "tm_min_bps_delta": +6.0, "cap_factor": 0.7, "mm_enable": False},
            "SEVERE": {"tt_min_bps_delta": +5.0, "tm_min_bps_delta": +8.0, "cap_factor": 0.5, "mm_enable": False,
                       "ioc_only": True},
        }
        # --- PnL guard (config & état) ---
        self._pnl_guard_lvl1 = _cfg_float(self.cfg, "pnl_guard_day_lvl1_pct", -0.3)  # %
        self._pnl_guard_lvl2 = _cfg_float(self.cfg, "pnl_guard_day_lvl2_pct", -0.7)  # %
        self._pnl_cooldown_s = _cfg_int(self.cfg, "pnl_cooldown_s", 1800)  # 30 min
        self._last_bad_ts = 0.0

    def _rm_check_fee_reserves(self) -> None:
        """
        Vérifie les réserves de tokens de fees (BNB/MNT…) et passe des ordres d'appoint (MARKET/IOC).
        Exécutée toutes les 15–30 min (ou à l’alerte “sous seuil”).
        """
        try:
            targets = self.fee_reserves.targets_quote()
            if not targets:
                return

            # 1) lire les soldes (adapte get_balance(...) à tes access)
            balances = {}
            for k in targets.keys():
                ex, tok = k.split(":")
                try:
                    bal = float(self.get_balance(ex, tok) or 0.0)  # <- implémentation projet
                except Exception:
                    bal = 0.0
                balances[k] = bal

            # 2) vérifier low-watermark et top-up si nécessaire
            actions = []
            for k, target_amt in targets.items():
                cur = float(balances.get(k, 0.0))
                low = float(self.fee_reserves.low_watermarks.get(k, 0.0))
                if cur < low:
                    step = float(min(self.fee_reserves.topup_step,
                                     (target_amt - cur)) if target_amt > cur else self.fee_reserves.topup_step)
                    resp = self.fee_buyer.topup_for_key(k, quote_amount=step)
                    actions.append({"what": "fee_token_topup", "key": k, "step": step, "resp": resp})

            if actions and getattr(self, "logger", None):
                self.logger.info("Fee token top-ups: %s", actions)

        except Exception:
            if getattr(self, "logger", None):
                self.logger.exception("Fee reserves check failed")



    def _get_pnl_day_pct(self) -> float:
        """
        Source flexible:
          - si self.pnl_guard_provider est défini (callable) => l’utiliser,
          - sinon, fallback sur self.pnl_day_pct (si exposé ailleurs),
          - sinon 0.0 (guard inactif).
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
        if self.rm_mode in ("OPP_VOL", "SEVERE") and day > min(self._pnl_guard_lvl1, -0.05):
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


    def _reb_cost_fn(self, route: dict) -> float:
        """
        Coût net en bps pour REB (pur, sans I/O).
        Utilise SFC.get_total_cost_pct(..., side="TM") car le bridge se fait en maker côté destination.
        """
        try:
            pct = float(self.slip_collector.get_total_cost_pct(
                route, side="TM", size_quote=float(getattr(self.cfg, "rebal_size_quote", 2000.0)),
                slippage_kind="ewma", prudence_key="NORMAL"
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

    def _cancel_open_mm_quotes_on_exchange(self, ex: str, *, reason: str = "preempt_tt_tm") -> None:
        """
        Préempte la liquidité MM côté Engine (si supporté).
        Tolérant: no-op si la méthode n'existe pas.
        """
        eng = getattr(self, "engine", None)
        if not eng:
            return
        for name in ("cancel_mm_quotes_on_exchange", "cancel_makers_on_exchange", "cancel_mm_on_exchange"):
            if hasattr(eng, name):
                try:
                    getattr(eng, name)(exchange=str(ex).upper(), reason=reason)
                except Exception:
                    logging.exception("Unhandled exception")
                break

    # === RM: capital net & profil ===
    @staticmethod
    def compute_capital_net_per_subaccount(self, gross_equity: float, fee_reserve_total: float) -> float:
        """
        Capital net exploitable = equity - réserves tokens fees (BNB/MNT...).
        gross_equity et fee_reserve_total sont par sous-compte.
        """
        return max(0.0, float(gross_equity) - float(fee_reserve_total))

    @staticmethod
    def decide_capital_profile(self, net_per_sc: float) -> str:
        if net_per_sc < 2000: return "NANO"
        if net_per_sc < 5000: return "MICRO"
        if net_per_sc < 30000: return "SMALL"
        if net_per_sc < 100000: return "MID"
        return "LARGE"

    # === /RM: capital net & profil ===

    def _apply_caps_and_preempt(self, strategy: str, ex: str, desired_notional: float) -> float:
        """
        Applique le cap notionnel par (stratégie,CEX). Si TT/TM dépasse:
          - préempte MM sur ce CEX (si autorisé), puis tronque à cap.
        """
        cap = float(((self.per_strategy_notional_cap or {}).get(strategy, {}) or {}).get(str(ex).upper(), float("inf")))
        if desired_notional <= cap:
            return max(0.0, desired_notional)
        if strategy in ("TT", "TM") and self.preempt_mm_for_tt_tm:
            self._cancel_open_mm_quotes_on_exchange(ex, reason="preempt_tt_tm")
        return max(0.0, cap)

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

    # modules/risk_manager.py — class RiskManager

    # risk_manager.py — class RiskManager

    def engine_enqueue_bundle(self, bundle: dict) -> None:
        """
        Soumet vers l’Engine avec SOFT CAP par SC et priorités:
          - TT/hedge jamais bloqués
          - cancels non bloqués (coalescing ailleurs)
          - makers (TM/MM) lissés (delay court configurable)
        API publique intacte.
        """
        engine = getattr(self, "engine", None)
        if not engine:
            if getattr(self, "log", None): self.log.warning("RM: no engine bound; drop bundle")
            return

        self._ensure_sc_softcap()

        meta = bundle.get("meta", {})
        ex = (meta.get("exchange") or bundle.get("exchange") or "").upper()
        alias = meta.get("account_alias") or meta.get("alias")
        branch = self._branch_of(meta, bundle)  # "TT"/"TM"/"MM"
        kind = (meta.get("kind") or bundle.get("kind") or "").upper()

        # Priorité: hedges non bloqués
        if kind in ("HEDGE", "TAKER") or branch == "TT":
            return self._enqueue_non_blocking(engine, bundle)

        # Cancels: laissent passer
        if kind == "CANCEL":
            return self._enqueue_non_blocking(engine, bundle)

        # Makers: appliquer SOFT CAP
        if not (ex and alias):
            return self._enqueue_non_blocking(engine, bundle)  # fail-open

        sc_key = (ex, alias, branch)
        gate = self._get_sc_bucket(sc_key)
        if gate.try_acquire():
            self._obs_sc_counter("sc_soft_acquired_total", ex, alias, branch)
            return self._enqueue_non_blocking(engine, bundle)

        # Quota SC atteint → lisser
        self._obs_sc_counter("sc_soft_delayed_total", ex, alias, branch)
        defer_ms = float(self._cfg("rl.sc.defer_ms", 20.0))
        self._schedule_delay(engine, bundle, defer_ms)

    # risk_manager.py — class RiskManager (suite)

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


    def _branch_of(self, meta: dict, bundle: dict) -> str:
        b = (meta.get("branch") or bundle.get("branch") or "").upper()
        if b in ("TT", "TM", "MM"):
            return b
        k = (meta.get("kind") or bundle.get("kind") or "").upper()
        if k in ("HEDGE", "TAKER"): return "TT"
        if k in ("MAKER_TM", "TM"):  return "TM"
        if k in ("MAKER_MM", "MM"):  return "MM"
        return "UNKNOWN"


    def _enqueue_non_blocking(self, engine, bundle):
        submit_coro = getattr(engine, "submit", None)
        if callable(submit_coro):
            try:
                import asyncio, threading
                try:
                    asyncio.get_running_loop().create_task(submit_coro(bundle))
                except RuntimeError:
                    threading.Thread(target=lambda: asyncio.run(submit_coro(bundle)),
                                     name="rm-submit-offloop", daemon=True).start()
                return
            except Exception:
                pass
        enqueue_fn = getattr(engine, "enqueue_bundle", None)
        place_fn = getattr(engine, "place_bundle", None)
        if callable(enqueue_fn):
            try:
                import inspect, asyncio, threading
                res = enqueue_fn(bundle)
                if inspect.iscoroutine(res):
                    try:
                        asyncio.get_running_loop().create_task(res)
                    except RuntimeError:
                        threading.Thread(target=lambda: asyncio.run(res),
                                         name="rm-enqueue-offloop", daemon=True).start()
            except Exception:
                pass
            return
        if callable(place_fn):
            place_fn(bundle)

    def _schedule_delay(self, engine, bundle, defer_ms: float):
        import asyncio, threading, time
        def _task():
            try:
                time.sleep(max(0.0, defer_ms / 1000.0))
                self._enqueue_non_blocking(engine, bundle)
            except Exception:
                if getattr(self, "log", None): self.log.exception("RM delay enqueue failed")

        try:
            asyncio.get_running_loop().create_task(self._async_delay_and_enqueue(engine, bundle, defer_ms))
        except RuntimeError:
            threading.Thread(target=_task, name="rm-delay-enqueue", daemon=True).start()

    async def _async_delay_and_enqueue(self, engine, bundle, defer_ms: float):
        import asyncio
        await asyncio.sleep(max(0.0, defer_ms / 1000.0))
        self._enqueue_non_blocking(engine, bundle)

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
                    except Exception:
                        pass

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
                INVENTORY_USD.labels(exu, q).set(self.virt_balances[exu][q])
        except Exception:
            logging.exception("Unhandled exception")
        return True

    # --- risk_manager.py (dans class RiskManager) ---

    def bind_reconciler(self, reconciler) -> None:
        """
        Injection tardive d'un PrivateWSReconciler unique (géré par le Boot).
        Idempotent. N'échoue jamais.
        """
        try:
            self.reconciler = reconciler
            if self.reconciler is None:
                return
            # Lookup inflight côté RM (si disponible)
            if hasattr(self, "lookup_inflight"):
                self.reconciler._lookup = self.lookup_inflight
            # Optionnels: helpers is_inflight / metrics, si présents
            if hasattr(self, "is_inflight"):
                self.reconciler._is_inflight = self.is_inflight
        except Exception:
            # best-effort: ne jamais faire tomber le RM si l'injection échoue
            pass

    def set_engine(self, engine) -> None:
        """
        Injection tardive de l'Engine (après création réelle dans le Boot).
        Repose les callbacks pour la resync (si reconciler présent).
        Idempotent. N'échoue jamais.
        """
        try:
            self.engine = engine
            if getattr(self, "reconciler", None) and self.engine:
                self.reconciler._resync_order = getattr(self.engine, "resync_order", None)
                self.reconciler._resync_alias = getattr(self.engine, "resync_alias", None)
        except Exception:
            pass

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
                INVENTORY_USD.labels(exu, q).set(self.virt_balances[exu][q])
        except Exception:
            logging.exception("Unhandled exception")


    def _update_inventory_metrics(self) -> None:
        """
        Pousse INVENTORY_USD(ex,quote) = somme des montants quote (USDC/EUR) sur tous les alias.
        Appeler après refresh balances et après transferts.
        """
        if INVENTORY_USD is None:
            return
        try:
            snap = self._balances_snapshot()
            by_ex_quote = {}  # ex -> quote -> total
            for ex, accounts in (snap or {}).items():
                exu = str(ex).upper()
                for _alias, assets in (accounts or {}).items():
                    for q in ("USDC", "EUR"):
                        if q in assets:
                            by_ex_quote.setdefault(exu, {}).setdefault(q, 0.0)
                            by_ex_quote[exu][q] += float(assets.get(q, 0.0))
            for ex, m in by_ex_quote.items():
                for q, v in (m or {}).items():
                    INVENTORY_USD.labels(ex=ex, quote=q).set(float(v))
        except Exception:
            logging.exception("Unhandled exception")

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

        self._tasks = [
            asyncio.create_task(self._loop_orderbooks(), name="rm-orderbooks"),
            asyncio.create_task(self._loop_balances(), name="rm-balances"),
            asyncio.create_task(self._loop_rebalancing(), name="rm-rebalancing"),
            asyncio.create_task(self._loop_volatility(), name="rm-volatility"),
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

        # Démarre la colle RM<->MBF a posteriori (pour ne pas bloquer start())
        if hasattr(self, "_mbf_glue"):
            try:
                await self._mbf_glue.start()
            except Exception:
                logger.exception("[RiskManager] mbf_glue.start() failed")

    async def stop(self) -> None:
        """Arrêt idempotent, join propre des boucles et de la glue."""
        self._running = False

        # Arrêt glue d'abord (évite push tardifs)
        if hasattr(self, "_mbf_glue"):
            try:
                await self._mbf_glue.stop()
            except Exception:
                logger.exception("[RiskManager] mbf_glue.stop() failed")

        tasks = getattr_list(self, "_tasks")
        for t in tasks:
            if t and not t.done():
                t.cancel()
        if tasks:
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception:
                pass
        self._tasks = []

        if hasattr(self, "ready_event"):
            try:
                self.ready_event.clear()
            except Exception:
                pass

        logger.info("[RiskManager] 🛑 Orchestrateur arrêté")

    # ------------------------------------------------------------------
    # API externes protégées par readiness (patch demandé)
    # ------------------------------------------------------------------

    # ==== [ADD THESE METHODS INSIDE class RiskManager (helpers section)] =========
    @staticmethod
    def _hash_decision_id(self, pair: str, buy: str, sell: str, ts_ns: int) -> str:
        base = f"{pair}|{buy}|{sell}|{ts_ns // 1_000_000}"  # tranche à la ms
        return str(abs(hash(base)))

    def _current_prudence(self, pair: str) -> str:
        # Volatility monitor -> RM -> VM : on reste compatible, on essaie de lire l’état courant si dispo
        try:
            vm = getattr(self, "vol_manager", None)
            if vm and hasattr(vm, "get_prudence_for_pair"):
                return str(vm.get_prudence_for_pair(pair) or "NORMAL")
        except Exception:
            pass
        return "NORMAL"

    def _budget_allows(self, strategy: str, notional_quote: float) -> tuple[bool, str]:
        cap = float(self.daily_strategy_budget_quote.get(strategy, 0.0))
        if cap <= 0.0:
            return True, ""
        spent = float(self._spent_today_quote.get(strategy, 0.0))
        if spent + float(notional_quote) > cap:
            return False, "BUDGET_EXCEEDED"
        return True, ""

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

            # Horloge
            try:
                ctx["time_skew_ms"] = float(TIME_SKEW_MS.get())  # import déjà présent dans ton fichier
            except Exception:
                ctx["time_skew_ms"] = 0.0
            if ctx["time_skew_ms"] > float(self.max_clock_skew_ms):
                return (False, "TIME_SKEW", ctx)

            # Fraîcheur OB (inchangé côté alimentation : volatility monitor -> RM -> VM ; ici on lit juste les OB du RM)
            b = self.get_book_snapshot(buy, pair)
            s = self.get_book_snapshot(sell, pair)
            if not (self._fresh_enough(b) and self._fresh_enough(s)):
                return (False, "STALE_BOOKS", ctx)

            # Prudence (VM) — on ne bloque que si ALERT
            prudence = self._current_prudence(pair)
            ctx["prudence"] = prudence
            if prudence == "ALERT":
                return (False, "PRUDENCE_ALERT", ctx)

            # Budget par stratégie (on lit le choix provisoire si fourni par le scanner sinon on re-choisira plus tard)
            strat = str((opp.get("expected_net_bps") or {}).get("best", "")).upper() or "TT"
            q, amt = self._normalize_notional_tuple(opp)  # ta fonction déjà existante
            ctx["notional_quote"] = {"quote": q, "amount": float(amt)}
            ok, why = self._budget_allows(strat, amt)
            if not ok:
                return (False, why, ctx)

            return (True, "", ctx)
        finally:
            try:
                RM_PREFLIGHT_MS.observe((time.perf_counter() - t0) * 1000.0)
            except Exception:
                pass

    def _emit_decision_record(self, status: str, reason: str, opp: dict, ctx: dict) -> None:
        """Émet un enregistrement JSONL minimal + métriques (admitted|skipped)."""
        try:
            pair = str(opp.get("pair") or opp.get("symbol") or "")
            buy = str(opp.get("buy_exchange") or "").upper()
            sell = str(opp.get("sell_exchange") or "").upper()
            ts_ns = time.time_ns()
            rec = DecisionRecord(
                decision_id=self._hash_decision_id(pair, buy, sell, ts_ns),
                status=status,
                reason=reason,
                pair=pair,
                buy_exchange=buy,
                sell_exchange=sell,
                prudence=str(ctx.get("prudence", "UNKNOWN")),
                slippage_kind=getattr_str(self, "slippage_source", "ewma"),
                ts_ns=ts_ns,
                explain=ctx or {},
            )
            # métriques
            RM_DECISIONS_TOTAL.labels(status).inc()
            if status == "skipped":
                RM_SKIPS_TOTAL.labels(reason or "UNKNOWN").inc()
            # JSONL optionnel
            if getattr(self, "decision_log_path", ""):
                with open(self.decision_log_path, "a", encoding="utf-8") as f:
                    f.write(rec.to_json() + "\n")
        except Exception:
            pass

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

    # =============================================================================

    # ==== [REPLACE WHOLE METHOD handle_opportunity WITH THIS ONE] ================
    async def handle_opportunity(self, opp: Dict[str, Any]) -> None:
        """
        Entrée stratégie (ex-Scanner) — pré-filtre industry-like puis délégation.
        Ne modifie pas la logique de décision interne : on garde exactement le pipeline existant.
        """
        # Readiness (comme avant)
        self._ensure_ready()

        # Pré-filtre
        admit, reason, ctx = self._preflight_gate(opp)
        if not admit:
            # audit + métriques puis sortie
            try:
                self._emit_decision_record("skipped", reason, opp, ctx)
            finally:
                return

        # audit "admitted" minimal (le détail final peut être émis plus loin si tu veux)
        self._emit_decision_record("admitted", "", opp, ctx)

        # Délégation au pipeline existant (inchangé)
        try:
            # si on_scanner_opportunity est synchrone dans ton code actuel, enlève "await" (garde une seule variante)
            res = self.on_scanner_opportunity(opp)
            if inspect.iscoroutine(res):
                await res
        except Exception:
            logging.exception("Unhandled exception during on_scanner_opportunity")

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
                        q = str(plan.get("quantum_quote") or "NA").upper()
                        REBAL_PLAN_QUANTUM_QUOTE.labels(q).set(float(plan.get("quantum") or 0.0))
                except Exception:
                    pass

            if hasattr(self.rebalancing, "push_history"):
                try:
                    self.rebalancing.push_history(imb, plan or {})
                except Exception:
                    logging.exception("Unhandled exception")

            await self._alert(
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

            # 1) Alerte centralisée (pager/digest)
            try:
                asyncio.create_task(self._alert(module, event.get("message", ""), pair=pair, alert_type=level))
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
                elif ev == "planned":
                    q = str(event.get("quote") or "NA").upper()
                    quantum = float(event.get("quantum_quote") or 0.0)
                    try:
                        REBAL_PLAN_QUANTUM_QUOTE.labels(q).set(quantum)
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

            # VOL: la boucle _loop_volatility publie déjà VOL_* → no-op ici.

        except Exception:
            try:
                logger.debug("[RiskManager] _submodule_event fallback", exc_info=False)
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
                min_ts_by_ex = {}
                for ex, pairs in (self._last_books or {}).items():
                    ts = []
                    for d in (pairs or {}).values():
                        t = int(d.get("exchange_ts_ms") or d.get("recv_ts_ms") or 0)
                        if t: ts.append(t)
                    if ts:
                        skew = (time.time() * 1000.0) - min(ts)
                        try:
                            TIME_SKEW_MS.labels(ex).set(max(0.0, skew))
                        except Exception:
                            pass
                if not isinstance(books, dict):
                    await asyncio.sleep(self.t_books)
                    continue

                self._last_books = books or {}
                mark_books_fresh("ALL")

                # Slippage & fees collector (passif)
                if self.slip_collector and hasattr(self.slip_collector, "collect_from_orderbooks"):
                    try:
                        self.slip_collector.collect_from_orderbooks(self._last_books)
                    except Exception:
                        logger.debug("[RiskManager] slippage collector collect failed", exc_info=False)

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

    async def _loop_balances(self):
        while self._running:
            try:
                if getattr_bool(self.cfg, "dry_run", False):
                    bals = self._virtual_balances.copy()
                    self._last_balances = bals
                    # pousser l'inventaire par ex/quote vers Prometheus
                    self._update_inventory_metrics()



                else:
                    # BalanceFetcher multi-comptes
                    bals = await self.balance_fetcher.get_all_balances(force_refresh=True)
                    self._last_balances = bals
                    # pousser l'inventaire par ex/quote vers Prometheus
                    self._update_inventory_metrics()
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

                if self.rebalancing and hasattr(self.rebalancing, "update_balances"):
                    self.rebalancing.update_balances(bals)
                    mark_balances_fresh()
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

                    await self._alert(
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
                for pair in self.symbols:
                    # 1) Monitor (inputs “bruts” issus du router)
                    metrics = self.vol_monitor.get_current_metrics(pair) or {}
                    thresholds = self.vol_monitor.get_current_thresholds(pair) or {}

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
                    sig = metrics.get("prudence_signal") or self.vol_monitor.get_prudence_signal(pair)
                    self._paused[pair] = (str(sig).lower() in ("modéré", "modere", "élevé", "eleve"))

                set_rm_paused_count(sum(1 for v in self._paused.values() if v))
                self.last_update = time.time()
                self._mark_loop_success("volatility")

            except Exception as e:
                logger.exception(f"[RiskManager] volatility loop: {e}")
                self._mark_loop_error("volatility", e)
            await asyncio.sleep(self.t_vol)

    # ===================== Revalidation & Profitability API =====================
    def _apply_mode_overrides(self) -> None:
        """
        Applique l'overlay du mode courant (sans jamais dépasser les caps du profil).
        - Ajuste min_bps TT (base_min_bps) et min_bps TM (tm_min_required_bps)
        - Recalcule les caps notionnels TT/TM/MM pour USDC/USDT/EUR (profile-aware × cap_factor)
        - Active/désactive MM et communique un hint IOC-only au moteur si nécessaire
        """
        # Profil capital (fallback safe)
        prof = getattr(self, "capital_profile", None) \
               or getattr(getattr(self, "pacer", None), "_profile", None) \
               or "NANO"

        ov = self._overlay.get(self.rm_mode, {})

        # 1) Min bps dynamiques
        base_tt = float(getattr(self, "base_min_bps", getattr(self.cfg, "base_min_bps", 6.5)))
        delta_tt = float(ov.get("tt_min_bps_delta", 0.0))
        self.base_min_bps = max(0.0, base_tt + delta_tt)

        base_tm = float(getattr(self, "tm_min_required_bps", getattr(self.cfg, "tm_min_required_bps_base", 11.0)))
        delta_tm = float(ov.get("tm_min_bps_delta", 0.0))
        self.tm_min_required_bps = max(0.0, base_tm + delta_tm)

        # 2) Caps notionnels profile-aware × cap_factor (down-clamp only)
        cap_eff = float(self._profile_cap_notional(profile=prof))  # méthode existante
        factor = float(ov.get("cap_factor", 1.0))

        caps = dict(getattr(self, "per_strategy_notional_cap", {}))  # {'TT': {'USDC': ...}, ...}
        for strat in ("TT", "TM", "MM"):
            for q in ("USDC", "USDT", "EUR"):
                cur = float(((caps.get(strat) or {}).get(q) or 0.0) or 0.0)
                target = min(cur or cap_eff, cap_eff) * factor  # JAMAIS d'upscale > cap_eff
                caps.setdefault(strat, {})[q] = max(10.0, float(target))
        self.per_strategy_notional_cap = caps

        # 3) MM & IOC-only
        self.enable_mm = bool(ov.get("mm_enable", getattr(self, "enable_mm", False)))
        if not self.enable_mm:
            try:
                # best-effort : adapte la venue si besoin
                self._cancel_open_mm_quotes_on_exchange("BINANCE", reason=f"rm_overlay:{self.rm_mode}")
            except Exception:
                pass

        # Hint pour l'Engine (TM en IOC si nécessaire)
        self._ioc_only = bool(ov.get("ioc_only", False))

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
        return float(getattr(self.cfg, "opp_age_fallback_s", 9_999.0))

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
        # 1) Fraîcheur du book
        age_s = self._latest_book_age_s()
        age_ok = bool(age_s <= float(getattr(self.cfg, "opp_vol_slip_age_s_max", 1.2)))

        # 2) Volatilité p95 (bps)
        try:
            vm = self.vol_manager.get_current_metrics(None) if hasattr(self, "vol_manager") else {}
            vol_p95 = float((vm or {}).get("p95_bps", 0.0))
        except Exception:
            vol_p95 = 0.0
        vol_ok = bool(vol_p95 <= float(getattr(self.cfg, "opp_vol_p95_bps_max", 40.0)))

        # 3) Rate-limits / 429
        rl_ok = bool(getattr(self, "rate_limits_healthy", True))

        # 4) Simulateur (shadow)
        try:
            shadow_p50 = float(getattr(self, "shadow_error_bps_p50", 0.0))
        except Exception:
            shadow_p50 = 0.0
        shadow_ok = bool(shadow_p50 <= float(getattr(self.cfg, "opp_shadow_p50_bps_max", 2.5)))

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
        age_s = self._latest_book_age_s()
        stale_bad = bool(age_s > float(getattr(self.cfg, "opp_vol_exit_slip_age_s_max", 1.6)))

        try:
            vm = self.vol_manager.get_current_metrics(None) if hasattr(self, "vol_manager") else {}
            vol_p95 = float((vm or {}).get("p95_bps", 0.0))
        except Exception:
            vol_p95 = 0.0
        vol_bad = bool(vol_p95 >= float(getattr(self.cfg, "oppvol_p95_bps_min", 80.0)))

        pws_bad = not bool(getattr(self, "private_ws_healthy", True))

        return bool(vol_bad or stale_bad or pws_bad)

    def _split_penalty_bps(self, buy_ex: str, sell_ex: str) -> float:
        """
        Penalty deterministica basata su metriche SPLIT correnti e soglie cfg.
        """
        mode = str(getattr(self, "split_mode", "EU_ONLY")).upper()
        if mode != "SPLIT":
            return 0.0

        base = float(getattr(self, "split_base_delta_ms", 0.0))
        skew = float(getattr(self, "split_skew_ms", 0.0))
        stale = float(getattr(self, "split_stale_ms", 0.0))

        thr_base = float(getattr(self.cfg, "split_breach_thr_base_ms", 180.0))
        thr_skew = float(getattr(self.cfg, "split_breach_thr_skew_ms", 40.0))
        thr_stal = float(getattr(self.cfg, "split_breach_thr_stale_ms", 1300.0))

        ratio = max(
            base / max(thr_base, 1e-9),
            skew / max(thr_skew, 1e-9),
            stale / max(thr_stal, 1e-9),
            0.0
        )
        cap = float(getattr(self.cfg, "split_penalty_bps_max", 6.0))
        return float(min(cap, cap * ratio))

    def _total_cost_bps(self, buy_ex: str, sell_ex: str, pair_key: str) -> float:
        """
        bps = 1e4 * total_cost_pct + split_penalty_bps
        """
        pct = self.get_total_cost_pct(buy_ex, sell_ex, pair_key)
        return float(1e4 * pct + self._split_penalty_bps(buy_ex, sell_ex))

    def _tick_mode(self) -> None:
        # 1) PnL-guard (peut forcer OPP_VOL / SEVERE)
        self._pnl_guard_tick()

        now = time.time()
        cur = self.rm_mode

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
            timeout_s = int(getattr(self, "_opp_volume_timeout_s",
                                    getattr(self, "_mode_timeout_s", 1800)))
            if (now - self._mode_since > timeout_s) or (not self._green_calme()) or net_floor_hit:
                self.rm_mode, self._mode_since = "NORMAL", now

        elif cur in ("OPP_VOL", "SEVERE"):
            # Sortie sous conditions vertes + hystérésis de sortie
            exit_hyst = int(getattr(self, "_exit_hyst_s", 120))
            if self._green_calme():
                if (now - getattr(self, "_last_all_green_ts", now)) >= exit_hyst:
                    self.rm_mode, self._mode_since = "NORMAL", now
            else:
                # Marqueur : dernier "pas calme" (sert pour l'hystérésis de sortie)
                self._last_all_green_ts = now

        # ---- Entrées ----
        if self.rm_mode == "NORMAL":
            if self._red_tempete():
                self.rm_mode, self._mode_since = "OPP_VOL", now
            elif self._green_calme():
                # Hystérésis d'entrée : temps écoulé depuis le dernier "pas calme"
                enter_hyst = int(getattr(self, "_enter_hyst_s", 180))
                if (now - getattr(self, "_last_all_green_ts", now)) >= enter_hyst:
                    self.rm_mode, self._mode_since = "OPP_VOLUME", now
            else:
                # Marqueur : dernier "pas calme" (reset la fenêtre d'hystérésis)
                self._last_all_green_ts = now

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

    def revalidate_arbitrage(

            self,
            *,
            buy_ex: str,
            sell_ex: str,
            pair_key: str,
            expected_net: float | None,  # ratio (ex: 0.0015 = 15 bps)
            max_drift_bps: float = 7.0,
            min_required_bps: float | None = None,
            is_rebalancing: bool = False,
            allow_final_loss_bps: float = 0.0,
            # nouveaux overrides explicites (optionnels, sans casser l’existant)
            buy_px_override: float | None = None,
            sell_px_override: float | None = None,
            # compat avec l’existant
            price_overrides: dict | None = None,
    ) -> bool:
        """
        Revalidation last-mile :
        - lit TOB (Router) avec overrides éventuels,
        - applique frais + slippage,
        - vérifie bps nets vs seuils/tolérances.
        """
        try:
            pk = (pair_key or "").replace("-", "").upper()
            bo = price_overrides or {}

            # --- 1) TOB live (Engine doit être branché au Router) ---
            b_bid, b_ask = self.get_top_of_book(buy_ex, pk)
            s_bid, s_ask = self.get_top_of_book(sell_ex, pk)

            # BUY = on paie l'ASK ; SELL = on frappe le BID
            buy_ask = float(
                buy_px_override
                if buy_px_override is not None else
                (bo.get("buy_ask") if bo else b_ask)
            )
            sell_bid = float(
                sell_px_override
                if sell_px_override is not None else
                (bo.get("sell_bid") if bo else s_bid)
            )

            # Garde-fous prix
            if buy_ask <= 0 or sell_bid <= 0 or sell_bid <= buy_ask:
                return False

            # --- 2) Frais & slippage (fallbacks tolérants) ---
            def _fee(ex: str, role: str) -> float:
                # essaie _fee_pct(ex, role) → get_fee_pct(ex, role) → variantes tolérantes
                for sig in (
                        lambda: self._fee_pct(ex, role),
                        lambda: self.get_fee_pct(ex, role),
                        lambda: self.get_fee_pct(ex, None, role),
                        lambda: self.get_fee_pct(ex, role, None),
                ):
                    try:
                        return float(sig())
                    except Exception:
                        logging.exception("Unhandled exception")
                return 0.0

            def _slip(ex: str, pair: str, side: str) -> float:
                # essaie _slip_pct(ex, pair, "buy"/"sell") → get_slippage_pct(ex, pair, "BUY"/"SELL")
                for (fn, sd) in (
                        (getattr(self, "_slip_pct", None), side.lower()),
                        (getattr(self, "get_slippage_pct", None), side.upper()),
                ):
                    if callable(fn):
                        try:
                            return float(fn(ex, pair, sd))
                        except Exception:
                            logging.exception("Unhandled exception")
                return 0.0

            fee_buy = max(0.0, _fee(buy_ex, "taker"))
            fee_sell = max(0.0, _fee(sell_ex, "taker"))
            slip_buy = max(0.0, _slip(buy_ex, pk, "buy"))
            slip_sell = max(0.0, _slip(sell_ex, pk, "sell"))

            # Net sur 1$ de base: on paie buy_ask*(1+fee+slip); on reçoit sell_bid*(1-fee-slip)
            buy_cost = buy_ask * (1.0 + fee_buy + slip_buy)
            sell_take = sell_bid * (1.0 - fee_sell - slip_sell)
            net_ratio = (sell_take - buy_cost) / buy_cost
            net_bps = 1e4 * net_ratio

            # --- 3) Seuils / tolérances ---
            base_min_bps = float(getattr(self, "base_min_bps", getattr(self.cfg, "base_min_bps", 20.0)))

            min_req = 0.0 if is_rebalancing else float(
                min_required_bps if min_required_bps is not None else base_min_bps)



            # Tolérance de perte finale (toujours appliquée)
            if net_bps < -float(allow_final_loss_bps or 0.0):
                return False

            # Cohérence avec l’attendu (on tolère “mieux que prévu”)
            if expected_net is not None:
                exp_bps = 1e4 * float(expected_net)
                if (exp_bps - net_bps) > float(max_drift_bps or 0.0):
                    return False

            return net_bps >= min_req

        except Exception:
            # durcir: on refuse proprement + compteur
            try:
                self.metrics_refusals = getattr(self, "metrics_refusals", 0) + 1
            except Exception:
                logging.exception("Unhandled exception")
            return False

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
           - Si très au-dessous du seuil: refuse sans sim.
        2) Sinon (zone grise) et si simulateur dispo: lance la simulation et tranche sur son 'status'.
        Retour: (ok, res_or_none) où res contient toujours un bloc 'analytic' avec net_bps_est, vwap_buy/vwap_sell.
        """
        try:
            pk = self._norm_pair(pair_key)
            if usdc_amt <= 0:
                return (False, None)

            # ---- Depth actuelle (sanitisée) ----
            buy_asks, _ = self._levels_for(buy_ex, pk)
            _, sell_bids = self._levels_for(sell_ex, pk)
            if not buy_asks or not sell_bids:
                return (False, None)

            # ---- Estimation analytique rapide (VWAP + fees + slip) ----
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

            # Seuils (TM=0, TT=base_min_bps)
            min_req = float(getattr(self, "base_min_bps", getattr(self.cfg, "base_min_bps", 20.0)))
            thr_bps = 0.0 if str(strategy).upper() == "TM" else min_req

            # Bande de décision "auto" pour savoir si l'on simule (zone grise)
            margin_bps = float(getattr(self.cfg, "fragment_sim_margin_bps", 5.0))

            analytic_block = {
                "method": "analytic",
                "vwap_buy": vwap_buy,
                "vwap_sell": vwap_sell,
                "net_bps": net_bps_est,
                "threshold_bps": thr_bps,
                "margin_bps": margin_bps,
            }

            # ---- Décision anticipée (sans simulation) ----
            if net_bps_est >= (thr_bps + margin_bps):
                res = {
                    "status": "rentable",
                    "decision": "analytic_confident_win",
                    **analytic_block,
                }
                return (True, res)

            if net_bps_est <= (thr_bps - margin_bps):
                res = {
                    "status": "rejete",
                    "reason": "analytic_confident_loss",
                    **analytic_block,
                }
                return (False, res)

            # ---- Zone grise -> Simulation si possible, sinon tranche sur analytique borderline ----
            sim_ok = False
            sim_res: Optional[Dict[str, Any]] = None

            if getattr(self, "simulator", None) and hasattr(self.simulator, "simulate"):
                try:
                    to = float(timeout_s if timeout_s is not None
                               else getattr(self.cfg, "simulation_timeout_s", 2.0))
                    f_buy, f_sell = self._fees_for_branch(buy_ex, sell_ex, pk, str(strategy).upper())
                    sim_res = await self.simulator.simulate(
                        opportunity={
                            "pair": pk,
                            "buy_exchange": buy_ex,
                            "sell_exchange": sell_ex,
                            "volume_possible_usdc": float(usdc_amt),
                            "fees_buy_pct": float(f_buy),
                            "fees_sell_pct": float(f_sell),
                        },
                        buy_levels_raw=buy_asks,
                        sell_levels_raw=sell_bids,
                        capital_available_usdc=float(usdc_amt),
                        strategy=strategy.upper(),
                        rebalancing_mode=False,
                        timeout=to,
                    )
                    if sim_res:
                        # enrichit avec l'analytique pour traçabilité
                        sim_res = dict(sim_res)
                        sim_res["method"] = "simulator"
                        sim_res["analytic"] = analytic_block
                        sim_ok = (str(sim_res.get("status", "")).lower() == "rentable")
                except Exception:
                    sim_ok = False
                    sim_res = None

            if sim_res is not None:
                return (sim_ok, sim_res)

            # Fallback si simulateur indispo/erreur: tranche sur l'estimation analytique borderline
            borderline_ok = (net_bps_est >= thr_bps)
            res = {
                "status": "rentable" if borderline_ok else "rejete",
                "decision": "analytic_borderline_fallback",
                **analytic_block,
            }
            return (borderline_ok, res)

        except Exception as e:
            return (False, {"status": "rejete", "reason": f"error:{e.__class__.__name__}"})

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
                break
            except Exception as exc:
                if FEESYNC_ERRORS:
                    try:
                        FEESYNC_ERRORS.inc()
                    except Exception:
                        pass
                log.debug("fee_sync_refresh_once failed", exc_info=False)
                self._mark_loop_error("fee_sync", exc)
            finally:
                await asyncio.sleep(_read_interval())

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
        snap = self.get_balance_snapshot_for_rebal(mode=mode, cached_only=cached_only)  # cf. méthode existante

        accounts = snap.get(ex, {}) or {}
        if account_alias:
            assets = accounts.get(account_alias, {}) or {}
            return float(assets.get(tok, 0.0))

        # somme cross-alias
        total = 0.0
        for assets in accounts.values():
            total += float((assets or {}).get(tok, 0.0))
        return total


    async def _apply_adjustments(self, pair: str, adjustments: Dict[str, Any]):
        try:
            if hasattr(self.simulator, "update_volatility_metrics"):
                self.simulator.update_volatility_metrics(pair, self.vol_monitor.get_current_metrics(pair))
            if hasattr(self.simulator, "update_trade_parameters"):
                self.simulator.update_trade_parameters(pair, adjustments)
        except Exception:
            logger.debug(f"[RiskManager] simulator update failed for {pair}", exc_info=False)

    async def _alert(self, module: str, message: str, pair: Optional[str] = None, alert_type: str = "INFO"):
        if not self.alert_cb:
            return
        try:
            r = self.alert_cb(module, message, pair=pair, alert_type=alert_type)
            if inspect.isawaitable(r):
                await r
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

        # Interfaccia hard del VM (già presente nel modulo): ingest_spread_bps(exchange, pair, spread_bps, ts)
        # vedi definizione in volatility_manager.py【turn23file1:L35-L37】
        self.vol_manager.ingest_spread_bps(ex, pk, vb, ts=ts_ns)

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
        Utilise la lecture d'orderbooks courants + fees/slip dynamiques du RM.
        """
        try:
            buy_ex = str(cross.get("to_exchange")).upper()
            sell_ex = str(cross.get("from_exchange")).upper()
            pair = (cross.get("pair_key") or cross.get("pair") or "").replace("-", "").upper()
            if not (buy_ex and sell_ex and pair):
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
                allow_final_loss_bps=self.rebal_allow_loss_bps
            )
            return bool(ok)
        except Exception:
            return False

    def set_rebalancing_callback(self, cb: Optional[Callable[[Dict[str, Any]], Any]]) -> None:
        self._rebalancing_cb = cb

    def is_rebalancing_active(self) -> bool:
        return time.time() < float(self._rebalancing_until)

    def get_balance_snapshot_for_rebal(self, mode: str | None = None, cached_only: bool = True):
        """Snapshot instantané (real|virtual|merged) pour la logique rebal, sans I/O réseau."""
        if hasattr(self, "_mbf_glue"):
            return self._mbf_glue.snapshot(mode=mode, cached_only=cached_only)
        return {}

    async def _handle_rebalancing_op(self, op: Dict[str, Any]) -> None:
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
        elif t == "internal_subaccount_transfer":
            await self._exec_internal_subaccount_transfer(op)
        elif t == "rebalancing_trade":
            await self._forward_rebalancing_trade(op)
        elif t in ("overlay_compensation", "crypto_topup_hint"):
            pass

    def _mark_loop_success(self, loop_name: str) -> None:
        state = self._loop_health.setdefault(loop_name, {"last_success": 0.0, "consecutive_errors": 0})
        state["last_success"] = time.time()
        state["consecutive_errors"] = 0

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
        else:
            legacy = self._legacy_convert(op)
            if legacy:
                if legacy["type"] == "rebalancing_trade":
                    await self._forward_rebalancing_trade(legacy)
                elif legacy["type"].startswith("internal_"):
                    try:
                        if legacy["type"] == "internal_wallet_transfer":
                            await self._exec_internal_wallet_transfer(legacy)
                        elif legacy["type"] == "internal_subaccount_transfer":
                            await self._exec_internal_subaccount_transfer(legacy)
                    except Exception:
                        logger.exception("[RiskManager] legacy internal transfer failed")

    def _legacy_convert(self, op: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if str(op.get("type")) == "internal_transfer":
            return {
                "type": "internal_subaccount_transfer",
                "exchange": op.get("exchange"),
                "from_alias": op.get("from_alias"),
                "to_alias": op.get("to_alias"),
                "ccy": "USDC",
                "amount": float(op.get("amount_usdc", 0.0) or 0.0),
            }
        return None

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

    async def _exec_internal_wallet_transfer(self, op: Dict[str, Any]) -> None:
        ex = str(op.get("exchange")).upper()
        alias = str(op.get("alias") or op.get("account_alias") or "TT").upper()
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
            res = fn(ex=ex, alias=alias, from_wallet=from_wallet, to_wallet=to_wallet, ccy=ccy, amount=amount)
            if inspect.isawaitable(res):
                await res
            await self._alert("RiskManager", f"✅ Internal WALLET transfer {ex}[{alias}] {from_wallet}→{to_wallet} {amount} {ccy}")
        except Exception:
            logger.exception("[RiskManager] internal wallet transfer failed")

    async def _exec_internal_subaccount_transfer(self, op: Dict[str, Any]) -> None:
        ex = str(op.get("exchange")).upper()
        from_alias = str(op.get("from_alias") or "TT").upper()
        to_alias = str(op.get("to_alias") or "TM").upper()
        ccy = str(op.get("ccy") or "USDC").upper()
        amount = float(op.get("amount") or op.get("amount_usdc") or 0.0)
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
            res = fn(ex=ex, from_alias=from_alias, to_alias=to_alias, ccy=ccy, amount=amount)
            if inspect.isawaitable(res):
                await res
            await self._alert("RiskManager", f"✅ Internal SUBACCOUNT transfer {ex} {from_alias}→{to_alias} {amount} {ccy}")
            if getattr_bool(self.cfg, "dry_run", False):
                self.adjust_virtual_balance(ex, from_alias, ccy, -amount)
                self.adjust_virtual_balance(ex, to_alias, ccy, +amount)
        except Exception:
            logger.exception("[RiskManager] internal subaccount transfer failed")

    def _make_internal_transfer(self, ex: str, from_alias: str, to_alias: str, amount_usdc: float) -> Dict[str, Any]:
        now_ms = int(time.time() * 1000)
        return {
            "type": "internal_transfer",
            "ts_ms": now_ms,
            "exchange": str(ex).upper(),
            "from_alias": from_alias,
            "to_alias": to_alias,
            "amount_usdc": float(amount_usdc),
            "meta": {"source": "RebalancingManager"},
        }

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
            amt = float(tr.get("amount_usdc") or tr.get("amount") or 0.0)
            actions.append(
                self._make_internal_transfer(tr.get("exchange"), str(tr.get("from_alias")), str(tr.get("to_alias")),
                                             amt))
        x = self._make_cross_cex_opportunity(plan)
        if x:
            actions.append(x)
        return actions

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

    def _norm_pair(s: str) -> str:
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
        if bool(getattr(self.cfg, "dry_run", False)) and self._virtual_balances:
            return {ex: {al: dict(assets) for al, assets in ad.items()} for ex, ad in self._virtual_balances.items()}
        bals = getattr(self.rebalancing, "latest_balances", {}) or {}
        return {
            str(ex).upper(): {al: dict(assets or {}) for al, assets in (ad or {}).items()}
            for ex, ad in bals.items()
        }

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
    def _available_quote(self, exchange: str, alias: str, quote: str) -> float:
        if bool(getattr(self.cfg, "dry_run", False)):
            return float(((self._virtual_balances.get(exchange.upper(), {}) or {}).get(alias, {}) or {}).get(quote, 0.0))
        bals = getattr(self.rebalancing, "latest_balances", {}) or {}
        return float(((bals.get(exchange.upper(), {}) or {}).get(alias, {}) or {}).get(quote, 0.0))

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
        cfg = getattr(self, "bot_cfg", self.cfg)

        # Fees freshness (SFC = sorgente primaria)
        if not self.slip_collector:
            raise NotReadyError("SFC not attached")
        fee_age = now - float(getattr(self.slip_collector, "last_fee_sync_ts", 0.0))
        fee_ttl_strict = float(getattr(cfg, "FEE_SNAPSHOT_TTL_S", 900.0))
        fee_ttl_tol = float(getattr(cfg, "FEE_TOLERATED_AGE_S", 3600.0))

        # Volatilità (VM)
        if not self.vol_manager:
            raise NotReadyError("VM not attached")
        vm = self.vol_manager.get_current_metrics(pair_key) or {}
        vol_age = float(vm.get("age_s", float("inf")))
        vol_ttl_strict = float(getattr(cfg, "VOL_SNAPSHOT_TTL_S", 5.0))
        vol_ttl_tol = float(getattr(cfg, "DEGRADED_VOL_TTL_S", 12.0))

        # TOB (hard strict)
        b_bid, b_ask = self.get_top_of_book(buy_ex, pair_key, max_age_s=float(getattr(cfg, "TOB_MAX_AGE_S", 1.0)))
        s_bid, s_ask = self.get_top_of_book(sell_ex, pair_key, max_age_s=float(getattr(cfg, "TOB_MAX_AGE_S", 1.0)))
        if s_bid <= b_ask:
            raise InconsistentStateError(f"No edge: {sell_ex}->{buy_ex} bid<=ask")

        # Strict verde
        if fee_age <= fee_ttl_strict and vol_age <= vol_ttl_strict:
            return None

        # Giallo (attività degradata esplicita, nessun valore inventato)
        if fee_age <= fee_ttl_tol and vol_age <= vol_ttl_tol:
            return {
                "reason": "JAUNE_FEE" if fee_age > fee_ttl_strict else "JAUNE_VOL",
                "tm_controls": {
                    "hedge_ratio": float(
                        getattr(cfg, "DEGRADED_HEDGE_RATIO", 0.75)) if vol_age > vol_ttl_strict else None,
                    "ttl_ms": int(getattr(cfg, "TM_EXPOSURE_TTL_MS", 2500)),
                    "queuepos_max_usd": int(getattr(cfg, "TM_QUEUEPOS_MAX_AHEAD_USD", 25000)),
                    "ioc_only": bool(vol_age > vol_ttl_strict),
                },
                "caps": {"size_factor": float(getattr(cfg, "DEGRADED_SIZE_FACTOR", 0.7))},
                "min_bps_lift_bps": float(
                    getattr(cfg, "DEGRADED_MIN_BPS_LIFT", 4.0)) if vol_age > vol_ttl_strict else 0.0,
                "cost_penalty_bps": float(
                    getattr(cfg, "DEGRADED_FEE_PENALTY_BPS", 3.0)) if fee_age > fee_ttl_strict else 0.0,
                "bundle_concurrency_delta": int(getattr(cfg, "DEGRADED_CONCURRENCY_DELTA", -1)),
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
        ob = self._orderbooks.get((exchange.upper(), pair_key))
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


    # --- VM integration (PASSIF) -------------------------------------------------
    def _vm_adjustments(self, pair_key: str) -> tuple[float, float, float]:
        """
        STRICT: richiede VolatilityManager; nessun fallback.
        Ritorna (boost_min_required_bps, size_factor, tm_neutral_hedge_ratio).
        - boost_min_required_bps in bps (float)
        - size_factor in [0.30, 1.00]
        """
        if self.vol_manager is None:
            raise RuntimeError("VolatilityManager non inizializzato")

        pk = self._norm_pair(pair_key)
        adj = self.vol_manager.step(pk)  # deve esistere e tornare il dict atteso
        boost_bps = float(adj.get("min_bps_boost", 0.0)) * 1e4
        size_factor = float(adj.get("size_factor", 1.0))
        hedge_ratio = float(adj.get("tm_neutral_hedge_ratio", getattr(self.cfg, "tm_exposure_ttl_hedge_ratio", 0.50)))

        # clamp industry-like
        size_factor = max(0.30, min(size_factor, 1.00))
        return boost_bps, size_factor, hedge_ratio

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
    ) -> bool:
        pk = self._norm_pair(pair_key)

        if min_required_bps is None:
            if self.dynamic_min_required and not is_rebalancing:
                min_required_bps = self._dynamic_min_required_bps(pk)
            else:
                min_required_bps = 0.0 if is_rebalancing else self.base_min_bps

        # ---- VM : prudence -> boost du seuil (en bps) -------------------------------
        try:
            vm_boost_bps, _, _ = self._vm_adjustments(pk)
            if vm_boost_bps > 0.0 and not is_rebalancing:
                min_required_bps = float(min_required_bps) + float(vm_boost_bps)
        except Exception:
            pass

        books = self._last_books or {}
        try:
            buy_book = books.get(buy_ex, {}).get(pk, {}) or {}
            sell_book = books.get(sell_ex, {}).get(pk, {}) or {}

            if not (self._fresh_enough(buy_book) and self._fresh_enough(sell_book)):
                self.penalize_pair(pk, reason="revalidate_drift")
                return False

            buy_ask = float(buy_book.get("best_ask", 0) or 0)
            sell_bid = float(sell_book.get("best_bid", 0) or 0)
            if buy_ask <= 0 or sell_bid <= 0 or sell_bid <= buy_ask:
                self.penalize_pair(pk, reason="revalidate_drift")
                return False

            mid = (buy_ask + sell_bid) / 2.0
            spread_norm = (sell_bid - buy_ask) / mid

            total_cost = self.get_total_cost_pct(buy_ex, sell_ex, pk)
            net_now = spread_norm - total_cost

            if (net_now * 1e4) < float(min_required_bps):
                if is_rebalancing and allow_final_loss_bps > 0:
                    return net_now >= -(allow_final_loss_bps / 1e4)
                self.penalize_pair(pk, reason="revalidate_drift")
                return False

            if expected_net is not None:
                drift_bps = (expected_net - net_now) * 1e4
                if drift_bps > float(max_drift_bps):
                    self.penalize_pair(pk, reason="revalidate_drift")
                    return False

            return True
        except Exception:
            return False


    # ------------------------------------------------------------------
    # API exposées à l’extérieur (alias capable)
    # ------------------------------------------------------------------
    def get_minimum_volume_required(self, exchange: str, pair_key: str, *,
                                    account_alias: Optional[str] = None) -> float:
        """
        Min notional pragmatique (en devise de cotation). Dry-run: cfg.min_usdc.
        Prod: max(cfg.min_usdc, min_notional_cex) borné par le cash disponible (alias si fourni).
        """
        try:
            base_min = getattr_float(self.cfg, "min_usdc", 1000.0)
            # si règles CEX présentes, on les prend en compte
            rule_min = float(self.compute_cex_min_notional_usdc(exchange, pair_key) or 0.0)

            if getattr_bool(self.cfg, "dry_run", False):
                return max(base_min, rule_min)

            # borne par cash disponible (quote) côté prod
            bals_full = getattr(self.rebalancing, "latest_balances", {}) or {}
            ex = str(exchange).upper()
            quote = _pair_quote(pair_key)
            if account_alias:
                cash = float(((bals_full.get(ex, {}) or {}).get(account_alias, {}) or {}).get(quote, 0.0))
            else:
                cash = sum(float((assets or {}).get(quote, 0.0)) for assets in (bals_full.get(ex, {}) or {}).values())

            safety = 0.95
            cap = max(0.0, cash * safety)
            need = max(base_min, rule_min)
            return float(min(need, cap)) if cap > 0 else need
        except Exception:
            return getattr_float(self.cfg, "min_usdc", 1000.0)

    # ---------- Fees & slippage dynamiques ----------

    def get_fees(self, exchange: str, pair_key: str) -> float:
        return self.get_fee_pct(exchange, pair_key, "taker")


    def _mm_cost_bps(self, route: dict, *, size_quote: float, prudence_key: str = "NORMAL") -> float:
        """
        Raccourci: coût MM (maker/maker) en bps, publié avec le label side="MM".
        """
        return self._total_cost_bps(route, side="MM", size_quote=size_quote, prudence_key=prudence_key)

    def get_fee_pct(self, exchange: str, pair_key: str, mode: str = "taker") -> float:
        if self.slip_collector is None or not hasattr(self.slip_collector, "get_fee_pct"):
            raise RuntimeError("SlippageAndFeesCollector non pronto (get_fee_pct)")
        return float(self.slip_collector.get_fee_pct(exchange, self._norm_pair(pair_key), mode))

    def get_slippage(self, exchange: str, pair_key: str, side: str = "buy") -> float:
        """
        Strict: usa il solo 'recent' consolidato dal Collector.
        (Interfaccia mínima già usata nel file corrente)
        """
        if self.slip_collector is None or not hasattr(self.slip_collector, "get_recent_slippage"):
            raise RuntimeError("SlippageAndFeesCollector non pronto (recent_slippage)")
        return float(self.slip_collector.get_recent_slippage(self._norm_pair(pair_key)))

    def get_total_cost_pct(self, buy_ex: str, sell_ex: str, pair_key: str) -> float:
        """
        Strict: costo % = fee_buy + fee_sell + slip_buy + slip_sell.
        Nessun fallback allo SlippageHandler.
        """
        pk = self._norm_pair(pair_key)
        fb = self.get_fee_pct(buy_ex, pk, "taker")
        fs = self.get_fee_pct(sell_ex, pk, "taker")
        sb = self.get_slippage(buy_ex, pk, "buy")
        ss = self.get_slippage(sell_ex, pk, "sell")
        return float(max(0.0, fb) + max(0.0, fs) + max(0.0, sb) + max(0.0, ss))

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
        kind = getattr_str(self.cfg, "sfc_slippage_source", "ewma").lower()
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
            dyn_bps = getattr_float(self, "base_min_bps", 20.0)

        # Convertit la ceiling fraction → bps
        ceiling_frac = getattr_float(self.cfg, "dyn_min_required_bps_ceiling_pct", 0.003)  # ex: 0.003
        ceiling_bps = max(getattr_float(self, "base_min_bps", 20.0) + ceiling_frac * 10_000.0,
                          ceiling_frac * 10_000.0)
        if dyn_bps > ceiling_bps:
            return False

        # --- slippage buy/sell avec source contrôlée par cfg
        max_slip = getattr_float(self.cfg, "fastpath_slip_bps_max", 0.003)  # fraction
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
        mbid, mask = self.get_top_of_book(maker_ex, pair_key)
        tbid, task = self.get_top_of_book(taker_ex, pair_key)
        if min(mbid, mask, tbid, task) <= 0:
            return -1e9
        if maker_side == "SELL":
            brut = (mask - task) / max(task, 1e-12)
            total_cost = self.get_total_cost_pct(
                buy_ex=taker_ex, sell_ex=maker_ex, pair_key=pair_key,
                maker_on_sell=True, maker_on_buy=False,
            )
        else:
            brut = (tbid - mbid) / max(mbid, 1e-12)
            total_cost = self.get_total_cost_pct(
                buy_ex=maker_ex, sell_ex=taker_ex, pair_key=pair_key,
                maker_on_buy=True, maker_on_sell=False,
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

    def should_tm_non_neutral(self, *, pair_key: str, maker_ex: str, taker_ex: str, usdc_amt: float) -> Tuple[bool, str, float]:
        cfg = self.cfg
        e_sell = self._tm_edge_bps(maker_side="SELL", maker_ex=maker_ex, taker_ex=taker_ex, pair_key=pair_key)
        e_buy = self._tm_edge_bps(maker_side="BUY", maker_ex=maker_ex, taker_ex=taker_ex, pair_key=pair_key)
        best_side, best_edge = (("SELL", e_sell) if e_sell >= e_buy else ("BUY", e_buy))

        try:
            vol_bps = float(self.vol_manager.get_p95_bps(pair_key))
        except Exception:
            vol_bps = 0.0
        try:
            slip_bps = float(self.slip_collector.get_recent_slippage(pair_key) * 1e4)
        except Exception:
            slip_bps = 0.0

        depth_ok = self._depth_ratio_ok(maker_ex, pair_key, usdc_amt, getattr(cfg, "tm_nn_min_depth_ratio", 1.4))

        ok = (
            best_edge >= getattr_float(cfg, "tm_nn_min_edge_bps", 3.0) and
            vol_bps <= getattr_float(cfg, "tm_nn_max_vol_bps", 60.0) and
            slip_bps <= getattr_float(cfg, "tm_nn_max_slip_bps", 25.0) and
            depth_ok
        )
        return (ok, best_side, best_edge)

    def decide_tm_mode(self, *, pair_key: str, maker_ex: str, taker_ex: str, usdc_amt: float) -> Dict[str, Any]:
        default_mode = getattr_str(self.cfg, "tm_default_mode", "NEUTRAL").upper()
        e_sell = self._tm_edge_bps(maker_side="SELL", maker_ex=maker_ex, taker_ex=taker_ex, pair_key=pair_key)
        e_buy = self._tm_edge_bps(maker_side="BUY", maker_ex=maker_ex, taker_ex=taker_ex, pair_key=pair_key)
        maker_side, edge = (("SELL", e_sell) if e_sell >= e_buy else ("BUY", e_buy))

        if default_mode != "NON_NEUTRAL":
            ok, best_side, best_edge = self.should_tm_non_neutral(
                pair_key=pair_key, maker_ex=maker_ex, taker_ex=taker_ex, usdc_amt=usdc_amt
            )
            if ok:
                return {
                    "mode": "NON_NEUTRAL",
                    "maker_side": best_side,
                    "edge_bps": best_edge,
                    "hedge_ratio": getattr_float(self.cfg, "tm_nn_hedge_ratio", 0.65),
                }
            return {
                "mode": "NEUTRAL",
                "maker_side": maker_side,
                "edge_bps": edge,
                "hedge_ratio": getattr_float(self.cfg, "tm_neutral_hedge_ratio", 0.60),
            }
        else:
            ok, best_side, best_edge = self.should_tm_non_neutral(
                pair_key=pair_key, maker_ex=maker_ex, taker_ex=taker_ex, usdc_amt=usdc_amt
            )
            if ok:
                return {
                    "mode": "NON_NEUTRAL",
                    "maker_side": best_side,
                    "edge_bps": best_edge,
                    "hedge_ratio": getattr_float(self.cfg, "tm_nn_hedge_ratio", 0.65),
                }
            return {
                "mode": "NEUTRAL",
                "maker_side": maker_side,
                "edge_bps": edge,
                "hedge_ratio": getattr_float(self.cfg, "tm_neutral_hedge_ratio", 0.60),
            }

    # ------------------------------------------------------------------
    # Admin helpers (watchdogs / discovery)
    # ------------------------------------------------------------------
    def set_allowed_routes(self, routes):
        self.allowed_routes = {(str(a).upper(), str(b).upper()) for (a, b) in (routes or [])}

    def enable_route(self, a, b):
        self.allowed_routes.add((str(a).upper(), str(b).upper()))

    def disable_route(self, a, b):
        self.allowed_routes.discard((str(a).upper(), str(b).upper()))

    def disable_exchange(self, ex):
        exu = str(ex).upper()
        self.allowed_routes = {(a, b) for (a, b) in self.allowed_routes if a != exu and b != exu}

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
        if not getattr_bool(self.cfg, "dry_run", False):
            return
        self._virtual_balances = {
            str(ex).upper(): {al: dict(assets or {}) for al, assets in (accounts or {}).items()}
            for ex, accounts in (balances or {}).items()
        }

    def adjust_virtual_balance(self, exchange: str, account_alias: str, asset: str, delta: float) -> None:
        if not getattr_bool(self.cfg, "dry_run", False):
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
        if not getattr_bool(self.cfg, "dry_run", False):
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
                        INVENTORY_USD.labels(exu, q).set(v)
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
            j_ack_min = getattr_int(self.cfg, "jitter_ack_ms_min", 8)
            j_ack_max = getattr_int(self.cfg, "jitter_ack_ms_max", 25)
            j_fill_min = getattr_int(self.cfg, "jitter_fill_ms_min", 25)
            j_fill_max = getattr_int(self.cfg, "jitter_fill_ms_max", 120)
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
        if not getattr_bool(self.cfg, "dry_run", False):
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

        for ex, pairs in (self._last_books or {}).items():
            for pk in (pairs or {}).keys():
                sb = ss = recent = None
                # 1) priorité SlippageHandler s'il est branché
                if self.slippage_handler and hasattr(self.slippage_handler, "get_slippage"):
                    try:
                        sb = float(self.slippage_handler.get_slippage(ex, pk, "buy"))
                        ss = float(self.slippage_handler.get_slippage(ex, pk, "sell"))
                    except Exception:
                        logging.exception("Unhandled exception")
                # 2) fallback collector “recent”
                if (sb is None or ss is None) and self.slip_collector:
                    try:
                        recent = float(self.slip_collector.get_recent_slippage(pk))
                    except Exception:
                        recent = None
                    if sb is None: sb = recent
                    if ss is None: ss = recent

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
    def on_scanner_opportunity(self, opp: Dict[str, Any]) -> None:
        """
        P2: scheduler multi-branches TT/TM/MM simultanés + caps notionnels et préemption MM,
            REB lock par combo, shadow non-bloquant.
        - TTL-strict slip/vol (2s/5s)
        - Caps par (branche,CEX) via _apply_caps_and_preempt (préempte MM sur TT/TM si nécessaire)
        - REB: lock combo et exécute TM neutral
        """
        now = time.time()

        # --- 0) Freshness guards (TTL strict) ------------------------------------
        slip_getter = getattr(self, "slippage_handler", None)
        vol_getter = getattr(self, "volatility_monitor", None)
        slip_age_ok = True
        vol_age_ok = True
        if slip_getter and hasattr(slip_getter, "last_age_seconds"):
            slip_age_ok = (slip_getter.last_age_seconds(opp) or 0) <= 2.0
        if vol_getter and hasattr(vol_getter, "last_age_seconds"):
            vol_age_ok = (vol_getter.last_age_seconds(opp) or 0) <= 5.0
        if not (slip_age_ok and vol_age_ok):
            if getattr(self, "log", None): self.log.debug("RM.SKIP: stale slip/vol (TTL P2)")
            return

        # --- 1) Contexte & combo --------------------------------------------------
        pair = opp.get("pair") or opp.get("symbol") or "UNKNOWN"
        buy_ex = (opp.get("buy_ex") or opp.get("route", {}).get("buy_ex") or "").upper()
        sell_ex = (opp.get("sell_ex") or opp.get("route", {}).get("sell_ex") or "").upper()
        combo_key = f"{pair}|{buy_ex}->{sell_ex}"

        if not hasattr(self, "_reb_locks"): self._reb_locks = {}

        # --- 2) REB lock ----------------------------------------------------------
        if self._reb_locks.get(combo_key, 0) > now:
            if getattr(self, "log", None): self.log.debug(f"RM.REB_LOCK active for {combo_key}")
            return

        needs_reb = False
        try:
            needs_reb = bool(getattr(self, "needs_rebalance_for_combo")(combo_key))
        except Exception:
            needs_reb = False

        if needs_reb:
            lock_ttl = getattr(self, "reb_lock_ttl_sec", 15.0)
            self._reb_locks[combo_key] = now + lock_ttl
            reb_bundle = self._build_bundle(opp, strategy="REB")
            if reb_bundle:
                self._multicast_shadow(reb_bundle)  # engine enqueue + shadow (helper)
            return

        # --- 3) Budgets d’in-flight (branche×profil) & pacer ----------------------
        profile = getattr(self, "capital_profile", "LARGE")
        pacer = getattr(self, "pacer", None)

        def _cap(branch: str) -> int:
            base_caps = {
                "NANO": {"TT": 2, "TM": 1, "MM": 1, "REB": 1},
                "MICRO": {"TT": 3, "TM": 2, "MM": 1, "REB": 2},
                "SMALL": {"TT": 4, "TM": 3, "MM": 2, "REB": 2},
                "MID": {"TT": 6, "TM": 4, "MM": 3, "REB": 3},
                "LARGE": {"TT": 8, "TM": 6, "MM": 4, "REB": 4},
            }
            cap = base_caps.get(profile, base_caps["LARGE"]).get(branch, 0)
            if pacer and hasattr(pacer, "factor_for_branch"):
                try:
                    cap = max(0, int(round(cap * pacer.factor_for_branch(branch))))
                except Exception:
                    pass
            return cap

        caps = {"TT": _cap("TT"), "TM": _cap("TM"), "MM": _cap("MM")}

        # --- 4) Éligibilités de base ----------------------------------------------
        strategies: List[str] = []
        if opp.get("tt_ok", True): strategies.append("TT")
        if opp.get("tm_ok", True): strategies.append("TM")
        if opp.get("mm_ok", True): strategies.append("MM")

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

        for strat in strategies:
            if caps.get(strat, 0) <= 0:
                continue

            # Applique cap par CEX (min des deux côtés pour TT/TM)
            allow_buy = getattr(self, "_apply_caps_and_preempt", None)
            allow_sell = getattr(self, "_apply_caps_and_preempt", None)
            allowed = desired
            if callable(allow_buy):
                if strat in ("TT", "TM"):
                    nb = allow_buy(strat, buy_ex, desired)
                    ns = allow_sell(strat, sell_ex, desired)
                    allowed = min(nb, ns, desired)
                elif strat == "MM":
                    allowed = allow_buy(strat, buy_ex or sell_ex, desired)
            # Skip si trop petit
            if allowed < max(1.0, min_trade_usdc):
                continue

            # Construire un opp “capé” pour le bundle
            opp2 = dict(opp)
            opp2["notional_usdc"] = float(allowed)
            opp2["notional_quote"] = {"ccy": "USDC", "amount": float(allowed)}

            bundle = self._build_bundle(opp2, strategy=strat)
            if not bundle:
                continue

            # Shadow + envoi (non-bloquant via helper interne)
            self._multicast_shadow(bundle)
            caps[strat] -= 1
            sent_any = True

        return

    # ---------------------------------------------------------------------
    # REMPLACEMENT 2/3
    # ---------------------------------------------------------------------
    def _build_bundle(self, opp: Dict[str, Any], strategy: str) -> Optional[Dict[str, Any]]:
        """
        P0: construit un bundle exécutable standard (dict) en s'appuyant sur payloads.make_submit_bundle
        et payloads.submit_leg_from_intent. La fragmentation est suggérée par le Simulateur (suggest_slices)
        et embarquée en meta (cohortes G1/G2/G3).
        """


        pair = opp.get("pair") or opp.get("symbol")
        buy_ex = opp.get("buy_ex") or opp.get("route", {}).get("buy_ex")
        sell_ex = opp.get("sell_ex") or opp.get("route", {}).get("sell_ex")
        pk = self._norm_pair(pair or "")

        route = {"buy_ex": buy_ex, "sell_ex": sell_ex, "pair": pair}
        profile = getattr(self, "capital_profile", "LARGE")
        tif = "IOC" if strategy in ("TT", "TM") else "GTC"
        client_id = getattr(self, "client_id", "default")
        notional = opp.get("notional_quote") or {"ccy": "USDC", "amount": float(opp.get("notional_usdc", 0) or 0)}

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
            elif strategy == "MM":
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

        # Fragmentation suggérée par le Simulateur (industry-grade)
        frag_meta = None
        simulator = getattr(self, "simulator", None)
        if simulator and hasattr(simulator, "suggest_slices"):
            try:
                plan = simulator.suggest_slices(pair=pair,
                                                notional_usdc=float(notional["amount"]),
                                                volatility_hint=getattr(self, "volatility_hint_for_pair",
                                                                        lambda p: None)(pair),
                                                min_fragment_usdc=getattr(self, "min_fragment_usdc", 200),
                                                frontload_weights=[0.50, 0.35, 0.15],
                                                frontload_group_size=3)
                frag_meta = {
                    "cohort": "G1", "idx": 0, "total": int(plan.get("count", 1)),
                    "weights": [0.50, 0.35, 0.15],
                    "plan": plan
                }
            except Exception:
                frag_meta = None

        # Contrôles TM (queuepos/TTL/hedge) en meta additifs (facultatifs)
        tm_controls = None
        if mode == "TM":
            tm_controls = {
                "queuepos_max_usd": getattr(self, "tm_queuepos_max_usd", 25000),
                "ttl_ms": getattr(self, "tm_exposure_ttl_ms", 2500),
                "hedge_ratio": getattr(self, "tm_hedge_ratio", 0.50),
            }
        # Merge overrides degraded (si présents)
        if degraded and isinstance(degraded.get("tm_controls"), dict):
            tm_controls = (tm_controls or {}) | degraded["tm_controls"]

        # SPLIT/EU_ONLY en meta additif (facultatif)
        split_meta = None
        split_mode = getattr(self, "split_mode", "EU_ONLY")  # "EU_ONLY"|"SPLIT"
        if split_mode in ("EU_ONLY", "SPLIT"):
            split_meta = {
                "mode": split_mode,
                "skew_ms": getattr(self, "split_skew_ms", 20 if split_mode == "SPLIT" else 12),
                "base_delta_ms": getattr(self, "split_base_delta_ms", 180 if split_mode == "SPLIT" else 140),
            }
        # [INSÉRER ICI — juste après 'caps_local |= degraded["caps"]' et avant 'if not legs:']
        # --- Guard pré-bundle: vol fraîche + coût total SFC (no fallback implicite)
        _pre_cost = self._prebundle_guard(
            pair=pair,
            route=route,
            side=strategy,  # "TT" | "TM" | "MM" (selon ta logique appelante)
            notional_quote=notional,
        )
        # Optionnel: exposer _pre_cost dans le contexte/trace si tu le journalises

        # Caps par défaut (identiques à ton inline actuel)
        caps_local = {
            "inflight_cap": getattr(self, "cap_hint_" + strategy.lower(), None),
            "bundle_concurrency": getattr(self, "tt_bundle_concurrency_max", 3) if strategy == "TT" else None,
            "headroom_min": getattr(self, "inflight_headroom_min", 1),
        }
        # Merge overrides degraded si fournis
        if degraded and isinstance(degraded.get("caps"), dict):
            caps_local |= degraded["caps"]

        if not legs:
            raise RMError("BUNDLE_EMPTY_LEGS")

        for i, leg in enumerate(legs):
            q = float(leg.get("qty") or 0.0)
            px = float(leg.get("px_limit") or 0.0)
            if q <= 0.0 or px <= 0.0:
                inc_rm_reject(reason="BUNDLE_EMPTY_PARAMS", pair=pair, route=f"{buy_ex}->{sell_ex}")
                raise RMError(f"BUNDLE_EMPTY_PARAMS leg={i} qty={q} px={px}")

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
        )
        return bundle

    # ---------------------------------------------------------------------
    # REMPLACEMENT 3/3
    # ---------------------------------------------------------------------
    def _multicast_shadow(self, bundle: Dict[str, Any]) -> None:
        # 1) Envoi Engine (non-bloquant, avec fallbacks gérés)
        self.engine_enqueue_bundle(bundle)
        # 2) Shadow Simu (non-bloquant, sampling interne)
        self.shadow_simulate(bundle, l2_cache=getattr(self, "l2_cache", None))

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
            if getattr(self, "log", None): self.log.warning("RM.REJECT_READY: engine not ready")
            if hasattr(self, "obs_inc"): self.obs_inc("rm_reject_total", reason="REJECT_ENGINE_NOT_READY")
            return

        # --- 2) Envoi sérialisé sous verrou -------------------------------------
        async with self._pairlocks[combo_key]:
            # Shadow non-bloquant pour enrichir les métriques (ne décide pas)
            try:
                self.shadow_simulate(order_bundle, l2_cache=getattr(self, "l2_cache", None))
            except Exception:
                pass

            submit_timeout = float(getattr(self, "engine_submit_timeout_s", 3.0))
            max_retries_net = int(getattr(self, "engine_net_retry", 1))  # une retentative réseau max
            attempt = 0
            last_exc = None

            while attempt <= max_retries_net:
                attempt += 1
                try:
                    r = None
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
                    if getattr(self, "log", None): self.log.warning(f"RM.NACK_TIMEOUT combo={combo_key}")
                    if hasattr(self, "obs_inc"): self.obs_inc("rm_reject_total", reason="NACK_TIMEOUT")
                    # P1: on ne boucle pas indéfiniment -> on tente une seule fois de plus si autorisé
                except Exception as e:
                    last_exc = e
                    msg = str(e).lower()
                    reason = "NACK_REJECT"
                    degrade = False
                    if any(x in msg for x in ("429", "too many", "rate limit")):
                        reason = "NACK_429";
                        degrade = True
                    elif any(x in msg for x in ("5xx", "internal", "temporarily")):
                        reason = "NACK_5XX";
                        degrade = True
                    if getattr(self, "log", None): self.log.exception(f"RM.{reason} combo={combo_key}")
                    if hasattr(self, "obs_inc"): self.obs_inc("rm_reject_total", reason=reason)

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
                    self.mute_route_for(buy_ex, sell_ex, pair, ttl_s=mute_s, reason="ENGINE_ERRORS")
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
        RM consomme les FILL/PARTIAL :
          - reality-check fees (BF) -> peut marquer vip_stale pour refresh,
          - reconciler : observe + resync async (ordre -> alias),
          - slippage : optionnel.
        """
        try:
            status = str((evt or {}).get("status") or (evt or {}).get("type") or "").upper()
            if status not in ("FILL", "PARTIAL"):
                return

            # reality-check fees (passif)
            try:
                bf = getattr(self, "balance_fetcher", None)
                if bf and hasattr(bf, "observe_fill_fee_reality_check"):
                    bf.observe_fill_fee_reality_check(evt)
            except Exception:
                pass

            # reconciler
            try:
                rec = getattr(self, "reconciler", None)
                if rec:
                    rec.observe_fill_event(evt)
                    import asyncio
                    ex = (evt or {}).get("exchange");
                    al = (evt or {}).get("alias");
                    oid = (evt or {}).get("order_id")
                    asyncio.create_task(rec.correlate_and_maybe_resync(ex, al, oid))
            except Exception:
                pass

            # slippage observer (si dispo)
            try:
                sh = getattr(self, "slippage_handler", None)
                if sh and hasattr(sh, "observe_slippage"):
                    sh.observe_slippage(evt)
            except Exception:
                pass

        except Exception:
            logger.exception("[RiskManager] on_private_event error")

    # risk_manager.py — dans class RiskManager
    def _split_auto_fallback_tick(self) -> None:
        """
        Auto-fallback SPLIT -> EU_ONLY si dégradation inter-région persistante,
        puis auto-restore EU_ONLY -> SPLIT après stabilisation.
        S'appuie sur les métriques publiques RM:
          - split_base_delta_ms, split_stale_ms, split_skew_ms
        et sur la config (cfg):
          - split_breach_thr_base_ms / split_breach_thr_skew_ms / split_breach_thr_stale_ms
          - split_breach_min_s (durée de dépassement avant fallback)
          - split_fallback_cooldown_s (durée minimale en EU_ONLY)
          - split_restore_stable_s (fenêtre stable pour restaurer SPLIT)
        """
        now = time.time()

        mode = str(getattr(self, "split_mode", "EU_ONLY")).upper()

        base = float(getattr(self, "split_base_delta_ms", 0.0))
        skew = float(getattr(self, "split_skew_ms", 0.0))
        stale = float(getattr(self, "split_stale_ms", 0.0))

        thr_base = float(getattr(self.cfg, "split_breach_thr_base_ms", 180.0))  # ~EU<->US ack-delta
        thr_skew = float(getattr(self.cfg, "split_breach_thr_skew_ms", 40.0))  # skew clock inter-pods
        thr_stal = float(getattr(self.cfg, "split_breach_thr_stale_ms", 1300.0))  # obsolescence books cross

        breach = (base >= thr_base) or (skew >= thr_skew) or (stale >= thr_stal)

        min_breach_s = float(getattr(self.cfg, "split_breach_min_s", 3.0))
        cd_s = float(getattr(self.cfg, "split_fallback_cooldown_s", 60.0))
        restore_s = float(getattr(self.cfg, "split_restore_stable_s", 20.0))

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
    ) -> Dict[str, Any]:
        """
        Renvoie un plan de fragments front-loaded, basé sur la profondeur actuelle.
        `total_usdc` = notional **quote** (USDC/EUR).
        """
        pk = self._norm_pair(pair_key)
        total = float(max(0.0, total_usdc))
        if total <= 0:
            return {"amounts": [], "groups": [], "avg_fragment_usdc": 0.0, "auto": True}

        # ---- VM : prudence -> réduction douce des tailles ---------------------------
        try:
            _, size_factor, _ = self._vm_adjustments(pk)
            if size_factor < 1.0:
                total = max(float(getattr(self, "min_fragment_usdc", 200.0)), total * size_factor)
        except Exception:
            pass

        asks, bids = self.get_orderbook_depth(buy_ex, pk)
        if not asks or not bids:
            return {"amounts": [total], "groups": ["G1"], "avg_fragment_usdc": total, "auto": False}

        try:
            plan = self.simulator.suggest_slices(
                budget_usdc=total, asks=asks, bids=bids,
                target_participation=float(self.target_ladder_participation),
                max_frags=int(self.max_fragments),
                min_frag_usdc=float(self.min_fragment_usdc),
                safety_pad=float(self.fragment_safety_pad),
            )
            cnt = int(max(1, plan.get("count", 1)))
            avg = float(plan.get("fragment_usdc", total)) if cnt > 0 else total
            auto = bool(plan.get("auto", True))
        except Exception:
            min_frag = float(self.min_fragment_usdc)
            max_frags = int(self.max_fragments)
            cnt = int(max(1, min(max_frags, round(total / max(min_frag, 1.0)))))
            avg = total / cnt
            auto = False

        weights = list(self.frontload_weights or [0.5, 0.35, 0.15])
        if sum(weights) <= 0:
            weights = [1.0, 0.0, 0.0]
        s = sum(weights)
        weights = [w / s for w in weights]

        amounts: List[float] = []
        groups: List[str] = []
        if cnt <= 3:
            base = max(avg, float(self.min_fragment_usdc))
            for i in range(cnt):
                left = total - sum(amounts)
                amt = base if i < (cnt - 1) else max(0.0, left)
                amounts.append(min(amt, max(0.0, left)))
                groups.append("G1")
        else:
            g1n = max(1, min(cnt, 3))
            g2n = max(0, min(cnt - g1n, 3))
            g3n = max(0, cnt - g1n - g2n)
            gns = [g1n, g2n, g3n]
            labels = ["G1", "G2", "G3"]
            for label, w, n in zip(labels, weights, gns):
                if n <= 0:
                    continue
                target = total * w
                slice_amt = max(1.0, target / n)
                for i in range(n):
                    left = total - sum(amounts)
                    amt = slice_amt if i < (n - 1) else max(0.0, left)
                    amounts.append(min(amt, max(0.0, left)))
                    groups.append(label)
            diff = total - sum(amounts)
            if abs(diff) > 1e-6:
                amounts[-1] = max(0.0, amounts[-1] + diff)

        return {
            "amounts": amounts,
            "groups": groups,
            "avg_fragment_usdc": (sum(amounts) / max(1, len(amounts))),
            "auto": auto,
        }

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
            inc_rm_reject(reason="STALE_VOL", pair=pair,
                          route=f"{route.get('buy_ex', '?')}->{route.get('sell_ex', '?')}")
            raise RMError("STALE_VOL")

        # 2) Prudence et source de slippage
        prudence = vm.get_prudence(pair).upper() if hasattr(vm, "get_prudence") else "UNKNOWN"
        slip_kind = getattr(self, "slippage_source", "ewma")

        # 3) Coût total via collecteur unique (SFC)
        sfc = getattr(self, "slip_collector", None) or getattr(self, "sfc", None)
        if not sfc or not hasattr(sfc, "get_total_cost_pct"):
            inc_rm_reject(reason="SFC_UNAVAILABLE", pair=pair,
                          route=f"{route.get('buy_ex', '?')}->{route.get('sell_ex', '?')}")
            raise RMError("SFC_UNAVAILABLE")

        cost_frac = float(sfc.get_total_cost_pct(
            route=route,
            side=side,
            size_quote=float(notional_quote or 0.0),
            slippage_kind=slip_kind,
            prudence_key=prudence,
            ts_ns=None,
            explain={"stage": "prebundle"}
        ))
        # cost_frac doit être numérique et >= 0
        if not (cost_frac >= 0.0):
            inc_rm_reject(reason="COST_COMPUTE_ERROR", pair=pair,
                          route=f"{route.get('buy_ex', '?')}->{route.get('sell_ex', '?')}")
            raise RMError("COST_COMPUTE_ERROR")
        return cost_frac

    # ------------------------------------------------------------------
    # Statut
    # ------------------------------------------------------------------
    def get_status(self) -> Dict[str, Any]:
        return {
            "module": "RiskManager",
            "healthy": self._running,
            "last_update": self.last_update,
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
                "base_min_bps": self.base_min_bps,
                "dynamic_K": self.dynamic_K,
                "min_bps_floor": self.min_bps_floor,
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
                "tm_policy": {
                    "default_mode": getattr(self.cfg, "tm_default_mode", "NEUTRAL"),
                    "neutral_hedge_ratio": getattr(self.cfg, "tm_neutral_hedge_ratio", 0.60),
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
                    "fragment_safety_pad": self.fragment_safety_pad,
                    "target_ladder_participation": self.target_ladder_participation,
                    "frontload_weights": list(self.frontload_weights or []),
                },
            },
            "submodules": {
                "VolatilityManager": getattr(self.vol_manager, "get_status", lambda: {})(),
                "SlippageAndFeesCollector": getattr(self.slip_collector, "get_status", lambda: {})(),
                "RebalancingManager": getattr(self.rebalancing, "get_status", lambda: {})(),
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
                if inspect.iscoroutinefunction(_orig):
                    async def _wrapped(self, *args, __orig=_orig, **kwargs):
                        ts = time.perf_counter_ns()
                        try:
                            return await __orig(self, *args, **kwargs)
                        finally:
                            try:
                                mark_rm_to_engine(ts)
                            except Exception:
                                logging.exception("Unhandled exception")
                else:
                    def _wrapped(self, *args, __orig=_orig, **kwargs):
                        ts = time.perf_counter_ns()
                        try:
                            return __orig(self, *args, **kwargs)
                        finally:
                            try:
                                mark_rm_to_engine(ts)
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

        # vue par défaut: prod → real ; dry-run → merged
        default_mode = "merged" if bool(getattr(cfg, "dry_run", False)) else "real"
        self.mode = str(getattr(cfg, "rm_balance_view_mode", default_mode)).lower()

        # fréquence de push vers rebal manager
        self.poll_s = float(getattr(cfg, "rm_balance_poll_s", 3.0))
        self.task = None

    # --- lectures ---
    def snapshot(self, mode: str | None = None, *, cached_only: bool = False):
        m = (mode or self.mode).lower()
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
                await t
            except Exception:
                pass
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

    cfg = getattr(rm, "cfg", None)

    quotes = list(getattr(cfg, "ccy_filter", []) or []) or ["USDC", "EUR"]
    quotes = [q.upper() for q in quotes if q]

    min_usdc = float(getattr(cfg, "min_usdc", 1000.0))
    min_map = {"USDC": min_usdc}
    if "EUR" in quotes:
        min_map["EUR"] = float(getattr(cfg, "min_eur", 0.0))

    enabled_exchanges = list(getattr(rm, "exchanges", []) or getattr(cfg, "enabled_exchanges", []) or [])
    enabled_aliases   = list(getattr(cfg, "aliases", []) or ["TT", "TM"])
    preferred_pairs   = list(getattr(cfg, "pairs", []) or ["ETHUSDC", "BTCUSDC", "ETHEUR", "BTCEUR"])

    # >>> CHANGEMENT CLÉ : on passe rm=rm <<<
    rm.rebal_mgr = RebalancingManager(
        rm=rm,
        quote_currencies=quotes,
        min_cash_per_quote=min_map,
        enabled_exchanges=[e.upper() for e in enabled_exchanges],
        enabled_aliases=[a.upper() for a in enabled_aliases],
        preferred_pairs=[p.replace("-", "").upper() for p in preferred_pairs],
        history_limit=200,
        target_diff_quote=200.0,
        internal_transfer_threshold=250.0,
        overlay_comp_threshold=100.0,
        cross_cex_haircut=0.80,
        min_crypto_value_usdc=1000.0,
        virtual_wallets=("EUR" in quotes),
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
