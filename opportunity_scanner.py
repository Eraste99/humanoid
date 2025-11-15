# -*- coding: utf-8 -*-
from __future__ import annotations
"""
OpportunityScanner — tri-CEX (Binance / Coinbase / Bybit)

Patch & review notes (v2.7)
---------------------------
- Backpressure : log + métriques + drop "oldest" (deque maxlen).
- Observabilité : compteurs des opps bloquées par pré-filtres (fast bps, slippage, min_notional, négatif TT/TM, dedup).
- Filtrage Universe dynamique : TTL par paire d'audition, avec autopause (via LHM) si aucune opp émise dans le délai.
- Heuristique TM approfondie : pondération des `tm_estimates` par profondeur multi-niveaux (N niveaux) côté jambe taker.
- VOL PATCH : ingestion de volatilité micro (bps) côté Scanner + EMA, pénalisation douce du score,
  relèvement local du min_required_bps, exposition dans hints/status.
- TM/TT adaptatif : pénalité maker réduite quand vol↑ ; seuil TT (skew ms) relâché quand vol↑.
- Priorisation mixte LHM+Scanner.
- FEES PATCH : cache + APIs d’ingestion (update_fees_metrics / ingest_fees_bulk).
- NOUVEAU (mini-patchs intégrés) :
  * Rejets unifiés (_record_rejection) : envoie Prometheus + hook historique optionnel.
  * Compteurs Scanner: emitted + histogramme de latence Router→Scanner (observe_scanner_latency).
  * Marqueur de fraîcheur ToB par exchange pour estimer une latence si besoin (fallback).
"""
# Safe import pour deque/defaultdict, même si un "collections.py" local traîne
# Safe import pour deque/defaultdict, même si un "collections.py" local traîne
try:
    from collections import deque, defaultdict
except Exception:
    try:
        # Fallback CPython: module accéléré en C
        from _collections import deque  # type: ignore
        from collections import defaultdict  # type: ignore
    except Exception:
        # Dernier filet: mini-implémentations (suffisent pour maxlen + accès manquant)
        class deque(list):  # type: ignore
            def __init__(self, *args, maxlen=None):
                self.maxlen = maxlen
                super().__init__(*args)
            def append(self, x):
                super().append(x)
                if self.maxlen and len(self) > self.maxlen:
                    del self[0:len(self)-self.maxlen]
        class _DefaultDict(dict):  # type: ignore
            def __init__(self, factory):
                super().__init__()
                self._factory = factory
            def __missing__(self, key):
                v = self._factory()
                self[key] = v
                return v
        def defaultdict(factory):  # type: ignore
            return _DefaultDict(factory)

import asyncio
import asyncio, random, time
import uuid
import logging
from decimal import Decimal, InvalidOperation, getcontext
from prometheus_client import Histogram
from modules.rm_compat import getattr_int, getattr_float, getattr_str, getattr_bool, getattr_dict, getattr_list
from contracts.errors import DataStaleError

SCANNER_EVAL_MS = Histogram("scanner_evaluate_ms", "Time to evaluate an opportunity", ["pair","route"])
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple, Set
getcontext().prec = 28

# Observabilité (tolérant : no-op si modules/observability absent)
try:
    from modules.observability import (
        bump_scanner,               # métriques historiques déjà utilisées
        inc_scanner_rejection,      # nouveau
        inc_scanner_emitted,        # nouveau
        observe_scanner_latency,    # nouveau
    )
except Exception:  # pragma: no cover
    def bump_scanner(*args, **kwargs):  # type: ignore
        return
    def inc_scanner_rejection(*args, **kwargs):  # type: ignore
        return
    def inc_scanner_emitted(*args, **kwargs):  # type: ignore
        return
    def observe_scanner_latency(*args, **kwargs):  # type: ignore
        return

logger = logging.getLogger("OpportunityScanner")


# --- PATCH A AJOUTER (tout en haut, après imports/observability) ---
try:
    # registres communs
    from modules.obs_metrics import (
        SCANNER_DECISION_MS,   # histogramme global (pair, route)
        inc_blocked,           # opportunities_blocked_total{module,reason,pair}
        mark_scanner_to_rm,    # (si wrapper plus bas présent)
    )
except Exception:  # no-op fallbacks en cas d'import partiel
    class _N:
        def labels(self, *a, **k): return self
        def observe(self, *_a, **_k): pass
    def inc_blocked(*_a, **_k): pass
    def mark_scanner_to_rm(*_a, **_k): pass
    SCANNER_DECISION_MS = _N()


# Helpers observability existants (si présents)
try:
    from modules.observability import (
        bump_scanner, inc_scanner_rejection, inc_scanner_emitted, observe_scanner_latency
    )
except Exception:
    def bump_scanner(*_a, **_k): pass
    def inc_scanner_rejection(*_a, **_k): pass
    def inc_scanner_emitted(*_a, **_k): pass
    def observe_scanner_latency(*_a, **_k): pass

# --- OBS/METRICS Fallbacks & imports manquants ---
try:
    # registres communs (si dispo dans ton projet)
    from modules.obs_metrics import (
        SCANNER_DECISION_MS,          # histogramme (optionnel)
        SC_BANNED,                    # gauge/counter
        SC_PROMOTED_PRIMARY,          # gauge/counter
        SC_ROTATION_PRIMARY_SIZE,     # gauge
        SC_ROTATION_AUDITION_SIZE,    # gauge
        SC_STRATEGY_SCORE,            # gauge
        SC_ELIGIBLE,                  # gauge
        inc_blocked,                  # function(module, reason, pair)
        mark_scanner_to_rm,           # latence Scanner→RM
    )
except Exception:
    class _MetricNoOp:
        def labels(self, *a, **k): return self
        def observe(self, *a, **k): pass
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
    SCANNER_DECISION_MS = _MetricNoOp()
    SC_BANNED = _MetricNoOp()
    SC_PROMOTED_PRIMARY = _MetricNoOp()
    SC_ROTATION_PRIMARY_SIZE = _MetricNoOp()
    SC_ROTATION_AUDITION_SIZE = _MetricNoOp()
    SC_STRATEGY_SCORE = _MetricNoOp()
    SC_ELIGIBLE = _MetricNoOp()
    def inc_blocked(*_a, **_k): pass
    def mark_scanner_to_rm(*_a, **_k): pass

# --- Canonique B6: RM_DECISION_MS{cohort} (PRIMARY|AUDITION) ---
try:
    from modules.obs_metrics import RM_DECISION_MS  # histogramme attendu
except Exception:
    class _MetricNoOp_RMDecision:
        def labels(self, *a, **k): return self
        def observe(self, *a, **k): pass
    RM_DECISION_MS = _MetricNoOp_RMDecision()


# --- Helpers quotas / pacer (token-bucket) -----------------------------------
class _TokenBucket:
    def __init__(self, rate_per_s: float, capacity: float | None = None) -> None:
        self.rate = max(0.1, float(rate_per_s))
        self.capacity = float(capacity if capacity is not None else self.rate * 2.0)
        self._tokens = self.capacity
        self._t_last = time.time()

    def _refill(self) -> None:
        now = time.time()
        dt = max(0.0, now - self._t_last)
        if dt:
            self._tokens = min(self.capacity, self._tokens + dt * self.rate)
            self._t_last = now

    def allow(self, n: float = 1.0) -> bool:
        self._refill()
        if self._tokens >= n:
            self._tokens -= n
            return True
        return False

    def load(self) -> float:
        """0.0 = idle, 1.0 = saturé (tokens ~ 0)."""
        self._refill()
        return 1.0 - min(1.0, self._tokens / max(self.capacity, 1e-9))


# --- Métriques rate-limit (no-op si obs_metrics absent) ----------------------
try:
    from modules.obs_metrics import (
        SCANNER_GLOBAL_LOAD,         # gauge 0..1
        SCANNER_RATE_LIMITED_TOTAL,  # counter(kind, cohort)
    )
except Exception:
    class _NoopMetric:
        def labels(self, *a, **k): return self
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
    SCANNER_GLOBAL_LOAD = _NoopMetric()
    SCANNER_RATE_LIMITED_TOTAL = _NoopMetric()


# -------------------- helpers --------------------
def D(x) -> Decimal:
    try:
        return x if isinstance(x, Decimal) else Decimal(str(x))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")

def _norm_ex(ex: str) -> str:
    return (ex or "").upper()

def _norm_pair(pk: str) -> str:
    return (pk or "").replace("-", "").upper()

def _route_combo(a: str, b: str) -> str:
    ex1, ex2 = _norm_ex(a), _norm_ex(b)
    return f"{min(ex1, ex2)}/{max(ex1, ex2)}"

def _quote_of_pair(pair_key: str) -> str:
    pk = _norm_pair(pair_key)
    if pk.endswith("USDC"): return "USDC"
    if pk.endswith("EUR"):  return "EUR"
    # défaut conservateur: USDC (compat historique)
    return "USDC"

# --- PATCH ROTATION (placer avant class OpportunityScanner) ---
class _Rotation:
    def __init__(self, *, primary_n: int, audition_m: int, hysteresis_min: int, ban_ttl_min: int):
        self.primary_n = int(max(1, primary_n))
        self.audition_m = int(max(0, audition_m))
        self.hyst = hysteresis_min * 60.0
        self.ban_ttl = ban_ttl_min * 60.0

        self.primary: List[str] = []
        self.audition: List[str] = []
        self._promoted_at: Dict[str, float] = {}
        self._banned_until: Dict[str, float] = {}

    def ban(self, pair: str) -> None:
        now = time.time()
        self._banned_until[pair] = now + self.ban_ttl
        if pair in self.primary:
            self.primary.remove(pair)
        if pair in self.audition:
            self.audition.remove(pair)
        # métrique (no-op si indispo)
        try:
            SC_BANNED.labels("MM").inc()
        except Exception:
            pass

    def is_banned(self, pair: str) -> bool:
        return time.time() < self._banned_until.get(pair, 0.0)

    def update(self, *, ranked_pairs: List[str], allow_promote: bool = True) -> None:
        now = time.time()
        rp = [p for p in ranked_pairs if not self.is_banned(p)]
        seen: Set[str] = set()
        rp = [p for p in rp if not (p in seen or seen.add(p))]

        target_primary = rp[: self.primary_n]
        new_primary: List[str] = []
        for p in target_primary:
            if p in self.primary:
                new_primary.append(p); continue
            if allow_promote:
                new_primary.append(p)
                self._promoted_at[p] = now
                SC_PROMOTED_PRIMARY.labels("MM").inc()

        for p in self.primary:
            if p in target_primary and p not in new_primary:
                new_primary.append(p)

        self.primary = new_primary[: self.primary_n]
        start = self.primary_n
        self.audition = rp[start : start + self.audition_m]

        SC_ROTATION_PRIMARY_SIZE.labels("MM").set(len(self.primary))
        SC_ROTATION_AUDITION_SIZE.labels("MM").set(len(self.audition))



class OpportunityScanner:


    """
    Scanner d'opportunités inter-CEX (BUY sur A, SELL sur B), neutre TT/TM.

    Entrées:
      - Mode push: update_orderbook(event) appelé par le MarketDataRouter.
      - Mode pull: get_books() fourni + tick() périodique (priorisé par LHM).

    Paramètres notables:
      - min_spread_net (fraction), min_required_bps (bps), max_time_skew_s (s)
      - scan_interval (s), on_opportunity(opp) callback optionnel
      - max_notional_usd (cap quote), allow_loss_bps_rebal (hint)

    Nouveaux paramètres (optionnels):
      - dedup_cooldown_s, backpressure, audition TTL, TM profondeur
      - VOL PATCH: intégration vol micro
      - Priorisation mixte: priority_weight_logger, priority_weight_scanner
      - FEES PATCH: ingestion/caching maker/taker fees
    """

    def __init__(self, cfg, risk_manager, market_router, history_logger=None):
        """
        Scanner piloté 100% par BotConfig.
        - Aucune lecture d'env ici (0 os.getenv)
        - Les paramètres 'router_*' vivent dans cfg.router.*
        - TTL slip/vol vient des sections dédiées (cfg.slip / cfg.vol)
        - Les seuils/rythmes/scoring peuvent être overridés dans cfg.scanner.*
        """
        from collections import deque, defaultdict
        from decimal import Decimal as D
        import logging

        # --- dépendances / config ---
        self.cfg = cfg
        self.risk_manager = risk_manager
        self.rm = risk_manager  # compat legacy
        self.router = market_router
        self.history = history_logger
        self.logger = logging.getLogger("OpportunityScanner")
        if self.router is None:
            raise RuntimeError("Scanner: market_router is required")
        if self.risk_manager is None:
            raise RuntimeError("Scanner: risk_manager is required")

        # -------------------------------
        # 1) Paramètres Router (re-logés)
        # -------------------------------
        r = self.cfg.router
        self._shards = int(r.shards_per_exchange)
        self._pair_queue_max = int(r.out_queues_maxlen)
        self._coalesce_window_ms = int(r.coalesce_window_ms)
        self._require_l2_first = bool(r.require_l2_first)

        # structures de files par paire
        self._queues = defaultdict(lambda: deque(maxlen=self._pair_queue_max))  # pair -> deque
        self._last_enqueued_ts_ms = {}  # pair -> int(ms)

        # --------------------------------
        # 2) Paramètres Scanner (centrale)
        # --------------------------------
        s = self.cfg.scanner
        # workers / windows / habilitations
        self._workers = int(getattr(s, "workers", 1))
        self._dedup_window_s = float(getattr(s, "dedup_window_s", 0.5))
        self._ttl_hints_s = float(getattr(s, "ttl_hints_s", 1.0))
        self._audition_ttl_min = int(getattr(s, "audition_ttl_min", 5))
        self._ban_ttl_min = int(getattr(s, "ban_ttl_min", 3))
        self._hysteresis_min = int(getattr(s, "hysteresis_min", 2))
        self._enable_mm_hints = bool(getattr(s, "enable_mm_hints", False))
        self._binance_depth_level = int(getattr(s, "binance_depth_level", 10))

        # seuils/rythme (override possibles via cfg.scanner.*)
        self.min_spread_net = D(getattr(s, "min_spread_net", 0.002))          # 20 bps
        self.min_required_bps = float(getattr(s, "min_required_bps", 15.0))   # bps
        self.drift_guard_bps = float(getattr(s, "drift_guard_bps", 10.0))     # bps
        self.max_pairs_per_tick = int(getattr(s, "max_pairs_per_tick", 40))
        self.max_time_skew_s = D(getattr(s, "max_time_skew_s", 0.20))         # 200 ms
        self.scan_interval = float(getattr(s, "scan_interval", 0.5))
        self.dedup_cooldown_s = float(max(0.05, getattr(s, "dedup_cooldown_s", 0.35)))
        self.backpressure_log_every = int(max(1, getattr(s, "backpressure_log_every", 100)))
        self.max_opportunities = int(max(100, getattr(s, "max_opportunities", 5000)))

        # audition / autopause
        self.audition_ttl_s = float(max(0.0, getattr(s, "audition_ttl_s", 300.0)))
        self.autopause_duration_s = float(max(0.0, getattr(s, "autopause_duration_s", 600.0)))

        # profondeur TM & pondération
        self.tm_depth_levels = int(max(1, getattr(s, "tm_depth_levels", 5)))
        self.tm_depth_weight_exponent = float(max(0.25, getattr(s, "tm_depth_weight_exponent", 1.0)))

        # -------------------------------
        # 3) TTL externes (slip/vol dédiés)
        # -------------------------------
        self._slip_ttl_s = int(self.cfg.slip.ttl_s)
        self._vol_ttl_s  = int(self.cfg.vol.ttl_s)

        # -----------------------------------------
        # 4) Filtrage périmètre / routes (globaux)
        # -----------------------------------------
        self._enabled_ex = set(x.upper() for x in self.cfg.g.enabled_exchanges)
        self._routes = {(a.upper(), b.upper()) for (a, b) in self.cfg.g.allowed_routes}

        # ------------------------------------------------------
        # 5) Sizing/exec (piloté par cfg ; fallback RM si besoin)
        # ------------------------------------------------------
        # max notionnel côté scanner (si exposé), sinon par défaut 0 (= délègue au RM)
        self.max_notional_quote = float(
            getattr(s, "max_notional_quote",
                    getattr(self.rm.cfg, "scanner_max_notional_quote",
                            getattr(self.rm.cfg, "scanner_max_notional_usd", 0.0)))
        )
        self.default_timeout_s = float(getattr(s, "default_timeout_s", 2.0))
        self.allow_loss_bps_rebal = float(getattr(s, "allow_loss_bps_rebal", 10.0))
        # agrégation slippage TT optionnelle (pilotée par RM si présent)
        self.sum_slippage_tt = bool(getattr(self.rm.cfg, "scanner_sum_slippage_tt", False))

        # ------------------------------------
        # 6) États & métriques d'exécution
        # ------------------------------------
        from collections import deque
        self.orderbooks = {}                               # type: Dict[str, Dict[str, dict]]
        self.opportunities_brutes = deque(maxlen=self.max_opportunities)
        self._last_emit = {}                                # type: Dict[tuple, float]
        self._running = False
        self._task = None                                   # type: Optional[asyncio.Task]
        self.last_scan_time = 0.0
        self.opportunity_count = 0
        self.active_pairs_count = 0
        self.scan_frequency = 0.0
        self.average_spread = D("0")
        self.net_positive_count = 0
        self._last_opp_ts_by_pair = {}                     # type: Dict[str, float]

        # Compteurs de filtrage / backpressure
        self.queue_drops = 0
        self.backpressure_events = 0
        self.blocks = {
            "fast_prefilter": 0,
            "slip": 0,
            "min_notional": 0,
            "inventory": 0,
            "neg_both": 0,
            "below_tm_hint": 0,
            "below_min_bps": 0,
            "dedup": 0,
        }

        # ------------------------------------
        # 7) Hints & univers (pilotés config)
        # ------------------------------------
        # mapping canon→natif (injecté via BotConfig si présent)
        self.pairs_map = getattr(self.rm.cfg, "pairs_map", {}) or {}

        # combos activés (mode push). Implémentation locale via configs globales
        self.enabled_combos = getattr(self, "_load_enabled_combos", lambda: [])()

        # Hints paramétrables (avec fallbacks)
        self.emit_hints = getattr_bool(s, "emit_hints", True)
        self.hint_cfg = getattr_dict(s, "hint_cfg") or {
            "tt_max_skew_ms": getattr(self.rm.cfg, "scanner_max_skew_ms", 200),
            "tm_min_depth_ratio": getattr(self.rm.cfg, "tm_nn_min_depth_ratio", 1.4),
            "tm_max_vol_bps": getattr(self.rm.cfg, "tm_nn_max_vol_bps", 60.0),
            "tm_max_slip_bps": getattr(self.rm.cfg, "tm_nn_max_slip_bps", 25.0),
            "tm_default_hedge_ratio": getattr(self.rm.cfg, "tm_neutral_hedge_ratio", 0.60),
            "tm_hint_min_est_net_bps": getattr(self.rm.cfg, "tm_hint_min_est_net_bps", 5.0),
            "tm_maker_queue_risk_bps": getattr(self.rm.cfg, "tm_maker_queue_risk_bps", 3.0),
            "prefilter_slip_bps": getattr(self.rm.cfg, "prefilter_slip_bps", 30.0),
            "tm_vol_bias_k": getattr(self.rm.cfg, "tm_vol_bias_k", 0.6),
            "tt_skew_vol_k": getattr(self.rm.cfg, "tt_skew_vol_k", 1.0),
        }

        # Univers dynamique (alimenté par DiscoveryController si utilisé)
        self._universe = None              # type: Optional[set]
        self._audition_pairs = set()
        self._audition_deadlines = {}
        # throttles rotation MM (si activée côté RM/Engine)
        self._last_rotation_ts = 0.0
        self._rotation_period_s = 2.0

        # ------------------------------------
        # 8) Feeds auxiliaires & caches
        # ------------------------------------
        self._slip_cache = {}     # {(EX,PAIR): {"bps": float|None, "ts": float}}
        self._slip_threshold_bps = {}  # par pair
        self._fees_cache = {}     # {(EX,PAIR): {"taker": float|None, "maker": float|None, "ts": float}}

        # ------------------------------------
        # 9) Volatility (patch scoring local)
        # ------------------------------------
        self._vol_bps = {}        # pair -> vol instant bps
        self._vol_ema = {}        # pair -> ema bps
        self.vol_alpha_penalty = getattr_float(s, "vol_alpha_penalty", 0.15)
        self.vol_soft_cap_bps  = getattr_float(s, "vol_soft_cap_bps", 40.0)
        self.vol_beta_min_req  = getattr_float(s, "vol_beta_min_req", 0.20)
        self.vol_chaos_bps     = getattr_float(s, "vol_chaos_bps", 120.0)
        self.vol_ema_lambda    = float(min(0.99, max(0.0, getattr(s, "vol_ema_lambda", 0.7))))

        # ------------------------------------
        # 10) Priorisation mixte (LHM + scanner)
        # ------------------------------------
        self.priority_weight_logger  = float(max(0.0, getattr(s, "priority_weight_logger", 0.7)))
        self.priority_weight_scanner = float(max(0.0, getattr(s, "priority_weight_scanner", 0.3)))
        self._internal_scores = {}  # moyenne récente des scores par paire

        # ------------------------------------
        # 11) Pacers / quotas d'évaluations
        # ------------------------------------
        # --- vitesses par cohorte et cap global (overrides possibles via cfg.scanner.*) ---
        # Rétro-compat : si CORE/SANDBOX manquent, on retombe sur PRIMARY/AUDITION.
        self.eval_hz_primary = float(getattr(s, "scanner_eval_hz_primary", 25.0))
        self.eval_hz_audition = float(getattr(s, "scanner_eval_hz_audition", 5.0))
        self.eval_hz_core = float(getattr(s, "scanner_eval_hz_core", self.eval_hz_primary))
        self.eval_hz_sandbox = float(getattr(s, "scanner_eval_hz_sandbox", self.eval_hz_audition))
        self.global_eval_hz = float(getattr(s, "scanner_global_eval_hz", 200.0))

        # token-buckets
        self._tb_global = _TokenBucket(rate_per_s=self.global_eval_hz, capacity=self.global_eval_hz * 2.0)
        self._tb_pair = {}

        # Cohortes (sets) — rétro-compat: PRIMARY/AUDITION existaient déjà
        self._core_pairs = set()
        self._primary_pairs = set()
        self._audition_pairs = set()
        self._sandbox_pairs = set()

    # --- Hooks historiques + rejets unifiés (méthodes de classe) ---
    def set_history_logger(self, sink: Callable[[dict], Any]) -> None:
        """Optionnel: connecter LoggerHistoriqueManager.opportunity (callable(dict))."""
        self._hist_logger = sink

    def _should_consider_mm_for_pair(self, pair: str) -> bool:
        """
        Gating MM P0 : actif si `enable_maker_maker` est ON et
        (paire seedée OU présente dans la rotation primaire/audition).
        """
        p = _norm_pair(pair)
        cfg = self.cfg
        if not bool(getattr(cfg, "enable_maker_maker", False)):
            return False
        if p in set(getattr(cfg, "mm_seed_pairs", [])):
            return True
        if bool(getattr(cfg, "mm_rotation_enabled", False)):
            return (p in getattr(self._rot_mm, "primary", [])) or (p in getattr(self._rot_mm, "audition", []))
        return False

    def _record_rejection(
        self,
        *,
        reason: str,
        route: str,
        pair: str,
        ctx: Optional[dict] = None,
    ) -> None:
        """Rejet centralisé: métriques + log + historique (facultatif)."""
        # compteur P0 unifié
        try:
            inc_blocked("scanner", reason, pair)
        except Exception:
            pass

        # métrique legacy si branchée
        try:
            inc_scanner_rejection(reason=reason, route=route, pair=pair)
        except Exception:
            pass

        # log contextualisé
        try:
            logger.warning(
                "[Scanner][reject] pair=%s route=%s reason=%s ctx=%s",
                pair, route, reason, ctx or {}
            )
        except Exception:
            pass

        # historique (facultatif)
        if self._hist_logger is not None:
            try:
                self._hist_logger({
                    "_hist_kind": "opportunity_rejected",
                    "ts": time.time(),
                    "reason": reason,
                    "route": route,
                    "pair": pair,
                    **(ctx or {}),
                })
            except Exception:
                logger.exception("history_logger error")

    # ---------------- Config combos -----------------
    def _load_enabled_combos(self) -> List[str]:
        cfg = getattr(self.risk_manager, "cfg", None)
        combos: List[str] = []
        if cfg and getattr(cfg, "enabled_combos", None):
            for c in getattr(cfg, "enabled_combos"):
                c2 = str(c).strip().upper()
                if "/" in c2:
                    a, b = c2.split("/", 1)
                    combos.append(_route_combo(a, b))
        else:
            # Fallback legacy
            if getattr(cfg, "combo_binance_coinbase", True):
                combos.append(_route_combo("BINANCE", "COINBASE"))
            if getattr(cfg, "combo_binance_bybit", True):
                combos.append(_route_combo("BINANCE", "BYBIT"))
            if getattr(cfg, "combo_bybit_coinbase", True):
                combos.append(_route_combo("BYBIT", "COINBASE"))
        # Si rien et qu'on est en mode pull avec `exchanges`, on génère toutes les paires d'EX.
        if not combos and self.exchanges:
            for i, a in enumerate(self.exchanges):
                for b in self.exchanges[i+1:]:
                    combos.append(_route_combo(a, b))
        return sorted(list(dict.fromkeys(combos)))

    # ---------------- Utils ----------------
    def _native_symbol(self, exchange: str, pair_key: str) -> str:
        ex_key = _norm_ex(exchange)
        pk = _norm_pair(pair_key)
        try:
            exmap = self.pairs_map.get(ex_key) or {}
            if pk in exmap:
                return exmap[pk]
        except Exception:
            logging.exception("Unhandled exception")
        if ex_key == "COINBASE":
            q = _quote_of_pair(pk)
            base = pk[:-len(q)] if q and pk.endswith(q) else pk
            return f"{base}-{q}" if q else pk
        return pk

    @staticmethod
    def _agg_depth_qty(levels: List[List[float]], *, side: str, limit_price: float, need_qty: float, max_levels: int) -> float:
        """Agrège la quantité dispo sur les `max_levels` premiers niveaux jusqu'à `limit_price`."""
        if not levels or need_qty <= 0:
            return 0.0
        qty = 0.0
        take = 0
        try:
            for price, q in levels:
                if take >= max_levels:
                    break
                take += 1
                if (side == "asks" and float(price) <= float(limit_price)) or (side == "bids" and float(price) >= float(limit_price)):
                    qty += max(0.0, float(q))
                else:
                    break
        except Exception:
            return 0.0
        return min(qty, float(need_qty) * 2.0)  # cap à 2x le besoin

    def _depth_weight(self, depth_ratio: float) -> float:
        r = max(0.0, float(depth_ratio))
        base = min(1.0, r / max(1e-9, float(self.hint_cfg.get("tm_min_depth_ratio", 1.4)) * 1.5))
        return pow(base, self.tm_depth_weight_exponent)

    # === MarketDataRouter · Phase 3 · helpers (BEGIN) ===
    def _norm_pair(self, pair: str) -> str:
        return (pair or "").replace("-", "").upper()

    # === MarketDataRouter · Phase 3 · helpers (END) ===

    def ingest_volatility_bps(self, pair: str, exchange: str, vol_bps: float, ts: float | None = None) -> None:
        self._vol_ema = getattr(self, "_vol_ema", {})
        self._vol_ts = getattr(self, "_vol_ts", {})
        key = (str(exchange).upper(), (pair or "").replace("-", "").upper())
        lam = float(getattr(self, "vol_ema_lambda", 0.7))
        prev = self._vol_ema.get(key)
        v = float(vol_bps)
        self._vol_ema[key] = (lam * prev + (1 - lam) * v) if isinstance(prev, (int, float)) else v
        self._vol_ts[key] = float(ts or time.time())

    def get_volatility_bps(self, pair: str, ema: bool = True, ttl_s: float | None = None) -> float | None:
        self._vol_ema = getattr(self, "_vol_ema", {})
        self._vol_ts = getattr(self, "_vol_ts", {})
        pk = (pair or "").replace("-", "").upper()
        # agrège sur les CEX connus (max) ; autre stratégie possible (p95, avg)
        vals = []
        now = time.time()
        ttl = float(ttl_s or getattr(self, "vol_ttl_s", 5.0))
        for (ex, p), v in list(self._vol_ema.items()):
            if p != pk:
                continue
            age = now - float(self._vol_ts.get((ex, p), 0.0))
            if age <= ttl:
                vals.append(float(v))
        return max(vals) if vals else None

    def _to_bps(self, frac_or_decimal) -> float:
        try:
            return float(frac_or_decimal) * 1e4
        except Exception:
            return 0.0

    # ------------ Scoring interne (mix avec LHM) ------------
    def _refresh_internal_scores(self, window_s: float = 180.0) -> None:
        """Calcule une moyenne récente des scores (avec pénalité vol) par paire sur `window_s`."""
        now = time.time()
        sums: Dict[str, float] = {}
        counts: Dict[str, int]  = {}
        # itère depuis la fin (plus récentes)
        for opp in reversed(self.opportunities_brutes):
            ts = float(opp.get("timestamp") or 0.0)
            if ts <= 0 or (now - ts) > window_s:
                break
            pair = _norm_pair(opp.get("pair") or "")
            sc   = float(opp.get("score") or 0.0)
            # ne garde que les scores positifs pour booster la priorisation
            if sc > 0:
                sums[pair]   = sums.get(pair, 0.0) + sc
                counts[pair] = counts.get(pair, 0) + 1
        scores: Dict[str, float] = {}
        for p, s in sums.items():
            c = max(1, counts.get(p, 1))
            scores[p] = s / c
        self._internal_scores = scores

    # ---------------- FEES PATCH: APIs publiques ----------------
    def update_fees_metrics(
        self,
        exchange: str,
        pair_key: str,
        *,
        taker: Optional[float] = None,
        maker: Optional[float] = None,
    ) -> None:
        """MAJ unitaire du cache fees (fractions, ex: 0.001 = 10 bps)."""
        ex = _norm_ex(exchange); pk = _norm_pair(pair_key)
        d = self._fees_cache.setdefault((ex, pk), {"taker": None, "maker": None, "ts": 0.0})
        if taker is not None: d["taker"] = float(max(0.0, taker))
        if maker is not None: d["maker"] = float(max(0.0, maker))
        d["ts"] = time.time()

    def ingest_fees_bulk(self, payload: Dict[str, Dict[str, Dict[str, Optional[float]]]]) -> None:
        """
        Ingestion bulk :
        payload = { "BINANCE": {"ETHUSDC": {"taker":0.001, "maker":0.0006}, ...}, ... }
        """
        for ex, pairs in (payload or {}).items():
            for pk, v in (pairs or {}).items():
                self.update_fees_metrics(ex, pk, taker=v.get("taker"), maker=v.get("maker"))

    # -- helpers de lecture (cache → fallback RM) --
    def _fee_bps(self, ex: str, role: str, pair: str) -> Optional[float]:
        """Retourne les fees en bps, priorité au cache; fallback RM (signatures tolérées)."""
        try:
            d = self._fees_cache.get((_norm_ex(ex), _norm_pair(pair)))
            if d and d.get(role) is not None:
                return float(d[role]) * 1e4
        except Exception:
            logging.exception("Unhandled exception")

        g = getattr(self.risk_manager, "get_fee_pct", None)
        if not callable(g):
            return None

        try:
            return float(g(_norm_ex(ex), _norm_pair(pair), role)) * 1e4
        except TypeError:
            logger.exception("Scanner get_fee_pct signature mismatch for %s/%s", ex, pair)
        except Exception:
            logger.exception("Scanner get_fee_pct error for %s/%s", ex, pair)
        return None

    def _fee_frac(self, ex: str, role: str, pair: str) -> float:
        """Retourne les fees en fraction (ex: 0.001), cache → fallback RM → 0.0."""
        bps = self._fee_bps(ex, role, pair)
        if isinstance(bps, (int, float)):
            return float(bps) / 1e4
        return 0.0

    # ---------------- Entrées (push) ----------------
    def update_orderbook(self, data: dict) -> None:
        # Fraîcheur ToB par exchange (utile pour estimer la latence)
        try:
            ex_for_fresh = _norm_ex(data.get("exchange"))
            if ex_for_fresh:
                self._last_seen_books_by_ex[ex_for_fresh] = time.time()
        except Exception:
            logging.exception("Unhandled exception")

        if not data or not data.get("active", False):
            return

        ex = _norm_ex(data.get("exchange"))
        pair_raw = data.get("pair_key") or data.get("symbol")
        if not pair_raw:
            return
        pair = _norm_pair(pair_raw)

        # Univers/pauses (LHM)
        if self._universe is not None and pair not in self._universe:
            return
        try:
            if hasattr(self.logger_historique_manager, "get_priority_pairs"):
                priority = self.logger_historique_manager.get_priority_pairs()
                if priority and pair not in priority:
                    return
            if hasattr(self.logger_historique_manager, "is_paused") and self.logger_historique_manager.is_paused(pair):
                return
        except Exception:
            logging.exception("Unhandled exception")

        # Stocker le snapshot puis quotas
        self.orderbooks.setdefault(ex, {})[pair] = data
        allowed, _why = self._should_scan_now(pair)
        if not allowed:
            return

        # Évalue uniquement la paire concernée
        self.check_opportunity(pair)

    # ---------------- Pull utils ----------------
    def _top_pairs(self) -> List[str]:
        """Priorisation mixte: Logger (priorité) + scoring interne en renfort; conserve la découverte via audition TTL."""
        base = set(self._universe or self.pairs or [])
        if not base:
            return []

        # 1) scoring interne récent
        self._refresh_internal_scores(window_s=180.0)
        internal = self._internal_scores
        max_int = max(internal.values(), default=0.0) or 1.0

        # 2) ranking Logger
        ranked = None
        try:
            ranked = getattr(self.logger_historique_manager, "get_ranked_pairs", lambda: None)()
        except Exception:
            ranked = None
        ranked = [_norm_pair(p) for p in (ranked or []) if _norm_pair(p) in base]

        # 3) construire score combiné
        WL = self.priority_weight_logger
        WS = self.priority_weight_scanner
        combo_scores: Dict[str, float] = {}

        rank_score: Dict[str, float] = {}
        if ranked:
            n = len(ranked)
            for i, p in enumerate(ranked):
                rank_score[p] = float(n - i) / float(n)

        for p in base:
            s_int = (internal.get(p, 0.0) / max_int) if max_int > 0 else 0.0
            s_lhm = rank_score.get(p, 0.0)
            combo_scores[p] = WL * s_lhm + WS * s_int

        ordered = sorted(combo_scores.items(), key=lambda kv: kv[1], reverse=True)
        return [p for p, _ in ordered[: self.max_pairs_per_tick]]

    def _fee_cost_fast(self, buy_ex: str, sell_ex: str, pair: str) -> float:
        """Pré-filtre rapide STRICT: (frais taker buy + sell) + max(slippage buy/sell) — pas de valeurs inventées."""
        pk = _norm_pair(pair)

        # Fees: cache -> RM (ok d'avoir 0 si non trouvées, c'est un coût)
        f_buy = self._fees_cache.get((_norm_ex(buy_ex), pk), {}).get("taker")
        f_sell = self._fees_cache.get((_norm_ex(sell_ex), pk), {}).get("taker")
        if f_buy is None:
            f_buy = float(self.risk_manager.get_fee_pct(buy_ex, pk, "taker"))
        if f_sell is None:
            f_sell = float(self.risk_manager.get_fee_pct(sell_ex, pk, "taker"))

        # Slippage: cache -> RM, sinon REJET en amont (pas de "0" de confort)
        slip_buy = self._cached_slip(buy_ex, pk, "buy")
        if slip_buy is None:
            slip_buy = self.risk_manager.get_slippage(buy_ex, pk, "buy")
            if slip_buy is None:
                # le rejet se fera au niveau appelant (évaluation opportunité)
                raise RuntimeError(f"slippage missing {buy_ex} {pk} BUY")

        slip_sell = self._cached_slip(sell_ex, pk, "sell")
        if slip_sell is None:
            slip_sell = self.risk_manager.get_slippage(sell_ex, pk, "sell")
            if slip_sell is None:
                raise RuntimeError(f"slippage missing {sell_ex} {pk} SELL")

        slip = max(float(slip_buy), float(slip_sell))
        if getattr(self, "sum_slippage_tt", False):
            slip = float(slip_buy) + float(slip_sell)

        return float((f_buy or 0.0) + (f_sell or 0.0) + slip)

    # ---------------- Feed slippage (depuis RM) ----------------
    def update_slippage_metrics(
        self,
        exchange: str,
        pair_key: str,
        *,
        buy: Optional[float] = None,
        sell: Optional[float] = None,
        recent: Optional[float] = None,
        threshold_bps: Optional[float] = None,
    ) -> None:
        ex = _norm_ex(exchange); pk = _norm_pair(pair_key)
        d = self._slip_cache.setdefault((ex, pk), {"buy": None, "sell": None, "recent": None, "ts": 0.0})
        if buy is not None:  d["buy"] = float(max(0.0, buy))
        if sell is not None: d["sell"] = float(max(0.0, sell))
        if recent is not None: d["recent"] = float(max(0.0, recent))
        d["ts"] = time.time()
        if threshold_bps is not None:
            self._slip_threshold_bps[pk] = float(max(0.0, threshold_bps))

    def ingest_slippage_bulk(
        self,
        payload: Dict[str, Dict[str, Dict[str, Optional[float]]]],
        *,
        threshold_by_pair_bps: Optional[Dict[str, float]] = None,
    ) -> None:
        for ex, pairs in (payload or {}).items():
            for pk, v in (pairs or {}).items():
                self.update_slippage_metrics(ex, pk, buy=v.get("buy"), sell=v.get("sell"), recent=v.get("recent"))
        for pk, thr in (threshold_by_pair_bps or {}).items():
            self._slip_threshold_bps[_norm_pair(pk)] = float(thr)

    def _cached_slip(self, exchange: str, pair: str, side: str) -> Optional[float]:
        d = self._slip_cache.get((_norm_ex(exchange), _norm_pair(pair))) or {}
        val = d.get(side.lower()) if side else None
        return float(val) if val is not None else None

    # ---------------- Détection (mode push) ----------------
    def _synced_snapshots_for_pair(self, pair_key: str):
        snaps: List[Tuple[str, dict, Decimal]] = []
        for ex, pairs in self.orderbooks.items():
            snap = pairs.get(pair_key)
            if not snap:
                continue
            ts_ms = snap.get("exchange_ts_ms") or snap.get("recv_ts_ms")
            try:
                ts_s = D(ts_ms) / D(1000)
            except Exception:
                ts_s = D(time.time())
            snaps.append((ex, snap, ts_s))

        n = len(snaps)
        for i in range(n):
            for j in range(i + 1, n):
                ex_i, a, t_i = snaps[i]
                ex_j, b, t_j = snaps[j]
                combo = _route_combo(ex_i, ex_j)
                if combo not in self.enabled_combos:
                    continue
                if abs(t_i - t_j) <= self.max_time_skew_s:
                    yield (ex_i, a, ex_j, b)
                    yield (ex_j, b, ex_i, a)

    def check_opportunity(self, pair_key: str) -> None:
        any_eval = False
        for ex_buy, buy_data, ex_sell, sell_data in self._synced_snapshots_for_pair(pair_key):
            any_eval = True
            self._evaluate_pair(buy_data, sell_data)
        if any_eval:
            self.last_scan_time = time.time()

    # ---------- Pilotage de l'univers ----------
    def set_universe(self, *, mode: str,
                     primary: List[str],
                     audition: List[str],
                     core: Optional[List[str]] = None,
                     sandbox: Optional[List[str]] = None) -> None:
        """
        Définit l'univers scanné et les cohortes.
        Rétro-compat:
          - Ancienne signature (primary, audition) reste valable (core/sandbox vides)
          - Si core/sandbox omis → fallback sur primary/audition uniquement
        Conserve le métier existant: TTL d'audition + re-création des buckets par paire.
        """
        import time
        core = core or []
        primary = primary or []
        audition = audition or []
        sandbox = sandbox or []

        # Normalise
        norm = lambda arr: {_norm_pair(p) for p in (arr or [])}
        self._core_pairs = norm(core)
        self._primary_pairs = norm(primary)
        self._audition_pairs = norm(audition)
        self._sandbox_pairs = norm(sandbox)

        # Univers = union (si core/sandbox vides → rétro-compat 2 tiers)
        universe = list(dict.fromkeys(core + primary + audition + sandbox)) or list(
            dict.fromkeys((primary or []) + (audition or [])))
        self.scan_only(universe)

        # TTL d'audition (conserve ton métier existant)
        now = time.time()
        base_ttl = float(getattr(self, "audition_ttl_s", 900.0))  # fallback 15 min si non présent
        self._audition_deadlines = {}
        for p in self._audition_pairs:
            self._audition_deadlines[p] = now + base_ttl

        # Buckets par cohorte (respecte 4 Hz, fallback si CORE/SANDBOX absents)
        self._tb_pair = {}
        for p in universe:
            pk = _norm_pair(p)
            if pk in self._core_pairs:
                rate = float(getattr(self, "eval_hz_core", getattr(self, "eval_hz_primary", 25.0)))
            elif pk in self._primary_pairs:
                rate = float(getattr(self, "eval_hz_primary", 25.0))
            elif pk in self._audition_pairs:
                rate = float(getattr(self, "eval_hz_audition", 5.0))
            elif pk in self._sandbox_pairs:
                rate = float(getattr(self, "eval_hz_sandbox", getattr(self, "eval_hz_audition", 5.0)))
            else:
                # conservateur (comme avant): toute paire non classée = AUDITION
                rate = float(getattr(self, "eval_hz_audition", 5.0))
            self._tb_pair[pk] = _TokenBucket(rate_per_s=rate, capacity=rate * 2.0)

        # (facultatif) métriques tailles si elles existent — sinon no-op
        try:
            SC_ROTATION_PRIMARY_SIZE.labels("MM").set(len(self._primary_pairs))
            SC_ROTATION_AUDITION_SIZE.labels("MM").set(len(self._audition_pairs))
            # Si/Quand tu ajoutes CORE/SANDBOX:
            # SC_ROTATION_CORE_SIZE.set(len(self._core_pairs))
            # SC_ROTATION_SANDBOX_SIZE.set(len(self._sandbox_pairs))
        except Exception:
            pass

    def scan_only(self, universe: List[str]) -> None:
        try:
            self._universe = { _norm_pair(p) for p in (universe or []) }
        except Exception:
            self._universe = None
        try:
            self.active_pairs_count = len(self._universe or [])
        except Exception:
            logging.exception("Unhandled exception")

    def _check_audition_ttl(self) -> None:
        if not self._audition_pairs:
            return
        now = time.time()
        ttl = self._current_audition_ttl_s()
        to_pause: List[str] = []
        for p in list(self._audition_pairs):
            deadline = float(self._audition_deadlines.get(p, 0.0) or 0.0)
            if not deadline:
                self._audition_deadlines[p] = now + ttl
                continue
            if now >= deadline:
                last_ts = float(self._last_opp_ts_by_pair.get(p, 0.0) or 0.0)
                if last_ts <= 0 or (now - last_ts) >= ttl:
                    to_pause.append(p)
                else:
                    # vue active récente → repousse la deadline
                    self._audition_deadlines[p] = now + ttl

        for p in to_pause:
            try:
                if hasattr(self.logger_historique_manager, "pause_pair") and self.autopause_duration_s > 0:
                    self.logger_historique_manager.pause_pair(p, duration=int(self.autopause_duration_s))
                    self.logger.info("[Scanner] Autopause audition %s (TTL=%ss)", p, int(ttl))
            except Exception:
                logging.exception("Unhandled exception")
            self._audition_pairs.discard(p)
            self._audition_deadlines.pop(p, None)

    # --- HELPERS MM — CANONIQUE (UNIFIÉ) ---



    def _expected_net_bps_mm(self, a_ex: str, b_ex: str, pair: str, a_price: float, b_price: float) -> float:
        """
        Approximates net bps pour MM entre prix maker a/b, avec coût de hedge conservateur.
        """
        if a_price <= 0 or b_price <= 0:
            return float("-inf")
        mid = (a_price + b_price) / 2.0
        brut = abs(a_price - b_price) / max(mid, 1e-12)
        hedge_cost_bps = float(getattr(self.cfg, "mm_hedge_cost_bps", 5.0))
        hedge_cost = hedge_cost_bps / 1e4

        return (brut - hedge_cost) * 1e4

    # --- QUOTE-AGNOSTIC HELPERS (avec alias rétro-compat) ---

    def _depth_quote_snapshot(self, ex: str, pair: str) -> float:
        """
        Somme notional QUOTE (proxy depth) sur N niveaux côté ask et bid, puis max.
        N via cfg.binance_depth_level (def 10).
        """
        N = int(getattr(self.cfg, "binance_depth_level", 10) or 10)
        d = (self.orderbooks.get(ex, {}) or {}).get(pair) or {}
        ob = d.get("orderbook") or {}
        asks = (ob.get("asks") or d.get("asks") or [])[:N]
        bids = (ob.get("bids") or d.get("bids") or [])[:N]

        def _cum(levels):
            s = 0.0
            for px, qty in levels:
                try:
                    s += float(px) * float(qty)
                except Exception:
                    pass
            return s

        return max(_cum(asks), _cum(bids))

    def _depth_p95_quote(self, ex: str, pair: str) -> float:
        """
        P95 de la fenêtre glissante des depth (QUOTE) pour (ex,pair).
        On réutilise le buffer existant pour éviter tout ajout d’état inutile.
        """
        key = (ex, pair)
        # on garde le nom du buffer existant pour ne rien casser ailleurs
        self._depth_window_usd[key].append(self._depth_quote_snapshot(ex, pair))
        arr = list(self._depth_window_usd[key])
        if not arr:
            return 0.0
        k = max(0, int(round(0.95 * (len(arr) - 1))))
        return sorted(arr)[k]

    def _queuepos_est_quote(self, ex: str, pair: str, maker_px: float, side: str) -> float:
        """
        Estime la queue ahead (en notional QUOTE) au L1 côté opposé.
        maker BUY => regarde les bids ; maker SELL => regarde les asks.
        """
        d = (self.orderbooks.get(ex, {}) or {}).get(pair) or {}
        levels = (d.get("orderbook") or {}).get("asks" if side == "SELL" else "bids") \
                 or d.get("asks" if side == "SELL" else "bids") or []
        if not levels:
            return 0.0
        try:
            px, qty = levels[0]
            return float(px) * float(qty)
        except Exception:
            return 0.0

    # --- ALIAS RÉTRO-COMPAT (ne casse aucun appel existant) ---
    _depth_usd_snapshot = _depth_quote_snapshot
    _depth_p95_usd = _depth_p95_quote
    _queuepos_est_usd = _queuepos_est_quote

    # --- SCORE MM (quote-agnostic + fallback de config) ---
    def _score_mm(
            self, *, a_ex: str, b_ex: str, pair: str, maker_px_a: float, maker_px_b: float
    ) -> tuple[float, dict]:
        """
        Score MM : combine net_bps estimé, proba de fill bilatérale (queue vs depth) et pénalité vol.
        Quote-agnostic : toutes les grandeurs de profondeur/queue sont en notional QUOTE.
        """
        cfg = self.cfg

        vol_ema = float(self._vol_ema.get(pair, 0.0))

        dA = self._depth_p95_quote(a_ex, pair)
        dB = self._depth_p95_quote(b_ex, pair)
        qposA = self._queuepos_est_quote(a_ex, pair, maker_px_a, side="SELL")
        qposB = self._queuepos_est_quote(b_ex, pair, maker_px_b, side="BUY")

        # Fallbacks de config : *_quote puis *_usd (rétro-compat)
        depth_min_quote = float(
            getattr(cfg, "mm_depth_min_quote",
                    getattr(cfg, "mm_depth_min_usd", 7500.0))
        )
        qpos_max_quote = float(
            getattr(cfg, "mm_qpos_max_ahead_quote",
                    getattr(cfg, "mm_qpos_max_ahead_usd", 5000.0))
        )
        mm_min_net_bps = float(
            getattr(cfg, "mm_min_net_bps", 0.0005)  # 5 bps par défaut (0.0005 en fraction)
        ) * 1e4

        def _expected_net_bps_mm(a_price: float, b_price: float) -> float:
            if a_price <= 0 or b_price <= 0:
                return float("-inf")
            mid = (a_price + b_price) / 2.0
            brut = abs(a_price - b_price) / max(mid, 1e-12)
            hedge_cost_bps = float(getattr(cfg, "mm_hedge_cost_bps", 5.0))
            return (brut - hedge_cost_bps / 1e4) * 1e4

        net_bps = _expected_net_bps_mm(maker_px_a, maker_px_b)

        # Éligibilité dure (vol, profondeur minimale, queue ahead maximale)
        if vol_ema > float(getattr(cfg, "mm_vol_bps_max", 6.0)):
            eligible = False
        elif dA < depth_min_quote or dB < depth_min_quote:
            eligible = False
        elif qposA > qpos_max_quote or qposB > qpos_max_quote:
            eligible = False
        else:
            eligible = True

        if not eligible:
            score = -1e9
            p_both = 0.0
        else:
            def _p(qpos, depth):
                if depth <= 0:
                    return 0.0
                return max(0.0, min(1.0, 1.0 - (qpos / (depth + 1e-9))))

            pA = _p(qposA, dA)
            pB = _p(qposB, dB)
            p_both = pA * pB

            # pondérations conservatrices + net_bps au-dessus d’un plancher
            w1, w2, w3 = 1.0, 1.0, 0.5
            score = max(0.0, net_bps - mm_min_net_bps) * w1 \
                    + (p_both - 0.5) * 100 * w2 \
                    - vol_ema * w3

        hints = {
            "maker_px": {"A": maker_px_a, "B": maker_px_b},
            "depth": {"A": dA, "B": dB},  # QUOTE
            "qpos": {"A": qposA, "B": qposB},  # QUOTE
            "vol_bps_ema": vol_ema,
            "net_bps": net_bps,
            "p_both": p_both,
            "ttl_ms": int(getattr(cfg, "mm_ttl_ms", 2200)),
        }
        return score, hints

    def _rotation_tick_mm(self, *, candidate_pairs: List[str]) -> None:
        """
        Tick de rotation MM : injection simple d'un ranking (ici l'ordre reçu).
        """
        ranked_pairs = list(candidate_pairs)
        self._rot_mm.update(ranked_pairs=ranked_pairs, allow_promote=True)

    # ---------- Quotas & charge ---------------------------------------------
    def _cohort_of(self, pair: str) -> str:
        """
        Retourne la cohorte Scanner pour une paire parmi: CORE|PRIMARY|AUDITION|SANDBOX.
        Ordre de résolution: CORE > PRIMARY > AUDITION > SANDBOX.
        """
        pk = _norm_pair(pair)
        if pk in self._core_pairs:     return "CORE"
        if pk in self._primary_pairs:  return "PRIMARY"
        if pk in self._audition_pairs: return "AUDITION"
        if pk in self._sandbox_pairs:  return "SANDBOX"
        # Rétro-compat conservatrice: si hors listes, traite comme AUDITION
        return "AUDITION"

    # À mettre dans opportunity_scanner.py (dans la classe)
    def apply_runtime_config(self, cfg) -> None:
        """
        P0/P1: Hz par tier + cap global + dedup + deques (+ workers + MM hints).
        Compatible avec:
          - SCANNER_HZ={"PRIMARY":..,"AUDITION":..,"CORE":..,"SANDBOX":..}
          - anciens scanner_eval_hz_* déjà lus dans __init__
        """
        # --- Hz par tier (fallback rétro-compat si CORE/SANDBOX absents) ---
        hz = dict(getattr(cfg, "SCANNER_HZ", {}))
        self.eval_hz_core = float(hz.get("CORE", getattr(self, "eval_hz_core", getattr(self, "eval_hz_primary", 25.0))))
        self.eval_hz_primary = float(hz.get("PRIMARY", getattr(self, "eval_hz_primary", 25.0)))
        self.eval_hz_audition = float(hz.get("AUDITION", getattr(self, "eval_hz_audition", 5.0)))
        self.eval_hz_sandbox = float(
            hz.get("SANDBOX", getattr(self, "eval_hz_sandbox", getattr(self, "eval_hz_audition", 5.0))))
        self.global_eval_hz = float(getattr(cfg, "scanner_global_eval_hz", getattr(self, "global_eval_hz", 200.0)))

        # --- token-buckets ---
        self._tb_global = _TokenBucket(rate_per_s=self.global_eval_hz, capacity=self.global_eval_hz * 2.0)
        self._tb_pair = {}

        # --- dedup & deques (déjà présents) ---
        self.dedup_cooldown_s = float(getattr(cfg, "SCANNER_DEDUP_COOLDOWN_S", getattr(self, "dedup_cooldown_s", 0.16)))
        dq = dict(getattr(cfg, "SCANNER_DEQUE_MAX", {}))
        self._deque_max_core = int(dq.get("CORE", getattr(self, "_deque_max_core", 1500)))
        self._deque_max_primary = int(dq.get("PRIMARY", getattr(self, "_deque_max_primary", 1500)))
        self._deque_max_audition = int(dq.get("AUDITION", getattr(self, "_deque_max_audition", 500)))
        self._deque_max_sandbox = int(dq.get("SANDBOX", getattr(self, "_deque_max_sandbox", 300)))

        # --- (inchangé) workers & MM hints: on les CONSERVE ---
        self._workers_target = int(getattr(cfg, "SCANNER_WORKERS", getattr(self, "_workers_target", 4)))
        self._enable_mm_hints = bool(getattr(cfg, "ENABLE_MM_HINTS", getattr(self, "_enable_mm_hints", True)))

    def _cohort_of(self, pair: str) -> str:
        pk = _norm_pair(pair)
        if pk in getattr(self, "_core_pairs", set()):     return "CORE"
        if pk in getattr(self, "_primary_pairs", set()):  return "PRIMARY"
        if pk in getattr(self, "_audition_pairs", set()): return "AUDITION"
        if pk in getattr(self, "_sandbox_pairs", set()):  return "SANDBOX"
        return "AUDITION"  # défaut sûr

    def _ensure_buckets_for_pair(self, pair: str) -> _TokenBucket:
        pk = _norm_pair(pair)
        tb = self._tb_pair.get(pk)
        if tb is not None:
            return tb
        cohort = self._cohort_of(pk)
        rate_map = {
            "CORE": float(getattr(self, "eval_hz_core", getattr(self, "eval_hz_primary", 25.0))),
            "PRIMARY": float(getattr(self, "eval_hz_primary", 25.0)),
            "AUDITION": float(getattr(self, "eval_hz_audition", 5.0)),
            "SANDBOX": float(getattr(self, "eval_hz_sandbox", getattr(self, "eval_hz_audition", 5.0))),
        }
        rate = max(0.5, float(rate_map.get(cohort, getattr(self, "eval_hz_audition", 5.0))))
        tb = _TokenBucket(rate_per_s=rate, capacity=rate * 2.0)
        self._tb_pair[pk] = tb
        return tb

    def _should_scan_now(self, pair: str) -> tuple[bool, str]:
        """
        Gate central : cap global + bucket par paire (cohorte).
        Retour (ok, raison_bloque).
        """
        # global
        if hasattr(self, "_tb_global") and not self._tb_global.allow():
            self._record_rejection("rate_limit_global", route="*", pair=pair)
            return False, "global"

        # pair bucket (4 tiers)
        cohort = self._cohort_of(pair)
        tb = self._tb_pair.get(_norm_pair(pair))
        if tb is None:
            tb = self._ensure_buckets_for_pair(pair)

        if not tb.allow():
            self._record_rejection("rate_limit_pair", route="*", pair=pair, ctx={"cohort": cohort})
            return False, "pair_bucket"

        return True, ""

    # opportunity_scanner.py (dans la classe)
    def _slip_age_seconds(self, pair: str) -> float:
        p = self._norm_pair(pair)
        ts = (getattr(self, "_slip_ts", {}) or {}).get(p) or 0.0
        return max(0.0, time.time() - float(ts))

    def _vol_age_seconds(self, pair: str) -> float:
        p = self._norm_pair(pair)
        ts = (getattr(self, "_vol_ts", {}) or {}).get(p) or 0.0
        return max(0.0, time.time() - float(ts))

    def _risk_feeds_fresh(self, pair: str) -> bool:
        """
        Vérifie la fraicheur des flux auxiliaires (slippage/volatility)
        en se basant EXCLUSIVEMENT sur cfg.slip.ttl_s et cfg.vol.ttl_s.
        """
        slip_age = self._slip_age_seconds(pair)  # suppose des compteurs internes existants
        vol_age = self._vol_age_seconds(pair)
        return (slip_age is not None and slip_age <= float(self.cfg.slip.ttl_s)) and \
            (vol_age is not None and vol_age <= float(self.cfg.vol.ttl_s))

    def _dynamic_thresholds_for(self, pair: str) -> tuple[float, float]:
        """
        (min_required_bps_boost, min_notional_mult)
        Charge élevée => on durcit d'abord AUDITION puis PRIMARY.
        """
        try:
            load = self._tb_global.load()  # 0..1
        except Exception:
            load = 0.0
        x = max(0.0, load - 0.5) / 0.5  # 0..1 quand load ∈ [0.5,1.0]

        if _norm_pair(pair) in self._primary_pairs:
            boost_bps = 3.0 * x           # PRIMARY: +0..3 bps
            min_notional_mult = 1.0 + 0.25 * x
        else:
            boost_bps = 6.0 * x           # AUDITION: +0..6 bps
            min_notional_mult = 1.0 + 0.50 * x

        return float(boost_bps), float(min_notional_mult)

    def _current_audition_ttl_s(self) -> float:
        """Réduit la TTL audition jusqu'à −50% si charge proche de 1."""
        try:
            load = self._tb_global.load()
        except Exception:
            load = 0.0
        shrink = 0.5 * min(1.0, max(0.0, (load - 0.5) / 0.4))  # 0 à load≤0.5 ; 0.5 à load≥0.9
        return max(60.0, float(self.audition_ttl_s) * (1.0 - shrink))


    # ---------------- Cœur évaluation ----------------
    def _evaluate_pair(self, buy_data: dict, sell_data: dict) -> None:
        _t0 = time.perf_counter()
        now = time.time()
        pair = (buy_data.get("pair_key") or buy_data.get("symbol") or "").replace("-", "").upper()
        buy_ex = str((buy_data.get("exchange") or "")).upper()
        sell_ex = str((sell_data.get("exchange") or "")).upper()
        route_combo = f"{min(buy_ex, sell_ex)}/{max(buy_ex, sell_ex)}"
        quote = _quote_of_pair(pair)

        buy_price = D(buy_data.get("best_ask"))
        sell_price = D(sell_data.get("best_bid"))
        buy_vol = D(buy_data.get("ask_volume") or 0)
        sell_vol = D(sell_data.get("bid_volume") or 0)
        if buy_price <= 0 or sell_price <= 0:
            return

        # Slippage pré-filtre (feed RM si dispo)
        thr_bps = float(self._slip_threshold_bps.get(pair, self.hint_cfg["prefilter_slip_bps"]))
        c_buy = self._cached_slip(buy_ex, pair, "buy")
        c_sell = self._cached_slip(sell_ex, pair, "sell")
        if c_buy is not None and (c_buy * 1e4) > thr_bps:
            self.blocks["slip"] += 1
            self._record_rejection(reason="slippage", route=route_combo, pair=pair,
                                   ctx={"side": "buy", "bps": c_buy * 1e4, "thr_bps": thr_bps})
            return
        if c_sell is not None and (c_sell * 1e4) > thr_bps:
            self.blocks["slip"] += 1
            self._record_rejection(reason="slippage", route=route_combo, pair=pair,
                                   ctx={"side": "sell", "bps": c_sell * 1e4, "thr_bps": thr_bps})
            return

        # Notionnels en quote + minNotional dynamique (charge ↑ => multiplicateur)
        vol_buy_quote = buy_price * buy_vol
        vol_sell_quote = sell_price * sell_vol
        vol_possible_quote = min(vol_buy_quote, vol_sell_quote)

        val_min = None
        try:
            val_min = self.risk_manager.get_minimum_volume_required(buy_ex, pair)
        except Exception as e:
            self._record_rejection(
                reason="min_notional_missing",
                route=route_combo,
                pair=pair,
                ctx={"err": type(e).__name__}
            )
            return
        if val_min is None or float(val_min) <= 0.0:
            self._record_rejection(
                reason="min_notional_missing",
                route=route_combo,
                pair=pair
            )
            return
        min_notional = D(val_min)

        # >>> NOUVEAU : multiplicateur minNotional sous charge (audition prioritaire)
        boost_bps_dyn, min_notional_mult_dyn = self._dynamic_thresholds_for(pair)
        if vol_possible_quote < (min_notional * D(min_notional_mult_dyn)):
            self.blocks["min_notional"] += 1
            self._record_rejection(
                reason="min_notional",
                route=route_combo,
                pair=pair,
                ctx={
                    "vol_possible_quote": float(vol_possible_quote),
                    "min_notional": float(min_notional),
                    "min_notional_mult": float(min_notional_mult_dyn),
                },
            )
            return

        spread_brut = sell_price - buy_price
        mid = (sell_price + buy_price) / D(2)
        if mid <= 0:
            return
        spread_norm = spread_brut / mid
        if spread_norm <= 0:
            return

        # Coûts dynamiques (TAKER) — cache → RM
        def _slip(ex: str, side: str) -> Optional[Decimal]:
            # 1) cache
            v = self._cached_slip(ex, pair, side)
            if v is None:
                # 2) source stricte via RM -> SFC
                v = self.risk_manager.get_slippage(ex, pair, side=side)
            if v is None:
                # Rejet explicite: pas de valeur inventée
                self.blocks["slip"] += 1
                self._record_rejection(
                    reason="slippage_missing",
                    route=route_combo,
                    pair=pair,
                    ctx={"ex": ex, "side": side},
                )
                return None
            try:
                return D(v)
            except Exception:
                self.blocks["slip"] += 1
                self._record_rejection(
                    reason="slippage_invalid",
                    route=route_combo,
                    pair=pair,
                    ctx={"ex": ex, "side": side, "raw": v},
                )
                return None

        # Fees strictes via RM (fraction, ex: 0.001); aucun 0 “de confort”
        try:
            fb = self.risk_manager.get_fee_pct(buy_ex, pair, "taker")
            fs = self.risk_manager.get_fee_pct(sell_ex, pair, "taker")
        except Exception as e:
            self._record_rejection(
                reason="fee_missing",
                route=route_combo,
                pair=pair,
                ctx={"err": type(e).__name__, "buy_ex": buy_ex, "sell_ex": sell_ex}
            )
            return
        if fb is None or fs is None:
            self._record_rejection(
                reason="fee_missing",
                route=route_combo,
                pair=pair,
                ctx={"buy_ex": buy_ex, "sell_ex": sell_ex}
            )
            return

        fees_buy_taker = D(fb)
        fees_sell_taker = D(fs)
        slip_buy = _slip(buy_ex, "buy")
        slip_sell = _slip(sell_ex, "sell")
        if slip_buy is None or slip_sell is None:
            return
        total_cost = fees_buy_taker + fees_sell_taker + slip_buy + slip_sell
        spread_net = spread_norm - total_cost

        # -------- VOL PATCH + NOUVEAU boost charge -------------------------
        vol_bps_scanner = self.get_volatility_bps(pair, ema=True)
        vol_penalty_bps = 0.0
        if isinstance(vol_bps_scanner, (int, float)):
            excess = max(0.0, float(vol_bps_scanner) - self.vol_soft_cap_bps)
            vol_penalty_bps = excess * self.vol_alpha_penalty  # bps
            local_min_required_bps = float(self.min_required_bps) + self.vol_beta_min_req * excess
        else:
            local_min_required_bps = float(self.min_required_bps)

        # >>> NOUVEAU : boost bps sous charge (audition d'abord)
        local_min_required_bps += float(boost_bps_dyn)

        # Pré-filtre VOL + charge
        if float(spread_net * D(1e4)) < local_min_required_bps:
            self.blocks["fast_prefilter"] += 1
            self._record_rejection(
                reason="below_min_bps",
                route=route_combo,
                pair=pair,
                ctx={
                    "net_bps": float(spread_net * D(1e4)),
                    "min_required_bps": float(local_min_required_bps),
                },
            )
            return

        # --- (reste de votre fonction inchangé : TM/TT hints, MM hints, sizing, dédup,
        #      payload/hints/metrics, historique, callbacks, SCANNER_EVAL_MS, etc.) ---

        # Fees en BPS (cache → fallback RM)
        buy_taker_fee_bps = self._fee_bps(buy_ex, "taker", pair)
        sell_taker_fee_bps = self._fee_bps(sell_ex, "taker", pair)
        buy_maker_fee_bps = self._fee_bps(buy_ex, "maker", pair)
        sell_maker_fee_bps = self._fee_bps(sell_ex, "maker", pair)

        if buy_taker_fee_bps is None or sell_taker_fee_bps is None:
            self._record_rejection(
                reason="fee_missing_hints",
                route=route_combo,
                pair=pair,
                ctx={"stage": "tm_estimate"}
            )
            return

        slip_buy_bps = self._to_bps(slip_buy)
        slip_sell_bps = self._to_bps(slip_sell)

        ob_buy_asks = (buy_data.get("orderbook") or {}).get("asks") or []
        ob_sell_bids = (sell_data.get("orderbook") or {}).get("bids") or []

        need_buy_qty = float(((vol_possible_quote / buy_price) if buy_price > 0 else D(0)))
        need_sell_qty = float(((vol_possible_quote / sell_price) if sell_price > 0 else D(0)))

        agg_buy_on_sell = self._agg_depth_qty(
            ob_sell_bids, side="bids", limit_price=float(sell_price),
            need_qty=need_sell_qty, max_levels=self.tm_depth_levels
        )
        agg_sell_on_buy = self._agg_depth_qty(
            ob_buy_asks, side="asks", limit_price=float(buy_price),
            need_qty=need_buy_qty, max_levels=self.tm_depth_levels
        )

        depth_ratio_sell_total = (agg_buy_on_sell / max(need_sell_qty, 1e-12)) if need_sell_qty > 0 else 0.0
        depth_ratio_buy_total = (agg_sell_on_buy / max(need_buy_qty, 1e-12)) if need_buy_qty > 0 else 0.0

        weight_sell_taker = self._depth_weight(depth_ratio_sell_total)
        weight_buy_taker = self._depth_weight(depth_ratio_buy_total)

        vol_bps_rm = 0.0
        try:
            vol_bps_rm = float(getattr(self.risk_manager, "get_volatility_bps", lambda _pair: 0.0)(pair))
        except Exception:
            logging.exception("Unhandled exception")
        vol_final = float(vol_bps_scanner if isinstance(vol_bps_scanner, (int, float)) else vol_bps_rm)
        soft = float(self.vol_soft_cap_bps)
        chaos = float(self.vol_chaos_bps)
        vol_factor = 0.0
        if vol_final > soft and chaos > soft:
            vol_factor = min(1.0, (vol_final - soft) / max(1e-9, (chaos - soft)))

        bias_k = float(self.hint_cfg.get("tm_vol_bias_k", 0.6))
        maker_queue_penalty = float(self.hint_cfg["tm_maker_queue_risk_bps"])
        maker_queue_penalty_eff = maker_queue_penalty * max(0.3, (1.0 - bias_k * vol_factor))

        tt_base = float(self.hint_cfg.get("tt_max_skew_ms", 200))
        tt_k = float(self.hint_cfg.get("tt_skew_vol_k", 1.0))
        tt_limit_ms_eff = tt_base * (1.0 + tt_k * vol_factor)

        spread_bps = float(spread_norm * D(1e4))

        tm_estimates: List[Dict[str, Any]] = []
        if buy_maker_fee_bps is not None:
            cost_bps = (buy_maker_fee_bps + sell_taker_fee_bps + slip_sell_bps + maker_queue_penalty_eff)
            est = spread_bps - cost_bps
            tm_estimates.append({
                "maker_side": "buy",
                "est_net_bps": est,
                "depth_weight": weight_sell_taker,
                "est_net_bps_weighted": est * weight_sell_taker,
                "assumptions": {
                    "maker_fee_bps": buy_maker_fee_bps,
                    "taker_fee_bps_other": sell_taker_fee_bps,
                    "taker_slip_bps_other": slip_sell_bps,
                    "maker_queue_penalty_bps": maker_queue_penalty_eff,
                    "taker_depth_ratio_total": depth_ratio_sell_total,
                }
            })
        if sell_maker_fee_bps is not None:
            cost_bps = (sell_maker_fee_bps + buy_taker_fee_bps + slip_buy_bps + maker_queue_penalty_eff)
            est = spread_bps - cost_bps
            tm_estimates.append({
                "maker_side": "sell",
                "est_net_bps": est,
                "depth_weight": weight_buy_taker,
                "est_net_bps_weighted": est * weight_buy_taker,
                "assumptions": {
                    "maker_fee_bps": sell_maker_fee_bps,
                    "taker_fee_bps_other": buy_taker_fee_bps,
                    "taker_slip_bps_other": slip_buy_bps,
                    "maker_queue_penalty_bps": maker_queue_penalty_eff,
                    "taker_depth_ratio_total": depth_ratio_buy_total,
                }
            })

        tm_best_bps = max((e.get("est_net_bps_weighted", e.get("est_net_bps", -1e9)) for e in tm_estimates),
                          default=-1e9)

        if float(spread_net) < 0.0 and tm_best_bps < 0.0:
            self.blocks["neg_both"] += 1
            self._record_rejection(reason="neg_both", route=route_combo, pair=pair,
                                   ctx={"tt_net_pct": float(spread_net * 100), "tm_best_bps": tm_best_bps})
            return

        tm_hint_min = float(self.hint_cfg["tm_hint_min_est_net_bps"])  # bps
        if float(spread_net) < float(self.min_spread_net) and tm_best_bps < tm_hint_min:
            self.blocks["below_tm_hint"] += 1
            self._record_rejection(reason="below_tm_hint", route=route_combo, pair=pair,
                                   ctx={"tt_net_pct": float(spread_net * 100), "tm_best_bps": tm_best_bps,
                                        "tm_hint_min_bps": tm_hint_min})
            return

        # Sizing (depth-aware + cap + inventaire)
        target_quote = vol_possible_quote
        if self.max_notional_quote and self.max_notional_quote > 0:
            target_quote = min(target_quote, D(self.max_notional_quote))
        val_min = None
        try:
            val_min = self.risk_manager.get_minimum_volume_required(buy_ex, pair)
        except Exception as e:
            self._record_rejection(
                reason="min_notional_missing",
                route=route_combo,
                pair=pair,
                ctx={"err": type(e).__name__}
            )
            return
        if val_min is None or float(val_min) <= 0.0:
            self._record_rejection(
                reason="min_notional_missing",
                route=route_combo,
                pair=pair
            )
            return
        min_notional = D(val_min)

        size_ok = False
        attempts = 0
        while target_quote >= min_notional:
            try:
                valid = self.risk_manager.validate_inventory_bundle(
                    buy_ex=buy_ex, sell_ex=sell_ex, pair_key=pair,
                    vol_usdc_buy=float(target_quote), vol_usdc_sell=float(target_quote),
                    is_rebalancing=False,
                )
            except Exception:
                valid = False
            if valid:
                size_ok = True
                break
            target_quote *= D("0.7")
            attempts += 1
            if attempts > 5:
                break
        if not size_ok:
            self.blocks["inventory"] += 1
            self._record_rejection(reason="inventory", route=route_combo, pair=pair,
                                   ctx={"min_notional": float(min_notional)})
            return

        # Dédup court
        k = (pair, buy_ex, sell_ex)
        if (now - self._last_emit.get(k, 0.0)) < self.dedup_cooldown_s:
            self.blocks["dedup"] += 1
            self._record_rejection(reason="dedup", route=route_combo, pair=pair,
                                   ctx={"cooldown_s": self.dedup_cooldown_s})
            return
        self._last_emit[k] = now

        opp_id = uuid.uuid4().hex
        arb_id = opp_id
        t_detect = now
        t_scan_done = time.time()
        expected_net = float(spread_net)
        is_rebal = False
        try:
            is_rebal = bool(getattr(self.risk_manager, "rebalancing", object()).is_rebalancing())
        except Exception:
            logging.exception("Unhandled exception")
        allow_loss_bps = float(self.allow_loss_bps_rebal) if is_rebal else 0.0

        leg_buy = {
            "exchange": buy_ex,
            "symbol": self._native_symbol(buy_ex, pair),
            "side": "BUY",
            "price": float(buy_price),
            "quote": quote,
            "volume_quote": float(target_quote),
            "volume_usdc": float(target_quote),
            "account_alias": None,
            "wallet_key": None,
            "meta": {"best_price": float(buy_price), "type": ("rebalancing" if is_rebal else "standard")},
        }
        leg_sell = {
            "exchange": sell_ex,
            "symbol": self._native_symbol(sell_ex, pair),
            "side": "SELL",
            "price": float(sell_price),
            "quote": quote,
            "volume_quote": float(target_quote),
            "volume_usdc": float(target_quote),
            "account_alias": None,
            "wallet_key": None,
            "meta": {"best_price": float(sell_price), "type": ("rebalancing" if is_rebal else "standard")},
        }

        payload = {
            "type": ("rebalancing" if is_rebal else "bundle"),
            "legs": [leg_buy, leg_sell],
            "pair_key": pair,
            "quote": quote,
            "expected_net_spread": expected_net,
            "timeout_s": float(self.default_timeout_s),
            "meta": {"allow_loss_bps": allow_loss_bps} if allow_loss_bps > 0 else {},
            "opp_id": opp_id,
            "arb_id": arb_id,
            "t_detect": t_detect,
            "ts_buy_ex_ms": int(buy_data.get("exchange_ts_ms") or buy_data.get("recv_ts_ms") or 0),
            "ts_sell_ex_ms": int(sell_data.get("exchange_ts_ms") or sell_data.get("recv_ts_ms") or 0),
        }

        # Hints TT/TM (inchangés)
        hints: Dict[str, Any] = {}
        if self.emit_hints:
            try:
                tsb = payload["ts_buy_ex_ms"];
                tss = payload["ts_sell_ex_ms"]
                tt_skew_ms = abs((tss or 0) - (tsb or 0))
            except Exception:
                tt_skew_ms = 9_999

            def _top_qty(levels):
                try:
                    return float(levels[0][1])
                except Exception:
                    # hint uniquement : on n’arrête pas, mais on trace en debug
                    self.logger.debug("[Scanner] hint top_qty manquant (%s %s)", pair, route_combo)
                    return 0.0

            need_buy_qty_ = float((target_quote / buy_price)) if buy_price > 0 else 0.0
            need_sell_qty_ = float((target_quote / sell_price)) if sell_price > 0 else 0.0
            ob_buy_asks_ = ob_buy_asks
            ob_sell_bids_ = ob_sell_bids
            depth_ratio_buy_L1 = (_top_qty(ob_buy_asks_) / max(need_buy_qty_, 1e-12)) if ob_buy_asks_ else 0.0
            depth_ratio_sell_L1 = (_top_qty(ob_sell_bids_) / max(need_sell_qty_, 1e-12)) if ob_sell_bids_ else 0.0
            depth_ratio_min_L1 = float(min(depth_ratio_buy_L1, depth_ratio_sell_L1))
            slip_worst_bps = max(self._to_bps(slip_buy), self._to_bps(slip_sell))

            tm_base_ok = (
                    depth_ratio_min_L1 >= float(self.hint_cfg["tm_min_depth_ratio"]) and
                    slip_worst_bps <= float(self.hint_cfg["tm_max_slip_bps"]) and
                    vol_final <= float(self.vol_chaos_bps) and
                    max(weight_buy_taker, weight_sell_taker) > 0.3
            )
            tm_hint_min = float(self.hint_cfg["tm_hint_min_est_net_bps"])  # bps
            tm_net_ok = any(
                (e.get("est_net_bps_weighted", -1e9)) >= tm_hint_min for e in tm_estimates) if tm_estimates else False

            hints = {
                "TT_score": 1.0 if tt_skew_ms <= tt_limit_ms_eff else 0.0,
                "TM_score": 1.0 if (tm_base_ok and (tm_estimates and tm_net_ok)) else 0.0,
                "fees_bps": {
                    "buy_taker": buy_taker_fee_bps,
                    "sell_taker": sell_taker_fee_bps,
                    "buy_maker": buy_maker_fee_bps,
                    "sell_maker": sell_maker_fee_bps,
                },
                "tm_estimates": tm_estimates,
                "why": {
                    "tt_skew_ms": tt_skew_ms,
                    "tt_limit_ms_eff": tt_limit_ms_eff,
                    "depth_ratio_min_L1": depth_ratio_min_L1,
                    "taker_depth_ratio_total": {
                        "on_sell": depth_ratio_sell_total,
                        "on_buy": depth_ratio_buy_total,
                    },
                    "slip_worst_bps": slip_worst_bps,
                    "vol_bps_scanner_ema": vol_bps_scanner,
                    "vol_bps_rm": vol_bps_rm,
                    "vol_factor": vol_factor,
                    "maker_queue_penalty_eff_bps": maker_queue_penalty_eff,
                    "min_required_bps_local": local_min_required_bps,
                    "hedge_ratio_hint": float(self.hint_cfg["tm_default_hedge_ratio"]),
                    "drift_guard_bps": float(self.drift_guard_bps),
                }
            }

        # Hints MM (inchangé)
        try:
            if self._should_consider_mm_for_pair(pair):
                ma = float((sell_data.get("best_ask") or 0.0))
                mb = float((buy_data.get("best_bid") or 0.0))
                score_mm, mm_hints = self._score_mm(a_ex=sell_ex, b_ex=buy_ex, pair=pair, maker_px_a=ma, maker_px_b=mb)
                SC_STRATEGY_SCORE.labels(pair, "MM").set(score_mm)
                SC_ELIGIBLE.labels(pair, "MM").set(1.0 if score_mm > -1e8 else 0.0)
                hints["MM"] = mm_hints
                hints["MM_score"] = float(score_mm)
                hints.setdefault("expected_net_bps", {})
                hints["expected_net_bps"]["TT"] = float(spread_net * 1e4)
                hints["expected_net_bps"]["TM"] = float(tm_best_bps)
                hints["expected_net_bps"]["MM"] = float(mm_hints.get("net_bps", float("nan")))
            else:
                SC_ELIGIBLE.labels(pair, "MM").set(0.0)
        except Exception:
            logging.exception("Unhandled exception")

        # Score net (pénalité VOL douce) — pondéré par la taille effectivement sélectionnée
        vol_penalty_frac = D(vol_penalty_bps / 1e4) if vol_penalty_bps > 0 else D(0)
        score_net = (spread_net - vol_penalty_frac) * D(target_quote)

        # Latence WS→Scanner (estimation robuste)
        buy_ms = int(payload["ts_buy_ex_ms"] or 0)
        sell_ms = int(payload["ts_sell_ex_ms"] or 0)
        if buy_ms and sell_ms:
            sec = max(0.0, (time.time() * 1000.0 - min(buy_ms, sell_ms)) / 1000.0)
        else:
            t_buy = float(self._last_seen_books_by_ex.get(buy_ex, 0.0) or 0.0)
            t_sell = float(self._last_seen_books_by_ex.get(sell_ex, 0.0) or 0.0)
            last_both = min(t_buy, t_sell) if (t_buy and t_sell) else 0.0
            sec = max(0.0, time.time() - last_both) if last_both > 0 else 0.0
        latency_ws_to_scan_ms = int(round(sec * 1000.0))

        opp = {
            "opp_id": opp_id,
            "arb_id": arb_id,
            "pair": pair,
            "quote": quote,
            "buy_exchange": buy_ex,
            "sell_exchange": sell_ex,
            "buy_price": float(buy_price),
            "sell_price": float(sell_price),
            "spread_brut_pct": float((spread_norm) * 100),
            "spread_net_pct": float(spread_net * 100),
            "volume_top_buy": float(buy_vol),
            "volume_top_sell": float(sell_vol),
            "volume_possible_quote": float(vol_possible_quote),
            "volume_selected_quote": float(target_quote),
            "volume_possible_usdc": float(vol_possible_quote),
            "volume_selected_usdc": float(target_quote),
            "timestamp": now,
            "t_detect": t_detect,
            "t_scan_done": t_scan_done,
            "latency_ws_to_scan_ms": latency_ws_to_scan_ms,
            "score": float(score_net.quantize(Decimal("0.0001"))),
            "type": "rebalancing" if is_rebal else "standard",
            "branch_candidates": ["TT", "TM"] + (["MM"] if self._should_consider_mm_for_pair(pair) else []),
            "hints": hints,
            "payload": payload,
            "meta": {
                "route_combo": route_combo,
                "vol_penalty_bps": vol_penalty_bps,
                "min_required_bps_local": local_min_required_bps,
            },
        }

        # Backpressure/queue
        dropped = len(self.opportunities_brutes) >= self._maxlen
        if dropped:
            self.queue_drops += 1
            self.backpressure_events += 1
            if (self.backpressure_events % self.backpressure_log_every) == 0:
                self.logger.warning("[Scanner] Backpressure: queue full (%d); dropped oldest x%d",
                                    self._maxlen, self.backpressure_events)
            try:
                bump_scanner(queue_drop=1)
            except Exception:
                logging.exception("Unhandled exception")

        self.opportunities_brutes.append(opp)
        self.opportunity_count += 1
        if float(spread_net) > 0.0:
            self.net_positive_count += 1

        self._last_opp_ts_by_pair[pair] = now
        self.average_spread = (
                                      (self.average_spread * D(self.opportunity_count - 1)) + (D(spread_net) * D(100))
                              ) / D(self.opportunity_count)

        rec = getattr(self.logger_historique_manager, "record_opportunity", None)
        if callable(rec):
            try:
                if asyncio.iscoroutinefunction(rec):
                    asyncio.create_task(rec(opp))
                else:
                    rec(opp)
            except Exception:
                logging.exception("Unhandled exception")

        self.logger.info(
            "✅ Opp %s | %s | net=%.3f%% vol=%s %s",
            opp["opp_id"], route_combo, opp["spread_net_pct"],
            f"{opp['volume_selected_quote']:.0f}", quote,
        )

        try:
            inc_scanner_emitted(route=route_combo, pair=pair)
        except Exception:
            logging.exception("Unhandled exception")
        try:
            observe_scanner_latency(route=route_combo, seconds=float(latency_ws_to_scan_ms) / 1000.0)
        except Exception:
            logging.exception("Unhandled exception")
        try:
            bump_scanner(spread_pct=float(spread_net * 100), active_pairs=self.active_pairs_count)
        except Exception:
            logging.exception("Unhandled exception")

        try:
            if self.on_opportunity:
                if asyncio.iscoroutinefunction(self.on_opportunity):
                    asyncio.create_task(self.on_opportunity(opp))
                else:
                    self.on_opportunity(opp)
        except Exception as e:
            self.logger.exception("on_opportunity callback error: %s", e)

        dt_ms = (time.perf_counter() - _t0) * 1000.0
        dt_ms = (time.perf_counter() - _t0) * 1000.0

        # Canonique: histogramme par cohorte (PRIMARY/AUDITION)
        try:
            RM_DECISION_MS.labels(cohort=self._cohort_of(pair)).observe(dt_ms)
        except Exception:
            pass

        # Aliases / compat existante (vous les gardez)
        SCANNER_DECISION_MS.labels(pair, route_combo).observe(dt_ms)
        SCANNER_EVAL_MS.labels(pair, route_combo).observe(dt_ms)

    # ---------------- Pull mode: tick ----------------
    def tick(self) -> None:
        if not callable(self.get_books):
            return
        books = self.get_books() or {}
        now = time.time()
        self.last_scan_time = now

        # Priorisation mixte (LHM + scoring interne)
        pairs_to_scan = self._top_pairs()
        exs = self.exchanges or list(books.keys())

        for pk in pairs_to_scan:
            # Quotas pair + global
            allowed, _ = self._should_scan_now(pk)
            if not allowed:
                continue

            for i, sell_ex in enumerate(exs):
                for buy_ex in exs:
                    if _route_combo(buy_ex, sell_ex) not in self.enabled_combos:
                        continue

                    b_sell = (books.get(sell_ex, {}) or {}).get(pk) or {}
                    b_buy = (books.get(buy_ex, {}) or {}).get(pk) or {}
                    bid = float(b_sell.get("best_bid") or 0.0)
                    ask = float(b_buy.get("best_ask") or 0.0)
                    if bid <= 0 or ask <= 0 or bid <= ask:
                        continue

                    # Pré-filtre rapide (fees/slip cache → fallback RM)
                    # Pré-filtre rapide (fees/slip cache → fallback RM)
                    mid = 0.5 * (bid + ask)
                    gross = (bid - ask) / max(mid, 1e-12)
                    try:
                        cost = self._fee_cost_fast(buy_ex, sell_ex, pk)
                    except (DataStaleError, RuntimeError) as e:
                        self._record_rejection(
                            reason="prefilter_cost_unavailable",
                            route=_route_combo(buy_ex, sell_ex),
                            pair=_norm_pair(pk),
                            ctx={"err": type(e).__name__}
                        )
                        continue  # on passe à la route suivante sans casser le tick
                    net_now = gross - cost

                    # Min requis dynamique (vol + charge)
                    local_min_req_bps = self.min_required_bps
                    vol_bps_scanner = self.get_volatility_bps(pk, ema=True)
                    if isinstance(vol_bps_scanner, (int, float)):
                        excess = max(0.0, float(vol_bps_scanner) - self.vol_soft_cap_bps)
                        local_min_req_bps = float(self.min_required_bps) + self.vol_beta_min_req * excess
                    boost_bps, _mult = self._dynamic_thresholds_for(pk)
                    min_req = (local_min_req_bps + boost_bps) / 1e4

                    if net_now < min_req:
                        self.blocks["fast_prefilter"] += 1
                        self._record_rejection(
                            reason="below_min_bps",
                            route=_route_combo(buy_ex, sell_ex),
                            pair=_norm_pair(pk),
                            ctx={"net_bps": float(net_now * 1e4), "min_required_bps": float(min_req * 1e4)},
                        )
                        continue

                    # Construire snapshots "compat router" puis évaluer
                    now_ms = int(time.time() * 1000)
                    buy_data = {
                        "exchange": buy_ex,
                        "pair_key": pk,
                        "active": True,
                        "best_bid": float(b_buy.get("best_bid") or 0.0),
                        "best_ask": float(b_buy.get("best_ask") or 0.0),
                        "bid_volume": float(b_buy.get("bid_volume") or 0.0),
                        "ask_volume": float(b_buy.get("ask_volume") or 0.0),
                        "orderbook": {
                            "bids": (b_buy.get("orderbook") or {}).get("bids") or b_buy.get("bids") or [],
                            "asks": (b_buy.get("orderbook") or {}).get("asks") or b_buy.get("asks") or [],
                        },
                        "exchange_ts_ms": int(b_buy.get("exchange_ts_ms") or b_buy.get("recv_ts_ms") or now_ms),
                        "recv_ts_ms": int(b_buy.get("recv_ts_ms") or b_buy.get("exchange_ts_ms") or now_ms),
                    }
                    sell_data = {
                        "exchange": sell_ex,
                        "pair_key": pk,
                        "active": True,
                        "best_bid": float(b_sell.get("best_bid") or 0.0),
                        "best_ask": float(b_sell.get("best_ask") or 0.0),
                        "bid_volume": float(b_sell.get("bid_volume") or 0.0),
                        "ask_volume": float(b_sell.get("ask_volume") or 0.0),
                        "orderbook": {
                            "bids": (b_sell.get("orderbook") or {}).get("bids") or b_sell.get("bids") or [],
                            "asks": (b_sell.get("orderbook") or {}).get("asks") or b_sell.get("asks") or [],
                        },
                        "exchange_ts_ms": int(b_sell.get("exchange_ts_ms") or b_sell.get("recv_ts_ms") or now_ms),
                        "recv_ts_ms": int(b_sell.get("recv_ts_ms") or b_sell.get("exchange_ts_ms") or now_ms),
                    }
                    self._evaluate_pair(buy_data, sell_data)

    # ---------------- Status / lifecycle ----------------
    def _is_healthy(self) -> bool:
        return (time.time() - self.last_scan_time) < 5.0

    def get_status(self) -> dict:
        # calcule les paires actives comme avant
        self.active_pairs_count = sum(
            1 for ex_data in self.orderbooks.values()
            for _, data in ex_data.items()
            if data.get("active", False)
        )

        status = {
            "module": "OpportunityScanner",
            "healthy": self._is_healthy(),
            "last_update": self.last_scan_time,
            "details": f"{self.opportunity_count} opportunités, {self.active_pairs_count} paires actives",
            "metrics": {
                "scanner_global_load": float(getattr(self._tb_global, "load", lambda: 0.0)()),
                "opportunity_count": self.opportunity_count,
                "scan_frequency": self.scan_frequency,
                "active_pairs": self.active_pairs_count,
                "average_spread_pct": float(self.average_spread.quantize(Decimal("0.0001"))),
                "net_positive_opportunities": self.net_positive_count,
                "min_required_bps": self.min_required_bps,
                "drift_guard_bps": self.drift_guard_bps,
                "dedup_cooldown_s": self.dedup_cooldown_s,
                "backpressure_queue_max": self._maxlen,
                "backpressure_drops": self.queue_drops,
                "blocks": dict(self.blocks),
            },
            "config": {
                "enabled_combos": list(self.enabled_combos),
                "audition_ttl_s": self.audition_ttl_s,
                "autopause_duration_s": self.autopause_duration_s,
                "tm_depth_levels": self.tm_depth_levels,
                "tm_depth_weight_exponent": self.tm_depth_weight_exponent,
                # VOL PATCH exposé
                "vol_alpha_penalty": self.vol_alpha_penalty,
                "vol_soft_cap_bps": self.vol_soft_cap_bps,
                "vol_beta_min_req": self.vol_beta_min_req,
                "vol_chaos_bps": self.vol_chaos_bps,
                "vol_ema_lambda": self.vol_ema_lambda,
                # Mix priorité
                "priority_weight_logger": self.priority_weight_logger,
                "priority_weight_scanner": self.priority_weight_scanner,
            },
            "submodules": {},
        }

        # ---- AJOUT 1/2: mm_rotation dans CONFIG ----
        status["config"]["mm_rotation"] = {
            "primary": list(getattr(self._rot_mm, "primary", [])),
            "audition": list(getattr(self._rot_mm, "audition", [])),
        }

        # ---- AJOUT 2/2: mm_rotation à la RACINE ----
        status["mm_rotation"] = {
            "primary": list(getattr(self._rot_mm, "primary", [])),
            "audition": list(getattr(self._rot_mm, "audition", [])),

        }

        return status

    async def start(self):
        if getattr(self, "_running", False):
            return
        self._running = True
        self._tasks = []
        workers = max(1, int(getattr(self, "_workers_target", 1)))
        self.logger.info("[Scanner] start (workers=%s)", workers)
        for i in range(workers):
            self._tasks.append(asyncio.create_task(self._scan_loop(), name=f"scanner-worker-{i + 1}"))
            await asyncio.sleep(0.02)  # jitter

    async def stop(self):
        if not getattr(self, "_running", False):
            return
        self._running = False
        for t in list(getattr(self, "_tasks", [])):
            t.cancel()
        self._tasks = []
        self.logger.info("[Scanner] stopped")

    async def restart(self):
        await self.stop()
        await self.start()

    async def _scan_loop(self):
        await asyncio.sleep(random.random() * 0.03)  # jitter
        hard_until = 0.0
        while getattr(self, "_running", False):
            try:
                gl = self._tb_global.load() if hasattr(self, "_tb_global") else 0.0
                now = time.time()

                # Hard shedding: si charge >0.95, on gèle AUDITION et on réduit PRIMARY 20% pendant 3s
                if gl > 0.95:
                    if now > hard_until:
                        self._shed_primary = max(0.5, getattr(self, "_shed_primary", 1.0) * 0.8)
                        self._shed_audition = 0.0
                        hard_until = now + 3.0
                elif hard_until and now > hard_until:
                    # retour progressif
                    self._shed_primary = 1.0
                    self._shed_audition = 1.0
                    hard_until = 0.0

                pair = self._next_candidate_pair()
                if not pair:
                    await asyncio.sleep(0.002);
                    continue

                allowed, why = self._should_scan_now(pair)
                if not allowed:
                    await asyncio.sleep(0.0008 if why == "pair_bucket" else 0.0015);
                    continue

                # appliquer shedding sur buckets
                cohort = "PRIMARY" if pair in getattr(self, "_primary_pairs", set()) else "AUDITION"
                if cohort == "AUDITION" and getattr(self, "_shed_audition", 1.0) <= 0.0:
                    continue
                if cohort == "PRIMARY" and getattr(self, "_shed_primary", 1.0) < 1.0:
                    # probabilité de passer selon shedding
                    if random.random() > getattr(self, "_shed_primary", 1.0):
                        continue

                if not self._dedup_ok(pair):
                    continue

                ev = self._pop_latest_event(pair)
                if not ev:
                    continue

                if not self._risk_feeds_fresh(pair):
                    self._record_rejection("stale_risk_feeds", route="*", pair=pair)
                    continue

                self._evaluate_routes_for_pair(pair, ev, enable_mm=self._enable_mm_hints)

            except asyncio.CancelledError:
                break
            except Exception:
                self.logger.exception("[Scanner] worker error")
                await asyncio.sleep(0.005)


# === BEGIN LATENCY INSTRUMENTATION (Scanner→RM) ===
try:
    import time, inspect
    from modules.obs_metrics import mark_scanner_to_rm
    ScannerClass = None
    for _n, _o in list(globals().items()):
        if _n in ("OpportunityScanner", "Scanner", "OpportunityScannerV2") and isinstance(_o, type):
            ScannerClass = _o
            break

    # Cherche une méthode qui appelle le RM (noms fréquents)
    for meth_name in ("_emit_to_rm", "emit_to_rm", "push_to_rm", "submit_to_rm"):
        if ScannerClass and hasattr(ScannerClass, meth_name):
            _orig = getattr(ScannerClass, meth_name)
            if inspect.iscoroutinefunction(_orig):
                async def _wrapped(self, *args, __orig=_orig, **kwargs):
                    ts = time.perf_counter_ns()
                    res = await __orig(self, *args, **kwargs)
                    try: mark_scanner_to_rm(ts)
                    except Exception:
                        logging.exception("Unhandled exception")
                    return res
            else:
                def _wrapped(self, *args, __orig=_orig, **kwargs):
                    ts = time.perf_counter_ns()
                    res = __orig(self, *args, **kwargs)
                    try: mark_scanner_to_rm(ts)
                    except Exception:
                        logging.exception("Unhandled exception")
                    return res
            setattr(ScannerClass, meth_name, _wrapped)
            break
except Exception:
    logging.exception("Unhandled exception")
# === END LATENCY INSTRUMENTATION (Scanner→RM) ===

