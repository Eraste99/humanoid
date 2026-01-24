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

import math
import asyncio, random, time
import uuid
import logging
import inspect
from decimal import Decimal, InvalidOperation, getcontext
from contracts.errors import DataStaleError
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple, Set
getcontext().prec = 28

# Observabilité (tolérant : no-op si modules/obs_metrics absent)
try:
    from modules.obs_metrics import (
        bump_scanner,
        inc_scanner_rejection,
        inc_scanner_emitted,
        inc_scanner_decision,
        observe_scanner_latency,
        note_scanner_cfg,
        record_pipeline_latency,
        set_pipeline_backlog,
        should_trace_latency,
    )
except ImportError:  # pragma: no cover
    def bump_scanner(*a, **k): return
    def inc_scanner_rejection(*a, **k): return
    def inc_scanner_emitted(*a, **k): return
    def inc_scanner_decision(*a, **k): return
    def observe_scanner_latency(*a, **k): return
    def note_scanner_cfg(*a, **k): return

logger = logging.getLogger("OpportunityScanner")


# --- PATCH A AJOUTER (tout en haut, après imports/observability) ---
# --- OBS/METRICS Fallbacks & imports manquants ---
try:
    # registres communs (si dispo dans ton projet)
    from modules.obs_metrics import (
        SCANNER_DECISION_MS,  # histogramme (optionnel)
        SCANNER_EVAL_MS,
        SC_BANNED,  # gauge/counter
        SC_PROMOTED_PRIMARY,  # gauge/counter
        SC_ROTATION_PRIMARY_SIZE,  # gauge

        SC_ROTATION_AUDITION_SIZE,    # gauge
        SC_STRATEGY_SCORE,            # gauge
        SC_ELIGIBLE,                  # gauge
        SIM_PRIME_TOTAL,
        SIM_PRIME_ERROR_TOTAL,
        inc_blocked,                  # function(module, reason, pair)
        mark_scanner_to_rm_ts,        # latence Scanner→RM (wrapper ts_ns)
    )
except Exception:
    class _MetricNoOp:
        def labels(self, *a, **k): return self
        def observe(self, *a, **k): pass
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass

    SCANNER_DECISION_MS = _MetricNoOp()
    SCANNER_EVAL_MS = _MetricNoOp()

    SC_BANNED = _MetricNoOp()
    SC_PROMOTED_PRIMARY = _MetricNoOp()
    SC_ROTATION_PRIMARY_SIZE = _MetricNoOp()

    SC_ROTATION_AUDITION_SIZE = _MetricNoOp()
    SC_STRATEGY_SCORE = _MetricNoOp()
    SC_ELIGIBLE = _MetricNoOp()
    SIM_PRIME_TOTAL = _MetricNoOp()
    SIM_PRIME_ERROR_TOTAL = _MetricNoOp()

    def inc_blocked(*_a, **_k): pass

    def mark_scanner_to_rm_ts(*_a, **_k): pass

# --- Canonique B6: RM_DECISION_MS{cohort} (PRIMARY|AUDITION) ---
try:
    from modules.obs_metrics import RM_DECISION_MS  # histogramme attendu
except Exception:
    class _MetricNoOp_RMDecision:
        def labels(self, *a, **k): return self
        def observe(self, *a, **k): pass
    RM_DECISION_MS = _MetricNoOp_RMDecision()


# --- Helpers quotas / pacer (token-bucket) -----------------------------------
from modules.utils.rate_limiter import TokenBucket
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
    if pk.endswith("USDT"): return "USDT"
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
        self._state_change_cb = None

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
        cb = getattr(self, "_state_change_cb", None)
        if callable(cb):
            try:
                cb(pair, "banned")
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

    def set_state_change_cb(self, fn):
        self._state_change_cb = fn


class OpportunityScanner:
    @property
    def is_actually_dry(self) -> bool:
        """Détection centralisée du mode DRY_RUN (Simulation)."""
        return (
            str(getattr(self.cfg, "MODE", "")).upper() == "DRY_RUN"
            or bool(getattr(getattr(self.cfg, "rm", None), "dry_run", False))
            or bool(getattr(self.cfg, "dry_run", False))
        )


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

    def __init__(self, cfg, risk_manager, market_router, history_logger=None, simulator=None):
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
        self.simulator = simulator

        self.rm = risk_manager  # compat legacy
        self.router = market_router
        self.history = history_logger
        # LHM optionnel: peut être injecté après coup
        # On initialise à None pour éviter les AttributeError au boot.
        self.logger_historique_manager = None
        # Callback opportunité optionnel (set via set_on_opportunity)
        self.on_opportunity = None
        # Sink optionnel pour l'historique d'opps (set_history_logger)
        self._hist_logger = None
        self.logger = logging.getLogger("OpportunityScanner")
        if self.router is None:
            raise RuntimeError("Scanner: market_router is required")
        if self.risk_manager is None:
            raise RuntimeError("Scanner: risk_manager is required")
        self._warned_aliases: set[str] = set()

        # -------------------------------
        # 1) Paramètres Router (re-logés)
        # -------------------------------
        r = self.cfg.router


        # pair_queue_max : valeur initiale conservatrice, recalée ensuite via apply_runtime_config
        # pair_queue_max : capacité de file PAR PAIRE (Scanner), ne pas dériver du Router.
        # On seed depuis la config Scanner (ou fallback interne), puis apply_runtime_config() recale si besoin.
        self._pair_queue_max = 0
        try:
            self._pair_queue_max = int(self._compute_pair_queue_max_from_cfg(self.cfg))
        except Exception:
            self._pair_queue_max = 1000

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
        self._workers_target = self._workers
        
        # P0: Augmenter radicalement la capacité de traitement simultané pour le Top 120
        # Sinon le Scanner sature dès qu'il dépasse 30 paires
        self._max_pairs_per_tick = int(getattr(s, "max_pairs_per_tick", 150))
        if self.is_actually_dry:
            self._max_pairs_per_tick = 500
            self.logger.info("[Scanner] DRY_RUN: Capacité par tick augmentée à %d paires", self._max_pairs_per_tick)

        if self.is_actually_dry and self._workers_target < 4:
            self._workers_target = 4
            self._workers = 4
            self.logger.info("[Scanner] DRY_RUN: boosting workers count to %d", self._workers_target)
        self._dedup_window_s = float(getattr(s, "dedup_window_s", 0.5))
        self._ttl_hints_s = float(getattr(s, "ttl_hints_s", 1.0))
        self._audition_ttl_min = int(getattr(s, "audition_ttl_min", 5))
        self._ban_ttl_min = int(getattr(s, "ban_ttl_min", 3))
        self._hysteresis_min = int(getattr(s, "hysteresis_min", 2))
        # Kill-switch MM unifié : global (rm) puis opt-in scanner
        rm_cfg = getattr(self.cfg, "rm", None)
        global_mm_enabled = bool(getattr(rm_cfg, "enable_maker_maker", getattr(self.cfg, "enable_maker_maker", False)))
        scanner_mm_enabled = bool(getattr(s, "enable_mm_hints", False))
        if scanner_mm_enabled and not global_mm_enabled:
            self.logger.warning("SCANNER_ENABLE_MM_HINTS=1 ignored because ENABLE_MAKER_MAKER=0")
        self._enable_mm_hints = global_mm_enabled and scanner_mm_enabled
        self.mm_mode = str(getattr(s, "mm_mode", "OFF") or "OFF").upper()
        self._binance_depth_level = int(getattr(s, "binance_depth_level", 10))

        # seuils/rythme (override possibles via cfg.scanner.*)
        self.min_spread_net = float(getattr(s, "min_spread_net", 0.002))      # 20 bps
        self.min_required_bps = float(getattr(s, "min_required_bps", 15.0))   # bps
        self.drift_guard_bps = float(getattr(s, "drift_guard_bps", 10.0))     # bps
        self.max_pairs_per_tick = int(getattr(s, "max_pairs_per_tick", 40))
        self.max_time_skew_s = float(getattr(s, "max_time_skew_s", 0.20))     # 200 ms
        self.scan_interval = float(getattr(s, "scan_interval", 0.5))
        self.dedup_cooldown_s = float(
            max(0.05, float(getattr(s, "dedup_cooldown_s", 0.35)))
        )
        self.backpressure_log_every = int(max(1, getattr(s, "backpressure_log_every", 100)))
        self.max_opportunities = int(max(100, getattr(s, "max_opportunities", 5000)))

        try:
            note_scanner_cfg(self.scan_interval, self.min_required_bps, self.max_pairs_per_tick)
        except Exception:
            pass

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

        # Mode principal (TT/TM/MIXED)
        self.scanner_mode = str(getattr(s, "scanner_mode", "MIXED") or "MIXED").upper()
        if self.scanner_mode not in {"TT", "TM", "MIXED"}:
            self.scanner_mode = "MIXED"

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
        # 6) États & métriques d'exécution
        # ------------------------------------
        from collections import deque
        self.orderbooks = {}  # type: Dict[str, Dict[str, dict]]
        self.opportunities_brutes = deque(maxlen=self.max_opportunities)
        # Taille max utilisée aussi pour le backpressure (cf. _evaluate_pair/snapshot)
        self._maxlen = int(self.max_opportunities)

        self._last_emit = {}  # type: Dict[tuple, float]
        self._last_mm_snapshots = {}  # (pair, exchange) -> dict
        self._mm_churn_rates = {}    # (pair, exchange) -> float
        self._running = False
        self._task = None  # type: Optional[asyncio.Task]
        self.last_scan_time = 0.0
        self.opportunity_count = 0
        self.active_pairs_count = 0
        self.scan_frequency = 0.0
        self.average_spread = 0.0
        self.net_positive_count = 0
        self._last_opp_ts_by_pair = {}  # type: Dict[str, float]

        # Dédup courte par paire (worker mode)
        self._last_scan_ts_by_pair = {}  # type: Dict[str, float]

        # Fraîcheur des books par exchange (latence WS→Scanner)
        self._last_seen_books_by_ex = {}  # type: Dict[str, float]

        # Fenêtre glissante de profondeur QUOTE par (exchange, pair)
        # utilisée par _depth_quote_snapshot / _depth_usd_p95
        self._depth_window_usd = defaultdict(lambda: deque(maxlen=64))

        # Liste d'exchanges utilisée en mode pull (tick); peut être override par Boot
        self.exchanges: List[str] = []
        # Fallback pour _top_pairs si l'univers n'est pas encore synchronisé
        self.pairs: List[str] = list(getattr(self.cfg.g, "pairs", []) or [])


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
        self.emit_hints = bool(getattr(s, "emit_hints", True))
        self.hint_cfg = getattr(s, "hint_cfg", None) or {
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
        # IMPORTANT: knobs rotation vivent dans cfg.scanner.*, pas au top-level cfg
        if bool(getattr(s, "mm_rotation_enabled", False)):

            self._rot_mm = _Rotation(
                primary_n=int(getattr(s, "mm_rotation_primary_n", 4)),
                audition_m=int(getattr(s, "mm_rotation_audition_m", 4)),
                hysteresis_min=int(getattr(s, "mm_rotation_hysteresis_min", 2)),
                ban_ttl_min=int(getattr(s, "mm_rotation_ban_ttl_min", 3)),
            )
            try:
                self._rot_mm.set_state_change_cb(
                    lambda pair, status: self._log_rotation_decision(
                        pair=pair,
                        mode="MM",
                        old_active=None,
                        new_active=False,
                        reason_code="MM_BAN" if status == "banned" else "MM_ROTATE",
                    )
                )
            except Exception:
                self.logger.exception("[Scanner][MM] unable to attach rotation logger", exc_info=True)
        # ------------------------------------
        # 8) Feeds auxiliaires & caches
        # ------------------------------------
        self._slip_cache = {}  # {(EX,PAIR): {"bps": float|None, "ts": float}}
        self._slip_ts = {}  # pair -> ts (dernier snapshot slip)
        self._slip_threshold_bps = {}  # par pair
        self._fees_cache = {}     # {(EX,PAIR): {"taker": float|None, "maker": float|None, "ts": float}}

        # ------------------------------------
        # 9) Volatility (patch scoring local)
        # ------------------------------------
        self._vol_bps = {}        # pair -> vol instant bps
        self._vol_ema = {}        # (ex, pair) -> ema bps
        self.vol_alpha_penalty = float(getattr(s, "vol_alpha_penalty", 0.15))
        self.vol_soft_cap_bps  = float(getattr(s, "vol_soft_cap_bps", 40.0))
        self.vol_beta_min_req  = float(getattr(s, "vol_beta_min_req", 0.20))
        self.vol_chaos_bps     = float(getattr(s, "vol_chaos_bps", 120.0))
        self.vol_ema_lambda    = float(min(0.99, max(0.0, getattr(s, "vol_ema_lambda", 0.7))))

        # ------------------------------------
        # 10) Priorisation mixte (LHM + scanner)
        # ------------------------------------
        self.priority_weight_logger  = float(max(0.0, getattr(s, "priority_weight_logger", 0.7)))
        self.priority_weight_scanner = float(max(0.0, getattr(s, "priority_weight_scanner", 0.3)))
        self._internal_scores = {}  # moyenne récente des scores par paire

        # --- Caches de priorité (LHM) ---
        self._priority_cache = None         # List[str] ou None
        self._priority_last_refresh = 0.0

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

        # --------------------------------
        # Overrides HFT / DRY_RUN (P0: Priorité absolue sur le .env)
        # --------------------------------
        if self.is_actually_dry:
            # On force des quotas virtuellement illimités pour ne pas brider la simulation
            self.global_eval_hz = 5000.0
            self.eval_hz_primary = 1000.0
            self.eval_hz_audition = 500.0
            self.logger.info("[Scanner] DRY_RUN DETECTED: Forcing HFT/Infinite Quotas (Ignoring .env)")

        # token-buckets
        effective_hz = self.global_eval_hz
        
        global_burst = float(getattr(self.cfg.scanner, "scanner_global_eval_burst_factor", 2.0))
        self._tb_global = TokenBucket(
            rate_per_s=effective_hz,
            burst=max(1, int(effective_hz * global_burst)),
            name="scanner_global",
        )
        self.logger.info("[Scanner] global rate limiter: rate=%s/s (dry_run=%s)", effective_hz, self.is_actually_dry)
        self._tb_pair = {}

        # P0: On mémorise le temps de boot pour une grâce initiale sur le staleness
        self._boot_ts = time.monotonic()

        # --- Shedding pilotable ---
        self._shed_load_threshold = float(getattr(s, "shed_load_threshold", 0.95))
        self._shed_primary_factor = float(getattr(s, "shed_primary_factor", 0.8))
        self._shed_primary_min = float(getattr(s, "shed_primary_min", 0.5))
        self._shed_audition_factor = float(getattr(s, "shed_audition_factor", 0.0))
        self._shed_cooldown_s = float(getattr(s, "shed_cooldown_s", 3.0))
        self._hard_shed_until = 0.0
        self._shed_primary = 1.0
        self._shed_audition = 1.0

        # Cohortes (sets) — rétro-compat: PRIMARY/AUDITION existaient déjà
        self._core_pairs = set()
        self._primary_pairs = set()
        self._audition_pairs = set()
        self._sandbox_pairs = set()

        # Liste statique de repli (si discovery off)
        self.pairs = getattr(self.cfg.g, "pairs", []) or []

    # --- Hooks historiques + rejets unifiés (méthodes de classe) ---
    def set_history_logger(self, sink: Callable[[dict], Any]) -> None:
        """Optionnel: connecter LoggerHistoriqueManager.opportunity (callable(dict))."""
        self._hist_logger = sink

    def _should_consider_mm_for_pair(self, pair: str) -> bool:
        """
        Éligibilité MM (couche scanner/hints, "soft") :

        - Gate dur: RM doit autoriser le MM (cfg.rm.enable_maker_maker)
        - Seed pairs: cfg.scanner.mm_seed_pairs => True immédiat
        - Sinon: on autorise sur tiers CORE/PRIMARY (déduit de l'univers actuel)
        - Si rotation activée: la paire doit être dans rot.primary ou rot.audition
          et ne doit pas être bannie
        """
        s = getattr(self.cfg, "scanner", None)
        rm_cfg = getattr(self.cfg, "rm", None)

        # Fail-closed si cfg incomplet
        if s is None or rm_cfg is None:
            return False

        # Gate dur côté RM (c'est RM qui décide si le MM existe, scanner ne fait que des hints)
        if not bool(getattr(rm_cfg, "enable_maker_maker", False)):
            return False

        p = _norm_pair(pair)

        # Seeds explicites (pilotage opérateur)
        seed_pairs = {_norm_pair(x) for x in (getattr(s, "mm_seed_pairs", ()) or ())}
        if p in seed_pairs:
            return True

        # Déterminer le tier de la paire selon l'univers actuel
        tier = "AUDITION"
        if p in getattr(self, "_core_pairs", set()):
            tier = "CORE"
        elif p in getattr(self, "_primary_pairs", set()):
            tier = "PRIMARY"
        elif p in getattr(self, "_sandbox_pairs", set()):
            tier = "SANDBOX"

        # Sans knob "allowed_tiers" dans BotConfig, on reste conservateur:
        # hints MM autorisés uniquement sur CORE/PRIMARY par défaut.
        if tier not in ("CORE", "PRIMARY"):
            return False

        # Si rotation activée, on exige la promotion (primary/audition) et pas de ban
        if bool(getattr(s, "mm_rotation_enabled", False)):
            rot = getattr(self, "_rot_mm", None)
            if rot is None:
                return False
            if rot.is_banned(p):
                return False
            if (p not in rot.primary) and (p not in rot.audition):
                return False

        return True

    def _record_rejection(
        self,
        *,
        reason: str,
        route: str,
        pair: str,
        ctx: Optional[dict] = None,
        strategy: Optional[str] = None,
    ) -> None:
        """Rejet centralisé: métriques + log + historique (facultatif)."""
        
        # 1) Métriques Prometheus (Toujours incrémentées, ultra-rapide)
        try:
            # compteur P0 unifié
            inc_blocked("scanner", reason, pair)
            # métrique harmonisée
            from modules.obs_metrics import inc_scanner_rejection
            inc_scanner_rejection(reason=reason, route=route, pair=pair, strategy=strategy or "TT")
        except Exception:
            pass

        # 2) FAST GATE : Échantillonnage statistique (1/500 par défaut pour les rejets communs)
        # On ne traite le contexte et les logs que si l'échantillonnage passe.
        is_dry = self.is_actually_dry
        
        # Rejets structurels ou rares : on garde un échantillonnage plus généreux
        is_rare = reason not in (
            "book_stale", "below_min_bps", "min_notional", "slippage",
            "SCANNER_PAIR_NOT_IN_PRIORITY", "SCANNER_PAIR_PAUSED",
            "rate_limit_global", "rate_limit_pair", "backlog_purge",
            "below_tm_hint", "neg_both"
        )
        
        # On incrémente un compteur interne pour l'échantillonnage déterministe par raison
        if not hasattr(self, "_rejection_counters"):
            self._rejection_counters = defaultdict(int)
        
        self._rejection_counters[reason] += 1
        count = self._rejection_counters[reason]
        
        # Logique d'échantillonnage : 
        # - Toujours loguer les 3 premiers rejets d'une raison donnée
        # - Puis 1/500 pour les rejets communs
        # - Puis 1/50 pour les rejets rares ou en DRY_RUN
        sample_rate = 500
        if is_rare or is_dry:
            sample_rate = 50
            
        should_log = (count <= 3) or (count % sample_rate == 0)

        if not should_log:
            return

        # 3) Enrichissement du contexte (Seulement si non filtré)
        if ctx is not None and "age_ms" not in ctx:
            try:
                now_ms = int(time.time() * 1000)
                ex = ctx.get("exchange")
                if ex and pair:
                    snap = self.orderbooks.get(ex, {}).get(pair)
                    if snap:
                        recv_ts = snap.get("recv_ts_ms")
                        if recv_ts:
                            ctx["rejection_age_ms"] = now_ms - int(recv_ts)
            except Exception:
                pass

        # 4) Log contextualisé
        try:
            is_dry = self.is_actually_dry
            # P0: Log filtré pour éviter le flood.
            # En DRY_RUN, on continue de masquer les rejets de "bruit" (bps, notionnel, prio) 
            # mais on laisse passer les erreurs "structurelles" (fees, slip, vol, no_books)
            # pour permettre la calibration du bot avant la prod.
            if not is_dry or reason not in (
                "book_stale", "below_min_bps", "min_notional",
                "SCANNER_PAIR_NOT_IN_PRIORITY", "SCANNER_PAIR_PAUSED",
                "rate_limit_global", "rate_limit_pair", "backlog_purge",
                "below_tm_hint"
            ):
                logger.warning(
                    "[Scanner][reject] pair=%s route=%s reason=%s ctx=%s",
                    pair or "UNKNOWN", route, reason, ctx or {}
                )
        except Exception:
            pass

        # 5) Historique (facultatif)
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
        # TTL vol : priorité à l'argument, sinon SLO public, sinon cfg.vol.ttl_s
        if ttl_s is not None:
            ttl = float(ttl_s)
        else:
            ex = next(iter(self.exchanges or []), "")
            ttl = self._public_ttl(ex, "vol_ttl_s", getattr(self.cfg.vol, "ttl_s",
                                                            float(getattr(self, "_vol_ttl_s", self.cfg.vol.ttl_s))))

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
        if taker is not None:
            taker_val = float(taker)
            d["taker"] = taker_val if taker_val > 0.0 else None
        if maker is not None:
            maker_val = float(maker)
            d["maker"] = maker_val if maker_val >= 0.0 else None
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
        role_norm = str(role or "").lower()
        try:
            d = self._fees_cache.get((_norm_ex(ex), _norm_pair(pair)))
            if d and d.get(role) is not None:
                fee_val = float(d[role])
                if role_norm == "taker" and fee_val <= 0.0:
                    return None
                return fee_val * 1e4
        except Exception:
            logging.exception("Unhandled exception")

        g = getattr(self.risk_manager, "get_fee_pct", None)
        if not callable(g):
            return None

        try:
            fee_val = float(g(_norm_ex(ex), _norm_pair(pair), role))
            if role_norm == "taker" and fee_val <= 0.0:
                return None
            return fee_val * 1e4
        except TypeError:
            logger.exception("Scanner get_fee_pct signature mismatch for %s/%s", ex, pair)
        except Exception:
            logger.exception("Scanner get_fee_pct error for %s/%s", ex, pair)
        return None

    def _fee_frac(self, ex: str, role: str, pair: str) -> Optional[float]:
        """Retourne les fees en fraction (ex: 0.001), cache → fallback RM."""
        bps = self._fee_bps(ex, role, pair)
        if isinstance(bps, (int, float)):
            return float(bps) / 1e4
        return None

    def _get_priority_list(self) -> Optional[List[str]]:
        """Récupère et cache la liste des paires prioritaires du LHM."""
        now = time.time()
        # Refresh toutes les 1.0s pour éviter la saturation CPU/Log
        if self._priority_cache is not None and (now - self._priority_last_refresh) < 1.0:
            return self._priority_cache

        mgr = getattr(self, "logger_historique_manager", None)
        if mgr is None:
            return None

        priority = None
        try:
            # 1) si possible, priorité par mode (TT/TM)
            if self.scanner_mode in {"TT", "TM"}:
                get_prio_by_mode = getattr(mgr, "get_priority_pairs_by_mode", None)
                if callable(get_prio_by_mode):
                    priority = get_prio_by_mode(self.scanner_mode)

            # 2) fallback sur priorité globale si rien trouvé
            if priority is None:
                get_prio = getattr(mgr, "get_priority_pairs", None)
                if callable(get_prio):
                    priority = get_prio()
        except Exception:
            priority = None

        self._priority_cache = priority
        self._priority_last_refresh = now
        return priority

    # ---------------- Entrées (push) ----------------
    def update_orderbook(self, data: dict) -> None:
        # Fraîcheur ToB par exchange (utile pour estimer la latence)
        try:
            ex_for_fresh = _norm_ex(data.get("exchange"))
            if ex_for_fresh:
                self._last_seen_books_by_ex[ex_for_fresh] = time.monotonic()
        except Exception:
            pass

        if not data:
            return

        # P1: Validation légère de schéma à la frontière Router -> Scanner.
        # EXTRÊMEMENT RAPIDE (Fast Path). On ne check que le minimum vital.
        ex = _norm_ex(data.get("exchange"))
        pair_raw = data.get("pair_key") or data.get("symbol")
        if not pair_raw or not ex:
            return
        pair = _norm_pair(pair_raw)

        # P0: Mise à jour systématique du cache latest
        self.orderbooks.setdefault(ex, {})[pair] = data

        if not data.get("active", True):
            self._record_rejection(reason="inactive", route="*", pair=pair, ctx={"exchange": ex})
            return

        # P0: Calcul d'âge par MONOTONIC (Précis, Athlétique, Insensible aux sauts d'heure)
        now_mono_ns = time.monotonic_ns()
        recv_mono_ns = data.get("recv_mono_ns")

        if recv_mono_ns is None:
            recv_mono_ns = now_mono_ns
            data["recv_mono_ns"] = recv_mono_ns
        age_ms = (now_mono_ns - int(recv_mono_ns)) // 1_000_000

        book_ttl_ms = data.get("book_ttl_ms")
        if book_ttl_ms is None:
            book_ttl_ms = self._book_ttl_ms_default(ex)
        
        # P0: On respecte les TTL de prod même en DRY_RUN pour permettre la calibration.
        is_actually_dry = self.is_actually_dry

        if age_ms > int(book_ttl_ms or 1200):
            # P0 Observabilité
            try:
                from modules.obs_metrics import SCANNER_EVENT_AGE_MS
                if SCANNER_EVENT_AGE_MS:
                    SCANNER_EVENT_AGE_MS.labels(exchange=ex, pair=pair).observe(age_ms)
            except Exception:
                pass

            # P0: Grâce initiale de 5s au démarrage du Scanner
            if (time.monotonic() - self._boot_ts) < 5.0:
                return

            self._record_rejection(
                reason="book_stale",
                route="*",
                pair=pair,
                ctx={"exchange": ex, "age_ms": age_ms, "ttl_ms": int(book_ttl_ms or 1200)},
            )
            return
        
        # P0 Observabilité (Success)
        try:
            from modules.obs_metrics import SCANNER_EVENT_AGE_MS
            if SCANNER_EVENT_AGE_MS:
                SCANNER_EVENT_AGE_MS.labels(exchange=ex, pair=pair).observe(age_ms)
        except Exception:
            pass

        # --- NOUVEAU : Incrémenter le Rate Limiter Scanner ---
        try:
            from modules.obs_metrics import SCANNER_RATE_LIMITED_TOTAL
            # On ne veut pas importer Engine à chaque tick, on vérifie si présent
            if SCANNER_RATE_LIMITED_TOTAL:
                pass # Déjà géré par _record_rejection ou le loop principal
        except ImportError:
            pass
        

        # --- NOUVEAU : Mesure latence Router -> Scanner ---
        pub_ts = data.get("publish_ts_ms")
        if pub_ts:
            try:
                now_ms_local = int(time.time() * 1000)
                transfer_lat = now_ms_local - int(pub_ts)
                from modules.obs_metrics import ENGINE_ROUTER_TO_SCANNER_LAT_MS, _MetricNoOp
                if ENGINE_ROUTER_TO_SCANNER_LAT_MS is not None and not isinstance(ENGINE_ROUTER_TO_SCANNER_LAT_MS, _MetricNoOp):
                    ENGINE_ROUTER_TO_SCANNER_LAT_MS.observe(float(transfer_lat))
            except Exception: pass

        # Univers/pauses (LHM)
        is_dry = self.is_actually_dry
        if is_dry:
            # P0: Forcer le passage en DRY_RUN
            pass
        elif self._universe is not None and pair not in self._universe:
            self._record_rejection(reason="SCANNER_PAIR_NOT_IN_UNIVERSE", route="*", pair=pair, ctx={"exchange": ex})
            return
        try:
            priority = self._get_priority_list()
            if priority and pair not in priority:
                self._record_rejection(
                    reason="SCANNER_PAIR_NOT_IN_PRIORITY", route="*", pair=pair, ctx={"exchange": ex}
                )
                return

            mgr = getattr(self, "logger_historique_manager", None)
            if mgr is not None:
                # P0: En DRY_RUN, on bypass le pause check pour ne pas bloquer les tests
                is_paused_fn = getattr(mgr, "is_paused", None)
                # Ne pas écraser is_actually_dry : il inclut cfg.rm.dry_run
                if (not is_dry) and callable(is_paused_fn) and is_paused_fn(pair):
                    self._record_rejection(reason="SCANNER_PAIR_PAUSED", route="*", pair=pair, ctx={"exchange": ex})
                    return

                # --- NOUVEAU : Incrémenter le Rate Limiter Scanner ---
                from modules.obs_metrics import SCANNER_RATE_LIMITED_TOTAL
                if hasattr(self, "_tb_global") and not self._tb_global.try_consume():
                    if SCANNER_RATE_LIMITED_TOTAL is not None and not hasattr(SCANNER_RATE_LIMITED_TOTAL, "_MetricNoOp"):
                        # P0: Fix ValueError: Incorrect label names (besoin de 'kind' et 'cohort')
                        SCANNER_RATE_LIMITED_TOTAL.labels(kind="global", cohort="ALL").inc()
                    self._record_rejection(reason="rate_limit_global", route="*", pair=pair, ctx={"exchange": ex})
                    return

        except Exception:
            logging.exception("Unhandled exception")


        # Quotas par paire
        allowed, _why = self._should_scan_now(pair)
        if self._workers_target > 0:
            if allowed:
                self._enqueue_for_workers(pair, data)
            return
        if not allowed:
            return

        # Évalue uniquement la paire concernée (mode pull / sans workers)
        start_proc = time.perf_counter_ns()
        self.check_opportunity(pair)

        # Instrumentation latence Scanner Pull (P1)
        if should_trace_latency(self.cfg):
            proc_ms = (time.perf_counter_ns() - start_proc) / 1e6
            if getattr(self.cfg.obs, "enable_segment_metrics_scanner", True):
                record_pipeline_latency("scanner_proc", proc_ms, route="*", exchange=data.get("exchange", "none"))

    # ---------------- Pull utils ----------------
    def _enqueue_for_workers(self, pair: str, event: dict) -> None:
        try:
            dq = self._queues[pair]

            # Instrumentation latence queue interne Scanner (P1)
            if should_trace_latency(self.cfg):
                event["scanner_internal_exit_ns"] = time.perf_counter_ns()

            # P1: Purge agressive pendant la phase de boot (5s)
            # Si on accumule trop dans la file d'une paire alors qu'on n'a pas encore fini de booter,
            # on ne garde que le plus frais pour éviter le burst de staleness à T=5.01s.
            if (time.monotonic() - self._boot_ts) < 5.0 and len(dq) >= 1:
                dq.clear()

            if self._pair_queue_max and len(dq) >= self._pair_queue_max:
                self._record_rejection(reason="queue_full", route="*", pair=pair)
            dq.append(event)
            self._last_enqueued_ts_ms[pair] = int(event.get("exchange_ts_ms") or event.get("recv_ts_ms") or 0)
        except Exception:
            logger.exception("[Scanner] enqueue failed for %s", pair)

    def get_books(self) -> Dict[str, Dict[str, dict]]:
        """
        Mode pull : retourne un snapshot des orderbooks par exchange/pair.

        Par défaut on expose simplement le cache interne alimenté par
        update_orderbook(). Un environnement peut surcharger cette méthode
        (monkey-patch) tant qu'il renvoie un mapping du type :

            {
                "BINANCE": { "BTCUSDC": {...}, ... },
                "BYBIT":   { ... },
                ...
            }

        où chaque valeur interne est un dict contenant au minimum les
        champs utilisés par le Scanner (best_bid/best_ask, orderbook, ts, ...).
        """
        books = self.orderbooks or {}
        # Shallow-copy pour éviter que l'appelant ne mute nos dicts internes.
        return {ex: dict(pairs) for ex, pairs in books.items()}


    # ---------------- Pull utils ----------------
    def _top_pairs(self) -> List[str]:
        """Priorisation canonique PairHistory, raffinée par le score interne Scanner."""
        base = sorted({_norm_pair(p) for p in (self._universe or self.pairs or []) if _norm_pair(p)})
        if not base:
            return []

        self._refresh_internal_scores(window_s=180.0)
        internal = self._internal_scores
        max_int = max(internal.values(), default=0.0) or 1.0

        rank_map: Dict[str, int] = {}
        try:
            if self.scanner_mode in {"TT", "TM"} and hasattr(
                self.logger_historique_manager, "get_rank_map_by_mode"
            ):
                rank_map = self.logger_historique_manager.get_rank_map_by_mode(
                    self.scanner_mode
                )
            elif hasattr(self.logger_historique_manager, "get_rank_map"):
                rank_map = self.logger_historique_manager.get_rank_map(None)
        except Exception:
            rank_map = {}

        if not rank_map:
            try:
                mgr = getattr(self, "logger_historique_manager", None)
                ranked = []
                if mgr is not None:
                    get_ranked = getattr(mgr, "get_ranked_pairs", None)
                    if callable(get_ranked):
                        ranked = get_ranked() or []
                rank_map = {
                    _norm_pair(p): i + 1
                    for i, p in enumerate(ranked)
                    if _norm_pair(p) in base
                }
            except Exception:
                rank_map = {}

        rank_map = {
            _norm_pair(p): int(r)
            for p, r in (rank_map or {}).items()
            if _norm_pair(p) in base and isinstance(r, (int, float))
        }


        base_pairs = list(base)
        internal_norm = {
            p: (internal.get(p, 0.0) / max_int) if max_int > 0 else 0.0 for p in base_pairs
        }

        def _sort_key(pk: str) -> tuple:
            rk = rank_map.get(pk)
            has_rank = rk is not None
            return (
                0 if has_rank else 1,
                rk if rk is not None else float("inf"),
                -internal_norm.get(pk, 0.0),
                pk,
            )

        ordered = sorted(base_pairs, key=_sort_key)
        return ordered[: self.max_pairs_per_tick]

    def _fee_cost_fast(self, buy_ex: str, sell_ex: str, pair: str) -> float:
        """Pré-filtre rapide STRICT: (frais taker buy + sell) + max(slippage buy/sell) — pas de valeurs inventées."""
        pk = _norm_pair(pair)

        # Fees: cache -> RM (fail-closed si inconnues ou ambiguës)
        try:
            f_buy = float(self.risk_manager.get_fee_pct(buy_ex, pk, "taker"))
        except DataStaleError:
            raise
        except Exception:
            raise DataStaleError("fee_unknown")
        try:
            f_sell = float(self.risk_manager.get_fee_pct(sell_ex, pk, "taker"))
        except DataStaleError:
            raise
        except Exception:
            raise DataStaleError("fee_unknown")

        if f_buy is None or f_sell is None or f_buy <= 0.0 or f_sell <= 0.0:
            raise DataStaleError("fee_unknown")

        slip_ttl = min(
            self._public_ttl(buy_ex, "slip_ttl_s", getattr(self.cfg.slip, "ttl_s", 5.0)),
            self._public_ttl(sell_ex, "slip_ttl_s", getattr(self.cfg.slip, "ttl_s", 5.0)),
        )
        slip_age = self._slip_age_seconds(pk)
        if not math.isfinite(slip_age) or slip_age > slip_ttl:
            raise DataStaleError("slip_unknown_or_stale")
        if f_buy is None:
            f_buy = float(self.risk_manager.get_fee_pct(buy_ex, pk, "taker"))
        if f_sell is None:
            f_sell = float(self.risk_manager.get_fee_pct(sell_ex, pk, "taker"))

        # Slippage: cache -> RM, sinon REJET en amont (pas de "0" de confort)
        try:
            slip_buy = self.risk_manager.get_slippage(buy_ex, pk, "buy")
        except DataStaleError:
            raise
        if slip_buy is None:
            slip_buy = self.risk_manager.get_slippage(buy_ex, pk, "buy")
            if slip_buy is None:
                raise DataStaleError("slip_unknown_or_stale")

        try:
            slip_sell = self.risk_manager.get_slippage(sell_ex, pk, "sell")
        except DataStaleError:
            raise

        if slip_sell is None:
            slip_sell = self.risk_manager.get_slippage(sell_ex, pk, "sell")
            if slip_sell is None:
                raise DataStaleError("slip_unknown_or_stale")

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
        ts_now = time.time()
        d["ts"] = ts_now
        self._slip_ts[_norm_pair(pair_key)] = ts_now
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
    def _book_ttl_ms_default(self, exchange: str) -> Optional[int]:
        """
        Retourne le TTL par défaut pour les orderbooks.
        Optimisation HFT : On utilise une valeur légèrement plus large dans le Scanner 
        que dans le RiskManager (RM) pour laisser passer les opportunités vers le gating final
        sans double rejet précoce.
        """
        ttl_ms = None
        try:
            router_cfg = getattr(self.cfg, "router", None)
            # Si le Router dit 1200ms, le Scanner accepte 1500ms pour éviter les micro-conflits
            ttl_ms = getattr(router_cfg, "stale_ms", None) if router_cfg is not None else None
            if isinstance(ttl_ms, (int, float)) and ttl_ms > 0:
                return int(ttl_ms) + 300
        except Exception:
            ttl_ms = None
        try:
            max_book_age_s = getattr(self.cfg, "max_book_age_s", None)
            if isinstance(max_book_age_s, (int, float)) and max_book_age_s > 0:
                return int(float(max_book_age_s) * 1000.0) + 300
        except Exception:
            pass
        return 1500 # Default fallback raisonnable pour 120 paires

    def _snapshot_ts_seconds(self, snap: dict, *, pair_key: str, exchange: str) -> Optional[float]:
        if not snap.get("active", True):
            self._record_rejection(
                reason="inactive",
                route="*",
                pair=pair_key,
                ctx={"exchange": exchange},
            )
            return None

        recv_ts_ms = snap.get("recv_ts_ms")
        book_ttl_ms = snap.get("book_ttl_ms")
        if book_ttl_ms is None:
            book_ttl_ms = self._book_ttl_ms_default(exchange)

        # P0: calcul d'âge robuste par MONOTONIC
        now_mono_ns = time.monotonic_ns()
        recv_mono_ns = snap.get("recv_mono_ns")
        
        if recv_mono_ns is not None:
            age_ms = (now_mono_ns - int(recv_mono_ns)) // 1_000_000
        else:
            if recv_ts_ms is None:
                self._record_rejection(reason="schema_missing_field", route="*", pair=pair_key, ctx={"exchange": exchange, "field": "recv_ts_ms"})
                return None
            age_ms = int(time.time() * 1000) - int(recv_ts_ms)

        if age_ms > int(book_ttl_ms or 1500):
            if (time.monotonic() - getattr(self, "_boot_ts", 0)) < 5.0:
                return None
            if age_ms > 60000:
                self.logger.warning("[Scanner] ABSURD AGE detected in _snapshot_ts_seconds for %s: %sms", pair_key, age_ms)
            self._record_rejection(
                reason="book_stale",
                route="*",
                pair=pair_key,
                ctx={"exchange": exchange, "age_ms": age_ms, "ttl_ms": int(book_ttl_ms or 1500)},
            )
            return None

        ts_ms = snap.get("exchange_ts_ms") or recv_ts_ms
        if ts_ms is None:
            return None
        return float(ts_ms) / 1000.0
    def _synced_snapshots_for_pair(self, pair_key: str):
        snaps: List[Tuple[str, dict, float]] = []
        # P0: On identifie l'âge maximal toléré pour synchronisation (plus strict que _snapshot_ts_seconds)
        now_ms = int(time.time() * 1000)
        mono_now_ns = time.monotonic_ns()

        for ex, pairs in self.orderbooks.items():
            snap = pairs.get(pair_key)
            if not snap:
                continue
            
            # --- AJOUT P1 : Double check de fraicheur avant sync ---
            # Si une paire n'a pas été mise à jour depuis longtemps dans orderbooks,
            # on ne veut même pas tenter de la synchroniser.
            recv_mono_ns = snap.get("recv_mono_ns")
            if recv_mono_ns is not None:
                age_ms = int((mono_now_ns - int(recv_mono_ns)) / 1_000_000)
            else:
                recv_ts = snap.get("recv_ts_ms")
                age_ms = (now_ms - int(recv_ts)) if recv_ts else 999999
            
            # Si le snapshot a plus de 2.5s, on l'ignore silencieusement ici (déjà loggué par _snapshot_ts_seconds si appelé)
            if age_ms > 2500:
                continue

            ts_s = self._snapshot_ts_seconds(snap, pair_key=pair_key, exchange=ex)
            if ts_s is None:
                continue
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

    def _evaluate_routes_for_pair(self, pair: str, ev: dict, enable_mm: bool) -> None:
        """
        Reconstruit les routes TT/TM à partir des snapshots synchronisés
        pour une paire et évalue chaque combinaison via _evaluate_pair.
        Respecte les filtres d'univers déjà appliqués dans la boucle principale.
        """
        _ = ev  # ev conservé pour compat éventuelle
        pair_key = _norm_pair(pair)
        for ex_buy, buy_data, ex_sell, sell_data in self._synced_snapshots_for_pair(pair_key):
            self._evaluate_pair(buy_data, sell_data)


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

        try:
            tier_map = {
                "CORE": self._core_pairs,
                "PRIMARY": self._primary_pairs,
                "AUDITION": self._audition_pairs,
                "SANDBOX": self._sandbox_pairs,
            }
            if hasattr(self.logger_historique_manager, "set_tier"):
                for tier_name, pairs in tier_map.items():
                    for pair in pairs:
                        try:
                            self.logger_historique_manager.set_tier(pair, tier_name)
                        except Exception:
                            self.logger.exception("[Scanner] persist tier failed", exc_info=True)
        except Exception:
            self.logger.exception("[Scanner] tier persistence failed")

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
        burst_factor = float(getattr(self.cfg.scanner, "scanner_eval_burst_factor", 2.0))
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
            self._tb_pair[pk] = TokenBucket(
                rate_per_s=rate,
                burst=max(1, int(rate * burst_factor)),
                name="scanner_pair:universe",
            )

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
            # Pause côté LHM si possible
            try:
                mgr = getattr(self, "logger_historique_manager", None)
                if mgr is not None and self.autopause_duration_s > 0:
                    pause_pair = getattr(mgr, "pause_pair", None)
                    if callable(pause_pair):
                        pause_pair(p, duration=int(self.autopause_duration_s))
                        self.logger.info(
                            "[Scanner] Autopause audition %s (TTL=%ss)", p, int(ttl)
                        )
            except Exception:
                logging.exception("Unhandled exception")

            # Mise à jour locale des structures audition
            self._audition_pairs.discard(p)
            self._audition_deadlines.pop(p, None)

            # Persist tier removal côté LHM
            try:
                mgr = getattr(self, "logger_historique_manager", None)
                if mgr is not None:
                    set_tier = getattr(mgr, "set_tier", None)
                    if callable(set_tier):
                        set_tier(p, None)
            except Exception:
                self.logger.exception("[Scanner] persist tier removal failed", exc_info=True)

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
        N = int(getattr(self.cfg.scanner, "binance_depth_level", 10))
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

    # --- MICROSTRUCTURE (MM Adaptive) ---
    def _compute_microprice_imbalance(self, data: dict) -> tuple[float, float]:
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        if not bids or not asks:
            return 0.0, 0.0

        # On prend les 3 premiers niveaux pour l'imbalance
        b_vol = sum(float(b[1]) for b in bids[:3])
        a_vol = sum(float(a[1]) for a in asks[:3])

        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])

        total_vol = b_vol + a_vol
        if total_vol <= 0:
            return (best_bid + best_ask) / 2.0, 0.0

        imbalance = (b_vol - a_vol) / total_vol
        microprice = (best_bid * a_vol + best_ask * b_vol) / total_vol
        return microprice, imbalance

    def _update_churn(self, pair: str, exchange: str, data: dict) -> float:
        key = (pair, exchange)
        last = self._last_mm_snapshots.get(key)
        now = time.time()
        
        if last is None:
            self._last_mm_snapshots[key] = data.copy()
            self._last_mm_snapshots[key]["_t_internal"] = now
            return 0.0

        # Churn = nombre de changements dans le top 3 / dt
        changes = 0
        cur_bids = data.get("bids", [])
        last_bids = last.get("bids", [])
        for i in range(min(3, len(cur_bids), len(last_bids))):
            if cur_bids[i] != last_bids[i]: changes += 1
            
        cur_asks = data.get("asks", [])
        last_asks = last.get("asks", [])
        for i in range(min(3, len(cur_asks), len(last_asks))):
            if cur_asks[i] != last_asks[i]: changes += 1

        dt = max(0.001, now - last.get("_t_internal", now - 0.1))
        
        self._last_mm_snapshots[key] = data.copy()
        self._last_mm_snapshots[key]["_t_internal"] = now

        # EWMA sur le churn
        old_churn = self._mm_churn_rates.get(key, 0.0)
        instant_churn = changes / dt
        new_churn = old_churn * 0.9 + instant_churn * 0.1
        self._mm_churn_rates[key] = new_churn

        try:
            from modules.obs_metrics import MM_CHURN_RATE
            if MM_CHURN_RATE:
                MM_CHURN_RATE.labels(exchange=exchange, pair=pair).set(new_churn)
        except Exception: pass

        return new_churn

    def _compute_depth_profile(self, data: dict) -> float:
        # Ratio volume L1 / volume Top 5. Plus c'est proche de 1, plus c'est "top-heavy" (fragile)
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        if not bids or not asks: return 0.0

        v1 = float(bids[0][1]) + float(asks[0][1])
        v5 = sum(float(b[1]) for b in bids[:min(5, len(bids))]) + sum(float(a[1]) for a in asks[:min(5, len(asks))])

        if v5 <= 0: return 0.0
        ratio = v1 / v5
        
        try:
            pair = data.get("pair_key") or data.get("symbol")
            ex = data.get("exchange")
            from modules.obs_metrics import MM_DEPTH_PROFILE_RATIO
            if MM_DEPTH_PROFILE_RATIO and pair and ex:
                MM_DEPTH_PROFILE_RATIO.labels(exchange=ex, pair=pair).set(ratio)
        except Exception: pass

        return ratio

    # --- SCORE MM (quote-agnostic + fallback de config) ---
    def _score_mm(
            self, *, a_ex: str, b_ex: str, pair: str, maker_px_a: float, maker_px_b: float,
            a_data: Optional[dict] = None, b_data: Optional[dict] = None
    ) -> tuple[float, dict]:
        """
        Score MM : combine net_bps estimé, proba de fill bilatérale (queue vs depth) et pénalité vol.
        Quote-agnostic : toutes les grandeurs de profondeur/queue sont en notional QUOTE.
        """
        s = self.cfg.scanner

        vol_ema = float(self._vol_ema.get(pair, 0.0))

        dA = self._depth_p95_quote(a_ex, pair)
        dB = self._depth_p95_quote(b_ex, pair)
        qposA = self._queuepos_est_quote(a_ex, pair, maker_px_a, side="SELL")
        qposB = self._queuepos_est_quote(b_ex, pair, maker_px_b, side="BUY")

        # Indicateurs microstructure additionnels
        micro_A, imb_A = (0.0, 0.0)
        micro_B, imb_B = (0.0, 0.0)
        churn_A, churn_B = (0.0, 0.0)
        dp_A, dp_B = (0.0, 0.0)

        if a_data:
            micro_A, imb_A = self._compute_microprice_imbalance(a_data)
            churn_A = self._update_churn(pair, a_ex, a_data)
            dp_A = self._compute_depth_profile(a_data)
        if b_data:
            micro_B, imb_B = self._compute_microprice_imbalance(b_data)
            churn_B = self._update_churn(pair, b_ex, b_data)
            dp_B = self._compute_depth_profile(b_data)

        # Fallbacks de config : *_quote puis *_usd (rétro-compat)
        depth_min_quote = float(
            getattr(s, "mm_depth_min_quote",
                    getattr(s, "mm_depth_min_usd", 7500.0))
        )
        qpos_max_quote = float(
            getattr(s, "mm_qpos_max_ahead_quote",
                    getattr(s, "mm_qpos_max_ahead_usd", 5000.0))
        )
        mm_min_net_bps = float(
            getattr(s, "mm_min_net_bps", 0.0005)  # 5 bps par défaut (0.0005 en fraction)
        ) * 1e4

        def _expected_net_bps_mm(a_price: float, b_price: float) -> float:
            if a_price <= 0 or b_price <= 0:
                return float("-inf")
            mid = (a_price + b_price) / 2.0
            brut = abs(a_price - b_price) / max(mid, 1e-12)
            hedge_cost_bps = float(getattr(s, "mm_hedge_cost_bps", 5.0))
            return (brut - hedge_cost_bps / 1e4) * 1e4

        net_bps = _expected_net_bps_mm(maker_px_a, maker_px_b)

        # Éligibilité dure (vol, profondeur minimale, queue ahead maximale).
        # Scanner = couche "soft" : on coupe les opportunités MM si la vol EMA dépasse
        # un seuil dédié (cfg.mm_vol_bps_max), mais le RM garde la main sur la décision
        # finale et les caps par profil.
        mm_vol_max = float(getattr(s, "mm_vol_bps_max", self.vol_soft_cap_bps))
        if vol_ema > mm_vol_max:
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
            "ttl_ms": int(getattr(s, "mm_ttl_ms", 2200)),
            # Nouveaux indicateurs
            "microprice": {"A": micro_A, "B": micro_B},
            "imbalance": {"A": imb_A, "B": imb_B},
            "churn_rate": {"A": churn_A, "B": churn_B},
            "depth_profile": {"A": dp_A, "B": dp_B},
        }
        return score, hints

    def _log_rotation_decision(self, **kwargs) -> None:
        mgr = getattr(self, "logger_historique_manager", None) or getattr(self, "history", None)
        fn = getattr(mgr, "log_rotation_decision", None)
        if callable(fn):
            try:
                fn(**kwargs)
            except Exception:
                self.logger.exception("[Scanner] log_rotation_decision failed", exc_info=True)

    def _rotation_tick_mm(self, *, candidate_pairs: List[str]) -> None:
        """
        Tick de rotation MM : injection simple d'un ranking (ici l'ordre reçu).
        """
        rot = getattr(self, "_rot_mm", None)
        if rot is None:
            return

        def _snapshot_state() -> dict:
            banned = set()
            try:
                banned = {p for p, ts in getattr(rot, "_banned_until", {}).items() if rot.is_banned(p)}
            except Exception:
                banned = set()
            return {
                "primary": set(getattr(rot, "primary", []) or []),
                "audition": set(getattr(rot, "audition", []) or []),
                "banned": banned,
            }

        before = _snapshot_state()
        ranked_pairs = list(candidate_pairs)

        use_pairhistory = bool(getattr(self.cfg, "mm_use_pairhistory", True))
        if use_pairhistory and hasattr(self.logger_historique_manager, "get_priority_pairs_by_mode" ):
            try:
                ranked_from_history = self.logger_historique_manager.get_priority_pairs_by_mode("MM")
                if ranked_from_history:
                    ranked_pairs = list(dict.fromkeys(ranked_from_history + ranked_pairs))
            except Exception:
                logger.exception("[Scanner][MM] pairhistory ranking unavailable", exc_info=True)

        self._rot_mm.update(ranked_pairs=ranked_pairs, allow_promote=True)
        try:
            after = _snapshot_state()
            self._log_mm_rotation_changes(before, after)
        except Exception:
            self.logger.exception("[Scanner][MM] rotation logging failed", exc_info=True)

    def _log_mm_rotation_changes(self, before: Dict[str, Set[str]], after: Dict[str, Set[str]]) -> None:
        def _state_of(pair: str, state: Dict[str, Set[str]]) -> Optional[str]:
            if pair in state.get("banned", set()):
                return "banned"
            if pair in state.get("primary", set()):
                return "primary"
            if pair in state.get("audition", set()):
                return "audition"
            return None

        all_pairs: Set[str] = set()
        for st in (before, after):
            for arr in st.values():
                all_pairs.update(arr or set())

        for pair in all_pairs:
            old = _state_of(pair, before)
            new = _state_of(pair, after)
            if old == new:
                continue

            old_active = old in {"primary", "audition"}
            new_active = new in {"primary", "audition"}

            if new == "banned":
                reason = "MM_BAN"
            elif old == "banned" and new_active:
                reason = "MM_UNBAN"
            elif new == "primary":
                reason = "MM_PROMOTE"
            elif new == "audition" and old != "primary":
                reason = "MM_AUDITION"
            elif old == "primary" and new == "audition":
                reason = "MM_DEMOTE"
            elif old_active and not new_active:
                reason = "MM_ROTATE_OUT"
            else:
                reason = "MM_ROTATE"

            self._log_rotation_decision(
                pair=pair,
                mode="MM",
                old_active=old_active,
                new_active=new_active,
                reason_code=reason,
            )
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

    def get_cohort(self, pair: str) -> str:
        """API read-only minimale: expose la cohorte actuelle d'une paire."""
        return self._cohort_of(pair)

    def _strict_config(self) -> bool:
        rm_cfg = getattr(self.cfg, "rm", None)
        return bool(getattr(rm_cfg, "strict_config", False))

    # À mettre dans opportunity_scanner.py (dans la classe)
    def apply_runtime_config(self, cfg: Any) -> None:
        """
        Recalage runtime depuis cfg (non destructif).
        IMPORTANT: pair_queue_max est une capacité "par paire" (pas "par tick")
        """
        now = time.time()

        # --- Pair queue max (par paire, pas "par tick")
        # Recalcul propre: dépend du PRIMARY deque (ou fallback)
        try:
            new_pair_max = int(self._compute_pair_queue_max_from_cfg(cfg))
            old_pair_max = int(getattr(self, "_pair_queue_max", 1500) or 1500)

            if new_pair_max != old_pair_max:
                self._pair_queue_max = new_pair_max
                logger.info(
                    "[Scanner] pair_queue_max ajusté à %d (via PRIMARY_DEQUE_MAX)",
                    new_pair_max,
                )
        except Exception:
            pass

        # --- Touch timestamp (utile si tu as du watchdog)
        try:
            self._last_runtime_cfg_ts = now
        except Exception:
            pass

    def _compute_pair_queue_max_from_cfg(self, cfg: Any) -> int:
        """
        Déduit une capacité "par paire" robuste.

        Règle:
        - On part du deque PRIMARY (capacité de la cohorte PRIMARY),
          car c'est la meilleure approximation d'une pression "par paire".
        - AUCUNE division par max_pairs_per_tick (sinon tu retombes dans 5000 -> 30).
        - On garde un plancher de sécurité.
        """
        # Base: PRIMARY deque max si dispo, sinon fallback conservateur
        base_primary = int(getattr(self, "_deque_max_primary", 1500) or 1500)

        # Si cfg.scanner a un override explicite, on le respecte (si tu l’ajoutes plus tard).
        sc = getattr(cfg, "scanner", None)
        if sc is not None:
            try:
                v = getattr(sc, "pair_queue_max", None)
                if v is not None:
                    return max(50, int(v))
            except Exception:
                pass
            try:
                # Si tu pilotes deque_max_primary côté cfg.scanner, on le prend en priorité
                v = getattr(sc, "deque_max_primary", None)
                if v is not None:
                    base_primary = max(50, int(v))
            except Exception:
                pass

        # Plancher: jamais trop petit (sinon étouffement)
        # (50 est volontairement petit mais safe; en pratique base_primary domine)
        return max(50, int(base_primary))

    def _ensure_buckets_for_pair(self, pair: str) -> TokenBucket:
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
        burst_factor = float(getattr(self.cfg.scanner, "scanner_eval_burst_factor", 2.0))
        tb = TokenBucket(
            rate_per_s=rate,
            burst=max(1, int(rate * burst_factor)),
            name=f"scanner_pair:{cohort}",
        )
        self._tb_pair[pk] = tb
        return tb

    def _should_scan_now(self, pair: str) -> tuple[bool, str]:
        """
        Gate central : cap global + bucket par paire (cohorte).
        Retour (ok, raison_bloque).
        """
        # global
        if hasattr(self, "_tb_global") and not self._tb_global.try_consume():
            self._record_rejection(reason="rate_limit_global", route="*", pair=pair)
            return False, "global"

        # pair bucket (4 tiers)
        cohort = self._cohort_of(pair)
        tb = self._tb_pair.get(_norm_pair(pair))
        if tb is None:
            tb = self._ensure_buckets_for_pair(pair)

        if not tb.try_consume():
            self._record_rejection(reason="rate_limit_pair", route="*", pair=pair, ctx={"cohort": cohort})
            return False, "pair_bucket"

        return True, ""

    def _tb_load(self, tb: TokenBucket) -> float:
        try:
            tokens = float(tb.next_tokens())
            capacity = float(getattr(tb, "capacity", 1.0))
            if capacity <= 0:
                return 0.0
            return 1.0 - min(1.0, tokens / capacity)
        except Exception:
            return 0.0
    # opportunity_scanner.py (dans la classe)
    def _slip_age_seconds(self, pair: str) -> float:
        p = self._norm_pair(pair)
        now = time.time()
        ages = []
        for (ex, pk), meta in getattr(self, "_slip_cache", {}).items():
            if pk != p:
                continue
            ts_val = meta.get("ts")
            if ts_val:
                ages.append(max(0.0, now - float(ts_val)))

        if not ages:
            ts = (getattr(self, "_slip_ts", {}) or {}).get(p)
            if ts:
                ages.append(max(0.0, now - float(ts)))

        return min(ages) if ages else float("inf")

    def _vol_age_seconds(self, pair: str) -> float:
        p = self._norm_pair(pair)
        now = time.time()
        ages = []
        for (ex, pk), ts in getattr(self, "_vol_ts", {}).items():
            if pk != p:
                continue
            if ts:
                ages.append(max(0.0, now - float(ts)))

        return min(ages) if ages else float("inf")

    def _public_ttl(self, exchange: str, attr: str, cfg_default: float) -> float:
        ttl = None
        try:
            cfg = getattr(self, "cfg", None)
            slo_map = getattr(cfg, "slo", None) if cfg is not None else None
            if slo_map:
                g_cfg = getattr(cfg, "g", None)
                mode_key = str(getattr(g_cfg, "deployment_mode", "SPLIT")).upper()
                per_ex = slo_map.get(mode_key) or {}
                path_slo = per_ex.get(_norm_ex(exchange))
                if path_slo is not None and getattr(path_slo, "public", None) is not None:
                    ttl_val = float(getattr(path_slo.public, attr, 0.0) or 0.0)
                    if ttl_val > 0.0:
                        ttl = ttl_val
        except Exception:
            ttl = None

        if ttl is None:
            ttl = float(cfg_default)
        return float(ttl)

    def _risk_feeds_fresh(self, pair: str) -> tuple[bool, Optional[str], dict]:
        """
        Vérifie la fraicheur des flux auxiliaires (slippage/volatility).
        Optimisation HFT : En mode PROD, on délègue ce check au RiskManager 
        pour économiser du CPU sur le Hot Path. 
        """
        if not self.is_actually_dry:
            return True, None, {}

        # En mode DRY_RUN, on garde le check local pour la simulation
        ex = next(iter(self.exchanges or []), "")
        slip_ttl = self._public_ttl(ex, "slip_ttl_s", getattr(self.cfg.slip, "ttl_s", 5.0))
        vol_ttl = self._public_ttl(ex, "vol_ttl_s", getattr(self.cfg.vol, "ttl_s", 5.0))

        slip_age = self._slip_age_seconds(pair)
        vol_age = self._vol_age_seconds(pair)
        if not math.isfinite(slip_age) or slip_age > slip_ttl:
            return False, "slip_unknown_or_stale", {"age_s": slip_age, "ttl_s": slip_ttl}
        if not math.isfinite(vol_age) or vol_age > vol_ttl:
            return False, "vol_unknown_or_stale", {"age_s": vol_age, "ttl_s": vol_ttl}
        return True, None, {}

    def _resolve_exchange_region(self, exchange: str) -> str:
        exu = _norm_ex(exchange)
        rm = getattr(self, "risk_manager", None)
        rm_resolve = getattr(rm, "_resolve_exchange_region", None)
        if callable(rm_resolve):
            try:
                region = rm_resolve(exu)
                if region:
                    return str(region).upper()
            except Exception:
                pass
        cfg = getattr(self, "cfg", None)
        g_cfg = getattr(cfg, "g", None) if cfg is not None else None
        region_map = None
        if g_cfg is not None:
            for attr in ("exchange_region_map", "cex_region_map", "engine_region_map", "engine_pod_map"):
                mp = getattr(g_cfg, attr, None)
                if isinstance(mp, dict) and mp:
                    region_map = mp
                    break
        if region_map:
            if exu in region_map:
                return str(region_map[exu]).upper()
            for key, val in region_map.items():
                if str(key).upper() == exu:
                    return str(val).upper()
        return "UNKNOWN"

    def _jp_disabled(self, region: Optional[str]) -> bool:
        cfg = getattr(self, "cfg", None)
        g_cfg = getattr(cfg, "g", None) if cfg is not None else None
        enable_jp = bool(getattr(g_cfg, "enable_jp", False)) if g_cfg is not None else bool(
            getattr(cfg, "enable_jp", False)
        )
        region_norm = str(region or "UNKNOWN").upper()
        return (region_norm in {"JP", "UNKNOWN"}) and not enable_jp

    def _dynamic_thresholds_for(self, pair: str) -> tuple[float, float]:
        """
        (min_required_bps_boost, min_notional_mult)
        Charge élevée => on durcit d'abord AUDITION puis PRIMARY.
        """
        try:
            load = self._tb_load(self._tb_global)  # 0..1
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
            load = self._tb_load(self._tb_global)
        except Exception:
            load = 0.0
        shrink = 0.5 * min(1.0, max(0.0, (load - 0.5) / 0.4))  # 0 à load≤0.5 ; 0.5 à load≥0.9
        return max(60.0, float(self.audition_ttl_s) * (1.0 - shrink))


    # ---------------- Cœur évaluation ----------------
    def _evaluate_pair(self, buy_data: dict, sell_data: dict) -> None:
        _t0 = time.perf_counter()
        # Instrumentation latence (P1)
        tracing_enabled = should_trace_latency(self.cfg)
        
        now = time.time()
        pair = (buy_data.get("pair_key") or buy_data.get("symbol") or "").replace("-", "").upper()
        buy_ex = str((buy_data.get("exchange") or "")).upper()
        sell_ex = str((sell_data.get("exchange") or "")).upper()
        route_combo = f"{min(buy_ex, sell_ex)}/{max(buy_ex, sell_ex)}"
        quote = _quote_of_pair(pair)

        def _record_latency():
            eval_us = (time.perf_counter() - _t0) * 1_000_000
            # On ne loggue que les evaluations un peu lourdes
            if eval_us > 1000: # > 1ms
                 self.logger.info("[Scanner] Eval %s: %.2fus", pair, eval_us)

        def _book_fresh(snapshot: dict, ex_name: str) -> bool:
            # P0: On respecte les conditions de fraîcheur même en DRY_RUN 
            # pour permettre la calibration du bot sur l'infra cible.
            is_actually_dry = self.is_actually_dry

            if not snapshot.get("active", True):
                self._record_rejection(reason="inactive", route=route_combo, pair=pair, ctx={"exchange": ex_name})
                return False

            recv_ts_ms = snapshot.get("recv_ts_ms")
            if recv_ts_ms is None:
                self._record_rejection(
                    reason="schema_missing_field",
                    route=route_combo,
                    pair=pair,
                    ctx={"exchange": ex_name, "field": "recv_ts_ms"},
                )
                return False
            book_ttl_ms = snapshot.get("book_ttl_ms")
            
            # P0: Grâce initiale de 5s au démarrage du Scanner
            if (time.monotonic() - getattr(self, "_boot_ts", 0)) < 5.0:
                return True

            if book_ttl_ms is None:
                book_ttl_ms = self._book_ttl_ms_default(ex_name)

            if book_ttl_ms is None or float(book_ttl_ms) <= 0.0:
                self._record_rejection(
                    reason="schema_missing_field",
                    route=route_combo,
                    pair=pair,
                    ctx={"exchange": ex_name, "field": "book_ttl_ms"},
                )
                return False
            # P0: calcul d'âge robuste aux sauts NTP (wall clock)
            recv_mono_ns = snapshot.get("recv_mono_ns")
            try:
                if recv_mono_ns is not None:
                    age_ms = int((time.monotonic_ns() - int(recv_mono_ns)) / 1_000_000)
                    if age_ms < 0:
                        age_ms = 0
                else:
                    age_ms = int(time.time() * 1000) - int(recv_ts_ms)
            except Exception:
                self._record_rejection(
                    reason="schema_missing_field",
                    route=route_combo,
                    pair=pair,
                    ctx={"exchange": ex_name, "field": "recv_ts_ms/recv_mono_ns"},
                )
                return False
            # Diagnostic: si wall age est énorme mais mono age est OK => saut d'horloge probable
            if recv_mono_ns is not None:
                try:
                    age_wall = int(time.time() * 1000) - int(recv_ts_ms)
                    if age_wall > 5000 and age_ms < 500:
                        # Log échantillonné (1/sec max par exchange)
                        now_s = int(time.time())
                        k = f"_clockjump_last_{ex_name}"
                        if getattr(self, k, 0) != now_s:
                            setattr(self, k, now_s)
                            self.logger.warning(
                                "[Scanner][clock_jump?] ex=%s age_wall_ms=%s age_mono_ms=%s",
                                ex_name, age_wall, age_ms
                            )
                except Exception:
                    pass

            if age_ms > int(book_ttl_ms):
                self._record_rejection(
                    reason="book_stale",
                    route=route_combo,
                    pair=pair,
                    ctx={"exchange": ex_name, "age_ms": age_ms, "ttl_ms": int(book_ttl_ms)},
                )
                return False
            return True

        if not _book_fresh(buy_data, buy_ex) or not _book_fresh(sell_data, sell_ex):
            return

        # 1. FAST GATE : Prix et spread brut (Types natifs float pour la performance)
        try:
            buy_price = float(buy_data.get("best_ask") or 0)
            sell_price = float(sell_data.get("best_bid") or 0)
        except (ValueError, TypeError):
            return

        if buy_price <= 0 or sell_price <= 0 or sell_price <= buy_price:
            return

        buy_vol = float(buy_data.get("ask_volume") or 0)
        sell_vol = float(sell_data.get("bid_volume") or 0)
        if buy_vol <= 0 or sell_vol <= 0:
            return

        slip_ttl = min(
            self._public_ttl(buy_ex, "slip_ttl_s", getattr(self.cfg.slip, "ttl_s", 5.0)),
            self._public_ttl(sell_ex, "slip_ttl_s", getattr(self.cfg.slip, "ttl_s", 5.0)),
        )
        def _leg_age(ex_name: str) -> float:
            try:
                ts_val = (self._slip_cache.get((_norm_ex(ex_name), _norm_pair(pair))) or {}).get("ts")
                if ts_val:
                    return max(0.0, now - float(ts_val))
            except Exception:
                pass
            return float("inf")

        age_buy_ex = _leg_age(buy_ex)
        age_sell_ex = _leg_age(sell_ex)
        slip_age = max(age_buy_ex, age_sell_ex)
        if not math.isfinite(slip_age) or slip_age > slip_ttl:
            self.blocks["slip"] += 1
            self._record_rejection(
                reason="slip_unknown_or_stale",
                route=route_combo,
                pair=pair,
                ctx={"age_s": slip_age, "ttl_s": slip_ttl},
            )
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

        # 2. FAST GATE : Notionnels en quote + minNotional
        vol_buy_quote = buy_price * buy_vol
        vol_sell_quote = sell_price * sell_vol
        vol_possible_quote = min(vol_buy_quote, vol_sell_quote)

        val_min = None
        try:
            val_min = self.risk_manager.get_minimum_volume_required(buy_ex, pair)
        except Exception:
            return

        if val_min is None or float(val_min) <= 0.0:
            return
        
        min_notional = float(val_min)
        boost_bps_dyn, min_notional_mult_dyn = self._dynamic_thresholds_for(pair)
        
        if vol_possible_quote < (min_notional * float(min_notional_mult_dyn)):
            # On ne record que si c'est vraiment bas, sinon on drop silencieusement pour le CPU
            if vol_possible_quote < (min_notional * 0.5):
                self.blocks["min_notional"] += 1
                self._record_rejection(
                    reason="min_notional",
                    route=route_combo,
                    pair=pair,
                    ctx={
                        "vol_quote": vol_possible_quote,
                        "min_notional": min_notional
                    },
                )
            return

        # 3. Calcul du spread norm (float)
        spread_brut = sell_price - buy_price
        mid = (sell_price + buy_price) / 2.0
        spread_norm = spread_brut / mid

        # Coûts dynamiques (TAKER) — cache → RM
        def _slip(ex: str, side: str) -> Optional[float]:
            # 1) cache
            v = self._cached_slip(ex, pair, side)
            if v is None:
                # 2) source stricte via RM -> SFC
                try:
                    v = self.risk_manager.get_slippage(ex, pair, side=side)
                except Exception:
                    v = None

            if v is None:
                return None
            return float(v)

        # Fees strictes via RM (fraction, ex: 0.001); aucun 0 “de confort”
        try:
            fb = self.risk_manager.get_fee_pct(buy_ex, pair, "taker")
            fs = self.risk_manager.get_fee_pct(sell_ex, pair, "taker")
        except DataStaleError:
            self._record_rejection(
                reason="fee_unknown",
                route=route_combo,
                pair=pair,
                ctx={"buy_ex": buy_ex, "sell_ex": sell_ex},
            )
            return
        except Exception as e:
            self._record_rejection(
                reason="fee_unknown",
                route=route_combo,
                pair=pair,
                ctx={"err": type(e).__name__, "buy_ex": buy_ex, "sell_ex": sell_ex}
            )
            return
        if fb is None or fs is None or float(fb) <= 0.0 or float(fs) <= 0.0:
            return

        fees_buy_taker = float(fb)
        fees_sell_taker = float(fs)
        slip_buy = _slip(buy_ex, "buy")
        slip_sell = _slip(sell_ex, "sell")
        
        if slip_buy is None or slip_sell is None:
            self._record_rejection(
                reason="slip_unknown",
                route=route_combo,
                pair=pair,
                ctx={"buy_ex": buy_ex, "sell_ex": sell_ex},
            )
            return
            
        total_cost = fees_buy_taker + fees_sell_taker + slip_buy + slip_sell
        spread_net = spread_norm - total_cost

        # -------- VOL PATCH + NOUVEAU boost charge -------------------------
        vol_ttl = min(
            self._public_ttl(buy_ex, "vol_ttl_s", getattr(self.cfg.vol, "ttl_s", 5.0)),
            self._public_ttl(sell_ex, "vol_ttl_s", getattr(self.cfg.vol, "ttl_s", 5.0)),
        )
        vol_bps_scanner = self.get_volatility_bps(pair, ema=True, ttl_s=vol_ttl)
        vol_penalty_bps = 0.0
        if not isinstance(vol_bps_scanner, (int, float)):
            self._record_rejection(
                reason="vol_unknown",
                route=route_combo,
                pair=pair,
                ctx={"buy_ex": buy_ex, "sell_ex": sell_ex},
            )
            return
            
        excess = max(0.0, float(vol_bps_scanner) - self.vol_soft_cap_bps)
        vol_penalty_bps = excess * self.vol_alpha_penalty  # bps
        local_min_required_bps = float(self.min_required_bps) + self.vol_beta_min_req * excess

        # >>> NOUVEAU : boost bps sous charge (audition d'abord)
        local_min_required_bps += float(boost_bps_dyn)

        if (spread_net * 1e4) < local_min_required_bps:
            self.blocks["fast_prefilter"] += 1
            # On ne record que si on est proche du seuil ou si on veut débugger
            if (spread_net * 1e4) > (local_min_required_bps - 5.0):
                self._record_rejection(
                    reason="below_min_bps",
                    route=route_combo,
                    pair=pair,
                    ctx={
                        "net_bps": spread_net * 1e4,
                        "min_bps": local_min_required_bps
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
                reason="fee_unknown",
                route=route_combo,
                pair=pair,
                ctx={"stage": "tm_estimate"}
            )
            return

        slip_buy_bps = self._to_bps(slip_buy)
        slip_sell_bps = self._to_bps(slip_sell)

        ob_buy_asks = (buy_data.get("orderbook") or {}).get("asks") or []
        ob_sell_bids = (sell_data.get("orderbook") or {}).get("bids") or []

        need_buy_qty = (vol_possible_quote / buy_price) if buy_price > 0 else 0.0
        need_sell_qty = (vol_possible_quote / sell_price) if sell_price > 0 else 0.0

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

        vol_final = float(vol_bps_scanner)
        soft = float(self.vol_soft_cap_bps)
        chaos = float(self.vol_chaos_bps)
        vol_factor = 0.0
        vol_bps_rm = 0.0
        if vol_final > soft and chaos > soft:
            vol_factor = min(1.0, (vol_final - soft) / max(1e-9, (chaos - soft)))

        bias_k = float(self.hint_cfg.get("tm_vol_bias_k", 0.6))
        maker_queue_penalty = float(self.hint_cfg["tm_maker_queue_risk_bps"])
        maker_queue_penalty_eff = maker_queue_penalty * max(0.3, (1.0 - bias_k * vol_factor))

        tt_base = float(self.hint_cfg.get("tt_max_skew_ms", 200))
        tt_k = float(self.hint_cfg.get("tt_skew_vol_k", 1.0))
        tt_limit_ms_eff = tt_base * (1.0 + tt_k * vol_factor)

        spread_bps = spread_norm * 1e4

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

        # Pre-evaluation des hints MM pour permettre l'opportunisme même sans arb TT/TM
        mm_score = -1e9
        mm_hints = {}
        mm_eligible = False
        try:
            mm_mode = (getattr(self, "mm_mode", "OFF") or "OFF").upper()
            if mm_mode != "OFF" and self._should_consider_mm_for_pair(pair):
                ma = float((sell_data.get("best_ask") or 0.0))
                mb = float((buy_data.get("best_bid") or 0.0))
                mm_score, mm_hints = self._score_mm(
                    a_ex=sell_ex, b_ex=buy_ex, pair=pair, maker_px_a=ma, maker_px_b=mb,
                    a_data=sell_data, b_data=buy_data
                )
                mm_eligible = mm_score > -1e8
                SC_STRATEGY_SCORE.labels(pair, "MM").set(mm_score)
                SC_ELIGIBLE.labels(pair, "MM").set(1.0 if mm_eligible else 0.0)
        except Exception:
            pass

        if spread_net < 0.0 and tm_best_bps < 0.0 and not mm_eligible:
            self.blocks["neg_both"] += 1
            # Ici on est dans un entre-deux TT/TM/MM, on utilise MIXED ou le mode actuel
            self._record_rejection(reason="neg_both", route=route_combo, pair=pair,
                                   ctx={"tt_net_pct": spread_net * 100, "tm_best_bps": tm_best_bps, "mm_score": mm_score},
                                   strategy="MIXED")
            return

        tm_hint_min = float(self.hint_cfg["tm_hint_min_est_net_bps"])  # bps
        if spread_net < float(self.min_spread_net) and tm_best_bps < tm_hint_min and not mm_eligible:
            self.blocks["below_tm_hint"] += 1
            self._record_rejection(reason="below_tm_hint", route=route_combo, pair=pair,
                                   ctx={"tt_net_pct": spread_net * 100, "tm_best_bps": tm_best_bps,
                                        "tm_hint_min_bps": tm_hint_min, "mm_score": mm_score},
                                   strategy="MIXED")
            return

        # Sizing (depth-aware + cap + inventaire)
        target_quote = vol_possible_quote
        if self.max_notional_quote and self.max_notional_quote > 0:
            target_quote = min(target_quote, float(self.max_notional_quote))
        
        # min_notional déjà récupéré plus haut, on réutilise
        min_notional = float(val_min)

        size_ok = False
        attempts = 0
        while target_quote >= min_notional:
            try:
                valid = self.risk_manager.validate_inventory_bundle(
                    buy_ex=buy_ex, sell_ex=sell_ex, pair_key=pair,
                    vol_usdc_buy=target_quote, vol_usdc_sell=target_quote,
                    is_rebalancing=False,
                )
            except Exception:
                valid = False
            if valid:
                size_ok = True
                break
            target_quote *= 0.7
            attempts += 1
            if attempts > 5:
                break
        if not size_ok:
            self.blocks["inventory"] += 1
            self._record_rejection(reason="inventory", route=route_combo, pair=pair,
                                   ctx={"min_notional": min_notional})
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
        decision_ts_ms = int(now * 1000)
        buy_recv_ts_ms = int(buy_data.get("recv_ts_ms") or 0)
        sell_recv_ts_ms = int(sell_data.get("recv_ts_ms") or 0)
        recv_candidates = [ts for ts in (buy_recv_ts_ms, sell_recv_ts_ms) if ts > 0]

        book_ts_ms = int(min(recv_candidates)) if recv_candidates else 0
        book_age_ms = max(0, decision_ts_ms - book_ts_ms) if book_ts_ms > 0 else 0


        region_buy = self._resolve_exchange_region(buy_ex)
        region_sell = self._resolve_exchange_region(sell_ex)
        deployment_mode = str(getattr(getattr(self.cfg, "g", object()), "deployment_mode", "UNKNOWN")).upper()
        tier = self._cohort_of(pair)

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

        # Determination de la stratégie la plus probable pour l'observabilité
        if is_rebal:
            strat_label = "REB"
        elif mm_eligible and float(spread_net) < float(self.min_spread_net) and tm_best_bps < tm_hint_min:
            # Distinction Mono vs Cross pour routing alias
            if str(buy_ex).upper() == str(sell_ex).upper():
                strat_label = "MM_MONO"
            else:
                strat_label = "MM_CROSS"
        elif self.scanner_mode == "MIXED":
            if float(spread_net) >= float(self.min_spread_net):
                strat_label = "TT"
            elif tm_best_bps >= float(self.hint_cfg["tm_hint_min_est_net_bps"]):
                strat_label = "TM"
            else:
                strat_label = "MIXED"
        else:
            strat_label = self.scanner_mode

        payload = {
            "type": ("rebalancing" if is_rebal else "bundle"),
            "legs": [leg_buy, leg_sell],
            "pair_key": pair,
            "buy_ex": buy_ex,
            "sell_ex": sell_ex,
            "strategy": strat_label,
            "quote": quote,
            "quote_ccy": quote,
            "notional_quote": {"ccy": quote, "amount": float(target_quote)},
            "notional_quote_amount": float(target_quote),
            "volume_usdc": float(target_quote),
            "book_age_ms": int(book_age_ms),
            "book_ts_ms": int(book_ts_ms),
            "fees_buy": float(fees_buy_taker),
            "fees_sell": float(fees_sell_taker),
            "slip_buy": float(slip_buy),
            "slip_sell": float(slip_sell),
            "vol_bps": float(vol_bps_scanner),
            "decision_ts_ms": decision_ts_ms,
            "region_buy": region_buy,
            "region_sell": region_sell,
            "deployment_mode": deployment_mode,
            "tier": tier,
            "expected_net_spread": expected_net,
            "timeout_s": float(self.default_timeout_s),
            "meta": {"allow_loss_bps": allow_loss_bps} if allow_loss_bps > 0 else {},
            "opp_id": opp_id,
            "arb_id": arb_id,
            "t_detect": t_detect,
            "ts_buy_ex_ms": int(buy_data.get("exchange_ts_ms") or buy_data.get("recv_ts_ms") or 0),
            "ts_sell_ex_ms": int(sell_data.get("exchange_ts_ms") or sell_data.get("recv_ts_ms") or 0),
        }

        # --- REEQUILIBRAGE MM PAR DESACTIVATION DE LEG ---
        # Si on est en MM (Mono ou Cross), on vérifie si l'inventaire impose de couper un côté.
        if strat_label in ("MM_MONO", "MM_CROSS"):
            try:
                base_asset = pair.replace(quote, "")
                final_legs = []
                for leg in payload["legs"]:
                    side = leg["side"]
                    ex = leg["exchange"]
                    restriction = self.risk_manager.get_mm_leg_restriction(ex, strat_label, base_asset, pair_key=pair)
                    if restriction == side:
                        # Ce côté aggrave l'inventaire -> on le désactive (Self-rebal passif)
                        payload.setdefault("meta", {})["leg_restricted"] = side
                        self.logger.info("[Scanner] MM Rebalancing: disabling %s leg for %s on %s (inventory drift)", side, pair, ex)
                    elif restriction == "BOTH":
                        # Les deux côtés sont restreints (phase 3 ou imbalance adverse)
                        payload.setdefault("meta", {})["mm_blocked_reason"] = "STUCK_OR_ADVERSE"
                        final_legs = []
                        break
                    else:
                        final_legs.append(leg)
                
                if not final_legs:
                    # Les deux côtés sont restreints (rare) ou un seul leg restant était restreint
                    return

                payload["legs"] = final_legs
                if len(final_legs) == 1:
                    payload["meta"]["mm_mode"] = "SINGLE"
            except Exception:
                pass
        sim_snapshot = {
            "asks": list((buy_data.get("asks") or [])[: self._binance_depth_level]),
            "bids": list((sell_data.get("bids") or [])[: self._binance_depth_level]),
            "recv_ts_ms": int(book_ts_ms),

        }
        payload["sim_snapshot"] = sim_snapshot
        
        try:
            sim = getattr(self, "simulator", None)
            if sim:
                branch = payload.get("strategy") or "TT"
                SIM_PRIME_TOTAL.labels(branch=str(branch).upper()).inc()
                sim.prime(
                    branch=branch,
                    route_combo=route_combo,
                    pair=pair,
                    quote=quote,
                    notional_quote=float(target_quote),
                    book_snapshot=sim_snapshot,
                    vol_size_factor=None,
                )
                if hasattr(sim, "start"):
                    try:
                        sim.start()
                    except Exception:
                        pass
        except Exception:
            SIM_PRIME_ERROR_TOTAL.labels(branch=str(payload.get("strategy") or "TT")).inc()

        if self._jp_disabled(region_buy) or self._jp_disabled(region_sell):
            self._record_rejection(
                reason="region_disabled_jp",
                route=route_combo,
                pair=pair,
                ctx={"region_buy": region_buy, "region_sell": region_sell},
            )
            return

        required_fields = [
            "pair_key",
            "buy_ex",
            "sell_ex",
            "strategy",
            "quote",
            "notional_quote",
            "book_age_ms",
            "fees_buy",
            "fees_sell",
            "slip_buy",
            "slip_sell",
            "vol_bps",
            "decision_ts_ms",
        ]
        missing = [key for key in required_fields if payload.get(key) is None]
        notional_quote = payload.get("notional_quote")
        if not (isinstance(notional_quote, dict) and float(notional_quote.get("amount") or 0.0) > 0.0):
            missing.append("notional_quote")
        if missing:
            self._record_rejection(
                reason="missing_required_field",
                route=route_combo,
                pair=pair,
                ctx={"missing": sorted(set(missing))},
            )
            return

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

            # Hints MM (déjà évalués plus haut)
            if mm_eligible:
                hints["MM"] = mm_hints
                hints["MM_score"] = float(mm_score)
                hints.setdefault("expected_net_bps", {})
                hints["expected_net_bps"]["TT"] = float(spread_net * 1e4)
                hints["expected_net_bps"]["TM"] = float(tm_best_bps)
                hints["expected_net_bps"]["MM"] = float(mm_hints.get("net_bps", float("nan")))
            else:
                # On s'assure que SC_ELIGIBLE est à 0 si on arrive ici sans mm_eligible
                try: SC_ELIGIBLE.labels(pair, "MM").set(0.0)
                except: pass

        # Score net (pénalité VOL douce) — pondéré par la taille effectivement sélectionnée
        vol_penalty_frac = (vol_penalty_bps / 1e4) if vol_penalty_bps > 0 else 0.0
        score_net = (spread_net - vol_penalty_frac) * target_quote

        # Latence WS→Scanner (estimation robuste)
        # Latence WS→Scanner (estimation robuste, monotonic)
        now_mono_ns = time.monotonic_ns()
        buy_mono_ns = buy_data.get("recv_mono_ns")
        sell_mono_ns = sell_data.get("recv_mono_ns")
        if buy_mono_ns and sell_mono_ns:
            sec = max(0.0, (now_mono_ns - min(int(buy_mono_ns), int(sell_mono_ns))) / 1_000_000_000.0)
        else:
            now_mono = time.monotonic()
            t_buy = float(self._last_seen_books_by_ex.get(buy_ex, 0.0) or 0.0)
            t_sell = float(self._last_seen_books_by_ex.get(sell_ex, 0.0) or 0.0)
            last_both = min(t_buy, t_sell) if (t_buy and t_sell) else 0.0
            sec = max(0.0, now_mono - last_both) if last_both > 0 else 0.0
        latency_ws_to_scan_ms = int(round(sec * 1000.0))

        opp = {
            "opp_id": opp_id,
            # Propagation latence (P1)
            "router_entry_ns": buy_data.get("router_entry_ns") or sell_data.get("router_entry_ns"),
            "router_exit_ns": buy_data.get("router_exit_ns") or sell_data.get("router_exit_ns"),
            "scanner_entry_ns": buy_data.get("scanner_entry_ns") or sell_data.get("scanner_entry_ns"),
            "arb_id": arb_id,
            "pair": pair,
            "symbol": pair,
            "quote": quote,
            "buy_exchange": buy_ex,
            "sell_exchange": sell_ex,
            "route": {
                "buy_exchange": buy_ex,
                "sell_exchange": sell_ex,
            },
            "notional_quote": {
                "ccy": quote,
                "amount": float(target_quote),
            },
            "buy_price": float(buy_price),
            "sell_price": float(sell_price),
            "spread_brut_pct": float(spread_norm * 100),
            "spread_net_pct": float(spread_net * 100),
            "volume_top_buy": float(buy_vol),
            "volume_top_sell": float(sell_vol),
            "volume_possible_quote": float(vol_possible_quote),
            "volume_selected_quote": float(target_quote),
            "timestamp": now,
            "t_detect": t_detect,
            "t_scan_done": t_scan_done,
            "latency_ws_to_scan_ms": latency_ws_to_scan_ms,
            "score": round(float(score_net), 4),
            "type": "bundle",
            "branch_candidates": ["TT", "TM"] + (["MM"] if self._should_consider_mm_for_pair(pair) else []),
            "hints": hints,
            "payload": payload,
            "meta": {
                "route_combo": route_combo,
                "vol_penalty_bps": vol_penalty_bps,
                "min_required_bps_local": local_min_required_bps,
                "strategy": "standard",
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
                                      (float(self.average_spread) * (self.opportunity_count - 1)) + (spread_net * 100)
                              ) / self.opportunity_count

        mgr = getattr(self, "logger_historique_manager", None)
        rec = getattr(mgr, "record_opportunity", None) if mgr is not None else None
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
            strategy = payload.get("strategy", self.scanner_mode)
            inc_scanner_emitted(route=route_combo, pair=pair, strategy=strategy)
            inc_scanner_decision(pair=pair, strategy=strategy)
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

        # --- Scanner → RM : latence et statut appel RM ---------------------------
        ts_rm_ns = time.perf_counter_ns()
        ok_rm = False
        reason_rm = "no_callback"

        try:
            if tracing_enabled:
                opp["scanner_exit_ns"] = time.perf_counter_ns()
            cb = self.on_opportunity
            if cb:
                ack_timeout_s = float(getattr(getattr(self.cfg, "scanner", object()), "rm_ack_timeout_s", 0.0) or 0.0)
                if asyncio.iscoroutinefunction(cb):
                    # On ne bloque pas sur le RM : tâche async
                    task = asyncio.create_task(cb(opp))
                    ok_rm = True
                    reason_rm = "async_task"
                else:
                    result = cb(opp)
                    if inspect.isawaitable(result):
                        task = asyncio.create_task(result)
                        ok_rm = True
                        reason_rm = "async_task"
                    else:
                        task = None
                        ok_rm = True
                        reason_rm = "ok"

                if ack_timeout_s > 0.0 and task is not None:
                    async def _await_ack(wait_task: asyncio.Task) -> None:
                        try:
                            await asyncio.wait_for(wait_task, timeout=ack_timeout_s)
                        except asyncio.TimeoutError:
                            self._record_rejection(
                                reason="rm_ack_timeout",
                                route=route_combo,
                                pair=pair,
                                ctx={"timeout_s": ack_timeout_s},
                            )
                            try:
                                mark_scanner_to_rm_ts(
                                    ts_rm_ns,
                                    ok=False,
                                    reason="rm_ack_timeout",
                                    pair=pair,
                                    route=route_combo,
                                    region=region_buy,
                                    strategy=payload.get("strategy"),
                                )
                            except Exception:
                                pass
                        except Exception:
                            self._record_rejection(
                                reason="rm_enqueue_failed",
                                route=route_combo,
                                pair=pair,
                                ctx={"phase": "ack_wait"},
                            )

                    asyncio.create_task(_await_ack(task))
            else:
                # Pas de callback configuré → on logge et on marque en NOK
                reason_rm = "no_callback"
        except Exception as e:
            ok_rm = False
            reason_rm = "rm_enqueue_failed"
            self.logger.exception("on_opportunity callback error: %s", e)
            self._record_rejection(
                reason="rm_enqueue_failed",
                route=route_combo,
                pair=pair,
                ctx={"err": type(e).__name__},
            )
        finally:
            try:
                # Mesure Scanner → RM avec labels minimaux (pair, route)
                mark_scanner_to_rm_ts(
                    ts_rm_ns,
                    ok=ok_rm,
                    reason=reason_rm,
                    pair=pair,
                    route=route_combo,
                    region=region_buy,
                    strategy=payload.get("strategy"),
                )
            except Exception:
                # Observabilité best-effort
                pass

        dt_ms = (time.perf_counter() - _t0) * 1000.0
        dt_ms = (time.perf_counter() - _t0) * 1000.0

        # Canonique: histogramme par cohorte (PRIMARY/AUDITION)
        try:
            from modules.obs_metrics import RM_DECISION_MS
            if RM_DECISION_MS is not None and not hasattr(RM_DECISION_MS, "_MetricNoOp"):
                RM_DECISION_MS.labels(cohort=self._cohort_of(pair)).observe(dt_ms)
        except Exception:
            pass

        # Aliases / compat existante (vous les gardez)
        try:
            from modules.obs_metrics import SCANNER_DECISION_MS, SCANNER_EVAL_MS
            if SCANNER_DECISION_MS is not None and not hasattr(SCANNER_DECISION_MS, "_MetricNoOp"):
                SCANNER_DECISION_MS.observe(dt_ms)
            if SCANNER_EVAL_MS is not None and not hasattr(SCANNER_EVAL_MS, "_MetricNoOp"):
                SCANNER_EVAL_MS.labels(pair=pair, route=route_combo).observe(dt_ms)
        except Exception:
            pass
        
        # P0: Fin de mesure latence précise
        _record_latency()

    # ---------------- Pull mode: tick ----------------
    def tick(self) -> None:
        # pull-mode tick() : doit être fail-loud si books absents (sinon D7 = silence)
        books = self.get_books() or {}
        now = time.time()
        self.last_scan_time = now

        if not books:
            # Throttle: 1 rejet / 10s max
            last = float(getattr(self, "_last_no_books_rej_ts", 0.0) or 0.0)
            if (now - last) >= 10.0:
                setattr(self, "_last_no_books_rej_ts", now)
                try:
                    self._record_rejection(reason="no_books", route="*", pair="GLOBAL", ctx={"where": "tick"})
                except Exception:
                    pass
            # Heartbeat load (même si 0)
            try:
                gl = self._tb_load(self._tb_global) if hasattr(self, "_tb_global") else 0.0
                bump_scanner(load=float(gl))
            except Exception:
                pass
            return

        # Heartbeat load (pull-mode)
        try:
            gl = self._tb_load(self._tb_global) if hasattr(self, "_tb_global") else 0.0
            bump_scanner(load=float(gl))
        except Exception:
            pass

        pairs_to_scan = self._top_pairs()
        exs = [ex for ex in (self.exchanges or list(books.keys())) if ex]

        for pk in pairs_to_scan:
            if not pk:
                continue
            allowed, _ = self._should_scan_now(pk)
            if not allowed:
                continue

            for sell_ex in exs:
                for buy_ex in exs:
                    if _route_combo(buy_ex, sell_ex) not in self.enabled_combos:
                        continue

                    b_sell = (books.get(sell_ex, {}) or {}).get(pk) or {}
                    b_buy = (books.get(buy_ex, {}) or {}).get(pk) or {}
                    bid = float(b_sell.get("best_bid") or 0.0)
                    ask = float(b_buy.get("best_ask") or 0.0)
                    if bid <= 0 or ask <= 0 or bid <= ask:
                        continue

                    mid = 0.5 * (bid + ask)
                    gross = (bid - ask) / max(mid, 1e-12)
                    try:
                        cost = self._fee_cost_fast(buy_ex, sell_ex, pk)
                    except DataStaleError as e:
                        reason = str(e) if str(e) in {"fee_unknown",
                                                      "slip_unknown_or_stale"} else "prefilter_cost_unavailable"
                        self._record_rejection(
                            reason=reason,
                            route=_route_combo(buy_ex, sell_ex),
                            pair=_norm_pair(pk),
                            ctx={"err": type(e).__name__},
                        )
                        continue
                    net_now = gross - cost

                    local_min_req_bps = self.min_required_bps
                    vol_ttl = min(
                        self._public_ttl(buy_ex, "vol_ttl_s", getattr(self.cfg.vol, "ttl_s", 5.0)),
                        self._public_ttl(sell_ex, "vol_ttl_s", getattr(self.cfg.vol, "ttl_s", 5.0)),
                    )
                    vol_bps_scanner = self.get_volatility_bps(pk, ema=True, ttl_s=vol_ttl)
                    if not isinstance(vol_bps_scanner, (int, float)):
                        self._record_rejection(
                            reason="vol_unknown_or_stale",
                            route=_route_combo(buy_ex, sell_ex),
                            pair=_norm_pair(pk),
                            ctx={"ttl_s": vol_ttl},
                        )
                        continue
                    excess = max(0.0, float(vol_bps_scanner) - self.vol_soft_cap_bps)
                    local_min_req_bps = float(self.min_required_bps) + self.vol_beta_min_req * excess
                    boost_bps, _mult = self._dynamic_thresholds_for(pk)
                    min_req = (local_min_req_bps + boost_bps) / 1e4

                    if net_now <= min_req:
                        self._record_rejection(
                            reason="below_min_bps",
                            route=_route_combo(buy_ex, sell_ex),
                            pair=_norm_pair(pk),
                            ctx={"net": float(net_now), "min_req": float(min_req)},
                        )
                        continue

                    # On a une opp brute positive : on délègue à la pipeline normale
                    self.check_opportunity(pk)

    # ---------------- Status / lifecycle ----------------
    def _is_healthy(self) -> bool:
        return (time.time() - self.last_scan_time) < 5.0

    def get_status(self) -> dict:
        # calcule les paires actives comme avant
        self.active_pairs_count = sum(
            1 for ex_data in self.orderbooks.values()
            for _, data in ex_data.items()
            if data.get("active", True)
        )

        status = {
            "module": "OpportunityScanner",
            "healthy": self._is_healthy(),
            "last_update": self.last_scan_time,
            "details": f"{self.opportunity_count} opportunités, {self.active_pairs_count} paires actives",
            "metrics": {
                "scanner_global_load": float(self._tb_load(self._tb_global)),
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
            "banned": list(getattr(getattr(self, "_rot_mm", None), "_banned_until", {}).keys()),
            "seed": list(getattr(self.cfg, "mm_seed_pairs", []) or []),
            "sizes": {
                "primary": len(getattr(self._rot_mm, "primary", [])),
                "audition": len(getattr(self._rot_mm, "audition", [])),
                "banned": len(getattr(getattr(self, "_rot_mm", None), "_banned_until", {}) or {}),
            },
        }

        # ---- AJOUT 2/2: mm_rotation à la RACINE ----
        status["mm_rotation"] = {
            "primary": list(getattr(self._rot_mm, "primary", [])),
            "audition": list(getattr(self._rot_mm, "audition", [])),
            "banned": list(getattr(getattr(self, "_rot_mm", None), "_banned_until", {}).keys()),
            "seed": list(getattr(self.cfg, "mm_seed_pairs", []) or []),
        }

        return status

    async def start(self):
        if getattr(self, "_running", False):
            return
        workers = int(getattr(self, "_workers_target", 1))
        if workers <= 0:
            self.logger.info("[Scanner] start skipped (workers=0, pull-mode)")
            return
        self._running = True
        self._tasks = []

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

    def _apply_load_shedding(self, load: float, now: float) -> None:
        threshold = float(getattr(self, "_shed_load_threshold", 0.95))
        if load > threshold:
            if now > getattr(self, "_hard_shed_until", 0.0):
                self._shed_primary = max(
                    float(getattr(self, "_shed_primary_min", 0.5)),
                    float(getattr(self, "_shed_primary", 1.0)) * float(getattr(self, "_shed_primary_factor", 0.8)),
                )
                self._shed_audition = float(getattr(self, "_shed_audition_factor", 0.0))
                self._hard_shed_until = now + float(getattr(self, "_shed_cooldown_s", 3.0))
        elif getattr(self, "_hard_shed_until", 0.0) and now > getattr(self, "_hard_shed_until", 0.0):
            self._shed_primary = 1.0
            self._shed_audition = 1.0
            self._hard_shed_until = 0.0

    def _next_candidate_pair(self) -> Optional[str]:
        """
        Sélectionne la prochaine paire à scanner pour les workers (_scan_loop).

        Stratégie :
        1) Si des évènements existent dans _queues, on privilégie une paire
           avec du backlog (file non vide).
        2) Sinon on itère en round-robin sur _top_pairs(), qui reflète
           l’univers + la priorisation (CORE/PRIMARY/AUDITION/SANDBOX).
        """
        # 1) Backlog explicite (mode évènementiel, future-proof)
        try:
            queues = getattr(self, "_queues", {}) or {}
            non_empty = [(pk, dq) for pk, dq in queues.items() if dq]
        except Exception:
            non_empty = []

        if non_empty:
            # On choisit la paire avec la file la plus longue (plus efficace que sort)
            return max(non_empty, key=lambda kv: len(kv[1]))[0]

        # 2) Fallback : round-robin sur l'univers priorisé
        pairs = getattr(self, "_rr_pairs", None)
        idx = int(getattr(self, "_rr_index", 0))

        if not pairs:
            pairs = self._top_pairs()
            self._rr_pairs = list(pairs)
            self._rr_index = 0
            idx = 0

        if not pairs:
            return None

        if idx >= len(pairs):
            pairs = self._top_pairs()
            self._rr_pairs = list(pairs)
            self._rr_index = 0
            idx = 0
            if not pairs:
                return None

        pair = pairs[idx]
        self._rr_index = idx + 1
        return pair

    def _dedup_ok(self, pair: str) -> bool:
        """
        Garde-fou de déduplication courte par paire pour le mode worker.

        On évite de rescanner boucles infinies sur la même paire si un worker
        vient de la traiter il y a moins de `_dedup_window_s` secondes,
        même si aucune opp n’a été émise (pré-filtres bloquants, etc.).
        """
        window = float(getattr(self, "_dedup_window_s", 0.0) or 0.0)
        if window <= 0.0:
            return True

        try:
            pk = self._norm_pair(pair)
        except Exception:
            pk = (pair or "").replace("-", "").upper()

        now = time.monotonic()
        last = float(self._last_scan_ts_by_pair.get(pk, 0.0) or 0.0)
        if last and (now - last) < window:
            # On compte dans le bloc "dedup" mais sans spammer les logs
            try:
                self.blocks["dedup"] += 1
            except Exception:
                pass
            return False

        self._last_scan_ts_by_pair[pk] = now
        return True

    def _pop_latest_event(self, pair: str) -> Optional[Tuple[dict, bool]]:
        """
        Récupère le dernier évènement pour une paire dans le mode worker.

        - Si _queues contient des évènements pour cette paire → on prend le plus récent (LIFO).
        - Sinon, évènement synthétique basé sur le dernier snapshot orderbook connu.
        - Fallback Router: si cache interne vide mais Router a une vue books, on hydrate le cache.

        Retourne: (event_dict, is_synthetic)
        """
        try:
            pk = self._norm_pair(pair)
        except Exception:
            pk = (pair or "").replace("-", "").upper()

        # 1) File évènementielle
        dq = None
        try:
            queues = getattr(self, "_queues", {}) or {}
            dq = queues.get(pk)
        except Exception:
            dq = None

        if dq:
            # P1: Freshness check. Si la file est pleine d'événements vieux, on purge.
            now_mono_ns = time.monotonic_ns()
            # On réduit à 1500ms pour être plus agressif
            ttl_ms = 1500
            
            # --- AJOUT P1 : Si la file est trop longue (> 50), on drop tout sauf le dernier
            if len(dq) > 50:
                self._record_rejection(reason="backlog_purge", route="*", pair=pk, ctx={"len": len(dq)})
                ev = dq.pop()
                dq.clear()
                dq.append(ev)

            while dq:
                try:
                    ev = dq.pop() # LIFO
                    recv_mono_ns = ev.get("recv_mono_ns")
                    if recv_mono_ns is None:
                        recv_mono_ns = now_mono_ns
                        ev["recv_mono_ns"] = recv_mono_ns
                    age_ms = (now_mono_ns - int(recv_mono_ns)) // 1_000_000

                    if age_ms > ttl_ms:
                        continue
                    
                    # Instrumentation latence queue interne Scanner (P1)
                    if getattr(self.cfg.obs, "enable_latency_tracing", False):
                        exit_ns = ev.get("scanner_internal_exit_ns")
                        if exit_ns:
                            delay_ms = (time.perf_counter_ns() - int(exit_ns)) / 1e6
                            if getattr(self.cfg.obs, "enable_segment_metrics_scanner", True):
                                record_pipeline_latency("scanner_internal_queue", delay_ms, exchange=ev.get("exchange", "none"))

                    return ev, False
                except IndexError:
                    break

        # 2) Cache interne orderbooks
        now_mono_ns = time.monotonic_ns()
        for ex, per_pair in (self.orderbooks or {}).items():
            if pk in (per_pair or {}):
                data = per_pair[pk]
                recv_mono_ns = data.get("recv_mono_ns")
                if recv_mono_ns is None:
                    recv_mono_ns = now_mono_ns
                    data["recv_mono_ns"] = recv_mono_ns
                age_ms = (now_mono_ns - int(recv_mono_ns)) // 1_000_000

                if age_ms > 2000: # Cache trop vieux
                    continue
                return {"pair": pk, "exchange": ex, "ts": time.time()}, True

        # 3) Fallback Router → hydrate cache interne
        r = getattr(self, "router", None)
        fn = (
                getattr(r, "get_latest_orderbooks_light", None)
                or getattr(r, "get_latest_orderbooks", None)
                or getattr(r, "get_orderbooks", None)
        )
        if callable(fn):
            snap = fn() or {}
            try:
                # hydrate minimalement self.orderbooks[ex][pk]
                for ex, per_pair in (snap or {}).items():
                    if not isinstance(per_pair, dict):
                        continue
                    d = per_pair.get(pk)
                    if not isinstance(d, dict) or not d:
                        continue
                    exu = str(ex).upper()
                    self.orderbooks.setdefault(exu, {})[pk] = dict(d)
            except Exception:
                pass

            # si on a réussi à hydrater au moins un exchange pour pk → event synthétique
            try:
                for ex, per_pair in (self.orderbooks or {}).items():
                    if pk in (per_pair or {}):
                        return {"pair": pk, "exchange": "ROUTER", "ts": time.time()}, True
            except Exception:
                pass

        return None

    async def _scan_loop(self):
        await asyncio.sleep(random.random() * 0.03)  # jitter

        while getattr(self, "_running", False):
            try:
                gl = self._tb_load(self._tb_global) if hasattr(self, "_tb_global") else 0.0
                now = time.time()

                # Heartbeat load → D7 scanner_global_load (throttle ~4Hz)
                last_hb = float(getattr(self, "_last_load_metric_ts", 0.0) or 0.0)
                if (now - last_hb) >= 0.25:
                    setattr(self, "_last_load_metric_ts", now)
                    try:
                        bump_scanner(load=float(gl))
                    except Exception:
                        pass

                self._apply_load_shedding(gl, now)

                pair = self._next_candidate_pair()
                if not pair:
                    # Throttle 1 rejet / 30s max
                    last = float(getattr(self, "_last_no_pairs_rej_ts", 0.0) or 0.0)
                    if (now - last) >= 30.0:
                        setattr(self, "_last_no_pairs_rej_ts", now)
                        try:
                            self._record_rejection(reason="no_pairs", route="*", pair="GLOBAL",
                                                   ctx={"where": "scan_loop"})
                        except Exception:
                            pass
                    await asyncio.sleep(0.005)
                    continue

                res = self._pop_latest_event(pair)
                if not res:
                    # Fail-loud (sinon Grafana = "No data" et on ne sait pas pourquoi)
                    last = float(getattr(self, "_last_no_books_rej_ts", 0.0) or 0.0)
                    if (now - last) >= 10.0:
                        setattr(self, "_last_no_books_rej_ts", now)
                        try:
                            self._record_rejection(reason="no_books", route="*", pair=_norm_pair(pair),
                                                   ctx={"where": "scan_loop"})
                        except Exception:
                            pass
                    await asyncio.sleep(0.005)
                    continue

                ev, is_synthetic = res

                if is_synthetic:
                    allowed, _why = self._should_scan_now(pair)
                    if not allowed:
                        await asyncio.sleep(0.005)
                        continue
                    await asyncio.sleep(0.001)

                cohort = "PRIMARY" if pair in getattr(self, "_primary_pairs", set()) else "AUDITION"
                if cohort == "AUDITION" and getattr(self, "_shed_audition", 1.0) <= 0.0:
                    await asyncio.sleep(0.001)
                    continue
                if cohort == "PRIMARY" and getattr(self, "_shed_primary", 1.0) < 1.0:
                    if random.random() > getattr(self, "_shed_primary", 1.0):
                        await asyncio.sleep(0.001)
                        continue

                if not self._dedup_ok(pair):
                    await asyncio.sleep(0.001)
                    continue

                feeds_ok, feeds_reason, feeds_ctx = self._risk_feeds_fresh(pair)
                if not feeds_ok:
                    self._record_rejection(
                        reason=feeds_reason or "stale_risk_feeds",
                        route="*",
                        pair=_norm_pair(pair),
                        ctx=feeds_ctx,
                    )
                    await asyncio.sleep(0.001)
                    continue

                start_proc = time.perf_counter_ns()
                self._evaluate_routes_for_pair(pair, ev, enable_mm=self._enable_mm_hints)

                # Instrumentation latence Scanner (P1)
                tracing_enabled = should_trace_latency(self.cfg)

                if tracing_enabled:
                    proc_ms = (time.perf_counter_ns() - start_proc) / 1e6
                    if getattr(self.cfg.obs, "enable_segment_metrics_scanner", True):
                        record_pipeline_latency("scanner_proc", proc_ms, route="*", exchange=ev.get("exchange", "none"))

                await asyncio.sleep(0)

            except asyncio.CancelledError:
                break
            except Exception:
                self.logger.exception("[Scanner] worker error")
                await asyncio.sleep(0.01)


