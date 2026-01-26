"""
BotConfig — refactor unifié

Objectifs:
- Un *seul* point d'entrée de configuration: BotConfig.from_env()
- Sections par module + Globaux transversaux (profil, région, quotes, routes, anti-crossing)
- Overlays automatiques: profil / région (SPLIT/EU_ONLY) / quote / branches & routes / mode (DRY_RUN|PROD)
- Alias rétro-compat pour les clés historiques (ex: SLIP_TTL_S, router_shards...)
- Accès à plat pour l'ancien code: cfg.LEGACY_KEY via __getattr__

NB: Ce fichier n'importe aucun autre module de l'app (pour rester autonome).

"""
from __future__ import annotations

import os
import json
import ast
import logging
import hashlib
import time
import enum
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Tuple, Optional, Union
from contracts import payloads as contracts

# ------------------------------
# Taxonomie canonique (branches / profils capital)
# ------------------------------
# Utilisées comme référence unique par RiskManager, ExecutionEngine, etc.
ALLOWED_BRANCHES = ("TT", "TM", "MM", "MM_MONO", "MM_CROSS", "REB")
ALLOWED_CAPITAL_PROFILES = ("NANO", "MICRO", "SMALL", "MID", "LARGE")
class DeploymentMode(str, enum.Enum):
    EU_ONLY = "EU_ONLY"
    SPLIT = "SPLIT"
    JP_ONLY = "JP_ONLY"


DEPLOYMENT_MODES = {mode.value for mode in DeploymentMode}


# ------------------------------
# Helpers de parsing d'env
# ------------------------------

class _Env:
    TRUE = {"1","true","yes","on","y","t"}
    FALSE = {"0","false","no","off","n","f"}
    _deprecated: Dict[str, str] = {
        # Clés fantômes (aucun effet runtime)
        "CONFIG_OVERRIDES": "ignored (cfg.overrides not consumed)",
        "DISCOVERY_ENABLED_EXCHANGES": "ignored (discovery follows ENABLED_EXCHANGES)",
    }

    _deprecated_noted: set = set()

    @staticmethod
    def get(name: str, default: Optional[str]=None) -> Optional[str]:
        if name in _Env._deprecated:
            if name not in _Env._deprecated_noted and os.getenv(name) is not None:
                logging.getLogger(__name__).warning(
                    "Env %s is deprecated and ignored; %s",
                    name,
                    _Env._deprecated.get(name) or "remove it from configuration",
                )
                _Env._deprecated_noted.add(name)
            return default
        val = os.getenv(name)
        if val is None:
            return default

        # Nettoyage des commentaires de fin de ligne et espaces
        if "#" in val:
            val = val.split("#")[0]
        val = val.strip()
        return val if val else default

    @staticmethod
    def get_bool(name: str, default: bool=False) -> bool:
        v = _Env.get(name)
        if v is None:
            return default
        v2 = v.strip().lower()
        if v2 in _Env.TRUE:
            return True
        if v2 in _Env.FALSE:
            return False
        return default

    @staticmethod
    def get_int(name: str, default: int=0) -> int:
        v = _Env.get(name)
        try:
            return int(v) if v is not None else default
        except Exception:
            return default

    @staticmethod
    def get_float(name: str, default: float=0.0) -> float:
        v = _Env.get(name)
        try:
            return float(v) if v is not None else default
        except Exception:
            return default

    @staticmethod
    def get_list(name: str, default: Optional[List[str]]=None, sep: str = ",") -> List[str]:
        v = _Env.get(name)
        if v is None:
            return list(default or [])
        if isinstance(v, str) and not v.strip():
            return []
        # try JSON first
        try:
            x = json.loads(v)
            if isinstance(x, list):
                return x

        except Exception:
            pass
            # try Python literal list/tuple
            try:
                x = ast.literal_eval(v)
                if isinstance(x, (list, tuple)):
                    return list(x)
            except Exception:
                pass
        # fallback: CSV
        separator = "," if sep is None else sep
        return [t.strip() for t in str(v).split(separator) if t.strip()]

    @staticmethod
    def get_list_float(name: str, default: Optional[List[float]]=None) -> List[float]:
        v = _Env.get_list(name)
        if not v:
            return list(default or [])
        try:
            return [float(x) for x in v]
        except Exception:
            return list(default or [])

    @staticmethod
    def get_dict(name: str, default: Optional[Dict[str, Any]]=None) -> Dict[str, Any]:
        v = _Env.get(name)
        if v is None:
            return dict(default or {})
        try:
            x = json.loads(v)
            if isinstance(x, dict):
                return x
        except Exception:
            pass
        # last resort: Python literal
        try:
            x = ast.literal_eval(v)
            if isinstance(x, dict):
                return x
        except Exception:
            pass
        return dict(default or {})

    @staticmethod
    def get_routes(name: str, default: Optional[List[Tuple[str,str]]]=None) -> List[Tuple[str,str]]:
        v = _Env.get(name)
        if v is None:
            return list(default or [])
        try:
            x = json.loads(v)
            if isinstance(x, list):
                routes = []
                for item in x:
                    if isinstance(item, (list, tuple)) and len(item) == 2:
                        routes.append((str(item[0]), str(item[1])))
                return routes
        except Exception:
            pass
        # python literal
        try:
            x = ast.literal_eval(v)
            if isinstance(x, list):
                routes = []
                for item in x:
                    if isinstance(item, (list, tuple)) and len(item) == 2:
                        routes.append((str(item[0]), str(item[1])))
                return routes
        except Exception:
            pass
        # fallback: semicolon-separated pairs "A:B;C:D"
        pairs = []
        for chunk in v.split(';'):
            if ':' in chunk:
                a,b = chunk.split(':',1)
                a,b = a.strip(), b.strip()
                if a and b:
                    pairs.append((a,b))
        return pairs

class ConfigError(Exception):
    def __init__(self, reason_code: str, field: str) -> None:
        self.error_kind = "config_error"
        self.reason_code = contracts.normalize_reason_code(reason_code) or str(reason_code)
        self.field = field
        super().__init__(f"{self.reason_code}:{field}")

# ------------------------------
# Sections de config
# ------------------------------

@dataclass
class Globals:
    deployment_mode: str = "SPLIT"              # "EU_ONLY" | "SPLIT" | "JP_ONLY"
    pod_region: str = "EU"  # "EU" | "US" | "JP"
    enable_jp: bool = False
    exchange_region_map: Dict[str, str] = field(default_factory=lambda: {
        "BINANCE": "JP",
        "BYBIT": "JP",
        "COINBASE": "US",
    })
    engine_pod_map: Dict[str,str] = field(default_factory=lambda: {
        "EU": "http://127.0.0.1:8080",
        "US": "http://127.0.0.1:8080",
        "JP": "http://127.0.0.1:8080",
    })
    split_latency: Dict[str,int] = field(default_factory=lambda: {
        "skew_ms": 25,
        "base_delta_ms": 180,
        "stale_ms": 1300,
        "cb_coalesce_bonus_ms": 12,
    })
    eu_latency: Dict[str, int] = field(default_factory=lambda: {
        "skew_ms": 12,
        "base_delta_ms": 140,
        "stale_ms": 1200,
    })
    jp_latency: Dict[str, int] = field(default_factory=lambda: {
        "skew_ms": 18,
        "base_delta_ms": 160,
        "stale_ms": 1200,
    })
    capital_profile: str = "SMALL"              # NANO|MICRO|SMALL|MID|LARGE
    enabled_exchanges: List[str] = field(default_factory=lambda: ["BINANCE","BYBIT","COINBASE"])
    allowed_routes: List[Tuple[str,str]] = field(default_factory=lambda: [
        ("BINANCE","BYBIT"),("BYBIT","BINANCE"),
        ("COINBASE","BINANCE"),("BINANCE","COINBASE"),
        ("COINBASE","BYBIT"),("BYBIT","COINBASE"),
    ])
    pair_universe_mode: str = "AUTO"            # AUTO|LIST|REGEX
    pair_whitelist: List[str] = field(default_factory=list)
    pair_regex: Optional[str] = None

    primary_quote: str = "USDC"                 # "USDC"|"EUR"
    min_usdc: float = 1000.0
    wallet_alias_by_quote: Dict[str,str] = field(default_factory=lambda: {
        "USDC":"spot_usdc", "EUR":"spot_eur"
    })
    min_notional_by_exchange_quote: Dict[str, float] = field(default_factory=lambda: {
        "BINANCE:USDC": 10.0,
        "BYBIT:USDC": 10.0,
        "COINBASE:USDC": 10.0,
        "BINANCE:EUR": 10.0,
        "BYBIT:EUR": 10.0,
        "COINBASE:EUR": 10.0,
    })
    pairs: List[str] = field(default_factory=list)  # usato dal Boot come fallback quando discovery è vuota/off

    enable_branches: Dict[str,bool] = field(default_factory=lambda: {"tt":True,"tm":True,"mm":False,"reb":True})
    branch_priority: List[str] = field(default_factory=lambda: ["tt","tm","reb","mm"])  # ordre
    branch_budgets_quote: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
        "USDC": {"TT":0.60, "TM":0.35, "MM":0.00, "REB":0.05},
        "EUR":  {"TT":0.60, "TM":0.35, "MM":0.00, "REB":0.05},
    })

    frontload_enabled: bool = True
    frontload_weights: List[float] = field(default_factory=lambda: [0.50,0.35,0.15])
    frontload_group_size: int = 3
    min_fragment_quote: Dict[str, float] = field(default_factory=lambda: {"USDC":200.0, "EUR":200.0})
    guards: Dict[str, float] = field(default_factory=lambda: {
        "anchor_max_staleness_ms": 1200,
        "anchor_halve_guard_ms": 300,
        "max_price_deviation_pct": 0.50,
    })
    vol_slip_ttl: Dict[str, int] = field(default_factory=lambda: {"slip_s": 2, "vol_s": 5})

    # Anti-crossing (makers-only)
    ac: Dict[str, Any] = field(default_factory=lambda: {
        "enabled": True,
        "scope": "pod",               # "pod" | "cluster"
        "price_band_bps": 1.5,
        "ttl_ms": 700,
        "backend": "",                # ex: redis://host:6379/0
        "namespace": "prod-eu-us",
        "on_violation": "cancel",     # cancel|skip|widen
        "band_width_ticks": 1,
        "ttl_ms_binance": 200,
        "ttl_ms_bybit": 200,
        "ttl_ms_coinbase": 200,
        "strategy_tm": "reprice",
        "strategy_mm": "delay",
        "reprice_ticks_tm": 1,
        "reprice_ticks_mm": 1,
        "delay_ms_tm": 60.0,
        "delay_ms_mm": 90.0,
        "pods_coord_url": None,
        "pods_coord_namespace": "ac",
    })

    pacer_mode: str = "NORMAL"
    mode: str = "DRY_RUN"  # DRY_RUN | PROD
    live_trading_armed: bool = False
    restart_mode: str = "HYBRID"
    ws_reload_cap_per_hour: Optional[int] = None
    ws_reload_cooldown_s: Optional[int] = None
    ws_reload_mute_s: Optional[int] = None
    full_restart_cap_per_hour: Optional[int] = None
    full_restart_cooldown_s: Optional[int] = None
    full_restart_mute_s: Optional[int] = None
    restart_escalate_after_fails: int = 5
    restart_escalate_delay_s: float = 300.0
    restart_max_downtime_s: float = 3600.0
    feature_switches: Dict[str,bool] = field(default_factory=lambda: {
        "private_ws": False,
        "balance_fetcher": False,
        "engine_real": False,
        "simulator": True,
    })
    log_dir: str = "logs"
    history_dir: str = "history"
# --- Observabilité / Alerting ---

@dataclass
class ObservabilityCfg:
    strict_obs: bool = False
    log_level: str = "INFO"
    status_port: int = 9110
    expose_metrics_on_status: bool = True
    enable_obs_port: bool = False
    obs_port: int = 9108

    # Latency Tracing (End-to-End)
    enable_latency_tracing: bool = True
    latency_sampling_rate: float = 1.0
    enable_segment_metrics_router: bool = True
    enable_segment_metrics_scanner: bool = True
    enable_segment_metrics_rm: bool = True
    enable_segment_metrics_engine: bool = True

@dataclass
class BootCfg:
    scanner_proxy_mode: str = "REJECT"  # STRICT_ORDER | BUFFER | REJECT
    scanner_proxy_buffer_maxlen: int = 5000

@dataclass
class TelegramCfg:
    enabled: bool = True
    bot_token: str = ""
    allowed_user_ids: List[int] = field(default_factory=list)
    chat_id_info: Optional[str] = None
    chat_id_warn: Optional[str] = None
    chat_id_crit: Optional[str] = None
    ack_pin: Optional[str] = None
    require_ack: bool = False


@dataclass
class WebhookCfg:
    url: str = ""
    hmac_secret: str = ""
    timeout_s: float = 5.0


@dataclass
class AlertingCfg:
    telegram: TelegramCfg = field(default_factory=TelegramCfg)
    webhook: WebhookCfg = field(default_factory=WebhookCfg)

@dataclass
class FlowSafetyConfig:
    """
    Profils de sécurité des flows (hedge/unwind/core/opportunistic...).

    - ALWAYS_ON : à préserver même en mode très dégradé (hedge/unwind).
    - LUXURY    : à couper en premier (opportunistic).
    - NORMAL    : flows standard soumis aux overlays.
    """

    profiles: Dict[str, str] = field(
        default_factory=lambda: {
            "hedge": "ALWAYS_ON",
            "unwind": "ALWAYS_ON",
            "rebalance": "NORMAL",
            "core": "NORMAL",
            "opportunistic": "LUXURY",
            "maintenance": "NORMAL",
        }
    )


# --- Router ---
@dataclass
class RouterCfg:
    coalesce_window_ms: int = 20
    stale_ms: int = 1200
    # shards_per_exchange = (BINANCE, BYBIT, COINBASE)
    shards_per_exchange: Tuple[int, int, int] = (2, 1, 1)
    ws_shards_by_exchange: Dict[str, int] = field(
        default_factory=lambda: {"BINANCE": 1, "BYBIT": 1, "COINBASE": 1}
    )
    staleness_mode: str = "WALLCLOCK"  # WALLCLOCK | EVENT_TIME
    deterministic_backpressure: bool = False
    out_queues_maxlen: int = 20000
    # P0: quotas stream-centrics optionnels par kind (combo/vol/slip/health)
    # ex: {"combo": 5000, "vol": 2000, "slip": 2000, "health": 2000}
    out_queues_maxlen_by_kind: Dict[str, int] = field(default_factory=dict)
    # P0: cadence cible ≈ 5 ms
    mux_poll_ms: int = 5
    require_l2_first: bool = True
    vol_onchange_bps: float = 2.5
    slip_onchange_bps: float = 7.0
    hb_onchange_bps: float = 1.0
    coalesce_maxlen: int = 3
    ws_source_backpressure_cooldown_s: float = 5.0
    # P0: matrix de comportement par tier, pilotée par env (ROUTER_DROP_POLICY)
    drop_policy: Dict[str, Any] = field(default_factory=lambda: {
        "CORE": "never",
        "PRIMARY": "conservative",
        "AUDITION": "best_effort",
        "SANDBOX": "drop_when_backlog",
    })
    scanner_queue_maxlen: int = 512
    scanner_queue_drop_policy: str = "DROP_OLDEST"  # DROP_OLDEST | DROP_NEW
    fail_fast_on_event_exception: bool = False

    # P0: paramètres de backpressure Router (ROUTER_BACKPRESSURE)
    deque_maxlen_per_ex: Dict[str, int] = field(default_factory=lambda: {
        "DEFAULT": 8,
        "BINANCE": 8,
        "BYBIT": 8,
        "COINBASE": 10,
    })
    backpressure: Dict[str, Any] = field(default_factory=lambda: {
        "HIGH_WM_RATIO": 0.75,
        "BP_COALESCE_BUMP_MS": 8,
        "BP_DEQUE_GROW": 8,
        "COOLDOWN_S": 5.0,
    })
    # P0: bump spécifique Coinbase en SPLIT (ROUTER_CB_COALESCE_BUMP_MS)
    cb_coalesce_bump_ms: int = 10
    # P0: plafond Hz par topic (ROUTER_TOPIC_MAX_HZ)
    topic_max_hz: Dict[str, float] = field(default_factory=dict)

# --- WS Public ---
@dataclass
class WsPublicCfg:
    ws_backoff: Dict[str,int] = field(default_factory=lambda: {"base_ms":500, "max_ms":15_000, "jitter":1})
    ws_backoff_seed: Optional[int] = None
    update_pairs_jitter_ms: int = 200
    connect_timeout_s: int = 10
    read_timeout_s: int = 30
    ping_interval_s: int = 20
    pong_timeout_s: int = 10
    auto_resubscribe: bool = True
    max_retries: int = 0  # 0 = infini (avec backoff)
    out_queue_put_timeout_s: float = 0.05
    staleness_interval_s: float = 1.0
    staleness_slo_s: Optional[float] = None
    disabled_exchanges: List[str] = field(default_factory=list)

    # NEW: Parité de connexion par exchange
    connect_timeout_s_by_ex: Dict[str, int] = field(default_factory=dict)
    read_timeout_s_by_ex: Dict[str, int] = field(default_factory=dict)
    ping_interval_s_by_ex: Dict[str, int] = field(default_factory=dict)
    pong_timeout_s_by_ex: Dict[str, int] = field(default_factory=dict)

    # P0: Décimation Coinbase L2 (Hz)
    coinbase_l2_max_hz: float = 20.0
    # P0: Décimation Bybit L2 (Hz)
    bybit_l2_max_hz: float = 20.0

# --- Watchdogs ---
@dataclass
class LHMWatchdogCfg:
    LHM_SLO_PIPELINE_LAG_MAX_SECONDS_TARGET: float = 5.0
    LHM_SLO_DROPPED_TRADES_BUDGET: float = 0.0

@dataclass
class WatchdogCfg:
    interval_s: float = 2.0
    cooldown_s: float = 5.0
    persistence_s: float = 120.0
    persistence_cycles: int = 3
    notify_only_default: bool = True

    # Champs pour CentralWatchdog (CWConfig)
    mode: str = "MANUAL"
    dedup_ttl_s: int = 10
    rate_limit_rps: float = 1.0
    rate_burst: int = 5
    reminder_every_s: int = 300
    status_history_size: int = 500
    lhm: LHMWatchdogCfg = field(default_factory=LHMWatchdogCfg)

    # Watchdogs individuels
    router_interval_s: float = 2.0
    router_health_stale_ms: int = 1300
    router_health_min_coverage_ratio: float = 0.80
    router_health_queue_max: int = 2000
    router_r2s_warn_ms: int = 13
    router_r2s_crit_ms: int = 25
    router_backlog_warn: float = 0.80
    router_backlog_crit: float = 0.95
    router_escalate_after_cycles: int = 3

    ws_public_interval_s: float = 2.0
    ws_public_stale_warn_ms: int = 900
    ws_public_stale_crit_ms: int = 1200
    ws_public_hb_gap_warn_s: int = 15
    ws_public_hb_gap_crit_s: int = 30
    ws_public_resub_warn_per_min: int = 3
    ws_public_resub_crit_per_min: int = 6
    ws_public_escalate_after_cycles: int = 3

    private_ws_interval_s: float = 2.0
    private_ws_hb_warn_s: int = 20
    private_ws_hb_crit_s: int = 35
    private_ws_ack_warn_ms: int = 150
    private_ws_ack_crit_ms: int = 220
    private_ws_fill_warn_ms: int = 200
    private_ws_fill_crit_ms: int = 280
    private_ws_queue_warn_ratio: float = 0.70
    private_ws_queue_crit_ratio: float = 0.90
    private_ws_rate429_warn: float = 0.001
    private_ws_rate429_crit: float = 0.002
    private_ws_dedup_warn: float = 0.20
    private_ws_dedup_crit: float = 0.35
    private_ws_escalate_after_cycles: int = 3
    private_ws_stale_ms: int = 35000

    engine_interval_s: float = 2.0
    engine_submit_ack_warn_ms: int = 130
    engine_submit_ack_crit_ms: int = 200
    engine_ack_fill_warn_ms: int = 180
    engine_ack_fill_crit_ms: int = 260
    engine_timeouts_warn_per_min: float = 0.5
    engine_timeouts_crit_per_min: float = 1.5
    engine_retries_warn_per_min: float = 1.0
    engine_retries_crit_per_min: float = 3.0
    engine_panic_hedge_warn_per_min: float = 0.5
    engine_panic_hedge_crit_per_min: float = 2.0
    engine_queuepos_blocked_warn_per_min: float = 1.0
    engine_queuepos_blocked_crit_per_min: float = 3.0
    engine_escalate_after_cycles: int = 3
    engine_queue_max: int = 200
    engine_blocked_ms: int = 30000

    balance_interval_s: float = 5.0
    balance_stale_s: float = 60.0
    balance_error_threshold: int = 5

    logger_interval_s: float = 2.0
    logger_trade_queue_warn: int = 200
    logger_trade_queue_crit: int = 1000
    logger_queue_stuck_checks: int = 5
    logger_writer_stall_s: float = 10.0
    logger_min_queue_for_writer_stall: int = 10
    logger_rotation_stall_factor: float = 3.0
    logger_rotation_hard_stall_factor: float = 6.0

    slippage_interval_s: float = 2.0
    slippage_age_warn_s: float = 2.0
    slippage_age_crit_s: float = 3.0
    slippage_p95_warn_bps: float = 15.0
    slippage_p99_crit_bps: float = 30.0
    slippage_escalate_after_cycles: int = 3

    volatility_interval_s: float = 2.0
    volatility_age_warn_s: float = 5.0
    volatility_age_crit_s: float = 8.0
    volatility_soft_cap_bps: float = 50.0
    volatility_hard_cap_bps: float = 80.0
    volatility_p95_warn_bps: float = 40.0
    volatility_p99_crit_bps: float = 70.0
    volatility_z_warn: float = 2.5
    volatility_z_crit: float = 3.5
    volatility_escalate_after_cycles: int = 3

    opportunity_interval_s: float = 2.0
    opportunity_effective_ratio_warn: float = 0.70
    opportunity_effective_ratio_crit: float = 0.40
    opportunity_decision_p95_warn_ms: int = 8
    opportunity_decision_p95_crit_ms: int = 15
    opportunity_emit_p95_warn_ms: int = 12
    opportunity_emit_p95_crit_ms: int = 25
    opportunity_backlog_warn_ratio: float = 0.80
    opportunity_backlog_crit_ratio: float = 0.95
    opportunity_dedup_warn_per_min: float = 5.0
    opportunity_dedup_crit_per_min: float = 15.0
    opportunity_rejection_ratio_warn: float = 0.60
    opportunity_rejection_ratio_crit: float = 0.80
    opportunity_slip_age_warn_s: float = 2.0
    opportunity_slip_age_crit_s: float = 3.0
    opportunity_vol_age_warn_s: float = 4.0
    opportunity_vol_age_crit_s: float = 6.0
    opportunity_fees_age_warn_s: float = 120.0
    opportunity_fees_age_crit_s: float = 300.0
    opportunity_scanner_err_warn_per_min: float = 0.5
    opportunity_scanner_err_crit_per_min: float = 2.0
    opportunity_escalate_after_cycles: int = 3

    discovery_interval_s: float = 5.0
    discovery_min_change_ratio: float = 0.05
    discovery_confirm_ticks: int = 2
    discovery_dwell_ticks: int = 2
    discovery_max_refresh_gap_s: int = 600

    rm_interval_s: float = 2.0
    rm_tick_warn_ms: int = 10
    rm_tick_crit_ms: int = 20
    rm_hb_warn_s: int = 5
    rm_hb_crit_s: int = 15
    rm_slip_age_warn_s: float = 2.0
    rm_slip_age_crit_s: float = 3.0
    rm_vol_age_warn_s: float = 4.0
    rm_vol_age_crit_s: float = 6.0
    rm_shadow_bias_warn_bps: float = 3.0
    rm_shadow_bias_crit_bps: float = 5.0
    rm_queuepos_warn_per_min: float = 3.0
    rm_queuepos_crit_per_min: float = 8.0
    rm_severe_warn_s: int = 120
    rm_severe_crit_s: int = 600
    rm_escalate_after_cycles: int = 3

# --- Discovery ---
@dataclass
class DiscoveryCfg:
    """
    Configuration de la découverte d'univers (marché public).
    Toutes les constantes métier (volumes, quotes, listes) transitent par cette structure.
    """
    # Paramètres techniques d'appel API
    http_timeout_s: int = 10
    retry_policy: Dict[str, int] = field(default_factory=lambda: {"retries": 3, "backoff_s": 1})
    max_inflight_requests: int = 8

    # Quotes autorisées pour l'univers (USDC / EUR par défaut)
    quotes_allowed: List[str] = field(default_factory=lambda: ["USDC", "EUR"])

    # Seuils de volume globaux
    min_24h_volume_usd: float = 100_000.0

    # Seuils de volume par quote (overrides explicites)
    min_quote_volume_usdc: Optional[float] = None
    min_quote_volume_eur: Optional[float] = None
    # Map générique par quote (e.g. {"USDC": 5e6, "EUR": 2e6})
    min_quote_volume_by_quote: Dict[str, float] = field(default_factory=dict)

    # Listes explicites pilotant l'univers
    whitelist: List[str] = field(default_factory=list)
    blacklist: List[str] = field(default_factory=list)
    enabled: bool = True

    # Paramètres P0 Marché Public
    # - top_n : cap global du nombre de paires actives en sortie Discovery
    top_n: int = 120

    # Meta-informations de mode / exchanges actifs
    # (recopiées depuis Globals pour éviter les relectures d'ENV partout)
    deployment_mode: str = "EU_ONLY"
    enabled_exchanges: List[str] = field(default_factory=list)

    # Ratios pour les quotes spécifiques
    # - eur_quote_volume_factor : fraction du seuil global appliquée à EUR si pas d'override
    # - min_quote_volume_floor : plancher absolu (anti-valeurs ridicules)
    eur_quote_volume_factor: float = 0.30
    min_quote_volume_floor: float = 1.0


# --- Scanner ---
# ---------------------------------------------------------------------------
# Router / Scanner & RiskManager configs
# ---------------------------------------------------------------------------


# --- Scanner ---

@dataclass
class ScannerCfg:
    # Workers et backpressure de base
    workers: int = 1
    backpressure_log_every: int = 1000
    max_opportunities: int = 1000

    # Load shedding pilotable
    shed_load_threshold: float = 0.95
    shed_primary_factor: float = 0.8
    shed_primary_min: float = 0.5
    shed_audition_factor: float = 0.0
    shed_cooldown_s: float = 3.0

    # Mode principal du Scanner (TT/TM/MIXED)
    scanner_mode: str = "MIXED"

    # P0: dedup & fenêtres de scan
    dedup_window_s: float = 0.18  # cible P0 ≈ 0.12–0.20 s
    dedup_cooldown_s: float = 0.16
    max_pairs_per_tick: int = 150
    scan_interval: float = 0.02
    max_time_skew_s: float = 0.3
    rm_ack_timeout_s: float = 0.0

    # P0: dimensionnement des deques par tier
    deque_max_core: int = 1800
    deque_max_primary: int = 1800
    deque_max_audition: int = 600
    deque_max_sandbox: int = 300

    # P0: fréquences d’évaluation par tier
    scanner_eval_hz_primary: float = 30.0
    scanner_eval_hz_core: float = 32.0
    scanner_eval_hz_audition: float = 5.0
    scanner_eval_hz_sandbox: float = 2.0
    scanner_global_eval_hz: float = 200.0
    scanner_eval_burst_factor: float = 2.0
    scanner_global_eval_burst_factor: float = 2.0

    # MM / hints publics (Scanner = couche "soft" pour le MM)
    # - mm_depth_min_quote / mm_qpos_max_ahead_quote : filtres de profondeur / queue pour les hints MM
    # - mm_min_net_bps / mm_hedge_cost_bps         : net bps minimal pour considérer un setup MM
    # - mm_vol_bps_max                             : seuil de vol (en bps Monitor) au-dessus duquel
    #   le Scanner désactive les opportunités MM côté hints (le RM reste la couche "hard").

    enable_mm_hints: bool = True
    binance_depth_level: int = 50
    mm_rotation_enabled: bool = False
    mm_use_pairhistory: bool = True
    mm_seed_pairs: Tuple[str, ...] = ()
    mm_depth_min_quote: float = 200.0
    mm_qpos_max_ahead_quote: float = 5000.0
    mm_min_net_bps: float = 0.0006
    mm_hedge_cost_bps: float = 3.0
    mm_vol_bps_max: float = 40.0

    # Rebalancing
    allow_loss_bps_rebal: float = 0.0


# --- Risk Manager ---
@dataclass
class RmSwitchKnobs:
    enter_hyst_s: int = 180
    exit_hyst_s: int = 120
    mode_timeout_s: int = 1800
    opp_volume_timeout_s: int = 1800
    net_floor_bps: float = 4.5
    opp_age_fallback_s: float = 9999.0
    opp_vol_slip_age_s_max: float = 1.2
    opp_vol_p95_bps_max: float = 40.0
    opp_shadow_p50_bps_max: float = 2.5
    opp_vol_exit_slip_age_s_max: float = 1.6
    oppvol_p95_bps_min: float = 80.0
    pnl_guard_day_lvl1_pct: float = -0.3
    pnl_guard_day_lvl2_pct: float = -0.7
    pnl_cooldown_s: int = 1800
    pnl_guard_recover_floor_pct: float = -0.05
    normal_tt_min_bps_delta: float = 0.0
    normal_tm_min_bps_delta: float = 0.0
    normal_cap_factor: float = 1.0
    normal_ioc_only: bool = False
    constr_tt_min_bps_delta: float = 1.0
    constr_tm_min_bps_delta: float = 1.0
    constr_cap_factor: float = 0.5
    constr_mm_enable: bool = True
    constr_ioc_only: bool = False
    opp_volume_tt_min_bps_delta: float = -2.0
    opp_volume_tm_min_bps_delta: float = -2.0
    opp_volume_cap_factor: float = 1.0
    opp_volume_mm_enable: bool = True
    opp_vol_tt_min_bps_delta: float = 3.0
    opp_vol_tm_min_bps_delta: float = 6.0
    opp_vol_cap_factor: float = 0.7
    opp_vol_mm_enable: bool = False
    severe_tt_min_bps_delta: float = 5.0
    severe_tm_min_bps_delta: float = 8.0
    severe_cap_factor: float = 0.5
    severe_mm_enable: bool = False
    severe_ioc_only: bool = True
    rm_reco_miss_per_minute_constrained: int = 30
    rm_reco_miss_per_minute_severe: int = 60
    rm_pws_heartbeat_gap_constrained_s: float = 6.0
    rm_pws_heartbeat_gap_severe_s: float = 12.0
    rm_pws_event_lag_constrained_ms: float = 600.0
    rm_pws_event_lag_severe_ms: float = 1200.0
    rm_pws_ack_latency_constrained_ms: float = 250.0
    rm_pws_ack_latency_severe_ms: float = 450.0
    rm_pws_fill_latency_constrained_ms: float = 700.0
    rm_pws_fill_latency_severe_ms: float = 1200.0
    rm_fee_token_min_pct: float = 5.0
    rm_fee_low_min_seconds: float = 60.0
    rm_invariant_strict: bool = False
    split_breach_thr_base_ms: float = 180.0
    split_breach_thr_skew_ms: float = 40.0
    split_breach_thr_stale_ms: float = 1300.0
    split_breach_min_s: float = 3.0
    split_fallback_cooldown_s: float = 60.0
    split_restore_stable_s: float = 20.0
    split_penalty_bps_max: float = 6.0


@dataclass
class RmSignalPolicy:
    require_vol_signal_for_opp: bool = True
    require_shadow_for_opp: bool = True
    require_rl_health_for_opp: bool = True
    require_pws_health_for_opp: bool = True
    require_book_age_for_opp: bool = True
@dataclass
class RiskManagerCfg:
    # Utilisé par le chantier M6-B (REB via hints, RM → Simu → Engine)
    enable_tt: bool = True
    enable_tm: bool = True
    enable_mm: bool = False
    enable_reb: bool = True
    base_min_bps: float = 20.0
    dynamic_k: float = 0.3
    min_bps_floor: float = 10.0
    min_bps_cap: float = 60.0
    ttl_factor_tt_degraded: float = 0.5
    ttl_factor_tm_degraded: float = 0.4
    ttl_factor_reb_degraded: float = 0.3
    ttl_factor_mm_degraded: float = 0.0
    mode_tick_interval_s: float = 1.0
    enable_maker_maker: bool = False
    ff_trading_state_unified: bool = False
    branch_priority: List[str] = field(default_factory=lambda: ["tt","tm","reb","mm"])
    branch_budgets_quote: Dict[str, Dict[str,float]] = field(default_factory=lambda: {
        "USDC": {"TT":0.60, "TM":0.35, "MM":0.00, "REB":0.05},
        "EUR":  {"TT":0.60, "TM":0.35, "MM":0.00, "REB":0.05},
    })
    # --- SPLIT (EU/US) : thresholds & caps ---
    # Utilisé par le RM pour détecter une dérive SPLIT (latence/skew/stale) et appliquer une pénalité/cutover.
    split_breach_thr_base_ms: float = 180.0
    split_breach_thr_skew_ms: float = 40.0
    split_breach_thr_stale_ms: float = 1300.0
    split_breach_min_s: float = 3.0
    split_fallback_cooldown_s: float = 60.0
    split_restore_stable_s: float = 20.0
    split_penalty_bps_max: float = 6.0




    # Ticket 10 — Caps d'inflight business par profil / branche
    # Profil = NANO/MICRO/SMALL/MID/LARGE (source: g.capital_profile)
    # Ces caps sont la "vérité business" que le RM appliquera, et que l'Engine devra respecter.

    # Inflight globaux "trading" (TT + TM + MM), par profil de capital.
    inflight_trading_by_profile: Dict[str, int] = field(default_factory=lambda: {
        "NANO": 5,
        "MICRO": 8,
        "SMALL": 12,
        "MID": 24,
        "LARGE": 48,
    })

    # Répartition des caps par branche TRADING (TT/TM/MM) pour chaque profil.
    # Invariant attendu côté RM plus tard:
    #   TT_cap + TM_cap + MM_cap <= inflight_trading_by_profile[profile]
    caps_trading_by_profile: Dict[str, Dict[str, int]] = field(default_factory=lambda: {
        # NANO : 2 inflights globaux => TT=1, TM=1, MM=0 (MM désactivé en bootstrap)
        "NANO":  {"TT": 2, "TM": 2, "MM": 1},
        # MICRO : 4 inflights globaux => TT=2, TM=1, MM=1
        "MICRO": {"TT": 3, "TM": 3, "MM": 2},
        # SMALL : 8 inflights globaux => TT=3, TM=3, MM=1
        "SMALL": {"TT": 5, "TM": 5, "MM": 2},
        # MID : 16 inflights globaux => TT=6, TM=5, MM=3
        "MID":   {"TT": 10, "TM": 10, "MM": 4},
        # LARGE : 32 inflights globaux => TT=12, TM=10, MM=6
        "LARGE": {"TT": 20, "TM": 20, "MM": 8},
    })

    # Caps REB — nombre de bundles de rebalancing simultanés par profil.
    # Distincts des inflight "trading" (TT/TM/MM).
    inflight_rebal_by_profile: Dict[str, int] = field(default_factory=lambda: {
        "NANO": 1,
        "MICRO": 1,
        "SMALL": 2,
        "MID": 3,
        "LARGE": 4,
    })

    # Shutdown / cancellation (RiskManager + glue)
    rm_stop_join_timeout_s: float = 2.0
    mbf_glue_stop_timeout_s: float = 1.0
    shutdown_dump_task_stacks: bool = True
    shutdown_stack_limit: int = 10
    rm_capital_move_threshold_usdc: float = 50.0
    rm_capital_move_refresh_max_delay_s: float = 300.0
    rm_capital_move_refresh_mode: str = "AUTO"
    rm_capital_drift_threshold_pct: float = 0.05
    rm_ws_balance_resync_min_interval_s: float = 60.0
    rebal_lock_ttl_s: float = 15.0
    inv_soft_drift_pct: float = 1.5
    inv_hard_drift_pct: float = 5.0

    # Readiness / callbacks
    trading_ready_require_scanner_hook: bool = False
    cb_timeout_s: float = 0.25
    cb_max_inflight: int = 200
    cb_executor_workers: int = 2

    # Config audit
    audit_config_on_start: bool = True
    strict_config: bool = False
    ff_fail_closed_caps: bool = True
    ff_enforce_preemption: bool = False
    ff_hedge_fast_lane: bool = False
    ff_tm_enabled: bool = False
    ff_mm_enabled: bool = False
    ff_mm_opportunistic_gating_enforced: bool = False
    ff_reb_enabled: bool = False
    switch_knobs: RmSwitchKnobs = field(default_factory=RmSwitchKnobs)
    signal_policy: RmSignalPolicy = field(default_factory=RmSignalPolicy)
    fallback_on_tick_exception: str = "CONSTRAINED"

    # Policy "Capital Ladder" — source canonique pour les profils NANO→LARGE.
    # Chaque profil porte :
    #   - min_capital_per_sc : capital net moyen par sous-compte requis pour "entrer" dans ce profil.
    #   - allow_auto_upgrade : le profil peut être atteint automatiquement par la ladder.
    #   - allow_auto_downgrade : le profil peut être quitté automatiquement.
    capital_ladder_cfg: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {
        "NANO": {
            "min_capital_per_sc": 0.0,
            "allow_auto_upgrade": True,
            "allow_auto_downgrade": False,
        },
        "MICRO": {
            "min_capital_per_sc": 2000.0,
            "allow_auto_upgrade": True,
            "allow_auto_downgrade": True,
        },
        "SMALL": {
            "min_capital_per_sc": 5000.0,
            "allow_auto_upgrade": True,
            "allow_auto_downgrade": True,
        },
        "MID": {
            "min_capital_per_sc": 30000.0,
            "allow_auto_upgrade": True,
            "allow_auto_downgrade": True,
        },
        "LARGE": {
            "min_capital_per_sc": 100000.0,
            "allow_auto_upgrade": False,  # upgrade vers LARGE = décision gouvernance
            "allow_auto_downgrade": True,
        },
    })
    # TT/TM delta (VaR-lite P0)
    tttm_exposure_soft_usd: float = 2000.0
    tttm_exposure_hard_usd: float = 5000.0
    tttm_exposure_by_asset: Dict[str, Dict[str, float]] = field(
        default_factory=lambda: {
            "BTC": {"soft_usd": 5000.0, "hard_usd": 15000.0},
            "ETH": {"soft_usd": 3000.0, "hard_usd": 10000.0},
        }
    )

    # Gestion des stuck legs TT
    tt_stuck_soft_usd: float = 1000.0
    tt_stuck_hard_usd: float = 3000.0
    tt_stuck_max_age_s: float = 10.0

    # Ticket 6 — Cap notional global par combo (TT+TM+REB) par profil (en quote USDC/EUR)
    # Aligné sur la borne “tail-risk” (_profile_cap_notional dans le RM).
    # Interprétation: cap notional max partagé par TT/TM/REB sur une combo
    # (route + pair + quote) pour le profil capital considéré.

    combo_cap_usd_by_profile: Dict[str, float] = field(default_factory=lambda: {
        "NANO": 200.0,  # loss_budget 0.5€ / (25 bps tail)
        "MICRO": 400.0,  # 1€  / 0.0025
        "SMALL": 800.0,  # 2€  / 0.0025
        "MID": 1600.0,  # 4€  / 0.0025
        "LARGE": 3200.0,  # 8€  / 0.0025
    })

    # Facteur de réduction si au moins un alias est en TTL=DEGRADED
    # (BLOCKED => cap combo = 0 dans le RM, déjà géré)
    combo_ttl_degraded_factor: float = 0.5


    default_notional: float = 500.0
    min_fragment_quote: Dict[str, float] = field(default_factory=lambda: {"USDC":200.0, "EUR":200.0})
    max_fragments: int = 3
    fragment_safety_pad: float = 0.10
    target_ladder_participation: float = 0.5
    frontload_weights: List[float] = field(default_factory=lambda: [0.50,0.35,0.15])
    frontload_group_size: int = 3

    max_book_age_s: float = 1.2
    max_clock_skew_ms: int = 120
    dyn_min_required_bps_ceiling_pct: float = 0.33
    fastpath_slip_bps_max: float = 4.0

    tm_default_mode: str = "NEUTRAL"  # NEUTRAL | NN
    tm_queuepos_max_ahead_usd: float = 20_000.0
    tm_queuepos_max_eta_ms: int = 1200
    tm_neutral_hedge_ratio: float = 0.60

    # Collat / marge alias-aware
    collat_default_min_usd: float = 500.0
    collat_alias_overrides: Dict[str, Dict] = field(default_factory=dict)
    collat_ratio_warn: float = 1.1
    collat_ratio_crit: float = 1.0
    collat_quotes: List[str] = field(default_factory=lambda: ["USDC", "USDT", "USD", "EUR"])
    # Collat / marge alias-aware (vue par alias pour le RiskManager)
    collat_ratio_low: float = 1.1

    sfc_slippage_source: str = "fills"  # fills|hybrid|off
    prefilter_slip_bps: float = 2.0
    mm_ttl_ms: int = 2200
    mm_alias_name: str = "MM"
    mm_depth_min_usd: float = 7500.0
    mm_qpos_max_ahead_usd: float = 5000.0
    mm_min_p_both: float = 0.0
    mm_min_net_bps: float = 0.0005
    mm_hedge_cost_bps: float = 5.0

    # -- MM MONO (Mono-CEX) --
    mm_mono_alias_name: str = "MM_MONO"
    mm_mono_reb_inventory_soft_pct: float = 5.0
    mm_mono_reb_inventory_hard_pct: float = 15.0
    mm_mono_reb_inventory_critical_pct: float = 25.0

    # -- MM CROSS (Cross-CEX) --
    mm_cross_alias_name: str = "MM_CROSS"
    mm_cross_reb_inventory_soft_pct: float = 5.0
    mm_cross_reb_inventory_hard_pct: float = 12.0
    mm_cross_reb_inventory_critical_pct: float = 20.0

    mm_delta_soft_usd: float = 2000.0
    mm_delta_hard_usd: float = 5000.0
    mm_delta_by_asset: Dict[str, Dict] = field(default_factory=dict)

    # MM delta hedge P0
    mm_hedge_enabled: bool = True
    mm_hedge_max_step_usd: float = 5000.0
    mm_hedge_cooldown_s: float = 10.0
    mm_hedge_allowed_exchanges: List[str] = field(default_factory=list)



    tt_hedge_enabled: bool = True
    tt_hedge_max_step_usd: float = 5000.0
    tt_hedge_cooldown_s: float = 5.0
    tt_hedge_fraction_of_expo: float = 0.5

    # Notional cible d'un slot MM (USD/USDC) par profil capital.
    # Utilisé pour dimensionner la taille d'un quote MM élémentaire.
    # Clés attendues en UPPERCASE (NANO, MICRO, SMALL, MID, LARGE).
    mm_slot_notional_usdc_by_profile: Dict[str, float] = field(
        default_factory=lambda: {
            "NANO": 25.0,
            "MICRO": 50.0,
            "SMALL": 120.0,
            "MID": 180.0,
            "LARGE": 250.0,
        }
    )
    # Ratio du budget MM global (profil) alloué par paire (cap pair).
    # Ex : 0.25 = max 25 % du budget MM profil sur une seule paire.
    # Clés attendues en UPPERCASE (NANO, MICRO, SMALL, MID, LARGE).
    mm_pair_cap_ratio_by_profile: Dict[str, float] = field(
        default_factory=lambda: {
            "NANO": 0.25,
            "MICRO": 0.25,
            "SMALL": 0.25,
            "MID": 0.30,
            "LARGE": 0.30,
        }
    )
    # Nombre max de slots MM simultanés par paire (tous côtés confondus).
    # Soft-cap : le RM/Engine ne doit pas ouvrir plus de slots actifs.
    # Clés attendues en UPPERCASE (NANO, MICRO, SMALL, MID, LARGE).
    mm_slots_per_pair_by_profile: Dict[str, int] = field(
        default_factory=lambda: {
            "NANO": 1,
            "MICRO": 2,
            "SMALL": 2,
            "MID": 3,
            "LARGE": 4,
        }
    )

    # Seuil MM côté RM (non utilisé pour l’instant) :
    # - Scanner.mm_vol_bps_max = couche soft : coupe les hints MM en vol élevée.
    # - RiskManager.mm_vol_bps_max = garde-fou hard éventuel (par profil / VM) si on
    #   veut, plus tard, empêcher le RM d’admettre du MM au-delà d’une certaine vol.
    #   Default aligné sur Scanner pour éviter les surprises quand on l’activera.
    mm_vol_bps_max: float = 40.0

    global_kill_switch: bool = False
    inventory_cap_quote: float = 1500.0
    min_buffer_quote: float = 0.0
    balance_ttl_s_normal: float = 60.0
    balance_ttl_s_degraded: float = 180.0
    balance_ttl_s_block: float = 600.0
    balance_unknown_policy: str = "DEGRADED"
    transfer_submitted_timeout_s: float = 300.0
    transfer_retry_policy: Dict[str, Any] = field(default_factory=dict)
    daily_strategy_budget_quote: Dict[str, float] = field(default_factory=dict)
    daily_budget_reset_interval_s: float = 86400.0
    decision_log_path: str = ""
    preempt_mm_for_tt_tm: bool = False
    per_strategy_notional_cap: Dict[str, Dict[str, float]] = field(default_factory=dict)

    # Paramètres REB — version globale + variantes profilées (optionnelles).
    # Le RM peut continuer à ne lire que les valeurs globales ; les maps
    # *_by_profile sont là pour la "desk policy" et les futurs raffinements.
    rebal_allow_loss_bps: float = 0.0
    rebal_allow_loss_bps_by_profile: Dict[str, float] = field(default_factory=lambda: {
        "NANO": 0.0,
        "MICRO": 0.0,
        "SMALL": 0.0,
        "MID": 0.0,
        "LARGE": 0.0,
    })
    rebal_volume_haircut: float = 0.80
    rebal_volume_haircut_by_profile: Dict[str, float] = field(default_factory=lambda: {
        "NANO": 0.80,
        "MICRO": 0.80,
        "SMALL": 0.80,
        "MID": 0.80,
        "LARGE": 0.80,
    })

    dry_run: bool = True


# --- Simulator ---
@dataclass
class SimulatorCfg:
    auto_fragment: bool = True
    max_fragments: int = 3
    min_fragment_usdc: float = 200.0
    vwap_guard_bps: float = 10.0
    maker_pad_ticks: int = 2
    maker_fill_ratio: float = 0.65
    maker_skew_bps: float = 1.0
    target_ladder_participation: float = 0.5
    simulator_mode: str = "ON"
    simulator_bypass_allowed_in_live: bool = False
    sim_cache_ttl_ms: int = 300
    sim_max_wait_ms_rm: int = 3
    sim_prime_depth_levels: int = 25
    sim_max_inflight_jobs: int = 32
    sim_shadow_max_inflight: int = 8
    sim_deterministic_trade_id: bool = False
    sim_notional_buckets_base: str = "250,500,1000,2000,4000"
    sim_buckets_per_profile: Dict[str, str] = field(default_factory=dict)
    sim_buckets_adapt_with_vol: bool = True
    sim_vol_size_factor_floor: float = 0.60
    sim_vol_size_factor_ceil: float = 1.00
    sim_prime_multibucket_k: int = 3
    sim_book_fingerprint_mode: str = "TOP_AND_SUMMARY"
    sim_book_fingerprint_levels: int = 5
    sim_timeout_each_s: float = 1.2
    sim_cb_coalescing_ms: float = 10.0
    sim_split_mode: str = "EU_ONLY"
    sim_mm_hints_interval_ms: int = 500
    sim_mm_hints_levels: int = 5
    sim_outputs_required_by_branch: Dict[str, List[str]] = field(default_factory=lambda: {
        "TT": ["fragmentation_plan"],
        "TM": ["fragmentation_plan"],
        "REB": ["fragmentation_plan"],
        "MM": [],
    })

# --- Engine ---
@dataclass
class EnginePacerKnobs:
    warmup_secs: float = 5.0
    escalate_bad_windows: int = 3
    deescalate_good_windows: int = 2
    hold_time_flags_secs: float = 5.0
    good_score_threshold: float = 0.25
    bad_score_threshold: float = 0.65
    weight_ack: float = 0.35
    weight_lag: float = 0.25
    weight_err: float = 0.25
    weight_drain: float = 0.15
    default_targets: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
        "EU": {
            "ack_target": 90.0,
            "ack_hi": 120.0,
            "ack_sev": 150.0,
            "lag_hi": 15.0,
            "lag_sev": 20.0,
            "err_hi": 0.005,
            "err_sev": 0.02,
            "drain_hi": 120.0,
            "drain_sev": 240.0,
        },
        "US": {
            "ack_target": 150.0,
            "ack_hi": 180.0,
            "ack_sev": 210.0,
            "lag_hi": 15.0,
            "lag_sev": 20.0,
            "err_hi": 0.005,
            "err_sev": 0.02,
            "drain_hi": 120.0,
            "drain_sev": 240.0,
        },
        "EU-CB": {
            "ack_target": 180.0,
            "ack_hi": 210.0,
            "ack_sev": 240.0,
            "lag_hi": 15.0,
            "lag_sev": 20.0,
            "err_hi": 0.005,
            "err_sev": 0.02,
            "drain_hi": 120.0,
            "drain_sev": 240.0,
        },
        "JP": {
            "ack_target": 150.0,
            "ack_hi": 180.0,
            "ack_sev": 210.0,
            "lag_hi": 15.0,
            "lag_sev": 20.0,
            "err_hi": 0.005,
            "err_sev": 0.02,
            "drain_hi": 120.0,
            "drain_sev": 240.0,
        },
    })

@dataclass
class EngineCfg:
    pacer_min_ms: int = 2
    pacer_init_ms: int = 0
    pacer_max_ms: int = 25
    pacer_jitter_ms: int = 2
    # Pacer : cibles régionales *strictement* bornées par les caps RM/Engine (down-clamp only).
    # Le Pacer ne peut jamais élargir la capacité au-delà des plafonds inflight RM/Engine.
    pacer_targets: Dict[str, int] = field(default_factory=lambda: {"EU": 8, "EU_CB": 12, "US": 12, "JP": 12})
    pacer_knobs: EnginePacerKnobs = field(default_factory=EnginePacerKnobs)
    # Ticket 10 — capacités techniques Engine (workers / inflight par CEX)
    engine_queue_max: int = 200

    # Nombre de workers (tâches ordre) par profil de capital.
    workers_by_profile: Dict[str, int] = field(default_factory=lambda: {
        "NANO": 4,
        "MICRO": 8,
        "SMALL": 16,
        "MID": 24,
        "LARGE": 40,
    })
    inflight_max_by_exchange: Dict[str, int] = field(default_factory=dict)
    # Capacité max d'ordres "inflight" par exchange ET par profil.
    # Par défaut, chaque CEX peut encaisser à lui seul le budget global du profil.
    # Plus tard, tu pourras brider un CEX en baissant ses valeurs via .env.
    inflight_max_by_exchange_by_profile: Dict[str, Dict[str, int]] = field(default_factory=lambda: {
        "NANO": {
            "BINANCE": 2,
            "BYBIT": 2,
            "COINBASE": 2,
        },
        "MICRO": {
            "BINANCE": 4,
            "BYBIT": 4,
            "COINBASE": 4,
        },
        "SMALL": {
            "BINANCE": 8,
            "BYBIT": 8,
            "COINBASE": 8,
        },
        "MID": {
            "BINANCE": 16,
            "BYBIT": 16,
            "COINBASE": 16,
        },
        "LARGE": {
            "BINANCE": 32,
            "BYBIT": 32,
            "COINBASE": 32,
        },
    })

    inflight_reserved_hedge_by_exchange_by_profile: Dict[str, Dict[str, int]] = field(
        default_factory=lambda: {
            "NANO": {"BINANCE": 1, "BYBIT": 1, "COINBASE": 1},
            "MICRO": {"BINANCE": 1, "BYBIT": 1, "COINBASE": 1},
            "SMALL": {"BINANCE": 1, "BYBIT": 1, "COINBASE": 1},
            "MID": {"BINANCE": 1, "BYBIT": 1, "COINBASE": 1},
            "LARGE": {"BINANCE": 1, "BYBIT": 1, "COINBASE": 1},
        }
    )
    inflight_reserved_cancel_by_exchange_by_profile: Dict[str, Dict[str, int]] = field(
        default_factory=lambda: {
            "NANO": {"BINANCE": 1, "BYBIT": 1, "COINBASE": 1},
            "MICRO": {"BINANCE": 1, "BYBIT": 1, "COINBASE": 1},
            "SMALL": {"BINANCE": 1, "BYBIT": 1, "COINBASE": 1},
            "MID": {"BINANCE": 1, "BYBIT": 1, "COINBASE": 1},
            "LARGE": {"BINANCE": 1, "BYBIT": 1, "COINBASE": 1},
        }
    )

    tt_max_skew_ms: int = 35
    tt_max_drift_bps: float = 10.0
    order_timeout_s: int = 3
    http_timeout_s: float = 5.0
    idempotency_ttl_s: float = 60.0
    ff_enforce_client_oid_deterministic: bool = False
    ff_fail_closed_idempotence: bool = True
    idempotence_on: bool = True
    ff_hedge_fast_lane: bool = False
    ff_enforce_preemption: bool = False
    ff_tm_enabled: bool = False
    ff_mm_enabled: bool = False
    ff_mm_opportunistic_gating_enforced: bool = False
    ff_reb_enabled: bool = False
    fail_closed_on_rm_override_exception: bool = True

    tm_exposure_ttl_ms: int = 1500
    tm_exposure_ttl_hedge_ratio: float = 0.5
    tm_watch_timeout_s: int = 2


    tm_queuepos_max_ahead_usd: float = 20000.0
    tm_queuepos_max_eta_ms: int = 1200
    tm_nn_hedge_ratio: float = 0.65

    maker_pad_ticks: int = 2
    tm_max_open_makers: int = 3

    frontload_enabled: bool = True
    frontload_weights: List[float] = field(default_factory=lambda: [0.50,0.35,0.15])
    frontload_group_size: int = 3
    min_fragment_quote: Dict[str,float] = field(default_factory=lambda: {"USDC":200.0, "EUR":200.0})

    anchor_max_staleness_ms: int = 1200
    anchor_halve_guard_ms: int = 300
    max_price_deviation_pct: float = 0.50

    mm_hysteresis_ms: int = 600
    mm_ttl_ms: int = 2200
    mm_min_quote_lifetime_ms: int = 400
    mm_replace_cooldown_ms: int = 200
    mm_sticky_band_ticks: float = 1.0
    mm_allow_auto_hedge: bool = False
    mm_hedge_schedule: List[float] = field(default_factory=lambda: [0.33, 0.66, 1.0])
    mm_use_progressive_hedge: bool = False
    mm_hedge_final_ratio: float = 1.0
    mm_dual_engine_enabled: bool = True
    mm_single_inventory_enabled: bool = True
    mm_scale_on_tt_tm: float = 0.6
    mm_pad_boost_on_tt_tm: int = 1
    mm_place_rate_limit_per_pair: int = 10
    mm_cancel_rate_limit_per_pair: int = 20
    mm_cancel_budget_per_pair_min: int = 60
    mm_cancel_budget_exhausted_penalty_lifetime_mult: float = 2.0
    mm_cancel_budget_exhausted_penalty_pad_ticks: int = 2
    mm_cross_ttl_ms: int = 3000
    mm_cross_hedge_schedule: List[float] = field(default_factory=lambda: [0.5, 1.0])
    mm_cross_panic_after_ms: int = 5000
    mm_cross_defensive_pad_ticks: int = 3
    mm_cross_defensive_lifetime_mult: float = 1.5
    mm_cross_429_threshold: float = 0.05
    # Exemple (ENV ENGINE_MM_VARIANTS, JSON):
    # {"neutral":{"SMALL":{"ladder_levels":2,"ladder_weights":[0.7,0.3],"ladder_step_ticks":1.0}}}
    mm_variants: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    vol_soft_cap_bps: float = 45.0
    vol_hard_cap_bps: float = 80.0
    freeze_tm_on_vol: bool = True
    freeze_mm_on_vol: bool = True
    depth_min_quote_tt: float = 200.0
    depth_min_quote_tm: float = 500.0
    depth_min_quote_mm: float = 1000.0
    depth_levels_check: int = 3
    shallow_book_blocks_tm: bool = True
    shallow_book_blocks_mm: bool = True

    # TT Policy
    tt_policy_version: str = "v1"
    tt_submit_mode: str = "adaptive"
    tt_tif_policy: str = "IOC"
    tt_tif_last_slice: str = "FOK"
    tt_ack_p95_staggered_threshold_ms: float = 150.0
    tt_stop_edge_floor_bps: float = -100.0
    tt_ack_slo_ms: float = 1000.0
    tt_time_budget_ms: float = 5000.0
    tt_require_private_ws_healthy: bool = True
    tt_max_router_age_ms: float = 5000.0
    tt_max_book_age_ms: float = 5000.0
    tt_private_ws_max_lag_ms: float = 2000.0
    tt_group_concurrency_by_pacer: Dict[str, int] = field(default_factory=lambda: {"NORMAL": 3, "CONSTRAINED": 1, "SEVERE": 1})

    # TM Policy
    tm_policy_version: str = "v1"
    tm_quote_lifetime_min_ms: float = 500.0
    tm_ladder_levels: int = 1
    tm_ladder_step_ticks: float = 1.0
    tm_replace_drift_ticks: float = 2.0
    tm_replace_max_age_ms: int = 2500
    tm_replace_budget: int = 10
    tm_cancel_budget: int = 10
    tm_retry_budget: int = 3
    tm_time_budget_ms: float = 5000.0
    tm_qpos_fail_mode: str = "block"
    tm_qpos_guard_hysteresis_ms: int = 1000
    tm_qpos_guard_release_ratio: float = 0.8
    tm_panic_trigger_ack_p95_ms: float = 500.0
    tm_panic_hedge_ratio: float = 1.1
    tm_panic_hedge_ttl_ms: int = 500
    tm_vol_cap_bps: float = 500.0
    tm_stop_edge_floor_bps_delta: float = -100.0
    tm_require_private_ws_healthy: bool = True
    tm_fallback_mode: str = "ioc_only"
    tm_hedge_order_type: str = "IOC"
    tm_revalidate_before_place: bool = True
    tm_revalidate_every_s: float = 0.0
    tm_maker_order_type: str = "GTC_POSTONLY"
    tm_spiral_breaker_429: bool = True

    # MM Policy
    mm_policy_version: str = "v1"
    mm_require_private_ws_healthy: bool = True
    mm_pad_ticks_base: float = 1.0
    mm_size_factor_base: float = 1.0
    mm_requote_min_ticks: float = 1.0
    mm_min_net_bps: float = 5.0
    mm_qpos_max_ahead_usd: float = 10000.0
    mm_toxicity_threshold: float = 0.8
    mm_jump_guard_threshold_bps: float = 10.0
    mm_jump_guard_freeze_s: float = 5.0
    mm_jitter_pct: float = 0.05
    mm_edge_fees_bps: float = 2.0
    mm_edge_buffer_bps: float = 1.0
    mm_hedge_aggressive_pad_mult: float = 0.5

    price_band_bps_floor: float = 15.0
    price_band_bps_cap: float = 50.0
    vol_price_band_k: float = 0.6
    circuit_escalation_window_s: float = 60.0
    circuit_mute_escalation: float = 1.7
    circuit_mute_min_s: float = 10.0
    circuit_mute_max_s: float = 900.0
    circuit_mute_s_tm: float = 300.0
    circuit_mute_s_mm: float = 300.0
    ready_autoset_on_start: bool = False
    ready_poll_every_s: float = 2.0
    require_private_ws_in_dry_run: bool = False

# --- Private WS Hub ---
@dataclass
class PrivateWSHubCfg:
    PWS_POOL_SIZE_EU: int = 2
    PWS_POOL_SIZE_US: int = 2
    PWS_REGION_MAP: Dict[str,str] = field(default_factory=lambda: {"BINANCE":"EU","BYBIT":"EU","COINBASE":"US"})
    PWS_QUEUE_MAXLEN: int = 5000
    PWS_QUEUE_SATURATION_RATIO: float = 0.85
    ff_pws_no_drop_critical_enforced: bool = False
    ff_pws_strict_dedup_enforced: bool = False
    ff_pws_disable_auto_wiring_prod: bool = True
    pws_drop_policy: str = "DROP_NEW"  # DROP_NEW | DROP_OLDEST
    PWS_CONNECT_TIMEOUT_S: int = 10
    PWS_READ_TIMEOUT_S: int = 30
    PWS_PING_INTERVAL_S: int = 20
    PWS_PONG_TIMEOUT_S: int = 10
    PWS_HEARTBEAT_MAX_GAP_S: int = 30
    
    # NEW: Parité de connexion par exchange pour WS privés
    connect_timeout_s_by_ex: Dict[str, int] = field(default_factory=dict)
    read_timeout_s_by_ex: Dict[str, int] = field(default_factory=dict)
    ping_interval_s_by_ex: Dict[str, int] = field(default_factory=dict)
    pong_timeout_s_by_ex: Dict[str, int] = field(default_factory=dict)

    PWS_STABLE_RESET_S: int = 30
    PWS_JITTER_MS: int = 2
    PWS_BACKOFF_BASE_MS: int = 500
    PWS_BACKOFF_MAX_MS: int = 20_000
    cb_ack_ws_enabled: bool = True
    cb_private_poll_interval_s: int = 2
    bybit_ws_private_url: str = "wss://stream.bybit.com/v5/private"
    PWS_PACER_EU: str = "NORMAL"
    PWS_PACER_US: str = "NORMAL"
    PWS_ALERT_PERIOD_S: int = 5
    PWS_STATUS_SOFT_GAP_S: float = 60.0
    PWS_STATUS_HARD_GAP_S: float = 180.0
    PWS_STATUS_ERROR_THRESHOLD: int = 10
    PWS_STATUS_RECONNECT_THRESHOLD: int = 20

@dataclass
class RebalancingCfg:
    # Utilisé par le chantier M6-B (REB via hints, RM → Simu → Engine)
    rebal_quantum_min_quote: float = 50.0
    rebal_internal_transfer_threshold: float = 250.0
    # Seuil de cash minimal par quote avant déclenchement rebalançage intra-CEX
    rebal_min_cash_per_quote: Dict[str, float] = field(default_factory=lambda: {"USDC": 1000.0, "EUR": 1000.0})
    # Cadence REB max (opérations/min). Doit rester bornée par inflight_rebal
    # multiplié par un ratio simple (cf. _sanity_check_rm_caps, ratio=3 par défaut).
    rebal_max_ops_per_min: int = 6
    rebal_ops_per_min_ratio: float = 3.0
    rebal_priority: List[str] = field(default_factory=lambda: ["CASH","CRYPTO","OVERLAY"])
    rebal_hint_ttl_s: int = 120
    rebal_slot_ttl_s: float = 120.0
    rebal_snapshots_missing_error_s: float = 45.0
    rebal_snapshots_error_cooldown_s: float = 60.0
    rebal_quantum_quote_map: Dict[str, float] = field(default_factory=lambda: {"USDC": 250.0, "EUR": 250.0})


# --- Reconciler ---
@dataclass
class ReconcilerCfg:
    RECO_ALERT_PERIOD_S: int = 30
    RECO_MISS_BURST_THRESHOLD: int = 5
    RECO_MISS_RECENT_THRESHOLD: float = 30.0
    RECO_ALIAS_RESYNC_MAX_AGE_S: float = 6 * 3600.0
    cooldown_s: float = 60.0
    stale_ms: int = 1500
    poll_every_s: float = 2.0
    dedup_max: int = 20000
    cold_every_h: float = 6.0

# --- Balance Fetcher ---
@dataclass
class BalanceFetcherCfg:
    refresh_interval_s: int = 10
    ttl_cache_s: int = 10
    bf_pacer_eu_ms: int = 50
    bf_pacer_us_ms: int = 60
    BF_PACER_EU: str = "NORMAL"
    BF_PACER_US: str = "NORMAL"
    fee_token_low_watermarks: Dict[str,float] = field(default_factory=lambda: {"BNB": 0.1, "MNT": 0.1})
    fee_low_watermark: float = 10.0
    fee_high_watermark: float = 50.0
    BF_ALERT_PERIOD_S: int = 60
    binance_rest_base: str = "https://api.binance.com"
    coinbase_api_base: str = "https://api.coinbase.com"
    bybit_api_base: str = "https://api.bybit.com"
    default_private_wallet: str = "SPOT"
    wallet_missing_log_interval_s: float = 60.0
    wallet_types: List[str] = field(default_factory=lambda: ["SPOT","FUNDING"])
    WS_BAL_TTL_SECONDS: float = 10.0
    ENABLE_WS_BALANCE_MERGE: bool = False
    ws_reco_miss_rate_thr_per_min: float = 30.0
    ws_reco_resync_age_thr_s: float = 6 * 3600.0
    reco_miss_burst_threshold: int = 5
    reco_miss_recent_threshold: int = 5
    reco_alias_resync_max_age_s: float = 6 * 3600.0
    # Readiness: état minimal exigé pour considérer le BalanceFetcher “READY”.
    # Les valeurs usuelles dans la stack: UNKNOWN < DEGRADED < READY.
    balances_ready_min_state: str = "DEGRADED"


# --- Slippage Handler ---
@dataclass
class SlippageCfg:
    ttl_s: int = 2
    heartbeat_s: int = 1
    max_bps_by_quote: Dict[str,float] = field(default_factory=lambda: {"USDC": 12.0, "EUR": 14.0})
    use_vwap_depth: bool = True
    fee_sync_max_concurrency: int = 4
    fee_sync_backoff_initial_s: float = 1.0
    fee_sync_backoff_max_s: float = 8.0
    fee_sync_max_retries: int = 3
    fee_sync_jitter_s: float = 0.2
    fee_reality_check_threshold_bps: float = 3.0

# --- Volatility Monitor ---
@dataclass
class VolatilityCfg:
    ttl_s: int = 5
    window_micro_m: int = 1
    window_long_m: int = 10
    winsor_pct: float = 0.01
    midprice_to_bps: float = 1e4
    to_bps_floor: float = 0.0
    to_bps_cap: float = 250.0
    ema_alpha: float = 0.20
    soft_cap_bps: float = 80.0
    chaos_cap_bps: float = 150.0
    hysteresis: float = 0.25
    vol_min_delta_bps: float = 0.5
    max_silence_s: float = 2.0
    vm_size_factor_map: Dict[str, float] = field(default_factory=lambda: {
        "NORMAL": 1.0,
        "CAREFUL": 0.85,
        "ALERT": 0.70,
    })
    vm_min_bps_boost_map: Dict[str, float] = field(default_factory=lambda: {
        "NORMAL": 0.0,
        "CAREFUL": 0.0002,
        "ALERT": 0.0005,
    })
    tm_neutral_hedge_ratio_map: Dict[str, float] = field(default_factory=lambda: {
        "NORMAL": 0.50,
        "CAREFUL": 0.55,
        "ALERT": 0.65,
    })
    # Seuils de prudence pour le VolatilityManager (sur p95_vol_bps "micro")
    # - normal → modere si p95 >= NORMAL_TO_CAREFUL
    # - modere → eleve si p95 >= CAREFUL_TO_ALERT
    # - eleve → modere si p95 <= ALERT_TO_CAREFUL
    # - modere → normal si p95 <= CAREFUL_TO_NORMAL
    vm_prudence_thresholds_bps: Dict[str, float] = field(default_factory=lambda: {
        "NORMAL_TO_CAREFUL": 8.0,  # p95 >=  8 bps → CAREFUL
        "CAREFUL_TO_ALERT": 15.0,  # p95 >= 15 bps → ALERT
        "ALERT_TO_CAREFUL": 12.0,  # p95 <= 12 bps → CAREFUL
        "CAREFUL_TO_NORMAL": 6.0,  # p95 <=  6 bps → NORMAL
    })

    vm_maker_pad_ticks_map: Dict[str, int] = field(default_factory=lambda: {
        "NORMAL": 0,
        "CAREFUL": 1,
        "ALERT": 2,
    })

# --- Rate Limiter ---
# --- Rate Limiter ---
@dataclass
class RateLimiterCfg:
    # Hard caps "globaux" (déjà en place)
    hard_caps_rps_by_exchange: Dict[str, float] = field(default_factory=dict)
    hard_caps_rps_by_exchange_kind: Dict[str, Dict[str, float]] = field(default_factory=dict)
    hard_caps_rps_by_kind: Dict[str, float] = field(default_factory=lambda: {"balances": 0.5})

    bursts_by_exchange: Dict[str, float] = field(default_factory=dict)
    bursts_by_exchange_kind: Dict[str, Dict[str, float]] = field(default_factory=dict)
    bursts_by_kind: Dict[str, float] = field(default_factory=lambda: {"balances": 1})

    priorities: List[str] = field(default_factory=lambda: ["hedge", "cancel", "maker"])
    fair: bool = True
    name_prefix: str = "rl"

    min_sleep_s: float = 0.001
    max_sleep_s: float = 0.250

    # Ticket RL global (déjà utilisé ailleurs)
    default_rate_per_s: float = 30.0
    default_burst: float = 30.0

    # 🔹 Nouveau : soft-cap SC (sub-account) consommé par RiskManager._cfg("rl.sc.*")
    #
    # Structure attendue (exemple):
    #   {
    #     "default": {"rate_per_s": 3.0, "burst": 6.0},
    #     "BINANCE": {
    #       "TT": {"rate_per_s": 6.0, "burst": 10.0},
    #       "TM": {"rate_per_s": 4.0, "burst": 8.0},
    #     },
    #     "BYBIT": { ... },
    #   }
    #
    # => flatten via _rebuild_flat_cache():
    #    rl.sc.default.rate_per_s
    #    rl.sc.BINANCE.TT.rate_per_s
    #    rl.sc.BINANCE.TM.burst
    sc: Dict[str, Dict[str, Dict[str, float]]] = field(
        default_factory=lambda: {
            "default": {"rate_per_s": 3.0, "burst": 6.0},
        }
    )

# --- Retry Policy ---
@dataclass
class RetryPolicyCfg:
    base_ms: int = 500
    max_ms: int = 20_000
    max_attempts: int = 5
    jitter: int = 1

# --- RPC Gateway ---
@dataclass
class RPCCfg:
    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 8443
    region: str = "EU"
    timeout_s: float = 2.0
    timeout_ms: int = 1500
    max_retries: int = 2
    rpc_idempotency_ttl_s: int = 600
    rpc_idempotency_memoize_response: bool = True
    loopback_inproc: bool = True
    ready_strict: bool = True
    mtls_enabled: bool = True
    ca_cert: Optional[str] = None
    server_cert: Optional[str] = None
    server_key: Optional[str] = None
    client_cert: Optional[str] = None
    client_key: Optional[str] = None
    require_client_cert: bool = True
    admin_enabled: bool = False
    admin_token: Optional[str] = None
    strict_validation: bool = False
    remote_base_url: Optional[str] = None
    rpc_server_bind: str = "0.0.0.0:8080"
    rpc_client_base: str = "http://127.0.0.1:8080"
    rpc_timeout_s: int = 3
    rpc_retries: int = 2
    rpc_enable_mtls: bool = False
    rpc_cert_paths: Dict[str,str] = field(default_factory=dict)
    rpc_max_payload_kb: int = 256


# --- Logger Historique ---
@dataclass
class LoggerCfg:
    out_dir: str = "/srv/app/logs"
    strict_obs: bool = False
    LHM_Q_STREAM_MAX: int = 20000
    LHM_STREAM_BATCH: int = 200
    LHM_DROP_WHEN_FULL: bool = False
    LHM_HIGH_WATERMARK_RATIO: float = 0.85
    LHM_MAX_QUEUE_PLATEAU_S: int = 3
    ff_fail_closed_logging: bool = True
    ff_logging_critical_streams: List[str] = field(default_factory=lambda: [
        "trade_fsm",
        "private_plane",
        "fills",
        "transfers",
        "engine_acks",
        "engine_submits",
        "fills_normalized",
        "privatews_events",
        "trades",
    ])
    ff_truth_model_enabled: bool = False
    ff_truth_fail_closed: bool = True
    LHM_MM_SAMPLING_QUOTES: float = 0.05
    LHM_MM_SAMPLING_CANCELS: float = 0.02
    LHM_DROP_LOG_SAMPLE_RATE: float = 0.1
    LHM_JSONL_QUEUE_CAP: int = 5000
    jsonl_queue_cap: Optional[int] = None
    LHM_JSONL_MAX_BYTES: int = 256 * 1024 * 1024
    LHM_JSONL_MAX_AGE_S: int = 3600
    tracker_history_limit: int = 6000
    tracker_rotation_interval: int = 1800
    tracker_max_active: int = 10
    tracker_pause_secs: int = 600
    tracker_min_daily_volume_usdc: float = 500_000.0
    tracker_slippage_bad_thr: float = 0.005
    tracker_e2e_latency_bad_ms: int = 1200
    tracker_window_opp_freq_s: int = 600
    tracker_window_score_s: int = 1800
    tracker_window_daily_s: int = 86400
    tracker_w_opp_score: float = 0.6
    tracker_w_spread_net: float = 1.0
    tracker_w_volume: float = 0.00001
    tracker_w_latency_penalty: float = 0.003
    tracker_penalty_trade_fail: float = 10.0
    tracker_penalty_slippage: float = 5.0
    tracker_penalty_vol_modere: float = 5.0
    tracker_penalty_vol_eleve: float = 10.0
    tracker_bonus_low_vol: float = 2.0
    tracker_bonus_high_freq: float = 10.0
    tracker_bias_ttl_s: int = 1500
    tracker_bonus_mode_TT: float = 2.0
    tracker_bonus_mode_TM: float = 1.5
    tracker_bonus_mode_REB: float = 0.5
    tracker_ema_alpha: float = 0.2
    tracker_ema_slip_alpha: float = 0.2
    tracker_route_latency_penalty_scale: float = 0.002
    tracker_route_win_bonus: float = 1.0
    tracker_route_fail_penalty: float = 2.0
    tracker_max_active_TT: Optional[int] = None
    tracker_max_active_TM: Optional[int] = None
    tracker_max_active_REB: Optional[int] = None
    tracker_max_active_MM: Optional[int] = None
    tracker_mm_w_pnl: float = 0.001
    tracker_mm_w_winrate: float = 20.0
    tracker_mm_penalty_latency: float = 1.5
    tracker_mm_penalty_slippage: float = 1.0
    tracker_constrain_universe_by_mode: bool = False
    LHM_DISK_WARN_PCT: float = 70.0
    LHM_DISK_CRIT_PCT: float = 85.0
    LHM_STORAGE_ALERT_COOLDOWN_S: float = 60.0
    LHM_DB_NAME: str = "loggerh"
    LHM_DB_MAX_BYTES: int = 256 * 1024 * 1024
    LHM_DB_MAX_AGE_S: int = 24 * 3600
    LHM_DB_LANE_ENABLED: bool = True
    LHM_DB_LANE_Q_MAX: int = 5000
    LHM_DB_LANE_BATCH_MAX: int = 2000
    LHM_DB_LANE_DRAIN_MS: float = 5.0
    LHM_DB_LANE_DROP_WHEN_FULL: bool = True
    LHM_DB_LANE_ERROR_THRESHOLD: int = 3
    LHM_WD_INTERVAL_S: float = 2.0
    LHM_WD_PLATEAU_WINDOW: int = 15
    LHM_WD_QUEUE_MIN_SIZE: int = 50
    LHM_WD_STALL_THRESHOLD_S: float = 10.0
    LHM_TRADE_BATCH_SIZE: int = 30
    LHM_TRADE_FLUSH_INTERVAL_S: float = 0.35
    LHM_SLO_WRITE_MS_P95_TARGET: float = 250.0
    LHM_SLO_QUEUE_DEPTH_MAX_TARGET: float = 2000.0
    LHM_SLO_PIPELINE_LAG_MAX_SECONDS_TARGET: float = 5.0
    LHM_SLO_DROPPED_TRADES_BUDGET: float = 0.0
    LHM_OPPORTUNITY_QUEUE_MAX: int = 5000
    LHM_OPPORTUNITY_DROP_WHEN_FULL: bool = True
    LHM_PNL_RECO_ENABLED: bool = False
    LHM_PNL_RECO_TOL_ABS_QUOTE: float = 25.0
    LHM_PNL_RECO_TOL_PCT: float = 0.01
    LHM_PNL_RECO_MAX_LOOKBACK_DAYS: int = 3
    LHM_PNL_RECO_DEFAULT_REGION: str = "EU"
    OPS_RETENTION_DAYS: int = 30
    # PIVOT: chemins pilotés par cfg.lhm (pas d'ENV dans LHM)
    storage_path: Optional[str] = None

# --- Dashboard / Tests (optionnels) ---
@dataclass
class DashboardCfg:
    OBS_BASE_URL: str = ""
    AUTOREFRESH_SEC: int = 2
    DEMO_MODE: bool = False

@dataclass
class TestsCfg:
    ENABLE_WS_LIVE: bool = False
    WS_LIVE_WINDOW_S: int = 10
    WS_LIVE_MIN_MSGS_PER_S: int = 1
    WS_LIVE_CLOSE_TIMEOUT_S: int = 3

# ------------------------------
# Contrat SLO / TTL public ↔ privé
# ------------------------------
@dataclass(frozen=True)
class SLOPublicPath:
    """
    SLO côté "public" (market data) pour un chemin (mode, exchange).
    - l2_fresh_max_s : âge max des orderbooks L2 pour dire "OK" côté Scanner/Router.
    - slip_ttl_s     : TTL max des points de slippage (SlippageHandler).
    - vol_ttl_s      : TTL max des snapshots de volatilité (VolatilityMonitor).
    """
    l2_fresh_max_s: float
    slip_ttl_s: float
    vol_ttl_s: float


@dataclass(frozen=True)
class SLOPrivatePath:
    """
    SLO côté "privé" (trading plane) pour un chemin (mode, exchange).
    - ack_target_ms / ack_hi_ms / ack_sev_ms : cibles latence ack p95 (Engine/Pacer).
    - order_timeout_s                       : timeout dur de l'Engine (submit→ack/fill).
    - tm_exposure_ttl_ms                    : TTL d'exposition TM avant hedge forcé.
    - balances_ttl_*                        : TTL des soldes avant dégradé / block.
    - pws_heartbeat_gap_max_s              : gap max heartbeat PrivateWS avant alerte.
    """
    ack_target_ms: float
    ack_hi_ms: float
    ack_sev_ms: float
    order_timeout_s: float
    tm_exposure_ttl_ms: float
    balances_ttl_s_normal: float
    balances_ttl_s_degraded: float
    balances_ttl_s_block: float
    pws_heartbeat_gap_max_s: float


@dataclass(frozen=True)
class PathSLO:
    """
    SLO complet pour un chemin (mode, exchange).

    Exemple de clé dans le contrat:
        slo["EU_ONLY"]["BINANCE"]  → PathSLO(public=..., private=...)
        slo["SPLIT"]["COINBASE"]   → PathSLO(public=..., private=...)
    """
    public: SLOPublicPath
    private: SLOPrivatePath


# ------------------------------
# BotConfig racine
# ------------------------------
@dataclass
class BotConfig:
    # Globaux
    g: Globals = field(default_factory=Globals)

    # Sections modules
    router: RouterCfg = field(default_factory=RouterCfg)
    ws_public: WsPublicCfg = field(default_factory=WsPublicCfg)
    wd: WatchdogCfg = field(default_factory=WatchdogCfg)
    discovery: DiscoveryCfg = field(default_factory=DiscoveryCfg)
    scanner: ScannerCfg = field(default_factory=ScannerCfg)
    rm: RiskManagerCfg = field(default_factory=RiskManagerCfg)
    sim: SimulatorCfg = field(default_factory=SimulatorCfg)
    engine: EngineCfg = field(default_factory=EngineCfg)
    pws: PrivateWSHubCfg = field(default_factory=PrivateWSHubCfg)
    rebal: RebalancingCfg = field(default_factory=RebalancingCfg)
    reconciler: ReconcilerCfg = field(default_factory=ReconcilerCfg)
    flow_safety: FlowSafetyConfig = field(default_factory=FlowSafetyConfig)

    balances: BalanceFetcherCfg = field(default_factory=BalanceFetcherCfg)
    slip: SlippageCfg = field(default_factory=SlippageCfg)
    vol: VolatilityCfg = field(default_factory=VolatilityCfg)
    rl: RateLimiterCfg = field(default_factory=RateLimiterCfg)
    retry: RetryPolicyCfg = field(default_factory=RetryPolicyCfg)
    rpc: RPCCfg = field(default_factory=RPCCfg)
    lhm: LoggerCfg = field(default_factory=LoggerCfg)
    dashboard: DashboardCfg = field(default_factory=DashboardCfg)
    tests: TestsCfg = field(default_factory=TestsCfg)
    obs: ObservabilityCfg = field(default_factory=ObservabilityCfg)
    boot: BootCfg = field(default_factory=BootCfg)
    alerting: AlertingCfg = field(default_factory=AlertingCfg)
    overrides: List[Dict[str, Any]] = field(default_factory=list)

    # cache à plat pour l'accès rétro-compat
    _flat_cache: Dict[str, Any] = field(default_factory=dict, init=False, repr=False)
    _alias_map: Dict[str, str] = field(default_factory=dict, init=False, repr=False)
    # dans la dataclass BotConfig (ou équivalent)
    scanner_global_eval_hz: float | None = None
    resolution_version: str = "v1"

    # --- Aliases pratiques pour les modules WS / Router -------------------

    @property
    def POD_REGION(self) -> str:
        """
        Alias utilisé par websockets_clients / Router pour la région du pod.

        Source unique : self.g.pod_region (env: POD_REGION).
        Valeurs typiques : "EU", "US", "EU_CB", ...
        """
        val = getattr(self.g, "pod_region", "EU")
        if not val:
            return "EU"
        return str(val).upper()

    @property
    def DEPLOYMENT_MODE(self) -> str:
        """
        Alias utilisé par websockets_clients / Router pour le mode de déploiement.

        Source unique : self.g.deployment_mode (env: DEPLOYMENT_MODE).
        Valeurs typiques : "EU_ONLY", "SPLIT", "JP_ONLY", ...
        """
        val = getattr(self.g, "deployment_mode", "EU_ONLY")
        if not val:
            return "EU_ONLY"
        return str(val).upper()


    @property
    def telegram(self) -> TelegramCfg:
        return self.alerting.telegram

    @property
    def webhook(self) -> WebhookCfg:
        return self.alerting.webhook

    @property
    def MODE(self) -> str:
        return str(getattr(self.g, "mode", "DRY_RUN"))

    @property
    def LIVE_TRADING_ARMED(self) -> bool:
        return bool(getattr(self.g, "live_trading_armed", False))
    @property
    def DESK_REBAL_POLICY(self) -> Dict[str, Any]:
        """
        Vue agrégée des paramètres de rebalancing (REB) pour pilotage "desk".
        Read-only, ne modifie aucune valeur.
        """
        rm = self.rm
        rebal = self.rebal

        ttl_normal = getattr(self, "RM_BALANCE_TTL_S_NORMAL", 60.0)
        ttl_degraded = getattr(self, "RM_BALANCE_TTL_S_DEGRADED", 180.0)
        ttl_block = getattr(self, "RM_BALANCE_TTL_S_BLOCK", 600.0)

        return {
            "caps": {
                "inflight_rebal_by_profile": getattr(rm, "inflight_rebal_by_profile", {}),
                "combo_cap_usd_by_profile": getattr(rm, "combo_cap_usd_by_profile", {}),
            },
            "loss_bps": {
                "global": getattr(rm, "rebal_allow_loss_bps", 0.0),
                "by_profile": getattr(rm, "rebal_allow_loss_bps_by_profile", {}),
            },
            "volume_haircut": {
                "global": getattr(rm, "rebal_volume_haircut", 0.80),
                "by_profile": getattr(rm, "rebal_volume_haircut_by_profile", {}),
            },
            "quantum": {
                "min_quote": getattr(rebal, "rebal_quantum_min_quote", 0.0),
                "per_quote": getattr(rebal, "rebal_quantum_quote_map", {}),
            },
            "rate": {
                "max_ops_per_min": getattr(rebal, "rebal_max_ops_per_min", 0),
            },
            "ttl_balances": {
                "normal_s": ttl_normal,
                "degraded_s": ttl_degraded,
                "block_s": ttl_block,
            },
        }





    # ------------- Construction -------------
    @staticmethod
    def from_env() -> "BotConfig":
        cfg = BotConfig()
        # Helper to centralize détection de collisions entre clés aliasées
        # --- Charger Globaux ---
        g = cfg.g
        _pod_region_env = _Env.get("POD_REGION", None)
        _region_alias_env = _Env.get("REGION", None)
        _deployment_mode_env = _Env.get("DEPLOYMENT_MODE", None)

        g.deployment_mode = _deployment_mode_env or g.deployment_mode
        g.pod_region = _pod_region_env or g.pod_region
        g.enable_jp = _Env.get_bool("ENABLE_JP", g.enable_jp)
        g.pacer_mode = _Env.get("PACER_MODE", g.pacer_mode)
        g.engine_pod_map = _Env.get_dict("ENGINE_POD_MAP", g.engine_pod_map)
        g.split_latency = _Env.get_dict("SPLIT_LATENCY", g.split_latency)
        g.eu_latency = _Env.get_dict("EU_LATENCY", g.eu_latency)
        g.jp_latency = _Env.get_dict("JP_LATENCY", g.jp_latency)
        g.capital_profile = _Env.get("CAPITAL_PROFILE", g.capital_profile)
        g.enabled_exchanges = _Env.get_list("ENABLED_EXCHANGES", g.enabled_exchanges)
        g.allowed_routes = _Env.get_routes("ALLOWED_ROUTES", g.allowed_routes)
        g.pair_universe_mode = _Env.get("PAIR_UNIVERSE_MODE", g.pair_universe_mode)
        g.pair_whitelist = _Env.get_list("PAIR_WHITELIST", g.pair_whitelist)
        g.pair_regex = _Env.get("PAIR_REGEX", g.pair_regex)

        g.primary_quote = _Env.get("PRIMARY_QUOTE", g.primary_quote)
        g.min_usdc = _Env.get_float("MIN_USDC", g.min_usdc)
        g.wallet_alias_by_quote = _Env.get_dict("WALLET_ALIAS_BY_QUOTE", g.wallet_alias_by_quote)
        g.min_notional_by_exchange_quote = _Env.get_dict("MIN_NOTIONAL_BY_EXCHANGE_QUOTE", g.min_notional_by_exchange_quote)
        g.frontload_enabled = _Env.get_bool("FRONTLOAD_ENABLED", getattr(g, "frontload_enabled", True))
        _enable_branches_env = _Env.get("ENABLE_BRANCHES", None)
        g.enable_branches = _Env.get_dict("ENABLE_BRANCHES", g.enable_branches)
        g.branch_priority = _Env.get_list("BRANCH_PRIORITY", g.branch_priority)
        g.branch_budgets_quote = _Env.get_dict("BRANCH_BUDGETS_QUOTE", g.branch_budgets_quote)

        g.frontload_weights = [float(x) for x in _Env.get_list("FRONTLOAD_WEIGHTS", g.frontload_weights)]
        env_min_frag = _Env.get_dict("MIN_FRAGMENT_QUOTE", None)
        if env_min_frag:
            g.min_fragment_quote = {str(k).upper(): float(v) for k, v in env_min_frag.items()}
        g.guards = _Env.get_dict("GUARDS", g.guards)
        _vol_slip_ttl_env = _Env.get("VOL_SLIP_TTL", None)

        g.vol_slip_ttl = _Env.get_dict("VOL_SLIP_TTL", g.vol_slip_ttl)
        _slip_ttl_env = _Env.get("SLIP_TTL_S", None)
        _vol_ttl_env = _Env.get("VOL_TTL_S", None)
        _legacy_ttl_present = _slip_ttl_env is not None or _vol_ttl_env is not None
        allow_legacy_ttl = _Env.get_bool("ALLOW_LEGACY_TTL_ENV", False)
        if _legacy_ttl_present and not allow_legacy_ttl:
            mode_upper = str(_Env.get("MODE", g.mode) or "").upper()
            logging.getLogger(__name__).warning(
                "Legacy SLIP_TTL_S/VOL_TTL_S détectés; ignorés (ALLOW_LEGACY_TTL_ENV=0)",
            )
            if mode_upper == "PROD":
                raise ConfigError("CONFIG_SCHEMA_INVALID", "vol_slip_ttl")
        elif _legacy_ttl_present:

            cfg._note_config_error("CONFIG_SCHEMA_INVALID", "vol_slip_ttl")
            logging.getLogger(__name__).warning(
                "Legacy SLIP_TTL_S/VOL_TTL_S détectés; VOL_SLIP_TTL reste la source canonique",
            )
            if _vol_slip_ttl_env is None:
                g.vol_slip_ttl = dict(g.vol_slip_ttl)
                if _slip_ttl_env is not None:
                    g.vol_slip_ttl["slip_s"] = _Env.get_int(
                        "SLIP_TTL_S", g.vol_slip_ttl.get("slip_s", cfg.slip.ttl_s)
                    )
                if _vol_ttl_env is not None:
                    g.vol_slip_ttl["vol_s"] = _Env.get_int(
                        "VOL_TTL_S", g.vol_slip_ttl.get("vol_s", cfg.vol.ttl_s)
                    )
        cfg.pws.ff_pws_strict_dedup_enforced = _Env.get_bool(
            "PWS_STRICT_DEDUP_ENFORCED",
            cfg.pws.ff_pws_strict_dedup_enforced,
        )

        g.ac = _Env.get_dict("AC_CONFIG", g.ac)
        # Overrides individuels Anti-Crossing
        g.ac["enabled"] = _Env.get_bool("AC_ENABLED", g.ac.get("enabled", True))
        g.ac["band_width_ticks"] = _Env.get_int("AC_BAND_WIDTH_TICKS", g.ac.get("band_width_ticks", 1))
        g.ac["ttl_ms"] = _Env.get_float("AC_TTL_MS", g.ac.get("ttl_ms", 200.0))
        g.ac["ttl_ms_binance"] = _Env.get_float("AC_TTL_MS_BINANCE", g.ac.get("ttl_ms_binance", g.ac["ttl_ms"]))
        g.ac["ttl_ms_bybit"] = _Env.get_float("AC_TTL_MS_BYBIT", g.ac.get("ttl_ms_bybit", g.ac["ttl_ms"]))
        g.ac["ttl_ms_coinbase"] = _Env.get_float("AC_TTL_MS_COINBASE", g.ac.get("ttl_ms_coinbase", g.ac["ttl_ms"]))
        g.ac["strategy_tm"] = _Env.get("AC_STRATEGY_TM", g.ac.get("strategy_tm", "reprice"))
        g.ac["strategy_mm"] = _Env.get("AC_STRATEGY_MM", g.ac.get("strategy_mm", "delay"))
        g.ac["reprice_ticks_tm"] = _Env.get_int("AC_REPRICE_TICKS_TM", g.ac.get("reprice_ticks_tm", 1))
        g.ac["reprice_ticks_mm"] = _Env.get_int("AC_REPRICE_TICKS_MM", g.ac.get("reprice_ticks_mm", 1))
        g.ac["delay_ms_tm"] = _Env.get_float("AC_DELAY_MS_TM", g.ac.get("delay_ms_tm", 60.0))
        g.ac["delay_ms_mm"] = _Env.get_float("AC_DELAY_MS_MM", g.ac.get("delay_ms_mm", 90.0))
        g.ac["pods_coord_url"] = _Env.get("PODS_COORD_URL", g.ac.get("pods_coord_url"))
        g.ac["pods_coord_namespace"] = _Env.get("PODS_COORD_NAMESPACE", g.ac.get("pods_coord_namespace", "ac"))
        g.mode = _Env.get("MODE", g.mode)
        g.live_trading_armed = _Env.get_bool("LIVE_TRADING_ARMED", g.live_trading_armed)
        g.restart_mode = _Env.get("RESTART_MODE", g.restart_mode)

        def _get_int_opt(name: str, default: Optional[int]) -> Optional[int]:
            raw = _Env.get(name, None)
            if raw is None:
                return default
            try:
                return int(raw)
            except Exception:
                return default

        g.ws_reload_cap_per_hour = _get_int_opt("WS_RELOAD_CAP_PER_HOUR", g.ws_reload_cap_per_hour)
        g.ws_reload_cooldown_s = _get_int_opt("WS_RELOAD_COOLDOWN_S", g.ws_reload_cooldown_s)
        g.ws_reload_mute_s = _get_int_opt("WS_RELOAD_MUTE_S", g.ws_reload_mute_s)
        g.full_restart_cap_per_hour = _get_int_opt("FULL_RESTART_CAP_PER_HOUR", g.full_restart_cap_per_hour)
        g.full_restart_cooldown_s = _get_int_opt("FULL_RESTART_COOLDOWN_S", g.full_restart_cooldown_s)
        g.full_restart_mute_s = _get_int_opt("FULL_RESTART_MUTE_S", g.full_restart_mute_s)
        g.restart_escalate_after_fails = _Env.get_int("RESTART_ESCALATE_AFTER_FAILS", g.restart_escalate_after_fails)
        g.restart_escalate_delay_s = _Env.get_float("RESTART_ESCALATE_DELAY_S", g.restart_escalate_delay_s)
        g.restart_max_downtime_s = _Env.get_float("RESTART_MAX_DOWNTIME_S", g.restart_max_downtime_s)

        # 👇 Ajoute ce bloc de compat
        if str(g.mode).upper() in ("DEV", "DEVELOPMENT"):
            g.mode = "DRY_RUN"
        mode_upper = str(g.mode or "").upper()

        def _conflict_or_warn(key_a: str, key_b: str, *, field: str, prefer: str) -> None:
            if _Env.get(key_a, None) is None or _Env.get(key_b, None) is None:
                return
            msg = f"Conflicting env keys for {field}: {key_a} + {key_b} (prefer {prefer})"
            if mode_upper == "PROD":
                raise ConfigError("CONFIG_SCHEMA_INVALID", field)
            logging.getLogger(__name__).warning(msg)

        _conflict_or_warn("VOL_SLIP_TTL", "SLIP_TTL_S", field="vol_slip_ttl", prefer="VOL_SLIP_TTL")
        _conflict_or_warn("VOL_SLIP_TTL", "VOL_TTL_S", field="vol_slip_ttl", prefer="VOL_SLIP_TTL")
        _conflict_or_warn(
            "TM_EXPOSURE_TTL_MS",
            "ENGINE_TM_EXPOSURE_TTL_MS",
            field="tm_exposure_ttl_ms",
            prefer="TM_EXPOSURE_TTL_MS",
        )
        _conflict_or_warn(
            "TM_EXPOSURE_TTL_HEDGE_RATIO",
            "TM_NEUTRAL_HEDGE_RATIO",
            field="tm_exposure_ttl_hedge_ratio",
            prefer="TM_EXPOSURE_TTL_HEDGE_RATIO",
        )
        if _pod_region_env is None and _region_alias_env is not None:
            logging.getLogger(__name__).warning(
                "REGION (legacy) utilisé faute de POD_REGION; privilégier POD_REGION",
            )
            g.pod_region = _region_alias_env
        if (
                _pod_region_env is not None
                and _region_alias_env is not None
                and str(_pod_region_env).upper() != str(_region_alias_env).upper()
        ):
            msg = f"POD_REGION={_pod_region_env} ≠ REGION={_region_alias_env}"
            logging.getLogger(__name__).error(msg)
            if str(g.mode).upper() == "PROD":
                raise ConfigError("CONFIG_SCHEMA_INVALID", "pod_region")
            cfg._note_config_error("CONFIG_SCHEMA_INVALID", "pod_region")
        g.pod_region = str(g.pod_region or "EU").upper()
        g.deployment_mode = str(g.deployment_mode or "SPLIT").upper()
        g.feature_switches = _Env.get_dict("FEATURE_SWITCHES", g.feature_switches)
        g.log_dir = _Env.get("LOG_DIR", g.log_dir)
        g.history_dir = _Env.get("HISTORY_DIR", g.history_dir)
        g.rm_capital_move_threshold_usdc = _Env.get_float("RM_CAPITAL_MOVE_THRESHOLD_USDC", cfg.rm.rm_capital_move_threshold_usdc)
        g.rm_capital_move_refresh_max_delay_s = _Env.get_float("RM_CAPITAL_MOVE_REFRESH_MAX_DELAY_S", cfg.rm.rm_capital_move_refresh_max_delay_s)
        g.rm_capital_move_refresh_mode = _Env.get("RM_CAPITAL_MOVE_REFRESH_MODE", cfg.rm.rm_capital_move_refresh_mode)
        g.rm_capital_drift_threshold_pct = _Env.get_float("RM_CAPITAL_DRIFT_THRESHOLD_PCT", cfg.rm.rm_capital_drift_threshold_pct)
        g.rm_ws_balance_resync_min_interval_s = _Env.get_float("RM_WS_BALANCE_RESYNC_MIN_INTERVAL_S", cfg.rm.rm_ws_balance_resync_min_interval_s)
        cfg.rm.rebal_lock_ttl_s = _Env.get_float("REBAL_LOCK_TTL_S", cfg.rm.rebal_lock_ttl_s)
        # --- Observabilité / Alerting ---
        cfg.obs.strict_obs = _Env.get_bool("STRICT_OBS", cfg.obs.strict_obs)
        cfg.obs.log_level = _Env.get("LOG_LEVEL", cfg.obs.log_level)
        cfg.obs.status_port = _Env.get_int("STATUS_PORT", cfg.obs.status_port)
        cfg.obs.expose_metrics_on_status = _Env.get_bool(
            "EXPOSE_METRICS_ON_9110", cfg.obs.expose_metrics_on_status
        )
        cfg.obs.enable_obs_port = _Env.get_bool("OBS_ENABLE_9108", cfg.obs.enable_obs_port)
        cfg.obs.obs_port = _Env.get_int("OBS_PORT", cfg.obs.obs_port)

        # Latency Tracing
        cfg.obs.enable_latency_tracing = _Env.get_bool("ENABLE_LATENCY_TRACING", cfg.obs.enable_latency_tracing)
        cfg.obs.latency_sampling_rate = _Env.get_float("LATENCY_SAMPLING_RATE", cfg.obs.latency_sampling_rate)
        cfg.obs.enable_segment_metrics_router = _Env.get_bool("ENABLE_SEGMENT_METRICS_ROUTER", cfg.obs.enable_segment_metrics_router)
        cfg.obs.enable_segment_metrics_scanner = _Env.get_bool("ENABLE_SEGMENT_METRICS_SCANNER", cfg.obs.enable_segment_metrics_scanner)
        cfg.obs.enable_segment_metrics_rm = _Env.get_bool("ENABLE_SEGMENT_METRICS_RM", cfg.obs.enable_segment_metrics_rm)
        cfg.obs.enable_segment_metrics_engine = _Env.get_bool("ENABLE_SEGMENT_METRICS_ENGINE", cfg.obs.enable_segment_metrics_engine)
        cfg.boot.scanner_proxy_mode = _Env.get("BOOT_SCANNER_PROXY_MODE", cfg.boot.scanner_proxy_mode)
        cfg.boot.scanner_proxy_buffer_maxlen = _Env.get_int(
            "BOOT_SCANNER_PROXY_BUFFER_MAXLEN", cfg.boot.scanner_proxy_buffer_maxlen
        )

        cfg.alerting.telegram.enabled = _Env.get_bool("TELEGRAM_ENABLE", cfg.alerting.telegram.enabled)
        cfg.alerting.telegram.bot_token = _Env.get("TELEGRAM_BOT_TOKEN", cfg.alerting.telegram.bot_token)
        if not cfg.alerting.telegram.bot_token:
            cfg.alerting.telegram.enabled = False
            logging.getLogger(__name__).info("Telegram désactivé (TOKEN manquant)")
        cfg.alerting.telegram.allowed_user_ids = [
            int(x)
            for x in _Env.get_list("TELEGRAM_ALLOWED_USER_IDS", cfg.alerting.telegram.allowed_user_ids)
            if str(x).strip()
        ]
        _generic_chat_id = _Env.get("TELEGRAM_CHAT_ID", None)
        cfg.alerting.telegram.chat_id_info = _Env.get(
            "TELEGRAM_CHAT_ID_INFO", cfg.alerting.telegram.chat_id_info or _generic_chat_id
        )
        cfg.alerting.telegram.chat_id_warn = _Env.get(
            "TELEGRAM_CHAT_ID_WARN", cfg.alerting.telegram.chat_id_warn or _generic_chat_id
        )
        cfg.alerting.telegram.chat_id_crit = _Env.get(
            "TELEGRAM_CHAT_ID_CRIT", cfg.alerting.telegram.chat_id_crit or _generic_chat_id
        )
        cfg.alerting.telegram.ack_pin = _Env.get("TELEGRAM_ACK_PIN", cfg.alerting.telegram.ack_pin)
        cfg.alerting.telegram.require_ack = _Env.get_bool(
            "TELEGRAM_REQUIRE_ACK", cfg.alerting.telegram.require_ack
        )
        cfg.alerting.webhook.url = _Env.get("RESTART_WEBHOOK_URL", cfg.alerting.webhook.url)
        cfg.alerting.webhook.hmac_secret = _Env.get(
            "RESTART_WEBHOOK_HMAC_KEY", cfg.alerting.webhook.hmac_secret
        )
        cfg.alerting.webhook.timeout_s = _Env.get_float("RESTART_WEBHOOK_TIMEOUT_S", cfg.alerting.webhook.timeout_s)
        _Env.get("CONFIG_OVERRIDES", None)  # trigger warning if set (deprecated/no-op)
        # --- Router cfg ---------------------------------------------------
        cfg.router.coalesce_window_ms = _Env.get_int("ROUTER_COALESCE_WINDOW_MS", cfg.router.coalesce_window_ms)
        cfg.router.stale_ms = _Env.get_int("ROUTER_STALE_MS", cfg.router.stale_ms)
        # P0: shards_per_exchange = (BINANCE, BYBIT, COINBASE)
        shards_raw = _Env.get("ROUTER_SHARDS_PER_EXCHANGE", None)
        if shards_raw is None:
            cfg.router.shards_per_exchange = tuple(cfg.router.shards_per_exchange)
        else:
            parsed = _Env.get_list("ROUTER_SHARDS_PER_EXCHANGE", [], sep=",")
            if len(parsed) == 1:
                try:
                    val = int(parsed[0])
                except Exception:
                    raise ConfigError("CONFIG_PARSE_ERROR", "router_shards_per_exchange") from None
                cfg.router.shards_per_exchange = (val, val, val)
            elif len(parsed) == 3:
                try:
                    cfg.router.shards_per_exchange = tuple(int(v) for v in parsed)
                except Exception:
                    raise ConfigError("CONFIG_PARSE_ERROR", "router_shards_per_exchange") from None
            else:
                raise ConfigError("CONFIG_PARSE_ERROR", "router_shards_per_exchange")
        cfg.router.ws_shards_by_exchange = _Env.get_dict(
            "ROUTER_WS_SHARDS", cfg.router.ws_shards_by_exchange
        )
        # Quota global (fallback) + quotas stream-centrics optionnels
        cfg.router.out_queues_maxlen = _Env.get_int("ROUTER_OUT_QUEUES_MAXLEN", cfg.router.out_queues_maxlen)
        cfg.router.out_queues_maxlen_by_kind = _Env.get_dict(
            "ROUTER_OUT_QUEUES_MAXLEN_BY_KIND", cfg.router.out_queues_maxlen_by_kind
        )
        cfg.router.mux_poll_ms = _Env.get_int("ROUTER_MUX_POLL_MS", cfg.router.mux_poll_ms)
        cfg.router.require_l2_first = _Env.get_bool("ROUTER_REQUIRE_L2_FIRST", cfg.router.require_l2_first)
        cfg.router.vol_onchange_bps = _Env.get_float("ROUTER_VOL_ONCHANGE_BPS", cfg.router.vol_onchange_bps)
        cfg.router.slip_onchange_bps = _Env.get_float("ROUTER_SLIP_ONCHANGE_BPS", cfg.router.slip_onchange_bps)
        cfg.router.hb_onchange_bps = _Env.get_float("ROUTER_HB_ONCHANGE_BPS", cfg.router.hb_onchange_bps)
        cfg.router.coalesce_maxlen = _Env.get_int("ROUTER_COALESCE_MAXLEN", cfg.router.coalesce_maxlen)
        cfg.router.ws_source_backpressure_cooldown_s = _Env.get_float(
            "ROUTER_WS_SOURCE_BACKPRESSURE_COOLDOWN_S",
            cfg.router.ws_source_backpressure_cooldown_s,
        )
        drop_policy_raw = _Env.get("ROUTER_DROP_POLICY", None)
        drop_policy_default = dict(cfg.router.drop_policy)
        if drop_policy_raw is not None:
            drop_policy = _Env.get_dict("ROUTER_DROP_POLICY", cfg.router.drop_policy)
            mode = str(getattr(getattr(cfg, "g", None), "mode", "") or "").upper()
            if mode == "PROD" and drop_policy != drop_policy_default:
                raise ConfigError("CONFIG_SCHEMA_INVALID", "router.drop_policy")
            cfg.router.drop_policy = drop_policy
        cfg.router.deque_maxlen_per_ex = _Env.get_dict("ROUTER_DEQUE_MAXLEN_PER_EX", cfg.router.deque_maxlen_per_ex)
        cfg.router.backpressure = _Env.get_dict("ROUTER_BACKPRESSURE", cfg.router.backpressure)
        cfg.router.cb_coalesce_bump_ms = _Env.get_int(
            "ROUTER_CB_COALESCE_BUMP_MS", cfg.router.cb_coalesce_bump_ms
        )
        cfg.router.topic_max_hz = _Env.get_dict("ROUTER_TOPIC_MAX_HZ", cfg.router.topic_max_hz)
        cfg.router.staleness_mode = _Env.get("ROUTER_STALENESS_MODE", cfg.router.staleness_mode)
        cfg.router.deterministic_backpressure = _Env.get_bool(
            "ROUTER_DETERMINISTIC_BACKPRESSURE", cfg.router.deterministic_backpressure
        )
        cfg.router.scanner_queue_maxlen = _Env.get_int(
            "ROUTER_SCANNER_QUEUE_MAXLEN", cfg.router.scanner_queue_maxlen
        )
        cfg.router.scanner_queue_drop_policy = _Env.get(
            "ROUTER_SCANNER_QUEUE_DROP_POLICY", cfg.router.scanner_queue_drop_policy
        )
        cfg.router.fail_fast_on_event_exception = _Env.get_bool(
            "ROUTER_FAIL_FAST_ON_EVENT_EXCEPTION", cfg.router.fail_fast_on_event_exception
        )

        cfg.ws_public.ws_backoff = _Env.get_dict("WS_BACKOFF", cfg.ws_public.ws_backoff)
        cfg.ws_public.ws_backoff_seed = _Env.get_int("WS_BACKOFF_SEED", cfg.ws_public.ws_backoff_seed or 0) or None
        cfg.ws_public.update_pairs_jitter_ms = _Env.get_int(
            "WS_UPDATE_PAIRS_JITTER_MS", cfg.ws_public.update_pairs_jitter_ms
        )
        cfg.ws_public.connect_timeout_s = _Env.get_int("WS_CONNECT_TIMEOUT_S", cfg.ws_public.connect_timeout_s)
        cfg.ws_public.out_queue_put_timeout_s = _Env.get_float(
            "WS_OUT_QUEUE_PUT_TIMEOUT_S", cfg.ws_public.out_queue_put_timeout_s
        )
        cfg.ws_public.staleness_interval_s = _Env.get_float(
            "WS_STALENESS_INTERVAL_S", cfg.ws_public.staleness_interval_s
        )
        _slo_env = _Env.get("WS_STALENESS_SLO_S", None)
        cfg.ws_public.staleness_slo_s = (
            float(_slo_env)
            if _slo_env is not None
            else cfg.ws_public.staleness_slo_s
        )
        cfg.ws_public.disabled_exchanges = _Env.get_list(
            "WS_DISABLED_EXCHANGES", cfg.ws_public.disabled_exchanges
        )
        cfg.ws_public.read_timeout_s = _Env.get_int("WS_READ_TIMEOUT_S", cfg.ws_public.read_timeout_s)
        cfg.ws_public.coinbase_l2_max_hz = _Env.get_float(
            "COINBASE_L2_MAX_HZ", cfg.ws_public.coinbase_l2_max_hz
        )
        cfg.ws_public.bybit_l2_max_hz = _Env.get_float(
            "BYBIT_L2_MAX_HZ", cfg.ws_public.bybit_l2_max_hz
        )

        # NEW: Parité de connexion par exchange (BINANCE_WS_CONNECT_TIMEOUT_S, etc.)
        for ex in ["BINANCE", "BYBIT", "COINBASE"]:
            v_conn = _Env.get_int(f"{ex}_WS_CONNECT_TIMEOUT_S", None)
            if v_conn is not None: cfg.ws_public.connect_timeout_s_by_ex[ex] = v_conn
            
            v_read = _Env.get_int(f"{ex}_WS_READ_TIMEOUT_S", None)
            if v_read is not None: cfg.ws_public.read_timeout_s_by_ex[ex] = v_read
            
            v_ping = _Env.get_int(f"{ex}_WS_PING_INTERVAL_S", None)
            if v_ping is not None: cfg.ws_public.ping_interval_s_by_ex[ex] = v_ping
            
            v_pong = _Env.get_int(f"{ex}_WS_PONG_TIMEOUT_S", None)
            if v_pong is not None: cfg.ws_public.pong_timeout_s_by_ex[ex] = v_pong
        # Budget journalier par stratégie (en quote, ex: {"TT": 1_000_000, "TM": 500_000})
        cfg.rm.daily_strategy_budget_quote = _Env.get_dict(
            "DAILY_STRATEGY_BUDGET_QUOTE", cfg.rm.daily_strategy_budget_quote
        ) or {}
        cfg.daily_strategy_budget_quote = cfg.rm.daily_strategy_budget_quote

        cfg.discovery.http_timeout_s = _Env.get_int("DISCOVERY_HTTP_TIMEOUT_S", cfg.discovery.http_timeout_s)
        cfg.wd.interval_s = _Env.get_float("WD_INTERVAL_S", cfg.wd.interval_s)
        cfg.wd.cooldown_s = _Env.get_float("WD_COOLDOWN_S", cfg.wd.cooldown_s)
        cfg.wd.persistence_s = _Env.get_float("WD_PERSISTENCE_S", cfg.wd.persistence_s)
        cfg.wd.persistence_cycles = _Env.get_int("WD_PERSISTENCE_CYCLES", cfg.wd.persistence_cycles)
        cfg.wd.notify_only_default = _Env.get_bool("WATCHDOG_NOTIFY_ONLY", cfg.wd.notify_only_default)

        cfg.wd.router_interval_s = _Env.get_float("WD_ROUTER_INTERVAL_S", cfg.wd.router_interval_s)
        cfg.wd.router_health_stale_ms = _Env.get_int("ROUTER_HEALTH_STALE_MS", cfg.wd.router_health_stale_ms)
        cfg.wd.router_health_min_coverage_ratio = _Env.get_float(
            "ROUTER_HEALTH_MIN_COVERAGE_RATIO", cfg.wd.router_health_min_coverage_ratio
        )
        cfg.wd.router_health_queue_max = _Env.get_int("ROUTER_HEALTH_QUEUE_MAX", cfg.wd.router_health_queue_max)
        cfg.wd.router_r2s_warn_ms = _Env.get_int("ROUTER_R2S_WARN_MS", cfg.wd.router_r2s_warn_ms)
        cfg.wd.router_r2s_crit_ms = _Env.get_int("ROUTER_R2S_CRIT_MS", cfg.wd.router_r2s_crit_ms)
        cfg.wd.router_backlog_warn = _Env.get_float("ROUTER_BACKLOG_WARN", cfg.wd.router_backlog_warn)
        cfg.wd.router_backlog_crit = _Env.get_float("ROUTER_BACKLOG_CRIT", cfg.wd.router_backlog_crit)
        cfg.wd.router_escalate_after_cycles = _Env.get_int(
            "ROUTER_ESCALATE_AFTER_CYCLES", cfg.wd.router_escalate_after_cycles
        )
        cfg.wd.ws_public_interval_s = _Env.get_float("WD_WS_PUBLIC_INTERVAL_S", cfg.wd.ws_public_interval_s)
        cfg.wd.ws_public_stale_warn_ms = _Env.get_int("WD_WS_PUBLIC_STALE_WARN_MS", cfg.wd.ws_public_stale_warn_ms)
        cfg.wd.ws_public_stale_crit_ms = _Env.get_int("WD_WS_PUBLIC_STALE_CRIT_MS", cfg.wd.ws_public_stale_crit_ms)
        cfg.wd.ws_public_hb_gap_warn_s = _Env.get_int("WD_WS_PUBLIC_HB_GAP_WARN_S", cfg.wd.ws_public_hb_gap_warn_s)
        cfg.wd.ws_public_hb_gap_crit_s = _Env.get_int("WD_WS_PUBLIC_HB_GAP_CRIT_S", cfg.wd.ws_public_hb_gap_crit_s)
        cfg.wd.ws_public_resub_warn_per_min = _Env.get_int(
            "WD_WS_PUBLIC_RESUB_WARN_PER_MIN", cfg.wd.ws_public_resub_warn_per_min
        )
        cfg.wd.ws_public_resub_crit_per_min = _Env.get_int(
            "WD_WS_PUBLIC_RESUB_CRIT_PER_MIN", cfg.wd.ws_public_resub_crit_per_min
        )
        cfg.wd.ws_public_escalate_after_cycles = _Env.get_int(
            "WD_WS_PUBLIC_ESCALATE_AFTER_CYCLES", cfg.wd.ws_public_escalate_after_cycles
        )

        cfg.wd.private_ws_interval_s = _Env.get_float("WD_PRIVATE_WS_INTERVAL_S", cfg.wd.private_ws_interval_s)
        cfg.wd.private_ws_hb_warn_s = _Env.get_int("WD_PRIVATE_WS_HB_WARN_S", cfg.wd.private_ws_hb_warn_s)
        cfg.wd.private_ws_hb_crit_s = _Env.get_int("WD_PRIVATE_WS_HB_CRIT_S", cfg.wd.private_ws_hb_crit_s)
        cfg.wd.private_ws_ack_warn_ms = _Env.get_int("WD_PRIVATE_WS_ACK_WARN_MS", cfg.wd.private_ws_ack_warn_ms)
        cfg.wd.private_ws_ack_crit_ms = _Env.get_int("WD_PRIVATE_WS_ACK_CRIT_MS", cfg.wd.private_ws_ack_crit_ms)
        cfg.wd.private_ws_fill_warn_ms = _Env.get_int("WD_PRIVATE_WS_FILL_WARN_MS", cfg.wd.private_ws_fill_warn_ms)
        cfg.wd.private_ws_fill_crit_ms = _Env.get_int("WD_PRIVATE_WS_FILL_CRIT_MS", cfg.wd.private_ws_fill_crit_ms)
        cfg.wd.private_ws_queue_warn_ratio = _Env.get_float(
            "WD_PRIVATE_WS_QUEUE_WARN_RATIO", cfg.wd.private_ws_queue_warn_ratio
        )
        cfg.wd.private_ws_queue_crit_ratio = _Env.get_float(
            "WD_PRIVATE_WS_QUEUE_CRIT_RATIO", cfg.wd.private_ws_queue_crit_ratio
        )
        cfg.wd.private_ws_rate429_warn = _Env.get_float("WD_PRIVATE_WS_RATE429_WARN", cfg.wd.private_ws_rate429_warn)
        cfg.wd.private_ws_rate429_crit = _Env.get_float("WD_PRIVATE_WS_RATE429_CRIT", cfg.wd.private_ws_rate429_crit)
        cfg.wd.private_ws_dedup_warn = _Env.get_float("WD_PRIVATE_WS_DEDUP_WARN", cfg.wd.private_ws_dedup_warn)
        cfg.wd.private_ws_dedup_crit = _Env.get_float("WD_PRIVATE_WS_DEDUP_CRIT", cfg.wd.private_ws_dedup_crit)
        cfg.wd.private_ws_escalate_after_cycles = _Env.get_int(
            "WD_PRIVATE_WS_ESCALATE_AFTER_CYCLES", cfg.wd.private_ws_escalate_after_cycles
        )
        cfg.wd.private_ws_stale_ms = _Env.get_int("PRIVATE_WS_STALE_MS", cfg.wd.private_ws_stale_ms)

        cfg.wd.engine_interval_s = _Env.get_float("WD_ENGINE_INTERVAL_S", cfg.wd.engine_interval_s)
        cfg.wd.engine_submit_ack_warn_ms = _Env.get_int(
            "WD_ENGINE_SUBMIT_ACK_WARN_MS", cfg.wd.engine_submit_ack_warn_ms
        )
        cfg.wd.engine_submit_ack_crit_ms = _Env.get_int(
            "WD_ENGINE_SUBMIT_ACK_CRIT_MS", cfg.wd.engine_submit_ack_crit_ms
        )
        cfg.wd.engine_ack_fill_warn_ms = _Env.get_int(
            "WD_ENGINE_ACK_FILL_WARN_MS", cfg.wd.engine_ack_fill_warn_ms
        )
        cfg.wd.engine_ack_fill_crit_ms = _Env.get_int(
            "WD_ENGINE_ACK_FILL_CRIT_MS", cfg.wd.engine_ack_fill_crit_ms
        )
        cfg.wd.engine_timeouts_warn_per_min = _Env.get_float(
            "WD_ENGINE_TIMEOUTS_WARN_PER_MIN", cfg.wd.engine_timeouts_warn_per_min
        )
        cfg.wd.engine_timeouts_crit_per_min = _Env.get_float(
            "WD_ENGINE_TIMEOUTS_CRIT_PER_MIN", cfg.wd.engine_timeouts_crit_per_min
        )
        cfg.wd.engine_retries_warn_per_min = _Env.get_float(
            "WD_ENGINE_RETRIES_WARN_PER_MIN", cfg.wd.engine_retries_warn_per_min
        )
        cfg.wd.engine_retries_crit_per_min = _Env.get_float(
            "WD_ENGINE_RETRIES_CRIT_PER_MIN", cfg.wd.engine_retries_crit_per_min
        )
        cfg.wd.engine_panic_hedge_warn_per_min = _Env.get_float(
            "WD_ENGINE_PANIC_HEDGE_WARN_PER_MIN", cfg.wd.engine_panic_hedge_warn_per_min
        )
        cfg.wd.engine_panic_hedge_crit_per_min = _Env.get_float(
            "WD_ENGINE_PANIC_HEDGE_CRIT_PER_MIN", cfg.wd.engine_panic_hedge_crit_per_min
        )
        cfg.wd.engine_queuepos_blocked_warn_per_min = _Env.get_float(
            "WD_ENGINE_QUEUEPOS_BLOCKED_WARN_PER_MIN", cfg.wd.engine_queuepos_blocked_warn_per_min
        )
        cfg.wd.engine_queuepos_blocked_crit_per_min = _Env.get_float(
            "WD_ENGINE_QUEUEPOS_BLOCKED_CRIT_PER_MIN", cfg.wd.engine_queuepos_blocked_crit_per_min
        )
        cfg.wd.engine_escalate_after_cycles = _Env.get_int(
            "WD_ENGINE_ESCALATE_AFTER_CYCLES", cfg.wd.engine_escalate_after_cycles
        )
        cfg.engine.engine_queue_max = _Env.get_int("ENGINE_QUEUE_MAX", cfg.engine.engine_queue_max)
        cfg.wd.engine_queue_max = cfg.engine.engine_queue_max
        cfg.wd.engine_blocked_ms = _Env.get_int("ENGINE_BLOCKED_MS", cfg.wd.engine_blocked_ms)

        cfg.wd.balance_interval_s = _Env.get_float("WD_BALANCE_INTERVAL_S", cfg.wd.balance_interval_s)
        cfg.wd.balance_stale_s = _Env.get_float("BALANCE_STALE_S", cfg.wd.balance_stale_s)
        cfg.wd.balance_error_threshold = _Env.get_int("WD_BALANCE_ERROR_THRESHOLD", cfg.wd.balance_error_threshold)

        cfg.wd.logger_interval_s = _Env.get_float("WD_LOGGER_INTERVAL_S", cfg.wd.logger_interval_s)
        cfg.wd.logger_trade_queue_warn = _Env.get_int("WD_LOGGER_TRADE_QUEUE_WARN", cfg.wd.logger_trade_queue_warn)
        cfg.wd.logger_trade_queue_crit = _Env.get_int("WD_LOGGER_TRADE_QUEUE_CRIT", cfg.wd.logger_trade_queue_crit)
        cfg.wd.logger_queue_stuck_checks = _Env.get_int(
            "WD_LOGGER_QUEUE_STUCK_CHECKS", cfg.wd.logger_queue_stuck_checks
        )
        cfg.wd.logger_writer_stall_s = _Env.get_float("WD_LOGGER_WRITER_STALL_S", cfg.wd.logger_writer_stall_s)
        cfg.wd.logger_min_queue_for_writer_stall = _Env.get_int(
            "WD_LOGGER_MIN_QUEUE_FOR_WRITER_STALL", cfg.wd.logger_min_queue_for_writer_stall
        )
        cfg.wd.logger_rotation_stall_factor = _Env.get_float(
            "WD_LOGGER_ROTATION_STALL_FACTOR", cfg.wd.logger_rotation_stall_factor
        )
        cfg.wd.logger_rotation_hard_stall_factor = _Env.get_float(
            "WD_LOGGER_ROTATION_HARD_STALL_FACTOR", cfg.wd.logger_rotation_hard_stall_factor
        )

        cfg.wd.slippage_interval_s = _Env.get_float("WD_SLIPPAGE_INTERVAL_S", cfg.wd.slippage_interval_s)
        cfg.wd.slippage_age_warn_s = _Env.get_float("WD_SLIPPAGE_AGE_WARN_S", cfg.wd.slippage_age_warn_s)
        cfg.wd.slippage_age_crit_s = _Env.get_float("WD_SLIPPAGE_AGE_CRIT_S", cfg.wd.slippage_age_crit_s)
        cfg.wd.slippage_p95_warn_bps = _Env.get_float("WD_SLIPPAGE_P95_WARN_BPS", cfg.wd.slippage_p95_warn_bps)
        cfg.wd.slippage_p99_crit_bps = _Env.get_float("WD_SLIPPAGE_P99_CRIT_BPS", cfg.wd.slippage_p99_crit_bps)
        cfg.wd.slippage_escalate_after_cycles = _Env.get_int(
            "WD_SLIPPAGE_ESCALATE_AFTER_CYCLES", cfg.wd.slippage_escalate_after_cycles
        )

        cfg.wd.volatility_interval_s = _Env.get_float("WD_VOLATILITY_INTERVAL_S", cfg.wd.volatility_interval_s)
        cfg.wd.volatility_age_warn_s = _Env.get_float("WD_VOLATILITY_AGE_WARN_S", cfg.wd.volatility_age_warn_s)
        cfg.wd.volatility_age_crit_s = _Env.get_float("WD_VOLATILITY_AGE_CRIT_S", cfg.wd.volatility_age_crit_s)
        cfg.wd.volatility_soft_cap_bps = _Env.get_float("WD_VOLATILITY_SOFT_CAP_BPS", cfg.wd.volatility_soft_cap_bps)
        cfg.wd.volatility_hard_cap_bps = _Env.get_float("WD_VOLATILITY_HARD_CAP_BPS", cfg.wd.volatility_hard_cap_bps)
        cfg.wd.volatility_p95_warn_bps = _Env.get_float("WD_VOLATILITY_P95_WARN_BPS", cfg.wd.volatility_p95_warn_bps)
        cfg.wd.volatility_p99_crit_bps = _Env.get_float("WD_VOLATILITY_P99_CRIT_BPS", cfg.wd.volatility_p99_crit_bps)
        cfg.wd.volatility_z_warn = _Env.get_float("WD_VOLATILITY_Z_WARN", cfg.wd.volatility_z_warn)
        cfg.wd.volatility_z_crit = _Env.get_float("WD_VOLATILITY_Z_CRIT", cfg.wd.volatility_z_crit)
        cfg.wd.volatility_escalate_after_cycles = _Env.get_int(
            "WD_VOLATILITY_ESCALATE_AFTER_CYCLES", cfg.wd.volatility_escalate_after_cycles
        )

        cfg.wd.opportunity_interval_s = _Env.get_float("WD_OPPORTUNITY_INTERVAL_S", cfg.wd.opportunity_interval_s)
        cfg.wd.opportunity_effective_ratio_warn = _Env.get_float(
            "WD_OPPORTUNITY_EFFECTIVE_RATIO_WARN", cfg.wd.opportunity_effective_ratio_warn
        )
        cfg.wd.opportunity_effective_ratio_crit = _Env.get_float(
            "WD_OPPORTUNITY_EFFECTIVE_RATIO_CRIT", cfg.wd.opportunity_effective_ratio_crit
        )
        cfg.wd.opportunity_decision_p95_warn_ms = _Env.get_int(
            "WD_OPPORTUNITY_DECISION_P95_WARN_MS", cfg.wd.opportunity_decision_p95_warn_ms
        )
        cfg.wd.opportunity_decision_p95_crit_ms = _Env.get_int(
            "WD_OPPORTUNITY_DECISION_P95_CRIT_MS", cfg.wd.opportunity_decision_p95_crit_ms
        )
        cfg.wd.opportunity_emit_p95_warn_ms = _Env.get_int(
            "WD_OPPORTUNITY_EMIT_P95_WARN_MS", cfg.wd.opportunity_emit_p95_warn_ms
        )
        cfg.wd.opportunity_emit_p95_crit_ms = _Env.get_int(
            "WD_OPPORTUNITY_EMIT_P95_CRIT_MS", cfg.wd.opportunity_emit_p95_crit_ms
        )
        cfg.wd.opportunity_backlog_warn_ratio = _Env.get_float(
            "WD_OPPORTUNITY_BACKLOG_WARN_RATIO", cfg.wd.opportunity_backlog_warn_ratio
        )
        cfg.wd.opportunity_backlog_crit_ratio = _Env.get_float(
            "WD_OPPORTUNITY_BACKLOG_CRIT_RATIO", cfg.wd.opportunity_backlog_crit_ratio
        )
        cfg.wd.opportunity_dedup_warn_per_min = _Env.get_float(
            "WD_OPPORTUNITY_DEDUP_WARN_PER_MIN", cfg.wd.opportunity_dedup_warn_per_min
        )
        cfg.wd.opportunity_dedup_crit_per_min = _Env.get_float(
            "WD_OPPORTUNITY_DEDUP_CRIT_PER_MIN", cfg.wd.opportunity_dedup_crit_per_min
        )
        cfg.wd.opportunity_rejection_ratio_warn = _Env.get_float(
            "WD_OPPORTUNITY_REJECTION_RATIO_WARN", cfg.wd.opportunity_rejection_ratio_warn
        )
        cfg.wd.opportunity_rejection_ratio_crit = _Env.get_float(
            "WD_OPPORTUNITY_REJECTION_RATIO_CRIT", cfg.wd.opportunity_rejection_ratio_crit
        )
        cfg.wd.opportunity_slip_age_warn_s = _Env.get_float(
            "WD_OPPORTUNITY_SLIP_AGE_WARN_S", cfg.wd.opportunity_slip_age_warn_s
        )
        cfg.wd.opportunity_slip_age_crit_s = _Env.get_float(
            "WD_OPPORTUNITY_SLIP_AGE_CRIT_S", cfg.wd.opportunity_slip_age_crit_s
        )
        cfg.wd.opportunity_vol_age_warn_s = _Env.get_float(
            "WD_OPPORTUNITY_VOL_AGE_WARN_S", cfg.wd.opportunity_vol_age_warn_s
        )
        cfg.wd.opportunity_vol_age_crit_s = _Env.get_float(
            "WD_OPPORTUNITY_VOL_AGE_CRIT_S", cfg.wd.opportunity_vol_age_crit_s
        )
        cfg.wd.opportunity_fees_age_warn_s = _Env.get_float(
            "WD_OPPORTUNITY_FEES_AGE_WARN_S", cfg.wd.opportunity_fees_age_warn_s
        )
        cfg.wd.opportunity_fees_age_crit_s = _Env.get_float(
            "WD_OPPORTUNITY_FEES_AGE_CRIT_S", cfg.wd.opportunity_fees_age_crit_s
        )
        cfg.wd.opportunity_scanner_err_warn_per_min = _Env.get_float(
            "WD_OPPORTUNITY_SCANNER_ERR_WARN_PER_MIN", cfg.wd.opportunity_scanner_err_warn_per_min
        )
        cfg.wd.opportunity_scanner_err_crit_per_min = _Env.get_float(
            "WD_OPPORTUNITY_SCANNER_ERR_CRIT_PER_MIN", cfg.wd.opportunity_scanner_err_crit_per_min
        )
        cfg.wd.opportunity_escalate_after_cycles = _Env.get_int(
            "WD_OPPORTUNITY_ESCALATE_AFTER_CYCLES", cfg.wd.opportunity_escalate_after_cycles
        )

        cfg.wd.discovery_interval_s = _Env.get_float("WD_DISCOVERY_INTERVAL_S", cfg.wd.discovery_interval_s)
        cfg.wd.discovery_min_change_ratio = _Env.get_float(
            "WD_DISCOVERY_MIN_CHANGE_RATIO", cfg.wd.discovery_min_change_ratio
        )
        cfg.wd.discovery_confirm_ticks = _Env.get_int("WD_DISCOVERY_CONFIRM_TICKS", cfg.wd.discovery_confirm_ticks)
        cfg.wd.discovery_dwell_ticks = _Env.get_int("WD_DISCOVERY_DWELL_TICKS", cfg.wd.discovery_dwell_ticks)
        cfg.wd.discovery_max_refresh_gap_s = _Env.get_int(
            "WD_DISCOVERY_MAX_REFRESH_GAP_S", cfg.wd.discovery_max_refresh_gap_s
        )

        cfg.wd.rm_interval_s = _Env.get_float("WD_RM_INTERVAL_S", cfg.wd.rm_interval_s)
        cfg.wd.rm_tick_warn_ms = _Env.get_int("WD_RM_TICK_WARN_MS", cfg.wd.rm_tick_warn_ms)
        cfg.wd.rm_tick_crit_ms = _Env.get_int("WD_RM_TICK_CRIT_MS", cfg.wd.rm_tick_crit_ms)
        cfg.wd.rm_hb_warn_s = _Env.get_int("WD_RM_HB_WARN_S", cfg.wd.rm_hb_warn_s)
        cfg.wd.rm_hb_crit_s = _Env.get_int("WD_RM_HB_CRIT_S", cfg.wd.rm_hb_crit_s)
        cfg.wd.rm_slip_age_warn_s = _Env.get_float("WD_RM_SLIP_AGE_WARN_S", cfg.wd.rm_slip_age_warn_s)
        cfg.wd.rm_slip_age_crit_s = _Env.get_float("WD_RM_SLIP_AGE_CRIT_S", cfg.wd.rm_slip_age_crit_s)
        cfg.wd.rm_vol_age_warn_s = _Env.get_float("WD_RM_VOL_AGE_WARN_S", cfg.wd.rm_vol_age_warn_s)
        cfg.wd.rm_vol_age_crit_s = _Env.get_float("WD_RM_VOL_AGE_CRIT_S", cfg.wd.rm_vol_age_crit_s)
        cfg.wd.rm_shadow_bias_warn_bps = _Env.get_float(
            "WD_RM_SHADOW_BIAS_WARN_BPS", cfg.wd.rm_shadow_bias_warn_bps
        )
        cfg.wd.rm_shadow_bias_crit_bps = _Env.get_float(
            "WD_RM_SHADOW_BIAS_CRIT_BPS", cfg.wd.rm_shadow_bias_crit_bps
        )
        cfg.wd.rm_queuepos_warn_per_min = _Env.get_float(
            "WD_RM_QUEUEPOS_WARN_PER_MIN", cfg.wd.rm_queuepos_warn_per_min
        )
        cfg.wd.rm_queuepos_crit_per_min = _Env.get_float(
            "WD_RM_QUEUEPOS_CRIT_PER_MIN", cfg.wd.rm_queuepos_crit_per_min
        )
        cfg.wd.rm_severe_warn_s = _Env.get_int("WD_RM_SEVERE_WARN_S", cfg.wd.rm_severe_warn_s)
        cfg.wd.rm_severe_crit_s = _Env.get_int("WD_RM_SEVERE_CRIT_S", cfg.wd.rm_severe_crit_s)
        cfg.wd.rm_escalate_after_cycles = _Env.get_int(
            "WD_RM_ESCALATE_AFTER_CYCLES", cfg.wd.rm_escalate_after_cycles
        )
        cfg.discovery.retry_policy = _Env.get_dict("DISCOVERY_RETRY_POLICY", cfg.discovery.retry_policy)
        cfg.discovery.max_inflight_requests = _Env.get_int("DISCOVERY_MAX_INFLIGHT", cfg.discovery.max_inflight_requests)
        cfg.discovery.quotes_allowed = _Env.get_list("DISCOVERY_QUOTES_ALLOWED", cfg.discovery.quotes_allowed)
        _conflict_or_warn(
            "DISCOVERY_MIN_24H_VOL_USD",
            "DISCOVERY_MIN_24H_VOLUME_USD",
            field="discovery.min_24h_volume_usd",
            prefer="DISCOVERY_MIN_24H_VOL_USD",
        )
        cfg.discovery.min_24h_volume_usd = _Env.get_float(
            "DISCOVERY_MIN_24H_VOL_USD", cfg.discovery.min_24h_volume_usd
        )
        # Alias compat pour l’ancienne clé (avec "VOLUME")
        if _Env.get("DISCOVERY_MIN_24H_VOL_USD", None) is None:
            cfg.discovery.min_24h_volume_usd = _Env.get_float(
                "DISCOVERY_MIN_24H_VOLUME_USD",
                cfg.discovery.min_24h_volume_usd,
            )

        # NEW: seuils quote-aware
        cfg.discovery.min_quote_volume_usdc = _Env.get_float(
            "DISCOVERY_MIN_QUOTE_VOLUME_USDC",
            cfg.discovery.min_quote_volume_usdc
        )
        cfg.discovery.min_quote_volume_eur = _Env.get_float(
            "DISCOVERY_MIN_QUOTE_VOLUME_EUR",
            cfg.discovery.min_quote_volume_eur
        )
        # Discovery : Quotes autorisées (USDC, USDT, EUR)
        cfg.discovery.quotes_allowed = _Env.get_list("DISCOVERY_QUOTES", ["USDC", "USDT", "EUR"])
        cfg.discovery.enabled = _Env.get_bool("DISCOVERY_ENABLED", cfg.discovery.enabled)
        cfg.discovery.top_n = _Env.get_int("DISCOVERY_TOP_N", cfg.discovery.top_n)
        cfg.discovery.min_24h_volume_usd = _Env.get_float("DISCOVERY_MIN_24H_VOLUME_USD", cfg.discovery.min_24h_volume_usd)
        cfg.discovery.whitelist = _Env.get_list("DISCOVERY_WHITELIST", cfg.discovery.whitelist)
        cfg.discovery.blacklist = _Env.get_list("DISCOVERY_BLACKLIST", cfg.discovery.blacklist)
        # soglie per-quote (opzionali)
        v_usdc = _Env.get_float("DISCOVERY_MIN_QUOTE_VOLUME_USDC", None)
        v_eur = _Env.get_float("DISCOVERY_MIN_QUOTE_VOLUME_EUR", None)
        if v_usdc is not None or v_eur is not None:
            mv = dict(cfg.discovery.min_quote_volume_by_quote or {})
            if v_usdc is not None: mv["USDC"] = float(v_usdc)
            if v_eur is not None: mv["EUR"] = float(v_eur)
            cfg.discovery.min_quote_volume_by_quote = mv

        # --- Fallback coppie (g.pairs) ---
        pairs_list = _Env.get_list("PAIRS", [])
        if pairs_list:
            cfg.g.pairs = [str(p).strip().upper() for p in pairs_list if str(p).strip()]

        if cfg.g.pairs and cfg.g.pair_whitelist:
            msg = "PAIR_WHITELIST ignored because PAIRS is set"
            if mode_upper == "PROD":
                raise ConfigError("CONFIG_SCHEMA_INVALID", "pair_whitelist")
            logging.getLogger(__name__).warning(msg)
        if cfg.g.pair_whitelist and not cfg.discovery.whitelist:
            cfg.discovery.whitelist = list(cfg.g.pair_whitelist)

        # --- RM caps richiesti dal RiskManager (canonique cfg.rm avec compat racine) ---
        def _float_or_none(name: str):
            v = _Env.get(name)
            if v in (None, "", "None"):
                return None
            try:
                return float(v)
            except Exception:
                return None

        inv = _float_or_none("INVENTORY_CAP_QUOTE")
        if inv is None:
            inv = _float_or_none("INVENTORY_CAP_USD")
        if inv is None:
            inv = getattr(cfg.rm, "inventory_cap_quote", 1500.0)
        cfg.rm.inventory_cap_quote = float(inv)

        buf = _float_or_none("MIN_BUFFER_QUOTE")
        if buf is None:
            buf = _float_or_none("MIN_BUFFER_USD")
        if buf is None:
            buf = getattr(cfg.rm, "min_buffer_quote", 0.0)
        cfg.rm.min_buffer_quote = float(buf)
        # TTL balances par alias (OK / DEGRADED / BLOCKED)
        cfg.rm.balance_ttl_s_normal = _Env.get_float(
            "RM_BALANCE_TTL_S_NORMAL",
            getattr(cfg.rm, "balance_ttl_s_normal", 60.0),
        )
        cfg.rm.balance_ttl_s_degraded = _Env.get_float(
            "RM_BALANCE_TTL_S_DEGRADED",
            getattr(cfg.rm, "balance_ttl_s_degraded", 180.0),
        )
        cfg.rm.balance_ttl_s_block = _Env.get_float(
            "RM_BALANCE_TTL_S_BLOCK",
            getattr(cfg.rm, "balance_ttl_s_block", 600.0),
        )
        cfg.rm.inv_soft_drift_pct = _Env.get_float("INV_SOFT_DRIFT_PCT", cfg.rm.inv_soft_drift_pct)
        cfg.rm.inv_hard_drift_pct = _Env.get_float("INV_HARD_DRIFT_PCT", cfg.rm.inv_hard_drift_pct)
        policy_raw = _Env.get(
            "RM_BALANCE_UNKNOWN_POLICY",
            getattr(cfg.rm, "balance_unknown_policy", "DEGRADED"),
        )
        policy_val = str(policy_raw or "DEGRADED").upper()
        if policy_val not in {"NEUTRAL", "DEGRADED", "BLOCKED"}:
            raise ConfigError("CONFIG_SCHEMA_INVALID", "rm.balance_unknown_policy")
        cfg.rm.balance_unknown_policy = policy_val
        cfg.rm.transfer_submitted_timeout_s = _Env.get_float(
            "TRANSFER_SUBMITTED_TIMEOUT_S",
            getattr(cfg.rm, "transfer_submitted_timeout_s", 300.0),
        )
        retry_policy = _Env.get_dict(
            "TRANSFER_RETRY_POLICY",
            getattr(cfg.rm, "transfer_retry_policy", {}),
        )
        if isinstance(retry_policy, dict):
            cfg.rm.transfer_retry_policy = retry_policy


        # Alias critiques (accélération des modes SEVERE)
        crit_aliases = _Env.get_list(
            "RM_CRITICAL_ALIASES",
            getattr(cfg, "RM_CRITICAL_ALIASES", []),
        )
        cfg.RM_CRITICAL_ALIASES = [
            a.strip() for a in (crit_aliases or []) if isinstance(a, str) and a.strip()
        ]

        # Temps min (s) en statut LOW/CRITICAL sur tokens de frais avant escalade
        cfg.RM_FEE_LOW_MIN_SECONDS = _Env.get_float(
            "RM_FEE_LOW_MIN_SECONDS",
            getattr(cfg, "RM_FEE_LOW_MIN_SECONDS", 60.0),
        )

        # Seuils Reconciler "miss" / minute (CONSTRAINED / SEVERE)
        cfg.RM_RECO_MISS_PER_MINUTE_CONSTRAINED = _Env.get_int(
            "RM_RECO_MISS_PER_MINUTE_CONSTRAINED",
            getattr(cfg, "RM_RECO_MISS_PER_MINUTE_CONSTRAINED", 30),
        )
        cfg.RM_RECO_MISS_PER_MINUTE_SEVERE = _Env.get_int(
            "RM_RECO_MISS_PER_MINUTE_SEVERE",
            getattr(cfg, "RM_RECO_MISS_PER_MINUTE_SEVERE", 60),
        )

        # Seuils PrivateWS (heartbeat & latences) pour overlays RM
        cfg.RM_PWS_HEARTBEAT_GAP_CONSTRAINED_S = _Env.get_float(
            "RM_PWS_HEARTBEAT_GAP_CONSTRAINED_S",
            getattr(cfg, "RM_PWS_HEARTBEAT_GAP_CONSTRAINED_S", 6.0),
        )
        cfg.RM_PWS_HEARTBEAT_GAP_SEVERE_S = _Env.get_float(
            "RM_PWS_HEARTBEAT_GAP_SEVERE_S",
            getattr(cfg, "RM_PWS_HEARTBEAT_GAP_SEVERE_S", 12.0),
        )
        cfg.RM_PWS_EVENT_LAG_CONSTRAINED_MS = _Env.get_float(
            "RM_PWS_EVENT_LAG_CONSTRAINED_MS",
            getattr(cfg, "RM_PWS_EVENT_LAG_CONSTRAINED_MS", 600.0),
        )
        cfg.RM_PWS_EVENT_LAG_SEVERE_MS = _Env.get_float(
            "RM_PWS_EVENT_LAG_SEVERE_MS",
            getattr(cfg, "RM_PWS_EVENT_LAG_SEVERE_MS", 1200.0),
        )
        cfg.RM_PWS_ACK_LATENCY_CONSTRAINED_MS = _Env.get_float(
            "RM_PWS_ACK_LATENCY_CONSTRAINED_MS",
            getattr(cfg, "RM_PWS_ACK_LATENCY_CONSTRAINED_MS", 250.0),
        )
        cfg.RM_PWS_ACK_LATENCY_SEVERE_MS = _Env.get_float(
            "RM_PWS_ACK_LATENCY_SEVERE_MS",
            getattr(cfg, "RM_PWS_ACK_LATENCY_SEVERE_MS", 450.0),
        )
        # Params CONSTRAINED / invariant RM
        cfg.RM_CONSTR_TT_MIN_BPS_DELTA = _Env.get_float(
            "RM_CONSTR_TT_MIN_BPS_DELTA",
            getattr(cfg, "RM_CONSTR_TT_MIN_BPS_DELTA", 1.0),
        )
        cfg.RM_CONSTR_TM_MIN_BPS_DELTA = _Env.get_float(
            "RM_CONSTR_TM_MIN_BPS_DELTA",
            getattr(cfg, "RM_CONSTR_TM_MIN_BPS_DELTA", 1.0),
        )
        cfg.RM_CONSTR_CAP_FACTOR = _Env.get_float(
            "RM_CONSTR_CAP_FACTOR",
            getattr(cfg, "RM_CONSTR_CAP_FACTOR", 0.5),
        )
        cfg.RM_CONSTR_IOC_ONLY = _Env.get_bool(
            "RM_CONSTR_IOC_ONLY",
            getattr(cfg, "RM_CONSTR_IOC_ONLY", False),
        )
        cfg.RM_INVARIANT_STRICT = _Env.get_bool(
            "RM_INVARIANT_STRICT",
            getattr(cfg, "RM_INVARIANT_STRICT", False),
        )
        cfg.RM_PWS_FILL_LATENCY_CONSTRAINED_MS = _Env.get_float(
            "RM_PWS_FILL_LATENCY_CONSTRAINED_MS",
            getattr(cfg, "RM_PWS_FILL_LATENCY_CONSTRAINED_MS", 700.0),
        )
        cfg.RM_PWS_FILL_LATENCY_SEVERE_MS = _Env.get_float(
            "RM_PWS_FILL_LATENCY_SEVERE_MS",
            getattr(cfg, "RM_PWS_FILL_LATENCY_SEVERE_MS", 1200.0),
        )
        cfg.rm.base_min_bps = _Env.get_float("RM_BASE_MIN_BPS", cfg.rm.base_min_bps)
        cfg.rm.dynamic_k = _Env.get_float("RM_DYNAMIC_K", cfg.rm.dynamic_k)
        cfg.rm.min_bps_floor = _Env.get_float("RM_MIN_BPS_FLOOR", cfg.rm.min_bps_floor)
        cfg.rm.min_bps_cap = _Env.get_float("RM_MIN_BPS_CAP", cfg.rm.min_bps_cap)
        cfg.rm.ttl_factor_tt_degraded = _Env.get_float(
            "RM_TTL_FACTOR_TT_DEGRADED", cfg.rm.ttl_factor_tt_degraded
        )
        cfg.rm.ttl_factor_tm_degraded = _Env.get_float(
            "RM_TTL_FACTOR_TM_DEGRADED", cfg.rm.ttl_factor_tm_degraded
        )
        cfg.rm.ttl_factor_reb_degraded = _Env.get_float(
            "RM_TTL_FACTOR_REB_DEGRADED", cfg.rm.ttl_factor_reb_degraded
        )
        cfg.rm.ttl_factor_mm_degraded = _Env.get_float(
            "RM_TTL_FACTOR_MM_DEGRADED", cfg.rm.ttl_factor_mm_degraded
        )
        cfg.rm.mode_tick_interval_s = _Env.get_float(
            "RM_MODE_TICK_INTERVAL_S",
            getattr(cfg.rm, "mode_tick_interval_s", 1.0),
        )

        # Min % fee tokens utilisé par RM (fallback si non piloté via balances.*)
        cfg.RM_FEE_TOKEN_MIN_PCT = _Env.get_float(
            "RM_FEE_TOKEN_MIN_PCT",
            getattr(cfg, "RM_FEE_TOKEN_MIN_PCT", 5.0),
        )
        rm_knobs = cfg.rm.switch_knobs
        rm_knobs.enter_hyst_s = _Env.get_int("RM_SWITCH_ENTER_HYST_S", rm_knobs.enter_hyst_s)
        rm_knobs.exit_hyst_s = _Env.get_int("RM_SWITCH_EXIT_HYST_S", rm_knobs.exit_hyst_s)
        rm_knobs.mode_timeout_s = _Env.get_int("RM_MODE_TIMEOUT_S", rm_knobs.mode_timeout_s)
        rm_knobs.opp_volume_timeout_s = _Env.get_int(
            "RM_OPP_VOLUME_TIMEOUT_S", rm_knobs.opp_volume_timeout_s
        )
        rm_knobs.net_floor_bps = _Env.get_float("RM_NET_FLOOR_BPS", rm_knobs.net_floor_bps)
        rm_knobs.opp_age_fallback_s = _Env.get_float("RM_OPP_AGE_FALLBACK_S", rm_knobs.opp_age_fallback_s)
        rm_knobs.opp_vol_slip_age_s_max = _Env.get_float(
            "RM_OPP_VOL_SLIP_AGE_S_MAX", rm_knobs.opp_vol_slip_age_s_max
        )
        rm_knobs.opp_vol_p95_bps_max = _Env.get_float(
            "RM_OPP_VOL_P95_BPS_MAX", rm_knobs.opp_vol_p95_bps_max
        )
        rm_knobs.opp_shadow_p50_bps_max = _Env.get_float(
            "RM_OPP_SHADOW_P50_BPS_MAX", rm_knobs.opp_shadow_p50_bps_max
        )
        rm_knobs.opp_vol_exit_slip_age_s_max = _Env.get_float(
            "RM_OPP_VOL_EXIT_SLIP_AGE_S_MAX", rm_knobs.opp_vol_exit_slip_age_s_max
        )
        rm_knobs.oppvol_p95_bps_min = _Env.get_float(
            "RM_OPPVOL_P95_BPS_MIN", rm_knobs.oppvol_p95_bps_min
        )
        rm_knobs.pnl_guard_day_lvl1_pct = _Env.get_float(
            "RM_PNL_GUARD_DAY_LVL1_PCT", rm_knobs.pnl_guard_day_lvl1_pct
        )
        rm_knobs.pnl_guard_day_lvl2_pct = _Env.get_float(
            "RM_PNL_GUARD_DAY_LVL2_PCT", rm_knobs.pnl_guard_day_lvl2_pct
        )
        rm_knobs.pnl_cooldown_s = _Env.get_int("RM_PNL_COOLDOWN_S", rm_knobs.pnl_cooldown_s)
        rm_knobs.pnl_guard_recover_floor_pct = _Env.get_float(
            "RM_PNL_GUARD_RECOVER_FLOOR_PCT", rm_knobs.pnl_guard_recover_floor_pct
        )
        rm_knobs.normal_tt_min_bps_delta = _Env.get_float(
            "RM_NORMAL_TT_MIN_BPS_DELTA", rm_knobs.normal_tt_min_bps_delta
        )
        rm_knobs.normal_tm_min_bps_delta = _Env.get_float(
            "RM_NORMAL_TM_MIN_BPS_DELTA", rm_knobs.normal_tm_min_bps_delta
        )
        rm_knobs.normal_cap_factor = _Env.get_float("RM_NORMAL_CAP_FACTOR", rm_knobs.normal_cap_factor)
        rm_knobs.normal_ioc_only = _Env.get_bool("RM_NORMAL_IOC_ONLY", rm_knobs.normal_ioc_only)
        rm_knobs.constr_tt_min_bps_delta = float(cfg.RM_CONSTR_TT_MIN_BPS_DELTA)
        rm_knobs.constr_tm_min_bps_delta = float(cfg.RM_CONSTR_TM_MIN_BPS_DELTA)
        rm_knobs.constr_cap_factor = float(cfg.RM_CONSTR_CAP_FACTOR)
        rm_knobs.constr_mm_enable = _Env.get_bool("RM_CONSTR_MM_ENABLE", rm_knobs.constr_mm_enable)
        rm_knobs.constr_ioc_only = bool(cfg.RM_CONSTR_IOC_ONLY)
        rm_knobs.opp_volume_tt_min_bps_delta = _Env.get_float(
            "RM_OPP_VOLUME_TT_MIN_BPS_DELTA", rm_knobs.opp_volume_tt_min_bps_delta
        )
        rm_knobs.opp_volume_tm_min_bps_delta = _Env.get_float(
            "RM_OPP_VOLUME_TM_MIN_BPS_DELTA", rm_knobs.opp_volume_tm_min_bps_delta
        )
        rm_knobs.opp_volume_cap_factor = _Env.get_float(
            "RM_OPP_VOLUME_CAP_FACTOR", rm_knobs.opp_volume_cap_factor
        )
        rm_knobs.opp_volume_mm_enable = _Env.get_bool(
            "RM_OPP_VOLUME_MM_ENABLE", rm_knobs.opp_volume_mm_enable
        )
        rm_knobs.opp_vol_tt_min_bps_delta = _Env.get_float(
            "RM_OPP_VOL_TT_MIN_BPS_DELTA", rm_knobs.opp_vol_tt_min_bps_delta
        )
        rm_knobs.opp_vol_tm_min_bps_delta = _Env.get_float(
            "RM_OPP_VOL_TM_MIN_BPS_DELTA", rm_knobs.opp_vol_tm_min_bps_delta
        )
        rm_knobs.opp_vol_cap_factor = _Env.get_float(
            "RM_OPP_VOL_CAP_FACTOR", rm_knobs.opp_vol_cap_factor
        )
        rm_knobs.opp_vol_mm_enable = _Env.get_bool(
            "RM_OPP_VOL_MM_ENABLE", rm_knobs.opp_vol_mm_enable
        )
        rm_knobs.severe_tt_min_bps_delta = _Env.get_float(
            "RM_SEVERE_TT_MIN_BPS_DELTA", rm_knobs.severe_tt_min_bps_delta
        )
        rm_knobs.severe_tm_min_bps_delta = _Env.get_float(
            "RM_SEVERE_TM_MIN_BPS_DELTA", rm_knobs.severe_tm_min_bps_delta
        )
        rm_knobs.severe_cap_factor = _Env.get_float("RM_SEVERE_CAP_FACTOR", rm_knobs.severe_cap_factor)
        rm_knobs.severe_mm_enable = _Env.get_bool("RM_SEVERE_MM_ENABLE", rm_knobs.severe_mm_enable)
        rm_knobs.severe_ioc_only = _Env.get_bool("RM_SEVERE_IOC_ONLY", rm_knobs.severe_ioc_only)
        rm_knobs.rm_reco_miss_per_minute_constrained = int(cfg.RM_RECO_MISS_PER_MINUTE_CONSTRAINED)
        rm_knobs.rm_reco_miss_per_minute_severe = int(cfg.RM_RECO_MISS_PER_MINUTE_SEVERE)
        rm_knobs.rm_pws_heartbeat_gap_constrained_s = float(cfg.RM_PWS_HEARTBEAT_GAP_CONSTRAINED_S)
        rm_knobs.rm_pws_heartbeat_gap_severe_s = float(cfg.RM_PWS_HEARTBEAT_GAP_SEVERE_S)
        rm_knobs.rm_pws_event_lag_constrained_ms = float(cfg.RM_PWS_EVENT_LAG_CONSTRAINED_MS)
        rm_knobs.rm_pws_event_lag_severe_ms = float(cfg.RM_PWS_EVENT_LAG_SEVERE_MS)
        rm_knobs.rm_pws_ack_latency_constrained_ms = float(cfg.RM_PWS_ACK_LATENCY_CONSTRAINED_MS)
        rm_knobs.rm_pws_ack_latency_severe_ms = float(cfg.RM_PWS_ACK_LATENCY_SEVERE_MS)
        rm_knobs.rm_pws_fill_latency_constrained_ms = float(cfg.RM_PWS_FILL_LATENCY_CONSTRAINED_MS)
        rm_knobs.rm_pws_fill_latency_severe_ms = float(cfg.RM_PWS_FILL_LATENCY_SEVERE_MS)
        rm_knobs.rm_fee_token_min_pct = float(cfg.RM_FEE_TOKEN_MIN_PCT)
        rm_knobs.rm_fee_low_min_seconds = float(cfg.RM_FEE_LOW_MIN_SECONDS)
        rm_knobs.rm_invariant_strict = bool(cfg.RM_INVARIANT_STRICT)
        signal_policy = cfg.rm.signal_policy
        signal_policy.require_vol_signal_for_opp = _Env.get_bool(
            "RM_REQUIRE_VOL_SIGNAL_FOR_OPP", signal_policy.require_vol_signal_for_opp
        )
        signal_policy.require_shadow_for_opp = _Env.get_bool(
            "RM_REQUIRE_SHADOW_FOR_OPP", signal_policy.require_shadow_for_opp
        )
        signal_policy.require_rl_health_for_opp = _Env.get_bool(
            "RM_REQUIRE_RL_HEALTH_FOR_OPP", signal_policy.require_rl_health_for_opp
        )
        signal_policy.require_pws_health_for_opp = _Env.get_bool(
            "RM_REQUIRE_PWS_HEALTH_FOR_OPP", signal_policy.require_pws_health_for_opp
        )
        signal_policy.require_book_age_for_opp = _Env.get_bool(
            "RM_REQUIRE_BOOK_AGE_FOR_OPP", signal_policy.require_book_age_for_opp
        )
        cfg.rm.fallback_on_tick_exception = str(
            _Env.get("RM_FALLBACK_ON_TICK_EXCEPTION", cfg.rm.fallback_on_tick_exception)
        ).upper()
        pacer_targets_raw = _Env.get_dict("ENGINE_PACER_TARGETS", cfg.engine.pacer_targets)
        pacer_targets_canon: Dict[str, Any] = {}
        for region, target in (pacer_targets_raw or {}).items():
            region_norm = str(region).upper().replace("_", "-")
            if region_norm in {"EU", "US", "JP", "EU-CB"}:
                pacer_targets_canon[region_norm] = target
            else:
                logging.getLogger(__name__).warning(
                    "ENGINE_PACER_TARGETS region %s ignored (unknown tag)", region
                )
        cfg.engine.pacer_targets = pacer_targets_canon

        # --- SPLIT breach knobs (CORRECT: écrire dans rm_knobs, pas dans cfg.rm) ---
        rm_knobs = cfg.rm.switch_knobs

        rm_knobs.split_breach_thr_base_ms = _Env.get_float(
            "RM_SPLIT_BREACH_THR_BASE_MS", rm_knobs.split_breach_thr_base_ms
        )
        rm_knobs.split_breach_thr_skew_ms = _Env.get_float(
            "RM_SPLIT_BREACH_THR_SKEW_MS", rm_knobs.split_breach_thr_skew_ms
        )
        rm_knobs.split_breach_thr_stale_ms = _Env.get_float(
            "RM_SPLIT_BREACH_THR_STALE_MS", rm_knobs.split_breach_thr_stale_ms
        )
        rm_knobs.split_breach_min_s = _Env.get_float(
            "RM_SPLIT_BREACH_MIN_S", rm_knobs.split_breach_min_s
        )
        rm_knobs.split_fallback_cooldown_s = _Env.get_float(
            "RM_SPLIT_FALLBACK_COOLDOWN_S", rm_knobs.split_fallback_cooldown_s
        )
        rm_knobs.split_restore_stable_s = _Env.get_float(
            "RM_SPLIT_RESTORE_STABLE_S", rm_knobs.split_restore_stable_s
        )
        rm_knobs.split_penalty_bps_max = _Env.get_float(
            "RM_SPLIT_PENALTY_BPS_MAX", rm_knobs.split_penalty_bps_max
        )

        # Profils de sécurité pour les flows (optionnel, sert de carte de priorisation)
        cfg.flow_safety.profiles = _Env.get_dict(
            "FLOW_SAFETY_PROFILES",
            cfg.flow_safety.profiles,
        )


        # --- Discovery : configuration centrale de l'univers -----------------

        # Facteur EUR vs USD et plancher absolu pour la quote EUR
        # (évitons un EUR ridiculement bas même si base_min est petit)
        cfg.discovery.eur_quote_volume_factor = float(
            _Env.get(
                "DISCOVERY_EUR_QUOTE_VOLUME_FACTOR",
                getattr(cfg.discovery, "eur_quote_volume_factor", 0.30),
            )
        )
        cfg.discovery.min_quote_volume_floor = float(
            _Env.get(
                "DISCOVERY_MIN_QUOTE_VOLUME_FLOOR",
                getattr(cfg.discovery, "min_quote_volume_floor", 1.0),
            )
        )

        # Filtrage par quotes et exchanges actifs
        q_allowed = _Env.get_list("DISCOVERY_QUOTES_ALLOWED", cfg.discovery.quotes_allowed or [])
        if q_allowed:
            cfg.discovery.quotes_allowed = [q.upper() for q in q_allowed]

        _Env.get("DISCOVERY_ENABLED_EXCHANGES", None)  # trigger warning if set (deprecated/no-op)

        # Whitelists / blacklists paires
        cfg.discovery.whitelist = _Env.get_list("DISCOVERY_WHITELIST", cfg.discovery.whitelist)
        cfg.discovery.blacklist = _Env.get_list("DISCOVERY_BLACKLIST", cfg.discovery.blacklist)

        # --- Scanner cfg ------------------------------------------------------
        cfg.scanner.workers = _Env.get_int("SCANNER_WORKERS", cfg.scanner.workers)
        cfg.scanner.shed_load_threshold = _Env.get_float(
            "SCANNER_SHED_LOAD_THRESHOLD", cfg.scanner.shed_load_threshold
        )
        cfg.scanner.shed_primary_factor = _Env.get_float(
            "SCANNER_SHED_PRIMARY_FACTOR", cfg.scanner.shed_primary_factor
        )
        cfg.scanner.shed_primary_min = _Env.get_float(
            "SCANNER_SHED_PRIMARY_MIN", cfg.scanner.shed_primary_min
        )
        cfg.scanner.shed_audition_factor = _Env.get_float(
            "SCANNER_SHED_AUDITION_FACTOR", cfg.scanner.shed_audition_factor
        )
        cfg.scanner.shed_cooldown_s = _Env.get_float(
            "SCANNER_SHED_COOLDOWN_S", cfg.scanner.shed_cooldown_s
        )
        cfg.scanner.scanner_mode = (_Env.get("SCANNER_MODE", cfg.scanner.scanner_mode) or cfg.scanner.scanner_mode)
        cfg.scanner.enable_mm_hints = _Env.get_bool("SCANNER_ENABLE_MM_HINTS", cfg.scanner.enable_mm_hints)
        cfg.scanner.binance_depth_level = _Env.get_int(
            "SCANNER_BINANCE_DEPTH_LEVEL", cfg.scanner.binance_depth_level
        )
        cfg.scanner.mm_rotation_enabled = _Env.get_bool(
            "SCANNER_MM_ROTATION_ENABLED", cfg.scanner.mm_rotation_enabled
        )
        cfg.scanner.mm_seed_pairs = tuple(
            _Env.get_list("SCANNER_MM_SEED_PAIRS", cfg.scanner.mm_seed_pairs, sep=",")
        )
        cfg.scanner.mm_depth_min_quote = _Env.get_float(
            "SCANNER_MM_DEPTH_MIN_QUOTE", cfg.scanner.mm_depth_min_quote
        )
        cfg.scanner.mm_qpos_max_ahead_quote = _Env.get_float(
            "SCANNER_MM_QPOS_MAX_AHEAD_QUOTE", cfg.scanner.mm_qpos_max_ahead_quote
        )
        cfg.scanner.mm_min_net_bps = _Env.get_float("SCANNER_MM_MIN_NET_BPS", cfg.scanner.mm_min_net_bps)
        cfg.scanner.mm_hedge_cost_bps = _Env.get_float("SCANNER_MM_HEDGE_COST_BPS", cfg.scanner.mm_hedge_cost_bps)
        cfg.scanner.mm_vol_bps_max = _Env.get_float("SCANNER_MM_VOL_BPS_MAX", cfg.scanner.mm_vol_bps_max)

        # P0: fenêtres & budgets de scan
        cfg.scanner.dedup_window_s = _Env.get_float("SCANNER_DEDUP_WINDOW_S", cfg.scanner.dedup_window_s)
        cfg.scanner.max_pairs_per_tick = _Env.get_int(
            "SCANNER_MAX_PAIRS_PER_TICK", cfg.scanner.max_pairs_per_tick
        )
        cfg.scanner.scan_interval = _Env.get_float("SCANNER_SCAN_INTERVAL_S", cfg.scanner.scan_interval)
        cfg.scanner.backpressure_log_every = _Env.get_int(
            "SCANNER_BACKPRESSURE_LOG_EVERY", cfg.scanner.backpressure_log_every
        )
        cfg.scanner.max_opportunities = _Env.get_int(
            "SCANNER_MAX_OPPORTUNITIES", cfg.scanner.max_opportunities
        )
        cfg.scanner.max_time_skew_s = _Env.get_float(
            "SCANNER_MAX_TIME_SKEW_S", cfg.scanner.max_time_skew_s
        )
        cfg.scanner.rm_ack_timeout_s = _Env.get_float(
            "SCANNER_RM_ACK_TIMEOUT_S", cfg.scanner.rm_ack_timeout_s
        )

        # P0: fréquences d'évaluation par tier
        cfg.scanner.scanner_eval_hz_primary = _Env.get_float(
            "SCANNER_EVAL_HZ_PRIMARY", cfg.scanner.scanner_eval_hz_primary
        )
        cfg.scanner.scanner_eval_hz_core = _Env.get_float(
            "SCANNER_EVAL_HZ_CORE", cfg.scanner.scanner_eval_hz_core
        )
        cfg.scanner.scanner_eval_hz_audition = _Env.get_float(
            "SCANNER_EVAL_HZ_AUDITION", cfg.scanner.scanner_eval_hz_audition
        )
        cfg.scanner.scanner_eval_hz_sandbox = _Env.get_float(
            "SCANNER_EVAL_HZ_SANDBOX", cfg.scanner.scanner_eval_hz_sandbox
        )
        cfg.scanner.allow_loss_bps_rebal = _Env.get_float(
            "SCANNER_ALLOW_LOSS_BPS_REBAL", cfg.scanner.allow_loss_bps_rebal
        )

        # Support legacy SCANNER_* keys pour du tuning runtime,
        # mais en propageant vers ScannerCfg pour garder une source unique.

        # --- Scanner / agrégateurs P0 (Ticket 6.A) ----------------------------
        raw_scanner_hz = _Env.get("SCANNER_HZ", None)
        cfg.SCANNER_HZ = _Env.get_dict("SCANNER_HZ", getattr(cfg, "SCANNER_HZ", {}))
        cfg.SCANNER_DEQUE_MAX = _Env.get_dict("SCANNER_DEQUE_MAX", getattr(cfg, "SCANNER_DEQUE_MAX", {}))
        cfg.SCANNER_DEDUP_COOLDOWN_S = _Env.get_float(
            "SCANNER_DEDUP_COOLDOWN_S",
            getattr(cfg, "SCANNER_DEDUP_COOLDOWN_S", getattr(cfg.scanner, "dedup_cooldown_s", 0.16)),
        )
        tier_env_keys = (
            "SCANNER_EVAL_HZ_PRIMARY",
            "SCANNER_EVAL_HZ_CORE",
            "SCANNER_EVAL_HZ_AUDITION",
            "SCANNER_EVAL_HZ_SANDBOX",
            "SCANNER_GLOBAL_EVAL_HZ",
        )
        tier_env_present = any(_Env.get(k, None) is not None for k in tier_env_keys)
        if raw_scanner_hz is not None and tier_env_present:
            msg = (
                "Conflicting env keys for scanner.eval_hz: "
                "SCANNER_HZ + SCANNER_EVAL_HZ_* (prefer SCANNER_HZ)"
            )
            if mode_upper == "PROD":
                raise ConfigError("CONFIG_SCHEMA_INVALID", "scanner.eval_hz")
            logging.getLogger(__name__).warning(msg)

        sc = cfg.scanner

        # Exposer le cooldown de dedup aussi dans ScannerCfg
        try:
            sc.dedup_cooldown_s = float(cfg.SCANNER_DEDUP_COOLDOWN_S)
        except Exception:
            pass

        if raw_scanner_hz is None:
            cfg.SCANNER_GLOBAL_EVAL_HZ = _Env.get_float(
                "SCANNER_GLOBAL_EVAL_HZ",
                getattr(cfg, "SCANNER_GLOBAL_EVAL_HZ", getattr(cfg.scanner, "scanner_global_eval_hz", 200.0)),
            )
            try:
                v = float(cfg.SCANNER_GLOBAL_EVAL_HZ)
            except Exception:
                v = float(getattr(sc, "scanner_global_eval_hz", 200.0))
            
            # Clamp et Warning pour HZ absurde (Macro 7-Diagnostic)
            # Un HZ > 100k est souvent une erreur de config ou va saturer un seul coeur CPU
            if v > 100000:
                logging.getLogger(__name__).warning(
                    "[Config] SCANNER_GLOBAL_EVAL_HZ=%.0f est excessivement élevé. Clamp à 100000.", v
                )
                v = 100000.0

            sc.scanner_global_eval_hz = v
            cfg.scanner_global_eval_hz = v
            cfg.SCANNER_HZ = {
                "CORE": sc.scanner_eval_hz_core,
                "PRIMARY": sc.scanner_eval_hz_primary,
                "AUDITION": sc.scanner_eval_hz_audition,
                "SANDBOX": sc.scanner_eval_hz_sandbox,
            }
        else:
            hz = cfg.SCANNER_HZ or {}
            if isinstance(hz, dict):
                v = hz.get("CORE") or hz.get("core")
                if v is not None:
                    try:
                        sc.scanner_eval_hz_core = float(v)
                    except Exception:
                        pass

                v = hz.get("PRIMARY") or hz.get("primary")
                if v is not None:
                    try:
                        sc.scanner_eval_hz_primary = float(v)
                    except Exception:
                        pass

                v = hz.get("AUDITION") or hz.get("audition")
                if v is not None:
                    try:
                        sc.scanner_eval_hz_audition = float(v)
                    except Exception:
                        pass

                v = hz.get("SANDBOX") or hz.get("sandbox")
                if v is not None:
                    try:
                        sc.scanner_eval_hz_sandbox = float(v)
                    except Exception:
                        pass

        cfg.rm.enable_maker_maker = _Env.get_bool("ENABLE_MAKER_MAKER", cfg.rm.enable_maker_maker)
        cfg.rm.ff_fail_closed_caps = _Env.get_bool(
            "RM_FF_FAIL_CLOSED_CAPS",
            cfg.rm.ff_fail_closed_caps,
        )
        _ff_trading_state_unified_raw = _Env.get("FF_TRADING_STATE_UNIFIED", None)
        cfg.rm.ff_trading_state_unified = _Env.get_bool(
            "FF_TRADING_STATE_UNIFIED",
            cfg.rm.ff_trading_state_unified,
        )
        if _ff_trading_state_unified_raw is None:
            mode_upper = str(getattr(cfg.g, "mode", "") or "").upper()
            feature_switches = getattr(cfg.g, "feature_switches", {}) or {}
            if mode_upper == "PROD" and (
                    bool(feature_switches.get("private_ws"))
                    or bool(feature_switches.get("balance_fetcher"))
            ):
                cfg.rm.ff_trading_state_unified = True

        cfg.rm.ff_enforce_preemption = _Env.get_bool(
            "FF_ENFORCE_PREEMPTION",
            cfg.rm.ff_enforce_preemption,
        )
        cfg.rm.ff_hedge_fast_lane = _Env.get_bool(
            "FF_HEDGE_FAST_LANE",
            cfg.rm.ff_hedge_fast_lane,
        )
        cfg.rm.ff_tm_enabled = _Env.get_bool(
            "FF_TM_ENABLED",
            cfg.rm.ff_tm_enabled,
        )
        cfg.rm.ff_mm_enabled = _Env.get_bool(
            "FF_MM_ENABLED",
            cfg.rm.ff_mm_enabled,
        )
        cfg.rm.ff_mm_opportunistic_gating_enforced = _Env.get_bool(
            "FF_MM_OPPORTUNISTIC_GATING_ENFORCED",
            cfg.rm.ff_mm_opportunistic_gating_enforced,
        )
        cfg.rm.ff_reb_enabled = _Env.get_bool(
            "FF_REB_ENABLED",
            cfg.rm.ff_reb_enabled,
        )

        # Branches RM (incluant REB) — alignées sur le chantier M6-B
        cfg.rm.branch_priority = _Env.get_list(
            "RM_BRANCH_PRIORITY",
            cfg.rm.branch_priority or cfg.g.branch_priority,
        )
        cfg.rm.branch_budgets_quote = _Env.get_dict(
            "RM_BRANCH_BUDGETS_QUOTE",
            cfg.rm.branch_budgets_quote or cfg.g.branch_budgets_quote,
        )
        cfg.rm.tm_queuepos_max_ahead_usd = _Env.get_float(
            "TM_QUEUEPOS_MAX_AHEAD_USD",
            cfg.rm.tm_queuepos_max_ahead_usd,
        )
        cfg.rm.tm_queuepos_max_eta_ms = _Env.get_int(
            "TM_QUEUEPOS_MAX_ETA_MS",
            cfg.rm.tm_queuepos_max_eta_ms,
        )
        cfg.rm.default_notional = _Env.get_float("RM_DEFAULT_NOTIONAL", cfg.rm.default_notional)
        cfg.rm.max_fragments = _Env.get_int("RM_MAX_FRAGMENTS", cfg.rm.max_fragments)
        cfg.rm.mm_alias_name = _Env.get("RM_MM_ALIAS_NAME", cfg.rm.mm_alias_name)
        cfg.rm.mm_depth_min_usd = _Env.get_float("RM_MM_DEPTH_MIN_USD", cfg.rm.mm_depth_min_usd)
        cfg.rm.mm_qpos_max_ahead_usd = _Env.get_float("RM_MM_QPOS_MAX_AHEAD_USD", cfg.rm.mm_qpos_max_ahead_usd)
        cfg.rm.mm_min_p_both = _Env.get_float("RM_MM_MIN_P_BOTH", cfg.rm.mm_min_p_both)
        cfg.rm.mm_min_net_bps = _Env.get_float("RM_MM_MIN_NET_BPS", cfg.rm.mm_min_net_bps)
        cfg.rm.mm_hedge_cost_bps = _Env.get_float("RM_MM_HEDGE_COST_BPS", cfg.rm.mm_hedge_cost_bps)
        cfg.rm.mm_vol_bps_max = _Env.get_float("RM_MM_VOL_BPS_MAX", cfg.rm.mm_vol_bps_max)
        cfg.rm.collat_default_min_usd = _Env.get_float(
            "RM_COLLAT_DEFAULT_MIN_USD", cfg.rm.collat_default_min_usd
        )
        cfg.rm.collat_alias_overrides = _Env.get_dict(
            "RM_COLLAT_ALIAS_OVERRIDES", cfg.rm.collat_alias_overrides
        )
        cfg.rm.collat_ratio_warn = _Env.get_float(
            "RM_COLLAT_RATIO_WARN", cfg.rm.collat_ratio_warn
        )
        cfg.rm.collat_ratio_crit = _Env.get_float(
            "RM_COLLAT_RATIO_CRIT", cfg.rm.collat_ratio_crit
        )
        cfg.rm.collat_quotes = _Env.get_list(
            "RM_COLLAT_QUOTES", cfg.rm.collat_quotes
        )

        cfg.rm.collat_ratio_low = _Env.get_float(
            "RM_COLLAT_RATIO_LOW",
            cfg.rm.collat_ratio_low,
        )

        # Expo TT/TM globale (VaR-lite)
        cfg.rm.tttm_exposure_soft_usd = _Env.get_float(
            "RM_TTTM_EXPOSURE_SOFT_USD",
            cfg.rm.tttm_exposure_soft_usd,
        )
        cfg.rm.tttm_exposure_hard_usd = _Env.get_float(
            "RM_TTTM_EXPOSURE_HARD_USD",
            cfg.rm.tttm_exposure_hard_usd,
        )
        # Overrides par asset via JSON optionnel (ex: {"BTC":{"soft_usd":5000,"hard_usd":15000}})
        cfg.rm.tttm_exposure_by_asset = _Env.get_dict(
            "RM_TTTM_EXPOSURE_BY_ASSET",
            cfg.rm.tttm_exposure_by_asset,
        )

        # Stuck legs TT
        cfg.rm.tt_stuck_soft_usd = _Env.get_float(
            "RM_TT_STUCK_SOFT_USD",
            cfg.rm.tt_stuck_soft_usd,
        )
        cfg.rm.tt_stuck_hard_usd = _Env.get_float(
            "RM_TT_STUCK_HARD_USD",
            cfg.rm.tt_stuck_hard_usd,
        )
        cfg.rm.tt_stuck_max_age_s = _Env.get_float(
            "RM_TT_STUCK_MAX_AGE_S",
            cfg.rm.tt_stuck_max_age_s,
        )

        # Liste de quotes considérées comme collat, ex: "USDC,USDT,USD,EUR"
        collat_q_env = _Env.get("RM_COLLAT_QUOTES", None)
        if collat_q_env:
            try:
                cfg.rm.collat_quotes = [
                    s.strip().upper()
                    for s in str(collat_q_env).split(",")
                    if s.strip()
                ]
            except Exception:
                pass


        cfg.rm.global_kill_switch = _Env.get_bool("GLOBAL_KILL_SWITCH", cfg.rm.global_kill_switch)
        cfg.rm.daily_strategy_budget_quote = _Env.get_dict("DAILY_STRATEGY_BUDGET_QUOTE",
                                                           cfg.rm.daily_strategy_budget_quote)
        cfg.rm.daily_budget_reset_interval_s = _Env.get_float("DAILY_BUDGET_RESET_INTERVAL_S",
                                                              cfg.rm.daily_budget_reset_interval_s)
        cfg.rm.decision_log_path = _Env.get("RM_DECISION_LOG_PATH", cfg.rm.decision_log_path)
        cfg.rm.preempt_mm_for_tt_tm = _Env.get_bool("PREEMPT_MM_FOR_TT_TM", cfg.rm.preempt_mm_for_tt_tm)
        cfg.rm.per_strategy_notional_cap = _Env.get_dict("PER_STRATEGY_NOTIONAL_CAP", cfg.rm.per_strategy_notional_cap)
        # Ticket 6 — Cap notional global par combo (TT+TM+REB) par profil
        cfg.rm.combo_cap_usd_by_profile = _Env.get_dict(
            "RM_COMBO_CAP_USD_BY_PROFILE",
            cfg.rm.combo_cap_usd_by_profile,
        )
        cfg.rm.combo_ttl_degraded_factor = _Env.get_float(
            "RM_COMBO_TTL_DEGRADED_FACTOR",
            cfg.rm.combo_ttl_degraded_factor,
        )

        cfg.rm.rebal_allow_loss_bps = _Env.get_float("REBAL_ALLOW_LOSS_BPS", cfg.rm.rebal_allow_loss_bps)
        cfg.rm.rebal_volume_haircut = _Env.get_float("REBAL_VOLUME_HAIRCUT", cfg.rm.rebal_volume_haircut)
        cfg.rm.rebal_allow_loss_bps_by_profile = _Env.get_dict(
            "RM_REBAL_ALLOW_LOSS_BPS_BY_PROFILE",
            cfg.rm.rebal_allow_loss_bps_by_profile,
        )
        cfg.rm.rebal_volume_haircut_by_profile = _Env.get_dict(
            "RM_REBAL_VOLUME_HAIRCUT_BY_PROFILE",
            cfg.rm.rebal_volume_haircut_by_profile,
        )

        # Canonique TM — hedge ratios (RM + Engine)

        # NEUTRAL: TM_EXPOSURE_TTL_HEDGE_RATIO (alias TM_NEUTRAL_HEDGE_RATIO)
        neutral_hr_env = _Env.get_float("TM_EXPOSURE_TTL_HEDGE_RATIO", None)
        if neutral_hr_env is None:
            neutral_hr_env = _Env.get_float(
                "TM_NEUTRAL_HEDGE_RATIO",
                getattr(cfg.rm, "tm_exposure_ttl_hedge_ratio", cfg.engine.tm_exposure_ttl_hedge_ratio),
            )
        neutral_hr = float(neutral_hr_env)

        # Propagation NEUTRAL (TTL hedge ratio) vers RM + Engine
        cfg.engine.tm_exposure_ttl_hedge_ratio = neutral_hr
        cfg.rm.tm_exposure_ttl_hedge_ratio = neutral_hr

        # NON-NEUTRAL: TM_NN_HEDGE_RATIO (canonique)
        nn_hr = _Env.get_float(
            "TM_NN_HEDGE_RATIO",
            getattr(cfg.rm, "tm_nn_hedge_ratio", 0.65),
        )
        setattr(cfg.rm, "tm_nn_hedge_ratio", nn_hr)
        setattr(cfg.engine, "tm_nn_hedge_ratio", nn_hr)

        # Ticket 10 — caps inflight business par profil / branche
        cfg.rm.inflight_trading_by_profile = _Env.get_dict(
            "RM_INFLIGHT_TRADING_BY_PROFILE",
            cfg.rm.inflight_trading_by_profile,
        )
        cfg.rm.caps_trading_by_profile = _Env.get_dict(
            "RM_CAPS_TRADING_BY_PROFILE",
            cfg.rm.caps_trading_by_profile,
        )
        cfg.rm.inflight_rebal_by_profile = _Env.get_dict(
            "RM_INFLIGHT_REBAL_BY_PROFILE",
            cfg.rm.inflight_rebal_by_profile,
        )
        cfg.rm.capital_ladder_cfg = _Env.get_dict(
            "RM_CAPITAL_LADDER_CFG",
            cfg.rm.capital_ladder_cfg,
        )


        cfg.sim.max_fragments = _Env.get_int("SIM_MAX_FRAGMENTS", cfg.sim.max_fragments)
        cfg.sim.min_fragment_usdc = _Env.get_float("SIM_MIN_FRAGMENT_USDC", cfg.sim.min_fragment_usdc)
        cfg.sim.simulator_mode = str(_Env.get("SIMULATOR_MODE", cfg.sim.simulator_mode)).upper()
        cfg.sim.simulator_bypass_allowed_in_live = _Env.get_bool(
            "SIMULATOR_BYPASS_ALLOWED_IN_LIVE", cfg.sim.simulator_bypass_allowed_in_live
        )
        cfg.sim.sim_cache_ttl_ms = _Env.get_int("SIM_CACHE_TTL_MS", cfg.sim.sim_cache_ttl_ms)
        cfg.sim.sim_max_wait_ms_rm = _Env.get_int("SIM_MAX_WAIT_MS_RM", cfg.sim.sim_max_wait_ms_rm)
        cfg.sim.sim_prime_depth_levels = _Env.get_int("SIM_PRIME_DEPTH_LEVELS", cfg.sim.sim_prime_depth_levels)
        cfg.sim.sim_max_inflight_jobs = _Env.get_int("SIM_MAX_INFLIGHT_JOBS", cfg.sim.sim_max_inflight_jobs)
        cfg.sim.sim_shadow_max_inflight = _Env.get_int(
            "SIM_SHADOW_MAX_INFLIGHT", cfg.sim.sim_shadow_max_inflight
        )
        cfg.sim.sim_deterministic_trade_id = _Env.get_bool(
            "SIM_DETERMINISTIC_TRADE_ID", cfg.sim.sim_deterministic_trade_id
        )
        cfg.sim.sim_notional_buckets_base = _Env.get(
            "SIM_NOTIONAL_BUCKETS_BASE", cfg.sim.sim_notional_buckets_base
        )
        cfg.sim.sim_buckets_per_profile = _Env.get_dict(
            "SIM_BUCKETS_PER_PROFILE", cfg.sim.sim_buckets_per_profile
        )

        cfg.sim.sim_buckets_adapt_with_vol = _Env.get_bool(
            "SIM_BUCKETS_ADAPT_WITH_VOL", cfg.sim.sim_buckets_adapt_with_vol
        )
        cfg.sim.sim_vol_size_factor_floor = _Env.get_float(
            "SIM_VOL_SIZE_FACTOR_FLOOR", cfg.sim.sim_vol_size_factor_floor
        )
        cfg.sim.sim_vol_size_factor_ceil = _Env.get_float(
            "SIM_VOL_SIZE_FACTOR_CEIL", cfg.sim.sim_vol_size_factor_ceil
        )
        cfg.sim.sim_prime_multibucket_k = _Env.get_int(
            "SIM_PRIME_MULTIBUCKET_K", cfg.sim.sim_prime_multibucket_k
        )
        cfg.sim.sim_timeout_each_s = _Env.get_float("SIM_TIMEOUT_EACH_S", cfg.sim.sim_timeout_each_s)
        cfg.sim.sim_cb_coalescing_ms = _Env.get_float("SIM_CB_COALESCING_MS", cfg.sim.sim_cb_coalescing_ms)
        cfg.sim.sim_split_mode = _Env.get("SIM_SPLIT_MODE", cfg.sim.sim_split_mode)
        cfg.sim.sim_book_fingerprint_mode = str(
            _Env.get("SIM_BOOK_FINGERPRINT_MODE", cfg.sim.sim_book_fingerprint_mode)
        ).upper()
        cfg.sim.sim_book_fingerprint_levels = _Env.get_int(
            "SIM_BOOK_FINGERPRINT_LEVELS", cfg.sim.sim_book_fingerprint_levels
        )
        cfg.sim.sim_mm_hints_interval_ms = _Env.get_int(
            "SIM_MM_HINTS_INTERVAL_MS", cfg.sim.sim_mm_hints_interval_ms
        )
        cfg.sim.sim_mm_hints_levels = _Env.get_int(
            "SIM_MM_HINTS_LEVELS", cfg.sim.sim_mm_hints_levels
        )
        cfg.sim.sim_outputs_required_by_branch = _Env.get_dict(
            "SIM_OUTPUTS_REQUIRED_BY_BRANCH", cfg.sim.sim_outputs_required_by_branch
        )

        # Ticket 10 — capacités techniques Engine (workers / inflight CEX par profil)
        cfg.engine.workers_by_profile = _Env.get_dict(
            "ENGINE_WORKERS_BY_PROFILE",
            cfg.engine.workers_by_profile,
        )
        cfg.engine.inflight_max_by_exchange_by_profile = _Env.get_dict(
            "ENGINE_INFLIGHT_MAX_BY_EXCHANGE_BY_PROFILE",
            cfg.engine.inflight_max_by_exchange_by_profile,
        )
        cfg.engine.inflight_reserved_hedge_by_exchange_by_profile = _Env.get_dict(
            "ENGINE_INFLIGHT_RESERVED_HEDGE_BY_EXCHANGE_BY_PROFILE",
            cfg.engine.inflight_reserved_hedge_by_exchange_by_profile,
        )
        cfg.engine.inflight_reserved_cancel_by_exchange_by_profile = _Env.get_dict(
            "ENGINE_INFLIGHT_RESERVED_CANCEL_BY_EXCHANGE_BY_PROFILE",
            cfg.engine.inflight_reserved_cancel_by_exchange_by_profile,
        )

        cfg.engine.tt_max_skew_ms = _Env.get_int("ENGINE_TT_MAX_SKEW_MS", cfg.engine.tt_max_skew_ms)
        cfg.engine.tt_max_drift_bps = _Env.get_float("ENGINE_TT_MAX_DRIFT_BPS", cfg.engine.tt_max_drift_bps)
        cfg.engine.tt_policy_version = _Env.get("ENGINE_TT_POLICY_VERSION", cfg.engine.tt_policy_version)
        cfg.engine.tt_submit_mode = _Env.get("ENGINE_TT_SUBMIT_MODE", cfg.engine.tt_submit_mode)
        cfg.engine.tt_tif_policy = _Env.get("ENGINE_TT_TIF_POLICY", cfg.engine.tt_tif_policy)
        cfg.engine.tt_tif_last_slice = _Env.get("ENGINE_TT_TIF_LAST_SLICE", cfg.engine.tt_tif_last_slice)
        cfg.engine.tt_ack_p95_staggered_threshold_ms = _Env.get_float("ENGINE_TT_ACK_P95_STAGGERED_THRESHOLD_MS", cfg.engine.tt_ack_p95_staggered_threshold_ms)
        cfg.engine.tt_stop_edge_floor_bps = _Env.get_float("ENGINE_TT_STOP_EDGE_FLOOR_BPS", cfg.engine.tt_stop_edge_floor_bps)
        cfg.engine.tt_ack_slo_ms = _Env.get_float("ENGINE_TT_ACK_SLO_MS", cfg.engine.tt_ack_slo_ms)
        cfg.engine.tt_time_budget_ms = _Env.get_float("ENGINE_TT_TIME_BUDGET_MS", cfg.engine.tt_time_budget_ms)
        cfg.engine.tt_require_private_ws_healthy = _Env.get_bool("ENGINE_TT_REQUIRE_PRIVATE_WS_HEALTHY", cfg.engine.tt_require_private_ws_healthy)
        cfg.engine.tt_max_router_age_ms = _Env.get_float("ENGINE_TT_MAX_ROUTER_AGE_MS", cfg.engine.tt_max_router_age_ms)
        cfg.engine.tt_max_book_age_ms = _Env.get_float("ENGINE_TT_MAX_BOOK_AGE_MS", cfg.engine.tt_max_book_age_ms)
        cfg.engine.tt_private_ws_max_lag_ms = _Env.get_float("ENGINE_TT_PRIVATE_WS_MAX_LAG_MS", cfg.engine.tt_private_ws_max_lag_ms)
        cfg.engine.tt_group_concurrency_by_pacer = _Env.get_dict("ENGINE_TT_GROUP_CONCURRENCY_BY_PACER", cfg.engine.tt_group_concurrency_by_pacer)

        cfg.engine.order_timeout_s = _Env.get_int("ENGINE_ORDER_TIMEOUT_S", cfg.engine.order_timeout_s)
        cfg.engine.http_timeout_s = _Env.get_float("ENGINE_HTTP_TIMEOUT_S", cfg.engine.http_timeout_s)

        # Canonique TM — TTL d'exposition (ms)
        ttl_ms_global = _Env.get_int(
            "TM_EXPOSURE_TTL_MS",  # clé globale
            _Env.get_int("ENGINE_TM_EXPOSURE_TTL_MS", cfg.engine.tm_exposure_ttl_ms),  # alias Engine
        )
        cfg.engine.tm_exposure_ttl_ms = ttl_ms_global
        setattr(cfg.rm, "tm_exposure_ttl_ms", ttl_ms_global)

        cfg.engine.tm_policy_version = _Env.get("ENGINE_TM_POLICY_VERSION", cfg.engine.tm_policy_version)
        cfg.engine.tm_quote_lifetime_min_ms = _Env.get_float("ENGINE_TM_QUOTE_LIFETIME_MIN_MS", cfg.engine.tm_quote_lifetime_min_ms)
        cfg.engine.tm_ladder_levels = _Env.get_int("ENGINE_TM_LADDER_LEVELS", cfg.engine.tm_ladder_levels)
        cfg.engine.tm_ladder_step_ticks = _Env.get_float("ENGINE_TM_LADDER_STEP_TICKS", cfg.engine.tm_ladder_step_ticks)
        cfg.engine.tm_replace_drift_ticks = _Env.get_float("ENGINE_TM_REPLACE_DRIFT_TICKS", cfg.engine.tm_replace_drift_ticks)
        cfg.engine.tm_replace_max_age_ms = _Env.get_int("ENGINE_TM_REPLACE_MAX_AGE_MS", cfg.engine.tm_replace_max_age_ms)
        cfg.engine.tm_replace_budget = _Env.get_int("ENGINE_TM_REPLACE_BUDGET", cfg.engine.tm_replace_budget)
        cfg.engine.tm_cancel_budget = _Env.get_int("ENGINE_TM_CANCEL_BUDGET", cfg.engine.tm_cancel_budget)
        cfg.engine.tm_retry_budget = _Env.get_int("ENGINE_TM_RETRY_BUDGET", cfg.engine.tm_retry_budget)
        cfg.engine.tm_time_budget_ms = _Env.get_float("ENGINE_TM_TIME_BUDGET_MS", cfg.engine.tm_time_budget_ms)
        cfg.engine.tm_qpos_fail_mode = _Env.get("ENGINE_TM_QPOS_FAIL_MODE", cfg.engine.tm_qpos_fail_mode)
        cfg.engine.tm_qpos_guard_hysteresis_ms = _Env.get_int("ENGINE_TM_QPOS_GUARD_HYSTERESIS_MS", cfg.engine.tm_qpos_guard_hysteresis_ms)
        cfg.engine.tm_qpos_guard_release_ratio = _Env.get_float("ENGINE_TM_QPOS_GUARD_RELEASE_RATIO", cfg.engine.tm_qpos_guard_release_ratio)
        cfg.engine.tm_panic_trigger_ack_p95_ms = _Env.get_float("ENGINE_TM_PANIC_TRIGGER_ACK_P95_MS", cfg.engine.tm_panic_trigger_ack_p95_ms)
        cfg.engine.tm_panic_hedge_ratio = _Env.get_float("ENGINE_TM_PANIC_HEDGE_RATIO", cfg.engine.tm_panic_hedge_ratio)
        cfg.engine.tm_panic_hedge_ttl_ms = _Env.get_int("ENGINE_TM_PANIC_HEDGE_TTL_MS", cfg.engine.tm_panic_hedge_ttl_ms)
        cfg.engine.tm_stop_edge_floor_bps_delta = _Env.get_float("ENGINE_TM_STOP_EDGE_FLOOR_BPS_DELTA", cfg.engine.tm_stop_edge_floor_bps_delta)
        cfg.engine.tm_require_private_ws_healthy = _Env.get_bool("ENGINE_TM_REQUIRE_PRIVATE_WS_HEALTHY", cfg.engine.tm_require_private_ws_healthy)
        cfg.engine.tm_fallback_mode = _Env.get("ENGINE_TM_FALLBACK_MODE", cfg.engine.tm_fallback_mode)
        cfg.engine.tm_hedge_order_type = _Env.get("ENGINE_TM_HEDGE_ORDER_TYPE", cfg.engine.tm_hedge_order_type)
        cfg.engine.tm_revalidate_before_place = _Env.get_bool("ENGINE_TM_REVALIDATE_BEFORE_PLACE", cfg.engine.tm_revalidate_before_place)
        cfg.engine.tm_maker_order_type = _Env.get("ENGINE_TM_MAKER_ORDER_TYPE", cfg.engine.tm_maker_order_type)
        cfg.engine.tm_spiral_breaker_429 = _Env.get_bool("ENGINE_TM_SPIRAL_BREAKER_429", cfg.engine.tm_spiral_breaker_429)

        cfg.engine.tm_queuepos_max_eta_ms = _Env.get_int("ENGINE_TM_QPOS_MAX_ETA_MS", cfg.engine.tm_queuepos_max_eta_ms)

        cfg.engine.vol_soft_cap_bps = _Env.get_float("ENGINE_VOL_SOFT_CAP_BPS", cfg.engine.vol_soft_cap_bps)
        cfg.engine.vol_hard_cap_bps = _Env.get_float("ENGINE_VOL_HARD_CAP_BPS", cfg.engine.vol_hard_cap_bps)
        cfg.engine.freeze_tm_on_vol = _Env.get_bool("ENGINE_FREEZE_TM_ON_VOL", cfg.engine.freeze_tm_on_vol)
        cfg.engine.idempotency_ttl_s = _Env.get_float(
            "ENGINE_IDEMPOTENCY_TTL_S",
            cfg.engine.idempotency_ttl_s,
        )
        cfg.engine.ff_enforce_client_oid_deterministic = _Env.get_bool(
            "ENGINE_ENFORCE_CLIENT_OID_DETERMINISTIC",
            cfg.engine.ff_enforce_client_oid_deterministic,
        )
        cfg.engine.ff_fail_closed_idempotence = _Env.get_bool(
            "ENGINE_FAIL_CLOSED_IDEMPOTENCE",
            cfg.engine.ff_fail_closed_idempotence,
        )
        cfg.engine.ff_hedge_fast_lane = _Env.get_bool(
            "ENGINE_FF_HEDGE_FAST_LANE",
            cfg.engine.ff_hedge_fast_lane,
        )
        cfg.engine.ff_enforce_preemption = _Env.get_bool(
            "ENGINE_FF_ENFORCE_PREEMPTION",
            cfg.engine.ff_enforce_preemption,
        )
        cfg.engine.ff_tm_enabled = _Env.get_bool(
            "ENGINE_FF_TM_ENABLED",
            cfg.engine.ff_tm_enabled,
        )
        cfg.engine.ff_mm_enabled = _Env.get_bool(
            "ENGINE_FF_MM_ENABLED",
            cfg.engine.ff_mm_enabled,
        )
        cfg.engine.ff_mm_opportunistic_gating_enforced = _Env.get_bool(
            "ENGINE_FF_MM_OPPORTUNISTIC_GATING_ENFORCED",
            cfg.engine.ff_mm_opportunistic_gating_enforced,
        )
        cfg.engine.ff_reb_enabled = _Env.get_bool(
            "ENGINE_FF_REB_ENABLED",
            cfg.engine.ff_reb_enabled,
        )
        cfg.engine.fail_closed_on_rm_override_exception = _Env.get_bool(
            "ENGINE_FAIL_CLOSED_ON_RM_OVERRIDE_EXCEPTION",
            cfg.engine.fail_closed_on_rm_override_exception,
        )
        pacer_knobs = cfg.engine.pacer_knobs
        pacer_knobs.warmup_secs = _Env.get_float("ENGINE_PACER_WARMUP_SECS", pacer_knobs.warmup_secs)
        pacer_knobs.escalate_bad_windows = _Env.get_int(
            "ENGINE_PACER_ESCALATE_BAD_WINDOWS", pacer_knobs.escalate_bad_windows
        )
        pacer_knobs.deescalate_good_windows = _Env.get_int(
            "ENGINE_PACER_DEESCALATE_GOOD_WINDOWS", pacer_knobs.deescalate_good_windows
        )
        pacer_knobs.hold_time_flags_secs = _Env.get_float(
            "ENGINE_PACER_HOLD_TIME_FLAGS_SECS", pacer_knobs.hold_time_flags_secs
        )
        pacer_knobs.good_score_threshold = _Env.get_float(
            "ENGINE_PACER_GOOD_SCORE_THRESHOLD", pacer_knobs.good_score_threshold
        )
        pacer_knobs.bad_score_threshold = _Env.get_float(
            "ENGINE_PACER_BAD_SCORE_THRESHOLD", pacer_knobs.bad_score_threshold
        )
        pacer_knobs.weight_ack = _Env.get_float("ENGINE_PACER_WEIGHT_ACK", pacer_knobs.weight_ack)
        pacer_knobs.weight_lag = _Env.get_float("ENGINE_PACER_WEIGHT_LAG", pacer_knobs.weight_lag)
        pacer_knobs.weight_err = _Env.get_float("ENGINE_PACER_WEIGHT_ERR", pacer_knobs.weight_err)
        pacer_knobs.weight_drain = _Env.get_float("ENGINE_PACER_WEIGHT_DRAIN", pacer_knobs.weight_drain)
        cfg.engine.pacer_min_ms = _Env.get_int("ENGINE_PACER_MIN_MS", cfg.engine.pacer_min_ms)
        cfg.engine.pacer_max_ms = _Env.get_int("ENGINE_PACER_MAX_MS", cfg.engine.pacer_max_ms)
        cfg.engine.pacer_init_ms = _Env.get_int("ENGINE_PACER_INIT_MS", cfg.engine.pacer_init_ms)
        cfg.engine.pacer_jitter_ms = _Env.get_int("ENGINE_PACER_JITTER_MS", cfg.engine.pacer_jitter_ms)
        targets_override = _Env.get_dict("ENGINE_PACER_DEFAULT_TARGETS", {})
        if targets_override:
            try:
                pacer_knobs.default_targets = targets_override
            except Exception:
                pass
        cfg.engine.depth_min_quote_tt = _Env.get_float("ENGINE_DEPTH_MIN_QUOTE_TT", cfg.engine.depth_min_quote_tt)
        cfg.engine.depth_min_quote_tm = _Env.get_float("ENGINE_DEPTH_MIN_QUOTE_TM", cfg.engine.depth_min_quote_tm)
        cfg.engine.depth_min_quote_mm = _Env.get_float("ENGINE_DEPTH_MIN_QUOTE_MM", cfg.engine.depth_min_quote_mm)
        cfg.engine.depth_levels_check = _Env.get_int("ENGINE_DEPTH_LEVELS_CHECK", cfg.engine.depth_levels_check)
        cfg.engine.price_band_bps_floor = _Env.get_float("ENGINE_PRICE_BAND_BPS_FLOOR", cfg.engine.price_band_bps_floor)
        cfg.engine.price_band_bps_cap = _Env.get_float("ENGINE_PRICE_BAND_BPS_CAP", cfg.engine.price_band_bps_cap)
        cfg.engine.vol_price_band_k = _Env.get_float("ENGINE_VOL_PRICE_BAND_K", cfg.engine.vol_price_band_k)
        cfg.engine.circuit_escalation_window_s = _Env.get_float("ENGINE_CIRCUIT_ESCALATION_WINDOW_S",
                                                                cfg.engine.circuit_escalation_window_s)
        cfg.engine.circuit_mute_escalation = _Env.get_float("ENGINE_CIRCUIT_MUTE_ESCALATION",
                                                            cfg.engine.circuit_mute_escalation)
        cfg.engine.circuit_mute_min_s = _Env.get_float("ENGINE_CIRCUIT_MUTE_MIN_S", cfg.engine.circuit_mute_min_s)
        cfg.engine.circuit_mute_max_s = _Env.get_float("ENGINE_CIRCUIT_MUTE_MAX_S", cfg.engine.circuit_mute_max_s)
        cfg.engine.circuit_mute_s_tm = _Env.get_float("ENGINE_CIRCUIT_MUTE_S_TM", cfg.engine.circuit_mute_s_tm)
        cfg.engine.circuit_mute_s_mm = _Env.get_float("ENGINE_CIRCUIT_MUTE_S_MM", cfg.engine.circuit_mute_s_mm)

        # TT Policy
        cfg.engine.tt_policy_version = _Env.get("ENGINE_TT_POLICY_VERSION", cfg.engine.tt_policy_version)
        cfg.engine.tt_submit_mode = _Env.get("ENGINE_TT_SUBMIT_MODE", cfg.engine.tt_submit_mode)
        cfg.engine.tt_tif_policy = _Env.get("ENGINE_TT_TIF_POLICY", cfg.engine.tt_tif_policy)
        cfg.engine.tt_stop_edge_floor_bps = _Env.get_float("ENGINE_TT_STOP_EDGE_FLOOR_BPS", cfg.engine.tt_stop_edge_floor_bps)
        cfg.engine.tt_ack_slo_ms = _Env.get_float("ENGINE_TT_ACK_SLO_MS", cfg.engine.tt_ack_slo_ms)
        cfg.engine.tt_time_budget_ms = _Env.get_float("ENGINE_TT_TIME_BUDGET_MS", cfg.engine.tt_time_budget_ms)
        cfg.engine.tt_require_private_ws_healthy = _Env.get_bool("ENGINE_TT_REQUIRE_PRIVATE_WS_HEALTHY", cfg.engine.tt_require_private_ws_healthy)
        cfg.engine.tt_max_router_age_ms = _Env.get_float("ENGINE_TT_MAX_ROUTER_AGE_MS", cfg.engine.tt_max_router_age_ms)
        cfg.engine.tt_max_book_age_ms = _Env.get_float("ENGINE_TT_MAX_BOOK_AGE_MS", cfg.engine.tt_max_book_age_ms)
        cfg.engine.tt_private_ws_max_lag_ms = _Env.get_float("ENGINE_TT_PRIVATE_WS_MAX_LAG_MS", cfg.engine.tt_private_ws_max_lag_ms)
        cfg.engine.tt_group_concurrency_by_pacer = _Env.get_dict("ENGINE_TT_GROUP_CONCURRENCY_BY_PACER", cfg.engine.tt_group_concurrency_by_pacer)

        # TM Policy
        cfg.engine.tm_policy_version = _Env.get("ENGINE_TM_POLICY_VERSION", cfg.engine.tm_policy_version)
        cfg.engine.tm_quote_lifetime_min_ms = _Env.get_float("ENGINE_TM_QUOTE_LIFETIME_MIN_MS", cfg.engine.tm_quote_lifetime_min_ms)
        cfg.engine.tm_ladder_levels = _Env.get_int("ENGINE_TM_LADDER_LEVELS", cfg.engine.tm_ladder_levels)
        cfg.engine.tm_ladder_step_ticks = _Env.get_float("ENGINE_TM_LADDER_STEP_TICKS", cfg.engine.tm_ladder_step_ticks)
        cfg.engine.tm_replace_drift_ticks = _Env.get_float("ENGINE_TM_REPLACE_DRIFT_TICKS", cfg.engine.tm_replace_drift_ticks)
        cfg.engine.tm_replace_max_age_ms = _Env.get_int("ENGINE_TM_REPLACE_MAX_AGE_MS", cfg.engine.tm_replace_max_age_ms)
        cfg.engine.tm_time_budget_ms = _Env.get_float("ENGINE_TM_TIME_BUDGET_MS", cfg.engine.tm_time_budget_ms)
        cfg.engine.tm_qpos_fail_mode = _Env.get("ENGINE_TM_QPOS_FAIL_MODE", cfg.engine.tm_qpos_fail_mode)
        cfg.engine.tm_qpos_guard_hysteresis_ms = _Env.get_int("ENGINE_TM_QPOS_GUARD_HYSTERESIS_MS", cfg.engine.tm_qpos_guard_hysteresis_ms)
        cfg.engine.tm_qpos_guard_release_ratio = _Env.get_float("ENGINE_TM_QPOS_GUARD_RELEASE_RATIO", cfg.engine.tm_qpos_guard_release_ratio)
        cfg.engine.tm_panic_trigger_ack_p95_ms = _Env.get_float("ENGINE_TM_PANIC_TRIGGER_ACK_P95_MS", cfg.engine.tm_panic_trigger_ack_p95_ms)
        cfg.engine.tm_panic_hedge_ratio = _Env.get_float("ENGINE_TM_PANIC_HEDGE_RATIO", cfg.engine.tm_panic_hedge_ratio)
        cfg.engine.tm_panic_hedge_ttl_ms = _Env.get_int("ENGINE_TM_PANIC_HEDGE_TTL_MS", cfg.engine.tm_panic_hedge_ttl_ms)
        cfg.engine.tm_stop_edge_floor_bps_delta = _Env.get_float("ENGINE_TM_STOP_EDGE_FLOOR_BPS_DELTA", cfg.engine.tm_stop_edge_floor_bps_delta)
        cfg.engine.tm_require_private_ws_healthy = _Env.get_bool("ENGINE_TM_REQUIRE_PRIVATE_WS_HEALTHY", cfg.engine.tm_require_private_ws_healthy)
        cfg.engine.tm_fallback_mode = _Env.get("ENGINE_TM_FALLBACK_MODE", cfg.engine.tm_fallback_mode)
        cfg.engine.tm_hedge_order_type = _Env.get("ENGINE_TM_HEDGE_ORDER_TYPE", cfg.engine.tm_hedge_order_type)
        cfg.engine.tm_revalidate_before_place = _Env.get_bool("ENGINE_TM_REVALIDATE_BEFORE_PLACE", cfg.engine.tm_revalidate_before_place)
        cfg.engine.tm_revalidate_every_s = _Env.get_float("ENGINE_TM_REVALIDATE_EVERY_S", cfg.engine.tm_revalidate_every_s)
        cfg.engine.tm_maker_order_type = _Env.get("ENGINE_TM_MAKER_ORDER_TYPE", cfg.engine.tm_maker_order_type)

        # MM Policy
        cfg.engine.mm_policy_version = _Env.get("ENGINE_MM_POLICY_VERSION", cfg.engine.mm_policy_version)
        cfg.engine.mm_require_private_ws_healthy = _Env.get_bool("ENGINE_MM_REQUIRE_PRIVATE_WS_HEALTHY", cfg.engine.mm_require_private_ws_healthy)
        cfg.engine.mm_pad_ticks_base = _Env.get_float("ENGINE_MM_PAD_TICKS_BASE", cfg.engine.mm_pad_ticks_base)
        cfg.engine.mm_size_factor_base = _Env.get_float("ENGINE_MM_SIZE_FACTOR_BASE", cfg.engine.mm_size_factor_base)
        cfg.engine.mm_requote_min_ticks = _Env.get_float("ENGINE_MM_REQUOTE_MIN_TICKS", cfg.engine.mm_requote_min_ticks)
        cfg.engine.mm_min_net_bps = _Env.get_float("ENGINE_MM_MIN_NET_BPS", cfg.engine.mm_min_net_bps)
        cfg.engine.mm_qpos_max_ahead_usd = _Env.get_float("ENGINE_MM_QPOS_MAX_AHEAD_USD", cfg.engine.mm_qpos_max_ahead_usd)
        cfg.engine.mm_ttl_ms = _Env.get_int("ENGINE_MM_TTL_MS", cfg.engine.mm_ttl_ms)
        cfg.engine.mm_hysteresis_ms = _Env.get_int("ENGINE_MM_HYSTERESIS_MS", cfg.engine.mm_hysteresis_ms)
        cfg.engine.mm_allow_auto_hedge = _Env.get_bool("ENGINE_MM_ALLOW_AUTO_HEDGE", cfg.engine.mm_allow_auto_hedge)
        cfg.engine.mm_hedge_schedule = _Env.get_list_float("ENGINE_MM_HEDGE_SCHEDULE", cfg.engine.mm_hedge_schedule)
        cfg.engine.mm_use_progressive_hedge = _Env.get_bool("ENGINE_MM_USE_PROGRESSIVE_HEDGE", cfg.engine.mm_use_progressive_hedge)
        cfg.engine.mm_hedge_final_ratio = _Env.get_float("ENGINE_MM_HEDGE_FINAL_RATIO", cfg.engine.mm_hedge_final_ratio)
        cfg.engine.mm_dual_engine_enabled = _Env.get_bool("ENGINE_MM_DUAL_ENGINE_ENABLED", cfg.engine.mm_dual_engine_enabled)
        cfg.engine.mm_single_inventory_enabled = _Env.get_bool("ENGINE_MM_SINGLE_INVENTORY_ENABLED", cfg.engine.mm_single_inventory_enabled)
        cfg.engine.mm_scale_on_tt_tm = _Env.get_float("ENGINE_MM_SCALE_ON_TT_TM", cfg.engine.mm_scale_on_tt_tm)
        cfg.engine.mm_pad_boost_on_tt_tm = _Env.get_int("ENGINE_MM_PAD_BOOST_ON_TT_TM", cfg.engine.mm_pad_boost_on_tt_tm)
        cfg.engine.mm_place_rate_limit_per_pair = _Env.get_int("ENGINE_MM_PLACE_RATE_LIMIT_PER_PAIR", cfg.engine.mm_place_rate_limit_per_pair)
        cfg.engine.mm_cancel_rate_limit_per_pair = _Env.get_int("ENGINE_MM_CANCEL_RATE_LIMIT_PER_PAIR", cfg.engine.mm_cancel_rate_limit_per_pair)
        cfg.engine.mm_cancel_budget_per_pair_min = _Env.get_int("ENGINE_MM_CANCEL_BUDGET_PER_PAIR_MIN", cfg.engine.mm_cancel_budget_per_pair_min)
        cfg.engine.mm_cancel_budget_exhausted_penalty_lifetime_mult = _Env.get_float("ENGINE_MM_CANCEL_BUDGET_EXHAUSTED_PENALTY_LIFETIME_MULT", cfg.engine.mm_cancel_budget_exhausted_penalty_lifetime_mult)
        cfg.engine.mm_cancel_budget_exhausted_penalty_pad_ticks = _Env.get_int("ENGINE_MM_CANCEL_BUDGET_EXHAUSTED_PENALTY_PAD_TICKS", cfg.engine.mm_cancel_budget_exhausted_penalty_pad_ticks)
        cfg.engine.mm_cross_ttl_ms = _Env.get_int("ENGINE_MM_CROSS_TTL_MS", cfg.engine.mm_cross_ttl_ms)
        cfg.engine.mm_cross_hedge_schedule = _Env.get_list_float("ENGINE_MM_CROSS_HEDGE_SCHEDULE", cfg.engine.mm_cross_hedge_schedule)
        cfg.engine.mm_cross_panic_after_ms = _Env.get_int("ENGINE_MM_CROSS_PANIC_AFTER_MS", cfg.engine.mm_cross_panic_after_ms)
        cfg.engine.mm_cross_defensive_pad_ticks = _Env.get_int("ENGINE_MM_CROSS_DEFENSIVE_PAD_TICKS", cfg.engine.mm_cross_defensive_pad_ticks)
        cfg.engine.mm_cross_defensive_lifetime_mult = _Env.get_float("ENGINE_MM_CROSS_DEFENSIVE_LIFETIME_MULT", cfg.engine.mm_cross_defensive_lifetime_mult)
        cfg.engine.mm_cross_429_threshold = _Env.get_float("ENGINE_MM_CROSS_429_THRESHOLD", cfg.engine.mm_cross_429_threshold)
        cfg.engine.mm_variants = _Env.get_dict("ENGINE_MM_VARIANTS", cfg.engine.mm_variants)
        cfg.engine.mm_min_quote_lifetime_ms = _Env.get_int("ENGINE_MM_MIN_QUOTE_LIFETIME_MS", cfg.engine.mm_min_quote_lifetime_ms)
        cfg.engine.mm_replace_cooldown_ms = _Env.get_int("ENGINE_MM_REPLACE_COOLDOWN_MS", cfg.engine.mm_replace_cooldown_ms)
        cfg.engine.mm_sticky_band_ticks = _Env.get_float("ENGINE_MM_STICKY_BAND_TICKS", cfg.engine.mm_sticky_band_ticks)
        cfg.engine.mm_toxicity_threshold = _Env.get_float("ENGINE_MM_TOXICITY_THRESHOLD", cfg.engine.mm_toxicity_threshold)
        cfg.engine.mm_jump_guard_threshold_bps = _Env.get_float("ENGINE_MM_JUMP_GUARD_THRESHOLD_BPS", cfg.engine.mm_jump_guard_threshold_bps)
        cfg.engine.mm_jump_guard_freeze_s = _Env.get_float("ENGINE_MM_JUMP_GUARD_FREEZE_S", cfg.engine.mm_jump_guard_freeze_s)
        cfg.engine.mm_jitter_pct = _Env.get_float("ENGINE_MM_JITTER_PCT", cfg.engine.mm_jitter_pct)
        cfg.engine.mm_edge_fees_bps = _Env.get_float("ENGINE_MM_EDGE_FEES_BPS", cfg.engine.mm_edge_fees_bps)
        cfg.engine.mm_edge_buffer_bps = _Env.get_float("ENGINE_MM_EDGE_BUFFER_BPS", cfg.engine.mm_edge_buffer_bps)
        cfg.engine.mm_hedge_aggressive_pad_mult = _Env.get_float("ENGINE_MM_HEDGE_AGGRESSIVE_PAD_MULT", cfg.engine.mm_hedge_aggressive_pad_mult)

        cfg.engine.workers_by_profile = _Env.get_dict(
            "ENGINE_WORKERS_BY_PROFILE",
            cfg.engine.workers_by_profile,
        )
        cfg.engine.inflight_max_by_exchange = _Env.get_dict(
            "ENGINE_INFLIGHT_MAX_BY_EXCHANGE",
            cfg.engine.inflight_max_by_exchange,
        )


        cfg.pws.PWS_POOL_SIZE_EU = _Env.get_int("PWS_POOL_SIZE_EU", cfg.pws.PWS_POOL_SIZE_EU)
        cfg.pws.PWS_POOL_SIZE_US = _Env.get_int("PWS_POOL_SIZE_US", cfg.pws.PWS_POOL_SIZE_US)
        cfg.pws.PWS_QUEUE_MAXLEN = _Env.get_int("PWS_QUEUE_MAXLEN", cfg.pws.PWS_QUEUE_MAXLEN)
        cfg.pws.PWS_QUEUE_SATURATION_RATIO = _Env.get_float(
            "PWS_QUEUE_SATURATION_RATIO", cfg.pws.PWS_QUEUE_SATURATION_RATIO
        )
        cfg.pws.ff_pws_no_drop_critical_enforced = _Env.get_bool(
            "PWS_NO_DROP_CRITICAL_ENFORCED",
            cfg.pws.ff_pws_no_drop_critical_enforced,
        )
        cfg.pws.ff_pws_strict_dedup_enforced = _Env.get_bool(
            "PWS_STRICT_DEDUP_ENFORCED", cfg.pws.ff_pws_strict_dedup_enforced
        )
        cfg.pws.ff_pws_disable_auto_wiring_prod = _Env.get_bool(
            "PWS_DISABLE_AUTO_WIRING_PROD",
            cfg.pws.ff_pws_disable_auto_wiring_prod,
        )
        cfg.pws.pws_drop_policy = _Env.get("PWS_DROP_POLICY", cfg.pws.pws_drop_policy)
        cfg.pws.PWS_PING_INTERVAL_S = _Env.get_int("PWS_PING_INTERVAL_S", cfg.pws.PWS_PING_INTERVAL_S)
        cfg.pws.PWS_PONG_TIMEOUT_S = _Env.get_int("PWS_PONG_TIMEOUT_S", cfg.pws.PWS_PONG_TIMEOUT_S)
        cfg.pws.PWS_HEARTBEAT_MAX_GAP_S = _Env.get_int("PWS_HEARTBEAT_MAX_GAP_S", cfg.pws.PWS_HEARTBEAT_MAX_GAP_S)

        # NEW: Parité de connexion par exchange pour WS privés (PWS_BINANCE_CONNECT_TIMEOUT_S, etc.)
        for ex in ["BINANCE", "BYBIT", "COINBASE"]:
            v_conn = _Env.get_int(f"PWS_{ex}_CONNECT_TIMEOUT_S", None)
            if v_conn is not None: cfg.pws.connect_timeout_s_by_ex[ex] = v_conn
            
            v_read = _Env.get_int(f"PWS_{ex}_READ_TIMEOUT_S", None)
            if v_read is not None: cfg.pws.read_timeout_s_by_ex[ex] = v_read
            
            v_ping = _Env.get_int(f"PWS_{ex}_PING_INTERVAL_S", None)
            if v_ping is not None: cfg.pws.ping_interval_s_by_ex[ex] = v_ping
            
            v_pong = _Env.get_int(f"PWS_{ex}_PONG_TIMEOUT_S", None)
            if v_pong is not None: cfg.pws.pong_timeout_s_by_ex[ex] = v_pong
        cfg.pws.PWS_STABLE_RESET_S = _Env.get_int("PWS_STABLE_RESET_S", cfg.pws.PWS_STABLE_RESET_S)
        cfg.pws.PWS_JITTER_MS = _Env.get_int("PWS_JITTER_MS", cfg.pws.PWS_JITTER_MS)
        cfg.pws.PWS_BACKOFF_BASE_MS = _Env.get_int("PWS_BACKOFF_BASE_MS", cfg.pws.PWS_BACKOFF_BASE_MS)
        cfg.pws.PWS_BACKOFF_MAX_MS = _Env.get_int("PWS_BACKOFF_MAX_MS", cfg.pws.PWS_BACKOFF_MAX_MS)
        cfg.pws.cb_private_poll_interval_s = _Env.get_int(
            "PWS_CB_PRIVATE_POLL_INTERVAL_S", cfg.pws.cb_private_poll_interval_s
        )
        cfg.pws.bybit_ws_private_url = _Env.get(
            "PWS_BYBIT_WS_PRIVATE_URL", cfg.pws.bybit_ws_private_url
        )
        cfg.pws.PWS_PACER_EU = _Env.get("PWS_PACER_EU", cfg.pws.PWS_PACER_EU)
        cfg.pws.PWS_PACER_US = _Env.get("PWS_PACER_US", cfg.pws.PWS_PACER_US)
        cfg.pws.PWS_ALERT_PERIOD_S = _Env.get_int("PWS_ALERT_PERIOD_S", cfg.pws.PWS_ALERT_PERIOD_S)

        cfg.reconciler.RECO_ALERT_PERIOD_S = _Env.get_int("RECO_ALERT_PERIOD_S", cfg.reconciler.RECO_ALERT_PERIOD_S)

        cfg.rebal.rebal_quantum_min_quote = _Env.get_float("REBAL_QUANTUM_MIN_QUOTE", cfg.rebal.rebal_quantum_min_quote)
        cfg.rebal.rebal_max_ops_per_min = _Env.get_int("REBAL_MAX_OPS_PER_MIN", cfg.rebal.rebal_max_ops_per_min)
        cfg.rebal.rebal_ops_per_min_ratio = _Env.get_float(
            "REBAL_OPS_PER_MIN_RATIO", cfg.rebal.rebal_ops_per_min_ratio
        )
        cfg.rebal.rebal_priority = _Env.get_list("REBAL_PRIORITY", cfg.rebal.rebal_priority)
        cfg.rebal.rebal_hint_ttl_s = _Env.get_int("REBAL_HINT_TTL_S", cfg.rebal.rebal_hint_ttl_s)
        cfg.rebal.rebal_internal_transfer_threshold = _Env.get_float(
            "REBAL_INTERNAL_TRANSFER_THRESHOLD",
            cfg.rebal.rebal_internal_transfer_threshold,
        )
        cfg.rebal.rebal_slot_ttl_s = _Env.get_float(
            "REBAL_SLOT_TTL_S",
            cfg.rebal.rebal_slot_ttl_s,
        )
        cfg.rebal.rebal_snapshots_missing_error_s = _Env.get_float("REBAL_SNAPSHOTS_MISSING_ERROR_S",
                                                                   cfg.rebal.rebal_snapshots_missing_error_s)
        cfg.rebal.rebal_snapshots_error_cooldown_s = _Env.get_float("REBAL_SNAPSHOTS_ERROR_COOLDOWN_S",
                                                                    cfg.rebal.rebal_snapshots_error_cooldown_s)
        cfg.rebal.rebal_quantum_quote_map = _Env.get_dict("REBAL_QUANTUM_QUOTE_MAP", cfg.rebal.rebal_quantum_quote_map)
        cfg.rebal.rebal_min_cash_per_quote = _Env.get_dict("REBAL_MIN_CASH_PER_QUOTE", cfg.rebal.rebal_min_cash_per_quote)

        cfg.balances.refresh_interval_s = _Env.get_int("BF_REFRESH_INTERVAL_S", cfg.balances.refresh_interval_s)
        cfg.balances.ttl_cache_s = _Env.get_int("BF_TTL_CACHE_S", cfg.balances.ttl_cache_s)
        cfg.balances.binance_rest_base = _Env.get("BINANCE_REST_BASE", cfg.balances.binance_rest_base)
        cfg.balances.coinbase_api_base = _Env.get("COINBASE_API_BASE", cfg.balances.coinbase_api_base)
        cfg.balances.bybit_api_base = _Env.get("BYBIT_API_BASE", cfg.balances.bybit_api_base)
        cfg.balances.default_private_wallet = _Env.get("DEFAULT_PRIVATE_WALLET", cfg.balances.default_private_wallet)
        cfg.balances.wallet_missing_log_interval_s = _Env.get_float("WALLET_MISSING_LOG_INTERVAL_S",
                                                                    cfg.balances.wallet_missing_log_interval_s)
        cfg.balances.wallet_types = _Env.get_list("BF_WALLET_TYPES", cfg.balances.wallet_types)
        cfg.balances.BF_PACER_EU = _Env.get("BF_PACER_EU", cfg.balances.BF_PACER_EU)
        cfg.balances.BF_PACER_US = _Env.get("BF_PACER_US", cfg.balances.BF_PACER_US)
        cfg.balances.fee_token_low_watermarks = _Env.get_dict("FEE_TOKEN_LOW_WATERMARKS", cfg.balances.fee_token_low_watermarks)
        cfg.balances.BF_ALERT_PERIOD_S = _Env.get_int("BF_ALERT_PERIOD_S", cfg.balances.BF_ALERT_PERIOD_S)
        # WS/private-plane merge & reconcile knobs (pilotables via env)
        # (BalanceFetcher les consomme via cfg.balances.*)
        cfg.balances.WS_BAL_TTL_SECONDS = _Env.get_float(
            "WS_BAL_TTL_SECONDS", cfg.balances.WS_BAL_TTL_SECONDS
        )
        cfg.balances.ENABLE_WS_BALANCE_MERGE = _Env.get_bool(
            "ENABLE_WS_BALANCE_MERGE", cfg.balances.ENABLE_WS_BALANCE_MERGE
        )
        cfg.balances.ws_reco_miss_rate_thr_per_min = _Env.get_float(
            "WS_RECO_MISS_RATE_THR_PER_MIN", cfg.balances.ws_reco_miss_rate_thr_per_min
        )
        cfg.balances.ws_reco_resync_age_thr_s = _Env.get_float(
            "WS_RECO_RESYNC_AGE_THR_S", cfg.balances.ws_reco_resync_age_thr_s
        )
        cfg.balances.reco_miss_burst_threshold = _Env.get_int("RECO_MISS_BURST_THRESHOLD", cfg.balances.reco_miss_burst_threshold)
        cfg.balances.reco_miss_recent_threshold = _Env.get_int("RECO_MISS_RECENT_THRESHOLD", cfg.balances.reco_miss_recent_threshold)
        cfg.balances.reco_alias_resync_max_age_s = _Env.get_float("RECO_ALIAS_RESYNC_MAX_AGE_S", cfg.balances.reco_alias_resync_max_age_s)

        # Readiness policy (consommé dans balance_fetcher._check_readiness)
        cfg.balances.balances_ready_min_state = _Env.get(
            "BALANCES_READY_MIN_STATE", cfg.balances.balances_ready_min_state
        )

        cfg.vol.midprice_to_bps = _Env.get_float("VOL_MIDPRICE_TO_BPS", cfg.vol.midprice_to_bps)
        cfg.vol.to_bps_floor = _Env.get_float("VOL_TO_BPS_FLOOR", cfg.vol.to_bps_floor)
        cfg.vol.to_bps_cap = _Env.get_float("VOL_TO_BPS_CAP", cfg.vol.to_bps_cap)
        cfg.vol.ema_alpha = _Env.get_float("VOL_EMA_ALPHA", cfg.vol.ema_alpha)
        cfg.vol.soft_cap_bps = _Env.get_float("VOL_SOFT_CAP_BPS", cfg.vol.soft_cap_bps)
        cfg.vol.chaos_cap_bps = _Env.get_float("VOL_CHAOS_CAP_BPS", cfg.vol.chaos_cap_bps)
        cfg.vol.hysteresis = _Env.get_float("VOL_HYSTERESIS", cfg.vol.hysteresis)
        cfg.vol.vol_min_delta_bps = _Env.get_float("VOL_MIN_DELTA_BPS", cfg.vol.vol_min_delta_bps)
        cfg.vol.max_silence_s = _Env.get_float("VOL_MAX_SILENCE_S", cfg.vol.max_silence_s)
        cfg.vol.window_long_m = _Env.get_int("VOL_WINDOW_LONG_M", cfg.vol.window_long_m)
        cfg.vol.window_micro_m = _Env.get_int("VOL_WINDOW_MICRO_M", cfg.vol.window_micro_m)
        cfg.vol.winsor_pct = _Env.get_float("VOL_WINSOR_PCT", cfg.vol.winsor_pct)
        cfg.rm.mm_ttl_ms = _Env.get_int("MM_TTL_MS", cfg.rm.mm_ttl_ms)
        cfg.vol.vm_size_factor_map = _Env.get_dict("VM_SIZE_FACTOR_MAP", cfg.vol.vm_size_factor_map)
        cfg.vol.vm_min_bps_boost_map = _Env.get_dict("VM_MIN_BPS_BOOST_MAP", cfg.vol.vm_min_bps_boost_map)
        cfg.vol.tm_neutral_hedge_ratio_map = _Env.get_dict("TM_NEUTRAL_HEDGE_RATIO_MAP",
                                                           cfg.vol.tm_neutral_hedge_ratio_map)
        cfg.vol.vm_prudence_thresholds_bps = _Env.get_dict("VM_PRUDENCE_THRESHOLDS_BPS",
                                                           cfg.vol.vm_prudence_thresholds_bps)
        cfg.vol.vm_maker_pad_ticks_map = _Env.get_dict("VM_MAKER_PAD_TICKS_MAP", cfg.vol.vm_maker_pad_ticks_map)
        cfg.slip.heartbeat_s = _Env.get_int("SLIP_HEARTBEAT_S", cfg.slip.heartbeat_s)
        cfg.slip.use_vwap_depth = _Env.get_bool("SLIP_USE_VWAP_DEPTH", cfg.slip.use_vwap_depth)
        cfg.slip.max_bps_by_quote = {
            k: float(v) for k, v in _Env.get_dict("SLIP_MAX_BPS_BY_QUOTE", cfg.slip.max_bps_by_quote).items()
        }
        cfg.slip.fee_sync_max_concurrency = _Env.get_int("FEE_SYNC_MAX_CONCURRENCY", cfg.slip.fee_sync_max_concurrency)
        cfg.slip.fee_sync_backoff_initial_s = _Env.get_float("FEE_SYNC_BACKOFF_INITIAL_S",
                                                             cfg.slip.fee_sync_backoff_initial_s)
        cfg.slip.fee_sync_backoff_max_s = _Env.get_float("FEE_SYNC_BACKOFF_MAX_S", cfg.slip.fee_sync_backoff_max_s)
        cfg.slip.fee_sync_max_retries = _Env.get_int("FEE_SYNC_MAX_RETRIES", cfg.slip.fee_sync_max_retries)
        cfg.slip.fee_sync_jitter_s = _Env.get_float("FEE_SYNC_JITTER_S", cfg.slip.fee_sync_jitter_s)
        cfg.slip.fee_reality_check_threshold_bps = _Env.get_float("FEE_REALITY_CHECK_THRESHOLD_BPS",
                                                                  cfg.slip.fee_reality_check_threshold_bps)

        # --- Rate Limiter (optionnel) ---
        rl_caps_ex_by = _Env.get_dict("RL_HARD_CAPS_RPS_BY_EXCHANGE", cfg.rl.hard_caps_rps_by_exchange)
        rl_caps_ex_legacy = _Env.get_dict("RL_HARD_CAPS_RPS", rl_caps_ex_by)
        if os.getenv("RL_HARD_CAPS_RPS") and os.getenv(
                "RL_HARD_CAPS_RPS_BY_EXCHANGE") and rl_caps_ex_by != rl_caps_ex_legacy:
            logging.getLogger(__name__).warning(
                "env collision on RL_HARD_CAPS_RPS vs RL_HARD_CAPS_RPS_BY_EXCHANGE; using canonical BY_EXCHANGE"
            )
        cfg.rl.hard_caps_rps_by_exchange = rl_caps_ex_by if os.getenv(
            "RL_HARD_CAPS_RPS_BY_EXCHANGE") else rl_caps_ex_legacy

        cfg.rl.hard_caps_rps_by_exchange_kind = _Env.get_dict(
            "RL_HARD_CAPS_RPS_BY_EXCHANGE_KIND",
            cfg.rl.hard_caps_rps_by_exchange_kind,
        )
        cfg.rl.hard_caps_rps_by_kind = _Env.get_dict("RL_HARD_CAPS_RPS_BY_KIND", cfg.rl.hard_caps_rps_by_kind)
        rl_bursts_ex_by = _Env.get_dict("RL_BURSTS_BY_EXCHANGE", cfg.rl.bursts_by_exchange)
        rl_bursts_ex_legacy = _Env.get_dict("RL_BURSTS", rl_bursts_ex_by)
        if os.getenv("RL_BURSTS") and os.getenv("RL_BURSTS_BY_EXCHANGE") and rl_bursts_ex_by != rl_bursts_ex_legacy:
            logging.getLogger(__name__).warning(
                "env collision on RL_BURSTS vs RL_BURSTS_BY_EXCHANGE; using canonical BY_EXCHANGE"
            )
        cfg.rl.bursts_by_exchange = rl_bursts_ex_by if os.getenv("RL_BURSTS_BY_EXCHANGE") else rl_bursts_ex_legacy

        cfg.rl.bursts_by_exchange_kind = _Env.get_dict(
            "RL_BURSTS_BY_EXCHANGE_KIND",
            cfg.rl.bursts_by_exchange_kind,
        )
        cfg.rl.bursts_by_kind = _Env.get_dict("RL_BURSTS_BY_KIND", cfg.rl.bursts_by_kind)
        cfg.rl.priorities = _Env.get_list("RL_PRIORITIES", cfg.rl.priorities)
        cfg.rl.fair = _Env.get_bool("RL_FAIR", cfg.rl.fair)
        cfg.rl.name_prefix = _Env.get("RL_NAME_PREFIX", cfg.rl.name_prefix)
        cfg.rl.min_sleep_s = _Env.get_float("RL_MIN_SLEEP_S", cfg.rl.min_sleep_s)
        cfg.rl.max_sleep_s = _Env.get_float("RL_MAX_SLEEP_S", cfg.rl.max_sleep_s)
        cfg.rl.default_rate_per_s = _Env.get_float("RL_DEFAULT_RATE_PER_S", cfg.rl.default_rate_per_s)
        cfg.rl.default_burst = _Env.get_int("RL_DEFAULT_BURST", cfg.rl.default_burst)

        # 🔹 Nouveau : configuration SC (sub-account) en JSON
        cfg.rl.sc = _Env.get_dict("RL_SC", cfg.rl.sc)



        # --- RPC Gateway (optionnel) ---
        cfg.rpc.enabled = _Env.get_bool("RPC_ENABLED", cfg.rpc.enabled)
        cfg.rpc.host = _Env.get("RPC_HOST", cfg.rpc.host)
        cfg.rpc.port = _Env.get_int("RPC_PORT", cfg.rpc.port)
        cfg.rpc.region = _Env.get("RPC_REGION", cfg.rpc.region)
        cfg.rpc.timeout_s = _Env.get_float("RPC_TIMEOUT_S", cfg.rpc.timeout_s)
        cfg.rpc.max_retries = _Env.get_int("RPC_MAX_RETRIES", cfg.rpc.max_retries)
        cfg.rpc.timeout_ms = _Env.get_int("RPC_TIMEOUT_MS", cfg.rpc.timeout_ms)
        cfg.rpc.mtls_enabled = _Env.get_bool("RPC_MTLS_ENABLED", cfg.rpc.mtls_enabled)
        cfg.rpc.loopback_inproc = _Env.get_bool("RPC_LOOPBACK_INPROC", cfg.rpc.loopback_inproc)
        cfg.rpc.ready_strict = _Env.get_bool("RPC_READY_STRICT", cfg.rpc.ready_strict)
        cfg.rpc.ca_cert = _Env.get("RPC_CA_CERT", cfg.rpc.ca_cert)
        cfg.rpc.server_cert = _Env.get("RPC_SERVER_CERT", cfg.rpc.server_cert)
        cfg.rpc.server_key = _Env.get("RPC_SERVER_KEY", cfg.rpc.server_key)
        cfg.rpc.client_cert = _Env.get("RPC_CLIENT_CERT", cfg.rpc.client_cert)
        cfg.rpc.client_key = _Env.get("RPC_CLIENT_KEY", cfg.rpc.client_key)
        cfg.rpc.require_client_cert = _Env.get_bool("RPC_REQUIRE_CLIENT_CERT", cfg.rpc.require_client_cert)
        cfg.rpc.rpc_server_bind = _Env.get("RPC_SERVER_BIND", cfg.rpc.rpc_server_bind)
        cfg.rpc.rpc_client_base = _Env.get("RPC_CLIENT_BASE", cfg.rpc.rpc_client_base)
        cfg.rpc.rpc_timeout_s = _Env.get_int("RPC_LEGACY_TIMEOUT_S", cfg.rpc.rpc_timeout_s)
        cfg.rpc.rpc_retries = _Env.get_int("RPC_RETRIES", cfg.rpc.rpc_retries)
        cfg.rpc.rpc_enable_mtls = _Env.get_bool("RPC_ENABLE_MTLS", cfg.rpc.rpc_enable_mtls)
        cfg.rpc.rpc_cert_paths = _Env.get_dict("RPC_CERT_PATHS", cfg.rpc.rpc_cert_paths)
        cfg.rpc.rpc_max_payload_kb = _Env.get_int("RPC_MAX_PAYLOAD_KB", cfg.rpc.rpc_max_payload_kb)

        # --- Regions / Split routing ---
        g = getattr(cfg, "g", cfg)

        g.exchange_region_map = {
            str(k).upper(): str(v).upper() for k, v in
            (_Env.get_dict("EXCHANGE_REGION_MAP", getattr(g, "exchange_region_map", {})) or {}).items()
        }
        g.engine_pod_map = {
            str(k).upper(): str(v) for k, v in
            (_Env.get_dict("ENGINE_POD_MAP", getattr(g, "engine_pod_map", {})) or {}).items()
        }

        # --- Logger / LHM (optionnel) ---
        cfg.lhm.out_dir = _Env.get("LHM_OUT_DIR", cfg.lhm.out_dir)
        cfg.lhm.strict_obs = _Env.get_bool("STRICT_OBS", cfg.lhm.strict_obs)
        cfg.lhm.LHM_Q_STREAM_MAX = _Env.get_int("LHM_Q_STREAM_MAX", cfg.lhm.LHM_Q_STREAM_MAX)
        cfg.lhm.LHM_STREAM_BATCH = _Env.get_int("LHM_STREAM_BATCH", cfg.lhm.LHM_STREAM_BATCH)
        cfg.lhm.LHM_DROP_WHEN_FULL = _Env.get_bool("LHM_DROP_WHEN_FULL", cfg.lhm.LHM_DROP_WHEN_FULL)
        cfg.lhm.LHM_HIGH_WATERMARK_RATIO = _Env.get_float("LHM_HIGH_WATERMARK_RATIO", cfg.lhm.LHM_HIGH_WATERMARK_RATIO)
        cfg.lhm.LHM_MAX_QUEUE_PLATEAU_S = _Env.get_int("LHM_MAX_QUEUE_PLATEAU_S", cfg.lhm.LHM_MAX_QUEUE_PLATEAU_S)
        cfg.lhm.ff_fail_closed_logging = _Env.get_bool("FF_FAIL_CLOSED_LOGGING", cfg.lhm.ff_fail_closed_logging)
        cfg.lhm.ff_logging_critical_streams = _Env.get_list(
            "FF_LOGGING_CRITICAL_STREAMS",
            cfg.lhm.ff_logging_critical_streams,
        )
        cfg.lhm.ff_truth_model_enabled = _Env.get_bool(
            "FF_TRUTH_MODEL_ENABLED",
            cfg.lhm.ff_truth_model_enabled,
        )
        cfg.lhm.ff_truth_fail_closed = _Env.get_bool(
            "FF_TRUTH_FAIL_CLOSED",
            cfg.lhm.ff_truth_fail_closed,
        )
        cfg.lhm.LHM_MM_SAMPLING_QUOTES = _Env.get_float("LHM_MM_SAMPLING_QUOTES", cfg.lhm.LHM_MM_SAMPLING_QUOTES)
        cfg.lhm.LHM_MM_SAMPLING_CANCELS = _Env.get_float("LHM_MM_SAMPLING_CANCELS", cfg.lhm.LHM_MM_SAMPLING_CANCELS)
        cfg.lhm.LHM_DROP_LOG_SAMPLE_RATE = _Env.get_float(
            "LHM_DROP_LOG_SAMPLE_RATE", cfg.lhm.LHM_DROP_LOG_SAMPLE_RATE
        )
        cfg.lhm.LHM_JSONL_QUEUE_CAP = _Env.get_int("LHM_JSONL_QUEUE_CAP", cfg.lhm.LHM_JSONL_QUEUE_CAP)
        cfg.lhm.LHM_JSONL_MAX_BYTES = _Env.get_int("LHM_JSONL_MAX_BYTES", cfg.lhm.LHM_JSONL_MAX_BYTES)
        cfg.lhm.LHM_JSONL_MAX_AGE_S = _Env.get_int("LHM_JSONL_MAX_AGE_S", cfg.lhm.LHM_JSONL_MAX_AGE_S)
        cfg.lhm.LHM_DISK_WARN_PCT = _Env.get_float("LHM_DISK_WARN_PCT", cfg.lhm.LHM_DISK_WARN_PCT)
        cfg.lhm.LHM_DISK_CRIT_PCT = _Env.get_float("LHM_DISK_CRIT_PCT", cfg.lhm.LHM_DISK_CRIT_PCT)
        cfg.lhm.LHM_STORAGE_ALERT_COOLDOWN_S = _Env.get_float(
            "LHM_STORAGE_ALERT_COOLDOWN_S", cfg.lhm.LHM_STORAGE_ALERT_COOLDOWN_S
        )
        cfg.lhm.LHM_DB_NAME = _Env.get("LHM_DB_NAME", cfg.lhm.LHM_DB_NAME)
        cfg.lhm.LHM_DB_MAX_BYTES = _Env.get_int("LHM_DB_MAX_BYTES", cfg.lhm.LHM_DB_MAX_BYTES)
        cfg.lhm.LHM_DB_MAX_AGE_S = _Env.get_int("LHM_DB_MAX_AGE_S", cfg.lhm.LHM_DB_MAX_AGE_S)
        cfg.lhm.LHM_DB_LANE_ENABLED = _Env.get_bool("LHM_DB_LANE_ENABLED", cfg.lhm.LHM_DB_LANE_ENABLED)
        cfg.lhm.LHM_DB_LANE_Q_MAX = _Env.get_int("LHM_DB_LANE_Q_MAX", cfg.lhm.LHM_DB_LANE_Q_MAX)
        cfg.lhm.LHM_DB_LANE_BATCH_MAX = _Env.get_int("LHM_DB_LANE_BATCH_MAX", cfg.lhm.LHM_DB_LANE_BATCH_MAX)
        cfg.lhm.LHM_DB_LANE_DRAIN_MS = _Env.get_float("LHM_DB_LANE_DRAIN_MS", cfg.lhm.LHM_DB_LANE_DRAIN_MS)
        cfg.lhm.LHM_DB_LANE_DROP_WHEN_FULL = _Env.get_bool(
            "LHM_DB_LANE_DROP_WHEN_FULL", cfg.lhm.LHM_DB_LANE_DROP_WHEN_FULL
        )
        cfg.lhm.LHM_WD_INTERVAL_S = _Env.get_float("LHM_WD_INTERVAL_S", cfg.lhm.LHM_WD_INTERVAL_S)
        cfg.lhm.LHM_WD_PLATEAU_WINDOW = _Env.get_int("LHM_WD_PLATEAU_WINDOW", cfg.lhm.LHM_WD_PLATEAU_WINDOW)
        cfg.lhm.LHM_WD_QUEUE_MIN_SIZE = _Env.get_int("LHM_WD_QUEUE_MIN_SIZE", cfg.lhm.LHM_WD_QUEUE_MIN_SIZE)
        cfg.lhm.LHM_WD_STALL_THRESHOLD_S = _Env.get_float("LHM_WD_STALL_THRESHOLD_S", cfg.lhm.LHM_WD_STALL_THRESHOLD_S)
        cfg.lhm.LHM_TRADE_BATCH_SIZE = _Env.get_int("LHM_TRADE_BATCH_SIZE", cfg.lhm.LHM_TRADE_BATCH_SIZE)
        cfg.lhm.LHM_TRADE_FLUSH_INTERVAL_S = _Env.get_float("LHM_TRADE_FLUSH_INTERVAL_S",
                                                            cfg.lhm.LHM_TRADE_FLUSH_INTERVAL_S)
        cfg.lhm.LHM_SLO_WRITE_MS_P95_TARGET = _Env.get_float("LHM_SLO_WRITE_MS_P95_TARGET",
                                                             cfg.lhm.LHM_SLO_WRITE_MS_P95_TARGET)
        cfg.lhm.LHM_SLO_QUEUE_DEPTH_MAX_TARGET = _Env.get_float("LHM_SLO_QUEUE_DEPTH_MAX_TARGET",
                                                                cfg.lhm.LHM_SLO_QUEUE_DEPTH_MAX_TARGET)
        cfg.lhm.LHM_SLO_PIPELINE_LAG_MAX_SECONDS_TARGET = _Env.get_float(
            "LHM_SLO_LAG_SECONDS_MAX_TARGET", cfg.lhm.LHM_SLO_PIPELINE_LAG_MAX_SECONDS_TARGET)
        cfg.lhm.LHM_SLO_DROPPED_TRADES_BUDGET = _Env.get_float("LHM_SLO_DROPPED_TRADES_BUDGET",
                                                               cfg.lhm.LHM_SLO_DROPPED_TRADES_BUDGET)
        cfg.lhm.LHM_OPPORTUNITY_QUEUE_MAX = _Env.get_int(
            "LHM_OPPORTUNITY_QUEUE_MAX", cfg.lhm.LHM_OPPORTUNITY_QUEUE_MAX
        )
        cfg.lhm.LHM_OPPORTUNITY_DROP_WHEN_FULL = _Env.get_bool(
            "LHM_OPPORTUNITY_DROP_WHEN_FULL", cfg.lhm.LHM_OPPORTUNITY_DROP_WHEN_FULL
        )
        cfg.lhm.LHM_PNL_RECO_ENABLED = _Env.get_bool("LHM_PNL_RECO_ENABLED", cfg.lhm.LHM_PNL_RECO_ENABLED)
        cfg.lhm.LHM_PNL_RECO_TOL_ABS_QUOTE = _Env.get_float(
            "LHM_PNL_RECO_TOL_ABS_QUOTE", cfg.lhm.LHM_PNL_RECO_TOL_ABS_QUOTE
        )
        cfg.lhm.LHM_PNL_RECO_TOL_PCT = _Env.get_float(
            "LHM_PNL_RECO_TOL_PCT", cfg.lhm.LHM_PNL_RECO_TOL_PCT
        )
        cfg.lhm.LHM_PNL_RECO_MAX_LOOKBACK_DAYS = _Env.get_int(
            "LHM_PNL_RECO_MAX_LOOKBACK_DAYS", cfg.lhm.LHM_PNL_RECO_MAX_LOOKBACK_DAYS
        )
        cfg.lhm.LHM_PNL_RECO_DEFAULT_REGION = _Env.get(
            "LHM_PNL_RECO_DEFAULT_REGION", cfg.lhm.LHM_PNL_RECO_DEFAULT_REGION
        )
        cfg.lhm.OPS_RETENTION_DAYS = _Env.get_int("OPS_RETENTION_DAYS", cfg.lhm.OPS_RETENTION_DAYS)
        # PIVOT: chemins pilotés par cfg.lhm

        cfg.lhm.storage_path = _Env.get("LHM_STORAGE_PATH", cfg.lhm.storage_path)

        # --- Dashboard (optionnel) ---
        cfg.dashboard.OBS_BASE_URL    = _Env.get("OBS_BASE_URL",     cfg.dashboard.OBS_BASE_URL)
        cfg.dashboard.AUTOREFRESH_SEC = _Env.get_int("AUTOREFRESH_SEC", cfg.dashboard.AUTOREFRESH_SEC)
        cfg.dashboard.DEMO_MODE       = _Env.get_bool("DEMO_MODE",   cfg.dashboard.DEMO_MODE)

        # --- Tests (optionnel) ---
        cfg.tests.ENABLE_WS_LIVE          = _Env.get_bool("ENABLE_WS_LIVE",          cfg.tests.ENABLE_WS_LIVE)
        cfg.tests.WS_LIVE_WINDOW_S        = _Env.get_int("WS_LIVE_WINDOW_S",         cfg.tests.WS_LIVE_WINDOW_S)
        cfg.tests.WS_LIVE_MIN_MSGS_PER_S  = _Env.get_int("WS_LIVE_MIN_MSGS_PER_S",   cfg.tests.WS_LIVE_MIN_MSGS_PER_S)
        cfg.tests.WS_LIVE_CLOSE_TIMEOUT_S = _Env.get_int("WS_LIVE_CLOSE_TIMEOUT_S",  cfg.tests.WS_LIVE_CLOSE_TIMEOUT_S)

        # --- Overlays transverses ---
        cfg._apply_mode_overlay()           # DRY_RUN/PROD
        cfg._apply_region_overlay()         # SPLIT/EU_ONLY + engine_pod_map
        cfg._apply_profile_overlay()        # capital_profile
        cfg._apply_quote_overlay()          # primary_quote & min notionals
        cfg._apply_branches_routes_overlay()# enable_branches + allowed_routes
        cfg.apply_overrides_inplace()

        # --- Wallet aliases (Engine) ---
        cfg.wallet_aliases = _Env.get_dict("WALLET_ALIASES", getattr(cfg, "wallet_aliases", {}))

        # --- Comptes par exchange (BalanceFetcher / PrivateWS) ---
        def _mk_accounts(ac_map, ex):
            src = (ac_map or {}).get(ex, {}) or {}
            out = {}
            for alias, creds in src.items():
                # Filtre simple: on garde les entrées avec clés minimales
                if ex in ("BINANCE", "BYBIT"):
                    if not all(k in creds for k in ("api_key", "secret")):
                        continue
                else:  # COINBASE AT
                    if not all(k in creds for k in ("api_key", "secret")):
                        continue
                out[str(alias).upper()] = dict(creds)
            return out

        ac = getattr(cfg.g, "ac", {}) or {}
        cfg.binance_accounts = _mk_accounts(ac, "BINANCE")
        cfg.bybit_accounts = _mk_accounts(ac, "BYBIT")
        cfg.coinbase_at_accounts = _mk_accounts(ac, "COINBASE")

        # --- Sanity checks RM / Desk REB ---
        cfg._sanity_check_rm_caps()

        # --- Aliases & flatten ---
        cfg._init_aliases()
        cfg._rebuild_flat_cache()
        for key in ("MODE", "LIVE_TRADING_ARMED"):
            alias = cfg._resolve_alias(key)
            if not alias:
                raise ConfigError("CONFIG_SCHEMA_INVALID", key)
            sentinel = object()
            if cfg.get_by_dotpath(alias, sentinel) is sentinel:
                raise ConfigError("CONFIG_SCHEMA_INVALID", key)
        # --- Validation fail-closed ---
        dep_mode = str(cfg.g.deployment_mode or "").upper()
        if dep_mode not in DEPLOYMENT_MODES:
            raise ConfigError("CONFIG_SCHEMA_INVALID", "deployment_mode")
        region = str(cfg.g.pod_region or "").upper()
        if region not in {"EU", "US", "JP"}:
            raise ConfigError("CONFIG_SCHEMA_INVALID", "pod_region")
        if region == "JP" and not getattr(cfg.g, "enable_jp", False):
            raise ConfigError("REGION_JP_DISABLED_BY_DEFAULT", "pod_region")
        profile = str(cfg.g.capital_profile or "").upper()
        if profile not in ALLOWED_CAPITAL_PROFILES:
            raise ConfigError("CONFIG_SCHEMA_INVALID", "capital_profile")
        for b in cfg.g.branch_priority:
            if str(b).upper() not in ALLOWED_BRANCHES:
                raise ConfigError("CONFIG_SCHEMA_INVALID", "branch_priority")
        cfg.resolution_version = "v1"
        return cfg

    def _note_config_error(self, reason_code: str, field: str) -> None:
        try:
            from modules import obs_metrics  # type: ignore
            obs_metrics.safe_inc(
                obs_metrics.NONFATAL_ERRORS_TOTAL,
                "nonfatal_errors_total",
                "bot_config",
                module="bot_config",
                kind="config_error",
            )
        except Exception:
            pass
        logging.getLogger(__name__).warning("config error %s on %s", reason_code, field)

    def resolve(
            self,
            *,
            profile: str | None = None,
            region: str | None = None,
            strategy: str | None = None,
            route: str | None = None,
            quote: str | None = None,
            strict: bool = False,
    ) -> Dict[str, Any]:
        def _handle_override_error(reason_code: str, path: str) -> None:
            if strict:
                raise ConfigError(reason_code, str(path))
            self._note_config_error(reason_code, str(path))
        base = {
            "min_notional_by_exchange_quote": dict(self.g.min_notional_by_exchange_quote),
            "branch_budgets_quote": dict(self.rm.branch_budgets_quote),
            "combo_cap_usd_by_profile": dict(self.rm.combo_cap_usd_by_profile),
        }
        now_ms = int(time.time() * 1000)
        for ov in self.overrides or []:
            try:
                value = ov.get("value")
                ttl_raw = ov.get("ttl_s")
                ttl_s = float(ttl_raw) if ttl_raw is not None else None
                reason = ov.get("reason")
                ts_ms = ov.get("ts_ms")
                path = ov.get("path")
                if value is None or ttl_s is None or reason is None or ts_ms is None or not path:
                    _handle_override_error("CONFIG_OVERRIDE_INVALID", str(path))
                    continue
                if ttl_s <= 0:
                    _handle_override_error("CONFIG_OVERRIDE_INVALID", str(path))
                    continue
                try:
                    ts_ms = int(ts_ms)
                except Exception:
                    _handle_override_error("CONFIG_OVERRIDE_INVALID", str(path))
                    continue
                if (now_ms - int(ts_ms)) > int(ttl_s * 1000):
                    _handle_override_error("CONFIG_OVERRIDE_EXPIRED", str(path))
                    continue
                if profile and ov.get("profile") and str(ov.get("profile")).upper() != str(profile).upper():
                    continue
                if region and ov.get("region") and str(ov.get("region")).upper() != str(region).upper():
                    continue
                if strategy and ov.get("strategy") and str(ov.get("strategy")).upper() != str(strategy).upper():
                    continue
                if route and ov.get("route") and str(ov.get("route")).upper() != str(route).upper():
                    continue
                if quote and ov.get("quote") and str(ov.get("quote")).upper() != str(quote).upper():
                    continue
                target: Any = base
                keys = str(path).split(".")
                for k in keys[:-1]:
                    if not isinstance(target, dict) or k not in target:
                        _handle_override_error("CONFIG_OVERRIDE_INVALID", str(path))
                        target = None
                        break
                    target = target.get(k)
                if target is None or not isinstance(target, dict):
                    continue
                leaf = keys[-1]
                if leaf not in target:
                    _handle_override_error("CONFIG_OVERRIDE_INVALID", str(path))
                    continue
                current = target.get(leaf)
                if isinstance(current, (int, float)) and isinstance(value, (int, float)):
                    if float(value) > float(current):
                        _handle_override_error("NO_UPSCALE_VIOLATION", str(path))
                        continue
                target[leaf] = value
            except Exception:
                _handle_override_error("CONFIG_OVERRIDE_INVALID", str(ov.get("path")))
        return base

    def apply_overrides_inplace(self) -> None:
        strict = str(getattr(self.g, "mode", "")).upper() == "PROD"
        resolved = self.resolve(
            profile=getattr(self.g, "capital_profile", None),
            region=getattr(self.g, "pod_region", None),
            strategy=getattr(self.g, "strategy", None),
            route=getattr(self.g, "route", None),
            quote=getattr(self.g, "primary_quote", None),
            strict=strict,
        )
        self.g.min_notional_by_exchange_quote = resolved["min_notional_by_exchange_quote"]
        self.rm.branch_budgets_quote = resolved["branch_budgets_quote"]
        self.rm.combo_cap_usd_by_profile = resolved["combo_cap_usd_by_profile"]
        self.g.branch_budgets_quote = resolved["branch_budgets_quote"]

    def snapshot_dict(self) -> Dict[str, Any]:
        def _scrub(value: Any) -> Any:
            if isinstance(value, dict):
                out: Dict[str, Any] = {}
                for k, v in value.items():
                    key = str(k)
                    if any(t in key.lower() for t in ("secret", "token", "password", "pem", "priv", "key")):
                        continue
                    out[key] = _scrub(v)
                return dict(sorted(out.items(), key=lambda kv: kv[0]))
            if isinstance(value, (list, tuple)):
                return [_scrub(v) for v in value]
            if isinstance(value, (str, int, float, bool)) or value is None:
                return value
            return str(value)

        snapshot = {
            "resolution_version": self.resolution_version,
            "g": asdict(self.g),
            "router": asdict(self.router),
            "ws_public": asdict(self.ws_public),
            "discovery": asdict(self.discovery),
            "scanner": asdict(self.scanner),
            "rm": asdict(self.rm),
            "sim": asdict(self.sim),
            "engine": asdict(self.engine),
            "pws": asdict(self.pws),
            "rebal": asdict(self.rebal),
            "reconciler": asdict(self.reconciler),
            "flow_safety": asdict(self.flow_safety),
            "balances": asdict(self.balances),
            "slip": asdict(self.slip),
            "vol": asdict(self.vol),
            "rl": asdict(self.rl),
            "retry": asdict(self.retry),
            "rpc": asdict(self.rpc),
            "lhm": asdict(self.lhm),
            "dashboard": asdict(self.dashboard),
            "tests": asdict(self.tests),
        }
        return _scrub(snapshot)

    def snapshot_hash(self) -> str:
        data = json.dumps(self.snapshot_dict(), sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(data.encode("utf-8")).hexdigest()

    def _sanity_check_rm_caps(cfg: "BotConfig") -> None:
        """
        Vérifications explicites des invariants de caps (RM/Engine/Pacer/REB).

        - Invariants RM: somme TT/TM/MM <= inflight_trading, inflight_rebal <= inflight_trading,
          combo_cap_usd_by_profile > 0, inflight_rebal borne par cap TM.
        - Invariants Engine: somme des caps CEX par profil >= inflight_trading, workers compatibles
          avec le cap (au moins 50% du plafond inflight RM), pacer_targets bornés par la capacité
          technique agrégée de la région (via engine_pod_map) et par le total Engine.
        - Invariants REB: inflight_rebal <= cap TM et rebal_max_ops_per_min dimensionné vs inflight REB.
        - Sanity checks additionnels (budgets, capital ladder) en warning.

        En PROD, les violations d'invariants peuvent refuser le boot (ValueError). En DRY_RUN, on logue
        en WARNING/ERROR et on continue pour faciliter le debug.
        """
        log = logging.getLogger(__name__)

        rm = getattr(cfg, "rm", None)
        g = getattr(cfg, "g", None)
        engine_cfg = getattr(cfg, "engine", None)
        rebal_cfg = getattr(cfg, "rebal", None)
        if rm is None or g is None:
            return

        mode = str(getattr(g, "mode", "DRY_RUN")).upper()
        fail_hard = mode == "PROD"

        errors: List[str] = []
        warnings: List[str] = []

        def _err(msg: str) -> None:
            errors.append(msg)

        def _warn(msg: str) -> None:
            warnings.append(msg)

        # 1) Invariants RM : trading caps vs inflight + inflight REB borné
        try:
            inflight = getattr(rm, "inflight_trading_by_profile", {}) or {}
            caps = getattr(rm, "caps_trading_by_profile", {}) or {}
            inflight_reb = getattr(rm, "inflight_rebal_by_profile", {}) or {}
            combo_caps = getattr(rm, "combo_cap_usd_by_profile", {}) or {}
            for profile, inflight_total in inflight.items():
                prof_caps = caps.get(profile) or {}
                tt_cap = int(prof_caps.get("TT", 0) or 0)
                tm_cap = int(prof_caps.get("TM", 0) or 0)
                mm_cap = int(prof_caps.get("MM", 0) or 0)
                total_caps = tt_cap + tm_cap + mm_cap
                if total_caps > int(inflight_total):
                    _err(
                        f"caps_trading_by_profile[{profile}] (TT+TM+MM={total_caps}) > inflight_trading_by_profile={inflight_total}"
                    )
                reb_cap = int(inflight_reb.get(profile, 0) or 0)
                if reb_cap > int(inflight_total):
                    _err(
                        f"inflight_rebal_by_profile[{profile}]={reb_cap} > inflight_trading_by_profile={inflight_total}"
                    )
                if reb_cap > tm_cap:
                    _err(
                        f"inflight_rebal_by_profile[{profile}]={reb_cap} > caps_trading_by_profile[{profile}].TM={tm_cap} (REB consomme TM)"
                    )
                combo_cap = combo_caps.get(profile, 0)
                try:
                    combo_val = float(combo_cap)
                except Exception:
                    combo_val = 0.0
                if combo_val <= 0:
                    _err(
                        f"combo_cap_usd_by_profile[{profile}]={combo_cap} <= 0 (REB effectivement désactivé pour ce profil)"
                    )
        except Exception as exc:
            _warn(f"unable to sanity-check RM trading/REB caps: {exc}")

        # 2) budgets branches par quote (warning seulement)
        try:
            budgets = getattr(g, "branch_budgets_quote", {}) or {}
            for quote, per_branch in budgets.items():
                total = 0.0
                for v in (per_branch or {}).values():
                    try:
                        total += float(v)
                    except Exception:
                        continue
                if total > 1.0001:
                    _warn(
                        f"BRANCH_BUDGETS_QUOTE[{quote}] total={total:.3f} > 1.0 (TT+TM+MM+REB)"
                    )
        except Exception as exc:
            _warn(f"unable to sanity-check branch_budgets_quote: {exc}")

        # 3) Invariants Engine vs RM
        try:
            inflight = getattr(rm, "inflight_trading_by_profile", {}) or {}
            engine_caps = getattr(engine_cfg, "inflight_max_by_exchange_by_profile", {}) or {}
            workers = getattr(engine_cfg, "workers_by_profile", {}) or {}
            pod_map = {k: str(v).upper() for k, v in (getattr(g, "engine_pod_map", {}) or {}).items()}
            pacer_targets = getattr(engine_cfg, "pacer_targets", {}) or {}
            # Règle simple pour les workers : au moins 50% du plafond inflight RM (ratio documenté).
            worker_ratio = 0.5
            for profile, inflight_total in inflight.items():
                per_venue = engine_caps.get(profile) or {}
                total_engine_cap = 0
                for v in (per_venue or {}).values():
                    try:
                        total_engine_cap += int(v or 0)
                    except Exception:
                        continue
                if total_engine_cap < int(inflight_total):
                    _err(
                        f"ENGINE inflight_max_by_exchange_by_profile[{profile}] total={total_engine_cap} < inflight_trading_by_profile={inflight_total}"
                    )
                min_workers = max(1, int(int(inflight_total) * worker_ratio))
                worker_count = int(workers.get(profile, 0) or 0)
                if worker_count < min_workers:
                    _err(
                        f"ENGINE workers_by_profile[{profile}]={worker_count} < {worker_ratio:.0%} du plafond inflight_trading_by_profile={inflight_total}"
                    )

                # Pacer: chaque target région <= capacité technique agrégée de la région ET <= capacité totale Engine.
                total_cap_all = total_engine_cap
                for region, target in (pacer_targets or {}).items():
                    try:
                        tgt_val = int(target)
                    except Exception:
                        continue
                    region_cap = 0
                    for ex, ex_region in pod_map.items():
                        if ex_region == str(region).upper():
                            try:
                                region_cap += int((per_venue or {}).get(ex, 0) or 0)
                            except Exception:
                                continue
                    # Si aucune brique régionale trouvée, on borne simplement par la capacité totale Engine.
                    max_cap = total_cap_all if region_cap == 0 else min(region_cap, total_cap_all)
                    rm_cap = int(inflight_total)
                    # Pacer = overlay infra uniquement : borné par RM et par l'estimation Engine.
                    if tgt_val > rm_cap:
                        _err(
                            f"pacer_targets[{region}]={tgt_val} > plafond RM inflight_trading_by_profile={rm_cap} (profil={profile})"
                        )
                    if tgt_val > max_cap:
                        _err(
                            f"pacer_targets[{region}]={tgt_val} > capacité Engine régionale estimée={max_cap} (profil={profile})"
                        )
        except Exception as exc:
            _warn(
                f"unable to sanity-check engine inflight caps / workers / pacer vs RM inflight_trading_by_profile: {exc}"
            )

        # 4) Invariants REB vs cadence
        try:
            inflight_reb = getattr(rm, "inflight_rebal_by_profile", {}) or {}
            if inflight_reb and rebal_cfg is not None:
                max_reb_inflight = max(int(v or 0) for v in inflight_reb.values())
                ops_per_min = int(getattr(rebal_cfg, "rebal_max_ops_per_min", 0) or 0)
                # Règle simple : cadence REB bornée par inflight_rebal * ratio (par défaut 3).
                ratio = float(getattr(rebal_cfg, "rebal_ops_per_min_ratio", 3.0) or 3.0)
                if ratio <= 0:
                    _err(
                        f"rebal_ops_per_min_ratio={ratio} doit être > 0 pour dimensionner la cadence REB"
                    )
                    ratio = 1.0

                max_allowed = int(max_reb_inflight * ratio)
                if ops_per_min > max_allowed:
                    _err(
                        f"rebal_max_ops_per_min={ops_per_min} > inflight_rebal_by_profile max={max_reb_inflight}"
                        f" * ratio={ratio}"
                    )
                if max_reb_inflight > 0 and ops_per_min <= 0:
                    _err(
                        "rebal_max_ops_per_min doit être > 0 quand inflight_rebal est non nul"
                    )
        except Exception as exc:
            _warn(f"unable to sanity-check REB cadence: {exc}")

        # 5) policy Capital Ladder (warning seulement)
        try:
            ladder_cfg = getattr(rm, "capital_ladder_cfg", {}) or {}
            if ladder_cfg:
                last_min = None
                for profile, policy in ladder_cfg.items():
                    policy = policy or {}
                    try:
                        min_cap = float(policy.get("min_capital_per_sc", 0.0) or 0.0)
                    except Exception:
                        continue
                    if min_cap < 0:
                        _warn(
                            f"capital_ladder_cfg[{profile}].min_capital_per_sc={min_cap:.3f} < 0 (corrige la config)"
                        )
                    if last_min is not None and min_cap < last_min:
                        _warn(
                            f"capital_ladder_cfg[{profile}].min_capital_per_sc={min_cap:.3f} < précédent={last_min:.3f} (ordre ladder incohérent NANO→...→LARGE)"
                        )
                    last_min = min_cap
        except Exception as exc:
            _warn(f"unable to sanity-check capital_ladder_cfg: {exc}")

        # Publication des résultats (warnings toujours logués, erreurs loguées puis éventuellement levées en PROD)
        for msg in warnings:
            log.warning("BotConfig: %s", msg)
        for msg in errors:
            if fail_hard:
                log.error("BotConfig: %s", msg)
            else:
                log.warning("BotConfig: %s", msg)

        if errors and fail_hard:
            raise ValueError(
                f"BotConfig invariants violés ({len(errors)}) en mode PROD. Exemple: {errors[0]}"
            )



    # ------------- Overlays -------------
    def _apply_mode_overlay(self) -> None:
        mode = self.g.mode.upper()
        if mode == "DRY_RUN":
            self.g.feature_switches.update({
                "private_ws": False,
                "balance_fetcher": False,
                "engine_real": False,
                "simulator": True,
            })
            self.rm.dry_run = True
        elif mode == "PROD":
            self.g.feature_switches.update({
                "private_ws": True,
                "balance_fetcher": True,
                "engine_real": True,
                "simulator": False,
            })
            self.rm.dry_run = False
            policy = str(getattr(self.rm, "balance_unknown_policy", "DEGRADED") or "DEGRADED").upper()
            if policy == "NEUTRAL":
                raise ConfigError("CONFIG_SCHEMA_INVALID", "rm.balance_unknown_policy")
        else:
            # valeur inattendue -> fallback DRY_RUN
            self.g.mode = "DRY_RUN"
            return self._apply_mode_overlay()

    def _apply_region_overlay(self) -> None:
        dm = self.g.deployment_mode.upper()
        if dm == "SPLIT":
            sl = self.g.split_latency
            # Router: bonus de coalescing CB; stale un peu plus large
            self.router.stale_ms = max(self.router.stale_ms, int(self.engine.anchor_max_staleness_ms))
            # Engine: décalages de timers pour CB (implicite par p95 cibles)
            # On ne modifie pas directement les valeurs par exchange ici;
            # l'Engine les lira via engine_pod_map & pacer_targets.
        elif dm == "EU_ONLY":
            eu_latency = self.g.eu_latency
            self.router.stale_ms = int(eu_latency.get("stale_ms", self.router.stale_ms))
            self.engine.anchor_max_staleness_ms = int(
                eu_latency.get("stale_ms", self.engine.anchor_max_staleness_ms)
            )
            # Alignment P0: Si on est en EU_ONLY, on s'attend à ce que BINANCE/BYBIT soient en EU
            # pour éviter le blocage par défaut dans discovery.
            logging.getLogger(__name__).info("[BotConfig] EU_ONLY mode: forcing BINANCE/BYBIT/COINBASE to EU region")
            for ex in ["BINANCE", "BYBIT", "COINBASE"]:
                self.g.exchange_region_map[ex] = "EU"
        elif dm == "JP_ONLY":
            jp_latency = self.g.jp_latency
            self.router.stale_ms = int(jp_latency.get("stale_ms", self.router.stale_ms))
            self.router.out_queues_maxlen = int(getattr(self.router, "out_queues_maxlen", 20000))
            self.wd.router_health_stale_ms = int(
                getattr(self.wd, "router_health_stale_ms", int(jp_latency.get("stale_ms", 1300)))
            )
            self.wd.ws_public_stale_warn_ms = int(getattr(self.wd, "ws_public_stale_warn_ms", 900))
            self.wd.ws_public_stale_crit_ms = int(getattr(self.wd, "ws_public_stale_crit_ms", 1200))
            self.wd.ws_public_interval_s = float(getattr(self.wd, "ws_public_interval_s", 2.0))
            self.engine.anchor_max_staleness_ms = int(
                jp_latency.get("stale_ms", self.engine.anchor_max_staleness_ms)
            )
            # En JP_ONLY, on s'assure que BINANCE/BYBIT sont JP (par défaut ils le sont)
            for ex in ["BINANCE", "BYBIT"]:
                self.g.exchange_region_map[ex] = "JP"
            if self.g.exchange_region_map.get("COINBASE") == "US":
                 self.g.exchange_region_map["COINBASE"] = "JP" # or stay US? US is not blocked.
        # Hub privé choisira ses URLs via PWS_REGION_MAP / engine_pod_map
        self.pws.PWS_REGION_MAP = dict(self.g.engine_pod_map)

    def _apply_profile_overlay(self) -> None:
        prof = self.g.capital_profile.upper()
        # Barèmes simples par taille (exemple):
        presets = {
            "NANO": dict(
                default_notional=50.0, min_fragment=50.0, qlen=5000, inflight=1
            ),
            "MICRO": dict(
                default_notional=150.0, min_fragment=100.0, qlen=8000, inflight=2
            ),
            "SMALL": dict(
                default_notional=500.0, min_fragment=200.0, qlen=20000, inflight=3
            ),
            "MID": dict(
                default_notional=1500.0, min_fragment=400.0, qlen=35000, inflight=4
            ),
            "LARGE": dict(
                default_notional=5000.0, min_fragment=1000.0, qlen=60000, inflight=6
            ),
        }
        p = presets.get(prof, presets["SMALL"])
        # RM sizing (down-clamp only)
        self.rm.default_notional = min(self.rm.default_notional, p["default_notional"])
        self.rm.max_fragments = min(self.rm.max_fragments,
                                    p["inflight"])  # approximation sizing vs inflight

        # Engine queue/parallelism
        self.engine.tm_max_open_makers = min(self.engine.tm_max_open_makers, p["inflight"])
        # Router queues
        self.router.out_queues_maxlen = min(self.router.out_queues_maxlen, p["qlen"])
        # Min fragment par quote
        mf = dict(self.g.min_fragment_quote)
        for q in mf:
            mf[q] = min(mf[q], p["min_fragment"])
        self.g.min_fragment_quote = mf
        self.engine.min_fragment_quote = mf
        self.rm.min_fragment_quote = mf
        if prof in {"SMALL", "MID", "LARGE"}:
            self.engine.ff_enforce_client_oid_deterministic = True

    def _apply_quote_overlay(self) -> None:
        quote = self.g.primary_quote.upper()
        # Rien d'invasif: on s'assure que min_fragment_quote contient la quote active
        if quote not in self.g.min_fragment_quote:
            self.g.min_fragment_quote[quote] = 200.0
        self.engine.min_fragment_quote = dict(self.g.min_fragment_quote)
        self.rm.min_fragment_quote = dict(self.g.min_fragment_quote)
        # TTL slip/vol aux sections dédiées
        self.slip.ttl_s = int(self.g.vol_slip_ttl.get("slip_s", self.slip.ttl_s))
        self.vol.ttl_s = int(self.g.vol_slip_ttl.get("vol_s", self.vol.ttl_s))
        logging.getLogger(__name__).info(
            "VOL_SLIP_TTL appliqué -> slip_ttl_s=%s vol_ttl_s=%s",
            self.slip.ttl_s,
            self.vol.ttl_s,
        )

    def _apply_branches_routes_overlay(self) -> None:
        # Branches
        eb = self.g.enable_branches
        self.rm.enable_tt = bool(eb.get("tt", self.rm.enable_tt))
        self.rm.enable_tm = bool(eb.get("tm", self.rm.enable_tm))
        self.rm.enable_mm = bool(eb.get("mm", self.rm.enable_mm))
        self.rm.enable_reb = bool(eb.get("reb", self.rm.enable_reb))
        # Routes
        # (Le Scanner/Router utiliseront enabled_exchanges/allowed_routes pour limiter l'univers et les flux)

    # ------------- Alias & flat -------------
    def _init_aliases(self) -> None:
        # alias rétro-compat -> chemin (dotpath dans BotConfig)
        self._alias_map = {
            "DAILY_STRATEGY_BUDGET_QUOTE": "rm.daily_strategy_budget_quote",
            "daily_strategy_budget_quote": "rm.daily_strategy_budget_quote",
            "enable_taker_taker": "rm.enable_tt",
            "ENABLE_TAKER_TAKER": "rm.enable_tt",
            "enable_taker_maker": "rm.enable_tm",
            "ENABLE_TAKER_MAKER": "rm.enable_tm",
            "enable_maker_maker": "rm.enable_mm",
            "ENABLE_MAKER_MAKER": "rm.enable_mm",
            "mm_alias_name": "rm.mm_alias_name",
            "MM_ALIAS_NAME": "rm.mm_alias_name",
            "preempt_mm_for_tt_tm": "rm.preempt_mm_for_tt_tm",
            "PREEMPT_MM_FOR_TT_TM": "rm.preempt_mm_for_tt_tm",
            "ff_enforce_preemption": "rm.ff_enforce_preemption",
            "FF_ENFORCE_PREEMPTION": "rm.ff_enforce_preemption",
            "ff_hedge_fast_lane": "rm.ff_hedge_fast_lane",
            "FF_HEDGE_FAST_LANE": "rm.ff_hedge_fast_lane",
            "ff_tm_enabled": "rm.ff_tm_enabled",
            "FF_TM_ENABLED": "rm.ff_tm_enabled",
            "ff_fail_closed_caps": "rm.ff_fail_closed_caps",
            "FF_FAIL_CLOSED_CAPS": "rm.ff_fail_closed_caps",
            "RM_FF_FAIL_CLOSED_CAPS": "rm.ff_fail_closed_caps",
            "ff_mm_enabled": "rm.ff_mm_enabled",
            "FF_MM_ENABLED": "rm.ff_mm_enabled",
            "ff_mm_opportunistic_gating_enforced": "rm.ff_mm_opportunistic_gating_enforced",
            "FF_MM_OPPORTUNISTIC_GATING_ENFORCED": "rm.ff_mm_opportunistic_gating_enforced",
            "ff_reb_enabled": "rm.ff_reb_enabled",
            "FF_REB_ENABLED": "rm.ff_reb_enabled",
            "TM_QUEUEPOS_MAX_AHEAD_USD": "rm.tm_queuepos_max_ahead_usd",
            "tm_queuepos_max_ahead_usd": "rm.tm_queuepos_max_ahead_usd",
            "TM_QUEUEPOS_MAX_ETA_MS": "rm.tm_queuepos_max_eta_ms",
            "tm_queuepos_max_eta_ms": "rm.tm_queuepos_max_eta_ms",
            "per_strategy_notional_cap": "rm.per_strategy_notional_cap",
            "PER_STRATEGY_NOTIONAL_CAP": "rm.per_strategy_notional_cap",
            "rebal_allow_loss_bps": "rm.rebal_allow_loss_bps",
            "REBAL_ALLOW_LOSS_BPS": "rm.rebal_allow_loss_bps",
            "rebal_volume_haircut": "rm.rebal_volume_haircut",
            "REBAL_VOLUME_HAIRCUT": "rm.rebal_volume_haircut",
            "global_kill_switch": "rm.global_kill_switch",
            "GLOBAL_KILL_SWITCH": "rm.global_kill_switch",
            # TTL slip/vol historiques
            "mm_ttl_ms": "rm.mm_ttl_ms",
            "MM_TTL_MS": "rm.mm_ttl_ms",
            "SLIP_TTL_S": "slip.ttl_s",
            "VOL_TTL_S": "vol.ttl_s",
            # Router legacy
            "router_shards": "router.shards_per_exchange",
            "router_coalesce_window_ms": "router.coalesce_window_ms",
            "router_pair_queue_max": "router.out_queues_maxlen",
            # Scanner legacy
            "binance_depth_level": "scanner.binance_depth_level",
            "mm_depth_min_quote": "scanner.mm_depth_min_quote",
            "MM_DEPTH_MIN_QUOTE": "scanner.mm_depth_min_quote",
            "mm_qpos_max_ahead_quote": "scanner.mm_qpos_max_ahead_quote",
            "MM_QPOS_MAX_AHEAD_QUOTE": "scanner.mm_qpos_max_ahead_quote",
            "mm_min_net_bps": "scanner.mm_min_net_bps",
            "MM_MIN_NET_BPS": "scanner.mm_min_net_bps",
            "mm_hedge_cost_bps": "scanner.mm_hedge_cost_bps",
            "MM_HEDGE_COST_BPS": "scanner.mm_hedge_cost_bps",
            "mm_vol_bps_max": "scanner.mm_vol_bps_max",
            "MM_VOL_BPS_MAX": "scanner.mm_vol_bps_max",
            # Dry-run legacy
            "dry_run": "rm.dry_run",
            # Globals commonly read flat
            "CAPITAL_PROFILE": "g.capital_profile",
            "PRIMARY_QUOTE": "g.primary_quote",
            "min_usdc": "g.min_usdc",
            "MIN_USDC": "g.min_usdc",
            "allowed_routes": "g.allowed_routes",
            "ALLOWED_ROUTES": "g.allowed_routes",
            "DEPLOYMENT_MODE": "g.deployment_mode",
            "POD_REGION": "g.pod_region",
            "MODE": "g.mode",
            "RUN_MODE": "g.mode",
            "LIVE_TRADING_ARMED": "g.live_trading_armed",
            "RESTART_MODE": "g.restart_mode",
            "WS_RELOAD_CAP_PER_HOUR": "g.ws_reload_cap_per_hour",
            "WS_RELOAD_COOLDOWN_S": "g.ws_reload_cooldown_s",
            "WS_RELOAD_MUTE_S": "g.ws_reload_mute_s",
            "FULL_RESTART_CAP_PER_HOUR": "g.full_restart_cap_per_hour",
            "FULL_RESTART_COOLDOWN_S": "g.full_restart_cooldown_s",
            "FULL_RESTART_MUTE_S": "g.full_restart_mute_s",
            # RM inventory/buffer
            "inventory_cap_quote": "rm.inventory_cap_quote",
            "INVENTORY_CAP_QUOTE": "rm.inventory_cap_quote",
            "inventory_cap_usd": "rm.inventory_cap_quote",
            "INVENTORY_CAP_USD": "rm.inventory_cap_quote",
            "min_buffer_quote": "rm.min_buffer_quote",
            "MIN_BUFFER_QUOTE": "rm.min_buffer_quote",
            "min_buffer_usd": "rm.min_buffer_quote",
            "MIN_BUFFER_USD": "rm.min_buffer_quote",
            "balance_unknown_policy": "rm.balance_unknown_policy",
            "BALANCE_UNKNOWN_POLICY": "rm.balance_unknown_policy",
            "transfer_submitted_timeout_s": "rm.transfer_submitted_timeout_s",
            "TRANSFER_SUBMITTED_TIMEOUT_S": "rm.transfer_submitted_timeout_s",
            "transfer_retry_policy": "rm.transfer_retry_policy",
            "TRANSFER_RETRY_POLICY": "rm.transfer_retry_policy",
            # Balance fetcher / fees
            "binance_rest_base": "balances.binance_rest_base",
            "BINANCE_REST_BASE": "balances.binance_rest_base",
            "coinbase_api_base": "balances.coinbase_api_base",
            "COINBASE_API_BASE": "balances.coinbase_api_base",
            "bybit_api_base": "balances.bybit_api_base",
            "BYBIT_API_BASE": "balances.bybit_api_base",
            "default_private_wallet": "balances.default_private_wallet",
            "DEFAULT_PRIVATE_WALLET": "balances.default_private_wallet",
            "wallet_missing_log_interval_s": "balances.wallet_missing_log_interval_s",
            "WALLET_MISSING_LOG_INTERVAL_S": "balances.wallet_missing_log_interval_s",
            "wallet_types": "balances.wallet_types",
            "WALLET_TYPES": "balances.wallet_types",
            "BF_ALERT_PERIOD_S": "balances.BF_ALERT_PERIOD_S",
            "BF_PACER_EU": "balances.BF_PACER_EU",
            "BF_PACER_US": "balances.BF_PACER_US",
            "FEE_LOW_WATERMARK": "balances.fee_low_watermark",
            "FEE_HIGH_WATERMARK": "balances.fee_high_watermark",
            "FEE_TOKEN_LOW_WATERMARKS": "balances.fee_token_low_watermarks",
            # Fee sync collector
            "fee_sync_max_concurrency": "slip.fee_sync_max_concurrency",
            "fee_sync_backoff_initial_s": "slip.fee_sync_backoff_initial_s",
            "fee_sync_backoff_max_s": "slip.fee_sync_backoff_max_s",
            "fee_sync_max_retries": "slip.fee_sync_max_retries",
            "fee_sync_jitter_s": "slip.fee_sync_jitter_s",
            "fee_reality_check_threshold_bps": "slip.fee_reality_check_threshold_bps",
            "FEE_SYNC_MAX_CONCURRENCY": "slip.fee_sync_max_concurrency",
            "FEE_SYNC_BACKOFF_INITIAL_S": "slip.fee_sync_backoff_initial_s",
            "FEE_SYNC_BACKOFF_MAX_S": "slip.fee_sync_backoff_max_s",
            "FEE_SYNC_MAX_RETRIES": "slip.fee_sync_max_retries",
            "FEE_SYNC_JITTER_S": "slip.fee_sync_jitter_s",
            "FEE_REALITY_CHECK_THRESHOLD_BPS": "slip.fee_reality_check_threshold_bps",
            "slip_heartbeat_s": "slip.heartbeat_s",
            "SLIP_HEARTBEAT_S": "slip.heartbeat_s",
            "slip_use_vwap_depth": "slip.use_vwap_depth",
            "SLIP_USE_VWAP_DEPTH": "slip.use_vwap_depth",
            "slip_max_bps_by_quote": "slip.max_bps_by_quote",
            "SLIP_MAX_BPS_BY_QUOTE": "slip.max_bps_by_quote",
            # Volatility monitor knobs
            "vol_ema_alpha": "vol.ema_alpha",
            "VOL_EMA_ALPHA": "vol.ema_alpha",
            "vol_soft_cap_bps": "vol.soft_cap_bps",
            "VOL_SOFT_CAP_BPS": "vol.soft_cap_bps",
            "vol_chaos_cap_bps": "vol.chaos_cap_bps",
            "VOL_CHAOS_CAP_BPS": "vol.chaos_cap_bps",
            "vol_hysteresis": "vol.hysteresis",
            "VOL_HYSTERESIS": "vol.hysteresis",
            "vol_min_delta_bps": "vol.vol_min_delta_bps",
            "VOL_MIN_DELTA_BPS": "vol.vol_min_delta_bps",
            "vol_max_silence_s": "vol.max_silence_s",
            "VOL_MAX_SILENCE_S": "vol.max_silence_s",
            "vol_window_long_m": "vol.window_long_m",
            "VOL_WINDOW_LONG_M": "vol.window_long_m",
            "vol_window_micro_m": "vol.window_micro_m",
            "VOL_WINDOW_MICRO_M": "vol.window_micro_m",
            "vol_winsor_pct": "vol.winsor_pct",
            "VOL_WINSOR_PCT": "vol.winsor_pct",
            # Volatility manager maps
            "vm_size_factor_map": "vol.vm_size_factor_map",
            "VM_SIZE_FACTOR_MAP": "vol.vm_size_factor_map",
            "vm_min_bps_boost_map": "vol.vm_min_bps_boost_map",
            "VM_MIN_BPS_BOOST_MAP": "vol.vm_min_bps_boost_map",
            "tm_neutral_hedge_ratio_map": "vol.tm_neutral_hedge_ratio_map",
            "TM_NEUTRAL_HEDGE_RATIO_MAP": "vol.tm_neutral_hedge_ratio_map",
            "vm_prudence_thresholds_bps": "vol.vm_prudence_thresholds_bps",
            "VM_PRUDENCE_THRESHOLDS_BPS": "vol.vm_prudence_thresholds_bps",
            "vm_maker_pad_ticks_map": "vol.vm_maker_pad_ticks_map",
            "VM_MAKER_PAD_TICKS_MAP": "vol.vm_maker_pad_ticks_map",
            # Rebalancing manager
            "rebal_quantum_min_quote": "rebal.rebal_quantum_min_quote",
            "REBAL_QUANTUM_MIN_QUOTE": "rebal.rebal_quantum_min_quote",
            "rebal_max_ops_per_min": "rebal.rebal_max_ops_per_min",
            "REBAL_MAX_OPS_PER_MIN": "rebal.rebal_max_ops_per_min",
            "rebal_priority": "rebal.rebal_priority",
            "REBAL_PRIORITY": "rebal.rebal_priority",
            "rebal_hint_ttl_s": "rebal.rebal_hint_ttl_s",
            "REBAL_HINT_TTL_S": "rebal.rebal_hint_ttl_s",
            "rebal_snapshots_missing_error_s": "rebal.rebal_snapshots_missing_error_s",
            "REBAL_SNAPSHOTS_MISSING_ERROR_S": "rebal.rebal_snapshots_missing_error_s",
            "rebal_snapshots_error_cooldown_s": "rebal.rebal_snapshots_error_cooldown_s",
            "REBAL_SNAPSHOTS_ERROR_COOLDOWN_S": "rebal.rebal_snapshots_error_cooldown_s",
            "rebal_quantum_quote_map": "rebal.rebal_quantum_quote_map",
            "REBAL_QUANTUM_QUOTE_MAP": "rebal.rebal_quantum_quote_map",
            "rebal_ops_per_min_ratio": "rebal.rebal_ops_per_min_ratio",
            "REBAL_OPS_PER_MIN_RATIO": "rebal.rebal_ops_per_min_ratio",
            "RM_BRANCH_PRIORITY": "rm.branch_priority",
            "RM_BRANCH_BUDGETS_QUOTE": "rm.branch_budgets_quote",
            # Private WS Hub
            "PWS_PACER_EU": "pws.PWS_PACER_EU",
            "PWS_PACER_US": "pws.PWS_PACER_US",
            "PWS_ALERT_PERIOD_S": "pws.PWS_ALERT_PERIOD_S",
            "PWS_QUEUE_MAXLEN": "pws.PWS_QUEUE_MAXLEN",
            "pws_queue_maxlen": "pws.PWS_QUEUE_MAXLEN",
            "PWS_POOL_SIZE_EU": "pws.PWS_POOL_SIZE_EU",
            "pws_pool_size_eu": "pws.PWS_POOL_SIZE_EU",
            "PWS_POOL_SIZE_US": "pws.PWS_POOL_SIZE_US",
            "pws_pool_size_us": "pws.PWS_POOL_SIZE_US",
            "PWS_DROP_POLICY": "pws.pws_drop_policy",
            "pws_drop_policy": "pws.pws_drop_policy",
            "PWS_NO_DROP_CRITICAL_ENFORCED": "pws.ff_pws_no_drop_critical_enforced",
            "pws_no_drop_critical_enforced": "pws.ff_pws_no_drop_critical_enforced",
            "PWS_STRICT_DEDUP_ENFORCED": "pws.ff_pws_strict_dedup_enforced",
            "pws_strict_dedup_enforced": "pws.ff_pws_strict_dedup_enforced",
            "PWS_QUEUE_SATURATION_RATIO": "pws.PWS_QUEUE_SATURATION_RATIO",
            "pws_queue_saturation_ratio": "pws.PWS_QUEUE_SATURATION_RATIO",
            "PWS_PING_INTERVAL_S": "pws.PWS_PING_INTERVAL_S",
            "PWS_PONG_TIMEOUT_S": "pws.PWS_PONG_TIMEOUT_S",
            "PWS_HEARTBEAT_MAX_GAP_S": "pws.PWS_HEARTBEAT_MAX_GAP_S",
            "PWS_STABLE_RESET_S": "pws.PWS_STABLE_RESET_S",
            "PWS_JITTER_MS": "pws.PWS_JITTER_MS",
            "PWS_BACKOFF_BASE_MS": "pws.PWS_BACKOFF_BASE_MS",
            "PWS_BACKOFF_MAX_MS": "pws.PWS_BACKOFF_MAX_MS",
            # Balance fetcher pacers (string states)
            "BF_PACER_EU_STR": "balances.BF_PACER_EU",
            "BF_PACER_US_STR": "balances.BF_PACER_US",
            "BALANCES_TTL_S_NORMAL": "rm.balance_ttl_s_normal",
            "BALANCES_TTL_S_DEGRADED": "rm.balance_ttl_s_degraded",
            "BALANCES_TTL_S_BLOCK": "rm.balance_ttl_s_block",
            "balances_ttl_s_normal": "rm.balance_ttl_s_normal",
            "balances_ttl_s_degraded": "rm.balance_ttl_s_degraded",
            "balances_ttl_s_block": "rm.balance_ttl_s_block",
            "RESTART_WEBHOOK_URL": "alerting.webhook.url",
            "RESTART_WEBHOOK_HMAC_KEY": "alerting.webhook.hmac_secret",
            "TELEGRAM_REQUIRE_ACK": "alerting.telegram.require_ack",
            # BalanceFetcher: WS merge/reconcile & readiness
            "WS_BAL_TTL_SECONDS": "balances.WS_BAL_TTL_SECONDS",
            "ws_bal_ttl_seconds": "balances.WS_BAL_TTL_SECONDS",

            "ENABLE_WS_BALANCE_MERGE": "balances.ENABLE_WS_BALANCE_MERGE",
            "enable_ws_balance_merge": "balances.ENABLE_WS_BALANCE_MERGE",

            "WS_RECO_MISS_RATE_THR_PER_MIN": "balances.ws_reco_miss_rate_thr_per_min",
            "ws_reco_miss_rate_thr_per_min": "balances.ws_reco_miss_rate_thr_per_min",

            "WS_RECO_RESYNC_AGE_THR_S": "balances.ws_reco_resync_age_thr_s",
            "ws_reco_resync_age_thr_s": "balances.ws_reco_resync_age_thr_s",

            "BALANCES_READY_MIN_STATE": "balances.balances_ready_min_state",
            "balances_ready_min_state": "balances.balances_ready_min_state",

        }

    def _rebuild_flat_cache(self) -> None:
        def _walk(prefix: str, obj: Any):
            if isinstance(obj, (BotConfig,)):
                # never happens (root)
                return
            if hasattr(obj, "__dataclass_fields__"):
                for k in obj.__dataclass_fields__.keys():
                    v = getattr(obj, k)
                    _walk(f"{prefix}{k}", v)
            elif isinstance(obj, dict):
                for k,v in obj.items():
                    _walk(f"{prefix}{k}", v)
            else:
                key = prefix.rstrip('.')
                self._flat_cache[key.upper()] = obj
        # reset
        self._flat_cache.clear()
        # walk each top field
        for section_name in [
            'g','router','ws_public','discovery','scanner','rm','sim','engine','pws','rebal','reconciler','balances','slip','vol','rl','retry','rpc','lhm','dashboard','tests']:
            _walk(f"{section_name}.", getattr(self, section_name))
        engine_cfg = getattr(self, "engine", None)
        pacer_targets = getattr(engine_cfg, "pacer_targets", {}) if engine_cfg else {}
        for region in ("EU", "EU_CB", "EU-CB", "US", "JP"):
            key = region.replace("-", "_")
            if region in pacer_targets:
                self._flat_cache[f"PACER_TARGETS_{key}"] = pacer_targets[region]

    def _resolve_alias(self, name: str) -> Optional[str]:
        if name in self._alias_map:
            return self._alias_map[name]
        # allow uppercase-with-underscores mapping to nested dot paths best-effort
        # e.g. ENGINE_TM_EXPOSURE_TTL_MS -> engine.tm_exposure_ttl_ms
        lowered = name.lower()
        if lowered.startswith("g_"):
            return "g." + lowered[2:]
        parts = lowered.split('_', 1)
        if len(parts) == 2:
            return parts[0] + "." + parts[1]
        return None

    def __getattr__(self, item: str) -> Any:
        # legacy flat access: cfg.SLIP_TTL_S / cfg.router_shards / cfg.ENGINE_TM_EXPOSURE_TTL_MS
        key = item.upper()
        if key in self._flat_cache:
            return self._flat_cache[key]
        alias = self._resolve_alias(item)
        if alias:
            sentinel = object()
            value = self.get_by_dotpath(alias, sentinel)
            if value is not sentinel:
                return value
            raise AttributeError(item)
        raise AttributeError(item)

    @property
    def slo(self) -> Dict[str, Dict[str, PathSLO]]:
        """
        Contrat SLO / TTL public ↔ privé, structuré comme:
            slo[deployment_mode][exchange] -> PathSLO

        - deployment_mode: "EU_ONLY", "SPLIT" ou "JP_ONLY"
        - exchange:        "BINANCE", "BYBIT", "COINBASE", ...

        Objectif:
        - unifier ce que le *public* a le droit d'appeler "OK"
          (fraîcheur L2, slip.age, vol.age),
        - avec ce que le *privé* peut réellement garantir
          (latence acks, timeouts Engine, TTL balances, staleness PrivateWS).

        Ce contrat est construit à partir des paramètres existants
        de BotConfig (router, engine, slip, vol, RM balances, PWS).
        """
        # --- SLO acks par région (mêmes ordres de grandeur que EnginePacer) ---
        # NB : ces valeurs pourront être harmonisées plus tard en lisant BotConfig
        # côté EnginePacer au lieu de _DEFAULT_TARGETS.
        ack_by_region: Dict[str, Tuple[float, float, float]] = {
            # region: (ack_target_ms, ack_hi_ms, ack_sev_ms)
            "EU":     (90.0, 120.0, 150.0),
            "US":     (150.0, 180.0, 210.0),
            "EU-CB":  (180.0, 210.0, 240.0),
            "EU_CB":  (180.0, 210.0, 240.0),  # alias toléré
            # Fallback générique
            "DEFAULT": (90.0, 120.0, 150.0),
        }

        # --- Source unique TTL slip/vol (déjà pilotés par env via cfg.slip/vol.ttl_s) ---
        slip_ttl_s = float(getattr(getattr(self, "slip", None), "ttl_s", 2.0))
        vol_ttl_s  = float(getattr(getattr(self, "vol",  None), "ttl_s", 5.0))

        # --- Fraîcheur L2 max : intersection Router / Engine / slip ---
        # On s'assure par construction que:
        #   l2_fresh_max_s <= slip_ttl_s
        #   l2_fresh_max_s <= 0.8 * order_timeout_s
        try:
            router_stale_s = float(getattr(self.router, "stale_ms", 1200)) / 1000.0
        except Exception:
            router_stale_s = 1.2

        try:
            anchor_stale_s = float(getattr(self.engine, "anchor_max_staleness_ms", 1200)) / 1000.0
        except Exception:
            anchor_stale_s = router_stale_s

        order_timeout_s = float(getattr(self.engine, "order_timeout_s", 3.0))
        # borne "dur" : on ne veut pas que le public considère OK au-delà de 80% du timeout Engine
        hard_cap_s = max(0.0, order_timeout_s * 0.8)

        l2_fresh_max_s = min(router_stale_s, anchor_stale_s, hard_cap_s, slip_ttl_s)

        # --- TTL balances (déjà injectées par from_env en overlay RM) ---
        # Defaults alignés sur RM_BALANCE_TTL_S_* dans from_env()
        balances_ttl_s_normal = float(getattr(self.rm, "balance_ttl_s_normal", 60.0))
        balances_ttl_s_degraded = float(getattr(self.rm, "balance_ttl_s_degraded", 180.0))
        balances_ttl_s_block = float(getattr(self.rm, "balance_ttl_s_block", 600.0))

        # --- TTL TM exposure / hedge ---
        tm_exposure_ttl_ms = float(getattr(self.engine, "tm_exposure_ttl_ms", 1500.0))

        # --- Staleness privé WS : gap heartbeat max ---
        pws_heartbeat_gap_max_s = float(
            getattr(self.pws, "PWS_HEARTBEAT_MAX_GAP_S", 30.0)
        )

        # --- Construction du contrat par mode / exchange ---
        slo: Dict[str, Dict[str, PathSLO]] = {}
        deployment_modes = ("EU_ONLY", "SPLIT", "JP_ONLY")

        # On normalise les exchanges à UPPER (BINANCE/BYBIT/COINBASE…)
        enabled_exchanges = [ex.upper() for ex in getattr(self.g, "enabled_exchanges", [])]

        for mode in deployment_modes:
            per_ex: Dict[str, PathSLO] = {}

            for ex in enabled_exchanges:
                # Région d'exécution de l'exchange (EU / US / EU-CB…)
                region = str(self.g.exchange_region_map.get(ex, self.g.pod_region)).upper()
                if region == "EU_CB" and ex == "COINBASE":
                    region_key = "EU-CB"
                else:
                    region_key = region

                ack_target_ms, ack_hi_ms, ack_sev_ms = ack_by_region.get(
                    region_key,
                    ack_by_region["DEFAULT"],
                )

                public = SLOPublicPath(
                    l2_fresh_max_s=l2_fresh_max_s,
                    slip_ttl_s=slip_ttl_s,
                    vol_ttl_s=vol_ttl_s,
                )
                private = SLOPrivatePath(
                    ack_target_ms=ack_target_ms,
                    ack_hi_ms=ack_hi_ms,
                    ack_sev_ms=ack_sev_ms,
                    order_timeout_s=order_timeout_s,
                    tm_exposure_ttl_ms=tm_exposure_ttl_ms,
                    balances_ttl_s_normal=balances_ttl_s_normal,
                    balances_ttl_s_degraded=balances_ttl_s_degraded,
                    balances_ttl_s_block=balances_ttl_s_block,
                    pws_heartbeat_gap_max_s=pws_heartbeat_gap_max_s,
                )

                per_ex[ex] = PathSLO(public=public, private=private)

            slo[mode] = per_ex

        return slo



    # ------------- Utils -------------
    def get_by_dotpath(self, path: str, default: Any=None) -> Any:
        cur: Any = self
        for part in path.split('.'):
            if hasattr(cur, part):
                cur = getattr(cur, part)
            elif isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return default
        return cur

    def as_dict(self) -> Dict[str, Any]:
        # dict profond propre (dataclasses -> dict)
        d = asdict(self)
        # retirer caches internes
        d.pop('_flat_cache', None)
        d.pop('_alias_map', None)
        return d

    # Export pratique pour diagnostics
    def to_json(self, indent: int=2) -> str:
        return json.dumps(self.as_dict(), indent=indent, sort_keys=False)



# -------------- Exemple d'utilisation --------------
if __name__ == "__main__":
    cfg = BotConfig.from_env()
    print("MODE:", cfg.g.mode)
    print("FEATURES:", cfg.g.feature_switches)
    print("ROUTES:", cfg.g.allowed_routes)
    print("PRIMARY_QUOTE:", cfg.g.primary_quote)
    # Accès rétro-compat:
    print("SLIP_TTL_S:", cfg.SLIP_TTL_S)
    print("ENGINE_TM_EXPOSURE_TTL_MS:", cfg.ENGINE_TM_EXPOSURE_TTL_MS)
