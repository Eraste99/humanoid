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
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Tuple, Optional, Union

# ------------------------------
# Taxonomie canonique (branches / profils capital)
# ------------------------------
# Utilisées comme référence unique par RiskManager, ExecutionEngine, etc.
ALLOWED_BRANCHES = ("TT", "TM", "MM", "REB")
ALLOWED_CAPITAL_PROFILES = ("NANO", "MICRO", "SMALL", "MID", "LARGE")


# ------------------------------
# Helpers de parsing d'env
# ------------------------------

class _Env:
    TRUE = {"1","true","yes","on","y","t"}
    FALSE = {"0","false","no","off","n","f"}

    @staticmethod
    def get(name: str, default: Optional[str]=None) -> Optional[str]:
        val = os.getenv(name)
        return val if val is not None else default

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
    def get_list(name: str, default: Optional[List[str]]=None) -> List[str]:
        v = _Env.get(name)
        if v is None:
            return list(default or [])
        # try JSON first
        try:
            x = json.loads(v)
            if isinstance(x, list):
                return x
        except Exception:
            pass
        # fallback: CSV
        return [t.strip() for t in v.split(',') if t.strip()]

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
        # fallback: semicolon-separated pairs "A:B;C:D"
        pairs = []
        for chunk in v.split(';'):
            if ':' in chunk:
                a,b = chunk.split(':',1)
                a,b = a.strip(), b.strip()
                if a and b:
                    pairs.append((a,b))
        return pairs


# ------------------------------
# Sections de config
# ------------------------------

@dataclass
class Globals:
    deployment_mode: str = "SPLIT"              # "EU_ONLY" | "SPLIT"
    pod_region: str = "EU"                      # "EU" | "US"
    engine_pod_map: Dict[str,str] = field(default_factory=lambda: {
        "BINANCE":"EU", "BYBIT":"EU", "COINBASE":"US"
    })
    split_latency: Dict[str,int] = field(default_factory=lambda: {
        "skew_ms": 25,
        "base_delta_ms": 180,
        "stale_ms": 1300,
        "cb_coalesce_bonus_ms": 12,
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
    })

    pacer_mode: str = "NORMAL"
    mode: str = "DRY_RUN"  # DRY_RUN | PROD
    feature_switches: Dict[str,bool] = field(default_factory=lambda: {
        "private_ws": False,
        "balance_fetcher": False,
        "engine_real": False,
        "simulator": True,
    })


# --- Router ---
@dataclass
class RouterCfg:
    coalesce_window_ms: int = 20
    stale_ms: int = 1200
    shards_per_exchange: int = 2
    out_queues_maxlen: int = 20000
    mux_poll_ms: int = 2
    require_l2_first: bool = True
    drop_policy: Dict[str,str] = field(default_factory=lambda: {
        "CORE": "never",
        "PRIMARY": "conservative",
        "AUDITION": "best_effort",
        "SANDBOX": "drop_when_backlog",
    })
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
    cb_coalesce_bump_ms: int = 12
    topic_max_hz: Dict[str, float] = field(default_factory=dict)

# --- WS Public ---
@dataclass
class WsPublicCfg:
    ws_backoff: Dict[str,int] = field(default_factory=lambda: {"base_ms":500, "max_ms":15_000, "jitter":1})
    connect_timeout_s: int = 10
    read_timeout_s: int = 30
    ping_interval_s: int = 20
    pong_timeout_s: int = 10
    auto_resubscribe: bool = True
    max_retries: int = 0  # 0 = infini (avec backoff)

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
# ---------------------------------------------------------------------------
# Router / Scanner & RiskManager configs
# ---------------------------------------------------------------------------


@dataclass
class RouterCfg:
    coalesce_window_ms: int = 20
    stale_ms: int = 1200
    shards_per_exchange: Tuple[int, int, int] = (2, 1, 1)
    out_queues_maxlen: int = 20000
    # P0: quotas stream-centrics optionnels par kind (combo/vol/slip/health)
    # ex: {"combo": 5000, "vol": 2000, "slip": 2000, "health": 2000}
    out_queues_maxlen_by_kind: Dict[str, int] = field(default_factory=dict)
    # P0: cadence cible ≈ 5 ms
    mux_poll_ms: int = 5
    require_l2_first: bool = True
    # P0: matrix de comportement par tier, pilotée par env (ROUTER_DROP_POLICY)
    drop_policy: Dict[str, Any] = field(default_factory=dict)
    # P0: taille des deques internes par exchange (ROUTER_DEQUE_MAXLEN_PER_EX)
    deque_maxlen_per_ex: Dict[str, int] = field(default_factory=dict)
    # P0: paramètres de backpressure Router (ROUTER_BACKPRESSURE)
    backpressure: Dict[str, Any] = field(default_factory=dict)
    # P0: bump spécifique Coinbase en SPLIT (ROUTER_CB_COALESCE_BUMP_MS)
    cb_coalesce_bump_ms: int = 10
    # P0: plafond Hz par topic (ROUTER_TOPIC_MAX_HZ)
    topic_max_hz: Dict[str, float] = field(default_factory=dict)


@dataclass
class ScannerCfg:
    # Workers et backpressure de base
    workers: int = 1
    backpressure_log_every: int = 1000
    max_opportunities: int = 1000

    # P0: dedup & fenêtres de scan
    dedup_window_s: float = 0.18  # cible P0 ≈ 0.12–0.20 s
    dedup_cooldown_s: float = 0.16
    max_pairs_per_tick: int = 150
    scan_interval: float = 0.02
    max_time_skew_s: float = 0.3

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

    # MM / hints publics (Scanner = couche "soft" pour le MM)
    # - mm_depth_min_quote / mm_qpos_max_ahead_quote : filtres de profondeur / queue pour les hints MM
    # - mm_min_net_bps / mm_hedge_cost_bps         : net bps minimal pour considérer un setup MM
    # - mm_vol_bps_max                             : seuil de vol (en bps Monitor) au-dessus duquel
    #   le Scanner désactive les opportunités MM côté hints (le RM reste la couche "hard").

    enable_mm_hints: bool = True
    binance_depth_level: int = 50
    mm_rotation_enabled: bool = False
    mm_seed_pairs: Tuple[str, ...] = ()
    mm_depth_min_quote: float = 200.0
    mm_qpos_max_ahead_quote: float = 5000.0
    mm_min_net_bps: float = 0.6
    mm_hedge_cost_bps: float = 3.0
    mm_vol_bps_max: float = 40.0

    # Rebalancing
    allow_loss_bps_rebal: float = 0.0


# --- Risk Manager ---
@dataclass
class RiskManagerCfg:
    enable_tt: bool = True
    enable_tm: bool = True
    enable_mm: bool = False
    enable_reb: bool = True
    enable_maker_maker: bool = False
    branch_priority: List[str] = field(default_factory=lambda: ["tt","tm","reb","mm"])
    branch_budgets_quote: Dict[str, Dict[str,float]] = field(default_factory=lambda: {
        "USDC": {"TT":0.60, "TM":0.35, "MM":0.00, "REB":0.05},
        "EUR":  {"TT":0.60, "TM":0.35, "MM":0.00, "REB":0.05},
    })

    # Ticket 10 — Caps d'inflight business par profil / branche
    # Profil = NANO/MICRO/SMALL/MID/LARGE (source: g.capital_profile)
    # Ces caps sont la "vérité business" que le RM appliquera, et que l'Engine devra respecter.

    # Inflight globaux "trading" (TT + TM + MM), par profil de capital.
    inflight_trading_by_profile: Dict[str, int] = field(default_factory=lambda: {
        "NANO": 2,
        "MICRO": 4,
        "SMALL": 8,
        "MID": 16,
        "LARGE": 32,
    })

    # Répartition des caps par branche TRADING (TT/TM/MM) pour chaque profil.
    # Invariant attendu côté RM plus tard:
    #   TT_cap + TM_cap + MM_cap <= inflight_trading_by_profile[profile]
    caps_trading_by_profile: Dict[str, Dict[str, int]] = field(default_factory=lambda: {
        # NANO : 2 inflights globaux => TT=1, TM=1, MM=0 (MM désactivé en bootstrap)
        "NANO":  {"TT": 1, "TM": 1, "MM": 0},
        # MICRO : 4 inflights globaux => TT=2, TM=1, MM=1
        "MICRO": {"TT": 2, "TM": 1, "MM": 1},
        # SMALL : 8 inflights globaux => TT=3, TM=3, MM=1
        "SMALL": {"TT": 3, "TM": 3, "MM": 1},
        # MID : 16 inflights globaux => TT=6, TM=5, MM=3
        "MID":   {"TT": 6, "TM": 5, "MM": 3},
        # LARGE : 32 inflights globaux => TT=12, TM=10, MM=6
        "LARGE": {"TT": 12, "TM": 10, "MM": 6},
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

    sfc_slippage_source: str = "fills"  # fills|hybrid|off
    prefilter_slip_bps: float = 2.0
    mm_ttl_ms: int = 2200
    mm_alias_name: str = "MM"
    mm_depth_min_usd: float = 7500.0
    mm_qpos_max_ahead_usd: float = 5000.0
    mm_min_p_both: float = 0.0
    mm_min_net_bps: float = 0.0005
    mm_hedge_cost_bps: float = 5.0
    # Seuil MM côté RM (non utilisé pour l’instant) :
    # - Scanner.mm_vol_bps_max = couche soft : coupe les hints MM en vol élevée.
    # - RiskManager.mm_vol_bps_max = garde-fou hard éventuel (par profil / VM) si on
    #   veut, plus tard, empêcher le RM d’admettre du MM au-delà d’une certaine vol.
    #   Default aligné sur Scanner pour éviter les surprises quand on l’activera.
    mm_vol_bps_max: float = 40.0

    global_kill_switch: bool = False
    daily_strategy_budget_quote: Dict[str, float] = field(default_factory=dict)
    daily_budget_reset_interval_s: float = 86400.0
    decision_log_path: str = ""
    preempt_mm_for_tt_tm: bool = True
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

# --- Engine ---
@dataclass
class EngineCfg:
    pacer_min_ms: int = 2
    pacer_max_ms: int = 25
    pacer_jitter_ms: int = 2
    # Pacer : cibles régionales *strictement* bornées par les caps RM/Engine (down-clamp only).
    # Le Pacer ne peut jamais élargir la capacité au-delà des plafonds inflight RM/Engine.
    pacer_targets: Dict[str, int] = field(default_factory=lambda: {"EU": 8, "EU_CB": 12, "US": 12})

    # Ticket 10 — capacités techniques Engine (workers / inflight par CEX)

    # Nombre de workers (tâches ordre) par profil de capital.
    workers_by_profile: Dict[str, int] = field(default_factory=lambda: {
        "NANO": 4,
        "MICRO": 8,
        "SMALL": 16,
        "MID": 24,
        "LARGE": 40,
    })

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


    tt_max_skew_ms: int = 35
    order_timeout_s: int = 3
    idempotence_on: bool = True

    tm_exposure_ttl_ms: int = 1500
    tm_exposure_ttl_hedge_ratio: float = 0.5
    tm_watch_timeout_s: int = 2

    tm_queuepos_max_ahead_usd: float = 20000.0
    tm_queuepos_max_eta_ms: int = 1200
    tm_nn_hedge_ratio: float = 0.65

    maker_pad_ticks: int = 2
    tm_max_open_makers: int = 3

    frontload_weights: List[float] = field(default_factory=lambda: [0.50,0.35,0.15])
    frontload_group_size: int = 3
    min_fragment_quote: Dict[str,float] = field(default_factory=lambda: {"USDC":200.0, "EUR":200.0})

    anchor_max_staleness_ms: int = 1200
    anchor_halve_guard_ms: int = 300
    max_price_deviation_pct: float = 0.50

    mm_hysteresis_s: float = 0.600
    mm_min_quote_lifetime_ms: int = 400
    vol_soft_cap_bps: float = 45.0
    vol_hard_cap_bps: float = 80.0
    freeze_tm_on_vol: bool = True
    depth_min_quote_tt: float = 200.0
    depth_min_quote_tm: float = 500.0
    depth_min_quote_mm: float = 1000.0
    depth_levels_check: int = 3
    price_band_bps_floor: float = 15.0
    price_band_bps_cap: float = 50.0
    vol_price_band_k: float = 0.6
    circuit_escalation_window_s: float = 60.0
    circuit_mute_escalation: float = 1.7
    circuit_mute_min_s: float = 10.0
    circuit_mute_max_s: float = 900.0
    circuit_mute_s_tm: float = 300.0
    circuit_mute_s_mm: float = 300.0

# --- Private WS Hub ---
@dataclass
class PrivateWSHubCfg:
    PWS_POOL_SIZE_EU: int = 2
    PWS_POOL_SIZE_US: int = 2
    PWS_REGION_MAP: Dict[str,str] = field(default_factory=lambda: {"BINANCE":"EU","BYBIT":"EU","COINBASE":"US"})
    PWS_QUEUE_MAXLEN: int = 5000
    PWS_QUEUE_SATURATION_RATIO: float = 0.85
    PWS_PING_INTERVAL_S: int = 20
    PWS_PONG_TIMEOUT_S: int = 10
    PWS_HEARTBEAT_MAX_GAP_S: int = 30
    PWS_STABLE_RESET_S: int = 30
    PWS_JITTER_MS: int = 2
    PWS_BACKOFF_BASE_MS: int = 500
    PWS_BACKOFF_MAX_MS: int = 20_000
    cb_ack_ws_enabled: bool = True
    cb_private_poll_interval_s: int = 2
    PWS_PACER_EU: str = "NORMAL"
    PWS_PACER_US: str = "NORMAL"
    PWS_ALERT_PERIOD_S: int = 5

@dataclass
class RebalancingCfg:
    rebal_quantum_min_quote: float = 50.0
    # Cadence REB max (opérations/min). Doit rester bornée par inflight_rebal
    # multiplié par un ratio simple (cf. _sanity_check_rm_caps, ratio=3 par défaut).
    rebal_max_ops_per_min: int = 6
    rebal_priority: List[str] = field(default_factory=lambda: ["CASH","CRYPTO","OVERLAY"])
    rebal_hint_ttl_s: int = 120
    rebal_snapshots_missing_error_s: float = 45.0
    rebal_snapshots_error_cooldown_s: float = 60.0
    rebal_quantum_quote_map: Dict[str, float] = field(default_factory=lambda: {"USDC": 250.0, "EUR": 250.0})


# --- Reconciler ---
@dataclass
class ReconcilerCfg:
    RECO_ALERT_PERIOD_S: int = 30
    RECO_MISS_BURST_THRESHOLD: int = 5

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


# --- Slippage Handler ---
@dataclass
class SlippageCfg:
    ttl_s: int = 2
    heartbeat_s: int = 1
    max_bps_by_quote: Dict[str,float] = field(default_factory=lambda: {"USDC": 12.0, "EUR": 14.0})
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
    ema_alpha: float = 0.20
    soft_cap_bps: float = 80.0
    chaos_cap_bps: float = 150.0
    hysteresis: float = 0.25
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
    hard_caps_rps_by_kind: Dict[str, float] = field(default_factory=dict)

    bursts_by_exchange: Dict[str, float] = field(default_factory=dict)
    bursts_by_exchange_kind: Dict[str, Dict[str, float]] = field(default_factory=dict)
    bursts_by_kind: Dict[str, float] = field(default_factory=dict)

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
    max_retries: int = 2
    mtls_enabled: bool = True
    ca_cert: Optional[str] = None
    server_cert: Optional[str] = None
    server_key: Optional[str] = None
    client_cert: Optional[str] = None
    client_key: Optional[str] = None
    require_client_cert: bool = True
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
    LHM_Q_STREAM_MAX: int = 20000
    LHM_STREAM_BATCH: int = 200
    LHM_DROP_WHEN_FULL: bool = True
    LHM_HIGH_WATERMARK_RATIO: float = 0.85
    LHM_MAX_QUEUE_PLATEAU_S: int = 3
    LHM_MM_SAMPLING_QUOTES: float = 0.05
    LHM_MM_SAMPLING_CANCELS: float = 0.02
    LHM_JSONL_QUEUE_CAP: int = 5000
    LHM_JSONL_MAX_BYTES: int = 256 * 1024 * 1024
    LHM_JSONL_MAX_AGE_S: int = 3600
    LHM_DISK_WARN_PCT: float = 70.0
    LHM_DISK_CRIT_PCT: float = 85.0
    LHM_DB_NAME: str = "loggerh"
    LHM_DB_MAX_BYTES: int = 256 * 1024 * 1024
    LHM_DB_MAX_AGE_S: int = 24 * 3600
    LHM_WD_INTERVAL_S: float = 2.0
    LHM_WD_PLATEAU_WINDOW: int = 15
    LHM_WD_QUEUE_MIN_SIZE: int = 50
    LHM_WD_STALL_THRESHOLD_S: float = 10.0
    LHM_TRADE_BATCH_SIZE: int = 30
    LHM_TRADE_FLUSH_INTERVAL_S: float = 0.35

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
# BotConfig racine
# ------------------------------
@dataclass
class BotConfig:
    # Globaux
    g: Globals = field(default_factory=Globals)

    # Sections modules
    router: RouterCfg = field(default_factory=RouterCfg)
    ws_public: WsPublicCfg = field(default_factory=WsPublicCfg)
    discovery: DiscoveryCfg = field(default_factory=DiscoveryCfg)
    scanner: ScannerCfg = field(default_factory=ScannerCfg)
    rm: RiskManagerCfg = field(default_factory=RiskManagerCfg)
    sim: SimulatorCfg = field(default_factory=SimulatorCfg)
    engine: EngineCfg = field(default_factory=EngineCfg)
    pws: PrivateWSHubCfg = field(default_factory=PrivateWSHubCfg)
    rebal: RebalancingCfg = field(default_factory=RebalancingCfg)
    reconciler: ReconcilerCfg = field(default_factory=ReconcilerCfg)

    balances: BalanceFetcherCfg = field(default_factory=BalanceFetcherCfg)
    slip: SlippageCfg = field(default_factory=SlippageCfg)
    vol: VolatilityCfg = field(default_factory=VolatilityCfg)
    rl: RateLimiterCfg = field(default_factory=RateLimiterCfg)
    retry: RetryPolicyCfg = field(default_factory=RetryPolicyCfg)
    rpc: RPCCfg = field(default_factory=RPCCfg)
    lhm: LoggerCfg = field(default_factory=LoggerCfg)
    dashboard: DashboardCfg = field(default_factory=DashboardCfg)
    tests: TestsCfg = field(default_factory=TestsCfg)

    # cache à plat pour l'accès rétro-compat
    _flat_cache: Dict[str, Any] = field(default_factory=dict, init=False, repr=False)
    _alias_map: Dict[str, str] = field(default_factory=dict, init=False, repr=False)
    # dans la dataclass BotConfig (ou équivalent)
    scanner_global_eval_hz: float | None = None

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
        Valeurs typiques : "EU_ONLY", "SPLIT", ...
        """
        val = getattr(self.g, "deployment_mode", "EU_ONLY")
        if not val:
            return "EU_ONLY"
        return str(val).upper()

        val = getattr(self.g, "deployment_mode", "EU_ONLY")
        if not val:
            return "EU_ONLY"
        return str(val).upper()

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


    # ------------- Construction -------------
    @staticmethod
    def from_env() -> "BotConfig":
        cfg = BotConfig()

        # --- Charger Globaux ---
        g = cfg.g
        g.deployment_mode = _Env.get("DEPLOYMENT_MODE", g.deployment_mode)
        g.pod_region = _Env.get("POD_REGION", g.pod_region)
        g.pacer_mode = _Env.get("PACER_MODE", g.pacer_mode)
        g.engine_pod_map = _Env.get_dict("ENGINE_POD_MAP", g.engine_pod_map)
        g.split_latency = _Env.get_dict("SPLIT_LATENCY", g.split_latency)
        g.capital_profile = _Env.get("CAPITAL_PROFILE", g.capital_profile)
        g.enabled_exchanges = _Env.get_list("ENABLED_EXCHANGES", g.enabled_exchanges)
        g.allowed_routes = _Env.get_routes("ALLOWED_ROUTES", g.allowed_routes)
        g.pair_universe_mode = _Env.get("PAIR_UNIVERSE_MODE", g.pair_universe_mode)
        g.pair_whitelist = _Env.get_list("PAIR_WHITELIST", g.pair_whitelist)
        g.pair_regex = _Env.get("PAIR_REGEX", g.pair_regex)

        g.primary_quote = _Env.get("PRIMARY_QUOTE", g.primary_quote)
        g.wallet_alias_by_quote = _Env.get_dict("WALLET_ALIAS_BY_QUOTE", g.wallet_alias_by_quote)
        g.min_notional_by_exchange_quote = _Env.get_dict("MIN_NOTIONAL_BY_EXCHANGE_QUOTE", g.min_notional_by_exchange_quote)

        g.enable_branches = _Env.get_dict("ENABLE_BRANCHES", g.enable_branches)
        g.branch_priority = _Env.get_list("BRANCH_PRIORITY", g.branch_priority)
        g.branch_budgets_quote = _Env.get_dict("BRANCH_BUDGETS_QUOTE", g.branch_budgets_quote)

        g.frontload_weights = [float(x) for x in _Env.get_list("FRONTLOAD_WEIGHTS", g.frontload_weights)]
        g.min_fragment_quote = {k: float(v) for k,v in _Env.get_dict("MIN_FRAGMENT_QUOTE", g.min_fragment_quote).items()}
        g.guards = _Env.get_dict("GUARDS", g.guards)
        g.vol_slip_ttl = _Env.get_dict("VOL_SLIP_TTL", g.vol_slip_ttl)

        g.ac = _Env.get_dict("AC_CONFIG", g.ac)
        g.mode = _Env.get("MODE", g.mode)
        # 👇 Ajoute ce bloc de compat
        if str(g.mode).upper() in ("DEV", "DEVELOPMENT"):
            g.mode = "DRY_RUN"
        g.feature_switches = _Env.get_dict("FEATURE_SWITCHES", g.feature_switches)

        # --- Router cfg ---------------------------------------------------
        cfg.router.coalesce_window_ms = _Env.get_int("ROUTER_COALESCE_WINDOW_MS", cfg.router.coalesce_window_ms)
        cfg.router.stale_ms = _Env.get_int("ROUTER_STALE_MS", cfg.router.stale_ms)
        # P0: shards_per_exchange = (BINANCE, BYBIT, COINBASE)
        cfg.router.shards_per_exchange = tuple(
            _Env.get_int("ROUTER_SHARDS_PER_EXCHANGE", cfg.router.shards_per_exchange) for _ in range(3)
        )
        # Quota global (fallback) + quotas stream-centrics optionnels
        cfg.router.out_queues_maxlen = _Env.get_int("ROUTER_OUT_QUEUES_MAXLEN", cfg.router.out_queues_maxlen)
        cfg.router.out_queues_maxlen_by_kind = _Env.get_dict(
            "ROUTER_OUT_QUEUES_MAXLEN_BY_KIND", cfg.router.out_queues_maxlen_by_kind
        )
        cfg.router.mux_poll_ms = _Env.get_int("ROUTER_MUX_POLL_MS", cfg.router.mux_poll_ms)
        cfg.router.require_l2_first = _Env.get_bool("ROUTER_REQUIRE_L2_FIRST", cfg.router.require_l2_first)
        cfg.router.drop_policy = _Env.get_dict("ROUTER_DROP_POLICY", cfg.router.drop_policy)
        cfg.router.deque_maxlen_per_ex = _Env.get_dict("ROUTER_DEQUE_MAXLEN_PER_EX", cfg.router.deque_maxlen_per_ex)
        cfg.router.backpressure = _Env.get_dict("ROUTER_BACKPRESSURE", cfg.router.backpressure)
        cfg.router.cb_coalesce_bump_ms = _Env.get_int(
            "ROUTER_CB_COALESCE_BUMP_MS", cfg.router.cb_coalesce_bump_ms
        )
        cfg.router.topic_max_hz = _Env.get_dict("ROUTER_TOPIC_MAX_HZ", cfg.router.topic_max_hz)


        cfg.ws_public.ws_backoff = _Env.get_dict("WS_BACKOFF", cfg.ws_public.ws_backoff)
        cfg.ws_public.connect_timeout_s = _Env.get_int("WS_CONNECT_TIMEOUT_S", cfg.ws_public.connect_timeout_s)
        cfg.ws_public.read_timeout_s = _Env.get_int("WS_READ_TIMEOUT_S", cfg.ws_public.read_timeout_s)
        # Budget journalier par stratégie (en quote, ex: {"TT": 1_000_000, "TM": 500_000})
        cfg.daily_strategy_budget_quote = _Env.get_dict("DAILY_STRATEGY_BUDGET_QUOTE", {}) or {}

        cfg.discovery.http_timeout_s = _Env.get_int("DISCOVERY_HTTP_TIMEOUT_S", cfg.discovery.http_timeout_s)
        cfg.discovery.retry_policy = _Env.get_dict("DISCOVERY_RETRY_POLICY", cfg.discovery.retry_policy)
        cfg.discovery.max_inflight_requests = _Env.get_int("DISCOVERY_MAX_INFLIGHT", cfg.discovery.max_inflight_requests)
        cfg.discovery.quotes_allowed = _Env.get_list("DISCOVERY_QUOTES_ALLOWED", cfg.discovery.quotes_allowed)
        cfg.discovery.min_24h_volume_usd = _Env.get_float("DISCOVERY_MIN_24H_VOL_USD", cfg.discovery.min_24h_volume_usd)
        # Alias compat pour l’ancienne clé (avec "VOLUME")
        cfg.discovery.min_24h_volume_usd = _Env.get_float(
            "DISCOVERY_MIN_24H_VOLUME_USD",
            cfg.discovery.min_24h_volume_usd
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
        # --- Discovery toggles & liste ---
        cfg.discovery.enabled = _Env.get_bool("DISCOVERY_ENABLED", cfg.discovery.enabled)
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
        pairs_csv = _Env.get("PAIRS", "").strip()
        if pairs_csv:
            cfg.g.pairs = [p.strip().upper() for p in pairs_csv.split(",") if p.strip()]
        # fallback: se PAIRS assente ma hai PAIR_WHITELIST popolata, usa quella
        if not getattr(cfg.g, "pairs", None) and getattr(cfg.g, "pair_whitelist", None):
            cfg.g.pairs = list(cfg.g.pair_whitelist)

        # --- RM caps richiesti dal RiskManager (root-level, non dentro cfg.rm) ---
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
            inv = 1500.0
        setattr(cfg, "inventory_cap_quote", float(inv))

        buf = _float_or_none("MIN_BUFFER_QUOTE")
        if buf is None:
            buf = _float_or_none("MIN_BUFFER_USD")
        if buf is None:
            buf = 0.0
        setattr(cfg, "min_buffer_quote", float(buf))

        # TTL balances par alias (OK / DEGRADED / BLOCKED)
        cfg.RM_BALANCE_TTL_S_NORMAL = _Env.get_float(
            "RM_BALANCE_TTL_S_NORMAL",
            getattr(cfg, "RM_BALANCE_TTL_S_NORMAL", 60.0),
        )
        cfg.RM_BALANCE_TTL_S_DEGRADED = _Env.get_float(
            "RM_BALANCE_TTL_S_DEGRADED",
            getattr(cfg, "RM_BALANCE_TTL_S_DEGRADED", 180.0),
        )
        cfg.RM_BALANCE_TTL_S_BLOCK = _Env.get_float(
            "RM_BALANCE_TTL_S_BLOCK",
            getattr(cfg, "RM_BALANCE_TTL_S_BLOCK", 600.0),
        )

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
        cfg.RM_PWS_FILL_LATENCY_CONSTRAINED_MS = _Env.get_float(
            "RM_PWS_FILL_LATENCY_CONSTRAINED_MS",
            getattr(cfg, "RM_PWS_FILL_LATENCY_CONSTRAINED_MS", 700.0),
        )
        cfg.RM_PWS_FILL_LATENCY_SEVERE_MS = _Env.get_float(
            "RM_PWS_FILL_LATENCY_SEVERE_MS",
            getattr(cfg, "RM_PWS_FILL_LATENCY_SEVERE_MS", 1200.0),
        )

        # Min % fee tokens utilisé par RM (fallback si non piloté via balances.*)
        cfg.RM_FEE_TOKEN_MIN_PCT = _Env.get_float(
            "RM_FEE_TOKEN_MIN_PCT",
            getattr(cfg, "RM_FEE_TOKEN_MIN_PCT", 5.0),
        )


        # --- Discovery : configuration centrale de l'univers -----------------
        # Volumes minimaux 24h (USD) et par quote
        cfg.discovery.min_24h_volume_usd = float(
            _Env.get("DISCOVERY_MIN_24H_VOLUME_USD", cfg.discovery.min_24h_volume_usd)
        )
        cfg.discovery.min_quote_volume_usdc = float(
            _Env.get(
                "DISCOVERY_MIN_QUOTE_VOLUME_USDC",
                cfg.discovery.min_quote_volume_usdc or cfg.discovery.min_24h_volume_usd,
            )
        )
        cfg.discovery.min_quote_volume_eur = float(
            _Env.get(
                "DISCOVERY_MIN_QUOTE_VOLUME_EUR",
                cfg.discovery.min_quote_volume_eur or cfg.discovery.min_24h_volume_usd,
            )
        )

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

        ex_enabled = _Env.get_list(
            "DISCOVERY_ENABLED_EXCHANGES",
            cfg.discovery.enabled_exchanges or cfg.g.enabled_exchanges,
        )
        if ex_enabled:
            cfg.discovery.enabled_exchanges = [e.upper() for e in ex_enabled]

        # Whitelists / blacklists paires
        cfg.discovery.whitelist = _Env.get_list("DISCOVERY_WHITELIST", cfg.discovery.whitelist)
        cfg.discovery.blacklist = _Env.get_list("DISCOVERY_BLACKLIST", cfg.discovery.blacklist)

        # --- Scanner cfg ------------------------------------------------------
        cfg.scanner.workers = _Env.get_int("SCANNER_WORKERS", cfg.scanner.workers)
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
        cfg.SCANNER_HZ = _Env.get_dict("SCANNER_HZ", getattr(cfg, "SCANNER_HZ", {}))
        cfg.SCANNER_DEQUE_MAX = _Env.get_dict("SCANNER_DEQUE_MAX", getattr(cfg, "SCANNER_DEQUE_MAX", {}))
        cfg.SCANNER_DEDUP_COOLDOWN_S = _Env.get_float(
            "SCANNER_DEDUP_COOLDOWN_S",
            getattr(cfg, "SCANNER_DEDUP_COOLDOWN_S", getattr(cfg.scanner, "dedup_cooldown_s", 0.16)),
        )
        cfg.SCANNER_GLOBAL_EVAL_HZ = _Env.get_float(
            "SCANNER_GLOBAL_EVAL_HZ",
            getattr(cfg, "SCANNER_GLOBAL_EVAL_HZ", getattr(cfg.scanner, "scanner_global_eval_hz", 200.0)),
        )

        sc = cfg.scanner

        # Exposer le cooldown de dedup aussi dans ScannerCfg
        try:
            sc.dedup_cooldown_s = float(cfg.SCANNER_DEDUP_COOLDOWN_S)
        except Exception:
            pass

        # Garder BotConfig.scanner_global_eval_hz, cfg.SCANNER_GLOBAL_EVAL_HZ
        # et ScannerCfg.scanner_global_eval_hz alignés
        try:
            v = float(cfg.SCANNER_GLOBAL_EVAL_HZ)
        except Exception:
            v = float(getattr(sc, "scanner_global_eval_hz", 200.0))
        sc.scanner_global_eval_hz = v
        cfg.scanner_global_eval_hz = v

        # Refléter SCANNER_HZ (legacy) dans ScannerCfg (P0 Marché Public)
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


        cfg.rm.enable_tt = _Env.get_bool("ENABLE_TT", cfg.rm.enable_tt)
        cfg.rm.enable_tm = _Env.get_bool("ENABLE_TM", cfg.rm.enable_tm)
        cfg.rm.enable_mm = _Env.get_bool("ENABLE_MM", cfg.rm.enable_mm)
        cfg.rm.enable_reb = _Env.get_bool("ENABLE_REB", cfg.rm.enable_reb)
        cfg.rm.enable_maker_maker = _Env.get_bool("ENABLE_MAKER_MAKER", cfg.rm.enable_maker_maker)
        cfg.rm.default_notional = _Env.get_float("RM_DEFAULT_NOTIONAL", cfg.rm.default_notional)
        cfg.rm.max_fragments = _Env.get_int("RM_MAX_FRAGMENTS", cfg.rm.max_fragments)
        cfg.rm.mm_alias_name = _Env.get("RM_MM_ALIAS_NAME", cfg.rm.mm_alias_name)
        cfg.rm.mm_depth_min_usd = _Env.get_float("RM_MM_DEPTH_MIN_USD", cfg.rm.mm_depth_min_usd)
        cfg.rm.mm_qpos_max_ahead_usd = _Env.get_float("RM_MM_QPOS_MAX_AHEAD_USD", cfg.rm.mm_qpos_max_ahead_usd)
        cfg.rm.mm_min_p_both = _Env.get_float("RM_MM_MIN_P_BOTH", cfg.rm.mm_min_p_both)
        cfg.rm.mm_min_net_bps = _Env.get_float("RM_MM_MIN_NET_BPS", cfg.rm.mm_min_net_bps)
        cfg.rm.mm_hedge_cost_bps = _Env.get_float("RM_MM_HEDGE_COST_BPS", cfg.rm.mm_hedge_cost_bps)
        cfg.rm.mm_vol_bps_max = _Env.get_float("RM_MM_VOL_BPS_MAX", cfg.rm.mm_vol_bps_max)
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
                getattr(cfg.rm, "tm_neutral_hedge_ratio", cfg.engine.tm_exposure_ttl_hedge_ratio),
            )
        neutral_hr = float(neutral_hr_env)

        # Propagation NEUTRAL (TTL hedge ratio) vers RM + Engine
        cfg.engine.tm_exposure_ttl_hedge_ratio = neutral_hr
        setattr(cfg.rm, "tm_exposure_ttl_hedge_ratio", neutral_hr)
        cfg.rm.tm_neutral_hedge_ratio = neutral_hr  # compat legacy

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

        # Ticket 10 — capacités techniques Engine (workers / inflight CEX par profil)
        cfg.engine.workers_by_profile = _Env.get_dict(
            "ENGINE_WORKERS_BY_PROFILE",
            cfg.engine.workers_by_profile,
        )
        cfg.engine.inflight_max_by_exchange_by_profile = _Env.get_dict(
            "ENGINE_INFLIGHT_MAX_BY_EXCHANGE_BY_PROFILE",
            cfg.engine.inflight_max_by_exchange_by_profile,
        )


        cfg.engine.tt_max_skew_ms = _Env.get_int("ENGINE_TT_MAX_SKEW_MS", cfg.engine.tt_max_skew_ms)
        cfg.engine.order_timeout_s = _Env.get_int("ENGINE_ORDER_TIMEOUT_S", cfg.engine.order_timeout_s)
        cfg.engine.tt_max_skew_ms = _Env.get_int("ENGINE_TT_MAX_SKEW_MS", cfg.engine.tt_max_skew_ms)
        cfg.engine.order_timeout_s = _Env.get_int("ENGINE_ORDER_TIMEOUT_S", cfg.engine.order_timeout_s)

        # Canonique TM — TTL d'exposition (ms)
        ttl_ms_global = _Env.get_int(
            "TM_EXPOSURE_TTL_MS",  # clé globale
            _Env.get_int("ENGINE_TM_EXPOSURE_TTL_MS", cfg.engine.tm_exposure_ttl_ms),  # alias Engine
        )
        cfg.engine.tm_exposure_ttl_ms = ttl_ms_global
        setattr(cfg.rm, "tm_exposure_ttl_ms", ttl_ms_global)

        cfg.engine.tm_queuepos_max_eta_ms = _Env.get_int("ENGINE_TM_QPOS_MAX_ETA_MS", cfg.engine.tm_queuepos_max_eta_ms)

        cfg.engine.tm_queuepos_max_eta_ms = _Env.get_int("ENGINE_TM_QPOS_MAX_ETA_MS", cfg.engine.tm_queuepos_max_eta_ms)

        cfg.engine.vol_soft_cap_bps = _Env.get_float("ENGINE_VOL_SOFT_CAP_BPS", cfg.engine.vol_soft_cap_bps)
        cfg.engine.vol_hard_cap_bps = _Env.get_float("ENGINE_VOL_HARD_CAP_BPS", cfg.engine.vol_hard_cap_bps)
        cfg.engine.freeze_tm_on_vol = _Env.get_bool("ENGINE_FREEZE_TM_ON_VOL", cfg.engine.freeze_tm_on_vol)
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

        # Ticket 10 — capacité technique Engine (workers / inflight par CEX)
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
        cfg.pws.PWS_PACER_EU = _Env.get("PWS_PACER_EU", cfg.pws.PWS_PACER_EU)
        cfg.pws.PWS_PACER_US = _Env.get("PWS_PACER_US", cfg.pws.PWS_PACER_US)
        cfg.pws.PWS_ALERT_PERIOD_S = _Env.get_int("PWS_ALERT_PERIOD_S", cfg.pws.PWS_ALERT_PERIOD_S)

        cfg.reconciler.RECO_ALERT_PERIOD_S = _Env.get_int("RECO_ALERT_PERIOD_S", cfg.reconciler.RECO_ALERT_PERIOD_S)

        cfg.rebal.rebal_quantum_min_quote = _Env.get_float("REBAL_QUANTUM_MIN_QUOTE", cfg.rebal.rebal_quantum_min_quote)
        cfg.rebal.rebal_max_ops_per_min = _Env.get_int("REBAL_MAX_OPS_PER_MIN", cfg.rebal.rebal_max_ops_per_min)
        cfg.rebal.rebal_priority = _Env.get_list("REBAL_PRIORITY", cfg.rebal.rebal_priority)
        cfg.rebal.rebal_hint_ttl_s = _Env.get_int("REBAL_HINT_TTL_S", cfg.rebal.rebal_hint_ttl_s)
        cfg.rebal.rebal_snapshots_missing_error_s = _Env.get_float("REBAL_SNAPSHOTS_MISSING_ERROR_S",
                                                                   cfg.rebal.rebal_snapshots_missing_error_s)
        cfg.rebal.rebal_snapshots_error_cooldown_s = _Env.get_float("REBAL_SNAPSHOTS_ERROR_COOLDOWN_S",
                                                                    cfg.rebal.rebal_snapshots_error_cooldown_s)
        cfg.rebal.rebal_quantum_quote_map = _Env.get_dict("REBAL_QUANTUM_QUOTE_MAP", cfg.rebal.rebal_quantum_quote_map)

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

        cfg.slip.ttl_s = _Env.get_int("SLIP_TTL_S", cfg.slip.ttl_s)  # alias historique
        cfg.vol.ttl_s = _Env.get_int("VOL_TTL_S", cfg.vol.ttl_s)  # alias historique
        cfg.rm.mm_ttl_ms = _Env.get_int("MM_TTL_MS", cfg.rm.mm_ttl_ms)
        cfg.vol.vm_size_factor_map = _Env.get_dict("VM_SIZE_FACTOR_MAP", cfg.vol.vm_size_factor_map)
        cfg.vol.vm_min_bps_boost_map = _Env.get_dict("VM_MIN_BPS_BOOST_MAP", cfg.vol.vm_min_bps_boost_map)
        cfg.vol.tm_neutral_hedge_ratio_map = _Env.get_dict("TM_NEUTRAL_HEDGE_RATIO_MAP",
                                                           cfg.vol.tm_neutral_hedge_ratio_map)
        cfg.vol.vm_prudence_thresholds_bps = _Env.get_dict("VM_PRUDENCE_THRESHOLDS_BPS",
                                                           cfg.vol.vm_prudence_thresholds_bps)
        cfg.vol.vm_maker_pad_ticks_map = _Env.get_dict("VM_MAKER_PAD_TICKS_MAP", cfg.vol.vm_maker_pad_ticks_map)
        cfg.slip.fee_sync_max_concurrency = _Env.get_int("FEE_SYNC_MAX_CONCURRENCY", cfg.slip.fee_sync_max_concurrency)
        cfg.slip.fee_sync_backoff_initial_s = _Env.get_float("FEE_SYNC_BACKOFF_INITIAL_S",
                                                             cfg.slip.fee_sync_backoff_initial_s)
        cfg.slip.fee_sync_backoff_max_s = _Env.get_float("FEE_SYNC_BACKOFF_MAX_S", cfg.slip.fee_sync_backoff_max_s)
        cfg.slip.fee_sync_max_retries = _Env.get_int("FEE_SYNC_MAX_RETRIES", cfg.slip.fee_sync_max_retries)
        cfg.slip.fee_sync_jitter_s = _Env.get_float("FEE_SYNC_JITTER_S", cfg.slip.fee_sync_jitter_s)
        cfg.slip.fee_reality_check_threshold_bps = _Env.get_float("FEE_REALITY_CHECK_THRESHOLD_BPS",
                                                                  cfg.slip.fee_reality_check_threshold_bps)

        # --- Rate Limiter (optionnel) ---
        cfg.rl.hard_caps_rps_by_exchange = _Env.get_dict("RL_HARD_CAPS_RPS", cfg.rl.hard_caps_rps_by_exchange)
        cfg.rl.hard_caps_rps_by_exchange_kind = _Env.get_dict("RL_HARD_CAPS_RPS_BY_EXCHANGE_KIND",
                                                              cfg.rl.hard_caps_rps_by_exchange_kind)
        cfg.rl.hard_caps_rps_by_kind = _Env.get_dict("RL_HARD_CAPS_RPS_BY_KIND", cfg.rl.hard_caps_rps_by_kind)
        cfg.rl.bursts_by_exchange = _Env.get_dict("RL_BURSTS", cfg.rl.bursts_by_exchange)
        cfg.rl.bursts_by_exchange_kind = _Env.get_dict("RL_BURSTS_BY_EXCHANGE_KIND", cfg.rl.bursts_by_exchange_kind)
        cfg.rl.bursts_by_kind = _Env.get_dict("RL_BURSTS_BY_KIND", cfg.rl.bursts_by_kind)
        cfg.rl.priorities = _Env.get_list("RL_PRIORITIES", cfg.rl.priorities)
        cfg.rl.fair = _Env.get_bool("RL_FAIR", cfg.rl.fair)
        cfg.rl.name_prefix = _Env.get("RL_NAME_PREFIX", cfg.rl.name_prefix)
        cfg.rl.min_sleep_s = _Env.get_float("RL_MIN_SLEEP_S", cfg.rl.min_sleep_s)
        cfg.rl.max_sleep_s = _Env.get_float("RL_MAX_SLEEP_S", cfg.rl.max_sleep_s)
        cfg.rl.default_rate_per_s = _Env.get_float("RL_DEFAULT_RATE_PER_S", cfg.rl.default_rate_per_s)
        cfg.rl.default_burst = _Env.get_int("RL_DEFAULT_BURST", cfg.rl.default_burst)
        # --- Rate Limiter global (déjà présent) ---
        cfg.rl.hard_caps_rps_by_exchange = _Env.get_dict(
            "RL_HARD_CAPS_RPS_BY_EXCHANGE",
            cfg.rl.hard_caps_rps_by_exchange,
        )
        cfg.rl.hard_caps_rps_by_exchange_kind = _Env.get_dict(
            "RL_HARD_CAPS_RPS_BY_EXCHANGE_KIND",
            cfg.rl.hard_caps_rps_by_exchange_kind,
        )
        cfg.rl.hard_caps_rps_by_kind = _Env.get_dict(
            "RL_HARD_CAPS_RPS_BY_KIND",
            cfg.rl.hard_caps_rps_by_kind,
        )
        cfg.rl.bursts_by_exchange = _Env.get_dict(
            "RL_BURSTS_BY_EXCHANGE",
            cfg.rl.bursts_by_exchange,
        )
        cfg.rl.bursts_by_exchange_kind = _Env.get_dict(
            "RL_BURSTS_BY_EXCHANGE_KIND",
            cfg.rl.bursts_by_exchange_kind,
        )
        cfg.rl.bursts_by_kind = _Env.get_dict(
            "RL_BURSTS_BY_KIND",
            cfg.rl.bursts_by_kind,
        )
        cfg.rl.default_rate_per_s = _Env.get_float(
            "RL_DEFAULT_RATE_PER_S",
            cfg.rl.default_rate_per_s,
        )
        cfg.rl.default_burst = _Env.get_float(
            "RL_DEFAULT_BURST",
            cfg.rl.default_burst,
        )

        # 🔹 Nouveau : configuration SC (sub-account) en JSON
        cfg.rl.sc = _Env.get_dict("RL_SC", cfg.rl.sc)



        # --- RPC Gateway (optionnel) ---
        cfg.rpc.enabled = _Env.get_bool("RPC_ENABLED", cfg.rpc.enabled)
        cfg.rpc.host = _Env.get("RPC_HOST", cfg.rpc.host)
        cfg.rpc.port = _Env.get_int("RPC_PORT", cfg.rpc.port)
        cfg.rpc.region = _Env.get("RPC_REGION", cfg.rpc.region)
        cfg.rpc.timeout_s = _Env.get_float("RPC_TIMEOUT_S", cfg.rpc.timeout_s)
        cfg.rpc.max_retries = _Env.get_int("RPC_MAX_RETRIES", cfg.rpc.max_retries)
        cfg.rpc.mtls_enabled = _Env.get_bool("RPC_MTLS_ENABLED", cfg.rpc.mtls_enabled)
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


        # --- Logger / LHM (optionnel) ---
        cfg.lhm.LHM_Q_STREAM_MAX = _Env.get_int("LHM_Q_STREAM_MAX", cfg.lhm.LHM_Q_STREAM_MAX)
        cfg.lhm.LHM_STREAM_BATCH = _Env.get_int("LHM_STREAM_BATCH", cfg.lhm.LHM_STREAM_BATCH)
        cfg.lhm.LHM_DROP_WHEN_FULL = _Env.get_bool("LHM_DROP_WHEN_FULL", cfg.lhm.LHM_DROP_WHEN_FULL)
        cfg.lhm.LHM_HIGH_WATERMARK_RATIO = _Env.get_float("LHM_HIGH_WATERMARK_RATIO", cfg.lhm.LHM_HIGH_WATERMARK_RATIO)
        cfg.lhm.LHM_MAX_QUEUE_PLATEAU_S = _Env.get_int("LHM_MAX_QUEUE_PLATEAU_S", cfg.lhm.LHM_MAX_QUEUE_PLATEAU_S)
        cfg.lhm.LHM_MM_SAMPLING_QUOTES = _Env.get_float("LHM_MM_SAMPLING_QUOTES", cfg.lhm.LHM_MM_SAMPLING_QUOTES)
        cfg.lhm.LHM_MM_SAMPLING_CANCELS = _Env.get_float("LHM_MM_SAMPLING_CANCELS", cfg.lhm.LHM_MM_SAMPLING_CANCELS)
        cfg.lhm.LHM_JSONL_QUEUE_CAP = _Env.get_int("LHM_JSONL_QUEUE_CAP", cfg.lhm.LHM_JSONL_QUEUE_CAP)
        cfg.lhm.LHM_JSONL_MAX_BYTES = _Env.get_int("LHM_JSONL_MAX_BYTES", cfg.lhm.LHM_JSONL_MAX_BYTES)
        cfg.lhm.LHM_JSONL_MAX_AGE_S = _Env.get_int("LHM_JSONL_MAX_AGE_S", cfg.lhm.LHM_JSONL_MAX_AGE_S)
        cfg.lhm.LHM_DISK_WARN_PCT = _Env.get_float("LHM_DISK_WARN_PCT", cfg.lhm.LHM_DISK_WARN_PCT)
        cfg.lhm.LHM_DISK_CRIT_PCT = _Env.get_float("LHM_DISK_CRIT_PCT", cfg.lhm.LHM_DISK_CRIT_PCT)
        cfg.lhm.LHM_DB_NAME = _Env.get("LHM_DB_NAME", cfg.lhm.LHM_DB_NAME)
        cfg.lhm.LHM_DB_MAX_BYTES = _Env.get_int("LHM_DB_MAX_BYTES", cfg.lhm.LHM_DB_MAX_BYTES)
        cfg.lhm.LHM_DB_MAX_AGE_S = _Env.get_int("LHM_DB_MAX_AGE_S", cfg.lhm.LHM_DB_MAX_AGE_S)
        cfg.lhm.LHM_WD_INTERVAL_S = _Env.get_float("LHM_WD_INTERVAL_S", cfg.lhm.LHM_WD_INTERVAL_S)
        cfg.lhm.LHM_WD_PLATEAU_WINDOW = _Env.get_int("LHM_WD_PLATEAU_WINDOW", cfg.lhm.LHM_WD_PLATEAU_WINDOW)
        cfg.lhm.LHM_WD_QUEUE_MIN_SIZE = _Env.get_int("LHM_WD_QUEUE_MIN_SIZE", cfg.lhm.LHM_WD_QUEUE_MIN_SIZE)
        cfg.lhm.LHM_WD_STALL_THRESHOLD_S = _Env.get_float("LHM_WD_STALL_THRESHOLD_S", cfg.lhm.LHM_WD_STALL_THRESHOLD_S)
        cfg.lhm.LHM_TRADE_BATCH_SIZE = _Env.get_int("LHM_TRADE_BATCH_SIZE", cfg.lhm.LHM_TRADE_BATCH_SIZE)
        cfg.lhm.LHM_TRADE_FLUSH_INTERVAL_S = _Env.get_float("LHM_TRADE_FLUSH_INTERVAL_S",
                                                            cfg.lhm.LHM_TRADE_FLUSH_INTERVAL_S)

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
        return cfg

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
            # Rien de spécial, latences homogènes
            pass
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
        # RM sizing
        self.rm.default_notional = max(self.rm.default_notional, p["default_notional"])
        self.rm.max_fragments = max(self.rm.max_fragments, p["inflight"])  # approximation sizing vs inflight
        # Engine queue/parallelism
        self.engine.tm_max_open_makers = max(self.engine.tm_max_open_makers, p["inflight"])
        # Router queues
        self.router.out_queues_maxlen = max(self.router.out_queues_maxlen, p["qlen"])
        # Min fragment par quote
        mf = dict(self.g.min_fragment_quote)
        for q in mf:
            mf[q] = max(mf[q], p["min_fragment"])
        self.g.min_fragment_quote = mf
        self.engine.min_fragment_quote = mf
        self.rm.min_fragment_quote = mf

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
            "enable_maker_maker": "rm.enable_maker_maker",
            "ENABLE_MAKER_MAKER": "rm.enable_maker_maker",
            "mm_alias_name": "rm.mm_alias_name",
            "MM_ALIAS_NAME": "rm.mm_alias_name",
            "preempt_mm_for_tt_tm": "rm.preempt_mm_for_tt_tm",
            "PREEMPT_MM_FOR_TT_TM": "rm.preempt_mm_for_tt_tm",
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
            "DEPLOYMENT_MODE": "g.deployment_mode",
            "POD_REGION": "g.pod_region",
            # RM inventory/buffer
            "inventory_cap_quote": "rm.inventory_cap_quote",
            "INVENTORY_CAP_QUOTE": "rm.inventory_cap_quote",
            "inventory_cap_usd": "rm.inventory_cap_quote",
            "INVENTORY_CAP_USD": "rm.inventory_cap_quote",
            "min_buffer_quote": "rm.min_buffer_quote",
            "MIN_BUFFER_QUOTE": "rm.min_buffer_quote",
            "min_buffer_usd": "rm.min_buffer_quote",
            "MIN_BUFFER_USD": "rm.min_buffer_quote",
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
            # Private WS Hub
            "PWS_PACER_EU": "pws.PWS_PACER_EU",
            "PWS_PACER_US": "pws.PWS_PACER_US",
            "PWS_ALERT_PERIOD_S": "pws.PWS_ALERT_PERIOD_S",
            # Balance fetcher pacers (string states)
            "BF_PACER_EU_STR": "balances.BF_PACER_EU",
            "BF_PACER_US_STR": "balances.BF_PACER_US",
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
            return self.get_by_dotpath(alias)
        raise AttributeError(item)



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
