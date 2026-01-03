# ENV_INDEX

Total keys: 631. Source unique: bot_config.py::BotConfig.from_env.

## Alias & precedence

- <expr>: ENABLE_MM, ENABLE_REB, ENABLE_TM, ENABLE_TT, SLIP_TTL_S, VOL_TTL_S
- _legacy_branches: ENABLE_MM, ENABLE_REB, ENABLE_TM, ENABLE_TT
- cfg.discovery.min_24h_volume_usd: DISCOVERY_MIN_24H_VOLUME_USD, DISCOVERY_MIN_24H_VOL_USD
- cfg.engine.pacer_init_ms: ENGINE_PACER_INIT, ENGINE_PACER_INIT_MS
- cfg.engine.pacer_jitter_ms: ENGINE_PACER_JITTER, ENGINE_PACER_JITTER_MS
- cfg.engine.pacer_max_ms: ENGINE_PACER_MAX, ENGINE_PACER_MAX_MS
- cfg.engine.pacer_min_ms: ENGINE_PACER_MIN, ENGINE_PACER_MIN_MS
- neutral_hr_env: TM_EXPOSURE_TTL_HEDGE_RATIO, TM_NEUTRAL_HEDGE_RATIO
- ttl_ms_global: ENGINE_TM_EXPOSURE_TTL_MS, TM_EXPOSURE_TTL_MS

## Violations (env usages hors BotConfig)

Aucune violation détectée.

## Index par clé

### AC_CONFIG
- Lecture: bot_config.py:L1785 (get_dict)
- Stockage: g.ac
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ALLOWED_ROUTES
- Lecture: bot_config.py:L1742 (get_routes)
- Stockage: g.allowed_routes
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### AUTOREFRESH_SEC
- Lecture: bot_config.py:L3512 (get_int)
- Stockage: cfg.dashboard.AUTOREFRESH_SEC
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### BALANCE_STALE_S
- Lecture: bot_config.py:L2087 (get_float)
- Stockage: cfg.wd.balance_stale_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### BF_PACER_EU
- Lecture: bot_config.py:L3310 (get)
- Stockage: cfg.balances.BF_PACER_EU
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### BF_PACER_US
- Lecture: bot_config.py:L3311 (get)
- Stockage: cfg.balances.BF_PACER_US
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### BF_REFRESH_INTERVAL_S
- Lecture: bot_config.py:L3301 (get_int)
- Stockage: cfg.balances.refresh_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### BF_TTL_CACHE_S
- Lecture: bot_config.py:L3302 (get_int)
- Stockage: cfg.balances.ttl_cache_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### BF_WALLET_TYPES
- Lecture: bot_config.py:L3309 (get_list)
- Stockage: cfg.balances.wallet_types
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### BINANCE_REST_BASE
- Lecture: bot_config.py:L3303 (get)
- Stockage: cfg.balances.binance_rest_base
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### BOOT_SCANNER_PROXY_BUFFER_MAXLEN
- Lecture: bot_config.py:L1849 (get_int)
- Stockage: cfg.boot.scanner_proxy_buffer_maxlen
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### BOOT_SCANNER_PROXY_MODE
- Lecture: bot_config.py:L1848 (get)
- Stockage: cfg.boot.scanner_proxy_mode
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### BRANCH_BUDGETS_QUOTE
- Lecture: bot_config.py:L1754 (get_dict)
- Stockage: g.branch_budgets_quote
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### BRANCH_PRIORITY
- Lecture: bot_config.py:L1753 (get_list)
- Stockage: g.branch_priority
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### BYBIT_API_BASE
- Lecture: bot_config.py:L3305 (get)
- Stockage: cfg.balances.bybit_api_base
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### CAPITAL_PROFILE
- Lecture: bot_config.py:L1740 (get)
- Stockage: g.capital_profile
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### COINBASE_API_BASE
- Lecture: bot_config.py:L3304 (get)
- Stockage: cfg.balances.coinbase_api_base
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### CONFIG_OVERRIDES
- Lecture: bot_config.py:L1877 (get)
- Stockage: overrides_raw
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DAILY_BUDGET_RESET_INTERVAL_S
- Lecture: bot_config.py:L2958 (get_float)
- Stockage: cfg.rm.daily_budget_reset_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DAILY_STRATEGY_BUDGET_QUOTE
- Lecture: bot_config.py:L2956 (get_dict), bot_config.py:L1980 (get_dict)
- Stockage: cfg.rm.daily_strategy_budget_quote
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DEFAULT_PRIVATE_WALLET
- Lecture: bot_config.py:L3306 (get)
- Stockage: cfg.balances.default_private_wallet
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DEMO_MODE
- Lecture: bot_config.py:L3513 (get_bool)
- Stockage: cfg.dashboard.DEMO_MODE
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DEPLOYMENT_MODE
- Lecture: bot_config.py:L1730 (get)
- Stockage: _deployment_mode_env
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DISCOVERY_BLACKLIST
- Lecture: bot_config.py:L2261 (get_list), bot_config.py:L2643 (get_list)
- Stockage: cfg.discovery.blacklist
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DISCOVERY_ENABLED
- Lecture: bot_config.py:L2259 (get_bool)
- Stockage: cfg.discovery.enabled
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DISCOVERY_ENABLED_EXCHANGES
- Lecture: bot_config.py:L2634 (get_list)
- Stockage: ex_enabled
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DISCOVERY_EUR_QUOTE_VOLUME_FACTOR
- Lecture: bot_config.py:L2617 (get)
- Stockage: cfg.discovery.eur_quote_volume_factor
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DISCOVERY_HTTP_TIMEOUT_S
- Lecture: bot_config.py:L1985 (get_int)
- Stockage: cfg.discovery.http_timeout_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DISCOVERY_MAX_INFLIGHT
- Lecture: bot_config.py:L2231 (get_int)
- Stockage: cfg.discovery.max_inflight_requests
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DISCOVERY_MIN_24H_VOLUME_USD
- Lecture: bot_config.py:L2244 (get_float)
- Stockage: cfg.discovery.min_24h_volume_usd
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DISCOVERY_MIN_24H_VOL_USD
- Lecture: bot_config.py:L2239 (get_float), bot_config.py:L2243 (get)
- Stockage: UNKNOWN, cfg.discovery.min_24h_volume_usd
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DISCOVERY_MIN_QUOTE_VOLUME_EUR
- Lecture: bot_config.py:L2254 (get_float), bot_config.py:L2264 (get_float)
- Stockage: cfg.discovery.min_quote_volume_eur, v_eur
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DISCOVERY_MIN_QUOTE_VOLUME_FLOOR
- Lecture: bot_config.py:L2623 (get)
- Stockage: cfg.discovery.min_quote_volume_floor
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DISCOVERY_MIN_QUOTE_VOLUME_USDC
- Lecture: bot_config.py:L2250 (get_float), bot_config.py:L2263 (get_float)
- Stockage: cfg.discovery.min_quote_volume_usdc, v_usdc
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DISCOVERY_QUOTES_ALLOWED
- Lecture: bot_config.py:L2232 (get_list), bot_config.py:L2630 (get_list)
- Stockage: cfg.discovery.quotes_allowed, q_allowed
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DISCOVERY_RETRY_POLICY
- Lecture: bot_config.py:L2230 (get_dict)
- Stockage: cfg.discovery.retry_policy
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### DISCOVERY_WHITELIST
- Lecture: bot_config.py:L2260 (get_list), bot_config.py:L2642 (get_list)
- Stockage: cfg.discovery.whitelist
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENABLED_EXCHANGES
- Lecture: bot_config.py:L1741 (get_list)
- Stockage: g.enabled_exchanges
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENABLE_BRANCHES
- Lecture: bot_config.py:L1751 (get), bot_config.py:L1752 (get_dict)
- Stockage: _enable_branches_env, g.enable_branches
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENABLE_JP
- Lecture: bot_config.py:L1734 (get_bool)
- Stockage: g.enable_jp
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENABLE_MAKER_MAKER
- Lecture: bot_config.py:L2832 (get_bool)
- Stockage: cfg.rm.enable_maker_maker
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENABLE_MM
- Lecture: bot_config.py:L2806 (get), bot_config.py:L2825 (get_bool)
- Stockage: <expr>, _legacy_branches
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENABLE_REB
- Lecture: bot_config.py:L2807 (get), bot_config.py:L2829 (get_bool)
- Stockage: <expr>, _legacy_branches
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENABLE_TM
- Lecture: bot_config.py:L2805 (get), bot_config.py:L2821 (get_bool)
- Stockage: <expr>, _legacy_branches
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENABLE_TT
- Lecture: bot_config.py:L2804 (get), bot_config.py:L2817 (get_bool)
- Stockage: <expr>, _legacy_branches
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENABLE_WS_LIVE
- Lecture: bot_config.py:L3516 (get_bool)
- Stockage: cfg.tests.ENABLE_WS_LIVE
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_BLOCKED_MS
- Lecture: bot_config.py:L2084 (get_int)
- Stockage: cfg.wd.engine_blocked_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_CIRCUIT_ESCALATION_WINDOW_S
- Lecture: bot_config.py:L3223 (get_float)
- Stockage: cfg.engine.circuit_escalation_window_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_CIRCUIT_MUTE_ESCALATION
- Lecture: bot_config.py:L3225 (get_float)
- Stockage: cfg.engine.circuit_mute_escalation
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_CIRCUIT_MUTE_MAX_S
- Lecture: bot_config.py:L3228 (get_float)
- Stockage: cfg.engine.circuit_mute_max_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_CIRCUIT_MUTE_MIN_S
- Lecture: bot_config.py:L3227 (get_float)
- Stockage: cfg.engine.circuit_mute_min_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_CIRCUIT_MUTE_S_MM
- Lecture: bot_config.py:L3230 (get_float)
- Stockage: cfg.engine.circuit_mute_s_mm
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_CIRCUIT_MUTE_S_TM
- Lecture: bot_config.py:L3229 (get_float)
- Stockage: cfg.engine.circuit_mute_s_tm
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_DEPTH_LEVELS_CHECK
- Lecture: bot_config.py:L3219 (get_int)
- Stockage: cfg.engine.depth_levels_check
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_DEPTH_MIN_QUOTE_MM
- Lecture: bot_config.py:L3218 (get_float)
- Stockage: cfg.engine.depth_min_quote_mm
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_DEPTH_MIN_QUOTE_TM
- Lecture: bot_config.py:L3217 (get_float)
- Stockage: cfg.engine.depth_min_quote_tm
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_DEPTH_MIN_QUOTE_TT
- Lecture: bot_config.py:L3216 (get_float)
- Stockage: cfg.engine.depth_min_quote_tt
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_ENFORCE_CLIENT_OID_DETERMINISTIC
- Lecture: bot_config.py:L3121 (get_bool)
- Stockage: cfg.engine.ff_enforce_client_oid_deterministic
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_FAIL_CLOSED_IDEMPOTENCE
- Lecture: bot_config.py:L3125 (get_bool)
- Stockage: cfg.engine.ff_fail_closed_idempotence
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_FAIL_CLOSED_ON_RM_OVERRIDE_EXCEPTION
- Lecture: bot_config.py:L3153 (get_bool)
- Stockage: cfg.engine.fail_closed_on_rm_override_exception
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_FF_ENFORCE_PREEMPTION
- Lecture: bot_config.py:L3133 (get_bool)
- Stockage: cfg.engine.ff_enforce_preemption
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_FF_HEDGE_FAST_LANE
- Lecture: bot_config.py:L3129 (get_bool)
- Stockage: cfg.engine.ff_hedge_fast_lane
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_FF_MM_ENABLED
- Lecture: bot_config.py:L3141 (get_bool)
- Stockage: cfg.engine.ff_mm_enabled
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_FF_MM_OPPORTUNISTIC_GATING_ENFORCED
- Lecture: bot_config.py:L3145 (get_bool)
- Stockage: cfg.engine.ff_mm_opportunistic_gating_enforced
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_FF_REB_ENABLED
- Lecture: bot_config.py:L3149 (get_bool)
- Stockage: cfg.engine.ff_reb_enabled
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_FF_TM_ENABLED
- Lecture: bot_config.py:L3137 (get_bool)
- Stockage: cfg.engine.ff_tm_enabled
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_FREEZE_TM_ON_VOL
- Lecture: bot_config.py:L3116 (get_bool)
- Stockage: cfg.engine.freeze_tm_on_vol
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_HTTP_TIMEOUT_S
- Lecture: bot_config.py:L3100 (get_float)
- Stockage: cfg.engine.http_timeout_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_IDEMPOTENCY_TTL_S
- Lecture: bot_config.py:L3117 (get_float)
- Stockage: cfg.engine.idempotency_ttl_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_INFLIGHT_MAX_BY_EXCHANGE
- Lecture: bot_config.py:L3237 (get_dict)
- Stockage: cfg.engine.inflight_max_by_exchange
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_INFLIGHT_MAX_BY_EXCHANGE_BY_PROFILE
- Lecture: bot_config.py:L3083 (get_dict)
- Stockage: cfg.engine.inflight_max_by_exchange_by_profile
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_INFLIGHT_RESERVED_CANCEL_BY_EXCHANGE_BY_PROFILE
- Lecture: bot_config.py:L3091 (get_dict)
- Stockage: cfg.engine.inflight_reserved_cancel_by_exchange_by_profile
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_INFLIGHT_RESERVED_HEDGE_BY_EXCHANGE_BY_PROFILE
- Lecture: bot_config.py:L3087 (get_dict)
- Stockage: cfg.engine.inflight_reserved_hedge_by_exchange_by_profile
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_ORDER_TIMEOUT_S
- Lecture: bot_config.py:L3097 (get_int), bot_config.py:L3099 (get_int)
- Stockage: cfg.engine.order_timeout_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_BAD_SCORE_THRESHOLD
- Lecture: bot_config.py:L3171 (get_float)
- Stockage: pacer_knobs.bad_score_threshold
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_DEESCALATE_GOOD_WINDOWS
- Lecture: bot_config.py:L3162 (get_int)
- Stockage: pacer_knobs.deescalate_good_windows
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_DEFAULT_TARGETS
- Lecture: bot_config.py:L3210 (get_dict)
- Stockage: targets_override
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_ESCALATE_BAD_WINDOWS
- Lecture: bot_config.py:L3159 (get_int)
- Stockage: pacer_knobs.escalate_bad_windows
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_GOOD_SCORE_THRESHOLD
- Lecture: bot_config.py:L3168 (get_float)
- Stockage: pacer_knobs.good_score_threshold
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_HOLD_TIME_FLAGS_SECS
- Lecture: bot_config.py:L3165 (get_float)
- Stockage: pacer_knobs.hold_time_flags_secs
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_INIT
- Lecture: bot_config.py:L3184 (get), bot_config.py:L3207 (get_int)
- Stockage: cfg.engine.pacer_init_ms, pacer_init_ms_legacy
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_INIT_MS
- Lecture: bot_config.py:L3180 (get), bot_config.py:L3206 (get_int)
- Stockage: cfg.engine.pacer_init_ms, pacer_init_ms_env
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_JITTER
- Lecture: bot_config.py:L3185 (get), bot_config.py:L3209 (get_int)
- Stockage: cfg.engine.pacer_jitter_ms, pacer_jitter_ms_legacy
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_JITTER_MS
- Lecture: bot_config.py:L3181 (get), bot_config.py:L3208 (get_int)
- Stockage: cfg.engine.pacer_jitter_ms, pacer_jitter_ms_env
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_MAX
- Lecture: bot_config.py:L3183 (get), bot_config.py:L3205 (get_int)
- Stockage: cfg.engine.pacer_max_ms, pacer_max_ms_legacy
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_MAX_MS
- Lecture: bot_config.py:L3179 (get), bot_config.py:L3204 (get_int)
- Stockage: cfg.engine.pacer_max_ms, pacer_max_ms_env
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_MIN
- Lecture: bot_config.py:L3182 (get), bot_config.py:L3203 (get_int)
- Stockage: cfg.engine.pacer_min_ms, pacer_min_ms_legacy
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_MIN_MS
- Lecture: bot_config.py:L3178 (get), bot_config.py:L3202 (get_int)
- Stockage: cfg.engine.pacer_min_ms, pacer_min_ms_env
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_TARGETS
- Lecture: bot_config.py:L2572 (get_dict)
- Stockage: pacer_targets_raw
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_WARMUP_SECS
- Lecture: bot_config.py:L3158 (get_float)
- Stockage: pacer_knobs.warmup_secs
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_WEIGHT_ACK
- Lecture: bot_config.py:L3174 (get_float)
- Stockage: pacer_knobs.weight_ack
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_WEIGHT_DRAIN
- Lecture: bot_config.py:L3177 (get_float)
- Stockage: pacer_knobs.weight_drain
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_WEIGHT_ERR
- Lecture: bot_config.py:L3176 (get_float)
- Stockage: pacer_knobs.weight_err
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PACER_WEIGHT_LAG
- Lecture: bot_config.py:L3175 (get_float)
- Stockage: pacer_knobs.weight_lag
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_POD_MAP
- Lecture: bot_config.py:L1736 (get_dict), bot_config.py:L3424 (get_dict)
- Stockage: g.engine_pod_map
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PRICE_BAND_BPS_CAP
- Lecture: bot_config.py:L3221 (get_float)
- Stockage: cfg.engine.price_band_bps_cap
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_PRICE_BAND_BPS_FLOOR
- Lecture: bot_config.py:L3220 (get_float)
- Stockage: cfg.engine.price_band_bps_floor
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_QUEUE_MAX
- Lecture: bot_config.py:L2082 (get_int)
- Stockage: cfg.engine.engine_queue_max
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_TM_EXPOSURE_TTL_MS
- Lecture: bot_config.py:L3105 (get_int)
- Stockage: ttl_ms_global
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_TM_QPOS_MAX_ETA_MS
- Lecture: bot_config.py:L3110 (get_int), bot_config.py:L3112 (get_int)
- Stockage: cfg.engine.tm_queuepos_max_eta_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_TT_MAX_SKEW_MS
- Lecture: bot_config.py:L3096 (get_int), bot_config.py:L3098 (get_int)
- Stockage: cfg.engine.tt_max_skew_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_VOL_HARD_CAP_BPS
- Lecture: bot_config.py:L3115 (get_float)
- Stockage: cfg.engine.vol_hard_cap_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_VOL_PRICE_BAND_K
- Lecture: bot_config.py:L3222 (get_float)
- Stockage: cfg.engine.vol_price_band_k
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_VOL_SOFT_CAP_BPS
- Lecture: bot_config.py:L3114 (get_float)
- Stockage: cfg.engine.vol_soft_cap_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ENGINE_WORKERS_BY_PROFILE
- Lecture: bot_config.py:L3079 (get_dict), bot_config.py:L3233 (get_dict)
- Stockage: cfg.engine.workers_by_profile
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### EU_LATENCY
- Lecture: bot_config.py:L1738 (get_dict)
- Stockage: g.eu_latency
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### EXCHANGE_REGION_MAP
- Lecture: bot_config.py:L3420 (get_dict)
- Stockage: g.exchange_region_map
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### EXPOSE_METRICS_ON_9110
- Lecture: bot_config.py:L1843 (get_bool)
- Stockage: cfg.obs.expose_metrics_on_status
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FEATURE_SWITCHES
- Lecture: bot_config.py:L1838 (get_dict)
- Stockage: g.feature_switches
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FEE_REALITY_CHECK_THRESHOLD_BPS
- Lecture: bot_config.py:L3345 (get_float)
- Stockage: cfg.slip.fee_reality_check_threshold_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FEE_SYNC_BACKOFF_INITIAL_S
- Lecture: bot_config.py:L3340 (get_float)
- Stockage: cfg.slip.fee_sync_backoff_initial_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FEE_SYNC_BACKOFF_MAX_S
- Lecture: bot_config.py:L3342 (get_float)
- Stockage: cfg.slip.fee_sync_backoff_max_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FEE_SYNC_JITTER_S
- Lecture: bot_config.py:L3344 (get_float)
- Stockage: cfg.slip.fee_sync_jitter_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FEE_SYNC_MAX_CONCURRENCY
- Lecture: bot_config.py:L3339 (get_int)
- Stockage: cfg.slip.fee_sync_max_concurrency
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FEE_SYNC_MAX_RETRIES
- Lecture: bot_config.py:L3343 (get_int)
- Stockage: cfg.slip.fee_sync_max_retries
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FF_ENFORCE_PREEMPTION
- Lecture: bot_config.py:L2841 (get_bool)
- Stockage: cfg.rm.ff_enforce_preemption
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FF_FAIL_CLOSED_LOGGING
- Lecture: bot_config.py:L3435 (get_bool)
- Stockage: cfg.lhm.ff_fail_closed_logging
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FF_HEDGE_FAST_LANE
- Lecture: bot_config.py:L2845 (get_bool)
- Stockage: cfg.rm.ff_hedge_fast_lane
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FF_LOGGING_CRITICAL_STREAMS
- Lecture: bot_config.py:L3436 (get_list)
- Stockage: cfg.lhm.ff_logging_critical_streams
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FF_MM_ENABLED
- Lecture: bot_config.py:L2853 (get_bool)
- Stockage: cfg.rm.ff_mm_enabled
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FF_MM_OPPORTUNISTIC_GATING_ENFORCED
- Lecture: bot_config.py:L2857 (get_bool)
- Stockage: cfg.rm.ff_mm_opportunistic_gating_enforced
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FF_REB_ENABLED
- Lecture: bot_config.py:L2861 (get_bool)
- Stockage: cfg.rm.ff_reb_enabled
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FF_TM_ENABLED
- Lecture: bot_config.py:L2849 (get_bool)
- Stockage: cfg.rm.ff_tm_enabled
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FF_TRADING_STATE_UNIFIED
- Lecture: bot_config.py:L2837 (get_bool)
- Stockage: cfg.rm.ff_trading_state_unified
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FF_TRUTH_FAIL_CLOSED
- Lecture: bot_config.py:L3444 (get_bool)
- Stockage: cfg.lhm.ff_truth_fail_closed
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FF_TRUTH_MODEL_ENABLED
- Lecture: bot_config.py:L3440 (get_bool)
- Stockage: cfg.lhm.ff_truth_model_enabled
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FLOW_SAFETY_PROFILES
- Lecture: bot_config.py:L2606 (get_dict)
- Stockage: cfg.flow_safety.profiles
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### FRONTLOAD_WEIGHTS
- Lecture: bot_config.py:L1756 (get_list)
- Stockage: g.frontload_weights
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### GLOBAL_KILL_SWITCH
- Lecture: bot_config.py:L2955 (get_bool)
- Stockage: cfg.rm.global_kill_switch
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### GUARDS
- Lecture: bot_config.py:L1758 (get_dict)
- Stockage: g.guards
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### JP_LATENCY
- Lecture: bot_config.py:L1739 (get_dict)
- Stockage: g.jp_latency
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_DB_LANE_BATCH_MAX
- Lecture: bot_config.py:L3466 (get_int)
- Stockage: cfg.lhm.LHM_DB_LANE_BATCH_MAX
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_DB_LANE_DRAIN_MS
- Lecture: bot_config.py:L3467 (get_float)
- Stockage: cfg.lhm.LHM_DB_LANE_DRAIN_MS
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_DB_LANE_DROP_WHEN_FULL
- Lecture: bot_config.py:L3468 (get_bool)
- Stockage: cfg.lhm.LHM_DB_LANE_DROP_WHEN_FULL
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_DB_LANE_ENABLED
- Lecture: bot_config.py:L3464 (get_bool)
- Stockage: cfg.lhm.LHM_DB_LANE_ENABLED
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_DB_LANE_Q_MAX
- Lecture: bot_config.py:L3465 (get_int)
- Stockage: cfg.lhm.LHM_DB_LANE_Q_MAX
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_DB_MAX_AGE_S
- Lecture: bot_config.py:L3463 (get_int)
- Stockage: cfg.lhm.LHM_DB_MAX_AGE_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_DB_MAX_BYTES
- Lecture: bot_config.py:L3462 (get_int)
- Stockage: cfg.lhm.LHM_DB_MAX_BYTES
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_DB_NAME
- Lecture: bot_config.py:L3461 (get)
- Stockage: cfg.lhm.LHM_DB_NAME
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_DISK_CRIT_PCT
- Lecture: bot_config.py:L3457 (get_float)
- Stockage: cfg.lhm.LHM_DISK_CRIT_PCT
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_DISK_WARN_PCT
- Lecture: bot_config.py:L3456 (get_float)
- Stockage: cfg.lhm.LHM_DISK_WARN_PCT
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_DROP_LOG_SAMPLE_RATE
- Lecture: bot_config.py:L3450 (get_float)
- Stockage: cfg.lhm.LHM_DROP_LOG_SAMPLE_RATE
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_DROP_WHEN_FULL
- Lecture: bot_config.py:L3432 (get_bool)
- Stockage: cfg.lhm.LHM_DROP_WHEN_FULL
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_HIGH_WATERMARK_RATIO
- Lecture: bot_config.py:L3433 (get_float)
- Stockage: cfg.lhm.LHM_HIGH_WATERMARK_RATIO
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_JSONL_MAX_AGE_S
- Lecture: bot_config.py:L3455 (get_int)
- Stockage: cfg.lhm.LHM_JSONL_MAX_AGE_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_JSONL_MAX_BYTES
- Lecture: bot_config.py:L3454 (get_int)
- Stockage: cfg.lhm.LHM_JSONL_MAX_BYTES
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_JSONL_QUEUE_CAP
- Lecture: bot_config.py:L3453 (get_int)
- Stockage: cfg.lhm.LHM_JSONL_QUEUE_CAP
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_MAX_QUEUE_PLATEAU_S
- Lecture: bot_config.py:L3434 (get_int)
- Stockage: cfg.lhm.LHM_MAX_QUEUE_PLATEAU_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_MM_SAMPLING_CANCELS
- Lecture: bot_config.py:L3449 (get_float)
- Stockage: cfg.lhm.LHM_MM_SAMPLING_CANCELS
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_MM_SAMPLING_QUOTES
- Lecture: bot_config.py:L3448 (get_float)
- Stockage: cfg.lhm.LHM_MM_SAMPLING_QUOTES
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_OPPORTUNITY_DROP_WHEN_FULL
- Lecture: bot_config.py:L3489 (get_bool)
- Stockage: cfg.lhm.LHM_OPPORTUNITY_DROP_WHEN_FULL
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_OPPORTUNITY_QUEUE_MAX
- Lecture: bot_config.py:L3486 (get_int)
- Stockage: cfg.lhm.LHM_OPPORTUNITY_QUEUE_MAX
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_OUT_DIR
- Lecture: bot_config.py:L3428 (get)
- Stockage: cfg.lhm.out_dir
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_PNL_RECO_DEFAULT_REGION
- Lecture: bot_config.py:L3502 (get)
- Stockage: cfg.lhm.LHM_PNL_RECO_DEFAULT_REGION
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_PNL_RECO_ENABLED
- Lecture: bot_config.py:L3492 (get_bool)
- Stockage: cfg.lhm.LHM_PNL_RECO_ENABLED
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_PNL_RECO_MAX_LOOKBACK_DAYS
- Lecture: bot_config.py:L3499 (get_int)
- Stockage: cfg.lhm.LHM_PNL_RECO_MAX_LOOKBACK_DAYS
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_PNL_RECO_TOL_ABS_QUOTE
- Lecture: bot_config.py:L3493 (get_float)
- Stockage: cfg.lhm.LHM_PNL_RECO_TOL_ABS_QUOTE
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_PNL_RECO_TOL_PCT
- Lecture: bot_config.py:L3496 (get_float)
- Stockage: cfg.lhm.LHM_PNL_RECO_TOL_PCT
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_Q_STREAM_MAX
- Lecture: bot_config.py:L3430 (get_int)
- Stockage: cfg.lhm.LHM_Q_STREAM_MAX
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_SLO_DROPPED_TRADES_BUDGET
- Lecture: bot_config.py:L3484 (get_float)
- Stockage: cfg.lhm.LHM_SLO_DROPPED_TRADES_BUDGET
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_SLO_LAG_SECONDS_MAX_TARGET
- Lecture: bot_config.py:L3482 (get_float)
- Stockage: cfg.lhm.LHM_SLO_PIPELINE_LAG_MAX_SECONDS_TARGET
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_SLO_QUEUE_DEPTH_MAX_TARGET
- Lecture: bot_config.py:L3480 (get_float)
- Stockage: cfg.lhm.LHM_SLO_QUEUE_DEPTH_MAX_TARGET
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_SLO_WRITE_MS_P95_TARGET
- Lecture: bot_config.py:L3478 (get_float)
- Stockage: cfg.lhm.LHM_SLO_WRITE_MS_P95_TARGET
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_STORAGE_ALERT_COOLDOWN_S
- Lecture: bot_config.py:L3458 (get_float)
- Stockage: cfg.lhm.LHM_STORAGE_ALERT_COOLDOWN_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_STORAGE_PATH
- Lecture: bot_config.py:L3508 (get)
- Stockage: cfg.lhm.storage_path
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_STREAM_BATCH
- Lecture: bot_config.py:L3431 (get_int)
- Stockage: cfg.lhm.LHM_STREAM_BATCH
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_TRADE_BATCH_SIZE
- Lecture: bot_config.py:L3475 (get_int)
- Stockage: cfg.lhm.LHM_TRADE_BATCH_SIZE
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_TRADE_FLUSH_INTERVAL_S
- Lecture: bot_config.py:L3476 (get_float)
- Stockage: cfg.lhm.LHM_TRADE_FLUSH_INTERVAL_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_WD_INTERVAL_S
- Lecture: bot_config.py:L3471 (get_float)
- Stockage: cfg.lhm.LHM_WD_INTERVAL_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_WD_PLATEAU_WINDOW
- Lecture: bot_config.py:L3472 (get_int)
- Stockage: cfg.lhm.LHM_WD_PLATEAU_WINDOW
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_WD_QUEUE_MIN_SIZE
- Lecture: bot_config.py:L3473 (get_int)
- Stockage: cfg.lhm.LHM_WD_QUEUE_MIN_SIZE
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LHM_WD_STALL_THRESHOLD_S
- Lecture: bot_config.py:L3474 (get_float)
- Stockage: cfg.lhm.LHM_WD_STALL_THRESHOLD_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LIVE_TRADING_ARMED
- Lecture: bot_config.py:L1787 (get_bool)
- Stockage: g.live_trading_armed
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### LOG_LEVEL
- Lecture: bot_config.py:L1841 (get)
- Stockage: cfg.obs.log_level
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### MIN_FRAGMENT_QUOTE
- Lecture: bot_config.py:L1757 (get_dict)
- Stockage: g.min_fragment_quote
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### MIN_NOTIONAL_BY_EXCHANGE_QUOTE
- Lecture: bot_config.py:L1750 (get_dict)
- Stockage: g.min_notional_by_exchange_quote
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### MIN_USDC
- Lecture: bot_config.py:L1748 (get_float)
- Stockage: g.min_usdc
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### MM_TTL_MS
- Lecture: bot_config.py:L3326 (get_int)
- Stockage: cfg.rm.mm_ttl_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### MODE
- Lecture: bot_config.py:L1786 (get)
- Stockage: g.mode
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### OBS_BASE_URL
- Lecture: bot_config.py:L3511 (get)
- Stockage: cfg.dashboard.OBS_BASE_URL
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### OBS_ENABLE_9108
- Lecture: bot_config.py:L1846 (get_bool)
- Stockage: cfg.obs.enable_obs_port
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### OBS_PORT
- Lecture: bot_config.py:L1847 (get_int)
- Stockage: cfg.obs.obs_port
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### OPS_RETENTION_DAYS
- Lecture: bot_config.py:L3505 (get_int)
- Stockage: cfg.lhm.OPS_RETENTION_DAYS
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PACER_MODE
- Lecture: bot_config.py:L1735 (get)
- Stockage: g.pacer_mode
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PAIRS
- Lecture: bot_config.py:L2272 (get_list)
- Stockage: pairs_list
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PAIR_REGEX
- Lecture: bot_config.py:L1745 (get)
- Stockage: g.pair_regex
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PAIR_UNIVERSE_MODE
- Lecture: bot_config.py:L1743 (get)
- Stockage: g.pair_universe_mode
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PAIR_WHITELIST
- Lecture: bot_config.py:L1744 (get_list)
- Stockage: g.pair_whitelist
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PER_STRATEGY_NOTIONAL_CAP
- Lecture: bot_config.py:L2962 (get_dict)
- Stockage: cfg.rm.per_strategy_notional_cap
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### POD_REGION
- Lecture: bot_config.py:L1728 (get)
- Stockage: _pod_region_env
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PREEMPT_MM_FOR_TT_TM
- Lecture: bot_config.py:L2961 (get_bool)
- Stockage: cfg.rm.preempt_mm_for_tt_tm
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PRIMARY_QUOTE
- Lecture: bot_config.py:L1747 (get)
- Stockage: g.primary_quote
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PRIVATE_WS_STALE_MS
- Lecture: bot_config.py:L2040 (get_int)
- Stockage: cfg.wd.private_ws_stale_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_ALERT_PERIOD_S
- Lecture: bot_config.py:L3276 (get_int)
- Stockage: cfg.pws.PWS_ALERT_PERIOD_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_BACKOFF_BASE_MS
- Lecture: bot_config.py:L3266 (get_int)
- Stockage: cfg.pws.PWS_BACKOFF_BASE_MS
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_BACKOFF_MAX_MS
- Lecture: bot_config.py:L3267 (get_int)
- Stockage: cfg.pws.PWS_BACKOFF_MAX_MS
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_BYBIT_WS_PRIVATE_URL
- Lecture: bot_config.py:L3271 (get)
- Stockage: cfg.pws.bybit_ws_private_url
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_CB_PRIVATE_POLL_INTERVAL_S
- Lecture: bot_config.py:L3268 (get_int)
- Stockage: cfg.pws.cb_private_poll_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_DISABLE_AUTO_WIRING_PROD
- Lecture: bot_config.py:L3256 (get_bool)
- Stockage: cfg.pws.ff_pws_disable_auto_wiring_prod
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_DROP_POLICY
- Lecture: bot_config.py:L3260 (get)
- Stockage: cfg.pws.pws_drop_policy
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_HEARTBEAT_MAX_GAP_S
- Lecture: bot_config.py:L3263 (get_int)
- Stockage: cfg.pws.PWS_HEARTBEAT_MAX_GAP_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_JITTER_MS
- Lecture: bot_config.py:L3265 (get_int)
- Stockage: cfg.pws.PWS_JITTER_MS
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_NO_DROP_CRITICAL_ENFORCED
- Lecture: bot_config.py:L3249 (get_bool)
- Stockage: cfg.pws.ff_pws_no_drop_critical_enforced
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_PACER_EU
- Lecture: bot_config.py:L3274 (get)
- Stockage: cfg.pws.PWS_PACER_EU
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_PACER_US
- Lecture: bot_config.py:L3275 (get)
- Stockage: cfg.pws.PWS_PACER_US
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_PING_INTERVAL_S
- Lecture: bot_config.py:L3261 (get_int)
- Stockage: cfg.pws.PWS_PING_INTERVAL_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_PONG_TIMEOUT_S
- Lecture: bot_config.py:L3262 (get_int)
- Stockage: cfg.pws.PWS_PONG_TIMEOUT_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_POOL_SIZE_EU
- Lecture: bot_config.py:L3243 (get_int)
- Stockage: cfg.pws.PWS_POOL_SIZE_EU
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_POOL_SIZE_US
- Lecture: bot_config.py:L3244 (get_int)
- Stockage: cfg.pws.PWS_POOL_SIZE_US
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_QUEUE_MAXLEN
- Lecture: bot_config.py:L3245 (get_int)
- Stockage: cfg.pws.PWS_QUEUE_MAXLEN
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_QUEUE_SATURATION_RATIO
- Lecture: bot_config.py:L3246 (get_float)
- Stockage: cfg.pws.PWS_QUEUE_SATURATION_RATIO
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_STABLE_RESET_S
- Lecture: bot_config.py:L3264 (get_int)
- Stockage: cfg.pws.PWS_STABLE_RESET_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### PWS_STRICT_DEDUP_ENFORCED
- Lecture: bot_config.py:L1780 (get_bool), bot_config.py:L3253 (get_bool)
- Stockage: cfg.pws.ff_pws_strict_dedup_enforced
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### REBAL_ALLOW_LOSS_BPS
- Lecture: bot_config.py:L2973 (get_float)
- Stockage: cfg.rm.rebal_allow_loss_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### REBAL_HINT_TTL_S
- Lecture: bot_config.py:L3286 (get_int)
- Stockage: cfg.rebal.rebal_hint_ttl_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### REBAL_INTERNAL_TRANSFER_THRESHOLD
- Lecture: bot_config.py:L3287 (get_float)
- Stockage: cfg.rebal.rebal_internal_transfer_threshold
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### REBAL_MAX_OPS_PER_MIN
- Lecture: bot_config.py:L3281 (get_int)
- Stockage: cfg.rebal.rebal_max_ops_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### REBAL_OPS_PER_MIN_RATIO
- Lecture: bot_config.py:L3282 (get_float)
- Stockage: cfg.rebal.rebal_ops_per_min_ratio
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### REBAL_PRIORITY
- Lecture: bot_config.py:L3285 (get_list)
- Stockage: cfg.rebal.rebal_priority
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### REBAL_QUANTUM_MIN_QUOTE
- Lecture: bot_config.py:L3280 (get_float)
- Stockage: cfg.rebal.rebal_quantum_min_quote
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### REBAL_QUANTUM_QUOTE_MAP
- Lecture: bot_config.py:L3299 (get_dict)
- Stockage: cfg.rebal.rebal_quantum_quote_map
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### REBAL_SLOT_TTL_S
- Lecture: bot_config.py:L3291 (get_float)
- Stockage: cfg.rebal.rebal_slot_ttl_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### REBAL_SNAPSHOTS_ERROR_COOLDOWN_S
- Lecture: bot_config.py:L3297 (get_float)
- Stockage: cfg.rebal.rebal_snapshots_error_cooldown_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### REBAL_SNAPSHOTS_MISSING_ERROR_S
- Lecture: bot_config.py:L3295 (get_float)
- Stockage: cfg.rebal.rebal_snapshots_missing_error_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### REBAL_VOLUME_HAIRCUT
- Lecture: bot_config.py:L2974 (get_float)
- Stockage: cfg.rm.rebal_volume_haircut
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RECO_ALERT_PERIOD_S
- Lecture: bot_config.py:L3278 (get_int)
- Stockage: cfg.reconciler.RECO_ALERT_PERIOD_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### REGION
- Lecture: bot_config.py:L1729 (get)
- Stockage: _region_alias_env
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RESTART_MODE
- Lecture: bot_config.py:L1788 (get)
- Stockage: g.restart_mode
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RESTART_WEBHOOK_HMAC_KEY
- Lecture: bot_config.py:L1873 (get)
- Stockage: cfg.alerting.webhook.hmac_secret
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RESTART_WEBHOOK_TIMEOUT_S
- Lecture: bot_config.py:L1876 (get_float)
- Stockage: cfg.alerting.webhook.timeout_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RESTART_WEBHOOK_URL
- Lecture: bot_config.py:L1872 (get)
- Stockage: cfg.alerting.webhook.url
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RL_BURSTS
- Lecture: bot_config.py:L3365 (get_dict)
- Stockage: rl_bursts_ex_legacy
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RL_BURSTS_BY_EXCHANGE
- Lecture: bot_config.py:L3364 (get_dict)
- Stockage: rl_bursts_ex_by
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RL_BURSTS_BY_EXCHANGE_KIND
- Lecture: bot_config.py:L3372 (get_dict)
- Stockage: cfg.rl.bursts_by_exchange_kind
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RL_BURSTS_BY_KIND
- Lecture: bot_config.py:L3376 (get_dict)
- Stockage: cfg.rl.bursts_by_kind
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RL_DEFAULT_BURST
- Lecture: bot_config.py:L3383 (get_int)
- Stockage: cfg.rl.default_burst
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RL_DEFAULT_RATE_PER_S
- Lecture: bot_config.py:L3382 (get_float)
- Stockage: cfg.rl.default_rate_per_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RL_FAIR
- Lecture: bot_config.py:L3378 (get_bool)
- Stockage: cfg.rl.fair
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RL_HARD_CAPS_RPS
- Lecture: bot_config.py:L3350 (get_dict)
- Stockage: rl_caps_ex_legacy
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RL_HARD_CAPS_RPS_BY_EXCHANGE
- Lecture: bot_config.py:L3349 (get_dict)
- Stockage: rl_caps_ex_by
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RL_HARD_CAPS_RPS_BY_EXCHANGE_KIND
- Lecture: bot_config.py:L3359 (get_dict)
- Stockage: cfg.rl.hard_caps_rps_by_exchange_kind
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RL_HARD_CAPS_RPS_BY_KIND
- Lecture: bot_config.py:L3363 (get_dict)
- Stockage: cfg.rl.hard_caps_rps_by_kind
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RL_MAX_SLEEP_S
- Lecture: bot_config.py:L3381 (get_float)
- Stockage: cfg.rl.max_sleep_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RL_MIN_SLEEP_S
- Lecture: bot_config.py:L3380 (get_float)
- Stockage: cfg.rl.min_sleep_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RL_NAME_PREFIX
- Lecture: bot_config.py:L3379 (get)
- Stockage: cfg.rl.name_prefix
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RL_PRIORITIES
- Lecture: bot_config.py:L3377 (get_list)
- Stockage: cfg.rl.priorities
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RL_SC
- Lecture: bot_config.py:L3386 (get_dict)
- Stockage: cfg.rl.sc
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_BALANCE_TTL_S_BLOCK
- Lecture: bot_config.py:L2318 (get_float)
- Stockage: cfg.rm.balance_ttl_s_block
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_BALANCE_TTL_S_DEGRADED
- Lecture: bot_config.py:L2314 (get_float)
- Stockage: cfg.rm.balance_ttl_s_degraded
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_BALANCE_TTL_S_NORMAL
- Lecture: bot_config.py:L2310 (get_float)
- Stockage: cfg.rm.balance_ttl_s_normal
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_BALANCE_UNKNOWN_POLICY
- Lecture: bot_config.py:L2322 (get)
- Stockage: policy_raw
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_BASE_MIN_BPS
- Lecture: bot_config.py:L2434 (get_float)
- Stockage: cfg.rm.base_min_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_BRANCH_BUDGETS_QUOTE
- Lecture: bot_config.py:L2871 (get_dict)
- Stockage: cfg.rm.branch_budgets_quote
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_BRANCH_PRIORITY
- Lecture: bot_config.py:L2867 (get_list)
- Stockage: cfg.rm.branch_priority
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_CAPITAL_LADDER_CFG
- Lecture: bot_config.py:L3021 (get_dict)
- Stockage: cfg.rm.capital_ladder_cfg
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_CAPS_TRADING_BY_PROFILE
- Lecture: bot_config.py:L3013 (get_dict)
- Stockage: cfg.rm.caps_trading_by_profile
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_COLLAT_ALIAS_OVERRIDES
- Lecture: bot_config.py:L2895 (get_dict)
- Stockage: cfg.rm.collat_alias_overrides
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_COLLAT_DEFAULT_MIN_USD
- Lecture: bot_config.py:L2892 (get_float)
- Stockage: cfg.rm.collat_default_min_usd
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_COLLAT_QUOTES
- Lecture: bot_config.py:L2904 (get_list), bot_config.py:L2943 (get)
- Stockage: cfg.rm.collat_quotes, collat_q_env
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_COLLAT_RATIO_CRIT
- Lecture: bot_config.py:L2901 (get_float)
- Stockage: cfg.rm.collat_ratio_crit
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_COLLAT_RATIO_LOW
- Lecture: bot_config.py:L2908 (get_float)
- Stockage: cfg.rm.collat_ratio_low
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_COLLAT_RATIO_WARN
- Lecture: bot_config.py:L2898 (get_float)
- Stockage: cfg.rm.collat_ratio_warn
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_COMBO_CAP_USD_BY_PROFILE
- Lecture: bot_config.py:L2964 (get_dict)
- Stockage: cfg.rm.combo_cap_usd_by_profile
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_COMBO_TTL_DEGRADED_FACTOR
- Lecture: bot_config.py:L2968 (get_float)
- Stockage: cfg.rm.combo_ttl_degraded_factor
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_CONSTR_CAP_FACTOR
- Lecture: bot_config.py:L2414 (get_float)
- Stockage: cfg.RM_CONSTR_CAP_FACTOR
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_CONSTR_IOC_ONLY
- Lecture: bot_config.py:L2418 (get_bool)
- Stockage: cfg.RM_CONSTR_IOC_ONLY
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_CONSTR_MM_ENABLE
- Lecture: bot_config.py:L2505 (get_bool)
- Stockage: rm_knobs.constr_mm_enable
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_CONSTR_TM_MIN_BPS_DELTA
- Lecture: bot_config.py:L2410 (get_float)
- Stockage: cfg.RM_CONSTR_TM_MIN_BPS_DELTA
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_CONSTR_TT_MIN_BPS_DELTA
- Lecture: bot_config.py:L2406 (get_float)
- Stockage: cfg.RM_CONSTR_TT_MIN_BPS_DELTA
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_CRITICAL_ALIASES
- Lecture: bot_config.py:L2356 (get_list)
- Stockage: crit_aliases
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_DECISION_LOG_PATH
- Lecture: bot_config.py:L2960 (get)
- Stockage: cfg.rm.decision_log_path
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_DEFAULT_NOTIONAL
- Lecture: bot_config.py:L2883 (get_float)
- Stockage: cfg.rm.default_notional
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_DYNAMIC_K
- Lecture: bot_config.py:L2435 (get_float)
- Stockage: cfg.rm.dynamic_k
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_FALLBACK_ON_TICK_EXCEPTION
- Lecture: bot_config.py:L2570 (get)
- Stockage: cfg.rm.fallback_on_tick_exception
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_FEE_LOW_MIN_SECONDS
- Lecture: bot_config.py:L2365 (get_float)
- Stockage: cfg.RM_FEE_LOW_MIN_SECONDS
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_FEE_TOKEN_MIN_PCT
- Lecture: bot_config.py:L2456 (get_float)
- Stockage: cfg.RM_FEE_TOKEN_MIN_PCT
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_FF_FAIL_CLOSED_CAPS
- Lecture: bot_config.py:L2833 (get_bool)
- Stockage: cfg.rm.ff_fail_closed_caps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_INFLIGHT_REBAL_BY_PROFILE
- Lecture: bot_config.py:L3017 (get_dict)
- Stockage: cfg.rm.inflight_rebal_by_profile
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_INFLIGHT_TRADING_BY_PROFILE
- Lecture: bot_config.py:L3009 (get_dict)
- Stockage: cfg.rm.inflight_trading_by_profile
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_INVARIANT_STRICT
- Lecture: bot_config.py:L2422 (get_bool)
- Stockage: cfg.RM_INVARIANT_STRICT
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_MAX_FRAGMENTS
- Lecture: bot_config.py:L2884 (get_int)
- Stockage: cfg.rm.max_fragments
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_MIN_BPS_CAP
- Lecture: bot_config.py:L2437 (get_float)
- Stockage: cfg.rm.min_bps_cap
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_MIN_BPS_FLOOR
- Lecture: bot_config.py:L2436 (get_float)
- Stockage: cfg.rm.min_bps_floor
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_MM_ALIAS_NAME
- Lecture: bot_config.py:L2885 (get)
- Stockage: cfg.rm.mm_alias_name
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_MM_DEPTH_MIN_USD
- Lecture: bot_config.py:L2886 (get_float)
- Stockage: cfg.rm.mm_depth_min_usd
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_MM_HEDGE_COST_BPS
- Lecture: bot_config.py:L2890 (get_float)
- Stockage: cfg.rm.mm_hedge_cost_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_MM_MIN_NET_BPS
- Lecture: bot_config.py:L2889 (get_float)
- Stockage: cfg.rm.mm_min_net_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_MM_MIN_P_BOTH
- Lecture: bot_config.py:L2888 (get_float)
- Stockage: cfg.rm.mm_min_p_both
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_MM_QPOS_MAX_AHEAD_USD
- Lecture: bot_config.py:L2887 (get_float)
- Stockage: cfg.rm.mm_qpos_max_ahead_usd
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_MM_VOL_BPS_MAX
- Lecture: bot_config.py:L2891 (get_float)
- Stockage: cfg.rm.mm_vol_bps_max
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_MODE_TICK_INTERVAL_S
- Lecture: bot_config.py:L2450 (get_float)
- Stockage: cfg.rm.mode_tick_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_MODE_TIMEOUT_S
- Lecture: bot_config.py:L2463 (get_int)
- Stockage: rm_knobs.mode_timeout_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_NET_FLOOR_BPS
- Lecture: bot_config.py:L2467 (get_float)
- Stockage: rm_knobs.net_floor_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_NORMAL_CAP_FACTOR
- Lecture: bot_config.py:L2500 (get_float)
- Stockage: rm_knobs.normal_cap_factor
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_NORMAL_IOC_ONLY
- Lecture: bot_config.py:L2501 (get_bool)
- Stockage: rm_knobs.normal_ioc_only
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_NORMAL_TM_MIN_BPS_DELTA
- Lecture: bot_config.py:L2497 (get_float)
- Stockage: rm_knobs.normal_tm_min_bps_delta
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_NORMAL_TT_MIN_BPS_DELTA
- Lecture: bot_config.py:L2494 (get_float)
- Stockage: rm_knobs.normal_tt_min_bps_delta
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_OPPVOL_P95_BPS_MIN
- Lecture: bot_config.py:L2481 (get_float)
- Stockage: rm_knobs.oppvol_p95_bps_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_OPP_AGE_FALLBACK_S
- Lecture: bot_config.py:L2468 (get_float)
- Stockage: rm_knobs.opp_age_fallback_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_OPP_SHADOW_P50_BPS_MAX
- Lecture: bot_config.py:L2475 (get_float)
- Stockage: rm_knobs.opp_shadow_p50_bps_max
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_OPP_VOLUME_CAP_FACTOR
- Lecture: bot_config.py:L2513 (get_float)
- Stockage: rm_knobs.opp_volume_cap_factor
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_OPP_VOLUME_MM_ENABLE
- Lecture: bot_config.py:L2516 (get_bool)
- Stockage: rm_knobs.opp_volume_mm_enable
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_OPP_VOLUME_TIMEOUT_S
- Lecture: bot_config.py:L2464 (get_int)
- Stockage: rm_knobs.opp_volume_timeout_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_OPP_VOLUME_TM_MIN_BPS_DELTA
- Lecture: bot_config.py:L2510 (get_float)
- Stockage: rm_knobs.opp_volume_tm_min_bps_delta
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_OPP_VOLUME_TT_MIN_BPS_DELTA
- Lecture: bot_config.py:L2507 (get_float)
- Stockage: rm_knobs.opp_volume_tt_min_bps_delta
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_OPP_VOL_CAP_FACTOR
- Lecture: bot_config.py:L2525 (get_float)
- Stockage: rm_knobs.opp_vol_cap_factor
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_OPP_VOL_EXIT_SLIP_AGE_S_MAX
- Lecture: bot_config.py:L2478 (get_float)
- Stockage: rm_knobs.opp_vol_exit_slip_age_s_max
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_OPP_VOL_MM_ENABLE
- Lecture: bot_config.py:L2528 (get_bool)
- Stockage: rm_knobs.opp_vol_mm_enable
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_OPP_VOL_P95_BPS_MAX
- Lecture: bot_config.py:L2472 (get_float)
- Stockage: rm_knobs.opp_vol_p95_bps_max
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_OPP_VOL_SLIP_AGE_S_MAX
- Lecture: bot_config.py:L2469 (get_float)
- Stockage: rm_knobs.opp_vol_slip_age_s_max
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_OPP_VOL_TM_MIN_BPS_DELTA
- Lecture: bot_config.py:L2522 (get_float)
- Stockage: rm_knobs.opp_vol_tm_min_bps_delta
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_OPP_VOL_TT_MIN_BPS_DELTA
- Lecture: bot_config.py:L2519 (get_float)
- Stockage: rm_knobs.opp_vol_tt_min_bps_delta
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_PNL_COOLDOWN_S
- Lecture: bot_config.py:L2490 (get_int)
- Stockage: rm_knobs.pnl_cooldown_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_PNL_GUARD_DAY_LVL1_PCT
- Lecture: bot_config.py:L2484 (get_float)
- Stockage: rm_knobs.pnl_guard_day_lvl1_pct
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_PNL_GUARD_DAY_LVL2_PCT
- Lecture: bot_config.py:L2487 (get_float)
- Stockage: rm_knobs.pnl_guard_day_lvl2_pct
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_PNL_GUARD_RECOVER_FLOOR_PCT
- Lecture: bot_config.py:L2491 (get_float)
- Stockage: rm_knobs.pnl_guard_recover_floor_pct
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_PWS_ACK_LATENCY_CONSTRAINED_MS
- Lecture: bot_config.py:L2397 (get_float)
- Stockage: cfg.RM_PWS_ACK_LATENCY_CONSTRAINED_MS
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_PWS_ACK_LATENCY_SEVERE_MS
- Lecture: bot_config.py:L2401 (get_float)
- Stockage: cfg.RM_PWS_ACK_LATENCY_SEVERE_MS
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_PWS_EVENT_LAG_CONSTRAINED_MS
- Lecture: bot_config.py:L2389 (get_float)
- Stockage: cfg.RM_PWS_EVENT_LAG_CONSTRAINED_MS
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_PWS_EVENT_LAG_SEVERE_MS
- Lecture: bot_config.py:L2393 (get_float)
- Stockage: cfg.RM_PWS_EVENT_LAG_SEVERE_MS
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_PWS_FILL_LATENCY_CONSTRAINED_MS
- Lecture: bot_config.py:L2426 (get_float)
- Stockage: cfg.RM_PWS_FILL_LATENCY_CONSTRAINED_MS
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_PWS_FILL_LATENCY_SEVERE_MS
- Lecture: bot_config.py:L2430 (get_float)
- Stockage: cfg.RM_PWS_FILL_LATENCY_SEVERE_MS
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_PWS_HEARTBEAT_GAP_CONSTRAINED_S
- Lecture: bot_config.py:L2381 (get_float)
- Stockage: cfg.RM_PWS_HEARTBEAT_GAP_CONSTRAINED_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_PWS_HEARTBEAT_GAP_SEVERE_S
- Lecture: bot_config.py:L2385 (get_float)
- Stockage: cfg.RM_PWS_HEARTBEAT_GAP_SEVERE_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_REBAL_ALLOW_LOSS_BPS_BY_PROFILE
- Lecture: bot_config.py:L2975 (get_dict)
- Stockage: cfg.rm.rebal_allow_loss_bps_by_profile
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_REBAL_VOLUME_HAIRCUT_BY_PROFILE
- Lecture: bot_config.py:L2979 (get_dict)
- Stockage: cfg.rm.rebal_volume_haircut_by_profile
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_RECO_MISS_PER_MINUTE_CONSTRAINED
- Lecture: bot_config.py:L2371 (get_int)
- Stockage: cfg.RM_RECO_MISS_PER_MINUTE_CONSTRAINED
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_RECO_MISS_PER_MINUTE_SEVERE
- Lecture: bot_config.py:L2375 (get_int)
- Stockage: cfg.RM_RECO_MISS_PER_MINUTE_SEVERE
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_REQUIRE_BOOK_AGE_FOR_OPP
- Lecture: bot_config.py:L2566 (get_bool)
- Stockage: signal_policy.require_book_age_for_opp
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_REQUIRE_PWS_HEALTH_FOR_OPP
- Lecture: bot_config.py:L2563 (get_bool)
- Stockage: signal_policy.require_pws_health_for_opp
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_REQUIRE_RL_HEALTH_FOR_OPP
- Lecture: bot_config.py:L2560 (get_bool)
- Stockage: signal_policy.require_rl_health_for_opp
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_REQUIRE_SHADOW_FOR_OPP
- Lecture: bot_config.py:L2557 (get_bool)
- Stockage: signal_policy.require_shadow_for_opp
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_REQUIRE_VOL_SIGNAL_FOR_OPP
- Lecture: bot_config.py:L2554 (get_bool)
- Stockage: signal_policy.require_vol_signal_for_opp
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_SEVERE_CAP_FACTOR
- Lecture: bot_config.py:L2537 (get_float)
- Stockage: rm_knobs.severe_cap_factor
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_SEVERE_IOC_ONLY
- Lecture: bot_config.py:L2539 (get_bool)
- Stockage: rm_knobs.severe_ioc_only
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_SEVERE_MM_ENABLE
- Lecture: bot_config.py:L2538 (get_bool)
- Stockage: rm_knobs.severe_mm_enable
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_SEVERE_TM_MIN_BPS_DELTA
- Lecture: bot_config.py:L2534 (get_float)
- Stockage: rm_knobs.severe_tm_min_bps_delta
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_SEVERE_TT_MIN_BPS_DELTA
- Lecture: bot_config.py:L2531 (get_float)
- Stockage: rm_knobs.severe_tt_min_bps_delta
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_SPLIT_BREACH_MIN_S
- Lecture: bot_config.py:L2592 (get_float)
- Stockage: cfg.rm.split_breach_min_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_SPLIT_BREACH_THR_BASE_MS
- Lecture: bot_config.py:L2583 (get_float)
- Stockage: cfg.rm.split_breach_thr_base_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_SPLIT_BREACH_THR_SKEW_MS
- Lecture: bot_config.py:L2586 (get_float)
- Stockage: cfg.rm.split_breach_thr_skew_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_SPLIT_BREACH_THR_STALE_MS
- Lecture: bot_config.py:L2589 (get_float)
- Stockage: cfg.rm.split_breach_thr_stale_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_SPLIT_FALLBACK_COOLDOWN_S
- Lecture: bot_config.py:L2595 (get_float)
- Stockage: cfg.rm.split_fallback_cooldown_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_SPLIT_PENALTY_BPS_MAX
- Lecture: bot_config.py:L2601 (get_float)
- Stockage: cfg.rm.split_penalty_bps_max
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_SPLIT_RESTORE_STABLE_S
- Lecture: bot_config.py:L2598 (get_float)
- Stockage: cfg.rm.split_restore_stable_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_SWITCH_ENTER_HYST_S
- Lecture: bot_config.py:L2461 (get_int)
- Stockage: rm_knobs.enter_hyst_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_SWITCH_EXIT_HYST_S
- Lecture: bot_config.py:L2462 (get_int)
- Stockage: rm_knobs.exit_hyst_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_TTL_FACTOR_MM_DEGRADED
- Lecture: bot_config.py:L2447 (get_float)
- Stockage: cfg.rm.ttl_factor_mm_degraded
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_TTL_FACTOR_REB_DEGRADED
- Lecture: bot_config.py:L2444 (get_float)
- Stockage: cfg.rm.ttl_factor_reb_degraded
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_TTL_FACTOR_TM_DEGRADED
- Lecture: bot_config.py:L2441 (get_float)
- Stockage: cfg.rm.ttl_factor_tm_degraded
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_TTL_FACTOR_TT_DEGRADED
- Lecture: bot_config.py:L2438 (get_float)
- Stockage: cfg.rm.ttl_factor_tt_degraded
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_TTTM_EXPOSURE_BY_ASSET
- Lecture: bot_config.py:L2923 (get_dict)
- Stockage: cfg.rm.tttm_exposure_by_asset
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_TTTM_EXPOSURE_HARD_USD
- Lecture: bot_config.py:L2918 (get_float)
- Stockage: cfg.rm.tttm_exposure_hard_usd
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_TTTM_EXPOSURE_SOFT_USD
- Lecture: bot_config.py:L2914 (get_float)
- Stockage: cfg.rm.tttm_exposure_soft_usd
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_TT_STUCK_HARD_USD
- Lecture: bot_config.py:L2933 (get_float)
- Stockage: cfg.rm.tt_stuck_hard_usd
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_TT_STUCK_MAX_AGE_S
- Lecture: bot_config.py:L2937 (get_float)
- Stockage: cfg.rm.tt_stuck_max_age_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RM_TT_STUCK_SOFT_USD
- Lecture: bot_config.py:L2929 (get_float)
- Stockage: cfg.rm.tt_stuck_soft_usd
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_BACKLOG_CRIT
- Lecture: bot_config.py:L2001 (get_float)
- Stockage: cfg.wd.router_backlog_crit
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_BACKLOG_WARN
- Lecture: bot_config.py:L2000 (get_float)
- Stockage: cfg.wd.router_backlog_warn
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_BACKPRESSURE
- Lecture: bot_config.py:L1938 (get_dict)
- Stockage: cfg.router.backpressure
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_CB_COALESCE_BUMP_MS
- Lecture: bot_config.py:L1939 (get_int)
- Stockage: cfg.router.cb_coalesce_bump_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_COALESCE_MAXLEN
- Lecture: bot_config.py:L1924 (get_int)
- Stockage: cfg.router.coalesce_maxlen
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_COALESCE_WINDOW_MS
- Lecture: bot_config.py:L1890 (get_int)
- Stockage: cfg.router.coalesce_window_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_DEQUE_MAXLEN_PER_EX
- Lecture: bot_config.py:L1937 (get_dict)
- Stockage: cfg.router.deque_maxlen_per_ex
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_DETERMINISTIC_BACKPRESSURE
- Lecture: bot_config.py:L1944 (get_bool)
- Stockage: cfg.router.deterministic_backpressure
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_DROP_POLICY
- Lecture: bot_config.py:L1929 (get), bot_config.py:L1932 (get_dict)
- Stockage: drop_policy, drop_policy_raw
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_ESCALATE_AFTER_CYCLES
- Lecture: bot_config.py:L2002 (get_int)
- Stockage: cfg.wd.router_escalate_after_cycles
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_FAIL_FAST_ON_EVENT_EXCEPTION
- Lecture: bot_config.py:L1953 (get_bool)
- Stockage: cfg.router.fail_fast_on_event_exception
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_HB_ONCHANGE_BPS
- Lecture: bot_config.py:L1923 (get_float)
- Stockage: cfg.router.hb_onchange_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_HEALTH_MIN_COVERAGE_RATIO
- Lecture: bot_config.py:L1994 (get_float)
- Stockage: cfg.wd.router_health_min_coverage_ratio
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_HEALTH_QUEUE_MAX
- Lecture: bot_config.py:L1997 (get_int)
- Stockage: cfg.wd.router_health_queue_max
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_HEALTH_STALE_MS
- Lecture: bot_config.py:L1993 (get_int)
- Stockage: cfg.wd.router_health_stale_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_MUX_POLL_MS
- Lecture: bot_config.py:L1919 (get_int)
- Stockage: cfg.router.mux_poll_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_OUT_QUEUES_MAXLEN
- Lecture: bot_config.py:L1915 (get_int)
- Stockage: cfg.router.out_queues_maxlen
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_OUT_QUEUES_MAXLEN_BY_KIND
- Lecture: bot_config.py:L1916 (get_dict)
- Stockage: cfg.router.out_queues_maxlen_by_kind
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_R2S_CRIT_MS
- Lecture: bot_config.py:L1999 (get_int)
- Stockage: cfg.wd.router_r2s_crit_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_R2S_WARN_MS
- Lecture: bot_config.py:L1998 (get_int)
- Stockage: cfg.wd.router_r2s_warn_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_REQUIRE_L2_FIRST
- Lecture: bot_config.py:L1920 (get_bool)
- Stockage: cfg.router.require_l2_first
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_SCANNER_QUEUE_DROP_POLICY
- Lecture: bot_config.py:L1950 (get)
- Stockage: cfg.router.scanner_queue_drop_policy
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_SCANNER_QUEUE_MAXLEN
- Lecture: bot_config.py:L1947 (get_int)
- Stockage: cfg.router.scanner_queue_maxlen
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_SHARDS_PER_EXCHANGE
- Lecture: bot_config.py:L1893 (get), bot_config.py:L1897 (get_list)
- Stockage: parsed, shards_raw
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_SLIP_ONCHANGE_BPS
- Lecture: bot_config.py:L1922 (get_float)
- Stockage: cfg.router.slip_onchange_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_STALENESS_MODE
- Lecture: bot_config.py:L1943 (get)
- Stockage: cfg.router.staleness_mode
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_STALE_MS
- Lecture: bot_config.py:L1891 (get_int)
- Stockage: cfg.router.stale_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_TOPIC_MAX_HZ
- Lecture: bot_config.py:L1942 (get_dict)
- Stockage: cfg.router.topic_max_hz
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_VOL_ONCHANGE_BPS
- Lecture: bot_config.py:L1921 (get_float)
- Stockage: cfg.router.vol_onchange_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_WS_SHARDS
- Lecture: bot_config.py:L1911 (get_dict)
- Stockage: cfg.router.ws_shards_by_exchange
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### ROUTER_WS_SOURCE_BACKPRESSURE_COOLDOWN_S
- Lecture: bot_config.py:L1925 (get_float)
- Stockage: cfg.router.ws_source_backpressure_cooldown_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_CA_CERT
- Lecture: bot_config.py:L3401 (get)
- Stockage: cfg.rpc.ca_cert
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_CERT_PATHS
- Lecture: bot_config.py:L3412 (get_dict)
- Stockage: cfg.rpc.rpc_cert_paths
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_CLIENT_BASE
- Lecture: bot_config.py:L3408 (get)
- Stockage: cfg.rpc.rpc_client_base
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_CLIENT_CERT
- Lecture: bot_config.py:L3404 (get)
- Stockage: cfg.rpc.client_cert
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_CLIENT_KEY
- Lecture: bot_config.py:L3405 (get)
- Stockage: cfg.rpc.client_key
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_ENABLED
- Lecture: bot_config.py:L3391 (get_bool)
- Stockage: cfg.rpc.enabled
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_ENABLE_MTLS
- Lecture: bot_config.py:L3411 (get_bool)
- Stockage: cfg.rpc.rpc_enable_mtls
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_HOST
- Lecture: bot_config.py:L3392 (get)
- Stockage: cfg.rpc.host
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_LEGACY_TIMEOUT_S
- Lecture: bot_config.py:L3409 (get_int)
- Stockage: cfg.rpc.rpc_timeout_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_LOOPBACK_INPROC
- Lecture: bot_config.py:L3399 (get_bool)
- Stockage: cfg.rpc.loopback_inproc
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_MAX_PAYLOAD_KB
- Lecture: bot_config.py:L3413 (get_int)
- Stockage: cfg.rpc.rpc_max_payload_kb
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_MAX_RETRIES
- Lecture: bot_config.py:L3396 (get_int)
- Stockage: cfg.rpc.max_retries
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_MTLS_ENABLED
- Lecture: bot_config.py:L3398 (get_bool)
- Stockage: cfg.rpc.mtls_enabled
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_PORT
- Lecture: bot_config.py:L3393 (get_int)
- Stockage: cfg.rpc.port
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_READY_STRICT
- Lecture: bot_config.py:L3400 (get_bool)
- Stockage: cfg.rpc.ready_strict
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_REGION
- Lecture: bot_config.py:L3394 (get)
- Stockage: cfg.rpc.region
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_REQUIRE_CLIENT_CERT
- Lecture: bot_config.py:L3406 (get_bool)
- Stockage: cfg.rpc.require_client_cert
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_RETRIES
- Lecture: bot_config.py:L3410 (get_int)
- Stockage: cfg.rpc.rpc_retries
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_SERVER_BIND
- Lecture: bot_config.py:L3407 (get)
- Stockage: cfg.rpc.rpc_server_bind
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_SERVER_CERT
- Lecture: bot_config.py:L3402 (get)
- Stockage: cfg.rpc.server_cert
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_SERVER_KEY
- Lecture: bot_config.py:L3403 (get)
- Stockage: cfg.rpc.server_key
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_TIMEOUT_MS
- Lecture: bot_config.py:L3397 (get_int)
- Stockage: cfg.rpc.timeout_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### RPC_TIMEOUT_S
- Lecture: bot_config.py:L3395 (get_float)
- Stockage: cfg.rpc.timeout_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_ALLOW_LOSS_BPS_REBAL
- Lecture: bot_config.py:L2715 (get_float)
- Stockage: cfg.scanner.allow_loss_bps_rebal
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_BACKPRESSURE_LOG_EVERY
- Lecture: bot_config.py:L2689 (get_int)
- Stockage: cfg.scanner.backpressure_log_every
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_BINANCE_DEPTH_LEVEL
- Lecture: bot_config.py:L2664 (get_int)
- Stockage: cfg.scanner.binance_depth_level
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_DEDUP_COOLDOWN_S
- Lecture: bot_config.py:L2726 (get_float)
- Stockage: cfg.SCANNER_DEDUP_COOLDOWN_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_DEDUP_WINDOW_S
- Lecture: bot_config.py:L2684 (get_float)
- Stockage: cfg.scanner.dedup_window_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_DEQUE_MAX
- Lecture: bot_config.py:L2725 (get_dict)
- Stockage: cfg.SCANNER_DEQUE_MAX
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_ENABLE_MM_HINTS
- Lecture: bot_config.py:L2663 (get_bool)
- Stockage: cfg.scanner.enable_mm_hints
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_EVAL_HZ_AUDITION
- Lecture: bot_config.py:L2709 (get_float)
- Stockage: cfg.scanner.scanner_eval_hz_audition
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_EVAL_HZ_CORE
- Lecture: bot_config.py:L2706 (get_float)
- Stockage: cfg.scanner.scanner_eval_hz_core
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_EVAL_HZ_PRIMARY
- Lecture: bot_config.py:L2703 (get_float)
- Stockage: cfg.scanner.scanner_eval_hz_primary
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_EVAL_HZ_SANDBOX
- Lecture: bot_config.py:L2712 (get_float)
- Stockage: cfg.scanner.scanner_eval_hz_sandbox
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_GLOBAL_EVAL_HZ
- Lecture: bot_config.py:L2756 (get_float)
- Stockage: cfg.SCANNER_GLOBAL_EVAL_HZ
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_HZ
- Lecture: bot_config.py:L2723 (get), bot_config.py:L2724 (get_dict)
- Stockage: cfg.SCANNER_HZ, raw_scanner_hz
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_MAX_OPPORTUNITIES
- Lecture: bot_config.py:L2692 (get_int)
- Stockage: cfg.scanner.max_opportunities
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_MAX_PAIRS_PER_TICK
- Lecture: bot_config.py:L2685 (get_int)
- Stockage: cfg.scanner.max_pairs_per_tick
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_MAX_TIME_SKEW_S
- Lecture: bot_config.py:L2695 (get_float)
- Stockage: cfg.scanner.max_time_skew_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_MM_DEPTH_MIN_QUOTE
- Lecture: bot_config.py:L2673 (get_float)
- Stockage: cfg.scanner.mm_depth_min_quote
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_MM_HEDGE_COST_BPS
- Lecture: bot_config.py:L2680 (get_float)
- Stockage: cfg.scanner.mm_hedge_cost_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_MM_MIN_NET_BPS
- Lecture: bot_config.py:L2679 (get_float)
- Stockage: cfg.scanner.mm_min_net_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_MM_QPOS_MAX_AHEAD_QUOTE
- Lecture: bot_config.py:L2676 (get_float)
- Stockage: cfg.scanner.mm_qpos_max_ahead_quote
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_MM_ROTATION_ENABLED
- Lecture: bot_config.py:L2667 (get_bool)
- Stockage: cfg.scanner.mm_rotation_enabled
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_MM_SEED_PAIRS
- Lecture: bot_config.py:L2671 (get_list)
- Stockage: cfg.scanner.mm_seed_pairs
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_MM_VOL_BPS_MAX
- Lecture: bot_config.py:L2681 (get_float)
- Stockage: cfg.scanner.mm_vol_bps_max
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_MODE
- Lecture: bot_config.py:L2662 (get)
- Stockage: cfg.scanner.scanner_mode
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_RM_ACK_TIMEOUT_S
- Lecture: bot_config.py:L2698 (get_float)
- Stockage: cfg.scanner.rm_ack_timeout_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_SCAN_INTERVAL_S
- Lecture: bot_config.py:L2688 (get_float)
- Stockage: cfg.scanner.scan_interval
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_SHED_AUDITION_FACTOR
- Lecture: bot_config.py:L2656 (get_float)
- Stockage: cfg.scanner.shed_audition_factor
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_SHED_COOLDOWN_S
- Lecture: bot_config.py:L2659 (get_float)
- Stockage: cfg.scanner.shed_cooldown_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_SHED_LOAD_THRESHOLD
- Lecture: bot_config.py:L2647 (get_float)
- Stockage: cfg.scanner.shed_load_threshold
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_SHED_PRIMARY_FACTOR
- Lecture: bot_config.py:L2650 (get_float)
- Stockage: cfg.scanner.shed_primary_factor
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_SHED_PRIMARY_MIN
- Lecture: bot_config.py:L2653 (get_float)
- Stockage: cfg.scanner.shed_primary_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SCANNER_WORKERS
- Lecture: bot_config.py:L2646 (get_int)
- Stockage: cfg.scanner.workers
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIMULATOR_BYPASS_ALLOWED_IN_LIVE
- Lecture: bot_config.py:L3030 (get_bool)
- Stockage: cfg.sim.simulator_bypass_allowed_in_live
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIMULATOR_MODE
- Lecture: bot_config.py:L3029 (get)
- Stockage: cfg.sim.simulator_mode
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_BOOK_FINGERPRINT_LEVELS
- Lecture: bot_config.py:L3065 (get_int)
- Stockage: cfg.sim.sim_book_fingerprint_levels
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_BOOK_FINGERPRINT_MODE
- Lecture: bot_config.py:L3063 (get)
- Stockage: cfg.sim.sim_book_fingerprint_mode
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_BUCKETS_ADAPT_WITH_VOL
- Lecture: bot_config.py:L3050 (get_bool)
- Stockage: cfg.sim.sim_buckets_adapt_with_vol
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_BUCKETS_PER_PROFILE
- Lecture: bot_config.py:L3046 (get_dict)
- Stockage: cfg.sim.sim_buckets_per_profile
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_CACHE_TTL_MS
- Lecture: bot_config.py:L3033 (get_int)
- Stockage: cfg.sim.sim_cache_ttl_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_DETERMINISTIC_TRADE_ID
- Lecture: bot_config.py:L3040 (get_bool)
- Stockage: cfg.sim.sim_deterministic_trade_id
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_MAX_FRAGMENTS
- Lecture: bot_config.py:L3027 (get_int)
- Stockage: cfg.sim.max_fragments
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_MAX_INFLIGHT_JOBS
- Lecture: bot_config.py:L3036 (get_int)
- Stockage: cfg.sim.sim_max_inflight_jobs
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_MAX_WAIT_MS_RM
- Lecture: bot_config.py:L3034 (get_int)
- Stockage: cfg.sim.sim_max_wait_ms_rm
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_MIN_FRAGMENT_USDC
- Lecture: bot_config.py:L3028 (get_float)
- Stockage: cfg.sim.min_fragment_usdc
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_MM_HINTS_INTERVAL_MS
- Lecture: bot_config.py:L3068 (get_int)
- Stockage: cfg.sim.sim_mm_hints_interval_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_MM_HINTS_LEVELS
- Lecture: bot_config.py:L3071 (get_int)
- Stockage: cfg.sim.sim_mm_hints_levels
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_NOTIONAL_BUCKETS_BASE
- Lecture: bot_config.py:L3043 (get)
- Stockage: cfg.sim.sim_notional_buckets_base
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_OUTPUTS_REQUIRED_BY_BRANCH
- Lecture: bot_config.py:L3074 (get_dict)
- Stockage: cfg.sim.sim_outputs_required_by_branch
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_PRIME_DEPTH_LEVELS
- Lecture: bot_config.py:L3035 (get_int)
- Stockage: cfg.sim.sim_prime_depth_levels
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_PRIME_MULTIBUCKET_K
- Lecture: bot_config.py:L3059 (get_int)
- Stockage: cfg.sim.sim_prime_multibucket_k
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_SHADOW_MAX_INFLIGHT
- Lecture: bot_config.py:L3037 (get_int)
- Stockage: cfg.sim.sim_shadow_max_inflight
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_VOL_SIZE_FACTOR_CEIL
- Lecture: bot_config.py:L3056 (get_float)
- Stockage: cfg.sim.sim_vol_size_factor_ceil
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SIM_VOL_SIZE_FACTOR_FLOOR
- Lecture: bot_config.py:L3053 (get_float)
- Stockage: cfg.sim.sim_vol_size_factor_floor
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SLIP_HEARTBEAT_S
- Lecture: bot_config.py:L3334 (get_int)
- Stockage: cfg.slip.heartbeat_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SLIP_MAX_BPS_BY_QUOTE
- Lecture: bot_config.py:L3337 (get_dict)
- Stockage: cfg.slip.max_bps_by_quote
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SLIP_TTL_S
- Lecture: bot_config.py:L1762 (get), bot_config.py:L1773 (get_int)
- Stockage: <expr>, _slip_ttl_env
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SLIP_USE_VWAP_DEPTH
- Lecture: bot_config.py:L3335 (get_bool)
- Stockage: cfg.slip.use_vwap_depth
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### SPLIT_LATENCY
- Lecture: bot_config.py:L1737 (get_dict)
- Stockage: g.split_latency
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### STATUS_PORT
- Lecture: bot_config.py:L1842 (get_int)
- Stockage: cfg.obs.status_port
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### STRICT_OBS
- Lecture: bot_config.py:L1840 (get_bool), bot_config.py:L3429 (get_bool)
- Stockage: cfg.lhm.strict_obs, cfg.obs.strict_obs
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### TELEGRAM_ACK_PIN
- Lecture: bot_config.py:L1868 (get)
- Stockage: cfg.alerting.telegram.ack_pin
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### TELEGRAM_ALLOWED_USER_IDS
- Lecture: bot_config.py:L1856 (get_list)
- Stockage: cfg.alerting.telegram.allowed_user_ids
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### TELEGRAM_BOT_TOKEN
- Lecture: bot_config.py:L1853 (get)
- Stockage: cfg.alerting.telegram.bot_token
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### TELEGRAM_CHAT_ID_CRIT
- Lecture: bot_config.py:L1865 (get)
- Stockage: cfg.alerting.telegram.chat_id_crit
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### TELEGRAM_CHAT_ID_INFO
- Lecture: bot_config.py:L1859 (get)
- Stockage: cfg.alerting.telegram.chat_id_info
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### TELEGRAM_CHAT_ID_WARN
- Lecture: bot_config.py:L1862 (get)
- Stockage: cfg.alerting.telegram.chat_id_warn
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### TELEGRAM_REQUIRE_ACK
- Lecture: bot_config.py:L1869 (get_bool)
- Stockage: cfg.alerting.telegram.require_ack
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### TM_EXPOSURE_TTL_HEDGE_RATIO
- Lecture: bot_config.py:L2987 (get_float)
- Stockage: neutral_hr_env
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### TM_EXPOSURE_TTL_MS
- Lecture: bot_config.py:L3103 (get_int)
- Stockage: ttl_ms_global
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### TM_NEUTRAL_HEDGE_RATIO
- Lecture: bot_config.py:L2989 (get_float)
- Stockage: neutral_hr_env
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### TM_NEUTRAL_HEDGE_RATIO_MAP
- Lecture: bot_config.py:L3329 (get_dict)
- Stockage: cfg.vol.tm_neutral_hedge_ratio_map
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### TM_NN_HEDGE_RATIO
- Lecture: bot_config.py:L3001 (get_float)
- Stockage: nn_hr
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### TM_QUEUEPOS_MAX_AHEAD_USD
- Lecture: bot_config.py:L2875 (get_float)
- Stockage: cfg.rm.tm_queuepos_max_ahead_usd
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### TM_QUEUEPOS_MAX_ETA_MS
- Lecture: bot_config.py:L2879 (get_int)
- Stockage: cfg.rm.tm_queuepos_max_eta_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### TRANSFER_RETRY_POLICY
- Lecture: bot_config.py:L2334 (get_dict)
- Stockage: retry_policy
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### TRANSFER_SUBMITTED_TIMEOUT_S
- Lecture: bot_config.py:L2330 (get_float)
- Stockage: cfg.rm.transfer_submitted_timeout_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VM_MAKER_PAD_TICKS_MAP
- Lecture: bot_config.py:L3333 (get_dict)
- Stockage: cfg.vol.vm_maker_pad_ticks_map
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VM_MIN_BPS_BOOST_MAP
- Lecture: bot_config.py:L3328 (get_dict)
- Stockage: cfg.vol.vm_min_bps_boost_map
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VM_PRUDENCE_THRESHOLDS_BPS
- Lecture: bot_config.py:L3331 (get_dict)
- Stockage: cfg.vol.vm_prudence_thresholds_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VM_SIZE_FACTOR_MAP
- Lecture: bot_config.py:L3327 (get_dict)
- Stockage: cfg.vol.vm_size_factor_map
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VOL_CHAOS_CAP_BPS
- Lecture: bot_config.py:L3319 (get_float)
- Stockage: cfg.vol.chaos_cap_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VOL_EMA_ALPHA
- Lecture: bot_config.py:L3317 (get_float)
- Stockage: cfg.vol.ema_alpha
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VOL_HYSTERESIS
- Lecture: bot_config.py:L3320 (get_float)
- Stockage: cfg.vol.hysteresis
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VOL_MAX_SILENCE_S
- Lecture: bot_config.py:L3322 (get_float)
- Stockage: cfg.vol.max_silence_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VOL_MIDPRICE_TO_BPS
- Lecture: bot_config.py:L3314 (get_float)
- Stockage: cfg.vol.midprice_to_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VOL_MIN_DELTA_BPS
- Lecture: bot_config.py:L3321 (get_float)
- Stockage: cfg.vol.vol_min_delta_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VOL_SLIP_TTL
- Lecture: bot_config.py:L1759 (get), bot_config.py:L1761 (get_dict)
- Stockage: _vol_slip_ttl_env, g.vol_slip_ttl
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VOL_SOFT_CAP_BPS
- Lecture: bot_config.py:L3318 (get_float)
- Stockage: cfg.vol.soft_cap_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VOL_TO_BPS_CAP
- Lecture: bot_config.py:L3316 (get_float)
- Stockage: cfg.vol.to_bps_cap
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VOL_TO_BPS_FLOOR
- Lecture: bot_config.py:L3315 (get_float)
- Stockage: cfg.vol.to_bps_floor
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VOL_TTL_S
- Lecture: bot_config.py:L1763 (get), bot_config.py:L1777 (get_int)
- Stockage: <expr>, _vol_ttl_env
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VOL_WINDOW_LONG_M
- Lecture: bot_config.py:L3323 (get_int)
- Stockage: cfg.vol.window_long_m
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VOL_WINDOW_MICRO_M
- Lecture: bot_config.py:L3324 (get_int)
- Stockage: cfg.vol.window_micro_m
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### VOL_WINSOR_PCT
- Lecture: bot_config.py:L3325 (get_float)
- Stockage: cfg.vol.winsor_pct
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WALLET_ALIASES
- Lecture: bot_config.py:L3530 (get_dict)
- Stockage: cfg.wallet_aliases
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WALLET_ALIAS_BY_QUOTE
- Lecture: bot_config.py:L1749 (get_dict)
- Stockage: g.wallet_alias_by_quote
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WALLET_MISSING_LOG_INTERVAL_S
- Lecture: bot_config.py:L3307 (get_float)
- Stockage: cfg.balances.wallet_missing_log_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WATCHDOG_NOTIFY_ONLY
- Lecture: bot_config.py:L1990 (get_bool)
- Stockage: cfg.wd.notify_only_default
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_BALANCE_ERROR_THRESHOLD
- Lecture: bot_config.py:L2088 (get_int)
- Stockage: cfg.wd.balance_error_threshold
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_BALANCE_INTERVAL_S
- Lecture: bot_config.py:L2086 (get_float)
- Stockage: cfg.wd.balance_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_COOLDOWN_S
- Lecture: bot_config.py:L1987 (get_float)
- Stockage: cfg.wd.cooldown_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_DISCOVERY_CONFIRM_TICKS
- Lecture: bot_config.py:L2198 (get_int)
- Stockage: cfg.wd.discovery_confirm_ticks
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_DISCOVERY_DWELL_TICKS
- Lecture: bot_config.py:L2199 (get_int)
- Stockage: cfg.wd.discovery_dwell_ticks
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_DISCOVERY_INTERVAL_S
- Lecture: bot_config.py:L2194 (get_float)
- Stockage: cfg.wd.discovery_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_DISCOVERY_MAX_REFRESH_GAP_S
- Lecture: bot_config.py:L2200 (get_int)
- Stockage: cfg.wd.discovery_max_refresh_gap_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_DISCOVERY_MIN_CHANGE_RATIO
- Lecture: bot_config.py:L2195 (get_float)
- Stockage: cfg.wd.discovery_min_change_ratio
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_ENGINE_ACK_FILL_CRIT_MS
- Lecture: bot_config.py:L2052 (get_int)
- Stockage: cfg.wd.engine_ack_fill_crit_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_ENGINE_ACK_FILL_WARN_MS
- Lecture: bot_config.py:L2049 (get_int)
- Stockage: cfg.wd.engine_ack_fill_warn_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_ENGINE_ESCALATE_AFTER_CYCLES
- Lecture: bot_config.py:L2079 (get_int)
- Stockage: cfg.wd.engine_escalate_after_cycles
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_ENGINE_INTERVAL_S
- Lecture: bot_config.py:L2042 (get_float)
- Stockage: cfg.wd.engine_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_ENGINE_PANIC_HEDGE_CRIT_PER_MIN
- Lecture: bot_config.py:L2070 (get_float)
- Stockage: cfg.wd.engine_panic_hedge_crit_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_ENGINE_PANIC_HEDGE_WARN_PER_MIN
- Lecture: bot_config.py:L2067 (get_float)
- Stockage: cfg.wd.engine_panic_hedge_warn_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_ENGINE_QUEUEPOS_BLOCKED_CRIT_PER_MIN
- Lecture: bot_config.py:L2076 (get_float)
- Stockage: cfg.wd.engine_queuepos_blocked_crit_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_ENGINE_QUEUEPOS_BLOCKED_WARN_PER_MIN
- Lecture: bot_config.py:L2073 (get_float)
- Stockage: cfg.wd.engine_queuepos_blocked_warn_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_ENGINE_RETRIES_CRIT_PER_MIN
- Lecture: bot_config.py:L2064 (get_float)
- Stockage: cfg.wd.engine_retries_crit_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_ENGINE_RETRIES_WARN_PER_MIN
- Lecture: bot_config.py:L2061 (get_float)
- Stockage: cfg.wd.engine_retries_warn_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_ENGINE_SUBMIT_ACK_CRIT_MS
- Lecture: bot_config.py:L2046 (get_int)
- Stockage: cfg.wd.engine_submit_ack_crit_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_ENGINE_SUBMIT_ACK_WARN_MS
- Lecture: bot_config.py:L2043 (get_int)
- Stockage: cfg.wd.engine_submit_ack_warn_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_ENGINE_TIMEOUTS_CRIT_PER_MIN
- Lecture: bot_config.py:L2058 (get_float)
- Stockage: cfg.wd.engine_timeouts_crit_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_ENGINE_TIMEOUTS_WARN_PER_MIN
- Lecture: bot_config.py:L2055 (get_float)
- Stockage: cfg.wd.engine_timeouts_warn_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_INTERVAL_S
- Lecture: bot_config.py:L1986 (get_float)
- Stockage: cfg.wd.interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_LOGGER_INTERVAL_S
- Lecture: bot_config.py:L2090 (get_float)
- Stockage: cfg.wd.logger_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_LOGGER_MIN_QUEUE_FOR_WRITER_STALL
- Lecture: bot_config.py:L2097 (get_int)
- Stockage: cfg.wd.logger_min_queue_for_writer_stall
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_LOGGER_QUEUE_STUCK_CHECKS
- Lecture: bot_config.py:L2093 (get_int)
- Stockage: cfg.wd.logger_queue_stuck_checks
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_LOGGER_ROTATION_HARD_STALL_FACTOR
- Lecture: bot_config.py:L2103 (get_float)
- Stockage: cfg.wd.logger_rotation_hard_stall_factor
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_LOGGER_ROTATION_STALL_FACTOR
- Lecture: bot_config.py:L2100 (get_float)
- Stockage: cfg.wd.logger_rotation_stall_factor
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_LOGGER_TRADE_QUEUE_CRIT
- Lecture: bot_config.py:L2092 (get_int)
- Stockage: cfg.wd.logger_trade_queue_crit
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_LOGGER_TRADE_QUEUE_WARN
- Lecture: bot_config.py:L2091 (get_int)
- Stockage: cfg.wd.logger_trade_queue_warn
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_LOGGER_WRITER_STALL_S
- Lecture: bot_config.py:L2096 (get_float)
- Stockage: cfg.wd.logger_writer_stall_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_BACKLOG_CRIT_RATIO
- Lecture: bot_config.py:L2151 (get_float)
- Stockage: cfg.wd.opportunity_backlog_crit_ratio
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_BACKLOG_WARN_RATIO
- Lecture: bot_config.py:L2148 (get_float)
- Stockage: cfg.wd.opportunity_backlog_warn_ratio
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_DECISION_P95_CRIT_MS
- Lecture: bot_config.py:L2139 (get_int)
- Stockage: cfg.wd.opportunity_decision_p95_crit_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_DECISION_P95_WARN_MS
- Lecture: bot_config.py:L2136 (get_int)
- Stockage: cfg.wd.opportunity_decision_p95_warn_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_DEDUP_CRIT_PER_MIN
- Lecture: bot_config.py:L2157 (get_float)
- Stockage: cfg.wd.opportunity_dedup_crit_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_DEDUP_WARN_PER_MIN
- Lecture: bot_config.py:L2154 (get_float)
- Stockage: cfg.wd.opportunity_dedup_warn_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_EFFECTIVE_RATIO_CRIT
- Lecture: bot_config.py:L2133 (get_float)
- Stockage: cfg.wd.opportunity_effective_ratio_crit
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_EFFECTIVE_RATIO_WARN
- Lecture: bot_config.py:L2130 (get_float)
- Stockage: cfg.wd.opportunity_effective_ratio_warn
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_EMIT_P95_CRIT_MS
- Lecture: bot_config.py:L2145 (get_int)
- Stockage: cfg.wd.opportunity_emit_p95_crit_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_EMIT_P95_WARN_MS
- Lecture: bot_config.py:L2142 (get_int)
- Stockage: cfg.wd.opportunity_emit_p95_warn_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_ESCALATE_AFTER_CYCLES
- Lecture: bot_config.py:L2190 (get_int)
- Stockage: cfg.wd.opportunity_escalate_after_cycles
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_FEES_AGE_CRIT_S
- Lecture: bot_config.py:L2181 (get_float)
- Stockage: cfg.wd.opportunity_fees_age_crit_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_FEES_AGE_WARN_S
- Lecture: bot_config.py:L2178 (get_float)
- Stockage: cfg.wd.opportunity_fees_age_warn_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_INTERVAL_S
- Lecture: bot_config.py:L2129 (get_float)
- Stockage: cfg.wd.opportunity_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_REJECTION_RATIO_CRIT
- Lecture: bot_config.py:L2163 (get_float)
- Stockage: cfg.wd.opportunity_rejection_ratio_crit
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_REJECTION_RATIO_WARN
- Lecture: bot_config.py:L2160 (get_float)
- Stockage: cfg.wd.opportunity_rejection_ratio_warn
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_SCANNER_ERR_CRIT_PER_MIN
- Lecture: bot_config.py:L2187 (get_float)
- Stockage: cfg.wd.opportunity_scanner_err_crit_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_SCANNER_ERR_WARN_PER_MIN
- Lecture: bot_config.py:L2184 (get_float)
- Stockage: cfg.wd.opportunity_scanner_err_warn_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_SLIP_AGE_CRIT_S
- Lecture: bot_config.py:L2169 (get_float)
- Stockage: cfg.wd.opportunity_slip_age_crit_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_SLIP_AGE_WARN_S
- Lecture: bot_config.py:L2166 (get_float)
- Stockage: cfg.wd.opportunity_slip_age_warn_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_VOL_AGE_CRIT_S
- Lecture: bot_config.py:L2175 (get_float)
- Stockage: cfg.wd.opportunity_vol_age_crit_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_OPPORTUNITY_VOL_AGE_WARN_S
- Lecture: bot_config.py:L2172 (get_float)
- Stockage: cfg.wd.opportunity_vol_age_warn_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_PERSISTENCE_CYCLES
- Lecture: bot_config.py:L1989 (get_int)
- Stockage: cfg.wd.persistence_cycles
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_PERSISTENCE_S
- Lecture: bot_config.py:L1988 (get_float)
- Stockage: cfg.wd.persistence_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_PRIVATE_WS_ACK_CRIT_MS
- Lecture: bot_config.py:L2024 (get_int)
- Stockage: cfg.wd.private_ws_ack_crit_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_PRIVATE_WS_ACK_WARN_MS
- Lecture: bot_config.py:L2023 (get_int)
- Stockage: cfg.wd.private_ws_ack_warn_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_PRIVATE_WS_DEDUP_CRIT
- Lecture: bot_config.py:L2036 (get_float)
- Stockage: cfg.wd.private_ws_dedup_crit
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_PRIVATE_WS_DEDUP_WARN
- Lecture: bot_config.py:L2035 (get_float)
- Stockage: cfg.wd.private_ws_dedup_warn
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_PRIVATE_WS_ESCALATE_AFTER_CYCLES
- Lecture: bot_config.py:L2037 (get_int)
- Stockage: cfg.wd.private_ws_escalate_after_cycles
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_PRIVATE_WS_FILL_CRIT_MS
- Lecture: bot_config.py:L2026 (get_int)
- Stockage: cfg.wd.private_ws_fill_crit_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_PRIVATE_WS_FILL_WARN_MS
- Lecture: bot_config.py:L2025 (get_int)
- Stockage: cfg.wd.private_ws_fill_warn_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_PRIVATE_WS_HB_CRIT_S
- Lecture: bot_config.py:L2022 (get_int)
- Stockage: cfg.wd.private_ws_hb_crit_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_PRIVATE_WS_HB_WARN_S
- Lecture: bot_config.py:L2021 (get_int)
- Stockage: cfg.wd.private_ws_hb_warn_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_PRIVATE_WS_INTERVAL_S
- Lecture: bot_config.py:L2020 (get_float)
- Stockage: cfg.wd.private_ws_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_PRIVATE_WS_QUEUE_CRIT_RATIO
- Lecture: bot_config.py:L2030 (get_float)
- Stockage: cfg.wd.private_ws_queue_crit_ratio
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_PRIVATE_WS_QUEUE_WARN_RATIO
- Lecture: bot_config.py:L2027 (get_float)
- Stockage: cfg.wd.private_ws_queue_warn_ratio
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_PRIVATE_WS_RATE429_CRIT
- Lecture: bot_config.py:L2034 (get_float)
- Stockage: cfg.wd.private_ws_rate429_crit
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_PRIVATE_WS_RATE429_WARN
- Lecture: bot_config.py:L2033 (get_float)
- Stockage: cfg.wd.private_ws_rate429_warn
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_RM_ESCALATE_AFTER_CYCLES
- Lecture: bot_config.py:L2227 (get_int)
- Stockage: cfg.wd.rm_escalate_after_cycles
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_RM_HB_CRIT_S
- Lecture: bot_config.py:L2208 (get_int)
- Stockage: cfg.wd.rm_hb_crit_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_RM_HB_WARN_S
- Lecture: bot_config.py:L2207 (get_int)
- Stockage: cfg.wd.rm_hb_warn_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_RM_INTERVAL_S
- Lecture: bot_config.py:L2204 (get_float)
- Stockage: cfg.wd.rm_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_RM_QUEUEPOS_CRIT_PER_MIN
- Lecture: bot_config.py:L2222 (get_float)
- Stockage: cfg.wd.rm_queuepos_crit_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_RM_QUEUEPOS_WARN_PER_MIN
- Lecture: bot_config.py:L2219 (get_float)
- Stockage: cfg.wd.rm_queuepos_warn_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_RM_SEVERE_CRIT_S
- Lecture: bot_config.py:L2226 (get_int)
- Stockage: cfg.wd.rm_severe_crit_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_RM_SEVERE_WARN_S
- Lecture: bot_config.py:L2225 (get_int)
- Stockage: cfg.wd.rm_severe_warn_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_RM_SHADOW_BIAS_CRIT_BPS
- Lecture: bot_config.py:L2216 (get_float)
- Stockage: cfg.wd.rm_shadow_bias_crit_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_RM_SHADOW_BIAS_WARN_BPS
- Lecture: bot_config.py:L2213 (get_float)
- Stockage: cfg.wd.rm_shadow_bias_warn_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_RM_SLIP_AGE_CRIT_S
- Lecture: bot_config.py:L2210 (get_float)
- Stockage: cfg.wd.rm_slip_age_crit_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_RM_SLIP_AGE_WARN_S
- Lecture: bot_config.py:L2209 (get_float)
- Stockage: cfg.wd.rm_slip_age_warn_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_RM_TICK_CRIT_MS
- Lecture: bot_config.py:L2206 (get_int)
- Stockage: cfg.wd.rm_tick_crit_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_RM_TICK_WARN_MS
- Lecture: bot_config.py:L2205 (get_int)
- Stockage: cfg.wd.rm_tick_warn_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_RM_VOL_AGE_CRIT_S
- Lecture: bot_config.py:L2212 (get_float)
- Stockage: cfg.wd.rm_vol_age_crit_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_RM_VOL_AGE_WARN_S
- Lecture: bot_config.py:L2211 (get_float)
- Stockage: cfg.wd.rm_vol_age_warn_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_ROUTER_INTERVAL_S
- Lecture: bot_config.py:L1992 (get_float)
- Stockage: cfg.wd.router_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_SLIPPAGE_AGE_CRIT_S
- Lecture: bot_config.py:L2109 (get_float)
- Stockage: cfg.wd.slippage_age_crit_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_SLIPPAGE_AGE_WARN_S
- Lecture: bot_config.py:L2108 (get_float)
- Stockage: cfg.wd.slippage_age_warn_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_SLIPPAGE_ESCALATE_AFTER_CYCLES
- Lecture: bot_config.py:L2112 (get_int)
- Stockage: cfg.wd.slippage_escalate_after_cycles
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_SLIPPAGE_INTERVAL_S
- Lecture: bot_config.py:L2107 (get_float)
- Stockage: cfg.wd.slippage_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_SLIPPAGE_P95_WARN_BPS
- Lecture: bot_config.py:L2110 (get_float)
- Stockage: cfg.wd.slippage_p95_warn_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_SLIPPAGE_P99_CRIT_BPS
- Lecture: bot_config.py:L2111 (get_float)
- Stockage: cfg.wd.slippage_p99_crit_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_VOLATILITY_AGE_CRIT_S
- Lecture: bot_config.py:L2118 (get_float)
- Stockage: cfg.wd.volatility_age_crit_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_VOLATILITY_AGE_WARN_S
- Lecture: bot_config.py:L2117 (get_float)
- Stockage: cfg.wd.volatility_age_warn_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_VOLATILITY_ESCALATE_AFTER_CYCLES
- Lecture: bot_config.py:L2125 (get_int)
- Stockage: cfg.wd.volatility_escalate_after_cycles
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_VOLATILITY_HARD_CAP_BPS
- Lecture: bot_config.py:L2120 (get_float)
- Stockage: cfg.wd.volatility_hard_cap_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_VOLATILITY_INTERVAL_S
- Lecture: bot_config.py:L2116 (get_float)
- Stockage: cfg.wd.volatility_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_VOLATILITY_P95_WARN_BPS
- Lecture: bot_config.py:L2121 (get_float)
- Stockage: cfg.wd.volatility_p95_warn_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_VOLATILITY_P99_CRIT_BPS
- Lecture: bot_config.py:L2122 (get_float)
- Stockage: cfg.wd.volatility_p99_crit_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_VOLATILITY_SOFT_CAP_BPS
- Lecture: bot_config.py:L2119 (get_float)
- Stockage: cfg.wd.volatility_soft_cap_bps
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_VOLATILITY_Z_CRIT
- Lecture: bot_config.py:L2124 (get_float)
- Stockage: cfg.wd.volatility_z_crit
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_VOLATILITY_Z_WARN
- Lecture: bot_config.py:L2123 (get_float)
- Stockage: cfg.wd.volatility_z_warn
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_WS_PUBLIC_ESCALATE_AFTER_CYCLES
- Lecture: bot_config.py:L2016 (get_int)
- Stockage: cfg.wd.ws_public_escalate_after_cycles
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_WS_PUBLIC_HB_GAP_CRIT_S
- Lecture: bot_config.py:L2009 (get_int)
- Stockage: cfg.wd.ws_public_hb_gap_crit_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_WS_PUBLIC_HB_GAP_WARN_S
- Lecture: bot_config.py:L2008 (get_int)
- Stockage: cfg.wd.ws_public_hb_gap_warn_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_WS_PUBLIC_INTERVAL_S
- Lecture: bot_config.py:L2005 (get_float)
- Stockage: cfg.wd.ws_public_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_WS_PUBLIC_RESUB_CRIT_PER_MIN
- Lecture: bot_config.py:L2013 (get_int)
- Stockage: cfg.wd.ws_public_resub_crit_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_WS_PUBLIC_RESUB_WARN_PER_MIN
- Lecture: bot_config.py:L2010 (get_int)
- Stockage: cfg.wd.ws_public_resub_warn_per_min
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_WS_PUBLIC_STALE_CRIT_MS
- Lecture: bot_config.py:L2007 (get_int)
- Stockage: cfg.wd.ws_public_stale_crit_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WD_WS_PUBLIC_STALE_WARN_MS
- Lecture: bot_config.py:L2006 (get_int)
- Stockage: cfg.wd.ws_public_stale_warn_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WS_BACKOFF
- Lecture: bot_config.py:L1957 (get_dict)
- Stockage: cfg.ws_public.ws_backoff
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WS_BACKOFF_SEED
- Lecture: bot_config.py:L1958 (get_int)
- Stockage: cfg.ws_public.ws_backoff_seed
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WS_CONNECT_TIMEOUT_S
- Lecture: bot_config.py:L1962 (get_int)
- Stockage: cfg.ws_public.connect_timeout_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WS_DISABLED_EXCHANGES
- Lecture: bot_config.py:L1975 (get_list)
- Stockage: cfg.ws_public.disabled_exchanges
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WS_LIVE_CLOSE_TIMEOUT_S
- Lecture: bot_config.py:L3519 (get_int)
- Stockage: cfg.tests.WS_LIVE_CLOSE_TIMEOUT_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WS_LIVE_MIN_MSGS_PER_S
- Lecture: bot_config.py:L3518 (get_int)
- Stockage: cfg.tests.WS_LIVE_MIN_MSGS_PER_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WS_LIVE_WINDOW_S
- Lecture: bot_config.py:L3517 (get_int)
- Stockage: cfg.tests.WS_LIVE_WINDOW_S
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WS_OUT_QUEUE_PUT_TIMEOUT_S
- Lecture: bot_config.py:L1963 (get_float)
- Stockage: cfg.ws_public.out_queue_put_timeout_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WS_READ_TIMEOUT_S
- Lecture: bot_config.py:L1978 (get_int)
- Stockage: cfg.ws_public.read_timeout_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WS_STALENESS_INTERVAL_S
- Lecture: bot_config.py:L1966 (get_float)
- Stockage: cfg.ws_public.staleness_interval_s
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WS_STALENESS_SLO_S
- Lecture: bot_config.py:L1969 (get)
- Stockage: _slo_env
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)

### WS_UPDATE_PAIRS_JITTER_MS
- Lecture: bot_config.py:L1959 (get_int)
- Stockage: cfg.ws_public.update_pairs_jitter_ms
- Valeurs possibles: NON CONTRAINT
- Usages: LU MAIS USAGE NON PROUVÉ (POSSIBLE KNOB MORT)
