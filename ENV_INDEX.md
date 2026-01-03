# ENV_INDEX

Total keys: 641. Source unique: bot_config.py::BotConfig.from_env.

## Index par clé

### AC_CONFIG
- READ: bot_config.py:L1785
- WRITE: cfg.g.ac
- USED:
  - execution_engine.py:1341
  - execution_engine.py:1368
  - execution_engine.py:1369
- STATUS: USED

### ALLOWED_ROUTES
- READ: bot_config.py:L1742
- WRITE: cfg.g.allowed_routes
- USED:
  - opportunity_scanner.py:391
- STATUS: USED

### AUTOREFRESH_SEC
- READ: bot_config.py:L3512
- WRITE: cfg.dashboard.AUTOREFRESH_SEC
- USED:
  - .codex_env_extraction.json:10970
  - .codex_env_extraction.json:4994
  - .env.atlas:130
- STATUS: USED

### BALANCE_STALE_S
- READ: bot_config.py:L2087
- WRITE: cfg.wd.balance_stale_s
- USED:
  - .codex_env_extraction.json:1606
  - .codex_env_extraction.json:6620
  - .env.atlas:2596
  - balance_fetcher_watchdog.py:56
- STATUS: USED

### BF_PACER_EU
- READ: bot_config.py:L3310
- WRITE: cfg.balances.BF_PACER_EU
- USED:
  - .codex_env_extraction.json:4189
  - .codex_env_extraction.json:9944
  - .env.atlas:59
- STATUS: USED

### BF_PACER_US
- READ: bot_config.py:L3311
- WRITE: cfg.balances.BF_PACER_US
- USED:
  - .codex_env_extraction.json:4196
  - .codex_env_extraction.json:9953
  - .env.atlas:64
- STATUS: USED

### BF_REFRESH_INTERVAL_S
- READ: bot_config.py:L3301
- WRITE: cfg.balances.refresh_interval_s
- USED:
  - .codex_env_extraction.json:4133
  - .codex_env_extraction.json:9872
  - .env.atlas:69
- STATUS: USED

### BF_TTL_CACHE_S
- READ: bot_config.py:L3302
- WRITE: cfg.balances.ttl_cache_s
- USED:
  - .codex_env_extraction.json:4140
  - .codex_env_extraction.json:9881
  - .env.atlas:74
- STATUS: USED

### BF_WALLET_TYPES
- READ: bot_config.py:L3309
- WRITE: cfg.balances.wallet_types
- USED:
  - .codex_env_extraction.json:4182
  - .codex_env_extraction.json:9935
  - .env.atlas:79
- STATUS: USED

### BINANCE_REST_BASE
- READ: bot_config.py:L3303
- WRITE: cfg.balances.binance_rest_base
- USED:
  - .codex_env_extraction.json:4147
  - .codex_env_extraction.json:9890
  - .env.atlas:84
- STATUS: USED

### BOOT_SCANNER_PROXY_BUFFER_MAXLEN
- READ: bot_config.py:L1849
- WRITE: cfg.boot.scanner_proxy_buffer_maxlen
- USED:
  - .codex_env_extraction.json:5733
  - .codex_env_extraction.json:927
  - .env.atlas:111
- STATUS: USED

### BOOT_SCANNER_PROXY_MODE
- READ: bot_config.py:L1848
- WRITE: cfg.boot.scanner_proxy_mode
- USED:
  - .codex_env_extraction.json:5724
  - .codex_env_extraction.json:920
  - .env.atlas:116
- STATUS: USED

### BRANCH_BUDGETS_QUOTE
- READ: bot_config.py:L1754
- WRITE: cfg.g.branch_budgets_quote
- USED: none
- STATUS: NO_USAGE_FOUND

### BRANCH_PRIORITY
- READ: bot_config.py:L1753
- WRITE: cfg.g.branch_priority
- USED: none
- STATUS: NO_USAGE_FOUND

### BYBIT_API_BASE
- READ: bot_config.py:L3305
- WRITE: cfg.balances.bybit_api_base
- USED:
  - .codex_env_extraction.json:4161
  - .codex_env_extraction.json:9908
  - .env.atlas:89
- STATUS: USED

### CAPITAL_PROFILE
- READ: bot_config.py:L1740
- WRITE: cfg.g.capital_profile
- USED:
  - smoke_test_all.py:1264
  - smoke_test_all.py:2200
  - smoke_test_all.py:2425
- STATUS: TEST_ONLY

### COINBASE_API_BASE
- READ: bot_config.py:L3304
- WRITE: cfg.balances.coinbase_api_base
- USED:
  - .codex_env_extraction.json:4154
  - .codex_env_extraction.json:9899
  - .env.atlas:94
- STATUS: USED

### CONFIG_OVERRIDES
- READ: bot_config.py:L1877
- WRITE: cfg.overrides
- USED: none
- STATUS: NO_USAGE_FOUND

### DAILY_BUDGET_RESET_INTERVAL_S
- READ: bot_config.py:L2958
- WRITE: cfg.rm.daily_budget_reset_interval_s
- USED:
  - .codex_env_extraction.json:3230
  - .codex_env_extraction.json:8708
  - .env.atlas:1238
- STATUS: USED

### DAILY_STRATEGY_BUDGET_QUOTE
- READ: bot_config.py:L1980,2956
- WRITE: cfg.rm.daily_strategy_budget_quote
- USED:
  - .codex_env_extraction.json:3223
  - .codex_env_extraction.json:5071
  - .codex_env_extraction.json:8692
  - .codex_env_extraction.json:8699
  - .env.atlas:1243
- STATUS: USED

### DEFAULT_PRIVATE_WALLET
- READ: bot_config.py:L3306
- WRITE: cfg.balances.default_private_wallet
- USED:
  - .codex_env_extraction.json:4168
  - .codex_env_extraction.json:9917
  - .env.atlas:99
- STATUS: USED

### DEMO_MODE
- READ: bot_config.py:L3513
- WRITE: cfg.dashboard.DEMO_MODE
- USED:
  - .codex_env_extraction.json:10979
  - .codex_env_extraction.json:5001
  - .env.atlas:135
- STATUS: USED

### DEPLOYMENT_MODE
- READ: bot_config.py:L1730
- WRITE: cfg.g.deployment_mode
- USED:
  - smoke_test_all.py:1263
  - smoke_test_all.py:1829
  - smoke_test_all.py:2199
  - smoke_test_all.py:2424
- STATUS: TEST_ONLY

### DISCOVERY_BLACKLIST
- READ: bot_config.py:L2261,2643
- WRITE: cfg.discovery.blacklist
- USED:
  - .codex_env_extraction.json:2145
  - .codex_env_extraction.json:2754
  - .codex_env_extraction.json:7348
  - .codex_env_extraction.json:7355
  - .env.atlas:147
- STATUS: USED

### DISCOVERY_ENABLED
- READ: bot_config.py:L2259
- WRITE: cfg.discovery.enabled
- USED:
  - .codex_env_extraction.json:2131
  - .codex_env_extraction.json:7323
  - .env.atlas:152
  - smoke_test_all.py:2202
  - smoke_test_all.py:2427
  - smoke_test_all.py:595
- STATUS: USED

### DISCOVERY_ENABLED_EXCHANGES
- READ: bot_config.py:L2634
- WRITE: cfg.discovery.enabled_exchanges
- USED: none
- STATUS: NO_USAGE_FOUND

### DISCOVERY_EUR_QUOTE_VOLUME_FACTOR
- READ: bot_config.py:L2617
- WRITE: cfg.discovery.eur_quote_volume_factor
- USED:
  - .codex_env_extraction.json:11051
  - .codex_env_extraction.json:5099
  - .env.atlas:157
- STATUS: USED

### DISCOVERY_HTTP_TIMEOUT_S
- READ: bot_config.py:L1985
- WRITE: cfg.discovery.http_timeout_s
- USED:
  - .codex_env_extraction.json:1221
  - .codex_env_extraction.json:6125
  - .env.atlas:162
- STATUS: USED

### DISCOVERY_MAX_INFLIGHT
- READ: bot_config.py:L2231
- WRITE: cfg.discovery.max_inflight_requests
- USED:
  - .codex_env_extraction.json:2096
  - .codex_env_extraction.json:7250
  - .env.atlas:167
- STATUS: USED

### DISCOVERY_MIN_24H_VOLUME_USD
- READ: bot_config.py:L2244
- WRITE: cfg.discovery.min_24h_volume_usd
- USED:
  - .codex_env_extraction.json:11042
  - .codex_env_extraction.json:2110
  - .codex_env_extraction.json:5085
  - .codex_env_extraction.json:7275
  - .env.atlas:172
  - .env.atlas:177
  - pairs_discovery.py:479
- STATUS: USED

### DISCOVERY_MIN_24H_VOL_USD
- READ: bot_config.py:L2239
- WRITE: cfg.discovery.min_24h_volume_usd
- USED:
  - .codex_env_extraction.json:11042
  - .codex_env_extraction.json:2110
  - .codex_env_extraction.json:5085
  - .codex_env_extraction.json:7275
  - .env.atlas:172
  - .env.atlas:177
  - pairs_discovery.py:479
- STATUS: USED

### DISCOVERY_MIN_QUOTE_VOLUME_EUR
- READ: bot_config.py:L2254,2264
- WRITE: cfg.discovery.min_quote_volume_eur
- USED:
  - .codex_env_extraction.json:2124
  - .codex_env_extraction.json:7307
  - .env.atlas:182
- STATUS: USED

### DISCOVERY_MIN_QUOTE_VOLUME_FLOOR
- READ: bot_config.py:L2623
- WRITE: cfg.discovery.min_quote_volume_floor
- USED:
  - .codex_env_extraction.json:11060
  - .codex_env_extraction.json:5106
  - .env.atlas:187
- STATUS: USED

### DISCOVERY_MIN_QUOTE_VOLUME_USDC
- READ: bot_config.py:L2250,2263
- WRITE: cfg.discovery.min_quote_volume_usdc
- USED:
  - .codex_env_extraction.json:2117
  - .codex_env_extraction.json:7291
  - .env.atlas:192
- STATUS: USED

### DISCOVERY_QUOTES_ALLOWED
- READ: bot_config.py:L2232,2630
- WRITE: cfg.discovery.quotes_allowed
- USED:
  - .codex_env_extraction.json:2103
  - .codex_env_extraction.json:7259
  - .env.atlas:197
- STATUS: USED

### DISCOVERY_RETRY_POLICY
- READ: bot_config.py:L2230
- WRITE: cfg.discovery.retry_policy
- USED:
  - .codex_env_extraction.json:2089
  - .codex_env_extraction.json:7241
  - .env.atlas:202
- STATUS: USED

### DISCOVERY_WHITELIST
- READ: bot_config.py:L2260,2642
- WRITE: cfg.discovery.whitelist
- USED:
  - .codex_env_extraction.json:2138
  - .codex_env_extraction.json:2747
  - .codex_env_extraction.json:7332
  - .codex_env_extraction.json:7339
  - .env.atlas:207
- STATUS: USED

### ENABLED_EXCHANGES
- READ: bot_config.py:L1741
- WRITE: cfg.g.enabled_exchanges
- USED:
  - opportunity_scanner.py:390
  - smoke_test_all.py:1265
  - smoke_test_all.py:1306
  - smoke_test_all.py:1831
  - smoke_test_all.py:1993
  - smoke_test_all.py:2074
  - smoke_test_all.py:2203
  - smoke_test_all.py:2428
  - smoke_test_all.py:597
- STATUS: USED

### ENABLE_BRANCHES
- READ: bot_config.py:L1751,1752
- WRITE: cfg.g.enable_branches
- USED: none
- STATUS: NO_USAGE_FOUND

### ENABLE_JP
- READ: bot_config.py:L1734
- WRITE: cfg.g.enable_jp
- USED: none
- STATUS: NO_USAGE_FOUND

### ENABLE_MAKER_MAKER
- READ: bot_config.py:L2832
- WRITE: cfg.rm.enable_maker_maker
- USED:
  - .codex_env_extraction.json:2971
  - .codex_env_extraction.json:8370
  - .env.atlas:1248
- STATUS: USED

### ENABLE_MM
- READ: bot_config.py:L2806,2825
- WRITE: —
- USED: none
- STATUS: NO_USAGE_FOUND

### ENABLE_REB
- READ: bot_config.py:L2807,2829
- WRITE: —
- USED: none
- STATUS: NO_USAGE_FOUND

### ENABLE_TM
- READ: bot_config.py:L2805,2821
- WRITE: —
- USED: none
- STATUS: NO_USAGE_FOUND

### ENABLE_TT
- READ: bot_config.py:L2804,2817
- WRITE: —
- USED: none
- STATUS: NO_USAGE_FOUND

### ENABLE_WS_LIVE
- READ: bot_config.py:L3516
- WRITE: cfg.tests.ENABLE_WS_LIVE
- USED:
  - .codex_env_extraction.json:10988
  - .codex_env_extraction.json:5008
  - .env.atlas:2468
- STATUS: USED

### ENGINE_BLOCKED_MS
- READ: bot_config.py:L2084
- WRITE: cfg.wd.engine_blocked_ms
- USED:
  - .codex_env_extraction.json:1592
  - .codex_env_extraction.json:6602
  - .env.atlas:2601
  - simulator_execution_watchdog.py:155
- STATUS: USED

### ENGINE_CIRCUIT_ESCALATION_WINDOW_S
- READ: bot_config.py:L3223
- WRITE: cfg.engine.circuit_escalation_window_s
- USED:
  - .codex_env_extraction.json:3860
  - .codex_env_extraction.json:9539
  - .env.atlas:221
- STATUS: USED

### ENGINE_CIRCUIT_MUTE_ESCALATION
- READ: bot_config.py:L3225
- WRITE: cfg.engine.circuit_mute_escalation
- USED:
  - .codex_env_extraction.json:3867
  - .codex_env_extraction.json:9548
  - .env.atlas:226
- STATUS: USED

### ENGINE_CIRCUIT_MUTE_MAX_S
- READ: bot_config.py:L3228
- WRITE: cfg.engine.circuit_mute_max_s
- USED:
  - .codex_env_extraction.json:3881
  - .codex_env_extraction.json:9566
  - .env.atlas:231
- STATUS: USED

### ENGINE_CIRCUIT_MUTE_MIN_S
- READ: bot_config.py:L3227
- WRITE: cfg.engine.circuit_mute_min_s
- USED:
  - .codex_env_extraction.json:3874
  - .codex_env_extraction.json:9557
  - .env.atlas:236
- STATUS: USED

### ENGINE_CIRCUIT_MUTE_S_MM
- READ: bot_config.py:L3230
- WRITE: cfg.engine.circuit_mute_s_mm
- USED:
  - .codex_env_extraction.json:3895
  - .codex_env_extraction.json:9584
  - .env.atlas:241
- STATUS: USED

### ENGINE_CIRCUIT_MUTE_S_TM
- READ: bot_config.py:L3229
- WRITE: cfg.engine.circuit_mute_s_tm
- USED:
  - .codex_env_extraction.json:3888
  - .codex_env_extraction.json:9575
  - .env.atlas:246
- STATUS: USED

### ENGINE_DEPTH_LEVELS_CHECK
- READ: bot_config.py:L3219
- WRITE: cfg.engine.depth_levels_check
- USED:
  - .codex_env_extraction.json:3832
  - .codex_env_extraction.json:9503
  - .env.atlas:251
- STATUS: USED

### ENGINE_DEPTH_MIN_QUOTE_MM
- READ: bot_config.py:L3218
- WRITE: cfg.engine.depth_min_quote_mm
- USED:
  - .codex_env_extraction.json:3825
  - .codex_env_extraction.json:9494
  - .env.atlas:256
- STATUS: USED

### ENGINE_DEPTH_MIN_QUOTE_TM
- READ: bot_config.py:L3217
- WRITE: cfg.engine.depth_min_quote_tm
- USED:
  - .codex_env_extraction.json:3818
  - .codex_env_extraction.json:9485
  - .env.atlas:261
- STATUS: USED

### ENGINE_DEPTH_MIN_QUOTE_TT
- READ: bot_config.py:L3216
- WRITE: cfg.engine.depth_min_quote_tt
- USED:
  - .codex_env_extraction.json:3811
  - .codex_env_extraction.json:9476
  - .env.atlas:266
- STATUS: USED

### ENGINE_ENFORCE_CLIENT_OID_DETERMINISTIC
- READ: bot_config.py:L3121
- WRITE: cfg.engine.ff_enforce_client_oid_deterministic
- USED:
  - .codex_env_extraction.json:3587
  - .codex_env_extraction.json:9168
  - .env.atlas:271
  - smoke_test_all.py:1268
  - smoke_test_all.py:1427
- STATUS: USED

### ENGINE_FAIL_CLOSED_IDEMPOTENCE
- READ: bot_config.py:L3125
- WRITE: cfg.engine.ff_fail_closed_idempotence
- USED:
  - .codex_env_extraction.json:3594
  - .codex_env_extraction.json:9177
  - .env.atlas:276
  - smoke_test_all.py:1269
  - smoke_test_all.py:1428
- STATUS: USED

### ENGINE_FAIL_CLOSED_ON_RM_OVERRIDE_EXCEPTION
- READ: bot_config.py:L3153
- WRITE: cfg.engine.fail_closed_on_rm_override_exception
- USED:
  - .codex_env_extraction.json:3643
  - .codex_env_extraction.json:9240
  - .env.atlas:281
- STATUS: USED

### ENGINE_FF_ENFORCE_PREEMPTION
- READ: bot_config.py:L3133
- WRITE: cfg.engine.ff_enforce_preemption
- USED:
  - .codex_env_extraction.json:3608
  - .codex_env_extraction.json:9195
  - .env.atlas:286
- STATUS: USED

### ENGINE_FF_HEDGE_FAST_LANE
- READ: bot_config.py:L3129
- WRITE: cfg.engine.ff_hedge_fast_lane
- USED:
  - .codex_env_extraction.json:3601
  - .codex_env_extraction.json:9186
  - .env.atlas:291
- STATUS: USED

### ENGINE_FF_MM_ENABLED
- READ: bot_config.py:L3141
- WRITE: cfg.engine.ff_mm_enabled
- USED:
  - .codex_env_extraction.json:3622
  - .codex_env_extraction.json:9213
  - .env.atlas:296
- STATUS: USED

### ENGINE_FF_MM_OPPORTUNISTIC_GATING_ENFORCED
- READ: bot_config.py:L3145
- WRITE: cfg.engine.ff_mm_opportunistic_gating_enforced
- USED:
  - .codex_env_extraction.json:3629
  - .codex_env_extraction.json:9222
  - .env.atlas:301
- STATUS: USED

### ENGINE_FF_REB_ENABLED
- READ: bot_config.py:L3149
- WRITE: cfg.engine.ff_reb_enabled
- USED:
  - .codex_env_extraction.json:3636
  - .codex_env_extraction.json:9231
  - .env.atlas:306
- STATUS: USED

### ENGINE_FF_TM_ENABLED
- READ: bot_config.py:L3137
- WRITE: cfg.engine.ff_tm_enabled
- USED:
  - .codex_env_extraction.json:3615
  - .codex_env_extraction.json:9204
  - .env.atlas:311
- STATUS: USED

### ENGINE_FREEZE_TM_ON_VOL
- READ: bot_config.py:L3116
- WRITE: cfg.engine.freeze_tm_on_vol
- USED:
  - .codex_env_extraction.json:3573
  - .codex_env_extraction.json:9150
  - .env.atlas:316
- STATUS: USED

### ENGINE_HTTP_TIMEOUT_S
- READ: bot_config.py:L3100
- WRITE: cfg.engine.http_timeout_s
- USED:
  - .codex_env_extraction.json:3531
  - .codex_env_extraction.json:9098
  - .env.atlas:321
- STATUS: USED

### ENGINE_IDEMPOTENCY_TTL_S
- READ: bot_config.py:L3117
- WRITE: cfg.engine.idempotency_ttl_s
- USED:
  - .codex_env_extraction.json:3580
  - .codex_env_extraction.json:9159
  - .env.atlas:326
- STATUS: USED

### ENGINE_INFLIGHT_MAX_BY_EXCHANGE
- READ: bot_config.py:L3237
- WRITE: cfg.engine.inflight_max_by_exchange
- USED:
  - .codex_env_extraction.json:3482
  - .codex_env_extraction.json:3909
  - .codex_env_extraction.json:9039
  - .codex_env_extraction.json:9593
  - .env.atlas:331
  - .env.atlas:336
- STATUS: USED

### ENGINE_INFLIGHT_MAX_BY_EXCHANGE_BY_PROFILE
- READ: bot_config.py:L3083
- WRITE: cfg.engine.inflight_max_by_exchange_by_profile
- USED:
  - .codex_env_extraction.json:3482
  - .codex_env_extraction.json:9039
  - .env.atlas:336
- STATUS: USED

### ENGINE_INFLIGHT_RESERVED_CANCEL_BY_EXCHANGE_BY_PROFILE
- READ: bot_config.py:L3091
- WRITE: cfg.engine.inflight_reserved_cancel_by_exchange_by_profile
- USED:
  - .codex_env_extraction.json:3496
  - .codex_env_extraction.json:9057
  - .env.atlas:341
- STATUS: USED

### ENGINE_INFLIGHT_RESERVED_HEDGE_BY_EXCHANGE_BY_PROFILE
- READ: bot_config.py:L3087
- WRITE: cfg.engine.inflight_reserved_hedge_by_exchange_by_profile
- USED:
  - .codex_env_extraction.json:3489
  - .codex_env_extraction.json:9048
  - .env.atlas:346
- STATUS: USED

### ENGINE_ORDER_TIMEOUT_S
- READ: bot_config.py:L3097,3099
- WRITE: cfg.engine.order_timeout_s
- USED:
  - .codex_env_extraction.json:3510
  - .codex_env_extraction.json:3524
  - .codex_env_extraction.json:9082
  - .codex_env_extraction.json:9089
  - .env.atlas:351
- STATUS: USED

### ENGINE_PACER_BAD_SCORE_THRESHOLD
- READ: bot_config.py:L3171
- WRITE: cfg.engine.pacer_knobs.bad_score_threshold
- USED: none
- STATUS: NO_USAGE_FOUND

### ENGINE_PACER_DEESCALATE_GOOD_WINDOWS
- READ: bot_config.py:L3162
- WRITE: cfg.engine.pacer_knobs.deescalate_good_windows
- USED: none
- STATUS: NO_USAGE_FOUND

### ENGINE_PACER_DEFAULT_TARGETS
- READ: bot_config.py:L3210
- WRITE: cfg.engine.pacer_knobs.default_targets
- USED: none
- STATUS: NO_USAGE_FOUND

### ENGINE_PACER_ESCALATE_BAD_WINDOWS
- READ: bot_config.py:L3159
- WRITE: cfg.engine.pacer_knobs.escalate_bad_windows
- USED: none
- STATUS: NO_USAGE_FOUND

### ENGINE_PACER_GOOD_SCORE_THRESHOLD
- READ: bot_config.py:L3168
- WRITE: cfg.engine.pacer_knobs.good_score_threshold
- USED: none
- STATUS: NO_USAGE_FOUND

### ENGINE_PACER_HOLD_TIME_FLAGS_SECS
- READ: bot_config.py:L3165
- WRITE: cfg.engine.pacer_knobs.hold_time_flags_secs
- USED: none
- STATUS: NO_USAGE_FOUND

### ENGINE_PACER_INIT
- READ: bot_config.py:L3184,3207
- WRITE: cfg.engine.pacer_init_ms
- USED:
  - .codex_env_extraction.json:3790
  - .codex_env_extraction.json:5190
  - .codex_env_extraction.json:9378
  - .codex_env_extraction.json:9442
- STATUS: USED

### ENGINE_PACER_INIT_MS
- READ: bot_config.py:L3180,3206
- WRITE: cfg.engine.pacer_init_ms
- USED:
  - .codex_env_extraction.json:3790
  - .codex_env_extraction.json:5190
  - .codex_env_extraction.json:9378
  - .codex_env_extraction.json:9442
- STATUS: USED

### ENGINE_PACER_JITTER
- READ: bot_config.py:L3185,3209
- WRITE: cfg.engine.pacer_jitter_ms
- USED:
  - .codex_env_extraction.json:3797
  - .codex_env_extraction.json:5197
  - .codex_env_extraction.json:9394
  - .codex_env_extraction.json:9458
- STATUS: USED

### ENGINE_PACER_JITTER_MS
- READ: bot_config.py:L3181,3208
- WRITE: cfg.engine.pacer_jitter_ms
- USED:
  - .codex_env_extraction.json:3797
  - .codex_env_extraction.json:5197
  - .codex_env_extraction.json:9394
  - .codex_env_extraction.json:9458
- STATUS: USED

### ENGINE_PACER_MAX
- READ: bot_config.py:L3183,3205
- WRITE: cfg.engine.pacer_max_ms
- USED:
  - .codex_env_extraction.json:3783
  - .codex_env_extraction.json:5183
  - .codex_env_extraction.json:9362
  - .codex_env_extraction.json:9426
- STATUS: USED

### ENGINE_PACER_MAX_MS
- READ: bot_config.py:L3179,3204
- WRITE: cfg.engine.pacer_max_ms
- USED:
  - .codex_env_extraction.json:3783
  - .codex_env_extraction.json:5183
  - .codex_env_extraction.json:9362
  - .codex_env_extraction.json:9426
- STATUS: USED

### ENGINE_PACER_MIN
- READ: bot_config.py:L3182,3203
- WRITE: cfg.engine.pacer_min_ms
- USED:
  - .codex_env_extraction.json:3776
  - .codex_env_extraction.json:5176
  - .codex_env_extraction.json:9346
  - .codex_env_extraction.json:9410
- STATUS: USED

### ENGINE_PACER_MIN_MS
- READ: bot_config.py:L3178,3202
- WRITE: cfg.engine.pacer_min_ms
- USED:
  - .codex_env_extraction.json:3776
  - .codex_env_extraction.json:5176
  - .codex_env_extraction.json:9346
  - .codex_env_extraction.json:9410
- STATUS: USED

### ENGINE_PACER_TARGETS
- READ: bot_config.py:L2572
- WRITE: —
- USED: none
- STATUS: NO_USAGE_FOUND

### ENGINE_PACER_WARMUP_SECS
- READ: bot_config.py:L3158
- WRITE: cfg.engine.pacer_knobs.warmup_secs
- USED: none
- STATUS: NO_USAGE_FOUND

### ENGINE_PACER_WEIGHT_ACK
- READ: bot_config.py:L3174
- WRITE: cfg.engine.pacer_knobs.weight_ack
- USED: none
- STATUS: NO_USAGE_FOUND

### ENGINE_PACER_WEIGHT_DRAIN
- READ: bot_config.py:L3177
- WRITE: cfg.engine.pacer_knobs.weight_drain
- USED: none
- STATUS: NO_USAGE_FOUND

### ENGINE_PACER_WEIGHT_ERR
- READ: bot_config.py:L3176
- WRITE: cfg.engine.pacer_knobs.weight_err
- USED: none
- STATUS: NO_USAGE_FOUND

### ENGINE_PACER_WEIGHT_LAG
- READ: bot_config.py:L3175
- WRITE: cfg.engine.pacer_knobs.weight_lag
- USED: none
- STATUS: NO_USAGE_FOUND

### ENGINE_POD_MAP
- READ: bot_config.py:L1736,3424
- WRITE: cfg.g.engine_pod_map
- USED: none
- STATUS: NO_USAGE_FOUND

### ENGINE_PRICE_BAND_BPS_CAP
- READ: bot_config.py:L3221
- WRITE: cfg.engine.price_band_bps_cap
- USED:
  - .codex_env_extraction.json:3846
  - .codex_env_extraction.json:9521
  - .env.atlas:356
- STATUS: USED

### ENGINE_PRICE_BAND_BPS_FLOOR
- READ: bot_config.py:L3220
- WRITE: cfg.engine.price_band_bps_floor
- USED:
  - .codex_env_extraction.json:3839
  - .codex_env_extraction.json:9512
  - .env.atlas:361
- STATUS: USED

### ENGINE_QUEUE_MAX
- READ: bot_config.py:L2082
- WRITE: cfg.engine.engine_queue_max
- USED:
  - .codex_env_extraction.json:1585
  - .codex_env_extraction.json:6593
  - .env.atlas:366
- STATUS: USED

### ENGINE_TM_EXPOSURE_TTL_MS
- READ: bot_config.py:L3105
- WRITE: cfg.engine.tm_exposure_ttl_ms
- USED: none
- STATUS: NO_USAGE_FOUND

### ENGINE_TM_QPOS_MAX_ETA_MS
- READ: bot_config.py:L3110,3112
- WRITE: cfg.engine.tm_queuepos_max_eta_ms
- USED:
  - .codex_env_extraction.json:3545
  - .codex_env_extraction.json:3552
  - .codex_env_extraction.json:9116
  - .codex_env_extraction.json:9123
  - .env.atlas:371
- STATUS: USED

### ENGINE_TT_MAX_SKEW_MS
- READ: bot_config.py:L3096,3098
- WRITE: cfg.engine.tt_max_skew_ms
- USED:
  - .codex_env_extraction.json:3503
  - .codex_env_extraction.json:3517
  - .codex_env_extraction.json:9066
  - .codex_env_extraction.json:9073
  - .env.atlas:376
- STATUS: USED

### ENGINE_VOL_HARD_CAP_BPS
- READ: bot_config.py:L3115
- WRITE: cfg.engine.vol_hard_cap_bps
- USED:
  - .codex_env_extraction.json:3566
  - .codex_env_extraction.json:9141
  - .env.atlas:381
- STATUS: USED

### ENGINE_VOL_PRICE_BAND_K
- READ: bot_config.py:L3222
- WRITE: cfg.engine.vol_price_band_k
- USED:
  - .codex_env_extraction.json:3853
  - .codex_env_extraction.json:9530
  - .env.atlas:386
- STATUS: USED

### ENGINE_VOL_SOFT_CAP_BPS
- READ: bot_config.py:L3114
- WRITE: cfg.engine.vol_soft_cap_bps
- USED:
  - .codex_env_extraction.json:3559
  - .codex_env_extraction.json:9132
  - .env.atlas:391
- STATUS: USED

### ENGINE_WORKERS_BY_PROFILE
- READ: bot_config.py:L3079,3233
- WRITE: cfg.engine.workers_by_profile
- USED:
  - .codex_env_extraction.json:3475
  - .codex_env_extraction.json:3902
  - .codex_env_extraction.json:9023
  - .codex_env_extraction.json:9030
  - .env.atlas:396
- STATUS: USED

### EU_LATENCY
- READ: bot_config.py:L1738
- WRITE: cfg.g.eu_latency
- USED: none
- STATUS: NO_USAGE_FOUND

### EXCHANGE_REGION_MAP
- READ: bot_config.py:L3420
- WRITE: cfg.g.exchange_region_map
- USED: none
- STATUS: NO_USAGE_FOUND

### EXPOSE_METRICS_ON_9110
- READ: bot_config.py:L1843
- WRITE: cfg.obs.expose_metrics_on_status
- USED:
  - .codex_env_extraction.json:5697
  - .codex_env_extraction.json:899
  - .env.atlas:805
  - smoke_test_all.py:2211
  - smoke_test_all.py:2437
  - smoke_test_all.py:612
- STATUS: USED

### FEATURE_SWITCHES
- READ: bot_config.py:L1838
- WRITE: cfg.g.feature_switches
- USED:
  - execution_engine.py:1365
  - smoke_test_all.py:1830
  - smoke_test_all.py:2204
  - smoke_test_all.py:2429
  - smoke_test_all.py:603
- STATUS: USED

### FEE_REALITY_CHECK_THRESHOLD_BPS
- READ: bot_config.py:L3345
- WRITE: cfg.slip.fee_reality_check_threshold_bps
- USED:
  - .codex_env_extraction.json:10187
  - .codex_env_extraction.json:4378
  - .env.atlas:2414
- STATUS: USED

### FEE_SYNC_BACKOFF_INITIAL_S
- READ: bot_config.py:L3340
- WRITE: cfg.slip.fee_sync_backoff_initial_s
- USED:
  - .codex_env_extraction.json:10151
  - .codex_env_extraction.json:4350
  - .env.atlas:2419
- STATUS: USED

### FEE_SYNC_BACKOFF_MAX_S
- READ: bot_config.py:L3342
- WRITE: cfg.slip.fee_sync_backoff_max_s
- USED:
  - .codex_env_extraction.json:10160
  - .codex_env_extraction.json:4357
  - .env.atlas:2424
- STATUS: USED

### FEE_SYNC_JITTER_S
- READ: bot_config.py:L3344
- WRITE: cfg.slip.fee_sync_jitter_s
- USED:
  - .codex_env_extraction.json:10178
  - .codex_env_extraction.json:4371
  - .env.atlas:2429
- STATUS: USED

### FEE_SYNC_MAX_CONCURRENCY
- READ: bot_config.py:L3339
- WRITE: cfg.slip.fee_sync_max_concurrency
- USED:
  - .codex_env_extraction.json:10142
  - .codex_env_extraction.json:4343
  - .env.atlas:2434
- STATUS: USED

### FEE_SYNC_MAX_RETRIES
- READ: bot_config.py:L3343
- WRITE: cfg.slip.fee_sync_max_retries
- USED:
  - .codex_env_extraction.json:10169
  - .codex_env_extraction.json:4364
  - .env.atlas:2439
- STATUS: USED

### FF_ENFORCE_PREEMPTION
- READ: bot_config.py:L2841
- WRITE: cfg.rm.ff_enforce_preemption
- USED:
  - .codex_env_extraction.json:2992
  - .codex_env_extraction.json:8397
  - .env.atlas:1253
- STATUS: USED

### FF_FAIL_CLOSED_LOGGING
- READ: bot_config.py:L3435
- WRITE: cfg.lhm.ff_fail_closed_logging
- USED:
  - .codex_env_extraction.json:10601
  - .codex_env_extraction.json:4707
  - .env.atlas:554
- STATUS: USED

### FF_HEDGE_FAST_LANE
- READ: bot_config.py:L2845
- WRITE: cfg.rm.ff_hedge_fast_lane
- USED:
  - .codex_env_extraction.json:2999
  - .codex_env_extraction.json:8406
  - .env.atlas:1258
- STATUS: USED

### FF_LOGGING_CRITICAL_STREAMS
- READ: bot_config.py:L3436
- WRITE: cfg.lhm.ff_logging_critical_streams
- USED:
  - .codex_env_extraction.json:10610
  - .codex_env_extraction.json:4714
  - .env.atlas:559
- STATUS: USED

### FF_MM_ENABLED
- READ: bot_config.py:L2853
- WRITE: cfg.rm.ff_mm_enabled
- USED:
  - .codex_env_extraction.json:3013
  - .codex_env_extraction.json:8424
  - .env.atlas:1263
- STATUS: USED

### FF_MM_OPPORTUNISTIC_GATING_ENFORCED
- READ: bot_config.py:L2857
- WRITE: cfg.rm.ff_mm_opportunistic_gating_enforced
- USED:
  - .codex_env_extraction.json:3020
  - .codex_env_extraction.json:8433
  - .env.atlas:1268
- STATUS: USED

### FF_REB_ENABLED
- READ: bot_config.py:L2861
- WRITE: cfg.rm.ff_reb_enabled
- USED:
  - .codex_env_extraction.json:3027
  - .codex_env_extraction.json:8442
  - .env.atlas:1273
- STATUS: USED

### FF_TM_ENABLED
- READ: bot_config.py:L2849
- WRITE: cfg.rm.ff_tm_enabled
- USED:
  - .codex_env_extraction.json:3006
  - .codex_env_extraction.json:8415
  - .env.atlas:1278
- STATUS: USED

### FF_TRADING_STATE_UNIFIED
- READ: bot_config.py:L2837
- WRITE: cfg.rm.ff_trading_state_unified
- USED:
  - .codex_env_extraction.json:2985
  - .codex_env_extraction.json:8388
  - .env.atlas:1283
- STATUS: USED

### FF_TRUTH_FAIL_CLOSED
- READ: bot_config.py:L3444
- WRITE: cfg.lhm.ff_truth_fail_closed
- USED:
  - .codex_env_extraction.json:10628
  - .codex_env_extraction.json:4728
  - .env.atlas:564
- STATUS: USED

### FF_TRUTH_MODEL_ENABLED
- READ: bot_config.py:L3440
- WRITE: cfg.lhm.ff_truth_model_enabled
- USED:
  - .codex_env_extraction.json:10619
  - .codex_env_extraction.json:4721
  - .env.atlas:569
- STATUS: USED

### FLOW_SAFETY_PROFILES
- READ: bot_config.py:L2606
- WRITE: cfg.flow_safety.profiles
- USED:
  - .codex_env_extraction.json:2726
  - .codex_env_extraction.json:8084
  - .env.atlas:410
- STATUS: USED

### FRONTLOAD_WEIGHTS
- READ: bot_config.py:L1756
- WRITE: cfg.g.frontload_weights
- USED: none
- STATUS: NO_USAGE_FOUND

### FULL_RESTART_CAP_PER_HOUR
- READ: bot_config.py:L1802
- WRITE: cfg.g.full_restart_cap_per_hour
- USED: none
- STATUS: NO_USAGE_FOUND

### FULL_RESTART_COOLDOWN_S
- READ: bot_config.py:L1803
- WRITE: cfg.g.full_restart_cooldown_s
- USED: none
- STATUS: NO_USAGE_FOUND

### FULL_RESTART_MUTE_S
- READ: bot_config.py:L1804
- WRITE: cfg.g.full_restart_mute_s
- USED: none
- STATUS: NO_USAGE_FOUND

### GLOBAL_KILL_SWITCH
- READ: bot_config.py:L2955
- WRITE: cfg.rm.global_kill_switch
- USED:
  - .codex_env_extraction.json:3216
  - .codex_env_extraction.json:8683
  - .env.atlas:1288
- STATUS: USED

### GUARDS
- READ: bot_config.py:L1758
- WRITE: cfg.g.guards
- USED: none
- STATUS: NO_USAGE_FOUND

### INVENTORY_CAP_QUOTE
- READ: bot_config.py:L2294
- WRITE: cfg.rm.inventory_cap_quote
- USED: none
- STATUS: NO_USAGE_FOUND

### INVENTORY_CAP_USD
- READ: bot_config.py:L2296
- WRITE: cfg.rm.inventory_cap_quote
- USED: none
- STATUS: NO_USAGE_FOUND

### JP_LATENCY
- READ: bot_config.py:L1739
- WRITE: cfg.g.jp_latency
- USED: none
- STATUS: NO_USAGE_FOUND

### LHM_DB_LANE_BATCH_MAX
- READ: bot_config.py:L3466
- WRITE: cfg.lhm.LHM_DB_LANE_BATCH_MAX
- USED:
  - .codex_env_extraction.json:10763
  - .codex_env_extraction.json:4833
  - .env.atlas:574
- STATUS: USED

### LHM_DB_LANE_DRAIN_MS
- READ: bot_config.py:L3467
- WRITE: cfg.lhm.LHM_DB_LANE_DRAIN_MS
- USED:
  - .codex_env_extraction.json:10772
  - .codex_env_extraction.json:4840
  - .env.atlas:579
- STATUS: USED

### LHM_DB_LANE_DROP_WHEN_FULL
- READ: bot_config.py:L3468
- WRITE: cfg.lhm.LHM_DB_LANE_DROP_WHEN_FULL
- USED:
  - .codex_env_extraction.json:10781
  - .codex_env_extraction.json:4847
  - .env.atlas:584
- STATUS: USED

### LHM_DB_LANE_ENABLED
- READ: bot_config.py:L3464
- WRITE: cfg.lhm.LHM_DB_LANE_ENABLED
- USED:
  - .codex_env_extraction.json:10745
  - .codex_env_extraction.json:4819
  - .env.atlas:589
- STATUS: USED

### LHM_DB_LANE_Q_MAX
- READ: bot_config.py:L3465
- WRITE: cfg.lhm.LHM_DB_LANE_Q_MAX
- USED:
  - .codex_env_extraction.json:10754
  - .codex_env_extraction.json:4826
  - .env.atlas:594
- STATUS: USED

### LHM_DB_MAX_AGE_S
- READ: bot_config.py:L3463
- WRITE: cfg.lhm.LHM_DB_MAX_AGE_S
- USED:
  - .codex_env_extraction.json:10736
  - .codex_env_extraction.json:4812
  - .env.atlas:599
- STATUS: USED

### LHM_DB_MAX_BYTES
- READ: bot_config.py:L3462
- WRITE: cfg.lhm.LHM_DB_MAX_BYTES
- USED:
  - .codex_env_extraction.json:10727
  - .codex_env_extraction.json:4805
  - .env.atlas:604
- STATUS: USED

### LHM_DB_NAME
- READ: bot_config.py:L3461
- WRITE: cfg.lhm.LHM_DB_NAME
- USED:
  - .codex_env_extraction.json:10718
  - .codex_env_extraction.json:4798
  - .env.atlas:609
- STATUS: USED

### LHM_DISK_CRIT_PCT
- READ: bot_config.py:L3457
- WRITE: cfg.lhm.LHM_DISK_CRIT_PCT
- USED:
  - .codex_env_extraction.json:10700
  - .codex_env_extraction.json:4784
  - .env.atlas:614
- STATUS: USED

### LHM_DISK_WARN_PCT
- READ: bot_config.py:L3456
- WRITE: cfg.lhm.LHM_DISK_WARN_PCT
- USED:
  - .codex_env_extraction.json:10691
  - .codex_env_extraction.json:4777
  - .env.atlas:619
- STATUS: USED

### LHM_DROP_LOG_SAMPLE_RATE
- READ: bot_config.py:L3450
- WRITE: cfg.lhm.LHM_DROP_LOG_SAMPLE_RATE
- USED:
  - .codex_env_extraction.json:10655
  - .codex_env_extraction.json:4749
  - .env.atlas:624
- STATUS: USED

### LHM_DROP_WHEN_FULL
- READ: bot_config.py:L3432
- WRITE: cfg.lhm.LHM_DROP_WHEN_FULL
- USED:
  - .codex_env_extraction.json:10574
  - .codex_env_extraction.json:4686
  - .env.atlas:629
- STATUS: USED

### LHM_HIGH_WATERMARK_RATIO
- READ: bot_config.py:L3433
- WRITE: cfg.lhm.LHM_HIGH_WATERMARK_RATIO
- USED:
  - .codex_env_extraction.json:10583
  - .codex_env_extraction.json:4693
  - .env.atlas:634
- STATUS: USED

### LHM_JSONL_MAX_AGE_S
- READ: bot_config.py:L3455
- WRITE: cfg.lhm.LHM_JSONL_MAX_AGE_S
- USED:
  - .codex_env_extraction.json:10682
  - .codex_env_extraction.json:4770
  - .env.atlas:639
- STATUS: USED

### LHM_JSONL_MAX_BYTES
- READ: bot_config.py:L3454
- WRITE: cfg.lhm.LHM_JSONL_MAX_BYTES
- USED:
  - .codex_env_extraction.json:10673
  - .codex_env_extraction.json:4763
  - .env.atlas:644
- STATUS: USED

### LHM_JSONL_QUEUE_CAP
- READ: bot_config.py:L3453
- WRITE: cfg.lhm.LHM_JSONL_QUEUE_CAP
- USED:
  - .codex_env_extraction.json:10664
  - .codex_env_extraction.json:4756
  - .env.atlas:649
  - logger_historique_manager.py:386
- STATUS: USED

### LHM_MAX_QUEUE_PLATEAU_S
- READ: bot_config.py:L3434
- WRITE: cfg.lhm.LHM_MAX_QUEUE_PLATEAU_S
- USED:
  - .codex_env_extraction.json:10592
  - .codex_env_extraction.json:4700
  - .env.atlas:654
- STATUS: USED

### LHM_MM_SAMPLING_CANCELS
- READ: bot_config.py:L3449
- WRITE: cfg.lhm.LHM_MM_SAMPLING_CANCELS
- USED:
  - .codex_env_extraction.json:10646
  - .codex_env_extraction.json:4742
  - .env.atlas:659
- STATUS: USED

### LHM_MM_SAMPLING_QUOTES
- READ: bot_config.py:L3448
- WRITE: cfg.lhm.LHM_MM_SAMPLING_QUOTES
- USED:
  - .codex_env_extraction.json:10637
  - .codex_env_extraction.json:4735
  - .env.atlas:664
- STATUS: USED

### LHM_OPPORTUNITY_DROP_WHEN_FULL
- READ: bot_config.py:L3489
- WRITE: cfg.lhm.LHM_OPPORTUNITY_DROP_WHEN_FULL
- USED:
  - .codex_env_extraction.json:10889
  - .codex_env_extraction.json:4931
  - .env.atlas:669
- STATUS: USED

### LHM_OPPORTUNITY_QUEUE_MAX
- READ: bot_config.py:L3486
- WRITE: cfg.lhm.LHM_OPPORTUNITY_QUEUE_MAX
- USED:
  - .codex_env_extraction.json:10880
  - .codex_env_extraction.json:4924
  - .env.atlas:674
- STATUS: USED

### LHM_OUT_DIR
- READ: bot_config.py:L3428
- WRITE: cfg.lhm.out_dir
- USED:
  - .codex_env_extraction.json:10547
  - .codex_env_extraction.json:4658
  - .env.atlas:679
- STATUS: USED

### LHM_PNL_RECO_DEFAULT_REGION
- READ: bot_config.py:L3502
- WRITE: cfg.lhm.LHM_PNL_RECO_DEFAULT_REGION
- USED:
  - .codex_env_extraction.json:10934
  - .codex_env_extraction.json:4966
  - .env.atlas:684
- STATUS: USED

### LHM_PNL_RECO_ENABLED
- READ: bot_config.py:L3492
- WRITE: cfg.lhm.LHM_PNL_RECO_ENABLED
- USED:
  - .codex_env_extraction.json:10898
  - .codex_env_extraction.json:4938
  - .env.atlas:689
- STATUS: USED

### LHM_PNL_RECO_MAX_LOOKBACK_DAYS
- READ: bot_config.py:L3499
- WRITE: cfg.lhm.LHM_PNL_RECO_MAX_LOOKBACK_DAYS
- USED:
  - .codex_env_extraction.json:10925
  - .codex_env_extraction.json:4959
  - .env.atlas:694
- STATUS: USED

### LHM_PNL_RECO_TOL_ABS_QUOTE
- READ: bot_config.py:L3493
- WRITE: cfg.lhm.LHM_PNL_RECO_TOL_ABS_QUOTE
- USED:
  - .codex_env_extraction.json:10907
  - .codex_env_extraction.json:4945
  - .env.atlas:699
- STATUS: USED

### LHM_PNL_RECO_TOL_PCT
- READ: bot_config.py:L3496
- WRITE: cfg.lhm.LHM_PNL_RECO_TOL_PCT
- USED:
  - .codex_env_extraction.json:10916
  - .codex_env_extraction.json:4952
  - .env.atlas:704
- STATUS: USED

### LHM_Q_STREAM_MAX
- READ: bot_config.py:L3430
- WRITE: cfg.lhm.LHM_Q_STREAM_MAX
- USED:
  - .codex_env_extraction.json:10556
  - .codex_env_extraction.json:4672
  - .env.atlas:709
- STATUS: USED

### LHM_SLO_DROPPED_TRADES_BUDGET
- READ: bot_config.py:L3484
- WRITE: cfg.lhm.LHM_SLO_DROPPED_TRADES_BUDGET
- USED:
  - .codex_env_extraction.json:10871
  - .codex_env_extraction.json:4917
  - .env.atlas:714
- STATUS: USED

### LHM_SLO_LAG_SECONDS_MAX_TARGET
- READ: bot_config.py:L3482
- WRITE: cfg.lhm.LHM_SLO_PIPELINE_LAG_MAX_SECONDS_TARGET
- USED:
  - .codex_env_extraction.json:10862
  - .codex_env_extraction.json:4910
  - .env.atlas:719
- STATUS: USED

### LHM_SLO_QUEUE_DEPTH_MAX_TARGET
- READ: bot_config.py:L3480
- WRITE: cfg.lhm.LHM_SLO_QUEUE_DEPTH_MAX_TARGET
- USED:
  - .codex_env_extraction.json:10853
  - .codex_env_extraction.json:4903
  - .env.atlas:724
- STATUS: USED

### LHM_SLO_WRITE_MS_P95_TARGET
- READ: bot_config.py:L3478
- WRITE: cfg.lhm.LHM_SLO_WRITE_MS_P95_TARGET
- USED:
  - .codex_env_extraction.json:10844
  - .codex_env_extraction.json:4896
  - .env.atlas:729
- STATUS: USED

### LHM_STORAGE_ALERT_COOLDOWN_S
- READ: bot_config.py:L3458
- WRITE: cfg.lhm.LHM_STORAGE_ALERT_COOLDOWN_S
- USED:
  - .codex_env_extraction.json:10709
  - .codex_env_extraction.json:4791
  - .env.atlas:734
- STATUS: USED

### LHM_STORAGE_PATH
- READ: bot_config.py:L3508
- WRITE: cfg.lhm.storage_path
- USED:
  - .codex_env_extraction.json:10952
  - .codex_env_extraction.json:4980
  - .env.atlas:739
- STATUS: USED

### LHM_STREAM_BATCH
- READ: bot_config.py:L3431
- WRITE: cfg.lhm.LHM_STREAM_BATCH
- USED:
  - .codex_env_extraction.json:10565
  - .codex_env_extraction.json:4679
  - .env.atlas:744
- STATUS: USED

### LHM_TRADE_BATCH_SIZE
- READ: bot_config.py:L3475
- WRITE: cfg.lhm.LHM_TRADE_BATCH_SIZE
- USED:
  - .codex_env_extraction.json:10826
  - .codex_env_extraction.json:4882
  - .env.atlas:749
- STATUS: USED

### LHM_TRADE_FLUSH_INTERVAL_S
- READ: bot_config.py:L3476
- WRITE: cfg.lhm.LHM_TRADE_FLUSH_INTERVAL_S
- USED:
  - .codex_env_extraction.json:10835
  - .codex_env_extraction.json:4889
  - .env.atlas:754
- STATUS: USED

### LHM_WD_INTERVAL_S
- READ: bot_config.py:L3471
- WRITE: cfg.lhm.LHM_WD_INTERVAL_S
- USED:
  - .codex_env_extraction.json:10790
  - .codex_env_extraction.json:4854
  - .env.atlas:759
- STATUS: USED

### LHM_WD_PLATEAU_WINDOW
- READ: bot_config.py:L3472
- WRITE: cfg.lhm.LHM_WD_PLATEAU_WINDOW
- USED:
  - .codex_env_extraction.json:10799
  - .codex_env_extraction.json:4861
  - .env.atlas:764
- STATUS: USED

### LHM_WD_QUEUE_MIN_SIZE
- READ: bot_config.py:L3473
- WRITE: cfg.lhm.LHM_WD_QUEUE_MIN_SIZE
- USED:
  - .codex_env_extraction.json:10808
  - .codex_env_extraction.json:4868
  - .env.atlas:769
- STATUS: USED

### LHM_WD_STALL_THRESHOLD_S
- READ: bot_config.py:L3474
- WRITE: cfg.lhm.LHM_WD_STALL_THRESHOLD_S
- USED:
  - .codex_env_extraction.json:10817
  - .codex_env_extraction.json:4875
  - .env.atlas:774
- STATUS: USED

### LIVE_TRADING_ARMED
- READ: bot_config.py:L1787
- WRITE: cfg.g.live_trading_armed
- USED:
  - bot_arbitrage.py:768
  - smoke_test_all.py:556
  - smoke_test_all.py:563
- STATUS: USED

### LOG_LEVEL
- READ: bot_config.py:L1841
- WRITE: cfg.obs.log_level
- USED:
  - .codex_env_extraction.json:5679
  - .codex_env_extraction.json:885
  - .env.atlas:810
- STATUS: USED

### MIN_BUFFER_QUOTE
- READ: bot_config.py:L2302
- WRITE: cfg.rm.min_buffer_quote
- USED: none
- STATUS: NO_USAGE_FOUND

### MIN_BUFFER_USD
- READ: bot_config.py:L2304
- WRITE: cfg.rm.min_buffer_quote
- USED: none
- STATUS: NO_USAGE_FOUND

### MIN_FRAGMENT_QUOTE
- READ: bot_config.py:L1757
- WRITE: cfg.g.min_fragment_quote
- USED: none
- STATUS: NO_USAGE_FOUND

### MIN_NOTIONAL_BY_EXCHANGE_QUOTE
- READ: bot_config.py:L1750
- WRITE: cfg.g.min_notional_by_exchange_quote
- USED: none
- STATUS: NO_USAGE_FOUND

### MIN_USDC
- READ: bot_config.py:L1748
- WRITE: cfg.g.min_usdc
- USED: none
- STATUS: NO_USAGE_FOUND

### MM_TTL_MS
- READ: bot_config.py:L3326
- WRITE: cfg.rm.mm_ttl_ms
- USED:
  - .codex_env_extraction.json:10070
  - .codex_env_extraction.json:4287
  - .env.atlas:1293
- STATUS: USED

### MODE
- READ: bot_config.py:L1786
- WRITE: cfg.g.mode
- USED:
  - bot_arbitrage.py:767
  - execution_engine.py:1365
  - obs_metrics.py:2275
  - smoke_test_all.py:1262
  - smoke_test_all.py:1828
  - smoke_test_all.py:2198
  - smoke_test_all.py:2423
  - smoke_test_all.py:554
  - smoke_test_all.py:561
- STATUS: USED

### OBS_BASE_URL
- READ: bot_config.py:L3511
- WRITE: cfg.dashboard.OBS_BASE_URL
- USED:
  - .codex_env_extraction.json:10961
  - .codex_env_extraction.json:4987
  - .env.atlas:140
- STATUS: USED

### OBS_ENABLE_9108
- READ: bot_config.py:L1846
- WRITE: cfg.obs.enable_obs_port
- USED:
  - .codex_env_extraction.json:5706
  - .codex_env_extraction.json:906
  - .env.atlas:815
  - smoke_test_all.py:2212
  - smoke_test_all.py:2438
  - smoke_test_all.py:613
- STATUS: USED

### OBS_PORT
- READ: bot_config.py:L1847
- WRITE: cfg.obs.obs_port
- USED:
  - .codex_env_extraction.json:5715
  - .codex_env_extraction.json:913
  - .env.atlas:820
- STATUS: USED

### OPS_RETENTION_DAYS
- READ: bot_config.py:L3505
- WRITE: cfg.lhm.OPS_RETENTION_DAYS
- USED:
  - .codex_env_extraction.json:10943
  - .codex_env_extraction.json:4973
  - .env.atlas:779
- STATUS: USED

### PACER_MODE
- READ: bot_config.py:L1735
- WRITE: cfg.g.pacer_mode
- USED: none
- STATUS: NO_USAGE_FOUND

### PAIRS
- READ: bot_config.py:L2272
- WRITE: cfg.g.pairs
- USED:
  - smoke_test_all.py:2201
  - smoke_test_all.py:2426
  - smoke_test_all.py:594
- STATUS: TEST_ONLY

### PAIR_REGEX
- READ: bot_config.py:L1745
- WRITE: cfg.g.pair_regex
- USED: none
- STATUS: NO_USAGE_FOUND

### PAIR_UNIVERSE_MODE
- READ: bot_config.py:L1743
- WRITE: cfg.g.pair_universe_mode
- USED: none
- STATUS: NO_USAGE_FOUND

### PAIR_WHITELIST
- READ: bot_config.py:L1744
- WRITE: cfg.g.pair_whitelist
- USED: none
- STATUS: NO_USAGE_FOUND

### PER_STRATEGY_NOTIONAL_CAP
- READ: bot_config.py:L2962
- WRITE: cfg.rm.per_strategy_notional_cap
- USED:
  - .codex_env_extraction.json:3251
  - .codex_env_extraction.json:8735
  - .env.atlas:1298
- STATUS: USED

### POD_REGION
- READ: bot_config.py:L1728
- WRITE: cfg.g.pod_region
- USED:
  - eod_pnl_runner.py:104
  - eod_pnl_runner.py:283
  - logger_historique_manager.py:2997
- STATUS: USED

### PREEMPT_MM_FOR_TT_TM
- READ: bot_config.py:L2961
- WRITE: cfg.rm.preempt_mm_for_tt_tm
- USED:
  - .codex_env_extraction.json:3244
  - .codex_env_extraction.json:8726
  - .env.atlas:1303
- STATUS: USED

### PRIMARY_QUOTE
- READ: bot_config.py:L1747
- WRITE: cfg.g.primary_quote
- USED: none
- STATUS: NO_USAGE_FOUND

### PRIVATE_WS_STALE_MS
- READ: bot_config.py:L2040
- WRITE: cfg.wd.private_ws_stale_ms
- USED:
  - .codex_env_extraction.json:1480
  - .codex_env_extraction.json:6458
  - .env.atlas:2606
  - private_ws_watchdog.py:116
  - private_ws_watchdog.py:117
- STATUS: USED

### PWS_ALERT_PERIOD_S
- READ: bot_config.py:L3276
- WRITE: cfg.pws.PWS_ALERT_PERIOD_S
- USED:
  - .codex_env_extraction.json:4049
  - .codex_env_extraction.json:9764
  - .env.atlas:973
- STATUS: USED

### PWS_BACKOFF_BASE_MS
- READ: bot_config.py:L3266
- WRITE: cfg.pws.PWS_BACKOFF_BASE_MS
- USED:
  - .codex_env_extraction.json:4007
  - .codex_env_extraction.json:9710
  - .env.atlas:978
- STATUS: USED

### PWS_BACKOFF_MAX_MS
- READ: bot_config.py:L3267
- WRITE: cfg.pws.PWS_BACKOFF_MAX_MS
- USED:
  - .codex_env_extraction.json:4014
  - .codex_env_extraction.json:9719
  - .env.atlas:983
- STATUS: USED

### PWS_BYBIT_WS_PRIVATE_URL
- READ: bot_config.py:L3271
- WRITE: cfg.pws.bybit_ws_private_url
- USED:
  - .codex_env_extraction.json:4028
  - .codex_env_extraction.json:9737
  - .env.atlas:988
- STATUS: USED

### PWS_CB_PRIVATE_POLL_INTERVAL_S
- READ: bot_config.py:L3268
- WRITE: cfg.pws.cb_private_poll_interval_s
- USED:
  - .codex_env_extraction.json:4021
  - .codex_env_extraction.json:9728
  - .env.atlas:993
- STATUS: USED

### PWS_DISABLE_AUTO_WIRING_PROD
- READ: bot_config.py:L3256
- WRITE: cfg.pws.ff_pws_disable_auto_wiring_prod
- USED:
  - .codex_env_extraction.json:3958
  - .codex_env_extraction.json:9647
  - .env.atlas:998
- STATUS: USED

### PWS_DROP_POLICY
- READ: bot_config.py:L3260
- WRITE: cfg.pws.pws_drop_policy
- USED:
  - .codex_env_extraction.json:3965
  - .codex_env_extraction.json:9656
  - .env.atlas:1003
- STATUS: USED

### PWS_HEARTBEAT_MAX_GAP_S
- READ: bot_config.py:L3263
- WRITE: cfg.pws.PWS_HEARTBEAT_MAX_GAP_S
- USED:
  - .codex_env_extraction.json:3986
  - .codex_env_extraction.json:9683
  - .env.atlas:1008
- STATUS: USED

### PWS_JITTER_MS
- READ: bot_config.py:L3265
- WRITE: cfg.pws.PWS_JITTER_MS
- USED:
  - .codex_env_extraction.json:4000
  - .codex_env_extraction.json:9701
  - .env.atlas:1013
- STATUS: USED

### PWS_NO_DROP_CRITICAL_ENFORCED
- READ: bot_config.py:L3249
- WRITE: cfg.pws.ff_pws_no_drop_critical_enforced
- USED:
  - .codex_env_extraction.json:3944
  - .codex_env_extraction.json:9638
  - .env.atlas:1018
- STATUS: USED

### PWS_PACER_EU
- READ: bot_config.py:L3274
- WRITE: cfg.pws.PWS_PACER_EU
- USED:
  - .codex_env_extraction.json:4035
  - .codex_env_extraction.json:9746
  - .env.atlas:1023
- STATUS: USED

### PWS_PACER_US
- READ: bot_config.py:L3275
- WRITE: cfg.pws.PWS_PACER_US
- USED:
  - .codex_env_extraction.json:4042
  - .codex_env_extraction.json:9755
  - .env.atlas:1028
- STATUS: USED

### PWS_PING_INTERVAL_S
- READ: bot_config.py:L3261
- WRITE: cfg.pws.PWS_PING_INTERVAL_S
- USED:
  - .codex_env_extraction.json:3972
  - .codex_env_extraction.json:9665
  - .env.atlas:1033
- STATUS: USED

### PWS_PONG_TIMEOUT_S
- READ: bot_config.py:L3262
- WRITE: cfg.pws.PWS_PONG_TIMEOUT_S
- USED:
  - .codex_env_extraction.json:3979
  - .codex_env_extraction.json:9674
  - .env.atlas:1038
- STATUS: USED

### PWS_POOL_SIZE_EU
- READ: bot_config.py:L3243
- WRITE: cfg.pws.PWS_POOL_SIZE_EU
- USED:
  - .codex_env_extraction.json:3916
  - .codex_env_extraction.json:9602
  - .env.atlas:1043
- STATUS: USED

### PWS_POOL_SIZE_US
- READ: bot_config.py:L3244
- WRITE: cfg.pws.PWS_POOL_SIZE_US
- USED:
  - .codex_env_extraction.json:3923
  - .codex_env_extraction.json:9611
  - .env.atlas:1048
- STATUS: USED

### PWS_QUEUE_MAXLEN
- READ: bot_config.py:L3245
- WRITE: cfg.pws.PWS_QUEUE_MAXLEN
- USED:
  - .codex_env_extraction.json:3930
  - .codex_env_extraction.json:9620
  - .env.atlas:1053
- STATUS: USED

### PWS_QUEUE_SATURATION_RATIO
- READ: bot_config.py:L3246
- WRITE: cfg.pws.PWS_QUEUE_SATURATION_RATIO
- USED:
  - .codex_env_extraction.json:3937
  - .codex_env_extraction.json:9629
  - .env.atlas:1058
- STATUS: USED

### PWS_STABLE_RESET_S
- READ: bot_config.py:L3264
- WRITE: cfg.pws.PWS_STABLE_RESET_S
- USED:
  - .codex_env_extraction.json:3993
  - .codex_env_extraction.json:9692
  - .env.atlas:1063
- STATUS: USED

### PWS_STRICT_DEDUP_ENFORCED
- READ: bot_config.py:L1780,3253
- WRITE: cfg.pws.ff_pws_strict_dedup_enforced
- USED:
  - .codex_env_extraction.json:3951
  - .codex_env_extraction.json:5602
  - .codex_env_extraction.json:5609
  - .codex_env_extraction.json:836
  - .env.atlas:1068
- STATUS: USED

### REBAL_ALLOW_LOSS_BPS
- READ: bot_config.py:L2973
- WRITE: cfg.rm.rebal_allow_loss_bps
- USED:
  - .codex_env_extraction.json:3272
  - .codex_env_extraction.json:3286
  - .codex_env_extraction.json:8762
  - .codex_env_extraction.json:8780
  - .env.atlas:1308
  - .env.atlas:1488
- STATUS: USED

### REBAL_HINT_TTL_S
- READ: bot_config.py:L3286
- WRITE: cfg.rebal.rebal_hint_ttl_s
- USED:
  - .codex_env_extraction.json:4091
  - .codex_env_extraction.json:9818
  - .env.atlas:1082
- STATUS: USED

### REBAL_INTERNAL_TRANSFER_THRESHOLD
- READ: bot_config.py:L3287
- WRITE: cfg.rebal.rebal_internal_transfer_threshold
- USED:
  - .codex_env_extraction.json:4098
  - .codex_env_extraction.json:9827
  - .env.atlas:1087
- STATUS: USED

### REBAL_MAX_OPS_PER_MIN
- READ: bot_config.py:L3281
- WRITE: cfg.rebal.rebal_max_ops_per_min
- USED:
  - .codex_env_extraction.json:4070
  - .codex_env_extraction.json:9791
  - .env.atlas:1092
- STATUS: USED

### REBAL_OPS_PER_MIN_RATIO
- READ: bot_config.py:L3282
- WRITE: cfg.rebal.rebal_ops_per_min_ratio
- USED:
  - .codex_env_extraction.json:4077
  - .codex_env_extraction.json:9800
  - .env.atlas:1097
- STATUS: USED

### REBAL_PRIORITY
- READ: bot_config.py:L3285
- WRITE: cfg.rebal.rebal_priority
- USED:
  - .codex_env_extraction.json:4084
  - .codex_env_extraction.json:9809
  - .env.atlas:1102
- STATUS: USED

### REBAL_QUANTUM_MIN_QUOTE
- READ: bot_config.py:L3280
- WRITE: cfg.rebal.rebal_quantum_min_quote
- USED:
  - .codex_env_extraction.json:4063
  - .codex_env_extraction.json:9782
  - .env.atlas:1107
- STATUS: USED

### REBAL_QUANTUM_QUOTE_MAP
- READ: bot_config.py:L3299
- WRITE: cfg.rebal.rebal_quantum_quote_map
- USED:
  - .codex_env_extraction.json:4126
  - .codex_env_extraction.json:9863
  - .env.atlas:1112
- STATUS: USED

### REBAL_SLOT_TTL_S
- READ: bot_config.py:L3291
- WRITE: cfg.rebal.rebal_slot_ttl_s
- USED:
  - .codex_env_extraction.json:4105
  - .codex_env_extraction.json:9836
  - .env.atlas:1117
- STATUS: USED

### REBAL_SNAPSHOTS_ERROR_COOLDOWN_S
- READ: bot_config.py:L3297
- WRITE: cfg.rebal.rebal_snapshots_error_cooldown_s
- USED:
  - .codex_env_extraction.json:4119
  - .codex_env_extraction.json:9854
  - .env.atlas:1122
- STATUS: USED

### REBAL_SNAPSHOTS_MISSING_ERROR_S
- READ: bot_config.py:L3295
- WRITE: cfg.rebal.rebal_snapshots_missing_error_s
- USED:
  - .codex_env_extraction.json:4112
  - .codex_env_extraction.json:9845
  - .env.atlas:1127
- STATUS: USED

### REBAL_VOLUME_HAIRCUT
- READ: bot_config.py:L2974
- WRITE: cfg.rm.rebal_volume_haircut
- USED:
  - .codex_env_extraction.json:3279
  - .codex_env_extraction.json:3293
  - .codex_env_extraction.json:8771
  - .codex_env_extraction.json:8789
  - .env.atlas:1313
  - .env.atlas:1493
- STATUS: USED

### RECO_ALERT_PERIOD_S
- READ: bot_config.py:L3278
- WRITE: cfg.reconciler.RECO_ALERT_PERIOD_S
- USED:
  - .codex_env_extraction.json:4056
  - .codex_env_extraction.json:9773
  - .env.atlas:1134
- STATUS: USED

### REGION
- READ: bot_config.py:L1729
- WRITE: cfg.g.pod_region
- USED:
  - eod_pnl_runner.py:104
  - eod_pnl_runner.py:283
  - logger_historique_manager.py:2997
- STATUS: USED

### RESTART_MODE
- READ: bot_config.py:L1788
- WRITE: cfg.g.restart_mode
- USED: none
- STATUS: NO_USAGE_FOUND

### RESTART_WEBHOOK_HMAC_KEY
- READ: bot_config.py:L1873
- WRITE: cfg.alerting.webhook.hmac_secret
- USED:
  - .codex_env_extraction.json:5805
  - .codex_env_extraction.json:983
  - .env.atlas:7
- STATUS: USED

### RESTART_WEBHOOK_TIMEOUT_S
- READ: bot_config.py:L1876
- WRITE: cfg.alerting.webhook.timeout_s
- USED:
  - .codex_env_extraction.json:5814
  - .codex_env_extraction.json:990
  - .env.atlas:12
- STATUS: USED

### RESTART_WEBHOOK_URL
- READ: bot_config.py:L1872
- WRITE: cfg.alerting.webhook.url
- USED:
  - .codex_env_extraction.json:5796
  - .codex_env_extraction.json:976
  - .env.atlas:17
- STATUS: USED

### RL_BURSTS
- READ: bot_config.py:L3365
- WRITE: cfg.rl.bursts_by_exchange
- USED:
  - .codex_env_extraction.json:10250
  - .codex_env_extraction.json:4427
  - .env.atlas:1148
- STATUS: USED

### RL_BURSTS_BY_EXCHANGE
- READ: bot_config.py:L3364,3370
- WRITE: cfg.rl.bursts_by_exchange
- USED:
  - .codex_env_extraction.json:10250
  - .codex_env_extraction.json:4427
  - .env.atlas:1148
- STATUS: USED

### RL_BURSTS_BY_EXCHANGE_KIND
- READ: bot_config.py:L3372
- WRITE: cfg.rl.bursts_by_exchange_kind
- USED:
  - .codex_env_extraction.json:10250
  - .codex_env_extraction.json:4427
  - .env.atlas:1148
- STATUS: USED

### RL_BURSTS_BY_KIND
- READ: bot_config.py:L3376
- WRITE: cfg.rl.bursts_by_kind
- USED:
  - .codex_env_extraction.json:10259
  - .codex_env_extraction.json:4434
  - .env.atlas:1153
- STATUS: USED

### RL_DEFAULT_BURST
- READ: bot_config.py:L3383
- WRITE: cfg.rl.default_burst
- USED:
  - .codex_env_extraction.json:10322
  - .codex_env_extraction.json:4483
  - .env.atlas:1158
- STATUS: USED

### RL_DEFAULT_RATE_PER_S
- READ: bot_config.py:L3382
- WRITE: cfg.rl.default_rate_per_s
- USED:
  - .codex_env_extraction.json:10313
  - .codex_env_extraction.json:4476
  - .env.atlas:1163
- STATUS: USED

### RL_FAIR
- READ: bot_config.py:L3378
- WRITE: cfg.rl.fair
- USED:
  - .codex_env_extraction.json:10277
  - .codex_env_extraction.json:4448
  - .env.atlas:1168
- STATUS: USED

### RL_HARD_CAPS_RPS
- READ: bot_config.py:L3350
- WRITE: cfg.rl.hard_caps_rps_by_exchange
- USED:
  - .codex_env_extraction.json:10214
  - .codex_env_extraction.json:4399
  - .env.atlas:1173
- STATUS: USED

### RL_HARD_CAPS_RPS_BY_EXCHANGE
- READ: bot_config.py:L3349,3356
- WRITE: cfg.rl.hard_caps_rps_by_exchange
- USED:
  - .codex_env_extraction.json:10214
  - .codex_env_extraction.json:4399
  - .env.atlas:1173
- STATUS: USED

### RL_HARD_CAPS_RPS_BY_EXCHANGE_KIND
- READ: bot_config.py:L3359
- WRITE: cfg.rl.hard_caps_rps_by_exchange_kind
- USED:
  - .codex_env_extraction.json:10214
  - .codex_env_extraction.json:4399
  - .env.atlas:1173
- STATUS: USED

### RL_HARD_CAPS_RPS_BY_KIND
- READ: bot_config.py:L3363
- WRITE: cfg.rl.hard_caps_rps_by_kind
- USED:
  - .codex_env_extraction.json:10223
  - .codex_env_extraction.json:4406
  - .env.atlas:1178
- STATUS: USED

### RL_MAX_SLEEP_S
- READ: bot_config.py:L3381
- WRITE: cfg.rl.max_sleep_s
- USED:
  - .codex_env_extraction.json:10304
  - .codex_env_extraction.json:4469
  - .env.atlas:1183
- STATUS: USED

### RL_MIN_SLEEP_S
- READ: bot_config.py:L3380
- WRITE: cfg.rl.min_sleep_s
- USED:
  - .codex_env_extraction.json:10295
  - .codex_env_extraction.json:4462
  - .env.atlas:1188
- STATUS: USED

### RL_NAME_PREFIX
- READ: bot_config.py:L3379
- WRITE: cfg.rl.name_prefix
- USED:
  - .codex_env_extraction.json:10286
  - .codex_env_extraction.json:4455
  - .env.atlas:1193
- STATUS: USED

### RL_PRIORITIES
- READ: bot_config.py:L3377
- WRITE: cfg.rl.priorities
- USED:
  - .codex_env_extraction.json:10268
  - .codex_env_extraction.json:4441
  - .env.atlas:1198
- STATUS: USED

### RL_SC
- READ: bot_config.py:L3386
- WRITE: cfg.rl.sc
- USED:
  - .codex_env_extraction.json:10331
  - .codex_env_extraction.json:4490
  - .env.atlas:1203
- STATUS: USED

### RM_BALANCE_TTL_S_BLOCK
- READ: bot_config.py:L2318
- WRITE: cfg.rm.balance_ttl_s_block
- USED:
  - .codex_env_extraction.json:2187
  - .codex_env_extraction.json:7391
  - .env.atlas:1318
- STATUS: USED

### RM_BALANCE_TTL_S_DEGRADED
- READ: bot_config.py:L2314
- WRITE: cfg.rm.balance_ttl_s_degraded
- USED:
  - .codex_env_extraction.json:2180
  - .codex_env_extraction.json:7382
  - .env.atlas:1323
- STATUS: USED

### RM_BALANCE_TTL_S_NORMAL
- READ: bot_config.py:L2310
- WRITE: cfg.rm.balance_ttl_s_normal
- USED:
  - .codex_env_extraction.json:2173
  - .codex_env_extraction.json:7373
  - .env.atlas:1328
- STATUS: USED

### RM_BALANCE_UNKNOWN_POLICY
- READ: bot_config.py:L2322
- WRITE: cfg.rm.balance_unknown_policy
- USED:
  - risk_manager.py:6323
- STATUS: USED

### RM_BASE_MIN_BPS
- READ: bot_config.py:L2434
- WRITE: cfg.rm.base_min_bps
- USED:
  - .codex_env_extraction.json:2334
  - .codex_env_extraction.json:7580
  - .env.atlas:1333
- STATUS: USED

### RM_BRANCH_BUDGETS_QUOTE
- READ: bot_config.py:L2871
- WRITE: cfg.rm.branch_budgets_quote
- USED:
  - .codex_env_extraction.json:3041
  - .codex_env_extraction.json:8460
  - .env.atlas:1338
- STATUS: USED

### RM_BRANCH_PRIORITY
- READ: bot_config.py:L2867
- WRITE: cfg.rm.branch_priority
- USED:
  - .codex_env_extraction.json:3034
  - .codex_env_extraction.json:8451
  - .env.atlas:1343
- STATUS: USED

### RM_CAPITAL_LADDER_CFG
- READ: bot_config.py:L3021
- WRITE: cfg.rm.capital_ladder_cfg
- USED:
  - .codex_env_extraction.json:3335
  - .codex_env_extraction.json:8843
  - .env.atlas:1348
- STATUS: USED

### RM_CAPS_TRADING_BY_PROFILE
- READ: bot_config.py:L3013
- WRITE: cfg.rm.caps_trading_by_profile
- USED:
  - .codex_env_extraction.json:3321
  - .codex_env_extraction.json:8825
  - .env.atlas:1353
  - risk_manager.py:14413
- STATUS: USED

### RM_COLLAT_ALIAS_OVERRIDES
- READ: bot_config.py:L2895
- WRITE: cfg.rm.collat_alias_overrides
- USED:
  - .codex_env_extraction.json:3132
  - .codex_env_extraction.json:8577
  - .env.atlas:1358
- STATUS: USED

### RM_COLLAT_DEFAULT_MIN_USD
- READ: bot_config.py:L2892
- WRITE: cfg.rm.collat_default_min_usd
- USED:
  - .codex_env_extraction.json:3125
  - .codex_env_extraction.json:8568
  - .env.atlas:1363
- STATUS: USED

### RM_COLLAT_QUOTES
- READ: bot_config.py:L2904,2943
- WRITE: cfg.rm.collat_quotes
- USED:
  - .codex_env_extraction.json:3153
  - .codex_env_extraction.json:8604
  - .env.atlas:1368
  - risk_manager.py:2226
- STATUS: USED

### RM_COLLAT_RATIO_CRIT
- READ: bot_config.py:L2901
- WRITE: cfg.rm.collat_ratio_crit
- USED:
  - .codex_env_extraction.json:3146
  - .codex_env_extraction.json:8595
  - .env.atlas:1373
- STATUS: USED

### RM_COLLAT_RATIO_LOW
- READ: bot_config.py:L2908
- WRITE: cfg.rm.collat_ratio_low
- USED:
  - .codex_env_extraction.json:3160
  - .codex_env_extraction.json:8620
  - .env.atlas:1378
- STATUS: USED

### RM_COLLAT_RATIO_WARN
- READ: bot_config.py:L2898
- WRITE: cfg.rm.collat_ratio_warn
- USED:
  - .codex_env_extraction.json:3139
  - .codex_env_extraction.json:8586
  - .env.atlas:1383
- STATUS: USED

### RM_COMBO_CAP_USD_BY_PROFILE
- READ: bot_config.py:L2964
- WRITE: cfg.rm.combo_cap_usd_by_profile
- USED:
  - .codex_env_extraction.json:3258
  - .codex_env_extraction.json:8744
  - .env.atlas:1388
- STATUS: USED

### RM_COMBO_TTL_DEGRADED_FACTOR
- READ: bot_config.py:L2968
- WRITE: cfg.rm.combo_ttl_degraded_factor
- USED:
  - .codex_env_extraction.json:3265
  - .codex_env_extraction.json:8753
  - .env.atlas:1393
- STATUS: USED

### RM_CONSTR_CAP_FACTOR
- READ: bot_config.py:L2414
- WRITE: cfg.RM_CONSTR_CAP_FACTOR
- USED:
  - .codex_env_extraction.json:2299
  - .codex_env_extraction.json:7535
  - .env.atlas:1600
- STATUS: USED

### RM_CONSTR_IOC_ONLY
- READ: bot_config.py:L2418
- WRITE: cfg.RM_CONSTR_IOC_ONLY
- USED:
  - .codex_env_extraction.json:2306
  - .codex_env_extraction.json:7544
  - .env.atlas:1607
- STATUS: USED

### RM_CONSTR_MM_ENABLE
- READ: bot_config.py:L2505
- WRITE: cfg.rm.switch_knobs.constr_mm_enable
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_CONSTR_TM_MIN_BPS_DELTA
- READ: bot_config.py:L2410
- WRITE: cfg.RM_CONSTR_TM_MIN_BPS_DELTA
- USED:
  - .codex_env_extraction.json:2292
  - .codex_env_extraction.json:7526
  - .env.atlas:1614
- STATUS: USED

### RM_CONSTR_TT_MIN_BPS_DELTA
- READ: bot_config.py:L2406
- WRITE: cfg.RM_CONSTR_TT_MIN_BPS_DELTA
- USED:
  - .codex_env_extraction.json:2285
  - .codex_env_extraction.json:7517
  - .env.atlas:1621
- STATUS: USED

### RM_CRITICAL_ALIASES
- READ: bot_config.py:L2356
- WRITE: cfg.RM_CRITICAL_ALIASES
- USED:
  - risk_manager.py:6428
- STATUS: USED

### RM_DECISION_LOG_PATH
- READ: bot_config.py:L2960
- WRITE: cfg.rm.decision_log_path
- USED:
  - .codex_env_extraction.json:3237
  - .codex_env_extraction.json:8717
  - .env.atlas:1398
- STATUS: USED

### RM_DEFAULT_NOTIONAL
- READ: bot_config.py:L2883
- WRITE: cfg.rm.default_notional
- USED:
  - .codex_env_extraction.json:3062
  - .codex_env_extraction.json:8487
  - .env.atlas:1403
- STATUS: USED

### RM_DYNAMIC_K
- READ: bot_config.py:L2435
- WRITE: cfg.rm.dynamic_k
- USED:
  - .codex_env_extraction.json:2341
  - .codex_env_extraction.json:7589
  - .env.atlas:1408
- STATUS: USED

### RM_FALLBACK_ON_TICK_EXCEPTION
- READ: bot_config.py:L2570
- WRITE: cfg.rm.fallback_on_tick_exception
- USED:
  - .codex_env_extraction.json:11196
  - .codex_env_extraction.json:5246
  - .env.atlas:1413
- STATUS: USED

### RM_FEE_LOW_MIN_SECONDS
- READ: bot_config.py:L2365
- WRITE: cfg.RM_FEE_LOW_MIN_SECONDS
- USED:
  - .codex_env_extraction.json:2222
  - .codex_env_extraction.json:7436
  - .env.atlas:1628
- STATUS: USED

### RM_FEE_TOKEN_MIN_PCT
- READ: bot_config.py:L2456
- WRITE: cfg.RM_FEE_TOKEN_MIN_PCT
- USED:
  - .codex_env_extraction.json:2397
  - .codex_env_extraction.json:7661
  - .env.atlas:1635
- STATUS: USED

### RM_FF_FAIL_CLOSED_CAPS
- READ: bot_config.py:L2833
- WRITE: cfg.rm.ff_fail_closed_caps
- USED:
  - .codex_env_extraction.json:2978
  - .codex_env_extraction.json:8379
  - .env.atlas:1418
- STATUS: USED

### RM_INFLIGHT_REBAL_BY_PROFILE
- READ: bot_config.py:L3017
- WRITE: cfg.rm.inflight_rebal_by_profile
- USED:
  - .codex_env_extraction.json:3328
  - .codex_env_extraction.json:8834
  - .env.atlas:1423
- STATUS: USED

### RM_INFLIGHT_TRADING_BY_PROFILE
- READ: bot_config.py:L3009
- WRITE: cfg.rm.inflight_trading_by_profile
- USED:
  - .codex_env_extraction.json:3314
  - .codex_env_extraction.json:8816
  - .env.atlas:1428
- STATUS: USED

### RM_INVARIANT_STRICT
- READ: bot_config.py:L2422
- WRITE: cfg.RM_INVARIANT_STRICT
- USED:
  - .codex_env_extraction.json:2313
  - .codex_env_extraction.json:7553
  - .env.atlas:1642
- STATUS: USED

### RM_MAX_FRAGMENTS
- READ: bot_config.py:L2884
- WRITE: cfg.rm.max_fragments
- USED:
  - .codex_env_extraction.json:3069
  - .codex_env_extraction.json:8496
  - .env.atlas:1433
- STATUS: USED

### RM_MIN_BPS_CAP
- READ: bot_config.py:L2437
- WRITE: cfg.rm.min_bps_cap
- USED:
  - .codex_env_extraction.json:2355
  - .codex_env_extraction.json:7607
  - .env.atlas:1438
- STATUS: USED

### RM_MIN_BPS_FLOOR
- READ: bot_config.py:L2436
- WRITE: cfg.rm.min_bps_floor
- USED:
  - .codex_env_extraction.json:2348
  - .codex_env_extraction.json:7598
  - .env.atlas:1443
- STATUS: USED

### RM_MM_ALIAS_NAME
- READ: bot_config.py:L2885
- WRITE: cfg.rm.mm_alias_name
- USED:
  - .codex_env_extraction.json:3076
  - .codex_env_extraction.json:8505
  - .env.atlas:1448
- STATUS: USED

### RM_MM_DEPTH_MIN_USD
- READ: bot_config.py:L2886
- WRITE: cfg.rm.mm_depth_min_usd
- USED:
  - .codex_env_extraction.json:3083
  - .codex_env_extraction.json:8514
  - .env.atlas:1453
- STATUS: USED

### RM_MM_HEDGE_COST_BPS
- READ: bot_config.py:L2890
- WRITE: cfg.rm.mm_hedge_cost_bps
- USED:
  - .codex_env_extraction.json:3111
  - .codex_env_extraction.json:8550
  - .env.atlas:1458
- STATUS: USED

### RM_MM_MIN_NET_BPS
- READ: bot_config.py:L2889
- WRITE: cfg.rm.mm_min_net_bps
- USED:
  - .codex_env_extraction.json:3104
  - .codex_env_extraction.json:8541
  - .env.atlas:1463
- STATUS: USED

### RM_MM_MIN_P_BOTH
- READ: bot_config.py:L2888
- WRITE: cfg.rm.mm_min_p_both
- USED:
  - .codex_env_extraction.json:3097
  - .codex_env_extraction.json:8532
  - .env.atlas:1468
- STATUS: USED

### RM_MM_QPOS_MAX_AHEAD_USD
- READ: bot_config.py:L2887
- WRITE: cfg.rm.mm_qpos_max_ahead_usd
- USED:
  - .codex_env_extraction.json:3090
  - .codex_env_extraction.json:8523
  - .env.atlas:1473
- STATUS: USED

### RM_MM_VOL_BPS_MAX
- READ: bot_config.py:L2891
- WRITE: cfg.rm.mm_vol_bps_max
- USED:
  - .codex_env_extraction.json:3118
  - .codex_env_extraction.json:8559
  - .env.atlas:1478
- STATUS: USED

### RM_MODE_TICK_INTERVAL_S
- READ: bot_config.py:L2450
- WRITE: cfg.rm.mode_tick_interval_s
- USED:
  - .codex_env_extraction.json:2390
  - .codex_env_extraction.json:7652
  - .env.atlas:1483
- STATUS: USED

### RM_MODE_TIMEOUT_S
- READ: bot_config.py:L2463
- WRITE: cfg.rm.switch_knobs.mode_timeout_s
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_NET_FLOOR_BPS
- READ: bot_config.py:L2467
- WRITE: cfg.rm.switch_knobs.net_floor_bps
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_NORMAL_CAP_FACTOR
- READ: bot_config.py:L2500
- WRITE: cfg.rm.switch_knobs.normal_cap_factor
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_NORMAL_IOC_ONLY
- READ: bot_config.py:L2501
- WRITE: cfg.rm.switch_knobs.normal_ioc_only
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_NORMAL_TM_MIN_BPS_DELTA
- READ: bot_config.py:L2497
- WRITE: cfg.rm.switch_knobs.normal_tm_min_bps_delta
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_NORMAL_TT_MIN_BPS_DELTA
- READ: bot_config.py:L2494
- WRITE: cfg.rm.switch_knobs.normal_tt_min_bps_delta
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_OPPVOL_P95_BPS_MIN
- READ: bot_config.py:L2481
- WRITE: cfg.rm.switch_knobs.oppvol_p95_bps_min
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_OPP_AGE_FALLBACK_S
- READ: bot_config.py:L2468
- WRITE: cfg.rm.switch_knobs.opp_age_fallback_s
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_OPP_SHADOW_P50_BPS_MAX
- READ: bot_config.py:L2475
- WRITE: cfg.rm.switch_knobs.opp_shadow_p50_bps_max
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_OPP_VOLUME_CAP_FACTOR
- READ: bot_config.py:L2513
- WRITE: cfg.rm.switch_knobs.opp_volume_cap_factor
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_OPP_VOLUME_MM_ENABLE
- READ: bot_config.py:L2516
- WRITE: cfg.rm.switch_knobs.opp_volume_mm_enable
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_OPP_VOLUME_TIMEOUT_S
- READ: bot_config.py:L2464
- WRITE: cfg.rm.switch_knobs.opp_volume_timeout_s
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_OPP_VOLUME_TM_MIN_BPS_DELTA
- READ: bot_config.py:L2510
- WRITE: cfg.rm.switch_knobs.opp_volume_tm_min_bps_delta
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_OPP_VOLUME_TT_MIN_BPS_DELTA
- READ: bot_config.py:L2507
- WRITE: cfg.rm.switch_knobs.opp_volume_tt_min_bps_delta
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_OPP_VOL_CAP_FACTOR
- READ: bot_config.py:L2525
- WRITE: cfg.rm.switch_knobs.opp_vol_cap_factor
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_OPP_VOL_EXIT_SLIP_AGE_S_MAX
- READ: bot_config.py:L2478
- WRITE: cfg.rm.switch_knobs.opp_vol_exit_slip_age_s_max
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_OPP_VOL_MM_ENABLE
- READ: bot_config.py:L2528
- WRITE: cfg.rm.switch_knobs.opp_vol_mm_enable
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_OPP_VOL_P95_BPS_MAX
- READ: bot_config.py:L2472
- WRITE: cfg.rm.switch_knobs.opp_vol_p95_bps_max
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_OPP_VOL_SLIP_AGE_S_MAX
- READ: bot_config.py:L2469
- WRITE: cfg.rm.switch_knobs.opp_vol_slip_age_s_max
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_OPP_VOL_TM_MIN_BPS_DELTA
- READ: bot_config.py:L2522
- WRITE: cfg.rm.switch_knobs.opp_vol_tm_min_bps_delta
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_OPP_VOL_TT_MIN_BPS_DELTA
- READ: bot_config.py:L2519
- WRITE: cfg.rm.switch_knobs.opp_vol_tt_min_bps_delta
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_PNL_COOLDOWN_S
- READ: bot_config.py:L2490
- WRITE: cfg.rm.switch_knobs.pnl_cooldown_s
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_PNL_GUARD_DAY_LVL1_PCT
- READ: bot_config.py:L2484
- WRITE: cfg.rm.switch_knobs.pnl_guard_day_lvl1_pct
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_PNL_GUARD_DAY_LVL2_PCT
- READ: bot_config.py:L2487
- WRITE: cfg.rm.switch_knobs.pnl_guard_day_lvl2_pct
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_PNL_GUARD_RECOVER_FLOOR_PCT
- READ: bot_config.py:L2491
- WRITE: cfg.rm.switch_knobs.pnl_guard_recover_floor_pct
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_PWS_ACK_LATENCY_CONSTRAINED_MS
- READ: bot_config.py:L2397
- WRITE: cfg.RM_PWS_ACK_LATENCY_CONSTRAINED_MS
- USED:
  - .codex_env_extraction.json:2271
  - .codex_env_extraction.json:7499
  - .env.atlas:1816
- STATUS: USED

### RM_PWS_ACK_LATENCY_SEVERE_MS
- READ: bot_config.py:L2401
- WRITE: cfg.RM_PWS_ACK_LATENCY_SEVERE_MS
- USED:
  - .codex_env_extraction.json:2278
  - .codex_env_extraction.json:7508
  - .env.atlas:1823
- STATUS: USED

### RM_PWS_EVENT_LAG_CONSTRAINED_MS
- READ: bot_config.py:L2389
- WRITE: cfg.RM_PWS_EVENT_LAG_CONSTRAINED_MS
- USED:
  - .codex_env_extraction.json:2257
  - .codex_env_extraction.json:7481
  - .env.atlas:1830
- STATUS: USED

### RM_PWS_EVENT_LAG_SEVERE_MS
- READ: bot_config.py:L2393
- WRITE: cfg.RM_PWS_EVENT_LAG_SEVERE_MS
- USED:
  - .codex_env_extraction.json:2264
  - .codex_env_extraction.json:7490
  - .env.atlas:1837
- STATUS: USED

### RM_PWS_FILL_LATENCY_CONSTRAINED_MS
- READ: bot_config.py:L2426
- WRITE: cfg.RM_PWS_FILL_LATENCY_CONSTRAINED_MS
- USED:
  - .codex_env_extraction.json:2320
  - .codex_env_extraction.json:7562
  - .env.atlas:1844
- STATUS: USED

### RM_PWS_FILL_LATENCY_SEVERE_MS
- READ: bot_config.py:L2430
- WRITE: cfg.RM_PWS_FILL_LATENCY_SEVERE_MS
- USED:
  - .codex_env_extraction.json:2327
  - .codex_env_extraction.json:7571
  - .env.atlas:1851
- STATUS: USED

### RM_PWS_HEARTBEAT_GAP_CONSTRAINED_S
- READ: bot_config.py:L2381
- WRITE: cfg.RM_PWS_HEARTBEAT_GAP_CONSTRAINED_S
- USED:
  - .codex_env_extraction.json:2243
  - .codex_env_extraction.json:7463
  - .env.atlas:1858
- STATUS: USED

### RM_PWS_HEARTBEAT_GAP_SEVERE_S
- READ: bot_config.py:L2385
- WRITE: cfg.RM_PWS_HEARTBEAT_GAP_SEVERE_S
- USED:
  - .codex_env_extraction.json:2250
  - .codex_env_extraction.json:7472
  - .env.atlas:1865
- STATUS: USED

### RM_REBAL_ALLOW_LOSS_BPS_BY_PROFILE
- READ: bot_config.py:L2975
- WRITE: cfg.rm.rebal_allow_loss_bps_by_profile
- USED:
  - .codex_env_extraction.json:3286
  - .codex_env_extraction.json:8780
  - .env.atlas:1488
- STATUS: USED

### RM_REBAL_VOLUME_HAIRCUT_BY_PROFILE
- READ: bot_config.py:L2979
- WRITE: cfg.rm.rebal_volume_haircut_by_profile
- USED:
  - .codex_env_extraction.json:3293
  - .codex_env_extraction.json:8789
  - .env.atlas:1493
- STATUS: USED

### RM_RECO_MISS_PER_MINUTE_CONSTRAINED
- READ: bot_config.py:L2371
- WRITE: cfg.RM_RECO_MISS_PER_MINUTE_CONSTRAINED
- USED:
  - .codex_env_extraction.json:2229
  - .codex_env_extraction.json:7445
  - .env.atlas:1872
- STATUS: USED

### RM_RECO_MISS_PER_MINUTE_SEVERE
- READ: bot_config.py:L2375
- WRITE: cfg.RM_RECO_MISS_PER_MINUTE_SEVERE
- USED:
  - .codex_env_extraction.json:2236
  - .codex_env_extraction.json:7454
  - .env.atlas:1879
- STATUS: USED

### RM_REQUIRE_BOOK_AGE_FOR_OPP
- READ: bot_config.py:L2566
- WRITE: cfg.rm.signal_policy.require_book_age_for_opp
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_REQUIRE_PWS_HEALTH_FOR_OPP
- READ: bot_config.py:L2563
- WRITE: cfg.rm.signal_policy.require_pws_health_for_opp
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_REQUIRE_RL_HEALTH_FOR_OPP
- READ: bot_config.py:L2560
- WRITE: cfg.rm.signal_policy.require_rl_health_for_opp
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_REQUIRE_SHADOW_FOR_OPP
- READ: bot_config.py:L2557
- WRITE: cfg.rm.signal_policy.require_shadow_for_opp
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_REQUIRE_VOL_SIGNAL_FOR_OPP
- READ: bot_config.py:L2554
- WRITE: cfg.rm.signal_policy.require_vol_signal_for_opp
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_SEVERE_CAP_FACTOR
- READ: bot_config.py:L2537
- WRITE: cfg.rm.switch_knobs.severe_cap_factor
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_SEVERE_IOC_ONLY
- READ: bot_config.py:L2539
- WRITE: cfg.rm.switch_knobs.severe_ioc_only
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_SEVERE_MM_ENABLE
- READ: bot_config.py:L2538
- WRITE: cfg.rm.switch_knobs.severe_mm_enable
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_SEVERE_TM_MIN_BPS_DELTA
- READ: bot_config.py:L2534
- WRITE: cfg.rm.switch_knobs.severe_tm_min_bps_delta
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_SEVERE_TT_MIN_BPS_DELTA
- READ: bot_config.py:L2531
- WRITE: cfg.rm.switch_knobs.severe_tt_min_bps_delta
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_SPLIT_BREACH_MIN_S
- READ: bot_config.py:L2592
- WRITE: cfg.rm.split_breach_min_s
- USED:
  - .codex_env_extraction.json:2698
  - .codex_env_extraction.json:8048
  - .env.atlas:1498
- STATUS: USED

### RM_SPLIT_BREACH_THR_BASE_MS
- READ: bot_config.py:L2583
- WRITE: cfg.rm.split_breach_thr_base_ms
- USED:
  - .codex_env_extraction.json:2677
  - .codex_env_extraction.json:8021
  - .env.atlas:1503
- STATUS: USED

### RM_SPLIT_BREACH_THR_SKEW_MS
- READ: bot_config.py:L2586
- WRITE: cfg.rm.split_breach_thr_skew_ms
- USED:
  - .codex_env_extraction.json:2684
  - .codex_env_extraction.json:8030
  - .env.atlas:1508
- STATUS: USED

### RM_SPLIT_BREACH_THR_STALE_MS
- READ: bot_config.py:L2589
- WRITE: cfg.rm.split_breach_thr_stale_ms
- USED:
  - .codex_env_extraction.json:2691
  - .codex_env_extraction.json:8039
  - .env.atlas:1513
- STATUS: USED

### RM_SPLIT_FALLBACK_COOLDOWN_S
- READ: bot_config.py:L2595
- WRITE: cfg.rm.split_fallback_cooldown_s
- USED:
  - .codex_env_extraction.json:2705
  - .codex_env_extraction.json:8057
  - .env.atlas:1518
- STATUS: USED

### RM_SPLIT_PENALTY_BPS_MAX
- READ: bot_config.py:L2601
- WRITE: cfg.rm.split_penalty_bps_max
- USED:
  - .codex_env_extraction.json:2719
  - .codex_env_extraction.json:8075
  - .env.atlas:1523
- STATUS: USED

### RM_SPLIT_RESTORE_STABLE_S
- READ: bot_config.py:L2598
- WRITE: cfg.rm.split_restore_stable_s
- USED:
  - .codex_env_extraction.json:2712
  - .codex_env_extraction.json:8066
  - .env.atlas:1528
- STATUS: USED

### RM_SWITCH_ENTER_HYST_S
- READ: bot_config.py:L2461
- WRITE: cfg.rm.switch_knobs.enter_hyst_s
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_SWITCH_EXIT_HYST_S
- READ: bot_config.py:L2462
- WRITE: cfg.rm.switch_knobs.exit_hyst_s
- USED: none
- STATUS: NO_USAGE_FOUND

### RM_TTL_FACTOR_MM_DEGRADED
- READ: bot_config.py:L2447
- WRITE: cfg.rm.ttl_factor_mm_degraded
- USED:
  - .codex_env_extraction.json:2383
  - .codex_env_extraction.json:7643
  - .env.atlas:1533
- STATUS: USED

### RM_TTL_FACTOR_REB_DEGRADED
- READ: bot_config.py:L2444
- WRITE: cfg.rm.ttl_factor_reb_degraded
- USED:
  - .codex_env_extraction.json:2376
  - .codex_env_extraction.json:7634
  - .env.atlas:1538
- STATUS: USED

### RM_TTL_FACTOR_TM_DEGRADED
- READ: bot_config.py:L2441
- WRITE: cfg.rm.ttl_factor_tm_degraded
- USED:
  - .codex_env_extraction.json:2369
  - .codex_env_extraction.json:7625
  - .env.atlas:1543
- STATUS: USED

### RM_TTL_FACTOR_TT_DEGRADED
- READ: bot_config.py:L2438
- WRITE: cfg.rm.ttl_factor_tt_degraded
- USED:
  - .codex_env_extraction.json:2362
  - .codex_env_extraction.json:7616
  - .env.atlas:1548
- STATUS: USED

### RM_TTTM_EXPOSURE_BY_ASSET
- READ: bot_config.py:L2923
- WRITE: cfg.rm.tttm_exposure_by_asset
- USED:
  - .codex_env_extraction.json:3181
  - .codex_env_extraction.json:8647
  - .env.atlas:1553
- STATUS: USED

### RM_TTTM_EXPOSURE_HARD_USD
- READ: bot_config.py:L2918
- WRITE: cfg.rm.tttm_exposure_hard_usd
- USED:
  - .codex_env_extraction.json:3174
  - .codex_env_extraction.json:8638
  - .env.atlas:1558
- STATUS: USED

### RM_TTTM_EXPOSURE_SOFT_USD
- READ: bot_config.py:L2914
- WRITE: cfg.rm.tttm_exposure_soft_usd
- USED:
  - .codex_env_extraction.json:3167
  - .codex_env_extraction.json:8629
  - .env.atlas:1563
- STATUS: USED

### RM_TT_STUCK_HARD_USD
- READ: bot_config.py:L2933
- WRITE: cfg.rm.tt_stuck_hard_usd
- USED:
  - .codex_env_extraction.json:3195
  - .codex_env_extraction.json:8665
  - .env.atlas:1568
- STATUS: USED

### RM_TT_STUCK_MAX_AGE_S
- READ: bot_config.py:L2937
- WRITE: cfg.rm.tt_stuck_max_age_s
- USED:
  - .codex_env_extraction.json:3202
  - .codex_env_extraction.json:8674
  - .env.atlas:1573
  - risk_manager.py:2621
- STATUS: USED

### RM_TT_STUCK_SOFT_USD
- READ: bot_config.py:L2929
- WRITE: cfg.rm.tt_stuck_soft_usd
- USED:
  - .codex_env_extraction.json:3188
  - .codex_env_extraction.json:8656
  - .env.atlas:1578
- STATUS: USED

### ROUTER_BACKLOG_CRIT
- READ: bot_config.py:L2001
- WRITE: cfg.wd.router_backlog_crit
- USED:
  - .codex_env_extraction.json:1312
  - .codex_env_extraction.json:6242
  - .env.atlas:2611
- STATUS: USED

### ROUTER_BACKLOG_WARN
- READ: bot_config.py:L2000
- WRITE: cfg.wd.router_backlog_warn
- USED:
  - .codex_env_extraction.json:1305
  - .codex_env_extraction.json:6233
  - .env.atlas:2616
- STATUS: USED

### ROUTER_BACKPRESSURE
- READ: bot_config.py:L1938
- WRITE: cfg.router.backpressure
- USED:
  - .codex_env_extraction.json:1109
  - .codex_env_extraction.json:5981
  - .env.atlas:1886
- STATUS: USED

### ROUTER_CB_COALESCE_BUMP_MS
- READ: bot_config.py:L1939
- WRITE: cfg.router.cb_coalesce_bump_ms
- USED:
  - .codex_env_extraction.json:1116
  - .codex_env_extraction.json:5990
  - .env.atlas:1891
- STATUS: USED

### ROUTER_COALESCE_MAXLEN
- READ: bot_config.py:L1924
- WRITE: cfg.router.coalesce_maxlen
- USED:
  - .codex_env_extraction.json:1081
  - .codex_env_extraction.json:5938
  - .env.atlas:1896
- STATUS: USED

### ROUTER_COALESCE_WINDOW_MS
- READ: bot_config.py:L1890
- WRITE: cfg.router.coalesce_window_ms
- USED:
  - .codex_env_extraction.json:1004
  - .codex_env_extraction.json:5832
  - .env.atlas:1901
- STATUS: USED

### ROUTER_DEQUE_MAXLEN_PER_EX
- READ: bot_config.py:L1937
- WRITE: cfg.router.deque_maxlen_per_ex
- USED:
  - .codex_env_extraction.json:1102
  - .codex_env_extraction.json:5972
  - .env.atlas:1906
- STATUS: USED

### ROUTER_DETERMINISTIC_BACKPRESSURE
- READ: bot_config.py:L1944
- WRITE: cfg.router.deterministic_backpressure
- USED:
  - .codex_env_extraction.json:1137
  - .codex_env_extraction.json:6017
  - .env.atlas:1911
- STATUS: USED

### ROUTER_DROP_POLICY
- READ: bot_config.py:L1929,1932
- WRITE: cfg.router.drop_policy
- USED: none
- STATUS: NO_USAGE_FOUND

### ROUTER_ESCALATE_AFTER_CYCLES
- READ: bot_config.py:L2002
- WRITE: cfg.wd.router_escalate_after_cycles
- USED:
  - .codex_env_extraction.json:1319
  - .codex_env_extraction.json:6251
  - .env.atlas:2621
- STATUS: USED

### ROUTER_FAIL_FAST_ON_EVENT_EXCEPTION
- READ: bot_config.py:L1953
- WRITE: cfg.router.fail_fast_on_event_exception
- USED:
  - .codex_env_extraction.json:1158
  - .codex_env_extraction.json:6044
  - .env.atlas:1916
- STATUS: USED

### ROUTER_HB_ONCHANGE_BPS
- READ: bot_config.py:L1923
- WRITE: cfg.router.hb_onchange_bps
- USED:
  - .codex_env_extraction.json:1074
  - .codex_env_extraction.json:5929
  - .env.atlas:1921
- STATUS: USED

### ROUTER_HEALTH_MIN_COVERAGE_RATIO
- READ: bot_config.py:L1994
- WRITE: cfg.wd.router_health_min_coverage_ratio
- USED:
  - .codex_env_extraction.json:1277
  - .codex_env_extraction.json:6197
  - .env.atlas:2626
- STATUS: USED

### ROUTER_HEALTH_QUEUE_MAX
- READ: bot_config.py:L1997
- WRITE: cfg.wd.router_health_queue_max
- USED:
  - .codex_env_extraction.json:1284
  - .codex_env_extraction.json:6206
  - .env.atlas:2631
- STATUS: USED

### ROUTER_HEALTH_STALE_MS
- READ: bot_config.py:L1993
- WRITE: cfg.wd.router_health_stale_ms
- USED:
  - .codex_env_extraction.json:1270
  - .codex_env_extraction.json:6188
  - .env.atlas:2636
- STATUS: USED

### ROUTER_MUX_POLL_MS
- READ: bot_config.py:L1919
- WRITE: cfg.router.mux_poll_ms
- USED:
  - .codex_env_extraction.json:1046
  - .codex_env_extraction.json:5893
  - .env.atlas:1926
- STATUS: USED

### ROUTER_OUT_QUEUES_MAXLEN
- READ: bot_config.py:L1915
- WRITE: cfg.router.out_queues_maxlen
- USED:
  - .codex_env_extraction.json:1032
  - .codex_env_extraction.json:1039
  - .codex_env_extraction.json:5875
  - .codex_env_extraction.json:5884
  - .env.atlas:1931
  - .env.atlas:1936
- STATUS: USED

### ROUTER_OUT_QUEUES_MAXLEN_BY_KIND
- READ: bot_config.py:L1916
- WRITE: cfg.router.out_queues_maxlen_by_kind
- USED:
  - .codex_env_extraction.json:1039
  - .codex_env_extraction.json:5884
  - .env.atlas:1936
- STATUS: USED

### ROUTER_R2S_CRIT_MS
- READ: bot_config.py:L1999
- WRITE: cfg.wd.router_r2s_crit_ms
- USED:
  - .codex_env_extraction.json:1298
  - .codex_env_extraction.json:6224
  - .env.atlas:2641
- STATUS: USED

### ROUTER_R2S_WARN_MS
- READ: bot_config.py:L1998
- WRITE: cfg.wd.router_r2s_warn_ms
- USED:
  - .codex_env_extraction.json:1291
  - .codex_env_extraction.json:6215
  - .env.atlas:2646
- STATUS: USED

### ROUTER_REQUIRE_L2_FIRST
- READ: bot_config.py:L1920
- WRITE: cfg.router.require_l2_first
- USED:
  - .codex_env_extraction.json:1053
  - .codex_env_extraction.json:5902
  - .env.atlas:1941
- STATUS: USED

### ROUTER_SCANNER_QUEUE_DROP_POLICY
- READ: bot_config.py:L1950
- WRITE: cfg.router.scanner_queue_drop_policy
- USED:
  - .codex_env_extraction.json:1151
  - .codex_env_extraction.json:6035
  - .env.atlas:1946
- STATUS: USED

### ROUTER_SCANNER_QUEUE_MAXLEN
- READ: bot_config.py:L1947
- WRITE: cfg.router.scanner_queue_maxlen
- USED:
  - .codex_env_extraction.json:1144
  - .codex_env_extraction.json:6026
  - .env.atlas:1951
- STATUS: USED

### ROUTER_SHARDS_PER_EXCHANGE
- READ: bot_config.py:L1893,1897
- WRITE: cfg.router.shards_per_exchange
- USED: none
- STATUS: NO_USAGE_FOUND

### ROUTER_SLIP_ONCHANGE_BPS
- READ: bot_config.py:L1922
- WRITE: cfg.router.slip_onchange_bps
- USED:
  - .codex_env_extraction.json:1067
  - .codex_env_extraction.json:5920
  - .env.atlas:1956
- STATUS: USED

### ROUTER_STALENESS_MODE
- READ: bot_config.py:L1943
- WRITE: cfg.router.staleness_mode
- USED:
  - .codex_env_extraction.json:1130
  - .codex_env_extraction.json:6008
  - .env.atlas:1961
- STATUS: USED

### ROUTER_STALE_MS
- READ: bot_config.py:L1891
- WRITE: cfg.router.stale_ms
- USED:
  - .codex_env_extraction.json:1011
  - .codex_env_extraction.json:5841
  - .env.atlas:1966
- STATUS: USED

### ROUTER_TOPIC_MAX_HZ
- READ: bot_config.py:L1942
- WRITE: cfg.router.topic_max_hz
- USED:
  - .codex_env_extraction.json:1123
  - .codex_env_extraction.json:5999
  - .env.atlas:1971
- STATUS: USED

### ROUTER_VOL_ONCHANGE_BPS
- READ: bot_config.py:L1921
- WRITE: cfg.router.vol_onchange_bps
- USED:
  - .codex_env_extraction.json:1060
  - .codex_env_extraction.json:5911
  - .env.atlas:1976
- STATUS: USED

### ROUTER_WS_SHARDS
- READ: bot_config.py:L1911
- WRITE: cfg.router.ws_shards_by_exchange
- USED:
  - .codex_env_extraction.json:1025
  - .codex_env_extraction.json:5866
  - .env.atlas:1981
- STATUS: USED

### ROUTER_WS_SOURCE_BACKPRESSURE_COOLDOWN_S
- READ: bot_config.py:L1925
- WRITE: cfg.router.ws_source_backpressure_cooldown_s
- USED:
  - .codex_env_extraction.json:1088
  - .codex_env_extraction.json:5947
  - .env.atlas:1986
- STATUS: USED

### RPC_CA_CERT
- READ: bot_config.py:L3401
- WRITE: cfg.rpc.ca_cert
- USED:
  - .codex_env_extraction.json:10430
  - .codex_env_extraction.json:4567
  - .env.atlas:1993
- STATUS: USED

### RPC_CERT_PATHS
- READ: bot_config.py:L3412
- WRITE: cfg.rpc.rpc_cert_paths
- USED:
  - .codex_env_extraction.json:10529
  - .codex_env_extraction.json:4644
  - .env.atlas:1998
- STATUS: USED

### RPC_CLIENT_BASE
- READ: bot_config.py:L3408
- WRITE: cfg.rpc.rpc_client_base
- USED:
  - .codex_env_extraction.json:10493
  - .codex_env_extraction.json:4616
  - .env.atlas:2003
- STATUS: USED

### RPC_CLIENT_CERT
- READ: bot_config.py:L3404
- WRITE: cfg.rpc.client_cert
- USED:
  - .codex_env_extraction.json:10457
  - .codex_env_extraction.json:4588
  - .env.atlas:2008
- STATUS: USED

### RPC_CLIENT_KEY
- READ: bot_config.py:L3405
- WRITE: cfg.rpc.client_key
- USED:
  - .codex_env_extraction.json:10466
  - .codex_env_extraction.json:4595
  - .env.atlas:2013
- STATUS: USED

### RPC_ENABLED
- READ: bot_config.py:L3391
- WRITE: cfg.rpc.enabled
- USED:
  - .codex_env_extraction.json:10340
  - .codex_env_extraction.json:4497
  - .env.atlas:2018
- STATUS: USED

### RPC_ENABLE_MTLS
- READ: bot_config.py:L3411
- WRITE: cfg.rpc.rpc_enable_mtls
- USED:
  - .codex_env_extraction.json:10520
  - .codex_env_extraction.json:4637
  - .env.atlas:2023
- STATUS: USED

### RPC_HOST
- READ: bot_config.py:L3392
- WRITE: cfg.rpc.host
- USED:
  - .codex_env_extraction.json:10349
  - .codex_env_extraction.json:4504
  - .env.atlas:2028
- STATUS: USED

### RPC_LEGACY_TIMEOUT_S
- READ: bot_config.py:L3409
- WRITE: cfg.rpc.rpc_timeout_s
- USED:
  - .codex_env_extraction.json:10502
  - .codex_env_extraction.json:4623
  - .env.atlas:2033
- STATUS: USED

### RPC_LOOPBACK_INPROC
- READ: bot_config.py:L3399
- WRITE: cfg.rpc.loopback_inproc
- USED:
  - .codex_env_extraction.json:10412
  - .codex_env_extraction.json:4553
  - .env.atlas:2038
- STATUS: USED

### RPC_MAX_PAYLOAD_KB
- READ: bot_config.py:L3413
- WRITE: cfg.rpc.rpc_max_payload_kb
- USED:
  - .codex_env_extraction.json:10538
  - .codex_env_extraction.json:4651
  - .env.atlas:2043
- STATUS: USED

### RPC_MAX_RETRIES
- READ: bot_config.py:L3396
- WRITE: cfg.rpc.max_retries
- USED:
  - .codex_env_extraction.json:10385
  - .codex_env_extraction.json:4532
  - .env.atlas:2048
- STATUS: USED

### RPC_MTLS_ENABLED
- READ: bot_config.py:L3398
- WRITE: cfg.rpc.mtls_enabled
- USED:
  - .codex_env_extraction.json:10403
  - .codex_env_extraction.json:4546
  - .env.atlas:2053
- STATUS: USED

### RPC_PORT
- READ: bot_config.py:L3393
- WRITE: cfg.rpc.port
- USED:
  - .codex_env_extraction.json:10358
  - .codex_env_extraction.json:4511
  - .env.atlas:2058
- STATUS: USED

### RPC_READY_STRICT
- READ: bot_config.py:L3400
- WRITE: cfg.rpc.ready_strict
- USED:
  - .codex_env_extraction.json:10421
  - .codex_env_extraction.json:4560
  - .env.atlas:2063
- STATUS: USED

### RPC_REGION
- READ: bot_config.py:L3394
- WRITE: cfg.rpc.region
- USED:
  - .codex_env_extraction.json:10367
  - .codex_env_extraction.json:4518
  - .env.atlas:2068
- STATUS: USED

### RPC_REQUIRE_CLIENT_CERT
- READ: bot_config.py:L3406
- WRITE: cfg.rpc.require_client_cert
- USED:
  - .codex_env_extraction.json:10475
  - .codex_env_extraction.json:4602
  - .env.atlas:2073
- STATUS: USED

### RPC_RETRIES
- READ: bot_config.py:L3410
- WRITE: cfg.rpc.rpc_retries
- USED:
  - .codex_env_extraction.json:10511
  - .codex_env_extraction.json:4630
  - .env.atlas:2078
- STATUS: USED

### RPC_SERVER_BIND
- READ: bot_config.py:L3407
- WRITE: cfg.rpc.rpc_server_bind
- USED:
  - .codex_env_extraction.json:10484
  - .codex_env_extraction.json:4609
  - .env.atlas:2083
- STATUS: USED

### RPC_SERVER_CERT
- READ: bot_config.py:L3402
- WRITE: cfg.rpc.server_cert
- USED:
  - .codex_env_extraction.json:10439
  - .codex_env_extraction.json:4574
  - .env.atlas:2088
- STATUS: USED

### RPC_SERVER_KEY
- READ: bot_config.py:L3403
- WRITE: cfg.rpc.server_key
- USED:
  - .codex_env_extraction.json:10448
  - .codex_env_extraction.json:4581
  - .env.atlas:2093
- STATUS: USED

### RPC_TIMEOUT_MS
- READ: bot_config.py:L3397
- WRITE: cfg.rpc.timeout_ms
- USED:
  - .codex_env_extraction.json:10394
  - .codex_env_extraction.json:4539
  - .env.atlas:2098
- STATUS: USED

### RPC_TIMEOUT_S
- READ: bot_config.py:L3395
- WRITE: cfg.rpc.timeout_s
- USED:
  - .codex_env_extraction.json:10376
  - .codex_env_extraction.json:4525
  - .env.atlas:2103
- STATUS: USED

### SCANNER_ALLOW_LOSS_BPS_REBAL
- READ: bot_config.py:L2715
- WRITE: cfg.scanner.allow_loss_bps_rebal
- USED:
  - .codex_env_extraction.json:2936
  - .codex_env_extraction.json:8327
  - .env.atlas:2110
- STATUS: USED

### SCANNER_BACKPRESSURE_LOG_EVERY
- READ: bot_config.py:L2689
- WRITE: cfg.scanner.backpressure_log_every
- USED:
  - .codex_env_extraction.json:2880
  - .codex_env_extraction.json:8255
  - .env.atlas:2115
- STATUS: USED

### SCANNER_BINANCE_DEPTH_LEVEL
- READ: bot_config.py:L2664
- WRITE: cfg.scanner.binance_depth_level
- USED:
  - .codex_env_extraction.json:2810
  - .codex_env_extraction.json:8165
  - .env.atlas:2120
- STATUS: USED

### SCANNER_DEDUP_COOLDOWN_S
- READ: bot_config.py:L2726
- WRITE: cfg.SCANNER_DEDUP_COOLDOWN_S
- USED:
  - .codex_env_extraction.json:2964
  - .codex_env_extraction.json:8361
  - .env.atlas:2252
  - opportunity_scanner.py:1729
- STATUS: USED

### SCANNER_DEDUP_WINDOW_S
- READ: bot_config.py:L2684
- WRITE: cfg.scanner.dedup_window_s
- USED:
  - .codex_env_extraction.json:2859
  - .codex_env_extraction.json:8228
  - .env.atlas:2125
- STATUS: USED

### SCANNER_DEQUE_MAX
- READ: bot_config.py:L2725
- WRITE: cfg.SCANNER_DEQUE_MAX
- USED:
  - .codex_env_extraction.json:2957
  - .codex_env_extraction.json:8352
  - .env.atlas:2259
  - opportunity_scanner.py:1729
- STATUS: USED

### SCANNER_ENABLE_MM_HINTS
- READ: bot_config.py:L2663
- WRITE: cfg.scanner.enable_mm_hints
- USED:
  - .codex_env_extraction.json:2803
  - .codex_env_extraction.json:8156
  - .env.atlas:2130
- STATUS: USED

### SCANNER_EVAL_HZ_AUDITION
- READ: bot_config.py:L2709
- WRITE: cfg.scanner.scanner_eval_hz_audition
- USED:
  - .codex_env_extraction.json:2922
  - .codex_env_extraction.json:8309
  - .env.atlas:2135
- STATUS: USED

### SCANNER_EVAL_HZ_CORE
- READ: bot_config.py:L2706
- WRITE: cfg.scanner.scanner_eval_hz_core
- USED:
  - .codex_env_extraction.json:2915
  - .codex_env_extraction.json:8300
  - .env.atlas:2140
- STATUS: USED

### SCANNER_EVAL_HZ_PRIMARY
- READ: bot_config.py:L2703
- WRITE: cfg.scanner.scanner_eval_hz_primary
- USED:
  - .codex_env_extraction.json:2908
  - .codex_env_extraction.json:8291
  - .env.atlas:2145
- STATUS: USED

### SCANNER_EVAL_HZ_SANDBOX
- READ: bot_config.py:L2712
- WRITE: cfg.scanner.scanner_eval_hz_sandbox
- USED:
  - .codex_env_extraction.json:2929
  - .codex_env_extraction.json:8318
  - .env.atlas:2150
- STATUS: USED

### SCANNER_GLOBAL_EVAL_HZ
- READ: bot_config.py:L2756
- WRITE: cfg.SCANNER_GLOBAL_EVAL_HZ
- USED:
  - .codex_env_extraction.json:11087
  - .codex_env_extraction.json:5127
  - .env.atlas:2266
- STATUS: USED

### SCANNER_HZ
- READ: bot_config.py:L2723,2724
- WRITE: cfg.SCANNER_HZ
- USED:
  - .codex_env_extraction.json:2950
  - .codex_env_extraction.json:8343
  - opportunity_scanner.py:1729
- STATUS: USED

### SCANNER_MAX_OPPORTUNITIES
- READ: bot_config.py:L2692
- WRITE: cfg.scanner.max_opportunities
- USED:
  - .codex_env_extraction.json:2887
  - .codex_env_extraction.json:8264
  - .env.atlas:2155
- STATUS: USED

### SCANNER_MAX_PAIRS_PER_TICK
- READ: bot_config.py:L2685
- WRITE: cfg.scanner.max_pairs_per_tick
- USED:
  - .codex_env_extraction.json:2866
  - .codex_env_extraction.json:8237
  - .env.atlas:2160
- STATUS: USED

### SCANNER_MAX_TIME_SKEW_S
- READ: bot_config.py:L2695
- WRITE: cfg.scanner.max_time_skew_s
- USED:
  - .codex_env_extraction.json:2894
  - .codex_env_extraction.json:8273
  - .env.atlas:2165
- STATUS: USED

### SCANNER_MM_DEPTH_MIN_QUOTE
- READ: bot_config.py:L2673
- WRITE: cfg.scanner.mm_depth_min_quote
- USED:
  - .codex_env_extraction.json:2824
  - .codex_env_extraction.json:8183
  - .env.atlas:2170
- STATUS: USED

### SCANNER_MM_HEDGE_COST_BPS
- READ: bot_config.py:L2680
- WRITE: cfg.scanner.mm_hedge_cost_bps
- USED:
  - .codex_env_extraction.json:2845
  - .codex_env_extraction.json:8210
  - .env.atlas:2175
- STATUS: USED

### SCANNER_MM_MIN_NET_BPS
- READ: bot_config.py:L2679
- WRITE: cfg.scanner.mm_min_net_bps
- USED:
  - .codex_env_extraction.json:2838
  - .codex_env_extraction.json:8201
  - .env.atlas:2180
- STATUS: USED

### SCANNER_MM_QPOS_MAX_AHEAD_QUOTE
- READ: bot_config.py:L2676
- WRITE: cfg.scanner.mm_qpos_max_ahead_quote
- USED:
  - .codex_env_extraction.json:2831
  - .codex_env_extraction.json:8192
  - .env.atlas:2185
- STATUS: USED

### SCANNER_MM_ROTATION_ENABLED
- READ: bot_config.py:L2667
- WRITE: cfg.scanner.mm_rotation_enabled
- USED:
  - .codex_env_extraction.json:2817
  - .codex_env_extraction.json:8174
  - .env.atlas:2190
- STATUS: USED

### SCANNER_MM_SEED_PAIRS
- READ: bot_config.py:L2671
- WRITE: cfg.scanner.mm_seed_pairs
- USED:
  - .codex_env_extraction.json:11078
  - .codex_env_extraction.json:5120
  - .env.atlas:2195
- STATUS: USED

### SCANNER_MM_VOL_BPS_MAX
- READ: bot_config.py:L2681
- WRITE: cfg.scanner.mm_vol_bps_max
- USED:
  - .codex_env_extraction.json:2852
  - .codex_env_extraction.json:8219
  - .env.atlas:2200
- STATUS: USED

### SCANNER_MODE
- READ: bot_config.py:L2662
- WRITE: cfg.scanner.scanner_mode
- USED:
  - .codex_env_extraction.json:11069
  - .codex_env_extraction.json:5113
  - .env.atlas:2205
- STATUS: USED

### SCANNER_RM_ACK_TIMEOUT_S
- READ: bot_config.py:L2698
- WRITE: cfg.scanner.rm_ack_timeout_s
- USED:
  - .codex_env_extraction.json:2901
  - .codex_env_extraction.json:8282
  - .env.atlas:2210
- STATUS: USED

### SCANNER_SCAN_INTERVAL_S
- READ: bot_config.py:L2688
- WRITE: cfg.scanner.scan_interval
- USED:
  - .codex_env_extraction.json:2873
  - .codex_env_extraction.json:8246
  - .env.atlas:2215
- STATUS: USED

### SCANNER_SHED_AUDITION_FACTOR
- READ: bot_config.py:L2656
- WRITE: cfg.scanner.shed_audition_factor
- USED:
  - .codex_env_extraction.json:2789
  - .codex_env_extraction.json:8138
  - .env.atlas:2220
- STATUS: USED

### SCANNER_SHED_COOLDOWN_S
- READ: bot_config.py:L2659
- WRITE: cfg.scanner.shed_cooldown_s
- USED:
  - .codex_env_extraction.json:2796
  - .codex_env_extraction.json:8147
  - .env.atlas:2225
- STATUS: USED

### SCANNER_SHED_LOAD_THRESHOLD
- READ: bot_config.py:L2647
- WRITE: cfg.scanner.shed_load_threshold
- USED:
  - .codex_env_extraction.json:2768
  - .codex_env_extraction.json:8111
  - .env.atlas:2230
- STATUS: USED

### SCANNER_SHED_PRIMARY_FACTOR
- READ: bot_config.py:L2650
- WRITE: cfg.scanner.shed_primary_factor
- USED:
  - .codex_env_extraction.json:2775
  - .codex_env_extraction.json:8120
  - .env.atlas:2235
- STATUS: USED

### SCANNER_SHED_PRIMARY_MIN
- READ: bot_config.py:L2653
- WRITE: cfg.scanner.shed_primary_min
- USED:
  - .codex_env_extraction.json:2782
  - .codex_env_extraction.json:8129
  - .env.atlas:2240
- STATUS: USED

### SCANNER_WORKERS
- READ: bot_config.py:L2646
- WRITE: cfg.scanner.workers
- USED:
  - .codex_env_extraction.json:2761
  - .codex_env_extraction.json:8102
  - .env.atlas:2245
- STATUS: USED

### SIMULATOR_BYPASS_ALLOWED_IN_LIVE
- READ: bot_config.py:L3030
- WRITE: cfg.sim.simulator_bypass_allowed_in_live
- USED:
  - .codex_env_extraction.json:3356
  - .codex_env_extraction.json:8870
  - .env.atlas:2307
- STATUS: USED

### SIMULATOR_MODE
- READ: bot_config.py:L3029
- WRITE: cfg.sim.simulator_mode
- USED:
  - .codex_env_extraction.json:11205
  - .codex_env_extraction.json:5288
  - .env.atlas:2312
- STATUS: USED

### SIM_BOOK_FINGERPRINT_LEVELS
- READ: bot_config.py:L3065
- WRITE: cfg.sim.sim_book_fingerprint_levels
- USED:
  - .codex_env_extraction.json:3447
  - .codex_env_extraction.json:8987
  - .env.atlas:2317
- STATUS: USED

### SIM_BOOK_FINGERPRINT_MODE
- READ: bot_config.py:L3063
- WRITE: cfg.sim.sim_book_fingerprint_mode
- USED:
  - .codex_env_extraction.json:11214
  - .codex_env_extraction.json:5295
  - .env.atlas:2322
- STATUS: USED

### SIM_BUCKETS_ADAPT_WITH_VOL
- READ: bot_config.py:L3050
- WRITE: cfg.sim.sim_buckets_adapt_with_vol
- USED:
  - .codex_env_extraction.json:3419
  - .codex_env_extraction.json:8951
  - .env.atlas:2327
- STATUS: USED

### SIM_BUCKETS_PER_PROFILE
- READ: bot_config.py:L3046
- WRITE: cfg.sim.sim_buckets_per_profile
- USED:
  - .codex_env_extraction.json:3412
  - .codex_env_extraction.json:8942
  - .env.atlas:2332
- STATUS: USED

### SIM_CACHE_TTL_MS
- READ: bot_config.py:L3033
- WRITE: cfg.sim.sim_cache_ttl_ms
- USED:
  - .codex_env_extraction.json:3363
  - .codex_env_extraction.json:8879
  - .env.atlas:2337
- STATUS: USED

### SIM_DETERMINISTIC_TRADE_ID
- READ: bot_config.py:L3040
- WRITE: cfg.sim.sim_deterministic_trade_id
- USED:
  - .codex_env_extraction.json:3398
  - .codex_env_extraction.json:8924
  - .env.atlas:2342
- STATUS: USED

### SIM_MAX_FRAGMENTS
- READ: bot_config.py:L3027
- WRITE: cfg.sim.max_fragments
- USED:
  - .codex_env_extraction.json:3342
  - .codex_env_extraction.json:8852
  - .env.atlas:2347
- STATUS: USED

### SIM_MAX_INFLIGHT_JOBS
- READ: bot_config.py:L3036
- WRITE: cfg.sim.sim_max_inflight_jobs
- USED:
  - .codex_env_extraction.json:3384
  - .codex_env_extraction.json:8906
  - .env.atlas:2352
- STATUS: USED

### SIM_MAX_WAIT_MS_RM
- READ: bot_config.py:L3034
- WRITE: cfg.sim.sim_max_wait_ms_rm
- USED:
  - .codex_env_extraction.json:3370
  - .codex_env_extraction.json:8888
  - .env.atlas:2357
- STATUS: USED

### SIM_MIN_FRAGMENT_USDC
- READ: bot_config.py:L3028
- WRITE: cfg.sim.min_fragment_usdc
- USED:
  - .codex_env_extraction.json:3349
  - .codex_env_extraction.json:8861
  - .env.atlas:2362
- STATUS: USED

### SIM_MM_HINTS_INTERVAL_MS
- READ: bot_config.py:L3068
- WRITE: cfg.sim.sim_mm_hints_interval_ms
- USED:
  - .codex_env_extraction.json:3454
  - .codex_env_extraction.json:8996
  - .env.atlas:2367
- STATUS: USED

### SIM_MM_HINTS_LEVELS
- READ: bot_config.py:L3071
- WRITE: cfg.sim.sim_mm_hints_levels
- USED:
  - .codex_env_extraction.json:3461
  - .codex_env_extraction.json:9005
  - .env.atlas:2372
- STATUS: USED

### SIM_NOTIONAL_BUCKETS_BASE
- READ: bot_config.py:L3043
- WRITE: cfg.sim.sim_notional_buckets_base
- USED:
  - .codex_env_extraction.json:3405
  - .codex_env_extraction.json:8933
  - .env.atlas:2377
- STATUS: USED

### SIM_OUTPUTS_REQUIRED_BY_BRANCH
- READ: bot_config.py:L3074
- WRITE: cfg.sim.sim_outputs_required_by_branch
- USED:
  - .codex_env_extraction.json:3468
  - .codex_env_extraction.json:9014
  - .env.atlas:2382
- STATUS: USED

### SIM_PRIME_DEPTH_LEVELS
- READ: bot_config.py:L3035
- WRITE: cfg.sim.sim_prime_depth_levels
- USED:
  - .codex_env_extraction.json:3377
  - .codex_env_extraction.json:8897
  - .env.atlas:2387
- STATUS: USED

### SIM_PRIME_MULTIBUCKET_K
- READ: bot_config.py:L3059
- WRITE: cfg.sim.sim_prime_multibucket_k
- USED:
  - .codex_env_extraction.json:3440
  - .codex_env_extraction.json:8978
  - .env.atlas:2392
- STATUS: USED

### SIM_SHADOW_MAX_INFLIGHT
- READ: bot_config.py:L3037
- WRITE: cfg.sim.sim_shadow_max_inflight
- USED:
  - .codex_env_extraction.json:3391
  - .codex_env_extraction.json:8915
  - .env.atlas:2397
- STATUS: USED

### SIM_VOL_SIZE_FACTOR_CEIL
- READ: bot_config.py:L3056
- WRITE: cfg.sim.sim_vol_size_factor_ceil
- USED:
  - .codex_env_extraction.json:3433
  - .codex_env_extraction.json:8969
  - .env.atlas:2402
- STATUS: USED

### SIM_VOL_SIZE_FACTOR_FLOOR
- READ: bot_config.py:L3053
- WRITE: cfg.sim.sim_vol_size_factor_floor
- USED:
  - .codex_env_extraction.json:3426
  - .codex_env_extraction.json:8960
  - .env.atlas:2407
- STATUS: USED

### SLIP_HEARTBEAT_S
- READ: bot_config.py:L3334
- WRITE: cfg.slip.heartbeat_s
- USED:
  - .codex_env_extraction.json:10124
  - .codex_env_extraction.json:4329
  - .env.atlas:2444
- STATUS: USED

### SLIP_MAX_BPS_BY_QUOTE
- READ: bot_config.py:L3337
- WRITE: cfg.slip.max_bps_by_quote
- USED:
  - .codex_env_extraction.json:11232
  - .codex_env_extraction.json:5309
  - .env.atlas:2449
- STATUS: USED

### SLIP_TTL_S
- READ: bot_config.py:L1762,1773
- WRITE: —
- USED: none
- STATUS: NO_USAGE_FOUND

### SLIP_USE_VWAP_DEPTH
- READ: bot_config.py:L3335
- WRITE: cfg.slip.use_vwap_depth
- USED:
  - .codex_env_extraction.json:10133
  - .codex_env_extraction.json:4336
  - .env.atlas:2454
- STATUS: USED

### SPLIT_LATENCY
- READ: bot_config.py:L1737
- WRITE: cfg.g.split_latency
- USED:
  - risk_manager.py:10104
  - risk_manager.py:16055
- STATUS: USED

### STATUS_PORT
- READ: bot_config.py:L1842
- WRITE: cfg.obs.status_port
- USED:
  - .codex_env_extraction.json:5688
  - .codex_env_extraction.json:892
  - .env.atlas:825
  - smoke_test_all.py:1125
  - smoke_test_all.py:2210
  - smoke_test_all.py:2221
  - smoke_test_all.py:2436
  - smoke_test_all.py:2441
  - smoke_test_all.py:611
- STATUS: USED

### STRICT_OBS
- READ: bot_config.py:L1840,3429
- WRITE: cfg.lhm.strict_obs, cfg.obs.strict_obs
- USED:
  - .codex_env_extraction.json:4665
  - .codex_env_extraction.json:5663
  - .codex_env_extraction.json:5670
  - .codex_env_extraction.json:878
  - .env.atlas:830
- STATUS: USED

### TELEGRAM_ACK_PIN
- READ: bot_config.py:L1868
- WRITE: cfg.alerting.telegram.ack_pin
- USED:
  - .codex_env_extraction.json:5778
  - .codex_env_extraction.json:962
  - .env.atlas:22
- STATUS: USED

### TELEGRAM_ALLOWED_USER_IDS
- READ: bot_config.py:L1856
- WRITE: cfg.alerting.telegram.allowed_user_ids
- USED:
  - .codex_env_extraction.json:11187
  - .codex_env_extraction.json:5211
  - .env.atlas:27
- STATUS: USED

### TELEGRAM_BOT_TOKEN
- READ: bot_config.py:L1853
- WRITE: cfg.alerting.telegram.bot_token
- USED:
  - .codex_env_extraction.json:5742
  - .codex_env_extraction.json:934
  - .env.atlas:32
- STATUS: USED

### TELEGRAM_CHAT_ID_CRIT
- READ: bot_config.py:L1865
- WRITE: cfg.alerting.telegram.chat_id_crit
- USED:
  - .codex_env_extraction.json:5769
  - .codex_env_extraction.json:955
  - .env.atlas:37
- STATUS: USED

### TELEGRAM_CHAT_ID_INFO
- READ: bot_config.py:L1859
- WRITE: cfg.alerting.telegram.chat_id_info
- USED:
  - .codex_env_extraction.json:5751
  - .codex_env_extraction.json:941
  - .env.atlas:42
- STATUS: USED

### TELEGRAM_CHAT_ID_WARN
- READ: bot_config.py:L1862
- WRITE: cfg.alerting.telegram.chat_id_warn
- USED:
  - .codex_env_extraction.json:5760
  - .codex_env_extraction.json:948
  - .env.atlas:47
- STATUS: USED

### TELEGRAM_REQUIRE_ACK
- READ: bot_config.py:L1869
- WRITE: cfg.alerting.telegram.require_ack
- USED:
  - .codex_env_extraction.json:5787
  - .codex_env_extraction.json:969
  - .env.atlas:52
- STATUS: USED

### TM_EXPOSURE_TTL_HEDGE_RATIO
- READ: bot_config.py:L2987
- WRITE: cfg.engine.tm_exposure_ttl_hedge_ratio, cfg.rm.tm_neutral_hedge_ratio
- USED: none
- STATUS: NO_USAGE_FOUND

### TM_EXPOSURE_TTL_MS
- READ: bot_config.py:L3103
- WRITE: cfg.engine.tm_exposure_ttl_ms
- USED: none
- STATUS: NO_USAGE_FOUND

### TM_NEUTRAL_HEDGE_RATIO
- READ: bot_config.py:L2989
- WRITE: cfg.engine.tm_exposure_ttl_hedge_ratio, cfg.rm.tm_neutral_hedge_ratio
- USED: none
- STATUS: NO_USAGE_FOUND

### TM_NEUTRAL_HEDGE_RATIO_MAP
- READ: bot_config.py:L3329
- WRITE: cfg.vol.tm_neutral_hedge_ratio_map
- USED:
  - .codex_env_extraction.json:10097
  - .codex_env_extraction.json:4308
  - .env.atlas:2502
- STATUS: USED

### TM_NN_HEDGE_RATIO
- READ: bot_config.py:L3001
- WRITE: —
- USED: none
- STATUS: NO_USAGE_FOUND

### TM_QUEUEPOS_MAX_AHEAD_USD
- READ: bot_config.py:L2875
- WRITE: cfg.rm.tm_queuepos_max_ahead_usd
- USED:
  - .codex_env_extraction.json:3048
  - .codex_env_extraction.json:8469
  - .env.atlas:1583
- STATUS: USED

### TM_QUEUEPOS_MAX_ETA_MS
- READ: bot_config.py:L2879
- WRITE: cfg.rm.tm_queuepos_max_eta_ms
- USED:
  - .codex_env_extraction.json:3055
  - .codex_env_extraction.json:8478
  - .env.atlas:1588
- STATUS: USED

### TRANSFER_RETRY_POLICY
- READ: bot_config.py:L2334
- WRITE: cfg.rm.transfer_retry_policy
- USED: none
- STATUS: NO_USAGE_FOUND

### TRANSFER_SUBMITTED_TIMEOUT_S
- READ: bot_config.py:L2330
- WRITE: cfg.rm.transfer_submitted_timeout_s
- USED:
  - .codex_env_extraction.json:2201
  - .codex_env_extraction.json:7409
  - .env.atlas:1593
- STATUS: USED

### VM_MAKER_PAD_TICKS_MAP
- READ: bot_config.py:L3333
- WRITE: cfg.vol.vm_maker_pad_ticks_map
- USED:
  - .codex_env_extraction.json:10115
  - .codex_env_extraction.json:4322
  - .env.atlas:2507
- STATUS: USED

### VM_MIN_BPS_BOOST_MAP
- READ: bot_config.py:L3328
- WRITE: cfg.vol.vm_min_bps_boost_map
- USED:
  - .codex_env_extraction.json:10088
  - .codex_env_extraction.json:4301
  - .env.atlas:2512
- STATUS: USED

### VM_PRUDENCE_THRESHOLDS_BPS
- READ: bot_config.py:L3331
- WRITE: cfg.vol.vm_prudence_thresholds_bps
- USED:
  - .codex_env_extraction.json:10106
  - .codex_env_extraction.json:4315
  - .env.atlas:2517
- STATUS: USED

### VM_SIZE_FACTOR_MAP
- READ: bot_config.py:L3327
- WRITE: cfg.vol.vm_size_factor_map
- USED:
  - .codex_env_extraction.json:10079
  - .codex_env_extraction.json:4294
  - .env.atlas:2522
- STATUS: USED

### VOL_CHAOS_CAP_BPS
- READ: bot_config.py:L3319
- WRITE: cfg.vol.chaos_cap_bps
- USED:
  - .codex_env_extraction.json:10007
  - .codex_env_extraction.json:4238
  - .env.atlas:2527
- STATUS: USED

### VOL_EMA_ALPHA
- READ: bot_config.py:L3317
- WRITE: cfg.vol.ema_alpha
- USED:
  - .codex_env_extraction.json:4224
  - .codex_env_extraction.json:9989
  - .env.atlas:2532
- STATUS: USED

### VOL_HYSTERESIS
- READ: bot_config.py:L3320
- WRITE: cfg.vol.hysteresis
- USED:
  - .codex_env_extraction.json:10016
  - .codex_env_extraction.json:4245
  - .env.atlas:2537
- STATUS: USED

### VOL_MAX_SILENCE_S
- READ: bot_config.py:L3322
- WRITE: cfg.vol.max_silence_s
- USED:
  - .codex_env_extraction.json:10034
  - .codex_env_extraction.json:4259
  - .env.atlas:2542
- STATUS: USED

### VOL_MIDPRICE_TO_BPS
- READ: bot_config.py:L3314
- WRITE: cfg.vol.midprice_to_bps
- USED:
  - .codex_env_extraction.json:4203
  - .codex_env_extraction.json:9962
  - .env.atlas:2547
- STATUS: USED

### VOL_MIN_DELTA_BPS
- READ: bot_config.py:L3321
- WRITE: cfg.vol.vol_min_delta_bps
- USED:
  - .codex_env_extraction.json:10025
  - .codex_env_extraction.json:4252
  - .env.atlas:2552
- STATUS: USED

### VOL_SLIP_TTL
- READ: bot_config.py:L1759,1761
- WRITE: cfg.g.vol_slip_ttl
- USED: none
- STATUS: NO_USAGE_FOUND

### VOL_SOFT_CAP_BPS
- READ: bot_config.py:L3318
- WRITE: cfg.vol.soft_cap_bps
- USED:
  - .codex_env_extraction.json:4231
  - .codex_env_extraction.json:9998
  - .env.atlas:2557
- STATUS: USED

### VOL_TO_BPS_CAP
- READ: bot_config.py:L3316
- WRITE: cfg.vol.to_bps_cap
- USED:
  - .codex_env_extraction.json:4217
  - .codex_env_extraction.json:9980
  - .env.atlas:2562
- STATUS: USED

### VOL_TO_BPS_FLOOR
- READ: bot_config.py:L3315
- WRITE: cfg.vol.to_bps_floor
- USED:
  - .codex_env_extraction.json:4210
  - .codex_env_extraction.json:9971
  - .env.atlas:2567
- STATUS: USED

### VOL_TTL_S
- READ: bot_config.py:L1763,1777
- WRITE: —
- USED: none
- STATUS: NO_USAGE_FOUND

### VOL_WINDOW_LONG_M
- READ: bot_config.py:L3323
- WRITE: cfg.vol.window_long_m
- USED:
  - .codex_env_extraction.json:10043
  - .codex_env_extraction.json:4266
  - .env.atlas:2572
- STATUS: USED

### VOL_WINDOW_MICRO_M
- READ: bot_config.py:L3324
- WRITE: cfg.vol.window_micro_m
- USED:
  - .codex_env_extraction.json:10052
  - .codex_env_extraction.json:4273
  - .env.atlas:2577
- STATUS: USED

### VOL_WINSOR_PCT
- READ: bot_config.py:L3325
- WRITE: cfg.vol.winsor_pct
- USED:
  - .codex_env_extraction.json:10061
  - .codex_env_extraction.json:4280
  - .env.atlas:2582
  - volatility_monitor.py:149
- STATUS: USED

### WALLET_ALIASES
- READ: bot_config.py:L3530
- WRITE: cfg.wallet_aliases
- USED:
  - .codex_env_extraction.json:11024
  - .codex_env_extraction.json:5036
  - .env.atlas:2589
- STATUS: USED

### WALLET_ALIAS_BY_QUOTE
- READ: bot_config.py:L1749
- WRITE: cfg.g.wallet_alias_by_quote
- USED: none
- STATUS: NO_USAGE_FOUND

### WALLET_MISSING_LOG_INTERVAL_S
- READ: bot_config.py:L3307
- WRITE: cfg.balances.wallet_missing_log_interval_s
- USED:
  - .codex_env_extraction.json:4175
  - .codex_env_extraction.json:9926
  - .env.atlas:104
- STATUS: USED

### WATCHDOG_NOTIFY_ONLY
- READ: bot_config.py:L1990
- WRITE: cfg.wd.notify_only_default
- USED:
  - .codex_env_extraction.json:1256
  - .codex_env_extraction.json:6170
  - .env.atlas:2651
- STATUS: USED

### WD_BALANCE_ERROR_THRESHOLD
- READ: bot_config.py:L2088
- WRITE: cfg.wd.balance_error_threshold
- USED:
  - .codex_env_extraction.json:1613
  - .codex_env_extraction.json:6629
  - .env.atlas:2656
  - balance_fetcher_watchdog.py:57
- STATUS: USED

### WD_BALANCE_INTERVAL_S
- READ: bot_config.py:L2086
- WRITE: cfg.wd.balance_interval_s
- USED:
  - .codex_env_extraction.json:1599
  - .codex_env_extraction.json:6611
  - .env.atlas:2661
  - balance_fetcher_watchdog.py:55
- STATUS: USED

### WD_COOLDOWN_S
- READ: bot_config.py:L1987
- WRITE: cfg.wd.cooldown_s
- USED:
  - .codex_env_extraction.json:1235
  - .codex_env_extraction.json:6143
  - .env.atlas:2666
  - balance_fetcher_watchdog.py:63
  - central_watchdog.py:390
  - logger_watchdog.py:69
  - opportunity_watchdog.py:130
  - pairs_discovery_watchdog.py:81
  - private_ws_watchdog.py:142
  - risk_manager_watchdog.py:113
  - simulator_execution_watchdog.py:147
  - slippage_watchdog.py:84
  - volatility_watchdog.py:97
  - websocket_watchdog.py:137
- STATUS: USED

### WD_DISCOVERY_CONFIRM_TICKS
- READ: bot_config.py:L2198
- WRITE: cfg.wd.discovery_confirm_ticks
- USED:
  - .codex_env_extraction.json:1956
  - .codex_env_extraction.json:7070
  - .env.atlas:2671
  - pairs_discovery_watchdog.py:71
- STATUS: USED

### WD_DISCOVERY_DWELL_TICKS
- READ: bot_config.py:L2199
- WRITE: cfg.wd.discovery_dwell_ticks
- USED:
  - .codex_env_extraction.json:1963
  - .codex_env_extraction.json:7079
  - .env.atlas:2676
  - pairs_discovery_watchdog.py:72
- STATUS: USED

### WD_DISCOVERY_INTERVAL_S
- READ: bot_config.py:L2194
- WRITE: cfg.wd.discovery_interval_s
- USED:
  - .codex_env_extraction.json:1942
  - .codex_env_extraction.json:7052
  - .env.atlas:2681
  - pairs_discovery_watchdog.py:75
- STATUS: USED

### WD_DISCOVERY_MAX_REFRESH_GAP_S
- READ: bot_config.py:L2200
- WRITE: cfg.wd.discovery_max_refresh_gap_s
- USED:
  - .codex_env_extraction.json:1970
  - .codex_env_extraction.json:7088
  - .env.atlas:2686
  - pairs_discovery_watchdog.py:73
- STATUS: USED

### WD_DISCOVERY_MIN_CHANGE_RATIO
- READ: bot_config.py:L2195
- WRITE: cfg.wd.discovery_min_change_ratio
- USED:
  - .codex_env_extraction.json:1949
  - .codex_env_extraction.json:7061
  - .env.atlas:2691
  - pairs_discovery_watchdog.py:70
- STATUS: USED

### WD_ENGINE_ACK_FILL_CRIT_MS
- READ: bot_config.py:L2052
- WRITE: cfg.wd.engine_ack_fill_crit_ms
- USED:
  - .codex_env_extraction.json:1515
  - .codex_env_extraction.json:6503
  - .env.atlas:2696
  - simulator_execution_watchdog.py:124
- STATUS: USED

### WD_ENGINE_ACK_FILL_WARN_MS
- READ: bot_config.py:L2049
- WRITE: cfg.wd.engine_ack_fill_warn_ms
- USED:
  - .codex_env_extraction.json:1508
  - .codex_env_extraction.json:6494
  - .env.atlas:2701
  - simulator_execution_watchdog.py:123
- STATUS: USED

### WD_ENGINE_ESCALATE_AFTER_CYCLES
- READ: bot_config.py:L2079
- WRITE: cfg.wd.engine_escalate_after_cycles
- USED:
  - .codex_env_extraction.json:1578
  - .codex_env_extraction.json:6584
  - .env.atlas:2706
  - simulator_execution_watchdog.py:133
- STATUS: USED

### WD_ENGINE_INTERVAL_S
- READ: bot_config.py:L2042
- WRITE: cfg.wd.engine_interval_s
- USED:
  - .codex_env_extraction.json:1487
  - .codex_env_extraction.json:6467
  - .env.atlas:2711
  - simulator_execution_watchdog.py:136
- STATUS: USED

### WD_ENGINE_PANIC_HEDGE_CRIT_PER_MIN
- READ: bot_config.py:L2070
- WRITE: cfg.wd.engine_panic_hedge_crit_per_min
- USED:
  - .codex_env_extraction.json:1557
  - .codex_env_extraction.json:6557
  - .env.atlas:2716
  - simulator_execution_watchdog.py:130
- STATUS: USED

### WD_ENGINE_PANIC_HEDGE_WARN_PER_MIN
- READ: bot_config.py:L2067
- WRITE: cfg.wd.engine_panic_hedge_warn_per_min
- USED:
  - .codex_env_extraction.json:1550
  - .codex_env_extraction.json:6548
  - .env.atlas:2721
  - simulator_execution_watchdog.py:129
- STATUS: USED

### WD_ENGINE_QUEUEPOS_BLOCKED_CRIT_PER_MIN
- READ: bot_config.py:L2076
- WRITE: cfg.wd.engine_queuepos_blocked_crit_per_min
- USED:
  - .codex_env_extraction.json:1571
  - .codex_env_extraction.json:6575
  - .env.atlas:2726
  - simulator_execution_watchdog.py:132
- STATUS: USED

### WD_ENGINE_QUEUEPOS_BLOCKED_WARN_PER_MIN
- READ: bot_config.py:L2073
- WRITE: cfg.wd.engine_queuepos_blocked_warn_per_min
- USED:
  - .codex_env_extraction.json:1564
  - .codex_env_extraction.json:6566
  - .env.atlas:2731
  - simulator_execution_watchdog.py:131
- STATUS: USED

### WD_ENGINE_RETRIES_CRIT_PER_MIN
- READ: bot_config.py:L2064
- WRITE: cfg.wd.engine_retries_crit_per_min
- USED:
  - .codex_env_extraction.json:1543
  - .codex_env_extraction.json:6539
  - .env.atlas:2736
  - simulator_execution_watchdog.py:128
- STATUS: USED

### WD_ENGINE_RETRIES_WARN_PER_MIN
- READ: bot_config.py:L2061
- WRITE: cfg.wd.engine_retries_warn_per_min
- USED:
  - .codex_env_extraction.json:1536
  - .codex_env_extraction.json:6530
  - .env.atlas:2741
  - simulator_execution_watchdog.py:127
- STATUS: USED

### WD_ENGINE_SUBMIT_ACK_CRIT_MS
- READ: bot_config.py:L2046
- WRITE: cfg.wd.engine_submit_ack_crit_ms
- USED:
  - .codex_env_extraction.json:1501
  - .codex_env_extraction.json:6485
  - .env.atlas:2746
  - simulator_execution_watchdog.py:122
- STATUS: USED

### WD_ENGINE_SUBMIT_ACK_WARN_MS
- READ: bot_config.py:L2043
- WRITE: cfg.wd.engine_submit_ack_warn_ms
- USED:
  - .codex_env_extraction.json:1494
  - .codex_env_extraction.json:6476
  - .env.atlas:2751
  - simulator_execution_watchdog.py:121
- STATUS: USED

### WD_ENGINE_TIMEOUTS_CRIT_PER_MIN
- READ: bot_config.py:L2058
- WRITE: cfg.wd.engine_timeouts_crit_per_min
- USED:
  - .codex_env_extraction.json:1529
  - .codex_env_extraction.json:6521
  - .env.atlas:2756
  - simulator_execution_watchdog.py:126
- STATUS: USED

### WD_ENGINE_TIMEOUTS_WARN_PER_MIN
- READ: bot_config.py:L2055
- WRITE: cfg.wd.engine_timeouts_warn_per_min
- USED:
  - .codex_env_extraction.json:1522
  - .codex_env_extraction.json:6512
  - .env.atlas:2761
  - simulator_execution_watchdog.py:125
- STATUS: USED

### WD_INTERVAL_S
- READ: bot_config.py:L1986
- WRITE: cfg.wd.interval_s
- USED:
  - .codex_env_extraction.json:1228
  - .codex_env_extraction.json:6134
  - .env.atlas:2766
  - central_watchdog.py:392
- STATUS: USED

### WD_LOGGER_INTERVAL_S
- READ: bot_config.py:L2090
- WRITE: cfg.wd.logger_interval_s
- USED:
  - .codex_env_extraction.json:1620
  - .codex_env_extraction.json:6638
  - .env.atlas:2771
  - logger_watchdog.py:56
- STATUS: USED

### WD_LOGGER_MIN_QUEUE_FOR_WRITER_STALL
- READ: bot_config.py:L2097
- WRITE: cfg.wd.logger_min_queue_for_writer_stall
- USED:
  - .codex_env_extraction.json:1655
  - .codex_env_extraction.json:6683
  - .env.atlas:2776
  - logger_watchdog.py:61
- STATUS: USED

### WD_LOGGER_QUEUE_STUCK_CHECKS
- READ: bot_config.py:L2093
- WRITE: cfg.wd.logger_queue_stuck_checks
- USED:
  - .codex_env_extraction.json:1641
  - .codex_env_extraction.json:6665
  - .env.atlas:2781
  - logger_watchdog.py:59
- STATUS: USED

### WD_LOGGER_ROTATION_HARD_STALL_FACTOR
- READ: bot_config.py:L2103
- WRITE: cfg.wd.logger_rotation_hard_stall_factor
- USED:
  - .codex_env_extraction.json:1669
  - .codex_env_extraction.json:6701
  - .env.atlas:2786
  - logger_watchdog.py:63
- STATUS: USED

### WD_LOGGER_ROTATION_STALL_FACTOR
- READ: bot_config.py:L2100
- WRITE: cfg.wd.logger_rotation_stall_factor
- USED:
  - .codex_env_extraction.json:1662
  - .codex_env_extraction.json:6692
  - .env.atlas:2791
  - logger_watchdog.py:62
- STATUS: USED

### WD_LOGGER_TRADE_QUEUE_CRIT
- READ: bot_config.py:L2092
- WRITE: cfg.wd.logger_trade_queue_crit
- USED:
  - .codex_env_extraction.json:1634
  - .codex_env_extraction.json:6656
  - .env.atlas:2796
  - logger_watchdog.py:58
- STATUS: USED

### WD_LOGGER_TRADE_QUEUE_WARN
- READ: bot_config.py:L2091
- WRITE: cfg.wd.logger_trade_queue_warn
- USED:
  - .codex_env_extraction.json:1627
  - .codex_env_extraction.json:6647
  - .env.atlas:2801
  - logger_watchdog.py:57
- STATUS: USED

### WD_LOGGER_WRITER_STALL_S
- READ: bot_config.py:L2096
- WRITE: cfg.wd.logger_writer_stall_s
- USED:
  - .codex_env_extraction.json:1648
  - .codex_env_extraction.json:6674
  - .env.atlas:2806
  - logger_watchdog.py:60
- STATUS: USED

### WD_OPPORTUNITY_BACKLOG_CRIT_RATIO
- READ: bot_config.py:L2151
- WRITE: cfg.wd.opportunity_backlog_crit_ratio
- USED:
  - .codex_env_extraction.json:1844
  - .codex_env_extraction.json:6926
  - .env.atlas:2811
  - opportunity_watchdog.py:109
- STATUS: USED

### WD_OPPORTUNITY_BACKLOG_WARN_RATIO
- READ: bot_config.py:L2148
- WRITE: cfg.wd.opportunity_backlog_warn_ratio
- USED:
  - .codex_env_extraction.json:1837
  - .codex_env_extraction.json:6917
  - .env.atlas:2816
  - opportunity_watchdog.py:108
- STATUS: USED

### WD_OPPORTUNITY_DECISION_P95_CRIT_MS
- READ: bot_config.py:L2139
- WRITE: cfg.wd.opportunity_decision_p95_crit_ms
- USED:
  - .codex_env_extraction.json:1816
  - .codex_env_extraction.json:6890
  - .env.atlas:2821
  - opportunity_watchdog.py:105
- STATUS: USED

### WD_OPPORTUNITY_DECISION_P95_WARN_MS
- READ: bot_config.py:L2136
- WRITE: cfg.wd.opportunity_decision_p95_warn_ms
- USED:
  - .codex_env_extraction.json:1809
  - .codex_env_extraction.json:6881
  - .env.atlas:2826
  - opportunity_watchdog.py:104
- STATUS: USED

### WD_OPPORTUNITY_DEDUP_CRIT_PER_MIN
- READ: bot_config.py:L2157
- WRITE: cfg.wd.opportunity_dedup_crit_per_min
- USED:
  - .codex_env_extraction.json:1858
  - .codex_env_extraction.json:6944
  - .env.atlas:2831
  - opportunity_watchdog.py:111
- STATUS: USED

### WD_OPPORTUNITY_DEDUP_WARN_PER_MIN
- READ: bot_config.py:L2154
- WRITE: cfg.wd.opportunity_dedup_warn_per_min
- USED:
  - .codex_env_extraction.json:1851
  - .codex_env_extraction.json:6935
  - .env.atlas:2836
  - opportunity_watchdog.py:110
- STATUS: USED

### WD_OPPORTUNITY_EFFECTIVE_RATIO_CRIT
- READ: bot_config.py:L2133
- WRITE: cfg.wd.opportunity_effective_ratio_crit
- USED:
  - .codex_env_extraction.json:1802
  - .codex_env_extraction.json:6872
  - .env.atlas:2841
  - opportunity_watchdog.py:103
- STATUS: USED

### WD_OPPORTUNITY_EFFECTIVE_RATIO_WARN
- READ: bot_config.py:L2130
- WRITE: cfg.wd.opportunity_effective_ratio_warn
- USED:
  - .codex_env_extraction.json:1795
  - .codex_env_extraction.json:6863
  - .env.atlas:2846
  - opportunity_watchdog.py:102
- STATUS: USED

### WD_OPPORTUNITY_EMIT_P95_CRIT_MS
- READ: bot_config.py:L2145
- WRITE: cfg.wd.opportunity_emit_p95_crit_ms
- USED:
  - .codex_env_extraction.json:1830
  - .codex_env_extraction.json:6908
  - .env.atlas:2851
  - opportunity_watchdog.py:107
- STATUS: USED

### WD_OPPORTUNITY_EMIT_P95_WARN_MS
- READ: bot_config.py:L2142
- WRITE: cfg.wd.opportunity_emit_p95_warn_ms
- USED:
  - .codex_env_extraction.json:1823
  - .codex_env_extraction.json:6899
  - .env.atlas:2856
  - opportunity_watchdog.py:106
- STATUS: USED

### WD_OPPORTUNITY_ESCALATE_AFTER_CYCLES
- READ: bot_config.py:L2190
- WRITE: cfg.wd.opportunity_escalate_after_cycles
- USED:
  - .codex_env_extraction.json:1935
  - .codex_env_extraction.json:7043
  - .env.atlas:2861
  - opportunity_watchdog.py:122
- STATUS: USED

### WD_OPPORTUNITY_FEES_AGE_CRIT_S
- READ: bot_config.py:L2181
- WRITE: cfg.wd.opportunity_fees_age_crit_s
- USED:
  - .codex_env_extraction.json:1914
  - .codex_env_extraction.json:7016
  - .env.atlas:2866
  - opportunity_watchdog.py:119
- STATUS: USED

### WD_OPPORTUNITY_FEES_AGE_WARN_S
- READ: bot_config.py:L2178
- WRITE: cfg.wd.opportunity_fees_age_warn_s
- USED:
  - .codex_env_extraction.json:1907
  - .codex_env_extraction.json:7007
  - .env.atlas:2871
  - opportunity_watchdog.py:118
- STATUS: USED

### WD_OPPORTUNITY_INTERVAL_S
- READ: bot_config.py:L2129
- WRITE: cfg.wd.opportunity_interval_s
- USED:
  - .codex_env_extraction.json:1788
  - .codex_env_extraction.json:6854
  - .env.atlas:2876
  - opportunity_watchdog.py:124
- STATUS: USED

### WD_OPPORTUNITY_REJECTION_RATIO_CRIT
- READ: bot_config.py:L2163
- WRITE: cfg.wd.opportunity_rejection_ratio_crit
- USED:
  - .codex_env_extraction.json:1872
  - .codex_env_extraction.json:6962
  - .env.atlas:2881
  - opportunity_watchdog.py:113
- STATUS: USED

### WD_OPPORTUNITY_REJECTION_RATIO_WARN
- READ: bot_config.py:L2160
- WRITE: cfg.wd.opportunity_rejection_ratio_warn
- USED:
  - .codex_env_extraction.json:1865
  - .codex_env_extraction.json:6953
  - .env.atlas:2886
  - opportunity_watchdog.py:112
- STATUS: USED

### WD_OPPORTUNITY_SCANNER_ERR_CRIT_PER_MIN
- READ: bot_config.py:L2187
- WRITE: cfg.wd.opportunity_scanner_err_crit_per_min
- USED:
  - .codex_env_extraction.json:1928
  - .codex_env_extraction.json:7034
  - .env.atlas:2891
  - opportunity_watchdog.py:121
- STATUS: USED

### WD_OPPORTUNITY_SCANNER_ERR_WARN_PER_MIN
- READ: bot_config.py:L2184
- WRITE: cfg.wd.opportunity_scanner_err_warn_per_min
- USED:
  - .codex_env_extraction.json:1921
  - .codex_env_extraction.json:7025
  - .env.atlas:2896
  - opportunity_watchdog.py:120
- STATUS: USED

### WD_OPPORTUNITY_SLIP_AGE_CRIT_S
- READ: bot_config.py:L2169
- WRITE: cfg.wd.opportunity_slip_age_crit_s
- USED:
  - .codex_env_extraction.json:1886
  - .codex_env_extraction.json:6980
  - .env.atlas:2901
  - opportunity_watchdog.py:115
- STATUS: USED

### WD_OPPORTUNITY_SLIP_AGE_WARN_S
- READ: bot_config.py:L2166
- WRITE: cfg.wd.opportunity_slip_age_warn_s
- USED:
  - .codex_env_extraction.json:1879
  - .codex_env_extraction.json:6971
  - .env.atlas:2906
  - opportunity_watchdog.py:114
- STATUS: USED

### WD_OPPORTUNITY_VOL_AGE_CRIT_S
- READ: bot_config.py:L2175
- WRITE: cfg.wd.opportunity_vol_age_crit_s
- USED:
  - .codex_env_extraction.json:1900
  - .codex_env_extraction.json:6998
  - .env.atlas:2911
  - opportunity_watchdog.py:117
- STATUS: USED

### WD_OPPORTUNITY_VOL_AGE_WARN_S
- READ: bot_config.py:L2172
- WRITE: cfg.wd.opportunity_vol_age_warn_s
- USED:
  - .codex_env_extraction.json:1893
  - .codex_env_extraction.json:6989
  - .env.atlas:2916
  - opportunity_watchdog.py:116
- STATUS: USED

### WD_PERSISTENCE_CYCLES
- READ: bot_config.py:L1989
- WRITE: cfg.wd.persistence_cycles
- USED:
  - .codex_env_extraction.json:1249
  - .codex_env_extraction.json:6161
  - .env.atlas:2921
- STATUS: USED

### WD_PERSISTENCE_S
- READ: bot_config.py:L1988
- WRITE: cfg.wd.persistence_s
- USED:
  - .codex_env_extraction.json:1242
  - .codex_env_extraction.json:6152
  - .env.atlas:2926
- STATUS: USED

### WD_PRIVATE_WS_ACK_CRIT_MS
- READ: bot_config.py:L2024
- WRITE: cfg.wd.private_ws_ack_crit_ms
- USED:
  - .codex_env_extraction.json:1410
  - .codex_env_extraction.json:6368
  - .env.atlas:2931
  - private_ws_watchdog.py:122
- STATUS: USED

### WD_PRIVATE_WS_ACK_WARN_MS
- READ: bot_config.py:L2023
- WRITE: cfg.wd.private_ws_ack_warn_ms
- USED:
  - .codex_env_extraction.json:1403
  - .codex_env_extraction.json:6359
  - .env.atlas:2936
  - private_ws_watchdog.py:121
- STATUS: USED

### WD_PRIVATE_WS_DEDUP_CRIT
- READ: bot_config.py:L2036
- WRITE: cfg.wd.private_ws_dedup_crit
- USED:
  - .codex_env_extraction.json:1466
  - .codex_env_extraction.json:6440
  - .env.atlas:2941
  - private_ws_watchdog.py:130
- STATUS: USED

### WD_PRIVATE_WS_DEDUP_WARN
- READ: bot_config.py:L2035
- WRITE: cfg.wd.private_ws_dedup_warn
- USED:
  - .codex_env_extraction.json:1459
  - .codex_env_extraction.json:6431
  - .env.atlas:2946
  - private_ws_watchdog.py:129
- STATUS: USED

### WD_PRIVATE_WS_ESCALATE_AFTER_CYCLES
- READ: bot_config.py:L2037
- WRITE: cfg.wd.private_ws_escalate_after_cycles
- USED:
  - .codex_env_extraction.json:1473
  - .codex_env_extraction.json:6449
  - .env.atlas:2951
  - private_ws_watchdog.py:131
- STATUS: USED

### WD_PRIVATE_WS_FILL_CRIT_MS
- READ: bot_config.py:L2026
- WRITE: cfg.wd.private_ws_fill_crit_ms
- USED:
  - .codex_env_extraction.json:1424
  - .codex_env_extraction.json:6386
  - .env.atlas:2956
  - private_ws_watchdog.py:124
- STATUS: USED

### WD_PRIVATE_WS_FILL_WARN_MS
- READ: bot_config.py:L2025
- WRITE: cfg.wd.private_ws_fill_warn_ms
- USED:
  - .codex_env_extraction.json:1417
  - .codex_env_extraction.json:6377
  - .env.atlas:2961
  - private_ws_watchdog.py:123
- STATUS: USED

### WD_PRIVATE_WS_HB_CRIT_S
- READ: bot_config.py:L2022
- WRITE: cfg.wd.private_ws_hb_crit_s
- USED:
  - .codex_env_extraction.json:1396
  - .codex_env_extraction.json:6350
  - .env.atlas:2966
  - private_ws_watchdog.py:115
- STATUS: USED

### WD_PRIVATE_WS_HB_WARN_S
- READ: bot_config.py:L2021
- WRITE: cfg.wd.private_ws_hb_warn_s
- USED:
  - .codex_env_extraction.json:1389
  - .codex_env_extraction.json:6341
  - .env.atlas:2971
  - private_ws_watchdog.py:119
- STATUS: USED

### WD_PRIVATE_WS_INTERVAL_S
- READ: bot_config.py:L2020
- WRITE: cfg.wd.private_ws_interval_s
- USED:
  - .codex_env_extraction.json:1382
  - .codex_env_extraction.json:6332
  - .env.atlas:2976
  - private_ws_watchdog.py:133
- STATUS: USED

### WD_PRIVATE_WS_QUEUE_CRIT_RATIO
- READ: bot_config.py:L2030
- WRITE: cfg.wd.private_ws_queue_crit_ratio
- USED:
  - .codex_env_extraction.json:1438
  - .codex_env_extraction.json:6404
  - .env.atlas:2981
  - private_ws_watchdog.py:126
- STATUS: USED

### WD_PRIVATE_WS_QUEUE_WARN_RATIO
- READ: bot_config.py:L2027
- WRITE: cfg.wd.private_ws_queue_warn_ratio
- USED:
  - .codex_env_extraction.json:1431
  - .codex_env_extraction.json:6395
  - .env.atlas:2986
  - private_ws_watchdog.py:125
- STATUS: USED

### WD_PRIVATE_WS_RATE429_CRIT
- READ: bot_config.py:L2034
- WRITE: cfg.wd.private_ws_rate429_crit
- USED:
  - .codex_env_extraction.json:1452
  - .codex_env_extraction.json:6422
  - .env.atlas:2991
  - private_ws_watchdog.py:128
- STATUS: USED

### WD_PRIVATE_WS_RATE429_WARN
- READ: bot_config.py:L2033
- WRITE: cfg.wd.private_ws_rate429_warn
- USED:
  - .codex_env_extraction.json:1445
  - .codex_env_extraction.json:6413
  - .env.atlas:2996
  - private_ws_watchdog.py:127
- STATUS: USED

### WD_RM_ESCALATE_AFTER_CYCLES
- READ: bot_config.py:L2227
- WRITE: cfg.wd.rm_escalate_after_cycles
- USED:
  - .codex_env_extraction.json:2082
  - .codex_env_extraction.json:7232
  - .env.atlas:3001
  - risk_manager_watchdog.py:105
- STATUS: USED

### WD_RM_HB_CRIT_S
- READ: bot_config.py:L2208
- WRITE: cfg.wd.rm_hb_crit_s
- USED:
  - .codex_env_extraction.json:2005
  - .codex_env_extraction.json:7133
  - .env.atlas:3006
  - risk_manager_watchdog.py:94
- STATUS: USED

### WD_RM_HB_WARN_S
- READ: bot_config.py:L2207
- WRITE: cfg.wd.rm_hb_warn_s
- USED:
  - .codex_env_extraction.json:1998
  - .codex_env_extraction.json:7124
  - .env.atlas:3011
  - risk_manager_watchdog.py:93
- STATUS: USED

### WD_RM_INTERVAL_S
- READ: bot_config.py:L2204
- WRITE: cfg.wd.rm_interval_s
- USED:
  - .codex_env_extraction.json:1977
  - .codex_env_extraction.json:7097
  - .env.atlas:3016
  - risk_manager_watchdog.py:107
- STATUS: USED

### WD_RM_QUEUEPOS_CRIT_PER_MIN
- READ: bot_config.py:L2222
- WRITE: cfg.wd.rm_queuepos_crit_per_min
- USED:
  - .codex_env_extraction.json:2061
  - .codex_env_extraction.json:7205
  - .env.atlas:3021
  - risk_manager_watchdog.py:102
- STATUS: USED

### WD_RM_QUEUEPOS_WARN_PER_MIN
- READ: bot_config.py:L2219
- WRITE: cfg.wd.rm_queuepos_warn_per_min
- USED:
  - .codex_env_extraction.json:2054
  - .codex_env_extraction.json:7196
  - .env.atlas:3026
  - risk_manager_watchdog.py:101
- STATUS: USED

### WD_RM_SEVERE_CRIT_S
- READ: bot_config.py:L2226
- WRITE: cfg.wd.rm_severe_crit_s
- USED:
  - .codex_env_extraction.json:2075
  - .codex_env_extraction.json:7223
  - .env.atlas:3031
  - risk_manager_watchdog.py:104
- STATUS: USED

### WD_RM_SEVERE_WARN_S
- READ: bot_config.py:L2225
- WRITE: cfg.wd.rm_severe_warn_s
- USED:
  - .codex_env_extraction.json:2068
  - .codex_env_extraction.json:7214
  - .env.atlas:3036
  - risk_manager_watchdog.py:103
- STATUS: USED

### WD_RM_SHADOW_BIAS_CRIT_BPS
- READ: bot_config.py:L2216
- WRITE: cfg.wd.rm_shadow_bias_crit_bps
- USED:
  - .codex_env_extraction.json:2047
  - .codex_env_extraction.json:7187
  - .env.atlas:3041
  - risk_manager_watchdog.py:100
- STATUS: USED

### WD_RM_SHADOW_BIAS_WARN_BPS
- READ: bot_config.py:L2213
- WRITE: cfg.wd.rm_shadow_bias_warn_bps
- USED:
  - .codex_env_extraction.json:2040
  - .codex_env_extraction.json:7178
  - .env.atlas:3046
  - risk_manager_watchdog.py:99
- STATUS: USED

### WD_RM_SLIP_AGE_CRIT_S
- READ: bot_config.py:L2210
- WRITE: cfg.wd.rm_slip_age_crit_s
- USED:
  - .codex_env_extraction.json:2019
  - .codex_env_extraction.json:7151
  - .env.atlas:3051
  - risk_manager_watchdog.py:96
- STATUS: USED

### WD_RM_SLIP_AGE_WARN_S
- READ: bot_config.py:L2209
- WRITE: cfg.wd.rm_slip_age_warn_s
- USED:
  - .codex_env_extraction.json:2012
  - .codex_env_extraction.json:7142
  - .env.atlas:3056
  - risk_manager_watchdog.py:95
- STATUS: USED

### WD_RM_TICK_CRIT_MS
- READ: bot_config.py:L2206
- WRITE: cfg.wd.rm_tick_crit_ms
- USED:
  - .codex_env_extraction.json:1991
  - .codex_env_extraction.json:7115
  - .env.atlas:3061
  - risk_manager_watchdog.py:92
- STATUS: USED

### WD_RM_TICK_WARN_MS
- READ: bot_config.py:L2205
- WRITE: cfg.wd.rm_tick_warn_ms
- USED:
  - .codex_env_extraction.json:1984
  - .codex_env_extraction.json:7106
  - .env.atlas:3066
  - risk_manager_watchdog.py:91
- STATUS: USED

### WD_RM_VOL_AGE_CRIT_S
- READ: bot_config.py:L2212
- WRITE: cfg.wd.rm_vol_age_crit_s
- USED:
  - .codex_env_extraction.json:2033
  - .codex_env_extraction.json:7169
  - .env.atlas:3071
  - risk_manager_watchdog.py:98
- STATUS: USED

### WD_RM_VOL_AGE_WARN_S
- READ: bot_config.py:L2211
- WRITE: cfg.wd.rm_vol_age_warn_s
- USED:
  - .codex_env_extraction.json:2026
  - .codex_env_extraction.json:7160
  - .env.atlas:3076
  - risk_manager_watchdog.py:97
- STATUS: USED

### WD_ROUTER_INTERVAL_S
- READ: bot_config.py:L1992
- WRITE: cfg.wd.router_interval_s
- USED:
  - .codex_env_extraction.json:1263
  - .codex_env_extraction.json:6179
  - .env.atlas:3081
- STATUS: USED

### WD_SLIPPAGE_AGE_CRIT_S
- READ: bot_config.py:L2109
- WRITE: cfg.wd.slippage_age_crit_s
- USED:
  - .codex_env_extraction.json:1690
  - .codex_env_extraction.json:6728
  - .env.atlas:3086
  - slippage_watchdog.py:73
- STATUS: USED

### WD_SLIPPAGE_AGE_WARN_S
- READ: bot_config.py:L2108
- WRITE: cfg.wd.slippage_age_warn_s
- USED:
  - .codex_env_extraction.json:1683
  - .codex_env_extraction.json:6719
  - .env.atlas:3091
  - slippage_watchdog.py:72
- STATUS: USED

### WD_SLIPPAGE_ESCALATE_AFTER_CYCLES
- READ: bot_config.py:L2112
- WRITE: cfg.wd.slippage_escalate_after_cycles
- USED:
  - .codex_env_extraction.json:1711
  - .codex_env_extraction.json:6755
  - .env.atlas:3096
  - slippage_watchdog.py:76
- STATUS: USED

### WD_SLIPPAGE_INTERVAL_S
- READ: bot_config.py:L2107
- WRITE: cfg.wd.slippage_interval_s
- USED:
  - .codex_env_extraction.json:1676
  - .codex_env_extraction.json:6710
  - .env.atlas:3101
  - slippage_watchdog.py:78
- STATUS: USED

### WD_SLIPPAGE_P95_WARN_BPS
- READ: bot_config.py:L2110
- WRITE: cfg.wd.slippage_p95_warn_bps
- USED:
  - .codex_env_extraction.json:1697
  - .codex_env_extraction.json:6737
  - .env.atlas:3106
  - slippage_watchdog.py:74
- STATUS: USED

### WD_SLIPPAGE_P99_CRIT_BPS
- READ: bot_config.py:L2111
- WRITE: cfg.wd.slippage_p99_crit_bps
- USED:
  - .codex_env_extraction.json:1704
  - .codex_env_extraction.json:6746
  - .env.atlas:3111
  - slippage_watchdog.py:75
- STATUS: USED

### WD_VOLATILITY_AGE_CRIT_S
- READ: bot_config.py:L2118
- WRITE: cfg.wd.volatility_age_crit_s
- USED:
  - .codex_env_extraction.json:1732
  - .codex_env_extraction.json:6782
  - .env.atlas:3116
  - volatility_watchdog.py:82
- STATUS: USED

### WD_VOLATILITY_AGE_WARN_S
- READ: bot_config.py:L2117
- WRITE: cfg.wd.volatility_age_warn_s
- USED:
  - .codex_env_extraction.json:1725
  - .codex_env_extraction.json:6773
  - .env.atlas:3121
  - volatility_watchdog.py:81
- STATUS: USED

### WD_VOLATILITY_ESCALATE_AFTER_CYCLES
- READ: bot_config.py:L2125
- WRITE: cfg.wd.volatility_escalate_after_cycles
- USED:
  - .codex_env_extraction.json:1781
  - .codex_env_extraction.json:6845
  - .env.atlas:3126
  - volatility_watchdog.py:89
- STATUS: USED

### WD_VOLATILITY_HARD_CAP_BPS
- READ: bot_config.py:L2120
- WRITE: cfg.wd.volatility_hard_cap_bps
- USED:
  - .codex_env_extraction.json:1746
  - .codex_env_extraction.json:6800
  - .env.atlas:3131
  - volatility_watchdog.py:84
- STATUS: USED

### WD_VOLATILITY_INTERVAL_S
- READ: bot_config.py:L2116
- WRITE: cfg.wd.volatility_interval_s
- USED:
  - .codex_env_extraction.json:1718
  - .codex_env_extraction.json:6764
  - .env.atlas:3136
  - volatility_watchdog.py:91
- STATUS: USED

### WD_VOLATILITY_P95_WARN_BPS
- READ: bot_config.py:L2121
- WRITE: cfg.wd.volatility_p95_warn_bps
- USED:
  - .codex_env_extraction.json:1753
  - .codex_env_extraction.json:6809
  - .env.atlas:3141
  - volatility_watchdog.py:85
- STATUS: USED

### WD_VOLATILITY_P99_CRIT_BPS
- READ: bot_config.py:L2122
- WRITE: cfg.wd.volatility_p99_crit_bps
- USED:
  - .codex_env_extraction.json:1760
  - .codex_env_extraction.json:6818
  - .env.atlas:3146
  - volatility_watchdog.py:86
- STATUS: USED

### WD_VOLATILITY_SOFT_CAP_BPS
- READ: bot_config.py:L2119
- WRITE: cfg.wd.volatility_soft_cap_bps
- USED:
  - .codex_env_extraction.json:1739
  - .codex_env_extraction.json:6791
  - .env.atlas:3151
  - volatility_watchdog.py:83
- STATUS: USED

### WD_VOLATILITY_Z_CRIT
- READ: bot_config.py:L2124
- WRITE: cfg.wd.volatility_z_crit
- USED:
  - .codex_env_extraction.json:1774
  - .codex_env_extraction.json:6836
  - .env.atlas:3156
  - volatility_watchdog.py:88
- STATUS: USED

### WD_VOLATILITY_Z_WARN
- READ: bot_config.py:L2123
- WRITE: cfg.wd.volatility_z_warn
- USED:
  - .codex_env_extraction.json:1767
  - .codex_env_extraction.json:6827
  - .env.atlas:3161
  - volatility_watchdog.py:87
- STATUS: USED

### WD_WS_PUBLIC_ESCALATE_AFTER_CYCLES
- READ: bot_config.py:L2016
- WRITE: cfg.wd.ws_public_escalate_after_cycles
- USED:
  - .codex_env_extraction.json:1375
  - .codex_env_extraction.json:6323
  - .env.atlas:3166
  - websocket_watchdog.py:126
- STATUS: USED

### WD_WS_PUBLIC_HB_GAP_CRIT_S
- READ: bot_config.py:L2009
- WRITE: cfg.wd.ws_public_hb_gap_crit_s
- USED:
  - .codex_env_extraction.json:1354
  - .codex_env_extraction.json:6296
  - .env.atlas:3171
  - websocket_watchdog.py:123
- STATUS: USED

### WD_WS_PUBLIC_HB_GAP_WARN_S
- READ: bot_config.py:L2008
- WRITE: cfg.wd.ws_public_hb_gap_warn_s
- USED:
  - .codex_env_extraction.json:1347
  - .codex_env_extraction.json:6287
  - .env.atlas:3176
  - websocket_watchdog.py:122
- STATUS: USED

### WD_WS_PUBLIC_INTERVAL_S
- READ: bot_config.py:L2005
- WRITE: cfg.wd.ws_public_interval_s
- USED:
  - .codex_env_extraction.json:1326
  - .codex_env_extraction.json:6260
  - .env.atlas:3181
  - websocket_watchdog.py:128
- STATUS: USED

### WD_WS_PUBLIC_RESUB_CRIT_PER_MIN
- READ: bot_config.py:L2013
- WRITE: cfg.wd.ws_public_resub_crit_per_min
- USED:
  - .codex_env_extraction.json:1368
  - .codex_env_extraction.json:6314
  - .env.atlas:3186
  - websocket_watchdog.py:125
- STATUS: USED

### WD_WS_PUBLIC_RESUB_WARN_PER_MIN
- READ: bot_config.py:L2010
- WRITE: cfg.wd.ws_public_resub_warn_per_min
- USED:
  - .codex_env_extraction.json:1361
  - .codex_env_extraction.json:6305
  - .env.atlas:3191
  - websocket_watchdog.py:124
- STATUS: USED

### WD_WS_PUBLIC_STALE_CRIT_MS
- READ: bot_config.py:L2007
- WRITE: cfg.wd.ws_public_stale_crit_ms
- USED:
  - .codex_env_extraction.json:1340
  - .codex_env_extraction.json:6278
  - .env.atlas:3196
  - websocket_watchdog.py:121
- STATUS: USED

### WD_WS_PUBLIC_STALE_WARN_MS
- READ: bot_config.py:L2006
- WRITE: cfg.wd.ws_public_stale_warn_ms
- USED:
  - .codex_env_extraction.json:1333
  - .codex_env_extraction.json:6269
  - .env.atlas:3201
  - websocket_watchdog.py:120
- STATUS: USED

### WS_BACKOFF
- READ: bot_config.py:L1957
- WRITE: cfg.ws_public.ws_backoff
- USED:
  - .codex_env_extraction.json:11033
  - .codex_env_extraction.json:1165
  - .codex_env_extraction.json:5064
  - .codex_env_extraction.json:6053
  - .env.atlas:3208
  - .env.atlas:3213
- STATUS: USED

### WS_BACKOFF_SEED
- READ: bot_config.py:L1958
- WRITE: cfg.ws_public.ws_backoff_seed
- USED:
  - .codex_env_extraction.json:11033
  - .codex_env_extraction.json:5064
  - .env.atlas:3213
- STATUS: USED

### WS_CONNECT_TIMEOUT_S
- READ: bot_config.py:L1962
- WRITE: cfg.ws_public.connect_timeout_s
- USED:
  - .codex_env_extraction.json:1179
  - .codex_env_extraction.json:6071
  - .env.atlas:3218
- STATUS: USED

### WS_DISABLED_EXCHANGES
- READ: bot_config.py:L1975
- WRITE: cfg.ws_public.disabled_exchanges
- USED:
  - .codex_env_extraction.json:1207
  - .codex_env_extraction.json:6107
  - .env.atlas:3223
- STATUS: USED

### WS_LIVE_CLOSE_TIMEOUT_S
- READ: bot_config.py:L3519
- WRITE: cfg.tests.WS_LIVE_CLOSE_TIMEOUT_S
- USED:
  - .codex_env_extraction.json:11015
  - .codex_env_extraction.json:5029
  - .env.atlas:2473
- STATUS: USED

### WS_LIVE_MIN_MSGS_PER_S
- READ: bot_config.py:L3518
- WRITE: cfg.tests.WS_LIVE_MIN_MSGS_PER_S
- USED:
  - .codex_env_extraction.json:11006
  - .codex_env_extraction.json:5022
  - .env.atlas:2478
- STATUS: USED

### WS_LIVE_WINDOW_S
- READ: bot_config.py:L3517
- WRITE: cfg.tests.WS_LIVE_WINDOW_S
- USED:
  - .codex_env_extraction.json:10997
  - .codex_env_extraction.json:5015
  - .env.atlas:2483
- STATUS: USED

### WS_OUT_QUEUE_PUT_TIMEOUT_S
- READ: bot_config.py:L1963
- WRITE: cfg.ws_public.out_queue_put_timeout_s
- USED:
  - .codex_env_extraction.json:1186
  - .codex_env_extraction.json:6080
  - .env.atlas:3228
- STATUS: USED

### WS_READ_TIMEOUT_S
- READ: bot_config.py:L1978
- WRITE: cfg.ws_public.read_timeout_s
- USED:
  - .codex_env_extraction.json:1214
  - .codex_env_extraction.json:6116
  - .env.atlas:3233
- STATUS: USED

### WS_RELOAD_CAP_PER_HOUR
- READ: bot_config.py:L1799
- WRITE: cfg.g.ws_reload_cap_per_hour
- USED: none
- STATUS: NO_USAGE_FOUND

### WS_RELOAD_COOLDOWN_S
- READ: bot_config.py:L1800
- WRITE: cfg.g.ws_reload_cooldown_s
- USED: none
- STATUS: NO_USAGE_FOUND

### WS_RELOAD_MUTE_S
- READ: bot_config.py:L1801
- WRITE: cfg.g.ws_reload_mute_s
- USED: none
- STATUS: NO_USAGE_FOUND

### WS_STALENESS_INTERVAL_S
- READ: bot_config.py:L1966
- WRITE: cfg.ws_public.staleness_interval_s
- USED:
  - .codex_env_extraction.json:1193
  - .codex_env_extraction.json:6089
  - .env.atlas:3238
- STATUS: USED

### WS_STALENESS_SLO_S
- READ: bot_config.py:L1969
- WRITE: cfg.ws_public.staleness_slo_s
- USED: none
- STATUS: NO_USAGE_FOUND

### WS_UPDATE_PAIRS_JITTER_MS
- READ: bot_config.py:L1959
- WRITE: cfg.ws_public.update_pairs_jitter_ms
- USED:
  - .codex_env_extraction.json:1172
  - .codex_env_extraction.json:6062
  - .env.atlas:3243
- STATUS: USED

