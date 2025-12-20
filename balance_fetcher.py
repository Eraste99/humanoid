# modules/balance_fetcher.py
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
MultiBalanceFetcher — tri-CEX, alias-aware (TT/TM/MM) + Overlay virtuel

• Exchanges: BINANCE, COINBASE (Advanced Trade), BYBIT (v5 Unified)
• Vues:
    - get_balances_snapshot(mode="real"|"virtual"|"merged")  # sans I/O
    - await get_balances(mode=..., force_refresh=...)        # avec I/O optionnel
    - await get_all_balances(force_refresh=False)            # réel pour tous
    - as_rm_snapshot() / as_buffers_snapshot()               # vues RM/Watchdog
• Overlay virtuel:
    - adjust_virtual(ex, alias, ccy, delta)
    - set_virtual(ex, alias, ccy, amount)  # absolu
    - reset_virtual(ex?, alias?, ccy?)

Alignement P0:
- Nommage exchange/alias en UPPER.
- self.cfg optionnelle (alias config) pour overrides (API bases, TTLs…).
- Ratelimit par (exchange, alias) + retry/backoff + jitter.
- Binance: sync server time pour éviter -1021 (si use_server_time=True).
- Coinbase AT: secret base64-décodé pour HMAC.
- Bybit v5 Unified: signature X-BAPI-*.
- Snapshots enrichis (age, fee-tokens, VIP hook facultatif).
- Métriques tolérantes (no-op fallback, try/except sur .labels()).
- Intégration Ticket 8 (transferts internes & capital en mouvement) :
    • ne consomme pas directement les events de transfert, mais fournit :
      - resync_balances_for_alias(ex, alias) pour un refresh agressif post-transfert,
      - as_rm_snapshot(...).meta["age_s"] comme base unique de TTL balances,
      - set_event_sink(...) pour publier des events "capital_refresh" facultatifs.
"""

import asyncio
import aiohttp
import time
import hmac
import hashlib
import base64
import logging
import json
import random
from typing import Dict, Any, Optional, Tuple, List,Set, Iterable

from contracts.payloads import (
    validate_balance_snapshot_rm_lite,
    validate_reco_alias_status_snapshot_lite,
    validate_ws_alias_status_snapshot_lite,
    normalize_reason_code,
)



logger = logging.getLogger("MultiBalanceFetcher")
logger.setLevel(logging.INFO)

# === Logger (module-level) ====================================================
import logging
from modules.rm_compat import getattr_int, getattr_float, getattr_str, getattr_bool, getattr_dict, getattr_list

log = logging.getLogger("BalanceFetcher")
# Optionnel: si ton app ne configure pas déjà le logging global
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO)


# ===== Metrics (fallback no-op si absents) ==================================
try:
    from modules.obs_metrics import (
        BF_API_LATENCY_MS,
        BF_API_ERRORS_TOTAL,
        BF_CACHE_AGE_SECONDS,
        BF_LAST_SUCCESS_TS,
        FEE_TOKEN_BALANCE,
        BF_HTTP_LATENCY_SECONDS,
        BF_HTTP_ERRORS_TOTAL,
        BF_FEE_TOKEN_LEVEL,
        BF_FEE_TOKEN_LOW_TOTAL,
        BF_BALANCES_TTL_NORMAL_SECONDS,
        BF_BALANCES_TTL_DEGRADED_SECONDS,
        BF_BALANCES_TTL_BLOCK_SECONDS,
        BF_BALANCES_HEALTH_STATE,
    )
except Exception:  # pragma: no cover
    class _NoopMetric:
        def labels(self, *_, **__): return self
        def inc(self, *_, **__): pass
        def observe(self, *_, **__): pass
        def set(self, *_, **__): pass

        def time(self, *_, **__):
            import contextlib
            return contextlib.nullcontext()

    BF_API_LATENCY_MS = BF_API_ERRORS_TOTAL = BF_CACHE_AGE_SECONDS = BF_LAST_SUCCESS_TS = FEE_TOKEN_BALANCE = _NoopMetric()
    BF_HTTP_LATENCY_SECONDS = BF_HTTP_ERRORS_TOTAL = BF_FEE_TOKEN_LEVEL = BF_FEE_TOKEN_LOW_TOTAL = _NoopMetric()
    BF_BALANCES_TTL_NORMAL_SECONDS = BF_BALANCES_TTL_DEGRADED_SECONDS = BF_BALANCES_TTL_BLOCK_SECONDS = BF_BALANCES_HEALTH_STATE = _NoopMetric()
else:
    class _NoopMetric:
        def labels(self, *_, **__): return self
        def inc(self, *_, **__): pass
        def observe(self, *_, **__): pass
        def set(self, *_, **__): pass
        def time(self, *_, **__):
            import contextlib
            return contextlib.nullcontext()
try:
    from modules.observability_pacer import PACER
except Exception:
    class _P0Pacer:
        def clamp(self, *_a, **_k): return 0.0
        def state(self): return "NORMAL"
    PACER = _P0Pacer()

# ===== Rate limiter (token bucket/min) ======================================
class _RateLimiter:
    def __init__(self, per_min: int):
        self.capacity = max(1, int(per_min))
        self.tokens = float(self.capacity)
        self.last = time.time()
        self._lock = asyncio.Lock()

    async def take(self):
        extra_sleep = 0.0
        while True:
            sleep_needed = 0.0
            async with self._lock:
                now = time.time()
                refill = (now - self.last) * (self.capacity / 60.0)
                self.tokens = min(self.capacity, self.tokens + refill)
                self.last = now
                if self.tokens < 1.0:
                    sleep_needed = (1.0 - self.tokens) * (60.0 / self.capacity)
                else:
                    self.tokens -= 1.0
                    try:
                        extra_sleep = float(PACER.clamp("bf_rate"))
                    except Exception:
                        extra_sleep = 0.0
            if sleep_needed > 0.0:
                await asyncio.sleep(sleep_needed)
                continue
            break
        if extra_sleep > 0.0:
            await asyncio.sleep(extra_sleep)


def _upper(x: Optional[str]) -> str:
    return (x or "").upper()


class MultiBalanceFetcher:
    """
        Source de vérité « data-plane » pour les soldes par exchange / alias / asset.

        - Maintient les vues `real`, `virtual` et `merged` via `get_balances_snapshot(mode=...)`.
        - Expose une vue normalisée pour le desk de risque via `as_rm_snapshot(...)`
          (RM + Rebalancing = trading-plane).
        - Toutes les décisions de caps / réservations / REB doivent partir d’un snapshot MBF
          (directement ou via `_RM_MBFGlue`), pas de structures locales parallèles.

        Cette classe ne porte aucune logique de caps métier : elle expose seulement l’état
        des soldes réels + overlay virtuel (dry-run, simulations), avec métadonnées d’âge
        et d’état des tokens de fees.
        """
    # Bases par défaut (overridables via self.cfg.*)
    BINANCE_API_DEFAULT = "https://api.binance.com"
    COINBASE_API_DEFAULT = "https://api.coinbase.com"
    BYBIT_API_DEFAULT   = "https://api.bybit.com"

    def __init__(
        self,
        *,
        binance_accounts: Dict[str, Dict[str, str]] | None,
        coinbase_at_accounts: Dict[str, Dict[str, str]] | None,
        bybit_accounts: Dict[str, Dict[str, str]] | None,
        # réseau/retry
        max_retries: int = 4,
        retry_base_delay: float = 0.35,
        retry_max_delay: float = 3.0,
        use_server_time: bool = True,
        # cache & vue
        cache_ttl: float = 5.0,
        ccy_filter: Optional[List[str]] = None,
        # logs & test
        verbose: bool = True,
        dry_run: bool = False,
        dry_defaults: Optional[Dict[str, Dict[str, Dict[str, float]]]] = None,
        # cadence BF (alignement P0)
        bf_poll_interval_s: float = 4.0,
        bf_rate_limit_per_min: int = 30,
        # cfg externe (alignement Hub/Reconciler)
        config: Any | None = None,
    ):
        self.verbose = bool(verbose)
        self.dry_run = bool(dry_run)
        self.max_retries = int(max_retries)
        self.retry_base_delay = float(retry_base_delay)
        self.retry_max_delay = float(retry_max_delay)
        self.cache_ttl = float(cache_ttl)
        self.use_server_time = bool(use_server_time)
        self.ccy_filter = [c.upper() for c in (ccy_filter or [])]
        self.bf_poll_interval_s = float(bf_poll_interval_s)
        self.bf_rate_limit_per_min = int(bf_rate_limit_per_min)

        # cfg: expose self.cfg (alias) + bases API
        self.config = config
        self.cfg = config
        self.BINANCE_API = getattr(self.cfg, "binance_rest_base", self.BINANCE_API_DEFAULT)
        self.COINBASE_API = getattr(self.cfg, "coinbase_api_base", self.COINBASE_API_DEFAULT)
        self.BYBIT_API    = getattr(self.cfg, "bybit_api_base", self.BYBIT_API_DEFAULT)

        # TTL balances par défaut (fallback BF) + cache SLO par exchange
        bf_cfg = getattr(self, "_bf_cfg", self.cfg)
        self._bal_ttl_default_normal = float(getattr(bf_cfg, "BALANCES_TTL_S_NORMAL", 10.0))
        self._bal_ttl_default_degraded = float(getattr(bf_cfg, "BALANCES_TTL_S_DEGRADED", 30.0))
        self._bal_ttl_default_block = float(getattr(bf_cfg, "BALANCES_TTL_S_BLOCK", 120.0))
        self._bal_ttl_by_ex: Dict[str, Dict[str, float]] = {}
        self._balances_health: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._balances_freshness_status: Dict[str, Any] = {"status": "UNKNOWN", "ready": False, "reason_code": None}
        # Seuils observabilité WS/Reconciler pour dériver capital_at_risk
        self._ws_miss_rate_thr = float(getattr(bf_cfg, "BF_WS_RECO_MISS_RATE_THR_PER_MIN", 30.0))
        self._ws_resync_age_thr_s = float(getattr(bf_cfg, "BF_WS_RECO_RESYNC_AGE_THR_S", 6 * 3600.0))

        # Comptes par alias (upper)
        self.binance_accounts = { _upper(k): v for k, v in (binance_accounts or {}).items() }
        self.coinbase_accounts= { _upper(k): v for k, v in (coinbase_at_accounts or {}).items() }
        self.bybit_accounts   = { _upper(k): v for k, v in (bybit_accounts or {}).items() }

        if not self.dry_run:
            for alias, cfg in self.binance_accounts.items():
                if "api_key" not in cfg or "secret" not in cfg:
                    raise ValueError(f"Clés Binance incomplètes pour {alias}")
            for alias, cfg in self.coinbase_accounts.items():
                if "api_key" not in cfg or "secret" not in cfg:
                    raise ValueError(f"Clés Coinbase AT incomplètes pour {alias}")
            for alias, cfg in self.bybit_accounts.items():
                if "api_key" not in cfg or "secret" not in cfg:
                    raise ValueError(f"Clés Bybit incomplètes pour {alias}")

        # Defaults dry-run (TT/TM/MM sur 3 exchanges)
        self._dry_defaults = dry_defaults or {
            "BINANCE":  {"TT": {"USDC": 4000.0}, "TM": {"USDC": 4000.0}, "MM": {"USDC": 4000.0}},
            "COINBASE": {"TT": {"USDC": 4000.0}, "TM": {"USDC": 4000.0}, "MM": {"USDC": 4000.0}},
            "BYBIT":    {"TT": {"USDC": 4000.0}, "TM": {"USDC": 4000.0}, "MM": {"USDC": 4000.0}},
        }
        self.canonical_aliases: Tuple[str, ...] = ("TT", "TM", "MM")

        # Cache et historique
        self._cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._history: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        self._cache_lock = asyncio.Lock()
        self._history_lock = asyncio.Lock()

        self._virt: Dict[str, Dict[str, Dict[str, float]]] = {}
        self._virt_mode_set_abs: bool = False
        self.latest_balances: Dict[str, Dict[str, Dict[str, float]]] = {"BINANCE": {}, "COINBASE": {}, "BYBIT": {}}
        self.latest_wallets: Dict[str, Dict[str, Dict[str, Dict[str, float]]]] = {}
        self._default_wallet_type = str(
            getattr(self.cfg, "default_private_wallet", "SPOT") if self.cfg else "SPOT"
        ).upper()
        self._expected_wallet_types = self._build_expected_wallet_types()
        self._wallet_missing_log: Dict[Tuple[str, str, str], float] = {}
        self._wallet_missing_log_interval_s = float(
            getattr(self.cfg, "wallet_missing_log_interval_s", 60.0)
            if self.cfg else 60.0
        )


        # HTTP
        self._session: Optional[aiohttp.ClientSession] = None
        self._timeout = aiohttp.ClientTimeout(total=7.0, connect=3.0, sock_read=5.0)
        self._connector: Optional[aiohttp.TCPConnector] = None
        # Binance time offset
        self._binance_time_offset_ms: int = 0
        self._last_binance_time_sync: float = 0.0

        # State
        self._running = False
        self._limiter: Dict[Tuple[str, str], _RateLimiter] = {}
        self.last_sync_time = time.time()
        self.successful_syncs = 0
        self.error_count = 0
        self.last_error: Optional[str] = None
        self.latency_ms: Dict[Tuple[str, str], Optional[int]] = {}

        # Snap enrichi (fee tokens, vip…)
        self._snap: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._merged_snapshots: Dict[Tuple[str, str], Dict[str, Any]] = {}  # vue merged flattenée pour l'alerting
        self._event_sink = None  # callback optionnel (alerts)

        # Source WS (optionnelle): si un flux "account WS" alimente ces snapshots, on merge conservateur
        self._ws_balances: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._ws_lock = asyncio.Lock()
        # TTL pour considérer la source WS
        self._ws_ttl_s = float(getattr(self.cfg, "WS_BAL_TTL_SECONDS", 10.0)) if self.cfg else 10.0
        self._enable_ws_balance_merge = bool(getattr(self.cfg, "ENABLE_WS_BALANCE_MERGE", False)) if self.cfg else False

        # Statut WS / Reconciler (optionnel) pour marquer le capital « à risque »
        # Ces providers sont injectés par le Boot / orchestrateur:
        #  - Hub: get_alias_ws_status_snapshot(ex, alias) -> dict
        #  - Reconciler: get_alias_status_snapshot(ex, alias) -> dict
        self._ws_hub_status_provider = None
        self._ws_reco_status_provider = None

        # P0: alias de compat pour lecture uniforme des paramètres éventuels
        if not hasattr(self, "cfg"):
            self.cfg = None

        self._refresh_balance_ttls_from_slo()
        self._alerts_tasks: Set[asyncio.Task] = set()

    # =========================== Lifecycle ===================================

    async def update_ws_account_balances(self, exchange: str, alias: str, balances: Dict[str, float]) -> None:
        """
        Point d'entrée (optionnel) pour un flux WS 'account'. Le HUB peut l'appeler s'il dispose
        d'une telle source fiable. Merge conservateur côté lecture.
        """
        import time
        key = (exchange.upper(), alias.upper())
        async with self._ws_lock:
            if not self._enable_ws_balance_merge:
                return None
            self._ws_balances[key] = {"ts": time.time(), "data": {k.upper(): float(v) for k,v in (balances or {}).items()}}

    async def _get_ws_balances_if_fresh(self, exchange: str, alias: str) -> Dict[str, float] | None:
        import time
        key = (exchange.upper(), alias.upper())
        async with self._ws_lock:
            rec = self._ws_balances.get(key)
            if not rec: return None
            if (time.time() - float(rec.get("ts", 0.0))) > self._ws_ttl_s:
                return None
            return dict(rec.get("data") or {})

    @staticmethod
    def _conservative_merge(rest: Dict[str,float], ws: Dict[str,float]) -> Dict[str,float]:
        """
        Merge conservateur: on prend le MIN par devise pour éviter d'overstater l'available.
        Si une devise n’est pas dans l’une des sources, on prend l’autre (mais pas la somme).
        """
        keys = set(rest.keys()) | set(ws.keys())
        out = {}
        for k in keys:
            r = float(rest.get(k, 0.0))
            w = float(ws.get(k, 0.0))
            if k in rest and k in ws:
                out[k] = min(r, w)
            else:
                out[k] = r if k in rest else w
        return out

    def _get_fee_token_low_map(self) -> Dict[str, float]:
        """
        Map des seuils bas par token, ex:
          {"BNB": 40.0, "MNT": 15.0}
        Peut être fourni par config: FEE_TOKEN_LOW_WATERMARKS = {"BNB": 40, "MNT": 15}
        """
        try:
            m = getattr_dict(self.cfg, "FEE_TOKEN_LOW_WATERMARKS")
            # valeurs de secours si vides
            if not m:
                m = {"BNB": 40.0, "MNT": 15.0}
            return {str(k).upper(): float(v) for k, v in m.items()}
        except Exception:
            return {"BNB": 40.0, "MNT": 15.0}

    @staticmethod
    def _compute_fee_level(balance, low_thr):
        """
        Helper interne pour normaliser les niveaux de tokens de fees.

        Retourne (level, high_watermark) avec:
        - level ∈ {"low", "ok", "high"}
        - high_watermark = 2 × low_thr (ou 0 si low_thr=0)
        """
        balance = float(balance or 0.0)
        low_thr = float(low_thr or 0.0)

        if low_thr <= 0.0:
            # Pas de seuil configuré : on considère tout solde > 0 comme "ok"
            if balance <= 0.0:
                return "low", 0.0
            return "ok", 0.0

        if balance < low_thr:
            return "low", 2.0 * low_thr
        if balance >= 2.0 * low_thr:
            return "high", 2.0 * low_thr

        return "ok", 2.0 * low_thr

    def _refresh_balance_ttls_from_slo(self) -> None:
        """Construit le cache TTL balances par exchange depuis cfg.slo (fallback BF)."""
        self._bal_ttl_by_ex.clear()

        cfg = getattr(self, "cfg", None)
        g = getattr(cfg, "g", None)
        mode_key = str(getattr(g, "deployment_mode", "SPLIT")).upper() if g else "SPLIT"

        slo_map = getattr(cfg, "slo", None) if cfg else None
        if not slo_map:
            log.warning(
                "[BalanceFetcher] aucun balances_ttl_s_* trouvé dans cfg.slo[mode=%s], fallback sur TTL BF par défaut",
                mode_key,
            )
            return

        per_ex = slo_map.get(mode_key) or {}

        for ex, path_slo in (per_ex or {}).items():
            exu = str(ex).upper()
            pvt = getattr(path_slo, "private", None)
            if pvt is None:
                continue

            t_norm = float(getattr(pvt, "balances_ttl_s_normal", 0.0) or 0.0)
            t_deg = float(getattr(pvt, "balances_ttl_s_degraded", 0.0) or 0.0)
            t_blk = float(getattr(pvt, "balances_ttl_s_block", 0.0) or 0.0)

            if t_norm <= 0.0 or t_deg <= 0.0 or t_blk <= 0.0:
                continue
            if not (t_norm <= t_deg <= t_blk):
                log.warning(
                    "[BalanceFetcher] TTL balances incohérents pour %s (mode=%s): normal=%s, degraded=%s, block=%s",
                    exu,
                    mode_key,
                    t_norm,
                    t_deg,
                    t_blk,
                )
                continue

            self._bal_ttl_by_ex[exu] = {
                "normal": t_norm,
                "degraded": t_deg,
                "block": t_blk,
            }
            try:
                BF_BALANCES_TTL_NORMAL_SECONDS.labels(exchange=exu).set(t_norm)
                BF_BALANCES_TTL_DEGRADED_SECONDS.labels(exchange=exu).set(t_deg)
                BF_BALANCES_TTL_BLOCK_SECONDS.labels(exchange=exu).set(t_blk)
            except Exception:
                pass

        if not self._bal_ttl_by_ex:
            log.warning(
                "[BalanceFetcher] aucun balances_ttl_s_* trouvé dans cfg.slo[mode=%s], fallback sur TTL BF par défaut",
                mode_key,
            )

    @staticmethod
    def _classify_balance_health(age_s: float, t_norm: float, t_deg: float, t_blk: float) -> str:
        if age_s <= t_norm:
            return "NORMAL"
        if age_s <= t_deg:
            return "DEGRADED"
        if age_s <= t_blk:
            return "BLOCK"
        return "BLOCK"

    def _get_bal_ttls_for_exchange(self, ex: str) -> Tuple[float, float, float]:
        exu = str(ex).upper()
        per_ex = self._bal_ttl_by_ex.get(exu)
        if per_ex is not None:
            return (
                per_ex["normal"],
                per_ex["degraded"],
                per_ex["block"],
            )
        return (
            self._bal_ttl_default_normal,
            self._bal_ttl_default_degraded,
            self._bal_ttl_default_block,
        )

    def _compute_balances_freshness_status(self) -> Dict[str, Any]:
        states = []
        affected: List[str] = []
        for (ex, al), rec in (self._balances_health or {}).items():
            state = str((rec or {}).get("state") or "").upper()
            if state:
                states.append(state)
                if state in {"DEGRADED", "BLOCK"}:
                    affected.append(f"{ex}.{al}")

        if not states:
            worst_state = "UNKNOWN"
        elif "BLOCK" in states:
            worst_state = "BLOCK"
        elif "DEGRADED" in states:
            worst_state = "DEGRADED"
        else:
            worst_state = "NORMAL"

        bf_cfg = getattr(self, "_bf_cfg", self.cfg)
        ready_min = str(getattr(bf_cfg, "balances_ready_min_state", "NORMAL") or "NORMAL").upper()
        severity = {"NORMAL": 0, "DEGRADED": 1, "BLOCK": 2}
        min_rank = severity.get(ready_min, 0)
        worst_rank = severity.get(worst_state, 99)
        ready = worst_state != "UNKNOWN" and worst_rank <= min_rank

        reason_code = None
        if worst_state == "BLOCK":
            reason_code = normalize_reason_code("RM_BALANCE_TTL_BLOCK") or "RM_BALANCE_TTL_BLOCK"
        elif worst_state == "DEGRADED":
            reason_code = normalize_reason_code("RM_BALANCE_TTL_DEGRADED") or "RM_BALANCE_TTL_DEGRADED"

        return {
            "status": worst_state,
            "ready": bool(ready),
            "reason_code": reason_code,
            "affected": affected,
            "as_of_ts": time.time(),
        }

    def get_balances_freshness_status(self) -> Dict[str, Any]:
        try:
            status = self._compute_balances_freshness_status()
            prev = self._balances_freshness_status
            if prev.get("status") != status.get("status"):
                self._balances_freshness_status = status
                sink = getattr(self, "_event_sink", None)
                if callable(sink):
                    try:
                        sink({
                            "type": "balances_freshness",
                            "status": status.get("status"),
                            "ready": bool(status.get("ready")),
                            "reason_code": status.get("reason_code"),
                            "affected": status.get("affected"),
                            "ts": status.get("as_of_ts"),
                        })
                    except Exception:
                        pass
            return dict(self._balances_freshness_status)
        except Exception:
            return dict(self._balances_freshness_status)

    async def run_alerts(self) -> None:
        """
        Boucle d'alerting des tokens de frais bas.
        Périodicité par config BF_ALERT_PERIOD_S (def 15s).
        """
        period = getattr_float(self.cfg, "BF_ALERT_PERIOD_S", 15.0)
        low_map = self._get_fee_token_low_map()

        task = asyncio.current_task()
        if task:
            self._alerts_tasks.add(task)
        try:
            while self._running:
                try:
                    # itère sur nos snapshots merged (ou real)
                    for (ex, al), snap in list(self._merged_snapshots.items()):
                        balances = (snap or {}).get("balances") or {}
                        for tok, thr in low_map.items():
                            level = float(balances.get(tok, 0.0) or 0.0)
                            try:
                                BF_FEE_TOKEN_LEVEL.labels(ex, al, tok).set(level)
                            except Exception:
                                pass

                        if level < thr:
                                try:
                                    BF_FEE_TOKEN_LOW_TOTAL.labels(ex, al, tok).inc()
                                except Exception:
                                    pass
                                try:
                                    log.warning("[BF][%s:%s] Token de frais %s bas: %.4f < %.4f", ex, al, tok, level, thr)
                                except Exception:
                                    pass
                except Exception:
                    pass
                if not self._running:
                    break
                await asyncio.sleep(max(5.0, period))
        except asyncio.CancelledError:
            pass
        finally:
            if task:
                self._alerts_tasks.discard(task)


    async def start(self) -> None:
        if self._session and not self._session.closed:
            self._running = True
            return

        self._connector = aiohttp.TCPConnector(limit=80, ssl=True, enable_cleanup_closed=True)
        self._session = aiohttp.ClientSession(timeout=self._timeout, connector=self._connector)
        if self.use_server_time and not self.dry_run and self.binance_accounts:
            try:
                await self._sync_binance_time_offset()
            except Exception as e:
                logger.warning(f"[Binance] Sync server time échoué: {e}")
        self._running = True

    async def stop(self) -> None:
        self._running = False
        for t in list(getattr(self, "_alerts_tasks", set())):
            t.cancel()
        try:
            if getattr(self, "_alerts_tasks", None):
                await asyncio.gather(*self._alerts_tasks, return_exceptions=True)
        except Exception:
            pass
        self._alerts_tasks = set()
        if self._session and not self._session.closed:
            await self._session.close()
        if self._connector and not getattr(self._connector, "closed", False):
            self._connector.close()
        self._session = None
        self._connector = None



    async def start_sync_loop(self, interval: Optional[float] = None) -> None:
        """Boucle passive (force_refresh=True) — à lancer depuis l’orchestrateur si voulu."""
        self._running = True
        every = float(interval if interval is not None else self.bf_poll_interval_s)
        try:
            while self._running:
                try:
                    await self.get_all_balances(force_refresh=True)
                except Exception:
                    logger.exception("[MBF] sync loop error")
                # pacer-aware sleep
                mult = 1.0
                try:
                    state_eu = getattr_str(self.cfg, "BF_PACER_EU", "NORMAL").upper()
                    state_us = getattr_str(self.cfg, "BF_PACER_US", "NORMAL").upper()
                    if state_eu == "DEGRADED": mult = max(mult, 1.4)
                    if state_eu == "SEVERE":   mult = max(mult, 2.0)
                    if state_us == "DEGRADED": mult = max(mult, 1.6)
                    if state_us == "SEVERE":   mult = max(mult, 2.3)
                except Exception:
                    pass
                base = self.bf_poll_interval_s if interval is None else interval
                await asyncio.sleep(max(0.25, base * mult))

        except asyncio.CancelledError:
            pass

    # =========================== Overlay virtuel =============================
    def adjust_virtual(self, exchange: str, alias: str, ccy: str, delta: float) -> None:
        ex, al, cy = _upper(exchange), _upper(alias), _upper(ccy)
        self._virt_mode_set_abs = False
        self._virt.setdefault(ex, {}).setdefault(al, {}).setdefault(cy, 0.0)
        self._virt[ex][al][cy] = float(self._virt[ex][al][cy]) + float(delta)

    def set_virtual(self, exchange: str, alias: str, ccy: str, amount: float) -> None:
        ex, al, cy = _upper(exchange), _upper(alias), _upper(ccy)
        self._virt_mode_set_abs = True
        self._virt.setdefault(ex, {}).setdefault(al, {})[cy] = float(amount)

    def reset_virtual(self, exchange: str | None = None, alias: str | None = None, ccy: str | None = None) -> None:
        if exchange is None:
            self._virt.clear(); self._virt_mode_set_abs = False; return
        ex = _upper(exchange)
        if alias is None:
            self._virt.pop(ex, None); return
        al = _upper(alias)
        if ccy is None:
            self._virt.get(ex, {}).pop(al, None); return
        cy = _upper(ccy)
        try:
            self._virt[ex][al].pop(cy, None)
        except Exception:
            pass

    # =========================== Utils internes ==============================
    def _known_aliases(self, exchange: str) -> List[str]:
        ex = _upper(exchange)
        cfg_aliases = set()
        if ex == "BINANCE":  cfg_aliases |= set((self.binance_accounts or {}).keys())
        if ex == "COINBASE": cfg_aliases |= set((self.coinbase_accounts or {}).keys())
        if ex == "BYBIT":    cfg_aliases |= set((self.bybit_accounts or {}).keys())
        virt_aliases = set((self._virt.get(ex) or {}).keys())
        canon = set(self.canonical_aliases)
        return sorted(cfg_aliases | virt_aliases | canon)

    def _build_expected_wallet_types(self) -> Set[str]:
        base: List[str] = [self._default_wallet_type or "SPOT"]
        cfg_wallets = None
        cfg_rm = getattr(self.cfg, "rm", None) if self.cfg else None
        if cfg_rm and getattr(cfg_rm, "wallet_types", None):
            cfg_wallets = getattr(cfg_rm, "wallet_types")
        elif self.cfg and getattr(self.cfg, "wallet_types", None):
            cfg_wallets = getattr(self.cfg, "wallet_types")
        if not cfg_wallets:
            base.append("FUNDING")
        else:
            base.extend(str(w or "").upper() for w in cfg_wallets)
        return {str(w or "").upper() for w in base if str(w or "").strip()}

    def _maybe_log_missing_wallet_snapshot(self, exchange: str, alias: str, wallet: str) -> None:
        interval = max(5.0, float(getattr(self, "_wallet_missing_log_interval_s", 60.0)))
        key = (str(exchange).upper(), str(alias).upper(), str(wallet).upper())
        now = time.time()
        last = float(self._wallet_missing_log.get(key, 0.0))
        if now - last < interval:
            return
        self._wallet_missing_log[key] = now
        try:
            logger.warning(
                "[BalanceFetcher] wallet snapshot missing via PWS deltas for %s[%s] wallet=%s",
                key[0], key[1], key[2],
            )
        except Exception:
            pass

    def _apply_ccy_filter(self, balances: Dict[str, float]) -> Dict[str, float]:
        if not self.ccy_filter:
            return balances
        return {c: float(v) for c, v in balances.items() if _upper(c) in self.ccy_filter}

    async def _sync_binance_time_offset(self) -> None:
        assert self._session is not None
        url = f"{self.BINANCE_API}/api/v3/time"
        t0 = time.time()
        async with self._session.get(url) as resp:
            resp.raise_for_status()
            js = await resp.json()
        server_ms = int(js.get("serverTime"))
        t1 = time.time()
        now_ms = int(((t0 + t1) / 2.0) * 1000)
        self._binance_time_offset_ms = server_ms - now_ms
        self._last_binance_time_sync = time.time()

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        headers: Dict[str, str] | None = None,
        params: Dict[str, Any] | None = None,
        key: Tuple[str, str] | None = None,

        endpoint: str = "http",
    ) -> Any:
        if self._session is None or self._session.closed:
            await self.start()

        # pacer clamp (bf_request)
        try:
            w = float(PACER.clamp("bf_request"))
            if w > 0.0:
                await asyncio.sleep(w)
        except Exception:
            pass

        ex_label, al_label = (key if key else ("NA", "NA"))
        attempt = 0
        while True:
            attempt += 1
            t0_wall = time.time()
            ok = False
            try:
                if key is not None:
                    if key not in self._limiter:
                        self._limiter[key] = _RateLimiter(self.bf_rate_limit_per_min)
                    await self._limiter[key].take()

                async def _do():
                    assert self._session is not None
                    async with self._session.request(method, url, headers=headers, params=params) as resp:
                        try:
                            js = await resp.json()
                        except Exception:
                            txt = await resp.text()
                            js = {"raw": txt}
                        if resp.status >= 400:
                            raise aiohttp.ClientResponseError(
                                request_info=resp.request_info,
                                history=resp.history,
                                status=resp.status,
                                message=str(js),
                                headers=resp.headers,
                            )
                        return js

                with BF_HTTP_LATENCY_SECONDS.labels(ex_label, al_label, endpoint).time():
                    js = await _do()

                try:
                    BF_API_LATENCY_MS.labels(ex_label, al_label, endpoint).observe(
                        max(0.0, (time.time() - t0_wall) * 1000.0)
                    )
                except Exception:
                    pass

                ok = True
                self.last_error = None
                if key:
                    self.latency_ms[key] = int((time.time() - t0_wall) * 1000)
                    try:
                        BF_LAST_SUCCESS_TS.labels(ex_label, al_label).set(time.time())
                    except Exception:
                        pass
                    return js

            except Exception as e:
                try:
                    BF_HTTP_ERRORS_TOTAL.labels(ex_label, al_label, endpoint).inc()
                    BF_API_ERRORS_TOTAL.labels(ex_label, al_label, endpoint, "other").inc()
                except Exception:
                    pass
                self.error_count += 1
                self.last_error = str(e)
                if attempt >= self.max_retries:
                    if self.verbose:
                        logger.error(f"[HTTP] {method} {url} failed after {attempt}: {e}")
                    raise
                backoff = min(self.retry_max_delay, self.retry_base_delay * (2 ** (attempt - 1)))
                backoff *= (0.7 + 0.6 * random.random())
                if self.verbose:
                    logger.warning(f"[HTTP] retry in {backoff:.2f}s — {e}")
                await asyncio.sleep(backoff)
            finally:
                # FYI: BF_API_LATENCY_MS déjà set plus haut; on évite double observe.
                pass

    # =========================== Binance =====================================
    async def get_binance_balances(self, alias: str, *, force_refresh: bool = False) -> Dict[str, float]:
        al = _upper(alias)
        key = ("BINANCE", al)

        if self.dry_run:
            return self._apply_ccy_filter(dict(self._dry_defaults["BINANCE"].get(al, {})))
        if al not in self.binance_accounts:
            raise KeyError(f"Alias Binance inconnu: {al}")
        if self._session is None or self._session.closed:
            await self.start()

        now = time.time()
        async with self._cache_lock:
            c = self._cache.get(key)
            if c and not force_refresh and (now - c.get("timestamp", 0.0) < self.cache_ttl):
                return self._apply_ccy_filter(dict(c.get("data", {})))

        creds = self.binance_accounts[al]
        ts_ms = int(time.time() * 1000 + (self._binance_time_offset_ms if self.use_server_time else 0))
        query = f"timestamp={ts_ms}&recvWindow=5000"
        sig = hmac.new(creds["secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
        url = f"{self.BINANCE_API}/api/v3/account?{query}&signature={sig}"
        headers = {"X-MBX-APIKEY": creds["api_key"]}

        try:
            js = await self._request_json("GET", url, headers=headers, key=key, endpoint="binance/account")

            # Re-try si décalage temps
            if isinstance(js, dict) and js.get("code") == -1021 and self.use_server_time:
                await self._sync_binance_time_offset()
                ts_ms = int(time.time() * 1000 + self._binance_time_offset_ms)
                query = f"timestamp={ts_ms}&recvWindow=5000"
                sig = hmac.new(creds["secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
                url = f"{self.BINANCE_API}/api/v3/account?{query}&signature={sig}"
                js = await self._request_json("GET", url, headers=headers, key=key, endpoint="binance/account")

            balances: Dict[str, float] = {}
            for a in (js.get("balances") or []):
                try:
                    free = float(a.get("free") or 0)
                    asset = _upper(a.get("asset"))
                    if free > 0 and asset:
                        balances[asset] = free
                except Exception:
                    continue

            balances = self._apply_ccy_filter(balances)
            async with self._cache_lock:
                self._cache[key] = {"timestamp": now, "data": balances}
            async with self._history_lock:
                self._history.setdefault(key, []).append({"timestamp": now, "data": balances})
            self._rebuild_latest_balances_snapshot()
            self.last_sync_time = now
            self.successful_syncs += 1
            # Merge conservateur avec source WS si fraiche (optionnelle)
            try:
                ws_bal = await self._get_ws_balances_if_fresh("BINANCE", alias)
                if ws_bal:
                    balances = self._conservative_merge(balances, ws_bal)
            except Exception:
                pass

            return balances
        except Exception:
            logger.exception("[Binance:%s] balances error", al)
            return {}

    # =========================== Coinbase Advanced Trade ======================
    @staticmethod
    def _cb_headers(secret: str, method: str, path: str, body: str = "", *, api_key: Optional[str] = None) -> Dict[str, str]:
        ts = str(int(time.time()))
        try:
            secret_bytes = base64.b64decode(secret)
        except Exception:
            secret_bytes = secret.encode()
        prehash = f"{ts}{method}{path}{body}".encode()
        sign = base64.b64encode(hmac.new(secret_bytes, prehash, hashlib.sha256).digest()).decode()
        headers = {
            "CB-ACCESS-TIMESTAMP": ts,
            "CB-ACCESS-SIGN": sign,
            "Content-Type": "application/json",
        }
        if api_key:
            headers["CB-ACCESS-KEY"] = api_key
        return headers

    async def get_coinbase_at_balances(self, alias: str, *, force_refresh: bool = False) -> Dict[str, float]:
        al = _upper(alias)
        key = ("COINBASE", al)

        if self.dry_run:
            return self._apply_ccy_filter(dict(self._dry_defaults["COINBASE"].get(al, {})))
        if al not in self.coinbase_accounts:
            raise KeyError(f"Alias Coinbase AT inconnu: {al}")
        if self._session is None or self._session.closed:
            await self.start()

        now = time.time()
        async with self._cache_lock:
            c = self._cache.get(key)
            if c and not force_refresh and (now - c.get("timestamp", 0.0) < self.cache_ttl):
                return self._apply_ccy_filter(dict(c.get("data", {})))

        cfg = self.coinbase_accounts[al]
        path = "/api/v3/brokerage/accounts"
        headers = self._cb_headers(cfg["secret"], "GET", path, api_key=cfg["api_key"])
        url = f"{self.COINBASE_API}{path}"

        try:
            js = await self._request_json("GET", url, headers=headers, key=key, endpoint="coinbase/accounts")
            balances: Dict[str, float] = {}
            for acc in (js.get("accounts") or []):
                try:
                    ccy = _upper(acc.get("currency") or (acc.get("balance") or {}).get("currency"))
                    amt = float((acc.get("available_balance") or acc.get("balance") or {}).get("value") or 0)
                    if amt > 0 and ccy:
                        balances[ccy] = balances.get(ccy, 0.0) + amt
                except Exception:
                    continue

            balances = self._apply_ccy_filter(balances)
            async with self._cache_lock:
                self._cache[key] = {"timestamp": now, "data": balances}
            async with self._history_lock:
                self._history.setdefault(key, []).append({"timestamp": now, "data": balances})
            self._rebuild_latest_balances_snapshot()
            self.last_sync_time = now
            self.successful_syncs += 1
            # Merge conservateur avec source WS si fraiche (optionnelle)
            try:
                ws_bal = await self._get_ws_balances_if_fresh("COINBASE", alias)
                if ws_bal:
                    balances = self._conservative_merge(balances, ws_bal)
            except Exception:
                pass

            return balances
        except Exception:
            logger.exception("[CoinbaseAT:%s] balances error", al)
            return {}

    # =========================== Bybit v5 Unified =============================
    @staticmethod
    def _bybit_sign(secret: str, ts_ms: str, method: str, path: str, query: str, body: str) -> str:
        raw = f"{ts_ms}{method}{path}{query}{body}".encode()
        return hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()

    async def get_bybit_balances(self, alias: str, *, force_refresh: bool = False) -> Dict[str, float]:
        al = _upper(alias)
        key = ("BYBIT", al)

        if self.dry_run:
            return self._apply_ccy_filter(dict(self._dry_defaults["BYBIT"].get(al, {})))
        if al not in self.bybit_accounts:
            raise KeyError(f"Alias Bybit inconnu: {al}")
        if self._session is None or self._session.closed:
            await self.start()

        now = time.time()
        async with self._cache_lock:
            c = self._cache.get(key)
            if c and not force_refresh and (now - c.get("timestamp", 0.0) < self.cache_ttl):
                return self._apply_ccy_filter(dict(c.get("data", {})))

        cfg = self.bybit_accounts[al]
        path = "/v5/account/wallet-balance"
        query = "?accountType=UNIFIED"
        ts_ms = str(int(time.time() * 1000))
        sign = self._bybit_sign(cfg["secret"], ts_ms, "GET", path, query, "")
        headers = {
            "X-BAPI-SIGN": sign,
            "X-BAPI-API-KEY": cfg["api_key"],
            "X-BAPI-TIMESTAMP": ts_ms,
            "X-BAPI-RECV-WINDOW": "5000",
        }
        url = f"{self.BYBIT_API}{path}{query}"

        try:
            js = await self._request_json("GET", url, headers=headers, key=key, endpoint="bybit/balance")
            balances: Dict[str, float] = {}
            for item in (js.get("result", {}).get("list") or []):
                for coin in (item.get("coin") or []):
                    ccy = _upper(coin.get("coin"))
                    free = float(coin.get("availableToWithdraw") or 0)
                    if free > 0 and ccy:
                        balances[ccy] = balances.get(ccy, 0.0) + free

            balances = self._apply_ccy_filter(balances)
            async with self._cache_lock:
                self._cache[key] = {"timestamp": now, "data": balances}
            async with self._history_lock:
                self._history.setdefault(key, []).append({"timestamp": now, "data": balances})
            self._rebuild_latest_balances_snapshot()
            self.last_sync_time = now
            self.successful_syncs += 1
            # Merge conservateur avec source WS si fraiche (optionnelle)
            try:
                ws_bal = await self._get_ws_balances_if_fresh("BYBIT", alias)
                if ws_bal:
                    balances = self._conservative_merge(balances, ws_bal)
            except Exception:
                pass

            return balances
        except Exception:
            logger.exception("[Bybit:%s] balances error", al)
            return {}

    # =========================== Agrégations (réel) ===========================
    async def get_all_balances(self, *, force_refresh: bool = False) -> Dict[str, Dict[str, Dict[str, float]]]:
        if self._session is None or self._session.closed:
            await self.start()

        tasks: List[asyncio.Task] = []
        order: List[Tuple[str, str]] = []

        for al in self.binance_accounts.keys():
            tasks.append(self.get_binance_balances(al, force_refresh=force_refresh))
            order.append(("BINANCE", al))
        for al in self.coinbase_accounts.keys():
            tasks.append(self.get_coinbase_at_balances(al, force_refresh=force_refresh))
            order.append(("COINBASE", al))
        for al in self.bybit_accounts.keys():
            tasks.append(self.get_bybit_balances(al, force_refresh=force_refresh))
            order.append(("BYBIT", al))

        res = await asyncio.gather(*tasks, return_exceptions=True)
        out: Dict[str, Dict[str, Dict[str, float]]] = {"BINANCE": {}, "COINBASE": {}, "BYBIT": {}}
        for (ex, al), data in zip(order, res):
            out[ex][al] = {} if isinstance(data, Exception) else (data or {})

        # met à jour latest_balances (réel)
        self.latest_balances = {ex: {al: dict(bal) for al, bal in per.items()} for ex, per in out.items()}

        # snapshots enrichis
        snap_tasks = []
        for ex, per_alias in out.items():
            for al, bal in per_alias.items():
                snap_tasks.append(self._record_snapshot(ex, al, bal))
        if snap_tasks:
            await asyncio.gather(*snap_tasks, return_exceptions=True)

        return out

    async def resync_balances_for_alias(self, exchange: str, alias: str) -> Dict[str, float]:
        """
        Resync ciblé des balances pour un (exchange, alias).

        Utilisé en mode dégradé (signaux WS/Reconciler) pour forcer un refresh
        REST ponctuel sans impacter les autres aliases.
        """
        ex, al = _upper(exchange), _upper(alias)

        if ex == "BINANCE":
            balances = await self.get_binance_balances(al, force_refresh=True)
        elif ex in ("COINBASE", "COINBASE_AT"):
            balances = await self.get_coinbase_at_balances(al, force_refresh=True)
        elif ex == "BYBIT":
            balances = await self.get_bybit_balances(al, force_refresh=True)
        else:
            raise ValueError(f"Exchange inconnu pour resync balances: {exchange}")

        # Event interne pour tracer le moment où MBF a effectivement rafraîchi
        # les balances pour (exchange, alias) après un signal de capital en mouvement.
        self._emit_capital_refresh_event(ex, al, source="resync_alias")

        return balances


    async def get_balances(self, mode: str = "real", *, force_refresh: bool = False) -> Dict[
        str, Dict[str, Dict[str, float]]]:
        if mode.lower() != "virtual":
            await self.get_all_balances(force_refresh=force_refresh)
        return self.get_balances_snapshot(mode)

    # =========================== Snapshots / vues =============================
    def get_balances_snapshot(self, mode: str = "real") -> Dict[str, Dict[str, Dict[str, float]]]:
        m = (mode or "real").lower()
        if m == "real":
            snap = {ex: {al: dict(bal) for al, bal in per.items()} for ex, per in (self.latest_balances or {}).items()}
            for ex in ("BINANCE", "COINBASE", "BYBIT"):
                snap.setdefault(ex, {})
                for al in self._known_aliases(ex):
                    snap[ex].setdefault(al, {})
            return snap
        if m == "virtual":
            return self._virt_view_filtered()
        # merged
        base = self.get_balances_snapshot("real")
        return self._merge_real_virtual(base)

    def _virt_view_filtered(self) -> Dict[str, Dict[str, Dict[str, float]]]:
        out: Dict[str, Dict[str, Dict[str, float]]] = {}
        for ex, per_al in (self._virt or {}).items():
            out[ex] = {}
            for al, per_ccy in (per_al or {}).items():
                out[ex][al] = self._apply_ccy_filter({cy: float(v) for cy, v in (per_ccy or {}).items()})
        for ex in ("BINANCE", "COINBASE", "BYBIT"):
            out.setdefault(ex, {})
            for al in self._known_aliases(ex):
                out[ex].setdefault(al, {})
        return out

    def _merge_real_virtual(self, real: Dict[str, Dict[str, Dict[str, float]]]) -> Dict[str, Dict[str, Dict[str, float]]]:
        virt = self._virt_view_filtered()
        out: Dict[str, Dict[str, Dict[str, float]]] = {}
        for ex in sorted(set(real.keys()) | set(virt.keys())):
            out[ex] = {}
            for al in sorted(set((real.get(ex) or {}).keys()) | set((virt.get(ex) or {}).keys())):
                out[ex][al] = {}
                ccys = set(((real.get(ex) or {}).get(al) or {}).keys()) | set(((virt.get(ex) or {}).get(al) or {}).keys())
                for cy in ccys:
                    r = float(((real.get(ex) or {}).get(al) or {}).get(cy, 0.0))
                    v = float(((virt.get(ex) or {}).get(al) or {}).get(cy, 0.0))
                    out[ex][al][cy] = v if (self._virt_mode_set_abs and cy in ((virt.get(ex) or {}).get(al) or {})) else (r + v)
        return out

    def _rebuild_latest_balances_snapshot(self) -> None:
        try:
            new_latest: Dict[str, Dict[str, Dict[str, float]]] = {"BINANCE": {}, "COINBASE": {}, "BYBIT": {}}
            for (ex, al), rec in list((self._cache or {}).items()):
                ex_up, al_up = _upper(ex), _upper(al)
                balances = self._apply_ccy_filter(((rec or {}).get("data")) or {})
                new_latest.setdefault(ex_up, {})
                new_latest[ex_up][al_up] = {cy: float(v) for cy, v in (balances or {}).items()}
            self.latest_balances = new_latest

            real_snap = self.get_balances_snapshot("real")
            merged_snap = self._merge_real_virtual(real_snap)

            new_merged: Dict[Tuple[str, str], Dict[str, Any]] = {}
            for ex, per_al in (merged_snap or {}).items():
                for al, bal in (per_al or {}).items():
                    ex_up, al_up = _upper(ex), _upper(al)
                    new_merged[(ex_up, al_up)] = {"balances": {cy: float(v) for cy, v in (bal or {}).items()}}
            self._merged_snapshots = new_merged

            for ex, per_al in (real_snap or {}).items():
                for al, bal in (per_al or {}).items():
                    self._record_snapshot_sync(ex, al, bal)
        except Exception:
            logger.exception("MultiBalanceFetcher: rebuild_latest_balances_snapshot failed")

    # Poches par quote (USDC/EUR/USD)
    def get_pockets_by_quote(self, *, mode: str = "real", quotes: Tuple[str, ...] = ("USDC", "EUR", "USD")) -> Dict[str, Dict[str, Dict[str, float]]]:
        snap = self.get_balances_snapshot(mode)
        qset = tuple(_upper(q) for q in quotes or ())
        out: Dict[str, Dict[str, Dict[str, float]]] = {}
        for ex, per in (snap or {}).items():
            out.setdefault(ex, {})
            for al, bal in (per or {}).items():
                out[ex].setdefault(al, {q: 0.0 for q in qset})
                for ccy, amt in (bal or {}).items():
                    u = _upper(ccy)
                    if u in qset:
                        out[ex][al][u] += float(amt or 0.0)
        return out

    # Niveaux tokens de frais (BNB/MNT) (lecture des balances)
    def get_fee_token_levels_from_balances(self, *, mode: str = "real", tokens: Tuple[str, ...] = ("BNB", "MNT")) -> Dict[str, Dict[str, Dict[str, float]]]:
        snap = self.get_balances_snapshot(mode)
        tset = tuple(_upper(t) for t in tokens or ())
        out: Dict[str, Dict[str, Dict[str, float]]] = {}
        for ex, per in (snap or {}).items():
            out.setdefault(ex, {})
            for al, bal in (per or {}).items():
                out[ex].setdefault(al, {})
                for tok in tset:
                    out[ex][al][tok] = float((bal or {}).get(tok, 0.0))
        return out

    def as_buffers_snapshot(
        self,
        *,
        mode: str = "merged",
        quotes: Iterable[str] = ("USDC",),
        fee_tokens: Iterable[str] = ("BNB", "MNT"),
        cached_only: bool = True,
    ) -> Dict[str, Any]:
        """
        Vue unique des « buffers » de capital pour le RiskManager et le
        simulateur d’exécution.

        Contrat de sortie (schéma logique) :

        {
            "pockets_by_quote": {
                "EXCHANGE": {"ALIAS": {"QUOTE": available_float}}
            },
            "meta": {
                "fee_tokens": {
                    "TOKEN": {
                        "EX.ALIAS": {
                            "balance": float,
                            "level": "low" | "ok" | "high",
                            "low_watermark": float,
                            "high_watermark": float,
                        },
                    },

                },
             "age_s": float,
            },
            "mode": "merged" | "real" | "virtual",
            "as_of_ts": <float monotonic>,
        }

        - `pockets_by_quote` est la base UNIQUE pour dimensionner les capacités
          de capital par quote / alias (caps profils, RM, simulateur).
        - `meta["fee_tokens"]` reprend la structure enrichie déjà produite par
          `_record_snapshot_sync`, pour consommation directe par le RM.
        """
        # Vue « poches par quote » : base de capacité capital
        view = (mode or "merged").lower()
        if view not in ("real", "virtual", "merged"):
            view = "merged"

        raw_pockets = self.get_pockets_by_quote(
            mode=view,
            quotes=tuple(quotes),
        )
        pockets: Dict[str, Dict[str, Dict[str, float]]] = {}
        for ex, per_alias in (raw_pockets or {}).items():
            exu = _upper(ex)
            ex_map = pockets.setdefault(exu, {})
            for alias, per_quote in (per_alias or {}).items():
                alu = _upper(alias)
                alias_map = ex_map.setdefault(alu, {})
                for quote, amt in (per_quote or {}).items():
                    alias_map[_upper(quote)] = float(amt or 0.0)

        meta_fee: Dict[str, Dict[str, Dict[str, Any]]] = {}
        meta_bal_health: Dict[str, Dict[str, Any]] = {}
        now = time.time()
        age_s = float("inf")
        for (ex, al), rec in list(getattr(self, "_snap", {}).items()):
            ts = float(rec.get("ts", 0.0) or 0.0)
            if ts > 0.0:
                age_s = min(age_s, max(0.0, now - ts)) if age_s != float("inf") else max(0.0, now - ts)
            bal_age = max(0.0, now - ts) if ts > 0.0 else float("inf")
            t_norm, t_deg, t_blk = self._get_bal_ttls_for_exchange(ex)
            state = self._classify_balance_health(bal_age, t_norm, t_deg, t_blk)
            prev = self._balances_health.get((ex, al), {}).get("state")
            last_change_ts = self._balances_health.get((ex, al), {}).get("last_change_ts", now)
            if prev != state:
                last_change_ts = now
            self._balances_health[(ex, al)] = {
                "state": state,
                "last_age_s": bal_age,
                "ttl_normal_s": t_norm,
                "ttl_degraded_s": t_deg,
                "ttl_block_s": t_blk,
                "last_change_ts": last_change_ts,
            }
            try:
                BF_BALANCES_HEALTH_STATE.labels(exchange=ex, alias=al).set(
                    0 if state == "NORMAL" else 1 if state == "DEGRADED" else 2
                )
                BF_BALANCES_TTL_NORMAL_SECONDS.labels(exchange=ex).set(t_norm)
                BF_BALANCES_TTL_DEGRADED_SECONDS.labels(exchange=ex).set(t_deg)
                BF_BALANCES_TTL_BLOCK_SECONDS.labels(exchange=ex).set(t_blk)
            except Exception:
                pass
            meta_bal_health[f"{ex}.{al}"] = {
                "state": state,
                "age_s": bal_age,
                "ttl_normal_s": t_norm,
                "ttl_degraded_s": t_deg,
                "ttl_block_s": t_blk,
            }
            for token, info in (rec.get("fee_tokens") or {}).items():
                tkn = str(token).upper()
                meta_fee.setdefault(tkn, {})
                meta_fee[tkn][f"{ex}.{al}"] = {
                    "balance": float(info.get("balance", 0.0) or 0.0),
                    "level": str(info.get("level", "")).lower() or "unknown",
                    "low_watermark": float(info.get("low_watermark", 0.0) or 0.0),
                    "high_watermark": float(info.get("high_watermark", 0.0) or 0.0),
                }

        freshness_status = self.get_balances_freshness_status()


        return {
            "mode": view,
            "as_of_ts": time.time(),
            "pockets_by_quote": pockets,
            "meta": {
                "fee_tokens": meta_fee,
                "age_s": age_s if age_s != float("inf") else None,
                "balances_health": meta_bal_health,
                "balances_freshness": freshness_status,
            },
        }
    def get_available_quote_simple(
        self,
        exchange: str,
        alias: str,
        *,
        quote_priority: Tuple[str, ...] = ("USDC", "EUR", "USD"),
        mode: str = "real",
    ) -> float:
        pockets = self.get_pockets_by_quote(mode=mode, quotes=quote_priority)
        per = ((pockets.get(_upper(exchange)) or {}).get(_upper(alias)) or {})
        for q in quote_priority:
            v = float(per.get(_upper(q), 0.0))
            if v > 0: return v
        return 0.0

    # =========================== Snap enrichi / VIP ==========================
    # =========================== Snap enrichi / VIP ==========================
    def set_event_sink(self, sink) -> None:
        """Enregistre un callback d'évènements internes MBF.

        Évènements émis actuellement (liste non exhaustive) :
          - {"type": "capital_refresh", "exchange": ..., "alias": ..., "source": ..., "ts": ...}
            → utilisé notamment par le Ticket 8 pour tracer la visibilité des resyncs
              de balances post-transfert interne.
        """
        self._event_sink = sink

    def set_ws_status_providers(
            self,
            *,
            hub: Optional[Any] = None,
            reconciler: Optional[Any] = None,
    ) -> None:
        """
        Injection optionnelle des providers de statut comptes WS:

        - hub: objet ou callable fournissant get_alias_ws_status_snapshot(ex, alias)
        - reconciler: objet ou callable fournissant get_alias_status_snapshot(ex, alias)

        Ces statuts sont agrégés dans as_rm_snapshot().meta["ws_accounts"] afin que
        le RiskManager puisse marquer un alias comme « capital_at_risk » et, si
        besoin, déclencher/prioriser un resync balances ciblé.
        """
        if hub is not None:
            fn = getattr(hub, "get_alias_ws_status_snapshot", hub)
            if callable(fn):
                self._ws_hub_status_provider = fn
        if reconciler is not None:
            fn = getattr(reconciler, "get_alias_status_snapshot", reconciler)
            if callable(fn):
                self._ws_reco_status_provider = fn

    def _emit_capital_refresh_event(self, exchange: str, alias: str, source: str = "resync_alias") -> None:
        """Émet un évènement interne lorsqu'un resync ciblé (exchange, alias)
        vient d'être exécuté.

        Utilisé dans le cadre du Ticket 8 pour corréler les transferts internes
        (Hub/RM) avec la mise à jour effective des balances MBF.
        """
        sink = getattr(self, "_event_sink", None)
        if not sink:
            return
        try:
            ev = {
                "type": "capital_refresh",
                "exchange": _upper(exchange),
                "alias": _upper(alias),
                "source": str(source),
                "ts": time.time(),
            }
            sink(ev)
        except Exception:
            log.exception("MultiBalanceFetcher: event_sink capital_refresh failed")


    async def _record_snapshot_sync(self, exchange: str, alias: str, balances: Dict[str, float]) -> None:
        ex, al = _upper(exchange), _upper(alias)
        now = time.time()
        rec = self._snap.get((ex, al), {"balances": {}, "fee_tokens": {}, "vip": {}, "vip_ts": 0.0})

        # Snapshot brut des soldes
        rec["balances"] = dict(balances or {})
        rec["ts"] = now

        # Normalisation des niveaux de tokens de fees pour RM / caps.
        # Source de seuils: FEE_TOKEN_LOW_WATERMARKS (par token) via BotConfig.
        low_map = self._get_fee_token_low_map() if self.cfg is not None else {}
        ft: Dict[str, Dict[str, Any]] = {}

        for tkn, low in (low_map or {}).items():
            bal = float(balances.get(tkn, 0.0) or 0.0)
            level, high = self._compute_fee_level(bal, low)

            ft[tkn] = {
                # Structure standardisée pour le RM
                "balance": bal,
                "level": level,  # "low" | "ok" | "high"
                "low_watermark": float(low or 0.0),
                "high_watermark": float(high or 0.0),
            }

        rec["fee_tokens"] = ft
        self._snap[(ex, al)] = rec

    async def _record_snapshot(self, exchange: str, alias: str, balances: Dict[str, float]) -> None:
        self._record_snapshot_sync(exchange, alias, balances)


    def _get_ws_accounts_status_for_alias(self, exchange: str, alias: str) -> Dict[str, Any]:
        """
        Agrège les statuts comptes WS pour un (exchange, alias) à partir des
        providers Hub / Reconciler, et dérive un flag capital_at_risk.
        Retourne toujours une structure minimale même si les providers ne
        remontent pas de données pour l’alias demandé.
        """
        ex, al = _upper(exchange), _upper(alias)

        hub_snap: Optional[Dict[str, Any]] = None
        reco_snap: Optional[Dict[str, Any]] = None

        try:
            provider = getattr(self, "_ws_hub_status_provider", None)
            if callable(provider):
                hub_snap = provider(ex, al) or {}
        except Exception:
            hub_snap = None

        try:
            provider = getattr(self, "_ws_reco_status_provider", None)
            if callable(provider):
                reco_snap = provider(ex, al) or {}
        except Exception:
            reco_snap = None

        hub_model = None
        reco_model = None

        try:
            if hub_snap:
                hub_model = validate_ws_alias_status_snapshot_lite(hub_snap)
        except Exception:
            hub_model = None

        try:
            if reco_snap:
                reco_model = validate_reco_alias_status_snapshot_lite(reco_snap)
        except Exception:
            reco_model = None

        hub_dict = hub_model.model_dump() if hub_model is not None else (hub_snap or {})
        reco_dict = reco_model.model_dump() if reco_model is not None else (reco_snap or {})

        hub_status = (hub_dict or {}).get("status", "WS_UNKNOWN")
        reco_status = (reco_dict or {}).get("status", "UNKNOWN")

        miss_rate_per_min = float((reco_dict or {}).get("miss_rate_per_min", 0.0) or 0.0)
        age_since_resync = float((reco_dict or {}).get("age_since_last_alias_resync_s", 0.0) or 0.0)


        capital_at_risk = self._derive_capital_at_risk(
            ws_status=hub_status,
            reco_status=reco_status,
            miss_rate_per_min=miss_rate_per_min,
            age_since_last_alias_resync_s=age_since_resync,
            thr_miss_rate=self._ws_miss_rate_thr,
            thr_resync_age_s=self._ws_resync_age_thr_s,
        )
        out: Dict[str, Any] = {
            "exchange": ex,
            "alias": al,
            "capital_at_risk": capital_at_risk,
            "hub_status": hub_status,
            "reco_status": reco_status,
            "miss_rate_per_min": miss_rate_per_min,
            "age_since_last_alias_resync_s": age_since_resync if age_since_resync > 0.0 else None,
            "raw_hub": hub_dict or {},
            "raw_reconciler": reco_dict or {},
        }

        if hub_dict:
            out.update({
                "hub_healthy": hub_dict.get("healthy"),
                "hub_heartbeat_gap_s": hub_dict.get("heartbeat_gap_s"),
                "hub_last_event_ts": hub_dict.get("last_event_ts"),
                "hub_errors_total": hub_dict.get("errors_total"),
                "hub_reconnects_total": hub_dict.get("reconnects_total"),
            })

        if reco_dict:
            out.update({
                "reco_miss_rate_per_min": reco_dict.get("miss_rate_per_min"),
                "reco_misses_recent": reco_dict.get("misses_recent"),
                "reco_last_alias_resync_ts": reco_dict.get("last_alias_resync_ts"),
                "reco_age_since_last_alias_resync_s": reco_dict.get("age_since_last_alias_resync_s"),
            })

        return out

    @staticmethod
    def _derive_capital_at_risk(
            *,
            ws_status: str,
            reco_status: str,
            miss_rate_per_min: float,
            age_since_last_alias_resync_s: float | None,
            thr_miss_rate: float,
            thr_resync_age_s: float,
    ) -> bool:
        """Heuristique commune Hub/Reconciler pour déterminer capital_at_risk.

        True si:
          - Reconciler BROKEN ou WS DOWN
          - Reconciler AT_RISK ou WS_DEGRADED avec miss_rate/resync_age au-delà
            des seuils fournis.
        """
        ws_status_u = (ws_status or "WS_UNKNOWN").upper()
        reco_status_u = (reco_status or "UNKNOWN").upper()

        if reco_status_u in ("BROKEN",):
            return True
        if ws_status_u in ("WS_DOWN",):
            return True

        if reco_status_u in ("AT_RISK",) or ws_status_u in ("WS_DEGRADED",):
            if float(miss_rate_per_min or 0.0) >= float(thr_miss_rate or 0.0):
                return True
            if float(age_since_last_alias_resync_s or 0.0) >= float(thr_resync_age_s or 0.0):
                return True

        return False

    def as_rm_snapshot(self, *, mode: str = "real", cached_only: bool = False) -> Dict[str, Any]:
        """
        Snapshot unifié prêt pour le RiskManager / Rebalancing.

        Retourne toujours un dict de la forme :
            {
                "mode": "real" | "virtual" | "merged",
                "balances": {                      # vue capitale opérationnelle
                    EXCHANGE: {ALIAS: {ASSET: float, ...}, ...},
                    ...
                },
                 "meta": {                          # métriques de fraîcheur & fees & WS
                    "age_s": { "EX.ALIAS": float, ... },
                    "fee_tokens": {
                        TOKEN: {
                            "EX.ALIAS": {
                                "balance": float,
                                "level": "low" | "ok" | "high",
                                "low_watermark": float,
                                "high_watermark": float,
                            },
                            ...,
                        },
                    "vip": { "EX.ALIAS": {...}, ... },},
                    "ws_accounts": {               # statut comptes WS agrégé
                           "EX.ALIAS": {
                            "hub_status": "WS_OK" | "WS_DEGRADED" | "WS_DOWN" | "WS_UNKNOWN",
                            "reco_status": "OK" | "AT_RISK" | "BROKEN" | "UNKNOWN",
                            "capital_at_risk": bool,
                            "miss_rate_per_min": float | None,
                            "age_since_last_alias_resync_s": float | None,
                            "raw_hub": {...},         # optionnel
                            "raw_reconciler": {...},  # optionnel
                        },
                        ...,
                    },
                    "balances_health": {
                        "EX.ALIAS": {
                            "state": "NORMAL" | "DEGRADED" | "BLOCK",
                            "age_s": float,
                            "ttl_normal_s": float,
                            "ttl_degraded_s": float,
                            "ttl_block_s": float,
                        },
                        ...,

                    },
                    "view": "real" | "virtual" | "merged",
                    "cached_only": bool,
                    "as_of_ts": float,  # horodatage monotonic
                },

        - `mode` est aligné sur `get_balances_snapshot` (real/virtual/merged).
        - `cached_only=True` ne déclenche aucune I/O : si rien n’est en cache,
          on renvoie juste des métadonnées cohérentes.
        - `meta["age_s"]` est la source UNIQUE des TTL balances par (exchange, alias)
          pour le RiskManager, y compris la surcouche « capital en mouvement »
          introduite au Ticket 8.
        """


        view = (mode or "real").lower()
        if view not in ("real", "virtual", "merged"):
            view = "real"

        # Vue brute selon le mode choisi (real/virtual/merged)
        base_balances = self.get_balances_snapshot(view) or {}

        now = time.time()
        meta_age: Dict[str, float] = {}
        meta_vip: Dict[str, Any] = {}
        meta_fee: Dict[str, Dict[str, Any]] = {}
        meta_ws_accounts: Dict[str, Dict[str, Any]] = {}
        meta_bal_health: Dict[str, Dict[str, Any]] = {}

        # Copie défensive des balances (pour éviter les mutations externes)
        balances: Dict[str, Dict[str, Dict[str, float]]] = {
            ex: {al: dict(ccy_map or {}) for al, ccy_map in (per or {}).items()}
            for ex, per in (base_balances or {}).items()
        }

        # Parcours du cache interne _snap pour enrichir meta + backfill éventuel
        for (ex, al), rec in list(self._snap.items()):
            ts = float(rec.get("ts", 0.0))
            age = max(0.0, now - ts) if ts > 0.0 else float("inf")
            meta_age[f"{ex}.{al}"] = age
            # Observabilité TTL cache par alias (gauge Prometheus)
            if ts > 0.0:
                try:
                    BF_CACHE_AGE_SECONDS.labels(ex, al).set(float(age))
                except Exception:
                    # On ne casse jamais la vue RM pour un problème de métriques.
                    pass


            vip = rec.get("vip") or {}
            if vip:
                meta_vip[f"{ex}.{al}"] = vip

            ft = rec.get("fee_tokens") or {}
            for tkn, info in (ft or {}).items():
                meta_fee.setdefault(tkn, {})
                meta_fee[tkn][f"{ex}.{al}"] = info

            # Statut comptes WS / Reconciler (optionnel)
            if getattr(self, "_ws_hub_status_provider", None) or getattr(self, "_ws_reco_status_provider", None):
                try:
                    ws_meta = self._get_ws_accounts_status_for_alias(ex, al)
                    if ws_meta:
                        meta_ws_accounts[f"{ex}.{al}"] = ws_meta
                except Exception:
                    # On ne casse jamais le snapshot RM sur erreur de télémétrie WS
                    pass

            t_norm, t_deg, t_blk = self._get_bal_ttls_for_exchange(ex)
            state = self._classify_balance_health(age, t_norm, t_deg, t_blk)
            prev = self._balances_health.get((ex, al), {}).get("state")
            last_change_ts = self._balances_health.get((ex, al), {}).get("last_change_ts", now)
            if prev != state:
                last_change_ts = now
            self._balances_health[(ex, al)] = {
                "state": state,
                "last_age_s": age,
                "ttl_normal_s": t_norm,
                "ttl_degraded_s": t_deg,
                "ttl_block_s": t_blk,
                "last_change_ts": last_change_ts,
            }
            meta_bal_health[f"{ex}.{al}"] = {
                "state": state,
                "age_s": age,
                "ttl_normal_s": t_norm,
                "ttl_degraded_s": t_deg,
                "ttl_block_s": t_blk,
            }
            try:
                BF_BALANCES_HEALTH_STATE.labels(exchange=ex, alias=al).set(
                    0 if state == "NORMAL" else 1 if state == "DEGRADED" else 2
                )
                BF_BALANCES_TTL_NORMAL_SECONDS.labels(exchange=ex).set(t_norm)
                BF_BALANCES_TTL_DEGRADED_SECONDS.labels(exchange=ex).set(t_deg)
                BF_BALANCES_TTL_BLOCK_SECONDS.labels(exchange=ex).set(t_blk)
            except Exception:
                pass

            # Si la vue choisie n'a pas encore de snapshot pour ce couple,
            # on backfill avec les balances "réelles" connues pour garder
            # la rétro-compat (utile en mode cached_only).
            if ex not in balances:
                balances[ex] = {}
            balances[ex].setdefault(al, dict(rec.get("balances") or {}))

        meta = {
            "age_s": meta_age,
            "vip": meta_vip,
            "fee_tokens": meta_fee,
            "ws_accounts": meta_ws_accounts,
            "balances_health": meta_bal_health,
            "balances_freshness": self.get_balances_freshness_status(),
            "view": view,
            "cached_only": bool(cached_only),
            "as_of_ts": time.monotonic(),
        }

        snapshot_payload = {
            "mode": view,
            "balances": balances,
            "meta": meta,
        }
        try:
            return validate_balance_snapshot_rm_lite(snapshot_payload).model_dump()
        except Exception:
            return snapshot_payload


    # =========================== Public helpers ===============================
    async def get_all_wallets(self) -> Dict[str, Any]:
        """Retourne un snapshot léger des wallets connus via flux privés/REST."""
        snap: Dict[str, Dict[str, Dict[str, Dict[str, float]]]] = {}
        expected = sorted(self._expected_wallet_types)
        for ex in ("BINANCE", "COINBASE", "BYBIT"):
            snap.setdefault(ex, {})
            known_aliases = self._known_aliases(ex)
            per_alias = (self.latest_wallets.get(ex) or {})
            for alias in known_aliases:
                per_wallet = {
                    wallet: dict(ccy_map or {})
                    for wallet, ccy_map in ((per_alias.get(alias) or {}).items())
                }
                for wallet in expected:
                    data = per_wallet.setdefault(wallet, {})
                    if not data:
                        self._maybe_log_missing_wallet_snapshot(ex, alias, wallet)
                snap[ex][alias] = per_wallet
        return snap

    async def get_wallets_snapshot(self) -> Dict[str, Any]:
        """Alias historique (utilisé par le Hub)."""
        return await self.get_all_wallets()


    async def get_status(self) -> Dict[str, Any]:
        async with self._cache_lock:
            cached_assets: Dict[str, Dict[str, List[str]]] = {}
            for (ex, al), entry in self._cache.items():
                cached_assets.setdefault(ex, {})[al] = list((entry.get("data") or {}).keys())

            virt_total = {ex: {al: sum((per or {}).values()) for al, per in (per_ex or {}).items()}
                          for ex, per_ex in (self._virt or {}).items()}
            pockets = self.get_pockets_by_quote(mode="real", quotes=("USDC", "EUR", "USD"))
            fee_tok = self.get_fee_token_levels_from_balances(mode="real", tokens=("BNB", "MNT"))

            return {
                "module": "MultiBalanceFetcher",
                "healthy": True,
                "running": self._running,
                "last_sync_time": self.last_sync_time,
                "error_count": self.error_count,
                "successful_syncs": self.successful_syncs,
                "cached_assets": cached_assets,
                "latency_ms": {f"{k[0]}:{k[1]}": v for k, v in self.latency_ms.items()},
                "last_error": self.last_error,
                "binance_time_offset_ms": self._binance_time_offset_ms,
                "overlay_mode": "absolute" if self._virt_mode_set_abs else "delta",
                "overlay_snapshot": self._virt_view_filtered(),
                "overlay_total_by_alias": virt_total,
                # Vues synthèse P0
                "pockets_by_quote": pockets,
                "fee_token_levels": fee_tok,
                "balances_freshness": self.get_balances_freshness_status(),
            }

    # Alias public (nommage canonique) — facilite le branchement depuis le Hub
    def ingest_account_update(
            self,
            exchange: str,
            alias: str,
            delta: Dict[str, float],
            wallet_type: Optional[str] = None,
    ) -> None:
        """
        Compatibilité: même effet que ingest_account_ws_delta (delta par devise).
        """
        return self.ingest_account_ws_delta(exchange, alias, delta, wallet_type=wallet_type)

    def ingest_account_ws_delta(
            self,
            exchange: str,
            alias: str,
            partial_balances: Dict[str, float],
            wallet_type: Optional[str] = None,
    ) -> None:
        """
        Ingestion opportuniste de snapshots venant d'un feed 'account WS'.
        Conserve l'hypothèse conservatrice: on ne décrémente jamais un solde réel
        sans confirmation REST (on ne fait que relever des hausses, et on écrase sur ccy absent).
        """
        exu, alu = exchange.upper(), alias.upper()
        if not partial_balances:
            return

        if any(isinstance(v, dict) for v in partial_balances.values()):
            for wallet, balances in partial_balances.items():
                if isinstance(balances, dict):
                    self.ingest_account_ws_delta(exchange, alias, balances, wallet_type=wallet)
            return

        cur = ((self.latest_balances.get(exu) or {}).get(alu) or {})
        merged = dict(cur)
        filtered: Dict[str, float] = {}
        for ccy, val in (partial_balances or {}).items():
            try:
                filtered[str(ccy).upper()] = float(val)
            except Exception:
                continue
        filtered = self._apply_ccy_filter(filtered)
        for ccy, v in filtered.items():
            if v >= float(cur.get(ccy, 0.0)):
                merged[ccy] = v

        default_wallet = self._default_wallet_type or "SPOT"
        if (wallet_type or default_wallet).upper() == default_wallet:
            self.latest_balances.setdefault(exu, {})[alu] = merged

        per_alias = self.latest_wallets.setdefault(exu, {})
        per_wallet = per_alias.setdefault(alu, {})
        w = (wallet_type or default_wallet).upper()
        per_wallet[w] = dict(filtered)
        self._wallet_missing_log.pop((exu, alu, w), None)


    def prefer_ws_for_keys(self, exchange: str, alias: str, ccys: Tuple[str, ...]) -> None:
        """
        Marque des devises pour lesquelles on accepte d'écraser le REST par le WS
        (ex: tokens de fee ultra-volatils sur account WS).
        """
        exu, alu = exchange.upper(), alias.upper()
        cur = ((self.latest_balances.get(exu) or {}).get(alu) or {})
        # drapeau local; ici on ne persiste pas, on utilise juste ce helper dans ton orchestrateur
        for c in (ccys or ()):
            if c in cur:
                pass  # no-op: la simple présence peut guider l'orchestrateur pour prioriser l'update WS

    def ingest_wallet_snapshot(
            self,
            exchange: str,
            alias: str,
            wallet_type: str,
            balances: Dict[str, float],
    ) -> None:
        """Ingestion explicite d'un snapshot de wallet (via Hub/transfert)."""
        exu, alu = exchange.upper(), alias.upper()
        w = (wallet_type or self._default_wallet_type or "SPOT").upper()
        per_alias = self.latest_wallets.setdefault(exu, {})
        per_wallet = per_alias.setdefault(alu, {})
        per_wallet[w] = {str(k).upper(): float(v) for k, v in (balances or {}).items()}
        self._wallet_missing_log.pop((exu, alu, w), None)


    def observe_fill_fee_reality_check(self, evt: Dict[str, Any]) -> None:
        """Compare les fees rapportés avec les snapshots connus pour déclencher un refresh léger."""
        if not evt:
            return
        exu = _upper(evt.get("exchange"))
        alu = _upper(evt.get("alias"))
        fee_ccy = _upper(evt.get("fee_ccy") or evt.get("fee_currency") or evt.get("fee_asset"))
        fee_paid = evt.get("fee_paid") or evt.get("fee") or evt.get("fee_qty")
        if not (exu and alu and fee_ccy):
            return
        try:
            paid = float(fee_paid or 0.0)
        except Exception:
            paid = 0.0
        rec = self.latest_wallets.setdefault(exu, {}).setdefault(alu, {})
        wallet = rec.get(self._default_wallet_type) or {}
        known = float(wallet.get(fee_ccy, 0.0))
        if paid > known:
            wallet[fee_ccy] = paid
            rec[self._default_wallet_type] = wallet
        try:
            FEE_TOKEN_BALANCE.labels(exu, alu, fee_ccy).set(wallet.get(fee_ccy, 0.0))
        except Exception:
            pass


class PnlRecoCexAdapter:
    """
    Adaptateur PnL pour la réconciliation CEX ↔ DB (M5-D-2 / M5-E-1).

    Rôle:
    - Réutiliser la config et, si possible, les mêmes clients que BalanceFetcher
      pour reconstruire un PnL "jour local" par (exchange, account_alias).
    - Fournir une méthode `fetch_pnl_for_day(...)` compatible avec le runner EOD.

    ⚠️ IMPORTANT:
    - Cette classe N'ENVOIE AUCUN ORDRE.
    - Elle ne fait que lire des trades / fills / PnL auprès des CEX.
    """

    def __init__(self, cfg, balance_fetcher=None):
        """
        Paramètres
        ----------
        cfg:
            BotConfig (ou sous-ensemble) déjà utilisé par le BalanceFetcher.
        balance_fetcher:
            Instance existante de BalanceFetcher (optionnel). Si None,
            l'adaptateur peut en instancier une pour réutiliser ses clients.
            Pour une première version, on peut laisser ce paramètre inutilisé
            et simplement construire ses propres clients par CEX.
        """
        self.cfg = cfg
        self._bf = balance_fetcher  # facultatif: hook si tu veux réutiliser les clients existants

        # TODO (quand tu voudras): initialiser ici les clients REST/HTTP
        # par CEX pour récupérer les trades/fills du jour.

    async def fetch_pnl_for_day(
        self,
        exchange: str,
        account_alias: str,
        local_day: str,
        region: str,
    ):
        """
        Calcule un PnL jour côté CEX pour (exchange,account_alias).

        Contrat de sortie:
            {
                "net_profit_quote": float,
                "fees_quote": float,
                "turnover_quote": float,
                "trades": int,
            }

        Implémentation minimale attendue (à faire quand tu seras prêt):
        - Résoudre la fenêtre temporelle [start_of_day, end_of_day] dans
          la timezone de `region` (EU/US).
        - Appeler les endpoints trades/fills de la CEX pour cet account_alias.
        - Rejouer les trades pour calculer:
            * net_profit_quote (dans la quote canonique, ex. USDC),
            * fees_quote,
            * turnover_quote,
            * trades (compte de fills / executions).
        - Retourner ce dict.

        Pour l'instant, on laisse un NotImplementedError pour ne pas
        inventer de logique CEX-specific ici.
        """
        raise NotImplementedError(
            "PnlRecoCexAdapter.fetch_pnl_for_day() doit être "
            "implémenté pour chaque CEX (Binance/Bybit/Coinbase) en s'appuyant "
            "sur leurs endpoints de trades/fills."
        )
