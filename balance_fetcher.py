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
from typing import Dict, Any, Optional, Tuple, List,Set

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
    )
except Exception:  # pragma: no cover
    class _NoopMetric:
        def labels(self, *_, **__): return self
        def inc(self, *_, **__): pass
        def observe(self, *_, **__): pass
        def set(self, *_, **__): pass

    BF_API_LATENCY_MS = BF_API_ERRORS_TOTAL = BF_CACHE_AGE_SECONDS = BF_LAST_SUCCESS_TS = FEE_TOKEN_BALANCE = _NoopMetric()
    BF_HTTP_LATENCY_SECONDS = BF_HTTP_ERRORS_TOTAL = BF_FEE_TOKEN_LEVEL = BF_FEE_TOKEN_LOW_TOTAL = _NoopMetric()

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

    async def take(self):
        now = time.time()
        refill = (now - self.last) * (self.capacity / 60.0)
        self.tokens = min(self.capacity, self.tokens + refill)
        self.last = now
        if self.tokens < 1.0:
            await asyncio.sleep((1.0 - self.tokens) * (60.0 / self.capacity))
        # pacer clamp (bf_rate)
        try:
            w = float(PACER.clamp("bf_rate"))
            if w > 0.0:
                await asyncio.sleep(w)
        except Exception:
            pass
        self.tokens -= 1.0


def _upper(x: Optional[str]) -> str:
    return (x or "").upper()


class MultiBalanceFetcher:
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
        self._connector = aiohttp.TCPConnector(limit=80, ssl=True, enable_cleanup_closed=True)

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
        self._event_sink = None  # callback optionnel (alerts)
        # Source WS (optionnelle): si un flux "account WS" alimente ces snapshots, on merge conservateur
        self._ws_balances: Dict[Tuple[str,str], Dict[str, Any]] = {}
        self._ws_lock = asyncio.Lock()
        # TTL pour considérer la source WS
        self._ws_ttl_s = getattr_float(self, "WS_BAL_TTL_SECONDS", 10.0)
        # P0: alias de compat pour lecture uniforme des paramètres éventuels
        if not hasattr(self, "cfg"):
            self.cfg = None

    # =========================== Lifecycle ===================================

    async def update_ws_account_balances(self, exchange: str, alias: str, balances: Dict[str, float]) -> None:
        """
        Point d'entrée (optionnel) pour un flux WS 'account'. Le HUB peut l'appeler s'il dispose
        d'une telle source fiable. Merge conservateur côté lecture.
        """
        import time
        key = (exchange.upper(), alias.upper())
        async with self._ws_lock:
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

    async def run_alerts(self) -> None:
        """
        Boucle d'alerting des tokens de frais bas.
        Périodicité par config BF_ALERT_PERIOD_S (def 15s).
        """
        period = getattr_float(self.cfg, "BF_ALERT_PERIOD_S", 15.0)
        low_map = self._get_fee_token_low_map()

        while True:
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
            await asyncio.sleep(max(5.0, period))


    async def start(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout, connector=self._connector)
        if self.use_server_time and not self.dry_run and self.binance_accounts:
            try:
                await self._sync_binance_time_offset()
            except Exception as e:
                logger.warning(f"[Binance] Sync server time échoué: {e}")
        self._running = True

    async def stop(self) -> None:
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None



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
                ws_bal = await self._get_ws_balances_if_fresh("BINANCE", alias)  # adapte "BINANCE"/"COINBASE"/"BYBIT"
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
                ws_bal = await self._get_ws_balances_if_fresh("BINANCE", alias)  # adapte "BINANCE"/"COINBASE"/"BYBIT"
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
                ws_bal = await self._get_ws_balances_if_fresh("BINANCE", alias)  # adapte "BINANCE"/"COINBASE"/"BYBIT"
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

    async def get_balances(self, mode: str = "real", *, force_refresh: bool = False) -> Dict[str, Dict[str, Dict[str, float]]]:
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
        quotes: Tuple[str, ...] = ("USDC", "EUR", "USD"),
        fee_tokens: Tuple[str, ...] = ("BNB", "MNT"),
        mode: str = "real",
    ) -> Dict[str, Any]:
        pockets = self.get_pockets_by_quote(mode=mode, quotes=quotes)
        tokens  = self.get_fee_token_levels_from_balances(mode=mode, tokens=fee_tokens)
        return {"pockets_by_quote": pockets, "fee_token_levels": tokens}

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
    def set_event_sink(self, sink) -> None:
        self._event_sink = sink

    async def _record_snapshot(self, exchange: str, alias: str, balances: Dict[str, float]) -> None:
        ex, al = _upper(exchange), _upper(alias)
        now = time.time()
        rec = self._snap.get((ex, al), {"balances": {}, "fee_tokens": {}, "vip": {}, "vip_ts": 0.0})
        rec["balances"] = dict(balances or {})
        rec["ts"] = now

        # fee token levels with watermarks
        low = getattr_float(self.cfg, "FEE_LOW_WATERMARK", 15.0) if self.cfg is not None else 15.0
        high = getattr_float(self.cfg, "FEE_HIGH_WATERMARK", 40.0) if self.cfg is not None else 40.0
        ft = rec.get("fee_tokens") or {}
        for tkn in ("BNB", "MNT"):
            bal = float(rec["balances"].get(tkn, 0.0))
            ft[tkn] = {"bal": bal, "low": low, "high": high, "is_low": bal < low}
            try:
                FEE_TOKEN_BALANCE.labels(ex, al, tkn).set(bal)
            except Exception:
                pass
        rec["fee_tokens"] = ft

        self._snap[(ex, al)] = rec
        try:
            BF_CACHE_AGE_SECONDS.labels(ex, al).set(0.0)
            BF_LAST_SUCCESS_TS.labels(ex, al).set(now)
        except Exception:
            pass

    def as_rm_snapshot(self, *, mode: str = "real", cached_only: bool = False) -> Dict[str, Any]:
        """Retourne un snapshot prêt pour le RiskManager.

        Le paramètre ``mode`` est aligné sur ``get_balances_snapshot`` et permet
        enfin de distinguer les vues ``real``/``virtual``/``merged``. Le
        ``cached_only`` ne déclenche aucune I/O supplémentaire : si aucune donnée
        n’est disponible on renvoie simplement les métadonnées (âges infinis).
        """

        view = (mode or "real").lower()
        if view not in ("real", "virtual", "merged"):
            view = "real"

        balances = self.get_balances_snapshot(view)
        now = time.time()
        meta_age: Dict[str, float] = {}
        meta_vip: Dict[str, Any] = {}
        meta_fee: Dict[str, Dict[str, Any]] = {}

        # Copie des balances (pour éviter toute mutation externe)
        out: Dict[str, Any] = {ex: {al: dict(ccy_map or {}) for al, ccy_map in (per or {}).items()}
                               for ex, per in (balances or {}).items()}

        for (ex, al), rec in list(self._snap.items()):
            ts = float(rec.get("ts", 0.0))
            age = max(0.0, now - ts) if ts > 0.0 else float("inf")
            meta_age[f"{ex}.{al}"] = age

            vip = rec.get("vip") or {}
            if vip:
                meta_vip[f"{ex}.{al}"] = vip

            ft = rec.get("fee_tokens") or {}
            for tkn, info in ft.items():
                meta_fee.setdefault(tkn, {})
                meta_fee[tkn][f"{ex}.{al}"] = info

            # Si la vue choisie n'a pas encore de snapshot pour ce couple,
            # expose quand même les balances réelles connues pour garder la
            # rétro-compatibilité (utile en mode cached_only).
            out.setdefault(ex, {}).setdefault(al, dict(rec.get("balances") or {}))

        out["meta"] = {
            "age_s": meta_age,
            "vip": meta_vip,
            "fee_tokens": meta_fee,
            "view": view,
            "cached_only": bool(cached_only),
        }
        return out

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