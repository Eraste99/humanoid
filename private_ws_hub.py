# -*- coding: utf-8 -*-
from __future__ import annotations
"""
PrivateWSHub — tri-CEX, multi-alias (TT/TM/MM)
==============================================

Rôle
----
• Centralise les événements privés **orders + fills** par (exchange, alias).
• Fournit des **passerelles de transferts** internes pour le RiskManager:
  - Transferts intra-wallet (FUNDING ↔ SPOT...) via `transfer_wallet(...)`
  - Transferts intra-sous-compte (TT ↔ TM) via `transfer_subaccount(...)`

Intégration transferts
----------------------
Passez un dict `transfer_clients={"BINANCE": binance_client, "BYBIT": bybit_client, ...}`
au constructeur OU après coup via `connect_transfer_clients(...)`.

Clients attendus (sync ou async) :
    transfer_wallet(ex, alias, from_wallet, to_wallet, ccy, amount)
    transfer_subaccount(ex, from_alias, to_alias, ccy, amount)
    (optionnels) get_wallets_snapshot() | get_all_wallets()

Événements émis via callback (register_callback) :
  - "order"   : mise à jour d’ordre
  - "fill"    : exécutions (patch: `quote`, `quote_qty`, `usdc_qty` ajoutés)
  - "transfer": résultat d’un transfert (OK/ERROR)
      {
        "type": "transfer",
        "subtype": "wallet" | "subaccount",
        "exchange": "BINANCE"|"BYBIT"|"COINBASE",
        "alias": "TT"|"TM"|None,
        "status": "OK"|"ERROR",
        "payload": {...}, "result": {...}|None, "error": "...",
        "ts": 1700000000.0
      }

API
---
  hub = PrivateWSHub(..., transfer_clients=..., config=BotConfigOrNone)
  hub.register_callback(cb)         # l'ancien cb est promu en cb RM (double émission)
  await hub.start() / await hub.stop()

  # Transferts
  await hub.transfer_wallet(ex, alias, from_wallet, to_wallet, ccy, amount)
  await hub.transfer_subaccount(ex, from_alias, to_alias, ccy, amount)

  # Snapshots optionnels de wallets (si clients les exposent)
  await hub.get_wallets_snapshot()

Statut
------
  hub.get_status()
"""

from dataclasses import dataclass
from collections import OrderedDict, defaultdict, deque
from typing import Callable, Optional, Any, Dict, Tuple, List
import asyncio
import aiohttp
import base64
import hashlib
import hmac
import inspect
import json
import logging
import random
import time
import asyncio as _asyncio
from modules.rm_compat import getattr_int, getattr_float, getattr_str, getattr_bool, getattr_dict, getattr_list

# --- Prometheus metrics (fallback no-op si registre absent) -------------------
try:
    from modules.obs_metrics import (
        PWS_DEDUP_HITS_TOTAL,
        PWS_RECONNECTS_TOTAL,
        PWS_EVENT_LAG_MS,
        PWS_TRANSFERS_TOTAL,
        PWS_EVENTS_TOTAL,
        PWS_BACKOFF_SECONDS,
        PWS_HEARTBEAT_GAP_SECONDS,
        PWS_DROPPED_TOTAL,
        PWS_ACK_LATENCY_MS,
        PWS_FILL_LATENCY_MS,
        # AJOUTER dans la liste existante :
        WS_FAILOVER_TOTAL, PWS_POOL_SIZE,

    )
except Exception:  # pragma: no cover
    class _NoopMetric:
        def labels(self, *_, **__): return self
        def inc(self, *_, **__): pass
        def observe(self, *_, **__): pass
        def set(self, *_, **__): pass
    PWS_DEDUP_HITS_TOTAL = _NoopMetric()
    PWS_RECONNECTS_TOTAL = _NoopMetric()
    PWS_EVENT_LAG_MS = _NoopMetric()
    PWS_TRANSFERS_TOTAL = _NoopMetric()
    PWS_EVENTS_TOTAL = _NoopMetric()
    PWS_BACKOFF_SECONDS = _NoopMetric()
    PWS_HEARTBEAT_GAP_SECONDS = _NoopMetric()
    PWS_DROPPED_TOTAL = _NoopMetric()
    PWS_ACK_LATENCY_MS = _NoopMetric()
    PWS_FILL_LATENCY_MS = _NoopMetric()

# === Imports métriques (fallback robuste) =====================================
try:
    from modules.obs_metrics import Counter, Gauge, Histogram
except Exception:
    try:
        from prometheus_client import Counter, Gauge, Histogram  # fallback direct
    except Exception:
        class _NoopMetric:
            def labels(self, *_, **__): return self
            def inc(self, *_, **__):  return None
            def observe(self, *_, **__): return None
            def set(self, *_, **__):  return None
        Counter = Gauge = Histogram = _NoopMetric  # no-op si Prometheus absent

# --- Import direct des métriques centrales (compat si absentes) ---------------
try:
    from modules.obs_metrics import PWS_QUEUE_DEPTH, PWS_QUEUE_CAP, PWS_DEDUP_HITS_TOTAL
except Exception:
    # Compat: si obs_metrics indisponible ou métriques non exportées
    try:
        PWS_QUEUE_DEPTH = Gauge("pws_queue_depth", "PrivateWS submit queue depth", ["exchange","alias","kind"])
        PWS_QUEUE_CAP   = Gauge("pws_queue_cap",   "PrivateWS submit queue capacity", ["exchange","alias","kind"])
    except Exception:
        class _NoopMetric:
            def labels(self, *_, **__): return self
            def set(self, *_, **__):    return None
        PWS_QUEUE_DEPTH = _NoopMetric()
        PWS_QUEUE_CAP   = _NoopMetric()
    # PWS_DEDUP_HITS_TOTAL est déjà initialisé plus haut à _NoopMetric(), il sera utilisé si import KO.


# === Metrics Alerting (Hub) ===================================================
try:
    PWS_QUEUE_FILL_RATIO = Gauge(
        "pws_queue_fill_ratio",
        "Ratio de remplissage des files privées (0..1)",
        ["exchange", "alias", "kind"],
    )
    PWS_QUEUE_SATURATION_TOTAL = Counter(
        "pws_queue_saturation_total",
        "Saturation détectée sur une file privée (>= seuil)",
        ["exchange", "alias", "kind"],
    )
    PWS_HEARTBEAT_GAP_BREACH_TOTAL = Counter(
        "pws_heartbeat_gap_breach_total",
        "Nombre de fois où le gap heartbeat hub a dépassé le seuil",
        ["exchange", "alias"],
    )
    PWS_ALERT_TOTAL = Counter(
        "pws_alert_total",
        "Alertes émises par le PrivateWSHub",
        ["severity", "reason", "exchange", "alias", "kind"],
    )
except Exception:
    # no-op si déjà enregistrées
    pass

log = logging.getLogger("PrivateWSHub")
log.setLevel(logging.INFO)

# Pacer global (clamps dynamiques)
try:
    from modules.observability_pacer import PACER
except Exception:  # pragma: no cover
    class _P0Pacer:
        def clamp(self, *_a, **_k): return 0.0
        def state(self): return "NORMAL"
    PACER = _P0Pacer()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> float:
    return time.time()

def _upper(x: Optional[str]) -> str:
    return (x or "").upper()

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def _quote_ccy_from_symbol(sym: str) -> str:
    s = (sym or "").replace("-", "").upper()
    for q in ("USDC", "EUR", "USD", "USDT"):
        if s.endswith(q):
            return q
    return ""

def _observe_event_lag_ms(exchange: str, alias: str, ts_ms: Optional[float]) -> None:
    """Observe le lag (ms) entre l'horodatage serveur et maintenant."""
    try:
        if ts_ms is None:
            return
        t = float(ts_ms)
        if t <= 0:
            return
        if t < 3e10:  # timestamp probablement en secondes
            t *= 1000.0
        now_ms = _now() * 1000.0
        lag = max(0.0, now_ms - t)
        PWS_EVENT_LAG_MS.labels(exchange=_upper(exchange), alias=_upper(alias)).observe(lag)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Keys / accounts (structure indicative)
# ---------------------------------------------------------------------------

@dataclass
class SubAccountKeys:
    api_key: str
    secret: str
    passphrase: Optional[str] = None  # non utilisé ici


# ---------------------------------------------------------------------------
# Base WS client
# ---------------------------------------------------------------------------

class _BaseWSClient:
    def __init__(self, cfg, exchange: str, alias: str):
        """
        WS privé (orders/acks) piloté par cfg.pws.*
        - région par exchange, tailles de files, heartbeats, backoff…
        - zéro os.getenv ; tout vient de la config.
        """
        import asyncio
        from typing import Optional, Callable
        import aiohttp

        self.cfg = cfg
        self.exchange = str(exchange).upper()
        self.alias = str(alias).upper()

        # --- état runtime / wiring bas niveau ---
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._task: Optional[asyncio.Task] = None
        self._callback: Optional[Callable[[dict], None]] = None
        self._rm_callback: Optional[Callable[[dict], None]] = None
        self._running = False
        self._last_event_ts: float = 0.0
        self._reconnects: int = 0
        self._errors: int = 0

        # --- cfg.pws (source de vérité) ---
        pws = self.cfg.pws
        self._region_map  = dict(getattr(pws, "PWS_REGION_MAP", {"BINANCE":"EU","BYBIT":"EU","COINBASE":"US"}))
        self._queue_max   = getattr_int(pws, "PWS_QUEUE_MAXLEN", 10_000)
        self._sat_ratio   = getattr_float(pws, "PWS_QUEUE_SATURATION_RATIO", 0.90)
        self._ping_s      = getattr_int(pws, "PWS_PING_INTERVAL_S", 20)
        self._pong_to_s   = getattr_int(pws, "PWS_PONG_TIMEOUT_S", 10)
        self._hb_max_gap  = getattr_int(pws, "PWS_HEARTBEAT_MAX_GAP_S", 30)
        self._stable_reset_s = getattr_int(pws, "PWS_STABLE_RESET_S", 3600)
        self._jitter_ms   = getattr_int(pws, "PWS_JITTER_MS", 50)
        self._bo_base_ms  = getattr_int(pws, "PWS_BACKOFF_BASE_MS", 500)
        self._bo_max_ms   = getattr_int(pws, "PWS_BACKOFF_MAX_MS", 15_000)

        # dérivés pratiques
        self._bo_base_s = self._bo_base_ms / 1000.0
        self._bo_max_s  = self._bo_max_ms / 1000.0

        # --- garde DRY_RUN/PROD ---
        if self.cfg.g.mode == "DRY_RUN" and self.cfg.g.feature_switches.get("private_ws", False):
            raise RuntimeError("DRY_RUN: private_ws must be OFF")

    def register_callback(self, cb):
        """
        Enregistre le callback principal (Engine) et conserve le précédent
        comme callback RM si non déjà fixé. Permet double émission RM→Engine.
        """
        prev = getattr(self, "_callback", None)
        if self._rm_callback is None and prev is not None and prev is not cb:
            self._rm_callback = prev
        self._callback = cb

    async def start(self):
        import aiohttp, asyncio
        if self._running:
            return
        self._running = True
        self._session = aiohttp.ClientSession(trust_env=True)
        # TODO: ouvrir la bonne URL selon self._region_map[self.exchange]
        self._task = asyncio.create_task(self._run())


    async def stop(self) -> None:
        # P0: cancel heartbeat task si présent
        hb = getattr(self, "_hb_task", None)
        if hb and not hb.done():
            hb.cancel()
            try:
                await hb
            except Exception:
                pass

        self._running = False
        if self._task:
            self._task.cancel()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._session and not self._session.closed:
            await self._session.close()
        self._ws = None
        self._session = None

    def _emit(self, ev: dict) -> None:
        self._last_event_ts = _now()
        # 1) callback RM (si présent) — non bloquant si coroutine
        rm_cb = getattr(self, "_rm_callback", None)
        if rm_cb:
            try:
                import inspect, asyncio
                if inspect.iscoroutinefunction(rm_cb):
                    asyncio.create_task(rm_cb(ev))
                else:
                    rm_cb(ev)
            except Exception:
                log.exception("[%s:%s] RM callback error", self.exchange, self.alias)
        # 2) callback Engine (principal)
        if self._callback:
            try:
                self._callback(ev)
            except Exception:
                log.exception("[%s:%s] callback error", self.exchange, self.alias)

    def get_status(self) -> Dict[str, Any]:
        return {
            "exchange": self.exchange,
            "alias": self.alias,
            "healthy": self._running and (self._ws is not None),
            "last_event_ts": self._last_event_ts,
            "reconnects": self._reconnects,
            "errors": self._errors,
        }


# ---------------------------------------------------------------------------
# Binance UserStream (listenKey)
# ---------------------------------------------------------------------------

class BinanceUserStream(_BaseWSClient):
    def __init__(self, alias: str, api_key: str,
                 rest_base: str = "https://api.binance.com",
                 ws_base: str = "wss://stream.binance.com:9443"):
        super().__init__("BINANCE", alias)
        self.api_key = api_key
        self._ws_idx = 0
        self.rest_base = rest_base
        self._ws_bases = [ws_base] if isinstance(ws_base, str) else list(ws_base or [])
        if not self._ws_bases:
            self._ws_bases = ["wss://stream.binance.com:9443"]
        self._ws_ix = 0
        self._listen_key: Optional[str] = None
        self._keepalive_task: Optional[asyncio.Task] = None

    async def _create_or_refresh_listen_key(self) -> None:
        assert self._session
        headers = {"X-MBX-APIKEY": self.api_key}
        if not self._listen_key:
            url = f"{self.rest_base}/api/v3/userDataStream"
            async with self._session.post(url, headers=headers, timeout=10) as resp:
                resp.raise_for_status()
                self._listen_key = (await resp.json())["listenKey"]
        else:
            url = f"{self.rest_base}/api/v3/userDataStream?listenKey={self._listen_key}"
            async with self._session.put(url, headers=headers, timeout=10) as resp:
                resp.raise_for_status()

    @staticmethod
    def _map_status(x: str) -> str:
        m = {
            "NEW": "ACK",
            "PARTIALLY_FILLED": "PARTIAL",
            "FILLED": "FILLED",
            "CANCELED": "CANCELED",
            "REJECTED": "REJECTED",
            "PENDING_CANCEL": "ACK",
            "EXPIRED": "CANCELED",
        }
        return m.get((x or "").upper(), "ACK")

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._session = aiohttp.ClientSession(trust_env=True)
        await self._create_or_refresh_listen_key()
        self._task = asyncio.create_task(self._run_ws())
        self._keepalive_task = asyncio.create_task(self._ka_loop())
        log.info("[BINANCE:%s] user stream started", self.alias)

    async def stop(self) -> None:
        if self._keepalive_task:
            self._keepalive_task.cancel()
        await super().stop()

    async def _ka_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(20 * 60)
                try:
                    await self._create_or_refresh_listen_key()
                except Exception:
                    log.exception("[BINANCE:%s] keepalive failed", self.alias)
        except asyncio.CancelledError:
            pass

    def _handle_exec_report(self, msg: dict) -> None:
        # ts serveur (ms) -> observe lag + latences
        try:
            server_ts_ms = float(msg.get("E") or msg.get("T") or 0) or None
        except Exception:
            server_ts_ms = None
        _observe_event_lag_ms("BINANCE", self.alias, server_ts_ms)
        ts_ex = (server_ts_ms / 1000.0) if server_ts_ms else None
        ts_loc = _now()

        status_raw = str(msg.get("X", "")).upper()
        status = self._map_status(status_raw)
        symbol = str(msg.get("s", "")).upper()
        side = str(msg.get("S", "")).upper()
        client_id = (str(msg.get("c")) if msg.get("c") else None)
        ex_order_id = (str(msg.get("i")) if msg.get("i") else None)
        price = _safe_float(msg.get("p"))
        filled_cum = _safe_float(msg.get("z"))

        # ORDER

        self._emit({
            "type": "order",
            "exchange": "BINANCE",
            "alias": self.alias,
            "client_id": client_id,
            "exchange_order_id": ex_order_id,
            "status": status,
            "filled_qty": filled_cum,
            "symbol": symbol,
            "side": side,
            "price": price,
            "reason": str(msg.get("r") or msg.get("x") or ""),
            "ts_exchange": ts_ex,
            "ts_local": ts_loc,
            "ts": ts_loc,
        })

        # FILL (delta)
        if status_raw in ("PARTIALLY_FILLED", "FILLED"):
            last_px = _safe_float(msg.get("L"))
            last_qty = _safe_float(msg.get("l"))
            if last_px > 0 and last_qty > 0:
                quote = _quote_ccy_from_symbol(symbol)
                quote_qty = last_px * last_qty
                usdc_qty = quote_qty if quote == "USDC" else 0.0
                expected_px = price or last_px
                self._emit({
                    "type": "fill",
                    "status": "FILL",
                    "exchange": "BINANCE",
                    "alias": self.alias,
                    "symbol": symbol,
                    "side": side,
                    "expected_px": expected_px,
                    "fill_px": last_px,
                    "base_qty": last_qty,
                    "quote": quote,
                    "quote_qty": quote_qty,
                    "usdc_qty": usdc_qty,
                    "client_id": client_id,
                    "exchange_order_id": ex_order_id,
                    "ts_exchange": ts_ex,
                    "ts_local": ts_loc,
                    "ts": ts_loc,
                })

    async def _run_ws(self) -> None:
        assert self._session and self._listen_key
        backoff = 1.0
        while self._running:
            base = self._ws_bases[self._ws_ix % len(self._ws_bases)]
            url = f"{base}/ws/{self._listen_key}"
            try:
                async with self._session.ws_connect(url, heartbeat=30) as ws:
                    self._ws = ws
                    log.info("[BINANCE:%s] WS connected (%s)", self.alias, base)
                    backoff = 1.0
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                js = json.loads(msg.data)
                                if js.get("e") == "executionReport":
                                    self._handle_exec_report(js)
                            except Exception:
                                log.exception("[BINANCE:%s] parse error", self.alias)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            raise RuntimeError("WS error")
            except asyncio.CancelledError:
                break
            except Exception:
                self._errors += 1
                self._reconnects += 1
                # rotation endpoint (failover)
                try:
                    old = self._ws_bases[self._ws_ix % len(self._ws_bases)]
                    self._ws_ix = (self._ws_ix + 1) % max(1, len(self._ws_bases))
                    new = self._ws_bases[self._ws_ix % len(self._ws_bases)]
                    if new != old:
                        WS_FAILOVER_TOTAL.labels(exchange="BINANCE", alias=self.alias, endpoint=new).inc()
                except Exception:
                    pass
                # backoff jitteré
                delay = min(30.0, backoff * (0.3 + 0.7 * random.random()))
                try:
                    PWS_RECONNECTS_TOTAL.labels(exchange="BINANCE", alias=self.alias).inc()
                    log.info("[PrivateWSHub] %s", json.dumps({
                        "pws_reconnect": True, "exchange": "BINANCE",
                        "alias": self.alias, "delay_s": round(delay, 2), "endpoint": base
                    }))
                except Exception:
                    pass
                log.exception("[BINANCE:%s] reconnect in %.2fs (next endpoint idx=%d)", self.alias, delay, self._ws_ix)
                await asyncio.sleep(delay)
                backoff = min(backoff * 2, 30.0)


# ---------------------------------------------------------------------------
# Bybit v5 private WS (order topic)
# ---------------------------------------------------------------------------

class BybitPrivateWS(_BaseWSClient):
    """
    Client WS privé Bybit — multi-endpoints + failover rotatif.
    Émet les events "order" → callback Hub (ACK/PARTIAL/FILLED/etc.)
    """
    def __init__(self, alias: str, api_key: str, secret: str,
                 ws_url: str | None = "wss://stream.bybit.com/v5/private",
                 ws_urls: List[str] | None = None):
        super().__init__("BYBIT", alias)
        self.api_key = api_key
        self.secret = secret.encode()

        # Multi-endpoints: priorise ws_urls, sinon dérive de ws_url
        if ws_urls and isinstance(ws_urls, (list, tuple)) and len(ws_urls) > 0:
            self._ws_urls = list(ws_urls)
        else:
            self._ws_urls = [ws_url] if isinstance(ws_url, str) else ["wss://stream.bybit.com/v5/private"]
        self._ws_ix = 0  # index courant de l'endpoint

    @staticmethod
    def _sign(ts_ms: int, api_key: str, secret: bytes) -> str:
        recv_window = "5000"
        raw = f"{ts_ms}{api_key}{recv_window}".encode()
        return hmac.new(secret, raw, hashlib.sha256).hexdigest()

    @staticmethod
    def _map_status(x: str) -> str:
        m = {
            "NEW": "ACK",
            "CREATED": "ACK",
            "PARTIALLY_FILLED": "PARTIAL",
            "PARTIALLYFILLED": "PARTIAL",
            "FILLED": "FILLED",
            "CANCELED": "CANCELED",
            "CANCELLED": "CANCELED",
            "REJECTED": "REJECTED",
            "PARTIALLY_CANCELED": "PARTIAL",
            "EXPIRED": "CANCELED",
        }
        return m.get((x or "").upper(), "ACK")

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._session = aiohttp.ClientSession(trust_env=True)
        self._task = asyncio.create_task(self._run_ws())
        log.info("[BYBIT:%s] private ws started", self.alias)

    async def _run_ws(self) -> None:
        assert self._session
        backoff_s = 1.0
        while self._running:
            base = self._ws_urls[self._ws_ix % len(self._ws_urls)]
            try:
                async with self._session.ws_connect(base, heartbeat=20) as ws:
                    self._ws = ws
                    ts_ms = int(_now() * 1000)
                    sign = self._sign(ts_ms, self.api_key, self.secret)
                    await ws.send_json({"op": "auth", "args": [self.api_key, ts_ms, sign, "5000"]})
                    await ws.send_json({"op": "subscribe", "args": ["order"]})
                    log.info("[BYBIT:%s] WS connected & subscribed (%s)", self.alias, base)
                    backoff_s = 1.0

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                js = json.loads(msg.data)
                                if js.get("op") in ("auth", "subscribe"):
                                    continue
                                if str(js.get("topic", "")).startswith("order"):
                                    data = js.get("data") or []
                                    if isinstance(data, dict):
                                        data = [data]
                                    for row in data:
                                        try:
                                            status = self._map_status(row.get("orderStatus") or row.get("o", ""))
                                            order_id = row.get("orderId") or row.get("i")
                                            client_id = row.get("orderLinkId") or row.get("c")
                                            symbol = row.get("symbol") or row.get("s")
                                            side = (row.get("side") or row.get("S") or "").upper()
                                            price = _safe_float(row.get("price") or row.get("p"))
                                            filled_cum = _safe_float(row.get("cumExecQty") or row.get("z"))
                                            ts_ex = _safe_float(row.get("updatedTime") or row.get("T"))
                                            ts_loc = _now()
                                            self._emit({
                                                "type": "order",
                                                "status": status,
                                                "exchange": "BYBIT",
                                                "alias": self.alias,
                                                "orderId": order_id,
                                                "clientId": client_id,
                                                "symbol": symbol,
                                                "side": side,
                                                "price": price,
                                                "filled_qty": filled_cum,
                                                "ts_exchange": ts_ex,
                                                "ts_local": ts_loc,
                                                "ts": ts_loc,
                                            })
                                        except Exception:
                                            continue
                            except Exception:
                                continue
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                self._ws = None  # socket fermé proprement → on repart
            except asyncio.CancelledError:
                return
            except Exception:
                # failover: endpoint suivant + métrique
                try:
                    WS_FAILOVER_TOTAL.labels("BYBIT", self.alias).inc()
                except Exception:
                    pass
                self._ws_ix = (self._ws_ix + 1) % len(self._ws_urls)

            try:
                await asyncio.sleep(backoff_s)
            except asyncio.CancelledError:
                return
            backoff_s = min(backoff_s * 2.0, 30.0)

# ---------------------------------------------------------------------------
# Coinbase Advanced Trade: poller (orders + fills)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------
# Coinbase Advanced Trade: lightweight WS "orders" ACK (optional)
# ---------------------------------------------------------------------
class CoinbaseOrdersWS(_BaseWSClient):
    """
    Client WS minimal pour ACK "orders" (fills conservés via poller REST).
    Auth pluggable via `auth_msg_factory` (callable ts_ms -> dict message).
    """
    def __init__(self, alias: str, *, ws_urls=None, auth_msg_factory=None, subscribe_payload=None):
        super().__init__("COINBASE", alias)
        self.ws_urls = list(ws_urls or ["wss://advanced-trade-ws.coinbase.com"])
        self._ws_idx = 0
        self._auth_msg_factory = auth_msg_factory    # ex: lambda ts: {...}
        # Par défaut on cible un canal "orders" (adapter selon ton infra)
        self._subscribe_payload = subscribe_payload or {"type":"subscribe","channel":"orders"}

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._session = aiohttp.ClientSession(trust_env=True)
        self._task = asyncio.create_task(self._run_ws())
        log.info("[COINBASE:%s] orders-ack ws started", self.alias)

    async def _run_ws(self) -> None:
        assert self._session
        backoff = 1.0
        while self._running:
            try:
                base = self.ws_urls[self._ws_idx % len(self.ws_urls)]
                async with self._session.ws_connect(base, heartbeat=20) as ws:
                    self._ws = ws
                    # auth si fournie
                    try:
                        import time
                        if callable(self._auth_msg_factory):
                            ts = int(time.time()*1000)
                            await ws.send_json(self._auth_msg_factory(ts))
                    except Exception:
                        log.warning("[COINBASE:%s] auth_msg_factory failed; continuing unauth", self.alias)
                    # subscribe
                    await ws.send_json(self._subscribe_payload)
                    log.info("[COINBASE:%s] WS connected & subscribed (ACK only)", self.alias)
                    backoff = 1.0
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                js = json.loads(msg.data)
                                # Adapter à ton format réel: on mappe un ACK minimal si on voit un ordre
                                row = js.get("order") or js.get("data") or js
                                if row and isinstance(row, dict):
                                    symbol = str(row.get("product_id") or row.get("symbol") or "").replace("-","").upper()
                                    st = str(row.get("status") or row.get("order_status") or "ACK").upper()
                                    status = "ACK" if st not in ("REJECTED","CANCELED","FILLED","PARTIALLY_FILLED") else st
                                    client_id = row.get("client_order_id") or row.get("clientOrderId")
                                    ex_order_id = row.get("order_id") or row.get("orderId")
                                    self._emit({
                                        "type": "order",
                                        "exchange": "COINBASE",
                                        "alias": self.alias,
                                        "client_id": client_id,
                                        "exchange_order_id": ex_order_id,
                                        "status": status,
                                        "symbol": symbol,
                                        "ts": _now(),
                                        "meta": {"source":"ws"},
                                    })
                            except Exception:
                                log.exception("[COINBASE:%s] parse error", self.alias)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            raise RuntimeError("WS error")
            except asyncio.CancelledError:
                break
            except Exception:
                self._errors += 1
                self._reconnects += 1
                # failover endpoint
                try:
                    old = self.ws_urls[self._ws_idx % len(self.ws_urls)]
                    self._ws_idx = (self._ws_idx + 1) % max(1, len(self.ws_urls))
                    new = self.ws_urls[self._ws_idx % len(self.ws_urls)]
                    if new != old:
                        WS_FAILOVER_TOTAL.labels(exchange="COINBASE", alias=self.alias, endpoint=new).inc()
                except Exception:
                    pass
                delay = min(30.0, backoff * (0.3 + 0.7 * random.random()))
                PWS_RECONNECTS_TOTAL.labels(exchange="COINBASE", alias=self.alias).inc()
                log.exception("[COINBASE:%s] reconnect in %.2fs", self.alias, delay)
                await asyncio.sleep(delay)
                backoff = min(backoff * 2, 30.0)



class CoinbaseATPoller:
    """
    Poll REST orders + fills (pas de WS 'orders' stable côté AT).
    - Émet seulement les changements d'état pour les ORDERS.
    - Émet des FILL events normalisés.
    (signature HMAC avec secret base64-décodé)
    """

    def __init__(self, alias: str, api_key: str, secret: str, *,
                 api_base: str = "https://api.coinbase.com",
                 interval_s: float = 1.5):
        self.alias = _upper(alias)
        self.api_key = api_key
        self._secret_raw = secret  # base64-encoded secret (AT)
        self.api_base = api_base
        self.interval_s = float(interval_s)
        self._seen_fills: set[str] = set()
        self._seen_fills_max: int = 5000
        self._session: Optional[aiohttp.ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self._callback: Optional[Callable[[dict], None]] = None
        self._rm_callback: Optional[Callable[[dict], None]] = None
        self._running = False
        self._last_status: Dict[str, str] = {}  # order_id -> status
        self._last_event_ts: float = 0.0
        self._errors: int = 0

    def register_callback(self, cb):
        prev = getattr(self, "_callback", None)
        if self._rm_callback is None and prev is not None and prev is not cb:
            self._rm_callback = prev
        self._callback = cb

    def _emit(self, ev: dict) -> None:
        self._last_event_ts = _now()
        # RM d'abord (si existant)
        rm_cb = getattr(self, "_rm_callback", None)
        if rm_cb:
            try:
                import inspect, asyncio
                if inspect.iscoroutinefunction(rm_cb):
                    asyncio.create_task(rm_cb(ev))
                else:
                    rm_cb(ev)
            except Exception:
                log.exception("[CoinbaseATPoller:%s] RM callback error", self.alias)
        # Engine ensuite
        if self._callback:
            try:
                self._callback(ev)
            except Exception:
                log.exception("[CoinbaseATPoller] callback error")

    @staticmethod
    def _b64_decode_secret(s: str) -> bytes:
        try:
            return base64.b64decode(s)
        except Exception:
            return s.encode()

    def _extract_ts_ms(self, row: dict) -> Optional[float]:
        candidates = ("ts", "trade_time", "time", "created_time", "completion_time")
        for k in candidates:
            v = row.get(k)
            if v is None:
                continue
            if isinstance(v, (int, float)):
                try:
                    return float(v) if float(v) > 3e10 else float(v) * 1000.0
                except Exception:
                    continue
            if isinstance(v, str):
                try:
                    from datetime import datetime
                    iso = v.replace("Z", "+00:00") if v.endswith("Z") else v
                    return datetime.fromisoformat(iso).timestamp() * 1000.0
                except Exception:
                    continue
        return None

    def _sign(self, ts: str, method: str, path: str, body: str = "") -> str:
        prehash = f"{ts}{method}{path}{body}".encode()
        secret_bytes = self._b64_decode_secret(self._secret_raw)
        mac = hmac.new(secret_bytes, prehash, hashlib.sha256).digest()
        return base64.b64encode(mac).decode()

    async def _get_json(self, method: str, path: str, body: str = "") -> Any:
        assert self._session
        url = f"{self.api_base}{path}"
        ts = str(int(time.time()))
        sign = self._sign(ts, method, path, body)
        headers = {
            "CB-ACCESS-KEY": self.api_key,
            "CB-ACCESS-SIGN": sign,
            "CB-ACCESS-TIMESTAMP": ts,
            "Content-Type": "application/json",
        }
        async with self._session.request(method, url, headers=headers, data=body or None, timeout=10) as resp:
            txt = await resp.text()
            resp.raise_for_status()
            try:
                return json.loads(txt)
            except Exception:
                return {"raw": txt}

    @staticmethod
    def _map_status(x: str) -> str:
        s = (x or "").lower()
        if s in ("open", "pending"): return "ACK"
        if s in ("filled", "done"): return "FILLED"
        if s in ("partially_filled", "partial"): return "PARTIAL"
        if s in ("cancelled", "canceled"): return "CANCELED"
        if s in ("rejected", "failed"): return "REJECTED"
        return "ACK"

    def _emit_order(self, row: dict) -> None:
        oid = str(row.get("order_id") or row.get("orderId") or "")
        if not oid:
            return
        ts_ms = self._extract_ts_ms(row)
        _observe_event_lag_ms("COINBASE", self.alias, ts_ms)
        ts_ex = (ts_ms / 1000.0) if ts_ms else None
        ts_loc = _now()

        status = self._map_status(row.get("status") or row.get("order_status") or "")
        ev = {
            "type": "order",
            "exchange": "COINBASE",
            "alias": self.alias,
            "client_id": row.get("client_order_id") or row.get("clientOrderId"),
            "exchange_order_id": oid,
            "status": status,
            "filled_qty": _safe_float(row.get("filled_quantity") or row.get("filled_size")),
            "reason": row.get("reject_reason", "") or row.get("error", ""),
            "symbol": (row.get("product_id") or row.get("symbol") or "").replace("-", "").upper(),
            "side": _upper(row.get("side")),
            "price": _safe_float(row.get("price")),
            "ts_exchange": ts_ex,
            "ts_local": ts_loc,
            "ts": ts_loc,
        }
        ev["meta"] = {"source": "poller"}
        # (dans _emit_fill, même chose sur l'event avant self._emit(ev))


        self._emit(ev)

    def _emit_fill(self, row: dict) -> None:
        fid = str(row.get("trade_id") or row.get("tradeId") or row.get("sequence") or "")
        if fid and fid in self._seen_fills:
            return

        ts_ms = self._extract_ts_ms(row)
        _observe_event_lag_ms("COINBASE", self.alias, ts_ms)
        ts_ex = (ts_ms / 1000.0) if ts_ms else None
        ts_loc = _now()

        symbol_raw = (row.get("product_id") or row.get("symbol") or "")
        symbol = symbol_raw.replace("-", "").upper()
        fill_px = _safe_float(row.get("price"))
        base_qty = _safe_float(row.get("size") or row.get("filled_size"))
        if fill_px <= 0 or base_qty <= 0:
            return
        quote = _quote_ccy_from_symbol(symbol)
        quote_qty = fill_px * base_qty
        usdc_qty = quote_qty if quote == "USDC" else 0.0
        ev = {
            "type": "fill",
            "status": "FILL",
            "exchange": "COINBASE",
            "alias": self.alias,
            "symbol": symbol,
            "side": _upper(row.get("side")),
            "expected_px": fill_px,
            "fill_px": fill_px,
            "base_qty": base_qty,
            "quote": quote,
            "quote_qty": quote_qty,
            "usdc_qty": usdc_qty,
            "client_id": row.get("client_order_id") or row.get("clientOrderId"),
            "exchange_order_id": str(row.get("order_id") or row.get("orderId") or ""),
            "ts_exchange": ts_ex,
            "ts_local": ts_loc,
            "ts": ts_loc,
        }
        ev["meta"] = {"source": "poller"}
        # (dans _emit_fill, même chose sur l'event avant self._emit(ev))

        self._emit(ev)
        if fid:
            self._seen_fills.add(fid)
            if len(self._seen_fills) > self._seen_fills_max:
                self._seen_fills.clear()

    async def _poll_once(self) -> None:
        try:
            orders = await self._get_json("GET", "/api/v3/brokerage/orders/historical/batch")
            for row in (orders.get("orders") or orders.get("results") or []):
                oid = str(row.get("order_id") or row.get("orderId") or "")
                if not oid:
                    continue
                new = self._map_status(row.get("status") or row.get("order_status") or "")
                old = self._last_status.get(oid)
                if old != new:
                    self._last_status[oid] = new
                    self._emit_order(row)
        except Exception as e:
            self._errors += 1
            log.warning(f"[CoinbaseATPoller] orders poll: {e}")

        try:
            fills = await self._get_json("GET", "/api/v3/brokerage/orders/historical/fills")
            for row in (fills.get("fills") or []):
                self._emit_fill(row)
        except Exception as e:
            self._errors += 1
            log.warning(f"[CoinbaseATPoller] fills poll: {e}")

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._session = aiohttp.ClientSession(trust_env=True)
        self._task = asyncio.create_task(self._run())
        log.info("[CoinbaseATPoller:%s] started", self.alias)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None
        log.info("[CoinbaseATPoller:%s] stopped", self.alias)

    async def _run(self) -> None:
        try:
            while self._running:
                await self._poll_once()
                await asyncio.sleep(self.interval_s)
        except asyncio.CancelledError:
            pass

    def get_status(self) -> Dict[str, Any]:
        return {
            "module": "CoinbaseATPoller",
            "alias": self.alias,
            "healthy": self._running,
            "last_event_ts": self._last_event_ts,
            "errors": self._errors,
        }


# ---------------------------------------------------------------------------
# Hub / Manager
# ---------------------------------------------------------------------------

class PrivateWSHub:
    """
    Hub tri-CEX par alias (TT/TM/MM ou autres) :
      - binance_accounts:   { "TT": {"api_key":...}, ... }
      - bybit_accounts:     { "TT": {"api_key":..., "secret":...}, ... }
      - coinbase_at_accounts:{ "TT": {"api_key":..., "secret":...}, ... }

    + Passerelles de transferts via `transfer_clients` (facultatif).
    """

    def __init__(
        self,
        *,
        binance_accounts: Dict[str, Dict[str, str]] | None = None,
        bybit_accounts: Dict[str, Dict[str, str]] | None = None,
        coinbase_at_accounts: Dict[str, Dict[str, str]] | None = None,
        on_event: Optional[Callable[[dict], None]] = None,
        cb_interval_s: float = 1.5,
        transfer_clients: Optional[Dict[str, Any]] = None,
        config: Any | None = None,
        coinbase_ws_ack_clients: Dict[str, Any] | None = None,
        **_  # future-proof
    ):
        self._callback = on_event
        self._rm_callback: Optional[Callable[[dict], None]] = None
        self._observers: List[Callable[[dict], Any]] = []
        self._started = False
        self._hb_task: Optional[asyncio.Task] = None


        # LRU dédup (clé compacte)
        self._seen_events = OrderedDict()
        self._seen_limit = 10_000

        # Config / bases
        self.config = config
        if self.config is not None:
            try:
                cb_interval_s = float(getattr(self.config, "cb_private_poll_interval_s", cb_interval_s))
            except Exception:
                pass
            self._binance_rest_base = getattr(self.config, "binance_rest_base", "https://api.binance.com")
            self._binance_ws_priv  = getattr(self.config, "binance_ws_private_base", "wss://stream.binance.com:9443")
            self._bybit_ws_priv    = getattr(self.config, "bybit_ws_private_url", "wss://stream.bybit.com/v5/private")
            self._coinbase_api_base= getattr(self.config, "coinbase_api_base", "https://api.coinbase.com")
        else:
            self._binance_rest_base = "https://api.binance.com"
            self._binance_ws_priv   = "wss://stream.binance.com:9443"
            self._bybit_ws_priv     = "wss://stream.bybit.com/v5/private"
            self._coinbase_api_base = "https://api.coinbase.com"

        # Clients par alias
        self.binance_by_alias: Dict[str, BinanceUserStream] = {}
        self.bybit_by_alias: Dict[str, BybitPrivateWS] = {}
        self.cb_poller_by_alias: Dict[str, CoinbaseATPoller] = {}
        self.cb_ack_ws_by_alias: Dict[str, CoinbaseOrdersWS] = {}

        # 1) Clients explicites passés au ctor
        for al, cfg in (coinbase_ws_ack_clients or {}).items():
            alias = _upper(al)
            ws_urls_cfg = cfg.get("ws_urls")
            default_urls = getattr(self.config, "coinbase_ws_orders_urls", None)
            ws_urls = ws_urls_cfg or default_urls or ["wss://advanced-trade-ws.coinbase.com"]

            auth_msg_factory = cfg.get("auth_msg_factory")  # optionnel
            subscribe_payload = cfg.get("subscribe_payload") or {
                "type": "subscribe",
                "channel": "orders"
            }

            c = CoinbaseOrdersWS(
                alias=alias,
                ws_urls=ws_urls,
                auth_msg_factory=auth_msg_factory,
                subscribe_payload=subscribe_payload,
            )
            c.register_callback(self._dispatch(alias, "COINBASE"))
            self.cb_ack_ws_by_alias[alias] = c

        # 2) Auto-instanciation par config (flag) pour les mêmes alias que le poller
        try:
            if int(getattr(self.config, "cb_ack_ws_enabled", 0)):
                default_urls = getattr(self.config, "coinbase_ws_orders_urls", None) \
                               or ["wss://advanced-trade-ws.coinbase.com"]
                for al in list(self.cb_poller_by_alias.keys()):
                    alias = _upper(al)
                    if alias in self.cb_ack_ws_by_alias:
                        continue
                    c = CoinbaseOrdersWS(alias=alias, ws_urls=default_urls)
                    c.register_callback(self._dispatch(alias, "COINBASE"))
                    self.cb_ack_ws_by_alias[alias] = c
        except Exception:
            # pas bloquant
            pass
        # WS ACK Coinbase (facultatif) — dual-path
        self.cb_ack_ws_by_alias: Dict[str, CoinbaseOrdersWS] = {}
        # 1) Si des clients sont fournis explicitement (coinbase_ws_ack_clients={"TT": {...}})
        for al, cfg in (coinbase_ws_ack_clients or {}).items():
            alias = _upper(al)
            ws_urls = cfg.get("ws_urls") or getattr(self.config, "coinbase_ws_orders_urls", None) \
                      or ["wss://advanced-trade-ws.coinbase.com"]
            auth_msg_factory = cfg.get("auth_msg_factory")  # optionnel
            subscribe_payload = cfg.get("subscribe_payload") or {"type": "subscribe", "channel": "orders"}
            c = CoinbaseOrdersWS(alias=alias, ws_urls=ws_urls,
                                 auth_msg_factory=auth_msg_factory,
                                 subscribe_payload=subscribe_payload)
            c.register_callback(self._dispatch(alias, "COINBASE"))
            self.cb_ack_ws_by_alias[alias] = c

        # 2) Si activé par config (cb_ack_ws_enabled=1), on instancie pour les mêmes alias que le poller
        try:
            if int(getattr(self.config, "cb_ack_ws_enabled", 0)):
                default_ws_urls = getattr(self.config, "coinbase_ws_orders_urls", None) \
                                  or ["wss://advanced-trade-ws.coinbase.com"]
                for al in (list(self.cb_poller_by_alias.keys())):
                    alias = _upper(al)
                    if alias in self.cb_ack_ws_by_alias:
                        continue
                    c = CoinbaseOrdersWS(alias=alias, ws_urls=default_ws_urls)
                    c.register_callback(self._dispatch(alias, "COINBASE"))
                    self.cb_ack_ws_by_alias[alias] = c
        except Exception:
            pass


        # Transferts
        self._transfer_clients: Dict[str, Any] = {}
        if transfer_clients:
            self.connect_transfer_clients(transfer_clients)

        # Instantiation par alias — TT/TM/MM ou autre
        for al, cfg in (binance_accounts or {}).items():
            alias = _upper(al)
            c = BinanceUserStream(
                alias=alias,
                api_key=cfg["api_key"],
                rest_base=self._binance_rest_base,
                ws_base=self._binance_ws_priv,
            )
            c.register_callback(self._dispatch(alias, "BINANCE"))
            self.binance_by_alias[alias] = c

        # === Clients Bybit par alias (multi-endpoints supportés) ===============
        for al, cfg in (bybit_accounts or {}).items():
            alias = _upper(al)
            urls_cfg = getattr(self.config, "bybit_ws_private_urls", None)
            if urls_cfg and isinstance(urls_cfg, (list, tuple)) and len(urls_cfg) > 0:
                c = BybitPrivateWS(
                    alias=alias,
                    api_key=cfg["api_key"],
                    secret=cfg["secret"],
                    ws_urls=list(urls_cfg),
                )
            else:
                c = BybitPrivateWS(
                    alias=alias,
                    api_key=cfg["api_key"],
                    secret=cfg["secret"],
                    ws_url=getattr(self.config, "bybit_ws_private_url", "wss://stream.bybit.com/v5/private"),
                )
            c.register_callback(self._dispatch(alias, "BYBIT"))
            self.bybit_by_alias[alias] = c

        for al, cfg in (coinbase_at_accounts or {}).items():
            alias = _upper(al)
            c = CoinbaseATPoller(
                alias=alias,
                api_key=cfg["api_key"],
                secret=cfg["secret"],
                api_base=self._coinbase_api_base,
                interval_s=cb_interval_s,
            )
            c.register_callback(self._dispatch(alias, "COINBASE"))
            self.cb_poller_by_alias[alias] = c

        # === P0: QoS / queues / heartbeats - init unique (après création des clients) ===
        # Alias compat (le reste du code lit 'self.cfg')
        self.cfg = self.config

        # Priorités QoS (plus petit = plus prioritaire)
        if not hasattr(self, "_priority_map"):
            self._priority_map = {"fill": 0, "ack": 1, "reject": 2, "other": 3}

        # Files par (exchange, alias)
        if not hasattr(self, "_queues"):
            from collections import defaultdict, deque
            qmax = int(getattr(self.cfg, "PWS_QUEUE_MAXLEN", 1000))
            self._queues = defaultdict(lambda: deque(maxlen=qmax))

        # Heartbeats
        if not hasattr(self, "_last_hb"):
            self._last_hb = {}

        # Paramètres ping/backoff (défauts sûrs)
        self._ping_interval_s = float(getattr(self.cfg, "PWS_PING_INTERVAL_S", 10.0))
        self._pong_timeout_s = float(getattr(self.cfg, "PWS_PONG_TIMEOUT_S", 3.0))
        self._backoff_base_ms = int(getattr(self.cfg, "PWS_BACKOFF_BASE_MS", 200))
        self._backoff_max_ms = int(getattr(self.cfg, "PWS_BACKOFF_MAX_MS", 5000))

        # Pools WS par région (optionnel, valeur non bloquante si métrique absente)
        try:
            from modules.obs_metrics import PWS_POOL_SIZE
            PWS_POOL_SIZE.labels(region="EU").set(float(getattr(self.cfg, "PWS_POOL_SIZE_EU", 1)))
            PWS_POOL_SIZE.labels(region="US").set(float(getattr(self.cfg, "PWS_POOL_SIZE_US", 1)))
        except Exception:
            pass
        # Pools de connexion par région (EU/US) — sharding réel
        try:
            eu_size = int(getattr(self.cfg, "PWS_POOL_SIZE_EU", 1))
            us_size = int(getattr(self.cfg, "PWS_POOL_SIZE_US", 1))
        except Exception:
            eu_size, us_size = 1, 1
        self._pools: Dict[str, asyncio.Semaphore] = {
            "EU": asyncio.Semaphore(max(1, eu_size)),
            "US": asyncio.Semaphore(max(1, us_size)),
        }
        # Alias compat
        self._pools_by_region = self._pools

        # Mapping région (clé "EX:ALIAS" > "EX" > défaut "EU")
        self._region_map: Dict[str, str] = dict(getattr(self.cfg, "PWS_REGION_MAP", {}) or {})



    # ------------------------- QoS / métriques / drain -------------------------

    def set_qos_policy(self, priorities: dict) -> None:
        for k, v in (priorities or {}).items():
            if not isinstance(v, int) or v < 0:
                raise ValueError(f"Priorité invalide pour {k}: {v}")
        self._priority_map.update(priorities)
        try:
            logging.getLogger("PrivateWSHub").info("QoS policy mise à jour: %s", self._priority_map)
        except Exception:
            pass

    def _get_region(self, exchange: str, alias: str) -> str:
        """
        Retourne "EU" ou "US" selon mapping. Priorité:
        1) clé "EX:ALIAS", 2) clé "EX", 3) défaut "EU".
        """
        exu, alu = _upper(exchange), _upper(alias)
        key1 = f"{exu}:{alu}"
        if key1 in self._region_map:
            return str(self._region_map[key1]).upper()
        if exu in self._region_map:
            return str(self._region_map[exu]).upper()
        return "EU"


    def on_event(self, event: dict) -> None:
        """
        Entrée unique: QoS, métriques (ACK/FILL + lag + heartbeats), drain minimal.
        """
        import time as _t
        ev = dict(event or {})
        exu = _upper(ev.get("exchange") or "UNKNOWN")
        alu = _upper(ev.get("alias") or "NA")
        status = _upper(ev.get("status") or "OTHER")
        source = ((ev.get("meta") or {}).get("source") or "ws")

        # Normalisation légère pour cohérence
        if status == "FILLED":
            status = "FILL"
            ev["status"] = "FILL"
        if status == "OTHER" and str(ev.get("type")).lower() == "fill":
            status = "FILL"
            ev["status"] = "FILL"

        ev.setdefault("ts_local", _t.time())
        key = (exu, alu)

        # Heartbeat frais
        self._last_hb[key] = ev["ts_local"]
        try:
            PWS_HEARTBEAT_GAP_SECONDS.labels(exu, alu).set(0.0)
        except Exception:
            pass

        # Lag serveur->local si ts présent (ts_ms|ts_exchange|ts)
        try:
            v_ts = ev.get("ts_ms") or ev.get("ts_exchange") or ev.get("ts")
            if v_ts is not None:
                ts_ms = float(v_ts)
                if ts_ms < 3e10:  # secondes -> ms
                    ts_ms *= 1000.0
                _observe_event_lag_ms(exu, alu, ts_ms)
        except Exception:
            pass

        # Compteurs + latences ACK/FILL
        try:
            PWS_EVENTS_TOTAL.labels(exu, alu, status.lower(), source).inc()
            self._pws_observe_latency(exu, status, ev)
        except Exception:
            pass
        if status == "FILL" and "ts_exchange" not in ev:
            ev["ts_exchange"] = ev.get("ts_local")

        # QoS & drop policy
        if status in ("FILL", "PARTIAL"):
            cat = "fill"
        elif status == "ACK":
            cat = "ack"
        elif status == "REJECT":
            cat = "reject"
        else:
            cat = "other"

        pri = self._priority_map.get(cat, 3)
        q = self._queues[key]

        try:
            if pri <= 1:
                q.appendleft(ev)  # fills/acks en tête
            else:
                if len(q) == q.maxlen and pri >= 2:
                    try:
                        PWS_DROPPED_TOTAL.labels(exu, alu, f"queue_full_{cat}").inc()
                    except Exception:
                        pass
                    return
                q.append(ev)
        except Exception:
            try:
                PWS_DROPPED_TOTAL.labels(exu, alu, "exception").inc()
            except Exception:
                pass
            return

        # Drain minimal (double émission dans _pws_drain_one)
        try:
            self._pws_drain_one(key)
        except Exception:
            log.exception("[PrivateWSHub] drain_one error")

    def _pws_drain_one(self, key: tuple) -> None:
        """
        Émet 1 événement (le plus prioritaire dispo) vers RM (si présent) puis Engine.
        """
        q = self._queues.get(key)
        if not q or not q:
            return
        ev = q.popleft()
        # Publie la profondeur/capacité de la file après pop (kind = type d’event)
        try:
            ex, al = key
            kind = str(ev.get("type", "other")).lower()
            depth = len(q)
            cap = getattr(q, "maxlen", 0) or getattr(self, "_queue_max", 0) or 0
            PWS_QUEUE_DEPTH.labels(ex, al, kind).set(float(depth))
            PWS_QUEUE_CAP.labels(ex, al, kind).set(float(cap))
        except Exception:
            pass

            # 1) callback RM (async/sync sans bloquer)
            self._deliver_callback(getattr(self, "_rm_callback", None), ev, label="RM")

            # 2) callback Engine (sync)
            self._deliver_callback(getattr(self, "_callback", None), ev, label="Engine")

            # 3) Observateurs additionnels (balance fetcher, watchdogs…)
            for cb in list(self._observers):
                self._deliver_callback(cb, ev, label="Observer")

        def _deliver_callback(self, cb: Optional[Callable[[dict], Any]], ev: dict, label: str) -> None:
            if not cb:
                return
            try:
                if inspect.iscoroutinefunction(cb):
                    asyncio.create_task(cb(ev))
                else:
                    cb(ev)
            except Exception:
                log.exception("[PrivateWSHub] %s callback failed", label)

        # 2) callback Engine (sync)
        if getattr(self, "_callback", None):
            try:
                self._callback(ev)
            except Exception:
                logging.getLogger("PrivateWSHub").exception("Engine callback a levé une exception")

    def _pws_observe_latency(self, exchange: str, status: str, ev: dict) -> None:
        """Observe la latence entre ts_exchange et ts_local pour ACK/FILL/PARTIAL."""
        try:
            ts_ex = float(ev.get("ts_exchange") or 0.0)
            ts_loc = float(ev.get("ts_local") or _now())
            if ts_ex <= 0.0:
                return
            lat_ms = max(0.0, (ts_loc - ts_ex) * 1000.0)
            if status == "ACK":
                PWS_ACK_LATENCY_MS.labels(exchange).observe(lat_ms)
            elif status in ("FILL", "PARTIAL"):
                PWS_FILL_LATENCY_MS.labels(exchange).observe(lat_ms)
        except Exception:
            pass

    # ------------------------- dispatch / callbacks ----------------------------

    def _dedup_key(self, ev: dict):
        """Clé robuste de dédup: (exchange, alias, orderId/clientId, seq/ts, type)"""
        ex = ev.get("exchange") or ev.get("ex")
        alias = ev.get("alias") or ev.get("account") or "default"
        oid = (
            ev.get("exchange_order_id")
            or ev.get("orderId")
            or ev.get("id")
            or ev.get("clientOrderId")
            or ev.get("client_id")
        )
        seq = (
            ev.get("seq")
            or ev.get("u")
            or ev.get("E")
            or ev.get("ts")
            or ev.get("T")
            or ev.get("timestamp")
        )
        typ = ev.get("type")
        return (ex, alias, oid, seq, typ)

    def _dedup_seen(self, key) -> bool:
        if key in self._seen_events:
            self._seen_events.move_to_end(key, last=True)
            return True
        self._seen_events[key] = True
        if len(self._seen_events) > self._seen_limit:
            self._seen_events.popitem(last=False)
        return False

    def _dispatch(self, *args):
        """
        - _dispatch(ev: dict)
        - _dispatch(alias, exchange) -> closure(ev)
        Applique la dédup et normalise exchange/alias.
        """
        # Émission directe
        if len(args) == 1 and isinstance(args[0], dict):
            ev = dict(args[0] or {})
            exu = str((ev.get("exchange") or "UNKNOWN")).upper()
            alu = str((ev.get("alias") or "NA")).upper()
            ev.setdefault("exchange", exu)
            ev.setdefault("alias", alu)
            try:
                key_d = self._dedup_key(ev)
                if self._dedup_seen(key_d):
                    try:
                        PWS_DEDUP_HITS_TOTAL.labels(exchange=exu, alias=alu).inc()
                    except Exception:
                        pass
                    return
            except Exception:
                pass
            try:
                self.on_event(ev)
            except Exception:
                log.exception("[PrivateWSHub] dispatch(ev) failure")
            return

        # Fabrique de closure
        if len(args) == 2:
            alias, exchange = args
            exu = str(exchange).upper()
            alu = str(alias).upper()

            def _inner(ev: dict):
                e = dict(ev or {})
                e.setdefault("alias", alu)
                e.setdefault("exchange", exu)
                try:
                    key_d = self._dedup_key(e)
                    if self._dedup_seen(key_d):
                        try:
                            PWS_DEDUP_HITS_TOTAL.labels(exchange=exu, alias=alu).inc()
                        except Exception:
                            pass
                        return
                except Exception:
                    pass
                try:
                    self.on_event(e)
                except Exception:
                    log.exception("[PrivateWSHub] on_event pipeline error")

            return _inner

        raise TypeError("Invalid _dispatch usage. Use _dispatch(ev_dict) or _dispatch(alias, exchange).")

    def register_callback(self, cb, *, role: str = "engine") -> None:
        """
        Enregistre un callback consommateur.
        role="engine" (defaut), "risk" ou "observer" (fan-out additionnel).
        """
        if not cb:
            return
        kind = str(role or "engine").lower()
        if kind == "engine":
            prev = getattr(self, "_callback", None)
            if prev and prev is not cb and self._rm_callback is None:
                self._rm_callback = prev
            self._callback = cb
            return
        if kind in ("risk", "rm", "riskmanager"):
            self._rm_callback = cb
            return
        if kind == "observer":
            self._observers.append(cb)
            return
        raise ValueError(f"unknown callback role={role}")


    # ------------------------- lifecycle --------------------------------------

    async def run_heartbeat_probe(self) -> None:
        import time, asyncio
        try:
            while True:
                now = time.time()
                try:
                    for (ex, alias), last in list(self._last_hb.items()):
                        gap = max(0.0, now - float(last))
                        PWS_HEARTBEAT_GAP_SECONDS.labels(ex, alias).set(gap)
                except Exception:
                    pass
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            return

    def register_alert_callback(self, cb) -> None:
        """Option: branche un callback externe (AlertDispatcher)."""
        self._alert_cb = cb

    def _notify_alert(self, severity: str, reason: str, exchange: str = "-", alias: str = "-", kind: str = "-",
                      message: str = "") -> None:
        try:
            PWS_ALERT_TOTAL.labels(severity, reason, exchange, alias, kind).inc()
        except Exception:
            pass
        try:
            if severity.upper() in ("CRITICAL", "ERROR"):
                log.error("[ALERT][%s][%s] %s (%s:%s/%s)", severity, reason, message, exchange, alias, kind)
            else:
                log.warning("[ALERT][%s][%s] %s (%s:%s/%s)", severity, reason, message, exchange, alias, kind)
        except Exception:
            pass
        cb = getattr(self, "_alert_cb", None)
        if cb:
            try:
                cb({
                    "component": "PrivateWSHub",
                    "severity": severity,
                    "reason": reason,
                    "exchange": exchange,
                    "alias": alias,
                    "kind": kind,
                    "message": message,
                })
            except Exception:
                pass

    def _pws_backoff_bounds(self, region: str) -> tuple[int, int]:
        """
        Consolidé: bornes de backoff WS privés, modulées par l'état PACER.
        - Lit PWS_BACKOFF_BASE_MS / PWS_BACKOFF_MAX_MS (défauts 200/5000).
        - DEGRADED: cap ≤ 2000 ms ; SEVERE: cap ≤ 800 ms.
        - base_ms ≥ 50 ms, cap_ms ≥ base_ms.
        """
        base_ms = int(getattr(self.cfg, "PWS_BACKOFF_BASE_MS", getattr(self, "_backoff_base_ms", 200)))
        cap_ms = int(getattr(self.cfg, "PWS_BACKOFF_MAX_MS", getattr(self, "_backoff_max_ms", 5000)))
        base_ms = max(50, base_ms)
        cap_ms = max(base_ms, cap_ms)
        try:
            state = str(getattr(self.cfg, f"PWS_PACER_{region}", "NORMAL")).upper()
            if state == "DEGRADED":
                cap_ms = min(cap_ms, 2000)
            elif state == "SEVERE":
                cap_ms = min(cap_ms, 800)
        except Exception:
            pass
        return base_ms, cap_ms

    async def run_alerts(self) -> None:
        """
        Boucle d'alerting (queue saturation + heartbeat gaps).
        Seuils côté config:
            - PWS_QUEUE_SATURATION_RATIO (def 0.85)
            - PWS_HEARTBEAT_MAX_GAP_S (def 5.0)
            - PWS_ALERT_PERIOD_S (def 5.0)
        """
        ratio_thr = float(getattr(self.cfg, "PWS_QUEUE_SATURATION_RATIO", 0.85))
        hb_max = float(getattr(self.cfg, "PWS_HEARTBEAT_MAX_GAP_S", 5.0))
        period = float(getattr(self.cfg, "PWS_ALERT_PERIOD_S", 5.0))

        while getattr(self, "_started", False):
            now = _now()

            # 1) Heartbeat gaps
            try:
                for (ex, al), last in list(self._last_hb.items()):
                    gap = max(0.0, now - float(last))
                    # Optionnel: exposer la jauge de gap si distincte
                    if gap > hb_max:
                        try:
                            PWS_HEARTBEAT_GAP_BREACH_TOTAL.labels(ex, al).inc()
                        except Exception:
                            pass
                        self._notify_alert(
                            severity="WARN",
                            reason="heartbeat_gap",
                            exchange=ex, alias=al, kind="-",
                            message=f"Heartbeat gap={gap:.2f}s > {hb_max:.2f}s",
                        )
            except Exception:
                pass

            # 2) Queue saturation
            try:
                for (ex, al, kind), dq in list(self._queues.items()):
                    maxlen = getattr(dq, "maxlen", 0) or 0
                    fill = 0.0 if maxlen <= 0 else (len(dq) / float(maxlen))
                    try:
                        PWS_QUEUE_FILL_RATIO.labels(ex, al, kind).set(fill)
                    except Exception:
                        pass
                    # Publie depth/cap (aligne le dashboard Grafana)
                    try:
                        depth = len(dq)
                        cap = getattr(dq, "maxlen", 0) or self._queue_max
                        PWS_QUEUE_DEPTH.labels(ex, al, kind).set(depth)
                        PWS_QUEUE_CAP.labels(ex, al, kind).set(cap)
                    except Exception:
                        pass

                    if maxlen > 0 and fill >= ratio_thr:
                        try:
                            PWS_QUEUE_SATURATION_TOTAL.labels(ex, al, kind).inc()
                        except Exception:
                            pass
                        self._notify_alert(
                            severity="WARN",
                            reason="queue_saturation",
                            exchange=ex, alias=al, kind=kind,
                            message=f"Queue fill={fill:.2%} (seuil {ratio_thr:.0%})",
                        )
            except Exception:
                pass

            await asyncio.sleep(max(1.0, period))

    async def reconnect_with_backoff(self, exchange: str, alias: str, connect_coro_factory):
        """Enveloppe de (re)connexion WS privée avec backoff borné + jitter + alertes.
        - Lit bornes via _pws_backoff_bounds(region) (pacer-aware).
        - Jitter aléatoire court pour éviter l'effet troupeau.
        - Remet à zéro le backoff après une session stable >= stable_s.
        """
        import asyncio, random, time, logging
        region = self._get_region(exchange, alias)
        pool = (getattr(self, "_pools_by_region", {}) or {}).get(region)
        base_ms, cap_ms = self._pws_backoff_bounds(region)
        backoff_ms = max(50, base_ms)
        stable_s = float(getattr(self.cfg, "PWS_STABLE_RESET_S", 30.0))
        jitter_ms = int(getattr(self.cfg, "PWS_JITTER_MS", 50))
        last_ok = 0.0
        while True:
            try:
                t0 = time.time()
                if pool is not None:
                    async with pool:
                        await connect_coro_factory()
                else:
                    await connect_coro_factory()
                # si on sort proprement, c'est une reconnexion volontaire
                try:
                    PWS_RECONNECTS_TOTAL.labels(exchange, alias).inc()
                except Exception:
                    pass
                last_ok = time.time()
                # reset du backoff si la session a tenu
                if (last_ok - t0) >= stable_s:
                    backoff_ms = max(50, base_ms)
            except asyncio.CancelledError:
                return
            except Exception as e:
                # alerte + maj métriques
                try:
                    self._notify_alert("WARN", "ws_disconnect", exchange, alias, "private_ws", str(e))
                except Exception:
                    pass
                # croissance avec jitter
                sleep_ms = min(cap_ms, int(backoff_ms * 2))
                j = random.randint(0, max(0, jitter_ms))
                await asyncio.sleep((sleep_ms + j) / 1000.0)
                backoff_ms = sleep_ms

    async def start(self) -> None:
        """
        Démarre tous les clients (Binance/Bybit/COINBASE poller + ACK WS),
        instrumente les (re)connexions avec backoff pacer-aware,
        lance la sonde heartbeat 1 Hz.
        """
        if getattr(self, "_started", False):
            return

        tasks: List[asyncio.Task] = []

        def _schedule_client(exchange: str, alias: str, client) -> None:
            if client is None or not hasattr(client, "start"):
                return

            exu, alu = str(exchange).upper(), str(alias).upper()
            region = self._get_region(exu, alu)
            pool = self._pools.get(region) or next(iter(self._pools.values()))

            async def _connect_once():
                # clamp léger selon le PACER (non bloquant)
                try:
                    clamp = float(PACER.clamp("pws_connect"))
                    if clamp > 0:
                        await asyncio.sleep(clamp)
                except Exception:
                    pass
                await client.start()

            async def _with_pool():
                async with pool:
                    await _connect_once()

            # Démarrage via backoff pacer-aware (Hub)
            if hasattr(self, "reconnect_with_backoff") and callable(getattr(self, "reconnect_with_backoff")):
                tasks.append(asyncio.create_task(self.reconnect_with_backoff(exu, alu, _with_pool)))
            else:
                tasks.append(asyncio.create_task(_with_pool()))

        # WS ACK Coinbase (dual-path)
        for al, c in (getattr(self, "cb_ack_ws_by_alias", {}) or {}).items():
            _schedule_client("COINBASE", al, c)

        # Binance / Bybit / Coinbase Poller
        for al, c in (getattr(self, "binance_by_alias", {}) or {}).items():
            _schedule_client("BINANCE", al, c)
        for al, c in (getattr(self, "bybit_by_alias", {}) or {}).items():
            _schedule_client("BYBIT", al, c)
        for al, c in (getattr(self, "cb_poller_by_alias", {}) or {}).items():
            _schedule_client("COINBASE", al, c)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Heartbeat 1 Hz (jauge PWS_HEARTBEAT_GAP_SECONDS alimentée par la tâche)
        try:
            if getattr(self, "_hb_task", None) is None or self._hb_task.done():
                self._hb_task = asyncio.create_task(self.run_heartbeat_probe())
        except Exception:
            log.exception("[PrivateWSHub] heartbeat probe start failed")

        self._started = True
        # Alerting (queue/heartbeat)
        try:
            if getattr(self, "_alerts_task", None) is None or self._alerts_task.done():
                self._alerts_task = asyncio.create_task(self.run_alerts())
        except Exception:
            log.exception("[PrivateWSHub] alerts loop start failed")

        try:
            log.info("[PrivateWSHub] started (%d clients)", len(tasks))
        except Exception:
            pass

    async def stop(self) -> None:
        if not self._started:
            return
        tasks: List[asyncio.Task] = []
        for c in self.binance_by_alias.values():
            tasks.append(asyncio.create_task(c.stop()))
        for c in self.bybit_by_alias.values():
            tasks.append(asyncio.create_task(c.stop()))
        for c in self.cb_poller_by_alias.values():
            tasks.append(asyncio.create_task(c.stop()))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        for c in getattr(self, "cb_ack_ws_by_alias", {}).values():
            tasks.append(asyncio.create_task(c.stop()))


        if self._hb_task:
            self._hb_task.cancel()
            self._hb_task = None

        self._started = False
        log.info("[PrivateWSHub] stopped")

    # ------------------------- transfer clients --------------------------------

    def connect_transfer_clients(self, mapping: Dict[str, Any]) -> None:
        """
        Enregistre/actualise les clients de transferts.
        mapping: {"BINANCE": client, "BYBIT": client, "COINBASE": client, ...}
        """
        for ex, cli in (mapping or {}).items():
            if not cli:
                continue
            self._transfer_clients[_upper(ex)] = cli

    def has_transfer_client(self, exchange: str) -> bool:
        return _upper(exchange) in self._transfer_clients

    def get_transfer_client(self, exchange: str) -> Any | None:
        return self._transfer_clients.get(_upper(exchange))

    async def _call_transfer_method(self, ex: str, method: str, payload: Dict[str, Any]) -> Tuple[bool, Any, Optional[str]]:
        exu = _upper(ex)
        cli = self._transfer_clients.get(exu)
        if not cli:
            return False, None, f"no transfer client for {exu}"
        fn = getattr(cli, method, None)
        if not callable(fn):
            return False, None, f"client for {exu} has no method {method}"
        try:
            res = fn(**payload)
            if inspect.isawaitable(res):
                res = await res
            return True, res, None
        except Exception as e:
            return False, None, f"{type(e).__name__}: {e}"

    def _emit_transfer_event(self, *, subtype: str, exchange: str, alias: Optional[str], status: str,
                             payload: Dict[str, Any], result: Any = None, error: Optional[str] = None) -> None:
        ev = {
            "type": "transfer",
            "subtype": str(subtype).lower(),  # "wallet" | "subaccount"
            "exchange": _upper(exchange),
            "alias": _upper(alias) if alias else None,
            "status": _upper(status),
            "payload": dict(payload),
            "result": result,
            "error": error,
            "ts": _now(),
        }
        try:
            PWS_TRANSFERS_TOTAL.labels(
                exchange=ev["exchange"],
                subtype=ev["subtype"],
                status=ev["status"],
            ).inc()
        except Exception:
            pass
        try:
            log.info("[PrivateWSHub] %s", json.dumps({
                "pws_transfer": True,
                "exchange": ev["exchange"],
                "alias": ev["alias"],
                "subtype": ev["subtype"],
                "status": ev["status"],
                "error": ev.get("error"),
            }))
        except Exception:
            pass

        if self._callback:
            try:
                self._callback(ev)
            except Exception:
                log.exception("[PrivateWSHub] transfer callback error")

    async def transfer_wallet(self, *, ex: str, alias: str, from_wallet: str, to_wallet: str,
                              ccy: str, amount: float) -> Dict[str, Any]:
        exu, alu = _upper(ex), _upper(alias)
        payload = {
            "ex": exu, "alias": alu,
            "from_wallet": _upper(from_wallet), "to_wallet": _upper(to_wallet),
            "ccy": _upper(ccy), "amount": float(amount),
        }

        if amount <= 0:
            err = "amount must be > 0"
            self._emit_transfer_event(subtype="wallet", exchange=exu, alias=alu, status="ERROR",
                                      payload=payload, error=err)
            return {"ok": False, "result": None, "error": err, "exchange": exu, "alias": alu, "payload": payload}
        if _upper(from_wallet) == _upper(to_wallet):
            err = "from_wallet and to_wallet must differ"
            self._emit_transfer_event(subtype="wallet", exchange=exu, alias=alu, status="ERROR",
                                      payload=payload, error=err)
            return {"ok": False, "result": None, "error": err, "exchange": exu, "alias": alu, "payload": payload}

        ok, res, err = await self._call_transfer_method(exu, "transfer_wallet", payload)
        self._emit_transfer_event(subtype="wallet", exchange=exu, alias=alu,
                                  status=("OK" if ok else "ERROR"),
                                  payload=payload, result=res, error=err)
        return {"ok": ok, "result": res, "error": err, "exchange": exu, "alias": alu, "payload": payload}

    async def transfer_subaccount(self, *, ex: str, from_alias: str, to_alias: str,
                                  ccy: str, amount: float) -> Dict[str, Any]:
        exu = _upper(ex)
        payload = {
            "ex": exu, "from_alias": _upper(from_alias), "to_alias": _upper(to_alias),
            "ccy": _upper(ccy), "amount": float(amount),
        }

        if amount <= 0:
            err = "amount must be > 0"
            self._emit_transfer_event(subtype="subaccount", exchange=exu, alias=_upper(to_alias),
                                      status="ERROR", payload=payload, error=err)
            return {"ok": False, "result": None, "error": err, "exchange": exu, "payload": payload}
        if _upper(from_alias) == _upper(to_alias):
            err = "from_alias and to_alias must differ"
            self._emit_transfer_event(subtype="subaccount", exchange=exu, alias=_upper(to_alias),
                                      status="ERROR", payload=payload, error=err)
            return {"ok": False, "result": None, "error": err, "exchange": exu, "payload": payload}

        ok, res, err = await self._call_transfer_method(exu, "transfer_subaccount", payload)
        self._emit_transfer_event(subtype="subaccount", exchange=exu, alias=_upper(to_alias),
                                  status=("OK" if ok else "ERROR"),
                                  payload=payload, result=res, error=err)
        return {"ok": ok, "result": res, "error": err, "exchange": exu, "payload": payload}

    async def get_wallets_snapshot(self) -> Dict[str, Any] | None:
        """
        Agrège un snapshot de wallets depuis les transfer_clients s'ils exposent
        `get_wallets_snapshot()` ou `get_all_wallets()`.
        """
        out: Dict[str, Any] = {}
        had_any = False
        for ex, cli in self._transfer_clients.items():
            try:
                fn = None
                for name in ("get_wallets_snapshot", "get_all_wallets"):
                    if hasattr(cli, name):
                        fn = getattr(cli, name)
                        break
                if not fn:
                    continue
                res = fn()
                if inspect.isawaitable(res):
                    res = await res
                if res:
                    out[ex] = res
                    had_any = True
            except Exception:
                log.exception("[PrivateWSHub] get_wallets_snapshot from %s failed", ex)
        return out if had_any else None

    # ------------------------- status -----------------------------------------

    def get_status(self) -> Dict[str, Any]:
        return {
            "module": "PrivateWSHub",
            "healthy": self._started,
            "submodules": {
                "binance": {al: c.get_status() for al, c in self.binance_by_alias.items()},
                "bybit":   {al: c.get_status() for al, c in self.bybit_by_alias.items()},
                "coinbase":{al: c.get_status() for al, c in self.cb_poller_by_alias.items()},
            },
            "transfers": {
                "available": sorted(self._transfer_clients.keys()),
                "has_binance": "BINANCE" in self._transfer_clients,
                "has_bybit": "BYBIT" in self._transfer_clients,
                "has_coinbase": "COINBASE" in self._transfer_clients or "COINBASE_AT" in self._transfer_clients,
            },
        }

