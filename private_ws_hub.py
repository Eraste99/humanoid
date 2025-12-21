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
  - "transfer": résultat d’un transfert interne (OK/ERROR), consommé par le RM.
      # Contrat minimal (Ticket 8 — capital en mouvement) :
      {
        "type": "transfer",
        "subtype": "wallet" | "subaccount",
        "exchange": "BINANCE"|"BYBIT"|"COINBASE",
        "alias": "TT"|"TM"|None,      # alias focus (dest alias pour subtype="subaccount")
        "status": "OK"|"ERROR",
        "ccy": "USDC",
        "amount": 100.0,              # en ccy
        "from_alias": "TT"|None,      # pour subtype="subaccount"
        "to_alias": "TM"|None,        # pour subtype="subaccount"
        "from_wallet": "SPOT"|None,   # pour subtype="wallet"
        "to_wallet": "FUNDING"|None,  # pour subtype="wallet"
        "payload": {...},             # payload brut, toujours présent
        "result": {...}|None,
        "error": "...",
        "ts": 1700000000.0
      }

register_callback(cb, role="engine") supporte plusieurs rôles :
    • role="engine"  → callback principal Engine
    • role="risk"    → RiskManager (fan-out avant Engine)
    • role="observer"→ observateurs additionnels (watchdogs…)
    
API
---
  hub = PrivateWSHub(..., transfer_clients=..., config=BotConfigOrNone)
  hub.register_callback(cb)         # l'ancien cb est promu en cb RM (double émission)
  await hub.start() / await hub.stop()

  # Transferts
  await hub.transfer_wallet(ex, alias, from_wallet, to_wallet, ccy, amount)
  await hub.transfer_subaccount(ex, from_alias, to_alias, ccy, amount)

  # Snapshots optionnels de wallets (si clients les exposent)
   # Snapshots optionnels de wallets (si clients les exposent)
  await hub.get_wallets_snapshot()

Contrat événements FILL/PARTIAL
-------------------------------
  Event transmis vers RM/Reconciler si et seulement si :
    • status in {"FILL", "PARTIAL"} (après normalisation),
    • payload contient au minimum :
        - exchange, alias, symbol, side,
        - fill_px, base_qty, quote, quote_qty,
        - au moins un identifiant: client_id ou exchange_order_id,
        - timestamp ts_local (toujours) + ts_exchange (ou dérivé de ts_ms).

  Les événements "order" (type="order") avec status FILLED/CANCELED restent de
  type "order" et sont traités côté Engine, mais ne passent plus le filtre
  FILL/PARTIAL s'ils ne respectent pas ce contrat.

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
from contracts.payloads import normalize_reason_code
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
        PWS_HEARTBEAT_GAP_SLO_SECONDS,
        PWS_HEALTH_STATE,
        PWS_HEARTBEAT_GAP_BREACH_TOTAL,
        PWS_DROPPED_TOTAL,
        PWS_ACK_LATENCY_MS,
        PWS_FILL_LATENCY_MS,
        PWS_QUEUE_FILL_RATIO,
        PWS_QUEUE_SATURATION_TOTAL,
        PWS_QUEUE_DEPTH,
        PWS_QUEUE_CAP,
        PWS_CALLBACK_ERRORS_TOTAL,
        PWS_ALERT_TOTAL,
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
    PWS_HEARTBEAT_GAP_SLO_SECONDS = _NoopMetric()
    PWS_HEALTH_STATE = _NoopMetric()
    PWS_HEARTBEAT_GAP_BREACH_TOTAL = _NoopMetric()
    PWS_DROPPED_TOTAL = _NoopMetric()
    PWS_ACK_LATENCY_MS = _NoopMetric()
    PWS_FILL_LATENCY_MS = _NoopMetric()
    PWS_QUEUE_FILL_RATIO = _NoopMetric()
    PWS_QUEUE_SATURATION_TOTAL = _NoopMetric()
    PWS_QUEUE_DEPTH = _NoopMetric()
    PWS_QUEUE_CAP = _NoopMetric()
    PWS_ALERT_TOTAL = _NoopMetric()
    PWS_CALLBACK_ERRORS_TOTAL = _NoopMetric()

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

def _log_callback_exception(task: "asyncio.Task", label: str) -> None:
    try:
        exc = task.exception()
    except asyncio.CancelledError:
        return
    except Exception:
        return
    if exc is None:
        return
    try:
        PWS_CALLBACK_ERRORS_TOTAL.labels(label).inc()
    except Exception:
        pass
    try:
        log.warning("[PrivateWSHub] callback %s raised: %s", label, exc)
    except Exception:
        pass

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

        # --- cfg.pws (source de vérité) — compat si cfg/pws absent ---
        pws = getattr(self.cfg, "pws", None) if self.cfg is not None else None
        if pws is None:
            class _PWSStub:
                pass

            pws = _PWSStub()

        self._region_map = dict(getattr(pws, "PWS_REGION_MAP", {"BINANCE": "EU", "BYBIT": "EU", "COINBASE": "US"}))
        self._queue_max = getattr_int(pws, "PWS_QUEUE_MAXLEN", 10_000)
        self._sat_ratio = getattr_float(pws, "PWS_QUEUE_SATURATION_RATIO", 0.90)
        self._ping_s = getattr_int(pws, "PWS_PING_INTERVAL_S", 20)
        self._pong_to_s = getattr_int(pws, "PWS_PONG_TIMEOUT_S", 10)
        self._hb_max_gap = getattr_int(pws, "PWS_HEARTBEAT_MAX_GAP_S", 30)
        self._stable_reset_s = getattr_int(pws, "PWS_STABLE_RESET_S", 3600)
        self._jitter_ms = getattr_int(pws, "PWS_JITTER_MS", 50)
        self._bo_base_ms = getattr_int(pws, "PWS_BACKOFF_BASE_MS", 500)
        self._bo_max_ms = getattr_int(pws, "PWS_BACKOFF_MAX_MS", 15_000)

        # dérivés pratiques
        self._bo_base_s = self._bo_base_ms / 1000.0
        self._bo_max_s = self._bo_max_ms / 1000.0

        # --- garde DRY_RUN/PROD (si cfg.g présent) ---
        g = getattr(self.cfg, "g", None) if self.cfg is not None else None
        feature_switches = getattr(g, "feature_switches", {}) if g is not None else {}
        mode = getattr(g, "mode", None)
        if mode == "DRY_RUN" and feature_switches.get("private_ws", False):
            raise RuntimeError("DRY_RUN: private_ws must be OFF")

    def register_callback(self, cb, *, role: str = "engine") -> None:
        """API uniforme: role="engine" (défaut) ou role="risk"."""
        if not cb:
            return
        kind = str(role or "engine").lower()
        if kind == "engine":
            prev = getattr(self, "_callback", None)
            if self._rm_callback is None and prev is not None and prev is not cb:
                self._rm_callback = prev
            self._callback = cb
            return
        if kind in ("risk", "rm", "riskmanager"):
            self._rm_callback = cb
            return
        raise ValueError(f"unknown callback role={role}")


    async def start(self):
        import aiohttp, asyncio
        if self._running:
            return
        self._running = True
        self._session = aiohttp.ClientSession(trust_env=True)
        # TODO: ouvrir la bonne URL selon self._region_map[self.exchange]
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        """
        Boucle principale WebSocket pour un client privé.

        La classe de base ne connaît pas le protocole concret (auth, subscribe,
        format des messages, etc.). Deux modèles d’utilisation possibles :

        - soit une sous-classe redéfinit `start()` et ne passe jamais par `_run()`
          (cas actuel de BinanceUserStream / BybitPrivateWS / CoinbaseOrdersWS) ;
        - soit une sous-classe garde `start()` tel quel et surchargera `_run()`
          pour implémenter la vraie boucle WS (connexion, souscriptions, read loop).

        Si cette implémentation de base est appelée telle quelle, c’est un
        problème de wiring : on préfère lever un NotImplementedError explicite.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} doit surcharger `_run()` ou `start()` "
            "pour implémenter la boucle WebSocket privée."
        )


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
    def __init__(
        self,
        cfg: Any,
        alias: str,
        api_key: str,
        rest_base: str = "https://api.binance.com",
        ws_base: str = "wss://stream.binance.com:9443",
    ):
        super().__init__(cfg, "BINANCE", alias)
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

        # Paramètres hérités de _BaseWSClient (cfg.pws.*)
        base_s = float(getattr(self, "_bo_base_s", 1.0) or 1.0)
        max_s = float(getattr(self, "_bo_max_s", 30.0) or 30.0)
        jitter_s = max(0.0, float(getattr(self, "_jitter_ms", 50)) / 1000.0)
        heartbeat_s = int(getattr(self, "_ping_s", 30) or 30)

        backoff = base_s
        while self._running:
            base = self._ws_bases[self._ws_ix % len(self._ws_bases)]
            url = f"{base}/ws/{self._listen_key}"
            try:
                async with self._session.ws_connect(url, heartbeat=heartbeat_s) as ws:
                    self._ws = ws
                    log.info("[BINANCE:%s] WS connected (%s)", self.alias, base)
                    backoff = base_s  # reset après une connexion OK
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
                        WS_FAILOVER_TOTAL.labels(
                            exchange="BINANCE",
                            alias=_upper(self.alias),
                            endpoint=new,
                        ).inc()
                except Exception:
                    pass

                # backoff exponentiel avec jitter cfg.pws (PWS_BACKOFF_*, PWS_JITTER_MS)
                delay = min(
                    max_s,
                    backoff + (random.random() * jitter_s),
                )
                try:
                    PWS_RECONNECTS_TOTAL.labels(
                        exchange="BINANCE",
                        alias=_upper(self.alias),
                    ).inc()
                    log.info(
                        "[PrivateWSHub] %s",
                        json.dumps(
                            {
                                "pws_reconnect": True,
                                "exchange": "BINANCE",
                                "alias": _upper(self.alias),
                                "delay_s": round(delay, 2),
                                "endpoint": base,
                            }
                        ),
                    )
                except Exception:
                    pass
                log.exception(
                    "[BINANCE:%s] reconnect in %.2fs (next endpoint idx=%d)",
                    _upper(self.alias),
                    delay,
                    self._ws_ix,
                )
                await asyncio.sleep(delay)
                backoff = min(backoff * 2.0, max_s)


# ---------------------------------------------------------------------------
# Bybit v5 private WS (order topic)
# ---------------------------------------------------------------------------

class BybitPrivateWS(_BaseWSClient):
    """
    Client WS privé Bybit — multi-endpoints + failover rotatif.
    Émet les events "order" → callback Hub (ACK/PARTIAL/FILLED/etc.)
    """
    def __init__(
        self,
        cfg: Any,
        alias: str,
        api_key: str,
        secret: str,
        ws_url: str | None = "wss://stream.bybit.com/v5/private",
        ws_urls: List[str] | None = None,
    ):
        super().__init__(cfg, "BYBIT", alias)
        self.api_key = api_key
        self.secret = secret.encode()


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

        base_s = float(getattr(self, "_bo_base_s", 1.0) or 1.0)
        max_s = float(getattr(self, "_bo_max_s", 30.0) or 30.0)
        jitter_s = max(0.0, float(getattr(self, "_jitter_ms", 50)) / 1000.0)
        heartbeat_s = int(getattr(self, "_ping_s", 20) or 20)

        backoff_s = base_s
        while self._running:
            base = self._ws_urls[self._ws_ix % len(self._ws_urls)]
            try:
                async with self._session.ws_connect(base, heartbeat=heartbeat_s) as ws:
                    self._ws = ws
                    ts_ms = int(_now() * 1000)
                    sign = self._sign(ts_ms, self.api_key, self.secret)
                    await ws.send_json(
                        {"op": "auth", "args": [self.api_key, ts_ms, sign, "5000"]}
                    )
                    await ws.send_json({"op": "subscribe", "args": ["order"]})
                    log.info("[BYBIT:%s] WS connected & subscribed (%s)", self.alias, base)
                    backoff_s = base_s

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
                    WS_FAILOVER_TOTAL.labels("BYBIT", _upper(self.alias)).inc()
                except Exception:
                    pass
                self._ws_ix = (self._ws_ix + 1) % len(self._ws_urls)

                delay = min(
                    max_s,
                    backoff_s + (random.random() * jitter_s),
                )
                try:
                    PWS_RECONNECTS_TOTAL.labels(
                        exchange="BYBIT",
                        alias=_upper(self.alias),
                    ).inc()
                except Exception:
                    pass
                log.exception(
                    "[BYBIT:%s] reconnect in %.2fs (next endpoint idx=%d)",
                    _upper(self.alias),
                    delay,
                    self._ws_ix,
                )
                await asyncio.sleep(delay)
                backoff_s = min(backoff_s * 2.0, max_s)


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
    def __init__(
        self,
        cfg: Any,
        alias: str,
        *,
        ws_urls=None,
        auth_msg_factory=None,
        subscribe_payload=None,
    ):
        super().__init__(cfg, "COINBASE", alias)
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

        base_s = float(getattr(self, "_bo_base_s", 1.0) or 1.0)
        max_s = float(getattr(self, "_bo_max_s", 30.0) or 30.0)
        jitter_s = max(0.0, float(getattr(self, "_jitter_ms", 50)) / 1000.0)
        heartbeat_s = int(getattr(self, "_ping_s", 20) or 20)

        backoff = base_s
        while self._running:
            try:
                base = self.ws_urls[self._ws_idx % len(self.ws_urls)]
                async with self._session.ws_connect(base, heartbeat=heartbeat_s) as ws:
                    self._ws = ws
                    # auth si fournie
                    try:
                        import time
                        if callable(self._auth_msg_factory):
                            ts = int(time.time() * 1000)
                            await ws.send_json(self._auth_msg_factory(ts))
                    except Exception:
                        log.warning(
                            "[COINBASE:%s] auth_msg_factory failed; continuing unauth",
                            self.alias,
                        )
                    # subscribe
                    await ws.send_json(self._subscribe_payload)
                    log.info(
                        "[COINBASE:%s] WS connected & subscribed (ACK only)",
                        self.alias,
                    )
                    backoff = base_s

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            ...
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
                        WS_FAILOVER_TOTAL.labels(
                            exchange="COINBASE",
                            alias=_upper(self.alias),
                            endpoint=new,
                        ).inc()
                except Exception:
                    pass

                delay = min(
                    max_s,
                    backoff + (random.random() * jitter_s),
                )
                try:
                    PWS_RECONNECTS_TOTAL.labels(
                        exchange="COINBASE",
                        alias=_upper(self.alias),
                    ).inc()
                except Exception:
                    pass
                log.exception(
                    "[COINBASE:%s] reconnect in %.2fs",
                    _upper(self.alias),
                    delay,
                )
                await asyncio.sleep(delay)
                backoff = min(backoff * 2.0, max_s)


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

    def register_callback(self, cb, *, role: str = "engine") -> None:
        if not cb:
            return
        kind = str(role or "engine").lower()
        if kind == "engine":
            prev = getattr(self, "_callback", None)
            if self._rm_callback is None and prev is not None and prev is not cb:
                self._rm_callback = prev
            self._callback = cb
            return
        if kind in ("risk", "rm", "riskmanager"):
            self._rm_callback = cb
            return
        raise ValueError(f"unknown callback role={role}")


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
        # Callbacks wiring (RM / Engine / observers)
        self._callback = on_event
        self._rm_callback: Optional[Callable[[dict], None]] = None
        self._observers: List[Callable[[dict], Any]] = []
        # Indicateur compat: True si _rm_callback a été déduit automatiquement
        # à partir d'un précédent callback "engine" (mode test uniquement).
        self._wiring_auto_rm_from_engine: bool = False

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
                self.config,
                alias=alias,
                ws_urls=ws_urls,
                auth_msg_factory=auth_msg_factory,
                subscribe_payload=subscribe_payload,
            )
            if alias in self.cb_ack_ws_by_alias:
                log.warning(
                    "[PrivateWSHub] overriding Coinbase ACK client for alias %s", alias
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
                    c = CoinbaseOrdersWS(self.config, alias=alias, ws_urls=default_urls)

                    c.register_callback(self._dispatch(alias, "COINBASE"))
                    self.cb_ack_ws_by_alias[alias] = c
        except Exception:
            # pas bloquant
            pass

        # Transferts
        self._transfer_clients: Dict[str, Any] = {}
        if transfer_clients:
            self.connect_transfer_clients(transfer_clients)


        # Instantiation par alias — TT/TM/MM ou autre
        for al, creds in (binance_accounts or {}).items():
            alias = _upper(al)
            c = BinanceUserStream(
                self.config,  # cfg = BotConfig ou équivalent
                alias=alias,
                api_key=creds["api_key"],
                rest_base=self._binance_rest_base,
                ws_base=self._binance_ws_priv,
            )
            c.register_callback(self._dispatch(alias, "BINANCE"))
            self.binance_by_alias[alias] = c

        # === Clients Bybit par alias (multi-endpoints supportés) ===============
        # === Clients Bybit par alias (multi-endpoints supportés) ===============
        for al, creds in (bybit_accounts or {}).items():
            alias = _upper(al)
            urls_cfg = getattr(self.config, "bybit_ws_private_urls", None)
            if urls_cfg and isinstance(urls_cfg, (list, tuple)) and len(urls_cfg) > 0:
                c = BybitPrivateWS(
                    self.config,
                    alias=alias,
                    api_key=creds["api_key"],
                    secret=creds["secret"],
                    ws_urls=list(urls_cfg),
                )
            else:
                c = BybitPrivateWS(
                    self.config,
                    alias=alias,
                    api_key=creds["api_key"],
                    secret=creds["secret"],
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
        # Source de vérité PWS : privilégier cfg.pws si présent, sinon cfg (legacy)
        self._pws_cfg = getattr(self.config, "pws", self.config)


        # Priorités QoS (plus petit = plus prioritaire)
        if not hasattr(self, "_priority_map"):
            self._priority_map = {"fill": 0, "ack": 1, "reject": 2, "other": 3}

        # Files par (exchange, alias)
        if not hasattr(self, "_queues"):
            from collections import defaultdict, deque
            qmax = int(getattr(self.cfg, "PWS_QUEUE_MAXLEN", 1000))
            self._queue_max = qmax
            self._queues = defaultdict(deque)

        if not hasattr(self, "_queue_max"):
            self._queue_max = int(getattr(self.cfg, "PWS_QUEUE_MAXLEN", 1000))

        # Heartbeats
        if not hasattr(self, "_last_hb"):
            self._last_hb = {}

        # Paramètres ping/backoff (défauts sûrs)
        pws = getattr(self, "_pws_cfg", self.cfg)
        self._ping_interval_s = float(getattr(pws, "PWS_PING_INTERVAL_S", 10.0))
        self._pong_timeout_s = float(getattr(pws, "PWS_PONG_TIMEOUT_S", 3.0))
        self._backoff_base_ms = int(getattr(pws, "PWS_BACKOFF_BASE_MS", 200))
        self._backoff_max_ms = int(getattr(pws, "PWS_BACKOFF_MAX_MS", 5000))
        self._queue_backpressure_ms = int(getattr(pws, "PWS_QUEUE_BACKPRESSURE_MS", 50))
        self._critical_drop_seen = bool(getattr(self, "_critical_drop_seen", False))


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

            # Gap heartbeat PWS (SLO privé par exchange + fallback global)
            self._pws_gap_slo_by_ex: Dict[str, float] = {}
            pws = getattr(self, "_pws_cfg", self.cfg)
            self._pws_gap_slo_default: float = float(getattr(pws, "PWS_HEARTBEAT_MAX_GAP_S", 5.0))
            self._refresh_pws_gap_slo_from_slo()

            # Health state par (exchange, alias)
            self._pws_health: Dict[Tuple[str, str], Dict[str, Any]] = {}

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

    def emit(self, event: dict, dedup: bool = True) -> None:
        """Publie un événement privé en appliquant la dédup (par défaut)."""
        if dedup:
            self._dispatch(event)
        else:
            self.on_event(event)

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

        etype = str(ev.get("type") or "").lower()

        # Normalise exchange/alias dans l'événement
        ev.setdefault("exchange", exu)
        ev.setdefault("alias", alu)

        # Validation minimale du contrat FILL/PARTIAL pour les événements type="fill"
        if etype == "fill" and status in ("FILL", "PARTIAL"):
            required = ("symbol", "side", "fill_px", "base_qty", "quote", "quote_qty")
            missing = [name for name in required if ev.get(name) in (None, "")]
            has_id = bool(ev.get("client_id") or ev.get("exchange_order_id"))
            if missing or not has_id:
                reason = "invalid_fill_contract"
                try:
                    PWS_DROPPED_TOTAL.labels(exu, alu, reason).inc()
                except Exception:
                    pass
                try:
                    log.warning(
                        "[PrivateWSHub] évènement FILL/PARTIAL invalide, drop: "
                        "missing=%s has_id=%s ev_head=%s",
                        missing,
                        has_id,
                        {
                            "exchange": ev.get("exchange"),
                            "alias": ev.get("alias"),
                            "symbol": ev.get("symbol"),
                            "side": ev.get("side"),
                            "status": ev.get("status"),
                            "type": ev.get("type"),
                            "client_id": ev.get("client_id"),
                            "exchange_order_id": ev.get("exchange_order_id"),
                        },
                    )
                except Exception:
                    pass
                return

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
        cap = int(getattr(self, "_queue_max", 0) or 0)

        try:
            if pri <= 1:
                if cap > 0 and len(q) >= cap:
                    timeout_s = max(0.0, float(self._queue_backpressure_ms) / 1000.0)
                    deadline = _t.time() + timeout_s
                    while len(q) >= cap and _t.time() < deadline:
                        try:
                            self._pws_drain_one(key)
                        except Exception:
                            break
                        if len(q) >= cap:
                            _t.sleep(0.001)
                if cap > 0 and len(q) >= cap:
                    reason_code = normalize_reason_code(
                        "PWS_QUEUE_BACKPRESSURE_TIMEOUT") or "PWS_QUEUE_BACKPRESSURE_TIMEOUT"
                    try:
                        PWS_DROPPED_TOTAL.labels(exu, alu, reason_code).inc()
                    except Exception:
                        pass
                    try:
                        from modules.obs_metrics import inc_blocked
                        inc_blocked("private_ws_hub", reason_code, None)
                    except Exception:
                        pass
                    self._critical_drop_seen = True
                    self._notify_alert(
                        severity="CRITICAL",
                        reason=reason_code,
                        exchange=exu,
                        alias=alu,
                        kind=cat,
                        message="PrivateWS queue saturated for critical ACK/FILL",
                    )
                    return
                q.appendleft(ev)  # fills/acks en tête
            else:
                if cap > 0 and len(q) >= cap and pri >= 2:
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
        En mode PROD, on s'attend à ce que RM ET Engine soient câblés ; un manque
        de callback est traité comme wiring incomplet (métriques + log).
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
            ex = ex if "ex" in locals() else "-"
            al = al if "al" in locals() else "-"
            kind = kind if "kind" in locals() else "-"

        # 1) callback RM / Engine (wiring check)
        rm_cb = getattr(self, "_rm_callback", None)
        eng_cb = getattr(self, "_callback", None)

        # Wiring incomplet → alerte explicite (prod / test visibles)
        try:
            if rm_cb is None:
                self._notify_alert(
                    severity="ERROR",
                    reason="missing_rm_callback",
                    exchange=ex,
                    alias=al,
                    kind=kind,
                    message="PrivateWSHub: événement privé sans callback RiskManager",
                )
            if eng_cb is None:
                self._notify_alert(
                    severity="ERROR",
                    reason="missing_engine_callback",
                    exchange=ex,
                    alias=al,
                    kind=kind,
                    message="PrivateWSHub: événement privé sans callback ExecutionEngine",
                )
        except Exception:
            pass

        self._deliver_callback(rm_cb, ev, label="RM")

        # 2) callback Engine (sync)
        self._deliver_callback(eng_cb, ev, label="Engine")

        # 3) Observateurs additionnels (balance fetcher, watchdogs…)
        for cb in list(self._observers):
            self._deliver_callback(cb, ev, label="Observer")


    def _deliver_callback(self, cb: Optional[Callable[[dict], Any]], ev: dict, label: str) -> None:
        if not cb:
            return
        try:
            result = cb(ev)
            if inspect.isawaitable(result):
                try:
                    task = asyncio.create_task(result)
                    task.add_done_callback(
                        lambda t, lbl=label: _log_callback_exception(t, lbl)
                    )
                except RuntimeError:
                    log.warning(
                        "[PrivateWSHub] %s callback coroutine sans boucle active", label
                    )
        except Exception:
            log.exception("[PrivateWSHub] %s callback failed", label)


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

        Contrat wiring:
          - role="engine": callback unique pour l'ExecutionEngine
            (on_private_order_update).
          - role="risk"|"rm"|"riskmanager": callback unique pour le RiskManager
            (on_private_event).
          - role="observer": callbacks additionnels (fan-out lecture seule).

        En PROD, on s'attend à avoir AU MOINS un callback "engine" ET un
        callback "risk". Le fallback qui réutilise un ancien callback "engine"
        comme "_rm_callback" est conservé pour compatibilité/tests uniquement
        et marqué via self._wiring_auto_rm_from_engine.
        """
        if not cb:
            return

        kind = str(role or "engine").lower()

        if kind == "engine":
            prev = getattr(self, "_callback", None)
            if prev is not None and prev is not cb and getattr(self, "_rm_callback", None) is None:
                # Mode compat: on promeut l'ancien callback "engine" en RM si aucun
                # callback RM n'est encore enregistré. Marqué comme auto-wiring.
                self._rm_callback = prev
                self._wiring_auto_rm_from_engine = True
                try:
                    log.warning(
                        "[PrivateWSHub] auto-wiring précédent callback engine en RM "
                        "(mode compat/test uniquement)"
                    )
                except Exception:
                    pass
                try:
                    # Alerte observable pour les dashboards (wiring incomplet).
                    self._notify_alert(
                        severity="WARNING",
                        reason="auto_rm_from_engine",
                        exchange="-",
                        alias="-",
                        kind="wiring",
                        message="RM callback déduit automatiquement depuis engine (compat/test)",
                    )
                except Exception:
                    pass
            self._callback = cb
            return

        if kind in ("risk", "rm", "riskmanager"):
            self._rm_callback = cb
            self._wiring_auto_rm_from_engine = False
            return

        if kind == "observer":
            self._observers.append(cb)
            return

        raise ValueError(f"unknown callback role={role!r}")

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

    def register_alert(
            self,
            severity: str,
            reason: str,
            message: str,
            *,
            exchange: str = "-",
            alias: str = "-",
            kind: str = "-",
    ) -> None:
        """Émet une alerte explicite (tests/unitaires, monitoring externe)."""
        self._notify_alert(
            severity=severity,
            reason=reason,
            exchange=exchange,
            alias=alias,
            kind=kind,
            message=message,
        )

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
        - Lit PWS_BACKOFF_BASE_MS / PWS_BACKOFF_MAX_MS depuis cfg.pws (fallback cfg).
        - DEGRADED: cap ≤ 2000 ms ; SEVERE: cap ≤ 800 ms.
        - base_ms ≥ 50 ms, cap_ms ≥ base_ms.
        """
        pws = getattr(self, "_pws_cfg", self.cfg)
        base_ms = int(getattr(pws, "PWS_BACKOFF_BASE_MS", 200))
        cap_ms = int(getattr(pws, "PWS_BACKOFF_MAX_MS", 5000))
        base_ms = max(50, base_ms)
        cap_ms = max(base_ms, cap_ms)
        try:
            state = str(getattr(pws, f"PWS_PACER_{region}", "NORMAL")).upper()
            if state == "DEGRADED":
                cap_ms = min(cap_ms, 2000)
            elif state == "SEVERE":
                cap_ms = min(cap_ms, 800)
        except Exception:
            pass
        return base_ms, cap_ms

    def _refresh_pws_gap_slo_from_slo(self) -> None:
        """Construit le cache de gaps heartbeat par exchange depuis cfg.slo."""
        self._pws_gap_slo_by_ex.clear()

        pws = getattr(self, "_pws_cfg", self.cfg)
        self._pws_gap_slo_default = float(getattr(pws, "PWS_HEARTBEAT_MAX_GAP_S", 5.0))

        cfg = getattr(self, "cfg", None)
        slo_map = getattr(cfg, "slo", None)
        g = getattr(cfg, "g", None)
        mode_key = str(getattr(g, "deployment_mode", "SPLIT")).upper()
        per_ex = slo_map.get(mode_key) if slo_map else {}

        for ex, path_slo in (per_ex or {}).items():
            exu = str(ex).upper()
            pvt = getattr(path_slo, "private", None)
            if pvt is None:
                continue
            gap = float(getattr(pvt, "pws_heartbeat_gap_max_s", 0.0) or 0.0)
            if gap > 0.0:
                self._pws_gap_slo_by_ex[exu] = gap

        if not self._pws_gap_slo_by_ex:
            try:
                log.warning(
                    "[PrivateWSHub] aucun pws_heartbeat_gap_max_s dans cfg.slo[mode=%s], "
                    "fallback sur PWS_HEARTBEAT_MAX_GAP_S",
                    mode_key,
                )
            except Exception:
                pass

    async def run_alerts(self) -> None:
        """
               Boucle d'alerting (queue saturation + heartbeat gaps).
               Seuils côté config:
                   - PWS_QUEUE_SATURATION_RATIO (def 0.85)
                   - PWS_HEARTBEAT_MAX_GAP_S (def 5.0)
                   - PWS_ALERT_PERIOD_S (def 5.0)
               """
        pws = getattr(self, "_pws_cfg", self.cfg)
        ratio_thr = float(getattr(pws, "PWS_QUEUE_SATURATION_RATIO", 0.85))
        period = float(getattr(pws, "PWS_ALERT_PERIOD_S", 5.0))

        while getattr(self, "_started", False):
            now = _now()

            # 1) Heartbeat gaps
            try:
                for (ex, al), last in list(self._last_hb.items()):
                    gap = max(0.0, now - float(last))
                    hb_crit = self._pws_gap_slo_by_ex.get(ex, self._pws_gap_slo_default)
                    hb_warn = 0.7 * hb_crit

                    entry = self._pws_health.get((ex, al)) or {
                        "state": "HEALTHY",
                        "last_gap_s": 0.0,
                        "gap_slo_s": hb_crit,
                        "last_change_ts": now,
                        "healthy_streak": 0,
                        "warn_streak": 0,
                        "critical_streak": 0,
                    }

                    if gap <= hb_warn:
                        entry["healthy_streak"] = entry.get("healthy_streak", 0) + 1
                        entry["warn_streak"] = 0
                        entry["critical_streak"] = 0
                    elif gap <= hb_crit:
                        entry["warn_streak"] = entry.get("warn_streak", 0) + 1
                        entry["healthy_streak"] = 0
                        entry["critical_streak"] = 0
                    else:
                        entry["critical_streak"] = entry.get("critical_streak", 0) + 1
                        entry["healthy_streak"] = 0
                        entry["warn_streak"] = 0

                    old_state = str(entry.get("state", "HEALTHY"))
                    new_state = old_state

                    if entry.get("critical_streak", 0) >= 1 and gap > hb_crit:
                        new_state = "CRITICAL"
                    elif entry.get("warn_streak", 0) >= 2 and gap > hb_warn:
                        new_state = "WARN"
                    elif entry.get("healthy_streak", 0) >= 2 and gap <= hb_warn:
                        new_state = "HEALTHY"

                    entry["last_gap_s"] = gap
                    entry["gap_slo_s"] = hb_crit

                    if new_state != old_state:
                        entry["state"] = new_state
                        entry["last_change_ts"] = now
                    self._pws_health[(ex, al)] = entry

                    try:
                        PWS_HEARTBEAT_GAP_SECONDS.labels(ex, al).set(gap)
                        PWS_HEARTBEAT_GAP_SLO_SECONDS.labels(ex, al).set(hb_crit)
                        PWS_HEALTH_STATE.labels(ex, al).set(
                            0 if new_state == "HEALTHY" else 1 if new_state == "WARN" else 2
                        )
                    except Exception:
                        pass

                    if new_state == "CRITICAL" and old_state != "CRITICAL":
                        try:
                            PWS_HEARTBEAT_GAP_BREACH_TOTAL.labels(ex, al).inc()
                        except Exception:
                            pass
                        self._notify_alert(
                            severity="WARN",
                            reason="heartbeat_gap",
                            exchange=ex, alias=al, kind="-",
                            message=f"PrivateWS {ex}/{al} CRITICAL: gap={gap:.2f}s > slo={hb_crit:.2f}s",
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
        pws = getattr(self, "_pws_cfg", self.cfg)
        stable_s = float(getattr(pws, "PWS_STABLE_RESET_S", 30.0))
        jitter_ms = int(getattr(pws, "PWS_JITTER_MS", 50))

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
        en appliquant seulement le PACER + pools régionaux sur le "connect".
        La logique de backoff / reconnect vit dans les clients eux-mêmes.
        Lance ensuite la sonde heartbeat 1 Hz.
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
                # Le Hub borne juste la concurrence + applique le PACER.
                # La logique de backoff / reconnect vit dans le client (start/_run_ws/_run).
                async with pool:
                    await _connect_once()

            # Démarrage simple : les clients gèrent eux-mêmes le backoff / reconnect
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

        for c in getattr(self, "cb_ack_ws_by_alias", {}).values():
            tasks.append(asyncio.create_task(c.stop()))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        for t_attr in ("_hb_task", "_alerts_task"):
            t = getattr(self, t_attr, None)
            if t:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
                setattr(self, t_attr, None)

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

    def _emit_transfer_event(
            self,
            *,
            subtype: str,
            exchange: str,
            alias: Optional[str],
            status: str,
            payload: Dict[str, Any],
            result: Any = None,
            error: Optional[str] = None,
    ) -> None:
        """Émet un évènement de transfert interne (wallet ou subaccount).

        Contrat (cf. docstring module & Ticket 8) :
          - type="transfer"
          - subtype="wallet"|"subaccount"
          - exchange="BINANCE"/"BYBIT"/"COINBASE"
          - alias: alias focus (dest alias pour subtype="subaccount")
          - ccy, amount: devise & montant en ccy
          - from_alias / to_alias: pour subtype="subaccount"
          - from_wallet / to_wallet: pour subtype="wallet"
          - payload: copie brute du payload passé au client
        """
        subtype_norm = str(subtype).lower()
        exu = _upper(exchange)
        alias_u = _upper(alias) if alias else None

        base_payload = dict(payload or {})
        try:
            ccy = _upper(base_payload.get("ccy")) if base_payload.get("ccy") else None
        except Exception:
            ccy = None
        try:
            amount = float(base_payload.get("amount") or 0.0)
        except Exception:
            amount = 0.0

        from_alias: Optional[str] = None
        to_alias: Optional[str] = None
        from_wallet: Optional[str] = None
        to_wallet: Optional[str] = None

        if subtype_norm == "wallet":
            # Transfert intra-wallet pour un alias logique (TT/TM/MM).
            try:
                from_wallet = _upper(base_payload.get("from_wallet")) if base_payload.get("from_wallet") else None
            except Exception:
                from_wallet = None
            try:
                to_wallet = _upper(base_payload.get("to_wallet")) if base_payload.get("to_wallet") else None
            except Exception:
                to_wallet = None

            if alias_u is None:
                try:
                    alias_u = _upper(base_payload.get("alias")) if base_payload.get("alias") else None
                except Exception:
                    alias_u = None
            from_alias = alias_u
            to_alias = alias_u

        elif subtype_norm == "subaccount":
            # Transfert entre alias (TT ↔ TM) sur un même exchange.
            try:
                from_alias = _upper(base_payload.get("from_alias")) if base_payload.get("from_alias") else None
            except Exception:
                from_alias = None
            try:
                to_alias = _upper(base_payload.get("to_alias")) if base_payload.get("to_alias") else None
            except Exception:
                to_alias = None
            if alias_u is None:
                alias_u = to_alias or from_alias

        ev = {
            "type": "transfer",
            "subtype": subtype_norm,  # "wallet" | "subaccount"
            "exchange": exu,
            "alias": alias_u,
            "status": _upper(status),
            "ccy": ccy,
            "amount": amount,
            "from_alias": from_alias,
            "to_alias": to_alias,
            "from_wallet": from_wallet,
            "to_wallet": to_wallet,
            "payload": base_payload,
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
            log.info(
                "[PrivateWSHub] %s",
                json.dumps(
                    {
                        "pws_transfer": True,
                        "exchange": ev["exchange"],
                        "alias": ev["alias"],
                        "subtype": ev["subtype"],
                        "status": ev["status"],
                        "ccy": ev.get("ccy"),
                        "amount": ev.get("amount"),
                        "from_alias": ev.get("from_alias"),
                        "to_alias": ev.get("to_alias"),
                        "from_wallet": ev.get("from_wallet"),
                        "to_wallet": ev.get("to_wallet"),
                        "error": ev.get("error"),
                    }
                ),
            )
        except Exception:
            pass

        if self._callback:
            try:
                self._callback(ev)
            except Exception:
                log.exception("[PrivateWSHub] transfer callback error")


    async def transfer_wallet(self, *, ex: str, alias: str, from_wallet: str, to_wallet: str,
                              ccy: str, amount: float) -> Dict[str, Any]:
        """
               Transfert intra-wallet (ex: FUNDING ↔ SPOT) pour un alias logique (TT/TM/MM).

               Contrat:
                 - ex: "BINANCE" | "BYBIT" | "COINBASE"
                 - alias: alias logique (TT/TM/MM...) côté bot
                 - from_wallet / to_wallet: noms de wallet CEX (SPOT/FUNDING...), uppercased
                 - ccy, amount: devise et montant transféré (en ccy)

               Évènement émis vers RM (cf. _emit_transfer_event):
                 - subtype="wallet"
                 - exchange=ex, alias=alias
                 - payload={"ex","alias","from_wallet","to_wallet","ccy","amount"}
                 - champs dérivés: from_alias=alias, to_alias=alias
               """
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
        """
                Transfert entre sous-comptes / alias (TT ↔ TM) pour un même exchange.

                Contrat:
                  - ex: "BINANCE" | "BYBIT" | "COINBASE"
                  - from_alias / to_alias: alias logiques TT/TM/MM..., uppercased
                  - ccy, amount: devise et montant transféré (en ccy)

                Évènement émis vers RM (cf. _emit_transfer_event):
                  - subtype="subaccount"
                  - exchange=ex
                  - alias = to_alias (alias focus = destination)
                  - payload={"ex","from_alias","to_alias","ccy","amount"}
                  - champs dérivés: from_alias / to_alias remplis au top-level de l'event
                """
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

    # --------------------- WS health par alias ---------------------------

    def _collect_ws_client_status_for_alias(self, exchange: str, alias: str) -> Dict[str, Dict[str, Any]]:
        """
        Regroupe les statuts bruts des clients WS/poller pour un (exchange, alias).

        Retourne un dict par type de client, ex:
          {
            "orders_ws": {...},     # Binance / Bybit / Coinbase-ACK
            "rest_poller": {...},   # CoinbaseATPoller
          }
        Les valeurs sont directement celles de client.get_status().
        """
        exu = str(exchange).upper()
        alu = _upper(alias)
        clients: Dict[str, Dict[str, Any]] = {}

        # BINANCE: un seul user-stream privé par alias
        if exu == "BINANCE":
            c = self.binance_by_alias.get(alu)
            if c is not None:
                try:
                    clients["orders_ws"] = c.get_status()
                except Exception:
                    log.exception("[PrivateWSHub] get_status BINANCE %s failed", alu)

        # BYBIT: un client privé multi-endpoints par alias
        elif exu == "BYBIT":
            c = self.bybit_by_alias.get(alu)
            if c is not None:
                try:
                    clients["orders_ws"] = c.get_status()
                except Exception:
                    log.exception("[PrivateWSHub] get_status BYBIT %s failed", alu)

        # COINBASE: REST poller (source principale) + WS ACK optionnel
        elif exu in ("COINBASE", "COINBASE_AT"):
            p = self.cb_poller_by_alias.get(alu)
            if p is not None:
                try:
                    clients["rest_poller"] = p.get_status()
                except Exception:
                    log.exception("[PrivateWSHub] get_status Coinbase poller %s failed", alu)
            w = self.cb_ack_ws_by_alias.get(alu)
            if w is not None:
                try:
                    clients["orders_ws"] = w.get_status()
                except Exception:
                    log.exception("[PrivateWSHub] get_status Coinbase ACK %s failed", alu)
        else:
            # Exchange inconnu côté Hub
            pass

        return clients

    def get_alias_ws_status_snapshot(self, exchange: str, alias: str) -> Dict[str, Any]:
        """
        Snapshot de santé WS "alias-centré" pour (exchange, alias).

        Utilisé par:
          - RiskManager / Boot (vue alias),
          - MultiBalanceFetcher (marquer un alias 'capital_at_risk'),
          - Observabilité (hub.get_status).

        Statut agrégé:
          - status: "WS_OK" | "WS_DEGRADED" | "WS_DOWN" | "WS_UNKNOWN"
          - healthy: bool global (au moins un client 'healthy')
            * WS_DEGRADED : trafic vu récemment mais gap/error/reconnects élevés
              ou pas encore de trafic (boot). Le flux reste partiellement
              exploitable.
            * WS_DOWN : aucun client healthy ou gap très élevé → alias considéré
              indisponible tant que la situation persiste.
        """

        exu = str(exchange).upper()
        alu = _upper(alias)
        clients = self._collect_ws_client_status_for_alias(exu, alu)

        # Aucune source pour cet alias
        if not clients:
            return {
                "exchange": exu,
                "alias": alu,
                "status": "WS_UNKNOWN",
                "healthy": False,
                "heartbeat_gap_s": None,
                "last_event_ts": None,
                "errors_total": 0,
                "reconnects_total": 0,
                "clients": {},
            }

        now = _now()
        last_ts_vals = []
        errors_total = 0
        reconnects_total = 0
        any_healthy = False

        for _kind, st in clients.items():
            if not isinstance(st, dict):
                continue
            ts = _safe_float(st.get("last_event_ts"), 0.0)
            if ts > 0.0:
                last_ts_vals.append(ts)
            err = st.get("errors", 0) or 0
            rec = st.get("reconnects", 0) or 0
            try:
                errors_total += int(err)
            except Exception:
                pass
            try:
                reconnects_total += int(rec)
            except Exception:
                pass
            if st.get("healthy"):
                any_healthy = True

        last_ts = max(last_ts_vals) if last_ts_vals else None
        heartbeat_gap_s = (now - last_ts) if last_ts is not None else None

        # Heuristiques simples (config-driven, fallback sur les valeurs historiques)
        pws_cfg = getattr(self, "_pws_cfg", getattr(self, "config", None))
        soft_default = 60.0  # au-delà, on considère l'alias dégradé
        hard_default = 180.0  # au-delà, on considère l'alias DOWN
        error_default = 10
        reconnect_default = 20

        soft_gap_s = float(getattr(pws_cfg, "PWS_STATUS_SOFT_GAP_S", soft_default) or soft_default)
        hard_gap_s = float(getattr(pws_cfg, "PWS_STATUS_HARD_GAP_S", hard_default) or hard_default)
        err_thr = int(getattr(pws_cfg, "PWS_STATUS_ERROR_THRESHOLD", error_default) or error_default)
        reconnect_thr = int(getattr(pws_cfg, "PWS_STATUS_RECONNECT_THRESHOLD", reconnect_default) or reconnect_default)


        gap = heartbeat_gap_s if heartbeat_gap_s is not None else None

        if gap is None:
            # Pas encore d'events; on considère l'alias comme dégradé tant qu'on n'a pas vu de trafic
            if any_healthy:
                status = "WS_DEGRADED"
                healthy = True
            else:
                status = "WS_DEGRADED"
                healthy = False
        else:
            if (not any_healthy) or (gap >= hard_gap_s):
                status = "WS_DOWN"
                healthy = False
            elif (gap >= soft_gap_s) or (errors_total >= err_thr) or (reconnects_total >= reconnect_thr):
                status = "WS_DEGRADED"
                healthy = True
            else:
                status = "WS_OK"
                healthy = True

        return {
            "exchange": exu,
            "alias": alu,
            "status": status,
            "healthy": healthy,
            "heartbeat_gap_s": heartbeat_gap_s,
            "last_event_ts": last_ts,
            "errors_total": errors_total,
            "reconnects_total": reconnects_total,
            "clients": clients,
        }

    def get_all_alias_ws_status(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        Snapshot complet par alias:

        {
          "BINANCE": { "TT": {...}, "TM": {...}, ... },
          "BYBIT": { ... },
          "COINBASE": { ... },
        }

        Utilisable par:
          - RiskManager (vue globale alias),
          - watchdog / observabilité,
          - scripts de debug.
        """
        out: Dict[str, Dict[str, Dict[str, Any]]] = {}

        # BINANCE
        for al in self.binance_by_alias.keys():
            alu = _upper(al)
            out.setdefault("BINANCE", {})[alu] = self.get_alias_ws_status_snapshot("BINANCE", alu)

        # BYBIT
        for al in self.bybit_by_alias.keys():
            alu = _upper(al)
            out.setdefault("BYBIT", {})[alu] = self.get_alias_ws_status_snapshot("BYBIT", alu)

        # COINBASE (union poller + ACK)
        cb_aliases = set(list(self.cb_poller_by_alias.keys()) + list(self.cb_ack_ws_by_alias.keys()))
        for al in cb_aliases:
            alu = _upper(al)
            out.setdefault("COINBASE", {})[alu] = self.get_alias_ws_status_snapshot("COINBASE", alu)

        return out


    # ------------------------- status -----------------------------------------

    def get_status(self) -> Dict[str, Any]:
        """
        Statut synthétique du Hub, y compris wiring callbacks.
        Utilisé par le RiskManager / Boot pour décider du READY global.
        """
        wiring = {
            "has_engine_callback": bool(getattr(self, "_callback", None)),
            "has_rm_callback": bool(getattr(self, "_rm_callback", None)),
            "observer_count": len(getattr(self, "_observers", []) or []),
            "auto_rm_from_engine": bool(getattr(self, "_wiring_auto_rm_from_engine", False)),
        }
        accounts_ws = self.get_all_alias_ws_status()
        health: Dict[str, Dict[str, Any]] = {}
        try:
            for (ex, al), vals in (self._pws_health or {}).items():
                exu = _upper(ex)
                alu = _upper(al)
                health.setdefault(exu, {})[alu] = {
                    "state": vals.get("state", "HEALTHY"),
                    "gap_s": vals.get("last_gap_s", 0.0),
                    "gap_slo_s": vals.get("gap_slo_s", self._pws_gap_slo_by_ex.get(exu, self._pws_gap_slo_default)),
                    "last_change_ts": vals.get("last_change_ts", 0.0),
                }
        except Exception:
            pass
        return {
            "module": "PrivateWSHub",
            "healthy": self._started,
            "critical_drop_seen": bool(getattr(self, "_critical_drop_seen", False)),
            "wiring": wiring,
            "submodules": {
                "binance": {al: c.get_status() for al, c in self.binance_by_alias.items()},
                "bybit": {al: c.get_status() for al, c in self.bybit_by_alias.items()},
                "coinbase": {al: c.get_status() for al, c in self.cb_poller_by_alias.items()},
            },
            "accounts_ws": accounts_ws,
            "transfers": {
                "available": sorted(self._transfer_clients.keys()),
                "has_binance": "BINANCE" in self._transfer_clients,
                "has_bybit": "BYBIT" in self._transfer_clients,
                "has_coinbase": "COINBASE" in self._transfer_clients or "COINBASE_AT" in self._transfer_clients,
            },
            "private_ws_health": health,
        }
