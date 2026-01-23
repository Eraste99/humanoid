# -*- coding: utf-8 -*-
"""
RPC Gateway (EU<->US) — P0 ready
--------------------------------
- HTTP RPC via aiohttp (POST /rpc/{method})
- Endpoints cibles (côté server): submit_bundle, cancel, status, stream (SSE)
- Budgets: timeout 1.5–2.0s (configurable via BotConfig), retry ≤ 2 (expo + jitter)
- mTLS: côté serveur (CERT_REQUIRED) et côté client (client cert)
- Idempotence stricte: X-Idempotency-Key ou dérivée (client_order_id/bundle_id/trace_id/ts_ns)
- Observabilité:
    * RPC_LATENCY_MS{method,region}
    * RPC_ERR_TOTAL{code,method,region}
    * RPC_RETRIES_TOTAL{method,region}
    * rpc_payload_rejected_total (Counter)
- Aligné BotConfig: tous les paramètres pris de cfg (pas de os.getenv ici)
- Pas de dépendance dure aux modèles: payload = dict ou objet; le handler applicatif gère la validation.

Dépendances:
- aiohttp (server & client)
- ssl (stdlib)
"""

from __future__ import annotations
import asyncio
import contextlib

from modules.utils import json_utils as json
import logging
import ssl
import time
import random
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Optional, NamedTuple, Dict

try:
    from aiohttp import web, ClientSession, ClientTimeout
except Exception as e:  # pragma: no cover
    class _DummyJSONResponse:
        def __init__(self, payload=None, status=200):
            self.payload = payload
            self.status = status


    class _DummyWeb:
        def __getattr__(self, name):
            if name == "middleware":
                return lambda fn: fn
            if name == "json_response":
                return lambda payload=None, status=200: _DummyJSONResponse(payload, status)
            if name == "Request":
                return object
            if name == "Application":
                return object
            if name == "AppRunner":
                return object
            if name == "TCPSite":
                return object
            raise RuntimeError("aiohttp unavailable")

    class _DummySession:
        def __init__(self, *a, **k):
            raise RuntimeError("aiohttp unavailable")

    class _DummyTimeout:
        def __init__(self, *a, **k):
            raise RuntimeError("aiohttp unavailable")

    web, ClientSession, ClientTimeout = _DummyWeb(), _DummySession, _DummyTimeout
    # Metrics disabled log once
    print("[rpc_gateway] aiohttp unavailable, falling back to dummy stubs")

from modules.bot_config import BotConfig
from contracts.payloads import validate_submit_bundle_lite, validate_cancel_lite
import modules.obs_metrics as obs  # on accède par getattr avec fallback no-op


# ---------- Metrics helpers (tolérants si les constantes n'existent pas encore) ----------

class _NoopMetric:
    def labels(self, **kw): return self
    def observe(self, *a, **k): return None
    def inc(self, *a, **k): return None
    def set(self, *a, **k): return None

    def time(self):
        import contextlib
        return contextlib.nullcontext()

_METRICS_DISABLED_LOGGED = False
_RPC_WARNED_KEYS: set[str] = set()

def _get_metric(name: str):
    global _METRICS_DISABLED_LOGGED
    metric = getattr(obs, name, None)
    if metric is None:
        if not _METRICS_DISABLED_LOGGED:
            _METRICS_DISABLED_LOGGED = True
            try:
                print("[rpc_gateway] metrics disabled/no-op")
            except Exception:
                pass
        return _NoopMetric()
    return metric
def _warn_once(key: str, msg: str, *, level: int = logging.WARNING) -> None:
    if key in _RPC_WARNED_KEYS:
        return
    _RPC_WARNED_KEYS.add(key)
    try:
        logger.log(level, msg)
    except Exception:
        pass

def _strict_config(cfg: Optional[BotConfig]) -> bool:
    g_cfg = getattr(cfg, "g", None) if cfg is not None else None
    rpc_cfg = getattr(cfg, "rpc", None) if cfg is not None else None
    return bool(
        getattr(g_cfg, "strict_config", False)
        or getattr(rpc_cfg, "strict_config", False)
    )

def _resolve_rpc_cfg(cfg: BotConfig):
    return getattr(cfg, "rpc", cfg)

def _resolve_rpc_region(cfg: BotConfig, default_region: str) -> str:
    g_cfg = getattr(cfg, "g", None)
    g_region = getattr(g_cfg, "region", None) if g_cfg is not None else None
    if g_region:
        return str(g_region).upper()
    if hasattr(cfg, "region"):
        _warn_once("rpc_region_legacy", "[rpc_gateway] cfg.region is deprecated; use cfg.g.region or deployment_mode")
        legacy_region = getattr(cfg, "region", None)
        if legacy_region:
            return str(legacy_region).upper()
    mode = str(getattr(g_cfg, "deployment_mode", "") or "").upper()
    if mode in ("US_ONLY",):
        return "US"
    if mode in ("EU_ONLY", "SPLIT", "JP_ONLY"):
        return "EU"
    _warn_once("rpc_region_fallback", "[rpc_gateway] region missing; using default EU")
    return str(default_region).upper()

def _resolve_rpc_attr(rpc_cfg, cfg: BotConfig, name: str, *, default=None, legacy_root: str | None = None):
    if hasattr(rpc_cfg, name):
        return getattr(rpc_cfg, name)
    if legacy_root and hasattr(cfg, legacy_root):
        _warn_once(
            f"rpc_legacy_{legacy_root}",
            f"[rpc_gateway] {legacy_root} is deprecated; use rpc.{name}",
        )
        return getattr(cfg, legacy_root)
    return default

def _observe(metric, value: float, **labels):
    try:
        metric.labels(**labels).observe(value)
    except Exception:
        try:
            metric.observe(value)  # fallback
        except Exception:
            pass

def _inc(metric, amount: float = 1.0, **labels):
    try:
        metric.labels(**labels).inc(amount)
    except Exception:
        try:
            metric.inc(amount)
        except Exception:
            pass

def _set(metric, value: float, **labels):
    try:
        metric.labels(**labels).set(value)
    except Exception:
        try:
            metric.set(value)
        except Exception:
            pass

RPC_LATENCY_MS = _get_metric("RPC_LATENCY_MS")
RPC_ERR_TOTAL = _get_metric("RPC_ERR_TOTAL")
RPC_RETRIES_TOTAL = _get_metric("RPC_RETRIES_TOTAL")
# Canon P0: RPC_PAYLOAD_REJECTED_TOTAL a le label ["model"]
RPC_PAYLOAD_REJECTED_TOTAL = _get_metric("RPC_PAYLOAD_REJECTED_TOTAL")
RPC_METRICS_DISABLED_TOTAL = _get_metric("RPC_METRICS_DISABLED_TOTAL")
RPC_IDEMPOTENCY_HIT_TOTAL = _get_metric("RPC_IDEMPOTENCY_HIT_TOTAL")


# ---------- Idempotency store (TTL) ----------

class _TTLIdempotencyStore:
    """Simple TTL store (in-mem) pour idempotence stricte des requêtes RPC côté serveur."""
    def __init__(self, maxsize: int = 10000, ttl_s: int = 600):
        self._maxsize = maxsize
        self._ttl_s = ttl_s
        self._store: Dict[str, tuple[float, Optional[dict]]] = {}
        self._lock = asyncio.Lock()

    async def peek(self, key: str) -> tuple[bool, Optional[dict]]:
        now = time.time()
        async with self._lock:
            # GC opportuniste
            if len(self._store) > self._maxsize:
                # drop des plus vieux ~probabiliste (O(n), acceptable P0)
                expired = [k for k, t in self._store.items() if t[0] <= now]
                for k in expired[: self._maxsize // 10 or 1]:
                    self._store.pop(k, None)
            entry = self._store.get(key)
            if entry:
                exp, resp = entry
                if exp > now:
                    return True, resp
                self._store.pop(key, None)
            return False, None

    async def put(self, key: str, response: Optional[dict] = None) -> None:
        now = time.time()
        async with self._lock:
            self._store[key] = (now + self._ttl_s, response)

    async def mark(self, key: str, response: Optional[dict] = None) -> None:
        await self.put(key, response)

# ---------- Handlers interface ----------

class RPCHandlers(NamedTuple):
    submit_bundle: Callable[[Any], Awaitable[Dict[str, Any]]]
    cancel: Callable[[Any], Awaitable[Dict[str, Any]]]
    status: Callable[[Any], Awaitable[Dict[str, Any]]]
    stream: Callable[[Any], AsyncIterator[Dict[str, Any]]]  # SSE


# ---------- Config dataclass (vue RPC extraite de BotConfig) ----------

@dataclass
class RPCSettings:
    enabled: bool
    host: str
    port: int
    region: str  # "EU" | "US"
    timeout_s: float
    max_retries: int  # ≤ 2
    idempotency_ttl_s: int
    memoize_response: bool
    mtls_enabled: bool
    ca_cert: Optional[str]
    server_cert: Optional[str]
    server_key: Optional[str]
    client_cert: Optional[str]
    client_key: Optional[str]
    require_client_cert: bool

    @staticmethod
    def from_cfg(cfg: BotConfig, *, default_host="0.0.0.0", default_port=8443, default_region="EU") -> "RPCSettings":
        # Les noms exacts des clés d'env sont libres côté BotConfig;
        # ici on consomme uniquement les attributs exposés par cfg.
        rpc_cfg = _resolve_rpc_cfg(cfg)
        enabled = _resolve_rpc_attr(rpc_cfg, cfg, "enabled", default=True, legacy_root="rpc_enabled")
        host = _resolve_rpc_attr(rpc_cfg, cfg, "host", default=default_host, legacy_root="rpc_host")
        port = int(getattr(rpc_cfg, "port", default_port))
        region = _resolve_rpc_region(cfg, default_region)
        timeout_ms = getattr(rpc_cfg, "timeout_ms", 0)
        if timeout_ms is not None:
            timeout_ms = int(timeout_ms)
        else:
            timeout_ms = None
        timeout_s = float(getattr(rpc_cfg, "timeout_s", 2.0))
        if timeout_ms is not None:
            try:
                timeout_s = max(timeout_s, float(timeout_ms) / 1000.0)
            except Exception:
                pass
        max_retries = int(min(getattr(rpc_cfg, "max_retries", 2), 2))
        idempotency_ttl_s = int(getattr(rpc_cfg, "rpc_idempotency_ttl_s", 600))
        memoize_response = bool(getattr(rpc_cfg, "rpc_idempotency_memoize_response", True))
        mtls_enabled = bool(getattr(rpc_cfg, "mtls_enabled", True))
        ca_cert = getattr(rpc_cfg, "ca_cert", None)
        server_cert = getattr(rpc_cfg, "server_cert", None)
        server_key = getattr(rpc_cfg, "server_key", None)
        client_cert = getattr(rpc_cfg, "client_cert", None)
        client_key = getattr(rpc_cfg, "client_key", None)
        require_client_cert = bool(getattr(rpc_cfg, "require_client_cert", True))
        g_cfg = getattr(cfg, "g", None)
        mode = str(getattr(g_cfg, "mode", "DRY_RUN")).upper()
        armed = bool(getattr(g_cfg, "live_trading_armed", False))
        if not hasattr(rpc_cfg, "enabled") and mode == "PROD" and armed:
            msg = "[rpc_gateway] rpc.enabled missing in PROD+armed"
            _warn_once("rpc_enabled_missing", msg, level=logging.CRITICAL)
            if _strict_config(cfg):
                raise RuntimeError("rpc.enabled missing in PROD+armed")
        if mtls_enabled and mode == "PROD" and armed:
            if not (ca_cert and server_cert and server_key):
                msg = "[rpc_gateway] rpc mTLS enabled but cert paths missing in PROD+armed"
                _warn_once("rpc_mtls_missing", msg, level=logging.CRITICAL)
                if _strict_config(cfg):
                    raise RuntimeError("rpc mTLS certs missing in PROD+armed")
        return RPCSettings(
            enabled, host, port, region, timeout_s, max_retries, idempotency_ttl_s, memoize_response,
            mtls_enabled, ca_cert, server_cert, server_key, client_cert, client_key, require_client_cert
        )


# ---------- TLS helpers ----------

def _make_server_ssl(settings: RPCSettings) -> Optional[ssl.SSLContext]:
    if not settings.mtls_enabled:
        return None
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    if settings.ca_cert:
        ctx.load_verify_locations(settings.ca_cert)
    if settings.server_cert and settings.server_key:
        ctx.load_cert_chain(settings.server_cert, settings.server_key)
    if settings.require_client_cert:
        ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx

def _make_client_ssl(settings: RPCSettings) -> Optional[ssl.SSLContext]:
    if not settings.mtls_enabled:
        return None
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    if settings.ca_cert:
        ctx.load_verify_locations(settings.ca_cert)
    if settings.client_cert and settings.client_key:
        ctx.load_cert_chain(settings.client_cert, settings.client_key)
    return ctx


# ---------- Server ----------

class RPCServer:
    def __init__(self, cfg: BotConfig, handlers: RPCHandlers, *, lhm=None, admin_token: str | None = None):
        self.cfg = cfg
        self.settings = RPCSettings.from_cfg(cfg)
        self.handlers = handlers
        self._app = None
        self._runner = None
        self._site = None
        self._lhm = lhm  # <= LoggerHistoriqueManager (facultatif)
        rpc_cfg = _resolve_rpc_cfg(cfg)
        self._admin_enabled = bool(getattr(rpc_cfg, "admin_enabled", False))
        self._admin_token = admin_token or str(getattr(rpc_cfg, "admin_token", "") or "") or None
        self._strict_validation = bool(getattr(rpc_cfg, "strict_validation", False))
        g_cfg = getattr(cfg, "g", None)
        mode = str(getattr(g_cfg, "mode", "DRY_RUN") or "DRY_RUN").upper()
        self._prod_armed = bool(mode == "PROD" and bool(getattr(g_cfg, "live_trading_armed", False)))
        if self._prod_armed and self._admin_enabled and not self._admin_token:
            msg = "[rpc_gateway] admin_enabled without admin_token in PROD+armed"
            _warn_once("rpc_admin_token_missing", msg, level=logging.CRITICAL)
            if _strict_config(cfg):
                raise RuntimeError("admin_token missing in PROD+armed")
        self._status_handler = None

    # ---------- API wrappers (conformes à la spec, utiles en tests) ----------
    async def submit_bundle(self, payload: dict) -> dict:
        # Idempotence côté API directe (tests) : on réutilise la même logique que les handlers HTTP
        key = self._derive_idem_key_from_payload(payload)
        if not key:
            _inc(RPC_PAYLOAD_REJECTED_TOTAL, 1.0, model="RPC_MISSING_IDEMPOTENCY_KEY")
            raise ValueError("RPC_MISSING_IDEMPOTENCY_KEY")

        res, replayed = await self._process_idempotent_flow(
            key=key,
            method="submit_bundle",
            payload=payload,
            handler=self.handlers.submit_bundle,
        )
        if replayed:
            _inc(RPC_IDEMPOTENCY_HIT_TOTAL, 1.0, method="submit_bundle", region=self.settings.region)
        return res

    async def cancel(self, payload: dict) -> dict:
        key = self._derive_idem_key_from_payload(payload)
        if not key:
            _inc(RPC_PAYLOAD_REJECTED_TOTAL, 1.0, model="RPC_MISSING_IDEMPOTENCY_KEY")
            raise ValueError("RPC_MISSING_IDEMPOTENCY_KEY")
        seen, resp = await self._idem.peek(key)
        if seen:
            _inc(RPC_IDEMPOTENCY_HIT_TOTAL, 1.0, method="cancel", region=self.settings.region)
            return resp or {"replayed": True}

        res = await self.handlers.cancel(payload)
        await self._idem.put(key, res)
        return res

    async def status(self, req: dict) -> dict:
        return await self.handlers.status(req or {})

    async def stream(self, req: dict) -> AsyncIterator[dict]:
        # Passe-plat vers l’async generator fourni
        async for ev in self.handlers.stream(req or {}):
            yield ev


    async def start(self):
        if not self.settings.enabled:
            return  # no-op
        app = web.Application(middlewares=[self._mw_timing_and_errors])
        app.add_routes([
            web.get("/rpc/health", self._handle_health),
            web.post("/rpc/submit_bundle", self._handle_submit_bundle),
            web.post("/rpc/cancel", self._handle_cancel),
            web.post("/rpc/status", self._handle_status),
            web.get("/rpc/stream", self._handle_stream_sse),


        ])
        if self._admin_enabled and self._admin_token:
            app.add_routes([
                web.get("/pnl/funnel", self._handle_pnl_funnel),
                web.get("/logs/replay", self._handle_logs_replay),
                web.post("/eod/run", self._handle_eod_run),
            ])
        if getattr(self, "_status_handler", None):
            try:
                app.router.add_get("/status", self._status_handler)
            except Exception:
                pass
        self._app = app
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        ssl_ctx = _make_server_ssl(self.settings)
        self._site = web.TCPSite(self._runner, host=self.settings.host, port=self.settings.port, ssl_context=ssl_ctx)
        await self._site.start()

    async def stop(self):
        if self._site:
            await self._site.stop()
        if self._runner:
            await self._runner.cleanup()
        self._site = None
        self._runner = None
        self._app = None

    # ----- middlewares / wrappers -----

    @web.middleware
    async def _mw_timing_and_errors(self, request, handler):
        t0 = time.perf_counter()
        route = getattr(request.match_info, "route", None)
        method = "unknown"
        try:
            if route and getattr(route, "resource", None):
                method = str(route.resource.canonical)
            elif request.path:
                method = request.path
            method = method.replace("/rpc/", "") if "/rpc/" in method else method
        except Exception:
            method = "unknown"

        method = str(method).replace("/rpc/", "")  # nicer label
        try:
            resp = await handler(request)
            dt_ms = (time.perf_counter() - t0) * 1000.0
            _observe(RPC_LATENCY_MS, dt_ms, method=method, region=self.settings.region)
            return resp
        except web.HTTPException as e:
            _inc(RPC_ERR_TOTAL, 1.0, code=str(e.status), method=method, region=self.settings.region)
            payload = {
                "ok": False,
                "error": {"code": str(e.status), "message": (e.text or e.reason or "http_error")},
            }
            return web.json_response(payload, status=e.status)
        except Exception as e:
            code = type(e).__name__ or "exception"
            _inc(RPC_ERR_TOTAL, 1.0, code=code, method=method, region=self.settings.region)
            payload = {"ok": False, "error": {"code": code, "message": str(e) or code}}
            return web.json_response(payload, status=500)

    async def _handle_pnl_funnel(self, request):
        self._require_admin(request)
        if not self._lhm:
            raise web.HTTPServiceUnavailable(text="LHM unavailable")
        try:
            q = request.rel_url.query
            since_ms = int(q.get("since_ms")) if q.get("since_ms") else None
            until_ms = int(q.get("until_ms")) if q.get("until_ms") else None
        except Exception:
            raise web.HTTPBadRequest(text="invalid query params")
        res = await self._lhm.get_funnel(since_ms=since_ms, until_ms=until_ms)  # façades LHM
        return web.json_response(res)

    async def _handle_logs_replay(self, request):
        self._require_admin(request)
        if not self._lhm:
            raise web.HTTPServiceUnavailable(text="LHM unavailable")
        q = request.rel_url.query
        kind = (q.get("kind") or "trades").strip().lower()
        day = q.get("day")  # "YYYY-MM-DD" ou None
        try:
            limit = int(q.get("limit")) if q.get("limit") else 1000
        except Exception:
            raise web.HTTPBadRequest(text="invalid limit")
        rows = await self._lhm.replay_from_logs(day=day, kind=kind, limit=limit)
        return web.json_response({"kind": kind, "day": day, "limit": limit, "rows": rows})

    async def _handle_eod_run(self, request):
        self._require_admin(request)
        if not self._lhm:
            raise web.HTTPServiceUnavailable(text="LHM unavailable")

        try:
            body = await self._read_json(request, method="eod_run", required=False) or {}
            pem_path = body.get("sign_priv_pem_path")
        except Exception:
            pem_path = None
        meta = await self._lhm.run_eod_reconciliation(sign_priv_pem_path=pem_path)
        return web.json_response({"ok": True, "meta": meta})

    # --- rpc_gateway.py (dans class RPCServer) ---
    def register_status_endpoint(self, boot) -> None:
        """
        Prépare la route GET /status reflétant Boot.get_status() et un
        éventuel bus d'évènements (si cfg.status_sink est utilisé par le Boot).
        À appeler AVANT start() pour que la route soit bien montée.
        """
        self._boot_ref = boot
        # l'attribut self.app doit exister avant runner.setup(); s'il n'existe pas
        # encore dans ton implémentation, crée-le ici de façon conservatrice.


        async def _handle_status(req):
            boot = getattr(self, "_boot_ref", None)
            st = {}
            if boot and hasattr(boot, "get_status"):
                try:
                    st = boot.get_status() or {}
                except Exception:
                    st = {"state": "UNKNOWN"}
            rm_block = {}
            if boot is not None:
                try:
                    rm = getattr(boot, "risk_manager", None)
                    if rm is not None:
                        rm_block = {
                            "rm_mode": str(getattr(rm, "rm_mode", "UNKNOWN")),
                            "pacer_mode": str(getattr(rm, "pacer_mode", "UNKNOWN")),
                            "trade_mode": str(getattr(rm, "trade_mode", "UNKNOWN")),
                        }
                        if hasattr(rm, "private_plane_state"):
                            rm_block["private_plane_state"] = str(rm.private_plane_state)
                except Exception:
                    rm_block = {"rm_mode": "UNKNOWN"}
            if rm_block:
                try:
                    st.setdefault("rm", rm_block)
                except Exception:
                    pass
            # Optionnel: si tu stockes un buffer d'événements en mémoire,
            # expose-le ici; sinon on renvoie juste le statut.
            return web.json_response({"boot": st})

        self._status_handler = _handle_status


    # ----- endpoint handlers -----

    async def _handle_health(self, request: web.Request):
        return web.json_response({"ok": True, "region": self.settings.region})

    async def _handle_submit_bundle(self, request: web.Request):
        return await self._handle_idempotent_request(
            request,
            method="submit_bundle",
            validator=self._validate_submit_payload,
            handler=self.handlers.submit_bundle,
        )

    async def _handle_cancel(self, request: web.Request):
        return await self._handle_idempotent_request(
            request,
            method="cancel",
            validator=self._validate_cancel_payload,
            handler=self.handlers.cancel,
        )

    async def _handle_status(self, request: web.Request):
        body = await self._read_json(request, method="status", required=False)
        res = await self.handlers.status(body or {})
        return web.json_response(res)

    async def _handle_stream_sse(self, request: web.Request):
        """
        Stream d'évènements en Server-Sent Events (texte/event-stream).
        Le handler `handlers.stream` doit être un async generator de dicts.
        """
        response = web.StreamResponse(
            status=200, reason='OK',
            headers={'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache'}
        )
        await response.prepare(request)
        try:
            async for item in self.handlers.stream({}):
                line = f"data: {json.dumps(item, separators=(',', ':'))}\n\n"
                try:
                    await response.write(line.encode("utf-8"))
                except (asyncio.CancelledError, ConnectionResetError):
                    break
                except Exception:
                    break
                await response.drain()
        finally:
            with contextlib.suppress(Exception):
                await response.write_eof()

        return response

    # ----- utils -----
    async def _handle_idempotent_request(self, request: web.Request, *, method: str, validator: Callable[[dict], dict],
                                         handler: Callable[[dict], Awaitable[dict]]):


        body = await self._read_json(request, method=method)
        if body is None:
            _inc(RPC_PAYLOAD_REJECTED_TOTAL, 1.0, model="unknown")
            raise web.HTTPBadRequest(text="invalid JSON")
        idem = self._extract_idempotency_key(request, body)

        if not idem:
            _inc(RPC_PAYLOAD_REJECTED_TOTAL, 1.0, model="RPC_MISSING_IDEMPOTENCY_KEY")
            raise web.HTTPBadRequest(text="missing idempotency key")
        body = validator(body)
        res, replayed = await self._process_idempotent_flow(
            key=idem,
            method=method,
            payload=body,
            handler=handler,
        )
        if replayed:
            _inc(RPC_IDEMPOTENCY_HIT_TOTAL, 1.0, method=method, region=self.settings.region)
            return web.json_response(res or {"replayed": True}, headers={"X-Idempotent-Replay": "1"})
        return web.json_response(res)

    async def _process_idempotent_flow(
            self,
            *,
            key: str,
            method: str,
            payload: dict,
            handler: Callable[[dict], Awaitable[dict]],
    ) -> tuple[dict, bool]:
        op_id = f"RPC/{key}"
        lhm = self._lhm
        if not lhm:
            raise web.HTTPServiceUnavailable(text="idempotency store unavailable")

        try:
            state = await asyncio.to_thread(lhm.get_rpc_idem_state, op_id)
        except Exception as e:
            self._mark_rpc_idem_degraded(e)
            raise web.HTTPServiceUnavailable(text="idempotency degraded")

        if state:
            status = str(state.get("status") or "").upper()
            if status == "DONE":
                cached = state.get("payload")
                return cached or {"replayed": True}, True
            if status == "STARTED":
                raise web.HTTPConflict(text="idempotent request already started")
            if status == "FAILED":
                raise web.HTTPConflict(text="idempotent request previously failed")

        try:
            await asyncio.to_thread(
                lhm.mark_rpc_idem_started,
                op_id=op_id,
                payload=self._safe_rpc_payload(method, payload),
            )
        except Exception as e:
            self._mark_rpc_idem_degraded(e)
            raise web.HTTPServiceUnavailable(text="idempotency degraded")

        try:
            res = await handler(payload)
        except Exception as e:
            with contextlib.suppress(Exception):
                await asyncio.to_thread(
                    lhm.mark_rpc_idem_failed,
                    op_id=op_id,
                    payload=self._safe_rpc_payload(method, payload),
                    last_error=str(e)[:400],
                )
            raise

        expires_ts_ms: Optional[int] = None
        if self.settings.idempotency_ttl_s > 0:
            expires_ts_ms = int(time.time() * 1000 + max(0, self.settings.idempotency_ttl_s) * 1000)

        try:
            await asyncio.to_thread(
                lhm.mark_rpc_idem_done,
                op_id=op_id,
                payload=self._safe_rpc_payload(method, payload),
                response=res if self.settings.memoize_response else None,
                expires_ts_ms=expires_ts_ms,
            )
        except Exception as e:
            self._mark_rpc_idem_degraded(e)
            raise web.HTTPServiceUnavailable(text="idempotency degraded")

        return res, False

    def _safe_rpc_payload(self, method: str, payload: Optional[dict]) -> dict:
        snap: dict[str, Any] = {"method": method}
        if isinstance(payload, dict):
            try:
                snap["payload_keys"] = sorted(payload.keys())
            except Exception:
                snap["payload_repr"] = str(payload)
        return snap

    def _mark_rpc_idem_degraded(self, exc: Optional[Exception] = None) -> None:
        mgr = self._lhm
        if not mgr:
            return
        payload = {"error": str(exc)} if exc else None
        try:
            if hasattr(mgr, "activate_fail_closed_latch"):
                mgr.activate_fail_closed_latch(
                    op_id="RPC/IDEMPOTENCY_DEGRADED",
                    reason="RPC_IDEMPOTENCY_DEGRADED",
                    payload=payload,
                )
        except Exception:
            pass
    async def _read_json(self, request: web.Request, *, method: str, required: bool = True) -> Optional[dict]:
        try:
            raw = await request.read()
            if not raw:
                return {} if not required else None
            return json.loads(raw.decode("utf-8"))
        except Exception:
            _inc(RPC_ERR_TOTAL, 1.0, code="400", method=method, region=self.settings.region)
            return None

    def _validate_submit_payload(self, body: dict) -> dict:
        try:
            model = validate_submit_bundle_lite(body, prod=self._prod_armed)
            data = model.model_dump() if hasattr(model, "model_dump") else getattr(model, "dict", lambda **_: {})(
                exclude_none=False)
            if data.get("_schema_invalid"):
                _inc(RPC_PAYLOAD_REJECTED_TOTAL, 1.0, model="SubmitBundleRequest")
                reason = data.get("_schema_reason") or data.get("reason_code") or "invalid submit_bundle payload"
                raise web.HTTPBadRequest(text=str(reason))
            if self._strict_validation:
                return data
            return data
        except web.HTTPBadRequest:
            raise
        except Exception:
            _inc(RPC_PAYLOAD_REJECTED_TOTAL, 1.0, model="SubmitBundleRequest")
            raise web.HTTPBadRequest(text="invalid submit_bundle payload")

    def _validate_cancel_payload(self, body: dict) -> dict:
        try:
            model = validate_cancel_lite(body, prod=self._prod_armed)
            data = model.model_dump() if hasattr(model, "model_dump") else getattr(model, "dict", lambda **_: {})(
                exclude_none=False)
            if data.get("_schema_invalid"):
                _inc(RPC_PAYLOAD_REJECTED_TOTAL, 1.0, model="CancelRequest")
                reason = data.get("_schema_reason") or data.get("reason_code") or "invalid cancel payload"
                raise web.HTTPBadRequest(text=str(reason))
            return data
        except web.HTTPBadRequest:
            raise
        except Exception:
            _inc(RPC_PAYLOAD_REJECTED_TOTAL, 1.0, model="CancelRequest")
            raise web.HTTPBadRequest(text="invalid cancel payload")

    def _require_admin(self, request: web.Request) -> None:
        if not self._admin_enabled:
            raise web.HTTPForbidden(text="admin endpoints disabled")
        token = request.headers.get("X-Admin-Token") or request.rel_url.query.get("token")
        if not self._admin_token or token != self._admin_token:
            raise web.HTTPUnauthorized(text="missing/invalid admin token")

    def _derive_idem_key_from_payload(self, body: Optional[dict]) -> Optional[str]:
        """
        Construit une clé d'idempotence stable à partir des champs usuels si présents.
        Priorité: meta.idempotency_key | idempotency_key | client_order_id | bundle_id | trace_id | ts_ns.
        """
        if not isinstance(body, dict):
            return None
        meta = body.get("meta") if isinstance(body.get("meta"), dict) else {}
        for key in ("idempotency_key",):
            v = meta.get(key) if isinstance(meta, dict) else None
            if v:
                return str(v)
            v = body.get(key)
            if v:
                return str(v)
        parts = []
        for k in ("client_order_id", "bundle_id", "trace_id", "ts_ns"):
            v = body.get(k)
            if v is not None:
                parts.append(f"{k}:{v}")
        return "|".join(parts) if parts else None

    def _extract_idempotency_key(
            self,
            request: web.Request,
            body: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """
        Extraction robuste:
          1) Headers (X-Idempotency-Key / Idempotency-Key)
          2) Payload (meta.idempotency_key / idempotency_key / client_order_id / bundle_id / trace_id / ts_ns)
        """
        idem = request.headers.get("X-Idempotency-Key") or request.headers.get("Idempotency-Key")
        idem = self._sanitize_idempotency_key(idem)
        if idem:
            return idem

        if isinstance(body, dict):
            idem = self._derive_idem_key_from_payload(body)
            idem = self._sanitize_idempotency_key(idem)
            if idem:
                return idem

        return None


# ---------- Client ----------

class RPCClient:
    """
    Client RPC avec retries ≤ 2, timeout ~2s, mTLS si activé.
    Utilisation:
        client = RPCClient(cfg, region="US")
        await client.call("submit_bundle", body={...})
    """
    def __init__(self, cfg: BotConfig, region: str, base_url: Optional[str] = None):
        self.cfg = cfg
        self.settings = RPCSettings.from_cfg(cfg, default_region=region)
        self._ssl_ctx = _make_client_ssl(self.settings)
        self._session: Optional[ClientSession] = None
        # endpoint distant (ex: cfg.engine_pod_map["US"])
        rpc_cfg = _resolve_rpc_cfg(cfg)
        remote_base = getattr(rpc_cfg, "remote_base_url", None) or getattr(rpc_cfg, "rpc_client_base", None)
        if remote_base is None:
            _warn_once("rpc_remote_base_missing", "[rpc_gateway] rpc.remote_base_url missing; using fallback")
            remote_base = "https://engine-us:8443"
        self._base_url = base_url or remote_base

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            timeout = ClientTimeout(total=self.settings.timeout_s)
            self._session = ClientSession(timeout=timeout)

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def call(
            self,
            method: str,
            *,
            body: dict,
            idempotency_key: Optional[str] = None,
            timeout_s: Optional[float] = None,
            retries: Optional[int] = None,
    ) -> dict:
        """
        Appel POST /rpc/{method}
        Retente ≤ retries (par défaut cfg.rpc_max_retries ≤ 2) sur 5xx/timeout/conn ; retry sur 429.
        Pas de retry sur autres 4xx.
        """
        await self._ensure_session()
        url = f"{self._base_url}/rpc/{method}"
        attempts = 0
        last_exc = None
        headers = {}
        if idempotency_key:
            headers["X-Idempotency-Key"] = idempotency_key

        # Budgets par appel (override possibles)
        budget_timeout = float(timeout_s if timeout_s is not None else self.settings.timeout_s)
        max_retries = int(min(retries if retries is not None else self.settings.max_retries, 2))

        while attempts <= max_retries:
            t0 = time.perf_counter()
            try:
                async with self._session.post(
                        url,
                        json=body,
                        ssl=self._ssl_ctx,
                        headers=headers,
                        timeout=ClientTimeout(total=budget_timeout),
                ) as resp:
                    dt_ms = (time.perf_counter() - t0) * 1000.0
                    _observe(RPC_LATENCY_MS, dt_ms, method=method, region=self.settings.region)

                    if resp.status == 200:
                        return await resp.json()

                    # erreurs
                    _inc(RPC_ERR_TOTAL, 1.0, code=str(resp.status), method=method, region=self.settings.region)

                    if resp.status == 429:
                        await asyncio.sleep(self._retry_delay(attempts))
                        attempts += 1
                        _inc(RPC_RETRIES_TOTAL, 1.0, method=method, region=self.settings.region)
                        continue

                    if 400 <= resp.status < 500:
                        # pas de retry
                        txt = await resp.text()
                        raise RuntimeError(f"RPC {method} HTTP {resp.status}: {txt}")

                    # 5xx -> retry
                    await asyncio.sleep(self._retry_delay(attempts))
                    attempts += 1
                    _inc(RPC_RETRIES_TOTAL, 1.0, method=method, region=self.settings.region)
                    continue

            except asyncio.TimeoutError as e:
                _inc(RPC_ERR_TOTAL, 1.0, code="timeout", method=method, region=self.settings.region)
                last_exc = e
            except Exception as e:
                _inc(RPC_ERR_TOTAL, 1.0, code="conn", method=method, region=self.settings.region)
                last_exc = e

            # retry on network/timeout exceptions
            await asyncio.sleep(self._retry_delay(attempts))
            attempts += 1
            _inc(RPC_RETRIES_TOTAL, 1.0, method=method, region=self.settings.region)

        # out of retries
        if last_exc:
            raise last_exc
        raise RuntimeError(f"RPC {method} failed without explicit exception")

    def _retry_delay(self, attempts: int) -> float:
        # expo backoff (100ms * 2^attempts) + jitter [0..100ms], borné à 600ms
        base = min(0.1 * (2 ** attempts), 0.6)
        return base + random.random() * 0.1

# ---------- Execution Submitters (canonique) ----------


class ExecutionSubmitter:
    async def submit_bundle(self, payload: dict) -> dict:  # pragma: no cover - interface
        raise NotImplementedError


class InprocSubmitter(ExecutionSubmitter):
    """Loopback ultra-rapide: appelle le handler canonique sans HTTP."""

    def __init__(self, rpc_server: RPCServer):
        self.rpc_server = rpc_server

    async def submit_bundle(self, payload: dict) -> dict:
        body = self.rpc_server._validate_submit_payload(payload)
        return await self.rpc_server.submit_bundle(body)


class RpcSubmitter(ExecutionSubmitter):
    """Soumission via RPC réseau standard."""

    def __init__(self, rpc_client: RPCClient):
        self.rpc_client = rpc_client

    async def submit_bundle(self, payload: dict) -> dict:
        g_cfg = getattr(self.rpc_client.cfg, "g", None)
        mode = str(getattr(g_cfg, "mode", "DRY_RUN") or "DRY_RUN").upper()
        prod = bool(mode == "PROD" and bool(getattr(g_cfg, "live_trading_armed", False)))
        validated = validate_submit_bundle_lite(payload, prod=prod)
        data = validated.model_dump() if hasattr(validated, "model_dump") else validated.dict(exclude_none=False)
        idem = data.get("meta", {}).get("idempotency_key") or data.get("idempotency_key")
        return await self.rpc_client.call("submit_bundle", body=data, idempotency_key=idem)


def make_submitter_for_exchange(
        cfg: BotConfig,
        exchange: str,
        *,
        local_region: Optional[str] = None,
        exchange_region_map: Optional[Dict[str, str]] = None,
        engine_pod_map: Optional[Dict[str, str]] = None,
        rpc_server: Optional[RPCServer] = None,
        rpc_client_factory: Callable[[str, str], RPCClient] | None = None,
) -> ExecutionSubmitter:
    ex = str(exchange or "").upper()
    region_map = {k.upper(): str(v).upper() for k, v in (exchange_region_map or {}).items()}
    target_region = region_map.get(ex, str(local_region or "EU").upper())
    rpc_cfg = _resolve_rpc_cfg(cfg)
    loopback = bool(getattr(rpc_cfg, "loopback_inproc", True))
    if target_region == str(local_region or "EU").upper() and loopback:
        if rpc_server is None:
            raise ValueError("RPC server required for inproc submitter")
        return InprocSubmitter(rpc_server)

    pod_map = {k.upper(): v for k, v in (engine_pod_map or {}).items()}
    base_url = pod_map.get(target_region)
    if not base_url:
        raise ValueError(f"Missing engine_pod_map endpoint for region {target_region}")
    factory = rpc_client_factory or (lambda region, url: RPCClient(cfg, region=region, base_url=url))
    client = factory(target_region, base_url)
    return RpcSubmitter(client)