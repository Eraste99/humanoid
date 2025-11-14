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
import json
import ssl
import time
import random
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Optional, NamedTuple, Dict

try:
    from aiohttp import web, ClientSession, ClientTimeout
except Exception as e:  # pragma: no cover
    raise RuntimeError("aiohttp manquant: ajoutez-le à requirements-core.txt") from e

from modules.bot_config import BotConfig
from modules.rm_compat import getattr_int, getattr_float, getattr_str, getattr_bool, getattr_dict, getattr_list

import modules.obs_metrics as obs  # on accède par getattr avec fallback no-op


# ---------- Metrics helpers (tolérants si les constantes n'existent pas encore) ----------

class _NoopMetric:
    def labels(self, **kw): return self
    def observe(self, *a, **k): return None
    def inc(self, *a, **k): return None
    def set(self, *a, **k): return None

def _get_metric(name: str):
    return getattr(obs, name, _NoopMetric())

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



# ---------- Idempotency store (TTL) ----------

class _TTLIdempotencyStore:
    """Simple TTL store (in-mem) pour idempotence stricte des requêtes RPC côté serveur."""
    def __init__(self, maxsize: int = 10000, ttl_s: int = 600):
        self._maxsize = maxsize
        self._ttl_s = ttl_s
        self._store: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def seen(self, key: str) -> bool:
        now = time.time()
        async with self._lock:
            # GC opportuniste
            if len(self._store) > self._maxsize:
                # drop des plus vieux ~probabiliste (O(n), acceptable P0)
                expired = [k for k, t in self._store.items() if t < now]
                for k in expired[: self._maxsize // 10 or 1]:
                    self._store.pop(k, None)
            # check
            t = self._store.get(key)
            if t and t > now:
                return True
            # mark
            self._store[key] = now + self._ttl_s
            return False


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
        enabled = getattr(cfg, "rpc_enabled", True)
        host = getattr(cfg, "rpc_host", default_host)
        port = getattr_int(cfg, "rpc_port", default_port)
        region = getattr(cfg, "region", default_region)  # label métrique
        timeout_s = getattr_float(cfg, "rpc_timeout_s", 2.0)
        max_retries = int(min(getattr(cfg, "rpc_max_retries", 2), 2))
        mtls_enabled = getattr_bool(cfg, "rpc_mtls_enabled", True)
        ca_cert = getattr(cfg, "rpc_ca_cert", None)
        server_cert = getattr(cfg, "rpc_server_cert", None)
        server_key = getattr(cfg, "rpc_server_key", None)
        client_cert = getattr(cfg, "rpc_client_cert", None)
        client_key = getattr(cfg, "rpc_client_key", None)
        require_client_cert = getattr_bool(cfg, "rpc_require_client_cert", True)
        return RPCSettings(
            enabled, host, port, region, timeout_s, max_retries,
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
        self._idem = _TTLIdempotencyStore(maxsize=20000, ttl_s=600)
        self._lhm = lhm  # <= LoggerHistoriqueManager (facultatif)
        self._admin_token = admin_token  # <= protège /eod/run ; sinon open-readonly

    # ---------- API wrappers (conformes à la spec, utiles en tests) ----------
    async def submit_bundle(self, payload: dict) -> dict:
        # Idempotence côté API directe (tests) : on réutilise la même logique que les handlers HTTP
        key = self._derive_idem_key_from_payload(payload)
        if key and await self._idem.seen(key):
            return {"replayed": True}
        return await self.handlers.submit_bundle(payload)

    async def cancel(self, payload: dict) -> dict:
        key = self._derive_idem_key_from_payload(payload)
        if key and await self._idem.seen(key):
            return {"replayed": True}
        return await self.handlers.cancel(payload)

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
            web.post("/rpc/submit_bundle", self._wrap_idempotent(self._handle_submit_bundle)),
            web.post("/rpc/cancel", self._wrap_idempotent(self._handle_cancel)),
            web.post("/rpc/status", self._handle_status),
            web.get("/rpc/stream", self._handle_stream_sse),
            web.get("/pnl/funnel", self._handle_pnl_funnel),
            web.get("/logs/replay", self._handle_logs_replay),
            web.post("/eod/run", self._handle_eod_run),

        ])
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
            elif request.path_qs:
                method = request.path_qs
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
            raise
        except Exception:
            _inc(RPC_ERR_TOTAL, 1.0, code="500", method=method, region=self.settings.region)
            raise

    async def _handle_pnl_funnel(self, request):
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
        if not self._lhm:
            raise web.HTTPServiceUnavailable(text="LHM unavailable")
        # petit garde-fou: token d’admin optionnel (header ou query)
        token = request.headers.get("X-Admin-Token") or request.rel_url.query.get("token")
        if self._admin_token and token != self._admin_token:
            raise web.HTTPUnauthorized(text="missing/invalid admin token")
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
        try:
            from aiohttp import web
        except Exception as e:
            # Si aiohttp indisponible, on degrade silencieusement.
            self._status_route_error = f"aiohttp missing: {e!r}"
            return

        app = getattr(self, "app", None)
        if app is None:
            self.app = web.Application()
            app = self.app

        async def _handle_status(req):
            boot = getattr(self, "_boot_ref", None)
            st = {}
            if boot and hasattr(boot, "get_status"):
                try:
                    st = boot.get_status() or {}
                except Exception:
                    st = {"state": "UNKNOWN"}
            # Optionnel: si tu stockes un buffer d'événements en mémoire,
            # expose-le ici; sinon on renvoie juste le statut.
            return web.json_response({"boot": st})

        # éviter double-registration
        try:
            app.router.add_get("/status", _handle_status)
        except Exception:
            # déjà enregistré ou app non routable: on ignore
            pass

    def _wrap_idempotent(self, fn: Callable):
        async def inner(request):
            # Ici : check rapide sur headers uniquement (pour ne pas consommer le body prématurément)
            idem = request.headers.get("X-Idempotency-Key") or request.headers.get("Idempotency-Key")
            if idem and await self._idem.seen(idem):
                return web.json_response({"replayed": True}, headers={"X-Idempotent-Replay": "1"})
            return await fn(request)

        return inner

    # ----- endpoint handlers -----

    async def _handle_health(self, request: web.Request):
        return web.json_response({"ok": True, "region": self.settings.region})

    async def _handle_submit_bundle(self, request: web.Request):
        body = await self._read_json(request, method="submit_bundle")
        if body is None:
            _inc(RPC_PAYLOAD_REJECTED_TOTAL, 1.0, model="unknown")
            raise web.HTTPBadRequest(text="invalid JSON")
        # Idempotence: headers d’abord (wrapper), puis fallback body
        idem = request.headers.get("X-Idempotency-Key") or request.headers.get("Idempotency-Key")
        if not idem:
            idem = self._derive_idem_key_from_payload(body)
        if idem and await self._idem.seen(idem):
            return web.json_response({"replayed": True}, headers={"X-Idempotent-Replay": "1"})
        res = await self.handlers.submit_bundle(body)
        return web.json_response(res)

    async def _handle_cancel(self, request: web.Request):
        body = await self._read_json(request, method="cancel")
        if body is None:
            _inc(RPC_PAYLOAD_REJECTED_TOTAL, 1.0, model="unknown")
            raise web.HTTPBadRequest(text="invalid JSON")
        idem = request.headers.get("X-Idempotency-Key") or request.headers.get("Idempotency-Key")
        if not idem:
            idem = self._derive_idem_key_from_payload(body)
        if idem and await self._idem.seen(idem):
            return web.json_response({"replayed": True}, headers={"X-Idempotent-Replay": "1"})
        res = await self.handlers.cancel(body)
        return web.json_response(res)

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
        async for item in self.handlers.stream({}):
            line = f"data: {json.dumps(item, separators=(',',':'))}\n\n"
            await response.write(line.encode("utf-8"))
        await response.write_eof()
        return response

    # ----- utils -----

    async def _read_json(self, request: web.Request, *, method: str, required: bool = True) -> Optional[dict]:
        try:
            raw = await request.read()
            if not raw:
                return {} if not required else None
            return json.loads(raw.decode("utf-8"))
        except Exception:
            _inc(RPC_ERR_TOTAL, 1.0, code="400", method=method, region=self.settings.region)
            return None

    def _derive_idem_key_from_payload(self, body: Optional[dict]) -> Optional[str]:
        """
        Construit une clé d'idempotence stable à partir des champs usuels si présents.
        Priorité: client_order_id | bundle_id | trace_id | ts_ns (concat).
        """
        if not isinstance(body, dict):
            return None
        parts = []
        for k in ("client_order_id", "bundle_id", "trace_id", "ts_ns"):
            v = body.get(k)
            if v is not None:
                parts.append(f"{k}:{v}")
        return "|".join(parts) if parts else None


    def _extract_idempotency_key(self, request: web.Request) -> Optional[str]:
        # priorités: en-tête dédié, sinon derive du body si présent
        key = request.headers.get("X-Idempotency-Key") or request.headers.get("Idempotency-Key")
        if key:
            return key
        try:
            # derive de quelques champs usuels si dispo
            # NOTE: lecture non bloquante (on suppose _read_json sera re-parsé ensuite)
            pass
        except Exception:
            pass
        return None


# ---------- Client ----------

class RPCClient:
    """
    Client RPC avec retries ≤ 2, timeout ~2s, mTLS si activé.
    Utilisation:
        client = RPCClient(cfg, region="US")
        await client.call("submit_bundle", body={...})
    """
    def __init__(self, cfg: BotConfig, region: str):
        self.cfg = cfg
        self.settings = RPCSettings.from_cfg(cfg, default_region=region)
        self._ssl_ctx = _make_client_ssl(self.settings)
        self._session: Optional[ClientSession] = None
        # endpoint distant (ex: cfg.engine_pod_map["US"])
        self._base_url = getattr(cfg, "rpc_remote_base_url", "https://engine-us:8443")

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
