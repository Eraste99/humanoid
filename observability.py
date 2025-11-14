"""
Compatibility SHIM for legacy 'modules.observability'

But de ce shim :
- Ré-exporter *tout* depuis modules.obs_metrics (métriques + helpers).
- Fournir un petit serveur HTTP :
    * /metrics (Prometheus)
    * /health (agrège get_status() des modules enregistrés)
    * /api/engine/status (+ alias /engine/status)
    * /api/risk/snapshot  (+ alias /risk/snapshot)
    * /api/scanner/snapshot (+ alias /scanner/snapshot)
    * /api/router/health (+ alias /router/health) : cache interne opt-in
    * /central/last_event et /central/status (si central_watchdog enregistré)
- Exposer quelques helpers "legacy" en les mappant vers obs_metrics.

Notes :
- Si 'aiohttp' est absent : on démarre uniquement un /metrics Prometheus via start_http_server(port).
- Ce shim est transitoire : migre progressivement vers 'from modules.obs_metrics import ...',
  puis supprime ce fichier.
"""
from __future__ import annotations
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional
from modules.obs_metrics import *
from modules.obs_metrics import __all__ as _OBS_ALL, ENGINE_SUBMIT_TO_ACK_MS, ENGINE_ACK_TO_FILL_MS, RPC_LATENCY_MS, RPC_ERR_TOTAL, RPC_RETRIES_TOTAL, RPC_PAYLOAD_REJECTED_TOTAL, SCANNER_REJECTIONS_TOTAL, SCANNER_EMITTED_TOTAL, ROUTER_TO_SCANNER_MS, mark_bf_latency, mark_books_fresh, mark_balances_fresh, set_rm_paused_count, set_dynamic_min, inc_rm_reject
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest, REGISTRY, start_http_server
try:
    from aiohttp import web
except Exception:
    web = None
log = logging.getLogger('observability_shim')
_ROUTER_HEALTH_CACHE: Dict[str, Dict[str, Any]] = {}

def router_health_on_beat(exchange: str, *, last_ex_ts_ms: int, last_recv_ts_ms: int, age_ms: float, pairs_seen: int | List[str] | None=None) -> None:
    """
    Alimente le cache HTTP /api/router/health. Sans impact sur les métriques.
    """
    try:
        exu = str(exchange).upper()
        n_pairs = len(pairs_seen) if isinstance(pairs_seen, (list, tuple)) else int(pairs_seen or 0)
        _ROUTER_HEALTH_CACHE[exu] = {'exchange': exu, 'last_ex_ts_ms': int(last_ex_ts_ms), 'last_recv_ts_ms': int(last_recv_ts_ms), 'age_ms': float(age_ms), 'pairs_seen': n_pairs, 'updated_ts': time.time()}
    except Exception:
        log.exception('router_health_on_beat failed')

def ws_public_on_frame(exchange: str) -> None:
    log.debug('ws_public_on_frame(%s) [legacy no-op]', exchange)

def ws_public_set_staleness(exchange: str, seconds: float) -> None:
    log.debug('ws_public_set_staleness(%s, %.3f) [legacy no-op]', exchange, seconds)

def mark_books_fresh_by_exchange(exchange: str) -> None:
    try:
        mark_books_fresh((_pair := 'ALL'))
    except Exception:
        log.exception('mark_books_fresh_by_exchange failed')

def router_on_combo_event(combo: str, skew_ms: float | None=None, ages_ms_by_exchange: dict[str, float] | None=None) -> None:
    try:
        if isinstance(skew_ms, (int, float)):
            ROUTER_COMBO_SKEW_MS.labels(str(combo)).observe(max(0.0, float(skew_ms)))
    except Exception:
        log.exception('router_on_combo_event failed')

def feesync_on_refresh(ok: bool) -> None:
    log.debug('feesync_on_refresh(%s) [legacy no-op]', ok)

def feesync_on_apply(exchange: str, alias: str) -> None:
    log.debug('feesync_on_apply(%s,%s) [legacy no-op]', exchange, alias)

def feesync_on_error() -> None:
    log.debug('feesync_on_error() [legacy no-op]')

def discovery_on_run(enabled_exchanges: list[str], pairs_per_exchange: dict[str, int]) -> None:
    log.debug('discovery_on_run(%d exchanges) [legacy no-op]', len(enabled_exchanges or []))

def private_on_poller_tick(exchange: str) -> None:
    log.debug('private_on_poller_tick(%s) [legacy no-op]', exchange)

def private_on_event(exchange: str, typ: str) -> None:
    log.debug('private_on_event(%s,%s) [legacy no-op]', exchange, typ)

def snapshot_inventory(balances: dict[str, dict[str, float]], mids: dict[str, float]) -> None:
    log.debug('snapshot_inventory(...) [legacy no-op]')

def bump_scanner(spread_pct: float | None, active_pairs: int | None) -> None:
    log.debug('bump_scanner(spread=%s, active=%s) [legacy no-op]', spread_pct, active_pairs)

def set_tm_open_makers(n: int) -> None:
    log.debug('set_tm_open_makers(%d) [legacy no-op]', n)

def tm_on_maker_placed(exchange: str, symbol: str, side: str) -> None:
    log.debug('tm_on_maker_placed(%s,%s,%s) [legacy no-op]', exchange, symbol, side)

def tm_on_maker_canceled(exchange: str, symbol: str) -> None:
    log.debug('tm_on_maker_canceled(%s,%s) [legacy no-op]', exchange, symbol)

def tm_on_hedge_sent(exchange: str, symbol: str, side: str, lag_seconds: float | None=None) -> None:
    log.debug('tm_on_hedge_sent(%s,%s,%s,lag=%s) [legacy no-op]', exchange, symbol, side, lag_seconds)

def tm_on_maker_fill_ratio(ratio: float) -> None:
    log.debug('tm_on_maker_fill_ratio(%.3f) [legacy no-op]', ratio)

def mm_on_opp(pair: str) -> None:
    log.debug('mm_on_opp(%s) [legacy no-op]', pair)

def mm_on_both_filled(pair: str) -> None:
    log.debug('mm_on_both_filled(%s) [legacy no-op]', pair)

def mm_on_single_fill_hedged(pair: str) -> None:
    log.debug('mm_on_single_fill_hedged(%s) [legacy no-op]', pair)

def mm_on_panic_hedge(pair: str) -> None:
    log.debug('mm_on_panic_hedge(%s) [legacy no-op]', pair)

def set_engine_queue(n: int) -> None:
    try:
        ENGINE_SUBMIT_QUEUE_DEPTH.set(max(0, int(n)))
    except Exception:
        log.exception('set_engine_queue failed')
from typing import Any

def _norm_shim(v: Any) -> str:
    if v is None:
        return 'none'
    s = str(v)
    return s if s else 'empty'

def inc_engine_trade(result: str, kind: str, mode: str='standard') -> None:
    try:
        TRADES_LIVE_DAY_TOTAL.labels(_norm_shim(result)).inc()
    except Exception:
        log.exception('inc_engine_trade failed')

def observe_engine_latency(seconds: float) -> None:
    try:
        ENGINE_DRAIN_LATENCY_MS.observe(max(0.0, float(seconds) * 1000.0))
    except Exception:
        log.exception('observe_engine_latency failed')

def mark_engine_ack(submit_ts_ns: int) -> None:
    """
    Legacy → mappe sur ENGINE_SUBMIT_TO_ACK_MS du nouveau module.
    """
    try:
        dt_ms = max(0.0, (time.perf_counter_ns() - int(submit_ts_ns)) / 1000000.0)
        ENGINE_SUBMIT_TO_ACK_MS.observe(dt_ms)
    except Exception:
        log.exception('mark_engine_ack failed')

def record_pipeline_timings(trace: Dict[str, Any]) -> None:
    """
    Legacy → on mappe sur les histos Engine du nouveau module (best-effort).
    Attend des clés entières (ms) : t_engine_submit_ms, t_engine_ack_ms, t_first_fill_ms.
    """
    try:
        t_sub = trace.get('t_engine_submit_ms')
        t_ack = trace.get('t_engine_ack_ms')
        t_ff = trace.get('t_first_fill_ms')
        if isinstance(t_sub, (int, float)) and isinstance(t_ack, (int, float)) and (t_ack >= t_sub):
            ENGINE_SUBMIT_TO_ACK_MS.observe(float(t_ack - t_sub))
        if isinstance(t_sub, (int, float)) and isinstance(t_ff, (int, float)) and (t_ff >= t_sub):
            ENGINE_ACK_TO_FILL_MS.observe(float(t_ff - t_sub))
    except Exception:
        log.exception('record_pipeline_timings failed')

def inc_scanner_rejection(reason: str, route: str='n/a', pair: str='n/a') -> None:
    """
    Legacy → mappe sur SCANNER_REJECTIONS_TOTAL (labels: reason).
    Les labels route/pair sont conservés pour compat mais non utilisés par la nouvelle métrique.
    """
    try:
        SCANNER_REJECTIONS_TOTAL.labels(str(reason)).inc()
    except Exception:
        log.exception('inc_scanner_rejection failed')

def inc_scanner_emitted(route: str='n/a', pair: str='n/a') -> None:
    """
    Legacy → mappe sur SCANNER_EMITTED_TOTAL.
    """
    try:
        SCANNER_EMITTED_TOTAL.inc()
    except Exception:
        log.exception('inc_scanner_emitted failed')

def observe_scanner_latency(route: str, seconds: float) -> None:
    """
    Legacy → mappe sur ROUTER_TO_SCANNER_MS (route, ms).
    """
    try:
        ROUTER_TO_SCANNER_MS.labels(str(route)).observe(max(0.0, float(seconds) * 1000.0))
    except Exception:
        log.exception('observe_scanner_latency failed')

def set_engine_running(flag: bool) -> None:
    try:
        log.debug('set_engine_running(%s) [legacy no-op]', flag)
    except Exception:
        pass

class ObsServer:
    """
    Serveur HTTP tolérant :
      - Si aiohttp est disponible → expose /health, /metrics, /api/*, /central/*
      - Sinon → démarre seulement un /metrics Prometheus via start_http_server(port)
    Utilisation :
        srv = ObsServer(port=9108)
        srv.register_modules(engine=..., risk=..., scanner=..., router=..., central_watchdog=...)
        await srv.start()
    """

    def __init__(self, *, host: str='0.0.0.0', port: int=9108) -> None:
        self.host = host
        self.port = int(port)
        self._runner: Optional['web.AppRunner'] = None
        self._site: Optional['web.TCPSite'] = None
        self._modules: Dict[str, Any] = {}
        self._started_prometheus_only = False

    def register_modules(self, **modules: Any) -> None:
        self._modules.update({k: v for k, v in modules.items() if v is not None})

    async def start(self) -> None:
        if web is None:
            if not self._started_prometheus_only:
                start_http_server(self.port)
                self._started_prometheus_only = True
                log.info('ObsServer (shim): prometheus_client /metrics on :%d (aiohttp absent)', self.port)
            return
        app = web.Application()

        @web.middleware
        async def _cors_mw(request, handler):
            resp = await handler(request)
            resp.headers['Access-Control-Allow-Origin'] = '*'
            return resp
        app.middlewares.append(_cors_mw)
        app.add_routes([web.get('/health', self._health), web.get('/metrics', self._metrics), web.get('/api/engine/status', self._engine_status), web.get('/engine/status', self._engine_status), web.get('/api/risk/snapshot', self._risk_snapshot), web.get('/risk/snapshot', self._risk_snapshot), web.get('/api/scanner/snapshot', self._scanner_snapshot), web.get('/scanner/snapshot', self._scanner_snapshot), web.get('/api/router/health', self._router_health), web.get('/router/health', self._router_health), web.get('/central/last_event', self._central_last_event), web.get('/central/status', self._central_status)])
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()
        log.info('ObsServer (shim) started on %s:%d (aiohttp)', self.host, self.port)

    async def stop(self) -> None:
        if self._site:
            await self._site.stop()
        if self._runner:
            await self._runner.cleanup()
        self._site = None
        self._runner = None

    async def _health(self, request):
        statuses: Dict[str, Any] = {}
        healthy = True
        for name, mod in self._modules.items():
            try:
                st = mod.get_status() if hasattr(mod, 'get_status') else {'healthy': True}
            except Exception:
                st = {'healthy': False, 'details': 'get_status failed'}
            statuses[name] = st
            healthy = healthy and bool(st.get('healthy', True))
        return web.json_response({'healthy': healthy, 'modules': statuses})

    async def _metrics(self, request):
        data = generate_latest(REGISTRY)
        return web.Response(body=data, content_type=CONTENT_TYPE_LATEST)

    async def _engine_status(self, request):
        eng = self._modules.get('engine')
        out: Dict[str, Any] = {}
        if eng:
            snap = None
            for name in ('export_status', 'snapshot', 'status', 'to_dict', 'export'):
                if hasattr(eng, name):
                    res = getattr(eng, name)()
                    if asyncio.iscoroutine(res):
                        res = await res
                    snap = res
                    break
            if isinstance(snap, dict):
                out = snap
            else:
                out = {'status': getattr(eng, 'state', 'running'), 'latency_ms': getattr(eng, 'latency_ms', {'ack_p50': None, 'fill_p50': None})}
        return web.json_response(out or {'status': 'unknown'})

    async def _risk_snapshot(self, request):
        rm = self._modules.get('risk')
        out: Dict[str, Any] = {}
        if rm:
            snap = None
            for name in ('export_snapshot', 'snapshot', 'to_dict', 'export'):
                if hasattr(rm, name):
                    res = getattr(rm, name)()
                    if asyncio.iscoroutine(res):
                        res = await res
                    snap = res
                    break
            if isinstance(snap, dict):
                out = snap
            else:
                fees = None
                for n in ('export_fees_snapshot', 'get_all_fees'):
                    if hasattr(rm, n):
                        fees = getattr(rm, n)()
                        break
                prud = 'unknown'
                vm = getattr(rm, 'volatility_monitor', None)
                if vm is not None:
                    prud = getattr(vm, 'prudence', None) or getattr(vm, 'get_prudence_signal', lambda *_: 'unknown')('ALL')
                out = {'fees': fees or {}, 'slippage': getattr(rm, '_ext_slip', {}), 'volatility_bps': getattr(rm, '_ext_vol_bps', {}), 'prudence': prud}
        return web.json_response(out or {})

    async def _scanner_snapshot(self, request):
        sc = self._modules.get('scanner')
        out: Dict[str, Any] = {}
        if sc:
            snap = None
            for name in ('export_snapshot', 'snapshot', 'to_dict', 'export'):
                if hasattr(sc, name):
                    res = getattr(sc, name)()
                    if asyncio.iscoroutine(res):
                        res = await res
                    snap = res
                    break
            if isinstance(snap, dict):
                out = snap
            else:
                opps = []
                for name in ('get_recent_opportunities', 'recent_opportunities', 'opportunities'):
                    if hasattr(sc, name):
                        data = getattr(sc, name)
                        data = data() if callable(data) else data
                        if isinstance(data, list):
                            opps = data
                            break
                out = {'opportunities': opps, 'summary': {'count': len(opps)}}
        return web.json_response(out or {})

    async def _router_health(self, request):
        try:
            return web.json_response({'exchanges': _ROUTER_HEALTH_CACHE})
        except Exception:
            return web.json_response({'error': 'failed to read router health'}, status=500)

    async def _central_last_event(self, request):
        cw = self._modules.get('central_watchdog')
        if not cw or not hasattr(cw, 'get_status'):
            return web.json_response({'error': 'central_watchdog not registered'}, status=404)
        try:
            st = cw.get_status() or {}
            return web.json_response({'last_event': st.get('last_event')})
        except Exception:
            return web.json_response({'error': 'failed to read central last event'}, status=500)

    async def _central_status(self, request):
        cw = self._modules.get('central_watchdog')
        if not cw or not hasattr(cw, 'get_status'):
            return web.json_response({'error': 'central_watchdog not registered'}, status=404)
        try:
            return web.json_response(cw.get_status())
        except Exception:
            return web.json_response({'error': 'failed to read central status'}, status=500)
__all__ = sorted(set(_OBS_ALL) | {'ObsServer', 'router_health_on_beat', 'mark_engine_ack', 'record_pipeline_timings', 'inc_scanner_rejection', 'inc_scanner_emitted', 'observe_scanner_latency', 'set_engine_running'})