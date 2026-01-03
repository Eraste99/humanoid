# modules/websockets_clients.py
# -*- coding: utf-8 -*-
"""
WebSocketExchangeClient — Binance + Coinbase Exchange + Bybit (Spot L1+L2)
V2 unifiée avec BackoffPolicy intégrée.

Points clés
-----------
- Backoff intégré: jitter "décorrélé" (AWS-style), bornes exposées pour les tests.
- API exposée pour tests:
    * on_close() déclenche un cycle de reconnexion
    * reconnect_with_backoff() / reconnect()
    * compute_backoff(), last_backoff, next_delay
    * min_backoff/base_backoff, max_backoff
- Abonnements dédupliqués (set), resubscribe à l’ouverture.
- reload() / update_pairs() font un soft-reload (re-chunk + re-subscribe).
- Superviseur qui gère les listeners et redémarre en cas de panne.
- L1 (bookTicker/ticker) + L2 (depth/level2) fusionnés dans un event unique.

Compatibilité tests
-------------------
Les tests recherchent: on_close(), reconnect/reconnect_with_backoff(), compute_backoff(), et
les attributs min_backoff/base_backoff/max_backoff + next_delay/last_backoff.
"""

from __future__ import annotations
import json
import time
import random
import socket
import logging
import asyncio
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple
from collections import defaultdict, OrderedDict, deque
from dataclasses import dataclass
import websockets  # pip install websockets
from modules.retry_policy import with_retry, awith_retry, BackoffPolicy, ErrKind
# --- imports locaux (pas d'effets globaux) ---
import asyncio, json, random
from contracts.payloads import MarketEvent


# --- Prometheus: WS reconnect/backoff (fallback no-op si absent) ---
try:
    from modules.obs_metrics import (
        WS_RECONNECTS_TOTAL,
        WS_BACKOFF_SECONDS,
        WS_CONNECTIONS_OPEN,
        note_ws_public_cfg,
        WS_SYMBOL_UNMAPPED_TOTAL,
    )
except Exception:  # pragma: no cover
    class _Noop:
        def labels(self, *a, **k): return self
        def inc(self, *a, **k): return
        def observe(self, *a, **k): return
        def set(self, *a, **k): return
    WS_RECONNECTS_TOTAL = _Noop()
    WS_BACKOFF_SECONDS  = _Noop()
    WS_CONNECTIONS_OPEN = _Noop()
    WS_PUBLIC_DROPPED_TOTAL = _Noop()
    WS_SYMBOL_UNMAPPED_TOTAL = _Noop()

    def note_ws_public_cfg(*_a, **_k):
        return None

# --- Helpers d'observabilité WS publics (labels exchange / region / deployment_mode) ---
try:
    from modules.obs_metrics import (
        ws_public_note_connection_open,
        ws_public_note_connection_closed,
        ws_public_note_reconnect,
        ws_public_note_event_ok,
        ws_public_note_event_dropped,
        ws_public_staleness,
    )
except Exception:  # pragma: no cover
    # Fallback no-op : on ne casse jamais le flux WS si obs_metrics n'est pas à jour
    def ws_public_note_connection_open(*a, **k):  # type: ignore[no-redef]
        return None

    def ws_public_note_connection_closed(*a, **k):  # type: ignore[no-redef]
        return None

    def ws_public_note_reconnect(*a, **k):  # type: ignore[no-redef]
        return None

    def ws_public_note_event_ok(*a, **k):  # type: ignore[no-redef]
        return None

    def ws_public_note_event_dropped(*a, **k):  # type: ignore[no-redef]
        return None

    def ws_public_staleness(*a, **k):  # type: ignore[no-redef]
        return None


logger = logging.getLogger("WebSocketExchangeClient")

# --- Backoff WS local (pas celui de retry_policy) ---
from dataclasses import field

@dataclass
class WsBackoffPolicy:
    base: float = 0.5
    cap: float = 30.0
    jitter: bool = True
    _curr: float = field(default=0.0, init=False)

    def reset(self) -> None:
        self._curr = 0.0

    def next_delay(self, rng: random.Random) -> float:
        # Decorrelated jitter (AWS style)
        last = self._curr or self.base
        if self.jitter:
            # uniform in [base, min(cap, last*3)]
            d = rng.uniform(self.base, min(self.cap, last * 3.0))
        else:
            # expo borné sans jitter
            d = min(self.cap, last * 2.0)
        self._curr = d
        return d


# ======================== BackoffPolicy unifiée ========================

# =================== helpers structures locales (L1/L2) ===================
@dataclass
class Level:
    px: float
    qty: float

@dataclass
class BookSnapshot:
    bids: List[Level]
    asks: List[Level]


def _set_tcp_nodelay(ws):
    try:
        transport = getattr(ws, "transport", None)
        sock = transport.get_extra_info("socket") if transport else None
        if sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except Exception:
        logging.debug("TCP_NODELAY set failed", exc_info=True)


# =========================== Client WebSockets ===========================
class WebSocketExchangeClient:
    # Endpoints tri-CEX
    BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"
    COINBASE_WS     = "wss://ws-feed.exchange.coinbase.com"
    BYBIT_WS_SPOT   = "wss://stream.bybit.com/v5/public/spot"

    def __init__(
        self,
        cfg,
        url: str,
        loop=None,
        *,
        # orchestrateur
        pairs: list,
        out_queue: "asyncio.Queue",
        pair_mapping: dict,
        verbose: bool = False,
        enabled_exchanges: list | None = None,   # ["BINANCE","COINBASE","BYBIT"]
        # chunk sizes
        binance_chunk_size: int = 12,
        coinbase_chunk_size: int = 25,
        bybit_chunk_size: int = 25,
        # depths/intervals
        depth_level: int = 10,             # Binance depth level (5/10/20...)
        binance_interval_ms: int = 100,    # 100ms
        supervisor_backoff_s: float = 1.0, # backoff superviseur (panne inattendue)
        # backoff policy (laisser None pour prendre cfg.ws_public)
        base_backoff: float | None = None,   # secondes (ex: 0.5s)
        max_backoff:  float | None = None,   # secondes (ex: 30s)
        jitter: bool | None = None,
        # config optionnelle annexe (ex: seed tests)
        config: object | None = None,
        shard_id: str = "S0",
    ):
        """
        Client WS public, **config-driven** par cfg.ws_public + paramètres runtime.
        - backoff: base/max/jitter (ms dans cfg, secondes ici)
        - timeouts: connect/read (s)
        - heartbeats: ping/pong (s)
        - auto_resubscribe, max_retries
        - gestion multi-exchanges (pairs, chunking, depth, out_queue)
        """

        # --- wiring de base ---
        self.cfg = cfg
        self.url = url
        self.loop = loop
        self.verbose = bool(verbose)
        self.shard_id = str(shard_id)

        # --- contexte région / mode (pod) ---
        # Source: BotConfig / Boot (pas de heuristique locale ici)
        base_cfg = getattr(cfg, "bot_cfg", cfg)
        pod_region = getattr(base_cfg, "POD_REGION", "EU")
        dep_mode = getattr(base_cfg, "DEPLOYMENT_MODE", "EU_ONLY")

        # Normalisation en MAJUSCULES pour les labels Prometheus
        self.region = str(pod_region).upper()
        self.deployment_mode = str(dep_mode).upper()

        # --- cfg.ws_public (source de vérité) ---
        wscfg = self.cfg.ws_public
        bo_cfg = dict(getattr(wscfg, "ws_backoff", {}))
        cfg_bo_base_s = float(bo_cfg.get("base_ms", 500)) / 1000.0
        cfg_bo_cap_s = float(bo_cfg.get("max_ms", 15_000)) / 1000.0
        cfg_bo_jitter = bool(bo_cfg.get("jitter", 1))

        # --- cfg.ws_public (source de vérité) ---
        wscfg = self.cfg.ws_public
        bo_cfg = dict(getattr(wscfg, "ws_backoff", {}))
        cfg_bo_base_s = float(bo_cfg.get("base_ms", 500)) / 1000.0
        cfg_bo_cap_s  = float(bo_cfg.get("max_ms", 15_000)) / 1000.0
        cfg_bo_jitter = bool(bo_cfg.get("jitter", 1))

        # Priorité args > cfg > defaults
        self._bo_base_s = float(base_backoff if base_backoff is not None else cfg_bo_base_s)
        self._bo_cap_s  = float(max_backoff  if max_backoff  is not None else cfg_bo_cap_s)
        self._bo_jitter = bool(jitter        if jitter       is not None else cfg_bo_jitter)

        self._connect_timeout_s = int(getattr(wscfg, "connect_timeout_s", 10))
        self._read_timeout_s    = int(getattr(wscfg, "read_timeout_s", 30))
        self._ping_interval_s   = int(getattr(wscfg, "ping_interval_s", 20))
        self._pong_timeout_s    = int(getattr(wscfg, "pong_timeout_s", 10))
        self._auto_resubscribe  = bool(getattr(wscfg, "auto_resubscribe", True))
        self._max_retries       = int(getattr(wscfg, "max_retries", 0))

        # --- orchestrateur / univers ---
        self.pairs = list(pairs)
        self.out_queue = out_queue
        self.pair_mapping = dict(pair_mapping)          # canon -> {ex:"EXSYM"}
        self.enabled_exchanges = [e.upper() for e in (enabled_exchanges or ["BINANCE","COINBASE","BYBIT"])]

        # chunking
        # chunking
        self.binance_chunk_size = max(1, int(binance_chunk_size))
        self.coinbase_chunk_size = max(1, int(coinbase_chunk_size))
        self.bybit_chunk_size = max(1, int(bybit_chunk_size))

        try:
            note_ws_public_cfg(
                ping_interval_s=self._ping_interval_s,
                pong_timeout_s=self._pong_timeout_s,
                connect_timeout_s=self._connect_timeout_s,
                read_timeout_s=self._read_timeout_s,
                out_queue_put_timeout_s=self._out_queue_put_timeout_s,
                chunk_size_by_exchange={
                    "BINANCE": self.binance_chunk_size,
                    "COINBASE": self.coinbase_chunk_size,
                    "BYBIT": self.bybit_chunk_size,
                },
            )
        except Exception:
            pass

        # depth / cadence exchange-specific
        self.depth_level = int(depth_level)
        self.binance_interval_ms = int(binance_interval_ms)

        # supervision
        self._supervisor_backoff_s = float(supervisor_backoff_s)

        self._ws_policy = WsBackoffPolicy(base=self._bo_base_s, cap=self._bo_cap_s, jitter=self._bo_jitter)
        self.base_backoff = float(self._bo_base_s)
        self.min_backoff  = float(self._bo_base_s)  # alias pour tests
        self.max_backoff  = float(self._bo_cap_s)
        self.last_backoff: float = 0.0
        self.next_delay: float = 0.0

        # RNG deterministe optionnelle (tests)
        seed_val = getattr(wscfg, "ws_backoff_seed", None)
        self._rng = random.Random(int(seed_val)) if seed_val is not None else random.Random()

        # --- RL buckets (fallback no-op si non fournis par cfg) ---
        class _NopBucket:
            def acquire(self, *a, **k): return None

        self._rl_unsub_bucket = getattr(cfg, "ws_unsub_bucket", _NopBucket())
        self._rl_sub_bucket = getattr(cfg, "ws_sub_bucket", _NopBucket())

        # --- lifecycle/supervision & state ---
        self._session = None
        self._ws = None
        self._stopping = False
        self._running = False
        self._supervisor_task: Optional["asyncio.Task"] = None
        self.tasks: List["asyncio.Task"] = []  # listeners actifs
        self._reload_event = asyncio.Event()
        self._backoff_inflight = asyncio.Lock()

        # connexions / métriques
        self._open_connections = 0
        self._open_by_exchange: Dict[str, int] = defaultdict(int)
        try:
            for _ex in ("BINANCE", "COINBASE", "BYBIT"):
                WS_CONNECTIONS_OPEN.labels(exchange=_ex).set(0)  # type: ignore[name-defined]
        except Exception:
            pass

        # --- JSON fast-path ---
        try:
            import orjson as _oj  # type: ignore
            self._json_loads = _oj.loads
            self._json_dumps = _oj.dumps
        except Exception:
            self._json_loads = json.loads
            self._json_dumps = lambda x: json.dumps(x).encode()

        # --- caches L1/L2 : {exchange: {ex_symbol: ...}} ---
        self._l1: Dict[str, Dict[str, Tuple[float, float, int]]] = {"BINANCE": {}, "COINBASE": {}, "BYBIT": {}}
        self._l2: Dict[str, Dict[str, Tuple[list, list, int]]]   = {"BINANCE": {}, "COINBASE": {}, "BYBIT": {}}

        # --- télémétrie ---
        self.last_update: Dict[str, Dict[str, int]] = {"BINANCE": {}, "COINBASE": {}, "BYBIT": {}}
        self.latency: Dict[str, Dict[str, Optional[int]]] = {"BINANCE": {}, "COINBASE": {}, "BYBIT": {}}

        # --- abonnements & mapping ---
        self._subs: set[str] = set()       # sujets demandés
        self._subscribed: set[str] = set()  # sujets effectifs
        self._connected = False
        self._seen_events = OrderedDict()
        self._seen_max = 50_000
        self._out_queue_put_timeout_s = float(getattr(wscfg, "out_queue_put_timeout_s", 0.05))
        self._out_queue_drops: Dict[str, int] = defaultdict(int)
        self._out_queue_last_drop_reason: Dict[str, str] = {}
        self._out_queue_last_drop_ts: Dict[str, float] = defaultdict(float)
        self._bp_last_emit_ts: Dict[str, float] = defaultdict(float)
        self._last_publish_ts: float = 0.0
        self._last_publish_by_exchange: Dict[str, float] = defaultdict(float)
        self._bp_last_log_ts: Dict[str, float] = defaultdict(float)
        now_ms = int(time.time() * 1000)
        self._last_recv_ts_ms: Dict[str, int] = {
            "BINANCE": now_ms,
            "COINBASE": now_ms,
            "BYBIT": now_ms,
        }
        self._disabled_last_note_ts: Dict[str, float] = defaultdict(float)
        self._staleness_task: Optional["asyncio.Task"] = None
        self._control_events_pending: Deque[Dict[str, Any]] = deque(maxlen=64)
        self._snapshot_runs = 0
        self._snapshot_pairs = 0
        self._snapshot_last_ts = 0.0
        self._unmapped_seen: Dict[str, int] = defaultdict(int)

        # mapping inverse exchange_symbol -> pair_key
        try:
            self._rebuild_inv_map()  # si la méthode existe déjà dans la classe
        except AttributeError:
            # fallback simple si non présente
            self._inv_map = {}

    def _ws_connect_kwargs(self) -> Dict[str, float]:
        return {
            "ping_interval": self._ping_interval_s,
            "ping_timeout": self._pong_timeout_s,
            "close_timeout": self._read_timeout_s,
            "open_timeout": self._connect_timeout_s,
        }
    def _unsubscribe_impl(self, batch: List[str]) -> None:
        # Hook d’implémentation spécifique CEX si nécessaire.
        # Par défaut, les listeners reconstruisent la souscription côté serveur → no-op ici.
        try:
            logger.debug('{"ws_unsub_impl":"noop","batch":%d}', len(batch))
        except Exception:
            pass

    def _subscribe_impl(self, batch: List[str]) -> None:
        # Hook d’implémentation spécifique CEX si nécessaire.
        try:
            logger.debug('{"ws_sub_impl":"noop","batch":%d}', len(batch))
        except Exception:
            pass

    async def _maybe_await(self, fn, *args, **kwargs):
        """
        Exécute fn(...), qu'il soit sync ou async, sans bloquer l'event loop.
        """
        res = fn(*args, **kwargs)
        if asyncio.iscoroutine(res):
            return await res
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: res)
    async def aupdate_pairs(
        self,
        add: Optional[List[str]] = None,
        remove: Optional[List[str]] = None,
        chunk_size: int = 100,
        jitter_ms: int = 200,
    ) -> None:
        """
        Applique les diffs d'abonnements sans bloquer :
        - Unsubs en chunks puis Subs en chunks
        - Pacing via RL buckets
        - Jitter pour étaler la charge
        """
        add = add or []
        remove = remove or []
        cfg_jitter_ms = int(getattr(self.cfg.ws_public, "update_pairs_jitter_ms", 200))
        if jitter_ms is None or (jitter_ms == 200 and cfg_jitter_ms != 200):
            jitter_ms = cfg_jitter_ms

        # 1) Unsubscribe d'abord
        for i in range(0, len(remove), chunk_size):
            batch = remove[i:i+chunk_size]
            try:
                self._rl_unsub_bucket.acquire(1)
            except Exception:
                logging.exception("RL acquire (unsub) failed")
            await self._maybe_await(self._unsubscribe_impl, batch)
            self._subscribed.difference_update(batch)
            await asyncio.sleep((jitter_ms + self._rng.randint(0, jitter_ms)) / 1000.0)

        # 2) Subscribe ensuite
        for i in range(0, len(add), chunk_size):
            batch = add[i:i+chunk_size]
            try:
                self._rl_sub_bucket.acquire(1)
            except Exception:
                logging.exception("RL acquire (sub) failed")
            await self._maybe_await(self._subscribe_impl, batch)
            self._subscribed.update(batch)
            await asyncio.sleep((jitter_ms + self._rng.randint(0, jitter_ms)) / 1000.0)

    def update_pairs(
        self,
        add: Optional[List[str]] = None,
        remove: Optional[List[str]] = None,
        chunk_size: int = 100,
        jitter_ms: int = 200,
    ) -> None:
        """
        Wrapper non-bloquant :
        - si une event loop est présente : planifie aupdate_pairs(...)
        - sinon : exécute via asyncio.run (thread superviseur)
        """
        cfg_jitter_ms = int(getattr(self.cfg.ws_public, "update_pairs_jitter_ms", 200))
        if jitter_ms is None or (jitter_ms == 200 and cfg_jitter_ms != 200):
            jitter_ms = cfg_jitter_ms
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.aupdate_pairs(add, remove, chunk_size, jitter_ms))
        except RuntimeError:
            asyncio.run(self.aupdate_pairs(add, remove, chunk_size, jitter_ms))

    # ---------- Snapshots en vagues (async) ----------
    async def aschedule_snapshots_in_waves(
        self,
        pairs: List[str],
        per_minute: int = 50,
    ) -> None:
        """
        Prend des snapshots L2 par vagues (~per_minute) sans bloquer.
        Étale naturellement la charge sur ~60s entre lots.
        """
        if not pairs:
            return
        batch: List[str] = []
        loop = asyncio.get_running_loop()
        t0 = loop.time()

        for p in pairs:
            batch.append(p)
            if len(batch) >= per_minute:
                await self._maybe_await(self._take_snapshot, batch)
                batch.clear()
                # cadence ~1 minute par lot
                while (loop.time() - t0) < 60.0:
                    await asyncio.sleep(0.5)
                t0 = loop.time()

        if batch:
            await self._maybe_await(self._take_snapshot, batch)

    def schedule_snapshots_in_waves(
        self,
        pairs: List[str],
        per_minute: int = 50,
    ) -> None:
        """
        Wrapper non-bloquant :
        - si une event loop est présente : planifie aschedule_snapshots_in_waves(...)
        - sinon : exécute via asyncio.run (thread superviseur)
        """
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.aschedule_snapshots_in_waves(pairs, per_minute))
        except RuntimeError:
            asyncio.run(self.aschedule_snapshots_in_waves(pairs, per_minute))

    async def _take_snapshot(self, batch: List[str]) -> None:
        if not batch:
            return
        emitted = 0
        missing: List[str] = []
        for pair_key in batch:
            mapping = self.pair_mapping.get(pair_key) or {}
            for ex, symbol in mapping.items():
                if not symbol:
                    continue
                sym = str(symbol).upper()
                exu = str(ex).upper()
                l1 = self._l1.get(exu, {}).get(sym)
                l2 = self._l2.get(exu, {}).get(sym)
                if not l1 or not l2:
                    missing.append(f"{exu}:{pair_key}")
                    continue
                try:
                    await self._emit_if_ready(exu, sym)
                    emitted += 1
                except Exception:
                    logger.exception("[WS] snapshot emit failed", extra={"exchange": exu, "pair": pair_key})
        self._snapshot_runs += 1
        self._snapshot_pairs += len(batch)
        self._snapshot_last_ts = time.time()
        if missing and self.verbose:
            logger.debug("[WS] snapshot missing feeds: %s", ",".join(missing[:10]))

    async def _do_close(self) -> None:
        for task in list(self.tasks):
            try:
                task.cancel()
            except Exception:
                pass
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
        self._open_connections = 0
        self._connected = False

    async def _do_open(self) -> None:
        self._reload_event.set()

    # --- métriques internes par exchange ---
    def _metrics_conn_open(self, exchange: str) -> None:
        ex = exchange.upper()
        try:
            # compteur local par exchange
            self._open_by_exchange[ex] = max(0, int(self._open_by_exchange.get(ex, 0)) + 1)
            # métrique legacy (exchange-only) pour compatibilité
            WS_CONNECTIONS_OPEN.labels(exchange=ex).set(self._open_by_exchange[ex])  # type: ignore[name-defined]
        except Exception:
            logging.exception("Unhandled exception in _metrics_conn_open")
        # helper high-level labelisé exchange / region / deployment_mode
        try:
            ws_public_note_connection_open(  # type: ignore[name-defined]
                exchange=ex,
                region=getattr(self, "region", "EU"),
                deployment_mode=getattr(self, "deployment_mode", "EU_ONLY"),
                open_count=self._open_by_exchange.get(ex, 0),
            )
        except Exception:
            # on ne casse jamais le flux WS pour un problème de métriques
            pass

    def _metrics_conn_closed(self, exchange: str) -> None:
        ex = exchange.upper()
        try:
            self._open_by_exchange[ex] = max(0, int(self._open_by_exchange.get(ex, 0)) - 1)
            if self._open_by_exchange[ex] <= 0:
                self._open_by_exchange[ex] = 0
            # métrique legacy (exchange-only)
            WS_CONNECTIONS_OPEN.labels(exchange=ex).set(self._open_by_exchange[ex])  # type: ignore[name-defined]
        except Exception:
            pass
        # helper high-level labelisé exchange / region / deployment_mode
        try:
            ws_public_note_connection_closed(  # type: ignore[name-defined]
                exchange=ex,
                region=getattr(self, "region", "EU"),
                deployment_mode=getattr(self, "deployment_mode", "EU_ONLY"),
                open_count=self._open_by_exchange.get(ex, 0),
            )
        except Exception:
            pass

    def _metrics_reconnect(self, exchange: str, delay_s: float, *, reason: str) -> None:
        ex = exchange.upper()
        # métriques legacy (compat dashboards actuels)
        try:
            try:
                WS_RECONNECTS_TOTAL.labels(exchange=ex, reason=reason).inc()  # type: ignore[name-defined]
            except Exception:
                WS_RECONNECTS_TOTAL.labels(exchange=ex).inc()  # type: ignore[name-defined]
            WS_BACKOFF_SECONDS.labels(exchange=ex).set(max(0.0, float(delay_s)))  # type: ignore[name-defined]
        except Exception:
            pass
        # helper high-level avec labels exchange / region / deployment_mode
        try:
            ws_public_note_reconnect(  # type: ignore[name-defined]
                exchange=ex,
                region=getattr(self, "region", "EU"),
                deployment_mode=getattr(self, "deployment_mode", "EU_ONLY"),
                reason=reason,
                delay_s=float(delay_s),
            )
        except Exception:
            pass
        # logging texte inchangé
        try:
            logger.info(
                '{"ws_event":"reconnect_scheduled","exchange":"%s","delay_s":%.3f,'
                '"policy":"decorr_jitter","open_conns":%d,"reason":"%s"}',
                ex,
                float(delay_s),
                int(self._open_connections),
                reason,
            )
        except Exception:
            logger.exception("Erreur lors du log de reconnect WS")


    # ------------------- mapping utils -------------------
    def _rebuild_inv_map(self):
        self._inv_map = {
            "binance":  {v.get("binance"):  k for k, v in self.pair_mapping.items() if v.get("binance")},
            "coinbase": {v.get("coinbase"): k for k, v in self.pair_mapping.items() if v.get("coinbase")},
            "bybit":    {v.get("bybit"):    k for k, v in self.pair_mapping.items() if v.get("bybit")},
        }

    def _note_drop(self, exchange: str, *, reason: str, kind: str) -> None:
        ws_public_note_event_dropped(
            exchange=exchange,
            region=getattr(self, "region", "EU"),
            deployment_mode=getattr(self, "deployment_mode", "EU_ONLY"),
            reason=reason,
            kind=kind,
        )

    def _note_ok(self, exchange: str, *, kind: str) -> None:
        ws_public_note_event_ok(
            exchange=exchange,
            region=getattr(self, "region", "EU"),
            deployment_mode=getattr(self, "deployment_mode", "EU_ONLY"),
            kind=kind,
        )

    @staticmethod
    def _normalize_ts_ms(raw_ts: Any, recv_ts_ms: int) -> int:
        if raw_ts is None:
            return int(recv_ts_ms)
        if isinstance(raw_ts, str):
            ts = raw_ts.strip()
            if not ts:
                return int(recv_ts_ms)
            if "T" in ts or "t" in ts:
                try:
                    iso = ts.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(iso)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return int(dt.timestamp() * 1000)
                except Exception:
                    return int(recv_ts_ms)
            try:
                raw_ts = float(ts)
            except Exception:
                return int(recv_ts_ms)
        try:
            ts_val = float(raw_ts)
        except Exception:
            return int(recv_ts_ms)
        if ts_val < 1e12:
            return int(ts_val * 1000)
        return int(ts_val)

    def _exchange_region_disabled(self, exchange: str, pair_key: Optional[str] = None) -> bool:
        if getattr(self.cfg, "g", None) is not None:
            enable_jp = bool(getattr(self.cfg.g, "enable_jp", False))
        else:
            enable_jp = bool(getattr(self.cfg, "enable_jp", False))
        if enable_jp:
            return False
        if pair_key:
            mapping = self.pair_mapping.get(pair_key) or {}
            region_hint = (mapping.get("region_hint") or {}).get(exchange.upper())
            if str(region_hint).upper() == "JP":
                return True
        return False

    def unify_pair_key(self, exchange_symbol: str, exchange: str) -> Optional[str]:
        return self._inv_map.get(exchange.lower(), {}).get(exchange_symbol)

    def _note_frame_received(self, exchange: str, recv_ts_ms: int) -> None:
        self._last_recv_ts_ms[exchange.upper()] = int(recv_ts_ms)

    def _exchange_disabled_by_flag(self, exchange: str) -> bool:
        wscfg = getattr(self.cfg, "ws_public", None)
        disabled_raw = getattr(wscfg, "disabled_exchanges", []) if wscfg is not None else []
        disabled = set(str(e).upper() for e in (disabled_raw or []))
        return exchange.upper() in disabled

    def _maybe_note_disabled(self, exchange: str) -> None:
        ex = exchange.upper()
        now = time.time()
        if (now - self._disabled_last_note_ts[ex]) >= 60.0:
            self._note_drop(ex, reason="disabled_by_flag", kind="subscribe")
            self._disabled_last_note_ts[ex] = now

    async def _staleness_loop(self) -> None:
        wscfg = getattr(self.cfg, "ws_public", None)
        interval_s = float(getattr(wscfg, "staleness_interval_s", 1.0))
        slo_s = getattr(wscfg, "staleness_slo_s", None)
        while self._running:
            now_ms = int(time.time() * 1000)
            for ex in ("BINANCE", "COINBASE", "BYBIT"):
                if ex not in self.enabled_exchanges:
                    continue
                if self._exchange_disabled_by_flag(ex):
                    continue
                last_ms = self._last_recv_ts_ms.get(ex, now_ms)
                staleness_s = max(0.0, (now_ms - last_ms) / 1000.0)
                ws_public_staleness(
                    ex,
                    region=getattr(self, "region", "EU"),
                    deployment_mode=getattr(self, "deployment_mode", "EU_ONLY"),
                    seconds=staleness_s,
                )
                if isinstance(slo_s, (int, float)) and staleness_s > float(slo_s):
                    self._note_drop(ex, reason="stale", kind="health")
            await asyncio.sleep(max(0.2, interval_s))

    def get_symbol(self, pair_key: str, exchange: str) -> str:
        m = self.pair_mapping.get(pair_key)
        if not m:
            raise ValueError(f"Pair '{pair_key}' inconnue")
        s = m.get(exchange.lower())
        if not s:
            raise ValueError(f"Symbole manquant pour {exchange} / {pair_key}")
        return s

    @staticmethod
    def _chunks(items: List[str], n: int):
        for i in range(0, len(items), n):
            yield items[i:i+n]

    # ------------------- Abonnements (dédup) -------------------
    def subscribe(self, topic: str) -> None:
        """Dédup simple: garde un set et (ré)envoie à l'ouverture."""
        if topic in self._subs:
            return
        self._subs.add(topic)
        if self._connected:
            self._send_sub(topic)

    def _send_sub(self, topic: str) -> None:
        # Implémentation réelle: envoyer un frame SUBSCRIBE à l'exchange concerné.
        # Comme on gère 3 exchanges distincts ici, le resub se fait par listener.
        pass

    def _resubscribe_all(self) -> None:
        # Chaque listener reconstruit ses souscriptions à l'ouverture,
        # ce hook permet néanmoins de brancher des subscriptions communes si besoin.
        for _t in sorted(self._subs):
            self._send_sub(_t)

    # ------------------- Hooks WS (expo tests) -------------------
    def on_open(self, exchange: Optional[str] = None, *a, **k) -> None:
        # 1 socket de plus
        self._open_connections += 1
        self._connected = self._open_connections > 0
        if self._open_connections == 1:
            # reset backoff seulement quand la toute première socket s'ouvre
            self._ws_policy.reset()
            self.last_backoff = 0.0
            self.next_delay = 0.0
            try:
                logger.info('{"ws_policy":"reset_backoff","open_conns":1}')
            except Exception:
                logging.exception("Unhandled exception")
        else:
            # no-reset policy: on garde le backoff courant
            try:
                logger.info('{"ws_policy":"no_reset","open_conns":%d}', self._open_connections)
            except Exception:
                logging.exception("Unhandled exception")
            # marquer la readiness à la 1ère socket ouverte
        if hasattr(self, "_mark_ready"):
            self._mark_ready()

        self._resubscribe_all()
        if exchange:
            payload = {
                "__ws_reconnect__": True,
                "exchange": str(exchange).upper(),
                "reason": "listener_open",
                "ts_ms": int(time.time() * 1000),
            }
            self._queue_control_event(payload)

    def on_close(self, *a, **k) -> None:
        # 1 socket de moins
        self._open_connections = max(0, self._open_connections - 1)
        self._connected = self._open_connections > 0


    def compute_backoff(self) -> float:
        d = float(self._ws_policy.next_delay(self._rng))
        d = min(max(d, self.min_backoff), self.max_backoff)
        self.last_backoff = d
        self.next_delay = d
        return d


    def reconnect(self) -> None:
        self.reconnect_with_backoff()


    # ------------------- Émission pipeline -------------------
    async def _emit_if_ready(self, exchange: str, ex_symbol: str) -> None:
        ex = exchange.upper()
        l1 = self._l1[ex].get(ex_symbol)
        if not l1:
            return
        bid, ask, ex_ts = l1
        if bid <= 0 or ask <= 0 or ask < bid:
            self._note_drop(ex, reason="schema_mismatch", kind="emit")
            return
        bids, asks, l2_ts = self._l2[ex].get(ex_symbol, ([], [], ex_ts))
        now_ms = int(time.time() * 1000)
        ex_ts_norm = self._normalize_ts_ms(ex_ts, now_ms)
        lat = (now_ms - ex_ts_norm) if ex_ts_norm else None
        pk = self.unify_pair_key(ex_symbol, ex)
        if not pk:
            WS_SYMBOL_UNMAPPED_TOTAL.labels(exchange=ex).inc()
            self._note_drop(ex, reason="unknown_pair", kind="emit")
            self._unmapped_seen[ex] += 1
            return
        if self._exchange_region_disabled(ex, pk):
            self._note_drop(ex, reason="region_disabled_jp", kind="emit")
            return
        self.last_update[ex][pk] = now_ms
        self.latency[ex][pk] = lat
        try:
            event = MarketEvent(
                exchange=ex,
                pair_key=pk,
                ex_symbol=ex_symbol,
                best_bid=float(bid),
                best_ask=float(ask),
                bid_volume=float(bids[0][1]) if bids else 0.0,
                ask_volume=float(asks[0][1]) if asks else 0.0,
                orderbook={"bids": bids, "asks": asks} if (bids or asks) else {},
                exchange_ts_ms=int(ex_ts_norm) if ex_ts_norm else None,
                recv_ts_ms=now_ms,
                latency_ms=lat,
                active=True,
            ).model_dump(exclude_none=True)
            event["shard"] = self.shard_id
        except Exception:
            logger.exception("[WS] market event validation failed", extra={"exchange": ex, "symbol": ex_symbol})
            self._note_drop(ex, reason="schema_mismatch", kind="emit")
            return

        key = (ex, ex_symbol, int(ex_ts_norm or 0))
        if key in self._seen_events:
            self._note_drop(ex, reason="dedup", kind="emit")
            return
        self._seen_events[key] = True
        # LRU bornée
        if len(self._seen_events) > self._seen_max:
            self._seen_events.popitem(last=False)

        ok = await self._publish_event(event, exchange=ex, reason="emit")
        if not ok:
            now = time.time()
            if (now - self._bp_last_log_ts[ex]) >= 1.0:
                logger.warning(
                    '[WS] out_queue saturated (exchange=%s, symbol=%s) — event dropped',
                    ex,
                    ex_symbol,
                )
                self._bp_last_log_ts[ex] = now

    async def _publish_event(self, payload: Dict[str, Any], *, exchange: str, reason: str) -> bool:
        try:
            self.out_queue.put_nowait(payload)
            self._note_out_success(exchange)
            return True
        except asyncio.QueueFull:
            pass

        timeout = max(0.0, float(self._out_queue_put_timeout_s))
        if timeout == 0.0:
            self._note_backpressure(exchange, reason="queue_full")
            return False
        try:
            await asyncio.wait_for(self.out_queue.put(payload), timeout=timeout)
            self._note_out_success(exchange)
            return True
        except asyncio.TimeoutError:
            self._note_backpressure(exchange, reason="queue_full")
            return False

    def _note_out_success(self, exchange: str) -> None:
        now = time.time()
        self._last_publish_ts = now
        ex = exchange.upper()
        self._last_publish_by_exchange[ex] = now

        self._note_ok(ex, kind="combo")
        self._flush_control_events()

    def _note_backpressure(self, exchange: str, *, reason: str) -> None:
        ex = exchange.upper()
        self._out_queue_drops[ex] += 1
        self._out_queue_last_drop_reason[ex] = reason
        self._out_queue_last_drop_ts[ex] = time.time()


        self._note_drop(ex, reason=reason, kind="combo")

        try:
            logger.warning(
                "Backpressure on out_queue for %s, drop #%d, reason=%s",
                ex,
                self._out_queue_drops[ex],
                reason,
            )
        except Exception:
            logger.exception("Erreur lors du log de backpressure")

        now = time.time()
        if (now - self._bp_last_emit_ts[ex]) >= 1.0:
            payload = {
                "__ws_backpressure__": True,
                "exchange": ex,
                "reason": reason,
                "drops": self._out_queue_drops[ex],
                "queue_depth": getattr(self.out_queue, "qsize", lambda: 0)(),
                "ts_ms": int(now * 1000),
            }
            self._bp_last_emit_ts[ex] = now
            self._queue_control_event(payload)

    def _queue_control_event(self, payload: Dict[str, Any]) -> None:
        payload.setdefault("shard", self.shard_id)
        self._control_events_pending.append(payload)
        self._flush_control_events()

    def _flush_control_events(self) -> None:
        while self._control_events_pending:
            try:
                self.out_queue.put_nowait(self._control_events_pending[0])
                self._control_events_pending.popleft()
            except asyncio.QueueFull:
                break




    # ======================= BINANCE listener =======================
    async def _binance_listener_chunk(self, pairs_subset: List[str]):
        if self._exchange_disabled_by_flag("BINANCE"):
            self._maybe_note_disabled("BINANCE")
            return
        syms = []
        for p in pairs_subset:
            if self._exchange_region_disabled("BINANCE", p):
                self._note_drop("BINANCE", reason="region_disabled_jp", kind="subscribe")
                continue
            if self.pair_mapping.get(p, {}).get("binance"):
                syms.append(self.get_symbol(p, "binance").lower())
            if not syms:
                self._note_drop("BINANCE", reason="unknown_pair", kind="subscribe")
                return
        streams = "/".join([f"{s}@bookTicker" for s in syms] +
                           [f"{s}@depth{self.depth_level}@{self.binance_interval_ms}ms" for s in syms])
        url = f"{self.BINANCE_WS_BASE}?streams={streams}"
        while self._running:
            did_close = False
            try:
                async with websockets.connect(url, **self._ws_connect_kwargs()) as ws:
                    _set_tcp_nodelay(ws)
                    self.on_open(exchange="BINANCE")  # reset backoff + resub
                    self._metrics_conn_open("BINANCE")

                    async for raw in ws:
                        recv_ts_ms = int(time.time() * 1000)
                        self._note_frame_received("BINANCE", recv_ts_ms)
                        try:
                            msg = self._json_loads(raw)
                        except Exception:
                            self._note_drop("BINANCE", reason="parse_error", kind="frame")
                            continue
                        data = msg.get("data") or {}
                        stream = (msg.get("stream") or "").lower()
                        ex_symbol = stream.split("@")[0].upper() if "@" in stream else data.get("s", " ").upper()
                        if not ex_symbol:
                            self._note_drop("BINANCE", reason="schema_mismatch", kind="frame")
                            continue
                        recv_ts_ms = int(time.time() * 1000)
                        if data.get("e") == "bookTicker":
                            try:
                                bid = float(data.get("b") or data.get("B") or 0)
                                ask = float(data.get("a") or data.get("A") or 0)
                                if bid <= 0 or ask <= 0:
                                    self._note_drop("BINANCE", reason="schema_mismatch", kind="l1")
                                    continue
                            except Exception:
                                self._note_drop("BINANCE", reason="parse_error", kind="l1")
                                continue
                            ex_ts = self._normalize_ts_ms(data.get("T") or data.get("E"), recv_ts_ms)
                            self._l1["BINANCE"][ex_symbol] = (bid, ask, ex_ts)
                            await self._emit_if_ready("BINANCE", ex_symbol)
                            continue
                        if "bids" in data and "asks" in data:

                            bids = [(float(px), float(q)) for px, q, *_ in data.get("bids", [])]
                            asks = [(float(px), float(q)) for px, q, *_ in data.get("asks", [])]
                            ex_ts = self._normalize_ts_ms(data.get("T") or data.get("E"), recv_ts_ms)
                            bids = bids[: self.depth_level]
                            asks = asks[: self.depth_level]
                            self._l2["BINANCE"][ex_symbol] = (bids, asks, ex_ts)
                            await self._emit_if_ready("BINANCE", ex_symbol)
                            continue
                        self._note_drop("BINANCE", reason="schema_mismatch", kind="frame")
            except Exception as e:
                self._queue_control_event({
                    "__ws_error__": True,
                    "exchange": "BINANCE",
                    "reason": "listener_error",
                    "error": str(e),
                    "ts_ms": int(time.time() * 1000),
                })
                # backoff unifié (une seule fois)
                self.on_close();
                did_close = True
                self._metrics_conn_closed("BINANCE")
                await self._sleep_backoff("BINANCE", reason="backoff")

            finally:
                # si reload demandé -> fin immédiate sans double on_close()
                if self._reload_event.is_set():
                    return
                if not did_close:
                    self.on_close()
                    self._metrics_conn_closed("BINANCE")

    # ======================= COINBASE listener =======================
    async def _coinbase_listener_chunk(self, pairs_subset: List[str]):
        if self._exchange_disabled_by_flag("COINBASE"):
            self._maybe_note_disabled("COINBASE")
            return
        prods = []
        for p in pairs_subset:
            if self._exchange_region_disabled("COINBASE", p):
                self._note_drop("COINBASE", reason="region_disabled_jp", kind="subscribe")
                continue
            if self.pair_mapping.get(p, {}).get("coinbase"):
                prods.append(self.get_symbol(p, "coinbase"))

        if not prods:
            return
        sub = {
            "type": "subscribe",
            "product_ids": prods,
            "channels": [
                {"name": "ticker", "product_ids": prods},
                {"name": "level2", "product_ids": prods},
            ],
        }
        while self._running:
            did_close = False
            try:
                async with websockets.connect(self.COINBASE_WS, **self._ws_connect_kwargs()) as ws:
                    _set_tcp_nodelay(ws)
                    self.on_open(exchange="COINBASE")
                    self._metrics_conn_open("COINBASE")

                    await ws.send(json.dumps(sub))
                    l2_book: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(lambda: {"bids": {}, "asks": {}})
                    async for raw in ws:
                        recv_ts_ms = int(time.time() * 1000)
                        self._note_frame_received("COINBASE", recv_ts_ms)
                        try:
                            msg = self._json_loads(raw)
                        except Exception:
                            self._note_drop("COINBASE", reason="parse_error", kind="frame")
                            continue
                        t = msg.get("type")
                        pid = msg.get("product_id")
                        if not pid:
                            self._note_drop("COINBASE", reason="schema_mismatch", kind="frame")
                            continue
                        recv_ts_ms = int(time.time() * 1000)
                        if t == "ticker":
                            try:
                                best_bid = msg.get("best_bid")
                                best_ask = msg.get("best_ask")
                                if best_bid is None or best_ask is None:
                                    self._note_drop("COINBASE", reason="schema_mismatch", kind="l1")
                                    continue
                                bid = float(best_bid)
                                ask = float(best_ask)
                                if bid <= 0 or ask <= 0:
                                    self._note_drop("COINBASE", reason="schema_mismatch", kind="l1")
                                    continue
                            except Exception:
                                self._note_drop("COINBASE", reason="parse_error", kind="l1")
                                continue
                            ex_ts = self._normalize_ts_ms(msg.get("time"), recv_ts_ms)
                            self._l1["COINBASE"][pid] = (bid, ask, ex_ts)
                            await self._emit_if_ready("COINBASE", pid)
                            continue
                        if t in ("snapshot", "l2update"):
                            if t == "snapshot":
                                book = l2_book[pid]
                                book["bids"] = {px: float(sz) for px, sz in msg.get("bids", [])}
                                book["asks"] = {px: float(sz) for px, sz in msg.get("asks", [])}
                                ex_ts = self._normalize_ts_ms(msg.get("time"), recv_ts_ms)
                            else:
                                book = l2_book[pid]
                                ex_ts = self._normalize_ts_ms(msg.get("time"), recv_ts_ms)
                                for chg in msg.get("changes", []):
                                    side, px, sz = chg
                                    if side == "buy":
                                        if float(sz) == 0.0:
                                            book["bids"].pop(px, None)
                                        else:
                                            book["bids"][px] = float(sz)
                                    else:
                                        if float(sz) == 0.0:
                                            book["asks"].pop(px, None)
                                        else:
                                            book["asks"][px] = float(sz)
                            bids = sorted(((float(px), float(sz)) for px, sz in l2_book[pid]["bids"].items()),
                                          key=lambda x: x[0], reverse=True)[: self.depth_level]
                            asks = sorted(((float(px), float(sz)) for px, sz in l2_book[pid]["asks"].items()),
                                          key=lambda x: x[0])[: self.depth_level]
                            self._l2["COINBASE"][pid] = (bids, asks, ex_ts)
                            await self._emit_if_ready("COINBASE", pid)
                            continue
                        self._note_drop("COINBASE", reason="schema_mismatch", kind="frame")
            except Exception as e:
                self._queue_control_event({
                    "__ws_error__": True,
                    "exchange": "COINBASE",
                    "reason": "listener_error",
                    "error": str(e),
                    "ts_ms": int(time.time() * 1000),
                })
                self.on_close();
                did_close = True
                self._metrics_conn_closed("COINBASE")
                await self._sleep_backoff("COINBASE", reason="backoff")
            finally:
                if self._reload_event.is_set():
                    return
                if not did_close:
                    self.on_close()
                    self._metrics_conn_closed("COINBASE")

    # ======================= BYBIT listener =======================
    async def _bybit_listener_chunk(self, pairs_subset: List[str]):
        if self._exchange_disabled_by_flag("BYBIT"):
            self._maybe_note_disabled("BYBIT")
            return
        syms = []
        for p in pairs_subset:
            if self._exchange_region_disabled("BYBIT", p):
                self._note_drop("BYBIT", reason="region_disabled_jp", kind="subscribe")
                continue
            if self.pair_mapping.get(p, {}).get("bybit"):
                syms.append(self.get_symbol(p, "bybit"))
        if not syms:
            return
        args = [f"tickers.{s}" for s in syms] + [f"orderbook.50.{s}" for s in syms]
        sub = {"op": "subscribe", "args": args}
        while self._running:
            did_close = False
            try:
                async with websockets.connect(self.BYBIT_WS_SPOT, **self._ws_connect_kwargs()) as ws:
                    _set_tcp_nodelay(ws)
                    self.on_open(exchange="BYBIT")
                    self._metrics_conn_open("BYBIT")

                    await ws.send(self._json_dumps(sub))
                    async for raw in ws:
                        recv_ts_ms = int(time.time() * 1000)
                        self._note_frame_received("BYBIT", recv_ts_ms)
                        try:
                            msg = self._json_loads(raw)
                        except Exception:
                            self._note_drop("BYBIT", reason="parse_error", kind="frame")
                            continue
                        topic = msg.get("topic") or ""
                        data = msg.get("data")
                        if not topic or data is None:
                            self._note_drop("BYBIT", reason="schema_mismatch", kind="frame")
                            continue
                        parts = topic.split(".")
                        if len(parts) < 2:
                            self._note_drop("BYBIT", reason="schema_mismatch", kind="frame")
                            continue
                        ch, sym = parts[0], parts[-1]
                        recv_ts_ms = int(time.time() * 1000)
                        if ch == "tickers":
                            d = data if isinstance(data, dict) else (data[0] if data else {})
                            bid = float(d.get("bid1Price", 0.0) or d.get("bidPrice", 0.0))
                            ask = float(d.get("ask1Price", 0.0) or d.get("askPrice", 0.0))
                            if bid <= 0 or ask <= 0:
                                self._note_drop("BYBIT", reason="schema_mismatch", kind="l1")
                                continue
                            ex_ts = self._normalize_ts_ms(d.get("ts"), recv_ts_ms)
                            self._l1["BYBIT"][sym] = (bid, ask, ex_ts)
                            await self._emit_if_ready("BYBIT", sym)
                            continue
                        if ch == "orderbook":
                            d = data if isinstance(data, dict) else (data[0] if data else {})
                            bids = [(float(px), float(sz)) for px, sz, *_ in d.get("b", [])][: self.depth_level]
                            asks = [(float(px), float(sz)) for px, sz, *_ in d.get("a", [])][: self.depth_level]
                            ex_ts = self._normalize_ts_ms(d.get("ts"), recv_ts_ms)
                            self._l2["BYBIT"][sym] = (bids, asks, ex_ts)
                            await self._emit_if_ready("BYBIT", sym)
                            continue
                        self._note_drop("BYBIT", reason="schema_mismatch", kind="frame")
            except Exception as e:
                self._queue_control_event({
                    "__ws_error__": True,
                    "exchange": "BYBIT",
                    "reason": "listener_error",
                    "error": str(e),
                    "ts_ms": int(time.time() * 1000),
                })
                self.on_close();
                did_close = True
                self._metrics_conn_closed("BYBIT")
                await self._sleep_backoff("BYBIT", reason="backoff")
            finally:
                if self._reload_event.is_set():
                    return
                if not did_close:
                    self.on_close()
                    self._metrics_conn_closed("BYBIT")

    # ======================= supervisor =======================
    async def _supervisor(self) -> None:
        """Boucle de supervision: (re)construit/annule les listeners selon
        `self.pairs`, gère reloads et pannes inattendues.
        """
        while self._running:
            # Construire sous-listes par exchange selon présence du symbole
            binance_pairs = [p for p in self.pairs if self.pair_mapping.get(p, {}).get("binance")]
            coinbase_pairs = [p for p in self.pairs if self.pair_mapping.get(p, {}).get("coinbase")]
            bybit_pairs   = [p for p in self.pairs if self.pair_mapping.get(p, {}).get("bybit")]

            # Créer les tâches listeners
            self.tasks = []
            if "BINANCE" in self.enabled_exchanges and not self._exchange_disabled_by_flag("BINANCE"):
                for subset in self._chunks(binance_pairs, self.binance_chunk_size):
                    if subset:
                        self.tasks.append(asyncio.create_task(self._binance_listener_chunk(subset)))
            if "COINBASE" in self.enabled_exchanges and not self._exchange_disabled_by_flag("COINBASE"):
                for subset in self._chunks(coinbase_pairs, self.coinbase_chunk_size):
                    if subset:
                        self.tasks.append(asyncio.create_task(self._coinbase_listener_chunk(subset)))
            if "BYBIT" in self.enabled_exchanges and not self._exchange_disabled_by_flag("BYBIT"):
                for subset in self._chunks(bybit_pairs, self.bybit_chunk_size):
                    if subset:
                        self.tasks.append(asyncio.create_task(self._bybit_listener_chunk(subset)))

            if not self.tasks:
                # Rien à faire; attendre un reload ou l'arrêt
                for ex in self.enabled_exchanges:
                    if self._exchange_disabled_by_flag(ex):
                        self._maybe_note_disabled(ex)
                reload_fut = asyncio.create_task(self._reload_event.wait())
                done, _ = await asyncio.wait({reload_fut}, return_when=asyncio.FIRST_COMPLETED)
                if reload_fut in done:
                    self._reload_event.clear()
                continue

            # Attendre soit un reload, soit la fin d'une des tâches (panne)
            reload_fut = asyncio.create_task(self._reload_event.wait())
            wait_set = set(self.tasks) | {reload_fut}
            done, pending = await asyncio.wait(wait_set, return_when=asyncio.FIRST_COMPLETED)

            # Toujours annuler les listeners restants pour repartir proprement
            for t in self.tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)
            self.tasks.clear()

            if reload_fut in done:
                # Soft reload demandé
                self._reload_event.clear()
                # Loop immédiat pour re-chunk
                continue

            # Sinon: panne/fin inattendue d'un listener → petit backoff
            await asyncio.sleep(self._supervisor_backoff_s)

    # ======================= lifecycle =======================
    # --- à mettre dans WebSocketExchangeClient ------------------------------------


    async def start(self, wait_ready: float = 5.0) -> None:
        """
        Démarre le superviseur en tâche de fond (non bloquant).
        Attend un handshake rapide via ready_event, puis rend la main.
        """
        if getattr(self, "_running", False):
            return
        self._running = True

        # Event de readiness (créé si absent)
        if not hasattr(self, "ready_event"):
            self.ready_event = asyncio.Event()

        # Lancer le superviseur en tâche
        self._supervisor_task = asyncio.create_task(self._supervisor(),
                                                    name=f"{getattr(self, 'name', 'ws')}-supervisor")
        self._staleness_task = asyncio.create_task(self._staleness_loop(),
                                                   name=f"{getattr(self, 'name', 'ws')}-staleness")
        # Attente best-effort d’un handshake rapide (subscription ack / open)
        try:
            await asyncio.wait_for(self.ready_event.wait(), timeout=float(wait_ready))
        except asyncio.TimeoutError:
            logger.warning("[WS] Ready timeout après %.1fs — on continue quand même.", wait_ready)

    async def stop(self) -> None:
        """Arrêt idempotent avec join propre de la tâche superviseur."""
        self._running = False
        t = getattr(self, "_supervisor_task", None)
        if t:
            t.cancel()
            try:
                await t
            except Exception:
                pass
        self._supervisor_task = None
        st = getattr(self, "_staleness_task", None)
        if st:
            st.cancel()
            try:
                await st
            except Exception:
                pass
        self._staleness_task = None
        if hasattr(self, "ready_event"):
            try:
                self.ready_event.clear()
            except Exception:
                pass

    # Helper pour marquer la readiness (à appeler au bon moment)
    def _mark_ready(self) -> None:
        try:
            if hasattr(self, "ready_event") and not self.ready_event.is_set():
                self.ready_event.set()
        except Exception:
            pass

    # ============== API reload/resubscribe live ==============
    def get_current_subscriptions(self) -> List[str]:
        return list(self._subscribed)


    async def _sleep_backoff(self, exchange: str, *, reason: str) -> None:
        """Applique le backoff configuré avant de retenter une reconnexion."""
        async with self._backoff_inflight:
            delay = float(self.compute_backoff())
            try:
                self._metrics_reconnect(exchange, delay, reason=reason)
            except Exception:
                pass
            await asyncio.sleep(delay)

    def reconnect_with_backoff(self) -> None:
        """Entrée legacy : force un reload après un vrai backoff calculé."""
        async def _runner() -> None:
            await self._sleep_backoff("ALL", reason="reconnect")
            self._reload_event.set()

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_runner())
        except RuntimeError:  # pragma: no cover - hors boucle async
            asyncio.run(_runner())

    def reload(self) -> None:
        """Déclenche un soft-reload sans changer la liste de paires."""
        self._reload_event.set()

    # ======================= status =======================
    def get_status(self) -> Dict[str, Any]:
        return {
            "module": "WebSocketExchangeClient",
            "healthy": self._running and (self._supervisor_task is not None) and (not self._supervisor_task.done()),
            "last_update": self.last_update,
            "metrics": {
                "latency_ms": self.latency,
                "out_queue_drops": dict(self._out_queue_drops),
                "last_publish_ts": self._last_publish_ts,
                "last_publish_by_exchange": dict(self._last_publish_by_exchange),
                "snapshots": {
                    "runs": self._snapshot_runs,
                    "pairs_processed": self._snapshot_pairs,
                    "last_ts": self._snapshot_last_ts,
                },
            },
            "details": "WS actifs (tri-CEX: Binance/Coinbase/Bybit)",
            "backpressure": {
                "last_drop_reason": dict(self._out_queue_last_drop_reason),
                "last_drop_ts": dict(self._out_queue_last_drop_ts),
            },
        }