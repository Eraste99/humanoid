# -*- coding: utf-8 -*-
"""
private_ws_reconciler.py — fallback polling si WS privé devient intermittent.
À piloter DEPUIS l'ExecutionEngine (accès FSM + apply_reconciliation).

Alignement P0/Hub :
- Evénements Hub: 'fill' avec 'client_id' standardisé (fallback clientOrderId, etc.)
- Latences/horodatages gérés côté Hub ; ici on se concentre sur idempotence + resync.
"""
from __future__ import annotations
import inspect
import asyncio
import logging
import time
from collections import deque
from typing import Callable, Awaitable, Optional, Tuple, List, Dict, Any, Set, Deque
from modules.rm_compat import getattr_int, getattr_float, getattr_str, getattr_bool, getattr_dict, getattr_list

try:
    from modules.observability_pacer import PACER
except Exception:
    class _P0Pacer:
        def clamp(self, *_a, **_k): return 0.0
        def state(self): return "NORMAL"
    PACER = _P0Pacer()

def _now() -> float:
    """Horodatage local en secondes (float)."""
    return time.time()


log = logging.getLogger("PrivateWSReconciler")

# --- OBS/METRICS (no-op fallback si absents) --------------------------------
try:
    from modules.obs_metrics import (
        RECONCILE_MISS_TOTAL,  # counter{exchange,alias,reason}
        WS_RECO_RUN_MS,  # histogram{exchange}
        WS_RECO_ERRORS_TOTAL,  # counter{exchange}
        RECONCILE_RESYNC_TOTAL,  # counter{exchange,alias,scope}
        RECONCILE_RESYNC_FAILED_TOTAL,  # counter{exchange,alias,scope}
        RECONCILE_RESYNC_LATENCY_MS,
        COLD_RESYNC_TOTAL,
        COLD_RESYNC_RUN_MS,  # histogram{exchange,alias,scope}
        WS_RECO_MISS_PER_MINUTE,
        WS_RECO_MISS_BURST_TOTAL,
    )



except Exception:  # pragma: no cover
    class _NoopMetric:
        def labels(self, *_, **__): return self
        def inc(self, *_, **__): pass
        def observe(self, *_, **__): pass
    RECONCILE_MISS_TOTAL = _NoopMetric()
    WS_RECO_RUN_MS = _NoopMetric()
    WS_RECO_ERRORS_TOTAL = _NoopMetric()
    RECONCILE_RESYNC_TOTAL = _NoopMetric()
    RECONCILE_RESYNC_FAILED_TOTAL = _NoopMetric()
    RECONCILE_RESYNC_LATENCY_MS = _NoopMetric()
    COLD_RESYNC_TOTAL = _NoopMetric()
    COLD_RESYNC_RUN_MS = _NoopMetric()
    WS_RECO_MISS_PER_MINUTE = _NoopMetric()
    WS_RECO_MISS_BURST_TOTAL = _NoopMetric()

# --- LRUSet pour idempotence bornée -----------------------------------------
class _LRUSet:
    def __init__(self, maxlen: int = 20000) -> None:
        self._maxlen = int(max(1000, maxlen))
        self._q: deque = deque()
        self._s: set = set()

    def add(self, key: tuple) -> bool:
        """Ajoute la clé si absente. Retourne True si c'est un *nouveau* (non vu)."""
        if key in self._s:
            return False
        self._s.add(key)
        self._q.append(key)
        if len(self._q) > self._maxlen:
            old = self._q.popleft()
            self._s.discard(old)
        return True

    def __contains__(self, key: tuple) -> bool:
        return key in self._s


class PrivateWSReconciler:
    """
    Deux modes supportés (compat arrière) :

    1) Mode "riche" (legacy):
       PrivateWSReconciler(
           venue_name, list_open_orders, list_recent_fills, apply_reconciliation,
           stale_ms=..., poll_every_s=..., is_inflight_client_id=..., request_full_resync=...
       )

       - `list_open_orders`: () -> Awaitable[List[dict]]
       - `list_recent_fills`: () -> Awaitable[List[dict]]
       - `apply_reconciliation`: (opens: List[dict], fills: List[dict], venue: str) -> Awaitable[None]

    2) Mode "léger" (P0):
       PrivateWSReconciler(
           cooldown_s=..., stale_ms=..., poll_every_s=..., dedup_max=...,
           venue_name=None, list_open_orders=None, list_recent_fills=None, apply_reconciliation=None,
           is_inflight_client_id=None, request_full_resync=None
       )
       -> Les hooks sont injectés plus tard par l'Engine : _lookup/_resync_order/_resync_alias.

    Hooks P0 (optionnels) que l'orchestrateur peut poser :
      - self._lookup(exchange, alias, client_id) -> Any|None
      - self._resync_order(exchange, alias, client_id) -> awaitable[bool]
      - self._resync_alias(exchange, alias) -> awaitable[bool]
    """
    # NOTE: On maintient la signature __init__ hybride pour compatibilité.
    def __init__(
        self, *args,
        cooldown_s: float = 60.0,
        venue_name: Optional[str] = None,
        list_open_orders: Optional[Callable[[], Awaitable[List[dict]]]] = None,
        list_recent_fills: Optional[Callable[[], Awaitable[List[dict]]]] = None,
        apply_reconciliation: Optional[Callable[[List[dict], List[dict], str], Awaitable[None]]] = None,
        stale_ms: int = 1500,
        poll_every_s: float = 2.0,
        is_inflight_client_id: Optional[Callable[[str], bool]] = None,
        request_full_resync: Optional[Callable[[str], Awaitable[None]]] = None,
        dedup_max: int = 20000,
        cold_every_h: Optional[float] = None,
    ) -> None:

        # --- Détection du mode "riche" legacy si signature positionnelle fournie ---
        if len(args) >= 4 and all(callable(x) for x in args[1:4]):
            venue_name = args[0]
            list_open_orders = args[1]
            list_recent_fills = args[2]
            apply_reconciliation = args[3]

        # --- Commun aux deux modes ---
        self.venue: str = str(venue_name).upper() if venue_name is not None else "UNKNOWN"
        self._list_open_orders = list_open_orders
        self._list_recent_fills = list_recent_fills
        self._apply_reco = apply_reconciliation
        self._is_inflight = is_inflight_client_id
        self._request_full_resync = request_full_resync

        self._stale_ms = int(stale_ms)
        self._poll_every_s = float(poll_every_s)
        self._cooldown_s = float(cooldown_s)

        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._last_ws_ns = time.perf_counter_ns()

        # Misses & cooldowns par (exchange, alias)
        self._alias_miss_counter: Dict[Tuple[str, str], int] = {}
        self._last_alias_resync: Dict[Tuple[str, str], float] = {}

        # Hooks P0 posés ultérieurement par l'orchestrateur
        self._lookup: Optional[Callable[[str, str, str], Any]] = None
        self._resync_order: Optional[Callable[[str, str, str], Awaitable[bool]]] = None
        self._resync_alias: Optional[Callable[[str, str], Awaitable[bool]]] = None

        # Dédup bornée (client_id/fill-key)
        self._seen_keys = _LRUSet(maxlen=int(dedup_max))

        # Champs client_id tolérants (fallback si évènement non normalisé).
        # Ordre de priorité:
        #   1) client_id "métier" (Engine)
        #   2) variantes clientOrderId
        #   3) identifiants CEX (exchange_order_id / orderId/...)
        self._client_id_fields = (
            # ID logique piloté par l'Engine / RM
            "client_id",
            # Variantes clientOrderId (CEX)
            "clientOrderId",
            "client_order_id",
            "clientId",
            "origClientOrderId",
            # ID CEX (fallback si client_id absent)
            "exchange_order_id",
            "orderId",
            "order_id",
            "orderID",
            "clOrdID",
            "client_oid",
            # Héritage/local
            "cl_id",
            "cid",
        )

        # Cold-resync scheduler (désactivé si request_full_resync absent)
        self._cold_every_h = float(
            cold_every_h
            if cold_every_h is not None
            else getattr(self, "cold_resync_interval_h", 6.0)
        )
        self._cold_task = None
        self._event_sink = None  # optionnel: injecte ton LHM/Watchdog
        self._missing_hook_warned: Set[Tuple[str, str, str]] = set()

        # Fenêtre glissante pour le taux de miss (~60s)
        # NB: on utilise directement deque(), importé depuis collections.
        self._miss_win: Deque[float] = deque(maxlen=512)
        self._miss_alert_task: Optional[asyncio.Task] = None


    def _record_miss(self) -> None:
        # Appeler ceci à chaque "miss" détecté (en plus du compteur existant)
        try:
            self._miss_win.append(_now())
        except Exception as exc:
            log.exception("[Reconciler] unable to record miss: %s", exc)

    def _compute_miss_rate_per_minute(self) -> float:
        """
        Retourne le taux de miss/minute sur la fenêtre glissante (~60s).

        Utilisé par :
          - run_miss_alerts (Prometheus/alerting),
          - health()/get_alias_status_snapshot (statut métier alias).
        """
        try:
            now = _now()
            win = self._miss_win
            # On nettoie localement au cas où run_miss_alerts n'est pas actif.
            while win and (now - win[0]) > 60.0:
                win.popleft()
            return float(len(win))
        except Exception:
            return 0.0

    async def run_miss_alerts(self, threshold_per_minute: int = 30, period_s: float = 5.0) -> None:
        """
        Alerte si le nombre de 'miss' sur ~60s dépasse le seuil.
        - Tolère l’absence de self.cfg.
        - S’arrête proprement via self._stop.

        Observabilité :
          - WS_RECO_MISS_PER_MINUTE{exchange,alias}
          - WS_RECO_MISS_BURST_TOTAL{exchange,alias}
          - WS_RECO_ERRORS_TOTAL{exchange}
        """
        # Normalisation stricte des labels pour jointure alias-centric
        ex_raw = getattr(self, "venue", getattr(self, "exchange", "UNKNOWN"))
        al_raw = getattr(self, "alias", "UNKNOWN")
        ex = str(ex_raw).upper()
        al = str(al_raw).upper()
        cfg = getattr(self, "cfg", None)

        while not self._stop.is_set():
            now = _now()
            try:
                # purge < now-60
                while self._miss_win and (now - self._miss_win[0]) > 60.0:
                    self._miss_win.popleft()
                rate = len(self._miss_win)

                # Gauge per-minute (fenêtre glissante ~60s)
                try:
                    WS_RECO_MISS_PER_MINUTE.labels(ex, al).set(float(rate))
                except Exception:
                    pass

                # Burst detection configurable
                thr = int(getattr(cfg, "RECO_MISS_BURST_THRESHOLD", threshold_per_minute))
                if rate >= thr:
                    try:
                        WS_RECO_MISS_BURST_TOTAL.labels(ex, al).inc()
                    except Exception:
                        pass
                    log.warning("[Reconciler:%s:%s] Burst de miss: %d/min ≥ seuil", ex, al, rate)

            except Exception as exc:
                log.exception("[Reconciler] miss_alert loop error: %s", exc)
                try:
                    WS_RECO_ERRORS_TOTAL.labels(exchange=ex).inc()
                except Exception:
                    pass

            period = max(1.0, float(getattr(cfg, "RECO_ALERT_PERIOD_S", period_s)))
            try:
                # wake-up anticipé si stop() est appelé
                await asyncio.wait_for(self._stop.wait(), timeout=period)
            except asyncio.TimeoutError:
                pass

    # ----------------------------- Utils -------------------------------------

    def set_event_sink(self, sink: Callable[[dict], None] | None) -> None:
        self._event_sink = sink

    # ----------------------------- Alerts/Events -----------------------------

    def _emit_event(self, event: str, **payload: Any) -> None:
        if not self._event_sink:
            return
        body = {"module": "PWS", "event": event, "ts": time.time()}
        if payload:
            body.update(payload)
        try:
            self._event_sink(body)
        except Exception:
            log.debug("[Reconciler] event_sink emit failed", exc_info=False)

    def _notify_hook_missing(self, hook: str, exchange: str, alias: str) -> None:
        key = (hook, exchange, alias)
        if key in self._missing_hook_warned:
            return
        self._missing_hook_warned.add(key)
        log.warning("[Reconciler:%s:%s] hook %s missing", exchange, alias, hook)
        self._emit_event(
            "reco_hook_missing",
            hook=hook,
            exchange=exchange,
            alias=alias,
        )

    def _record_resync_metric(self, exchange: str, alias: str, scope: str) -> None:
        try:
            RECONCILE_RESYNC_TOTAL.labels(exchange, alias, scope).inc()
        except Exception:
            pass

    def _observe_resync_latency(self, exchange: str, alias: str, scope: str, start_ts: float) -> None:
        try:
            RECONCILE_RESYNC_LATENCY_MS.labels(exchange, alias, scope).observe(
                max(0.0, (time.time() - start_ts) * 1000.0)
            )
        except Exception:
            pass

    def _on_resync_failure(
        self,
        exchange: str,
        alias: str,
        scope: str,
        reason: str,
        error: Optional[BaseException] = None,
    ) -> None:
        try:
            RECONCILE_RESYNC_FAILED_TOTAL.labels(exchange, alias, scope).inc()
        except Exception:
            pass
        try:
            WS_RECO_ERRORS_TOTAL.labels(exchange).inc()
        except Exception:
            pass
        msg = f"[Reconciler:{exchange}:{alias}] resync {scope} failed ({reason})"
        if error is not None:
            log.exception(msg)
        else:
            log.warning(msg)
        payload = {
            "exchange": exchange,
            "alias": alias,
            "scope": scope,
            "reason": reason,
        }
        if error is not None:
            payload["error"] = str(error)
        self._emit_event("reco_resync_failed", **payload)


    async def _cold_resync_loop(self) -> None:
        """
        Exécute un cold-resync périodique (4–6h par défaut).
        """
        import asyncio, time
        if not callable(getattr(self, "_request_full_resync", None)) and not callable(getattr(self, "request_full_resync", None)):
            return
        while not self._stop.is_set():
            try:
                # attente jusqu'à la prochaine exécution
                hours = max(1.0, float(self._cold_every_h))
                await asyncio.wait_for(self._stop.wait(), timeout=hours * 3600.0)
                if self._stop.is_set():
                    break

                # clamp pacer (non-bloquant opérationnellement)
                try:
                    c = float(PACER.clamp("cold_resync"))
                    if c > 0.0:
                        await asyncio.sleep(c)
                except Exception:
                    pass

                t0 = time.perf_counter()
                ok = False
                try:
                    fn = getattr(self, "_request_full_resync", None) or getattr(self, "request_full_resync", None)
                    venue = getattr(self, "venue", "UNKNOWN")
                    if callable(fn):
                        await fn(venue)
                        ok = True
                        COLD_RESYNC_TOTAL.labels(exchange=venue, result="OK").inc()
                    else:
                        COLD_RESYNC_TOTAL.labels(exchange=venue, result="NOOP").inc()
                except Exception:
                    venue = getattr(self, "venue", "UNKNOWN")
                    COLD_RESYNC_TOTAL.labels(exchange=venue, result="ERROR").inc()
                finally:
                    try:
                        ms = (time.perf_counter() - t0) * 1000.0
                        COLD_RESYNC_RUN_MS.labels(venue).observe(ms)
                    except Exception:
                        pass
                    # event sink best-effort
                    try:
                        if self._event_sink:
                            self._event_sink({"type":"cold_resync","venue":venue,"ok":ok,"ts":time.time()})
                    except Exception:
                        pass
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("[Reconciler] cold_resync_loop error")
                WS_RECO_ERRORS_TOTAL.labels(exchange=getattr(self,"venue","UNKNOWN")).inc()



    def _pacer_wait(self, kind: str) -> float:
        try:
            return float(PACER.clamp(kind))
        except Exception:
            return 0.0


    def mark_ws_activity(self) -> None:
        """À appeler par l'Engine/Hub lorsqu'un event WS est reçu pour ce venue."""
        self._last_ws_ns = time.perf_counter_ns()

    def note_seen_client_id(self, client_id: Optional[str]) -> None:
        """Optionnel: marquer un client_id vu pour idempotence locale (WS événementiel)."""
        if not client_id:
            return
        self._seen_keys.add((self.venue, "cid", str(client_id)))

    def _extract_client_id(self, row: dict) -> Optional[str]:
        if not isinstance(row, dict):
            return None
        for k in self._client_id_fields:
            v = row.get(k)
            if v:
                return str(v)
        # parfois nested: {"order": {...}}
        try:
            order = row.get("order") or {}
            for k in self._client_id_fields:
                v = order.get(k)
                if v:
                    return str(v)
        except Exception:
            pass
        return None

    def _fill_key(self, row: dict) -> tuple:
        """
        Clé de dédup d'un fill (robuste mais bornée) :
        - venue
        - client_id (ou 'no_cid')
        - trade_id/sequence/ts_ms bucket si dispo
        - (px, qty, side) arrondis (filet de sécurité)
        """
        cid = self._extract_client_id(row) or "no_cid"
        tid = row.get("trade_id") or row.get("tradeId") or row.get("sequence")
        t_ms = None
        for k in ("ts_ms", "ts", "timestamp", "time", "created_time", "completion_time"):
            v = row.get(k)
            if v is not None:
                try:
                    # seconds->ms si nécessaire
                    f = float(v)
                    t_ms = int(f if f > 3e10 else f * 1000.0)
                    break
                except Exception:
                    continue
        if t_ms is not None:
            bucket = t_ms // 5  # bucket 5ms
        else:
            bucket = None
        # filet (px/qty/side arrondis)
        try:
            px = float(row.get("fill_px") or row.get("price") or 0.0)
            qty = float(row.get("base_qty") or row.get("size") or row.get("filled_size") or 0.0)
        except Exception:
            px, qty = 0.0, 0.0
        side = str(row.get("side") or "").upper()
        return (self.venue, cid, tid, bucket, round(px, 8), round(qty, 8), side)

    # ----------------------- Comptage & corrélation --------------------------

    def observe_fill_event(self, ev: dict) -> None:
        """
        À appeler dès réception d’un *fill* (évènement déjà normalisé par Hub/RM).

        Contrat minimal pour ev:
          - type="fill"
          - status in {"FILL", "PARTIAL"}
          - exchange, alias
          - au moins un identifiant corrélable: client_id ou exchange_order_id

        Rôle:
          - Détecter les "miss" (fill sans inflight connu) via _lookup/_is_inflight.
          - Alimenter RECONCILE_MISS_TOTAL{exchange,alias,reason}.
        """
        if not ev:
            return

        # Normalisation légère
        ex = str((ev.get("exchange") or "UNKNOWN")).upper()
        al = str((ev.get("alias") or "-")).upper()
        cid = self._extract_client_id(ev)
        key = (ex, al)

        # Sans identifiant corrélable => violation du contrat en amont (Hub/RM)
        if not cid:
            reason = "no_id"
            try:
                RECONCILE_MISS_TOTAL.labels(ex, al, reason).inc()
                self._record_miss()
            except Exception:
                pass
            try:
                log.warning(
                    "[Reconciler:%s:%s] observe_fill_event sans client_id/exchange_order_id, drop head=%s",
                    ex,
                    al,
                    {
                        "symbol": ev.get("symbol"),
                        "side": ev.get("side"),
                        "status": ev.get("status"),
                        "type": ev.get("type"),
                    },
                )
            except Exception:
                pass
            # On incrémente le compteur de miss alias pour déclencher la logique de resync
            self._alias_miss_counter[key] = int(self._alias_miss_counter.get(key, 0)) + 1
            return

        inflight = None
        try:
            if callable(self._lookup):
                inflight = self._lookup(ex, al, cid)
            elif callable(self._is_inflight):
                inflight = True if self._is_inflight(cid) else None
        except Exception:
            inflight = None

        if inflight is None:
            # Fill "orphelin" (pas d'ordre inflight correspondant côté Engine)
            reason = "orphan"
            try:
                RECONCILE_MISS_TOTAL.labels(ex, al, reason).inc()
                self._record_miss()
            except Exception:
                pass
            self._alias_miss_counter[key] = int(self._alias_miss_counter.get(key, 0)) + 1

    async def correlate_and_maybe_resync(
            self, exchange: str, alias: str, order_identifier: Optional[str], *, id_kind: Optional[str] = None
    ) -> None:
        """
        Après un miss détecté, tente:
          1) resync ciblé (order) si client_id dispo
          2) resync alias si >=2 misses & cooldown OK
        """
        key = (exchange, alias)
        misses = int(self._alias_miss_counter.get(key, 0))
        if misses <= 0:
            return

        t0 = time.time()
        scope = None
        ok = False
        attempted = False
        try:
            # 1) resync order
            if order_identifier:
                if callable(self._resync_order):
                    scope = "order"
                    attempted = True
                    order_ok = False
                    order_exc: Optional[BaseException] = None
                    try:
                        params = {
                            "exchange": exchange,
                            "alias": alias,
                            "order_id": order_identifier,
                        }
                        if id_kind and "id_kind" in inspect.signature(self._resync_order).parameters:
                            params["id_kind"] = id_kind
                        order_ok = bool(await self._resync_order(**params))
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        order_exc = exc
                        self._on_resync_failure(exchange, alias, scope, "exception", error=exc)
                    finally:
                        self._record_resync_metric(exchange, alias, scope)
                    if order_exc is None and not order_ok:
                        self._on_resync_failure(exchange, alias, scope, "returned_false")
                    ok = order_ok
                else:
                    self._notify_hook_missing("resync_order", exchange, alias)
                    self._on_resync_failure(exchange, alias, "order", "hook_missing")

            # 2) resync alias si besoin
            if (not ok) and misses >= 2:
                if callable(self._resync_alias):
                    last = float(self._last_alias_resync.get(key, 0.0))
                    if (time.time() - last) >= self._cooldown_s:
                        scope = "alias"
                        attempted = True
                        alias_ok = False
                        alias_exc: Optional[BaseException] = None
                        try:
                            alias_ok = bool(await self._resync_alias(exchange, alias))
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            alias_exc = exc
                            self._on_resync_failure(exchange, alias, scope, "exception", error=exc)
                        finally:
                            self._record_resync_metric(exchange, alias, scope)
                        if alias_exc is None:
                            self._last_alias_resync[key] = time.time()
                            if not alias_ok:
                                self._on_resync_failure(exchange, alias, scope, "returned_false")
                        ok = alias_ok
                else:
                    self._notify_hook_missing("resync_alias", exchange, alias)
                    self._on_resync_failure(exchange, alias, "alias", "hook_missing")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._on_resync_failure(exchange, alias, scope or "unknown", "exception", error=exc)
        finally:
            if attempted and scope:
                self._observe_resync_latency(exchange, alias, scope, t0)
            if ok:
                self._alias_miss_counter[key] = 0  # reset indulgent

    # ----------------------------- Loop --------------------------------------

    async def _run(self) -> None:
        """
        Boucle de polling de secours si l'intervalle depuis la dernière activité WS
        dépasse `stale_ms`. En mode P0 (hooks absents), la passe est simplement sautée.
        """
        while not self._stop.is_set():
            try:
                stale = (time.perf_counter_ns() - self._last_ws_ns) / 1e6 > self._stale_ms
                if stale:
                    t0 = time.perf_counter()
                    # pacer-aware: étirer la passe si DEGRADED/SEVERE
                    try:
                        w = self._pacer_wait("reco_pass")
                        if w > 0.0:
                            await asyncio.sleep(w)
                    except Exception:
                        pass

                    try:
                        # Garde P0: si un des hooks legacy manque, on saute proprement.
                        if not (callable(self._list_open_orders)
                                and callable(self._list_recent_fills)
                                and callable(self._apply_reco)):
                            await asyncio.sleep(self._poll_every_s)
                        else:
                            opens = await self._list_open_orders()
                            raw_fills = await self._list_recent_fills()

                            # Dédup des fills + détection MISS (via _is_inflight si disponible)
                            fills: List[dict] = []
                            miss_detected = False
                            for f in raw_fills or []:
                                fk = self._fill_key(f)
                                if not self._seen_keys.add(fk):
                                    continue
                                fills.append(f)
                                cid = self._extract_client_id(f)
                                if cid and callable(self._is_inflight):
                                    try:
                                        if not self._is_inflight(cid):
                                            miss_detected = True
                                            RECONCILE_MISS_TOTAL.labels(self.venue, "-", "poll").inc()
                                            self._record_miss()
                                    except Exception:
                                        pass

                            await self._apply_reco(opens, fills, self.venue)

                            # Optionnel: pleine resynchronisation si un MISS a été vu pendant la passe
                            if miss_detected and callable(self._request_full_resync):
                                try:
                                    await self._request_full_resync(self.venue)
                                except Exception:
                                    log.exception("PrivateWSReconciler: request_full_resync failed")

                    except Exception:
                        try:
                            WS_RECO_ERRORS_TOTAL.labels(exchange=self.venue).inc()
                        except Exception:
                            pass
                        log.exception("PrivateWSReconciler: reconciliation step failed")
                    finally:
                        try:
                            dt_ms = (time.perf_counter() - t0) * 1000.0
                            WS_RECO_RUN_MS.labels(exchange=self.venue).observe(dt_ms)
                        except Exception:
                            pass

                # Attente non bloquante entre passes
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self._poll_every_s)
                except asyncio.TimeoutError:
                    pass

            except asyncio.CancelledError:
                break
            except Exception:
                try:
                    WS_RECO_ERRORS_TOTAL.labels(exchange=self.venue).inc()
                except Exception:
                    pass
                log.exception("PrivateWSReconciler: loop error")

    # ----------------------------- API ---------------------------------------

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name=f"WSReco[{self.venue}]")
        if self._miss_alert_task is None or self._miss_alert_task.done():
            self._miss_alert_task = asyncio.create_task(self.run_miss_alerts())

        if self._cold_task is None:
            self._cold_task = asyncio.create_task(self._cold_resync_loop(), name=f"WSRecoCold[{self.venue}]")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            try:
                await self._task
            except Exception:
                logging.exception("Unhandled exception")
        if self._cold_task:
            try:
                await self._cold_task
            except Exception:
                logging.exception("Unhandled exception (cold)")
        if self._miss_alert_task:
            try:
                await self._miss_alert_task
            except Exception:
                logging.exception("Unhandled exception (miss_alerts)")

    def health(self, exchange: str, alias: str) -> dict:
        """
        Vue santé/corrélation pour un couple (exchange, alias).

        Inclut:
          - métriques locales (misses, dernier resync alias),
          - état de wiring des hooks requis:
              * _lookup
              * _is_inflight
              * _resync_order
              * _resync_alias
          - un statut métier dérivé pour l'alias du point de vue Reconciler.
        """
        # On normalise en UPPER pour coller aux clés internes (_alias_miss_counter)
        ex_u = str(exchange).upper()
        alias_u = str(alias).upper()
        key = (ex_u, alias_u)

        last = float(self._last_alias_resync.get(key, 0.0))
        age = (time.time() - last) if last else None

        wiring = {
            "lookup": callable(getattr(self, "_lookup", None)),
            "is_inflight": callable(getattr(self, "_is_inflight", None)),
            "resync_order": callable(getattr(self, "_resync_order", None)),
            "resync_alias": callable(getattr(self, "_resync_alias", None)),
        }
        wiring_ok = all(bool(v) for v in wiring.values())

        misses_recent = int(self._alias_miss_counter.get(key, 0))
        miss_rate_per_min = self._compute_miss_rate_per_minute()

        # Heuristique de classement local (Reconciler-only)
        cfg = getattr(self, "cfg", None)
        try:
            burst_thr = float(getattr(cfg, "RECO_MISS_BURST_THRESHOLD", 30.0))
        except Exception:
            burst_thr = 30.0
        try:
            recent_thr = float(getattr(cfg, "RECO_MISS_RECENT_THRESHOLD", burst_thr))
        except Exception:
            recent_thr = burst_thr
        try:
            max_age_s = float(getattr(cfg, "RECO_ALIAS_RESYNC_MAX_AGE_S", 6 * 3600.0))
        except Exception:
            max_age_s = 6 * 3600.0

        age_val = float(age or 0.0)

        if not wiring_ok:
            status = "UNKNOWN"
        elif misses_recent <= 0 and miss_rate_per_min <= 0.0:
            # Pas de signal suspect récent
            status = "OK"
        else:
            # Cas sévère : bursts forts + pas de resync récent -> BROKEN
            if miss_rate_per_min >= 2.0 * burst_thr and age_val > max_age_s:
                status = "BROKEN"
            # Cas intermédiaire : bursts, beaucoup de miss ou resync trop ancien -> AT_RISK
            elif (
                    miss_rate_per_min >= burst_thr
                    or misses_recent >= recent_thr
                    or age_val > max_age_s
            ):
                status = "AT_RISK"
            else:
                status = "OK"

        return {
            "venue": self.venue,
            "exchange": ex_u,
            "alias": alias_u,
            "misses_recent": misses_recent,
            "miss_rate_per_min": miss_rate_per_min,
            "last_alias_resync_ts": last,
            "age_since_last_alias_resync_s": age,
            "wiring": wiring,
            "wiring_ok": wiring_ok,
            "status": status,
        }

    def get_alias_status_snapshot(self, exchange: str, alias: str) -> Dict[str, Any]:
        """
        Snapshot stable pour un couple (exchange, alias) destiné aux
        consommateurs externes (MBF, RM, watchdog, etc.).

        Retourne un dict minimal avec :
          - exchange, alias
          - status ("OK" | "AT_RISK" | "BROKEN" | "UNKNOWN")
              * AT_RISK : on observe des misses/bursts ou un resync trop ancien,
                mais la situation reste récupérable.
              * BROKEN : le reconciler n'arrive plus à suivre (bursts sévères
                et resync très vieux), l'alias est considéré cassé tant que
                l'état persiste.
          - misses_recent, miss_rate_per_min
          - last_alias_resync_ts, age_since_last_alias_resync_s
          - wiring_ok, wiring
        """
        h = self.health(exchange, alias) or {}
        
        return {
            "exchange": h.get("exchange", str(exchange).upper()),
            "alias": h.get("alias", str(alias).upper()),
            "status": h.get("status", "UNKNOWN"),
            "misses_recent": int(h.get("misses_recent", 0) or 0),
            "miss_rate_per_min": float(h.get("miss_rate_per_min", 0.0) or 0.0),
            "last_alias_resync_ts": h.get("last_alias_resync_ts"),
            "age_since_last_alias_resync_s": h.get("age_since_last_alias_resync_s"),
            "wiring_ok": bool(h.get("wiring_ok", False)),
            "wiring": h.get("wiring", {}) or {},
        }

    def is_alias_at_risk(self, exchange: str, alias: str) -> bool:
        """
        Helper métier : True si l'alias est considéré 'à risque' du point de vue
        Reconciler (utilisé plus tard par MBF/RM pour marquer le capital à risque).
        """
        snap = self.get_alias_status_snapshot(exchange, alias)
        return snap.get("status") in ("AT_RISK", "BROKEN")
    def get_status(self) -> Dict[str, Any]:
        return {
            "running": bool(self._task and not self._task.done()),
            "cold_loop_running": bool(getattr(self, "_cold_task", None) and not getattr(self, "_cold_task").done()),
            "miss_alerts_running": bool(getattr(self, "_miss_alert_task", None) and not getattr(self, "_miss_alert_task").done()),
            "last_ws_ns": getattr(self, "_last_ws_ns", None),
            "wiring_ok": bool(getattr(self, "_wiring_checked", False)),
        }

    def start_cold_resync_loop(self, *, period_hours: float = 6.0) -> None:
        """Déclenche un full-resync périodique; ne bloque jamais et supporte Cancel."""
        import asyncio
        if getattr(self, "_cold_task", None) and not self._cold_task.done():
            return

        async def _cold():
            try:
                while True:
                    await asyncio.sleep(max(1.0, float(period_hours) * 3600.0))
                    fn = getattr(self, "_request_full_resync", None) or getattr(self, "request_full_resync", None)
                    if callable(fn):
                        try:
                            await fn(self.venue)
                        except Exception:
                            logging.exception("PrivateWSReconciler: cold resync failed")
            except asyncio.CancelledError:
                return

        self._cold_task = asyncio.create_task(_cold(), name=f"WSRecoCold[{self.venue}]")

    def stop_cold_resync_loop(self) -> None:
        t = getattr(self, "_cold_task", None)
        if t and not t.done():
            t.cancel()

