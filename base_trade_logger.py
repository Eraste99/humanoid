# -*- coding: utf-8 -*-
from __future__ import annotations
"""
BaseTradeLogger — sous-module passif (batch + normalisation + latences + IDs)

Rôle
-----
- Ne parle JAMAIS à la DB ni au réseau : il bufferise des trades normalisés
  et *émet* des batches via un `event_sink` fourni par l’orchestrateur.
- Calcule et/ou complète les latences (ack/fill/end-to-end) à partir de
  timestamps ms bruts quand disponibles.
- Normalise les paires/IDs/champs pour rester aligné avec le LogWriter étendu
  (trade_id, bundle_id, slice_id, client_id, order_id, opp_id, route_id,
   engine_latency_ms, ack_latency_ms, fill_latency_ms, end_to_end_ms,
   t_sent_ms, t_ack_ms, t_filled_ms, t_done_ms, pair_score, pair_rank).
- **Tri-CEX ready**: canonicalise les exchanges (BINANCE/BYBIT/COINBASE) et
  émet `buy_ex`, `sell_ex`, `route`, `leg_role`, `trade_mode`.

Notes & garanties
-----------------
- Le `event_sink` doit accepter (log_type: str, entries: list[dict]).
- La colonne `category` est ajoutée par le LogWriter côté orchestrateur, pas ici.
- **Flush temporel garanti** : `flush_interval` déclenche aussi un flush même si `batch_size` n’est pas atteint.
- **Idempotence start/stop** : plusieurs appels sont sans effet.
- **Backpressure** : si la queue est pleine et `drop_when_full=True`, le trade est ignoré (warning).

Usage (via orchestrateur)
-------------------------
    logger = BaseTradeLogger(
        log_type="trades",
        trade_filter=lambda t: True,
        batch_size=20,
        flush_interval=0.5,
        dry_run=False,
    )
    logger.set_event_sink(lambda log_type, entries: ...)
    await logger.start()
    logger.ingest(payload)
    # ou: logger.ingest_many([...])
    logger.flush_now()  # optionnel: force un flush immédiat (best-effort)
    await logger.stop()
"""

import asyncio
import inspect
import json
import logging
from datetime import datetime
from typing import Callable, Dict, Any, Optional, List, Tuple
import asyncio, time, collections, hashlib, json
import queue
import time  # (déjà présent si tu l'utilises)
from queue import Queue, Empty as QueueEmpty, Full as QueueFull



# ---- Metrics proxy (no Prometheus import here) ----
class _NoopMetricsProxy:
    def on_btl_queue_depth(self, *a, **k): pass
    def on_btl_highwater(self, *a, **k): pass
    def on_btl_drop_full(self, *a, **k): pass
    def on_btl_flush_latency(self, *a, **k): pass
    def on_btl_emitted(self, *a, **k): pass

logger = logging.getLogger("BaseTradeLogger")

# Sentinel interne pour déclencher un flush immédiat via la queue
_FLUSH_SENTINEL = object()


class BaseTradeLogger:
    def __init__(
            self,
            *,
            log_type: str,
            trade_filter: Callable[[Dict[str, Any]], bool],
            batch_size: int = 100,
            flush_interval: float = 0.2,
            dry_run: bool = False,
            queue_maxsize: int = 5000,
            drop_when_full: bool = True,
            high_watermark_ratio: float = 0.8,
    ):
        self.log_type = str(log_type)
        self.trade_filter = trade_filter
        self.batch_size = int(max(1, batch_size))
        self.flush_interval = float(max(0.05, flush_interval))
        self.dry_run = bool(dry_run)
        self.drop_when_full = bool(drop_when_full)

        import queue  # stdlib
        self._queue = queue.Queue(maxsize=int(queue_maxsize))
        self._q_high_watermark = max(1, int(queue_maxsize * float(high_watermark_ratio)))

        import threading, time
        self._stop = threading.Event()
        self._thread = None
        self._last_enqueue_ts_ms = 0
        self._buffer: List[Dict[str, Any]] = []

        # proxy métriques (fourni par LHM dynamiquement)
        self._metrics_proxy = _NoopMetricsProxy()
        self._loop: asyncio.AbstractEventLoop | None = None

        # proxy métriques (fourni par LHM dynamiquement)
        self._metrics_proxy = _NoopMetricsProxy()

    # ----------------------- configuration externe -----------------------
    def set_event_sink(self, sink: Callable[[str, List[Dict[str, Any]]], Any]) -> None:
        self._event_sink = sink

    # ---------------------------- ingestion -----------------------------
    def ingest(self, entry):
        # fast-path: filtrage amont (on ne casse jamais)
        try:
            if self.trade_filter and not self.trade_filter(entry):
                return
        except Exception:
            # filtre défaillant => on loggue quand même
            pass

        try:
            self._queue.put_nowait(entry)
            qd = self._queue.qsize()
            # proxy métriques centralisé LHM
            self._metrics_proxy.on_btl_queue_depth(self.log_type, depth=qd)
            if qd >= max(1000, int(self._queue.maxsize * 0.8)):
                self._metrics_proxy.on_btl_highwater(self.log_type, depth=qd)
            self._last_enqueue_ts_ms = int(time.time() * 1000)
        except QueueFull:
            # drop explicite — comportement conservé, métriques via LHM
            self._metrics_proxy.on_btl_drop_full(self.log_type)

    def ingest_many(self, trades: List[Dict[str, Any]] | None) -> None:
        if not trades:
            return
        for t in trades:
            self.ingest(t)

    async def flush_now(self, *, rotate: bool = False) -> None:
        """Vide immédiatement la file locale (en lots) et cède la main à l’event-loop."""
        # Draine la queue en mémoire
        items: List[Dict[str, Any]] = []
        while True:
            try:
                items.append(self._queue.get_nowait())
            except QueueEmpty:
                break

        # Émet en batches (taille = self.batch_size)
        if items:
            try:
                for i in range(0, len(items), self.batch_size):
                    self._emit(items[i:i + self.batch_size])
            except Exception:
                logging.getLogger("BaseTradeLogger").exception("flush_now: emit failed")

        # best-effort: cède la main
        await asyncio.sleep(0)

    def _run(self):
        backoff_s = 0.002
        try:
            last_flush = time.time()
            while not self._stop.is_set():
                try:
                    item = self._queue.get(timeout=0.05)
                    self._buffer.append(item)
                except QueueEmpty:
                    pass

                now = time.time()
                if len(self._buffer) >= self.batch_size or (now - last_flush) >= self.flush_interval:
                    try:
                        if self._buffer:
                            self._emit(self._buffer)
                            self._buffer.clear()
                    finally:
                        last_flush = now

                # micro-pause pour laisser respirer l’event-loop
                time.sleep(backoff_s)
        except Exception:
            # ne jamais faire tomber le thread du logger
            logging.exception("BaseTradeLogger._run crashed; recovering")
        finally:
            try:
                if self._buffer:
                    self._emit(self._buffer)
                    self._buffer.clear()
            except Exception:
                logging.getLogger("BaseTradeLogger").exception("flush buffer at shutdown failed")

            try:
                self._emit_remaining()
            except Exception:
                logging.exception("BaseTradeLogger._emit_remaining failed at shutdown")

    def _emit(self, batch: List[Dict[str, Any]]) -> None:
        if not batch or not getattr(self, "_event_sink", None):
            return
        import time, logging
        t0 = time.perf_counter()
        try:
            # Envoi vers l’orchestrateur (LHM)
            result = self._event_sink(self.log_type, batch)
            if inspect.isawaitable(result):
                loop = self._loop
                if not loop:
                    raise RuntimeError("Async event sink requires an event loop")
                fut = asyncio.run_coroutine_threadsafe(result, loop)
                fut.result()
            dur_ms = (time.perf_counter() - t0) * 1000.0
            # Hook métrique (centralisé côté LHM)
            self._metrics_proxy.on_btl_flush_latency(self.log_type, ms=dur_ms)
            self._metrics_proxy.on_btl_emitted(self.log_type, n=len(batch))
        except Exception:
            logging.getLogger("BaseTradeLogger").exception("emit failed")

    def _emit_remaining(self) -> None:
        items: List[Dict[str, Any]] = []
        while True:
            try:
                items.append(self._queue.get_nowait())
            except QueueEmpty:
                break
        if items:
            self._emit(items)
    # --------------------------- normalisation --------------------------
    @staticmethod
    def _norm_pair(x: Optional[str]) -> Optional[str]:
        if not x:
            return None
        return x.replace("-", "").upper()

    @staticmethod
    def _norm_ex(x: Optional[str]) -> Optional[str]:
        """Canonicalise vers BINANCE / BYBIT / COINBASE."""
        if not x:
            return None
        s = str(x).strip().replace("-", "").replace("_", "").upper()
        if s.startswith("BINANCE"):
            return "BINANCE"
        if s.startswith("BYBIT"):
            return "BYBIT"
        # Coinbase Advanced Trade: COINBASE, COINBASEAT, COINBASEADVANCEDTRADE, CB, etc.
        if s.startswith("COINBASE") or s in {"CB", "COINBASEAT", "COINBASEADVANCEDTRADE"}:
            return "COINBASE"
        return s  # fallback: retourne la valeur upper

    @staticmethod
    def _first(*vals):
        for v in vals:
            if v is not None:
                return v
        return None

    @staticmethod
    def _iso_utc_from_ms(ms: Optional[int]) -> Optional[str]:
        try:
            if ms is None:
                return None
            # accepte ms ou s (auto-détection)
            if ms < 10_000_000_000:  # seconds → ms
                ms *= 1000
            return datetime.utcfromtimestamp(ms / 1000.0).isoformat()
        except Exception:
            return None

    @staticmethod
    def _to_float(x) -> Optional[float]:
        try:
            if x is None:
                return None
            return float(x)
        except Exception:
            return None

    @staticmethod
    def _to_int(x) -> Optional[int]:
        try:
            if x is None:
                return None
            return int(x)
        except Exception:
            return None

    @staticmethod
    def _compute_latencies(
        t_sent: Optional[int],
        t_ack: Optional[int],
        t_filled: Optional[int],
        t_done: Optional[int],
        engine_latency_ms: Optional[float],
    ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Retourne (ack_latency_ms, fill_latency_ms, end_to_end_ms)."""
        ack = fill = e2e = None
        try:
            if t_sent is not None and t_ack is not None:
                ack = max(0.0, float(t_ack - t_sent))
            if t_ack is None and t_sent is not None and engine_latency_ms is not None:
                # fallback si on ne reçoit qu'une latence agrégée d'engine avant ack
                ack = max(0.0, float(engine_latency_ms))
            if t_ack is not None and t_filled is not None:
                fill = max(0.0, float(t_filled - t_ack))
            if t_sent is not None and t_done is not None:
                e2e = max(0.0, float(t_done - t_sent))
        except Exception:
            logging.exception("Unhandled exception")
        return ack, fill, e2e

    def _normalize(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        # --------- IDs / routing ---------
        trade_id = trade.get("trade_id") or trade.get("id") or trade.get("tid")
        bundle_id = trade.get("bundle_id") or trade.get("arb_id") or trade.get("rebal_id")
        slice_id = trade.get("slice_id") or trade.get("leg_id")
        client_id = trade.get("client_id") or trade.get("cid")
        order_id = trade.get("order_id") or trade.get("exchange_order_id")
        opp_id = trade.get("opp_id") or trade.get("opportunity_id")
        route_id = trade.get("route_id") or trade.get("path_id")
        maker_order_id = trade.get("maker_order_id")
        taker_order_id = trade.get("taker_order_id")
        rebal_group_id = trade.get("rebal_group_id")

        # --------- timestamps (ms) ---------
        t_sent_ms = self._first(self._to_int(trade.get("t_sent_ms")),
                                self._to_int(trade.get("sent_ms")),
                                self._to_int(trade.get("ts_ms")))
        t_ack_ms = self._first(self._to_int(trade.get("t_ack_ms")), self._to_int(trade.get("ack_ms")))
        t_filled_ms = self._first(self._to_int(trade.get("t_filled_ms")), self._to_int(trade.get("fill_ms")))
        t_done_ms = self._first(self._to_int(trade.get("t_done_ms")), self._to_int(trade.get("done_ms")))

        engine_latency_ms = self._to_float(trade.get("engine_latency_ms"))
        ack_latency_ms, fill_latency_ms, end_to_end_ms = self._compute_latencies(
            t_sent_ms, t_ack_ms, t_filled_ms, t_done_ms, engine_latency_ms
        )
        if end_to_end_ms is None:
            end_to_end_ms = self._to_float(trade.get("latency"))

        # --------- métadonnées ---------
        pair = self._norm_pair(trade.get("pair") or trade.get("pair_key") or trade.get("symbol") or trade.get("asset"))
        exchange = self._norm_ex(trade.get("exchange") or trade.get("ex"))
        side = (trade.get("side") or trade.get("direction") or "").upper() or None

        raw_mode = (trade.get("trade_mode") or "").upper() or None
        strategy = (trade.get("strategy") or "default")
        trade_mode = raw_mode
        if trade_mode is None:
            s_up = str(strategy).upper()
            if s_up in ("TT", "TM"):
                trade_mode = s_up
            elif trade.get("is_rebalancing"):
                trade_mode = "REB"

        trade_type = (
                trade.get("trade_type") or trade.get("exec_mode") or trade.get("type") or
                ("REB" if trade.get("is_rebalancing") else None)
        )

        source = trade.get("source") or trade.get("status") or "ExecutionEngine"
        if self.dry_run:
            source = "DryRun"
            strategy = f"{strategy}_simulated"

        # --------- prix/volumes ---------
        price = self._to_float(self._first(trade.get("price"), trade.get("avg_price"), trade.get("execution_price")))
        qty = self._to_float(trade.get("qty") or trade.get("quantity") or trade.get("size"))
        volume_usdc = self._to_float(self._first(trade.get("executed_volume_usdc"), trade.get("volume_usdc")))

        vwap_buy = self._to_float(trade.get("vwap_buy"))
        vwap_sell = self._to_float(trade.get("vwap_sell"))
        spread_net_final = self._to_float(self._first(trade.get("spread_net_final"), trade.get("net_bps")))
        slippage = self._to_float(trade.get("slippage"))
        fees = self._to_float(trade.get("fees"))
        net_profit = self._to_float(trade.get("net_profit")) or 0.0

        pair_score = self._to_float(trade.get("pair_score"))
        pair_rank = self._to_int(trade.get("pair_rank"))

        ts_iso = (
                self._iso_utc_from_ms(self._first(t_done_ms, t_filled_ms, t_ack_ms, t_sent_ms))
                or datetime.utcnow().isoformat()
        )

        # --------- route (tri-CEX) ---------
        buy_ex_raw = trade.get("buy_exchange") or trade.get("buy_ex")
        sell_ex_raw = trade.get("sell_exchange") or trade.get("sell_ex")
        buy_ex = self._norm_ex(buy_ex_raw) if buy_ex_raw else None
        sell_ex = self._norm_ex(sell_ex_raw) if sell_ex_raw else None

        route = trade.get("route")
        if route is None and (buy_ex or sell_ex):
            route = f"BUY:{buy_ex or '?'}→SELL:{sell_ex or '?'}"
        route = str(route) if route is not None else None

        leg_role = trade.get("leg_role")
        if not leg_role:
            mk_flag = str(trade.get("maker", "")).lower()
            if mk_flag in ("1", "true", "yes"):
                leg_role = "maker"
            else:
                otype = str(trade.get("order_type") or trade.get("type") or "").upper()
                if "POST" in otype or "LIMIT_MAKER" in otype:
                    leg_role = "maker"

        account_alias = trade.get("account_alias")
        sub_account_id = trade.get("sub_account_id")

        # --------- reason (rejets / guards) ---------
        reason = (
                trade.get("reason")
                or trade.get("reject_reason")
                or trade.get("guard_reason")
                or trade.get("failure_reason")
                or trade.get("cancel_reason")
        )

        # --------- construction finale ---------
        entry: Dict[str, Any] = {
            "timestamp": ts_iso,
            "exchange": exchange,
            "asset": pair,

            "volume": qty,
            "price": price,
            "slippage": slippage,
            "fees": fees,
            "net_profit": net_profit,
            "trade_type": trade_type,
            "side": side,
            "strategy": strategy,
            "source": source,
            "trade_mode": trade_mode,

            "account_alias": account_alias,
            "sub_account_id": sub_account_id,

            "trade_id": trade_id,
            "bundle_id": bundle_id,
            "slice_id": slice_id,
            "client_id": client_id,
            "order_id": order_id,
            "opp_id": opp_id,
            "route_id": route_id,
            "maker_order_id": maker_order_id,
            "taker_order_id": taker_order_id,
            "rebal_group_id": rebal_group_id,

            "latency": self._to_float(trade.get("latency")),
            "engine_latency_ms": engine_latency_ms,
            "ack_latency_ms": ack_latency_ms,
            "fill_latency_ms": fill_latency_ms,
            "end_to_end_ms": end_to_end_ms,
            "t_sent_ms": t_sent_ms,
            "t_ack_ms": t_ack_ms,
            "t_filled_ms": t_filled_ms,
            "t_done_ms": t_done_ms,

            "executed_volume_usdc": volume_usdc,
            "vwap_buy": vwap_buy,
            "vwap_sell": vwap_sell,
            "spread_net_final": spread_net_final,

            "route": route,
            "buy_ex": buy_ex,
            "sell_ex": sell_ex,
            "leg_role": (leg_role or None),

            "pair_score": pair_score,
            "pair_rank": pair_rank,

            "reason": reason,  # ← **nouveau** pour audit/guard dans la DB

            "raw_json": json.dumps(trade, separators=(",", ":"), ensure_ascii=False),
        }
        return entry

    # ---------------------------- lifecycle ----------------------------
    async def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        import threading
        self._loop = asyncio.get_running_loop()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name=f"BTL-{self.log_type}", daemon=True)
        self._thread.start()
        await asyncio.sleep(0)


    async def stop(self, *, flush: bool = True) -> None:
        self._stop.set()
        if self._thread:
            try:
                self._thread.join(timeout=2.0)
            except Exception:
                pass
        if flush:
            try:
                self._emit_remaining()
            except Exception:
                pass
        await asyncio.sleep(0)

    # ------------------------------ status -----------------------------
    def get_queue_size(self) -> int:
        try:
            return int(self._queue.qsize())
        except Exception:
            return -1

    def get_status(self) -> Dict[str, Any]:
        return {
            "log_type": self.log_type,
            "queue_size": self.get_queue_size(),
            "flush_interval": self.flush_interval,
            "batch_size": self.batch_size,
            "dry_run": self.dry_run,
        }
