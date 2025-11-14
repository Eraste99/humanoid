# -*- coding: utf-8 -*-
"""
SlippageHandler v2.2 — depth‑aware + seuils dynamiques + **frais dynamiques** + BUS (per‑CEX slip)

Diffs v2.2 vs v2.1
-------------------
- **Bus consumer dédié**: ne consomme *que* les 3 routes `cex:*.slip`.
  - `attach_bus_slip_queues({"BINANCE": q1, "COINBASE": q2, "BYBIT": q3})`
  - `detach_bus_consumers()` pour arrêt propre.
- **on_slip(msg)**: traducteur payload→`ingest_snapshot(...)` (budget‑based priorité USDC/EUR).
- Le reste de l’API v2.1 est conservé (budgets/fees/dynamic thresholds/pull‑mode optionnel).

Attendu côté Router
-------------------
Message `cex:EX.slip` (voir MarketDataRouter) :
{
  "exchange": "BINANCE",
  "pair_key": "BTCUSDC",
  "slip_metric_bps": 12.3,           # proxy (info)
  "orderbook": {"bids": [[p,q],...], "asks": [[p,q],...]},
  "top_bid_vol": 1.23, "top_ask_vol": 0.98,
  "ts_ex_ms": 1700000000000, "recv_ts_ms": 1700000000500
}

"""
from __future__ import annotations
import asyncio
import time
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, Any, Callable, List, Optional, Tuple
from modules.rm_compat import getattr_int, getattr_float, getattr_str, getattr_bool, getattr_dict, getattr_list

logger = logging.getLogger("SlippageHandler")


# ----------------------------- Frais dynamiques -----------------------------
class _FeeSchedule:
    def __init__(self) -> None:
        self.default: Dict[str, Dict[str, float]] = defaultdict(lambda: {"maker": 0.001, "taker": 0.001})
        self.per_pair: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)

    @staticmethod
    def _now_ts() -> float: return time.time()
    @staticmethod
    def _norm_ex(ex: str) -> str: return (ex or "").upper()
    @staticmethod
    def _norm_pair(pk: str) -> str: return (pk or "").replace("-", "").upper()

    def set_default(self, exchange: str, maker: float, taker: float) -> None:
        ex = self._norm_ex(exchange)
        self.default[ex] = {"maker": float(max(0.0, maker)), "taker": float(max(0.0, taker))}

    def set_pair(self, exchange: str, pair_key: str, *, maker: Optional[float] = None, taker: Optional[float] = None,
                 start_ts: Optional[float] = None, end_ts: Optional[float] = None) -> None:
        ex = self._norm_ex(exchange)
        pk = self._norm_pair(pair_key)
        entry = {
            "maker": float(max(0.0, maker if maker is not None else self.default[ex]["maker"])),
            "taker": float(max(0.0, taker if taker is not None else self.default[ex]["taker"])),
            "start": float(start_ts) if start_ts else 0.0,
            "end": float(end_ts) if end_ts else None,
        }
        self.per_pair[(ex, pk)].append(entry)

    def get_fee(self, exchange: str, pair_key: str, mode: str = "taker") -> float:
        ex = self._norm_ex(exchange)
        pk = self._norm_pair(pair_key)
        mode = (mode or "taker").lower()
        now = self._now_ts()
        entries = self.per_pair.get((ex, pk), [])
        for ent in reversed(entries):
            st, en = float(ent.get("start", 0.0) or 0.0), ent.get("end")
            if (st == 0.0 or now >= st) and (en is None or now <= float(en)):
                return float(ent.get(mode, self.default[ex][mode]))
        return float(self.default[ex][mode])


class SlippageHandler:
    def __init__(
            self,
            cfg,
            depth_limit: int = 10,
            refresh_interval: float = 1.0,
            verbose: bool = False,
            volume_hint_usdc: float = 200.0,
            volume_hint_eur: float = 200.0,
            slippage_threshold: float = 0.005,  # seuil fixe minimal legacy (fallback, fraction)
            alert_callback: Optional[Callable[[str, str, Optional[str], str], Any]] = None,
            alert_cooldown_s: float = 10.0,
            # --- paramètres de seuil dynamique ---
            dyn_window_minutes: int = 30,
            dyn_factor: float = 1.5,
            dyn_min_floor: float = 0.003,  # 0.3%
            history_days_max: int = 2,
            # --- push mode ---
            push_use_qty_mode: bool = False,
    ):
        """
        Handler du slippage piloté par cfg.slip.* + paramètres runtime.

        Priorités:
          1) Arguments de __init__ (si fournis) > 2) cfg.slip / cfg.g > 3) valeurs par défaut.

        cfg.slip attend au minimum:
          - ttl_s (int), heartbeat_s (int), max_bps_by_quote (dict ex: {"USDC": 12.0, "EUR": 14.0})
        """
        # imports locaux pour rester autonome
        from collections import defaultdict, deque
        from datetime import timedelta
        import asyncio

        # --------- Source de vérité config ---------
        self.cfg = cfg
        s = self.cfg.slip  # section dédiée Slippage
        g = self.cfg.g  # globaux (pour quotes/overlays)

        # --------- TTL / Heartbeat / Seuils par quote (depuis cfg) ---------
        # (Les args __init__ ne proposent pas TTL/heartbeat, donc cfg -> direct)
        self._ttl_s = getattr_int(s, "ttl_s", 2)
        self._heartbeat_s = getattr_int(s, "heartbeat_s", 1)
        # max slippage “dur” autorisé par quote (bps)
        self._max_bps_by_quote = dict(getattr(s, "max_bps_by_quote", {"USDC": 12.0, "EUR": 14.0}))

        # --------- Paramètres runtime (arguments, avec fallback cfg si utile) ---------
        self.depth_limit = int(depth_limit)
        self.refresh_interval = float(refresh_interval)
        self.verbose = bool(verbose)

        # Budgets “hints” par devise de cotation:
        # si l’appelant n’a pas changé les défauts 200/200, on peut substituer avec un fallback depuis cfg
        # (par ex. min_fragment quote globale si tu veux coller au sizing du profil).
        # On garde cependant LES ARGUMENTS comme priorité (déjà appliqués ci-dessus).
        mfq = dict(getattr(g, "min_fragment_quote", {"USDC": 200.0, "EUR": 200.0}))
        vol_usdc = float(volume_hint_usdc if volume_hint_usdc is not None else mfq.get("USDC", 200.0))
        vol_eur = float(volume_hint_eur if volume_hint_eur is not None else mfq.get("EUR", 200.0))

        self._pair_budget: Dict[str, float] = {}
        self._quote_budget: Dict[str, float] = {"USDC": vol_usdc, "EUR": vol_eur}

        # seuil legacy min (fraction, ex: 0.005 = 50 bps)
        self.slippage_threshold = float(slippage_threshold)
        self.alert_callback = alert_callback
        self.alert_cooldown_s = float(alert_cooldown_s)
        self._last_alert_ts: Dict[str, float] = {}

        # --------- État interne des mesures ---------
        # slippage_data[exchange][symbol] = {"buy": float|None, "sell": float|None}
        self.slippage_data: Dict[str, Dict[str, Dict[str, Optional[float]]]] = {}
        self.last_update: Optional[float] = None
        self.slippage_avg: float = 0.0
        self.update_count: int = 0

        self._running = False
        self.healthy: bool = True

        # --------- Historique & dyn thresholds ---------
        self._history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=48_000))
        self._dyn_window = timedelta(minutes=int(dyn_window_minutes))
        self._dyn_factor = float(dyn_factor)
        self._dyn_min_floor = float(dyn_min_floor)
        self._history_max_age = timedelta(days=int(history_days_max))

        # --------- Frais dynamiques (stub existant) ---------
        self._fees = _FeeSchedule()  # supposé fourni dans le module

        # --------- Push mode ---------
        self.push_use_qty_mode = bool(push_use_qty_mode)

        # --------- Bus internes (consommateurs de flux slip par CEX) ---------
        self._bus_tasks: Dict[str, asyncio.Task] = {}

    # Petits helpers (inchangés)
    def ttl_seconds(self) -> int:
        return self._ttl_s

    def max_bps_allowed(self, quote: str) -> float:
        return float(self._max_bps_by_quote.get(str(quote).upper(), 9999.0))
    # -------------------- utils --------------------
    @staticmethod
    def _to_f(x) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    @staticmethod
    def _norm_ex(ex: str) -> str:
        return (ex or "").upper()

    @staticmethod
    def _norm_pair(pk: str) -> str:
        return (pk or "").replace("-", "").upper()

    def _infer_quote(self, symbol: str) -> Optional[str]:
        sym = self._norm_pair(symbol)
        for q in sorted(self._quote_budget.keys(), key=len, reverse=True):
            if sym.endswith(q):
                return q
        return None

    def set_quote_budget(self, quote: str, amount: float) -> None:
        self._quote_budget[(quote or "").upper()] = float(max(0.0, amount))

    def set_pair_budget(self, pair_key: str, amount: float) -> None:
        self._pair_budget[self._norm_pair(pair_key)] = float(max(0.0, amount))

    def _budget_for(self, symbol: str) -> float:
        sym = self._norm_pair(symbol)
        if sym in self._pair_budget:
            return float(self._pair_budget[sym])
        q = self._infer_quote(sym)
        if q and q in self._quote_budget:
            return float(self._quote_budget[q])
        return float(self._quote_budget.get("USDC", 0.0))

    def _prune_history(self, now_s: float) -> None:
        cutoff = now_s - self._history_max_age.total_seconds()
        for sym, dq in list(self._history.items()):
            while dq and dq[0][0] < cutoff:
                dq.popleft()
            if not dq:
                self._history.pop(sym, None)

    def _append_history(self, symbol: str, slip: float, now_s: Optional[float] = None) -> None:
        if slip is None:
            return
        ts = now_s if now_s is not None else time.time()
        self._history[self._norm_pair(symbol)].append((ts, float(slip)))

    async def _maybe_alert(self, pair: str, slip: float):
        if not self.alert_callback:
            return
        now = time.time()
        last = self._last_alert_ts.get(pair, 0.0)
        if (now - last) < self.alert_cooldown_s:
            return
        self._last_alert_ts[pair] = now
        try:
            r = self.alert_callback(
                "SlippageHandler",
                f"Slippage élevé {pair}: {slip:.2%} (≥ {self.get_dynamic_threshold(pair):.2%})",
                pair=pair,
                alert_type="WARNING",
            )
            if asyncio.iscoroutine(r):
                await r
        except Exception:
            logging.exception("Unhandled exception")

    # -------------------- Frais dynamiques (API) --------------------
    def set_default_fees(self, exchange: str, *, maker: float, taker: float) -> None:
        self._fees.set_default(exchange, maker, taker)

    def set_pair_fees(self, exchange: str, pair_key: str, *, maker: Optional[float] = None, taker: Optional[float] = None,
                      start_ts: Optional[float] = None, end_ts: Optional[float] = None) -> None:
        self._fees.set_pair(exchange, pair_key, maker=maker, taker=taker, start_ts=start_ts, end_ts=end_ts)

    def get_fee_pct(self, exchange: str, pair_key: str, mode: str = "taker") -> float:
        return float(self._fees.get_fee(exchange, pair_key, mode))

    def get_total_fees_pct(
        self,
        buy_ex: str,
        sell_ex: str,
        pair_key: str,
        *,
        maker_on_buy: bool = False,
        maker_on_sell: bool = False,
    ) -> float:
        mode_buy = "maker" if bool(maker_on_buy) else "taker"
        mode_sell = "maker" if bool(maker_on_sell) else "taker"
        fb = self.get_fee_pct(buy_ex, pair_key, mode_buy)
        fs = self.get_fee_pct(sell_ex, pair_key, mode_sell)
        return float(max(0.0, fb + fs))

    # -------------------- Mode push (router) --------------------
    def set_push_qty_mode(self, enabled: bool) -> None:
        self.push_use_qty_mode = bool(enabled)

    def ingest_snapshot(
        self,
        exchange: str,
        symbol: str,
        orderbook: Dict[str, Any],
        bid_volume: float = 0.0,
        ask_volume: float = 0.0,
    ) -> None:
        """
        Ingestion d'un snapshot: orderbook = {"bids":[[p,qty],...], "asks":[[p,qty],...]}
        v2.2: priorité au **VWAP budget en devise de cotation** (USDC/EUR) selon le symbole.
        Le mode qty est utilisé seulement si `push_use_qty_mode=True`.
        """
        try:
            sym = self._norm_pair(symbol)
            bids = (orderbook or {}).get("bids") or []
            asks = (orderbook or {}).get("asks") or []

            buy_slip = None
            sell_slip = None

            if bids and asks:
                budget = self._budget_for(sym)
                if budget > 0:
                    best_bid = self._to_f(bids[0][0])
                    best_ask = self._to_f(asks[0][0])
                    if best_bid > 0 and best_ask > best_bid:
                        buy_vwap = self._vwap_depth(asks, budget, side="buy")
                        sell_vwap = self._vwap_depth(bids, budget, side="sell")
                        if buy_vwap > 0:
                            buy_slip = max(0.0, (buy_vwap - best_ask) / best_ask)
                        if sell_vwap > 0:
                            sell_slip = max(0.0, (best_bid - sell_vwap) / best_bid)

            if self.push_use_qty_mode:
                if asks and ask_volume:
                    qs = self._simulate_slippage(asks, float(ask_volume))
                    if qs is not None:
                        buy_slip = qs
                if bids and bid_volume:
                    qs = self._simulate_slippage(bids, float(bid_volume))
                    if qs is not None:
                        sell_slip = qs

            if (buy_slip is None or sell_slip is None) and bids and asks:
                bb = self._to_f(bids[0][0])
                ba = self._to_f(asks[0][0])
                if bb > 0 and ba > bb:
                    mid = 0.5 * (bb + ba)
                    approx = (ba - bb) / mid
                    if buy_slip is None:
                        buy_slip = approx
                    if sell_slip is None:
                        sell_slip = approx

            ex = self._norm_ex(exchange)
            self.slippage_data.setdefault(ex, {})[sym] = {"buy": buy_slip, "sell": sell_slip}
            now_s = time.time()
            self.last_update = now_s
            self.update_count += 1
            self.healthy = True

            worst = None
            if buy_slip is not None and sell_slip is not None:
                worst = max(buy_slip, sell_slip)
            elif buy_slip is not None:
                worst = buy_slip
            elif sell_slip is not None:
                worst = sell_slip
            if worst is not None:
                self._append_history(sym, float(worst), now_s=now_s)
                self._prune_history(now_s)
        except Exception as e:
            logger.error(f"[SlippageHandler] ingest_snapshot error: {e}")
            self.healthy = False

    # --------------------- Mode pull (périodique — optionnel) ------------------
    async def start(self, get_orderbooks):
        if self._running:
            logger.warning("SlippageHandler déjà en cours.")
            return
        self._running = True
        self.slippage_data.clear()
        self.slippage_avg = 0.0
        self.last_update = None
        self.update_count = 0
        self.healthy = True

        if get_orderbooks is None:
            logger.info("🚀 SlippageHandler (push‑only) démarré")
            try:
                while self._running:
                    await asyncio.sleep(self.refresh_interval)
            finally:
                self._running = False
            return

        await self.run(get_orderbooks)

    async def stop(self):
        self._running = False
        logger.info("🛑 SlippageHandler arrêté")

    async def run(self, get_orderbooks):
        logger.info("🚀 SlippageHandler démarré (pull)")
        while self._running:
            try:
                raw = await get_orderbooks() if asyncio.iscoroutinefunction(get_orderbooks) else get_orderbooks()
                norm: Dict[str, Dict[str, Dict[str, Any]]] = {}
                for ex, pairs in (raw or {}).items():
                    if not isinstance(pairs, dict):
                        continue
                    ed = norm.setdefault(self._norm_ex(ex), {})
                    for sym, d in pairs.items():
                        if isinstance(d, dict) and ("orderbook" in d or "bids" in d or "asks" in d):
                            if "orderbook" in d:
                                ob = d.get("orderbook") or {}
                                bids = ob.get("bids") or d.get("bids") or []
                                asks = ob.get("asks") or d.get("asks") or []
                            else:
                                bids = d.get("bids") or []
                                asks = d.get("asks") or []
                            best_bid = d.get("best_bid") or (bids[0][0] if bids else 0.0)
                            best_ask = d.get("best_ask") or (asks[0][0] if asks else 0.0)
                            active = d.get("active", True)
                        else:
                            continue
                        if not active:
                            continue
                        ed[self._norm_pair(sym)] = {
                            "best_bid": self._to_f(best_bid),
                            "best_ask": self._to_f(best_ask),
                            "bids": bids or [],
                            "asks": asks or [],
                        }

                self.collect_from_orderbooks(norm)
                self.healthy = True
            except Exception as e:
                logger.error(f"[SlippageHandler] run() error: {e}")
                self.healthy = False

            await asyncio.sleep(self.refresh_interval)

    # --------------------- Ingestion "all_books" --------------------
    def collect_from_orderbooks(self, all_books: Dict[str, Dict[str, Any]]) -> None:
        if not isinstance(all_books, dict):
            return

        total = 0.0
        n = 0
        now_s = time.time()

        for ex, pairs in all_books.items():
            if not isinstance(pairs, dict):
                continue
            book_ex = self.slippage_data.setdefault(ex, {})
            for pair, d in pairs.items():
                bid = self._to_f((d or {}).get("best_bid"))
                ask = self._to_f((d or {}).get("best_ask"))
                if bid <= 0 or ask <= 0 or bid >= ask:
                    continue

                bids = (d or {}).get("bids") or []
                asks = (d or {}).get("asks") or []

                if bids and asks:
                    budget = self._budget_for(pair)
                    buy_vwap = self._vwap_depth(asks, budget, side="buy") if budget > 0 else 0.0
                    sell_vwap = self._vwap_depth(bids, budget, side="sell") if budget > 0 else 0.0
                    buy_slip = max(0.0, (buy_vwap - ask) / ask) if buy_vwap > 0 else 0.0
                    sell_slip = max(0.0, (bid - sell_vwap) / bid) if sell_vwap > 0 else 0.0
                    slip = max(buy_slip, sell_slip)
                    book_ex[pair] = {"buy": buy_slip, "sell": sell_slip}
                else:
                    mid = (bid + ask) / 2.0
                    slip = (ask - bid) / mid if mid > 0 else 0.0
                    book_ex[pair] = {"buy": slip, "sell": slip}

                total += slip
                n += 1

                self._append_history(pair, float(slip), now_s=now_s)
                if slip >= self.get_dynamic_threshold(pair):
                    asyncio.create_task(self._maybe_alert(pair, slip))

        self.last_update = now_s
        self.update_count += n
        self._prune_history(now_s)
        self.slippage_avg = round(total / n, 6) if n else 0.0

        if self.verbose:
            logger.info(f"[SlippageHandler] MAJ: pairs={n}, avg={self.slippage_avg:.4%}")

    # ------------------------- Calculs core -------------------------
    def _vwap_depth(self, levels: List[Tuple[Any, Any]], quote_budget: float, side: str) -> float:
        budget = float(max(0.0, quote_budget))
        if budget <= 0 or not levels:
            return 0.0
        lvls = list(levels)
        try:
            lvls = sorted(lvls, key=lambda x: float(x[0]), reverse=(side == "sell"))
        except Exception:
            logging.exception("Unhandled exception")
        cost = 0.0
        qty_filled = 0.0
        remain = budget
        for p, q in lvls[: self.depth_limit]:
            try:
                p = float(p); q = float(q)
            except Exception:
                continue
            if p <= 0 or q <= 0:
                continue
            max_qty = remain / p
            take = min(q, max_qty)
            if take <= 0:
                break
            cost += take * p
            qty_filled += take
            remain -= take * p
            if remain <= 1e-9:
                break
        if qty_filled <= 0:
            return 0.0
        return cost / qty_filled

    def _simulate_slippage(self, levels: List[Tuple[Any, Any]], volume: float) -> Optional[float]:
        if volume <= 0 or not levels:
            return None
        total_cost = 0.0
        total_volume = 0.0
        for price_str, qty_str in levels[: self.depth_limit]:
            try:
                price = float(price_str)
                qty = float(qty_str)
            except Exception:
                if self.verbose:
                    logger.warning(f"Valeurs invalides carnet: price={price_str}, qty={qty_str}")
                continue
            if total_volume + qty >= volume:
                remaining = volume - total_volume
                total_cost += remaining * price
                total_volume += remaining
                break
            else:
                total_cost += qty * price
                total_volume += qty
        if total_volume < volume:
            return None
        avg_price = total_cost / volume
        best_price = float(levels[0][0])
        slippage = abs(avg_price - best_price) / max(best_price, 1e-12)
        return round(slippage, 6)

    # -------------------------- Seuil dynamique -------------------------------
    def get_dynamic_threshold(self, symbol: str) -> float:
        try:
            now_s = time.time()
            dq = self._history.get(self._norm_pair(symbol))
            if not dq:
                return max(self._dyn_min_floor, self.slippage_threshold)
            lo_cut = now_s - self._dyn_window.total_seconds()
            window_vals = [v for (ts, v) in dq if ts >= lo_cut]
            if len(window_vals) < 20:
                return max(self._dyn_min_floor, self.slippage_threshold)
            window_vals.sort()
            k = int(round(0.95 * (len(window_vals) - 1)))
            p95 = window_vals[max(0, min(len(window_vals) - 1, k))]
            return max(self._dyn_min_floor, p95 * self._dyn_factor)
        except Exception:
            return max(self._dyn_min_floor, self.slippage_threshold)

    # -------------------------- API -------------------------------
    def get_slippage(self, exchange: str, symbol: str, side: Optional[str] = None) -> Optional[float]:
        d = self.slippage_data.get(self._norm_ex(exchange), {}).get(self._norm_pair(symbol), {})
        if side is None:
            vals = [d.get("buy"), d.get("sell")]
        else:
            vals = [d.get(side)]
        vals = [v for v in vals if v is not None]
        return (sum(vals) / len(vals)) if vals else None

    def get_all_slippages(self) -> Dict[str, Dict[str, Dict[str, Optional[float]]]]:
        return self.slippage_data

    def update_config(
        self,
        depth_limit: Optional[int] = None,
        refresh_interval: Optional[float] = None,
        volume_hint_usdc: Optional[float] = None,
        volume_hint_eur: Optional[float] = None,
        volume_hint_by_quote: Optional[Dict[str, float]] = None,
        slippage_threshold: Optional[float] = None,
        alert_cooldown_s: Optional[float] = None,
        dyn_window_minutes: Optional[int] = None,
        dyn_factor: Optional[float] = None,
        dyn_min_floor: Optional[float] = None,
        history_days_max: Optional[int] = None,
        push_use_qty_mode: Optional[bool] = None,
    ):
        if depth_limit is not None:
            self.depth_limit = int(depth_limit)
        if refresh_interval is not None:
            self.refresh_interval = float(refresh_interval)
        if volume_hint_usdc is not None:
            self._quote_budget["USDC"] = float(volume_hint_usdc)
        if volume_hint_eur is not None:
            self._quote_budget["EUR"] = float(volume_hint_eur)
        if volume_hint_by_quote is not None:
            for q, v in (volume_hint_by_quote or {}).items():
                self._quote_budget[(q or "").upper()] = float(max(0.0, v))
        if slippage_threshold is not None:
            self.slippage_threshold = float(slippage_threshold)
        if alert_cooldown_s is not None:
            self.alert_cooldown_s = float(alert_cooldown_s)
        if dyn_window_minutes is not None:
            self._dyn_window = timedelta(minutes=int(dyn_window_minutes))
        if dyn_factor is not None:
            self._dyn_factor = float(dyn_factor)
        if dyn_min_floor is not None:
            self._dyn_min_floor = float(dyn_min_floor)
        if history_days_max is not None:
            self._history_max_age = timedelta(days=int(history_days_max))
        if push_use_qty_mode is not None:
            self.push_use_qty_mode = bool(push_use_qty_mode)
        logger.info(
            f"[SlippageHandler] config: depth_limit={self.depth_limit}, refresh_interval={self.refresh_interval}, "
            f"quote_budgets={self._quote_budget}, slippage_threshold={self.slippage_threshold}, "
            f"dyn_window={self._dyn_window}, dyn_factor={self._dyn_factor}, dyn_min_floor={self._dyn_min_floor}, "
            f"history_days_max={self._history_max_age}, push_use_qty_mode={self.push_use_qty_mode}"
        )

    def get_status(self) -> Dict[str, Any]:
        slips = [
            v
            for ex in self.slippage_data.values()
            for sym in ex.values()
            for v in sym.values()
            if v is not None
        ]
        slippage_max = max(slips) if slips else 0.0
        dyn_preview = {}
        try:
            current_pairs = {sym for ex in self.slippage_data.values() for sym in ex.keys()}
            top = sorted(current_pairs, key=lambda s: self.get_dynamic_threshold(s), reverse=True)[:10]
            for s in top:
                dyn_preview[s] = self.get_dynamic_threshold(s)
        except Exception:
            dyn_preview = {}

        now = time.time()
        age_s = (now - self.last_update) if self.last_update else None

        return {
            "module": "SlippageHandler",
            "healthy": self.healthy,
            "last_update_epoch_s": self.last_update,
            "age_s": age_s,
            "update_count": self.update_count,
            "details": "Slippage sous contrôle" if self.healthy else "Erreur détectée",
            "metrics": {
                "slippage_avg": self.slippage_avg,
                "slippage_max": slippage_max,
                "dynamic_thresholds_preview": dyn_preview,
                "quote_budgets": dict(self._quote_budget),
            },
            "submodules": {},
        }

    # -------------------------- BUS (per‑CEX slip) --------------------------
    # slippage_handler.py
    # === SlippageHandler ===
    def attach_bus_slip_queues(self, queues_by_ex: dict[str, asyncio.Queue]) -> None:
        """Démarre un consumer par CEX pour le bus 'slip'."""

        async def _consume(ex: str, q: asyncio.Queue):
            while True:
                msg = await q.get()
                try:
                    # IMPORTANT: on_slip est async -> await
                    await self.on_slip(msg)
                except Exception:
                    logging.exception("Unhandled exception in slip consumer")
                finally:
                    try:
                        q.task_done()
                    except Exception:
                        pass

        # cancel anciens tasks si besoin
        if hasattr(self, "_bus_tasks"):
            for t in self._bus_tasks.values():
                try:
                    t.cancel()
                except Exception:
                    pass
        self._bus_tasks = {}

        for ex, q in (queues_by_ex or {}).items():
            if not isinstance(q, asyncio.Queue):
                continue
            t = asyncio.create_task(_consume(str(ex).upper(), q), name=f"slip-{ex}")
            self._bus_tasks[str(ex).upper()] = t
        logger.info("[SlippageHandler] slip consumers: %s", list(self._bus_tasks.keys()))

    async def on_slip(self, msg: dict) -> None:
        """
        Consomme un message 'slip' du Router.
        Attendu: {
          "exchange": "BINANCE", "pair_key": "BTCUSDT",
          "orderbook": {"bids":[[px,qty],...], "asks":[[px,qty],...]},
          "top_bid_vol": float, "top_ask_vol": float,
          "best_bid": float, "best_ask": float,
          "exchange_ts_ms": int, "recv_ts_ms": int
        }
        Met à jour: self._slip_bps[(ex,pair,side)], self._slip_ts[(ex,pair)]
        """
        try:
            ex = (msg.get("exchange") or "").upper()
            pair = (msg.get("pair_key") or "").replace("-", "").upper()
            ob = (msg.get("orderbook") or {})
            bids = ob.get("bids") or []
            asks = ob.get("asks") or []
            best_bid = float(msg.get("best_bid") or (bids[0][0] if bids else 0.0))
            best_ask = float(msg.get("best_ask") or (asks[0][0] if asks else 0.0))
            top_bid_vol = float(msg.get("top_bid_vol") or 0.0)
            top_ask_vol = float(msg.get("top_ask_vol") or 0.0)

            # Estimation conservative de slip bps côté taker
            # (tu peux garder ta formule actuelle; ci-dessous: proxy simple)
            def _bps(px_move_frac: float) -> float:
                return max(0.0, px_move_frac) * 1e4

            slip_buy_bps = _bps((best_ask - best_bid) / max(best_bid, 1e-12))  # acheter -> traverse ask
            slip_sell_bps = slip_buy_bps  # symétrie simple; remplace par ton calcul si dispo

            self._slip_bps = getattr(self, "_slip_bps", {})
            self._slip_ts = getattr(self, "_slip_ts", {})
            self._slip_bps[(ex, pair, "buy")] = slip_buy_bps
            self._slip_bps[(ex, pair, "sell")] = slip_sell_bps
            self._slip_ts[(ex, pair)] = time.time()

        except Exception:
            logging.exception("[Slip] on_slip error")

    # slippage_handler.py
    def get_slippage_bps(self, exchange: str, pair_key: str, side: str | None = None, *,
                         ttl_s: float | None = None) -> float | None:
        """
        Retourne le slippage estimé (en bps) si frais (TTL), sinon None.
        TTL: SLIP_TTL_S (bot_cfg) ou 3.0s par défaut.
        side: "buy" | "sell" | None (None => max des deux si dispos)
        """
        ex = (exchange or "").upper()
        pk = (pair_key or "").replace("-", "").upper()
        bps = getattr(self, "_slip_bps", {})
        tsd = getattr(self, "_slip_ts", {})

        now = time.time()
        ttl = 3.0
        try:
            if hasattr(self, "bot_cfg"):
                ttl = float(getattr(self.bot_cfg, "slip", getattr(self.bot_cfg, "cfg", None)).ttl_s if hasattr(self.bot_cfg, "slip") else self.bot_cfg.slip.ttl_s)
        except Exception:
            pass

        if side is None:
            vals = []
            for s in ("buy", "sell"):
                v = bps.get((ex, pk, s))
                t = tsd.get((ex, pk))
                if v is not None and t is not None and (now - float(t)) <= ttl:
                    vals.append(float(v))
            return max(vals) if vals else None

        v = bps.get((ex, pk, str(side).lower()))
        t = tsd.get((ex, pk))
        if v is None or t is None:
            return None
        return float(v) if (now - float(t)) <= ttl else None

    def detach_bus_consumers(self) -> None:
        for ex, t in list(self._bus_tasks.items()):
            t.cancel()
        self._bus_tasks.clear()
