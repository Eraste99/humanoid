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

logger = logging.getLogger("SlippageHandler")

try:
    from modules.obs_metrics import (
        SLIP_SAMPLE_TOTAL,
        SLIP_DECISION_TOTAL,
        SLIP_P95_BPS,
        SLIP_P99_BPS,
        set_slip_age_seconds,
        note_slip_ttl_seconds,
        note_slip_drop,
        inc_blocked,
    )
except Exception:  # pragma: no cover


    class _Noop:
        def labels(self, *_, **__):
            return self

        def set(self, *_, **__):
            return None

        def inc(self, *_, **__):
            return None

        def observe(self, *_, **__):
            return None


    SLIP_SAMPLE_TOTAL = _Noop()
    SLIP_DECISION_TOTAL = _Noop()
    SLIP_P95_BPS = _Noop()
    SLIP_P99_BPS = _Noop()


    def set_slip_age_seconds(*_, **__):
        return None

    def note_slip_ttl_seconds(*_, **__):
        return None

    def note_slip_drop(*_, **__):
        return None

    def inc_blocked(*_, **__):
        return None

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
        self._missing_slo_warned: set[tuple[str, str]] = set()
        self._slo_resolution_warned = False

        # --------- TTL / Heartbeat / Seuils par quote (depuis cfg) ---------
        # (Les args __init__ ne proposent pas TTL/heartbeat, donc cfg -> direct)
        self._ttl_s = int(getattr(s, "ttl_s", 2))
        self._heartbeat_s = int(getattr(s, "heartbeat_s", 1))
        self._use_vwap_depth = bool(getattr(s, "use_vwap_depth", True))
        # max slippage “dur” autorisé par quote (bps)
        self._max_bps_by_quote = dict(getattr(s, "max_bps_by_quote", {"USDC": 12.0, "EUR": 14.0}))

        try:
            note_slip_ttl_seconds(self._ttl_s)
        except Exception:
            pass

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
        self._primary_quote = str(getattr(g, "primary_quote", "USDC")).upper()
        self._default_strategy = self._trade_mode()


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
        # --------- Optimisation P0: Cache p95 (Hot Path) ---------
        self._p95_cache: Dict[str, float] = {}
        self._last_p95_calc_s = 0.0
        self._p95_calc_interval = 1.0  # recalcul 1Hz

    # Petits helpers (inchangés)
    def ttl_seconds(self) -> int:
        return self._ttl_s

    def _strict_config(self) -> bool:
        rm_cfg = getattr(self.cfg, "rm", None)
        slip_cfg = getattr(self.cfg, "slip", None)
        return bool(
            getattr(rm_cfg, "strict_config", False)
            or getattr(slip_cfg, "strict_config", False)
        )

    def _deployment_mode(self) -> str:
        g_cfg = getattr(self.cfg, "g", None)
        mode = getattr(g_cfg, "deployment_mode", None) if g_cfg else None
        mode_norm = str(mode).strip().upper() if mode else ""
        if not mode_norm:
            msg = "deployment_mode missing; using EU_ONLY fallback"
            if self._strict_config():
                raise RuntimeError(msg)
            logger.warning("[SlippageHandler] %s", msg)
            mode_norm = "EU_ONLY"
        if mode_norm not in {"EU_ONLY", "SPLIT", "JP_ONLY"}:
            logger.warning("[SlippageHandler] unknown deployment_mode=%s; falling back to EU_ONLY", mode_norm)
            mode_norm = "EU_ONLY"
        return mode_norm

    def _trade_mode(self) -> str:
        rm_cfg = getattr(self.cfg, "rm", None)
        g_cfg = getattr(self.cfg, "g", None)
        rm_mode = getattr(rm_cfg, "trade_mode", None) if rm_cfg else None
        g_mode = getattr(g_cfg, "trade_mode", None) if g_cfg else None
        rm_norm = str(rm_mode).strip().upper() if rm_mode else ""
        g_norm = str(g_mode).strip().upper() if g_mode else ""
        if rm_norm and g_norm and rm_norm != g_norm:
            msg = f"trade_mode mismatch: rm.trade_mode={rm_norm} g.trade_mode={g_norm}"
            if self._strict_config():
                raise RuntimeError(msg)
            logger.warning("[SlippageHandler] %s; using rm.trade_mode", msg)
        if rm_norm:
            return rm_norm
        if g_norm:
            return g_norm
        return "TT"

    def _resolve_ttl_s(self, exchange: str, ttl_s: float | None) -> float:
        base_ttl = float(ttl_s) if ttl_s is not None else float(getattr(self, "_ttl_s", 2.0))
        mode_key = self._deployment_mode()
        exu = str(exchange or "").upper()
        slo_ttl = None
        try:
            cfg = getattr(self, "cfg", None)
            slo_map = getattr(cfg, "slo", None) if cfg is not None else None
            if slo_map:
                per_ex = slo_map.get(mode_key) or {}
                path_slo = per_ex.get(exu)
                if path_slo is not None and getattr(path_slo, "public", None) is not None:
                    slo_ttl_s = float(getattr(path_slo.public, "slip_ttl_s", 0.0) or 0.0)
                    if slo_ttl_s > 0.0:
                        slo_ttl = slo_ttl_s
        except Exception:
            if not self._slo_resolution_warned:
                self._slo_resolution_warned = True
                logger.warning(
                    "[SlippageHandler] unable to resolve SLO-based slip TTL; using cfg.slip.ttl_s",
                    exc_info=False,
                )
            return base_ttl

        if slo_ttl is None:
            warn_key = (mode_key, exu)
            if warn_key not in self._missing_slo_warned:
                self._missing_slo_warned.add(warn_key)
                logger.warning(
                    "[SlippageHandler] missing SLO slip_ttl_s for mode=%s exchange=%s; using cfg.slip.ttl_s",
                    mode_key,
                    exu,
                )
            return base_ttl

        ttl = min(base_ttl, slo_ttl)
        if ttl_s is not None and ttl_s > ttl:
            logger.debug(
                "[SlippageHandler] ttl_s clamped by SLO",
                extra={"requested_ttl": base_ttl, "slo_ttl": slo_ttl, "effective_ttl": ttl},
            )
        return ttl
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

    @staticmethod
    def _note_drop(reason: str, exchange: str, pair: str) -> None:
        try:
            note_slip_drop(reason, exchange)
        except Exception:
            pass

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

    def _budget_for(self, symbol: str, *, strategy: Optional[str] = None) -> Optional[float]:
        sym = self._norm_pair(symbol)
        if sym in self._pair_budget:
            return float(self._pair_budget[sym])
        q = self._infer_quote(sym)
        if not q:
            inc_blocked("slippage_handler", "quote_unknown", sym)
            return None
        strat = str(strategy or self._default_strategy or "TT").upper()
        if strat not in ("TT", "TM", "MM", "REB"):
            inc_blocked("slippage_handler", "strategy_unknown", sym)
            return None
        base = float(self._quote_budget.get(q, 0.0))
        ratio = None
        try:
            ratio = (getattr(self.cfg.g, "branch_budgets_quote", {}) or {}).get(q, {}).get(strat)
        except Exception:
            ratio = None
        if isinstance(ratio, (int, float)):
            base = base * float(ratio)
        return max(0.0, float(base))


    def _prune_history(self, now_s: float) -> None:
        cutoff = now_s - self._history_max_age.total_seconds()
        for sym, dq in list(self._history.items()):
            while dq and dq[0][0] < cutoff:
                dq.popleft()
            if not dq:
                self._history.pop(sym, None)
                self._p95_cache.pop(sym, None)
        
        # Recalcul des percentiles à cadence fixe (1 Hz)
        if now_s - self._last_p95_calc_s >= self._p95_calc_interval:
            self._update_p95_cache(now_s)

    def _update_p95_cache(self, now_s: float) -> None:
        """Recalcule les p95 pour toutes les paires actives (O(N) amorti)."""
        self._last_p95_calc_s = now_s
        lo_cut = now_s - self._dyn_window.total_seconds()
        
        for pk, dq in self._history.items():
            try:
                # On ne prend que les valeurs de la fenêtre glissante
                vals = [v for (ts, v) in dq if ts >= lo_cut]
                if len(vals) < 20:
                    continue
                
                # Tri une fois par seconde au lieu de chaque event
                vals.sort()
                k = int(round(0.95 * (len(vals) - 1)))
                p95 = vals[max(0, min(len(vals) - 1, k))]
                self._p95_cache[pk] = float(p95)
                
                # Métriques (optionnel, sample)
                try:
                    if SLIP_P95_BPS:
                        SLIP_P95_BPS.labels(pair=pk).set(float(p95) * 1e4)
                except Exception:
                    pass
            except Exception:
                continue

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
        strategy: Optional[str] = None,
    ) -> None:
        """
        Ingestion d'un snapshot: orderbook = {"bids":[[p,qty],...], "asks":[[p,qty],...]}
        v2.2: priorité au **VWAP budget en devise de cotation** (USDC/EUR) selon le symbole.
        Le mode qty est utilisé seulement si `push_use_qty_mode=True`.
        Unité interne stockée: fraction (ex: 0.001 = 10 bps). Sortie canonique: get_slippage_bps().
        """
        try:
            sym = self._norm_pair(symbol)
            bids = (orderbook or {}).get("bids") or []
            asks = (orderbook or {}).get("asks") or []
            if not orderbook:
                self._note_drop("missing_orderbook", exchange, sym)
                return
            if not bids or not asks:
                self._note_drop("depth_insufficient", exchange, sym)
                return

            buy_slip = None
            sell_slip = None

            if bids and asks and self._use_vwap_depth:
                budget = self._budget_for(sym, strategy=strategy)
                if budget is None:
                    return
                if budget > 0:
                    best_bid = self._to_f(bids[0][0])
                    best_ask = self._to_f(asks[0][0])
                    if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                        self._note_drop("bad_bid_ask", exchange, sym)
                        return
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
                if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                    self._note_drop("bad_bid_ask", exchange, sym)
                    return
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
                    self._note_drop("bad_bid_ask", ex, pair)
                    continue

                bids = (d or {}).get("bids") or []
                asks = (d or {}).get("asks") or []
                if not bids or not asks:
                    self._note_drop("depth_insufficient", ex, pair)
                    continue

                if bids and asks and self._use_vwap_depth:
                    budget = self._budget_for(pair)
                    if budget is None:
                        continue
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
                if slip >= self.get_dynamic_threshold(pair, strategy=None):
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
    def get_dynamic_threshold(self, symbol: str, *, strategy: Optional[str] = None) -> float:
        try:
            pk = self._norm_pair(symbol)
            p95 = self._p95_cache.get(pk)
            
            if p95 is None:
                # Fallback sur calcul à la demande uniquement si cache vide
                dq = self._history.get(pk)
                if not dq or len(dq) < 20:
                    return max(self._dyn_min_floor, self.slippage_threshold)
                
                now_s = time.time()
                lo_cut = now_s - self._dyn_window.total_seconds()
                window_vals = [v for (ts, v) in dq if ts >= lo_cut]
                if len(window_vals) < 20:
                    return max(self._dyn_min_floor, self.slippage_threshold)
                window_vals.sort()
                k = int(round(0.95 * (len(window_vals) - 1)))
                p95 = window_vals[max(0, min(len(window_vals) - 1, k))]
                self._p95_cache[pk] = p95

            q = self._infer_quote(symbol)
            if not q:
                return max(self._dyn_min_floor, self.slippage_threshold)
            
            strat = str(strategy or self._default_strategy or "TT").upper()
            
            ratio = None
            try:
                ratio = (getattr(self.cfg.g, "branch_budgets_quote", {}) or {}).get(q, {}).get(strat)
            except Exception:
                ratio = None
            scale = 1.0 / float(ratio) if isinstance(ratio, (int, float)) and ratio > 0 else 1.0
            
            return max(self._dyn_min_floor, p95 * self._dyn_factor * scale)
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

    def get_slippage_fraction(self, exchange: str, symbol: str, side: Optional[str] = None) -> Optional[float]:
        """Alias explicite (fraction 0..1) pour différencier de la version bps TTL."""
        return self.get_slippage(exchange, symbol, side)

    def get_all_slippages(self) -> Dict[str, Dict[str, Dict[str, Optional[float]]]]:
        return self.slippage_data

    def last_age_seconds(self, exchange: str, pair_key: str) -> Optional[float]:
        ex = (exchange or "").upper()
        pk = (pair_key or "").replace("-", "").upper()
        tsd = getattr(self, "_slip_ts_mono", None)
        if tsd is None:
            tsd = getattr(self, "_slip_ts", {})

        t = tsd.get((ex, pk))
        if t is None:
            return None
        return max(0.0, time.monotonic() - float(t))

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

    def set_risk_manager(self, rm: Any) -> None:
        self.risk_manager = rm


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

        st: Dict[str, Any] = {
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
        rm = getattr(self, "risk_manager", None)
        if rm is not None:
            try:
                modes = {
                    "rm_mode": str(getattr(rm, "rm_mode", "UNKNOWN")),
                    "trade_mode": str(getattr(rm, "trade_mode", "UNKNOWN")),
                }
                if hasattr(rm, "private_plane_state"):
                    modes["private_plane_state"] = str(rm.private_plane_state)
                st["modes"] = modes
            except Exception:
                pass
        return st


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
                    try:
                        self._note_drop("consumer_error", ex, "UNKNOWN")
                    except Exception:
                        pass
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
            ex = self._norm_ex(msg.get("exchange"))
            pair = self._norm_pair(msg.get("pair_key") or msg.get("symbol") or "")
            if not ex or not pair:
                self._note_drop("missing_orderbook", ex or "UNKNOWN", pair or "UNKNOWN")
                return

            ob = (msg.get("orderbook") or {})
            bids = ob.get("bids") or []
            asks = ob.get("asks") or []
            best_bid = float(msg.get("best_bid") or (bids[0][0] if bids else 0.0))
            best_ask = float(msg.get("best_ask") or (asks[0][0] if asks else 0.0))
            top_bid_vol = float(msg.get("top_bid_vol") or 0.0)
            top_ask_vol = float(msg.get("top_ask_vol") or 0.0)
            has_book = bool(bids and asks)
            best_prices_valid = best_bid > 0 and best_ask > best_bid

            slip_buy_bps: Optional[float] = None

            slip_sell_bps: Optional[float] = None
            slip_metric = msg.get("slip_metric_bps")
            if isinstance(slip_metric, dict):
                if slip_metric.get("buy") is not None:
                    slip_buy_bps = max(0.0, float(slip_metric.get("buy")))
                if slip_metric.get("sell") is not None:
                    slip_sell_bps = max(0.0, float(slip_metric.get("sell")))
            elif isinstance(slip_metric, (int, float)):
                val = max(0.0, float(slip_metric))
                slip_buy_bps = slip_sell_bps = val

            if slip_buy_bps is None or slip_sell_bps is None:
                if not has_book:
                    self._note_drop("depth_insufficient", ex, pair)
                    return
                self.ingest_snapshot(
                    exchange=ex,
                    symbol=pair,
                    orderbook=ob,
                    bid_volume=top_bid_vol,
                    ask_volume=top_ask_vol,
                )
                snap = (self.slippage_data.get(ex, {}) or {}).get(pair, {})
                if slip_buy_bps is None and snap.get("buy") is not None:
                    slip_buy_bps = max(0.0, float(snap.get("buy"))) * 1e4
                if slip_sell_bps is None and snap.get("sell") is not None:
                    slip_sell_bps = max(0.0, float(snap.get("sell"))) * 1e4

            if (slip_buy_bps is None or slip_sell_bps is None) and not best_prices_valid:
                self._note_drop("bad_bid_ask", ex, pair)
                return

            if (slip_buy_bps is None or slip_sell_bps is None) and best_prices_valid:
                approx = (best_ask - best_bid) / max(best_bid, 1e-12) * 1e4
                if slip_buy_bps is None:
                    slip_buy_bps = approx
                if slip_sell_bps is None:
                    slip_sell_bps = approx

            if slip_buy_bps is None and slip_sell_bps is None:
                self._note_drop("no_slippage", ex, pair)
                logger.debug("[Slip] missing data for %s/%s", ex, pair)
                return

            self._slip_bps = getattr(self, "_slip_bps", {})
            self._slip_ts = getattr(self, "_slip_ts", {})
            self._slip_ts_mono = getattr(self, "_slip_ts_mono", {})
            ts_ms = (
                    msg.get("recv_ts_ms")
                    or msg.get("exchange_ts_ms")
                    or msg.get("ts_ms")
                    or msg.get("ts_ex_ms")
                    or 0
            )
            now_wall_s = (float(ts_ms) / 1000.0) if ts_ms else time.time()
            now_mono_s = time.monotonic()
            if slip_buy_bps is not None:
                self._slip_bps[(ex, pair, "buy")] = float(slip_buy_bps)
            if slip_sell_bps is not None:
                self._slip_bps[(ex, pair, "sell")] = float(slip_sell_bps)
            self._slip_ts[(ex, pair)] = now_wall_s
            self._slip_ts_mono[(ex, pair)] = now_mono_s

            try:
                event_age_s = max(0.0, now_wall_s - float(ts_ms) / 1000.0) if ts_ms else 0.0
                if slip_buy_bps is not None:
                    set_slip_age_seconds(pair, ex, "buy", event_age_s)
                if slip_sell_bps is not None:
                   set_slip_age_seconds(pair, ex, "sell", event_age_s)
            except Exception:
                pass

        except Exception:
            try:
                self._note_drop("on_slip_error", msg.get("exchange", "UNKNOWN"), msg.get("pair_key", "UNKNOWN"))
            except Exception:
                pass

            try:
                self._note_drop("parse_error", msg.get("exchange", "UNKNOWN"), msg.get("pair_key", "UNKNOWN"))
            except Exception:
                pass

    # slippage_handler.py
    def get_slippage_bps(
            self,
            exchange: str,
            pair_key: str,
            side: str | None = None,
            *,
            ttl_s: float | None = None,
    ) -> float | None:
        """
        Retourne le slippage estimé (en bps) si frais (TTL), sinon None.
        Unité interne stockée en fraction; conversion bps effectuée à l'ingestion.

        TTL:
          - si `ttl_s` est fourni à l'appel, il peut durcir le TTL,
          - le contrat SLO public (cfg.slo[deployment_mode][exchange].public.slip_ttl_s)
            reste un plafond : effective_ttl = min(base, slo) si slo > 0,
          - base = ttl_s explicite sinon self._ttl_s (cfg.slip.ttl_s, donc pilotée par BotConfig).

        side: "buy" | "sell" | None (None => max des deux si disponibles).
        Drop reason stable si dépassement de plafond: "max_bps_exceeded".
        """
        ex = (exchange or "").upper()
        pk = (pair_key or "").replace("-", "").upper()
        bps = getattr(self, "_slip_bps", {})
        tsd = getattr(self, "_slip_ts", {})
        quote = self._infer_quote(pk)

        now = time.time()

        # --- Résolution du TTL (SLO public → fallback cfg.slip.ttl_s) ------------
        ttl = self._resolve_ttl_s(ex, ttl_s)

        # --- Application du TTL sur les snapshots -------------------------------
        mode_upper = str(getattr(getattr(self, "cfg", None), "MODE", "PROD")).upper()
        is_dry = (mode_upper == "DRY_RUN")

        if side is None:
            vals: list[float] = []
            for s in ("buy", "sell"):
                v = bps.get((ex, pk, s))
                t = tsd.get((ex, pk))
                if v is not None and t is not None:
                    age = now - float(t)
                    if age <= ttl:
                        vals.append(float(v))
                    else:
                        self._note_drop("ttl_expired", ex, pk)
            
            if not vals and is_dry:
                # P0: Fallback simulateur pour ne pas bloquer les opportunités au démarrage
                return 1.0 # 1 bps par défaut
                
            if not vals:
                return None
            slip = max(vals)
            if quote:
                max_allowed = self.max_bps_allowed(quote)
                if slip > max_allowed:
                    self._note_drop("max_bps_exceeded", ex, pk)
                    return None
            return slip

        v = bps.get((ex, pk, str(side).lower()))
        t = tsd.get((ex, pk))
        
        if (v is None or t is None):
            if is_dry:
                return 1.0
            return None

        age = now - float(t)
        # (On ne met pas set_slip_age_seconds ici pour ne pas surcharger de calls ; on reste P0.)
        if age <= ttl:
            slip = float(v)
            if quote:
                max_allowed = self.max_bps_allowed(quote)
                if slip > max_allowed:
                    self._note_drop("max_bps_exceeded", ex, pk)
                    return None
            return slip
        
        if is_dry:
            return 1.0
            
        self._note_drop("ttl_expired", ex, pk)
        return None

    def get_recent_slippage_bps(
            self,
            exchange: str,
            pair_key: str,
            side: str | None = None,
            *,
            ttl_s: float | None = None,
            strategy: str | None = None,
    ) -> float | None:
        _ = strategy  # stratégie non requise pour la lecture TTL actuelle
        return self.get_slippage_bps(exchange, pair_key, side, ttl_s=ttl_s)

    async def detach_bus_consumers(self) -> None:
        tasks = list(self._bus_tasks.values())
        for t in tasks:
            try:
                t.cancel()
            except Exception:
                pass
        if tasks:
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception:
                pass
        self._bus_tasks.clear()
