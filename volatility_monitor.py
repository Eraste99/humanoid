# -*- coding: utf-8 -*-
"""
VolatilityMonitor — standardisé (relative), robuste & aligné Router — **BUS per‑CEX vol only**

Objectif
--------
Mesurer la volatilité **relative** (coefficient de variation) des prix mid et du
spread L1 par paire à partir des *snapshots normalisés* du **MarketDataRouter**.
Cette révision ajoute un consumer BUS pour ne consommer **que** les 3 routes `cex:*.vol`.

Nouveautés (bus):
- `on_vol(msg)` accepte les payloads `cex:EX.vol` (cf. Router patch B incl. best_bid/best_ask/mid)
- `attach_bus_vol_queues({"BINANCE": q1, "COINBASE": q2, "BYBIT": q3})` démarre les 3 consumers
- `detach_bus_consumers()` pour arrêt propre

API historique (pull/update_from_orderbook) conservée.
"""
from __future__ import annotations
from collections import deque, defaultdict
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, List, Optional
import logging
import math
import time
import asyncio
import numpy as np
from modules.rm_compat import getattr_int, getattr_float, getattr_str, getattr_bool, getattr_dict, getattr_list

logger = logging.getLogger("VolatilityMonitor")
# Observabilité (fallback no-op si obs_metrics absent)
try:
    from modules.obs_metrics import (
        VOL_PRICE_VOL_MICRO, VOL_SPREAD_VOL_MICRO,
        VOL_PRICE_PCTL, VOL_SPREAD_PCTL,
        VOL_ANOMALY_TOTAL, VOL_SIGNAL_STATE,
    )
except Exception:  # pragma: no cover
    class _Noop:
        def labels(self, *a, **k): return self
        def observe(self, *a, **k): return
        def inc(self, *a, **k): return
        def set(self, *a, **k): return
    VOL_PRICE_VOL_MICRO = VOL_SPREAD_VOL_MICRO = _Noop()
    VOL_PRICE_PCTL = VOL_SPREAD_PCTL = _Noop()
    VOL_ANOMALY_TOTAL = VOL_SIGNAL_STATE = _Noop()


def _to_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def _norm_pair(s: str) -> str:
    return (s or "").replace("-", "").upper()


def _norm_ex(s: str) -> str:
    return (s or "").upper()


class VolatilityMonitor:
    def __init__(
        self,
        cfg,
        *,
        window_long_minutes: int = 60,
        window_micro_minutes: int = 2,
        historical_days: int = 2,
        maxlen_points: int = 20000,
        winsor_lo_pct: float = 1.0,
        winsor_hi_pct: float = 99.0,
        log_throttle_secs: float = 5.0,
    ) -> None:
        """
        Monitor de volatilité piloté par cfg.vol.* + paramètres runtime.
        Priorité: arguments explicites > cfg.vol > defaults.
        """
        from collections import defaultdict, deque
        from datetime import timedelta, datetime
        import asyncio
        from typing import Optional, Dict

        self.cfg = cfg
        v = self.cfg.vol

        # --- TTL & cœur vol depuis cfg ---
        self._ttl_s          = getattr_int(v, "ttl_s", 5)
        self._ema_alpha      = getattr_float(v, "ema_alpha", 0.20)
        self._soft_cap_bps   = getattr_float(v, "soft_cap_bps", 80.0)
        self._chaos_cap_bps  = getattr_float(v, "chaos_cap_bps", 150.0)
        self._hysteresis     = getattr_float(v, "hysteresis", 0.25)

        # --- Fenêtres: si l'appelant garde les defaults (60 / 2), on prend cfg.vol.window_*_m s'ils existent
        _DEF_LONG = 60
        _DEF_MICR = 2
        w_long_m  = getattr_int(v, "window_long_m", window_long_minutes if window_long_minutes != _DEF_LONG else _DEF_LONG)
        w_micro_m = getattr_int(v, "window_micro_m", window_micro_minutes if window_micro_minutes != _DEF_MICR else _DEF_MICR)
        # si l'appelant a laissé les defaults et que cfg fournit une valeur, on l'utilise
        if window_long_minutes == _DEF_LONG and hasattr(v, "window_long_m"):
            w_long_m = int(v.window_long_m)
        else:
            w_long_m = int(window_long_minutes)
        if window_micro_minutes == _DEF_MICR and hasattr(v, "window_micro_m"):
            w_micro_m = int(v.window_micro_m)
        else:
            w_micro_m = int(window_micro_minutes)

        self.window_long  = timedelta(minutes=w_long_m)
        self.window_micro = timedelta(minutes=w_micro_m)

        # --- Winsorisation: si l'appelant garde 1%/99%, on dérive des cfg.vol.winsor_pct (ex: 0.01 => 1/99)
        _DEF_WLO, _DEF_WHI = 1.0, 99.0
        if winsor_lo_pct == _DEF_WLO and winsor_hi_pct == _DEF_WHI and hasattr(v, "winsor_pct"):
            w = float(v.winsor_pct)  # fraction (ex: 0.01)
            self._wlo = 100.0 * w
            self._whi = 100.0 - 100.0 * w
        else:
            self._wlo = float(winsor_lo_pct)
            self._whi = float(winsor_hi_pct)

        # --- Fenêtre historique & logs
        self.historical_window  = timedelta(days=int(historical_days))
        self._log_throttle      = float(log_throttle_secs)

        def _hist_deque() -> deque:
            return deque(maxlen=int(maxlen_points))

        self.price_history: Dict[Tuple[str, str], deque] = defaultdict(_hist_deque)
        self.spread_history: Dict[Tuple[str, str], deque] = defaultdict(_hist_deque)
        self.historical_price_vols: Dict[Tuple[str, str], deque] = defaultdict(_hist_deque)
        self.historical_spread_vols: Dict[Tuple[str, str], deque] = defaultdict(_hist_deque)

        # --- Seuils init & statut signal
        self.signal_status: Dict[str, str] = defaultdict(lambda: "normal")
        self.thresholds: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {"price_vol": 0.05, "spread_vol": 0.10}
        )

        # --- État runtime
        self.last_update: Optional[datetime] = None
        self.volatility_index: float = 0.0
        self.update_count: int = 0
        self._last_log_ts: Dict[str, float] = {}

        # --- BUS/integ
        self._bus_tasks: Dict[str, asyncio.Task] = {}
        self._scanner = None


    # API simple: TTL
    def ttl_seconds(self) -> int:
        return self._ttl_s

    def _pair_percentiles(self, pair: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        """Retourne (price_p95, price_p99, spread_p95, spread_p99) sur historiques micro-window."""
        pair = _norm_pair(pair)
        price_vols: List[float] = []
        spread_vols: List[float] = []
        for dq in self._series_for_pair(self.historical_price_vols, pair):
            price_vols.extend(v for _, v in dq)
        for dq in self._series_for_pair(self.historical_spread_vols, pair):
            spread_vols.extend(v for _, v in dq)

        if len(price_vols) >= 50:
            price_p95 = float(np.percentile(price_vols, 95))
            price_p99 = float(np.percentile(price_vols, 99))
        else:
            price_p95 = price_p99 = None
        if len(spread_vols) >= 50:
            spread_p95 = float(np.percentile(spread_vols, 95))
            spread_p99 = float(np.percentile(spread_vols, 99))
        else:
            spread_p95 = spread_p99 = None
        return price_p95, price_p99, spread_p95, spread_p99

    # --- Wiring optionnel vers le Scanner (pour forward bps) ---
    def set_scanner(self, scanner) -> None:
        """Brancher le Scanner pour forward ema_vol_bps -> scanner.ingest_volatility_bps({pair:bps})."""
        self._scanner = scanner

    def _forward_to_scanner(self, exchange: str, pair: str, vol_bps: float) -> None:
        scanner = getattr(self, "_scanner", None)
        if not scanner or not hasattr(scanner, "ingest_volatility_bps"):
            return
        try:
            scanner.ingest_volatility_bps(pair, exchange, float(vol_bps), ts=time.time())
        except Exception:
            logger.debug("[VolatilityMonitor] scanner forward failed", exc_info=True)


    # ---- Profils de liquidité (optionnel) ----
    def set_liquidity_profile(self, pair_key: str, level: str) -> None:
        level = (level or "").lower()
        if level == "high":
            self.thresholds[_norm_pair(pair_key)] = {"price_vol": 0.03, "spread_vol": 0.05}
        elif level == "medium":
            self.thresholds[_norm_pair(pair_key)] = {"price_vol": 0.05, "spread_vol": 0.10}
        else:
            self.thresholds[_norm_pair(pair_key)] = {"price_vol": 0.08, "spread_vol": 0.15}

    # ---- Mise à jour depuis snapshot Router ----
    def ingest_snapshot(self, data: Dict[str, Any]) -> None:
        self.update_from_orderbook(data)

    def update_from_orderbook(self, data: Dict[str, Any]) -> None:
        if not data or not data.get("active", True):
            return
        pair = _norm_pair(data.get("pair_key") or data.get("symbol") or "")
        if not pair:
            return
        exchange = _norm_ex(data.get("exchange") or data.get("venue"))
        if not exchange:
            return

        bid = _to_float(data.get("best_bid"))
        ask = _to_float(data.get("best_ask"))


        if bid <= 0 or ask <= 0 or bid >= ask:
            return

        ts_ms = data.get("exchange_ts_ms") or data.get("recv_ts_ms")
        try:
            ts = float(ts_ms) / 1000.0 if ts_ms is not None else None
        except Exception:
            ts = None
        timestamp = datetime.utcfromtimestamp(ts) if ts else datetime.utcnow()
        self.last_update = timestamp

        mid = (bid + ask) / 2.0
        spread = ask - bid

        key = (exchange, pair)
        self.price_history[key].append((timestamp, mid))
        self.spread_history[key].append((timestamp, spread))

        self._cleanup_old_data(exchange, pair, timestamp)
        self._evaluate_prudence(pair, exchange=exchange, now=timestamp)

        self.update_count += 1
        try:
            # Observe les valeurs micro courantes (post _evaluate_prudence qui remplit l'historique)
            now_dt = timestamp
            pair_norm = pair
            pv = self.get_price_volatility(pair_norm, window="micro", now=now_dt)
            sv = self.get_spread_volatility(pair_norm, window="micro", now=now_dt)
            VOL_PRICE_VOL_MICRO.labels(pair=pair_norm).observe(float(pv))
            VOL_SPREAD_VOL_MICRO.labels(pair=pair_norm).observe(float(sv))
        except Exception:
            pass

    # ---- Maintenance fenêtres ----
    def _cleanup_old_data(self, exchange: str, pair: str, now: datetime) -> None:
        key = (_norm_ex(exchange), _norm_pair(pair))
        dq = self.price_history.get(key)
        if dq is not None:
            self.price_history[key] = deque(
                ((t, p) for t, p in dq if now - t <= self.window_long),
                maxlen=dq.maxlen,
            )
        dq = self.spread_history.get(key)
        if dq is not None:
            self.spread_history[key] = deque(
                ((t, s) for t, s in dq if now - t <= self.window_long),
                maxlen=dq.maxlen,
            )
        dq = self.historical_price_vols.get(key)
        if dq is not None:
            self.historical_price_vols[key] = deque(
                ((t, v) for t, v in dq if now - t <= self.historical_window),
                maxlen=dq.maxlen,
            )
        dq = self.historical_spread_vols.get(key)
        if dq is not None:
            self.historical_spread_vols[key] = deque(
                ((t, v) for t, v in dq if now - t <= self.historical_window),
                maxlen=dq.maxlen,
            )

    # ---- Utils vol ----
    def _clean_values(self, values: List[float]) -> np.ndarray:
        if not values:
            return np.array([], dtype=float)
        arr = np.asarray(values, dtype=float)
        arr = arr[np.isfinite(arr)]
        if arr.size == 0:
            return arr
        lo, hi = np.percentile(arr, self._wlo), np.percentile(arr, self._whi)
        if math.isfinite(lo) and math.isfinite(hi) and hi > lo:
            arr = np.clip(arr, lo, hi)
        return arr

    def _relative_volatility(self, values: List[float]) -> float:
        arr = self._clean_values(values)
        if arr.size < 2:
            return 0.0
        m = float(np.mean(arr))
        if m <= 0:
            return 0.0
        sd = float(np.std(arr))
        return sd / m

    def _series_for_pair(
        self,
        store: Dict[Tuple[str, str], deque],
        pair: str,
        exchange: Optional[str] = None,
    ) -> List[deque]:
        pk = _norm_pair(pair)
        if exchange:
            key = (_norm_ex(exchange), pk)
            dq = store.get(key)
            return [dq] if dq else []
        return [dq for (ex_key, pk_key), dq in store.items() if pk_key == pk]

    def _window_values(self, series_collection, duration: timedelta, now: Optional[datetime] = None) -> List[float]:
        now = now or datetime.utcnow()
        if isinstance(series_collection, deque):
            collections = [series_collection]
        else:
            collections = [dq for dq in (series_collection or []) if dq]
        values: List[float] = []
        for series in collections:
            values.extend(val for (t, val) in series if now - t <= duration)
        return values

    def get_price_volatility(
        self,
        pair: str,
        window: str = "long",
        now: Optional[datetime] = None,
        exchange: Optional[str] = None,
    ) -> float:
        pair = _norm_pair(pair)
        duration = self.window_long if window == "long" else self.window_micro
        series = self._series_for_pair(self.price_history, pair, exchange)
        return self._relative_volatility(self._window_values(series, duration, now))


    def get_volatility(self, exchange: str, pair_key: str) -> Optional[float]:
        """
        P1/P2: retourne la volatilité *relative* si mesure fraîche (TTL), sinon None.
        (Le mapping vers bps sera ajouté après P2, comme demandé.)
        """
        ex = str(exchange).upper()
        pk = str(pair_key).upper()
        rec = getattr(self, "_last_vol", {}).get((ex, pk))
        if not rec:
            return None

        age_s = max(0.0, time.time() - float(rec.get("ts_recv_s", 0.0)))
        ttl = 5.0
        if hasattr(self, "bot_cfg"):
            ttl = float(self.bot_cfg.vol.ttl_s)
        if age_s > ttl:
            return None

        vol_rel = rec.get("vol_rel")
        return float(vol_rel) if vol_rel is not None else None

    def get_spread_volatility(
        self,
        pair: str,
        window: str = "long",
        now: Optional[datetime] = None,
        exchange: Optional[str] = None,
    ) -> float:
        pair = _norm_pair(pair)
        duration = self.window_long if window == "long" else self.window_micro
        series = self._series_for_pair(self.spread_history, pair, exchange)
        return self._relative_volatility(self._window_values(series, duration, now))

    # ---- Seuils dynamiques ----
    def _get_dynamic_thresholds(self, pair: str) -> Tuple[float, float]:
        pair = _norm_pair(pair)
        price_vols: List[float] = []
        spread_vols: List[float] = []
        for dq in self._series_for_pair(self.historical_price_vols, pair):
            price_vols.extend(v for _, v in dq)
        for dq in self._series_for_pair(self.historical_spread_vols, pair):
            spread_vols.extend(v for _, v in dq)

        if len(price_vols) >= 100 and len(spread_vols) >= 100:
            price_thresh = float(np.percentile(price_vols, 95))
            spread_thresh = float(np.percentile(spread_vols, 95))
        else:
            price_thresh = float(self.thresholds[pair]["price_vol"])  # profil par défaut
            spread_thresh = float(self.thresholds[pair]["spread_vol"])
        return price_thresh, spread_thresh

    def _should_log(self, pair: str) -> bool:
        now = datetime.utcnow().timestamp()
        last = self._last_log_ts.get(pair, 0.0)
        if (now - last) >= self._log_throttle:
            self._last_log_ts[pair] = now
            return True
        return False

    def _evaluate_prudence(
        self,
        pair: str,
        *,
        exchange: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> None:
        pair = _norm_pair(pair)
        now = now or datetime.utcnow()
        price_vol_micro = self.get_price_volatility(pair, window="micro", now=now, exchange=exchange)
        spread_vol_micro = self.get_spread_volatility(pair, window="micro", now=now, exchange=exchange)

        # Alimente l'historique micro
        if exchange:
            key = (_norm_ex(exchange), pair)
            self.historical_price_vols[key].append((now, price_vol_micro))
            self.historical_spread_vols[key].append((now, spread_vol_micro))

        # Seuils dynamiques (profil par défaut si historique insuffisant)
        price_thr, spread_thr = self._get_dynamic_thresholds(pair)

        # --- Publication périodique p95/p99 (throttle commun au logging) ---
        if self._should_log(pair):
            try:
                p95p, p99p, p95s, p99s = self._pair_percentiles(pair)
                if p95p is not None:
                    VOL_PRICE_PCTL.labels(pair=pair, pct="p95").set(float(p95p))
                if p99p is not None:
                    VOL_PRICE_PCTL.labels(pair=pair, pct="p99").set(float(p99p))
                if p95s is not None:
                    VOL_SPREAD_PCTL.labels(pair=pair, pct="p95").set(float(p95s))
                if p99s is not None:
                    VOL_SPREAD_PCTL.labels(pair=pair, pct="p99").set(float(p99s))
            except Exception:
                pass

        # --- Détection d’anomalies (franchissements) ---
        try:
            # moderate: > p95 (ou seuil dynamique) ; high: > p99 si dispo (sinon > 2x p95)
            p95p, p99p, p95s, p99s = self._pair_percentiles(pair)
            # PRICE
            if p95p is not None and price_vol_micro > p95p:
                sev = "high" if (
                                            p99p is not None and price_vol_micro > p99p) or price_vol_micro > 2.0 * p95p else "moderate"
                VOL_ANOMALY_TOTAL.labels(pair=pair, metric="price", severity=sev).inc()
            # SPREAD
            if p95s is not None and spread_vol_micro > p95s:
                sev = "high" if (
                                            p99s is not None and spread_vol_micro > p99s) or spread_vol_micro > 2.0 * p95s else "moderate"
                VOL_ANOMALY_TOTAL.labels(pair=pair, metric="spread", severity=sev).inc()
        except Exception:
            pass

        # --- Mise à jour du statut + gauge état ---
        prev = self.signal_status.get(pair, "normal")
        if price_vol_micro > price_thr or spread_vol_micro > spread_thr:
            new_state = "élevé" if (price_vol_micro > 2 * price_thr or spread_vol_micro > 2 * spread_thr) else "modéré"
        else:
            new_state = "normal"
        self.signal_status[pair] = new_state

        try:
            VOL_SIGNAL_STATE.labels(pair=pair).set({"normal": 0, "modéré": 1, "élevé": 2}[new_state])
        except Exception:
            pass

        # Log (inchangé)
        if self._should_log(pair):
            logger.info(
                f"[{pair}] thr: price_vol={price_thr:.4f}, spread_vol={spread_thr:.4f} | "
                f"micro={price_vol_micro:.4f}/{spread_vol_micro:.4f}"
            )

    # ---- API publique ----
    def get_prudence_signal(self, pair: str) -> str:
        return self.signal_status[_norm_pair(pair)]


    def get_current_thresholds(self, pair: str) -> Dict[str, float]:
        pair = _norm_pair(pair)
        price_thr, spread_thr = self._get_dynamic_thresholds(pair)
        return {"price_vol_threshold": price_thr, "spread_vol_threshold": spread_thr}

    def get_metrics_bulk(self, pairs: List[str]) -> Dict[str, Dict[str, Any]]:
        now = datetime.utcnow()
        out: Dict[str, Dict[str, Any]] = {}
        for p in pairs:
            pp = _norm_pair(p)
            out[pp] = {
                "price_volatility_long": self.get_price_volatility(pp, "long", now),
                "price_volatility_micro": self.get_price_volatility(pp, "micro", now),
                "spread_volatility_long": self.get_spread_volatility(pp, "long", now),
                "spread_volatility_micro": self.get_spread_volatility(pp, "micro", now),
                "prudence_signal": self.get_prudence_signal(pp),
            }
        return out

    def get_signal_history(self) -> Dict[str, str]:
        return dict(self.signal_status)

    def reset(self) -> None:
        self.price_history.clear()
        self.spread_history.clear()
        self.signal_status.clear()
        self.historical_price_vols.clear()
        self.historical_spread_vols.clear()
        self.update_count = 0
        self.last_update = None

    def get_status(self) -> Dict[str, Any]:
        vols: List[float] = []
        now_dt = datetime.utcnow()
        seen_pairs = {pk for (_, pk) in self.price_history.keys()}
        for pair in seen_pairs:
            v = self.get_price_volatility(pair, window="micro", now=now_dt)
            if v > 0:
                vols.append(v)
        self.volatility_index = float(np.mean(vols)) if vols else 0.0

        last_ts = self.last_update.timestamp() if self.last_update else None
        age_s = (time.time() - last_ts) if last_ts else None

        return {
            "module": "VolatilityMonitor",
            "healthy": True,
            "last_update": last_ts,
            "age_s": age_s,
            "details": "Volatilité surveillée (relative)",
            "updates": self.update_count,
            "update_count": self.update_count,
            "metrics": {"volatility_index": round(self.volatility_index, 6)},
            "submodules": {},
        }

    def stop_monitoring(self) -> None:
        logger.info("🛑 VolatilityMonitor stoppé.")

    # ----------------------- BUS (per‑CEX vol) -----------------------
    # volatility_monitor.py
    def set_bps_mapping(self, *, midprice_to_bps: float = 1e4, floor_bps: float = 0.0, cap_bps: float = 250.0) -> None:
        """
        Définit un mapping simple de la vol relative -> micro-vol en bps pour le Scanner.
          - midprice_to_bps: multiplicateur pour convertir une fraction en bps
        """
        self._vol_map = {"k": float(midprice_to_bps), "floor": float(floor_bps), "cap": float(cap_bps)}

    def _to_bps(self, rel_vol: float) -> float:
        m = getattr(self, "_vol_map", {"k": 1e4, "floor": 0.0, "cap": 250.0})
        x = float(rel_vol) * m["k"]
        return max(m["floor"], min(m["cap"], x))

    def on_vol(self, msg: dict) -> None:
        """
        Consomme `cex:EX.vol` ; bid/ask > 0 ; passe par le pipeline interne,
        puis alimente `_last_vol[(EX, PK)] = {"vol_rel": v, "ts_recv_s": ...}` pour le TTL.
        """
        try:
            ex = str(msg.get("exchange") or "").upper()
            pk = str(msg.get("pair_key") or "").upper()
            bid = float(msg.get("best_bid") or 0.0)
            ask = float(msg.get("best_ask") or 0.0)
            if not ex or not pk or bid <= 0 or ask <= 0 or bid >= ask:
                return

            data = {
                "active": bool(msg.get("active", True)),
                "pair_key": pk,
                "best_bid": bid,
                "best_ask": ask,
                "exchange_ts_ms": int(msg.get("exchange_ts_ms") or 0),
                "recv_ts_ms": int(msg.get("recv_ts_ms") or 0),
            }

            # Ingestion dans l'historique interne (signature à 1 seul param "data")
            self.ingest_snapshot(data)

            # Volatilité relative micro (déjà disponible via l’historique interne)
            vol_rel = float(self.get_price_volatility(pk, window="micro", exchange=ex))
            vol_bps = self._to_bps(vol_rel)

            # TTL cache consommé par get_volatility()
            ts_ms = msg.get("recv_ts_ms") or msg.get("exchange_ts_ms")
            ts_recv_s = (float(ts_ms) / 1000.0) if ts_ms else time.time()
            if not hasattr(self, "_last_vol"):
                self._last_vol = {}
            self._last_vol[(ex, pk)] = {"vol_rel": vol_rel, "vol_bps": vol_bps, "ts_recv_s": ts_recv_s}
            self._forward_to_scanner(ex, pk, vol_bps)

        except Exception:
            logging.exception("VolatilityMonitor.on_vol error")

    def attach_bus_vol_queues(self, queues_by_ex: dict[str, asyncio.Queue]) -> None:
        """Démarre un consumer par CEX pour le bus 'vol'."""

        async def _consume(ex: str, q: asyncio.Queue):
            while True:
                msg = await q.get()
                try:
                    self.on_vol(msg)
                except Exception:
                    logging.exception("Unhandled exception in vol consumer")
                finally:
                    try:
                        q.task_done()
                    except Exception:
                        pass

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
            t = asyncio.create_task(_consume(str(ex).upper(), q), name=f"vol-{ex}")
            self._bus_tasks[str(ex).upper()] = t
        logger.info("[VolatilityMonitor] vol consumers: %s", list(self._bus_tasks.keys()))

    def detach_bus_consumers(self) -> None:
        for ex, t in list(self._bus_tasks.items()):
            t.cancel()
        self._bus_tasks.clear()
