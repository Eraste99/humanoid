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
from contracts.payloads import VolEvent, _norm_exchange, _norm_pair_key
import numpy as np

logger = logging.getLogger("VolatilityMonitor")
# Observabilité (fallback no-op si obs_metrics absent)
try:
    from modules.obs_metrics import (
        VOL_PRICE_VOL_MICRO,
        VOL_SPREAD_VOL_MICRO,
        VOL_PRICE_PCTL,
        VOL_SPREAD_PCTL,
        VOL_ANOMALY_TOTAL,
        VOL_SIGNAL_STATE,
        set_vol_age_seconds,
        note_vol_ttl_seconds,
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

    VOL_PRICE_VOL_MICRO = _Noop()
    VOL_SPREAD_VOL_MICRO = _Noop()
    VOL_PRICE_PCTL = _Noop()
    VOL_SPREAD_PCTL = _Noop()
    VOL_ANOMALY_TOTAL = _Noop()
    VOL_SIGNAL_STATE = _Noop()


    def set_vol_age_seconds(*_, **__):
        return None


    def note_vol_ttl_seconds(*_, **__):
        return None

    def inc_blocked(*_, **__):
        return None

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
        self._missing_slo_warned: set[tuple[str, str]] = set()
        self._slo_resolution_warned = False

        self._ttl_s = int(getattr(v, "ttl_s", 5))
        self._ema_alpha = float(getattr(v, "ema_alpha", 0.20))
        self._soft_cap_bps = float(getattr(v, "soft_cap_bps", 80.0))
        self._chaos_cap_bps = float(getattr(v, "chaos_cap_bps", 150.0))
        self._hysteresis = float(getattr(v, "hysteresis", 0.25))
        self._vol_min_delta_bps = float(getattr(v, "vol_min_delta_bps", 0.5))
        self._max_silence_s = float(getattr(v, "max_silence_s", 2.0))
        self._last_forward: Dict[Tuple[str, str], Dict[str, float]] = {}
        # --- Cross-exchange divergence (corr + dislocation) --------------------
        # Par défaut: OFF (zéro impact perf/comportement).
        self._xex_dislocation_enabled = bool(getattr(v, "xex_dislocation_enabled", False))
        self._xex_corr_enabled = bool(getattr(v, "xex_corr_enabled", False))
        self._xex_prudence_enabled = bool(getattr(v, "xex_prudence_enabled", False))

        # Routes autorisées (source "mécanique" déjà dans cfg.g.allowed_routes)
        g = getattr(self.cfg, "g", None)
        routes = list(getattr(g, "allowed_routes", [])) if g else []
        neighbors: Dict[str, set[str]] = {}
        undirected: set[tuple[str, str]] = set()
        for r in routes:
            try:
                a, b = r
                a = _norm_ex(a); b = _norm_ex(b)
                if a and b and a != b:
                    undirected.add(tuple(sorted((a, b))))
                    neighbors.setdefault(a, set()).add(b)
                    neighbors.setdefault(b, set()).add(a)
            except Exception:
                continue
        self._xex_routes = undirected
        self._xex_neighbors = neighbors

        # TTL/cadence/paramètres corr
        self._xex_ttl_s = float(getattr(v, "xex_ttl_s", float(self._ttl_s)))
        self._xex_step_ms = int(getattr(v, "xex_step_ms", 200))  # resample grid (ms)
        self._xex_min_points = int(getattr(v, "xex_min_points", 60))  # min nb returns
        self._xex_compute_min_interval_s = float(getattr(v, "xex_compute_min_interval_s", 0.35))
        self._xex_corr_gate_vol_bps = float(getattr(v, "xex_corr_gate_vol_bps", float(self._soft_cap_bps)))

        # Par défaut: corr calculée seulement si liste explicitement fournie (sinon 0 CPU).
        self._xex_corr_pairs = set(_norm_pair(p) for p in list(getattr(v, "xex_corr_pairs", [])))

        # Seuils divergence (defaults overridables)
        self._xex_corr_warn = float(getattr(v, "xex_corr_warn", 0.97))
        self._xex_corr_crit = float(getattr(v, "xex_corr_crit", 0.92))
        self._xex_disloc_warn_bps = float(getattr(v, "xex_disloc_warn_bps", 6.0))
        self._xex_disloc_crit_bps = float(getattr(v, "xex_disloc_crit_bps", 12.0))

        # Cache: (pair, ex1, ex2) -> dict(disloc_bps, corr, ts_disloc_s, ts_corr_s, ...)
        self._xex_cache: Dict[tuple[str, str, str], Dict[str, float]] = {}


        try:
            note_vol_ttl_seconds(self._ttl_s)
        except Exception:
            pass

        # --- Fenêtres: si l'appelant garde les defaults (60 / 2), on prend cfg.vol.window_*_m s'ils existent
        _DEF_LONG = 60
        _DEF_MICR = 2
        w_long_m  = int(getattr(v, "window_long_m", window_long_minutes if window_long_minutes != _DEF_LONG else _DEF_LONG))
        w_micro_m = int(getattr(v, "window_micro_m", window_micro_minutes if window_micro_minutes != _DEF_MICR else _DEF_MICR))
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

        # Optimisation P0: Index pour éviter le scan global O(N_pairs)
        self._pair_to_keys: Dict[str, set[Tuple[str, str]]] = defaultdict(set)

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
        self._last_calc_ms = getattr(self, "_last_calc_ms", {})

        mid = (bid + ask) / 2.0
        spread = ask - bid

        key = (exchange, pair)
        if key not in self.price_history:
            self._pair_to_keys[pair].add(key)
        
        self.price_history[key].append((timestamp, mid))
        self.spread_history[key].append((timestamp, spread))

        self._cleanup_old_data(exchange, pair, timestamp)
        self._evaluate_prudence(pair, exchange=exchange, now=timestamp)

        self.update_count += 1
        
        # P1: Throttle des calculs de volatilité micro (HFT Optimization)
        # On ne recalcule la vol micro que toutes les 200ms par paire pour économiser du CPU.
        now_ms = int(time.time() * 1000)
        calc_key = (exchange, pair, "micro")
        if now_ms - self._last_calc_ms.get(calc_key, 0) < 200:
            return

        self._last_calc_ms[calc_key] = now_ms

        try:
            # Observe les valeurs micro courantes (post _evaluate_prudence qui remplit l'historique)
            now_dt = timestamp
            pair_norm = pair
            pv = self.get_price_volatility(pair_norm, window="micro", now=now_dt, exchange=exchange)
            sv = self.get_spread_volatility(pair_norm, window="micro", now=now_dt, exchange=exchange)
            VOL_PRICE_VOL_MICRO.labels(pair=pair_norm).observe(float(pv))
            VOL_SPREAD_VOL_MICRO.labels(pair=pair_norm).observe(float(sv))
        except Exception:
            pass

    # ---- Maintenance fenêtres ----
    def _cleanup_old_data(self, exchange: str, pair: str, now: datetime) -> None:
        ex_norm = _norm_ex(exchange)
        pk_norm = _norm_pair(pair)
        key = (ex_norm, pk_norm)
        
        dq = self.price_history.get(key)
        if dq is not None:
            while dq and now - dq[0][0] > self.window_long:
                dq.popleft()
            if not dq:
                self.price_history.pop(key, None)
                if pk_norm in self._pair_to_keys:
                    self._pair_to_keys[pk_norm].discard(key)
        
        dq = self.spread_history.get(key)
        if dq is not None:
            while dq and now - dq[0][0] > self.window_long:
                dq.popleft()
            if not dq:
                self.spread_history.pop(key, None)

        dq = self.historical_price_vols.get(key)
        if dq is not None:
            while dq and now - dq[0][0] > self.historical_window:
                dq.popleft()
            if not dq:
                self.historical_price_vols.pop(key, None)

        dq = self.historical_spread_vols.get(key)
        if dq is not None:
            while dq and now - dq[0][0] > self.historical_window:
                dq.popleft()
            if not dq:
                self.historical_spread_vols.pop(key, None)

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
        
        # Optimisation P0: Utilise l'index au lieu de store.items()
        keys = self._pair_to_keys.get(pk, set())
        return [store[k] for k in keys if k in store]

    def _window_values(self, series_collection, duration: timedelta, now: Optional[datetime] = None) -> List[float]:
        now = now or datetime.utcnow()
        if isinstance(series_collection, deque):
            collections = [series_collection]
        else:
            collections = [dq for dq in (series_collection or []) if dq]
        
        values: List[float] = []
        for series in collections:
            # Optimisation P0: Parcours inverse car les deques sont chronologiques.
            # On s'arrête dès qu'on sort de la fenêtre.
            for t, val in reversed(series):
                if now - t <= duration:
                    values.append(val)
                else:
                    break
        return values

    def _strict_config(self) -> bool:
        rm_cfg = getattr(self.cfg, "rm", None)
        vol_cfg = getattr(self.cfg, "vol", None)
        return bool(
            getattr(rm_cfg, "strict_config", False)
            or getattr(vol_cfg, "strict_config", False)
        )

    def _deployment_mode(self) -> str:
        g_cfg = getattr(self.cfg, "g", None)
        mode = getattr(g_cfg, "deployment_mode", None) if g_cfg else None
        mode_norm = str(mode).strip().upper() if mode else ""
        if not mode_norm:
            msg = "deployment_mode missing; using EU_ONLY fallback"
            if self._strict_config():
                raise RuntimeError(msg)
            logger.warning("[VolatilityMonitor] %s", msg)
            mode_norm = "EU_ONLY"
        if mode_norm not in {"EU_ONLY", "SPLIT", "JP_ONLY"}:
            logger.warning("[VolatilityMonitor] unknown deployment_mode=%s; falling back to EU_ONLY", mode_norm)
            mode_norm = "EU_ONLY"
        return mode_norm

    def _resolve_ttl_s(self, exchange: str, ttl_s: Optional[float]) -> float:
        base_ttl = float(ttl_s) if ttl_s is not None else float(getattr(self, "_ttl_s", 5.0))
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
                    vol_ttl_s = float(getattr(path_slo.public, "vol_ttl_s", 0.0) or 0.0)
                    if vol_ttl_s > 0.0:
                        slo_ttl = vol_ttl_s
        except Exception:
            if not self._slo_resolution_warned:
                self._slo_resolution_warned = True
                logger.warning(
                    "[VolatilityMonitor] unable to resolve SLO-based vol TTL; using cfg.vol.ttl_s",
                    exc_info=False,
                )
            return base_ttl

        if slo_ttl is None:
            warn_key = (mode_key, exu)
            if warn_key not in self._missing_slo_warned:
                self._missing_slo_warned.add(warn_key)
                logger.warning(
                    "[VolatilityMonitor] missing SLO vol_ttl_s for mode=%s exchange=%s; using cfg.vol.ttl_s",
                    mode_key,
                    exu,
                )
            return base_ttl

        ttl = min(base_ttl, slo_ttl)
        if ttl_s is not None and ttl_s > ttl:
            logger.debug(
                "[VolatilityMonitor] requested vol TTL clamped by SLO",
                extra={"requested_ttl": base_ttl, "slo_ttl": slo_ttl, "effective_ttl": ttl},
            )
        return ttl
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

    def get_volatility(
            self,
            exchange: str,
            pair_key: str,
            ttl_s: Optional[float] = None,
    ) -> Optional[float]:
        """
        P1/P2: retourne la volatilité *relative* si mesure fraîche (TTL), sinon None.

        TTL effectif :
          - ttl_s explicite peut durcir la fraîcheur,
          - SLO public plafonne toujours : cfg.slo[deployment_mode][exchange].public.vol_ttl_s,
          - sinon fallback legacy : cfg.vol.ttl_s (injecté dans self._ttl_s).
        """
        ex = str(exchange or "").upper()
        pk = _norm_pair(pair_key)
        rec = getattr(self, "_last_vol", {}).get((ex, pk))
        if not rec:
            return None

        # --- Résolution du TTL (SLO public plafonne) -----------------------------
        ttl = self._resolve_ttl_s(ex, ttl_s)
        # --- Application du TTL sur la dernière mesure --------------------------
        age_s = max(0.0, time.time() - float(rec.get("ts_recv_s", 0.0)))
        if age_s > ttl:
            return None

        vol_rel = rec.get("vol_rel")
        try:
            return float(vol_rel) if vol_rel is not None else None
        except Exception:
            return None

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
        """
               Prudence signal: dépend de la vol en bps (micro) et de l'âge de la mesure.
               - si âge > TTL -> "élevé"
               - sinon, mapping basé sur vol_bps (converti depuis la vol relative micro)
               """
        pair = _norm_pair(pair)
        now = now or datetime.utcnow()
        price_vol_micro = self.get_price_volatility(pair, window="micro", now=now, exchange=exchange)
        spread_vol_micro = self.get_spread_volatility(pair, window="micro", now=now, exchange=exchange)
        vol_bps = self._to_bps(max(price_vol_micro, spread_vol_micro))
        # ---- Cross-exchange divergence (optional) ----
        if exchange and self._xex_any_enabled():
            try:
                self._xex_update_for_exchange(exchange=str(exchange), pair=pair, now=now, vol_bps=float(vol_bps))
            except Exception:
                logger.debug("[VolatilityMonitor] xex_update failed", exc_info=False)

        age_s = self.last_age_seconds(exchange or "", pair) if exchange else None


        # Alimente l'historique micro
        if exchange:
            key = (_norm_ex(exchange), pair)
            self.historical_price_vols[key].append((now, price_vol_micro))
            self.historical_spread_vols[key].append((now, spread_vol_micro))

        # Seuils dynamiques (profil par défaut si historique insuffisant)
        price_thr, spread_thr = self._get_dynamic_thresholds(pair)

        do_log = self._should_log(pair)
        
        # --- Publication périodique p95/p99 (throttle commun au logging) ---
        if do_log:
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
        if age_s is not None and age_s > float(self._ttl_s):
            new_state = "élevé"
        elif vol_bps >= float(self._chaos_cap_bps):
            new_state = "élevé"
        elif vol_bps >= float(self._soft_cap_bps):
            new_state = "modéré"
        else:
            new_state = "normal"
        self.signal_status[pair] = new_state

        # ---- Cross-exchange prudence overlay (optional, only up-clamp) ----
        if exchange and getattr(self, "_xex_prudence_enabled", False):
            try:
                ov = self._xex_prudence_overlay(exchange=str(exchange), pair=pair)
                if ov == "élevé":
                    new_state = "élevé"
                elif ov == "modéré" and new_state == "normal":
                    new_state = "modéré"
            except Exception:
                logger.debug("[VolatilityMonitor] xex_overlay failed", exc_info=False)

        try:
            VOL_SIGNAL_STATE.labels(pair=pair).set({"normal": 0, "modéré": 1, "élevé": 2}[new_state])
        except Exception:
            pass

        # Log (inchangé)
        if do_log:
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
        if hasattr(self, "_last_vol"):
            self._last_vol.clear()

    def set_risk_manager(self, rm: Any) -> None:
        self.risk_manager = rm

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

        st: Dict[str, Any] = {
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

    async def stop(self) -> None:
        await self.detach_bus_consumers()
        logger.info("🛑 VolatilityMonitor stoppé.")

    async def stop_monitoring(self) -> None:
        await self.stop()
    def _xex_any_enabled(self) -> bool:
        return bool(
            getattr(self, "_xex_dislocation_enabled", False)
            or getattr(self, "_xex_corr_enabled", False)
            or getattr(self, "_xex_prudence_enabled", False)
        )

    def _xex_key(self, pair: str, ex_a: str, ex_b: str) -> tuple[str, str, str]:
        pk = _norm_pair(pair)
        a = _norm_ex(ex_a); b = _norm_ex(ex_b)
        x, y = (a, b) if a <= b else (b, a)
        return (pk, x, y)

    def _xex_latest_mid(self, exchange: str, pair: str) -> tuple[Optional[float], Optional[datetime]]:
        key = (_norm_ex(exchange), _norm_pair(pair))
        dq = self.price_history.get(key)
        if not dq:
            return None, None
        try:
            t, mid = dq[-1]
            return float(mid), t
        except Exception:
            return None, None

    def _xex_series_window(self, exchange: str, pair: str, now: datetime, duration: timedelta) -> List[tuple[datetime, float]]:
        key = (_norm_ex(exchange), _norm_pair(pair))
        dq = self.price_history.get(key)
        if not dq:
            return []
        cutoff = now - duration
        out: List[tuple[datetime, float]] = []
        # iterate from newest backwards, stop once outside window
        for t, v in reversed(dq):
            if t < cutoff:
                break
            try:
                out.append((t, float(v)))
            except Exception:
                continue
        out.reverse()
        return out

    def _xex_dislocation_bps(self, mid_a: float, mid_b: float) -> float:
        denom = (mid_a + mid_b) / 2.0
        if denom <= 0:
            return 0.0
        return abs(mid_a - mid_b) / denom * 1e4

    def _xex_resample_ffill(self, series: List[tuple[datetime, float]], start: datetime, end: datetime, step_s: float) -> np.ndarray:
        if not series or step_s <= 0:
            return np.array([], dtype=float)
        # series is sorted by time
        i = 0
        last = None
        n = int(max(0.0, (end - start).total_seconds()) / step_s) + 1
        out = np.empty(n, dtype=float)
        out.fill(np.nan)

        # advance pointer up to start
        while i < len(series) and series[i][0] <= start:
            last = series[i][1]
            i += 1

        t = start
        for k in range(n):
            while i < len(series) and series[i][0] <= t:
                last = series[i][1]
                i += 1
            if last is not None:
                out[k] = float(last)
            t = t + timedelta(seconds=step_s)
        return out

    def _xex_corr_log_returns(self, ex_a: str, ex_b: str, pair: str, now: datetime) -> Optional[float]:
        # garde-fou: corr activée seulement si whitelist explicitement fournie
        if not getattr(self, "_xex_corr_pairs", set()):
            return None
        if _norm_pair(pair) not in self._xex_corr_pairs:
            return None

        step_s = max(0.05, float(getattr(self, "_xex_step_ms", 200)) / 1000.0)
        a = self._xex_series_window(ex_a, pair, now, self.window_micro)
        b = self._xex_series_window(ex_b, pair, now, self.window_micro)
        if len(a) < 5 or len(b) < 5:
            return None

        start = now - self.window_micro
        start = max(start, a[0][0], b[0][0])
        end = now
        pa = self._xex_resample_ffill(a, start, end, step_s)
        pb = self._xex_resample_ffill(b, start, end, step_s)
        if pa.size < 10 or pb.size < 10:
            return None

        mask = np.isfinite(pa) & np.isfinite(pb) & (pa > 0) & (pb > 0)
        pa = pa[mask]
        pb = pb[mask]
        if pa.size < 10 or pb.size < 10:
            return None

        ra = np.diff(np.log(pa))
        rb = np.diff(np.log(pb))
        min_pts = int(getattr(self, "_xex_min_points", 60))
        if ra.size < min_pts or rb.size < min_pts:
            return None

        sa = float(np.std(ra))
        sb = float(np.std(rb))
        if sa < 1e-12 or sb < 1e-12:
            # marché plat / quasi-plat : corr "neutre"
            return 1.0

        c = float(np.corrcoef(ra, rb)[0, 1])
        if not math.isfinite(c):
            return None
        return max(-1.0, min(1.0, c))

    def _xex_update_for_exchange(self, *, exchange: str, pair: str, now: datetime, vol_bps: float) -> None:
        if not self._xex_any_enabled():
            return
        ex = _norm_ex(exchange)
        pk = _norm_pair(pair)
        others = list(getattr(self, "_xex_neighbors", {}).get(ex, set()))
        if not others:
            return

        now_s = time.time()
        ttl = float(getattr(self, "_xex_ttl_s", float(self._ttl_s)))
        compute_iv = float(getattr(self, "_xex_compute_min_interval_s", 0.35))

        for other in others:
            if not other or other == ex:
                continue
            k = self._xex_key(pk, ex, other)
            rec = self._xex_cache.get(k, {})

            # --- DISLOCATION (cheap) ---
            if getattr(self, "_xex_dislocation_enabled", False) or getattr(self, "_xex_prudence_enabled", False):
                mid_a, ta = self._xex_latest_mid(ex, pk)
                mid_b, tb = self._xex_latest_mid(other, pk)
                if mid_a is not None and mid_b is not None and ta is not None and tb is not None:
                    # refuse stale points
                    if (now - ta).total_seconds() <= ttl and (now - tb).total_seconds() <= ttl:
                        rec["disloc_bps"] = float(self._xex_dislocation_bps(mid_a, mid_b))
                        rec["ts_disloc_s"] = float(now_s)
                        rec["mid_a"] = float(mid_a)
                        rec["mid_b"] = float(mid_b)
                        self._xex_cache[k] = rec

            # --- CORRELATION (heavier) ---
            if not getattr(self, "_xex_corr_enabled", False):
                continue
            # gate: ne calcule la corr que si vol_bps >= gate (typiquement soft_cap)
            gate = float(getattr(self, "_xex_corr_gate_vol_bps", float(self._soft_cap_bps)))
            if float(vol_bps) < gate:
                continue
            last_corr_s = float(rec.get("ts_corr_s", 0.0) or 0.0)
            if (now_s - last_corr_s) < compute_iv:
                continue

            corr = self._xex_corr_log_returns(ex, other, pk, now)
            if corr is not None:
                rec["corr"] = float(corr)
                rec["ts_corr_s"] = float(now_s)
                self._xex_cache[k] = rec

    def _xex_prudence_overlay(self, *, exchange: str, pair: str) -> Optional[str]:
        if not getattr(self, "_xex_prudence_enabled", False):
            return None
        ex = _norm_ex(exchange)
        pk = _norm_pair(pair)
        others = list(getattr(self, "_xex_neighbors", {}).get(ex, set()))
        if not others:
            return None

        now_s = time.time()
        ttl = float(getattr(self, "_xex_ttl_s", float(self._ttl_s)))

        worst_disloc = None
        worst_corr = None  # min corr

        for other in others:
            k = self._xex_key(pk, ex, other)
            rec = self._xex_cache.get(k)
            if not rec:
                continue

            # disloc
            td = float(rec.get("ts_disloc_s", 0.0) or 0.0)
            if td > 0.0 and (now_s - td) <= ttl:
                d = float(rec.get("disloc_bps", 0.0) or 0.0)
                worst_disloc = d if worst_disloc is None else max(worst_disloc, d)

            # corr
            tc = float(rec.get("ts_corr_s", 0.0) or 0.0)
            if tc > 0.0 and (now_s - tc) <= ttl:
                c = rec.get("corr", None)
                if c is not None:
                    c = float(c)
                    worst_corr = c if worst_corr is None else min(worst_corr, c)

        if worst_disloc is None and worst_corr is None:
            return None

        dis_w = float(getattr(self, "_xex_disloc_warn_bps", 6.0))
        dis_c = float(getattr(self, "_xex_disloc_crit_bps", 12.0))
        cor_w = float(getattr(self, "_xex_corr_warn", 0.97))
        cor_c = float(getattr(self, "_xex_corr_crit", 0.92))

        if (worst_disloc is not None and worst_disloc >= dis_c) or (worst_corr is not None and worst_corr <= cor_c):
            return "élevé"
        if (worst_disloc is not None and worst_disloc >= dis_w) or (worst_corr is not None and worst_corr <= cor_w):
            return "modéré"
        return None

    def get_xex_metrics(self, ex_a: str, ex_b: str, pair: str, ttl_s: Optional[float] = None) -> Optional[Dict[str, float]]:
        """Getter optionnel (non requis par RM) — utile si tu veux consommer ailleurs."""
        pk, x, y = self._xex_key(pair, ex_a, ex_b)
        rec = self._xex_cache.get((pk, x, y))
        if not rec:
            return None
        now_s = time.time()
        ttl = float(ttl_s) if ttl_s is not None else float(getattr(self, "_xex_ttl_s", float(self._ttl_s)))

        out: Dict[str, float] = {}
        td = float(rec.get("ts_disloc_s", 0.0) or 0.0)
        if td > 0 and (now_s - td) <= ttl and "disloc_bps" in rec:
            out["disloc_bps"] = float(rec["disloc_bps"])
            out["disloc_age_s"] = float(now_s - td)

        tc = float(rec.get("ts_corr_s", 0.0) or 0.0)
        if tc > 0 and (now_s - tc) <= ttl and "corr" in rec:
            out["corr"] = float(rec["corr"])
            out["corr_age_s"] = float(now_s - tc)

        return out or None

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

    def _resolve_region(self, exchange: str) -> str:
        exu = _norm_ex(exchange)
        cfg = getattr(self, "cfg", None)
        g = getattr(cfg, "g", None)
        region_map = None
        for obj in (cfg, g):
            if obj is None:
                continue
            for attr in ("exchange_region_map", "cex_region_map", "engine_region_map", "engine_pod_map"):
                mp = getattr(obj, attr, None)
                if isinstance(mp, dict):
                    region_map = mp
                    break
            if region_map is not None:
                break
        if region_map is None:
            return "UNKNOWN"
        region = None
        if exu in region_map:
            region = region_map.get(exu)
        else:
            for k, v in region_map.items():
                if _norm_ex(k) == exu:
                    region = v
                    break
        if region is None:
            return "UNKNOWN"
        r = str(region).upper()
        if r.startswith("JP") or r.startswith("TOKYO") or r.startswith("APAC"):
            return "JP"
        if r.startswith("US"):
            return "US"
        if r.startswith("EU"):
            return "EU"
        return "UNKNOWN"

    def _jp_blocked(self, exchange: str) -> bool:
        cfg = getattr(self, "cfg", None)
        g = getattr(cfg, "g", None)
        enable_jp = bool(getattr(g, "enable_jp", False)) if g is not None else bool(getattr(cfg, "enable_jp", False))
        if enable_jp:
            return False
        return self._resolve_region(exchange) in ("JP", "UNKNOWN")

    def last_age_seconds(self, exchange: str, pair_key: str) -> Optional[float]:
        rec = getattr(self, "_last_vol", {}).get((_norm_ex(exchange), _norm_pair(pair_key)))
        if not rec:
            return None
        return max(0.0, time.time() - float(rec.get("ts_recv_s", 0.0)))

    def _should_forward(self, exchange: str, pair_key: str, vol_bps: float) -> bool:
        key = (_norm_ex(exchange), _norm_pair(pair_key))
        now_s = time.time()
        last = self._last_forward.get(key)
        if last is None:
            self._last_forward[key] = {"vol_bps": float(vol_bps), "ts_s": now_s}
            return True
        delta = abs(float(vol_bps) - float(last.get("vol_bps", 0.0)))
        elapsed = now_s - float(last.get("ts_s", 0.0))
        if delta >= self._vol_min_delta_bps or elapsed >= self._max_silence_s:
            self._last_forward[key] = {"vol_bps": float(vol_bps), "ts_s": now_s}
            return True
        inc_blocked("volatility_monitor", "hysteresis_hold", _norm_pair(pair_key))
        return False

    def on_vol(self, msg: dict) -> None:
        """
        Consomme `cex:EX.vol` ; bid/ask > 0 ; passe par le pipeline interne,
        puis alimente `_last_vol[(EX, PK)] = {"vol_rel": v, "ts_recv_s": ...}` pour le TTL.
        """
        try:
            # P0 End-to-End Consistency: Validation via VolEvent
            try:
                # On utilise VolEvent pour la validation canonique
                # Note: VolEvent attend exchange, symbol/pair_key, best_bid/best_ask
                v_ev = VolEvent(**msg)
                ex = v_ev.exchange
                pk = v_ev.pair_key
                bid = v_ev.best_bid
                ask = v_ev.best_ask
            except Exception:
                from modules.obs_metrics import PAYLOAD_INVALID_TOTAL
                if PAYLOAD_INVALID_TOTAL:
                    PAYLOAD_INVALID_TOTAL.labels(kind="VolatilityMonitor", reason="payload_invalid").inc()
                inc_blocked("volatility_monitor", "schema_invalid", "UNKNOWN")
                return

            if bid <= 0 or ask <= 0 or bid >= ask:
                inc_blocked("volatility_monitor", "schema_mismatch", _norm_pair(pk))
                return

            data = {
                "active": v_ev.active,
                "pair_key": pk,
                "best_bid": bid,
                "best_ask": ask,
                "exchange_ts_ms": v_ev.exchange_ts_ms or 0,
                "recv_ts_ms": v_ev.recv_ts_ms or 0,
            }

            # Ingestion dans l'historique interne (signature à 1 seul param "data")
            self.ingest_snapshot(data)

            # Volatilité relative micro (déjà disponible via l’historique interne)
            vol_rel = float(self.get_price_volatility(pk, window="micro", exchange=ex))
            vol_bps = self._to_bps(vol_rel)

            # TTL cache consommé par get_volatility()
            ts_ms = v_ev.recv_ts_ms or v_ev.exchange_ts_ms
            ts_recv_s = (float(ts_ms) / 1000.0) if ts_ms else time.time()

            if not hasattr(self, "_last_vol"):
                self._last_vol = {}
            self._last_vol[(ex, pk)] = {
                "vol_rel": vol_rel,
                "vol_bps": vol_bps,
                "ts_recv_s": ts_recv_s,
            }

            # Observabilité : âge du dernier snapshot vol pour cette paire
            try:
                age_s = max(0.0, time.time() - ts_recv_s)
                set_vol_age_seconds(pk, age_s)
            except Exception:
                # best-effort, ne casse jamais le flux
                pass

            try:
                self.last_update = datetime.utcfromtimestamp(ts_recv_s)
            except Exception:
                self.last_update = datetime.utcnow()

            if self._jp_blocked(ex):
                inc_blocked("volatility_monitor", "region_disabled_jp", pk)
                return
            if not self._should_forward(ex, pk, vol_bps):
                return

            self._forward_to_scanner(ex, pk, vol_bps)

        except Exception:
            logging.exception("VolatilityMonitor.on_vol error")

    def attach_bus_vol_queues(self, queues_by_ex: dict[str, asyncio.Queue]) -> None:
        """Démarre un consumer par CEX pour le bus 'vol'."""

        async def _consume(ex: str, q: asyncio.Queue):
            """Consumer 'vol' par CEX.

            P0: si le consumer ralentit (ou si `on_vol()` devient coûteux), la queue peut
            monter au HIGH_WM_RATIO côté Router et provoquer des drops.

            Stratégie : en présence de backlog, on draine rapidement la queue et on ne garde
            que le dernier message par paire (coalescing), puis on traite le batch.
            """
            MAX_DRAIN = 256  # borné pour préserver la latence event-loop

            def _pk(m: dict) -> str:
                try:
                    return str(m.get("pair_key") or m.get("pair") or "UNKNOWN").upper()
                except Exception:
                    return "UNKNOWN"

            while True:
                msg = await q.get()
                try:
                    batch: dict[str, dict] = {_pk(msg): msg}

                    drained = 0
                    while drained < MAX_DRAIN and not q.empty():
                        try:
                            m2 = q.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                        drained += 1
                        try:
                            batch[_pk(m2)] = m2
                        finally:
                            try:
                                q.task_done()
                            except Exception:
                                pass

                    for m in batch.values():
                        try:
                            self.on_vol(m)
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

    async def detach_bus_consumers(self) -> None:
        tasks = list(self._bus_tasks.values())
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._bus_tasks.clear()