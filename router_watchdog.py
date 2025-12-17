# -*- coding: utf-8 -*-
"""
MarketDataRouterWatchdog — version "ceinture" notify-only (v3)
==============================================================

Objectif
--------
Surveiller le MarketDataRouter **sans aucune action locale** :
- Pas de restart/stop/start, seulement des événements (health/alert).
- Tient compte du mode (EU_ONLY / SPLIT) pour les seuils de latence/staleness.
- Tolérant aux états partiels : si une métrique manque, on n'échoue pas, on journalise.

Entrée attendue (`state_fn`)
----------------------------
`state = await state_fn()` (ou sync) → dict par ex.:
{
  "now_ts": float,
  "latency": {"router_to_scanner_p95_ms": float, "router_to_scanner_p99_ms": float},
  "queues": {
     "combo": {"depth": int, "max": int, "dropped_total": int, "coalesced_total": int},
     "vol":   {"depth": int, "max": int},
     "slip":  {"depth": int, "max": int},
     "health":{"depth": int, "max": int}
  },
  "staleness": {"stale_ms": float, "base_delta_ms": float, "skew_ms": float},
  "shards": {"BINANCE:0": {"lag_ms": float, "reconnects": int}, ...}
}

Note: les clés manquantes sont gérées.
"""
from __future__ import annotations
import asyncio, time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from base_watchdog import BaseWatchdogV2

StateFn = Callable[[], Dict[str, Any] | Awaitable[Dict[str, Any]]]


@dataclass
class RouterThresholds:
    # Latence Router→Scanner (p95)
    r2s_warn_ms: int = 13
    r2s_crit_ms: int = 25
    # Staleness L2
    stale_warn_ms: int = 900
    stale_crit_ms: int = 1200
    # Occupation des queues
    q_warn_ratio: float = 0.80
    q_crit_ratio: float = 0.95
    # Gap shard / lag
    shard_lag_warn_ms: int = 150
    shard_lag_crit_ms: int = 300
    # Escalade globale
    escalate_after_cycles: int = 3


@dataclass
class RouterModeTuning:
    mode: str = "EU_ONLY"  # "EU_ONLY" ou "SPLIT"
    # En SPLIT, on tolère un peu plus (réseau US/EU)
    split_ms_factor: float = 1.15

    def apply(self, t: RouterThresholds) -> RouterThresholds:
        if str(self.mode).upper() != "SPLIT":
            return t
        return RouterThresholds(
            r2s_warn_ms=int(t.r2s_warn_ms * self.split_ms_factor),
            r2s_crit_ms=int(t.r2s_crit_ms * self.split_ms_factor),
            stale_warn_ms=int(t.stale_warn_ms * self.split_ms_factor),
            stale_crit_ms=int(t.stale_crit_ms * self.split_ms_factor),
            q_warn_ratio=t.q_warn_ratio,
            q_crit_ratio=t.q_crit_ratio,
            shard_lag_warn_ms=int(t.shard_lag_warn_ms * self.split_ms_factor),
            shard_lag_crit_ms=int(t.shard_lag_crit_ms * self.split_ms_factor),
            escalate_after_cycles=t.escalate_after_cycles,
        )


@dataclass
class RouterWatchdogConfig:
    check_interval: float = 2.0
    thresholds: RouterThresholds = field(default_factory=RouterThresholds)
    tuning: RouterModeTuning = field(default_factory=RouterModeTuning)


class MarketDataRouterWatchdog(BaseWatchdogV2):
    """Watchdog Router passif (notify-only)."""

    def __init__(
        self,
        state_fn: StateFn,
        *,
        name: str = "MarketDataRouterWatchdog",
        config: Optional[RouterWatchdogConfig] = None,
        notify_only: bool = True,
        verbose: bool = True,
    ) -> None:
        cfg = config or RouterWatchdogConfig()
        applied = cfg.tuning.apply(cfg.thresholds)
        super().__init__(
            name=name,
            check_interval=cfg.check_interval,
            restart_cooldown_s=30.0,
            max_restarts_per_min=3,
            verbose=verbose,
            notify_only=notify_only,
        )
        self.cfg = cfg
        self.th = applied
        self.state_fn = state_fn
        self._stale_cycles: int = 0

    async def _check_once(self) -> None:
        state = await self._get_state()
        now = float(state.get("now_ts", time.time()))
        qmap = state.get("queues", {}) or {}
        lat = state.get("latency", {}) or {}
        stale = state.get("staleness", {}) or {}
        shards = state.get("shards", {}) or {}

        # 1) Latence Router→Scanner
        r2s_p95 = float(lat.get("router_to_scanner_p95_ms", 0.0))
        r2s_p99 = float(lat.get("router_to_scanner_p99_ms", 0.0))
        if r2s_p95 > 0:
            if r2s_p95 >= self.th.r2s_crit_ms:
                self.emit_event(type="alert", level="CRIT", message="router_to_scanner_latency_crit",
                                r2s_p95_ms=int(r2s_p95), r2s_p99_ms=int(r2s_p99),
                                thresholds={"crit": self.th.r2s_crit_ms}, intent="request_full_restart")
            elif r2s_p95 >= self.th.r2s_warn_ms:
                self.emit_event(type="health", level="WARN", message="router_to_scanner_latency_warn",
                                r2s_p95_ms=int(r2s_p95), r2s_p99_ms=int(r2s_p99),
                                thresholds={"warn": self.th.r2s_warn_ms})
        else:
            self.emit_event(type="health", level="INFO", message="router_to_scanner_latency_unset")

        # 2) Occupation queues
        for qname in ("combo", "vol", "slip", "health"):
            q = qmap.get(qname) or {}
            d, m = int(q.get("depth", 0)), int(q.get("max", 0) or 1)
            ratio = d / float(m)
            if ratio >= self.th.q_crit_ratio:
                self.emit_event(type="alert", level="CRIT", message="queue_saturation_crit",
                                queue=qname, depth=d, max=m, ratio=round(ratio, 3),
                                thresholds={"crit_ratio": self.th.q_crit_ratio}, intent="request_full_restart")
            elif ratio >= self.th.q_warn_ratio:
                self.emit_event(type="health", level="WARN", message="queue_saturation_warn",
                                queue=qname, depth=d, max=m, ratio=round(ratio, 3),
                                thresholds={"warn_ratio": self.th.q_warn_ratio})

            # Drops/coalescing si dispo
            dropped = int(q.get("dropped_total", 0))
            coalesced = int(q.get("coalesced_total", 0))
            if dropped > 0:
                self.emit_event(type="alert", level="WARN", message="queue_drops_detected",
                                queue=qname, dropped_total=dropped, coalesced_total=coalesced)

        # 3) Staleness L2
        stale_ms = float(stale.get("stale_ms", 0.0))
        base_delta = float(stale.get("base_delta_ms", 0.0))
        skew = float(stale.get("skew_ms", 0.0))
        if stale_ms >= self.th.stale_warn_ms:
            self._stale_cycles += 1
            lvl = "CRIT" if stale_ms >= self.th.stale_crit_ms else "WARN"
            msg = "staleness_crit" if lvl == "CRIT" else "staleness_warn"
            self.emit_event(type=("alert" if lvl == "CRIT" else "health"), level=lvl, message=msg,
                            stale_ms=int(stale_ms), base_delta_ms=int(base_delta), skew_ms=int(skew),
                            thresholds={"warn": self.th.stale_warn_ms, "crit": self.th.stale_crit_ms},
                            **({"intent": "request_full_restart"} if lvl == "CRIT" else {}))
        else:
            self._stale_cycles = 0

        # 4) Shards
        for shard, sinfo in shards.items():
            lag = float((sinfo or {}).get("lag_ms", 0.0))
            if lag >= self.th.shard_lag_crit_ms:
                self.emit_event(type="alert", level="CRIT", message="shard_lag_crit",
                                shard=shard, lag_ms=int(lag),
                                thresholds={"crit": self.th.shard_lag_crit_ms}, intent="request_full_restart")
            elif lag >= self.th.shard_lag_warn_ms:
                self.emit_event(type="health", level="WARN", message="shard_lag_warn",
                                shard=shard, lag_ms=int(lag),
                                thresholds={"warn": self.th.shard_lag_warn_ms})

        # 5) Escalade globale si staleness persiste
        if self._stale_cycles >= max(1, self.th.escalate_after_cycles):
            self.emit_event(type="alert", level="CRIT", message="router_global_staleness_persistent",
                            cycles=self._stale_cycles, intent="request_full_restart")

    async def _get_state(self) -> Dict[str, Any]:
        try:
            res = self.state_fn()
            if asyncio.iscoroutine(res):
                return await res
            return res or {}
        except Exception as e:
            self.emit_event(type="error", level="ERROR", message=f"state_fn failed: {e}")
            return {}

    def get_status(self) -> Dict[str, Any]:
        s = super().get_status()
        s.update({
            "module": "MarketDataRouterWatchdog",
            "metrics": {**s.get("metrics", {}),
                         "stale_cycles": self._stale_cycles,
                         "r2s_warn_ms": self.th.r2s_warn_ms,
                         "r2s_crit_ms": self.th.r2s_crit_ms,
                         "stale_warn_ms": self.th.stale_warn_ms,
                         "stale_crit_ms": self.th.stale_crit_ms,
                         "q_warn_ratio": self.th.q_warn_ratio,
                         "q_crit_ratio": self.th.q_crit_ratio},
        })
        return s
