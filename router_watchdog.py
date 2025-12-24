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

from modules.watchdog.base_watchdog import BaseWatchdogV2
from modules.bot_config import BotConfig

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
        state_fn: Optional[StateFn] = None,
        *,
        router: Optional[Any] = None,
        bot_config: Optional[BotConfig] = None,
        name: str = "MarketDataRouterWatchdog",
        config: Optional[RouterWatchdogConfig] = None,
        notify_only: bool = True,
        verbose: bool = True,
    ) -> None:
        cfg = config or RouterWatchdogConfig()
        bot_cfg = bot_config
        if bot_cfg is None:
            bot_cfg = BotConfig.from_env()
            self.record_fallback("bot_config")
        wd_cfg = bot_cfg.wd
        super().__init__(
            name=name,
            check_interval=wd_cfg.router_interval_s,
            restart_cooldown_s=30.0,
            max_restarts_per_min=3,
            event_cooldown_s=wd_cfg.cooldown_s,
            verbose=verbose,
            notify_only=notify_only,
        )
        self.cfg = cfg
        self.state_fn = state_fn
        self.router = router
        self.health_stale_ms = int(wd_cfg.router_health_stale_ms)
        self.health_min_coverage_ratio = float(wd_cfg.router_health_min_coverage_ratio)
        self.health_queue_max = int(wd_cfg.router_health_queue_max)
        self._last_health_by_exchange: Dict[str, int] = {}
        self._last_pairs_seen: Dict[str, set[str]] = {}

    async def _check_once(self) -> None:
        snapshot = await self.collect_snapshot()
        severity, reasons, details = self.evaluate(snapshot)
        self.emit_health_event(
            severity=severity,
            reasons=reasons,
            details=details,
            component="Router",
            module="MarketDataRouter",
            observed_at_ms=snapshot.get("observed_at_ms"),
        )

    async def collect_snapshot(self) -> Dict[str, Any]:
        observed_at_ms = self.now_ts_ms()
        missing: list[str] = []
        errors: list[str] = []
        state: Dict[str, Any] = {}
        if self.state_fn:
            state = await self.safe_call(self.state_fn, default={}, errors=errors, error_label="state_fn")
        if not state and self.router is not None:
            state = await self.safe_call(getattr(self.router, "get_status", None), default={}, errors=errors, error_label="router.get_status")
        health = self._drain_health_queues(observed_at_ms, state or {}, missing)
        return {
            "observed_at_ms": observed_at_ms,
            "module_state": state or {},
            "health": health,
            "missing": missing,
            "errors": errors,
        }

    def evaluate(self, snapshot: Dict[str, Any]) -> Tuple[str, list[str], Dict[str, Any]]:
        reasons: list[str] = []
        details: Dict[str, Any] = {"health": snapshot.get("health", {})}
        missing = list(snapshot.get("missing") or [])
        if missing:
            reasons.append("MISSING_FIELD")
            details["missing_fields"] = missing

        now_ms = int(snapshot.get("observed_at_ms") or self.now_ts_ms())
        health_map = snapshot.get("health", {}) or {}
        for ex, info in health_map.items():
            last_event_ts_ms = self.safe_ts_ms(info, "last_event_ts_ms", default=None, missing=missing)
            age_ms = self.safe_age_ms(last_event_ts_ms, now_ms)
            if age_ms is None:
                reasons.append("MISSING_FIELD")
                details.setdefault("missing_fields", []).append(f"MISSING_FIELD:{ex}:last_event_ts_ms")
            else:
                if age_ms >= self.health_stale_ms:
                    reasons.append("PUBLIC_FEED_STALE")
                info["age_ms"] = age_ms
            queue_size = self.safe_int(info, "queue_size", default=0, missing=missing)
            if queue_size > self.health_queue_max:
                reasons.append("HEALTH_QUEUE_BACKLOG")

            expected = self.safe_get(info, "expected_pairs", default=None, missing=missing)
            seen = self.safe_get(info, "pairs_seen_count", default=None, missing=missing)
            if isinstance(expected, int) and expected > 0 and isinstance(seen, int):
                ratio = float(seen) / float(expected)
                info["coverage_ratio"] = round(ratio, 4)
                if ratio < self.health_min_coverage_ratio:
                    reasons.append("PUBLIC_FEED_COVERAGE_LOW")

        severity = "OK"
        if "PUBLIC_FEED_STALE" in reasons:
            severity = "CRIT"
        elif reasons:
            severity = "WARN"
        return severity, reasons, details

    def _expected_pairs(self, state: Dict[str, Any]) -> Optional[int]:
        candidates = [
            state.get("active_pairs"),
            state.get("pairs"),
            getattr(self.router, "active_pairs", None) if self.router is not None else None,
            getattr(self.router, "pairs", None) if self.router is not None else None,
            getattr(getattr(self.router, "bot_cfg", None), "g", None).pairs if self.router is not None and getattr(self.router, "bot_cfg", None) else None,
        ]
        for cand in candidates:
            if isinstance(cand, (list, set, tuple)):
                return int(len(cand))
            if isinstance(cand, dict):
                return int(len(cand))
        return None

    def _drain_health_queues(self, observed_at_ms: int, state: Dict[str, Any], missing: list[str]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        router = self.router
        out_queues = self.safe_get(router, "out_queues", default=None, missing=missing)
        if not isinstance(out_queues, dict):
            return out
        expected_pairs = self._expected_pairs(state)
        for route_name, qmap in out_queues.items():
            if not isinstance(route_name, str) or not route_name.startswith("cex:"):
                continue
            ex = route_name.split(":", 1)[1].upper()
            q = (qmap or {}).get("health")
            if q is None:
                missing.append(f"MISSING_FIELD:{route_name}.health")
                continue
            queue_size = self.safe_get(q, "qsize", default=0, missing=missing)
            try:
                queue_size = int(q.qsize())
            except Exception:
                queue_size = int(queue_size or 0)
            drained = 0
            pairs_seen: set[str] = set(self._last_pairs_seen.get(ex, set()))
            last_event_ts_ms = int(self._last_health_by_exchange.get(ex, 0))
            while True:
                try:
                    ev = q.get_nowait()
                except Exception:
                    break
                drained += 1
                if isinstance(ev, dict):
                    pair_key = ev.get("pair_key")
                    if pair_key:
                        pairs_seen.add(str(pair_key))
                    ts = ev.get("recv_ts_ms") or ev.get("exchange_ts_ms")
                    try:
                        ts_val = int(ts)
                    except Exception:
                        ts_val = 0
                    if ts_val > last_event_ts_ms:
                        last_event_ts_ms = ts_val

            self._last_pairs_seen[ex] = pairs_seen
            if last_event_ts_ms:
                self._last_health_by_exchange[ex] = last_event_ts_ms
            out[ex] = {
                "queue_size": queue_size,
                "drained": drained,
                "pairs_seen_count": len(pairs_seen),
                "expected_pairs": expected_pairs,
                "last_event_ts_ms": last_event_ts_ms or None,
                "observed_at_ms": observed_at_ms,
            }
        return out

    async def _get_state(self) -> Dict[str, Any]:
        if not self.state_fn:
            return {}
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
                         "health_stale_ms": self.health_stale_ms,
                         "health_min_coverage_ratio": self.health_min_coverage_ratio,
                         "health_queue_max": self.health_queue_max},
        })
        return s