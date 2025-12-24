# -*- coding: utf-8 -*-
"""
VolatilityWatchdog — version "ceinture" notify-only (v3)
=======================================================

Objectif
--------
Surveiller la métrique de volatilité **sans aucune action locale** : uniquement des
évènements health/alert, orchestrés ensuite par le CentralWatchdog.

Entrée (`state_fn`)
-------------------
`state = await state_fn()` (ou sync) → dict suggéré (tolérant):
{
  "now_ts": float,
  "global": {"ema_bps": float, "p95_bps": float, "p99_bps": float, "age_s": float, "chaos": bool},
  "exchanges": { "BINANCE": {"ema_bps": float, "p95_bps": float, "p99_bps": float, "age_s": float, "chaos": bool}, ... },
  "pairs": {
     "BINANCE:BTCUSDC": {"ema_bps": float, "p95_bps": float, "p99_bps": float, "age_s": float, "zscore": float},
     ...
  }
}

Nota: `zscore` optionnel pour détecter spikes (sinon on approx avec p99 vs ema).
"""
from __future__ import annotations
import asyncio, time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional

from modules.watchdog.base_watchdog import BaseWatchdogV2
from modules.bot_config import BotConfig

StateFn = Callable[[], Dict[str, Any] | Awaitable[Dict[str, Any]]]


@dataclass
class VolThresholds:
    # Fraîcheur des données
    age_warn_s: float = 5.0
    age_crit_s: float = 8.0
    # Caps globaux (majors)
    soft_cap_bps: float = 50.0
    hard_cap_bps: float = 80.0
    # P95/P99 bornes génériques
    p95_warn_bps: float = 40.0
    p99_crit_bps: float = 70.0
    # Spike detector
    z_warn: float = 2.5
    z_crit: float = 3.5
    # Persistance
    escalate_after_cycles: int = 3


@dataclass
class VolatilityWatchdogConfig:
    check_interval: float = 2.0
    thresholds: VolThresholds = field(default_factory=VolThresholds)
    # Overrides possibles par exchange ou par pair
    per_exchange_caps: Dict[str, Dict[str, float]] = field(default_factory=dict)  # {"BINANCE": {"soft":60,"hard":90}}
    per_pair_caps: Dict[str, Dict[str, float]] = field(default_factory=dict)      # {"BINANCE:BTCUSDC": {"soft":55,"hard":85}}


class VolatilityWatchdog(BaseWatchdogV2):
    """Watchdog Volatility passif (notify-only)."""

    def __init__(
        self,
        state_fn: StateFn,
        *,
        name: str = "VolatilityWatchdog",
        config: Optional[VolatilityWatchdogConfig] = None,
        notify_only: bool = True,
        verbose: bool = True,
        bot_config: Optional[BotConfig] = None,
    ) -> None:
        bot_cfg = bot_config or BotConfig.from_env()
        if bot_config is None:
            self.record_fallback("bot_config")
        th = VolThresholds(
            age_warn_s=bot_cfg.wd.volatility_age_warn_s,
            age_crit_s=bot_cfg.wd.volatility_age_crit_s,
            soft_cap_bps=bot_cfg.wd.volatility_soft_cap_bps,
            hard_cap_bps=bot_cfg.wd.volatility_hard_cap_bps,
            p95_warn_bps=bot_cfg.wd.volatility_p95_warn_bps,
            p99_crit_bps=bot_cfg.wd.volatility_p99_crit_bps,
            z_warn=bot_cfg.wd.volatility_z_warn,
            z_crit=bot_cfg.wd.volatility_z_crit,
            escalate_after_cycles=bot_cfg.wd.volatility_escalate_after_cycles,
        )
        cfg = config or VolatilityWatchdogConfig(check_interval=bot_cfg.wd.volatility_interval_s, thresholds=th)
        super().__init__(
            name=name,
            check_interval=cfg.check_interval,
            restart_cooldown_s=30.0,
            max_restarts_per_min=3,
            event_cooldown_s=bot_cfg.wd.cooldown_s,
            verbose=verbose,
            notify_only=notify_only,
        )
        self.cfg = cfg
        self.th = cfg.thresholds
        self.state_fn = state_fn
        self._bad_cycles: int = 0

    async def _check_once(self) -> None:
        snapshot = await self.collect_snapshot()
        severity, reasons, details = self.evaluate(snapshot)
        self.emit_health_event(
            severity=severity,
            reasons=reasons,
            details=details,
            component="Volatility",
            module="VolatilityMonitor",
            observed_at_ms=snapshot.get("observed_at_ms"),
        )

    # ---- helpers ----
    def _caps_for_exchange(self, ex: str, soft: float, hard: float) -> tuple[float, float]:
        o = self.cfg.per_exchange_caps.get((ex or "").upper(), {})
        return float(o.get("soft", soft)), float(o.get("hard", hard))

    def _caps_for_pair(self, pair: str, soft: float, hard: float) -> tuple[float, float]:
        o = self.cfg.per_pair_caps.get(pair or "", {})
        return float(o.get("soft", soft)), float(o.get("hard", hard))

    async def _get_state(self) -> Dict[str, Any]:
        try:
            r = self.state_fn()
            if asyncio.iscoroutine(r):
                return await r
            return r or {}
        except Exception as e:
            self.emit_event(type="error", level="ERROR", message=f"state_fn failed: {e}")
            return {}

    async def collect_snapshot(self) -> Dict[str, Any]:
        observed_at_ms = self.now_ts_ms()
        missing: list[str] = []
        errors: list[str] = []
        state: Dict[str, Any] = {}
        if self.state_fn:
            state = await self.safe_call(self.state_fn, default={}, errors=errors, error_label="state_fn")
        if not state:
            missing.append("MISSING_FIELD:state_fn")
        return {
            "observed_at_ms": observed_at_ms,
            "module_state": state or {},
            "missing": missing,
            "errors": errors,
        }

    def evaluate(self, snapshot: Dict[str, Any]) -> tuple[str, list[str], Dict[str, Any]]:
        s = snapshot.get("module_state", {}) or {}
        missing = list(snapshot.get("missing") or [])
        g = self.safe_get(s, "global", default={}, missing=missing)
        exmap = self.safe_get(s, "exchanges", default={}, missing=missing)
        pairs = self.safe_get(s, "pairs", default={}, missing=missing)
        if not isinstance(g, dict):
            g = {}
        if not isinstance(exmap, dict):
            exmap = {}
        if not isinstance(pairs, dict):
            pairs = {}

        reasons: list[str] = []
        details: Dict[str, Any] = {}
        any_warn = False
        any_crit = False

        g_age = self.safe_float(g, "age_s", default=0.0, missing=missing)
        g_p95 = self.safe_float(g, "p95_bps", default=0.0, missing=missing)
        g_p99 = self.safe_float(g, "p99_bps", default=0.0, missing=missing)
        g_ema = self.safe_float(g, "ema_bps", default=0.0, missing=missing)
        g_cha = bool(self.safe_get(g, "chaos", default=False, missing=missing))

        soft = self.th.soft_cap_bps
        hard = self.th.hard_cap_bps

        if g_age >= self.th.age_crit_s or g_p99 >= max(self.th.p99_crit_bps, hard):
            reasons.append("WD_STALE")
            any_crit = True
        elif g_age >= self.th.age_warn_s or g_p95 >= max(self.th.p95_warn_bps, soft) or g_cha:
            any_warn = True

        for ex, m in exmap.items():
            m = m or {}
            age = self.safe_float(m, "age_s", default=0.0, missing=missing)
            p95 = self.safe_float(m, "p95_bps", default=0.0, missing=missing)
            p99 = self.safe_float(m, "p99_bps", default=0.0, missing=missing)
            ema = self.safe_float(m, "ema_bps", default=0.0, missing=missing)
            chaos = bool(self.safe_get(m, "chaos", default=False, missing=missing))
            soft_e, hard_e = self._caps_for_exchange(ex, soft, hard)

            if age >= self.th.age_crit_s or p99 >= max(self.th.p99_crit_bps, hard_e):
                reasons.append("WD_STALE")
                any_crit = True
            elif age >= self.th.age_warn_s or p95 >= max(self.th.p95_warn_bps, soft_e) or chaos:
                any_warn = True

        for pair, m in pairs.items():
            m = m or {}
            age = self.safe_float(m, "age_s", default=0.0, missing=missing)
            p95 = self.safe_float(m, "p95_bps", default=0.0, missing=missing)
            p99 = self.safe_float(m, "p99_bps", default=0.0, missing=missing)
            ema = self.safe_float(m, "ema_bps", default=0.0, missing=missing)
            z = self.safe_float(m, "zscore", default=0.0, missing=missing)
            soft_p, hard_p = self._caps_for_pair(pair, soft, hard)

            spike_warn = (z >= self.th.z_warn) or (ema > 0 and (p99 - ema) >= self.th.z_warn * (ema / 10.0))
            spike_crit = (z >= self.th.z_crit) or (ema > 0 and (p99 - ema) >= self.th.z_crit * (ema / 10.0))

            if age >= self.th.age_crit_s or p99 >= max(self.th.p99_crit_bps, hard_p) or spike_crit:
                reasons.append("WD_STALE")
                any_crit = True
            elif age >= self.th.age_warn_s or p95 >= max(self.th.p95_warn_bps, soft_p) or spike_warn:
                any_warn = True

        if any_warn or any_crit:
            self._bad_cycles += 1
        else:
            self._bad_cycles = 0

        if self._bad_cycles >= max(1, self.th.escalate_after_cycles):
            reasons.append("WD_STALE")
            any_crit = True

        if missing:
            reasons.append("MISSING_FIELD")
            details["missing_fields"] = missing

        details.update({
            "bad_cycles": self._bad_cycles,
            "global": {"age_s": g_age, "p95_bps": g_p95, "p99_bps": g_p99, "ema_bps": g_ema, "chaos": g_cha},
        })

        severity = "OK"
        if any_crit or "WD_STALE" in reasons:
            severity = "CRIT"
        elif any_warn or reasons:
            severity = "WARN"
        return severity, reasons, details

    def get_status(self) -> Dict[str, Any]:
        s = super().get_status()
        s.update({
            "module": "VolatilityWatchdog",
            "metrics": {**s.get("metrics", {}),
                         "bad_cycles": self._bad_cycles,
                         "age_warn_s": self.th.age_warn_s, "age_crit_s": self.th.age_crit_s,
                         "soft_cap_bps": self.th.soft_cap_bps, "hard_cap_bps": self.th.hard_cap_bps,
                         "p95_warn_bps": self.th.p95_warn_bps, "p99_crit_bps": self.th.p99_crit_bps,
                         "z_warn": self.th.z_warn, "z_crit": self.th.z_crit},
        })
        return s
