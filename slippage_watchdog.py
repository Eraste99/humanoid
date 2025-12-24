# -*- coding: utf-8 -*-
"""
SlippageWatchdog — version "ceinture" notify-only (v3)
=====================================================

Objectif
--------
Surveiller la dérive de slippage **sans aucune action locale**; émettre des
évènements health/alert. Aucune régulation en ligne ici; seulement notify.

Entrée (`state_fn`)
-------------------
`state = await state_fn()` → dict suggéré:
{
  "now_ts": float,
  "global": {"p95_bps": float, "p99_bps": float, "age_s": float},
  "exchanges": { "BINANCE": {"p95_bps": float, "p99_bps": float, "age_s": float}, ... },
  "pairs": {
     "BINANCE:BTCUSDC:BUY":  {"p95_bps": float, "p99_bps": float, "age_s": float},
     "BINANCE:BTCUSDC:SELL": {"p95_bps": float, "p99_bps": float, "age_s": float},
     ...
  }
}

Remarques
---------
- Les valeurs sont en bps (positif = pire que mid). On applique abs() par prudence.
- Overrides par exchange/pair/side possibles.
"""
from __future__ import annotations
import asyncio, time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from modules.watchdog.base_watchdog import BaseWatchdogV2
from modules.bot_config import BotConfig

StateFn = Callable[[], Dict[str, Any] | Awaitable[Dict[str, Any]]]


@dataclass
class SlipThresholds:
    age_warn_s: float = 2.0
    age_crit_s: float = 3.0
    p95_warn_bps: float = 15.0
    p99_crit_bps: float = 30.0
    escalate_after_cycles: int = 3


@dataclass
class SlippageWatchdogConfig:
    check_interval: float = 2.0
    thresholds: SlipThresholds = field(default_factory=SlipThresholds)
    per_exchange_caps: Dict[str, Dict[str, float]] = field(default_factory=dict)   # {"BINANCE": {"p95_warn": 12, "p99_crit": 25}}
    per_pair_caps: Dict[str, Dict[str, float]] = field(default_factory=dict)       # {"BINANCE:BTCUSDC:BUY": {"p95_warn": 10, "p99_crit": 20}}


    def __init__(
            self,
            state_fn: Optional[StateFn] = None,
            *,
            name: str = "SlippageWatchdog",
            config: Optional[SlippageWatchdogConfig] = None,
            notify_only: bool = True,
            verbose: bool = True,
            bot_config: Optional[BotConfig] = None,
    ) -> None:
        bot_cfg = bot_config or BotConfig.from_env()
        if bot_config is None:
            self.record_fallback("bot_config")
        th = SlipThresholds(
            age_warn_s=bot_cfg.wd.slippage_age_warn_s,
            age_crit_s=bot_cfg.wd.slippage_age_crit_s,
            p95_warn_bps=bot_cfg.wd.slippage_p95_warn_bps,
            p99_crit_bps=bot_cfg.wd.slippage_p99_crit_bps,
            escalate_after_cycles=bot_cfg.wd.slippage_escalate_after_cycles,
        )
        cfg = config or SlippageWatchdogConfig(check_interval=bot_cfg.wd.slippage_interval_s, thresholds=th)
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
            component="Slippage",
            module="SlippageHandler",
            observed_at_ms=snapshot.get("observed_at_ms"),
        )
    # ---- helpers ----
    def _caps_for_exchange(self, ex: str) -> Tuple[float, float]:
        o = self.cfg.per_exchange_caps.get((ex or "").upper(), {})
        return float(o.get("p95_warn", self.th.p95_warn_bps)), float(o.get("p99_crit", self.th.p99_crit_bps))

    def _caps_for_pair(self, pair: str) -> Tuple[float, float]:
        o = self.cfg.per_pair_caps.get(pair or "", {})
        return float(o.get("p95_warn", self.th.p95_warn_bps)), float(o.get("p99_crit", self.th.p99_crit_bps))

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

    def evaluate(self, snapshot: Dict[str, Any]) -> Tuple[str, list[str], Dict[str, Any]]:
        missing = list(snapshot.get("missing") or [])
        s = snapshot.get("module_state", {}) or {}
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
        g_p95 = abs(self.safe_float(g, "p95_bps", default=0.0, missing=missing))
        g_p99 = abs(self.safe_float(g, "p99_bps", default=0.0, missing=missing))
        if g_age >= self.th.age_crit_s or g_p99 >= self.th.p99_crit_bps:
            reasons.append("WD_STALE")
            any_crit = True
        elif g_age >= self.th.age_warn_s or g_p95 >= self.th.p95_warn_bps:
            any_warn = True

        for ex, m in exmap.items():
            m = m or {}
            age = self.safe_float(m, "age_s", default=0.0, missing=missing)
            p95 = abs(self.safe_float(m, "p95_bps", default=0.0, missing=missing))
            p99 = abs(self.safe_float(m, "p99_bps", default=0.0, missing=missing))

            w, c = self._caps_for_exchange(ex)
            if age >= self.th.age_crit_s or p99 >= c:
                reasons.append("WD_STALE")
                any_crit = True
            elif age >= self.th.age_warn_s or p95 >= w:
                any_warn = True

        for key, m in pairs.items():
            m = m or {}
            age = self.safe_float(m, "age_s", default=0.0, missing=missing)
            p95 = abs(self.safe_float(m, "p95_bps", default=0.0, missing=missing))
            p99 = abs(self.safe_float(m, "p99_bps", default=0.0, missing=missing))
            w, c = self._caps_for_pair(key)
            if age >= self.th.age_crit_s or p99 >= c:
                reasons.append("WD_STALE")
                any_crit = True
            elif age >= self.th.age_warn_s or p95 >= w:
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

        details["bad_cycles"] = self._bad_cycles
        severity = "OK"
        if any_crit or "WD_STALE" in reasons:
            severity = "CRIT"
        elif any_warn or reasons:
            severity = "WARN"
        return severity, reasons, details

    def get_status(self) -> Dict[str, Any]:
        s = super().get_status()
        s.update({
            "module": "SlippageWatchdog",
            "metrics": {**s.get("metrics", {}),
                         "bad_cycles": self._bad_cycles,
                         "age_warn_s": self.th.age_warn_s, "age_crit_s": self.th.age_crit_s,
                         "p95_warn_bps": self.th.p95_warn_bps, "p99_crit_bps": self.th.p99_crit_bps},
        })
        return s
