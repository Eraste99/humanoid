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

from base_watchdog import BaseWatchdogV2

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


class SlippageWatchdog(BaseWatchdogV2):
    """Watchdog Slippage passif (notify-only)."""

    def __init__(
        self,
        state_fn: StateFn,
        *,
        name: str = "SlippageWatchdog",
        config: Optional[SlippageWatchdogConfig] = None,
        notify_only: bool = True,
        verbose: bool = True,
    ) -> None:
        cfg = config or SlippageWatchdogConfig()
        super().__init__(
            name=name,
            check_interval=cfg.check_interval,
            restart_cooldown_s=30.0,
            max_restarts_per_min=3,
            verbose=verbose,
            notify_only=notify_only,
        )
        self.cfg = cfg
        self.th = cfg.thresholds
        self.state_fn = state_fn
        self._bad_cycles: int = 0

    async def _check_once(self) -> None:
        s = await self._get_state()
        g = s.get("global", {}) or {}
        exmap = s.get("exchanges", {}) or {}
        pairs = s.get("pairs", {}) or {}

        any_warn = False
        any_crit = False

        # 1) Global
        g_age = float(g.get("age_s", 0.0))
        g_p95 = abs(float(g.get("p95_bps", 0.0)))
        g_p99 = abs(float(g.get("p99_bps", 0.0)))
        if g_age >= self.th.age_crit_s or g_p99 >= self.th.p99_crit_bps:
            self.emit_event(type="alert", level="CRIT", message="slip_global_crit",
                            age_s=g_age, p95_bps=g_p95, p99_bps=g_p99,
                            thresholds={"age_crit": self.th.age_crit_s, "p99_crit": self.th.p99_crit_bps},
                            intent="request_full_restart")
            any_crit = True
        elif g_age >= self.th.age_warn_s or g_p95 >= self.th.p95_warn_bps:
            self.emit_event(type="health", level="WARN", message="slip_global_warn",
                            age_s=g_age, p95_bps=g_p95, p99_bps=g_p99,
                            thresholds={"age_warn": self.th.age_warn_s, "p95_warn": self.th.p95_warn_bps})
            any_warn = True

        # 2) Par exchange
        for ex, m in exmap.items():
            age = float((m or {}).get("age_s", 0.0))
            p95 = abs(float((m or {}).get("p95_bps", 0.0)))
            p99 = abs(float((m or {}).get("p99_bps", 0.0)))
            w, c = self._caps_for_exchange(ex)

            if age >= self.th.age_crit_s or p99 >= c:
                self.emit_event(type="alert", level="CRIT", message="slip_exchange_crit",
                                exchange=ex, age_s=age, p95_bps=p95, p99_bps=p99,
                                thresholds={"age_crit": self.th.age_crit_s, "p99_crit": c},
                                intent="request_full_restart")
                any_crit = True
            elif age >= self.th.age_warn_s or p95 >= w:
                self.emit_event(type="health", level="WARN", message="slip_exchange_warn",
                                exchange=ex, age_s=age, p95_bps=p95, p99_bps=p99,
                                thresholds={"age_warn": self.th.age_warn_s, "p95_warn": w})
                any_warn = True

        # 3) Par pair/side
        for key, m in pairs.items():
            age = float((m or {}).get("age_s", 0.0))
            p95 = abs(float((m or {}).get("p95_bps", 0.0)))
            p99 = abs(float((m or {}).get("p99_bps", 0.0)))
            w, c = self._caps_for_pair(key)

            if age >= self.th.age_crit_s or p99 >= c:
                self.emit_event(type="alert", level="CRIT", message="slip_pair_crit",
                                pair=key, age_s=age, p95_bps=p95, p99_bps=p99,
                                thresholds={"age_crit": self.th.age_crit_s, "p99_crit": c},
                                intent="request_full_restart")
                any_crit = True
            elif age >= self.th.age_warn_s or p95 >= w:
                self.emit_event(type="health", level="WARN", message="slip_pair_warn",
                                pair=key, age_s=age, p95_bps=p95, p99_bps=p99,
                                thresholds={"age_warn": self.th.age_warn_s, "p95_warn": w})
                any_warn = True

        # 4) Persistance
        if any_warn or any_crit:
            self._bad_cycles += 1
        else:
            self._bad_cycles = 0

        if self._bad_cycles >= max(1, self.th.escalate_after_cycles):
            self.emit_event(type="alert", level="CRIT", message="slip_persistent_degradation",
                            cycles=self._bad_cycles, intent="request_full_restart")

        if self._bad_cycles == 0 and not any_warn and not any_crit:
            self.emit_event(type="health", level="INFO", message="slip_ok")

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
