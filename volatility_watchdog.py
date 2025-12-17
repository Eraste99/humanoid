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

from base_watchdog import BaseWatchdogV2

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
    ) -> None:
        cfg = config or VolatilityWatchdogConfig()
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
        g_p95 = float(g.get("p95_bps", 0.0))
        g_p99 = float(g.get("p99_bps", 0.0))
        g_ema = float(g.get("ema_bps", 0.0))
        g_cha = bool(g.get("chaos", False))

        soft = self.th.soft_cap_bps
        hard = self.th.hard_cap_bps

        if g_age >= self.th.age_crit_s or g_p99 >= max(self.th.p99_crit_bps, hard):
            self.emit_event(type="alert", level="CRIT", message="vol_global_crit",
                            age_s=g_age, p95_bps=g_p95, p99_bps=g_p99, ema_bps=g_ema, chaos=g_cha,
                            thresholds={"age_crit": self.th.age_crit_s, "p99_crit": self.th.p99_crit_bps, "hard": hard},
                            intent="request_full_restart")
            any_crit = True
        elif g_age >= self.th.age_warn_s or g_p95 >= max(self.th.p95_warn_bps, soft) or g_cha:
            self.emit_event(type="health", level="WARN", message="vol_global_warn",
                            age_s=g_age, p95_bps=g_p95, p99_bps=g_p99, ema_bps=g_ema, chaos=g_cha,
                            thresholds={"age_warn": self.th.age_warn_s, "p95_warn": self.th.p95_warn_bps, "soft": soft})
            any_warn = True

        # 2) Par exchange
        for ex, m in exmap.items():
            age = float((m or {}).get("age_s", 0.0))
            p95 = float((m or {}).get("p95_bps", 0.0))
            p99 = float((m or {}).get("p99_bps", 0.0))
            ema = float((m or {}).get("ema_bps", 0.0))
            chaos = bool((m or {}).get("chaos", False))
            soft_e, hard_e = self._caps_for_exchange(ex, soft, hard)

            if age >= self.th.age_crit_s or p99 >= max(self.th.p99_crit_bps, hard_e):
                self.emit_event(type="alert", level="CRIT", message="vol_exchange_crit",
                                exchange=ex, age_s=age, p95_bps=p95, p99_bps=p99, ema_bps=ema, chaos=chaos,
                                thresholds={"age_crit": self.th.age_crit_s, "p99_crit": self.th.p99_crit_bps, "hard": hard_e},
                                intent="request_full_restart")
                any_crit = True
            elif age >= self.th.age_warn_s or p95 >= max(self.th.p95_warn_bps, soft_e) or chaos:
                self.emit_event(type="health", level="WARN", message="vol_exchange_warn",
                                exchange=ex, age_s=age, p95_bps=p95, p99_bps=p99, ema_bps=ema, chaos=chaos,
                                thresholds={"age_warn": self.th.age_warn_s, "p95_warn": self.th.p95_warn_bps, "soft": soft_e})
                any_warn = True

        # 3) Par pair
        for pair, m in pairs.items():
            age = float((m or {}).get("age_s", 0.0))
            p95 = float((m or {}).get("p95_bps", 0.0))
            p99 = float((m or {}).get("p99_bps", 0.0))
            ema = float((m or {}).get("ema_bps", 0.0))
            z   = float((m or {}).get("zscore", 0.0))
            soft_p, hard_p = self._caps_for_pair(pair, soft, hard)

            # Spike detection: zscore si dispo sinon p99>>ema
            spike_warn = (z >= self.th.z_warn) or (ema > 0 and (p99 - ema) >= self.th.z_warn * (ema/10.0))
            spike_crit = (z >= self.th.z_crit) or (ema > 0 and (p99 - ema) >= self.th.z_crit * (ema/10.0))

            if age >= self.th.age_crit_s or p99 >= max(self.th.p99_crit_bps, hard_p) or spike_crit:
                self.emit_event(type="alert", level="CRIT", message="vol_pair_crit",
                                pair=pair, age_s=age, p95_bps=p95, p99_bps=p99, ema_bps=ema, zscore=z,
                                thresholds={"age_crit": self.th.age_crit_s, "p99_crit": self.th.p99_crit_bps, "hard": hard_p, "z_crit": self.th.z_crit},
                                intent="request_full_restart")
                any_crit = True
            elif age >= self.th.age_warn_s or p95 >= max(self.th.p95_warn_bps, soft_p) or spike_warn:
                self.emit_event(type="health", level="WARN", message="vol_pair_warn",
                                pair=pair, age_s=age, p95_bps=p95, p99_bps=p99, ema_bps=ema, zscore=z,
                                thresholds={"age_warn": self.th.age_warn_s, "p95_warn": self.th.p95_warn_bps, "soft": soft_p, "z_warn": self.th.z_warn})
                any_warn = True

        # 4) Persistance
        if any_warn or any_crit:
            self._bad_cycles += 1
        else:
            self._bad_cycles = 0

        if self._bad_cycles >= max(1, self.th.escalate_after_cycles):
            self.emit_event(type="alert", level="CRIT", message="vol_persistent_degradation",
                            cycles=self._bad_cycles, intent="request_full_restart")

        if self._bad_cycles == 0 and not any_warn and not any_crit:
            self.emit_event(type="health", level="INFO", message="vol_ok")

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
