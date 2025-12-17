# -*- coding: utf-8 -*-
"""
RiskManagerWatchdog — version "ceinture" notify-only (v3)
=========================================================

Objectif
--------
Surveiller le RiskManager **sans aucune action locale** et produire des évènements
standardisés (health/alert). Aucune mutation, aucun restart — uniquement notify.

Entrée (`state_fn`)
-------------------
`state = await state_fn()` (ou sync) → dict suggéré (tolérant aux clés manquantes):
{
  "now_ts": float,
  "loop": {"tick_ms_p95": float, "heartbeat_gap_s": float},
  "mode": "NORMAL"|"OPP_VOLUME"|"OPP_VOLATILITE"|"SEVERE",
  "pacer": "NORMAL"|"CONSTRAINED"|"SEVERE",
  "gates": {
      "vol_chaos": bool, "shallow_book": bool, "min_notional": bool,
      "queuepos_guard_hits_per_min": float
  },
  "hints": {"shadow_bias_bps_p50": float, "slip_age_s": float, "vol_age_s": float},
  "admission": {"rate_admit": float, "rate_reject": float, "pending_backlog": int}
}

Notes
-----
- Les seuils suivent la feuille de route RM modes spéciaux (P0/P1) et SLO TTL slip/vol.
- Escalade CRIT quand dérive persiste N cycles (escalate_after_cycles).
- Tous les CRIT incluent intent="request_full_restart" (orchestration par CW/pod supervisor).
"""
from __future__ import annotations
import asyncio, time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional

from base_watchdog import BaseWatchdogV2

StateFn = Callable[[], Dict[str, Any] | Awaitable[Dict[str, Any]]]


@dataclass
class RmThresholds:
    # Boucle RM
    tick_warn_ms: int = 10
    tick_crit_ms: int = 20
    hb_warn_s: int = 5
    hb_crit_s: int = 15
    # Qualité signaux
    slip_age_warn_s: float = 2.0
    slip_age_crit_s: float = 3.0
    vol_age_warn_s: float = 4.0
    vol_age_crit_s: float = 6.0
    shadow_bias_warn_bps: float = 3.0
    shadow_bias_crit_bps: float = 5.0
    # Garde queue position
    queuepos_warn_per_min: float = 3.0
    queuepos_crit_per_min: float = 8.0
    # Durée en mode SEVERE
    severe_warn_s: int = 120   # 2 min
    severe_crit_s: int = 600   # 10 min
    # Escalade persistance
    escalate_after_cycles: int = 3


@dataclass
class RiskManagerWatchdogConfig:
    check_interval: float = 2.0
    thresholds: RmThresholds = field(default_factory=RmThresholds)


class RiskManagerWatchdog(BaseWatchdogV2):
    """Watchdog RiskManager passif (notify-only)."""

    def __init__(
        self,
        state_fn: StateFn,
        *,
        name: str = "RiskManagerWatchdog",
        config: Optional[RiskManagerWatchdogConfig] = None,
        notify_only: bool = True,
        verbose: bool = True,
    ) -> None:
        cfg = config or RiskManagerWatchdogConfig()
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
        self._severe_since_ts: float = 0.0

    async def _check_once(self) -> None:
        s = await self._get_state()
        now = float(s.get("now_ts", time.time()))

        loop = s.get("loop", {}) or {}
        tick_p95 = float(loop.get("tick_ms_p95", 0.0))
        hb_gap = float(loop.get("heartbeat_gap_s", 0.0))

        mode = str(s.get("mode", "")).upper() or "UNKNOWN"
        pacer = str(s.get("pacer", "")).upper() or "UNKNOWN"
        gates = s.get("gates", {}) or {}
        hints = s.get("hints", {}) or {}
        adm = s.get("admission", {}) or {}

        any_warn = False
        any_crit = False

        # 1) Boucle & heartbeats
        if tick_p95 >= self.th.tick_crit_ms or hb_gap >= self.th.hb_crit_s:
            self.emit_event(type="alert", level="CRIT", message="rm_loop_crit",
                            tick_ms_p95=int(tick_p95), hb_gap_s=int(hb_gap),
                            thresholds={"tick_crit": self.th.tick_crit_ms, "hb_crit": self.th.hb_crit_s},
                            intent="request_full_restart")
            any_crit = True
        elif tick_p95 >= self.th.tick_warn_ms or hb_gap >= self.th.hb_warn_s:
            self.emit_event(type="health", level="WARN", message="rm_loop_warn",
                            tick_ms_p95=int(tick_p95), hb_gap_s=int(hb_gap),
                            thresholds={"tick_warn": self.th.tick_warn_ms, "hb_warn": self.th.hb_warn_s})
            any_warn = True

        # 2) Qualité slip/vol & shadow bias
        slip_age = float(hints.get("slip_age_s", 0.0))
        vol_age = float(hints.get("vol_age_s", 0.0))
        shadow = float(hints.get("shadow_bias_bps_p50", 0.0))
        if slip_age >= self.th.slip_age_crit_s or vol_age >= self.th.vol_age_crit_s or shadow >= self.th.shadow_bias_crit_bps:
            self.emit_event(type="alert", level="CRIT", message="rm_inputs_crit",
                            slip_age_s=slip_age, vol_age_s=vol_age, shadow_bias_bps_p50=shadow,
                            thresholds={"slip_crit": self.th.slip_age_crit_s, "vol_crit": self.th.vol_age_crit_s,
                                        "shadow_crit": self.th.shadow_bias_crit_bps},
                            intent="request_full_restart")
            any_crit = True
        elif slip_age >= self.th.slip_age_warn_s or vol_age >= self.th.vol_age_warn_s or shadow >= self.th.shadow_bias_warn_bps:
            self.emit_event(type="health", level="WARN", message="rm_inputs_warn",
                            slip_age_s=slip_age, vol_age_s=vol_age, shadow_bias_bps_p50=shadow,
                            thresholds={"slip_warn": self.th.slip_age_warn_s, "vol_warn": self.th.vol_age_warn_s,
                                        "shadow_warn": self.th.shadow_bias_warn_bps})
            any_warn = True

        # 3) Queue-position guard hits
        qpos = float(gates.get("queuepos_guard_hits_per_min", 0.0))
        if qpos >= self.th.queuepos_crit_per_min:
            self.emit_event(type="alert", level="CRIT", message="rm_queuepos_guard_crit",
                            hits_per_min=qpos, thresholds={"crit": self.th.queuepos_crit_per_min},
                            intent="request_full_restart")
            any_crit = True
        elif qpos >= self.th.queuepos_warn_per_min:
            self.emit_event(type="health", level="WARN", message="rm_queuepos_guard_warn",
                            hits_per_min=qpos, thresholds={"warn": self.th.queuepos_warn_per_min})
            any_warn = True

        # 4) Mode SEVERE durée
        if mode == "SEVERE":
            if self._severe_since_ts <= 0:
                self._severe_since_ts = now
            spent = now - self._severe_since_ts
            if spent >= self.th.severe_crit_s:
                self.emit_event(type="alert", level="CRIT", message="rm_severe_persistent",
                                mode=mode, seconds=int(spent), thresholds={"crit": self.th.severe_crit_s},
                                intent="request_full_restart")
                any_crit = True
            elif spent >= self.th.severe_warn_s:
                self.emit_event(type="health", level="WARN", message="rm_severe_ongoing",
                                mode=mode, seconds=int(spent), thresholds={"warn": self.th.severe_warn_s})
                any_warn = True
        else:
            self._severe_since_ts = 0.0

        # 5) Escalade globale par persistance (hystérésis cycles)
        if any_warn or any_crit:
            self._bad_cycles += 1
        else:
            self._bad_cycles = 0

        if self._bad_cycles >= max(1, self.th.escalate_after_cycles):
            self.emit_event(type="alert", level="CRIT", message="rm_persistent_degradation",
                            cycles=self._bad_cycles, mode=mode, pacer=pacer, intent="request_full_restart")

        # vert
        if self._bad_cycles == 0 and not any_warn and not any_crit:
            self.emit_event(type="health", level="INFO", message="rm_ok", mode=mode, pacer=pacer)

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
            "module": "RiskManagerWatchdog",
            "metrics": {**s.get("metrics", {}),
                         "bad_cycles": self._bad_cycles,
                         "severe_since_ts": self._severe_since_ts,
                         "tick_warn_ms": self.th.tick_warn_ms, "tick_crit_ms": self.th.tick_crit_ms,
                         "hb_warn_s": self.th.hb_warn_s, "hb_crit_s": self.th.hb_crit_s,
                         "slip_age_warn_s": self.th.slip_age_warn_s, "slip_age_crit_s": self.th.slip_age_crit_s,
                         "vol_age_warn_s": self.th.vol_age_warn_s, "vol_age_crit_s": self.th.vol_age_crit_s,
                         "shadow_warn_bps": self.th.shadow_bias_warn_bps, "shadow_crit_bps": self.th.shadow_bias_crit_bps,
                         "queuepos_warn_per_min": self.th.queuepos_warn_per_min,
                         "queuepos_crit_per_min": self.th.queuepos_crit_per_min,
                         "severe_warn_s": self.th.severe_warn_s, "severe_crit_s": self.th.severe_crit_s},
        })
        return s
