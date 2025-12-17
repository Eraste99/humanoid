# -*- coding: utf-8 -*-
"""
OpportunityWatchdog — version "ceinture" notify-only (v3)
=========================================================

Objectif
--------
Surveiller l'Opportunity Scanner **sans aucune action locale** :
- Pas de restart/apply; uniquement des évènements `health/alert` normalisés.
- Hystérésis via cycles persistants; CRIT → intent="request_full_restart".
- Tolérant aux clés manquantes dans le snapshot.

Entrée (`state_fn`)
-------------------
`state = await state_fn()` (ou sync) → dict **suggéré**:
{
  "now_ts": float,
  "load": {"target_hz": float, "effective_hz": float},
  "latency": {"decision_p95_ms": float, "emit_p95_ms": float},
  "emissions": {
      "emitted_per_min": float,
      "rejected_per_min": float,
      "rejection_reasons": {"risk_gate": float, "too_old": float, "dedup": float, ...},
      "dedup_hits_per_min": float
  },
  "backlog": {"CORE": {"depth": int, "max": int}, "PRIMARY": {...}, "AUDITION": {...}},
  "hints": {"slip_age_s": float, "vol_age_s": float, "fees_age_s": float},
  "errors": {"scanner_errors_per_min": float}
}

Notes
-----
- Les clés peuvent manquer : on n'émet alors qu'un `health` INFO.
- Les seuils sont "mode-agnostic" ici; si besoin, instancier avec un config spécifique.
"""
from __future__ import annotations
import asyncio, time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional

from base_watchdog import BaseWatchdogV2

StateFn = Callable[[], Dict[str, Any] | Awaitable[Dict[str, Any]]]


@dataclass
class OppThresholds:
    # Charge & cadence
    effective_ratio_warn: float = 0.70   # effective_hz / target_hz
    effective_ratio_crit: float = 0.40
    # Latence de décision / émission
    decision_p95_warn_ms: int = 8
    decision_p95_crit_ms: int = 15
    emit_p95_warn_ms: int = 12
    emit_p95_crit_ms: int = 25
    # Backlog ratios
    backlog_warn_ratio: float = 0.80
    backlog_crit_ratio: float = 0.95
    # Dédup / rejets
    dedup_warn_per_min: float = 5.0
    dedup_crit_per_min: float = 15.0
    rejection_ratio_warn: float = 0.60   # rejected / (emitted+rejected)
    rejection_ratio_crit: float = 0.80
    # Hints fraîcheur
    slip_age_warn_s: float = 2.0
    slip_age_crit_s: float = 3.0
    vol_age_warn_s: float = 4.0
    vol_age_crit_s: float = 6.0
    fees_age_warn_s: float = 120.0
    fees_age_crit_s: float = 300.0
    # Erreurs scanner
    scanner_err_warn_per_min: float = 0.5
    scanner_err_crit_per_min: float = 2.0
    # Persistance
    escalate_after_cycles: int = 3


@dataclass
class OpportunityWatchdogConfig:
    check_interval: float = 2.0
    thresholds: OppThresholds = field(default_factory=OppThresholds)


class OpportunityWatchdog(BaseWatchdogV2):
    """Watchdog Opportunity (Scanner) — passif / notify-only."""

    def __init__(
        self,
        state_fn: StateFn,
        *,
        name: str = "OpportunityWatchdog",
        config: Optional[OpportunityWatchdogConfig] = None,
        notify_only: bool = True,
        verbose: bool = True,
    ) -> None:
        cfg = config or OpportunityWatchdogConfig()
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
        load = s.get("load", {}) or {}
        lat  = s.get("latency", {}) or {}
        emi  = s.get("emissions", {}) or {}
        bkl  = s.get("backlog", {}) or {}
        hnt  = s.get("hints", {}) or {}
        err  = s.get("errors", {}) or {}

        any_warn = False
        any_crit = False

        # 1) Charge & cadence
        tgt = float(load.get("target_hz", 0.0))
        eff = float(load.get("effective_hz", 0.0))
        ratio = (eff / tgt) if tgt > 0 else 1.0
        if tgt > 0:
            if ratio <= self.th.effective_ratio_crit:
                self.emit_event(type="alert", level="CRIT", message="opp_cadence_crit",
                                target_hz=tgt, effective_hz=eff, ratio=round(ratio,3),
                                thresholds={"crit": self.th.effective_ratio_crit}, intent="request_full_restart")
                any_crit = True
            elif ratio <= self.th.effective_ratio_warn:
                self.emit_event(type="health", level="WARN", message="opp_cadence_warn",
                                target_hz=tgt, effective_hz=eff, ratio=round(ratio,3),
                                thresholds={"warn": self.th.effective_ratio_warn})
                any_warn = True
        else:
            self.emit_event(type="health", level="INFO", message="opp_cadence_unset")

        # 2) Latences décision/émission
        d95 = float(lat.get("decision_p95_ms", 0.0))
        e95 = float(lat.get("emit_p95_ms", 0.0))
        if d95 >= self.th.decision_p95_crit_ms or e95 >= self.th.emit_p95_crit_ms:
            self.emit_event(type="alert", level="CRIT", message="opp_latency_crit",
                            decision_p95_ms=int(d95), emit_p95_ms=int(e95),
                            thresholds={"d95_crit": self.th.decision_p95_crit_ms, "e95_crit": self.th.emit_p95_crit_ms},
                            intent="request_full_restart")
            any_crit = True
        elif d95 >= self.th.decision_p95_warn_ms or e95 >= self.th.emit_p95_warn_ms:
            self.emit_event(type="health", level="WARN", message="opp_latency_warn",
                            decision_p95_ms=int(d95), emit_p95_ms=int(e95),
                            thresholds={"d95_warn": self.th.decision_p95_warn_ms, "e95_warn": self.th.emit_p95_warn_ms})
            any_warn = True

        # 3) Backlog par tier
        for tier, meta in bkl.items():
            d = int((meta or {}).get("depth", 0))
            m = int((meta or {}).get("max", 0) or 1)
            r = d / float(m)
            if r >= self.th.backlog_crit_ratio:
                self.emit_event(type="alert", level="CRIT", message="opp_backlog_crit",
                                tier=tier, depth=d, max=m, ratio=round(r,3),
                                thresholds={"crit_ratio": self.th.backlog_crit_ratio}, intent="request_full_restart")
                any_crit = True
            elif r >= self.th.backlog_warn_ratio:
                self.emit_event(type="health", level="WARN", message="opp_backlog_warn",
                                tier=tier, depth=d, max=m, ratio=round(r,3),
                                thresholds={"warn_ratio": self.th.backlog_warn_ratio})
                any_warn = True

        # 4) Dédup / rejets
        em = float(emi.get("emitted_per_min", 0.0))
        rj = float(emi.get("rejected_per_min", 0.0))
        dd = float(emi.get("dedup_hits_per_min", 0.0))
        tot = em + rj
        rej_ratio = (rj / tot) if tot > 0 else 0.0

        if dd >= self.th.dedup_crit_per_min:
            self.emit_event(type="alert", level="CRIT", message="opp_dedup_crit",
                            dedup_hits_per_min=dd, thresholds={"crit": self.th.dedup_crit_per_min},
                            intent="request_full_restart")
            any_crit = True
        elif dd >= self.th.dedup_warn_per_min:
            self.emit_event(type="health", level="WARN", message="opp_dedup_warn",
                            dedup_hits_per_min=dd, thresholds={"warn": self.th.dedup_warn_per_min})
            any_warn = True

        if rej_ratio >= self.th.rejection_ratio_crit and tot > 0:
            self.emit_event(type="alert", level="CRIT", message="opp_rejection_ratio_crit",
                            rejected_per_min=rj, emitted_per_min=em, rejection_ratio=round(rej_ratio,3),
                            thresholds={"crit": self.th.rejection_ratio_crit}, intent="request_full_restart")
            any_crit = True
        elif rej_ratio >= self.th.rejection_ratio_warn and tot > 0:
            self.emit_event(type="health", level="WARN", message="opp_rejection_ratio_warn",
                            rejected_per_min=rj, emitted_per_min=em, rejection_ratio=round(rej_ratio,3),
                            thresholds={"warn": self.th.rejection_ratio_warn})
            any_warn = True

        # 5) Hints fraîcheur
        slip_age = float(hnt.get("slip_age_s", 0.0))
        vol_age  = float(hnt.get("vol_age_s", 0.0))
        fees_age = float(hnt.get("fees_age_s", 0.0))
        if slip_age >= self.th.slip_age_crit_s or vol_age >= self.th.vol_age_crit_s or fees_age >= self.th.fees_age_crit_s:
            self.emit_event(type="alert", level="CRIT", message="opp_hints_crit",
                            slip_age_s=slip_age, vol_age_s=vol_age, fees_age_s=fees_age,
                            thresholds={"slip_crit": self.th.slip_age_crit_s, "vol_crit": self.th.vol_age_crit_s, "fees_crit": self.th.fees_age_crit_s},
                            intent="request_full_restart")
            any_crit = True
        elif slip_age >= self.th.slip_age_warn_s or vol_age >= self.th.vol_age_warn_s or fees_age >= self.th.fees_age_warn_s:
            self.emit_event(type="health", level="WARN", message="opp_hints_warn",
                            slip_age_s=slip_age, vol_age_s=vol_age, fees_age_s=fees_age,
                            thresholds={"slip_warn": self.th.slip_age_warn_s, "vol_warn": self.th.vol_age_warn_s, "fees_warn": self.th.fees_age_warn_s})
            any_warn = True

        # 6) Erreurs scanner
        se = float(err.get("scanner_errors_per_min", 0.0))
        if se >= self.th.scanner_err_crit_per_min:
            self.emit_event(type="alert", level="CRIT", message="opp_errors_crit",
                            scanner_errors_per_min=se, thresholds={"crit": self.th.scanner_err_crit_per_min},
                            intent="request_full_restart")
            any_crit = True
        elif se >= self.th.scanner_err_warn_per_min:
            self.emit_event(type="health", level="WARN", message="opp_errors_warn",
                            scanner_errors_per_min=se, thresholds={"warn": self.th.scanner_err_warn_per_min})
            any_warn = True

        # 7) Persistance
        if any_warn or any_crit:
            self._bad_cycles += 1
        else:
            self._bad_cycles = 0

        if self._bad_cycles >= max(1, self.th.escalate_after_cycles):
            self.emit_event(type="alert", level="CRIT", message="opp_persistent_degradation",
                            cycles=self._bad_cycles, intent="request_full_restart")

        if self._bad_cycles == 0 and not any_warn and not any_crit:
            self.emit_event(type="health", level="INFO", message="opp_ok")

    # ---- helpers ----
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
            "module": "OpportunityWatchdog",
            "metrics": {**s.get("metrics", {}),
                         "bad_cycles": self._bad_cycles,
                         "effective_ratio_warn": self.th.effective_ratio_warn,
                         "effective_ratio_crit": self.th.effective_ratio_crit,
                         "decision_p95_warn_ms": self.th.decision_p95_warn_ms,
                         "decision_p95_crit_ms": self.th.decision_p95_crit_ms,
                         "emit_p95_warn_ms": self.th.emit_p95_warn_ms,
                         "emit_p95_crit_ms": self.th.emit_p95_crit_ms,
                         "backlog_warn_ratio": self.th.backlog_warn_ratio,
                         "backlog_crit_ratio": self.th.backlog_crit_ratio,
                         "dedup_warn_per_min": self.th.dedup_warn_per_min,
                         "dedup_crit_per_min": self.th.dedup_crit_per_min,
                         "rejection_ratio_warn": self.th.rejection_ratio_warn,
                         "rejection_ratio_crit": self.th.rejection_ratio_crit,
                         "slip_age_warn_s": self.th.slip_age_warn_s,
                         "slip_age_crit_s": self.th.slip_age_crit_s,
                         "vol_age_warn_s": self.th.vol_age_warn_s,
                         "vol_age_crit_s": self.th.vol_age_crit_s,
                         "fees_age_warn_s": self.th.fees_age_warn_s,
                         "fees_age_crit_s": self.th.fees_age_crit_s,
                         "scanner_err_warn_per_min": self.th.scanner_err_warn_per_min,
                         "scanner_err_crit_per_min": self.th.scanner_err_crit_per_min},
        })
        return s
