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
from typing import Any, Awaitable, Callable, Dict, Optional,Tuple

from modules.watchdog.base_watchdog import BaseWatchdogV2
from modules.bot_config import BotConfig

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
        state_fn: Optional[StateFn] = None,
        *,
        name: str = "OpportunityWatchdog",
        config: Optional[OpportunityWatchdogConfig] = None,
        notify_only: bool = True,
        verbose: bool = True,
        bot_config: Optional[BotConfig] = None,
    ) -> None:
        bot_cfg = bot_config or BotConfig.from_env()
        if bot_config is None:
            self.record_fallback("bot_config")
        th = OppThresholds(
            effective_ratio_warn=bot_cfg.wd.opportunity_effective_ratio_warn,
            effective_ratio_crit=bot_cfg.wd.opportunity_effective_ratio_crit,
            decision_p95_warn_ms=bot_cfg.wd.opportunity_decision_p95_warn_ms,
            decision_p95_crit_ms=bot_cfg.wd.opportunity_decision_p95_crit_ms,
            emit_p95_warn_ms=bot_cfg.wd.opportunity_emit_p95_warn_ms,
            emit_p95_crit_ms=bot_cfg.wd.opportunity_emit_p95_crit_ms,
            backlog_warn_ratio=bot_cfg.wd.opportunity_backlog_warn_ratio,
            backlog_crit_ratio=bot_cfg.wd.opportunity_backlog_crit_ratio,
            dedup_warn_per_min=bot_cfg.wd.opportunity_dedup_warn_per_min,
            dedup_crit_per_min=bot_cfg.wd.opportunity_dedup_crit_per_min,
            rejection_ratio_warn=bot_cfg.wd.opportunity_rejection_ratio_warn,
            rejection_ratio_crit=bot_cfg.wd.opportunity_rejection_ratio_crit,
            slip_age_warn_s=bot_cfg.wd.opportunity_slip_age_warn_s,
            slip_age_crit_s=bot_cfg.wd.opportunity_slip_age_crit_s,
            vol_age_warn_s=bot_cfg.wd.opportunity_vol_age_warn_s,
            vol_age_crit_s=bot_cfg.wd.opportunity_vol_age_crit_s,
            fees_age_warn_s=bot_cfg.wd.opportunity_fees_age_warn_s,
            fees_age_crit_s=bot_cfg.wd.opportunity_fees_age_crit_s,
            scanner_err_warn_per_min=bot_cfg.wd.opportunity_scanner_err_warn_per_min,
            scanner_err_crit_per_min=bot_cfg.wd.opportunity_scanner_err_crit_per_min,
            escalate_after_cycles=bot_cfg.wd.opportunity_escalate_after_cycles,
        )
        cfg = config or OpportunityWatchdogConfig(check_interval=bot_cfg.wd.opportunity_interval_s, thresholds=th)
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
            component="OpportunityScanner",
            module="OpportunityScanner",
            observed_at_ms=snapshot.get("observed_at_ms"),
        )

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
        load = self.safe_get(s, "load", default={}, missing=missing)
        lat = self.safe_get(s, "latency", default={}, missing=missing)
        emi = self.safe_get(s, "emissions", default={}, missing=missing)
        bkl = self.safe_get(s, "backlog", default={}, missing=missing)
        hnt = self.safe_get(s, "hints", default={}, missing=missing)
        err = self.safe_get(s, "errors", default={}, missing=missing)
        if not isinstance(load, dict):
            load = {}
        if not isinstance(lat, dict):
            lat = {}
        if not isinstance(emi, dict):
            emi = {}
        if not isinstance(bkl, dict):
            bkl = {}
        if not isinstance(hnt, dict):
            hnt = {}
        if not isinstance(err, dict):
            err = {}
        reasons: list[str] = []
        details: Dict[str, Any] = {}


        any_warn = False
        any_crit = False

        tgt = self.safe_float(load, "target_hz", default=0.0, missing=missing)
        eff = self.safe_float(load, "effective_hz", default=0.0, missing=missing)
        ratio = (eff / tgt) if tgt > 0 else 1.0
        if tgt > 0:
            if ratio <= self.th.effective_ratio_crit:
                reasons.append("WD_LOOP_STOPPED")
                any_crit = True
            elif ratio <= self.th.effective_ratio_warn:
                any_warn = True
        else:
            missing.append("MISSING_FIELD:target_hz")

        d95 = self.safe_float(lat, "decision_p95_ms", default=0.0, missing=missing)
        e95 = self.safe_float(lat, "emit_p95_ms", default=0.0, missing=missing)
        if d95 >= self.th.decision_p95_crit_ms or e95 >= self.th.emit_p95_crit_ms:
            reasons.append("WD_LOOP_STOPPED")
            any_crit = True
        elif d95 >= self.th.decision_p95_warn_ms or e95 >= self.th.emit_p95_warn_ms:
            any_warn = True

        for tier, meta in bkl.items():
            meta = meta or {}
            d = self.safe_int(meta, "depth", default=0, missing=missing)
            m = self.safe_int(meta, "max", default=1, missing=missing) or 1
            r = d / float(m)
            if r >= self.th.backlog_crit_ratio:
                reasons.append("WD_QUEUE_BACKLOG")
                any_crit = True
            elif r >= self.th.backlog_warn_ratio:
                any_warn = True

        em = self.safe_float(emi, "emitted_per_min", default=0.0, missing=missing)
        rj = self.safe_float(emi, "rejected_per_min", default=0.0, missing=missing)
        dd = self.safe_float(emi, "dedup_hits_per_min", default=0.0, missing=missing)
        tot = em + rj
        rej_ratio = (rj / tot) if tot > 0 else 0.0

        if dd >= self.th.dedup_crit_per_min:
            reasons.append("WD_LOOP_STOPPED")
            any_crit = True
        elif dd >= self.th.dedup_warn_per_min:
            any_warn = True

        if rej_ratio >= self.th.rejection_ratio_crit and tot > 0:
            reasons.append("WD_LOOP_STOPPED")
            any_crit = True
        elif rej_ratio >= self.th.rejection_ratio_warn and tot > 0:
            any_warn = True

        slip_age = self.safe_float(hnt, "slip_age_s", default=0.0, missing=missing)
        vol_age = self.safe_float(hnt, "vol_age_s", default=0.0, missing=missing)
        fees_age = self.safe_float(hnt, "fees_age_s", default=0.0, missing=missing)
        if slip_age >= self.th.slip_age_crit_s or vol_age >= self.th.vol_age_crit_s or fees_age >= self.th.fees_age_crit_s:
            reasons.append("WD_STALE")
            any_crit = True
        elif slip_age >= self.th.slip_age_warn_s or vol_age >= self.th.vol_age_warn_s or fees_age >= self.th.fees_age_warn_s:
            any_warn = True

        se = self.safe_float(err, "scanner_errors_per_min", default=0.0, missing=missing)
        if se >= self.th.scanner_err_crit_per_min:
            reasons.append("WD_LOOP_STOPPED")
            any_crit = True
        elif se >= self.th.scanner_err_warn_per_min:
            any_warn = True

        if any_warn or any_crit:
            self._bad_cycles += 1
        else:
            self._bad_cycles = 0

        if self._bad_cycles >= max(1, self.th.escalate_after_cycles):
            reasons.append("WD_LOOP_STOPPED")
            any_crit = True

        if missing:
            reasons.append("MISSING_FIELD")
            details["missing_fields"] = missing

        details["bad_cycles"] = self._bad_cycles
        severity = "OK"
        if any_crit:
            severity = "CRIT"
        elif any_warn or reasons:
            severity = "WARN"
        return severity, reasons, details

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
