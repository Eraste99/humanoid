# -*- coding: utf-8 -*-
"""
PrivateWSHubWatchdog — version "ceinture" notify-only (v3)
==========================================================

Objectif
--------
Surveiller le Hub WebSocket privé (acks/fills) **sans aucune action locale** :
- Aucune tentative de restart; uniquement des événements (health/alert).
- Seuils adaptés par mode (EU_ONLY/SPLIT) et par exchange si overrides fournis.
- Escalade CRIT → `intent="request_full_restart"`.

Entrée attendue (`state_fn`)
----------------------------
`state = await state_fn()` (ou sync) → dict par ex.:
{
  "now_ts": float,
  "global": {"hb_gap_s": float, "acks_p95_ms": float, "fills_p95_ms": float, "rate_429": float},
  "streams": {
      "BINANCE:TT": {"hb_gap_s": float, "acks_p95_ms": float, "fills_p95_ms": float,
                      "queue_depth": int, "queue_max": int, "dedup_hit_rate": float, "rate_429": float},
      ...
  }
}

Notes: champs optionnels tolérés; manquants ⇒ health INFO.
"""
from __future__ import annotations
import asyncio, time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from modules.watchdog.base_watchdog import BaseWatchdogV2
from modules.bot_config import BotConfig

StateFn = Callable[[], Dict[str, Any] | Awaitable[Dict[str, Any]]]


@dataclass
class PwsThresholds:
    # Heartbeat
    hb_warn_s: int = 20
    hb_crit_s: int = 35
    # Latences acks/fills (p95)
    ack_warn_ms: int = 150
    ack_crit_ms: int = 220
    fill_warn_ms: int = 200
    fill_crit_ms: int = 280
    # Saturation des files
    q_warn_ratio: float = 0.70
    q_crit_ratio: float = 0.90
    # 429
    rate429_warn: float = 0.001   # 0.1 %
    rate429_crit: float = 0.002   # 0.2 %
    # dedup anomalies (ratio de hits sur 1.0)
    dedup_warn: float = 0.20
    dedup_crit: float = 0.35
    # Escalade CRIT globale si hb/acks/fills lents persistent
    escalate_after_cycles: int = 3


@dataclass
class PwsModeTuning:
    mode: str = "EU_ONLY"
    split_hb_factor: float = 1.15
    split_ms_factor: float = 1.20

    def apply(self, t: PwsThresholds) -> PwsThresholds:
        if str(self.mode).upper() != "SPLIT":
            return t
        return PwsThresholds(
            hb_warn_s=int(t.hb_warn_s * self.split_hb_factor),
            hb_crit_s=int(t.hb_crit_s * self.split_hb_factor),
            ack_warn_ms=int(t.ack_warn_ms * self.split_ms_factor),
            ack_crit_ms=int(t.ack_crit_ms * self.split_ms_factor),
            fill_warn_ms=int(t.fill_warn_ms * self.split_ms_factor),
            fill_crit_ms=int(t.fill_crit_ms * self.split_ms_factor),
            q_warn_ratio=t.q_warn_ratio,
            q_crit_ratio=t.q_crit_ratio,
            rate429_warn=t.rate429_warn,
            rate429_crit=t.rate429_crit,
            dedup_warn=t.dedup_warn,
            dedup_crit=t.dedup_crit,
            escalate_after_cycles=t.escalate_after_cycles,
        )


@dataclass
class PrivateWSWatchdogConfig:
    check_interval: float = 2.0
    thresholds: PwsThresholds = field(default_factory=PwsThresholds)
    tuning: PwsModeTuning = field(default_factory=PwsModeTuning)
    # Overrides par exchange: {"BINANCE": {"ack_warn_ms":120, "ack_crit_ms":180, ...}}
    per_exchange_overrides: Dict[str, Dict[str, int | float]] = field(default_factory=dict)


class PrivateWSHubWatchdog(BaseWatchdogV2):
    """Watchdog PrivateWS passif (notify-only)."""

    def __init__(
        self,
        state_fn: Optional[StateFn] = None,
        *,
        hub: Optional[Any] = None,
        name: str = "PrivateWSHubWatchdog",
        config: Optional[PrivateWSWatchdogConfig] = None,
        notify_only: bool = True,
        verbose: bool = True,
        bot_config: Optional[BotConfig] = None,

    ) -> None:
        bot_cfg = bot_config or BotConfig.from_env()
        if bot_config is None:
            self.record_fallback("bot_config")
        hb_crit_s = bot_cfg.wd.private_ws_hb_crit_s
        if bot_cfg.wd.private_ws_stale_ms:
            hb_crit_s = max(hb_crit_s, int(bot_cfg.wd.private_ws_stale_ms / 1000))
        th = PwsThresholds(
            hb_warn_s=bot_cfg.wd.private_ws_hb_warn_s,
            hb_crit_s=hb_crit_s,
            ack_warn_ms=bot_cfg.wd.private_ws_ack_warn_ms,
            ack_crit_ms=bot_cfg.wd.private_ws_ack_crit_ms,
            fill_warn_ms=bot_cfg.wd.private_ws_fill_warn_ms,
            fill_crit_ms=bot_cfg.wd.private_ws_fill_crit_ms,
            q_warn_ratio=bot_cfg.wd.private_ws_queue_warn_ratio,
            q_crit_ratio=bot_cfg.wd.private_ws_queue_crit_ratio,
            rate429_warn=bot_cfg.wd.private_ws_rate429_warn,
            rate429_crit=bot_cfg.wd.private_ws_rate429_crit,
            dedup_warn=bot_cfg.wd.private_ws_dedup_warn,
            dedup_crit=bot_cfg.wd.private_ws_dedup_crit,
            escalate_after_cycles=bot_cfg.wd.private_ws_escalate_after_cycles,
        )
        cfg = config or PrivateWSWatchdogConfig(check_interval=bot_cfg.wd.private_ws_interval_s, thresholds=th)
        if config is None:
            cfg.tuning.mode = str(getattr(bot_cfg.g, "deployment_mode", cfg.tuning.mode))
        applied = cfg.tuning.apply(cfg.thresholds)
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
        self.th = applied
        self.state_fn = state_fn
        self.hub = hub
        self._global_bad_cycles: int = 0

    async def _check_once(self) -> None:
        snapshot = await self.collect_snapshot()
        severity, reasons, details = self.evaluate(snapshot)
        self.emit_health_event(
            severity=severity,
            reasons=reasons,
            details=details,
            component="PrivateWS",
            module="PrivateWS",
            observed_at_ms=snapshot.get("observed_at_ms"),
        )

        # ---- helpers ----

    def _per_exchange_ms(self, ex: str) -> Tuple[int, int, int, int]:
        ex = (ex or "").upper()
        o = self.cfg.per_exchange_overrides.get(ex, {})
        ack_warn = int(o.get("ack_warn_ms", self.th.ack_warn_ms))
        ack_crit = int(o.get("ack_crit_ms", self.th.ack_crit_ms))
        fill_warn = int(o.get("fill_warn_ms", self.th.fill_warn_ms))
        fill_crit = int(o.get("fill_crit_ms", self.th.fill_crit_ms))
        return ack_warn, ack_crit, fill_warn, fill_crit

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
        if not state and self.hub is not None:
            state = await self.safe_call(getattr(self.hub, "get_status", None), default={}, errors=errors,
                                         error_label="hub.get_status")
        if not state:
            missing.append("MISSING_FIELD:get_status")
        return {
            "observed_at_ms": observed_at_ms,
            "module_state": state or {},
            "missing": missing,
            "errors": errors,
        }

    def evaluate(self, snapshot: Dict[str, Any]) -> Tuple[str, list[str], Dict[str, Any]]:
        state = snapshot.get("module_state", {}) or {}
        missing = list(snapshot.get("missing") or [])
        reasons: list[str] = []
        details: Dict[str, Any] = {}
        global_m = self.safe_get(state, "global", default={}, missing=missing)
        streams = self.safe_get(state, "streams", default={}, missing=missing)
        if not isinstance(global_m, dict):
            global_m = {}
        if not isinstance(streams, dict):
            streams = {}

        any_warn = False
        any_crit = False

        g_hb = self.safe_float(global_m, "hb_gap_s", default=0.0, missing=missing)
        g_ack = self.safe_float(global_m, "acks_p95_ms", default=0.0, missing=missing)
        g_fill = self.safe_float(global_m, "fills_p95_ms", default=0.0, missing=missing)
        g_429 = self.safe_float(global_m, "rate_429", default=0.0, missing=missing)


        if g_hb >= self.th.hb_crit_s or g_ack >= self.th.ack_crit_ms or g_fill >= self.th.fill_crit_ms or g_429 >= self.th.rate429_crit:
                reasons.append("PRIVATE_WS_STALE")
                any_crit = True
        elif g_hb >= self.th.hb_warn_s or g_ack >= self.th.ack_warn_ms or g_fill >= self.th.fill_warn_ms or g_429 >= self.th.rate429_warn:\
                    any_warn = True


        for key, info in streams.items():
            ex = (key.split(":", 1)[0] if ":" in key else key).upper()
            info = info or {}
            hb = self.safe_float(info, "hb_gap_s", default=0.0, missing=missing)
            ack = self.safe_float(info, "acks_p95_ms", default=0.0, missing=missing)
            fill = self.safe_float(info, "fills_p95_ms", default=0.0, missing=missing)
            qd = self.safe_int(info, "queue_depth", default=0, missing=missing)
            qmax = self.safe_int(info, "queue_max", default=1, missing=missing) or 1
            ratio = qd / float(qmax)
            dedup = self.safe_float(info, "dedup_hit_rate", default=0.0, missing=missing)
            r429 = self.safe_float(info, "rate_429", default=0.0, missing=missing)
            ack_warn, ack_crit, fill_warn, fill_crit = self._per_exchange_ms(ex)


            if hb >= self.th.hb_crit_s or ack >= ack_crit or fill >= fill_crit or ratio >= self.th.q_crit_ratio or r429 >= self.th.rate429_crit or dedup >= self.th.dedup_crit:
                reasons.append("PRIVATE_WS_STALE")
                any_crit = True
            elif hb >= self.th.hb_warn_s or ack >= ack_warn or fill >= fill_warn or ratio >= self.th.q_warn_ratio or r429 >= self.th.rate429_warn or dedup >= self.th.dedup_warn:
                any_warn = True


        if any_crit or any_warn:
            self._global_bad_cycles += 1
        else:
            self._global_bad_cycles = 0

        if self._global_bad_cycles >= max(1, self.th.escalate_after_cycles):
            reasons.append("PRIVATE_WS_STALE")
            any_crit = True

        if missing:
            reasons.append("MISSING_FIELD")
            details["missing_fields"] = missing

        details["bad_cycles"] = self._global_bad_cycles
        severity = "OK"
        if any_crit or "PRIVATE_WS_STALE" in reasons:
            severity = "CRIT"
        elif any_warn or reasons:
            severity = "WARN"
        return severity, reasons, details

    def get_status(self) -> Dict[str, Any]:
        s = super().get_status()
        s.update({
            "module": "PrivateWSHubWatchdog",
            "metrics": {**s.get("metrics", {}),
                         "global_bad_cycles": self._global_bad_cycles,
                         "hb_warn_s": self.th.hb_warn_s, "hb_crit_s": self.th.hb_crit_s,
                         "ack_warn_ms": self.th.ack_warn_ms, "ack_crit_ms": self.th.ack_crit_ms,
                         "fill_warn_ms": self.th.fill_warn_ms, "fill_crit_ms": self.th.fill_crit_ms,
                         "q_warn_ratio": self.th.q_warn_ratio, "q_crit_ratio": self.th.q_crit_ratio,
                         "rate429_warn": self.th.rate429_warn, "rate429_crit": self.th.rate429_crit,
                         "dedup_warn": self.th.dedup_warn, "dedup_crit": self.th.dedup_crit},
        })
        return s
