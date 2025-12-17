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

from base_watchdog import BaseWatchdogV2

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
        state_fn: StateFn,
        *,
        name: str = "PrivateWSHubWatchdog",
        config: Optional[PrivateWSWatchdogConfig] = None,
        notify_only: bool = True,
        verbose: bool = True,
    ) -> None:
        cfg = config or PrivateWSWatchdogConfig()
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
        self._global_bad_cycles: int = 0

    async def _check_once(self) -> None:
        state = await self._get_state()
        now = float(state.get("now_ts", time.time()))
        global_m = state.get("global", {}) or {}
        streams = state.get("streams", {}) or {}

        # 1) Global
        g_hb = float(global_m.get("hb_gap_s", 0.0))
        g_ack = float(global_m.get("acks_p95_ms", 0.0))
        g_fill = float(global_m.get("fills_p95_ms", 0.0))
        g_429 = float(global_m.get("rate_429", 0.0))

        any_warn = False
        any_crit = False

        if g_hb >= self.th.hb_crit_s or g_ack >= self.th.ack_crit_ms or g_fill >= self.th.fill_crit_ms or g_429 >= self.th.rate429_crit:
            self.emit_event(type="alert", level="CRIT", message="pws_global_crit",
                            hb_gap_s=int(g_hb), acks_p95_ms=int(g_ack), fills_p95_ms=int(g_fill), rate_429=g_429,
                            thresholds={"hb_crit_s": self.th.hb_crit_s, "ack_crit_ms": self.th.ack_crit_ms,
                                        "fill_crit_ms": self.th.fill_crit_ms, "rate429_crit": self.th.rate429_crit},
                            intent="request_full_restart")
            any_crit = True
        elif (g_hb >= self.th.hb_warn_s) or (g_ack >= self.th.ack_warn_ms) or (g_fill >= self.th.fill_warn_ms) or (g_429 >= self.th.rate429_warn):
            self.emit_event(type="health", level="WARN", message="pws_global_warn",
                            hb_gap_s=int(g_hb), acks_p95_ms=int(g_ack), fills_p95_ms=int(g_fill), rate_429=g_429,
                            thresholds={"hb_warn_s": self.th.hb_warn_s, "ack_warn_ms": self.th.ack_warn_ms,
                                        "fill_warn_ms": self.th.fill_warn_ms, "rate429_warn": self.th.rate429_warn})
            any_warn = True

        # 2) Streams détaillés
        for key, info in streams.items():
            ex = (key.split(":", 1)[0] if ":" in key else key).upper()
            hb = float((info or {}).get("hb_gap_s", 0.0))
            ack = float((info or {}).get("acks_p95_ms", 0.0))
            fill = float((info or {}).get("fills_p95_ms", 0.0))
            qd = int((info or {}).get("queue_depth", 0))
            qmax = int((info or {}).get("queue_max", 0) or 1)
            ratio = qd / float(qmax)
            dedup = float((info or {}).get("dedup_hit_rate", 0.0))
            r429 = float((info or {}).get("rate_429", 0.0))

            ack_warn, ack_crit, fill_warn, fill_crit = self._per_exchange_ms(ex)

            # WARN/CRIT par stream
            if hb >= self.th.hb_crit_s or ack >= ack_crit or fill >= fill_crit or ratio >= self.th.q_crit_ratio or r429 >= self.th.rate429_crit or dedup >= self.th.dedup_crit:
                self.emit_event(type="alert", level="CRIT", message="pws_stream_crit",
                                stream=key, hb_gap_s=int(hb), acks_p95_ms=int(ack), fills_p95_ms=int(fill),
                                queue_depth=qd, queue_max=qmax, queue_ratio=round(ratio, 3), dedup_hit_rate=dedup,
                                rate_429=r429,
                                thresholds={"hb_crit_s": self.th.hb_crit_s, "ack_crit_ms": ack_crit,
                                            "fill_crit_ms": fill_crit, "q_crit_ratio": self.th.q_crit_ratio,
                                            "rate429_crit": self.th.rate429_crit, "dedup_crit": self.th.dedup_crit},
                                intent="request_full_restart")
                any_crit = True
            elif hb >= self.th.hb_warn_s or ack >= ack_warn or fill >= fill_warn or ratio >= self.th.q_warn_ratio or r429 >= self.th.rate429_warn or dedup >= self.th.dedup_warn:
                self.emit_event(type="health", level="WARN", message="pws_stream_warn",
                                stream=key, hb_gap_s=int(hb), acks_p95_ms=int(ack), fills_p95_ms=int(fill),
                                queue_depth=qd, queue_max=qmax, queue_ratio=round(ratio, 3), dedup_hit_rate=dedup,
                                rate_429=r429,
                                thresholds={"hb_warn_s": self.th.hb_warn_s, "ack_warn_ms": ack_warn,
                                            "fill_warn_ms": fill_warn, "q_warn_ratio": self.th.q_warn_ratio,
                                            "rate429_warn": self.th.rate429_warn, "dedup_warn": self.th.dedup_warn})
                any_warn = True

        # 3) Escalade globale si problèmes persistants
        if any_crit or any_warn:
            self._global_bad_cycles += 1
        else:
            self._global_bad_cycles = 0

        if self._global_bad_cycles >= max(1, self.th.escalate_after_cycles):
            self.emit_event(type="alert", level="CRIT", message="pws_global_persistent",
                            cycles=self._global_bad_cycles, intent="request_full_restart")

        # tout vert
        if not any_warn and not any_crit and self._global_bad_cycles == 0:
            self.emit_event(type="health", level="INFO", message="pws_ok")

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
