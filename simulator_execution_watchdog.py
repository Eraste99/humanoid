# -*- coding: utf-8 -*-
"""
Simulator/ExecutionEngine Watchdog — version "ceinture" notify-only (v3)
=======================================================================

Objectif
--------
Surveiller l'ExecutionEngine et (optionnellement) le DynamicExecutionSimulator **sans aucune action locale**.
On émet des évènements health/alert, jamais de restart/cancel/appel Engine.

Entrée (`state_fn`)
-------------------
`state = await state_fn()` (ou sync) → dict suggéré:
{
  "now_ts": float,
  "global": {"submit_to_ack_p95_ms": float, "ack_to_fill_p95_ms": float,
              "timeouts_per_min": float, "retries_per_min": float,
              "panic_hedge_per_min": float, "queuepos_blocked_per_min": float,
              "inflight": {"TT": int, "TM": int, "MM": int, "REB": int},
              "bundle_concurrency_effective": int},
  "exchanges": {
     "BINANCE": {"ack_p95_ms": float, "fill_p95_ms": float,
                  "timeouts_per_min": float, "retries_per_min": float,
                  "panic_hedge_per_min": float, "queuepos_blocked_per_min": float},
     "BYBIT":   {...},
     "COINBASE":{...}
  }
}

Notes
-----
- Tolérant aux champs manquants. Overrides par exchange possibles.
- EU_ONLY/SPLIT modulent les seuils CB (plus permissifs).
"""
from __future__ import annotations
import asyncio, time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from base_watchdog import BaseWatchdogV2

StateFn = Callable[[], Dict[str, Any] | Awaitable[Dict[str, Any]]]


@dataclass
class ExchLatency:
    ack_warn_ms: int
    ack_crit_ms: int
    fill_warn_ms: int
    fill_crit_ms: int


@dataclass
class EngineThresholds:
    # Latences globales
    submit_ack_warn_ms: int = 130
    submit_ack_crit_ms: int = 200
    ack_fill_warn_ms: int = 180
    ack_fill_crit_ms: int = 260
    # Taux d'incidents par minute
    timeouts_warn_per_min: float = 0.5
    timeouts_crit_per_min: float = 1.5
    retries_warn_per_min: float = 1.0
    retries_crit_per_min: float = 3.0
    panic_hedge_warn_per_min: float = 0.5
    panic_hedge_crit_per_min: float = 2.0
    queuepos_blocked_warn_per_min: float = 1.0
    queuepos_blocked_crit_per_min: float = 3.0
    # Escalade persistance
    escalate_after_cycles: int = 3

    # Defaults par exchange (EU_ONLY). CB est ajusté par mode
    per_exchange: Dict[str, ExchLatency] = field(default_factory=lambda: {
        "BINANCE": ExchLatency(ack_warn_ms=120, ack_crit_ms=180, fill_warn_ms=170, fill_crit_ms=240),
        "BYBIT":   ExchLatency(ack_warn_ms=120, ack_crit_ms=180, fill_warn_ms=170, fill_crit_ms=240),
        "COINBASE":ExchLatency(ack_warn_ms=180, ack_crit_ms=250, fill_warn_ms=220, fill_crit_ms=300),
    })


@dataclass
class EngineModeTuning:
    mode: str = "EU_ONLY"
    split_ms_factor: float = 1.20  # Coinbase plus permissif en SPLIT

    def apply_exchange(self, ex: str, x: ExchLatency) -> ExchLatency:
        if str(self.mode).upper() != "SPLIT":
            return x
        if ex.upper() == "COINBASE":
            return ExchLatency(
                ack_warn_ms=int(x.ack_warn_ms * self.split_ms_factor),
                ack_crit_ms=int(x.ack_crit_ms * self.split_ms_factor),
                fill_warn_ms=int(x.fill_warn_ms * self.split_ms_factor),
                fill_crit_ms=int(x.fill_crit_ms * self.split_ms_factor),
            )
        return x


@dataclass
class EngineWatchdogConfig:
    check_interval: float = 2.0
    thresholds: EngineThresholds = field(default_factory=EngineThresholds)
    tuning: EngineModeTuning = field(default_factory=EngineModeTuning)


class SimulatorExecutionWatchdog(BaseWatchdogV2):
    """Watchdog Execution/Simulator passif (notify-only)."""

    def __init__(
        self,
        state_fn: StateFn,
        *,
        name: str = "SimulatorExecutionWatchdog",
        config: Optional[EngineWatchdogConfig] = None,
        notify_only: bool = True,
        verbose: bool = True,
    ) -> None:
        cfg = config or EngineWatchdogConfig()
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
        now = float(s.get("now_ts", time.time()))
        g = s.get("global", {}) or {}
        exmap = s.get("exchanges", {}) or {}

        any_warn = False
        any_crit = False

        # 1) Globaux
        g_sa = float(g.get("submit_to_ack_p95_ms", 0.0))
        g_af = float(g.get("ack_to_fill_p95_ms", 0.0))
        g_to = float(g.get("timeouts_per_min", 0.0))
        g_rt = float(g.get("retries_per_min", 0.0))
        g_ph = float(g.get("panic_hedge_per_min", 0.0))
        g_qb = float(g.get("queuepos_blocked_per_min", 0.0))

        if g_sa >= self.th.submit_ack_crit_ms or g_af >= self.th.ack_fill_crit_ms:
            self.emit_event(type="alert", level="CRIT", message="engine_latency_crit",
                            submit_ack_p95_ms=int(g_sa), ack_fill_p95_ms=int(g_af),
                            thresholds={"sa_crit": self.th.submit_ack_crit_ms, "af_crit": self.th.ack_fill_crit_ms},
                            intent="request_full_restart")
            any_crit = True
        elif g_sa >= self.th.submit_ack_warn_ms or g_af >= self.th.ack_fill_warn_ms:
            self.emit_event(type="health", level="WARN", message="engine_latency_warn",
                            submit_ack_p95_ms=int(g_sa), ack_fill_p95_ms=int(g_af),
                            thresholds={"sa_warn": self.th.submit_ack_warn_ms, "af_warn": self.th.ack_fill_warn_ms})
            any_warn = True

        for label, val, warn, crit, msg in [
            ("timeouts_per_min", g_to, self.th.timeouts_warn_per_min, self.th.timeouts_crit_per_min, "timeouts"),
            ("retries_per_min",  g_rt, self.th.retries_warn_per_min,  self.th.retries_crit_per_min,  "retries"),
            ("panic_hedge_per_min", g_ph, self.th.panic_hedge_warn_per_min, self.th.panic_hedge_crit_per_min, "panic_hedge"),
            ("queuepos_blocked_per_min", g_qb, self.th.queuepos_blocked_warn_per_min, self.th.queuepos_blocked_crit_per_min, "queuepos_blocked"),
        ]:
            if val >= crit:
                self.emit_event(type="alert", level="CRIT", message=f"engine_{msg}_crit",
                                **{label: val, "threshold": crit}, intent="request_full_restart")
                any_crit = True
            elif val >= warn:
                self.emit_event(type="health", level="WARN", message=f"engine_{msg}_warn",
                                **{label: val, "threshold": warn})
                any_warn = True

        # 2) Par exchange
        for ex, info in exmap.items():
            lat = self._ex_thresholds(ex)
            ack = float((info or {}).get("ack_p95_ms", 0.0))
            fil = float((info or {}).get("fill_p95_ms", 0.0))
            to  = float((info or {}).get("timeouts_per_min", 0.0))
            rt  = float((info or {}).get("retries_per_min", 0.0))
            ph  = float((info or {}).get("panic_hedge_per_min", 0.0))
            qb  = float((info or {}).get("queuepos_blocked_per_min", 0.0))

            if ack >= lat.ack_crit_ms or fil >= lat.fill_crit_ms:
                self.emit_event(type="alert", level="CRIT", message="engine_exchange_latency_crit",
                                exchange=ex, ack_p95_ms=int(ack), fill_p95_ms=int(fil),
                                thresholds={"ack_crit": lat.ack_crit_ms, "fill_crit": lat.fill_crit_ms},
                                intent="request_full_restart")
                any_crit = True
            elif ack >= lat.ack_warn_ms or fil >= lat.fill_warn_ms:
                self.emit_event(type="health", level="WARN", message="engine_exchange_latency_warn",
                                exchange=ex, ack_p95_ms=int(ack), fill_p95_ms=int(fil),
                                thresholds={"ack_warn": lat.ack_warn_ms, "fill_warn": lat.fill_warn_ms})
                any_warn = True

            for label, val, warn, crit, msg in [
                ("timeouts_per_min", to, self.th.timeouts_warn_per_min, self.th.timeouts_crit_per_min, "timeouts"),
                ("retries_per_min",  rt, self.th.retries_warn_per_min,  self.th.retries_crit_per_min,  "retries"),
                ("panic_hedge_per_min", ph, self.th.panic_hedge_warn_per_min, self.th.panic_hedge_crit_per_min, "panic_hedge"),
                ("queuepos_blocked_per_min", qb, self.th.queuepos_blocked_warn_per_min, self.th.queuepos_blocked_crit_per_min, "queuepos_blocked"),
            ]:
                if val >= crit:
                    self.emit_event(type="alert", level="CRIT", message=f"engine_exchange_{msg}_crit",
                                    exchange=ex, **{label: val, "threshold": crit}, intent="request_full_restart")
                    any_crit = True
                elif val >= warn:
                    self.emit_event(type="health", level="WARN", message=f"engine_exchange_{msg}_warn",
                                    exchange=ex, **{label: val, "threshold": warn})
                    any_warn = True

        # 3) Escalade persistance
        if any_warn or any_crit:
            self._bad_cycles += 1
        else:
            self._bad_cycles = 0

        if self._bad_cycles >= max(1, self.th.escalate_after_cycles):
            self.emit_event(type="alert", level="CRIT", message="engine_persistent_degradation",
                            cycles=self._bad_cycles, intent="request_full_restart")

        # tout vert
        if self._bad_cycles == 0 and not any_warn and not any_crit:
            self.emit_event(type="health", level="INFO", message="engine_ok")

    # ---- helpers ----
    def _ex_thresholds(self, ex: str) -> ExchLatency:
        base = self.th.per_exchange.get(ex.upper()) or self.th.per_exchange.get("BINANCE")
        return self.cfg.tuning.apply_exchange(ex, base)

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
            "module": "SimulatorExecutionWatchdog",
            "metrics": {**s.get("metrics", {}),
                         "bad_cycles": self._bad_cycles,
                         "sa_warn_ms": self.th.submit_ack_warn_ms, "sa_crit_ms": self.th.submit_ack_crit_ms,
                         "af_warn_ms": self.th.ack_fill_warn_ms,  "af_crit_ms": self.th.ack_fill_crit_ms},
        })
        return s
