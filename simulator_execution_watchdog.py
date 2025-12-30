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

from modules.watchdog.base_watchdog import BaseWatchdogV2
from modules.bot_config import BotConfig

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
    def __init__(
            self,
            state_fn: Optional[StateFn] = None,
            *,
            engine: Optional[Any] = None,
            name: str = "SimulatorExecutionWatchdog",
            config: Optional[EngineWatchdogConfig] = None,
            notify_only: bool = True,
            verbose: bool = True,
            bot_config: Optional[BotConfig] = None,
    ) -> None:
        bot_cfg = bot_config or BotConfig.from_env()
        if bot_config is None:
            self.record_fallback("bot_config")
        thresholds = EngineThresholds(
            submit_ack_warn_ms=bot_cfg.wd.engine_submit_ack_warn_ms,
            submit_ack_crit_ms=bot_cfg.wd.engine_submit_ack_crit_ms,
            ack_fill_warn_ms=bot_cfg.wd.engine_ack_fill_warn_ms,
            ack_fill_crit_ms=bot_cfg.wd.engine_ack_fill_crit_ms,
            timeouts_warn_per_min=bot_cfg.wd.engine_timeouts_warn_per_min,
            timeouts_crit_per_min=bot_cfg.wd.engine_timeouts_crit_per_min,
            retries_warn_per_min=bot_cfg.wd.engine_retries_warn_per_min,
            retries_crit_per_min=bot_cfg.wd.engine_retries_crit_per_min,
            panic_hedge_warn_per_min=bot_cfg.wd.engine_panic_hedge_warn_per_min,
            panic_hedge_crit_per_min=bot_cfg.wd.engine_panic_hedge_crit_per_min,
            queuepos_blocked_warn_per_min=bot_cfg.wd.engine_queuepos_blocked_warn_per_min,
            queuepos_blocked_crit_per_min=bot_cfg.wd.engine_queuepos_blocked_crit_per_min,
            escalate_after_cycles=bot_cfg.wd.engine_escalate_after_cycles,
        )
        cfg = config or EngineWatchdogConfig(
            check_interval=bot_cfg.wd.engine_interval_s,
            thresholds=thresholds,
            tuning=EngineModeTuning(mode=str(getattr(bot_cfg.g, "deployment_mode", "EU_ONLY"))),
        )
        if config is None:
            cfg.tuning.mode = str(getattr(bot_cfg.g, "deployment_mode", cfg.tuning.mode))
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
        self.engine = engine
        self.engine_blocked_ms = int(bot_cfg.wd.engine_blocked_ms)
        self.engine_queue_max = int(bot_cfg.wd.engine_queue_max)
        self._bad_cycles: int = 0
        self._last_inflight: Optional[int] = None
        self._last_inflight_ts: float = 0.0


    async def _check_once(self) -> None:
        snapshot = await self.collect_snapshot()
        severity, reasons, details = self.evaluate(snapshot)
        self.emit_health_event(
            severity=severity,
            reasons=reasons,
            details=details,
            component="Engine",
            module="ExecutionEngine",
            observed_at_ms=snapshot.get("observed_at_ms"),
        )

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

    async def collect_snapshot(self) -> Dict[str, Any]:
        observed_at_ms = self.now_ts_ms()
        missing: list[str] = []
        errors: list[str] = []
        state: Dict[str, Any] = {}
        if self.state_fn:
            state = await self.safe_call(self.state_fn, default={}, errors=errors, error_label="state_fn")
        if not state and self.engine is not None:
            state = await self.safe_call(getattr(self.engine, "get_status", None), default={}, errors=errors,
                                         error_label="engine.get_status")
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
        reasons: list[str] = []
        details: Dict[str, Any] = {}
        missing = list(snapshot.get("missing") or [])
        g = self.safe_get(state, "global", default={}, missing=missing)
        exmap = self.safe_get(state, "exchanges", default={}, missing=missing)
        if not isinstance(g, dict):
            g = {}
        if not isinstance(exmap, dict):
            exmap = {}

        any_warn = False
        any_crit = False

        if g:
            g_sa = self.safe_float(g, "submit_to_ack_p95_ms", default=0.0, missing=missing)
            g_af = self.safe_float(g, "ack_to_fill_p95_ms", default=0.0, missing=missing)
            g_to = self.safe_float(g, "timeouts_per_min", default=0.0, missing=missing)
            g_rt = self.safe_float(g, "retries_per_min", default=0.0, missing=missing)
            g_ph = self.safe_float(g, "panic_hedge_per_min", default=0.0, missing=missing)
            g_qb = self.safe_float(g, "queuepos_blocked_per_min", default=0.0, missing=missing)
            if g_sa >= self.th.submit_ack_crit_ms or g_af >= self.th.ack_fill_crit_ms:
                reasons.append("ENGINE_BLOCKED")
                any_crit = True
            elif g_sa >= self.th.submit_ack_warn_ms or g_af >= self.th.ack_fill_warn_ms:
                any_warn = True
            if g_to >= self.th.timeouts_crit_per_min or g_rt >= self.th.retries_crit_per_min:
                reasons.append("ENGINE_BLOCKED")
                any_crit = True
            elif g_to >= self.th.timeouts_warn_per_min or g_rt >= self.th.retries_warn_per_min:
                any_warn = True
            if g_ph >= self.th.panic_hedge_crit_per_min or g_qb >= self.th.queuepos_blocked_crit_per_min:
                reasons.append("ENGINE_BLOCKED")
                any_crit = True
            elif g_ph >= self.th.panic_hedge_warn_per_min or g_qb >= self.th.queuepos_blocked_warn_per_min:
                any_warn = True

        for ex, info in exmap.items():
            lat = self._ex_thresholds(ex)
            info = info or {}
            ack = self.safe_float(info, "ack_p95_ms", default=0.0, missing=missing)
            fil = self.safe_float(info, "fill_p95_ms", default=0.0, missing=missing)
            to = self.safe_float(info, "timeouts_per_min", default=0.0, missing=missing)
            rt = self.safe_float(info, "retries_per_min", default=0.0, missing=missing)
            ph = self.safe_float(info, "panic_hedge_per_min", default=0.0, missing=missing)
            qb = self.safe_float(info, "queuepos_blocked_per_min", default=0.0, missing=missing)
            if ack >= lat.ack_crit_ms or fil >= lat.fill_crit_ms:
                reasons.append("ENGINE_BLOCKED")
                any_crit = True
            elif ack >= lat.ack_warn_ms or fil >= lat.fill_warn_ms:
                any_warn = True
            if to >= self.th.timeouts_crit_per_min or rt >= self.th.retries_crit_per_min:
                reasons.append("ENGINE_BLOCKED")
                any_crit = True
            elif to >= self.th.timeouts_warn_per_min or rt >= self.th.retries_warn_per_min:
                any_warn = True
            if ph >= self.th.panic_hedge_crit_per_min or qb >= self.th.queuepos_blocked_crit_per_min:
                reasons.append("ENGINE_BLOCKED")
                any_crit = True
            elif ph >= self.th.panic_hedge_warn_per_min or qb >= self.th.queuepos_blocked_warn_per_min:
                any_warn = True

        orders_inflight = self.safe_get(state, "orders_inflight", default=None, missing=missing)
        if isinstance(orders_inflight, int):
            if orders_inflight > self.engine_queue_max:
                reasons.append("WD_QUEUE_BACKLOG")
            now_ts = time.time()
            if self._last_inflight is None or self._last_inflight != orders_inflight:
                self._last_inflight = orders_inflight
                self._last_inflight_ts = now_ts
            elif orders_inflight > 0 and (now_ts - self._last_inflight_ts) * 1000 >= self.engine_blocked_ms:
                reasons.append("ENGINE_BLOCKED")

        if any_warn or any_crit:
            self._bad_cycles += 1
        else:
            self._bad_cycles = 0

        if self._bad_cycles >= max(1, self.th.escalate_after_cycles):
            reasons.append("ENGINE_BLOCKED")
            any_crit = True

        if missing:
            reasons.append("MISSING_FIELD")
            details["missing_fields"] = missing

        details["bad_cycles"] = self._bad_cycles
        severity = "OK"
        if any_crit or "ENGINE_BLOCKED" in reasons:
            severity = "CRIT"
        elif any_warn or reasons:
            severity = "WARN"
        return severity, reasons, details

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
