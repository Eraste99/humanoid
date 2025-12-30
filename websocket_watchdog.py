# -*- coding: utf-8 -*-
"""
WebSocketExchangeWatchdog — version "ceinture" notify-only (v3)
================================================================

Objectif
--------
Surveiller les clients WebSocket (publics) **sans jamais agir** localement :
- Aucune tentative de reload/restart.
- Émission d'événements normalisés (health/alert) vers un sink (AlertDispatcher/CentralWatchdog).
- Hystérésis simple par compteurs consécutifs.
- Seuils mode-aware (EU_ONLY vs SPLIT) et tolérances par exchange.

Dépendance minimale
-------------------
- Requiert `BaseWatchdogV2` (boucle, emit_event, notify_only déjà géré au niveau base).
- N'impose **aucun** import du client WS; on consomme un `state_fn()` fourni par le boot.

API attendue pour `state_fn`
----------------------------
`state = await state_fn()` ou `state_fn()` sync → dict du type:
{
  "now_ts": float (epoch seconds, optionnel),
  "exchanges": {
      "BINANCE": {
          "hb_gap_s": float,                 # gap heartbeat agrégé (optionnel)
          "resub_rate_per_min": float,       # resub/min (optionnel)
          "streams": {
              "BTCUSDC@depth": {"last_msg_ts": float},
              "ETHUSDC@depth": {"last_msg_ts": float},
              ...
          }
      },
      "BYBIT": { ... },
      "COINBASE": { ... }
  }
}

Notes
-----
- Si certaines clés manquent, le watchdog reste tolérant et émet des warnings explicites.
- Toutes les actions (reload/restart) sont **remplacées** par des évènements `intent="request_full_restart"`.
"""
from __future__ import annotations
import asyncio, time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from modules.watchdog.base_watchdog import BaseWatchdogV2
from modules.bot_config import BotConfig

StateFn = Callable[[], Dict[str, Any] | Awaitable[Dict[str, Any]]]


@dataclass
class WsThresholds:
    # Staleness par stream/pair
    stale_warn_ms: int = 900   # EU_ONLY par défaut
    stale_crit_ms: int = 1200
    # Heartbeat agrégé & resub storms
    hb_gap_warn_s: int = 15
    hb_gap_crit_s: int = 30
    resub_warn_per_min: int = 3
    resub_crit_per_min: int = 6
    # Escalade (cycles consécutifs de staleness pour CRIT global)
    escalate_after_cycles: int = 3


@dataclass
class WsModeTuning:
    mode: str = "EU_ONLY"  # "EU_ONLY" ou "SPLIT"
    # Multiplicateurs en SPLIT (réseau transatlantique, tolérance plus large)
    split_ms_factor: float = 1.2
    split_hb_factor: float = 1.2

    def apply(self, t: WsThresholds) -> WsThresholds:
        if str(self.mode).upper() != "SPLIT":
            return t
        return WsThresholds(
            stale_warn_ms=int(t.stale_warn_ms * self.split_ms_factor),
            stale_crit_ms=int(t.stale_crit_ms * self.split_ms_factor),
            hb_gap_warn_s=int(t.hb_gap_warn_s * self.split_hb_factor),
            hb_gap_crit_s=int(t.hb_gap_crit_s * self.split_hb_factor),
            resub_warn_per_min=t.resub_warn_per_min,  # inchangé
            resub_crit_per_min=t.resub_crit_per_min,
            escalate_after_cycles=t.escalate_after_cycles,
        )


@dataclass
class WebSocketWatchdogConfig:
    check_interval: float = 2.0
    thresholds: WsThresholds = field(default_factory=WsThresholds)
    tuning: WsModeTuning = field(default_factory=WsModeTuning)
    # Par exchange (optionnel) → overrides des ms de staleness (warn, crit)
    per_exchange_ms: Dict[str, Tuple[int, int]] = field(default_factory=dict)


class WebSocketExchangeWatchdog(BaseWatchdogV2):
    """Watchdog WS passif/notify-only.

    - Ne tente **jamais** de reload/restart local.
    - Émet des events "health" (WARN) et "alert" (CRIT) avec intent facultatif "request_full_restart".
    """

    def __init__(
        self,
        state_fn: StateFn,
        *,
        name: str = "WebSocketExchangeWatchdog",
        config: Optional[WebSocketWatchdogConfig] = None,
        notify_only: bool = True,
        verbose: bool = True,
        bot_config: Optional[BotConfig] = None,
    ) -> None:
        bot_cfg = bot_config or BotConfig.from_env()
        if bot_config is None:
            self.record_fallback("bot_config")
        th = WsThresholds(
            stale_warn_ms=bot_cfg.wd.ws_public_stale_warn_ms,
            stale_crit_ms=bot_cfg.wd.ws_public_stale_crit_ms,
            hb_gap_warn_s=bot_cfg.wd.ws_public_hb_gap_warn_s,
            hb_gap_crit_s=bot_cfg.wd.ws_public_hb_gap_crit_s,
            resub_warn_per_min=bot_cfg.wd.ws_public_resub_warn_per_min,
            resub_crit_per_min=bot_cfg.wd.ws_public_resub_crit_per_min,
            escalate_after_cycles=bot_cfg.wd.ws_public_escalate_after_cycles,
        )
        cfg = config or WebSocketWatchdogConfig(check_interval=bot_cfg.wd.ws_public_interval_s, thresholds=th)
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
        self._global_stale_cycles: int = 0

    # -------------- core check --------------
    async def _check_once(self) -> None:
        snapshot = await self.collect_snapshot()
        severity, reasons, details = self.evaluate(snapshot)
        self.emit_health_event(
            severity=severity,
            reasons=reasons,
            details=details,
            component="WebSocketPublic",
            module="WebSocketsClients",
            observed_at_ms=snapshot.get("observed_at_ms"),
        )

    # -------------- helpers --------------
    async def _get_state(self) -> Dict[str, Any]:
        try:
            res = self.state_fn()
            if asyncio.iscoroutine(res):
                return await res
            return res or {}
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
        state = snapshot.get("module_state", {}) or {}
        missing = list(snapshot.get("missing") or [])
        reasons: list[str] = []
        details: Dict[str, Any] = {}

        now_ms = int(snapshot.get("observed_at_ms") or self.now_ts_ms())
        ex_map = self.safe_get(state, "exchanges", default={}, missing=missing)
        if not isinstance(ex_map, dict):
            ex_map = {}
        any_warn = False
        any_crit = False
        stale_any_exchange = False

        if not ex_map:
            reasons.append("MISSING_FIELD")
            details["missing_fields"] = missing or ["MISSING_FIELD:exchanges"]

        for ex, info in ex_map.items():
            info = info or {}
            streams = self.safe_get(info, "streams", default={}, missing=missing)
            if not isinstance(streams, dict) or not streams:
                any_warn = True
                continue

            warn_ms, crit_ms = self._ms_thresholds_for_exchange(ex)

            max_age_ms = 0.0
            for stream, meta in streams.items():
                meta = meta or {}
                last_ts_ms = self.safe_ts_ms(meta, "last_msg_ts", default=None, missing=missing)
                if last_ts_ms is None:
                    continue
                age_ms = max(0.0, float(now_ms - last_ts_ms))
                max_age_ms = max(max_age_ms, age_ms)
                if age_ms >= warn_ms:
                    stale_any_exchange = True
                    any_warn = True
                if age_ms >= crit_ms:
                    reasons.append("PUBLIC_FEED_STALE")
                    any_crit = True
            hb_gap_s = self.safe_float(info, "hb_gap_s", default=0.0, missing=missing)
            resub_rate = self.safe_float(info, "resub_rate_per_min", default=0.0, missing=missing)
            details.setdefault("exchanges", {})[ex] = {
                "max_age_ms": int(max_age_ms),
                "hb_gap_s": hb_gap_s,
                "resub_rate_per_min": resub_rate,
            }

            if hb_gap_s >= self.th.hb_gap_crit_s or resub_rate >= self.th.resub_crit_per_min:
                reasons.append("PUBLIC_FEED_STALE")
            elif hb_gap_s >= self.th.hb_gap_warn_s or resub_rate >= self.th.resub_warn_per_min:
                any_warn = True

        if stale_any_exchange:
            self._global_stale_cycles += 1
        else:
            self._global_stale_cycles = 0

        if self._global_stale_cycles >= max(1, self.th.escalate_after_cycles):
            reasons.append("PUBLIC_FEED_STALE")
            any_crit = True

            # On ne reset pas immédiatement pour laisser la coalescence anti-spam faire son travail

        if missing:
            reasons.append("MISSING_FIELD")
            details["missing_fields"] = missing

        details["global_stale_cycles"] = self._global_stale_cycles

        severity = "OK"
        if any_crit or "PUBLIC_FEED_STALE" in reasons:
            severity = "CRIT"
        elif any_warn or reasons:
            severity = "WARN"
        return severity, reasons, details

    def _ms_thresholds_for_exchange(self, ex: str) -> Tuple[int, int]:
        ex = (ex or "").upper()
        if ex in self.cfg.per_exchange_ms:
            w, c = self.cfg.per_exchange_ms[ex]
            return int(w), int(c)
        return int(self.th.stale_warn_ms), int(self.th.stale_crit_ms)

    # -------------- status --------------
    def get_status(self) -> Dict[str, Any]:
        s = super().get_status()
        s.update({
            "module": "WebSocketExchangeWatchdog",
            "metrics": {**s.get("metrics", {}),
                         "global_stale_cycles": self._global_stale_cycles,
                         "stale_warn_ms": self.th.stale_warn_ms,
                         "stale_crit_ms": self.th.stale_crit_ms,
                         "hb_gap_warn_s": self.th.hb_gap_warn_s,
                         "hb_gap_crit_s": self.th.hb_gap_crit_s,
                         "resub_warn_per_min": self.th.resub_warn_per_min,
                         "resub_crit_per_min": self.th.resub_crit_per_min},
        })
        return s
