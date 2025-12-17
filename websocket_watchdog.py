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

from base_watchdog import BaseWatchdogV2

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
    ) -> None:
        cfg = config or WebSocketWatchdogConfig()
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
        self._global_stale_cycles: int = 0

    # -------------- core check --------------
    async def _check_once(self) -> None:
        state = await self._get_state()
        now = float(state.get("now_ts", time.time()))
        ex_map: Dict[str, Any] = state.get("exchanges", {}) or {}

        if not ex_map:
            self.emit_event(type="health", level="WARN", message="ws_state_empty", reason="no_exchanges")
            return

        any_warn = False
        any_crit = False
        stale_any_exchange = False

        for ex, info in ex_map.items():
            streams = (info or {}).get("streams", {}) or {}
            if not streams:
                # Pas de streams : on avertit mais on n'escalade pas tout de suite
                self.emit_event(type="health", level="WARN", message="no_streams", exchange=ex)
                continue

            # Seuils effectifs pour cet exchange
            warn_ms, crit_ms = self._ms_thresholds_for_exchange(ex)

            # Mesures
            stale_pairs: Dict[str, float] = {}
            max_age_ms = 0.0
            for stream, meta in streams.items():
                last_ts = float((meta or {}).get("last_msg_ts", 0.0))
                if last_ts <= 0.0:
                    continue
                age_ms = max(0.0, (now - last_ts) * 1000.0)
                max_age_ms = max(max_age_ms, age_ms)
                if age_ms >= warn_ms:
                    stale_pairs[stream] = age_ms

            # Heartbeat & resub
            hb_gap_s = float((info or {}).get("hb_gap_s", 0.0))
            resub_rate = float((info or {}).get("resub_rate_per_min", 0.0))

            # Évaluation par exchange
            if stale_pairs or hb_gap_s >= self.th.hb_gap_warn_s or resub_rate >= self.th.resub_warn_per_min:
                stale_any_exchange = stale_any_exchange or bool(stale_pairs)
                level = "WARN"
                reason = []
                if stale_pairs:
                    reason.append("stale_pairs")
                if hb_gap_s >= self.th.hb_gap_warn_s:
                    reason.append("hb_gap")
                if resub_rate >= self.th.resub_warn_per_min:
                    reason.append("resub_storm")
                self.emit_event(
                    type="health", level=level, message="ws_exchange_warn",
                    exchange=ex, max_age_ms=int(max_age_ms),
                    stale_pairs={k: int(v) for k, v in stale_pairs.items()},
                    hb_gap_s=int(hb_gap_s), resub_per_min=int(resub_rate),
                    thresholds={"warn_ms": warn_ms, "crit_ms": crit_ms,
                                "hb_gap_warn_s": self.th.hb_gap_warn_s,
                                "hb_gap_crit_s": self.th.hb_gap_crit_s,
                                "resub_warn_per_min": self.th.resub_warn_per_min,
                                "resub_crit_per_min": self.th.resub_crit_per_min},
                    reasons=reason,
                )
                any_warn = True

            # CRIT par exchange
            if (max_age_ms >= crit_ms) or (hb_gap_s >= self.th.hb_gap_crit_s) or (resub_rate >= self.th.resub_crit_per_min):
                self.emit_event(
                    type="alert", level="CRIT", message="ws_exchange_crit",
                    exchange=ex, max_age_ms=int(max_age_ms), hb_gap_s=int(hb_gap_s), resub_per_min=int(resub_rate),
                    thresholds={"crit_ms": crit_ms, "hb_gap_crit_s": self.th.hb_gap_crit_s,
                                "resub_crit_per_min": self.th.resub_crit_per_min},
                    intent="request_full_restart",
                )
                any_crit = True

        # Escalade globale si staleness persiste N cycles
        if stale_any_exchange:
            self._global_stale_cycles += 1
        else:
            self._global_stale_cycles = 0

        if self._global_stale_cycles >= max(1, self.th.escalate_after_cycles):
            self.emit_event(
                type="alert", level="CRIT", message="ws_global_stale_persistent",
                cycles=self._global_stale_cycles, intent="request_full_restart",
            )
            # On ne reset pas immédiatement pour laisser la coalescence anti-spam faire son travail

        # Si tout est vert
        if not any_warn and not any_crit and self._global_stale_cycles == 0:
            self.emit_event(type="health", level="INFO", message="ws_ok")

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
