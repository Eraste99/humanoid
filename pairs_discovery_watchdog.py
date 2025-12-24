# -*- coding: utf-8 -*-
"""
PairsDiscoveryWatchdog — version "ceinture" notify-only (v3)
=============================================================

Objectif
--------
- Surveiller et *évaluer* les propositions d'univers de paires **sans jamais appliquer** localement.
- Publier des évènements "proposal" (candidate/final) et des alertes (rate-limit, erreurs, gap de refresh).
- Respecter confirmation & dwell avant de marquer une proposition comme "finale" (à appliquer par l'orchestrateur global).

Dépendance minimale
-------------------
- Requiert `BaseWatchdogV2`.
- Nécessite deux callbacks fournis par le boot (read-only):
  * `discover_fn()` → dict avec au moins `pairs: List[str]` et/ou `mapping: Dict[str, Any]`.
      - Champs optionnels: `retry_after_s: float`, `error: str`, `meta: {...}`.
  * `get_current_universe_fn()` → dict `{ "pairs": [...], "mapping": {...} }` actuel (si dispo), sinon vide.

Aucun `apply_*` n'est exécuté ici; c'est **notify-only**.
"""
from __future__ import annotations
import asyncio, time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple

from modules.watchdog.base_watchdog import BaseWatchdogV2
from modules.bot_config import BotConfig

DiscoverFn = Callable[[], Dict[str, Any] | Awaitable[Dict[str, Any]]]
GetUniverseFn = Callable[[], Dict[str, Any] | Awaitable[Dict[str, Any]]]


@dataclass
class DiscoveryThresholds:
    min_change_ratio: float = 0.05     # 5 % de delta (add+rem)/taille actuelle
    confirm_ticks: int = 2            # confirmations consécutives avant "final"
    dwell_ticks: int = 2              # stabilité minimale depuis 1ère vue
    max_refresh_gap_s: int = 600      # alerte si pas de découverte pendant 10 min


@dataclass
class PairsDiscoveryConfig:
    check_interval: float = 5.0
    thresholds: DiscoveryThresholds = field(default_factory=DiscoveryThresholds)


class PairsDiscoveryWatchdog(BaseWatchdogV2):
    """Watchdog Discovery passif/notify-only.

    - Émet des proposals (candidate/final) **sans** appeler apply/update.
    - Tient compte des rate-limits (`retry_after_s`) remontés par la discovery.
    """

    def __init__(
        self,
        discover_fn: DiscoverFn,
        get_current_universe_fn: Optional[GetUniverseFn] = None,
        *,
        name: str = "PairsDiscoveryWatchdog",
        config: Optional[PairsDiscoveryConfig] = None,
        notify_only: bool = True,
        verbose: bool = True,
        bot_config: Optional[BotConfig] = None,
    ) -> None:
        bot_cfg = bot_config or BotConfig.from_env()
        if bot_config is None:
            self.record_fallback("bot_config")
        th = DiscoveryThresholds(
            min_change_ratio=bot_cfg.wd.discovery_min_change_ratio,
            confirm_ticks=bot_cfg.wd.discovery_confirm_ticks,
            dwell_ticks=bot_cfg.wd.discovery_dwell_ticks,
            max_refresh_gap_s=bot_cfg.wd.discovery_max_refresh_gap_s,
        )
        cfg = config or PairsDiscoveryConfig(check_interval=bot_cfg.wd.discovery_interval_s, thresholds=th)
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
        self.discover_fn = discover_fn
        self.get_current_universe_fn = get_current_universe_fn or (lambda: {"pairs": [], "mapping": {}})

        # État de proposition
        self._last_discovery_ts: float = 0.0
        self._retry_after_until_ts: float = 0.0
        self._first_seen_ts: float = 0.0
        self._confirm_count: int = 0
        self._last_candidate_key: Optional[str] = None

    # -------------- core check --------------
    async def _check_once(self) -> None:
        now = time.time()
        if now < self._retry_after_until_ts:
            self.emit_event(type="health", level="INFO", message="discovery_backoff_active",
                            retry_after_remaining_s=round(self._retry_after_until_ts - now, 2))
            snapshot = {
                "observed_at_ms": self.now_ts_ms(),
                "last_discovery_ts": self._last_discovery_ts,
                "retry_after_until_ts": self._retry_after_until_ts,
            }
            severity, reasons, details = self._evaluate_snapshot(snapshot)
            self.emit_health_event(
                severity=severity,
                reasons=reasons,
                details=details,
                component="PairsDiscovery",
                observed_at_ms=snapshot["observed_at_ms"],
            )
            return

        # Récupère l'univers courant pour calculer le diff
        current = await self._get_current()
        cur_pairs: Set[str] = set(current.get("pairs", []) or [])
        cur_mapping: Dict[str, Any] = current.get("mapping", {}) or {}

        # Appel discovery (tolérant)
        result = await self._run_discover()
        if not result:
            # Gap de refresh
            if (now - self._last_discovery_ts) > self.cfg.thresholds.max_refresh_gap_s:
                self.emit_event(type="alert", level="WARN", message="discovery_gap",
                                gap_s=int(now - self._last_discovery_ts), max_refresh_gap_s=self.cfg.thresholds.max_refresh_gap_s)
            snapshot = {
                "observed_at_ms": self.now_ts_ms(),
                "last_discovery_ts": self._last_discovery_ts,
                "retry_after_until_ts": self._retry_after_until_ts,
            }
            severity, reasons, details = self._evaluate_snapshot(snapshot)
            self.emit_health_event(
                severity=severity,
                reasons=reasons,
                details=details,
                component="PairsDiscovery",
                observed_at_ms=snapshot["observed_at_ms"],
            )
            return

        self._last_discovery_ts = now
        nxt_pairs: List[str] = list(result.get("pairs", []) or [])
        nxt_mapping: Dict[str, Any] = dict(result.get("mapping", {}) or {})

        # Diff & ratio
        add, rem, ratio = self._compute_diff(cur_pairs, set(nxt_pairs))
        meta = result.get("meta", {}) or {}

        # Publication candidate si changement notable
        if ratio >= max(0.0, self.cfg.thresholds.min_change_ratio):
            key = self._candidate_key(nxt_pairs, nxt_mapping)
            if key != self._last_candidate_key:
                self._last_candidate_key = key
                self._first_seen_ts = now
                self._confirm_count = 0

            self._confirm_count += 1
            dwell_ok = (now - self._first_seen_ts) >= (self.cfg.thresholds.dwell_ticks * self.cfg.check_interval)
            confirm_ok = self._confirm_count >= self.cfg.thresholds.confirm_ticks

            # always emit candidate
            self.emit_event(
                type="proposal", level="INFO", message="candidate",
                diff={"add": sorted(add), "rem": sorted(rem)}, ratio=round(ratio, 4),
                pairs=nxt_pairs, mapping=nxt_mapping, meta=meta,
                confirm_count=self._confirm_count, dwell_ok=dwell_ok, confirm_ok=confirm_ok,
            )

            # final quand dwell & confirm atteints (toujours notify-only)
            if dwell_ok and confirm_ok:
                self.emit_event(
                    type="proposal", level="INFO", message="final",
                    diff={"add": sorted(add), "rem": sorted(rem)}, ratio=round(ratio, 4),
                    pairs=nxt_pairs, mapping=nxt_mapping, meta=meta,
                )
        else:
            # pas de changement significatif
            self.emit_event(type="health", level="INFO", message="no_significant_change", ratio=round(ratio, 4))

        snapshot = {
            "observed_at_ms": self.now_ts_ms(),
            "last_discovery_ts": self._last_discovery_ts,
            "retry_after_until_ts": self._retry_after_until_ts,
        }
        severity, reasons, details = self._evaluate_snapshot(snapshot)
        self.emit_health_event(
            severity=severity,
            reasons=reasons,
            details=details,
            component="PairsDiscovery",
            observed_at_ms=snapshot["observed_at_ms"],
        )
    # -------------- helpers --------------
    async def _get_current(self) -> Dict[str, Any]:
        try:
            res = self.get_current_universe_fn()
            if asyncio.iscoroutine(res):
                return await res
            return res or {"pairs": [], "mapping": {}}
        except Exception as e:
            self.emit_event(type="error", level="ERROR", message=f"get_current_universe failed: {e}")
            return {"pairs": [], "mapping": {}}

    async def _run_discover(self) -> Dict[str, Any]:
        try:
            r = self.discover_fn()
            if asyncio.iscoroutine(r):
                r = await r
            r = r or {}
            retry_after = float(r.get("retry_after_s", 0.0) or 0.0)
            if retry_after > 0.0:
                self._retry_after_until_ts = time.time() + retry_after
                self.emit_event(type="alert", level="WARN", message="rate_limited", retry_after_s=retry_after)
            if r.get("error"):
                self.emit_event(type="alert", level="WARN", message="discovery_error", error=str(r.get("error")))
            return r
        except Exception as e:
            self.emit_event(type="alert", level="ERROR", message=f"discover_fn crashed: {e}")
            return {}

    @staticmethod
    def _compute_diff(cur: Set[str], nxt: Set[str]) -> Tuple[Set[str], Set[str], float]:
        add = nxt - cur
        rem = cur - nxt
        base = max(1, len(cur))
        ratio = (len(add) + len(rem)) / float(base)
        return add, rem, ratio

    @staticmethod
    def _candidate_key(pairs: List[str], mapping: Dict[str, Any]) -> str:
        # Une clé compacte et déterministe pour détecter les changements de proposition
        return f"p:{hash(tuple(sorted(pairs)))}|m:{hash(tuple(sorted(mapping.keys())))}"

    # -------------- status --------------
    def get_status(self) -> Dict[str, Any]:
        s = super().get_status()
        s.update({
            "module": "PairsDiscoveryWatchdog",
            "metrics": {**s.get("metrics", {}),
                         "last_discovery_ts": self._last_discovery_ts,
                         "retry_after_until_ts": self._retry_after_until_ts,
                         "confirm_count": self._confirm_count,
                         "dwell_ticks": self.cfg.thresholds.dwell_ticks,
                         "confirm_ticks": self.cfg.thresholds.confirm_ticks,
                         "min_change_ratio": self.cfg.thresholds.min_change_ratio},
        })
        return s

    def _evaluate_snapshot(self, snapshot: Dict[str, Any]) -> Tuple[str, list[str], Dict[str, Any]]:
        reasons: list[str] = []
        details: Dict[str, Any] = {}
        now = time.time()
        last = float(snapshot.get("last_discovery_ts") or 0.0)
        if last <= 0:
            reasons.append("MISSING_FIELD")
            details["missing_fields"] = ["last_discovery_ts"]
        else:
            gap = now - last
            details["refresh_gap_s"] = gap
            if gap >= self.cfg.thresholds.max_refresh_gap_s:
                reasons.append("WD_STALE")
        severity = "OK"
        if "WD_STALE" in reasons:
            severity = "WARN"
        elif reasons:
            severity = "WARN"
        return severity, reasons, details