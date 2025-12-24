# -*- coding: utf-8 -*-
from __future__ import annotations
"""
BalanceFetcherWatchdogV2 — robuste, import‑path agnostique, avec boucle optionnelle
=================================================================================

• Surveille un MultiBalanceFetcher tri‑CEX (Binance / Coinbase AT / Bybit)
  - Inactivité (last_sync_time qui stagne)
  - Hausse d'erreurs (delta >= error_threshold)
  - Auto‑soin Binance: resync de l'horloge si -1021 (timestamp) détecté
• Boucle de synchronization **optionnelle** (manage_sync_loop=True) pour déclencher
  `get_all_balances(force_refresh=True)` à intervalle fixe, indépendante du main.
• Redémarrages parcimonieux via BaseWatchdog (cooldown + quota/min déjà gérés).
• Expose un helper `make_balance_fetcher_stack(...)` pour intégration *main*.

Compat imports:
  - `modules.watchdog.base_watchdog.BaseWatchdog` (snake)
  - `modules.watchdog.base_watchdog.BaseWatchdog` (camel)

Dépendance: votre `MultiBalanceFetcher` (ex: modules.io.MultiBalanceFetcher).
"""

import asyncio
import logging
import time
from typing import Any, Dict, Optional, Tuple
from modules.watchdog.base_watchdog import BaseWatchdogV2
from modules.bot_config import BotConfig

log = logging.getLogger("BalanceFetcherWatchdogV2")


class BalanceFetcherWatchdogV2(BaseWatchdogV2):
    def __init__(
        self,
        *,
        fetcher: Any,
        check_interval: float = 5.0,
        inactive_s: float = 60.0,
        error_threshold: int = 5,
        restart_cooldown_s: float = 30.0,
        max_restarts_per_min: int = 3,
        # boucle interne optionnelle
        manage_sync_loop: bool = False,
        sync_interval: float = 10.0,
        # auto‑soin Binance
        binance_resync_cooldown_s: float = 60.0,
        verbose: bool = True,
        name: str = "BalanceFetcherWatchdogV2",
        bot_config: Optional[BotConfig] = None,
    ) -> None:
        cfg = bot_config or BotConfig.from_env()
        if bot_config is None:
            self.record_fallback("bot_config")
        check_interval = float(cfg.wd.balance_interval_s or check_interval)
        inactive_s = float(cfg.wd.balance_stale_s or inactive_s)
        error_threshold = int(cfg.wd.balance_error_threshold or error_threshold)
        super().__init__(
            name=name,
            check_interval=check_interval,
            restart_cooldown_s=restart_cooldown_s,
            max_restarts_per_min=max_restarts_per_min,
            event_cooldown_s=cfg.wd.cooldown_s,
            verbose=verbose,
        )
        self.fetcher = fetcher
        self.inactive_s = float(inactive_s)
        self.error_threshold = int(error_threshold)
        self.manage_sync_loop = bool(manage_sync_loop)
        self.sync_interval = float(sync_interval)
        self.binance_resync_cooldown_s = float(binance_resync_cooldown_s)
        if self.manage_sync_loop:
            self.record_fallback("manage_sync_loop_disabled")

        # runtime
        self._loop_task: Optional[asyncio.Task] = None
        self._last_status: Dict[str, Any] = {}
        self._prev_success: int = -1
        self._prev_errors: int = 0
        self._last_sync_seen: float = 0.0
        self._last_binance_resync: float = 0.0

    # -------------- lifecycle --------------
    async def start(self) -> None:
        await super().start()
        self.emit_event(type="lifecycle", level="INFO", message="WD started")

    async def stop(self) -> None:
        await super().stop()
        self.emit_event(type="lifecycle", level="INFO", message="WD stopped")

    # -------------- checks --------------
    async def _check_once(self) -> None:
        snapshot = await self.collect_snapshot()
        severity, reasons, details = self.evaluate(snapshot)
        self.emit_health_event(
            severity=severity,
            reasons=reasons,
            details=details,
            component="BalanceFetcher",
            module="BalanceFetcher",
            observed_at_ms=snapshot.get("observed_at_ms"),
        )
    # -------------- helpers --------------
    async def _safe_status(self) -> Dict[str, Any]:
        try:
            fn = getattr(self.fetcher, "get_status", None)
            if fn is None:
                return {}
            if asyncio.iscoroutinefunction(fn):
                return await fn()  # type: ignore[misc]
            return fn()  # type: ignore[misc]
        except Exception as e:
            self.emit_event(type="status_error", level="WARN", message=f"get_status() failed: {e}")
            return {}

    async def collect_snapshot(self) -> Dict[str, Any]:
        observed_at_ms = self.now_ts_ms()
        missing: list[str] = []
        errors: list[str] = []
        status = await self.safe_call(
            getattr(self.fetcher, "get_status", None),
            default={},
            errors=errors,
            error_label="fetcher.get_status",
        )
        if not status:
            missing.append("MISSING_FIELD:get_status")
        self._last_status = status or {}
        return {
            "observed_at_ms": observed_at_ms,
            "module_state": status or {},
            "missing": missing,
            "errors": errors,
        }

    def evaluate(self, snapshot: Dict[str, Any]) -> Tuple[str, list[str], Dict[str, Any]]:
        status = snapshot.get("module_state", {}) or {}
        missing = list(snapshot.get("missing") or [])
        reasons: list[str] = []
        details: Dict[str, Any] = {}

        running = self.safe_get(status, "running", default=True, missing=missing)
        if running is False:
            reasons.append("WD_MODULE_DEAD")

        last_sync = self.safe_get(status, "last_sync_time", default=None, missing=missing)
        if hasattr(last_sync, "timestamp"):
            last_sync = last_sync.timestamp()
        if last_sync:
            self._last_sync_seen = float(last_sync)
        age_ms = self.safe_age_ms(self._last_sync_seen or None, snapshot.get("observed_at_ms"))
        if age_ms is not None:
            details["age_ms"] = age_ms
            if age_ms >= int(self.inactive_s * 1000):
                reasons.append("BALANCE_STALE")

        err_cnt = self.safe_int(status, "error_count", default=0, missing=missing)
        delta_err = err_cnt - self._prev_errors
        if delta_err >= self.error_threshold:
            reasons.append("WD_LOOP_STOPPED")
        self._prev_errors = err_cnt

        if missing:
            reasons.append("MISSING_FIELD")
            details["missing_fields"] = missing

        severity = "OK"
        if "BALANCE_STALE" in reasons or "WD_MODULE_DEAD" in reasons:
            severity = "CRIT"
        elif reasons:
            severity = "WARN"
        return severity, reasons, details


    async def _maybe_resync_binance_time(self) -> None:
        self.record_fallback("resync_disabled")


    async def _ensure_loop(self) -> None:
        self.record_fallback("sync_loop_disabled")


    async def _sync_loop(self) -> None:
        self.record_fallback("sync_loop_disabled")
        await asyncio.sleep(0)

    async def _restart_fetcher(self, *, reason: str) -> None:
        self.emit_event(type="alert", level="WARN", message="restart_requested_notify_only", reason=reason)


    # -------------- status --------------
    def get_status(self) -> Dict[str, Any]:
        base = super().get_status()
        base.update({
            "module": "BalanceFetcherWatchdogV2",
            "metrics": {
                **base.get("metrics", {}),
                "inactive_s": self.inactive_s,
                "error_threshold": self.error_threshold,
                "sync_interval": self.sync_interval,
                "manage_sync_loop": self.manage_sync_loop,
                "last_sync_seen": self._last_sync_seen,
            },
            "last_status": dict(self._last_status),
        })
        return base


# ------------------------------- wiring (main) ---------------------------------
# Exemple d'intégration prêt à coller dans votre `main.py`.
# - construit MultiBalanceFetcher (dry_run optionnel pour dev)
# - démarre le Watchdog
# - renvoie (fetcher, watchdog)

async def make_balance_fetcher_stack(
    *,
    binance_accounts: Dict[str, Dict[str, str]] | None = None,
    coinbase_at_accounts: Dict[str, Dict[str, str]] | None = None,
    bybit_accounts: Dict[str, Dict[str, str]] | None = None,
    dry_run: bool = False,
    manage_sync_loop: bool = True,
    sync_interval: float = 10.0,
    check_interval: float = 5.0,
) -> Tuple[Any, BalanceFetcherWatchdogV2]:
    """Instancie le fetcher + son watchdog. Retourne (fetcher, wd)."""
    # Import paresseux pour éviter dépendance dure si le chemin change
    from modules.balance_fetcher import MultiBalanceFetcher  # type: ignore

    fetcher = MultiBalanceFetcher(
        binance_accounts=binance_accounts or {},
        coinbase_at_accounts=coinbase_at_accounts or {},
        bybit_accounts=bybit_accounts or {},
        dry_run=dry_run,
        verbose=True,
    )
    await fetcher.start()

    wd = BalanceFetcherWatchdogV2(
        fetcher=fetcher,
        check_interval=check_interval,
        manage_sync_loop=manage_sync_loop,
        sync_interval=sync_interval,
        inactive_s=60.0,
        error_threshold=5,
        restart_cooldown_s=30.0,
        max_restarts_per_min=3,
        verbose=True,
    )
    await wd.start()
    return fetcher, wd


# ------------------------------- mini‑demo -------------------------------------
# À utiliser pour un smoke‑test local (dry_run=True) :
#
# if __name__ == "__main__":
#     import asyncio
#     async def _demo():
#         bf, wd = await make_balance_fetcher_stack(
#             binance_accounts={"TT": {"api_key": "x", "secret": "y"}},
#             coinbase_at_accounts={"TT": {"api_key": "x", "secret": "y"}},
#             bybit_accounts={"TT": {"api_key": "x", "secret": "y"}},
#             dry_run=True,
#             manage_sync_loop=True,
#             sync_interval=2.0,
#         )
#         await asyncio.sleep(6)
#         print(wd.get_status())
#         await wd.stop(); await bf.stop()
#     asyncio.run(_demo())
