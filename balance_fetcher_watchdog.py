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
# ---- BaseWatchdog import résilient ----
try:  # snake
    from modules.watchdog.base_watchdog import BaseWatchdogV2  # type: ignore
except Exception:  # camel fallback
    from modules.watchdog.base_watchdog import BaseWatchdog  # type: ignore

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
    ) -> None:
        super().__init__(
            name=name,
            check_interval=check_interval,
            restart_cooldown_s=restart_cooldown_s,
            max_restarts_per_min=max_restarts_per_min,
            verbose=verbose,
        )
        self.fetcher = fetcher
        self.inactive_s = float(inactive_s)
        self.error_threshold = int(error_threshold)
        self.manage_sync_loop = bool(manage_sync_loop)
        self.sync_interval = float(sync_interval)
        self.binance_resync_cooldown_s = float(binance_resync_cooldown_s)

        # runtime
        self._loop_task: Optional[asyncio.Task] = None
        self._last_status: Dict[str, Any] = {}
        self._prev_success: int = -1
        self._prev_errors: int = 0
        self._last_sync_seen: float = 0.0
        self._last_binance_resync: float = 0.0

    # -------------- lifecycle --------------
    async def start(self) -> None:
        if self.manage_sync_loop and (not self._loop_task or self._loop_task.done()):
            await self._ensure_loop()
        await super().start()
        self.emit_event(type="lifecycle", level="INFO", message="WD started")

    async def stop(self) -> None:
        if self.manage_sync_loop and self._loop_task and not self._loop_task.done():
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                logging.exception("Unhandled exception")
            finally:
                self._loop_task = None
        await super().stop()
        self.emit_event(type="lifecycle", level="INFO", message="WD stopped")

    # -------------- checks --------------
    async def _check_once(self) -> None:
        st = await self._safe_status()
        if not st:
            await self._restart_fetcher(reason="no_status")
            return

        self._last_status = st
        now = time.time()

        last_sync = float(st.get("last_sync_time", 0.0) or 0.0)
        err_cnt = int(st.get("error_count", 0) or 0)
        succ_cnt = int(st.get("successful_syncs", 0) or 0)
        last_err = str(st.get("last_error") or "")

        progressed = (succ_cnt > self._prev_success) if self._prev_success >= 0 else True
        self._prev_success = succ_cnt

        if last_sync > 0:
            self._last_sync_seen = last_sync
        inactive_for = now - (self._last_sync_seen or 0.0)

        # auto‑soin: -1021 (Binance timestamp) → resync horloge (avec cooldown)
        if "-1021" in last_err and (now - self._last_binance_resync) >= self.binance_resync_cooldown_s:
            await self._maybe_resync_binance_time()

        # inactivité prolongée → restart
        if inactive_for >= self.inactive_s:
            await self._restart_fetcher(reason=f"inactive_{int(inactive_for)}s")
            return

        # dérive d'erreurs → restart
        delta_err = err_cnt - self._prev_errors
        if delta_err >= self.error_threshold:
            await self._restart_fetcher(reason=f"errors_rising(+{delta_err})")
        self._prev_errors = err_cnt

        # nudge léger si aucune progression depuis le dernier check
        if not progressed:
            try:
                coro = self.fetcher.get_all_balances(force_refresh=True)
                if asyncio.iscoroutine(coro):
                    asyncio.create_task(coro)
            except Exception:
                logging.exception("Unhandled exception")

        # si on doit gérer la boucle, s'assurer qu'elle tourne
        if self.manage_sync_loop and (not self._loop_task or self._loop_task.done()):
            await self._ensure_loop()

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

    async def _maybe_resync_binance_time(self) -> None:
        try:
            fn = getattr(self.fetcher, "_sync_binance_time_offset", None)
            if not fn:
                return
            res = fn()
            if asyncio.iscoroutine(res):
                await res
            self._last_binance_resync = time.time()
            self.emit_event(type="self_heal", level="INFO", message="Resync Binance server time (-1021)")
        except Exception as e:
            if self.verbose:
                log.warning("[BF WD] Binance time resync failed: %s", e)

    async def _ensure_loop(self) -> None:
        try:
            self._loop_task = asyncio.create_task(self._sync_loop())
            if self.verbose:
                log.info("[%s] internal sync loop started (%.2fs)", self.name, self.sync_interval)
        except Exception as e:
            self.emit_event(type="loop_error", level="ERROR", message=f"unable to start loop: {e}")

    async def _sync_loop(self) -> None:
        try:
            while True:
                try:
                    coro = self.fetcher.get_all_balances(force_refresh=True)
                    if asyncio.iscoroutine(coro):
                        await coro
                except Exception as e:
                    if self.verbose:
                        log.warning("[BF WD] sync tick failed: %s", e)
                await asyncio.sleep(self.sync_interval)
        except asyncio.CancelledError:
            logging.exception("Unhandled exception")
    async def _restart_fetcher(self, *, reason: str) -> None:
        async def _stop():
            fn = getattr(self.fetcher, "stop", None)
            if fn:
                r = fn()
                if asyncio.iscoroutine(r):
                    await r

        async def _start():
            fn = getattr(self.fetcher, "start", None)
            if fn:
                r = fn()
                if asyncio.iscoroutine(r):
                    await r
            if self.manage_sync_loop:
                # redémarre aussi la boucle interne
                if self._loop_task and not self._loop_task.done():
                    self._loop_task.cancel()
                    try:
                        await self._loop_task
                    except asyncio.CancelledError:
                        logging.exception("Unhandled exception")
                await self._ensure_loop()

        await self._restart_component(reason=reason, stop_fn=_stop, start_fn=_start, post_delay_s=0.0)
        self.emit_event(type="module_restarted", level="WARN", message="BalanceFetcher restarted", reason=reason)

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
