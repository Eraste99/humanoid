# path: modules/watchdog/logger_watchdog_v2.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import asyncio, logging, time
from collections import deque
from typing import Any, Dict, Optional
from modules.watchdog.base_watchdog import BaseWatchdogV2
logger = logging.getLogger("LoggerWatchdogV2")


class LoggerWatchdogV2(BaseWatchdogV2):
    """
    Watchdog unifié pour LoggerHistoriqueManager + sous-modules.

    Ce qu’on surveille:
      • BaseTradeLogger  → queue_size WARN/CRIT + “queue plate”
      • LogWriter        → progression log_count (liveness)
      • PairHistoryTracker → “rotation” fraîche; nudge rotate_now() avant restart

    Politique:
      • WARN/CRIT si queue_size dépasse des seuils
      • 'stall_writer' si queue TradeLogger > 0 et log_count n’évolue plus pendant writer_stall_s
      • 'rotation_stale' si aucune rotation tracker depuis rotation_stall_s
      • Restart au niveau Manager; rate-limité par BaseWatchdog
    """

    def __init__(
        self,
        *,
        manager: Any,
        check_interval: float = 2.0,

        # TradeLogger
        trade_queue_warn: int = 200,
        trade_queue_crit: int = 1000,
        queue_stuck_checks: int = 5,          # nb de checks consécutifs avec queue inchangée

        # LogWriter
        writer_stall_s: float = 10.0,         # pas de progression log_count alors que la queue > 0
        min_queue_for_writer_stall: int = 10, # on ignore les micro-buffers

        # Tracker
        rotation_stall_factor: float = 3.0,   # x * manager.rotate_every_s avant WARN
        rotation_hard_stall_factor: float = 6.0, # x * rotate_every_s avant restart

        restart_cooldown_s: float = 15.0,
        max_restarts_per_min: int = 3,
        name: str = "LoggerWatchdogV2",
        verbose: bool = True,
    ) -> None:
        super().__init__(
            name=name,
            check_interval=check_interval,
            restart_cooldown_s=restart_cooldown_s,
            max_restarts_per_min=max_restarts_per_min,
            verbose=verbose,
        )
        self.mgr = manager

        # thresholds
        self.trade_queue_warn = int(trade_queue_warn)
        self.trade_queue_crit = int(trade_queue_crit)
        self.queue_stuck_checks = int(max(3, queue_stuck_checks))

        self.writer_stall_s = float(writer_stall_s)
        self.min_queue_for_writer_stall = int(min_queue_for_writer_stall)

        self.rotation_stall_factor = float(max(1.0, rotation_stall_factor))
        self.rotation_hard_stall_factor = float(max(self.rotation_stall_factor, rotation_hard_stall_factor))

        # state
        self._last_check_ts: float = 0.0

        self._q_hist: deque[int] = deque(maxlen=self.queue_stuck_checks)
        self._last_log_count: Optional[int] = None
        self._last_logcount_ts: float = 0.0

        self._last_rotation_seen: Optional[float] = None
        self._last_rotation_ts: float = 0.0
        self._nudge_rotate_ts: float = 0.0

    # ------------------------ helpers ------------------------

    def _safe_status(self) -> Dict[str, Any]:
        """Récupère le status agrégé + fallback sur attributs internes si besoin."""
        try:
            st = self.mgr.get_status() if hasattr(self.mgr, "get_status") else {}
        except Exception as e:
            self.emit_event(type="alert", level="CRIT", message=f"manager_status_error:{e}")
            return {}

        # writer
        writer_st = (st or {}).get("writer")
        if not writer_st:
            writer = getattr(self.mgr, "_writer", None)
            if writer and hasattr(writer, "get_status"):
                try:
                    writer_st = writer.get_status()
                except Exception:
                    writer_st = {}
        st["writer"] = writer_st or {}

        # trade_logger
        tl_st = (st or {}).get("trade_logger")
        if not tl_st:
            tl = getattr(self.mgr, "_trade_logger", None)
            if tl and hasattr(tl, "get_status"):
                try:
                    tl_st = tl.get_status()
                except Exception:
                    tl_st = {}
        st["trade_logger"] = tl_st or {}

        # tracker
        tr_st = (st or {}).get("tracker")
        if not tr_st:
            tr = getattr(self.mgr, "_tracker", None)
            if tr and hasattr(tr, "get_status"):
                try:
                    tr_st = tr.get_status()
                except Exception:
                    tr_st = {}
        st["tracker"] = tr_st or {}

        return st or {}

    async def _restart_manager(self, *, reason: str) -> None:
        async def _do_restart():
            if hasattr(self.mgr, "restart"):
                res = self.mgr.restart()
                if asyncio.iscoroutine(res):
                    await res
            else:
                if hasattr(self.mgr, "stop"):
                    r = self.mgr.stop()
                    if asyncio.iscoroutine(r): await r
                if hasattr(self.mgr, "start"):
                    r = self.mgr.start()
                    if asyncio.iscoroutine(r): await r

        await self._restart_component(reason=reason, stop_fn=None, start_fn=_do_restart, post_delay_s=0.0)
        self.emit_event(
            type="module_restarted",
            level="WARN",
            message="LoggerHistoriqueManager redémarré",
            module="LoggerHistoriqueManager",
            reason=reason,
        )

    # ------------------------- core check -------------------------

    async def _check_once(self) -> None:
        self._last_check_ts = time.time()
        st = self._safe_status()
        if not st:
            await self._restart_manager(reason="status_unavailable")
            return

        writer = st.get("writer") or {}
        tlog = st.get("trade_logger") or {}
        tracker = st.get("tracker") or {}

        # ------------- TradeLogger (queue) -------------
        qsize = int(tlog.get("queue_size", 0) or 0)
        self._q_hist.append(qsize)

        if qsize >= self.trade_queue_crit:
            self.emit_event(type="alert", level="CRIT", message=f"TradeLogger queue CRIT={qsize}")
        elif qsize >= self.trade_queue_warn:
            self.emit_event(type="alert", level="WARN", message=f"TradeLogger queue WARN={qsize}")

        # queue “plate” (inchangée N checks d’affilée)
        if qsize > 0 and len(self._q_hist) == self._q_hist.maxlen and len(set(self._q_hist)) == 1:
            await self._restart_manager(reason=f"trade_queue_stuck(q={qsize})")
            return

        # ------------- LogWriter (liveness) -------------
        log_count = writer.get("log_count")
        now = time.time()
        if isinstance(log_count, int):
            if self._last_log_count is None:
                self._last_log_count, self._last_logcount_ts = log_count, now
            else:
                if log_count != self._last_log_count:
                    # progression OK
                    self._last_log_count = log_count
                    self._last_logcount_ts = now

        # writer stall = pas de progression log_count alors que la queue grandit/reste élevée
        if (
            isinstance(log_count, int)
            and qsize >= max(self.min_queue_for_writer_stall, 1)
            and (now - self._last_logcount_ts) >= self.writer_stall_s
        ):
            await self._restart_manager(reason=f"stall_writer(q={qsize},no_progress={now - self._last_logcount_ts:.1f}s)")
            return

        # ------------- Tracker (rotation fraîche) -------------
        rotate_every_s = float(st.get("rotate_every_s") or 0.0)
        last_rotation = float(tracker.get("last_rotation") or 0.0)  # epoch s attendu par PairHistoryTracker

        # mémorise la dernière valeur observée (utile si 0 au boot)
        if last_rotation > 0:
            if (self._last_rotation_seen is None) or (last_rotation != self._last_rotation_seen):
                self._last_rotation_seen = last_rotation
                self._last_rotation_ts = now

        stall_window_soft = (rotate_every_s * self.rotation_stall_factor) if rotate_every_s > 0 else 180.0
        stall_window_hard = (rotate_every_s * self.rotation_hard_stall_factor) if rotate_every_s > 0 else 360.0

        # si l’âge depuis le dernier changement observé dépasse soft → nudge rotate; si > hard → restart
        if self._last_rotation_ts > 0:
            age = now - self._last_rotation_ts
            if age >= stall_window_soft and age < stall_window_hard:
                # nudge une seule fois toutes les 30s
                if now - self._nudge_rotate_ts >= 30.0 and hasattr(self.mgr, "rotate_now"):
                    try:
                        self.mgr.rotate_now()
                        self._nudge_rotate_ts = now
                        self.emit_event(type="alert", level="WARN", message=f"tracker_rotation_stale(age={age:.0f}s) → rotate_now()")
                    except Exception:
                        # si le nudge échoue, on escalade tout de suite
                        await self._restart_manager(reason=f"rotation_nudge_failed(age={age:.0f}s)")
                        return
            elif age >= stall_window_hard:
                await self._restart_manager(reason=f"rotation_stale(age={age:.0f}s)")
                return

    # ------------------------- external status -------------------------

    def get_status(self) -> Dict[str, Any]:
        base = super().get_status()
        base.update({
            "module": "LoggerWatchdogV2",
            "last_check": self._last_check_ts,
            "settings": {
                "trade_queue_warn": self.trade_queue_warn,
                "trade_queue_crit": self.trade_queue_crit,
                "writer_stall_s": self.writer_stall_s,
                "min_queue_for_writer_stall": self.min_queue_for_writer_stall,
                "queue_stuck_checks": self.queue_stuck_checks,
                "rotation_stall_factor": self.rotation_stall_factor,
                "rotation_hard_stall_factor": self.rotation_hard_stall_factor,
            },
            "internals": {
                "last_log_count": self._last_log_count,
                "last_logcount_ts": self._last_logcount_ts,
                "last_rotation_seen": self._last_rotation_seen,
                "last_rotation_ts": self._last_rotation_ts,
            },
        })
        return base
