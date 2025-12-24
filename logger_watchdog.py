# path: modules/watchdog/logger_watchdog_v2.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import asyncio, logging, time
from collections import deque
from typing import Any, Dict, Optional,Tuple
from modules.watchdog.base_watchdog import BaseWatchdogV2
from modules.bot_config import BotConfig
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
            bot_config: Optional[BotConfig] = None,
    ) -> None:
        cfg = bot_config or BotConfig.from_env()
        if bot_config is None:
            self.record_fallback("bot_config")
        check_interval = float(cfg.wd.logger_interval_s or check_interval)
        trade_queue_warn = int(cfg.wd.logger_trade_queue_warn or trade_queue_warn)
        trade_queue_crit = int(cfg.wd.logger_trade_queue_crit or trade_queue_crit)
        queue_stuck_checks = int(cfg.wd.logger_queue_stuck_checks or queue_stuck_checks)
        writer_stall_s = float(cfg.wd.logger_writer_stall_s or writer_stall_s)
        min_queue_for_writer_stall = int(cfg.wd.logger_min_queue_for_writer_stall or min_queue_for_writer_stall)
        rotation_stall_factor = float(cfg.wd.logger_rotation_stall_factor or rotation_stall_factor)
        rotation_hard_stall_factor = float(cfg.wd.logger_rotation_hard_stall_factor or rotation_hard_stall_factor)
        super().__init__(
            name=name,
            check_interval=check_interval,
            restart_cooldown_s=restart_cooldown_s,
            max_restarts_per_min=max_restarts_per_min,
            event_cooldown_s=cfg.wd.cooldown_s,
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
        self.emit_event(
            type="alert",
            level="WARN",
            message="restart_requested_notify_only",
            module="LoggerHistoriqueManager",
            reason=reason,
        )
    # ------------------------- core check -------------------------

    async def _check_once(self) -> None:
        snapshot = await self.collect_snapshot()
        severity, reasons, details = self.evaluate(snapshot)
        self.emit_health_event(
            severity=severity,
            reasons=reasons,
            details=details,
            component="LoggerHistoriqueManager",
            observed_at_ms=snapshot.get("observed_at_ms"),
        )
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

    async def collect_snapshot(self) -> Dict[str, Any]:
        self._last_check_ts = time.time()
        observed_at_ms = self.now_ts_ms()
        missing: list[str] = []
        errors: list[str] = []
        status = await self.safe_call(getattr(self.mgr, "get_status", None), default={}, errors=errors,
                                      error_label="mgr.get_status")
        if not status:
            missing.append("get_status")
        return {
            "observed_at_ms": observed_at_ms,
            "module_state": status or {},
            "missing": missing,
            "errors": errors,
        }

    def evaluate(self, snapshot: Dict[str, Any]) -> Tuple[str, list[str], Dict[str, Any]]:
        st = snapshot.get("module_state", {}) or {}
        missing = list(snapshot.get("missing") or [])
        reasons: list[str] = []
        details: Dict[str, Any] = {}

        writer = st.get("writer") or {}
        tracker = st.get("tracker") or {}
        qsize = self.safe_get(st, "trade_queue_size", default=0, missing=missing)
        qsize = int(qsize or 0)
        self._q_hist.append(qsize)

        if qsize >= self.trade_queue_crit:
            reasons.append("WD_QUEUE_BACKLOG")
        elif qsize >= self.trade_queue_warn:
            reasons.append("WD_QUEUE_BACKLOG")

        if qsize > 0 and len(self._q_hist) == self._q_hist.maxlen and len(set(self._q_hist)) == 1:
            reasons.append("WD_LOOP_STOPPED")

        log_count = writer.get("log_count")
        now = time.time()
        if isinstance(log_count, int):
            if self._last_log_count is None:
                self._last_log_count, self._last_logcount_ts = log_count, now
            elif log_count != self._last_log_count:
                self._last_log_count = log_count
                self._last_logcount_ts = now

        if (
                isinstance(log_count, int)
                and qsize >= max(self.min_queue_for_writer_stall, 1)
                and (now - self._last_logcount_ts) >= self.writer_stall_s
        ):
            reasons.append("WD_LOOP_STOPPED")

        rotation_status = st.get("rotation_status") or {}
        last_rotation_ts = rotation_status.get("last_rotation_ts")
        if last_rotation_ts:
            self._last_rotation_ts = float(last_rotation_ts)
        if self._last_rotation_ts > 0:
            age = now - self._last_rotation_ts
            details["rotation_age_s"] = age
            rotate_every_s = float(st.get("rotate_every_s") or 0.0)
            stall_window_soft = (rotate_every_s * self.rotation_stall_factor) if rotate_every_s > 0 else 180.0
            stall_window_hard = (rotate_every_s * self.rotation_hard_stall_factor) if rotate_every_s > 0 else 360.0
            if age >= stall_window_hard:
                reasons.append("WD_LOOP_STOPPED")
            elif age >= stall_window_soft:
                reasons.append("WD_STALE")

        if missing:
            reasons.append("MISSING_FIELD")
            details["missing_fields"] = missing

        severity = "OK"
        if "WD_LOOP_STOPPED" in reasons:
            severity = "CRIT"
        elif reasons:
            severity = "WARN"
        return severity, reasons, details
