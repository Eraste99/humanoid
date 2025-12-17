# modules/WatchDog/base_watchdog.py
from __future__ import annotations
import asyncio, logging, time, os
from collections import defaultdict, deque
from typing import Any, Awaitable, Callable, Deque, Dict, Optional
EventSink = Callable[[Dict[str, Any]], Any]  # sync ou async

class BaseWatchdogV2:
    """
    Boucle async + emit_event() unifié + helpers de restart.
    Patch v2.1: support **notify_only** (ceinture) : toute demande de restart devient un **évènement**.
    - Priorité: NOTIFY_ONLY env (WATCHDOG_NOTIFY_ONLY=1 par défaut) > param notify_only > False
    """

    def __init__(
        self,
        *,
        name: str,
        check_interval: float = 5.0,
        restart_cooldown_s: float = 15.0,
        max_restarts_per_min: int = 3,
        event_cooldown_s: float = 5.0,
        verbose: bool = True,
        logger: Optional[logging.Logger] = None,
        event_sink: Optional[EventSink] = None,
        notify_only: Optional[bool] = None,     # <<< NEW (compat total, optionnel)
    ) -> None:
        self.name = name
        self.check_interval = float(check_interval)
        self.restart_cooldown = float(restart_cooldown_s)
        self.max_restarts_per_min = int(max_restarts_per_min)
        self.event_cooldown = float(event_cooldown_s)
        self.verbose = verbose

        self.log = logger or logging.getLogger(self.name)
        self._sink: Optional[EventSink] = event_sink

        # --- CEINTURE notify-only ---
        env_flag = str(os.getenv("WATCHDOG_NOTIFY_ONLY", "1")).strip().lower()
        env_default = env_flag in ("1", "true", "yes", "on")
        self.notify_only: bool = env_default if notify_only is None else bool(notify_only)

        # boucle
        self._running = False
        self._task: Optional[asyncio.Task] = None

        # métriques
        self.last_check_ts: float = 0.0
        self.restart_count: int = 0
        self.last_restart_reason: Optional[str] = None
        self._last_restart_ts: float = 0.0
        self._restart_times: Deque[float] = deque(maxlen=20)

        # anti-spam d’évènements & compteurs consécutifs
        self._last_event_ts: Dict[str, float] = {}
        self._event_cooldown_ts: Dict[str, float] = {}
        self._consec: Dict[str, int] = defaultdict(int)

    # -------- lifecycle --------
    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        if self.verbose:
            self.log.info(f"[{self.name}] ✅ Démarré")

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self.verbose:
            self.log.info(f"[{self.name}] 🛑 Stoppé")

    async def restart(self, reason: str = "manual") -> None:
        # CEINTURE: en notify-only, on **n’exécute pas** de restart
        if self.notify_only:
            self.emit_event(
                type="alert", level="WARN",
                message=f"restart_skipped_notify_only:{reason}",
                intent="request_full_restart"
            )
            return
        await self.stop()
        self._mark_restart(reason)
        await self.start()

    # -------- core loop --------
    async def _loop(self) -> None:
        try:
            while self._running:
                self.last_check_ts = time.time()
                try:
                    await self._check_once()
                except Exception as e:
                    self.emit_event(type="error", level="ERROR", message=f"_check_once raised: {e}")
                await asyncio.sleep(max(0.05, self.check_interval))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.emit_event(type="error", level="ERROR", message=f"loop crashed: {e}")

    # -------- events --------
    def register_event_sink(self, sink: EventSink) -> None:
        self._sink = sink

    def _cooldown_key(self, type: str, message: str) -> str:
        return f"{type}:{message}"

    def emit_event(self, *, type: str, level: str, message: str, **data: Any) -> None:
        # anti-spam simple par clé
        key = self._cooldown_key(type, message)
        now = time.time()
        last = self._event_cooldown_ts.get(key, 0.0)
        if (now - last) < self.event_cooldown:
            return
        self._event_cooldown_ts[key] = now

        evt = {
            "ts": now,
            "watchdog": self.name,
            "type": type,
            "level": level,
            "message": message,
            **({"data": data} if data else {}),
        }

        # Logging local
        if self.verbose:
            if   level == "ERROR": self.log.error(f"[{self.name}] {type}: {message} | {data}")
            elif level in ("WARN", "WARNING"): self.log.warning(f"[{self.name}] {type}: {message} | {data}")
            else: self.log.info(f"[{self.name}] {type}: {message} | {data}")

        # Callback (sync/async) best-effort (ne jamais bloquer)
        if callable(self._sink):
            try:
                res = self._sink(evt)
                if asyncio.iscoroutine(res):
                    asyncio.create_task(res)
            except Exception:
                pass

    # -------- restart helpers --------
    def _can_restart(self) -> bool:
        now = time.time()
        if (now - self._last_restart_ts) < self.restart_cooldown:
            return False
        one_min = now - 60.0
        times = self._restart_times
        while times and times[0] < one_min:
            times.popleft()
        return len(times) < self.max_restarts_per_min

    def _mark_restart(self, reason: str) -> None:
        now = time.time()
        self._last_restart_ts = now
        self._restart_times.append(now)
        self.restart_count += 1
        self.last_restart_reason = reason
        self.emit_event(type="restart", level="WARN", message=f"Restart ({reason})")

    async def _restart_component(
        self,
        *,
        reason: str,
        stop_fn: Optional[Callable[[], Awaitable[Any] | Any]] = None,
        start_fn: Optional[Callable[[], Awaitable[Any] | Any]] = None,
        post_delay_s: float = 0.0,
    ) -> None:
        # CEINTURE: notify-only → on **n’exécute pas**; on notifie une intention globale
        if self.notify_only:
            self.emit_event(
                type="alert", level="WARN",
                message=f"restart_requested_notify_only:{reason}",
                intent="request_full_restart"
            )
            return

        if not self._can_restart():
            self.emit_event(type="restart_skip", level="INFO", message=f"Cooldown/quota. Raison={reason}")
            return
        try:
            if stop_fn:
                r = stop_fn()
                if asyncio.iscoroutine(r): await r
            if post_delay_s > 0:
                await asyncio.sleep(post_delay_s)
            if start_fn:
                r2 = start_fn()
                if asyncio.iscoroutine(r2): await r2
            self._mark_restart(reason)
        except Exception as e:
            self.emit_event(type="restart_fail", level="ERROR", message=f"Échec restart: {e}")

    # -------- debounce helpers --------
    def consec_inc(self, key: str, *, reset_other: bool = False) -> int:
        if reset_other: self._consec.clear()
        self._consec[key] += 1
        return self._consec[key]
    def consec_reset(self, key: str) -> None:
        self._consec[key] = 0
    def consec_should_trigger(self, key: str, threshold: int, *, reset_on_fire: bool = True) -> bool:
        n = self._consec.get(key, 0)
        if n >= max(1, threshold):
            if reset_on_fire: self._consec[key] = 0
            return True
        return False

    # -------- status --------
    def get_status(self) -> Dict[str, Any]:
        return {
            "module": self.name,
            "healthy": self._running,
            "details": "OK" if self._running else "stopped",
            "metrics": {
                "check_interval": self.check_interval,
                "restart_cooldown_s": self.restart_cooldown,
                "max_restarts_per_min": self.max_restarts_per_min,
                "restart_count": self.restart_count,
                "last_restart_reason": self.last_restart_reason,
                "last_check_ts": self.last_check_ts,
                "notify_only": self.notify_only,
            },
        }
