# modules/WatchDog/base_watchdog.py
from __future__ import annotations

import asyncio, logging, time, os
from collections import defaultdict, deque
from typing import Any, Awaitable, Callable, Deque, Dict, Iterable, Optional
from modules.obs_metrics import (
    watchdog_fallback_used,
    watchdog_degraded_total,
    watchdog_restart_intent_total,
    watchdog_snapshot_total,
)
from contracts.payloads import normalize_reason_code
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
        self._last_snapshot: Dict[str, Any] = {}

    @staticmethod
    def now_ts_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _coerce_severity(severity: str) -> str:
        sev = str(severity or "").upper()
        return sev if sev in ("OK", "WARN", "CRIT") else "OK"

    def safe_get(
            self,
            obj: Any,
            path: str | Iterable[str],
            default: Any = None,
            *,
            missing: Optional[list[str]] = None,
    ) -> Any:
        if obj is None:
            if missing is not None:
                missing.append(f"MISSING_FIELD:{path}")
            return default
        steps = [path] if isinstance(path, str) else list(path)
        cur = obj
        for key in steps:
            try:
                if isinstance(cur, dict):
                    if key in cur:
                        cur = cur[key]
                    else:
                        if missing is not None:
                            missing.append(f"MISSING_FIELD:{key}")
                        return default
                else:
                    if hasattr(cur, str(key)):
                        cur = getattr(cur, str(key))
                    else:
                        if missing is not None:
                            missing.append(f"MISSING_FIELD:{key}")
                        return default
            except Exception:
                if missing is not None:
                    missing.append(f"MISSING_FIELD:{key}")
                return default
        return default if cur is None else cur

    def safe_int(
            self,
            obj: Any,
            path: str | Iterable[str],
            default: int = 0,
            *,
            missing: Optional[list[str]] = None,
    ) -> int:
        value = self.safe_get(obj, path, default=None, missing=missing)
        try:
            return int(value)
        except Exception:
            return int(default)

    def safe_float(
            self,
            obj: Any,
            path: str | Iterable[str],
            default: float = 0.0,
            *,
            missing: Optional[list[str]] = None,
    ) -> float:
        value = self.safe_get(obj, path, default=None, missing=missing)
        try:
            return float(value)
        except Exception:
            return float(default)

    def safe_ts_ms(
            self,
            obj: Any,
            path: str | Iterable[str],
            default: Optional[int] = None,
            *,
            missing: Optional[list[str]] = None,
    ) -> Optional[int]:
        value = self.safe_get(obj, path, default=None, missing=missing)
        if value is None:
            return default
        try:
            ts = float(value)
        except Exception:
            return default
        if ts <= 0:
            return default
        if ts < 1e12:
            ts *= 1000.0
        return int(ts)

    async def safe_call(
            self,
            fn: Optional[Callable[..., Any]],
            *args: Any,
            default: Any = None,
            errors: Optional[list[str]] = None,
            error_label: Optional[str] = None,
            **kwargs: Any,
    ) -> Any:
        if not callable(fn):
            if errors is not None:
                errors.append(error_label or "callable_missing")
            return default
        try:
            res = fn(*args, **kwargs)
            if asyncio.iscoroutine(res):
                return await res
            return res
        except Exception as exc:
            if errors is not None:
                errors.append(error_label or f"call_failed:{exc}")
            return default

    @staticmethod
    def safe_len(obj: Any, default: int = 0) -> int:
        try:
            return int(len(obj))  # type: ignore[arg-type]
        except Exception:
            return int(default)

    @staticmethod
    def safe_age_ms(last_ts: Optional[float | int], now_ms: Optional[int] = None) -> Optional[int]:
        if last_ts is None:
            return None
        try:
            last_val = float(last_ts)
        except Exception:
            return None
        if now_ms is None:
            now_ms = BaseWatchdogV2.now_ts_ms()
        if last_val <= 0:
            return None
        if last_val < 1e12:
            last_val *= 1000.0
        return max(0, int(now_ms - last_val))

    def normalize_reason(self, reason: Optional[str]) -> Optional[str]:
        norm = normalize_reason_code(reason)
        return norm if norm else None

    def normalize_reasons(self, reasons: Iterable[str]) -> list[str]:
        out: list[str] = []
        for reason in reasons:
            norm = self.normalize_reason(reason)
            if norm:
                out.append(norm)
        return out

    def record_fallback(self, key: str) -> None:
        try:
            self.log.warning("[%s] ⚠️ fallback threshold used: %s", self.name, key)
        except Exception:
            pass
        watchdog_fallback_used(self.name, key)

    def build_snapshot(
            self,
            healthy: bool,
            severity: str,
            reasons: Iterable[str],
            details: Dict[str, Any],
            *,
            module: Optional[str] = None,
            observed_at_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        sev = self._coerce_severity(severity)
        observed_ms = int(observed_at_ms or self.now_ts_ms())
        normalized_reasons = self.normalize_reasons(reasons)
        details_out = dict(details or {})
        missing_fields = details_out.get("missing_fields") or []
        if isinstance(missing_fields, (list, tuple)) and missing_fields:
            if "MISSING_FIELD" not in normalized_reasons:
                normalized_reasons.append("MISSING_FIELD")
            for key in sorted({str(k) for k in missing_fields}):
                self.record_fallback(key)
        elif "MISSING_FIELD" in normalized_reasons:
            self.record_fallback("MISSING_FIELD")

        snapshot = {
            "watchdog": self.name,
            "module": module or self.name,
            "healthy": bool(healthy),
            "severity": sev,
            "reasons": normalized_reasons,
            "observed_at_ms": observed_ms,
            "details": details_out,
        }
        watchdog_snapshot_total(self.name, sev)
        if sev in ("WARN", "CRIT"):
            for reason in normalized_reasons:
                watchdog_degraded_total(self.name, reason)
        self._last_snapshot = dict(snapshot)
        return snapshot

    def emit_health_event(
            self,
            *,
            severity: str,
            reasons: Iterable[str],
            details: Dict[str, Any],
            component: Optional[str] = None,
            module: Optional[str] = None,
            observed_at_ms: Optional[int] = None,
            **dims: Any,
    ) -> None:
        snapshot = self.build_snapshot(
            healthy=(self._coerce_severity(severity) == "OK"),
            severity=severity,
            reasons=reasons,
            details=details,
            module=module,
            observed_at_ms=observed_at_ms,
        )
        payload: Dict[str, Any] = dict(snapshot)
        payload["component"] = component or module or self.name
        payload.update({k: v for k, v in dims.items() if v is not None})
        msg = f"health_snapshot:{snapshot['severity']}:{','.join(snapshot['reasons']) if snapshot['reasons'] else 'ok'}"
        self.emit_event(
            type="health_snapshot",
            level=("INFO" if snapshot["severity"] == "OK" else snapshot["severity"]),
            message=msg,
            **payload,
        )
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
            "last_snapshot": dict(self._last_snapshot),
        }

    def get_health_snapshot(self) -> Dict[str, Any]:
        if self._last_snapshot:
            return dict(self._last_snapshot)
        details = {"running": bool(self._running)}
        return self.build_snapshot(
            healthy=bool(self._running),
            severity="OK" if self._running else "CRIT",
            reasons=[],
            details=details,
            module=self.name,
            observed_at_ms=self.now_ts_ms(),
        )