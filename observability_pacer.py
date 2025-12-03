"""
observability_pacer.py — Pacer global NORMAL/DEGRADED/SEVERE avec clamps dynamiques.
Usage:
    from modules.observability_pacer import PACER
    delay = PACER.clamp("pws_connect")  # secondes à attendre avant une action
    state = PACER.state()               # "NORMAL"|"DEGRADED"|"SEVERE"
"""
from __future__ import annotations
import time, threading, logging
from dataclasses import dataclass, field
from typing import Dict, Callable, Optional
try:
    from modules.obs_metrics import PACER_STATE, PACER_CLAMP_SECONDS
except Exception:

    class _Noop:

        def labels(self, *_, **__):
            return self

        def inc(self, *_, **__):
            pass

        def observe(self, *_, **__):
            pass

        def set(self, *_, **__):
            pass
    PACER_STATE = PACER_CLAMP_SECONDS = _Noop()
_STATE_IDX = {'NORMAL': 0, 'DEGRADED': 1, 'SEVERE': 2}

class _Pacer:

    def __init__(self) -> None:
        self._clamps = {'pws_connect': {'NORMAL': 0.0, 'DEGRADED': 0.2, 'SEVERE': 0.5}, 'pws_reconnect': {'NORMAL': 0.0, 'DEGRADED': 0.3, 'SEVERE': 0.8}, 'bf_request': {'NORMAL': 0.0, 'DEGRADED': 0.1, 'SEVERE': 0.25}, 'bf_rate': {'NORMAL': 0.0, 'DEGRADED': 0.1, 'SEVERE': 0.25}, 'reco_pass': {'NORMAL': 0.0, 'DEGRADED': 0.2, 'SEVERE': 0.6}, 'cold_resync': {'NORMAL': 0.0, 'DEGRADED': 0.0, 'SEVERE': 0.0}}
        self._state = 'NORMAL'
        self._lock = threading.RLock()
        PACER_STATE.set(_STATE_IDX[self._state])

    def configure(self, kind: str, normal: float, degraded: float, severe: float) -> None:
        with self._lock:
            self._clamps[kind] = {'NORMAL': float(normal), 'DEGRADED': float(degraded), 'SEVERE': float(severe)}

    def set_state(self, state: str) -> None:
        s = str(state).upper()
        if s not in _STATE_IDX:
            return
        with self._lock:
            self._state = s
            PACER_STATE.set(_STATE_IDX[s])

    def state(self) -> str:
        with self._lock:
            return self._state

    def clamp(self, kind: str) -> float:
        with self._lock:
            val = float(self._clamps.get(kind, {}).get(self._state, 0.0))
        try:
            PACER_CLAMP_SECONDS.labels(kind).set(val)
        except Exception:
            pass
        return val
PACER = _Pacer()

# --- [Alerts] AlertDispatcher centralisé (M5-B3) ------------------------------

_alert_log = logging.getLogger("alert_dispatcher")


@dataclass
class AlertEvent:
    """
    Event d'alerte standardisé, consommable par un sink externe
    (Telegram, email, CentralWatchdog...).
    """
    code: str
    severity: str  # "INFO" | "WARNING" | "CRIT"
    message: str
    ts: float
    labels: Dict[str, str] = field(default_factory=dict)


class AlertDispatcher:
    """
    Dispatcher central d'alertes avec anti-spam simple.

    M5-B3: expose notamment les helpers:
      - alert_pnl_lhm_lag(...)
      - alert_pnl_lhm_drops_trade(...)

    Le sink optionnel est une fonction Callable[[AlertEvent], None]
    (ex: wrapper Telegram/email ou bus CentralWatchdog).
    """

    def __init__(
        self,
        *,
        sink: Optional[Callable[[AlertEvent], None]] = None,
        max_per_window: int = 50,
        window_seconds: float = 60.0,
    ) -> None:
        self._sink = sink
        self._max_per_window = max(1, int(max_per_window))
        self._window_seconds = float(window_seconds)
        self._lock = threading.RLock()
        self._window_start = time.time()
        self._count = 0
        self._last_per_code: Dict[tuple[str, str], float] = {}

    # --- gestion du sink -----------------------------------------------------

    def set_sink(self, sink: Optional[Callable[[AlertEvent], None]]) -> None:
        """Configure ou remplace le sink externe (best-effort)."""
        with self._lock:
            self._sink = sink

    # --- anti-spam -----------------------------------------------------------

    def _allow(self, code: str, severity: str, min_interval_s: float) -> bool:
        """
        Retourne True si l'alerte peut être émise (hystérésis + budget fenêtre).
        """
        now = time.time()
        key = (code, severity)
        with self._lock:
            # Fenêtre glissante globale
            if now - self._window_start >= self._window_seconds:
                self._window_start = now
                self._count = 0

            if self._count >= self._max_per_window:
                return False

            last = self._last_per_code.get(key, 0.0)
            if now - last < min_interval_s:
                return False

            self._last_per_code[key] = now
            self._count += 1

        return True

    # --- API principale ------------------------------------------------------

    def emit(self, code: str, severity: str, message: str, **labels: str) -> None:
        """
        Émet une alerte best-effort (log + sink optionnel).

        severity est normalisé en {"INFO","WARNING","CRIT"}.
        """
        sev = (severity or "INFO").upper()
        if sev not in ("INFO", "WARNING", "CRIT"):
            sev = "INFO"

        # Hystérésis par défaut:
        #  - INFO    → au plus 1 toutes les 30s
        #  - WARNING → au plus 1 toutes les 60s
        #  - CRIT    → au plus 1 toutes les 10s
        base_interval = 30.0 if sev == "INFO" else 60.0 if sev == "WARNING" else 10.0
        if not self._allow(code, sev, base_interval):
            return

        evt = AlertEvent(
            code=code,
            severity=sev,
            message=message,
            ts=time.time(),
            labels=dict(labels) if labels else {},
        )

        # Log local (toujours)
        try:
            _alert_log.warning(
                "[%s][%s] %s | labels=%r", evt.code, evt.severity, evt.message, evt.labels
            )
        except Exception:
            # Surtout ne jamais casser le flux métier
            pass

        # Sink externe (optionnel)
        sink = None
        with self._lock:
            sink = self._sink
        if sink is not None:
            try:
                sink(evt)
            except Exception:
                _alert_log.exception("Alert sink failed for %s", evt.code)

    # --- Helpers dédiés PnL LHM (M5-B3) --------------------------------------

    def alert_pnl_lhm_lag(
        self,
        lag_seconds: float,
        slo_seconds: float,
        *,
        mode: str = "online",
    ) -> None:
        """
        Alerte dédiée au lag du pipeline LHM.

        - WARNING si lag >= 0.8 * SLO
        - CRIT    si lag >= 1.0 * SLO
        """
        try:
            lag = float(lag_seconds)
            slo = max(0.0, float(slo_seconds))
        except Exception:
            return

        if slo <= 0.0:
            return

        ratio = lag / slo if slo > 0.0 else 0.0
        if ratio < 0.80:
            # En dessous de 80% du SLO, on reste silencieux
            return

        code = "ALERT_PNL_PIPELINE_LHM_LAG"
        severity = "CRIT" if ratio >= 1.0 else "WARNING"
        msg = f"LHM pipeline lag {lag:.2f}s (SLO {slo:.2f}s, ratio={ratio:.2f})"

        self.emit(
            code,
            severity,
            msg,
            kind="lhm_pipeline_lag",
            mode=mode,
            ratio=f"{ratio:.3f}",
        )

    def alert_pnl_lhm_drops_trade(
        self,
        dropped: int,
        budget: float,
        *,
        window: str = "5m",
    ) -> None:
        """
        Alerte dédiée aux drops JSONL sur le stream 'trades'.

        - WARNING si dropped > 0 mais <= budget
        - CRIT    si dropped > budget
        """
        try:
            n = int(dropped)
            b = max(0.0, float(budget))
        except Exception:
            return

        if n <= 0 and b <= 0.0:
            # Rien à signaler
            return

        if b > 0.0 and n > b:
            severity = "CRIT"
        elif n > 0:
            severity = "WARNING"
        else:
            # n == 0 and b > 0 → pas d'alerte
            return

        code = "ALERT_PNL_PIPELINE_LHM_DROPS_TRADE"
        msg = f"Drops JSONL trades détectés: {n} sur fenêtre {window} (budget={b:.0f})"

        self.emit(
            code,
            severity,
            msg,
            kind="lhm_drops_trade",
            window=window,
            budget=str(int(b)),
        )


# Instance globale par défaut (utilisable partout)
ALERT_DISPATCHER = AlertDispatcher()
