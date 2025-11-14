"""
observability_pacer.py — Pacer global NORMAL/DEGRADED/SEVERE avec clamps dynamiques.
Usage:
    from modules.observability_pacer import PACER
    delay = PACER.clamp("pws_connect")  # secondes à attendre avant une action
    state = PACER.state()               # "NORMAL"|"DEGRADED"|"SEVERE"
"""
from __future__ import annotations
import time, threading
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