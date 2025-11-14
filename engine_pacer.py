# -*- coding: utf-8 -*-
"""
EnginePacer — industry-like pacing & concurrency controller (EU/US/EU-CB + capital profiles)

Objectif :
- Maintenir un débit d'ordres élevé SANS casser les SLO (latence ack p95, loop-lag p95, err_rate, drain latency).
- Dégrader proprement (TT/TM > MM) via flags : mm_frozen / ioc_only.
- Tenir compte des profils de capital (NANO/MICRO/SMALL/MID/LARGE) via des clamps (inflight, pacing) et fairness per-ex / per-pair.
- Gérer plusieurs régions (EU, US, EU-CB) — EU-CB = Coinbase exécuté depuis EU (latence RTT plus tolérante).
- FSM avec hystérésis et hold-time pour éviter l’effet yo-yo.

Interface principale :
    pacer = EnginePacer(region="EU", capital_profile="NANO", min_ms=0, max_ms=250, init_ms=0, jitter_ms=1)
    pacer.set_region_targets("EU", overrides={...})
    pacer.set_region_targets("US", overrides={...})
    pacer.set_region_targets("EU-CB", overrides={...})
    pacer.set_region_map({"BINANCE":"EU","BYBIT":"EU","COINBASE":"US"})  # utilisé côté Engine si besoin
    pacer.set_capital_profile("MICRO")

    # Dans la boucle de l'engine (toutes les ~1-2s) :
    pacer.update(
        region="EU",
        p95_submit_ack_ms=..., loop_lag_ms=..., err_rate=...,
        queue_depth=..., backpressure=False, reason=None
    )

    # Ping de profondeur de file pour la mesure de drain-latency :
    pacer.on_queue_depth(depth=int, region="EU")

    # Politique à appliquer :
    pol = pacer.policy("EU")  # dict: pacing_ms, inflight_max, flags={ioc_only, mm_frozen}, mode, profile

Notes :
- Le RM gère l’upgrade/downgrade de PROFIL (capital), pas le pacer. Le pacer applique seulement les clamps du profil actif.
- Les métriques Prometheus sont optionnelles (fallback no-op si absentes).
"""
from __future__ import annotations

import time
import random
import threading
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Tuple

# --------- Metrics (Prometheus) optional import ---------
try:
    # Essaie d'importer des métriques standard du projet (fallback si absent)
    from obs_metrics import (
        ENGINE_PACING_BACKPRESSURE_TOTAL,
        ENGINE_DRAIN_LATENCY_MS,
        ENGINE_PACER_DELAY_MS,
        ENGINE_PACER_INFLIGHT_MAX,
        ENGINE_PACER_MODE,
    )
except Exception:  # pragma: no cover
    class _NoOp:
        def labels(self, *args, **kwargs): return self
        def inc(self, *args, **kwargs): return None
        def set(self, *args, **kwargs): return None
        def observe(self, *args, **kwargs): return None
    ENGINE_PACING_BACKPRESSURE_TOTAL = _NoOp()
    ENGINE_DRAIN_LATENCY_MS          = _NoOp()
    ENGINE_PACER_DELAY_MS            = _NoOp()
    ENGINE_PACER_INFLIGHT_MAX        = _NoOp()
    ENGINE_PACER_MODE                = _NoOp()

# --------- Capital profiles (clamps) ---------
# Les bornes ci-dessous contraignent la FSM (inflight & pacing). La FSM ajuste à l’intérieur.
_CAPITAL_PROFILES: Dict[str, Dict[str, Any]] = {
    "NANO":  dict(inflight_min=1, inflight_max=1, per_ex=1, per_pair=1, pacing_min=10,  pacing_max=250, base_pacing=150, mm_allowed=False),
    "MICRO": dict(inflight_min=2, inflight_max=4, per_ex=2, per_pair=1, pacing_min=5,   pacing_max=200, base_pacing=100, mm_allowed=False),
    "SMALL": dict(inflight_min=4, inflight_max=8, per_ex=4, per_pair=2, pacing_min=2,   pacing_max=160, base_pacing=60,  mm_allowed=True),
    "MID":   dict(inflight_min=8, inflight_max=16, per_ex=8, per_pair=3, pacing_min=0,  pacing_max=120, base_pacing=30,  mm_allowed=True),
    "LARGE": dict(inflight_min=16, inflight_max=32, per_ex=16, per_pair=4, pacing_min=0, pacing_max=90, base_pacing=20,  mm_allowed=True),
}

# --------- Default regional targets ---------
# Cibles (hautes/sévères) utilisées pour normaliser les gaps → score de sévérité S∈[0,1].
_DEFAULT_TARGETS: Dict[str, Dict[str, float]] = {
    "EU":    dict(ack_target=90.0,  ack_hi=120.0, ack_sev=150.0, lag_hi=15.0, lag_sev=20.0, err_hi=0.005, err_sev=0.02, drain_hi=120.0, drain_sev=240.0),
    "US":    dict(ack_target=150.0, ack_hi=180.0, ack_sev=210.0, lag_hi=15.0, lag_sev=20.0, err_hi=0.005, err_sev=0.02, drain_hi=120.0, drain_sev=240.0),
    # EU-CB = Coinbase exécuté depuis EU (RTT transatlantique → ack plus tolérant)
    "EU-CB": dict(ack_target=180.0, ack_hi=210.0, ack_sev=240.0, lag_hi=15.0, lag_sev=20.0, err_hi=0.005, err_sev=0.02, drain_hi=120.0, drain_sev=240.0),
}

# --------- FSM thresholds / knobs ---------
_WARMUP_SECS = 5.0
_ESCALATE_BAD_WINDOWS = 3    # nombre de fenêtres "mauvaises" d’affilée pour monter d’un cran
_DEESCALATE_GOOD_WINDOWS = 2 # fenêtres "bonnes" d’affilée pour redescendre
_HOLD_TIME_FLAGS_SECS = 5.0  # hold-time minimal avant dégel des flags

# pondérations du score S
_W_ACK  = 0.35
_W_LAG  = 0.25
_W_ERR  = 0.25
_W_DRAIN= 0.15

# modes (pour métriques / debug)
_MODE_NORMAL     = 0
_MODE_CONSTRAIN  = 1
_MODE_SEVERE     = 2

# états internes
_STATE_WARMUP = "WARMUP"
_STATE_NORMAL = "NORMAL"
_STATE_CONSTR = "CONSTRAINED"
_STATE_SEVERE = "SEVERE"
_STATE_RECOV  = "RECOVERING"


@dataclass
class RegionState:
    # mesures lissées (on reçoit déjà des p95 de l’engine ; on ne garde qu’un lissage léger)
    ack_ms: float = 0.0
    lag_ms: float = 0.0
    err_rate: float = 0.0
    drain_ms: float = 0.0

    # queue tracking pour mesurer drain latency
    _drain_open_ts: Optional[float] = None

    # FSM
    state: str = _STATE_WARMUP
    bad_win: int = 0
    good_win: int = 0
    last_flag_set_ts: float = 0.0  # hold-time mm_frozen/ioc_only

    # policy courante
    pacing_ms: int = 0
    inflight_max: int = 1
    flags: Dict[str, bool] = field(default_factory=lambda: {"mm_frozen": False, "ioc_only": False})
    mode: int = _MODE_NORMAL

    # derniers inputs (debug / trace)
    last_reason: Optional[str] = None
    last_update_ts: float = 0.0


class EnginePacer:
    """
    Pacer industry-like multirégion, contraint par le profil de capital.
    """
    def __init__(
        self,
        region: str = "EU",
        capital_profile: str = "NANO",
        min_ms: int = 0,
        max_ms: int = 250,
        init_ms: int = 0,
        jitter_ms: int = 1,
        targets_overrides: Optional[Dict[str, Dict[str, float]]] = None,
        region_map: Optional[Dict[str, str]] = None,
    ):
        self._lock = threading.RLock()
        self._default_region = self._norm_region(region)
        self._regions: Dict[str, RegionState] = {}
        self._targets: Dict[str, Dict[str, float]] = dict(_DEFAULT_TARGETS)
        if targets_overrides:
            for k, v in targets_overrides.items():
                self._targets[self._norm_region(k)] = {**self._targets.get(self._norm_region(k), {}), **v}
        self._profile = self._norm_profile(capital_profile)

        # clamps globaux (peuvent être serrés par le profil)
        self._min_ms = int(min_ms)
        self._max_ms = int(max_ms)
        self._init_ms = int(init_ms)
        self._jitter_ms = int(jitter_ms)

        # mapping exchange -> region (hint pour l’Engine si besoin)
        self._region_map = dict(region_map) if region_map else {}

        # initialise l'état de la région par défaut
        self._ensure_region(self._default_region, cold_start=True)

    # ------------------ helpers ------------------

    @staticmethod
    def _norm_region(r: Optional[str]) -> str:
        if not r:
            return "EU"
        r = str(r).upper().replace("_", "-")
        if r.startswith("EU-CB"):
            return "EU-CB"
        if r.startswith("US"):
            return "US"
        return "EU"

    @staticmethod
    def _norm_profile(p: Optional[str]) -> str:
        p = str(p or "NANO").upper()
        return p if p in _CAPITAL_PROFILES else "NANO"

    def _ensure_region(self, region: str, cold_start: bool = False) -> RegionState:
        region = self._norm_region(region)
        st = self._regions.get(region)
        if st is None:
            st = RegionState()
            prof = _CAPITAL_PROFILES[self._profile]
            st.pacing_ms   = max(self._min_ms, min(self._max_ms, max(self._init_ms, prof["base_pacing"])))
            st.inflight_max= prof["inflight_min"]
            st.state = _STATE_WARMUP if cold_start else _STATE_NORMAL
            self._regions[region] = st
        return st

    def _ema(self, prev: float, new: Optional[float], alpha: float = 0.3) -> float:
        if new is None:
            return prev
        if prev == 0.0:
            return float(new)
        return (1.0 - alpha) * prev + alpha * float(new)

    # ------------------ public API ------------------

    def set_capital_profile(self, profile: str) -> None:
        """Changer le profil de capital (clamps)."""
        with self._lock:
            self._profile = self._norm_profile(profile)
            # clamp immédiatement les régions en cours
            for rg, st in self._regions.items():
                self._apply_clamps(rg, st)

    def set_region_targets(self, region: str, overrides: Dict[str, float]) -> None:
        """Surcharger les cibles d'une région (EU/US/EU-CB)."""
        rg = self._norm_region(region)
        with self._lock:
            base = self._targets.get(rg, {})
            self._targets[rg] = {**base, **(overrides or {})}

    def set_region_map(self, ex_to_region: Dict[str, str]) -> None:
        """Déclarer le mapping exchange -> region (utile pour routing côté Engine)."""
        with self._lock:
            self._region_map = {k: self._norm_region(v) for k, v in (ex_to_region or {}).items()}

    # ------------------ update / policy ------------------

    def update(
        self,
        region: Optional[str] = None,
        p95_submit_ack_ms: Optional[float] = None,
        loop_lag_ms: Optional[float] = None,
        err_rate: Optional[float] = None,
        queue_depth: Optional[int] = None,
        backpressure: bool = False,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Mise à jour périodique (≈ toutes les 1–2 s) des mesures et calcul d'une nouvelle policy.
        Renvoie la policy de la région mise à jour.
        """
        rg = self._norm_region(region or self._default_region)
        with self._lock:
            st = self._ensure_region(rg)
            # MAJ mesures (EMA léger)
            st.ack_ms  = self._ema(st.ack_ms,  p95_submit_ack_ms,  alpha=0.3)
            st.lag_ms  = self._ema(st.lag_ms,  loop_lag_ms,        alpha=0.3)
            st.err_rate= self._ema(st.err_rate,err_rate,           alpha=0.3)
            # drain_ms est mis à jour via on_queue_depth; ici on peut lisser un peu si queue_depth==0
            if queue_depth is not None and queue_depth == 0 and st.drain_ms > 0.0:
                st.drain_ms = self._ema(st.drain_ms, st.drain_ms, alpha=0.15)

            st.last_reason = reason
            st.last_update_ts = time.monotonic()

            # Backpressure explicite (ex. RL CEX)
            if backpressure:
                try:
                    ENGINE_PACING_BACKPRESSURE_TOTAL.labels(bucket="explicit", region=rg).inc(1)
                except Exception:
                    pass

            # Calcul du score de sévérité
            targets = self._targets.get(rg, _DEFAULT_TARGETS["EU"])
            S = self._severity_score(st, targets)

            # Transition FSM
            self._fsm_step(rg, st, S)

            # Calcul de la policy
            self._compute_policy(rg, st, S)

            # Metrics
            self._export_metrics(rg, st)

            return self._policy_dict(rg, st)

    def on_queue_depth(self, depth: int, region: Optional[str] = None) -> None:
        """Appel fréquent (dès que la gauge de queue change) pour dater précisément la drain-latency."""
        rg = self._norm_region(region or self._default_region)
        now = time.monotonic()
        with self._lock:
            st = self._ensure_region(rg)
            if depth and st._drain_open_ts is None:
                st._drain_open_ts = now
            elif depth == 0 and st._drain_open_ts is not None:
                elapsed_ms = (now - st._drain_open_ts) * 1000.0
                st._drain_open_ts = None
                # observe et stocke
                st.drain_ms = self._ema(st.drain_ms, elapsed_ms, alpha=0.5)
                try:
                    ENGINE_DRAIN_LATENCY_MS.observe(elapsed_ms)
                except Exception:
                    pass

    def policy(self, region: Optional[str] = None) -> Dict[str, Any]:
        """Obtenir la policy actuelle pour une région (ou la région par défaut)."""
        rg = self._norm_region(region or self._default_region)
        with self._lock:
            st = self._ensure_region(rg)
            return self._policy_dict(rg, st)

    # alias attendu par certains moteurs
    @property
    def current_policy(self) -> Dict[str, Any]:
        return self.policy(self._default_region)

    # ------------------ core logic ------------------

    def _severity_score(self, st: RegionState, tgt: Dict[str, float]) -> float:
        # normalisation des gaps entre "haut" et "sévère" (bride à [0,1])
        def gap(val: float, lo: float, hi: float) -> float:
            if val <= lo: return 0.0
            if val >= hi: return 1.0
            return (val - lo) / max(1e-9, (hi - lo))

        g_ack   = gap(st.ack_ms,  tgt["ack_hi"],   tgt["ack_sev"])
        g_lag   = gap(st.lag_ms,  tgt["lag_hi"],   tgt["lag_sev"])
        g_err   = gap(st.err_rate,tgt["err_hi"],   tgt["err_sev"])
        g_drain = gap(st.drain_ms,tgt["drain_hi"], tgt["drain_sev"])

        S = _W_ACK*g_ack + _W_LAG*g_lag + _W_ERR*g_err + _W_DRAIN*g_drain
        return max(0.0, min(1.0, S))

    def _fsm_step(self, rg: str, st: RegionState, S: float) -> None:
        now = time.monotonic()

        # warmup court : on descend rapidement vers NORMAL
        if st.state == _STATE_WARMUP:
            if (now - st.last_update_ts) >= _WARMUP_SECS:
                st.state = _STATE_NORMAL
            else:
                # pas d'escalade en warmup
                return

        # détection "bonne" ou "mauvaise" fenêtre
        good = (S < 0.25)
        bad  = (S >= 0.65)

        if bad:
            st.bad_win += 1
            st.good_win = 0
        elif good:
            st.good_win += 1
            st.bad_win = 0
        else:
            # neutre
            st.bad_win = max(0, st.bad_win - 1)
            st.good_win = max(0, st.good_win - 1)

        # escalade / désescalade avec hystérésis
        if st.bad_win >= _ESCALATE_BAD_WINDOWS:
            st.bad_win = 0
            if st.state == _STATE_NORMAL:
                st.state = _STATE_CONSTR
            elif st.state in (_STATE_CONSTR, _STATE_RECOV):
                st.state = _STATE_SEVERE
                # set flags + hold-time
                st.flags["mm_frozen"] = True
                st.flags["ioc_only"]  = True
                st.last_flag_set_ts = now
        elif st.good_win >= _DEESCALATE_GOOD_WINDOWS:
            st.good_win = 0
            if st.state == _STATE_SEVERE:
                st.state = _STATE_RECOV
            elif st.state == _STATE_CONSTR:
                st.state = _STATE_NORMAL
            elif st.state == _STATE_RECOV:
                st.state = _STATE_NORMAL

        # hold-time minimal pour les flags
        if st.flags.get("mm_frozen") or st.flags.get("ioc_only"):
            if (now - st.last_flag_set_ts) >= _HOLD_TIME_FLAGS_SECS and st.state in (_STATE_NORMAL, _STATE_RECOV):
                st.flags["mm_frozen"] = False
                st.flags["ioc_only"]  = False

    def _apply_clamps(self, rg: str, st: RegionState) -> None:
        prof = _CAPITAL_PROFILES[self._profile]
        # pacing
        st.pacing_ms = max(
            max(self._min_ms, prof["pacing_min"]),
            min(st.pacing_ms, min(self._max_ms, prof["pacing_max"]))
        )
        # inflight
        st.inflight_max = max(prof["inflight_min"], min(st.inflight_max, prof["inflight_max"]))

    def _compute_policy(self, rg: str, st: RegionState, S: float) -> None:
        prof = _CAPITAL_PROFILES[self._profile]
        # base pacing vers le bas en NORMAL (AIMD) ; sinon multiplicatif
        if st.state == _STATE_NORMAL:
            st.mode = _MODE_NORMAL
            st.pacing_ms = max(prof["pacing_min"], st.pacing_ms - 5)  # AIMD step
            st.inflight_max = min(prof["inflight_max"], max(prof["inflight_min"], st.inflight_max + 1))
        elif st.state == _STATE_CONSTR:
            st.mode = _MODE_CONSTRAIN
            mul = (1.0 + 0.5 * S)
            st.pacing_ms = int(max(prof["pacing_min"], min(prof["pacing_max"], st.pacing_ms * mul)))
            st.inflight_max = max(prof["inflight_min"], int(st.inflight_max / mul))
        elif st.state == _STATE_SEVERE:
            st.mode = _MODE_SEVERE
            mul = (1.0 + 1.5 * S)
            st.pacing_ms = int(max(prof["pacing_min"], min(prof["pacing_max"], st.pacing_ms * mul)))
            st.inflight_max = max(1, int(max(prof["inflight_min"], st.inflight_max / mul)))
            # flags déjà set via FSM ; on les laisse actifs jusqu’au hold-time
        elif st.state == _STATE_RECOV:
            st.mode = _MODE_CONSTRAIN
            # recovery douce : on réduit modérément pacing, on remonte inflight prudemment
            st.pacing_ms = max(prof["pacing_min"], int(st.pacing_ms * 0.9))
            st.inflight_max = min(prof["inflight_max"], max(prof["inflight_min"], st.inflight_max + 1))

        # clamps globaux + jitter léger
        self._apply_clamps(rg, st)
        j = random.randint(0, max(0, self._jitter_ms))
        st.pacing_ms = int(max(self._min_ms, min(self._max_ms, st.pacing_ms + j)))

        # NANO : sécurité forte (inflight=1, MM non autorisée)
        if self._profile == "NANO":
            st.inflight_max = 1
            # la MM est gérée au niveau Engine via flags mm_frozen/maker gating

    def _policy_dict(self, rg: str, st: RegionState) -> Dict[str, Any]:
        return {
            "region": rg,
            "profile": self._profile,
            "pacing_ms": int(st.pacing_ms),
            "inflight_max": int(st.inflight_max),
            "flags": dict(st.flags),
            "mode": int(st.mode),  # 0=NORMAL,1=CONSTRAINED,2=SEVERE
            "state": st.state,
            "metrics": {
                "ack_ms": round(st.ack_ms, 2),
                "lag_ms": round(st.lag_ms, 2),
                "err_rate": round(st.err_rate, 5),
                "drain_ms": round(st.drain_ms, 2),
            },
            "reason": st.last_reason,
            "ts": st.last_update_ts,
        }

    def _export_metrics(self, rg: str, st: RegionState) -> None:
        try:
            ENGINE_PACER_DELAY_MS.labels(region=rg, profile=self._profile, mode=str(st.mode)).set(st.pacing_ms)
            ENGINE_PACER_INFLIGHT_MAX.labels(region=rg, profile=self._profile, mode=str(st.mode)).set(st.inflight_max)
            ENGINE_PACER_MODE.labels(region=rg, profile=self._profile).set(st.mode)
        except Exception:
            pass
