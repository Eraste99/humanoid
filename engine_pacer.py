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

import logging
import time
import random
import threading
import hashlib
import json
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from modules.bot_config import BotConfig

logger = logging.getLogger(__name__)

# --------- Metrics (Prometheus) optional import ---------
try:
    # Essaie d'importer des métriques standard du projet (fallback si absent)
    from obs_metrics import (
        ENGINE_PACING_BACKPRESSURE_TOTAL,
        ENGINE_DRAIN_LATENCY_MS,
        ENGINE_PACER_DELAY_MS,
        ENGINE_PACER_INFLIGHT_MAX,
        ENGINE_PACER_MODE,
        PACER_ACK_HI_MS,
        PACER_ACK_SEV_MS,
        PACER_ACK_TARGET_MS,
        PACER_STATE,
        PACER_CLAMP_SECONDS,
    )
except Exception:  # pragma: no cover
    try:
        from modules import obs_metrics as _obs

        ENGINE_PACING_BACKPRESSURE_TOTAL = _obs.ENGINE_PACING_BACKPRESSURE_TOTAL
        ENGINE_DRAIN_LATENCY_MS = _obs.ENGINE_DRAIN_LATENCY_MS
        ENGINE_PACER_DELAY_MS = _obs.ENGINE_PACER_DELAY_MS
        ENGINE_PACER_INFLIGHT_MAX = _obs.ENGINE_PACER_INFLIGHT_MAX
        ENGINE_PACER_MODE = _obs.ENGINE_PACER_MODE
        PACER_ACK_HI_MS = _obs.PACER_ACK_HI_MS
        PACER_ACK_SEV_MS = _obs.PACER_ACK_SEV_MS
        PACER_ACK_TARGET_MS = _obs.PACER_ACK_TARGET_MS
        PACER_STATE = _obs.PACER_STATE
        PACER_CLAMP_SECONDS = _obs.PACER_CLAMP_SECONDS
    except Exception:
        class _NoOp:
            def labels(self, *args, **kwargs): return self

            def inc(self, *args, **kwargs): return None

            def set(self, *args, **kwargs): return None

            def observe(self, *args, **kwargs): return None


        ENGINE_PACING_BACKPRESSURE_TOTAL = _NoOp()
        ENGINE_DRAIN_LATENCY_MS = _NoOp()
        ENGINE_PACER_DELAY_MS = _NoOp()
        ENGINE_PACER_INFLIGHT_MAX = _NoOp()
        ENGINE_PACER_MODE = _NoOp()
        PACER_ACK_HI_MS = _NoOp()
        PACER_ACK_SEV_MS = _NoOp()
        PACER_ACK_TARGET_MS = _NoOp()
        PACER_STATE = _NoOp()
        PACER_CLAMP_SECONDS = _NoOp()
        _METRICS_FALLBACK_LOGGED = True


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
    warmup_start_ts: float = 0.0

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
        bot_cfg: Optional["BotConfig"] = None,
    ):
        global _METRICS_FALLBACK_LOGGED
        if _METRICS_FALLBACK_LOGGED:
            try:
                logger.warning("[EnginePacer] metrics fallback to no-op counters")
            except Exception:
                pass
            _METRICS_FALLBACK_LOGGED = False

        self._lock = threading.RLock()
        self._bot_cfg = bot_cfg
        engine_cfg = getattr(bot_cfg, "engine", None) if bot_cfg else None
        knobs = getattr(engine_cfg, "pacer_knobs", None)
        if knobs is None:
            from modules.bot_config import EnginePacerKnobs
            knobs = EnginePacerKnobs()
        self._pacer_knobs = knobs
        self._default_region = self._norm_region(region)
        self._regions: Dict[str, RegionState] = {}
        self._targets: Dict[str, Dict[str, float]] = dict(self._pacer_knobs.default_targets)
        self._targets_overrides: Dict[str, Dict[str, float]] = {}
        if targets_overrides:
            for k, v in targets_overrides.items():
                rg = self._norm_region(k)
                self._targets_overrides[rg] = {**self._targets_overrides.get(rg, {}), **(v or {})}
        self._profile = self._norm_profile(capital_profile)
        
        # clamps globaux (peuvent être serrés par le profil)
        self._min_ms = int(min_ms)
        self._max_ms = int(max_ms)
        self._init_ms = int(init_ms)
        self._jitter_ms = int(jitter_ms)

        # Clamps profil → dérivés uniquement de la config Engine (Ticket 10)
        # Ici on ne redéfinit AUCUNE grille NANO/MICRO/… : c’est déjà décidé par BotConfig/RM.
        self._pacing_min_profile = int(self._min_ms)
        self._pacing_max_profile = int(self._max_ms)

        # Plancher technique global sur l’inflight (>=1), plafond optionnel
        # qui pourra être fixé depuis l’Engine à partir des caps RM/BotConfig.
        self._inflight_min_profile = 1
        self._inflight_ceiling: Optional[int] = None


        # mapping exchange -> region (hint pour l’Engine si besoin)
        self._region_map = {k.upper(): self._norm_region(v) for k, v in (region_map or {}).items()}

        # charge les cibles depuis les SLO si disponibles
        self._refresh_targets_from_slo()

        # initialise l'état de la région par défaut
        self._ensure_region(self._default_region, cold_start=True)
        try:
            payload = asdict(self._pacer_knobs)
            snapshot = json.dumps(payload, sort_keys=True, default=str)
            snap_hash = hashlib.sha1(snapshot.encode("utf-8")).hexdigest()
            logger.info("[EnginePacer] pacer_knobs=%s snapshot_hash=%s", payload, snap_hash)
        except Exception:
            logger.exception("[EnginePacer] pacer_knobs snapshot failed", exc_info=False)

    # ------------------ helpers ------------------

    @staticmethod
    def _norm_region(r: Optional[str]) -> str:
        """
        Normalise le libellé de région en tag interne.

        - "EU", "eu", "eu_only"   → "EU"
        - "US", "us-east"         → "US"
        - "EU-CB", "eu_cb", ...   → "EU-CB"
        - "JP", "TOKYO", "APAC"   → "JP"
        """
        if not r:
            return "EU"
        r = str(r).upper().replace("_", "-")
        if r.startswith("EU-CB"):
            return "EU-CB"
        if r.startswith("US"):
            return "US"
        if r.startswith("JP") or r.startswith("TOKYO") or r.startswith("APAC"):
            return "JP"
        return "EU"

    @staticmethod
    def _norm_profile(p: Optional[str]) -> str:
        """
        Normalise le libellé de profil en simple tag (NANO/MICRO/…).
        Aucune logique de caps ici : tout est piloté par BotConfig/RM.
        """
        return str(p or "NANO").upper()

    def _ensure_region(self, region: str, cold_start: bool = False) -> RegionState:
        region = self._norm_region(region)
        st = self._regions.get(region)
        if st is None:
            st = RegionState()
            # Pacing initial piloté par la config Engine (pacer_min/max_ms, pacer_init_ms).
            base = self._init_ms if self._init_ms > 0 else self._min_ms
            st.pacing_ms = max(self._min_ms, min(self._max_ms, base))
            # Cap inflight purement technique : >=1. Les caps métier restent côté RM/Engine.
            st.inflight_max = 1
            st.state = _STATE_WARMUP if cold_start else _STATE_NORMAL
            if st.state == _STATE_WARMUP:
                st.warmup_start_ts = time.monotonic()
            self._regions[region] = st
        return st

    def _ema(self, prev: float, new: Optional[float], alpha: float = 0.3) -> float:
        if new is None:
            return prev
        if prev == 0.0:
            return float(new)
        return (1.0 - alpha) * prev + alpha * float(new)

    def _apply_target_overrides(self) -> None:
        for rg, override in self._targets_overrides.items():
            base = self._targets.get(rg, {})
            self._targets[rg] = {**base, **override}

    def _push_ack_metrics(self) -> None:
        try:
            for region, vals in self._targets.items():
                labels = dict(region=region, profile=self._profile)
                ack_target = vals.get("ack_target")
                ack_hi = vals.get("ack_hi")
                ack_sev = vals.get("ack_sev")
                if ack_target is not None:
                    PACER_ACK_TARGET_MS.labels(**labels).set(float(ack_target))
                if ack_hi is not None:
                    PACER_ACK_HI_MS.labels(**labels).set(float(ack_hi))
                if ack_sev is not None:
                    PACER_ACK_SEV_MS.labels(**labels).set(float(ack_sev))
        except Exception:
            pass

    def _refresh_targets_from_slo(self) -> None:
        """
        Reconstruit les cibles par région à partir des SLO privés.
        """
        with self._lock:
            self._targets = dict(self._pacer_knobs.default_targets)

            cfg = self._bot_cfg
            if cfg is None:
                self._apply_target_overrides()
                self._push_ack_metrics()
                return

            try:
                mode_key = str(getattr(getattr(cfg, "g", None), "deployment_mode", "")).upper()
            except Exception:
                mode_key = ""

            if not hasattr(cfg, "slo"):
                logger.warning("[EnginePacer] SLO private absent pour mode=%s, fallback sur pacer_knobs defaults",
                               mode_key)
                self._apply_target_overrides()
                self._push_ack_metrics()
                return

            slo_map = getattr(cfg, "slo", {}) or {}

            per_ex = slo_map.get(mode_key)
            if per_ex is None:
                logger.warning("[EnginePacer] SLO private absent pour mode=%s, fallback sur pacer_knobs defaults", mode_key)
                self._apply_target_overrides()
                self._push_ack_metrics()
                return

            if not self._region_map:
                logger.warning(
                    "[EnginePacer] Region map vide, fallback sur region par défaut=%s pour agrégation SLO",
                    self._default_region,
                )

            agg: Dict[str, Dict[str, Optional[float]]] = {}
            for ex, path_slo in per_ex.items():
                private = getattr(path_slo, "private", None)
                if not private:
                    continue

                region = self._region_map.get(str(ex).upper(), self._default_region)
                vals = agg.setdefault(region, {"ack_target": None, "ack_hi": None, "ack_sev": None})

                for key, attr in (("ack_target", "ack_target_ms"), ("ack_hi", "ack_hi_ms"), ("ack_sev", "ack_sev_ms")):
                    try:
                        raw_val = float(getattr(private, attr, 0.0))
                    except Exception:
                        continue
                    if raw_val <= 0:
                        continue
                    current = vals.get(key)
                    vals[key] = max(current, raw_val) if current is not None else raw_val

            agg = {r: v for r, v in agg.items() if any(val is not None for val in v.values())}
            if not agg:
                logger.warning(
                    "[EnginePacer] Aucun chemin SLO privé trouvé pour mode=%s, fallback sur pacer_knobs defaults",
                    mode_key,
                )
                self._apply_target_overrides()
                self._push_ack_metrics()
                return

            for region, vals in agg.items():
                base = dict(self._targets.get(region, self._targets.get(self._default_region, {})))
                if vals.get("ack_target") is not None:
                    base["ack_target"] = vals["ack_target"]
                if vals.get("ack_hi") is not None:
                    base["ack_hi"] = vals["ack_hi"]
                if vals.get("ack_sev") is not None:
                    base["ack_sev"] = vals["ack_sev"]
                self._targets[region] = base

            # ré-applique les overrides éventuels
            self._apply_target_overrides()
            self._push_ack_metrics()

    # ------------------ public API ------------------

    def set_bot_config(self, cfg: "BotConfig") -> None:
        with self._lock:
            self._bot_cfg = cfg
        self._refresh_targets_from_slo()

    def set_capital_profile(self, profile: str) -> None:
        """Changer le profil de capital (clamps)."""
        with self._lock:
            self._profile = self._norm_profile(profile)
            # clamp immédiatement les régions en cours
            for rg, st in self._regions.items():
                self._apply_clamps(rg, st)
            self._push_ack_metrics()

    def set_inflight_ceiling(self, cap: Optional[int]) -> None:
        """
        Fixe un plafond global d'inflight recommandé (optionnel),
        typiquement dérivé des caps RM/BotConfig (Ticket 10).

        cap=None => pas de plafond spécifique côté pacer (seuls RM/Engine bornent).
        """
        with self._lock:
            if cap is None:
                self._inflight_ceiling = None
            else:
                try:
                    self._inflight_ceiling = max(1, int(cap))
                except Exception:
                    self._inflight_ceiling = 1
            for rg, st in self._regions.items():
                self._apply_clamps(rg, st)


    def set_region_targets(self, region: str, overrides: Dict[str, float]) -> None:
        """Surcharger les cibles d'une région (EU/US/EU-CB)."""
        rg = self._norm_region(region)
        with self._lock:
            base = self._targets.get(rg, {})
            self._targets[rg] = {**base, **(overrides or {})}
            self._targets_overrides[rg] = {**self._targets_overrides.get(rg, {}), **(overrides or {})}
            self._push_ack_metrics()

    def set_region_map(self, ex_to_region: Dict[str, str]) -> None:
        """Déclarer le mapping exchange -> region (utile pour routing côté Engine)."""
        with self._lock:
            self._region_map = {k.upper(): self._norm_region(v) for k, v in (ex_to_region or {}).items()}
        if self._bot_cfg is not None:
            self._refresh_targets_from_slo()


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
            targets = self._targets.get(rg, self._pacer_knobs.default_targets.get("EU", {}))
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

    def get_pacer_mode(self, region: Optional[str] = None) -> str:
        """
        Vue consolidée du mode infra (Macro 5) pour une région.

        Retourne un pacer_mode canonique consommable par le RM:
          - "NORMAL"
          - "CONSTRAINED"
          - "SEVERE"

        Le mapping interne state/mode -> pacer_mode est défini par _derive_pacer_mode().
        """
        rg = self._norm_region(region or self._default_region)
        with self._lock:
            st = self._ensure_region(rg)
            return self._derive_pacer_mode(st)

    def get_pacer_snapshot(self, region: Optional[str] = None) -> Dict[str, Any]:
        """
        Snapshot consolidé (read-only) du Pacer pour une région.

        Structure retournée :
            {
                "region": str,
                "pacer_mode": "NORMAL" | "CONSTRAINED" | "SEVERE",
                "ack_p95_ms": float | None,
                "err_rate": float | None,
                "backpressure_level": float | None,
                "inflight": int | None,
                "as_of_ts": float,   # horodatage monotonic
            }
        """
        rg = self._norm_region(region or self._default_region)
        with self._lock:
            st = self._ensure_region(rg)
            pacer_mode = self._derive_pacer_mode(st)
            has_samples = st.last_update_ts > 0.0

            return {
                "region": rg,
                "pacer_mode": pacer_mode,
                "ack_p95_ms": float(st.ack_ms) if has_samples else None,
                "err_rate": float(st.err_rate) if has_samples else None,
                "backpressure_level": None,
                "inflight": int(st.inflight_max) if st.inflight_max is not None else None,
                "as_of_ts": time.monotonic(),
            }

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

        knobs = self._pacer_knobs
        S = (
                float(knobs.weight_ack) * g_ack
                + float(knobs.weight_lag) * g_lag
                + float(knobs.weight_err) * g_err
                + float(knobs.weight_drain) * g_drain
        )
        return max(0.0, min(1.0, S))

    def _fsm_step(self, rg: str, st: RegionState, S: float) -> None:
        now = time.monotonic()

        # warmup court : on descend rapidement vers NORMAL
        if st.state == _STATE_WARMUP:
            if not st.warmup_start_ts:
                st.warmup_start_ts = now
            if (now - st.warmup_start_ts) >= float(self._pacer_knobs.warmup_secs):
                st.state = _STATE_NORMAL
            else:
                # pas d'escalade en warmup
                return

        # détection "bonne" ou "mauvaise" fenêtre
        good = (S < float(self._pacer_knobs.good_score_threshold))
        bad = (S >= float(self._pacer_knobs.bad_score_threshold))

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
        if st.bad_win >= int(self._pacer_knobs.escalate_bad_windows):
            st.bad_win = 0
            if st.state == _STATE_NORMAL:
                st.state = _STATE_CONSTR
            elif st.state in (_STATE_CONSTR, _STATE_RECOV):
                st.state = _STATE_SEVERE
                # set flags + hold-time
                st.flags["mm_frozen"] = True
                st.flags["ioc_only"]  = True
                st.last_flag_set_ts = now
        elif st.good_win >= int(self._pacer_knobs.deescalate_good_windows):
            st.good_win = 0
            if st.state == _STATE_SEVERE:
                st.state = _STATE_RECOV
            elif st.state == _STATE_CONSTR:
                st.state = _STATE_NORMAL
            elif st.state == _STATE_RECOV:
                st.state = _STATE_NORMAL

        # hold-time minimal pour les flags
        if st.flags.get("mm_frozen") or st.flags.get("ioc_only"):
            if (
                    (now - st.last_flag_set_ts) >= float(self._pacer_knobs.hold_time_flags_secs)
                    and st.state in (_STATE_NORMAL, _STATE_RECOV)
            ):
                st.flags["mm_frozen"] = False
                st.flags["ioc_only"]  = False

    def _apply_clamps(self, rg: str, st: RegionState) -> None:
        # Pacing : uniquement borné par la config Engine (min/max globaux).
        pacing_min = int(self._pacing_min_profile)
        pacing_max = int(self._pacing_max_profile)
        st.pacing_ms = max(pacing_min, min(st.pacing_ms, pacing_max))

        # Inflight : plancher technique = 1 ; plafond optionnel dérivé de BotConfig/RM.
        ceiling = self._inflight_ceiling
        if ceiling is not None:
            try:
                cap = max(1, int(ceiling))
            except Exception:
                cap = 1
            st.inflight_max = max(1, min(st.inflight_max, cap))
        else:
            st.inflight_max = max(1, st.inflight_max)

    def _compute_policy(self, rg: str, st: RegionState, S: float) -> None:
        pacing_min = int(self._pacing_min_profile)
        pacing_max = int(self._pacing_max_profile)
        inflight_floor = int(self._inflight_min_profile)
        ceiling = self._inflight_ceiling

        if st.state == _STATE_NORMAL:
            st.mode = _MODE_NORMAL
            # AIMD vers le bas sur la latence : on relaxe doucement
            st.pacing_ms = max(pacing_min, st.pacing_ms - 5)
            st.inflight_max = max(inflight_floor, st.inflight_max + 1)

        elif st.state == _STATE_CONSTR:
            st.mode = _MODE_CONSTRAIN
            mul = (1.0 + 0.5 * S)
            st.pacing_ms = int(max(pacing_min, min(pacing_max, st.pacing_ms * mul)))
            st.inflight_max = max(inflight_floor, int(st.inflight_max / mul))

        elif st.state == _STATE_SEVERE:
            st.mode = _MODE_SEVERE
            mul = (1.0 + 1.5 * S)
            st.pacing_ms = int(max(pacing_min, min(pacing_max, st.pacing_ms * mul)))
            st.inflight_max = max(1, int(max(inflight_floor, st.inflight_max / mul)))

        elif st.state == _STATE_RECOV:
            st.mode = _MODE_CONSTRAIN
            # Recovery douce : on réduit modérément pacing, on remonte inflight prudemment
            st.pacing_ms = max(pacing_min, int(st.pacing_ms * 0.9))
            st.inflight_max = max(inflight_floor, st.inflight_max + 1)

        # Clamps globaux + jitter léger
        self._apply_clamps(rg, st)
        j = random.randint(0, max(0, self._jitter_ms))
        st.pacing_ms = int(max(self._min_ms, min(self._max_ms, st.pacing_ms + j)))

    def _derive_pacer_mode(self, st: RegionState) -> str:
        """
        Mapping interne (FSM) -> pacer_mode métier (Macro 5).

        Règles :
          - SEVERE  : toujours "SEVERE" (infra en crise).
          - CONSTRAINED : "CONSTRAINED" quand l'état est contraint ou en récupération.
          - WARMUP : traité comme "CONSTRAINED" (pas encore "vert").
          - NORMAL : "NORMAL", sauf si le code mode indique une sévérité plus forte.

        On utilise state en premier, puis st.mode (0/1/2) comme filet de sécurité.
        """
        s = str(st.state or "").upper()

        if s == _STATE_SEVERE:
            return "SEVERE"
        if s in (_STATE_CONSTR, _STATE_WARMUP, _STATE_RECOV):
            return "CONSTRAINED"

        # Fallback sur le code numérique (0/1/2) pour robustesse
        if st.mode == _MODE_SEVERE:
            return "SEVERE"
        if st.mode == _MODE_CONSTRAIN:
            return "CONSTRAINED"

        return "NORMAL"


    def _policy_dict(self, rg: str, st: RegionState) -> Dict[str, Any]:
        pacer_mode = self._derive_pacer_mode(st)
        return {
            "region": rg,
            "profile": self._profile,
            "pacing_ms": int(st.pacing_ms),
            "inflight_max": int(st.inflight_max),
            "flags": dict(st.flags),
            "mode": int(st.mode),  # 0=NORMAL,1=CONSTRAINED,2=SEVERE
            "pacer_mode": pacer_mode,
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
            PACER_STATE.set(st.mode)
            PACER_CLAMP_SECONDS.labels(kind="mm_frozen").set(1 if st.flags.get("mm_frozen") else 0)
            PACER_CLAMP_SECONDS.labels(kind="ioc_only").set(1 if st.flags.get("ioc_only") else 0)
        except Exception:
            pass