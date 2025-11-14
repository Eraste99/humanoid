# modules/volatility_manager.py
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
VolatilityManager — tri-CEX (Binance / Bybit / Coinbase)
=======================================================

Rôle
----
• Mesure une "volatilité micro" depuis les spreads L1 (best bid/ask) en **bps** (et fraction),
  via EWMA + p95, par **pair** (et optionnellement par exchange).
• Produit un **signal de prudence** ("normal" | "modere" | "eleve") avec **hystérésis**
  et **cool-off** pour éviter le flapping.
• Fournit des **ajustements non-bloquants** pour le RM/Engine:
  - size_factor (multiplicateur de taille),
  - min_bps_boost (boost de seuil en fraction, ex: 5 bps = 0.0005),
  - tm_neutral_hedge_ratio (suggéré pour le hedge TM neutre).
• Strictement **passif**: aucune tâche interne; toutes les méthodes sont sync.

Intégration
-----------
- Passer `cfg` (instance de BotConfig) au constructeur pour consommer:
  vm_size_factor_map, vm_min_bps_boost_map, tm_neutral_hedge_ratio_map.
  Optionnellement `vm_prudence_thresholds_bps` (dict) depuis le cfg:

    {
      "NORMAL_TO_CAREFUL": 8.0,
      "CAREFUL_TO_ALERT": 15.0,
      "ALERT_TO_CAREFUL": 12.0,
      "CAREFUL_TO_NORMAL": 6.0
    }

- Ingestion:
    vm.ingest_orderbook_top(ex, "ETHUSDC", best_bid, best_ask, ts=None)
    vm.ingest_spread_bps(ex, "ETHUSDC", spread_bps, ts=None)
    vm.ingest_volatility_bps(ex, "ETHUSDC", spread_bps, ts=None)  # alias
    vm.ingest_metrics("ETHUSDC", {"spread_bps": 7.2, "ts": time.time()})

- Décision:
    adj = vm.step("ETHUSDC")  # retourne dict "prudence" + ajustements + métriques
    state = vm.get_prudence("ETHUSDC")  # "normal" | "modere" | "eleve"

Observabilité
-------------
- Essaie d’importer modules.obs_metrics (Histogram/Gauge/Counter). Sinon no-op.
- Métriques exposées (si Prometheus dispo):
    VM_SPREAD_EWMA_BPS{pair}
    VM_SPREAD_P95_BPS{pair}
    VM_PRUDENCE_STATE{pair}  (0/1/2)
    VM_PRUDENCE_TRANSITIONS_TOTAL{pair,from,to}
    VM_LAST_OB_AGE_S{pair}

Unités
------
- spread_bps : base points (10 bps = 10.0). Équivalent fraction = bps / 10_000.
- min_bps_boost : fraction (5 bps = 0.0005).
"""

import time
from collections import deque, defaultdict
from statistics import median
from typing import Dict, Any, Optional, Tuple
from modules.rm_compat import getattr_int, getattr_float, getattr_str, getattr_bool, getattr_dict, getattr_list




# Observabilité — sous-module PASSIF (no Prometheus ici)
from typing import Callable, Optional, Dict, Any
import logging, time

log = logging.getLogger("volatility_manager")

EventSink = Callable[[str, Dict[str, Any]], None]
def _now() -> float: return time.time()

class _NoMetric:
    def labels(self, *a, **k): return self
    def observe(self, *a, **k): return
    def inc(self, *a, **k): return
    def set(self, *a, **k): return

# Noms attendus par le code (no-op)
VM_SPREAD_EWMA_BPS              = _NoMetric()
VM_SPREAD_P95_BPS               = _NoMetric()
VM_PRUDENCE_STATE               = _NoMetric()
VM_LAST_OB_AGE_S                = _NoMetric()
VM_PRUDENCE_TRANSITIONS_TOTAL   = _NoMetric()

# --- Petites aides -----------------------------------------------------------
def _now() -> float: return time.time()
def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default
def _norm_pair(pk: str) -> str:
    return (pk or "").replace("-", "").upper()

class VolatilityManager:
    """
    Passive Volatility Manager (pas de tâches internes).
    """

    # États de prudence (FR) : "normal" | "modere" | "eleve"
    _STATE_TO_CODE = {"normal": 0, "modere": 1, "eleve": 2}

    def __init__(self,
                 *,
                 cfg: Optional[Any] = None,
                 history_limit: int = 240,
                 ewma_alpha: float = 0.18,
                 window_s: float = 120.0,
                 cooloff_s: float = 1.2) -> None:
        """
        :param cfg: BotConfig (utilisé pour maps & seuils). Peut être None (fallbacks sûrs).
        :param history_limit: taille max de l'historique par (pair, exchange).
        :param ewma_alpha: alpha EWMA (0<alpha<=1).
        :param window_s: durée max des échantillons conservés (pruning par âge).
        :param cooloff_s: délai min entre deux step(pair) pour limiter la charge.
        """
        self.cfg = cfg
        self.history_limit = max(8, int(history_limit))
        self.ewma_alpha = float(ewma_alpha)
        self.window_s = float(window_s)
        self.cooloff_s = float(cooloff_s)

        # Historique par (pair, exchange) → deque[(ts, spread_bps)]
        self._hist: Dict[Tuple[str, str], deque] = defaultdict(lambda: deque(maxlen=self.history_limit))
        # EWMA par (pair, exchange)
        self._ewma: Dict[Tuple[str, str], float] = defaultdict(float)
        # Last top-of-book ts par (pair, exchange)
        self._last_ts: Dict[Tuple[str, str], float] = {}

        # Agrégats par pair (tous exchanges confondus)
        self._pair_last_step_ts: Dict[str, float] = defaultdict(lambda: 0.0)
        self._pair_prudence: Dict[str, str] = defaultdict(lambda: "normal")
        self._pair_prudence_last_change: Dict[str, float] = defaultdict(lambda: 0.0)
        self._pair_metrics_cache: Dict[str, Dict[str, Any]] = {}

        # Seuils par défaut (en bps) + hysteresis
        defaults = {
            "NORMAL_TO_CAREFUL": 8.0,   # si p95 >=  8 bps → modere
            "CAREFUL_TO_ALERT":  15.0,  # si p95 >= 15 bps → eleve
            "ALERT_TO_CAREFUL":  12.0,  # si p95 <= 12 bps → modere
            "CAREFUL_TO_NORMAL": 6.0,   # si p95 <=  6 bps → normal
        }
        self._thr: Dict[str, float] = dict(defaults)
        if cfg is not None:
            try:
                custom = getattr(cfg, "vm_prudence_thresholds_bps", None)
                if isinstance(custom, dict):
                    for k, v in custom.items():
                        self._thr[k] = _safe_float(v, self._thr.get(k, defaults.get(k, 0.0)))
            except Exception:
                pass

    # ------------------------------- Ingestion --------------------------------

    def get_current_metrics(self, pair: str) -> dict:
        """
        Retourne un snapshot lisible par le RM (zéro I/O):
          { 'pair':..., 'ewma_bps':float, 'p95_bps':float, 'band':'normal|modere|eleve',
            'last_spread_bps':float, 'last_ts':float, 'age_s':float, 'samples':int }
        Tolérant aux différentes implémentations internes (historiques/caches).
        """
        pk = _norm_pair(pair)
        now = _now()

        # Historiques potentiels
        hist = None
        for cand in ("history_by_pair", "_pair_history", "_hist_by_pair", "_history"):
            hist = getattr(self, cand, {}).get(pk) if hasattr(self, cand) else None
            if hist:
                break

        # EWMA/P95 potentiels
        ewma = None
        for cand in ("ewma_by_pair", "_ewma_by_pair", "_ewma"):
            d = getattr(self, cand, None)
            if isinstance(d, dict):
                ewma = d.get(pk)
                if ewma is not None:
                    break

        p95 = None
        for cand in ("p95_by_pair", "_p95_by_pair", "_p95"):
            d = getattr(self, cand, None)
            if isinstance(d, dict):
                p95 = d.get(pk)
                if p95 is not None:
                    break

        # Dernier point & âge
        last_spread = None
        last_ts = None
        if hist:
            try:
                item = hist[-1] if isinstance(hist, (list, tuple)) else list(hist)[-1]
                last_spread = float(item.get("spread_bps", item.get("bps", 0.0)))
                last_ts = float(item.get("ts", 0.0))
            except Exception:
                pass

        # Prudence via API existante si dispo
        try:
            band = self.get_prudence(pk)  # "normal" | "modere" | "eleve"
        except Exception:
            band = "normal"

        # Fallbacks : si pas d’ewma/p95 en cache, recalcule vite sur les N derniers
        if (ewma is None or p95 is None) and hist:
            vals = []
            for it in (hist if len(hist) <= 240 else hist[-240:]):
                try:
                    vals.append(float(it.get("spread_bps", it.get("bps", 0.0))))
                except Exception:
                    pass
            if vals:
                if ewma is None:
                    a = getattr_float(self, "ewma_alpha", 0.18)
                    e = 0.0
                    for v in vals:
                        e = a * v + (1 - a) * e
                    ewma = e
                if p95 is None:
                    vals_sorted = sorted(vals)
                    idx = max(0, min(len(vals_sorted) - 1, int(0.95 * (len(vals_sorted) - 1))))
                    p95 = vals_sorted[idx]

        age_s = (now - last_ts) if last_ts else float("inf")
        return {
            "pair": pk,
            "ewma_bps": float(ewma or 0.0),
            "p95_bps": float(p95 or 0.0),
            "band": str(band or "normal"),
            "last_spread_bps": float(last_spread or 0.0),
            "last_ts": float(last_ts or 0.0),
            "age_s": float(age_s),
            "samples": int(len(hist) if hist is not None else 0),
        }

    def ingest_orderbook_top(self, exchange: str, pair: str,
                             best_bid: float, best_ask: float,
                             ts: Optional[float] = None) -> None:
        """
        Ingestion depuis un top-of-book : calcule spread en bps et ajoute à l'historique.
        Ignore si données invalides (ask<=bid ou <=0).
        """
        ex = (exchange or "").upper()
        pk = _norm_pair(pair)
        bid = _safe_float(best_bid, 0.0)
        ask = _safe_float(best_ask, 0.0)
        if ask <= 0.0 or bid <= 0.0 or ask <= bid:
            return
        mid = 0.5 * (ask + bid)
        spread_frac = (ask - bid) / mid
        spread_bps = spread_frac * 10_000.0
        self.ingest_spread_bps(ex, pk, spread_bps, ts=ts)


    def ingest_spread_bps(self, exchange: str, pair: str,
                          spread_bps: float,
                          ts: Optional[float] = None) -> None:
        """
        Ingestion directe d'un spread en bps (float).
        """
        ex = (exchange or "").upper()
        pk = _norm_pair(pair)
        sbps = _safe_float(spread_bps, 0.0)
        if sbps <= 0.0:
            return
        t = _now() if ts is None else float(ts)
        key = (pk, ex)

        # Pruning temporel (fenêtre glissante)
        hist = self._hist[key]
        cutoff = t - self.window_s
        while hist and hist[0][0] < cutoff:
            hist.popleft()

        hist.append((t, sbps))
        self._last_ts[key] = t

        # EWMA
        prev = self._ewma[key]
        alpha = self.ewma_alpha
        self._ewma[key] = (alpha * sbps) + ((1.0 - alpha) * prev if prev > 0.0 else alpha * sbps)

        # Invalide le cache pair
        if pk in self._pair_metrics_cache:
            del self._pair_metrics_cache[pk]

    # Alias sémantique (compat projets précédents)
    def ingest_volatility_bps(self, by_pair: Dict[str, float]) -> None:
        """Ingestion optionnelle de vol micro en bps (par paire) — met aussi à jour l'horodatage."""
        if not by_pair:
            return
        if not hasattr(self, "_vol_ts"):
            self._vol_ts: Dict[str, float] = {}
        lam = getattr_float(self, "vol_ema_lambda", getattr(self, "vol_alpha_penalty", 0.7))
        for pair, bps in (by_pair or {}).items():
            p = _norm_pair(pair)
            x = float(max(0.0, bps))
            # valeur brute et EMA lissée
            self._vol_bps[p] = x
            prev = self._vol_ema.get(p)
            self._vol_ema[p] = (lam * prev + (1.0 - lam) * x) if isinstance(prev, (int, float)) else x
            # timestamp pour TTL
            self._vol_ts[p] = time.time()


    def ingest_metrics(self, pair: str, metrics: Dict[str, Any]) -> None:
        """
        Ingestion générique (ex: depuis Scanner):
            {"exchange": "BINANCE", "spread_bps": 7.2, "ts": ...}
        """
        if not metrics:
            return
        ex = (metrics.get("exchange") or "").upper()
        if not ex:
            return
        sbps = metrics.get("spread_bps", None)
        if sbps is None:
            # possibilité: bid/ask → calcule spread
            bid = metrics.get("best_bid", None)
            ask = metrics.get("best_ask", None)
            if bid is not None and ask is not None:
                self.ingest_orderbook_top(ex, pair, bid, ask, ts=metrics.get("ts"))
            return
        self.ingest_spread_bps(ex, pair, sbps, ts=metrics.get("ts"))

    # ------------------------------ Calculs -----------------------------------

    def _aggregate_pair_metrics(self, pair: str) -> Dict[str, Any]:
        """
        Calcule les métriques agrégées par pair (tous exchanges).
        - EWMA pair: médiane des EWMA par exchange (robuste aux outliers).
        - p95 pair: quantile sur l'union des historiques (bounded par window_s).
        - last_age_s: âge du dernier top-of-book (min sur les exchanges).
        """
        pk = _norm_pair(pair)
        # Construit la liste des clés (pk, ex) présents
        keys = [k for k in self._hist.keys() if k[0] == pk]
        if not keys:
            return {
                "pair": pk, "n_samples": 0, "ewma_bps": 0.0, "p95_bps": 0.0,
                "last_bps": 0.0, "last_age_s": float("inf"),
                "ewma_frac": 0.0, "p95_frac": 0.0,
            }

        # EWMA par exchange
        ewmas = []
        last_ts_list = []
        last_bps = 0.0
        union: list[float] = []

        cutoff = _now() - self.window_s

        for key in keys:
            hist = self._hist[key]
            # prune doux (lecture) – conserve en mémoire, filtre à la volée
            recent = [sbps for (ts, sbps) in hist if ts >= cutoff]
            if recent:
                union.extend(recent)
            e = self._ewma.get(key, 0.0)
            if e > 0.0:
                ewmas.append(e)
            last_ts = self._last_ts.get(key, 0.0)
            if last_ts > 0.0:
                last_ts_list.append(last_ts)
                # approx "last_bps" = dernier point de cet exchange (celui au ts max sera retenu)
                # (simple et suffisant pour debug)
                last_bps = max(last_bps, hist[-1][1] if hist else 0.0)

        n = len(union)
        if n == 0:
            return {
                "pair": pk, "n_samples": 0, "ewma_bps": 0.0, "p95_bps": 0.0,
                "last_bps": 0.0, "last_age_s": float("inf"),
                "ewma_frac": 0.0, "p95_frac": 0.0,
            }

        union.sort()
        idx = max(0, int(0.95 * (n - 1)))
        p95 = float(union[idx])

        ewma_pair = median(ewmas) if ewmas else p95  # fallback robuste

        last_age_s = float("inf")
        if last_ts_list:
            last_age_s = max(0.0, _now() - max(last_ts_list))

        # Gauges/Histos
        try:
            VM_LAST_OB_AGE_S.labels(pk).set(last_age_s)
            VM_SPREAD_EWMA_BPS.labels(pk).observe(ewma_pair)
            VM_SPREAD_P95_BPS.labels(pk).observe(p95)
        except Exception:
            pass

        return {
            "pair": pk,
            "n_samples": n,
            "ewma_bps": ewma_pair,
            "p95_bps": p95,
            "last_bps": last_bps,
            "last_age_s": last_age_s,
            "ewma_frac": ewma_pair / 10_000.0,
            "p95_frac": p95 / 10_000.0,
        }

    def get_current_metrics(self, pair: str) -> dict:
        """
        Getter consolidé (passif) pour le RM.
        Retourne un dict minimal et stable : ewma_vol_bps, p95_vol_bps, last_age_s, band.
        - Aucun état actif ni I/O.
        - Tolérant si la paire est inconnue (0.0 / "normal").
        """
        pk = (pair or "").replace("-", "").upper()
        st = self._state.get(pk, {})
        try:
            ewma = float(st.get("ewma_vol_bps", 0.0))
            p95 = float(st.get("p95_vol_bps", 0.0))
            ts = float(st.get("last_ts", 0.0))
        except Exception:
            ewma = p95 = ts = 0.0

        now = time.time()
        age = max(0.0, now - ts) if ts > 0 else 1e9

        try:
            band = str(self.get_prudence(pk)).lower()  # "normal"|"modere"|"eleve"
        except Exception:
            band = "normal"

        return {
            "pair": pk,
            "ewma_vol_bps": ewma,
            "p95_vol_bps": p95,
            "last_age_s": age,
            "band": band,
        }

    # ---------------------------- Prudence & Hystérésis -----------------------

    def _decide_prudence_from_p95(self, p95_bps: float, current: str) -> str:
        """
        Politique de transitions avec hystérésis:
        - normal → modere si p95 >= NORMAL_TO_CAREFUL
        - modere → eleve si p95 >= CAREFUL_TO_ALERT
        - eleve → modere si p95 <= ALERT_TO_CAREFUL
        - modere → normal si p95 <= CAREFUL_TO_NORMAL
        """
        thr = self._thr
        s = current
        if s == "normal":
            if p95_bps >= thr["NORMAL_TO_CAREFUL"]:
                return "modere"
            return "normal"
        if s == "modere":
            if p95_bps >= thr["CAREFUL_TO_ALERT"]:
                return "eleve"
            if p95_bps <= thr["CAREFUL_TO_NORMAL"]:
                return "normal"
            return "modere"
        # current == "eleve"
        if p95_bps <= thr["ALERT_TO_CAREFUL"]:
            return "modere"
        return "eleve"

    def _prudence_key_for_cfg(self, fr_state: str) -> str:
        # Mapping FR→CFG maps
        return {"normal": "NORMAL", "modere": "CAREFUL", "eleve": "ALERT"}.get(fr_state, "NORMAL")

    def get_prudence(self, pair: str) -> str:
        """
        État de prudence courant ("normal" | "modere" | "eleve").
        """
        return self._pair_prudence.get(_norm_pair(pair), "normal")

    # ---------------------------- API principale ------------------------------

    def step(self, pair: str,
             metrics: Optional[Dict[str, Any]] = None,
             thresholds: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Pas de calcul principal pour `pair`.
        - Optionnellement ingère `metrics` (voir ingest_metrics).
        - Optionnellement met à jour les thresholds locaux (non persistant globalement).
        - Applique cool-off par pair.
        - Calcule ewma/p95 + prudence (hystérésis).
        - Retourne ajustements non-bloquants + métriques courantes.

        :return: {
            "pair": ..., "prudence": "normal|modere|eleve",
            "min_bps_boost": <fraction>, "size_factor": <float>, "tm_neutral_hedge_ratio": <float>,
            "metrics": {...}, "changed": <bool>, "cooloff": <bool>
        }
        """
        pk = _norm_pair(pair)
        if metrics:
            self.ingest_metrics(pk, metrics)
        if thresholds:
            # mise à jour ponctuelle (optionnelle) — non persistée globalement
            try:
                for k, v in (thresholds or {}).items():
                    if k in self._thr:
                        self._thr[k] = _safe_float(v, self._thr[k])
            except Exception:
                pass

        now = _now()
        if (now - self._pair_last_step_ts[pk]) < self.cooloff_s:
            # Retourne le dernier état connu (sans recalcul) pour limiter la charge
            state = self._pair_prudence[pk]
            met = self.get_current_metrics(pk)
            key = self._prudence_key_for_cfg(state)
            size_factor, min_boost, hedge = self._maps_from_cfg(key)
            return {
                "pair": pk, "prudence": state, "cooloff": True, "changed": False,
                "min_bps_boost": min_boost, "size_factor": size_factor,
                "tm_neutral_hedge_ratio": hedge, "metrics": met,
                "maker_pad_ticks": self._pad_ticks_from_cfg(key),
                "lift_min_bps": float(min_boost * 1e4),  # alias bps (RM-friendly)

            }
        self._pair_last_step_ts[pk] = now

        # (Re)calcule les métriques instantanées
        met = self._aggregate_pair_metrics(pk)
        self._pair_metrics_cache[pk] = dict(met)

        cur = self._pair_prudence[pk]
        nxt = self._decide_prudence_from_p95(met.get("p95_bps", 0.0), cur)
        changed = (nxt != cur)
        if changed:
            self._pair_prudence[pk] = nxt
            self._pair_prudence_last_change[pk] = now
            try:
                VM_PRUDENCE_TRANSITIONS_TOTAL.labels(pk, cur, nxt).inc()
            except Exception:
                pass

        # Gauge état
        try:
            VM_PRUDENCE_STATE.labels(pk).set(self._STATE_TO_CODE.get(nxt, 0))
        except Exception:
            pass

        key = self._prudence_key_for_cfg(nxt)
        size_factor, min_boost, hedge = self._maps_from_cfg(key)

        return {
            "pair": pk,
            "prudence": nxt,      # "normal" | "modere" | "eleve"
            "min_bps_boost": min_boost,        # fraction (ex: 0.0005 = 5 bps)
            "size_factor": size_factor,        # multiplicateur de taille
            "tm_neutral_hedge_ratio": hedge,   # ex: 0.50 / 0.55 / 0.65
            "metrics": met,
            "changed": changed,
            "cooloff": False,
            "maker_pad_ticks": self._pad_ticks_from_cfg(self._prudence_key_for_cfg(nxt)),
            "lift_min_bps": float(min_boost * 1e4),

        }

    # ----------------------------- Utilitaires --------------------------------

    def _maps_from_cfg(self, cfg_key: str) -> Tuple[float, float, float]:
        """
        Lit les maps du BotConfig (ou fallback sûrs) pour:
        - size_factor,
        - min_bps_boost (fraction),
        - tm_neutral_hedge_ratio.
        """
        size_factor = 1.0
        min_bps_boost = 0.0
        hedge_ratio = 0.50
        cfg = self.cfg
        if cfg is not None:
            try:
                size_factor = float(getattr(cfg, "vm_size_factor_map", {}).get(cfg_key, 1.0))
                min_bps_boost = float(getattr(cfg, "vm_min_bps_boost_map", {}).get(cfg_key, 0.0))
                hedge_ratio = float(getattr(cfg, "tm_neutral_hedge_ratio_map", {}).get(
                    cfg_key, getattr(cfg, "tm_exposure_ttl_hedge_ratio", 0.50)
                ))
            except Exception:
                pass
        else:
            # Fallback legacy compatibles
            if cfg_key == "ALERT":
                size_factor, min_bps_boost, hedge_ratio = 0.70, 0.0005, 0.65
            elif cfg_key == "CAREFUL":
                size_factor, min_bps_boost, hedge_ratio = 0.85, 0.0002, 0.55
            else:
                size_factor, min_bps_boost, hedge_ratio = 1.00, 0.0000, 0.50
        return size_factor, min_bps_boost, hedge_ratio

    def _pad_ticks_from_cfg(self, cfg_key: str) -> int:
        """
        Map prudence -> pad ticks pour TM/MM (fallback sûrs si non configuré).
        cfg_key ∈ {"NORMAL","CAREFUL","ALERT"}
        """
        try:
            return int(getattr(self.cfg, "vm_maker_pad_ticks_map", {}).get(cfg_key,
                                                                           {"NORMAL": 0, "CAREFUL": 1, "ALERT": 2}[
                                                                               cfg_key]))
        except Exception:
            return 0

    # ------------------------------- Statut -----------------------------------

    def get_status(self) -> Dict[str, Any]:
        """
        Snapshot lisible (pour watchdogs / debug).
        """
        out = {}
        now = _now()
        for (pk, ex), hist in self._hist.items():
            last_ts = self._last_ts.get((pk, ex), 0.0)
            age = (now - last_ts) if last_ts else float("inf")
            ew = self._ewma.get((pk, ex), 0.0)
            d = out.setdefault(pk, {"per_ex": {}, "prudence": self._pair_prudence.get(pk, "normal")})
            d["per_ex"][ex] = {"age_s": age, "ewma_bps": ew, "n": len(hist)}
        return out
