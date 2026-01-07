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
- Passer `cfg_root` (instance de BotConfig) au constructeur pour consommer:
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
import math
import time
from collections import deque, defaultdict
from statistics import median
from typing import Dict, Any, Optional, Tuple
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

    VolatilityManager is instantiated by RiskManager and receives BotConfig root (cfg_root).
    Canonical config inputs: cfg_root.vol.* (maps + thresholds) and cfg_root.engine.* for
    tm_exposure_ttl_hedge_ratio.
    """

    # États de prudence (FR) : "normal" | "modere" | "eleve"
    _STATE_TO_CODE = {"normal": 0, "modere": 1, "eleve": 2}

    def __init__(self,
                 *,
                 cfg_root: Optional[Any] = None,
                 history_limit: int = 240,
                 ewma_alpha: float = 0.18,
                 window_s: float = 120.0,
                 cooloff_s: float = 1.2) -> None:
        """
        :param cfg_root: BotConfig root (utilisé pour maps & seuils). Doit être fourni.
        :param history_limit: taille max de l'historique par (pair, exchange).
        :param ewma_alpha: alpha EWMA (0<alpha<=1).
        :param window_s: durée max des échantillons conservés (pruning par âge).
        :param cooloff_s: délai min entre deux step(pair) pour limiter la charge.
        """
        if cfg_root is None:
            raise ValueError("cfg_root is required to initialize VolatilityManager")
        vol_cfg = getattr(cfg_root, "vol", None)
        if vol_cfg is None:
            raise ValueError("cfg_root.vol is required to initialize VolatilityManager")
        self.cfg_root = cfg_root
        self.vol_cfg = vol_cfg
        self.engine_cfg = getattr(cfg_root, "engine", None)
        self.rm_cfg = getattr(cfg_root, "rm", None)
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
        # Volatilité bps (ingestion externe)
        self._vol_bps: Dict[str, float] = {}
        self._vol_ema: Dict[str, float] = {}
        self._vol_ts: Dict[str, float] = {}


        # Agrégats par pair (tous exchanges confondus)
        self._pair_prudence: Dict[str, str] = defaultdict(lambda: "normal")
        self._pair_prudence_last_change: Dict[str, float] = defaultdict(lambda: 0.0)
        self._pair_metrics_cache: Dict[str, Dict[str, Any]] = {}
        self._stale_alert_ts: Dict[str, float] = defaultdict(lambda: 0.0)
        self._event_sink: Optional[Callable[[Dict[str, Any]], Any]] = None
        self._pair_last_step_ts: Dict[str, float] = defaultdict(float)

        # Seuils par défaut (en bps) + hysteresis
        defaults = {
            "NORMAL_TO_CAREFUL": 8.0,   # si p95 >=  8 bps → modere
            "CAREFUL_TO_ALERT":  15.0,  # si p95 >= 15 bps → eleve
            "ALERT_TO_CAREFUL":  12.0,  # si p95 <= 12 bps → modere
            "CAREFUL_TO_NORMAL": 6.0,   # si p95 <=  6 bps → normal
        }
        self._thr: Dict[str, float] = dict(defaults)
        try:
            custom = getattr(self.vol_cfg, "vm_prudence_thresholds_bps", None)
            if isinstance(custom, dict):
                for k, v in custom.items():
                    self._thr[k] = _safe_float(v, self._thr.get(k, defaults.get(k, 0.0)))
        except Exception:
            pass

    # ------------------------------- Ingestion --------------------------------
    def set_event_sink(self, sink: Optional[Callable[[Dict[str, Any]], Any]]) -> None:
        self._event_sink = sink

    def _emit(self, level: str, message: str, **details: Any) -> None:
        if not self._event_sink:
            return
        try:
            payload = {"module": "VolatilityManager", "level": level, "message": message}
            if details:
                payload.update(details)
            self._event_sink(payload)
        except Exception as exc:
            log.warning("volatility event sink failed: %s", exc, exc_info=False)

    def get_current_metrics(self, pair: Optional[str]) -> Dict[str, Any]:
        """
        Snapshot lisible des métriques de volatilité.

        Retourne un dict avec au moins :
            {
                "pair": str | None,
                "ewma_bps": float,
                "p95_bps": float,
                "band": "normal" | "modere" | "eleve",
                "last_spread_bps": float,
                "last_ts": float | None,
                "age_s": float,
                "samples": int,
                "ewma_vol_bps": float,
                "p95_vol_bps": float,
                "last_age_s": float,
            }

        - Si `pair` est fourni : snapshot pour cette paire.
          On privilégie `_pair_metrics_cache` (alimenté par step()), puis on retombe
          sur `_aggregate_pair_metrics` si nécessaire.
        - Si `pair` est None : vue globale basée sur le cache (agrégation des paires connues).
        """
        now = _now()

        # --- 1) Vue globale (pair=None) ---
        if pair is None:
            cache = self._pair_metrics_cache or {}
            if not cache:
                return {
                    "pair": None,
                    "ewma_bps": 0.0,
                    "p95_bps": 0.0,
                    "band": "normal",
                    "last_spread_bps": 0.0,
                    "last_ts": None,
                    "age_s": float("inf"),
                    "samples": 0,
                    "ewma_vol_bps": 0.0,
                    "p95_vol_bps": 0.0,
                    "last_age_s": float("inf"),
                }

            def _collect_float(key: str) -> list[float]:
                vals: list[float] = []
                for rec in cache.values():
                    v = rec.get(key)
                    if v is None:
                        continue
                    try:
                        vals.append(float(v))
                    except (TypeError, ValueError):
                        continue
                return vals

            ewma_list = _collect_float("ewma_bps")
            p95_list = _collect_float("p95_bps")

            last_ts_list: list[float] = []
            for rec in cache.values():
                ts = rec.get("last_ts")
                if isinstance(ts, (int, float)):
                    last_ts_list.append(float(ts))

            if last_ts_list:
                last_ts = max(last_ts_list)
                age_s = max(0.0, now - last_ts)
            else:
                last_ts = None
                age_s = float("inf")

            bands = {str(rec.get("band") or "").lower() for rec in cache.values()}
            if "eleve" in bands:
                band = "eleve"
            elif "modere" in bands:
                band = "modere"
            else:
                band = "normal"

            samples = 0
            for rec in cache.values():
                v = rec.get("samples")
                try:
                    samples += int(v)
                except (TypeError, ValueError):
                    continue

            return {
                "pair": None,
                "ewma_bps": median(ewma_list) if ewma_list else 0.0,
                "p95_bps": median(p95_list) if p95_list else 0.0,
                "band": band,
                "last_spread_bps": 0.0,
                "last_ts": last_ts,
                "age_s": age_s,
                "samples": samples,
                "ewma_vol_bps": median(ewma_list) if ewma_list else 0.0,
                "p95_vol_bps": median(p95_list) if p95_list else 0.0,
                "last_age_s": age_s,
            }

        # --- 2) Vue par paire ---
        pk = _norm_pair(pair)

        # 2a) D'abord le cache instantané
        cached = self._pair_metrics_cache.get(pk)
        if isinstance(cached, dict) and cached:
            out = dict(cached)
            out.setdefault("pair", pk)

            # Normalise quelques champs et recalcule l'âge si possible
            ts = out.get("last_ts")
            if isinstance(ts, (int, float)):
                try:
                    age_s = max(0.0, now - float(ts))
                    out["age_s"] = age_s
                    out["last_age_s"] = age_s
                except Exception:
                    pass
            else:
                out.setdefault("age_s", float("inf"))
                out.setdefault("last_age_s", out.get("age_s", float("inf")))

            # Alias cohérents
            out.setdefault("ewma_vol_bps", float(out.get("ewma_bps", 0.0)))
            out.setdefault("p95_vol_bps", float(out.get("p95_bps", 0.0)))
            if "last_spread_bps" not in out and "last_bps" in out:
                out["last_spread_bps"] = float(out.get("last_bps", 0.0))
            out.setdefault("samples", int(out.get("n_samples", 0)))

            # Prudence FR si absente
            out.setdefault("band", self.get_prudence(pk))
            return out

        # 2b) Fallback : agrégation par pair via l'état interne moderne
        met = self._aggregate_pair_metrics(pk)

        last_age_s = float(met.get("last_age_s", float("inf")))
        if math.isfinite(last_age_s):
            last_ts = now - last_age_s
        else:
            last_ts = 0.0

        try:
            band = self.get_prudence(pk)
        except Exception:
            band = "normal"

        snapshot: Dict[str, Any] = {
            "pair": pk,
            "ewma_bps": float(met.get("ewma_bps", 0.0)),
            "p95_bps": float(met.get("p95_bps", 0.0)),
            "band": str(band or "normal"),
            "last_spread_bps": float(met.get("last_bps", 0.0)),
            "last_ts": float(last_ts),
            "age_s": float(last_age_s),
            "samples": int(met.get("n_samples", 0)),
            "ewma_vol_bps": float(met.get("ewma_bps", 0.0)),
            "p95_vol_bps": float(met.get("p95_bps", 0.0)),
            "last_age_s": float(last_age_s),
        }

        # On met à jour le cache pour les prochains appels
        self._pair_metrics_cache[pk] = dict(snapshot)
        return snapshot


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
        lam = 0.7
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

    def get_prudence_key(self, pair: str) -> str:
        """
        Variante "clé de config" de get_prudence().

        Retourne une valeur normalisée pour la configuration :
            - "NORMAL"
            - "CAREFUL"
            - "ALERT"

        En cas d'erreur ou de valeur inattendue, retourne "NORMAL".
        """
        pk = _norm_pair(pair)
        fr_state = self.get_prudence(pk)
        try:
            key = self._prudence_key_for_cfg(fr_state)
        except Exception:
            # Défensif : on ne laisse jamais passer une valeur exotique.
            key = "NORMAL"

        key = str(key or "").upper()
        if key not in ("NORMAL", "CAREFUL", "ALERT"):
            return "NORMAL"
        return key

    def last_age_seconds(self, _opp: Optional[Any] = None) -> float:
        """
        Age (secondes) de la dernière observation de volatilité/spread.

        Version ultime (P1 pair-aware + anti-trou):
        - si _opp is None => vue globale (pipeline vol frais/pas frais)
        - sinon => on essaie d'extraire la pair (str/dict/attrs + base/quote)
                  et on renvoie l'âge de CETTE pair.
        - si pair introuvable => +inf (fail-closed) pour éviter qu'une vue globale fraîche
          masque une paire stale.

        Retourne +inf s'il n'y a aucune donnée.
        """

        def _norm_pair_safe(s: str) -> str:
            try:
                # si ton module a déjà _norm_pair(), on l'utilise
                fn = globals().get("_norm_pair")
                if callable(fn):
                    return fn(s)
            except Exception:
                pass
            # fallback minimal
            return str(s or "").replace("-", "").replace("/", "").upper().strip()

        def _extract_pair(obj: Any) -> str:
            try:
                if obj is None:
                    return ""

                # cas simple : pair directement en str
                if isinstance(obj, str):
                    return _norm_pair_safe(obj)

                # dict opp
                if isinstance(obj, dict):
                    for k in ("pair", "pair_key", "symbol", "market", "instrument"):
                        v = obj.get(k)
                        if v:
                            return _norm_pair_safe(str(v))
                    # fallback base/quote
                    b = obj.get("base")
                    q = obj.get("quote")
                    if b and q:
                        return _norm_pair_safe(str(b) + str(q))
                    return ""

                # objet opp (attributs)
                for attr in ("pair_key", "pair", "symbol", "market", "instrument"):
                    v = getattr(obj, attr, None)
                    if v:
                        return _norm_pair_safe(str(v))

                return ""
            except Exception:
                return ""

        # --- Vue globale si aucun contexte ---
        if _opp is None:
            try:
                metrics = self.get_current_metrics(pair=None)
            except Exception:
                return float("inf")
            try:
                return float(metrics.get("last_age_s", metrics.get("age_s", float("inf"))))
            except Exception:
                return float("inf")

        # --- Vue par paire (fail-closed si pair inconnue) ---
        pk = _extract_pair(_opp)
        if not pk:
            return float("inf")

        try:
            metrics = self.get_current_metrics(pair=pk)
        except Exception:
            return float("inf")

        try:
            return float(metrics.get("last_age_s", metrics.get("age_s", float("inf"))))
        except Exception:
            return float("inf")



    def get_p95_bps(self, pair: str) -> float:
        """
        Retourne la volatilité p95 en bps pour `pair`.

        - Source principale : cache `_pair_metrics_cache` alimenté par `step()`.
        - Fallback : recalcul via `_aggregate_pair_metrics(pair)` si le cache est vide
          ou incohérent.

        :param pair: symbole logique (ex: "ETHUSDC").
        :return: p95_bps (float, base points).
        """
        pk = _norm_pair(pair)

        # 1) Essaie d'abord le cache instantané
        met = self._pair_metrics_cache.get(pk)
        if not isinstance(met, dict):
            # 2) Fallback : agrégation directe sur les historiques
            met = self._aggregate_pair_metrics(pk)
            # On met à jour le cache pour les prochains appels
            self._pair_metrics_cache[pk] = dict(met)

        try:
            return float(met.get("p95_bps", 0.0))
        except Exception:
            return 0.0

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
        if (now - self._pair_last_step_ts.get(pk, 0.0)) < self.cooloff_s:
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
        age = float(met.get("last_age_s", 0.0))
        if age > self.window_s and (now - self._stale_alert_ts[pk]) >= self.cooloff_s:
            self._stale_alert_ts[pk] = now
            self._emit("WARNING", "volatility_metrics_stale", pair=pk, age_s=age)

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
            if nxt == "eleve":
                self._emit("WARNING", "prudence_escalated", pair=pk, p95_bps=met.get("p95_bps", 0.0))
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
        size_factor = float(getattr(self.vol_cfg, "vm_size_factor_map", {}).get(cfg_key, 1.0))
        min_bps_boost = float(getattr(self.vol_cfg, "vm_min_bps_boost_map", {}).get(cfg_key, 0.0))
        if self.engine_cfg is not None:
            try:
                default_hr = float(getattr(self.engine_cfg, "tm_exposure_ttl_hedge_ratio", 0.50))
            except Exception:
                default_hr = 0.50
        elif self.rm_cfg is not None:
            try:
                default_hr = float(getattr(self.rm_cfg, "tm_exposure_ttl_hedge_ratio", 0.50))
            except Exception:
                default_hr = 0.50
        else:
            default_hr = 0.50
        hedge_ratio = float(getattr(self.vol_cfg, "tm_neutral_hedge_ratio_map", {}).get(cfg_key, default_hr))
        return size_factor, min_bps_boost, hedge_ratio

    def _pad_ticks_from_cfg(self, cfg_key: str) -> int:
        """
        Map prudence -> pad ticks pour TM/MM (fallback sûrs si non configuré).
        cfg_key ∈ {"NORMAL","CAREFUL","ALERT"}
        """
        try:
            return int(getattr(self.vol_cfg, "vm_maker_pad_ticks_map", {}).get(
                cfg_key, {"NORMAL": 0, "CAREFUL": 1, "ALERT": 2}[cfg_key]
            ))
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
