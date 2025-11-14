# -*- coding: utf-8 -*-
from __future__ import annotations
"""
PairHistoryTracker — tracker PASSIF TT / TM / REB, tri-CEX ready.

Nouveautés tri-CEX:
- Ingestion & historiques PAR ROUTE (BUY_EX→SELL_EX) en plus du PAR PAIR.
- EMA latences par route, scoring de route, best_route par paire.
- Expose best_route & route_latency_ema dans collect_pair_metrics / snapshots.

Nouveautés "par branche":
- Quotas par mode (TT/TM/REB) avec active_pairs_by_mode et rotation par mode.
- APIs: get_active_pairs_by_mode, get_priority_pairs_by_mode, get_rank_map_by_mode,
        set_max_active_by_mode.
- (Optionnel) Contrainte d'univers par mode: set_universe_for_mode, set_constrain_universe_by_mode.

Rétro-compatible: si buy_ex/sell_ex/route ne sont pas fournis, on retombe
sur le comportement purement "pair".
"""

import time
from dataclasses import dataclass
from collections import deque, defaultdict
from typing import Dict, Any, List, Optional, Tuple, Iterable, Set
import logger
import os
# -------------------- helpers --------------------

def _to_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def _now() -> float:
    return time.time()

def _norm_pair(p: Optional[str]) -> str:
    return (p or "").replace("-", "").upper()

def _norm_ex(x: Optional[str]) -> Optional[str]:
    if not x:
        return None
    s = str(x).strip().replace("-", "").replace("_", "").upper()
    if s.startswith("BINANCE"):
        return "BINANCE"
    if s.startswith("BYBIT"):
        return "BYBIT"
    if s.startswith("COINBASE") or s in {"CB", "COINBASEAT", "COINBASEADVANCEDTRADE"}:
        return "COINBASE"
    return s

def _route_key(pair: str, buy_ex: Optional[str], sell_ex: Optional[str], route: Optional[str]) -> Optional[str]:
    if route:
        return str(route)
    b = _norm_ex(buy_ex)
    s = _norm_ex(sell_ex)
    # APRÈS (strict) : si une seule extrémité manque → pas de clé route
    if not b or not s:
        return None
    return f"BUY:{b}→SELL:{s}"

# -------------------- configuration --------------------

@dataclass
class TrackerConfig:
    # Capacités & fenêtres
    history_limit: int = 6000
    rotation_interval: int = 1800          # 30 min
    max_active: int = 10                   # plafond global (fallback si quotas par mode désactivés)
    pause_secs: int = 600

    # Seuils / garde-fous
    min_daily_volume_usdc: float = 500_000.0
    slippage_bad_thr: float = 0.005        # 50 bps
    e2e_latency_bad_ms: int = 1200         # > 1.2s → pénalité

    # Fenêtres (secondes)
    window_opp_freq_s: int = 600           # 10 min
    window_score_s: int = 1800             # 30 min
    window_daily_s: int = 86400            # 24 h

    # Poids de score (unités aproximatives)
    w_opp_score: float = 0.6               # score brut opportunités
    w_spread_net: float = 1.0              # spread net moyen (en points de %)
    w_volume: float = 0.00001              # volume (USDC) daily
    w_latency_penalty: float = 0.003       # pénalité par ms-100 (voir _latency_penalty)
    penalty_trade_fail: float = 10.0
    penalty_slippage: float = 5.0
    penalty_vol_modere: float = 5.0
    penalty_vol_eleve: float = 10.0
    bonus_low_vol: float = 2.0
    bonus_high_freq: float = 10.0

    # Modes (biais récents)
    bias_ttl_s: int = 1500                 # ~25 min de demi-vie
    bonus_mode_TT: float = 2.0
    bonus_mode_TM: float = 1.5
    bonus_mode_REB: float = 0.5

    # EMA latence & slippage
    ema_alpha: float = 0.2                 # lissage latence
    ema_slip_alpha: float = 0.2            # lissage slippage

    # Pondération route (léger impact)
    route_latency_penalty_scale: float = 0.002
    route_win_bonus: float = 1.0
    route_fail_penalty: float = 2.0

    # --- quotas par branche (facultatifs; None => pas de quota explicite) ---
    max_active_TT: Optional[int] = None
    max_active_TM: Optional[int] = None
    max_active_REB: Optional[int] = None

    # --- contrainte d'univers par mode (facultatif) ---
    constrain_universe_by_mode: bool = False


# -------------------- tracker --------------------

class PairHistoryTracker:
    def __init__(self, config: Optional[TrackerConfig] = None):
        self.cfg = config or TrackerConfig()

        # Historiques (FIFO bornés) — PAIR
        self.opps_by_pair: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.cfg.history_limit))
        self.trades_by_pair: Dict[str, deque] = defaultdict(lambda: deque(maxlen=self.cfg.history_limit))

        # Historiques (FIFO bornés) — ROUTE (clé: (pair, route_str))
        self.opps_by_route: Dict[Tuple[str, str], deque] = defaultdict(lambda: deque(maxlen=self.cfg.history_limit))
        self.trades_by_route: Dict[Tuple[str, str], deque] = defaultdict(lambda: deque(maxlen=self.cfg.history_limit))

        # Métriques courantes
        self.volatility_by_pair: Dict[str, Dict[str, Any]] = {}
        self.slippage_by_pair: Dict[str, float] = {}   # slippage courant instantané / EMA

        # États & dérivés
        self.last_trade_result: Dict[str, str] = {}  # "success"|"fail"|"unknown"
        self.lat_ema_by_pair: Dict[str, Dict[str, float]] = {}          # {pair: {e2e, ack, fill, last_ts}}
        self.lat_ema_by_route: Dict[Tuple[str, str], Dict[str, float]] = {}  # {(pair, route): {...}}
        self.mode_bias_by_pair: Dict[str, Dict[str, float]] = {}        # {pair: {TT, TM, REB, ts}}

        # Scores & sélections
        self.pair_scores: Dict[str, float] = {}
        self.route_scores: Dict[Tuple[str, str], float] = {}
        self.best_route_by_pair: Dict[str, Optional[str]] = {}

        self.active_pairs: List[str] = []
        self.paused_until: Dict[str, float] = {}
        self.last_rotation: float = 0.0

        # --- Nouveautés par mode ---
        self.active_pairs_by_mode: Dict[str, List[str]] = {"TT": [], "TM": [], "REB": []}
        self.max_active_by_mode: Dict[str, Optional[int]] = {
            "TT": self.cfg.max_active_TT,
            "TM": self.cfg.max_active_TM,
            "REB": self.cfg.max_active_REB,
        }
        self.universe_by_mode: Dict[str, Set[str]] = {"TT": set(), "TM": set(), "REB": set()}

    # ---------- ingestion API ----------

    def ingest_opportunity(self, opp: Dict[str, Any]) -> None:
        """
        opp attendu (tolère manquants):
        {
          "pair"|"pair_key": "ETHUSDC",
          "timestamp": time.time(),
          "score": float,
          "spread_net": float (%, ex: 0.23) OU fraction (0.0023),
          "volume_possible_usdc": float,
          "trade_mode": "TT"|"TM"|"REB"|None,
          "id": "...", "route_id": "...",
          "buy_ex": "...", "sell_ex": "...", "route": "BUY:...→SELL:..."
        }
        """
        pair = _norm_pair(opp.get("pair") or opp.get("pair_key"))
        if not pair:
            return
        ts = float(opp.get("timestamp", _now()))
        spread_net = _to_float(opp.get("spread_net"))
        if spread_net > 1.0:
            spread_net = spread_net / 100.0

        buy_ex = opp.get("buy_ex") or opp.get("buy_exchange")
        sell_ex = opp.get("sell_ex") or opp.get("sell_exchange")
        route = _route_key(pair, buy_ex, sell_ex, opp.get("route"))

        entry = {
            "ts": ts,
            "score": _to_float(opp.get("score")),
            "spread_net": spread_net,
            "volume_usdc": _to_float(opp.get("volume_possible_usdc")),
            "trade_mode": str(opp.get("trade_mode") or "").upper() or None,
            "id": opp.get("id"),
            "route_id": opp.get("route_id"),
            "buy_ex": _norm_ex(buy_ex),
            "sell_ex": _norm_ex(sell_ex),
            "route": route,
        }
        self.opps_by_pair[pair].append(entry)
        if route:
            self.opps_by_route[(pair, route)].append(entry)

        self._refresh_mode_bias(pair, entry.get("trade_mode"), ts)

    def ingest_trade(self, trade: Dict[str, Any]) -> None:
        """
        trade attendu (tolère manquants):
        {
          "pair"|"symbol"|"asset": "ETHUSDC",
          "timestamp": sec epoch (facultatif),
          "executed_volume_usdc": float,
          "net_profit": float,
          "status": "executed"|"simulated"|"failed"|...,
          "trade_mode": "TT"|"TM"|"REB"|None,
          "end_to_end_ms": int|None, "ack_latency_ms": int|None, "fill_latency_ms": int|None,
          "trade_id": str|None, "account_alias": str|None,
          "buy_ex": "...", "sell_ex": "...", "route": "BUY:...→SELL:...", "leg_role": "maker|hedge|None"
        }
        """
        pair = _norm_pair(trade.get("pair") or trade.get("symbol") or trade.get("asset"))
        if not pair:
            return
        ts = float(trade.get("timestamp", _now()))
        e2e = _to_float(trade.get("end_to_end_ms")) or None
        ack = _to_float(trade.get("ack_latency_ms")) or None
        fill = _to_float(trade.get("fill_latency_ms")) or None

        buy_ex = trade.get("buy_ex") or trade.get("buy_exchange")
        sell_ex = trade.get("sell_ex") or trade.get("sell_exchange")
        route = _route_key(pair, buy_ex, sell_ex, trade.get("route"))

        entry = {
            "ts": ts,
            "executed_volume_usdc": _to_float(trade.get("executed_volume_usdc")),
            "net_profit": _to_float(trade.get("net_profit")),
            "status": str(trade.get("status", "unknown")),
            "trade_mode": (str(trade.get("trade_mode") or "").upper() or None),
            "e2e_ms": e2e,
            "ack_ms": ack,
            "fill_ms": fill,
            "trade_id": trade.get("trade_id"),
            "account_alias": trade.get("account_alias"),
            "buy_ex": _norm_ex(buy_ex),
            "sell_ex": _norm_ex(sell_ex),
            "route": route,
            "leg_role": trade.get("leg_role"),
        }

        self.trades_by_pair[pair].append(entry)
        if route:
            self.trades_by_route[(pair, route)].append(entry)

        # résultats récents
        if entry["status"] in ("failed", "non_rentable"):
            self.last_trade_result[pair] = "fail"
        elif entry["status"] in ("executed", "simulated", "rentable", "rebalancing_accept"):
            self.last_trade_result[pair] = "success"

        # latences EMA (pair + route)
        self._update_latency_ema_pair(pair, e2e, ack, fill, ts)
        if route:
            self._update_latency_ema_route((pair, route), e2e, ack, fill, ts)

        # biais mode
        self._refresh_mode_bias(pair, entry.get("trade_mode"), ts)

    def ingest_volatility(self, pair: str, metrics: Dict[str, Any]) -> None:
        pair = _norm_pair(pair)
        if not pair:
            return
        self.volatility_by_pair[pair] = dict(metrics or {})

    def ingest_slippage(self, pair: str, slippage_fraction: float) -> None:
        pair = _norm_pair(pair)
        if not pair:
            return
        old = self.slippage_by_pair.get(pair)
        val = max(0.0, float(slippage_fraction))
        if old is None:
            self.slippage_by_pair[pair] = val
        else:
            a = float(self.cfg.ema_slip_alpha)
            self.slippage_by_pair[pair] = a * val + (1 - a) * old

    # ---------- helpers latence & biais ----------

    def _ema_update(self, old: Optional[float], new: Optional[float], a: float) -> Optional[float]:
        if new is None or new <= 0:
            return old
        if old is None:
            return float(new)
        return a * float(new) + (1 - a) * float(old)

    def _update_latency_ema_pair(self, pair: str, e2e: Optional[float], ack: Optional[float], fill: Optional[float], ts: float) -> None:
        if pair not in self.lat_ema_by_pair:
            self.lat_ema_by_pair[pair] = {"e2e": None, "ack": None, "fill": None, "last_ts": 0.0}
        obj = self.lat_ema_by_pair[pair]
        a = float(self.cfg.ema_alpha)
        obj["e2e"] = self._ema_update(obj.get("e2e"), e2e, a)
        obj["ack"] = self._ema_update(obj.get("ack"), ack, a)
        obj["fill"] = self._ema_update(obj.get("fill"), fill, a)
        obj["last_ts"] = ts

    # --- Helpers staleness (latence) ---

    def _is_latency_stale_pair(self, pair: str, now_s: Optional[float] = None) -> bool:
        now_s = float(now_s) if now_s is not None else float(_now())
        win = float(getattr(self.cfg, "window_score_s"))
        obj = self.lat_ema_by_pair.get(pair)
        if not obj:
            return False
        last = float(obj.get("last_ts") or 0.0)
        if last <= 0.0:
            return False
        return (now_s - last) > win

    def _collect_staleness_flags(self, pair: str, now_s: Optional[float] = None) -> Dict[str, Any]:
        """Retourne des drapeaux simples consommables par RM/LHM sans changer les scores."""
        now_s = float(now_s) if now_s is not None else float(_now())
        flags = {"stale_latency": self._is_latency_stale_pair(pair, now_s)}
        return flags

    def _update_latency_ema_route(self, key: Tuple[str, str], e2e: Optional[float], ack: Optional[float], fill: Optional[float], ts: float) -> None:
        if key not in self.lat_ema_by_route:
            self.lat_ema_by_route[key] = {"e2e": None, "ack": None, "fill": None, "last_ts": 0.0}
        obj = self.lat_ema_by_route[key]
        a = float(self.cfg.ema_alpha)
        obj["e2e"] = self._ema_update(obj.get("e2e"), e2e, a)
        obj["ack"] = self._ema_update(obj.get("ack"), ack, a)
        obj["fill"] = self._ema_update(obj.get("fill"), fill, a)
        obj["last_ts"] = ts

    def _refresh_mode_bias(self, pair: str, mode: Optional[str], ts: float) -> None:
        if not mode:
            return
        b = self.mode_bias_by_pair.setdefault(pair, {"TT": 0.0, "TM": 0.0, "REB": 0.0, "ts": ts})
        age = max(0.0, ts - float(b.get("ts", ts)))
        ttl = float(self.cfg.bias_ttl_s)
        decay = max(0.0, 1.0 - age / ttl) if ttl > 0 else 0.0
        for k in ("TT", "TM", "REB"):
            b[k] = b.get(k, 0.0) * decay
        mode = mode.upper()
        if mode in b:
            b[mode] = min(1.0, b.get(mode, 0.0) + 0.3)
        b["ts"] = ts

    # ---------- pause/resume ----------

    def pause_pair(self, pair: str, duration: Optional[int] = None) -> None:
        self.paused_until[_norm_pair(pair)] = _now() + float(duration or self.cfg.pause_secs)

    def is_paused(self, pair: str) -> bool:
        pair = _norm_pair(pair)
        until = self.paused_until.get(pair, 0.0)
        if until and _now() < until:
            return True
        self.paused_until.pop(pair, None)
        return False

    # ---------- calculs dérivés (pair) ----------

    def _est_daily_volume(self, pair: str, now: float) -> float:
        vol = 0.0
        thr = now - self.cfg.window_daily_s
        for o in self.opps_by_pair.get(pair, []):
            if o["ts"] >= thr:
                vol += _to_float(o.get("volume_usdc"))
        for t in self.trades_by_pair.get(pair, []):
            if t["ts"] >= thr:
                vol += _to_float(t.get("executed_volume_usdc"))
        return vol

    def _recent_freq(self, pair: str, now: float) -> float:
        thr_freq = now - self.cfg.window_opp_freq_s
        opps = [o for o in self.opps_by_pair.get(pair, []) if o["ts"] >= thr_freq]
        return len(opps) / max(1.0, (self.cfg.window_opp_freq_s / 60.0))  # /min

    def _avg_spread_net(self, pair: str, now: float) -> float:
        thr = now - self.cfg.window_score_s
        opps = [o for o in self.opps_by_pair.get(pair, []) if o["ts"] >= thr]
        if not opps:
            return 0.0
        return sum(_to_float(o.get("spread_net")) for o in opps) / max(1, len(opps))

    def _win_rate_24h(self, pair: str, now: float) -> Tuple[float, int]:
        thr = now - 86400
        wins = losses = 0
        for t in self.trades_by_pair.get(pair, []):
            if t.get("ts", 0) >= thr:
                np = t.get("net_profit")
                if isinstance(np, (int, float)) and np != 0:
                    if np > 0:
                        wins += 1
                    else:
                        losses += 1
                else:
                    sign = t.get("net_profit_sign")
                    if sign in (1, -1):
                        if sign > 0:
                            wins += 1
                        else:
                            losses += 1
                    else:
                        # on ignore proprement (option: métrique côté LHM déjà renseignée)
                        pass
        tot = wins + losses
        return ((wins / tot) if tot else 0.0, tot)

    def _latency_penalty(self, pair: str) -> float:
        obj = self.lat_ema_by_pair.get(pair)
        if not obj:
            return 0.0
        e2e = _to_float(obj.get("e2e"))
        if e2e <= 0:
            return 0.0
        excess = max(0.0, e2e - float(self.cfg.e2e_latency_bad_ms))
        return self.cfg.w_latency_penalty * (excess / 100.0)

    def _mode_bonus(self, pair: str) -> float:
        b = self.mode_bias_by_pair.get(pair, {})
        return (
            self.cfg.bonus_mode_TT * _to_float(b.get("TT")) +
            self.cfg.bonus_mode_TM * _to_float(b.get("TM")) +
            self.cfg.bonus_mode_REB * _to_float(b.get("REB"))
        )

    # ---------- calculs dérivés (route) ----------

    def _route_stats_last24(self, key: Tuple[str, str], now: float) -> Tuple[int, int]:
        """retourne (wins, losses) sur 24h pour la route."""
        thr = now - 86400
        wins = losses = 0
        for t in self.trades_by_route.get(key, []):
            if t["ts"] >= thr:
                np = _to_float(t.get("net_profit"))
                if np > 0:
                    wins += 1
                elif np < 0:
                    losses += 1
        return wins, losses

    def _route_latency_penalty(self, key: Tuple[str, str]) -> float:
        obj = self.lat_ema_by_route.get(key)
        if not obj:
            return 0.0
        e2e = _to_float(obj.get("e2e"))
        if e2e <= 0:
            return 0.0
        # pénalité légère par rapport au pair-penalty (coefficient dédié)
        return self.cfg.route_latency_penalty_scale * (max(0.0, e2e - float(self.cfg.e2e_latency_bad_ms)) / 100.0)

    def _calc_route_score(self, pair: str, route: str, now: float) -> float:
        """Score 'léger' de route: opp-score moyen + petite pénalité latence + bonus/malus WR."""
        key = (pair, route)
        # opps récents
        thr = now - self.cfg.window_score_s
        opps = [o for o in self.opps_by_route.get(key, []) if o["ts"] >= thr]
        if not opps:
            return -5.0  # route peu vivante
        score = sum(_to_float(o.get("score")) for o in opps) / max(1, len(opps))

        # latence route (pénalité douce)
        score -= self._route_latency_penalty(key)

        # WR 24h léger bonus/malus
        wins, losses = self._route_stats_last24(key, now)
        tot = wins + losses
        if tot >= 3:
            wr = wins / tot
            if wr >= 0.60:
                score += self.cfg.route_win_bonus
            elif wr <= 0.30:
                score -= self.cfg.route_fail_penalty

        return round(score, 2)

    # ---------- scoring pair (principal) ----------

    def _calc_score(self, pair: str, now: float) -> float:
        cfg = self.cfg
        thr_score = now - cfg.window_score_s

        opps = [o for o in self.opps_by_pair.get(pair, []) if o["ts"] >= thr_score]
        if not opps:
            return -10.0

        freq = self._recent_freq(pair, now)
        avg_spread_net = self._avg_spread_net(pair, now)          # fraction
        vol_daily = self._est_daily_volume(pair, now)

        score = 0.0
        score += cfg.w_opp_score * (sum(_to_float(o.get("score")) for o in opps) / max(len(opps), 1))
        score += cfg.w_spread_net * (avg_spread_net * 100.0)      # en points de %
        score += cfg.w_volume * vol_daily

        if freq > (cfg.window_opp_freq_s / 600.0) * 0.3:
            score += cfg.bonus_high_freq

        # slippage courant (EMA)
        slip = self.slippage_by_pair.get(pair, 0.0)
        if slip > cfg.slippage_bad_thr:
            score -= cfg.penalty_slippage

        # résultats
        last_res = self.last_trade_result.get(pair, "unknown")
        if last_res == "fail":
            score -= cfg.penalty_trade_fail
        elif last_res == "success":
            score += 3.0

        # prudence/volatilité
        prudence = (self.volatility_by_pair.get(pair, {}).get("prudence_signal") or "normal").lower()
        if prudence in ("modéré", "modere"):
            score -= cfg.penalty_vol_modere
        elif prudence in ("élevé", "eleve"):
            score -= cfg.penalty_vol_eleve
        else:
            score += cfg.bonus_low_vol

        # latence (pair)
        score -= self._latency_penalty(pair)
        # mode bias global (faible)
        score += self._mode_bonus(pair)

        # ajustement via meilleure route (léger)
        best_route = self._pick_best_route(pair, now)
        if best_route:
            route_score = self.route_scores.get((pair, best_route), 0.0)
            score += 0.10 * route_score

        return round(score, 2)

    # ---------- sélection & rotation ----------

    def _pick_best_route(self, pair: str, now: float) -> Optional[str]:
        """calcule et mémorise la meilleure route récente pour une paire donnée."""
        keys = [rk for rk in self.opps_by_route.keys() if rk[0] == pair] or \
               [rk for rk in self.trades_by_route.keys() if rk[0] == pair]
        if not keys:
            self.best_route_by_pair[pair] = None
            return None
        scored: List[Tuple[str, float]] = []
        for _, route in keys:
            s = self._calc_route_score(pair, route, now)
            self.route_scores[(pair, route)] = s
            scored.append((route, s))
        scored.sort(key=lambda x: x[1], reverse=True)
        best = scored[0][0] if scored else None
        self.best_route_by_pair[pair] = best
        return best

    def _mode_specific_score(self, pair: str, base_score: float, mode: str) -> float:
        """Score utilisé pour classer par mode: base + (bonus_mode_mode * bias_mode)."""
        b = self.mode_bias_by_pair.get(pair, {})
        bias = _to_float(b.get(mode, 0.0))
        bonus = getattr(self.cfg, f"bonus_mode_{mode}", 0.0)
        return round(base_score + bonus * bias, 3)

    def _apply_mode_universe(self, pairs: Iterable[str], mode: str) -> List[str]:
        if not self.cfg.constrain_universe_by_mode:
            return list(pairs)
        universe = self.universe_by_mode.get(mode)
        if not universe:
            return list(pairs)
        return [p for p in pairs if p in universe]

    # pair_history_tracker.py — dans class PairHistoryTracker

    def collect_pair_metrics(self, pair_key: str) -> dict:
        """
        Retourne un snapshot riche pour une paire :
          - scores (pair + meilleure route)
          - ema latences (pair + route)
          - slippage/volatilité instantanés
          - best_route (clé BUY:EX→SELL:EX) si dispo
        """
        pk = _norm_pair(pair_key)
        out = {
            "pair": pk,
            "score": float(self.pair_scores.get(pk, 0.0)),
            "volatility": dict(self.volatility_by_pair.get(pk, {})),
            "slippage": float(self.slippage_by_pair.get(pk, 0.0)),
            "ema_latency_ms": dict(self.lat_ema_by_pair.get(pk, {})),
            "best_route": None,
            "route_score": None,
            "route_latency_ema_ms": None,
            "last_trade_result": self.last_trade_result.get(pk, "unknown"),
        }
        # meilleure route si connue
        best = None
        best_score = None
        for (p, route), sc in self.route_scores.items():
            if p != pk:
                continue
            if best is None or sc > best_score:
                best, best_score = route, sc
        if best:
            out["best_route"] = best
            out["route_score"] = float(best_score or 0.0)
            out["route_latency_ema_ms"] = dict(self.lat_ema_by_route.get((pk, best), {}))
        return out

    def rotate_pairs(self, *, now: float | None = None) -> dict:
        """
        Rotation douce des paires actives par mode (TT/TM/REB) avec quotas.
        Met à jour les sets self.active_pairs_by_mode[mode] et renvoie un rapport.
        """
        now = now or _now()
        rep = {"TT": [], "TM": [], "REB": [], "banned": [], "timestamp": now}
        # Calcul des classements par mode (déjà alimentés continuellement ailleurs)
        rank_TT = self.get_priority_pairs_by_mode("TT", k=self.cfg.max_active_TT or self.cfg.max_active)
        rank_TM = self.get_priority_pairs_by_mode("TM", k=self.cfg.max_active_TM or self.cfg.max_active)
        rank_REB = self.get_priority_pairs_by_mode("REB", k=self.cfg.max_active_REB or self.cfg.max_active)
        # Appliquer
        self.active_pairs_by_mode["TT"] = set(rank_TT)
        self.active_pairs_by_mode["TM"] = set(rank_TM)
        self.active_pairs_by_mode["REB"] = set(rank_REB)
        rep["TT"], rep["TM"], rep["REB"] = rank_TT, rank_TM, rank_REB
        # Rapport punitions/pauses (si implémentés)
        pauses = getattr(self, "paused_until", {})
        rep["paused"] = {p: t for p, t in pauses.items() if t > now}
        return rep

    # log_writer.py — dans class LogWriter

    def finalize_day(self, *, sign_priv_pem_path: str | None = None) -> dict:
        """
        Fin de journée : checkpoint WAL + VACUUM + hash + (option) signature ECDSA.
        Retourne un dict {db_path, bytes, sha256, sig_path?}.
        """
        path = self.db_path
        meta = {"db_path": path, "bytes": 0, "sha256": None, "sig_path": None}
        try:
            # checkpoint WAL + optimize
            self.wal_checkpoint("TRUNCATE")
        except Exception:
            logger.exception("finalize_day: wal_checkpoint failed")
        try:
            self.optimize()
        except Exception:
            logger.exception("finalize_day: optimize failed")
        # tailles & hash
        try:
            meta["bytes"] = os.path.getsize(path)
            h = self._rolling_hash_file(path)
            meta["sha256"] = h
        except Exception:
            logger.exception("finalize_day: rolling hash failed")
        # signature optionnelle
        if sign_priv_pem_path:
            try:
                sigp = self._ecdsa_sign(path, sign_priv_pem_path)
                meta["sig_path"] = sigp
            except Exception:
                logger.exception("finalize_day: ecdsa sign failed")
        # petit backup compressé
        try:
            self.backup_database()
        except Exception:
            logger.exception("finalize_day: backup failed")
        return meta

    def _rolling_hash_file(self, filepath: str, *, chunk: int = 1 << 20) -> str:
        """
        Hash incrémental SHA-256 d’un gros fichier sans épuiser la RAM.
        """
        import hashlib
        h = hashlib.sha256()
        with open(filepath, "rb") as f:
            while True:
                b = f.read(chunk)
                if not b: break
                h.update(b)
        return h.hexdigest()

    def _ecdsa_sign(self, filepath: str, pem_priv_key_path: str) -> str:
        """
        Signe le hash du fichier via ECDSA (secp256k1 si dispo, sinon secp256r1).
        Écrit le .sig à côté du fichier et retourne le chemin de la signature.
        """
        try:
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import ec
            from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature
            from cryptography.hazmat.backends import default_backend
        except Exception as e:
            raise RuntimeError("cryptography package required for ECDSA signing") from e

        digest_hex = self._rolling_hash_file(filepath)
        digest = bytes.fromhex(digest_hex)

        with open(pem_priv_key_path, "rb") as fh:
            key = serialization.load_pem_private_key(fh.read(), password=None, backend=default_backend())
        # secp256k1 si possible, sinon r1
        if not isinstance(key.curve, (ec.SECP256K1, ec.SECP256R1)):
            key = ec.generate_private_key(ec.SECP256R1(), default_backend())

        sig = key.sign(digest, ec.ECDSA(hashes.SHA256()))
        r, s = encode_dss_signature(sig)  # DER->(r,s) pour portabilité
        out = f"{filepath}.sig"
        with open(out, "wb") as fh:
            fh.write(sig)
        return out

    # ---------- vues ----------

    def get_active_pairs(self) -> List[str]:
        return list(self.active_pairs)

    def get_priority_pairs(self, k: Optional[int] = None) -> List[str]:
        k = k or self.cfg.max_active
        return [p for p in self.active_pairs if not self.is_paused(p)][:k]

    def get_score(self, pair: str) -> float:
        return float(self.pair_scores.get(_norm_pair(pair), 0.0))

    def get_rank_map(self) -> Dict[str, int]:
        return {p: i + 1 for i, p in enumerate(self.active_pairs)}

    # --- nouvelles vues par mode ---

    def get_active_pairs_by_mode(self, mode: Optional[str] = None) -> List[str]:
        """Si mode=None → renvoie la vue globale; sinon la vue spécifique."""
        if not mode:
            return self.get_active_pairs()
        mode = mode.upper()
        return list(self.active_pairs_by_mode.get(mode, []))

    def get_priority_pairs_by_mode(self, mode: str, k: Optional[int] = None) -> List[str]:
        mode = (mode or "").upper()
        base = self.get_active_pairs_by_mode(mode)
        k = k or (self.max_active_by_mode.get(mode) or len(base))
        return [p for p in base if not self.is_paused(p)][:k]

    def get_rank_map_by_mode(self, mode: Optional[str] = None) -> Dict[str, int]:
        if not mode:
            return self.get_rank_map()
        mode = mode.upper()
        arr = self.get_active_pairs_by_mode(mode)
        return {p: i + 1 for i, p in enumerate(arr)}

    # --- univers/quotas par mode (réglages) ---

    def set_max_active_by_mode(self, **kw) -> None:
        """
        Exemple:
            set_max_active_by_mode(TT=10, TM=6, REB=2)
        Toute valeur None est ignorée.
        """
        for mode, val in kw.items():
            m = (mode or "").upper()
            if m not in ("TT", "TM", "REB"):
                continue
            if val is None:
                continue
            try:
                v = int(val)
                if v >= 0:
                    self.max_active_by_mode[m] = v
            except Exception:
                continue

    def set_universe_for_mode(self, mode: str, pairs: Iterable[str]) -> None:
        """Définit un sous-univers de paires autorisées pour un mode."""
        mode = (mode or "").upper()
        if mode not in self.universe_by_mode:
            return
        self.universe_by_mode[mode] = { _norm_pair(p) for p in (pairs or []) }

    def set_constrain_universe_by_mode(self, enabled: bool) -> None:
        self.cfg.constrain_universe_by_mode = bool(enabled)

    # ---------- routes & snapshots ----------

    def get_best_routes(self, pair: str, k: int = 3) -> List[str]:
        """Top-k routes pour une paire (récentes)."""
        p = _norm_pair(pair)
        now = _now()
        # refresh route_scores si besoin
        self._pick_best_route(p, now)
        routes = [(rk[1], self.route_scores.get(rk, -999)) for rk in self.route_scores.keys() if rk[0] == p]
        routes.sort(key=lambda x: x[1], reverse=True)
        return [r for r, _ in routes[:max(1, k)]]

    def get_pair_snapshot(self, pair: str) -> Dict[str, Any]:
        """Instantané enrichi (score, latences EMA, route)."""
        p = _norm_pair(pair)
        now = _now()
        win_rate, n_trades = self._win_rate_24h(p, now)
        lat_ema = self.lat_ema_by_pair.get(p, {})
        best_route = self.best_route_by_pair.get(p) or self._pick_best_route(p, now)
        route_lat = self.lat_ema_by_route.get((p, best_route), {}) if best_route else {}

        return {
            "pair": p,
            "score": self.pair_scores.get(p),
            "rank": self.get_rank_map().get(p),
            "best_route": best_route,
            "latency_ema_ms": {
                "e2e": lat_ema.get("e2e"),
                "ack": lat_ema.get("ack"),
                "fill": lat_ema.get("fill"),
            },
            "route_latency_ema_ms": {
                "e2e": route_lat.get("e2e"),
                "ack": route_lat.get("ack"),
                "fill": route_lat.get("fill"),
            } if best_route else None,
            "slippage_ema": self.slippage_by_pair.get(p),
            "last_trade_result": self.last_trade_result.get(p, "unknown"),
            "win_rate_24h": win_rate,
            "trades_24h": n_trades,
        }

    def get_pair_snapshot_enriched(self, pair: str, now_s: Optional[float] = None) -> Dict[str, Any]:
        base = self.get_pair_snapshot(pair)  # API existante
        base = dict(base or {})
        flags = self._collect_staleness_flags(pair, now_s)
        base.setdefault("flags", {}).update(flags)
        return base

    def wr_ignored_trades_total(self, since_s: Optional[float] = None) -> int:
        """
        Compte les trades sans net_profit ET sans net_profit_sign.
        Optionnel: borne temporelle depuis since_s (en secondes unix).
        """
        cutoff = float(since_s) if since_s is not None else None
        total = 0
        for pair, dq in self.trades_by_pair.items():
            for t in dq:
                ts = float(_to_float(t.get("ts_s"), 0.0))  # ts_s si présent, sinon 0
                if cutoff is not None and ts and ts < cutoff:
                    continue
                npv = t.get("net_profit", None)
                nps = t.get("net_profit_sign", None)
                if (npv is None) and (nps is None):
                    total += 1
        return total

    def get_status_enriched(self) -> Dict[str, Any]:
        st = dict(self.get_status() or {})
        try:
            st["wr_ignored_trades_total"] = int(self.wr_ignored_trades_total())
        except Exception:
            # pas d’impact business; on évite de casser le status
            pass
        return st

    def get_ranked_pairs(self) -> List[str]:
        ranks = self.get_rank_map()  # {pair: 1, 2, ...}
        return [p for p, _r in sorted(ranks.items(), key=lambda kv: kv[1])]  # 1 = top

    # ---------- export métriques ----------


    # ---------- statut & contrôle ----------

    def get_status(self) -> Dict[str, Any]:
        return {
            "module": "PairHistoryTracker",
            "healthy": True,
            "active_pairs": list(self.active_pairs),
            "active_pairs_by_mode": {m: list(v) for m, v in self.active_pairs_by_mode.items()},
            "paused_pairs": [p for p in list(self.paused_until.keys()) if self.is_paused(p)],
            "top_scores": {p: self.pair_scores.get(p) for p in self.active_pairs},
            "rotation_interval": self.cfg.rotation_interval,
            "last_rotation": self.last_rotation or 0.0,
            "best_routes": {p: self.best_route_by_pair.get(p) for p in self.active_pairs},
            "max_active_by_mode": dict(self.max_active_by_mode),
            "constrain_universe_by_mode": bool(self.cfg.constrain_universe_by_mode),
        }

    # Réglages dynamiques pratiques
    def set_max_active(self, n: int) -> None:
        self.cfg.max_active = int(max(1, n))

    def set_rotation_interval(self, seconds: int) -> None:
        self.cfg.rotation_interval = int(max(5, seconds))

    def reset_pair(self, pair: str) -> None:
        p = _norm_pair(pair)
        self.opps_by_pair.pop(p, None)
        self.trades_by_pair.pop(p, None)
        # purge routes liées
        for key in [rk for rk in list(self.opps_by_route.keys()) if rk[0] == p]:
            self.opps_by_route.pop(key, None)
        for key in [rk for rk in list(self.trades_by_route.keys()) if rk[0] == p]:
            self.trades_by_route.pop(key, None)
        for key in [rk for rk in list(self.lat_ema_by_route.keys()) if rk[0] == p]:
            self.lat_ema_by_route.pop(key, None)

        self.volatility_by_pair.pop(p, None)
        self.slippage_by_pair.pop(p, None)
        self.last_trade_result.pop(p, None)
        self.lat_ema_by_pair.pop(p, None)
        self.mode_bias_by_pair.pop(p, None)
        self.pair_scores.pop(p, None)
        self.best_route_by_pair.pop(p, None)

        try:
            self.active_pairs.remove(p)
        except ValueError:
            import logging
            logging.exception("Unhandled exception")