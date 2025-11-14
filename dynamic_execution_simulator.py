# ================= modules/dynamic_execution_simulator.py =================
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
DynamicExecutionSimulator — passif, 3 branches de combos CEX + TT/TM
--------------------------------------------------------------------

Rôle
====
- Simulateur d’exécution “depth-aware” (consommation niveau par niveau),
  sans I/O réseau, encapsulé par le **RiskManager**.
- Supporte 3 combos cross-CEX:
    1) BINANCE -> COINBASE
    2) COINBASE -> BYBIT
    3) BYBIT   -> BINANCE
- Deux stratégies:
    • TT (Taker/Taker)  : taker des deux côtés (VWAP des asks/bids consommés)
    • TM (Taker/Maker)  : taker côté achat, maker côté vente (prix maker conservateur)

Intégrations RiskManager
========================
- Volatilité: `update_volatility_metrics(symbol, metrics)`
- Slippage: `update_slippage(exchange, pair, side, value)`
- Paramètres dynamiques: `update_trade_parameters(symbol, adjustments)`
- Event sink (callback injecté par le RiskManager): `set_event_sink(callable)`

IDs
===
- combo_key : "EX_A->EX_B"
- strategy  : "TT" | "TM"
- trade_id  : "{strategy}:{combo_key}:{pair}:{ts_ms}:{nonce}"
"""

import asyncio
import logging
import random
import time
import math
from datetime import datetime
from typing import List, Tuple, Dict, Any, Optional, Callable, Set
from modules.rm_compat import getattr_int, getattr_float, getattr_str, getattr_bool, getattr_dict, getattr_list
from contracts.errors import (
    RMError, NotReadyError, DataStaleError, InconsistentStateError, ExternalServiceError,SimulationError
)
# Hooks observabilité (facultatifs)
# Prometheus metrics (graceful fallback si absents)
try:
    from modules.obs_metrics import SIM_DECISION_MS, SIMULATED_VWAP_DEVIATION_BPS
except Exception:  # pragma: no cover
    class _NoopHist:
        def observe(self, *a, **k):  # no-op
            return
    SIM_DECISION_MS = _NoopHist()
    SIMULATED_VWAP_DEVIATION_BPS = _NoopHist()

try:
    from modules.observability import sim_on_run
except Exception:  # pragma: no cover
    def sim_on_run(mode: str, vwap_dev: Optional[float] = None, fragments: int = 0, blocked: bool = False) -> None:
        return

# Signaux non-fatals (facultatif, no-op si absent)
try:
    from modules.observability import report_nonfatal
except Exception:  # pragma: no cover
    def report_nonfatal(module: str, kind: str, err: Optional[BaseException] = None, **labels) -> None:
        return

logger = logging.getLogger("DynamicExecutionSimulator")

Number = float
Level = Tuple[Number, Number]  # (price, qty_base)

# Combos autorisés
_DEFAULT_ROUTES = {
    ("BINANCE", "BYBIT"),
    ("BYBIT", "BINANCE"),
    ("BINANCE", "COINBASE"),
    ("COINBASE", "BINANCE"),
    ("BYBIT", "COINBASE"),
    ("COINBASE", "BYBIT"),
}

# --- PATCH: constantes numériques sûres ---
_EPS_QTY = 1e-12
_EPS_PRICE = 1e-12


class DynamicExecutionSimulator:
    def __init__(self, config, depth_limit: int = 10):
        self.bot_cfg = config
        self.config = config  # <-- alias back-compat
        self.depth_limit = int(depth_limit)
        self._allowed_routes: Set[Tuple[str, str]] = set()
        self._fee_map_pct: Dict[str, Dict[str, float]] = {}  # {EX:{maker: frac, taker: frac}}

        # Contexte poussé par le RiskManager
        self.volatility_metrics: Dict[str, Dict[str, Any]] = {}
        self.trade_parameters: Dict[str, Dict[str, Any]] = {}
        self._slippage: Dict[Tuple[str, str, str], float] = {}  # (EX, PAIR, SIDE) -> slip
        self._event_sink: Optional[Callable[[dict], None]] = None

        # État & métriques locales (protégées par _lock)
        self._lock = asyncio.Lock()
        self.last_result: Optional[Dict[str, Any]] = None
        self.simulation_count = 0
        self.avg_vwap_deviation = 0.0
        self.blocked_trade_count = 0
        self.fragmented_trade_count = 0
        self.last_simulation_time = 0.0
        self.last_restart_reason = None
        self.last_latency_ms: float = 0.0  # NEW: latence dernière simu (ms)

    # ----------------- hooks RiskManager -----------------
    def set_event_sink(self, sink: Optional[Callable[[dict], None]]) -> None:
        self._event_sink = sink

    # --------- NOUVEAU: injection de routes autorisées ---------
    def update_allowed_routes(self, routes: List[Tuple[str, str]]) -> None:
        try:
            self._allowed_routes = {(a.upper(), b.upper()) for a, b in (routes or [])}
        except Exception as e:
            report_nonfatal("DynamicExecutionSimulator", "update_allowed_routes_failed", e, phase="init")
            self._allowed_routes = set()

    def set_fee_map_pct(self, fee_map: Dict[str, Dict[str, float]]) -> None:
        """
        Alimente la carte des frais en fractions, p.ex. {"BINANCE":{"maker":0.000, "taker":0.0005}, ...}
        """
        m: Dict[str, Dict[str, float]] = {}
        for ex, rec in (fee_map or {}).items():
            try:
                exu = str(ex).upper()
                mk = float((rec or {}).get("maker", 0.0) or 0.0)
                tk = float((rec or {}).get("taker", 0.0) or 0.0)
                m[exu] = {"maker": mk, "taker": tk}
            except Exception as e:
                report_nonfatal("DynamicExecutionSimulator", "set_fee_map_pct_skip_entry", e, exchange=str(ex))
                continue
        self._fee_map_pct = m



    def _fee_default(self, exchange: str, role: str) -> float:
        """Retourne une fraction de fee depuis la fee map interne, sinon 0.0."""
        try:
            return float(self._fee_map_pct.get(str(exchange).upper(), {}).get(role, 0.0) or 0.0)
        except Exception:
            return 0.0

    def update_volatility_metrics(self, symbol: str, metrics: Dict[str, Any]):
        self.volatility_metrics[symbol] = dict(metrics or {})

    def update_trade_parameters(self, symbol: str, adjustments: Dict[str, Any]):
        self.trade_parameters[symbol] = dict(adjustments or {})

    def update_slippage(self, exchange: str, pair: str, side: str, value: float) -> None:
        k = (str(exchange).upper(), str(pair).replace("-", "").upper(), str(side).upper())
        try:
            self._slippage[k] = float(value)
        except Exception as e:
            report_nonfatal("DynamicExecutionSimulator", "update_slippage_failed", e, phase="update_slippage")
            logging.exception("Unhandled exception")

    # ------------------------- helpers --------------------------
    @staticmethod
    def _sanitize_levels(levels: List[Tuple[Any, Any]], *, sort_asc: Optional[bool] = None) -> List[Level]:
        """Nettoie et (optionnellement) trie les niveaux (ignore NaN/inf et <=0)."""
        out: List[Level] = []
        for p, q in (levels or []):
            try:
                pf, qf = float(p), float(q)
                if not (math.isfinite(pf) and math.isfinite(qf)):
                    continue
                if pf > _EPS_PRICE and qf > _EPS_QTY:
                    out.append((pf, qf))
            except Exception:
                continue
        if sort_asc is True:
            out.sort(key=lambda x: x[0])
        elif sort_asc is False:
            out.sort(key=lambda x: x[0], reverse=True)
        return out

    def _cum_usdc(self, levels: List[Level]) -> float:
        total = 0.0
        for p, q in levels[: self.depth_limit]:
            total += p * q
        return total

    def _slip(self, exchange: str, pair: str, side: str) -> float:
        return float(self._slippage.get((exchange.upper(), pair.replace("-", "").upper(), side.upper()), 0.0))

    @staticmethod
    def _mk_trade_id(strategy: str, combo_key: str, pair: str) -> str:
        ts_ms = int(time.time() * 1000)
        nonce = format(random.randint(0, 0xFFFF), "04x").upper()
        return f"{strategy}:{combo_key}:{pair}:{ts_ms}:{nonce}"

    @staticmethod
    def _combo_key(buy_ex: str, sell_ex: str) -> str:
        return f"{buy_ex.upper()}->{sell_ex.upper()}"

    # --------- NOUVEAU: check route via state local ou fallback 6 routes ---------
    def _combo_allowed(self, buy_ex: str, sell_ex: str) -> bool:
        a, b = str(buy_ex).upper(), str(sell_ex).upper()
        if self._allowed_routes:
            return (a, b) in self._allowed_routes
        return (a, b) in _DEFAULT_ROUTES

    def _consume_depth_fragment(
        self,
        buy_levels: List[Level],   # asks: ascending
        sell_levels: List[Level],  # bids: descending
        fragment_usdc_budget: float,
    ) -> Optional[Dict[str, float]]:
        if fragment_usdc_budget <= 0:
            return None

        i_buy, i_sell = 0, 0
        buy = [(p, q) for p, q in buy_levels[: self.depth_limit]]
        sell = [(p, q) for p, q in sell_levels[: self.depth_limit]]

        cost_usdc = 0.0
        revenue_usdc = 0.0
        exec_base_total = 0.0
        budget_left = float(fragment_usdc_budget)

        while budget_left > _EPS_QTY and i_buy < len(buy) and i_sell < len(sell):
            buy_price, buy_qty = buy[i_buy]
            sell_price, sell_qty = sell[i_sell]
            if sell_price <= _EPS_PRICE or buy_price <= _EPS_PRICE or sell_price <= buy_price:
                break

            max_qty_from_budget = budget_left / buy_price
            qty_exec = min(buy_qty, sell_qty, max_qty_from_budget)
            if qty_exec <= _EPS_QTY:
                break

            cost = qty_exec * buy_price
            revenue = qty_exec * sell_price

            cost_usdc += cost
            revenue_usdc += revenue
            exec_base_total += qty_exec
            budget_left -= cost

            new_buy_qty = buy_qty - qty_exec
            new_sell_qty = sell_qty - qty_exec
            buy[i_buy] = (buy_price, new_buy_qty)
            sell[i_sell] = (sell_price, new_sell_qty)
            if new_buy_qty <= _EPS_QTY:
                i_buy += 1
            if new_sell_qty <= _EPS_QTY:
                i_sell += 1

        if exec_base_total <= _EPS_QTY or cost_usdc <= 0:
            return None

        vwap_buy = cost_usdc / exec_base_total
        vwap_sell = revenue_usdc / exec_base_total
        return {
            "exec_base_qty": exec_base_total,
            "cost_usdc": cost_usdc,
            "revenue_usdc": revenue_usdc,
            "vwap_buy": vwap_buy,
            "vwap_sell": vwap_sell,
        }

    # --------------------------- planification de slices ---------------------------
    def suggest_slices(
            self,
            *,
            budget_usdc: float,
            asks: List[Tuple[Any, Any]],
            bids: List[Tuple[Any, Any]],
            target_participation: Optional[float] = None,
            max_frags: Optional[int] = None,
            min_frag_usdc: Optional[float] = None,
            safety_pad: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Propose un plan de fragmentation côté RM.plan_fragments().
        - budget_usdc: notionnel en devise de cotation (USDC/EUR)
        - target_participation: % visé de la profondeur (asks/bids) utilisable (0..1)
        - max_frags: nb max de fragments
        - min_frag_usdc: taille mini d’un fragment (notional quote)
        - safety_pad: coefficient <1.0 pour rester conservateur sur la depth
        """
        # 1) Sanitize & métriques ladder
        asks_n = self._sanitize_levels(asks, sort_asc=True)
        bids_n = self._sanitize_levels(bids, sort_asc=False)

        # Si pas de depth exploitable -> 1 fragment = tout
        if not asks_n or not bids_n or budget_usdc <= 0:
            return {
                "auto": False,
                "count": 1 if budget_usdc > 0 else 0,
                "fragment_usdc": float(budget_usdc),
                "amounts": [float(budget_usdc)] if budget_usdc > 0 else [],
            }

        # Cap utilisable côté ladder (quote)
        ladder_quote = min(self._cum_usdc(asks_n), self._cum_usdc(bids_n))

        # 2) Paramètres avec fallbacks cfg
        cfg = self.config
        tp = float(target_participation if target_participation is not None
                   else getattr(cfg, "target_ladder_participation", 0.25))
        tp = max(0.0, min(tp, 1.0))

        mf = int(max_frags if max_frags is not None else getattr_int(cfg, "max_fragments", 5))
        mf = max(1, mf)

        mn = float(min_frag_usdc if min_frag_usdc is not None else getattr_float(cfg, "min_fragment_usdc", 200.0))
        mn = max(1.0, mn)

        pad = float(safety_pad if safety_pad is not None else getattr_float(cfg, "fragment_safety_pad", 0.9))
        pad = max(0.5, min(pad, 1.0))

        # 3) Budget effectif (capé par ladder * participation * pad)
        usable_cap = ladder_quote * tp * pad
        eff_budget = max(0.0, min(float(budget_usdc), float(usable_cap)))

        # Si ladder trop faible → on prend le budget complet (on laissera RM revalider)
        if eff_budget <= 0:
            eff_budget = float(budget_usdc)

        # 4) Nombre de fragments cible
        #    - on évite des fragments < min_frag_usdc
        #    - on respecte mf
        #    - on autorise 1 fragment si petit budget
        if eff_budget <= mn:
            count = 1
        else:
            count = int(min(mf, max(1, round(eff_budget / mn))))
        # Recalcule d’une taille “moyenne” indicative
        frag_usdc = eff_budget / count

        # 5) Répartition front-load (50/35/15) si count >= 3, sinon équi
        weights = [0.5, 0.35, 0.15]
        s = sum(weights)
        weights = [w / s for w in weights]

        amounts: List[float] = []
        if count <= 3:
            # fragments (quasi) égaux avec dernier = reste
            base = max(mn, frag_usdc)
            for i in range(count):
                left = eff_budget - sum(amounts)
                amt = base if i < (count - 1) else max(0.0, left)
                amounts.append(min(amt, max(0.0, left)))
        else:
            g1n = max(1, min(count, 3))
            g2n = max(0, min(count - g1n, 3))
            g3n = max(0, count - g1n - g2n)
            groups = [(weights[0], g1n), (weights[1], g2n), (weights[2], g3n)]
            for w, n in groups:
                if n <= 0:
                    continue
                target = eff_budget * w
                slice_amt = max(mn, target / n)
                for i in range(n):
                    left = eff_budget - sum(amounts)
                    amt = slice_amt if i < (n - 1) else max(0.0, left)
                    amounts.append(min(amt, max(0.0, left)))
            # Correction d’arrondi si besoin
            diff = eff_budget - sum(amounts)
            if abs(diff) > 1e-6 and amounts:
                amounts[-1] = max(0.0, amounts[-1] + diff)

        return {
            "auto": True,
            "count": len(amounts),
            "fragment_usdc": (sum(amounts) / max(1, len(amounts))),
            "amounts": amounts,
        }

    # ------------------------- noyau stateless -------------------------
    async def _simulate_core(
        self,
        opportunity: Dict[str, Any],
        buy_levels: List[Level],
        sell_levels: List[Level],
        *,
        capital_available_usdc: float,  # conservé pour compatibilité (non utilisé directement)
        strategy: str,
        fees_buy_pct: float,
        fees_sell_pct: float,
        vwap_guard_bps: float,
        rebalancing_mode: bool,
        fragment_count: int,
        fragment_usdc: float,
        auto_fragment: bool,
        maker_fill_ratio: float,
        maker_skew_bps: float,
        vol: Dict[str, Any],
        params: Dict[str, Any],
        combo_key: str,
    ) -> Tuple[Optional[Dict[str, Any]], float]:
        """
        Exécute la simulation sans modifier l'état de l'objet.
        Renvoie (result, deviation_vwap).
        """
        symbol = str(opportunity.get("pair", "UNKNOWN")).replace("-", "").upper()
        buy_ex = str(opportunity.get("buy_exchange", "")).upper()
        sell_ex = str(opportunity.get("sell_exchange", "")).upper()

        # slippage par jambe (depuis état local)
        slip_buy = self._slip(buy_ex, symbol, "BUY")
        slip_sell = self._slip(sell_ex, symbol, "SELL")

        total_fees_pct = float(fees_buy_pct) + float(fees_sell_pct)

        total_exec_base = 0.0
        total_cost = 0.0
        total_revenue = 0.0
        vwap_buy_accum = 0.0
        vwap_sell_accum = 0.0

        maker_price_used: Optional[float] = None  # PATCH: exposer le prix maker utilisé

        for i in range(max(1, fragment_count)):
            if strategy.upper() == "TT":
                frag = self._consume_depth_fragment(buy_levels, sell_levels, fragment_usdc)
                if not frag:
                    if rebalancing_mode:
                        logger.warning(f"[Sim:TT] Fragment {i+1}/{fragment_count} non exécutable — rebalancing")
                        continue
                    return (None, 0.0)

                vwap_buy = frag["vwap_buy"]
                vwap_sell = frag["vwap_sell"]

                if vwap_guard_bps > 0:
                    bps_gap = abs(vwap_sell - vwap_buy) / max(vwap_buy, _EPS_PRICE) * 1e4
                    if bps_gap > vwap_guard_bps and not rebalancing_mode:
                        return (None, 0.0)

                cost_adj = frag["cost_usdc"] * (1.0 + max(0.0, slip_buy))
                revenue_adj = frag["revenue_usdc"] * (1.0 - max(0.0, slip_sell))

                total_exec_base += frag["exec_base_qty"]
                total_cost += cost_adj
                total_revenue += revenue_adj
                vwap_buy_accum += vwap_buy * frag["exec_base_qty"]
                vwap_sell_accum += vwap_sell * frag["exec_base_qty"]

            else:  # TM  (taker côté achat, maker côté vente par défaut)
                frag = self._consume_depth_fragment(buy_levels, sell_levels, fragment_usdc)

                if not frag:
                    if rebalancing_mode:
                        logger.warning(f"[Sim:TM] Fragment {i + 1}/{fragment_count} non exécutable — rebalancing")
                        continue
                    return (None, 0.0)

                # --- NOUVEAU: maker price selon tick+pad (alignement Engine), fallback bps ---
                maker_price_used = None

                # On suppose maker sur l’exchange de vente (SELL).
                gf = getattr(self.config, "get_pair_filters", None)
                tick = 0.0
                if callable(gf):
                    try:
                        f = gf(sell_ex, symbol) or {}
                        tick = float(f.get("tick_size", 0) or 0.0)
                    except Exception:
                        tick = 0.0

                # meilleur bid côté SELL (on l'a via sell_levels)
                best_bid_sell = max(p for p, _ in sell_levels[: max(1, self.depth_limit)])

                # essayer de récupérer un best_ask SELL (optionnel), sinon fallback
                opp_best_ask = None
                try:
                    opp_best_ask = float(opportunity.get("sell_best_ask") or opportunity.get("maker_best_ask") or 0.0)
                except Exception:
                    opp_best_ask = None

                # pad ticks partagé avec l'Engine si dispo; sinon interpréter maker_skew_bps en ticks=0
                pad_ticks = int(getattr(self.config, "maker_pad_ticks", 0) or 0)

                if opp_best_ask and opp_best_ask > 0 and tick > 0:
                    # Alignement Engine: SELL maker = best_ask + pad_ticks * tick
                    maker_price = max(0.0, opp_best_ask + max(0, pad_ticks) * tick)
                elif tick > 0:
                    # Fallback “tick-pad” à partir du best_bid: place au moins 1 tick au-dessus du bid
                    maker_price = max(0.0, best_bid_sell + max(1, pad_ticks) * tick)
                else:
                    # Fallback bps historique si tick inconnu
                    maker_price = best_bid_sell * (1.0 + abs(maker_skew_bps) / 1e4)

                maker_price_used = maker_price

                # PATCH: calibrer l'achat taker sur le fill maker estimé
                exec_qty_buy = frag["exec_base_qty"] * max(0.0, min(1.0, maker_fill_ratio))
                if exec_qty_buy <= _EPS_QTY:
                    if rebalancing_mode:
                        pass
                    else:
                        return (None, 0.0)

                buy_ratio = exec_qty_buy / max(frag["exec_base_qty"], _EPS_QTY)
                cost_adj = frag["cost_usdc"] * buy_ratio * (1.0 + max(0.0, slip_buy))
                revenue_maker = exec_qty_buy * maker_price

                total_exec_base += exec_qty_buy
                total_cost += cost_adj
                total_revenue += revenue_maker
                vwap_buy_accum += frag["vwap_buy"] * exec_qty_buy
                vwap_sell_accum += maker_price * exec_qty_buy

        if total_exec_base <= _EPS_QTY or total_cost <= 0:
            return (None, 0.0)

        vwap_buy = vwap_buy_accum / total_exec_base if total_exec_base > 0 else 0.0
        vwap_sell = vwap_sell_accum / total_exec_base if total_exec_base > 0 else 0.0

        gross = (total_revenue - total_cost) / max(total_cost, _EPS_PRICE)
        net_final_spread = gross - float(total_fees_pct)
        if net_final_spread < 0 and not rebalancing_mode:
            return (None, abs(vwap_sell - vwap_buy))

        deviation = abs(vwap_sell - vwap_buy)

        trade_id = self._mk_trade_id(strategy=strategy.upper(), combo_key=combo_key, pair=symbol)
        result = {
            "timestamp": datetime.utcnow().isoformat(),
            "trade_id": trade_id,
            "pair": symbol,
            "combo_key": combo_key,
            "strategy": strategy.upper(),
            "buy_exchange": buy_ex,
            "sell_exchange": sell_ex,
            "executed_volume_usdc": round(total_cost, 2),
            "exec_base_qty": round(total_exec_base, 8),
            "vwap_buy": round(vwap_buy, 6),
            "vwap_sell": round(vwap_sell, 6),
            "spread_net_final": round(net_final_spread * 100, 4),   # %
            "net_bps": round(net_final_spread * 1e4, 2),            # bps
            "fees": {"buy_pct": fees_buy_pct, "sell_pct": fees_sell_pct, "total_pct": float(total_fees_pct)},
            "slippage": {
                "buy_taker_pct": slip_buy if strategy.upper() in ("TT", "TM") else 0.0,
                "sell_taker_pct": slip_sell if strategy.upper() == "TT" else 0.0
            },
            "volatility": vol,
            "parameters": params,
            "status": "rentable" if net_final_spread >= 0 else "non_rentable",
            "mode": "rebalancing" if rebalancing_mode else "standard",
            "fragments": {
                "count": int(max(1, fragment_count)),
                "avg_fragment_usdc": round(fragment_usdc, 2),
                "auto": bool(auto_fragment),
            },
        }
        if maker_price_used is not None:
            result["maker_price"] = round(maker_price_used, 6)

        # Observabilité (ne modifie pas l'état de l'objet)
        try:
            sim_on_run("rebalancing" if rebalancing_mode else "standard",
                       vwap_dev=deviation, fragments=int(max(1, fragment_count)))
        except Exception as e:
            report_nonfatal("DynamicExecutionSimulator", "sim_on_run_failed", e, phase="_simulate_core")
            logging.exception("Unhandled exception")

        return (result, deviation)

    # ------------------------- simulateur principal (TT / TM) -------------------------
    async def simulate(
        self,
        opportunity: Dict[str, Any],
        buy_levels_raw: List[Tuple[Any, Any]],
        sell_levels_raw: List[Tuple[Any, Any]],
        *,
        capital_available_usdc: float,
        strategy: str = "TT",               # "TT" | "TM"
        fees_total_pct: Optional[float] = None,
        rebalancing_mode: bool = False,
        timeout: float = 2.0,
    ) -> Optional[Dict[str, Any]]:
        """
        Simulation d'une seule branche (TT ou TM).
        """
        symbol = str(opportunity.get("pair", "UNKNOWN")).replace("-", "").upper()
        buy_ex  = str(opportunity.get("buy_exchange", "")).upper()
        sell_ex = str(opportunity.get("sell_exchange", "")).upper()

        if not self._combo_allowed(buy_ex, sell_ex):
            logger.debug(f"[Sim] Combo non autorisé: {buy_ex}->{sell_ex}")
            return None
        if capital_available_usdc <= 0:
            return None

        # Livres: asks asc / bids desc
        buy_levels  = self._sanitize_levels(buy_levels_raw,  sort_asc=True)
        sell_levels = self._sanitize_levels(sell_levels_raw, sort_asc=False)
        if not buy_levels or not sell_levels:
            return None

        combo_key = self._combo_key(buy_ex, sell_ex)
        params = dict(self.trade_parameters.get(symbol, {}))
        if bool(params.get("block_trade", False)):
            self.blocked_trade_count += 1
            sim_on_run("rebalancing" if rebalancing_mode else "standard", blocked=True)
            return None

        max_usdc_opportunity = float(opportunity.get("volume_possible_usdc", float("inf")))
        budget_total_raw = min(float(capital_available_usdc), max_usdc_opportunity)
        if budget_total_raw <= 0:
            return None

        # Règles dynamiques
        vol = dict(self.volatility_metrics.get(symbol, {}))
        size_factor = float(params.get("size_factor", vol.get("size_factor", 1.0)))
        volume_factor = float(params.get("volume_factor", getattr(self.config, "volume_factor", 1.0))) * max(0.0, min(size_factor, 1.0))
        budget_total = max(0.0, min(1.0, volume_factor)) * budget_total_raw
        if budget_total <= 0:
            return None

        # Fragmentation
        auto_fragment = bool(params.get("auto_fragment", getattr(self.config, "auto_fragment", True)))
        fragment_trade = bool(params.get("fragment_trade", False))
        fragment_count = max(1, int(params.get("fragment_count", 1)))
        vwap_guard_bps = float(params.get("vwap_guard_bps", getattr(self.config, "vwap_guard_bps", 0.0)))

        if auto_fragment:
            plan = self.suggest_slices(
                budget_usdc=budget_total,
                asks=buy_levels,
                bids=sell_levels,
                target_participation=float(getattr(self.config, "target_ladder_participation", 0.25)),
                max_frags=int(getattr(self.config, "max_fragments", 5)),
                min_frag_usdc=float(getattr(self.config, "min_fragment_usdc", 200.0)),
                safety_pad=float(getattr(self.config, "fragment_safety_pad", 0.9)),
            )
            fragment_count = max(1, int(plan["count"]))
            fragment_trade = True
            fragment_usdc = float(plan["fragment_usdc"]) if fragment_count > 0 else 0.0
        else:
            fragment_count = fragment_count if fragment_trade else 1
            fragment_usdc = budget_total / max(1, fragment_count)

        # PATCH: comptage local des trades fragmentés
        if fragment_count > 1:
            self.fragmented_trade_count += 1

        # Paramètres TM
        maker_fill_ratio = float(params.get("maker_fill_ratio", getattr(self.config, "maker_fill_ratio", 0.5)))
        maker_skew_bps   = float(params.get("maker_skew_bps",   getattr(self.config, "maker_skew_bps",   1.0)))

        # Fees par jambe (fallback sur fee map du simu, sinon fees_total_pct/2, sinon 0)
        fees_buy_pct = float(opportunity.get("fees_buy_pct", params.get("fees_buy_pct", None)) or 0.0)
        fees_sell_pct = float(opportunity.get("fees_sell_pct", params.get("fees_sell_pct", None)) or 0.0)

        if fees_buy_pct == 0.0 and fees_sell_pct == 0.0:
            if strategy.upper() == "TT":
                fees_buy_pct = self._fee_default(buy_ex, "taker")
                fees_sell_pct = self._fee_default(sell_ex, "taker")
            else:  # TM: taker côté BUY, maker côté SELL (selon design)
                fees_buy_pct = self._fee_default(buy_ex, "taker")
                fees_sell_pct = self._fee_default(sell_ex, "maker")

        if fees_total_pct is not None and (fees_buy_pct == 0.0 and fees_sell_pct == 0.0):
            fees_buy_pct = float(fees_total_pct) * 0.5
            fees_sell_pct = float(fees_total_pct) * 0.5

        # Mesure latence (simulateur)
        # Mesure latence (simulateur) + exécution
        _t0 = time.perf_counter()
        result = None
        deviation = 0.0
        try:
            result, deviation = await asyncio.wait_for(
                self._simulate_core(
                    opportunity, buy_levels, sell_levels,
                    capital_available_usdc=budget_total,
                    strategy=strategy.upper(),
                    fees_buy_pct=fees_buy_pct, fees_sell_pct=fees_sell_pct,
                    vwap_guard_bps=vwap_guard_bps,
                    rebalancing_mode=rebalancing_mode,
                    fragment_count=fragment_count,
                    fragment_usdc=fragment_usdc,
                    auto_fragment=auto_fragment,
                    maker_fill_ratio=maker_fill_ratio,
                    maker_skew_bps=maker_skew_bps,
                    vol=vol, params=params, combo_key=combo_key,
                ),
                timeout=timeout
            )
        except asyncio.TimeoutError as e:
            report_nonfatal("DynamicExecutionSimulator", "simulate_timeout", e, phase="simulate")
            logger.error("[Sim] ⏱️ Timeout (%ss)", timeout)
            raise SimulationError("TIMEOUT")  # pas de retour “None” silencieux



        finally:
            try:
                self.last_latency_ms = (time.perf_counter() - _t0) * 1000.0
            except Exception:
                self.last_latency_ms = 0.0

        # Prometheus: latence de décision (ms)
        try:
            SIM_DECISION_MS.observe(self.last_latency_ms)
        except Exception:
            pass

        # Prometheus: écart VWAP en bps si résultat dispo
        if result is not None:
            try:
                vbuy = float(result.get("vwap_buy", 0.0) or 0.0)
                vsell = float(result.get("vwap_sell", 0.0) or 0.0)
                bps = (abs(vsell - vbuy) / max(vbuy, 1e-12)) * 1e4
                SIMULATED_VWAP_DEVIATION_BPS.observe(bps)
            except Exception:
                pass

        # Mise à jour atomique des métriques & event sink
        async with self._lock:
            if result is not None:
                n_prev = self.simulation_count
                self.avg_vwap_deviation = (self.avg_vwap_deviation * n_prev + deviation) / (n_prev + 1)
                self.simulation_count += 1
                self.last_simulation_time = datetime.utcnow().timestamp()
                self.last_result = result
                if self._event_sink:
                    try:
                        self._event_sink({
                            "module": "DynamicExecutionSimulator",
                            "type": "simulation_result",
                            "trade_id": result["trade_id"],
                            "combo_key": result["combo_key"],
                            "strategy": result["strategy"],
                            "pair": result["pair"],
                            "payload": result,
                        })
                    except Exception as e:
                        report_nonfatal("DynamicExecutionSimulator", "event_sink_error", e, phase="simulate")
                        logger.exception("[DynamicExecutionSimulator] event_sink callback error")

        return result

    # ------------------------- TT et TM en parallèle -------------------------
    # === dynamic_execution_simulator.py ===
    # Dans class DynamicExecutionSimulator


    async def simulate_both_parallel(self, payload: dict) -> dict:
        """
        P1: Exécution parallèle TT & TM simulées, avec:
          - Limiteur de concurrence (semaphore) pour éviter backlog,
          - Timeouts indépendants par bras et annulation croisée,
          - EU_ONLY/SPLIT: pénalités latence appliquées,
          - Sortie normalisée: {ok, reason, sim_vwap_dev_bps, fills_expected_ratio, sim_latency_ms, guards}.
        """
        # --- 0) Limiteur de concurrence global -----------------------------------
        if not hasattr(self, "_sem"):
            max_conc = int(getattr(self, "max_concurrency", 64))
            self._sem = asyncio.Semaphore(max(1, max_conc))

        async with self._sem:
            t0 = time.time()
            timeout_each = float(getattr(self, "timeout_each_s", 1.2))
            split_mode = getattr(self, "split_mode", "EU_ONLY")
            cb_bonus = 0.0
            if split_mode == "SPLIT":
                cb_bonus = float(getattr(self, "cb_coalescing_ms", 10.0)) / 1000.0

            # --- 1) Définition des jobs TT/TM ------------------------------------
            async def _sim_tt():
                # Calcule un VWAP dev “taker/taker” sensible à la profondeur
                return await self._simulate_tt(payload, extra_latency_s=cb_bonus)

            async def _sim_tm():
                # Calcule un score maker/taker + probabilité de fill/hedge
                return await self._simulate_tm(payload, extra_latency_s=cb_bonus)

            # --- 2) Run en parallèle + annulation croisée -------------------------
            tt = asyncio.create_task(_sim_tt())
            tm = asyncio.create_task(_sim_tm())
            done, pending = await asyncio.wait({tt, tm}, timeout=timeout_each, return_when=asyncio.ALL_COMPLETED)

            # Annulation si timeout partiel
            if pending:
                for p in pending:
                    p.cancel()

            # --- 3) Agrégation des résultats -------------------------------------
            # --- 3) Agrégation des résultats — STRICT, pas de fallback muet ---
            def _extract(task, label: str):
                # task annulée (timeout partiel) => TIMEOUT explicite
                if task.cancelled():
                    return {
                        "ok": False, "reason": f"{label}_TIMEOUT",
                        "dev_bps": float("inf"), "fills_ratio": 0.0,
                        "lat_ms": timeout_each * 1000.0, "guards": {}
                    }
                try:
                    r = task.result()
                    # Certaines branches renvoient None (combo non autorisé, depth vide, budget=0)
                    if not r:
                        return {
                            "ok": False, "reason": f"{label}_NONE_RESULT",
                            "dev_bps": float("inf"), "fills_ratio": 0.0,
                            "lat_ms": timeout_each * 1000.0, "guards": {}
                        }
                    # Normalisation champs attendus
                    r.setdefault("ok", False)
                    r.setdefault("dev_bps", float("inf"))
                    r.setdefault("fills_ratio", 0.0)
                    r.setdefault("lat_ms", timeout_each * 1000.0)
                    r.setdefault("guards", {})
                    r.setdefault("reason", None)
                    return r
                except SimulationError as e:
                    return {
                        "ok": False, "reason": f"{label}_SIM_ERROR:{e}",
                        "dev_bps": float("inf"), "fills_ratio": 0.0,
                        "lat_ms": timeout_each * 1000.0, "guards": {}
                    }
                except Exception as e:
                    # Pas de “pass” : on expose la classe d’erreur
                    return {
                        "ok": False, "reason": f"{label}_TASK_ERROR:{e.__class__.__name__}",
                        "dev_bps": float("inf"), "fills_ratio": 0.0,
                        "lat_ms": timeout_each * 1000.0, "guards": {}
                    }

            tt_r = _extract(tt, "TT")
            tm_r = _extract(tm, "TM")

            ok_tt = bool(tt_r.get("ok", False))
            ok_tm = bool(tm_r.get("ok", False))

            # Sélection conservatrice : si au moins une branche OK, on prend celle au dev_bps minimal.
            if ok_tt or ok_tm:
                if ok_tt and (not ok_tm or float(tt_r.get("dev_bps", float("inf"))) <= float(
                        tm_r.get("dev_bps", float("inf")))):
                    chosen = tt_r
                else:
                    chosen = tm_r
                dev_bps = float(chosen.get("dev_bps", float("inf")))
                fills_ratio = float(chosen.get("fills_ratio", 0.0))
                reason = None  # succès explicite
            else:
                # Double échec → reason concaténée, jamais “SIM_TIMEOUT” générique
                dev_bps = float("inf")
                fills_ratio = 0.0
                reason = f"TT:{tt_r.get('reason')}|TM:{tm_r.get('reason')}"

            sim_lat_ms = int((time.time() - t0 + cb_bonus) * 1000.0)

            return {
                "ok": math.isfinite(dev_bps),
                "reason": reason,
                "sim_vwap_dev_bps": float(dev_bps if math.isfinite(dev_bps) else 1e6),
                "fills_expected_ratio": float(fills_ratio),
                "sim_latency_ms": sim_lat_ms,
                "guards": {
                    "tt_ok": ok_tt,
                    "tm_ok": ok_tm,
                    "tt_reason": tt_r.get("reason"),
                    "tm_reason": tm_r.get("reason"),
                    "split_mode": split_mode
                }
            }

    # ------------------------- Helpers Maker -------------------------
    def quote_for_maker(self, side: str, levels: List[Level], *, depth_clip: int = 5, skew_bps: float = 1.0) -> Optional[float]:
        """Prix maker conservateur basé sur le meilleur niveau ± skew (bps)."""
        lv = self._sanitize_levels(levels)[: max(1, depth_clip)]
        if not lv:
            return None
        if side.lower() == "buy":
            best_bid = max(p for p, _ in lv)
            return best_bid * (1 - abs(skew_bps) / 1e4)
        else:
            best_ask = min(p for p, _ in lv)
            return best_ask * (1 + abs(skew_bps) / 1e4)

    @staticmethod
    def plan_maker_ladder(
        side: str,
        *,
        best_bid: float,
        best_ask: float,
        tick: float,
        slices: int,
        pad_ticks: int = 0,
    ) -> List[float]:
        """Ladder de prix maker (post-only).
        SELL: >= best_ask + k*tick ; BUY: <= best_bid - k*tick
        """
        slices = max(1, int(slices))
        tick = max(0.0, float(tick or 0.0))
        pad = max(0, int(pad_ticks or 0))
        prices: List[float] = []
        if side.lower() == "sell":
            base = best_ask
            for i in range(slices):
                k = i + pad
                prices.append(max(0.0, base + k * tick))
        else:
            base = best_bid
            for i in range(slices):
                k = i + pad
                prices.append(max(0.0, base - k * tick))
        return prices

    # --------------------------- status API ---------------------------
    def get_last_result(self) -> Optional[Dict[str, Any]]:
        return self.last_result

    def get_status(self) -> Dict[str, Any]:
        return {
            "module": "DynamicExecutionSimulator",
            "healthy": True,
            "last_update": self.last_simulation_time,
            "details": (
                f"Dernière simu à {self.last_simulation_time}, "
                f"volume simulé : {self.last_result.get('executed_volume_usdc', 0) if self.last_result else 0}"
            ),
            "metrics": {
                "simulations_run": self.simulation_count,
                "avg_vwap_deviation": round(self.avg_vwap_deviation, 8),
                "blocked_trades": self.blocked_trade_count,
                "fragmented_trades": self.fragmented_trade_count,
                "fee_map_exchanges": len(self._fee_map_pct or {}),
                "last_latency_ms": round(self.last_latency_ms, 2),
            },
            "last_restart_reason": self.last_restart_reason,
            "submodules": {},
        }

    async def restart(self, reason: str = "inconnu"):
        logger.warning(f"[DynamicExecutionSimulator] 🔁 Redémarrage demandé : {reason}")
        self.last_restart_reason = reason
        self.simulation_count = 0
        self.avg_vwap_deviation = 0.0
        self.blocked_trade_count = 0
        self.fragmented_trade_count = 0
        self.last_result = None
        self.last_latency_ms = 0.0
