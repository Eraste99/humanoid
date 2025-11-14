# modules/rebalancing_manager.py
from __future__ import annotations
"""
Rebalancing manager — Hints-only, passive, industry-like
========================================================

• Ce sous-module est **encapsulé** et **passif** : aucune I/O, aucune boucle interne.
• Il expose les **mêmes noms** de méthodes que l’ancienne version pour compat :
  - update_balances / update_wallets / ingest_snapshot / detect_imbalance / build_plan / plan_to_operations / get_status / start / stop …
• Il ne fait que **détecter** et **proposer des hints** (wallet transfers, internal transfers,
  petites poches crypto, overlay compensation). Le **RM** décide et exécute.

Changements clés vs ancien :
- Suppression de l’orchestration asynchrone et du bridge/cross-CEX « actif ».
- Ajout d’anti-thrash : quantum minimal, budget/minute, priorités strictes (pilotées par cfg).
- Valorisation simple via top-of-book ingérés (le RM pousse les snapshots OB/prix).
"""

import time
from collections import defaultdict, deque
from typing import Dict, Any, Optional, List, Callable, Tuple, Iterable


# Observabilité — sous-module PASSIF (no Prometheus ici)
from typing import Callable, Optional, Dict, Any
import logging, time

log = logging.getLogger("rebalancing_manager")

# Hook générique (le RM peut injecter un callback pour recevoir des événements)
EventSink = Callable[[str, Dict[str, Any]], None]

def _now() -> float:  # utilitaire local (si besoin)
    return time.time()

# Placeholders no-op (compat avec les appels existants .labels/.observe/.inc/.set)
class _NoMetric:
    def labels(self, *a, **k): return self
    def observe(self, *a, **k): return
    def inc(self, *a, **k): return
    def set(self, *a, **k): return

# Noms attendus par le code existant — tous no-op ici
REBAL_PLAN_BUILD_MS    = _NoMetric()
REBAL_OPERATIONS_TOTAL = _NoMetric()
REBAL_SNAPSHOTS_AGE_S  = _NoMetric()

# ---------------------------------------------------------------------
# Helpers
def _to_f(x) -> float:
    try: return float(x)
    except Exception: return 0.0

def _now() -> float: return time.time()

def _norm(s: str) -> str: return (s or "").strip().upper()

import json

def _norm_list(val, default):
    """Accepte None / 'None' / '' / CSV / JSON list / list-like → liste Python."""
    if val in (None, "", "None"):
        return list(default)
    if isinstance(val, str):
        s = val.strip()
        if not s or s.lower() == "none":
            return list(default)
        if s.startswith("["):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return parsed
            except Exception:
                return list(default)
        # CSV
        return [t.strip() for t in s.split(",") if t.strip()]
    if isinstance(val, (list, tuple, set)):
        return list(val)
    return list(default)

def _norm_list_upper(val, default):
    return [str(x).upper() for x in _norm_list(val, default)]

def _norm_pairs(val, default):
    """Normalise les paires: supprime '-', uppercases, filtre vide."""
    raw = _norm_list(val, default)
    out = []
    for p in raw:
        try:
            s = str(p).replace("-", "").upper()
            if s:
                out.append(s)
        except Exception:
            continue
    return out


# =====================================================================
#                         REBALANCING MANAGER
# =====================================================================
class RebalancingManager:
    """
    Sous-module **passif** :
    - Le RM pousse : balances, wallets, orderbooks (via ingest_snapshot), overlay, politiques.
    - Le module retourne : un **plan de hints** priorisés, avec anti-thrash.
    """

    # ----------- Construction / configuration -----------
    def __init__(
        self,
        *,
        rm,  # RiskManager parent (gardé pour compat ; non utilisé pour I/O)
        # --- Devises / seuils
        quote_currencies: Optional[Iterable[str]] = None,       # ["USDC","EUR"]
        min_cash_per_quote: Optional[Dict[str, float]] = None,  # {"USDC":1000.0,"EUR":1000.0}
        min_crypto_value_usdc: float = 1000.0,

        # --- Périmètre
        enabled_exchanges: Optional[Iterable[str]] = None,      # ["BINANCE","BYBIT","COINBASE"]
        enabled_assets: Optional[Iterable[str]] = None,         # universe crypto suivi
        enabled_aliases: Optional[Iterable[str]] = None,        # ["TT","TM"]

        # --- Stratégie (legacy conservés pour compat)
        history_limit: int = 200,
        target_diff_quote: float = 200.0,
        internal_transfer_threshold: float = 250.0,
        overlay_comp_threshold: float = 100.0,
        cross_cex_haircut: float = 0.80,
        preferred_pairs: Optional[List[str]] = None,

        # --- Wallets (compat)
        wallet_types: Optional[List[str]] = None,               # ["SPOT","FUNDING"]
        preferred_wallet: str = "SPOT",
        min_cash_per_wallet: Optional[Dict[str, Dict[str, float]]] = None,
        virtual_wallets: bool = True,
        virtual_parking_wallet_name: str = "PARK",

        # --- Orchestration legacy (désormais inactifs, gardés pour compat)
        rebal_check_interval_s: float = None,
        edge_min_bps: float = None,
        route_switch_margin_bps: float = None,
        route_persistence_s: float = None,
        plankey_cooldown_s: float = None,
        bridge_pivots: Optional[List[str]] = None,
        max_staleness_ms: int = None,
        bridge_max_hops: int = 2,
        bridge_max_cost_bps: float = 6.0,
    ) -> None:
        # Parent / cfg (source de vérité), mais ce module reste passif.
        self.rm = rm
        self.cfg = getattr(rm, "config", None) or getattr(rm, "cfg", None)
        g = getattr(self.cfg, "g", None)


        # Devises / seuils
        self.quote_currencies = _norm_list_upper(
            quote_currencies if quote_currencies is not None else getattr(self.cfg, "quote_currencies", None),
            ["USDC", "EUR"]
        )

        self.min_cash_per_quote = {k.upper(): float(v) for k, v in (min_cash_per_quote or {"USDC": 1000.0, "EUR": 1000.0}).items()}
        self.min_crypto_value_usdc = float(min_crypto_value_usdc)

        # Périmètre (avec fallbacks cfg)
        # --- enabled_exchanges: tolérant None / str CSV / JSON / liste ---
        self.enabled_exchanges = _norm_list_upper(
            enabled_exchanges if enabled_exchanges is not None else (
                    getattr(self.cfg, "enabled_exchanges", None) or (getattr(g, "enabled_exchanges", None))
            ),
            ["BINANCE", "BYBIT", "COINBASE"]
        )
        self.enabled_exchanges = _norm_list_upper(
            enabled_exchanges if enabled_exchanges is not None else (
                    getattr(self.cfg, "enabled_exchanges", None) or (getattr(g, "enabled_exchanges", None))
            ),
            ["BINANCE", "BYBIT", "COINBASE"]
        )

        self.enabled_assets    = [a.upper() for a in (enabled_assets or [
            "ETH","BTC","SOL","ADA","XRP","DOGE","DOT","AVAX","AXS","LTC","SHIB","UNI","LINK",
            "ATOM","XLM","ALGO","FIL","LDO","APE","BNB"
        ])]

        # Stratégie (legacy conservés)
        self.history_limit = int(history_limit)
        self.target_diff_quote = float(target_diff_quote)
        self.internal_transfer_threshold = float(internal_transfer_threshold)
        self.overlay_comp_threshold = float(overlay_comp_threshold)
        self.cross_cex_haircut = float(cross_cex_haircut)
        self.preferred_pairs = _norm_list_upper(
            preferred_pairs if preferred_pairs is not None else getattr(self.cfg, "preferred_pairs", None),
            ["ETHUSDC", "BTCUSDC", "ETHEUR", "BTCEUR"]
        )

        # Wallets (compat)
        self.wallet_types = [w.upper() for w in (wallet_types or ["SPOT", "FUNDING"])]
        self.preferred_wallet = str(preferred_wallet or "SPOT").upper()
        base_min_wallet = {w: {q: 0.0 for q in self.quote_currencies} for w in self.wallet_types}
        base_min_wallet[self.preferred_wallet] = {q: self.min_cash_per_quote.get(q, 0.0) for q in self.quote_currencies}
        self.min_cash_per_wallet: Dict[str, Dict[str, float]] = {
            w.upper(): {q.upper(): float(v) for q, v in vals.items()}
            for w, vals in ((min_cash_per_wallet or base_min_wallet).items())
        }
        self.virtual_wallets = bool(virtual_wallets)
        self.virtual_parking_wallet_name = str(virtual_parking_wallet_name).upper()

        # Politique anti-thrash (lue depuis cfg si dispo)
        self.rebal_quantum_min_quote: float = float(getattr(self.cfg, "rebal_quantum_min_quote", 50.0))
        self.rebal_max_ops_per_min: int = int(getattr(self.cfg, "rebal_max_ops_per_min", 6))
        self.rebal_priority: List[str] = list(getattr(self.cfg, "rebal_priority", ["CASH","CRYPTO","OVERLAY"]))
        self.rebal_hint_ttl_s: int = int(getattr(self.cfg, "rebal_hint_ttl_s", 120))

        # Snapshots ingérés par le RM
        # OB: latest_orderbooks[EX][SYMBOL] = {"bid":..., "ask":..., "ts": ...}
        self.latest_orderbooks: Dict[str, Dict[str, Dict[str, float]]] = {}
        # balances: latest_balances[EX][ALIAS] = {"USDC":..., "EUR":..., "ETH":...}
        self.latest_balances:   Dict[str, Dict[str, Dict[str, float]]] = {}
        # wallets: latest_wallets[EX][ALIAS][WALLET] = {"USDC":..., "EUR":...}
        self.latest_wallets:    Dict[str, Dict[str, Dict[str, Dict[str, float]]]] = {}

        # Overlay (virtuel) par devise — clé: "ALIAS:EX_A->EX_B|CCY" → montant
        self._overlay_flow: Dict[str, float] = defaultdict(float)

        # Historique (debug) & rate-limit
        self.history: deque = deque(maxlen=256)
        self._emit_ts = deque(maxlen=512)

        # Event sink (compat)
        self._event_sink: Optional[Callable[[Dict[str, Any]], Any]] = None

        # Dernière mise à jour (utile pour l’âge des snapshots)
        self.last_update: float = 0.0

    # -------------------------- Event sink (compat) ---------------------------
    def set_event_sink(self, sink: Optional[Callable[[Dict[str, Any]], Any]]) -> None:
        self._event_sink = sink

    def _emit(self, level: str, message: str, **kw) -> None:
        if not self._event_sink:
            return
        try:
            evt = {"module": "RebalancingManager", "level": level, "message": message}
            if kw: evt.update(kw)
            self._event_sink(evt)
        except Exception:
            pass

    # ------------------------------- Ingestion --------------------------------
    def ingest_snapshot(self, data: Dict[str, Any]) -> None:
        """data: {exchange, pair_key|symbol, best_bid, best_ask, active=True}."""
        try:
            ex = _norm(data.get("exchange", ""))
            if ex not in self.enabled_exchanges: return
            sym = _norm(data.get("pair_key") or data.get("symbol") or "")
            if not sym: return
            bid = _to_f(data.get("best_bid"))
            ask = _to_f(data.get("best_ask"))
            if bid <= 0 or ask <= 0 or ask <= bid:  # garde-fous
                return
            self.latest_orderbooks.setdefault(ex, {})[sym] = {"bid": bid, "ask": ask, "ts": _now()}
            self.last_update = _now()
        except Exception:
            pass

    def update_balances(self, balances: Dict[str, Dict[str, Dict[str, float]]]) -> None:
        """
        balances: {"BINANCE":{"TT":{"USDC":..., "ETH":...}, "TM":{...}}, "BYBIT":{...}}
        """
        try:
            norm: Dict[str, Dict[str, Dict[str, float]]] = {}
            for ex, per_alias in (balances or {}).items():
                exu = _norm(ex)
                if exu not in self.enabled_exchanges:
                    continue
                norm[exu] = {}
                for alias, assets in (per_alias or {}).items():
                    al = _norm(alias)
                    if self.enabled_aliases and al not in self.enabled_aliases:
                        continue
                    norm[exu][al] = {str(a).upper(): _to_f(q) for a, q in (assets or {}).items()}
            self.latest_balances = norm
            self.last_update = _now()
        except Exception:
            pass

    def update_wallets(self, wallets: Dict[str, Dict[str, Dict[str, Dict[str, float]]]]) -> None:
        """
        wallets: {"BINANCE":{"TT":{"SPOT":{"USDC":...},"FUNDING":{...}},"TM":{...}}, ...}
        """
        try:
            norm: Dict[str, Dict[str, Dict[str, Dict[str, float]]]] = {}
            for ex, per_alias in (wallets or {}).items():
                exu = _norm(ex)
                if exu not in self.enabled_exchanges:
                    continue
                norm[exu] = {}
                for alias, per_wallet in (per_alias or {}).items():
                    al = _norm(alias)
                    if self.enabled_aliases and al not in self.enabled_aliases:
                        continue
                    norm[exu][al] = {}
                    for wallet, ccy_map in (per_wallet or {}).items():
                        w = _norm(wallet)
                        if w not in self.wallet_types:
                            continue
                        norm[exu][al][w] = {str(c).upper(): _to_f(v) for c, v in (ccy_map or {}).items()}
            self.latest_wallets = norm
            self.last_update = _now()
        except Exception:
            pass

    # ------------------------------ Overlay (compat) --------------------------
    @staticmethod
    def _dir_key(self, alias: str, ex_from: str, ex_to: str, ccy: str) -> str:
        _norm = lambda x: str(x or "").upper()
        return f"{_norm(alias)}:{_norm(ex_from)}->{_norm(ex_to)}|{_norm(ccy)}"

    @staticmethod
    def _sym_label(alias: str, ex_a: str, ex_b: str, ccy: str) -> str:
        return f"{_norm(alias)}:{_norm(ex_a)}->{_norm(ex_b)}|{_norm(ccy)}"

    def overlay_add_flow(self, alias: str, ex_from: str, ex_to: str, amount: float) -> None:
        """Ajoute un flux d’overlay simulé (en devise `ccy`), utile pour flécher les compensations."""
        key = self._dir_key(alias, ex_from, ex_to, self.quote_currencies[0])
        self._overlay_flow[key] += _to_f(amount)

    def overlay_snapshot(self) -> Dict[str, float]:
        """Retourne l’overlay agrégé (read-only)."""
        return dict(self._overlay_flow)

    def _overlay_compensations(self) -> List[Dict[str, Any]]:
        """Transforme l’overlay en hints directionnels."""
        hints: List[Dict[str, Any]] = []
        for k, v in (self._overlay_flow or {}).items():
            if abs(v) < self.overlay_comp_threshold:
                continue
            alias, rest = k.split(":", 1)
            path, ccy = rest.split("|", 1)
            ex_a, ex_b = path.split("->", 1)
            hints.append({
                "type": "overlay_comp_hint",
                "alias": _norm(alias),
                "from": {"exchange": _norm(ex_a)},
                "to":   {"exchange": _norm(ex_b)},
                "asset": _norm(ccy),
                "exposure_quote": round(_to_f(v), 2),
                "valuation_quote": _norm(ccy),
                "suggested_action": "decrease_long" if v > 0 else "decrease_short",
                "ttl_s": self.rebal_hint_ttl_s,
                "created_ts": _now(),
                "priority": "OVERLAY",
                "reason": "overlay_comp",
            })
        return hints

    # ----------------------------- Prix & valeurs -----------------------------
    def _rm_top_of_book(self, ex: str, symbol: str) -> Tuple[float, float]:
        """Retourne (bid, ask) du dernier OB frais si dispo."""
        ob = (self.latest_orderbooks.get(_norm(ex), {}) or {}).get(_norm(symbol), {})
        return _to_f(ob.get("bid")), _to_f(ob.get("ask"))

    def _mid(self, ex: str, base: str, quote: str) -> float:
        sym = f"{_norm(base)}{_norm(quote)}"
        b, a = self._rm_top_of_book(ex, sym)
        if b > 0 and a > 0:
            return 0.5 * (b + a)
        return 0.0

    def _mid_any(self, ex: str, base: str, quote: str) -> float:
        """Mid price sur n’importe quel symbol BQ ou QB (via 1/px)."""
        m = self._mid(ex, base, quote)
        if m > 0: return m
        m = self._mid(ex, quote, base)
        if m > 0: return 1.0 / m
        return 0.0

    def _value_in_quote(self, exchange: str, asset: str, qty: float, quote: str = "USDC") -> float:
        a, q = _norm(asset), _norm(quote)
        if a == q:
            return float(qty)
        px = self._mid_any(exchange, a, q)
        return float(qty * max(0.0, px))

    # ---------------------- Détection & planning (hints) ----------------------
    def _aggregate_quote_by_exchange(self, quote: str) -> Dict[str, float]:
        out: Dict[str, float] = {}
        q = _norm(quote)
        for ex, accounts in (self.latest_balances or {}).items():
            total = 0.0
            for _, assets in (accounts or {}).items():
                total += _to_f((assets or {}).get(q, 0.0))
            out[ex] = total
        return out

    def _choose_pair_for_cross(self, ex_from: str, ex_to: str, quote: str) -> Optional[str]:
        """Compat: renvoie un symbol commun se terminant par `quote` si nécessaire (non utilisé ici)."""
        books_from = set((self.latest_orderbooks.get(_norm(ex_from), {}) or {}).keys())
        books_to   = set((self.latest_orderbooks.get(_norm(ex_to),   {}) or {}).keys())
        common = {p for p in (books_from & books_to) if p.endswith(_norm(quote))}
        if not common:
            return None
        for p in self.preferred_pairs:
            if p in common:
                return p
        return next(iter(common), None)

    def _best_alias_for(self, ex: str, quote: str) -> str:
        accounts = (self.latest_balances.get(_norm(ex), {}) or {})
        if not accounts:
            return "TT"
        best_alias, best_val = "TT", -1.0
        for al, assets in accounts.items():
            v = _to_f((assets or {}).get(_norm(quote), 0.0))
            if v > best_val:
                best_alias, best_val = al, v
        return best_alias

    def detect_imbalance(self) -> Optional[Dict[str, Any]]:
        """Retourne un snapshot d'imbalance ou None si RAS (hors overlay)."""
        if not self.latest_balances:
            return None

        out = {
            "CASH": {q: {} for q in self.quote_currencies},
            "CRYPTO": {},
            "OVERLAY": self.overlay_snapshot(),
        }

        # 1) CASH par CEX/alias
        for ex, accounts in self.latest_balances.items():
            for q in self.quote_currencies:
                needed: Dict[str, float] = {}
                target_min = self.min_cash_per_quote.get(q, 0.0)
                if target_min <= 0.0:
                    continue
                for al, assets in (accounts or {}).items():
                    have = _to_f((assets or {}).get(q, 0.0))
                    if have < target_min:
                        needed[al] = round(target_min - have, 2)
                if needed:
                    out["CASH"][q].setdefault(ex, {}).update(needed)

        # 2) petites poches CRYPTO (valorisées en USDC)
        for ex, accounts in self.latest_balances.items():
            for alias, assets in (accounts or {}).items():
                for asset, qty in (assets or {}).items():
                    a = _norm(asset)
                    if a in self.quote_currencies:
                        continue
                    if a not in self.enabled_assets:
                        continue
                    val = self._value_in_quote(ex, a, _to_f(qty), "USDC")
                    if 0 < val < self.min_crypto_value_usdc:
                        out["CRYPTO"].setdefault(ex, {}).setdefault(alias, {})[a] = round(val, 2)

        return out

    # --- 1) Intra-wallet (SPOT/FUNDING vers preferred_wallet) ---
    def _plan_intra_wallet_transfers(self) -> List[Dict[str, Any]]:
        plans: List[Dict[str, Any]] = []
        target_wallet = self.preferred_wallet
        for ex, per_alias in (self.latest_wallets or {}).items():
            for alias, per_wallet in (per_alias or {}).items():
                for q, target_min in (self.min_cash_per_wallet.get(target_wallet, {}) or {}).items():
                    have = _to_f(((per_wallet.get(target_wallet) or {}).get(q)))
                    if have >= target_min:
                        continue
                    remaining = round(target_min - have, 2)
                    # draine depuis autres wallets
                    for w, ccy_map in (per_wallet or {}).items():
                        if w == target_wallet:
                            continue
                        have = _to_f((ccy_map or {}).get(q, 0.0))
                        if have <= 0:
                            continue
                        amt = round(min(remaining, have), 2)
                        if amt <= 0:
                            continue
                        plans.append({
                            "exchange": ex, "alias": alias,
                            "from_wallet": w, "to_wallet": target_wallet,
                            "amount": amt, "ccy": q,
                            "type": "transfer", "scope": "intra_cex",
                            "priority": "CASH", "ttl_s": self.rebal_hint_ttl_s,
                            "created_ts": _now(), "reason": "cash_rebalance_wallet",
                        })
                        remaining = round(remaining - amt, 2)
                        if remaining <= 0:
                            break
        return plans

    # --- 2) Intra-CEX (TT↔TM) ---
    def _plan_internal_transfers(self) -> List[Dict[str, Any]]:
        plans: List[Dict[str, Any]] = []
        for ex, accounts in (self.latest_balances or {}).items():
            for q in self.quote_currencies:
                holdings = {al: _to_f((assets or {}).get(q, 0.0)) for al, assets in (accounts or {}).items()}
                if not holdings:
                    continue
                target_min = self.min_cash_per_quote.get(q, 0.0)
                weakest = [al for al, v in sorted(holdings.items(), key=lambda kv: kv[1]) if v < target_min]
                richest = [al for al, v in sorted(holdings.items(), key=lambda kv: kv[1], reverse=True) if v > target_min]
                for wa in weakest:
                    for ra in richest:
                        if wa == ra:
                            continue
                        diff = holdings[ra] - holdings[wa]
                        need = target_min - holdings[wa]
                        move = round(max(0.0, min(diff, need)), 2)
                        if move <= 0:
                            continue
                        if move < max(self.internal_transfer_threshold, self.rebal_quantum_min_quote):
                            continue
                        plans.append({
                            "exchange": ex,
                            "from": {"alias": ra}, "to": {"alias": wa},
                            "amount": move, "ccy": q,
                            "type": "transfer", "scope": "intra_cex",
                            "priority": "CASH", "ttl_s": self.rebal_hint_ttl_s,
                            "created_ts": _now(), "reason": "cash_rebalance_alias",
                        })
                        holdings[ra] -= move
                        holdings[wa] += move
                        if holdings[wa] >= target_min:
                            break
        return plans

    # --- 3) Cross-CEX (stub compat ; renvoie None) ---
    def _plan_cross_cex(self) -> Optional[Dict[str, Any]]:
        return None  # hints-only ; le RM décidera d’un bridge ou d’un trade cross

    def _plan_pre_bridge(self, cross: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        return None  # idem

    # ----------------------------- Build plan ---------------------------------
    def build_plan_raw(self, imbalance: Dict[str, Any], constraints: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        t0 = time.perf_counter_ns()

        # Observabilité : âges
        ages = self._freshness_ages()
        try:
            for k, v in ages.items(): REBAL_SNAPSHOTS_AGE_S.labels(k).set(float(v))
        except Exception:
            pass

        # 0) overlay netting
        overlay_comp = self._overlay_compensations()
        # 1) intra-wallet
        wallet_transfers = self._plan_intra_wallet_transfers()
        # 2) intra-CEX
        internal = self._plan_internal_transfers()

        # 3) petites poches crypto (hints)
        crypto_imb = (imbalance or {}).get("CRYPTO") or {}
        topups: List[Dict[str, Any]] = []
        for ex, per_alias in (crypto_imb.items()):
            for alias, assets in (per_alias or {}).items():
                for asset, val_usdc in (assets or {}).items():
                    pk_usdc = f"{_norm(asset)}USDC"
                    bid, ask = self._rm_top_of_book(ex, pk_usdc)
                    price = ask or bid
                    if price > 0:
                        deficit = max(self.min_crypto_value_usdc - float(val_usdc), 0.0)
                        qty = round(deficit / price, 6) if deficit > 0 else 0.0
                        if qty > 0:
                            topups.append({
                                "type": "crypto_topup_hint",
                                "exchange": _norm(ex), "alias": _norm(alias),
                                "asset": _norm(asset), "qty": qty,
                                "valuation_quote": "USDC",
                                "value_quote": round(float(val_usdc), 2),
                                "target_value_quote": self.min_crypto_value_usdc,
                                "priority": "CRYPTO", "ttl_s": self.rebal_hint_ttl_s,
                                "created_ts": _now(), "reason": "small_pocket",
                            })

        # 4) cross-CEX (priorisé par la devise la + déséquilibrée) — désactivé en hints-only
        cross = self._plan_cross_cex()
        # 4.bis) pré-bridge éventuel
        bridge_pre = self._plan_pre_bridge(cross)

        # ----------------- Anti-thrash: quantum & rate-limit ------------------
        def _apply_quantum(ops: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            out = []
            qmin = float(self.rebal_quantum_min_quote)
            for op in (ops or []):
                amt = _to_f(op.get("amount", 0.0))
                if amt and amt < qmin:
                    continue
                out.append(op)
            return out

        wallet_transfers = _apply_quantum(wallet_transfers)
        internal = _apply_quantum(internal)
        # topups/overlay_comp sont des hints (pas d'amount strict) → laissés tels quels

        # --- Priorités + rate limit/minute
        buckets = {
            "CASH": (wallet_transfers + internal + ([bridge_pre] if cross else [])),
            "CRYPTO": topups,
            "OVERLAY": overlay_comp,
        }
        ordered: List[Dict[str, Any]] = []
        for prio in (self.rebal_priority or ["CASH","CRYPTO","OVERLAY"]):
            ordered.extend([op for op in buckets.get(prio, []) if op])

        # rate limit glissant 60s
        now = _now()
        self._emit_ts = type(self._emit_ts)([t for t in self._emit_ts if (now - t) < 60.0], maxlen=self._emit_ts.maxlen)
        budget = max(0, int(self.rebal_max_ops_per_min) - len(self._emit_ts))
        selected = ordered[:budget] if budget < len(ordered) else list(ordered)
        self._emit_ts.extend([now] * len(selected))

        # Répartition (compat callbacks)
        def _take(src: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            ids = set(map(id, selected))
            return [op for op in src if id(op) in ids]

        sel_overlay  = _take(overlay_comp)
        sel_wallet   = _take(wallet_transfers)
        sel_internal = _take(internal)
        sel_crypto   = _take(topups)
        sel_bridge   = bridge_pre if (bridge_pre and id(bridge_pre) in set(map(id, selected))) else None

        plan = {
            "overlay_comp": sel_overlay,
            "wallet_transfers": sel_wallet,
            "internal_transfers": sel_internal,
            "crypto_topups": sel_crypto,
            "rebalancing_trade": None,   # compat (désactivé)
            "bridge_pre": sel_bridge,    # compat (rarement non-None ici)
            "t_ms": (time.perf_counter_ns() - t0) / 1e6,
            "snapshots_age_s": ages,
            "cost_fn": self._reb_cost_fn,  # <-- fourni par le RM
        }

        # Observabilité (compteurs)
        try:
            REBAL_OPERATIONS_TOTAL.labels(op="overlay_comp_hint").inc(len(sel_overlay))
            REBAL_OPERATIONS_TOTAL.labels(op="wallet_transfer").inc(len(sel_wallet))
            REBAL_OPERATIONS_TOTAL.labels(op="internal_transfer").inc(len(sel_internal))
            REBAL_OPERATIONS_TOTAL.labels(op="crypto_topup_hint").inc(len(sel_crypto))
            REBAL_PLAN_BUILD_MS.observe(plan["t_ms"])
        except Exception:
            pass

        return plan

    def build_plan(self, target_alloc: dict, constraints: dict | None = None) -> dict:
        """
        Sortie normalisée (READY|SKIP + steps + quantum_quote) pour consommation directe par le RM.
        - Aucun I/O : coûts/px viennent du RM (ex: cost_fn) si besoin, fournis via 'constraints'.
        - Rétro-compat : on fusionne les champs legacy du plan 'raw' dans le retour final.
        """
        # 1) Obtenir un plan "raw" depuis l'implémentation existante, sans I/O
        raw = None
        for name in ("_build_plan_raw", "build_plan_core", "_legacy_build_plan"):
            fn = getattr(self, name, None)
            if callable(fn):
                raw = fn(target_alloc, constraints)
                break
        if raw is None:
            # fallback ultra conservateur : plan vide (SKIP)
            raw = {}

        # 2) Deriver les steps (liste d’opérations abstraites)
        steps = raw.get("steps")
        if not isinstance(steps, list):
            try:
                steps = list(self.plan_to_operations(raw) or [])
            except Exception:
                steps = []

        # 3) Quantum par quote (toujours présent)
        qmap = raw.get("quantum_quote")
        if not isinstance(qmap, dict) or not qmap:
            qmap = getattr(self.cfg, "rebal_quantum_quote_map", None) or {"USDC": 250.0, "EUR": 250.0}

        # 4) Statut READY|SKIP garanti
        status = "READY" if steps else "SKIP"

        # 5) Fusion: on renvoie le triplet normalisé + les champs legacy inchangés
        plan = {"steps": steps, "quantum_quote": qmap, "status": status}
        try:
            plan.update(raw)  # compat: on garde le détail historique si présent
        except Exception:
            pass
        return plan

    def _normalize_reb_plan(self, plan: dict) -> dict:
        """
        Normalise {"steps":[...], "quantum_quote":{quote:float}, "status":"READY|SKIP"}.
        """
        p = dict(plan or {})
        steps = p.get("steps") or p.get("operations") or []
        if not isinstance(steps, list):
            steps = [steps]
        q = p.get("quantum_quote") or {}
        if not isinstance(q, dict):
            q = {}
        status = str(p.get("status", "READY" if steps else "SKIP")).upper()
        if status not in ("READY", "SKIP"):
            status = "READY" if steps else "SKIP"
        return {"steps": steps, "quantum_quote": q, "status": status}

    # -------------------------- Outils & statuts (compat) ---------------------
    def estimate_cross_cex_net_bps(self) -> float:
        """Compat: renvoie 0.0 (cross-CEX inactif dans cette version hints-only)."""
        return 0.0

    def plan_to_operations(self, plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Compat: transforme un plan en liste d'opérations typées (pour RM).
        Ici ça reste des hints ; le RM choisit quoi exécuter et comment.
        """
        ops: List[Dict[str, Any]] = []
        for op in plan.get("overlay_comp") or []:
            ops.append({"type": "overlay_compensation", **op})
        for w in plan.get("wallet_transfers") or []:
            ops.append({"type": "internal_wallet_transfer", **w})
        for it in plan.get("internal_transfers") or []:
            ops.append({"type": "internal_alias_transfer", **it})
        for t in plan.get("crypto_topups") or []:
            ops.append({"type": "crypto_topup_hint", **t})
        if plan.get("bridge_pre"):
            ops.append({"type": "bridge_pre_hint", **plan["bridge_pre"]})
        return ops

    def push_history(self, imbalance: Dict[str, Any], plan: Dict[str, Any]) -> None:
        self.history.append({"ts": _now(), "imbalance": imbalance, "plan": plan})

    def get_imbalance_history(self) -> List[Dict[str, Any]]:
        return list(self.history)

    def _freshness_ages(self) -> Dict[str, float]:
        now = _now()
        def _min_age_cash() -> float:
            ages = []
            for ex, per_alias in (self.latest_balances or {}).items():
                for _, assets in (per_alias or {}).items():
                    # pas d’horodatage individuel => on prend last_update
                    pass
            return max(0.0, now - (self.last_update or now))
        def _min_age_wallets() -> float:
            return max(0.0, now - (self.last_update or now))
        def _min_age_price() -> float:
            # on dérive de orderbooks ts
            ts = []
            for ex, per in (self.latest_orderbooks or {}).items():
                for _, ob in (per or {}).items():
                    if "ts" in ob: ts.append(ob["ts"])
            return max(0.0, now - max(ts) ) if ts else float("inf")
        def _min_age_overlay() -> float:
            return 0.0 if self._overlay_flow else float("inf")
        return {"cash": _min_age_cash(), "crypto": _min_age_wallets(), "price": _min_age_price(), "overlay": _min_age_overlay()}

    def get_status(self) -> Dict[str, Any]:
        return {
            "balances": self.latest_balances,
            "wallets": self.latest_wallets,
            "orderbooks_count": sum(len(v or {}) for v in self.latest_orderbooks.values()),
            "overlay": self.overlay_snapshot(),
            "policy": {
                "quote_currencies": self.quote_currencies,
                "min_cash_per_quote": self.min_cash_per_quote,
                "min_crypto_value_usdc": self.min_crypto_value_usdc,
                "rebal_quantum_min_quote": self.rebal_quantum_min_quote,
                "rebal_max_ops_per_min": self.rebal_max_ops_per_min,
                "rebal_priority": self.rebal_priority,
                "rebal_hint_ttl_s": self.rebal_hint_ttl_s,
            },
        }

    # ---------------------- Orchestration legacy (no-op) ----------------------
    def start(self):  # compat
        return

    def stop(self):   # compat
        return

    # ------------------- Stubs bridge/cross (compat) -------------------------
    def _best_quote_px(self, exchange: str, symbol: str, side: str) -> float:
        """Compat: meilleur prix (bid/ask) si OB connu."""
        b, a = self._rm_top_of_book(exchange, symbol)
        return a if (side or "").upper().startswith("B") else b

    def _score_bridge_leg_cost_bps(self, exchange: str, symbol: str) -> float:
        """Compat: coût de jambe (spread approximatif, bps)."""
        b, a = self._rm_top_of_book(exchange, symbol)
        if b > 0 and a > 0:
            mid = 0.5*(a+b)
            return (a - b) / mid * 10_000.0
        return 0.0

    def _bridge_plan(self, exchange: str, from_q: str, to_q: str, amt_q: float) -> Optional[Dict[str, Any]]:
        """Compat: renvoie None (aucun bridge auto ici)."""
        return None


