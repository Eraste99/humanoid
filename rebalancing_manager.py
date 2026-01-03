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
from contracts.payloads import canonical_transfer_id

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

        self.enabled_aliases = _norm_list_upper(
            enabled_aliases if enabled_aliases is not None else getattr(self.cfg, "enabled_aliases", None),
            ["TT", "TM"]
        )
        self.enabled_assets    = [a.upper() for a in (enabled_assets or [
            "ETH","BTC","SOL","ADA","XRP","DOGE","DOT","AVAX","AXS","LTC","SHIB","UNI","LINK",
            "ATOM","XLM","ALGO","FIL","LDO","APE","BNB"
        ])]

        # Stratégie (legacy conservés)
        self.history_limit = int(history_limit)
        self.target_diff_quote = float(target_diff_quote)
        if internal_transfer_threshold is None:
            internal_transfer_threshold = getattr(self.cfg, "rebal_internal_transfer_threshold", 250.0)
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
        self.rebal_priority: List[str] = list(getattr(self.cfg, "rebal_priority", ["CASH", "CRYPTO", "OVERLAY"]))
        self.rebal_hint_ttl_s: int = int(getattr(self.cfg, "rebal_hint_ttl_s", 120))
        self._reb_slot_ttl_s: float = float(
            getattr(self.cfg, "rebal_slot_ttl_s", getattr(self.cfg, "rebal_hint_ttl_s", 120))
        )
        cfg_min_frag = getattr(self.cfg, "min_fragment_usdc", None)
        cfg_min_map = getattr(self.cfg, "min_fragment_quote", None) or {}
        derived_min = max(cfg_min_map.get(q, 0.0) for q in self.quote_currencies) if cfg_min_map else 0.0
        try:
            derived_min = float(derived_min)
        except Exception:
            derived_min = 0.0
        base_min_frag = 0.0
        try:
            base_min_frag = float(cfg_min_frag) if cfg_min_frag is not None else 0.0
        except Exception:
            base_min_frag = 0.0
        self.rebal_fragment_min_quote = max(self.rebal_quantum_min_quote, base_min_frag, derived_min)
        rm_cfg = getattr(self.cfg, "rm", None)
        profile = str(getattr(getattr(self.cfg, "g", None), "capital_profile", "LARGE") or "LARGE").upper()
        inflight_reb = getattr(rm_cfg, "inflight_rebal_by_profile", {}) if rm_cfg else {}
        self.rebal_inflight_cap: int = int((inflight_reb or {}).get(profile) or (inflight_reb or {}).get("LARGE") or 0)
        combo_caps = getattr(rm_cfg, "combo_cap_usd_by_profile", {}) if rm_cfg else {}
        base_combo_cap = float((combo_caps or {}).get(profile) or (combo_caps or {}).get("LARGE") or 0.0)
        degraded_factor = 1.0
        try:
            degraded_factor = float(getattr(rm_cfg, "combo_ttl_degraded_factor", 1.0)) if rm_cfg else 1.0
        except Exception:
            degraded_factor = 1.0
        degraded_factor = min(max(degraded_factor, 0.0), 1.0)
        self.combo_cap_effective = base_combo_cap * degraded_factor if base_combo_cap > 0 else 0.0
        self.rebal_ops_blocked_by_caps: int = 0
        self.rebal_plan_clipped_by_caps: int = 0
        self._last_combo_cap_ratio: Optional[float] = None
        self._last_rebal_cap_status: Optional[str] = None
        self._reb_active_slots: deque = deque(maxlen=512)
        self._rebalancing_assets: Dict[Tuple[str, str], float] = {}
        self._inflight_transfers: Dict[str, Dict[str, Any]] = {}

        # Snapshots ingérés par le RM
        # OB: latest_orderbooks[EX][SYMBOL] = {"bid":..., "ask":..., "ts": ...}
        self.latest_orderbooks: Dict[str, Dict[str, Dict[str, float]]] = {}
        # balances: latest_balances[EX][ALIAS] = {"USDC":..., "EUR":..., "ETH":...}
        self.latest_balances:   Dict[str, Dict[str, Dict[str, float]]] = {}
        # wallets: latest_wallets[EX][ALIAS][WALLET] = {"USDC":..., "EUR":...}
        self.latest_wallets: Dict[str, Dict[str, Dict[str, Dict[str, float]]]] = {}

        # Overlay (virtuel) par devise — clé: "ALIAS:EX_A->EX_B|CCY" → montant
        self._overlay_flow: Dict[str, float] = defaultdict(float)

        # Garde-fous snapshots
        self._snapshots_missing_since: Optional[float] = None
        self._snapshots_last_error_ts: float = 0.0
        self._snapshots_missing_error_s: float = max(
            5.0,
            float(getattr(self.cfg, "rebal_snapshots_missing_error_s", 45.0)),
        )
        self._snapshots_error_cooldown_s: float = max(
            5.0,
            float(getattr(self.cfg, "rebal_snapshots_error_cooldown_s", 60.0)),
        )

        # Historique (debug) & rate-limit
        self.history: deque = deque(maxlen=256)
        self._emit_ts = deque(maxlen=512)


        # Event sink (compat)
        self._event_sink: Optional[Callable[[Dict[str, Any]], Any]] = None

        # Coût externe (fourni par le RM)
        self._reb_cost_fn: Optional[Callable[[Dict[str, Any]], float]] = None

        # Dernière mise à jour (utile pour l’âge des snapshots)
        self.last_update: float = 0.0

    # -------------------------- Event sink (compat) ---------------------------
    def set_event_sink(self, sink: Optional[Callable[[Dict[str, Any]], Any]]) -> None:
        self._event_sink = sink

    @property
    def rebal_ops_emitted_last_min(self) -> int:
        return len(self._emit_ts)

    @property
    def inflight_rebal_current(self) -> int:
        return self._current_inflight_count()


    def set_cost_function(self, fn: Optional[Callable[[Dict[str, Any]], float]]) -> None:
        """Permet au RM d'injecter la fonction de coût utilisée dans les plans."""
        self._reb_cost_fn = fn

    def _emit(self, level: str, message: str, **kw) -> None:
        if not self._event_sink:
            return
        try:
            evt = {"module": "RebalancingManager", "level": level, "message": message}
            if kw: evt.update(kw)
            self._event_sink(evt)
        except Exception as exc:
            log.warning("event sink failed: %s", exc, exc_info=False)

    def _prune_rebal_slots(self, now: float) -> None:
        ttl = float(self._reb_slot_ttl_s or 0.0)
        if ttl <= 0:
            self._reb_active_slots.clear()
            return
        self._reb_active_slots = type(self._reb_active_slots)(
            [t for t in self._reb_active_slots if (now - t) < ttl],
            maxlen=self._reb_active_slots.maxlen,
        )
        self._prune_rebal_assets(now)

    def _current_inflight_count(self) -> int:
        if self._inflight_transfers:
            return len(self._inflight_transfers)
        return len(self._reb_active_slots)

    def _canonical_transfer_id(self, payload: Dict[str, Any]) -> Optional[str]:
        try:
            return canonical_transfer_id(payload)
        except Exception:
            return None

    def _ensure_transfer_id(self, op: Dict[str, Any], transfer_type: str) -> Optional[str]:
        if not op:
            return None
        existing = op.get("transfer_id")
        if existing:
            return str(existing)
        from_alias = op.get("from_alias") or (op.get("from") or {}).get("alias") or op.get("alias")
        to_alias = op.get("to_alias") or (op.get("to") or {}).get("alias") or op.get("alias")
        if from_alias:
            op.setdefault("from_alias", from_alias)
        if to_alias:
            op.setdefault("to_alias", to_alias)
        payload = {
            "exchange": op.get("exchange"),
            "from_alias": from_alias,
            "to_alias": to_alias,
            "from_wallet": op.get("from_wallet"),
            "to_wallet": op.get("to_wallet"),
            "ccy": op.get("ccy"),
            "amount": op.get("amount"),
            "type": transfer_type,
        }
        transfer_id = self._canonical_transfer_id(payload)
        if transfer_id:
            op["transfer_id"] = transfer_id
        return transfer_id

    def _register_inflight_transfers(self, operations: List[Dict[str, Any]], now: float) -> None:
        if not operations:
            return
        for op in operations:
            if not isinstance(op, dict):
                continue
            if op.get("from_wallet") or op.get("to_wallet"):
                transfer_id = self._ensure_transfer_id(op, "internal_wallet_transfer")
            elif op.get("from_alias") or op.get("to_alias") or op.get("from") or op.get("to"):
                transfer_id = self._ensure_transfer_id(op, "internal_subaccount_transfer")
            else:
                transfer_id = None
            if transfer_id:
                self._inflight_transfers.setdefault(transfer_id, {"state": "SUBMITTED", "ts": now})

    def restore_inflight_from_journal(self, *, lhm=None, now_ms: Optional[int] = None) -> None:
        lhm = lhm or getattr(self.rm, "_lhm_manager", None) or getattr(self.rm, "history_logger", None)
        if lhm is None:
            return
        now_ms = now_ms or int(time.time() * 1000)
        try:
            rows = lhm.list_inflight_transfers(include_expired=True)
        except Exception:
            log.exception("restore_inflight_from_journal failed")
            return
        self._inflight_transfers = {}
        for row in rows or []:
            op_id = str(row.get("op_id") or "")
            if not op_id.startswith("XFER/"):
                continue
            transfer_id = op_id.split("/", 1)[-1]
            status = str(row.get("status") or "").upper()
            expires = row.get("expires_ts_ms")
            if expires is not None and int(expires) <= now_ms:
                continue
            self._inflight_transfers[transfer_id] = {"state": status or "SUBMITTED", "ts": _now()}

    def mark_transfer_status(self, transfer_id: Optional[str], status: Optional[str]) -> None:
        if not transfer_id:
            return
        st = str(status or "").upper()
        if st in {"SETTLED", "FAILED", "ERROR", "CANCELLED", "REJECTED"}:
            self._inflight_transfers.pop(str(transfer_id), None)
            return
        self._inflight_transfers[str(transfer_id)] = {"state": st or "SUBMITTED", "ts": _now()}

    def _prune_rebal_assets(self, now: float) -> None:
        ttl = float(self._reb_slot_ttl_s or 0.0)
        if ttl <= 0:
            self._rebalancing_assets.clear()
            return
        kept: Dict[Tuple[str, str], float] = {}
        for key, ts in list(self._rebalancing_assets.items()):
            if (now - ts) < ttl:
                kept[key] = ts
        self._rebalancing_assets = kept

    def _register_rebalancing_assets(self, operations: List[Dict[str, Any]], now: float) -> None:
        if not operations:
            return

        def _extract_assets(op: Dict[str, Any]) -> List[Tuple[str, str]]:
            assets: List[Tuple[str, str]] = []
            try:
                ex = str(op.get("exchange") or op.get("ex") or op.get("venue") or "NA").upper()
            except Exception:
                ex = "NA"

            possible_assets: List[str] = []
            for key in ("asset", "base", "ccy"):
                val = op.get(key)
                if val:
                    possible_assets.append(str(val).upper())
            pair = op.get("pair") or op.get("symbol")
            if pair:
                pk = str(pair).replace("-", "").upper()
                possible_assets.append(pk)
                for q in self.quote_currencies or []:
                    if pk.endswith(str(q).upper()) and len(pk) > len(q):
                        possible_assets.append(pk[: -len(str(q))])

            for asset in possible_assets:
                if asset:
                    assets.append((ex, asset))
            return assets

        for op in operations:
            for key in _extract_assets(op or {}):
                self._rebalancing_assets[key] = now

    def is_asset_under_rebalancing(self, exchange: str, asset: str) -> bool:
        now = _now()
        self._prune_rebal_assets(now)
        exu = str(exchange or "NA").upper()
        au = str(asset or "").upper()
        if not au:
            return False
        return (exu, au) in self._rebalancing_assets or ("NA", au) in self._rebalancing_assets

    def _estimate_plan_notional(self, operations: List[Dict[str, Any]]) -> float:
        total = 0.0
        for op in operations or []:
            try:
                amt = float(op.get("amount", 0.0) or 0.0)
            except Exception:
                amt = 0.0
            if amt > 0:
                total += amt
        return total

# -------------------------- Snapshots guards ----------------------------

    def _has_balances_snapshot(self) -> bool:
        for per_alias in (self.latest_balances or {}).values():
            for assets in (per_alias or {}).values():
                if assets:
                    return True
        return False

    def _has_wallet_snapshot(self) -> bool:
        for per_alias in (self.latest_wallets or {}).values():
            for per_wallet in (per_alias or {}).values():
                for ccy_map in (per_wallet or {}).values():
                    if ccy_map:
                        return True
        return False

    def _has_orderbook_snapshot(self) -> bool:
        for per in (self.latest_orderbooks or {}).values():
            for ob in (per or {}).values():
                if ob.get("bid") or ob.get("ask"):
                    return True
        return False

    def _snapshots_ready(self) -> bool:
        """Vérifie la présence de snapshots avant traitement et déclenche un event si absent."""
        if self._has_balances_snapshot():
            self._snapshots_missing_since = None
            return True

        now = _now()
        if self._snapshots_missing_since is None:
            self._snapshots_missing_since = now
            return False

        missing_for = now - self._snapshots_missing_since
        if missing_for >= self._snapshots_missing_error_s:
            if (now - self._snapshots_last_error_ts) >= self._snapshots_error_cooldown_s:
                states = {
                    "balances": self._has_balances_snapshot(),
                    "wallets": self._has_wallet_snapshot(),
                    "orderbooks": self._has_orderbook_snapshot(),
                }
                missing = [k for k, ok in states.items() if not ok]
                self._snapshots_last_error_ts = now
                self._emit(
                    "ERROR",
                    "snapshots_missing",
                    missing_since_s=round(missing_for, 1),
                    missing_components=missing,
                )
        return False

    # ------------------------------- Ingestion --------------------------------
    def ingest_snapshot(self, data: Dict[str, Any]) -> None:
        """data: {exchange, pair_key|symbol, best_bid, best_ask, active=True}."""
        try:
            ex = _norm((data or {}).get("exchange", ""))
            if ex not in self.enabled_exchanges:
                return
            sym = _norm((data or {}).get("pair_key") or (data or {}).get("symbol") or "")
            if not sym:
                return
            bid = _to_f((data or {}).get("best_bid"))
            ask = _to_f((data or {}).get("best_ask"))
            if bid <= 0 or ask <= 0 or ask <= bid:  # garde-fous
                return
            self.latest_orderbooks.setdefault(ex, {})[sym] = {"bid": bid, "ask": ask, "ts": _now()}
            self.last_update = _now()
        except Exception as exc:
            log.exception("ingest_snapshot failed")
            self._emit("ERROR", "ingest_snapshot_failed", exchange=(data or {}).get("exchange"), error=str(exc))

    def update_balances(self, balances: Dict[str, Any]) -> None:
        """
        Alimente la vue capital du Rebalancer à partir du MBF / RM.

        Entrées acceptées :
        - snapshot complet MBF/RM :
            {"mode": ..., "balances": {ex: {alias: {asset: amount}}}, "meta": {...}}
        - ou directement le dict balances :
            {ex: {alias: {asset: amount}}} (legacy/tests).

        Effets :
        - met à jour `self.latest_balances` (miroir capital pour Rebalancing + RM),
        - ne gère aucun cap métier ni overlay : ces décisions restent côté RM.
        """
        raw = balances or {}

        # Support des snapshots complets MBF/RM
        if isinstance(raw, dict) and "balances" in raw and isinstance(raw["balances"], dict):
            raw = raw["balances"] or {}

        latest: Dict[str, Dict[str, Dict[str, float]]] = {}

        for ex, per_alias in (raw or {}).items():
            if not ex:
                continue
            exu = ex.upper()
            # On ne garde que les exchanges activés
            if hasattr(self, "enabled_exchanges") and self.enabled_exchanges:
                if exu not in self.enabled_exchanges:
                    continue

            dst_ex = latest.setdefault(exu, {})
            for alias, per_ccy in (per_alias or {}).items():
                alias_key = alias or ""
                dst_alias = dst_ex.setdefault(alias_key, {})
                for asset, amount in (per_ccy or {}).items():
                    if amount is None:
                        continue
                    dst_alias[asset.upper()] = float(amount or 0.0)

        self.latest_balances = latest


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
        except Exception as exc:
            log.exception("update_wallets failed")
            self._emit("ERROR", "update_wallets_failed", error=str(exc))


    # ------------------------------ Overlay (compat) --------------------------
    @staticmethod
    def _dir_key(self, alias: str, ex_from: str, ex_to: str, ccy: str) -> str:
        _norm_local = lambda x: str(x or "").upper()
        return f"{_norm_local(alias)}:{_norm_local(ex_from)}->{_norm_local(ex_to)}|{_norm_local(ccy)}"

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
        if not self._snapshots_ready():
            return None

        out = {
            "CASH": {q: {} for q in self.quote_currencies},
            "CRYPTO": {},
            "OVERLAY": self.overlay_snapshot(),
        }
        has_cash_need = False
        has_crypto_need = False

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
                    has_cash_need = True

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
                        has_crypto_need = True

                has_overlay_need = any(abs(v) > 0.0 for v in (out["OVERLAY"] or {}).values())

                if not (has_cash_need or has_crypto_need or has_overlay_need):
                    return None

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
                            "exchange": ex,
                            "alias": alias,
                            "from_alias": alias,
                            "to_alias": alias,
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

        if not self._snapshots_ready():
            qmap = getattr(self.cfg, "rebal_quantum_quote_map", None) or {"USDC": 250.0, "EUR": 250.0}
            ages = self._freshness_ages()
            return {
                "overlay_comp": [],
                "wallet_transfers": [],
                "internal_transfers": [],
                "crypto_topups": [],
                "rebalancing_trade": None,
                "bridge_pre": None,
                "t_ms": 0.0,
                "snapshots_age_s": ages,
                "steps": [],
                "quantum_quote": qmap,
                "status": "SKIP",
                "reason": "snapshots_missing",
            }

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
            qmin = float(self.rebal_fragment_min_quote)
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
        for prio in (self.rebal_priority or ["CASH", "CRYPTO", "OVERLAY"]):
            ordered.extend([op for op in buckets.get(prio, []) if op])

        # rate limit glissant 60s
        now = _now()
        self._emit_ts = type(self._emit_ts)([t for t in self._emit_ts if (now - t) < 60.0], maxlen=self._emit_ts.maxlen)
        self._prune_rebal_slots(now)
        inflight_current = self._current_inflight_count()
        available_slots = max(0, int(self.rebal_inflight_cap) - inflight_current)
        budget_rate = max(0, int(self.rebal_max_ops_per_min) - len(self._emit_ts))
        budget = min(len(ordered), budget_rate, available_slots)
        blocked_by_caps = 0
        if available_slots <= 0:
            blocked_by_caps = len(ordered)
            self._last_rebal_cap_status = "REB_CAP_REACHED"
        elif available_slots < len(ordered):
            blocked_by_caps = len(ordered) - min(len(ordered), available_slots)
            self._last_rebal_cap_status = "REB_CAP_REACHED"
        else:
            self._last_rebal_cap_status = "OK"

        selected = ordered[:budget] if budget < len(ordered) else list(ordered)
        if blocked_by_caps > 0:
            self.rebal_ops_blocked_by_caps += blocked_by_caps
            self.rebal_plan_clipped_by_caps += 1
        self._emit_ts.extend([now] * len(selected))
        self._reb_active_slots.extend([now] * len(selected))
        self._register_inflight_transfers(selected, now)
        self._register_rebalancing_assets(selected, now)

        plan_notional = self._estimate_plan_notional(selected)
        self._last_combo_cap_ratio = None
        if self.combo_cap_effective > 0:
            try:
                self._last_combo_cap_ratio = plan_notional / self.combo_cap_effective
            except Exception:
                self._last_combo_cap_ratio = None

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
            "rebalancing_trade": None,  # compat (désactivé)
            "bridge_pre": sel_bridge,  # compat (rarement non-None ici)
            "t_ms": (time.perf_counter_ns() - t0) / 1e6,
            "snapshots_age_s": ages,
            "rebal_cap_status": self._last_rebal_cap_status or "OK",
            "combo_cap_ratio_planned": self._last_combo_cap_ratio,
        }

        if callable(self._reb_cost_fn):
            plan["cost_fn"] = self._reb_cost_fn

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
        # 1) Obtenir un plan "raw" depuis l'implémentation existante, sans I/O.
        # On privilégie d'abord build_plan_raw (nouvelle API), puis les variantes legacy.
        raw = None
        for name in ("build_plan_raw", "_build_plan_raw", "build_plan_core", "_legacy_build_plan"):
            fn = getattr(self, name, None)
            if callable(fn):
                raw = fn(target_alloc, constraints)
                break
        if raw is None:
            # Fallback ultra conservateur : plan vide (SKIP)
            raw = {}

        # 2) Dériver les steps (liste d’opérations abstraites)
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
        plan: Dict[str, Any] = {"steps": steps, "quantum_quote": qmap, "status": status}
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
    def estimate_cross_cex_net_bps(
        self,
        *,
        pair_key: str,
        from_exchange: str,
        to_exchange: str,
        fee_from_pct: Optional[float] = None,
        fee_to_pct: Optional[float] = None,
        slip_from_pct: Optional[float] = None,
        slip_to_pct: Optional[float] = None,
    ) -> float:
        """
        Estime le net (bps) d'un cross-CEX en tenant compte du spread, fees et slippage.

        Retourne un float (bps). Valeur négative si l'opération est coûteuse.
        """

        pk = _norm(pair_key)
        sell_ex = _norm(from_exchange)
        buy_ex = _norm(to_exchange)
        if not (pk and sell_ex and buy_ex):
            return float("-inf")

        sell_bid, _ = self._rm_top_of_book(sell_ex, pk)
        _, buy_ask = self._rm_top_of_book(buy_ex, pk)
        if sell_bid <= 0.0 or buy_ask <= 0.0:
            return float("-inf")

        mid = 0.5 * (sell_bid + buy_ask)
        if mid <= 0.0:
            return float("-inf")

        spread_norm = (sell_bid - buy_ask) / mid
        haircut = float(self.cross_cex_haircut or 1.0)
        haircut = min(1.0, max(0.0, haircut))
        spread_norm *= haircut if haircut > 0 else 0.0

        def _fee_pct(ex: str, role: str) -> float:
            fn = getattr(self.rm, "get_fee_pct", None) if self.rm else None
            if callable(fn):
                try:
                    return max(0.0, float(fn(ex, pk, role)))
                except Exception:
                    return 0.0
            return 0.0

        def _slip_pct(ex: str, side: str) -> float:
            fn = getattr(self.rm, "get_slippage", None) if self.rm else None
            if callable(fn):
                try:
                    return max(0.0, float(fn(ex, pk, side)))
                except Exception:
                    return 0.0
            return 0.0

        cost_pct = 0.0
        cost_pct += max(0.0, float(fee_from_pct)) if fee_from_pct is not None else _fee_pct(sell_ex, "taker")
        cost_pct += max(0.0, float(fee_to_pct)) if fee_to_pct is not None else _fee_pct(buy_ex, "taker")
        cost_pct += max(0.0, float(slip_from_pct)) if slip_from_pct is not None else _slip_pct(sell_ex, "sell")
        cost_pct += max(0.0, float(slip_to_pct)) if slip_to_pct is not None else _slip_pct(buy_ex, "buy")

        net_pct = spread_norm - cost_pct
        return net_pct * 10_000.0

    def plan_to_operations(self, plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Compat: transforme un plan en liste d'opérations typées (pour RM).
        Ici ça reste des hints ; le RM choisit quoi exécuter et comment.
        """
        ops: List[Dict[str, Any]] = []

        # Overlay: purement indicatif, laissé au RM
        for op in plan.get("overlay_comp") or []:
            base = dict(op or {})
            ops.append({**base, "type": "overlay_compensation"})

        # Transferts entre wallets (SPOT/FUNDING/DERIV ...)
        for w in plan.get("wallet_transfers") or []:
            base = dict(w or {})
            # On force le type attendu par le RM
            self._ensure_transfer_id(base, "internal_wallet_transfer")
            ops.append({**base, "type": "internal_wallet_transfer"})

        # Transferts intra-CEX entre alias (TT/TM/MM...)
        for it in plan.get("internal_transfers") or []:
            base = dict(it or {})

            # Normalisation des alias
            from_alias = base.pop("from_alias", None)
            to_alias = base.pop("to_alias", None)

            src = base.get("from") or {}
            dst = base.get("to") or {}

            if not from_alias and isinstance(src, dict):
                from_alias = src.get("alias")
            if not to_alias and isinstance(dst, dict):
                to_alias = dst.get("alias")

            if from_alias:
                base["from_alias"] = from_alias
            if to_alias:
                base["to_alias"] = to_alias

            # Type aligné sur l'API du RM
            self._ensure_transfer_id(base, "internal_subaccount_transfer")
            ops.append({**base, "type": "internal_subaccount_transfer"})

        # Top-ups crypto indicatifs
        for t in plan.get("crypto_topups") or []:
            base = dict(t or {})
            ops.append({**base, "type": "crypto_topup_hint"})

        # Pré-bridge éventuel (hint purement informatif)
        if plan.get("bridge_pre"):
            base = dict(plan.get("bridge_pre") or {})
            ops.append({**base, "type": "bridge_pre_hint"})

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
            "rebal_caps": {
                "inflight_cap": self.rebal_inflight_cap,
                "inflight_current": self.inflight_rebal_current,
                "inflight_transfers": len(self._inflight_transfers),
                "ops_emitted_last_min": self.rebal_ops_emitted_last_min,
                "ops_blocked_by_caps": self.rebal_ops_blocked_by_caps,
                "plan_clipped_by_caps": self.rebal_plan_clipped_by_caps,
                "combo_cap_effective": self.combo_cap_effective,
                "combo_cap_ratio_planned": self._last_combo_cap_ratio,
                "rebal_cap_status": self._last_rebal_cap_status,
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


