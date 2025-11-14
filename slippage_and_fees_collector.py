# modules/slippage_and_fees_collector.py
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
SlippageAndFeesCollector — Industry-grade (tri-CEX)
===================================================

Objectif
--------
Collecter et servir des **frais dynamiques par compte** (exchange + alias), avec:
• Snapshots de frais "base" et "effectifs" (VIP/token) + overrides par paire.
• FeeSync robuste (concurrence bornée, backoff + jitter, TTL, détection de changements).
• Politiques VIP (paliers volume 30j) et rabais token (BNB/MNT/...), venues-aware.
• Providers optionnels (volume 30j, soldes/quote) pour affiner les "effective fees".
• Observabilité Prometheus (fallback no-op).

Terminologie
------------
- exchange: "BINANCE", "BYBIT", "COINBASE".
- alias: sous-compte logique (ex: "TT", "TM").
- base fees: maker/taker "bruts" fournis par l’API.
- effective fees: maker/taker après rabais VIP/Token réellement appliqués.

Intégration rapide
------------------
sfc = SlippageAndFeesCollector(cfg=bot_cfg)

# 1) Connexion des clients "fees" (API CEX) et providers (optionnels)
sfc.connect_fee_clients({"BINANCE":{"TT": binance_client_tt, "TM": binance_client_tm}})
sfc.set_vip_schedule("BINANCE", [
    {"min_30d_usd": 0,         "maker": 0.0010, "taker": 0.0010},  # VIP0
    {"min_30d_usd": 1_000_000, "maker": 0.0009, "taker": 0.0009},  # VIP1
])
sfc.set_fee_token_policy("BINANCE", token_code="BNB", maker_discount=0.25, taker_discount=0.25,
                         min_quote_buffer=200.0, disable_on_prudence={"ALERT"})
sfc.set_volume_provider("BINANCE", "TT", provider=binance_volume_30d_provider)      # async -> float USD
sfc.set_balance_provider("BINANCE","TT", provider=binance_balance_provider)          # async -> dict{"USDC":..., "BNB":...}

# 2) Refresh ponctuel (à cadence P0: toutes les 5-10 min ou on-change côté CEX)
await sfc.fee_sync_refresh_once()

# 3) Lecture des frais effectifs (avec rabais si actifs)
mk, tk = sfc.get_effective_fees("BINANCE", "TT", pair="ETHUSDC", prudence_key="NORMAL")  # fractions

# 4) Coût total (frais + slippage) optionnel
total = sfc.get_total_cost_pct("ETHUSDC",
    buy_leg = {"ex":"BINANCE","alias":"TT","side":"buy","role":"taker"},
    sell_leg= {"ex":"COINBASE","alias":"TT","side":"sell","role":"taker"},
    slippage_kind=getattr(bot_cfg, "sfc_slippage_source", "ewma"))

Notes
-----
• Pas de boucle interne permanente: la cadence de refresh est pilotée depuis ton Boot/RM.
• SFC détecte les changements et publie des métriques; tu peux enregistrer un callback on_change si besoin.
"""

import asyncio, time, random, math
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional, Tuple, List, Callable, Awaitable
from collections import defaultdict, deque



# Observabilité — sous-module PASSIF (no Prometheus ici)
from typing import Callable, Optional, Dict, Any
import logging, time

log = logging.getLogger("slippage_and_fees_collector")

EventSink = Callable[[str, Dict[str, Any]], None]
def _now() -> float: return time.time()

class _NoMetric:
    def labels(self, *a, **k): return self
    def observe(self, *a, **k): return
    def inc(self, *a, **k): return
    def set(self, *a, **k): return

# Noms attendus par le code (no-op)
SFC_FEE_LAST_REFRESH_TS   = _NoMetric()
SFC_FEE_SYNC_DURATION_MS  = _NoMetric()
SFC_FEE_SYNC_ERRORS_TOTAL = _NoMetric()
SFC_FEE_REFRESH_TOTAL     = _NoMetric()
SFC_FEE_CHANGED_TOTAL     = _NoMetric()
SFC_FEE_EFFECTIVE_PCT     = _NoMetric()
SFC_VIP_TIER              = _NoMetric()
SFC_TOKEN_ENABLED         = _NoMetric()

# --- Helpers ----------------------------------------------------------------
def _norm_ex(x: str) -> str:   return (x or "").strip().upper()
def _norm_pair(x: str) -> str: return (x or "").replace("-", "").upper()
def _safe_f(x: Any, d: float=0.0) -> float:
    try: return float(x)
    except Exception: return float(d)
def _nearly_equal(a: float, b: float, eps: float = 1e-9) -> bool:
    return abs(a - b) <= eps * max(1.0, abs(a), abs(b))

# ============================ Data Models ====================================

@dataclass
class FeeScheduleTier:
    """Palier VIP (volume 30j en USD) → frais bruts (fractions)."""
    min_30d_usd: float
    maker: float
    taker: float

@dataclass
class FeeTokenPolicy:
    """Politique de token de frais (rabais) par venue."""
    token_code: str = ""              # ex: BNB, MNT
    maker_discount: float = 0.0       # ex: 0.25 (25%)
    taker_discount: float = 0.0
    min_quote_buffer: float = 0.0     # montant min quote pour activer
    disable_on_prudence: set[str] = field(default_factory=set)  # {"ALERT", ...}

@dataclass
class FeeSnapshot:
    """Snapshot par alias (base + overrides + VIP + effectifs)."""
    maker_base: float = 0.0
    taker_base: float = 0.0
    per_pair: Dict[str, Dict[str, float]] = field(default_factory=dict)  # {PAIR: {"maker":f,"taker":f}}
    vip_tier: int = 0
    maker_vip: float = 0.0
    taker_vip: float = 0.0
    token_enabled: bool = False
    maker_effective: float = 0.0
    taker_effective: float = 0.0
    last_refresh_ts: float = 0.0
    src: str = "api"  # "api"|"manual"
    ttl_s: float = 600.0

    def effective_for(self, role: str, pair: Optional[str] = None) -> float:
        """Retourne la fraction effective (pair override > alias default)."""
        role = (role or "taker").strip().lower()
        if pair:
            pp = self.per_pair.get(_norm_pair(pair))
            if pp and role in pp:
                return float(pp[role])
        return float(self.maker_effective if role == "maker" else self.taker_effective)

    def is_fresh(self, now: Optional[float] = None) -> bool:
        t = time.time() if now is None else float(now)
        return (t - float(self.last_refresh_ts)) <= float(self.ttl_s)

# ============================== Collector ====================================

class SlippageAndFeesCollector:
    """
    • Pas de boucle interne permanente.
    • FeeSync sur demande (refresh_once) avec concurrence bornée et backoff+jitter.
    • Connaît des providers (volume 30j, balances) pour déterminer VIP et token.
    • Calcule des "effective fees" à partir des base fees + VIP + token policy.
    """

    # --- Slippage minimal (pour get_total_cost_pct). Tu peux l’enrichir ailleurs si besoin. ---
    def __init__(self, *,
                 cfg: Optional[Any] = None,
                 slippage_lookback_s: Optional[float] = None):
        self.cfg = cfg
        self.lookback_s = float(slippage_lookback_s if slippage_lookback_s is not None
                                else getattr(cfg, "sfc_slippage_lookback_s", 60))
        # Slippage buffers (optionnels)
        self._ewma: Dict[Tuple[str,str,str], float] = defaultdict(float)
        self._hist: Dict[Tuple[str,str,str], deque] = defaultdict(lambda: deque(maxlen=3600))
        self._event_sink = None  # callback vers RM (observabilité centralisée)

        # Fees landscape
        self._fee_clients: Dict[str, Dict[str, Any]] = defaultdict(dict)             # ex -> alias -> client
        self._snapshots: Dict[str, Dict[str, FeeSnapshot]] = defaultdict(dict)       # ex -> alias -> snapshot
        self._vip_schedules: Dict[str, List[FeeScheduleTier]] = defaultdict(list)    # ex -> [tiers...]
        self._token_policies: Dict[str, FeeTokenPolicy] = {}                         # ex -> policy

        # Providers optionnels
        self._volume_providers: Dict[Tuple[str,str], Callable[[], Awaitable[float]]] = {}
        self._balance_providers: Dict[Tuple[str,str], Callable[[], Awaitable[Dict[str,float]]]] = {}

        # Callbacks on-change (optionnel)
        self._on_change: Optional[Callable[[str,str,FeeSnapshot,FeeSnapshot], None]] = None
        self._fee_token_targets: Dict[str, Dict[str, float]] = defaultdict(dict)  # ex -> {token: target_pct}

    # --------------------------- Slippage (minimal) ---------------------------

    # -------------------- Fee token level / targets (hooks RM) --------------------

    def get_total_cost_pct_route(
            self,
            route: dict,
            *,
            maker_on_buy: bool = False,
            maker_on_sell: bool = False,
            slippage_kind: str | None = None,
    ) -> float:
        """
        Wrapper passif: dérive pair + jambes depuis un route dict
          {'buy_ex':..., 'sell_ex':..., 'base':'ETH', 'quote':'USDC', ['buy_alias','sell_alias']}
        et appelle la version paire+jambes déjà existante.
        Retourne une fraction (ex: 0.0018 = 18 bps).
        """
        if not isinstance(route, dict):
            return 0.0
        base = str(route.get("base") or "").upper()
        quote = str(route.get("quote") or "").upper()
        pair = f"{base}{quote}"
        buy_ex = str(route.get("buy_ex") or "").upper()
        sell_ex = str(route.get("sell_ex") or "").upper()
        buy_alias = str(route.get("buy_alias") or "TT").upper()
        sell_alias = str(route.get("sell_alias") or "TT").upper()

        buy_leg = {"ex": buy_ex, "alias": buy_alias, "side": "buy", "role": "maker" if maker_on_buy else "taker"}
        sell_leg = {"ex": sell_ex, "alias": sell_alias, "side": "sell", "role": "maker" if maker_on_sell else "taker"}

        if slippage_kind is None:
            slippage_kind = str(getattr(self.cfg, "sfc_slippage_source", "ewma"))

        # Version 'paire + jambes' déjà présente dans ce module
        return float(self.get_total_cost_pct(pair, buy_leg=buy_leg, sell_leg=sell_leg, slippage_kind=slippage_kind))

    def update_fee_token_level(self, ex: str, token_code: str, units: float) -> None:
        """
        Le RM pousse périodiquement le solde actuel de token de frais (ex: BNB, MNT).
        On publie la métrique FEE_TOKEN_LEVEL{ex, token}.
        """
        try:
            from modules.obs_metrics import FEE_TOKEN_LEVEL  # Gauge(ex, token)
        except Exception:
            FEE_TOKEN_LEVEL = None
        if FEE_TOKEN_LEVEL:
            try:
                FEE_TOKEN_LEVEL.labels(_norm_ex(ex), token_code.upper()).set(float(units or 0.0))
            except Exception:
                pass

    def set_fee_token_target_percent(self, ex: str, token_code: str, target_pct: float) -> None:
        """
        Cible (% du portefeuille quote) que tu souhaites garder en token de frais.
        Le RM lit ça dans le cfg et le pousse ici. On le retient en mémoire et on publie la métrique.
        """
        try:
            from modules.obs_metrics import FEE_TOKEN_TARGET_PERCENT  # Gauge(ex, token)
        except Exception:
            FEE_TOKEN_TARGET_PERCENT = None

        exn = _norm_ex(ex)
        tok = token_code.upper()
        self._fee_token_targets[exn][tok] = float(max(0.0, target_pct))

        if FEE_TOKEN_TARGET_PERCENT:
            try:
                FEE_TOKEN_TARGET_PERCENT.labels(exn, tok).set(self._fee_token_targets[exn][tok])
            except Exception:
                pass

    def observe_slippage(self, ex: str, pair: str, side: str, frac: float, ts: Optional[float]=None) -> None:
        exu, pk, sd = _norm_ex(ex), _norm_pair(pair), ("buy" if (side or "").lower().startswith("b") else "sell")
        if frac is None: return
        v = max(0.0, _safe_f(frac, 0.0))
        t = time.time() if ts is None else float(ts)
        key = (exu, pk, sd)
        dq = self._hist[key]
        cutoff = t - self.lookback_s
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        dq.append((t, v))
        # EWMA
        prev = self._ewma[key]; a = 0.22
        self._ewma[key] = (a * v) + ((1 - a) * prev if prev > 0.0 else a * v)

    def get_slippage(self, ex: str, pair: str, side: str, *, kind: str="ewma", default: float=0.0) -> float:
        exu, pk, sd = _norm_ex(ex), _norm_pair(pair), ("buy" if (side or "").lower().startswith("b") else "sell")
        dq = self._hist.get((exu, pk, sd))
        if not dq: return float(default)
        if kind == "last": return float(dq[-1][1])
        if kind == "p95":
            cutoff = time.time() - self.lookback_s
            arr = [v for (ts, v) in dq if ts >= cutoff]
            if not arr: return float(default)
            arr.sort(); idx = max(0, int(0.95*(len(arr)-1)))
            return float(arr[idx])
        return float(self._ewma.get((exu, pk, sd), default))

    # ------------------------------ Fee policies -----------------------------

    def set_vip_schedule(self, exchange: str, tiers: List[Dict[str, Any]]) -> None:
        """
        Déclare les paliers VIP d’un exchange: liste triée par min_30d_usd croissant.
        Chaque entrée: {"min_30d_usd": float, "maker": float, "taker": float}
        """
        exu = _norm_ex(exchange)
        lst: List[FeeScheduleTier] = []
        for t in (tiers or []):
            lst.append(FeeScheduleTier(
                min_30d_usd = float(t.get("min_30d_usd", 0.0)),
                maker = _safe_f(t.get("maker", 0.0)), taker = _safe_f(t.get("taker", 0.0))
            ))
        lst.sort(key=lambda x: x.min_30d_usd)
        self._vip_schedules[exu] = lst

    def set_fee_token_policy(self, exchange: str, *,
                             token_code: str,
                             maker_discount: float,
                             taker_discount: float,
                             min_quote_buffer: float = 0.0,
                             disable_on_prudence: Optional[set[str]] = None) -> None:
        """
        Configure le rabais token (ex: BNB -25%).
        maker_discount/taker_discount en pourcentage (0.25 = -25%).
        """
        exu = _norm_ex(exchange)
        self._token_policies[exu] = FeeTokenPolicy(
            token_code=token_code.strip().upper(),
            maker_discount=float(maker_discount),
            taker_discount=float(taker_discount),
            min_quote_buffer=float(min_quote_buffer),
            disable_on_prudence=set(disable_on_prudence or set())
        )

    def connect_fee_clients(self, mapping: Dict[str, Dict[str, Any]]) -> None:
        """
        Enregistre les clients CEX pour lire les frais.
        mapping: {"BINANCE":{"TT": client_tt, "TM": client_tm}, ...}
        """
        for ex, per in (mapping or {}).items():
            self._fee_clients[_norm_ex(ex)].update(per or {})

    def set_volume_provider(self, exchange: str, alias: str,
                            provider: Callable[[], Awaitable[float]]) -> None:
        """
        Provider async qui retourne le volume 30j en USD pour (exchange, alias).
        """
        self._volume_providers[(_norm_ex(exchange), alias)] = provider

    def set_balance_provider(self, exchange: str, alias: str,
                             provider: Callable[[], Awaitable[Dict[str,float]]]) -> None:
        """
        Provider async qui retourne un dict de soldes (symboles → montants) pour (exchange, alias).
        Utilisé pour vérifier l'éligibilité token (ex: BNB >= buffer).
        """
        self._balance_providers[(_norm_ex(exchange), alias)] = provider

    def on_change(self, cb: Callable[[str,str,FeeSnapshot,FeeSnapshot], None]) -> None:
        """Callback optionnel appelé quand un snapshot change: (ex, alias, old, new)."""
        self._on_change = cb

    # ---------------------------- Core refresh --------------------------------

    async def fee_sync_refresh_once(self) -> None:
        """
        Rafraîchit TOUTES les paires (exchange, alias) enregistrées.
        Concurrence bornée par cfg.fee_sync_max_concurrency (défaut 4).
        """
        sem = asyncio.Semaphore(max(1, int(getattr(self.cfg, "fee_sync_max_concurrency", 4))))
        tasks = []
        for ex, per in list(self._fee_clients.items()):
            for alias, cli in list(per.items()):
                tasks.append(asyncio.create_task(self._refresh_one_guarded(sem, ex, alias, cli)))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _refresh_one_guarded(self, sem: asyncio.Semaphore, ex: str, alias: str, cli: Any) -> None:
        async with sem:
            try:
                await self._refresh_one(ex, alias, cli)
            except Exception:
                try: SFC_FEE_SYNC_ERRORS_TOTAL.labels(ex, alias).inc()
                except Exception: pass

    async def _refresh_one(self, ex: str, alias: str, cli: Any) -> None:
        """
        Lit les frais auprès du client (signatures souples) puis
        calcule les frais "effectifs" (VIP + token) pour l'alias.
        """
        cfg = self.cfg
        initial_s = float(getattr(cfg, "fee_sync_backoff_initial_s", 1.0))
        max_s     = float(getattr(cfg, "fee_sync_backoff_max_s", 8.0))
        max_retries = int(getattr(cfg, "fee_sync_max_retries", 3))
        jitter_s  = float(getattr(cfg, "fee_sync_jitter_s", 0.2))

        async def _call_api():
            # Normalisation des signatures: get_trading_fees / get_fees / maker+taker séparés
            if hasattr(cli, "get_trading_fees") and callable(getattr(cli, "get_trading_fees")):
                return await cli.get_trading_fees(alias=alias)
            if hasattr(cli, "get_fees") and callable(getattr(cli, "get_fees")):
                return await cli.get_fees(alias=alias)
            # Fallback: méthodes séparées
            mk = await cli.get_maker_fee(alias=alias) if hasattr(cli, "get_maker_fee") else 0.0
            tk = await cli.get_taker_fee(alias=alias) if hasattr(cli, "get_taker_fee") else 0.0
            return {"maker": mk, "taker": tk, "per_pair": {}}

        # Backoff avec métriques de durée
        delay = initial_s; last_exc = None
        for _ in range(max(1, max_retries)):
            try:
                t0 = time.perf_counter_ns()
                res = await _call_api()
                dt_ms = (time.perf_counter_ns() - t0) / 1e6
                try:
                    SFC_FEE_SYNC_DURATION_MS.labels(ex, alias).observe(dt_ms)
                    SFC_FEE_REFRESH_TOTAL.labels(ex, alias).inc()
                except Exception:
                    pass
                break
            except Exception as e:
                last_exc = e
                try: SFC_FEE_SYNC_ERRORS_TOTAL.labels(ex, alias).inc()
                except Exception: pass
                await asyncio.sleep(delay + random.uniform(0.0, jitter_s))
                delay = min(delay * 2.0, max_s)
        else:
            if last_exc: raise last_exc
            res = {}

        snap_new = await self._build_snapshot_from_api(ex, alias, res)
        snap_old = self._snapshots.get(ex, {}).get(alias)
        changed = self._diff_snapshots(snap_old, snap_new)

        self._snapshots.setdefault(ex, {})[alias] = snap_new
        try:
            SFC_FEE_LAST_REFRESH_TS.labels(ex, alias).set(snap_new.last_refresh_ts)
            SFC_VIP_TIER.labels(ex, alias).set(float(snap_new.vip_tier))
            SFC_TOKEN_ENABLED.labels(ex, alias).set(1.0 if snap_new.token_enabled else 0.0)
        except Exception:
            pass
        if changed:
            try: SFC_FEE_CHANGED_TOTAL.labels(ex, alias).inc()
            except Exception: pass
            if self._on_change:
                try: self._on_change(ex, alias, snap_old, snap_new)
                except Exception: pass

        # Publie quelques gauges pour lecture rapide
        try:
            SFC_FEE_EFFECTIVE_PCT.labels(ex, alias, "maker", "default", "").set(snap_new.maker_effective)
            SFC_FEE_EFFECTIVE_PCT.labels(ex, alias, "taker", "default", "").set(snap_new.taker_effective)
            for p, ft in (snap_new.per_pair or {}).items():
                SFC_FEE_EFFECTIVE_PCT.labels(ex, alias, "maker", "pair", p).set(ft.get("maker", snap_new.maker_effective))
                SFC_FEE_EFFECTIVE_PCT.labels(ex, alias, "taker", "pair", p).set(ft.get("taker", snap_new.taker_effective))
        except Exception:
            pass
        self._emit(
            "INFO", "fee_sync_done",
            exchange=ex, alias=alias,
            vip_tier=snap_new.vip_tier,
            token_enabled=snap_new.token_enabled,
            last_refresh_ts=snap_new.last_refresh_ts,
        )

    async def _build_snapshot_from_api(self, ex: str, alias: str, res: Dict[str, Any]) -> FeeSnapshot:
        """
        Construit un FeeSnapshot à partir de la réponse API et applique:
        1) VIP schedule (si disponible directement ou via provider volume + policy locale),
        2) token policy (si éligible via balance provider + prudence),
        3) per_pair overrides (si fournis par l'API).
        """
        exu = _norm_ex(ex)
        maker = _safe_f(res.get("maker", 0.0))
        taker = _safe_f(res.get("taker", 0.0))
        per_pair_raw = res.get("per_pair") or {}
        now = time.time()
        ttl_s = float(getattr(self.cfg, "FEE_SNAPSHOT_TTL_S", 600.0))

        # 1) VIP — si l’API fournit déjà un "effective_*" + "vip_tier", on les prend directement
        vip_tier = int(res.get("vip_tier", -1))
        mk_vip = _safe_f(res.get("effective_maker", -1.0))
        tk_vip = _safe_f(res.get("effective_taker", -1.0))
        if vip_tier >= 0 and mk_vip >= 0.0 and tk_vip >= 0.0:
            maker_vip, taker_vip = mk_vip, tk_vip
        else:
            # Sinon, on calcule à partir des paliers locaux + provider volume
            vol_30d = await self._maybe_fetch_volume(exu, alias)
            maker_vip, taker_vip, vip_tier = self._resolve_vip_from_schedule(exu, maker, taker, vol_30d)

        # 2) Token policy — si l’API expose déjà des "effective_*", on assume qu’ils incluent le rabais
        # Sinon, on applique notre policy si éligible (balances + prudence).
        token_enabled = False
        if (mk_vip >= 0.0 and tk_vip >= 0.0 and vip_tier >= 0 and
            "effective_maker" in res and "effective_taker" in res):
            mk_eff, tk_eff = maker_vip, taker_vip
            token_enabled = bool(res.get("token_enabled", False))
        else:
            mk_eff, tk_eff, token_enabled = await self._apply_token_policy(exu, alias, maker_vip, taker_vip)

        # 3) Per-pair overrides (si fournis par l’API) — on présume qu’ils sont déjà "effectifs"
        per_pair: Dict[str, Dict[str, float]] = {}
        for p, v in (per_pair_raw or {}).items():
            pk = _norm_pair(p)
            m = _safe_f(v.get("maker", mk_eff))
            t = _safe_f(v.get("taker", tk_eff))
            per_pair[pk] = {"maker": m, "taker": t}

        snap = FeeSnapshot(
            maker_base=maker, taker_base=taker,
            per_pair=per_pair,
            vip_tier=max(0, vip_tier),
            maker_vip=maker_vip, taker_vip=taker_vip,
            token_enabled=token_enabled,
            maker_effective=mk_eff, taker_effective=tk_eff,
            last_refresh_ts=now, src=("api" if res else "manual"),
            ttl_s=ttl_s,
        )
        return snap

    async def _maybe_fetch_volume(self, ex: str, alias: str) -> Optional[float]:
        prov = self._volume_providers.get((ex, alias))
        if not prov: return None
        try:
            v = await prov()
            return float(v) if v is not None else None
        except Exception:
            return None

    # --- Event sink (optionnel) ----------------------------------------------
    def set_event_sink(self, sink):
        """Enregistre un callback pour remonter des événements au RiskManager."""
        self._event_sink = sink

    def _emit(self, level: str, message: str, **kw):
        sink = getattr(self, "_event_sink", None)
        if sink and callable(sink):
            try:
                payload = {"module": "SFC", "level": level, "message": message}
                payload.update(kw)
                sink(payload)
            except Exception:
                pass

    def _resolve_vip_from_schedule(self, ex: str, maker_base: float, taker_base: float,
                                   vol_30d_usd: Optional[float]) -> Tuple[float,float,int]:
        """
        Applique la grille VIP locale (si définie). Si pas de vol→ VIP0.
        """
        tiers = self._vip_schedules.get(ex) or []
        if not tiers or vol_30d_usd is None:
            # pas de policy → on garde les base fees en VIP0
            return maker_base, taker_base, 0
        tier_idx = 0
        for i, t in enumerate(tiers):
            if vol_30d_usd >= t.min_30d_usd:
                tier_idx = i
            else:
                break
        t = tiers[tier_idx]
        # Certains venues ont des base fees ≠ schedule ; on prend le MIN
        mk = min(maker_base, t.maker) if maker_base > 0 else t.maker
        tk = min(taker_base, t.taker) if taker_base > 0 else t.taker
        return mk, tk, tier_idx

    async def _apply_token_policy(self, ex: str, alias: str,
                                  maker_in: float, taker_in: float,
                                  prudence_key: Optional[str] = None) -> Tuple[float,float,bool]:
        """
        Applique un rabais token si policy connue et ressources suffisantes.
        Si prudence_key ∈ disable_on_prudence → rabais désactivé.
        Si balance_provider renvoie un solde token >= buffer → rabais activé.
        """
        pol = self._token_policies.get(ex)
        if not pol or not pol.token_code:
            return maker_in, taker_in, False
        if prudence_key and prudence_key in pol.disable_on_prudence:
            return maker_in, taker_in, False

        # Vérifie le buffer (via balance provider)
        prov = self._balance_providers.get((ex, alias))
        if prov:
            try:
                balances = await prov()  # ex: {"USDC":..., "BNB":..., "EUR":...}
            except Exception:
                balances = {}
        else:
            balances = {}

        # Si pas d’info balance → on applique quand même ? Par défaut: non.
        # (évite un "rabais fantôme" si l’exchange n’a pas activé l’option)
        token_bal = _safe_f(balances.get(pol.token_code, 0.0), 0.0)
        quote_buf = _safe_f(pol.min_quote_buffer, 0.0)
        if token_bal <= 0.0 and quote_buf > 0.0:
            return maker_in, taker_in, False

        mk = maker_in * (1.0 - float(pol.maker_discount))
        tk = taker_in * (1.0 - float(pol.taker_discount))
        return mk, tk, True

    # ----------------------------- Lecture ------------------------------------

    def get_snapshot(self, ex: str, alias: str) -> Optional[FeeSnapshot]:
        return self._snapshots.get(_norm_ex(ex), {}).get(alias)

    def get_fees(self, ex: str, alias: str, pair: Optional[str]=None) -> Tuple[float,float]:
        """
        Frais EFFECTIFS (maker,taker) si snapshot frais; sinon (0,0).
        """
        snap = self.get_snapshot(ex, alias)
        if not snap: return 0.0, 0.0
        return snap.effective_for("maker", pair), snap.effective_for("taker", pair)

    def get_effective_fees(self, ex: str, alias: str, *, pair: Optional[str]=None,
                           prudence_key: Optional[str]=None) -> Tuple[float,float]:
        """
        Version "sécurisée": si snapshot expiré (TTL), retourne la base, sinon les effectifs.
        Si prudence_key est fournie et qu’une policy token existe mais non appliquée,
        on peut (optionnel) souhaiter recalculer un rabais — ici on évite pour la prédictibilité.
        """
        snap = self.get_snapshot(ex, alias)
        if not snap: return 0.0, 0.0
        if not snap.is_fresh():
            # fallback: base (sans surprises)
            mk = snap.per_pair.get(_norm_pair(pair), {}).get("maker", snap.maker_base) if pair else snap.maker_base
            tk = snap.per_pair.get(_norm_pair(pair), {}).get("taker", snap.taker_base) if pair else snap.taker_base
            return mk, tk
        return snap.effective_for("maker", pair), snap.effective_for("taker", pair)

    # ---------------------------- Coût total ----------------------------------

    def leg_fee_pct(self, ex: str, alias: str, *, pair: Optional[str], role: str) -> float:
        mk, tk = self.get_effective_fees(ex, alias, pair=pair)
        return float(mk if (role or "taker").lower() == "maker" else tk)

    def leg_cost_pct(self, ex: str, alias: str, pair: str, side: str, role: str,
                     *, slippage_kind: Optional[str]=None, default_slippage: float=0.0) -> float:
        k = (slippage_kind or getattr(self.cfg, "sfc_slippage_source","ewma")).lower()
        slip = self.get_slippage(ex, pair, side, kind=k, default=default_slippage)
        fee  = self.leg_fee_pct(ex, alias, pair=pair, role=role)
        return float(max(0.0, slip) + max(0.0, fee))

    def get_total_cost_pct(
            self,
            route_or_pair,
            *,
            buy_leg: Optional[Dict[str, Any]] = None,
            sell_leg: Optional[Dict[str, Any]] = None,
            side: Optional[str] = None,
            size_quote: Optional[float] = None,
            slippage_kind: Optional[str] = None,
            prudence_key: Optional[str] = None,
    ) -> float:
        """
        Coût total (frais + slippage estimé) en fraction (ex: 0.0012 = 12 bps).

        Forme A (RM):
            get_total_cost_pct(route, side="TT"|"TM"|"MM", size_quote=..., slippage_kind="ewma")
            route = {"buy_ex":..., "sell_ex":..., "base":"BTC", "quote":"USDC", (opt) buy_alias, sell_alias}

            - TT : buy=taker / sell=taker (aliases défaut "TT"/"TT")
            - TM : buy=taker / sell=maker (aliases défaut "TT"/"TM")
            - MM : buy=maker / sell=maker (aliases défaut "MM"/"MM")

            Les champs buy_alias/sell_alias dans route ont priorité s'ils sont fournis.

        Forme B (legacy):
            get_total_cost_pct("BTCUSDC", buy_leg={ex,alias,role}, sell_leg={...}, slippage_kind="ewma")
        """
        k = (slippage_kind or getattr(self.cfg, "sfc_slippage_source", "ewma")).lower()
        size_q = float(size_quote or 0.0)
        prud = str(prudence_key or "NORMAL")

        # ---------- Forme A: route ----------
        if isinstance(route_or_pair, dict):
            route = dict(route_or_pair or {})
            base = str(route.get("base", "")).upper()
            quote = str(route.get("quote", "")).upper()
            pair = (base + quote).replace("-", "")

            buy_ex = _norm_ex(route.get("buy_ex"))
            sell_ex = _norm_ex(route.get("sell_ex"))
            side_u = (side or "TT").upper()

            if side_u == "TT":
                buy_role, sell_role = "taker", "taker"
                buy_alias = route.get("buy_alias") or "TT"
                sell_alias = route.get("sell_alias") or "TT"
            elif side_u == "TM":
                buy_role, sell_role = "taker", "maker"
                buy_alias = route.get("buy_alias") or "TT"
                sell_alias = route.get("sell_alias") or "TM"
            elif side_u == "MM":
                buy_role, sell_role = "maker", "maker"
                buy_alias = route.get("buy_alias") or "MM"
                sell_alias = route.get("sell_alias") or "MM"
            else:
                # fallback sûr
                buy_role, sell_role = "taker", "taker"
                buy_alias = route.get("buy_alias") or "TT"
                sell_alias = route.get("sell_alias") or "TT"

            c_buy = self.leg_cost_pct(
                exchange=buy_ex, alias=str(buy_alias).upper(), role=buy_role,
                pair=pair, side="buy", size_quote=size_q,
                prudence_key=prud, slippage_kind=k
            )
            c_sell = self.leg_cost_pct(
                exchange=sell_ex, alias=str(sell_alias).upper(), role=sell_role,
                pair=pair, side="sell", size_quote=size_q,
                prudence_key=prud, slippage_kind=k
            )
            return float(max(0.0, c_buy) + max(0.0, c_sell))

        # ---------- Forme B: pair + legs (legacy) ----------
        pair = str(route_or_pair or "").replace("-", "").upper()
        if not isinstance(buy_leg, dict) or not isinstance(sell_leg, dict):
            buy_leg = buy_leg or {"ex": "", "alias": "TT", "role": "taker"}
            sell_leg = sell_leg or {"ex": "", "alias": "TT", "role": "taker"}

        c_buy = self.leg_cost_pct(
            exchange=_norm_ex(buy_leg.get("ex")),
            alias=str(buy_leg.get("alias", "TT")).upper(),
            role=str(buy_leg.get("role", "taker")).lower(),
            pair=pair, side="buy", size_quote=size_q,
            prudence_key=prud, slippage_kind=k
        )
        c_sell = self.leg_cost_pct(
            exchange=_norm_ex(sell_leg.get("ex")),
            alias=str(sell_leg.get("alias", "TT")).upper(),
            role=str(sell_leg.get("role", "taker")).lower(),
            pair=pair, side="sell", size_quote=size_q,
            prudence_key=prud, slippage_kind=k
        )
        return float(max(0.0, c_buy) + max(0.0, c_sell))

    def set_reality_check_callback(self, cb: Callable[[Dict[str, Any]], Any]) -> None:
        """Optionnel: callback appelé si déviation > seuil_bps."""
        self._rc_cb = cb

    def on_fill_fee_reality_check(self, fill: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compare fees payées vs théoriques. Retourne un dict avec déviation en bps.
        Champs tolérés: exchange, alias, symbol, side, role/liquidity, quote_qty/usdc_qty,
                        fee_quote/fee_usdc.
        """
        ex = (_norm_ex(fill.get("exchange")) or "UNKNOWN")
        alias = str(fill.get("alias") or "TT").upper()
        sym = _norm_pair(fill.get("symbol") or "")
        side = str(fill.get("side") or "").upper()

        # notional quote
        q = fill.get("quote_qty")
        if q is None and fill.get("usdc_qty") is not None:
            q = fill.get("usdc_qty")
        notional = max(0.0, _safe_f(q, 0.0))

        # fee payée en quote
        fee_paid = _safe_f(fill.get("fee_quote", fill.get("fee_usdc", 0.0)), 0.0)

        role = str(fill.get("liquidity", fill.get("role", "taker"))).lower()
        if role not in ("maker", "taker"):
            role = "taker"

        # frais EFFECTIFS (incl. VIP/token/override)
        mk, tk = self.get_effective_fees(ex, alias, pair=sym, prudence_key="NORMAL")
        exp_fee = (tk if role == "taker" else mk) * notional

        denom = notional if notional > 0 else 1e-12
        dev_bps = abs(fee_paid - exp_fee) / denom * 1e4

        threshold = float(getattr(self.cfg, "fee_reality_check_threshold_bps", 3.0))
        exceeded = bool(dev_bps > threshold)

        res = {
            "exchange": ex, "alias": alias, "symbol": sym, "side": side, "role": role,
            "notional": notional, "expected_fee": exp_fee, "paid_fee": fee_paid,
            "deviation_bps": dev_bps, "threshold_bps": threshold, "exceeded": exceeded,
        }

        if exceeded and callable(getattr(self, "_rc_cb", None)):
            try:
                self._rc_cb(res)
            except Exception:
                pass

        return res

    # --------------------------- Utilitaires ----------------------------------

    def _diff_snapshots(self, old: Optional[FeeSnapshot], new: FeeSnapshot) -> bool:
        if old is None: return True
        if not _nearly_equal(old.maker_effective, new.maker_effective): return True
        if not _nearly_equal(old.taker_effective, new.taker_effective): return True
        if old.vip_tier != new.vip_tier: return True
        if old.token_enabled != new.token_enabled: return True
        # Compare per_pair (clés + valeurs)
        kp_old = set((k, round(v.get("maker",0.0),10), round(v.get("taker",0.0),10)) for k,v in (old.per_pair or {}).items())
        kp_new = set((k, round(v.get("maker",0.0),10), round(v.get("taker",0.0),10)) for k,v in (new.per_pair or {}).items())
        return kp_old != kp_new

    def get_status(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"exchanges": {}}
        for ex, per in self._snapshots.items():
            exd = out["exchanges"].setdefault(ex, {})
            for alias, snap in per.items():
                exd[alias] = {
                    "base": {"maker": snap.maker_base, "taker": snap.taker_base},
                    "vip": {"tier": snap.vip_tier, "maker": snap.maker_vip, "taker": snap.taker_vip},
                    "effective": {"maker": snap.maker_effective, "taker": snap.taker_effective},
                    "token_enabled": snap.token_enabled,
                    "per_pair": snap.per_pair,
                    "last_refresh_ts": snap.last_refresh_ts,
                    "ttl_s": snap.ttl_s,
                    "fresh": snap.is_fresh(),
                }
        return out
