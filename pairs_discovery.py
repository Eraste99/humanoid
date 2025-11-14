# -*- coding: utf-8 -*-
"""
Découverte dynamique des paires **USDC** et **EUR** communes entre
Binance / Coinbase Exchange / Bybit (spot uniquement).

Améliorations (patch):
- Gestion spécifique **429 Rate Limit** avec support de `Retry-After` (seconds)
  et exception dédiée `RateLimitBackoff` consommable par les watchdogs.
- Helper numérique `fnum` (robuste à None/NaN/inf/str) pour toutes conversions.
- Bybit: fallback sur `turnover24h` (souvent en **quote**) si dispo, sinon `lastPrice*volume24h`.
- Option **facultative** d'FX pour le *ranking global* entre EUR et USDC (`eur_usdc_fx`).
  *Les scores par combo restent dans leur devise d'origine; seule la comparaison
  inter-devises pour `top_pairs` peut être normalisée si `eur_usdc_fx` est fourni.*

Compatibilité :
- API inchangée (retourne `(pair_mapping, top_pairs)`).
- `discover_usdc_pairs(...)` reste un alias rétro-compatible.
"""
from __future__ import annotations
import asyncio
import math
from typing import Dict, List, Tuple, Optional, Set, Any
from typing import Dict, List, Set, Tuple
import aiohttp
from typing import Dict, List, Tuple, Optional, Set
import aiohttp, asyncio, random, math
from asyncio_throttle import Throttler
from modules.rm_compat import getattr_int, getattr_float, getattr_str, getattr_bool, getattr_dict, getattr_list

try:
    from asyncio_throttle import Throttler  # pip install asyncio-throttle
except Exception:
    from modules.utils.rate_limiter import AsyncRateLimiter as _RL
    class Throttler:  # shim compatible "async with Throttler(...)"
        def __init__(self, rate_limit: float) -> None:
            self._rl = _RL(rate=float(rate_limit), capacity=max(1, int(rate_limit)))
        async def __aenter__(self):
            await self._rl.acquire(kind="discovery", exchange="global")
        async def __aexit__(self, exc_type, exc, tb):
            return False


# --------------------- Endpoints publics ---------------------
# Binance
BINANCE_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"
BINANCE_24HR_TICKERS  = "https://api.binance.com/api/v3/ticker/24hr"

# Coinbase Exchange (ex-Coinbase Pro)
CB_PRODUCTS = "https://api.exchange.coinbase.com/products"
CB_STATS    = "https://api.exchange.coinbase.com/products/stats"  # 24h stats pour toutes les paires

# Bybit (v5 unifié)
BYBIT_INSTR   = "https://api.bybit.com/v5/market/instruments-info?category=spot"
BYBIT_TICKERS = "https://api.bybit.com/v5/market/tickers?category=spot"

# --------------------- Exceptions & utils ---------------------
class RateLimitBackoff(Exception):
    """À lever quand l'API renvoie un 429 avec une indication de délai.
    Le watchdog peut caler sa prochaine exécution sur `retry_after_s`.
    """
    def __init__(self, retry_after_s: float):
        super().__init__(f"Rate limited, retry after {retry_after_s}s")
        self.retry_after_s = float(retry_after_s)


def _pk(base: str, quote: str) -> str:
    return f"{base.upper()}{quote.upper()}"


def _binance_ok_quote(quote: str) -> bool:
    return quote and quote.upper() in ("USDC", "EUR")


def _cb_ok_quote(quote: str) -> bool:
    return quote and quote.upper() in ("USDC", "EUR")


def _bybit_ok_quote(quote: str) -> bool:
    return quote and quote.upper() in ("USDC", "EUR")


def _norm_quote_min(quote: str, min_usdc: float, min_eur: float) -> float:
    return float(min_usdc if (quote or "USDC").upper() == "USDC" else min_eur)


def fnum(x: Any, default: float = 0.0) -> float:
    """Conversion flottante robuste (None/str/NaN/inf -> default)."""
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default


async def _fetch_json(session: aiohttp.ClientSession, url: str, *, retries: int = 3, timeout: int = 15):
    """GET JSON avec gestion 429/Retry-After + retry exponentiel pour erreurs transitoires."""
    last: Optional[Exception] = None
    for k in range(retries):
        try:
            async with session.get(url, timeout=timeout) as r:
                # 429: respecter Retry-After si fourni (secondes)
                if r.status == 429:
                    ra = r.headers.get("Retry-After")
                    delay = fnum(ra, default=0.0) if ra is not None else 0.0
                    if delay > 0:
                        # Propage au surveillant pour ajuster la cadence
                        raise RateLimitBackoff(delay)
                    # Sinon: backoff exponentiel local
                    await asyncio.sleep(1 * (2 ** k))
                    continue

                r.raise_for_status()
                return await r.json()
        except RateLimitBackoff:
            # Propage vers l'appelant (watchdog pourra s'aligner sur le délai)
            raise
        except aiohttp.ClientResponseError as e:
            # Réessaie sur 5xx
            if e.status in {500, 502, 503, 504} and k < retries - 1:
                await asyncio.sleep(1 * (2 ** k))
                last = e
                continue
            last = e
            break
        except Exception as e:
            last = e
            if k < retries - 1:
                await asyncio.sleep(1 * (2 ** k))
                continue
            break
    raise RuntimeError(f"GET {url} failed after {retries} retries: {last}")


# pairs_discovery.py — helpers importables par boot



def build_universe_partition(
    all_pairs_by_combo: Dict[str, List[str]],
    combo_shares: Dict[str, float],
    tier_targets: Dict[str, int],
) -> Dict[str, Dict[str, Set[str]]]:
    """
    Fabrique {tier: {combo: set(pairs)}} selon les cibles de tiers et le partage par combo.
    Hypothèse: all_pairs_by_combo[combo] est déjà trié (volume/qualité).
    """
    tiers = {k: {} for k in ("CORE","PRIMARY","AUDITION","SANDBOX")}
    for tier, target in tier_targets.items():
        for combo, share in combo_shares.items():
            n = max(0, int(target * share))
            tiers[tier][combo] = set(all_pairs_by_combo.get(combo, [])[:n])
    return tiers

def compute_diffs(current: Set[str], nxt: Set[str]) -> Tuple[Set[str], Set[str]]:
    """
    Calcule (add, remove) entre deux ensembles.
    """
    add = nxt - current
    rem = current - nxt
    return add, rem



# --------------------- Loaders par CEX ---------------------
async def _load_binance(session: aiohttp.ClientSession) -> Tuple[Dict[str, dict], Dict[str, float]]:
    """
    Retourne:
      meta: { "BASEQUOTE": {...} } pour symboles TRADING avec quote in {USDC, EUR}
      volq: { "BASEQUOTE": quoteVolume_24h_float }
    """
    exinfo, tickers = await asyncio.gather(
        _fetch_json(session, BINANCE_EXCHANGE_INFO),
        _fetch_json(session, BINANCE_24HR_TICKERS),
    )
    symbols = [s for s in exinfo.get("symbols", []) if (s or {}).get("status") == "TRADING"]
    meta: Dict[str, dict] = {}
    for s in symbols:
        base = (s or {}).get("baseAsset"); quote = (s or {}).get("quoteAsset")
        sym  = (s or {}).get("symbol")
        if not (_binance_ok_quote(quote) and base and sym):
            continue
        meta[sym] = {"base": base.upper(), "quote": quote.upper(), "symbol": sym}

    volq: Dict[str, float] = {}
    for t in tickers or []:
        sym = (t or {}).get("symbol")
        if sym in meta:
            v = fnum((t or {}).get("quoteVolume"), 0.0)
            volq[sym] = v
    return meta, volq


async def _load_coinbase(session: aiohttp.ClientSession) -> Tuple[Dict[str, dict], Dict[str, float]]:
    """
    Coinbase Exchange:
      - products: /products -> [{id:"ETH-USDC", base_currency, quote_currency, status}]
      - stats:    /products/stats -> {"ETH-USDC":{"last":"..","open":"..","volume":"..", ...}, ...}
    On calcule le volume **quote** ≈ volume_base_24h * last_price.
    """
    products, stats = await asyncio.gather(
        _fetch_json(session, CB_PRODUCTS),
        _fetch_json(session, CB_STATS),
    )

    # Filtre produits "online"
    meta: Dict[str, dict] = {}
    for p in products or []:
        try:
            status = (p.get("status") or "").lower()
            base = p.get("base_currency")
            quote = p.get("quote_currency")
            pid = p.get("id")  # ex: "ETH-USDC"
            if status not in ("online", "active", "trading"):
                continue
            if not (_cb_ok_quote(quote) and base and pid):
                continue
            meta[pid] = {"base": base.upper(), "quote": quote.upper(), "product_id": pid}
        except Exception:
            continue

    volq: Dict[str, float] = {}
    st_all = stats or {}
    # st_all est un dict keyed par "product_id"
    for pid, st in (st_all or {}).items():
        if pid not in meta:
            continue
        last = fnum((st or {}).get("last"), 0.0)  # en quote
        vol_base = fnum((st or {}).get("volume"), 0.0)  # base 24h
        volq[pid] = (last * vol_base) if last > 0 else 0.0

    return meta, volq


async def _load_bybit(session: aiohttp.ClientSession) -> Tuple[Dict[str, dict], Dict[str, float]]:
    """
    Bybit v5:
      - instruments-info (spot): quoteCoin/baseCoin/symbol/status
      - tickers (spot): lastPrice, volume24h (base), turnover24h (souvent en quote)
    On calcule le volume **quote** ≈ `turnover24h` si dispo/valide, sinon `lastPrice * volume24h`.
    """
    ins, tks = await asyncio.gather(
        _fetch_json(session, BYBIT_INSTR),
        _fetch_json(session, BYBIT_TICKERS),
    )

    meta: Dict[str, dict] = {}
    for it in ((ins or {}).get("result", {}) or {}).get("list", []) or []:
        try:
            if (it.get("category") or "").lower() != "spot":
                continue
            symbol = it.get("symbol")  # ex: "ETHUSDC"
            base = it.get("baseCoin")
            quote = it.get("quoteCoin")
            status = (it.get("status") or "").lower()  # "Trading" vs ...
            if status not in ("trading", "online", "tradable"):
                continue
            if not (_bybit_ok_quote(quote) and base and symbol):
                continue
            meta[symbol] = {"base": base.upper(), "quote": quote.upper(), "symbol": symbol}
        except Exception:
            continue

    volq: Dict[str, float] = {}
    for tk in ((tks or {}).get("result", {}) or {}).get("list", []) or []:
        sym = (tk or {}).get("symbol")
        if sym not in meta:
            continue
        # Priorité au turnover (déjà en quote) si valide
        turnover_q = fnum((tk or {}).get("turnover24h"), -1.0)
        if turnover_q > 0:
            volq[sym] = turnover_q
            continue
        last = fnum((tk or {}).get("lastPrice"), 0.0)
        vol_base = fnum((tk or {}).get("volume24h"), 0.0)
        volq[sym] = (last * vol_base) if last > 0 else 0.0
    return meta, volq


# --------------------- Discovery tri-CEX ---------------------

async def discover_pairs_3cex(
    cfg,
    top_n: int = 80,
    *,
    # filtres optionnels (priorité aux args > cfg.discovery)
    allowlist: Optional[List[str]] = None,
    denylist: Optional[List[str]] = None,
    enabled_exchanges: Optional[List[str]] = None,   # ["BINANCE","COINBASE","BYBIT"]
    # seuils quote-aware (si None -> dérivés de cfg.discovery.min_24h_volume_usd)
    min_quote_volume_usdc: Optional[float] = None,
    min_quote_volume_eur: Optional[float]  = None,
    # comparabilité EUR->USDC pour le tri global
    eur_usdc_fx: Optional[float] = None,
) -> Tuple[Dict[str, Dict[str, object]], List[str]]:
    """
    Découverte des paires pilotée par cfg.discovery.* + scoring combos.
    - http_timeout_s, max_inflight_requests, retry_policy (base_ms,max_ms,max_attempts,jitter)
    - quotes_allowed (doit contenir USDC/EUR), whitelist/blacklist, min_24h_volume_usd (base)
    - Retourne:
        pair_mapping: {
          "BTCUSDC": {
            "quote": "USDC",
            "binance": "BTCUSDC" | None,
            "coinbase": "BTC-USDC" | None,
            "bybit": "BTCUSDC" | None,
            "volumes": {"BINANCE": float, "COINBASE": float, "BYBIT": float},
            "combos":  {"BINANCE/COINBASE": float, "BINANCE/BYBIT": float, "BYBIT/COINBASE": float},
          }, ...
        }
        top_pairs: liste des pk triés par meilleur score combo (EUR upscalé par fx)
    """

    from collections import defaultdict

    # --------------------------
    # Helpers locaux autonomes
    # --------------------------
    def _pk(base: str, quote: str) -> str:
        return f"{str(base).upper()}{str(quote).upper()}"

    def fnum(x, default=0.0) -> float:
        try:
            if x is None: return float(default)
            return float(x)
        except Exception:
            return float(default)

    def _norm_quote_min(quote: str, thr_usdc: float, thr_eur: float) -> float:
        return float(thr_usdc if str(quote).upper() == "USDC" else thr_eur)

    def _quote_from_pk(pk: str) -> str:
        if pk.endswith("USDC"): return "USDC"
        if pk.endswith("EUR"):  return "EUR"
        return "USDC"

    # --------------------------
    # 1) Paramètres de config
    # --------------------------
    d = cfg.discovery
    http_timeout_s = getattr_int(d, "http_timeout_s", 10)
    max_inflight   = getattr_int(d, "max_inflight_requests", 8)
    rp_cfg         = dict(getattr(d, "retry_policy", {"base_ms":500,"max_ms":20_000,"max_attempts":5,"jitter":1}))
    quotes_allowed = set(q.upper() for q in getattr(d, "quotes_allowed", ["USDC","EUR"]))
    # Seuils: si non fournis, dériver de min_24h_volume_usd (EUR = 0.3x par défaut comme ta version)
    # Seuils: si non fournis en args, on lit la conf; sinon on dérive d’un base_min
    base_min = getattr_float(d, "min_24h_volume_usd", 100_000.0)

    conf_usdc = getattr(d, "min_quote_volume_usdc", None)
    conf_eur = getattr(d, "min_quote_volume_eur", None)

    thr_usdc = float(
        min_quote_volume_usdc
        if min_quote_volume_usdc is not None else
        (conf_usdc if conf_usdc is not None else base_min)
    )
    thr_eur = float(
        min_quote_volume_eur
        if min_quote_volume_eur is not None else
        (conf_eur if conf_eur is not None else max(1.0, 0.30 * base_min))
    )

    # Listes allow/deny (args > cfg)
    whitelist = set(x.upper() for x in (allowlist if allowlist is not None else getattr(d, "whitelist", [])))
    blacklist = set(x.upper() for x in (denylist  if denylist  is not None else getattr(d, "blacklist", [])))

    # Exchanges activés
    enabled = [e.upper() for e in (enabled_exchanges or cfg.g.enabled_exchanges)]
    enabled = [e for e in enabled if e in ("BINANCE", "COINBASE", "BYBIT")]
    if len(enabled) < 2:
        return {}, []

    # --------------------------
    # 2) HTTP client + throttler
    # --------------------------
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=http_timeout_s), headers={"User-Agent": "PairsDiscovery/3"}) as session:
        limiter = Throttler(rate_limit=max_inflight, period=1.0)

        async def _fetch_json(url: str):
            base = float(rp_cfg.get("base_ms", 500)) / 1000.0
            cap  = float(rp_cfg.get("max_ms", 20_000)) / 1000.0
            attempts = int(rp_cfg.get("max_attempts", 5))
            jitter   = bool(rp_cfg.get("jitter", 1))
            last = None
            for k in range(attempts):
                try:
                    async with limiter:
                        async with session.get(url) as r:
                            if r.status == 429:
                                ra = r.headers.get("Retry-After")
                                if ra:
                                    await asyncio.sleep(float(ra))
                                    continue
                            r.raise_for_status()
                            return await r.json()
                except Exception as e:
                    last = e
                    sleep_s = min(cap, base * (2 ** k))
                    if jitter:
                        sleep_s *= random.random()
                    await asyncio.sleep(sleep_s)
            raise last or RuntimeError(f"discovery fetch failed: {url}")

        # --------------------------
        # 3) Loaders par exchange
        # --------------------------
        async def _load_binance():
            # symbols + 24h tickers -> quoteVolume
            try:
                info = await _fetch_json("https://api.binance.com/api/v3/exchangeInfo")
                tick = await _fetch_json("https://api.binance.com/api/v3/ticker/24hr")
            except Exception:
                return {}, {}
            meta = {}
            for s in (info or {}).get("symbols", []):
                if str(s.get("status","")).upper() != "TRADING":
                    continue
                base = s.get("baseAsset"); quote = s.get("quoteAsset")
                if not base or not quote: continue
                if quote.upper() not in quotes_allowed:  # garde USDC/EUR seulement
                    continue
                meta[s.get("symbol")] = {"base": base, "quote": quote}
            volq = {}
            for t in (tick or []):
                sym = t.get("symbol")
                if sym in meta:
                    qv = t.get("quoteVolume")  # déjà en quote-ccy
                    volq[sym] = fnum(qv, 0.0)
            return meta, volq

        async def _load_coinbase():
            # products + stats (24h) -> approx quoteVolume = last * base_volume
            try:
                products = await _fetch_json("https://api.exchange.coinbase.com/products")
            except Exception:
                return {}, {}
            # fetch stats par produit avec limite de concurrence
            stats = {}
            async def _load_stat(pid: str):
                try:
                    s = await _fetch_json(f"https://api.exchange.coinbase.com/products/{pid}/stats")
                    stats[pid] = s
                except Exception:
                    stats[pid] = None

            tasks = []
            sem = asyncio.Semaphore(max_inflight)
            async def _task(pid):
                async with sem:
                    await _load_stat(pid)

            pids = []
            for p in (products or []):
                quote = (p or {}).get("quote_currency","").upper()
                base  = (p or {}).get("base_currency","").upper()
                if not base or not quote:
                    continue
                if quote not in quotes_allowed:
                    continue
                pid = p.get("id") or f"{base}-{quote}"
                pids.append(pid)

            await asyncio.gather(*( _task(pid) for pid in pids ))

            meta = {}
            volq = {}
            for p in (products or []):
                base = (p or {}).get("base_currency","")
                quote= (p or {}).get("quote_currency","")
                pid  = (p or {}).get("id") or f"{base}-{quote}"
                if not base or not quote or quote.upper() not in quotes_allowed:
                    continue
                meta[pid] = {"base": base, "quote": quote, "product_id": pid}
                st = stats.get(pid) or {}
                # Coinbase stats fields: last, open, volume (BASE units)
                last = fnum(st.get("last"), 0.0) or fnum(st.get("last_price"), 0.0)
                base_vol = fnum(st.get("volume"), 0.0)
                volq[pid] = base_vol * last  # approx quote-volume
            return meta, volq

        async def _load_bybit():
            # spot symbols + 24h ticker
            try:
                syms = await _fetch_json("https://api.bybit.com/spot/v3/public/symbols")
                t24  = await _fetch_json("https://api.bybit.com/spot/quote/v1/ticker/24hr")
            except Exception:
                return {}, {}
            meta = {}
            for s in (syms or {}).get("result", {}).get("list", []):
                base = s.get("baseCoin"); quote = s.get("quoteCoin")
                if not base or not quote:
                    continue
                if quote.upper() not in quotes_allowed:
                    continue
                exsym = s.get("name") or f"{base}{quote}"
                meta[exsym] = {"base": base, "quote": quote}
            volq = {}
            for t in (t24 or {}).get("result", []):
                sym = t.get("symbol")
                if sym in meta:
                    # Bybit 24h ticker expose "qv" (quote volume) parfois, sinon calc:
                    qv = t.get("qv")
                    if qv is None:
                        last = fnum(t.get("lastPrice"), 0.0)
                        base_vol = fnum(t.get("volume"), 0.0)
                        qv = base_vol * last
                    volq[sym] = fnum(qv, 0.0)
            return meta, volq

        # --------------------------
        # 4) Collecte parallélisée
        # --------------------------
        # si un exchange n'est pas activé → renvoyer vide
        (b_meta, b_volq) = ({}, {})
        (c_meta, c_volq) = ({}, {})
        (y_meta, y_volq) = ({}, {})

        tasks = []
        if "BINANCE" in enabled:  tasks.append(_load_binance())
        if "COINBASE" in enabled: tasks.append(_load_coinbase())
        if "BYBIT" in enabled:    tasks.append(_load_bybit())

        res = await asyncio.gather(*tasks, return_exceptions=True)
        i = 0
        if "BINANCE" in enabled:
            if isinstance(res[i], Exception): (b_meta, b_volq) = ({}, {})
            else: (b_meta, b_volq) = res[i]
            i += 1
        if "COINBASE" in enabled:
            if isinstance(res[i], Exception): (c_meta, c_volq) = ({}, {})
            else: (c_meta, c_volq) = res[i]
            i += 1
        if "BYBIT" in enabled:
            if isinstance(res[i], Exception): (y_meta, y_volq) = ({}, {})
            else: (y_meta, y_volq) = res[i]

    # --------------------------
    # 5) Normalisation & filtres
    # --------------------------
    allow: Optional[Set[str]] = {a.upper() for a in (allowlist or [])} or (set(x.upper() for x in whitelist) if whitelist else None)
    deny: Set[str] = set(x.upper() for x in blacklist)

    def _fill_maps(meta: Dict[str, dict], volq: Dict[str, float], ex: str):
        sym_map: Dict[str, str] = {}
        vol_map: Dict[str, float] = {}
        for sym, m in (meta or {}).items():
            base = (m or {}).get("base"); quote = (m or {}).get("quote")
            if not base or not quote:
                continue
            if quote and quote.upper() not in quotes_allowed:
                continue
            if allow is not None and base.upper() not in allow:
                continue
            if base.upper() in deny:
                continue
            pk = _pk(base, quote)
            v = fnum(volq.get(sym, 0.0), 0.0)
            # Coinbase garde product_id hyphéné pour mapping externe
            sym_map[pk] = sym
            vol_map[pk] = v
        return sym_map, vol_map

    b_syms, b_vols = _fill_maps(b_meta, b_volq, "BINANCE")
    c_syms, c_vols = _fill_maps(c_meta, c_volq, "COINBASE")
    y_syms, y_vols = _fill_maps(y_meta, y_volq, "BYBIT")

    # --------------------------
    # 6) pair_mapping enrichi
    # --------------------------
    pair_mapping: Dict[str, Dict[str, object]] = {}

    def _ensure(pk: str, quote: str):
        if pk not in pair_mapping:
            pair_mapping[pk] = {
                "quote": quote,
                "binance": None, "coinbase": None, "bybit": None,
                "volumes": {"BINANCE": 0.0, "COINBASE": 0.0, "BYBIT": 0.0},
                "combos":  {"BINANCE/COINBASE": 0.0, "BINANCE/BYBIT": 0.0, "BYBIT/COINBASE": 0.0},
            }

    for pk, sym in b_syms.items():
        _ensure(pk, _quote_from_pk(pk))
        pair_mapping[pk]["binance"] = sym
        pair_mapping[pk]["volumes"]["BINANCE"] = fnum(b_vols.get(pk, 0.0), 0.0)
    for pk, sym in c_syms.items():
        _ensure(pk, _quote_from_pk(pk))
        pair_mapping[pk]["coinbase"] = sym
        pair_mapping[pk]["volumes"]["COINBASE"] = fnum(c_vols.get(pk, 0.0), 0.0)
    for pk, sym in y_syms.items():
        _ensure(pk, _quote_from_pk(pk))
        pair_mapping[pk]["bybit"] = sym
        pair_mapping[pk]["volumes"]["BYBIT"] = fnum(y_vols.get(pk, 0.0), 0.0)

    # --------------------------
    # 7) Scoring des combos
    # --------------------------
    def _score_combo(pk: str, a: str, b: str) -> float:
        quote = (pair_mapping[pk]["quote"] or "USDC").upper()
        thr = _norm_quote_min(quote, thr_usdc, thr_eur)
        va = fnum((pair_mapping[pk]["volumes"] or {}).get(a, 0.0), 0.0)
        vb = fnum((pair_mapping[pk]["volumes"] or {}).get(b, 0.0), 0.0)
        if va >= thr and vb >= thr:
            return float(min(va, vb))
        return 0.0

    combos = [("BINANCE","COINBASE"), ("BINANCE","BYBIT"), ("BYBIT","COINBASE")]
    combos = [c for c in combos if all(x in enabled for x in c)]

    best_scores: Dict[str, float] = {}
    for pk in list(pair_mapping.keys()):
        for a, b in combos:
            has_a = pair_mapping[pk].get(a.lower()) is not None if a != "COINBASE" else pair_mapping[pk].get("coinbase") is not None
            has_b = pair_mapping[pk].get(b.lower()) is not None if b != "COINBASE" else pair_mapping[pk].get("coinbase") is not None
            if not (has_a and has_b):
                continue
            s = _score_combo(pk, a, b)
            key = f"{a}/{b}"
            pair_mapping[pk]["combos"][key] = s
            if s > 0:
                best_scores[pk] = max(best_scores.get(pk, 0.0), s)

    # --------------------------
    # 8) Tri global (EUR → USDC via FX)
    # --------------------------
    def _global_score(pk: str) -> float:
        s = best_scores.get(pk, 0.0)
        if eur_usdc_fx and (pair_mapping.get(pk, {}).get("quote") == "EUR"):
            return s * float(eur_usdc_fx)
        return s

    ranked = sorted(best_scores.keys(), key=_global_score, reverse=True)
    top_pairs = ranked[: max(0, int(top_n))]

    return pair_mapping, top_pairs

# ---------- Alias de compat pour l'ancien nom ----------
async def discover_usdc_pairs(
    top_n: int = 80,
    *,
    min_quote_volume: float = 100_000.0,            # rétro-compat (USDC) — voir ci-dessous
    allowlist: Optional[List[str]] = None,
    denylist: Optional[List[str]] = None,
    include_usdt_fallback: bool = False,            # ignoré (on ne fait plus USDT)
    enabled_exchanges: Optional[List[str]] = None,  # nouveau param
    min_quote_volume_eur: float = 30_000.0,         # nouveau param
) -> Tuple[Dict[str, Dict[str, object]], List[str]]:
    """
    Wrapper rétro-compatible.
    - `min_quote_volume` joue le rôle de min_quote_volume_usdc.
    - `include_usdt_fallback` est ignoré (pas d’USDT).
    - Ajoute `enabled_exchanges` et `min_quote_volume_eur`.
    """
    return await discover_pairs_3cex(
        top_n=top_n,
        min_quote_volume_usdc=float(min_quote_volume),
        min_quote_volume_eur=float(min_quote_volume_eur),
        allowlist=allowlist,
        denylist=denylist,
        enabled_exchanges=enabled_exchanges,
    )


# test rapide manuel
if __name__ == "__main__":
    deny = ["PEPE","SHIB","FLOKI","BONK","WIF","DOGE"]
    try:
        pairs_map, pairs = asyncio.run(discover_pairs_3cex(
            top_n=80,
            min_quote_volume_usdc=100_000,
            min_quote_volume_eur=30_000,
            denylist=deny,
            enabled_exchanges=["BINANCE","COINBASE","BYBIT"],
            eur_usdc_fx=None,  # pas de normalisation inter‑devises par défaut
        ))
        print(f"Pairs ({len(pairs)}):", pairs[:20], "...")
        if pairs:
            k = pairs[0]
            print("Mapping example:", k, "->", pairs_map[k])
    except RateLimitBackoff as e:
        print(f"Rate limited; retry after {e.retry_after_s}s")
