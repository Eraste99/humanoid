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
import logging
import math
from types import SimpleNamespace
import time
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("pairs_discovery")


import aiohttp

# Dépendances attendues en prod: aiohttp + asyncio-throttle.
# On garde un fallback ultra-simple pour les environnements de test.
try:
    from asyncio_throttle import Throttler  # pip install asyncio-throttle
except Exception:  # pragma: no cover
    class Throttler:  # type: ignore
        """Fallback minimal: limite via semaphore (pas de lissage temporel fin)."""

        def __init__(self, rate_limit: int = 8, period: float = 1.0):
            self._sem = asyncio.Semaphore(max(1, int(rate_limit)))

        async def __aenter__(self):
            await self._sem.acquire()
            return None

        async def __aexit__(self, exc_type, exc, tb):
            self._sem.release()
            return False


from contracts.payloads import DiscoveryResult
from modules.bot_config import DiscoveryCfg

try:
    from modules.obs_metrics import (
        discovery_note_api_error,
        discovery_note_filtered,
        discovery_note_rate_limit,
        discovery_note_stage,
        discovery_observe_run_ms,
    )
except Exception:  # pragma: no cover
    def discovery_note_stage(*_, **__):
        return None


    def discovery_note_filtered(*_, **__):
        return None


    def discovery_note_api_error(*_, **__):
        return None


    def discovery_note_rate_limit(*_, **__):
        return None

    def discovery_observe_run_ms(*_, **__):
        return None

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

class _noop_async_cm:
    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc, tb):
        return False

def _pk(base: str, quote: str) -> str:
    return f"{base.upper()}{quote.upper()}"


def _binance_ok_quote(quote: str) -> bool:
    return quote and quote.upper() in ("USDC", "USDT", "EUR")


def _cb_ok_quote(quote: str) -> bool:
    return quote and quote.upper() in ("USDC", "USDT", "EUR")


def _bybit_ok_quote(quote: str) -> bool:
    return quote and quote.upper() in ("USDC", "USDT", "EUR")


def _norm_quote_min(quote: str, min_usdc: float, min_usdt: float, min_eur: float) -> float:
    q = str(quote or "USDC").upper()
    if q == "USDC": return float(min_usdc)
    if q == "USDT": return float(min_usdt)
    if q == "EUR":  return float(min_eur)
    return float(min_usdc)


def fnum(x: Any, default: float = 0.0) -> float:
    """Conversion flottante robuste (None/str/NaN/inf -> default)."""
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default


def _build_backoff_policy(retry_cfg: Optional[dict]) -> Any:

    """Adapte les formats legacy ({retries, backoff_s}) et v2 ({base_ms, max_ms, ...})."""
    try:
        try:
            from modules.retry_policy import BackoffPolicy
        except ImportError:
            from retry_policy import BackoffPolicy
    except Exception:  # pragma: no cover - fallback inert
        class _P:  # type: ignore
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        return _P()

    if not retry_cfg:
        return BackoffPolicy()

    # Format legacy léger
    if isinstance(retry_cfg, dict) and {"retries", "backoff_s"} <= set(retry_cfg):
        retries = max(1, int(retry_cfg.get("retries", 3)))
        backoff_s = float(retry_cfg.get("backoff_s", 1.0))
        return BackoffPolicy(
            min_backoff_s=backoff_s,
            max_backoff_s=backoff_s,
            budget_tries=retries,
            cap_total_s=float(retry_cfg.get("cap_total_s", 120.0)),
            full_jitter=bool(retry_cfg.get("full_jitter", True)),
        )

    # Fallback: mapping v2/v3 via helper
    return BackoffPolicy.from_cfg(retry_cfg)


async def _fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    policy,
    timeout: int,
    exchange: str,
    limiter=None,
    on_error=None,
):
    """GET JSON best-effort via retry_policy.awith_retry (gestions 429 cohérentes)."""
    try:
        try:
            from modules.retry_policy import awith_retry
        except ImportError:
            from retry_policy import awith_retry
    except Exception as e:  # pragma: no cover - infra manquante
        logger.error(f"[discovery] retry_policy missing or import failed: {e!r}")
        raise RuntimeError("retry_policy missing") from e

    def _note_error(reason: str) -> None:
        if on_error is None:
            return
        try:
            on_error(exchange, reason)
        except Exception:
            return None

    async def _op():
        async with (limiter or _noop_async_cm()):
            noted_error = False
            try:
                async with session.get(url, timeout=timeout) as r:
                    if r.status == 429:
                        discovery_note_rate_limit(exchange)
                        _note_error("api_error")
                        noted_error = True
                        ra = r.headers.get("Retry-After")
                        delay = fnum(ra, default=0.0) if ra is not None else 0.0
                        if delay > 0:
                            raise RateLimitBackoff(delay)

                    r.raise_for_status()
                    try:
                        return await r.json()
                    except Exception:
                        _note_error("parse_error")
                        noted_error = True
                        raise
            except RateLimitBackoff:
                raise
            except asyncio.TimeoutError:
                logger.error(f"[discovery] Timeout calling {exchange} {url}")
                _note_error("api_error")
                raise
            except Exception as e:
                if not noted_error:
                    logger.error(f"[discovery] Error calling {exchange} {url}: {e!r}")
                    _note_error("api_error")
                raise


    outcome = await awith_retry(_op, venue=str(exchange), policy=policy)
    if outcome.ok:
        return outcome.result

    # Ratelimit prolongé: propage RateLimitBackoff si dispo pour que le watchdog s'aligne
    if isinstance(outcome.last_exception, RateLimitBackoff):
        raise outcome.last_exception

    # Harmonise l'erreur finale
    msg = f"GET {url} failed after {outcome.attempts} attempts ({outcome.kind or 'UNKNOWN'})"
    if outcome.last_exception:
        msg += f": {outcome.last_exception!r}"
    raise RuntimeError(msg)

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

async def _load_binance(
    session: aiohttp.ClientSession,
    *,
    policy,
    timeout_s: int,
    limiter=None,
    on_api_error=None,
) -> Tuple[Dict[str, dict], Dict[str, float]]:
    """
    Retourne:
      meta: { "BASEQUOTE": {...} } pour symboles TRADING
      volq: { "BASEQUOTE": quoteVolume_24h_float }
    """
    exinfo, tickers = await asyncio.gather(
        _fetch_json(
            session,
            BINANCE_EXCHANGE_INFO,
            policy=policy,
            timeout=timeout_s,
            exchange="BINANCE",
            limiter=limiter,
            on_error=on_api_error,
        ),
        _fetch_json(
            session,
            BINANCE_24HR_TICKERS,
            policy=policy,
            timeout=timeout_s,
            exchange="BINANCE",
            limiter=limiter,
            on_error=on_api_error,
        ),
        return_exceptions=True
    )

    if isinstance(exinfo, Exception):
        msg = f"Binance exchangeInfo API failed: {exinfo!r}"
        logger.error(f"[discovery] {msg}")
        if on_api_error: on_api_error("BINANCE", msg)
        return {}, {}

    meta: Dict[str, dict] = {}
    symbols = (exinfo or {}).get("symbols", [])
    logger.info(f"[discovery] BINANCE: Received {len(symbols)} raw instruments")
    
    for s in symbols:
        if (s or {}).get("status") != "TRADING":
            continue
        base = (s or {}).get("baseAsset"); quote = (s or {}).get("quoteAsset")
        sym  = (s or {}).get("symbol")
        if not (base and quote and sym):
            continue
        meta[sym] = {"base": base.upper(), "quote": quote.upper(), "symbol": sym}

    volq: Dict[str, float] = {}
    if isinstance(tickers, Exception):
        logger.warning(f"[discovery] Binance 24hr tickers API unavailable: {tickers!r}")
        tks_all = []
    else:
        tks_all = tickers or []
    
    for t in tks_all:
        sym = (t or {}).get("symbol")
        if sym in meta:
            v = fnum((t or {}).get("quoteVolume"), 0.0)
            volq[sym] = v
            
    logger.info(f"[discovery] BINANCE: Found {len(meta)} tradable instruments and {len(volq)} with volume")
    return meta, volq


async def _load_coinbase(
    session: aiohttp.ClientSession,
    *,
    policy,
    timeout_s: int,
    limiter=None,
    on_api_error=None,
) -> Tuple[Dict[str, dict], Dict[str, float]]:
    """
    Coinbase Exchange:
      - products: /products -> [{id:"ETH-USDC", base_currency, quote_currency, status}]
      - stats:    /products/stats -> {"ETH-USDC":{"last":"..","open":"..","volume":"..", ...}, ...}
    On calcule le volume **quote** ≈ volume_base_24h * last_price.
    """
    # stats:    /products/stats -> {"ETH-USDC":{"last":"..","open":"..","volume":"..", ...}, ...}
    # /!\ Coinbase Exchange n'a pas d'endpoint global /stats.
    # On tente CB_STATS mais si ça échoue, on continue avec meta seule (volume = 0).
    products, stats = await asyncio.gather(
        _fetch_json(
            session,
            CB_PRODUCTS,
            policy=policy,
            timeout=timeout_s,
            exchange="COINBASE",
            limiter=limiter,
            on_error=on_api_error,
        ),
        _fetch_json(
            session,
            CB_STATS,
            policy=policy,
            timeout=timeout_s,
            exchange="COINBASE",
            limiter=limiter,
            on_error=on_api_error,
        ),
        return_exceptions=True
    )

    if isinstance(products, Exception):
        msg = f"Coinbase products API failed: {products!r}"
        logger.error(f"[discovery] {msg}")
        if on_api_error: on_api_error("COINBASE", msg)
        return {}, {}

    if not isinstance(products, list):
        msg = f"Coinbase products API returned unexpected type: {type(products)} (likely error message)"
        logger.error(f"[discovery] {msg}. Response snippet: {str(products)[:200]}")
        if on_api_error: on_api_error("COINBASE", msg)
        return {}, {}

    # Filtre produits "online"
    meta: Dict[str, dict] = {}
    for p in products:
        try:
            status = (p.get("status") or "").lower()
            base = p.get("base_currency")
            quote = p.get("quote_currency")
            pid = p.get("id")  # ex: "ETH-USDC"
            if status not in ("online", "active", "trading"):
                continue
            if not (base and quote and pid):
                continue
            meta[pid] = {"base": base.upper(), "quote": quote.upper(), "product_id": pid}
        except Exception:
            continue

    volq: Dict[str, float] = {}
    if isinstance(stats, Exception):
        logger.warning(f"[discovery] Coinbase stats API unavailable (falling back to 0 volume): {stats!r}")
        st_all = {}
    else:
        st_all = stats or {}

    # st_all est un dict keyed par "product_id"
    for pid, st in (st_all or {}).items():
        if pid not in meta:
            continue
        last = fnum((st or {}).get("last"), 0.0)  # en quote
        vol_base = fnum((st or {}).get("volume"), 0.0)  # base 24h
        volq[pid] = (last * vol_base) if last > 0 else 0.0

    return meta, volq


async def _load_bybit(
    session: aiohttp.ClientSession,
    *,
    policy,
    timeout_s: int,
    limiter=None,
    on_api_error=None,
) -> Tuple[Dict[str, dict], Dict[str, float]]:
    """
    Bybit v5:
      - instruments-info (spot): quoteCoin/baseCoin/symbol/status
      - tickers (spot): lastPrice, volume24h (base), turnover24h (souvent en quote)
    On calcule le volume **quote** ≈ `turnover24h` si dispo/valide, sinon `lastPrice * volume24h`.
    """
    ins, tks = await asyncio.gather(
        _fetch_json(
            session,
            BYBIT_INSTR,
            policy=policy,
            timeout=timeout_s,
            exchange="BYBIT",
            limiter=limiter,
            on_error=on_api_error,
        ),
        _fetch_json(
            session,
            BYBIT_TICKERS,
            policy=policy,
            timeout=timeout_s,
            exchange="BYBIT",
            limiter=limiter,
            on_error=on_api_error,
        ),
        return_exceptions=True
    )

    if isinstance(ins, Exception):
        msg = f"Bybit instruments API failed: {ins!r}"
        logger.error(f"[discovery] {msg}")
        if on_api_error: on_api_error("BYBIT", msg)
        return {}, {}

    if not isinstance(ins, dict):
        msg = f"Bybit instruments API returned unexpected type: {type(ins)}"
        logger.error(f"[discovery] {msg}. Response snippet: {str(ins)[:200]}")
        if on_api_error: on_api_error("BYBIT", msg)
        return {}, {}

    if ins.get("retCode") != 0:
        msg = f"Bybit instruments API returned retCode={ins.get('retCode')}: {ins.get('retMsg')}"
        logger.error(f"[discovery] {msg}")
        if on_api_error: on_api_error("BYBIT", msg)
        return {}, {}

    meta: Dict[str, dict] = {}
    ins_list = (ins.get("result") or {}).get("list") if isinstance(ins.get("result"), dict) else []
    logger.info(f"[discovery] BYBIT: Received {len(ins_list or [])} raw instruments")
    if not isinstance(ins_list, list):
        logger.warning(f"[discovery] Bybit instruments list missing or invalid type: {type(ins_list)}")
        ins_list = []

    for it in ins_list:
        try:
            # P0: Assouplissement maximal pour Bybit V5
            # On accepte tout instrument qui a un base/quote/symbol
            symbol = it.get("symbol")
            base = it.get("baseCoin")
            quote = it.get("quoteCoin")
            
            if not (base and quote and symbol):
                continue
                
            # Filtrage optionnel par catégorie seulement si présent
            cat = (it.get("category") or "spot").lower()
            if cat != "spot":
                continue
                
            meta[symbol] = {"base": base.upper(), "quote": quote.upper(), "symbol": symbol}
        except Exception:
            continue

    volq: Dict[str, float] = {}
    if isinstance(tks, Exception):
        logger.warning(f"[discovery] Bybit tickers API unavailable: {tks!r}")
        tks_all = []
    else:
        tks_all = ((tks or {}).get("result", {}) or {}).get("list", []) or []

    for tk in tks_all:
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
    
    logger.info(f"[discovery] BYBIT: Found {len(meta)} spot instruments and {len(volq)} with volume")
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
    include_result: bool = False,
) -> Tuple[Dict[str, Dict[str, object]], List[str]] | Tuple[Dict[str, Dict[str, object]], List[str], DiscoveryResult]:
    """
    Découverte des paires pilotée par cfg.discovery.* + scoring combos.
    - http_timeout_s, max_inflight_requests, retry_policy (base_ms,max_ms,max_attempts,jitter)
    - quotes_allowed (USDC/USDT/EUR), whitelist/blacklist, min_24h_volume_usd (base)
    - Retourne:
        pair_mapping: {
          "BTCUSDC": {
            "quote": "USDC",
            "base": "BTC",
            "binance": "BTCUSDC" | None,
            "coinbase": "BTC-USDC" | None,
            "bybit": "BTCUSDC" | None,
            "volumes": {"BINANCE": float, "COINBASE": float, "BYBIT": float},
            "region_hint": {"BINANCE": "EU", "COINBASE": "EU", "BYBIT": "JP"},
            "combos":  {"BINANCE/COINBASE": float, "BINANCE/BYBIT": float, "BYBIT/COINBASE": float},
          }, ...
        }
        top_pairs: liste des pk triés par meilleur score combo (EUR upscalé par fx)
        Notionnels et volumes sont exprimés **en devise quote** (pas de conversion FX implicite).
        Les paires JP sont invalidées par défaut si `enable_jp` n'est pas activé.
        include_result=True retourne aussi une DiscoveryResult (audit)
    """

    from collections import defaultdict
    start = time.perf_counter()
    stage_counts: Dict[str, int] = {}
    filtered_counts: Dict[str, int] = {}
    api_errors: Dict[str, int] = {}

    def _add_stage(stage: str, count: int) -> None:
        c = max(0, int(count))
        stage_counts[stage] = stage_counts.get(stage, 0) + c
        try:
            discovery_note_stage(stage, c)
        except Exception:
            pass

    def _add_filtered(reason: str, count: int) -> None:
        c = max(0, int(count))
        if c <= 0:
            return
        filtered_counts[reason] = filtered_counts.get(reason, 0) + c
        try:
            discovery_note_filtered(reason, c)
        except Exception:
            pass

    def _mark_api_error(exchange: str) -> None:
        api_errors[exchange] = api_errors.get(exchange, 0) + 1
        try:
            discovery_note_api_error(exchange)
        except Exception:
            pass

    def _note_api_error(exchange: str, reason: str) -> None:
        _mark_api_error(exchange)
        if reason:
            _add_filtered(reason, 1)

    # --------------------------
    # Helpers locaux autonomes
    # --------------------------

    def _quote_from_pk(pk: str) -> str:
        if pk.endswith("USDC"):
            return "USDC"
        if pk.endswith("USDT"):
            return "USDT"
        if pk.endswith("EUR"):
            return "EUR"
        return "USDC"

    # --------------------------
    # 1) Paramètres de config
    # --------------------------
    d = getattr(cfg, "discovery", None) or DiscoveryCfg()

    # Respecte cfg.discovery.enabled : permet de désactiver la discovery sans surprises.
    if not bool(getattr(d, "enabled", True)):
        _add_filtered("discovery_disabled", 1)
        run_ms = (time.perf_counter() - start) * 1000.0
        try:
            discovery_observe_run_ms(run_ms)
        except Exception:
            pass
        ranking_mode = "fx_scaled" if eur_usdc_fx else "quote_isolated"
        result = DiscoveryResult(
            stage_counts=stage_counts,
            filtered_counts=filtered_counts,
            api_errors=api_errors,
            run_ms=run_ms,
            top_pairs=[],
            fx_applied=bool(eur_usdc_fx),
            ranking_mode=ranking_mode,
        )
        if include_result:
            return {}, [], result
        return {}, []

    # Exchanges activés (priorité: args > cfg.discovery.enabled_exchanges (si non vide) > cfg.g.enabled_exchanges)
    enabled_src = "none"
    enabled = enabled_exchanges
    if enabled:
        enabled_src = "args.enabled_exchanges"
    else:
        de = getattr(d, "enabled_exchanges", None)
        if de:
            enabled = de
            enabled_src = "cfg.discovery.enabled_exchanges"
        else:
            g = getattr(cfg, "g", None)
            ge = getattr(g, "enabled_exchanges", None) if g is not None else None
            if ge:
                enabled = ge
                enabled_src = "cfg.g.enabled_exchanges"

    enabled = [str(e).upper() for e in (enabled or [])]
    enabled = [e for e in enabled if e in ("BINANCE", "COINBASE", "BYBIT")]

    logger.info(
        "[discovery] Starting 3-CEX discovery. Enabled(requested)=%s | Enabled(resolved)=%s (source=%s). Top N: %s",
        enabled_exchanges,
        enabled,
        enabled_src,
        top_n,
    )

    if len(enabled) < 2:
        _add_filtered("no_enabled_exchanges", 1)
        run_ms = (time.perf_counter() - start) * 1000.0
        try:
            discovery_observe_run_ms(run_ms)
        except Exception:
            pass
        ranking_mode = "fx_scaled" if eur_usdc_fx else "quote_isolated"
        result = DiscoveryResult(
            stage_counts=stage_counts,
            filtered_counts=filtered_counts,
            api_errors=api_errors,
            run_ms=run_ms,
            top_pairs=[],
            fx_applied=bool(eur_usdc_fx),
            ranking_mode=ranking_mode,
        )
        if include_result:
            return {}, [], result
        return {}, []

    http_timeout_s = int(getattr(d, "http_timeout_s", 10))
    max_inflight   = int(getattr(d, "max_inflight_requests", 8))
    rp_cfg = getattr(d, "retry_policy", {"base_ms": 500, "max_ms": 20_000, "max_attempts": 5, "jitter": 1})
    backoff_policy = _build_backoff_policy(rp_cfg)

    # Quotes: on respecte cfg.discovery.quotes_allowed, mais on garde un fallback incluant USDT.
    quotes_allowed = set(q.upper() for q in getattr(d, "quotes_allowed", ["USDC", "USDT", "EUR"]))

    # --- Min volumes (100% piloté par cfg.discovery ; plus de os.getenv) ---
    is_actually_dry = bool(getattr(cfg, "dry_run", False))
    base_min_default = 0.0 if is_actually_dry else 100_000.0

    if is_actually_dry:
        base_min = 0.0
        logger.info(f"[discovery] DRY_RUN mode: forcing base_min_volume={base_min}")
    else:
        base_min = float(getattr(d, "min_24h_volume_usd", base_min_default))

    min_by_quote = getattr(d, "min_quote_volume_by_quote", None) or {}

    conf_usdc = min_by_quote.get("USDC")
    conf_usdt = min_by_quote.get("USDT")
    conf_eur  = min_by_quote.get("EUR")

    if conf_usdc is None:
        conf_usdc = getattr(d, "min_quote_volume_usdc", None)
    if conf_eur is None:
        conf_eur = getattr(d, "min_quote_volume_eur", None)
    # tolérance: si jamais min_quote_volume_usdt existe dans une version future
    if conf_usdt is None:
        conf_usdt = getattr(d, "min_quote_volume_usdt", None)

    # Overrides via args (priorité aux args)
    if min_quote_volume_usdc is not None:
        conf_usdc = float(min_quote_volume_usdc)
    if min_quote_volume_eur is not None:
        conf_eur = float(min_quote_volume_eur)

    eur_factor = float(getattr(d, "eur_quote_volume_factor", 0.30))
    eur_floor  = float(getattr(d, "min_quote_volume_floor", 1.0))

    thr_usdc = conf_usdc if conf_usdc is not None else base_min
    thr_usdt = conf_usdt if conf_usdt is not None else base_min
    thr_eur  = conf_eur  if conf_eur  is not None else max(eur_floor, eur_factor * base_min)

    def _min_for_quote(q: str) -> float:
        qu = (q or "").upper()
        if qu == "EUR":
            return float(thr_eur)
        if qu == "USDT":
            return float(thr_usdt)
        return float(thr_usdc)

    # Listes allow/deny (args > cfg)
    whitelist = set(x.upper() for x in (allowlist if allowlist is not None else getattr(d, "whitelist", [])))
    blacklist = set(x.upper() for x in (denylist  if denylist  is not None else getattr(d, "blacklist", [])))

    enable_jp = bool(getattr(getattr(cfg, "g", cfg), "enable_jp", False))
    region_map = None
    for obj in (cfg, getattr(cfg, "g", None)):
        if obj is None:
            continue
        for attr in ("exchange_region_map", "cex_region_map", "engine_region_map", "engine_pod_map"):
            mp = getattr(obj, attr, None)
            if isinstance(mp, dict):
                region_map = mp
                break
        if region_map is not None:
            break

    def _normalize_region(region: Optional[str]) -> Optional[str]:
        if region is None:
            return None
        r = str(region).upper()
        if r.startswith("JP") or r.startswith("TOKYO") or r.startswith("APAC"):
            return "JP"
        if r.startswith("US"):
            return "US"
        if r.startswith("EU"):
            return "EU"
        return "UNKNOWN"

    def _resolve_region(exchange: str) -> Optional[str]:
        if not region_map:
            return None
        exu = str(exchange).upper()
        if exu in region_map:
            return _normalize_region(region_map[exu])
        for k, v in region_map.items():
            if str(k).upper() == exu:
                return _normalize_region(v)
        return None

    region_blocked = {}
    region_hints: Dict[str, str] = {}
    for ex in enabled:
        region = _resolve_region(ex)
        region_hints[ex] = region or "UNKNOWN"
        # On ne bloque que si explicitement JP et que enable_jp=False.
        region_blocked[ex] = (region == "JP") and not enable_jp

    # --------------------------
    # 2) HTTP client + throttler
    # --------------------------
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=http_timeout_s),
        headers={"User-Agent": "PairsDiscovery/3"},
    ) as session:
        limiter = Throttler(rate_limit=max_inflight, period=1.0)

        # --------------------------
        # 4) Collecte parallélisée
        # --------------------------
        (b_meta, b_volq) = ({}, {})
        (c_meta, c_volq) = ({}, {})
        (y_meta, y_volq) = ({}, {})

        fetch_tasks = []
        if "BINANCE" in enabled:
            fetch_tasks.append(_load_binance(
                session,
                policy=backoff_policy,
                timeout_s=http_timeout_s,
                limiter=limiter,
                on_api_error=_note_api_error,
            ))
        if "COINBASE" in enabled:
            fetch_tasks.append(_load_coinbase(
                session,
                policy=backoff_policy,
                timeout_s=http_timeout_s,
                limiter=limiter,
                on_api_error=_note_api_error,
            ))
        if "BYBIT" in enabled:
            fetch_tasks.append(_load_bybit(
                session,
                policy=backoff_policy,
                timeout_s=http_timeout_s,
                limiter=limiter,
                on_api_error=_note_api_error,
            ))

        res = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        i = 0
        if "BINANCE" in enabled:
            if isinstance(res[i], Exception):
                (b_meta, b_volq) = ({}, {})
                if api_errors.get("BINANCE", 0) == 0:
                    _mark_api_error("BINANCE")
            else:
                (b_meta, b_volq) = res[i]
                logger.info(f"[discovery] BINANCE: Loaded {len(b_meta)} meta and {len(b_volq)} volumes")
            i += 1
        if "COINBASE" in enabled:
            if isinstance(res[i], Exception):
                (c_meta, c_volq) = ({}, {})
                if api_errors.get("COINBASE", 0) == 0:
                    _mark_api_error("COINBASE")
            else:
                (c_meta, c_volq) = res[i]
                logger.info(f"[discovery] COINBASE: Loaded {len(c_meta)} meta and {len(c_volq)} volumes")
            i += 1
        if "BYBIT" in enabled:
            if isinstance(res[i], Exception):
                (y_meta, y_volq) = ({}, {})
                if api_errors.get("BYBIT", 0) == 0:
                    _mark_api_error("BYBIT")
            else:
                (y_meta, y_volq) = res[i]
                logger.info(f"[discovery] BYBIT: Loaded {len(y_meta)} meta and {len(y_volq)} volumes")
            i += 1

    # --------------------------
    # 5) Normalisation & filtres
    # --------------------------
    allow: Optional[Set[str]] = {a.upper() for a in (allowlist or [])} or (
        set(x.upper() for x in whitelist) if whitelist else None)
    deny: Set[str] = set(x.upper() for x in blacklist)

    def _fill_maps(meta: Dict[str, dict], volq: Dict[str, float], ex: str):
        sym_map: Dict[str, str] = {}
        vol_map: Dict[str, float] = {}
        quote_map: Dict[str, str] = {}
        base_map: Dict[str, str] = {}
        pk_meta: Dict[str, Tuple[str, str]] = {}
        raw = len(meta or {})
        if not raw:
            _add_filtered("empty_meta", 1)
            logger.warning("[discovery] empty meta map", extra={"exchange": ex})
            stats = {"raw": 0, "kept": 0}
            return sym_map, vol_map, quote_map, base_map, stats
        if region_blocked.get(ex):
            if raw:
                _add_filtered("region_disabled_jp", raw)
            stats = {"raw": raw, "kept": 0}
            return sym_map, vol_map, quote_map, base_map, stats
        for sym, m in (meta or {}).items():
            base = (m or {}).get("base")
            quote = (m or {}).get("quote")
            if not base or not quote:
                _add_filtered("missing_base_quote", 1)
                continue
            quote_u = str(quote).upper()
            if quote_u not in quotes_allowed:
                _add_filtered("quote_not_allowed", 1)
                continue
            base_u = base.upper()
            pk = _pk(base_u, quote_u)

            if allow is not None and base_u not in allow and pk not in allow:
                _add_filtered("whitelist", 1)
                continue
            if base_u in deny or pk in deny:
                _add_filtered("denylist", 1)
                continue

            if pk in pk_meta:
                _add_filtered("pk_collision", 1)
                continue
            pk_meta[pk] = (base_u, quote_u)

            v = fnum(volq.get(sym, 0.0), 0.0)
            sym_map[pk] = sym
            vol_map[pk] = v
            quote_map[pk] = quote_u
            base_map[pk] = base_u

        stats = {"raw": raw, "kept": len(sym_map)}
        return sym_map, vol_map, quote_map, base_map, stats

    b_syms, b_vols, b_quotes, b_bases, b_stats = _fill_maps(b_meta, b_volq, "BINANCE")
    c_syms, c_vols, c_quotes, c_bases, c_stats = _fill_maps(c_meta, c_volq, "COINBASE")
    y_syms, y_vols, y_quotes, y_bases, y_stats = _fill_maps(y_meta, y_volq, "BYBIT")

    for ex, stats in ("binance", b_stats), ("coinbase", c_stats), ("bybit", y_stats):
        _add_stage(f"{ex}_raw", stats.get("raw", 0))
        _add_stage(f"{ex}_eligible", stats.get("kept", 0))

    # --------------------------
    # 6) pair_mapping enrichi
    # --------------------------
    pair_mapping: Dict[str, Dict[str, object]] = {}

    def _ensure(pk: str, quote: str, base: Optional[str] = None) -> bool:
        if not quote:
            _add_filtered("missing_quote_field", 1)
            return False
        if pk in pair_mapping:
            if str(pair_mapping[pk].get("quote")).upper() != quote:
                _add_filtered("pk_collision", 1)
                return False
            if base:
                existing_base = str(pair_mapping[pk].get("base") or "").upper()
                if existing_base and existing_base != base:
                    _add_filtered("pk_collision", 1)
                    return False
                if not existing_base:
                    pair_mapping[pk]["base"] = base
            return True
        pair_mapping[pk] = {
            "quote": quote,
            "base": base or "",
            "binance": None, "coinbase": None, "bybit": None,
            "volumes": {"BINANCE": 0.0, "COINBASE": 0.0, "BYBIT": 0.0},
            "region_hint": dict(region_hints),
            "combos":  {"BINANCE/COINBASE": 0.0, "BINANCE/BYBIT": 0.0, "BYBIT/COINBASE": 0.0},
        }
        return True

    for pk, sym in b_syms.items():
        if not _ensure(pk, b_quotes.get(pk, _quote_from_pk(pk)), b_bases.get(pk)):
            continue
        pair_mapping[pk]["binance"] = sym
        pair_mapping[pk]["volumes"]["BINANCE"] = fnum(b_vols.get(pk, 0.0), 0.0)

    for pk, sym in c_syms.items():
        if not _ensure(pk, c_quotes.get(pk, _quote_from_pk(pk)), c_bases.get(pk)):
            continue
        pair_mapping[pk]["coinbase"] = sym
        pair_mapping[pk]["volumes"]["COINBASE"] = fnum(c_vols.get(pk, 0.0), 0.0)

    for pk, sym in y_syms.items():
        if not _ensure(pk, y_quotes.get(pk, _quote_from_pk(pk)), y_bases.get(pk)):
            continue
        pair_mapping[pk]["bybit"] = sym
        pair_mapping[pk]["volumes"]["BYBIT"] = fnum(y_vols.get(pk, 0.0), 0.0)

    _add_stage("pair_mapping", len(pair_mapping))

    # --------------------------
    # 7) Scoring des combos
    # --------------------------
    combos = [("BINANCE", "COINBASE"), ("BINANCE", "BYBIT"), ("BYBIT", "COINBASE")]
    combos = [c for c in combos if all(x in enabled for x in c)]

    best_scores: Dict[str, float] = {}
    min_volume_marked: Set[str] = set()

    for pk in list(pair_mapping.keys()):
        for a, b in combos:
            has_a = (
                pair_mapping[pk].get(a.lower()) is not None
                if a != "COINBASE"
                else pair_mapping[pk].get("coinbase") is not None
            )
            has_b = (
                pair_mapping[pk].get(b.lower()) is not None
                if b != "COINBASE"
                else pair_mapping[pk].get("coinbase") is not None
            )
            if not (has_a and has_b):
                continue

            quote = (pair_mapping[pk]["quote"] or "USDC").upper()
            thr = _min_for_quote(quote)

            va = fnum((pair_mapping[pk]["volumes"] or {}).get(a, 0.0), 0.0)
            vb = fnum((pair_mapping[pk]["volumes"] or {}).get(b, 0.0), 0.0)
            if va < thr or vb < thr:
                if pk not in min_volume_marked:
                    _add_filtered("min_volume", 1)
                    min_volume_marked.add(pk)
                continue

            s = float(min(va, vb))
            key = f"{a}/{b}"
            pair_mapping[pk]["combos"][key] = s
            if s > 0:
                best_scores[pk] = max(best_scores.get(pk, 0.0), s)

    # ✅ métriques d'audit: comptées UNE SEULE FOIS (sinon sur-comptage)
    _add_stage("combos_eligible", len(best_scores))
    drop_combo = max(0, len(pair_mapping) - len(best_scores))
    if drop_combo:
        _add_filtered("combo_not_eligible", drop_combo)

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

    _add_stage("top_pairs", len(top_pairs))
    drop_rank = max(0, len(best_scores) - len(top_pairs))
    if drop_rank:
        _add_filtered("rank_cutoff", drop_rank)

    run_ms = (time.perf_counter() - start) * 1000.0
    try:
        discovery_observe_run_ms(run_ms)
    except Exception:
        pass

    ranking_mode = "fx_scaled" if eur_usdc_fx else "quote_isolated"
    result = DiscoveryResult(
        stage_counts=stage_counts,
        filtered_counts=filtered_counts,
        api_errors=api_errors,
        run_ms=run_ms,
        top_pairs=top_pairs,
        fx_applied=bool(eur_usdc_fx),
        ranking_mode=ranking_mode,
    )

    if include_result:
        return pair_mapping, top_pairs, result
    return pair_mapping, top_pairs


# ---------- Alias de compat pour l'ancien nom ----------
async def discover_usdc_pairs(
        cfg=None,
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
    effective_cfg = cfg
    if effective_cfg is None:
        effective_cfg = SimpleNamespace(
            discovery=DiscoveryCfg(
                min_quote_volume_usdc=float(min_quote_volume),
                min_quote_volume_eur=float(min_quote_volume_eur),
            ),
            g={"enabled_exchanges": enabled_exchanges or []},
        )
    else:
        # Propagation rétro-compat si on ne dispose pas d'override explicite
        if getattr(getattr(effective_cfg, "discovery", None), "min_quote_volume_usdc", None) is None:
            setattr(effective_cfg.discovery, "min_quote_volume_usdc", float(min_quote_volume))
        if getattr(getattr(effective_cfg, "discovery", None), "min_quote_volume_eur", None) is None:
            setattr(effective_cfg.discovery, "min_quote_volume_eur", float(min_quote_volume_eur))

    return await discover_pairs_3cex(
        effective_cfg,
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
