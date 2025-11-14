# -- coding: utf-8 --
# NOTE: Version "Merged v2" — patches intégrés (edge-guard, skew, queue-pos, TTL hedge, retry)


from __future__ import annotations
"""
ExecutionEngine — aligné avec RiskManager (TT + TM), tri-CEX (Binance / Bybit / Coinbase),
multi-wallet (alias), fragmentation dynamique « profitable », front-loading (G1/G2/G3),
hedge TM, FSM + WS privés, dry-run balances, traçage timestamps + Autopilot Rebalancing intégré.

Patches intégrés (sélection sécurité/perf):
- Knobs: tt_max_skew_ms, tm_queuepos_max_ahead_usd, tm_queuepos_max_eta_ms,
         tm_exposure_ttl_ms, tm_exposure_ttl_hedge_ratio (par défaut 50%).
- Edge guard rapide avant envoi (rétrécissement de l'edge en live).
- IDs déterministes (_cid) + envoi dual avec skew contrôlé (_dual_submit).
- Heuristique queue-position pour TM (+ ETA optionnelle) + watchdog de drift ticks.
- TTL en NN: hedge partial auto à l'échéance (panic_hedge avec ratio configurable).
- _with_retry pour HTTP CEX.

Ce fichier contient: utilitaires, data, FSM, et la moitié haute de ExecutionEngine
(jusqu'à la fin du pipeline TT). La seconde moitié (TM + HTTP + callbacks utilitaires)
se trouve dans PART 2.
"""

import asyncio, time, collections, hashlib, json
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from math import ceil
import base64
import contextlib
import hashlib
import hmac
import json
import logging
import socket
import time
import uuid
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple
from contextlib import asynccontextmanager
from modules.engine_pacer import EnginePacer  # ensure import top-level
from modules.rm_compat import getattr_int, getattr_float, getattr_str, getattr_bool, getattr_dict, getattr_list
from contracts.errors import EngineSubmitError, EngineCancelError, ExternalServiceError
# --- deps async/http ---
# aiohttp is optional in some envs; guard its import
try:
    import aiohttp  # type: ignore
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore

def _as_int_or(v, default: int) -> int:
    try:
        return int(v if v is not None else default)
    except Exception:
        return int(default)


def _as_float_or(v, default: float) -> float:
    try:
        return float(v if v is not None else default)
    except Exception:
        return float(default)


def _as_dict_or_empty(v):
    try:
        return dict(v) if isinstance(v, dict) else {}
    except Exception:
        return {}


from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

_REGION_TZ = {"EU": "Europe/Rome", "US": "America/New_York"}

class PnLAggregator:
    def __init__(self):
        self._last_local_day = {"EU": None, "US": None, "UTC": None}

    def _now_local_day(self, region: str) -> str:
        dt_utc = datetime.now(timezone.utc)
        if region == "UTC" or ZoneInfo is None:
            return dt_utc.strftime("%Y-%m-%d")
        try:
            tz = ZoneInfo(_REGION_TZ.get(region, "Europe/Rome"))
            return dt_utc.astimezone(tz).strftime("%Y-%m-%d")
        except Exception:
            return dt_utc.strftime("%Y-%m-%d")

    def _ensure_resets(self):
        try:
            from obs_metrics import PNL_LIVE_DAY_USD
            for reg in ("EU", "US", "UTC"):
                day = self._now_local_day(reg if reg != "UTC" else "UTC")
                if self._last_local_day.get(reg) != day:
                    self._last_local_day[reg] = day
                    # reset logique: on met à 0 (labels variés)
                    for br in ("TT","TM","MM","REB"):
                        for mode in ("EU_ONLY","SPLIT"):
                            try:
                                PNL_LIVE_DAY_USD.labels(region=reg, branch=br, mode=mode).set(0.0)
                            except Exception:
                                pass
        except Exception:
            pass

    def on_trade_event(self, rec: dict) -> None:
        """
        Consomme un enregistrement (celui envoyé au history_sink).
        Attend au mieux: trade_mode (branch), deployment_mode, pod_region, net_profit (ou sign).
        Ne persiste rien: métriques uniquement.
        """
        self._ensure_resets()
        try:
            from obs_metrics import PNL_LIVE_DAY_USD, TRADES_LIVE_DAY_TOTAL, DERIVED_NET_PROFIT_SIGN_TOTAL, MISSING_NET_PROFIT_TOTAL
        except Exception:
            return  # no-ops si non dispo

        # 1) Filtrage des events pertinents
        status = str(rec.get("status") or rec.get("source") or "").upper()
        if status in {"REJECTED", "CANCELED", "ERROR"}:
            return  # pas de PnL
        branch = str(rec.get("trade_mode") or "").upper() or "TT"
        mode = str(rec.get("deployment_mode") or "").upper() or "EU_ONLY"
        region = str(rec.get("pod_region") or "").upper() or "EU"

        # 2) Profit: priorité au net_profit
        profit = rec.get("net_profit")
        sign = None
        if isinstance(profit, (int, float)) and profit != 0:
            sign = 1 if profit > 0 else -1
        else:
            # fallback sur sign; sinon par bps
            s = rec.get("net_profit_sign")
            if s in (1, -1): sign = int(s)
            else:
                nbps = rec.get("net_bps") or rec.get("spread_net_final_bps")
                if isinstance(nbps, (int, float)) and nbps != 0:
                    sign = 1 if nbps > 0 else -1
                    try: DERIVED_NET_PROFIT_SIGN_TOTAL.labels(module="ENGINE").inc()
                    except Exception: pass
                else:
                    try: MISSING_NET_PROFIT_TOTAL.labels(module="ENGINE", branch=branch).inc()
                    except Exception: pass

        # 3) Montant à incrémenter (live): si on n’a que le signe, on incrémente les compteurs; le montant reste 0
        inc_amount = float(profit) if isinstance(profit, (int, float)) else 0.0

        # 4) Publier EU/US & vue UTC
        for reg in (region, "UTC"):
            try:
                # PnL total
                PNL_LIVE_DAY_USD.labels(region=reg, branch=branch, mode=mode).set_function(
                    # set_function n’est pas dispo partout ; sinon on set() incrémental :
                    None  # fallback ci-dessous
                )
            except Exception:
                pass

        # fallback: set() incrémental si set_function indisponible
        try:
            cur = PNL_LIVE_DAY_USD.labels(region=region, branch=branch, mode=mode)
            if inc_amount != 0.0:
                cur.set((cur._value.get() if hasattr(cur, "_value") else 0.0) + inc_amount)  # best-effort
        except Exception:
            pass
        try:
            cur = PNL_LIVE_DAY_USD.labels(region="UTC", branch=branch, mode=mode)
            if inc_amount != 0.0:
                cur.set((cur._value.get() if hasattr(cur, "_value") else 0.0) + inc_amount)  # best-effort
        except Exception:
            pass

        # Win/Loss counters (même si montant inconnu)
        if sign in (1, -1):
            res = "win" if sign > 0 else "loss"
            try: TRADES_LIVE_DAY_TOTAL.labels(result=res, region=region, branch=branch, mode=mode).inc()
            except Exception: pass
            try: TRADES_LIVE_DAY_TOTAL.labels(result=res, region="UTC", branch=branch, mode=mode).inc()
            except Exception: pass

# === AntiCrossing (single-pod, in-memory) ================================

# ================== KV partagé (multi-pods) ==================

class _ACSharedKV:
    """
    KV asynchrone minimal pour anti-crossing / quotas distribués.
    Tente redis.asyncio (ou aioredis) ; sinon fallback in-memory (best-effort).
    Méthodes: set_nx_px(key, value, ttl_ms), get(key), delete(key).
    """
    def __init__(self, url: str | None = None, namespace: str = "ac", loop=None):
        self.namespace = namespace.strip(":")
        self._loop = loop
        self._mem = {}
        self._mem_exp = {}
        self._redis = None
        self._url = url

        try:
            import redis.asyncio as redis  # redis-py >= 4.2
            if url:
                self._redis = redis.from_url(url, decode_responses=True)
        except Exception:
            try:
                import aioredis  # type: ignore
                if url:
                    self._redis = aioredis.from_url(url, decode_responses=True)
            except Exception:
                self._redis = None

    def _ns(self, key: str) -> str:
        return f"{self.namespace}:{key}"

    async def set_nx_px(self, key: str, value: str, ttl_ms: float) -> bool:
        key = self._ns(key)
        if self._redis is not None:
            try:
                # NX + PX
                return bool(await self._redis.set(key, value, nx=True, px=int(max(1, ttl_ms))))
            except Exception:
                pass
        # fallback mémoire (best-effort)
        import time
        now = time.time()
        # purge
        dead = [k for k,t in self._mem_exp.items() if t <= now]
        for k in dead:
            self._mem.pop(k, None); self._mem_exp.pop(k, None)
        if key in self._mem:
            return False
        self._mem[key] = value
        self._mem_exp[key] = now + ttl_ms/1000.0
        return True

    async def get(self, key: str) -> str | None:
        key = self._ns(key)
        if self._redis is not None:
            try:
                return await self._redis.get(key)
            except Exception:
                pass
        import time
        now = time.time()
        exp = self._mem_exp.get(key)
        if exp and exp <= now:
            self._mem.pop(key, None); self._mem_exp.pop(key, None)
            return None
        return self._mem.get(key)

    async def delete(self, key: str, expected_value: str | None = None) -> None:
        key_ns = self._ns(key)
        if self._redis is not None:
            try:
                if expected_value is None:
                    await self._redis.delete(key_ns); return
                # CAS simple: supprime seulement si la valeur correspond
                lua = """
                local k = KEYS[1]
                local v = ARGV[1]
                local cur = redis.call('GET', k)
                if cur == v then
                  return redis.call('DEL', k)
                end
                return 0
                """
                await self._redis.eval(lua, 1, key_ns, expected_value)
                return
            except Exception:
                pass
        # fallback mémoire
        cur = self._mem.get(key_ns)
        if expected_value is None or cur == expected_value:
            self._mem.pop(key_ns, None); self._mem_exp.pop(key_ns, None)


# ================== Anti-crossing multi-pods ==================

class AntiCrossingGuardMultiPod:
    """
    Réservation distribuée d'une 'bande de prix' (makers TM/MM) avec KV partagé.
    - Clé: {CEX}:{SYMBOL}:{SIDE}:{BAND}
    - Valeur: {pod_id}:{coid}
    - TTL court (ms), SET NX PX (atomic)
    - Stratégie par branche: TM=delay|reprice, MM=delay|reprice (config)
    Config attendue (via engine.config):
      pods_enabled (bool)               [def: True]
      pods_id (str)                     [def: "pod1"]
      pods_coord_url (str|None)         [ex: "redis://host:6379/0"]
      pods_coord_namespace (str)        [def: "ac"]
      ac_enabled (bool)                 [def: True]
      ac_band_width_ticks (int)         [def: 1]
      ac_ttl_ms[,_BINANCE,_BYBIT,_COINBASE] (float) [def: 200]
      ac_strategy_tm ('reprice'|'delay') [def: 'reprice']
      ac_strategy_mm ('reprice'|'delay') [def: 'delay']
      ac_reprice_ticks_tm (int)         [def: 1]
      ac_reprice_ticks_mm (int)         [def: 1]
      ac_delay_ms_tm (float)            [def: 60.0]
      ac_delay_ms_mm (float)            [def: 90.0]
    """
    def __init__(self, engine):
        import time
        self.engine = engine
        self._now = time.time
        self._coid_to_keys = {}  # coid -> [key,...]

        cfg = getattr(engine, "config", None)
        def _cfg(name, default):
            if cfg is None: return default
            if hasattr(cfg, name): return getattr(cfg, name)
            if isinstance(cfg, dict): return cfg.get(name, default)
            return default

        self.enabled   = bool(_cfg("ac_enabled", True))
        self.pod_id    = str(_cfg("pods_id", "pod1"))
        self.band_w    = max(1, int(_cfg("ac_band_width_ticks", 1)))
        self.ttl_ms_def= float(_cfg("ac_ttl_ms", 200.0))
        self.ttl_ms_ex = {
            "BINANCE": float(_cfg("ac_ttl_ms_BINANCE", self.ttl_ms_def)),
            "BYBIT": float(_cfg("ac_ttl_ms_BYBIT", self.ttl_ms_def)),
            "COINBASE": float(_cfg("ac_ttl_ms_COINBASE", self.ttl_ms_def)),
        }
        self.strategy_tm = str(_cfg("ac_strategy_tm", "reprice")).lower()
        self.strategy_mm = str(_cfg("ac_strategy_mm", "delay")).lower()
        self.reprice_tm  = int(_cfg("ac_reprice_ticks_tm", 1))
        self.reprice_mm  = int(_cfg("ac_reprice_ticks_mm", 1))
        self.delay_tm_ms = float(_cfg("ac_delay_ms_tm", 60.0))
        self.delay_mm_ms = float(_cfg("ac_delay_ms_mm", 90.0))

        url = _cfg("pods_coord_url", None)
        ns  = str(_cfg("pods_coord_namespace", "ac"))
        self.kv = _ACSharedKV(url=url, namespace=ns)

    def _ttl_ms_for(self, ex: str) -> float:
        return self.ttl_ms_ex.get((ex or "").upper(), self.ttl_ms_def)

    def _tick_size(self, meta, order) -> float:
        tick = (meta.get("tick_size") if isinstance(meta, dict) else None) \
               or (meta.get("instrument", {}).get("tick_size") if isinstance(meta, dict) else None) \
               or order.get("tick_size") \
               or 1.0
        try:
            t = float(tick)
            return t if t > 0 else 1.0
        except Exception:
            return 1.0

    def _band(self, price: float, tick: float) -> int:
        try:
            return (round(price / tick)) // self.band_w
        except Exception:
            return 0

    def _key(self, ex: str, symbol: str, side: str, band: int) -> str:
        return f"{(ex or '').upper()}:{(symbol or '').upper()}:{(side or '').upper()}:{int(band)}"

    async def reserve_or_delay(self, ex: str, meta: dict, order: dict):
        """
        Makers only: tente SET NX PX sur la bande.
        En cas de conflit (tenue par un autre pod), applique stratégie TM/MM.
        """
        import asyncio, time
        if not self.enabled:
            return
        kind   = (meta.get("kind") or "").upper()
        branch = (meta.get("branch") or "").upper()
        is_tm  = branch == "TM" or kind == "MAKER_TM"
        is_mm  = branch == "MM" or kind == "MAKER_MM"
        if not (is_tm or is_mm):
            return

        symbol = meta.get("symbol") or order.get("symbol") or meta.get("pair") or ""
        side   = (order.get("side") or meta.get("side") or "").upper()
        price  = float(order.get("price") or 0.0)
        tick   = self._tick_size(meta, order)
        band   = self._band(price, tick)
        key    = self._key(ex, symbol, side, band)
        ttl_ms = max(1.0, self._ttl_ms_for(ex))

        coid = order.get("clientOrderId") or order.get("client_order_id") or meta.get("client_order_id")
        if not coid:
            coid = f"ac_{int(time.time()*1e6)}"
            order["clientOrderId"] = coid

        self._coid_to_keys.setdefault(coid, [])

        # Tentative de réservation
        value = f"{self.pod_id}:{coid}"
        ok = await self.kv.set_nx_px(key, value, ttl_ms)
        if ok:
            self._coid_to_keys[coid].append(key)
            return

        # Conflit: qui tient ?
        holder = await self.kv.get(key)
        if holder == value:
            # Nous-mêmes (renouvellement/dup) → OK
            return

        strategy = self.strategy_tm if is_tm else self.strategy_mm
        if strategy == "reprice":
            ticks = self.reprice_tm if is_tm else self.reprice_mm
            if side == "BUY":
                order["price"] = max(0.0, price - ticks * tick)
            else:
                order["price"] = price + ticks * tick
            # Essai de réserver la nouvelle bande
            price2 = float(order.get("price"))
            band2  = self._band(price2, tick)
            key2   = self._key(ex, symbol, side, band2)
            ok2 = await self.kv.set_nx_px(key2, value, ttl_ms)
            if ok2:
                self._coid_to_keys[coid].append(key2)
            if not (ok if 'ok' in locals() else ok2):
                AC_RESERVE_CONFLICT_TOTAL.labels(
                    branch=("TM" if is_tm else "MM"),
                    ex=(ex or "").upper()
                ).inc()
                # la stratégie delay/reprice existante reste inchangée

            return
        else:
            delay_ms = self.delay_tm_ms if is_tm else self.delay_mm_ms
            await asyncio.sleep(max(0.0, float(delay_ms)/1000.0))
            # retry unique
            ok2 = await self.kv.set_nx_px(key, value, ttl_ms)
            if ok2:
                self._coid_to_keys[coid].append(key)
            if not (ok if 'ok' in locals() else ok2):
                AC_RESERVE_CONFLICT_TOTAL.labels(
                    branch=("TM" if is_tm else "MM"),
                    ex=(ex or "").upper()
                ).inc()
                # la stratégie delay/reprice existante reste inchangée

            return

    async def release_for_coid(self, coid: str):
        if not coid:
            return
        keys = self._coid_to_keys.pop(coid, [])
        for k in keys:
            try:
                await self.kv.delete(k, expected_value=f"{self.engine.config.pods_id}:{coid}" if hasattr(self.engine, "config") and hasattr(self.engine.config, "pods_id") else None)
            except Exception as e:
                AC_RELEASE_ERRORS_TOTAL.labels(kind="kv_delete").inc()
                logging.getLogger("ExecutionEngine").debug("[AC] release_for_coid delete failed: %s", e, exc_info=False)



class AntiCrossingGuardSinglePod:
    """
    Réservation courte d'une 'bande de prix' pour éviter les auto-collisions makers.
    Clé: {CEX}:{SYMBOL}:{SIDE}:{BAND}. TTL court; makers TM/MM seulement.
    Config (via engine.config):
      ac_enabled (bool) [def: True]
      ac_band_width_ticks (int) [def: 1]
      ac_ttl_ms[, .BINANCE/.BYBIT/.COINBASE] (int) [def: 200]
      ac_strategy_tm ('reprice'|'delay') [def: 'reprice']
      ac_strategy_mm ('reprice'|'delay') [def: 'delay']
      ac_reprice_ticks_tm (int) [def: 1]
      ac_reprice_ticks_mm (int) [def: 1]
      ac_delay_ms_tm (float ms) [def: 60.0]
      ac_delay_ms_mm (float ms) [def: 90.0]
    """
    def __init__(self, engine):
        import time
        self.engine = engine
        self._now = time.time
        self._holds = {}          # key -> (expires_ts, coid)
        self._coid_to_keys = {}   # coid -> [key,...]

        # lecture conf (avec défauts)
        cfg = getattr(engine, "config", None)
        def _cfg(name, default):
            if cfg is None: return default
            # autorise attributs style ac_enabled ou dict-like
            if hasattr(cfg, name): return getattr(cfg, name)
            if isinstance(cfg, dict): return cfg.get(name, default)
            return default

        self.enabled = bool(_cfg("ac_enabled", True))
        self.band_w  = max(1, int(_cfg("ac_band_width_ticks", 1)))
        self.ttl_ms_default = float(_cfg("ac_ttl_ms", 200.0))
        self.ttl_ms_ex = {
            "BINANCE": float(_cfg("ac_ttl_ms_BINANCE", self.ttl_ms_default)),
            "BYBIT": float(_cfg("ac_ttl_ms_BYBIT", self.ttl_ms_default)),
            "COINBASE": float(_cfg("ac_ttl_ms_COINBASE", self.ttl_ms_default)),
        }
        self.strategy_tm = str(_cfg("ac_strategy_tm", "reprice")).lower()
        self.strategy_mm = str(_cfg("ac_strategy_mm", "delay")).lower()
        self.reprice_tm  = int(_cfg("ac_reprice_ticks_tm", 1))
        self.reprice_mm  = int(_cfg("ac_reprice_ticks_mm", 1))
        self.delay_tm_ms = float(_cfg("ac_delay_ms_tm", 60.0))
        self.delay_mm_ms = float(_cfg("ac_delay_ms_mm", 90.0))

    def _ttl_ms_for(self, ex: str) -> float:
        return self.ttl_ms_ex.get((ex or "").upper(), self.ttl_ms_default)

    def _gc(self):
        now = self._now()
        dead = [k for k,(t,_) in self._holds.items() if t <= now]
        for k in dead: self._holds.pop(k, None)

    def _tick_size(self, meta, order) -> float:
        # Cherche tick_size via meta ou order; fallback 1.0
        tick = (meta.get("tick_size") if isinstance(meta, dict) else None) \
               or (meta.get("instrument", {}).get("tick_size") if isinstance(meta, dict) else None) \
               or order.get("tick_size") \
               or 1.0
        try:
            t = float(tick)
            return t if t > 0 else 1.0
        except Exception:
            return 1.0

    def _band(self, price: float, tick: float) -> int:
        try:
            q = round(price / tick)
            return q // self.band_w
        except Exception:
            return 0

    def _key(self, ex: str, symbol: str, side: str, band: int) -> str:
        return f"{(ex or '').upper()}:{(symbol or '').upper()}:{(side or '').upper()}:{int(band)}"

    async def reserve_or_delay(self, ex: str, meta: dict, order: dict):
        """
        Makers only: réserve une bande; si occupée → applique la stratégie TM/MM.
        Peut modifier le 'price' de l'ordre en mode reprice.
        """
        import asyncio, time
        if not self.enabled:
            return

        kind   = (meta.get("kind") or "").upper()
        branch = (meta.get("branch") or "").upper()
        is_tm  = branch == "TM" or kind == "MAKER_TM"
        is_mm  = branch == "MM" or kind == "MAKER_MM"

        # Ne traiter que makers
        if not (is_tm or is_mm):
            return

        symbol = meta.get("symbol") or order.get("symbol") or meta.get("pair") or ""
        side   = (order.get("side") or meta.get("side") or "").upper()
        price  = float(order.get("price") or 0.0)
        tick   = self._tick_size(meta, order)
        band   = self._band(price, tick)
        key    = self._key(ex, symbol, side, band)
        ttl_s  = max(0.0, self._ttl_ms_for(ex) / 1000.0)

        # coid pour suivi/release
        coid = order.get("clientOrderId") or order.get("client_order_id") or meta.get("client_order_id")
        if not coid:
            # fallback sur horodatage unique si pas de coid
            coid = f"ac_{int(time.time()*1e6)}"
            order["clientOrderId"] = coid  # ne casse pas l'API

        # GC des réservations périmées
        self._gc()

        # Si la bande est libre → on réserve
        if key not in self._holds:
            self._holds[key] = (self._now() + ttl_s, coid)
            self._coid_to_keys.setdefault(coid, []).append(key)
            return

        # Si déjà nous-mêmes → ok
        _, holder = self._holds.get(key, (0.0, None))
        if holder == coid:
            return

        # Conflit: choisir stratégie selon branche
        strategy = self.strategy_tm if is_tm else self.strategy_mm
        if strategy == "reprice":
            # rendre la quote moins agressive: BUY → -ticks ; SELL → +ticks
            ticks = self.reprice_tm if is_tm else self.reprice_mm
            if side == "BUY":
                order["price"] = max(0.0, price - ticks * tick)
            else:  # SELL
                order["price"] = price + ticks * tick
            # recalcule la nouvelle bande + réserve
            price2 = float(order.get("price"))
            band2  = self._band(price2, tick)
            key2   = self._key(ex, symbol, side, band2)
            self._gc()
            self._holds[key2] = (self._now() + ttl_s, coid)
            self._coid_to_keys.setdefault(coid, []).append(key2)
            return
        else:
            # delay court puis réessai (une seule fois)
            delay_ms = self.delay_tm_ms if is_tm else self.delay_mm_ms
            await asyncio.sleep(max(0.0, float(delay_ms) / 1000.0))
            self._gc()
            if key not in self._holds:
                self._holds[key] = (self._now() + ttl_s, coid)
                self._coid_to_keys.setdefault(coid, []).append(key)
            # sinon, on laisse passer sans réserver pour éviter de bloquer
            return

    def release_for_coid(self, coid: str):
        if not coid:
            return
        keys = self._coid_to_keys.pop(coid, [])
        for k in keys:
            self._holds.pop(k, None)

# ---------------------------------------------------------------------------
# Fallbacks / stubs for external symbols (metrics, utils, observability).
# These make the module import-safe even if your environment injects the real ones.
# They are NO-OP and will be overshadowed by your real definitions if present.
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logging.getLogger(__name__).setLevel(logging.INFO)

# Metrics stubs (NO-OP if not provided by your env)
# Metrics stubs (NO-OP if not provided by your env)
class _NoopMetric:
    def labels(self, **_kw):
        return self
    def inc(self, *_a, **_kw):
        return self
    def observe(self, *_a, **_kw):
        return self
    def set(self, *_a, **_kw):
        return self



if "ENGINE_QUEUEPOS_BLOCKED_TOTAL" not in globals():
    ENGINE_QUEUEPOS_BLOCKED_TOTAL = _NoopMetric()
if "ENGINE_CANCELLATIONS_TOTAL" not in globals():
    ENGINE_CANCELLATIONS_TOTAL = _NoopMetric()
if "ENGINE_RETRIES_TOTAL" not in globals():
    ENGINE_RETRIES_TOTAL = _NoopMetric()

# === EXTRA ENGINE METRICS (stubs si absents) ===
if "AC_KV_ERRORS_TOTAL" not in globals():
    AC_KV_ERRORS_TOTAL = _NoopMetric()
if "AC_RESERVE_CONFLICT_TOTAL" not in globals():
    AC_RESERVE_CONFLICT_TOTAL = _NoopMetric()
if "AC_RELEASE_ERRORS_TOTAL" not in globals():
    AC_RELEASE_ERRORS_TOTAL = _NoopMetric()
if "ENGINE_WORKER_ERRORS_TOTAL" not in globals():
    ENGINE_WORKER_ERRORS_TOTAL = _NoopMetric()
if "ENGINE_INFLIGHT_GAUGE_SET_ERRORS_TOTAL" not in globals():
    ENGINE_INFLIGHT_GAUGE_SET_ERRORS_TOTAL = _NoopMetric()
if "ENGINE_BEST_PRICE_MISSING_TOTAL" not in globals():
    ENGINE_BEST_PRICE_MISSING_TOTAL = _NoopMetric()


# Helper/stub functions used by the engine
if "inc_engine_trade" not in globals():
    def inc_engine_trade(*_a, **_kw):
        pass

if "report_nonfatal" not in globals():
    def report_nonfatal(module: str, kind: str, exc: Exception, *, phase: str = ""):
        logger.warning("[report_nonfatal] %s/%s (%s): %s", module, kind, phase, exc)




# dépendances projet (stubs si besoin)
from modules.utils.rate_limiter import TokenBucket
import asyncio, contextlib, logging
# ---------------------------------------------------------------------------
# Observability & metrics (réel) + logger
# ---------------------------------------------------------------------------
import logging
logger = logging.getLogger("ExecutionEngine")
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Observability & metrics (réel) + logger
# ---------------------------------------------------------------------------
logger = logging.getLogger("ExecutionEngine")
logger.setLevel(logging.INFO)

try:
    from modules.observability import (
        set_engine_running,
        observe_engine_latency,
        inc_engine_trade,
        set_engine_queue,
        inc_ack_timeout,
        mark_engine_ack,
        report_nonfatal,
        MM_FILLS_BOTH,
        MM_SINGLE_FILL_HEDGED,
        MM_PANIC_HEDGE_TOTAL,
    )
    from modules.obs_metrics import (
        ENGINE_SUBMIT_TO_ACK_MS,
        ENGINE_ACK_TO_FILL_MS,
        ENGINE_CANCELLATIONS_TOTAL,
        ENGINE_QUEUEPOS_BLOCKED_TOTAL,
        ENGINE_RETRIES_TOTAL,
        ENGINE_SUBMIT_QUEUE_DEPTH,  # <-- gauge {exchange}
        INFLIGHT_GAUGE,  # <-- gauge {exchange}
    )
except Exception:
    # on garde les stubs déclarés plus haut
    # on garde les stubs déclarés plus haut
    pass

    # Garantir présence des gauges, même en NO-OP (si import a échoué)
try:
    ENGINE_SUBMIT_QUEUE_DEPTH
except NameError:
    ENGINE_SUBMIT_QUEUE_DEPTH = _NoopMetric()
try:
    INFLIGHT_GAUGE
except NameError:
    INFLIGHT_GAUGE = _NoopMetric()

# --- MM metrics ---
from modules.observability import MM_FILLS_BOTH, MM_SINGLE_FILL_HEDGED, MM_PANIC_HEDGE_TOTAL
from modules.obs_metrics import ENGINE_SUBMIT_TO_ACK_MS, ENGINE_ACK_TO_FILL_MS

logger = logging.getLogger("ExecutionEngine")

# =============================== Utils ===============================
class WalletRouter:
    """
    Router d’alias (multi-wallet).

    Configs supportées:
      1) Simple (rétro-compatible):
         config.wallet_aliases = {
           "BINANCE":  {"TT": ["TT","TT2"], "TM": ["TM","TM2"]},
           "BYBIT":    {"TT": ["TT","TT2"], "TM": ["TM","TM2"]},
           "COINBASE": {"TT": ["TT","TT2"], "TM": ["TM","TM2"]},
         }

      2) Par quote (recommandé):
         config.wallet_aliases = {
           "BINANCE": {"TT": {"USDC": ["TT_USDC","TT2_USDC"], "EUR": ["TT_EUR","TT2_EUR"]},
                        "TM": {"USDC": ["TM_USDC","TM2_USDC"], "EUR": ["TM_EUR","TM2_EUR"]}},
           "BYBIT":   {"TT": {"USDC": ["TT_USDC"], "EUR": ["TT_EUR"]},
                        "TM": {"USDC": ["TM_USDC"], "EUR": ["TM_EUR"]}},
           "COINBASE":{"TT": {"USDC": ["TT_USDC"], "EUR": ["TT_EUR"]},
                        "TM": {"USDC": ["TM_USDC"], "EUR": ["TM_EUR"]}},
         }
    """

    def __init__(self, config):
        self.aliases = getattr(config, "wallet_aliases", {}) or {}
        self._rr_idx: Dict[Tuple[str, str], int] = {}
        self._rr_idx_q: Dict[Tuple[str, str, str], int] = {}

    def pick(self, exchange: str, strategy: str, prefer: Optional[str] = None) -> str:
        ex = str(exchange or "").upper()
        st = str(strategy or "TT").upper()
        if prefer:
            return prefer
        pool = (self.aliases.get(ex, {}) or {}).get(st, []) or [st]
        if isinstance(pool, dict):
            # cas mapping par quote — ne pas utiliser ici
            pool = (
                pool.get("USDC")
                or pool.get("USD")
                or pool.get("USDT")
                or pool.get("EUR")
                or []
            )
        if len(pool) == 1:
            return pool[0]
        k = (ex, st)
        i = self._rr_idx.get(k, 0) % len(pool)
        self._rr_idx[k] = i + 1
        return pool[i]

    def pick_by_quote(
        self, exchange: str, strategy: str, quote: str, prefer: Optional[str] = None
    ) -> str:
        """Sélectionne un alias en tenant compte de la quote si configurée; fallback sur pick."""
        ex = str(exchange or "").upper()
        st = str(strategy or "TT").upper()
        qt = str(quote or "USDC").upper()
        if prefer:
            return prefer
        exmap = self.aliases.get(ex, {}) or {}
        pool = exmap.get(st)
        if isinstance(pool, dict):
            # mapping par quote
            qpool = (
                pool.get(qt)
                or pool.get("USDC")
                or pool.get("USD")
                or pool.get("USDT")
                or pool.get("EUR")
                or []
            )
            if not qpool:
                return self.pick(exchange, strategy)
            if len(qpool) == 1:
                return qpool[0]
            k = (ex, st, qt)
            i = self._rr_idx_q.get(k, 0) % len(qpool)
            self._rr_idx_q[k] = i + 1
            return qpool[i]
        # rétro-compat
        return self.pick(exchange, strategy)


class SymbolUtils:
    QUOTES = ("USDC", "USDT", "EUR", "USD")

    @staticmethod
    def split_base_quote(symbol: str) -> Tuple[str, str]:
        s = str(symbol or "").replace("-", "").upper()
        for q in SymbolUtils.QUOTES:
            if s.endswith(q):
                return s[: -len(q)], q
        return s, "USDC"


# =============================== Data ===============================
@dataclass
class EngineStats:
    total_executed: int = 0
    total_rebalancing_executed: int = 0
    rejected_by_risk: int = 0
    total_failed: int = 0
    queue_length: int = 0
    last_executed_trade: Optional[Dict[str, Any]] = None
    rejected_symbols: Optional[Dict[str, int]] = None
    error_count: int = 0
    trade_count: int = 0
    execution_latency: float = 0.0
    last_trade_time: float = 0.0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "total_executed": self.total_executed,
            "total_rebalancing_executed": self.total_rebalancing_executed,
            "rejected_by_risk": self.rejected_by_risk,
            "total_failed": self.total_failed,
            "queue_length": self.queue_length,
            "last_executed_trade": self.last_executed_trade,
            "rejected_symbols": dict(self.rejected_symbols or {}),
            "error_count": self.error_count,
            "trade_count": self.trade_count,
            "execution_latency": self.execution_latency,
            "last_trade_time": self.last_trade_time,
        }


# ================================ FSM ================================
class OrderFSM:
    STATES = {"NEW", "ACK", "PARTIAL", "FILLED", "REJECTED", "CANCELED", "ERROR"}

    def __init__(self, client_id: str, ttl_s: float = 2.5):
        self.client_id = client_id
        self.state = "NEW"
        self.exchange_order_id: Optional[str] = None
        self.created_at = time.time()
        self.last_update = self.created_at
        self.ttl_s = float(ttl_s)
        self.filled_qty = 0.0
        self.target_qty = 0.0
        self.reason: Optional[str] = None
        # timings
        self.t_sent_ms: Optional[int] = None
        self.t_ack_ms: Optional[int] = None
        self.t_filled_ms: Optional[int] = None

    def mark_sent(self):
        self.t_sent_ms = int(time.time() * 1000)

    def on_ack(self, ex_oid: str, target_qty: Optional[float] = None):
        if self.state in {"NEW", "ACK", "PARTIAL"}:
            self.state = "ACK"
            self.exchange_order_id = ex_oid or self.exchange_order_id
            if target_qty is not None:
                self.target_qty = float(target_qty)
            self.t_ack_ms = int(time.time() * 1000)
            self.last_update = time.time()

    def on_partial(self, filled_qty: float):
        if self.state in {"ACK", "PARTIAL"}:
            self.state = "PARTIAL"
            self.filled_qty = float(filled_qty)
            self.last_update = time.time()

    def on_filled(self):
        self.state = "FILLED"
        self.t_filled_ms = int(time.time() * 1000)
        self.last_update = time.time()

    def on_reject(self, reason: str = ""):
        self.state = "REJECTED"
        self.reason = reason
        self.last_update = time.time()

    def on_cancel(self):
        self.state = "CANCELED"
        self.last_update = time.time()

    def expired_wo_ack(self) -> bool:
        return self.state == "NEW" and (time.time() - self.created_at) > self.ttl_s


# =========================== ExecutionEngine =========================
class NotReadyError(RuntimeError):
    pass

class ExecutionEngine:

    # === PACER: init & targets (EU / US / EU-CB) ===


    def _ensure_pacer_on(self) -> None:
        """
        Initialise ou reconfigure le Pacer selon la config (profil capital, régions, cibles).
        Appelée au boot et à chaque rechargement de config.
        """
        region = str(getattr(self.config, "region", "EU")).upper()
        cap_profile = str(
            getattr(self.config, "capital_profile", getattr(self.config, "engine_profile", "NANO"))).upper()

        # cibles par région depuis la config
        targets_overrides = {}
        eucb = getattr(self.config, "pacer_targets_eucb", None)
        eu = getattr(self.config, "pacer_targets_eu", None)
        us = getattr(self.config, "pacer_targets_us", None)
        if eu:   targets_overrides["EU"] = eu
        if us:   targets_overrides["US"] = us
        if eucb: targets_overrides["EU-CB"] = eucb

        # mapping exchange -> région (utile si SPLIT)
        region_map = getattr(self.config, "engine_pod_map", {"BINANCE": "EU", "BYBIT": "EU", "COINBASE": "US"})

        # bornes globales de pacing (serrées par le profil ensuite)
        min_ms = int(getattr(self.config, "pacer_min_ms", 0))
        max_ms = int(getattr(self.config, "pacer_max_ms", 250))
        init_ms = int(getattr(self.config, "pacer_init_ms", 0))
        jitter = int(getattr(self.config, "pacer_jitter_ms", 1))

        self._pacer = EnginePacer(
            region=region,
            capital_profile=cap_profile,
            min_ms=min_ms, max_ms=max_ms, init_ms=init_ms, jitter_ms=jitter,
            targets_overrides=targets_overrides,
            region_map=region_map,
        )

    # === /PACER: init & targets ===

    def _pacing_sleep_s(self, regime: str) -> float:
        try:
            self._ensure_pacer_on()
            if getattr(self, "_pacer", None):
                return float(self._pacer.next_sleep_s())
        except Exception:
            pass
        if str(regime or "").lower().startswith("eleve"):
            return float(getattr(self, "pacing_ms_high", 140)) / 1000.0
        return float(getattr(self, "pacing_ms_normal", 0)) / 1000.0

    # === PACER: update helper (à appeler chaque tick ~1-2s) ===
    def _pacer_update(
            self,
            *,
            region: str | None = None,
            p95_submit_ack_ms: float | None = None,
            loop_lag_ms: float | None = None,
            err_rate: float | None = None,
            queue_depth: int | None = None,
            backpressure: bool = False,
            reason: str = "engine_tick",
    ) -> None:
        if not getattr(self, "_pacer", None):
            return
        try:
            # valeurs par défaut (lecture des gauges/attrs si None)
            region = region or str(getattr(self.config, "region", "EU")).upper()
            if p95_submit_ack_ms is None:
                p95_submit_ack_ms = float(getattr(self.metrics, "p95_submit_ack_ms", 0.0)) if hasattr(self,
                                                                                                      "metrics") else 0.0
            if loop_lag_ms is None:
                loop_lag_ms = float(getattr(self.metrics, "loop_lag_p95_ms", 0.0)) if hasattr(self, "metrics") else 0.0
            if err_rate is None:
                err_rate = float(getattr(self.metrics, "last_err_rate", 0.0)) if hasattr(self, "metrics") else 0.0
            if queue_depth is None:
                queue_depth = int(self.submit_queue.qsize()) if hasattr(self, "submit_queue") else 0

            self._pacer.update(
                region=region,
                p95_submit_ack_ms=float(p95_submit_ack_ms or 0.0),
                loop_lag_ms=float(loop_lag_ms or 0.0),
                err_rate=float(err_rate or 0.0),
                queue_depth=int(queue_depth or 0),
                backpressure=bool(backpressure),
                reason=reason,
            )
        except Exception:
            pass  # jamais bloquant

    # === /PACER: update helper ===

    # ---------------------------------------------------------------------------


    def __init__(
            self,
            cfg,
            private_ws,
            rate_limiter,
            retry_policy,
            history_logger=None,
            *,
            config=None,
            risk_manager=None,
            venue_keys: dict | None = None,
            max_concurrent: int = 1,
            ready_event: "asyncio.Event | None" = None,
    ):
        """
        Engine piloté par cfg.engine.* + anti-crossing makers-only via cfg.g.ac.*
        Zéro os.getenv : tout est config-driven.
        """
        import asyncio
        from collections import deque
        from typing import Optional, Dict, Any, Tuple, List

        # --- Wiring de base ---
        self.pws = private_ws
        self.private_ws_hub = private_ws
        self.rl = rate_limiter
        self.retry = retry_policy
        self.history = history_logger


        # Sous-config "engine" (si fournie) ; sinon cfg.engine ; sinon cfg
        self.config = config or getattr(self.cfg, "engine", self.cfg)
        self.risk_manager = risk_manager

        # --- Garde DRY_RUN/PROD cohérente ---
        if self.cfg.g.mode == "DRY_RUN" and self.cfg.g.feature_switches.get("engine_real", False):
            raise RuntimeError("DRY_RUN: engine_real must be OFF")

        # --- Anti-crossing (makers-only) piloté par cfg.g.ac ---
        ac = dict(self.cfg.g.ac)
        self._ac_enabled = bool(ac.get("enabled", True))
        self._ac_scope = str(ac.get("scope", "pod")).lower()  # "pod" | "cluster"
        self._ac_bps = float(ac.get("price_band_bps", 1.5))
        self._ac_ttl_ms = int(ac.get("ttl_ms", 700))
        self._ac_backend = ac.get("backend") or ""  # Redis/RPC optionnel
        self._ac_namespace = str(ac.get("namespace", "prod-eu-us"))
        self._ac_action = str(ac.get("on_violation", "cancel")).lower()  # cancel|skip|widen
        self._ac_coord = None  # brancher le client Redis/RPC ailleurs si utilisé

        # --- Venue keys / readiness ---
        self.venue_keys = venue_keys or {"BINANCE": {}, "BYBIT": {}, "COINBASE": {}}
        self._order_keys_hint: Dict[str, Tuple[str, Optional[str], Optional[str]]] = {}
        self.ready_event = ready_event or asyncio.Event()
        self._auto_ready_on_start: bool = bool(getattr(self.config, "ready_autoset_on_start", False))

        # --- File d’ordres bornée & workers ---
        qmax = int(getattr(self.config, "engine_submit_queue_size", 256))
        qmax = max(1, qmax)
        self.order_queue: asyncio.Queue = asyncio.Queue(maxsize=qmax)
        self.running: bool = False
        self._session: Optional[Any] = None  # aiohttp.ClientSession si utilisé
        self._workers: List[Any] = []
        self._max_concurrent = max(1, int(getattr(self.config, "max_concurrent", max_concurrent)))

        # --- Rate limiting par CEX (utilise ta classe TokenBucket existante) ---
        # au lieu de self._rl_binance_order = TokenBucket(...):
        self._rl_binance_order = self.rl.bucket_for("BINANCE", "order")
        self._rl_bybit_order = self.rl.bucket_for("BYBIT", "order")
        self._rl_coinbase_order = self.rl.bucket_for("COINBASE", "order")

        # --- Listeners / history sink ---
        self.listeners: List[Dict[str, Any]] = []
        self.history_sink: Optional[Any] = None

        # --- Stats / meta (utilise ta classe EngineStats) ---
        self.stats = EngineStats(rejected_symbols={})
        self.last_restart_reason: Optional[str] = None

        # --- Gardes prix & TIFs / timeouts ---
        self.max_price_deviation_pct = float(getattr(self.config, "max_price_deviation_pct", 0.005))
        self.default_tif_single = str(getattr(self.config, "default_tif_single", "GTC")).upper()
        self.default_tif_bundle = str(getattr(self.config, "default_tif_bundle", "IOC")).upper()
        self.order_timeout_s = float(getattr(self.config, "order_timeout_s", 2.5))

        # --- FSM / mapping client_id→(exchange,symbol) (tes classes) ---
        self._order_fsm: Dict[str, OrderFSM] = {}
        self._client_symbol_map: Dict[str, Tuple[str, str]] = {}  # client_id -> (EXCHANGE, SYMBOL)

        # --- WS privés / hedges ---
        # --- WS privés / hedges ---
        self._ws_clients: List[Any] = []
        if self.private_ws_hub:
            try:
                self.private_ws_hub.register_callback(self.handle_order_update, role="engine")
            except TypeError:
                # anciennes versions sans param role
                self.private_ws_hub.register_callback(self.handle_order_update)
            except Exception:
                logger.exception("[ExecutionEngine] unable to register engine callback on hub")
        self._tm_hedges: Dict[str, Dict[str, Any]] = {}  # maker_client_id -> plan hedge

        # --- Pacing / slicing ---
        self.max_inflight_slices = int(getattr(self.config, "max_inflight_slices", 3))
        self.pacing_ms_normal = int(getattr(self.config, "pacing_ms_normal", 0))
        self.pacing_ms_moderate = int(getattr(self.config, "pacing_ms_moderate", 80))
        self.pacing_ms_high = int(getattr(self.config, "pacing_ms_high", 140))

        # --- Ancre staleness ---
        self.anchor_max_staleness_ms = int(getattr(self.config, "anchor_max_staleness_ms",
                                                   getattr(self.config, "cross_staleness_ms", 300)))
        self.anchor_halve_guard_ms = int(getattr(self.config, "anchor_halve_guard_ms", 150))
        self._seen_client_ids: set[str] = set()

        # ==== TM / branches activées ====
        self.enable_taker_maker = bool(getattr(self.config, "enable_taker_maker", True))
        self.enable_taker_taker = bool(getattr(self.config, "enable_taker_taker", True))
        self.tm_weights = list(getattr(self.config, "tm_weights", [0.5, 0.3, 0.2]))
        self.tm_max_open_makers = int(getattr(self.config, "tm_max_open_makers", 3))
        self.tm_watch_timeout_s = float(getattr(self.config, "tm_watch_timeout_s", 3.0))
        self.maker_pad_ticks = int(getattr(self.config, "maker_pad_ticks", 0))  # compat

        # ==== STP (anti self-match) & circuits vol/profondeur ====
        self.stp_pre_taker_enabled = bool(getattr(self.config, "stp_pre_taker_enabled", True))
        self.stp_move_ticks = int(getattr(self.config, "stp_move_ticks", max(1, self.maker_pad_ticks)))

        # Volatilité (bps, côté Engine)
        self.vol_soft_cap_bps = float(getattr(self.config, "vol_soft_cap_bps", 45.0))
        self.vol_hard_cap_bps = float(getattr(self.config, "vol_hard_cap_bps", 80.0))
        self.freeze_tm_on_vol = bool(getattr(self.config, "freeze_tm_on_vol", True))
        self.freeze_mm_on_vol = bool(getattr(self.config, "freeze_mm_on_vol", True))

        # Gardes profondeur (quotes)
        self.depth_min_quote_tt = float(getattr(self.config, "depth_min_quote_tt", 200.0))
        self.depth_min_quote_tm = float(getattr(self.config, "depth_min_quote_tm", 500.0))
        self.depth_min_quote_mm = float(getattr(self.config, "depth_min_quote_mm", 1000.0))
        self.depth_levels_check = int(getattr(self.config, "depth_levels_check", 3))
        self.shallow_book_blocks_tm = bool(getattr(self.config, "shallow_book_blocks_tm", True))
        self.shallow_book_blocks_mm = bool(getattr(self.config, "shallow_book_blocks_mm", True))

        # Kill-switch temporisés (mute par paire & branche)
        self.circuit_mute_s_tm = float(getattr(self.config, "circuit_mute_s_tm", 300.0))
        self.circuit_mute_s_mm = float(getattr(self.config, "circuit_mute_s_mm", 300.0))
        self._mute_until = {"TM": {}, "MM": {}}  # pair_key → epoch

        # ==== MM (Engine) ====
        self.mm_ttl_ms = int(getattr(self.config, "mm_ttl_ms", 2300))
        self.mm_hedge_schedule = tuple(
            getattr(self.config, "mm_hedge_schedule", ((0.50, 0.30), (0.80, 0.50), (1.00, 0.70)))
        )

        # ==== Fragmentation front-loaded ====
        self.frontload_weights = list(getattr(self.config, "frontload_weights", [0.6, 0.3, 0.1]))
        self.min_fragment_quote = float(getattr(self.config, "min_fragment_quote", 200.0))

        # ==== SAFE DEFAULTS / STATE ====
        self._mm_scaled_until = {}
        self._mm_scaled_pairs = set()
        self._min_required_bps_offset = {}  # pair_key -> +bps

        # Knobs price-band vol-aware
        self.price_band_bps_floor = float(getattr(self.config, "price_band_bps_floor", 15.0))
        self.price_band_bps_cap = float(getattr(self.config, "price_band_bps_cap", 50.0))
        self.vol_price_band_k = float(getattr(self.config, "vol_price_band_k", 0.6))

        # Hystérésis scaling MM lors d’activité TT/TM
        self.mm_hysteresis_ms = int(getattr(self.config, "mm_hysteresis_ms", 2000))
        self.mm_scale_on_tt_tm = float(getattr(self.config, "mm_scale_on_tt_tm", 0.6))
        self.mm_pad_boost_on_tt_tm = int(getattr(self.config, "mm_pad_boost_on_tt_tm", 1))

        # ==== Concurrence / pacing global par branche ====
        self.max_parallel_pairs_tt = int(getattr(self.config, "max_parallel_pairs_tt", 2))
        self.max_parallel_pairs_tm = int(getattr(self.config, "max_parallel_pairs_tm", 2))
        self._sem_tt_pairs = asyncio.Semaphore(self.max_parallel_pairs_tt)
        self._sem_tm_pairs = asyncio.Semaphore(self.max_parallel_pairs_tm)

        # ---- Multi-wallet router & timings (ta classe) ----
        self.wallet_router = WalletRouter(self.config)

        self._ack_marked: set[str] = set()
        self._ack_ring = deque(maxlen=20_000)
        self._submit_ts_ns: Dict[str, int] = {}
        self.pnl_agg = PnLAggregator()

        # ---- Routes autorisées (RM > config > défauts) ----
        default_routes = {
            ("BINANCE", "BYBIT"), ("BYBIT", "BINANCE"),
            ("BINANCE", "COINBASE"), ("COINBASE", "BINANCE"),
            ("BYBIT", "COINBASE"), ("COINBASE", "BYBIT"),
        }

        def _norm_ex(x: str) -> str:
            return str(x).upper()

        def _nr(rs):
            out = set()
            for a, b in (rs or []): out.add((_norm_ex(a), _norm_ex(b)))
            return out

        cfg_routes = _nr(getattr(self.config, "allowed_routes", None)) or default_routes
        rm_routes = _nr(getattr(self.risk_manager, "allowed_routes", None)) if self.risk_manager else set()
        self._allowed_routes = rm_routes or cfg_routes or default_routes

        # ================== AUTOPILOT REBALANCING ==================
        self._rebalancing_cb: Optional[Any] = None
        self._rebal_loop_task: Optional[Any] = None
        self._rebal_last_plan_key: Optional[str] = None
        self._rebal_first_seen_ts: float = 0.0
        self._rebal_last_fire_ts: float = 0.0
        self._rebal_cooldown_s: float = 5.0
        self.rebal_autopilot_enabled = bool(getattr(self.config, "rebal_autopilot_enabled", True))
        self._t_submit_ms: Dict[str, int] = {}

        # Carte simple de frais (si fournie)
        self.fee_map_pct = (
                getattr(self, "fee_map_pct", {})
                or getattr(self.config, "fee_map_pct", {})
                or {}
        )

        # ============ KNOBS additionnels (fusion) ============
        self.tt_max_skew_ms = int(getattr(self.config, "tt_max_skew_ms", 120))
        self.tm_queuepos_max_ahead_quote = float(
            getattr(self.config, "tm_queuepos_max_ahead_quote",
                    getattr(self.config, "tm_queuepos_max_ahead_usd", 25_000.0))
        )
        self.tm_queuepos_max_eta_ms = int(getattr(self.config, "tm_queuepos_max_eta_ms", 0))
        self.tm_exposure_ttl_ms = int(
            getattr(self.config, "tm_exposure_ttl_ms",
                    int(getattr(self.config, "tm_nn_max_exposure_s", 3.0) * 1000))
        )
        self.tm_exposure_ttl_hedge_ratio = float(getattr(self.config, "tm_exposure_ttl_hedge_ratio", 0.50))

        # ---- Pacer optionnel ----
        try:
            self._ensure_pacer_on()
        except Exception:
            pass

        # ---- Inflight ceilings par venue (semaphores) + métriques ----
        lim_bin = int(getattr(self.config, "inflight_max_binance", getattr(self.config, "INFLIGHT_MAX_BINANCE", 8)))
        lim_byb = int(getattr(self.config, "inflight_max_bybit", getattr(self.config, "INFLIGHT_MAX_BYBIT", 8)))
        lim_cb = int(
            getattr(self.config, "inflight_max_coinbase", getattr(self.config, "INFLIGHT_MAX_COINBASE", 6)))
        self._sem_inflight = {
            "BINANCE": asyncio.Semaphore(max(1, lim_bin)),
            "BYBIT": asyncio.Semaphore(max(1, lim_byb)),
            "COINBASE": asyncio.Semaphore(max(1, lim_cb)),
        }
        self._inflight_curr = {"BINANCE": 0, "BYBIT": 0, "COINBASE": 0}

        try:
            for ex in ("BINANCE", "BYBIT", "COINBASE"):
                INFLIGHT_GAUGE.labels(exchange=ex).set(0)
        except Exception:
            pass
        try:
            ENGINE_SUBMIT_QUEUE_DEPTH.labels(exchange="ALL").set(0)
        except Exception:
            pass

    async def submit_maker_or_delay(self, order: dict, meta: dict) -> Optional[str]:
        """
        Tente de réserver une bande de prix (anti-crossing) avant soumission du maker.
        - Convertit ac.price_band_bps -> ticks selon tick_size & px.
        - Si conflit: applique ac.on_violation (cancel|skip|widen).
        - Retourne order_id si envoyé, None sinon.
        """
        if not self._ac_enabled or not self._is_maker(order):
            return await self._send_order_real(order, meta)

        px = float(order["price"])
        tick = self._tick_size(meta, order)
        band_px = (self._ac_bps / 10_000.0) * px
        band_ticks = max(1, int(round(band_px / max(1e-12, tick))))

        ok = await self._ac_reserve(pair=order["symbol"], side=order["side"],
                                    price=px, band_ticks=band_ticks, ttl_ms=self._ac_ttl_ms)
        if not ok:
            if self._ac_action == "skip":
                return None
            if self._ac_action == "widen":
                # élargir légèrement de 1 tick
                order = dict(order)
                order["price"] = px - tick if order["side"] == "sell" else px + tick
            # "cancel" -> on ne soumet pas
            if self._ac_action == "cancel":
                return None

        return await self._send_order_real(order, meta)

    async def _ac_reserve(self, pair: str, side: str, price: float, band_ticks: int, ttl_ms: int) -> bool:
        """
        Réserve une bande de prix pour éviter maker×maker.
        Scope "pod": garde en mémoire locale.
        Scope "cluster": utilise un coord (Redis/RPC) si self._ac_backend est fourni.
        """
        key = f"{self._ac_namespace}:{pair}:{side}"
        low = price - band_ticks * self._tick_size_simple(pair)
        high = price + band_ticks * self._tick_size_simple(pair)

        if self._ac_scope == "cluster" and self._ac_backend:
            # TODO: implémenter SETNX + PEXPIRE (Redis) ou RPC équivalent
            # Retour True si la bande est libre, False si occupée
            return True

        # Scope pod (local): réutiliser une structure en RAM
        now = self._now_ms()
        gc_before = now - 5_000
        self._ac_local = getattr(self, "_ac_local", {})
        # GC
        for k in list(self._ac_local.keys()):
            if self._ac_local[k]["expires_at"] < gc_before:
                self._ac_local.pop(k, None)

        # Conflit ?
        cur = self._ac_local.get(key)
        if cur and cur["low"] <= price <= cur["high"] and cur["expires_at"] >= now:
            return False

        self._ac_local[key] = {"low": low, "high": high, "expires_at": now + ttl_ms}
        return True

    # execution_engine.py — dans class ExecutionEngine
    def _rm_ioc_mm_overrides(self, *, tif: str, maker: bool, post_only: bool) -> tuple[str, bool, bool, bool]:
        """
        Applique les overrides imposés par le RiskManager (overlay de mode).
        Retourne (tif, maker, post_only, skip_maker_leg).
        - Si RM force IOC-only => tif="IOC", post_only=False, maker=False (pas d'ordre maker en IOC).
        - Si RM disable MM => on marque skip_maker_leg=True pour ignorer tout ordre maker.
        """
        skip = False
        rm = getattr(self, "risk_manager", None)
        if rm is not None:
            # IOC-only (overlay)
            if bool(getattr(rm, "_ioc_only", False)):
                tif = "IOC"
                post_only = False
                maker = False
            # MM désactivé (overlay)
            if hasattr(rm, "enable_mm") and (rm.enable_mm is False) and maker:
                skip = True
                maker = False
                post_only = False
        return tif, maker, post_only, skip

    # Helpers simplifiés
    def _is_maker(self, order: dict) -> bool:
        return order.get("type") in ("LIMIT", "POST_ONLY", "MAKER")

    def _tick_size(self, meta: dict, order: dict) -> float:
        return float(meta.get("tick_size", 1e-6))

    def _tick_size_simple(self, pair: str) -> float:
        return 1e-6

    def _now_ms(self) -> int:
        import time
        return int(time.time() * 1000)


    def _normalize_order_to_private_event(self, exchange: str, alias: str, data: dict, source: str = "resync") -> dict:
        """
        Convertit un ordre REST en event privé standardisé.
        """
        import time
        status_raw = str(data.get("status") or data.get("state") or "").upper()
        if status_raw in ("FILLED", "FILLS", "DONE"):
            status = "FILL"
        elif status_raw in ("PARTIALLY_FILLED", "PARTIAL"):
            status = "PARTIAL"
        elif status_raw in ("NEW", "OPEN", "ACKNOWLEDGED", "ACK"):
            status = "ACK"
        elif status_raw in ("REJECTED", "CANCELED", "CANCELLED"):
            status = "REJECT"
        else:
            status = "ACK"  # défaut conservateur

        ev = {
            "exchange": exchange,
            "alias": alias,
            "status": status,
            "order_id": data.get("orderId") or data.get("order_id") or data.get("id"),
            "client_id": data.get("clientOrderId") or data.get("client_id") or data.get("cid"),
            "ts_exchange": float(data.get("transactTime") or data.get("timestamp") or data.get("ts") or time.time()),
            "ts_local": time.time(),
            "filled_qty": data.get("executedQty") or data.get("filled_qty"),
            "filled_quote": data.get("cummulativeQuoteQty") or data.get("filled_quote"),
            "fee_ccy": data.get("feeAsset") or data.get("fee_ccy"),
            "fee": data.get("fee"),
            "meta": {"source": source},
        }
        return ev

    def _normalize_fill_to_private_event(self, exchange: str, alias: str, data: dict, source: str = "resync") -> dict:
        """
        Convertit un fill/trade REST en event privé standardisé (toujours FILL ou PARTIAL).
        """
        import time
        ev = {
            "exchange": exchange,
            "alias": alias,
            "status": "FILL",
            "order_id": data.get("orderId") or data.get("order_id") or data.get("id"),
            "client_id": data.get("clientOrderId") or data.get("client_id") or data.get("cid"),
            "ts_exchange": float(data.get("time") or data.get("timestamp") or data.get("ts") or time.time()),
            "ts_local": time.time(),
            "filled_qty": data.get("qty") or data.get("executedQty") or data.get("filled_qty"),
            "filled_quote": data.get("quoteQty") or data.get("cummulativeQuoteQty") or data.get("filled_quote"),
            "fee_ccy": data.get("commissionAsset") or data.get("fee_ccy"),
            "fee": data.get("commission") or data.get("fee"),
            "meta": {"source": source},
        }
        return ev

    # --- Exchange normalization helpers ---


    def _update_submit_queue_gauge(self):
        """
        Met à jour la gauge de profondeur de file et notifie le PACER pour la mesure
        de drain-latency (passage de queue>0 à 0).
        Compatible order_queue et _submit_queue via get_queue_depth().
        """
        try:
            depth = int(self.get_queue_depth())
        except Exception:
            depth = 0

        # Prometheus / observability
        try:
            ENGINE_SUBMIT_QUEUE_DEPTH.labels(exchange="ALL").set(depth)
        except Exception:
            pass
        try:
            set_engine_queue(depth)
        except Exception:
            pass

        # PACER: ping drain-latency (EU / US / EU-CB selon config.region)
        try:
            if getattr(self, "_pacer", None):
                self._pacer.on_queue_depth(depth, region=str(getattr(self.config, "region", "EU")).upper())
        except Exception:
            # ne jamais casser l'engine pour l’observabilité
            pass
        # === /PACER: ping drain-latency ===

    # modules/execution_engine.py — class ExecutionEngine



    @asynccontextmanager
    async def _inflight_scope(self, exchange: str):
        """
        Semaphore par CEX (plafond hard) + gauge in-flight robuste.
        """
        ex = (exchange or "").upper()
        semaphores = getattr(self, "_sem_inflight", {}) or {}
        sem = semaphores.get(ex)
        if not sem:
            yield
            return
        await sem.acquire()
        try:
            self._inflight_curr = getattr(self, "_inflight_curr", {})
            self._inflight_curr[ex] = int(self._inflight_curr.get(ex, 0)) + 1
            try:
                INFLIGHT_GAUGE.labels(exchange=ex).set(self._inflight_curr[ex])  # noqa: PROM gauge
            except Exception as e:
                ENGINE_INFLIGHT_GAUGE_SET_ERRORS_TOTAL.labels(exchange=ex).inc()
                logging.getLogger("ExecutionEngine").debug("[Engine] INFLIGHT_GAUGE.set failed: %s", e, exc_info=False)
            yield
        finally:
            try:
                self._inflight_curr[ex] = max(0, int(self._inflight_curr.get(ex, 1)) - 1)
                try:
                    INFLIGHT_GAUGE.labels(exchange=ex).set(self._inflight_curr[ex])
                except Exception as e:
                    ENGINE_INFLIGHT_GAUGE_SET_ERRORS_TOTAL.labels(exchange=ex).inc()
                    logging.getLogger("ExecutionEngine").debug("[Engine] INFLIGHT_GAUGE.set failed: %s", e,
                                                               exc_info=False)
            finally:
                sem.release()

    def _ex_upper(self, ex: str) -> str:
        return str(ex or "").strip().upper()

    def _ex_venue(self, ex: str) -> str:
        # Clé canonique pour venue_keys (PascalCase)
        EX_CANON = {"BINANCE": "Binance", "BYBIT": "Bybit", "COINBASE": "Coinbase"}
        return EX_CANON.get(self._ex_upper(ex), str(ex or "").strip().title())

    # --------------------------- readiness ---------------------------
    def mark_ready(self) -> None:
        """Permet de passer l’engine en état 'prêt' explicitement."""
        try:
            self.ready_event.set()
        except Exception:
            logging.exception("Unhandled exception")

    def _spawn(self, coro: Coroutine[Any, Any, Any], name: str = "") -> asyncio.Task:
        if not hasattr(self, "_bg_tasks"):
            self._bg_tasks = set()
        t = asyncio.create_task(coro, name=name or None)
        self._bg_tasks.add(t)
        t.add_done_callback(lambda _t: self._bg_tasks.discard(_t))
        return t

    # 1) remplace la méthode
    def _ack_mark_once(self, key: str) -> bool:
        if key in self._ack_marked:
            return False
        self._ack_marked.add(key)
        self._ack_ring.append(key)
        if len(self._ack_marked) > (self._ack_ring.maxlen * 2):
            self._ack_marked.intersection_update(set(self._ack_ring))
        return True

    def mark_not_ready(self) -> None:
        """Permet de bloquer toute émission tant que la synchro/WS n'est pas stabilisée."""
        try:
            self.ready_event.clear()
        except Exception:
            logging.exception("Unhandled exception")

    # modules/execution_engine.py
    # modules/execution_engine.py
    def get_queue_depth(self) -> int:
        """
        Retourne la profondeur de file de l'engine en priorisant la nouvelle file `order_queue`.
        Fallback legacy sur `_submit_queue`. Tolérant aux implémentations sans qsize().
        """
        for attr in ("order_queue", "_submit_queue"):
            q = getattr(self, attr, None)
            if q is None:
                continue
            try:
                if hasattr(q, "qsize"):
                    return int(q.qsize())
                if hasattr(q, "__len__"):
                    return int(len(q))  # type: ignore[arg-type]
            except Exception:
                # on essaie l’attribut suivant
                continue

        # Dernier filet (valeur déjà observée/maintenue par l’engine)
        try:
            return int(getattr(getattr(self, "stats", None), "queue_length", 0))
        except Exception:
            return 0

    def _ensure_ready(self):
        if not self.ready_event.is_set():
            # On logge en WARNING pour bien voir les tentatives prématurées
            logger.warning("[Engine] Appel avant readiness — action refusée")
            raise NotReadyError("ExecutionEngine not ready")

    def _fmt_pair(self, s: str) -> str:
        return str(s or "").replace("-", "").upper()

    def _ob_latest(self, exchange: str, pair_key: str) -> dict:
        ex = self._ex_upper(exchange)
        pk = self._fmt_pair(pair_key)
        src = getattr(self.risk_manager, "rebal_mgr", self.risk_manager)
        return (getattr(src, "latest_orderbooks", {}) or {}).get(ex, {}).get(pk, {}) or {}

    # --------------------------- wiring ---------------------------
    def set_history_logger(self, sink: Callable[[Dict[str, Any]], Any]):
        self.history_sink = sink

    def register_listener(
        self, listener_callback: Callable, trade_type: Optional[str] = None, priority: int = 0
    ):
        self.listeners.append(
            {"callback": listener_callback, "type": trade_type, "priority": priority}
        )
        self.listeners.sort(key=lambda x: x["priority"], reverse=True)

    def _release_client_id(self, cid: Optional[str]) -> None:
        if cid:
            self._seen_client_ids.discard(str(cid))

    # execution_engine.py — class ExecutionEngine

    def on_private_order_update(self, event: dict):
        """
        WS privé: ACK/PARTIAL/FILL/REJECT
        - Trace lat_submit_to_ack_ms
        - (opt) lat_submit_to_fill_ms
        - Libère la réservation anti-crossing (single/multi-pods) pour ce coid
        - Passe à l’handler interne si présent
        """
        import time, asyncio
        etype = (event.get("type") or event.get("status") or "").upper()
        coid = (event.get("clientOrderId") or event.get("client_order_id")
                or event.get("clOrdID") or event.get("cid"))

        # SUBMIT -> ACK
        if etype in ("ACK", "NEW", "ACCEPTED") and coid and hasattr(self, "_pending_submit_ts"):
            ts0 = self._pending_submit_ts.pop(coid, None)
            if ts0 is not None and hasattr(self, "obs_hist"):
                self.obs_hist("lat_submit_to_ack_ms", (time.time() - ts0) * 1000.0)

        # SUBMIT -> FILL (optionnel)
        if bool(getattr(self.config, "obs_trace_submit_to_fill", False)) and coid and hasattr(self,
                                                                                              "_pending_submit_ts"):
            if etype in ("FILL", "PARTIAL_FILL"):
                ts0 = self._pending_submit_ts.get(coid)
                if ts0 is not None and hasattr(self, "obs_hist"):
                    self.obs_hist("lat_submit_to_fill_ms", (time.time() - ts0) * 1000.0)
                if etype == "FILL":
                    self._pending_submit_ts.pop(coid, None)

        # Libération anti-crossing (async si multi-pods)
        guard = getattr(self, "anti_crossing_guard", None)
        if guard and coid:
            try:
                rel = guard.release_for_coid(coid)
                if hasattr(rel, "__await__"):  # coroutine -> schedule
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(rel)
                    except RuntimeError:
                        # pas de loop → best-effort sync (ignore)
                        pass
            except Exception:
                pass

        inner = getattr(self, "_on_private_order_update_inner", None)
        if callable(inner):
            try:
                return inner(event)
            except Exception as e:
                if getattr(self, "log", None): self.log.exception("on_private_order_update inner failed", exc_info=e)

    def _cleanup_client(self, cid: Optional[str]) -> None:
        if not cid:
            return
        self._client_symbol_map.pop(cid, None)
        self._order_keys_hint.pop(cid, None)
        self._release_client_id(cid)

    def attach_ws_clients(self, *clients: Any):
        for c in clients:
            if c:
                try:
                    c.register_callback(self.handle_order_update)
                    self._ws_clients.append(c)
                except Exception:
                    logger.warning(
                        "[ExecutionEngine] WS client ne supporte pas register_callback()"
                    )

    async def start_streams(self):
        for c in self._ws_clients:
            try:
                r = c.start()
                await r if asyncio.iscoroutine(r) else None
            except Exception as e:
                report_nonfatal("ExecutionEngine", "start_stream_failed", e, phase="start_streams")
                logger.debug("[ExecutionEngine] start_stream failed", exc_info=False)

    # execution_engine.py — class ExecutionEngine

    async def _drain_submit_queue(self):
        """
        Drain de la file:
          - route + place chaque bundle
          - mesure enqueue→submit
          - micro-respiration configurable
        """
        import asyncio, time
        q = getattr(self, "_q", None)
        if q is None:
            return
        defer_ms = float(getattr(self.config, "engine_worker_idle_sleep_ms", 2.0))
        while True:
            item = await q.get()
            if item is None:
                q.task_done()
                break
            cid = item.get("cid")
            b = item.get("bundle", {})
            ts_en = float(item.get("ts", time.time()))
            try:
                if hasattr(self, "obs_hist"):
                    self.obs_hist("engine_enqueue_to_submit_ms", (time.time() - ts_en) * 1000.0)
                await self._route_and_place(b, cid)
            except Exception as e:
                if getattr(self, "log", None): self.log.exception("worker route_and_place failed", exc_info=e)
            finally:
                q.task_done()
            await asyncio.sleep(defer_ms / 1000.0)

    # execution_engine.py — class ExecutionEngine

    async def _route_and_place(self, bundle: dict, cid: str):
        """
        Route par venue et place les ordres.
        Conserve l’API des placers natifs (_binance_limit/_bybit_limit/_coinbase_limit).
        """
        legs = bundle.get("legs") or bundle.get("orders") or []
        meta = bundle.get("meta", {})
        if not legs:
            ex = bundle.get("exchange")
            od = bundle.get("order")
            if ex and od:
                await self._place_one(ex, od, cid, meta)
            return
        for leg in legs:
            ex = leg.get("exchange") or leg.get("ex") or meta.get("exchange")
            od = leg.get("order") or leg
            if not ex or not od:
                continue
            await self._place_one(ex, od, cid, meta)

    # execution_engine.py — class ExecutionEngine

    async def _place_one(self, exchange: str, order: dict, cid: str, meta: dict):
        """
        Respecte:
          - RL hard par CEX
          - Plafond in-flight local par CEX
          - Anti-crossing makers only (Single ou Multi-pods)
          - Armement SUBMIT→ACK
          - Priorités: hedges jamais bloqués
        """
        import time
        ex = (exchange or "").upper()

        kind = (meta.get("kind") or "").upper()
        branch = (meta.get("branch") or "").upper()
        is_hedge = (kind in ("HEDGE", "TAKER")) or (branch == "TT")

        # RL CEX (si présent)
        rl = None
        if ex == "BINANCE":
            rl = getattr(self, "_rl_binance_order", None)
        elif ex == "BYBIT":
            rl = getattr(self, "_rl_bybit_order", None)
        elif ex == "COINBASE":
            rl = getattr(self, "_rl_coinbase_order", None)
        if rl is not None:
            await rl.acquire()

        # Armement SUBMIT→ACK (pending map)
        t_submit = time.time()
        if not hasattr(self, "_pending_submit_ts"):
            self._pending_submit_ts = {}
        coid = order.get("clientOrderId") or order.get("client_order_id") or cid
        self._pending_submit_ts[coid] = t_submit

        # Anti-crossing makers only (P3 multi-pods si activé)
        if not is_hedge:
            self._ensure_anti_crossing_guard()
            guard = getattr(self, "anti_crossing_guard", None)
            if guard:
                try:
                    await guard.reserve_or_delay(ex, meta, order)
                except Exception:
                    pass

        # Plafond in-flight (local) + place natif
        async with self._inflight_scope(ex):
            if ex == "BINANCE":
                await self._binance_limit(order, meta)
            elif ex == "BYBIT":
                await self._bybit_limit(order, meta)
            elif ex == "COINBASE":
                await self._coinbase_limit(order, meta)
            else:
                raise ValueError(f"Unsupported exchange {ex}")

    async def resync_order(self, exchange: str, alias: str, order_id: str) -> bool:
        """
        P0: resynchronise un ordre précis via REST et émet un événement privé normalisé.
        Retourne True si au moins un event a été émis.
        """
        import asyncio, time, inspect

        ex, al = str(exchange).upper(), str(alias).upper()
        try:
            # 1) récupérer l'ordre via un client REST si dispo
            client = getattr(self, "rest_clients", {}).get(ex) or getattr(self, "http_clients", {}).get(ex)
            if not client or not hasattr(client, "get_order"):
                return False
            data = await client.get_order(alias=al, order_id=order_id)

            # 2) normaliser l'ordre en event privé
            ev = self._normalize_order_to_private_event(ex, al, data, source="resync_order")

            # 3) publier l'event (Hub si dispo, sinon handler interne)
            hub = getattr(self, "private_ws_hub", None)
            if hub and hasattr(hub, "on_event"):
                hub.on_event(ev)
            else:
                handler = getattr(self, "on_private_event", None)
                if handler:
                    if inspect.iscoroutinefunction(handler):
                        await handler(ev)
                    else:
                        handler(ev)
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            return False

    async def resync_alias(self, exchange: str, alias: str) -> bool:
        """
        P0: resynchronise "au mieux" un alias (récents fills/orders) et émet les events privés normalisés.
        Retourne True si au moins un event a été émis.
        """
        import asyncio, time

        ex, al = str(exchange).upper(), str(alias).upper()
        emitted = False
        try:
            client = getattr(self, "rest_clients", {}).get(ex) or getattr(self, "http_clients", {}).get(ex)
            hub = getattr(self, "private_ws_hub", None)

            # Priorité aux fills récents s'ils existent
            if client and hasattr(client, "list_recent_fills"):
                fills = await client.list_recent_fills(alias=al, limit=50)
                for f in fills or []:
                    ev = self._normalize_fill_to_private_event(ex, al, f, source="resync_alias")
                    if hub and hasattr(hub, "on_event"):
                        hub.on_event(ev)
                    else:
                        if hasattr(self, "on_private_event"):
                            await self.on_private_event(ev) if asyncio.iscoroutinefunction(
                                self.on_private_event) else self.on_private_event(ev)
                    emitted = True

            # À défaut, on remonte les ordres ouverts / récents
            if not emitted and client and hasattr(client, "list_open_orders"):
                orders = await client.list_open_orders(alias=al, limit=50)
                for o in orders or []:
                    ev = self._normalize_order_to_private_event(ex, al, o, source="resync_alias")
                    if hub and hasattr(hub, "on_event"):
                        hub.on_event(ev)
                    else:
                        if hasattr(self, "on_private_event"):
                            await self.on_private_event(ev) if asyncio.iscoroutinefunction(
                                self.on_private_event) else self.on_private_event(ev)
                    emitted = True
        except asyncio.CancelledError:
            raise
        except Exception:
            return emitted
        return emitted

    async def stop_streams(self):
        for c in self._ws_clients:
            try:
                r = c.stop()
                await r if asyncio.iscoroutine(r) else None
            except Exception as e:
                report_nonfatal("ExecutionEngine", "stop_stream_failed", e, phase="stop_streams")
                logger.debug("[ExecutionEngine] stop_stream failed", exc_info=False)

    def set_trade_modes(self, *, enable_tt: Optional[bool] = None, enable_tm: Optional[bool] = None):
        if enable_tt is not None:
            self.enable_taker_taker = bool(enable_tt)
        if enable_tm is not None:
            self.enable_taker_maker = bool(enable_tm)

    # --------------------- AUTOPILOT: API publique ---------------------
    def setup_rebalancing(self, rebalancing_callback: Optional[Callable[[dict], None]] = None) -> None:
        """Enregistre un callback optionnel pour internal_transfer/overlay et prépare la boucle."""
        self._rebalancing_cb = rebalancing_callback

    def start_rebalancing_loop(self) -> None:
        if self._rebal_loop_task or not self.rebal_autopilot_enabled:
            return
        interval = float(getattr(self.config, "rebal_check_interval_s", 10.0))
        if interval <= 0:
            logger.warning("[RebalAutopilot] disabled (interval<=0)")
            return
        self._rebal_loop_task = asyncio.create_task(self._rebal_loop())
        logger.info("[RebalAutopilot] loop started (%.2fs)", interval)

    def stop_rebalancing_loop(self) -> None:
        t = self._rebal_loop_task
        if t:
            t.cancel()
        self._rebal_loop_task = None
        logger.info("[RebalAutopilot] loop stopped")

    # --------------------------- public API ---------------------------
    async def execute(self, payload: Dict[str, Any]):
        # READINESS GUARD
        self._ensure_ready()

        now_ms = int(time.time() * 1000)
        trace = payload.setdefault("trace", {})
        trace.setdefault("trace_id", str(uuid.uuid4()))
        trace.setdefault("t_engine_submit_ms", now_ms)

        # Backpressure soft: ne bloque pas, refuse proprement si la file est pleine
        try:
            self.order_queue.put_nowait(payload)
        except asyncio.QueueFull:
            # MAJ des gauges
            self._update_submit_queue_gauge()
            return {"accepted": False, "reason": "backpressure"}

        # MAJ des gauges
        self.stats.queue_length = self.order_queue.qsize()
        self._update_submit_queue_gauge()
        return {"accepted": True}

    async def place_two_makers_with_hedge(self, bundle: dict):
        """
        MM: 2 makers en // (A & B) + hedge progressif côté manquant.
        Guards: net-bps (fees+slip+buffer) + queue-position bilatéral.
        """
        async with self._sem_mm_pairs:  # back-pressure MM
            pair = str(bundle.get("pair") or "").replace("-", "").upper()
            a = bundle.get("a") or {}
            b = bundle.get("b") or {}
            a_ex = str(a.get("ex") or "").upper()
            b_ex = str(b.get("ex") or "").upper()
            ttl_ms = int(bundle.get("ttl_ms", getattr(self.config, "mm_ttl_ms", 2200)))
            if not pair or not a_ex or not b_ex:
                return

            a_amt = float(((a.get("notional_quote") or {}).get("amount") or 0.0))
            b_amt = float(((b.get("notional_quote") or {}).get("amount") or 0.0))
            if min(a_amt, b_amt) <= 0:
                return

            # TOB bruts
            bid_a, ask_a = getattr(self.risk_manager, "get_top_of_book", lambda *args: (0.0, 0.0))(a_ex, pair) or (0.0,
                                                                                                                   0.0)
            bid_b, ask_b = getattr(self.risk_manager, "get_top_of_book", lambda *args: (0.0, 0.0))(b_ex, pair) or (0.0,
                                                                                                                   0.0)

            # Routing SELL/BID — BUY/ASK
            sell_ex = a_ex if bid_a >= bid_b else b_ex
            buy_ex = a_ex if ask_a <= ask_b else b_ex
            if sell_ex == buy_ex:
                sell_ex = a_ex if bid_a >= bid_b else b_ex
                buy_ex = b_ex if sell_ex == a_ex else a_ex

            bid_sel = bid_a if sell_ex == a_ex else bid_b
            ask_buy = ask_a if buy_ex == a_ex else ask_b
            if min(bid_sel, ask_buy) <= 0:
                return

            # -------- Guard 1: net-bps (fees + slip + buffer) --------
            # net ≈ (bid_sel/ask_buy - 1)  ; buffer en ratio
            fee_sell = float(self._fee_pct(sell_ex, "maker"))
            fee_buy = float(self._fee_pct(buy_ex, "maker"))
            net_ratio = (bid_sel / max(1e-12, ask_buy)) - 1.0
            buffer_ratio = (self.mm_min_net_bps + self.mm_slip_bps) / 1e4 + fee_sell + fee_buy
            if net_ratio <= buffer_ratio:
                await self._hist("trade", {
                    "_hist_kind": "MM", "pair": pair, "status": "skipped",
                    "reason": "net_bps_guard", "sell_ex": sell_ex, "buy_ex": buy_ex,
                    "bid_sel": bid_sel, "ask_buy": ask_buy,
                    "net_ratio": float(net_ratio), "buffer_ratio": float(buffer_ratio),
                    "timestamp": time.time(),
                })
                return

            # -------- Guard 2: queue-position bilatéral (USD) --------
            # Prix maker “au carnet”: SELL≈ask ; BUY≈bid (+/- pad ticks si tu veux)
            px_sell = ask_a if sell_ex == a_ex else ask_b
            px_buy = bid_a if buy_ex == a_ex else bid_b
            # Option: utiliser l’échelle de ticks maison
            px_sell = self._price_ladder_maker(sell_ex, pair, "SELL", (bid_a if sell_ex == a_ex else bid_b),
                                               (ask_a if sell_ex == a_ex else ask_b), idx=0) or px_sell
            px_buy = self._price_ladder_maker(buy_ex, pair, "BUY",
                                              (bid_a if buy_ex == a_ex else bid_b),
                                              (ask_a if buy_ex == a_ex else ask_b), idx=0) or px_buy

            ahead_sell_usd = self._estimate_ahead_usd(sell_ex, pair, px_sell, "SELL")
            ahead_buy_usd = self._estimate_ahead_usd(buy_ex, pair, px_buy, "BUY")

            if (ahead_sell_usd > self.mm_qpos_max_ahead_usd) or (ahead_buy_usd > self.mm_qpos_max_ahead_usd):
                await self._hist("trade", {
                    "_hist_kind": "MM", "pair": pair, "status": "skipped",
                    "reason": "queuepos_guard", "sell_ex": sell_ex, "buy_ex": buy_ex,
                    "ahead_sell_usd": float(ahead_sell_usd), "ahead_buy_usd": float(ahead_buy_usd),
                    "qpos_cap_usd": float(self.mm_qpos_max_ahead_usd), "timestamp": time.time(),
                })
                return

            # Montants par CEX
            amt_sell = a_amt if sell_ex == a_ex else b_amt
            amt_buy = a_amt if buy_ex == a_ex else b_amt
            bundle_id = f"MM-{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}"

            # Historisation "planning"
            await self._hist("trade", {
                "_hist_kind": "MM", "pair": pair, "status": "planning",
                "sell_ex": sell_ex, "buy_ex": buy_ex, "px_sell": px_sell, "px_buy": px_buy,
                "ahead_sell_usd": float(ahead_sell_usd), "ahead_buy_usd": float(ahead_buy_usd),
                "net_ratio": float(net_ratio), "buffer_ratio": float(buffer_ratio),
                "timestamp": time.time(),
            })

            # 1) Place makers
            sell_cid = await self._mm_place_maker(pair_key=pair, exchange=sell_ex, side="SELL",
                                                  amount_quote=amt_sell, bundle_id=bundle_id)
            buy_cid = await self._mm_place_maker(pair_key=pair, exchange=buy_ex, side="BUY",
                                                 amount_quote=amt_buy, bundle_id=bundle_id)

            deadline = time.monotonic() + (ttl_ms / 1000.0)
            is_dry = bool(getattr(self.config, "dry_run", True))
            progressive_task = None
            hedged_progress_usdc = 0.0
            progressive_started = False

            # 2) Boucle d’attente avec hedging progressif si asymétrie
            while time.monotonic() < deadline:
                if is_dry:
                    filled_sell = True;
                    filled_buy = True
                else:
                    filled_sell = self._mm_is_filled(sell_cid)
                    filled_buy = self._mm_is_filled(buy_cid)

                if filled_sell and filled_buy:
                    if progressive_task:
                        progressive_task.cancel()
                    MM_FILLS_BOTH.labels(pair).inc()
                    await self._hist("trade", {"_hist_kind": "MM", "pair": pair, "status": "both_filled",
                                               "sell_cid": sell_cid, "buy_cid": buy_cid, "timestamp": time.time()})
                    break

                # Asymétrie détectée → démarrer le hedging progressif (une seule fois)
                if self.mm_use_progressive_hedge and not progressive_started:
                    if filled_sell and not filled_buy:
                        progressive_started = True
                        progressive_task = self._spawn(
                            self._mm_progressive_hedge(
                                pair_key=pair, exchange=buy_ex, side="BUY",
                                notional_quote=amt_buy, schedule=self.mm_hedge_schedule,
                                deadline=deadline, bundle_id=bundle_id
                            ),
                            name=f"mm-hedge-{bundle_id}"

                        )
                        await self._hist("trade",
                                         {"_hist_kind": "MM", "pair": pair, "status": "progressive_hedge_start",
                                          "side": "BUY", "exchange": buy_ex, "schedule": self.mm_hedge_schedule,
                                          "timestamp": time.time()})
                    elif filled_buy and not filled_sell:
                        progressive_started = True
                        progressive_task = self._spawn(
                            self._mm_progressive_hedge(
                                pair_key=pair, exchange=sell_ex, side="SELL",
                                notional_quote=amt_sell, schedule=self.mm_hedge_schedule,
                                deadline=deadline, bundle_id=bundle_id
                            ),
                            name=f"mm-hedge-{bundle_id}"
                        )
                        await self._hist("trade",
                                         {"_hist_kind": "MM", "pair": pair, "status": "progressive_hedge_start",
                                          "side": "SELL", "exchange": sell_ex, "schedule": self.mm_hedge_schedule,
                                          "timestamp": time.time()})
                await asyncio.sleep(0.01)

            # 3) Épilogue à l’échéance: compléter jusqu’à mm_hedge_final_ratio si asymétrie
            try:
                filled_sell = self._mm_is_filled(sell_cid) if not is_dry else True
                filled_buy = self._mm_is_filled(buy_cid) if not is_dry else True

                if progressive_task:
                    try:
                        hedged_progress_usdc = await progressive_task
                    except asyncio.CancelledError:
                        # annulation normale à la fin si les deux makers ont rempli
                        pass
                    except Exception:
                        MM_PANIC_HEDGE_TOTAL.labels(pair).inc()

                if filled_sell and filled_buy:
                    pass  # déjà traité
                elif filled_sell and not filled_buy:
                    MM_SINGLE_FILL_HEDGED.labels(pair).inc()
                    # top-up si cumul < ratio final
                    final_usdc = max(0.0, self.mm_hedge_final_ratio * amt_buy - hedged_progress_usdc)
                    if final_usdc > 0:
                        await self._mm_panic_hedge_ioc(pair_key=pair, exchange=buy_ex, side="BUY",
                                                       amount_quote=final_usdc, bundle_id=bundle_id)
                    await self._hist("trade", {"_hist_kind": "MM", "pair": pair, "status": "single_fill_hedged",
                                               "filled": "SELL", "topup_usdc": final_usdc, "timestamp": time.time()})
                elif filled_buy and not filled_sell:
                    MM_SINGLE_FILL_HEDGED.labels(pair).inc()
                    final_usdc = max(0.0, self.mm_hedge_final_ratio * amt_sell - hedged_progress_usdc)
                    if final_usdc > 0:
                        await self._mm_panic_hedge_ioc(pair_key=pair, exchange=sell_ex, side="SELL",
                                                       amount_quote=final_usdc, bundle_id=bundle_id)
                    await self._hist("trade", {"_hist_kind": "MM", "pair": pair, "status": "single_fill_hedged",
                                               "filled": "BUY", "topup_usdc": final_usdc, "timestamp": time.time()})
                else:
                    # Aucun fill → rien à hedger (on annule)
                    await self._hist("trade", {"_hist_kind": "MM", "pair": pair, "status": "ttl_cancel",
                                               "timestamp": time.time()})

            except Exception:
                MM_PANIC_HEDGE_TOTAL.labels(pair).inc()
                raise
            finally:
                # 4) Cancel makers restants
                await self._mm_cancel_open_makers([sell_cid, buy_cid])

    # === execution_engine.py ===
    # Dans class ExecutionEngine

    # modules/execution_engine.py — class ExecutionEngine

    async def submit(self, bundle: dict):
        """
        Soumission non-bloquante:
          - Garde readiness strict (hedges jamais bloqués si override conf)
          - Idempotence via CID déterministe
          - Backpressure (watermarks configurables + policy defer/reject)
          - Observabilité: enqueue_ms + profondeur file
        """
        # 0) Readiness
        drop_makers = bool(getattr(self.config, "engine_ready_drop_makers_when_not_ready", False))
        if not getattr(self, "_ready", True) and not getattr(self, "is_ready", lambda: True)():
            # Hedges (TT) autorisés en dégradé si le flag est False
            kind = (bundle.get("meta", {}).get("kind") or "").upper()
            if drop_makers or kind in ("MAKER", "MAKER_TM", "MAKER_MM"):
                raise RuntimeError("ENGINE_NOT_READY")

        # 1) Idempotence
        if not hasattr(self, "_seen_cids"):
            self._seen_cids = set()
        import hashlib, json, time, asyncio
        h = hashlib.sha256()
        h.update(json.dumps(bundle, sort_keys=True, default=str).encode("utf-8"))
        cid = h.hexdigest()[:16]
        if cid in self._seen_cids:
            return

        # 2) Queue & watermarks
        if not hasattr(self, "_q"):
            maxsize = int(getattr(self.config, "engine_queue_max", 2048))
            import asyncio as _asyncio
            self._q = _asyncio.Queue(maxsize=max(64, maxsize))

        high_wm = int(getattr(self.config, "engine_queue_high_wm", int(self._q.maxsize * 0.85)))
        policy = str(getattr(self.config, "engine_enqueue_overflow_policy", "defer")).lower()  # "defer"|"reject"

        if self._q.qsize() >= high_wm and policy == "reject":
            raise RuntimeError("ENGINE_QUEUE_HIGH_WATERMARK")

        if self._q.qsize() >= high_wm and policy == "defer":
            await asyncio.sleep(float(getattr(self.config, "engine_defer_sleep_ms", 8)) / 1000.0)

        # 3) Enqueue + obs
        t0 = time.time()
        await self._q.put({"cid": cid, "bundle": bundle, "ts": t0})
        self._seen_cids.add(cid)
        try:
            if hasattr(self, "obs_hist"):
                self.obs_hist("engine_enqueue_ms", (time.time() - t0) * 1000.0)
        except Exception:
            pass
        try:
            if hasattr(self, "_update_submit_queue_gauge"):
                self._update_submit_queue_gauge()
        except Exception:
            pass

    async def start(self):
        """
        Démarre l'engine :
          - crée la session HTTP si live (dry_run=False),
          - lance les workers,
          - démarre les flux WS privés,
          - démarre l’autopilot de rebalancing,
          - passe en 'ready' si configuré.
        Idempotent : ne fait rien si déjà démarré.
        """
        if self.running:
            return

        live_mode = not getattr(self.config, "dry_run", True)
        if live_mode and aiohttp is None:
            raise RuntimeError("aiohttp manquant: impossible de démarrer en live (dry_run=False).")

        # Assure que le pacer est prêt (si disponible)
        try:
            self._ensure_pacer_on()
        except Exception:
            logger.debug("[ExecutionEngine] _ensure_pacer_on() a échoué (non bloquant).", exc_info=False)

        self.running = True
        created_session = False

        try:
            # Session HTTP en mode live
            if live_mode:
                if (getattr(self, "_session", None) is None) or getattr(self._session, "closed", False):
                    self._session = aiohttp.ClientSession()
                    created_session = True

            # Workers
            worker_count = max(1, int(getattr(self, "_max_concurrent", 1)))
            self._workers = [
                asyncio.create_task(self._worker_loop(i), name=f"exec-worker-{i}")
                for i in range(worker_count)
            ]

            # Flux WS privés
            try:
                await self.start_streams()
            except Exception:
                logger.debug("[ExecutionEngine] start_streams() a échoué (non bloquant).", exc_info=False)

            # Boucle d’autopilot rebalancing
            try:
                if self.rebal_autopilot_enabled:
                    self.start_rebalancing_loop()
            except Exception:
                logger.debug("[ExecutionEngine] rebalancing loop start failed (non bloquant).", exc_info=False)

            # Readiness automatique
            if bool(getattr(self, "_auto_ready_on_start", False)):
                self.mark_ready()

            # Observabilité
            set_engine_running(True)
            try:
                set_engine_queue(self.get_queue_depth())
            except Exception:
                set_engine_queue(0)

            logger.info("[ExecutionEngine] ✅ Démarré (%d workers).", len(self._workers))

        except Exception:
            # Restauration d’un état propre en cas d’échec de démarrage
            logger.exception("[ExecutionEngine] Échec au démarrage — rollback en cours.")
            self.running = False

            # Annule les workers déjà créés
            for w in self._workers:
                w.cancel()
            await asyncio.gather(*self._workers, return_exceptions=True)
            self._workers.clear()

            # Ferme la session HTTP créée ici
            if created_session and getattr(self, "_session", None):
                with contextlib.suppress(Exception):
                    await self._session.close()
                self._session = None

            set_engine_running(False)
            set_engine_queue(0)
            raise

    # --------------------------- lifecycle ---------------------------
    async def stop(self):
        if not self.running:
            return
        self.running = False

        # ... annuler workers / ws / boucles internes d’abord ...
        for w in self._workers:
            w.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

        # Puis annuler proprement les tâches de fond
        try:
            await self._cancel_bg_tasks(timeout=2.0)
        except Exception:
            logger.debug("Failed to cancel background tasks cleanly", exc_info=True)

        # ... ensuite fermer la session HTTP, arrêter les streams, etc. ...
        if self._session:
            await self._session.close()
            self._session = None

        # reste de ton stop()…

        try:
            await self.stop_streams()
        except Exception:
            logger.debug("[ExecutionEngine] WS streams stop failed", exc_info=False)
        # Autopilot
        try:
            self.stop_rebalancing_loop()
        except Exception:
            logger.debug("[ExecutionEngine] rebal loop stop failed", exc_info=False)

        # READINESS: on repasse en not-ready
        try:
            self.ready_event.clear()
        except Exception:
            logging.exception("Unhandled exception")
        self._submit_ts_ns.clear()
        self._ack_marked.clear()
        self._ack_ring.clear()

        logger.info("[ExecutionEngine] 🛑 Stoppé.")
        set_engine_running(False)
        set_engine_queue(0)

    async def restart(self, reason: str = "inconnu"):
        logger.warning(f"[ExecutionEngine] 🔁 Redémarrage demandé : {reason}")
        self.last_restart_reason = reason
        await self.stop()
        await self.start()

    # --------------------------- worker loop ---------------------------
    async def _worker_loop(self, wid: int):
        try:
            while self.running:
                try:
                    payload = await asyncio.wait_for(self.order_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                self.stats.queue_length = self.order_queue.qsize()
                self._update_submit_queue_gauge()
                set_engine_queue(self.stats.queue_length)
                # PACER: push queue depth (drain latency + signal boucle fermée)
                try:
                    self._pacer_update()
                except Exception:
                    pass

                t0 = time.time()
                try:
                    t_lower = str((payload or {}).get("type", "") or "").lower()

                    if t_lower == "internal_transfer":
                        await self._handle_internal_transfer(payload)
                        continue

                    if t_lower == "rebalancing_trade":
                        payload = self._convert_rebalancing_trade_to_bundle(payload)

                    if t_lower in ("arbitrage", "arbitrage_bundle"):
                        payload = self._convert_arbitrage_to_bundle(payload)

                    if payload.get("type") in ("bundle", "rebalancing"):
                        await self._exec_bundle(payload)
                    else:
                        await self._exec_single(payload)
                except Exception as e:
                    ENGINE_WORKER_ERRORS_TOTAL.labels(phase=f"W{wid}").inc()
                    report_nonfatal("ExecutionEngine", "worker_handle_payload_failed", e,
                    phase=f"W{wid}")
                    logger.exception(
                    f"[ExecutionEngine/W{wid}] Erreur traitement payload: {e}")
                    self.stats.total_failed += 1
                    self.stats.error_count += 1
                finally:
                    self.stats.execution_latency = time.time() - t0
                    self.stats.last_trade_time = time.time()
                    observe_engine_latency(self.stats.execution_latency)
                    self.order_queue.task_done()
        except asyncio.CancelledError:
            logger.info(f"[ExecutionEngine/W{wid}] annulé.")

    # --------------------------- WS order updates ---------------------------
    def handle_order_update(self, event: Dict[str, Any]):
        clid = event.get("client_id")
        if not clid:
            return

        fsm = self._order_fsm.get(clid)
        st = str(event.get("status", "")).upper()

        # --- FSM transitions + métriques ACK dédoublonnées ---
        if fsm:
            if st == "ACK":
                fsm.on_ack(event.get("exchange_order_id", ""), None)
                try:
                    key = f"CLID:{clid}"
                    if self._ack_mark_once(key):
                        ts_ns = getattr(self, "_submit_ts_ns", {}).pop(clid, None)
                        if ts_ns is None:
                            return  # pas de timestamp → pas de latence à tracer

                        now_ns = time.perf_counter_ns()
                        latency_ms = max(0, int((now_ns - ts_ns) / 1_000_000))

                        ex_lab = (self._client_symbol_map.get(clid, ("?", "?"))[0] or "?")
                        ENGINE_SUBMIT_TO_ACK_MS.labels(exchange=str(ex_lab)).observe(latency_ms)
                        mark_engine_ack(latency_ms)  # wrapper tolérant: reçoit un delta en ms
                except Exception:
                    logging.exception("Unhandled exception")

            elif st == "FILLED":
                fsm.on_filled()
                try:
                    if fsm.t_ack_ms and fsm.t_filled_ms:
                        key = f"FILL:{clid}"
                        if self._ack_mark_once(key):
                            delta = max(0, int(fsm.t_filled_ms - fsm.t_ack_ms))
                            ENGINE_ACK_TO_FILL_MS.observe(delta)
                            asyncio.create_task(self._hist("latency_watermark", {
                                "kind": "ACK_TO_FILL",
                                "client_id": clid,
                                "exchange": self._client_symbol_map.get(clid, ("?", "?"))[0],
                                "symbol": self._client_symbol_map.get(clid, ("?", "?"))[1],
                                "t_sent_ms": fsm.t_sent_ms,
                                "t_ack_ms": fsm.t_ack_ms,
                                "t_filled_ms": fsm.t_filled_ms,
                                "latency_ms": delta,
                                "timestamp": time.time(),
                            }))
                except Exception:
                    logging.exception("Unhandled exception")

            elif st == "REJECTED":
                fsm.on_reject(event.get("reason", ""))
            elif st == "CANCELED":
                fsm.on_cancel()

        # --- TM hedge driver (partial/filled) ---
        plan = self._tm_hedges.get(clid)
        try:
            if plan:
                filled = float(event.get("filled_qty", 0.0))
                prev = float(plan.get("seen_filled_qty", 0.0))
                delta = max(0.0, filled - prev)
                plan["seen_filled_qty"] = filled

                if delta > 0:
                    hedge_ex = plan["hedge_exchange"]
                    sym = plan["symbol"]
                    pk = sym.replace("-", "")
                    px = 0.0
                    get_tob = getattr(self.risk_manager, "get_top_of_book", None)
                    if callable(get_tob):
                        bid, ask = get_tob(hedge_ex, pk) or (0.0, 0.0)
                        px = float(bid if plan["hedge_side"] == "SELL" else ask)
                    if px <= 0:
                        px = float(plan.get("ref_price", 0.0))

                    hedge_ratio = float(plan.get("hedge_ratio", getattr(self.config, "tm_neutral_hedge_ratio", 1.0)))
                    usdc_amt = max(0.0, delta * px * hedge_ratio)
                    if usdc_amt > 0:
                        _, q = SymbolUtils.split_base_quote(sym)
                        hedge_order = {
                            "type": "single",
                            "exchange": hedge_ex,
                            "symbol": sym,
                            "side": plan["hedge_side"],
                            "price": px,
                            "volume_usdc": usdc_amt,
                            "client_id": f"H{int(time.time() * 1000)}",
                            "meta": {
                                "best_price": px,
                                "best_ts": int(time.time() * 1000),
                                "tif_override": "IOC",
                                "fastpath_ok": True,
                                "skip_inventory": True,
                                "bundle_id": plan.get("bundle_id"),
                                "slice_id": plan.get("slice_id"),
                                "slice_group": plan.get("slice_group"),
                                "slice_weight": plan.get("slice_weight"),
                                "planned_usdc": usdc_amt,
                                # hedge = taker → router TT pour l’alias
                                "strategy": "TT",
                                "account_alias": self.wallet_router.pick_by_quote(hedge_ex, "TT", q),
                            },
                        }
                        asyncio.create_task(self._exec_single(hedge_order))
        except Exception:
            logger.debug("[TM Hedge] erreur handle_order_update", exc_info=False)

        # --- cleanup sur statut terminal ---
        # 2) dans handle_order_update(), en bas (cleanup terminal):
        if st in {"FILLED", "REJECTED", "CANCELED"}:
            self._tm_hedges.pop(clid, None)
            self._seen_client_ids.discard(clid)
            self._ack_marked.discard(f"CLID:{clid}")
            self._ack_marked.discard(f"FILL:{clid}")
            self._ack_marked.discard(clid)  # par prudence
            self._submit_ts_ns.pop(clid, None)  # purge haute précision

    # --------------------------- AUTOPILOT: boucle -------------------------
    async def _rebal_loop(self):
        interval = float(getattr(self.config, "rebal_check_interval_s", 10.0))
        persist_s = float(getattr(self.config, "rebal_persist_s", 180.0))
        min_usdc = float(getattr(self.config, "rebal_min_usdc", 1500.0))
        allow_loss_bps = float(getattr(self.config, "rebal_allow_loss_bps", 3.0))

        while True:
            try:
                await asyncio.sleep(interval)

                # 1) Détection
                imb = (
                    self.risk_manager.rebal_mgr.detect_imbalance()
                    if hasattr(self.risk_manager, "rebal_mgr")
                    else self.risk_manager.detect_imbalance()
                )
                if not imb:
                    self._rebal_last_plan_key = None
                    self._rebal_first_seen_ts = 0.0
                    continue

                plan_builder = (
                    self.risk_manager.rebal_mgr
                    if hasattr(self.risk_manager, "rebal_mgr")
                    else self.risk_manager
                )
                plan = plan_builder.build_plan(imb)
                try:
                    plan_builder.push_history(imb, plan)
                except Exception:
                    logging.exception("Unhandled exception")

                # 1.a) Overlay + intra-CEX : callback optionnel (non bloquant)
                self._dispatch_side_ops(plan)

                # 2) Cross-CEX ?
                cross = plan.get("cross_cex") or None
                if not cross:
                    self._rebal_last_plan_key = None
                    self._rebal_first_seen_ts = 0.0
                    continue

                # 3) Garde-fous amont
                amount_quote = float(cross.get("amount_quote") or 0.0)
                if amount_quote < min_usdc:
                    continue

                # 4) Persistance du même plan
                key = self._plan_signature(cross)
                now = time.time()
                if key != self._rebal_last_plan_key:
                    self._rebal_last_plan_key = key
                    self._rebal_first_seen_ts = now

                if (now - self._rebal_first_seen_ts) < persist_s:
                    continue
                if (now - self._rebal_last_fire_ts) < self._rebal_cooldown_s:
                    continue

                # 5) Check net bps attendu
                fee_from = self._fee_pct(cross["from_exchange"], "maker")
                fee_to = self._fee_pct(cross["to_exchange"], "taker")

                slip_from = 0.0
                slip_to = 0.0

                estimator = getattr(self.risk_manager, "rebal_mgr", self.risk_manager)
                est_bps = estimator.estimate_cross_cex_net_bps(
                    pair_key=cross["pair_key"],
                    from_exchange=cross["from_exchange"],
                    to_exchange=cross["to_exchange"],
                    fee_from_pct=fee_from,
                    fee_to_pct=fee_to,
                    slip_from_pct=slip_from,
                    slip_to_pct=slip_to,
                )

                if est_bps < -allow_loss_bps:
                    logger.info(
                        "[RebalAutopilot] skip cross (est_bps=%.1f < -%.1f)",
                        est_bps,
                        allow_loss_bps,
                    )
                    continue

                # 6) Conversion amount_quote -> qty base (via mid)
                mid = self._get_mid_for_pair_xcex(
                    cross["from_exchange"], cross["to_exchange"], cross["pair_key"]
                )
                if mid <= 0:
                    logger.info("[RebalAutopilot] skip cross (mid indisponible)")
                    continue

                base_qty = round(amount_quote / mid, 6)
                if base_qty <= 0:
                    continue

                # 7) Générer et router le bundle (via file engine)
                payload = self._make_rebal_payload_for_engine(cross, amount_quote, est_bps)
                await self.submit(payload)

                self._rebal_last_fire_ts = now


            except asyncio.CancelledError:

                break

            except Exception as e:

                report_nonfatal("ExecutionEngine", "rebal_loop_error", e, phase="_rebal_loop")

                logger.exception("[RebalAutopilot] loop error")

    # -------------------- AUTOPILOT helpers --------------------
    def _dispatch_side_ops(self, plan: Dict[str, Any]) -> None:
        cb = self._rebalancing_cb
        # overlay netting
        for op in plan.get("overlay_comp", []) or []:
            if cb:
                try:
                    cb({"type": "overlay_compensation", **op})
                except Exception:
                    report_nonfatal("ExecutionEngine", "overlay_callback_failed", None,
                                                         phase="rebal_side_ops")
                    logger.exception("[RebalAutopilot] overlay cb error")
            else:
                logger.info("[RebalAutopilot] overlay_comp: %s", op)
        # intra-CEX transfers
        for op in plan.get("internal_transfers", []) or []:
            if cb:
                try:
                    cb({"type": "internal_transfer", **op})
                except Exception:
                   report_nonfatal("ExecutionEngine", "transfer_callback_failed", None,
                                                         phase="rebal_side_ops")
                   logger.exception("[RebalAutopilot] transfer cb error")
            else:
                # fallback: on pousse un job interne dans la file engine
                job = {"type": "internal_transfer", **op}
                try:
                    asyncio.create_task(self.submit(job))
                except Exception:
                       report_nonfatal("ExecutionEngine", "internal_transfer_enqueue_failed", None,
                                                         phase="rebal_side_ops")
                       logger.debug("[RebalAutopilot] internal_transfer enqueue fail", exc_info=False)
    def _plan_signature(self, cross: Dict[str, Any]) -> str:
        # signature “stable” du plan cross (ignorer petites fluctuations)
        f = cross["from_exchange"]
        t = cross["to_exchange"]
        pk = cross["pair_key"]
        q = cross.get("quote", "USDC")
        amt = round(float(cross.get("amount_quote") or 0.0) / 100.0)  # bucket 100 USDC
        return f"{f}->{t}|{pk}|{q}|{amt}"


    def _get_mid_for_pair_xcex(self, ex_from: str, ex_to: str, pair_key: str) -> float:
        # essaie un mid depuis rebal_mgr (il maintient les OB par CEX)
        pk = self._fmt_pair(pair_key)
        src = getattr(self.risk_manager, "rebal_mgr", self.risk_manager)
        ob_from = self._ob_latest(ex_from, pk)
        ob_to   = self._ob_latest(ex_to, pk)
        try:
            b1, a1 = float(ob_from.get("best_bid") or 0), float(ob_from.get("best_ask") or 0)
            b2, a2 = float(ob_to.get("best_bid") or 0), float(ob_to.get("best_ask") or 0)
        except Exception:
            b1 = a1 = b2 = a2 = 0.0
        mids = []
        if b1 > 0 and a1 > 0 and b1 < a1:
            mids.append(0.5 * (b1 + a1))
        if b2 > 0 and a2 > 0 and b2 < a2:
            mids.append(0.5 * (b2 + a2))
        return sum(mids) / len(mids) if mids else 0.0

    def _make_rebal_payload_for_engine(
        self, cross: Dict[str, Any], amount_quote: float, est_bps: float
    ) -> Dict[str, Any]:
        """Construit un payload rebalancing_trade consommé par le worker → pipeline TM NEUTRAL."""
        pk = cross["pair_key"].replace("-", "").upper()
        sell_leg = {
            "exchange": cross["from_exchange"],
            "symbol": pk,
            "side": "SELL",
            "price": 0.0,  # hydraté par _hydrate_prices_for_legs
            "volume_usdc": amount_quote,
            "meta": {"type": "rebalancing"},
        }
        buy_leg = {
            "exchange": cross["to_exchange"],
            "symbol": pk,
            "side": "BUY",
            "price": 0.0,
            "volume_usdc": amount_quote,
            "meta": {"type": "rebalancing"},
        }
        opp = {
            "type": "rebalancing_trade",
            "pair": pk,
            "net_bps": float(est_bps),
            "payload": {"legs": [sell_leg, buy_leg]},
            "timeout_s": float(getattr(self.config, "order_timeout_s", 2.5)),
        }
        return opp

    # --------------------------- PATCH: handlers utils -----------------------
    async def _handle_internal_transfer(self, job: Dict[str, Any]) -> None:
        # READINESS GUARD (utile si on veut bloquer même les side-ops tant que non prêt)
        self._ensure_ready()

        ex = str(job.get("exchange") or "").upper()
        from_alias = str(job.get("from_alias") or "TT").upper()
        to_alias = str(job.get("to_alias") or "TT").upper()
        amt = float(job.get("amount_usdc") or job.get("amount") or 0.0)
        ccy = str(job.get("ccy") or "USDC").upper()
        if amt <= 0:
            return
        if getattr(self.config, "dry_run", True):
            try:
                self.risk_manager.adjust_virtual_balance(ex, from_alias, ccy, -amt)
                self.risk_manager.adjust_virtual_balance(ex, to_alias, ccy, +amt)
                logger.info("↔️  Transfer interne %s: %s→%s  %.2f %s", ex, from_alias, to_alias, amt, ccy)
            except Exception:
                logger.exception("[Engine] adjust_virtual_balance failed")
        else:
            logger.info(
                "↔️  (LIVE) Transfer interne %s: %s→%s  %.2f %s (stub)", ex, from_alias, to_alias, amt, ccy
            )

    def _best_price_from_rm(self, exchange: str, pair_key: str, side: str) -> Tuple[float, Optional[int]]:
        try:
            ex = self._ex_upper(exchange)
            pk = self._fmt_pair(pair_key)
            ob = self._ob_latest(ex, pk) or {}
            # timestamp (ms) côté OB, sinon None
            try:
                ts = int(ob.get("exchange_ts_ms") or ob.get("recv_ts_ms") or 0) or None
            except Exception:
                ts = None

            side_up = str(side or "").upper()
            if side_up == "BUY":
                # BUY (taker) → on paie l'ASK
                best = float(ob.get("best_ask") or 0.0)
            else:
                # SELL (taker) → on touche le BID
                best = float(ob.get("best_bid") or 0.0)

            if best <= 0.0:
                # Fallback minimal via TOB du RM
                ENGINE_BEST_PRICE_MISSING_TOTAL.labels(exchange=ex, pair=pk, side=side_up).inc()

                get_tob = getattr(self.risk_manager, "get_top_of_book", None)
                if callable(get_tob):
                    bid, ask = get_tob(ex, pk) or (0.0, 0.0)
                    best = (ask if side_up == "BUY" else bid) or 0.0

            return float(best), ts
        except Exception as e:
            ENGINE_BEST_PRICE_MISSING_TOTAL.labels(
                exchange=str(exchange).upper(),
                pair=str(pair_key).upper().replace("-", ""),
                side=str(side).upper()
            ).inc()
            return 0.0, None

    def _ensure_anti_crossing_guard(self):
        """
        Sélectionne le guard anti-crossing:
          - Multi-pods si pods_enabled True
          - Sinon Single-pod (in-memory)
        """
        if getattr(self, "anti_crossing_guard", None):
            return
        cfg = getattr(self, "config", None)
        pods_enabled = getattr_bool(cfg, "pods_enabled", False) if cfg else False
        try:
            if pods_enabled:
                self.anti_crossing_guard = AntiCrossingGuardMultiPod(self)
            else:
                # fallback single-pod si déjà ajouté en P2
                if 'AntiCrossingGuardSinglePod' in globals():
                    self.anti_crossing_guard = AntiCrossingGuardSinglePod(self)
                else:
                    self.anti_crossing_guard = None
        except Exception:
            self.anti_crossing_guard = None

    def _hydrate_prices_for_legs(self, legs: List[Dict[str, Any]]) -> None:
        for l in legs or []:
            ex = str(l.get("exchange", ""))
            sym = str(l.get("symbol", ""))
            pk = sym.replace("-", "")
            side = str(l.get("side", ""))
            meta = l.setdefault("meta", {})
            price = float(l.get("price") or 0.0)
            if price <= 0:
                best, ts = self._best_price_from_rm(ex, pk, side)
                if best > 0:
                    l["price"] = float(best)
                    meta.setdefault("best_price", float(best))
                    if ts:
                        meta.setdefault("best_ts", int(ts))
                else:
                    l["price"] = 0.0

    def _convert_rebalancing_trade_to_bundle(self, opp: Dict[str, Any]) -> Dict[str, Any]:
        legs = (opp.get("payload") or {}).get("legs", []) or []
        if len(legs) != 2:
            return opp
        self._hydrate_prices_for_legs(legs)
        for l in legs:
            l.setdefault("meta", {})
        payload = {
            "type": "bundle",
            "legs": legs,
            "pair_key": opp.get("pair") or legs[0].get("symbol", "{}").replace("-", ""),
            "expected_net_spread": float(opp.get("net_bps", 0.0) or 0.0) / 1e4,
            "timeout_s": getattr_float(self.config, "order_timeout_s", 2.5),
            "meta": {
                "strategy": "TM",
                "type": "rebalancing",
                "allow_loss_bps": getattr_float(self.config, "rebal_allow_loss_bps", 0.0),
                "tm": {"mode": "NEUTRAL", "hedge_ratio": 1.0},
            },
        }
        return payload

    def _convert_arbitrage_to_bundle(self, opp: Dict[str, Any]) -> Dict[str, Any]:
        if str(opp.get("type")).lower() == "bundle":
            return self._normalize_bundle_fields(opp)
        legs = opp.get("legs") or (opp.get("payload") or {}).get("legs", []) or []
        if not legs:
            legs = (opp.get("payload") or {}).get("legs", []) or []
        if len(legs) == 2:
            self._hydrate_prices_for_legs(legs)
        out = dict(opp)
        out["type"] = "bundle"
        out["legs"] = legs
        if "pair_key" not in out:
            out["pair_key"] = out.get("pair") or (
                legs[0].get("symbol", "").replace("-", "") if legs else None
            )
        return self._normalize_bundle_fields(out)

    def _normalize_bundle_fields(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        meta = dict(payload.get("meta") or {})
        if "metadata" in payload:
            md = payload.pop("metadata") or {}
            if isinstance(md, dict):
                meta = {**md, **meta}
        if "strategy" in payload and "strategy" not in meta:
            meta["strategy"] = payload.pop("strategy")
        if "tm" in payload and "tm" not in meta:
            meta["tm"] = payload.pop("tm")
        payload["meta"] = meta
        if "pair_key" not in payload:
            pk = payload.get("pair")
            if pk:
                payload["pair_key"] = str(pk).replace("-", "")
        payload.setdefault("timeout_s", getattr_float(self.config, "order_timeout_s", 2.5))
        return payload

    # --------------------------- helpers ---------------------------
    @staticmethod
    def _to_ms(ts: Optional[float]) -> Optional[float]:
        if ts is None:
            return None
        try:
            t = float(ts)
        except Exception:
            return None
        return t if t > 1e12 else (t * 1000.0)

    def _within_guard(self, price: float, anchor: float) -> bool:
        if anchor <= 0:
            return True
        dev = abs(price - anchor) / anchor
        return dev <= self.max_price_deviation_pct

    def _within_anchor_guard(
        self, price: float, anchor_price: float, anchor_ts_ms: Optional[float]
    ) -> bool:
        if anchor_price <= 0:
            return True
        if anchor_ts_ms is None:
            return self._within_guard(price, anchor_price)
        now_ms = time.time() * 1000.0
        age = max(0.0, now_ms - anchor_ts_ms)
        if age > float(self.anchor_max_staleness_ms):
            return False
        allowed = self.max_price_deviation_pct
        if age >= float(self.anchor_halve_guard_ms):
            allowed *= 0.5
        dev = abs(price - anchor_price) / anchor_price
        return dev <= allowed

    def _get_tif(self, order_meta: Dict[str, Any], *, is_bundle: bool) -> str:
        tif = str(
            order_meta.get("tif_override")
            or (self.default_tif_bundle if is_bundle else self.default_tif_single)
        ).upper()
        return tif if tif in ("IOC", "FOK", "GTC") else "GTC"

    def _normalize_for_exchange(
            self, exchange: str, symbol: str, side: str, qty: float, price: float
    ) -> Tuple[float, float]:
        try:
            gf = getattr(self.config, "get_pair_filters", None)
            if gf is None:
                return round(qty, 8), round(price, 8)
            f = gf(exchange, symbol) or {}
            ts = float(f.get("tick_size", 0) or 0.0)
            ss = float(f.get("step_size", 0) or 0.0)
            mn = float(f.get("min_notional", 0) or 0.0)

            from math import floor, ceil
            def _round_step(x: float, step: float) -> float:
                if step <= 0:
                    return float(x)
                return float(floor((float(x) + 1e-12) / step) * step)

            def _round_price(x: float, step: float, side: str) -> float:
                if step <= 0:
                    return float(x)
                s = str(side or "").upper()
                if s == "BUY":  # BUY → arrondi vers le haut
                    return float(ceil((float(x) - 1e-12) / step) * step)
                # SELL → arrondi vers le bas
                return float(floor((float(x) + 1e-12) / step) * step)

            n_price = _round_price(price, ts, side) if ts > 0 else price
            n_qty = _round_step(qty, ss) if ss > 0 else qty
            if mn > 0 and (n_price * n_qty) < mn:
                need_qty = (mn / max(n_price, 1e-12))
                n_qty = _round_step(need_qty, ss) if ss > 0 else need_qty
            return float(round(max(0.0, n_qty), 8)), float(round(max(0.0, n_price), 8))
        except Exception:
            return round(qty, 8), round(price, 8)

    def _fee_pct(self, exchange: str, role: str) -> float:
        ex = str(exchange).upper()
        role = "maker" if role.lower().startswith("mak") else "taker"

        # 1) RiskManager
        fn = getattr(self.risk_manager, "get_fee_pct", None)
        if callable(fn):
            try:
                return float(fn(ex, role))
            except TypeError:
                return float(fn(ex, role, None))
            except Exception:
                logging.exception("Unhandled exception")

        # 2) Engine fee_map_pct (2 formats supportés)
        mp = getattr(self, "fee_map_pct", {}) or {}
        ent = mp.get(ex)
        if isinstance(ent, dict):
            v = ent.get(role)
            if v is not None:
                return float(v)
        elif isinstance(ent, (int, float)):
            return float(ent)

        # 3) Fallbacks config
        fb = (getattr(self.config, "fee_fallbacks", {}) or {}).get(ex) or {}
        return float(fb.get(role, 0.0))

    @staticmethod
    def _norm_ex(ex: str) -> str:
        return str(ex or "").strip().upper()

    def _price_ladder_maker(
        self, exchange: str, symbol: str, side: str, best_bid: float, best_ask: float, idx: int
    ) -> float:
        tick = 0.0
        gf = getattr(self.config, "get_pair_filters", None)
        if callable(gf):
            try:
                f = gf(exchange, symbol) or {}
                tick = float(f.get("tick_size", 0) or 0.0)
            except Exception:
                tick = 0.0
        k = (self.maker_pad_ticks + idx) if tick > 0 else 0
        if side.upper() == "SELL":
            base = max(best_ask, 0.0)
            return max(0.0, base + (k * tick))
        else:
            base = max(best_bid, 0.0)
            return max(0.0, base - (k * tick))

    # ======= Queue-position (USD devant à notre prix) =======
    # ======= Queue-position (QUOTE devant à notre prix) =======
    # ======= Queue-position (QUOTE devant à notre prix) =======
    def _estimate_ahead_quote(self, exchange: str, symbol: str, price: float, side: str) -> float:
        """
        Renvoie le notional en devise de cotation (QUOTE) devant notre prix,
        en multipliant la quantité cumulée 'devant' par le meilleur prix pertinent.
        """
        try:
            ex = self._ex_upper(exchange)
            pk = self._fmt_pair(symbol)
            ob = self._ob_latest(ex, pk) or {}
            levels = ob.get("asks" if side.upper() == "SELL" else "bids") or []

            ahead_qty = 0.0
            if side.upper() == "SELL":
                for p, q in levels:
                    if p < price:
                        ahead_qty += float(q)
                    else:
                        break
            else:
                for p, q in levels:
                    if p > price:
                        ahead_qty += float(q)
                    else:
                        break

            best = float(ob.get("best_ask") if side.upper() == "SELL" else ob.get("best_bid") or 0.0)
            if best <= 0:
                bid, ask = getattr(self.risk_manager, "get_top_of_book", lambda *a: (0.0, 0.0))(ex, pk) or (0.0, 0.0)
                best = (ask if side.upper() == "SELL" else bid)
            return float(ahead_qty) * max(1e-12, best)
        except Exception:
            return 0.0

    def _apply_tm_controls_and_caps(self, bundle: dict) -> None:
        tmc = bundle.get("tm_controls") or {}
        caps = bundle.get("caps") or {}
        if "queuepos_max_usd" in tmc and tmc["queuepos_max_usd"] is not None:
            self.tm_queuepos_max_ahead_usd = int(tmc["queuepos_max_usd"])
        if "ttl_ms" in tmc and tmc["ttl_ms"] is not None:
            self.tm_exposure_ttl_ms = int(tmc["ttl_ms"])
        if "hedge_ratio" in tmc and tmc["hedge_ratio"] is not None:
            self.tm_exposure_ttl_hedge_ratio = float(tmc["hedge_ratio"])
        if tmc.get("ioc_only", False):
            for o in bundle.get("orders", []):
                o["tif"] = "IOC"
        if "size_factor" in caps and caps["size_factor"] is not None:
            self.current_size_factor = float(caps["size_factor"])
        if "bundle_concurrency_delta" in bundle and bundle["bundle_concurrency_delta"] is not None:
            self.adjust_bundle_concurrency(int(bundle["bundle_concurrency_delta"]))



    def _submit_or_raise(self, order: dict) -> dict:
        try:
            return self._exchange_submit(order)  # tua chiamata SDK/signed REST
        except (TimeoutError, ConnectionError) as e:
            raise ExternalServiceError(f"Submit timeout: {order.get('id')}") from e
        except Exception as e:
            # Niente pass o default: errore tipizzato
            raise EngineSubmitError(f"Submit failed: {order.get('id')}") from e

    def _cancel_or_raise(self, order_id: str) -> None:
        try:
            self._exchange_cancel(order_id)
        except (TimeoutError, ConnectionError) as e:
            raise ExternalServiceError(f"Cancel timeout: {order_id}") from e
        except Exception as e:
            raise EngineCancelError(f"Cancel failed: {order_id}") from e

    # Compatibilité: certaines parties appellent *_usd → alias vers la source unique
    def _estimate_ahead_usd(self, exchange: str, symbol: str, price: float, side: str) -> float:
        """
        Estimation simplifiée du notional (USD/quote) devant nous dans la file du carnet.
        - SELL (on place à l'ASK)  : "devant" = asks strictement < notre prix
        - BUY  (on place au BID)   : "devant" = bids strictement > notre prix
        Fallbacks robustes si OB indispo.
        """
        try:
            ex = (exchange or "").upper()
            pk = self._fmt_pair(symbol)
            ob = self._ob_latest(ex, pk) or {}
            levels = ob.get("asks" if str(side).upper() == "SELL" else "bids") or []
            ahead_qty = 0.0
            s_up = str(side).upper()
            if s_up == "SELL":
                for p, q in levels:
                    if float(p) < float(price):
                        ahead_qty += float(q)
                    else:
                        break
            else:  # BUY
                for p, q in levels:
                    if float(p) > float(price):
                        ahead_qty += float(q)
                    else:
                        break

            best = float(ob.get("best_ask") if s_up == "SELL" else ob.get("best_bid") or 0.0)
            if best <= 0:
                # Fallback: top-of-book via RM si dispo
                get_top = getattr(self.risk_manager, "get_top_of_book", None)
                if callable(get_top):
                    bid, ask = get_top(ex, pk) or (0.0, 0.0)
                    best = (ask if s_up == "SELL" else bid)
            return float(ahead_qty) * max(1e-12, best)
        except Exception:
            return 0.0

    # ======= Hedging progressif (planning en ratios cumulés) =======
    async def _mm_progressive_hedge(self, *, pair_key: str, exchange: str, side: str,
                                    notional_quote: float, schedule: list[tuple[float, float]],
                                    deadline: float, bundle_id: str) -> float:
        """
        Exécute plusieurs IOC (cumul < 1.0) répartis jusqu’au TTL.
        - schedule: [(t_frac in 0..1, target_ratio_cumulatif), ...]
        Retourne le montant USDC total hedgé.
        """
        hedged = 0.0
        start = time.monotonic()
        total = float(max(0.0, notional_quote))
        for t_frac, target_ratio in (schedule or []):
            t_frac = float(min(max(0.0, t_frac), 1.0))
            t_point = start + (deadline - start) * t_frac
            delay = max(0.0, t_point - time.monotonic())
            if delay: await asyncio.sleep(delay)
            target_usdc = total * float(min(max(0.0, target_ratio), 1.0))
            add = max(0.0, target_usdc - hedged)
            if add <= 0: continue
            await self._mm_panic_hedge_ioc(pair_key=pair_key, exchange=exchange, side=side,
                                           amount_quote=add, bundle_id=bundle_id)
            hedged += add
        return hedged

    # ======================= MM helpers (makers + hedge) =======================
    def _mm_new_client_id(self, prefix: str = "MM") -> str:
        return f"{prefix}{int(time.time() * 1000)}{uuid.uuid4().hex[:6]}"

    def _mm_quote(self, pair_key: str) -> str:
        _, q = SymbolUtils.split_base_quote(pair_key)
        return q

    def _mm_is_filled(self, client_id: str) -> bool:
        fsm = self._order_fsm.get(client_id)
        return bool(fsm and fsm.state == "FILLED")

    async def _mm_place_maker(self, *, pair_key: str, exchange: str, side: str,
                              amount_quote: float, bundle_id: str) -> str:
        """
        Soumet un maker GTC/PostOnly via le chemin 'single' existant.
        Retourne le client_id (FSM/WS feront la suite).
        """
        ex = self._norm_ex(exchange)
        sym = pair_key.replace("-", "").upper()
        best, ts = self._best_price_from_rm(ex, sym, side)
        # fallback minimal si pas d'ancre
        price = float(best or 0.0)
        if price <= 0.0:
            bid, ask = getattr(self.risk_manager, "get_top_of_book", lambda *a: (0.0, 0.0))(ex, sym) or (0.0, 0.0)
            price = ask if side.upper() == "SELL" else bid

        clid = self._mm_new_client_id()
        quote = self._mm_quote(sym)
        order = {
            "type": "single",
            "exchange": ex,
            "symbol": sym,
            "side": side.upper(),
            "price": float(price),
            "volume_usdc": float(amount_quote),  # NB: utilisé comme "notional quote" (USDC/EUR géré plus bas)
            "client_id": clid,
            "meta": {
                "best_price": float(price),
                "best_ts": int(ts or time.time() * 1000),
                "tif_override": "GTC",
                "maker": True,  # → LIMIT_MAKER / PostOnly
                "strategy": "TM",  # routing wallet_router by quote (MM ~ TM côté alias)
                "bundle_id": bundle_id,
                "fastpath_ok": True,
                "skip_inventory": False,  # MM réserve déjà en amont côté RM
                "account_alias": self.wallet_router.pick_by_quote(ex, "TM", quote),
            },
        }
        await self._exec_single(order)  # passe par la file + guards existants
        return clid

    async def _mm_panic_hedge_ioc(self, *, pair_key: str, exchange: str, side: str,
                                  amount_quote: float, bundle_id: str) -> None:
        """
        Hedge 'taker' en IOC côté manquant (pour neutraliser l'exposition).
        """
        ex = self._norm_ex(exchange)
        sym = pair_key.replace("-", "").upper()
        bid, ask = getattr(self.risk_manager, "get_top_of_book", lambda *a: (0.0, 0.0))(ex, sym) or (0.0, 0.0)
        px = float(ask if side.upper() == "BUY" else bid)
        if px <= 0:
            return
        quote = self._mm_quote(sym)
        order = {
            "type": "single",
            "exchange": ex,
            "symbol": sym,
            "side": side.upper(),
            "price": px,
            "volume_usdc": float(amount_quote),
            "meta": {
                "best_price": px,
                "best_ts": int(time.time() * 1000),
                "tif_override": "IOC",
                "fastpath_ok": True,
                "skip_inventory": True,
                "strategy": "TT",
                "bundle_id": bundle_id,
                "account_alias": self.wallet_router.pick_by_quote(ex, "TT", quote),
            },
        }
        await self._exec_single(order)

    async def _mm_cancel_open_makers(self, client_ids: list[str]) -> None:
        """
        Annule proprement les makers restants (NEW/ACK/PARTIAL) par client_id.
        """
        for cid in client_ids:
            fsm = self._order_fsm.get(cid)
            if not fsm or fsm.state in {"FILLED", "REJECTED", "CANCELED"}:
                continue
            ex, sym = self._client_symbol_map.get(cid, (None, None))
            if ex and sym:
                try:
                    await self._cancel_order(ex, sym, cid)
                except Exception:
                  report_nonfatal("ExecutionEngine", "cancel_open_maker_failed", None,
                                                         phase="_mm_cancel_open_makers")
                  logging.exception("Unhandled exception")

    # ========================================================================

    # dans ExecutionEngine.__init__ (ou en haut de part2)
    def _is_dry(self, exchange: str) -> bool:
        per = getattr(self.config, "dry_run_map", {}) or {}
        return bool(getattr(self.config, "dry_run", True)) or bool(per.get(self._ex_upper(exchange), False))

    # --------- fragments helpers (simu & front-loaded) ----------
    def _extract_sim_fragments(self, payload: Dict[str, Any]) -> Tuple[Optional[int], Optional[float]]:
        fr = (((payload or {}).get("meta") or {}).get("simulation") or {}).get("fragments") or {}
        try:
            cnt = int(fr.get("count")) if fr.get("count") is not None else None
        except Exception:
            cnt = None
        try:
            avg = float(fr.get("avg_fragment_usdc")) if fr.get("avg_fragment_usdc") is not None else None
        except Exception:
            avg = None
        return cnt, avg

    def _build_frontloaded_slices(
        self, total_usdc: float, desired_count: Optional[int], avg_fragment_usdc: Optional[float]
    ) -> List[Tuple[float, str, int, float]]:
        total = max(0.0, float(total_usdc))
        if total <= 0:
            return []
        if desired_count and desired_count > 0:
            N = int(desired_count)
        elif avg_fragment_usdc and avg_fragment_usdc > 0:
            N = max(1, int(round(total / float(avg_fragment_usdc))))
        else:
            N = max(1, int(round(total / max(self.min_fragment_usdc, 1.0))))
        N = max(1, min(64, N))
        group_size = max(1, int(self.frontload_group_size))
        g1 = min(group_size, N)
        rem_after_g1 = N - g1
        g2 = min(group_size, rem_after_g1) if rem_after_g1 > 0 else 0
        rem_after_g2 = rem_after_g1 - g2
        g3 = max(0, rem_after_g2)
        w = list(self.frontload_weights or [0.5, 0.35, 0.15])
        if len(w) < 3:
            w = (w + [0.0, 0.0, 0.0])[:3]
        active = []
        if g1 > 0:
            active.append(("G1", g1, w[0]))
        if g2 > 0:
            active.append(("G2", g2, w[1]))
        if g3 > 0:
            active.append(("G3", g3, w[2]))
        s = sum(x[2] for x in active) or 1.0
        active = [(label, size, weight / s) for (label, size, weight) in active]
        slices: List[Tuple[float, str, int, float]] = []
        allocated = 0.0
        for label, size, w_cohort in active:
            budget = total * float(w_cohort)
            if label == active[-1][0]:
                budget = max(0.0, total - allocated)
            if size <= 0 or budget <= 0:
                continue
            per_slice = budget / size
            if per_slice < self.min_fragment_usdc and size > 1:
                size = max(1, int(min(size, ceil(budget / self.min_fragment_usdc))))
                per_slice = budget / size
            for i in range(size):
                amt = per_slice
                slices.append((amt, label, i, amt / total))
                allocated += amt
        diff = total - sum(a for a, _, _, _ in slices)
        if abs(diff) >= 1e-6 and slices:
            last = slices[-1]
            slices[-1] = (
                max(0.0, last[0] + diff),
                last[1],
                last[2],
                max(0.0, (last[0] + diff) / total),
            )
        if len(slices) >= 2 and slices[-1][0] < (self.min_fragment_usdc * 0.5):
            new_last_amt = slices[-1][0] + slices[-2][0]
            new_w = new_last_amt / total
            label = slices[-2][1]
            idx = slices[-2][2]
            slices[-2] = (new_last_amt, label, idx, new_w)
            slices.pop()
        return slices

    async def _cancel_bg_tasks(self, *, timeout: float = 2.0) -> None:
        tasks = [t for t in getattr(self, "_bg_tasks", set()) if not t.done()]
        if not tasks:
            # Rien en vol : purge l’ensemble si présent
            if hasattr(self, "_bg_tasks"):
                self._bg_tasks.clear()
            return

        # 1) Première vague de cancel
        for t in tasks:
            t.cancel()

        # 2) Attente bornée
        done, pending = await asyncio.wait(tasks, timeout=timeout)

        # 3) Log et vidage des exceptions des tâches terminées
        for t in done:
            try:
                _ = t.result()
            except asyncio.CancelledError:
                pass  # normal
            except Exception:
                name = getattr(t, "get_name", lambda: None)() or repr(t)
                logger.debug("BG task %s raised during cancel", name, exc_info=True)

        # 4) Si certaines traînent encore, on insiste et on attend une dernière fois
        if pending:
            for t in pending:
                t.cancel()  # force encore une fois
            with contextlib.suppress(Exception):
                await asyncio.gather(*pending, return_exceptions=True)

        # 5) Nettoyage de l’ensemble
        if hasattr(self, "_bg_tasks"):
            self._bg_tasks.difference_update(done | pending)
            self._bg_tasks.clear()
    # --------------------------- single order -------------------------

    async def _watchdog_order(self, exchange: str, symbol: str, client_id: str):
        try:
            ttl = self.order_timeout_s
            start = time.time()
            while (time.time() - start) < (ttl + 0.25):
                fsm = self._order_fsm.get(client_id)
                if not fsm:
                    return
                if fsm.state in {"ACK", "PARTIAL", "FILLED", "REJECTED", "CANCELED"}:
                    return
                await asyncio.sleep(0.05)
            fsm = self._order_fsm.get(client_id)
            if fsm and fsm.expired_wo_ack():
                logger.warning(f"[Watchdog] ACK non reçu pour {client_id} ({exchange}/{symbol}).")
                inc_ack_timeout()
                # PACER: signal d'erreur/timeout → backpressure "errors"
                try:
                    self._pacer_update(err_rate=1.0, backpressure=True, reason="errors")
                except Exception:
                    pass

                try:
                    await self._cancel_order(exchange, symbol, client_id, reason="ack_timeout")
                    # HISTO ICI ✅
                    await self._hist("order_watchdog", {
                        "event": "ack_timeout_cancelled",
                        "exchange": exchange,
                        "symbol": symbol,
                        "client_id": client_id,
                        "timeout_s": self.order_timeout_s,
                        "ts": time.time(),
                    })
                except Exception:
                    report_nonfatal("ExecutionEngine", "cancel_on_ack_timeout_failed", None, phase="watchdog")
                    logging.exception("Unhandled exception")
        except Exception:
            report_nonfatal("ExecutionEngine", "watchdog_error", None, phase="watchdog")
            logger.debug("[Watchdog] erreur", exc_info=False)

    # -------------------- Helpers STP & Circuits --------------------


    def _depth(self, exchange: str, symbol: str) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
        """
        Récupère (asks, bids) normalisés depuis le dernier orderbook connu.
        Remplace par ton accès interne si besoin.
        """
        try:
            ob = (self._last_books or {}).get(exchange, {}).get(symbol.replace("_", "-").upper()) or {}
            asks = [(float(p), float(q)) for p, q in (ob.get("asks") or [])][:128]
            bids = [(float(p), float(q)) for p, q in (ob.get("bids") or [])][:128]
            return asks, bids
        except Exception:
            return [], []

    def _depth_quote_available(self, exchange: str, symbol: str, side: str, max_levels: int = 3) -> float:
        """
        Somme de profondeur convertie en QUOTE sur les 'max_levels' premiers niveaux.
        """
        asks, bids = self._depth(exchange, symbol)
        lvls = asks if side.upper() == "BUY" else bids
        total_quote = 0.0
        scanned = 0
        for p, q in lvls:
            total_quote += float(p) * float(q)
            scanned += 1
            if scanned >= int(max_levels):
                break
        return float(total_quote)

    def _volatility_bps(self, pair_key: str) -> float:
        try:
            fn = getattr(self.risk_manager, "get_volatility_bps", None)
            if callable(fn):
                return float(fn(pair_key))
        except Exception:
            pass
        return 0.0



    async def _sfc_fee_sync_loop(self, interval_s: float = None) -> None:
        """
        Boucle de refresh SFC (pilotée par RM/Boot). A appeler via create_task() au boot.
        """
        import asyncio, logging
        log = getattr(self, "logger", None) or logging.getLogger("risk_manager")
        itv = float(interval_s or getattr(self.cfg, "sfc_refresh_interval_s", 60.0))
        while True:
            try:
                if self.slip_collector and hasattr(self.slip_collector, "fee_sync_refresh_once"):
                    await self.slip_collector.fee_sync_refresh_once()
                    # (optionnel) publier age_s des snapshots ici si tu le souhaites
            except Exception:
                log.debug("[RiskManager] sfc fee_sync_refresh_once failed", exc_info=False)
            await asyncio.sleep(itv)

    async def _stp_pad_makers_before_taker(self, pair_key: str, buy_leg: dict, sell_leg: dict) -> None:
        """
        Anti self-match: avant tout taker sur (ex,symbol), on 'libère' les makers opposés.
        Implémentation simple: cancel immédiat des makers opposés sur la même venue/symbole.
        """
        if not bool(self.stp_pre_taker_enabled):
            return

        async def _cancel_opposite_makers(exchange: str, symbol: str, side_taker: str):
            # side maker opposé = côté où on risquerait l'auto-match
            opposite_side = "SELL" if side_taker.upper() == "BUY" else "BUY"
            # Parcours de l’univers d’ordres vivants (FSM) et cancel ciblé
            to_cancel = []
            for cid, fsm in list(self._order_fsm.items()):
                try:
                    meta = getattr(fsm, "meta", {}) if hasattr(fsm, "meta") else {}
                    ex = str(meta.get("exchange") or meta.get("ex") or "").upper()
                    sym = str(meta.get("symbol") or "").upper()
                    maker = bool(meta.get("maker", False))
                    side = str(meta.get("side") or "").upper()
                    if ex == exchange and sym == symbol and maker and side == opposite_side:
                        to_cancel.append(cid)
                except Exception:
                    continue
            for cid in to_cancel:
                try:
                    await self._cancel_order(exchange, symbol, cid, reason="stp_pre_taker")
                except Exception:
                    logging.exception("STP cancel failed")

        # buy_leg taker ?
        if not bool(buy_leg.get("meta", {}).get("maker", False)):
            await _cancel_opposite_makers(str(buy_leg["exchange"]).upper(), str(buy_leg["symbol"]).upper(),
                                          side_taker="BUY")

        # sell_leg taker ?
        if not bool(sell_leg.get("meta", {}).get("maker", False)):
            await _cancel_opposite_makers(str(sell_leg["exchange"]).upper(), str(sell_leg["symbol"]).upper(),
                                          side_taker="SELL")

    # -------------------- Helpers (Roadmap) --------------------

    def _now(self) -> float:
        try:
            return time.time()
        except Exception:
            return float(datetime.utcnow().timestamp())

    def _volatility_bps(self, pair_key: str) -> float:
        try:
            fn = getattr(self.risk_manager, "get_volatility_bps", None)
            if callable(fn):
                return float(fn(pair_key))
        except Exception:
            pass
        return 0.0

    def _depth(self, exchange: str, symbol: str):
        """
        Retourne (asks, bids) list[(px, qty)] depuis le dernier orderbook connu.
        Doit rester léger: fallback [] en cas d'absence.
        """
        try:
            ex = str(exchange).upper()
            sym = str(symbol).upper().replace("-", "")
            ob = (self._last_books or {}).get(ex, {}).get(sym) or {}
            asks = [(float(p), float(q)) for p, q in (ob.get("asks") or [])][:64]
            bids = [(float(p), float(q)) for p, q in (ob.get("bids") or [])][:64]
            return asks, bids
        except Exception:
            return [], []

    def _depth_quote_available(self, exchange: str, symbol: str, side: str, max_levels: int = 3) -> float:
        asks, bids = self._depth(exchange, symbol)
        lvls = asks if str(side).upper() == "BUY" else bids
        total_quote = 0.0
        for i, (p, q) in enumerate(lvls):
            total_quote += float(p) * float(q)
            if i + 1 >= int(max_levels):
                break
        return float(total_quote)

    def _is_muted(self, branch: str, pair_key: str) -> bool:
        """Retourne True si la branche (TM/MM) est temporairement mutée pour pair_key."""
        try:
            until = float((self._mute_until.get(branch) or {}).get(pair_key, 0.0))
            return self._now() < until
        except Exception:
            return False

    def _mute_branch(self, branch: str, pair_key: str, seconds: float, *, reason: str = "circuit") -> None:
        """Active un mute (TTL) pour une branche ('TM' ou 'MM') sur la paire, avec escalade exponentielle.
        - Compatible avec les anciens appels sans 'reason'
        - Borne le TTL min/max via la config
        - Incrémente une métrique (si dispo)
        """
        import time

        # --- helpers conf (sans nombres en dur) ---
        cfg = getattr(self, "config", None)

        def _cfg(name, default):
            if cfg is None: return default
            if hasattr(cfg, name): return getattr(cfg, name)
            if isinstance(cfg, dict): return cfg.get(name, default)
            return default

        # --- temps courant (tolérant si self._now absent) ---
        _now_fn = getattr(self, "_now", None)
        now = _now_fn() if callable(_now_fn) else time.time()

        br = str(branch or "").upper()
        pk = str(pair_key or "").upper()
        base = float(max(0.0, seconds))

        # --- états internes ---
        if not hasattr(self, "_mute_until"):
            self._mute_until = {"TM": {}, "MM": {}}
        if not hasattr(self, "_mute_counts"):
            self._mute_counts = {"TM": {}, "MM": {}}
        if not hasattr(self, "_mute_last"):
            self._mute_last = {"TM": {}, "MM": {}}

        last = float((self._mute_last.get(br) or {}).get(pk, 0.0))
        count = int((self._mute_counts.get(br) or {}).get(pk, 0))
        window_s = float(_cfg("circuit_escalation_window_s", 60.0))

        # escalade si répétition dans la fenêtre
        if (now - last) <= window_s:
            count += 1
        else:
            count = 1
        self._mute_counts.setdefault(br, {})[pk] = count
        self._mute_last.setdefault(br, {})[pk] = now

        factor = float(_cfg("circuit_mute_escalation", 1.7)) ** max(0, count - 1)
        ttl = base * factor
        ttl = max(ttl, float(_cfg("circuit_mute_min_s", 10.0)))
        ttl = min(ttl, float(_cfg("circuit_mute_max_s", 900.0)))

        # applique le mute
        self._mute_until.setdefault(br, {})[pk] = now + ttl

        # --- observabilité (optionnelle) ---
        try:
            from obs_metrics import ENGINE_MUTE_TOTAL  # gauge/counter si dispo
            ENGINE_MUTE_TOTAL.labels(br, pk, reason).inc()
        except Exception:
            pass
        try:
            if getattr(self, "log", None):
                self.log.info("engine._mute_branch",
                              extra={"branch": br, "pair": pk, "ttl_s": ttl, "count": count, "reason": reason})
        except Exception:
            pass

    def _pre_trade_circuits(self, pair_key: str, buy_leg: dict, sell_leg: dict) -> bool:
        """
        Gate avant submit:
          - Mutes actifs (TM/MM) par paire
          - Volatilité (soft/hard) avec freeze/mute branche
          - Profondeur minimale en QUOTE via _depth_quote_available(...), makers-only
        True => continuer, False => drop (le call-site journalise).
        """
        # --- helpers conf (zéro dur) ---
        cfg = getattr(self, "config", None)

        def _cfg(name, default):
            if cfg is None: return default
            if hasattr(cfg, name): return getattr(cfg, name)
            if isinstance(cfg, dict): return cfg.get(name, default)
            return default

        try:
            # --- détecter makers/takers par jambe ---
            meta_a = buy_leg.get("meta") or {}
            meta_b = sell_leg.get("meta") or {}
            is_maker_a = bool(meta_a.get("maker", False))
            is_maker_b = bool(meta_b.get("maker", False))
            # branche: MM (= deux makers), TM (= un seul maker)
            is_mm = is_maker_a and is_maker_b
            is_tm = (is_maker_a ^ is_maker_b)

            # --- mutes actifs (TM/MM) ---
            if is_tm and self._is_muted("TM", pair_key):
                return False
            if is_mm and self._is_muted("MM", pair_key):
                return False

            # --- volatilité (bps) ---
            vol_bps = float(self._volatility_bps(pair_key))
            vol_soft = float(_cfg("vol_soft_cap_bps", 45.0))
            vol_hard = float(_cfg("vol_hard_cap_bps", 80.0))
            freeze_tm = bool(_cfg("freeze_tm_on_vol", True))
            freeze_mm = bool(_cfg("freeze_mm_on_vol", True))

            if vol_bps >= vol_hard:
                # hard stop makers (refroidir)
                self._mute_branch("TM", pair_key, float(_cfg("circuit_mute_s_tm", 300.0)), reason="vol_hard")
                self._mute_branch("MM", pair_key, float(_cfg("circuit_mute_s_mm", 300.0)), reason="vol_hard")
                return False
            if vol_bps >= vol_soft:
                if is_tm and freeze_tm:
                    self._mute_branch("TM", pair_key, float(_cfg("circuit_mute_s_tm", 120.0)), reason="vol_soft")
                    return False
                if is_mm and freeze_mm:
                    self._mute_branch("MM", pair_key, float(_cfg("circuit_mute_s_mm", 180.0)), reason="vol_soft")
                    return False

            # --- shallow book (en QUOTE) — makers only ---
            if is_tm or is_mm:
                # paramètres
                max_lvls = int(_cfg("depth_check_max_levels", 3))
                min_q_tm = float(_cfg("depth_min_quote_tm", 500.0))
                min_q_mm = float(_cfg("depth_min_quote_mm", 1000.0))
                allow_shlw = bool(_cfg("allow_shallow_books", False))

                # buy_leg: on va "BUY" → on consomme les ASKS (profondeur quote côté ASK)
                ex_buy = buy_leg.get("exchange")
                sym_buy = buy_leg.get("symbol") or buy_leg.get("pair")
                q_buy = float(self._depth_quote_available(ex_buy, sym_buy, "BUY", max_levels=max_lvls))

                # sell_leg: on va "SELL" → on consomme les BIDS (profondeur quote côté BID)
                ex_sell = sell_leg.get("exchange")
                sym_sell = sell_leg.get("symbol") or sell_leg.get("pair")
                q_sell = float(self._depth_quote_available(ex_sell, sym_sell, "SELL", max_levels=max_lvls))

                min_needed = min_q_mm if is_mm else min_q_tm
                shallow = (q_buy < min_needed) or (q_sell < min_needed)

                if shallow and not allow_shlw:
                    # mute la branche concernée, pénalise la paire côté RM
                    if is_mm:
                        self._mute_branch("MM", pair_key, float(_cfg("circuit_mute_s_mm", 300.0)), reason="shallow")
                    else:
                        self._mute_branch("TM", pair_key, float(_cfg("circuit_mute_s_tm", 300.0)), reason="shallow")
                    try:
                        pen = getattr(self, "risk_manager", None)
                        if pen and hasattr(pen, "penalize_pair"):
                            pen.penalize_pair(pair_key, reason="shallow_book")
                    except Exception:
                        pass
                    return False

            # tout est vert
            return True

        except Exception:
            # En cas d’exception inattendue: prudence → ne pas exécuter
            try:
                if getattr(self, "log", None):
                    self.log.exception("_pre_trade_circuits failed")
            except Exception:
                pass
            return False

    async def _stp_pad_makers_before_taker(self, pair_key: str, buy_leg: dict, sell_leg: dict) -> None:
        """
        Anti self-match: on annule les makers opposés sur la même venue/symbole
        juste avant d'envoyer un TAKER.
        """
        if not bool(getattr(self.config, "stp_pre_taker_enabled", True)):
            return

        async def _cancel_opposite_makers(exchange: str, symbol: str, side_taker: str):
            opposite_side = "SELL" if side_taker.upper() == "BUY" else "BUY"
            to_cancel = []
            for cid, fsm in list(self._order_fsm.items()):
                try:
                    meta = getattr(fsm, "meta", {}) if hasattr(fsm, "meta") else {}
                    ex = str(meta.get("exchange") or meta.get("ex") or "").upper()
                    sym = str(meta.get("symbol") or "").upper().replace("-", "")
                    maker = bool(meta.get("maker", False))
                    side = str(meta.get("side") or "").upper()
                    if ex == str(exchange).upper() and sym == str(symbol).upper().replace("-",
                                                                                          "") and maker and side == opposite_side:
                        to_cancel.append(cid)
                except Exception:
                    continue
            for cid in to_cancel:
                try:
                    await self._cancel_order(str(exchange).upper(), str(symbol).upper(), cid, reason="stp_pre_taker")
                except Exception:
                    logging.exception("STP cancel failed")

        # buy_leg taker ?
        if not bool((buy_leg.get("meta") or {}).get("maker", False)):
            await _cancel_opposite_makers(buy_leg["exchange"], buy_leg["symbol"], side_taker="BUY")
        # sell_leg taker ?
        if not bool((sell_leg.get("meta") or {}).get("maker", False)):
            await _cancel_opposite_makers(sell_leg["exchange"], sell_leg["symbol"], side_taker="SELL")

    def _mark_pair_activity(self, pair_key: str, activity: str) -> None:
        """
        Note une activité TT/TM sur la paire => déclenche scaling MM temporaire.
        """
        try:
            if activity not in ("TT", "TM"):
                return
            until = self._now() + float(self.mm_hysteresis_ms) / 1000.0
            self._mm_scaled_until[pair_key] = until
            self._mm_scaled_pairs.add(pair_key)
        except Exception:
            logging.exception("Unhandled in _mark_pair_activity")

    async def _restore_mm_after_hysteresis(self, pair_key: str) -> None:
        """
        Restaure l'état normal MM après mm_hysteresis_ms sans nouvelle activité TT/TM.
        """
        try:
            await asyncio.sleep(max(0.0, float(self.mm_hysteresis_ms) / 1000.0))
            if self._now() >= float(self._mm_scaled_until.get(pair_key, 0.0)):
                self._mm_scaled_pairs.discard(pair_key)
        except Exception:
            logging.exception("Unhandled in _restore_mm_after_hysteresis")

    def _apply_mm_scaling_for_pair(self, pair_key: str, leg_meta: dict) -> dict:
        """
        Applique un pad/scale sur un maker si la paire est en phase 'scaled' (TT/TM actifs récemment).
        Retourne une copie ajustée du meta (pad ticks, size factor).
        """
        meta = dict(leg_meta or {})
        if pair_key in self._mm_scaled_pairs and bool(meta.get("maker", False)):
            try:
                # scale size (le sizing final reste contrôlé par normalisation & minNotional)
                meta["mm_size_scale"] = float(self.mm_scale_on_tt_tm)
                # pad de protection
                meta["pad_ticks"] = int(meta.get("pad_ticks", 0)) + int(self.mm_pad_boost_on_tt_tm)
            except Exception:
                logging.exception("Unhandled in _apply_mm_scaling_for_pair")
        return meta

    def _within_price_band(self, price: float, best_anchor: float, pair_key: str, vol_bps: float = None) -> bool:
        """
        Price-band dynamique en bps : band = clamp( floor, cap, k * vol_bps ).
        """
        try:
            if best_anchor <= 0:
                return True
            if vol_bps is None:
                vol_bps = self._volatility_bps(pair_key)
            band = max(self.price_band_bps_floor,
                       min(self.price_band_bps_cap, float(self.vol_price_band_k) * float(vol_bps)))
            diff_bps = abs((float(price) - float(best_anchor)) / float(best_anchor)) * 1e4  # 1bp = 0.01%
            return diff_bps <= band
        except Exception:
            return True

    def _apply_reality_gap_offset(self, pair_key: str, expected_net_bps: float = None,
                                  realized_net_bps: float = None) -> None:
        """
        Si l'écart (réalité - attendu) est défavorable et significatif, on ajoute un offset
        temporaire au min_required_bps (piloté par RM/Scanner), stocké localement.
        """
        try:
            if expected_net_bps is None or realized_net_bps is None:
                return
            gap = float(expected_net_bps) - float(realized_net_bps)  # positif => on était trop optimiste
            if gap <= 0:
                # on peut relâcher doucement l'offset
                cur = float(self._min_required_bps_offset.get(pair_key, 0.0))
                self._min_required_bps_offset[pair_key] = max(0.0, cur * 0.8)
                return
            # ajoute 50% de la dérive (bounded)
            cur = float(self._min_required_bps_offset.get(pair_key, 0.0))
            bump = min(20.0, max(0.0, 0.5 * gap))
            self._min_required_bps_offset[pair_key] = min(50.0, cur + bump)
        except Exception:
            logging.exception("Unhandled in _apply_reality_gap_offset")

    # --------------------------- bundle (TT/TM) ---------------------------
    async def _exec_bundle(self, payload: Dict[str, Any]):
        # READINESS GUARD
        self._ensure_ready()

        legs = payload.get("legs") or (payload.get("payload", {}) or {}).get("legs", [])
        if len(legs) != 2:
            self.stats.total_failed += 1
            return

        timeout_s = float(payload.get("timeout_s", self.order_timeout_s))
        try:
            pair_key = self._fmt_pair(payload.get("pair_key") or legs[0]["symbol"])
        except Exception:
            pair_key = str(payload.get("pair_key") or legs[0].get("symbol", "").replace("-", "").upper())
        expected = float(payload.get("expected_net_spread", 0.0) or 0.0)

        # Hydrate best/best_ts etc.
        self._hydrate_prices_for_legs(legs)

        # Identify BUY / SELL legs
        buy_leg = next((l for l in legs if str(l.get("side", "")).upper() == "BUY"), None)
        sell_leg = next((l for l in legs if str(l.get("side", "")).upper() == "SELL"), None)
        if not buy_leg or not sell_leg:
            self._reject(pair_key, payload, "bundle sans BUY/SELL")
            return

        # Alias par défaut si absents (router par quote & stratégie)
        for l in (buy_leg, sell_leg):
            m = l.setdefault("meta", {}) or {}
            if not m.get("account_alias"):
                is_maker = bool(m.get("maker", False))
                strat = "TM" if is_maker else "TT"
                _, q = SymbolUtils.split_base_quote(l["symbol"])
                m["account_alias"] = self.wallet_router.pick_by_quote(
                    self._norm_ex(l["exchange"]), strat, q
                )

        # Volumes par jambe
        vol_buy = float(buy_leg.get("volume_usdc", 0.0) or 0.0)
        vol_sell = float(sell_leg.get("volume_usdc", 0.0) or 0.0)

        # Régime
        try:
            regime = (
                str(self.risk_manager.get_regime(pair_key))
                .lower().replace("é", "e").replace("è", "e").replace("ê", "e")
            )
        except Exception:
            regime = "normal"

        # Fastpath ?
        try:
            fastpath_ok = bool(self.risk_manager.is_fastpath_ok(
                buy_leg["exchange"], sell_leg["exchange"], pair_key
            ))
        except Exception:
            fastpath_ok = False

        # Route autorisée (tri-CEX)
        if (self._norm_ex(buy_leg["exchange"]), self._norm_ex(sell_leg["exchange"])) not in self._allowed_routes:
            self._reject(pair_key, payload, "route CEX non autorisée (tri-CEX)")
            return

        # Rebal → TM forcé
        is_rebal = (
                payload.get("type") == "rebalancing"
                or (payload.get("meta") or {}).get("type") == "rebalancing"
                or any((l.get("meta") or {}).get("type") == "rebalancing" for l in legs)
        )
        strategy = str((payload.get("meta") or {}).get("strategy", "")).upper()
        if is_rebal:
            strategy = "TM"
        if strategy not in {"TM", "TT"}:
            strategy = "TT"
        if not self.enable_taker_maker and strategy == "TM":
            strategy = "TT"
        if not self.enable_taker_taker and strategy == "TT":
            strategy = "TM" if self.enable_taker_maker else "TT"

        # Inventaire bundle
        if not self.risk_manager.validate_inventory_bundle(
                buy_ex=buy_leg["exchange"],
                sell_ex=sell_leg["exchange"],
                pair_key=pair_key,
                vol_usdc_buy=vol_buy,
                vol_usdc_sell=vol_sell,
                is_rebalancing=is_rebal,
        ):
            self._reject(pair_key, payload, "inventory_cap/skew (bundle)")
            return

        # Perte finale (panic hedge)
        allow_loss_bps = max(
            float((payload.get("meta") or {}).get("allow_loss_bps", 0) or 0),
            float((buy_leg.get("meta") or {}).get("allow_loss_bps", 0) or 0),
            float((sell_leg.get("meta") or {}).get("allow_loss_bps", 0) or 0),
        )

        # Revalidation arbitrage
        try:
            ok_arbi = self.risk_manager.revalidate_arbitrage(
                buy_ex=buy_leg["exchange"],
                sell_ex=sell_leg["exchange"],
                pair_key=pair_key,
                expected_net=(expected if expected > 0 else None),
                max_drift_bps=7.0,
                min_required_bps=None,
                is_rebalancing=is_rebal,
                allow_final_loss_bps=allow_loss_bps,
            )
        except TypeError:
            ok_arbi = self.risk_manager.revalidate_arbitrage(
                buy_ex=buy_leg["exchange"],
                sell_ex=sell_leg["exchange"],
                pair_key=pair_key,
                expected_net=(expected if expected > 0 else None),
                max_drift_bps=7.0,
                min_required_bps=(0.0 if is_rebal else 20.0),
                is_rebalancing=is_rebal,
            )
        if not ok_arbi and (not is_rebal):
            self._reject(pair_key, payload, "revalidation arbitrage (dérive/net)")
            return

        # Guards jambe par jambe (ancre + RM)
        for leg in legs:
            ex, sym, price, vol = (
                leg["exchange"], leg["symbol"],
                float(leg["price"]), float(leg["volume_usdc"])
            )
            meta = leg.get("meta") or {}
            best = float(meta.get("best_price", price))
            best_ts_ms = self._to_ms(meta.get("best_ts"))
            if not self._within_anchor_guard(price, best, best_ts_ms):
                self._reject(sym, leg, "price_guard (bundle/anchor)")
                return

            pk_leg = sym.replace("-", "").upper()
            if fastpath_ok:
                if not self.risk_manager.validate_trade(ex, pk_leg, vol):
                    self._reject(sym, leg, "refusé (bundle validate fast)")
                    return
            else:
                if not self.risk_manager.validate_trade(ex, pk_leg, vol):
                    self._reject(sym, leg, "refusé (bundle validate)")
                    return
                if not self.risk_manager.revalidate_trade(ex, pk_leg, vol):
                    self._reject(sym, leg, "refusé (bundle revalidate)")
                    return

        # === CIRCUITS + STP + SCALING-HOOK (avant tout submit) ===
        if not self._pre_trade_circuits(pair_key, buy_leg, sell_leg):
            await self._hist("trade", {
                "status": "dropped",
                "reason": "circuit_breaker",
                "exchange_buy": str(buy_leg["exchange"]).upper(),
                "exchange_sell": str(sell_leg["exchange"]).upper(),
                "symbol": str(buy_leg["symbol"]).upper(),
                "pair_key": pair_key,
                "ts": time.time(),
            })
            return
        try:
            await self._stp_pad_makers_before_taker(pair_key, buy_leg, sell_leg)
        except Exception:
            logging.exception("STP pre-taker failed in bundle")

        is_maker_buy = bool((buy_leg.get("meta") or {}).get("maker", False))
        is_maker_sell = bool((sell_leg.get("meta") or {}).get("maker", False))
        if (not is_maker_buy) or (not is_maker_sell):
            self._mark_pair_activity(pair_key, "TT")
            try:
                self._spawn(self._restore_mm_after_hysteresis(pair_key), name=f"mm-rest-{pair_key[-6:]}")
            except Exception:
                pass
        if is_maker_buy:
            buy_leg["meta"] = self._apply_mm_scaling_for_pair(pair_key, buy_leg.get("meta") or {})
        if is_maker_sell:
            sell_leg["meta"] = self._apply_mm_scaling_for_pair(pair_key, sell_leg.get("meta") or {})
        # === /CIRCUITS + STP + SCALING-HOOK ===

        # Bundle ID + fragments (simu/stats)
        bundle_id = payload.get("bundle_id") or f"BND-{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}"
        frag_count, frag_avg = self._extract_sim_fragments(payload)

        # Pipelines
        if strategy == "TM":
            async with self._sem_tm_pairs:
                await self._run_pipeline_tm(
                    payload_meta=(payload.get("meta") or {}),
                    bundle_id=bundle_id,
                    pair_key=pair_key,
                    regime=regime,
                    buy_leg=buy_leg,
                    sell_leg=sell_leg,
                    expected=expected,
                    allow_loss_bps=allow_loss_bps,
                    timeout_s=timeout_s,
                    is_rebalancing=is_rebal,
                    sim_frag_count=frag_count,
                    sim_frag_avg_usdc=frag_avg,
                )
        else:
            async with self._sem_tt_pairs:
                await self._run_pipeline_tt(
                    bundle_id=bundle_id,
                    pair_key=pair_key,
                    regime=regime,
                    fastpath_ok=fastpath_ok,
                    buy_leg=buy_leg,
                    sell_leg=sell_leg,
                    expected=expected,
                    allow_loss_bps=allow_loss_bps,
                    timeout_s=timeout_s,
                    is_rebalancing=is_rebal,
                    sim_frag_count=frag_count,
                    sim_frag_avg_usdc=frag_avg,
                )

    # --------------------------- TT (taker/taker) ---------------------------
    async def _run_pipeline_tt(
            self,
            *,
            bundle_id: str,
            pair_key: str,
            regime: str,
            fastpath_ok: bool,
            buy_leg: Dict[str, Any],
            sell_leg: Dict[str, Any],
            expected: float,
            allow_loss_bps: float,
            timeout_s: float,
            is_rebalancing: bool,
            sim_frag_count: Optional[int],
            sim_frag_avg_usdc: Optional[float],
    ):
        # READINESS GUARD
        self._ensure_ready()

        total_usdc = min(float(buy_leg["volume_usdc"]), float(sell_leg["volume_usdc"]))
        if total_usdc <= 0:
            return

        # ===== Plan fragments (Simulation > RM > fallback front-load) =====
        slices: List[Tuple[float, str, int, float]] = []
        desired = sim_frag_count if self.frontload_enabled else None
        avg = sim_frag_avg_usdc if self.frontload_enabled else None

        if (desired and desired > 0) or (avg and avg > 0):
            slices = self._build_frontloaded_slices(total_usdc, desired, avg)
        else:
            plan = None
            try:
                plan = self.risk_manager.plan_fragments(
                    pair_key=pair_key,
                    buy_ex=buy_leg["exchange"],
                    sell_ex=sell_leg["exchange"],
                    total_usdc=total_usdc,
                    strategy="TT",
                    regime=regime,
                )
            except Exception:
                plan = None
            if plan and (plan.get("amounts") or []):
                amts = [float(a) for a in plan.get("amounts", []) if float(a) > 0]
                grps = list(plan.get("groups", []))
                for i, a in enumerate(amts):
                    g = grps[i] if i < len(grps) else "G1"
                    slices.append((a, g, i, a / total_usdc))
            else:
                slices = self._build_frontloaded_slices(total_usdc, None, None)

        if not slices:
            slices = [(total_usdc, "G1", 0, 1.0)]

        cohorts: Dict[str, List[Tuple[int, float, float]]] = {"G1": [], "G2": [], "G3": []}
        for global_idx, (amt, label, _idx_in_group, w) in enumerate(slices):
            cohorts.setdefault(str(label), []).append((global_idx, float(amt), float(w)))

        order = ["G1", "G2", "G3"]
        # PACER: plafond d'inflight recommandé (anti-collision)
        try:
            pol = getattr(self, "_pacer", None).current_policy if getattr(self, "_pacer", None) else {
                "inflight_max": self.max_inflight_slices}
            infl_cap = int(pol.get("inflight_max", self.max_inflight_slices))
        except Exception:
            infl_cap = self.max_inflight_slices
        inflight_sem = asyncio.Semaphore(max(1, min(self.max_inflight_slices, infl_cap)))

        lock = asyncio.Lock()
        consecutive_fails = 0

        # ====== helpers TT ======
        async def _edge_guard_ok(
                buy_ex: str,
                sell_ex: str,
                pair_key: str,
                *,
                expected_net_ratio: Optional[float],
                allow_loss_bps: float,
                is_rebal: bool,
        ) -> bool:
            try:
                return self.risk_manager.revalidate_arbitrage(
                    buy_ex=buy_ex,
                    sell_ex=sell_ex,
                    pair_key=pair_key,
                    expected_net=(expected_net_ratio if expected_net_ratio else None),
                    max_drift_bps=7.0,
                    min_required_bps=(0.0 if is_rebal else None),
                    is_rebalancing=is_rebal,
                    allow_final_loss_bps=allow_loss_bps,
                )
            except Exception:
                return False

        def _cid(*, kind: str, bundle_id: str, slice_id: str, leg: str) -> str:
            base = f"{bundle_id}:{slice_id}:{leg}:{kind}"
            return base.replace(":", "")[:32]

        async def _dual_submit(buy_order, sell_order, *, max_skew_ms: int) -> Tuple[bool, bool]:
            t_buy = asyncio.create_task(self._exec_single(buy_order))
            if max_skew_ms and max_skew_ms > 0:
                await asyncio.sleep(max(0.0, max_skew_ms / 1000.0))
            t_sell = asyncio.create_task(self._exec_single(sell_order))
            res = await asyncio.gather(t_buy, t_sell, return_exceptions=True)
            ok_b = (not isinstance(res[0], Exception)) and bool(res[0])
            ok_s = (not isinstance(res[1], Exception)) and bool(res[1])
            return ok_b, ok_s

        async def _panic_hedge(ex: str, symbol: str, side: str, needed_usdc: float, *, max_loss_bps: float):
            bid, ask = (
                getattr(self.risk_manager, "get_top_of_book", lambda *args: (0, 0))(
                    ex, symbol.replace("-", "").upper()
                )
            )
            px = ask if side.upper() == "BUY" else bid
            if px <= 0:
                return
            order = {
                "type": "single",
                "exchange": ex,
                "symbol": symbol,
                "side": side,
                "price": px,
                "volume_usdc": needed_usdc,
                "meta": {
                    "tif_override": "IOC",
                    "fastpath_ok": True,
                    "skip_inventory": True,
                    "strategy": "TT",
                },
            }
            await self._exec_single(order)

        async def _run_one_slice(
                glabel: str, global_idx: int, usdc_amt: float, weight: float, is_last_of_all: bool
        ) -> bool:
            ok_fast = await _edge_guard_ok(
                buy_leg["exchange"],
                sell_leg["exchange"],
                pair_key,
                expected_net_ratio=(expected if expected else None),
                allow_loss_bps=allow_loss_bps,
                is_rebal=is_rebalancing,
            )
            if not ok_fast:
                return False

            # Profitabilité slice TT (si API dispo)
            try:
                ok_prof, _ = await self.risk_manager.is_fragment_profitable(
                    pair_key=pair_key,
                    buy_ex=buy_leg["exchange"],
                    sell_ex=sell_leg["exchange"],
                    usdc_amt=float(usdc_amt),
                    strategy="TT",
                )
                if not ok_prof:
                    return False
            except Exception:
                logging.exception("Unhandled exception")

            px_buy = float(buy_leg["price"])
            px_sell = float(sell_leg["price"])
            best_buy = float(buy_leg.get("meta", {}).get("best_price", px_buy))
            best_sell = float(sell_leg.get("meta", {}).get("best_price", px_sell))
            best_buy_ts = self._to_ms(buy_leg.get("meta", {}).get("best_ts"))
            best_sell_ts = self._to_ms(sell_leg.get("meta", {}).get("best_ts"))
            if not self._within_anchor_guard(px_buy, best_buy, best_buy_ts):
                return False
            if not self._within_anchor_guard(px_sell, best_sell, best_sell_ts):
                return False

            slice_tag = f"{bundle_id}-TT-{glabel}-{global_idx}"
            _, q_buy = SymbolUtils.split_base_quote(buy_leg["symbol"])
            _, q_sell = SymbolUtils.split_base_quote(sell_leg["symbol"])

            try:
                flags = getattr(self, "_pacer", None).current_policy.get("flags", {}) if getattr(self, "_pacer",
                                                                                                 None) else {}
            except Exception:
                flags = {}

            meta_common = {
                "best_ts": int(time.time() * 1000),
                "tif_override": ("IOC" if flags.get("ioc_only", False) else (
                    "IOC" if (not is_last_of_all or regime != "eleve") else "FOK")),
                "fastpath_ok": fastpath_ok,
                "skip_inventory": True,
                "bundle_id": bundle_id,
                "slice_id": slice_tag,
                "strategy": "TT",
                "slice_group": glabel,
                "slice_index": global_idx,
                "slice_weight": float(weight),
                "planned_usdc": float(usdc_amt),
            }

            # IDs déterministes
            buy_cid = _cid(kind="LMT", bundle_id=bundle_id, slice_id=slice_tag, leg="BUY")
            sell_cid = _cid(kind="LMT", bundle_id=bundle_id, slice_id=slice_tag, leg="SELL")

            order_buy = {
                "type": "single",
                "exchange": buy_leg["exchange"],
                "symbol": buy_leg["symbol"],
                "side": "BUY",
                "price": px_buy,
                "volume_usdc": usdc_amt,
                "client_id": buy_cid,
                "meta": {
                    **meta_common,
                    "best_price": best_buy,
                    "account_alias": self.wallet_router.pick_by_quote(
                        self._norm_ex(buy_leg["exchange"]), "TT", q_buy
                    ),
                },
            }
            order_sell = {
                "type": "single",
                "exchange": sell_leg["exchange"],
                "symbol": sell_leg["symbol"],
                "side": "SELL",
                "price": px_sell,
                "volume_usdc": usdc_amt,
                "client_id": sell_cid,
                "meta": {
                    **meta_common,
                    "best_price": best_sell,
                    "account_alias": self.wallet_router.pick_by_quote(
                        self._norm_ex(sell_leg["exchange"]), "TT", q_sell
                    ),
                },
            }
            ok_guard, why_guard = self._pre_submit_guards({
                "legs": [order_buy, order_sell],
                "expected_net_spread": (expected or 0.0),
                "meta": {"strategy": "TT", "allow_loss_bps": allow_loss_bps}
            })
            if not ok_guard:
                return False

            ok_b, ok_s = await _dual_submit(order_buy, order_sell, max_skew_ms=self.tt_max_skew_ms)
            if ok_b and ok_s and self.history_sink:
                self.history_sink({
                    "pair": pair_key,
                    "timestamp": time.time(),
                    "executed_volume_usdc": float(usdc_amt),
                    "status": "executed",
                    "trade_mode": "TT",
                    "buy_ex": buy_leg["exchange"],
                    "sell_ex": sell_leg["exchange"],
                    "route": f"BUY:{buy_leg['exchange']}→SELL:{sell_leg['exchange']}",
                    "trade_id": slice_tag,
                })

            # Panic-hedge si une jambe échoue
            if (not ok_b) and ok_s:
                await _panic_hedge(
                    sell_leg["exchange"], sell_leg["symbol"], "BUY", usdc_amt, max_loss_bps=allow_loss_bps
                )
            elif ok_b and (not ok_s):
                await _panic_hedge(
                    buy_leg["exchange"], buy_leg["symbol"], "SELL", usdc_amt, max_loss_bps=allow_loss_bps
                )

            await asyncio.sleep(self._pacing_sleep_s(regime))
            return (ok_b and ok_s)

        async def _run_group(label: str, items: List[Tuple[int, float, float]], is_last_group: bool):
            nonlocal consecutive_fails

            async def _guarded(idx: int, amt: float, w: float, is_last_slice: bool):
                nonlocal consecutive_fails
                async with inflight_sem:
                    ok = await _run_one_slice(label, idx, amt, w, is_last_slice and is_last_group)
                    async with lock:
                        if ok:
                            consecutive_fails = 0
                        else:
                            consecutive_fails += 1

            tasks = [
                asyncio.create_task(
                    _guarded(gidx, amt, w, is_last_slice=(i == len(items) - 1))
                )
                for i, (gidx, amt, w) in enumerate(items)
            ]
            if tasks:
                await asyncio.gather(*tasks)

        async def _run_groups():
            for i, lab in enumerate(order):
                items = cohorts.get(lab) or []
                if not items:
                    continue
                ok_grp = await _edge_guard_ok(
                    buy_leg["exchange"],
                    sell_leg["exchange"],
                    pair_key,
                    expected_net_ratio=(expected if expected else None),
                    allow_loss_bps=allow_loss_bps,
                    is_rebal=is_rebalancing,
                )
                if not ok_grp and not is_rebalancing:
                    break
                await _run_group(lab, items, is_last_group=(i == len(order) - 1))
                if consecutive_fails >= 2:
                    break

        try:
            await asyncio.wait_for(_run_groups(), timeout=timeout_s)
        except asyncio.TimeoutError:
            logger.error("⏱️ Pipeline TT timeout.")

    # ---------------------------------------------------------------------------
    # IMPORTANT: we assume a base class `ExecutionEngine` exists earlier in your codebase.
    # Do NOT redefine it here. All bindings below will attach onto that class.
    # ---------------------------------------------------------------------------

    async def _run_pipeline_tm(
            self: "ExecutionEngine",
            *,
            payload_meta: Dict[str, Any],
            bundle_id: str,
            pair_key: str,
            regime: str,
            buy_leg: Dict[str, Any],
            sell_leg: Dict[str, Any],
            expected: float,
            allow_loss_bps: float,
            timeout_s: float,
            is_rebalancing: bool,
            sim_frag_count: Optional[int],
            sim_frag_avg_usdc: Optional[float],
    ):
        # READINESS GUARD
        self._ensure_ready()

        total_usdc = min(float(buy_leg["volume_usdc"]), float(sell_leg["volume_usdc"]))
        # PACER: mode dégradé "gel MM" → on sort immédiatement
        try:
            flags = getattr(self, "_pacer", None).current_policy.get("flags", {}) if getattr(self, "_pacer",
                                                                                             None) else {}
            if flags.get("mm_frozen", False):
                await self._hist("trade", {"_hist_kind": "TM", "status": "mm_frozen", "pair": pair_key,
                                           "timestamp": time.time()})
                return
        except Exception:
            pass

        if total_usdc <= 0:
            return

        maker_leg_side = str(
            (buy_leg.get("meta", {}).get("maker_leg") or sell_leg.get("meta", {}).get("maker_leg") or "").upper()
        )
        if maker_leg_side not in {"BUY", "SELL"}:
            maker_leg_side = "SELL"
        maker_leg = sell_leg if maker_leg_side == "SELL" else buy_leg
        taker_leg = buy_leg if maker_leg_side == "SELL" else sell_leg
        hedge_side = "BUY" if maker_leg_side == "SELL" else "SELL"

        tm_dec = (payload_meta.get("tm") or {})
        can_nn = bool(getattr(self.risk_manager, "allow_tm_non_neutral", False))
        mode = str(tm_dec.get("mode") or getattr(self.config, "tm_default_mode", "NEUTRAL")).upper()
        if mode in {"NON_NEUTRAL"}:
            mode = "NN"
        if mode == "NN" and not can_nn:
            mode = "NEUTRAL"

        default_nn = float(getattr(self.config, "tm_nn_hedge_ratio", 0.60))
        neutral_hr = float(getattr(self.config, "tm_neutral_hedge_ratio", 1.0))
        hedge_ratio = float(tm_dec.get("hedge_ratio", (neutral_hr if mode == "NEUTRAL" else default_nn)))
        if is_rebalancing:
            mode, hedge_ratio = "NEUTRAL", 1.0

        # ===== Plan fragments (Simulation > RM > fallback) =====
        slices: List[Tuple[float, str, int, float]] = []
        desired = None
        if self.frontload_enabled:
            if sim_frag_count and sim_frag_count > 0:
                desired = min(max(1, sim_frag_count), self.tm_max_open_makers)
        avg = sim_frag_avg_usdc if self.frontload_enabled else None

        if desired or (avg and avg > 0):
            slices = self._build_frontloaded_slices(total_usdc, desired, avg)
        else:
            plan = None
            try:
                plan = self.risk_manager.plan_fragments(
                    pair_key=pair_key,
                    buy_ex=buy_leg["exchange"],
                    sell_ex=sell_leg["exchange"],
                    total_usdc=total_usdc,
                    strategy="TM",
                    regime=regime,
                )
            except Exception:
                plan = None
            if plan and (plan.get("amounts") or []):
                amts = [float(a) for a in plan.get("amounts", []) if float(a) > 0]
                grps = list(plan.get("groups", []))
                for i, a in enumerate(amts[: self.tm_max_open_makers]):
                    g = grps[i] if i < len(grps) else "G1"
                    slices.append((a, g, i, a / total_usdc))
            else:
                if self.frontload_enabled:
                    slices = self._build_frontloaded_slices(total_usdc, self.tm_max_open_makers, None)
                else:
                    weights = self.tm_weights if (self.tm_weights and sum(self.tm_weights) > 0) else [1.0]
                    weights = weights[: self.tm_max_open_makers]
                    s = sum(weights) or 1.0
                    weights = [w / s for w in weights]
                    slices = [(total_usdc * w, "G1", i, w) for i, w in enumerate(weights)]

        if not slices:
            slices = [(total_usdc, "G1", 0, 1.0)]

        # TOB maker
        get_tob = getattr(self.risk_manager, "get_top_of_book", None)
        bid_m, ask_m = 0.0, 0.0
        if callable(get_tob):
            bid_m, ask_m = get_tob(maker_leg["exchange"], pair_key) or (0.0, 0.0)
        if bid_m <= 0 or ask_m <= 0:
            bid_m = float(maker_leg.get("meta", {}).get("best_bid", 0.0) or 0.0)
            ask_m = float(maker_leg.get("meta", {}).get("best_ask", 0.0) or 0.0)
            if bid_m <= 0 or ask_m <= 0:
                if maker_leg_side == "SELL":
                    ask_m = float(maker_leg.get("price", 0.0) or 0.0)
                    bid_m = max(0.0, ask_m * 0.999)
                else:
                    bid_m = float(maker_leg.get("price", 0.0) or 0.0)
                    ask_m = max(0.0, bid_m * 1.001)

        maker_tasks: List[asyncio.Task] = []
        open_makers: List[Tuple[str, str]] = []

        # Helpers spécifiques TM
        def _cid(*, kind: str, bundle_id: str, slice_id: str, leg: str) -> str:
            base = f"{bundle_id}:{slice_id}:{leg}:{kind}"
            return base.replace(":", "")[:32]


        # bind (once here; we keep the binding later as well for safety)

        def _tick_drift(exchange: str, symbol: str, drift_abs: float) -> float:
            try:
                gf = getattr(self.config, "get_pair_filters", None)
                if callable(gf):
                    f = gf(exchange, symbol) or {}
                    ts = float(f.get("tick_size", 0) or 0.0)
                    if ts > 0:
                        return drift_abs / ts
            except Exception:
                logging.exception("Unhandled exception")
            return 0.0

        async def _tm_replace_watch(
                ex: str,
                symbol: str,
                client_id: str,
                side: str,
                ref_best: float,
                *,
                max_age_ms: int = 2500,
                max_drift_ticks: int = 2,
        ):
            start = time.time()
            get_tob2 = getattr(self.risk_manager, "get_top_of_book", None)
            while (time.time() - start) * 1000 < max_age_ms:
                await asyncio.sleep(0.15)
                if not get_tob2:
                    break
                bid, ask = get_tob2(ex, symbol.replace("-", "").upper()) or (0.0, 0.0)
                best = ask if side.upper() == "SELL" else bid
                if best <= 0:
                    continue
                drift = abs(best - ref_best)
                if _tick_drift(ex, symbol, drift) >= max_drift_ticks:
                    try:
                        await self._cancel_order(ex, symbol, client_id, reason="maker_drift")
                    except Exception:
                        logging.exception("Unhandled exception")
                    break

        def _estimate_ahead_qty(ex: str, symbol: str, price: float, side: str) -> float:
            try:
                src = getattr(self.risk_manager, "rebal_mgr", self.risk_manager)
                ob = (getattr(src, "latest_orderbooks", {}) or {}).get(ex, {}).get(symbol.replace("-", "").upper(),
                                                                                   {})
                levels = ob.get("asks" if side.upper() == "SELL" else "bids") or []
                ahead = 0.0
                if side.upper() == "SELL":
                    for p, q in levels:
                        if p < price:
                            ahead += float(q)
                        else:
                            break
                else:
                    for p, q in levels:
                        if p > price:
                            ahead += float(q)
                        else:
                            break
                return float(ahead)
            except Exception:
                return 0.0

        async def _panic_hedge(ex: str, symbol: str, side: str, needed_usdc: float, *, max_loss_bps: float):
            bid, ask = (
                getattr(self.risk_manager, "get_top_of_book", lambda *args: (0, 0))(
                    ex, symbol.replace("-", "").upper()
                )
            )
            px = ask if side.upper() == "BUY" else bid
            if px <= 0:
                return
            order = {
                "type": "single",
                "exchange": ex,
                "symbol": symbol,
                "side": side,
                "price": px,
                "volume_usdc": needed_usdc,
                "meta": {
                    "tif_override": "IOC",
                    "fastpath_ok": True,
                    "skip_inventory": True,
                    "strategy": "TT",
                },
            }
            await self._exec_single(order)

        # Construction des makers
        for i, (amt, glabel, _idx_in_group, w) in enumerate(slices[: self.tm_max_open_makers]):
            # Profitabilité slice TM (ex-ante) — sauf rebalancing
            if not is_rebalancing:
                try:
                    sim_buy_ex = taker_leg["exchange"] if maker_leg_side == "SELL" else maker_leg["exchange"]
                    sim_sell_ex = maker_leg["exchange"] if maker_leg_side == "SELL" else taker_leg["exchange"]
                    ok_prof, _ = await self.risk_manager.is_fragment_profitable(
                        pair_key=pair_key,
                        buy_ex=sim_buy_ex,
                        sell_ex=sim_sell_ex,
                        usdc_amt=float(amt),
                        strategy="TM",
                    )
                    if not ok_prof:
                        continue
                except Exception:
                    logging.exception("Unhandled exception")

            best_price = (ask_m if maker_leg_side == "SELL" else bid_m)
            px_maker = self._price_ladder_maker(
                maker_leg["exchange"], maker_leg["symbol"], maker_leg_side, bid_m, ask_m, i
            ) or best_price

            # Queue-position guard
            ahead_quote = self._estimate_ahead_quote(
                maker_leg["exchange"], maker_leg["symbol"], px_maker, maker_leg_side)
            if ahead_quote > self.tm_queuepos_max_ahead_quote:
                ENGINE_QUEUEPOS_BLOCKED_TOTAL.labels(
                    reason="ahead",  # label canonique
                    exchange=self._norm_ex(maker_leg["exchange"]),
                    symbol=(getattr(self, "_fmt_pair", lambda s: s.replace("-", "").upper())(maker_leg["symbol"])),
                ).inc()
                logger.info("[TM] queuepos_blocked",
                            extra={"pair": pair_key, "exchange": maker_leg["exchange"],
                                   "symbol": maker_leg["symbol"],
                                   "ahead_quote": float(ahead_quote),
                                   "qpos_cap_quote": float(self.tm_queuepos_max_ahead_quote)})
                continue

            # --- Queue-position guard (QUOTE) + ETA (auto-compat) ---
            # Calcul du "ahead" en devise de cotation (QUOTE).
            # Si _estimate_ahead_quote n'existe pas encore, on retombe sur _estimate_ahead_usd.
            _est_ahead = getattr(self, "_estimate_ahead_quote", None) or getattr(self, "_estimate_ahead_usd")
            ahead_quote = float(_est_ahead(maker_leg["exchange"], maker_leg["symbol"], px_maker, maker_leg_side))

            # Cap en QUOTE (fallback si tm_queuepos_max_ahead_quote n'existe pas encore)
            qpos_cap = float(getattr(self, "tm_queuepos_max_ahead_quote",
                                     getattr(self, "tm_queuepos_max_ahead_usd", 25_000.0)))

            if ahead_quote > qpos_cap:
                ENGINE_QUEUEPOS_BLOCKED_TOTAL.labels(
                    reason="ahead",
                    exchange=self._norm_ex(maker_leg["exchange"]),
                    symbol=self._fmt_pair(maker_leg["symbol"]),
                ).inc()
                logger.info(
                    "[TM] queuepos_blocked",
                    extra={
                        "pair": pair_key,
                        "exchange": maker_leg["exchange"],
                        "symbol": self._fmt_pair(maker_leg["symbol"]),
                        "ahead_quote": float(ahead_quote),
                        "qpos_cap_quote": float(qpos_cap),
                    },
                )
                continue

            # ----- ETA guard -----
            quote_per_s = 0.0
            if self.tm_queuepos_max_eta_ms > 0:
                # Le RM expose souvent estimate_queue_drain_usd_per_s (legacy) → on l'interprète comme QUOTE/s.
                est = getattr(self.risk_manager, "estimate_queue_drain_usd_per_s", None)
                if callable(est):
                    try:
                        quote_per_s = float(est(maker_leg["exchange"], maker_leg["symbol"], maker_leg_side) or 0.0)
                    except TypeError:
                        quote_per_s = float(est(maker_leg["exchange"], pair_key, maker_leg_side) or 0.0)

                if quote_per_s == 0.0:
                    logger.debug(
                        "[TM] ETA guard actif mais aucun débit estimé (quote_per_s=0) — skip ETA",
                        extra={
                            "exchange": maker_leg["exchange"],
                            "symbol": self._fmt_pair(maker_leg["symbol"]),
                            "side": maker_leg_side,
                            "eta_cap_ms": self.tm_queuepos_max_eta_ms,
                        },
                    )
                else:
                    eta_ms = (ahead_quote / max(quote_per_s, 1e-12)) * 1000.0
                    if eta_ms > float(self.tm_queuepos_max_eta_ms):
                        ENGINE_QUEUEPOS_BLOCKED_TOTAL.labels(
                            reason="eta",
                            exchange=self._norm_ex(maker_leg["exchange"]),
                            symbol=self._fmt_pair(maker_leg["symbol"]),
                        ).inc()
                        logger.info(
                            "[TM] queuepos_blocked ETA",
                            extra={
                                "pair": pair_key,
                                "exchange": maker_leg["exchange"],
                                "symbol": self._fmt_pair(maker_leg["symbol"]),
                                "eta_ms": float(eta_ms),
                                "eta_cap_ms": float(self.tm_queuepos_max_eta_ms),
                                "ahead_quote": float(ahead_quote),
                                "quote_per_s": float(quote_per_s),
                            },
                        )
                        continue

            slice_id = f"{bundle_id}-TM-{glabel}-{i}"
            _, q_maker = SymbolUtils.split_base_quote(maker_leg["symbol"])
            meta_m = {
                "best_price": best_price,
                "best_ts": int(time.time() * 1000),
                "maker": True,
                "tif_override": "GTC",
                "skip_inventory": False,
                "fastpath_ok": True,
                "bundle_id": bundle_id,
                "slice_id": slice_id,
                "slice_group": glabel,
                "slice_index": i,
                "slice_weight": float(w),
                "planned_usdc": float(amt),
                "strategy": "TM",
                "account_alias": self.wallet_router.pick_by_quote(
                    self._norm_ex(maker_leg["exchange"]), "TM", q_maker
                ),
            }
            maker_clid = _cid(kind="PO", bundle_id=bundle_id, slice_id=slice_id, leg=maker_leg_side)
            order_maker = {
                "type": "single",
                "exchange": maker_leg["exchange"],
                "symbol": maker_leg["symbol"],
                "side": maker_leg_side,
                "price": px_maker,
                "volume_usdc": amt,
                "client_id": maker_clid,
                "meta": meta_m,
            }

            # Hedge plan (taker) — déclenché par fills du maker
            self._tm_hedges[maker_clid] = {
                "bundle_id": bundle_id,
                "slice_id": slice_id,
                "slice_group": glabel,
                "slice_weight": float(w),
                "symbol": taker_leg["symbol"],
                "hedge_exchange": taker_leg["exchange"],
                "hedge_side": hedge_side,
                "ref_price": float(taker_leg.get("price", 0.0) or 0.0),
                "seen_filled_qty": 0.0,
                "hedge_ratio": hedge_ratio,
                "maker_exchange": maker_leg["exchange"],
                "maker_side": maker_leg_side,
            }
            buy_ex = taker_leg["exchange"] if maker_leg_side == "SELL" else maker_leg["exchange"]
            sell_ex = maker_leg["exchange"] if maker_leg_side == "SELL" else taker_leg["exchange"]
            route = f"BUY:{buy_ex}→SELL:{sell_ex}"

            maker_evt = {
                "pair": pair_key,
                "timestamp": time.time(),
                "executed_volume_usdc": 0.0,
                "status": "maker_placed",
                "trade_mode": "TM",
                "buy_ex": buy_ex,
                "sell_ex": sell_ex,
                "route": route,
                "leg_role": "maker",
                "trade_id": maker_clid,
                "account_alias": meta_m["account_alias"],
            }
            await self._hist("trade", maker_evt)

            open_makers.append((maker_leg["exchange"], maker_clid))
            okg, whyg = self._pre_submit_guards({"exchange": maker_leg["exchange"], "symbol": maker_leg["symbol"],
                                                 "side": maker_leg_side, "price": px_maker, "meta": meta_m})
            if not okg:
                continue

            maker_tasks.append(asyncio.create_task(self._exec_single(order_maker)))

            # Watch replacement si le best dérive trop
            self._spawn(
                _tm_replace_watch(
                    maker_leg["exchange"], maker_leg["symbol"], maker_clid, maker_leg_side, best_price
                ),
                name=f"tm-repl-{maker_clid[-6:]}"
            )

        async def _tm_watchdog_and_cancel():
            """
            Watchdog TM :
              - si la paire est en 'mute' TM, CANCEL immédiatement tous les makers (raison='tm_muted');
              - sinon, attendre tm_watch_timeout_s puis CANCEL NEW/ACK (raison='ack_timeout').
            """
            try:
                pk = pair_key
            except NameError:
                try:
                    pk = (maker_leg.get("symbol") or "").replace("-", "").upper()
                except Exception:
                    pk = ""

            try:
                if pk and self._is_muted("TM", pk):
                    for ex_, clid in list(open_makers):
                        try:
                            await self._cancel_order(ex_, maker_leg["symbol"], clid, reason="tm_muted")
                        except Exception:
                            logging.exception("tm_muted cancel failed")
                    return
            except Exception:
                logging.exception("mute check failed")

            await asyncio.sleep(self.tm_watch_timeout_s)
            for ex_, clid in list(open_makers):
                fsm = self._order_fsm.get(clid)
                if fsm and fsm.state in {"NEW", "ACK"}:
                    try:
                        await self._cancel_order(ex_, maker_leg["symbol"], clid, reason="ack_timeout")
                    except Exception:
                        logging.exception("ack_timeout cancel failed")

        async def _tm_nn_ttl_enforcer():
            if mode == "NEUTRAL":
                return
            await asyncio.sleep(max(0, self.tm_exposure_ttl_ms) / 1000.0)
            target_ratio = max(0.0, min(1.0, float(self.tm_exposure_ttl_hedge_ratio)))
            if target_ratio <= 0.0:
                return

            # Somme des hedges déjà réalisés via fills maker (USDC * hedge_ratio)
            done = 0.0
            for clid, plan in list(self._tm_hedges.items()):
                if plan.get("bundle_id") != bundle_id:
                    continue
                seen_base = float(plan.get("seen_filled_qty", 0.0))
                ref_px = float(plan.get("ref_price", 0.0))
                hratio = float(plan.get("hedge_ratio", hedge_ratio))
                done += max(0.0, seen_base * ref_px * hratio)

            target = total_usdc * target_ratio
            needed_usdc = max(0.0, target - done)
            if needed_usdc <= 0:
                return

            side_now = ("BUY" if hedge_side.upper() == "BUY" else "SELL")
            await _panic_hedge(
                taker_leg["exchange"], taker_leg["symbol"], side_now, needed_usdc, max_loss_bps=allow_loss_bps
            )

        # Lancer watchdog & TTL
        self._spawn(_tm_watchdog_and_cancel(), name=f"tm-wd-{bundle_id[-6:]}")
        if mode == "NN":
            self._spawn(_tm_nn_ttl_enforcer(), name=f"tm-ttl-{bundle_id[-6:]}")

        # ✅ S'assurer que les submissions makers sont parties
        if maker_tasks:
            await asyncio.gather(*maker_tasks, return_exceptions=True)

    # ---- HTTP helpers + retry -------------------------------------------------
    def _pre_submit_guards(self, order_or_bundle: Dict[str, Any]) -> tuple[bool, str]:
        """
        (ok, reason)
        - Single: vérifie anchor/staleness vs meta.best_price/best_ts
        - Bundle (TT/TM): vérifie RM.revalidate_arbitrage + anchors des deux jambes
        """
        try:
            if "legs" not in order_or_bundle:
                # SINGLE
                o = order_or_bundle
                px = float(o.get("price") or 0.0)
                meta = o.get("meta", {}) or {}
                best = float(meta.get("best_price", px) or 0.0)
                best_ts_ms = self._to_ms(meta.get("best_ts"))
                if px <= 0 or not self._within_anchor_guard(px, best, best_ts_ms):
                    return False, "anchor_guard/single"
                return True, ""
            # BUNDLE
            legs = order_or_bundle.get("legs", [])
            if len(legs) != 2:
                return False, "bundle_bad_legs"
            buy = next((l for l in legs if str(l.get("side")).upper() == "BUY"), None)
            sell = next((l for l in legs if str(l.get("side")).upper() == "SELL"), None)
            if not buy or not sell:
                return False, "bundle_missing_buy_sell"
            pair_key = (buy.get("symbol") or sell.get("symbol") or "").replace("-", "").upper()

            # anchors par jambe
            for l in (buy, sell):
                px = float(l.get("price") or 0.0)
                meta = l.get("meta", {}) or {}
                best = float(meta.get("best_price", px) or 0.0)
                best_ts_ms = self._to_ms(meta.get("best_ts"))
                if px <= 0 or not self._within_anchor_guard(px, best, best_ts_ms):
                    return False, "anchor_guard/bundle"

            # revalidate RM (si dispo)
            rm = getattr(self, "risk_manager", None)
            if rm and hasattr(rm, "revalidate_arbitrage"):
                try:
                    expected = order_or_bundle.get("expected_net_spread")
                    allow_loss_bps = float(
                        (order_or_bundle.get("meta") or {}).get("allow_loss_bps", 0.0)
                    )
                    # ... à l’intérieur de _pre_submit_guards, bloc RM.revalidate_arbitrage ...
                    try:
                        ok = rm.revalidate_arbitrage(
                            buy_ex=buy["exchange"], sell_ex=sell["exchange"], pair_key=pair_key,
                            expected_net=(expected if expected else None),
                            max_drift_bps=float(getattr(self.config, "max_drift_bps", 7.0)),
                            min_required_bps=None,
                            is_rebalancing=((order_or_bundle.get("meta") or {}).get("type") == "rebalancing"),
                            buy_px_override=float(buy["price"]), sell_px_override=float(sell["price"]),
                        )
                    except TypeError:
                        # Ancienne signature sans overrides
                        ok = rm.revalidate_arbitrage(
                            buy_ex=buy["exchange"], sell_ex=sell["exchange"], pair_key=pair_key,
                            expected_net=(expected if expected else None),
                            max_drift_bps=float(getattr(self.config, "max_drift_bps", 7.0)),
                            min_required_bps=None,
                            is_rebalancing=((order_or_bundle.get("meta") or {}).get("type") == "rebalancing"),
                        )
                    if not ok:
                        return False, "rm_revalidate_fail"

                except Exception as e:
                    report_nonfatal("ExecutionEngine", "rm_revalidate_exc", e, phase="_pre_submit_guards")
                    return False, "rm_revalidate_exc"

            return True, ""
        except Exception as e:
            return False, f"guards_exc/{type(e).__name__}"

    async def _with_retry(self: "ExecutionEngine", fn: Any, *a, **kw):
        attempts = 3
        for i in range(attempts):
            try:
                res = await fn(*a, **kw)
                if i > 0:
                    ENGINE_RETRIES_TOTAL.labels(kind="success_after_retry").inc()
                return res
            except Exception as e:
                retryable = False
                if aiohttp and isinstance(e, aiohttp.ClientResponseError):
                    status = e.status
                    retryable = (status == 429) or (500 <= status < 600)
                elif aiohttp and isinstance(e, aiohttp.ServerTimeoutError):
                    retryable = True
                elif isinstance(e, asyncio.TimeoutError):
                    retryable = True
                else:
                    # autres exceptions réseau: on retente une fois, sinon fatal
                    retryable = (i < attempts - 1)

                if i < attempts - 1 and retryable:
                    ENGINE_RETRIES_TOTAL.labels(kind="retryable").inc()
                    await asyncio.sleep(0.08 * (2 ** i))
                    continue

                ENGINE_RETRIES_TOTAL.labels(kind="final_failure").inc()
                raise

    # --------------------------- low-level HTTP ------------------------
    def _pick_keys(self, exchange: str, account_alias: Optional[str], strategy: Optional[str]) -> Dict[str, str]:
        ex_name = self._ex_venue(exchange)  # <-- canonique pour venue_keys
        ex_map = self.venue_keys.get(ex_name, {}) or {}
        if account_alias and account_alias in ex_map:
            return ex_map[account_alias]
        strat = str(strategy or "TT").upper()
        if strat in ex_map:
            return ex_map[strat]
        return next(iter(ex_map.values()), {})

    # ---------------------------------------------------------------------------
    # LIMIT ORDERS (TT/TM) — Industry-like with PACER hooks
    # Remplace intégralement la méthode existante.
    # ---------------------------------------------------------------------------
    # ---------------------------------------------------------------------------
    # LIMIT ORDERS (TT/TM) — Industry-like with PACER hooks
    # Remplace intégralement la méthode existante.
    # ---------------------------------------------------------------------------
    # execution_engine.py — class ExecutionEngine
    async def _place_limit(
            self: "ExecutionEngine",
            exchange: str,
            symbol: str,
            side: str,
            qty: float,
            price: float,
            client_id: str,
            *,
            tif: str = "GTC",
            maker: bool = False,
            meta: dict = None,
    ):
        """
        LIMIT (TT/TM) avec:
          - Hints PACER (IOC / MM freeze),
          - Overlays RM (IOC-only / MM off),
          - Cap inflight global,
          - Pacing post-envoi.
        """
        import asyncio
        self._ensure_ready()  # readiness Engine

        ex = self._norm_ex(exchange)
        meta = dict(meta or {})
        alias = meta.get("account_alias")
        strategy = meta.get("strategy")

        # --- Step 2: PACER + RM overlays ---
        pol = getattr(self, "_pacer", None).current_policy if getattr(self, "_pacer", None) else None
        post_only = bool(meta.get("post_only", False))
        tif = str(meta.get("tif") or tif or "GTC").upper()
        maker = bool(meta.get("maker", maker))

        # PACER flags
        if pol and bool((pol.get("flags") or {}).get("ioc_only")):
            tif, post_only, maker = "IOC", False, False
        if pol and bool((pol.get("flags") or {}).get("mm_frozen")):
            maker, post_only = False, False

        # RM overlays
        tif, maker, post_only, skip_maker = self._rm_ioc_mm_overrides(
            tif=tif, maker=maker, post_only=post_only
        )
        if skip_maker and meta.get("maker", False):
            # Leg maker interdit par RM: on refuse proprement ce leg
            return {"status": "REJECT", "reason": "mm_disabled_by_rm"}

        # --- Cap inflight (global) ---
        try:
            pol = getattr(self, "_pacer", None).current_policy if getattr(self, "_pacer", None) else {}
            inflight_cap = int((pol or {}).get("inflight_max", 1))
            if hasattr(self, "_inflight_curr") and isinstance(self._inflight_curr, dict):
                current_inflight = sum(int(v or 0) for v in self._inflight_curr.values())
            else:
                current_inflight = int(getattr(self, "_inflight", 0))
            if current_inflight >= inflight_cap:
                return {"status": "DEFERRED", "reason": "pacer_inflight_cap", "cap": inflight_cap}
        except Exception:
            pass

        # --- Routing vers le placer natif + retry standard ---
        if ex == "BINANCE":
            async def _do():
                return await self._binance_limit(
                    symbol, side, qty, price, client_id,
                    tif=tif, maker=maker, keys=self._pick_keys("Binance", alias, strategy)
                )

            resp = await self._with_retry(_do)

        elif ex == "BYBIT":
            async def _do():
                return await self._bybit_limit(
                    symbol, side, qty, price, client_id,
                    tif=tif, maker=maker, keys=self._pick_keys("Bybit", alias, strategy)
                )

            resp = await self._with_retry(_do)

        elif ex == "COINBASE":
            async def _do():
                return await self._coinbase_limit(
                    product_id=symbol, side=side, qty=qty, price=price, client_id=client_id,
                    tif=tif, maker=maker, keys=self._pick_keys("Coinbase", alias, strategy)
                )

            resp = await self._with_retry(_do)

        else:
            raise ValueError(f"Unsupported exchange for limit order: {ex}")

        # Pacing post-envoi (PACER)
        try:
            pol = getattr(self, "_pacer", None).current_policy if getattr(self, "_pacer", None) else {}
            delay_ms = int((pol or {}).get("pacing_ms", 0))
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000.0)
        except Exception:
            pass

        return resp

    # ----- Binance -----
    async def _binance_limit(self: "ExecutionEngine", symbol: str, side: str, qty: float, price: float,
                             client_id: str, *,
                             tif: str = "GTC", maker: bool = False, keys: Dict[str, str] = None):
        assert self._session, "ClientSession not started"
        await self._rl_binance_order.acquire()
        url = "https://api.binance.com/api/v3/order"
        ts = int(time.time() * 1000)
        otype = "LIMIT_MAKER" if maker else "LIMIT"
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": otype,
            "newClientOrderId": client_id,
            "quantity": f"{qty:.8f}",
            "price": f"{price:.8f}",
            "recvWindow": 5000,
            "timestamp": ts,
        }
        if not maker:
            params["timeInForce"] = tif
        query = "&".join(f"{k}={v}" for k, v in params.items())
        sig = hmac.new((keys or {}).get("secret", "").encode(), query.encode(), hashlib.sha256).hexdigest()
        headers = {"X-MBX-APIKEY": (keys or {}).get("api_key", "")}
        params["signature"] = sig
        async with self._session.post(url, params=params, headers=headers, timeout=5) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _binance_cancel(self: "ExecutionEngine", symbol: str, client_id: Optional[str] = None,
                              order_id: Optional[str] = None, *, keys: Dict[str, str] | None = None):
        assert self._session, "ClientSession not started"
        await self._rl_binance_order.acquire()
        url = "https://api.binance.com/api/v3/order"
        ts = int(time.time() * 1000)
        params = {"symbol": symbol, "recvWindow": 5000, "timestamp": ts}
        if order_id:
            params["orderId"] = order_id
        elif client_id:
            params["origClientOrderId"] = client_id
        else:
            raise ValueError("Binance cancel: need order_id or client_id")
        q = "&".join(f"{k}={v}" for k, v in params.items())
        k = keys or self._pick_keys("Binance", None, None)
        sig = hmac.new(k.get("secret", "").encode(), q.encode(), hashlib.sha256).hexdigest()
        headers = {"X-MBX-APIKEY": k.get("api_key", "")}
        params["signature"] = sig
        async with self._session.delete(url, params=params, headers=headers, timeout=5) as resp:
            resp.raise_for_status()
            return await resp.json()

    # ----- Bybit v5 (Spot) -----
    async def _bybit_limit(self: "ExecutionEngine", symbol: str, side: str, qty: float, price: float,
                           client_id: str,
                           *, tif: str = "GTC", maker: bool = False, keys: Dict[str, str] = None):
        assert self._session, "ClientSession not started"
        await self._rl_bybit_order.acquire()
        url = "https://api.bybit.com/v5/order/create"
        ts = str(int(time.time() * 1000))
        recv_window = "5000"
        tif_map = {"IOC": "IOC", "FOK": "FOK", "GTC": "GTC"}
        ord_type = "Limit"
        time_in_force = "PostOnly" if maker else tif_map.get(tif.upper(), "GTC")
        body = {
            "category": "spot",
            "symbol": symbol,
            "side": "Buy" if side.upper() == "BUY" else "Sell",
            "orderType": ord_type,
            "qty": f"{qty:.8f}",
            "price": f"{price:.8f}",
            "timeInForce": time_in_force,
            "orderLinkId": client_id,
        }
        body_json = json.dumps(body, separators=(",", ":"))
        to_sign = ts + (keys or {}).get("api_key", "") + recv_window + body_json
        sign = hmac.new((keys or {}).get("secret", "").encode(), to_sign.encode(), hashlib.sha256).hexdigest()
        headers = {
            "X-BAPI-API-KEY": (keys or {}).get("api_key", ""),
            "X-BAPI-SIGN": sign,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json",
        }
        async with self._session.post(url, headers=headers, data=body_json, timeout=7) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _bybit_cancel(self: "ExecutionEngine", symbol: str, client_id: Optional[str] = None,
                            order_id: Optional[str] = None, *, keys: Dict[str, str] | None = None):
        assert self._session, "ClientSession not started"
        await self._rl_bybit_order.acquire()
        url = "https://api.bybit.com/v5/order/cancel"
        ts = str(int(time.time() * 1000))
        recv_window = "5000"
        body = {"category": "spot", "symbol": symbol}
        if order_id:
            body["orderId"] = order_id
        elif client_id:
            body["orderLinkId"] = client_id
        else:
            raise ValueError("Bybit cancel: need order_id or client_id")
        body_json = json.dumps(body, separators=(",", ":"))
        k = keys or self._pick_keys("Bybit", None, None)
        to_sign = ts + k.get("api_key", "") + recv_window + body_json
        sign = hmac.new(k.get("secret", "").encode(), to_sign.encode(), hashlib.sha256).hexdigest()
        headers = {
            "X-BAPI-API-KEY": k.get("api_key", ""),
            "X-BAPI-SIGN": sign,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json",
        }
        async with self._session.post(url, headers=headers, data=body_json, timeout=7) as resp:
            resp.raise_for_status()
            return await resp.json()

    # ----- Coinbase AT (stub sécu) -----
    # GARDE SEULEMENT **UNE** version de _coinbase_limit et mets le garde dry-run en tête
    async def _coinbase_limit(
            self,
            product_id,
            side,
            qty,
            price,
            client_id,
            *,
            tif="GTC",
            maker=False,
            keys=None
    ):
        """
        Coinbase Advanced Trade (brokerage) — création d'ordre LIMIT.
        - Unifie la méthode (une seule version).
        - Garde dry-run en tête.
        - Normalise product_id (ex: BTCUSDC -> BTC-USDC).
        - Map TIF -> order_configuration.* (GTC/IOC/FOK).
        - post_only uniquement pour GTC (LIMIT_MAKER-like); si maker avec IOC/FOK, on fallback en GTC+post_only.
        - Signature AT: HMAC-SHA256(base64(secret), timestamp + method + path + body) puis base64 encode.
        """
        # Dry-run court-circuite l'appel HTTP
        if self._is_dry("COINBASE"):
            return {"status": "simulated", "exchange": "Coinbase", "clientOrderId": client_id}

        assert self._session, "ClientSession not started"
        await self._rl_coinbase_order.acquire()

        # Normalisation du product_id (BTCUSDC -> BTC-USDC) si besoin
        orig = str(product_id)
        if "-" not in orig:
            s = orig.replace("-", "").upper()
            for q in ("USDC", "USDT", "EUR", "USD"):
                if s.endswith(q):
                    product_id = f"{s[:-len(q)]}-{q}"
                    break
            else:
                # pas de quote reconnue -> on garde tel quel
                product_id = s

        # Mapping TIF → configuration Coinbase AT
        tif_u = str(tif).upper()
        # Si maker demandé sur IOC/FOK, fallback en GTC post_only (Coinbase n'accepte pas post_only sur IOC/FOK)
        if maker and tif_u in ("IOC", "FOK"):
            tif_u = "GTC"

        order_cfg_key = {
            "GTC": "limit_limit_gtc",
            "IOC": "limit_limit_ioc",
            "FOK": "limit_limit_fok",
        }.get(tif_u, "limit_limit_gtc")

        order_configuration = {
            order_cfg_key: {
                "base_size": f"{float(qty):.8f}",
                "limit_price": f"{float(price):.8f}",
            }
        }
        # post_only seulement pour GTC
        if order_cfg_key == "limit_limit_gtc":
            order_configuration[order_cfg_key]["post_only"] = bool(maker)

        body = {
            "client_order_id": str(client_id),
            "product_id": str(product_id),
            "side": str(side).upper(),  # "BUY" / "SELL"
            "order_configuration": order_configuration,
        }

        # ---- Signature Coinbase Advanced Trade ----
        url = "https://api.coinbase.com/api/v3/brokerage/orders"
        path = "/api/v3/brokerage/orders"
        method = "POST"
        ts = str(int(time.time()))
        body_json = json.dumps(body, separators=(",", ":"))

        api_key = (keys or {}).get("api_key", "")
        secret_b64 = (keys or {}).get("secret", "")

        # secret AT est base64-encodé -> on décode avant HMAC
        try:
            secret = base64.b64decode(secret_b64)
        except Exception:
            # fallback: si le secret n'est pas base64, on tente brut (environnement legacy)
            secret = secret_b64.encode()

        prehash = ts + method + path + body_json
        sig = hmac.new(secret, prehash.encode(), hashlib.sha256).digest()
        sig_b64 = base64.b64encode(sig).decode()

        headers = {
            "CB-ACCESS-KEY": api_key,
            "CB-ACCESS-SIGN": sig_b64,
            "CB-ACCESS-TIMESTAMP": ts,
            "Content-Type": "application/json",
        }

        # POST
        async with self._session.post(url, headers=headers, data=body_json, timeout=7) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _coinbase_cancel(self, symbol: str, client_id: str | None = None, order_id: str | None = None, *,
                               keys: dict | None = None):
        # DRY-RUN
        if getattr(self, "_is_dry", lambda *_: False)("COINBASE"):
            return {"status": "simulated", "exchange": "Coinbase", "clientOrderId": client_id, "orderId": order_id}

        assert self._session, "ClientSession not started"
        await self._rl_coinbase_order.acquire()

        url = "https://api.coinbase.com/api/v3/brokerage/orders/cancel"
        ts = str(int(time.time() * 1000))
        payload = {}
        if order_id:
            payload["order_id"] = order_id
        if client_id:
            payload["client_order_id"] = client_id
        body = json.dumps(payload, separators=(",", ":"))
        prehash = ts + "POST" + "/api/v3/brokerage/orders/cancel" + body

        sig = self._cb_sign((keys or {}).get("secret", ""), prehash)
        headers = {
            "CB-ACCESS-KEY": (keys or {}).get("api_key", ""),
            "CB-ACCESS-SIGN": sig,
            "CB-ACCESS-TIMESTAMP": ts,
            "Content-Type": "application/json",
        }

        async with self._session.post(url, headers=headers, data=body, timeout=7) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _exec_single(self, order: Dict[str, Any]) -> bool:
        # READINESS GUARD
        self._ensure_ready()

        ex = order.get("exchange")
        symbol = order.get("symbol")
        side = str(order.get("side") or "").upper()
        price = float(order.get("price", 0))
        vol_usdc = float(order.get("volume_usdc", 0))
        client_id = order.get("client_id") or f"S{int(time.time() * 1000)}"



        # anti-dup
        if client_id in self._seen_client_ids:
            logger.info("[Engine] duplicate client_id ignoré: %s", client_id)
            return False
        self._seen_client_ids.add(client_id)

        # map cancel ultérieur
        try:
            self._client_symbol_map[client_id] = (self._norm_ex(ex), str(symbol).replace("-", "").upper())
        except Exception:
            logging.exception("Unhandled exception in _client_symbol_map set")

        meta = (order.get("meta") or {})
        is_maker = bool(meta.get("maker", False))

        pair_key = (meta.get("pair_key")
                    or str(symbol).replace("-", "").upper())

        # 1) Circuits (vol/profondeur/mutes)
        if not self._pre_trade_circuits(pair_key,
                                        {"exchange": ex, "symbol": symbol, "meta": meta},
                                        {"exchange": ex, "symbol": symbol, "meta": {}}):
            self._reject(symbol, order, "circuit_breaker(single)")
            self._cleanup_client(client_id)
            return False

        # 2) STP pré-taker (neut. makers opposés sur la même venue/symbole)
        try:
            dummy_buy = {"exchange": ex, "symbol": symbol, "meta": {"maker": (side.upper() == "SELL")}}
            dummy_sell = {"exchange": ex, "symbol": symbol, "meta": {"maker": (side.upper() == "BUY")}}
            await self._stp_pad_makers_before_taker(pair_key, dummy_buy, dummy_sell)
        except Exception:
            logging.exception("STP pre-taker failed (single)")

        # 3) Marquer activité TT (single = taker) → scaling MM + hysteresis
        self._mark_pair_activity(pair_key, "TT")
        try:
            self._spawn(self._restore_mm_after_hysteresis(pair_key), name=f"mm-rest-{pair_key[-6:]}")
        except Exception:
            pass

        # champs requis
        if not all([ex, symbol, side]) or price <= 0 or vol_usdc <= 0:
            self._reject(symbol, order, "invalide")
            self._cleanup_client(client_id)
            return False

        # pair_key normalisé
        try:
            pair_key = self._fmt_pair(meta.get("pair_key") or symbol)
        except Exception:
            pair_key = str(meta.get("pair_key") or str(symbol).replace("-", "").upper())

        # stratégie & alias auto
        if not meta.get("strategy"):
            meta["strategy"] = "TM" if is_maker else "TT"
        if not meta.get("account_alias"):
            try:
                _, q = SymbolUtils.split_base_quote(symbol)
                meta["account_alias"] = self.wallet_router.pick_by_quote(
                    self._norm_ex(ex),
                    "TM" if is_maker else "TT",
                    q,
                )
            except Exception:
                logging.exception("Wallet routing failed; fallback alias empty")
                meta.setdefault("account_alias", "")

        # === CIRCUITS + STP + SCALING-HOOK (avant tout submit) ===
        # On fabrique 2 "legs" cohérents pour réutiliser les circuits (TT si taker, MM si maker)
        taker_like = {"exchange": ex, "symbol": symbol, "meta": {"maker": False}}
        maker_like = {"exchange": ex, "symbol": symbol, "meta": {"maker": True}}
        leg_a = maker_like if is_maker else taker_like
        leg_b = maker_like if is_maker else taker_like

        # 1) Circuits (vol/profondeur/mute)
        if not self._pre_trade_circuits(pair_key, leg_a, leg_b):
            await self._hist("trade", {
                "status": "dropped",
                "reason": "circuit_breaker",
                "exchange": str(ex).upper(),
                "symbol": str(symbol).upper(),
                "pair_key": pair_key,
                "ts": time.time(),
            })
            self._cleanup_client(client_id)
            return False

        # 2) STP : neutraliser makers opposés avant un TAKER single
        if (not is_maker) and getattr_bool(self.config, "stp_pre_taker_enabled", True):
            opposite_side = "SELL" if side == "BUY" else "BUY"
            to_cancel = []
            for cid, fsm in list(self._order_fsm.items()):
                try:
                    m = getattr(fsm, "meta", {}) if hasattr(fsm, "meta") else {}
                    ex_m = str((m.get("exchange") or m.get("ex") or "")).upper()
                    sym_m = str(m.get("symbol") or "").upper().replace("-", "")
                    maker_m = bool(m.get("maker", False))
                    side_m = str(m.get("side") or "").upper()
                    if ex_m == self._norm_ex(ex) and sym_m == str(symbol).upper().replace("-",
                                                                                          "") and maker_m and side_m == opposite_side:
                        to_cancel.append(cid)
                except Exception:
                    continue
            for cid in to_cancel:
                try:
                    await self._cancel_order(self._norm_ex(ex), str(symbol).upper(), cid, reason="stp_pre_taker")
                except Exception:
                    logging.exception("STP pre-taker cancel failed")

        # 3) Scaling MM si activité TT/TM (single taker => on marque l'activité)
        if not is_maker:
            self._mark_pair_activity(pair_key, "TT")
            try:
                self._spawn(self._restore_mm_after_hysteresis(pair_key), name=f"mm-rest-{pair_key[-6:]}")
            except Exception:
                pass
        # === /CIRCUITS + STP + SCALING-HOOK ===

        # price-guard (ancre) + price-band vol-aware
        best = float(meta.get("best_price", price))
        best_ts_ms = self._to_ms(meta.get("best_ts"))
        if not self._within_anchor_guard(price, best, best_ts_ms):
            self._reject(symbol, order, "price_guard(anchor)")
            self._cleanup_client(client_id)
            return False
        try:
            vol_bps = self._volatility_bps(pair_key)
            if not self._within_price_band(price, best, pair_key, vol_bps):
                self._reject(symbol, order, f"price_band(vol={round(vol_bps, 1)}bps)")
                self._cleanup_client(client_id)
                return False
        except Exception:
            logging.exception("price-band check failed")

        # qty base (normalisation filtres)
        qty_base = round(vol_usdc / price, 8)
        qty_base, price = self._normalize_for_exchange(ex, symbol, side, qty_base, price)
        if qty_base <= 0 or price <= 0:
            self._reject(symbol, order, "qty_or_price_after_normalize_zero")
            self._cleanup_client(client_id)
            return False

        # Re-check ancre après normalisation
        if not self._within_anchor_guard(price, best, best_ts_ms):
            self._reject(symbol, order, "price_guard(anchor/post-normalize)")
            self._cleanup_client(client_id)
            return False

        # ---- DRY-RUN
        if bool(getattr(self.config, "dry_run", True)):
            alias = str(meta.get("account_alias")).upper()
            await self._apply_fill_to_balances(ex, alias, symbol, side, vol_usdc)

            # Hedge TM immédiat (simulation) si maker
            if meta.get("strategy") == "TM" and is_maker and client_id in self._tm_hedges:
                plan = self._tm_hedges.get(client_id) or {}
                hedge_ex = plan.get("hedge_exchange")
                hedge_side = plan.get("hedge_side")
                hedge_sym = plan.get("symbol", symbol)
                hedge_ratio = float(plan.get("hedge_ratio", getattr(self.config, "tm_neutral_hedge_ratio", 1.0)))
                pk = hedge_sym.replace("-", "")
                bid, ask = 0.0, 0.0
                get_tob = getattr(self.risk_manager, "get_top_of_book", None)
                if callable(get_tob):
                    bid, ask = get_tob(hedge_ex, pk) or (0.0, 0.0)
                hedge_px = float(bid if hedge_side == "SELL" else ask) or float(plan.get("ref_price", price))
                _, q = SymbolUtils.split_base_quote(hedge_sym)
                hedge_order = {
                    "type": "single",
                    "exchange": hedge_ex,
                    "symbol": hedge_sym,
                    "side": hedge_side,
                    "price": hedge_px,
                    "volume_usdc": vol_usdc * hedge_ratio,
                    "client_id": f"H{int(time.time() * 1000)}",
                    "meta": {
                        "best_price": hedge_px,
                        "best_ts": int(time.time() * 1000),
                        "tif_override": "IOC",
                        "fastpath_ok": True,
                        "skip_inventory": True,
                        "bundle_id": plan.get("bundle_id"),
                        "slice_id": plan.get("slice_id"),
                        "slice_group": plan.get("slice_group"),
                        "slice_weight": plan.get("slice_weight"),
                        "planned_usdc": vol_usdc * hedge_ratio,
                        "strategy": "TT",
                        "account_alias": self.wallet_router.pick_by_quote(hedge_ex, "TT", q),
                    },
                }
                await self._exec_single(hedge_order)

            result = {
                "status": "simulated",
                "exchange": ex,
                "symbol": symbol,
                "side": side,
                "qty": qty_base,
                "price": price,
                "executed_usdc": round(qty_base * price, 2),
                "client_id": client_id,
                "executed_at": datetime.utcnow().isoformat(),
                "type": "single",
                "meta": meta,
            }
            inc_engine_trade("executed", "single", "rebalancing" if (meta.get("type") == "rebalancing") else "standard")
            await self._on_filled(result, order_type="single", is_rebal=(meta.get("type") == "rebalancing"))
            await self._hist("trade", result)
            self._tm_hedges.pop(client_id, None)
            self._cleanup_client(client_id)
            return True

        # ---- LIVE
        try:
            tif = self._get_tif(meta, is_bundle=False)
            fsm = OrderFSM(client_id, ttl_s=self.order_timeout_s)
            fsm.target_qty = qty_base
            fsm.mark_sent()
            # --- attacher meta (et quelques champs utiles) au FSM pour STP/diag
            try:
                fsm.meta = dict(meta or {})
                fsm.meta.update({
                    "exchange": self._norm_ex(ex),
                    "symbol": str(symbol).upper(),
                    "side": str(side).upper(),
                    "maker": bool(is_maker),
                })
            except Exception:
                logging.exception("Unhandled exception attaching meta to FSM")

            self._order_fsm[client_id] = fsm

            # hints pour cancel signé (multi-wallet)
            try:
                alias_hint = str(meta.get("account_alias", "")).upper()
                strat_hint = meta.get("strategy")
                self._order_keys_hint[client_id] = (self._norm_ex(ex), alias_hint, strat_hint)
            except Exception:
                logging.exception("Unhandled exception setting order_keys_hint")

            # watchdog ACK
            self._spawn(self._watchdog_order(ex, symbol, client_id), name=f"wd-ack-{client_id[-6:]}")

            ok, why = self._pre_submit_guards({
                "exchange": ex, "symbol": symbol, "side": side, "price": price, "meta": meta
            })
            if not ok:
                self._reject(symbol, order, f"pre_submit_guard({why})")
                self._cleanup_client(client_id)
                return False

            self._t_submit_ms[client_id] = int(time.time() * 1000)
            try:
                self._submit_ts_ns[client_id] = time.perf_counter_ns()
            except Exception:
                logging.exception("Unhandled exception setting _submit_ts_ns")

            # POST
            async with self._inflight_scope(ex):
                resp = await self._place_limit(
                    ex, symbol, side, qty_base, price, client_id, tif=tif, maker=is_maker, meta=meta
                )

            # marquer ACK si renvoyé inline
            try:
                ex_oid = str(
                    (resp or {}).get("orderId")
                    or (resp or {}).get("result", {}).get("orderId")
                    or (resp or {}).get("data", [{}])[0].get("ordId")
                )
                if ex_oid:
                    fsm.on_ack(ex_oid, target_qty=qty_base)
            except Exception:
                logging.exception("Unhandled exception extracting orderId")

            # log "submitted"
            submit_evt = {
                "status": "submitted",
                "exchange": ex,
                "symbol": symbol,
                "side": side,
                "qty": qty_base,
                "price": price,
                "client_id": client_id,
                "exchange_response": resp,
                "submitted_at": datetime.utcnow().isoformat(),
                "type": "single",
                "meta": meta,
                "t_sent_ms": fsm.t_sent_ms,
                "t_ack_ms": fsm.t_ack_ms,
            }
            await self._hist("trade", submit_evt)

            # Attente optionnelle ACK
            if bool(meta.get("wait_ack", False)):
                await self._wait_ack(client_id, timeout=self.order_timeout_s)

            return True

        except Exception as e:
            logger.error(f"❌ Exec {ex} fail: {e}")
            self.stats.total_failed += 1
            self.stats.error_count += 1
            inc_engine_trade("failed", "single", "standard")
        finally:
            pass

        self._cleanup_client(client_id)
        return False


# ---------------------------------------------------------------------------
# Coinbase signer (Advanced Trade): base64(HMAC_SHA256(secret, prehash))
# prehash = timestamp + method + request_path + body
# ---------------------------------------------------------------------------
def _cb_sign(_: "ExecutionEngine", secret: str, prehash: str) -> str:
    try:
        raw = hmac.new(str(secret or "").encode(), str(prehash or "").encode(), hashlib.sha256).digest()
        return base64.b64encode(raw).decode()
    except Exception:
        logging.exception("ExecutionEngine._cb_sign: failed")
        return ""

# ---------------------------------------------------------------------------
# Historisation compacte (NO-OP si pas de history_sink)
# ---------------------------------------------------------------------------
async def _hist(self: "ExecutionEngine", kind: str, data: dict) -> None:
    try:
        rec = dict(data or {})
        rec.setdefault("_kind", str(kind))
        if callable(self.history_sink):
            # asynchrone permis: on pousse côté sink si présent

            try:
                self.history_sink(rec)
            except Exception:
                logging.exception("ExecutionEngine._hist: sink failed")
            try:
                if hasattr(self, "pnl_agg") and self.pnl_agg:
                    self.pnl_agg.on_trade_event(rec)
            except Exception:
                logging.exception("PnLAggregator.on_trade_event failed")

        else:
            # fallback log discret
            logging.getLogger("ExecutionEngine").debug("[HIST] %s", rec)
    except Exception:
        logging.exception("ExecutionEngine._hist: error")

# ---------------------------------------------------------------------------
# Rejet "propre" (stats + navette observabilité + historisation)
# ---------------------------------------------------------------------------
def _reject(self: "ExecutionEngine", symbol_or_pair: str, order_or_payload: dict, reason: str) -> None:
    try:
        self.stats.rejected_by_risk += 1
        sym = str(symbol_or_pair or "").replace("-", "").upper()
        rej = self.stats.rejected_symbols or {}
        rej[sym] = int(rej.get(sym, 0)) + 1
        self.stats.rejected_symbols = rej
        ENGINE_CANCELLATIONS_TOTAL.labels(reason=str(reason or "rejected")).inc()

        evt = {
            "status": "rejected",
            "reason": reason,
            "symbol": sym,
            "payload": {k: order_or_payload.get(k) for k in ("type", "exchange", "side", "price", "volume_usdc", "meta")},
            "ts": time.time(),
        }
        asyncio.create_task(self._hist("reject", evt))
    except Exception:
        logging.exception("ExecutionEngine._reject: error")

# ---------------------------------------------------------------------------
# Notification fill DRY-RUN (listeners + stats)
# ---------------------------------------------------------------------------
async def _on_filled(self: "ExecutionEngine", result: dict, *, order_type: str, is_rebal: bool) -> None:
    """
    Callbacks post-FILL :
      - Maintien des compteurs & dernier trade
      - Notification listeners
      - Learning 'reality-gap' (offset min_required_bps par paire)
    """
    try:
        # ==== Stats ====
        self.stats.total_executed += 1
        if is_rebal:
            self.stats.total_rebalancing_executed += 1
        self.stats.last_executed_trade = dict(result or {})
        self.stats.trade_count += 1

        # ==== Listeners (déjà triés par priorité à l'enregistrement) ====
        for l in self.listeners:
            cb = l.get("callback")
            t = l.get("type")
            if not callable(cb):
                continue
            if (t is None) or (t == order_type):
                try:
                    out = cb(result)
                    if asyncio.iscoroutine(out):
                        await out
                except Exception:
                    logging.getLogger("ExecutionEngine").debug("listener error", exc_info=True)

        # ==== Reality-gap learning (offset min_required_bps) ====
        try:
            m = (result.get("meta") or {})
            pk = str(m.get("pair_key") or (result.get("symbol") or "").replace("-", "").upper())
            exp_bps = None
            real_bps = None

            # Attendu : fourni par RM/Scanner si dispo
            if "expected_net_bps" in m:
                exp_bps = float(m.get("expected_net_bps"))

            # Réalisé : direct si fourni, sinon reconstruit (raw - fees - slip)
            if "net_bps" in m:
                real_bps = float(m.get("net_bps"))
            else:
                raw_bps  = float(m.get("raw_bps", 0.0))
                fees_bps = float(m.get("fees_bps", 0.0))
                slip_bps = float(m.get("slip_bps", 0.0))
                real_bps = raw_bps - fees_bps - max(0.0, slip_bps)

            if pk and (exp_bps is not None) and (real_bps is not None):
                self._apply_reality_gap_offset(pk, expected_net_bps=exp_bps, realized_net_bps=real_bps)
        except Exception:
            logging.exception("Unhandled in reality-gap offset")

    except Exception:
        logging.exception("ExecutionEngine._on_filled: error")

# ---------------------------------------------------------------------------
# Attente d'un ACK (utilisée si meta.wait_ack=True)
# ---------------------------------------------------------------------------
async def _wait_ack(self: "ExecutionEngine", client_id: str, *, timeout: float = 2.5) -> bool:
    try:
        deadline = time.time() + float(timeout or 0.0)
        while time.time() < deadline:
            fsm = self._order_fsm.get(client_id)
            if not fsm:
                await asyncio.sleep(0.01)
                continue
            if fsm.state in {"ACK", "PARTIAL", "FILLED"}:
                return True
            if fsm.state in {"REJECTED", "CANCELED", "ERROR"}:
                return False
            await asyncio.sleep(0.01)
    except Exception:
        logging.exception("ExecutionEngine._wait_ack: error")
    return False

# ---------------------------------------------------------------------------
# DRY-RUN: applique (très) simple effet de trade côté balances virtuelles
# - On ne gère que la quote (USDC/EUR/USD/USDT) ici, la base est ignorée.
# ---------------------------------------------------------------------------
async def _apply_fill_to_balances(self: "ExecutionEngine", exchange: str, alias: str, symbol: str, side: str, usdc: float) -> None:
    try:
        ex = self._ex_upper(exchange)
        # FIX: utiliser la classe module-level directement
        _, quote = SymbolUtils.split_base_quote(symbol)
        amt = float(usdc or 0.0)
        if amt <= 0:
            return
        delta = (-amt if str(side).upper() == "BUY" else +amt)
        adj = getattr(self.risk_manager, "adjust_virtual_balance", None)
        if callable(adj):
            adj(ex, str(alias or "TT").upper(), str(quote or "USDC").upper(), float(delta))
    except Exception:
        logging.exception("ExecutionEngine._apply_fill_to_balances: error")

# ---------------------------------------------------------------------------
# Annulation d'un ordre par client_id (route vers la bonne CEX)
# Utilise _order_keys_hint pour retrouver alias/stratégie → clés API.
# ---------------------------------------------------------------------------
async def _cancel_order(self: "ExecutionEngine", exchange: str, symbol: str, client_id: str, *, reason: str = "") -> dict:
    try:
        ex_u = self._ex_upper(exchange)
        # note métrique
        ENGINE_CANCELLATIONS_TOTAL.labels(reason=str(reason or "cancel")).inc()

        alias, strat = None, None
        try:
            _, alias, strat = self._order_keys_hint.get(client_id, (ex_u, None, None))
        except Exception:
            pass

        # FSM local → marquer "CANCELED" si on a un état
        fsm = self._order_fsm.get(client_id)
        if fsm:
            fsm.on_cancel()

        # DRY-RUN → rien à appeler
        if self._is_dry(ex_u):
            evt = {"exchange": ex_u, "symbol": symbol, "client_id": client_id, "status": "cancel_simulated", "reason": reason}
            await self._hist("cancel", evt)
            return evt

        # LIVE: dispatch
        if ex_u == "BINANCE":
            res = await self._binance_cancel(symbol.replace("-", "").upper(), client_id=client_id,
                                             keys=self._pick_keys("Binance", alias, strat))
        elif ex_u == "BYBIT":
            res = await self._bybit_cancel(symbol.replace("-", "").upper(), client_id=client_id,
                                           keys=self._pick_keys("Bybit", alias, strat))
        elif ex_u == "COINBASE":
            # Coinbase AT accepte client_id pour cancel
            res = await self._coinbase_cancel(symbol, client_id=client_id, keys=self._pick_keys("Coinbase", alias, strat))
        else:
            raise ValueError(f"Exchange non supporté pour cancel: {exchange}")

        await self._hist("cancel", {"exchange": ex_u, "symbol": symbol, "client_id": client_id, "status": "cancel_sent", "reason": reason, "resp": res})
        return res
    except Exception as e:
        logging.getLogger("ExecutionEngine").debug("cancel error", exc_info=True)
        raise e

# ---------------------------------------------------------------------------
# Bind dynamique sur la classe ExecutionEngine
# ---------------------------------------------------------------------------
ExecutionEngine._cb_sign = _cb_sign
ExecutionEngine._hist = _hist
ExecutionEngine._reject = _reject
ExecutionEngine._on_filled = _on_filled
ExecutionEngine._wait_ack = _wait_ack
ExecutionEngine._apply_fill_to_balances = _apply_fill_to_balances
ExecutionEngine._cancel_order = _cancel_order
# === FIN MERGED ADDITIONS ====================================================
