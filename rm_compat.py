# -*- coding: utf-8 -*-
from __future__ import annotations
import asyncio
from typing import Any, Dict, List, Tuple, Optional
import logging, time
_LOG = logging.getLogger('RMCompat')
_USD_ALIAS_WARN_EVERY_S = 60.0
QUOTES_ALLOWED = ('USDC', 'EUR', 'USD')
from typing import Any, Dict


from typing import Any, Dict, List, Optional
import json


from typing import Any, Dict, List

__all__ = [
    "getattr_int", "getattr_float", "getattr_str",
    "getattr_bool", "getattr_dict", "getattr_list",
    "_as_int_or", "_as_float_or","_as_bool_or","_as_str_or",
    "_as_dict_or_empty",
]

def _as_int_or(self, x: Any, default: int) -> int:  # noqa: D401
    return _as_int_or(x, default)


def _as_float_or(self, x: Any, default: float) -> float:
    return _as_float_or(x, default)


def _as_bool_or(self, x: Any, default: bool) -> bool:
    return _as_bool_or(x, default)


def _as_str_or(self, x: Any, default: str) -> str:
    return _as_str_or(x, default)


def _as_dict_or_empty(self, x: Any) -> Dict[str, Any]:
    return _as_dict_or_empty(x)


def _as_list_or_empty(self, x: Any) -> List[Any]:
    return _as_list_or_empty(x)

def getattr_int(obj: Any, name: str, default: int) -> int:
    v = getattr(obj, name, default)
    return int(default) if v is None else int(v)

def getattr_float(obj: Any, name: str, default: float) -> float:
    v = getattr(obj, name, default)
    return float(default) if v is None else float(v)

def getattr_str(obj: Any, name: str, default: str) -> str:
    v = getattr(obj, name, default)
    return str(default) if v is None else str(v)

def getattr_bool(obj: Any, name: str, default: bool) -> bool:
    v = getattr(obj, name, default)
    return bool(default) if v is None else bool(v)

def getattr_dict(obj: Any, name: str) -> Dict[str, Any]:
    # même logique que dict(getattr(..., {})) avec tolérance None
    v = getattr(obj, name, {})
    v = {} if v is None else v
    return dict(v)

def getattr_list(obj: Any, name: str) -> List[Any]:
    # équivalent list(getattr(..., [])) avec tolérance None
    v = getattr(obj, name, [])
    v = [] if v is None else v
    return list(v)



def _pair_quote(pk: str) -> str:
    s = (pk or '').replace('-', '').upper()
    for q in QUOTES_ALLOWED:
        if s.endswith(q):
            return q
    return 'USDC'

class RMCompat:
    """
    À mixer dans ton Engine: fournit ready_event, submit(...) multi-formats,
    normalisation notional-quote -> qty base, requalif 'bridge' => rebalancing,
    et queue_position(...).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ready_event: asyncio.Event = getattr(self, 'ready_event', None) or asyncio.Event()
        self._get_top_of_book = None
        self._get_orderbook_depth = None
        self._usd_alias_warn_state: Dict[str, float] = {}

    def connect_marketdata(self, get_top_of_book, get_orderbook_depth):
        self._get_top_of_book = get_top_of_book
        self._get_orderbook_depth = get_orderbook_depth

    def _warn_usd_alias(self, key: str) -> None:
        """Warn contrôlé pour usages *_usd (fenêtre 60s)."""
        try:
            now = time.time()
            last = self._usd_alias_warn_state.get(key, 0.0)
            if now - last >= _USD_ALIAS_WARN_EVERY_S:
                self._usd_alias_warn_state[key] = now
                _LOG.warning("[RMCompat] '%s' est déprécié — utilisez '*_quote' (quote-agnostic).", key)
        except Exception:
            pass

    def _top(self, ex: str, pair: str) -> Tuple[float, float]:
        if callable(self._get_top_of_book):
            try:
                return self._get_top_of_book(ex, pair)
            except Exception:
                import logging
                logging.exception('Unhandled exception')
        return (0.0, 0.0)

    def _depth(self, ex: str, pair: str):
        if callable(self._get_orderbook_depth):
            try:
                return self._get_orderbook_depth(ex, pair)
            except Exception:
                logging.exception('Unhandled exception')
        return ([], [])

    async def submit(self, payload: Dict[str, Any]):
        t = str(payload.get('type', '')).lower()
        if t == 'rebalancing_trade':
            cross = payload.get('cross') or payload
            bundle = self._cross_to_bundle(cross)
            return await self._submit_bundle(bundle)
        if t == 'arbitrage_bundle':
            return await self._submit_bundle(payload)
        if t == 'single':
            meta = payload.setdefault('meta', {})
            if meta.get('type') == 'bridge':
                meta['type'] = 'rebalancing'
                meta.setdefault('strategy', 'TT')
                meta.setdefault('tif_override', 'IOC')
            leg = self._normalize_single(payload)
            return await self._submit_single_normalized(leg)
        if hasattr(super(), 'submit'):
            return await super().submit(payload)
        raise RuntimeError('Engine.submit: type unsupported')

    def _cross_to_bundle(self, cross: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compat rebalancing: supporte amount_usdc -> volume_quote + warning.
        """
        x = cross or {}
        pair = (x.get('pair_key') or x.get('pair') or '').replace('-', '').upper()
        quote = _pair_quote(pair)
        amt_q = None
        if 'amount_quote' in x and x['amount_quote'] is not None:
            amt_q = float(x['amount_quote'])
        elif 'amount_usdc' in x and x['amount_usdc'] is not None:
            self._warn_usd_alias('amount_usdc')
            amt_q = float(x['amount_usdc'])
        else:
            amt_q = 0.0
        return {'type': 'arbitrage_bundle', 'pair': pair, 'strategy': 'TM', 'legs': [{'exchange': x['to_exchange'], 'alias': x.get('buy_alias') or 'TT', 'side': 'BUY', 'symbol': pair, 'volume_quote': amt_q, 'quote': quote}, {'exchange': x['from_exchange'], 'alias': x.get('sell_alias') or 'TT', 'side': 'SELL', 'symbol': pair, 'volume_quote': amt_q, 'quote': quote}], 'meta': {'type': 'rebalancing', 'allow_loss_bps': x.get('allow_loss_bps', 0.0)}}

    def _normalize_single(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Entrée: {"type":"single","exchange","symbol","side","price"?,"qty"?, "volume_quote|volume_usdc|volume"?}
        Sortie: même ordre, avec qty/base et price garantis (notional->qty si besoin).
        """
        ex = order['exchange']
        sym = (order['symbol'] or '').replace('-', '').upper()
        side = str(order['side']).upper()
        price = float(order.get('price') or 0.0)
        if price <= 0.0:
            bid, ask = self._top(ex, sym)
            price = ask if side == 'BUY' else bid
            order['price'] = price
        qty = order.get('qty')
        if qty is None:
            notional = None
            if 'volume_quote' in order and order['volume_quote'] is not None:
                notional = float(order['volume_quote'])
            elif 'volume_usdc' in order and order['volume_usdc'] is not None:
                self._warn_usd_alias('volume_usdc')
                notional = float(order['volume_usdc'])
            elif 'volume' in order and order['volume'] is not None:
                notional = float(order['volume'])
            if notional is not None and price > 0:
                qty = max(0.0, notional / price)
                order['qty'] = qty
        order.setdefault('alias', 'TT')
        return order

    async def _submit_bundle(self, bundle: Dict[str, Any]):
        """
        Normalise chaque jambe (notional->qty) puis délègue aux adapters.
        """
        pair = (bundle.get('pair') or '').replace('-', '').upper()
        for leg in bundle.get('legs') or []:
            leg.setdefault('symbol', pair)
            self._normalize_leg_notional(leg)
        if hasattr(self, '_execute_bundle'):
            return await self._execute_bundle(bundle)
        if hasattr(self, 'execute_bundle'):
            return await self.execute_bundle(bundle)
        res = []
        for leg in bundle.get('legs', []):
            leg.setdefault('meta', {}).setdefault('tif_override', 'IOC')
            res.append(await self._submit_single_normalized(leg))
        return res

    def _normalize_leg_notional(self, leg: Dict[str, Any]) -> None:
        ex = leg['exchange']
        sym = (leg['symbol'] or '').replace('-', '').upper()
        side = str(leg['side']).upper()
        price = float(leg.get('price') or 0.0)
        if price <= 0.0:
            bid, ask = self._top(ex, sym)
            price = ask if side == 'BUY' else bid
            leg['price'] = price
        if leg.get('qty') is None:
            notional = None
            if 'volume_quote' in leg and leg['volume_quote'] is not None:
                notional = float(leg['volume_quote'])
            elif 'volume_usdc' in leg and leg['volume_usdc'] is not None:
                self._warn_usd_alias('volume_usdc')
                notional = float(leg['volume_usdc'])
            elif 'volume' in leg and leg['volume'] is not None:
                notional = float(leg['volume'])
            if notional is not None and price > 0:
                leg['qty'] = max(0.0, notional / price)
        leg.setdefault('alias', 'TT')
        leg.setdefault('quote', _pair_quote(sym))

    async def _submit_single_normalized(self, leg: Dict[str, Any]):
        """
        Délègue au chemin ‘single’ de l’engine une fois qty/price prêts.
        Ton implémentation existante peut s’appeler _execute_single / execute_single / submit_single.
        """
        if hasattr(self, '_execute_single'):
            return await self._execute_single(leg)
        if hasattr(self, 'execute_single'):
            return await self.execute_single(leg)
        if hasattr(self, 'submit_single'):
            return await self.submit_single(leg)
        if hasattr(super(), 'submit'):
            return await super().submit({'type': 'single', **leg})
        raise RuntimeError('Engine.single path missing')

    def queue_position(self, *, exchange: str, symbol: str, side: str, price: float) -> Dict[str, float]:
        """
        Estime la quantité ‘devant’ à ton prix (approximation book-level).
        """
        asks, bids = self._depth(exchange, symbol.replace('-', '').upper())
        levels = asks if side.upper() == 'BUY' else bids
        qty_ahead = 0.0
        for p, q in levels:
            p = float(p)
            q = float(q)
            if side.upper() == 'BUY':
                if p < price:
                    break
                if abs(p - price) <= 1e-12 or p > price:
                    qty_ahead += q
            else:
                if p > price:
                    break
                if abs(p - price) <= 1e-12 or p < price:
                    qty_ahead += q
        return {'qty_ahead': float(qty_ahead), 'levels_scanned': float(len(levels))}

    def apply_config_compat_aliases(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Renomme proprement les clés *_usd en *_quote (avec warning unique par clé).
        Usage: self.apply_config_compat_aliases(self.config)
        """
        if not isinstance(cfg, dict):
            return cfg
        out = {}
        for k, v in cfg.items():
            if isinstance(k, str) and k.endswith('_usd'):
                nk = k[:-4] + '_quote'
                self._warn_usd_alias(k)
                out[nk] = v
            else:
                out[k] = v
        return out