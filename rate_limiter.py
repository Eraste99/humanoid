from __future__ import annotations
import asyncio
import time
import threading
from typing import Optional, Callable, Awaitable, Any
import asyncio
import threading
import time
from typing import Any, Awaitable, Callable, Optional, Dict, Tuple
from modules.rm_compat import getattr_int, getattr_float, getattr_str, getattr_bool, getattr_dict, getattr_list

class TokenBucket:
    """
    Token bucket hybride (async + sync) pour limiter un débit (ex: POST /order).

    Paramètres :
      - rate_per_s (float)  : jetons générés par seconde   (alias: rate)
      - burst (int)         : capacité maximale (alias: capacity)
      - name (str)          : label pour logs/metrics
      - fair (bool)         : si True, refuse les try_* s'il y a des waiters
      - min_sleep_s / max_sleep_s : bornes de sommeil

    API async :
      - await acquire(n=1, timeout=None)
      - await try_acquire(n=1) -> bool
      - permit(n=1, timeout=None) -> async ctx-mgr
      - rate_limited(n=1, timeout=None) -> décorateur

    API sync :
      - try_consume(n=1) -> bool
      - wait(n=1, timeout=None)
      - permit_sync(n=1, timeout=None) -> ctx-mgr
      - rate_limited_sync(n=1, timeout=None) -> décorateur

    Introspection :
      - next_tokens(), next_available_in(n), get_status()
      - update()/update_sync() pour changer rate/burst à chaud
    """

    def __init__(self, rate_per_s: Optional[float]=None, burst: Optional[int]=None, name: str='', *, capacity: Optional[int]=None, rate: Optional[float]=None, fair: bool=False, min_sleep_s: float=0.005, max_sleep_s: float=0.05):
        if rate_per_s is None and rate is not None:
            rate_per_s = float(rate)
        if burst is None and capacity is not None:
            burst = int(capacity)
        if rate_per_s is None or burst is None:
            raise ValueError('Provide both rate_per_s (or rate) and burst (or capacity).')
        if rate_per_s <= 0:
            raise ValueError('rate_per_s must be > 0')
        self.rate: float = float(rate_per_s)
        self.capacity: int = int(max(1, burst))
        self.tokens: float = float(self.capacity)
        self.ts: float = time.perf_counter()
        self._async_lock = asyncio.Lock()
        self._thread_lock = threading.Lock()
        self.name = name or 'TokenBucket'
        self.fair = bool(fair)
        self.min_sleep_s = float(min_sleep_s)
        self.max_sleep_s = float(max_sleep_s)
        self.granted: int = 0
        self.blocked: int = 0
        self._async_waiters: int = 0
        self._sync_waiters: int = 0

    def _now(self) -> float:
        return time.perf_counter()

    def _refill_unlocked(self, now: Optional[float]=None) -> None:
        now = self._now() if now is None else now
        dt = now - self.ts
        if dt > 0:
            self.tokens = min(self.capacity, self.tokens + dt * self.rate)
            self.ts = now

    async def acquire(self, n: float=1.0, timeout: Optional[float]=None) -> None:
        if n <= 0:
            return
        n = float(n)
        deadline = None if timeout is None else self._now() + float(timeout)
        registered = False
        try:
            while True:
                async with self._async_lock:
                    self._refill_unlocked()
                    if self.tokens >= n:
                        self.tokens -= n
                        self.granted += int(n)
                        return
                    need = n - self.tokens
                    wait = max(self.min_sleep_s, need / self.rate)
                    self.blocked += 1
                    if self.fair and (not registered):
                        self._async_waiters += 1
                        registered = True
                if timeout is not None:
                    remain = deadline - self._now()
                    if remain <= 0 or wait > remain:
                        eta = self.next_available_in(n)
                        raise asyncio.TimeoutError(f'{self.name}.acquire timeout (need={n}, eta~{eta:.3f}s, tokens~{self.next_tokens():.2f})')
                await asyncio.sleep(wait)
        finally:
            if self.fair and registered:
                async with self._async_lock:
                    if self._async_waiters > 0:
                        self._async_waiters -= 1

    async def try_acquire(self, n: float=1.0) -> bool:
        if n <= 0:
            return True
        n = float(n)
        async with self._async_lock:
            if self.fair and (self._async_waiters > 0 or self._sync_waiters > 0):
                return False
            self._refill_unlocked()
            if self.tokens >= n:
                self.tokens -= n
                self.granted += int(n)
                return True
            return False

    async def update(self, rate_per_s: Optional[float]=None, burst: Optional[int]=None) -> None:
        async with self._async_lock:
            self._refill_unlocked()
            if rate_per_s is not None:
                if rate_per_s <= 0:
                    raise ValueError('rate_per_s must be > 0')
                self.rate = float(rate_per_s)
            if burst is not None:
                burst = int(max(1, burst))
                self.capacity = burst
                self.tokens = min(self.tokens, float(self.capacity))

    class _PermitAsync:

        def __init__(self, bucket: 'TokenBucket', n: float, timeout: float | None):
            self._b = bucket
            self._n = float(max(0.0, n))
            self._t = timeout

        async def __aenter__(self) -> 'TokenBucket._PermitAsync':
            if self._n > 0:
                await self._b.acquire(self._n, self._t)
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            return False

    def permit(self, n: float=1.0, timeout: float | None=None) -> 'TokenBucket._PermitAsync':
        return TokenBucket._PermitAsync(self, n, timeout)

    def rate_limited(self, n: float=1.0, timeout: float | None=None) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:

        def deco(fn: Callable[..., Awaitable[Any]]):

            async def wrapper(*args, **kwargs):
                await self.acquire(n, timeout)
                return await fn(*args, **kwargs)
            return wrapper
        return deco

    def try_consume(self, n: float=1.0) -> bool:
        if n <= 0:
            return True
        n = float(n)
        with self._thread_lock:
            if self.fair and (self._async_waiters > 0 or self._sync_waiters > 0):
                return False
            self._refill_unlocked()
            if self.tokens >= n:
                self.tokens -= n
                self.granted += int(n)
                return True
            return False

    def wait(self, n: float=1.0, timeout: Optional[float]=None) -> None:
        if n <= 0:
            return
        n = float(n)
        deadline = None if timeout is None else self._now() + float(timeout)
        registered = False
        try:
            while True:
                with self._thread_lock:
                    self._refill_unlocked()
                    if self.tokens >= n:
                        self.tokens -= n
                        self.granted += int(n)
                        return
                    need = n - self.tokens
                    sleep_s = max(self.min_sleep_s, need / self.rate)
                    self.blocked += 1
                    if self.fair and (not registered):
                        self._sync_waiters += 1
                        registered = True
                if timeout is not None:
                    remain = deadline - self._now()
                    if remain <= 0 or sleep_s > remain:
                        eta = self.next_available_in(n)
                        raise TimeoutError(f'{self.name}.wait timeout (need={n}, eta~{eta:.3f}s, tokens~{self.next_tokens():.2f})')
                time.sleep(min(sleep_s, self.max_sleep_s))
        finally:
            if self.fair and registered:
                with self._thread_lock:
                    if self._sync_waiters > 0:
                        self._sync_waiters -= 1

    def update_sync(self, rate_per_s: Optional[float]=None, burst: Optional[int]=None) -> None:
        with self._thread_lock:
            self._refill_unlocked()
            if rate_per_s is not None:
                if rate_per_s <= 0:
                    raise ValueError('rate_per_s must be > 0')
                self.rate = float(rate_per_s)
            if burst is not None:
                burst = int(max(1, burst))
                self.capacity = burst
                self.tokens = min(self.tokens, float(self.capacity))

    class _PermitSync:

        def __init__(self, bucket: 'TokenBucket', n: float, timeout: float | None):
            self._b = bucket
            self._n = float(max(0.0, n))
            self._t = timeout

        def __enter__(self) -> 'TokenBucket._PermitSync':
            if self._n > 0:
                self._b.wait(self._n, self._t)
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

    def permit_sync(self, n: float=1.0, timeout: float | None=None) -> 'TokenBucket._PermitSync':
        return TokenBucket._PermitSync(self, n, timeout)

    def rate_limited_sync(self, n: float=1.0, timeout: float | None=None) -> Callable[[Callable[..., Any]], Callable[..., Any]]:

        def deco(fn: Callable[..., Any]):

            def wrapper(*args, **kwargs):
                self.wait(n, timeout)
                return fn(*args, **kwargs)
            return wrapper
        return deco

    def next_tokens(self) -> float:
        now = self._now()
        dt = now - self.ts
        return min(self.capacity, self.tokens + max(0.0, dt) * self.rate)

    def next_available_in(self, n: float=1.0) -> float:
        if n <= 0:
            return 0.0
        tokens = self.next_tokens()
        if tokens >= n:
            return 0.0
        need = n - tokens
        return max(0.0, need / self.rate)

    def get_status(self) -> dict:
        est_tokens = self.next_tokens()
        return {'name': self.name, 'rate_per_s': self.rate, 'capacity': self.capacity, 'approx_tokens': round(est_tokens, 3), 'granted': int(self.granted), 'blocked_events': int(self.blocked), 'async_waiters': int(self._async_waiters), 'sync_waiters': int(self._sync_waiters), 'next_available_in_s': round(self.next_available_in(1.0), 4)}

    def __repr__(self) -> str:
        return f'<TokenBucket {self.get_status()}>'

class RateLimiter:
    """
    RL global piloté par cfg.rl.* (zéro os.getenv).
    Crée des buckets par (EXCHANGE, kind) avec hiérarchie d’overrides :

      1) par exchange+kind :   rl.hard_caps_rps_by_exchange_kind[EX][kind]
      2) par exchange seul :   rl.hard_caps_rps_by_exchange[EX]
      3) par kind (global) :   rl.hard_caps_rps_by_kind[kind]
      4) défaut global :       rl.default_rate_per_s

    Idem pour les bursts :
      rl.bursts_by_exchange_kind / rl.bursts_by_exchange / rl.bursts_by_kind / rl.default_burst

    Autres clés (optionnelles) :
      - priorities: ["hedge","cancel","maker", ...]
      - fair: bool (True = protège les waiters)
      - name_prefix: str pour labelliser les buckets
      - min_sleep_s / max_sleep_s : tuning des waits
    """


    def __init__(self, cfg_rl: Any):
        self.cfg = cfg_rl

        # list(): notre getattr_list n'a pas de default custom → fallback avec "or ..."
        self._priorities = getattr_list(self.cfg, 'priorities') or ['hedge', 'cancel', 'maker']

        self._fair = getattr_bool(self.cfg, 'fair', True)
        self._name_prefix = getattr_str(self.cfg, 'name_prefix', 'RL')

        # durées en secondes → float
        self._min_sleep_s = getattr_float(self.cfg, 'min_sleep_s', 0.005)
        self._max_sleep_s = getattr_float(self.cfg, 'max_sleep_s', 0.05)

        # rate/burst
        self._default_rate = getattr_float(self.cfg, 'default_rate_per_s', 9.0)
        self._default_burst = getattr_int(self.cfg, 'default_burst', 10)

        # dicts: notre getattr_dict prend (obj, name) uniquement (default = {})
        self._rate_ex_kind: Dict[str, Dict[str, float]] = getattr_dict(self.cfg, 'hard_caps_rps_by_exchange_kind')
        self._rate_ex: Dict[str, float] = getattr_dict(self.cfg, 'hard_caps_rps_by_exchange')
        self._rate_kind: Dict[str, float] = getattr_dict(self.cfg, 'hard_caps_rps_by_kind')

        self._burst_ex_kind: Dict[str, Dict[str, int]] = getattr_dict(self.cfg, 'bursts_by_exchange_kind')
        self._burst_ex: Dict[str, int] = getattr_dict(self.cfg, 'bursts_by_exchange')
        self._burst_kind: Dict[str, int] = getattr_dict(self.cfg, 'bursts_by_kind')

        self._buckets: Dict[Tuple[str, str], TokenBucket] = {}

    @staticmethod
    def _EX(x: str) -> str:
        return str(x).upper()

    @staticmethod
    def _K(k: str) -> str:
        return str(k).lower()

    def _resolve_limits(self, exchange: str, kind: str) -> Tuple[float, int]:
        ex = self._EX(exchange)
        kd = self._K(kind)
        r = (self._rate_ex_kind.get(ex, {}) or {}).get(kd) or self._rate_ex.get(ex) or self._rate_kind.get(kd) or self._default_rate
        b = int((self._burst_ex_kind.get(ex, {}) or {}).get(kd) or self._burst_ex.get(ex) or self._burst_kind.get(kd) or self._default_burst)
        return (float(r), int(max(1, b)))

    def bucket_for(self, exchange: str, kind: str) -> TokenBucket:
        key = (self._EX(exchange), self._K(kind))
        tb = self._buckets.get(key)
        if tb is None:
            rate, burst = self._resolve_limits(*key)
            name = f'{self._name_prefix}:{key[0]}:{key[1]}'
            tb = TokenBucket(rate_per_s=rate, burst=burst, name=name, fair=self._fair, min_sleep_s=self._min_sleep_s, max_sleep_s=self._max_sleep_s)
            self._buckets[key] = tb
        return tb

    async def acquire(self, exchange: str, kind: str, n: float=1.0, timeout: float | None=None) -> None:
        await self.bucket_for(exchange, kind).acquire(n, timeout)

    async def try_acquire(self, exchange: str, kind: str, n: float=1.0) -> bool:
        return await self.bucket_for(exchange, kind).try_acquire(n)

    def permit(self, exchange: str, kind: str, n: float=1.0, timeout: float | None=None):
        return self.bucket_for(exchange, kind).permit(n, timeout)

    def try_consume(self, exchange: str, kind: str, n: float=1.0) -> bool:
        return self.bucket_for(exchange, kind).try_consume(n)

    def wait(self, exchange: str, kind: str, n: float=1.0, timeout: float | None=None) -> None:
        return self.bucket_for(exchange, kind).wait(n, timeout)

    def permit_sync(self, exchange: str, kind: str, n: float=1.0, timeout: float | None=None):
        return self.bucket_for(exchange, kind).permit_sync(n, timeout)

    async def update_limits(self, exchange: Optional[str], kind: Optional[str], rate_per_s: Optional[float]=None, burst: Optional[int]=None) -> None:
        """
        Met à jour à chaud un seau spécifique. Si exchange/kind est None, met à jour tous.
        """
        if exchange is None or kind is None:
            tasks = []
            for (ex, kd), tb in self._buckets.items():
                r, b = self._resolve_limits(ex, kd)
                if rate_per_s is not None:
                    r = rate_per_s
                if burst is not None:
                    b = burst
                tasks.append(tb.update(r, b))
            await asyncio.gather(*tasks)
            return
        ex = self._EX(exchange)
        kd = self._K(kind)
        tb = self.bucket_for(ex, kd)
        await tb.update(rate_per_s=rate_per_s, burst=burst)

    def get_status(self) -> Dict[Tuple[str, str], dict]:
        return {key: tb.get_status() for key, tb in self._buckets.items()}

    @property
    def priorities(self) -> list[str]:
        return list(self._priorities)