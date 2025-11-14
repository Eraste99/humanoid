# retry_policy.py — v3 fusion (compat + riche)
from __future__ import annotations

import asyncio, random, time
from dataclasses import dataclass
from typing import Any, Callable, Optional, Tuple

# ======================== Taxonomie ========================

class ErrKind:
    RETRYABLE  = "RETRYABLE"    # 5xx, timeouts, réseaux transitoires, WS drop/close
    RATELIMIT  = "RATELIMIT"    # 429 / quota / window
    FATAL      = "FATAL"        # 4xx non-retryable, auth, input invalide

@dataclass
class RetryOutcome:
    ok: bool
    result: Any
    attempts: int           # nombre total d'essais effectués
    elapsed_s: float
    kind: Optional[str] = None
    last_exception: Optional[BaseException] = None

# ===================== Classification ======================

def map_error(venue: str, exc: BaseException) -> str:
    """
    Classifie l'erreur -> ErrKind.
    'venue' peut encoder {pod}:{cex}:{family} pour l'observabilité.
    """
    m = (str(exc) or "").lower()

    # timeouts / réseau transitoire
    if any(k in m for k in (
        "timeout", "temporarily unavailable", "connection reset",
        "connection aborted", "unreachable", "try again", "timed out"
    )):
        return ErrKind.RETRYABLE

    # Ratelimit
    if "429" in m or "rate limit" in m or "too many requests" in m:
        return ErrKind.RATELIMIT

    # 5xx / 4xx (heuristique texte, on reste prudent)
    if any(k in m for k in (" 5xx", "internal server error", "service unavailable", "bad gateway")):
        return ErrKind.RETRYABLE
    if any(k in m for k in (" 4xx", "bad request", "forbidden", "unauthorized", "not found")):
        return ErrKind.FATAL

    # WebSocket closures soft
    if any(k in m for k in ("websocket", "ws", "closed", "close frame", "ping timeout", "pong timeout")):
        return ErrKind.RETRYABLE

    # défaut: prudence
    return ErrKind.RETRYABLE

# ====================== Politique v3 =======================

@dataclass
class BackoffPolicy:
    """
    Politique de retry/backoff **riche** avec compat héritée.

    Champs "riches" (v3) — source de vérité:
      - min_backoff_s (float) : backoff minimal
      - max_backoff_s (float) : plafond backoff par tentative
      - budget_tries (int)    : tentatives totales (essai initial inclus)
      - cap_total_s (float)   : plafond de temps cumulé
      - full_jitter (bool)    : True = full-jitter ; False = expo pur
      - on_retry(kind, attempt, sleep_s, elapsed_s) : hook
      - on_giveup(kind, attempt, elapsed_s)         : hook
      - allow_attempt(attempt) -> bool               : breaker optionnel
      - is_idempotent (bool)  : False -> pas de retry sur FATAL/429 pour ops non idempotentes

    Compat héritée (v2) — acceptée par from_cfg():
      - base_ms, max_ms, max_attempts, jitter (0/1)

    Remarques compat:
      * `max_attempts` (hérité) est mappé sur `budget_tries`.
      * `base_ms`/`max_ms` → `min_backoff_s`/`max_backoff_s` (ms → s).
    """
    # v3 (source de vérité)
    min_backoff_s: float = 0.3
    max_backoff_s: float = 30.0
    budget_tries: int = 6
    cap_total_s: float = 120.0
    full_jitter: bool = True
    on_retry: Optional[Callable[[str, int, float, float], None]] = None
    on_giveup: Optional[Callable[[str, int, float], None]] = None
    allow_attempt: Optional[Callable[[int], bool]] = None
    is_idempotent: bool = True

    # ---- Compat: construit depuis un objet cfg (v2 ou v3) ----
    @staticmethod
    def from_cfg(retry_cfg) -> "BackoffPolicy":
        """
        Accepte deux styles:
          - v2: {base_ms, max_ms, max_attempts, jitter}
          - v3: {min_backoff_s, max_backoff_s, budget_tries, cap_total_s, full_jitter, ...}
        Priorité: si un champ v3 est présent, on l'utilise. Sinon, on mappe depuis v2.
        """
        if retry_cfg is None:
            return BackoffPolicy()

        # détecte présence de champs v3
        has_v3 = any(
            hasattr(retry_cfg, k) for k in
            ("min_backoff_s", "max_backoff_s", "budget_tries", "cap_total_s", "full_jitter")
        )

        if has_v3:
            p = BackoffPolicy(
                min_backoff_s = float(getattr(retry_cfg, "min_backoff_s", 0.3)),
                max_backoff_s = float(getattr(retry_cfg, "max_backoff_s", 30.0)),
                budget_tries  = int(getattr(retry_cfg,  "budget_tries", 6)),
                cap_total_s   = float(getattr(retry_cfg, "cap_total_s", 120.0)),
                full_jitter   = bool(getattr(retry_cfg, "full_jitter", True)),
                on_retry      = getattr(retry_cfg, "on_retry", None),
                on_giveup     = getattr(retry_cfg, "on_giveup", None),
                allow_attempt = getattr(retry_cfg, "allow_attempt", None),
                is_idempotent = bool(getattr(retry_cfg, "is_idempotent", True)),
            )
        else:
            # v2 -> v3
            base_ms      = int(getattr(retry_cfg, "base_ms", 500))
            cap_ms       = int(getattr(retry_cfg, "max_ms", 20_000))
            max_attempts = int(getattr(retry_cfg, "max_attempts", 5))
            jitter       = int(getattr(retry_cfg, "jitter", 1))

            p = BackoffPolicy(
                min_backoff_s = max(0.0, base_ms / 1000.0),
                max_backoff_s = max(0.0, cap_ms  / 1000.0),
                budget_tries  = int(max_attempts),     # note: inclus essai initial
                cap_total_s   = float(getattr(retry_cfg, "cap_total_s", 120.0)),
                full_jitter   = bool(jitter),
                is_idempotent = bool(getattr(retry_cfg, "is_idempotent", True)),
            )

        # hooks facultatifs même en v2 (si fournis)
        for k in ("on_retry", "on_giveup", "allow_attempt"):
            if hasattr(retry_cfg, k):
                setattr(p, k, getattr(retry_cfg, k))

        return p

    # utilitaires
    def sleep_for_attempt(self, attempt: int) -> float:
        """
        Calcule la durée d'attente avant la prochaine tentative (attempt>=1 pour le 1er retry).
        Full-jitter si activé, sinon expo pur clampé.
        """
        attempt = max(1, int(attempt))
        exp = min(self.max_backoff_s, self.min_backoff_s * (2 ** (attempt - 1)))
        if self.full_jitter:
            return random.uniform(self.min_backoff_s, exp)
        return exp

# ===================== Helpers communs ======================

def _elapsed_s(since: float) -> float:
    return time.perf_counter() - since

# ===================== Implémentations =====================

def with_retry(
    op: Callable[[], Any],
    venue: str = "",
    policy: Optional[BackoffPolicy] = None,
) -> RetryOutcome:
    """
    Exécute op() avec retry/backoff (sync).
    - Respecte budget de tentatives ET plafond de temps.
    - Signale au pacer via policy.on_retry(kind, attempt, sleep_s, elapsed_s).
    - Idempotence: si is_idempotent=False, on **n'insiste pas** sur FATAL/RATELIMIT.
    """
    p = policy or BackoffPolicy()
    start = time.perf_counter()
    attempt = 0

    while True:
        # breaker optionnel
        if p.allow_attempt and not p.allow_attempt(attempt):
            if p.on_giveup: p.on_giveup("BREAKER", attempt, _elapsed_s(start))
            return RetryOutcome(False, None, attempt, _elapsed_s(start), kind="BREAKER")

        try:
            res = op()
            return RetryOutcome(True, res, attempt + 1, _elapsed_s(start))
        except BaseException as exc:
            kind = map_error(venue, exc)

            # idempotence stricte: on évite de retenter sur certains genres
            if not p.is_idempotent and kind in (ErrKind.FATAL, ErrKind.RATELIMIT):
                if p.on_giveup: p.on_giveup(kind, attempt + 1, _elapsed_s(start))
                return RetryOutcome(False, exc, attempt + 1, _elapsed_s(start), kind=kind, last_exception=exc)

            attempt += 1
            # budgets
            if attempt >= p.budget_tries or _elapsed_s(start) >= p.cap_total_s:
                if p.on_giveup: p.on_giveup(kind, attempt, _elapsed_s(start))
                return RetryOutcome(False, exc, attempt, _elapsed_s(start), kind=kind, last_exception=exc)

            # backoff
            sleep_s = p.sleep_for_attempt(attempt)
            if p.on_retry: p.on_retry(kind, attempt, sleep_s, _elapsed_s(start))
            time.sleep(sleep_s)

async def awith_retry(
    aop: Callable[[], Any],
    venue: str = "",
    policy: Optional[BackoffPolicy] = None,
) -> RetryOutcome:
    """
    Version asynchrone (asyncio) de with_retry.
    Même richesse et mêmes gardes.
    """
    p = policy or BackoffPolicy()
    start = time.perf_counter()
    attempt = 0

    while True:
        if p.allow_attempt and not p.allow_attempt(attempt):
            if p.on_giveup: p.on_giveup("BREAKER", attempt, _elapsed_s(start))
            return RetryOutcome(False, None, attempt, _elapsed_s(start), kind="BREAKER")

        try:
            res = await aop()
            return RetryOutcome(True, res, attempt + 1, _elapsed_s(start))
        except BaseException as exc:
            kind = map_error(venue, exc)

            if not p.is_idempotent and kind in (ErrKind.FATAL, ErrKind.RATELIMIT):
                if p.on_giveup: p.on_giveup(kind, attempt + 1, _elapsed_s(start))
                return RetryOutcome(False, exc, attempt + 1, _elapsed_s(start), kind=kind, last_exception=exc)

            attempt += 1
            if attempt >= p.budget_tries or _elapsed_s(start) >= p.cap_total_s:
                if p.on_giveup: p.on_giveup(kind, attempt, _elapsed_s(start))
                return RetryOutcome(False, exc, attempt, _elapsed_s(start), kind=kind, last_exception=exc)

            sleep_s = p.sleep_for_attempt(attempt)
            if p.on_retry: p.on_retry(kind, attempt, sleep_s, _elapsed_s(start))
            await asyncio.sleep(sleep_s)
