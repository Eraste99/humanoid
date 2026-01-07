from __future__ import annotations
# contracts/errors.py
# Eccezioni tipizzate per pipeline trading (no fallback impliciti)
from typing import Any, Dict

class RMError(Exception):
    error_kind = "rm_error"

    def __init__(self, message: str | None = None, **metadata: Any) -> None:
        super().__init__(message if message is not None else self.error_kind)
        self.metadata: Dict[str, Any] = dict(metadata) if metadata else {}


    @property
    def kind(self) -> str:
        return getattr(self, "error_kind", self.__class__.__name__)


class ConfigError(RMError):
    error_kind = "config_error"


class NotReadyError(RMError):
    error_kind = "not_ready"


class DataStaleError(RMError):
    error_kind = "data_stale"


class InconsistentStateError(RMError):
    error_kind = "inconsistent_state"

class InvariantViolationError(InconsistentStateError):
      """Raised when a safety invariant is violated in RM/Engine."""
      error_kind = "invariant_violation"

class RateLimitTimeoutError(RMError, TimeoutError):
    """Raised when a rate limiter times out waiting for tokens."""
    error_kind = "rate_limit_timeout"

class ExternalServiceError(RMError):
    error_kind = "external_service"


class SimulationError(RMError):
    error_kind = "simulation_error"


class EngineSubmitError(RMError):
    error_kind = "engine_submit_error"

    def __init__(self, reason: str | None = None, **metadata: Any) -> None:
        super().__init__(reason if reason is not None else self.error_kind, **metadata)
        self.reason = reason if reason is not None else self.error_kind
        self.reason_code = self.reason


class EngineCancelError(RMError):
    error_kind = "engine_cancel_error"
