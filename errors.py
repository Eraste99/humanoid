# contracts/errors.py
# Eccezioni tipizzate per pipeline trading (no fallback impliciti)

class RMError(Exception):
    error_kind = "rm_error"

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


class ExternalServiceError(RMError):
    error_kind = "external_service"


class SimulationError(RMError):
    error_kind = "simulation_error"


class EngineSubmitError(RMError):
    error_kind = "engine_submit_error"


class EngineCancelError(RMError):
    error_kind = "engine_cancel_error"
