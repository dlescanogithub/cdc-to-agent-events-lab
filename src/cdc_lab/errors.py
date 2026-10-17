"""Exceptions used by the pipeline. None of them carry credentials."""


class CdcError(Exception):
    """Base error for this package."""


class TransientError(CdcError):
    """Downstream failure that may succeed on retry."""


class PermanentError(CdcError):
    """Downstream failure that must go straight to the dead-letter queue."""


class CrashInjected(CdcError):
    """Simulated process death at a named crash point. Tests only."""


class NotApproved(CdcError):
    """A draft was sent without a recorded human approval."""


class AgentFailure(CdcError):
    """The stub agent failed. The change event itself stays durable."""


class CassetteMiss(CdcError):
    """No recorded response for this request. The fake provider never calls out."""


class ConfigError(CdcError):
    """Environment or scenario configuration is invalid."""
