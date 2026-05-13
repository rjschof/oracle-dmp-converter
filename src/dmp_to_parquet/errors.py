"""Project-specific exceptions."""


class DmpToParquetError(Exception):
    """Base exception for converter failures."""


class DockerError(DmpToParquetError):
    """Raised when Docker cannot start or manage Oracle."""


class DataPumpError(DmpToParquetError):
    """Raised when expdp or impdp fails."""


class PlanningError(DmpToParquetError):
    """Raised when a table cannot be planned safely."""
