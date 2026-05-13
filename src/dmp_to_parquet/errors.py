"""Project-specific exceptions."""


class DmpToParquetError(Exception):
    """Base exception for converter failures."""


class DockerError(DmpToParquetError):
    """Raised when Docker cannot start or manage Oracle."""


class DockerImageError(DockerError):
    """Raised when a Docker image cannot be pulled or found."""


class DockerContainerError(DockerError):
    """Raised when container lifecycle operations fail."""


class DockerPortError(DockerError):
    """Raised when mapped ports cannot be resolved."""


class DockerReadinessError(DockerError):
    """Raised when Oracle does not become ready in time."""


class DockerExecError(DockerError):
    """Raised when executing a command in a container fails."""


class DataPumpError(DmpToParquetError):
    """Raised when expdp or impdp fails."""


class PlanningError(DmpToParquetError):
    """Raised when a table cannot be planned safely."""
