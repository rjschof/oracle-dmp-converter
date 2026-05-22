"""Oracle administrative connection helpers and DIRECTORY-object bootstrap."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from oracle_dmp_converter.oracle.conn import create_directory, oracle_connection
from oracle_dmp_converter.runtime.container_oracle import ContainerOracle

LOGGER = logging.getLogger(__name__)

DEFAULT_DUMP_DIRECTORY = "ORACLE_DMC_DUMP"
ORACLE_DMC_DISCOVERY = "ORACLE_DMC_DISCOVERY"
ORACLE_DMC_INSPECT = "ORACLE_DMC_INSPECT"
ORACLE_DMC_CONVERT = "ORACLE_DMC_CONVERT"

DEFAULT_CONTAINER_DUMP_PATH = "/dumps"
DEFAULT_CONTAINER_DISCOVERY_PATH = "/work/discovery"
DEFAULT_CONTAINER_INSPECT_PATH = "/work/inspect"
DEFAULT_CONTAINER_CONVERT_PATH = "/work/convert"


@dataclass(frozen=True)
class OracleAdminConnection:
    """Connection parameters for an Oracle administrative user."""

    host: str
    port: int
    service: str
    user: str
    password: str


def admin_for_container(container: ContainerOracle, password: str) -> OracleAdminConnection:
    """Build an :class:`OracleAdminConnection` for a running container."""
    return OracleAdminConnection(
        host="localhost",
        port=container.mapped_port(),
        service=container.service,
        user="system",
        password=password,
    )


def _connect(admin: OracleAdminConnection):
    return oracle_connection(
        host=admin.host,
        port=admin.port,
        service=admin.service,
        user=admin.user,
        password=admin.password,
    )


def create_dump_directory(admin: OracleAdminConnection) -> None:
    """Create the ORACLE_DMC_DUMP DIRECTORY object inside the container."""
    with _connect(admin) as conn:
        create_directory(conn, DEFAULT_DUMP_DIRECTORY, DEFAULT_CONTAINER_DUMP_PATH)


def create_work_dir_directories(admin: OracleAdminConnection) -> None:
    """Create the discovery / inspect / convert DIRECTORY objects."""
    with _connect(admin) as conn:
        create_directory(conn, ORACLE_DMC_DISCOVERY, DEFAULT_CONTAINER_DISCOVERY_PATH)
        create_directory(conn, ORACLE_DMC_INSPECT, DEFAULT_CONTAINER_INSPECT_PATH)
        create_directory(conn, ORACLE_DMC_CONVERT, DEFAULT_CONTAINER_CONVERT_PATH)
