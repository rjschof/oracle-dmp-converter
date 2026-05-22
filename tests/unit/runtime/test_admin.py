"""Unit tests for runtime/admin.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from oracle_dmp_converter.runtime.admin import (
    DEFAULT_CONTAINER_DUMP_PATH,
    DEFAULT_DUMP_DIRECTORY,
    ORACLE_DMC_CONVERT,
    ORACLE_DMC_DISCOVERY,
    ORACLE_DMC_INSPECT,
    OracleAdminConnection,
    admin_for_container,
    create_dump_directory,
    create_work_dir_directories,
)


def _make_mock_container(port: int = 1521) -> MagicMock:
    container = MagicMock()
    container.mapped_port.return_value = port
    container.service = "FREEPDB1"
    return container


class TestOracleAdminConnection:
    def test_fields(self) -> None:
        admin = OracleAdminConnection(
            host="localhost", port=1521, service="FREEPDB1", user="system", password="pw"
        )
        assert admin.host == "localhost"
        assert admin.port == 1521
        assert admin.service == "FREEPDB1"


class TestAdminForContainer:
    def test_builds_admin_from_container(self) -> None:
        container = _make_mock_container(port=15210)
        admin = admin_for_container(container, "secret")
        assert admin.host == "localhost"
        assert admin.port == 15210
        assert admin.service == "FREEPDB1"
        assert admin.user == "system"
        assert admin.password == "secret"


class TestCreateDumpDirectory:
    def test_calls_create_directory(self) -> None:
        admin = OracleAdminConnection("localhost", 1521, "FREE", "system", "pw")
        mock_conn = MagicMock()
        with (
            patch("oracle_dmp_converter.runtime.admin.oracle_connection") as mock_ctx,
            patch("oracle_dmp_converter.runtime.admin.create_directory") as mock_create,
        ):
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            create_dump_directory(admin)
        mock_create.assert_called_once_with(
            mock_conn, DEFAULT_DUMP_DIRECTORY, DEFAULT_CONTAINER_DUMP_PATH
        )


class TestCreateWorkDirDirectories:
    def test_creates_all_three_directories(self) -> None:
        admin = OracleAdminConnection("localhost", 1521, "FREE", "system", "pw")
        mock_conn = MagicMock()
        with (
            patch("oracle_dmp_converter.runtime.admin.oracle_connection") as mock_ctx,
            patch("oracle_dmp_converter.runtime.admin.create_directory") as mock_create,
        ):
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            create_work_dir_directories(admin)
        assert mock_create.call_count == 3
        calls = mock_create.call_args_list
        directories_created = {c[0][1] for c in calls}
        assert ORACLE_DMC_DISCOVERY in directories_created
        assert ORACLE_DMC_INSPECT in directories_created
        assert ORACLE_DMC_CONVERT in directories_created
