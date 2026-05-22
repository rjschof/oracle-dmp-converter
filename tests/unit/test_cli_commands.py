"""Unit tests for cli/commands.py (mocked converter, no Docker required)."""
# pylint: disable=protected-access

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from oracle_dmp_converter.cli import main
from oracle_dmp_converter.cli.commands import _resolve_dump_paths
from oracle_dmp_converter.models import (
    ChunkPlan,
    ColumnMetadata,
    ConversionPlan,
    DumpFormat,
    DumpManifest,
    TableMetadata,
    TablePlan,
    TableStrategy,
)
from oracle_dmp_converter.persistence.serialization import load_plan, save_manifest, save_plan

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _manifest(tmp_path: Path, dump_format: DumpFormat = DumpFormat.DATAPUMP) -> DumpManifest:
    return DumpManifest(
        dump_paths=(str(tmp_path / "test.dmp"),),
        oracle_image="gvenzl/oracle-free:23-faststart",
        container_runtime="docker",
        dump_format=dump_format,
        tables=(
            TableMetadata(
                schema="MYSCHEMA",
                name="ORDERS",
                columns=(ColumnMetadata("ID", "NUMBER", 1, nullable=False),),
            ),
        ),
    )


def _plan(tmp_path: Path, dump_format: DumpFormat = DumpFormat.DATAPUMP) -> ConversionPlan:
    return ConversionPlan(
        dump_paths=(str(tmp_path / "test.dmp"),),
        tables=(
            TablePlan(
                schema="MYSCHEMA",
                table="ORDERS",
                strategy=TableStrategy.WHOLE_TABLE,
                chunks=(ChunkPlan(name="whole", strategy=TableStrategy.WHOLE_TABLE),),
            ),
        ),
        oracle_image="gvenzl/oracle-free:23-faststart",
        container_runtime="docker",
        dump_format=dump_format,
    )


def _write_dump(tmp_path: Path) -> Path:
    p = tmp_path / "test.dmp"
    p.write_bytes(b"\x00")
    return p


# ---------------------------------------------------------------------------
# doctor
# ---------------------------------------------------------------------------


class TestDoctorCommand:
    def test_succeeds_when_docker_available(self) -> None:
        with patch("oracle_dmp_converter.cli.commands.docker_available", return_value=True):
            result = CliRunner().invoke(main, ["doctor"])
        assert result.exit_code == 0

    def test_fails_when_docker_unavailable(self) -> None:
        with patch("oracle_dmp_converter.cli.commands.docker_available", return_value=False):
            result = CliRunner().invoke(main, ["doctor"])
        assert result.exit_code != 0
        assert "not available" in result.output.lower()

    def test_accepts_podman_runtime(self) -> None:
        with patch("oracle_dmp_converter.cli.commands.docker_available", return_value=True):
            result = CliRunner().invoke(main, ["doctor", "--container-runtime", "podman"])
        assert result.exit_code == 0

    def test_warns_when_podman_socket_missing(self) -> None:
        with (
            patch("oracle_dmp_converter.cli.commands.docker_available", return_value=True),
            patch("oracle_dmp_converter.cli.commands._podman_socket_url", return_value=None),
            patch("oracle_dmp_converter.cli.commands._selinux_enforcing", return_value=False),
        ):
            result = CliRunner().invoke(main, ["doctor", "--container-runtime", "podman"])
        assert result.exit_code == 0
        assert "podman.socket" in result.output

    def test_no_socket_warning_for_docker_runtime(self) -> None:
        with (
            patch("oracle_dmp_converter.cli.commands.docker_available", return_value=True),
            patch("oracle_dmp_converter.cli.commands._selinux_enforcing", return_value=False),
        ):
            result = CliRunner().invoke(main, ["doctor", "--container-runtime", "docker"])
        assert result.exit_code == 0
        assert "podman.socket" not in result.output

    def test_prints_selinux_info_when_enforcing(self) -> None:
        with (
            patch("oracle_dmp_converter.cli.commands.docker_available", return_value=True),
            patch("oracle_dmp_converter.cli.commands._podman_socket_url", return_value=None),
            patch("oracle_dmp_converter.cli.commands._selinux_enforcing", return_value=True),
        ):
            result = CliRunner().invoke(main, ["doctor", "--container-runtime", "podman"])
        assert result.exit_code == 0
        assert "SELinux" in result.output


# ---------------------------------------------------------------------------
# inspect
# ---------------------------------------------------------------------------


class TestInspectCommand:
    def test_inspect_calls_converter_inspect(self, tmp_path: Path) -> None:
        dump = _write_dump(tmp_path)
        mock_converter = MagicMock()
        mock_converter.__enter__ = MagicMock(return_value=mock_converter)
        mock_converter.__exit__ = MagicMock(return_value=False)
        mock_converter.inspect.return_value = None

        with (
            patch(
                "oracle_dmp_converter.cli.commands.OracleDMPConverter",
                return_value=mock_converter,
            ),
            patch("oracle_dmp_converter.cli.commands.cleanup_stale_session"),
            patch(
                "oracle_dmp_converter.cli.commands.session_path_for",
                return_value=tmp_path / "session.json",
            ),
        ):
            result = CliRunner().invoke(
                main,
                [
                    "inspect",
                    "--dump",
                    str(dump),
                    "--work-dir",
                    str(tmp_path / "work"),
                    "--oracle-password",
                    "pw",
                ],
            )

        assert result.exit_code == 0, result.output
        mock_converter.inspect.assert_called_once()

    def test_inspect_cleans_stale_session_when_present(self, tmp_path: Path) -> None:
        dump = _write_dump(tmp_path)
        session_path = tmp_path / "work" / "session.json"
        session_path.parent.mkdir(parents=True, exist_ok=True)
        session_path.write_text("{}")

        mock_converter = MagicMock()
        mock_converter.__enter__ = MagicMock(return_value=mock_converter)
        mock_converter.__exit__ = MagicMock(return_value=False)

        with (
            patch(
                "oracle_dmp_converter.cli.commands.OracleDMPConverter",
                return_value=mock_converter,
            ),
            patch("oracle_dmp_converter.cli.commands.cleanup_stale_session") as mock_cleanup,
        ):
            CliRunner().invoke(
                main,
                [
                    "inspect",
                    "--dump",
                    str(dump),
                    "--work-dir",
                    str(tmp_path / "work"),
                    "--oracle-password",
                    "pw",
                ],
            )

        mock_cleanup.assert_called_once()

    def test_inspect_wraps_converter_value_error(self, tmp_path: Path) -> None:
        """ValueError raised by OracleDMPConverter.inspect() becomes a ClickException."""
        dump = _write_dump(tmp_path)
        mock_converter = MagicMock()
        mock_converter.__enter__ = MagicMock(return_value=mock_converter)
        mock_converter.__exit__ = MagicMock(return_value=False)
        mock_converter.inspect.side_effect = ValueError("bad dump file")

        with (
            patch(
                "oracle_dmp_converter.cli.commands.OracleDMPConverter",
                return_value=mock_converter,
            ),
            patch(
                "oracle_dmp_converter.cli.commands.session_path_for",
                return_value=tmp_path / "session.json",
            ),
        ):
            result = CliRunner().invoke(
                main,
                [
                    "inspect",
                    "--dump",
                    str(dump),
                    "--work-dir",
                    str(tmp_path / "work"),
                    "--oracle-password",
                    "pw",
                ],
            )

        assert result.exit_code != 0
        assert "bad dump file" in result.output


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


class TestPlanCommand:
    def test_plan_writes_plan_yaml(self, tmp_path: Path) -> None:
        _write_dump(tmp_path)
        work_dir = tmp_path / "work"
        work_dir.mkdir()
        manifest = _manifest(tmp_path)
        manifest_path = work_dir / "manifest.json"
        save_manifest(manifest_path, manifest)

        result = CliRunner().invoke(
            main,
            ["plan", "--manifest", str(manifest_path)],
        )

        assert result.exit_code == 0, result.output
        assert (work_dir / "plan.yaml").exists()

    def test_plan_raises_on_image_mismatch(self, tmp_path: Path) -> None:
        work_dir = tmp_path / "work"
        work_dir.mkdir()
        manifest = _manifest(tmp_path)
        manifest_path = work_dir / "manifest.json"
        save_manifest(manifest_path, manifest)

        config_path = tmp_path / "config.yaml"
        config_path.write_text("oracle:\n  image: different-image:latest\n")

        result = CliRunner().invoke(
            main,
            [
                "plan",
                "--manifest",
                str(manifest_path),
                "--config",
                str(config_path),
            ],
        )

        assert result.exit_code != 0
        assert "mismatch" in result.output.lower()

    def test_plan_oracle_image_flag_overrides_manifest(self, tmp_path: Path) -> None:
        """--oracle-image passed to plan sets effective_image (line 196)."""
        _write_dump(tmp_path)
        work_dir = tmp_path / "work"
        work_dir.mkdir()
        manifest = _manifest(tmp_path)
        manifest_path = work_dir / "manifest.json"
        save_manifest(manifest_path, manifest)

        result = CliRunner().invoke(
            main,
            ["plan", "--manifest", str(manifest_path), "--oracle-image", "custom-image:latest"],
        )

        assert result.exit_code == 0, result.output
        saved = load_plan(work_dir / "plan.yaml")
        assert saved.oracle_image == "custom-image:latest"


# ---------------------------------------------------------------------------
# convert
# ---------------------------------------------------------------------------


class TestConvertCommand:
    def test_convert_oneshot_calls_converter_run(self, tmp_path: Path) -> None:
        dump = _write_dump(tmp_path)
        mock_converter = MagicMock()
        mock_converter.__enter__ = MagicMock(return_value=mock_converter)
        mock_converter.__exit__ = MagicMock(return_value=False)

        with (
            patch(
                "oracle_dmp_converter.cli.commands.OracleDMPConverter",
                return_value=mock_converter,
            ),
            patch(
                "oracle_dmp_converter.cli.commands.session_path_for",
                return_value=tmp_path / "work" / "session.json",
            ),
        ):
            result = CliRunner().invoke(
                main,
                [
                    "convert",
                    "--dump",
                    str(dump),
                    "--output",
                    str(tmp_path / "out"),
                    "--work-dir",
                    str(tmp_path / "work"),
                    "--oracle-password",
                    "pw",
                ],
            )

        assert result.exit_code == 0, result.output
        mock_converter.run.assert_called_once()

    def test_convert_with_plan_calls_converter_convert(self, tmp_path: Path) -> None:
        dump = _write_dump(tmp_path)
        work_dir = tmp_path / "work"
        work_dir.mkdir()
        plan = _plan(tmp_path)
        plan_path = work_dir / "plan.yaml"
        save_plan(plan_path, plan)

        # Create a session file so convert knows inspect has run
        session_path = work_dir / "session.json"
        session_path.write_text(
            json.dumps(
                {
                    "container_name": "oracle-abc",
                    "container_runtime": "docker",
                    "oracle_image": "gvenzl/oracle-free:23-faststart",
                    "oracle_service": "FREEPDB1",
                    "work_dir": str(work_dir),
                    "dump_dir": str(tmp_path),
                    "created_at": "2024-01-01T12:00:00+00:00",
                }
            )
        )

        mock_converter = MagicMock()
        mock_converter.__enter__ = MagicMock(return_value=mock_converter)
        mock_converter.__exit__ = MagicMock(return_value=False)

        with patch(
            "oracle_dmp_converter.cli.commands.OracleDMPConverter",
            return_value=mock_converter,
        ):
            result = CliRunner().invoke(
                main,
                [
                    "convert",
                    "--plan",
                    str(plan_path),
                    "--dump",
                    str(dump),
                    "--output",
                    str(tmp_path / "out"),
                    "--work-dir",
                    str(work_dir),
                    "--oracle-password",
                    "pw",
                ],
            )

        assert result.exit_code == 0, result.output
        mock_converter.convert.assert_called_once()

    def test_convert_fails_without_session_when_plan_given(self, tmp_path: Path) -> None:
        dump = _write_dump(tmp_path)
        work_dir = tmp_path / "work"
        work_dir.mkdir()
        plan = _plan(tmp_path)
        plan_path = work_dir / "plan.yaml"
        save_plan(plan_path, plan)
        # No session.json

        result = CliRunner().invoke(
            main,
            [
                "convert",
                "--plan",
                str(plan_path),
                "--dump",
                str(dump),
                "--output",
                str(tmp_path / "out"),
                "--work-dir",
                str(work_dir),
                "--oracle-password",
                "pw",
            ],
        )

        assert result.exit_code != 0

    def test_convert_accepts_format_avro(self, tmp_path: Path) -> None:
        dump = _write_dump(tmp_path)
        mock_converter = MagicMock()
        mock_converter.__enter__ = MagicMock(return_value=mock_converter)
        mock_converter.__exit__ = MagicMock(return_value=False)

        with (
            patch(
                "oracle_dmp_converter.cli.commands.OracleDMPConverter",
                return_value=mock_converter,
            ),
            patch(
                "oracle_dmp_converter.cli.commands.session_path_for",
                return_value=tmp_path / "work" / "session.json",
            ),
        ):
            result = CliRunner().invoke(
                main,
                [
                    "convert",
                    "--dump",
                    str(dump),
                    "--output",
                    str(tmp_path / "out"),
                    "--work-dir",
                    str(tmp_path / "work"),
                    "--oracle-password",
                    "pw",
                    "--format",
                    "avro",
                ],
            )

        assert result.exit_code == 0, result.output

    def test_convert_defaults_work_and_output_dirs(self, tmp_path: Path) -> None:
        """convert with no --work-dir and no --output uses default dirs (lines 275, 277)."""
        dump = _write_dump(tmp_path)
        mock_converter = MagicMock()
        mock_converter.__enter__ = MagicMock(return_value=mock_converter)
        mock_converter.__exit__ = MagicMock(return_value=False)

        with (
            patch(
                "oracle_dmp_converter.cli.commands.OracleDMPConverter",
                return_value=mock_converter,
            ),
            patch(
                "oracle_dmp_converter.cli.commands.session_path_for",
                return_value=tmp_path / "work2" / "session.json",
            ),
        ):
            result = CliRunner().invoke(
                main,
                ["convert", "--dump", str(dump), "--oracle-password", "pw"],
            )

        assert result.exit_code == 0, result.output
        mock_converter.run.assert_called_once()

    def test_convert_uses_config_oracle_image(self, tmp_path: Path) -> None:
        """convert without --oracle-image uses config.oracle_image when set (line 291)."""
        dump = _write_dump(tmp_path)
        config_path = tmp_path / "config.yaml"
        config_path.write_text("oracle:\n  image: config-image:latest\n")

        mock_converter = MagicMock()
        mock_converter.__enter__ = MagicMock(return_value=mock_converter)
        mock_converter.__exit__ = MagicMock(return_value=False)

        with (
            patch(
                "oracle_dmp_converter.cli.commands.OracleDMPConverter",
                return_value=mock_converter,
            ) as mock_cls,
            patch(
                "oracle_dmp_converter.cli.commands.session_path_for",
                return_value=tmp_path / "session.json",
            ),
        ):
            result = CliRunner().invoke(
                main,
                [
                    "convert",
                    "--dump",
                    str(dump),
                    "--config",
                    str(config_path),
                    "--work-dir",
                    str(tmp_path / "work"),
                    "--oracle-password",
                    "pw",
                ],
            )

        assert result.exit_code == 0, result.output
        settings = mock_cls.call_args[0][0]
        assert settings.oracle_image == "config-image:latest"

    def test_convert_wraps_converter_value_error(self, tmp_path: Path) -> None:
        """ValueError raised by OracleDMPConverter.run() becomes a ClickException."""
        dump = _write_dump(tmp_path)
        mock_converter = MagicMock()
        mock_converter.__enter__ = MagicMock(return_value=mock_converter)
        mock_converter.__exit__ = MagicMock(return_value=False)
        mock_converter.run.side_effect = ValueError("conversion failed")

        with (
            patch(
                "oracle_dmp_converter.cli.commands.OracleDMPConverter",
                return_value=mock_converter,
            ),
            patch(
                "oracle_dmp_converter.cli.commands.session_path_for",
                return_value=tmp_path / "session.json",
            ),
        ):
            result = CliRunner().invoke(
                main,
                [
                    "convert",
                    "--dump",
                    str(dump),
                    "--work-dir",
                    str(tmp_path / "work"),
                    "--oracle-password",
                    "pw",
                ],
            )

        assert result.exit_code != 0
        assert "conversion failed" in result.output


# ---------------------------------------------------------------------------
# _resolve_dump_paths
# ---------------------------------------------------------------------------


class TestResolveDumpPaths:
    def test_cli_paths_take_precedence(self, tmp_path: Path) -> None:
        dump = tmp_path / "cli.dmp"
        dump.write_bytes(b"\x00")
        result = _resolve_dump_paths((dump,), ("/plan/dump.dmp",))
        assert str(dump.resolve()) in [str(p) for p in result]

    def test_falls_back_to_plan_paths_when_no_cli_paths(self) -> None:
        result = _resolve_dump_paths((), ("/dumps/test.dmp",))
        assert len(result) == 1
        assert result[0].name == "test.dmp"


# ---------------------------------------------------------------------------
# _build_settings — ValueError wrapping
# ---------------------------------------------------------------------------


class TestBuildSettings:
    def test_mixed_dump_dirs_raises_click_exception(self, tmp_path: Path) -> None:
        """Dump files from different directories cause _build_settings to raise ClickException."""
        d1 = tmp_path / "dir1" / "a.dmp"
        d2 = tmp_path / "dir2" / "b.dmp"
        d1.parent.mkdir(parents=True)
        d2.parent.mkdir(parents=True)
        d1.write_bytes(b"\x00")
        d2.write_bytes(b"\x00")

        result = CliRunner().invoke(
            main,
            [
                "inspect",
                "--dump",
                str(d1),
                "--dump",
                str(d2),
                "--work-dir",
                str(tmp_path / "work"),
                "--oracle-password",
                "pw",
            ],
        )

        assert result.exit_code != 0
        assert "single parent directory" in result.output
