from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from oracle_dmp_converter import cli


class _StubOracleDMPConverter:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def doctor(self, container_runtime: str) -> None:
        self.calls.append(("doctor", {"container_runtime": container_runtime}))

    def inspect(
        self,
        *,
        dump_paths: tuple[Path, ...],
        work_dir: Path,
        manifest_path: Path | None,
        oracle_image: str,
        oracle_password: str,
        container_runtime: str,
    ) -> None:
        self.calls.append(
            (
                "inspect",
                {
                    "dump_paths": dump_paths,
                    "work_dir": work_dir,
                    "manifest_path": manifest_path,
                    "oracle_image": oracle_image,
                    "oracle_password": oracle_password,
                    "container_runtime": container_runtime,
                },
            )
        )

    def plan(
        self,
        *,
        manifest_path: Path,
        config_path: Path | None,
        oracle_image: str | None,
        plan_path: Path | None,
    ) -> None:
        self.calls.append(
            (
                "plan",
                {
                    "manifest_path": manifest_path,
                    "config_path": config_path,
                    "oracle_image": oracle_image,
                    "plan_path": plan_path,
                },
            )
        )

    def convert(
        self,
        *,
        plan_path: Path | None,
        dump_paths: tuple[Path, ...],
        config_path: Path | None,
        output_dir: Path,
        output_format: str,
        work_dir: Path | None,
        oracle_image: str,
        oracle_password: str,
        container_runtime: str | None,
        keep_alive: bool,
    ) -> None:
        self.calls.append(
            (
                "convert",
                {
                    "plan_path": plan_path,
                    "dump_paths": dump_paths,
                    "config_path": config_path,
                    "output_dir": output_dir,
                    "output_format": output_format,
                    "work_dir": work_dir,
                    "oracle_image": oracle_image,
                    "oracle_password": oracle_password,
                    "container_runtime": container_runtime,
                    "keep_alive": keep_alive,
                },
            )
        )


def test_doctor_command_proxies_to_oracle_dmp_converter(monkeypatch) -> None:
    stub = _StubOracleDMPConverter()
    monkeypatch.setattr(cli, "_CLI_CONVERTER", stub)

    result = CliRunner().invoke(cli.main, ["doctor", "--container-runtime", "podman"])

    assert result.exit_code == 0, result.output
    assert stub.calls == [("doctor", {"container_runtime": "podman"})]


def test_inspect_command_proxies_to_oracle_dmp_converter(monkeypatch, tmp_path: Path) -> None:
    stub = _StubOracleDMPConverter()
    monkeypatch.setattr(cli, "_CLI_CONVERTER", stub)
    dump = tmp_path / "sample.dmp"
    dump.write_text("placeholder")

    result = CliRunner().invoke(
        cli.main,
        [
            "inspect",
            "--dump",
            str(dump),
            "--work-dir",
            str(tmp_path / "work"),
            "--output",
            str(tmp_path / "manifest.json"),
            "--oracle-image",
            "custom-image",
            "--oracle-password",
            "secret",
            "--container-runtime",
            "podman",
        ],
    )

    assert result.exit_code == 0, result.output
    assert stub.calls == [
        (
            "inspect",
            {
                "dump_paths": (dump,),
                "work_dir": tmp_path / "work",
                "manifest_path": tmp_path / "manifest.json",
                "oracle_image": "custom-image",
                "oracle_password": "secret",
                "container_runtime": "podman",
            },
        )
    ]


def test_plan_command_proxies_to_oracle_dmp_converter(monkeypatch, tmp_path: Path) -> None:
    stub = _StubOracleDMPConverter()
    monkeypatch.setattr(cli, "_CLI_CONVERTER", stub)
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")

    result = CliRunner().invoke(
        cli.main,
        [
            "plan",
            "--manifest",
            str(manifest),
            "--output",
            str(tmp_path / "plan.yaml"),
            "--oracle-image",
            "custom-image",
        ],
    )

    assert result.exit_code == 0, result.output
    assert stub.calls == [
        (
            "plan",
            {
                "manifest_path": manifest,
                "config_path": None,
                "oracle_image": "custom-image",
                "plan_path": tmp_path / "plan.yaml",
            },
        )
    ]


def test_convert_command_proxies_to_oracle_dmp_converter(monkeypatch, tmp_path: Path) -> None:
    stub = _StubOracleDMPConverter()
    monkeypatch.setattr(cli, "_CLI_CONVERTER", stub)
    dump = tmp_path / "sample.dmp"
    dump.write_text("placeholder")

    result = CliRunner().invoke(
        cli.main,
        [
            "convert",
            "--dump",
            str(dump),
            "--output",
            str(tmp_path / "out"),
            "--format",
            "csv",
            "--work-dir",
            str(tmp_path / "work"),
            "--oracle-image",
            "custom-image",
            "--oracle-password",
            "secret",
            "--container-runtime",
            "podman",
            "--keep-alive",
        ],
    )

    assert result.exit_code == 0, result.output
    assert stub.calls == [
        (
            "convert",
            {
                "plan_path": None,
                "dump_paths": (dump,),
                "config_path": None,
                "output_dir": tmp_path / "out",
                "output_format": "csv",
                "work_dir": tmp_path / "work",
                "oracle_image": "custom-image",
                "oracle_password": "secret",
                "container_runtime": "podman",
                "keep_alive": True,
            },
        )
    ]
