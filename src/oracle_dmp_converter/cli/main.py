"""Click group and shared logging setup."""

from __future__ import annotations

import logging

import click

from oracle_dmp_converter.cli.commands import convert, doctor, inspect, plan_command


@click.group()
def main() -> None:
    """Convert Oracle Data Pump dumps to Parquet, Avro, or CSV."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


main.add_command(doctor)
main.add_command(inspect)
main.add_command(plan_command)
main.add_command(convert)
