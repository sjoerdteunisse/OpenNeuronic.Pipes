from __future__ import annotations

import pytest
from typer.testing import CliRunner

from openneuronic.pipes.cli.__main__ import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Root app
# ---------------------------------------------------------------------------


def test_help_shows_command_groups() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for group in ("run", "deploy", "status", "schema", "contract", "bookmark", "replay", "lineage"):
        assert group in result.output


def test_version_command() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.output


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def test_run_pipe_help() -> None:
    result = runner.invoke(app, ["run", "pipe", "--help"])
    assert result.exit_code == 0
    assert "pipe_id" in result.output.lower() or "PIPE_ID" in result.output


def test_run_opus_help() -> None:
    result = runner.invoke(app, ["run", "opus", "--help"])
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# deploy
# ---------------------------------------------------------------------------


def test_deploy_pipe_dry_run_outputs_yaml() -> None:
    result = runner.invoke(app, ["deploy", "pipe", "orders-sync", "--env", "prod", "--dry-run"])
    assert result.exit_code == 0
    # Should contain YAML/JSON output with the pipe id
    assert "orders-sync" in result.output


def test_deploy_opus_dry_run() -> None:
    result = runner.invoke(app, ["deploy", "opus", "nightly-etl", "--dry-run"])
    assert result.exit_code == 0
    assert "nightly-etl" in result.output


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------


def test_schema_diff_help() -> None:
    result = runner.invoke(app, ["schema", "diff", "--help"])
    assert result.exit_code == 0


def test_schema_diff_unknown_schema_exits_nonzero() -> None:
    result = runner.invoke(app, ["schema", "diff", "NoSuchSchema", "1", "2"])
    assert result.exit_code != 0


def test_schema_diff_known_schema() -> None:
    """Import a test module that registers schemas, then diff them."""
    import importlib
    import sys

    # Dynamically create a temporary module with two schema versions
    import types
    from openneuronic.pipes.schema.base import Schema, schema_version
    from openneuronic.pipes.schema.field import Field
    from openneuronic.pipes.schema.field_type import FieldType
    from openneuronic.pipes.schema.registry import schema_registry

    # Register two versions under a unique name to avoid cross-test pollution
    @schema_version(1)
    class _DiffSchemaA(Schema):
        id = Field(FieldType.INTEGER, nullable=False, primary_key=True)

    @schema_version(2, previous=_DiffSchemaA)
    class _DiffSchemaB(_DiffSchemaA):
        name = Field(FieldType.STRING, nullable=True)

    result = runner.invoke(app, ["schema", "diff", "_DiffSchemaA", "1", "2"])
    assert result.exit_code == 0
    assert "_DiffSchemaA" in result.output


def test_schema_migrate_help() -> None:
    result = runner.invoke(app, ["schema", "migrate", "--help"])
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# contract
# ---------------------------------------------------------------------------


def test_contract_show_help() -> None:
    result = runner.invoke(app, ["contract", "show", "--help"])
    assert result.exit_code == 0


def test_contract_validate_help() -> None:
    result = runner.invoke(app, ["contract", "validate", "--help"])
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# bookmark
# ---------------------------------------------------------------------------


def test_bookmark_show_help() -> None:
    result = runner.invoke(app, ["bookmark", "show", "--help"])
    assert result.exit_code == 0


def test_bookmark_reset_aborts_without_confirmation() -> None:
    result = runner.invoke(app, ["bookmark", "reset", "some-pipe"], input="n\n")
    assert "Aborted" in result.output or result.exit_code != 0


# ---------------------------------------------------------------------------
# replay
# ---------------------------------------------------------------------------


def test_replay_create_help() -> None:
    result = runner.invoke(app, ["replay", "create", "--help"])
    assert result.exit_code == 0


def test_replay_run_help() -> None:
    result = runner.invoke(app, ["replay", "run", "--help"])
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# lineage
# ---------------------------------------------------------------------------


def test_lineage_show_help() -> None:
    result = runner.invoke(app, ["lineage", "show", "--help"])
    assert result.exit_code == 0
