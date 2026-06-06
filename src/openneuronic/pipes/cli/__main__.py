"""onpipes CLI — command-line interface for OpenNeuronic.Pipes."""
from __future__ import annotations

import importlib
import json
import sys
from typing import Optional

try:
    import typer
except ImportError as exc:
    raise ImportError(
        "typer is required for the CLI. "
        "Install with: pip install 'openneuronic-pipes[cli]'"
    ) from exc

_VERSION = "0.1.0"

app = typer.Typer(
    name="onpipes",
    help="OpenNeuronic.Pipes — data movement and transformation platform.",
    no_args_is_help=True,
)

# ---------------------------------------------------------------------------
# Sub-command groups
# ---------------------------------------------------------------------------

run_app      = typer.Typer(help="Run a pipe or opus.")
deploy_app   = typer.Typer(help="Generate deployment manifests.")
status_app   = typer.Typer(help="Show runtime status.")
schema_app   = typer.Typer(help="Schema introspection and diffing.")
contract_app = typer.Typer(help="Contract inspection and validation.")
bookmark_app = typer.Typer(help="Bookmark management.")
replay_app   = typer.Typer(help="Replay point management.")
lineage_app  = typer.Typer(help="Lineage graph inspection.")

app.add_typer(run_app,      name="run")
app.add_typer(deploy_app,   name="deploy")
app.add_typer(status_app,   name="status")
app.add_typer(schema_app,   name="schema")
app.add_typer(contract_app, name="contract")
app.add_typer(bookmark_app, name="bookmark")
app.add_typer(replay_app,   name="replay")
app.add_typer(lineage_app,  name="lineage")


# ---------------------------------------------------------------------------
# version
# ---------------------------------------------------------------------------

@app.command()
def version() -> None:
    """Print the installed package version."""
    typer.echo(f"openneuronic-pipes {_VERSION}")


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

@run_app.command("pipe")
def run_pipe(
    pipe_id: str = typer.Argument(..., help="Pipe identifier to execute."),
    module: Optional[str] = typer.Option(None, "--module", "-m", help="Python module to import before running."),
) -> None:
    """Run a named pipe in-process using LocalRunner."""
    _maybe_import(module)
    typer.echo(f"[run pipe] {pipe_id} — not yet wired to a live registry. "
               "Import your pipe and call LocalRunner().run(pipe) directly.")


@run_app.command("opus")
def run_opus(
    opus_id: str = typer.Argument(..., help="Opus identifier to execute."),
    module: Optional[str] = typer.Option(None, "--module", "-m", help="Python module to import before running."),
) -> None:
    """Run a named opus in-process using OpusRunner."""
    _maybe_import(module)
    typer.echo(f"[run opus] {opus_id} — not yet wired to a live registry. "
               "Import your opus and call OpusRunner().run(opus) directly.")


# ---------------------------------------------------------------------------
# deploy
# ---------------------------------------------------------------------------

@deploy_app.command("pipe")
def deploy_pipe(
    pipe_id: str = typer.Argument(..., help="Pipe identifier."),
    env: str = typer.Option("production", "--env", help="Target environment label."),
    profile: str = typer.Option("standard", "--profile", help="Resource profile: light|standard|heavy."),
    schedule: Optional[str] = typer.Option(None, "--schedule", help="Cron schedule for CronJob generation."),
    module: Optional[str] = typer.Option(None, "--module", "-m"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print manifests without applying."),
    output: str = typer.Option("yaml", "--output", "-o", help="Output format: yaml|json."),
) -> None:
    """Generate Kubernetes manifests for a pipe."""
    _maybe_import(module)
    from openneuronic.pipes.core.enums import ResourceProfile
    from openneuronic.pipes.deploy.manifests import ManifestGenerator

    rp = ResourceProfile(profile)
    gen = ManifestGenerator(image_tag=env)
    # Without a live registry we generate a placeholder manifest.
    placeholder = gen._k8s.configmap(
        name=f"onpipes-pipe-{pipe_id}",
        data={"pipe_id": pipe_id, "env": env, "profile": profile},
    )
    _output({"config": placeholder}, output)
    if dry_run:
        typer.echo("(dry-run — manifests not applied)", err=True)


@deploy_app.command("opus")
def deploy_opus(
    opus_id: str = typer.Argument(..., help="Opus identifier."),
    env: str = typer.Option("production", "--env"),
    profile: str = typer.Option("standard", "--profile"),
    module: Optional[str] = typer.Option(None, "--module", "-m"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    output: str = typer.Option("yaml", "--output", "-o"),
) -> None:
    """Generate Kubernetes manifests for an opus."""
    _maybe_import(module)
    from openneuronic.pipes.deploy.k8s import K8sManifestBuilder
    builder = K8sManifestBuilder()
    placeholder = builder.configmap(
        name=f"onpipes-opus-{opus_id}",
        data={"opus_id": opus_id, "env": env, "profile": profile},
    )
    _output({"config": placeholder}, output)
    if dry_run:
        typer.echo("(dry-run — manifests not applied)", err=True)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

@status_app.command("pipe")
def status_pipe(
    pipe_id: str = typer.Argument(...),
) -> None:
    """Show the last run status for a pipe (requires Redis)."""
    typer.echo(f"[status pipe] {pipe_id} — Redis state store not configured.")


@status_app.command("opus")
def status_opus(
    opus_id: str = typer.Argument(...),
) -> None:
    """Show the last run status for an opus (requires Redis)."""
    typer.echo(f"[status opus] {opus_id} — Redis state store not configured.")


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------

@schema_app.command("diff")
def schema_diff(
    schema_name: str = typer.Argument(..., help="Schema class name (must be in the registry)."),
    from_version: int = typer.Argument(..., help="Source schema version."),
    to_version: int = typer.Argument(..., help="Target schema version."),
    module: Optional[str] = typer.Option(None, "--module", "-m", help="Module to import to populate the registry."),
) -> None:
    """Show the diff between two schema versions."""
    _maybe_import(module)
    from openneuronic.pipes.schema.registry import schema_registry
    from openneuronic.pipes.schema.diff import diff_schemas

    try:
        old_cls = schema_registry.get(schema_name, from_version)
        new_cls = schema_registry.get(schema_name, to_version)
    except KeyError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    diff = diff_schemas(old_cls, new_cls)

    if not diff.changes:
        typer.echo(f"No differences between {schema_name} v{from_version} and v{to_version}.")
        return

    typer.echo(f"Schema diff: {schema_name} v{from_version} → v{to_version}")
    typer.echo(f"{'Field':<30} {'Kind':<12} {'Safe'}")
    typer.echo("-" * 55)
    from openneuronic.pipes.schema.diff import ChangeKind
    for change in diff.changes:
        safe = "yes" if change.kind == ChangeKind.SAFE else "NO"
        typer.echo(f"{change.name:<30} {change.kind:<12} {safe}")


@schema_app.command("migrate")
def schema_migrate(
    schema_name: str = typer.Argument(...),
    to_version: int = typer.Option(..., "--to"),
    module: Optional[str] = typer.Option(None, "--module", "-m"),
) -> None:
    """Show the migration path to a target schema version."""
    _maybe_import(module)
    from openneuronic.pipes.schema.registry import schema_registry

    path = schema_registry.migration_path(schema_name, 1, to_version)
    typer.echo(f"Migration path for {schema_name} → v{to_version}:")
    for version in path:
        typer.echo(f"  v{version}")


# ---------------------------------------------------------------------------
# contract
# ---------------------------------------------------------------------------

@contract_app.command("show")
def contract_show(
    contract_name: str = typer.Argument(...),
    module: Optional[str] = typer.Option(None, "--module", "-m"),
) -> None:
    """Print a summary of a contract."""
    _maybe_import(module)
    from openneuronic.pipes.contracts.registry import contract_registry

    try:
        cls = contract_registry.get_latest(contract_name)
    except KeyError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Contract: {contract_name}")
    for attr in ("primary_key", "required_fields", "compatibility", "freshness_sla", "owner"):
        val = getattr(cls, attr, None)
        if val:
            typer.echo(f"  {attr}: {val}")


@contract_app.command("validate")
def contract_validate(
    pipe_id: str = typer.Option(..., "--pipe"),
    module: Optional[str] = typer.Option(None, "--module", "-m"),
) -> None:
    """Validate that a pipe satisfies its contract at deploy time."""
    _maybe_import(module)
    typer.echo(f"[contract validate] {pipe_id} — import your pipe and call ContractEnforcer.enforce_deploy().")


# ---------------------------------------------------------------------------
# bookmark
# ---------------------------------------------------------------------------

@bookmark_app.command("show")
def bookmark_show(
    pipe_id: str = typer.Argument(...),
) -> None:
    """Show the current bookmark for a pipe (requires Redis)."""
    typer.echo(f"[bookmark show] {pipe_id} — Redis bookmark store not configured.")


@bookmark_app.command("reset")
def bookmark_reset(
    pipe_id: str = typer.Argument(...),
    confirm: bool = typer.Option(False, "--yes", help="Skip confirmation prompt."),
) -> None:
    """Reset (delete) the bookmark for a pipe."""
    if not confirm:
        confirmed = typer.confirm(f"Reset bookmark for pipe {pipe_id!r}?")
        if not confirmed:
            raise typer.Abort()
    typer.echo(f"[bookmark reset] {pipe_id} — Redis bookmark store not configured.")


# ---------------------------------------------------------------------------
# replay
# ---------------------------------------------------------------------------

@replay_app.command("create")
def replay_create(
    pipe_id: Optional[str] = typer.Option(None, "--pipe"),
    from_run: Optional[str] = typer.Option(None, "--from-run"),
    module: Optional[str] = typer.Option(None, "--module", "-m"),
) -> None:
    """Create a replay point from a prior run."""
    _maybe_import(module)
    typer.echo(f"[replay create] pipe={pipe_id} from_run={from_run} — replay store not configured.")


@replay_app.command("run")
def replay_run(
    replay_id: str = typer.Option(..., "--replay-id"),
    dry_run: bool = typer.Option(False, "--dry-run"),
) -> None:
    """Execute a replay from a stored replay point."""
    typer.echo(f"[replay run] {replay_id} dry_run={dry_run} — replay store not configured.")


# ---------------------------------------------------------------------------
# lineage
# ---------------------------------------------------------------------------

@lineage_app.command("show")
def lineage_show(
    pipe_id: Optional[str] = typer.Option(None, "--pipe"),
    run_id: Optional[str] = typer.Option(None, "--run"),
) -> None:
    """Show lineage events for a pipe or run."""
    typer.echo(f"[lineage show] pipe={pipe_id} run={run_id} — lineage store not configured.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _maybe_import(module: str | None) -> None:
    if module:
        try:
            importlib.import_module(module)
        except ImportError as exc:
            typer.echo(f"Error importing module {module!r}: {exc}", err=True)
            raise typer.Exit(1)


def _output(manifests: dict, fmt: str) -> None:
    if fmt == "json":
        typer.echo(json.dumps(manifests, indent=2))
        return
    try:
        import yaml
        for manifest in manifests.values():
            typer.echo("---")
            typer.echo(yaml.dump(manifest, default_flow_style=False, sort_keys=False))
    except ImportError:
        typer.echo(json.dumps(manifests, indent=2))


if __name__ == "__main__":
    app()
