#!/usr/bin/env python3
"""Qualify one reviewed SECURITY DEFINER transition on two independent stacks."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import re
import stat
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import unquote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))
import security_definer_audit_v2 as audit

REPOSITORY = "tiangong-lca/database-engine"
RECEIPT_SCHEMA = "database.security-definer-transition-qualification-receipt.v1"
CONFIG_PATH = Path("supabase/config.toml")
LOCAL_CONFIG_OVERRIDES = {
    ("project_id",),
    ("api", "port"),
    ("db", "port"),
    ("db", "shadow_port"),
    ("db", "pooler", "port"),
    ("studio", "port"),
    ("inbucket", "port"),
    ("inbucket", "smtp_port"),
    ("inbucket", "pop3_port"),
    ("analytics", "port"),
    ("edge_runtime", "inspector_port"),
}


@dataclass(frozen=True)
class Connection:
    host: str
    port: int
    database: str
    user: str
    password: str | None

    def command(self, *args: str) -> list[str]:
        return [
            "psql", "--host", self.host, "--port", str(self.port),
            "--dbname", self.database, "--username", self.user, *args,
        ]

    def environment(self) -> dict[str, str]:
        environment = {key: value for key, value in os.environ.items() if not key.startswith("PG")}
        environment.pop("DATABASE_URL", None)
        if self.password is not None:
            environment["PGPASSWORD"] = self.password
        return environment


@dataclass(frozen=True)
class ReviewedFile:
    relative: str
    raw: bytes


def canonical_relative_path(value: str | Path) -> str:
    """Require one exact, portable repository-relative path spelling."""
    rendered = os.fspath(value)
    if (not rendered or "\\" in rendered
            or any(ord(character) < 32 for character in rendered)):
        raise ValueError("reviewed input path is not a canonical repository-relative POSIX path")
    path = PurePosixPath(rendered)
    if (path.is_absolute() or rendered != path.as_posix()
            or any(part in {"", ".", ".."} for part in path.parts)):
        raise ValueError("reviewed input path is not a canonical repository-relative POSIX path")
    return rendered


def _git_regular_blob(record: bytes, relative: str, *, source: str) -> str:
    entries = [entry for entry in record.split(b"\0") if entry]
    if len(entries) != 1:
        raise ValueError(f"reviewed input is not one {source} file: {relative}")
    try:
        metadata, encoded_path = entries[0].split(b"\t", 1)
        fields = metadata.decode("ascii").split()
        decoded_path = encoded_path.decode("utf-8")
    except (UnicodeDecodeError, ValueError) as exc:
        raise ValueError(f"reviewed input has invalid {source} metadata: {relative}") from exc
    if decoded_path != relative:
        raise ValueError(f"reviewed input {source} path differs: {relative}")
    if source == "index":
        if len(fields) != 3 or fields[2] != "0":
            raise ValueError(f"reviewed input is not a stage-0 index file: {relative}")
        mode, object_id = fields[0], fields[1]
    else:
        if len(fields) != 3 or fields[1] != "blob":
            raise ValueError(f"reviewed input is not a HEAD blob: {relative}")
        mode, object_id = fields[0], fields[2]
    if mode not in {"100644", "100755"} or not re.fullmatch(r"[0-9a-f]{40,64}", object_id):
        raise ValueError(f"reviewed input is not a git regular file: {relative}")
    return object_id


def _git_output(command: list[str], *, cwd: Path) -> bytes:
    return subprocess.run(command, cwd=cwd, check=True, stdout=subprocess.PIPE).stdout


def read_nofollow(root: Path, relative: str) -> bytes:
    """Read a regular file without following any path component."""
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
        raise ValueError("no-follow reviewed input reads are unavailable on this platform")
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    file_flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    if hasattr(os, "O_CLOEXEC"):
        directory_flags |= os.O_CLOEXEC
        file_flags |= os.O_CLOEXEC
    descriptor = os.open(root, directory_flags)
    try:
        parts = PurePosixPath(relative).parts
        for part in parts[:-1]:
            next_descriptor = os.open(part, directory_flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
        file_descriptor = os.open(parts[-1], file_flags, dir_fd=descriptor)
        try:
            if not stat.S_ISREG(os.fstat(file_descriptor).st_mode):
                raise ValueError(f"reviewed input is not a filesystem regular file: {relative}")
            chunks: list[bytes] = []
            while chunk := os.read(file_descriptor, 1024 * 1024):
                chunks.append(chunk)
            return b"".join(chunks)
        finally:
            os.close(file_descriptor)
    except OSError as exc:
        raise ValueError(f"reviewed input cannot be read without following links: {relative}") from exc
    finally:
        os.close(descriptor)


def canonical(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def run(command: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> None:
    subprocess.run(command, cwd=cwd, env=env, check=True)


def capture(command: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> str:
    return subprocess.run(
        command, cwd=cwd, env=env, check=True, text=True,
        stdout=subprocess.PIPE,
    ).stdout.strip()


def parse_connection(value: str) -> Connection:
    """Parse a loopback libpq URI without ever returning the secret in argv."""
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("local database URL has an invalid host or port") from exc
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise ValueError("local database URL must use postgres or postgresql")
    if parsed.query or parsed.fragment:
        raise ValueError("local database URL query overrides and fragments are forbidden")
    if not parsed.netloc or parsed.hostname is None or port is None:
        raise ValueError("local database URL must contain one explicit host and port")
    if "," in parsed.netloc or "," in parsed.hostname:
        raise ValueError("multi-host database URLs are forbidden")
    host = parsed.hostname.lower()
    if host == "localhost":
        host = "127.0.0.1"
    else:
        try:
            address = ipaddress.ip_address(host)
        except ValueError as exc:
            raise ValueError("local database URL must use one loopback TCP host") from exc
        if not address.is_loopback:
            raise ValueError("local database URL must use one loopback TCP host")
    database = unquote(parsed.path.removeprefix("/"))
    user = unquote(parsed.username or "")
    safe_name = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]*")
    if not safe_name.fullmatch(database) or not safe_name.fullmatch(user):
        raise ValueError("local database URL must contain one database and user")
    return Connection(host, port, database, user,
                      unquote(parsed.password) if parsed.password is not None else None)


def stack_connection(workdir: Path) -> Connection:
    result = capture(
        ["supabase", "status", "--workdir", str(workdir), "--output", "json"],
        cwd=workdir,
    )
    try:
        value = json.loads(result)["DB_URL"]
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise ValueError("supabase status did not return one DB_URL") from exc
    if not isinstance(value, str):
        raise ValueError("supabase status DB_URL is not a string")
    return parse_connection(value)


def exact_head(workdir: Path) -> str:
    return capture(["git", "rev-parse", "HEAD"], cwd=workdir)


def git_root(path: Path) -> Path:
    candidate = path if path.is_dir() else path.parent
    return Path(capture(["git", "rev-parse", "--show-toplevel"], cwd=candidate)).resolve()


def nested_changes(before: Any, after: Any, prefix: tuple[str, ...] = ()) -> set[tuple[str, ...]]:
    if isinstance(before, dict) and isinstance(after, dict):
        changes: set[tuple[str, ...]] = set()
        for key in before.keys() | after.keys():
            changes.update(nested_changes(before.get(key), after.get(key), (*prefix, key)))
        return changes
    return set() if before == after else {prefix}


def validate_stack_root(workdir: Path, expected_base: str) -> dict[str, Any]:
    if git_root(workdir) != workdir:
        raise ValueError(f"qualification workdir is not an exact repository root: {workdir}")
    if exact_head(workdir) != expected_base:
        raise ValueError(f"qualification workdir is not at the reviewed base: {workdir}")
    status = _git_output(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=workdir,
    ).decode("utf-8").splitlines()
    if any(line != f" M {CONFIG_PATH.as_posix()}" for line in status):
        raise ValueError(f"qualification workdir has non-local-config changes: {workdir}")
    committed = tomllib.loads(capture(["git", "show", f"HEAD:{CONFIG_PATH.as_posix()}"], cwd=workdir))
    current = tomllib.loads((workdir / CONFIG_PATH).read_text(encoding="utf-8"))
    changes = nested_changes(committed, current)
    if not changes <= LOCAL_CONFIG_OVERRIDES:
        rendered = sorted(".".join(path) for path in changes - LOCAL_CONFIG_OVERRIDES)
        raise ValueError(f"qualification config contains forbidden overrides: {rendered}")
    if status and not changes:
        raise ValueError("qualification config differs bytewise without a reviewed local override")
    for path in changes:
        value: Any = current
        for key in path:
            value = value[key]
        if path == ("project_id",):
            if not isinstance(value, str) or not re.fullmatch(r"[a-z0-9_-]+", value):
                raise ValueError("local project_id override is invalid")
        elif not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= 65535:
            raise ValueError(f"local port override {'.'.join(path)} is invalid")
    return current


def validate_source_file(path: str | Path, source_root: Path) -> ReviewedFile:
    relative = canonical_relative_path(path)
    index_record = _git_output(
        ["git", "ls-files", "--stage", "-z", "--", relative], cwd=source_root,
    )
    index_object = _git_regular_blob(index_record, relative, source="index")
    head_record = _git_output(
        ["git", "ls-tree", "-z", "HEAD", "--", relative], cwd=source_root,
    )
    head_object = _git_regular_blob(head_record, relative, source="HEAD")
    if index_object != head_object:
        raise ValueError(f"reviewed input index differs from HEAD: {relative}")
    committed = _git_output(["git", "cat-file", "blob", head_object], cwd=source_root)
    raw = read_nofollow(source_root, relative)
    if raw != committed:
        raise ValueError(f"schema replay input differs from source commit: {relative}")
    return ReviewedFile(relative, raw)


def validate_commit_source_file(
    path: str | Path, source_root: Path, commit_sha: str,
) -> ReviewedFile:
    """Read one immutable regular-file blob from an explicitly reviewed commit."""
    relative = canonical_relative_path(path)
    if not re.fullmatch(r"[0-9a-f]{40}", commit_sha):
        raise ValueError("reviewed source commit must be an exact 40-hex commit")
    record = _git_output(
        ["git", "ls-tree", "-z", commit_sha, "--", relative], cwd=source_root,
    )
    object_id = _git_regular_blob(record, relative, source=f"commit {commit_sha}")
    raw = _git_output(["git", "cat-file", "blob", object_id], cwd=source_root)
    return ReviewedFile(relative, raw)


def require_ancestor(ancestor: str, descendant: str, *, source_root: Path, label: str) -> None:
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        cwd=source_root, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    if result.returncode != 0:
        raise ValueError(f"{label} is not an ancestor of the reviewed source history")


def reject_external_psql_includes(reviewed: ReviewedFile) -> None:
    """Require frozen replay inputs to be self-contained SQL bytes."""
    if re.search(rb"(?m)^\s*\\i(?:r)?(?:\s|$)", reviewed.raw):
        raise ValueError(f"reviewed SQL contains an external psql include: {reviewed.relative}")


def validate_receipt(
    args: argparse.Namespace, source_root: Path,
) -> tuple[dict[str, Any], bytes, bytes]:
    receipt_file = validate_source_file(args.qualification_receipt, audit.ROOT)
    raw = receipt_file.raw
    if hashlib.sha256(raw).hexdigest() != args.expected_qualification_receipt_sha256:
        raise ValueError("qualification receipt bytes differ from reviewed SHA-256")
    try:
        receipt = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("qualification receipt is not JSON") from exc
    if raw != canonical(receipt):
        raise ValueError("qualification receipt is not canonical byte-for-byte")
    receipt_source = receipt.get("source", {})
    source_commit = receipt_source.get("commitSha", "")
    if not re.fullmatch(r"[0-9a-f]{40}", source_commit):
        raise ValueError("qualification receipt source commit must be an exact 40-hex commit")
    require_ancestor(args.expected_base, source_commit, source_root=source_root,
                     label="reviewed base commit")
    require_ancestor(source_commit, exact_head(source_root), source_root=source_root,
                     label="receipt source commit")
    migration_file = validate_commit_source_file(args.migration, source_root, source_commit)
    rollback_file = validate_commit_source_file(args.rollback, source_root, source_commit)
    reject_external_psql_includes(migration_file)
    reject_external_psql_includes(rollback_file)
    if hashlib.sha256(migration_file.raw).hexdigest() != args.expected_migration_sha256:
        raise ValueError("migration bytes differ from reviewed SHA-256")
    if hashlib.sha256(rollback_file.raw).hexdigest() != args.expected_rollback_sha256:
        raise ValueError("rollback bytes differ from reviewed SHA-256")
    fixture, fixture_sha = audit.read_hashed_json(
        audit.CONTRACT_DIR / "security_definer_transition_fixture.v1.json",
        audit.CONTRACT_DIR / "security_definer_transition_fixture.v1.sha256",
    )
    audit.validate_transition_fixture(fixture, audit.LINEAGE_SHA.read_text(encoding="utf-8").strip())
    source = fixture["source"]
    if not PurePosixPath(migration_file.relative).name.startswith(source["migrationVersion"] + "_"):
        raise ValueError("migration path does not match the reviewed migration version")
    expected = {
        "schemaVersion": RECEIPT_SCHEMA,
        "issue": source["issue"],
        "source": {
            "repository": REPOSITORY,
            "commitSha": source_commit,
            "fixturePath": "supabase/tests/contracts/security_definer_transition_fixture.v1.json",
            "fixtureSha256": fixture_sha,
        },
        "baseCommitSha": args.expected_base,
        "migrationVersion": source["migrationVersion"],
        "migration": {"path": migration_file.relative, "sha256": args.expected_migration_sha256},
        "rollback": {"path": rollback_file.relative, "sha256": args.expected_rollback_sha256},
    }
    if receipt != expected:
        raise ValueError(f"reviewed qualification receipt content differs: {receipt_file.relative}")
    if source["exactBaseDatabaseCommitSha"] != args.expected_base:
        raise ValueError("reviewed fixture base differs from qualification base")
    if source["migrationSha256"] != args.expected_migration_sha256:
        raise ValueError("migration bytes differ from the source-bound fixture")
    return receipt, migration_file.raw, rollback_file.raw


def reset(workdir: Path) -> Connection:
    run(["supabase", "db", "reset", "--workdir", str(workdir)], cwd=workdir)
    return stack_connection(workdir)


def query(connection: Connection, sql: str, *, cwd: Path) -> str:
    return capture(
        connection.command("-qAtX", "-v", "ON_ERROR_STOP=1", "-c", sql),
        cwd=cwd, env=connection.environment(),
    )


def database_identity(connection: Connection, *, cwd: Path) -> dict[str, Any]:
    raw = query(connection, """
select json_build_object(
  'database', current_database(),
  'user', current_user,
  'systemIdentifier', system_identifier::text
)::text
from pg_control_system();
""", cwd=cwd)
    try:
        identity = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("database identity query did not return JSON") from exc
    if not re.fullmatch(r"[0-9]+", str(identity.get("systemIdentifier", ""))):
        raise ValueError("database identity lacks pg_control_system().system_identifier")
    if identity.get("database") != connection.database or identity.get("user") != connection.user:
        raise ValueError("database session identity differs from the normalized connection target")
    return {
        "host": connection.host,
        "port": connection.port,
        "database": identity["database"],
        "user": identity["user"],
        "systemIdentifier": identity["systemIdentifier"],
    }


def validate_independent_identities(identities: list[dict[str, Any]]) -> None:
    normalized = [
        {key: identity[key] for key in ("host", "port", "database", "user")}
        for identity in identities
    ]
    if normalized[0] == normalized[1]:
        raise ValueError("qualification workdirs resolve to the same normalized database identity")
    if identities[0]["systemIdentifier"] == identities[1]["systemIdentifier"]:
        raise ValueError("qualification databases share pg_control_system().system_identifier")


def apply_sql(connection: Connection, sql: bytes, *, cwd: Path) -> None:
    subprocess.run(
        connection.command("-X", "-v", "ON_ERROR_STOP=1", "-f", "-"),
        cwd=cwd, env=connection.environment(), check=True, input=sql,
    )


def load_catalog(connection: Connection, *, cwd: Path) -> dict[str, dict[str, Any]]:
    result = subprocess.run(
        connection.command("-qAtX", "-v", "ON_ERROR_STOP=1"),
        cwd=cwd, env=connection.environment(), check=True, text=True,
        input=audit.catalog_proof_query(), stdout=subprocess.PIPE,
    )
    rows = json.loads(result.stdout)
    catalog = {row["objectKey"]: row for row in rows}
    if len(catalog) != len(rows):
        raise ValueError("governed routine catalog contains duplicate exact signatures")
    return catalog


def baseline_bytes(connection: Connection, *, cwd: Path) -> bytes:
    inventory, inventory_hash = audit.read_hashed_json(audit.INVENTORY, audit.INVENTORY_SHA)
    baseline, baseline_hash = audit.read_hashed_json(audit.BASELINE_AUDIT, audit.BASELINE_AUDIT_SHA)
    lineage, lineage_hash = audit.read_hashed_json(audit.LINEAGE, audit.LINEAGE_SHA)
    audit.validate_lineage(lineage, inventory, inventory_hash, baseline_hash)
    observed = audit.build_audit(
        lineage, lineage_hash, baseline, load_catalog(connection, cwd=cwd), audit.exposed_schemas(),
    )
    committed, _ = audit.read_hashed_json(audit.OUT, audit.SHA)
    if observed != committed:
        raise ValueError("clean stack baseline differs from committed v2 audit")
    return audit.canonical(observed).encode("utf-8")


def transitioned_bytes(connection: Connection, *, cwd: Path) -> bytes:
    baseline, _ = audit.read_hashed_json(audit.BASELINE_AUDIT, audit.BASELINE_AUDIT_SHA)
    lineage, _ = audit.read_hashed_json(audit.LINEAGE, audit.LINEAGE_SHA)
    fixture, fixture_hash = audit.read_hashed_json(
        audit.CONTRACT_DIR / "security_definer_transition_fixture.v1.json",
        audit.CONTRACT_DIR / "security_definer_transition_fixture.v1.sha256",
    )
    audit.validate_transition_fixture(fixture, audit.LINEAGE_SHA.read_text(encoding="utf-8").strip())
    by_original = {row["originalObjectKey"]: row for row in lineage["lineages"]}
    for move in fixture["moves"]:
        row = by_original[move["originalObjectKey"]]
        row["canonicalObjectKey"] = move["canonicalObjectKey"]
        row["compatibilityAliases"] = move["compatibilityAliases"]
    observed = audit.build_audit(
        lineage, audit.sha256_text(audit.canonical(lineage)), baseline,
        load_catalog(connection, cwd=cwd), audit.exposed_schemas(),
    )
    for field, value in fixture["expected"].items():
        if observed["summary"].get(field) != value:
            raise ValueError(f"transition summary {field} differs from source-bound fixture")
    observed["qualificationFixtureSha256"] = fixture_hash
    return audit.canonical(observed).encode("utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ci", action="store_true", help="required explicit integration-mode acknowledgement")
    parser.add_argument("--workdir", action="append", required=True)
    parser.add_argument("--source-workdir", type=Path, required=True)
    parser.add_argument("--migration", type=Path, required=True)
    parser.add_argument("--rollback", type=Path, required=True)
    parser.add_argument("--qualification-receipt", type=Path, required=True)
    parser.add_argument("--expected-qualification-receipt-sha256", required=True)
    parser.add_argument("--expected-migration-sha256", required=True)
    parser.add_argument("--expected-rollback-sha256", required=True)
    parser.add_argument("--expected-base", required=True)
    args = parser.parse_args()
    if not args.ci:
        raise SystemExit("transition qualification requires explicit --ci integration mode")
    if len(args.workdir) != 2:
        raise SystemExit("exactly two independent --workdir values are required")
    if not re.fullmatch(r"[0-9a-f]{40}", args.expected_base):
        raise SystemExit("--expected-base must be an exact 40-hex commit")
    for value in (args.expected_qualification_receipt_sha256,
                  args.expected_migration_sha256, args.expected_rollback_sha256):
        if not re.fullmatch(r"[0-9a-f]{64}", value):
            raise SystemExit("expected receipt and SQL hashes must be exact lowercase SHA-256")
    workdirs = [Path(value).resolve(strict=True) for value in args.workdir]
    source_root = args.source_workdir.resolve(strict=True)
    if len({*workdirs, source_root}) != 3:
        raise SystemExit("source and qualification workdirs must be three independent repository roots")
    configs = [validate_stack_root(workdir, args.expected_base) for workdir in workdirs]
    if (configs[0]["project_id"] == configs[1]["project_id"]
            or configs[0]["db"]["port"] == configs[1]["db"]["port"]):
        raise SystemExit("qualification roots require distinct local project_id and db.port overrides")
    if git_root(source_root) != source_root or capture(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"], cwd=source_root,
    ):
        raise SystemExit("reviewed schema replay source root must be exactly clean")
    _, migration_sql, rollback_sql = validate_receipt(args, source_root)

    # Reset and identify both stacks before applying any transition SQL.
    connections = [reset(workdir) for workdir in workdirs]
    identities = [database_identity(connection, cwd=workdir)
                  for connection, workdir in zip(connections, workdirs)]
    validate_independent_identities(identities)

    baseline_a = baseline_bytes(connections[0], cwd=workdirs[0])
    baseline_b = baseline_bytes(connections[1], cwd=workdirs[1])
    if baseline_b != baseline_a:
        raise ValueError("two independent clean stacks produced different baseline bytes")
    apply_sql(connections[0], migration_sql, cwd=workdirs[0])
    transitioned_a1 = transitioned_bytes(connections[0], cwd=workdirs[0])
    apply_sql(connections[0], rollback_sql, cwd=workdirs[0])
    rolled_back_a = baseline_bytes(connections[0], cwd=workdirs[0])
    if rolled_back_a != baseline_a:
        raise ValueError("operator rollback did not restore exact baseline audit bytes")
    apply_sql(connections[0], migration_sql, cwd=workdirs[0])
    transitioned_a2 = transitioned_bytes(connections[0], cwd=workdirs[0])
    if transitioned_a2 != transitioned_a1:
        raise ValueError("rollforward retry did not reproduce exact transitioned audit bytes")
    apply_sql(connections[1], migration_sql, cwd=workdirs[1])
    transitioned_b = transitioned_bytes(connections[1], cwd=workdirs[1])
    if transitioned_b != transitioned_a1:
        raise ValueError("two independent clean stacks produced different transition bytes")
    print(json.dumps({
        "baselineSha256": hashlib.sha256(baseline_a).hexdigest(),
        "transitionSha256": hashlib.sha256(transitioned_a1).hexdigest(),
        "databaseIdentities": identities,
        "independentStacks": 2,
        "operatorRollback": True,
        "rollforward": True,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
