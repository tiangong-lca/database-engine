#!/usr/bin/env python3
"""Fail-closed local database contract runner."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import subprocess
import sys
from pathlib import Path, PurePosixPath
from urllib.parse import quote, unquote, urlsplit

from identity_collaboration_target import apply_target_environment, resolve_target

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "supabase/tests/manifest.json"
TRANSITION_FIXTURE = ROOT / "supabase/tests/contracts/security_definer_transition_fixture.v1.json"
IDENTITY_QUALIFICATION_SCRIPTS = (
    "scripts/test_identity_collaboration_policy_variants.py",
    "scripts/test_identity_collaboration_rollback.py",
)


def supabase_command(*args: str) -> list[str]:
    command = ["supabase"]
    if workdir := os.environ.get("SUPABASE_WORKDIR"):
        command.extend(["--workdir", workdir])
    command.extend(args)
    return command


def run(command: list[str], *, env: dict[str, str] | None = None) -> None:
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=ROOT, env=env, check=True)


def run_destructive_identity_qualification(*, enabled: bool, contract_selected: bool) -> None:
    """Run the complete fail-closed Issue #355 destructive qualification gate."""
    if not enabled:
        return
    if not contract_selected:
        raise SystemExit("selected suite does not contain the Issue #355 identity contract")
    for script in IDENTITY_QUALIFICATION_SCRIPTS:
        run([sys.executable, script])


def database_cli_target(value: str) -> tuple[str, dict[str, str]]:
    """Return a password-free, single-host CLI URL and PGPASSWORD environment."""
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise SystemExit("DATABASE_URL has an invalid host or port") from exc
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise SystemExit("DATABASE_URL must use postgres or postgresql")
    if parsed.query or parsed.fragment:
        raise SystemExit("DATABASE_URL query overrides and fragments are forbidden")
    if not parsed.netloc or parsed.hostname is None or port is None:
        raise SystemExit("DATABASE_URL must contain one explicit TCP host and port")
    host = unquote(parsed.hostname)
    if ("," in parsed.netloc or "," in host or "/" in host or "\\" in host
            or any(character.isspace() or ord(character) < 32 for character in host)):
        raise SystemExit("DATABASE_URL must use one TCP host; multi-host and socket targets are forbidden")
    try:
        loopback = host == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        loopback = False
    if not loopback:
        raise SystemExit("DATABASE_URL must use a literal loopback host for canonical-local validation")
    user = unquote(parsed.username or "")
    database = unquote(parsed.path.removeprefix("/"))
    if not user or not database or "/" in database:
        raise SystemExit("DATABASE_URL must contain one user and database")
    if ":" in host:
        host = f"[{host}]"
    target = f"{parsed.scheme}://{quote(user, safe='')}@{host}:{port}/{quote(database, safe='')}"
    environment = {key: item for key, item in os.environ.items() if not key.startswith("PG")}
    environment.pop("DATABASE_URL", None)
    if parsed.password is not None:
        environment["PGPASSWORD"] = unquote(parsed.password)
    return target, environment


def canonical_database_url() -> str:
    """Resolve and validate the one local database used by every contract probe."""
    if value := os.environ.get("DATABASE_URL"):
        database_cli_target(value)
        return value
    result = subprocess.run(
        supabase_command("status", "--output", "json"), cwd=ROOT, check=True,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        value = json.loads(result.stdout)["DB_URL"]
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise SystemExit("supabase status did not return one DB_URL") from exc
    if not isinstance(value, str):
        raise SystemExit("supabase status DB_URL must be a string")
    database_cli_target(value)
    return value


def pending_security_definer_transition() -> list[Path]:
    if not TRANSITION_FIXTURE.is_file():
        return []
    raw = TRANSITION_FIXTURE.read_bytes()
    digest_path = TRANSITION_FIXTURE.with_suffix(".sha256")
    if not digest_path.is_file() or digest_path.read_text(encoding="utf-8").strip() != hashlib.sha256(raw).hexdigest():
        raise SystemExit("SECURITY DEFINER transition fixture hash differs from reviewed bytes")
    try:
        fixture = json.loads(raw)
        version = fixture["source"]["migrationVersion"]
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise SystemExit("SECURITY DEFINER transition fixture cannot determine migrationVersion") from exc
    canonical = (json.dumps(
        fixture, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ) + "\n").encode()
    if raw != canonical:
        raise SystemExit("SECURITY DEFINER transition fixture is not canonical byte-for-byte")
    if not isinstance(version, str) or not version.isdigit() or len(version) != 14:
        raise SystemExit("SECURITY DEFINER transition fixture migrationVersion is invalid")
    return sorted((ROOT / "supabase/migrations").glob(f"{version}_*.sql"))


def check_lint() -> None:
    command = supabase_command("db", "lint")
    environment = None
    if db_url := os.environ.get("DATABASE_URL"):
        cli_database_url, environment = database_cli_target(db_url)
        command.extend(["--db-url", cli_database_url])
    else:
        command.append("--local")
    command.extend(["--level", "warning", "--fail-on", "none"])
    print("+", " ".join(command), flush=True)
    result = subprocess.run(
        command, cwd=ROOT, env=environment, check=True, text=True, stdout=subprocess.PIPE,
    )
    report = json.loads(result.stdout)
    actual = {
        (item["function"], issue["sqlState"], issue["message"])
        for item in report for issue in item["issues"] if issue["level"] == "error"
    }
    allowlist_path = ROOT / "supabase/tests/contracts/lint_error_allowlist.json"
    allowlist = json.loads(allowlist_path.read_text(encoding="utf-8"))
    expected = {(e["function"], e["sqlState"], e["message"]) for e in allowlist["entries"]}
    if actual != expected:
        unexpected = sorted(actual - expected)
        stale = sorted(expected - actual)
        raise SystemExit(f"lint error baseline mismatch; unexpected={unexpected}; stale={stale}")
    print(f"lint errors match {len(expected)} exact reviewed analyzer rules")


def tracked_test_files() -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "supabase/tests"], cwd=ROOT, check=True,
        text=True, stdout=subprocess.PIPE,
    )
    return sorted(line for line in result.stdout.splitlines() if line)


def load_and_validate_manifest() -> tuple[dict, dict[str, list[str]]]:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    classified: dict[str, list[str]] = {
        item["name"]: [] for item in manifest["classifications"]
    }
    errors: list[str] = []
    for path in tracked_test_files():
        matches = [
            item["name"] for item in manifest["classifications"]
            if PurePosixPath(path).match(item["glob"])
        ]
        if len(matches) != 1:
            errors.append(f"{path}: expected exactly one classification, got {matches}")
        else:
            classified[matches[0]].append(path)
    for suite_name, suite in manifest["suites"].items():
        for path in suite.get("files", []):
            if path not in tracked_test_files():
                errors.append(f"suite {suite_name}: missing tracked file {path}")
        for path, reason in suite.get("excludedFiles", {}).items():
            if path not in tracked_test_files():
                errors.append(f"suite {suite_name}: stale excluded path {path}")
            if not reason.strip():
                errors.append(f"suite {suite_name}: excluded path lacks reason: {path}")
        if suite.get("excludedFiles") and not suite.get("excludedFollowUp", "").startswith("https://github.com/"):
            errors.append(f"suite {suite_name}: exclusions lack a GitHub follow-up")
    if errors:
        raise SystemExit("manifest classification failed:\n" + "\n".join(errors))
    return manifest, classified


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", default="canonical-local")
    parser.add_argument("--skip-reset", action="store_true")
    parser.add_argument("--skip-lint", action="store_true")
    parser.add_argument("--skip-data-api", action="store_true")
    parser.add_argument("--security-definer-transition-workdir", action="append")
    parser.add_argument("--security-definer-transition-source-workdir")
    parser.add_argument("--security-definer-transition-migration")
    parser.add_argument("--security-definer-transition-rollback")
    parser.add_argument("--security-definer-transition-qualification-receipt")
    parser.add_argument("--security-definer-transition-qualification-receipt-sha256")
    parser.add_argument("--security-definer-transition-migration-sha256")
    parser.add_argument("--security-definer-transition-rollback-sha256")
    parser.add_argument("--security-definer-transition-base")
    parser.add_argument(
        "--run-destructive-identity-qualification", action="store_true",
        help=(
            "opt in to the complete Issue #355 dual-policy/RLS plus "
            "rollback/roll-forward and lock-failure DDL gate"
        ),
    )
    args = parser.parse_args()
    qualification = [
        args.security_definer_transition_workdir,
        args.security_definer_transition_source_workdir,
        args.security_definer_transition_migration,
        args.security_definer_transition_rollback,
        args.security_definer_transition_qualification_receipt,
        args.security_definer_transition_qualification_receipt_sha256,
        args.security_definer_transition_migration_sha256,
        args.security_definer_transition_rollback_sha256,
        args.security_definer_transition_base,
    ]
    qualification_requested = any(value is not None for value in qualification)
    pending_transition = pending_security_definer_transition()
    qualification_enabled = qualification_requested or bool(pending_transition)
    if qualification_enabled:
        if any(value is None for value in qualification):
            reason = f"; pending migration={pending_transition[0].relative_to(ROOT)}" if pending_transition else ""
            raise SystemExit(
                "SECURITY DEFINER transition qualification requires every exact input" + reason
            )
        if len(args.security_definer_transition_workdir) != 2:
            raise SystemExit("SECURITY DEFINER transition qualification requires exactly two workdirs")
        if args.suite != "canonical-local" or args.skip_reset or args.skip_lint or args.skip_data_api:
            raise SystemExit(
                "SECURITY DEFINER transition qualification requires the complete canonical-local CI mode"
            )
    manifest, classified = load_and_validate_manifest()
    if args.suite not in manifest["suites"]:
        raise SystemExit(f"unknown suite: {args.suite}")
    suite = manifest["suites"][args.suite]
    files = suite.get("files") or [
        path for path in classified[suite["classification"]]
        if path not in suite.get("excludedFiles", {})
    ]
    if not files:
        raise SystemExit(f"suite {args.suite} selected no files")
    identity_qualification = "supabase/tests/20260801_identity_collaboration_expand.sql" in files
    if args.run_destructive_identity_qualification and not identity_qualification:
        raise SystemExit("selected suite does not contain the Issue #355 identity contract")
    if args.suite == "worker-control-plane":
        if args.skip_reset or args.skip_data_api:
            raise SystemExit(
                "worker-control-plane qualification requires reset and all Data API phase probes"
            )
        run([sys.executable, "scripts/test_worker_control_plane_physical_upgrade.py"])
        run([sys.executable, "scripts/test_worker_control_plane_physical_rollback.py"])
    if not args.skip_reset:
        if os.environ.get("DATABASE_URL") and not os.environ.get("SUPABASE_WORKDIR"):
            raise SystemExit("DATABASE_URL qualification requires --skip-reset or SUPABASE_WORKDIR")
        run(supabase_command("db", "reset", "--local"))
    target = resolve_target()
    apply_target_environment(target)
    test_command = supabase_command("test", "db", *files)
    cli_database_url, test_environment = database_cli_target(target.database_url)
    test_command.extend(["--db-url", cli_database_url])
    run(test_command, env=test_environment)
    if not args.skip_data_api:
        run([sys.executable, "scripts/test_worker_control_plane_data_api.py"])
    if identity_qualification:
        if not args.skip_data_api:
            run([sys.executable, "scripts/test_identity_collaboration_data_api.py"])
        run([sys.executable, "scripts/test_identity_collaboration_concurrency.py"])
        run_destructive_identity_qualification(
            enabled=args.run_destructive_identity_qualification,
            contract_selected=identity_qualification,
        )
    if not args.skip_lint:
        # CLI defaults to exit zero even when it prints ERROR diagnostics.
        check_lint()
    run([sys.executable, "scripts/export_database_contract.py", "--check"])
    run([sys.executable, "scripts/schema_boundary_phase.py"])
    run([sys.executable, "scripts/public_inventory_closure.py", "--check"])
    run([sys.executable, "scripts/security_definer_audit.py", "--check"])
    run([sys.executable, "scripts/security_definer_audit_v2.py", "--check"])
    run([
        sys.executable, "-m", "unittest",
        "scripts.test_security_definer_audit_v2",
        "scripts.test_security_definer_audit_v2_transition_integration_runner",
    ])
    if not args.skip_data_api:
        conformance_environment = os.environ.copy()
        conformance_environment["ISSUE333_DATABASE_URL"] = canonical_database_url()
        run([
            sys.executable, "-m", "unittest",
            "scripts.test_security_definer_audit_v2_postgrest_conformance",
        ], env=conformance_environment)
    if qualification_enabled:
        command = [
            sys.executable, "scripts/test_security_definer_audit_v2_transition_integration.py",
            "--ci",
            "--source-workdir", args.security_definer_transition_source_workdir,
            "--migration", args.security_definer_transition_migration,
            "--rollback", args.security_definer_transition_rollback,
            "--qualification-receipt", args.security_definer_transition_qualification_receipt,
            "--expected-qualification-receipt-sha256",
            args.security_definer_transition_qualification_receipt_sha256,
            "--expected-migration-sha256", args.security_definer_transition_migration_sha256,
            "--expected-rollback-sha256", args.security_definer_transition_rollback_sha256,
            "--expected-base", args.security_definer_transition_base,
        ]
        for workdir in args.security_definer_transition_workdir:
            command.extend(["--workdir", workdir])
        run(command)
    run([
        "git", "diff", "--exit-code", "--",
        "supabase/workspace/remote_schema.sql",
        "supabase/workspace/global",
        "supabase/workspace/schemas",
    ])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
