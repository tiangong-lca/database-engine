#!/usr/bin/env python3
"""Fail-closed local database contract runner."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath
from urllib.parse import quote, unquote, urlsplit

try:
    from .identity_collaboration_target import apply_target_environment, resolve_target
except ImportError:  # Direct script execution keeps scripts/ on sys.path.
    from identity_collaboration_target import apply_target_environment, resolve_target

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "supabase/tests/manifest.json"
TRANSITION_FIXTURE = ROOT / "supabase/tests/contracts/security_definer_transition_fixture.v1.json"
AUDIT_V2 = ROOT / "supabase/tests/contracts/security_definer_audit_v2.json"
IDENTITY_QUALIFICATION_SCRIPTS = (
    "scripts/test_identity_collaboration_policy_variants.py",
    "scripts/test_identity_collaboration_rollback.py",
)
IDENTITY_QUALIFICATION_VERSION = "20260801061000"
TRANSIENT_LOCAL_RESET_MARKERS = ("context deadline exceeded", "Error status 502")


def supabase_command(*args: str) -> list[str]:
    command = ["supabase"]
    if workdir := os.environ.get("SUPABASE_WORKDIR"):
        command.extend(["--workdir", workdir])
    command.extend(args)
    return command


def run(command: list[str], *, env: dict[str, str] | None = None) -> None:
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=ROOT, env=env, check=True)


def latest_migration_version() -> str:
    versions = [
        path.name.split("_", 1)[0]
        for path in (ROOT / "supabase/migrations").glob("*.sql")
    ]
    if not versions:
        raise SystemExit("repository contains no migration version")
    return max(versions)


def reset_local_database(*, version: str | None = None) -> None:
    """Reset one selected local stack and tolerate only proved CLI restart noise."""
    command = supabase_command("db", "reset", "--local")
    if version is not None:
        command.extend(["--version", version])
    print("+", " ".join(command), flush=True)
    result = subprocess.run(
        command, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if result.returncode == 0:
        return
    combined = result.stdout + result.stderr
    expected_version = version or latest_migration_version()
    if any(marker in combined for marker in TRANSIENT_LOCAL_RESET_MARKERS):
        database_url = canonical_database_url()
        cli_database_url, environment = database_cli_target(database_url)
        head = subprocess.run(
            [
                "psql", cli_database_url, "-XAt", "-v", "ON_ERROR_STOP=1", "-c",
                "select max(version) from supabase_migrations.schema_migrations;",
            ],
            cwd=ROOT, env=environment, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        if head.returncode == 0 and head.stdout.strip() == expected_version:
            print(
                "accepted transient Supabase service restart failure after exact "
                f"migration-head readback: {expected_version}",
                flush=True,
            )
            return
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    result.check_returncode()


def run_destructive_identity_qualification(*, enabled: bool, contract_selected: bool) -> None:
    """Run the complete fail-closed Issue #355 destructive qualification gate."""
    if not enabled:
        return
    if not contract_selected:
        raise SystemExit("selected suite does not contain the Issue #355 identity contract")
    # The reviewed operator rollback intentionally admits only the exact Issue
    # #355 migration head. Later migrations must not weaken that production
    # guard, so rehearse this destructive gate at its own exact version and
    # restore the selected stack to the current repository head afterwards.
    reset_local_database(version=IDENTITY_QUALIFICATION_VERSION)
    qualification_error: Exception | None = None
    try:
        for script in IDENTITY_QUALIFICATION_SCRIPTS:
            run([sys.executable, script])
    except Exception as exc:
        qualification_error = exc
    try:
        reset_local_database()
    except Exception as restore_error:
        if qualification_error is not None:
            qualification_error.add_note(
                f"restoring the canonical migration head also failed: {restore_error}"
            )
            raise qualification_error
        raise
    if qualification_error is not None:
        raise qualification_error


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
    issue = fixture["source"].get("issue")
    if issue is None:
        return sorted((ROOT / "supabase/migrations").glob(f"{version}_*.sql"))
    try:
        issue_number = int(issue.rsplit("#", 1)[1])
        audit = json.loads(AUDIT_V2.read_text(encoding="utf-8"))
        source = audit["source"]
        completed = source["completedTransitions"]
        current = source["currentTransition"]
    except (FileNotFoundError, json.JSONDecodeError, KeyError, TypeError, ValueError, IndexError) as exc:
        raise SystemExit("SECURITY DEFINER audit-v2 transition state is invalid") from exc
    issue_marker = f"issue-{issue_number}-"
    if any(
        isinstance(item, dict) and issue_marker in str(item.get("batch", ""))
        for item in completed
    ):
        return []
    if issue_marker not in str(current.get("batch", "")):
        raise SystemExit(
            "SECURITY DEFINER fixture is neither the current nor a completed transition"
        )
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


def tracked_repository_files() -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE,
    )
    return sorted(line for line in result.stdout.splitlines() if line)


def stable_json_sha256(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def suite_evidence(suite_name: str, files: list[str], excluded_count: int) -> dict[str, object]:
    migration_versions = sorted(
        match.group(1)
        for path in (ROOT / "supabase/migrations").glob("*.sql")
        if (match := re.match(r"^(\d+)_", path.name))
    )
    if not migration_versions:
        raise SystemExit("repository has no numeric Supabase migrations")
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True,
        text=True, stdout=subprocess.PIPE,
    ).stdout.strip()
    worktree_dirty = bool(subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE,
    ).stdout.strip())
    cli_version = subprocess.run(
        ["supabase", "--version"], cwd=ROOT, check=True,
        text=True, stdout=subprocess.PIPE,
    ).stdout.strip()
    return {
        "suite": suite_name,
        "gitCommit": commit,
        "worktreeDirty": worktree_dirty,
        "migrationHead": migration_versions[-1],
        "supabaseCliVersion": cli_version,
        "manifestSha256": hashlib.sha256(MANIFEST.read_bytes()).hexdigest(),
        "selectedCount": len(files),
        "excludedCount": excluded_count,
        "filesSha256": stable_json_sha256(files),
        "files": files,
    }


def validate_manifest(
    manifest: dict, tracked_files: list[str],
) -> dict[str, list[str]]:
    classified: dict[str, list[str]] = {
        item["name"]: [] for item in manifest["classifications"]
    }
    errors: list[str] = []
    if manifest.get("schemaVersion") != "database-test-manifest.v2":
        errors.append("manifest schemaVersion must be database-test-manifest.v2")
    if len(classified) != len(manifest["classifications"]):
        errors.append("classification names must be unique")
    for path in tracked_files:
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
            if path not in tracked_files:
                errors.append(f"suite {suite_name}: missing tracked file {path}")
        excluded = suite.get("excludedFiles", {})
        for path, metadata in excluded.items():
            if path not in tracked_files:
                errors.append(f"suite {suite_name}: stale excluded path {path}")
            required = {"reason", "category", "disposition", "trackingIssue", "replacementFiles"}
            if not isinstance(metadata, dict) or set(metadata) != required:
                errors.append(
                    f"suite {suite_name}: exclusion metadata fields differ: {path}"
                )
                continue
            if not all(
                isinstance(metadata[field], str) and metadata[field].strip()
                for field in ("reason", "category", "disposition", "trackingIssue")
            ):
                errors.append(f"suite {suite_name}: exclusion metadata is incomplete: {path}")
            if not metadata["trackingIssue"].startswith("https://github.com/"):
                errors.append(f"suite {suite_name}: exclusion lacks a GitHub issue: {path}")
            replacements = metadata["replacementFiles"]
            if not isinstance(replacements, list) or len(replacements) != len(set(replacements)):
                errors.append(f"suite {suite_name}: replacement files differ: {path}")
                continue
            for replacement in replacements:
                if replacement not in tracked_files:
                    errors.append(
                        f"suite {suite_name}: missing exclusion replacement {replacement}: {path}"
                    )
                if replacement in excluded:
                    errors.append(
                        f"suite {suite_name}: replacement is itself excluded {replacement}: {path}"
                    )
        baseline = suite.get("exclusionBaseline")
        if excluded:
            if not isinstance(baseline, dict) or set(baseline) != {"count", "pathsSha256"}:
                errors.append(f"suite {suite_name}: exclusion baseline is missing")
            else:
                paths = sorted(excluded)
                if baseline["count"] != len(paths):
                    errors.append(f"suite {suite_name}: exclusion count grew or shrank")
                if baseline["pathsSha256"] != stable_json_sha256(paths):
                    errors.append(f"suite {suite_name}: exclusion path baseline differs")
        selected = suite.get("files") or [
            path for path in classified.get(suite.get("classification"), [])
            if path not in excluded
        ]
        if len(selected) != len(set(selected)):
            errors.append(f"suite {suite_name}: selected files are not unique")
    if errors:
        raise SystemExit("manifest classification failed:\n" + "\n".join(errors))
    return classified


def load_and_validate_manifest() -> tuple[dict, dict[str, list[str]]]:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    return manifest, validate_manifest(manifest, tracked_test_files())


def validate_activation_contract(
    suite_name: str, suite: dict, tracked_files: list[str], *, required: bool,
) -> dict[str, str] | None:
    activation = suite.get("activation")
    if activation is None:
        return {}
    expected_fields = {"requiredWhenPaths", "requiredPaths", "artifactPatterns"}
    if not isinstance(activation, dict) or set(activation) != expected_fields:
        raise SystemExit(f"suite {suite_name}: activation contract fields differ")
    for field in ("requiredWhenPaths", "requiredPaths"):
        values = activation[field]
        if (
            not isinstance(values, list) or not values
            or len(values) != len(set(values))
            or not all(isinstance(value, str) and value for value in values)
        ):
            raise SystemExit(f"suite {suite_name}: activation {field} is invalid")
    patterns = activation["artifactPatterns"]
    required_labels = {
        "freezeJson", "freezeSha256", "freezeSchema",
        "receiptJson", "receiptSha256", "receiptSchema",
        "apiPreExpandMigration", "physicalCutMigration",
    }
    if (
        not isinstance(patterns, dict) or set(patterns) != required_labels
        or not all(isinstance(value, str) and value for value in patterns.values())
        or len(patterns.values()) != len(set(patterns.values()))
    ):
        raise SystemExit(f"suite {suite_name}: activation artifactPatterns is invalid")
    activated_by = sorted(
        path for path in tracked_files
        if any(PurePosixPath(path).match(pattern) for pattern in activation["requiredWhenPaths"])
    )
    if not activated_by and not required:
        return None
    errors = [
        f"missing required activation path {path}"
        for path in activation["requiredPaths"] if path not in tracked_files
    ]
    artifacts: dict[str, str] = {}
    for label, pattern in patterns.items():
        matches = sorted(
            path for path in tracked_files if PurePosixPath(path).match(pattern)
        )
        if len(matches) != 1:
            errors.append(
                f"activation artifact {label} must match exactly once: {pattern}; "
                f"matches={matches}"
            )
        else:
            artifacts[label] = matches[0]
    versioned_labels = {
        "freezeJson", "freezeSha256", "freezeSchema",
        "receiptJson", "receiptSha256", "receiptSchema",
    }
    versions = {
        match.group(1)
        for label, path in artifacts.items() if label in versioned_labels
        if (match := re.search(r"\.v(\d+)(?:\.schema)?\.(?:json|sha256)$", path))
    }
    if len(artifacts.keys() & versioned_labels) == len(versioned_labels) and len(versions) != 1:
        errors.append(f"activation freeze/receipt artifact versions differ: {sorted(versions)}")
    if errors:
        reason = ", ".join(activated_by) if activated_by else "explicit invocation"
        raise SystemExit(
            f"suite {suite_name}: partial activation triggered by {reason}:\n"
            + "\n".join(errors)
        )
    return artifacts


def run_activation_verifiers(
    artifacts: dict[str, str], required_paths: list[str],
) -> None:
    if not artifacts:
        return
    verifier = ROOT / "scripts/freeze_issue_357_expand_manifest.py"
    runtime_paths = {
        f"required:{relative}": relative for relative in required_paths
    } | artifacts
    for label, relative in runtime_paths.items():
        path = ROOT / relative
        if not path.is_file() or path.is_symlink():
            raise SystemExit(
                f"Issue #357 activation {label} must be a regular non-symlink file: {relative}"
            )
    run([
        sys.executable, str(verifier), "check-freeze",
        "--freeze", artifacts["freezeJson"],
        "--sha256", artifacts["freezeSha256"],
        "--schema", artifacts["freezeSchema"],
    ])
    run([
        sys.executable, str(verifier), "check-receipt",
        "--receipt", artifacts["receiptJson"],
        "--receipt-sha256", artifacts["receiptSha256"],
        "--schema", artifacts["receiptSchema"],
        "--freeze", artifacts["freezeJson"],
        "--freeze-sha256", artifacts["freezeSha256"],
        "--require-authorized",
    ])
    run([
        sys.executable, str(verifier), "check-delivery",
        "--freeze", artifacts["freezeJson"],
        "--freeze-sha256", artifacts["freezeSha256"],
        "--freeze-schema", artifacts["freezeSchema"],
        "--receipt", artifacts["receiptJson"],
        "--receipt-sha256", artifacts["receiptSha256"],
        "--receipt-schema", artifacts["receiptSchema"],
        "--generator", "scripts/generate_issue_357_expand_sql.py",
        "--api-pre-expand-migration", artifacts["apiPreExpandMigration"],
        "--physical-cut-migration", artifacts["physicalCutMigration"],
        "--require-phase-authorization",
        "--require-exact-generated-bytes",
    ])
    run([
        sys.executable, "-m", "unittest",
        "scripts.test_freeze_issue_357_expand_manifest",
        "scripts.test_generate_issue_357_expand_sql",
        "scripts.test_issue_357_exposure_contract",
    ])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", default="canonical-local")
    parser.add_argument("--validate-manifest-only", action="store_true")
    parser.add_argument("--list", action="store_true", help="list the exact selected test files and exit")
    parser.add_argument(
        "--if-activated", action="store_true",
        help="exit successfully only when no activation path exists; partial activation fails closed",
    )
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
    manifest, classified = load_and_validate_manifest()
    tracked_files = tracked_repository_files()
    if args.suite not in manifest["suites"]:
        raise SystemExit(f"unknown suite: {args.suite}")
    suite = manifest["suites"][args.suite]
    activation_artifacts = validate_activation_contract(
        args.suite, suite, tracked_files, required=not args.if_activated,
    )
    if activation_artifacts is None:
        print(f"suite {args.suite}: not activated")
        return 0
    run_activation_verifiers(
        activation_artifacts, suite.get("activation", {}).get("requiredPaths", []),
    )
    files = suite.get("files") or [
        path for path in classified[suite["classification"]]
        if path not in suite.get("excludedFiles", {})
    ]
    if not files:
        raise SystemExit(f"suite {args.suite} selected no files")
    if args.validate_manifest_only:
        print("manifest classification passed")
        return 0
    if args.list:
        print(json.dumps(
            suite_evidence(args.suite, files, len(suite.get("excludedFiles", {}))),
            indent=2, sort_keys=True,
        ))
        return 0
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
        reset_local_database()
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
