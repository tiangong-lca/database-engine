#!/usr/bin/env python3
"""Fail-closed local database contract runner."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "supabase/tests/manifest.json"


def supabase_command(*args: str) -> list[str]:
    command = ["supabase"]
    if workdir := os.environ.get("SUPABASE_WORKDIR"):
        command.extend(["--workdir", workdir])
    command.extend(args)
    return command


def run(command: list[str]) -> None:
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def check_lint() -> None:
    command = supabase_command("db", "lint")
    if (db_url := os.environ.get("DATABASE_URL")) and not os.environ.get("SUPABASE_WORKDIR"):
        command.extend(["--db-url", db_url])
    else:
        command.append("--local")
    command.extend(["--level", "warning", "--fail-on", "none"])
    print("+", " ".join(command), flush=True)
    result = subprocess.run(command, cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE)
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
    args = parser.parse_args()
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
    if not args.skip_reset:
        run(["supabase", "db", "reset", "--local"])
    test_command = supabase_command("test", "db", *files)
    if (db_url := os.environ.get("DATABASE_URL")) and not os.environ.get("SUPABASE_WORKDIR"):
        test_command.extend(["--db-url", db_url])
    else:
        test_command.append("--local")
    run(test_command)
    if not args.skip_data_api:
        run([sys.executable, "scripts/test_worker_control_plane_data_api.py"])
    if not args.skip_lint:
        # CLI defaults to exit zero even when it prints ERROR diagnostics.
        check_lint()
    run([sys.executable, "scripts/export_database_contract.py", "--check"])
    run([sys.executable, "scripts/schema_boundary_phase.py"])
    run([sys.executable, "scripts/public_inventory_closure.py", "--check"])
    run([
        "git", "diff", "--exit-code", "--",
        "supabase/workspace/remote_schema.sql",
        "supabase/workspace/global",
        "supabase/workspace/schemas",
    ])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
