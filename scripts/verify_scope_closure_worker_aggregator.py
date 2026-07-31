#!/usr/bin/env python3
"""Feed database-engine owner records to the exact Worker provider aggregator."""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any, Sequence


WORKER_SHA = "e5a7f769f4716266271eea53cb5233781635174f"
AGGREGATOR_PATH = "scripts/scope_closure_qualification.py"


class VerificationError(RuntimeError):
    pass


def _git(worker: Path, *args: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(worker), *args), check=False, capture_output=True, text=True
    )
    if completed.returncode != 0:
        raise VerificationError("exact Worker aggregator commit is unavailable")
    return completed.stdout


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise VerificationError("owner record is missing or invalid") from exc
    if not isinstance(value, dict):
        raise VerificationError("owner record must be an object")
    return value


def verify(worker: Path, records: Sequence[Path]) -> dict[str, Any]:
    _git(worker, "cat-file", "-e", f"{WORKER_SHA}^{{commit}}")
    source = _git(worker, "show", f"{WORKER_SHA}:{AGGREGATOR_PATH}")
    with tempfile.TemporaryDirectory(prefix="worker-provider-aggregator-") as tempdir:
        module_path = Path(tempdir) / "scope_closure_qualification.py"
        module_path.write_text(source, encoding="utf-8")
        spec = importlib.util.spec_from_file_location("exact_worker_aggregator", module_path)
        if spec is None or spec.loader is None:
            raise VerificationError("exact Worker aggregator could not be loaded")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        merged: dict[str, Any] = {}
        accepted: list[str] = []
        expected_run_id: str | None = None
        expected_sha: str | None = None
        for path in records:
            value = _load(path)
            owner = value.get("owner")
            if owner not in {"database", "storage"}:
                raise VerificationError("only database and storage owner records are accepted")
            run_id = value.get("runId")
            component_sha = value.get("componentSha")
            if expected_run_id is None:
                expected_run_id = run_id
                expected_sha = component_sha
            if run_id != expected_run_id or component_sha != expected_sha:
                raise VerificationError("owner records do not share one run and component SHA")
            child = module._validate_owner_result(
                value,
                owner=owner,
                component="database",
                component_sha=component_sha,
                run_id=run_id,
            )
            module._merge_evidence(merged, child["evidence"])
            accepted.append(owner)
        if sorted(accepted) != ["database", "storage"]:
            raise VerificationError("both database and storage owner records are required")
        return {
            "workerSha": WORKER_SHA,
            "acceptedOwners": sorted(accepted),
            "mergedSections": sorted(merged),
            "schemaAdaptation": False,
        }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-repo", required=True)
    parser.add_argument("records", nargs=2)
    args = parser.parse_args(argv)
    try:
        result = verify(
            Path(args.worker_repo).expanduser().resolve(),
            [Path(value).expanduser().resolve() for value in args.records],
        )
    except VerificationError as exc:
        print(f"verification failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
