#!/usr/bin/env python3
"""Validate Issue #408's exact registry of non-Worker database callers.

This contract is deliberately negative: every registered caller is owned by a
capability other than ``lca_worker_runtime``.  A source entry is accepted only
when its exact commit/path resolves to the recorded Git blob in the owning
repository.  Static source absence is never promoted to hosted consumer-zero.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = ROOT / "supabase" / "tests" / "contracts"
DEFAULT_REGISTRY = CONTRACT_DIR / "lca_worker_runtime_external_callers.v1.json"
DEFAULT_SCHEMA = CONTRACT_DIR / "lca_worker_runtime_external_callers.v1.schema.json"
DEFAULT_SHA256 = CONTRACT_DIR / "lca_worker_runtime_external_callers.v1.sha256"

SCHEMA_VERSION = "database.lca-worker-runtime-external-callers.v1"
WORKER_ROLE = "lca_worker_runtime"
FULL_SHA = re.compile(r"[0-9a-f]{40}")
SHA256 = re.compile(r"[0-9a-f]{64}")
SAFE_PATH = re.compile(r"(?!/)(?!.*(?:^|/)\.\.(?:/|$))[A-Za-z0-9_.@/+\-]+")

EXPECTED_CALLER_IDS = frozenset(
    {
        "edge-postgrest-service-runtime",
        "edge-direct-postgres-embedding-runtime",
        "release-actor-public-cli",
        "utilities-service-operator-runtime",
        "utilities-manual-investigation-runbook",
        "workspace-worker-identity-operator",
        "workspace-manual-database-runbooks",
        "database-pgtap-capability-fixtures",
        "database-python-runtime-capability-fixtures",
    }
)

EXPECTED_TOP_LEVEL = {
    "schemaVersion",
    "issue",
    "workerCapabilityRole",
    "contractRule",
    "callers",
}
EXPECTED_CALLER_KEYS = {
    "id",
    "identityOwner",
    "capabilityOwner",
    "transport",
    "runtimeRole",
    "includedInWorkerManifest",
    "exclusionReason",
    "dynamicSelectorDisposition",
    "hostedTelemetryRequired",
    "hostedTelemetryReason",
    "observedCapabilityReferences",
    "provenance",
}
EXPECTED_DYNAMIC_KEYS = {"classification", "status", "detail"}
EXPECTED_PROVENANCE_KEYS = {
    "repository",
    "commit",
    "path",
    "gitBlob",
    "classification",
}
ALLOWED_DYNAMIC_STATUSES = {
    "closed",
    "separately-tracked",
    "not-applicable",
    "test-only",
}
REFERENCE_ALLOWED_IDS = {
    "workspace-worker-identity-operator",
    "database-pgtap-capability-fixtures",
    "database-python-runtime-capability-fixtures",
}

REPOSITORY_DIRECTORY = {
    "linancn/tiangong-lca-edge-functions": "tiangong-lca-edge-functions",
    "chukeaa/tiangong-lca-release": "tiangong-lca-release",
    "tiangong-lca/utilities": "tiangong-lca-utilities",
    "tiangong-lca/workspace": ".",
    "tiangong-lca/database-engine": "database-engine",
}


class RegistryError(ValueError):
    """Raised when the external-caller registry is not exact."""


def _require_exact_keys(value: Any, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        raise RegistryError(f"{label} keys are not exact")
    return value


def _require_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RegistryError(f"{label} must be non-empty text")
    return value


def _validate_shape(document: Any) -> dict[str, Any]:
    root = _require_exact_keys(document, EXPECTED_TOP_LEVEL, "registry")
    if root["schemaVersion"] != SCHEMA_VERSION:
        raise RegistryError("registry schemaVersion differs")
    if root["issue"] != "tiangong-lca/database-engine#408":
        raise RegistryError("registry issue binding differs")
    if root["workerCapabilityRole"] != WORKER_ROLE:
        raise RegistryError("registry worker capability role differs")
    _require_text(root["contractRule"], "contractRule")
    if not isinstance(root["callers"], list) or not root["callers"]:
        raise RegistryError("callers must be a non-empty array")

    seen_ids: set[str] = set()
    for index, raw_caller in enumerate(root["callers"]):
        label = f"callers[{index}]"
        caller = _require_exact_keys(raw_caller, EXPECTED_CALLER_KEYS, label)
        caller_id = _require_text(caller["id"], f"{label}.id")
        if caller_id in seen_ids:
            raise RegistryError(f"duplicate caller id: {caller_id}")
        seen_ids.add(caller_id)

        identity_owner = _require_text(caller["identityOwner"], f"{label}.identityOwner")
        capability_owner = _require_text(
            caller["capabilityOwner"], f"{label}.capabilityOwner"
        )
        runtime_role = _require_text(caller["runtimeRole"], f"{label}.runtimeRole")
        _require_text(caller["transport"], f"{label}.transport")
        exclusion_reason = _require_text(
            caller["exclusionReason"], f"{label}.exclusionReason"
        )
        if caller["includedInWorkerManifest"] is not False:
            raise RegistryError(f"non-Worker caller included in Worker manifest: {caller_id}")
        if WORKER_ROLE in {identity_owner, capability_owner, runtime_role}:
            raise RegistryError(f"non-Worker caller reuses {WORKER_ROLE}: {caller_id}")

        dynamic = _require_exact_keys(
            caller["dynamicSelectorDisposition"], EXPECTED_DYNAMIC_KEYS,
            f"{label}.dynamicSelectorDisposition",
        )
        _require_text(dynamic["classification"], f"{label}.dynamicSelectorDisposition.classification")
        status = _require_text(dynamic["status"], f"{label}.dynamicSelectorDisposition.status")
        _require_text(dynamic["detail"], f"{label}.dynamicSelectorDisposition.detail")
        if status not in ALLOWED_DYNAMIC_STATUSES:
            raise RegistryError(f"dynamic selector remains unclassified: {caller_id}")

        if type(caller["hostedTelemetryRequired"]) is not bool:
            raise RegistryError(f"hostedTelemetryRequired must be boolean: {caller_id}")
        _require_text(caller["hostedTelemetryReason"], f"{label}.hostedTelemetryReason")

        references = caller["observedCapabilityReferences"]
        if not isinstance(references, list) or any(
            not isinstance(item, str) or not item for item in references
        ) or len(references) != len(set(references)):
            raise RegistryError(f"observed capability references are invalid: {caller_id}")
        if WORKER_ROLE in references and caller_id not in REFERENCE_ALLOWED_IDS:
            raise RegistryError(
                f"only deployment/test evidence may reference {WORKER_ROLE}: {caller_id}"
            )
        if WORKER_ROLE in references and not any(
            token in exclusion_reason.lower() for token in ("operator", "test", "fixture")
        ):
            raise RegistryError(f"capability reference lacks non-runtime disposition: {caller_id}")

        provenance = caller["provenance"]
        if not isinstance(provenance, list) or not provenance:
            raise RegistryError(f"provenance is empty: {caller_id}")
        seen_sources: set[tuple[str, str, str]] = set()
        for source_index, raw_source in enumerate(provenance):
            source_label = f"{label}.provenance[{source_index}]"
            source = _require_exact_keys(
                raw_source, EXPECTED_PROVENANCE_KEYS, source_label
            )
            repository = _require_text(source["repository"], f"{source_label}.repository")
            commit = _require_text(source["commit"], f"{source_label}.commit")
            path = _require_text(source["path"], f"{source_label}.path")
            blob = _require_text(source["gitBlob"], f"{source_label}.gitBlob")
            _require_text(source["classification"], f"{source_label}.classification")
            if repository not in REPOSITORY_DIRECTORY:
                raise RegistryError(f"unregistered source repository: {repository}")
            if FULL_SHA.fullmatch(commit) is None or FULL_SHA.fullmatch(blob) is None:
                raise RegistryError(f"source commit/blob is not exact: {source_label}")
            if SAFE_PATH.fullmatch(path) is None:
                raise RegistryError(f"source path is unsafe: {source_label}")
            source_key = (repository, commit, path)
            if source_key in seen_sources:
                raise RegistryError(f"duplicate source provenance: {source_label}")
            seen_sources.add(source_key)

    if seen_ids != EXPECTED_CALLER_IDS:
        missing = sorted(EXPECTED_CALLER_IDS - seen_ids)
        extra = sorted(seen_ids - EXPECTED_CALLER_IDS)
        raise RegistryError(f"caller census differs: missing={missing}, extra={extra}")
    return root


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode:
        raise RegistryError(
            f"git readback failed for {repo}: {' '.join(args)}: {result.stderr.strip()}"
        )
    return result.stdout.strip()


def discover_workspace_root() -> Path | None:
    candidates = [ROOT, *ROOT.parents]
    try:
        common_dir = Path(_git(ROOT, "rev-parse", "--git-common-dir")).resolve()
        candidates.extend([common_dir, *common_dir.parents])
    except RegistryError:
        pass
    for candidate in candidates:
        if (
            (candidate / ".gitmodules").is_file()
            and (candidate / "database-engine").exists()
            and (candidate / "tiangong-lca-edge-functions").exists()
        ):
            return candidate.resolve()
    return None


def repository_roots(workspace_root: Path | None = None) -> dict[str, Path]:
    workspace = workspace_root.resolve() if workspace_root else discover_workspace_root()
    if workspace is None:
        raise RegistryError(
            "workspace root is required for exact cross-repository source readback"
        )
    roots: dict[str, Path] = {}
    for repository, relative in REPOSITORY_DIRECTORY.items():
        candidate = workspace if relative == "." else workspace / relative
        if repository == "tiangong-lca/database-engine" and not candidate.exists():
            candidate = ROOT
        if not candidate.exists():
            raise RegistryError(f"source repository checkout is missing: {repository}")
        roots[repository] = candidate.resolve()
    return roots


def validate_source_readback(
    document: Mapping[str, Any], repo_roots: Mapping[str, Path]
) -> None:
    for caller in document["callers"]:
        for source in caller["provenance"]:
            repository = source["repository"]
            repo = repo_roots.get(repository)
            if repo is None:
                raise RegistryError(f"source repository mapping is missing: {repository}")
            observed_commit = _git(repo, "rev-parse", f"{source['commit']}^{{commit}}")
            if observed_commit != source["commit"]:
                raise RegistryError(
                    f"source commit drifted: {repository}@{source['commit']}"
                )
            try:
                observed_blob = _git(
                    repo, "rev-parse", f"{source['commit']}:{source['path']}"
                )
            except RegistryError as exc:
                raise RegistryError(
                    f"source path drifted: {repository}@{source['commit']}:{source['path']}"
                ) from exc
            if observed_blob != source["gitBlob"]:
                raise RegistryError(
                    "source blob drifted: "
                    f"{repository}@{source['commit']}:{source['path']} "
                    f"expected={source['gitBlob']} observed={observed_blob}"
                )


def validate_registry_files(
    registry_path: Path = DEFAULT_REGISTRY,
    schema_path: Path = DEFAULT_SCHEMA,
    sha256_path: Path = DEFAULT_SHA256,
    *,
    repo_roots: Mapping[str, Path] | None = None,
    require_source_readback: bool = True,
) -> dict[str, Any]:
    registry_bytes = registry_path.read_bytes()
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    if schema.get("$id") != SCHEMA_VERSION or schema.get("additionalProperties") is not False:
        raise RegistryError("registry JSON Schema identity/closure differs")
    document = _validate_shape(json.loads(registry_bytes))

    recorded_hash = sha256_path.read_text(encoding="ascii").strip()
    observed_hash = hashlib.sha256(registry_bytes).hexdigest()
    if SHA256.fullmatch(recorded_hash) is None or recorded_hash != observed_hash:
        raise RegistryError(
            f"registry sha256 differs: expected={recorded_hash!r} observed={observed_hash}"
        )

    if require_source_readback:
        validate_source_readback(document, repo_roots or repository_roots())
    return document


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    parser.add_argument("--sha256", type=Path, default=DEFAULT_SHA256)
    parser.add_argument("--workspace-root", type=Path)
    parser.add_argument(
        "--artifact-only",
        action="store_true",
        help="validate only registry shape/schema/hash; this is not exact source qualification",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        roots = None if args.artifact_only else repository_roots(args.workspace_root)
        document = validate_registry_files(
            args.registry,
            args.schema,
            args.sha256,
            repo_roots=roots,
            require_source_readback=not args.artifact_only,
        )
    except (OSError, json.JSONDecodeError, RegistryError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True))
        return 1
    print(
        json.dumps(
            {
                "ok": True,
                "schemaVersion": document["schemaVersion"],
                "callerCount": len(document["callers"]),
                "sourceCount": sum(len(row["provenance"]) for row in document["callers"]),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
