#!/usr/bin/env python3
"""Exact-Git-tree consumer evidence for database-engine Issue #390.

The scanner never reads consumer worktree files.  Every input is a full commit
SHA, and every source byte is read from the corresponding Git object database.
Only hashes and structural classifications are retained in the checked
artifact; matching source lines are never copied into evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

import jsonschema


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "supabase/tests/contracts/lca_result_family_pre_ddl.v1.json"
ARTIFACT_PATH = (
    ROOT / "supabase/tests/contracts/lca_result_family_external_git_tree.v1.json"
)
ARTIFACT_SHA_PATH = ARTIFACT_PATH.with_suffix(".sha256")
ARTIFACT_SCHEMA_PATH = ARTIFACT_PATH.with_suffix(".schema.json")

SCHEMA_VERSION = "lca-result-family-external-git-tree.v1"
PATH_RULE_VERSION = "issue-390-external-path-rules.v1"
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
REGULAR_MODES = {"100644", "100755"}

PHYSICAL_TARGETS = (
    "lca_factorization_registry",
    "lca_latest_all_unit_results",
    "lca_result_cache",
    "lca_results",
)
LEGACY_ROUTINES = (
    "lca_read_job_projection",
    "lca_read_latest_single_solve_result",
    "lca_read_result_projection",
)
STABLE_API_ROUTINES = (
    "cmd_lca_admit_result_cache_v1",
    "cmd_lca_reconcile_result_cache_v1",
    "cmd_lca_touch_result_cache_v1",
    "lca_read_job_projection_v1",
    "lca_read_latest_all_unit_result_v1",
    "lca_read_latest_single_solve_result_v1",
    "lca_read_result_cache_v1",
    "lca_read_result_projection_v1",
)
TOKEN_CLASSES = {
    **{name: "physical-relation" for name in PHYSICAL_TARGETS},
    **{name: "legacy-routine" for name in LEGACY_ROUTINES},
    **{name: "stable-api-routine" for name in STABLE_API_ROUTINES},
}
TOKEN_PATTERN = re.compile(
    rb"(?<![A-Za-z0-9_])("
    + b"|".join(
        re.escape(name.encode("ascii"))
        for name in sorted(TOKEN_CLASSES, key=len, reverse=True)
    )
    + rb")(?![A-Za-z0-9_])",
    re.IGNORECASE,
)

DIRECT_TRANSPORTS = {
    "data-api-relation",
    "legacy-rpc",
    "legacy-rpc-selector",
    "raw-sql-relation",
    "sql-relation",
}


class EvidenceError(ValueError):
    """An exact-tree input or checked artifact is invalid."""


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
            separators=(",", ": "),
        ).encode("utf-8")
        + b"\n"
    )


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def run_git(repo: Path, *arguments: str, text: bool = True) -> str | bytes:
    result = subprocess.run(
        ["git", "-C", str(repo), *arguments],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=text,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() if text else result.stderr.decode(errors="replace").strip()
        raise EvidenceError(
            f"git {' '.join(arguments)} failed for {repo}: {stderr or 'unknown error'}"
        )
    return result.stdout


def normalize_remote(value: str) -> str:
    normalized = value.strip().removesuffix(".git")
    normalized = re.sub(r"^git@github\.com:", "github.com/", normalized)
    normalized = re.sub(r"^ssh://git@github\.com/", "github.com/", normalized)
    normalized = re.sub(r"^https?://github\.com/", "github.com/", normalized)
    return normalized.lower()


def validate_repo_identity(repo: Path, canonical_repo: str) -> None:
    if repo.is_symlink() or not repo.is_dir():
        raise EvidenceError(f"consumer repository path is not a safe directory: {repo}")
    remote = str(run_git(repo, "config", "--get", "remote.origin.url")).strip()
    expected = f"github.com/{canonical_repo}".lower()
    if normalize_remote(remote) != expected:
        raise EvidenceError(
            f"consumer repository origin mismatch for {repo}: "
            f"expected {expected}, got {normalize_remote(remote)}"
        )


def commit_tree(repo: Path, commit: str) -> str:
    if not FULL_SHA.fullmatch(commit):
        raise EvidenceError(f"consumer commit is not a full lowercase SHA: {commit!r}")
    run_git(repo, "cat-file", "-e", f"{commit}^{{commit}}")
    tree = str(run_git(repo, "rev-parse", f"{commit}^{{tree}}")).strip()
    if not FULL_SHA.fullmatch(tree):
        raise EvidenceError(f"commit {commit} did not resolve to an exact tree")
    return tree


def tree_entries(repo: Path, commit: str) -> list[dict[str, str]]:
    raw = run_git(repo, "ls-tree", "-r", "-z", commit, text=False)
    assert isinstance(raw, bytes)
    entries: list[dict[str, str]] = []
    for record in raw.split(b"\0"):
        if not record:
            continue
        metadata, separator, encoded_path = record.partition(b"\t")
        if not separator:
            raise EvidenceError(f"malformed ls-tree record in {commit}")
        parts = metadata.decode("ascii").split()
        if len(parts) != 3:
            raise EvidenceError(f"malformed ls-tree metadata in {commit}")
        mode, object_type, blob = parts
        path = encoded_path.decode("utf-8", errors="surrogateescape")
        entries.append(
            {"mode": mode, "objectType": object_type, "blob": blob, "path": path}
        )
    return sorted(entries, key=lambda row: row["path"])


def read_blob(repo: Path, blob: str) -> bytes:
    value = run_git(repo, "cat-file", "blob", blob, text=False)
    assert isinstance(value, bytes)
    return value


def surface_for(repo_key: str, path: str) -> tuple[str, str] | None:
    """Return source kind and execution reachability for a matched path."""

    candidate = PurePosixPath(path)
    parts = candidate.parts
    first = parts[0] if parts else ""

    if repo_key == "next":
        if path.startswith("docker/volumes/functions/"):
            return "generated-mirror", "active-runtime"
        if path == "docker/volumes/db/init/data.sql":
            return "generated-snapshot", "active-runtime"
        if path.startswith("docker/volumes/db/init-scripts/"):
            return "native", "active-runtime"
        if first == "src":
            return "native", "active-runtime"
        if first in {"test", "tests"}:
            return "native", "test-only"
        if first == "docker":
            return "native", "operator-only"
        if first == "docs" or path.startswith("README") or path.endswith(".md"):
            return "historical", "inert-historical"
        return None

    if repo_key == "edge":
        if path.startswith("supabase/functions/"):
            return "native", "active-runtime"
        if first == "test":
            return "native", "test-only"
        if first == "scripts" or path.startswith("supabase/.env") or path == "test.example.http":
            return "native", "operator-only"
        if first == "docs" or path.startswith("README") or path.endswith(".md"):
            return "historical", "inert-historical"
        return None

    if repo_key == "worker":
        if first in {"src", "crates", "tools"}:
            return "native", "active-runtime"
        if first in {"test", "tests"}:
            return "native", "test-only"
        if path.startswith("supabase/migrations/") or first == "docs" or path.endswith(".md"):
            return "historical", "inert-historical"
        if first in {"scripts", "supabase", ".github"}:
            return "native", "operator-only"
        return None

    if repo_key == "utilities":
        if first == "src":
            return "native", "active-runtime"
        if first in {"test", "tests"}:
            return "native", "test-only"
        if first in {"contracts", "runbooks", "scripts"}:
            return "native", "operator-only"
        if first == "docs" or path.startswith("README") or path.endswith(".md"):
            return "historical", "inert-historical"
        return None

    if first in {"src", "app", "supabase", "packages"}:
        return "native", "active-runtime"
    if first in {"test", "tests", "fixtures"}:
        return "native", "test-only"
    if first in {"scripts", "contracts", "runbooks", ".github", "docker"}:
        return "native", "operator-only"
    if first == "docs" or path.startswith("README") or path.endswith(".md"):
        return "historical", "inert-historical"
    return None


def transport_for(
    line: bytes, token: str, token_class: str, *, repo_key: str, path: str
) -> str:
    text = line.decode("utf-8", errors="replace")
    escaped = re.escape(token)
    if re.search(rf"(?:storage|bucket)[^\n]{{0,100}}{escaped}", text, re.IGNORECASE):
        return "storage-token"
    if re.search(rf"/functions/v1/{escaped}\b", text, re.IGNORECASE):
        return "http-function-route"
    if token_class == "physical-relation" and re.search(
        rf"\.from\s*\(\s*['\"`]((?:public|private)\.)?{escaped}['\"`]",
        text,
        re.IGNORECASE,
    ):
        return "data-api-relation"
    if token_class in {"legacy-routine", "stable-api-routine"} and re.search(
        rf"\.rpc\s*\(\s*['\"`]{escaped}['\"`]", text, re.IGNORECASE
    ):
        return "legacy-rpc" if token_class == "legacy-routine" else "stable-api-rpc"
    if token_class == "physical-relation" and re.search(
        rf"\b(?:from|join|update|into|table|references|delete\s+from)\s+"
        rf"(?:(?:public|private)\s*\.\s*)?{escaped}\b",
        text,
        re.IGNORECASE,
    ):
        return "sql-relation"
    if token_class in {"legacy-routine", "stable-api-routine"} and re.search(
        rf"\b(?:create|alter|comment\s+on|drop)\s+function\s+"
        rf"(?:(?:public|private|api)\s*\.\s*)?{escaped}\b",
        text,
        re.IGNORECASE,
    ):
        return "sql-routine-definition"
    if token_class == "legacy-routine" and re.search(
        rf"['\"`]{escaped}['\"`]", text, re.IGNORECASE
    ):
        return "legacy-rpc-selector"
    if token_class == "physical-relation" and re.search(
        rf"['\"`](?:public|private)\.{escaped}['\"`]", text, re.IGNORECASE
    ):
        if (
            repo_key == "edge"
            and path
            == "supabase/functions/_shared/capabilities/lca_result_family.ts"
            and re.fullmatch(
                rf"\s*['\"`]public\.{escaped}['\"`]\s*,?\s*",
                text,
                re.IGNORECASE,
            )
        ):
            return "legacy-boundary-contract-metadata"
        return "raw-sql-relation"
    if token_class == "stable-api-routine":
        return "stable-api-capability-token"
    return "lexical-candidate"


def dynamic_selector_lines(data: bytes) -> Iterable[tuple[int, bytes, str]]:
    patterns = {
        "dynamic-from": re.compile(rb"\.from\s*\(\s*(?!['\"`])"),
        "dynamic-rpc": re.compile(rb"\.rpc\s*\(\s*(?!['\"`])"),
    }
    for line_number, line in enumerate(data.splitlines(), 1):
        for kind, pattern in patterns.items():
            if pattern.search(line):
                yield line_number, line, kind


def evidence_for_blob(
    *, repo_key: str, repo_name: str, commit: str, entry: dict[str, str], data: bytes
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    matches: list[dict[str, Any]] = []
    dynamics: list[dict[str, Any]] = []
    surface = surface_for(repo_key, entry["path"])
    lines = data.splitlines()
    for line_number, line in enumerate(lines, 1):
        for match in TOKEN_PATTERN.finditer(line):
            token = match.group(1).decode("ascii").lower()
            token_class = TOKEN_CLASSES[token]
            source_kind, reachability = surface or ("unclassified", "unclassified")
            matches.append(
                {
                    "repo": repo_name,
                    "commit": commit,
                    "blob": entry["blob"],
                    "path": entry["path"],
                    "line": line_number,
                    "token": token,
                    "tokenClass": token_class,
                    "transport": transport_for(
                        line,
                        token,
                        token_class,
                        repo_key=repo_key,
                        path=entry["path"],
                    ),
                    "sourceKind": source_kind,
                    "executionReachability": reachability,
                    "lineSha256": sha256_bytes(line.strip()),
                    "binary": b"\0" in data,
                }
            )
    if surface and surface[1] == "active-runtime":
        for line_number, line, kind in dynamic_selector_lines(data):
            dynamics.append(
                {
                    "repo": repo_name,
                    "commit": commit,
                    "blob": entry["blob"],
                    "path": entry["path"],
                    "line": line_number,
                    "kind": kind,
                    "lineSha256": sha256_bytes(line.strip()),
                }
            )
    return matches, dynamics


def ledger_digest(entries: list[dict[str, Any]]) -> str:
    rows = [
        {
            "path": row["path"],
            "mode": row["mode"],
            "objectType": row["objectType"],
            "blob": row["blob"],
            "size": row.get("size"),
        }
        for row in entries
    ]
    return sha256_bytes(canonical_bytes(rows))


def scan_repository(workspace_root: Path, spec: dict[str, Any]) -> dict[str, Any]:
    repo_key = spec["key"]
    repo_name = spec["repository"]
    commit = spec["commit"]
    repo = workspace_root / spec["workspacePath"]
    validate_repo_identity(repo, repo_name)
    tree = commit_tree(repo, commit)
    entries = tree_entries(repo, commit)
    evidence: list[dict[str, Any]] = []
    dynamic: list[dict[str, Any]] = []
    unsupported: list[dict[str, str]] = []
    scanned: list[dict[str, Any]] = []
    for entry in entries:
        if entry["mode"] not in REGULAR_MODES or entry["objectType"] != "blob":
            unsupported.append(entry)
            scanned.append({**entry, "size": None})
            continue
        data = read_blob(repo, entry["blob"])
        scanned.append({**entry, "size": len(data)})
        blob_evidence, blob_dynamic = evidence_for_blob(
            repo_key=repo_key,
            repo_name=repo_name,
            commit=commit,
            entry=entry,
            data=data,
        )
        evidence.extend(blob_evidence)
        dynamic.extend(blob_dynamic)

    evidence.sort(
        key=lambda row: (row["path"], row["line"], row["token"], row["transport"])
    )
    dynamic.sort(key=lambda row: (row["path"], row["line"], row["kind"]))
    unsupported.sort(key=lambda row: row["path"])
    blockers: list[dict[str, Any]] = []
    for row in evidence:
        if row["sourceKind"] == "unclassified":
            blockers.append(
                {
                    "kind": "unclassified-matched-path",
                    "path": row["path"],
                    "blob": row["blob"],
                }
            )
        if row["binary"]:
            blockers.append(
                {"kind": "binary-target-match", "path": row["path"], "blob": row["blob"]}
            )
        if (
            row["executionReachability"] == "active-runtime"
            and row["tokenClass"] in {"physical-relation", "legacy-routine"}
            and row["transport"] == "lexical-candidate"
        ):
            blockers.append(
                {
                    "kind": "active-runtime-semantic-review-required",
                    "path": row["path"],
                    "blob": row["blob"],
                    "line": row["line"],
                    "token": row["token"],
                }
            )
    for row in unsupported:
        blockers.append(
            {
                "kind": "unsupported-tree-entry",
                "path": row["path"],
                "mode": row["mode"],
                "objectType": row["objectType"],
            }
        )
    for row in dynamic:
        blockers.append(
            {
                "kind": "dynamic-selector-review-required",
                "path": row["path"],
                "blob": row["blob"],
                "line": row["line"],
                "selectorKind": row["kind"],
            }
        )

    aggregate = Counter(
        (
            row["sourceKind"],
            row["executionReachability"],
            row["tokenClass"],
            row["transport"],
        )
        for row in evidence
    )
    recognized_runtime_direct = sum(
        1
        for row in evidence
        if row["executionReachability"] == "active-runtime"
        and row["tokenClass"] in {"physical-relation", "legacy-routine"}
        and row["transport"] in DIRECT_TRANSPORTS
    )
    unique_blockers = sorted(
        {canonical_bytes(row): row for row in blockers}.values(),
        key=lambda row: canonical_bytes(row),
    )
    return {
        "key": repo_key,
        "repository": repo_name,
        "workspacePath": spec["workspacePath"],
        "commit": commit,
        "tree": tree,
        "treeEntryCount": len(entries),
        "scannedRegularBlobCount": len(scanned) - len(unsupported),
        "treeLedgerSha256": ledger_digest(scanned),
        "unsupportedEntries": unsupported,
        "evidence": evidence,
        "dynamicSelectorCandidates": dynamic,
        "aggregates": [
            {
                "sourceKind": key[0],
                "executionReachability": key[1],
                "tokenClass": key[2],
                "transport": key[3],
                "count": count,
            }
            for key, count in sorted(aggregate.items())
        ],
        "recognizedRuntimeDirectLegacyOccurrenceCount": recognized_runtime_direct,
        "unresolvedActiveRuntimeEvidenceCount": sum(
            1
            for row in unique_blockers
            if row["kind"]
            in {
                "active-runtime-semantic-review-required",
                "dynamic-selector-review-required",
            }
        ),
        "blockers": unique_blockers,
    }


def prefixed_tree(repo: Path, commit: str, prefix: str) -> dict[str, str]:
    normalized = prefix.rstrip("/") + "/"
    result: dict[str, str] = {}
    for entry in tree_entries(repo, commit):
        if not entry["path"].startswith(normalized):
            continue
        relative = entry["path"][len(normalized) :]
        if not relative:
            continue
        result[relative] = f"{entry['mode']}:{entry['objectType']}:{entry['blob']}"
    return result


def mirror_evidence(
    workspace_root: Path, specs: dict[str, dict[str, Any]], scanned: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    next_spec, edge_spec = specs["next"], specs["edge"]
    next_repo = workspace_root / next_spec["workspacePath"]
    edge_repo = workspace_root / edge_spec["workspacePath"]
    receipt_path = "docker/volumes/functions/.source-revision.json"
    receipt_blob = str(
        run_git(next_repo, "rev-parse", f"{next_spec['commit']}:{receipt_path}")
    ).strip()
    receipt = json.loads(read_blob(next_repo, receipt_blob))
    source_commit = receipt.get("commit")
    source_path = receipt.get("sourcePath")
    if normalize_remote(str(receipt.get("repository", ""))) != normalize_remote(
        f"https://github.com/{edge_spec['repository']}.git"
    ):
        raise EvidenceError("Next Edge mirror receipt repository is not canonical")
    if source_path != "supabase/functions" or not isinstance(source_commit, str):
        raise EvidenceError("Next Edge mirror receipt has an invalid source binding")
    commit_tree(edge_repo, source_commit)
    mirror = prefixed_tree(
        next_repo, next_spec["commit"], "docker/volumes/functions"
    )
    mirror.pop(".source-revision.json", None)
    source = prefixed_tree(edge_repo, source_commit, source_path)
    missing = sorted(set(source) - set(mirror))
    extra = sorted(set(mirror) - set(source))
    drift = sorted(path for path in set(source) & set(mirror) if source[path] != mirror[path])
    source_digest = sha256_bytes(canonical_bytes(source))
    mirror_digest = sha256_bytes(canonical_bytes(mirror))
    current_edge = edge_spec["commit"]
    blockers: list[dict[str, Any]] = []
    if missing or extra or drift or source_digest != mirror_digest:
        blockers.append(
            {
                "kind": "next-edge-mirror-source-parity-failed",
                "missingCount": len(missing),
                "extraCount": len(extra),
                "driftCount": len(drift),
            }
        )
    if source_commit != current_edge:
        blockers.append(
            {
                "kind": "next-edge-mirror-stale-current-edge",
                "mirrorSourceCommit": source_commit,
                "currentEdgeCommit": current_edge,
            }
        )
    mirror_rows = [
        row
        for row in scanned["next"]["evidence"]
        if row["sourceKind"] == "generated-mirror"
    ]
    direct_count = sum(
        1
        for row in mirror_rows
        if row["tokenClass"] in {"physical-relation", "legacy-routine"}
        and row["transport"] in DIRECT_TRANSPORTS
    )
    if direct_count:
        blockers.append(
            {"kind": "next-edge-mirror-active-legacy-consumers", "count": direct_count}
        )
    return {
        "receiptPath": receipt_path,
        "receiptBlob": receipt_blob,
        "sourceRepository": edge_spec["repository"],
        "sourceCommit": source_commit,
        "sourcePath": source_path,
        "sourceTreeLedgerSha256": source_digest,
        "mirrorTreeLedgerSha256": mirror_digest,
        "missingPaths": missing,
        "extraPaths": extra,
        "driftPaths": drift,
        "matchesReceiptSource": not (missing or extra or drift) and source_digest == mirror_digest,
        "matchesCurrentEdgeCommit": source_commit == current_edge,
        "recognizedRuntimeDirectLegacyOccurrenceCount": direct_count,
        "unresolvedActiveRuntimeEvidenceCount": sum(
            1
            for row in scanned["next"]["blockers"]
            if row.get("path", "").startswith("docker/volumes/functions/")
            and row["kind"]
            in {
                "active-runtime-semantic-review-required",
                "dynamic-selector-review-required",
            }
        ),
        "blockers": blockers,
    }


def snapshot_evidence(
    workspace_root: Path, specs: dict[str, dict[str, Any]], scanned: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    next_spec = specs["next"]
    next_repo = workspace_root / next_spec["workspacePath"]
    path = "docker/volumes/db/init/data.sql"
    blob = str(run_git(next_repo, "rev-parse", f"{next_spec['commit']}:{path}")).strip()
    receipt_path = "docker/volumes/db/init/.source-revision.json"
    receipt_exists = subprocess.run(
        ["git", "-C", str(next_repo), "cat-file", "-e", f"{next_spec['commit']}:{receipt_path}"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ).returncode == 0
    rows = [row for row in scanned["next"]["evidence"] if row["path"] == path]
    public_family_count = sum(
        1
        for row in rows
        if row["tokenClass"] in {"physical-relation", "legacy-routine"}
        and row["transport"] in DIRECT_TRANSPORTS | {"sql-routine-definition"}
    )
    blockers = []
    if not receipt_exists:
        blockers.append(
            {"kind": "next-db-snapshot-missing-exact-database-source-receipt"}
        )
    if public_family_count:
        blockers.append(
            {
                "kind": "next-db-snapshot-active-public-result-family",
                "count": public_family_count,
            }
        )
    return {
        "path": path,
        "blob": blob,
        "sourceKind": "generated-snapshot",
        "executionReachability": "active-runtime",
        "exactSourceReceiptPath": receipt_path,
        "exactSourceReceiptPresent": receipt_exists,
        "publicFamilyEvidenceCount": public_family_count,
        "blockers": blockers,
    }


def database_current_evidence(contract: dict[str, Any]) -> dict[str, Any]:
    source = contract["externalGitTreeGate"]["databaseCurrentEvidence"]
    commit = source["commit"]
    commit_tree(ROOT, commit)
    migrations = [
        row["path"]
        for row in tree_entries(ROOT, commit)
        if row["path"].startswith("supabase/migrations/") and row["path"].endswith(".sql")
    ]
    if not migrations:
        raise EvidenceError("database current evidence commit has no migrations")
    migration_head = max(PurePosixPath(path).name.split("_", 1)[0] for path in migrations)
    catalog_path = "supabase/tests/contracts/database_catalog.json"
    catalog_sha_path = "supabase/tests/contracts/database_catalog.sha256"
    catalog_blob = str(run_git(ROOT, "rev-parse", f"{commit}:{catalog_path}")).strip()
    catalog = read_blob(ROOT, catalog_blob)
    sidecar = str(run_git(ROOT, "show", f"{commit}:{catalog_sha_path}")).strip()
    digest = sha256_bytes(catalog)
    if sidecar != digest:
        raise EvidenceError("database current catalog sidecar does not match exact blob")
    parsed = json.loads(catalog)
    facade_names = sorted(
        row["name"]
        for row in parsed["functions"]
        if row["schema"] == "api" and row["name"] in STABLE_API_ROUTINES
    )
    if facade_names != sorted(STABLE_API_ROUTINES):
        raise EvidenceError("database current evidence does not contain exact eight facades")
    public_relations = sorted(
        row["name"]
        for row in parsed["relations"]
        if row["schema"] == "public" and row["name"] in PHYSICAL_TARGETS
    )
    public_routines = sorted(
        row["name"]
        for row in parsed["functions"]
        if row["schema"] == "public" and row["name"] in LEGACY_ROUTINES
    )
    return {
        "commit": commit,
        "tree": str(run_git(ROOT, "rev-parse", f"{commit}^{{tree}}")).strip(),
        "migrationHead": migration_head,
        "catalogPath": catalog_path,
        "catalogBlob": catalog_blob,
        "catalogSha256": digest,
        "stableApiFacadeNames": facade_names,
        "publicPhysicalTargetNames": public_relations,
        "publicLegacyRoutineNames": public_routines,
        "physicalTargetsRemainAtFrozenPublicBoundary": (
            public_relations == sorted(PHYSICAL_TARGETS)
            and public_routines == sorted(LEGACY_ROUTINES)
        ),
    }


def build_artifact(workspace_root: Path, contract: dict[str, Any]) -> dict[str, Any]:
    gate = contract["externalGitTreeGate"]
    if gate["pathRuleVersion"] != PATH_RULE_VERSION:
        raise EvidenceError("pre-DDL contract path-rule version is unsupported")
    specs = {row["key"]: row for row in gate["repositories"]}
    expected_keys = {"edge", "worker", "utilities", "next", "cli", "mcp", "release", "dataFoundry"}
    if set(specs) != expected_keys:
        raise EvidenceError("external repository input set is not exact")
    repositories = [scan_repository(workspace_root, specs[key]) for key in sorted(specs)]
    scanned = {row["key"]: row for row in repositories}
    mirror = mirror_evidence(workspace_root, specs, scanned)
    snapshot = snapshot_evidence(workspace_root, specs, scanned)
    database = database_current_evidence(contract)
    blockers = [
        {"repository": row["repository"], **blocker}
        for row in repositories
        for blocker in row["blockers"]
    ]
    blockers.extend({"repository": specs["next"]["repository"], **row} for row in mirror["blockers"])
    blockers.extend({"repository": specs["next"]["repository"], **row} for row in snapshot["blockers"])
    artifact = {
        "schemaVersion": SCHEMA_VERSION,
        "issue": "tiangong-lca/database-engine#397",
        "parentIssue": contract["issue"],
        "pathRuleVersion": PATH_RULE_VERSION,
        "targets": {
            "physicalRelations": list(PHYSICAL_TARGETS),
            "legacyRoutines": list(LEGACY_ROUTINES),
            "stableApiRoutines": list(STABLE_API_ROUTINES),
        },
        "databaseCurrentEvidence": database,
        "repositories": repositories,
        "nextEdgeMirror": mirror,
        "nextDatabaseSnapshot": snapshot,
        "blockers": sorted(blockers, key=lambda row: canonical_bytes(row)),
        "staticEvidenceComplete": False,
        "ddlAuthorized": False,
    }
    validate_artifact(artifact, contract)
    return artifact


def validate_artifact(artifact: dict[str, Any], contract: dict[str, Any]) -> None:
    if artifact.get("schemaVersion") != SCHEMA_VERSION:
        raise EvidenceError("external Git-tree artifact schema version differs")
    if artifact.get("ddlAuthorized") is not False or artifact.get("staticEvidenceComplete") is not False:
        raise EvidenceError("external Git-tree artifact must remain non-authorizing")
    if not artifact.get("blockers"):
        raise EvidenceError("external Git-tree artifact unexpectedly has no blockers")
    repositories = artifact.get("repositories", [])
    if len(repositories) != 8 or len({row["key"] for row in repositories}) != 8:
        raise EvidenceError("external Git-tree artifact repository set differs")
    inputs = {row["key"]: row for row in contract["externalGitTreeGate"]["repositories"]}
    for row in repositories:
        source = inputs.get(row["key"])
        if source is None or row["commit"] != source["commit"] or row["repository"] != source["repository"]:
            raise EvidenceError(f"external Git-tree repository receipt differs: {row.get('key')}")
        if row["treeEntryCount"] != row["scannedRegularBlobCount"] + len(row["unsupportedEntries"]):
            raise EvidenceError(f"tree-entry accounting differs: {row['key']}")
        for evidence in row["evidence"]:
            if "source" in evidence or "text" in evidence:
                raise EvidenceError("external evidence must not retain source text")
            if not FULL_SHA.fullmatch(evidence["blob"]):
                raise EvidenceError("external evidence blob is not exact")
            if not re.fullmatch(r"[0-9a-f]{64}", evidence["lineSha256"]):
                raise EvidenceError("external evidence line hash is invalid")
        aggregate = Counter(
            (
                evidence["sourceKind"],
                evidence["executionReachability"],
                evidence["tokenClass"],
                evidence["transport"],
            )
            for evidence in row["evidence"]
        )
        expected_aggregates = [
            {
                "sourceKind": key[0],
                "executionReachability": key[1],
                "tokenClass": key[2],
                "transport": key[3],
                "count": count,
            }
            for key, count in sorted(aggregate.items())
        ]
        if row["aggregates"] != expected_aggregates:
            raise EvidenceError(f"external evidence aggregate differs: {row['key']}")
        recognized = sum(
            1
            for evidence in row["evidence"]
            if evidence["executionReachability"] == "active-runtime"
            and evidence["tokenClass"] in {"physical-relation", "legacy-routine"}
            and evidence["transport"] in DIRECT_TRANSPORTS
        )
        if row["recognizedRuntimeDirectLegacyOccurrenceCount"] != recognized:
            raise EvidenceError(f"recognized direct occurrence count differs: {row['key']}")
        unresolved = sum(
            1
            for blocker in row["blockers"]
            if blocker["kind"]
            in {
                "active-runtime-semantic-review-required",
                "dynamic-selector-review-required",
            }
        )
        if row["unresolvedActiveRuntimeEvidenceCount"] != unresolved:
            raise EvidenceError(f"unresolved active evidence count differs: {row['key']}")
    edge = next(row for row in repositories if row["key"] == "edge")
    if edge["commit"] != "8588f1b9dbe5c24dfbad7d704f956a09ba3b7904":
        raise EvidenceError("Edge exact consumer evidence commit differs")
    if edge["recognizedRuntimeDirectLegacyOccurrenceCount"] != 0:
        raise EvidenceError("Edge exact runtime still has recognized direct legacy occurrences")
    database = artifact["databaseCurrentEvidence"]
    expected_database = contract["externalGitTreeGate"]["databaseCurrentEvidence"]
    if database["commit"] != expected_database["commit"]:
        raise EvidenceError("database current exact evidence commit differs")
    if database["migrationHead"] != expected_database["migrationHead"]:
        raise EvidenceError("database current exact migration head differs")
    if not database["physicalTargetsRemainAtFrozenPublicBoundary"]:
        raise EvidenceError("database current boundary moved before authorization")
    if len(database["stableApiFacadeNames"]) != 8:
        raise EvidenceError("database current facade count differs")
    if artifact["nextEdgeMirror"]["matchesCurrentEdgeCommit"]:
        raise EvidenceError("reviewed v1 artifact must retain the observed stale Next mirror blocker")
    if artifact["nextDatabaseSnapshot"]["exactSourceReceiptPresent"]:
        raise EvidenceError("reviewed v1 artifact unexpectedly claims a Next DB source receipt")


def checked_artifact(contract: dict[str, Any]) -> dict[str, Any]:
    raw = ARTIFACT_PATH.read_bytes()
    sidecar = ARTIFACT_SHA_PATH.read_text(encoding="utf-8").strip()
    digest = sha256_bytes(raw)
    if sidecar != digest:
        raise EvidenceError("external Git-tree artifact SHA sidecar differs")
    if raw != canonical_bytes(json.loads(raw)):
        raise EvidenceError("external Git-tree artifact is not canonical JSON")
    artifact = json.loads(raw)
    schema = json.loads(ARTIFACT_SCHEMA_PATH.read_text(encoding="utf-8"))
    try:
        jsonschema.Draft202012Validator(schema).validate(artifact)
    except jsonschema.ValidationError as error:
        raise EvidenceError(
            f"external Git-tree artifact schema validation failed: {error.json_path}"
        ) from error
    validate_artifact(artifact, contract)
    reference = contract["externalGitTreeGate"]["artifact"]
    if reference != {
        "path": str(ARTIFACT_PATH.relative_to(ROOT)),
        "sha256Path": str(ARTIFACT_SHA_PATH.relative_to(ROOT)),
        "schemaPath": str(ARTIFACT_SCHEMA_PATH.relative_to(ROOT)),
        "sha256": digest,
        "schemaVersion": SCHEMA_VERSION,
    }:
        raise EvidenceError("pre-DDL external artifact reference differs")
    return artifact


def write_artifact(artifact: dict[str, Any]) -> str:
    payload = canonical_bytes(artifact)
    digest = sha256_bytes(payload)
    ARTIFACT_PATH.write_bytes(payload)
    ARTIFACT_SHA_PATH.write_text(digest + "\n", encoding="utf-8")
    return digest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--check", action="store_true", help="check committed artifact and manifest binding")
    action.add_argument("--scan-external", metavar="WORKSPACE_ROOT", help="regenerate from exact external Git trees")
    action.add_argument("--verify-external", metavar="WORKSPACE_ROOT", help="recompute and compare without writing")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    if args.check:
        artifact = checked_artifact(contract)
        print(
            json.dumps(
                {
                    "artifactSha256": ARTIFACT_SHA_PATH.read_text(encoding="utf-8").strip(),
                    "blockerCount": len(artifact["blockers"]),
                    "ddlAuthorized": artifact["ddlAuthorized"],
                    "repositoryCount": len(artifact["repositories"]),
                },
                sort_keys=True,
            )
        )
        return 0
    workspace_root = Path(args.scan_external or args.verify_external).resolve()
    artifact = build_artifact(workspace_root, contract)
    if args.verify_external:
        if canonical_bytes(artifact) != ARTIFACT_PATH.read_bytes():
            raise EvidenceError("exact external Git-tree regeneration differs")
        print(json.dumps({"verified": True, "blockerCount": len(artifact["blockers"])}, sort_keys=True))
        return 0
    digest = write_artifact(artifact)
    print(json.dumps({"artifactSha256": digest, "blockerCount": len(artifact["blockers"])}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except EvidenceError as error:
        print(f"Issue #397 external Git-tree gate failed: {error}", file=sys.stderr)
        raise SystemExit(1)
