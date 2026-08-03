#!/usr/bin/env python3
"""Build the Issue #408 Worker source-consumer evidence from one exact Git tree.

This is a source inventory, not Contract authorization.  It reads committed Git
blobs only, never connects to PostgreSQL, and deliberately leaves
``contractReady`` false until attributed runtime SQL evidence closes the listed
residue.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = ROOT / "supabase/tests/contracts"
SCHEMA_PATH = CONTRACT_DIR / "lca_worker_runtime_consumer.v1.schema.json"
ARTIFACT_PATH = CONTRACT_DIR / "lca_worker_runtime_consumer.7c0a95f8.v1.json"
SHA_PATH = ARTIFACT_PATH.with_suffix(".sha256")
SCHEMA_VERSION = "database.lca-worker-runtime-consumer.v1"
WORKER_COMMIT = "7c0a95f8aa4b8952e002bff7b91847e31cf2487b"
DEFAULT_WORKER_REPO = Path(
    os.environ.get("ISSUE408_WORKER_REPO", ROOT.parent / "tiangong-lca-worker")
)


CONNECTION_FAMILIES = [
    {"id": "solver-main", "entrypoints": ["solver-worker"], "environment": ["DATABASE_URL", "CONN"], "fallback": "DATABASE_URL->CONN", "applicationName": "solver-worker", "roleStrategy": "session login plus transaction-local request.jwt.claim.role=service_role for facade calls", "readOnly": False},
    {"id": "solver-queue", "entrypoints": ["solver-worker"], "environment": ["QUEUE_DATABASE_URL", "QUEUE_CONN"], "fallback": "queue URL -> queue CONN -> solver-main pool", "applicationName": "solver-worker-queue", "roleStrategy": "connection login", "readOnly": False},
    {"id": "document-validation", "entrypoints": ["solver-worker"], "environment": ["DOCUMENT_VALIDATION_DATABASE_URL"], "fallback": "forbidden", "applicationName": "solver-worker-document-validation", "roleStrategy": "dedicated LOGIN, SESSION_USER=CURRENT_USER, unique membership in lca_worker_runtime, no role GUC", "readOnly": False},
    {"id": "package-main", "entrypoints": ["package-worker"], "environment": ["DATABASE_URL", "CONN"], "fallback": "DATABASE_URL->CONN", "applicationName": "package-worker", "roleStrategy": "session login plus service-role GUC for worker facade", "readOnly": False},
    {"id": "package-queue", "entrypoints": ["package-worker"], "environment": ["QUEUE_DATABASE_URL", "QUEUE_CONN"], "fallback": "queue URL -> queue CONN -> package-main pool", "applicationName": "package-worker-queue", "roleStrategy": "connection login", "readOnly": False},
    {"id": "review-submit-main", "entrypoints": ["review-submit-gate-runner"], "environment": ["DATABASE_URL", "CONN"], "fallback": "DATABASE_URL->CONN", "applicationName": "review-submit-gate-runner", "roleStrategy": "session login plus service-role GUC for worker facade", "readOnly": False},
    {"id": "review-submit-queue", "entrypoints": ["review-submit-gate-runner"], "environment": ["QUEUE_DATABASE_URL", "QUEUE_CONN"], "fallback": "queue URL -> queue CONN -> review-submit-main pool", "applicationName": "review-submit-gate-runner-queue", "roleStrategy": "connection login", "readOnly": False},
    {"id": "snapshot-builder", "entrypoints": ["snapshot-builder"], "environment": ["DATABASE_URL", "CONN"], "fallback": "DATABASE_URL->CONN", "applicationName": "snapshot-builder", "roleStrategy": "connection login", "readOnly": False},
    {"id": "snapshot-gc", "entrypoints": ["snapshot-gc"], "environment": ["DATABASE_URL", "CONN"], "fallback": "DATABASE_URL->CONN", "applicationName": "snapshot-gc", "roleStrategy": "connection login", "readOnly": False},
    {"id": "package-gc", "entrypoints": ["package-gc"], "environment": ["DATABASE_URL", "CONN"], "fallback": "DATABASE_URL->CONN", "applicationName": "package-gc", "roleStrategy": "connection login", "readOnly": False},
    {"id": "artifact-gc", "entrypoints": ["artifact-gc"], "environment": ["DATABASE_URL", "CONN"], "fallback": "DATABASE_URL->CONN", "applicationName": "artifact-gc", "roleStrategy": "service-role GUC facade calls", "readOnly": False},
    {"id": "result-gc", "entrypoints": ["result-gc"], "environment": ["DATABASE_URL", "CONN"], "fallback": "DATABASE_URL->CONN", "applicationName": "result-gc", "roleStrategy": "connection login", "readOnly": False},
    {"id": "maintenance-worker", "entrypoints": ["maintenance-worker"], "environment": ["DATABASE_URL", "CONN"], "fallback": "DATABASE_URL->CONN", "applicationName": "maintenance-worker", "roleStrategy": "service-role GUC worker facade plus delegated subprocesses", "readOnly": False},
    {"id": "maintenance-enqueue", "entrypoints": ["maintenance-enqueue"], "environment": ["DATABASE_URL", "CONN"], "fallback": "DATABASE_URL->CONN", "applicationName": "maintenance-enqueue", "roleStrategy": "service-role GUC facade", "readOnly": False},
    {"id": "process-flow-graph", "entrypoints": ["process-flow-graph-cache-builder"], "environment": ["DATABASE_URL", "CONN"], "fallback": "DATABASE_URL->CONN", "applicationName": "driver default", "roleStrategy": "connection login", "readOnly": True},
]


# identifier, verbs, connection families, source files, qualification mode
RELATIONS = [
    ("public.contacts", ["SELECT", "INSERT"], ["snapshot-builder", "package-main", "solver-main"], ["crates/solver-worker/src/bin/snapshot_builder.rs", "crates/solver-worker/src/package_execution.rs", "crates/solver-worker/src/scope_closure.rs"], "mixed"),
    ("public.sources", ["SELECT", "INSERT"], ["snapshot-builder", "package-main", "solver-main"], ["crates/solver-worker/src/bin/snapshot_builder.rs", "crates/solver-worker/src/package_execution.rs", "crates/solver-worker/src/scope_closure.rs"], "mixed"),
    ("public.unitgroups", ["SELECT", "INSERT"], ["snapshot-builder", "package-main", "solver-main"], ["crates/solver-worker/src/bin/snapshot_builder.rs", "crates/solver-worker/src/package_execution.rs", "crates/solver-worker/src/scope_closure.rs"], "mixed"),
    ("public.flowproperties", ["SELECT", "INSERT"], ["snapshot-builder", "package-main", "solver-main"], ["crates/solver-worker/src/bin/snapshot_builder.rs", "crates/solver-worker/src/package_execution.rs", "crates/solver-worker/src/scope_closure.rs"], "mixed"),
    ("public.flows", ["SELECT", "INSERT"], ["snapshot-builder", "package-main", "solver-main", "process-flow-graph"], ["crates/solver-worker/src/bin/snapshot_builder.rs", "crates/solver-worker/src/package_execution.rs", "crates/solver-worker/src/scope_closure.rs", "crates/solver-worker/src/bin/process_flow_graph_cache_builder.rs"], "mixed"),
    ("public.processes", ["SELECT", "INSERT"], ["snapshot-builder", "package-main", "solver-main", "review-submit-main", "process-flow-graph"], ["crates/solver-worker/src/bin/snapshot_builder.rs", "crates/solver-worker/src/package_execution.rs", "crates/solver-worker/src/scope_closure.rs", "crates/solver-worker/src/review_submit_gate_runner.rs", "crates/solver-worker/src/bin/process_flow_graph_cache_builder.rs", "crates/solver-worker/src/queue.rs"], "mixed"),
    ("public.lifecyclemodels", ["SELECT", "INSERT"], ["package-main", "solver-main"], ["crates/solver-worker/src/package_execution.rs", "crates/solver-worker/src/scope_closure.rs"], "mixed"),
    ("public.lciamethods", ["SELECT"], ["snapshot-builder", "solver-main"], ["crates/solver-worker/src/bin/snapshot_builder.rs", "crates/solver-worker/src/scope_closure.rs"], "qualified"),
    ("private.lca_active_snapshots", ["SELECT", "INSERT", "UPDATE"], ["solver-main", "snapshot-gc"], ["crates/solver-worker/src/db.rs", "crates/solver-worker/src/snapshot_retention.rs"], "qualified"),
    ("private.lca_network_snapshots", ["SELECT", "INSERT", "UPDATE", "DELETE"], ["solver-main", "snapshot-builder", "snapshot-gc"], ["crates/solver-worker/src/db.rs", "crates/solver-worker/src/bin/snapshot_builder.rs", "crates/solver-worker/src/snapshot_retention.rs"], "qualified"),
    ("private.lca_snapshot_artifacts", ["SELECT", "INSERT", "UPDATE"], ["solver-main", "snapshot-builder"], ["crates/solver-worker/src/db.rs", "crates/solver-worker/src/bin/snapshot_builder.rs", "crates/solver-worker/src/queue.rs"], "qualified"),
    ("private.worker_jobs", ["SELECT", "UPDATE"], ["solver-main", "package-main", "review-submit-main", "result-gc"], ["crates/solver-worker/src/db.rs", "crates/solver-worker/src/queue.rs", "crates/solver-worker/src/package_execution.rs", "crates/solver-worker/src/worker_control_plane.rs"], "qualified"),
    ("private.worker_job_artifacts", ["SELECT", "INSERT"], ["solver-main", "maintenance-worker"], ["crates/solver-worker/src/db.rs", "crates/solver-worker/src/scope_closure.rs", "crates/solver-worker/src/worker_control_plane.rs"], "qualified"),
    ("public.lca_jobs", ["SELECT", "UPDATE"], ["solver-main"], ["crates/solver-worker/src/db.rs"], "search_path"),
    ("public.lca_results", ["SELECT", "INSERT", "UPDATE", "DELETE"], ["solver-main", "result-gc"], ["crates/solver-worker/src/db.rs", "crates/solver-worker/src/queue.rs", "crates/solver-worker/src/bin/result_gc.rs", "crates/solver-worker/src/worker_control_plane.rs"], "mixed"),
    ("public.lca_result_cache", ["SELECT", "UPDATE"], ["solver-main", "result-gc"], ["crates/solver-worker/src/db.rs", "crates/solver-worker/src/queue.rs", "crates/solver-worker/src/worker_control_plane.rs"], "mixed"),
    ("public.lca_latest_all_unit_results", ["INSERT", "UPDATE"], ["solver-main"], ["crates/solver-worker/src/db.rs", "crates/solver-worker/src/queue.rs"], "qualified"),
    ("public.lca_factorization_registry", ["UPDATE"], ["solver-main"], ["crates/solver-worker/src/queue.rs"], "qualified"),
    ("public.lca_process_index", ["SELECT"], ["solver-main"], ["crates/solver-worker/src/db.rs"], "search_path"),
    ("public.lca_flow_index", ["SELECT"], ["solver-main"], ["crates/solver-worker/src/db.rs"], "search_path"),
    ("public.lca_technosphere_entries", ["SELECT"], ["solver-main"], ["crates/solver-worker/src/db.rs"], "search_path"),
    ("public.lca_biosphere_entries", ["SELECT"], ["solver-main"], ["crates/solver-worker/src/db.rs"], "search_path"),
    ("public.lca_characterization_factors", ["SELECT"], ["solver-main"], ["crates/solver-worker/src/db.rs"], "search_path"),
    ("public.lca_package_jobs", ["SELECT", "UPDATE", "DELETE"], ["package-main", "package-gc"], ["crates/solver-worker/src/package_db.rs", "crates/solver-worker/src/package_execution.rs", "crates/solver-worker/src/package_retention.rs", "crates/solver-worker/src/bin/package_worker.rs"], "mixed"),
    ("public.lca_package_artifacts", ["SELECT", "INSERT", "UPDATE"], ["package-main", "package-gc"], ["crates/solver-worker/src/package_db.rs", "crates/solver-worker/src/package_execution.rs", "crates/solver-worker/src/package_retention.rs", "crates/solver-worker/src/bin/package_worker.rs"], "mixed"),
    ("public.lca_package_export_items", ["SELECT", "INSERT", "UPDATE"], ["package-main"], ["crates/solver-worker/src/package_execution.rs", "crates/solver-worker/src/bin/package_worker.rs"], "mixed"),
    ("public.lca_package_request_cache", ["SELECT", "UPDATE", "DELETE"], ["package-main", "package-gc"], ["crates/solver-worker/src/package_db.rs", "crates/solver-worker/src/package_retention.rs", "crates/solver-worker/src/bin/package_worker.rs"], "mixed"),
    ("public.dataset_review_submit_gate_runs", ["SELECT", "UPDATE"], ["review-submit-main"], ["crates/solver-worker/src/review_submit_gate_runner.rs"], "qualified"),
    ("public.lcia_result_packages", ["SELECT"], ["solver-main"], ["crates/solver-worker/src/queue.rs"], "qualified"),
    ("public.lca_release_publications", ["SELECT"], ["solver-main"], ["crates/solver-worker/src/scope_closure.rs"], "qualified"),
    ("public.lca_release_runs", ["SELECT"], ["solver-main"], ["crates/solver-worker/src/scope_closure.rs"], "qualified"),
    ("public.lcia_scope_closure_checks", ["SELECT"], ["solver-main"], ["crates/solver-worker/src/scope_closure.rs"], "qualified"),
    ("public.lcia_scope_closure_data_snapshots", ["SELECT"], ["solver-main"], ["crates/solver-worker/src/scope_closure.rs"], "qualified"),
    ("public.lcia_scope_closure_issues", ["SELECT"], ["solver-main"], ["crates/solver-worker/src/scope_closure.rs"], "qualified"),
    ("public.lcia_scope_closure_issue_occurrences", ["SELECT"], ["solver-main"], ["crates/solver-worker/src/scope_closure.rs"], "qualified"),
    ("public.lcia_scope_closure_issue_roots", ["SELECT"], ["solver-main"], ["crates/solver-worker/src/scope_closure.rs"], "qualified"),
    ("public.lca_snapshot_gc_runs", ["INSERT", "UPDATE"], ["snapshot-gc"], ["crates/solver-worker/src/snapshot_retention.rs"], "qualified"),
    ("public.lca_snapshot_gc_run_items", ["INSERT", "UPDATE"], ["snapshot-gc"], ["crates/solver-worker/src/snapshot_retention.rs"], "qualified"),
]


ROUTINES = [
    ("public.worker_claim_jobs", ["solver-main", "package-main", "review-submit-main", "maintenance-worker"], "crates/solver-worker/src/worker_control_plane.rs"),
    ("public.worker_heartbeat_job", ["solver-main", "package-main", "review-submit-main", "maintenance-worker"], "crates/solver-worker/src/worker_control_plane.rs"),
    ("public.worker_record_job_result", ["solver-main", "package-main", "review-submit-main", "maintenance-worker"], "crates/solver-worker/src/worker_control_plane.rs"),
    ("public.worker_enqueue_job", ["maintenance-enqueue"], "crates/solver-worker/src/bin/maintenance_enqueue.rs"),
    ("public.cmd_lcia_result_package_mark_ready", ["solver-main"], "crates/solver-worker/src/db.rs"),
    ("public.cmd_dataset_review_submit_gate_record_result", ["review-submit-main"], "crates/solver-worker/src/review_submit_gate_runner.rs"),
    ("private.lcia_scope_closure_sha256", ["solver-main"], "crates/solver-worker/src/scope_closure.rs"),
    ("public.svc_lcia_scope_closure_build_binding", ["solver-main"], "crates/solver-worker/src/queue.rs"),
    ("public.svc_lcia_scope_closure_check_get_worker_input", ["solver-main"], "crates/solver-worker/src/scope_closure.rs"),
    ("public.svc_lcia_scope_closure_claim_scan_execution", ["solver-main"], "crates/solver-worker/src/scope_closure.rs"),
    ("public.svc_lcia_scope_closure_reuse_completed_scan", ["solver-main"], "crates/solver-worker/src/scope_closure.rs"),
    ("public.svc_lcia_scope_closure_finalize_reused_scan", ["solver-main"], "crates/solver-worker/src/scope_closure.rs"),
    ("public.svc_lcia_scope_closure_fail_before_scan", ["solver-main"], "crates/solver-worker/src/scope_closure.rs"),
    ("public.svc_lcia_scope_closure_check_record_result_v3", ["solver-main"], "crates/solver-worker/src/scope_closure.rs"),
    ("public.svc_lcia_scope_closure_artifact_write_set_create_v2", ["solver-main"], "crates/solver-worker/src/scope_closure.rs"),
    ("public.svc_lcia_scope_closure_artifact_write_set_register_batch_v2", ["solver-main"], "crates/solver-worker/src/scope_closure.rs"),
    ("public.svc_lcia_scope_closure_artifact_write_set_status_v2", ["solver-main"], "crates/solver-worker/src/scope_closure.rs"),
    ("public.svc_lcia_scope_closure_artifact_write_set_seal_v2", ["solver-main"], "crates/solver-worker/src/scope_closure.rs"),
    ("public.svc_lcia_scope_closure_artifact_write_set_finalize_v2", ["solver-main"], "crates/solver-worker/src/scope_closure.rs"),
    ("public.svc_lcia_scope_closure_artifact_write_set_fail_v2", ["solver-main"], "crates/solver-worker/src/scope_closure.rs"),
    ("public.svc_lcia_scope_closure_artifact_gc_preview", ["artifact-gc"], "crates/solver-worker/src/bin/artifact_gc.rs"),
    ("public.svc_lcia_scope_closure_artifact_gc_claim", ["artifact-gc"], "crates/solver-worker/src/bin/artifact_gc.rs"),
    ("public.svc_lcia_scope_closure_artifact_gc_renew", ["artifact-gc"], "crates/solver-worker/src/bin/artifact_gc.rs"),
    ("public.svc_lcia_scope_closure_artifact_write_set_reconcile", ["artifact-gc"], "crates/solver-worker/src/bin/artifact_gc.rs"),
    ("public.svc_lcia_scope_closure_artifact_write_set_reconcile_complete", ["artifact-gc"], "crates/solver-worker/src/bin/artifact_gc.rs"),
    ("public.svc_lcia_scope_closure_artifact_gc_complete", ["artifact-gc"], "crates/solver-worker/src/bin/artifact_gc.rs"),
    ("public.svc_lcia_scope_closure_artifact_gc_fail", ["artifact-gc"], "crates/solver-worker/src/bin/artifact_gc.rs"),
    ("util.preview_lca_snapshot_retention", ["snapshot-gc"], "crates/solver-worker/src/snapshot_retention.rs"),
    ("util.list_lca_snapshot_gc_candidates", ["snapshot-gc"], "crates/solver-worker/src/snapshot_retention.rs"),
    ("pgmq.read", ["solver-queue", "package-queue"], "crates/solver-worker/src/db.rs"),
    ("pgmq.archive", ["solver-queue", "package-queue"], "crates/solver-worker/src/db.rs"),
    ("pgmq.send", ["package-main"], "crates/solver-worker/src/package_db.rs"),
    ("private.svc_lcia_document_validation_evidence_lookup", ["document-validation"], "crates/solver-worker/src/document_validation_db.rs"),
    ("private.svc_lcia_document_validation_evidence_record", ["document-validation"], "crates/solver-worker/src/document_validation_db.rs"),
]


DYNAMIC_RESOLVERS = [
    {"id": "package-root-table", "source": "crates/solver-worker/src/package_execution.rs", "needle": "fn table_name(table: PackageRootTable)", "construction": "enum-to-unqualified-relation", "closedAllowlist": ["public.contacts", "public.sources", "public.unitgroups", "public.flowproperties", "public.flows", "public.processes", "public.lifecyclemodels"], "inputOrigin": "PackageRootTable enum", "resolved": True},
    {"id": "scope-closure-dataset-category", "source": "crates/solver-worker/src/scope_closure.rs", "needle": "pub const fn table_name(&self)", "construction": "enum-to-public-qualified-relation", "closedAllowlist": ["public.contacts", "public.flowproperties", "public.flows", "public.lciamethods", "public.lifecyclemodels", "public.processes", "public.sources", "public.unitgroups"], "inputOrigin": "DatasetCategory enum", "resolved": True},
    {"id": "process-flow-graph-table", "source": "crates/solver-worker/src/bin/process_flow_graph_cache_builder.rs", "needle": "fetch_all_rows_read_only(&pool, \"flows\"", "construction": "caller-bounded-string-to-public-qualified-relation", "closedAllowlist": ["public.flows", "public.processes"], "inputOrigin": "two literal main callers", "resolved": True},
    {"id": "matrix-triplet-query", "source": "crates/solver-worker/src/db.rs", "needle": "async fn fetch_triplets(", "construction": "SQL argument", "closedAllowlist": ["public.lca_technosphere_entries", "public.lca_biosphere_entries", "public.lca_characterization_factors"], "inputOrigin": "three internal literal callers", "resolved": True},
    {"id": "pgmq-queue-name", "source": "crates/solver-worker/src/db.rs", "needle": "fn pgmq_queue_name_literal", "construction": "validated runtime queue identifier", "closedAllowlist": [], "inputOrigin": "runtime configuration", "resolved": False},
    {"id": "maintenance-subprocess-override", "source": "crates/solver-worker/src/bin/maintenance_worker.rs", "needle": "std::env::var(format!", "construction": "environment-overridable executable", "closedAllowlist": [], "inputOrigin": "runtime environment", "resolved": False},
]


CAPABILITIES = [
    {"identifier": "private.svc_lcia_document_validation_evidence_lookup(jsonb)", "privilege": "EXECUTE", "expected": "granted", "directCall": True},
    {"identifier": "private.svc_lcia_document_validation_evidence_record(jsonb,uuid)", "privilege": "EXECUTE", "expected": "granted", "directCall": True},
    *[{"identifier": f"private.worker_lca_result_gc_{name}_v1{signature}", "privilege": "EXECUTE", "expected": "granted", "directCall": False} for name, signature in [
        ("attest", "(uuid)"), ("claim", "(text,integer,integer)"),
        ("fail", "(uuid,uuid,text)"), ("fence", "(uuid,uuid)"),
        ("finalize", "(uuid,uuid,text)"), ("preview", "(integer)"),
        ("renew", "(uuid,uuid,integer)"),
    ]],
    {"identifier": "public.svc_lcia_document_validation_evidence_lookup(jsonb)", "privilege": "EXECUTE", "expected": "denied", "directCall": False},
    {"identifier": "public.svc_lcia_document_validation_evidence_record(jsonb,uuid)", "privilege": "EXECUTE", "expected": "denied", "directCall": False},
    {"identifier": "public.lcia_document_validation_evidence", "privilege": "ALL_TABLE", "expected": "denied", "directCall": False},
]


DECLARED_ONLY = {"private.worker_job_events", "private.worker_job_kinds", "public.worker_job_domain_refs"}
NON_SQL_QUALIFIED = {
    "private.v1": {
        "classification": "database-contract-id-suffix",
        "detail": "The token occurs only inside document-validation-evidence.private.v1 metadata, not SQL.",
        "source": ("crates/solver-worker/src/document_validation_db.rs", "document-validation-evidence.private.v1"),
    },
}


def run(repo: Path, *args: str, check: bool = True) -> str:
    result = subprocess.run(["git", *args], cwd=repo, check=check, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result.stdout.strip()


def canonical(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def blob_text(repo: Path, path: str) -> str:
    return run(repo, "show", f"{WORKER_COMMIT}:{path}")


def blob_sha(repo: Path, path: str) -> str:
    return run(repo, "rev-parse", f"{WORKER_COMMIT}:{path}")


def runtime_text(text: str) -> str:
    """Exclude inline cfg(test) modules so fixtures cannot become runtime evidence."""
    marker = re.search(r"(?m)^\s*#\[cfg\(test\)\]\s*\n\s*mod\s+tests\s*\{", text)
    return text[: marker.start()] if marker else text


def source_ref(repo: Path, path: str, needle: str) -> dict[str, Any]:
    text = runtime_text(blob_text(repo, path))
    for number, line in enumerate(text.splitlines(), 1):
        if needle in line:
            return {"path": path, "line": number, "blobSha": blob_sha(repo, path), "lineSha256": hashlib.sha256(line.encode()).hexdigest(), "needle": needle}
    # Dynamic/unqualified objects often appear without their final public schema.
    leaf = needle.split(".")[-1]
    for number, line in enumerate(text.splitlines(), 1):
        if leaf in line:
            return {"path": path, "line": number, "blobSha": blob_sha(repo, path), "lineSha256": hashlib.sha256(line.encode()).hexdigest(), "needle": leaf}
    raise ValueError(f"source evidence missing: {path}: {needle}")


def validate_worker_tree(repo: Path) -> None:
    if run(repo, "rev-parse", "HEAD") != WORKER_COMMIT:
        raise ValueError("Worker HEAD differs from the reviewed exact commit")
    if run(repo, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("Worker tree is not exact-clean (tracked or untracked drift)")
    if run(repo, "diff-tree", "--no-commit-id", "--name-only", "-r", WORKER_COMMIT) is None:
        raise AssertionError("unreachable")


def source_tree(repo: Path) -> tuple[list[str], str]:
    paths = [path for path in run(repo, "ls-tree", "-r", "--name-only", WORKER_COMMIT, "crates/solver-worker/src").splitlines() if path.endswith(".rs")]
    rows = [f"{path}\t{blob_sha(repo, path)}" for path in sorted(paths)]
    return sorted(paths), hashlib.sha256(("\n".join(rows) + "\n").encode()).hexdigest()


def discover_sql_objects(repo: Path, paths: list[str]) -> set[str]:
    schemas = "public|private|util|pgmq"
    qualified = re.compile(rf"\b(?:{schemas})\.[a-z_][a-z0-9_]*\b")
    known_unqualified = {item[0].split(".", 1)[1] for item in RELATIONS if item[4] in {"search_path", "mixed"}}
    found: set[str] = set()
    for path in paths:
        text = runtime_text(blob_text(repo, path))
        for match in qualified.finditer(text):
            identifier = match.group(0)
            found.add(identifier)
        for name in known_unqualified:
            if re.search(rf"(?is)\b(?:FROM|JOIN|UPDATE|INTO|DELETE\s+FROM)\s+{re.escape(name)}\b", text):
                found.add(f"public.{name}")
    for resolver in DYNAMIC_RESOLVERS:
        found.update(resolver["closedAllowlist"])
    return found


def build(repo: Path) -> dict[str, Any]:
    validate_worker_tree(repo)
    paths, tree_sha = source_tree(repo)
    accesses = []
    for identifier, verbs, families, source_paths, mode in RELATIONS:
        refs = [source_ref(repo, path, identifier) for path in source_paths]
        accesses.append({
            "identifier": identifier, "objectKind": "relation", "verbs": verbs,
            "connectionFamilies": families, "surfaceClass": "production-runtime",
            "schemaQualified": mode == "qualified", "searchPathDependent": mode in {"search_path", "mixed"},
            "construction": mode, "required": True, "sources": refs,
        })
    for identifier, families, path in ROUTINES:
        accesses.append({
            "identifier": identifier, "objectKind": "routine", "verbs": ["EXECUTE"],
            "connectionFamilies": families, "surfaceClass": "production-runtime",
            "schemaQualified": True, "searchPathDependent": False,
            "construction": "literal-or-validated-format", "required": True,
            "sources": [source_ref(repo, path, identifier)],
        })
    resolvers = []
    for resolver in DYNAMIC_RESOLVERS:
        item = dict(resolver)
        item["source"] = source_ref(repo, resolver["source"], resolver["needle"])
        item.pop("needle")
        resolvers.append(item)
    expected = {item["identifier"] for item in accesses}
    capabilities = {item["identifier"].split("(", 1)[0] for item in CAPABILITIES}
    discovered = discover_sql_objects(repo, paths)
    non_sql = {
        identifier: {
            "identifier": identifier,
            "classification": metadata["classification"],
            "detail": metadata["detail"],
            "source": source_ref(repo, *metadata["source"]),
        }
        for identifier, metadata in NON_SQL_QUALIFIED.items()
    }
    accepted = expected | capabilities | DECLARED_ONLY | set(non_sql)
    unclassified = sorted(discovered - accepted)
    missing = sorted(expected - discovered)
    if unclassified or missing:
        raise ValueError(f"bidirectional source completeness failed: unclassified={unclassified}, missing={missing}")
    return {
        "schemaVersion": SCHEMA_VERSION,
        "source": {
            "issue": "tiangong-lca/database-engine#408",
            "workerRepository": "linancn/tiangong-lca-worker",
            "workerCommit": WORKER_COMMIT,
            "refKind": "pull-request-head",
            "pullRequest": "https://github.com/linancn/tiangong-lca-worker/pull/208",
            "canonical": False,
            "canonicalBaseAtCapture": "2ee74ffaf431c0d43b9613bcb6bfed76fa447b66",
            "sourceTreeSha256": tree_sha,
            "trackedRuntimeRustFileCount": len(paths),
            "derivation": "exact committed Git blobs; working-tree files are not read",
        },
        "scope": {
            "included": ["crates/solver-worker/src/**/*.rs production paths"],
            "excluded": ["inline #[cfg(test)] modules", "crates/**/tests/**", "docs/**", "scripts/**", "tools/**", "supabase/migrations/**"],
            "excludedSurfaceClasses": ["test-only", "documentation", "diagnostic-script", "migration-source"],
        },
        "connectionFamilies": CONNECTION_FAMILIES,
        "accesses": sorted(accesses, key=lambda item: (item["objectKind"], item["identifier"])),
        "dynamicResolvers": resolvers,
        "requiredCapabilities": CAPABILITIES,
        "declaredOnlyIdentifiers": sorted(DECLARED_ONLY),
        "nonSqlQualifiedIdentifiers": [non_sql[key] for key in sorted(non_sql)],
        "completeness": {
            "sourceDiscoveredIdentifiers": sorted(discovered),
            "manifestIdentifiers": sorted(expected),
            "unclassifiedSourceIdentifiers": unclassified,
            "manifestIdentifiersMissingFromSource": missing,
            "qualifiedIdentifierClosed": not unclassified and not missing,
            "unqualifiedIdentifierClosed": False,
            "bidirectionalSourceClosed": False,
        },
        "residue": [
            {"code": "runtime_sql_trace_missing", "blocking": True, "detail": "No exact deployment/job-family SQL trace has yet been compared bidirectionally with this source inventory."},
            {"code": "pgmq_queue_identity_runtime_value", "blocking": True, "detail": "Runtime queue names are validated identifiers but their deployed value set is not source-closed."},
            {"code": "maintenance_binary_override", "blocking": True, "detail": "Maintenance child executable paths may be overridden by runtime environment."},
            {"code": "search_path_dependent_sql", "blocking": True, "detail": "Unqualified package, legacy result, and matrix relations remain dependent on the login search_path."},
            {"code": "non_runtime_surfaces_not_attributed", "blocking": True, "detail": "Maintenance scripts and BW25/diagnostic tools require a separate surface-class artifact before Contract."},
            {"code": "unqualified_sql_extraction_not_parser_closed", "blocking": True, "detail": "The B0 lexical scanner classifies the reviewed unqualified allowlist but is not a parser-complete proof for newly introduced bare identifiers."},
        ],
        "sourceArtifactComplete": False,
        "contractReady": False,
    }


def validate_single_source_ref(repo: Path, ref: dict[str, Any]) -> None:
    if ref["path"].startswith(("docs/", "scripts/", "crates/solver-worker/tests/")):
        raise ValueError("test/docs/scripts source cannot prove production runtime")
    text = runtime_text(blob_text(repo, ref["path"]))
    lines = text.splitlines()
    if not 1 <= ref["line"] <= len(lines):
        raise ValueError("source line is outside exact blob")
    if blob_sha(repo, ref["path"]) != ref["blobSha"]:
        raise ValueError("source blob SHA drift")
    line = lines[ref["line"] - 1]
    if hashlib.sha256(line.encode()).hexdigest() != ref["lineSha256"] or ref["needle"] not in line:
        raise ValueError("source line evidence drift")


def validate_source_refs(repo: Path, artifact: dict[str, Any]) -> None:
    for access in artifact["accesses"]:
        if access["surfaceClass"] != "production-runtime":
            raise ValueError("non-runtime source was misclassified as production runtime")
        for ref in access["sources"]:
            validate_single_source_ref(repo, ref)
    for item in artifact["nonSqlQualifiedIdentifiers"]:
        validate_single_source_ref(repo, item["source"])


def verify(repo: Path) -> dict[str, Any]:
    expected = build(repo)
    actual_bytes = ARTIFACT_PATH.read_bytes()
    if actual_bytes != canonical(expected):
        raise ValueError("Worker source consumer artifact drift")
    if hashlib.sha256(actual_bytes).hexdigest() != SHA_PATH.read_text().strip():
        raise ValueError("Worker source consumer artifact hash drift")
    actual = json.loads(actual_bytes)
    if actual["contractReady"] is not False or actual["sourceArtifactComplete"] is not False:
        raise ValueError("source evidence cannot authorize Contract")
    if not actual["residue"] or not all(item["blocking"] is True for item in actual["residue"]):
        raise ValueError("unresolved dynamic/runtime residue must remain blocking")
    validate_source_refs(repo, actual)
    return actual


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-repo", type=Path, default=DEFAULT_WORKER_REPO)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.refresh == args.check:
        parser.error("choose exactly one of --refresh or --check")
    if args.refresh:
        artifact = build(args.worker_repo)
        data = canonical(artifact)
        ARTIFACT_PATH.write_bytes(data)
        SHA_PATH.write_text(hashlib.sha256(data).hexdigest() + "\n")
    else:
        artifact = verify(args.worker_repo)
    print(json.dumps({"workerCommit": artifact["source"]["workerCommit"], "accessCount": len(artifact["accesses"]), "contractReady": artifact["contractReady"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
