#!/usr/bin/env python3
"""Validate the exact Issue #408 B0 Worker runtime permission generations."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = ROOT / "supabase/tests/contracts"
PERMISSION = CONTRACT_DIR / "lca_worker_runtime_permission.v1.json"
PERMISSION_SHA = CONTRACT_DIR / "lca_worker_runtime_permission.v1.sha256"
PERMISSION_SCHEMA = CONTRACT_DIR / "lca_worker_runtime_permission.v1.schema.json"
ROLLOUT = CONTRACT_DIR / "lca_worker_runtime_rollout.v1.json"
ROLLOUT_SHA = CONTRACT_DIR / "lca_worker_runtime_rollout.v1.sha256"
ROLLOUT_SCHEMA = CONTRACT_DIR / "lca_worker_runtime_rollout.v1.schema.json"

BASE_COMMIT = "269ef181e103bf57a7e15c6e82f5291005f33ded"
CANDIDATE_COMMIT = "06ab3e6b017e732b15d1edd9c7ef8f4a35139187"
BASE_MIGRATION_HEAD = "20260803090000"
CANDIDATE_MIGRATION_HEAD = "20260803163000"
ROLE = "lca_worker_runtime"

RESULT_GC_ROUTINES = (
    ("worker_lca_result_gc_attest_v1", "p_result_id uuid"),
    ("worker_lca_result_gc_claim_v1", "p_worker_id text, p_limit integer, p_lease_seconds integer"),
    ("worker_lca_result_gc_fail_v1", "p_operation_id uuid, p_claim_token uuid, p_error_code text"),
    ("worker_lca_result_gc_fence_v1", "p_operation_id uuid, p_claim_token uuid"),
    ("worker_lca_result_gc_finalize_v1", "p_operation_id uuid, p_claim_token uuid, p_object_outcome text"),
    ("worker_lca_result_gc_preview_v1", "p_limit integer"),
    ("worker_lca_result_gc_renew_v1", "p_operation_id uuid, p_claim_token uuid, p_lease_seconds integer"),
)
DOCUMENT_VALIDATION_ROUTINES = (
    ("svc_lcia_document_validation_evidence_lookup", "p_cache_keys jsonb"),
    ("svc_lcia_document_validation_evidence_record", "p_records jsonb, p_source_worker_job_id uuid"),
)


class ContractError(ValueError):
    """Raised when a permission or rollout artifact drifts."""


def formatted(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2) + "\n"


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest_value(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def digest_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def routine(name: str, arguments: str, *, owner: str) -> dict[str, Any]:
    return {
        "objectKey": f"private.{name}({arguments})",
        "schema": "private",
        "name": name,
        "identityArguments": arguments,
        "objectOwner": owner,
        "privilege": "EXECUTE",
        "grantor": owner,
        "grantable": False,
    }


def generation(
    generation_id: str,
    database_commit: str,
    migration_head: str,
    routine_specs: tuple[tuple[str, str], ...],
) -> dict[str, Any]:
    routines = [
        routine(name, arguments, owner="lca_result_gc_executor")
        for name, arguments in RESULT_GC_ROUTINES
    ]
    routines.extend(
        routine(name, arguments, owner="postgres")
        for name, arguments in routine_specs
    )
    payload = {
        "generation": generation_id,
        "databaseCommit": database_commit,
        "migrationHead": migration_head,
        "role": ROLE,
        "roleAttributes": {
            "login": False,
            "inherit": True,
            "superuser": False,
            "createDatabase": False,
            "createRole": False,
            "bypassRls": False,
            "replication": False,
            "roleConfig": None,
        },
        "creatorMembershipEdges": [{
            "member": "postgres",
            "role": ROLE,
            "grantor": "supabase_admin",
            "inherit": False,
            "set": False,
            "admin": True,
        }],
        "runtimeLoginMembershipPolicy": {
            "includedInManifest": False,
            "requiredReceipt": "workspace-identity-run-exact-readback",
            "memberClass": "deployment-owned-login",
            "role": ROLE,
            "inherit": True,
            "set": False,
            "admin": False,
        },
        "schemaPrivileges": [{
            "schema": "private",
            "usage": True,
            "create": False,
        }],
        "relationPrivileges": [],
        "sequencePrivileges": [],
        "routinePrivileges": routines,
        "counts": {
            "schemaPrivilegeCount": 1,
            "relationPrivilegeCount": 0,
            "sequencePrivilegeCount": 0,
            "routinePrivilegeCount": len(routines),
        },
    }
    return {**payload, "manifestSha256": digest_value(payload)}


def expected_permission() -> dict[str, Any]:
    return {
        "schemaVersion": "database.lca-worker-runtime-permission.v1",
        "issue": "tiangong-lca/database-engine#408",
        "role": ROLE,
        "policy": {
            "comparison": "ordered-exact-equality",
            "equalityScope": "capability-role-definition-and-object-privileges",
            "missingEntry": "reject",
            "extraEntry": "reject",
            "signatureDrift": "reject",
            "hashDrift": "reject",
            "schemaWideFutureGrants": "forbidden",
            "defaultPrivileges": "forbidden",
            "runtimeLoginMemberships": "excluded-require-exact-deployment-receipt",
            "roleAndCreatorEvidence": "exact-migration-self-guard-plus-hosted-receipt-required",
        },
        "generations": [
            generation("pre407-seven", BASE_COMMIT, BASE_MIGRATION_HEAD, ()),
            generation(
                "issue407-nine",
                CANDIDATE_COMMIT,
                CANDIDATE_MIGRATION_HEAD,
                DOCUMENT_VALIDATION_ROUTINES,
            ),
        ],
    }


def expected_rollout(permission: dict[str, Any] | None = None) -> dict[str, Any]:
    permission = permission or expected_permission()
    old, new = permission["generations"]
    return {
        "schemaVersion": "database.lca-worker-runtime-rollout.v1",
        "issue": "tiangong-lca/database-engine#408",
        "role": ROLE,
        "strategy": "exact-old-or-new-only",
        "old": {
            "generation": old["generation"],
            "databaseCommit": old["databaseCommit"],
            "migrationHead": old["migrationHead"],
            "manifestSha256": old["manifestSha256"],
        },
        "new": {
            "generation": new["generation"],
            "databaseCommit": new["databaseCommit"],
            "migrationHead": new["migrationHead"],
            "manifestSha256": new["manifestSha256"],
        },
        "acceptedManifestSha256": [old["manifestSha256"], new["manifestSha256"]],
        "supersetPolicy": "reject",
        "candidateFull": {
            "status": "blocked",
            "manifestSha256": None,
            "reason": "full Worker consumer and permission closure is not complete",
        },
    }


def _validate_sha(value: object, label: str) -> None:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ContractError(f"{label} must be an exact SHA-256")


def validate_permission(value: dict[str, Any]) -> None:
    expected = expected_permission()
    if value != expected:
        raise ContractError(
            "permission generations drifted; ordered exact equality rejects missing, extra, reordered, or changed entries"
        )
    for item in value["generations"]:
        _validate_sha(item["manifestSha256"], f"{item['generation']} manifestSha256")
        payload = {key: item[key] for key in item if key != "manifestSha256"}
        if digest_value(payload) != item["manifestSha256"]:
            raise ContractError(f"{item['generation']} manifest hash drifted")


def validate_rollout(value: dict[str, Any], permission: dict[str, Any]) -> None:
    validate_permission(permission)
    if value != expected_rollout(permission):
        raise ContractError("rollout drifted; only the exact ordered old/new manifests are accepted")
    if value["candidateFull"] != {
        "status": "blocked",
        "manifestSha256": None,
        "reason": "full Worker consumer and permission closure is not complete",
    }:
        raise ContractError("candidate-full must remain blocked")


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ContractError(f"{path.name} must contain one JSON object")
    return value


def git_output(*args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=ROOT, check=False, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise ContractError(f"immutable database Git input is unavailable: {' '.join(args)}")
    return result.stdout


def verify_git_sources(permission: dict[str, Any]) -> None:
    for item in permission["generations"]:
        commit = item["databaseCommit"]
        git_output("cat-file", "-e", f"{commit}^{{commit}}")
        paths = git_output("ls-tree", "-r", "--name-only", commit).splitlines()
        versions = sorted(
            match.group(1)
            for path in paths
            if (match := re.fullmatch(r"supabase/migrations/([0-9]{14})_.+\.sql", path))
        )
        if not versions or versions[-1] != item["migrationHead"]:
            raise ContractError(f"{item['generation']} migration head is not exact")

        catalog = json.loads(git_output(
            "show", f"{commit}:supabase/tests/contracts/database_catalog.json"
        ))
        role_acl = re.compile(rf"(?:^|[,{{]){re.escape(ROLE)}=([^/]*)/([^,}}]+)")
        observed_rows = [
            row for row in catalog["functions"]
            if role_acl.search(row.get("acl", ""))
        ]
        observed_routines = sorted(
            f"{row['schema']}.{row['name']}({row['identityArguments']})"
            for row in observed_rows
        )
        expected_routines = sorted(row["objectKey"] for row in item["routinePrivileges"])
        if observed_routines != expected_routines:
            raise ContractError(f"{item['generation']} routine ACL census differs from its exact Git catalog")
        by_key = {
            f"{row['schema']}.{row['name']}({row['identityArguments']})": row
            for row in observed_rows
        }
        for expected_row in item["routinePrivileges"]:
            observed = by_key[expected_row["objectKey"]]
            expected_acl = (
                "{" +
                f"{expected_row['objectOwner']}=X/{expected_row['objectOwner']}," +
                f"{ROLE}=X/{expected_row['grantor']}" +
                "}"
            )
            if observed.get("acl") != expected_acl or expected_row["grantable"] is not False:
                raise ContractError(
                    f"{item['generation']} routine owner/grantor/grantability ACL differs"
                )
        observed_relations = [
            row for row in catalog["relations"]
            if role_acl.search(row.get("acl", ""))
            or role_acl.search(json.dumps(row.get("columnAcl", []), sort_keys=True))
        ]
        if observed_relations or item["relationPrivileges"] or item["sequencePrivileges"]:
            raise ContractError(f"{item['generation']} relation/sequence ACL census must be empty")
        private_schema = [row for row in catalog["schemas"] if row["name"] == "private"]
        if len(private_schema) != 1:
            raise ContractError(f"{item['generation']} private schema census drifted")
        schema_match = role_acl.search(private_schema[0].get("acl", ""))
        if schema_match is None or schema_match.groups() != ("U", "postgres"):
            raise ContractError(f"{item['generation']} private schema USAGE grant drifted")
        for key in ("defaultPrivileges", "effectiveDefaultPrivileges"):
            if any(role_acl.search(json.dumps(row, sort_keys=True)) for row in catalog[key]):
                raise ContractError(f"{item['generation']} {key} contains forbidden Worker defaults")

    merge_base = git_output("merge-base", BASE_COMMIT, CANDIDATE_COMMIT).strip()
    if merge_base != BASE_COMMIT:
        raise ContractError("Issue #407 candidate is not descended from the frozen origin/dev base")


def validate_schema_headers() -> None:
    expected = {
        PERMISSION_SCHEMA: "database.lca-worker-runtime-permission.v1",
        ROLLOUT_SCHEMA: "database.lca-worker-runtime-rollout.v1",
    }
    for path, schema_version in expected.items():
        schema = load_json(path)
        if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
            raise ContractError(f"{path.name} must use JSON Schema 2020-12")
        if schema.get("type") != "object" or schema.get("additionalProperties") is not False:
            raise ContractError(f"{path.name} root must be a closed object")
        version = schema.get("properties", {}).get("schemaVersion", {}).get("const")
        if version != schema_version:
            raise ContractError(f"{path.name} schemaVersion drifted")


def check() -> None:
    permission = load_json(PERMISSION)
    rollout = load_json(ROLLOUT)
    validate_permission(permission)
    validate_rollout(rollout, permission)
    verify_git_sources(permission)
    validate_schema_headers()
    expected_files = {
        PERMISSION: formatted(expected_permission()),
        ROLLOUT: formatted(expected_rollout(permission)),
    }
    for path, expected_text in expected_files.items():
        if path.read_text(encoding="utf-8") != expected_text:
            raise ContractError(f"{path.name} formatting/order drifted")
    for artifact, digest_path in ((PERMISSION, PERMISSION_SHA), (ROLLOUT, ROLLOUT_SHA)):
        expected_digest = digest_path.read_text(encoding="utf-8").strip()
        _validate_sha(expected_digest, digest_path.name)
        if digest_file(artifact) != expected_digest:
            raise ContractError(f"{artifact.name} file hash drifted")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="validate checked-in artifacts")
    args = parser.parse_args()
    if not args.check:
        parser.error("--check is required; this B0 tool never mutates files or databases")
    check()
    print("Issue #408 Worker runtime permission generations and rollout passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
