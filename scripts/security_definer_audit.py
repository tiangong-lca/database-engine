#!/usr/bin/env python3
"""Build and verify the full public SECURITY DEFINER owner/runtime audit."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = ROOT / "supabase/tests/contracts"
INVENTORY = CONTRACT_DIR / "public_object_inventory.genesis.json"
INVENTORY_SHA = CONTRACT_DIR / "public_object_inventory.genesis.sha256"
OUT = CONTRACT_DIR / "security_definer_audit.json"
SHA = CONTRACT_DIR / "security_definer_audit.sha256"

ROLES = ("PUBLIC", "anon", "authenticated", "service_role", "api_internal_executor", "postgres")
EXPECTED = {
    "routineCount": 241,
    "issue333OwnerRuntimeResidue": 129,
    "issue333ApiResidue": 90,
    "issue333PrivateResidue": 39,
    "issue339ValidatedFacades": 14,
    "inventoryStaticClosure": 98,
    "baselineAnonAdvisorWarnings": 77,
    "baselineAuthenticatedAdvisorWarnings": 124,
    "baselineAdvisorWarnings": 201,
    "currentAnonEffectiveExecute": 93,
    "currentAuthenticatedEffectiveExecute": 140,
    "currentPublicEffectiveExecute": 17,
    "currentServiceRoleEffectiveExecute": 171,
}

DEFINITION_QUERY = r"""
select coalesce(jsonb_agg(jsonb_build_object(
  'objectKey', format('%I.%I(%s)', n.nspname, p.proname,
    pg_get_function_identity_arguments(p.oid)),
  'definition', pg_get_functiondef(p.oid)
) order by format('%I.%I(%s)', n.nspname, p.proname,
    pg_get_function_identity_arguments(p.oid))), '[]'::jsonb)::text
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where n.nspname = 'public' and p.prokind = 'f' and p.prosecdef;
"""


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def database_url() -> str:
    if value := os.environ.get("DATABASE_URL"):
        return value
    status = subprocess.run(
        ["supabase", "status", "--output", "json"], cwd=ROOT, check=True,
        text=True, stdout=subprocess.PIPE,
    )
    return json.loads(status.stdout)["DB_URL"]


def require_loopback_database_url(value: str) -> str:
    hostname = urlparse(value).hostname
    if hostname not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("SECURITY DEFINER audit requires a loopback database URL")
    return value


def load_inventory() -> tuple[dict[str, Any], str]:
    raw = INVENTORY.read_bytes()
    recorded = INVENTORY_SHA.read_text(encoding="utf-8")
    actual = hashlib.sha256(raw).hexdigest()
    if recorded != actual + "\n":
        raise ValueError("public inventory hash does not match committed bytes")
    return json.loads(raw), actual


def load_definitions(db_url: str | None = None) -> dict[str, str]:
    local_url = require_loopback_database_url(db_url or database_url())
    result = subprocess.run(
        ["psql", local_url, "-AtX", "-v", "ON_ERROR_STOP=1", "-c", DEFINITION_QUERY],
        cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE,
    )
    rows = json.loads(result.stdout)
    return {row["objectKey"]: row["definition"] for row in rows}


def authorization_signals(definition: str, obj: dict[str, Any]) -> list[str]:
    patterns = (
        ("auth-uid", r"\bauth\s*\.\s*uid\s*\("),
        ("auth-role", r"\bauth\s*\.\s*role\s*\("),
        ("jwt-claims", r"request\.jwt\.claims|current_setting\s*\(\s*['\"]request\.jwt"),
        ("session-identity", r"\b(current_user|current_role|session_user)\b"),
        ("service-role-literal", r"['\"]service_role['\"]"),
        ("row-security-setting", r"\brow_security\b"),
    )
    lowered = definition.lower()
    signals = [name for name, pattern in patterns if re.search(pattern, lowered)]
    helper_targets = [
        edge["target_key"] for edge in obj.get("dependencies", [])
        if re.search(r"\.(assert_|.*(?:is_|has_|can_).*(?:manager|owner|member|role|access|permission))", edge["target_key"])
    ]
    if helper_targets:
        signals.append("authorization-helper-dependency")
    if obj["catalog"].get("dynamicSql"):
        signals.append("dynamic-sql")
    return sorted(set(signals)) or ["none-observed"]


def cohort(obj: dict[str, Any]) -> str:
    if obj["consumerClosure"] != "owner-runtime-confirmation-required":
        return "inventory-static-closure"
    if obj["targetSchema"] == "api" and obj["ownerRole"] == "api_internal_executor":
        return "issue339-validated-rls-facade"
    return "issue333-owner-runtime-residue"


def target_requirement(obj: dict[str, Any], item_cohort: str, role: str) -> str:
    if role == "PUBLIC":
        return "deny-explicitly"
    if role == "postgres":
        return "operator-only-not-runtime-contract"
    if role == "api_internal_executor":
        if item_cohort == "issue339-validated-rls-facade":
            return "validated-rls-bound-owner"
        return "transitive-dependency-review-required"
    if obj["targetSchema"] == "private":
        if role in ("anon", "authenticated"):
            return "deny-data-api"
        return "service-or-dedicated-role-confirmation-required"
    if item_cohort == "issue339-validated-rls-facade":
        return "preserve-only-with-positive-and-negative-contract"
    return "owner-confirmation-and-role-specific-tests-required"


def risk_level(obj: dict[str, Any], grants: set[str], signals: list[str]) -> str:
    if "PUBLIC" in grants:
        return "critical-public-execute"
    if "anon" in grants:
        return "high-anonymous-execute"
    if obj["catalog"].get("dynamicSql") or signals == ["none-observed"]:
        return "high-static-authorization-uncertainty"
    if "authenticated" in grants:
        return "medium-authenticated-execute"
    return "bounded-service-or-transitive-execute"


def build(inventory: dict[str, Any], inventory_hash: str, definitions: dict[str, str]) -> dict[str, Any]:
    acl_by_object: dict[str, set[str]] = {}
    for grant in inventory["security"]["routineAcl"]:
        if grant["privilege_type"] == "EXECUTE":
            acl_by_object.setdefault(grant["object_key"], set()).add(grant["grantee"])

    routines = []
    for obj in inventory["objects"]:
        if obj["objectType"] != "function" or not obj["catalog"]["security_definer"]:
            continue
        key = obj["objectKey"]
        if key not in definitions:
            raise ValueError(f"live SECURITY DEFINER definition is missing: {key}")
        definition_hash = hashlib.sha256(definitions[key].encode("utf-8")).hexdigest()
        if definition_hash != obj["catalog"]["definitionSha256"]:
            raise ValueError(f"definition hash differs from immutable inventory: {key}")
        grants = acl_by_object.get(key, set())
        signals = authorization_signals(definitions[key], obj)
        item_cohort = cohort(obj)
        role_matrix = []
        for role in ROLES:
            sources = sorted(source for source in grants if source in (role, "PUBLIC"))
            role_matrix.append({
                "role": role,
                "observedCurrentExecute": bool(sources),
                "observedGrantSources": sources,
                "requiredContractDisposition": target_requirement(obj, item_cohort, role),
            })
        residue = item_cohort == "issue333-owner-runtime-residue"
        blockers = list(obj.get("blockers", []))
        if residue:
            blockers.extend([
                "Owner sign-off and runtime evidence are required before Contract.",
                "Issue #358 must bind the final role matrix and positive/negative tests.",
            ])
        routines.append({
            "objectKey": key,
            "targetSchema": obj["targetSchema"],
            "migrationBatch": obj["migrationBatch"],
            "cohort": item_cohort,
            "observed": {
                "ownerRole": obj["ownerRole"],
                "securityDefiner": True,
                "searchPath": sorted(
                    value for value in obj["catalog"]["config"] if value.startswith("search_path=")
                ),
                "definitionSha256": definition_hash,
                "dynamicSql": bool(obj["catalog"].get("dynamicSql")),
                "consumerClosure": obj["consumerClosure"],
                "intendedConsumers": obj["intendedConsumers"],
                "directConsumerEvidence": obj.get("consumerEvidence", []),
                "transitiveConsumerEvidence": obj.get("transitiveConsumerEvidence", []),
                "dependencyCount": len(obj.get("dependencies", [])),
                "dependentCount": len(obj.get("dependents", [])),
                "authorizationSignals": signals,
            },
            "inferred": {
                "riskLevel": risk_level(obj, grants, signals),
                "signalLimit": "static-signals-are-not-runtime-authorization-proof",
            },
            "required": {
                "ownerSignoff": "required-before-contract" if residue else "tracked-by-downstream-contract",
                "runtimeEvidence": "required-before-contract" if residue else "retain-existing-static-evidence",
                "roleMatrix": role_matrix,
                "positiveNegativeTests": "required-by-exact-signature-before-contract",
                "handoffIssue": "tiangong-lca/database-engine#358",
            },
            "confirmed": {
                "issue339AclValidation": item_cohort == "issue339-validated-rls-facade",
                "staticConsumerClosure": obj["consumerClosure"] != "owner-runtime-confirmation-required",
                "ownerRuntime": False,
                "platformOwnerDefaultPrivileges": False,
            },
            "blockers": sorted(set(blockers)),
        })

    routines.sort(key=lambda item: item["objectKey"])
    audit = {
        "schemaVersion": "database.security-definer-audit.v1",
        "source": {
            "inventorySchemaVersion": inventory["schemaVersion"],
            "inventorySha256": inventory_hash,
            "databaseSchemaSha": inventory["source"]["databaseSchemaSha"],
            "issue": "tiangong-lca/database-engine#333",
        },
        "boundaries": {
            "repoOwnedAcl": {
                "issue": "tiangong-lca/database-engine#339",
                "status": "validated",
            },
            "platformOwnerDefaultPrivileges": {
                "issue": "tiangong-lca/database-engine#352",
                "status": "blocked",
                "coveredByThisAudit": False,
            },
            "contractMigration": {
                "issue": "tiangong-lca/database-engine#358",
                "status": "not-started",
                "coveredByThisAudit": False,
            },
        },
        "summary": {},
        "auditArtifactComplete": True,
        "contractReady": False,
        "routines": routines,
    }
    audit["summary"] = summarize(audit)
    validate(audit, inventory, inventory_hash)
    return audit


def summarize(audit: dict[str, Any]) -> dict[str, int]:
    routines = audit["routines"]
    residue = [item for item in routines if item["cohort"] == "issue333-owner-runtime-residue"]

    def current(role: str) -> int:
        return sum(
            next(row["observedCurrentExecute"] for row in item["required"]["roleMatrix"] if row["role"] == role)
            for item in routines
        )

    return {
        "routineCount": len(routines),
        "issue333OwnerRuntimeResidue": len(residue),
        "issue333ApiResidue": sum(item["targetSchema"] == "api" for item in residue),
        "issue333PrivateResidue": sum(item["targetSchema"] == "private" for item in residue),
        "issue339ValidatedFacades": sum(item["cohort"] == "issue339-validated-rls-facade" for item in routines),
        "inventoryStaticClosure": sum(item["cohort"] == "inventory-static-closure" for item in routines),
        "baselineAnonAdvisorWarnings": 77,
        "baselineAuthenticatedAdvisorWarnings": 124,
        "baselineAdvisorWarnings": 201,
        "currentAnonEffectiveExecute": current("anon"),
        "currentAuthenticatedEffectiveExecute": current("authenticated"),
        "currentPublicEffectiveExecute": current("PUBLIC"),
        "currentServiceRoleEffectiveExecute": current("service_role"),
    }


def validate(audit: dict[str, Any], inventory: dict[str, Any], inventory_hash: str) -> None:
    if audit.get("schemaVersion") != "database.security-definer-audit.v1":
        raise ValueError("unexpected SECURITY DEFINER audit schemaVersion")
    if audit.get("source", {}).get("inventorySha256") != inventory_hash:
        raise ValueError("SECURITY DEFINER audit is not bound to the committed inventory hash")
    if audit.get("source", {}).get("inventorySchemaVersion") != inventory["schemaVersion"]:
        raise ValueError("SECURITY DEFINER audit inventory schemaVersion differs from its source")
    if audit.get("source", {}).get("databaseSchemaSha") != inventory["source"]["databaseSchemaSha"]:
        raise ValueError("SECURITY DEFINER audit schema SHA differs from inventory provenance")
    if audit.get("source", {}).get("issue") != "tiangong-lca/database-engine#333":
        raise ValueError("SECURITY DEFINER audit source Issue is not #333")
    if audit.get("summary") != EXPECTED:
        raise ValueError(f"SECURITY DEFINER audit summary differs from reviewed baseline: {audit.get('summary')}")
    if audit.get("auditArtifactComplete") is not True:
        raise ValueError("SECURITY DEFINER audit artifact is not complete")
    if audit.get("contractReady") is not False:
        raise ValueError("SECURITY DEFINER audit must remain fail closed before Issue #358")
    if audit.get("boundaries", {}).get("repoOwnedAcl") != {
        "issue": "tiangong-lca/database-engine#339",
        "status": "validated",
    }:
        raise ValueError("Issue #339 repo-owned ACL boundary differs from validated evidence")
    platform = audit.get("boundaries", {}).get("platformOwnerDefaultPrivileges", {})
    if platform != {
        "issue": "tiangong-lca/database-engine#352",
        "status": "blocked",
        "coveredByThisAudit": False,
    }:
        raise ValueError("Issue #352 platform-owner blocker must remain explicit and unresolved")
    if audit.get("boundaries", {}).get("contractMigration") != {
        "issue": "tiangong-lca/database-engine#358",
        "status": "not-started",
        "coveredByThisAudit": False,
    }:
        raise ValueError("Issue #358 Contract handoff must remain explicit and unresolved")
    routines = audit.get("routines", [])
    keys = [item.get("objectKey") for item in routines]
    if len(keys) != len(set(keys)):
        raise ValueError("SECURITY DEFINER audit contains duplicate signatures")

    inventory_objects = {
        obj["objectKey"]: obj for obj in inventory["objects"]
        if obj["objectType"] == "function" and obj["catalog"]["security_definer"]
    }
    if set(keys) != set(inventory_objects):
        raise ValueError("SECURITY DEFINER audit signature set differs from immutable inventory")
    acl_by_object: dict[str, set[str]] = {}
    for grant in inventory["security"]["routineAcl"]:
        if grant["privilege_type"] == "EXECUTE":
            acl_by_object.setdefault(grant["object_key"], set()).add(grant["grantee"])

    for item in routines:
        obj = inventory_objects[item["objectKey"]]
        expected_observed = {
            "ownerRole": obj["ownerRole"],
            "securityDefiner": True,
            "searchPath": sorted(
                value for value in obj["catalog"]["config"] if value.startswith("search_path=")
            ),
            "definitionSha256": obj["catalog"]["definitionSha256"],
            "dynamicSql": bool(obj["catalog"].get("dynamicSql")),
            "consumerClosure": obj["consumerClosure"],
            "intendedConsumers": obj["intendedConsumers"],
            "directConsumerEvidence": obj.get("consumerEvidence", []),
            "transitiveConsumerEvidence": obj.get("transitiveConsumerEvidence", []),
            "dependencyCount": len(obj.get("dependencies", [])),
            "dependentCount": len(obj.get("dependents", [])),
        }
        if item.get("targetSchema") != obj["targetSchema"] or item.get("migrationBatch") != obj["migrationBatch"]:
            raise ValueError(f"routing facts differ from immutable inventory: {item['objectKey']}")
        if item.get("cohort") != cohort(obj):
            raise ValueError(f"audit cohort differs from immutable inventory: {item['objectKey']}")
        for field, expected_value in expected_observed.items():
            if item.get("observed", {}).get(field) != expected_value:
                raise ValueError(f"observed {field} differs from immutable inventory: {item['objectKey']}")
        if item["observed"]["securityDefiner"] is not True:
            raise ValueError(f"non-SECURITY DEFINER routine entered audit: {item['objectKey']}")
        if not item["observed"]["searchPath"]:
            raise ValueError(f"fixed search_path evidence is missing: {item['objectKey']}")
        if [row["role"] for row in item["required"]["roleMatrix"]] != list(ROLES):
            raise ValueError(f"role matrix is incomplete or unordered: {item['objectKey']}")
        grants = acl_by_object.get(item["objectKey"], set())
        for row in item["required"]["roleMatrix"]:
            expected_sources = sorted(source for source in grants if source in (row["role"], "PUBLIC"))
            if row["observedGrantSources"] != expected_sources or row["observedCurrentExecute"] != bool(expected_sources):
                raise ValueError(f"observed role matrix differs from immutable inventory: {item['objectKey']}")
        if item["confirmed"]["ownerRuntime"] is not False:
            raise ValueError(f"static evidence cannot claim runtime owner confirmation: {item['objectKey']}")
        if item["inferred"]["signalLimit"] != "static-signals-are-not-runtime-authorization-proof":
            raise ValueError(f"static authorization signal limitation is missing: {item['objectKey']}")


def verify_committed() -> dict[str, Any]:
    inventory, inventory_hash = load_inventory()
    raw = OUT.read_bytes()
    audit = json.loads(raw)
    if raw != canonical(audit).encode("utf-8"):
        raise ValueError("committed SECURITY DEFINER audit JSON is not canonical")
    if SHA.read_text(encoding="utf-8") != hashlib.sha256(raw).hexdigest() + "\n":
        raise ValueError("committed SECURITY DEFINER audit hash does not match JSON bytes")
    validate(audit, inventory, inventory_hash)
    return audit


def write_or_check(write: bool) -> None:
    inventory, inventory_hash = load_inventory()
    generated = build(inventory, inventory_hash, load_definitions())
    rendered = canonical(generated)
    digest = hashlib.sha256(rendered.encode("utf-8")).hexdigest() + "\n"
    if write:
        OUT.write_text(rendered, encoding="utf-8")
        SHA.write_text(digest, encoding="utf-8")
    else:
        verify_committed()
        if OUT.read_text(encoding="utf-8") != rendered or SHA.read_text(encoding="utf-8") != digest:
            raise ValueError("committed SECURITY DEFINER audit differs from exact-schema generation")
    print(json.dumps({"summary": generated["summary"], "sha256": digest.strip()}, sort_keys=True))


def check_frozen_baseline() -> dict[str, Any]:
    """Verify immutable v1 bytes/provenance without reading transition catalog state."""
    audit = verify_committed()
    print(json.dumps({
        "summary": audit["summary"],
        "sha256": SHA.read_text(encoding="utf-8").strip(),
        "frozenBaseline": True,
    }, sort_keys=True))
    return audit


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--write", action="store_true")
    group.add_argument("--check", action="store_true")
    group.add_argument("--check-live-baseline", action="store_true")
    args = parser.parse_args()
    if args.check:
        check_frozen_baseline()
    else:
        write_or_check(args.write)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
