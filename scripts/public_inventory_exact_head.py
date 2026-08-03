#!/usr/bin/env python3
"""Generate and verify the Issue #405 exact-head public-object closure.

This tool is deliberately read-only with respect to PostgreSQL.  ``--refresh``
rewrites only checked-in inventory artifacts from an exact local database.
It never emits or executes DDL and it can never authorize Contract.
"""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import ipaddress
import json
import os
import re
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent))
import public_inventory_closure as v1


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = ROOT / "supabase/tests/contracts"
PARTITION = CONTRACT_DIR / "public_object_partition.v2.tsv"
PARTITION_SHA = CONTRACT_DIR / "public_object_partition.v2.sha256"
LEDGER = CONTRACT_DIR / "public_object_target_ledger.tsv"
LEDGER_SHA = CONTRACT_DIR / "public_object_target_ledger.sha256"
INVENTORY = CONTRACT_DIR / "public_object_inventory.json"
INVENTORY_SHA = CONTRACT_DIR / "public_object_inventory.sha256"
INVENTORY_SCHEMA = CONTRACT_DIR / "public_object_inventory.schema.json"
DROP_CHECKLIST = CONTRACT_DIR / "public_object_contract_drop_checklist.v2.json"
DROP_CHECKLIST_SHA = CONTRACT_DIR / "public_object_contract_drop_checklist.v2.sha256"
DROP_CHECKLIST_SCHEMA = CONTRACT_DIR / "public_object_contract_drop_checklist.v2.schema.json"

SCHEMA_VERSION = "database.public-object-inventory-closure.v2"
DROP_SCHEMA_VERSION = "database.public-object-contract-drop-checklist.v2"
DATABASE_SOURCE_SHA = "c5356d2b0d340f9c5c31a645479be5f3d19a52db"
MIGRATION_HEAD = "20260803090000"
MIGRATION_TREE_SHA256 = "9fb83e143cef7dc8d12d058bbb7d22ecce38877dd3cef116f2bfd700b986bf9c"
PREDECESSOR_SHA256 = "2526146dc64e2b32bdf9afb2ebcc0495f5a174f241c2abbd7f0f1b5348aa8c18"
EXPECTED_COUNTS = {"function": 336, "table": 49, "view": 12}
EXPECTED_PARTITIONS = {
    "core": 9,
    "predecessor": 37,
    "issue357": 117,
    "issue358": 230,
    "omitted-explicit": 4,
}
CORE_KEYS = {f"public.{name}" for name in v1.CORE_TABLES}

LEDGER_FIELDS = (
    "object_key", "object_type", "current_schema", "object_name", "owner_role",
    "rls_enabled", "anon_access", "authenticated_access", "service_access",
    "estimated_rows", "candidate_target", "candidate_basis", "review_status",
    "security_mode", "search_path", "partition", "physical_moved",
    "compat_present", "adapter_only", "retired",
)

COUNTERPART_SQL = r"""
select coalesce(jsonb_agg(item order by item->>'schema',item->>'name',item->>'identityArguments'),
                          '[]'::jsonb)::text
from (
  select jsonb_build_object(
    'schema',n.nspname,'name',c.relname,
    'objectType',case c.relkind when 'r' then 'table' when 'p' then 'partitioned_table'
      when 'v' then 'view' when 'm' then 'materialized_view' end,
    'identityArguments','', 'physical',c.relkind in ('r','p')) item
  from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname in ('api','private','util','archive') and c.relkind in ('r','p','v','m')
  union all
  select jsonb_build_object(
    'schema',n.nspname,'name',p.proname,
    'objectType',case p.prokind when 'p' then 'procedure' else 'function' end,
    'identityArguments',pg_get_function_identity_arguments(p.oid), 'physical',false) item
  from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where n.nspname in ('api','private','util','archive') and p.prokind in ('f','p')
) candidates;
"""

EXTENSION_SQL = r"""
with public_objects as (
  select c.oid, 'pg_class'::regclass as classid,
         format('public.%I',c.relname) as object_key
  from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind in ('r','p','v','m')
  union all
  select p.oid, 'pg_proc'::regclass,
         format('public.%I(%s)',p.proname,pg_get_function_identity_arguments(p.oid))
  from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='public' and p.prokind in ('f','p')
)
select coalesce(jsonb_agg(jsonb_build_object('objectKey',o.object_key,'extension',e.extname)
                          order by o.object_key),'[]'::jsonb)::text
from public_objects o
join pg_depend d on d.classid=o.classid and d.objid=o.oid and d.deptype='e'
join pg_extension e on e.oid=d.refobjid;
"""


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream, delimiter="\t"))


def canonical_tsv(rows: list[dict[str, Any]], fields: tuple[str, ...]) -> bytes:
    import io
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(
        stream, fieldnames=fields, delimiter="\t", lineterminator="\n",
        extrasaction="ignore",
    )
    writer.writeheader()
    for row in sorted(rows, key=lambda item: item[fields[0]]):
        writer.writerow({field: row.get(field, "") for field in fields})
    return stream.getvalue().encode("utf-8")


def psql_json(sql: str, db_url: str) -> Any:
    result = subprocess.run(
        ["psql", db_url, "-XAt", "-v", "ON_ERROR_STOP=1", "-c", sql],
        cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE,
    )
    return json.loads(result.stdout)


def db_url(value: str | None) -> str:
    """Require an explicit, unambiguous loopback PostgreSQL URL."""
    if not value:
        raise ValueError("live actions require an explicit --db-url loopback URL")
    if any(character.isspace() for character in value) or "\\" in value or "%" in value:
        raise ValueError("database URL contains ambiguous encoding")
    try:
        parsed = urlsplit(value)
        host = parsed.hostname
        _ = parsed.port
    except ValueError as error:
        raise ValueError("database URL is not a valid loopback PostgreSQL URL") from error
    if parsed.scheme not in {"postgres", "postgresql"} or not host or not parsed.path:
        raise ValueError("database URL is not a valid PostgreSQL URL")
    if parsed.query or parsed.fragment:
        raise ValueError("database URL query overrides and fragments are forbidden")
    if host != "localhost":
        try:
            if not ipaddress.ip_address(host).is_loopback:
                raise ValueError("database URL host must be loopback")
        except ValueError as error:
            if str(error) == "database URL host must be loopback":
                raise
            raise ValueError("database URL host must be loopback") from error
    return value


def migration_tree_sha256() -> str:
    lines = [
        f"{path.name}\t{sha256_bytes(path.read_bytes())}"
        for path in sorted((ROOT / "supabase/migrations").glob("*.sql"))
    ]
    return sha256_bytes(("\n".join(lines) + "\n").encode("utf-8"))


def expected_migration_versions() -> list[str]:
    return [path.name.split("_", 1)[0] for path in sorted((ROOT / "supabase/migrations").glob("*.sql"))]


def validate_live_migrations(url: str) -> None:
    applied = psql_json(
        "select coalesce(jsonb_agg(version order by version),'[]'::jsonb)::text "
        "from supabase_migrations.schema_migrations", url,
    )
    expected = expected_migration_versions()
    if applied != expected:
        raise ValueError(
            f"live applied migration history differs from reviewed exact set/head: "
            f"expected={len(expected)}, applied={len(applied)}"
        )


def load_live(url: str) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, str]]]:
    validate_live_migrations(url)
    old = os.environ.get("DATABASE_URL")
    try:
        os.environ["DATABASE_URL"] = url
        catalog = v1.load_catalog()
    finally:
        if old is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = old
    counterparts = psql_json(COUNTERPART_SQL, url)
    extension_owned = psql_json(EXTENSION_SQL, url)
    return catalog, counterparts, extension_owned


def validate_source() -> None:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, text=True,
        stdout=subprocess.PIPE,
    ).stdout.strip()
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", DATABASE_SOURCE_SHA, head], cwd=ROOT,
        check=False,
    )
    if result.returncode != 0:
        raise ValueError(f"HEAD does not descend from exact source {DATABASE_SOURCE_SHA}")
    migrations = sorted((ROOT / "supabase/migrations").glob("*.sql"))
    if not migrations or migrations[-1].name.split("_", 1)[0] != MIGRATION_HEAD:
        raise ValueError("committed migration head differs from the reviewed exact head")
    actual_tree = migration_tree_sha256()
    if actual_tree != MIGRATION_TREE_SHA256:
        raise ValueError(f"committed migration tree differs from reviewed exact tree: {actual_tree}")


def validate_partition(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    keys = [row.get("object_key", "") for row in rows]
    if len(keys) != len(set(keys)):
        raise ValueError("partition contains duplicate object identities")
    counts = Counter(row.get("partition") for row in rows)
    if dict(sorted(counts.items())) != dict(sorted(EXPECTED_PARTITIONS.items())):
        raise ValueError(f"partition count drift: {dict(counts)}")
    if {row["object_key"] for row in rows if row["partition"] == "core"} != CORE_KEYS:
        raise ValueError("core partition differs from the exact nine allowlist")
    if any(row["target_schema"] == "public" for row in rows if row["partition"] != "core"):
        raise ValueError("non-core partition contains a public target")
    if any(row["target_schema"] != "public" for row in rows if row["partition"] == "core"):
        raise ValueError("core partition contains a non-public target")
    return {row["object_key"]: row for row in rows}


def acl_matrix(catalog: dict[str, Any], object_key: str, object_type: str) -> dict[str, bool]:
    section = "routineAcl" if object_type in ("function", "procedure") else "relationAcl"
    privilege = "EXECUTE" if section == "routineAcl" else "SELECT"
    grants = {
        row["grantee"] for row in catalog[section]
        if row["object_key"] == object_key and row["privilege_type"] == privilege
    }
    return {
        "anon_access": bool(grants & {"PUBLIC", "anon"}),
        "authenticated_access": bool(grants & {"PUBLIC", "authenticated"}),
        "service_access": bool(grants & {"PUBLIC", "service_role"}),
    }


def routine_parts(object_key: str) -> tuple[str, str]:
    match = re.fullmatch(r"public\.([^()]+)\((.*)\)", object_key)
    if not match:
        raise ValueError(f"invalid routine identity: {object_key}")
    return match.group(1), match.group(2)


def normalized_arguments(arguments: str, target_schema: str) -> str:
    return re.sub(rf"\b{re.escape(target_schema)}\.", "", arguments)


def lifecycle_state(
    actual: dict[str, Any], assignment: dict[str, str], candidates: list[dict[str, Any]],
) -> dict[str, bool]:
    target = assignment["target_schema"]
    if target in {"public", "retire"}:
        matches: list[dict[str, Any]] = []
    elif actual["object_type"] in ("function", "procedure"):
        name, arguments = routine_parts(actual["object_key"])
        matches = [
            item for item in candidates
            if item["schema"] == target and item["name"] == name
            and item["objectType"] == actual["object_type"]
            and normalized_arguments(item["identityArguments"], target) == arguments
        ]
    else:
        matches = [
            item for item in candidates
            if item["schema"] == target and item["name"] == actual["object_name"]
        ]
    counterpart_exists = bool(matches)
    physical_moved = any(item["physical"] for item in matches)
    public_is_adapter_kind = actual["object_type"] in (
        "view", "materialized_view", "function", "procedure",
    )
    return {
        "physicalMoved": physical_moved,
        "compatPresent": counterpart_exists,
        "adapterOnly": counterpart_exists and public_is_adapter_kind,
        "retired": False,
    }


def refreshed_ledger(
    catalog: dict[str, Any], counterparts: list[dict[str, Any]], partition: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    previous = {row["object_key"]: row for row in read_tsv(LEDGER)} if LEDGER.exists() else {}
    live = {item["object_key"]: item for item in catalog["relations"] + catalog["routines"]}
    if set(live) != set(partition):
        missing = sorted(set(partition) - set(live))
        unknown = sorted(set(live) - set(partition))
        raise ValueError(f"live/partition identity drift: missing={missing}, unknown={unknown}")
    rows: list[dict[str, Any]] = []
    for key in sorted(live):
        actual = live[key]
        assignment = partition[key]
        state = lifecycle_state(actual, assignment, counterparts)
        prior = previous.get(key, {})
        is_routine = actual["object_type"] in ("function", "procedure")
        security_mode = ""
        search_path = ""
        if is_routine:
            security_mode = "security_definer" if actual["security_definer"] else "security_invoker"
            search_path = ";".join(actual.get("config", []))
        elif actual["object_type"] in ("view", "materialized_view"):
            security_mode = "security_invoker" if "security_invoker=true" in actual.get("options", []) else "view_default"
        rows.append({
            "object_key": key,
            "object_type": actual["object_type"],
            "current_schema": "public",
            "object_name": actual["object_name"],
            "owner_role": actual["owner_role"],
            "rls_enabled": "true" if actual.get("rls_enabled") else ("false" if actual["object_type"] in ("table", "partitioned_table") else ""),
            **{name: "true" if value else "false" for name, value in acl_matrix(catalog, key, actual["object_type"]).items()},
            "estimated_rows": prior.get("estimated_rows") or "0",
            "candidate_target": assignment["target_schema"],
            "candidate_basis": assignment["decision_basis"],
            "review_status": prior.get("review_status") or "pending",
            "security_mode": security_mode,
            "search_path": search_path,
            "partition": assignment["partition"],
            "physical_moved": str(bool(state["physicalMoved"])).lower(),
            "compat_present": str(bool(state["compatPresent"])).lower(),
            "adapter_only": str(bool(state["adapterOnly"])).lower(),
            "retired": "false",
        })
    return rows


def dependencies(catalog: dict[str, Any]) -> list[dict[str, str]]:
    edges: list[dict[str, str]] = []
    for section, kind in (
        ("foreignKeys", "foreign-key"), ("triggers", "trigger-function"),
        ("rewrites", "view-rewrite"), ("policyDependencies", "policy"),
        ("signatureDependencies", "routine-signature-composite"),
    ):
        edges.extend({**edge, "kind": kind} for edge in catalog[section])
    for edge in v1.body_dependencies(catalog):
        edges.append({
            **{key: value for key, value in edge.items() if key != "dependency_name"},
            "kind": edge["dependency_name"],
        })
    return sorted(edges, key=lambda edge: (
        edge["source_key"], edge["target_key"], edge["kind"], edge.get("dependency_name", ""),
    ))


def build_inventory(
    catalog_input: dict[str, Any], ledger_rows: list[dict[str, Any]], extension_owned: list[dict[str, str]],
    ledger_sha: str, partition_sha: str,
) -> dict[str, Any]:
    catalog = copy.deepcopy(catalog_input)
    edges = dependencies(catalog)
    actual = {item["object_key"]: item for item in catalog["relations"] + catalog["routines"]}
    outgoing: dict[str, list[dict[str, str]]] = defaultdict(list)
    incoming: dict[str, list[dict[str, str]]] = defaultdict(list)
    for edge in edges:
        outgoing[edge["source_key"]].append(edge)
        incoming[edge["target_key"]].append(edge)
    consumers = v1.load_consumers()
    evidence = consumers.get("objects", {})
    objects = []
    residue = {
        "missingFromLiveCatalog": [],
        "unmappedLiveObjects": [],
        "duplicatePartitionObjects": [],
        "partitionCountDrift": [],
        "objectsWithoutConsumerClosure": [],
        "dynamicSqlReviewRequired": [],
        "retirementBlocked": [],
        "contractAuthorizationMissing": ["database-engine#358 Contract authorization has not been granted"],
    }
    for row in ledger_rows:
        key = row["object_key"]
        item = actual[key]
        object_evidence = evidence.get(key, [])
        closure = "exact-static-evidence" if object_evidence else "owner-runtime-confirmation-required"
        blockers = []
        if row["partition"] != "core" and closure != "exact-static-evidence":
            residue["objectsWithoutConsumerClosure"].append(key)
            blockers.append("Exact runtime telemetry and owner sign-off are required before Contract.")
        if item.get("dynamicSql"):
            residue["dynamicSqlReviewRequired"].append(key)
            blockers.append("Dynamic SQL/regclass evidence remains a Contract blocker.")
        if row["candidate_target"] == "retire":
            residue["retirementBlocked"].append(key)
            blockers.append("Retirement requires static/runtime/owner zero evidence.")
        target = row["candidate_target"]
        objects.append({
            "objectKey": key,
            "objectType": row["object_type"],
            "currentSchema": "public",
            "objectName": row["object_name"],
            "ownerRole": row["owner_role"],
            "targetSchema": target,
            "partition": row["partition"],
            "decision": "retain" if row["partition"] == "core" else "move-or-retire-after-contract",
            "decisionBasis": row["candidate_basis"],
            "ownerRepo": "tiangong-lca/database-engine",
            "sourceOfTruth": "supabase/migrations/**",
            # Preserve the stable domain batch used by existing focused gates;
            # ``partition`` separately records the Issue #405 exactly-once owner.
            "migrationBatch": v1.migration_batch({
                "object_name": row["object_name"],
                "object_type": row["object_type"],
            }, target),
            "testGate": "issue-405-exact-head-partition-and-contract-drop-closure",
            "intendedConsumers": {
                "public": ["anon", "authenticated", "service_role"],
                "api": ["explicit-browser-or-service-grants"],
                "private": ["service-or-dedicated-direct-db-role"],
                "util": ["operator-role"],
                "archive": ["operator-role"],
                "retire": [],
            }[target],
            "roleContract": "explicit-object-grants-plus-schema-usage-plus-rls-when-relational",
            "consumerClosure": closure,
            "consumerEvidence": object_evidence,
            "transitiveConsumerEvidence": [],
            "blockers": blockers,
            "lifecycleState": {
                "physicalMoved": row["physical_moved"] == "true",
                "compatPresent": row["compat_present"] == "true",
                "adapterOnly": row["adapter_only"] == "true",
                "retired": row["retired"] == "true",
            },
            "catalog": item,
            "dependencies": outgoing[key],
            "dependents": incoming[key],
        })
    counts = dict(sorted(Counter(obj["objectType"] for obj in objects).items()))
    partition_counts = dict(sorted(Counter(obj["partition"] for obj in objects).items()))
    noncore = {obj["objectKey"] for obj in objects if obj["partition"] != "core"}
    return {
        "schemaVersion": SCHEMA_VERSION,
        "source": {
            "issue": "tiangong-lca/database-engine#405",
            "databaseSchemaSha": DATABASE_SOURCE_SHA,
            "migrationHead": MIGRATION_HEAD,
            "migrationTreeSha256": MIGRATION_TREE_SHA256,
            "predecessorInventorySha256": PREDECESSOR_SHA256,
            "partitionSha256": partition_sha,
            "ledgerSha256": ledger_sha,
        },
        "counts": counts,
        "partitionCounts": partition_counts,
        "finalPublicAllowlist": sorted(CORE_KEYS),
        "objects": objects,
        "dependencies": edges,
        "migrationPlan": v1.dependency_plan(set(actual), edges, objects),
        "contractDropPlan": v1.dependency_plan(noncore, edges, objects),
        "security": {
            "relationAcl": catalog["relationAcl"], "routineAcl": catalog["routineAcl"],
            "policies": catalog["policies"], "defaultPrivileges": catalog["defaultPrivileges"],
            "schemaAcl": catalog["schemaAcl"], "publications": catalog["publications"],
            "extensionOwnedPublicObjectsExcluded": extension_owned,
        },
        "consumerEvidence": {"repositories": consumers["repositories"]},
        "residue": residue,
        "contractReady": False,
    }


def build_drop_checklist(inventory: dict[str, Any]) -> dict[str, Any]:
    entries = []
    for item in inventory["objects"]:
        if item["partition"] == "core":
            continue
        kind = {
            "table": "TABLE", "partitioned_table": "TABLE", "view": "VIEW",
            "materialized_view": "MATERIALIZED VIEW", "function": "FUNCTION",
            "procedure": "PROCEDURE",
        }[item["objectType"]]
        entries.append({
            "objectKey": item["objectKey"],
            "objectType": item["objectType"],
            "partition": item["partition"],
            "targetSchema": item["targetSchema"],
            "dropIdentity": f"{kind} {item['objectKey']}",
            "lifecycleState": item["lifecycleState"],
            "status": "blocked",
            "blockers": [
                "exact-SHA consumer zero is not complete",
                "persistent Dev burn-in is not complete",
                "database-engine#358 Contract authorization is absent",
            ],
        })
    return {
        "schemaVersion": DROP_SCHEMA_VERSION,
        "sourceInventorySha256": "",  # bound after inventory bytes are canonicalized
        "finalPublicAllowlist": sorted(CORE_KEYS),
        "entryCount": len(entries),
        "entries": entries,
        "contractReady": False,
    }


def expected_drop_identity(item: dict[str, Any]) -> str:
    kind = {
        "table": "TABLE", "partitioned_table": "TABLE", "view": "VIEW",
        "materialized_view": "MATERIALIZED VIEW", "function": "FUNCTION",
        "procedure": "PROCEDURE",
    }[item["objectType"]]
    return f"{kind} {item['objectKey']}"


def validate_contracts(
    inventory: dict[str, Any], checklist: dict[str, Any],
    partition_rows: list[dict[str, str]] | None = None,
    ledger_rows: list[dict[str, str]] | None = None,
) -> None:
    errors = []
    partition_rows = read_tsv(PARTITION) if partition_rows is None else partition_rows
    ledger_rows = read_tsv(LEDGER) if ledger_rows is None else ledger_rows
    partition = validate_partition(partition_rows)
    ledger = {row["object_key"]: row for row in ledger_rows}
    if len(ledger) != len(ledger_rows):
        errors.append("ledger contains duplicate object identities")
    if inventory.get("schemaVersion") != SCHEMA_VERSION:
        errors.append("unexpected inventory schema version")
    if inventory.get("counts") != EXPECTED_COUNTS:
        errors.append(f"catalog count drift: {inventory.get('counts')}")
    if inventory.get("partitionCounts") != dict(sorted(EXPECTED_PARTITIONS.items())):
        errors.append(f"partition drift: {inventory.get('partitionCounts')}")
    keys = [item["objectKey"] for item in inventory.get("objects", [])]
    if len(keys) != 397 or len(set(keys)) != 397:
        errors.append("inventory must contain exactly 397 unique identities")
    noncore = set(keys) - CORE_KEYS
    objects = {item["objectKey"]: item for item in inventory.get("objects", [])}
    if set(objects) != set(partition) or set(ledger) != set(partition):
        errors.append("partition, ledger, and inventory identity sets differ")
    recomputed_counts = dict(sorted(Counter(item["objectType"] for item in objects.values()).items()))
    recomputed_partitions = dict(sorted(Counter(item["partition"] for item in objects.values()).items()))
    if inventory.get("counts") != recomputed_counts:
        errors.append("inventory counts are not derived from objects")
    if inventory.get("partitionCounts") != recomputed_partitions:
        errors.append("inventory partitionCounts are not derived from objects")
    for key in set(objects) & set(partition) & set(ledger):
        item, assignment, row = objects[key], partition[key], ledger[key]
        expected = (
            assignment["partition"], assignment["target_schema"], assignment["decision_basis"]
        )
        if (item["partition"], item["targetSchema"], item["decisionBasis"]) != expected:
            errors.append(f"inventory assignment differs from partition: {key}")
        if (row["partition"], row["candidate_target"], row["candidate_basis"]) != expected:
            errors.append(f"ledger assignment differs from partition: {key}")
        if row["object_type"] != item["objectType"] or row["object_name"] != item["objectName"]:
            errors.append(f"ledger identity metadata differs from inventory: {key}")
        ledger_state = {
            "physicalMoved": row["physical_moved"] == "true",
            "compatPresent": row["compat_present"] == "true",
            "adapterOnly": row["adapter_only"] == "true",
            "retired": row["retired"] == "true",
        }
        if ledger_state != item["lifecycleState"]:
            errors.append(f"ledger lifecycle state differs from inventory: {key}")
    drop_keys = [item["objectKey"] for item in checklist.get("entries", [])]
    if len(drop_keys) != 388 or len(set(drop_keys)) != 388 or set(drop_keys) != noncore:
        errors.append("Contract DROP checklist must close exactly once over all 388 residue identities")
    if checklist.get("entryCount") != 388:
        errors.append("Contract DROP checklist count must be 388")
    if any(entry.get("status") != "blocked" for entry in checklist.get("entries", [])):
        errors.append("every Contract DROP identity must remain blocked")
    for entry in checklist.get("entries", []):
        item = objects.get(entry.get("objectKey"))
        if not item:
            continue
        if (
            entry.get("objectType") != item["objectType"]
            or entry.get("partition") != item["partition"]
            or entry.get("targetSchema") != item["targetSchema"]
            or entry.get("lifecycleState") != item["lifecycleState"]
            or entry.get("dropIdentity") != expected_drop_identity(item)
        ):
            errors.append(f"DROP checklist identity/assignment differs from inventory: {item['objectKey']}")
    valid_state_keys = {"physicalMoved", "compatPresent", "adapterOnly", "retired"}
    if any(set(item.get("lifecycleState", {})) != valid_state_keys for item in inventory.get("objects", [])):
        errors.append("lifecycle state model is incomplete")
    if inventory.get("contractReady") is not False or checklist.get("contractReady") is not False:
        errors.append("Issue #405 artifacts can never set contractReady=true")
    if inventory["security"].get("extensionOwnedPublicObjectsExcluded"):
        errors.append("extension-owned public objects must be excluded from the application inventory")
    if errors:
        raise ValueError("Issue #405 exact-head validation failed:\n" + "\n".join(errors))


def schema_validate(inventory: dict[str, Any], checklist: dict[str, Any]) -> None:
    try:
        import jsonschema
    except ImportError as error:
        raise ValueError("jsonschema is required for inventory validation") from error
    jsonschema.Draft202012Validator(
        json.loads(INVENTORY_SCHEMA.read_text(encoding="utf-8")),
    ).validate(inventory)
    jsonschema.Draft202012Validator(
        json.loads(DROP_CHECKLIST_SCHEMA.read_text(encoding="utf-8")),
    ).validate(checklist)


def write_hash(path: Path, payload: bytes) -> str:
    digest = sha256_bytes(payload)
    path.write_text(digest + "\n", encoding="utf-8")
    return digest


def refresh(url: str) -> dict[str, Any]:
    validate_source()
    partition_rows = read_tsv(PARTITION)
    partition = validate_partition(partition_rows)
    partition_payload = canonical_tsv(
        partition_rows, ("object_key", "partition", "target_schema", "decision_basis"),
    )
    if PARTITION.read_bytes() != partition_payload:
        raise ValueError("partition TSV is not deterministic canonical order/bytes")
    partition_sha = write_hash(PARTITION_SHA, partition_payload)
    catalog, counterparts, extension_owned = load_live(url)
    ledger_rows = refreshed_ledger(catalog, counterparts, partition)
    ledger_payload = canonical_tsv(ledger_rows, LEDGER_FIELDS)
    LEDGER.write_bytes(ledger_payload)
    ledger_sha = write_hash(LEDGER_SHA, ledger_payload)
    inventory = build_inventory(catalog, ledger_rows, extension_owned, ledger_sha, partition_sha)
    inventory_payload = canonical(inventory).encode("utf-8")
    inventory_sha = sha256_bytes(inventory_payload)
    checklist = build_drop_checklist(inventory)
    checklist["sourceInventorySha256"] = inventory_sha
    validate_contracts(inventory, checklist, partition_rows, ledger_rows)
    schema_validate(inventory, checklist)
    INVENTORY.write_bytes(inventory_payload)
    write_hash(INVENTORY_SHA, inventory_payload)
    checklist_payload = canonical(checklist).encode("utf-8")
    DROP_CHECKLIST.write_bytes(checklist_payload)
    checklist_sha = write_hash(DROP_CHECKLIST_SHA, checklist_payload)
    return summary(inventory, checklist, inventory_sha, checklist_sha)


def verify() -> dict[str, Any]:
    validate_source()
    partition_rows = read_tsv(PARTITION)
    validate_partition(partition_rows)
    artifacts = (
        (PARTITION, PARTITION_SHA), (LEDGER, LEDGER_SHA),
        (INVENTORY, INVENTORY_SHA), (DROP_CHECKLIST, DROP_CHECKLIST_SHA),
    )
    for path, digest_path in artifacts:
        expected = digest_path.read_text(encoding="utf-8")
        actual = sha256_bytes(path.read_bytes()) + "\n"
        if expected != actual:
            raise ValueError(f"hash drift: {path.relative_to(ROOT)}")
    if PARTITION.read_bytes() != canonical_tsv(
        partition_rows, ("object_key", "partition", "target_schema", "decision_basis"),
    ):
        raise ValueError("partition TSV is not canonical")
    ledger_rows = read_tsv(LEDGER)
    if LEDGER.read_bytes() != canonical_tsv(ledger_rows, LEDGER_FIELDS):
        raise ValueError("ledger TSV is not canonical")
    inventory = json.loads(INVENTORY.read_text(encoding="utf-8"))
    checklist = json.loads(DROP_CHECKLIST.read_text(encoding="utf-8"))
    if INVENTORY.read_bytes() != canonical(inventory).encode("utf-8"):
        raise ValueError("inventory JSON is not canonical")
    if DROP_CHECKLIST.read_bytes() != canonical(checklist).encode("utf-8"):
        raise ValueError("DROP checklist JSON is not canonical")
    if inventory["source"]["partitionSha256"] != PARTITION_SHA.read_text().strip():
        raise ValueError("inventory does not bind the partition hash")
    if inventory["source"]["ledgerSha256"] != LEDGER_SHA.read_text().strip():
        raise ValueError("inventory does not bind the ledger hash")
    if checklist["sourceInventorySha256"] != INVENTORY_SHA.read_text().strip():
        raise ValueError("DROP checklist does not bind the inventory hash")
    validate_contracts(inventory, checklist)
    schema_validate(inventory, checklist)
    return summary(
        inventory, checklist, INVENTORY_SHA.read_text().strip(),
        DROP_CHECKLIST_SHA.read_text().strip(),
    )


def live_check(url: str) -> dict[str, Any]:
    committed = verify()
    partition = validate_partition(read_tsv(PARTITION))
    catalog, counterparts, extension_owned = load_live(url)
    regenerated_ledger = refreshed_ledger(catalog, counterparts, partition)
    regenerated_payload = canonical_tsv(regenerated_ledger, LEDGER_FIELDS)
    if regenerated_payload != LEDGER.read_bytes():
        raise ValueError("live exact-head ledger drift")
    inventory = build_inventory(
        catalog, regenerated_ledger, extension_owned,
        LEDGER_SHA.read_text().strip(), PARTITION_SHA.read_text().strip(),
    )
    payload = canonical(inventory).encode("utf-8")
    if payload != INVENTORY.read_bytes():
        raise ValueError("live exact-head inventory drift")
    committed["liveExactHead"] = True
    return committed


def catalog_fingerprint(url: str) -> str:
    catalog, counterparts, extension_owned = load_live(url)
    # Row estimates/data are absent by design; this is a pure catalog/ACL/dependency proof.
    body = copy.deepcopy(catalog)
    edges = dependencies(body)
    payload = canonical({
        "catalog": body, "dependencies": edges, "counterparts": counterparts,
        "extensionOwned": extension_owned,
    }).encode("utf-8")
    return sha256_bytes(payload)


def summary(
    inventory: dict[str, Any], checklist: dict[str, Any], inventory_sha: str,
    checklist_sha: str,
) -> dict[str, Any]:
    return {
        "schemaVersion": inventory["schemaVersion"],
        "databaseSchemaSha": DATABASE_SOURCE_SHA,
        "migrationHead": MIGRATION_HEAD,
        "counts": inventory["counts"],
        "partitionCounts": inventory["partitionCounts"],
        "dropIdentityCount": checklist["entryCount"],
        "inventorySha256": inventory_sha,
        "dropChecklistSha256": checklist_sha,
        "contractReady": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--refresh", action="store_true")
    action.add_argument("--check", action="store_true")
    action.add_argument("--check-live", action="store_true")
    action.add_argument("--compare-catalogs", action="store_true")
    parser.add_argument("--db-url")
    parser.add_argument("--other-db-url")
    args = parser.parse_args()
    if args.refresh:
        result = refresh(db_url(args.db_url))
    elif args.check:
        result = verify()
    elif args.check_live:
        result = live_check(db_url(args.db_url))
    else:
        if not args.db_url or not args.other_db_url:
            parser.error("--compare-catalogs requires --db-url and --other-db-url")
        first = catalog_fingerprint(db_url(args.db_url))
        second = catalog_fingerprint(db_url(args.other_db_url))
        if first != second:
            raise ValueError(f"catalog fingerprints differ: {first} != {second}")
        result = {"catalogFingerprintSha256": first, "equal": True}
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
