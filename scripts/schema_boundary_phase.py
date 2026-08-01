#!/usr/bin/env python3
"""Fail-closed Expand/Contract schema-boundary checker for Issue #354."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "supabase/tests/contracts/schema_boundary_phase.v1.json"
INVENTORY = ROOT / "supabase/tests/contracts/public_object_inventory.json"
ROLES = ("anon", "authenticated", "service_role")

SNAPSHOT_SQL = r"""
with schemas as (
  select n.nspname as name, owner.rolname as owner,
    jsonb_build_object(
      'anonUsage', has_schema_privilege('anon', n.oid, 'USAGE'),
      'anonCreate', has_schema_privilege('anon', n.oid, 'CREATE'),
      'authenticatedUsage', has_schema_privilege('authenticated', n.oid, 'USAGE'),
      'authenticatedCreate', has_schema_privilege('authenticated', n.oid, 'CREATE'),
      'service_roleUsage', has_schema_privilege('service_role', n.oid, 'USAGE'),
      'service_roleCreate', has_schema_privilege('service_role', n.oid, 'CREATE')
    ) as privileges
  from pg_namespace n join pg_roles owner on owner.oid = n.nspowner
  where n.nspname in ('api','private','util','archive')
), relations as (
  select n.nspname as schema, c.relname as name, c.relkind as kind,
    owner.rolname as owner,
    coalesce(c.reloptions @> array['security_invoker=true'], false) as security_invoker,
    has_table_privilege('anon', c.oid, 'SELECT') as anon_select,
    has_table_privilege('authenticated', c.oid, 'SELECT') as authenticated_select,
    has_table_privilege('service_role', c.oid, 'SELECT') as service_role_select,
    has_table_privilege('api_internal_executor', c.oid, 'SELECT') as executor_select,
    obj_description(c.oid, 'pg_class') as comment,
    (select jsonb_agg(a.attname order by a.attnum)
       from pg_attribute a where a.attrelid=c.oid and a.attnum>0 and not a.attisdropped) as columns
  from pg_class c
  join pg_namespace n on n.oid=c.relnamespace
  join pg_roles owner on owner.oid=c.relowner
  where n.nspname in ('public','api','private','util','archive')
    and c.relkind in ('r','p','v','m')
), routines as (
  select n.nspname as schema, p.proname as name, p.prokind as kind,
    pg_get_function_identity_arguments(p.oid) as identity_arguments
  from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='public' and p.prokind in ('f','p')
)
select jsonb_build_object(
  'schemas', (select coalesce(jsonb_agg(to_jsonb(x) order by name), '[]') from schemas x),
  'relations', (select coalesce(jsonb_agg(to_jsonb(x) order by schema,name), '[]') from relations x),
  'publicRoutines', (select coalesce(jsonb_agg(to_jsonb(x) order by name,identity_arguments), '[]') from routines x),
  'posture', (select posture from util.schema_boundary_phase)
)::text;
"""


def load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_snapshot(
    snapshot: dict[str, object], contract: dict[str, object], inventory: dict[str, object]
) -> list[str]:
    errors: list[str] = []
    phase = contract.get("phase")
    if phase not in {"expand", "contract"}:
        return [f"unsupported phase: {phase}"]

    schemas = {row["name"]: row for row in snapshot["schemas"]}
    for name, expected in contract["applicationSchemas"].items():
        actual = schemas.get(name)
        if actual is None:
            errors.append(f"missing application schema: {name}")
            continue
        if actual["owner"] != expected["owner"]:
            errors.append(f"schema owner drift: {name}={actual['owner']}")
        usage_roles = set(expected["usageRoles"])
        for role in ROLES:
            if actual["privileges"][f"{role}Create"]:
                errors.append(f"forbidden schema CREATE: {name}:{role}")
            if actual["privileges"][f"{role}Usage"] != (role in usage_roles):
                errors.append(f"schema USAGE drift: {name}:{role}")

    relations = {(row["schema"], row["name"]): row for row in snapshot["relations"]}
    core = set(contract["corePublicTables"])
    public_tables = {
        row["name"] for row in snapshot["relations"]
        if row["schema"] == "public" and row["kind"] in {"r", "p"}
    }
    missing_core = sorted(core - public_tables)
    if missing_core:
        errors.append(f"missing core public tables: {missing_core}")

    for mapping in contract["movedViews"]:
        name = mapping["name"]
        target = relations.get((mapping["targetSchema"], name))
        compatibility = relations.get((contract["expand"]["compatibilitySchema"], name))
        for label, relation in (("target", target), ("compatibility", compatibility)):
            if relation is None:
                errors.append(f"missing {label} view: {mapping['targetSchema'] if label == 'target' else 'public'}.{name}")
                continue
            if relation["kind"] != "v" or relation["owner"] != "postgres" or not relation["security_invoker"]:
                errors.append(f"{label} view owner/kind/security drift: {name}")
            if not relation["service_role_select"] or relation["anon_select"] or relation["authenticated_select"] or relation["executor_select"]:
                errors.append(f"{label} view ACL drift: {name}")
        if target is not None and compatibility is not None and target["columns"] != compatibility["columns"]:
            errors.append(f"compatibility columns drift: {name}")

    posture = snapshot["posture"]
    if posture.get("contractVersion") != contract["schemaVersion"] or posture.get("phase") != phase:
        errors.append("database phase posture does not match the checked contract")

    if phase == "expand":
        if not posture.get("expandReady") or posture.get("contractReady"):
            errors.append("Expand posture is not ready or incorrectly claims Contract readiness")
        target_by_table = {
            row["objectName"]: row["targetSchema"]
            for row in inventory["objects"] if row["objectType"] == "table"
        }
        unplanned = sorted(
            name for name in public_tables - core
            if target_by_table.get(name) in {None, "public"}
        )
        if unplanned:
            errors.append(f"non-core public tables lack a non-public target: {unplanned}")
    else:
        expected_tables = set(contract["contract"]["allowedPublicTables"])
        if public_tables != expected_tables:
            errors.append(
                f"Contract public table set drift: missing={sorted(expected_tables-public_tables)}, extra={sorted(public_tables-expected_tables)}"
            )
        public_views = sorted(
            row["name"] for row in snapshot["relations"]
            if row["schema"] == "public" and row["kind"] == "v"
        )
        public_materialized = sorted(
            row["name"] for row in snapshot["relations"]
            if row["schema"] == "public" and row["kind"] == "m"
        )
        routines = sorted(
            f"{row['name']}({row['identity_arguments']})" for row in snapshot["publicRoutines"]
        )
        if public_views != contract["contract"]["allowedPublicViews"]:
            errors.append(f"Contract public view residue: {public_views}")
        if public_materialized != contract["contract"]["allowedPublicMaterializedViews"]:
            errors.append(f"Contract public materialized-view residue: {public_materialized}")
        if routines != contract["contract"]["allowedPublicRoutines"]:
            errors.append(f"Contract public routine residue: {routines}")
        if not posture.get("contractReady"):
            errors.append("Contract phase does not claim readiness")
    return errors


def database_url() -> str:
    if value := os.environ.get("DATABASE_URL"):
        return value
    status = subprocess.run(
        ["supabase", "status", "--output", "json"], cwd=ROOT, check=True,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    return json.loads(status.stdout)["DB_URL"]


def read_snapshot() -> dict[str, object]:
    result = subprocess.run(
        ["psql", database_url(), "-qXAt", "-v", "ON_ERROR_STOP=1"],
        cwd=ROOT, input=SNAPSHOT_SQL, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if result.returncode:
        raise SystemExit("schema-boundary catalog readback failed")
    return json.loads(result.stdout)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, default=CONTRACT)
    parser.add_argument("--inventory", type=Path, default=INVENTORY)
    args = parser.parse_args()
    contract = load_json(args.contract)
    errors = validate_snapshot(read_snapshot(), contract, load_json(args.inventory))
    if errors:
        raise SystemExit("schema-boundary phase check failed:\n- " + "\n- ".join(errors))
    print(f"PASS schema-boundary phase={contract['phase']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
