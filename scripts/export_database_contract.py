#!/usr/bin/env python3
"""Export deterministic catalog/ACL/RLS/default-privilege metadata and hash."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "supabase/tests/contracts/database_catalog.json"
SHA = ROOT / "supabase/tests/contracts/database_catalog.sha256"
QUERY = r"""
with contract as (
  select jsonb_build_object(
    'schemas', (select coalesce(jsonb_agg(to_jsonb(x) order by name), '[]') from (
      select n.nspname name, owner.rolname owner, coalesce(n.nspacl::text,'null') acl
      from pg_namespace n join pg_roles owner on owner.oid=n.nspowner
      where n.nspname in ('public','api','private','util','archive')
    ) x),
    'relations', (select coalesce(jsonb_agg(x order by x->>'schema', x->>'name'), '[]') from (
      select jsonb_build_object('schema',n.nspname,'name',c.relname,'kind',c.relkind,
        'rls',c.relrowsecurity,'forceRls',c.relforcerowsecurity,'acl',coalesce(c.relacl::text,'null'),
        'columnAcl',coalesce((select jsonb_agg(jsonb_build_object(
          'column',attribute.attname,
          'grantee',case when acl.grantee=0 then 'PUBLIC' else grantee.rolname end,
          'privilege',acl.privilege_type,'grantable',acl.is_grantable,
          'grantor',case when acl.grantor=0 then 'PUBLIC' else grantor.rolname end)
          order by attribute.attname,
            case when acl.grantee=0 then 'PUBLIC' else grantee.rolname end,
            acl.privilege_type,acl.is_grantable,
            case when acl.grantor=0 then 'PUBLIC' else grantor.rolname end)
          from pg_attribute attribute
          cross join lateral aclexplode(attribute.attacl) acl
          left join pg_roles grantee on grantee.oid=acl.grantee
          left join pg_roles grantor on grantor.oid=acl.grantor
          where attribute.attrelid=c.oid and attribute.attnum>0
            and not attribute.attisdropped),'[]'::jsonb)) x
      from pg_class c join pg_namespace n on n.oid=c.relnamespace
      where n.nspname in ('public','api','private','util','archive') and c.relkind in ('r','p','v','m','S')
    ) q),
    'indexes', (select coalesce(jsonb_agg(to_jsonb(x) order by schemaname,tablename,indexname), '[]') from (
      select schemaname,tablename,indexname,indexdef
      from pg_indexes
      where schemaname in ('public','api','private','util','archive')
    ) x),
    'policies', (select coalesce(jsonb_agg(to_jsonb(x) order by schemaname,tablename,policyname), '[]') from (
      select schemaname,tablename,policyname,permissive,roles,cmd,qual,with_check from pg_policies
      where schemaname in ('public','api','private','util','archive')
    ) x),
    'functions', (select coalesce(jsonb_agg(x order by x->>'schema',x->>'name',x->>'identityArguments'), '[]') from (
      select jsonb_build_object('schema',n.nspname,'name',p.proname,
        'identityArguments',pg_get_function_identity_arguments(p.oid),
        'result',pg_get_function_result(p.oid),'securityDefiner',p.prosecdef,
        'config',coalesce(to_jsonb(p.proconfig),'[]'::jsonb),'acl',coalesce(p.proacl::text,'null')) x
      from pg_proc p join pg_namespace n on n.oid=p.pronamespace
      where p.prokind='f' and n.nspname in ('public','api','private','util','archive')
    ) q),
    'defaultPrivileges', (select coalesce(jsonb_agg(to_jsonb(x) order by owner,schema,object_type), '[]') from (
      select d.defaclrole::regrole::text owner,coalesce(n.nspname,'*') schema,
        d.defaclobjtype object_type,coalesce(d.defaclacl::text,'null') acl
      from pg_default_acl d left join pg_namespace n on n.oid=d.defaclnamespace
      where n.nspname in ('public','api','private','util','archive')
    ) x)
  ) payload
)
select payload::text from contract;
"""

GENERATION_GUARD_QUERY = r"""
with forbidden_maintain as (
  select n.nspname schema_name, c.relname relation_name
  from pg_class c
  join pg_namespace n on n.oid=c.relnamespace
  cross join lateral aclexplode(coalesce(c.relacl, acldefault(
    case when c.relkind='S' then 'S'::"char" else 'r'::"char" end, c.relowner
  ))) a
  join pg_roles grantee on grantee.oid=a.grantee
  where n.nspname in ('public','api','private','util','archive')
    and c.relkind in ('r','p','v','m','S')
    and grantee.rolname='service_role' and a.privilege_type='MAINTAIN'
    and c.relname in (
      'comments','contacts','flowproperties','flows','ilcd','lciamethods','lifecyclemodels',
      'processes','reviews','roles','sources','teams','unitgroups','users',
      'worker_domain_traceability_cutoffs','worker_domain_traceability_violations',
      'worker_job_domain_refs','worker_legacy_lifecycle_audit',
      'worker_legacy_table_retirement_blockers'
    )
), forbidden_internal_execute as (
  select n.nspname schema_name, p.proname function_name,
    case when a.grantee=0 then 'PUBLIC' else grantee.rolname end grantee
  from pg_proc p
  join pg_namespace n on n.oid=p.pronamespace
  cross join lateral aclexplode(coalesce(p.proacl, acldefault('f',p.proowner))) a
  left join pg_roles grantee on grantee.oid=a.grantee
  where p.prokind='f'
    and a.privilege_type='EXECUTE'
    and (
      (n.nspname in ('private','util','archive')
       and (a.grantee=0 or grantee.rolname in ('anon','authenticated')))
      or
      (n.nspname='public'
       and p.proname in (
         'ilcd_classification_get','ilcd_flow_categorization_get',
         'ilcd_location_get','policy_is_current_user_in_roles'
       )
       and grantee.rolname='anon')
    )
), forbidden_lifecycle_execute as (
  select role_name, p.proname function_name
  from pg_proc p
  join pg_namespace n on n.oid=p.pronamespace
  cross join (values ('anon'),('authenticated')) roles(role_name)
  where n.nspname='public'
    and p.proname in ('save_lifecycle_model_bundle','delete_lifecycle_model_bundle')
    and has_function_privilege(role_name,p.oid,'EXECUTE')
)
select jsonb_build_object(
  'serviceRoleMaintain', coalesce((select jsonb_agg(to_jsonb(x)) from forbidden_maintain x),'[]'),
  'forbiddenInternalExecute', coalesce((select jsonb_agg(to_jsonb(x)) from forbidden_internal_execute x),'[]'),
  'forbiddenLifecycleExecute', coalesce((select jsonb_agg(to_jsonb(x)) from forbidden_lifecycle_execute x),'[]')
)::text;
"""


def validate_generation_guard(guard: dict[str, object]) -> None:
    expected = {"serviceRoleMaintain", "forbiddenInternalExecute", "forbiddenLifecycleExecute"}
    if set(guard) != expected:
        raise SystemExit("database catalog generation guard returned an unexpected shape")
    polluted = [name for name in sorted(expected) if guard[name] != []]
    if polluted:
        raise SystemExit("database catalog generation refused polluted ACL state: " + ",".join(polluted))


def export() -> tuple[str, str]:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        status = subprocess.run(
            ["supabase", "status", "--output", "json"], cwd=ROOT, check=True,
            text=True, stdout=subprocess.PIPE,
        )
        db_url = json.loads(status.stdout)["DB_URL"]
    result = subprocess.run(
        ["psql", db_url, "-XAt", "-v", "ON_ERROR_STOP=1", "-c", QUERY],
        cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE,
    )
    guard_result = subprocess.run(
        ["psql", db_url, "-XAt", "-v", "ON_ERROR_STOP=1", "-c", GENERATION_GUARD_QUERY],
        cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE,
    )
    validate_generation_guard(json.loads(guard_result.stdout))
    payload = json.dumps(json.loads(result.stdout), ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    return payload, hashlib.sha256(payload.encode()).hexdigest() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.write == args.check:
        parser.error("choose exactly one of --write or --check")
    payload, digest = export()
    if args.write:
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(payload, encoding="utf-8")
        SHA.write_text(digest, encoding="utf-8")
        print(digest.strip())
        return 0
    if OUT.read_text(encoding="utf-8") != payload or SHA.read_text(encoding="utf-8") != digest:
        raise SystemExit("database catalog contract drift detected; review and run --write")
    print(digest.strip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
