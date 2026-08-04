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
        'rls',c.relrowsecurity,'forceRls',c.relforcerowsecurity,'acl',coalesce(c.relacl::text,'null')) x
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
