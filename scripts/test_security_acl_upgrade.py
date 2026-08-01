#!/usr/bin/env python3
"""Populated security-ACL upgrade, failure, retry, parity, and rollback proof."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = "20260731164051"
MIGRATION = ROOT / "supabase/migrations/20260801022717_issue_339_security_acl_expand.sql"
ROLLBACK = ROOT / "supabase/operator/issue_339_restore_expand_acl.sql"


def run(command: list[str], *, sql: str | None = None, expect_failure: bool = False) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, cwd=ROOT, input=sql, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if expect_failure == (result.returncode == 0):
        raise SystemExit(f"unexpected command result ({result.returncode}): {' '.join(command)}\n{result.stdout}\n{result.stderr}")
    return result


def psql(db_url: str, sql: str, *, expect_failure: bool = False) -> subprocess.CompletedProcess[str]:
    return run(["psql", db_url, "-XAt", "-v", "ON_ERROR_STOP=1"], sql=sql, expect_failure=expect_failure)


def reset_to_base() -> None:
    command = ["supabase", "db", "reset", "--local", "--version", BASE]
    transient_markers = ("context deadline exceeded", "Error status 502")
    for attempt in range(3):
        result = subprocess.run(command, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode == 0:
            return
        combined = result.stdout + result.stderr
        if attempt < 2 and any(marker in combined for marker in transient_markers):
            continue
        raise SystemExit(f"base reset failed ({result.returncode}):\n{combined}")


def target_acl_signature(db_url: str) -> str:
    query = r"""
      with payload as (
        select jsonb_build_object(
          'schemas', (select coalesce(jsonb_agg(to_jsonb(x) order by schema_name,grantee,privilege_type),'[]') from (
            select n.nspname schema_name,case when a.grantee=0 then 'PUBLIC' else r.rolname end grantee,a.privilege_type
            from pg_namespace n cross join lateral aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a
            left join pg_roles r on r.oid=a.grantee
            where n.nspname in ('private','util','archive') and (a.grantee=0 or r.rolname in ('anon','authenticated'))
          ) x),
          'relations', (select coalesce(jsonb_agg(to_jsonb(x) order by schema_name,relation_name,grantee,privilege_type),'[]') from (
            select n.nspname schema_name,c.relname relation_name,case when a.grantee=0 then 'PUBLIC' else r.rolname end grantee,a.privilege_type
            from pg_class c join pg_namespace n on n.oid=c.relnamespace
            cross join lateral aclexplode(coalesce(c.relacl,acldefault(case when c.relkind='S' then 'S'::"char" else 'r'::"char" end,c.relowner))) a
            left join pg_roles r on r.oid=a.grantee
            where c.relkind in ('r','p','v','m','S') and n.nspname in ('public','private','util','archive')
              and c.relname not in ('security_acl_expand_20260801_snapshot','security_acl_expand_posture')
              and (a.grantee=0 or r.rolname in ('anon','authenticated'))
          ) x),
          'functions', (select coalesce(jsonb_agg(to_jsonb(x) order by function_name,grantee,privilege_type),'[]') from (
            select p.oid::regprocedure::text function_name,case when a.grantee=0 then 'PUBLIC' else r.rolname end grantee,a.privilege_type
            from pg_proc p join pg_namespace n on n.oid=p.pronamespace
            cross join lateral aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
            left join pg_roles r on r.oid=a.grantee
            where p.prokind='f' and (n.nspname in ('private','util','archive')
              or (n.nspname='public' and p.proname in ('save_lifecycle_model_bundle','delete_lifecycle_model_bundle')))
              and (a.grantee=0 or r.rolname in ('anon','authenticated','service_role'))
          ) x),
          'facades', (select coalesce(jsonb_agg(to_jsonb(x) order by function_name),'[]') from (
            select p.oid::regprocedure::text function_name,owner_role.rolname owner,p.prosecdef security_definer
            from pg_proc p join pg_namespace n on n.oid=p.pronamespace
            join pg_roles owner_role on owner_role.oid=p.proowner
            where n.nspname='public' and p.prokind='f' and pg_get_functiondef(p.oid) like '%private.%'
              and not p.prosecdef
          ) x),
          'defaults', (select coalesce(jsonb_agg(to_jsonb(x) order by owner,schema_name,object_type,grantee,privilege_type),'[]') from (
            select owner_role.rolname owner,n.nspname schema_name,d.defaclobjtype object_type,
              case when a.grantee=0 then 'PUBLIC' else r.rolname end grantee,a.privilege_type
            from pg_default_acl d join pg_namespace n on n.oid=d.defaclnamespace
            join pg_roles owner_role on owner_role.oid=d.defaclrole
            cross join lateral aclexplode(d.defaclacl) a left join pg_roles r on r.oid=a.grantee
            where owner_role.rolname='postgres' and n.nspname in ('public','private','util','archive')
              and (a.grantee=0 or r.rolname in ('anon','authenticated','service_role'))
          ) x)
        ) value
      ) select md5(value::text) from payload;
    """
    return psql(db_url, query).stdout.strip()


def main() -> int:
    reset_to_base()
    db_url = json.loads(run(["supabase", "status", "--output", "json"]).stdout)["DB_URL"]
    psql(db_url, """
      insert into public.worker_jobs(
        id,job_kind,worker_runtime,worker_queue,requester_type,status,
        payload_schema_version,payload_json
      ) values (
        '33900000-0000-4000-8000-000000000001','lca.snapshot_gc','calculator',
        'maintenance','service','queued','lca.snapshot_gc.request.v1',
        '{"issue":339,"populated":true}'::jsonb
      );
    """)
    before_signature = target_acl_signature(db_url)
    migration = MIGRATION.read_text(encoding="utf-8")
    for required in ("set local lock_timeout = '5s'", "set local statement_timeout = '2min'"):
        if required not in migration.lower():
            raise SystemExit(f"migration lock budget missing: {required}")

    failed = migration.rsplit("commit;", 1)[0] + "select 1/0;\ncommit;\n"
    psql(db_url, failed, expect_failure=True)
    if target_acl_signature(db_url) != before_signature:
        raise SystemExit("failed migration changed target ACLs")
    residue = psql(db_url, "select to_regclass('archive.security_acl_expand_20260801_snapshot'), to_regclass('util.security_acl_expand_posture');").stdout.strip()
    if residue != "|":
        raise SystemExit(f"failed migration left schema residue: {residue}")

    psql(db_url, migration)
    psql(db_url, migration)
    populated = psql(db_url, "select count(*) from public.worker_jobs where id='33900000-0000-4000-8000-000000000001' and payload_json @> '{\"populated\":true}';").stdout.strip()
    if populated != "1":
        raise SystemExit("populated business row changed during ACL migration")
    run(["supabase", "test", "db", "supabase/tests/20260801_security_acl_expand.sql", "--local"])

    run(["psql", db_url, "-X", "-v", "ON_ERROR_STOP=1", "-f", str(ROLLBACK)])
    if target_acl_signature(db_url) != before_signature:
        raise SystemExit("catalog-driven compatibility rollback did not restore the pre-Expand ACL signature")
    if psql(db_url, "select count(*) from public.worker_jobs where id='33900000-0000-4000-8000-000000000001';").stdout.strip() != "1":
        raise SystemExit("rollback changed populated business data")

    psql(db_url, migration)
    run(["supabase", "test", "db", "supabase/tests/20260801_security_acl_expand.sql", "--local"])
    print(f"PASS populated {BASE}->20260801022717; failure atomic; retry stable; data parity; catalog rollback; roll-forward stable")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
