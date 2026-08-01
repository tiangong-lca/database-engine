#!/usr/bin/env python3
"""Prove Issue #356 rollback restores the exact pre-Expand catalog/data phase."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import security_definer_audit_v2 as audit
import test_worker_control_plane_data_api as data_api

ROOT = Path(__file__).resolve().parents[1]
BASE = "20260801042600"
MIGRATION = ROOT / "supabase/migrations/20260801060304_issue_356_worker_control_plane_physical_expand.sql"
ROLLBACK = ROOT / "supabase/operator/issue_356_restore_public_worker_control_plane.sql"


def run(command: list[str], *, sql: str | None = None,
        env: dict[str, str] | None = None, fail: bool = False) -> str:
    result = subprocess.run(command, cwd=ROOT, env=env, input=sql, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if (result.returncode == 0) == fail:
        raise SystemExit(
            f"unexpected rollback qualification result {result.returncode}: "
            f"{command[0]}\n{result.stderr}"
        )
    return result.stdout.strip()


def psql(url: str, sql: str) -> str:
    connection = audit.parse_loopback_connection(url)
    return run(
        connection.command("-XAt", "-v", "ON_ERROR_STOP=1"),
        env=connection.environment(), sql=sql,
    )


def snapshot(url: str) -> str:
    return psql(url, r"""
      with rel as (
        select c.oid,c.reltype,n.nspname,c.relname,c.relowner,c.relrowsecurity,
          c.relforcerowsecurity,c.relreplident,coalesce(c.relacl::text,'') acl,
          (select jsonb_agg(pg_get_indexdef(i.indexrelid) order by i.indexrelid)
            from pg_index i where i.indrelid=c.oid) indexes,
          (select jsonb_agg(pg_get_constraintdef(x.oid,true) order by x.oid)
            from pg_constraint x where x.conrelid=c.oid) constraints,
          (select jsonb_agg(pg_get_triggerdef(t.oid,true) order by t.oid)
            from pg_trigger t where t.tgrelid=c.oid and not t.tgisinternal) triggers
        from pg_class c join pg_namespace n on n.oid=c.relnamespace
        where c.relname in ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')
          and n.nspname='public' and c.relkind='r'
      ), routines as (
        select p.oid,p.oid::regprocedure::text signature,p.proowner,p.prolang,p.prorettype,
          p.prokind,p.provolatile,p.proisstrict,p.prosecdef,p.proleakproof,p.proparallel,
          coalesce(p.proconfig::text,'') config,coalesce(p.proacl::text,'') acl,
          pg_get_functiondef(p.oid) definition
        from pg_proc p join pg_namespace n on n.oid=p.pronamespace
        where p.prokind='f' and n.nspname in ('public','api','private','util','archive')
          and pg_get_functiondef(p.oid) ~ 'public\.worker_(jobs|job_events|job_artifacts|job_kinds|job_payload)'
      )
      select jsonb_build_object(
        'relations',(select jsonb_agg(to_jsonb(r) order by nspname,relname) from rel r),
        'pilotViews',(select jsonb_agg(jsonb_build_object(
          'name',c.relname,'owner',c.relowner,'acl',coalesce(c.relacl::text,''),
          'options',c.reloptions,'definition',pg_get_viewdef(c.oid,true)) order by c.relname)
          from pg_class c join pg_namespace n on n.oid=c.relnamespace
          where n.nspname='private' and c.relkind='v' and c.relname in
            ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')),
        'routines',(select jsonb_agg(to_jsonb(r) order by oid) from routines r),
        'rows',(select jsonb_agg(to_jsonb(j) order by id) from public.worker_jobs j),
        'views354',(select jsonb_agg(jsonb_build_object('oid',c.oid,'def',pg_get_viewdef(c.oid,true),'acl',c.relacl)
          order by c.relname) from pg_class c join pg_namespace n on n.oid=c.relnamespace
          where n.nspname='public' and c.relname in
          ('worker_domain_traceability_cutoffs','worker_domain_traceability_violations',
           'worker_job_domain_refs','worker_legacy_lifecycle_audit','worker_legacy_table_retirement_blockers'))
      )::text;
    """)


def physical_expand_snapshot(url: str) -> str:
    return psql(url, r"""
      with functions as materialized (
        select * from pg_proc where prokind='f'
      ), objects as (
        select 'relation' kind,n.nspname||'.'||c.relname object_key,c.oid::text oid,
          c.relkind::text properties,coalesce(c.relacl::text,'') acl,
          case when c.relkind='v' then pg_get_viewdef(c.oid,true) else '' end definition
        from pg_class c join pg_namespace n on n.oid=c.relnamespace
        where n.nspname in ('public','private') and c.relname in
          ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')
        union all
        select 'routine',n.nspname||'.'||p.oid::regprocedure::text,p.oid::text,
          p.prosecdef::text||':'||coalesce(p.proconfig::text,''),
          coalesce(p.proacl::text,''),pg_get_functiondef(p.oid)
        from functions p join pg_namespace n on n.oid=p.pronamespace
        where n.nspname in ('public','private','api','util','archive') and (
          p.proname like 'worker_%' or pg_get_functiondef(p.oid) ~
            '(public|private)\.worker_(jobs|job_events|job_artifacts|job_kinds|job_payload)'
        )
      )
      select md5(
        (select string_agg(kind||':'||object_key||':'||oid||':'||properties||':'||acl||':'||
          md5(definition),E'\n' order by kind,object_key,oid) from objects)||':'||
        (select count(*)::text from private.worker_jobs)||':'||
        coalesce((select md5(string_agg(id::text||':'||status||':'||payload_json::text,
          E'\n' order by id)) from private.worker_jobs),'')
      );
    """)


def apply_rollback(url: str, *, fail: bool = False) -> None:
    connection = audit.parse_loopback_connection(url)
    run(
        connection.command("-X", "-v", "ON_ERROR_STOP=1", "-f", str(ROLLBACK)),
        env=connection.environment(), fail=fail,
    )


def main() -> int:
    if "DATABASE_URL" not in os.environ:
        command=["supabase","db","reset","--version",BASE]
        if workdir:=os.environ.get("SUPABASE_WORKDIR"):
            command.extend(["--workdir",workdir])
        run(command)
        status=["supabase","status","--output","json"]
        if workdir:=os.environ.get("SUPABASE_WORKDIR"):
            status.extend(["--workdir",workdir])
        url=json.loads(run(status))["DB_URL"]
    else:
        url=os.environ["DATABASE_URL"]
    psql(url,"""
      insert into public.worker_jobs(id,job_kind,worker_runtime,worker_queue,requester_type,
        status,payload_schema_version,payload_json)
      values('35600000-0000-4000-8000-000000000001','lca.snapshot_gc','calculator',
        'maintenance','service','queued','lca.snapshot_gc.request.v1','{"rollback":true}');
    """)
    before=json.loads(snapshot(url))
    data_api.qualify_phase("r", label="rollback baseline")
    psql(url,MIGRATION.read_text(encoding="utf-8"))
    valid_expand = physical_expand_snapshot(url)
    data_api.qualify_phase("v", label="rollback initial Expand")

    # Any definition drift must fail before the first DROP and leave the
    # deliberately tampered Expand phase byte-for-byte unchanged.
    psql(url, "alter function public.worker_cancel_job(uuid,uuid,text) set search_path=public;")
    tampered_expand = physical_expand_snapshot(url)
    if tampered_expand == valid_expand:
        raise SystemExit("rollback malicious preflight fixture did not change the post-state")
    apply_rollback(url, fail=True)
    if physical_expand_snapshot(url) != tampered_expand:
        raise SystemExit("failed rollback preflight mutated the physical Expand phase")

    # The migration retry is the only reviewed repair for a drifted adapter.
    psql(url,MIGRATION.read_text(encoding="utf-8"))
    if physical_expand_snapshot(url) != valid_expand:
        raise SystemExit("migration retry did not restore the exact reviewed Expand phase")
    apply_rollback(url)
    after=json.loads(snapshot(url))
    if after != before:
        changed=[key for key in before if before[key] != after.get(key)]
        raise SystemExit(f"rollback snapshot differs in: {changed}")
    data_api.qualify_phase("r", label="rollback restored baseline")
    psql(url,MIGRATION.read_text(encoding="utf-8"))
    if psql(url,"select to_regclass('private.worker_jobs') is not null and to_regclass('public.worker_jobs') is not null;") != "t":
        raise SystemExit("rollback roll-forward did not restore Expand")
    data_api.qualify_phase("v", label="rollback roll-forward Expand")
    print("PASS Issue #356 exact rollback catalog/data snapshot and roll-forward")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
