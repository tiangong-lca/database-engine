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
          (select jsonb_agg(jsonb_build_object(
            'num',a.attnum,'name',a.attname,'acl',coalesce(a.attacl::text,''))
            order by a.attnum) from pg_attribute a
            where a.attrelid=c.oid and a.attnum>0 and not a.attisdropped) column_acl,
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
          jsonb_build_object(
            'kind',c.relkind,'owner',c.relowner,'rls',c.relrowsecurity,
            'forceRls',c.relforcerowsecurity,'replicaIdentity',c.relreplident,
            'columns',(select coalesce(jsonb_agg(jsonb_build_object(
              'num',a.attnum,'name',a.attname,
              'type',format_type(a.atttypid,a.atttypmod),'notNull',a.attnotnull,
              'identity',a.attidentity,'generated',a.attgenerated,
              'default',pg_get_expr(d.adbin,d.adrelid),
              'acl',coalesce(a.attacl::text,'')) order by a.attnum),'[]')
              from pg_attribute a left join pg_attrdef d
                on d.adrelid=a.attrelid and d.adnum=a.attnum
              where a.attrelid=c.oid and a.attnum>0 and not a.attisdropped),
            'constraints',(select coalesce(jsonb_agg(jsonb_build_object(
              'name',x.conname,'definition',pg_get_constraintdef(x.oid,true),
              'validated',x.convalidated) order by x.conname),'[]')
              from pg_constraint x where x.conrelid=c.oid),
            'indexes',(select coalesce(jsonb_agg(jsonb_build_object(
              'name',ic.relname,'definition',pg_get_indexdef(i.indexrelid),
              'valid',i.indisvalid,'ready',i.indisready) order by ic.relname),'[]')
              from pg_index i join pg_class ic on ic.oid=i.indexrelid
              where i.indrelid=c.oid),
            'triggers',(select coalesce(jsonb_agg(jsonb_build_object(
              'name',t.tgname,'definition',pg_get_triggerdef(t.oid,true),
              'enabled',t.tgenabled) order by t.tgname),'[]')
              from pg_trigger t where t.tgrelid=c.oid and not t.tgisinternal),
            'publications',(select coalesce(jsonb_agg(p.pubname order by p.pubname),'[]')
              from pg_publication_rel pr join pg_publication p on p.oid=pr.prpubid
              where pr.prrelid=c.oid)
          )::text properties,coalesce(c.relacl::text,'') acl,
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


def wal_insert_lsn(url: str) -> str:
    return psql(url, "select pg_current_wal_insert_lsn();")


def assert_preflight_rejects_without_mutation(
    url: str, *, label: str, tamper: str, repair: str | None = None,
) -> None:
    """Prove one reviewed malicious catalog state fails before rollback DDL."""
    psql(url, tamper)
    before = physical_expand_snapshot(url)
    wal_before = wal_insert_lsn(url)
    apply_rollback(url, fail=True)
    wal_after = wal_insert_lsn(url)
    after = physical_expand_snapshot(url)
    if after != before:
        raise SystemExit(f"{label}: failed rollback preflight mutated catalog or data")
    if wal_after != wal_before:
        raise SystemExit(
            f"{label}: failed rollback preflight emitted WAL: {wal_before} -> {wal_after}"
        )
    if repair:
        psql(url, repair)
    psql(url, MIGRATION.read_text(encoding="utf-8"))


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

    # Every mutable surface bound by the rollback preflight must fail before
    # the first DROP/ALTER and emit no WAL.  Direct repair is needed only for
    # owner and structural tampering; the migration retry converges ACLs and
    # compatibility objects.
    scenarios = (
        {
            "label": "moved canonical owner tamper",
            "tamper": """
              create role issue_356_rollback_owner nologin;
              grant create on schema private to issue_356_rollback_owner;
              grant issue_356_rollback_owner to postgres;
              alter function private.worker_cancel_job(uuid,uuid,text)
                owner to issue_356_rollback_owner;
            """,
            "repair": """
              alter function private.worker_cancel_job(uuid,uuid,text) owner to postgres;
              revoke create on schema private from issue_356_rollback_owner;
              revoke issue_356_rollback_owner from postgres;
              drop role issue_356_rollback_owner;
            """,
        },
        {
            "label": "moved canonical ACL tamper",
            "tamper": "grant execute on function private.worker_cancel_job(uuid,uuid,text) to anon;",
        },
        {
            "label": "physical column ACL tamper",
            "tamper": "grant update(error_message) on private.worker_jobs to service_role;",
        },
        {
            "label": "physical structure tamper",
            "tamper": "alter table private.worker_jobs add column issue_356_rollback_tamper text;",
            "repair": "alter table private.worker_jobs drop column issue_356_rollback_tamper;",
        },
        {
            "label": "compatibility wrapper ACL tamper",
            "tamper": "grant execute on function public.worker_cancel_job(uuid,uuid,text) to anon;",
        },
        {
            "label": "compatibility view column ACL tamper",
            "tamper": "grant update(status) on public.worker_jobs to service_role;",
        },
    )
    for scenario in scenarios:
        assert_preflight_rejects_without_mutation(url, **scenario)
        if physical_expand_snapshot(url) != valid_expand:
            raise SystemExit(
                f"{scenario['label']}: repair did not restore the exact reviewed Expand phase"
            )
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
