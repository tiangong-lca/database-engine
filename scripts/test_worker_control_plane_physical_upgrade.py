#!/usr/bin/env python3
"""Issue #356 populated/scale/lock/WAL/failure/retry qualification."""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

import security_definer_audit_v2 as audit
import test_worker_control_plane_data_api as data_api

ROOT = Path(__file__).resolve().parents[1]
BASE = "20260801042600"
MIGRATION = ROOT / "supabase/migrations/20260801060304_issue_356_worker_control_plane_physical_expand.sql"
TARGETS = "('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts')"


def run(
    command: list[str], *, sql: str | None = None, fail: bool = False,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, cwd=ROOT, env=env, input=sql, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if (result.returncode == 0) == fail:
        raise SystemExit(
            f"unexpected command result {result.returncode}: {' '.join(command)}\n"
            f"{result.stdout}\n{result.stderr}"
        )
    return result


def psql(url: str, sql: str, *, fail: bool = False) -> str:
    connection = audit.parse_loopback_connection(url)
    return run(
        connection.command("-XAt", "-v", "ON_ERROR_STOP=1"),
        env=connection.environment(), sql=sql, fail=fail,
    ).stdout.strip()


def supabase(*args: str) -> subprocess.CompletedProcess[str]:
    command = ["supabase", *args]
    if workdir := os.environ.get("SUPABASE_WORKDIR"):
        command.extend(["--workdir", workdir])
    return run(command)


def database_url() -> str:
    if value := os.environ.get("DATABASE_URL"):
        return value
    return json.loads(supabase("status", "--output", "json").stdout)["DB_URL"]


def snapshot(url: str, schema: str) -> dict[str, object]:
    raw = psql(url, f"""
      with rels as (
        select c.oid, c.reltype, c.relname, c.relowner, c.relrowsecurity,
          c.relforcerowsecurity, c.relreplident, coalesce(c.relacl::text,'') acl,
          (select count(*) from pg_index i where i.indrelid=c.oid) indexes,
          (select count(*) from pg_constraint x where x.conrelid=c.oid) constraints,
          (select count(*) from pg_trigger t where t.tgrelid=c.oid and not t.tgisinternal) triggers
        from pg_class c join pg_namespace n on n.oid=c.relnamespace
        where n.nspname='{schema}' and c.relkind='r' and c.relname in {TARGETS}
      ), routines as (
        select p.oid, p.oid::regprocedure::text signature, p.proowner, p.prolang,
          p.prorettype, p.prokind, p.provolatile, p.proisstrict, p.prosecdef,
          p.proleakproof, p.proparallel, coalesce(p.proconfig::text,'') config,
          coalesce(p.proacl::text,'') acl, pg_get_functiondef(p.oid) definition
        from pg_proc p join pg_namespace n on n.oid=p.pronamespace
        where p.prokind='f' and n.nspname in ('public','api','private','util','archive')
          and pg_get_functiondef(p.oid) ~ '(public|private)\\.worker_(jobs|job_events|job_artifacts|job_kinds|job_payload)'
      )
      select jsonb_build_object(
        'relations',(select jsonb_agg(to_jsonb(r) order by relname) from rels r),
        'routines',(select jsonb_agg(to_jsonb(r) order by oid) from routines r),
        'rows',jsonb_build_object(
          'kinds',(select count(*) from {schema}.worker_job_kinds),
          'jobs',(select count(*) from {schema}.worker_jobs),
          'events',(select count(*) from {schema}.worker_job_events),
          'artifacts',(select count(*) from {schema}.worker_job_artifacts),
          'checksum',(select md5(string_agg(id::text||':'||status||':'||payload_json::text,E'\\n' order by id))
            from {schema}.worker_jobs)
        ),
        'views354',(select md5(string_agg(c.relname||':'||md5(pg_get_viewdef(c.oid,true))||':'||
          coalesce(c.relacl::text,''),E'\\n' order by c.relname)) from pg_class c
          join pg_namespace n on n.oid=c.relnamespace where n.nspname='public' and c.relname in
          ('worker_domain_traceability_cutoffs','worker_domain_traceability_violations',
           'worker_job_domain_refs','worker_legacy_lifecycle_audit','worker_legacy_table_retirement_blockers'))
      )::text;
    """)
    return json.loads(raw)


def normalized_after(before: dict[str, object], after: dict[str, object]) -> None:
    if before["rows"] != after["rows"]:
        raise SystemExit("row count/checksum drifted across physical move")
    if before["views354"] != after["views354"]:
        raise SystemExit("#354 five-view definition/ACL hash drifted")
    before_rel = {row["relname"]: row for row in before["relations"]}
    after_rel = {row["relname"]: row for row in after["relations"]}
    for name, old in before_rel.items():
        new = after_rel.get(name)
        if new is None:
            raise SystemExit(f"physical relation disappeared: {name}")
        for field in (
            "oid", "reltype", "relname", "relowner", "relrowsecurity",
            "relforcerowsecurity", "relreplident", "indexes", "constraints", "triggers",
        ):
            if old[field] != new[field]:
                raise SystemExit(f"physical relation property drift {name}.{field}")

    after_by_oid = {row["oid"]: row for row in after["routines"]}
    compared = 0
    for old in before["routines"]:
        new = after_by_oid.get(old["oid"])
        if new is None:
            raise SystemExit(f"routine OID disappeared: {old['signature']}")
        expected_definition = old["definition"]
        allowlisted_public_invokers = {
            "cmd_dataset_review_submit_gate_payload",
            "cmd_dataset_review_submit_job_payload",
            "lcia_result_package_bind_closure_certificate",
            "worker_job_payload",
        }
        if str(old["signature"]).split("(", 1)[0] not in allowlisted_public_invokers:
            for name in ("worker_job_artifacts", "worker_job_events", "worker_job_kinds",
                         "worker_job_payload", "worker_jobs"):
                expected_definition = expected_definition.replace(f"public.{name}", f"private.{name}")
        for field in ("proowner","prolang","prorettype","prokind","provolatile",
                      "proisstrict","prosecdef","proleakproof","proparallel","acl"):
            if old[field] != new[field]:
                raise SystemExit(f"routine property drift {field}: {old['signature']}")
        # The twelve moved routines intentionally receive a hardened search_path.
        if not str(new["signature"]).startswith("private.worker_") and old["config"] != new["config"]:
            raise SystemExit(f"non-target routine config drift: {old['signature']}")
        if new["definition"] != expected_definition and not str(new["signature"]).startswith("private.worker_"):
            raise SystemExit(f"routine rewrite differs from exact replacement: {old['signature']}")
        compared += 1
    if compared < 62:
        raise SystemExit(f"routine closure unexpectedly small: {compared}")


def verify_minimum_acl(url: str) -> None:
    matrix = psql(url, """
      select
        has_table_privilege('service_role','private.worker_jobs','SELECT') and
        has_column_privilege('service_role','private.worker_jobs','phase','UPDATE') and
        has_column_privilege('service_role','private.worker_jobs','progress','UPDATE') and
        has_column_privilege('service_role','private.worker_jobs','diagnostics','UPDATE') and
        has_column_privilege('service_role','private.worker_jobs','heartbeat_at','UPDATE') and
        has_column_privilege('service_role','private.worker_jobs','lease_expires_at','UPDATE') and
        has_column_privilege('service_role','private.worker_jobs','updated_at','UPDATE') and
        not has_column_privilege('service_role','private.worker_jobs','status','UPDATE') and
        not has_table_privilege('service_role','private.worker_jobs','INSERT') and
        not has_table_privilege('service_role','private.worker_jobs','DELETE') and
        has_table_privilege('service_role','private.worker_job_artifacts','SELECT') and
        has_column_privilege('service_role','private.worker_job_artifacts','job_id','INSERT') and
        has_column_privilege('service_role','private.worker_job_artifacts','artifact_type','INSERT') and
        has_column_privilege('service_role','private.worker_job_artifacts','content_type','INSERT') and
        has_column_privilege('service_role','private.worker_job_artifacts','metadata','INSERT') and
        has_column_privilege('service_role','private.worker_job_artifacts','visibility','INSERT') and
        not has_column_privilege('service_role','private.worker_job_artifacts','storage_path','INSERT') and
        not has_table_privilege('service_role','private.worker_job_events','SELECT') and
        not has_table_privilege('service_role','private.worker_job_kinds','SELECT');
    """)
    if matrix != "t":
        raise SystemExit("minimum service role Worker relation ACL drifted")


def main() -> int:
    if "DATABASE_URL" not in os.environ:
        supabase("db", "reset", "--version", BASE)
    url = database_url()
    rows = int(os.environ.get("ISSUE_356_SCALE_ROWS", "1000000"))
    migration = MIGRATION.read_text(encoding="utf-8")
    for budget in ("set local lock_timeout = '5s'", "set local statement_timeout = '2min'"):
        if budget not in migration.lower():
            raise SystemExit(f"migration budget missing: {budget}")

    psql(url, f"""
      insert into public.worker_jobs(
        id,job_kind,worker_runtime,worker_queue,requester_type,status,
        payload_schema_version,payload_json,created_at,updated_at
      )
      select ('10000000-0000-4000-8000-'||lpad(g::text,12,'0'))::uuid,
        'lca.snapshot_gc','calculator','maintenance','service','completed',
        'lca.snapshot_gc.request.v1',jsonb_build_object('scale',g),
        '2026-01-01'::timestamptz + g*interval '1 millisecond',
        '2026-01-01'::timestamptz + g*interval '1 millisecond'
      from generate_series(1,{rows}) g;
      insert into public.worker_job_events(id,job_id,event_type,status,details)
      select '20000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001','completed','completed','{{"upgrade":true}}';
      insert into public.worker_job_artifacts(id,job_id,artifact_type,content_type,metadata,visibility)
      select '30000000-0000-4000-8000-000000000001',
        '10000000-0000-4000-8000-000000000001','upgrade-proof','application/json','{{}}','operator';
    """)
    before = snapshot(url, "public")
    data_api.qualify_phase("r", label="populated baseline")

    # The deliberately failed transaction must leave the populated source intact.
    psql(url, migration.rsplit("commit;", 1)[0] + "select 1/0;\ncommit;", fail=True)
    if snapshot(url, "public") != before:
        raise SystemExit("failure atomicity snapshot drift")

    wal_start = psql(url, "select pg_current_wal_lsn();")
    connection = audit.parse_loopback_connection(url)
    blocker = subprocess.Popen(
        connection.command("-XAt", "-v", "ON_ERROR_STOP=1"), cwd=ROOT,
        env=connection.environment(),
        text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    assert blocker.stdin is not None
    blocker.stdin.write("begin; select count(*) from public.worker_jobs; select pg_sleep(0.25); commit;\n")
    blocker.stdin.close()
    time.sleep(0.05)
    started = time.monotonic()
    psql(url, migration)
    seconds = time.monotonic() - started
    blocker.wait(timeout=10)
    if blocker.returncode != 0:
        raise SystemExit("controlled concurrent reader failed")
    wal_bytes = int(psql(url, f"select pg_wal_lsn_diff(pg_current_wal_lsn(),'{wal_start}');"))
    if wal_bytes > 128 * 1024 * 1024:
        raise SystemExit(f"metadata migration WAL budget exceeded: {wal_bytes}")

    after = snapshot(url, "private")
    normalized_after(before, after)
    verify_minimum_acl(url)
    data_api.qualify_phase("v", label="populated Expand")
    psql(url, migration)  # controlled retry
    if snapshot(url, "private") != after:
        raise SystemExit("retry changed the exact post-state snapshot")
    phase = psql(url, """
      select (select count(*) from private.worker_jobs),
        (select count(*) from public.worker_jobs),
        (select count(*) from pg_proc p join pg_namespace n on n.oid=p.pronamespace
          where p.prosecdef and n.nspname in ('public','private','util'));
    """)
    if phase != f"{rows}|{rows}|315":
        raise SystemExit(f"post-state count/security closure drift: {phase}")
    print(json.dumps({
        "status": "PASS", "baseMigration": BASE, "scaleRows": rows,
        "expandSeconds": round(seconds, 3), "migrationWalBytes": wal_bytes,
        "failureAtomic": True, "controlledReader": True, "retryExact": True,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
