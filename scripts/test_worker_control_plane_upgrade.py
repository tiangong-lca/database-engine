#!/usr/bin/env python3
"""Populated base-to-head, failure atomicity, and migration retry proof."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = "20260731124000"
EXPAND = ROOT / "supabase/migrations/20260731163321_worker_control_plane_private_expand.sql"
REPAIR = ROOT / "supabase/migrations/20260731164051_repair_canonical_lint_errors.sql"


def run(command: list[str], *, sql: str | None = None, expect_failure: bool = False) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, cwd=ROOT, input=sql, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if expect_failure == (result.returncode == 0):
        raise SystemExit(f"unexpected command result ({result.returncode}): {' '.join(command)}\n{result.stdout}\n{result.stderr}")
    return result


def psql(db_url: str, sql: str, *, expect_failure: bool = False) -> subprocess.CompletedProcess[str]:
    return run(["psql", db_url, "-XAt", "-v", "ON_ERROR_STOP=1"], sql=sql, expect_failure=expect_failure)


def main() -> int:
    run(["supabase", "db", "reset", "--local", "--version", BASE])
    status = run(["supabase", "status", "--output", "json"])
    db_url = json.loads(status.stdout)["DB_URL"]
    fixture = """
      insert into public.worker_jobs(id,job_kind,worker_runtime,worker_queue,requester_type,status,payload_schema_version,payload_json)
      values ('beef0000-0000-4000-8000-000000000001','lca.snapshot_gc','calculator','maintenance','service','queued','lca.snapshot_gc.request.v1','{"populated":true}');
      insert into public.worker_job_events(id,job_id,event_type,status,details)
      values ('beef0000-0000-4000-8000-000000000002','beef0000-0000-4000-8000-000000000001','queued','queued','{"populated":true}');
      insert into public.worker_job_artifacts(id,job_id,artifact_type,content_type,metadata,visibility)
      values ('beef0000-0000-4000-8000-000000000003','beef0000-0000-4000-8000-000000000001','upgrade-proof','application/json','{"populated":true}','operator');
    """
    psql(db_url, fixture)
    expand = EXPAND.read_text(encoding="utf-8")
    for required in ("set local lock_timeout = '5s'", "set local statement_timeout = '2min'"):
        if required not in expand.lower():
            raise SystemExit(f"migration lock budget missing: {required}")
    domain_before = psql(db_url, "select md5(pg_get_viewdef('public.worker_job_domain_refs'::regclass,true)||coalesce((select relacl::text from pg_class where oid='public.worker_job_domain_refs'::regclass),'')||coalesce((select reloptions::text from pg_class where oid='public.worker_job_domain_refs'::regclass),''));").stdout.strip()
    failure = expand.rsplit("commit;", 1)[0] + "select 1/0;\ncommit;\n"
    psql(db_url, failure, expect_failure=True)
    atomic = psql(db_url, "select to_regclass('private.worker_jobs') is null, count(*) from public.worker_jobs where id='beef0000-0000-4000-8000-000000000001';")
    if atomic.stdout.strip() != "t|1":
        raise SystemExit(f"failed migration left residue: {atomic.stdout}")
    started = time.monotonic()
    psql(db_url, expand)
    expand_seconds = time.monotonic() - started
    psql(db_url, expand)  # retry must converge
    psql(db_url, REPAIR.read_text(encoding="utf-8"))
    parity = psql(db_url, """
      select
        (select count(*) from public.worker_job_kinds)=(select count(*) from private.worker_job_kinds),
        (select count(*) from public.worker_jobs)=(select count(*) from private.worker_jobs),
        (select count(*) from public.worker_job_events)=(select count(*) from private.worker_job_events),
        (select count(*) from public.worker_job_artifacts)=(select count(*) from private.worker_job_artifacts),
        exists(select 1 from private.worker_job_kinds where job_kind='lca.snapshot_gc'),
        exists(select 1 from private.worker_jobs where id='beef0000-0000-4000-8000-000000000001'),
        exists(select 1 from private.worker_job_events where id='beef0000-0000-4000-8000-000000000002'),
        exists(select 1 from private.worker_job_artifacts where id='beef0000-0000-4000-8000-000000000003' and metadata @> '{"populated":true}'),
        to_regclass('public.worker_jobs_job_kind_concurrency_created_idx') is not null;
    """)
    if parity.stdout.strip() != "t|t|t|t|t|t|t|t|t":
        raise SystemExit(f"populated row parity failed: {parity.stdout}")
    domain_after = psql(db_url, "select md5(pg_get_viewdef('public.worker_job_domain_refs'::regclass,true)||coalesce((select relacl::text from pg_class where oid='public.worker_job_domain_refs'::regclass),'')||coalesce((select reloptions::text from pg_class where oid='public.worker_job_domain_refs'::regclass),''));").stdout.strip()
    if domain_before != domain_after:
        raise SystemExit("public.worker_job_domain_refs catalog contract drifted")
    run(["supabase", "test", "db", "supabase/tests/20260731_worker_control_plane_private_expand.sql", "--local"])
    print(f"PASS rehearsal populated upgrade {BASE} -> 20260731164051; four-relation parity; concurrency index present; domain projection stable; failure atomic; retry stable; expandSeconds={expand_seconds:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
