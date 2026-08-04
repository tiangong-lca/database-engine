#!/usr/bin/env python3
"""Destructive loopback qualification for Issue #414 physical Expand."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import secrets
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import psycopg
from psycopg import sql


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "supabase/migrations/20260804123000_issue_414_snapshot_gc_audit_physical_expand.sql"
ROLLBACK = ROOT / "supabase/operator/20260804_issue_414_snapshot_gc_audit_physical_expand_rollback.sql"
ROLLFORWARD = ROOT / "supabase/operator/20260804_issue_414_snapshot_gc_audit_physical_expand_rollforward.sql"
PREFIX = "41400000-0000-4000-8000-"
RUN_IDS = [uuid.UUID(f"{PREFIX}{index:012d}") for index in range(100, 105)]
API_RUN_ID = uuid.UUID(f"{PREFIX}{9000:012d}")


def scalar(conn: psycopg.Connection, sql: str, params: tuple = ()):
    return conn.execute(sql, params).fetchone()[0]


def psql(url: str, path: Path, *, expect_success: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["psql", url, "-X", "-v", "ON_ERROR_STOP=1", "-f", str(path)],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if expect_success and result.returncode:
        raise AssertionError(f"{path.name} failed:\n{result.stdout[-4000:]}")
    if not expect_success and not result.returncode:
        raise AssertionError(f"{path.name} unexpectedly succeeded")
    return result


def relation_state(conn: psycopg.Connection) -> tuple[str | None, str | None, str | None, str | None]:
    return tuple(
        scalar(conn, "select pg_catalog.to_regclass(%s)::text", (name,))
        for name in (
            "public.lca_snapshot_gc_runs",
            "public.lca_snapshot_gc_run_items",
            "private.lca_snapshot_gc_runs",
            "private.lca_snapshot_gc_run_items",
        )
    )


def table_oracle(conn: psycopg.Connection, schema: str) -> dict[str, tuple[int, str, int]]:
    result: dict[str, tuple[int, str, int]] = {}
    for name in ("lca_snapshot_gc_runs", "lca_snapshot_gc_run_items"):
        count, digest, oid = conn.execute(
            f"""
            select count(*),
              pg_catalog.md5(coalesce(pg_catalog.string_agg(
                pg_catalog.md5(pg_catalog.to_jsonb(source_row)::text), ''
                order by source_row.id
              ), '')),
              %s::pg_catalog.regclass::oid
            from {schema}.{name} source_row
            """,
            (f"{schema}.{name}",),
        ).fetchone()
        result[name] = (count, digest, oid)
    return result


def seed(conn: psycopg.Connection) -> None:
    with conn.cursor() as cursor:
        cursor.executemany(
            """
            insert into public.lca_snapshot_gc_runs (
              id, mode, status, as_of, diagnostics
            ) values (%s, 'dry_run', 'running', '2026-08-04 00:00:00+00', %s)
            """,
            [
                (run_id, json.dumps({"issue": 414, "run": index}))
                for index, run_id in enumerate(RUN_IDS)
            ],
        )
    conn.execute(
        """
        insert into public.lca_snapshot_gc_run_items (
          id, run_id, candidate_type, snapshot_id, bucket_id, object_name,
          storage_bytes, reason, delete_db_snapshot, action_status,
          created_at, updated_at
        )
        select
          ('41400000-0000-4000-8001-' || lpad(series::text, 12, '0'))::uuid,
          (%s::uuid[])[(series %% 5) + 1],
          case when series %% 2 = 0 then 'snapshot_directory'
               else 'orphan_storage_directory' end,
          null,
          'issue-414',
          'fixture/' || series,
          series,
          'production-equivalent-fixture',
          false,
          'planned',
          '2026-08-04 00:00:00+00'::timestamptz + series * interval '1 millisecond',
          '2026-08-04 00:00:00+00'::timestamptz + series * interval '1 millisecond'
        from pg_catalog.generate_series(1, 3205) series
        """,
        (RUN_IDS,),
    )
    conn.commit()


def assert_lock_timeout(url: str, expected: dict[str, tuple[int, str, int]]) -> int:
    with psycopg.connect(url) as holder:
        holder.execute("select count(*) from public.lca_snapshot_gc_runs")
        started = time.monotonic()
        result = psql(url, MIGRATION, expect_success=False)
        elapsed_ms = int((time.monotonic() - started) * 1000)
        if "lock timeout" not in result.stdout.lower():
            raise AssertionError(f"migration did not fail on lock timeout: {result.stdout[-2000:]}")
        if not 4500 <= elapsed_ms <= 8000:
            raise AssertionError(f"unexpected lock timeout duration: {elapsed_ms}ms")
    with psycopg.connect(url) as check:
        if relation_state(check) != (
            "lca_snapshot_gc_runs", "lca_snapshot_gc_run_items", None, None
        ):
            raise AssertionError("lock conflict left a partial relation move")
        if table_oracle(check, "public") != expected:
            raise AssertionError("lock conflict changed fixture data or OIDs")
    return elapsed_ms


def create_worker_login(url: str) -> tuple[str, str]:
    login = f"issue414_worker_{secrets.token_hex(6)}"
    password = secrets.token_urlsafe(32)
    with psycopg.connect(url, autocommit=True) as admin:
        admin.execute(
            sql.SQL(
                "create role {} login password {} "
                "nosuperuser nocreatedb nocreaterole nobypassrls inherit"
            ).format(sql.Identifier(login), sql.Literal(password))
        )
        admin.execute(
            sql.SQL(
                "grant lca_worker_runtime to {} "
                "with inherit true, set false, admin false"
            ).format(sql.Identifier(login))
        )
    return login, password


def login_url(url: str, login: str, password: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((
        parsed.scheme,
        f"{urllib.parse.quote(login, safe='')}:{urllib.parse.quote(password, safe='')}"
        f"@{parsed.hostname}:{parsed.port}",
        parsed.path,
        parsed.query,
        parsed.fragment,
    ))


def assert_role_matrix(url: str, login: str, password: str) -> None:
    worker_url = login_url(url, login, password)
    run_id = uuid.UUID(f"{PREFIX}{9100:012d}")
    item_id = uuid.UUID(f"{PREFIX}{9101:012d}")
    with psycopg.connect(worker_url) as worker:
        posture = worker.execute(
            """
            select session_user = current_user,
              not rolsuper and not rolbypassrls,
              pg_catalog.pg_has_role(session_user, 'lca_worker_runtime', 'member')
            from pg_catalog.pg_roles where rolname = session_user
            """
        ).fetchone()
        if posture != (True, True, True):
            raise AssertionError(f"Worker LOGIN posture drifted: {posture}")
        returned = scalar(worker, """
          insert into private.lca_snapshot_gc_runs (id,mode,status,diagnostics)
          values (%s,'dry_run','running','{}') returning id
        """, (run_id,))
        if returned != run_id:
            raise AssertionError("Worker INSERT RETURNING drifted")
        worker.execute("""
          insert into private.lca_snapshot_gc_run_items (
            id,run_id,candidate_type,bucket_id,object_name,reason
          ) values (%s,%s,'orphan_storage_directory','issue-414','role/item','role')
        """, (item_id, run_id))
        worker.execute(
            "update private.lca_snapshot_gc_run_items set action_status='dry_run' where id=%s",
            (item_id,),
        )
        worker.execute(
            "update private.lca_snapshot_gc_runs set status='succeeded' where id=%s",
            (run_id,),
        )
        try:
            worker.execute(
                "delete from private.lca_snapshot_gc_runs where id=%s", (run_id,)
            )
        except psycopg.errors.InsufficientPrivilege:
            worker.rollback()
        else:
            raise AssertionError("Worker unexpectedly acquired DELETE")

    with psycopg.connect(url) as admin:
        for role in ("anon", "authenticated"):
            admin.execute(f"set local role {role}")
            try:
                admin.execute("select count(*) from public.lca_snapshot_gc_runs")
            except psycopg.errors.InsufficientPrivilege:
                admin.rollback()
            else:
                raise AssertionError(f"{role} unexpectedly read the public compatibility view")
        admin.execute("set local role service_role")
        count = scalar(admin, "select count(*) from public.lca_snapshot_gc_runs")
        if count < 1:
            raise AssertionError("service_role public compatibility read drifted")
        admin.rollback()


def concurrent_worker_writes(url: str, login: str, password: str) -> int:
    worker_url = login_url(url, login, password)

    def one(index: int) -> None:
        run_id = uuid.UUID(f"{PREFIX}{9200 + index:012d}")
        item_id = uuid.UUID(f"{PREFIX}{9300 + index:012d}")
        with psycopg.connect(worker_url) as conn:
            conn.execute("""
              insert into private.lca_snapshot_gc_runs (id,mode,status,diagnostics)
              values (%s,'dry_run','running','{}')
            """, (run_id,))
            conn.execute("""
              insert into private.lca_snapshot_gc_run_items (
                id,run_id,candidate_type,bucket_id,object_name,reason
              ) values (%s,%s,'orphan_storage_directory','issue-414',%s,'concurrency')
            """, (item_id, run_id, f"concurrency/{index}"))
            conn.execute(
                "update private.lca_snapshot_gc_runs set status='succeeded' where id=%s",
                (run_id,),
            )
            conn.commit()

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(one, range(8)))
    return 8


def request_status(request: urllib.request.Request) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def assert_data_api(api_url: str, anon_key: str, service_key: str) -> None:
    root = api_url.rstrip("/") + "/rest/v1/lca_snapshot_gc_runs"
    common = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
        "Accept-Profile": "public",
        "Content-Profile": "public",
    }
    payload = json.dumps({
        "id": str(API_RUN_ID), "mode": "dry_run", "status": "running",
        "diagnostics": {"issue": 414, "transport": True},
    }).encode()
    status, _ = request_status(urllib.request.Request(
        root, data=payload, headers={**common, "Prefer": "return=representation"}, method="POST"
    ))
    if status not in (200, 201):
        raise AssertionError(f"service public compatibility POST failed: {status}")
    query = root + "?id=eq." + str(API_RUN_ID)
    status, body = request_status(urllib.request.Request(query, headers=common))
    if status != 200 or not json.loads(body):
        raise AssertionError(f"service public compatibility GET failed: {status}")
    anon = {
        "apikey": anon_key, "Authorization": f"Bearer {anon_key}",
        "Accept-Profile": "public",
    }
    status, _ = request_status(urllib.request.Request(query, headers=anon))
    if status not in (401, 403):
        raise AssertionError(f"anonymous compatibility read was not denied: {status}")
    private = {**common, "Accept-Profile": "private", "Content-Profile": "private"}
    status, _ = request_status(urllib.request.Request(query, headers=private))
    if status not in (404, 406):
        raise AssertionError(f"private PostgREST profile was exposed: {status}")


def assert_stack(url: str, expected_system_id: str, project_id: str, container: str) -> None:
    with psycopg.connect(url) as conn:
        actual_system_id = str(scalar(conn, "select system_identifier from pg_control_system()"))
        head = str(scalar(conn, "select max(version) from supabase_migrations.schema_migrations"))
    if actual_system_id != expected_system_id or head != "20260804123000":
        raise SystemExit(f"candidate identity mismatch: system={actual_system_id} head={head}")
    inspect = json.loads(subprocess.check_output(["docker", "inspect", container], text=True))[0]
    labels = inspect["Config"].get("Labels") or {}
    if labels.get("com.supabase.cli.project") != project_id:
        raise SystemExit("candidate container/project binding drifted")


def cleanup(url: str, login: str | None) -> None:
    with psycopg.connect(url, autocommit=True) as admin:
        if scalar(admin, "select to_regclass('private.lca_snapshot_gc_runs') is null"):
            psql(url, ROLLFORWARD)
        admin.execute(
            "delete from private.lca_snapshot_gc_run_items where id::text like '41400000-0000-4000-8001-%' or id::text like '41400000-0000-4000-8000-%'"
        )
        admin.execute(
            "delete from private.lca_snapshot_gc_runs where id::text like '41400000-0000-4000-8000-%'"
        )
        if login and scalar(admin, "select count(*) from pg_roles where rolname=%s", (login,)):
            admin.execute(
                sql.SQL("revoke lca_worker_runtime from {}").format(
                    sql.Identifier(login)
                )
            )
            admin.execute(sql.SQL("drop role {}").format(sql.Identifier(login)))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-url", required=True)
    parser.add_argument("--expected-system-id", required=True)
    parser.add_argument("--expected-project-id", required=True)
    parser.add_argument("--expected-container", required=True)
    parser.add_argument("--execution-mode", choices=("ci-hard-bound", "local-explicit-isolated"), required=True)
    parser.add_argument("--api-url")
    parser.add_argument("--confirm-isolated-destructive-test", action="store_true")
    args = parser.parse_args()
    parsed = urllib.parse.urlsplit(args.db_url)
    if parsed.hostname not in ("127.0.0.1", "localhost", "::1"):
        raise SystemExit("Issue 414 runtime accepts loopback databases only")
    if not args.confirm_isolated_destructive_test:
        raise SystemExit("--confirm-isolated-destructive-test is required")
    if args.execution_mode == "ci-hard-bound":
        if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("CI") != "true":
            raise SystemExit("ci-hard-bound requires GitHub Actions")
        if args.expected_project_id != "database-engine" or parsed.port != 55322:
            raise SystemExit("ci-hard-bound project or port drifted")
    elif not args.expected_project_id.startswith("database-engine-414-") or parsed.port == 55322:
        raise SystemExit("local run requires a unique database-engine-414-* stack and non-default port")

    anon_key = os.environ.get("ISSUE414_ANON_KEY")
    service_key = os.environ.get("ISSUE414_SERVICE_KEY")
    if len([value for value in (args.api_url, anon_key, service_key) if value]) not in (0, 3):
        raise SystemExit("--api-url and both ISSUE414 keys must be provided together")
    assert_stack(args.db_url, args.expected_system_id, args.expected_project_id, args.expected_container)

    login: str | None = None
    try:
        psql(args.db_url, ROLLBACK)
        with psycopg.connect(args.db_url) as conn:
            seed(conn)
            predecessor = table_oracle(conn, "public")
        lock_timeout_ms = assert_lock_timeout(args.db_url, predecessor)
        psql(args.db_url, MIGRATION)
        with psycopg.connect(args.db_url) as conn:
            expanded = table_oracle(conn, "private")
        if expanded != predecessor:
            raise AssertionError("physical move changed populated fixture data or OIDs")

        login, password = create_worker_login(args.db_url)
        assert_role_matrix(args.db_url, login, password)
        concurrency = concurrent_worker_writes(args.db_url, login, password)
        if args.api_url and anon_key and service_key:
            assert_data_api(args.api_url, anon_key, service_key)

        with psycopg.connect(args.db_url) as conn:
            before_roundtrip = table_oracle(conn, "private")
            wal_before = scalar(conn, "select pg_current_wal_lsn()")
        psql(args.db_url, MIGRATION)
        with psycopg.connect(args.db_url) as conn:
            retry_wal = int(scalar(
                conn, "select pg_wal_lsn_diff(pg_current_wal_lsn(), %s::pg_lsn)",
                (str(wal_before),),
            ))
            if table_oracle(conn, "private") != before_roundtrip:
                raise AssertionError("idempotent retry changed data or OIDs")
        if retry_wal > 4 * 1024 * 1024:
            raise AssertionError(f"idempotent retry WAL exceeded budget: {retry_wal}")

        psql(args.db_url, ROLLBACK)
        with psycopg.connect(args.db_url) as conn:
            if table_oracle(conn, "public") != before_roundtrip:
                raise AssertionError("operator rollback changed data or OIDs")
        psql(args.db_url, ROLLFORWARD)
        with psycopg.connect(args.db_url) as conn:
            if table_oracle(conn, "private") != before_roundtrip:
                raise AssertionError("explicit roll-forward changed data or OIDs")

        print(json.dumps({
            "ok": True,
            "fixtureItems": predecessor["lca_snapshot_gc_run_items"][0],
            "concurrencySessions": concurrency,
            "lockTimeoutMs": lock_timeout_ms,
            "retryWalBytes": retry_wal,
            "transport": bool(args.api_url),
        }, sort_keys=True))
    finally:
        cleanup(args.db_url, login)


if __name__ == "__main__":
    main()
