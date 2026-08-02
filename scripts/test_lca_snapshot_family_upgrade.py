#!/usr/bin/env python3
"""Destructive local-only migration proof for database-engine Issue #376."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "supabase/tests/contracts/lca_snapshot_family_expand.v1.json"
MIGRATIONS = ROOT / "supabase/migrations"


def run(
    command: list[str], *, sql: str | None = None, expect_failure: bool = False
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["PGSSLMODE"] = "disable"
    result = subprocess.run(
        command,
        cwd=ROOT,
        input=sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=environment,
    )
    failed = result.returncode != 0
    if failed != expect_failure:
        raise SystemExit(
            f"unexpected command result ({result.returncode}): {command[0]}\n"
            f"{result.stdout}\n{result.stderr}"
        )
    return result


def psql(db_url: str, sql: str, *, expect_failure: bool = False) -> subprocess.CompletedProcess[str]:
    return run(
        ["psql", db_url, "-qXAt", "-v", "ON_ERROR_STOP=1"],
        sql=sql,
        expect_failure=expect_failure,
    )


def scalar(db_url: str, sql: str) -> str:
    return psql(db_url, sql).stdout.strip()


def json_query(db_url: str, sql: str) -> object:
    return json.loads(scalar(db_url, sql))


def assert_loopback_database(db_url: str) -> None:
    parsed = urlparse(db_url)
    if parsed.scheme not in ("postgres", "postgresql"):
        raise SystemExit("--db-url must be a PostgreSQL URL")
    if parsed.hostname not in ("127.0.0.1", "localhost", "::1"):
        raise SystemExit("Issue #376 qualification is local-only and requires loopback")


def assert_shape(db_url: str, schema: str) -> None:
    expected = 3 if schema == "private" else 0
    private_count = int(scalar(db_url, """
      select count(*) from pg_class c join pg_namespace n on n.oid=c.relnamespace
      where n.nspname='private' and c.relkind='r' and c.relname in
        ('lca_active_snapshots','lca_network_snapshots','lca_snapshot_artifacts');
    """))
    public_table_count = int(scalar(db_url, """
      select count(*) from pg_class c join pg_namespace n on n.oid=c.relnamespace
      where n.nspname='public' and c.relkind='r' and c.relname in
        ('lca_active_snapshots','lca_network_snapshots','lca_snapshot_artifacts');
    """))
    if private_count != expected or public_table_count != 3 - expected:
        raise SystemExit(
            f"unexpected physical shape: private={private_count}, public={public_table_count}"
        )


ORACLE_SQL = """
with relations(name, relation_oid) as (
  values
    ('lca_active_snapshots', to_regclass('{schema}.lca_active_snapshots')),
    ('lca_network_snapshots', to_regclass('{schema}.lca_network_snapshots')),
    ('lca_snapshot_artifacts', to_regclass('{schema}.lca_snapshot_artifacts'))
), facts as (
  select name, relation_oid::bigint as oid,
    case name
      when 'lca_active_snapshots' then
        (select jsonb_build_object(
          'rows', count(*),
          'pk', coalesce(sum(hashtextextended(scope,376)::numeric),0)::text,
          'content', coalesce(sum(hashtextextended(
            to_jsonb(t)::text,1376)::numeric),0)::text)
         from {schema}.lca_active_snapshots t)
      when 'lca_network_snapshots' then
        (select jsonb_build_object(
          'rows', count(*),
          'pk', coalesce(sum(hashtextextended(id::text,376)::numeric),0)::text,
          'content', coalesce(sum(hashtextextended(
            to_jsonb(t)::text,1376)::numeric),0)::text)
         from {schema}.lca_network_snapshots t)
      when 'lca_snapshot_artifacts' then
        (select jsonb_build_object(
          'rows', count(*),
          'pk', coalesce(sum(hashtextextended(id::text,376)::numeric),0)::text,
          'content', coalesce(sum(hashtextextended(
            to_jsonb(t)::text,1376)::numeric),0)::text)
         from {schema}.lca_snapshot_artifacts t)
    end as data
  from relations
)
select jsonb_object_agg(name, jsonb_build_object('oid',oid,'data',data) order by name)::text
from facts;
"""


def oracle(db_url: str, schema: str) -> dict[str, object]:
    value = json_query(db_url, ORACLE_SQL.format(schema=schema))
    if not isinstance(value, dict) or len(value) != 3:
        raise SystemExit(f"incomplete {schema} LCA snapshot oracle")
    return value


def seed(db_url: str, rows: int) -> None:
    psql(db_url, f"""
      insert into public.lca_network_snapshots (
        id,scope,process_filter,source_hash,status
      )
      select ('37600000-0000-4000-8000-' || lpad(i::text,12,'0'))::uuid,
             case when i % 2 = 0 then 'full_library' else 'data_product' end,
             jsonb_build_object('fixture',i), md5(i::text), 'ready'
      from generate_series(1,{rows}) i;

      insert into public.lca_snapshot_artifacts (
        id,snapshot_id,artifact_url,artifact_sha256,artifact_byte_size,
        artifact_format,process_count,flow_count,impact_count,a_nnz,b_nnz,c_nnz,
        coverage,status
      )
      select ('37700000-0000-4000-8000-' || lpad(i::text,12,'0'))::uuid,
             ('37600000-0000-4000-8000-' || lpad(i::text,12,'0'))::uuid,
             's3://issue376/' || i, repeat('a',64), i, 'snapshot-hdf5:v1',
             i, i+1, i+2, i+3, i+4, i+5, jsonb_build_object('fixture',i), 'ready'
      from generate_series(1,{rows}) i;

      insert into public.lca_active_snapshots(scope,snapshot_id,source_hash)
      values
        ('full_library','37600000-0000-4000-8000-000000000002',md5('2')),
        ('data_product','37600000-0000-4000-8000-000000000001',md5('1'));
    """)


def start_lock_holder(db_url: str) -> subprocess.Popen[str]:
    process = subprocess.Popen(
        ["psql", db_url, "-qXAt", "-v", "ON_ERROR_STOP=1"],
        cwd=ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdin is not None
    process.stdin.write("""
      set application_name='issue-376-second-alter-holder';
      begin;
      lock table public.lca_network_snapshots in access share mode;
      select pg_sleep(30);
    """)
    process.stdin.close()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if scalar(db_url, """
          select exists (
            select 1 from pg_stat_activity a join pg_locks l on l.pid=a.pid
            where a.application_name='issue-376-second-alter-holder'
              and l.relation='public.lca_network_snapshots'::regclass and l.granted
          );
        """) == "t":
            return process
        time.sleep(0.05)
    process.terminate()
    raise SystemExit("lock holder did not acquire the second ALTER target")


def stop_holder(db_url: str, process: subprocess.Popen[str]) -> None:
    psql(db_url, """
      select pg_terminate_backend(pid)
      from pg_stat_activity
      where application_name='issue-376-second-alter-holder'
        and pid <> pg_backend_pid();
    """)
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


ROLLBACK_SQL = """
begin;
drop function api.lca_snapshot_active_read_v1(text),
  api.lca_snapshot_scope_read_v1(uuid), api.lca_snapshot_resolve_v1(text,jsonb),
  api.lca_snapshot_artifact_read_v1(uuid), api.lca_snapshot_artifact_latest_v1(),
  api.cmd_lca_snapshot_create_v1(uuid,text,jsonb,uuid);
drop view public.lca_active_snapshots, public.lca_network_snapshots,
  public.lca_snapshot_artifacts;
alter table private.lca_active_snapshots set schema public;
alter table private.lca_network_snapshots set schema public;
alter table private.lca_snapshot_artifacts set schema public;
commit;
"""


def assert_same_oracle(expected: dict[str, object], actual: dict[str, object], phase: str) -> None:
    if expected != actual:
        raise SystemExit(f"Issue #376 {phase} changed OID/row/PK/content oracle")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-url", required=True, help="destructive isolated loopback Supabase DB")
    parser.add_argument("--representative-rows", type=int)
    parser.add_argument("--allow-small-fixture", action="store_true")
    args = parser.parse_args()
    assert_loopback_database(args.db_url)

    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    qualification = contract["qualification"]
    rows = args.representative_rows or qualification["defaultRepresentativeRows"]
    if rows < qualification["minimumQualificationRows"] and not args.allow_small_fixture:
        parser.error("qualification requires minimumQualificationRows")
    if rows < 2:
        parser.error("representative rows must be at least two")

    expected_commit = contract["databaseBaseCommit"]
    actual_commit = run(["git", "rev-parse", "HEAD"]).stdout.strip()
    ancestry = subprocess.run(
        ["git", "merge-base", "--is-ancestor", expected_commit, actual_commit],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if ancestry.returncode != 0:
        raise SystemExit(
            f"database base is not an ancestor of checkout: base={expected_commit}, head={actual_commit}"
        )
    predecessor = contract["predecessorMigrationHead"]
    migration = MIGRATIONS / contract["migration"]
    if not migration.is_file() or migration.name.split("_", 1)[0] != "20260802091342":
        raise SystemExit("Issue #376 exact migration binding is invalid")
    numeric_head = max(path.name.split("_", 1)[0] for path in MIGRATIONS.glob("[0-9]*.sql"))
    if numeric_head != "20260802091342":
        raise SystemExit(f"Issue #376 migration is not repository head: {numeric_head}")
    base_migrations = run([
        "git", "ls-tree", "-r", "--name-only", expected_commit, "--", "supabase/migrations",
    ]).stdout.splitlines()
    base_versions = [
        Path(path).name.split("_", 1)[0]
        for path in base_migrations
        if Path(path).name[:1].isdigit() and "_" in Path(path).name
    ]
    if not base_versions or max(base_versions) != predecessor:
        actual_base_head = max(base_versions) if base_versions else "<none>"
        raise SystemExit(
            f"database base migration head mismatch: expected={predecessor}, actual={actual_base_head}"
        )
    committed_migration_delta = set(run([
        "git", "diff", "--name-only", expected_commit, actual_commit,
        "--", "supabase/migrations",
    ]).stdout.splitlines())
    expected_migration_path = migration.relative_to(ROOT).as_posix()
    if actual_commit == expected_commit:
        staged_identity = run([
            "git", "ls-files", "--stage", "--", expected_migration_path,
        ]).stdout
        if not staged_identity.strip():
            raise SystemExit(
                "base-checkout development run requires the Issue #376 migration in the Git index"
            )
    elif committed_migration_delta != {expected_migration_path}:
        raise SystemExit(
            "committed migration delta from exact base must contain only Issue #376: "
            f"{sorted(committed_migration_delta)}"
        )

    run([
        "supabase", "db", "reset", "--db-url", args.db_url,
        "--version", predecessor, "--no-seed", "--yes",
    ])
    assert_shape(args.db_url, "public")
    seed(args.db_url, rows)
    base_oracle = oracle(args.db_url, "public")

    holder = start_lock_holder(args.db_url)
    try:
        started = time.monotonic()
        lock_failure = psql(args.db_url, migration.read_text(encoding="utf-8"), expect_failure=True)
        lock_seconds = time.monotonic() - started
    finally:
        stop_holder(args.db_url, holder)
    budgets = qualification["budgets"]
    if not (budgets["lockFailureMinimumSeconds"] <= lock_seconds <= budgets["lockFailureMaximumSeconds"]):
        raise SystemExit(f"lock timeout outside budget: {lock_seconds:.3f}s")
    if "lock timeout" not in lock_failure.stderr.lower():
        raise SystemExit("second ALTER contention did not fail through lock_timeout")
    if "alter table public.lca_network_snapshots set schema private" not in lock_failure.stderr.lower():
        raise SystemExit("lock fault did not reach the reviewed second ALTER target")
    assert_shape(args.db_url, "public")
    assert_same_oracle(base_oracle, oracle(args.db_url, "public"), "lock rollback")

    wal_start = scalar(args.db_url, "select pg_current_wal_lsn()::text")
    started = time.monotonic()
    run(["supabase", "migration", "up", "--db-url", args.db_url])
    upgrade_seconds = time.monotonic() - started
    wal_end = scalar(args.db_url, "select pg_current_wal_lsn()::text")
    wal_bytes = int(scalar(
        args.db_url, f"select pg_wal_lsn_diff('{wal_end}','{wal_start}')::bigint"
    ))
    if upgrade_seconds > budgets["upgradeSeconds"] or wal_bytes > budgets["migrationWalBytes"]:
        raise SystemExit(f"upgrade budget exceeded: seconds={upgrade_seconds:.3f}, wal={wal_bytes}")
    assert_shape(args.db_url, "private")
    head_oracle = oracle(args.db_url, "private")
    assert_same_oracle(base_oracle, head_oracle, "clean roll-forward")

    retry_wal_start = scalar(args.db_url, "select pg_current_wal_lsn()::text")
    started = time.monotonic()
    psql(args.db_url, migration.read_text(encoding="utf-8"))
    retry_seconds = time.monotonic() - started
    retry_wal_end = scalar(args.db_url, "select pg_current_wal_lsn()::text")
    retry_wal_bytes = int(scalar(
        args.db_url,
        f"select pg_wal_lsn_diff('{retry_wal_end}','{retry_wal_start}')::bigint",
    ))
    if retry_seconds > budgets["retrySeconds"] or retry_wal_bytes > budgets["retryWalBytes"]:
        raise SystemExit(f"retry budget exceeded: seconds={retry_seconds:.3f}, wal={retry_wal_bytes}")
    assert_same_oracle(head_oracle, oracle(args.db_url, "private"), "retry")

    psql(args.db_url, "grant select on private.lca_network_snapshots to authenticated")
    drift_failure = psql(
        args.db_url, migration.read_text(encoding="utf-8"), expect_failure=True
    )
    if "source acl drift for private.lca_network_snapshots" not in drift_failure.stderr.lower():
        raise SystemExit("private retry drift did not fail at the exact preflight")
    assert_same_oracle(head_oracle, oracle(args.db_url, "private"), "private-drift failure")
    psql(args.db_url, "revoke select on private.lca_network_snapshots from authenticated")

    psql(args.db_url, ROLLBACK_SQL)
    assert_shape(args.db_url, "public")
    assert_same_oracle(base_oracle, oracle(args.db_url, "public"), "committed rollback")
    psql(args.db_url, migration.read_text(encoding="utf-8"))
    assert_shape(args.db_url, "private")
    assert_same_oracle(head_oracle, oracle(args.db_url, "private"), "committed roll-forward")

    summary = {
        "status": "passed",
        "databaseBaseCommit": expected_commit,
        "predecessorMigrationHead": predecessor,
        "migrationHead": "20260802091342",
        "representativeRows": rows,
        "lockFailureSeconds": round(lock_seconds, 3),
        "upgradeSeconds": round(upgrade_seconds, 3),
        "migrationWalBytes": wal_bytes,
        "retrySeconds": round(retry_seconds, 3),
        "retryWalBytes": retry_wal_bytes,
        "oracle": head_oracle,
    }
    print(json.dumps(summary, sort_keys=True))
    print("PASS Issue #376 populated upgrade, faults, retry, rollback, and roll-forward")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
