#!/usr/bin/env python3
"""Local-only production-equivalent populated base-to-head upgrade proof."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "supabase/tests/contracts/production_equivalent_upgrade.v1.json"
FIXTURE_PATH = ROOT / "supabase/tests/fixtures/20260801_production_equivalent_upgrade.sql"
MIGRATIONS = ROOT / "supabase/migrations"


def run(
    command: list[str], *, input_text: str | None = None, expect_failure: bool = False
) -> subprocess.CompletedProcess[str]:
    display = ["<database-url>" if part.startswith("postgresql://") else part for part in command]
    print("+", " ".join(display), flush=True)
    environment = None
    if "--db-url" in command:
        environment = os.environ.copy()
        environment["PGSSLMODE"] = "disable"
    result = subprocess.run(
        command, cwd=ROOT, input=input_text, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=environment,
    )
    failed = result.returncode != 0
    if failed != expect_failure:
        raise SystemExit(
            f"unexpected command result ({result.returncode}): {' '.join(display)}\n"
            f"{result.stdout}\n{result.stderr}"
        )
    return result


def psql(db_url: str, sql: str, *, expect_failure: bool = False) -> subprocess.CompletedProcess[str]:
    return run(
        ["psql", db_url, "-qXAt", "-v", "ON_ERROR_STOP=1"],
        input_text=sql, expect_failure=expect_failure,
    )


def scalar(db_url: str, sql: str) -> str:
    return psql(db_url, sql).stdout.strip()


def json_query(db_url: str, sql: str) -> object:
    output = scalar(db_url, sql)
    return json.loads(output)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def stable_hash(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return sha256_bytes(encoded)


def migration_files(base: str, head: str) -> list[Path]:
    selected: list[Path] = []
    for path in sorted(MIGRATIONS.glob("*.sql")):
        match = re.match(r"^(\d+)_", path.name)
        if match and base < match.group(1) <= head:
            selected.append(path)
    if not selected or re.match(r"^(\d+)_", selected[-1].name).group(1) != head:
        raise SystemExit(f"expected migration head {head} was not selected")
    return selected


DATA_ORACLE_SQL = r"""
begin;
create temporary table issue_341_oracle (
  relation text primary key,
  row_count bigint not null,
  primary_key_hash text not null,
  row_hash text not null
) on commit drop;

do $oracle$
declare
  relation_row record;
  primary_key_expression text;
  relation_count bigint;
  primary_key_hash text;
  row_hash text;
begin
  for relation_row in
    select namespace.nspname as schema_name, class.relname as relation_name,
           class.oid as relation_oid
    from pg_catalog.pg_class as class
    join pg_catalog.pg_namespace as namespace on namespace.oid = class.relnamespace
    where namespace.nspname in ('public', 'private', 'util', 'archive', 'api')
      and class.relkind in ('r', 'p')
    order by namespace.nspname, class.relname
  loop
    select string_agg(format('to_jsonb(t.%I)', attribute.attname), ', ' order by key_column.ordinality)
      into primary_key_expression
    from pg_catalog.pg_index as index_row
    cross join lateral unnest(index_row.indkey::smallint[]) with ordinality
      as key_column(attribute_number, ordinality)
    join pg_catalog.pg_attribute as attribute
      on attribute.attrelid = index_row.indrelid
     and attribute.attnum = key_column.attribute_number
    where index_row.indrelid = relation_row.relation_oid
      and index_row.indisprimary;

    primary_key_expression := case
      when primary_key_expression is null then quote_literal('<no-primary-key>')
      else format('jsonb_build_array(%s)::text', primary_key_expression)
    end;

    execute format(
      'select count(*), '
      'coalesce(sum(pg_catalog.hashtextextended((%s)::text, 341)::numeric), 0)::text, '
      'coalesce(sum(pg_catalog.hashtextextended(to_jsonb(t)::text, 1341)::numeric), 0)::text '
      'from %I.%I as t',
      primary_key_expression, relation_row.schema_name, relation_row.relation_name
    ) into relation_count, primary_key_hash, row_hash;

    insert into issue_341_oracle values (
      relation_row.schema_name || '.' || relation_row.relation_name,
      relation_count, primary_key_hash, row_hash
    );
  end loop;
end
$oracle$;

select jsonb_object_agg(
  relation,
  jsonb_build_object(
    'rowCount', row_count,
    'primaryKeyHash', primary_key_hash,
    'rowHash', row_hash
  ) order by relation
)::text
from issue_341_oracle;
rollback;
"""


CATALOG_INVARIANTS_SQL = r"""
with facts as (
  select 'relation-security' as category,
    jsonb_build_object(
      'schema', n.nspname, 'name', c.relname, 'kind', c.relkind,
      'rls', c.relrowsecurity, 'forceRls', c.relforcerowsecurity,
      'acl', coalesce(c.relacl::text, 'null')
    ) as fact
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid = c.relnamespace
  where n.nspname in ('public','private','util','archive') and c.relkind in ('r','p')
  union all
  select 'constraint', jsonb_build_object(
    'schema', n.nspname, 'table', c.relname, 'name', con.conname,
    'type', con.contype, 'validated', con.convalidated,
    'definition', pg_catalog.pg_get_constraintdef(con.oid, true)
  )
  from pg_catalog.pg_constraint con
  join pg_catalog.pg_class c on c.oid = con.conrelid
  join pg_catalog.pg_namespace n on n.oid = c.relnamespace
  where n.nspname in ('public','private','util','archive')
  union all
  select 'trigger', jsonb_build_object(
    'schema', n.nspname, 'table', c.relname, 'name', t.tgname,
    'enabled', t.tgenabled, 'definition', pg_catalog.pg_get_triggerdef(t.oid, true)
  )
  from pg_catalog.pg_trigger t
  join pg_catalog.pg_class c on c.oid = t.tgrelid
  join pg_catalog.pg_namespace n on n.oid = c.relnamespace
  where n.nspname in ('public','private','util','archive') and not t.tgisinternal
  union all
  select 'policy', to_jsonb(p) - 'qual' - 'with_check'
    || jsonb_build_object('qual', coalesce(p.qual, ''), 'withCheck', coalesce(p.with_check, ''))
  from pg_catalog.pg_policies p
  where p.schemaname in ('public','private','util','archive')
  union all
  select 'publication', jsonb_build_object(
    'publication', p.pubname, 'schema', n.nspname, 'table', c.relname
  )
  from pg_catalog.pg_publication_rel pr
  join pg_catalog.pg_publication p on p.oid = pr.prpubid
  join pg_catalog.pg_class c on c.oid = pr.prrelid
  join pg_catalog.pg_namespace n on n.oid = c.relnamespace
)
select coalesce(jsonb_agg(jsonb_build_object('category', category, 'fact', fact)
  order by category, fact::text), '[]'::jsonb)::text from facts;
"""


FIXTURE_SURFACES_SQL = r"""
select jsonb_build_object(
  'identity', exists(select 1 from public.users where id='34100000-0000-4000-8000-000000000001'),
  'notification', exists(select 1 from public.notifications where id='34100000-0000-4000-8000-000000000020'),
  'audit', exists(select 1 from public.command_audit_log where command='issue_341_fixture'),
  'review', exists(select 1 from public.reviews where id='34100000-0000-4000-8000-000000000010'),
  'worker-active', exists(select 1 from public.worker_jobs where status='queued' and concurrency_key='issue-341-queued'),
  'worker-lease', exists(select 1 from public.worker_jobs where status='running' and lease_token is not null),
  'worker-retry', exists(select 1 from public.worker_jobs where status='stale' and attempt_count=2),
  'worker-failure', exists(select 1 from public.worker_jobs where status='failed' and retryable=false),
  'worker-artifact', exists(select 1 from public.worker_job_artifacts where artifact_type='upgrade-evidence'),
  'package', exists(select 1 from public.lca_package_artifacts where id='34100000-0000-4000-8000-000000000302' and worker_job_id='34100000-0000-4000-8000-000000000107'),
  'package-evidence-million-scale', (select count(*) from public.lca_package_export_items where job_id='34100000-0000-4000-8000-000000000301'),
  'cache', exists(select 1 from public.lca_package_request_cache where request_key='issue-341-cache'),
  'release', exists(select 1 from public.lca_release_runs where id='34100000-0000-4000-8000-000000000401'),
  'closure', exists(select 1 from public.lcia_scope_closure_checks where id='34100000-0000-4000-8000-000000000501')
)::text;
"""


ROLE_MATRIX_SQL = r"""
select jsonb_build_object(
  'apiSchema', to_regnamespace('api') is not null,
  'apiAnonUsage', has_schema_privilege('anon', 'api', 'usage'),
  'apiAuthenticatedUsage', has_schema_privilege('authenticated', 'api', 'usage'),
  'apiServiceUsage', has_schema_privilege('service_role', 'api', 'usage'),
  'privateWorkerAnonDenied', not has_table_privilege('anon', 'private.worker_jobs', 'select'),
  'privateWorkerAuthenticatedDenied', not has_table_privilege('authenticated', 'private.worker_jobs', 'select'),
  'apiProcessesAnonSelect', has_table_privilege('anon', 'api.processes_v1', 'select'),
  'apiWorkerAnonDenied', not has_function_privilege('anon', 'api.worker_read_jobs_by_ids_v1(uuid[],boolean)', 'execute'),
  'apiWorkerServiceExecute', has_function_privilege('service_role', 'api.worker_read_jobs_by_ids_v1(uuid[],boolean)', 'execute'),
  'privateWorkerServiceSelect', has_table_privilege('service_role', 'private.worker_jobs', 'select'),
  'apiNotPublished', not exists (
    select 1 from pg_catalog.pg_publication_rel pr
    join pg_catalog.pg_class c on c.oid=pr.prrelid
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='api'
  )
)::text;
"""


def assert_all_true(values: dict[str, object], *, numeric_key: str | None = None, minimum: int = 0) -> None:
    for key, value in values.items():
        if key == numeric_key:
            if not isinstance(value, int) or value < minimum:
                raise SystemExit(f"{key}={value!r} is below required {minimum}")
        elif value is not True:
            raise SystemExit(f"required proof failed: {key}={value!r}")


def inject_failure(sql: str) -> str:
    if not re.search(r"\bcommit;\s*$", sql, flags=re.IGNORECASE):
        raise SystemExit("migration does not end in COMMIT; and cannot be fault-injected safely")
    return re.sub(
        r"\bcommit;\s*$", "select 1 / 0;\ncommit;\n", sql,
        count=1, flags=re.IGNORECASE,
    )


def wait_for_holder(db_url: str, application_name: str) -> None:
    for _ in range(100):
        count = scalar(db_url, f"select count(*) from pg_stat_activity where application_name='{application_name}' and wait_event='PgSleep';")
        if count == "1":
            return
        time.sleep(0.05)
    raise SystemExit(f"lock holder {application_name} did not become ready")


def start_lock_holder(db_url: str, mode: str, application_name: str) -> subprocess.Popen[str]:
    sql = (
        f"set application_name='{application_name}'; begin; "
        f"lock table public.worker_jobs in {mode} mode; select pg_sleep(30); rollback;"
    )
    process = subprocess.Popen(
        ["psql", db_url, "-XAt", "-v", "ON_ERROR_STOP=1"], cwd=ROOT,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    assert process.stdin is not None
    process.stdin.write(sql)
    process.stdin.close()
    wait_for_holder(db_url, application_name)
    return process


def stop_holder(process: subprocess.Popen[str]) -> None:
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--representative-rows", type=int)
    parser.add_argument("--evidence-out", type=Path, required=True)
    parser.add_argument(
        "--db-url",
        help="explicit loopback PostgreSQL URL for an isolated local Supabase stack",
    )
    parser.add_argument("--allow-dirty", action="store_true")
    args = parser.parse_args()

    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    rows = args.representative_rows or contract["defaultRepresentativeRows"]
    if rows < 1:
        parser.error("--representative-rows must be positive")
    if rows < contract["minimumQualificationRows"] and not args.allow_dirty:
        parser.error("qualification runs require at least minimumQualificationRows")

    dirty = bool(run(["git", "status", "--porcelain"]).stdout.strip())
    if dirty and not args.allow_dirty:
        raise SystemExit("qualification evidence requires a clean exact commit; use --allow-dirty only while developing")

    base = contract["baseVersion"]
    head = contract["expectedHeadVersion"]
    migration_paths = migration_files(base, head)
    migration_manifest = [
        {
            "version": re.match(r"^(\d+)_", path.name).group(1),
            "path": path.relative_to(ROOT).as_posix(),
            "sha256": sha256_bytes(path.read_bytes()),
        }
        for path in migration_paths
    ]

    db_url = args.db_url
    if db_url:
        parsed_url = urlparse(db_url)
        if parsed_url.scheme not in ("postgres", "postgresql") or parsed_url.hostname not in ("127.0.0.1", "localhost", "::1"):
            raise SystemExit("--db-url must identify an explicit loopback PostgreSQL database")
        target_args = ["--db-url", db_url]
    else:
        status = json.loads(run(["supabase", "status", "--output", "json"]).stdout)
        db_url = status["DB_URL"]
        parsed_url = urlparse(db_url)
        if parsed_url.hostname not in ("127.0.0.1", "localhost", "::1"):
            raise SystemExit("selected Supabase database is not loopback-local")
        target_args = ["--local"]
    run(["supabase", "db", "reset", *target_args, "--version", base, "--no-seed", "--yes"])
    fixture_started = time.monotonic()
    run([
        "psql", db_url, "-X", "-v", "ON_ERROR_STOP=1",
        "-v", f"representative_rows={rows}", "-f", str(FIXTURE_PATH),
    ])
    fixture_seconds = time.monotonic() - fixture_started
    if fixture_seconds > contract["budgets"]["fixtureSeconds"]:
        raise SystemExit(f"fixture budget exceeded: {fixture_seconds:.3f}s")

    surfaces = json_query(db_url, FIXTURE_SURFACES_SQL)
    assert isinstance(surfaces, dict)
    assert_all_true(
        surfaces, numeric_key="package-evidence-million-scale",
        minimum=rows,
    )
    if set(surfaces) != set(contract["requiredFixtureSurfaces"]):
        raise SystemExit("fixture surface contract and runner output diverged")

    data_before = json_query(db_url, DATA_ORACLE_SQL)
    catalog_before = json_query(db_url, CATALOG_INVARIANTS_SQL)
    catalog_before_hash = stable_hash(catalog_before)

    fault_results: list[dict[str, object]] = []
    for path in migration_paths:
        started = time.monotonic()
        result = psql(db_url, inject_failure(path.read_text(encoding="utf-8")), expect_failure=True)
        elapsed = time.monotonic() - started
        if "division by zero" not in result.stderr.lower():
            raise SystemExit(f"fault injection for {path.name} failed for an unexpected reason")
        if stable_hash(json_query(db_url, CATALOG_INVARIANTS_SQL)) != catalog_before_hash:
            raise SystemExit(f"failed migration left catalog residue: {path.name}")
        if json_query(db_url, DATA_ORACLE_SQL) != data_before:
            raise SystemExit(f"failed migration changed row/PK/hash oracle: {path.name}")
        fault_results.append({"version": migration_manifest[len(fault_results)]["version"], "seconds": round(elapsed, 3)})
        # Establish the successfully committed prerequisite before faulting the
        # next migration. Migration history remains at the selected base, so
        # the later CLI roll-forward still exercises the complete pending set.
        if path != migration_paths[-1]:
            psql(db_url, path.read_text(encoding="utf-8"))

    exclusive_holder = start_lock_holder(db_url, "access exclusive", "issue-341-exclusive-holder")
    try:
        lock_started = time.monotonic()
        lock_result = psql(db_url, migration_paths[0].read_text(encoding="utf-8"), expect_failure=True)
        lock_seconds = time.monotonic() - lock_started
    finally:
        stop_holder(exclusive_holder)
    budgets = contract["budgets"]
    if not (budgets["lockFailureMinimumSeconds"] <= lock_seconds <= budgets["lockFailureMaximumSeconds"]):
        raise SystemExit(f"lock failure time outside budget: {lock_seconds:.3f}s")
    if "lock timeout" not in lock_result.stderr.lower():
        raise SystemExit("exclusive lock fault did not fail via lock_timeout")
    if stable_hash(json_query(db_url, CATALOG_INVARIANTS_SQL)) != catalog_before_hash:
        raise SystemExit("lock-timeout migration left catalog residue")
    if json_query(db_url, DATA_ORACLE_SQL) != data_before:
        raise SystemExit("lock-timeout migration changed row/PK/hash oracle")

    wal_start = scalar(db_url, "select pg_current_wal_lsn()::text;")
    read_holder = start_lock_holder(db_url, "access share", "issue-341-reader-holder")
    try:
        upgrade_started = time.monotonic()
        run(["supabase", "migration", "up", *target_args])
        upgrade_seconds = time.monotonic() - upgrade_started
    finally:
        stop_holder(read_holder)
    if upgrade_seconds > budgets["upgradeSeconds"]:
        raise SystemExit(f"upgrade budget exceeded: {upgrade_seconds:.3f}s")
    wal_end = scalar(db_url, "select pg_current_wal_lsn()::text;")
    wal_bytes = int(scalar(db_url, f"select pg_wal_lsn_diff('{wal_end}', '{wal_start}')::bigint;"))
    if wal_bytes > budgets["migrationWalBytes"]:
        raise SystemExit(f"migration WAL budget exceeded: {wal_bytes}")

    data_after = json_query(db_url, DATA_ORACLE_SQL)
    if data_after != data_before:
        raise SystemExit("successful base-to-head upgrade changed row/PK/hash oracle")
    catalog_after = json_query(db_url, CATALOG_INVARIANTS_SQL)
    if catalog_after != catalog_before:
        raise SystemExit("constraints/ACL/RLS/triggers/publication invariants drifted")

    role_matrix = json_query(db_url, ROLE_MATRIX_SQL)
    assert isinstance(role_matrix, dict)
    assert_all_true(role_matrix)
    missing_objects = [
        name for name in contract["expectedHeadObjects"]
        if scalar(db_url, f"select to_regclass('{name}') is not null;") != "t"
    ]
    if missing_objects:
        raise SystemExit(f"expected head objects missing: {missing_objects}")

    history = json_query(db_url, "select coalesce(jsonb_agg(version order by version), '[]')::text from supabase_migrations.schema_migrations where version > '" + base + "';")
    expected_history = [item["version"] for item in migration_manifest]
    if history != expected_history:
        raise SystemExit(f"migration history mismatch: expected={expected_history}, actual={history}")

    retry_wal_start = scalar(db_url, "select pg_current_wal_lsn()::text;")
    retry_started = time.monotonic()
    retry = run(["supabase", "migration", "up", *target_args])
    retry_seconds = time.monotonic() - retry_started
    retry_wal_end = scalar(db_url, "select pg_current_wal_lsn()::text;")
    retry_wal_bytes = int(scalar(db_url, f"select pg_wal_lsn_diff('{retry_wal_end}', '{retry_wal_start}')::bigint;"))
    no_pending_migrations = "up to date" in (retry.stdout + retry.stderr).lower()
    if not no_pending_migrations:
        raise SystemExit("idempotent migration retry did not report an up-to-date local database")
    if json_query(db_url, DATA_ORACLE_SQL) != data_before:
        raise SystemExit("idempotent migration retry changed row/PK/hash oracle")

    migration_list = run(["supabase", "migration", "list", *target_args]).stdout
    evidence = {
        "schemaVersion": "production-equivalent-upgrade-evidence.v1",
        "status": "passed",
        "scope": "isolated-local-supabase",
        "git": {
            "commit": run(["git", "rev-parse", "HEAD"]).stdout.strip(),
            "dirty": dirty,
        },
        "runtime": {
            "supabaseCli": run(["supabase", "--version"]).stdout.strip().splitlines()[0],
            "postgres": scalar(db_url, "show server_version;"),
        },
        "migration": {
            "baseVersion": base,
            "headVersion": head,
            "files": migration_manifest,
            "historyAfter": history,
            "listSha256": sha256_bytes(migration_list.encode()),
        },
        "fixture": {
            "representativeRows": rows,
            "seconds": round(fixture_seconds, 3),
            "surfaces": surfaces,
        },
        "oracles": {
            "relationCount": len(data_before),
            "beforeSha256": stable_hash(data_before),
            "afterSha256": stable_hash(data_after),
            "catalogInvariantSha256": catalog_before_hash,
            "catalogInvariantCategories": [
                "constraints", "relation ACL/RLS", "policies", "triggers", "publication membership"
            ],
        },
        "failureAtomicity": fault_results,
        "lockContention": {
            "exclusiveFailureSeconds": round(lock_seconds, 3),
            "compatibleReaderHeldDuringUpgrade": True,
        },
        "upgrade": {
            "seconds": round(upgrade_seconds, 3),
            "walBytes": wal_bytes,
            "recovery": "roll-forward-after-transactional-fault-and-lock-timeout",
        },
        "retry": {
            "seconds": round(retry_seconds, 3),
            "walBytes": retry_wal_bytes,
            "noPendingMigrations": no_pending_migrations,
        },
        "roleMatrix": role_matrix,
        "expectedHeadObjects": contract["expectedHeadObjects"],
        "budgets": budgets,
    }
    args.evidence_out.parent.mkdir(parents=True, exist_ok=True)
    args.evidence_out.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        f"PASS production-equivalent upgrade {base}->{head}; rows={rows}; "
        f"upgradeSeconds={upgrade_seconds:.3f}; migrationWalBytes={wal_bytes}; "
        f"evidence={args.evidence_out}", flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
