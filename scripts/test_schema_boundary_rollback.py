#!/usr/bin/env python3
"""Exercise Issue #354 rollback and roll-forward against a local head database."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "supabase/migrations/20260801042547_issue_354_schema_view_compat_expand.sql"
ROLLBACK = ROOT / "supabase/operator/issue_354_restore_schema_boundary.sql"


def run(command: list[str], *, sql: str | None = None) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command, cwd=ROOT, input=sql, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if result.returncode != 0:
        raise SystemExit(f"schema-boundary command failed ({result.returncode}): {' '.join(command[:1])}")
    return result


def psql(db_url: str, sql: str) -> str:
    return run(
        ["psql", db_url, "-XAt", "-v", "ON_ERROR_STOP=1"], sql=sql
    ).stdout.strip()


def database_url() -> str:
    if value := os.environ.get("DATABASE_URL"):
        return value
    command = ["supabase"]
    if workdir := os.environ.get("SUPABASE_WORKDIR"):
        command.extend(["--workdir", workdir])
    command.extend(["status", "--output", "json"])
    return json.loads(run(command).stdout)["DB_URL"]


def view_signature(db_url: str, schema: str) -> str:
    pairs = (
        ("worker_domain_traceability_cutoffs", "private"),
        ("worker_domain_traceability_violations", "util"),
        ("worker_job_domain_refs", "api"),
        ("worker_legacy_lifecycle_audit", "util"),
        ("worker_legacy_table_retirement_blockers", "util"),
    )
    values = ",".join(
        f"('{name}','{schema if schema != 'target' else target}')" for name, target in pairs
    )
    return psql(db_url, f"""
      with expected(name,schema_name) as (values {values})
      select string_agg(format('%s:%s', expected.name, class.oid), ',' order by expected.name)
      from expected
      join pg_namespace namespace on namespace.nspname=expected.schema_name
      join pg_class class on class.relnamespace=namespace.oid and class.relname=expected.name
      where class.relkind='v';
    """)


def service_role_maintain_count(db_url: str) -> str:
    return psql(db_url, """
      select count(*)
      from (values
        ('worker_domain_traceability_cutoffs'),('worker_domain_traceability_violations'),
        ('worker_job_domain_refs'),('worker_legacy_lifecycle_audit'),
        ('worker_legacy_table_retirement_blockers')
      ) expected(name)
      where has_table_privilege('service_role', format('public.%I', expected.name), 'MAINTAIN');
    """)


def rollback(db_url: str, maintain: str) -> None:
    run([
        "psql", db_url, "-X", "-v", "ON_ERROR_STOP=1",
        "-v", f"source_service_role_maintain={maintain}", "-f", str(ROLLBACK),
    ])


def assert_rollback_evidence_rejected(db_url: str, value: str | None) -> None:
    command = ["psql", db_url, "-X", "-v", "ON_ERROR_STOP=1"]
    if value is not None:
        command.extend(["-v", f"source_service_role_maintain={value}"])
    command.extend(["-f", str(ROLLBACK)])
    result = subprocess.run(
        command, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if result.returncode == 0:
        raise SystemExit(f"rollback accepted invalid evidence value: {value!r}")


def main() -> int:
    db_url = database_url()
    target_oids = view_signature(db_url, "target")
    if target_oids.count(":") != 5:
        raise SystemExit("Issue #354 target views are not at Expand head")

    assert_rollback_evidence_rejected(db_url, None)
    assert_rollback_evidence_rejected(db_url, "yes")
    if view_signature(db_url, "target") != target_oids:
        raise SystemExit("rejected rollback evidence changed the Expand target views")

    rollback(db_url, "false")
    try:
        public_oids = view_signature(db_url, "public")
        if public_oids != target_oids:
            raise SystemExit("rollback failed to preserve the five canonical view OIDs")
        residue = psql(db_url, """
          select count(*) from pg_class class join pg_namespace namespace on namespace.oid=class.relnamespace
          where class.relkind='v' and namespace.nspname in ('api','private','util')
            and class.relname in (
              'worker_domain_traceability_cutoffs','worker_domain_traceability_violations',
              'worker_job_domain_refs','worker_legacy_lifecycle_audit',
              'worker_legacy_table_retirement_blockers','schema_boundary_phase'
            );
        """)
        if residue != "0":
            raise SystemExit("rollback left Issue #354 target or phase-view residue")
        if service_role_maintain_count(db_url) != "0":
            raise SystemExit("false rollback evidence restored unexpected service_role MAINTAIN")
    finally:
        psql(db_url, MIGRATION.read_text(encoding="utf-8"))

    if view_signature(db_url, "target") != target_oids:
        raise SystemExit("roll-forward failed to preserve the five canonical view OIDs")

    rollback(db_url, "true")
    try:
        if view_signature(db_url, "public") != target_oids:
            raise SystemExit("MAINTAIN-variant rollback failed to preserve the five canonical view OIDs")
        if service_role_maintain_count(db_url) != "5":
            raise SystemExit("true rollback evidence did not restore service_role MAINTAIN")
    finally:
        psql(db_url, MIGRATION.read_text(encoding="utf-8"))

    if view_signature(db_url, "target") != target_oids:
        raise SystemExit("MAINTAIN-variant roll-forward failed to preserve the five canonical view OIDs")
    print("PASS Issue #354 rollback and roll-forward preserve OIDs for both proven ACL variants")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
