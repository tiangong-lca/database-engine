#!/usr/bin/env python3
"""Destructive loopback-only Phase B physical qualification for Issue #407.

The predecessor URL is a distinct read-only exact-head oracle. The candidate
URL must be a separately identified disposable local stack at Phase B head.
This probe rolls only the candidate backward and forward, creates randomized
LOGIN roles, loads 296k namespaced rows, and removes its roles/rows before
returning.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import hashlib
import json
import os
import re
import secrets
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import psycopg


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "supabase/migrations/20260804100000_issue_407_document_validation_evidence_physical_expand.sql"
ROLLBACK = ROOT / "supabase/operator/20260804_issue_407_document_validation_evidence_physical_expand_rollback.sql"
ROLLFORWARD = ROOT / "supabase/operator/20260804_issue_407_document_validation_evidence_physical_expand_rollforward.sql"
RUN_TOKEN = secrets.token_hex(8)
RUN_NAMESPACE = uuid.uuid4()
LOGIN = f"issue407_worker_{RUN_TOKEN}"
API_LOGIN = f"issue407_api_{RUN_TOKEN}"
PREFIX = f"issue-407-{RUN_TOKEN}-"
FIXTURE_VERSION = f"407.{RUN_TOKEN}"
CREATED_ROLES: set[str] = set()
FAULT_SEQUENCE = f"issue407_fault_seq_{RUN_TOKEN}"
FAULT_FUNCTION = f"issue407_fault_fn_{RUN_TOKEN}"
FAULT_TRIGGER = f"issue407_fault_trg_{RUN_TOKEN}"


def require_loopback(url: str) -> None:
    parsed = urlsplit(url)
    if parsed.scheme not in {"postgres", "postgresql"} or parsed.hostname not in {
        "127.0.0.1", "localhost", "::1",
    } or parsed.port is None:
        raise SystemExit("database URLs must be explicit loopback PostgreSQL targets")


def expected_migration_versions(head: str) -> tuple[str, ...]:
    versions = tuple(sorted(
        path.name.split("_", 1)[0]
        for path in (ROOT / "supabase/migrations").glob("*.sql")
        if path.name.split("_", 1)[0] <= head
    ))
    if not versions or versions[-1] != head:
        raise AssertionError(f"expected migration head is absent from checkout: {head}")
    return versions


def database_identity(url: str) -> tuple[str, tuple[str, ...]]:
    with psycopg.connect(url) as conn:
        system_identifier = scalar(
            conn, "select system_identifier::text from pg_control_system()"
        )
        versions = tuple(row[0] for row in conn.execute(
            "select version from supabase_migrations.schema_migrations order by version"
        ))
        return system_identifier, versions


def inspect_container(container: str) -> dict:
    return json.loads(subprocess.run(
        ["docker", "inspect", container], check=True, text=True,
        stdout=subprocess.PIPE,
    ).stdout)[0]


def assert_stack_binding(
    url: str, *, container: str, project_id: str, api_url: str | None = None
) -> None:
    if container != f"supabase_db_{project_id}":
        raise AssertionError("database container name is not derived from project identity")
    inspection = inspect_container(container)
    if inspection["State"].get("Running") is not True:
        raise AssertionError(f"database container is not running: {container}")
    labels = inspection["Config"].get("Labels") or {}
    if labels.get("com.supabase.cli.project") != project_id or labels.get(
        "com.docker.compose.project"
    ) != project_id:
        raise AssertionError(
            f"database project labels drifted: {labels}"
        )
    expected_port = str(urlsplit(url).port)
    bindings = inspection["NetworkSettings"]["Ports"].get("5432/tcp") or []
    if {row.get("HostPort") for row in bindings} != {expected_port}:
        raise AssertionError(
            f"database container port drifted: {bindings}; expected {expected_port}"
        )
    in_container_system_id = subprocess.run(
        ["docker", "exec", container, "psql", "-X", "-U", "postgres", "-d", "postgres",
         "-Atqc", "select system_identifier::text from pg_control_system()"],
        check=True, text=True, stdout=subprocess.PIPE,
    ).stdout.strip()
    if in_container_system_id != database_identity(url)[0]:
        raise AssertionError("network target and bound database container identities differ")

    if api_url is None:
        return
    parsed_api = urlsplit(api_url)
    if parsed_api.scheme != "http" or parsed_api.hostname not in {"127.0.0.1", "localhost"} \
            or parsed_api.port is None or parsed_api.path not in {"", "/"}:
        raise AssertionError("API URL must be an explicit loopback root URL")
    kong = inspect_container(f"supabase_kong_{project_id}")
    rest = inspect_container(f"supabase_rest_{project_id}")
    for component, row in (("kong", kong), ("rest", rest)):
        component_labels = row["Config"].get("Labels") or {}
        if row["State"].get("Running") is not True or component_labels.get(
            "com.supabase.cli.project"
        ) != project_id or component_labels.get("com.docker.compose.project") != project_id:
            raise AssertionError(f"{component} is not bound to candidate project {project_id}")
    api_bindings = kong["NetworkSettings"]["Ports"].get("8000/tcp") or []
    if {row.get("HostPort") for row in api_bindings} != {str(parsed_api.port)}:
        raise AssertionError("API URL port is not the candidate project's Kong binding")
    networks = [set(row["NetworkSettings"]["Networks"]) for row in (inspection, kong, rest)]
    if not set.intersection(*networks):
        raise AssertionError("candidate database, Kong, and PostgREST do not share a network")


def psql(url: str, path: Path, *, expect_success: bool = True) -> subprocess.CompletedProcess[str]:
    parsed = urlsplit(url)
    environment = os.environ.copy()
    if parsed.password is not None:
        environment["PGPASSWORD"] = unquote(parsed.password)
    result = subprocess.run(
        [
            "psql", "-X", "--host", parsed.hostname or "",
            "--port", str(parsed.port), "--username", unquote(parsed.username or "postgres"),
            "--dbname", unquote(parsed.path.removeprefix("/") or "postgres"),
            "-v", "ON_ERROR_STOP=1", "-f", str(path),
        ],
        cwd=ROOT, env=environment, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if expect_success and result.returncode:
        raise AssertionError(result.stdout + result.stderr)
    return result


def login_url(url: str, password: str, role: str = LOGIN) -> str:
    parsed = urlsplit(url)
    host = parsed.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    netloc = f"{quote(role)}:{quote(password)}@{host}:{parsed.port}"
    return urlunsplit((parsed.scheme, netloc, parsed.path, "", ""))


def scalar(conn: psycopg.Connection, sql: str, params: tuple = ()):
    return conn.execute(sql, params).fetchone()[0]


def fixture(index: int) -> dict:
    return {
        "datasetType": "Process",
        "datasetId": str(uuid.uuid5(RUN_NAMESPACE, str(index))),
        "datasetVersion": FIXTURE_VERSION,
        "canonicalContentHash": f"{PREFIX}content-{index}",
        "documentValidatorVersion": "issue-407-validator",
        "documentValidationProfile": "issue-407-profile",
        "validationReportSchemaVersion": "v1",
        "validatorEngineFingerprint": "issue-407-engine",
        "tidasSchemaLockSha256": "issue-407-schema",
        "status": "passed",
        "summary": {"issue": 407, "index": index},
        "issueArtifactRef": {"kind": "runtime"},
        "issueArtifactHash": f"{PREFIX}artifact-{index}",
    }


def capture_behavior(url: str) -> dict:
    with psycopg.connect(url) as oracle:
        before_oracle = oracle.execute("""
          select count(*)::bigint,
                 coalesce(md5(string_agg(md5(row_to_json(e)::text),'' order by e.id)),md5(''))
          from public.lcia_document_validation_evidence e
        """).fetchone()
    with psycopg.connect(url) as conn:
        conn.execute("begin")
        conn.execute("select set_config('request.jwt.claim.role','service_role',true)")
        conn.execute("select set_config('request.jwt.claims','{\"role\":\"service_role\"}',true)")
        blank = scalar(conn, "select public.svc_lcia_document_validation_evidence_lookup('[]'::jsonb)")
        record = scalar(
            conn,
            "select public.svc_lcia_document_validation_evidence_record(%s::jsonb,null)",
            (json.dumps([fixture(1)]),),
        )
        # Keep every non-UUID identity component equal to the row inserted in
        # this transaction.  The legacy predecessor may otherwise short-circuit
        # an unmatched/empty join before evaluating the UUID cast, making this
        # oracle depend on table statistics and the chosen query plan.
        invalid_key = {
            key: value for key, value in fixture(1).items() if key not in {
                "status", "summary", "issueArtifactRef", "issueArtifactHash"
            }
        }
        invalid_key["datasetId"] = "bad"
        invalid = scalar(
            conn,
            "select public.svc_lcia_document_validation_evidence_lookup(%s::jsonb)",
            (json.dumps([invalid_key]),),
        )
        populated = scalar(
            conn,
            "select public.svc_lcia_document_validation_evidence_lookup(%s::jsonb)",
            (json.dumps([{k: v for k, v in fixture(1).items() if k not in {
                "status", "summary", "issueArtifactRef", "issueArtifactHash"
            }}]),),
        )
        first = fixture(4)
        second = {**first, "status": "failed", "summary": {"winner": "second"}}
        first["summary"] = {"winner": "first"}
        scalar(
            conn,
            "select public.svc_lcia_document_validation_evidence_record(%s::jsonb,null)",
            (json.dumps([first, second]),),
        )
        forward_winner = conn.execute(
            "select status,summary from public.lcia_document_validation_evidence where dataset_id=%s and dataset_version=%s",
            (first["datasetId"], FIXTURE_VERSION),
        ).fetchone()
        reverse_first = fixture(5)
        reverse_second = {
            **reverse_first, "status": "failed", "summary": {"winner": "reverse-first"}
        }
        reverse_first["summary"] = {"winner": "reverse-second"}
        scalar(
            conn,
            "select public.svc_lcia_document_validation_evidence_record(%s::jsonb,null)",
            (json.dumps([reverse_second, reverse_first]),),
        )
        reverse_winner = conn.execute(
            "select status,summary from public.lcia_document_validation_evidence where dataset_id=%s and dataset_version=%s",
            (reverse_first["datasetId"], FIXTURE_VERSION),
        ).fetchone()
        mixed = [fixture(2), fixture(3)]
        mixed[1]["datasetId"] = "not-a-uuid"
        mixed_before = scalar(
            conn,
            "select count(*) from public.lcia_document_validation_evidence where dataset_version=%s",
            (FIXTURE_VERSION,),
        )
        mixed_response = scalar(
            conn,
            "select public.svc_lcia_document_validation_evidence_record(%s::jsonb,null)",
            (json.dumps(mixed),),
        )
        mixed_after = scalar(
            conn,
            "select count(*) from public.lcia_document_validation_evidence where dataset_version=%s",
            (FIXTURE_VERSION,),
        )
        conn.rollback()
    with psycopg.connect(url) as oracle:
        after_oracle = oracle.execute("""
          select count(*)::bigint,
                 coalesce(md5(string_agg(md5(row_to_json(e)::text),'' order by e.id)),md5(''))
          from public.lcia_document_validation_evidence e
        """).fetchone()
    if after_oracle != before_oracle:
        raise AssertionError(f"behavior oracle changed committed rows: {before_oracle} != {after_oracle}")
    return {
        "blank": blank, "invalid": invalid, "record": record,
        "populated": populated, "mixed": mixed_response,
        "mixedRows": mixed_after - mixed_before,
        "forwardDuplicateWinner": forward_winner,
        "reverseDuplicateWinner": reverse_winner,
    }


def migration_inner() -> str:
    sql = MIGRATION.read_text(encoding="utf-8")
    begin = re.search(r"(?im)^begin;\s*", sql)
    commit = list(re.finditer(r"(?im)^commit;\s*$", sql))
    if begin is None or not commit:
        raise AssertionError("Phase B migration transaction envelope is missing")
    return sql[begin.end():commit[-1].start()]


def rollback_inner() -> str:
    sql = ROLLBACK.read_text(encoding="utf-8")
    begin = re.search(r"(?im)^begin;\s*", sql)
    commit = list(re.finditer(r"(?im)^commit;\s*$", sql))
    if begin is None or not commit:
        raise AssertionError("Phase B rollback transaction envelope is missing")
    return sql[begin.end():commit[-1].start()]


def assert_preflight_negatives(url: str) -> None:
    bad_owner = f"issue407_bad_owner_{RUN_TOKEN}"
    cases = {
        "overload": "create function private.svc_lcia_document_validation_evidence_lookup(text) returns jsonb language sql as $$select '{}'::jsonb$$",
        "owner": f"create role {bad_owner}; alter function private.svc_lcia_document_validation_evidence_lookup(jsonb) owner to {bad_owner}",
        "search_path": "alter function private.svc_lcia_document_validation_evidence_lookup(jsonb) set search_path=public",
        "body": "create or replace function private.svc_lcia_document_validation_evidence_lookup(p_cache_keys jsonb) returns jsonb language plpgsql security definer set search_path=pg_catalog,pg_temp as $$begin return '{\"ok\":false}'::jsonb; end$$",
        "source_index": "drop index public.lcia_document_validation_evidence_lookup_idx",
        "source_default": "alter table public.lcia_document_validation_evidence alter column summary set default jsonb_build_object('drift',true)",
        "source_reloptions": "alter table public.lcia_document_validation_evidence set (fillfactor=80)",
        "source_attstattarget": "alter table public.lcia_document_validation_evidence alter column dataset_type set statistics 17",
        "source_all_tables_publication": "create publication issue407_all_tables for all tables",
        "source_rls": "alter table public.lcia_document_validation_evidence disable row level security",
        "source_acl": "grant update on public.lcia_document_validation_evidence to authenticated",
        "source_comment": "comment on table public.lcia_document_validation_evidence is 'drift'",
    }
    inner = migration_inner()
    for name, mutation in cases.items():
        with psycopg.connect(url) as conn:
            try:
                for statement in mutation.split("; "):
                    conn.execute(statement)
                conn.execute(inner, prepare=False)
            except psycopg.Error:
                conn.rollback()
            else:
                raise AssertionError(f"malicious {name} state passed preflight")
        with psycopg.connect(url) as check:
            count = scalar(check, """
              select count(*) from pg_proc p join pg_namespace n on n.oid=p.pronamespace
              where n.nspname='private' and p.proname like 'svc_lcia_document_validation_evidence_%%'
            """)
            topology = check.execute("""
              select to_regclass('public.lcia_document_validation_evidence') is not null,
                     to_regclass('private.lcia_document_validation_evidence') is null,
                     (select relkind from pg_class
                      where oid='public.lcia_document_validation_evidence'::regclass)
            """).fetchone()
            if count != 2 or topology != (True, True, "r"):
                raise AssertionError(
                    f"{name} executed physical DDL before failing: "
                    f"routines={count} topology={topology}"
                )


def assert_retry_negatives(url: str) -> None:
    cases = {
        "retry_body": """
          create or replace function private.svc_lcia_document_validation_evidence_lookup(p_cache_keys jsonb)
          returns jsonb language plpgsql security definer set search_path=pg_catalog,pg_temp
          as $$begin return '{"ok":false}'::jsonb; end$$
        """,
        "retry_acl": "grant execute on function private.svc_lcia_document_validation_evidence_lookup(jsonb) to authenticated",
    }
    inner = migration_inner()
    for name, mutation in cases.items():
        with psycopg.connect(url) as conn:
            try:
                conn.execute(mutation)
                conn.execute(inner, prepare=False)
            except psycopg.Error as error:
                if "private canonical routine definition or ACL drifted" not in str(error):
                    raise AssertionError(f"{name} failed for the wrong reason: {error}") from error
                conn.rollback()
            else:
                raise AssertionError(f"{name} was silently overwritten on retry")


def assert_rollback_negatives(url: str) -> None:
    cases = {
        "body": """
          create or replace function private.svc_lcia_document_validation_evidence_lookup(p_cache_keys jsonb)
          returns jsonb language sql security definer set search_path=pg_catalog,pg_temp
          as $$select '{"ok":false}'::jsonb$$
        """,
        "extra_overload": """
          create function public.svc_lcia_document_validation_evidence_lookup(text)
          returns jsonb language sql as $$select '{}'::jsonb$$
        """,
        "comment": """
          comment on function private.svc_lcia_document_validation_evidence_lookup(jsonb)
          is 'drift'
        """,
    }
    inner = rollback_inner()
    with psycopg.connect(url) as baseline_conn:
        baseline = catalog_snapshot(baseline_conn)
    for name, mutation in cases.items():
        with psycopg.connect(url) as conn:
            try:
                conn.execute(mutation)
                conn.execute(inner, prepare=False)
            except psycopg.Error:
                conn.rollback()
            else:
                raise AssertionError(f"malicious rollback {name} state passed preflight")
        with psycopg.connect(url) as check:
            if catalog_snapshot(check) != baseline:
                raise AssertionError(f"rollback negative {name} changed catalog")


def assert_private_role_runtime(url: str) -> None:
    password = secrets.token_urlsafe(32)  # gitleaks:allow -- generated ephemeral secret
    with psycopg.connect(url, autocommit=True) as admin:
        if scalar(admin, "select count(*) from pg_roles where rolname=%s", (LOGIN,)):
            raise AssertionError(f"random role collision: {LOGIN}")
        admin.execute(
            f"create role {LOGIN} login password '{password}' "
            "nosuperuser nocreatedb nocreaterole nobypassrls inherit"
        )
        CREATED_ROLES.add(LOGIN)
        admin.execute(
            f"grant lca_worker_runtime to {LOGIN} "
            "with inherit true, set false, admin false"
        )
        role_state = admin.execute("""
          select role_row.rolreplication,role_row.rolconfig,
                 grantor.rolname,membership.admin_option,
                 membership.inherit_option,membership.set_option
          from pg_roles role_row
          join pg_auth_members membership on membership.member=role_row.oid
          join pg_roles grantor on grantor.oid=membership.grantor
          where role_row.rolname=%s
            and membership.roleid='lca_worker_runtime'::regrole
        """, (LOGIN,)).fetchone()
        if role_state != (False, None, "postgres", False, True, False):
            raise AssertionError(f"runtime LOGIN edge drifted: {role_state}")
    try:
        with psycopg.connect(login_url(url, password)) as worker:
            identity = worker.execute(
                "select session_user,current_user,pg_has_role(session_user,'lca_worker_runtime','member')"
            ).fetchone()
            if identity != (LOGIN, LOGIN, True):
                raise AssertionError(f"unexpected LOGIN identity: {identity}")
            result = scalar(
                worker,
                "select private.svc_lcia_document_validation_evidence_lookup('[]'::jsonb)",
            )
            if result != {"ok": True, "data": []}:
                raise AssertionError(f"private LOGIN response drifted: {result}")
            recorded = scalar(
                worker,
                "select private.svc_lcia_document_validation_evidence_record(%s::jsonb,null)",
                (json.dumps([fixture(900)]),),
            )
            if recorded != {"ok": True, "data": {"insertedCount": 1}}:
                raise AssertionError(f"private LOGIN record drifted: {recorded}")
            try:
                worker.execute("set role lca_worker_runtime")
            except psycopg.errors.InsufficientPrivilege:
                worker.rollback()
            else:
                raise AssertionError("dedicated LOGIN unexpectedly acquired SET ROLE")

        for role in ("anon", "authenticated", "service_role", "api_internal_executor"):
            negative_login = f"issue407_neg_{role[:12]}_{RUN_TOKEN}"
            negative_password = secrets.token_urlsafe(32)  # gitleaks:allow -- generated ephemeral secret
            with psycopg.connect(url, autocommit=True) as admin:
                if scalar(admin, "select count(*) from pg_roles where rolname=%s", (negative_login,)):
                    raise AssertionError(f"random role collision: {negative_login}")
                admin.execute(
                    f"create role {negative_login} login password '{negative_password}' "
                    "nosuperuser nocreatedb nocreaterole nobypassrls inherit"
                )
                CREATED_ROLES.add(negative_login)
                admin.execute(
                    f"grant {role} to {negative_login} "
                    "with inherit true, set false, admin false"
                )
            try:
                with psycopg.connect(login_url(url, negative_password, negative_login)) as conn:
                    try:
                        conn.execute("select private.svc_lcia_document_validation_evidence_lookup('[]'::jsonb)")
                    except psycopg.errors.InsufficientPrivilege:
                        conn.rollback()
                    else:
                        raise AssertionError(f"{role} executed the private lookup")
            finally:
                with psycopg.connect(url, autocommit=True) as admin:
                    admin.execute(f"revoke {role} from {negative_login}")
                    admin.execute(f"drop role {negative_login}")
                    CREATED_ROLES.discard(negative_login)

        # A retry must continue to accept one safe real LOGIN membership edge.
        psql(url, MIGRATION)
        with psycopg.connect(url, autocommit=True) as admin:
            admin.execute(
                f"grant service_role to {LOGIN} "
                "with inherit true, set false, admin false"
            )
        # The planned unified restricted Worker transport may inherit both the
        # service transport role and lca_worker_runtime, but neither via SET.
        psql(url, MIGRATION)
        with psycopg.connect(login_url(url, password)) as unified:
            identity = unified.execute(
                "select session_user,current_user,"
                "pg_has_role(session_user,'lca_worker_runtime','member'),"
                "pg_has_role(session_user,'service_role','member')"
            ).fetchone()
            if identity != (LOGIN, LOGIN, True, True):
                raise AssertionError(f"unified LOGIN identity drifted: {identity}")
            try:
                unified.execute("set role service_role")
            except psycopg.errors.InsufficientPrivilege:
                unified.rollback()
            else:
                raise AssertionError("unified LOGIN unexpectedly acquired service SET ROLE")
    finally:
        with psycopg.connect(url, autocommit=True) as admin:
            admin.execute(f"revoke service_role from {LOGIN}")
            admin.execute(f"revoke lca_worker_runtime from {LOGIN}")
            admin.execute(f"drop role {LOGIN}")
            CREATED_ROLES.discard(LOGIN)


def assert_api_executor_public_compat(url: str) -> None:
    password = secrets.token_urlsafe(32)  # gitleaks:allow -- generated ephemeral secret
    with psycopg.connect(url, autocommit=True) as admin:
        if scalar(admin, "select count(*) from pg_roles where rolname=%s", (API_LOGIN,)):
            raise AssertionError(f"random role collision: {API_LOGIN}")
        admin.execute(
            f"create role {API_LOGIN} login password '{password}' "
            "nosuperuser nocreatedb nocreaterole nobypassrls inherit"
        )
        CREATED_ROLES.add(API_LOGIN)
        admin.execute(
            f"grant api_internal_executor to {API_LOGIN} "
            "with inherit true, set false, admin false"
        )
    try:
        parsed = urlsplit(url)
        host = parsed.hostname or ""
        if ":" in host:
            host = f"[{host}]"
        probe_url = urlunsplit((
            parsed.scheme,
            f"{quote(API_LOGIN)}:{quote(password)}@{host}:{parsed.port}",
            parsed.path, "", "",
        ))
        with psycopg.connect(probe_url) as probe:
            probe.execute("select set_config('request.jwt.claim.role','service_role',true)")
            probe.execute("select set_config('request.jwt.claims','{\"role\":\"service_role\"}',true)")
            response = scalar(
                probe,
                "select public.svc_lcia_document_validation_evidence_lookup('[]'::jsonb)",
            )
            if response != {"ok": True, "data": []}:
                raise AssertionError(f"API executor public compatibility drifted: {response}")
    finally:
        with psycopg.connect(url, autocommit=True) as admin:
            admin.execute(f"revoke api_internal_executor from {API_LOGIN}")
            admin.execute(f"drop role {API_LOGIN}")
            CREATED_ROLES.discard(API_LOGIN)


def assert_role_matrix(url: str) -> None:
    expected = {
        "postgres": (True, True, True, True, True),
        "anon": (False, False, False, False, False),
        "authenticated": (False, False, False, False, False),
        "service_role": (True, False, False, True, True),
        "api_internal_executor": (True, False, False, True, True),
        "lca_worker_runtime": (True, False, True, False, False),
    }
    with psycopg.connect(url) as conn:
        for role, wanted in expected.items():
            observed = conn.execute("""
              select has_schema_privilege(%s,'private','USAGE'),
                     has_schema_privilege(%s,'private','CREATE'),
                     has_function_privilege(%s,
                       'private.svc_lcia_document_validation_evidence_lookup(jsonb)','EXECUTE')
                       and has_function_privilege(%s,
                       'private.svc_lcia_document_validation_evidence_record(jsonb,uuid)','EXECUTE'),
                     has_function_privilege(%s,
                       'public.svc_lcia_document_validation_evidence_lookup(jsonb)','EXECUTE')
                       and has_function_privilege(%s,
                       'public.svc_lcia_document_validation_evidence_record(jsonb,uuid)','EXECUTE'),
                     has_table_privilege(%s,'public.lcia_document_validation_evidence','SELECT')
                       or has_table_privilege(%s,'public.lcia_document_validation_evidence','INSERT')
                       or has_table_privilege(%s,'public.lcia_document_validation_evidence','UPDATE')
                       or has_table_privilege(%s,'public.lcia_document_validation_evidence','DELETE')
                       or has_table_privilege(%s,'public.lcia_document_validation_evidence','TRUNCATE')
                       or has_table_privilege(%s,'public.lcia_document_validation_evidence','REFERENCES')
                       or has_table_privilege(%s,'public.lcia_document_validation_evidence','TRIGGER')
            """, (role, role, role, role, role, role, role, role, role, role, role, role, role)).fetchone()
            if observed != wanted:
                raise AssertionError(f"role matrix drifted for {role}: {observed} != {wanted}")


def call_record(url: str, records: list[dict]) -> int:
    with psycopg.connect(url) as conn:
        conn.execute("set local statement_timeout='10s'")
        response = scalar(
            conn,
            "select private.svc_lcia_document_validation_evidence_record(%s::jsonb,null)",
            (json.dumps(records),),
        )
        return int(response["data"]["insertedCount"])


def assert_concurrency(url: str) -> dict:
    with psycopg.connect(url) as conn:
        conn.execute(
            "delete from public.lcia_document_validation_evidence where dataset_version=%s",
            (FIXTURE_VERSION,),
        )
    password = secrets.token_urlsafe(32)  # gitleaks:allow -- generated ephemeral secret
    with psycopg.connect(url, autocommit=True) as admin:
        if scalar(admin, "select count(*) from pg_roles where rolname=%s", (LOGIN,)):
            raise AssertionError(f"random role collision: {LOGIN}")
        admin.execute(
            f"create role {LOGIN} login password '{password}' "
            "nosuperuser nocreatedb nocreaterole nobypassrls inherit"
        )
        CREATED_ROLES.add(LOGIN)
        admin.execute(
            f"grant lca_worker_runtime to {LOGIN} "
            "with inherit true, set false, admin false"
        )
    runtime_url = login_url(url, password)
    try:
        one = fixture(1000)
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            counts = list(pool.map(lambda _: call_record(runtime_url, [one]), range(20)))
        if sum(counts) != 1:
            raise AssertionError(f"same-key concurrency inserted {sum(counts)} rows")

        universe = [fixture(1100 + item) for item in range(60)]
        batches = []
        for item in range(20):
            batch = universe[item:item + 40]
            batches.append(batch if item % 2 == 0 else list(reversed(batch)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            counts = list(pool.map(lambda records: call_record(runtime_url, records), batches))
        if sum(counts) != 59:
            raise AssertionError(f"reverse/overlap concurrency inserted {sum(counts)} rows")
    finally:
        with psycopg.connect(url, autocommit=True) as admin:
            admin.execute(f"revoke lca_worker_runtime from {LOGIN}")
            admin.execute(f"drop role {LOGIN}")
            CREATED_ROLES.discard(LOGIN)
    return {"connections": 20, "sameKeyInserted": 1, "overlapInserted": 59}


def table_hash(conn: psycopg.Connection) -> tuple[int, str]:
    return conn.execute("""
      select count(*)::integer,
             md5(string_agg(md5(row_to_json(evidence)::text), '' order by evidence.id))
      from public.lcia_document_validation_evidence evidence
      where evidence.dataset_version=%s
    """, (FIXTURE_VERSION,)).fetchone()


def catalog_snapshot(conn: psycopg.Connection) -> dict:
    relation_oid = scalar(
        conn, "select 'public.lcia_document_validation_evidence'::regclass::oid"
    )
    public_oids = conn.execute("""
      select p.proname,p.oid
      from pg_proc p join pg_namespace n on n.oid=p.pronamespace
      where n.nspname='public' and p.proname in (
        'svc_lcia_document_validation_evidence_lookup',
        'svc_lcia_document_validation_evidence_record'
      ) order by p.proname
    """).fetchall()
    rows = conn.execute("""
      select category, value from (
        select 'relation' category, concat_ws('|',c.relowner::regrole,c.relkind,
          c.relpersistence,c.relrowsecurity,c.relforcerowsecurity,c.relreplident,
          coalesce(am.amname,''),coalesce(ts.spcname,''),coalesce(c.reloptions::text,''),
          c.relispartition,coalesce(pg_get_expr(c.relpartbound,c.oid),''),
          coalesce(c.relacl::text,''),coalesce(obj_description(c.oid,'pg_class'),'')) value
        from pg_class c left join pg_am am on am.oid=c.relam
          left join pg_tablespace ts on ts.oid=c.reltablespace
        where c.oid='public.lcia_document_validation_evidence'::regclass
        union all
        select 'column', concat_ws('|',a.attnum,a.attname,format_type(a.atttypid,a.atttypmod),
          a.attnotnull,a.attidentity,a.attgenerated,a.attstorage,a.attcompression,
          a.attstattarget,coalesce(a.attoptions::text,''),a.atthasmissing,
          coalesce(a.attmissingval::text,''),coalesce(coll.collname,''),
          coalesce(pg_get_expr(d.adbin,d.adrelid),''),coalesce(a.attacl::text,''),
          coalesce(col_description(a.attrelid,a.attnum),''))
        from pg_attribute a left join pg_attrdef d on d.adrelid=a.attrelid and d.adnum=a.attnum
          left join pg_collation coll on coll.oid=a.attcollation
        where a.attrelid='public.lcia_document_validation_evidence'::regclass
          and a.attnum>0 and not a.attisdropped
        union all
        select 'constraint', concat_ws('|',conname,contype,condeferrable,condeferred,
          convalidated,coalesce(conkey::text,''),coalesce(confrelid::regclass::text,''),
          coalesce(confkey::text,''),pg_get_constraintdef(oid,true))
        from pg_constraint where conrelid='public.lcia_document_validation_evidence'::regclass
        union all
        select 'index', concat_ws('|',indexrelid::regclass,indisunique,indisprimary,
          indisvalid,indisready,coalesce(indkey::text,''),
          coalesce(pg_get_expr(indexprs,indrelid),''),coalesce(pg_get_expr(indpred,indrelid),''),
          pg_get_indexdef(indexrelid))
        from pg_index where indrelid='public.lcia_document_validation_evidence'::regclass
        union all
        select 'trigger', concat_ws('|',case when t.tgisinternal then coalesce(c.conname,'<internal>')
          else t.tgname end,t.tgenabled,t.tgisinternal,t.tgtype,t.tgfoid::regprocedure,
          case when t.tgisinternal then '' else pg_get_triggerdef(t.oid,true) end)
        from pg_trigger t left join pg_constraint c on c.oid=t.tgconstraint
        where t.tgrelid='public.lcia_document_validation_evidence'::regclass
        union all
        select 'policy', concat_ws('|',polname,polcmd,polpermissive,polroles::text,
          coalesce(pg_get_expr(polqual,polrelid),''),coalesce(pg_get_expr(polwithcheck,polrelid),''))
        from pg_policy where polrelid='public.lcia_document_validation_evidence'::regclass
        union all
        select 'publication', concat_ws('|',p.pubname,p.puballtables,p.pubinsert,p.pubupdate,
          p.pubdelete,p.pubtruncate,p.pubviaroot,
          coalesce(pg_get_expr(pr.prqual,pr.prrelid),''),
          coalesce(pr.prattrs::text,''))
        from pg_publication p left join pg_publication_rel pr
          on p.oid=pr.prpubid
         and pr.prrelid='public.lcia_document_validation_evidence'::regclass
        where p.puballtables or pr.prrelid is not null
        union all
        select 'dependency', concat_ws('|',d.deptype,
          regexp_replace(pg_describe_object(d.classid,d.objid,d.objsubid),'pg_toast_[0-9]+','pg_toast_<oid>','g'),
          regexp_replace(pg_describe_object(d.refclassid,d.refobjid,d.refobjsubid),'pg_toast_[0-9]+','pg_toast_<oid>','g'))
        from pg_depend d where
          (d.classid='pg_class'::regclass and d.objid='public.lcia_document_validation_evidence'::regclass)
          or (d.refclassid='pg_class'::regclass and d.refobjid='public.lcia_document_validation_evidence'::regclass)
      ) catalog order by category,value
    """).fetchall()
    digest = hashlib.sha256(
        json.dumps(rows, separators=(",", ":"), default=str).encode()
    ).hexdigest()
    return {
        "relationOid": relation_oid,
        "publicFunctionOids": public_oids,
        "catalogSha256": digest,
    }


def public_function_catalog(conn: psycopg.Connection, *, include_oid: bool) -> list:
    rows = conn.execute("""
      select p.proname,pg_get_function_identity_arguments(p.oid),
             case when %s then p.oid else null end,
             p.proowner::regrole::text,l.lanname,p.prokind,
             pg_get_function_result(p.oid),pg_get_function_arguments(p.oid),
             p.prosecdef,p.provolatile,p.proparallel,p.proisstrict,p.proleakproof,
             p.proconfig,p.proacl::text,md5(p.prosrc),obj_description(p.oid,'pg_proc')
      from pg_proc p join pg_namespace n on n.oid=p.pronamespace
      join pg_language l on l.oid=p.prolang
      where n.nspname='public' and p.proname in (
        'svc_lcia_document_validation_evidence_lookup',
        'svc_lcia_document_validation_evidence_record'
      ) order by p.proname,pg_get_function_identity_arguments(p.oid)
    """, (include_oid,)).fetchall()
    return [list(row) for row in rows]


def assert_fault_and_lock_atomicity(url: str) -> dict:
    normal, fault = sorted(
        (fixture(2099), fixture(2100)),
        key=lambda item: (
            item["datasetType"], item["datasetId"], item["datasetVersion"],
            item["canonicalContentHash"], item["documentValidatorVersion"],
            item["documentValidationProfile"], item["validationReportSchemaVersion"],
            item["validatorEngineFingerprint"], item["tidasSchemaLockSha256"],
        ),
    )
    sequence_created = False
    function_created = False
    trigger_created = False
    try:
        # Cleanup protection begins before the first mutation.
        with psycopg.connect(url, autocommit=True) as admin:
            admin.execute(f"create sequence public.{FAULT_SEQUENCE}")
            sequence_created = True
            admin.execute(f"""
              create function public.{FAULT_FUNCTION}()
              returns trigger language plpgsql as $function$
              begin
                perform nextval('public.{FAULT_SEQUENCE}');
                if new.canonical_content_hash = '{fault["canonicalContentHash"]}' then
                  raise exception 'issue407_commit_before_fault';
                end if;
                return new;
              end
              $function$
            """)
            function_created = True
            admin.execute(f"""
              create trigger {FAULT_TRIGGER}
              before insert on private.lcia_document_validation_evidence
              for each row execute function public.{FAULT_FUNCTION}()
            """)
            trigger_created = True
        with psycopg.connect(url) as conn:
            conn.execute("set local statement_timeout='5s'")
            try:
                conn.execute(
                    "select private.svc_lcia_document_validation_evidence_record(%s::jsonb,null)",
                    (json.dumps([normal, fault]),),
                )
            except psycopg.errors.RaiseException:
                conn.rollback()
            else:
                raise AssertionError("commit-before fault did not abort the batch")
        with psycopg.connect(url) as check:
            rows = scalar(
                check,
                "select count(*) from public.lcia_document_validation_evidence where dataset_id in (%s,%s)",
                (normal["datasetId"], fault["datasetId"]),
            )
            if rows:
                raise AssertionError("commit-before fault left a partial batch")
            if scalar(check, f"select last_value from public.{FAULT_SEQUENCE}") != 2:
                raise AssertionError("fault did not occur after one successful row attempt")
    finally:
        with psycopg.connect(url, autocommit=True) as admin:
            if trigger_created:
                admin.execute(
                    f"drop trigger {FAULT_TRIGGER} "
                    "on private.lcia_document_validation_evidence"
                )
            if function_created:
                admin.execute(f"drop function public.{FAULT_FUNCTION}()")
            if sequence_created:
                admin.execute(f"drop sequence public.{FAULT_SEQUENCE}")

    locked = fixture(2200)
    holder = psycopg.connect(url)
    waiter = psycopg.connect(url)
    try:
        holder.execute(
            "select private.svc_lcia_document_validation_evidence_record(%s::jsonb,null)",
            (json.dumps([locked]),),
        )
        waiter.execute("set local statement_timeout='1500ms'")
        try:
            waiter.execute(
                "select private.svc_lcia_document_validation_evidence_record(%s::jsonb,null)",
                (json.dumps([locked]),),
            )
        except psycopg.errors.QueryCanceled:
            waiter.rollback()
        else:
            raise AssertionError("conflicting insert exceeded the bounded lock contract")
        holder.rollback()
    finally:
        holder.close()
        waiter.close()
    with psycopg.connect(url) as check:
        if scalar(
            check,
            "select count(*) from public.lcia_document_validation_evidence where dataset_id=%s",
            (locked["datasetId"],),
        ):
            raise AssertionError("conflict timeout left a committed row")
    return {"faultStep": 2, "lockTimeoutMs": 1500, "partialRows": 0}


def assert_volume_retry_rollback(
    url: str, predecessor_public: list, predecessor_catalog_sha: str
) -> dict:
    with psycopg.connect(url) as conn:
        conn.execute(
            "delete from public.lcia_document_validation_evidence where dataset_version=%s",
            (FIXTURE_VERSION,),
        )
        conn.execute("""
          insert into public.lcia_document_validation_evidence (
            dataset_type,dataset_id,dataset_version,canonical_content_hash,
            document_validator_version,document_validation_profile,
            validation_report_schema_version,validator_engine_fingerprint,
            tidas_schema_lock_sha256,status,summary,issue_artifact_ref
          )
          select 'Process', md5(%s||item::text)::uuid, %s, md5(%s||item::text),
                 'validator-v1', 'profile-v1', 'report-v1', 'engine-v1',
                 'schema-v1', 'passed', jsonb_build_object('nested',jsonb_build_object('item',item)),
                 jsonb_build_object('kind','volume')
          from generate_series(1,296000) item
        """, (PREFIX, FIXTURE_VERSION, PREFIX + "volume-content-"))
        before = table_hash(conn)
        if before[0] != 296000:
            raise AssertionError(f"volume fixture has {before[0]} rows")
        plan = scalar(conn, """
          explain (analyze, buffers, format json)
          select * from public.lcia_document_validation_evidence
          where dataset_type='Process' and dataset_id=md5(%s||'150000')::uuid
            and dataset_version=%s
            and canonical_content_hash=md5(%s||'150000')
            and document_validator_version='validator-v1'
            and document_validation_profile='profile-v1'
            and validation_report_schema_version='report-v1'
            and validator_engine_fingerprint='engine-v1'
            and tidas_schema_lock_sha256='schema-v1'
        """, (PREFIX, FIXTURE_VERSION, PREFIX + "volume-content-"))
        if "Index" not in json.dumps(plan):
            raise AssertionError(f"296k lookup did not use an index: {plan}")
        for batch_size in (0, 1, 50, 500, 1500):
            returned = scalar(conn, """
              with selected as (
                select jsonb_build_object(
                  'datasetType',dataset_type,'datasetId',dataset_id,
                  'datasetVersion',dataset_version,
                  'canonicalContentHash',canonical_content_hash,
                  'documentValidatorVersion',document_validator_version,
                  'documentValidationProfile',document_validation_profile,
                  'validationReportSchemaVersion',validation_report_schema_version,
                  'validatorEngineFingerprint',validator_engine_fingerprint,
                  'tidasSchemaLockSha256',tidas_schema_lock_sha256
                ) key
                from public.lcia_document_validation_evidence
                order by dataset_id limit %s
              )
              select jsonb_array_length(
                private.svc_lcia_document_validation_evidence_lookup(
                  coalesce(jsonb_agg(key),'[]'::jsonb)
                )->'data'
              ) from selected
            """, (batch_size,))
            if returned != batch_size:
                raise AssertionError(f"lookup batch {batch_size} returned {returned}")
        conn.commit()

    with psycopg.connect(url) as conn:
        baseline_catalog = catalog_snapshot(conn)
        baseline_functions = public_function_catalog(conn, include_oid=True)
        wal_before = scalar(conn, "select pg_current_wal_lsn()")
    psql(url, MIGRATION)
    with psycopg.connect(url) as conn:
        wal_bytes = scalar(conn, "select pg_wal_lsn_diff(pg_current_wal_lsn(),%s::pg_lsn)", (str(wal_before),))
        after_retry = table_hash(conn)
        retry_catalog = catalog_snapshot(conn)
        retry_functions = public_function_catalog(conn, include_oid=True)
    if after_retry != before or retry_catalog != baseline_catalog or retry_functions != baseline_functions or wal_bytes > 4 * 1024 * 1024:
        raise AssertionError(f"retry drift/WAL budget failed: before={before} after={after_retry} wal={wal_bytes}")

    psql(url, ROLLBACK)
    with psycopg.connect(url) as conn:
        after_rollback = table_hash(conn)
        private_count = scalar(conn, """
          select count(*) from pg_proc p join pg_namespace n on n.oid=p.pronamespace
          where n.nspname='private' and p.proname like 'svc_lcia_document_validation_evidence_%%'
        """)
        rollback_catalog = catalog_snapshot(conn)
        rollback_public = public_function_catalog(conn, include_oid=False)
    if (
        after_rollback != before
        or private_count != 2
        or rollback_catalog["catalogSha256"] != predecessor_catalog_sha
        or rollback_public != predecessor_public
    ):
        raise AssertionError("operator rollback changed rows or retained private routines")
    psql(url, ROLLFORWARD)
    with psycopg.connect(url) as conn:
        rollforward_catalog = catalog_snapshot(conn)
        rollforward_functions = public_function_catalog(conn, include_oid=True)
        if (
            table_hash(conn) != before
            or rollforward_catalog["catalogSha256"] != baseline_catalog["catalogSha256"]
            or rollforward_functions != baseline_functions
        ):
            raise AssertionError("roll-forward changed the 296k fixture hash")
    return {
        "rows": before[0], "fixtureHash": before[1],
        "retryWalBytes": int(wal_bytes), "lookupBatches": [0, 1, 50, 500, 1500],
    }


def assert_telemetry(url: str, container: str) -> None:
    since = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with psycopg.connect(url) as conn:
        conn.execute("select set_config('request.jwt.claim.role','service_role',true)")
        conn.execute("select set_config('request.jwt.claims','{\"role\":\"service_role\"}',true)")
        conn.execute("select public.svc_lcia_document_validation_evidence_lookup('[]'::jsonb)")
        conn.execute("select public.svc_lcia_document_validation_evidence_record('[]'::jsonb,null)")
        conn.commit()
    time.sleep(0.2)
    logs = subprocess.run(
        ["docker", "logs", "--since", since, container],
        text=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    ).stdout
    lookup = [line for line in logs.splitlines() if "issue_407_public_compat function=lookup" in line]
    record = [line for line in logs.splitlines() if "issue_407_public_compat function=record" in line]
    if len(lookup) != 1 or len(record) != 1:
        raise AssertionError(f"telemetry count mismatch: lookup={len(lookup)} record={len(record)}")
    if PREFIX in "\n".join(lookup + record):
        raise AssertionError("telemetry leaked fixture identity")
    sensitive = re.compile(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|"
        r"eyJ|authorization|request\.headers|p_cache_keys|p_records|datasetId|canonicalContentHash",
        re.IGNORECASE,
    )
    if sensitive.search("\n".join(lookup + record)):
        raise AssertionError("telemetry contains a sensitive identifier or parameter")


def postgrest(anon_key: str, service_key: str, api_url: str) -> None:
    base = api_url.rstrip("/") + "/rest/v1/rpc/"
    cases = [
        ("svc_lcia_document_validation_evidence_lookup", b'{"p_cache_keys":[]}', {"apikey": anon_key, "Authorization": f"Bearer {anon_key}", "Content-Type": "application/json", "Accept-Profile": "public", "Content-Profile": "public"}, {401, 403}, None),
        ("svc_lcia_document_validation_evidence_lookup", b'{"p_cache_keys":[]}', {"apikey": service_key, "Authorization": f"Bearer {service_key}", "Content-Type": "application/json", "Accept-Profile": "private", "Content-Profile": "private"}, {404, 406}, None),
        ("svc_lcia_document_validation_evidence_lookup", b'{"p_cache_keys":[]}', {"apikey": service_key, "Authorization": f"Bearer {service_key}", "Content-Type": "application/json", "Accept-Profile": "public", "Content-Profile": "public"}, {200}, {"ok": True, "data": []}),
        ("svc_lcia_document_validation_evidence_record", b'{"p_records":[],"p_source_worker_job_id":null}', {"apikey": service_key, "Authorization": f"Bearer {service_key}", "Content-Type": "application/json", "Accept-Profile": "public", "Content-Profile": "public"}, {200}, {"ok": True, "data": {"insertedCount": 0}}),
    ]
    for function, payload, headers, expected, expected_body in cases:
        request = urllib.request.Request(base + function, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                status = response.status
                body = response.read()
        except urllib.error.HTTPError as error:
            status = error.code
            body = error.read()
        if status not in expected:
            raise AssertionError(f"unexpected PostgREST status for {function}: {status}; expected {expected}")
        if expected_body is not None:
            try:
                actual_body = json.loads(body)
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                raise AssertionError(
                    f"PostgREST returned non-JSON success for {function}"
                ) from error
            if actual_body != expected_body:
                raise AssertionError(
                    f"unexpected PostgREST success body for {function}: "
                    f"{actual_body!r}; expected {expected_body!r}"
                )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--predecessor-db-url", required=True)
    parser.add_argument("--candidate-db-url", required=True)
    parser.add_argument("--expected-predecessor-system-id", required=True)
    parser.add_argument("--expected-candidate-system-id", required=True)
    parser.add_argument("--expected-predecessor-project-id", required=True)
    parser.add_argument("--expected-predecessor-container", required=True)
    parser.add_argument("--expected-candidate-project-id", required=True)
    parser.add_argument("--expected-candidate-container", required=True)
    parser.add_argument(
        "--execution-mode", choices=("ci-hard-bound", "local-explicit-isolated"),
        required=True,
    )
    parser.add_argument("--expected-predecessor-head", default="20260803163000")
    parser.add_argument("--expected-candidate-head", default="20260804100000")
    parser.add_argument("--api-url")
    parser.add_argument("--confirm-isolated-destructive-test", action="store_true")
    args = parser.parse_args()
    require_loopback(args.predecessor_db_url)
    require_loopback(args.candidate_db_url)
    if not args.confirm_isolated_destructive_test:
        raise SystemExit("--confirm-isolated-destructive-test is required")
    anon_key = os.environ.get("ISSUE407_ANON_KEY")
    service_key = os.environ.get("ISSUE407_SERVICE_KEY")
    if len([value for value in (args.api_url, anon_key, service_key) if value]) not in (0, 3):
        raise SystemExit(
            "--api-url, ISSUE407_ANON_KEY, and ISSUE407_SERVICE_KEY must be provided together"
        )

    if args.execution_mode == "ci-hard-bound":
        if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("CI") != "true":
            raise SystemExit("ci-hard-bound mode requires the hosted CI environment")
        if (
            args.expected_predecessor_project_id != "database-engine-407-predecessor"
            or args.expected_candidate_project_id != "database-engine"
            or urlsplit(args.predecessor_db_url).port != 62322
            or urlsplit(args.candidate_db_url).port != 55322
            or not args.api_url or urlsplit(args.api_url).port != 55321
        ):
            raise SystemExit("ci-hard-bound stack identities or ports drifted")
    elif not args.expected_candidate_project_id.startswith("database-engine-407-"):
        raise SystemExit(
            "local destructive candidate project must use database-engine-407-* isolation"
        )

    predecessor_identity = database_identity(args.predecessor_db_url)
    candidate_identity = database_identity(args.candidate_db_url)
    expected_predecessor_versions = expected_migration_versions(
        args.expected_predecessor_head
    )
    expected_candidate_versions = expected_migration_versions(args.expected_candidate_head)
    if predecessor_identity != (
        args.expected_predecessor_system_id, expected_predecessor_versions
    ):
        raise SystemExit(
            f"predecessor identity mismatch: {predecessor_identity}"
        )
    if candidate_identity != (
        args.expected_candidate_system_id, expected_candidate_versions
    ):
        raise SystemExit(f"candidate identity mismatch: {candidate_identity}")
    if predecessor_identity[0] == candidate_identity[0]:
        raise SystemExit("predecessor and candidate must be distinct PostgreSQL clusters")
    assert_stack_binding(
        args.predecessor_db_url,
        container=args.expected_predecessor_container,
        project_id=args.expected_predecessor_project_id,
    )
    assert_stack_binding(
        args.candidate_db_url,
        container=args.expected_candidate_container,
        project_id=args.expected_candidate_project_id,
        api_url=args.api_url,
    )

    try:
        # The predecessor oracle is a separate, exact cluster and is never
        # migrated or rolled back by this harness.
        predecessor = capture_behavior(args.predecessor_db_url)
        with psycopg.connect(args.predecessor_db_url) as predecessor_conn:
            predecessor_public = public_function_catalog(
                predecessor_conn, include_oid=False
            )
            predecessor_catalog_sha = catalog_snapshot(
                predecessor_conn
            )["catalogSha256"]
        head = capture_behavior(args.candidate_db_url)
        if predecessor != head or predecessor["mixedRows"] != 0:
            raise AssertionError(f"predecessor/head parity failed: {predecessor} != {head}")

        # Candidate-only rollback creates an isolated predecessor-shaped state
        # for malicious preflight tests; it is not used as the behavior oracle.
        psql(args.candidate_db_url, ROLLBACK)
        assert_preflight_negatives(args.candidate_db_url)
        psql(args.candidate_db_url, MIGRATION)
        assert_retry_negatives(args.candidate_db_url)
        assert_rollback_negatives(args.candidate_db_url)

        assert_private_role_runtime(args.candidate_db_url)
        assert_api_executor_public_compat(args.candidate_db_url)
        assert_role_matrix(args.candidate_db_url)
        concurrency = assert_concurrency(args.candidate_db_url)
        atomicity = assert_fault_and_lock_atomicity(args.candidate_db_url)
        volume = assert_volume_retry_rollback(
            args.candidate_db_url, predecessor_public, predecessor_catalog_sha
        )
        assert_telemetry(args.candidate_db_url, args.expected_candidate_container)
        if args.api_url and anon_key and service_key:
            postgrest(anon_key, service_key, args.api_url)
        print(json.dumps({
            "ok": True, "concurrency": concurrency,
            "atomicity": atomicity, "volume": volume,
            "transport": bool(args.api_url), "telemetry": "exactly-once",
        }, sort_keys=True))
    finally:
        with psycopg.connect(args.candidate_db_url, autocommit=True) as admin:
            admin.execute(
                "delete from public.lcia_document_validation_evidence where dataset_version=%s",
                (FIXTURE_VERSION,),
            )
            for created_role in list(CREATED_ROLES):
                admin.execute(f"drop role {created_role}")
                CREATED_ROLES.discard(created_role)
        with psycopg.connect(args.candidate_db_url) as check:
            private_count = scalar(check, """
              select count(*) from pg_proc p join pg_namespace n on n.oid=p.pronamespace
              where n.nspname='private' and p.proname in (
                'svc_lcia_document_validation_evidence_lookup',
                'svc_lcia_document_validation_evidence_record'
              )
            """)
        with psycopg.connect(args.candidate_db_url) as check:
            private_table_exists = scalar(
                check,
                "select to_regclass('private.lcia_document_validation_evidence') is not null",
            )
        if not private_table_exists:
            psql(args.candidate_db_url, ROLLFORWARD)
        if database_identity(args.candidate_db_url)[1] != expected_candidate_versions:
            raise AssertionError("candidate migration head drifted during cleanup")


if __name__ == "__main__":
    main()
