#!/usr/bin/env python3
"""Fail-closed offline gate for database-engine Issue #390 pre-DDL evidence."""

from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.issue_390_pre_ddl_gate import (
    PGLAST_PARSER_VERSION,
    PGLAST_VERSION,
    pre_ddl_migration_violations,
    pre_ddl_sql_signals,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = (
    ROOT / "supabase/tests/contracts/lca_result_family_pre_ddl.v1.json"
)
INVENTORY_PATH = ROOT / "supabase/tests/contracts/public_object_inventory.json"
PHASE_PATH = ROOT / "supabase/tests/contracts/schema_boundary_phase.v1.json"
CATALOG_PATH = ROOT / "supabase/tests/contracts/database_catalog.json"
CATALOG_SHA_PATH = ROOT / "supabase/tests/contracts/database_catalog.sha256"
MIGRATIONS = ROOT / "supabase/migrations"

PHYSICAL_TARGETS = {
    "public.lca_factorization_registry",
    "public.lca_latest_all_unit_results",
    "public.lca_result_cache",
    "public.lca_results",
}
ROUTINE_TARGETS = {
    "public.lca_read_job_projection(p_requested_by uuid, p_worker_job_id uuid, p_legacy_job_id uuid, p_include_internal boolean)",
    "public.lca_read_latest_single_solve_result(p_requested_by uuid, p_snapshot_id uuid, p_process_index integer)",
    "public.lca_read_result_projection(p_requested_by uuid, p_result_id uuid, p_required_artifact_format text, p_include_internal boolean)",
}
ROUTINE_NAMES = {
    "lca_read_job_projection",
    "lca_read_latest_single_solve_result",
    "lca_read_result_projection",
}

FREEZE_BASE_COMMIT = "0e68237a036e0994b70f246653e370b7890f2b30"


def migration_tree_entries(repo: Path, revision: str) -> dict[str, tuple[str, str]]:
    rows = subprocess.run(
        ["git", "ls-tree", "-r", revision, "--", "supabase/migrations"],
        cwd=repo,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout.splitlines()
    entries = {}
    for line in rows:
        metadata, path = line.split("\t", 1)
        mode, object_type, object_sha = metadata.split()
        if object_type != "blob":
            entries[path] = (mode, f"non-blob:{object_type}:{object_sha}")
        else:
            entries[path] = (mode, object_sha)
    return entries


def append_only_parent_edge_violations(
    repo: Path, base: str, head: str
) -> list[str]:
    commits = subprocess.run(
        [
            "git",
            "rev-list",
            "--reverse",
            "--topo-order",
            "--ancestry-path",
            f"{base}..{head}",
        ],
        cwd=repo,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout.splitlines()
    violations = []
    for commit in commits:
        lineage = subprocess.run(
            ["git", "rev-list", "--parents", "-n", "1", commit],
            cwd=repo,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout.split()
        child_entries = migration_tree_entries(repo, commit)
        for parent in lineage[1:]:
            governed_parent = parent == base or subprocess.run(
                ["git", "merge-base", "--is-ancestor", base, parent],
                cwd=repo,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            ).returncode == 0
            if not governed_parent:
                continue
            for path, expected in migration_tree_entries(repo, parent).items():
                if child_entries.get(path) != expected:
                    violations.append(f"{parent}->{commit}:{path}")
    return violations


def acl_has_privilege(acl: str, role: str, privilege: str) -> bool:
    for entry in acl.removeprefix("{").removesuffix("}").split(","):
        grantee, separator, grant = entry.partition("=")
        if separator and grantee == role:
            return privilege in grant.partition("/")[0]
    return False


class Issue390PreDdlGateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        cls.inventory = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
        cls.phase = json.loads(PHASE_PATH.read_text(encoding="utf-8"))
        cls.catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))

    def test_contract_is_exactly_pre_ddl_and_not_authorized(self) -> None:
        self.assertEqual(self.contract["issue"], "tiangong-lca/database-engine#390")
        self.assertEqual(self.contract["phase"], "pre-ddl-consumer-cut")
        self.assertFalse(self.contract["ddlAuthorized"])
        self.assertEqual(self.contract["databaseBaseCommit"], FREEZE_BASE_COMMIT)
        self.assertEqual(set(self.contract["physicalTargets"]), PHYSICAL_TARGETS)
        self.assertEqual(set(self.contract["routineTargets"]), ROUTINE_TARGETS)
        self.assertEqual(
            self.contract["dependencyClosure"],
            {
                "derivation": "Recursive application-object references from the final committed definitions at databaseBaseCommit; PostgreSQL catalog builtins are external anchors.",
                "relations": ["private.worker_jobs", "public.worker_jobs"],
                "routines": [
                    "private.worker_job_payload(private.worker_jobs, boolean)",
                    "public.lca_legacy_job_type(text)",
                    "public.worker_job_payload(public.worker_jobs, boolean)",
                ],
            },
        )
        self.assertEqual(
            self.contract["sqlParser"],
            {
                "name": "pglast",
                "version": PGLAST_VERSION.removeprefix("v"),
                "postgresParserVersion": PGLAST_PARSER_VERSION,
            },
        )
        self.assertEqual(
            self.contract["apiFacadeProtection"],
            {
                "schema": "api",
                "namePrefixes": ["lca_", "cmd_lca_"],
                "serviceGrantees": ["service_role", "api_internal_executor"],
            },
        )
        migration_gate = self.contract["migrationGate"]
        self.assertTrue(migration_gate["baseMigrationBlobsImmutable"])
        self.assertTrue(migration_gate["committedMigrationHistoryAppendOnly"])
        self.assertEqual(
            migration_gate["newMigrationsAllowed"],
            "target-neutral-static-or-reviewed-ast-facade",
        )
        self.assertFalse(migration_gate["relationMovingDdlAllowed"])
        self.assertFalse(migration_gate["browserPrivateCompatibilityGrantAllowed"])
        self.assertFalse(
            migration_gate["historicalAuthenticatedSelectRemovalAllowed"]
        )
        allowlist = migration_gate["allowedTargetTouchingMigrations"]
        self.assertEqual(
            len({(row["path"], row["gitBlob"]) for row in allowlist}),
            len(allowlist),
        )
        for row in allowlist:
            self.assertEqual(
                set(row), {"path", "gitBlob", "classification"}
            )
            self.assertTrue(row["path"].startswith("supabase/migrations/"))
            self.assertTrue(row["path"].endswith(".sql"))
            self.assertRegex(row["gitBlob"], r"^[0-9a-f]{40}$")
            self.assertEqual(
                row["classification"], "additive-api-service-only-reviewed"
            )

    def test_target_neutral_migrations_are_allowed_without_global_freeze(self) -> None:
        allowed = [
            "CREATE TABLE private.unrelated_job_receipts (id uuid PRIMARY KEY);",
            "ALTER TABLE private.unrelated_job_receipts ADD COLUMN note text;",
            "CREATE TABLE private.lca_results_archive (id uuid PRIMARY KEY);",
            "ALTER TABLE private.unrelated_job_receipts ADD CONSTRAINT note_present CHECK (note IS NOT NULL) NOT VALID;",
            "ALTER TABLE private.unrelated_job_receipts ADD CONSTRAINT receipt_parent FOREIGN KEY (id) REFERENCES private.receipt_parents(id) NOT VALID;",
            "CREATE VIEW api.unrelated_receipts WITH (security_invoker=true) AS SELECT id FROM public.processes;",
            "CREATE SCHEMA unrelated_owned AUTHORIZATION postgres;",
            "CREATE FUNCTION api.unrelated_constant() RETURNS pg_catalog.int4 LANGUAGE sql SECURITY INVOKER SET search_path='' AS $$ SELECT 1 $$;",
        ]
        for sql in allowed:
            self.assertEqual(pre_ddl_sql_signals(sql), [], sql)
            self.assertEqual(
                pre_ddl_migration_violations(
                    path="supabase/migrations/20990101000000_unrelated.sql",
                    git_blob="0" * 40,
                    sql=sql,
                    allowlist=[],
                ),
                [],
                sql,
            )

    def test_protected_and_opaque_sql_requires_exact_reviewed_blob(self) -> None:
        guarded = [
            "ALTER TABLE public.lca_results SET SCHEMA private;",
            'ALTER TABLE public."lca_result_cache" RENAME TO old_cache;',
            "DROP FUNCTION public.lca_read_result_projection(uuid,uuid,text,boolean);",
            "CREATE VIEW public.lca_latest_all_unit_results AS SELECT 1;",
            "REVOKE SELECT ON public.lca_results FROM authenticated;",
            'REVOKE ALL ON public.lca_results FROM "authenticated";',
            "GRANT USAGE ON SCHEMA private TO authenticated;",
            'GRANT USAGE ON SCHEMA "private" TO "authenticated";',
            'ALTER TABLE public.U&"lca\\005fresults" SET SCHEMA private;',
            "DO $$ BEGIN EXECUTE 'ALTER ' || 'TABLE ' || 'public.' || 'lca_' || 'results SET SCHEMA private'; END $$;",
            "SELECT format('ALTER TABLE %I.%I SET SCHEMA private', 'public', 'lca_results') \\gexec;",
        ]
        for sql in guarded:
            self.assertNotEqual(pre_ddl_sql_signals(sql), [], sql)
            reviewed = [
                {
                    "path": "supabase/migrations/20990101000001_guarded.sql",
                    "gitBlob": "1" * 40,
                    "classification": "additive-api-service-only-reviewed",
                }
            ]
            self.assertNotEqual(
                pre_ddl_migration_violations(
                    path="supabase/migrations/20990101000001_guarded.sql",
                    git_blob="1" * 40,
                    sql=sql,
                    allowlist=[],
                ),
                [],
                sql,
            )
            self.assertNotEqual(
                pre_ddl_migration_violations(
                    path="supabase/migrations/20990101000001_guarded.sql",
                    git_blob="1" * 40,
                    sql=sql,
                    allowlist=reviewed,
                ),
                [],
                f"exact allowlist must not override hard denial: {sql}",
            )

    def test_ast_gate_resists_lexical_role_and_execution_bypasses(self) -> None:
        denied = [
            "ALTER/**/TABLE public.lca_results SET/**/SCHEMA private;",
            "DROP/**/TABLE public.lca_result_cache;",
            "REVOKE/**/SELECT ON public.lca_results FROM/**/authenticated;",
            "GRANT USAGE ON SCHEMA private TO service_role, authenticated;",
            "REVOKE SELECT ON public.lca_results FROM service_role, authenticated;",
            "GRANT api_internal_executor TO authenticated;",
            "DROP OWNED BY authenticated;",
            "DROP OWNED BY postgres;",
            "DROP OWNED BY supabase_admin;",
            "REASSIGN OWNED BY postgres TO unrelated_owner;",
            "REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM authenticated;",
            "REVOKE USAGE ON SCHEMA public FROM authenticated;",
            "REVOKE USAGE ON SCHEMA public FROM PUBLIC;",
            "ALTER ROLE authenticated SUPERUSER;",
            "ALTER ROLE anon BYPASSRLS;",
            "DROP ROLE service_role;",
            "CREATE ROLE api_internal_executor SUPERUSER;",
            "ALTER SCHEMA public RENAME TO displaced_public;",
            "ALTER SCHEMA private RENAME TO displaced_private;",
            "CALL admin.run_sql('ALTER TABLE public.lca_results SET SCHEMA private');",
            "SELECT admin.run_sql('ALTER TABLE public.lca_results SET SCHEMA private');",
            "DO LANGUAGE plpgsql $$ BEGIN PERFORM admin.run_sql('ALTER TABLE public.lca_results SET SCHEMA private'); END $$;",
        ]
        for sql in denied:
            signals = pre_ddl_sql_signals(sql)
            self.assertTrue(
                any(signal.startswith("hard-deny:") for signal in signals), sql
            )
            self.assertNotEqual(
                pre_ddl_migration_violations(
                    path="supabase/migrations/20990101000003_bypass.sql",
                    git_blob="4" * 40,
                    sql=sql,
                    allowlist=[
                        {
                            "path": "supabase/migrations/20990101000003_bypass.sql",
                            "gitBlob": "4" * 40,
                            "classification": "additive-api-service-only-reviewed",
                        }
                    ],
                ),
                [],
                sql,
            )

    def test_ast_identifier_semantics_ignore_literals_and_case_distinct_names(self) -> None:
        allowed = [
            "COMMENT ON TABLE private.unrelated_job_receipts IS 'public.lca_results';",
            'ALTER TABLE public."LCA_RESULTS" SET SCHEMA private;',
        ]
        for sql in allowed:
            self.assertEqual(pre_ddl_sql_signals(sql), [], sql)

    def test_allowlisted_facade_rejects_non_catalog_helper_execution(self) -> None:
        path = "supabase/migrations/20990101000004_helper.sql"
        blob = "5" * 40
        sql = """
        CREATE FUNCTION api.lca_result_cache_read_v1(p_id pg_catalog.uuid)
        RETURNS pg_catalog.jsonb LANGUAGE sql SECURITY INVOKER SET search_path = ''
        AS $$ SELECT admin.run_sql('DROP TABLE public.lca_result_cache')
               FROM public.lca_result_cache WHERE id = p_id $$;
        REVOKE ALL ON FUNCTION api.lca_result_cache_read_v1(pg_catalog.uuid) FROM PUBLIC, anon, authenticated;
        GRANT EXECUTE ON FUNCTION api.lca_result_cache_read_v1(pg_catalog.uuid) TO service_role;
        """
        violations = pre_ddl_migration_violations(
            path=path,
            git_blob=blob,
            sql=sql,
            allowlist=[
                {
                    "path": path,
                    "gitBlob": blob,
                    "classification": "additive-api-service-only-reviewed",
                }
            ],
        )
        self.assertIn("hard-deny:opaque-function-call", violations)

    def test_allowlisted_facade_rejects_custom_operator_type_and_relation(self) -> None:
        path = "supabase/migrations/20990101000005_custom_object.sql"
        blob = "6" * 40
        templates = [
            "SELECT id OPERATOR(admin.=) id FROM public.lca_result_cache",
            "SELECT id::admin.evil FROM public.lca_result_cache",
            "SELECT id FROM admin.evil, public.lca_result_cache",
        ]
        for body in templates:
            sql = f"""
            CREATE FUNCTION api.lca_result_cache_read_v1(p_id pg_catalog.uuid)
            RETURNS pg_catalog.jsonb LANGUAGE sql SECURITY INVOKER SET search_path = ''
            AS $$ {body} $$;
            REVOKE ALL ON FUNCTION api.lca_result_cache_read_v1(pg_catalog.uuid) FROM PUBLIC, anon, authenticated;
            GRANT EXECUTE ON FUNCTION api.lca_result_cache_read_v1(pg_catalog.uuid) TO service_role;
            """
            self.assertNotEqual(
                pre_ddl_migration_violations(
                    path=path,
                    git_blob=blob,
                    sql=sql,
                    allowlist=[
                        {
                            "path": path,
                            "gitBlob": blob,
                            "classification": "additive-api-service-only-reviewed",
                        }
                    ],
                ),
                [],
                body,
            )

    def test_gate_denies_schema_wide_browser_opaque_and_config_bypasses(self) -> None:
        denied = [
            "GRANT SELECT ON ALL TABLES IN SCHEMA public TO authenticated;",
            "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO authenticated;",
            "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA api TO authenticated;",
            "GRANT EXECUTE ON ALL ROUTINES IN SCHEMA api TO authenticated;",
            "GRANT USAGE ON SCHEMA private TO preexisting_browser_parent;",
            "INSERT INTO private.audit_log(id) VALUES (admin.run_sql('x'));",
            "UPDATE private.audit_log SET id = admin.run_sql('x');",
            "CREATE TABLE private.audit_copy AS SELECT admin.run_sql('x');",
            "CREATE TRIGGER attack AFTER INSERT ON private.audit_log EXECUTE FUNCTION admin.run_sql();",
            "SELECT 1 OPERATOR(admin.+) 2;",
            "SELECT 'x'::admin.evil;",
            "COPY (SELECT 1) TO PROGRAM 'admin_evil';",
            "IMPORT FOREIGN SCHEMA public FROM SERVER attacker INTO private;",
            "LOAD 'admin_evil';",
            "ALTER SCHEMA private OWNER TO authenticated;",
            "ALTER SCHEMA public OWNER TO authenticated;",
            "ALTER SCHEMA api RENAME TO api_old;",
            "DROP SCHEMA api CASCADE;",
            "ALTER SCHEMA api OWNER TO authenticated;",
            "GRANT CREATE ON SCHEMA api TO authenticated;",
            "REVOKE USAGE ON SCHEMA api FROM service_role;",
            "REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA api FROM service_role;",
            "REVOKE EXECUTE ON ALL ROUTINES IN SCHEMA api FROM service_role;",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA api GRANT EXECUTE ON FUNCTIONS TO authenticated;",
            "ALTER ROLE authenticator SET pgrst.db_schemas TO 'public, api, private';",
            "ALTER ROLE authenticator SET pgrst.db_schemas TO 'api,graphql_public';",
            "ALTER ROLE authenticator RESET pgrst.db_schemas;",
            "ALTER DATABASE postgres SET pgrst.db_extra_search_path TO 'public,archive';",
            "ALTER DATABASE postgres RESET ALL;",
            "ALTER ROLE authenticated RENAME TO old_authenticated;",
            "DROP ROLE authenticator;",
            "ALTER ROLE authenticated SET search_path TO admin, pg_catalog;",
            "GRANT api_internal_executor TO service_role;",
            "SET search_path TO admin, pg_catalog; SELECT to_jsonb(1);",
            "SELECT 'x'::evil;",
            "SELECT !! 1;",
            "SELECT * FROM private.preexisting_dynamic_view;",
            "INSERT INTO private.audit_log DEFAULT VALUES;",
            "DROP POLICY lca_results_authenticated_select ON public.lca_results;",
            "DROP TRIGGER result_trigger ON public.lca_results;",
            "DROP RULE result_rule ON public.lca_results;",
            "DROP POLICY result_policy ON lca_results;",
            "DROP TRIGGER result_trigger ON lca_results;",
            "DROP RULE result_rule ON lca_results;",
            "ALTER INDEX public.lca_results_pkey RENAME TO hidden_index;",
            "DROP INDEX public.lca_result_cache_lookup_idx;",
            "DROP SCHEMA util CASCADE;",
            "ALTER FUNCTION api.evil(uuid) RENAME TO lca_cache_read;",
            "ALTER FUNCTION private.lca_cache_read(uuid) SET SCHEMA api;",
            "ALTER TABLE private.audit_log ADD CONSTRAINT audit_uq UNIQUE(id);",
            "ALTER TABLE private.audit_log ADD COLUMN code uuid UNIQUE;",
            "ALTER TABLE private.audit_log ALTER COLUMN id TYPE admin.evil USING id::text;",
            "ALTER TABLE private.audit_log ALTER COLUMN id TYPE text USING id::text;",
            "ALTER TABLE private.audit_log ADD CONSTRAINT audit_check CHECK (id IS NOT NULL);",
            "ALTER TABLE private.audit_log VALIDATE CONSTRAINT audit_check;",
            "CREATE TABLE private.custom_typed (value admin.evil DEFAULT 'x');",
            "ALTER TABLE public.unrelated OWNER TO authenticated;",
            "ALTER VIEW api.unrelated OWNER TO authenticated;",
            "ALTER SEQUENCE public.seq OWNER TO authenticated;",
            "ALTER FUNCTION private.foo() OWNER TO authenticated;",
            "ALTER DATABASE postgres OWNER TO authenticated;",
            "ALTER TABLE private.audit_log ENABLE TRIGGER evil;",
            "ALTER TABLE private.audit_log ENABLE RULE evil;",
            "SET ROLE authenticated;",
            "SET LOCAL ROLE authenticated;",
            "RESET ROLE;",
            "SET SESSION AUTHORIZATION authenticated;",
            "CREATE SCHEMA unrelated AUTHORIZATION authenticated;",
            "CREATE TABLE private.foo(id int) USING evil;",
            "SET default_table_access_method = evil; CREATE TABLE private.foo(id int);",
            "ALTER TABLE private.foo SET ACCESS METHOD evil;",
            "ALTER TABLE private.foo SET LOGGED;",
            "ALTER TABLE private.foo SET UNLOGGED;",
            "ALTER TABLE private.foo SET TABLESPACE evil;",
            "ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO authenticated;",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA util GRANT SELECT ON TABLES TO authenticated;",
            "ALTER DEFAULT PRIVILEGES IN SCHEMA archive GRANT USAGE ON TYPES TO authenticated;",
            "GRANT USAGE ON SCHEMA util TO authenticated;",
            "GRANT SELECT ON util.secret TO authenticated;",
            "GRANT EXECUTE ON FUNCTION util.secret() TO authenticated;",
            "GRANT SELECT ON ALL TABLES IN SCHEMA archive TO authenticated;",
            "GRANT SELECT ON ALL TABLES IN SCHEMA api TO authenticated;",
            "REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM service_role;",
            "REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public FROM api_internal_executor;",
            "REVOKE EXECUTE ON ALL ROUTINES IN SCHEMA public FROM service_role;",
            "REVOKE USAGE ON SCHEMA public FROM service_role;",
            "REVOKE USAGE ON SCHEMA private FROM api_internal_executor;",
            "CREATE VIEW api.foo AS SELECT * FROM private.secret;",
            "CREATE VIEW api.lca_foo WITH (security_invoker=true) AS SELECT * FROM public.processes;",
            "CREATE FUNCTION api.other() RETURNS SETOF private.secret LANGUAGE sql SECURITY DEFINER SET search_path='' AS $$ SELECT * FROM private.secret $$;",
            "CREATE FUNCTION api.other(p uuid) RETURNS jsonb LANGUAGE sql SECURITY INVOKER SET search_path='' AS $$ SELECT NULL::jsonb $$;",
            "CREATE VIEW public.foo AS SELECT * FROM private.secret;",
            "CREATE FUNCTION public.foo() RETURNS SETOF private.secret LANGUAGE sql SECURITY DEFINER SET search_path='' AS $$ SELECT * FROM private.secret $$;",
            "ALTER TABLE private.t ADD COLUMN x uuid DEFAULT gen_random_uuid();",
            "ALTER TABLE private.t ADD COLUMN x timestamptz DEFAULT clock_timestamp();",
            "ALTER TABLE private.t ADD COLUMN normalized text GENERATED ALWAYS AS (lower(name)) STORED;",
            "ALTER TABLE private.t ADD COLUMN required text NOT NULL;",
            "ALTER TABLE private.t ALTER COLUMN x SET NOT NULL;",
            "CREATE TABLE private.foo(id int, EXCLUDE USING evil (id WITH =));",
            "CREATE TABLE private.foo(id int, EXCLUDE USING gist (id WITH OPERATOR(admin.=)));",
            "CREATE TABLE private.foo (LIKE private.source INCLUDING INDEXES);",
            "CREATE TABLE private.foo PARTITION OF private.parent FOR VALUES FROM (1) TO (2);",
            "CREATE OR REPLACE FUNCTION public.lca_legacy_job_type(pg_catalog.text) RETURNS pg_catalog.text LANGUAGE sql SECURITY INVOKER SET search_path='' AS $$ SELECT NULL::pg_catalog.text $$;",
            "CREATE OR REPLACE FUNCTION private.worker_job_payload(private.worker_jobs, pg_catalog.boolean) RETURNS pg_catalog.jsonb LANGUAGE sql SECURITY INVOKER SET search_path='' AS $$ SELECT '{}'::pg_catalog.jsonb $$;",
            "CREATE OR REPLACE VIEW public.worker_jobs WITH (security_invoker=true) AS SELECT * FROM private.worker_jobs;",
            "ALTER TABLE private.worker_jobs ADD COLUMN result_family_bypass pg_catalog.text;",
            "CREATE FUNCTION private.foo() RETURNS pg_catalog.int4 LANGUAGE sql SECURITY DEFINER SET search_path='' AS $$ SELECT 1 $$; ALTER FUNCTION private.foo() SET SCHEMA public; GRANT EXECUTE ON FUNCTION public.foo() TO authenticated;",
            "CREATE FUNCTION private.foo() RETURNS pg_catalog.int4 LANGUAGE sql SECURITY DEFINER SET search_path='' AS $$ SELECT 1 $$; ALTER FUNCTION private.foo() SET SCHEMA api; GRANT EXECUTE ON FUNCTION api.foo() TO authenticated;",
            "CREATE VIEW private.foo AS SELECT * FROM private.secret; ALTER VIEW private.foo SET SCHEMA public; GRANT SELECT ON public.foo TO authenticated;",
            "ALTER TABLE private.secret SET SCHEMA public; GRANT SELECT ON public.secret TO authenticated;",
            "ALTER VIEW private.secret SET SCHEMA api; GRANT SELECT ON api.secret TO authenticated;",
            "CREATE FUNCTION public.foo(p_requested_by pg_catalog.uuid, p_worker_job_id pg_catalog.uuid, p_legacy_job_id pg_catalog.uuid, p_include_internal pg_catalog.bool, p_extra pg_catalog.text DEFAULT NULL) RETURNS pg_catalog.jsonb LANGUAGE sql SECURITY INVOKER SET search_path='' AS $$ SELECT '{}'::pg_catalog.jsonb $$; ALTER FUNCTION public.foo(pg_catalog.uuid,pg_catalog.uuid,pg_catalog.uuid,pg_catalog.bool,pg_catalog.text) RENAME TO lca_read_job_projection;",
            "CREATE FUNCTION public.foo(pg_catalog.text, pg_catalog.text DEFAULT NULL) RETURNS pg_catalog.text LANGUAGE sql SECURITY INVOKER SET search_path='' AS $$ SELECT NULL::pg_catalog.text $$; ALTER FUNCTION public.foo(pg_catalog.text,pg_catalog.text) RENAME TO lca_legacy_job_type;",
            "CREATE TABLE public.foo(id pg_catalog.uuid); ALTER TABLE public.foo RENAME TO lca_results;",
            "CREATE VIEW public.foo WITH (security_invoker=true) AS SELECT * FROM public.processes; ALTER VIEW public.foo RENAME TO worker_jobs;",
            "CREATE VIEW api.foo WITH (security_invoker=true) AS SELECT 1 AS x; ALTER VIEW api.foo RENAME TO lca_results;",
            "CREATE VIEW api.foo WITH (security_invoker=true) AS SELECT 1 AS x; ALTER VIEW api.foo RENAME TO cmd_lca_probe;",
        ]
        for sql in denied:
            self.assertTrue(
                any(
                    signal.startswith("hard-deny:")
                    for signal in pre_ddl_sql_signals(sql)
                ),
                sql,
            )

    def test_allowlisted_facade_uses_exact_overload_and_final_acl_state(self) -> None:
        path = "supabase/migrations/20990101000006_overloads.sql"
        blob = "7" * 40
        reviewed = [
            {
                "path": path,
                "gitBlob": blob,
                "classification": "additive-api-service-only-reviewed",
            }
        ]
        create_uuid = """
        CREATE FUNCTION api.lca_cache_read(p_id pg_catalog.uuid)
        RETURNS pg_catalog.jsonb LANGUAGE sql SECURITY INVOKER SET search_path = ''
        AS $$ SELECT to_jsonb(r) FROM public.lca_result_cache AS r WHERE id = p_id $$;
        """
        create_text = """
        CREATE FUNCTION api.lca_cache_read(p_id pg_catalog.text)
        RETURNS pg_catalog.jsonb LANGUAGE sql SECURITY INVOKER SET search_path = ''
        AS $$ SELECT to_jsonb(r) FROM public.lca_result_cache AS r WHERE id::text = p_id $$;
        """
        cases = [
            create_uuid
            + create_text
            + "REVOKE ALL ON FUNCTION api.lca_cache_read(pg_catalog.text) FROM PUBLIC, anon, authenticated;"
            + "GRANT EXECUTE ON FUNCTION api.lca_cache_read(pg_catalog.text) TO service_role;",
            create_uuid
            + "REVOKE GRANT OPTION FOR EXECUTE ON FUNCTION api.lca_cache_read(pg_catalog.uuid) FROM PUBLIC, anon, authenticated;"
            + "GRANT EXECUTE ON FUNCTION api.lca_cache_read(pg_catalog.uuid) TO service_role;",
            create_uuid
            + "REVOKE ALL ON FUNCTION api.lca_cache_read(pg_catalog.uuid) FROM PUBLIC, anon, authenticated;"
            + "GRANT EXECUTE ON FUNCTION api.lca_cache_read(pg_catalog.uuid) TO service_role;"
            + "REVOKE EXECUTE ON FUNCTION api.lca_cache_read(pg_catalog.uuid) FROM service_role;",
            create_uuid
            + "ALTER FUNCTION api.lca_cache_read(pg_catalog.uuid) SECURITY DEFINER;"
            + "REVOKE ALL ON FUNCTION api.lca_cache_read(pg_catalog.uuid) FROM PUBLIC, anon, authenticated;"
            + "GRANT EXECUTE ON FUNCTION api.lca_cache_read(pg_catalog.uuid) TO service_role;",
            create_uuid
            + "SAVEPOINT facade_acl;"
            + "REVOKE ALL ON FUNCTION api.lca_cache_read(pg_catalog.uuid) FROM PUBLIC, anon, authenticated;"
            + "ROLLBACK TO SAVEPOINT facade_acl;"
            + "GRANT EXECUTE ON FUNCTION api.lca_cache_read(pg_catalog.uuid) TO service_role;",
        ]
        for sql in cases:
            self.assertNotEqual(
                pre_ddl_migration_violations(
                    path=path,
                    git_blob=blob,
                    sql=sql,
                    allowlist=reviewed,
                ),
                [],
                sql,
            )

    def test_allowlisted_facade_cte_scope_is_lexical(self) -> None:
        path = "supabase/migrations/20990101000009_cte_scope.sql"
        blob = "a" * 40
        reviewed = [
            {
                "path": path,
                "gitBlob": blob,
                "classification": "additive-api-service-only-reviewed",
            }
        ]

        def migration(body: str) -> str:
            return f"""
            CREATE FUNCTION api.lca_cache_read(p_id pg_catalog.uuid)
            RETURNS pg_catalog.jsonb LANGUAGE sql SECURITY INVOKER SET search_path = ''
            AS $$ {body} $$;
            REVOKE ALL ON FUNCTION api.lca_cache_read(pg_catalog.uuid) FROM PUBLIC, anon, authenticated;
            GRANT EXECUTE ON FUNCTION api.lca_cache_read(pg_catalog.uuid) TO service_role;
            """

        invalid_bodies = [
            "SELECT to_jsonb(e) FROM evil e WHERE EXISTS (WITH evil AS (SELECT id FROM public.lca_result_cache) SELECT 1 FROM evil)",
            "SELECT to_jsonb(r) FROM public.lca_result_cache r, x WHERE EXISTS (WITH x AS (SELECT 1) SELECT 1)",
            "WITH x AS (SELECT * FROM x), target AS (SELECT id FROM public.lca_result_cache) SELECT pg_catalog.to_jsonb(x) FROM x,target",
            "WITH x AS (SELECT * FROM later), later AS (SELECT id FROM public.lca_result_cache) SELECT pg_catalog.to_jsonb(x) FROM x,later",
            "WITH RECURSIVE x AS (SELECT id FROM public.lca_result_cache UNION ALL SELECT id FROM x) SELECT pg_catalog.to_jsonb(x) FROM x",
        ]
        for body in invalid_bodies:
            self.assertNotEqual(
                pre_ddl_migration_violations(
                    path=path,
                    git_blob=blob,
                    sql=migration(body),
                    allowlist=reviewed,
                ),
                [],
                body,
            )

        valid_body = "WITH scoped AS (SELECT id FROM public.lca_result_cache) SELECT to_jsonb(scoped) FROM scoped"
        self.assertEqual(
            pre_ddl_migration_violations(
                path=path,
                git_blob=blob,
                sql=migration(valid_body),
                allowlist=reviewed,
            ),
            [],
        )

    def test_allowlisted_facade_rejects_defaults_and_unqualified_targets(self) -> None:
        path = "supabase/migrations/20990101000007_defaults.sql"
        blob = "8" * 40
        reviewed = [
            {
                "path": path,
                "gitBlob": blob,
                "classification": "additive-api-service-only-reviewed",
            }
        ]
        cases = [
            """
            CREATE FUNCTION api.lca_cache_read(p_id pg_catalog.uuid DEFAULT admin.run_sql('x'))
            RETURNS pg_catalog.jsonb LANGUAGE sql SECURITY INVOKER SET search_path = ''
            AS $$ SELECT to_jsonb(r) FROM public.lca_result_cache AS r $$;
            REVOKE ALL ON FUNCTION api.lca_cache_read(pg_catalog.uuid) FROM PUBLIC, anon, authenticated;
            GRANT EXECUTE ON FUNCTION api.lca_cache_read(pg_catalog.uuid) TO service_role;
            """,
            """
            CREATE FUNCTION api.lca_cache_read(p_id pg_catalog.uuid)
            RETURNS pg_catalog.jsonb LANGUAGE sql SECURITY INVOKER SET search_path = ''
            AS $$ SELECT to_jsonb(r) FROM lca_result_cache AS r $$;
            REVOKE ALL ON FUNCTION api.lca_cache_read(pg_catalog.uuid) FROM PUBLIC, anon, authenticated;
            GRANT EXECUTE ON FUNCTION api.lca_cache_read(pg_catalog.uuid) TO service_role;
            """,
        ]
        for sql in cases:
            self.assertNotEqual(
                pre_ddl_migration_violations(
                    path=path,
                    git_blob=blob,
                    sql=sql,
                    allowlist=reviewed,
                ),
                [],
                sql,
            )

    def test_api_lca_facade_lifecycle_cannot_be_mutated_after_creation(self) -> None:
        path = "supabase/migrations/20990101000008_facade_mutation.sql"
        blob = "9" * 40
        reviewed = [
            {
                "path": path,
                "gitBlob": blob,
                "classification": "additive-api-service-only-reviewed",
            }
        ]
        cases = [
            "GRANT EXECUTE ON FUNCTION api.lca_cache_read(pg_catalog.uuid) TO authenticated;",
            "ALTER FUNCTION api.lca_cache_read(pg_catalog.uuid) SECURITY DEFINER;",
            "DROP FUNCTION api.lca_cache_read(pg_catalog.uuid);",
            """
            CREATE OR REPLACE FUNCTION api.lca_cache_read(pg_catalog.uuid)
            RETURNS pg_catalog.jsonb LANGUAGE sql SECURITY DEFINER
            AS $$ SELECT NULL::pg_catalog.jsonb $$;
            """,
        ]
        for sql in cases:
            self.assertNotEqual(pre_ddl_sql_signals(sql), [], sql)
            self.assertNotEqual(
                pre_ddl_migration_violations(
                    path=path,
                    git_blob=blob,
                    sql=sql,
                    allowlist=reviewed,
                ),
                [],
                sql,
            )

    def test_additive_facade_requires_path_blob_and_classification_match(self) -> None:
        path = "supabase/migrations/20990101000002_api_facade.sql"
        blob = "2" * 40
        sql = """
        CREATE FUNCTION api.lca_result_cache_read_v1(p_id pg_catalog.uuid)
        RETURNS pg_catalog.jsonb LANGUAGE sql SECURITY INVOKER SET search_path = ''
        AS $$ SELECT to_jsonb(r) FROM public.lca_result_cache AS r WHERE id = p_id $$;
        REVOKE ALL ON FUNCTION api.lca_result_cache_read_v1(pg_catalog.uuid) FROM PUBLIC, anon, authenticated;
        GRANT EXECUTE ON FUNCTION api.lca_result_cache_read_v1(pg_catalog.uuid) TO service_role;
        """
        signals = pre_ddl_sql_signals(sql)
        self.assertIn(
            "protected-identifier:relation:public.lca_result_cache", signals
        )
        reviewed = [
            {
                "path": path,
                "gitBlob": blob,
                "classification": "additive-api-service-only-reviewed",
            }
        ]
        self.assertEqual(
            pre_ddl_migration_violations(
                path=path,
                git_blob=blob,
                sql=sql,
                allowlist=reviewed,
            ),
            [],
        )
        self.assertEqual(
            pre_ddl_migration_violations(
                path=path,
                git_blob="3" * 40,
                sql=sql,
                allowlist=reviewed,
            ),
            signals,
        )
        self.assertEqual(
            pre_ddl_migration_violations(
                path=path,
                git_blob=blob,
                sql=sql,
                allowlist=[{**reviewed[0], "classification": "unreviewed"}],
            ),
            signals,
        )

    def test_checkout_descends_from_base_and_preserves_base_migrations(self) -> None:
        base = FREEZE_BASE_COMMIT
        ancestry = subprocess.run(
            ["git", "merge-base", "--is-ancestor", base, "HEAD"],
            cwd=ROOT,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertEqual(ancestry.returncode, 0, ancestry.stderr)
        self.assertEqual(
            append_only_parent_edge_violations(ROOT, base, "HEAD"),
            [],
            "every governed parent migration set must be an unchanged subset "
            "of each child commit",
        )

        base_migrations = subprocess.run(
            [
                "git",
                "ls-tree",
                "-r",
                base,
                "--",
                "supabase/migrations",
            ],
            cwd=ROOT,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout.splitlines()
        base_entries = {}
        for line in base_migrations:
            metadata, path = line.split("\t", 1)
            mode, object_type, object_sha = metadata.split()
            self.assertEqual(object_type, "blob", f"non-blob base migration: {path}")
            base_entries[path] = (mode, object_sha)

        head_migrations = subprocess.run(
            ["git", "ls-tree", "-r", "HEAD", "--", "supabase/migrations"],
            cwd=ROOT,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout.splitlines()
        head_entries = {}
        for line in head_migrations:
            metadata, path = line.split("\t", 1)
            mode, object_type, object_sha = metadata.split()
            self.assertEqual(object_type, "blob", f"non-blob HEAD migration: {path}")
            head_entries[path] = (mode, object_sha)

        contract_path = CONTRACT_PATH.relative_to(ROOT).as_posix()
        history_entries = dict(base_entries)
        post_base_commits = subprocess.run(
            [
                "git",
                "rev-list",
                "--reverse",
                "--topo-order",
                "--ancestry-path",
                f"{base}..HEAD",
            ],
            cwd=ROOT,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout.splitlines()
        for commit in post_base_commits:
            committed_migrations = subprocess.run(
                ["git", "ls-tree", "-r", commit, "--", "supabase/migrations"],
                cwd=ROOT,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            ).stdout.splitlines()
            for line in committed_migrations:
                metadata, path = line.split("\t", 1)
                mode, object_type, object_sha = metadata.split()
                self.assertEqual(
                    object_type, "blob", f"non-blob historical migration: {path}"
                )
                observed = (mode, object_sha)
                if path in history_entries:
                    self.assertEqual(
                        observed,
                        history_entries[path],
                        f"migration changed after first committed appearance: {path}",
                    )
                else:
                    history_entries[path] = observed
                    commit_contract = json.loads(
                        subprocess.run(
                            ["git", "show", f"{commit}:{contract_path}"],
                            cwd=ROOT,
                            check=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True,
                        ).stdout
                    )
                    commit_sql = subprocess.run(
                        ["git", "show", f"{commit}:{path}"],
                        cwd=ROOT,
                        check=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                    ).stdout
                    commit_violations = pre_ddl_migration_violations(
                        path=path,
                        git_blob=object_sha,
                        sql=commit_sql,
                        allowlist=commit_contract.get("migrationGate", {}).get(
                            "allowedTargetTouchingMigrations", []
                        ),
                    )
                    self.assertEqual(
                        commit_violations,
                        [],
                        "migration was not authorized by the contract in its "
                        f"first committed appearance: {commit}:{path}",
                    )

        worktree_entries = {}
        for path in sorted(MIGRATIONS.rglob("*")):
            self.assertFalse(path.is_symlink(), f"migration symlink forbidden: {path}")
            if not path.is_file():
                continue
            relative = path.relative_to(ROOT).as_posix()
            object_sha = subprocess.run(
                ["git", "hash-object", "--", relative],
                cwd=ROOT,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            ).stdout.strip()
            mode = "100755" if path.stat().st_mode & 0o111 else "100644"
            worktree_entries[relative] = (mode, object_sha)

        index_migrations = subprocess.run(
            ["git", "ls-files", "--stage", "--", "supabase/migrations"],
            cwd=ROOT,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout.splitlines()
        index_entries = {}
        for line in index_migrations:
            metadata, path = line.split("\t", 1)
            mode, object_sha, stage = metadata.split()
            self.assertEqual(stage, "0", f"unmerged migration index entry: {path}")
            self.assertNotEqual(mode, "120000", f"migration symlink forbidden: {path}")
            index_entries[path] = (mode, object_sha)

        source_contracts = {
            "head": json.loads(
                subprocess.run(
                    ["git", "show", f"HEAD:{contract_path}"],
                    cwd=ROOT,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                ).stdout
            ),
            "index": json.loads(
                subprocess.run(
                    ["git", "show", f":{contract_path}"],
                    cwd=ROOT,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                ).stdout
            ),
            "worktree": self.contract,
        }
        source_allowlists = {
            source: contract.get("migrationGate", {}).get(
                "allowedTargetTouchingMigrations", []
            )
            for source, contract in source_contracts.items()
        }

        immutable_entries = {
            "head": history_entries,
            "index": head_entries,
            "worktree": head_entries,
        }

        for source, observed_entries in (
            ("head", head_entries),
            ("index", index_entries),
            ("worktree", worktree_entries),
        ):
            changed_base_entries = {
                path: {"expected": expected, "observed": observed_entries.get(path)}
                for path, expected in immutable_entries[source].items()
                if observed_entries.get(path) != expected
            }
            self.assertEqual(
                changed_base_entries,
                {},
                f"committed migration path/mode/blobs are immutable in the {source}",
            )

            new_paths = sorted(set(observed_entries) - set(base_entries))
            self.assertTrue(
                all(Path(path).suffix == ".sql" for path in new_paths),
                f"new {source} migration artifacts must be SQL files",
            )
            self.assertTrue(
                all(observed_entries[path][0] == "100644" for path in new_paths),
                f"new {source} migrations must be regular non-executable files",
            )
            violations = {}
            for relative in new_paths:
                if source == "worktree":
                    sql = (ROOT / relative).read_text(encoding="utf-8")
                else:
                    revision = "HEAD" if source == "head" else ""
                    object_spec = f"{revision}:{relative}"
                    sql = subprocess.run(
                        ["git", "show", object_spec],
                        cwd=ROOT,
                        check=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                    ).stdout
                detected = pre_ddl_migration_violations(
                    path=relative,
                    git_blob=observed_entries[relative][1],
                    sql=sql,
                    allowlist=source_allowlists[source],
                )
                if detected:
                    violations[relative] = detected
            self.assertEqual(
                violations,
                {},
                f"new {source} migrations contain pre-DDL target-boundary mutations",
            )

        base_versions = [
            Path(path).name.split("_", 1)[0]
            for path in base_entries
            if Path(path).suffix == ".sql" and "_" in Path(path).name
        ]
        self.assertEqual(
            max(base_versions), self.contract["predecessorMigrationHead"]
        )

        current_versions = [
            path.name.split("_", 1)[0]
            for path in MIGRATIONS.glob("[0-9]*.sql")
            if "_" in path.name
        ]
        self.assertIn(self.contract["predecessorMigrationHead"], current_versions)
        self.assertGreaterEqual(
            max(current_versions), self.contract["predecessorMigrationHead"]
        )

    def test_append_only_history_checks_parent_edges_and_parallel_branches(self) -> None:
        with tempfile.TemporaryDirectory(prefix="issue-390-history-") as directory:
            repo = Path(directory)
            subprocess.run(
                ["git", "init", "-q"], cwd=repo, check=True
            )
            subprocess.run(
                ["git", "config", "user.name", "Issue 390 Test"],
                cwd=repo,
                check=True,
            )
            subprocess.run(
                ["git", "config", "user.email", "issue-390@example.invalid"],
                cwd=repo,
                check=True,
            )
            migrations = repo / "supabase" / "migrations"
            migrations.mkdir(parents=True)
            base_path = migrations / "1_base.sql"
            base_path.write_text("select 1;\n", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(
                ["git", "commit", "-qm", "base"], cwd=repo, check=True
            )
            base = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repo,
                check=True,
                stdout=subprocess.PIPE,
                text=True,
            ).stdout.strip()

            subprocess.run(
                ["git", "checkout", "-qb", "sibling-a"], cwd=repo, check=True
            )
            (migrations / "2_a.sql").write_text("select 2;\n", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(
                ["git", "commit", "-qm", "sibling a"], cwd=repo, check=True
            )

            subprocess.run(
                ["git", "checkout", "-qb", "sibling-b", base],
                cwd=repo,
                check=True,
            )
            (migrations / "3_b.sql").write_text("select 3;\n", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(
                ["git", "commit", "-qm", "sibling b"], cwd=repo, check=True
            )
            subprocess.run(
                ["git", "merge", "-q", "--no-ff", "sibling-a", "-m", "merge"],
                cwd=repo,
                check=True,
            )
            merged = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repo,
                check=True,
                stdout=subprocess.PIPE,
                text=True,
            ).stdout.strip()
            self.assertEqual(
                append_only_parent_edge_violations(repo, base, merged), []
            )

            base_path.unlink()
            subprocess.run(
                ["git", "commit", "-qam", "delete base migration"],
                cwd=repo,
                check=True,
            )
            base_path.write_text("select 1;\n", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(
                ["git", "commit", "-qm", "restore base migration"],
                cwd=repo,
                check=True,
            )
            head = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repo,
                check=True,
                stdout=subprocess.PIPE,
                text=True,
            ).stdout.strip()
            violations = append_only_parent_edge_violations(repo, base, head)
            self.assertTrue(
                any(value.endswith(":supabase/migrations/1_base.sql") for value in violations),
                violations,
            )

    def test_inventory_assigns_all_seven_objects_to_private_lca_batch(self) -> None:
        targets = PHYSICAL_TARGETS | ROUTINE_TARGETS
        rows = {
            row["objectKey"]: row
            for row in self.inventory["objects"]
            if row["objectKey"] in targets
        }
        self.assertEqual(set(rows), targets)
        for row in rows.values():
            self.assertEqual(row["targetSchema"], "private")
            self.assertEqual(row["migrationBatch"], "33-lca")

        self.assertEqual(self.phase["phase"], "expand")
        self.assertFalse(self.phase["expand"]["contractReady"])
        self.assertFalse(self.inventory["contractReady"])

    def test_catalog_is_digest_bound_and_cross_checked_field_for_field(self) -> None:
        evidence = self.contract["persistentDevCatalog"]
        repository_catalog = evidence["repositoryCatalog"]
        self.assertEqual(
            repository_catalog["path"],
            "supabase/tests/contracts/database_catalog.json",
        )
        self.assertEqual(repository_catalog["sourceCommit"], FREEZE_BASE_COMMIT)
        current_digest = CATALOG_SHA_PATH.read_text(encoding="utf-8").strip()
        actual_current_digest = hashlib.sha256(CATALOG_PATH.read_bytes()).hexdigest()
        self.assertEqual(current_digest, actual_current_digest)
        base = self.contract["databaseBaseCommit"]
        base_catalog = subprocess.run(
            [
                "git",
                "show",
                f"{base}:supabase/tests/contracts/database_catalog.json",
            ],
            cwd=ROOT,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).stdout
        base_digest_file = subprocess.run(
            [
                "git",
                "show",
                f"{base}:supabase/tests/contracts/database_catalog.sha256",
            ],
            cwd=ROOT,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout.strip()
        actual_base_digest = hashlib.sha256(base_catalog).hexdigest()
        self.assertEqual(repository_catalog["sha256"], actual_base_digest)
        self.assertEqual(repository_catalog["sha256"], base_digest_file)
        baseline_catalog = json.loads(base_catalog)

        hosted = evidence["hostedObservation"]
        hosted_query = hosted["queryInput"]
        hosted_receipt = hashlib.sha256(
            json.dumps(
                hosted_query,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
        ).hexdigest()
        self.assertEqual(hosted["queryReceiptSha256"], hosted_receipt)
        self.assertEqual(hosted_query["projectRef"], evidence["projectRef"])
        self.assertEqual(hosted_query["capturedAt"], evidence["capturedAt"])
        self.assertEqual(hosted_query["migrationHead"], evidence["migrationHead"])

        catalog_relations = {
            row["name"]: row
            for row in self.catalog["relations"]
            if row["schema"] == "public"
            and f"public.{row['name']}" in PHYSICAL_TARGETS
        }
        catalog_policies = {
            name: sorted(
                (
                    row
                    for row in self.catalog["policies"]
                    if row["schemaname"] == "public" and row["tablename"] == name
                ),
                key=lambda row: row["policyname"],
            )
            for name in catalog_relations
        }
        catalog_indexes = {
            name: sorted(
                (
                    row
                    for row in self.catalog["indexes"]
                    if row["schemaname"] == "public" and row["tablename"] == name
                ),
                key=lambda row: row["indexname"],
            )
            for name in catalog_relations
        }
        baseline_relations = {
            row["name"]: row
            for row in baseline_catalog["relations"]
            if row["schema"] == "public"
            and f"public.{row['name']}" in PHYSICAL_TARGETS
        }
        baseline_policies = {
            name: sorted(
                (
                    row
                    for row in baseline_catalog["policies"]
                    if row["schemaname"] == "public" and row["tablename"] == name
                ),
                key=lambda row: row["policyname"],
            )
            for name in baseline_relations
        }
        baseline_indexes = {
            name: sorted(
                (
                    row
                    for row in baseline_catalog["indexes"]
                    if row["schemaname"] == "public" and row["tablename"] == name
                ),
                key=lambda row: row["indexname"],
            )
            for name in baseline_relations
        }
        self.assertEqual(catalog_relations, baseline_relations)
        self.assertEqual(catalog_policies, baseline_policies)
        self.assertEqual(catalog_indexes, baseline_indexes)

        relations = evidence["relations"]
        self.assertEqual({row["name"] for row in relations}, set(catalog_relations))
        for row in relations:
            name = row["name"]
            self.assertEqual(row["canonicalCatalog"], catalog_relations[name])
            self.assertEqual(row["policies"], catalog_policies[name])
            self.assertEqual(row["hostedOwner"], "postgres")
            acl = row["canonicalCatalog"]["acl"]
            self.assertEqual(
                row["effectiveSelect"],
                {
                    "anon": acl_has_privilege(acl, "anon", "r"),
                    "authenticated": acl_has_privilege(
                        acl, "authenticated", "r"
                    ),
                    "service_role": acl_has_privilege(acl, "service_role", "r"),
                    "api_internal_executor": acl_has_privilege(
                        acl, "api_internal_executor", "r"
                    ),
                },
            )

        hosted_relation_owners = {
            row["name"]: row["owner"] for row in hosted_query["relations"]
        }
        self.assertEqual(hosted_relation_owners, {name: "postgres" for name in catalog_relations})

        authenticated = {
            row["name"]
            for row in relations
            if row["effectiveSelect"]["authenticated"]
        }
        self.assertEqual(authenticated, {"lca_results"})
        self.assertTrue(all(row["canonicalCatalog"]["rls"] for row in relations))
        self.assertTrue(
            all(row["effectiveSelect"]["service_role"] for row in relations)
        )
        self.assertTrue(
            all(
                row["effectiveSelect"]["api_internal_executor"]
                for row in relations
            )
        )

        catalog_routines = {
            (row["name"], row["identityArguments"]): row
            for row in self.catalog["functions"]
            if row["schema"] == "public" and row["name"] in ROUTINE_NAMES
        }
        baseline_routines = {
            (row["name"], row["identityArguments"]): row
            for row in baseline_catalog["functions"]
            if row["schema"] == "public" and row["name"] in ROUTINE_NAMES
        }
        self.assertEqual(catalog_routines, baseline_routines)
        routines = evidence["routines"]
        self.assertEqual(len(routines), 3)
        self.assertEqual(
            {
                (
                    row["canonicalCatalog"]["name"],
                    row["canonicalCatalog"]["identityArguments"],
                )
                for row in routines
            },
            set(catalog_routines),
        )
        for row in routines:
            canonical = row["canonicalCatalog"]
            key = (canonical["name"], canonical["identityArguments"])
            self.assertEqual(canonical, catalog_routines[key])
            self.assertEqual(row["hostedOwner"], "postgres")
            acl = canonical["acl"]
            self.assertEqual(
                row["effectiveExecute"],
                {
                    "anon": acl_has_privilege(acl, "anon", "X"),
                    "authenticated": acl_has_privilege(
                        acl, "authenticated", "X"
                    ),
                    "service_role": acl_has_privilege(acl, "service_role", "X"),
                    "api_internal_executor": acl_has_privilege(
                        acl, "api_internal_executor", "X"
                    ),
                },
            )
            self.assertEqual(canonical["config"], ["search_path=public, pg_temp"])
            self.assertEqual(canonical["result"], "jsonb")
            self.assertTrue(canonical["securityDefiner"])

        hosted_routine_owners = {
            row["signature"]: row["owner"] for row in hosted_query["routines"]
        }
        self.assertEqual(
            hosted_routine_owners,
            {signature: "postgres" for signature in ROUTINE_TARGETS},
        )

        self.assertEqual(
            evidence["migrationHead"],
            self.contract["predecessorMigrationHead"],
        )

    def test_runtime_zero_match_is_explicitly_non_authorizing(self) -> None:
        evidence = self.contract["runtimeEvidence"]
        query = evidence["queryInput"]
        self.assertEqual(
            query["environments"],
            [
                {
                    "name": "persistentDev",
                    "projectRef": "fotofiyqnuyvgtotswie",
                    "matchedRequestCount": 0,
                },
                {
                    "name": "production",
                    "projectRef": "qgzvkongdjqiiamzbbts",
                    "matchedRequestCount": 0,
                },
            ],
        )
        self.assertEqual(query["windowStart"], "2026-08-01T15:12:00Z")
        self.assertEqual(query["windowEnd"], "2026-08-02T15:12:00Z")
        self.assertEqual(query["queryVersion"], "supabase-api-path-zero-scan.v1")
        self.assertEqual(len(query["normalizedTargetPaths"]), 7)
        self.assertEqual(len(set(query["normalizedTargetPaths"])), 7)
        self.assertTrue(
            all(path.startswith("/rest/v1/") for path in query["normalizedTargetPaths"])
        )
        receipt = hashlib.sha256(
            json.dumps(
                query,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
        ).hexdigest()
        self.assertEqual(evidence["queryReceiptSha256"], receipt)
        self.assertFalse(evidence["sufficientForAuthorization"])

        gate = self.contract["authenticatedDirectSelectGate"]
        self.assertEqual(gate["state"], "collecting")
        self.assertEqual(gate["familyWindowHours"], 24)
        self.assertEqual(gate["globalWindowHours"], 72)
        self.assertTrue(gate["resetsOnRelevantDeploymentOrContractChange"])
        self.assertFalse(gate["privateBrowserGrantAllowed"])
        self.assertEqual(
            gate["authority"],
            "https://github.com/tiangong-lca/workspace/issues/532#issuecomment-5158835898",
        )
        self.assertIn("missing", self.contract["ownerSignoffs"].values())

    def test_active_source_inventory_distinguishes_canonical_and_candidates(self) -> None:
        sources = self.contract["sourceInventory"]
        expected_canonical = {
            "edge": ("dev", "8b9629387d839bdff343a21353438a513eb54d9c", 40),
            "worker": ("main", "cabb2518a69272c20abe61692eadb292b95596f2", 28),
            "utilities": ("main", "9c00c9adb3d1afcb3173381d9247fa611cf5c0ec", 39),
            "next": ("dev", "ea70e7415c630443aa1112566e3c37f4344777e8", 1),
            "cli": ("main", "5cb359f1d0860df560c7571fa7547b2822b37c71", 0),
            "mcp": ("main", "0ab741e0881c70ce526e936d222939e38f4a4911", 0),
            "release": ("main", "f8d37018d898d23a51655272d129417eb9fad13a", 1),
            "dataFoundry": ("main", "c3c74555b71e1ba33ee80a5c5919630a27ba79df", 0),
        }
        self.assertEqual(
            {key for key, value in sources.items() if isinstance(value, dict) and "repository" in value},
            set(expected_canonical),
        )
        for name, (branch, commit, count) in expected_canonical.items():
            canonical = sources[name]["canonical"]
            self.assertEqual(canonical["branch"], branch)
            self.assertEqual(canonical["commit"], commit)
            self.assertEqual(canonical["scanMatchCount"], count)
            self.assertRegex(canonical["commit"], r"^[0-9a-f]{40}$")

        self.assertEqual(
            sources["edge"]["candidate"],
            {
                "qualifiedSourceCommit": "6080b4c2c95b00c2666f98af6bb90610042fc3da",
                "mergedAsCommit": "8b9629387d839bdff343a21353438a513eb54d9c",
            },
        )
        worker_candidate = sources["worker"]["candidate"]
        self.assertEqual(
            worker_candidate["commit"],
            "ba454b9b7f64d4b48ef51feb7e2c188ba24a3011",
        )
        predecessors = {
            row["pullRequest"]: {
                "commit": row["commit"],
                "presentInCandidate": row["presentInCandidate"],
            }
            for row in worker_candidate["requiredPredecessors"]
        }
        self.assertEqual(
            predecessors,
            {
                200: {
                    "commit": "10183162a1944252fd01eeb5ffc1548cbe8c4ec1",
                    "presentInCandidate": True,
                },
                197: {
                    "commit": "90c5ea3575efbf1918b6e0135cd72ead894c90a0",
                    "presentInCandidate": False,
                },
            },
        )
        self.assertFalse(sources["staticEvidenceComplete"])
        self.assertEqual(
            sources["next"]["canonical"]["classification"],
            "indirect-edge-http-consumer",
        )
        self.assertEqual(
            sources["cli"]["canonical"]["classification"],
            "no-direct-family-match",
        )
        self.assertEqual(
            sources["mcp"]["canonical"]["classification"],
            "no-direct-family-match",
        )


if __name__ == "__main__":
    unittest.main()
