#!/usr/bin/env python3
"""Mutation-negative tests for the Issue #398 migration-specific semantic gate."""

from __future__ import annotations

import os
import re
import unittest
from pathlib import Path

from scripts.issue_390_pre_ddl_gate import pre_ddl_migration_violations
from scripts.issue_398_result_gc_semantic_gate import (
    REVIEWED_AST_SHA256,
    REVIEWED_GIT_BLOB,
    RESULT_GC_CLASSIFICATION,
    RESULT_GC_MIGRATION_PATH,
    git_blob_oid,
    normalized_ast_sha256,
    reviewed_result_gc_migration_violations,
    semantic_violations,
)


class Issue398ResultGcSemanticGateTest(unittest.TestCase):
    def assert_semantic_violation(self, sql: str, expected: str) -> None:
        violations = semantic_violations(sql)
        self.assertIn(expected, violations, violations)

    def test_classification_rejects_nonreviewed_source(self) -> None:
        sql = "ALTER TABLE public.lca_results ADD COLUMN retention_partition_key text;"
        blob = git_blob_oid(sql)
        violations = reviewed_result_gc_migration_violations(
            path=RESULT_GC_MIGRATION_PATH,
            git_blob=blob,
            sql=sql,
        )
        self.assertIn("result-gc:git-blob-differs", violations)
        self.assertIn("result-gc:normalized-ast-differs", violations)
        self.assertNotIn("result-gc:review-receipt-not-configured", violations)
        pre_ddl = pre_ddl_migration_violations(
            path=RESULT_GC_MIGRATION_PATH,
            git_blob=blob,
            sql=sql,
            allowlist=[
                {
                    "path": RESULT_GC_MIGRATION_PATH,
                    "gitBlob": blob,
                    "classification": RESULT_GC_CLASSIFICATION,
                }
            ],
        )
        self.assertIn("result-gc:git-blob-differs", pre_ddl)

    def test_timeout_zero_is_rejected(self) -> None:
        self.assert_semantic_violation(
            "BEGIN; SET LOCAL lock_timeout = '0'; COMMIT;",
            "result-gc:timeout-setting-differs",
        )

    def test_do_dynamic_execution_is_rejected(self) -> None:
        self.assert_semantic_violation(
            "DO $$ BEGIN EXECUTE 'CREATE ROLE attacker'; END $$;",
            "result-gc:do-dynamic-or-network-execution",
        )

    def test_role_preflight_requires_transitive_membership_and_admin_checks(self) -> None:
        sql = """
        DO $$
        BEGIN
          -- create role lca_worker_runtime
          -- create role lca_result_gc_executor
          -- nologin inherit nosuperuser nocreatedb nocreaterole nobypassrls
          -- rolsuper rolcreaterole rolcreatedb rolcanlogin rolbypassrls rolinherit
          -- direct-only pg_auth_members admin_option is insufficient
          -- pg_has_role(..., 'member') member = roleid = comment spoof
          NULL;
        END $$;
        """
        self.assert_semantic_violation(
            sql,
            "result-gc:role-preflight-or-membership-shape-differs",
        )

    def test_grant_role_to_authenticated_with_admin_option_is_rejected(self) -> None:
        self.assert_semantic_violation(
            "GRANT lca_result_gc_executor TO authenticated WITH ADMIN OPTION;",
            "result-gc:role-grant-admin-or-grantor",
        )
        self.assert_semantic_violation(
            "GRANT lca_result_gc_executor TO authenticated WITH ADMIN OPTION;",
            "result-gc:temporary-role-membership-shape-differs",
        )

    def test_broad_acl_and_wrong_owner_are_rejected(self) -> None:
        self.assert_semantic_violation(
            "GRANT ALL ON public.lca_results TO authenticated;",
            "result-gc:grant-to-browser-or-service-role",
        )
        self.assert_semantic_violation(
            "ALTER FUNCTION private.worker_lca_result_gc_claim_v1() OWNER TO authenticated;",
            "result-gc:owner-shape-differs:worker_lca_result_gc_claim_v1",
        )

    def test_control_must_remain_disabled_by_default(self) -> None:
        self.assert_semantic_violation(
            """
            INSERT INTO private.lca_result_gc_control
              (singleton, claims_enabled, reason)
            VALUES (true, true, 'enabled');
            """,
            "result-gc:control-not-disabled-by-default",
        )

    def test_security_definer_requires_explicit_safe_search_path_and_no_dynamic_sql(self) -> None:
        for unsafe_path in ("''", "pg_temp, pg_catalog", "public"):
            with self.subTest(unsafe_path=unsafe_path):
                self.assert_semantic_violation(
                    f"""
                    CREATE FUNCTION private.worker_lca_result_gc_claim_v1()
                    RETURNS void LANGUAGE plpgsql SECURITY DEFINER
                    SET search_path = {unsafe_path}
                    AS $$ BEGIN NULL; END $$;
                    """,
                    "result-gc:function-envelope-differs:worker_lca_result_gc_claim_v1",
                )
        self.assert_semantic_violation(
            """
            CREATE FUNCTION private.worker_lca_result_gc_claim_v1()
            RETURNS void LANGUAGE plpgsql SECURITY DEFINER
            SET search_path = public
            AS $$ BEGIN EXECUTE 'DELETE FROM public.lca_results'; END $$;
            """,
            "result-gc:function-envelope-differs:worker_lca_result_gc_claim_v1",
        )
        self.assert_semantic_violation(
            """
            CREATE FUNCTION private.worker_lca_result_gc_claim_v1()
            RETURNS void LANGUAGE plpgsql SECURITY DEFINER
            SET search_path = pg_catalog, pg_temp
            AS $$ BEGIN
              -- claims_enabled for update skip locked lca_result_gc_operations claim_token
              NULL;
            END $$;
            """,
            "result-gc:function-state-token-missing:worker_lca_result_gc_claim_v1:claims_enabled",
        )

    def test_trigger_after_or_statement_level_is_rejected(self) -> None:
        self.assert_semantic_violation(
            """
            CREATE TRIGGER lca_results_gc_write_fence
            AFTER DELETE ON public.lca_results
            FOR EACH STATEMENT EXECUTE FUNCTION private.lca_result_gc_guard_result_write();
            """,
            "result-gc:trigger-shape-differs:lca_results_gc_write_fence",
        )

    def test_index_expression_or_predicate_function_is_rejected(self) -> None:
        self.assert_semantic_violation(
            """
            CREATE UNIQUE INDEX lca_results_gc_locator_uidx
            ON public.lca_results ((lower(artifact_url)))
            WHERE lower(retention_partition_key) IS NOT NULL;
            """,
            "result-gc:index-shape-differs:lca_results_gc_locator_uidx",
        )

    def test_check_constraint_function_or_validation_is_rejected(self) -> None:
        self.assert_semantic_violation(
            """
            ALTER TABLE public.lca_results
            ADD CONSTRAINT lca_results_retention_partition_key_chk
            CHECK (lower(retention_partition_key) ~ '^[0-9a-f]{64}$');
            """,
            "result-gc:retention-check-shape-differs",
        )

    def test_table_rls_and_policy_contract_are_exact(self) -> None:
        self.assert_semantic_violation(
            "ALTER TABLE private.lca_result_gc_control ENABLE ROW LEVEL SECURITY;",
            "result-gc:private-table-rls-shape-differs",
        )
        self.assert_semantic_violation(
            """
            CREATE POLICY lca_results_gc_executor_update
            ON public.lca_results FOR UPDATE TO authenticated
            USING (true) WITH CHECK (true);
            """,
            "result-gc:policy-shape-differs:lca_results_gc_executor_update",
        )

    def test_exact_tables_and_seven_worker_routines_are_required(self) -> None:
        self.assert_semantic_violation(
            "CREATE TABLE private.lca_result_gc_control (singleton boolean);",
            "result-gc:created-table-shape-differs:lca_result_gc_control",
        )
        self.assert_semantic_violation(
            """
            CREATE FUNCTION private.worker_lca_result_gc_claim_v1()
            RETURNS void LANGUAGE plpgsql SECURITY DEFINER
            SET search_path = pg_catalog, pg_temp
            AS $$ BEGIN NULL; END $$;
            """,
            "result-gc:function-state-token-missing:worker_lca_result_gc_claim_v1:claims_enabled",
        )
        self.assert_semantic_violation(
            """
            CREATE FUNCTION private.worker_lca_result_gc_claim_v1()
            RETURNS void LANGUAGE plpgsql SECURITY DEFINER
            SET search_path = pg_catalog, pg_temp
            AS $$ BEGIN NULL; END $$;
            """,
            "result-gc:function-set-differs",
        )


class Issue398FrozenMigrationMutationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root = Path(__file__).resolve().parents[1]
        configured = os.environ.get("ISSUE_398_MIGRATION_FIXTURE")
        cls.path = (
            Path(configured)
            if configured
            else root / RESULT_GC_MIGRATION_PATH
        )
        if not cls.path.is_file():
            raise unittest.SkipTest(
                "Issue #398 frozen migration is supplied only after its own branch is present"
            )
        cls.sql = cls.path.read_text(encoding="utf-8")

    def mutate_literal(self, old: str, new: str, expected: str, *, count: int = 1) -> None:
        self.assertGreaterEqual(self.sql.count(old), count, old)
        changed = self.sql.replace(old, new, count)
        violations = semantic_violations(changed)
        self.assertIn(expected, violations, violations)

    def mutate_regex(self, pattern: str, replacement: str, expected: str) -> None:
        changed, count = re.subn(pattern, replacement, self.sql, count=1, flags=re.I | re.S)
        self.assertEqual(count, 1, pattern)
        violations = semantic_violations(changed)
        self.assertIn(expected, violations, violations)

    def test_exact_migration_framework_self_check(self) -> None:
        violations = semantic_violations(self.sql)
        self.assertEqual(violations, [], violations)

    def test_exact_review_receipts_and_pre_ddl_dispatch(self) -> None:
        self.assertEqual(git_blob_oid(self.sql), REVIEWED_GIT_BLOB)
        self.assertEqual(normalized_ast_sha256(self.sql), REVIEWED_AST_SHA256)
        self.assertEqual(
            reviewed_result_gc_migration_violations(
                path=RESULT_GC_MIGRATION_PATH,
                git_blob=REVIEWED_GIT_BLOB or "",
                sql=self.sql,
            ),
            [],
        )
        self.assertEqual(
            pre_ddl_migration_violations(
                path=RESULT_GC_MIGRATION_PATH,
                git_blob=REVIEWED_GIT_BLOB or "",
                sql=self.sql,
                allowlist=[
                    {
                        "path": RESULT_GC_MIGRATION_PATH,
                        "gitBlob": REVIEWED_GIT_BLOB,
                        "classification": RESULT_GC_CLASSIFICATION,
                    }
                ],
            ),
            [],
        )

    def test_exact_receipts_fail_closed_on_path_blob_or_source_drift(self) -> None:
        cases = (
            (
                "supabase/migrations/20260802201934_wrong.sql",
                REVIEWED_GIT_BLOB or "",
                self.sql,
                "result-gc:path-differs",
            ),
            (
                RESULT_GC_MIGRATION_PATH,
                "0" * 40,
                self.sql,
                "result-gc:git-blob-differs",
            ),
            (
                RESULT_GC_MIGRATION_PATH,
                REVIEWED_GIT_BLOB or "",
                self.sql + "\n",
                "result-gc:git-blob-differs",
            ),
        )
        for path, blob, sql, expected in cases:
            with self.subTest(expected=expected):
                violations = reviewed_result_gc_migration_violations(
                    path=path,
                    git_blob=blob,
                    sql=sql,
                )
                self.assertIn(expected, violations, violations)

    def test_timeout_and_do_injection_mutations(self) -> None:
        self.mutate_literal(
            "set local lock_timeout = '5s';",
            "set local lock_timeout = '0';",
            "result-gc:timeout-setting-differs",
        )
        self.mutate_regex(
            r"(do \$roles\$.*?)(\nend\n\$roles\$;)",
            r"\1\n  execute 'create role attacker';\2",
            "result-gc:do-dynamic-or-network-execution",
        )

    def test_role_membership_admin_and_any_edge_mutations(self) -> None:
        self.mutate_literal(
            "pg_has_role",
            "role_membership_check_disabled",
            "result-gc:role-preflight-or-membership-shape-differs",
            count=self.sql.count("pg_has_role"),
        )
        self.mutate_literal(
            "admin_option",
            "membership_admin_flag_disabled",
            "result-gc:role-preflight-or-membership-shape-differs",
            count=self.sql.count("admin_option"),
        )
        self.mutate_literal(
            "roleid =",
            "roleid <>",
            "result-gc:role-preflight-or-membership-shape-differs",
            count=self.sql.count("roleid ="),
        )
        self.mutate_literal(
            "inherit_option",
            "creator_inherit_flag",
            "result-gc:role-preflight-or-membership-shape-differs",
            count=self.sql.count("inherit_option"),
        )
        self.mutate_literal(
            "v_creator_edge_count <> 2",
            "v_creator_edge_count <> 1",
            "result-gc:role-preflight-or-membership-shape-differs",
        )
        self.mutate_regex(
            r"grant lca_result_gc_executor to postgres;",
            "grant lca_result_gc_executor to authenticated with admin option;",
            "result-gc:role-grant-admin-or-grantor",
        )

    def test_control_acl_policy_and_owner_mutations(self) -> None:
        self.mutate_regex(
            r"values\s*\(\s*true\s*,\s*false\s*,",
            "values (true, true,",
            "result-gc:control-not-disabled-by-default",
        )
        self.mutate_regex(
            r"(create policy lca_results_gc_executor_update.*?to )lca_result_gc_executor",
            r"\1authenticated",
            "result-gc:policy-shape-differs:lca_results_gc_executor_update",
        )
        self.mutate_regex(
            r"(create policy lca_results_gc_executor_update.*?using \()true(\))",
            r"\1false\2",
            "result-gc:policy-shape-differs:lca_results_gc_executor_update",
        )
        self.mutate_regex(
            r"alter function private\.worker_lca_result_gc_claim_v1(.*?) owner to lca_result_gc_executor;",
            r"alter function private.worker_lca_result_gc_claim_v1\1 owner to authenticated;",
            "result-gc:owner-shape-differs:worker_lca_result_gc_claim_v1",
        )
        self.mutate_literal(
            "notify pgrst, 'reload schema';",
            "grant all on public.lca_results to authenticated;\nnotify pgrst, 'reload schema';",
            "result-gc:grant-to-browser-or-service-role",
        )

    def test_function_envelope_and_state_machine_mutations(self) -> None:
        self.mutate_regex(
            r"(create function private\.worker_lca_result_gc_claim_v1.*?set search_path = )pg_catalog, pg_temp",
            r"\1public",
            "result-gc:function-envelope-differs:worker_lca_result_gc_claim_v1",
        )
        self.mutate_literal(
            "claims_enabled",
            "claims_disabled",
            "result-gc:function-state-token-missing:worker_lca_result_gc_claim_v1:claims_enabled",
            count=self.sql.count("claims_enabled"),
        )
        self.mutate_literal(
            "deleting",
            "erasing",
            "result-gc:function-state-token-missing:worker_lca_result_gc_fence_v1:deleting",
            count=self.sql.count("deleting"),
        )
        self.mutate_literal(
            "missing",
            "absent",
            "result-gc:function-state-token-missing:worker_lca_result_gc_finalize_v1:missing",
            count=self.sql.count("missing"),
        )

    def test_trigger_index_and_constraint_mutations(self) -> None:
        self.mutate_regex(
            r"create trigger lca_results_gc_write_fence\s+before update or delete",
            "create trigger lca_results_gc_write_fence after update or delete",
            "result-gc:trigger-shape-differs:lca_results_gc_write_fence",
        )
        self.mutate_regex(
            r"(create trigger lca_results_gc_write_fence.*?for each )row",
            r"\1statement",
            "result-gc:trigger-shape-differs:lca_results_gc_write_fence",
        )
        self.mutate_literal(
            "on public.lca_results (artifact_url)",
            "on public.lca_results ((lower(artifact_url)))",
            "result-gc:index-shape-differs:lca_results_gc_locator_uidx",
        )
        self.mutate_regex(
            r"(constraint lca_results_retention_partition_key_chk check \()(.*?)(\) not valid;)",
            r"\1lower(retention_partition_key) is not null\3",
            "result-gc:retention-check-shape-differs",
        )

    def test_function_and_table_set_mutations(self) -> None:
        self.mutate_literal(
            "worker_lca_result_gc_fail_v1",
            "worker_lca_result_gc_fail_v2",
            "result-gc:function-set-differs",
            count=self.sql.count("worker_lca_result_gc_fail_v1"),
        )
        self.mutate_literal(
            "force row level security",
            "no force row level security",
            "result-gc:private-table-rls-shape-differs",
        )


if __name__ == "__main__":
    unittest.main()
