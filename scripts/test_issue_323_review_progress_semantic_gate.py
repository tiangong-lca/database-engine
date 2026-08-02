#!/usr/bin/env python3
"""Mutation-negative tests for the Issue #323 migration semantic gate."""

from __future__ import annotations

import unittest
from pathlib import Path

from scripts.issue_323_review_progress_semantic_gate import (
    REVIEW_PROGRESS_CLASSIFICATION,
    REVIEW_PROGRESS_MIGRATION_PATH,
    REVIEWED_AST_SHA256,
    REVIEWED_GIT_BLOB,
    git_blob_oid,
    normalized_ast_sha256,
    reviewed_review_progress_migration_violations,
    semantic_violations,
)
from scripts.issue_390_pre_ddl_gate import (
    pre_ddl_migration_violations,
    pre_ddl_sql_signals,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / REVIEW_PROGRESS_MIGRATION_PATH


class Issue323ReviewProgressSemanticGateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sql = MIGRATION.read_text(encoding="utf-8")

    def mutated(self, old: str, new: str) -> str:
        self.assertIn(old, self.sql)
        return self.sql.replace(old, new, 1)

    def assert_semantic_violation(
        self, old: str, new: str, expected: str
    ) -> None:
        violations = semantic_violations(self.mutated(old, new))
        self.assertIn(expected, violations, violations)

    def test_exact_reviewed_migration_passes(self) -> None:
        self.assertEqual(git_blob_oid(self.sql), REVIEWED_GIT_BLOB)
        self.assertEqual(normalized_ast_sha256(self.sql), REVIEWED_AST_SHA256)
        self.assertEqual(semantic_violations(self.sql), [])
        self.assertEqual(
            reviewed_review_progress_migration_violations(
                path=REVIEW_PROGRESS_MIGRATION_PATH,
                git_blob=REVIEWED_GIT_BLOB,
                sql=self.sql,
            ),
            [],
        )
        self.assertEqual(
            pre_ddl_migration_violations(
                path=REVIEW_PROGRESS_MIGRATION_PATH,
                git_blob=REVIEWED_GIT_BLOB,
                sql=self.sql,
                allowlist=[
                    {
                        "path": REVIEW_PROGRESS_MIGRATION_PATH,
                        "gitBlob": REVIEWED_GIT_BLOB,
                        "classification": REVIEW_PROGRESS_CLASSIFICATION,
                    }
                ],
            ),
            [],
        )

    def test_generic_hard_denies_remain_and_require_exact_classification(self) -> None:
        self.assertEqual(
            pre_ddl_sql_signals(self.sql),
            [
                "hard-deny:exposed-security-definer-function",
                "hard-deny:opaque-DoStmt",
                "hard-deny:opaque-procedural-function-body",
                "hard-deny:protected-runtime-role-membership-change",
                "hard-deny:unapproved-object-owner",
                "hard-deny:unclassified-statement:DoStmt",
                "hard-deny:untrusted-search-path",
            ],
        )
        for allowlist in (
            [],
            [
                {
                    "path": REVIEW_PROGRESS_MIGRATION_PATH,
                    "gitBlob": REVIEWED_GIT_BLOB,
                    "classification": "wrong-classification",
                }
            ],
        ):
            self.assertEqual(
                pre_ddl_migration_violations(
                    path=REVIEW_PROGRESS_MIGRATION_PATH,
                    git_blob=REVIEWED_GIT_BLOB,
                    sql=self.sql,
                    allowlist=allowlist,
                ),
                ["review-progress:exact-allowlist-entry-required"],
            )

    def test_blob_and_ast_receipts_reject_any_source_change(self) -> None:
        changed = self.mutated(
            "Current Reference Review child rows",
            "Changed Reference Review child rows",
        )
        violations = reviewed_review_progress_migration_violations(
            path=REVIEW_PROGRESS_MIGRATION_PATH,
            git_blob=git_blob_oid(changed),
            sql=changed,
        )
        self.assertIn("review-progress:git-blob-differs", violations)
        self.assertIn("review-progress:normalized-ast-differs", violations)

    def test_role_must_remain_non_inheriting(self) -> None:
        self.assert_semantic_violation(
            "nologin noinherit nosuperuser",
            "nologin inherit nosuperuser",
            "review-progress:role-create-contract-differs",
        )

    def test_do_blocks_reject_dynamic_execution(self) -> None:
        self.assert_semantic_violation(
            "begin\n  if not exists (",
            "begin\n  execute 'select 1';\n  if not exists (",
            "review-progress:do-dynamic-or-network-execution",
        )

    def test_relation_acl_cannot_grow_or_gain_write_access(self) -> None:
        self.assert_semantic_violation(
            "grant select on public.reviews, public.comments",
            "grant update on public.reviews, public.comments",
            "review-progress:acl-shape-differs",
        )
        self.assert_semantic_violation(
            "grant select on public.reviews, public.comments",
            "grant select on public.reviews, public.comments, public.notifications",
            "review-progress:acl-shape-differs",
        )

    def test_temporary_postgres_membership_must_be_revoked_by_grantor(self) -> None:
        self.assert_semantic_violation(
            "revoke review_progress_executor from postgres granted by current_user;",
            "",
            "review-progress:temporary-role-membership-shape-differs",
        )

    def test_owner_cannot_return_to_postgres(self) -> None:
        self.assert_semantic_violation(
            "owner to review_progress_executor;",
            "owner to postgres;",
            "review-progress:owner-shape-differs",
        )

    def test_browser_acl_cannot_expand(self) -> None:
        self.assert_semantic_violation(
            "from public, anon;",
            "from public;",
            "review-progress:acl-shape-differs",
        )

    def test_search_path_must_remain_exactly_trusted(self) -> None:
        self.assert_semantic_violation(
            "set search_path = pg_catalog, pg_temp",
            "set search_path = public, pg_temp",
            "review-progress:function-envelope-differs",
        )

    def test_rpc_signature_is_immutable(self) -> None:
        self.assert_semantic_violation(
            "p_root_review_id pg_catalog.uuid",
            "p_root_review_id pg_catalog.text",
            "review-progress:function-signature-differs",
        )

    def test_role_error_contract_is_immutable(self) -> None:
        self.assert_semantic_violation(
            "message = 'REVIEW_ROLE_REQUIRED'",
            "message = 'ACCESS_DENIED'",
            "review-progress:function-body-contract-differs",
        )

    def test_function_cannot_reference_an_extra_public_relation(self) -> None:
        self.assert_semantic_violation(
            "from public.comments as completed_comment",
            "from public.notifications as completed_comment",
            "review-progress:function-public-reference-set-differs",
        )


if __name__ == "__main__":
    unittest.main()
