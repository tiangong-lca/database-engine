#!/usr/bin/env python3
"""Mutation-negative tests for the Issue #323 notification semantic gate."""

from __future__ import annotations

import unittest
from pathlib import Path

from scripts.issue_323_review_notification_semantic_gate import (
    REVIEW_NOTIFICATION_CLASSIFICATION,
    REVIEW_NOTIFICATION_MIGRATION_PATH,
    REVIEWED_AST_SHA256,
    REVIEWED_GIT_BLOB,
    reviewed_review_notification_migration_violations,
    semantic_violations,
)
from scripts.issue_323_review_progress_semantic_gate import (
    git_blob_oid,
    normalized_ast_sha256,
)
from scripts.issue_390_pre_ddl_gate import (
    pre_ddl_migration_violations,
    pre_ddl_sql_signals,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / REVIEW_NOTIFICATION_MIGRATION_PATH


class Issue323ReviewNotificationSemanticGateTest(unittest.TestCase):
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
            reviewed_review_notification_migration_violations(
                path=REVIEW_NOTIFICATION_MIGRATION_PATH,
                git_blob=REVIEWED_GIT_BLOB,
                sql=self.sql,
            ),
            [],
        )
        self.assertEqual(
            pre_ddl_migration_violations(
                path=REVIEW_NOTIFICATION_MIGRATION_PATH,
                git_blob=REVIEWED_GIT_BLOB,
                sql=self.sql,
                allowlist=[
                    {
                        "path": REVIEW_NOTIFICATION_MIGRATION_PATH,
                        "gitBlob": REVIEWED_GIT_BLOB,
                        "classification": REVIEW_NOTIFICATION_CLASSIFICATION,
                    }
                ],
            ),
            [],
        )

    def test_generic_hard_denies_remain_and_require_exact_classification(self) -> None:
        self.assertEqual(
            pre_ddl_sql_signals(self.sql),
            [
                "hard-deny:data-executing-statement:IndexStmt",
                "hard-deny:exposed-function-signature-type",
                "hard-deny:exposed-security-definer-function",
                "hard-deny:opaque-procedural-function-body",
                "hard-deny:untrusted-search-path",
            ],
        )
        for allowlist in (
            [],
            [
                {
                    "path": REVIEW_NOTIFICATION_MIGRATION_PATH,
                    "gitBlob": REVIEWED_GIT_BLOB,
                    "classification": "wrong-classification",
                }
            ],
        ):
            self.assertEqual(
                pre_ddl_migration_violations(
                    path=REVIEW_NOTIFICATION_MIGRATION_PATH,
                    git_blob=REVIEWED_GIT_BLOB,
                    sql=self.sql,
                    allowlist=allowlist,
                ),
                ["review-notification:exact-allowlist-entry-required"],
            )

    def test_blob_and_ast_receipts_reject_any_source_change(self) -> None:
        changed = self.mutated("Authentication required", "Authentication is required")
        violations = reviewed_review_notification_migration_violations(
            path=REVIEW_NOTIFICATION_MIGRATION_PATH,
            git_blob=git_blob_oid(changed),
            sql=changed,
        )
        self.assertIn("review-notification:git-blob-differs", violations)
        self.assertIn("review-notification:normalized-ast-differs", violations)

    def test_index_must_remain_unique_and_exactly_scoped(self) -> None:
        self.assert_semantic_violation(
            "create unique index notifications_recipient_sender_type_dataset_uq",
            "create index notifications_recipient_sender_type_dataset_uq",
            "review-notification:index-shape-differs",
        )

    def test_index_predicate_cannot_include_event_key_rows(self) -> None:
        self.assert_semantic_violation(
            "where nullif(json->>'event_key', '') is null;",
            "where nullif(json->>'event_key', '') is not null;",
            "review-notification:index-predicate-differs",
        )

    def test_rpc_signature_is_immutable(self) -> None:
        self.assert_semantic_violation(
            "p_dataset_type text,",
            "p_dataset_type jsonb,",
            "review-notification:function-signature-differs",
        )

    def test_function_envelope_is_frozen_to_the_predecessor_posture(self) -> None:
        self.assert_semantic_violation(
            "set search_path = public, pg_temp",
            "set search_path = private, pg_temp",
            "review-notification:function-envelope-differs",
        )

    def test_function_rejects_dynamic_execution(self) -> None:
        self.assert_semantic_violation(
            "begin\n  if v_actor is null then",
            "begin\n  execute 'select 1';\n  if v_actor is null then",
            "review-notification:function-dynamic-or-network-execution",
        )

    def test_function_cannot_reference_an_extra_public_relation(self) -> None:
        self.assert_semantic_violation(
            "insert into public.notifications (",
            "insert into public.profiles (",
            "review-notification:function-public-reference-set-differs",
        )

    def test_legacy_conflict_predicate_is_required_in_the_function(self) -> None:
        self.assert_semantic_violation(
            ") where nullif(json->>'event_key', '') is null\n  do update",
            ")\n  do update",
            "review-notification:function-body-contract-differs",
        )

    def test_authorization_contract_is_immutable(self) -> None:
        self.assert_semantic_violation(
            "'code', 'RECIPIENT_NOT_TARGET_OWNER'",
            "'code', 'RECIPIENT_ALLOWED'",
            "review-notification:function-body-contract-differs",
        )

    def test_no_owner_or_acl_statement_can_be_added(self) -> None:
        changed = self.sql + "\nGRANT EXECUTE ON FUNCTION public.cmd_notification_send_validation_issue(uuid,text,uuid,text,text,text[],text[],integer,jsonb) TO anon;\n"
        violations = semantic_violations(changed)
        self.assertIn("review-notification:statement-sequence-differs", violations)
        self.assertIn("review-notification:statement-set-differs", violations)


if __name__ == "__main__":
    unittest.main()
