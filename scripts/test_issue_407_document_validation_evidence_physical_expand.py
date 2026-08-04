#!/usr/bin/env python3
"""Static fail-closed contract for Issue #407 Phase B physical Expand."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from scripts.issue_390_pre_ddl_gate import (
    reviewed_document_evidence_physical_migration_violations,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "supabase/migrations/20260804100000_issue_407_document_validation_evidence_physical_expand.sql"
ROLLBACK = ROOT / "supabase/operator/20260804_issue_407_document_validation_evidence_physical_expand_rollback.sql"
ROLLFORWARD = ROOT / "supabase/operator/20260804_issue_407_document_validation_evidence_physical_expand_rollforward.sql"
RUNTIME = ROOT / "scripts/test_issue_407_document_validation_evidence_runtime.py"
WORKFLOWS = (
    ROOT / ".github/workflows/database-validation.yml",
    ROOT / ".github/workflows/supabase-dev.yml",
)


def normalized(path: Path) -> str:
    return re.sub(r"\s+", " ", path.read_text(encoding="utf-8").lower()).strip()


class Issue407DocumentEvidencePhysicalExpandTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sql = normalized(MIGRATION)
        cls.rollback = normalized(ROLLBACK)

    def test_uses_metadata_move_and_one_invoker_compatibility_view(self) -> None:
        self.assertEqual(
            self.sql.count(
                "alter table public.lcia_document_validation_evidence set schema private"
            ),
            1,
        )
        self.assertIn(
            "create or replace view public.lcia_document_validation_evidence with (security_invoker = true)",
            self.sql,
        )
        self.assertNotIn("create table private.lcia_document_validation_evidence", self.sql)
        self.assertNotIn("insert into private.lcia_document_validation_evidence select", self.sql)

    def test_lock_precedes_identity_and_row_snapshot(self) -> None:
        lock = self.sql.index(
            "lock table public.lcia_document_validation_evidence in access exclusive mode"
        )
        snapshot = self.sql.index(
            "create temporary table issue_407_phase_b_relation_before"
        )
        move = self.sql.index(
            "alter table public.lcia_document_validation_evidence set schema private"
        )
        self.assertLess(lock, snapshot)
        self.assertLess(snapshot, move)

    def test_preflight_freezes_full_relation_and_routine_catalogs(self) -> None:
        for field in (
            "relation.relpersistence",
            "relation.relreplident",
            "relation.reloptions",
            "attribute.attstattarget",
            "attribute.attmissingval",
            "constraint_row.convalidated",
            "index_row.indisready",
            "trigger_row.tgisinternal",
            "policy.polroles",
            "publication.puballtables",
        ):
            self.assertIn(field, self.sql)
        for digest in (
            "13987c5504a3eae73c07533b3a3e39db",
            "4633234c541c50b1dd6bcc7dbb2e57d5",
            "bd277cd343a10462fc536a64390459c5",
            "2759f5215c8dd4b253db2ed2264cc8ab",
            "910ebb77d0b5335d5138dfe09a38f881",
            "72eb5d600ed803ee77a474c5ef8baf06",
            "6f6fb65152a4125c25babc79397d1626",
            "efd249089fa40ea58fe8efe3e1e894b0",
        ):
            self.assertIn(digest, self.sql)
        self.assertIn(
            "private canonical routine definition or acl drifted", self.sql
        )
        self.assertIn(
            "public compatibility routine definition or acl drifted", self.sql
        )

    def test_rollback_is_locked_fail_closed_and_has_explicit_rollforward(self) -> None:
        lock = self.rollback.index(
            "lock table private.lcia_document_validation_evidence in access exclusive mode"
        )
        snapshot = self.rollback.index(
            "create temporary table issue_407_phase_b_rollback_before"
        )
        self.assertLess(lock, snapshot)
        self.assertIn("rollback refuses relation catalog drift", self.rollback)
        self.assertIn("rollback refuses canonical routine drift", self.rollback)
        self.assertIn(
            "rollback refuses public compatibility routine drift", self.rollback
        )
        rollforward = ROLLFORWARD.read_text(encoding="utf-8")
        self.assertIn("\\ir ../migrations/20260804100000_", rollforward)
        self.assertIn("ordinary supabase runner will not replay", rollforward.lower())

    def test_runtime_and_ci_are_bound_to_phase_b(self) -> None:
        source = RUNTIME.read_text(encoding="utf-8")
        self.assertIn(MIGRATION.name, source)
        self.assertIn(ROLLBACK.name, source)
        self.assertIn(ROLLFORWARD.name, source)
        self.assertIn('default="20260803163000"', source)
        self.assertIn('default="20260804100000"', source)
        for workflow in WORKFLOWS:
            text = workflow.read_text(encoding="utf-8")
            self.assertIn("06ab3e6b017e732b15d1edd9c7ef8f4a35139187", text)
            self.assertIn(
                "scripts.test_issue_407_document_validation_evidence_physical_expand",
                text,
            )

    def test_reviewed_ast_gate_binds_exact_blob_and_physical_shape(self) -> None:
        source = MIGRATION.read_text(encoding="utf-8")
        blob = "534f7ddb13ff52a62e949e8f098859af038c5d4c"
        self.assertEqual(
            reviewed_document_evidence_physical_migration_violations(
                path=MIGRATION.relative_to(ROOT).as_posix(),
                git_blob=blob,
                sql=source,
            ),
            [],
        )
        self.assertNotEqual(
            reviewed_document_evidence_physical_migration_violations(
                path=MIGRATION.relative_to(ROOT).as_posix(),
                git_blob="0" * 40,
                sql=source,
            ),
            [],
        )


if __name__ == "__main__":
    unittest.main()
