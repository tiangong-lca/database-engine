#!/usr/bin/env python3
"""Static fail-closed contract for Issue #414 snapshot GC physical Expand."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from scripts.issue_390_pre_ddl_gate import (
    reviewed_snapshot_gc_physical_migration_violations,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "supabase/migrations/20260804123000_issue_414_snapshot_gc_audit_physical_expand.sql"
ROLLBACK = ROOT / "supabase/operator/20260804_issue_414_snapshot_gc_audit_physical_expand_rollback.sql"
ROLLFORWARD = ROOT / "supabase/operator/20260804_issue_414_snapshot_gc_audit_physical_expand_rollforward.sql"
RUNTIME = ROOT / "scripts/test_issue_414_snapshot_gc_audit_runtime.py"
WORKFLOWS = (
    ROOT / ".github/workflows/database-validation.yml",
    ROOT / ".github/workflows/supabase-dev.yml",
)


def normalized(path: Path) -> str:
    return re.sub(r"\s+", " ", path.read_text(encoding="utf-8").lower()).strip()


class Issue414SnapshotGcPhysicalExpandTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sql = normalized(MIGRATION)
        cls.rollback = normalized(ROLLBACK)

    def test_moves_both_tables_by_oid_and_exposes_invoker_views(self) -> None:
        for relation in ("lca_snapshot_gc_runs", "lca_snapshot_gc_run_items"):
            self.assertEqual(
                self.sql.count(
                    f"alter table public.{relation} set schema private"
                ),
                1,
            )
            self.assertIn(
                f"create or replace view public.{relation} "
                "with (security_invoker = true)",
                self.sql,
            )
            self.assertNotIn(f"create table private.{relation}", self.sql)
            self.assertNotIn(f"insert into private.{relation} select", self.sql)

    def test_lock_and_snapshot_precede_child_then_parent_move(self) -> None:
        lock = self.sql.index(
            "lock table public.lca_snapshot_gc_runs, "
            "public.lca_snapshot_gc_run_items in access exclusive mode"
        )
        snapshot = self.sql.index("create temporary table issue_414_before")
        move_items = self.sql.index(
            "alter table public.lca_snapshot_gc_run_items set schema private"
        )
        move_runs = self.sql.index(
            "alter table public.lca_snapshot_gc_runs set schema private"
        )
        self.assertLess(lock, snapshot)
        self.assertLess(snapshot, move_items)
        self.assertLess(move_items, move_runs)

    def test_fingerprint_is_semantic_and_reset_stable(self) -> None:
        for field in (
            "relation.relpersistence",
            "relation.relreplident",
            "attribute.attstattarget",
            "constraint_row.convalidated",
            "index_row.indisready",
            "trigger_row.tgisinternal",
            "publication.puballtables",
        ):
            self.assertIn(field, self.sql)
        self.assertIn("pg_catalog.pg_get_userbyid(role_oid)", self.sql)
        self.assertNotIn("policy.polroles::text", self.sql)
        for digest in (
            "ad841baccb43a081d8d4bfd0c5599d4f",
            "43e5174ff4856098340d2bc5e638b40d",
            "eddc56d22585cb6af9562b551afb06e8",
            "8b472ff342e5d2ad59a59dfda0df884e",
        ):
            self.assertIn(digest, self.sql)

    def test_rewrites_snapshot_helpers_to_private_canonical_tables(self) -> None:
        for relation in (
            "lca_active_snapshots",
            "lca_network_snapshots",
            "lca_snapshot_artifacts",
        ):
            self.assertIn(f"private.{relation}", self.sql)
        self.assertIn("util routine definition or acl drifted", self.sql)
        self.assertIn("util routine rewrite drifted", self.sql)

    def test_rollback_is_locked_and_rollforward_is_explicit(self) -> None:
        lock = self.rollback.index(
            "lock table private.lca_snapshot_gc_runs, "
            "private.lca_snapshot_gc_run_items in access exclusive mode"
        )
        snapshot = self.rollback.index(
            "create temporary table issue_414_rollback_before"
        )
        self.assertLess(lock, snapshot)
        self.assertIn("rollback preflight catalog drifted", self.rollback)
        rollforward = normalized(ROLLFORWARD)
        self.assertIn("\\ir ../migrations/20260804123000_", rollforward)
        self.assertIn("migration replay will not execute", rollforward.lower())

    def test_runtime_and_both_ci_workflows_are_bound_to_issue_414(self) -> None:
        runtime = RUNTIME.read_text(encoding="utf-8")
        for path in (MIGRATION, ROLLBACK, ROLLFORWARD):
            self.assertIn(path.name, runtime)
        self.assertIn('head != "20260804123000"', runtime)
        self.assertIn("max_workers=8", runtime)
        self.assertIn("generate_series(1, 3205)", runtime)
        for workflow in WORKFLOWS:
            source = workflow.read_text(encoding="utf-8")
            self.assertIn(
                "scripts.test_issue_414_snapshot_gc_audit_physical_expand",
                source,
            )
            self.assertIn(
                "scripts/test_issue_414_snapshot_gc_audit_runtime.py",
                source,
            )

    def test_reviewed_ast_gate_binds_exact_blob(self) -> None:
        source = MIGRATION.read_text(encoding="utf-8")
        blob = "12a6580738149ebd5f447b3f9182b474c9a0bc72"
        self.assertEqual(
            reviewed_snapshot_gc_physical_migration_violations(
                path=MIGRATION.relative_to(ROOT).as_posix(),
                git_blob=blob,
                sql=source,
            ),
            [],
        )
        self.assertNotEqual(
            reviewed_snapshot_gc_physical_migration_violations(
                path=MIGRATION.relative_to(ROOT).as_posix(),
                git_blob="0" * 40,
                sql=source,
            ),
            [],
        )


if __name__ == "__main__":
    unittest.main()
