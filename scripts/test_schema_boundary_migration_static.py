#!/usr/bin/env python3
"""Static safety contract for the Issue #354 migration and rollback."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "supabase/migrations/20260801042547_issue_354_schema_view_compat_expand.sql"
ROLLBACK = ROOT / "supabase/operator/issue_354_restore_schema_boundary.sql"


class SchemaBoundaryMigrationStaticTest(unittest.TestCase):
    def test_migration_uses_oid_preserving_moves_and_explicit_projections(self) -> None:
        sql = MIGRATION.read_text(encoding="utf-8").lower()
        self.assertEqual(sql.count("alter view public."), 10)
        self.assertEqual(sql.count(" set schema "), 5)
        self.assertNotRegex(sql, r"select\s+\*")
        self.assertNotIn(" cascade", sql)
        self.assertNotIn("security definer", sql)

    def test_rollback_is_restrict_only_and_does_not_reopen_schema_defaults(self) -> None:
        sql = ROLLBACK.read_text(encoding="utf-8").lower()
        self.assertEqual(len(re.findall(r"drop view public\..* restrict;", sql)), 5)
        self.assertNotIn(" cascade", sql)
        self.assertNotIn("alter default privileges", sql)
        self.assertNotIn("drop schema", sql)


if __name__ == "__main__":
    unittest.main()
