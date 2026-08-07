#!/usr/bin/env python3
"""Regression tests for repository-derived migration heads."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from resolve_migration_head import resolve_migration_head


class ResolveMigrationHeadTests(unittest.TestCase):
    def test_returns_latest_version_independent_of_filename_order(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            migrations = Path(directory)
            (migrations / "20260807103000_newer.sql").touch()
            (migrations / "20260806230500_older.sql").touch()

            self.assertEqual(resolve_migration_head(migrations), "20260807103000")

    def test_rejects_empty_migration_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "no SQL migrations"):
                resolve_migration_head(Path(directory))

    def test_rejects_malformed_sql_migration_name(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            migrations = Path(directory)
            (migrations / "latest_schema.sql").touch()

            with self.assertRaisesRegex(ValueError, "latest_schema.sql"):
                resolve_migration_head(migrations)

    def test_rejects_duplicate_migration_versions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            migrations = Path(directory)
            (migrations / "20260807103000_first.sql").touch()
            (migrations / "20260807103000_second.sql").touch()

            with self.assertRaisesRegex(ValueError, "duplicate migration versions"):
                resolve_migration_head(migrations)


if __name__ == "__main__":
    unittest.main()
