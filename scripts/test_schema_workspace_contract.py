#!/usr/bin/env python3
"""Offline tests for application-schema export and workspace defaults."""

from __future__ import annotations

import unittest

from scripts import _db_workflow
from scripts import export_database_contract


class SchemaWorkspaceContractTest(unittest.TestCase):
    def test_default_workspace_export_covers_all_application_schemas(self) -> None:
        self.assertEqual(
            _db_workflow.DEFAULT_SCHEMA_NAMES,
            ("public", "api", "private", "util", "archive"),
        )
        self.assertEqual(
            _db_workflow.resolve_schema_list(None),
            ["public", "api", "private", "util", "archive"],
        )

    def test_catalog_export_includes_schema_owner_and_acl(self) -> None:
        query = export_database_contract.QUERY
        self.assertIn("'schemas'", query)
        self.assertIn("owner.rolname owner", query)
        self.assertIn("n.nspacl::text", query)
        self.assertIn("('public','api','private','util','archive')", query)


if __name__ == "__main__":
    unittest.main()
