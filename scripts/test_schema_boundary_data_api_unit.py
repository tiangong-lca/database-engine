#!/usr/bin/env python3
"""Offline tests for the Issue #354 PostgREST boundary probe."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_schema_boundary_data_api as target


class SchemaBoundaryDataApiTest(unittest.TestCase):
    def test_profile_is_explicit_and_legacy_jwt_is_bearer(self) -> None:
        headers = target.request_headers("eyJservice-role", profile="api")
        self.assertEqual(headers["Accept-Profile"], "api")
        self.assertEqual(headers["Authorization"], "Bearer eyJservice-role")

    def test_opaque_key_is_not_bearer(self) -> None:
        headers = target.request_headers("sb_secret_opaque", profile="public")
        self.assertNotIn("Authorization", headers)

    def test_relation_url_uses_explicit_projection_and_limit(self) -> None:
        url = target.relation_url("http://localhost/rest/v1", "example", "id,name")
        self.assertEqual(url, "http://localhost/rest/v1/example?select=id%2Cname&limit=1")


if __name__ == "__main__":
    unittest.main()
