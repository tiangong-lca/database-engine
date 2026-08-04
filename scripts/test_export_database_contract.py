#!/usr/bin/env python3
"""Fail-closed tests for deterministic database catalog generation."""

from __future__ import annotations

import unittest

from scripts.export_database_contract import validate_generation_guard


class DatabaseCatalogGenerationGuardTest(unittest.TestCase):
    def test_clean_issue_339_posture_is_accepted(self) -> None:
        validate_generation_guard({
            "serviceRoleMaintain": [],
            "forbiddenInternalExecute": [],
            "forbiddenLifecycleExecute": [],
        })

    def test_each_polluted_surface_is_rejected(self) -> None:
        clean = {
            "serviceRoleMaintain": [],
            "forbiddenInternalExecute": [],
            "forbiddenLifecycleExecute": [],
        }
        for key in clean:
            with self.subTest(key=key), self.assertRaisesRegex(SystemExit, key):
                polluted = dict(clean)
                polluted[key] = [{"unexpected": "grant"}]
                validate_generation_guard(polluted)

    def test_unexpected_guard_shape_is_rejected(self) -> None:
        with self.assertRaisesRegex(SystemExit, "unexpected shape"):
            validate_generation_guard({"serviceRoleMaintain": []})


if __name__ == "__main__":
    unittest.main()
