#!/usr/bin/env python3
"""Fail-closed tests for deterministic database catalog generation."""

from __future__ import annotations

import unittest

from scripts.export_database_contract import QUERY, validate_generation_guard


class DatabaseCatalogGenerationGuardTest(unittest.TestCase):
    def test_effective_default_privilege_order_includes_grantability(self) -> None:
        self.assertIn(
            "order by owner_name,schema_name,object_type,grantee,privilege_type,is_grantable",
            QUERY,
        )

    def test_clean_issue_339_posture_is_accepted(self) -> None:
        validate_generation_guard({
            "serviceRoleMaintain": [],
            "forbiddenInternalExecute": [],
            "forbiddenLifecycleExecute": [],
            "repoOwnerDefaultPrivilegeResidue": [],
            "platformOwnerDefaultPrivilegeResidue": [{"issue": 352}],
            "platformOwnerBlocker": "tiangong-lca/database-engine#352",
        })

    def test_each_polluted_surface_is_rejected(self) -> None:
        clean = {
            "serviceRoleMaintain": [],
            "forbiddenInternalExecute": [],
            "forbiddenLifecycleExecute": [],
            "repoOwnerDefaultPrivilegeResidue": [],
            "platformOwnerDefaultPrivilegeResidue": [{"issue": 352}],
            "platformOwnerBlocker": "tiangong-lca/database-engine#352",
        }
        for key in (
            "serviceRoleMaintain",
            "forbiddenInternalExecute",
            "forbiddenLifecycleExecute",
            "repoOwnerDefaultPrivilegeResidue",
        ):
            with self.subTest(key=key), self.assertRaisesRegex(SystemExit, key):
                polluted = dict(clean)
                polluted[key] = [{"unexpected": "grant"}]
                validate_generation_guard(polluted)

    def test_platform_residue_is_retained_as_issue_352_evidence(self) -> None:
        validate_generation_guard({
            "serviceRoleMaintain": [],
            "forbiddenInternalExecute": [],
            "forbiddenLifecycleExecute": [],
            "repoOwnerDefaultPrivilegeResidue": [],
            "platformOwnerDefaultPrivilegeResidue": [{"grantee": "PUBLIC"}],
            "platformOwnerBlocker": "tiangong-lca/database-engine#352",
        })

    def test_platform_blocker_cannot_be_omitted_or_relabelled(self) -> None:
        with self.assertRaisesRegex(SystemExit, "platform-owner blocker"):
            validate_generation_guard({
                "serviceRoleMaintain": [],
                "forbiddenInternalExecute": [],
                "forbiddenLifecycleExecute": [],
                "repoOwnerDefaultPrivilegeResidue": [],
                "platformOwnerDefaultPrivilegeResidue": [],
                "platformOwnerBlocker": "resolved",
            })

    def test_unexpected_guard_shape_is_rejected(self) -> None:
        with self.assertRaisesRegex(SystemExit, "unexpected shape"):
            validate_generation_guard({"serviceRoleMaintain": []})


if __name__ == "__main__":
    unittest.main()
