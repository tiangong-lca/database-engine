#!/usr/bin/env python3
"""Offline tests for the Issue #354 phase checker."""

from __future__ import annotations

import copy
import unittest

from scripts import schema_boundary_phase as checker


def fixture() -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    contract = checker.load_json(checker.CONTRACT)
    schemas = []
    for name, expected in contract["applicationSchemas"].items():
        privileges = {}
        for role in checker.ROLES:
            privileges[f"{role}Usage"] = role in expected["usageRoles"]
            privileges[f"{role}Create"] = False
        schemas.append({"name": name, "owner": "postgres", "privileges": privileges})
    relations = [
        {"schema": "public", "name": name, "kind": "r"}
        for name in contract["corePublicTables"]
    ]
    for mapping in contract["movedViews"]:
        base = {
            "name": mapping["name"], "kind": "v", "owner": "postgres",
            "security_invoker": True, "service_role_select": True,
            "anon_select": False, "authenticated_select": False,
            "executor_select": False, "columns": ["one"], "comment": "contract",
        }
        relations.extend([
            {**base, "schema": mapping["targetSchema"]},
            {**base, "schema": "public"},
        ])
    snapshot = {
        "schemas": schemas,
        "relations": relations,
        "publicRoutines": [],
        "posture": {
            "contractVersion": contract["schemaVersion"], "phase": "expand",
            "expandReady": True, "contractReady": False,
        },
    }
    return snapshot, contract, {"objects": []}


class SchemaBoundaryPhaseTest(unittest.TestCase):
    def test_expand_accepts_core_tables_and_compatibility_views(self) -> None:
        snapshot, contract, inventory = fixture()
        self.assertEqual(checker.validate_snapshot(snapshot, contract, inventory), [])

    def test_expand_rejects_unplanned_non_core_public_table(self) -> None:
        snapshot, contract, inventory = fixture()
        snapshot["relations"].append({"schema": "public", "name": "surprise", "kind": "r"})
        errors = checker.validate_snapshot(snapshot, contract, inventory)
        self.assertIn("non-core public tables lack a non-public target: ['surprise']", errors)

    def test_contract_rejects_compatibility_and_routine_residue(self) -> None:
        snapshot, contract, inventory = fixture()
        contract = copy.deepcopy(contract)
        contract["phase"] = "contract"
        snapshot["posture"]["phase"] = "contract"
        snapshot["posture"]["contractReady"] = True
        snapshot["publicRoutines"] = [{"name": "legacy", "identity_arguments": "", "kind": "f"}]
        errors = checker.validate_snapshot(snapshot, contract, inventory)
        self.assertTrue(any(error.startswith("Contract public view residue:") for error in errors))
        self.assertEqual(errors[-1], "Contract public routine residue: ['legacy()']")

    def test_role_or_schema_acl_drift_fails_closed(self) -> None:
        snapshot, contract, inventory = fixture()
        snapshot["schemas"][0]["privileges"]["anonCreate"] = True
        target = next(row for row in snapshot["relations"] if row.get("schema") == "api")
        target["anon_select"] = True
        errors = checker.validate_snapshot(snapshot, contract, inventory)
        self.assertTrue(any("forbidden schema CREATE" in error for error in errors))
        self.assertTrue(any("target view ACL drift" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
