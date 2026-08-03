#!/usr/bin/env python3
"""Mutation-negative tests for the Issue #408 B0 permission rollout."""

from __future__ import annotations

import copy
import unittest

import jsonschema

from scripts import issue_408_worker_runtime_permission as contract


class WorkerRuntimePermissionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.permission = contract.expected_permission()
        self.rollout = contract.expected_rollout(self.permission)

    def assert_permission_rejected(self, mutation) -> None:
        changed = copy.deepcopy(self.permission)
        mutation(changed)
        with self.assertRaises(contract.ContractError):
            contract.validate_permission(changed)

    def assert_rollout_rejected(self, mutation) -> None:
        changed = copy.deepcopy(self.rollout)
        mutation(changed)
        with self.assertRaises(contract.ContractError):
            contract.validate_rollout(changed, self.permission)

    def test_checked_artifacts_are_exact(self) -> None:
        contract.check()

    def test_json_schemas_reject_nested_extra_fields(self) -> None:
        permission_schema = contract.load_json(contract.PERMISSION_SCHEMA)
        rollout_schema = contract.load_json(contract.ROLLOUT_SCHEMA)
        jsonschema.Draft202012Validator(permission_schema).validate(self.permission)
        jsonschema.Draft202012Validator(rollout_schema).validate(self.rollout)
        hostile_permission = copy.deepcopy(self.permission)
        hostile_permission["policy"]["allowFutureGrants"] = True
        hostile_permission["generations"][1]["unexpectedPrivilegeEnvelope"] = {}
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.Draft202012Validator(permission_schema).validate(hostile_permission)
        hostile_rollout = copy.deepcopy(self.rollout)
        hostile_rollout["new"]["unexpectedManifest"] = "f" * 64
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.Draft202012Validator(rollout_schema).validate(hostile_rollout)
        swapped = copy.deepcopy(self.permission)
        swapped["generations"].reverse()
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.Draft202012Validator(permission_schema).validate(swapped)
        duplicate = copy.deepcopy(self.permission)
        duplicate["generations"][1] = copy.deepcopy(duplicate["generations"][0])
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.Draft202012Validator(permission_schema).validate(duplicate)
        owner_grantor_mismatch = copy.deepcopy(self.permission)
        owner_grantor_mismatch["generations"][1]["routinePrivileges"][0]["grantor"] = "postgres"
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.Draft202012Validator(permission_schema).validate(owner_grantor_mismatch)

    def test_exact_generations_are_seven_then_nine(self) -> None:
        contract.validate_permission(self.permission)
        old, new = self.permission["generations"]
        self.assertEqual([old["counts"]["routinePrivilegeCount"], new["counts"]["routinePrivilegeCount"]], [7, 9])
        self.assertEqual(old["databaseCommit"], contract.BASE_COMMIT)
        self.assertEqual(new["databaseCommit"], contract.CANDIDATE_COMMIT)
        self.assertEqual(old["migrationHead"], contract.BASE_MIGRATION_HEAD)
        self.assertEqual(new["migrationHead"], contract.CANDIDATE_MIGRATION_HEAD)

    def test_superset_and_missing_routine_are_rejected(self) -> None:
        self.assert_permission_rejected(
            lambda value: value["generations"][1]["routinePrivileges"].append(
                copy.deepcopy(value["generations"][1]["routinePrivileges"][-1])
            )
        )
        self.assert_permission_rejected(lambda value: value["generations"][1]["routinePrivileges"].pop())

    def test_routine_order_and_signature_are_rejected(self) -> None:
        self.assert_permission_rejected(
            lambda value: value["generations"][1]["routinePrivileges"].reverse()
        )
        self.assert_permission_rejected(
            lambda value: value["generations"][1]["routinePrivileges"][0].__setitem__(
                "identityArguments", "p_result_id text"
            )
        )

    def test_commit_head_and_manifest_hash_are_rejected(self) -> None:
        self.assert_permission_rejected(
            lambda value: value["generations"][0].__setitem__("databaseCommit", contract.CANDIDATE_COMMIT)
        )
        self.assert_permission_rejected(
            lambda value: value["generations"][1].__setitem__("migrationHead", contract.BASE_MIGRATION_HEAD)
        )
        self.assert_permission_rejected(
            lambda value: value["generations"][1].__setitem__("manifestSha256", "0" * 64)
        )

    def test_role_schema_relation_and_sequence_drift_are_rejected(self) -> None:
        self.assert_permission_rejected(
            lambda value: value["generations"][1]["roleAttributes"].__setitem__("bypassRls", True)
        )
        self.assert_permission_rejected(
            lambda value: value["generations"][1]["schemaPrivileges"][0].__setitem__("create", True)
        )
        self.assert_permission_rejected(
            lambda value: value["generations"][1]["relationPrivileges"].append({"objectKey": "private.worker_jobs", "privilege": "SELECT"})
        )
        self.assert_permission_rejected(
            lambda value: value["generations"][1]["sequencePrivileges"].append({"objectKey": "private.unexpected_seq", "privilege": "USAGE"})
        )

    def test_membership_edge_drift_is_rejected(self) -> None:
        self.assert_permission_rejected(
            lambda value: value["generations"][1]["creatorMembershipEdges"][0].__setitem__("set", True)
        )
        self.assert_permission_rejected(
            lambda value: value["generations"][1]["runtimeLoginMembershipPolicy"].__setitem__("includedInManifest", True)
        )

    def test_runtime_login_membership_requires_a_separate_exact_receipt(self) -> None:
        generation = self.permission["generations"][1]
        self.assertEqual(
            self.permission["policy"]["equalityScope"],
            "capability-role-definition-and-object-privileges",
        )
        self.assertEqual(
            self.permission["policy"]["runtimeLoginMemberships"],
            "excluded-require-exact-deployment-receipt",
        )
        self.assertEqual(
            self.permission["policy"]["roleAndCreatorEvidence"],
            "exact-migration-self-guard-plus-hosted-receipt-required",
        )
        self.assertEqual(
            generation["runtimeLoginMembershipPolicy"],
            {
                "includedInManifest": False,
                "requiredReceipt": "workspace-identity-run-exact-readback",
                "memberClass": "deployment-owned-login",
                "role": contract.ROLE,
                "inherit": True,
                "set": False,
                "admin": False,
            },
        )

    def test_rollout_accepts_only_ordered_old_and_new(self) -> None:
        contract.validate_rollout(self.rollout, self.permission)
        self.assert_rollout_rejected(lambda value: value["acceptedManifestSha256"].reverse())
        self.assert_rollout_rejected(lambda value: value["acceptedManifestSha256"].pop())
        self.assert_rollout_rejected(lambda value: value["acceptedManifestSha256"].append("f" * 64))

    def test_rollout_hash_and_candidate_full_drift_are_rejected(self) -> None:
        self.assert_rollout_rejected(lambda value: value["new"].__setitem__("manifestSha256", "0" * 64))
        self.assert_rollout_rejected(lambda value: value["candidateFull"].__setitem__("status", "ready"))
        self.assert_rollout_rejected(lambda value: value["candidateFull"].__setitem__("manifestSha256", "f" * 64))


if __name__ == "__main__":
    unittest.main()
