import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "supabase/migrations/20260801061000_issue_355_identity_collaboration_expand.sql"
CONTRACT = ROOT / "supabase/tests/contracts/identity_collaboration_expand.v1.json"
INVENTORY = ROOT / "supabase/tests/contracts/public_object_inventory.json"


class IdentityCollaborationExpandStaticTest(unittest.TestCase):
    def setUp(self) -> None:
        self.sql = MIGRATION.read_text()
        self.contract = json.loads(CONTRACT.read_text())
        self.inventory = json.loads(INVENTORY.read_text())

    def test_exact_inventory_batch_is_covered(self) -> None:
        expected = {
            item["objectKey"]
            for item in self.inventory["objects"]
            if item["migrationBatch"] in {"30-identity", "34-collaboration"}
        }
        actual = {item["objectKey"] for item in self.contract["objects"]}
        self.assertEqual(16, len(expected))
        self.assertEqual(expected, actual)

    def test_expand_is_single_source_and_contract_gated(self) -> None:
        self.assertEqual("public", self.contract["singleWritePhysicalSchema"])
        self.assertFalse(self.contract["tablePhysicalMoveReady"])
        self.assertFalse(self.contract["productionMutationAllowed"])
        self.assertEqual(
            "49ea2a2a7d6ed306bbcb95db20656e3acc221381",
            self.contract["databaseBaseSha"],
        )
        for item in self.contract["objects"]:
            self.assertTrue(item["expandPhysical"].startswith("public."))
            self.assertTrue(item["privateProjection"].startswith("private."))

    def test_api_contracts_are_versioned_and_comments_are_reused(self) -> None:
        api_reads = [item["apiRead"] for item in self.contract["objects"] if "apiRead" in item]
        self.assertTrue(all(name.endswith("_v1") for name in api_reads))
        self.assertIn("api.review_comments_v1", api_reads)
        self.assertNotIn("create view api.review_comments_v1", self.sql.lower())
        self.assertNotIn("select *", self.sql.lower())

    def test_transaction_and_timeout_guards_are_versioned(self) -> None:
        normalized = self.sql.lower()
        self.assertIn("begin;", normalized)
        self.assertIn("commit;", normalized)
        self.assertIn("set local lock_timeout = '5s'", normalized)
        self.assertIn("set local statement_timeout = '60s'", normalized)
        self.assertIn("in access share mode", normalized)
        self.assertNotIn("access exclusive", normalized)
        self.assertIn("create or replace view private.comments", normalized)
        self.assertIn("create or replace function private.review_scope_checksum_v1", normalized)
        self.assertNotIn("language sql volatile security definer", normalized)
        self.assertNotIn("returns void language sql volatile\nsecurity definer", normalized)
        self.assertIn("issue355_routine_baseline", normalized)
        self.assertIn("reviewed predecessor routine signature/definition/property/acl fingerprint mismatch", normalized)
        self.assertIn("from actual except select signature,fingerprint from expected", normalized)

    def test_policy_fingerprint_is_hosted_role_oid_portable(self) -> None:
        normalized = self.sql.lower()
        self.assertNotIn("array_to_string(policy.polroles", normalized)
        self.assertNotIn("policy.polroles::text", normalized)
        self.assertIn("from unnest(policy.polroles) role_oid", normalized)
        self.assertIn("then 'public' else role_name.rolname", normalized)

    def test_users_policy_admits_only_exact_observed_predecessors(self) -> None:
        normalized = self.sql.lower()
        policy_contract = self.contract["sourcePolicyCompatibility"][
            "public.users/select by self and team and admin"
        ]
        self.assertEqual(
            [
                "57fd9c26617c29dc6edc92d231bbec85",
                "6ab74729e7e0ec6e9378542059d17cd0",
            ],
            policy_contract["admittedPolicySetMd5"],
        )
        self.assertEqual("preserve-exact-predecessor", policy_contract["expandDisposition"])
        self.assertEqual(
            "compatibility-only-not-approved-target",
            policy_contract["legacyVariantSecurityDisposition"],
        )
        for fingerprint in policy_contract["admittedPolicySetMd5"]:
            self.assertEqual(1, normalized.count(fingerprint))
        self.assertIn("actual.policy_hash = any(expected.policy_hashes)", normalized)
        self.assertIn("is distinct from baseline.policy_hash", normalized)

    def test_target_column_acl_is_converged_and_evidenced(self) -> None:
        normalized = self.sql.lower()
        self.assertIn("cross join lateral aclexplode(attribute.attacl)", normalized)
        self.assertIn("revoke all privileges on table %s from %i cascade", normalized)
        self.assertIn("revoke all privileges (%i) on table %s from %s cascade", normalized)
        self.assertIn("revoke all privileges on function %s from public cascade", normalized)
        self.assertIn("revoke all privileges on function %s from %i cascade", normalized)
        self.assertGreaterEqual(normalized.count("with ordinality acl"), 5)
        self.assertGreaterEqual(normalized.count("order by min(acl.ordinality)"), 5)
        self.assertIn("target column acl postcondition failed", normalized)
        self.assertEqual("none", self.contract["targetColumnAclPolicy"])


if __name__ == "__main__":
    unittest.main()
