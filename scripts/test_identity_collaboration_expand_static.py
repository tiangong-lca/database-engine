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
            "1b94c1ce7c132e5481c4a2594d6d9a957d7dc683",
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
        self.assertIn("issue355_routine_baseline", normalized)


if __name__ == "__main__":
    unittest.main()
