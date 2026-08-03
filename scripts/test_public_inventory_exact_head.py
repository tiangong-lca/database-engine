#!/usr/bin/env python3
"""Offline regression tests for the Issue #405 exact-head inventory."""

from __future__ import annotations

import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import public_inventory_exact_head as exact


class PublicInventoryExactHeadTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.inventory = json.loads(exact.INVENTORY.read_text(encoding="utf-8"))
        cls.checklist = json.loads(exact.DROP_CHECKLIST.read_text(encoding="utf-8"))
        cls.partition = exact.read_tsv(exact.PARTITION)

    def test_offline_check_reads_no_database(self) -> None:
        with mock.patch.object(exact, "load_live", side_effect=AssertionError("database forbidden")):
            result = exact.verify()
        self.assertEqual(result["counts"], exact.EXPECTED_COUNTS)
        self.assertFalse(result["contractReady"])

    def test_live_actions_require_explicit_loopback_urls(self) -> None:
        with mock.patch.dict("os.environ", {"DATABASE_URL": "postgresql://postgres@127.0.0.1/db"}):
            with self.assertRaisesRegex(ValueError, "explicit"):
                exact.db_url(None)
        for value in (
            "postgresql://postgres@db.example.com/postgres",
            "postgresql://localhost@db.example.com/postgres",
            "postgresql://postgres@127.0.0.1%2eevil/postgres",
            "postgresql://postgres@127.0.0.1/postgres?host=db.example.com",
            "postgresql://postgres@127.0.0.1/postgres?hostaddr=8.8.8.8",
            "postgresql://postgres@127.0.0.1/postgres?service=prod",
            "postgresql://postgres@127.0.0.1/postgres#remote",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                exact.db_url(value)
        for value in (
            "postgresql://postgres@127.0.0.1:5432/postgres",
            "postgresql://postgres@[::1]:5432/postgres",
            "postgresql://postgres@localhost:5432/postgres",
        ):
            self.assertEqual(exact.db_url(value), value)

    def test_migration_tree_and_live_history_fail_closed(self) -> None:
        with mock.patch.object(exact, "migration_tree_sha256", return_value="0" * 64):
            with self.assertRaisesRegex(ValueError, "migration tree"):
                exact.validate_source()
        with mock.patch.object(exact, "psql_json", return_value=["20260803090000"]):
            with self.assertRaisesRegex(ValueError, "applied migration history"):
                exact.validate_live_migrations("postgresql://postgres@127.0.0.1/postgres")

    def test_exact_counts_and_partition_are_closed(self) -> None:
        self.assertEqual(len(self.inventory["objects"]), 397)
        self.assertEqual(self.inventory["counts"], {"function": 336, "table": 49, "view": 12})
        self.assertEqual(self.inventory["partitionCounts"], dict(sorted(exact.EXPECTED_PARTITIONS.items())))
        self.assertEqual(set(self.inventory["finalPublicAllowlist"]), exact.CORE_KEYS)

    def test_388_drop_identities_are_exactly_once_and_non_executable(self) -> None:
        entries = self.checklist["entries"]
        keys = [entry["objectKey"] for entry in entries]
        expected = {item["objectKey"] for item in self.inventory["objects"]} - exact.CORE_KEYS
        self.assertEqual(len(keys), 388)
        self.assertEqual(len(set(keys)), 388)
        self.assertEqual(set(keys), expected)
        self.assertTrue(all(entry["status"] == "blocked" for entry in entries))
        self.assertTrue(all(not entry["dropIdentity"].lstrip().upper().startswith("DROP ") for entry in entries))
        self.assertFalse(self.checklist["contractReady"])

    def test_state_model_distinguishes_move_compat_adapter_and_retirement(self) -> None:
        states = [item["lifecycleState"] for item in self.inventory["objects"]]
        self.assertEqual(sum(state["physicalMoved"] for state in states), 7)
        self.assertEqual(sum(state["compatPresent"] for state in states), 42)
        self.assertEqual(sum(state["adapterOnly"] for state in states), 34)
        self.assertEqual(sum(state["retired"] for state in states), 0)
        moved = {item["objectKey"] for item in self.inventory["objects"] if item["lifecycleState"]["physicalMoved"]}
        self.assertEqual(moved, {
            "public.lca_active_snapshots", "public.lca_network_snapshots",
            "public.lca_snapshot_artifacts", "public.worker_job_artifacts",
            "public.worker_job_events", "public.worker_job_kinds", "public.worker_jobs",
        })

    def test_four_omitted_routines_have_owner_target_and_security_contract(self) -> None:
        omitted = [item for item in self.inventory["objects"] if item["partition"] == "omitted-explicit"]
        self.assertEqual(len(omitted), 4)
        by_key = {item["objectKey"]: item for item in omitted}
        lineage = next(item for key, item in by_key.items() if "private.worker_job_artifacts" in key)
        self.assertEqual(lineage["targetSchema"], "private")
        self.assertTrue(lineage["catalog"]["security_definer"])
        self.assertEqual(lineage["catalog"]["config"], ["search_path=public, pg_temp"])
        root_queries = [item for item in omitted if item is not lineage]
        self.assertEqual({item["targetSchema"] for item in root_queries}, {"api"})
        self.assertTrue(all(item["ownerRole"] == "postgres" for item in omitted))
        self.assertTrue(all(item["catalog"]["security_definer"] for item in root_queries))
        self.assertTrue(all(item["catalog"]["config"] == ["search_path=pg_catalog, pg_temp"] for item in root_queries))

    def test_duplicate_partition_identity_fails_closed(self) -> None:
        rows = copy.deepcopy(self.partition)
        rows.append(copy.deepcopy(rows[0]))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            exact.validate_partition(rows)

    def test_missing_partition_identity_fails_closed(self) -> None:
        rows = copy.deepcopy(self.partition[:-1])
        with self.assertRaisesRegex(ValueError, "partition count drift"):
            exact.validate_partition(rows)

    def test_partition_count_substitution_fails_closed(self) -> None:
        rows = copy.deepcopy(self.partition)
        rows[0]["partition"] = "predecessor"
        with self.assertRaisesRegex(ValueError, "partition count drift"):
            exact.validate_partition(rows)

    def test_cross_artifact_assignment_tamper_fails_closed(self) -> None:
        inventory = copy.deepcopy(self.inventory)
        first = next(item for item in inventory["objects"] if item["partition"] == "issue357")
        second = next(item for item in inventory["objects"] if item["partition"] == "issue358")
        first["partition"], second["partition"] = second["partition"], first["partition"]
        with self.assertRaisesRegex(ValueError, "differs from partition"):
            exact.validate_contracts(inventory, copy.deepcopy(self.checklist))
        inventory = copy.deepcopy(self.inventory)
        item = next(item for item in inventory["objects"] if item["partition"] != "core")
        item["targetSchema"] = "archive"
        checklist = copy.deepcopy(self.checklist)
        next(entry for entry in checklist["entries"] if entry["objectKey"] == item["objectKey"])["targetSchema"] = "archive"
        with self.assertRaisesRegex(ValueError, "differs from partition"):
            exact.validate_contracts(inventory, checklist)

    def test_unknown_live_identity_fails_closed(self) -> None:
        catalog = {
            "relations": [{
                "object_key": "public.unknown", "object_type": "table",
                "object_name": "unknown", "owner_role": "postgres", "rls_enabled": True,
            }],
            "routines": [], "relationAcl": [], "routineAcl": [],
        }
        partition = exact.validate_partition(copy.deepcopy(self.partition))
        with self.assertRaisesRegex(ValueError, "live/partition identity drift"):
            exact.refreshed_ledger(catalog, {"public.unknown": {}}, partition)

    def test_drop_checklist_missing_duplicate_and_ready_tamper_fail_closed(self) -> None:
        mutations = []
        missing = copy.deepcopy(self.checklist)
        missing["entries"].pop()
        mutations.append(missing)
        duplicate = copy.deepcopy(self.checklist)
        duplicate["entries"][-1] = copy.deepcopy(duplicate["entries"][0])
        mutations.append(duplicate)
        ready = copy.deepcopy(self.checklist)
        ready["contractReady"] = True
        mutations.append(ready)
        for checklist in mutations:
            with self.subTest(entry_count=len(checklist["entries"])):
                with self.assertRaises(ValueError):
                    exact.validate_contracts(copy.deepcopy(self.inventory), checklist)

    def test_drop_identity_kind_name_and_signature_tamper_fail_closed(self) -> None:
        for replacement in (
            "TABLE public.not_the_object",
            "VIEW public.contacts",
            "FUNCTION public.review_scope_checksum_v1(p_items text)",
        ):
            checklist = copy.deepcopy(self.checklist)
            checklist["entries"][0]["dropIdentity"] = replacement
            with self.subTest(replacement=replacement), self.assertRaisesRegex(ValueError, "DROP checklist"):
                exact.validate_contracts(copy.deepcopy(self.inventory), checklist)

    def test_counterpart_matching_is_target_schema_and_overload_exact(self) -> None:
        actual = {
            "object_key": "public.example(p_value uuid)", "object_name": "example",
            "object_type": "function",
        }
        assignment = {"target_schema": "private"}
        wrong = [
            {"schema": "api", "name": "example", "objectType": "function", "identityArguments": "p_value uuid", "physical": False},
            {"schema": "private", "name": "example", "objectType": "function", "identityArguments": "p_value text", "physical": False},
            {"schema": "private", "name": "example", "objectType": "function", "identityArguments": "p_value api.uuid", "physical": False},
        ]
        self.assertFalse(exact.lifecycle_state(actual, assignment, wrong)["compatPresent"])
        exact_match = wrong + [{
            "schema": "private", "name": "example", "objectType": "function",
            "identityArguments": "p_value uuid", "physical": False,
        }]
        state = exact.lifecycle_state(actual, assignment, exact_match)
        self.assertTrue(state["compatPresent"])
        self.assertTrue(state["adapterOnly"])
        self.assertFalse(state["physicalMoved"])

    def test_inventory_ready_count_and_state_tamper_fail_closed(self) -> None:
        mutations = []
        ready = copy.deepcopy(self.inventory)
        ready["contractReady"] = True
        mutations.append(ready)
        count = copy.deepcopy(self.inventory)
        count["counts"]["function"] -= 1
        mutations.append(count)
        state = copy.deepcopy(self.inventory)
        del state["objects"][0]["lifecycleState"]["adapterOnly"]
        mutations.append(state)
        for inventory in mutations:
            with self.subTest(counts=inventory.get("counts")):
                with self.assertRaises(ValueError):
                    exact.validate_contracts(inventory, copy.deepcopy(self.checklist))

    def test_inventory_and_checklist_state_cannot_drift_from_ledger(self) -> None:
        inventory = copy.deepcopy(self.inventory)
        item = next(item for item in inventory["objects"] if not item["lifecycleState"]["physicalMoved"])
        item["lifecycleState"]["physicalMoved"] = True
        checklist = copy.deepcopy(self.checklist)
        entry = next(entry for entry in checklist["entries"] if entry["objectKey"] == item["objectKey"])
        entry["lifecycleState"]["physicalMoved"] = True
        with self.assertRaisesRegex(ValueError, "ledger lifecycle state"):
            exact.validate_contracts(inventory, checklist)

    def test_canonical_bytes_and_hashes_replay(self) -> None:
        inventory_bytes = exact.canonical(copy.deepcopy(self.inventory)).encode("utf-8")
        checklist_bytes = exact.canonical(copy.deepcopy(self.checklist)).encode("utf-8")
        self.assertEqual(inventory_bytes, exact.INVENTORY.read_bytes())
        self.assertEqual(checklist_bytes, exact.DROP_CHECKLIST.read_bytes())
        self.assertEqual(hashlib.sha256(inventory_bytes).hexdigest(), exact.INVENTORY_SHA.read_text().strip())
        self.assertEqual(hashlib.sha256(checklist_bytes).hexdigest(), exact.DROP_CHECKLIST_SHA.read_text().strip())

    def test_hash_tamper_fails_offline_check(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            bad = Path(directory) / "bad.sha256"
            bad.write_text("0" * 64 + "\n", encoding="utf-8")
            with mock.patch.object(exact, "INVENTORY_SHA", bad):
                with self.assertRaisesRegex(ValueError, "hash drift"):
                    exact.verify()

    def test_generator_contains_no_schema_mutation_or_contract_authorization(self) -> None:
        source = Path(exact.__file__).read_text(encoding="utf-8").lower()
        self.assertNotIn("contractready\": true", source)
        self.assertNotRegex(source, r"\b(drop|alter|create)\s+(table|view|function|schema)\b")
        self.assertNotIn("supabase db push", source)


if __name__ == "__main__":
    unittest.main()
