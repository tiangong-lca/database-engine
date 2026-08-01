#!/usr/bin/env python3
"""Offline fail-closed tests for the SECURITY DEFINER audit contract."""

from __future__ import annotations

import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import security_definer_audit as audit


class SecurityDefinerAuditTest(unittest.TestCase):
    def setUp(self) -> None:
        self.inventory, self.inventory_hash = audit.load_inventory()
        self.committed = json.loads(audit.OUT.read_text(encoding="utf-8"))

    def test_full_241_routine_ledger_and_cohorts(self) -> None:
        self.assertEqual(self.committed["summary"], audit.EXPECTED)
        self.assertEqual(len(self.committed["routines"]), 241)

    def test_issue339_facades_remain_visible_but_excluded_from_residue(self) -> None:
        facades = [
            item for item in self.committed["routines"]
            if item["cohort"] == "issue339-validated-rls-facade"
        ]
        self.assertEqual(len(facades), 14)
        self.assertTrue(all(item["observed"]["ownerRole"] == "api_internal_executor" for item in facades))
        self.assertTrue(all(item["confirmed"]["issue339AclValidation"] for item in facades))

    def test_issue333_residue_is_exactly_90_api_and_39_private(self) -> None:
        residue = [
            item for item in self.committed["routines"]
            if item["cohort"] == "issue333-owner-runtime-residue"
        ]
        self.assertEqual(sum(item["targetSchema"] == "api" for item in residue), 90)
        self.assertEqual(sum(item["targetSchema"] == "private" for item in residue), 39)
        self.assertTrue(all(item["observed"]["ownerRole"] == "postgres" for item in residue))

    def test_static_signals_never_claim_runtime_confirmation(self) -> None:
        self.assertTrue(all(not item["confirmed"]["ownerRuntime"] for item in self.committed["routines"]))
        self.assertTrue(all(
            item["inferred"]["signalLimit"] == "static-signals-are-not-runtime-authorization-proof"
            for item in self.committed["routines"]
        ))

    def test_every_signature_has_complete_role_matrix(self) -> None:
        for item in self.committed["routines"]:
            with self.subTest(objectKey=item["objectKey"]):
                self.assertEqual(
                    [row["role"] for row in item["required"]["roleMatrix"]],
                    list(audit.ROLES),
                )

    def test_platform_owner_blocker_cannot_be_marked_resolved(self) -> None:
        tampered = copy.deepcopy(self.committed)
        tampered["boundaries"]["platformOwnerDefaultPrivileges"]["status"] = "validated"
        with self.assertRaisesRegex(ValueError, "platform-owner blocker"):
            audit.validate(tampered, self.inventory, self.inventory_hash)

    def test_missing_signature_fails_closed(self) -> None:
        tampered = copy.deepcopy(self.committed)
        tampered["routines"].pop()
        tampered["summary"] = audit.summarize(tampered)
        with self.assertRaisesRegex(ValueError, "reviewed baseline"):
            audit.validate(tampered, self.inventory, self.inventory_hash)

    def test_rehashed_signature_swap_fails_closed_offline(self) -> None:
        tampered = copy.deepcopy(self.committed)
        tampered["routines"][0]["objectKey"] = "public.fabricated_signature()"
        with self.assertRaisesRegex(ValueError, "signature set differs"):
            audit.validate(tampered, self.inventory, self.inventory_hash)

    def test_contract_handoff_cannot_be_marked_started(self) -> None:
        tampered = copy.deepcopy(self.committed)
        tampered["boundaries"]["contractMigration"]["status"] = "in-progress"
        with self.assertRaisesRegex(ValueError, "#358 Contract handoff"):
            audit.validate(tampered, self.inventory, self.inventory_hash)

    def test_role_matrix_tamper_fails_closed(self) -> None:
        tampered = copy.deepcopy(self.committed)
        tampered["routines"][0]["required"]["roleMatrix"].pop()
        with self.assertRaisesRegex(ValueError, "role matrix"):
            audit.validate(tampered, self.inventory, self.inventory_hash)

    def test_hash_tamper_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory) / "audit.json"
            digest = Path(directory) / "audit.sha256"
            out.write_text(audit.canonical(self.committed), encoding="utf-8")
            digest.write_text("0" * 64 + "\n", encoding="utf-8")
            original_out, original_sha = audit.OUT, audit.SHA
            try:
                audit.OUT, audit.SHA = out, digest
                with self.assertRaisesRegex(ValueError, "hash does not match"):
                    audit.verify_committed()
            finally:
                audit.OUT, audit.SHA = original_out, original_sha

    def test_canonical_bytes_are_deterministic(self) -> None:
        first = audit.canonical(copy.deepcopy(self.committed)).encode("utf-8")
        second = audit.canonical(copy.deepcopy(self.committed)).encode("utf-8")
        self.assertEqual(first, second)
        self.assertEqual(hashlib.sha256(first).digest(), hashlib.sha256(second).digest())

    def test_canonical_runner_contains_committed_generation_gate(self) -> None:
        runner = (audit.ROOT / "scripts/run_database_contract.py").read_text(encoding="utf-8")
        self.assertIn('"scripts/security_definer_audit.py", "--check"', runner)

    def test_non_loopback_database_url_is_rejected(self) -> None:
        self.assertEqual(
            audit.require_loopback_database_url("postgresql://postgres@127.0.0.1:5432/postgres"),
            "postgresql://postgres@127.0.0.1:5432/postgres",
        )
        with self.assertRaisesRegex(ValueError, "loopback database URL"):
            audit.require_loopback_database_url("postgresql://postgres@example.supabase.co/postgres")


if __name__ == "__main__":
    unittest.main()
