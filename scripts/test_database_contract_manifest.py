#!/usr/bin/env python3
"""Offline fail-closed tests for the database test manifest."""

from __future__ import annotations

import copy
import hashlib
import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import scripts.run_database_contract as runner
from scripts.test_issue_390_pre_ddl_gate import Issue390PreDdlGateTest


class DatabaseContractManifestTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = json.loads(runner.MANIFEST.read_text(encoding="utf-8"))
        cls.tracked = runner.tracked_test_files()

    def test_checked_manifest_has_unique_classification_and_67_canonical_files(self) -> None:
        classified = runner.validate_manifest(self.manifest, self.tracked)
        suite = self.manifest["suites"]["canonical-local"]
        selected = [
            path for path in classified[suite["classification"]]
            if path not in suite["excludedFiles"]
        ]
        self.assertEqual(len(classified["canonical-pgtap"]), 86)
        self.assertEqual(len(suite["excludedFiles"]), 19)
        self.assertEqual(len(selected), 67)

    def test_persistent_dev_validation_fetches_immutable_provenance_history(self) -> None:
        workflow = (runner.ROOT / ".github/workflows/supabase-dev.yml").read_text(
            encoding="utf-8"
        )
        self.assertRegex(
            workflow,
            re.compile(
                r"- name: Checkout repository\n"
                r"\s+uses: actions/checkout@v6\n"
                r"\s+with:\n"
                r"\s+fetch-depth: 0\n"
                r".*?- name: Setup Python",
                re.DOTALL,
            ),
        )

    def test_suite_evidence_binds_commit_head_cli_manifest_and_file_list(self) -> None:
        files = ["supabase/tests/example.sql"]
        with mock.patch.object(
            runner.subprocess, "run",
            side_effect=[
                subprocess.CompletedProcess([], 0, stdout="a" * 40 + "\n"),
                subprocess.CompletedProcess([], 0, stdout=""),
                subprocess.CompletedProcess([], 0, stdout="2.98.0\n"),
            ],
        ):
            evidence = runner.suite_evidence("canonical-local", files, 19)
        self.assertEqual(evidence["gitCommit"], "a" * 40)
        self.assertFalse(evidence["worktreeDirty"])
        self.assertEqual(evidence["migrationHead"], "20260802091342")
        self.assertEqual(evidence["supabaseCliVersion"], "2.98.0")
        self.assertEqual(evidence["filesSha256"], runner.stable_json_sha256(files))
        self.assertEqual(
            evidence["manifestSha256"],
            hashlib.sha256(runner.MANIFEST.read_bytes()).hexdigest(),
        )

    def test_all_100_sql_assets_are_owned_once_but_only_86_are_canonical_pgtap(self) -> None:
        classified = runner.validate_manifest(self.manifest, self.tracked)
        sql_paths = [path for path in self.tracked if path.endswith(".sql")]
        owned = [path for paths in classified.values() for path in paths if path.endswith(".sql")]
        self.assertEqual(len(sql_paths), 100)
        self.assertEqual(sorted(owned), sorted(sql_paths))
        self.assertEqual(len(classified["canonical-pgtap"]), 86)

    def test_exclusion_growth_and_path_substitution_fail(self) -> None:
        changed = copy.deepcopy(self.manifest)
        excluded = changed["suites"]["canonical-local"]["excludedFiles"]
        excluded["supabase/tests/20260404_dataset_command_rpcs.sql"] = copy.deepcopy(
            next(iter(excluded.values()))
        )
        with self.assertRaisesRegex(SystemExit, "exclusion count grew"):
            runner.validate_manifest(changed, self.tracked)

        changed = copy.deepcopy(self.manifest)
        excluded = changed["suites"]["canonical-local"]["excludedFiles"]
        excluded.pop("supabase/tests/20260403_update_security_smoke.sql")
        excluded["supabase/tests/20260404_dataset_command_rpcs.sql"] = copy.deepcopy(
            next(iter(excluded.values()))
        )
        with self.assertRaisesRegex(SystemExit, "exclusion path baseline differs"):
            runner.validate_manifest(changed, self.tracked)

    def test_exclusion_requires_exact_metadata_and_live_replacements(self) -> None:
        changed = copy.deepcopy(self.manifest)
        metadata = next(iter(changed["suites"]["canonical-local"]["excludedFiles"].values()))
        metadata.pop("category")
        with self.assertRaisesRegex(SystemExit, "metadata fields differ"):
            runner.validate_manifest(changed, self.tracked)

        changed = copy.deepcopy(self.manifest)
        metadata = next(iter(changed["suites"]["canonical-local"]["excludedFiles"].values()))
        metadata["replacementFiles"] = ["supabase/tests/missing.sql"]
        with self.assertRaisesRegex(SystemExit, "missing exclusion replacement"):
            runner.validate_manifest(changed, self.tracked)

    def test_settled_security_definer_fixture_is_not_requalified_forever(self) -> None:
        self.assertEqual(runner.pending_security_definer_transition(), [])

    def test_focused_activation_is_inactive_or_complete_never_partially_skipped(self) -> None:
        suite = self.manifest["suites"]["lca-private-expand"]
        repository_files = runner.tracked_repository_files()
        self.assertIsNone(runner.validate_activation_contract(
            "lca-private-expand", suite, repository_files, required=False,
        ))
        partial = repository_files + [
            "supabase/tests/contracts/lca_private_expand_freeze.v2.json",
        ]
        with self.assertRaisesRegex(SystemExit, "partial activation triggered"):
            runner.validate_activation_contract(
                "lca-private-expand", suite, partial, required=False,
            )

        mismatched = repository_files + suite["activation"]["requiredPaths"] + [
            "supabase/tests/contracts/lca_private_expand_freeze.v3.json",
            "supabase/tests/contracts/lca_private_expand_freeze.v3.sha256",
            "supabase/tests/contracts/lca_private_expand_freeze.v3.schema.json",
            "supabase/tests/contracts/lca_private_expand_cut_receipts.v2.json",
            "supabase/tests/contracts/lca_private_expand_cut_receipts.v2.sha256",
            "supabase/tests/contracts/lca_private_expand_cut_receipts.v2.schema.json",
            "supabase/migrations/20260802010101_issue_357_api_pre_expand.sql",
            "supabase/migrations/20260802020202_issue_357_physical_cut.sql",
        ]
        with self.assertRaisesRegex(SystemExit, "artifact versions differ"):
            runner.validate_activation_contract(
                "lca-private-expand", suite, mismatched, required=False,
            )

    def test_v3_physical_objects_and_exposure_shape_is_delegated_to_official_verifier(self) -> None:
        suite = self.manifest["suites"]["lca-private-expand"]
        repository_files = runner.tracked_repository_files()
        complete = repository_files + suite["activation"]["requiredPaths"] + [
            "supabase/tests/contracts/lca_private_expand_freeze.v3.json",
            "supabase/tests/contracts/lca_private_expand_freeze.v3.sha256",
            "supabase/tests/contracts/lca_private_expand_freeze.v3.schema.json",
            "supabase/tests/contracts/lca_private_expand_cut_receipts.v3.json",
            "supabase/tests/contracts/lca_private_expand_cut_receipts.v3.sha256",
            "supabase/tests/contracts/lca_private_expand_cut_receipts.v3.schema.json",
            "supabase/migrations/20260802010101_issue_357_api_pre_expand.sql",
            "supabase/migrations/20260802020202_issue_357_physical_cut.sql",
        ]
        artifacts = runner.validate_activation_contract(
            "lca-private-expand", suite, complete, required=False,
        )
        self.assertEqual(artifacts["freezeJson"], complete[-8])
        v3_shape = {
            "schemaVersion": "database.lca-private-expand-freeze.v3",
            "physicalObjects": [{"objectKey": "public.example"}],
            "exposureSurfaces": [{"identity": "api.example_v1"}],
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for relative in {
                *suite["activation"]["requiredPaths"], *artifacts.values(),
            }:
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fixture\n", encoding="utf-8")
            freeze_path = root / artifacts["freezeJson"]
            freeze_path.write_text(json.dumps(v3_shape), encoding="utf-8")

            def official_verifier(command: list[str], **_kwargs: object) -> None:
                if "check-freeze" in command:
                    value = json.loads(freeze_path.read_text(encoding="utf-8"))
                    self.assertIn("physicalObjects", value)
                    self.assertIn("exposureSurfaces", value)
                    self.assertIn("--schema", command)
                elif "check-receipt" in command:
                    self.assertIn("--require-authorized", command)
                    self.assertIn("--schema", command)
                elif "check-delivery" in command:
                    self.assertIn(artifacts["apiPreExpandMigration"], command)
                    self.assertIn(artifacts["physicalCutMigration"], command)
                    self.assertIn("scripts/generate_issue_357_expand_sql.py", command)
                    self.assertIn("--require-phase-authorization", command)
                    self.assertIn("--require-exact-generated-bytes", command)

            with mock.patch.object(runner, "ROOT", root), mock.patch.object(
                runner, "run", side_effect=official_verifier,
            ) as delegated:
                runner.run_activation_verifiers(
                    artifacts, suite["activation"]["requiredPaths"],
                )
        self.assertEqual(delegated.call_count, 4)
        self.assertIn("check-freeze", delegated.call_args_list[0].args[0])
        self.assertIn("check-receipt", delegated.call_args_list[1].args[0])
        self.assertIn("check-delivery", delegated.call_args_list[2].args[0])
        self.assertIn("scripts.test_generate_issue_357_expand_sql", delegated.call_args_list[3].args[0])

    def test_official_verifier_rejects_empty_stale_unauthorized_and_malformed_inputs(self) -> None:
        suite = self.manifest["suites"]["lca-private-expand"]
        repository_files = runner.tracked_repository_files()
        complete = repository_files + suite["activation"]["requiredPaths"] + [
            "supabase/tests/contracts/lca_private_expand_freeze.v3.json",
            "supabase/tests/contracts/lca_private_expand_freeze.v3.sha256",
            "supabase/tests/contracts/lca_private_expand_freeze.v3.schema.json",
            "supabase/tests/contracts/lca_private_expand_cut_receipts.v3.json",
            "supabase/tests/contracts/lca_private_expand_cut_receipts.v3.sha256",
            "supabase/tests/contracts/lca_private_expand_cut_receipts.v3.schema.json",
            "supabase/migrations/20260802010101_issue_357_api_pre_expand.sql",
            "supabase/migrations/20260802020202_issue_357_physical_cut.sql",
        ]
        artifacts = runner.validate_activation_contract(
            "lca-private-expand", suite, complete, required=False,
        )
        cases = {
            "empty-api": {"api": ""},
            "stale-physical": {"physical": "select 'stale';\n"},
            "unauthorized-receipt": {"authorized": False},
            "malformed-freeze-schema": {"freezeSchema": "{"},
        }
        for label, override in cases.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for relative in {
                    *suite["activation"]["requiredPaths"], *artifacts.values(),
                }:
                    path = root / relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("fixture\n", encoding="utf-8")
                (root / artifacts["freezeJson"]).write_text(
                    json.dumps({
                        "schemaVersion": "database.lca-private-expand-freeze.v3",
                        "physicalObjects": [], "exposureSurfaces": [],
                    }), encoding="utf-8",
                )
                (root / artifacts["receiptJson"]).write_text(
                    json.dumps({"authorized": override.get("authorized", True)}),
                    encoding="utf-8",
                )
                (root / artifacts["freezeSchema"]).write_text(
                    override.get("freezeSchema", "{}"), encoding="utf-8",
                )
                (root / artifacts["receiptSchema"]).write_text("{}", encoding="utf-8")
                (root / artifacts["apiPreExpandMigration"]).write_text(
                    override.get("api", "-- exact generated api pre-expand\n"), encoding="utf-8",
                )
                (root / artifacts["physicalCutMigration"]).write_text(
                    override.get("physical", "-- exact generated physical cut\n"), encoding="utf-8",
                )

                def fail_closed_verifier(command: list[str], **_kwargs: object) -> None:
                    failed = False
                    if "check-freeze" in command:
                        try:
                            json.loads((root / artifacts["freezeSchema"]).read_text())
                        except json.JSONDecodeError:
                            failed = True
                    elif "check-receipt" in command:
                        receipt = json.loads((root / artifacts["receiptJson"]).read_text())
                        failed = receipt.get("authorized") is not True
                    elif "check-delivery" in command:
                        failed = (
                            (root / artifacts["apiPreExpandMigration"]).read_text()
                            != "-- exact generated api pre-expand\n"
                            or (root / artifacts["physicalCutMigration"]).read_text()
                            != "-- exact generated physical cut\n"
                        )
                    if failed:
                        raise subprocess.CalledProcessError(1, command)

                with mock.patch.object(runner, "ROOT", root), mock.patch.object(
                    runner, "run", side_effect=fail_closed_verifier,
                ), self.assertRaises(subprocess.CalledProcessError):
                    runner.run_activation_verifiers(
                        artifacts, suite["activation"]["requiredPaths"],
                    )


if __name__ == "__main__":
    unittest.main()
