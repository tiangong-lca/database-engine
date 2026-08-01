#!/usr/bin/env python3
"""Offline control-flow contract for the Issue #355 destructive runner gate."""

from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_database_contract as runner  # noqa: E402


class DatabaseContractIdentityQualificationTest(unittest.TestCase):
    def test_enabled_gate_runs_policy_matrix_then_rollback_harness(self) -> None:
        with mock.patch.object(runner, "run") as run, mock.patch.object(
            runner, "reset_local_database",
        ) as reset:
            runner.run_destructive_identity_qualification(enabled=True, contract_selected=True)

        self.assertEqual(
            [
                mock.call([
                    sys.executable,
                    "scripts/test_identity_collaboration_policy_variants.py",
                ]),
                mock.call([
                    sys.executable,
                    "scripts/test_identity_collaboration_rollback.py",
                ]),
            ],
            run.call_args_list,
        )
        self.assertEqual(
            [mock.call(version=runner.IDENTITY_QUALIFICATION_VERSION), mock.call()],
            reset.call_args_list,
        )

    def test_policy_matrix_failure_propagates_and_stops_gate(self) -> None:
        failure = subprocess.CalledProcessError(
            1,
            [sys.executable, "scripts/test_identity_collaboration_policy_variants.py"],
        )
        with mock.patch.object(runner, "run", side_effect=failure) as run, mock.patch.object(
            runner, "reset_local_database",
        ) as reset:
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                runner.run_destructive_identity_qualification(
                    enabled=True,
                    contract_selected=True,
                )

        self.assertIs(caught.exception, failure)
        self.assertEqual(
            [
                mock.call([
                    sys.executable,
                    "scripts/test_identity_collaboration_policy_variants.py",
                ]),
            ],
            run.call_args_list,
        )
        self.assertEqual(
            [mock.call(version=runner.IDENTITY_QUALIFICATION_VERSION), mock.call()],
            reset.call_args_list,
        )

    def test_rollback_failure_also_propagates(self) -> None:
        failure = subprocess.CalledProcessError(
            1,
            [sys.executable, "scripts/test_identity_collaboration_rollback.py"],
        )
        with mock.patch.object(runner, "run", side_effect=[None, failure]) as run, mock.patch.object(
            runner, "reset_local_database",
        ) as reset:
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                runner.run_destructive_identity_qualification(
                    enabled=True,
                    contract_selected=True,
                )

        self.assertIs(caught.exception, failure)
        self.assertEqual(2, run.call_count)
        self.assertEqual(2, reset.call_count)

    def test_restore_failure_propagates_after_successful_qualification(self) -> None:
        failure = subprocess.CalledProcessError(1, ["supabase", "db", "reset"])
        with mock.patch.object(runner, "run"), mock.patch.object(
            runner, "reset_local_database", side_effect=[None, failure],
        ):
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                runner.run_destructive_identity_qualification(
                    enabled=True,
                    contract_selected=True,
                )
        self.assertIs(caught.exception, failure)

    def test_original_failure_survives_restore_failure(self) -> None:
        qualification_failure = subprocess.CalledProcessError(
            1, [sys.executable, "scripts/test_identity_collaboration_policy_variants.py"],
        )
        restore_failure = subprocess.CalledProcessError(1, ["supabase", "db", "reset"])
        with mock.patch.object(runner, "run", side_effect=qualification_failure), mock.patch.object(
            runner, "reset_local_database", side_effect=[None, restore_failure],
        ):
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                runner.run_destructive_identity_qualification(
                    enabled=True,
                    contract_selected=True,
                )
        self.assertIs(caught.exception, qualification_failure)
        self.assertTrue(any("restoring" in note for note in qualification_failure.__notes__))

    def test_disabled_gate_runs_nothing(self) -> None:
        with mock.patch.object(runner, "run") as run:
            runner.run_destructive_identity_qualification(
                enabled=False,
                contract_selected=True,
            )
        run.assert_not_called()

    def test_enabled_gate_rejects_suite_without_identity_contract(self) -> None:
        with mock.patch.object(runner, "run") as run:
            with self.assertRaisesRegex(SystemExit, "does not contain"):
                runner.run_destructive_identity_qualification(
                    enabled=True,
                    contract_selected=False,
                )
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
