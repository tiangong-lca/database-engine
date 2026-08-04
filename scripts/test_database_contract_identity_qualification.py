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
        with mock.patch.object(runner, "run") as run:
            runner.run_destructive_identity_qualification(
                enabled=True,
                contract_selected=True,
            )

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

    def test_policy_matrix_failure_propagates_and_stops_gate(self) -> None:
        failure = subprocess.CalledProcessError(
            1,
            [sys.executable, "scripts/test_identity_collaboration_policy_variants.py"],
        )
        with mock.patch.object(runner, "run", side_effect=failure) as run:
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                runner.run_destructive_identity_qualification(
                    enabled=True,
                    contract_selected=True,
                )

        self.assertIs(caught.exception, failure)
        run.assert_called_once_with([
            sys.executable,
            "scripts/test_identity_collaboration_policy_variants.py",
        ])

    def test_rollback_failure_also_propagates(self) -> None:
        failure = subprocess.CalledProcessError(
            1,
            [sys.executable, "scripts/test_identity_collaboration_rollback.py"],
        )
        with mock.patch.object(runner, "run", side_effect=[None, failure]) as run:
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                runner.run_destructive_identity_qualification(
                    enabled=True,
                    contract_selected=True,
                )

        self.assertIs(caught.exception, failure)
        self.assertEqual(2, run.call_count)

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
