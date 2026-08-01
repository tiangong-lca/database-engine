#!/usr/bin/env python3
"""Offline safety tests for the production-equivalent upgrade runner."""

from __future__ import annotations

import contextlib
import io
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import test_production_equivalent_upgrade as runner


class ProductionEquivalentUpgradeSafetyTest(unittest.TestCase):
    def test_both_database_url_schemes_are_redacted_from_command_and_errors(self) -> None:
        credential = "user:" + "redaction-marker"
        urls = (
            f"postgres://{credential}@127.0.0.1:5432/postgres",
            f"postgresql://{credential}@localhost:5432/postgres",
        )
        output = io.StringIO()
        with contextlib.redirect_stdout(output), self.assertRaises(SystemExit) as raised:
            runner.run(
                [
                    sys.executable,
                    "-c",
                    "import sys; print(sys.argv[1]); print(sys.argv[2], file=sys.stderr); raise SystemExit(1)",
                    *urls,
                ]
            )
        rendered = output.getvalue() + str(raised.exception)
        self.assertNotIn("redaction-marker", rendered)
        self.assertNotIn("postgres://", rendered)
        self.assertNotIn("postgresql://", rendered)
        self.assertGreaterEqual(rendered.count("<database-url>"), 2)

    def test_evidence_is_new_private_and_existing_target_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "evidence.json"
            prepared = runner.prepare_evidence_target(target)
            runner.write_new_evidence(prepared, {"status": "passed"})
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
            original = target.read_bytes()
            with self.assertRaisesRegex(SystemExit, "new path"):
                runner.prepare_evidence_target(target)
            self.assertEqual(target.read_bytes(), original)

    def test_symlink_evidence_target_is_refused_without_touching_destination(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "destination"
            destination.write_text("preserve", encoding="utf-8")
            target = Path(directory) / "evidence.json"
            os.symlink(destination, target)
            with self.assertRaisesRegex(SystemExit, "symlinks"):
                runner.prepare_evidence_target(target)
            self.assertEqual(destination.read_text(encoding="utf-8"), "preserve")

    def test_worktree_evidence_target_is_refused(self) -> None:
        target = runner.ROOT / ".issue-341-evidence-must-not-exist"
        self.assertFalse(target.exists())
        with self.assertRaisesRegex(SystemExit, "outside the worktree"):
            runner.prepare_evidence_target(target)
        self.assertFalse(target.exists())

    def test_retry_wal_contract_requires_exact_zero(self) -> None:
        runner.assert_retry_wal_bytes(0, 0)
        with self.assertRaisesRegex(SystemExit, "expected=0, actual=1"):
            runner.assert_retry_wal_bytes(1, 0)

    def test_new_head_relation_is_allowed_but_base_relation_drift_is_rejected(self) -> None:
        base = {"public.rows": {"rowCount": 1}}
        runner.assert_base_relations_preserved(
            base,
            {**base, "archive.acl_snapshot": {"rowCount": 10}},
        )
        with self.assertRaisesRegex(SystemExit, "changed=.*public.rows"):
            runner.assert_base_relations_preserved(
                base,
                {"public.rows": {"rowCount": 2}},
            )

    def test_reviewed_physical_relation_move_preserves_data_oracle(self) -> None:
        oracle = {"rowCount": 2, "primaryKeyHash": "pk", "rowHash": "rows"}
        runner.assert_base_relations_preserved(
            {"public.worker_jobs": oracle},
            {"private.worker_jobs": oracle},
            {"public.worker_jobs": "private.worker_jobs"},
        )
        with self.assertRaisesRegex(SystemExit, "did not occur exactly"):
            runner.assert_base_relations_preserved(
                {"public.worker_jobs": oracle},
                {"public.worker_jobs": oracle, "private.worker_jobs": oracle},
                {"public.worker_jobs": "private.worker_jobs"},
            )

    def test_configured_head_must_be_repository_latest_numeric_migration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            migrations = Path(directory)
            for name in ("100_base.sql", "200_expected.sql", "300_later.sql"):
                (migrations / name).write_text("select 1;\n", encoding="utf-8")
            with mock.patch.object(runner, "MIGRATIONS", migrations):
                with self.assertRaisesRegex(
                    SystemExit,
                    "expected migration head 200 is not repository latest numeric migration 300",
                ):
                    runner.migration_files("100", "200")


if __name__ == "__main__":
    unittest.main()
