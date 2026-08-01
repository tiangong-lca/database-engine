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


if __name__ == "__main__":
    unittest.main()
