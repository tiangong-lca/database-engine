#!/usr/bin/env python3
"""Offline fail-closed tests for the SECURITY DEFINER transition runner."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
import unittest
import urllib.parse
from pathlib import Path
from unittest import mock

import scripts.run_database_contract as contract_runner
import scripts.test_security_definer_audit_v2_transition_integration as harness


def database_uri(
    authority_host: str, *, user: str = "postgres", credential: str | None = None,
    path: str = "/postgres", query: str = "", fragment: str = "",
) -> str:
    identity = user if credential is None else ":".join((user, credential))
    authority = "@".join((identity, authority_host))
    return urllib.parse.urlunsplit(("postgresql", authority, path, query, fragment))


class TransitionIntegrationRunnerTest(unittest.TestCase):
    def test_connection_secret_uses_only_pgpassword_and_catalog_query_is_quiet(self) -> None:
        secret = "not-for-argv-or-errors"
        connection = harness.parse_connection(
            database_uri("127.0.0.1:54322", credential=secret)
        )
        command = connection.command("-c", "select 1")
        self.assertNotIn(secret, command)
        with mock.patch.dict(os.environ, {"DATABASE_URL": database_uri(
                                              "host", user="x", credential=secret, path="/db"),
                                          "PGOPTIONS": "-c search_path=evil"}):
            environment = connection.environment()
        self.assertEqual(environment["PGPASSWORD"], secret)
        self.assertNotIn("DATABASE_URL", environment)
        self.assertNotIn("PGOPTIONS", environment)
        with mock.patch.object(harness, "capture", return_value="[]") as capture:
            self.assertEqual(harness.query(connection, harness.audit.CATALOG_QUERY,
                                           cwd=Path(".")), "[]")
        invoked = capture.call_args.args[0]
        self.assertIn("-qAtX", invoked)
        self.assertNotIn(secret, invoked)
        completed = mock.Mock(stdout="[]")
        with mock.patch.object(harness.subprocess, "run", return_value=completed) as run:
            self.assertEqual(harness.load_catalog(connection, cwd=Path(".")), {})
        invoked = run.call_args.args[0]
        self.assertIn("-qAtX", invoked)
        self.assertNotIn(secret, invoked)
        self.assertEqual(run.call_args.kwargs["input"], harness.audit.catalog_proof_query())

    def test_database_url_rejects_query_multihost_and_socket_without_echoing_secret(self) -> None:
        secret = "never-echo-this"
        rejected = (
            database_uri("127.0.0.1:54322", credential=secret, query="sslmode=disable"),
            database_uri("127.0.0.1,localhost:54322", credential=secret),
            database_uri("", credential=secret, query="host=/tmp/postgres.sock"),
        )
        for value in rejected:
            with self.subTest(value=value), self.assertRaises(ValueError) as caught:
                harness.parse_connection(value)
            self.assertNotIn(secret, str(caught.exception))
        target, environment = contract_runner.database_cli_target(
            database_uri("127.0.0.1:54322", credential=secret)
        )
        self.assertNotIn(secret, target)
        self.assertEqual(environment["PGPASSWORD"], secret)
        self.assertNotIn("DATABASE_URL", environment)
        with self.assertRaises(SystemExit) as caught:
            contract_runner.database_cli_target(
                database_uri("%2Ftmp:54322", credential=secret)
            )
        self.assertNotIn(secret, str(caught.exception))
        with self.assertRaises(SystemExit) as caught:
            contract_runner.database_cli_target(
                database_uri("example.com:54322", credential=secret)
            )
        self.assertNotIn(secret, str(caught.exception))

    def test_canonical_database_url_resolves_and_validates_the_active_stack(self) -> None:
        url = database_uri("127.0.0.1:55322", credential="local-test-value")
        completed = mock.Mock(stdout=json.dumps({"DB_URL": url}))
        with mock.patch.dict(os.environ, {}, clear=True), \
                mock.patch.object(contract_runner.subprocess, "run", return_value=completed) as run:
            self.assertEqual(contract_runner.canonical_database_url(), url)
        self.assertEqual(
            run.call_args.args[0], ["supabase", "status", "--output", "json"],
        )
        self.assertEqual(run.call_args.kwargs["cwd"], contract_runner.ROOT)

        with mock.patch.dict(
                os.environ,
                {"DATABASE_URL": database_uri("remote:5432", user="user", path="/db")},
                clear=True,
        ), \
                mock.patch.object(contract_runner.subprocess, "run") as run:
            with self.assertRaisesRegex(SystemExit, "literal loopback"):
                contract_runner.canonical_database_url()
        run.assert_not_called()

    def test_same_normalized_identity_and_same_system_identifier_fail_closed(self) -> None:
        first = {
            "host": "127.0.0.1", "port": 54322, "database": "postgres",
            "user": "postgres", "systemIdentifier": "100",
        }
        with self.assertRaisesRegex(ValueError, "same normalized database identity"):
            harness.validate_independent_identities([first, dict(first)])
        second_endpoint_same_cluster = {**first, "port": 54323}
        with self.assertRaisesRegex(ValueError, "system_identifier"):
            harness.validate_independent_identities([first, second_endpoint_same_cluster])
        independent = {**second_endpoint_same_cluster, "systemIdentifier": "200"}
        harness.validate_independent_identities([first, independent])

    def test_dirty_repository_input_fails_before_config_or_database_access(self) -> None:
        root = Path("/tmp/qualification-root")
        with mock.patch.object(harness, "git_root", return_value=root), \
                mock.patch.object(harness, "exact_head", return_value="a" * 40), \
                mock.patch.object(harness, "capture", return_value=" M supabase/migrations/dirty.sql"):
            with self.assertRaisesRegex(ValueError, "non-local-config changes"):
                harness.validate_stack_root(root, "a" * 40)

    def test_receipt_byte_tamper_fails_against_reviewed_hash(self) -> None:
        raw = b'{"tampered":true}\n'
        args = argparse.Namespace(
            qualification_receipt=Path("receipt.json"),
            expected_qualification_receipt_sha256=hashlib.sha256(b"reviewed").hexdigest(),
        )
        reviewed = harness.ReviewedFile("receipt.json", raw)
        with mock.patch.object(harness, "validate_source_file", return_value=reviewed):
            with self.assertRaisesRegex(ValueError, "receipt bytes differ"):
                harness.validate_receipt(args, Path("."))

    def test_reviewed_paths_require_exact_canonical_relative_spelling(self) -> None:
        accepted = ("receipt.json", "supabase/migrations/20260801060304_move.sql")
        for value in accepted:
            with self.subTest(value=value):
                self.assertEqual(harness.canonical_relative_path(value), value)
        rejected = (
            "", "/tmp/receipt.json", "../receipt.json", "a/../receipt.json",
            "./receipt.json", "a//receipt.json", "a\\receipt.json", "a/receipt.json/",
        )
        for value in rejected:
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "canonical"):
                harness.canonical_relative_path(value)

    def test_git_metadata_rejects_untracked_non_stage0_and_non_regular_inputs(self) -> None:
        relative = "receipt.json"
        object_id = "a" * 40
        rejected = (
            (b"", "index", "not one"),
            (f"100644 {object_id} 1\t{relative}\0".encode(), "index", "stage-0"),
            (f"120000 {object_id} 0\t{relative}\0".encode(), "index", "git regular"),
            (f"160000 commit {object_id}\t{relative}\0".encode(), "HEAD", "HEAD blob"),
        )
        for record, source, message in rejected:
            with self.subTest(source=source, message=message), self.assertRaisesRegex(ValueError, message):
                harness._git_regular_blob(record, relative, source=source)

    def test_no_follow_reader_rejects_symlink_and_non_regular_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target.json"
            target.write_text("{}\n", encoding="utf-8")
            os.symlink(target.name, root / "link.json")
            with self.assertRaisesRegex(ValueError, "without following links"):
                harness.read_nofollow(root, "link.json")
            (root / "directory.json").mkdir()
            with self.assertRaisesRegex(ValueError, "filesystem regular"):
                harness.read_nofollow(root, "directory.json")
            if hasattr(os, "mkfifo"):
                os.mkfifo(root / "fifo.json")
                with self.assertRaisesRegex(ValueError, "filesystem regular"):
                    harness.read_nofollow(root, "fifo.json")

    def test_reviewed_file_must_be_tracked_at_head_and_byte_exact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            tracked = root / "tracked.sql"
            tracked.write_text("select 1;\n", encoding="utf-8")
            subprocess.run(["git", "add", "tracked.sql"], cwd=root, check=True)
            subprocess.run([
                "git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                "commit", "-q", "-m", "fixture",
            ], cwd=root, check=True)
            reviewed = harness.validate_source_file("tracked.sql", root)
            self.assertEqual(reviewed.relative, "tracked.sql")
            self.assertEqual(reviewed.raw, b"select 1;\n")

            (root / "untracked.sql").write_text("select 2;\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "not one index"):
                harness.validate_source_file("untracked.sql", root)

            tracked.write_text("select 3;\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "differs from source commit"):
                harness.validate_source_file("tracked.sql", root)

            subprocess.run(["git", "add", "tracked.sql"], cwd=root, check=True)
            tracked.write_text("select 1;\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "index differs from HEAD"):
                harness.validate_source_file("tracked.sql", root)

    def test_source_sql_is_read_from_reviewed_ancestor_commit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            tracked = root / "tracked.sql"
            tracked.write_text("select 1;\n", encoding="utf-8")
            subprocess.run(["git", "add", "tracked.sql"], cwd=root, check=True)
            subprocess.run([
                "git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                "commit", "-q", "-m", "source",
            ], cwd=root, check=True)
            source_commit = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=root, check=True, text=True,
                stdout=subprocess.PIPE,
            ).stdout.strip()
            tracked.write_text("select 2;\n", encoding="utf-8")
            subprocess.run(["git", "add", "tracked.sql"], cwd=root, check=True)
            subprocess.run([
                "git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                "commit", "-q", "-m", "evidence",
            ], cwd=root, check=True)
            reviewed = harness.validate_commit_source_file("tracked.sql", root, source_commit)
            self.assertEqual(reviewed.raw, b"select 1;\n")
            head = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=root, check=True, text=True,
                stdout=subprocess.PIPE,
            ).stdout.strip()
            harness.require_ancestor(source_commit, head, source_root=root, label="source")
            with self.assertRaisesRegex(ValueError, "not an ancestor"):
                harness.require_ancestor(head, source_commit, source_root=root, label="source")

    def test_pending_migration_detection_is_version_exact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            contracts = root / "supabase/tests/contracts"
            migrations = root / "supabase/migrations"
            contracts.mkdir(parents=True)
            migrations.mkdir(parents=True)
            fixture = contracts / "security_definer_transition_fixture.v1.json"
            raw = (json.dumps({"source": {"migrationVersion": "20260801060304"}},
                              sort_keys=True, separators=(",", ":")) + "\n").encode()
            fixture.write_bytes(raw)
            fixture.with_suffix(".sha256").write_text(hashlib.sha256(raw).hexdigest() + "\n",
                                                       encoding="utf-8")
            with mock.patch.object(contract_runner, "ROOT", root), \
                    mock.patch.object(contract_runner, "TRANSITION_FIXTURE", fixture):
                self.assertEqual(contract_runner.pending_security_definer_transition(), [])
                migration = migrations / "20260801060304_move_worker.sql"
                migration.write_text("select 1;\n", encoding="utf-8")
                self.assertEqual(contract_runner.pending_security_definer_transition(), [migration])

    def test_pending_migration_requires_flags_and_transition_forbids_suite_skips(self) -> None:
        pending = contract_runner.ROOT / "supabase/migrations/20260801060304_pending.sql"
        with mock.patch.object(contract_runner, "pending_security_definer_transition",
                               return_value=[pending]), \
                mock.patch("sys.argv", ["run_database_contract.py"]):
            with self.assertRaisesRegex(SystemExit, "requires every exact input.*pending migration"):
                contract_runner.main()
        complete = [
            "run_database_contract.py", "--skip-reset",
            "--security-definer-transition-workdir", "a",
            "--security-definer-transition-workdir", "b",
            "--security-definer-transition-source-workdir", "source",
            "--security-definer-transition-migration", "migration.sql",
            "--security-definer-transition-rollback", "rollback.sql",
            "--security-definer-transition-qualification-receipt", "receipt.json",
            "--security-definer-transition-qualification-receipt-sha256", "a" * 64,
            "--security-definer-transition-migration-sha256", "b" * 64,
            "--security-definer-transition-rollback-sha256", "c" * 64,
            "--security-definer-transition-base", "d" * 40,
        ]
        with mock.patch.object(contract_runner, "pending_security_definer_transition", return_value=[]), \
                mock.patch("sys.argv", complete):
            with self.assertRaisesRegex(SystemExit, "complete canonical-local CI mode"):
                contract_runner.main()


if __name__ == "__main__":
    unittest.main()
