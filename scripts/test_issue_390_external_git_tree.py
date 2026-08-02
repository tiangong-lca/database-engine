#!/usr/bin/env python3
"""Offline tests for the Issue #397 exact external Git-tree gate."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import jsonschema

from scripts import issue_390_external_git_tree as scanner


class Issue390ExternalGitTreeTest(unittest.TestCase):
    @staticmethod
    def git(repo: Path, *arguments: str) -> str:
        return subprocess.run(
            ["git", *arguments],
            cwd=repo,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout.strip()

    def make_repo(self, root: Path, name: str, canonical: str) -> Path:
        repo = root / name
        repo.mkdir()
        self.git(repo, "init", "-q")
        self.git(repo, "config", "user.name", "Issue 397 Fixture")
        self.git(repo, "config", "user.email", "issue397@example.invalid")
        self.git(repo, "remote", "add", "origin", f"git@github.com:{canonical}.git")
        return repo

    def commit_files(self, repo: Path, message: str, files: dict[str, bytes | str]) -> str:
        for relative, value in files.items():
            target = repo / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(value, bytes):
                target.write_bytes(value)
            else:
                target.write_text(value, encoding="utf-8")
        self.git(repo, "add", ".")
        self.git(repo, "commit", "-qm", message)
        return self.git(repo, "rev-parse", "HEAD")

    @staticmethod
    def spec(key: str, canonical: str, path: str, commit: str) -> dict[str, str]:
        return {
            "key": key,
            "repository": canonical,
            "workspacePath": path,
            "commit": commit,
        }

    def test_exact_commit_ignores_dirty_worktree_and_moving_head(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            repo = self.make_repo(workspace, "edge", "owner/edge")
            fixed = self.commit_files(
                repo,
                "fixed",
                {"supabase/functions/a.ts": "client.from('lca_results');\n"},
            )
            spec = self.spec("edge", "owner/edge", "edge", fixed)
            first = scanner.scan_repository(workspace, spec)
            (repo / "supabase/functions/a.ts").write_text(
                "client.from('unrelated');\n", encoding="utf-8"
            )
            self.commit_files(
                repo,
                "move head",
                {"supabase/functions/b.ts": "client.from('lca_result_cache');\n"},
            )
            second = scanner.scan_repository(workspace, spec)
            self.assertEqual(first, second)
            self.assertEqual(first["recognizedRuntimeDirectLegacyOccurrenceCount"], 1)

    def test_full_sha_and_canonical_origin_are_mandatory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            repo = self.make_repo(workspace, "edge", "owner/edge")
            commit = self.commit_files(repo, "one", {"README.md": "none\n"})
            with self.assertRaisesRegex(scanner.EvidenceError, "full lowercase SHA"):
                scanner.scan_repository(
                    workspace, self.spec("edge", "owner/edge", "edge", "HEAD")
                )
            with self.assertRaisesRegex(scanner.EvidenceError, "origin mismatch"):
                scanner.scan_repository(
                    workspace, self.spec("edge", "other/edge", "edge", commit)
                )
            with self.assertRaisesRegex(scanner.EvidenceError, "cat-file"):
                scanner.scan_repository(
                    workspace, self.spec("edge", "owner/edge", "edge", "f" * 40)
                )

    def test_unknown_path_binary_match_and_symlink_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            repo = self.make_repo(workspace, "edge", "owner/edge")
            (repo / "unknown").mkdir()
            (repo / "unknown/value.bin").write_bytes(b"\x00public.lca_results\n")
            os.symlink("value.bin", repo / "unknown/link")
            self.git(repo, "add", ".")
            self.git(repo, "commit", "-qm", "unsupported")
            commit = self.git(repo, "rev-parse", "HEAD")
            result = scanner.scan_repository(
                workspace, self.spec("edge", "owner/edge", "edge", commit)
            )
            kinds = {row["kind"] for row in result["blockers"]}
            self.assertIn("unclassified-matched-path", kinds)
            self.assertIn("binary-target-match", kinds)
            self.assertIn("unsupported-tree-entry", kinds)
            self.assertEqual(result["treeEntryCount"], 2)

    def test_large_snapshot_is_scanned_and_never_excluded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            repo = self.make_repo(workspace, "next", "owner/next")
            large = ("-- padding\n" * 200_000) + "select * from public.lca_results;\n"
            commit = self.commit_files(
                repo, "snapshot", {"docker/volumes/db/init/data.sql": large}
            )
            result = scanner.scan_repository(
                workspace, self.spec("next", "owner/next", "next", commit)
            )
            rows = result["evidence"]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["sourceKind"], "generated-snapshot")
            self.assertEqual(rows[0]["executionReachability"], "active-runtime")
            self.assertEqual(rows[0]["transport"], "sql-relation")
            source = Path(scanner.__file__).read_text(encoding="utf-8")
            self.assertNotIn(":!docker/volumes/db/init/data.sql", source)
            self.assertNotIn("git grep", source)

    def test_source_lines_are_hashed_not_retained(self) -> None:
        entry = {
            "path": "supabase/functions/a.ts",
            "blob": "a" * 40,
        }
        evidence, dynamic = scanner.evidence_for_blob(
            repo_key="edge",
            repo_name="owner/edge",
            commit="b" * 40,
            entry=entry,
            data=b"const secret='do-not-retain'; client.from('lca_results');\n",
        )
        self.assertEqual(len(evidence), 1)
        self.assertEqual(dynamic, [])
        encoded = json.dumps(evidence)
        self.assertNotIn("do-not-retain", encoded)
        self.assertNotIn("source", evidence[0])
        self.assertRegex(evidence[0]["lineSha256"], r"^[0-9a-f]{64}$")

    def test_storage_http_and_stable_api_are_not_legacy_direct_consumers(self) -> None:
        entry = {"path": "supabase/functions/a.ts", "blob": "a" * 40}
        evidence, _ = scanner.evidence_for_blob(
            repo_key="edge",
            repo_name="owner/edge",
            commit="b" * 40,
            entry=entry,
            data=(
                b"client.storage.from('lca_results');\n"
                b"const url='/functions/v1/lca_results';\n"
                b"client.rpc('lca_read_result_projection_v1');\n"
            ),
        )
        self.assertEqual(
            [row["transport"] for row in evidence],
            ["storage-token", "http-function-route", "stable-api-rpc"],
        )

    def test_edge_legacy_boundary_contract_metadata_is_not_a_direct_consumer(self) -> None:
        entry = {
            "path": "supabase/functions/_shared/capabilities/lca_result_family.ts",
            "blob": "a" * 40,
        }
        evidence, _ = scanner.evidence_for_blob(
            repo_key="edge",
            repo_name="owner/edge",
            commit="b" * 40,
            entry=entry,
            data=b"  'public.lca_results',\n",
        )
        self.assertEqual(
            evidence[0]["transport"], "legacy-boundary-contract-metadata"
        )

    def test_dynamic_selector_without_target_token_is_a_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            repo = self.make_repo(workspace, "edge", "owner/edge")
            commit = self.commit_files(
                repo,
                "dynamic",
                {
                    "supabase/functions/a.ts": "client.from(tableName);\n"
                },
            )
            result = scanner.scan_repository(
                workspace, self.spec("edge", "owner/edge", "edge", commit)
            )
            self.assertEqual(len(result["dynamicSelectorCandidates"]), 1)
            self.assertIn(
                "dynamic-selector-review-required",
                {row["kind"] for row in result["blockers"]},
            )

    def test_active_runtime_lexical_candidate_and_worker_roots_fail_closed(self) -> None:
        self.assertEqual(scanner.surface_for("worker", "crates/a/src/lib.rs"), ("native", "active-runtime"))
        self.assertEqual(scanner.surface_for("worker", "tools/check.py"), ("native", "active-runtime"))
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            repo = self.make_repo(workspace, "worker", "owner/worker")
            commit = self.commit_files(
                repo,
                "lexical",
                {"crates/a/src/lib.rs": "let name = lca_results;\n"},
            )
            result = scanner.scan_repository(
                workspace, self.spec("worker", "owner/worker", "worker", commit)
            )
            self.assertEqual(result["recognizedRuntimeDirectLegacyOccurrenceCount"], 0)
            self.assertEqual(result["unresolvedActiveRuntimeEvidenceCount"], 1)
            self.assertIn(
                "active-runtime-semantic-review-required",
                {row["kind"] for row in result["blockers"]},
            )

    def test_next_mirror_parity_and_stale_current_edge_are_distinct(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            edge = self.make_repo(workspace, "edge", "owner/edge")
            source_commit = self.commit_files(
                edge,
                "source",
                {"supabase/functions/a.ts": "client.from('lca_results');\n"},
            )
            current_commit = self.commit_files(
                edge,
                "current",
                {"supabase/functions/a.ts": "client.rpc('lca_read_result_projection_v1');\n"},
            )
            next_repo = self.make_repo(workspace, "next", "owner/next")
            receipt = {
                "repository": "https://github.com/owner/edge.git",
                "commit": source_commit,
                "sourcePath": "supabase/functions",
            }
            next_commit = self.commit_files(
                next_repo,
                "mirror",
                {
                    "docker/volumes/functions/a.ts": "client.from('lca_results');\n",
                    "docker/volumes/functions/.source-revision.json": json.dumps(receipt),
                },
            )
            specs = {
                "edge": self.spec("edge", "owner/edge", "edge", current_commit),
                "next": self.spec("next", "owner/next", "next", next_commit),
            }
            scanned = {
                key: scanner.scan_repository(workspace, value)
                for key, value in specs.items()
            }
            result = scanner.mirror_evidence(workspace, specs, scanned)
            self.assertTrue(result["matchesReceiptSource"])
            self.assertFalse(result["matchesCurrentEdgeCommit"])
            self.assertEqual(result["recognizedRuntimeDirectLegacyOccurrenceCount"], 1)
            self.assertEqual(
                {row["kind"] for row in result["blockers"]},
                {
                    "next-edge-mirror-stale-current-edge",
                    "next-edge-mirror-active-legacy-consumers",
                },
            )

    def test_mirror_receipt_identity_and_tree_parity_fail_closed(self) -> None:
        for case in ("repository", "path", "sha", "missing", "extra", "drift"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                workspace = Path(directory)
                edge = self.make_repo(workspace, "edge", "owner/edge")
                source_commit = self.commit_files(
                    edge,
                    "source",
                    {
                        "supabase/functions/a.ts": "export const a = 1;\n",
                        "supabase/functions/b.ts": "export const b = 2;\n",
                    },
                )
                next_repo = self.make_repo(workspace, "next", "owner/next")
                receipt = {
                    "repository": "https://github.com/owner/edge.git",
                    "commit": source_commit,
                    "sourcePath": "supabase/functions",
                }
                files = {
                    "docker/volumes/functions/a.ts": "export const a = 1;\n",
                    "docker/volumes/functions/b.ts": "export const b = 2;\n",
                }
                if case == "repository":
                    receipt["repository"] = "https://github.com/other/edge.git"
                elif case == "path":
                    receipt["sourcePath"] = "src"
                elif case == "sha":
                    receipt["commit"] = "f" * 40
                elif case == "missing":
                    files.pop("docker/volumes/functions/b.ts")
                elif case == "extra":
                    files["docker/volumes/functions/c.ts"] = "extra\n"
                elif case == "drift":
                    files["docker/volumes/functions/a.ts"] = "drift\n"
                files["docker/volumes/functions/.source-revision.json"] = json.dumps(receipt)
                next_commit = self.commit_files(next_repo, "mirror", files)
                specs = {
                    "edge": self.spec("edge", "owner/edge", "edge", source_commit),
                    "next": self.spec("next", "owner/next", "next", next_commit),
                }
                scanned = {
                    key: scanner.scan_repository(workspace, value)
                    for key, value in specs.items()
                }
                if case in {"repository", "path", "sha"}:
                    with self.assertRaises(scanner.EvidenceError):
                        scanner.mirror_evidence(workspace, specs, scanned)
                else:
                    result = scanner.mirror_evidence(workspace, specs, scanned)
                    self.assertFalse(result["matchesReceiptSource"])
                    self.assertIn(
                        "next-edge-mirror-source-parity-failed",
                        {row["kind"] for row in result["blockers"]},
                    )

    def test_checked_artifact_is_canonical_schema_valid_and_non_authorizing(self) -> None:
        contract = json.loads(scanner.CONTRACT_PATH.read_text(encoding="utf-8"))
        artifact = scanner.checked_artifact(contract)
        schema = json.loads(scanner.ARTIFACT_SCHEMA_PATH.read_text(encoding="utf-8"))
        jsonschema.Draft202012Validator(schema).validate(artifact)
        self.assertFalse(artifact["ddlAuthorized"])
        self.assertFalse(artifact["staticEvidenceComplete"])
        self.assertGreater(len(artifact["blockers"]), 0)

    def test_artifact_sidecar_aggregate_and_authorization_tamper_fail_closed(self) -> None:
        contract = json.loads(scanner.CONTRACT_PATH.read_text(encoding="utf-8"))
        artifact = json.loads(scanner.ARTIFACT_PATH.read_text(encoding="utf-8"))
        changed = copy.deepcopy(artifact)
        changed["ddlAuthorized"] = True
        with self.assertRaisesRegex(scanner.EvidenceError, "non-authorizing"):
            scanner.validate_artifact(changed, contract)
        changed = copy.deepcopy(artifact)
        changed["repositories"][0]["aggregates"] = []
        with self.assertRaisesRegex(scanner.EvidenceError, "aggregate"):
            scanner.validate_artifact(changed, contract)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact_path = root / "artifact.json"
            sidecar_path = root / "artifact.sha256"
            artifact_path.write_bytes(scanner.canonical_bytes(artifact))
            sidecar_path.write_text("0" * 64 + "\n", encoding="utf-8")
            with mock.patch.object(scanner, "ARTIFACT_PATH", artifact_path), mock.patch.object(
                scanner, "ARTIFACT_SHA_PATH", sidecar_path
            ):
                with self.assertRaisesRegex(scanner.EvidenceError, "sidecar"):
                    scanner.checked_artifact(contract)
            self.assertNotEqual(
                sidecar_path.read_text(encoding="utf-8").strip(),
                hashlib.sha256(artifact_path.read_bytes()).hexdigest(),
            )
            sidecar_path.write_text(
                hashlib.sha256(artifact_path.read_bytes()).hexdigest() + "\n",
                encoding="utf-8",
            )
            schema_path = root / "artifact.schema.json"
            schema_path.write_text(
                json.dumps({"type": "object", "required": ["impossible"]}),
                encoding="utf-8",
            )
            with mock.patch.object(scanner, "ARTIFACT_PATH", artifact_path), mock.patch.object(
                scanner, "ARTIFACT_SHA_PATH", sidecar_path
            ), mock.patch.object(scanner, "ARTIFACT_SCHEMA_PATH", schema_path):
                with self.assertRaisesRegex(scanner.EvidenceError, "schema validation"):
                    scanner.checked_artifact(contract)


if __name__ == "__main__":
    unittest.main()
