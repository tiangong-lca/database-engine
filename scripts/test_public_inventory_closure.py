#!/usr/bin/env python3
"""Offline unit tests for the public inventory closure builder."""

from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import public_inventory_closure as inventory


class PublicInventoryClosureTest(unittest.TestCase):
    def setUp(self) -> None:
        self.committed = json.loads(inventory.OUT.read_text(encoding="utf-8"))

    def test_target_normalization_is_fail_safe(self) -> None:
        row = {"object_key": "public.old", "object_type": "table", "object_name": "old", "candidate_target": "private_or_retire"}
        self.assertEqual(inventory.target_for(row), "private")
        row["candidate_target"] = "util_or_retire"
        self.assertEqual(inventory.target_for(row), "util")

    def test_core_table_is_always_retained(self) -> None:
        row = {"object_key": "public.processes", "object_type": "table", "object_name": "processes", "candidate_target": "private"}
        self.assertEqual(inventory.target_for(row), "public")

    def test_dynamic_and_regclass_body_dependencies_are_recorded(self) -> None:
        catalog = {
            "relations": [{"object_key": "public.jobs", "object_name": "jobs"}],
            "routines": [{
                "object_key": "public.read_jobs()", "object_name": "read_jobs",
                "definition": "create function public.read_jobs() returns void language plpgsql as $$ begin execute format('select * from public.jobs'); perform to_regclass('public.jobs'); end $$;",
            }],
        }
        edges = inventory.body_dependencies(copy.deepcopy(catalog))
        self.assertEqual(edges[0]["source_key"], "public.read_jobs()")
        self.assertEqual(edges[0]["target_key"], "public.jobs")
        self.assertEqual(edges[0]["dependency_name"], "routine-body-dynamic")

    def test_dependency_plan_groups_cycles_and_reverses_contract_order(self) -> None:
        objects = [
            {"objectKey": "public.a", "migrationBatch": "a"},
            {"objectKey": "public.b", "migrationBatch": "b"},
            {"objectKey": "public.c", "migrationBatch": "c"},
        ]
        edges = [
            {"source_key": "public.a", "target_key": "public.b"},
            {"source_key": "public.b", "target_key": "public.a"},
            {"source_key": "public.c", "target_key": "public.a"},
        ]
        plan = inventory.dependency_plan({"public.a", "public.b", "public.c"}, edges, objects)
        expand = plan["expandDependencyGroups"]
        self.assertEqual(expand[0]["objects"], ["public.a", "public.b"])
        self.assertTrue(expand[0]["cyclic"])
        self.assertEqual(expand[1]["objects"], ["public.c"])
        self.assertEqual(plan["contractDependencyGroups"][0]["objects"], ["public.c"])

    def test_source_metadata_uses_reviewed_immutable_full_shas(self) -> None:
        inventory.validate_source(inventory.SOURCE)
        self.assertNotIn("origin/dev", json.dumps(inventory.SOURCE, sort_keys=True))

    def test_source_metadata_tamper_fails_closed(self) -> None:
        tampered = dict(inventory.SOURCE)
        tampered["databaseSchemaSha"] = tampered["databaseInventorySha"]
        with self.assertRaisesRegex(ValueError, "differs from the reviewed immutable inputs"):
            inventory.validate_source(tampered)

    def test_missing_source_sha_fails_closed(self) -> None:
        missing = dict(inventory.SOURCE)
        del missing["databaseMergeBaseSha"]
        with self.assertRaisesRegex(ValueError, "differs from the reviewed immutable inputs"):
            inventory.validate_source(missing)

    def test_unreachable_immutable_sha_fails_closed(self) -> None:
        unreachable = dict(inventory.SOURCE)
        unreachable["databaseSchemaSha"] = "f" * 40
        with self.assertRaisesRegex(ValueError, "missing or unreachable"):
            inventory.validate_source(unreachable, require_expected=False)

    def test_moving_remote_does_not_change_fixed_source_or_merge_base(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory) / "database"
            repo.mkdir()

            def git(*args: str) -> str:
                result = subprocess.run(
                    ["git", *args], cwd=repo, check=True, text=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
                return result.stdout.strip()

            git("init", "-q")
            git("config", "user.name", "Inventory Fixture")
            git("config", "user.email", "inventory@example.invalid")
            primary_branch = git("symbolic-ref", "--short", "HEAD")

            def commit(name: str) -> str:
                (repo / "fixture.txt").write_text(name, encoding="utf-8")
                git("add", "fixture.txt")
                git("commit", "-q", "-m", name)
                return git("rev-parse", "HEAD")

            catalog = commit("catalog")
            merge_base = commit("merge-base")
            git("branch", "inventory")
            base = commit("review-base")
            git("checkout", "-q", "inventory")
            inventory_sha = commit("inventory")
            schema_sha = commit("schema-replay")
            git("update-ref", "refs/remotes/origin/dev", base)

            source = {
                **inventory.SOURCE,
                "workspacePinnedDatabaseSha": catalog,
                "databaseSchemaSha": schema_sha,
                "databaseBaseSha": base,
                "databaseInventorySha": inventory_sha,
                "databaseMergeBaseSha": merge_base,
            }
            inventory.validate_source(source, database_repo=repo, require_expected=False)

            git("checkout", "-q", primary_branch)
            moved_remote = commit("moved-remote")
            git("update-ref", "refs/remotes/origin/dev", moved_remote)
            inventory.validate_source(source, database_repo=repo, require_expected=False)
            self.assertEqual(source["databaseMergeBaseSha"], merge_base)

    def test_workspace_baseline_replays_exact_database_gitlink(self) -> None:
        workspace = Path("/tmp/immutable-workspace-fixture")

        def fixed_git(repo: Path, *args: str) -> str:
            if repo == workspace and args[0] == "ls-tree":
                return f"160000 commit {inventory.SOURCE['workspacePinnedDatabaseSha']}\tdatabase-engine"
            if args[0] == "merge-base" and "--is-ancestor" not in args:
                return inventory.SOURCE["databaseMergeBaseSha"]
            return ""

        with mock.patch.object(inventory, "git_output", side_effect=fixed_git):
            inventory.validate_source(inventory.SOURCE, workspace_repo=workspace)

    def test_hash_tamper_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory) / "inventory.json"
            digest = Path(directory) / "inventory.sha256"
            payload = inventory.canonical({"source": inventory.SOURCE})
            out.write_text(payload, encoding="utf-8")
            digest.write_text("0" * 64 + "\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "SHA-256 does not match"):
                inventory.verify_committed_artifacts(out, digest)

    def test_two_generations_are_byte_identical(self) -> None:
        first = inventory.canonical(copy.deepcopy(self.committed)).encode("utf-8")
        second = inventory.canonical(copy.deepcopy(self.committed)).encode("utf-8")
        self.assertEqual(first, second)
        self.assertEqual(hashlib.sha256(first).digest(), hashlib.sha256(second).digest())

    def test_schema_object_edge_count_and_contract_ready_tamper_fail_closed(self) -> None:
        mutations = []
        schema = copy.deepcopy(self.committed)
        schema["schemaVersion"] = "tampered"
        mutations.append(schema)
        objects = copy.deepcopy(self.committed)
        objects["objects"].pop()
        mutations.append(objects)
        edges = copy.deepcopy(self.committed)
        edges["dependencies"].pop()
        mutations.append(edges)
        counts = copy.deepcopy(self.committed)
        counts["counts"]["function"] -= 1
        mutations.append(counts)
        ready = copy.deepcopy(self.committed)
        ready["contractReady"] = True
        mutations.append(ready)
        for contract in mutations:
            with self.subTest(contract=contract.get("schemaVersion")):
                with self.assertRaises(ValueError):
                    inventory.validate(contract)

    def test_canonical_runner_keeps_committed_vs_generated_gate(self) -> None:
        runner = (inventory.ROOT / "scripts/run_database_contract.py").read_text(encoding="utf-8")
        self.assertIn('"scripts/public_inventory_closure.py", "--check"', runner)


if __name__ == "__main__":
    unittest.main()
