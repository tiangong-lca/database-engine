#!/usr/bin/env python3
"""Fail-closed offline gate for database-engine Issue #390 pre-DDL evidence."""

from __future__ import annotations

import hashlib
import json
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = (
    ROOT / "supabase/tests/contracts/lca_result_family_pre_ddl.v1.json"
)
INVENTORY_PATH = ROOT / "supabase/tests/contracts/public_object_inventory.json"
PHASE_PATH = ROOT / "supabase/tests/contracts/schema_boundary_phase.v1.json"
CATALOG_PATH = ROOT / "supabase/tests/contracts/database_catalog.json"
CATALOG_SHA_PATH = ROOT / "supabase/tests/contracts/database_catalog.sha256"
MIGRATIONS = ROOT / "supabase/migrations"

PHYSICAL_TARGETS = {
    "public.lca_factorization_registry",
    "public.lca_latest_all_unit_results",
    "public.lca_result_cache",
    "public.lca_results",
}
ROUTINE_TARGETS = {
    "public.lca_read_job_projection(p_requested_by uuid, p_worker_job_id uuid, p_legacy_job_id uuid, p_include_internal boolean)",
    "public.lca_read_latest_single_solve_result(p_requested_by uuid, p_snapshot_id uuid, p_process_index integer)",
    "public.lca_read_result_projection(p_requested_by uuid, p_result_id uuid, p_required_artifact_format text, p_include_internal boolean)",
}
ROUTINE_NAMES = {
    "lca_read_job_projection",
    "lca_read_latest_single_solve_result",
    "lca_read_result_projection",
}


def acl_has_privilege(acl: str, role: str, privilege: str) -> bool:
    for entry in acl.removeprefix("{").removesuffix("}").split(","):
        grantee, separator, grant = entry.partition("=")
        if separator and grantee == role:
            return privilege in grant.partition("/")[0]
    return False


class Issue390PreDdlGateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        cls.inventory = json.loads(INVENTORY_PATH.read_text(encoding="utf-8"))
        cls.phase = json.loads(PHASE_PATH.read_text(encoding="utf-8"))
        cls.catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))

    def test_contract_is_exactly_pre_ddl_and_not_authorized(self) -> None:
        self.assertEqual(self.contract["issue"], "tiangong-lca/database-engine#390")
        self.assertEqual(self.contract["phase"], "pre-ddl-consumer-cut")
        self.assertFalse(self.contract["ddlAuthorized"])
        self.assertEqual(set(self.contract["physicalTargets"]), PHYSICAL_TARGETS)
        self.assertEqual(set(self.contract["routineTargets"]), ROUTINE_TARGETS)

        issue_390_migrations = sorted(MIGRATIONS.glob("*issue_390*.sql"))
        self.assertEqual(
            issue_390_migrations,
            [],
            "Issue #390 DDL is forbidden while ddlAuthorized=false",
        )

    def test_checkout_descends_from_exact_dev_base_and_head_is_unchanged(self) -> None:
        base = self.contract["databaseBaseCommit"]
        ancestry = subprocess.run(
            ["git", "merge-base", "--is-ancestor", base, "HEAD"],
            cwd=ROOT,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertEqual(ancestry.returncode, 0, ancestry.stderr)

        base_migrations = subprocess.run(
            [
                "git",
                "ls-tree",
                "-r",
                base,
                "--",
                "supabase/migrations",
            ],
            cwd=ROOT,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout.splitlines()
        base_blobs = {}
        for line in base_migrations:
            metadata, path = line.split("\t", 1)
            _mode, object_type, object_sha = metadata.split()
            if object_type == "blob":
                base_blobs[path] = object_sha

        worktree_blobs = {}
        for path in sorted(MIGRATIONS.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(ROOT).as_posix()
            object_sha = subprocess.run(
                ["git", "hash-object", "--", relative],
                cwd=ROOT,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            ).stdout.strip()
            worktree_blobs[relative] = object_sha

        index_migrations = subprocess.run(
            ["git", "ls-files", "--stage", "--", "supabase/migrations"],
            cwd=ROOT,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout.splitlines()
        index_blobs = {}
        for line in index_migrations:
            metadata, path = line.split("\t", 1)
            _mode, object_sha, stage = metadata.split()
            self.assertEqual(stage, "0", f"unmerged migration index entry: {path}")
            index_blobs[path] = object_sha

        self.assertEqual(
            worktree_blobs,
            base_blobs,
            "all migration additions, deletions, renames, and content edits are forbidden while ddlAuthorized=false",
        )
        self.assertEqual(
            index_blobs,
            base_blobs,
            "the migration index must exactly match the base while ddlAuthorized=false",
        )

        versions = [
            path.name.split("_", 1)[0]
            for path in MIGRATIONS.glob("[0-9]*.sql")
            if "_" in path.name
        ]
        self.assertEqual(max(versions), self.contract["predecessorMigrationHead"])

    def test_inventory_assigns_all_seven_objects_to_private_lca_batch(self) -> None:
        targets = PHYSICAL_TARGETS | ROUTINE_TARGETS
        rows = {
            row["objectKey"]: row
            for row in self.inventory["objects"]
            if row["objectKey"] in targets
        }
        self.assertEqual(set(rows), targets)
        for row in rows.values():
            self.assertEqual(row["targetSchema"], "private")
            self.assertEqual(row["migrationBatch"], "33-lca")

        self.assertEqual(self.phase["phase"], "expand")
        self.assertFalse(self.phase["expand"]["contractReady"])
        self.assertFalse(self.inventory["contractReady"])

    def test_catalog_is_digest_bound_and_cross_checked_field_for_field(self) -> None:
        evidence = self.contract["persistentDevCatalog"]
        repository_catalog = evidence["repositoryCatalog"]
        self.assertEqual(
            repository_catalog["path"],
            "supabase/tests/contracts/database_catalog.json",
        )
        expected_digest = CATALOG_SHA_PATH.read_text(encoding="utf-8").strip()
        actual_digest = hashlib.sha256(CATALOG_PATH.read_bytes()).hexdigest()
        self.assertEqual(repository_catalog["sha256"], expected_digest)
        self.assertEqual(repository_catalog["sha256"], actual_digest)
        base = self.contract["databaseBaseCommit"]
        base_catalog = subprocess.run(
            [
                "git",
                "show",
                f"{base}:supabase/tests/contracts/database_catalog.json",
            ],
            cwd=ROOT,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).stdout
        base_digest_file = subprocess.run(
            [
                "git",
                "show",
                f"{base}:supabase/tests/contracts/database_catalog.sha256",
            ],
            cwd=ROOT,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout.strip()
        self.assertEqual(CATALOG_PATH.read_bytes(), base_catalog)
        self.assertEqual(expected_digest, base_digest_file)

        hosted = evidence["hostedObservation"]
        hosted_query = hosted["queryInput"]
        hosted_receipt = hashlib.sha256(
            json.dumps(
                hosted_query,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
        ).hexdigest()
        self.assertEqual(hosted["queryReceiptSha256"], hosted_receipt)
        self.assertEqual(hosted_query["projectRef"], evidence["projectRef"])
        self.assertEqual(hosted_query["capturedAt"], evidence["capturedAt"])
        self.assertEqual(hosted_query["migrationHead"], evidence["migrationHead"])

        catalog_relations = {
            row["name"]: row
            for row in self.catalog["relations"]
            if row["schema"] == "public"
            and f"public.{row['name']}" in PHYSICAL_TARGETS
        }
        catalog_policies = {
            name: sorted(
                (
                    row
                    for row in self.catalog["policies"]
                    if row["schemaname"] == "public" and row["tablename"] == name
                ),
                key=lambda row: row["policyname"],
            )
            for name in catalog_relations
        }

        relations = evidence["relations"]
        self.assertEqual({row["name"] for row in relations}, set(catalog_relations))
        for row in relations:
            name = row["name"]
            self.assertEqual(row["canonicalCatalog"], catalog_relations[name])
            self.assertEqual(row["policies"], catalog_policies[name])
            self.assertEqual(row["hostedOwner"], "postgres")
            acl = row["canonicalCatalog"]["acl"]
            self.assertEqual(
                row["effectiveSelect"],
                {
                    "anon": acl_has_privilege(acl, "anon", "r"),
                    "authenticated": acl_has_privilege(
                        acl, "authenticated", "r"
                    ),
                    "service_role": acl_has_privilege(acl, "service_role", "r"),
                    "api_internal_executor": acl_has_privilege(
                        acl, "api_internal_executor", "r"
                    ),
                },
            )

        hosted_relation_owners = {
            row["name"]: row["owner"] for row in hosted_query["relations"]
        }
        self.assertEqual(hosted_relation_owners, {name: "postgres" for name in catalog_relations})

        authenticated = {
            row["name"]
            for row in relations
            if row["effectiveSelect"]["authenticated"]
        }
        self.assertEqual(authenticated, {"lca_results"})
        self.assertTrue(all(row["canonicalCatalog"]["rls"] for row in relations))
        self.assertTrue(
            all(row["effectiveSelect"]["service_role"] for row in relations)
        )
        self.assertTrue(
            all(
                row["effectiveSelect"]["api_internal_executor"]
                for row in relations
            )
        )

        catalog_routines = {
            (row["name"], row["identityArguments"]): row
            for row in self.catalog["functions"]
            if row["schema"] == "public" and row["name"] in ROUTINE_NAMES
        }
        routines = evidence["routines"]
        self.assertEqual(len(routines), 3)
        self.assertEqual(
            {
                (
                    row["canonicalCatalog"]["name"],
                    row["canonicalCatalog"]["identityArguments"],
                )
                for row in routines
            },
            set(catalog_routines),
        )
        for row in routines:
            canonical = row["canonicalCatalog"]
            key = (canonical["name"], canonical["identityArguments"])
            self.assertEqual(canonical, catalog_routines[key])
            self.assertEqual(row["hostedOwner"], "postgres")
            acl = canonical["acl"]
            self.assertEqual(
                row["effectiveExecute"],
                {
                    "anon": acl_has_privilege(acl, "anon", "X"),
                    "authenticated": acl_has_privilege(
                        acl, "authenticated", "X"
                    ),
                    "service_role": acl_has_privilege(acl, "service_role", "X"),
                    "api_internal_executor": acl_has_privilege(
                        acl, "api_internal_executor", "X"
                    ),
                },
            )
            self.assertEqual(canonical["config"], ["search_path=public, pg_temp"])
            self.assertEqual(canonical["result"], "jsonb")
            self.assertTrue(canonical["securityDefiner"])

        hosted_routine_owners = {
            row["signature"]: row["owner"] for row in hosted_query["routines"]
        }
        self.assertEqual(
            hosted_routine_owners,
            {signature: "postgres" for signature in ROUTINE_TARGETS},
        )

        self.assertEqual(
            evidence["migrationHead"],
            self.contract["predecessorMigrationHead"],
        )

    def test_runtime_zero_match_is_explicitly_non_authorizing(self) -> None:
        evidence = self.contract["runtimeEvidence"]
        query = evidence["queryInput"]
        self.assertEqual(
            query["environments"],
            [
                {
                    "name": "persistentDev",
                    "projectRef": "fotofiyqnuyvgtotswie",
                    "matchedRequestCount": 0,
                },
                {
                    "name": "production",
                    "projectRef": "qgzvkongdjqiiamzbbts",
                    "matchedRequestCount": 0,
                },
            ],
        )
        self.assertEqual(query["windowStart"], "2026-08-01T15:12:00Z")
        self.assertEqual(query["windowEnd"], "2026-08-02T15:12:00Z")
        self.assertEqual(query["queryVersion"], "supabase-api-path-zero-scan.v1")
        self.assertEqual(len(query["normalizedTargetPaths"]), 7)
        self.assertEqual(len(set(query["normalizedTargetPaths"])), 7)
        self.assertTrue(
            all(path.startswith("/rest/v1/") for path in query["normalizedTargetPaths"])
        )
        receipt = hashlib.sha256(
            json.dumps(
                query,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
        ).hexdigest()
        self.assertEqual(evidence["queryReceiptSha256"], receipt)
        self.assertFalse(evidence["sufficientForAuthorization"])

        gate = self.contract["authenticatedDirectSelectGate"]
        self.assertEqual(gate["state"], "collecting")
        self.assertEqual(gate["familyWindowHours"], 24)
        self.assertEqual(gate["globalWindowHours"], 72)
        self.assertTrue(gate["resetsOnRelevantDeploymentOrContractChange"])
        self.assertFalse(gate["privateBrowserGrantAllowed"])
        self.assertEqual(
            gate["authority"],
            "https://github.com/tiangong-lca/workspace/issues/532#issuecomment-5158835898",
        )
        self.assertIn("missing", self.contract["ownerSignoffs"].values())

    def test_active_source_inventory_distinguishes_canonical_and_candidates(self) -> None:
        sources = self.contract["sourceInventory"]
        expected_canonical = {
            "edge": ("dev", "8b9629387d839bdff343a21353438a513eb54d9c", 40),
            "worker": ("main", "cabb2518a69272c20abe61692eadb292b95596f2", 28),
            "utilities": ("main", "9c00c9adb3d1afcb3173381d9247fa611cf5c0ec", 39),
            "next": ("dev", "ea70e7415c630443aa1112566e3c37f4344777e8", 1),
            "cli": ("main", "5cb359f1d0860df560c7571fa7547b2822b37c71", 0),
            "mcp": ("main", "0ab741e0881c70ce526e936d222939e38f4a4911", 0),
            "release": ("main", "f8d37018d898d23a51655272d129417eb9fad13a", 1),
            "dataFoundry": ("main", "c3c74555b71e1ba33ee80a5c5919630a27ba79df", 0),
        }
        self.assertEqual(
            {key for key, value in sources.items() if isinstance(value, dict) and "repository" in value},
            set(expected_canonical),
        )
        for name, (branch, commit, count) in expected_canonical.items():
            canonical = sources[name]["canonical"]
            self.assertEqual(canonical["branch"], branch)
            self.assertEqual(canonical["commit"], commit)
            self.assertEqual(canonical["scanMatchCount"], count)
            self.assertRegex(canonical["commit"], r"^[0-9a-f]{40}$")

        self.assertEqual(
            sources["edge"]["candidate"],
            {
                "qualifiedSourceCommit": "6080b4c2c95b00c2666f98af6bb90610042fc3da",
                "mergedAsCommit": "8b9629387d839bdff343a21353438a513eb54d9c",
            },
        )
        worker_candidate = sources["worker"]["candidate"]
        self.assertEqual(
            worker_candidate["commit"],
            "ba454b9b7f64d4b48ef51feb7e2c188ba24a3011",
        )
        predecessors = {
            row["pullRequest"]: {
                "commit": row["commit"],
                "presentInCandidate": row["presentInCandidate"],
            }
            for row in worker_candidate["requiredPredecessors"]
        }
        self.assertEqual(
            predecessors,
            {
                200: {
                    "commit": "10183162a1944252fd01eeb5ffc1548cbe8c4ec1",
                    "presentInCandidate": True,
                },
                197: {
                    "commit": "90c5ea3575efbf1918b6e0135cd72ead894c90a0",
                    "presentInCandidate": False,
                },
            },
        )
        self.assertFalse(sources["staticEvidenceComplete"])
        self.assertEqual(
            sources["next"]["canonical"]["classification"],
            "indirect-edge-http-consumer",
        )
        self.assertEqual(
            sources["cli"]["canonical"]["classification"],
            "no-direct-family-match",
        )
        self.assertEqual(
            sources["mcp"]["canonical"]["classification"],
            "no-direct-family-match",
        )


if __name__ == "__main__":
    unittest.main()
