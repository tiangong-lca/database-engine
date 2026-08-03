#!/usr/bin/env python3
"""Offline tests for the non-authorizing Issue #390 qualification harness."""

from __future__ import annotations

import copy
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import jsonschema

from scripts import issue_390_physical_qualification as harness


class Issue390PhysicalQualificationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.plan = json.loads(harness.CONTRACT_PATH.read_text(encoding="utf-8"))
        cls.schema = json.loads(harness.SCHEMA_PATH.read_text(encoding="utf-8"))

    def valid_receipt(self) -> dict[str, object]:
        relations = []
        for ordinal, identity in enumerate(harness.EXPECTED_RELATIONS, start=1):
            schema_name, name = identity.split(".", 1)
            relations.append({
                "schema_name": schema_name,
                "name": name,
                "identity": identity,
                "oid": 1000 + ordinal,
                "composite_type_oid": 2000 + ordinal,
                "relkind": "r",
                "owner": "postgres",
                "data_oracle": {
                    "rowCount": 0,
                    "withinCaptureBudget": True,
                    "digestComplete": True,
                    "digestAlgorithm": "sha256",
                    "digestEncoding": "ordered-jsonb-text-lines-v1",
                    "primaryKeySha256": harness.sha256(b""),
                    "contentSha256": harness.sha256(b""),
                },
            })
        routines = []
        for ordinal, signature in enumerate(harness.EXPECTED_ROUTINES, start=1):
            routines.append({
                "planned_signature": signature,
                "observed_signature": signature,
                "schema_name": "public",
                "proname": signature.split("(", 1)[0].split(".", 1)[1],
                "routine_kind": "f",
                "oid": 3000 + ordinal,
                "owner": "postgres",
                "definition_sha256": harness.sha256(signature.encode()),
            })
        return {
            "schemaVersion": "database.lca-result-family-baseline-receipt.v1",
            "nonAuthorizing": True,
            "databaseProvenance": {
                "database_name": "postgres",
                "database_oid": 5,
                "server_address": "127.0.0.1",
                "server_port": 54322,
                "system_identifier": "7612345678901234567",
                "server_version": "18.4",
                "applied_migration_head": harness.EXPECTED_MIGRATION_HEAD,
                "applied_migration_count": harness.EXPECTED_MIGRATION_COUNT,
                "applied_migration_versions": harness.committed_migration_versions(
                    harness.EXPECTED_SOURCE_COMMIT
                ),
            },
            "operatorProof": {
                "current_role": "postgres",
                "role_super": True,
                "role_bypass_rls": True,
                "owns_all_targets_without_forced_rls": True,
                "fullRowVisibility": True,
            },
            "relations": relations,
            "routines": routines,
            "dependencyClosure": {
                "pgDependObjects": [],
                "pgDependEdges": [],
                "viewRewriteCandidates": [],
                "policyObjects": [],
                "publicationMemberships": [],
                "routineLexicalCandidates": [],
            },
        }

    def test_checked_plan_is_exact_schema_valid_and_non_authorizing(self) -> None:
        jsonschema.Draft202012Validator(self.schema).validate(self.plan)
        checked = harness.validate_contract()
        self.assertEqual(checked, self.plan)
        self.assertEqual(
            checked["artifactBinding"]["schemaSha256"],
            harness.sha256(harness.SCHEMA_PATH.read_bytes()),
        )
        authorization = checked["authorization"]
        for flag in (*harness.AUTHORIZATION_FLAGS, "qualificationExecutionAllowed"):
            self.assertIs(authorization[flag], False)
        self.assertEqual(checked["candidate"], {
            "migrationPath": None,
            "migrationSha256": None,
            "rollbackPath": None,
            "rollbackSha256": None,
            "populatedFixturePath": None,
            "populatedFixtureSha256": None,
            "targetMigrationHead": None,
        })

    def test_exact_source_scope_and_predecessor_are_bound(self) -> None:
        self.assertEqual(self.plan["source"]["commitSha"], harness.EXPECTED_SOURCE_COMMIT)
        self.assertEqual(
            harness.committed_migration_head(harness.EXPECTED_SOURCE_COMMIT),
            harness.EXPECTED_MIGRATION_HEAD,
        )
        versions = harness.committed_migration_versions(harness.EXPECTED_SOURCE_COMMIT)
        self.assertEqual(len(versions), harness.EXPECTED_MIGRATION_COUNT)
        self.assertEqual(
            harness.migration_set_sha256(versions), harness.EXPECTED_MIGRATION_SET_SHA256
        )
        self.assertEqual(self.plan["scope"]["relations"], harness.EXPECTED_RELATIONS)
        self.assertEqual(self.plan["scope"]["routines"], harness.EXPECTED_ROUTINES)
        self.assertEqual(len(self.plan["scope"]["relations"]), 4)
        self.assertEqual(len(self.plan["scope"]["routines"]), 3)

    def test_current_run_plan_exposes_every_blocker_and_phase(self) -> None:
        run_plan = harness.qualification_plan(self.plan)
        self.assertFalse(run_plan["executable"])
        blockers = set(run_plan["blockers"])
        for flag in harness.AUTHORIZATION_FLAGS:
            self.assertIn(f"qualification-plan:{flag}=false", blockers)
            self.assertIn(f"pre-ddl-contract:{flag}=false", blockers)
        self.assertIn("qualification-plan:qualificationExecutionAllowed=false", blockers)
        self.assertEqual(run_plan["phases"], [
            "fresh-upgrade", "populated-upgrade", "failure-atomicity", "lock-timeout",
            "wal-and-time-budget", "retry", "rollback", "roll-forward",
        ])
        self.assertEqual(
            set(run_plan["requiredInputs"]),
            {"freshDatabaseUrl", "populatedDatabaseUrl", "receiptDirectory", "candidateBindings"},
        )

    def test_baseline_query_covers_catalog_data_and_dependency_closure(self) -> None:
        sql = harness.BASELINE_SQL.lower()
        for token in (
            "transaction isolation level repeatable read read only",
            "pg_depend", "pg_constraint", "pg_rewrite", "pg_get_ruledef",
            "pg_get_functiondef", "prosrc", "reltype", "%rowtype", "regclass",
            "dynamic_sql_candidate", "pg_policies", "pg_publication_tables",
            "pg_get_indexdef", "pg_get_triggerdef", "relrowsecurity", "relforcerowsecurity",
            "rowcount", "primarykeysha256", "contentsha256", "confdeltype", "confupdtype",
            "pg_policy", "pg_publication_rel", "policyobjects", "publicationmemberships",
            "refclassid='pg_catalog.pg_class'::regclass", "pg_control_system",
            "applied_migration_versions", "system_identifier", "fullrowvisibility",
        ):
            self.assertIn(token, sql)
        for relation in (name.split(".", 1)[1] for name in harness.EXPECTED_RELATIONS):
            self.assertIn(relation, sql)
        for routine in (name.split("(", 1)[0].split(".", 1)[1] for name in harness.EXPECTED_ROUTINES):
            self.assertIn(routine, sql)
        self.assertNotRegex(sql, r"\b(alter|drop|truncate|insert|update|delete|create)\s+(table|function|view|schema)\b")
        self.assertNotIn("hashtextextended", sql)
        self.assertNotRegex(sql, r"\bsum\s*\(")
        self.assertIn("'gi'", sql)
        self.assertIn("app.classid=neighbor.classid", sql)
        self.assertIn("app.objid=neighbor.objid", sql)
        self.assertNotIn("using (classid,objid)", sql)

    def test_capture_is_loopback_only_read_only_and_non_authorizing(self) -> None:
        with self.assertRaisesRegex(ValueError, "loopback"):
            harness.assert_loopback("postgresql://postgres@example.com:5432/postgres")
        captured = self.valid_receipt()
        with mock.patch.object(harness, "psql_json", return_value=captured) as query:
            receipt = harness.capture_baseline(
                self.plan, "postgresql://postgres@127.0.0.1:54322/postgres"
            )
        query.assert_called_once_with(
            "postgresql://postgres@127.0.0.1:54322/postgres", harness.BASELINE_SQL
        )
        self.assertTrue(receipt["nonAuthorizing"])
        self.assertFalse(receipt["capture"]["authorizationClaim"])
        self.assertFalse(receipt["capture"]["disposableDatabaseClaim"])
        self.assertNotIn("source", receipt)
        self.assertEqual(receipt["repositoryPlan"], self.plan["source"])
        self.assertEqual(receipt["scope"], self.plan["scope"])

    def test_database_password_is_removed_from_psql_argv(self) -> None:
        encoded_component = "fixture" + "%40" + "value"
        with mock.patch.dict(os.environ, {
            "PATH": "/usr/bin", "PGPASSWORD": "ambient", "PGOPTIONS": "-c role=bad",
            "PGSERVICE": "bad", "DATABASE_URL": "postgresql://ambient",
            "SUPABASE_DB_URL": "postgresql://ambient-supabase",
        }, clear=True):
            target, environment = harness.psql_target(
                "postgresql://postgres:" + encoded_component + "@127.0.0.1:54322/postgres"
            )
        self.assertEqual(target, "postgresql://postgres@127.0.0.1:54322/postgres")
        self.assertEqual(environment["PGPASSWORD"], "fixture@value")
        self.assertEqual(environment["PGSSLMODE"], "disable")
        self.assertEqual(environment["PATH"], "/usr/bin")
        self.assertNotIn("PGOPTIONS", environment)
        self.assertNotIn("PGSERVICE", environment)
        self.assertNotIn("DATABASE_URL", environment)
        self.assertNotIn("SUPABASE_DB_URL", environment)
        self.assertNotIn(encoded_component, target)

    def test_live_sql_failure_surfaces_psql_diagnostic(self) -> None:
        failure = harness.subprocess.CalledProcessError(
            3, ["psql"], stderr="ERROR: ambiguous column reference"
        )
        with mock.patch.object(harness.subprocess, "run", side_effect=failure):
            with self.assertRaisesRegex(
                RuntimeError, "baseline SQL execution failed: ERROR: ambiguous"
            ):
                harness.psql_json(
                    "postgresql://postgres@127.0.0.1:54322/postgres", "select 1"
                )

    def test_no_current_state_can_reach_destructive_execution(self) -> None:
        self.assertTrue(harness.authorization_blockers(self.plan))
        changed = copy.deepcopy(self.plan)
        changed["authorization"]["ddlAuthorized"] = True
        with tempfile.TemporaryDirectory() as directory:
            contract_path = Path(directory) / "contract.json"
            sidecar_path = Path(directory) / "contract.sha256"
            raw = json.dumps(changed).encode()
            contract_path.write_bytes(raw)
            sidecar_path.write_text(harness.sha256(raw) + "\n", encoding="utf-8")
            with mock.patch.object(harness, "CONTRACT_PATH", contract_path), mock.patch.object(
                harness, "SHA_PATH", sidecar_path
            ):
                with self.assertRaises(jsonschema.ValidationError):
                    harness.validate_contract()

    def test_artifact_set_contains_no_issue_390_physical_ddl(self) -> None:
        scaffold = harness.scaffold_commit()
        parent = harness.git("rev-parse", f"{scaffold}^")
        self.assertEqual(parent, harness.EXPECTED_SOURCE_COMMIT)
        changed = [
            path for path in harness.git(
                "diff", "--name-only", f"{parent}..{scaffold}"
            ).splitlines()
            if path
        ]
        self.assertFalse(any(path.startswith("supabase/migrations/") for path in changed))
        self.assertFalse(any(path.startswith("supabase/operator/") for path in changed))

    def test_database_migration_provenance_drift_fails_closed(self) -> None:
        for mutation in ("missing-version", "wrong-head", "wrong-count"):
            receipt = self.valid_receipt()
            database = receipt["databaseProvenance"]
            if mutation == "missing-version":
                database["applied_migration_versions"] = database[
                    "applied_migration_versions"
                ][:-1]
            elif mutation == "wrong-head":
                database["applied_migration_head"] = "20260803085959"
            else:
                database["applied_migration_count"] -= 1
            with self.subTest(mutation=mutation), self.assertRaisesRegex(
                ValueError, "migration"
            ):
                harness.validate_baseline_receipt(self.plan, receipt)

    def test_relation_and_routine_cardinality_identity_and_kind_fail_closed(self) -> None:
        mutations = []
        missing_relation = self.valid_receipt()
        missing_relation["relations"].pop()
        mutations.append(missing_relation)
        wrong_kind = self.valid_receipt()
        wrong_kind["relations"][0]["relkind"] = "v"
        mutations.append(wrong_kind)
        duplicate_relation = self.valid_receipt()
        duplicate_relation["relations"][1]["identity"] = duplicate_relation["relations"][0]["identity"]
        mutations.append(duplicate_relation)
        missing_routine = self.valid_receipt()
        missing_routine["routines"].pop()
        mutations.append(missing_routine)
        wrong_signature = self.valid_receipt()
        wrong_signature["routines"][0]["observed_signature"] = "public.lca_read_job_projection(uuid)"
        mutations.append(wrong_signature)
        wrong_routine_kind = self.valid_receipt()
        wrong_routine_kind["routines"][0]["routine_kind"] = "p"
        mutations.append(wrong_routine_kind)
        for receipt in mutations:
            with self.subTest(receipt=receipt), self.assertRaises(ValueError):
                harness.validate_baseline_receipt(self.plan, receipt)

    def test_partial_rls_visibility_and_unapproved_operator_fail_closed(self) -> None:
        partial = self.valid_receipt()
        partial["operatorProof"].update({
            "role_super": False,
            "role_bypass_rls": False,
            "owns_all_targets_without_forced_rls": False,
            "fullRowVisibility": False,
        })
        with self.assertRaisesRegex(ValueError, "full-row RLS visibility"):
            harness.validate_baseline_receipt(self.plan, partial)
        wrong_role = self.valid_receipt()
        wrong_role["operatorProof"]["current_role"] = "service_role"
        with self.assertRaisesRegex(ValueError, "allowed owner-capable"):
            harness.validate_baseline_receipt(self.plan, wrong_role)

    def test_digest_budget_and_digest_mutation_fail_closed(self) -> None:
        over_budget = self.valid_receipt()
        over_budget["relations"][0]["data_oracle"].update({
            "rowCount": harness.MAX_BASELINE_ROWS_PER_RELATION + 1,
            "withinCaptureBudget": False,
            "digestComplete": False,
            "primaryKeySha256": None,
            "contentSha256": None,
        })
        with self.assertRaisesRegex(ValueError, "digest budget"):
            harness.validate_baseline_receipt(self.plan, over_budget)
        bad_digest = self.valid_receipt()
        bad_digest["relations"][0]["data_oracle"]["contentSha256"] = "0" * 63
        with self.assertRaisesRegex(ValueError, "content digest"):
            harness.validate_baseline_receipt(self.plan, bad_digest)

    def test_database_receipt_cannot_claim_repository_provenance(self) -> None:
        receipt = self.valid_receipt()
        receipt["source"] = self.plan["source"]
        with self.assertRaisesRegex(ValueError, "repository plan provenance"):
            harness.validate_baseline_receipt(self.plan, receipt)
        receipt = self.valid_receipt()
        receipt["databaseProvenance"]["server_address"] = "not-an-address"
        with self.assertRaisesRegex(ValueError, "address is invalid"):
            harness.validate_baseline_receipt(self.plan, receipt)

    def test_dependency_surface_and_distinct_instance_proofs_fail_closed(self) -> None:
        receipt = self.valid_receipt()
        receipt["dependencyClosure"].pop("policyObjects")
        with self.assertRaisesRegex(ValueError, "closure surfaces"):
            harness.validate_baseline_receipt(self.plan, receipt)
        fresh = self.valid_receipt()["databaseProvenance"]
        populated = copy.deepcopy(fresh)
        populated["database_oid"] = 6
        with self.assertRaisesRegex(ValueError, "share one cluster identity"):
            harness.assert_distinct_database_instances(fresh, populated)
        populated["system_identifier"] = "7612345678901234568"
        harness.assert_distinct_database_instances(fresh, populated)

    def test_schema_binding_and_contract_sidecar_fail_closed(self) -> None:
        changed = copy.deepcopy(self.plan)
        changed["artifactBinding"]["schemaSha256"] = "0" * 64
        with tempfile.TemporaryDirectory() as directory:
            contract_path = Path(directory) / "contract.json"
            sidecar_path = Path(directory) / "contract.sha256"
            raw = json.dumps(changed).encode()
            contract_path.write_bytes(raw)
            sidecar_path.write_text(harness.sha256(raw) + "\n", encoding="utf-8")
            with mock.patch.object(harness, "CONTRACT_PATH", contract_path), mock.patch.object(
                harness, "SHA_PATH", sidecar_path
            ):
                with self.assertRaisesRegex(ValueError, "Schema bytes differ"):
                    harness.validate_contract()
            sidecar_path.write_text("f" * 64 + "\n", encoding="utf-8")
            with mock.patch.object(harness, "CONTRACT_PATH", contract_path), mock.patch.object(
                harness, "SHA_PATH", sidecar_path
            ):
                with self.assertRaisesRegex(ValueError, "sidecar differs"):
                    harness.validate_contract()


@unittest.skipUnless(
    os.environ.get("ISSUE_390_BASELINE_DB_URL"),
    "set ISSUE_390_BASELINE_DB_URL to run the read-only live PostgreSQL capture",
)
class Issue390PhysicalQualificationLiveIntegrationTest(unittest.TestCase):
    def test_live_capture_executes_sql_and_validates_exact_predecessor(self) -> None:
        db_url = os.environ["ISSUE_390_BASELINE_DB_URL"]
        plan = harness.validate_contract()
        receipt = harness.capture_baseline(plan, db_url)
        harness.validate_baseline_receipt(plan, {
            key: receipt[key]
            for key in (
                "schemaVersion", "nonAuthorizing", "databaseProvenance",
                "operatorProof", "relations", "routines", "dependencyClosure",
            )
        })
        self.assertEqual(
            receipt["databaseProvenance"]["applied_migration_head"],
            harness.EXPECTED_MIGRATION_HEAD,
        )
        self.assertFalse(receipt["capture"]["authorizationClaim"])
        self.assertFalse(receipt["capture"]["disposableDatabaseClaim"])


if __name__ == "__main__":
    unittest.main()
