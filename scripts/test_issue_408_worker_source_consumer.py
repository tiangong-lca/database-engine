#!/usr/bin/env python3
"""Offline tests for the Issue #408 exact Worker source-consumer artifact."""

from __future__ import annotations

import copy
import hashlib
import json
import sys
import unittest
from pathlib import Path
from unittest import mock

import jsonschema

sys.path.insert(0, str(Path(__file__).resolve().parent))
import issue_408_worker_source_consumer as consumer


class WorkerSourceConsumerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.repo = consumer.DEFAULT_WORKER_REPO
        cls.artifact = json.loads(consumer.ARTIFACT_PATH.read_text())

    def test_artifact_replays_from_exact_clean_commit(self) -> None:
        actual = consumer.verify(self.repo)
        self.assertEqual(actual["source"]["workerCommit"], consumer.WORKER_COMMIT)
        self.assertFalse(actual["source"]["canonical"])
        self.assertEqual(actual["source"]["refKind"], "pull-request-head")
        self.assertFalse(actual["sourceArtifactComplete"])
        self.assertFalse(actual["contractReady"])

    def test_json_schema_rejects_contract_authorization(self) -> None:
        schema = json.loads(consumer.SCHEMA_PATH.read_text())
        jsonschema.Draft202012Validator(schema).validate(self.artifact)
        hostile = copy.deepcopy(self.artifact)
        hostile["contractReady"] = True
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.Draft202012Validator(schema).validate(hostile)

    def test_commit_drift_fails_closed_before_blob_derivation(self) -> None:
        real_run = consumer.run

        def drift(repo: Path, *args: str, **kwargs: object) -> str:
            if args == ("rev-parse", "HEAD"):
                return "0" * 40
            return real_run(repo, *args, **kwargs)

        with mock.patch.object(consumer, "run", side_effect=drift):
            with self.assertRaisesRegex(ValueError, "HEAD differs"):
                consumer.build(self.repo)

    def test_blob_and_line_evidence_are_exact_and_tamper_evident(self) -> None:
        consumer.validate_source_refs(self.repo, self.artifact)
        hostile = copy.deepcopy(self.artifact)
        hostile["accesses"][0]["sources"][0]["blobSha"] = "0" * 40
        with self.assertRaisesRegex(ValueError, "blob SHA"):
            consumer.validate_source_refs(self.repo, hostile)
        hostile = copy.deepcopy(self.artifact)
        hostile["accesses"][0]["sources"][0]["line"] += 1
        with self.assertRaisesRegex(ValueError, "line evidence"):
            consumer.validate_source_refs(self.repo, hostile)

    def test_test_docs_and_scripts_cannot_prove_runtime_access(self) -> None:
        for path in (
            "docs/fake.rs",
            "scripts/fake.rs",
            "crates/solver-worker/tests/fake.rs",
        ):
            hostile = copy.deepcopy(self.artifact)
            hostile["accesses"][0]["sources"][0]["path"] = path
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "cannot prove"):
                consumer.validate_source_refs(self.repo, hostile)

    def test_dynamic_resolvers_are_closed_or_explicitly_unresolved(self) -> None:
        by_id = {item["id"]: item for item in self.artifact["dynamicResolvers"]}
        self.assertEqual(
            set(by_id["package-root-table"]["closedAllowlist"]),
            {"public.contacts", "public.sources", "public.unitgroups", "public.flowproperties", "public.flows", "public.processes", "public.lifecyclemodels"},
        )
        self.assertEqual(
            set(by_id["scope-closure-dataset-category"]["closedAllowlist"]),
            {"public.contacts", "public.flowproperties", "public.flows", "public.lciamethods", "public.lifecyclemodels", "public.processes", "public.sources", "public.unitgroups"},
        )
        unresolved = {item["id"] for item in by_id.values() if not item["resolved"]}
        self.assertEqual(unresolved, {"pgmq-queue-name", "maintenance-subprocess-override"})
        residue = {item["code"] for item in self.artifact["residue"]}
        self.assertTrue({"pgmq_queue_identity_runtime_value", "maintenance_binary_override"} <= residue)

    def test_bidirectional_source_completeness_has_zero_difference(self) -> None:
        paths, _ = consumer.source_tree(self.repo)
        discovered = consumer.discover_sql_objects(self.repo, paths)
        manifest = {item["identifier"] for item in self.artifact["accesses"]}
        capabilities = {item["identifier"].split("(", 1)[0] for item in self.artifact["requiredCapabilities"]}
        non_sql = {item["identifier"] for item in self.artifact["nonSqlQualifiedIdentifiers"]}
        accepted = manifest | capabilities | set(self.artifact["declaredOnlyIdentifiers"]) | non_sql
        self.assertEqual(discovered - accepted, set())
        self.assertEqual(manifest - discovered, set())
        self.assertEqual(self.artifact["completeness"]["unclassifiedSourceIdentifiers"], [])
        self.assertEqual(self.artifact["completeness"]["manifestIdentifiersMissingFromSource"], [])
        self.assertTrue(self.artifact["completeness"]["qualifiedIdentifierClosed"])
        self.assertFalse(self.artifact["completeness"]["unqualifiedIdentifierClosed"])
        self.assertFalse(self.artifact["completeness"]["bidirectionalSourceClosed"])

    def test_unknown_qualified_identifier_is_not_silently_discarded(self) -> None:
        paths, _ = consumer.source_tree(self.repo)
        real_blob_text = consumer.blob_text

        def injected(repo: Path, path: str) -> str:
            text = real_blob_text(repo, path)
            if path == paths[0]:
                return 'const PROBE: &str = "select * from private.issue408_unknown_runtime";\n' + text
            return text

        with mock.patch.object(consumer, "blob_text", side_effect=injected):
            discovered = consumer.discover_sql_objects(self.repo, paths)
        self.assertIn("private.issue408_unknown_runtime", discovered)

    def test_non_sql_qualified_token_has_explicit_source_evidence(self) -> None:
        entries = {item["identifier"]: item for item in self.artifact["nonSqlQualifiedIdentifiers"]}
        self.assertEqual(set(entries), {"private.v1"})
        self.assertEqual(entries["private.v1"]["classification"], "database-contract-id-suffix")
        consumer.validate_single_source_ref(self.repo, entries["private.v1"]["source"])

    def test_connection_families_and_document_validation_capability_are_explicit(self) -> None:
        families = {item["id"] for item in self.artifact["connectionFamilies"]}
        self.assertTrue({"solver-main", "solver-queue", "document-validation", "package-main", "package-queue", "review-submit-main", "snapshot-builder", "snapshot-gc", "package-gc", "artifact-gc", "result-gc", "maintenance-worker", "maintenance-enqueue", "process-flow-graph"} <= families)
        document = next(item for item in self.artifact["connectionFamilies"] if item["id"] == "document-validation")
        self.assertEqual(document["fallback"], "forbidden")
        direct = {item["identifier"] for item in self.artifact["requiredCapabilities"] if item["directCall"]}
        self.assertEqual(direct, {
            "private.svc_lcia_document_validation_evidence_lookup(jsonb)",
            "private.svc_lcia_document_validation_evidence_record(jsonb,uuid)",
        })

    def test_search_path_debt_is_not_hidden(self) -> None:
        dependent = {item["identifier"] for item in self.artifact["accesses"] if item["searchPathDependent"]}
        self.assertTrue({"public.lca_jobs", "public.lca_process_index", "public.lca_flow_index", "public.lca_package_jobs", "public.contacts"} <= dependent)
        self.assertIn("search_path_dependent_sql", {item["code"] for item in self.artifact["residue"]})
        self.assertIn("unqualified_sql_extraction_not_parser_closed", {item["code"] for item in self.artifact["residue"]})

    def test_canonical_hash_is_exact(self) -> None:
        data = consumer.ARTIFACT_PATH.read_bytes()
        self.assertEqual(data, consumer.canonical(self.artifact))
        self.assertEqual(hashlib.sha256(data).hexdigest(), consumer.SHA_PATH.read_text().strip())


if __name__ == "__main__":
    unittest.main()
