#!/usr/bin/env python3
"""Static fail-closed contract for the Issue #407 Phase A migration."""

from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "supabase/migrations/20260803163000_issue_407_document_validation_evidence_expand.sql"
ROLLBACK = ROOT / "supabase/operator/20260803_issue_407_document_validation_evidence_expand_rollback.sql"
RUNTIME = ROOT / "scripts/test_issue_407_document_validation_evidence_runtime.py"
DATABASE_VALIDATION = ROOT / ".github/workflows/database-validation.yml"
SUPABASE_DEV = ROOT / ".github/workflows/supabase-dev.yml"


class Issue407DocumentEvidenceExpandTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sql = MIGRATION.read_text(encoding="utf-8")
        cls.normalized = re.sub(r"\s+", " ", cls.sql.lower()).strip()

    def test_phase_a_never_moves_or_replaces_the_public_relation(self) -> None:
        forbidden = (
            "alter table public.lcia_document_validation_evidence set schema",
            "alter table public.lcia_document_validation_evidence rename",
            "drop table public.lcia_document_validation_evidence",
            "create table private.lcia_document_validation_evidence",
            "create view public.lcia_document_validation_evidence",
            "create materialized view public.lcia_document_validation_evidence",
        )
        for fragment in forbidden:
            with self.subTest(fragment=fragment):
                self.assertNotIn(fragment, self.normalized)

    def test_preflight_is_exact_and_retry_aware(self) -> None:
        required = (
            "issue 407 requires exact predecessor migration 20260803090000",
            "issue 407 refuses a partial private routine state",
            "issue 407 public predecessor definition drifted",
            "issue 407 retry state has unsafe routine metadata",
            "da8b84eaaf6cfb408e2d2f4627827b36",
            "149cc3f6a715ae72bbceb458d3f33433",
            "c5ca4c53ff5746dba23a65d5170bc816",
            "issue 407 private schema role matrix drifted",
            "issue 407 postflight four-function catalog mismatch",
        )
        for fragment in required:
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, self.normalized)
        for catalog_field in (
            "relation.relam", "relation.reltablespace", "relation.reloptions",
            "relation.relispartition", "relation.relpartbound",
            "attribute.attstattarget", "attribute.attoptions",
            "attribute.atthasmissing", "attribute.attmissingval",
            "publication.puballtables or member.prrelid is not null",
        ):
            self.assertIn(catalog_field, self.normalized)
        self.assertEqual(self.normalized.count("create or replace function private.svc_lcia_document_validation_evidence_"), 2)
        self.assertEqual(self.normalized.count("create or replace function public.svc_lcia_document_validation_evidence_"), 2)

    def test_private_routines_have_exact_runtime_acl_and_safe_path(self) -> None:
        self.assertIn(
            "set search_path = pg_catalog, pg_temp",
            self.normalized,
        )
        self.assertIn(
            "from public, anon, authenticated, service_role, api_internal_executor, lca_worker_runtime; grant execute on function private.svc_lcia_document_validation_evidence_lookup(pg_catalog.jsonb), private.svc_lcia_document_validation_evidence_record(pg_catalog.jsonb, pg_catalog.uuid) to lca_worker_runtime;",
            self.normalized,
        )
        private_section = self.normalized.split(
            "create or replace function private.svc_lcia_document_validation_evidence_lookup",
            1,
        )[1].split(
            "create or replace function public.svc_lcia_document_validation_evidence_lookup",
            1,
        )[0]
        self.assertNotIn("util.is_service_request", private_section)
        self.assertNotRegex(
            private_section,
            r"grant\s+(select|insert|update|delete|all).*lcia_document_validation_evidence.*lca_worker_runtime",
        )

    def test_public_wrappers_are_single_path_and_log_no_parameters(self) -> None:
        public_section = self.normalized.split(
            "create or replace function public.svc_lcia_document_validation_evidence_lookup",
            1,
        )[1]
        self.assertEqual(
            public_section.count("return private.svc_lcia_document_validation_evidence_"),
            2,
        )
        self.assertNotIn(
            "insert into public.lcia_document_validation_evidence",
            public_section,
        )
        self.assertIn(
            "issue_407_public_compat function=lookup caller_category=%",
            public_section,
        )
        self.assertIn(
            "issue_407_public_compat function=record caller_category=%",
            public_section,
        )
        self.assertNotRegex(
            public_section,
            r"raise log[^;]*(p_cache_keys|p_records|p_source_worker_job_id)",
        )

    def test_public_compatibility_acl_is_preserved_exactly(self) -> None:
        self.assertIn(
            "from public, anon, authenticated, service_role, api_internal_executor, lca_worker_runtime; grant execute on function public.svc_lcia_document_validation_evidence_lookup(pg_catalog.jsonb), public.svc_lcia_document_validation_evidence_record(pg_catalog.jsonb, pg_catalog.uuid) to service_role, api_internal_executor;",
            self.normalized,
        )

    def test_retry_freezes_bodies_and_acls(self) -> None:
        for digest in (
            "bd277cd343a10462fc536a64390459c5",
            "2759f5215c8dd4b253db2ed2264cc8ab",
            "6f6fb65152a4125c25babc79397d1626",
            "efd249089fa40ea58fe8efe3e1e894b0",
        ):
            self.assertIn(digest, self.normalized)
        self.assertIn("issue 407 retry state definition or acl drifted", self.normalized)

    def test_record_order_and_operator_rollback_preserve_identities(self) -> None:
        self.assertIn("with ordinality as item(value, ordinal)", self.normalized)
        self.assertIn("item.ordinal", self.normalized)
        sql = re.sub(r"\s+", " ", ROLLBACK.read_text(encoding="utf-8").lower())
        self.assertNotIn("drop function public.svc_lcia_document_validation_evidence", sql)
        public_replace = sql.index(
            "create or replace function public.svc_lcia_document_validation_evidence_lookup"
        )
        private_drop = sql.index(
            "drop function private.svc_lcia_document_validation_evidence_lookup(jsonb)"
        )
        self.assertLess(public_replace, private_drop)
        self.assertIn(
            "comment on function public.svc_lcia_document_validation_evidence_lookup(jsonb) is null",
            sql,
        )
        self.assertIn("issue 407 rollback refuses missing or extra phase a overloads", sql)
        self.assertIn("issue 407 rollback predecessor postflight mismatch", sql)
        self.assertIn("issue 407 rollback retained a private routine", sql)
        self.assertIn("2759f5215c8dd4b253db2ed2264cc8ab", sql)
        self.assertNotIn("drop table public.lcia_document_validation_evidence", sql)

    def test_runtime_requires_destructive_confirmation_and_dynamic_secrets(self) -> None:
        source = RUNTIME.read_text(encoding="utf-8")
        self.assertIn("--confirm-isolated-destructive-test", source)
        self.assertIn("secrets.token_urlsafe", source)
        self.assertIn("with inherit true, set false, admin false", source.lower())
        self.assertIn("finally:", source)
        self.assertIn("--predecessor-db-url", source)
        self.assertIn("--candidate-db-url", source)
        self.assertIn("--expected-candidate-container", source)
        self.assertIn("--expected-predecessor-container", source)
        self.assertIn("--execution-mode", source)
        self.assertIn("expected_migration_versions", source)
        self.assertIn("supabase_kong_", source)
        self.assertIn("supabase_rest_", source)
        self.assertIn("network target and bound database container identities differ", source)
        self.assertNotIn('add_argument("--anon-key"', source)
        self.assertNotIn('add_argument("--service-key"', source)
        self.assertIn('os.environ.get("ISSUE407_ANON_KEY")', source)
        self.assertIn('os.environ.get("ISSUE407_SERVICE_KEY")', source)
        self.assertIn('invalid_key["datasetId"] = "bad"', source)
        self.assertIn('"Authorization": f"Bearer {service_key}"', source)
        self.assertIn("predecessor and candidate must be distinct postgresql clusters", source.lower())
        self.assertNotIn('delete from public.lcia_document_validation_evidence")', source.lower())
        self.assertNotIn("drop role if exists", source.lower())
        self.assertNotIn("PASSWORD =", source)

    def test_both_fresh_stack_workflows_share_the_exact_runtime_step(self) -> None:
        marker = "      - name: Run Issue 407 exact predecessor-to-candidate qualification\n"
        end = "      - name: Stop Issue 407 predecessor stack\n"
        blocks = []
        for path in (DATABASE_VALIDATION, SUPABASE_DEV):
            source = path.read_text(encoding="utf-8")
            self.assertEqual(source.count(marker), 1, path)
            blocks.append(source.split(marker, 1)[1].split(end, 1)[0])
            self.assertIn('export ISSUE407_ANON_KEY=', source)
            self.assertIn('export ISSUE407_SERVICE_KEY=', source)
            self.assertIn('--api-url http://127.0.0.1:55321', source)
            self.assertIn('--execution-mode ci-hard-bound', source)
            self.assertIn('--expected-predecessor-project-id database-engine-407-predecessor', source)
            self.assertIn('python -m scripts.issue_390_pre_ddl_gate --check', source)
        self.assertEqual(blocks[0], blocks[1])


if __name__ == "__main__":
    unittest.main()
