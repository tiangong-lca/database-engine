import contextlib
import io
import unittest
from pathlib import Path
from unittest.mock import patch

from supabase.tests.hosted.lca_snapshot_hosted_qualification import (
    DEV_REF,
    EDGE_DEPLOYMENT_RECEIPT,
    MIGRATION_HEAD,
    PRODUCTION_REF,
    _json_request,
    Config,
    QualificationError,
    RunNamespace,
    canonical_worker_fixture_sql,
    finish_with_reconcile,
    qualify,
    qualification_phase_error,
    reconcile_namespace,
    require_anonymous_unauthorized,
    require_response,
    resolve_keys,
    safe_diagnostic_details,
    select_current_keys,
    unexpected_phase_error,
)


ROOT = Path(__file__).resolve().parents[3]


def config(**overrides):
    values = {
        "repository": "tiangong-lca/database-engine",
        "git_ref": "refs/heads/dev",
        "project_ref": DEV_REF,
        "access_token": "masked-token",
        "publishable_key": None,
        "secret_key": None,
    }
    values.update(overrides)
    return Config(**values)


class TargetingTests(unittest.TestCase):
    def test_exact_target_is_accepted(self):
        config().validate()

    def test_production_and_wrong_ref_fail_closed(self):
        for candidate in (PRODUCTION_REF, "wrong-project"):
            with self.subTest(candidate=candidate), self.assertRaises(QualificationError):
                config(project_ref=candidate).validate()
        with self.assertRaises(QualificationError):
            config(git_ref="refs/heads/main").validate()

class KeyTests(unittest.TestCase):
    class FakeResponse:
        def __init__(self, status, payload):
            self.status = status
            self.payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return self.payload

    def test_success_transport_accepts_text_and_preserves_json_without_logging_content(self):
        secret_text = "plain-text-response-must-not-be-logged"
        responses = [
            self.FakeResponse(204, secret_text.encode()),
            self.FakeResponse(200, b'{"ok":true,"value":42}'),
        ]
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            patch("urllib.request.urlopen", side_effect=responses),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            self.assertEqual(_json_request("https://example.invalid/options"), (204, secret_text))
            self.assertEqual(_json_request("https://example.invalid/json"), (200, {"ok": True, "value": 42}))
        self.assertNotIn(secret_text, stdout.getvalue())
        self.assertNotIn(secret_text, stderr.getvalue())

    def test_selects_current_default_modern_keys(self):
        rows = [
            {"type": "secret", "name": "old", "api_key": "sb_secret_old", "disabled": True},
            {"type": "publishable", "name": "default", "api_key": "sb_publishable_current"},
            {"type": "secret", "name": "default", "api_key": "sb_secret_current"},
        ]
        self.assertEqual(select_current_keys(rows), ("sb_publishable_current", "sb_secret_current"))

    def test_rejects_wrong_role_or_legacy_keys(self):
        with self.assertRaises(QualificationError):
            select_current_keys([{"type": "legacy", "api_key": "eyJlegacy"}])

    def test_invalid_current_key_semantics_fail_closed(self):
        with self.assertRaises(QualificationError):
            require_response((401, {"message": "Invalid API key"}), 200)

    def test_anonymous_edge_contract_accepts_only_handler_or_gateway_401(self):
        accepted = (
            {"error": "unauthorized"},
            {"code": 401, "message": "Missing authorization header"},
            {"code": 401, "message": "Invalid Token or Protected Header formatting"},
            {"code": 401, "message": "Invalid JWT"},
        )
        for payload in accepted:
            with self.subTest(payload=payload):
                self.assertEqual(require_anonymous_unauthorized((401, payload)), payload)

    def test_anonymous_edge_contract_rejects_other_status_or_dto(self):
        secret = "anonymous-response-secret"
        rejected = (
            (403, {"error": "unauthorized"}),
            (401, None),
            (401, "Missing authorization header"),
            (401, {"error": "other"}),
            (401, {"code": "401", "message": "Missing authorization header"}),
            (401, {"code": 401, "message": ""}),
            (401, {"code": 401, "message": "invalid jwt"}),
            (401, {"code": 401, "message": "Missing authorization header", "extra": True}),
            (401, {"error": secret}),
        )
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            for response in rejected:
                with self.subTest(response=response), self.assertRaises(QualificationError):
                    require_anonymous_unauthorized(response)
        self.assertNotIn(secret, stdout.getvalue())
        self.assertNotIn(secret, stderr.getvalue())

    def test_management_first_and_fallback(self):
        calls = []
        def management(url, **kwargs):
            calls.append(url)
            return 200, [
                {"type": "publishable", "name": "default", "api_key": "sb_publishable_managed"},
                {"type": "secret", "name": "default", "api_key": "sb_secret_managed"},
            ]
        self.assertEqual(resolve_keys(config(publishable_key="sb_publishable_fallback", secret_key="sb_secret_fallback"), management), ("sb_publishable_managed", "sb_secret_managed"))
        self.assertEqual(len(calls), 1)
        self.assertEqual(resolve_keys(config(publishable_key="sb_publishable_fallback", secret_key="sb_secret_fallback"), lambda *a, **k: (503, {})), ("sb_publishable_fallback", "sb_secret_fallback"))

    def test_single_sided_and_role_swapped_fallback_fail(self):
        with self.assertRaises(QualificationError):
            resolve_keys(config(publishable_key="sb_publishable_only"), lambda *a, **k: (503, {}))
        with self.assertRaises(QualificationError):
            resolve_keys(config(publishable_key="sb_secret_swapped", secret_key="sb_publishable_swapped"), lambda *a, **k: (503, {}))

    def test_management_transport_failure_without_fallback_fails(self):
        with self.assertRaises(QualificationError):
            resolve_keys(config(), lambda *a, **k: (401, {"message": "wrong secret"}))


class CleanupTests(unittest.TestCase):
    class FakeClient:
        secret_key = "sb_secret_test"

        def __init__(self, *, actor=True, worker=True, storage=True, fail_actor=False):
            self.actor = actor
            self.worker = worker
            self.storage = storage
            self.fail_actor = fail_actor
            self.sql_calls = []
            self.storage_calls = []
            self.auth_calls = []

        def sql(self, query):
            self.sql_calls.append(query)
            if query.startswith("select id::text from auth.users"):
                if self.fail_actor:
                    raise RuntimeError("lost recovery transport")
                return [{"id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}] if self.actor else []
            if query.startswith("select id::text from private.worker_jobs"):
                return [{"id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}] if self.worker else []
            if query.startswith("select name from storage.objects"):
                return [{"name": "runs/marker/query.json"}] if self.storage else []
            if query.startswith("select id from storage.buckets"):
                return [{"id": "bucket"}] if self.storage else []
            if query.startswith("select (select count"):
                return [{
                    "network": 0, "artifact": 0, "active": 0, "result_cache": 0,
                    "worker_jobs": 0, "lca_results": 0,
                    "latest_results": 0, "storage_objects": 0, "storage_buckets": 0,
                    "users": 0, "sessions": 0,
                }]
            return []

        def storage_json(self, path, *, method, body=None):
            self.storage_calls.append((path, method, body))
            return 404, {"message": "already absent"}

        def auth(self, path, *, method, key, body=None, bearer=None):
            self.auth_calls.append((path, method))
            return 404, {"message": "already absent"}

    def setUp(self):
        self.run = RunNamespace(
            marker="marker", fixture_id=__import__("uuid").UUID("11111111-1111-4111-8111-111111111111"),
            create_id=__import__("uuid").UUID("22222222-2222-4222-8222-222222222222"),
            email="issue380-marker@example.invalid", bucket="issue380-marker", prefix="runs/marker",
        )

    def test_lost_create_response_recovers_actor_and_revokes_it(self):
        client = self.FakeClient(worker=False, storage=False)
        reconcile_namespace(client, self.run)
        self.assertTrue(any("delete from auth.sessions" in query for query in client.sql_calls))
        self.assertEqual(client.auth_calls, [("admin/users/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "DELETE")])

    def test_random_worker_id_is_discovered_cancelled_and_deleted(self):
        client = self.FakeClient(actor=False, storage=False)
        reconcile_namespace(client, self.run)
        worker_sql = "\n".join(client.sql_calls)
        self.assertIn("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", worker_sql)
        self.assertIn("status='cancelled'", worker_sql)
        self.assertIn("delete from private.worker_jobs", worker_sql)
        self.assertIn("subject_version", worker_sql)

    def test_canonical_worker_fixture_has_no_retired_legacy_dependency(self):
        import uuid

        runner = (ROOT / "supabase/tests/hosted/lca_snapshot_hosted_qualification.py").read_text()
        self.assertNotIn("public.lca_jobs", runner)
        snapshot_id = uuid.UUID("11111111-1111-4111-8111-111111111111")
        actor_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        sql = canonical_worker_fixture_sql(
            snapshot_id,
            actor_id,
            "marker",
            [
                (uuid.UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"), "lca.solve_all_unit", "lca.solve_all_unit.request.v1", {"snapshot_id": str(snapshot_id)}),
                (uuid.UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"), "lca.contribution_path", "lca.contribution_path.request.v1", {"snapshot_id": str(snapshot_id)}),
            ],
        )
        self.assertIn("insert into private.worker_jobs", sql)
        for column in ("worker_runtime", "worker_queue", "requester_type", "requested_by", "subject_version", "payload_json", "attempt_count", "visibility", "diagnostics"):
            self.assertIn(column, sql)
        self.assertIn("'lca.solve_all_unit'", sql)
        self.assertIn("'lca.contribution_path'", sql)
        self.assertIn("'calculator','solver'", sql)
        self.assertNotIn("lca_jobs", sql)

    def test_storage_cleanup_is_unconditional_and_404_idempotent(self):
        client = self.FakeClient(actor=False, worker=False, storage=True)
        reconcile_namespace(client, self.run)
        self.assertEqual(client.storage_calls[0][0], "object/issue380-marker")
        self.assertEqual(client.storage_calls[0][2], {"prefixes": ["runs/marker/query.json"]})
        self.assertEqual(client.storage_calls[1][0], "bucket/issue380-marker")

    def test_snapshot_cleanup_uses_parent_cascade_and_retains_artifact_readback(self):
        client = self.FakeClient(actor=False, worker=False, storage=False)
        reconcile_namespace(client, self.run)
        sql = "\n".join(client.sql_calls)
        self.assertNotIn("delete from private.lca_snapshot_artifacts", sql)
        self.assertIn("delete from private.lca_network_snapshots", sql)
        self.assertIn("from private.lca_snapshot_artifacts where snapshot_id", sql)

    def test_primary_and_recovery_errors_are_aggregated(self):
        client = self.FakeClient(actor=False, worker=False, storage=False, fail_actor=True)
        with self.assertRaises(ExceptionGroup) as raised:
            finish_with_reconcile(QualificationError("primary"), client, self.run)
        messages = [str(error) for error in raised.exception.exceptions]
        self.assertTrue(any("primary" in message for message in messages))
        self.assertTrue(any("discover-auth-actors" in message for message in messages))

    def test_unexpected_phase_error_names_phase_and_type_without_value(self):
        secret = "do-not-print-this-value"
        error = unexpected_phase_error("auth-boundary", RuntimeError(secret))
        self.assertEqual(str(error), "hosted phase failed: auth-boundary: RuntimeError")
        self.assertNotIn(secret, str(error))

    def test_qualification_phase_error_adds_static_label_without_value_or_duplication(self):
        secret = "qualification-secret-value"
        error = qualification_phase_error("edge-lca_solve-retry", QualificationError(secret))
        self.assertEqual(str(error), "hosted phase failed: edge-lca_solve-retry: QualificationError")
        self.assertNotIn(secret, str(error))
        self.assertIs(error, qualification_phase_error("ignored", error))

    def test_nested_primary_and_cleanup_diagnostics_are_recursive_and_secret_safe(self):
        primary_secret = "primary-secret-value"
        cleanup_secret = "cleanup-secret-value"
        error = ExceptionGroup(
            "outer group contains no reportable detail",
            [
                unexpected_phase_error("worker-artifact-fixture", ValueError(primary_secret)),
                ExceptionGroup(
                    "nested group contains no reportable detail",
                    [
                        QualificationError("namespace reconcile failed: discover-auth-actors: RuntimeError"),
                        RuntimeError(cleanup_secret),
                    ],
                ),
            ],
        )
        details = safe_diagnostic_details(error)
        self.assertEqual(
            details,
            [
                "hosted phase failed: worker-artifact-fixture: ValueError",
                "namespace reconcile failed: discover-auth-actors: RuntimeError",
                "RuntimeError",
            ],
        )
        rendered = "; ".join(details)
        self.assertNotIn(primary_secret, rendered)
        self.assertNotIn(cleanup_secret, rendered)
        self.assertNotIn("outer group", rendered)
        self.assertNotIn("nested group", rendered)


class QualifyPhaseFlowTests(unittest.TestCase):
    class FakeClient:
        def __init__(self, run, failure):
            self.run = run
            self.failure = failure
            self.create_calls = 0

        def management(self, path):
            return 200, [
                {"slug": name, "id": receipt[0], "version": receipt[1], "updated_at": receipt[2], "ezbr_sha256": receipt[3]}
                for name, receipt in EDGE_DEPLOYMENT_RECEIPT.items()
            ]

        def sql(self, query):
            if "supabase_migrations.schema_migrations" in query:
                return [{"migration_head": MIGRATION_HEAD}]
            if query.startswith("select id::text, btrim(version)"):
                return [{"id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "version": "1"}]
            if query.startswith("insert into private.worker_jobs") and self.failure == "worker":
                raise RuntimeError("worker-fixture-secret")
            return []

        def rest(self, name, args, *, key, bearer=None):
            if key == "sb_publishable_test":
                return 403, {"code": "42501"}
            if name == "cmd_lca_snapshot_create_v1":
                self.create_calls += 1
                return 200, {"created": self.create_calls == 1, "snapshotId": str(self.run.create_id)}
            rows = {
                "lca_snapshot_active_read_v1": {"snapshot_id": str(self.run.fixture_id), "source_hash": "source", "activated_at": "time"},
                "lca_snapshot_scope_read_v1": {"id": str(self.run.fixture_id), "scope": "full_library", "process_filter": {}, "status": "ready"},
                "lca_snapshot_resolve_v1": {"id": str(self.run.fixture_id), "created_at": "time", "process_filter": {}},
                "lca_snapshot_artifact_read_v1": {"snapshot_id": str(self.run.fixture_id), "artifact_url": "url", "artifact_format": "hdf5", "process_count": 1, "status": "ready", "created_at": "time"},
                "lca_snapshot_artifact_latest_v1": {"snapshot_id": str(self.run.fixture_id), "artifact_url": "url", "artifact_format": "hdf5", "process_count": 1, "status": "ready", "created_at": "time"},
            }
            return 200, rows[name]

        def auth(self, path, **kwargs):
            if path == "admin/users":
                return 200, {"user": {"id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}}
            return 200, {"access_token": "access-token"}

        def storage_json(self, path, **kwargs):
            return 200, {}

        def upload_json(self, bucket, path, payload):
            if self.failure == "upload" and path.endswith("snapshot-index-v1.json"):
                raise RuntimeError("upload-secret")

    def test_qualify_assigns_post_bucket_and_worker_fixture_failure_phases(self):
        run = RunNamespace(
            marker="11111111111141118111111111111111",
            fixture_id=__import__("uuid").UUID("11111111-1111-4111-8111-111111111111"),
            create_id=__import__("uuid").UUID("22222222-2222-4222-8222-222222222222"),
            email="issue380-flow@example.invalid",
            bucket="issue380-flow",
            prefix="runs/flow",
        )

        def finish(primary_error, client, namespace):
            if primary_error is not None:
                raise primary_error

        for failure, phase, secret in (
            ("upload", "storage-snapshot-index-upload", "upload-secret"),
            ("worker", "worker-result-fixture", "worker-fixture-secret"),
        ):
            with self.subTest(failure=failure):
                client = self.FakeClient(run, failure)
                stdout = io.StringIO()
                with (
                    patch("supabase.tests.hosted.lca_snapshot_hosted_qualification.resolve_keys", return_value=("sb_publishable_test", "sb_secret_test")),
                    patch("supabase.tests.hosted.lca_snapshot_hosted_qualification.HostedClient", return_value=client),
                    patch("supabase.tests.hosted.lca_snapshot_hosted_qualification.RunNamespace.create", return_value=run),
                    patch("supabase.tests.hosted.lca_snapshot_hosted_qualification.reconcile_namespace"),
                    patch("supabase.tests.hosted.lca_snapshot_hosted_qualification.finish_with_reconcile", side_effect=finish),
                    contextlib.redirect_stdout(stdout),
                    self.assertRaises(QualificationError) as raised,
                ):
                    qualify(config())
                self.assertEqual(str(raised.exception), f"hosted phase failed: {phase}: RuntimeError")
                self.assertNotIn(secret, str(raised.exception))
                self.assertNotIn(secret, stdout.getvalue())


class WorkflowSecurityTests(unittest.TestCase):
    def test_workflow_is_dev_only_and_has_no_arbitrary_selector(self):
        text = (ROOT / ".github/workflows/lca-snapshot-hosted-qualification.yml").read_text()
        trigger = text.split("permissions:", 1)[0]
        self.assertIn("  workflow_dispatch:\n", trigger)
        self.assertIn("  push:\n    branches:\n      - dev\n", trigger)
        self.assertIn("      - .github/workflows/lca-snapshot-hosted-qualification.yml\n", trigger)
        self.assertIn("      - supabase/tests/hosted/lca_snapshot_hosted_qualification.py\n", trigger)
        self.assertEqual(trigger.count("      - "), 3)
        self.assertIn("github.ref }}\" != 'refs/heads/dev'", text)
        self.assertIn("permissions:\n  contents: read", text)
        self.assertNotIn("repository:", text)
        self.assertNotIn("script:", text)
        self.assertNotIn("ref:", text)
        self.assertIn("actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803", text)
        self.assertIn("actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1", text)
        self.assertEqual(text.count("secrets.SUPABASE_ACCESS_TOKEN"), 1)
        self.assertLess(text.index("Run fail-closed hosted qualification"), text.index("secrets.SUPABASE_ACCESS_TOKEN"))


if __name__ == "__main__":
    unittest.main()
