import unittest
from pathlib import Path

from supabase.tests.hosted.lca_snapshot_hosted_qualification import (
    DEV_REF,
    PRODUCTION_REF,
    Config,
    QualificationError,
    RunNamespace,
    finish_with_reconcile,
    reconcile_namespace,
    require_response,
    resolve_keys,
    select_current_keys,
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
            if query.startswith("select id::text from public.worker_jobs"):
                return [{"id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}] if self.worker else []
            if query.startswith("select name from storage.objects"):
                return [{"name": "runs/marker/query.json"}] if self.storage else []
            if query.startswith("select id from storage.buckets"):
                return [{"id": "bucket"}] if self.storage else []
            if query.startswith("select (select count"):
                return [{
                    "network": 0, "artifact": 0, "active": 0, "result_cache": 0,
                    "worker_jobs": 0, "lca_jobs": 0, "lca_results": 0,
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
        self.assertIn("delete from public.worker_jobs", worker_sql)
        self.assertIn("subject_version", worker_sql)

    def test_storage_cleanup_is_unconditional_and_404_idempotent(self):
        client = self.FakeClient(actor=False, worker=False, storage=True)
        reconcile_namespace(client, self.run)
        self.assertEqual(client.storage_calls[0][0], "object/issue380-marker")
        self.assertEqual(client.storage_calls[0][2], {"prefixes": ["runs/marker/query.json"]})
        self.assertEqual(client.storage_calls[1][0], "bucket/issue380-marker")

    def test_primary_and_recovery_errors_are_aggregated(self):
        client = self.FakeClient(actor=False, worker=False, storage=False, fail_actor=True)
        with self.assertRaises(ExceptionGroup) as raised:
            finish_with_reconcile(QualificationError("primary"), client, self.run)
        messages = [str(error) for error in raised.exception.exceptions]
        self.assertTrue(any("primary" in message for message in messages))
        self.assertTrue(any("discover-auth-actors" in message for message in messages))


class WorkflowSecurityTests(unittest.TestCase):
    def test_workflow_is_dev_only_and_has_no_arbitrary_selector(self):
        text = (ROOT / ".github/workflows/lca-snapshot-hosted-qualification.yml").read_text()
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
