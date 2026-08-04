#!/usr/bin/env python3
"""Offline tests for the allowlisted persistent PostgREST config gate."""

from __future__ import annotations

import io
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import apply_postgrest_config as gate


class FakeClient:
    def __init__(self, remote: dict[str, object], ignore_patch: bool = False) -> None:
        self.remote = dict(remote)
        self.ignore_patch = ignore_patch
        self.patches: list[dict[str, object]] = []

    def get(self) -> dict[str, object]:
        return dict(self.remote)

    def patch(self, payload: dict[str, object]) -> None:
        self.patches.append(dict(payload))
        if not self.ignore_patch:
            self.remote.update(payload)


class ApplyPostgrestConfigTest(unittest.TestCase):
    desired = {
        "db_schema": "api,public,graphql_public",
        "db_extra_search_path": "public,extensions",
        "max_rows": 1000,
    }

    def test_exact_remote_binding_and_override_are_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text(
                """
[api]
schemas = ["api", "public", "graphql_public"]
extra_search_path = ["public", "extensions"]
max_rows = 500

[remotes.dev]
project_id = "abcdefghijklmnopqrst"

[remotes.dev.api]
max_rows = 1000
""".strip(),
                encoding="utf-8",
            )
            self.assertEqual(
                gate.load_desired_config(path, "abcdefghijklmnopqrst"), self.desired
            )

    def test_internal_schema_exposure_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text(
                """
[api]
schemas = ["api", "public", "graphql_public"]
extra_search_path = ["public", "private"]
max_rows = 1000
[remotes.dev]
project_id = "abcdefghijklmnopqrst"
""".strip(),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(gate.ConfigError, "internal schemas"):
                gate.load_desired_config(path, "abcdefghijklmnopqrst")

    def test_no_change_is_idempotent_and_does_not_patch(self) -> None:
        client = FakeClient(
            {
                "db_schema": "api, public, graphql_public",
                "db_extra_search_path": "public, extensions",
                "max_rows": 1000,
                "jwt_secret": "must-never-enter-a-patch",
            }
        )
        self.assertEqual(
            gate.reconcile(self.desired, client.get, client.patch, apply=True), ()
        )
        self.assertEqual(client.patches, [])

    def test_apply_patches_only_changed_allowlisted_fields(self) -> None:
        client = FakeClient(
            {
                "db_schema": "public,graphql_public",
                "db_extra_search_path": "public, extensions",
                "max_rows": 1000,
                "jwt_secret": "must-never-enter-a-patch",
            }
        )
        changed = gate.reconcile(self.desired, client.get, client.patch, apply=True)
        self.assertEqual(changed, ("db_schema",))
        self.assertEqual(
            client.patches, [{"db_schema": "api,public,graphql_public"}]
        )

    def test_check_mode_reports_drift_without_mutation(self) -> None:
        client = FakeClient({"db_schema": "public", "max_rows": 10})
        with self.assertRaisesRegex(gate.ConfigError, "config drift"):
            gate.reconcile(self.desired, client.get, client.patch, apply=False)
        self.assertEqual(client.patches, [])

    def test_failed_readback_is_blocking(self) -> None:
        client = FakeClient({"db_schema": "public"}, ignore_patch=True)
        with self.assertRaisesRegex(gate.ConfigError, "readback mismatch"):
            gate.reconcile(self.desired, client.get, client.patch, apply=True)

    def test_unreviewed_patch_field_is_rejected(self) -> None:
        client = gate.ManagementClient("abcdefghijklmnopqrst", "not-logged")
        with self.assertRaisesRegex(gate.ConfigError, "unreviewed fields"):
            client.patch({"jwt_secret": "forbidden"})

    def test_management_error_does_not_echo_response_or_token(self) -> None:
        def reject(*_args: object, **_kwargs: object) -> object:
            raise urllib.error.HTTPError(
                "https://api.supabase.com/redacted",
                500,
                "failure",
                {},
                io.BytesIO(b'{"jwt_secret":"must-not-be-logged"}'),
            )

        client = gate.ManagementClient(
            "abcdefghijklmnopqrst", "must-not-be-logged", opener=reject
        )
        with self.assertRaises(gate.ConfigError) as raised:
            client.get()
        self.assertEqual(
            str(raised.exception), "Management API GET failed with HTTP 500"
        )

    def test_workflow_serializes_and_applies_config_after_migrations(self) -> None:
        workflow = (gate.ROOT / ".github/workflows/supabase-dev.yml").read_text(
            encoding="utf-8"
        )
        migration_push = workflow.index("supabase db push --include-all")
        config_gate = workflow.index("scripts/apply_postgrest_config.py")
        self.assertLess(migration_push, config_gate)
        self.assertIn("group: supabase-dev", workflow)
        self.assertNotIn("supabase config push", workflow)


if __name__ == "__main__":
    unittest.main()
