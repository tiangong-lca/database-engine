#!/usr/bin/env python3
"""Offline profile-selection tests for the Worker Data API probe."""

from __future__ import annotations

import sys
import tomllib
import unittest
from subprocess import CompletedProcess
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_worker_control_plane_data_api as target


class WorkerControlPlaneDataApiProfileTest(unittest.TestCase):
    def test_public_openapi_profile_is_explicit_with_api_first_config(self) -> None:
        config_path = target.ROOT / "supabase/config.toml"
        with config_path.open("rb") as handle:
            schemas = tomllib.load(handle)["api"]["schemas"]
        self.assertEqual(schemas[0], "api")

        headers = target.request_headers("eyJservice-role", payload=None, profile="public")
        self.assertEqual(headers["Accept-Profile"], "public")
        self.assertNotIn("Content-Profile", headers)

    def test_public_rpc_profile_is_explicit(self) -> None:
        headers = target.request_headers(
            "eyJservice-role", payload={"p_job_ids": []}, profile="public"
        )
        self.assertEqual(headers["Content-Profile"], "public")
        self.assertNotIn("Accept-Profile", headers)

    def test_opaque_keys_are_not_sent_as_bearer_tokens(self) -> None:
        for credential in ("sb_publishable_opaque", "sb_secret_opaque"):
            with self.subTest(credential=credential.split("_")[1]):
                headers = target.request_headers(
                    credential, payload={"p_job_ids": []}, profile="public"
                )
                self.assertEqual(headers["apikey"], credential)
                self.assertNotIn("Authorization", headers)

    def test_legacy_jwt_is_sent_as_bearer_token(self) -> None:
        credential = "eyJlegacy-jwt"
        headers = target.request_headers(
            credential, payload={"p_job_ids": []}, profile="public"
        )
        self.assertEqual(headers["Authorization"], f"Bearer {credential}")

    def test_reload_failure_does_not_echo_database_url(self) -> None:
        database_url = "postgresql://" + "operator:" + "must-not-leak" + "@example.invalid/postgres"
        failed = CompletedProcess(args=["psql", database_url], returncode=2)
        with mock.patch.object(target.subprocess, "run", return_value=failed):
            with self.assertRaises(SystemExit) as raised:
                target.reload_schema(database_url)
        self.assertEqual(str(raised.exception), "PostgREST schema reload failed")
        self.assertNotIn(database_url, str(raised.exception))
        self.assertNotIn("must-not-leak", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
