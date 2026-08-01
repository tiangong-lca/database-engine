#!/usr/bin/env python3
"""Offline profile-selection tests for the Worker Data API probe."""

from __future__ import annotations

import sys
import tomllib
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
