#!/usr/bin/env python3
"""Offline two-stack target-binding negatives for the canonical runner."""

from __future__ import annotations

import os
import unittest
from unittest import mock

from scripts import identity_collaboration_target as target


class DatabaseContractTargetingTest(unittest.TestCase):
    def test_database_from_stack_a_cannot_use_status_from_stack_b(self) -> None:
        status_b = {
            "DB_URL": "postgresql://postgres:x@127.0.0.1:62002/postgres",
            "REST_URL": "http://127.0.0.1:62001/rest/v1",
            "ANON_KEY": "anon-b",
            "SERVICE_ROLE_KEY": "service-b",
            "JWT_SECRET": "jwt-b",
        }
        db_a = "postgresql://postgres:x@127.0.0.1:61002/postgres"
        identities = {db_a: "system-a:postgres", status_b["DB_URL"]: "system-b:postgres"}
        with mock.patch.dict(os.environ, {"DATABASE_URL": db_a}, clear=True), \
             mock.patch.object(target, "local_status", return_value=status_b), \
             mock.patch.object(target, "_database_identity", side_effect=identities.__getitem__):
            with self.assertRaisesRegex(SystemExit, "does not match"):
                target.resolve_target()

    def test_verified_context_overwrites_poisoned_stack_credentials(self) -> None:
        context = target.TargetContext(
            database_url="postgresql://postgres:x@127.0.0.1:61002/postgres",
            rest_url="http://127.0.0.1:61001/rest/v1",
            anon_key="anon-a", service_role_key="service-a", jwt_secret="jwt-a",
            workdir="/tmp/stack-a", identity="system-a:postgres",
        )
        poisoned = {
            "DATABASE_URL": "postgresql://postgres:x@127.0.0.1:62002/postgres",
            "SUPABASE_REST_URL": "http://127.0.0.1:62001/rest/v1",
            "SUPABASE_ANON_KEY": "anon-b",
            "SUPABASE_SERVICE_ROLE_KEY": "service-b",
            "SUPABASE_JWT_SECRET": "jwt-b",
        }
        with mock.patch.dict(os.environ, poisoned, clear=True):
            target.apply_target_environment(context)
            self.assertEqual(os.environ["DATABASE_URL"], context.database_url)
            self.assertEqual(os.environ["SUPABASE_REST_URL"], context.rest_url)
            self.assertEqual(os.environ["SUPABASE_ANON_KEY"], context.anon_key)
            self.assertEqual(os.environ["SUPABASE_SERVICE_ROLE_KEY"], context.service_role_key)
            self.assertEqual(os.environ["SUPABASE_JWT_SECRET"], context.jwt_secret)


if __name__ == "__main__":
    unittest.main()
