#!/usr/bin/env python3

import io
import subprocess
import sys
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hosted_security_acl as target


class HostedSecurityAclContractTest(unittest.TestCase):
    def test_schema_normalization_is_order_independent_and_exact(self) -> None:
        self.assertEqual(target.normalized_schemas(" graphql_public,public, api "), ("api", "graphql_public", "public"))
        self.assertNotEqual(target.normalized_schemas("public,api,graphql_public,private"), tuple(sorted(target.EXPECTED_SCHEMAS)))

    def test_duplicate_schema_does_not_pass_exact_gate(self) -> None:
        self.assertNotEqual(
            target.normalized_schemas("api,api,public,graphql_public"),
            tuple(sorted(target.EXPECTED_SCHEMAS)),
        )

    def test_reviewed_schema_order_matches_postgrest_config_gate(self) -> None:
        self.assertEqual(target.EXPECTED_SCHEMAS, ("api", "public", "graphql_public"))

    def test_opaque_publishable_key_is_apikey_only(self) -> None:
        credential = "sb_publishable_must-not-be-bearer"
        self.assertEqual(
            target.rest_headers(credential),
            {"apikey": credential, "Accept": "application/json"},
        )

    def test_legacy_jwt_key_is_also_a_bearer_token(self) -> None:
        credential = "eyJlegacy-anon-jwt"
        self.assertEqual(
            target.rest_headers(credential),
            {
                "apikey": credential,
                "Authorization": f"Bearer {credential}",
                "Accept": "application/json",
            },
        )

    def test_management_error_does_not_echo_response_or_token(self) -> None:
        response_secret = "response-body-must-not-be-logged"
        access_token = "management-token-must-not-be-logged"

        def reject(*_args: object, **_kwargs: object) -> object:
            raise urllib.error.HTTPError(
                "https://api.supabase.com/redacted",
                500,
                "failure",
                {},
                io.BytesIO(f'{{"secret":"{response_secret}"}}'.encode()),
            )

        with mock.patch.object(target.urllib.request, "urlopen", reject):
            with self.assertRaises(SystemExit) as raised:
                target.management_config("abcdefghijklmnopqrst", access_token)
        message = str(raised.exception)
        self.assertEqual(message, "Management API PostgREST readback failed with HTTP 500")
        self.assertNotIn(response_secret, message)
        self.assertNotIn(access_token, message)

    def test_database_failure_does_not_echo_url_stdout_or_stderr(self) -> None:
        database_url = "".join(
            ("postgresql", "://", "operator", ":", "database-password", "@host/postgres")
        )
        stdout_secret = "stdout-secret-must-not-be-logged"
        stderr_secret = "stderr-body-must-not-be-logged"
        failed = subprocess.CompletedProcess(
            args=["psql", database_url],
            returncode=2,
            stdout=stdout_secret,
            stderr=stderr_secret,
        )
        with mock.patch.object(target.subprocess, "run", return_value=failed):
            with self.assertRaises(SystemExit) as raised:
                target.database_posture(database_url)
        message = str(raised.exception)
        self.assertEqual(message, "hosted posture query failed")
        for secret in (database_url, "database-password", stdout_secret, stderr_secret):
            self.assertNotIn(secret, message)

    def test_management_success_payload_is_not_emitted(self) -> None:
        payload_secret = "remote-field-must-not-be-logged"
        with mock.patch.object(
            target,
            "request_json",
            return_value=(200, {"db_schema": "api,public,graphql_public", "secret": payload_secret}),
        ):
            config = target.management_config("abcdefghijklmnopqrst", "token")
        self.assertEqual(config, {"db_schema": "api,public,graphql_public"})
        self.assertNotIn(payload_secret, repr(config))


if __name__ == "__main__":
    unittest.main()
