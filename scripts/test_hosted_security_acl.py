#!/usr/bin/env python3

import io
import json
import os
import subprocess
import sys
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hosted_security_acl as target


class HostedSecurityAclContractTest(unittest.TestCase):
    def effective_posture(self) -> dict:
        return {
            "contractVersion": target.POSTURE_CONTRACT_VERSION,
            "defaultPrivilegeEvaluation": target.DEFAULT_PRIVILEGE_EVALUATION,
            "repoOwnerFunctionDefaultScope": target.REPO_OWNER_FUNCTION_DEFAULT_SCOPE,
            "evaluatedApplicationSchemas": ["public", "api", "private", "util", "archive"],
            "migrationReady": True,
            "hostedOperatorReady": False,
            "repoOwnerDefaultPrivilegeResidue": [],
            "platformOwnerDefaultPrivilegeResidue": [
                {
                    "schema_name": "public",
                    "object_type": "f",
                    "grantee": "PUBLIC",
                    "privilege_type": "EXECUTE",
                }
            ],
        }

    def test_database_only_accepts_repo_closure_with_issue_352_visible(self) -> None:
        target.validate_posture(self.effective_posture(), require_platform_owner=False)

    def test_hosted_gate_rejects_issue_352_effective_residue(self) -> None:
        with self.assertRaisesRegex(SystemExit, "issue #352"):
            target.validate_posture(self.effective_posture(), require_platform_owner=True)

    def test_hosted_gate_accepts_all_owner_defaults_closed(self) -> None:
        posture = self.effective_posture()
        posture["platformOwnerDefaultPrivilegeResidue"] = []
        posture["hostedOperatorReady"] = True
        target.validate_posture(posture, require_platform_owner=True)

    def test_old_explicit_row_only_posture_is_rejected(self) -> None:
        posture = self.effective_posture()
        posture["contractVersion"] = "security-acl.expand.v1"
        with self.assertRaisesRegex(SystemExit, "effective-default contract"):
            target.validate_posture(posture, require_platform_owner=False)

    def test_missing_effective_residue_arrays_are_rejected(self) -> None:
        posture = self.effective_posture()
        del posture["repoOwnerDefaultPrivilegeResidue"]
        with self.assertRaisesRegex(SystemExit, "invalid shape"):
            target.validate_posture(posture, require_platform_owner=False)

    def test_application_target_cannot_hide_global_function_scope(self) -> None:
        posture = self.effective_posture()
        posture["repoOwnerFunctionDefaultScope"] = "application-schemas-only"
        with self.assertRaisesRegex(SystemExit, "database-global"):
            target.validate_posture(posture, require_platform_owner=False)

    def test_schema_normalization_preserves_reviewed_precedence(self) -> None:
        self.assertEqual(
            target.normalized_schemas(" api, public,graphql_public "),
            target.EXPECTED_SCHEMAS,
        )
        self.assertNotEqual(
            target.normalized_schemas("public,api,graphql_public"),
            target.EXPECTED_SCHEMAS,
        )
        self.assertNotEqual(
            target.normalized_schemas("api,public,graphql_public,private"),
            target.EXPECTED_SCHEMAS,
        )

    def test_duplicate_schema_does_not_pass_exact_gate(self) -> None:
        self.assertNotEqual(
            target.normalized_schemas("api,api,public,graphql_public"),
            target.EXPECTED_SCHEMAS,
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

    def test_hosted_main_rejects_public_first_schema_precedence(self) -> None:
        posture = self.effective_posture()
        posture["platformOwnerDefaultPrivilegeResidue"] = []
        posture["hostedOperatorReady"] = True
        environment = {
            "SECURITY_ACL_DATABASE_URL": "postgresql://postgres@127.0.0.1:54322/postgres",
            "SECURITY_ACL_PROJECT_REF": "abcdefghijklmnopqrst",
            "SECURITY_ACL_SUPABASE_URL": "https://abcdefghijklmnopqrst.supabase.co",
            "SECURITY_ACL_ANON_KEY": "sb_publishable_test",
            "SUPABASE_ACCESS_TOKEN": "management-token",
        }
        with mock.patch.dict(os.environ, environment, clear=True), mock.patch.object(
            target, "database_posture", return_value=posture,
        ), mock.patch.object(
            target, "management_config",
            return_value={"db_schema": "public,api,graphql_public"},
        ), mock.patch.object(sys, "argv", ["hosted_security_acl.py"]):
            with self.assertRaisesRegex(SystemExit, "hosted exposed schemas mismatch"):
                target.main()

    def test_hosted_main_evidence_preserves_api_first_readback(self) -> None:
        posture = self.effective_posture()
        posture["platformOwnerDefaultPrivilegeResidue"] = []
        posture["hostedOperatorReady"] = True
        environment = {
            "SECURITY_ACL_DATABASE_URL": "postgresql://postgres@127.0.0.1:54322/postgres",
            "SECURITY_ACL_PROJECT_REF": "abcdefghijklmnopqrst",
            "SECURITY_ACL_SUPABASE_URL": "https://abcdefghijklmnopqrst.supabase.co",
            "SECURITY_ACL_ANON_KEY": "sb_publishable_test",
            "SUPABASE_ACCESS_TOKEN": "management-token",
        }
        with mock.patch.dict(os.environ, environment, clear=True), mock.patch.object(
            target, "database_posture", return_value=posture,
        ), mock.patch.object(
            target, "management_config",
            return_value={"db_schema": "api,public,graphql_public"},
        ), mock.patch.object(
            target, "assert_rest_boundaries", return_value=[],
        ), mock.patch.object(sys, "argv", ["hosted_security_acl.py"]), mock.patch(
            "sys.stdout", new_callable=io.StringIO,
        ) as stdout:
            self.assertEqual(target.main(), 0)
        evidence = json.loads(stdout.getvalue())
        self.assertEqual(evidence["exposedSchemas"], ["api", "public", "graphql_public"])


if __name__ == "__main__":
    unittest.main()
