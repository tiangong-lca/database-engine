#!/usr/bin/env python3
"""Conformance-check audit-v2 Data API claims with ephemeral PostgREST v14.7."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import security_definer_audit_v2 as audit

ARTIFACT = ROOT / "supabase/tests/contracts/security_definer_audit_v2.json"
ARTIFACT_SHA256 = ARTIFACT.with_suffix(".sha256")
POSTGREST_IMAGE = (
    "public.ecr.aws/supabase/postgrest@"
    "sha256:8b53afca2e239bc90a0facdb880710232886c38dae5743a57d66056e96d5596a"
)
STARTUP_TIMEOUT_SECONDS = 30


def run_docker(*arguments: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    """Run Docker without ever adding the database URI to its argv."""
    return subprocess.run(
        ["docker", *arguments],
        cwd=ROOT,
        env=env,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=20,
    )


def load_artifact() -> dict:
    raw = ARTIFACT.read_bytes()
    expected_digest = ARTIFACT_SHA256.read_text(encoding="utf-8").strip()
    if not re.fullmatch(r"[0-9a-f]{64}", expected_digest):
        raise AssertionError("audit-v2 SHA-256 sidecar is malformed")
    if hashlib.sha256(raw).hexdigest() != expected_digest:
        raise AssertionError("audit-v2 artifact bytes differ from its SHA-256 sidecar")
    value = json.loads(raw)
    if value.get("schemaVersion") != "database.security-definer-audit.v2":
        raise AssertionError("unexpected audit-v2 schemaVersion")
    if value.get("auditArtifactComplete") is not True:
        raise AssertionError("audit-v2 artifact is not complete")
    return value


def container_database_url(database_url: str) -> str:
    try:
        connection = audit.parse_loopback_connection(database_url)
    except ValueError as exc:
        raise AssertionError("conformance database must be an explicit loopback TCP URL") from exc
    if connection.credential is None:
        raise AssertionError("database URL must contain local test credentials")
    user = urllib.parse.quote(connection.user, safe="")
    credential = urllib.parse.quote(connection.credential, safe="")
    database = urllib.parse.quote(connection.database, safe="")
    authority = "@".join((
        ":".join((user, credential)),
        f"host.docker.internal:{connection.port}",
    ))
    return urllib.parse.urlunsplit(
        ("postgresql", authority, f"/{database}", "", "")
    )


def endpoint_name(endpoint: dict) -> str:
    object_key = endpoint.get("currentObjectKey", "")
    prefix = object_key.split("(", 1)[0]
    schema, separator, name = prefix.partition(".")
    if not separator or schema != endpoint.get("currentSchema") or not name:
        raise AssertionError(f"malformed audit endpoint key: {object_key!r}")
    return name


def anon_openapi_expected(endpoint: dict) -> bool:
    rows = [row for row in endpoint.get("roleMatrix", []) if row.get("role") == "anon"]
    if len(rows) != 1:
        raise AssertionError(
            f"endpoint must contain exactly one anon role row: {endpoint.get('currentObjectKey')}"
        )
    row = rows[0]
    required = (
        "dataApiExposedSchema",
        "dataApiTransportRole",
        "effectiveSchemaUsage",
        "effectiveExecute",
    )
    if any(field not in row for field in required):
        raise AssertionError(
            f"anon role row lacks Data API evidence: {endpoint.get('currentObjectKey')}"
        )
    shape = endpoint.get("postgrestShape", {})
    cache_eligible = shape.get("schemaCacheEligible", shape.get("eligible"))
    if not isinstance(cache_eligible, bool):
        raise AssertionError(
            f"endpoint lacks PostgREST schema-cache evidence: {endpoint.get('currentObjectKey')}"
        )
    return all(bool(row[field]) for field in required) and cache_eligible


def expected_routes_by_schema(artifact: dict) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    governed: dict[str, set[str]] = {schema: set() for schema in artifact["governedSchemas"]}
    expected: dict[str, set[str]] = {schema: set() for schema in artifact["exposedSchemas"]}
    for routine in artifact.get("routines", []):
        endpoints = []
        if routine.get("canonical") is not None:
            endpoints.append(routine["canonical"])
        endpoints.extend(routine.get("compatibilityAliases", []))
        for endpoint in endpoints:
            schema = endpoint.get("currentSchema")
            if schema not in governed:
                raise AssertionError(f"endpoint uses an ungoverned schema: {schema!r}")
            name = endpoint_name(endpoint)
            governed[schema].add(name)
            if anon_openapi_expected(endpoint):
                if schema not in expected:
                    raise AssertionError(
                        f"audit claims an anonymous Data API endpoint in unexposed schema {schema}"
                    )
                expected[schema].add(name)
    return governed, expected


def openapi_request(base_url: str, schema: str | None) -> tuple[int, dict, str]:
    headers = {"Accept": "application/openapi+json"}
    if schema is not None:
        headers["Accept-Profile"] = schema
    request = urllib.request.Request(
        f"{base_url}/",
        headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            raw = response.read()
            return response.status, json.loads(raw), response.headers.get("Server", "")
    except urllib.error.HTTPError as error:
        raw = error.read()
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {}
        return error.code, body, error.headers.get("Server", "")


class PostgrestV147ConformanceTest(unittest.TestCase):
    container_name = ""
    base_url = ""
    artifact: dict = {}

    @classmethod
    def setUpClass(cls) -> None:
        cls.artifact = load_artifact()
        image = run_docker("image", "inspect", POSTGREST_IMAGE)
        if image.returncode != 0:
            raise AssertionError(f"required local image is unavailable: {POSTGREST_IMAGE}")

        database_url = container_database_url(
            os.environ.get("ISSUE333_DATABASE_URL") or audit.database_url()
        )
        cls.container_name = f"db333-postgrest-v147-{os.getpid()}-{time.time_ns():x}"
        container_env = os.environ.copy()
        container_env.update(
            {
                "PGRST_DB_URI": database_url,
                "PGRST_DB_SCHEMAS": ",".join(cls.artifact["exposedSchemas"]),
                "PGRST_DB_ANON_ROLE": "anon",
                "PGRST_DB_EXTRA_SEARCH_PATH": "public,extensions",
                "PGRST_LOG_LEVEL": "crit",
            }
        )
        started = run_docker(
            "run",
            "--detach",
            "--rm",
            "--name",
            cls.container_name,
            "--publish",
            "127.0.0.1::3000",
            "--add-host",
            "host.docker.internal:host-gateway",
            "--env",
            "PGRST_DB_URI",
            "--env",
            "PGRST_DB_SCHEMAS",
            "--env",
            "PGRST_DB_ANON_ROLE",
            "--env",
            "PGRST_DB_EXTRA_SEARCH_PATH",
            "--env",
            "PGRST_LOG_LEVEL",
            POSTGREST_IMAGE,
            env=container_env,
        )
        if started.returncode != 0:
            cls._cleanup_container()
            raise AssertionError("ephemeral PostgREST v14.7 container failed to start")

        try:
            port_result = run_docker("port", cls.container_name, "3000/tcp")
            if port_result.returncode != 0:
                raise AssertionError("cannot resolve ephemeral PostgREST loopback port")
            bindings = [line.strip() for line in port_result.stdout.splitlines() if line.strip()]
            loopback = [line for line in bindings if line.startswith("127.0.0.1:")]
            if len(loopback) != 1:
                raise AssertionError("PostgREST must have exactly one IPv4 loopback binding")
            port = int(loopback[0].rsplit(":", 1)[1])
            cls.base_url = f"http://127.0.0.1:{port}"

            deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
            while time.monotonic() < deadline:
                status, _body, server = openapi_request(
                    cls.base_url, cls.artifact["exposedSchemas"][0]
                )
                if status == 200:
                    if "postgrest/14.7" not in server.lower():
                        raise AssertionError(
                            f"unexpected PostgREST Server version: {server or '<missing>'}"
                        )
                    break
                inspected = run_docker(
                    "inspect", "--format", "{{.State.Running}}", cls.container_name
                )
                if inspected.returncode != 0 or inspected.stdout.strip() != "true":
                    raise AssertionError("ephemeral PostgREST exited before becoming ready")
                time.sleep(0.25)
            else:
                raise AssertionError("ephemeral PostgREST did not become ready within 30 seconds")
        except BaseException:
            cls._cleanup_container()
            raise

    @classmethod
    def _cleanup_container(cls) -> None:
        if not cls.container_name:
            return
        run_docker("rm", "--force", cls.container_name)
        residue = run_docker("inspect", cls.container_name)
        if residue.returncode == 0:
            raise AssertionError("ephemeral PostgREST container cleanup left residue")

    @classmethod
    def tearDownClass(cls) -> None:
        cls._cleanup_container()

    def test_openapi_routes_match_audit_v2_anonymous_claims(self) -> None:
        governed, expected = expected_routes_by_schema(self.artifact)
        for schema in self.artifact["exposedSchemas"]:
            with self.subTest(schema=schema):
                status, body, _server = openapi_request(self.base_url, schema)
                self.assertEqual(status, 200, body)
                paths = body.get("paths")
                self.assertIsInstance(paths, dict, body)
                actual = {
                    urllib.parse.unquote(path.removeprefix("/rpc/"))
                    for path in paths
                    if path.startswith("/rpc/")
                }
                actual_governed = actual & governed.get(schema, set())
                self.assertEqual(
                    actual_governed,
                    expected[schema],
                    {
                        "schema": schema,
                        "unexpectedVisible": sorted(actual_governed - expected[schema]),
                        "missingVisible": sorted(expected[schema] - actual_governed),
                    },
                )

    def test_no_profile_uses_the_first_configured_schema(self) -> None:
        first_schema = self.artifact["exposedSchemas"][0]
        default_status, default_body, _server = openapi_request(self.base_url, None)
        explicit_status, explicit_body, _server = openapi_request(self.base_url, first_schema)
        self.assertEqual(default_status, 200, default_body)
        self.assertEqual(explicit_status, 200, explicit_body)
        self.assertEqual(default_body.get("paths"), explicit_body.get("paths"))

    def test_governed_internal_schemas_are_not_profiles(self) -> None:
        exposed = set(self.artifact["exposedSchemas"])
        for schema in sorted(set(self.artifact["governedSchemas"]) - exposed):
            with self.subTest(schema=schema):
                status, body, _server = openapi_request(self.base_url, schema)
                self.assertEqual(status, 406, body)
                self.assertEqual(body.get("code"), "PGRST106", body)


if __name__ == "__main__":
    unittest.main(verbosity=2)
