#!/usr/bin/env python3
"""Fail-closed hosted exposure, default-ACL, and REST-negative operator gate."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

EXPECTED_SCHEMAS = ("api", "public", "graphql_public")
INTERNAL_SCHEMAS = ("private", "util", "archive")
DENIED_PUBLIC_RELATIONS = ("lca_active_snapshots", "lca_factorization_registry")
DENIED_PRIVATE_RPC = "search_flows_latest_impl"
POSTURE_CONTRACT_VERSION = "security-acl.expand.v2"
DEFAULT_PRIVILEGE_EVALUATION = "built-in+global+per-schema-effective"
REPO_OWNER_FUNCTION_DEFAULT_SCOPE = "database-global-all-schemas"


def normalized_schemas(value: str) -> tuple[str, ...]:
    return tuple(sorted(part.strip() for part in value.split(",") if part.strip()))


def request_json(url: str, headers: dict[str, str], *, method: str = "GET", body: bytes | None = None) -> tuple[int, dict]:
    request = urllib.request.Request(url, headers=headers, method=method, data=body)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = response.read().decode("utf-8")
            return response.status, json.loads(payload or "{}")
    except urllib.error.HTTPError as error:
        payload = error.read().decode("utf-8")
        try:
            return error.code, json.loads(payload or "{}")
        except json.JSONDecodeError:
            return error.code, {"code": "NON_JSON_RESPONSE"}


def database_posture(database_url: str) -> dict:
    query = "begin read only; select posture::text from util.security_acl_expand_posture; commit;"
    result = subprocess.run(
        ["psql", database_url, "-XAt", "-v", "ON_ERROR_STOP=1", "-c", query],
        check=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise SystemExit("hosted posture query failed")
    lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
    if len(lines) != 1:
        raise SystemExit("hosted posture query did not return exactly one JSON record")
    try:
        posture = json.loads(lines[0])
    except json.JSONDecodeError:
        raise SystemExit("hosted posture query returned invalid JSON") from None
    if not isinstance(posture, dict):
        raise SystemExit("hosted posture query returned invalid JSON")
    return posture


def validate_posture(posture: dict, *, require_platform_owner: bool) -> None:
    if posture.get("contractVersion") != POSTURE_CONTRACT_VERSION:
        raise SystemExit("hosted posture does not use the reviewed effective-default contract")
    if posture.get("defaultPrivilegeEvaluation") != DEFAULT_PRIVILEGE_EVALUATION:
        raise SystemExit("hosted posture does not evaluate built-in, global, and per-schema defaults")
    if posture.get("repoOwnerFunctionDefaultScope") != REPO_OWNER_FUNCTION_DEFAULT_SCOPE:
        raise SystemExit("hosted posture does not report the database-global postgres function default")
    if posture.get("evaluatedApplicationSchemas") != ["public", "api", "private", "util", "archive"]:
        raise SystemExit("hosted posture application-schema evaluation target is not exact")
    repo_residue = posture.get("repoOwnerDefaultPrivilegeResidue")
    platform_residue = posture.get("platformOwnerDefaultPrivilegeResidue")
    if not isinstance(repo_residue, list) or not isinstance(platform_residue, list):
        raise SystemExit("hosted posture default-privilege evidence has an invalid shape")
    if repo_residue or not posture.get("migrationReady"):
        raise SystemExit("migration-owned ACL posture is not ready")
    if require_platform_owner and (platform_residue or not posture.get("hostedOperatorReady")):
        raise SystemExit("supabase_admin effective default privileges remain; issue #352 owner action is required")


def management_config(project_ref: str, access_token: str) -> dict:
    status, payload = request_json(
        f"https://api.supabase.com/v1/projects/{project_ref}/postgrest",
        {"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
    )
    if status != 200:
        raise SystemExit(f"Management API PostgREST readback failed with HTTP {status}")
    if not isinstance(payload, dict):
        raise SystemExit("Management API PostgREST readback returned an invalid response")
    # Keep unrelated Management API fields out of all downstream evidence and
    # exception paths. The hosted ACL gate needs only this reviewed field.
    return {"db_schema": payload.get("db_schema")}


def rest_headers(public_credential: str) -> dict[str, str]:
    headers = {"apikey": public_credential, "Accept": "application/json"}
    # Legacy anon keys are JWTs. Opaque sb_publishable_ keys are gateway API
    # keys and fail if they are also sent as Authorization bearer values.
    if public_credential.startswith("eyJ"):
        headers["Authorization"] = f"Bearer {public_credential}"
    return headers


def assert_rest_boundaries(supabase_url: str, public_credential: str) -> list[dict]:
    base_headers = rest_headers(public_credential)
    evidence: list[dict] = []
    for schema in INTERNAL_SCHEMAS:
        status, payload = request_json(
            f"{supabase_url.rstrip('/')}/rest/v1/__issue_339_negative_probe__?select=*",
            {**base_headers, "Accept-Profile": schema},
        )
        if status != 406 or payload.get("code") != "PGRST106":
            raise SystemExit(f"internal schema {schema} is not fail-closed through REST: HTTP {status}, code={payload.get('code')}")
        evidence.append({"probe": f"profile:{schema}", "status": status, "code": payload.get("code")})
    for relation in DENIED_PUBLIC_RELATIONS:
        status, payload = request_json(
            f"{supabase_url.rstrip('/')}/rest/v1/{relation}?select=*&limit=1",
            {**base_headers, "Accept-Profile": "public"},
        )
        if status < 400 or payload.get("code") not in {"42501", "PGRST301"}:
            raise SystemExit(f"anon relation probe unexpectedly reachable: {relation}, HTTP {status}, code={payload.get('code')}")
        evidence.append({"probe": f"relation:{relation}", "status": status, "code": payload.get("code")})
    status, payload = request_json(
        f"{supabase_url.rstrip('/')}/rest/v1/rpc/{DENIED_PRIVATE_RPC}",
        {**base_headers, "Content-Type": "application/json", "Content-Profile": "private"},
        method="POST",
        body=b"{}",
    )
    if status != 406 or payload.get("code") != "PGRST106":
        raise SystemExit(
            f"private RPC profile unexpectedly reachable: {DENIED_PRIVATE_RPC}, HTTP {status}, code={payload.get('code')}"
        )
    evidence.append({"probe": f"rpc:private.{DENIED_PRIVATE_RPC}", "status": status, "code": payload.get("code")})
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-only", action="store_true", help="run only the SQL posture gate (local/disposable proof)")
    parser.add_argument("--evidence", type=Path, help="write a secret-free JSON evidence record to a new file")
    args = parser.parse_args()

    database_url = os.environ.get("SECURITY_ACL_DATABASE_URL", "")
    if not database_url:
        raise SystemExit("SECURITY_ACL_DATABASE_URL is required")
    posture = database_posture(database_url)
    validate_posture(posture, require_platform_owner=not args.database_only)

    evidence = {"schemaVersion": "security-acl-hosted-evidence.v1", "posture": posture, "rest": []}
    if args.database_only:
        print(json.dumps(evidence, sort_keys=True))
        return 0

    project_ref = os.environ.get("SECURITY_ACL_PROJECT_REF", "")
    supabase_url = os.environ.get("SECURITY_ACL_SUPABASE_URL", "")
    public_credential = os.environ.get("SECURITY_ACL_ANON_KEY", "")
    access_token = os.environ.get("SUPABASE_ACCESS_TOKEN", "")
    if not re.fullmatch(r"[a-z]{20}", project_ref):
        raise SystemExit("SECURITY_ACL_PROJECT_REF must be the exact 20-letter project ref")
    if f"https://{project_ref}.supabase.co" != supabase_url.rstrip("/"):
        raise SystemExit("SECURITY_ACL_SUPABASE_URL does not match the exact project ref")
    if not public_credential or not access_token:
        raise SystemExit("SECURITY_ACL_ANON_KEY and SUPABASE_ACCESS_TOKEN are required")

    config = management_config(project_ref, access_token)
    if normalized_schemas(config.get("db_schema", "")) != tuple(sorted(EXPECTED_SCHEMAS)):
        raise SystemExit(f"hosted exposed schemas mismatch: expected {EXPECTED_SCHEMAS}")
    evidence["projectRef"] = project_ref
    evidence["exposedSchemas"] = list(EXPECTED_SCHEMAS)
    evidence["rest"] = assert_rest_boundaries(supabase_url, public_credential)
    if args.evidence:
        descriptor = os.open(args.evidence, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(evidence, handle, sort_keys=True)
            handle.write("\n")
    print(json.dumps(evidence, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
