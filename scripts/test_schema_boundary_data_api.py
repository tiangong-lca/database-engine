#!/usr/bin/env python3
"""Verify Issue #354 view boundaries through the local PostgREST API."""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PUBLIC_VIEWS = {
    "worker_domain_traceability_cutoffs": "domain_source,required_worker_column,cutover_at,traceability_required,contract_note",
    "worker_domain_traceability_violations": "domain_source,domain_id,domain_role,created_at,updated_at,cutover_at,violation_code,details",
    "worker_job_domain_refs": "worker_job_id,domain_source,domain_id,domain_role,legacy_job_id,status,created_at,updated_at",
    "worker_legacy_lifecycle_audit": "legacy_source,task_family,legacy_status,row_count,active_count,oldest_created_at,newest_created_at,latest_updated_at",
    "worker_legacy_table_retirement_blockers": "legacy_table,blocker_type,blocker_schema,blocker_name,blocker_identity,is_drop_restrict_blocker,details",
}


def request_headers(key: str, *, profile: str) -> dict[str, str]:
    headers = {"apikey": key, "Accept": "application/json", "Accept-Profile": profile}
    if key.startswith("eyJ"):
        headers["Authorization"] = f"Bearer {key}"
    return headers


def request(url: str, key: str, *, profile: str) -> tuple[int, object]:
    req = urllib.request.Request(url, headers=request_headers(key, profile=profile))
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


def reload_schema(db_url: str) -> None:
    result = subprocess.run(
        ["psql", db_url, "-XAt", "-v", "ON_ERROR_STOP=1", "-c", "notify pgrst, 'reload schema'"],
        cwd=ROOT,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if result.returncode != 0:
        raise SystemExit("PostgREST schema reload failed")


def status_environment() -> dict[str, str]:
    command = ["supabase"]
    if workdir := os.environ.get("SUPABASE_WORKDIR"):
        command.extend(["--workdir", workdir])
    command.extend(["status", "--output", "json"])
    return json.loads(subprocess.run(
        command, cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE
    ).stdout)


def relation_url(rest_url: str, name: str, columns: str) -> str:
    return f"{rest_url}/{name}?{urllib.parse.urlencode({'select': columns, 'limit': 1})}"


def main() -> int:
    status: dict[str, str] = {}
    credential_names = ("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_ANON_KEY")
    if not all(os.environ.get(name) for name in (
        "SUPABASE_REST_URL", *credential_names, "DATABASE_URL"
    )):
        status = status_environment()
    rest_url = os.environ.get("SUPABASE_REST_URL", status.get("REST_URL", "")).rstrip("/")
    credentials = [
        os.environ.get(name, status.get(name.removeprefix("SUPABASE_"), ""))
        for name in credential_names
    ]
    service_credential, anonymous_credential = credentials
    db_url = os.environ.get("DATABASE_URL", status.get("DB_URL", ""))

    reload_schema(db_url)
    # Wait for the schema reload without assuming immediate listener delivery.
    probe_url = relation_url(rest_url, "worker_job_domain_refs", PUBLIC_VIEWS["worker_job_domain_refs"])
    for _ in range(40):
        service_status, _ = request(probe_url, service_credential, profile="public")
        if service_status == 200:
            break
        time.sleep(0.25)
    else:
        raise AssertionError("PostgREST did not expose the Issue #354 compatibility views")

    for name, columns in PUBLIC_VIEWS.items():
        url = relation_url(rest_url, name, columns)
        service_status, service_body = request(url, service_credential, profile="public")
        assert service_status == 200 and isinstance(service_body, list), (name, service_status, service_body)
        anon_status, anon_body = request(url, anonymous_credential, profile="public")
        assert anon_status in (401, 403, 404) and isinstance(anon_body, dict), (name, anon_status, anon_body)

    api_status, api_body = request(
        relation_url(rest_url, "worker_job_domain_refs", PUBLIC_VIEWS["worker_job_domain_refs"]),
        service_credential,
        profile="api",
    )
    assert api_status == 200 and isinstance(api_body, list), (api_status, api_body)

    for profile in ("private", "util", "archive"):
        status_code, body = request(probe_url, service_credential, profile=profile)
        assert status_code == 406 and isinstance(body, dict) and body.get("code") == "PGRST106", (
            profile, status_code, body
        )

    print("PASS PostgREST enforces the Issue #354 schema/view boundary")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
