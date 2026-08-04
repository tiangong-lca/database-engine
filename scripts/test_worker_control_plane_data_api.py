#!/usr/bin/env python3
"""Verify the committed Worker RPCs are visible and fail closed in PostgREST."""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RPC_CASES = {
    "worker_read_jobs_by_ids": {"p_job_ids": [], "p_include_internal": True},
    "worker_list_jobs_by_concurrency_key": {
        "p_job_kind": "lca.snapshot_gc",
        "p_concurrency_key": "contract:empty",
        "p_statuses": ["queued"],
        "p_limit": 20,
        "p_include_internal": True,
    },
}


def request_headers(key: str, *, payload: dict | None, profile: str) -> dict[str, str]:
    headers = {
        "apikey": key,
        "Accept": "application/openapi+json" if payload is None else "application/json",
    }
    if key.startswith("eyJ"):
        headers["Authorization"] = f"Bearer {key}"
    headers["Accept-Profile" if payload is None else "Content-Profile"] = profile
    if payload is not None:
        headers["Content-Type"] = "application/json"
    return headers


def request(url: str, key: str, *, payload: dict | None = None, profile: str = "public") -> tuple[int, dict]:
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers=request_headers(key, payload=payload, profile=profile))
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


def main() -> int:
    rest_url = os.environ.get("SUPABASE_REST_URL")
    credential_names = ("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_ANON_KEY")
    credentials = [os.environ.get(name) for name in credential_names]
    db_url = os.environ.get("DATABASE_URL")
    if not all((rest_url, *credentials, db_url)):
        status = json.loads(subprocess.run(
            ["supabase", "status", "--output", "json"], cwd=ROOT, check=True,
            text=True, stdout=subprocess.PIPE,
        ).stdout)
        rest_url = rest_url or status["REST_URL"]
        credentials = [value or status[name.removeprefix("SUPABASE_")] for name, value in zip(credential_names, credentials)]
        db_url = db_url or status["DB_URL"]
    rest_url = rest_url.rstrip("/")
    service_credential, anonymous_credential = credentials

    # A clean reset can commit migrations before the restarted PostgREST
    # listener subscribes. Reload only after the service is healthy, then poll
    # the actual role-filtered OpenAPI catalog instead of assuming delivery.
    reload_schema(db_url)
    for _ in range(40):
        openapi_status, openapi = request(f"{rest_url}/", service_credential, profile="public")
        paths = openapi.get("paths", {}) if openapi_status == 200 else {}
        if all(f"/rpc/{name}" in paths for name in RPC_CASES):
            break
        time.sleep(0.25)
    else:
        raise AssertionError("PostgREST schema cache did not expose both Worker RPCs")

    for name, payload in RPC_CASES.items():
        service_status, service_body = request(
            f"{rest_url}/rpc/{name}", service_credential, payload=payload, profile="public",
        )
        assert service_status == 200 and service_body == {"ok": True, "data": []}, service_body
        anon_status, anon_body = request(
            f"{rest_url}/rpc/{name}", anonymous_credential, payload=payload, profile="public"
        )
        assert anon_status in (401, 403, 404) and anon_body.get("code") in ("42501", "PGRST202"), anon_body

    print("PASS PostgREST exposes both Worker RPCs to service_role and denies anon")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
