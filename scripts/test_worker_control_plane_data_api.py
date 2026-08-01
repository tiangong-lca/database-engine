#!/usr/bin/env python3
"""Verify Worker compatibility relations/RPCs and private-profile denial in PostgREST."""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import security_definer_audit_v2 as audit

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
WORKER_RELATIONS = (
    "worker_job_kinds", "worker_jobs", "worker_job_events", "worker_job_artifacts",
)
MINIMUM_SERVICE_RELATIONS = ("worker_jobs", "worker_job_artifacts")


def request_headers(
    key: str, *, payload: dict | None, profile: str, openapi: bool | None = None,
) -> dict[str, str]:
    if openapi is None:
        openapi = payload is None
    headers = {
        "apikey": key,
        "Accept": "application/openapi+json" if openapi else "application/json",
    }
    if key.startswith("eyJ"):
        headers["Authorization"] = f"Bearer {key}"
    headers["Accept-Profile" if payload is None else "Content-Profile"] = profile
    if payload is not None:
        headers["Content-Type"] = "application/json"
    return headers


def request(
    url: str, key: str, *, payload: dict | None = None, profile: str = "public",
    openapi: bool | None = None,
) -> tuple[int, dict]:
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data,
        headers=request_headers(key, payload=payload, profile=profile, openapi=openapi),
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


def reload_schema(db_url: str) -> None:
    connection = audit.parse_loopback_connection(db_url)
    result = subprocess.run(
        connection.command("-XAt", "-v", "ON_ERROR_STOP=1"),
        cwd=ROOT,
        env=connection.environment(),
        input="notify pgrst, 'reload schema';\n",
        text=True,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if result.returncode != 0:
        raise AssertionError("PostgREST schema reload failed")


def runtime() -> tuple[str, str, str, str]:
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
        credentials = [
            value or status[name.removeprefix("SUPABASE_")]
            for name, value in zip(credential_names, credentials)
        ]
        db_url = db_url or status["DB_URL"]
    audit.parse_loopback_connection(db_url)
    return rest_url.rstrip("/"), credentials[0], credentials[1], db_url


def qualify_phase(expected_relation_kind: str, *, label: str) -> None:
    if expected_relation_kind not in {"r", "v"}:
        raise AssertionError(f"unsupported Worker relation kind: {expected_relation_kind}")
    rest_url, service_credential, anonymous_credential, db_url = runtime()
    connection = audit.parse_loopback_connection(db_url)
    catalog = subprocess.run(
        connection.command("-XAt", "-v", "ON_ERROR_STOP=1"), cwd=ROOT,
        env=connection.environment(), input="""
          select string_agg(c.relkind, '' order by c.relname)
          from pg_class c join pg_namespace n on n.oid=c.relnamespace
          where n.nspname='public' and c.relname in
            ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts');
        """, text=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    ).stdout.strip()
    if catalog != expected_relation_kind * 4:
        raise AssertionError(f"{label}: unexpected public Worker catalog phase {catalog!r}")
    service_relations = WORKER_RELATIONS if expected_relation_kind == "r" else MINIMUM_SERVICE_RELATIONS

    # A migration can commit before the restarted PostgREST listener subscribes.
    # Reload only after the service is healthy, then poll
    # the actual role-filtered OpenAPI catalog instead of assuming delivery.
    reload_schema(db_url)
    for _ in range(40):
        openapi_status, openapi = request(f"{rest_url}/", service_credential, profile="public")
        paths = openapi.get("paths", {}) if openapi_status == 200 else {}
        if (all(f"/rpc/{name}" in paths for name in RPC_CASES)
                and all(f"/{name}" in paths for name in service_relations)):
            break
        time.sleep(0.25)
    else:
        raise AssertionError(f"{label}: PostgREST schema cache lacks Worker compatibility paths")

    for relation in service_relations:
        service_status, service_body = request(
            f"{rest_url}/{relation}?select=*&limit=1", service_credential,
            profile="public", openapi=False,
        )
        if service_status not in (200, 206) or not isinstance(service_body, list):
            raise AssertionError(f"{label}: service relation {relation}: {service_status} {service_body}")
    for relation in WORKER_RELATIONS:
        anon_status, anon_body = request(
            f"{rest_url}/{relation}?select=*&limit=1", anonymous_credential,
            profile="public", openapi=False,
        )
        if anon_status not in (401, 403, 404):
            raise AssertionError(f"{label}: anonymous relation {relation}: {anon_status} {anon_body}")

    for relation in set(WORKER_RELATIONS) - set(service_relations):
        service_status, service_body = request(
            f"{rest_url}/{relation}?select=*&limit=1", service_credential,
            profile="public", openapi=False,
        )
        if service_status not in (401, 403, 404):
            raise AssertionError(
                f"{label}: service unexpectedly reads internal-only {relation}: "
                f"{service_status} {service_body}"
            )

    for name, payload in RPC_CASES.items():
        service_status, service_body = request(
            f"{rest_url}/rpc/{name}", service_credential, payload=payload, profile="public",
        )
        if service_status != 200 or service_body != {"ok": True, "data": []}:
            raise AssertionError(
                f"{label}: service RPC {name}: {service_status} {service_body}"
            )
        anon_status, anon_body = request(
            f"{rest_url}/rpc/{name}", anonymous_credential, payload=payload, profile="public"
        )
        if (anon_status not in (401, 403, 404)
                or anon_body.get("code") not in ("42501", "PGRST202")):
            raise AssertionError(
                f"{label}: anonymous RPC {name}: {anon_status} {anon_body}"
            )

    private_status, private_body = request(
        f"{rest_url}/worker_jobs?select=id&limit=1", service_credential,
        profile="private", openapi=False,
    )
    if private_status != 406 or private_body.get("code") != "PGRST106":
        raise AssertionError(f"{label}: private profile was exposed: {private_status} {private_body}")


def main() -> int:
    _, _, _, db_url = runtime()
    connection = audit.parse_loopback_connection(db_url)
    result = subprocess.run(
        connection.command("-XAt", "-v", "ON_ERROR_STOP=1"), cwd=ROOT,
        env=connection.environment(), input="""
          select min(c.relkind)
          from pg_class c join pg_namespace n on n.oid=c.relnamespace
          where n.nspname='public' and c.relname in
            ('worker_job_kinds','worker_jobs','worker_job_events','worker_job_artifacts');
        """, text=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    qualify_phase(result.stdout.strip(), label="current")

    print("PASS PostgREST Worker relations/RPCs serve service_role, deny anon, and hide private")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
