#!/usr/bin/env python3
"""Loopback-only Data API and concurrency proof for Issue #390 facade v1."""

from __future__ import annotations

import base64
import concurrent.futures
import hashlib
import hmac
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from urllib.parse import urlparse

try:
    from .identity_collaboration_target import (
        VERIFIED_ENV,
        apply_target_environment,
        resolve_target,
    )
except ImportError:
    from identity_collaboration_target import (
        VERIFIED_ENV,
        apply_target_environment,
        resolve_target,
    )


ROOT = Path(__file__).resolve().parents[1]
RPC_NAMES = (
    "lca_read_job_projection_v1",
    "lca_read_result_projection_v1",
    "lca_read_latest_single_solve_result_v1",
    "lca_read_result_cache_v1",
    "cmd_lca_touch_result_cache_v1",
    "cmd_lca_admit_result_cache_v1",
    "cmd_lca_reconcile_result_cache_v1",
    "lca_read_latest_all_unit_result_v1",
)
INJECTED_TARGET_ENV = {
    "database_url": "DATABASE_URL",
    "rest_url": "SUPABASE_REST_URL",
    "anon_key": "SUPABASE_ANON_KEY",
    "service_role_key": "SUPABASE_SERVICE_ROLE_KEY",
    "jwt_secret": "SUPABASE_JWT_SECRET",
    "identity": VERIFIED_ENV,
}


def require_loopback(url: str) -> None:
    if urlparse(url).hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise RuntimeError(f"refusing non-loopback target: {urlparse(url).hostname}")


def runtime_context():
    injected = {
        field: os.environ.get(variable)
        for field, variable in INJECTED_TARGET_ENV.items()
    }
    verified = injected["identity"] is not None
    present = {field for field, value in injected.items() if value is not None}
    if verified and present != set(INJECTED_TARGET_ENV):
        missing = sorted(set(INJECTED_TARGET_ENV) - present)
        raise RuntimeError(f"partial canonical target environment; missing {missing}")

    if verified:
        require_loopback(str(injected["database_url"]))
        require_loopback(str(injected["rest_url"]))

    target = resolve_target()
    require_loopback(target.database_url)
    require_loopback(target.rest_url)
    if verified:
        mismatches = [
            field
            for field in INJECTED_TARGET_ENV
            if str(injected[field]).rstrip("/") != str(getattr(target, field)).rstrip("/")
        ]
        if mismatches:
            raise RuntimeError(
                "canonical target environment differs from selected Supabase stack: "
                + ", ".join(sorted(mismatches))
            )
        if os.environ.get("SUPABASE_WORKDIR") != target.workdir:
            raise RuntimeError(
                "canonical SUPABASE_WORKDIR differs from selected Supabase stack"
            )
    else:
        apply_target_environment(target)
    return target


def psql(db_url: str, sql: str) -> str:
    completed = subprocess.run(
        ["psql", db_url, "-X", "-q", "-v", "ON_ERROR_STOP=1", "-At"],
        cwd=ROOT,
        input=sql,
        capture_output=True,
        text=True,
    )
    if completed.returncode:
        raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
    return completed.stdout.strip()


def b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def authenticated_jwt(secret: str, subject: str) -> str:
    header = b64url(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    payload = b64url(
        json.dumps(
            {
                "aud": "authenticated",
                "exp": int(time.time()) + 3600,
                "role": "authenticated",
                "sub": subject,
            },
            separators=(",", ":"),
        ).encode()
    )
    signing_input = f"{header}.{payload}"
    signature = b64url(hmac.new(secret.encode(), signing_input.encode(), hashlib.sha256).digest())
    return f"{signing_input}.{signature}"


def rpc(
    rest_url: str,
    name: str,
    payload: dict[str, object],
    *,
    api_header: str,
    bearer: str,
    profile: str = "api",
) -> tuple[int, object]:
    request = urllib.request.Request(
        f"{rest_url}/rpc/{name}",
        data=json.dumps(payload, separators=(",", ":")).encode(),
        method="POST",
        headers={
            "Accept-Profile": profile,
            "Content-Profile": profile,
            "Content-Type": "application/json",
            "apikey": api_header,
            "Authorization": f"Bearer {bearer}",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = response.read().decode()
            return response.status, json.loads(body) if body else None
    except urllib.error.HTTPError as error:
        body = error.read().decode()
        try:
            parsed: object = json.loads(body) if body else None
        except json.JSONDecodeError:
            parsed = None
        return error.code, parsed


def expect_ok(result: tuple[int, object], name: str) -> object:
    status, body = result
    if not 200 <= status < 300:
        raise AssertionError(f"{name}: expected success, received HTTP {status}: {body}")
    return body


def require_object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise AssertionError(f"{name}: expected JSON object, received {value!r}")
    return value


def expect_keys(value: object, keys: set[str], name: str) -> dict[str, object]:
    item = require_object(value, name)
    if set(item) != keys:
        raise AssertionError(f"{name}: expected keys {sorted(keys)}, received {sorted(item)}")
    return item


def main() -> None:
    target = runtime_context()
    db_url = target.database_url
    rest_url = target.rest_url
    service_key = target.service_role_key
    anon_key = target.anon_key
    actor = str(uuid.uuid4())
    auth_token = authenticated_jwt(target.jwt_secret, actor)
    snapshot = str(uuid.uuid4())
    worker_job = str(uuid.uuid4())
    legacy_job = str(uuid.uuid4())
    result_id = str(uuid.uuid4())
    reconcile_worker_job = str(uuid.uuid4())
    reconcile_legacy_job = str(uuid.uuid4())
    reconcile_result_id = str(uuid.uuid4())
    ready_cache = str(uuid.uuid4())
    reconcile_cache = str(uuid.uuid4())
    latest_id = str(uuid.uuid4())
    namespace = uuid.uuid4().hex

    setup = f"""
      insert into public.lca_network_snapshots
        (id, scope, process_filter, provider_matching_rule, source_hash, status)
      values
        ('{snapshot}', 'full_library', '{{}}'::jsonb,
         'split_by_evidence_hybrid', 'issue-390-runtime-{namespace}', 'ready');
      insert into private.worker_jobs
        (id, job_kind, worker_queue, subject_type, subject_id, subject_version,
         requester_type, requested_by, status, payload_schema_version, payload_json,
         result_schema_version, result_json, finished_at)
      values
        ('{worker_job}', 'lca.solve_one', 'solver', 'lca_job', '{legacy_job}',
         '{snapshot}', 'user', '{actor}', 'completed', 'lca.solve_one.request.v1',
         jsonb_build_object('job_id', '{legacy_job}', 'snapshot_id', '{snapshot}'),
         'lca.solve.result.v1', '{{"ok":true}}'::jsonb, now());
      insert into private.worker_jobs
        (id, job_kind, worker_queue, subject_type, subject_id, subject_version,
         requester_type, requested_by, status, payload_schema_version, payload_json,
         result_schema_version, result_json, finished_at)
      values
        ('{reconcile_worker_job}', 'lca.solve_one', 'solver', 'lca_job',
         '{reconcile_legacy_job}', '{snapshot}', 'user', '{actor}', 'completed',
         'lca.solve_one.request.v1',
         jsonb_build_object('job_id', '{reconcile_legacy_job}', 'snapshot_id', '{snapshot}'),
         'lca.solve.result.v1', '{{"ok":true}}'::jsonb, now());
      insert into public.lca_results
        (id, job_id, worker_job_id, snapshot_id, payload, diagnostics,
         artifact_url, artifact_sha256, artifact_byte_size, artifact_format)
      values
        ('{result_id}', '{legacy_job}', '{worker_job}', '{snapshot}', '{{}}'::jsonb,
         '{{}}'::jsonb, 'storage://lca_results/{namespace}/result.h5',
         repeat('a', 64), 512, 'hdf5:v1');
      insert into public.lca_results
        (id, job_id, worker_job_id, snapshot_id, payload, diagnostics,
         artifact_url, artifact_sha256, artifact_byte_size, artifact_format)
      values
        ('{reconcile_result_id}', '{reconcile_legacy_job}', '{reconcile_worker_job}',
         '{snapshot}', '{{}}'::jsonb, '{{}}'::jsonb,
         'storage://lca_results/{namespace}/reconcile-result.h5',
         repeat('c', 64), 768, 'hdf5:v1');
      insert into public.lca_result_cache
        (id, scope, snapshot_id, request_key, request_payload, status,
         job_id, worker_job_id, result_id, hit_count)
      values
        ('{ready_cache}', 'prod', '{snapshot}', '{namespace}-ready',
         '{{"demand_mode":"single","demand":{{"process_index":1,"amount":1}}}}'::jsonb,
         'ready', '{legacy_job}', '{worker_job}', '{result_id}', 0),
        ('{reconcile_cache}', 'prod', '{snapshot}', '{namespace}-reconcile',
         '{{}}'::jsonb, 'pending', '{reconcile_legacy_job}',
         '{reconcile_worker_job}', null, 0);
      insert into public.lca_latest_all_unit_results
        (id, snapshot_id, job_id, worker_job_id, result_id, query_artifact_url,
         query_artifact_sha256, query_artifact_byte_size, query_artifact_format, status)
      values
        ('{latest_id}', '{snapshot}', '{legacy_job}', '{worker_job}', '{result_id}',
         'storage://lca_results/{namespace}/query.jsonl', repeat('b', 64), 1024,
         'jsonl:v1', 'ready');
    """
    cleanup = f"""
      delete from public.lca_result_cache where snapshot_id = '{snapshot}';
      delete from public.lca_latest_all_unit_results where snapshot_id = '{snapshot}';
      delete from public.lca_results where snapshot_id = '{snapshot}';
      delete from private.worker_jobs where id in ('{worker_job}', '{reconcile_worker_job}');
      delete from public.lca_network_snapshots where id = '{snapshot}';
    """

    args: dict[str, dict[str, object]] = {
        "lca_read_job_projection_v1": {
            "p_requested_by": actor,
            "p_worker_job_id": worker_job,
            "p_legacy_job_id": None,
            "p_include_internal": False,
        },
        "lca_read_result_projection_v1": {
            "p_requested_by": actor,
            "p_result_id": result_id,
            "p_required_artifact_format": "hdf5:v1",
            "p_include_internal": False,
        },
        "lca_read_latest_single_solve_result_v1": {
            "p_requested_by": actor,
            "p_snapshot_id": snapshot,
            "p_process_index": 1,
        },
        "lca_read_result_cache_v1": {
            "p_scope": "prod",
            "p_snapshot_id": snapshot,
            "p_request_key": f"{namespace}-ready",
        },
        "cmd_lca_touch_result_cache_v1": {"p_cache_id": ready_cache},
        "cmd_lca_admit_result_cache_v1": {
            "p_scope": "prod",
            "p_snapshot_id": snapshot,
            "p_request_key": f"{namespace}-admit",
            "p_request_payload": {"runtime": True},
            "p_legacy_job_id": str(uuid.uuid4()),
            "p_worker_job_id": None,
            "p_replace_ready": False,
        },
        "cmd_lca_reconcile_result_cache_v1": {
            "p_requested_by": actor,
            "p_cache_id": reconcile_cache,
        },
        "lca_read_latest_all_unit_result_v1": {"p_snapshot_id": snapshot},
    }

    try:
        psql(db_url, f"begin; {setup} commit;")
        service_results = {
            name: expect_ok(
                rpc(rest_url, name, args[name], api_header=service_key, bearer=service_key),
                name,
            )
            for name in RPC_NAMES
        }

        job = expect_keys(service_results["lca_read_job_projection_v1"], {"ok", "data"}, "job projection")
        job_data = require_object(job["data"], "job projection data")
        if job["ok"] is not True or require_object(job_data["job"], "job").get("workerJobId") != worker_job:
            raise AssertionError("job projection returned the wrong canonical worker")
        if require_object(job_data["result"], "job result").get("resultId") != result_id:
            raise AssertionError("job projection returned the wrong result")

        result = expect_keys(service_results["lca_read_result_projection_v1"], {"ok", "data"}, "result projection")
        result_data = require_object(result["data"], "result projection data")
        if result["ok"] is not True or require_object(result_data["result"], "projected result").get("resultId") != result_id:
            raise AssertionError("result projection returned the wrong result")

        latest_single = expect_keys(service_results["lca_read_latest_single_solve_result_v1"], {"ok", "data"}, "latest single")
        latest_single_data = require_object(latest_single["data"], "latest single data")
        if latest_single["ok"] is not True or latest_single_data.get("processIndex") != 1 or latest_single_data.get("amount") != 1:
            raise AssertionError("latest single projection returned the wrong demand")
        if require_object(latest_single_data["result"], "latest single result").get("resultId") != result_id:
            raise AssertionError("latest single projection returned the wrong result")

        cache = expect_keys(service_results["lca_read_result_cache_v1"], {"ok", "data"}, "cache read")
        cache_data = expect_keys(
            cache["data"],
            {"cacheId", "scope", "snapshotId", "requestKey", "status", "legacyJobId", "workerJobId", "resultId", "hitCount", "lastAccessedAt", "createdAt", "updatedAt"},
            "cache data",
        )
        if cache["ok"] is not True or cache_data.get("cacheId") != ready_cache or cache_data.get("resultId") != result_id or cache_data.get("hitCount") != 0:
            raise AssertionError("cache read returned the wrong canonical row")

        touch = expect_keys(service_results["cmd_lca_touch_result_cache_v1"], {"ok", "data"}, "cache touch")
        touch_data = expect_keys(
            touch["data"],
            {"cacheId", "status", "legacyJobId", "workerJobId", "resultId", "hitCount", "lastAccessedAt", "updatedAt"},
            "touch data",
        )
        if touch["ok"] is not True or touch_data.get("cacheId") != ready_cache or touch_data.get("hitCount") != 1:
            raise AssertionError("cache touch did not increment exactly once")

        admission = expect_keys(service_results["cmd_lca_admit_result_cache_v1"], {"ok", "outcome", "data"}, "cache admission")
        admission_data = expect_keys(
            admission["data"],
            {"cacheId", "scope", "snapshotId", "requestKey", "status", "legacyJobId", "workerJobId", "resultId", "hitCount", "lastAccessedAt", "createdAt", "updatedAt"},
            "admission data",
        )
        if admission["ok"] is not True or admission["outcome"] != "accepted" or admission_data.get("legacyJobId") != args["cmd_lca_admit_result_cache_v1"]["p_legacy_job_id"] or admission_data.get("hitCount") != 1:
            raise AssertionError("service admission did not return the accepted binding")

        reconcile = expect_keys(service_results["cmd_lca_reconcile_result_cache_v1"], {"ok", "code", "data"}, "cache reconcile")
        reconcile_data = require_object(reconcile["data"], "reconcile data")
        reconciled_cache = require_object(reconcile_data["cache"], "reconciled cache")
        if reconcile["ok"] is not True or reconcile["code"] != "reconciled" or reconciled_cache.get("resultId") != reconcile_result_id or reconciled_cache.get("hitCount") != 1:
            raise AssertionError("service reconcile did not bind and touch exactly once")

        latest_all = expect_keys(service_results["lca_read_latest_all_unit_result_v1"], {"ok", "data"}, "latest all-unit")
        latest_all_data = expect_keys(
            latest_all["data"],
            {"snapshotId", "resultId", "computedAt", "queryArtifactUrl", "queryArtifactFormat"},
            "latest all-unit data",
        )
        if latest_all["ok"] is not True or latest_all_data.get("snapshotId") != snapshot or latest_all_data.get("resultId") != result_id or latest_all_data.get("queryArtifactFormat") != "jsonl:v1":
            raise AssertionError("latest all-unit projection returned the wrong artifact")

        for role, key, bearer, expected_status in (
            ("anon", anon_key, anon_key, 401),
            ("authenticated", anon_key, auth_token, 403),
        ):
            for name in RPC_NAMES:
                status, denial = rpc(rest_url, name, args[name], api_header=key, bearer=bearer)
                denial_object = expect_keys(denial, {"code", "details", "hint", "message"}, f"{role}/{name} denial")
                if status != expected_status or denial_object.get("code") != "42501" or denial_object.get("message") != f"permission denied for function {name}":
                    raise AssertionError(f"{role}/{name}: unexpected denial HTTP {status}: {denial}")

        private_status, private_denial = rpc(
            rest_url,
            "lca_read_result_cache_v1",
            args["lca_read_result_cache_v1"],
            api_header=service_key,
            bearer=service_key,
            profile="private",
        )
        private_object = expect_keys(private_denial, {"code", "details", "hint", "message"}, "private profile denial")
        if private_status != 406 or private_object.get("code") != "PGRST106":
            raise AssertionError(f"private profile: expected 406/PGRST106, received {private_status}: {private_denial}")

        race_key = f"{namespace}-race"
        race_ids = [str(uuid.uuid4()) for _ in range(8)]

        def admit(legacy_id: str) -> object:
            return expect_ok(
                rpc(
                    rest_url,
                    "cmd_lca_admit_result_cache_v1",
                    {
                        "p_scope": "prod",
                        "p_snapshot_id": snapshot,
                        "p_request_key": race_key,
                        "p_request_payload": {"race": namespace},
                        "p_legacy_job_id": legacy_id,
                        "p_worker_job_id": None,
                        "p_replace_ready": False,
                    },
                    api_header=service_key,
                    bearer=service_key,
                ),
                "concurrent admission",
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            race_results = list(executor.map(admit, race_ids))
        outcomes = sorted(result.get("outcome") for result in race_results)
        if outcomes != ["accepted"] + ["reused"] * 7:
            raise AssertionError(f"unexpected different-binding race outcomes: {outcomes}")

        count, canonical_job, hit_count = psql(
            db_url,
            f"select count(*), min(job_id::text), min(hit_count) "
            f"from public.lca_result_cache where snapshot_id='{snapshot}' "
            f"and request_key='{race_key}';",
        ).split("|")
        if count != "1" or canonical_job not in race_ids or hit_count != "8":
            raise AssertionError(
                f"race canonical mismatch: count={count}, job={canonical_job}, hits={hit_count}"
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            replay_results = list(executor.map(admit, [canonical_job] * 8))
        if {result.get("outcome") for result in replay_results} != {"accepted"}:
            raise AssertionError("same-binding replay must be semantically accepted")
        final_hits = psql(
            db_url,
            f"select hit_count from public.lca_result_cache "
            f"where snapshot_id='{snapshot}' and request_key='{race_key}';",
        )
        if final_hits != "16":
            raise AssertionError(f"lost update in same-binding replay: hit_count={final_hits}")

        sql_race_key = f"{namespace}-sql-race"
        sql_race_ids = [str(uuid.uuid4()) for _ in range(8)]

        def admit_with_independent_backend(legacy_id: str) -> dict[str, object]:
            value = psql(
                db_url,
                f"""
                  begin;
                  set local role service_role;
                  select api.cmd_lca_admit_result_cache_v1(
                    'prod', '{snapshot}', '{sql_race_key}',
                    '{{"race":"{namespace}"}}'::jsonb,
                    '{legacy_id}', null, false
                  )::text;
                  commit;
                """,
            )
            return require_object(json.loads(value), "independent backend admission")

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            sql_race_results = list(executor.map(admit_with_independent_backend, sql_race_ids))
        sql_outcomes = sorted(result.get("outcome") for result in sql_race_results)
        if sql_outcomes != ["accepted"] + ["reused"] * 7:
            raise AssertionError(f"unexpected independent-backend outcomes: {sql_outcomes}")
        sql_count, sql_canonical_job, sql_hits = psql(
            db_url,
            f"select count(*), min(job_id::text), min(hit_count) "
            f"from public.lca_result_cache where snapshot_id='{snapshot}' "
            f"and request_key='{sql_race_key}';",
        ).split("|")
        if sql_count != "1" or sql_canonical_job not in sql_race_ids or sql_hits != "8":
            raise AssertionError(
                f"independent backend race mismatch: count={sql_count}, "
                f"job={sql_canonical_job}, hits={sql_hits}"
            )

        print(
            json.dumps(
                {
                    "dataApiServiceRpcCount": 8,
                    "negativeRoleRpcCount": 16,
                    "privateProfileStatus": 406,
                    "concurrencySessions": 8,
                    "independentDatabaseSessions": 8,
                    "independentDatabaseOutcomes": {"accepted": 1, "reused": 7},
                    "differentBindingOutcomes": {"accepted": 1, "reused": 7},
                    "sameBindingAccepted": 8,
                    "finalHitCount": 16,
                },
                sort_keys=True,
            )
        )
    finally:
        psql(db_url, f"begin; {cleanup} commit;")
        residue = psql(
            db_url,
            f"""
              select
                (select count(*) from public.lca_network_snapshots where id = '{snapshot}'),
                (select count(*) from private.worker_jobs where id in ('{worker_job}', '{reconcile_worker_job}')),
                (select count(*) from public.lca_results where snapshot_id = '{snapshot}'),
                (select count(*) from public.lca_result_cache where snapshot_id = '{snapshot}'),
                (select count(*) from public.lca_latest_all_unit_results where snapshot_id = '{snapshot}'),
                (select count(*) from public.lca_network_snapshots where source_hash like 'issue-390-runtime-%');
            """,
        )
        if residue != "0|0|0|0|0|0":
            raise AssertionError(f"runtime cleanup left fixture residue: {residue}")


if __name__ == "__main__":
    main()
