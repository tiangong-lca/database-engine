#!/usr/bin/env python3
"""Destructive Issue #398 role/runtime and two-session concurrency test."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import threading
import time
import uuid
from urllib.parse import urlsplit, urlunsplit

import psycopg
from psycopg import sql


def role_url(db_url: str, role: str, password: str) -> str:
    parsed = urlsplit(db_url)
    host = parsed.hostname or "127.0.0.1"
    if parsed.port:
        host = f"{host}:{parsed.port}"
    return urlunsplit((parsed.scheme, f"{role}:{password}@{host}", parsed.path, "", ""))


def assert_loopback(db_url: str) -> None:
    parsed = urlsplit(db_url)
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise SystemExit("--db-url must target an isolated loopback PostgreSQL instance")


def call(conn: psycopg.Connection, query: str, params: tuple = ()) -> dict:
    with conn.cursor() as cursor:
        cursor.execute(query, params)
        value = cursor.fetchone()[0]
    return value if isinstance(value, dict) else json.loads(value)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-url", required=True)
    parser.add_argument(
        "--confirm-isolated-destructive-test", action="store_true", required=True
    )
    args = parser.parse_args()
    assert_loopback(args.db_url)

    suffix = uuid.uuid4().hex[:10]
    runtime_role = f"issue398_runtime_{suffix}"
    denied_role = f"issue398_denied_{suffix}"
    password = f"issue398-{suffix}"
    snapshot_id = uuid.uuid4()
    worker_job_id = uuid.uuid4()
    requested_by = uuid.uuid4()
    result_ids = [uuid.uuid4() for _ in range(7)]

    admin = psycopg.connect(args.db_url, autocommit=True)
    runtime = None
    denied = None
    try:
        with admin.cursor() as cursor:
            cursor.execute(
                sql.SQL("create role {} login password {}").format(
                    sql.Identifier(runtime_role), sql.Literal(password)
                )
            )
            cursor.execute(
                sql.SQL("grant lca_worker_runtime to {}").format(
                    sql.Identifier(runtime_role)
                )
            )
            cursor.execute(
                sql.SQL("create role {} login password {}").format(
                    sql.Identifier(denied_role), sql.Literal(password)
                )
            )
            cursor.execute(
                sql.SQL("grant usage on schema private to {}").format(
                    sql.Identifier(denied_role)
                )
            )
            cursor.execute(
                "insert into private.lca_network_snapshots (id,status) values (%s,'ready')",
                (snapshot_id,),
            )
            cursor.execute(
                """insert into private.worker_jobs (
                       id,job_kind,worker_queue,requester_type,requested_by,
                       request_hash,status,payload_schema_version
                     ) values (%s,'lca.result_gc','maintenance','user',%s,
                       %s,'completed','v1')""",
                (worker_job_id, requested_by, f"issue-398-{suffix}"),
            )
            for ordinal, result_id in enumerate(result_ids):
                cursor.execute(
                    """insert into public.lca_results (
                           id,job_id,snapshot_id,worker_job_id,artifact_url,
                           artifact_sha256,artifact_byte_size,artifact_format,
                           created_at,expires_at
                         ) values (%s,%s,%s,%s,%s,%s,%s,'json',
                           now()-(%s * interval '1 hour'),now()-interval '1 day')""",
                    (
                        result_id,
                        uuid.uuid4(),
                        snapshot_id,
                        worker_job_id,
                        f"s3://issue-398/results/{result_id}/result.json",
                        f"{ordinal + 1:x}" * 64,
                        ordinal + 1,
                        10 - ordinal,
                    ),
                )
            cursor.execute("grant lca_result_gc_executor to postgres")
            cursor.execute("set role lca_result_gc_executor")
            cursor.execute(
                """update private.lca_result_gc_control
                   set claims_enabled=true,enabled_at=now(),enabled_by=%s,
                       reason='isolated runtime concurrency test'""",
                (runtime_role,),
            )
            cursor.execute("reset role")

        runtime = psycopg.connect(role_url(args.db_url, runtime_role, password))
        denied = psycopg.connect(role_url(args.db_url, denied_role, password))
        for result_id in result_ids:
            response = call(
                runtime,
                "select private.worker_lca_result_gc_attest_v1(%s)",
                (result_id,),
            )
            assert response["ok"] and response["outcome"] == "attested", response
        runtime.commit()

        with denied.cursor() as cursor:
            cursor.execute(
                """select count(*)
                   from pg_proc procedure
                   join pg_namespace namespace
                     on namespace.oid=procedure.pronamespace
                   where namespace.nspname='private'
                     and procedure.proname like 'worker_lca_result_gc_%_v1'
                     and has_function_privilege(current_user,procedure.oid,'EXECUTE')"""
            )
            assert cursor.fetchone()[0] == 0

        try:
            call(denied, "select private.worker_lca_result_gc_preview_v1(1)")
        except psycopg.errors.InsufficientPrivilege:
            denied.rollback()
        else:
            raise AssertionError("a real non-runtime login executed the GC contract")

        with admin.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    "grant execute on function "
                    "private.worker_lca_result_gc_preview_v1(integer) to {}"
                ).format(sql.Identifier(denied_role))
            )
        guarded = call(denied, "select private.worker_lca_result_gc_preview_v1(1)")
        denied.commit()
        assert guarded.get("code") == "result_gc_worker_role_required", guarded
        with admin.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    "revoke execute on function "
                    "private.worker_lca_result_gc_preview_v1(integer) from {}"
                ).format(sql.Identifier(denied_role))
            )

        claim = call(
            runtime,
            "select private.worker_lca_result_gc_claim_v1(%s,10,300)",
            (runtime_role,),
        )
        runtime.commit()
        items = claim["data"]["items"]
        assert len(items) == 6, claim

        renewed = call(
            runtime,
            "select private.worker_lca_result_gc_renew_v1(%s,%s,300)",
            (items[0]["operationId"], items[0]["claimToken"]),
        )
        runtime.commit()
        assert renewed["ok"] and renewed["data"]["state"] == "claimed", renewed

        def fence(item: dict, application_name: str | None = None) -> dict:
            connect_kwargs = (
                {"application_name": application_name} if application_name else {}
            )
            with psycopg.connect(
                role_url(args.db_url, runtime_role, password), **connect_kwargs
            ) as conn:
                return call(
                    conn,
                    "select private.worker_lca_result_gc_fence_v1(%s,%s)",
                    (item["operationId"], item["claimToken"]),
                )

        def wait_until_lock_wait(application_name: str) -> None:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                with admin.cursor() as cursor:
                    cursor.execute(
                        """select wait_event_type
                           from pg_stat_activity
                           where application_name=%s and state='active'""",
                        (application_name,),
                    )
                    row = cursor.fetchone()
                if row is not None and row[0] == "Lock":
                    return
                time.sleep(0.01)
            raise AssertionError(
                f"session {application_name} did not reach an observed lock wait"
            )

        assert (
            items[0]["retentionPartitionKey"]
            == items[1]["retentionPartitionKey"]
        )
        double_fence_barrier = threading.Barrier(2)
        double_fence_names = [
            f"issue398-double-fence-a-{suffix}",
            f"issue398-double-fence-b-{suffix}",
        ]

        def barrier_fence(pair: tuple[dict, str]) -> dict:
            item, application_name = pair
            double_fence_barrier.wait(timeout=5)
            return fence(item, application_name)

        partition_blocker = psycopg.connect(args.db_url)
        try:
            with partition_blocker.cursor() as cursor:
                cursor.execute(
                    "select pg_catalog.pg_advisory_xact_lock("
                    "pg_catalog.hashtextextended(%s,398))",
                    (items[0]["retentionPartitionKey"],),
                )
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                futures = [
                    pool.submit(barrier_fence, pair)
                    for pair in zip(items[:2], double_fence_names, strict=True)
                ]
                for application_name in double_fence_names:
                    wait_until_lock_wait(application_name)
                partition_blocker.commit()
                responses = [future.result(timeout=5) for future in futures]
        finally:
            partition_blocker.close()
        assert all(r.get("outcome") == "delete_ready" for r in responses), responses

        original_delete_token = items[0]["claimToken"]
        with admin.cursor() as cursor:
            cursor.execute("set role lca_result_gc_executor")
            cursor.execute(
                """update private.lca_result_gc_operations
                   set lease_expires_at=clock_timestamp()-interval '1 second'
                   where operation_id=%s""",
                (items[0]["operationId"],),
            )
            cursor.execute("reset role")
        recovery = call(
            runtime,
            "select private.worker_lca_result_gc_claim_v1(%s,10,300)",
            (runtime_role,),
        )
        runtime.commit()
        recovery_item = recovery["data"]["items"][0]
        assert recovery_item["phase"] == "delete_recovery", recovery
        assert recovery_item["generation"] == 2, recovery
        stale_finalize = call(
            runtime,
            "select private.worker_lca_result_gc_finalize_v1(%s,%s,'missing')",
            (items[0]["operationId"], original_delete_token),
        )
        runtime.commit()
        assert stale_finalize.get("code") == "result_gc_claim_invalid", stale_finalize
        items[0] = recovery_item
        missing_finalize = call(
            runtime,
            "select private.worker_lca_result_gc_finalize_v1(%s,%s,'missing')",
            (items[0]["operationId"], items[0]["claimToken"]),
        )
        runtime.commit()
        assert missing_finalize.get("outcome") == "finalized", missing_finalize
        missing_replay = call(
            runtime,
            "select private.worker_lca_result_gc_finalize_v1(%s,%s,'missing')",
            (items[0]["operationId"], items[0]["claimToken"]),
        )
        runtime.commit()
        assert missing_replay.get("outcome") == "replayed", missing_replay
        deleted_finalize = call(
            runtime,
            "select private.worker_lca_result_gc_finalize_v1(%s,%s,'deleted')",
            (items[1]["operationId"], items[1]["claimToken"]),
        )
        runtime.commit()
        assert deleted_finalize.get("outcome") == "finalized", deleted_finalize

        lock_conn = psycopg.connect(args.db_url)
        update_conn = psycopg.connect(args.db_url)
        try:
            with lock_conn.cursor() as cursor:
                cursor.execute("begin")
                cursor.execute(
                    "select 1 from private.lca_result_gc_operations where operation_id=%s for update",
                    (items[2]["operationId"],),
                )
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                update_race_name = f"issue398-update-race-{suffix}"
                future = pool.submit(fence, items[2], update_race_name)
                wait_until_lock_wait(update_race_name)
                try:
                    with update_conn.cursor() as cursor:
                        cursor.execute(
                            "update public.lca_results set payload='{}'::jsonb where id=%s",
                            (items[2]["resultId"],),
                        )
                except psycopg.errors.ObjectNotInPrerequisiteState as error:
                    assert "lca_result_gc_fence_blocks_concurrent_write" in str(error)
                    update_conn.rollback()
                else:
                    raise AssertionError("concurrent update did not fail closed")
                lock_conn.commit()
                response = future.result(timeout=5)
            assert response.get("outcome") == "delete_ready", response
            timeout_failure = call(
                runtime,
                "select private.worker_lca_result_gc_fail_v1(%s,%s,%s)",
                (
                    items[2]["operationId"],
                    items[2]["claimToken"],
                    "object_delete_timeout",
                ),
            )
            runtime.commit()
            assert timeout_failure.get("outcome") == "retry_required", timeout_failure
            timeout_finalize = call(
                runtime,
                "select private.worker_lca_result_gc_finalize_v1(%s,%s,'deleted')",
                (items[2]["operationId"], items[2]["claimToken"]),
            )
            runtime.commit()
            assert timeout_finalize.get("outcome") == "finalized", timeout_finalize
        finally:
            lock_conn.close()
            update_conn.close()

        reference_conn = psycopg.connect(args.db_url)
        try:
            with reference_conn.cursor() as cursor:
                cursor.execute("begin")
                cursor.execute(
                    """insert into public.lca_result_cache (
                           snapshot_id,request_key,request_payload,status,result_id
                         ) values (%s,%s,'{}'::jsonb,'ready',%s)""",
                    (snapshot_id, f"issue-398-{suffix}", items[3]["resultId"]),
                )
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                reference_race_name = f"issue398-reference-race-{suffix}"
                future = pool.submit(fence, items[3], reference_race_name)
                wait_until_lock_wait(reference_race_name)
                reference_conn.commit()
                response = future.result(timeout=5)
            assert response.get("outcome") == "ineligible", response
            assert response["data"]["reason"] == "active_cache_reference", response
        finally:
            reference_conn.close()

        fence_first = fence(items[5])
        assert fence_first.get("outcome") == "delete_ready", fence_first
        try:
            with admin.cursor() as cursor:
                cursor.execute(
                    """insert into public.lca_result_cache (
                           snapshot_id,request_key,request_payload,status,result_id
                         ) values (%s,%s,'{}'::jsonb,'ready',%s)""",
                    (
                        snapshot_id,
                        f"issue-398-fence-first-{suffix}",
                        items[5]["resultId"],
                    ),
                )
        except psycopg.errors.ObjectNotInPrerequisiteState as error:
            assert "lca_result_gc_delete_fence_blocks_reference" in str(error)
        else:
            raise AssertionError("a new reference was admitted after the GC fence")
        fence_first_finalize = call(
            runtime,
            "select private.worker_lca_result_gc_finalize_v1(%s,%s,'missing')",
            (items[5]["operationId"], items[5]["claimToken"]),
        )
        runtime.commit()
        assert fence_first_finalize.get("outcome") == "finalized", fence_first_finalize

        claimed_failure = call(
            runtime,
            "select private.worker_lca_result_gc_fail_v1(%s,%s,%s)",
            (
                items[4]["operationId"],
                items[4]["claimToken"],
                "pre_delete_validation_failed",
            ),
        )
        runtime.commit()
        assert claimed_failure.get("outcome") == "released", claimed_failure

        print(
            "PASS: real runtime ACL, renew/fail/recovery/finalize states, "
            "barriered double-fence ordering, observed lock waits, and "
            "bidirectional reference serialization"
        )
    finally:
        if runtime is not None:
            runtime.close()
        if denied is not None:
            denied.close()
        with admin.cursor() as cursor:
            cursor.execute("grant lca_result_gc_executor to postgres")
            cursor.execute("set role lca_result_gc_executor")
            cursor.execute(
                "delete from private.lca_result_gc_finalize_context "
                "where operation_id in (select operation_id from "
                "private.lca_result_gc_operations where target_result_id=any(%s))",
                (result_ids,),
            )
            cursor.execute(
                "delete from private.lca_result_gc_operations "
                "where target_result_id=any(%s)",
                (result_ids,),
            )
            cursor.execute(
                """update private.lca_result_gc_control
                   set claims_enabled=false,enabled_at=null,enabled_by=null,
                       reason='issue_398_disabled_by_default',updated_at=now()"""
            )
            cursor.execute("reset role")
            cursor.execute(
                "delete from public.lca_result_cache where request_key=%s",
                (f"issue-398-{suffix}",),
            )
            cursor.execute(
                "alter table public.lca_results disable trigger "
                "lca_results_gc_write_fence"
            )
            try:
                cursor.execute(
                    "delete from public.lca_results where id=any(%s)",
                    (result_ids,),
                )
            finally:
                cursor.execute(
                    "alter table public.lca_results enable trigger "
                    "lca_results_gc_write_fence"
                )
            cursor.execute(
                "delete from private.worker_jobs where id=%s", (worker_job_id,)
            )
            cursor.execute(
                "delete from private.lca_network_snapshots where id=%s",
                (snapshot_id,),
            )
            cursor.execute("revoke lca_result_gc_executor from postgres")
            cursor.execute(
                sql.SQL("revoke usage on schema private from {}").format(
                    sql.Identifier(denied_role)
                )
            )
            cursor.execute(
                sql.SQL("drop role if exists {}").format(sql.Identifier(runtime_role))
            )
            cursor.execute(
                sql.SQL("drop role if exists {}").format(sql.Identifier(denied_role))
            )
            cursor.execute(
                """select
                     (select count(*) from public.lca_results where id=any(%s))
                   + (select count(*) from private.worker_jobs where id=%s)
                   + (select count(*) from private.lca_network_snapshots where id=%s)
                   + (select count(*) from private.lca_result_gc_operations
                      where target_result_id=any(%s))
                   + (select count(*) from public.lca_result_cache
                      where request_key=%s)
                   + (select count(*) from pg_roles where rolname=any(%s))
                   + (select count(*) from private.lca_result_gc_control
                      where claims_enabled)""",
                (
                    result_ids,
                    worker_job_id,
                    snapshot_id,
                    result_ids,
                    f"issue-398-{suffix}",
                    [runtime_role, denied_role],
                ),
            )
            residue = cursor.fetchone()[0]
            if residue != 0:
                raise AssertionError(f"Issue #398 test left {residue} database rows")
            cursor.execute(
                """select
                     count(*) filter (where
                       member = 'postgres'::regrole
                       and roleid in (
                         'lca_worker_runtime'::regrole,
                         'lca_result_gc_executor'::regrole
                       )
                       and grantor = 'supabase_admin'::regrole
                       and admin_option
                       and not inherit_option
                       and not set_option
                     ),
                     count(*) filter (where not (
                       member = 'postgres'::regrole
                       and roleid in (
                         'lca_worker_runtime'::regrole,
                         'lca_result_gc_executor'::regrole
                       )
                       and grantor = 'supabase_admin'::regrole
                       and admin_option
                       and not inherit_option
                       and not set_option
                     ))
                   from pg_auth_members
                   where member in (
                       'lca_worker_runtime'::regrole,
                       'lca_result_gc_executor'::regrole
                     )
                      or roleid in (
                        'lca_worker_runtime'::regrole,
                        'lca_result_gc_executor'::regrole
                      )"""
            )
            creator_edges, unsafe_edges = cursor.fetchone()
            if creator_edges != 2 or unsafe_edges != 0:
                raise AssertionError(
                    "Issue #398 did not restore the exact PG17 creator-edge baseline: "
                    f"creator_edges={creator_edges}, unsafe_edges={unsafe_edges}"
                )
        admin.close()
        print(
            "PASS: exact fixture cleanup, zero residue, and exact creator-edge baseline"
        )


if __name__ == "__main__":
    main()
