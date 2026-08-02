#!/usr/bin/env python3
"""Fail-closed hosted qualification for the persistent Dev LCA snapshot family.

This runner is intentionally specific to database-engine Issue #380.  It never
loads consumer code and has no repository, ref, script, project, or endpoint
selector supplied by a caller.
"""

from __future__ import annotations

import json
import hashlib
import os
import secrets
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any, Callable


REPOSITORY = "tiangong-lca/database-engine"
DEV_REF = "fotofiyqnuyvgtotswie"
PRODUCTION_REF = "qgzvkongdjqiiamzbbts"
DATABASE_CONTRACT_SHA = "86ba7ee2c33e45df8008117a2dec3ee4deedc32c"
MIGRATION_HEAD = "20260802091342"
EDGE_SOURCE_HEAD = "6080b4c2c95b00c2666f98af6bb90610042fc3da"
EDGE_DEPLOYMENT_RECEIPT = {
    "lca_solve": ("878d7c40-f675-4c87-9687-2abd2ea71c2c", 22, 1785671706196, "56fc024e624450c054158cdf37a0c0a9329cec6b9fb5116210e123a70f6fac67"),
    "lca_query_results": ("3683b3d0-6242-43c0-9976-9817503ce4a2", 22, 1785671715093, "938590e9b5ea0f2cf60c9bf08223d31781bccb3f7625a7373af5c2f1fcc0651f"),
    "lca_contribution_path": ("48c78d31-8037-4901-bd86-d6fd5fc159f9", 21, 1785671723627, "d33fd33bdfa182a0143d731ef518ebf30e9a6d093a03f8484ec7a494d2fde4a6"),
    "app_data_product_commands": ("03f88df7-2cfd-48f5-80db-b7b7976e9a49", 17, 1785671732171, "8d30ff95aa2ba1f057fb490b5b7dfc1d211920a866c2da0e3c8378dc4090d22d"),
    "data_product_results": ("9c00a8d9-68d5-49c6-84eb-9044970d728b", 12, 1785671740181, "24eba3a6b60a62cb6e3b2299b57f361dd03e07d85261b5bfd5b5c1f744bc7525"),
}
RPC_CASES = (
    ("lca_snapshot_active_read_v1", {"p_scope": "issue380-hosted"}),
    ("lca_snapshot_scope_read_v1", None),
    ("lca_snapshot_resolve_v1", {"p_scope": "prod", "p_process_filter": {"issue380": True}}),
    ("lca_snapshot_artifact_read_v1", None),
    ("lca_snapshot_artifact_latest_v1", {}),
    ("cmd_lca_snapshot_create_v1", None),
)


class QualificationError(RuntimeError):
    pass


@dataclass(frozen=True)
class Config:
    repository: str
    git_ref: str
    project_ref: str
    access_token: str
    publishable_key: str | None
    secret_key: str | None

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            repository=os.environ.get("GITHUB_REPOSITORY", ""),
            git_ref=os.environ.get("GITHUB_REF", ""),
            project_ref=os.environ.get("SUPABASE_PROJECT_ID", ""),
            access_token=os.environ.get("SUPABASE_ACCESS_TOKEN", ""),
            publishable_key=os.environ.get("SUPABASE_DEV_PUBLISHABLE_KEY") or None,
            secret_key=os.environ.get("SUPABASE_DEV_SECRET_KEY") or None,
        )

    def validate(self) -> None:
        expected = {
            "repository": (self.repository, REPOSITORY),
            "git ref": (self.git_ref, "refs/heads/dev"),
            "project ref": (self.project_ref, DEV_REF),
        }
        for label, (actual, wanted) in expected.items():
            if actual != wanted:
                raise QualificationError(f"refusing unexpected {label}")
        if self.project_ref == PRODUCTION_REF:
            raise QualificationError("production project is forbidden")
        if not self.access_token:
            raise QualificationError("SUPABASE_ACCESS_TOKEN is required")


@dataclass(frozen=True)
class RunNamespace:
    marker: str
    fixture_id: uuid.UUID
    create_id: uuid.UUID
    email: str
    bucket: str
    prefix: str

    @classmethod
    def create(cls) -> "RunNamespace":
        root = uuid.uuid4()
        marker = root.hex
        return cls(
            marker=marker,
            fixture_id=uuid.uuid5(root, "fixture"),
            create_id=uuid.uuid5(root, "create"),
            email=f"issue380-{root}@example.invalid",
            bucket=f"issue380-{marker}",
            prefix=f"runs/{marker}",
        )


def mask(value: str) -> None:
    if value:
        print(f"::add-mask::{value}")


def _json_request(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: Any | None = None,
    timeout: int = 45,
) -> tuple[int, Any]:
    encoded = None if body is None else json.dumps(body, separators=(",", ":")).encode()
    request_headers = {"Accept": "application/json", "User-Agent": "database-engine-issue-380"}
    request_headers.update(headers or {})
    if encoded is not None:
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=encoded, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read()
            if not payload:
                return response.status, None
            try:
                parsed = json.loads(payload)
            except json.JSONDecodeError:
                parsed = payload.decode("utf-8", errors="replace")
            return response.status, parsed
    except urllib.error.HTTPError as error:
        payload = error.read()
        try:
            parsed = json.loads(payload) if payload else None
        except json.JSONDecodeError:
            parsed = None
        return error.code, parsed


def select_current_keys(rows: Any) -> tuple[str, str]:
    if not isinstance(rows, list):
        raise QualificationError("Management API key response is not a list")

    def choose(kind: str, prefix: str) -> str:
        candidates: list[tuple[bool, str]] = []
        for row in rows:
            if not isinstance(row, dict) or str(row.get("type", "")).lower() != kind:
                continue
            if row.get("disabled") is True or str(row.get("status", "active")).lower() != "active":
                continue
            value = row.get("api_key") or row.get("key") or row.get("value")
            if isinstance(value, str) and value.startswith(prefix):
                candidates.append((str(row.get("name", "")) == "default", value))
        candidates.sort(key=lambda candidate: candidate[0], reverse=True)
        if not candidates:
            raise QualificationError(f"no revealed active {kind} key")
        return candidates[0][1]

    return choose("publishable", "sb_publishable_"), choose("secret", "sb_secret_")


def resolve_keys(config: Config, requester: Callable[..., tuple[int, Any]] = _json_request) -> tuple[str, str]:
    if bool(config.publishable_key) != bool(config.secret_key):
        raise QualificationError("both project-specific key secrets must be configured together")
    status, payload = requester(
        f"https://api.supabase.com/v1/projects/{DEV_REF}/api-keys?reveal=true",
        headers={"Authorization": f"Bearer {config.access_token}"},
    )
    try:
        keys = select_current_keys(payload) if status == 200 else None
    except QualificationError:
        keys = None
    if keys is None:
        if not config.publishable_key or not config.secret_key:
            raise QualificationError(
                "Management token cannot reveal current keys; configure repository secrets "
                "SUPABASE_DEV_PUBLISHABLE_KEY and SUPABASE_DEV_SECRET_KEY"
            )
        keys = (config.publishable_key, config.secret_key)
    if not keys[0].startswith("sb_publishable_") or not keys[1].startswith("sb_secret_"):
        raise QualificationError("refusing legacy or wrong-role API key")
    mask(keys[0])
    mask(keys[1])
    return keys


class HostedClient:
    def __init__(self, config: Config, publishable_key: str, secret_key: str) -> None:
        self.config = config
        self.publishable_key = publishable_key
        self.secret_key = secret_key
        self.base_url = f"https://{DEV_REF}.supabase.co"

    def management(self, path: str, *, method: str = "GET", body: Any | None = None) -> tuple[int, Any]:
        return _json_request(
            f"https://api.supabase.com{path}",
            method=method,
            headers={"Authorization": f"Bearer {self.config.access_token}"},
            body=body,
        )

    def sql(self, query: str) -> Any:
        status, payload = self.management(
            f"/v1/projects/{DEV_REF}/database/query", method="POST", body={"query": query}
        )
        if status != 201 and status != 200:
            raise QualificationError(f"hosted SQL query failed with HTTP {status}")
        return payload

    def rest(
        self,
        routine: str,
        args: dict[str, Any],
        *,
        key: str,
        bearer: str | None = None,
    ) -> tuple[int, Any]:
        headers = {"apikey": key, "Content-Profile": "api"}
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        return _json_request(
            f"{self.base_url}/rest/v1/rpc/{routine}", method="POST", headers=headers, body=args
        )

    def auth(self, path: str, *, method: str, key: str, body: Any | None = None, bearer: str | None = None) -> tuple[int, Any]:
        headers = {"apikey": key}
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        return _json_request(f"{self.base_url}/auth/v1/{path}", method=method, headers=headers, body=body)

    def edge(self, name: str, *, method: str, body: Any | None, bearer: str | None) -> tuple[int, Any]:
        headers = {"apikey": self.publishable_key}
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        return _json_request(
            f"{self.base_url}/functions/v1/{name}", method=method, headers=headers, body=body
        )

    def storage_json(self, path: str, *, method: str, body: Any | None = None) -> tuple[int, Any]:
        return _json_request(
            f"{self.base_url}/storage/v1/{path}",
            method=method,
            headers={"apikey": self.secret_key, "Authorization": f"Bearer {self.secret_key}"},
            body=body,
        )

    def upload_json(self, bucket: str, path: str, payload: Any) -> None:
        data = json.dumps(payload, separators=(",", ":")).encode()
        request = urllib.request.Request(
            f"{self.base_url}/storage/v1/object/{bucket}/{path}",
            data=data,
            method="POST",
            headers={
                "apikey": self.secret_key,
                "Authorization": f"Bearer {self.secret_key}",
                "Content-Type": "application/json",
                "x-upsert": "false",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                if response.status not in (200, 201):
                    raise QualificationError("storage upload failed")
        except urllib.error.HTTPError as error:
            raise QualificationError(f"storage upload failed with HTTP {error.code}") from None


def require_response(actual: tuple[int, Any], status: int, payload: Any | None = None) -> Any:
    actual_status, actual_payload = actual
    if actual_status != status:
        raise QualificationError(f"unexpected hosted response status {actual_status}; wanted {status}")
    if payload is not None and actual_payload != payload:
        raise QualificationError("hosted response DTO mismatch")
    return actual_payload


def require_anonymous_unauthorized(actual: tuple[int, Any]) -> dict[str, Any]:
    status, payload = actual
    accepted = (
        {"error": "unauthorized"},
        {"code": 401, "message": "Missing authorization header"},
    )
    if status != 401:
        raise QualificationError("anonymous Edge response status mismatch")
    if not isinstance(payload, dict) or payload not in accepted:
        raise QualificationError("anonymous Edge response DTO mismatch")
    return payload


def _single_row(payload: Any) -> dict[str, Any]:
    if isinstance(payload, list) and len(payload) == 1 and isinstance(payload[0], dict):
        return payload[0]
    if isinstance(payload, dict):
        return payload
    raise QualificationError("unexpected hosted query response shape")


def canonical_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _rows(payload: Any, label: str) -> list[dict[str, Any]]:
    if not isinstance(payload, list) or any(not isinstance(row, dict) for row in payload):
        raise QualificationError(f"{label} response is not a row list")
    return payload


def _allow_status(actual: tuple[int, Any], allowed: tuple[int, ...], label: str) -> Any:
    status, payload = actual
    if status not in allowed:
        raise QualificationError(f"{label} failed with HTTP {status}")
    return payload


def reconcile_namespace(client: HostedClient, run: RunNamespace) -> None:
    """Discover and remove all hosted state belonging to one run namespace."""
    errors: list[BaseException] = []

    def attempt(label: str, action: Callable[[], Any]) -> Any:
        try:
            return action()
        except BaseException as error:
            errors.append(QualificationError(f"namespace reconcile failed: {label}: {type(error).__name__}"))
            return None

    actor_rows = attempt(
        "discover-auth-actors",
        lambda: _rows(
            client.sql(
                "select id::text from auth.users "
                f"where email={sql_literal(run.email)} "
                f"and raw_user_meta_data->>'issue380_marker'={sql_literal(run.marker)} order by id"
            ),
            "Auth actor discovery",
        ),
    )
    actor_ids = [str(row["id"]) for row in (actor_rows or []) if row.get("id")]
    actor_filter = (
        " or requested_by in (" + ",".join(sql_literal(value) + "::uuid" for value in actor_ids) + ")"
        if actor_ids
        else ""
    )
    snapshot_values = f"{sql_literal(str(run.fixture_id))},{sql_literal(str(run.create_id))}"
    worker_predicate = (
        f"subject_version in ({snapshot_values},{sql_literal(run.marker)}) "
        f"or payload_json->>'snapshot_id' in ({snapshot_values}){actor_filter}"
    )
    worker_rows = attempt(
        "discover-worker-jobs",
        lambda: _rows(
            client.sql(f"select id::text from private.worker_jobs where {worker_predicate} order by id"),
            "Worker discovery",
        ),
    )
    worker_ids = [str(row["id"]) for row in (worker_rows or []) if row.get("id")]
    if worker_ids:
        ids = ",".join(sql_literal(value) + "::uuid" for value in worker_ids)
        attempt(
            "cancel-worker-jobs",
            lambda: client.sql(
                f"update private.worker_jobs set status='cancelled', finished_at=coalesce(finished_at,now()), "
                f"cancelled_at=coalesce(cancelled_at,now()), leased_by=null, lease_token=null, lease_expires_at=null, "
                f"updated_at=now() where id in ({ids}) and status not in ('completed','failed','cancelled')"
            ),
        )

    fixture = sql_literal(str(run.fixture_id))
    create = sql_literal(str(run.create_id))
    for label, query in (
        ("delete-result-cache", f"delete from public.lca_result_cache where snapshot_id in ({fixture},{create})"),
        ("delete-latest-results", f"delete from public.lca_latest_all_unit_results where snapshot_id in ({fixture},{create})"),
        ("delete-results", f"delete from public.lca_results where snapshot_id in ({fixture},{create})"),
    ):
        attempt(label, lambda query=query: client.sql(query))
    if worker_ids:
        ids = ",".join(sql_literal(value) + "::uuid" for value in worker_ids)
        attempt(
            "detach-worker-job-lineage",
            lambda: client.sql(
                f"update private.worker_jobs set root_job_id=null,parent_job_id=null where id in ({ids})"
            ),
        )
        attempt("delete-worker-jobs", lambda: client.sql(f"delete from private.worker_jobs where id in ({ids})"))
    for label, query in (
        ("delete-active", f"delete from private.lca_active_snapshots where snapshot_id in ({fixture},{create})"),
        ("delete-artifacts", f"delete from private.lca_snapshot_artifacts where snapshot_id in ({fixture},{create})"),
        ("delete-snapshots", f"delete from private.lca_network_snapshots where id in ({fixture},{create})"),
    ):
        attempt(label, lambda query=query: client.sql(query))

    storage_rows = attempt(
        "discover-storage-objects",
        lambda: _rows(
            client.sql(
                f"select name from storage.objects where bucket_id={sql_literal(run.bucket)} order by name"
            ),
            "Storage object discovery",
        ),
    )
    object_names = [str(row["name"]) for row in (storage_rows or []) if row.get("name")]
    if object_names:
        attempt(
            "delete-storage-objects",
            lambda: _allow_status(
                client.storage_json(f"object/{run.bucket}", method="DELETE", body={"prefixes": object_names}),
                (200, 404),
                "Storage object delete",
            ),
        )
    bucket_rows = attempt(
        "discover-storage-bucket",
        lambda: _rows(
            client.sql(f"select id from storage.buckets where id={sql_literal(run.bucket)}"),
            "Storage bucket discovery",
        ),
    )
    if bucket_rows:
        attempt(
            "delete-storage-bucket",
            lambda: _allow_status(
                client.storage_json(f"bucket/{run.bucket}", method="DELETE"),
                (200, 404),
                "Storage bucket delete",
            ),
        )

    for actor_id in actor_ids:
        attempt("revoke-auth-sessions", lambda actor_id=actor_id: client.sql(f"delete from auth.sessions where user_id={sql_literal(actor_id)}::uuid"))
        attempt(
            "delete-auth-user",
            lambda actor_id=actor_id: _allow_status(
                client.auth(f"admin/users/{actor_id}", method="DELETE", key=client.secret_key),
                (200, 404),
                "Auth user delete",
            ),
        )

    residue = attempt(
        "readback",
        lambda: _single_row(
            client.sql(
                "select "
                f"(select count(*)::int from private.lca_network_snapshots where id in ({fixture},{create})) network,"
                f"(select count(*)::int from private.lca_snapshot_artifacts where snapshot_id in ({fixture},{create})) artifact,"
                f"(select count(*)::int from private.lca_active_snapshots where snapshot_id in ({fixture},{create})) active,"
                f"(select count(*)::int from public.lca_result_cache where snapshot_id in ({fixture},{create})) result_cache,"
                f"(select count(*)::int from private.worker_jobs where {worker_predicate}) worker_jobs,"
                f"(select count(*)::int from public.lca_results where snapshot_id in ({fixture},{create})) lca_results,"
                f"(select count(*)::int from public.lca_latest_all_unit_results where snapshot_id in ({fixture},{create})) latest_results,"
                f"(select count(*)::int from storage.objects where bucket_id={sql_literal(run.bucket)}) storage_objects,"
                f"(select count(*)::int from storage.buckets where id={sql_literal(run.bucket)}) storage_buckets,"
                f"(select count(*)::int from auth.users where email={sql_literal(run.email)} and raw_user_meta_data->>'issue380_marker'={sql_literal(run.marker)}) users,"
                f"(select count(*)::int from auth.sessions where user_id in (select id from auth.users where email={sql_literal(run.email)} and raw_user_meta_data->>'issue380_marker'={sql_literal(run.marker)})) sessions"
            )
        ),
    )
    if residue is not None and any(int(value) != 0 for value in residue.values()):
        errors.append(QualificationError("namespace reconcile readback is non-zero"))
    if errors:
        raise ExceptionGroup("namespace reconciliation failed", errors)


def finish_with_reconcile(primary_error: BaseException | None, client: HostedClient, run: RunNamespace) -> None:
    errors: list[BaseException] = [primary_error] if primary_error is not None else []
    try:
        reconcile_namespace(client, run)
    except ExceptionGroup as cleanup:
        errors.extend(cleanup.exceptions)
    except BaseException as cleanup:
        errors.append(cleanup)
    if errors:
        raise ExceptionGroup("hosted qualification or namespace reconciliation failed", errors)


def canonical_worker_fixture_sql(
    snapshot_id: uuid.UUID,
    requested_by: str,
    marker: str,
    jobs: list[tuple[uuid.UUID, str, str, dict[str, Any]]],
) -> str:
    rows: list[str] = []
    for job_id, job_kind, payload_schema_version, payload in jobs:
        payload_json = json.dumps(payload, separators=(",", ":"))
        rows.append(
            "("
            f"'{job_id}',{sql_literal(job_kind)},'calculator','solver',0,{sql_literal(str(snapshot_id))},"
            f"'lca_job','{job_id}','{snapshot_id}','user','{requested_by}',"
            f"{sql_literal(marker + ':' + str(job_id))},{sql_literal(canonical_hash(payload))},"
            f"'completed','user',1,3,{sql_literal(payload_schema_version)},{sql_literal(payload_json)}::jsonb,"
            "'{}'::jsonb,now(),now(),now(),now()"
            ")"
        )
    return (
        "insert into private.worker_jobs("
        "id,job_kind,worker_runtime,worker_queue,priority,queue_key,subject_type,subject_id,"
        "subject_version,requester_type,requested_by,idempotency_key,request_hash,status,visibility,"
        "attempt_count,max_attempts,payload_schema_version,payload_json,diagnostics,created_at,updated_at,"
        "started_at,finished_at) values "
        + ",".join(rows)
        + ";"
    )


def unexpected_phase_error(phase: str, error: BaseException) -> QualificationError:
    return QualificationError(f"hosted phase failed: {phase}: {type(error).__name__}")


def qualification_phase_error(phase: str, error: QualificationError) -> QualificationError:
    if str(error).startswith("hosted phase failed: "):
        return error
    return unexpected_phase_error(phase, error)


def safe_diagnostic_details(error: BaseException) -> list[str]:
    if isinstance(error, BaseExceptionGroup):
        return [detail for nested in error.exceptions for detail in safe_diagnostic_details(nested)]
    if isinstance(error, QualificationError):
        return [str(error)]
    return [type(error).__name__]


def qualify(config: Config) -> None:
    config.validate()
    publishable_key, secret_key = resolve_keys(config)
    client = HostedClient(config, publishable_key, secret_key)
    run = RunNamespace.create()
    namespace = uuid.UUID(run.marker)
    fixture_id, create_id = run.fixture_id, run.create_id
    artifact_id, impact_id = uuid.uuid5(namespace, "artifact"), uuid.uuid5(namespace, "impact")
    solve_job, contribution_job, all_unit_job = (uuid.uuid5(namespace, name) for name in ("solve-job", "contribution-job", "all-unit-job"))
    solve_result_id = uuid.uuid5(namespace, "solve-result")
    contribution_result_id = uuid.uuid5(namespace, "contribution-result")
    result_id = uuid.uuid5(namespace, "all-unit-result")
    bucket, email, marker = run.bucket, run.email, run.marker
    password = f"Issue380-{secrets.token_urlsafe(24)}-Aa1!"
    mask(password)
    user_id: str | None = None
    access_token: str | None = None
    primary_error: BaseException | None = None
    phase = "namespace-preflight"

    try:
        reconcile_namespace(client, run)
        phase = "deployed-contract"
        head = _single_row(client.sql("select max(version)::text migration_head from supabase_migrations.schema_migrations"))["migration_head"]
        if head != MIGRATION_HEAD:
            raise QualificationError("persistent Dev migration head mismatch")

        status, functions = client.management(f"/v1/projects/{DEV_REF}/functions")
        if status != 200 or not isinstance(functions, list):
            raise QualificationError("cannot read deployed Edge function metadata")
        deployed = {str(row.get("slug") or row.get("name")): row for row in functions if isinstance(row, dict)}
        for name, expected in EDGE_DEPLOYMENT_RECEIPT.items():
            row = deployed.get(name, {})
            actual = (str(row.get("id", "")), int(row.get("version", -1)), int(row.get("updated_at", -1)), str(row.get("ezbr_sha256", "")))
            if actual != expected:
                raise QualificationError(f"deployed Edge receipt mismatch: {name}")
        print(f"Edge receipt matched exact remote metadata for source worktree {EDGE_SOURCE_HEAD}; receipt is deployment-time evidence, not a commit-to-bundle proof")

        client.sql(
            "do $$ begin if (select count(*) from pg_proc p join pg_namespace n on n.oid=p.pronamespace "
            "where n.nspname='api' and p.proname in ('lca_snapshot_active_read_v1','lca_snapshot_scope_read_v1','lca_snapshot_resolve_v1','lca_snapshot_artifact_read_v1','lca_snapshot_artifact_latest_v1','cmd_lca_snapshot_create_v1') "
            "and has_function_privilege('service_role',p.oid,'execute') and not has_function_privilege('anon',p.oid,'execute') and not has_function_privilege('authenticated',p.oid,'execute')) <> 6 "
            "then raise exception 'issue380_acl_mismatch'; end if; end $$;"
        )
        phase = "snapshot-rpc-fixture"
        artifact_url = f"https://{DEV_REF}.supabase.co/storage/v1/s3/{bucket}/{run.prefix}/snapshot.h5"
        client.sql(
            f"insert into private.lca_network_snapshots(id,scope,process_filter,source_hash,status,created_by,created_at,updated_at) values('{fixture_id}','full_library','{{\"issue380\":true}}','issue380-source','ready','{uuid.uuid5(namespace, 'actor')}','2099-08-02T00:00:00Z','2099-08-02T00:00:00Z');"
            f"insert into private.lca_snapshot_artifacts(id,snapshot_id,artifact_url,artifact_sha256,artifact_byte_size,artifact_format,process_count,flow_count,impact_count,a_nnz,b_nnz,c_nnz,status,created_at,updated_at) values('{artifact_id}','{fixture_id}','{artifact_url}','{'a' * 64}',256,'hdf5',1,1,1,1,1,1,'ready','2099-08-02T00:00:01Z','2099-08-02T00:00:01Z');"
            f"insert into private.lca_active_snapshots(scope,snapshot_id,source_hash,activated_at,activated_by,note) values('issue380-hosted','{fixture_id}','issue380-source','2099-08-02T00:00:02Z','{uuid.uuid5(namespace, 'actor')}','Issue 380 hosted qualification');"
        )

        rpc_args = {
            "lca_snapshot_active_read_v1": {"p_scope": "issue380-hosted"},
            "lca_snapshot_scope_read_v1": {"p_snapshot_id": str(fixture_id)},
            "lca_snapshot_resolve_v1": {"p_scope": "prod", "p_process_filter": {"issue380": True}},
            "lca_snapshot_artifact_read_v1": {"p_snapshot_id": str(fixture_id)},
            "lca_snapshot_artifact_latest_v1": {},
            "cmd_lca_snapshot_create_v1": {"p_snapshot_id": str(create_id), "p_scope": "full_library", "p_process_filter": {"issue380Create": True}, "p_created_by": str(uuid.uuid5(namespace, "actor"))},
        }
        expected_keys = {
            "lca_snapshot_active_read_v1": {"snapshot_id", "source_hash", "activated_at"},
            "lca_snapshot_scope_read_v1": {"id", "scope", "process_filter", "status"},
            "lca_snapshot_resolve_v1": {"id", "created_at", "process_filter"},
            "lca_snapshot_artifact_read_v1": {"snapshot_id", "artifact_url", "artifact_format", "process_count", "status", "created_at"},
            "lca_snapshot_artifact_latest_v1": {"snapshot_id", "artifact_url", "artifact_format", "process_count", "status", "created_at"},
        }
        for name, args in rpc_args.items():
            phase = f"snapshot-rpc-{name}"
            payload = require_response(client.rest(name, args, key=secret_key), 200)
            if name == "cmd_lca_snapshot_create_v1":
                if payload != {"created": True, "snapshotId": str(create_id)}:
                    raise QualificationError("create RPC DTO mismatch")
            else:
                row = _single_row(payload)
                if set(row) != expected_keys[name] or str(row.get("snapshot_id") or row.get("id")) != str(fixture_id):
                    raise QualificationError(f"RPC DTO/fixture mismatch: {name}")
                if "process_count" in row and row["process_count"] != 1:
                    raise QualificationError(f"RPC process_count mismatch: {name}")
        phase = "snapshot-rpc-create-retry"
        require_response(client.rest("cmd_lca_snapshot_create_v1", rpc_args["cmd_lca_snapshot_create_v1"], key=secret_key), 200, {"created": False, "snapshotId": str(create_id)})
        for name, args in rpc_args.items():
            phase = f"snapshot-rpc-anonymous-{name}"
            status, payload = client.rest(name, args, key=publishable_key)
            if status not in (401, 403) or not isinstance(payload, dict) or payload.get("code") != "42501":
                raise QualificationError(f"anonymous RPC boundary mismatch: {name}")

        phase = "auth-admin-create"
        created = require_response(client.auth("admin/users", method="POST", key=secret_key, body={"email": email, "password": password, "email_confirm": True, "user_metadata": {"issue380_marker": marker}}), 200)
        user = created.get("user", created) if isinstance(created, dict) else None
        if not isinstance(user, dict) or not user.get("id"):
            raise QualificationError("Auth admin create response missing user")
        user_id = str(user["id"])
        phase = "auth-password-sign-in"
        signed_in = require_response(client.auth("token?grant_type=password", method="POST", key=publishable_key, body={"email": email, "password": password}), 200)
        if not isinstance(signed_in, dict) or not signed_in.get("access_token"):
            raise QualificationError("Auth sign-in response missing access token")
        access_token = str(signed_in["access_token"]); mask(access_token)
        for name, args in rpc_args.items():
            phase = f"snapshot-rpc-authenticated-{name}"
            status, payload = client.rest(name, args, key=publishable_key, bearer=access_token)
            if status not in (401, 403) or not isinstance(payload, dict) or payload.get("code") != "42501":
                raise QualificationError(f"authenticated RPC boundary mismatch: {name}")

        phase = "worker-artifact-fixture"
        process = _single_row(client.sql("select id::text, btrim(version)::text version from public.processes where state_code between 100 and 199 and btrim(version) <> '' order by id,version limit 1"))
        process_id, process_version = process["id"], process["version"]
        snapshot_index = {"version": 1, "snapshot_id": str(fixture_id), "process_count": 1, "impact_count": 1, "process_map": [{"process_id": process_id, "process_version": process_version, "process_index": 0}], "impact_map": [{"impact_id": str(impact_id), "impact_index": 0, "impact_key": "issue380", "impact_name": "Issue 380", "unit": "kg"}]}
        query_envelope = {"version": 1, "format": "all-unit-query:v1", "snapshot_id": str(fixture_id), "job_id": str(all_unit_job), "process_count": 1, "impact_count": 1, "h_matrix": [[42.5]]}
        phase = "storage-bucket-create"
        require_response(client.storage_json("bucket", method="POST", body={"id": bucket, "name": bucket, "public": False}), 200)
        phase = "storage-snapshot-index-upload"
        client.upload_json(bucket, f"{run.prefix}/snapshot-index-v1.json", snapshot_index)
        phase = "storage-query-upload"
        client.upload_json(bucket, f"{run.prefix}/query.json", query_envelope)

        solve_request = {"version": "lca_solve_v2", "scope": "prod", "snapshot_id": str(fixture_id), "demand_mode": "all_unit", "solve": {"return_x": False, "return_g": False, "return_h": True}, "print_level": 0}
        contribution_request = {"version": "lca_contribution_path_v1", "scope": "prod", "snapshot_id": str(fixture_id), "data_scope": "open_data", "process_id": process_id, "process_version": process_version, "process_index": 0, "impact_id": str(impact_id), "impact_index": 0, "amount": 1, "options": {"max_depth": 4, "top_k_children": 5, "cutoff_share": 0.01, "max_nodes": 200}, "print_level": 0}
        phase = "worker-result-fixture"
        client.sql(
            canonical_worker_fixture_sql(
                fixture_id,
                user_id,
                marker,
                [
                    (solve_job, "lca.solve_all_unit", "lca.solve_all_unit.request.v1", {"type": "solve_all_unit", "snapshot_id": str(fixture_id), "namespace": marker}),
                    (contribution_job, "lca.contribution_path", "lca.contribution_path.request.v1", {"type": "analyze_contribution_path", "snapshot_id": str(fixture_id), "namespace": marker}),
                    (all_unit_job, "lca.solve_all_unit", "lca.solve_all_unit.request.v1", {"type": "solve_all_unit", "snapshot_id": str(fixture_id), "namespace": marker, "artifact": "query"}),
                ],
            )
            + f"insert into public.lca_results(id,job_id,worker_job_id,snapshot_id,payload) values('{solve_result_id}','{solve_job}','{solve_job}','{fixture_id}','{{}}'),('{contribution_result_id}','{contribution_job}','{contribution_job}','{fixture_id}','{{}}'),('{result_id}','{all_unit_job}','{all_unit_job}','{fixture_id}','{{}}');"
            f"insert into public.lca_result_cache(scope,snapshot_id,request_key,request_payload,status,job_id,worker_job_id,result_id) values('prod','{fixture_id}','{canonical_hash(solve_request)}','{json.dumps(solve_request, separators=(',', ':'))}'::jsonb,'ready','{solve_job}','{solve_job}','{solve_result_id}'),('prod','{fixture_id}','{canonical_hash(contribution_request)}','{json.dumps(contribution_request, separators=(',', ':'))}'::jsonb,'ready','{contribution_job}','{contribution_job}','{contribution_result_id}');"
            f"insert into public.lca_latest_all_unit_results(snapshot_id,job_id,worker_job_id,result_id,query_artifact_url,query_artifact_sha256,query_artifact_byte_size,query_artifact_format,status,computed_at) values('{fixture_id}','{all_unit_job}','{all_unit_job}','{result_id}','https://{DEV_REF}.supabase.co/storage/v1/s3/{bucket}/{run.prefix}/query.json','{'b' * 64}',{len(json.dumps(query_envelope, separators=(',', ':')).encode())},'all-unit-query:v1','ready','2099-08-02T00:00:03Z');"
        )

        phase = "edge-endpoint-contract"
        bodies = {
            "lca_solve": {"snapshot_id": str(fixture_id), "demand_mode": "all_unit"},
            "lca_query_results": {"snapshot_id": str(fixture_id), "data_scope": "open_data", "mode": "process_all_impacts", "process_id": process_id, "process_version": process_version},
            "lca_contribution_path": {"snapshot_id": str(fixture_id), "data_scope": "open_data", "process_id": process_id, "process_version": process_version, "impact_id": str(impact_id)},
        }
        for name, body in bodies.items():
            phase = f"edge-{name}-cors"
            if client.edge(name, method="OPTIONS", body=None, bearer=None)[0] not in (200, 204):
                raise QualificationError(f"Edge CORS mismatch: {name}")
            phase = f"edge-{name}-anonymous"
            require_anonymous_unauthorized(client.edge(name, method="POST", body=body, bearer=None))
        expected_endpoint_results = {
            "lca_solve": str(solve_result_id),
            "lca_contribution_path": str(contribution_result_id),
        }
        for name, expected_result_id in expected_endpoint_results.items():
            phase = f"edge-{name}-authenticated"
            first = require_response(client.edge(name, method="POST", body=bodies[name], bearer=access_token), 200)
            phase = f"edge-{name}-retry"
            second = require_response(client.edge(name, method="POST", body=bodies[name], bearer=access_token), 200)
            if first != second or first.get("mode") != "cache_hit" or first.get("snapshot_id") != str(fixture_id) or not first.get("cache_key") or first.get("result_id") != expected_result_id:
                raise QualificationError(f"Edge success/retry identity mismatch: {name}")
        for attempt in range(2):
            phase = f"edge-lca_query_results-attempt-{attempt + 1}"
            query = require_response(client.edge("lca_query_results", method="POST", body=bodies["lca_query_results"], bearer=access_token), 200)
            if query.get("snapshot_id") != str(fixture_id) or query.get("result_id") != str(result_id) or query.get("mode") != "process_all_impacts" or query.get("data", {}).get("values", [{}])[0].get("value") != 42.5:
                raise QualificationError(f"Edge query DTO mismatch on attempt {attempt + 1}")
        bad = dict(bodies["lca_solve"]); bad.update({"demand_mode": "single", "demand": {"process_index": 1, "amount": 1}})
        phase = "edge-lca_solve-negative"
        require_response(client.edge("lca_solve", method="POST", body=bad, bearer=access_token), 400, {"error": "process_index_out_of_range", "process_index": 1, "process_count": 1})
    except QualificationError as error:
        primary_error = qualification_phase_error(phase, error)
    except BaseException as error:
        primary_error = unexpected_phase_error(phase, error)
    finally:
        finish_with_reconcile(primary_error, client, run)


def main() -> int:
    try:
        qualify(Config.from_env())
    except BaseException as error:
        detail = "; ".join(safe_diagnostic_details(error))
        print(f"hosted qualification failed: {detail}", file=sys.stderr)
        return 1
    print("persistent Dev hosted qualification passed; residue=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
