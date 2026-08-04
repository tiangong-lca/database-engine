#!/usr/bin/env python3
"""Build the lineage-aware, cross-schema SECURITY DEFINER audit."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import re
import stat
import subprocess
import tomllib
from collections import Counter
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = ROOT / "supabase/tests/contracts"
INVENTORY = CONTRACT_DIR / "public_object_inventory.genesis.json"
INVENTORY_SHA = CONTRACT_DIR / "public_object_inventory.genesis.sha256"
BASELINE_AUDIT = CONTRACT_DIR / "security_definer_audit.json"
BASELINE_AUDIT_SHA = CONTRACT_DIR / "security_definer_audit.sha256"
LINEAGE = CONTRACT_DIR / "privileged_routine_lineage.json"
LINEAGE_SHA = CONTRACT_DIR / "privileged_routine_lineage.sha256"
TRANSITION_BASELINE_LINEAGE = (
    CONTRACT_DIR / "privileged_routine_lineage.transition-000-issue-333-lineage-baseline.json"
)
TRANSITION_BASELINE_LINEAGE_SHA = (
    CONTRACT_DIR / "privileged_routine_lineage.transition-000-issue-333-lineage-baseline.sha256"
)
TRANSITION_BASELINE_AUDIT = (
    CONTRACT_DIR / "security_definer_audit_v2.transition-000-issue-333-lineage-baseline.json"
)
TRANSITION_BASELINE_AUDIT_SHA256 = (
    "53e955127545fee508bed8b67d85d497967c8e7397b79c70e988a015ea0e9359"
)
OUT = CONTRACT_DIR / "security_definer_audit_v2.json"
SHA = CONTRACT_DIR / "security_definer_audit_v2.sha256"
CONFIG = ROOT / "supabase/config.toml"

GOVERNED_SCHEMAS = ("api", "archive", "private", "public", "util")
ROLES = ("PUBLIC", "anon", "authenticated", "service_role", "api_internal_executor", "postgres")
TRUSTED_PLATFORM_ROLES = ("postgres", "supabase_admin", "pg_database_owner")
BASELINE_PROVENANCE = {
    "inventorySchemaVersion": "database.public-object-inventory-closure.v1",
    "inventorySha256": "2526146dc64e2b32bdf9afb2ebcc0495f5a174f241c2abbd7f0f1b5348aa8c18",
    "auditV1Sha256": "8fca15a8728c79a73784199950f182a9465c33098e3c0b6e5edd54836c6669f7",
    "databaseSchemaSha": "05d1387cc073da8161db782db978a77431ff8b3f",
    "lineageSnapshotSha256": "996a95d81ace738380a6de557cfc3723f58e1b3158aa25adc4427a54bc827eb9",
}
BASELINE_CURRENT_TRANSITION = {
    "sequence": 0,
    "batch": "issue-333-lineage-baseline",
    "databaseSchemaSha": "05d1387cc073da8161db782db978a77431ff8b3f",
    "predecessorArtifactSha256": "8fca15a8728c79a73784199950f182a9465c33098e3c0b6e5edd54836c6669f7",
}
EXPECTED_CURRENT_TRANSITION = {
    "sequence": 2,
    "batch": "issue-355-identity-collaboration",
    "databaseSchemaSha": "9e843e67783123f859035e3a720df910e87a644e",
    "predecessorArtifactSha256": "b1f7ac582b87f78786ea34a855806ffae42cb50ea419bc752f6e00c01da02b11",
}
EXPECTED_COMPLETED_TRANSITIONS: tuple[dict[str, Any], ...] = ({
    "sequence": 0,
    "batch": "issue-333-lineage-baseline",
    "databaseSchemaSha": "05d1387cc073da8161db782db978a77431ff8b3f",
    "predecessorArtifactSha256": "8fca15a8728c79a73784199950f182a9465c33098e3c0b6e5edd54836c6669f7",
    "predecessorAuditPath": "supabase/tests/contracts/security_definer_audit.json",
    "producedAuditV2Sha256": "53e955127545fee508bed8b67d85d497967c8e7397b79c70e988a015ea0e9359",
    "producedAuditV2Path": (
        "supabase/tests/contracts/"
        "security_definer_audit_v2.transition-000-issue-333-lineage-baseline.json"
    ),
    "receiptPath": (
        "supabase/tests/contracts/"
        "security_definer_transition_receipt.000-issue-333-lineage-baseline.json"
    ),
    "receiptSha256": "f8931edaa90a8cd2608d38b79e6d5fc24c3c82ff440f4d6a5aee65aeb792ca7d",
}, {
    "sequence": 1,
    "batch": "issue-356-worker-control-plane",
    "databaseSchemaSha": "7a609de6e68848a66ad8abfbec5681211302b108",
    "predecessorArtifactSha256": "53e955127545fee508bed8b67d85d497967c8e7397b79c70e988a015ea0e9359",
    "predecessorAuditPath": (
        "supabase/tests/contracts/"
        "security_definer_audit_v2.transition-000-issue-333-lineage-baseline.json"
    ),
    "producedAuditV2Sha256": "b1f7ac582b87f78786ea34a855806ffae42cb50ea419bc752f6e00c01da02b11",
    "producedAuditV2Path": (
        "supabase/tests/contracts/"
        "security_definer_audit_v2.transition-001-issue-356-worker-control-plane.json"
    ),
    "receiptPath": (
        "supabase/tests/contracts/"
        "security_definer_transition_receipt.001-issue-356-worker-control-plane.json"
    ),
    "receiptSha256": "31f3028d15985e7403025ab25aed0e33bd67a6b3a0d1f19e7a6e1318eff85ce1",
})

CATALOG_QUERY = r"""
select coalesce(jsonb_agg(jsonb_build_object(
  'objectKey', format('%I.%I(%s)', n.nspname, p.proname,
    pg_get_function_identity_arguments(p.oid)),
  'schema', n.nspname,
  'name', p.proname,
  'routineKind', case p.prokind when 'p' then 'procedure' else 'function' end,
  'securityDefiner', p.prosecdef,
  'ownerRole', pg_get_userbyid(p.proowner),
  'databaseOwnerRole', (
    select pg_get_userbyid(d.datdba) from pg_database d where d.datname = current_database()
  ),
  'language', l.lanname,
  'volatility', p.provolatile,
  'strict', p.proisstrict,
  'parallel', p.proparallel,
  'resultType', pg_get_function_result(p.oid),
  'returnTypeName', format_type(p.prorettype, null),
  'returnTypeKind', rt.typtype,
  'argumentNames', coalesce(to_jsonb(p.proargnames), '[]'::jsonb),
  'argumentModes', coalesce(to_jsonb(p.proargmodes), '[]'::jsonb),
  'inputArgumentNames', coalesce((
    select jsonb_agg(coalesce(p.proargnames[g.ordinality], '') order by g.ordinality)
    from unnest(p.proargtypes::oid[]) with ordinality g(type_oid, ordinality)
  ), '[]'::jsonb),
  'inputArgumentTypes', coalesce((
    select jsonb_agg(format_type(types.type_oid, null) order by types.ordinality)
    from unnest(p.proargtypes::oid[]) with ordinality types(type_oid, ordinality)
  ), '[]'::jsonb),
  'inputArgumentTypeKinds', coalesce((
    select jsonb_agg(t.typtype order by types.ordinality)
    from unnest(p.proargtypes::oid[]) with ordinality types(type_oid, ordinality)
    join pg_type t on t.oid = types.type_oid
  ), '[]'::jsonb),
  'inputArgumentRequired', coalesce((
    select jsonb_agg(
      types.ordinality <= (p.pronargs - p.pronargdefaults)
      order by types.ordinality
    )
    from unnest(p.proargtypes::oid[]) with ordinality types(type_oid, ordinality)
  ), '[]'::jsonb),
  'outputArgumentCount', coalesce((
    select count(*)
    from unnest(p.proargmodes) mode
    where mode in ('o'::"char", 'b'::"char", 't'::"char")
  ), 0),
  'config', coalesce(to_jsonb(p.proconfig), '[]'::jsonb),
  'definition', pg_get_functiondef(p.oid),
  'body', p.prosrc,
  'staticDependencies', case
    when l.lanname = 'plpgsql' and format_type(p.prorettype, null) not in ('trigger', 'event_trigger')
    then coalesce((
      select jsonb_agg(jsonb_build_object(
        'type', dependency.type,
        'schema', dependency.schema,
        'name', dependency.name,
        'params', dependency.params
      ) order by dependency.type, dependency.schema, dependency.name, dependency.params)
      from :"proof_schema".plpgsql_show_dependency_tb(p.oid::regprocedure) dependency
      where dependency.type in ('RELATION', 'TYPE')
    ), '[]'::jsonb)
    else '[]'::jsonb
  end,
  'staticDependencyEngine', case
    when l.lanname = 'plpgsql' and format_type(p.prorettype, null) not in ('trigger', 'event_trigger')
    then 'plpgsql-check-2.7'
    else 'strict-static-sql-qualification-v1'
  end,
  'directExecuteGrants', coalesce((
    select jsonb_agg(coalesce(grantee.rolname, 'PUBLIC') order by coalesce(grantee.rolname, 'PUBLIC'))
    from aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) x
    left join pg_roles grantee on grantee.oid = x.grantee
    where x.privilege_type = 'EXECUTE'
  ), '[]'::jsonb),
  'publicSchemaUsage', exists (
    select 1
    from aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) x
    where x.grantee = 0 and x.privilege_type = 'USAGE'
  ),
  'namedRoleMatrix', coalesce((
    select jsonb_agg(jsonb_build_object(
      'role', wanted.role,
      'schemaUsage', has_schema_privilege(r.oid, n.oid, 'USAGE'),
      'execute', has_function_privilege(r.oid, p.oid, 'EXECUTE'),
      'postgrestAuthenticatorCanSetRole', exists (
        select 1 from pg_roles authenticator
        where authenticator.rolname = 'authenticator'
          and pg_has_role(authenticator.oid, r.oid, 'SET')
      )
    ) order by wanted.position)
    from (values
      (1, 'anon'), (2, 'authenticated'), (3, 'service_role'),
      (4, 'api_internal_executor'), (5, 'postgres')
    ) wanted(position, role)
    join pg_roles r on r.rolname = wanted.role
  ), '[]'::jsonb),
  'effectiveCallerMatrix', coalesce((
    select jsonb_agg(jsonb_build_object(
      'role', r.rolname,
      'canLogin', r.rolcanlogin,
      'schemaUsage', has_schema_privilege(r.oid, n.oid, 'USAGE'),
      'execute', has_function_privilege(r.oid, p.oid, 'EXECUTE'),
      'postgrestAuthenticatorCanSetRole', exists (
        select 1 from pg_roles authenticator
        where authenticator.rolname = 'authenticator'
          and pg_has_role(authenticator.oid, r.oid, 'SET')
      )
    ) order by r.rolname)
    from pg_roles r
    where has_schema_privilege(r.oid, n.oid, 'USAGE')
      and has_function_privilege(r.oid, p.oid, 'EXECUTE')
  ), '[]'::jsonb),
  'schemaTrust', coalesce((
    select jsonb_agg(jsonb_build_object(
      'schema', candidate.nspname,
      'ownerRole', pg_get_userbyid(candidate.nspowner),
      'ownerTrusted', (
        candidate.nspowner = p.proowner
        or pg_get_userbyid(candidate.nspowner) in ('postgres', 'supabase_admin')
        or (
          pg_get_userbyid(candidate.nspowner) = 'pg_database_owner'
          and (select pg_get_userbyid(d.datdba) from pg_database d where d.datname = current_database())
              in ('postgres', 'supabase_admin')
        )
      ),
      'publicCreate', exists (
        select 1 from aclexplode(coalesce(candidate.nspacl, acldefault('n', candidate.nspowner))) ax
        where ax.grantee = 0 and ax.privilege_type = 'CREATE'
      ),
      'roleCreate', coalesce((
        select jsonb_object_agg(wanted.role, has_schema_privilege(role.oid, candidate.oid, 'CREATE'))
        from (values ('anon'), ('authenticated'), ('service_role'), ('api_internal_executor')) wanted(role)
        join pg_roles role on role.rolname = wanted.role
      ), '{}'::jsonb),
      'nonOwnerCallableCreateRoles', coalesce((
        select jsonb_agg(role.rolname order by role.rolname)
        from pg_roles role
        where role.oid <> p.proowner
          and role.rolname not in ('postgres', 'supabase_admin', 'pg_database_owner')
          and (
            role.rolcanlogin or exists (
              select 1 from pg_roles login_role
              where login_role.rolcanlogin
                and login_role.rolname not in ('postgres', 'supabase_admin', 'pg_database_owner')
                and pg_has_role(login_role.oid, role.oid, 'MEMBER')
            )
          )
          and has_schema_privilege(role.oid, n.oid, 'USAGE')
          and has_function_privilege(role.oid, p.oid, 'EXECUTE')
          and has_schema_privilege(role.oid, candidate.oid, 'CREATE')
      ), '[]'::jsonb)
    ) order by candidate.nspname)
    from pg_namespace candidate
    where candidate.nspname in ('api', 'archive', 'extensions', 'pg_catalog', 'private', 'public', 'util')
  ), '[]'::jsonb)
) order by format('%I.%I(%s)', n.nspname, p.proname,
  pg_get_function_identity_arguments(p.oid))), '[]'::jsonb)::text
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
join pg_language l on l.oid = p.prolang
join pg_type rt on rt.oid = p.prorettype
where n.nspname in ('api', 'archive', 'private', 'public', 'util')
  and p.prokind in ('f', 'p');
"""


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def read_hashed_json(path: Path, digest_path: Path) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    digest = sha256_bytes(raw)
    if digest_path.read_text(encoding="utf-8") != digest + "\n":
        raise ValueError(f"{path.name} hash does not match committed bytes")
    value = json.loads(raw)
    if raw != canonical(value).encode("utf-8"):
        raise ValueError(f"{path.name} is not canonical byte-for-byte")
    return value, digest


def canonical_contract_path(value: str) -> PurePosixPath:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ValueError("reviewed contract path must be a canonical repository-relative POSIX path")
    path = PurePosixPath(value)
    if (path.is_absolute() or path.as_posix() != value or "." in path.parts or ".." in path.parts
            or path.parts[:3] != ("supabase", "tests", "contracts") or len(path.parts) != 4):
        raise ValueError("reviewed contract path must be canonically beneath supabase/tests/contracts")
    return path


def git_index_mode(relative_path: str) -> str:
    result = subprocess.run(
        ["git", "ls-files", "--stage", "--error-unmatch", "--", relative_path],
        cwd=ROOT, check=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    )
    lines = [line for line in result.stdout.splitlines() if line]
    if result.returncode != 0 or len(lines) != 1:
        raise ValueError("reviewed contract path must be a unique stage-0 Git index entry")
    metadata, separator, indexed_path = lines[0].partition("\t")
    fields = metadata.split()
    if not separator or indexed_path != relative_path or len(fields) != 3 or fields[2] != "0":
        raise ValueError("reviewed contract path has an invalid Git index identity")
    mode = fields[0]
    if mode not in {"100644", "100755"}:
        raise ValueError("reviewed contract path must be a Git regular file")
    return mode


def read_reviewed_contract_bytes(value: str) -> bytes:
    """Read a reviewed contract through no-follow descriptors after Git identity proof."""
    path = canonical_contract_path(value)
    git_index_mode(value)
    directory_fd = os.open(ROOT, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for component in path.parts[:-1]:
            next_fd = os.open(
                component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory_fd,
            )
            os.close(directory_fd)
            directory_fd = next_fd
        file_fd = os.open(path.parts[-1], os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
        try:
            if not stat.S_ISREG(os.fstat(file_fd).st_mode):
                raise ValueError("reviewed contract path must resolve to a regular file")
            chunks: list[bytes] = []
            while chunk := os.read(file_fd, 1024 * 1024):
                chunks.append(chunk)
            return b"".join(chunks)
        finally:
            os.close(file_fd)
    except OSError as exc:
        raise ValueError("reviewed contract path cannot be opened without following links") from exc
    finally:
        os.close(directory_fd)


def database_url() -> str:
    if value := os.environ.get("DATABASE_URL"):
        return value
    result = subprocess.run(
        ["supabase", "status", "--output", "json"], cwd=ROOT, check=True,
        text=True, stdout=subprocess.PIPE,
    )
    return json.loads(result.stdout)["DB_URL"]


@dataclass(frozen=True)
class Connection:
    host: str
    port: int
    database: str
    user: str
    credential: str | None

    def command(self, *args: str) -> list[str]:
        return [
            "psql", "--host", self.host, "--port", str(self.port),
            "--dbname", self.database, "--username", self.user, *args,
        ]

    def environment(self) -> dict[str, str]:
        environment = {key: value for key, value in os.environ.items() if not key.startswith("PG")}
        environment.pop("DATABASE_URL", None)
        if self.credential is not None:
            environment["PGPASSWORD"] = self.credential
        return environment


def parse_loopback_connection(value: str) -> Connection:
    """Parse one explicit loopback TCP URI without returning its secret in argv."""
    if re.search(r"%(?![0-9A-Fa-f]{2})", value):
        raise ValueError("local database URL contains invalid percent encoding")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("local database URL has an invalid host or port") from exc
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise ValueError("local database URL must use postgres or postgresql")
    if parsed.query or parsed.fragment:
        raise ValueError("local database URL query overrides and fragments are forbidden")
    if not parsed.netloc or parsed.hostname is None or port is None:
        raise ValueError("local database URL must contain one explicit TCP host and port")
    host = unquote(parsed.hostname)
    if ("," in parsed.netloc or "," in host or "/" in host or "\\" in host
            or any(character.isspace() or ord(character) < 32 for character in host)):
        raise ValueError("local database URL must use one TCP host; multi-host and sockets are forbidden")
    try:
        loopback = host == "localhost" or ipaddress.ip_address(host).is_loopback
    except ValueError:
        loopback = False
    if not loopback:
        raise ValueError("lineage-aware SECURITY DEFINER audit requires a literal loopback database host")
    user = unquote(parsed.username or "")
    database = unquote(parsed.path.removeprefix("/"))
    if (not user or not database or "/" in database or
            any(character.isspace() or ord(character) < 32 for character in user + database)):
        raise ValueError("local database URL must contain one user and database")
    credential = unquote(parsed.password) if parsed.password is not None else None
    if credential is not None and any(ord(character) < 32 for character in credential):
        raise ValueError("local database URL password contains control characters")
    return Connection(host=host, port=port, database=database, user=user, credential=credential)


def exposed_schemas() -> list[str]:
    config = tomllib.loads(CONFIG.read_text(encoding="utf-8"))
    schemas = config["api"]["schemas"]
    if not isinstance(schemas, list) or not schemas or any(not isinstance(item, str) for item in schemas):
        raise ValueError("config.toml api.schemas must be a non-empty ordered string list")
    if len(schemas) != len(set(schemas)):
        raise ValueError("config.toml api.schemas contains duplicate schemas")
    return schemas


def catalog_proof_query() -> str:
    # plpgsql_check is available in the local PG17 image but is deliberately not
    # a schema migration.  Install it only inside a rolled-back proof transaction.
    return r"""
BEGIN;
CREATE EXTENSION IF NOT EXISTS plpgsql_check WITH SCHEMA extensions;
SELECT n.nspname AS proof_schema
FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace
WHERE e.extname = 'plpgsql_check' \gset
""" + CATALOG_QUERY + "\nROLLBACK;"


def load_catalog(db_url: str | None = None) -> dict[str, dict[str, Any]]:
    connection = parse_loopback_connection(db_url or database_url())
    result = subprocess.run(
        connection.command("-qAtX", "-v", "ON_ERROR_STOP=1"),
        cwd=ROOT, env=connection.environment(), check=True, text=True,
        input=catalog_proof_query(), stdout=subprocess.PIPE,
    )
    rows = json.loads(result.stdout)
    catalog = {row["objectKey"]: row for row in rows}
    if len(catalog) != len(rows):
        raise ValueError("governed routine catalog contains duplicate exact signatures")
    return catalog


def lineage_key(original_object_key: str) -> str:
    return sha256_text(BASELINE_PROVENANCE["inventorySha256"] + "\0" + original_object_key)


def transition_lineage_key(original_object_key: str, birth_transition: dict[str, Any]) -> str:
    return sha256_text(
        "transition\0" + birth_transition["databaseSchemaSha"] + "\0" + original_object_key
    )


def original_lineage_snapshot(lineages: list[dict[str, Any]]) -> str:
    immutable = [
        {
            field: row[field] for field in (
                "lineageKey", "origin", "originalObjectKey", "originalSchema",
                "originalDefinitionSha256", "originalRoutineKind",
            )
        }
        for row in lineages
    ]
    return sha256_text(canonical(immutable))


BUILTIN_TYPE_NAMES = {
    "bigint", "bigserial", "bit", "boolean", "bytea", "character", "date",
    "decimal", "double", "float4", "float8", "int2", "int4", "int8", "integer",
    "interval", "json", "jsonb", "numeric", "oid", "real", "record", "regclass",
    "regprocedure", "smallint", "text", "time", "timestamp", "timestamptz", "uuid", "varchar",
}


def _strip_sql_comments(value: str) -> str:
    value = re.sub(r"/\*.*?\*/", " ", value, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", value)


def static_search_path_proof(row: dict[str, Any]) -> tuple[dict[str, Any], bool, str | None]:
    """Prove that implicit pg_temp cannot capture a relation or custom type."""
    source = _strip_sql_comments(row["body"])
    lexical = re.sub(r"\$(?:[A-Za-z_][\w$]*)?\$.*?\$(?:[A-Za-z_][\w$]*)?\$", "''", source, flags=re.DOTALL)
    lexical = re.sub(r"'(?:''|[^'])*'", "''", lexical)
    qualified_dependencies: list[dict[str, Any]] = []
    unqualified_dependencies: list[dict[str, Any]] = []
    for dependency in row.get("staticDependencies", []):
        schema = re.escape(str(dependency["schema"]))
        name = re.escape(str(dependency["name"]))
        qualified = bool(re.search(
            rf'(?<![\w$])(?:"{schema}"|{schema})\s*\.\s*(?:"{name}"|{name})(?![\w$])',
            source, flags=re.IGNORECASE,
        ))
        (qualified_dependencies if qualified else unqualified_dependencies).append(dependency)

    ctes = {
        match.group(1).lower()
        for match in re.finditer(r'(?:\bwith\b|,)\s*([a-z_][\w$]*)\s+as\s+(?:materialized\s+)?\(', lexical, re.I)
    }
    unqualified_relations: set[str] = set()
    relation_pattern = re.compile(
        r'\b(?:from|join|update|insert\s+into|truncate\s+(?:table\s+)?|delete\s+from)\s+'
        r'(?:only\s+)?((?:"[^"]+"|[a-z_][\w$]*)(?:\s*\.\s*(?:"[^"]+"|[a-z_][\w$]*|%I))?)',
        re.I,
    )
    for match in relation_pattern.finditer(lexical):
        if lexical[max(0, match.start() - 12):match.start()].lower().rstrip().endswith("distinct"):
            continue
        token = re.sub(r'\s+', '', match.group(1))
        tail = lexical[match.end():].lstrip()
        if ("." not in token and token.strip('"').lower() not in ctes
                and token.strip('"').lower() not in {"lateral", "set"} and not tail.startswith("(")):
            unqualified_relations.add(token)

    unqualified_types: set[str] = set()
    for match in re.finditer(r'::\s*((?:"[^"]+"|[a-z_][\w$]*)(?:\s*\.\s*(?:"[^"]+"|[a-z_][\w$]*))?)', lexical, re.I):
        token = re.sub(r'\s+', '', match.group(1))
        if "." not in token and token.strip('"').lower() not in BUILTIN_TYPE_NAMES:
            unqualified_types.add(token)

    trusted_lookup_schemas = {
        "api", "archive", "extensions", "pg_catalog", "private", "public", "util",
    }
    runtime_lookup_tokens: list[str] = []
    unqualified_runtime_lookups: set[str] = set()
    unproven_runtime_lookup_count = 0

    def record_runtime_lookup(token: str) -> None:
        normalized = token.replace('"', "").strip()
        runtime_lookup_tokens.append(normalized)
        schema = normalized.split(".", 1)[0].lower() if "." in normalized else None
        if schema not in trusted_lookup_schemas:
            unqualified_runtime_lookups.add(normalized)

    for lookup in re.finditer(r"\b(?:to_regclass|to_regtype|nextval|currval|setval)\s*\(([^,)]*)", source, re.I):
        argument = lookup.group(1).strip()
        literal = re.fullmatch(
            r"'(?:''|[^'])*'(?:::(?:pg_catalog\.)?(?:text|regclass|regtype))?", argument, re.I,
        )
        if literal:
            record_runtime_lookup(argument.split("'", 2)[1].replace("''", "'"))
        else:
            unproven_runtime_lookup_count += 1
    for lookup in re.finditer(r"'((?:''|[^'])*)'\s*::\s*(?:pg_catalog\.)?(?:regclass|regtype)\b", source, re.I):
        record_runtime_lookup(lookup.group(1).replace("''", "'"))

    dynamic_templates: list[dict[str, Any]] = []
    unproven_dynamic_count = 0
    for match in re.finditer(r'\bexecute\b', source, re.I):
        literal = re.match(
            r"\s*(?P<format>format\s*\(\s*)?(?P<literal>'(?:''|[^'])*')",
            source[match.end():], re.I | re.DOTALL,
        )
        if literal is None:
            unproven_dynamic_count += 1
            continue
        template = literal.group("literal")[1:-1].replace("''", "'")
        targets = []
        for target_match in re.finditer(
            r'\b(?:from|join|update|insert\s+into|truncate\s+(?:table\s+)?|delete\s+from)\s+'
            r'(?:only\s+)?((?:%I|"[^"]+"|[a-z_][\w$]*)(?:\s*\.\s*(?:%I|"[^"]+"|[a-z_][\w$]*))?)',
            template, re.I,
        ):
            targets.append(re.sub(r'\s+', '', target_match.group(1)))
        trusted_dynamic_schemas = {
            "api", "archive", "extensions", "pg_catalog", "private", "public", "util",
        }
        unsafe_targets = [
            target for target in targets
            if ("." not in target
                or target.split(".", 1)[0].strip('"').lower() not in trusted_dynamic_schemas)
        ]
        unsafe_format = bool(literal.group("format") and re.search(r'(?<!%)%s', template, re.I))
        proven = not unsafe_targets and not unsafe_format
        dynamic_templates.append({
            "templateSha256": sha256_text(template),
            "relationTargets": targets,
            "provenQualified": proven,
        })
        if not proven:
            unproven_dynamic_count += 1
    proof = {
        "engine": row.get("staticDependencyEngine", "strict-static-sql-qualification-v1"),
        "dependencies": row.get("staticDependencies", []),
        "qualifiedDependencies": qualified_dependencies,
        "unqualifiedDependencies": unqualified_dependencies,
        "unqualifiedRelationTokens": sorted(unqualified_relations),
        "unqualifiedTypeTokens": sorted(unqualified_types),
        "runtimeLookupTokens": sorted(runtime_lookup_tokens),
        "unqualifiedRuntimeLookupTokens": sorted(unqualified_runtime_lookups),
        "unprovenRuntimeLookupCount": unproven_runtime_lookup_count,
        "dynamicSqlTemplates": dynamic_templates,
        "unprovenDynamicSqlStatementCount": unproven_dynamic_count,
    }
    if unqualified_dependencies:
        return proof, False, "unqualified-plpgsql-check-relation-or-type-dependency"
    if unqualified_relations:
        return proof, False, "unqualified-static-relation-reference"
    if unqualified_types:
        return proof, False, "unqualified-static-custom-type-reference"
    if unqualified_runtime_lookups:
        return proof, False, "unqualified-runtime-relation-or-type-lookup"
    if unproven_runtime_lookup_count:
        return proof, False, "unproven-runtime-relation-or-type-lookup"
    if unproven_dynamic_count:
        return proof, False, "unproven-dynamic-sql"
    return proof, True, None


def endpoint_properties(
    row: dict[str, Any], *, grandfathered_definition: bool = False,
) -> dict[str, Any]:
    properties = {
        "language": row["language"],
        "volatility": row["volatility"],
        "strict": row["strict"],
        "parallel": row["parallel"],
        "resultType": row["resultType"],
        "config": row["config"],
    }
    search_path = sorted(value for value in row["config"] if value.startswith("search_path="))
    parsed_path, path_trust, path_safe, unsafe_reason = search_path_evidence(search_path, row["schemaTrust"])
    proof, proof_safe, proof_reason = static_search_path_proof(row)
    explicit_compliant = path_safe
    grandfathered = not explicit_compliant and grandfathered_definition
    path_safe = explicit_compliant or (grandfathered and proof_safe)
    if grandfathered and proof_safe:
        unsafe_reason = None
    elif not explicit_compliant and proof_reason:
        unsafe_reason = f"{unsafe_reason};{proof_reason}"
    return {
        "currentObjectKey": row["objectKey"],
        "currentSchema": row["schema"],
        "routineKind": row["routineKind"],
        "securityDefiner": row["securityDefiner"],
        "ownerRole": row["ownerRole"],
        "searchPath": search_path,
        "fixedSearchPath": bool(search_path),
        "parsedSearchPath": parsed_path,
        "searchPathSchemaTrust": path_trust,
        "searchPathProof": proof,
        "searchPathDisposition": (
            "explicit-trusted-pg-temp-last" if explicit_compliant
            else "grandfathered-static-proof" if grandfathered and proof_safe
            else "grandfathered-unproven-residue" if grandfathered
            else "unsafe"
        ),
        "grandfatheredSearchPathResidue": grandfathered,
        "safeSearchPath": path_safe,
        "unsafeSearchPathReason": unsafe_reason,
        "definitionSha256": sha256_text(row["definition"]),
        "bodySha256": sha256_text(row["body"]),
        "propertiesSha256": sha256_text(canonical(properties)),
    }


def search_path_evidence(
    settings: list[str], schema_trust: list[dict[str, Any]],
) -> tuple[list[str], list[dict[str, Any]], bool, str | None]:
    if len(settings) != 1:
        return [], [], False, "missing-or-duplicate-search-path-setting"
    raw = settings[0].split("=", 1)[1].strip()
    if raw in {'""', "''", ""}:
        return [], [], False, "implicit-pg-temp-precedes-empty-search-path"
    names = [part.strip().strip('"') for part in raw.split(",")]
    if not names or any(not name for name in names):
        return names, [], False, "empty-search-path-component"
    if "$user" in names:
        return names, [], False, "user-dependent-search-path"
    if len(names) != len(set(names)):
        return names, [], False, "duplicate-search-path-schema"
    if names.count("pg_temp") != 1 or names[-1] != "pg_temp":
        return names, [], False, "pg-temp-must-appear-once-and-last"
    trust_by_name = {item["schema"]: item for item in schema_trust}
    evidence = []
    for name in names:
        if name == "pg_temp":
            evidence.append({"schema": name, "trusted": True, "reason": "explicit-last-temp-schema"})
            continue
        item = trust_by_name.get(name)
        if item is None:
            return names, evidence, False, f"unknown-search-path-schema:{name}"
        nonowner_create = bool(item["publicCreate"]) or bool(item["nonOwnerCallableCreateRoles"])
        evidence.append({
            "schema": name,
            "trusted": bool(item["ownerTrusted"]) and not nonowner_create,
            "ownerRole": item["ownerRole"],
            "ownerTrusted": bool(item["ownerTrusted"]),
            "publicCreate": bool(item["publicCreate"]),
            "roleCreate": item["roleCreate"],
            "nonOwnerCallableCreateRoles": item["nonOwnerCallableCreateRoles"],
        })
        if not item["ownerTrusted"]:
            return names, evidence, False, f"untrusted-search-path-schema-owner:{name}"
        if nonowner_create:
            return names, evidence, False, f"nonowner-create-on-search-path-schema:{name}"
    return names, evidence, True, None


def bootstrap_lineage(
    inventory: dict[str, Any], inventory_hash: str,
    baseline_audit_hash: str, catalog: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    public_objects = {
        item["objectKey"]: item for item in inventory["objects"]
        if item["objectType"] in {"function", "procedure"}
        and item["catalog"]["security_definer"]
    }
    privileged = {key: row for key, row in catalog.items() if row["securityDefiner"]}
    missing = sorted(set(public_objects) - set(privileged))
    if missing:
        raise ValueError(f"baseline public privileged routines missing from catalog: {missing}")

    lineages = []
    for key, row in sorted(privileged.items()):
        public = public_objects.get(key)
        origin = "original-public" if public else "native-nonpublic"
        if origin == "native-nonpublic" and row["schema"] == "public":
            raise ValueError(f"unmapped public privileged baseline routine: {key}")
        definition_hash = sha256_text(row["definition"])
        if public and definition_hash != public["catalog"]["definitionSha256"]:
            raise ValueError(f"public baseline definition differs from #353 inventory: {key}")
        lineages.append({
            "lineageKey": lineage_key(key),
            "origin": origin,
            "originalObjectKey": key,
            "originalSchema": row["schema"],
            "originalDefinitionSha256": definition_hash,
            "originalRoutineKind": row["routineKind"],
            "targetSchema": public["targetSchema"] if public else row["schema"],
            "migrationBatch": public["migrationBatch"] if public else f"native-{row['schema']}",
            "lifecycle": "active",
            "birthTransition": None,
            "retirement": None,
            "canonicalObjectKey": key,
            "compatibilityAliases": [],
        })

    contract = {
        "schemaVersion": "database.privileged-routine-lineage.v1",
        "source": {
            "baseline": dict(BASELINE_PROVENANCE),
            "completedTransitions": [],
            "currentTransition": dict(EXPECTED_CURRENT_TRANSITION),
            "issue": "tiangong-lca/database-engine#333",
        },
        "governedSchemas": list(GOVERNED_SCHEMAS),
        "lineages": lineages,
        "contractReady": False,
    }
    validate_lineage(contract, inventory, inventory_hash, baseline_audit_hash)
    return contract


def validate_lineage(
    lineage: dict[str, Any], inventory: dict[str, Any],
    inventory_hash: str, baseline_audit_hash: str,
    *, expected_current_transition: dict[str, Any] | None = None,
    expected_completed_transitions: tuple[dict[str, Any], ...] | None = None,
) -> None:
    if expected_current_transition is None:
        expected_current_transition = EXPECTED_CURRENT_TRANSITION
    if expected_completed_transitions is None:
        expected_completed_transitions = EXPECTED_COMPLETED_TRANSITIONS
    if lineage.get("schemaVersion") != "database.privileged-routine-lineage.v1":
        raise ValueError("unexpected privileged routine lineage schemaVersion")
    if inventory_hash != BASELINE_PROVENANCE["inventorySha256"]:
        raise ValueError("immutable #353 baseline inventory bytes changed")
    if inventory.get("schemaVersion") != BASELINE_PROVENANCE["inventorySchemaVersion"]:
        raise ValueError("immutable #353 baseline inventory schemaVersion changed")
    if baseline_audit_hash != BASELINE_PROVENANCE["auditV1Sha256"]:
        raise ValueError("immutable #333 v1 baseline audit bytes changed")
    source = lineage.get("source", {})
    if source.get("baseline") != BASELINE_PROVENANCE:
        raise ValueError("privileged routine immutable baseline provenance differs")
    if source.get("currentTransition") != expected_current_transition:
        raise ValueError("privileged routine current transition provenance differs from reviewed code")
    if source.get("issue") != "tiangong-lca/database-engine#333":
        raise ValueError("privileged routine lineage Issue differs")
    completed = source.get("completedTransitions")
    if not isinstance(completed, list):
        raise ValueError("privileged routine completed transition history is missing")
    if completed != list(expected_completed_transitions):
        raise ValueError("completed transition history differs from reviewed immutable receipts")
    predecessor = BASELINE_PROVENANCE["auditV1Sha256"]
    for sequence, transition in enumerate(completed):
        if transition.get("sequence") != sequence:
            raise ValueError("privileged routine completed transition sequence is not append-only")
        if transition.get("predecessorArtifactSha256") != predecessor:
            raise ValueError("privileged routine completed transition predecessor chain differs")
        produced = transition.get("producedAuditV2Sha256", "")
        if not re.fullmatch(r"[0-9a-f]{64}", produced):
            raise ValueError("privileged routine completed transition result hash is invalid")
        produced_path = transition.get("producedAuditV2Path", "")
        try:
            produced_raw = read_reviewed_contract_bytes(produced_path)
        except ValueError as exc:
            raise ValueError("privileged routine completed transition audit path is invalid") from exc
        if sha256_bytes(produced_raw) != produced:
            raise ValueError("privileged routine completed transition audit bytes differ")
        predecessor_path = transition.get("predecessorAuditPath", "")
        try:
            predecessor_raw = read_reviewed_contract_bytes(predecessor_path)
        except ValueError as exc:
            raise ValueError("privileged routine completed transition predecessor path is invalid") from exc
        if sha256_bytes(predecessor_raw) != transition["predecessorArtifactSha256"]:
            raise ValueError("privileged routine completed transition predecessor bytes differ")
        receipt_path = transition.get("receiptPath", "")
        receipt_sha = transition.get("receiptSha256", "")
        try:
            raw = read_reviewed_contract_bytes(receipt_path)
        except ValueError as exc:
            raise ValueError("privileged routine completed transition receipt path is invalid") from exc
        if sha256_bytes(raw) != receipt_sha or not re.fullmatch(r"[0-9a-f]{64}", receipt_sha):
            raise ValueError("privileged routine completed transition receipt bytes differ")
        receipt = json.loads(raw)
        expected_receipt = {
            "schemaVersion": "database.security-definer-transition-receipt.v1",
            "transition": {
                field: transition[field] for field in (
                    "sequence", "batch", "databaseSchemaSha", "predecessorArtifactSha256"
                )
            },
            "producedAuditV2Sha256": produced,
            "producedAuditV2Path": produced_path,
            "predecessorAuditPath": predecessor_path,
        }
        if raw != canonical(receipt).encode("utf-8") or receipt != expected_receipt:
            raise ValueError("privileged routine completed transition receipt content differs")
        predecessor = produced
    current = source["currentTransition"]
    if current["sequence"] != len(completed):
        raise ValueError("privileged routine current transition sequence does not follow history")
    if current["predecessorArtifactSha256"] != predecessor:
        raise ValueError("privileged routine current transition predecessor does not follow history")
    if lineage.get("governedSchemas") != list(GOVERNED_SCHEMAS):
        raise ValueError("privileged routine governed schema set differs")
    if lineage.get("contractReady") is not False:
        raise ValueError("privileged routine lineage must remain fail closed before Contract")
    rows = lineage.get("lineages", [])
    lineage_keys = [row.get("lineageKey") for row in rows]
    originals = [row.get("originalObjectKey") for row in rows]
    canonicals = [row.get("canonicalObjectKey") for row in rows if row.get("canonicalObjectKey")]
    aliases = [alias for row in rows for alias in row.get("compatibilityAliases", [])]
    for label, values in (("lineage keys", lineage_keys), ("original keys", originals),
                          ("canonical endpoints", canonicals), ("compatibility aliases", aliases)):
        if len(values) != len(set(values)):
            raise ValueError(f"privileged routine lineage contains duplicate {label}")
    if set(canonicals) & set(aliases):
        raise ValueError("canonical and compatibility endpoint sets overlap")
    for row in rows:
        birth = row.get("birthTransition")
        if birth is None:
            expected_key = lineage_key(row.get("originalObjectKey", ""))
        else:
            expected_key = transition_lineage_key(row.get("originalObjectKey", ""), birth)
        if row.get("lineageKey") != expected_key:
            raise ValueError(f"unstable lineage key: {row.get('originalObjectKey')}")
        if row.get("origin") not in {"original-public", "native-nonpublic", "transition-native"}:
            raise ValueError(f"invalid lineage origin: {row.get('originalObjectKey')}")
        if (row["origin"] == "transition-native") != (birth is not None):
            raise ValueError(f"transition-native lineage birth receipt differs: {row.get('originalObjectKey')}")
        if row.get("originalSchema") not in GOVERNED_SCHEMAS or row.get("targetSchema") not in GOVERNED_SCHEMAS:
            raise ValueError(f"invalid lineage schema: {row.get('originalObjectKey')}")
        if not re.fullmatch(r"[0-9a-f]{64}", row.get("originalDefinitionSha256", "")):
            raise ValueError(f"invalid original definition hash: {row.get('originalObjectKey')}")
        if row.get("originalRoutineKind") not in {"function", "procedure"}:
            raise ValueError(f"invalid original routine kind: {row.get('originalObjectKey')}")
        if birth is not None:
            validate_lifecycle_receipt("registration", row, birth, source)
        lifecycle = row.get("lifecycle")
        if lifecycle == "active":
            if not row.get("canonicalObjectKey") or row.get("retirement") is not None:
                raise ValueError(f"active lineage canonical/retirement differs: {row.get('originalObjectKey')}")
        elif lifecycle == "retired":
            if row.get("canonicalObjectKey") is not None or row.get("compatibilityAliases") != []:
                raise ValueError(f"retired lineage retains endpoint: {row.get('originalObjectKey')}")
            retirement = row.get("retirement")
            if not isinstance(retirement, dict):
                raise ValueError(f"retired lineage lacks reviewed receipt: {row.get('originalObjectKey')}")
            for field in ("consumerZeroEvidenceSha256", "ownerEvidenceSha256"):
                if not re.fullmatch(r"[0-9a-f]{64}", retirement.get(field, "")):
                    raise ValueError(f"retired lineage {field} is invalid: {row.get('originalObjectKey')}")
            endpoints = retirement.get("retiredEndpointKeys")
            if not isinstance(endpoints, list) or not endpoints or len(endpoints) != len(set(endpoints)):
                raise ValueError(f"retired lineage endpoint closure is invalid: {row.get('originalObjectKey')}")
            validate_lifecycle_receipt("retirement", row, retirement, source)
        else:
            raise ValueError(f"invalid lineage lifecycle: {row.get('originalObjectKey')}")
    baseline_public = {
        item["objectKey"] for item in inventory["objects"]
        if item["objectType"] in {"function", "procedure"} and item["catalog"]["security_definer"]
    }
    represented_public = {row["originalObjectKey"] for row in rows if row["origin"] == "original-public"}
    if represented_public != baseline_public:
        raise ValueError("lineage original-public set differs from immutable #353 inventory")
    genesis = [row for row in rows if row.get("birthTransition") is None]
    if original_lineage_snapshot(genesis) != BASELINE_PROVENANCE["lineageSnapshotSha256"]:
        raise ValueError("immutable original privileged lineage snapshot differs")


def validate_lifecycle_receipt(
    kind: str, row: dict[str, Any], reference: dict[str, Any], source: dict[str, Any],
) -> None:
    transitions = [*source["completedTransitions"], source["currentTransition"]]
    identity = {field: reference.get(field) for field in (
        "sequence", "batch", "databaseSchemaSha", "predecessorArtifactSha256",
    )}
    if not any(all(transition.get(field) == value for field, value in identity.items()) for transition in transitions):
        raise ValueError(f"lineage {kind} transition is not in reviewed history")
    receipt_path = reference.get("receiptPath", "")
    receipt_sha = reference.get("receiptSha256", "")
    try:
        raw = read_reviewed_contract_bytes(receipt_path)
    except ValueError as exc:
        raise ValueError(f"lineage {kind} receipt path is invalid") from exc
    if sha256_bytes(raw) != receipt_sha:
        raise ValueError(f"lineage {kind} receipt bytes differ")
    receipt = json.loads(raw)
    if raw != canonical(receipt).encode("utf-8"):
        raise ValueError(f"lineage {kind} receipt is not canonical")
    if receipt.get("schemaVersion") != f"database.privileged-routine-{kind}-receipt.v1":
        raise ValueError(f"lineage {kind} receipt schemaVersion differs")
    if receipt.get("lineageKey") != row["lineageKey"] or receipt.get("transition") != identity:
        raise ValueError(f"lineage {kind} receipt identity differs")
    if kind == "registration":
        expected_original = {
            "objectKey": row["originalObjectKey"],
            "schema": row["originalSchema"],
            "definitionSha256": row["originalDefinitionSha256"],
            "routineKind": row["originalRoutineKind"],
        }
        if receipt.get("original") != expected_original:
            raise ValueError("lineage registration receipt original identity differs")
        if receipt.get("targetSchema") != row["targetSchema"]:
            raise ValueError("lineage registration receipt target schema differs")
        if receipt.get("migrationBatch") != row["migrationBatch"]:
            raise ValueError("lineage registration receipt migration batch differs")
    if kind == "retirement":
        for field in ("consumerZeroEvidenceSha256", "ownerEvidenceSha256", "retiredEndpointKeys"):
            if receipt.get(field) != reference[field]:
                raise ValueError(f"lineage retirement receipt {field} differs")
        predecessor_path = reference.get("predecessorAuditPath", "")
        if receipt.get("predecessorAuditPath") != predecessor_path:
            raise ValueError("lineage retirement predecessor audit path differs from receipt")
        try:
            predecessor_raw = read_reviewed_contract_bytes(predecessor_path)
        except ValueError as exc:
            raise ValueError("lineage retirement predecessor audit path is invalid") from exc
        if sha256_bytes(predecessor_raw) != identity["predecessorArtifactSha256"]:
            raise ValueError("lineage retirement predecessor audit bytes differ")
        predecessor_audit = json.loads(predecessor_raw)
        if predecessor_raw != canonical(predecessor_audit).encode("utf-8"):
            raise ValueError("lineage retirement predecessor audit is not canonical")
        predecessor_rows = [item for item in predecessor_audit.get("routines", [])
                            if item.get("lineageKey") == row["lineageKey"]]
        if len(predecessor_rows) != 1 or predecessor_rows[0].get("lifecycle", "active") != "active":
            raise ValueError("lineage retirement predecessor state is not uniquely active")
        predecessor_row = predecessor_rows[0]
        canonical_endpoint = predecessor_row.get("canonical")
        if not isinstance(canonical_endpoint, dict):
            raise ValueError("lineage retirement predecessor canonical endpoint is missing")
        derived = [canonical_endpoint["currentObjectKey"], *[
            alias["currentObjectKey"] for alias in predecessor_row.get("compatibilityAliases", [])
        ]]
        if reference["retiredEndpointKeys"] != derived:
            raise ValueError("lineage retirement endpoint closure differs from predecessor audit")


def validate_transition_fixture(fixture: dict[str, Any], baseline_lineage_hash: str) -> None:
    if fixture.get("schemaVersion") != "database.security-definer-transition-fixture.v1":
        raise ValueError("unexpected SECURITY DEFINER transition fixture schemaVersion")
    source = fixture.get("source", {})
    expected = {
        "issue": "tiangong-lca/database-engine#356",
        "exactBaseDatabaseCommitSha": "597072ca34a62cdc93df9bf0896a9d361901852c",
        "baselineDatabaseSchemaSha": BASELINE_PROVENANCE["databaseSchemaSha"],
        "baselineLineageSha256": baseline_lineage_hash,
        "migrationVersion": "20260801060304",
        "expectedSourceClosure": {
            "lineageCount": 315,
            "originalPublicLineageCount": 241,
            "nativeNonPublicLineageCount": 74,
            "privilegedSchemaMoves": 11,
            "compositeSignatureMoves": 1,
            "compatibilityAliases": 23,
        },
    }
    for field, value in expected.items():
        if source.get(field) != value:
            raise ValueError(f"transition fixture source {field} differs")
    if not re.fullmatch(r"[0-9a-f]{64}", source.get("migrationSha256", "")):
        raise ValueError("transition fixture migration bytes hash is invalid")
    migration_receipt = CONTRACT_DIR / "security_definer_transition_fixture.v1.migration.sql"
    if sha256_bytes(migration_receipt.read_bytes()) != source["migrationSha256"]:
        raise ValueError("transition fixture migration receipt bytes differ")
    moves = fixture.get("moves", [])
    if len(moves) != 12 or sum(len(row.get("compatibilityAliases", [])) for row in moves) != 23:
        raise ValueError("transition fixture move/alias closure differs")


def authorization_signals(definition: str) -> list[str]:
    patterns = (
        ("auth-uid", r"\bauth\s*\.\s*uid\s*\("),
        ("auth-role", r"\bauth\s*\.\s*role\s*\("),
        ("jwt-claims", r"request\.jwt\.claims|current_setting\s*\(\s*['\"]request\.jwt"),
        ("session-identity", r"\b(current_user|current_role|session_user)\b"),
        ("service-role-literal", r"['\"]service_role['\"]"),
        ("row-security-setting", r"\brow_security\b"),
        ("dynamic-sql", r"\bexecute\b|\bregclass\b|\bregprocedure\b"),
    )
    lowered = definition.lower()
    return sorted(name for name, pattern in patterns if re.search(pattern, lowered)) or ["none-observed"]


def postgrest_shape(
    row: dict[str, Any], catalog: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Model PostgREST v14.7 schema-cache and overload resolution separately."""
    input_types = [str(value) for value in row.get("inputArgumentTypes", [])]
    input_type_kinds = [str(value) for value in row.get("inputArgumentTypeKinds", [])]
    input_names = [str(value) for value in row.get("inputArgumentNames", [])]
    input_required = [bool(value) for value in row.get("inputArgumentRequired", [])]
    result_name = str(row.get("returnTypeName", row.get("resultType", ""))).lower()
    output_count = int(row.get("outputArgumentCount", 0))
    if (len(input_types) != len(input_names) or len(input_types) != len(input_required)
            or len(input_types) != len(input_type_kinds)):
        raise ValueError(f"PostgREST argument evidence is misaligned: {row['objectKey']}")

    unnamed_count = sum(not name for name in input_names)
    raw_media_by_type = {
        "bytea": "application/octet-stream",
        "json": "application/json",
        "jsonb": "application/json",
        "text": "text/plain",
        "xml": "text/xml",
    }
    schema_cache_reason: str | None = None
    if row["routineKind"] != "function":
        schema_cache_reason = "procedure-not-in-postgrest-schema-cache"
    elif result_name == "trigger":
        schema_cache_reason = "trigger-excluded-from-postgrest-schema-cache"
    elif unnamed_count == 1 and (not input_types or input_types[0].lower() not in raw_media_by_type):
        # This intentionally follows the v14.7 schema-cache SQL, including its
        # first-input-type test for the one-unnamed-argument case.
        schema_cache_reason = "postgrest-schema-cache-rejects-unnamed-argument-shape"
    elif unnamed_count > 1:
        schema_cache_reason = "postgrest-schema-cache-rejects-multiple-unnamed-arguments"
    schema_cache_eligible = schema_cache_reason is None

    if result_name == "event_trigger":
        direct_reason = "event-trigger-requires-trigger-context"
    elif result_name == "internal":
        direct_reason = "internal-pseudo-type-is-not-sql-invocable"
    elif any(kind == "p" for kind in input_type_kinds):
        direct_reason = "pseudo-type-input-is-not-postgrest-materializable"
    else:
        direct_reason = None
    direct_supported = schema_cache_eligible and direct_reason is None

    def candidate_cache_eligible(candidate: dict[str, Any]) -> bool:
        candidate_names = [str(value) for value in candidate.get("inputArgumentNames", [])]
        candidate_types = [str(value) for value in candidate.get("inputArgumentTypes", [])]
        unnamed = sum(not name for name in candidate_names)
        candidate_result = str(candidate.get("returnTypeName", candidate.get("resultType", ""))).lower()
        return (
            candidate.get("routineKind") == "function"
            and candidate_result != "trigger"
            and unnamed <= 1
            and not (
                unnamed == 1
                and (not candidate_types or candidate_types[0].lower() not in raw_media_by_type)
            )
        )

    overloads = [
        candidate for candidate in catalog.values()
        if candidate["schema"] == row["schema"]
        and candidate["name"] == row["name"]
        and candidate_cache_eligible(candidate)
    ]

    def named_domain(candidate: dict[str, Any]) -> tuple[frozenset[str], frozenset[str]] | None:
        names = [str(value) for value in candidate.get("inputArgumentNames", [])]
        required_flags = [bool(value) for value in candidate.get("inputArgumentRequired", [])]
        if len(names) != len(required_flags) or any(not name for name in names):
            return None
        return (
            frozenset(name for name, required in zip(names, required_flags) if required),
            frozenset(names),
        )

    ambiguous_sets: list[list[str]] = []
    overload_keys: set[str] = set()
    unambiguous_key_set: list[str] | None = None
    raw_media_types: list[str] = []
    request_reason: str | None = None
    if not schema_cache_eligible:
        request_reason = "not-in-postgrest-schema-cache"
    elif unnamed_count == 0:
        own_domain = named_domain(row)
        if own_domain is None:
            raise ValueError(f"named PostgREST argument evidence is invalid: {row['objectKey']}")
        required_keys, all_keys = own_domain
        clauses: list[list[tuple[str, bool]]] = []
        examples: set[tuple[str, ...]] = set()
        for competitor in overloads:
            if competitor["objectKey"] == row["objectKey"]:
                continue
            domain = named_domain(competitor)
            if domain is None:
                continue
            competitor_required, competitor_all = domain
            intersection_required = required_keys | competitor_required
            intersection_all = all_keys & competitor_all
            if not intersection_required <= intersection_all:
                continue
            overload_keys.add(competitor["objectKey"])
            examples.add(tuple(sorted(intersection_required)))
            # A candidate request avoids this competitor by omitting at least
            # one of its additional required keys or including a key it does
            # not accept.  The resulting CNF is solved symbolically instead of
            # enumerating the candidate's 2^N optional-key powerset.
            clauses.append([
                *((key, False) for key in sorted(competitor_required - required_keys)),
                *((key, True) for key in sorted(all_keys - competitor_all)),
            ])
        ambiguous_sets = [list(value) for value in sorted(examples, key=lambda value: (len(value), value))[:16]]

        search_steps = 0

        def solve(
            remaining: list[list[tuple[str, bool]]], assignment: dict[str, bool],
        ) -> dict[str, bool] | None:
            nonlocal search_steps
            search_steps += 1
            if search_steps > 10_000:
                raise RuntimeError("PostgREST overload resolution proof exceeded its complexity budget")
            simplified: list[list[tuple[str, bool]]] = []
            for clause in remaining:
                if any(assignment.get(variable) is value for variable, value in clause):
                    continue
                unresolved = [literal for literal in clause if literal[0] not in assignment]
                if not unresolved:
                    return None
                simplified.append(unresolved)
            if not simplified:
                return assignment
            unit = next((clause[0] for clause in simplified if len(clause) == 1), None)
            if unit is not None:
                variable, value = unit
                return solve(simplified, {**assignment, variable: value})
            frequency = Counter(variable for clause in simplified for variable, _value in clause)
            variable = min(
                (key for key, count in frequency.items() if count == max(frequency.values())),
            )
            for value in (False, True):
                if result := solve(simplified, {**assignment, variable: value}):
                    return result
            return None

        try:
            witness = solve(clauses, {})
        except RuntimeError:
            request_reason = "overload-resolution-complexity-budget-exceeded"
        else:
            if witness is not None:
                unambiguous_key_set = sorted(
                    required_keys | {key for key, selected in witness.items() if selected}
                )
        if unambiguous_key_set is None and request_reason is None:
            request_reason = "all-named-request-shapes-are-ambiguous"
    elif len(input_names) == 1:
        media = raw_media_by_type[input_types[0].lower()]
        raw_media_types = [media]
        competitors = [
            candidate for candidate in overloads
            if candidate["objectKey"] != row["objectKey"]
            and len(candidate.get("inputArgumentNames", [])) == 1
            and len(candidate.get("inputArgumentTypes", [])) == 1
            and not str(candidate["inputArgumentNames"][0])
            and raw_media_by_type.get(str(candidate["inputArgumentTypes"][0]).lower()) == media
        ]
        overload_keys.update(candidate["objectKey"] for candidate in competitors)
        if competitors:
            ambiguous_sets = [["<raw-body>"]]
            request_reason = "ambiguous-single-unnamed-argument-fallback"
        else:
            unambiguous_key_set = ["<raw-body>"]
    else:
        request_reason = "mixed-named-and-unnamed-parameters-not-resolvable"

    request_resolvable = request_reason is None
    eligible = schema_cache_eligible and request_resolvable and direct_supported
    reason = schema_cache_reason or request_reason or direct_reason
    return {
        "modelVersion": "postgrest-v14.7-schema-cache-and-find-proc",
        "eligible": eligible,
        "reason": reason,
        "schemaCacheEligible": schema_cache_eligible,
        "schemaCacheReason": schema_cache_reason,
        "requestResolvable": request_resolvable,
        "requestResolutionReason": request_reason,
        "directInvocationSupported": direct_supported,
        "directInvocationReason": direct_reason,
        "returnTypeName": row.get("returnTypeName", row.get("resultType", "")),
        "returnTypeKind": row.get("returnTypeKind", "unknown"),
        "inputArgumentNames": input_names,
        "inputArgumentTypes": input_types,
        "inputArgumentTypeKinds": input_type_kinds,
        "inputArgumentRequired": input_required,
        "argumentModes": row.get("argumentModes", []),
        "outputArgumentCount": output_count,
        "rawBodyMediaTypes": raw_media_types,
        "unambiguousRequestKeySet": unambiguous_key_set,
        "hasAmbiguousRequestShape": bool(overload_keys),
        "ambiguousRequestKeySetExamples": ambiguous_sets,
        "ambiguousOverloadObjectKeys": sorted(overload_keys),
    }


def role_matrix(
    row: dict[str, Any], exposed: list[str] | set[str], shape: dict[str, Any],
) -> list[dict[str, Any]]:
    direct = set(row["directExecuteGrants"])
    named = {item["role"]: item for item in row["namedRoleMatrix"]}
    result = []
    for role in ROLES:
        sources = sorted(source for source in direct if source in {role, "PUBLIC"})
        if role == "PUBLIC":
            schema_usage = bool(row["publicSchemaUsage"])
            execute = "PUBLIC" in direct
        else:
            if role not in named:
                raise ValueError(f"required database role missing from catalog evidence: {role}")
            schema_usage = bool(named[role]["schemaUsage"])
            execute = bool(named[role]["execute"])
        authenticator_can_set_role = (
            bool(named[role]["postgrestAuthenticatorCanSetRole"])
            if role != "PUBLIC" else False
        )
        effective = schema_usage and execute
        transport_role = (
            role in {"anon", "authenticated", "service_role"}
            and authenticator_can_set_role
        )
        postgrest_routine = row["routineKind"] == "function"
        result.append({
            "role": role,
            "directGrantSources": sources,
            "effectiveSchemaUsage": schema_usage,
            "effectiveExecute": execute,
            "effectiveCallable": effective,
            "dataApiExposedSchema": row["schema"] in exposed,
            "dataApiTransportRole": transport_role,
            "postgrestAuthenticatorCanSetRole": authenticator_can_set_role,
            "postgrestRoutineKind": postgrest_routine,
            "postgrestSchemaCacheEndpoint": bool(shape["schemaCacheEligible"]),
            "postgrestRequestResolvable": bool(shape["requestResolvable"]),
            "postgrestDirectInvocationSupported": bool(shape["directInvocationSupported"]),
            "postgrestCallableShape": bool(shape["eligible"]),
            "effectiveDataApiEndpoint": (
                effective and row["schema"] in exposed and transport_role
                and bool(shape["schemaCacheEligible"])
            ),
            "effectiveDataApiCallable": (
                effective and row["schema"] in exposed and transport_role and bool(shape["eligible"])
            ),
        })
    return result


def observed_endpoint(
    row: dict[str, Any], exposed: set[str], endpoint_role: str,
    catalog: dict[str, dict[str, Any]], *, grandfathered_definition: bool = False,
) -> dict[str, Any]:
    for caller in row.get("effectiveCallerMatrix", []):
        if not isinstance(caller.get("postgrestAuthenticatorCanSetRole"), bool):
            raise ValueError(
                f"effective caller lacks PostgREST SET ROLE evidence: {row['objectKey']}"
            )
    endpoint = endpoint_properties(row, grandfathered_definition=grandfathered_definition)
    shape = postgrest_shape(row, catalog)
    endpoint.update({
        "endpointRole": endpoint_role,
        "databaseOwnerRole": row["databaseOwnerRole"],
        "directExecuteGrants": row["directExecuteGrants"],
        "effectiveCallerMatrix": row["effectiveCallerMatrix"],
        "postgrestShape": shape,
        "authorizationSignals": authorization_signals(row["definition"]),
        "roleMatrix": role_matrix(row, exposed, shape),
    })
    return endpoint


def build_audit(
    lineage: dict[str, Any], lineage_hash: str,
    baseline_audit: dict[str, Any], catalog: dict[str, dict[str, Any]],
    exposed: set[str], *, validate_result: bool = True,
) -> dict[str, Any]:
    baseline_by_key = {row["objectKey"]: row for row in baseline_audit["routines"]}
    routines = []
    claimed: set[str] = set()
    for mapping in lineage["lineages"]:
        baseline = baseline_by_key.get(mapping["originalObjectKey"])
        baseline_evidence = {
            "cohort": baseline["cohort"],
            "consumerClosure": baseline["observed"]["consumerClosure"],
            "ownerRuntimeConfirmed": baseline["confirmed"]["ownerRuntime"],
            "requiredRoleMatrix": baseline["required"]["roleMatrix"],
        } if baseline else {
            "cohort": "native-nonpublic",
            "consumerClosure": "not-part-of-original-public-inventory",
            "ownerRuntimeConfirmed": False,
            "requiredRoleMatrix": "independent-native-nonpublic-review-required",
        }
        if mapping["lifecycle"] == "retired":
            residue = sorted(set(mapping["retirement"]["retiredEndpointKeys"]) & set(catalog))
            if residue:
                raise ValueError(f"retired lineage endpoints remain in catalog: {residue}")
            routines.append({
                "lineageKey": mapping["lineageKey"],
                "origin": mapping["origin"],
                "originalObjectKey": mapping["originalObjectKey"],
                "originalSchema": mapping["originalSchema"],
                "originalDefinitionSha256": mapping["originalDefinitionSha256"],
                "originalRoutineKind": mapping["originalRoutineKind"],
                "targetSchema": mapping["targetSchema"],
                "migrationBatch": mapping["migrationBatch"],
                "lifecycle": "retired",
                "canonical": None,
                "compatibilityAliases": [],
                "retirement": mapping["retirement"],
                "baselineEvidence": baseline_evidence,
            })
            continue
        canonical_key = mapping["canonicalObjectKey"]
        if canonical_key not in catalog:
            raise ValueError(f"canonical privileged endpoint missing: {canonical_key}")
        canonical_row = catalog[canonical_key]
        if not canonical_row["securityDefiner"]:
            raise ValueError(f"canonical endpoint is not SECURITY DEFINER: {canonical_key}")
        grandfathered_definition = (
            mapping["origin"] in {"original-public", "native-nonpublic"}
            and sha256_text(canonical_row["definition"]) == mapping["originalDefinitionSha256"]
        )
        canonical_properties = endpoint_properties(
            canonical_row, grandfathered_definition=grandfathered_definition,
        )
        if (not canonical_properties["safeSearchPath"]
                and not canonical_properties["grandfatheredSearchPathResidue"]):
            raise ValueError(
                f"canonical privileged endpoint has unsafe search_path: {canonical_key}: "
                f"{canonical_properties['unsafeSearchPathReason']}"
            )
        alias_rows = []
        for alias_key in mapping["compatibilityAliases"]:
            if alias_key not in catalog:
                raise ValueError(f"compatibility alias endpoint missing: {alias_key}")
            if catalog[alias_key]["securityDefiner"]:
                raise ValueError(f"compatibility alias must be SECURITY INVOKER: {alias_key}")
            alias_properties = endpoint_properties(catalog[alias_key])
            if not alias_properties["safeSearchPath"]:
                raise ValueError(
                    f"compatibility alias endpoint has unsafe search_path: {alias_key}: "
                    f"{alias_properties['unsafeSearchPathReason']}"
                )
            alias_rows.append(catalog[alias_key])
        claimed.add(canonical_key)
        claimed.update(mapping["compatibilityAliases"])
        routines.append({
            "lineageKey": mapping["lineageKey"],
            "origin": mapping["origin"],
            "originalObjectKey": mapping["originalObjectKey"],
            "originalSchema": mapping["originalSchema"],
            "originalDefinitionSha256": mapping["originalDefinitionSha256"],
            "originalRoutineKind": mapping["originalRoutineKind"],
            "targetSchema": mapping["targetSchema"],
            "migrationBatch": mapping["migrationBatch"],
            "lifecycle": "active",
            "canonical": observed_endpoint(
                canonical_row, exposed, "canonical", catalog,
                grandfathered_definition=grandfathered_definition,
            ),
            "compatibilityAliases": [
                observed_endpoint(row, exposed, "compatibility-alias", catalog) for row in alias_rows
            ],
            "retirement": None,
            "baselineEvidence": baseline_evidence,
        })
    routines.sort(key=lambda row: row["lineageKey"])

    privileged = {key for key, row in catalog.items() if row["securityDefiner"]}
    claimed_privileged = {key for key in claimed if catalog[key]["securityDefiner"]}
    unregistered = sorted(privileged - claimed_privileged)
    if unregistered:
        raise ValueError(f"unregistered governed SECURITY DEFINER endpoints: {unregistered}")
    multiply_claimed = len(claimed) != sum(
        1 + len(row["compatibilityAliases"])
        for row in lineage["lineages"] if row["lifecycle"] == "active"
    )
    if multiply_claimed:
        raise ValueError("governed routine endpoint is claimed by multiple lineages")

    active = [row for row in routines if row["lifecycle"] == "active"]
    canonical_schema_counts = Counter(row["canonical"]["currentSchema"] for row in active)
    privileged_schema_counts = Counter(catalog[key]["schema"] for key in privileged)
    compatibility_count = sum(len(row["compatibilityAliases"]) for row in routines)
    privileged_compatibility_count = sum(
        alias["securityDefiner"] for row in routines for alias in row["compatibilityAliases"]
    )
    privileged_endpoints = [row["canonical"] for row in active]
    grandfathered_search_path_residue = sum(
        endpoint["grandfatheredSearchPathResidue"] for endpoint in privileged_endpoints
    )
    summary = {
        "lineageCount": len(routines),
        "activeLineageCount": len(active),
        "retiredLineageCount": len(routines) - len(active),
        "originalPublicLineageCount": sum(row["origin"] == "original-public" for row in routines),
        "genesisNativeNonPublicLineageCount": sum(row["origin"] == "native-nonpublic" for row in routines),
        "transitionNativeLineageCount": sum(row["origin"] == "transition-native" for row in routines),
        "canonicalPrivilegedEndpointCount": len(active),
        "compatibilityEndpointCount": compatibility_count,
        "privilegedCompatibilityEndpointCount": privileged_compatibility_count,
        "globalPrivilegedEndpointCount": len(privileged),
        "unregisteredPrivilegedEndpointCount": 0,
        "unsafeSearchPathEndpointCount": sum(
            not endpoint["safeSearchPath"] for endpoint in privileged_endpoints
        ),
        "explicitTrustedSearchPathEndpointCount": (
            len(privileged_endpoints) - grandfathered_search_path_residue
        ),
        "grandfatheredSearchPathResidueCount": grandfathered_search_path_residue,
        "searchPathContractReady": grandfathered_search_path_residue == 0,
        "canonicalPrivilegedBySchema": {
            schema: canonical_schema_counts.get(schema, 0) for schema in GOVERNED_SCHEMAS
        },
        "globalPrivilegedBySchema": {
            schema: privileged_schema_counts.get(schema, 0) for schema in GOVERNED_SCHEMAS
        },
        "issue333OwnerRuntimeResidue": sum(
            row["baselineEvidence"]["cohort"] == "issue333-owner-runtime-residue" for row in routines
        ),
    }
    audit = {
        "schemaVersion": "database.security-definer-audit.v2",
        "source": {
            "lineageSchemaVersion": lineage["schemaVersion"],
            "lineageSha256": lineage_hash,
            "baseline": lineage["source"]["baseline"],
            "completedTransitions": lineage["source"]["completedTransitions"],
            "currentTransition": lineage["source"]["currentTransition"],
            "issue": "tiangong-lca/database-engine#333",
        },
        "governedSchemas": list(GOVERNED_SCHEMAS),
        "exposedSchemas": list(exposed),
        "summary": summary,
        "auditArtifactComplete": True,
        "contractReady": False,
        "routines": routines,
    }
    if validate_result:
        validate_audit(audit, lineage, lineage_hash, baseline_audit, catalog, exposed)
    return audit


def validate_audit(
    audit: dict[str, Any], lineage: dict[str, Any], lineage_hash: str,
    baseline_audit: dict[str, Any], catalog: dict[str, dict[str, Any]], exposed: set[str],
) -> None:
    if audit.get("schemaVersion") != "database.security-definer-audit.v2":
        raise ValueError("unexpected lineage-aware audit schemaVersion")
    source = audit.get("source", {})
    if source.get("lineageSha256") != lineage_hash:
        raise ValueError("audit is not bound to exact committed lineage bytes")
    for field in ("baseline", "completedTransitions", "currentTransition"):
        if source.get(field) != lineage["source"][field]:
            raise ValueError(f"audit {field} differs from lineage provenance")
    if audit.get("governedSchemas") != list(GOVERNED_SCHEMAS):
        raise ValueError("audit governed schema set differs")
    if audit.get("exposedSchemas") != list(exposed):
        raise ValueError("audit exposed schema set differs from config.toml")
    if audit.get("auditArtifactComplete") is not True or audit.get("contractReady") is not False:
        raise ValueError("lineage-aware audit must be complete but fail closed before Contract")
    expected = build_audit(
        lineage, lineage_hash, baseline_audit, catalog, exposed, validate_result=False,
    )
    if audit != expected:
        raise ValueError("lineage-aware audit differs from exact lineage/catalog derivation")


def require_zero_search_path_residue(audit: dict[str, Any]) -> None:
    summary = audit["summary"]
    if (summary["grandfatheredSearchPathResidueCount"] != 0
            or summary["unsafeSearchPathEndpointCount"] != 0
            or summary["searchPathContractReady"] is not True):
        raise ValueError(
            "search_path Contract requires grandfatheredSearchPathResidueCount=0, "
            "unsafeSearchPathEndpointCount=0, and searchPathContractReady=true"
        )


def write_hashed_json(path: Path, digest_path: Path, value: dict[str, Any]) -> str:
    rendered = canonical(value)
    digest = sha256_text(rendered)
    path.write_text(rendered, encoding="utf-8")
    digest_path.write_text(digest + "\n", encoding="utf-8")
    return digest


def transition_advance_plan(
    lineage: dict[str, Any], produced_audit_sha: str, *, batch: str, database_schema_sha: str,
) -> dict[str, Any]:
    """Return the exact append-only source/receipt plan for the next migration batch."""
    committed_sha = SHA.read_text(encoding="utf-8").strip()
    if produced_audit_sha != committed_sha:
        raise ValueError("transition advance requires the exact live-verified committed audit SHA")
    if not re.fullmatch(r"[0-9a-f]{40}", database_schema_sha):
        raise ValueError("next transition database schema SHA must be an exact 40-hex commit")
    if not batch or not re.fullmatch(r"[a-z0-9][a-z0-9-]*", batch):
        raise ValueError("next transition batch must be a stable lowercase slug")
    current = dict(lineage["source"]["currentTransition"])
    sequence = current["sequence"]
    frozen_name = f"security_definer_audit_v2.transition-{sequence:03d}-{current['batch']}.json"
    frozen_path = f"supabase/tests/contracts/{frozen_name}"
    predecessor_path = (
        "supabase/tests/contracts/security_definer_audit.json" if sequence == 0
        else lineage["source"]["completedTransitions"][-1]["producedAuditV2Path"]
    )
    completed_without_receipt = {
        **current,
        "predecessorAuditPath": predecessor_path,
        "producedAuditV2Sha256": produced_audit_sha,
        "producedAuditV2Path": frozen_path,
    }
    receipt = {
        "schemaVersion": "database.security-definer-transition-receipt.v1",
        "transition": current,
        "predecessorAuditPath": predecessor_path,
        "producedAuditV2Sha256": produced_audit_sha,
        "producedAuditV2Path": frozen_path,
    }
    receipt_name = f"security_definer_transition_receipt.{sequence:03d}-{current['batch']}.json"
    receipt_path = f"supabase/tests/contracts/{receipt_name}"
    completed = {
        **completed_without_receipt,
        "receiptPath": receipt_path,
        "receiptSha256": sha256_text(canonical(receipt)),
    }
    next_transition = {
        "sequence": sequence + 1,
        "batch": batch,
        "databaseSchemaSha": database_schema_sha,
        "predecessorArtifactSha256": produced_audit_sha,
    }
    return {
        "requiredImmutableCopy": {
            "from": "supabase/tests/contracts/security_definer_audit_v2.json",
            "to": frozen_path,
            "sha256": produced_audit_sha,
        },
        "receiptPath": receipt_path,
        "receipt": receipt,
        "completedTransition": completed,
        "currentTransition": next_transition,
        "reviewedCodeConstants": {
            "EXPECTED_COMPLETED_TRANSITIONSAppend": completed,
            "EXPECTED_CURRENT_TRANSITION": next_transition,
        },
    }


def verify_committed(
    inventory: dict[str, Any], inventory_hash: str,
    baseline_audit: dict[str, Any], baseline_audit_hash: str,
    catalog: dict[str, dict[str, Any]], exposed: set[str],
) -> dict[str, Any]:
    lineage, lineage_hash = read_hashed_json(LINEAGE, LINEAGE_SHA)
    validate_lineage(lineage, inventory, inventory_hash, baseline_audit_hash)
    audit, _ = read_hashed_json(OUT, SHA)
    validate_audit(audit, lineage, lineage_hash, baseline_audit, catalog, exposed)
    return audit


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--bootstrap-write", action="store_true")
    group.add_argument("--write", action="store_true")
    group.add_argument("--check", action="store_true")
    group.add_argument("--plan-transition-advance", action="store_true")
    parser.add_argument("--batch")
    parser.add_argument("--database-schema-sha")
    parser.add_argument("--require-search-path-residue-zero", action="store_true")
    args = parser.parse_args()

    inventory, inventory_hash = read_hashed_json(INVENTORY, INVENTORY_SHA)
    baseline_audit, baseline_audit_hash = read_hashed_json(BASELINE_AUDIT, BASELINE_AUDIT_SHA)
    if args.plan_transition_advance:
        lineage, _ = read_hashed_json(LINEAGE, LINEAGE_SHA)
        catalog = load_catalog()
        exposed = exposed_schemas()
        audit_artifact = verify_committed(
            inventory, inventory_hash, baseline_audit, baseline_audit_hash, catalog, exposed,
        )
        _, produced_sha = read_hashed_json(OUT, SHA)
        if audit_artifact["source"]["currentTransition"] != lineage["source"]["currentTransition"]:
            raise ValueError("cannot advance a lineage whose current audit source differs")
        print(canonical(transition_advance_plan(
            lineage, produced_sha, batch=args.batch or "", database_schema_sha=args.database_schema_sha or "",
        )), end="")
        return 0
    catalog = load_catalog()
    exposed = exposed_schemas()
    if args.bootstrap_write:
        lineage = bootstrap_lineage(inventory, inventory_hash, baseline_audit_hash, catalog)
        lineage_hash = write_hashed_json(LINEAGE, LINEAGE_SHA, lineage)
        audit = build_audit(lineage, lineage_hash, baseline_audit, catalog, exposed)
        digest = write_hashed_json(OUT, SHA, audit)
    elif args.write:
        lineage, lineage_hash = read_hashed_json(LINEAGE, LINEAGE_SHA)
        validate_lineage(lineage, inventory, inventory_hash, baseline_audit_hash)
        audit = build_audit(lineage, lineage_hash, baseline_audit, catalog, exposed)
        digest = write_hashed_json(OUT, SHA, audit)
    else:
        audit = verify_committed(
            inventory, inventory_hash, baseline_audit, baseline_audit_hash, catalog, exposed,
        )
        digest = SHA.read_text(encoding="utf-8").strip()
    if args.require_search_path_residue_zero:
        require_zero_search_path_residue(audit)
    print(json.dumps({"summary": audit["summary"], "sha256": digest}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
