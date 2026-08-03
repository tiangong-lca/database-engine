#!/usr/bin/env python3
"""Non-authorizing Issue #390 physical-move qualification harness.

The v1 contract permits offline validation and a read-only loopback baseline
capture.  Destructive qualification remains wired but fail closed until the
reviewed pre-DDL contract, candidate bindings, and this plan all authorize it.
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import re
import subprocess
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

import jsonschema

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "supabase/tests/contracts/lca_result_family_physical_qualification.v1.json"
SCHEMA_PATH = ROOT / "supabase/tests/contracts/lca_result_family_physical_qualification.v1.schema.json"
SHA_PATH = ROOT / "supabase/tests/contracts/lca_result_family_physical_qualification.v1.sha256"
EXPECTED_SOURCE_COMMIT = "a29f26a9eb0a6c629ae34e187c6fce4a0c215b1d"
EXPECTED_MIGRATION_HEAD = "20260803090000"
EXPECTED_MIGRATION_COUNT = 181
EXPECTED_MIGRATION_SET_SHA256 = "660edebc4e24e230e3223ba76466660805d0c66d102c9fa3e1a2d76850a6c964"
MAX_BASELINE_ROWS_PER_RELATION = 100_000
BASELINE_STATEMENT_TIMEOUT_SECONDS = 120
ALLOWED_CAPTURE_ROLES = {"postgres", "supabase_admin"}
EXPECTED_RELATIONS = [
    "public.lca_results",
    "public.lca_result_cache",
    "public.lca_latest_all_unit_results",
    "public.lca_factorization_registry",
]
EXPECTED_ROUTINES = [
    "public.lca_read_job_projection(uuid,uuid,uuid,boolean)",
    "public.lca_read_result_projection(uuid,uuid,text,boolean)",
    "public.lca_read_latest_single_solve_result(uuid,uuid,integer)",
]
AUTHORIZATION_FLAGS = (
    "ddlAuthorized",
    "relationMovingDdlAllowed",
    "historicalAuthenticatedSelectRemovalAllowed",
)


def canonical(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path.name} must contain one JSON object")
    return value


def git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=ROOT, check=True, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    return result.stdout.strip()


def committed_migration_head(commit: str) -> str:
    versions = committed_migration_versions(commit)
    if not versions:
        raise ValueError("source commit has no numeric migration history")
    return max(versions)


def committed_migration_versions(commit: str) -> list[str]:
    paths = git(
        "ls-tree", "-r", "--name-only", commit, "--", "supabase/migrations"
    ).splitlines()
    return sorted(
        Path(path).name.split("_", 1)[0]
        for path in paths
        if re.fullmatch(r"supabase/migrations/[0-9]+_.*\.sql", path)
    )


def migration_set_sha256(versions: list[str]) -> str:
    return sha256(("\n".join(versions) + "\n").encode())


def scaffold_commit() -> str:
    relative = CONTRACT_PATH.relative_to(ROOT).as_posix()
    commits = git("log", "--format=%H", "--diff-filter=A", "--", relative).splitlines()
    if len(commits) != 1:
        raise ValueError("qualification scaffold must have exactly one introduction commit")
    commit = commits[0]
    parent = git("rev-parse", f"{commit}^")
    if parent != EXPECTED_SOURCE_COMMIT:
        raise ValueError("qualification scaffold direct parent differs from exact dev base")
    return commit


def validate_contract() -> dict[str, object]:
    raw = CONTRACT_PATH.read_bytes()
    expected_hash = SHA_PATH.read_text(encoding="utf-8").strip()
    if not re.fullmatch(r"[0-9a-f]{64}", expected_hash) or sha256(raw) != expected_hash:
        raise ValueError("qualification contract SHA-256 sidecar differs")
    contract = read_json(CONTRACT_PATH)
    binding = contract.get("artifactBinding")
    if not isinstance(binding, dict):
        raise ValueError("qualification artifact binding is missing")
    if binding.get("schemaPath") != SCHEMA_PATH.relative_to(ROOT).as_posix():
        raise ValueError("qualification schema path binding differs")
    schema_raw = SCHEMA_PATH.read_bytes()
    if binding.get("schemaSha256") != sha256(schema_raw):
        raise ValueError("qualification JSON Schema bytes differ from contract binding")
    schema = read_json(SCHEMA_PATH)
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.Draft202012Validator(schema).validate(contract)
    if contract.get("schemaVersion") != "database.lca-result-family-physical-qualification.v1":
        raise ValueError("qualification contract version differs")
    source = contract.get("source")
    authorization = contract.get("authorization")
    scope = contract.get("scope")
    candidate = contract.get("candidate")
    evidence = contract.get("evidence")
    dependency = contract.get("dependencyClosure")
    qualification = contract.get("qualification")
    for label, value in (
        ("source", source), ("authorization", authorization), ("scope", scope),
        ("candidate", candidate), ("evidence", evidence),
        ("dependencyClosure", dependency), ("qualification", qualification),
    ):
        if not isinstance(value, dict):
            raise ValueError(f"qualification {label} must be an object")
    assert isinstance(source, dict)
    assert isinstance(authorization, dict)
    assert isinstance(scope, dict)
    assert isinstance(candidate, dict)
    assert isinstance(evidence, dict)
    assert isinstance(dependency, dict)
    assert isinstance(qualification, dict)
    if source != {
        "repository": "tiangong-lca/database-engine",
        "commitSha": EXPECTED_SOURCE_COMMIT,
        "predecessorMigrationHead": EXPECTED_MIGRATION_HEAD,
        "predecessorMigrationCount": EXPECTED_MIGRATION_COUNT,
        "predecessorMigrationSetSha256": EXPECTED_MIGRATION_SET_SHA256,
    }:
        raise ValueError("qualification source tuple differs from exact origin/dev merge")
    versions = committed_migration_versions(EXPECTED_SOURCE_COMMIT)
    if max(versions) != EXPECTED_MIGRATION_HEAD:
        raise ValueError("exact source commit migration head differs")
    if len(versions) != EXPECTED_MIGRATION_COUNT:
        raise ValueError("exact source commit migration count differs")
    if migration_set_sha256(versions) != EXPECTED_MIGRATION_SET_SHA256:
        raise ValueError("exact source commit migration set differs")
    if scope.get("relations") != EXPECTED_RELATIONS or scope.get("routines") != EXPECTED_ROUTINES:
        raise ValueError("qualification scope differs from the exact four relations and three routines")
    if any(authorization.get(flag) is not False for flag in AUTHORIZATION_FLAGS):
        raise ValueError("v1 qualification artifact must keep every pre-DDL authorization false")
    if authorization.get("qualificationExecutionAllowed") is not False:
        raise ValueError("v1 destructive qualification must remain disabled")
    if any(value is not None for value in candidate.values()):
        raise ValueError("v1 candidate bindings must remain empty")
    if any(evidence.get(field) is not None for field in (
        "baselineReceipt", "freshUpgradeReceipt", "populatedUpgradeReceipt",
        "rollbackRollForwardReceipt", "lockWalTimeReceipt",
    )):
        raise ValueError("v1 evidence skeleton must not claim unrun qualification")
    required_surfaces = set(dependency.get("requiredSurfaces", []))
    if required_surfaces != {
        "pg_depend", "foreign-key-inbound-and-outbound", "view-and-pg_rewrite",
        "routine-definition-and-prosrc", "relation-composite-type-and-rowtype",
        "dynamic-sql-lexical-candidates", "regclass-and-regprocedure-candidates",
        "policy-and-publication-membership",
    }:
        raise ValueError("dependency closure surfaces are incomplete")
    phases = qualification.get("phases")
    if phases != [
        "fresh-upgrade", "populated-upgrade", "failure-atomicity", "lock-timeout",
        "wal-and-time-budget", "retry", "rollback", "roll-forward",
    ]:
        raise ValueError("qualification phase sequence differs")
    capture = qualification.get("baselineCapture")
    if capture != {
        "maxRowsPerRelation": MAX_BASELINE_ROWS_PER_RELATION,
        "statementTimeoutSeconds": BASELINE_STATEMENT_TIMEOUT_SECONDS,
        "digestAlgorithm": "sha256",
        "digestEncoding": "ordered-jsonb-text-lines-v1",
    }:
        raise ValueError("baseline capture budget or digest contract differs")
    return contract


def pre_ddl_contract(plan: dict[str, object]) -> dict[str, object]:
    authorization = plan["authorization"]
    assert isinstance(authorization, dict)
    relative = authorization["preDdlContract"]
    if not isinstance(relative, str):
        raise ValueError("pre-DDL contract path is invalid")
    path = (ROOT / relative).resolve()
    if ROOT not in path.parents:
        raise ValueError("pre-DDL contract escapes repository")
    return read_json(path)


def authorization_blockers(plan: dict[str, object]) -> list[str]:
    blockers: list[str] = []
    authorization = plan["authorization"]
    candidate = plan["candidate"]
    assert isinstance(authorization, dict)
    assert isinstance(candidate, dict)
    pre_ddl = pre_ddl_contract(plan)
    migration_gate = pre_ddl.get("migrationGate", {})
    if not isinstance(migration_gate, dict):
        blockers.append("pre-ddl:migration-gate-invalid")
        migration_gate = {}
    actual = {
        "ddlAuthorized": pre_ddl.get("ddlAuthorized"),
        "relationMovingDdlAllowed": migration_gate.get("relationMovingDdlAllowed"),
        "historicalAuthenticatedSelectRemovalAllowed": migration_gate.get(
            "historicalAuthenticatedSelectRemovalAllowed"
        ),
    }
    for flag in AUTHORIZATION_FLAGS:
        if authorization.get(flag) is not True:
            blockers.append(f"qualification-plan:{flag}=false")
        if actual.get(flag) is not True:
            blockers.append(f"pre-ddl-contract:{flag}=false")
    if authorization.get("qualificationExecutionAllowed") is not True:
        blockers.append("qualification-plan:qualificationExecutionAllowed=false")
    for field in (
        "migrationPath", "migrationSha256", "rollbackPath", "rollbackSha256",
        "populatedFixturePath", "populatedFixtureSha256", "targetMigrationHead",
    ):
        if not candidate.get(field):
            blockers.append(f"candidate:{field}=missing")
    return blockers


def assert_loopback(db_url: str) -> None:
    parsed = urlsplit(db_url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise ValueError("database URL must use PostgreSQL")
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("Issue #390 qualification is local-only and requires loopback")


def psql_target(db_url: str) -> tuple[str, dict[str, str]]:
    assert_loopback(db_url)
    parsed = urlsplit(db_url)
    if parsed.query or parsed.fragment or parsed.port is None:
        raise ValueError("database URL requires one explicit port and no query or fragment")
    user = unquote(parsed.username or "")
    database = unquote(parsed.path.removeprefix("/"))
    if not user or not database or "/" in database:
        raise ValueError("database URL requires one user and database")
    host = parsed.hostname or ""
    if host == "::1":
        host = "[::1]"
    target = (
        f"{parsed.scheme}://{quote(user, safe='')}@{host}:{parsed.port}/"
        f"{quote(database, safe='')}"
    )
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("PG") and key not in {"DATABASE_URL", "SUPABASE_DB_URL"}
    }
    environment["PGSSLMODE"] = "disable"
    if parsed.password is not None:
        environment["PGPASSWORD"] = unquote(parsed.password)
    return target, environment


def psql_json(db_url: str, sql: str) -> dict[str, object]:
    target, environment = psql_target(db_url)
    try:
        result = subprocess.run(
            ["psql", target, "-qXAt", "-v", "ON_ERROR_STOP=1"], cwd=ROOT,
            env=environment, input=sql, text=True, check=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or "").strip() or "psql returned no diagnostic"
        raise RuntimeError(f"baseline SQL execution failed: {detail}") from exc
    value = json.loads(result.stdout.strip())
    if not isinstance(value, dict):
        raise ValueError("baseline catalog query did not return one JSON object")
    return value


# The query deliberately captures catalog-native dependency edges and lexical
# candidates separately. PostgreSQL cannot represent every dynamic SQL or
# %ROWTYPE reference in pg_depend, so neither evidence class may substitute for
# the other during independent closure review.
BASELINE_SQL = r"""
begin transaction isolation level repeatable read read only;
set local search_path = '';
set local statement_timeout = '120s';
with recursive
target_rel(schema_name,name,oid,reltype,relkind,owner_oid,force_rls) as (
  select n.nspname,v.name,c.oid,c.reltype,c.relkind,c.relowner,c.relforcerowsecurity
  from (values ('lca_results'),('lca_result_cache'),
    ('lca_latest_all_unit_results'),('lca_factorization_registry')) v(name)
  left join pg_catalog.pg_namespace n on n.nspname='public'
  left join pg_catalog.pg_class c on c.relnamespace=n.oid and c.relname=v.name
), target_proc(planned_signature,oid) as (
  select v.signature,pg_catalog.to_regprocedure(v.signature)
  from (values
    ('public.lca_read_job_projection(uuid,uuid,uuid,boolean)'),
    ('public.lca_read_result_projection(uuid,uuid,text,boolean)'),
    ('public.lca_read_latest_single_solve_result(uuid,uuid,integer)')) v(signature)
), application_relations(oid) as (
  select c.oid from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname in ('public','private','api','util','archive')
), application_objects(classid,objid) as (
  select 'pg_catalog.pg_class'::regclass::oid,c.oid from pg_catalog.pg_class c join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname in ('public','private','api','util','archive')
  union select 'pg_catalog.pg_proc'::regclass::oid,p.oid from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname in ('public','private','api','util','archive')
  union select 'pg_catalog.pg_type'::regclass::oid,t.oid from pg_catalog.pg_type t join pg_catalog.pg_namespace n on n.oid=t.typnamespace
    where n.nspname in ('public','private','api','util','archive')
  union select 'pg_catalog.pg_rewrite'::regclass::oid,rw.oid from pg_catalog.pg_rewrite rw join pg_catalog.pg_class c on c.oid=rw.ev_class
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace where n.nspname in ('public','private','api','util','archive')
  union select 'pg_catalog.pg_constraint'::regclass::oid,x.oid from pg_catalog.pg_constraint x
    where x.conrelid in (select oid from application_relations)
       or x.confrelid in (select oid from application_relations)
  union select 'pg_catalog.pg_attrdef'::regclass::oid,d.oid from pg_catalog.pg_attrdef d
    where d.adrelid in (select oid from application_relations)
  union select 'pg_catalog.pg_trigger'::regclass::oid,t.oid from pg_catalog.pg_trigger t
    where t.tgrelid in (select oid from application_relations)
  union select 'pg_catalog.pg_policy'::regclass::oid,p.oid from pg_catalog.pg_policy p
    where p.polrelid in (select oid from application_relations)
  union select 'pg_catalog.pg_publication_rel'::regclass::oid,pr.oid
    from pg_catalog.pg_publication_rel pr
    where pr.prrelid in (select oid from application_relations)
), seeds(classid,objid) as (
  select 'pg_catalog.pg_class'::regclass::oid,oid from target_rel where oid is not null
  union select 'pg_catalog.pg_type'::regclass::oid,reltype from target_rel where reltype is not null
  union select 'pg_catalog.pg_proc'::regclass::oid,oid from target_proc where oid is not null
), closure(classid,objid,depth) as (
  select classid,objid,0 from seeds
  union
  select neighbor.classid,neighbor.objid,c.depth+1
  from closure c cross join lateral (
    select d.classid,d.objid from pg_depend d
      where d.refclassid=c.classid and d.refobjid=c.objid and d.deptype not in ('p','e')
    union
    select d.refclassid,d.refobjid from pg_depend d
      where d.classid=c.classid and d.objid=c.objid and d.deptype not in ('p','e')
  ) neighbor
  join application_objects app
    on app.classid=neighbor.classid and app.objid=neighbor.objid
  where c.depth<32
), relation_facts as (
  select r.schema_name,r.name,r.schema_name||'.'||r.name identity,
    c.oid::bigint oid,c.reltype::bigint composite_type_oid,c.relkind,
    pg_catalog.pg_get_userbyid(c.relowner) owner,coalesce(c.relacl::text,'') acl,
    c.relrowsecurity rls,c.relforcerowsecurity force_rls,c.relreplident replica_identity,
    (select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('attnum',a.attnum,'name',a.attname,
      'type',pg_catalog.format_type(a.atttypid,a.atttypmod),'notNull',a.attnotnull,
      'identity',a.attidentity,'generated',a.attgenerated,'acl',coalesce(a.attacl::text,''))
      order by a.attnum),'[]') from pg_attribute a where a.attrelid=c.oid
      and a.attnum>0 and not a.attisdropped) columns,
    (select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('name',x.conname,'type',x.contype,
      'relation',x.conrelid::regclass::text,'referencedRelation',nullif(x.confrelid,0)::regclass::text,
      'definition',pg_catalog.pg_get_constraintdef(x.oid,true),'validated',x.convalidated,
      'deleteAction',x.confdeltype,'updateAction',x.confupdtype) order by x.conname),'[]')
      from pg_catalog.pg_constraint x where x.conrelid=c.oid or x.confrelid=c.oid) constraints,
    (select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('name',ic.relname,
      'oid',ic.oid::bigint,'definition',pg_catalog.pg_get_indexdef(i.indexrelid),
      'valid',i.indisvalid,'ready',i.indisready) order by ic.relname),'[]')
      from pg_catalog.pg_index i join pg_catalog.pg_class ic on ic.oid=i.indexrelid where i.indrelid=c.oid) indexes,
    (select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('name',t.tgname,'oid',t.oid::bigint,
      'definition',pg_catalog.pg_get_triggerdef(t.oid,true),'enabled',t.tgenabled,
      'internal',t.tgisinternal) order by t.tgname),'[]') from pg_catalog.pg_trigger t where t.tgrelid=c.oid) triggers,
    (select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('name',p.pubname,
      'columns',p.attnames,'rowFilter',p.rowfilter) order by p.pubname),'[]')
      from pg_catalog.pg_publication_tables p
      where p.schemaname='public' and p.tablename=r.name) publications,
    (select coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(p) order by p.policyname),'[]')
      from pg_catalog.pg_policies p where p.schemaname='public' and p.tablename=r.name) policies,
    case r.name
      when 'lca_results' then (select pg_catalog.jsonb_build_object('rowCount',s.row_count,
        'withinCaptureBudget',s.row_count<=100000,'digestComplete',s.row_count<=100000,
        'digestAlgorithm','sha256','digestEncoding','ordered-jsonb-text-lines-v1',
        'primaryKeySha256',case when s.row_count<=100000 then pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
          coalesce((select pg_catalog.string_agg(pg_catalog.to_jsonb(t.id)::text,E'\n' order by t.id) from public.lca_results t),''),'UTF8')),'hex') end,
        'contentSha256',case when s.row_count<=100000 then pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
          coalesce((select pg_catalog.string_agg(pg_catalog.jsonb_build_array(t.id,pg_catalog.to_jsonb(t))::text,E'\n' order by t.id) from public.lca_results t),''),'UTF8')),'hex') end)
        from (select pg_catalog.count(*) row_count from public.lca_results) s)
      when 'lca_result_cache' then (select pg_catalog.jsonb_build_object('rowCount',s.row_count,
        'withinCaptureBudget',s.row_count<=100000,'digestComplete',s.row_count<=100000,
        'digestAlgorithm','sha256','digestEncoding','ordered-jsonb-text-lines-v1',
        'primaryKeySha256',case when s.row_count<=100000 then pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
          coalesce((select pg_catalog.string_agg(pg_catalog.to_jsonb(t.id)::text,E'\n' order by t.id) from public.lca_result_cache t),''),'UTF8')),'hex') end,
        'contentSha256',case when s.row_count<=100000 then pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
          coalesce((select pg_catalog.string_agg(pg_catalog.jsonb_build_array(t.id,pg_catalog.to_jsonb(t))::text,E'\n' order by t.id) from public.lca_result_cache t),''),'UTF8')),'hex') end)
        from (select pg_catalog.count(*) row_count from public.lca_result_cache) s)
      when 'lca_latest_all_unit_results' then (select pg_catalog.jsonb_build_object('rowCount',s.row_count,
        'withinCaptureBudget',s.row_count<=100000,'digestComplete',s.row_count<=100000,
        'digestAlgorithm','sha256','digestEncoding','ordered-jsonb-text-lines-v1',
        'primaryKeySha256',case when s.row_count<=100000 then pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
          coalesce((select pg_catalog.string_agg(pg_catalog.to_jsonb(t.id)::text,E'\n' order by t.id) from public.lca_latest_all_unit_results t),''),'UTF8')),'hex') end,
        'contentSha256',case when s.row_count<=100000 then pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
          coalesce((select pg_catalog.string_agg(pg_catalog.jsonb_build_array(t.id,pg_catalog.to_jsonb(t))::text,E'\n' order by t.id) from public.lca_latest_all_unit_results t),''),'UTF8')),'hex') end)
        from (select pg_catalog.count(*) row_count from public.lca_latest_all_unit_results) s)
      when 'lca_factorization_registry' then (select pg_catalog.jsonb_build_object('rowCount',s.row_count,
        'withinCaptureBudget',s.row_count<=100000,'digestComplete',s.row_count<=100000,
        'digestAlgorithm','sha256','digestEncoding','ordered-jsonb-text-lines-v1',
        'primaryKeySha256',case when s.row_count<=100000 then pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
          coalesce((select pg_catalog.string_agg(pg_catalog.to_jsonb(t.id)::text,E'\n' order by t.id) from public.lca_factorization_registry t),''),'UTF8')),'hex') end,
        'contentSha256',case when s.row_count<=100000 then pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
          coalesce((select pg_catalog.string_agg(pg_catalog.jsonb_build_array(t.id,pg_catalog.to_jsonb(t))::text,E'\n' order by t.id) from public.lca_factorization_registry t),''),'UTF8')),'hex') end)
        from (select pg_catalog.count(*) row_count from public.lca_factorization_registry) s)
    end data_oracle
  from target_rel r join pg_catalog.pg_class c on c.oid=r.oid
), routine_facts as (
  select t.planned_signature,
    pg_catalog.format('%I.%I(%s)',n.nspname,p.proname,pg_catalog.oidvectortypes(p.proargtypes)) observed_signature,
    n.nspname schema_name,p.proname,p.prokind routine_kind,p.oid::bigint oid,
    pg_catalog.pg_get_userbyid(p.proowner) owner,
    coalesce(p.proacl::text,'') acl,p.prorettype::regtype::text result_type,
    p.prosecdef security_definer,p.provolatile volatility,p.proisstrict strict,
    p.proparallel parallel,coalesce(p.proconfig::text,'') config,
    pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_functiondef(p.oid),'UTF8')),'hex') definition_sha256
  from target_proc t join pg_catalog.pg_proc p on p.oid=t.oid
  join pg_catalog.pg_namespace n on n.oid=p.pronamespace
), lexical_candidates as (
  select p.oid::regprocedure::text signature,n.nspname schema_name,
    pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc,'UTF8')),'hex') prosrc_sha256,
    (p.prosrc ~* 'execute|format\s*\(') dynamic_sql_candidate,
    (p.prosrc ~* '%rowtype|rowtype') rowtype_candidate,
    (p.prosrc ~* 'regclass|regprocedure') reg_object_candidate,
    array(select distinct pg_catalog.lower(match[1]) from pg_catalog.regexp_matches(p.prosrc,
      '(lca_results|lca_result_cache|lca_latest_all_unit_results|lca_factorization_registry|lca_read_job_projection|lca_read_result_projection|lca_read_latest_single_solve_result)','gi') matches(match)
      order by pg_catalog.lower(match[1])) target_tokens
  from pg_catalog.pg_proc p join pg_catalog.pg_namespace n on n.oid=p.pronamespace
  where n.nspname not in ('pg_catalog','information_schema') and p.prosrc ~
    '(?i)(lca_results|lca_result_cache|lca_latest_all_unit_results|lca_factorization_registry|lca_read_job_projection|lca_read_result_projection|lca_read_latest_single_solve_result)'
), rewrite_candidates as (
  select distinct rw.oid::bigint rewrite_oid,rw.ev_class::regclass::text relation,
    pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.pg_get_ruledef(rw.oid,true),'UTF8')),'hex') definition_sha256
  from pg_catalog.pg_rewrite rw join pg_catalog.pg_depend d
    on d.classid='pg_catalog.pg_rewrite'::regclass and d.objid=rw.oid
  where d.refclassid='pg_catalog.pg_class'::regclass
    and d.refobjid in (select oid from target_rel where oid is not null)
), policy_candidates as (
  select p.oid::bigint policy_oid,p.polrelid::regclass::text relation,p.polname,
    pg_catalog.pg_get_expr(p.polqual,p.polrelid,true) qualification,
    pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid,true) with_check
  from pg_catalog.pg_policy p where p.polrelid in (select oid from target_rel where oid is not null)
), publication_memberships as (
  select pr.oid::bigint membership_oid,p.oid::bigint publication_oid,p.pubname,
    pr.prrelid::regclass::text relation,pr.prattrs::text column_numbers,
    pg_catalog.pg_get_expr(pr.prqual,pr.prrelid,true) row_filter
  from pg_catalog.pg_publication_rel pr
  join pg_catalog.pg_publication p on p.oid=pr.prpubid
  where pr.prrelid in (select oid from target_rel where oid is not null)
), dependency_edges as (
  select distinct d.classid,d.objid,d.objsubid,d.refclassid,d.refobjid,d.refobjsubid,d.deptype
  from pg_catalog.pg_depend d
  where (d.classid,d.objid) in (select classid,objid from closure)
    and (d.refclassid,d.refobjid) in (select classid,objid from closure)
), operator_proof as (
  select r.rolname current_role,r.rolsuper role_super,r.rolbypassrls role_bypass_rls,
    coalesce((select pg_catalog.bool_and(t.owner_oid=r.oid and not t.force_rls)
      from target_rel t),false) owns_all_targets_without_forced_rls
  from pg_catalog.pg_roles r where r.rolname=current_user
), database_provenance as (
  select current_database() database_name,d.oid::bigint database_oid,
    pg_catalog.inet_server_addr()::text server_address,
    pg_catalog.inet_server_port() server_port,
    (select system_identifier::text from pg_catalog.pg_control_system()) system_identifier,
    current_setting('server_version') server_version,
    (select pg_catalog.max(version::text) from supabase_migrations.schema_migrations) applied_migration_head,
    (select pg_catalog.count(*) from supabase_migrations.schema_migrations) applied_migration_count,
    (select coalesce(pg_catalog.jsonb_agg(version::text order by version::text),'[]')
      from supabase_migrations.schema_migrations) applied_migration_versions
  from pg_catalog.pg_database d where d.datname=current_database()
)
select pg_catalog.jsonb_build_object(
  'schemaVersion','database.lca-result-family-baseline-receipt.v1',
  'nonAuthorizing',true,
  'databaseProvenance',(select pg_catalog.to_jsonb(d) from database_provenance d),
  'operatorProof',(select pg_catalog.to_jsonb(o)||pg_catalog.jsonb_build_object('fullRowVisibility',
    o.role_super or o.role_bypass_rls or o.owns_all_targets_without_forced_rls) from operator_proof o),
  'relations',(select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r) order by identity) from relation_facts r),
  'routines',(select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r) order by observed_signature) from routine_facts r),
  'dependencyClosure',pg_catalog.jsonb_build_object(
    'pgDependObjects',(select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('classid',o.classid::regclass::text,
      'objid',o.objid::bigint,'identity',pg_catalog.pg_describe_object(o.classid,o.objid,0),'depth',o.depth)
      order by o.classid::regclass::text,o.objid) from
      (select classid,objid,min(depth) depth from closure group by classid,objid) o),
    'pgDependEdges',(select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
      'dependent',pg_catalog.pg_describe_object(d.classid,d.objid,d.objsubid),
      'referenced',pg_catalog.pg_describe_object(d.refclassid,d.refobjid,d.refobjsubid),'type',d.deptype)
      order by d.classid,d.objid,d.objsubid,d.refclassid,d.refobjid,d.refobjsubid)
      ,'[]') from dependency_edges d),
    'viewRewriteCandidates',(select coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r)
      order by relation,rewrite_oid),'[]') from rewrite_candidates r),
    'policyObjects',(select coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(p)
      order by relation,polname),'[]') from policy_candidates p),
    'publicationMemberships',(select coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(p)
      order by pubname,relation,membership_oid),'[]') from publication_memberships p),
    'routineLexicalCandidates',(select coalesce(pg_catalog.jsonb_agg(pg_catalog.to_jsonb(l)
      order by signature),'[]') from lexical_candidates l)
  )
)::text;
rollback;
"""


def _normalized_signature(value: object) -> str:
    return re.sub(r"\s+", "", str(value))


def _require_sha256(value: object, label: str) -> None:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError(f"baseline receipt {label} must be lowercase SHA-256")


def validate_baseline_receipt(plan: dict[str, object], receipt: dict[str, object]) -> None:
    if receipt.get("schemaVersion") != "database.lca-result-family-baseline-receipt.v1":
        raise ValueError("baseline receipt version differs")
    if receipt.get("nonAuthorizing") is not True:
        raise ValueError("baseline receipt must remain explicitly non-authorizing")
    if "source" in receipt or "repositoryPlan" in receipt:
        raise ValueError("database receipt must not claim repository plan provenance")

    database = receipt.get("databaseProvenance")
    if not isinstance(database, dict):
        raise ValueError("baseline receipt database provenance is missing")
    required_identity = {
        "database_name", "database_oid", "server_address", "server_port",
        "system_identifier", "server_version", "applied_migration_head",
        "applied_migration_count", "applied_migration_versions",
    }
    if set(database) != required_identity:
        raise ValueError("baseline receipt database identity fields differ")
    try:
        ipaddress.ip_interface(str(database["server_address"]))
    except ValueError as exc:
        raise ValueError("baseline receipt server address is invalid") from exc
    if not isinstance(database["database_name"], str) or not database["database_name"]:
        raise ValueError("baseline receipt database name is invalid")
    if not isinstance(database["database_oid"], int) or database["database_oid"] <= 0:
        raise ValueError("baseline receipt database OID is invalid")
    if not isinstance(database["server_port"], int) or database["server_port"] <= 0:
        raise ValueError("baseline receipt server port is invalid")
    if not re.fullmatch(r"[0-9]+", str(database["system_identifier"])):
        raise ValueError("baseline receipt system identifier is invalid")
    expected_versions = committed_migration_versions(EXPECTED_SOURCE_COMMIT)
    if database["applied_migration_versions"] != expected_versions:
        raise ValueError("database applied migration set differs from exact predecessor")
    if database["applied_migration_head"] != EXPECTED_MIGRATION_HEAD:
        raise ValueError("database applied migration head differs from exact predecessor")
    if database["applied_migration_count"] != EXPECTED_MIGRATION_COUNT:
        raise ValueError("database applied migration count differs from exact predecessor")

    operator = receipt.get("operatorProof")
    if not isinstance(operator, dict):
        raise ValueError("baseline receipt operator visibility proof is missing")
    if operator.get("current_role") not in ALLOWED_CAPTURE_ROLES:
        raise ValueError("baseline capture role is not an allowed owner-capable operator")
    if operator.get("fullRowVisibility") is not True:
        raise ValueError("baseline capture lacks full-row RLS visibility proof")
    if not any(operator.get(field) is True for field in (
        "role_super", "role_bypass_rls", "owns_all_targets_without_forced_rls",
    )):
        raise ValueError("baseline capture operator proof is internally inconsistent")

    relations = receipt.get("relations")
    if not isinstance(relations, list) or len(relations) != len(EXPECTED_RELATIONS):
        raise ValueError("baseline receipt must contain exactly four relations")
    by_identity = {
        row.get("identity"): row for row in relations if isinstance(row, dict)
    }
    if set(by_identity) != set(EXPECTED_RELATIONS) or len(by_identity) != len(relations):
        raise ValueError("baseline relation identities differ or contain duplicates")
    for identity in EXPECTED_RELATIONS:
        row = by_identity[identity]
        if row.get("schema_name") != "public" or row.get("relkind") != "r":
            raise ValueError(f"baseline target is not an ordinary public table: {identity}")
        if not isinstance(row.get("oid"), int) or not isinstance(row.get("composite_type_oid"), int):
            raise ValueError(f"baseline target OID evidence is missing: {identity}")
        if not isinstance(row.get("owner"), str) or not row["owner"]:
            raise ValueError(f"baseline target owner evidence is missing: {identity}")
        oracle = row.get("data_oracle")
        if not isinstance(oracle, dict):
            raise ValueError(f"baseline target data oracle is missing: {identity}")
        if oracle.get("withinCaptureBudget") is not True or oracle.get("digestComplete") is not True:
            raise ValueError(f"baseline target exceeds the reviewed digest budget: {identity}")
        row_count = oracle.get("rowCount")
        if not isinstance(row_count, int) or not 0 <= row_count <= MAX_BASELINE_ROWS_PER_RELATION:
            raise ValueError(f"baseline target row count is outside budget: {identity}")
        if oracle.get("digestAlgorithm") != "sha256" or oracle.get("digestEncoding") != "ordered-jsonb-text-lines-v1":
            raise ValueError(f"baseline target digest contract differs: {identity}")
        _require_sha256(oracle.get("primaryKeySha256"), f"{identity} primary key digest")
        _require_sha256(oracle.get("contentSha256"), f"{identity} content digest")

    routines = receipt.get("routines")
    if not isinstance(routines, list) or len(routines) != len(EXPECTED_ROUTINES):
        raise ValueError("baseline receipt must contain exactly three routines")
    observed = {
        _normalized_signature(row.get("observed_signature")): row
        for row in routines if isinstance(row, dict)
    }
    expected = {_normalized_signature(signature) for signature in EXPECTED_ROUTINES}
    if set(observed) != expected or len(observed) != len(routines):
        raise ValueError("baseline routine signatures differ or contain duplicates")
    for signature, row in observed.items():
        if row.get("schema_name") != "public" or row.get("routine_kind") != "f":
            raise ValueError(f"baseline target is not an exact public function: {signature}")
        if _normalized_signature(row.get("planned_signature")) != signature:
            raise ValueError(f"baseline planned/observed routine signature differs: {signature}")
        if not isinstance(row.get("oid"), int) or not isinstance(row.get("owner"), str):
            raise ValueError(f"baseline routine identity evidence is incomplete: {signature}")
        _require_sha256(row.get("definition_sha256"), f"{signature} definition digest")

    dependency = receipt.get("dependencyClosure")
    if not isinstance(dependency, dict):
        raise ValueError("baseline dependency closure is missing")
    if set(dependency) != {
        "pgDependObjects", "pgDependEdges", "viewRewriteCandidates", "policyObjects",
        "publicationMemberships", "routineLexicalCandidates",
    }:
        raise ValueError("baseline dependency closure surfaces differ")
    if not all(isinstance(dependency[field], list) for field in dependency):
        raise ValueError("baseline dependency closure surface is not a list")


def assert_distinct_database_instances(
    fresh: dict[str, object], populated: dict[str, object]
) -> None:
    for label, identity in (("fresh", fresh), ("populated", populated)):
        if not isinstance(identity.get("system_identifier"), str):
            raise ValueError(f"{label} database lacks system identifier proof")
        if not isinstance(identity.get("database_oid"), int):
            raise ValueError(f"{label} database lacks database OID proof")
    if fresh["system_identifier"] == populated["system_identifier"]:
        raise ValueError("fresh and populated qualification databases share one cluster identity")


def capture_baseline(plan: dict[str, object], db_url: str) -> dict[str, object]:
    assert_loopback(db_url)
    receipt = psql_json(db_url, BASELINE_SQL)
    validate_baseline_receipt(plan, receipt)
    receipt["repositoryPlan"] = plan["source"]
    receipt["scope"] = plan["scope"]
    receipt["capture"] = {
        "mode": "read-only-repeatable-read",
        "payloadStored": False,
        "authorizationClaim": False,
        "disposableDatabaseClaim": False,
        "targetClassification": "loopback-only-identity-not-disposable-proof",
    }
    return receipt


def qualification_plan(plan: dict[str, object]) -> dict[str, object]:
    qualification = plan["qualification"]
    assert isinstance(qualification, dict)
    return {
        "schemaVersion": "database.lca-result-family-qualification-run-plan.v1",
        "executable": not authorization_blockers(plan),
        "blockers": authorization_blockers(plan),
        "phases": qualification["phases"],
        "budgets": qualification["budgets"],
        "requiredInputs": {
            "freshDatabaseUrl": "disposable loopback database reset to predecessor with a unique cluster system identifier",
            "populatedDatabaseUrl": "second disposable loopback database reset to predecessor with a different cluster system identifier",
            "receiptDirectory": "new local directory outside the repository",
            "candidateBindings": "exact migration, rollback, fixture paths and SHA-256 values",
        },
        "receiptRequirements": [
            "fresh-and-populated-distinct-system-identifiers-and-database-oids",
            "pre-post-rollback-roll-forward-exact-catalog-and-data-oracles",
            "second-target-lock-timeout-and-transactional-failure-atomicity",
            "measured-upgrade-retry-wall-time-and-wal-bytes",
            "zero-hosted-and-production-targets",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--print-run-plan", action="store_true")
    mode.add_argument("--capture-baseline", action="store_true")
    mode.add_argument("--qualify", action="store_true")
    parser.add_argument("--db-url")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--fresh-db-url")
    parser.add_argument("--populated-db-url")
    parser.add_argument("--receipt-dir", type=Path)
    args = parser.parse_args()
    plan = validate_contract()
    if args.capture_baseline:
        if not args.db_url or not args.output:
            parser.error("--capture-baseline requires --db-url and --output")
        receipt = capture_baseline(plan, args.db_url)
        args.output.write_bytes(canonical(receipt))
        print(json.dumps({"status": "captured-non-authorizing", "output": str(args.output), "sha256": sha256(canonical(receipt))}, sort_keys=True))
        return 0
    run_plan = qualification_plan(plan)
    if args.qualify:
        blockers = run_plan["blockers"]
        if blockers:
            raise SystemExit("destructive qualification is not authorized: " + ", ".join(blockers))
        if not args.fresh_db_url or not args.populated_db_url or not args.receipt_dir:
            parser.error(
                "an authorized successor requires --fresh-db-url, --populated-db-url, "
                "and --receipt-dir"
            )
        assert_loopback(args.fresh_db_url)
        assert_loopback(args.populated_db_url)
        if args.fresh_db_url == args.populated_db_url:
            parser.error("fresh and populated qualification databases must be distinct")
        raise SystemExit("v1 skeleton cannot execute a future authorized candidate; publish a reviewed successor contract and runner")
    if args.print_run_plan:
        print(json.dumps(run_plan, sort_keys=True))
    else:
        print(json.dumps({
            "status": "PASS", "mode": "offline-non-authorizing",
            "sourceCommit": EXPECTED_SOURCE_COMMIT,
            "migrationHead": EXPECTED_MIGRATION_HEAD,
            "destructiveQualificationExecutable": run_plan["executable"],
            "blockerCount": len(run_plan["blockers"]),
        }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
