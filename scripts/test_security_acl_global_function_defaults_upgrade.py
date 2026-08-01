#!/usr/bin/env python3
"""PG17 global future-function ACL upgrade, retry, restore, and parity proof."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = "20260801061000"
MIGRATION = ROOT / "supabase/migrations/20260801131918_issue_339_postgres_global_function_defaults.sql"
RESTORE = ROOT / "supabase/operator/issue_339_restore_postgres_global_function_defaults.sql"
TEST = ROOT / "supabase/tests/20260801_postgres_global_function_defaults.sql"


def supabase_command(*args: str) -> list[str]:
    command = ["supabase"]
    if workdir := os.environ.get("SUPABASE_WORKDIR"):
        command.extend(["--workdir", workdir])
    command.extend(args)
    return command


def run(command: list[str], *, sql: str | None = None, expect_failure: bool = False) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, cwd=ROOT, input=sql, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if expect_failure == (result.returncode == 0):
        raise SystemExit(
            f"unexpected command result ({result.returncode}): {' '.join(command)}\n{result.stdout}\n{result.stderr}"
        )
    return result


def psql(database_url: str, sql: str, *, expect_failure: bool = False) -> subprocess.CompletedProcess[str]:
    return run(
        ["psql", database_url, "-XAt", "-v", "ON_ERROR_STOP=1"],
        sql=sql,
        expect_failure=expect_failure,
    )


def reset_to_base() -> None:
    command = supabase_command("db", "reset", "--local", "--version", BASE)
    transient_markers = ("context deadline exceeded", "Error status 502")
    for attempt in range(3):
        result = subprocess.run(command, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode == 0:
            return
        combined = result.stdout + result.stderr
        if any(marker in combined for marker in transient_markers):
            # The CLI can time out while restarting optional local services after
            # PostgreSQL has already committed the exact requested reset. This
            # harness is database-only, so accept that narrow state only after a
            # direct catalog readback proves the requested migration head.
            status = subprocess.run(
                supabase_command("status", "--output", "json"), cwd=ROOT,
                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            if status.returncode == 0:
                try:
                    database_url = json.loads(status.stdout)["DB_URL"]
                except (json.JSONDecodeError, KeyError, TypeError):
                    database_url = ""
                if database_url:
                    head = subprocess.run(
                        ["psql", database_url, "-XAt", "-v", "ON_ERROR_STOP=1", "-c",
                         "select max(version) from supabase_migrations.schema_migrations;"],
                        cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    )
                    if head.returncode == 0 and head.stdout.strip() == BASE:
                        return
            if attempt < 2:
                continue
        raise SystemExit(f"base reset failed ({result.returncode}):\n{combined}")


def current_object_acl_signature(database_url: str) -> str:
    return psql(database_url, r"""
      with payload as (
        select jsonb_build_object(
          'schemas', (select coalesce(jsonb_agg(to_jsonb(x) order by schema_name), '[]') from (
            select n.nspname schema_name, coalesce(n.nspacl::text, 'null') acl
            from pg_namespace n
            where n.nspname in ('public','api','private','util','archive')
          ) x),
          'relations', (select coalesce(jsonb_agg(to_jsonb(x) order by schema_name,relation_name), '[]') from (
            select n.nspname schema_name,c.relname relation_name,c.relkind,coalesce(c.relacl::text,'null') acl
            from pg_class c join pg_namespace n on n.oid=c.relnamespace
            where n.nspname in ('public','api','private','util','archive')
              and c.relkind in ('r','p','v','m','S')
              and c.relname not in (
                'security_acl_effective_default_privileges',
                'security_acl_postgres_global_functions_20260801_snapshot'
              )
          ) x),
          'routines', (select coalesce(jsonb_agg(to_jsonb(x) order by routine_name), '[]') from (
            select p.oid::regprocedure::text routine_name,coalesce(p.proacl::text,'null') acl
            from pg_proc p join pg_namespace n on n.oid=p.pronamespace
            where n.nspname in ('public','api','private','util','archive')
          ) x)
        ) value
      ) select md5(value::text) from payload;
    """).stdout.strip()


def function_default_layer_signature(database_url: str) -> str:
    return psql(database_url, r"""
      with scopes(scope_schema,schema_oid) as (
        values
          ('*'::text,0::oid),
          ('public',(select oid from pg_namespace where nspname='public')),
          ('api',(select oid from pg_namespace where nspname='api')),
          ('private',(select oid from pg_namespace where nspname='private')),
          ('util',(select oid from pg_namespace where nspname='util')),
          ('archive',(select oid from pg_namespace where nspname='archive'))
      ), layers as (
        select scopes.scope_schema, defaults.oid is not null explicit_row,
          case when scopes.scope_schema='*'
            then coalesce(defaults.defaclacl,acldefault('f','postgres'::regrole))
            else defaults.defaclacl
          end acl
        from scopes
        left join pg_default_acl defaults
          on defaults.defaclrole='postgres'::regrole
         and defaults.defaclnamespace=scopes.schema_oid
         and defaults.defaclobjtype='f'
      ), entries as (
        select layers.scope_schema,
          case when acl.grantee=0 then 'PUBLIC' else grantee.rolname end grantee,
          acl.privilege_type,acl.is_grantable
        from layers
        cross join lateral aclexplode(layers.acl) acl
        left join pg_roles grantee on grantee.oid=acl.grantee
      )
      select md5(jsonb_build_object(
        'explicitLayers',(select jsonb_agg(scope_schema order by scope_schema)
                          from layers where explicit_row),
        'entries',(select coalesce(jsonb_agg(to_jsonb(entries)
          order by scope_schema,grantee,privilege_type,is_grantable),'[]') from entries)
      )::text);
    """).stdout.strip()


def application_row_count_signature(database_url: str) -> str:
    return psql(database_url, r"""
      select md5(jsonb_build_object(
        'processes',(select count(*) from public.processes),
        'flows',(select count(*) from public.flows),
        'contacts',(select count(*) from public.contacts),
        'sources',(select count(*) from public.sources),
        'unitgroups',(select count(*) from public.unitgroups),
        'flowproperties',(select count(*) from public.flowproperties),
        'lciamethods',(select count(*) from public.lciamethods),
        'lifecyclemodels',(select count(*) from public.lifecyclemodels),
        'ilcd',(select count(*) from public.ilcd),
        'worker_jobs',(select count(*) from private.worker_jobs)
      )::text);
    """).stdout.strip()


def main() -> int:
    reset_to_base()
    database_url = json.loads(run(supabase_command("status", "--output", "json")).stdout)["DB_URL"]

    server_version = psql(database_url, "show server_version_num;").stdout.strip()
    if not server_version.startswith("17"):
        raise SystemExit(f"expected PostgreSQL 17, got server_version_num={server_version}")

    implicit_public = psql(database_url, r"""
      create function public.__issue_339_before_global_revoke__() returns integer
      language sql immutable as $$ select 1 $$;
      select exists (
        select 1 from pg_proc p
        cross join lateral aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) acl
        where p.oid='public.__issue_339_before_global_revoke__()'::regprocedure
          and acl.grantee=0 and acl.privilege_type='EXECUTE'
      );
      drop function public.__issue_339_before_global_revoke__();
    """).stdout.splitlines()
    if "t" not in implicit_public:
        raise SystemExit("pre-migration PostgreSQL built-in PUBLIC EXECUTE was not reproduced")

    # Exercise restore against explicit global rows, the implicit PUBLIC
    # default, and an additive per-schema grant option. The custom table-default
    # grantee proves snapshot ACL convergence is not limited to known API roles.
    psql(database_url, r"""
      create role issue_339_snapshot_reader nologin;
      alter default privileges for role postgres in schema archive
        grant select on tables to issue_339_snapshot_reader;
      alter default privileges for role postgres
        grant execute on functions to anon, authenticated;
      alter default privileges for role postgres
        grant execute on functions to service_role with grant option;
      alter default privileges for role postgres in schema api
        grant execute on functions to service_role with grant option;
    """)
    explicit_global = psql(database_url, r"""
      select jsonb_build_object(
        'anon', exists(
          select 1 from pg_default_acl d cross join lateral aclexplode(d.defaclacl) acl
          where d.defaclrole='postgres'::regrole and d.defaclnamespace=0 and d.defaclobjtype='f'
            and acl.grantee='anon'::regrole and acl.privilege_type='EXECUTE'
        ),
        'authenticated', exists(
          select 1 from pg_default_acl d cross join lateral aclexplode(d.defaclacl) acl
          where d.defaclrole='postgres'::regrole and d.defaclnamespace=0 and d.defaclobjtype='f'
            and acl.grantee='authenticated'::regrole and acl.privilege_type='EXECUTE'
        ),
        'serviceGlobalGrantable', exists(
          select 1 from pg_default_acl d cross join lateral aclexplode(d.defaclacl) acl
          where d.defaclrole='postgres'::regrole and d.defaclnamespace=0 and d.defaclobjtype='f'
            and acl.grantee='service_role'::regrole and acl.privilege_type='EXECUTE' and acl.is_grantable
        ),
        'serviceApiGrantable', exists(
          select 1 from pg_default_acl d
          join pg_namespace n on n.oid=d.defaclnamespace
          cross join lateral aclexplode(d.defaclacl) acl
          where d.defaclrole='postgres'::regrole and d.defaclobjtype='f' and n.nspname='api'
            and acl.grantee='service_role'::regrole and acl.privilege_type='EXECUTE' and acl.is_grantable
        )
      )::text;
    """).stdout.strip()
    if json.loads(explicit_global) != {
        "anon": True,
        "authenticated": True,
        "serviceGlobalGrantable": True,
        "serviceApiGrantable": True,
    }:
        raise SystemExit("global and per-schema function-default fixture was not established")

    before_acl = current_object_acl_signature(database_url)
    before_defaults = function_default_layer_signature(database_url)
    before_rows = application_row_count_signature(database_url)
    migration = MIGRATION.read_text(encoding="utf-8")
    for required in (
        "alter default privileges for role postgres\n  revoke execute on functions",
        "defaultPrivilegeEvaluation",
        "acldefault(targets.object_type, targets.owner_oid)",
    ):
        if required not in migration:
            raise SystemExit(f"migration effective-default contract missing: {required}")

    failed = migration.rsplit("commit;", 1)[0] + "select 1/0;\ncommit;\n"
    psql(database_url, failed, expect_failure=True)
    if current_object_acl_signature(database_url) != before_acl:
        raise SystemExit("failed migration changed current object ACLs")
    if function_default_layer_signature(database_url) != before_defaults:
        raise SystemExit("failed migration changed global or per-schema function defaults")
    residue = psql(database_url, r"""
      select concat_ws(',',
        to_regclass('util.security_acl_effective_default_privileges'),
        to_regclass('archive.security_acl_postgres_global_functions_20260801_snapshot')
      );
    """).stdout.strip()
    if residue:
        raise SystemExit(f"failed migration left snapshot/effective-default residue: {residue}")

    psql(database_url, migration)
    after_acl = current_object_acl_signature(database_url)
    if after_acl != before_acl:
        raise SystemExit("global-default migration drifted current object ACLs")
    if application_row_count_signature(database_url) != before_rows:
        raise SystemExit("global-default migration changed application row counts")
    snapshot_acl = json.loads(psql(database_url, r"""
      select jsonb_build_object(
        'customDenied',not has_table_privilege(
          'issue_339_snapshot_reader',
          'archive.security_acl_postgres_global_functions_20260801_snapshot','SELECT'
        ),
        'ownerAllowed',has_table_privilege(
          'postgres','archive.security_acl_postgres_global_functions_20260801_snapshot','SELECT'
        ),
        'nonOwnerAclCount',(
          select count(*)
          from pg_class relation
          cross join lateral aclexplode(coalesce(
            relation.relacl,acldefault('r',relation.relowner)
          )) acl
          where relation.oid='archive.security_acl_postgres_global_functions_20260801_snapshot'::regclass
            and acl.grantee<>relation.relowner
        ),
        'apiGrantOptionCaptured',exists(
          select 1
          from archive.security_acl_postgres_global_functions_20260801_snapshot
          where scope_schema='api' and grantee='service_role'
            and privilege_type='EXECUTE' and is_grantable
        )
      )::text;
    """).stdout.strip())
    if snapshot_acl != {
        "customDenied": True,
        "ownerAllowed": True,
        "nonOwnerAclCount": 0,
        "apiGrantOptionCaptured": True,
    }:
        raise SystemExit(f"snapshot owner-only/layer capture failed: {snapshot_acl}")

    # Remove only the custom fixture's still-active table default before the
    # final catalog is inspected; the snapshot itself no longer references it.
    psql(database_url, r"""
      alter default privileges for role postgres in schema archive
        revoke select on tables from issue_339_snapshot_reader;
      drop role issue_339_snapshot_reader;
    """)

    psql(database_url, migration)
    if current_object_acl_signature(database_url) != after_acl:
        raise SystemExit("idempotent retry drifted current object ACLs")
    run(supabase_command("test", "db", str(TEST.relative_to(ROOT)), "--local"))

    run(["psql", database_url, "-X", "-v", "ON_ERROR_STOP=1", "-f", str(RESTORE)])
    if function_default_layer_signature(database_url) != before_defaults:
        raise SystemExit("restore did not reproduce every global/per-schema function-default layer")
    if current_object_acl_signature(database_url) != after_acl:
        raise SystemExit("restore changed current object ACLs")
    if application_row_count_signature(database_url) != before_rows:
        raise SystemExit("restore changed application row counts")

    psql(database_url, migration)
    run(supabase_command("test", "db", str(TEST.relative_to(ROOT)), "--local"))
    print(
        "PASS PG17 implicit PUBLIC default; failure atomic; effective global+schema gate; "
        "dynamic owner-only snapshot ACL; additive grant-option restore; retry stable; "
        "current ACL/data parity; exact layered restore; roll-forward stable"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
