#!/usr/bin/env python3
"""Exact-state reapply, rollback, and lock-failure proof for Issue #355."""

from __future__ import annotations

import hashlib
import subprocess
import time
from pathlib import Path

from identity_collaboration_target import verified_database_url

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "supabase/migrations/20260801061000_issue_355_identity_collaboration_expand.sql"
ROLLBACK = ROOT / "supabase/operator/issue_355_restore_identity_collaboration_expand.sql"
ROUTINES = (
    "review_append_scope_snapshot_v1", "review_revision_fingerprint_v1",
    "review_scope_all_reference_ids_v1", "review_scope_checksum_v1",
    "review_scope_current_items_v1", "review_scope_current_reference_ids_v1",
    "review_scope_current_snapshot_v1", "review_validate_scope_history_v1",
)


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if check and result.returncode != 0:
        raise SystemExit(
            f"Issue #355 qualification command failed: {command[0]}: {result.stderr.strip()}"
        )
    return result


def psql(db_url: str, sql: str) -> str:
    result = subprocess.run(
        ["psql", db_url, "-XAt", "-v", "ON_ERROR_STOP=1"], cwd=ROOT,
        input=sql, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise SystemExit(f"Issue #355 qualification SQL failed: {result.stderr.strip()}")
    return result.stdout.strip()


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def public_routine_fingerprint(db_url: str) -> str:
    names = ",".join(f"'{name}'" for name in ROUTINES)
    return digest(psql(db_url, f"""
      select string_agg(concat_ws(':',p.oid,p.proowner,p.prolang,p.prokind,
        p.provolatile,p.prosecdef,p.proisstrict,p.proparallel,
        p.proleakproof,p.procost,p.prorows,coalesce(p.proconfig::text,''),
        coalesce(p.proacl::text,''),pg_get_function_result(p.oid),
        pg_get_functiondef(p.oid),coalesce(obj_description(p.oid,'pg_proc'),''),
        coalesce((select string_agg(d.classid::text||':'||d.objid||':'||d.objsubid||':'||
          d.refclassid::text||':'||d.refobjid||':'||d.refobjsubid||':'||d.deptype::text,'|' order by
          d.classid,d.objid,d.objsubid,d.refclassid,d.refobjid,d.refobjsubid,d.deptype)
          from pg_depend d where d.objid=p.oid or d.refobjid=p.oid),''))
        ,'|' order by p.proname,p.proargtypes)
      from pg_proc p join pg_namespace n on n.oid=p.pronamespace
      where n.nspname='public' and p.proname in ({names});
    """))


def target_fingerprint(db_url: str, *, include_oid: bool = True) -> str:
    names = ",".join(f"'{name}'" for name in ROUTINES)
    return digest(psql(db_url, f"""
      select string_agg(value,'|' order by value) from (
        select concat_ws(':',n.nspname||'.'||c.relname,
          case when {str(include_oid).lower()} then c.oid::text else '' end,
          c.relowner,c.relkind,coalesce(c.relacl::text,''),
          coalesce((select string_agg(a.attname||':'||
            case when x.grantee=0 then 'PUBLIC' else grantee.rolname end||':'||
            x.privilege_type||':'||x.is_grantable::text||':'||
            case when x.grantor=0 then 'PUBLIC' else grantor.rolname end,
            '|' order by a.attname,
            case when x.grantee=0 then 'PUBLIC' else grantee.rolname end,
            x.privilege_type,x.is_grantable,
            case when x.grantor=0 then 'PUBLIC' else grantor.rolname end)
            from pg_attribute a cross join lateral aclexplode(a.attacl) x
            left join pg_roles grantee on grantee.oid=x.grantee
            left join pg_roles grantor on grantor.oid=x.grantor
            where a.attrelid=c.oid and a.attnum>0 and not a.attisdropped),''),
          coalesce(c.reloptions::text,''),pg_get_viewdef(c.oid,true),
          coalesce(obj_description(c.oid,'pg_class'),''),
          coalesce((select string_agg(pg_describe_object(d.classid,d.objid,d.objsubid)||':'||
            pg_describe_object(d.refclassid,d.refobjid,d.refobjsubid)||':'||d.deptype::text,'|' order by
            pg_describe_object(d.classid,d.objid,d.objsubid),
            pg_describe_object(d.refclassid,d.refobjid,d.refobjsubid),d.deptype)
            from pg_depend d where d.objid=c.oid or d.refobjid=c.oid),'')) value
        from pg_class c join pg_namespace n on n.oid=c.relnamespace
        where (n.nspname='private' and c.relname in ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users'))
           or (n.nspname='api' and c.relname in ('notifications_v1','reviews_v1','team_roles_v1','teams_v1','user_profiles_v1','identity_center_processed_events_v1','identity_center_users_v1'))
        union all
        select concat_ws(':',n.nspname||'.'||p.proname,
          case when {str(include_oid).lower()} then p.oid::text else '' end,
          p.proowner,p.prolang,p.prokind,p.provolatile,p.prosecdef,
          p.proisstrict,p.proparallel,p.proleakproof,p.procost,p.prorows,
          coalesce(p.proconfig::text,''),coalesce(p.proacl::text,''),
          pg_get_function_result(p.oid),pg_get_functiondef(p.oid),
          coalesce(obj_description(p.oid,'pg_proc'),''),
          coalesce((select string_agg(pg_describe_object(d.classid,d.objid,d.objsubid)||':'||
            pg_describe_object(d.refclassid,d.refobjid,d.refobjsubid)||':'||d.deptype::text,'|' order by
            pg_describe_object(d.classid,d.objid,d.objsubid),
            pg_describe_object(d.refclassid,d.refobjid,d.refobjsubid),d.deptype)
            from pg_depend d where d.objid=p.oid or d.refobjid=p.oid),''))
        from pg_proc p join pg_namespace n on n.oid=p.pronamespace
        where n.nspname='private' and p.proname in ({names})
      ) state;
    """))


def unrelated_acl_fingerprint(db_url: str) -> str:
    """Normalize relation, column, and routine ACLs on isolation proofs."""
    return digest(psql(db_url, """
      select concat_ws('|',
        coalesce((select string_agg(
          case when x.grantee=0 then 'PUBLIC' else grantee.rolname end||':'||
          x.privilege_type||':'||x.is_grantable::text||':'||
          case when x.grantor=0 then 'PUBLIC' else grantor.rolname end,
          '|' order by case when x.grantee=0 then 'PUBLIC' else grantee.rolname end,
          x.privilege_type,x.is_grantable,
          case when x.grantor=0 then 'PUBLIC' else grantor.rolname end)
          from pg_class c cross join lateral
            aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) x
          left join pg_roles grantee on grantee.oid=x.grantee
          left join pg_roles grantor on grantor.oid=x.grantor
          where c.oid='private.issue355_acl_unrelated'::regclass),''),
        coalesce((select string_agg(a.attname||':'||
          case when x.grantee=0 then 'PUBLIC' else grantee.rolname end||':'||
          x.privilege_type||':'||x.is_grantable::text||':'||
          case when x.grantor=0 then 'PUBLIC' else grantor.rolname end,
          '|' order by a.attname,
          case when x.grantee=0 then 'PUBLIC' else grantee.rolname end,
          x.privilege_type,x.is_grantable,
          case when x.grantor=0 then 'PUBLIC' else grantor.rolname end)
          from pg_attribute a cross join lateral aclexplode(a.attacl) x
          left join pg_roles grantee on grantee.oid=x.grantee
          left join pg_roles grantor on grantor.oid=x.grantor
          where a.attrelid='private.issue355_acl_unrelated'::regclass
            and a.attnum>0 and not a.attisdropped),''),
        coalesce((select string_agg(
          case when x.grantee=0 then 'PUBLIC' else grantee.rolname end||':'||
          x.privilege_type||':'||x.is_grantable::text||':'||
          case when x.grantor=0 then 'PUBLIC' else grantor.rolname end,
          '|' order by case when x.grantee=0 then 'PUBLIC' else grantee.rolname end,
          x.privilege_type,x.is_grantable,
          case when x.grantor=0 then 'PUBLIC' else grantor.rolname end)
          from pg_proc p cross join lateral
            aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) x
          left join pg_roles grantee on grantee.oid=x.grantee
          left join pg_roles grantor on grantor.oid=x.grantor
          where p.oid='private.issue355_acl_unrelated_routine(integer)'::regprocedure),''));
    """))


def standard_privilege_fingerprint(db_url: str) -> str:
    """Capture the reviewed standard-role relation/routine privilege matrix."""
    names = ",".join(f"'{name}'" for name in ROUTINES)
    return digest(psql(db_url, f"""
      with roles(role_name) as (values
        ('anon'),('authenticated'),('service_role'),('api_internal_executor')
      ), objects(object_oid,object_key,object_kind) as (
        select c.oid,n.nspname||'.'||c.relname,'relation'
        from pg_class c join pg_namespace n on n.oid=c.relnamespace
        where (n.nspname='private' and c.relname in
          ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users'))
           or (n.nspname='api' and c.relname in
          ('notifications_v1','reviews_v1','team_roles_v1','teams_v1','user_profiles_v1',
           'identity_center_processed_events_v1','identity_center_users_v1'))
        union all
        select p.oid,n.nspname||'.'||p.proname||'('||oidvectortypes(p.proargtypes)||')','routine'
        from pg_proc p join pg_namespace n on n.oid=p.pronamespace
        where n.nspname='private' and p.proname in ({names})
      )
      select string_agg(role_name||':'||object_key||':'||
        case when object_kind='relation'
          then has_table_privilege(role_name,object_oid,'select')
          else has_function_privilege(role_name,object_oid,'execute') end::text,
        '|' order by role_name,object_key)
      from roles cross join objects;
    """))


def assert_zero_targets(db_url: str) -> None:
    names = ",".join(f"'{name}'" for name in ROUTINES)
    remaining = psql(db_url, f"""
      select
        (select count(*) from pg_class c join pg_namespace n on n.oid=c.relnamespace
         where (n.nspname='private' and c.relname in
           ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users'))
            or (n.nspname='api' and c.relname in
           ('notifications_v1','reviews_v1','team_roles_v1','teams_v1','user_profiles_v1','identity_center_processed_events_v1','identity_center_users_v1')))
        +
        (select count(*) from pg_proc p join pg_namespace n on n.oid=p.pronamespace
         where n.nspname='private' and p.proname in ({names}));
    """)
    if remaining != "0":
        raise SystemExit(f"Issue #355 rollback left {remaining} target objects")


def apply_file(db_url: str, path: Path, *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return run(["psql", db_url, "-X", "-v", "ON_ERROR_STOP=1", "-f", str(path)], check=check)


def require_rejected(result: subprocess.CompletedProcess[str], label: str) -> None:
    if result.returncode == 0:
        raise SystemExit(f"Issue #355 negative unexpectedly succeeded: {label}")


def main() -> int:
    db_url = verified_database_url()
    public_before = public_routine_fingerprint(db_url)
    target_before = target_fingerprint(db_url)
    target_logical_before = target_fingerprint(db_url, include_oid=False)
    standard_privileges_before = standard_privilege_fingerprint(db_url)

    psql(db_url, """
      create function public.review_scope_checksum_v1(text) returns text
      language sql immutable strict set search_path='' as 'select $1';
    """)
    require_rejected(apply_file(db_url, MIGRATION, check=False), "extra public overload")
    if target_fingerprint(db_url) != target_before:
        raise SystemExit("Issue #355 extra-overload rejection changed target state")
    psql(db_url, "drop function public.review_scope_checksum_v1(text)")

    psql(db_url, "alter function public.review_scope_checksum_v1(jsonb) cost 101")
    require_rejected(apply_file(db_url, MIGRATION, check=False), "public routine property drift")
    if target_fingerprint(db_url) != target_before:
        raise SystemExit("Issue #355 source-drift rejection changed target state")
    psql(db_url, "alter function public.review_scope_checksum_v1(jsonb) cost 100")
    if public_routine_fingerprint(db_url) != public_before:
        raise SystemExit("Issue #355 source-drift negative did not restore predecessor state")

    psql(db_url, """
      create role issue355_drift_role nologin;
      create role issue355_acl_group nologin;
      create role issue355_acl_member nologin in role issue355_acl_group;
      -- Create every delegation chain leaf-first.  The grant root therefore
      -- has the highest role OID even though its ACL item is inserted first.
      create role issue355_relation_leaf nologin;
      create role issue355_relation_middle nologin;
      create role "issue355 quoted role" nologin;
      create role issue355_column_leaf nologin;
      create role issue355_column_middle nologin;
      create role issue355_column_root nologin;
      create role issue355_routine_leaf nologin;
      create role issue355_routine_middle nologin;
      create role issue355_routine_root nologin;
      create role issue355_all_columns nologin;
      grant issue355_drift_role to postgres;
      grant "issue355 quoted role", issue355_relation_middle,
        issue355_column_root, issue355_column_middle,
        issue355_routine_root, issue355_routine_middle to postgres;
      grant usage on schema api, private to "issue355 quoted role",
        issue355_relation_middle, issue355_column_root, issue355_column_middle,
        issue355_routine_root, issue355_routine_middle;
      grant create on schema api, private to issue355_drift_role;
      grant select on api.teams_v1 to supabase_auth_admin;
      grant select on api.identity_center_users_v1 to public;
      grant select (keycloak_sub) on api.identity_center_users_v1 to public;
      grant select on private.roles to issue355_acl_group;
      grant select ("json") on private.teams to issue355_acl_group;

      -- Quoted root/grantor plus a three-role relation delegation chain.
      grant select on api.teams_v1 to "issue355 quoted role" with grant option;
      set role "issue355 quoted role";
      grant select on api.teams_v1 to issue355_relation_middle with grant option;
      reset role;
      set role issue355_relation_middle;
      grant select on api.teams_v1 to issue355_relation_leaf;
      reset role;

      grant select (id) on private.teams to issue355_column_root with grant option;
      set role issue355_column_root;
      grant select (id) on private.teams to issue355_column_middle with grant option;
      reset role;
      set role issue355_column_middle;
      grant select (id) on private.teams to issue355_column_leaf;
      reset role;

      revoke execute on function private.review_scope_checksum_v1(jsonb) from public;
      grant execute on function private.review_scope_checksum_v1(jsonb)
        to issue355_routine_root with grant option;
      set role issue355_routine_root;
      grant execute on function private.review_scope_checksum_v1(jsonb)
        to issue355_routine_middle with grant option;
      reset role;
      set role issue355_routine_middle;
      grant execute on function private.review_scope_checksum_v1(jsonb)
        to issue355_routine_leaf;
      reset role;
      grant execute on function private.review_scope_current_items_v1(jsonb) to public;

      -- Put a column ACL on every one of the 15 target views so the replay
      -- proves all target attacls transition from effective to absent.
      do $all_target_columns$
      declare target record;
      begin
        for target in
          select c.oid::regclass as relation_name,
            (select a.attname from pg_attribute a where a.attrelid=c.oid
              and a.attnum>0 and not a.attisdropped order by a.attnum limit 1) as column_name
          from pg_class c join pg_namespace n on n.oid=c.relnamespace
          where (n.nspname='private' and c.relname in
            ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users'))
             or (n.nspname='api' and c.relname in
            ('notifications_v1','reviews_v1','team_roles_v1','teams_v1','user_profiles_v1',
             'identity_center_processed_events_v1','identity_center_users_v1'))
        loop
          execute format('grant select (%I) on table %s to issue355_all_columns',
            target.column_name,target.relation_name);
        end loop;
      end
      $all_target_columns$;

      create table private.issue355_acl_unrelated(id integer, payload text);
      create function private.issue355_acl_unrelated_routine(integer) returns integer
        language sql immutable strict set search_path='' as 'select $1';
      revoke execute on function private.issue355_acl_unrelated_routine(integer) from public;
      grant select on private.issue355_acl_unrelated to "issue355 quoted role" with grant option;
      set role "issue355 quoted role";
      grant select on private.issue355_acl_unrelated to issue355_relation_middle with grant option;
      reset role;
      set role issue355_relation_middle;
      grant select on private.issue355_acl_unrelated to issue355_relation_leaf;
      reset role;
      grant select (id) on private.issue355_acl_unrelated to issue355_column_root with grant option;
      set role issue355_column_root;
      grant select (id) on private.issue355_acl_unrelated to issue355_column_middle with grant option;
      reset role;
      set role issue355_column_middle;
      grant select (id) on private.issue355_acl_unrelated to issue355_column_leaf;
      reset role;
      grant execute on function private.issue355_acl_unrelated_routine(integer)
        to issue355_routine_root with grant option;
      set role issue355_routine_root;
      grant execute on function private.issue355_acl_unrelated_routine(integer)
        to issue355_routine_middle with grant option;
      reset role;
      set role issue355_routine_middle;
      grant execute on function private.issue355_acl_unrelated_routine(integer)
        to issue355_routine_leaf;
      reset role;

      alter view api.teams_v1 owner to issue355_drift_role;
      grant execute on function private.review_scope_checksum_v1(jsonb) to supabase_auth_admin;
      alter function private.review_scope_checksum_v1(jsonb) owner to issue355_drift_role;
    """)
    established_acl_variants = psql(db_url, """
      select has_table_privilege('issue355 quoted role','api.teams_v1','select')||'|'||
        has_table_privilege('issue355_acl_member','private.roles','select')||'|'||
        has_column_privilege('issue355_acl_member','private.teams','json','select')||'|'||
        has_table_privilege('issue355_relation_leaf','api.teams_v1','select')||'|'||
        has_column_privilege('issue355_column_leaf','private.teams','id','select')||'|'||
        has_function_privilege('issue355_routine_leaf','private.review_scope_checksum_v1(jsonb)','execute')||'|'||
        has_function_privilege('issue355_routine_leaf','private.review_scope_current_items_v1(jsonb)','execute')||'|'||
        has_table_privilege('issue355_column_leaf','api.identity_center_users_v1','select')||'|'||
        has_column_privilege('issue355_column_leaf','api.identity_center_users_v1','keycloak_sub','select')||'|'||
        (select count(*)=15 from pg_class c join pg_namespace n on n.oid=c.relnamespace
          where ((n.nspname='private' and c.relname in
            ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users'))
             or (n.nspname='api' and c.relname in
            ('notifications_v1','reviews_v1','team_roles_v1','teams_v1','user_profiles_v1',
             'identity_center_processed_events_v1','identity_center_users_v1')))
            and has_any_column_privilege('issue355_all_columns',c.oid,'select'))||'|'||
        ((select oid from pg_roles where rolname='issue355 quoted role')>
         (select oid from pg_roles where rolname='issue355_relation_leaf'))||'|'||
        ((select oid from pg_roles where rolname='issue355_column_root')>
         (select oid from pg_roles where rolname='issue355_column_leaf'))||'|'||
        ((select oid from pg_roles where rolname='issue355_routine_root')>
         (select oid from pg_roles where rolname='issue355_routine_leaf'))||'|'||
        (select min(x.ordinality) filter (where x.grantee=(select oid from pg_roles where rolname='issue355 quoted role'))<
                min(x.ordinality) filter (where x.grantee=to_regrole('issue355_relation_leaf'))
          from pg_class c cross join lateral
            aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) with ordinality x
          where c.oid='api.teams_v1'::regclass)||'|'||
        (select min(x.ordinality) filter (where x.grantee=to_regrole('issue355_column_root'))<
                min(x.ordinality) filter (where x.grantee=to_regrole('issue355_column_leaf'))
          from pg_attribute a cross join lateral aclexplode(a.attacl) with ordinality x
          where a.attrelid='private.teams'::regclass and a.attname='id')||'|'||
        (select min(x.ordinality) filter (where x.grantee=to_regrole('issue355_routine_root'))<
                min(x.ordinality) filter (where x.grantee=to_regrole('issue355_routine_leaf'))
          from pg_proc p cross join lateral
            aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) with ordinality x
          where p.oid='private.review_scope_checksum_v1(jsonb)'::regprocedure);
    """)
    if established_acl_variants != "|".join(["true"] * 16):
        raise SystemExit(
            "Issue #355 ACL negative fixture did not establish all grant variants: "
            f"{established_acl_variants}"
        )
    target_chain_before = target_fingerprint(db_url)
    unrelated_before = unrelated_acl_fingerprint(db_url)

    # Fail only after relation, column, and routine chains have all disappeared.
    # Earlier reviewed GRANTs are allowed through; the first adapter re-grant
    # after all three target-scoped CASCADE passes injects the failure.
    psql(db_url, """
      create function public.issue355_fail_after_acl_cascade() returns event_trigger
      language plpgsql as $failure$
      begin
        if tg_tag='GRANT'
           and not has_table_privilege('issue355_relation_leaf','api.teams_v1','select')
           and not has_column_privilege('issue355_column_leaf','private.teams','id','select')
           and not has_function_privilege('issue355_routine_leaf',
             'private.review_scope_checksum_v1(jsonb)','execute') then
          raise exception 'Issue #355 injected post-CASCADE failure';
        end if;
      end
      $failure$;
      create event trigger issue355_fail_after_acl_cascade on ddl_command_end
        when tag in ('GRANT') execute function public.issue355_fail_after_acl_cascade();
    """)
    require_rejected(apply_file(db_url, MIGRATION, check=False), "post-CASCADE failure atomicity")
    if target_fingerprint(db_url) != target_chain_before:
        raise SystemExit("Issue #355 post-CASCADE failure did not restore target grant chains")
    if unrelated_acl_fingerprint(db_url) != unrelated_before:
        raise SystemExit("Issue #355 post-CASCADE failure changed unrelated relation/column/routine ACL chains")
    psql(db_url, """
      drop event trigger issue355_fail_after_acl_cascade;
      drop function public.issue355_fail_after_acl_cascade();
    """)

    apply_file(db_url, MIGRATION)
    remaining_column_acl = psql(db_url, """
      select count(*)
      from pg_class c join pg_namespace n on n.oid=c.relnamespace
      join pg_attribute a on a.attrelid=c.oid and a.attnum>0 and not a.attisdropped
      cross join lateral aclexplode(a.attacl) acl
      where (n.nspname='private' and c.relname in
        ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users'))
         or (n.nspname='api' and c.relname in
        ('notifications_v1','reviews_v1','team_roles_v1','teams_v1','user_profiles_v1',
         'identity_center_processed_events_v1','identity_center_users_v1'));
    """)
    if remaining_column_acl != "0":
        raise SystemExit("Issue #355 migration left target column ACL entries")
    if unrelated_acl_fingerprint(db_url) != unrelated_before:
        raise SystemExit("Issue #355 target-scoped CASCADE changed unrelated relation/column/routine ACL chains")

    converged_acl_state = psql(db_url, """
      select
        (not has_table_privilege('issue355 quoted role','api.teams_v1','select'))||'|'||
        (not has_table_privilege('issue355_acl_member','private.roles','select'))||'|'||
        (not has_column_privilege('issue355_acl_member','private.teams','json','select'))||'|'||
        (not has_table_privilege('issue355_relation_leaf','api.teams_v1','select'))||'|'||
        (not has_column_privilege('issue355_column_leaf','private.teams','id','select'))||'|'||
        (not has_function_privilege('issue355_routine_leaf','private.review_scope_checksum_v1(jsonb)','execute'))||'|'||
        (not has_function_privilege('issue355_routine_leaf','private.review_scope_current_items_v1(jsonb)','execute'))||'|'||
        (not has_table_privilege('issue355_column_leaf','api.identity_center_users_v1','select'))||'|'||
        (not has_column_privilege('issue355_column_leaf','api.identity_center_users_v1','keycloak_sub','select'))||'|'||
        (select count(*)=15 from pg_class c join pg_namespace n on n.oid=c.relnamespace
          where ((n.nspname='private' and c.relname in
            ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users'))
             or (n.nspname='api' and c.relname in
            ('notifications_v1','reviews_v1','team_roles_v1','teams_v1','user_profiles_v1',
             'identity_center_processed_events_v1','identity_center_users_v1')))
            and not has_any_column_privilege('issue355_all_columns',c.oid,'select'))||'|'||
        (select count(*)=0
          from pg_class c join pg_namespace n on n.oid=c.relnamespace
          join pg_attribute a on a.attrelid=c.oid and a.attnum>0 and not a.attisdropped
          cross join lateral aclexplode(a.attacl) x
          left join pg_roles grantee on grantee.oid=x.grantee
          left join pg_roles grantor on grantor.oid=x.grantor
          where ((n.nspname='private' and c.relname in
            ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users'))
             or (n.nspname='api' and c.relname in
            ('notifications_v1','reviews_v1','team_roles_v1','teams_v1','user_profiles_v1',
             'identity_center_processed_events_v1','identity_center_users_v1')))
            and ((x.grantee<>0 and grantee.oid is null) or (x.grantor<>0 and grantor.oid is null)))||'|'||
        (select bool_and(has_column_privilege('postgres',c.oid,a.attname,'select'))
          from pg_class c join pg_namespace n on n.oid=c.relnamespace
          join pg_attribute a on a.attrelid=c.oid and a.attnum>0 and not a.attisdropped
          where (n.nspname='private' and c.relname in
            ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users'))
             or (n.nspname='api' and c.relname in
            ('notifications_v1','reviews_v1','team_roles_v1','teams_v1','user_profiles_v1',
             'identity_center_processed_events_v1','identity_center_users_v1')));
    """)
    if converged_acl_state != "|".join(["true"] * 12):
        raise SystemExit("Issue #355 ACL replay did not remove every effective drift variant")
    if standard_privilege_fingerprint(db_url) != standard_privileges_before:
        raise SystemExit("Issue #355 replay changed the reviewed standard-role privilege matrix")
    psql(db_url, """
      drop table private.issue355_acl_unrelated;
      drop function private.issue355_acl_unrelated_routine(integer);
      revoke create on schema api, private from issue355_drift_role;
      revoke issue355_drift_role from postgres;
      revoke "issue355 quoted role", issue355_relation_middle,
        issue355_column_root, issue355_column_middle,
        issue355_routine_root, issue355_routine_middle from postgres;
      revoke usage on schema api, private from "issue355 quoted role",
        issue355_relation_middle, issue355_column_root, issue355_column_middle,
        issue355_routine_root, issue355_routine_middle;
      drop role issue355_drift_role;
      drop role issue355_acl_member;
      drop role issue355_acl_group;
      drop role "issue355 quoted role";
      drop role issue355_relation_leaf;
      drop role issue355_relation_middle;
      drop role issue355_column_leaf;
      drop role issue355_column_middle;
      drop role issue355_column_root;
      drop role issue355_routine_leaf;
      drop role issue355_routine_middle;
      drop role issue355_routine_root;
      drop role issue355_all_columns;
    """)
    if target_fingerprint(db_url) != target_before or public_routine_fingerprint(db_url) != public_before:
        raise SystemExit("Issue #355 exact-state reapply did not converge owner/ACL/catalog state")

    psql(db_url, """
      create role issue355_tamper_leaf nologin;
      create role issue355_tamper_middle nologin;
      create role issue355_tamper_root nologin;
      grant issue355_tamper_root, issue355_tamper_middle to postgres;
      grant usage on schema api, private to issue355_tamper_root, issue355_tamper_middle;
      grant select on api.teams_v1 to issue355_tamper_root with grant option;
      set role issue355_tamper_root;
      grant select on api.teams_v1 to issue355_tamper_middle with grant option;
      reset role;
      set role issue355_tamper_middle;
      grant select on api.teams_v1 to issue355_tamper_leaf;
      reset role;
    """)
    require_rejected(apply_file(db_url, ROLLBACK, check=False), "tampered relation grant-option chain")
    psql(db_url, "revoke all privileges on api.teams_v1 from issue355_tamper_root cascade")
    if target_fingerprint(db_url) != target_before:
        raise SystemExit("Issue #355 relation-chain tamper negative changed target state")

    psql(db_url, """
      grant select (id) on api.teams_v1 to issue355_tamper_root with grant option;
      set role issue355_tamper_root;
      grant select (id) on api.teams_v1 to issue355_tamper_middle with grant option;
      reset role;
      set role issue355_tamper_middle;
      grant select (id) on api.teams_v1 to issue355_tamper_leaf;
      reset role;
    """)
    require_rejected(apply_file(db_url, ROLLBACK, check=False), "tampered column grant-option chain")
    psql(db_url, "revoke all privileges (id) on api.teams_v1 from issue355_tamper_root cascade")
    if target_fingerprint(db_url) != target_before:
        raise SystemExit("Issue #355 column-chain tamper negative changed target state")

    psql(db_url, """
      grant execute on function private.review_scope_checksum_v1(jsonb)
        to issue355_tamper_root with grant option;
      set role issue355_tamper_root;
      grant execute on function private.review_scope_checksum_v1(jsonb)
        to issue355_tamper_middle with grant option;
      reset role;
      set role issue355_tamper_middle;
      grant execute on function private.review_scope_checksum_v1(jsonb)
        to issue355_tamper_leaf;
      reset role;
    """)
    require_rejected(apply_file(db_url, ROLLBACK, check=False), "tampered routine grant-option chain")
    psql(db_url, """
      revoke all privileges on function private.review_scope_checksum_v1(jsonb)
        from issue355_tamper_root cascade;
      revoke issue355_tamper_root, issue355_tamper_middle from postgres;
      revoke usage on schema api, private from issue355_tamper_root, issue355_tamper_middle;
      drop role issue355_tamper_leaf;
      drop role issue355_tamper_middle;
      drop role issue355_tamper_root;
    """)
    if target_fingerprint(db_url) != target_before:
        raise SystemExit("Issue #355 routine-chain tamper negative changed target state")

    psql(db_url, "comment on view api.teams_v1 is 'issue355-tamper'")
    require_rejected(apply_file(db_url, ROLLBACK, check=False), "tampered target comment")
    psql(db_url, "comment on view api.teams_v1 is null")
    if target_fingerprint(db_url) != target_before:
        raise SystemExit("Issue #355 rollback tamper negative changed target state")

    psql(db_url, """
      insert into supabase_migrations.schema_migrations(version,statements,name)
      values ('99999999999999',array[]::text[],'issue355_negative');
    """)
    require_rejected(apply_file(db_url, ROLLBACK, check=False), "non-exact migration head")
    psql(db_url, "delete from supabase_migrations.schema_migrations where version='99999999999999'")
    if target_fingerprint(db_url) != target_before:
        raise SystemExit("Issue #355 migration-head negative changed target state")

    psql(db_url, "drop view api.teams_v1")
    require_rejected(apply_file(db_url, ROLLBACK, check=False), "partial target set")
    apply_file(db_url, MIGRATION)
    if target_fingerprint(db_url, include_oid=False) != target_logical_before:
        raise SystemExit("Issue #355 partial-target negative did not restore exact state")

    apply_file(db_url, ROLLBACK)
    assert_zero_targets(db_url)
    apply_file(db_url, ROLLBACK)
    assert_zero_targets(db_url)
    if public_routine_fingerprint(db_url) != public_before:
        raise SystemExit("Issue #355 rollback changed audited public routines")
    apply_file(db_url, MIGRATION)
    if target_fingerprint(db_url, include_oid=False) != target_logical_before:
        raise SystemExit("Issue #355 roll-forward did not restore exact logical state")
    target_after_rollforward = target_fingerprint(db_url)

    holder = subprocess.Popen(
        ["psql", db_url, "-X", "-v", "ON_ERROR_STOP=1", "-c",
         "begin; lock table public.comments in access exclusive mode; select pg_sleep(8); rollback;"],
        cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        time.sleep(1)
        started = time.monotonic()
        failed = apply_file(db_url, MIGRATION, check=False)
        elapsed = time.monotonic() - started
        if failed.returncode == 0 or not 4.5 <= elapsed <= 7.5:
            raise SystemExit("Issue #355 lock failure was not bounded and fail-closed")
    finally:
        holder.wait(timeout=10)
    if target_fingerprint(db_url) != target_after_rollforward or public_routine_fingerprint(db_url) != public_before:
        raise SystemExit("Issue #355 lock failure left partial state")

    print("PASS Issue #355 exact reapply, repeatable rollback/roll-forward, and failure atomicity")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
