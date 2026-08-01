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
      create role "issue355 quoted role" nologin;
      grant issue355_drift_role to postgres;
      grant create on schema api, private to issue355_drift_role;
      grant select on api.teams_v1 to supabase_auth_admin;
      grant select (id) on api.teams_v1 to "issue355 quoted role";
      grant select ("json") on private.teams to issue355_acl_group;
      alter view api.teams_v1 owner to issue355_drift_role;
      grant execute on function private.review_scope_checksum_v1(jsonb) to supabase_auth_admin;
      alter function private.review_scope_checksum_v1(jsonb) owner to issue355_drift_role;
    """)
    inherited_column_acl = psql(db_url, """
      select has_column_privilege('issue355 quoted role','api.teams_v1','id','select')
        || '|' ||
        has_column_privilege('issue355_acl_member','private.teams','json','select');
    """)
    if inherited_column_acl != "true|true":
        raise SystemExit("Issue #355 column-ACL negative fixture did not establish quoted/inherited grants")
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
    psql(db_url, """
      revoke create on schema api, private from issue355_drift_role;
      revoke issue355_drift_role from postgres;
      drop role issue355_drift_role;
      drop role issue355_acl_member;
      drop role issue355_acl_group;
      drop role "issue355 quoted role";
    """)
    if target_fingerprint(db_url) != target_before or public_routine_fingerprint(db_url) != public_before:
        raise SystemExit("Issue #355 exact-state reapply did not converge owner/ACL/catalog state")

    psql(db_url, """
      create role issue355_column_tamper nologin;
      grant select (id) on api.teams_v1 to issue355_column_tamper;
    """)
    require_rejected(apply_file(db_url, ROLLBACK, check=False), "tampered target column ACL")
    psql(db_url, """
      revoke select (id) on api.teams_v1 from issue355_column_tamper;
      drop role issue355_column_tamper;
    """)
    if target_fingerprint(db_url) != target_before:
        raise SystemExit("Issue #355 column-ACL tamper negative changed target state")

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
