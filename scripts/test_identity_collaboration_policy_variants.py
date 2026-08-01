#!/usr/bin/env python3
"""Disposable-local proof for the two exact Issue #355 users-policy inputs."""

from __future__ import annotations

import subprocess
from pathlib import Path

from identity_collaboration_target import verified_database_url

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "supabase/migrations/20260801061000_issue_355_identity_collaboration_expand.sql"
ROLLBACK = ROOT / "supabase/operator/issue_355_restore_identity_collaboration_expand.sql"
CANONICAL_HASH = "57fd9c26617c29dc6edc92d231bbec85"
LIVE_LEGACY_HASH = "6ab74729e7e0ec6e9378542059d17cd0"

POLICY_HASH_SQL = r"""
select md5(coalesce(string_agg(
  policy.polname||':'||policy.polcmd::text||':'||policy.polpermissive::text||':'||
  coalesce((select string_agg(case when role_oid=0 then 'PUBLIC' else role_name.rolname end,
    ',' order by case when role_oid=0 then 'PUBLIC' else role_name.rolname end)
    from unnest(policy.polroles) role_oid
    left join pg_roles role_name on role_name.oid=role_oid),'')||':'||
  coalesce(pg_get_expr(policy.polqual,policy.polrelid),'')||':'||
  coalesce(pg_get_expr(policy.polwithcheck,policy.polrelid),''),
  '|' order by policy.polname),''))
from pg_policy policy where policy.polrelid='public.users'::regclass;
"""

CANONICAL_POLICY_SQL = r"""
alter policy "select by self and team and admin" on public.users to authenticated using (
  id = (select auth.uid())
  or id in (select r.user_id from public.roles r
    where r.role::text='owner' and public.policy_is_team_public(r.team_id)=true)
  or id in (select r0.user_id from public.roles r0 where r0.team_id in
    (select r.team_id from public.roles r
      where r.user_id=(select auth.uid()) and r.role::text<>'rejected'))
  or public.policy_is_current_user_in_roles(
    '00000000-0000-0000-0000-000000000000'::uuid,
    array['admin'::text,'review-admin'::text,'review-member'::text])
);
"""

LIVE_LEGACY_POLICY_SQL = r"""
alter policy "select by self and team and admin" on public.users to authenticated using (
  id = (select auth.uid())
  or id in (select r.user_id from public.roles r
    where r.role::text='owner' and public.policy_is_team_public(r.team_id)=true)
  or exists (select 1 from public.roles r
    where r.role::text='owner' and r.user_id=(select auth.uid()))
  or id in (select r0.user_id from public.roles r0 where r0.team_id in
    (select r.team_id from public.roles r
      where r.user_id=(select auth.uid()) and r.role::text<>'rejected'))
  or public.policy_is_current_user_in_roles(
    '00000000-0000-0000-0000-000000000000'::uuid,
    array['admin'::text,'review-admin'::text,'review-member'::text])
);
"""


def run_psql(db_url: str, *, sql: str | None = None, path: Path | None = None,
             check: bool = True) -> subprocess.CompletedProcess[str]:
    command = ["psql", db_url, "-XAtq", "-v", "ON_ERROR_STOP=1"]
    if path is not None:
        command.extend(["-f", str(path)])
    result = subprocess.run(
        command, cwd=ROOT, input=sql, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if check and result.returncode != 0:
        raise SystemExit(f"Issue #355 policy-variant SQL failed: {result.stderr.strip()}")
    return result


def scalar(db_url: str, sql: str) -> str:
    return run_psql(db_url, sql=sql).stdout.strip()


def policy_hash(db_url: str) -> str:
    return scalar(db_url, POLICY_HASH_SQL)


def target_fingerprint(db_url: str) -> str:
    return scalar(db_url, r"""
      select md5(coalesce(string_agg(value,'|' order by value),'')) from (
        select concat_ws(':',n.nspname,c.relname,c.relowner,c.relkind,
          coalesce(c.reloptions::text,''),coalesce(c.relacl::text,''),pg_get_viewdef(c.oid,true)) value
        from pg_class c join pg_namespace n on n.oid=c.relnamespace
        where (n.nspname='private' and c.relname in
          ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users'))
           or (n.nspname='api' and c.relname in
          ('notifications_v1','reviews_v1','team_roles_v1','teams_v1','user_profiles_v1',
           'identity_center_processed_events_v1','identity_center_users_v1'))
        union all
        select concat_ws(':',n.nspname,p.proname,p.proargtypes,p.proowner,p.prosecdef,
          coalesce(p.proconfig::text,''),coalesce(p.proacl::text,''),pg_get_functiondef(p.oid))
        from pg_proc p join pg_namespace n on n.oid=p.pronamespace
        where n.nspname='private' and p.proname in
          ('review_append_scope_snapshot_v1','review_revision_fingerprint_v1',
           'review_scope_all_reference_ids_v1','review_scope_checksum_v1',
           'review_scope_current_items_v1','review_scope_current_reference_ids_v1',
           'review_scope_current_snapshot_v1','review_validate_scope_history_v1')
      ) target;
    """)


def assert_projection_authority_is_bounded(db_url: str) -> None:
    result = scalar(db_url, r"""
      with mapping(target,source) as (values
        ('api.review_comments_v1','public.comments'),
        ('api.identity_center_processed_events_v1','public.identity_center_processed_events'),
        ('api.identity_center_users_v1','public.identity_center_users'),
        ('api.notifications_v1','public.notifications'),('api.reviews_v1','public.reviews'),
        ('api.team_roles_v1','public.roles'),('api.teams_v1','public.teams'),
        ('api.user_profiles_v1','public.users'),('private.comments','public.comments'),
        ('private.identity_center_processed_events','public.identity_center_processed_events'),
        ('private.identity_center_users','public.identity_center_users'),
        ('private.notifications','public.notifications'),('private.reviews','public.reviews'),
        ('private.roles','public.roles'),('private.teams','public.teams'),
        ('private.users','public.users')
      ), governed(role_name) as (values ('anon'),('authenticated'),('service_role'),('api_internal_executor'))
      select
        not exists (select 1 from mapping,governed
          where has_table_privilege(role_name,target,'select')
            and not has_table_privilege(role_name,source,'select'))
        and (select bool_and(c.reloptions @> array['security_invoker=true'])
          from pg_class c join pg_namespace n on n.oid=c.relnamespace
          where n.nspname in ('api','private') and n.nspname||'.'||c.relname in
            (select target from mapping));
    """)
    if result != "t":
        raise SystemExit("Issue #355 projection authority exceeds its source")


def assert_rls_semantics(db_url: str, expected_hash: str) -> None:
    ids = [f"83550100-0000-4000-8000-{ordinal:012d}" for ordinal in range(1, 9)]
    all_ids = ",".join(f"'{value}'::uuid" for value in ids)
    if expected_hash == CANONICAL_HASH:
        expected = {
            "owner_a": [ids[0], ids[1], ids[4]],
            "member_a": [ids[0], ids[1], ids[4]],
            "owner_b": [ids[2], ids[4]],
            "outsider": [ids[3], ids[4]],
            "global_admin": ids,
            "review_admin": ids,
            "review_member": ids,
        }
        disposition = "repository-source"
    else:
        expected = {
            "owner_a": ids,
            "member_a": [ids[0], ids[1], ids[4]],
            "owner_b": ids,
            "outsider": [ids[3], ids[4]],
            "global_admin": ids,
            "review_admin": ids,
            "review_member": ids,
        }
        disposition = "live-legacy-source-preservation"

    assertions: list[str] = []
    actors = {
        "owner_a": ids[0], "member_a": ids[1], "owner_b": ids[2],
        "outsider": ids[3], "global_admin": ids[5],
        "review_admin": ids[6], "review_member": ids[7],
    }
    for label, actor_id in actors.items():
        expected_ids = ",".join(f"'{value}'::uuid" for value in expected[label])
        assertions.append(f"""
          select set_config('request.jwt.claim.role','authenticated',true) as ignored \\gset
          select set_config('request.jwt.claim.sub','{actor_id}',true) as ignored \\gset
          with expected(ids) as (values (array[{expected_ids}])),
          source(ids) as (
            select coalesce(array_agg(id order by id),array[]::uuid[])
            from public.users where id=any(array[{all_ids}])
          ), projected(ids) as (
            select coalesce(array_agg(id order by id),array[]::uuid[])
            from api.user_profiles_v1 where id=any(array[{all_ids}])
          )
          select '{disposition}:{label}|'||(source.ids=expected.ids)::text||'|'||
            (projected.ids=expected.ids)::text||'|'||(projected.ids=source.ids)::text||'|'||
            source.ids::text||'|'||expected.ids::text
          from expected,source,projected;
        """)

    result = run_psql(db_url, sql=f"""
      begin;
      insert into auth.users (
        instance_id,id,aud,role,email,encrypted_password,email_confirmed_at,
        raw_app_meta_data,raw_user_meta_data,created_at,updated_at,is_sso_user,is_anonymous
      )
      select '00000000-0000-0000-0000-000000000000',id,'authenticated','authenticated',
        'issue355-policy-'||ordinal||'@example.com','x',now(),'{{}}'::jsonb,
        jsonb_build_object('email','issue355-policy-'||ordinal||'@example.com'),
        now(),now(),false,false
      from unnest(array[{all_ids}]) with ordinality fixture(id,ordinal);

      insert into public.users(id,raw_user_meta_data)
      select id,jsonb_build_object('email','issue355-policy-'||ordinal||'@example.com')
      from unnest(array[{all_ids}]) with ordinality fixture(id,ordinal);

      insert into public.teams(id,"json",rank,is_public) values
        ('00000000-0000-0000-0000-000000000000','{{}}',0,false),
        ('83550100-0000-4000-8000-000000000101','{{}}',0,false),
        ('83550100-0000-4000-8000-000000000102','{{}}',0,false),
        ('83550100-0000-4000-8000-000000000103','{{}}',0,true)
      on conflict (id) do nothing;
      insert into public.roles(user_id,team_id,role) values
        ('{ids[0]}','83550100-0000-4000-8000-000000000101','owner'),
        ('{ids[1]}','83550100-0000-4000-8000-000000000101','member'),
        ('{ids[2]}','83550100-0000-4000-8000-000000000102','owner'),
        ('{ids[4]}','83550100-0000-4000-8000-000000000103','owner'),
        ('{ids[5]}','00000000-0000-0000-0000-000000000000','admin'),
        ('{ids[6]}','00000000-0000-0000-0000-000000000000','review-admin'),
        ('{ids[7]}','00000000-0000-0000-0000-000000000000','review-member');

      set local role authenticated;
      {''.join(assertions)}
      reset role;
      rollback;
    """)
    lines = [line for line in result.stdout.splitlines() if "|" in line]
    if len(lines) != len(actors) or any(line.split("|")[1:4] != ["true"] * 3 for line in lines):
        raise SystemExit(
            f"Issue #355 {disposition} RLS/source-projection matrix failed: {lines}"
        )


def apply_variant(db_url: str, policy_sql: str, expected_hash: str) -> str:
    run_psql(db_url, sql=policy_sql)
    if policy_hash(db_url) != expected_hash:
        raise SystemExit(f"Issue #355 fixture did not produce exact hash {expected_hash}")
    run_psql(db_url, path=MIGRATION)
    if policy_hash(db_url) != expected_hash:
        raise SystemExit("Issue #355 Expand changed its admitted source policy")
    assert_projection_authority_is_bounded(db_url)
    assert_rls_semantics(db_url, expected_hash)
    return target_fingerprint(db_url)


def main() -> int:
    db_url = verified_database_url()
    try:
        canonical_target = apply_variant(db_url, CANONICAL_POLICY_SQL, CANONICAL_HASH)
        run_psql(db_url, path=ROLLBACK)
        live_target = apply_variant(db_url, LIVE_LEGACY_POLICY_SQL, LIVE_LEGACY_HASH)
        if live_target != canonical_target:
            raise SystemExit("Issue #355 target differs between admitted source variants")
        run_psql(db_url, path=MIGRATION)
        if policy_hash(db_url) != LIVE_LEGACY_HASH or target_fingerprint(db_url) != live_target:
            raise SystemExit("Issue #355 live-variant retry was not stable")
        run_psql(db_url, path=ROLLBACK)
        live_reapplied = apply_variant(db_url, LIVE_LEGACY_POLICY_SQL, LIVE_LEGACY_HASH)
        if live_reapplied != live_target:
            raise SystemExit("Issue #355 live-variant rollback/reapply was not stable")

        run_psql(db_url, path=ROLLBACK)
        run_psql(db_url, sql=CANONICAL_POLICY_SQL + r"""
          alter policy "select by self and team and admin" on public.users
            using (id is null or id=(select auth.uid()));
        """)
        unknown_hash = policy_hash(db_url)
        if unknown_hash in {CANONICAL_HASH, LIVE_LEGACY_HASH}:
            raise SystemExit("Issue #355 unknown fixture collided with an admitted variant")
        zero_before = target_fingerprint(db_url)
        rejected = run_psql(db_url, path=MIGRATION, check=False)
        if rejected.returncode == 0:
            raise SystemExit("Issue #355 unknown policy variant unexpectedly succeeded")
        if "source relation relkind/RLS/ACL/policy preflight drift: {users}" not in rejected.stderr:
            raise SystemExit("Issue #355 unknown policy variant failed outside the exact preflight")
        if policy_hash(db_url) != unknown_hash or target_fingerprint(db_url) != zero_before:
            raise SystemExit("Issue #355 unknown policy rejection was not atomic")
    finally:
        restored_policy = run_psql(db_url, sql=CANONICAL_POLICY_SQL, check=False)
        restored_expand = run_psql(db_url, path=MIGRATION, check=False)
        if restored_policy.returncode != 0 or restored_expand.returncode != 0:
            raise SystemExit("Issue #355 qualification could not restore canonical local state")
        if policy_hash(db_url) != CANONICAL_HASH:
            raise SystemExit("Issue #355 qualification did not retain canonical local policy")

    print("PASS Issue #355 exact canonical/live policy admission, preservation, retry, rollback/reapply, and unknown atomic rejection")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
