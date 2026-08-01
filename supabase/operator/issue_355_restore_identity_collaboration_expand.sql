\set ON_ERROR_STOP on

begin;

set local lock_timeout = '5s';
set local statement_timeout = '60s';

do $preflight$
declare
  target_count integer;
begin
  select
    (select count(*) from pg_class relation join pg_namespace namespace on namespace.oid=relation.relnamespace
     where (namespace.nspname='private' and relation.relname in
       ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users'))
        or (namespace.nspname='api' and relation.relname in
       ('notifications_v1','reviews_v1','team_roles_v1','teams_v1','user_profiles_v1',
        'identity_center_processed_events_v1','identity_center_users_v1')))
    +
    (select count(*) from pg_proc routine join pg_namespace namespace on namespace.oid=routine.pronamespace
     where namespace.nspname='private' and routine.proname in
       ('review_append_scope_snapshot_v1','review_revision_fingerprint_v1',
        'review_scope_all_reference_ids_v1','review_scope_checksum_v1',
        'review_scope_current_items_v1','review_scope_current_reference_ids_v1',
        'review_scope_current_snapshot_v1','review_validate_scope_history_v1'))
  into target_count;

  if target_count=0 then
    return;
  end if;
  if target_count<>23 then
    raise exception 'Issue #355 rollback refuses partial/extra target set: %/23',target_count;
  end if;
  if (select max(version) from supabase_migrations.schema_migrations) <> '20260801061000'
     or (select count(*) from supabase_migrations.schema_migrations where version='20260801061000') <> 1 then
    raise exception 'Issue #355 rollback requires exact migration head 20260801061000';
  end if;

  if exists (
    with expected(object_key,fingerprint) as (values
      ('api.identity_center_processed_events_v1','8bfb1353f1f6a6d526b6c05339c8d7d5'),
      ('api.identity_center_users_v1','77c2b608b6ae5c23abafe1b14f117b8f'),
      ('api.notifications_v1','de4b6f2b9316e551ed03799f260dd40f'),
      ('api.reviews_v1','104400e8bd771436243b64f2694604ee'),
      ('api.team_roles_v1','c89b72ee66ba35f94f89ff98ab00e90e'),
      ('api.teams_v1','35e3e0b08cc61b106fd2f72d12959c83'),
      ('api.user_profiles_v1','a8eee4f5b217c59a643d75c85ff8a4c4'),
      ('private.comments','f6d5e2ba8c5a7789b3096724af4b9c32'),
      ('private.identity_center_processed_events','d63b4de0a064bfe5656fa0fb1950415b'),
      ('private.identity_center_users','3b8e0a17058a89ede3febe47c710e887'),
      ('private.notifications','d9083031626a704c4baf70d58b4ea37d'),
      ('private.reviews','b8d2da38e4b839a9669527d8867e032e'),
      ('private.roles','dd51afa95e88506a3804414d41be5c5d'),
      ('private.teams','e546e2511bb4f9ca92976833995da9e7'),
      ('private.users','20aaa4cff623d1672520d63fa5ce3a49'),
      ('private.review_append_scope_snapshot_v1(uuid, text, text, jsonb, uuid)','58a21252d890896177de0b397c76d649'),
      ('private.review_revision_fingerprint_v1(text, jsonb)','a01a3ebb834862e179e728738187e36e'),
      ('private.review_scope_all_reference_ids_v1(jsonb)','1c316d2949860f7b37d4f4d5124a2795'),
      ('private.review_scope_checksum_v1(jsonb)','789c054a5b60a1bc2f9d2d25a8252a97'),
      ('private.review_scope_current_items_v1(jsonb)','cb3b8ac691ca01a2a7042656e3accb64'),
      ('private.review_scope_current_reference_ids_v1(jsonb)','c87fef5d832e3ef13e8cac96edab9028'),
      ('private.review_scope_current_snapshot_v1(jsonb)','5eef2eda06966f634a8f1072c2c6ba54'),
      ('private.review_validate_scope_history_v1(uuid, jsonb)','7c6bf09b64daf105e044518965bbe2dc')
    ), relation_actual as (
      select namespace.nspname||'.'||relation.relname as object_key,
        md5(concat_ws('|',owner.rolname,relation.relkind::text,coalesce(relation.reloptions::text,''),
          coalesce((select string_agg(
            (case when acl.grantee=0 then 'PUBLIC' else grantee.rolname end)||':'||
            acl.privilege_type||':'||acl.is_grantable::text||':'||
            (case when acl.grantor=0 then 'PUBLIC' else grantor.rolname end),
            '|' order by case when acl.grantee=0 then 'PUBLIC' else grantee.rolname end,
            acl.privilege_type,acl.is_grantable,
            case when acl.grantor=0 then 'PUBLIC' else grantor.rolname end)
            from aclexplode(coalesce(relation.relacl,acldefault('r',relation.relowner))) acl
            left join pg_roles grantee on grantee.oid=acl.grantee
            left join pg_roles grantor on grantor.oid=acl.grantor),''),
          pg_get_viewdef(relation.oid,true),coalesce(obj_description(relation.oid,'pg_class'),''),
          coalesce((select string_agg(
            pg_describe_object(dependency.refclassid,dependency.refobjid,dependency.refobjsubid)||':'||dependency.deptype::text,
            '|' order by pg_describe_object(dependency.refclassid,dependency.refobjid,dependency.refobjsubid),dependency.deptype::text)
            from pg_rewrite rewrite join pg_depend dependency
              on dependency.classid='pg_rewrite'::regclass and dependency.objid=rewrite.oid
            where rewrite.ev_class=relation.oid),''))) as fingerprint
      from pg_class relation
      join pg_namespace namespace on namespace.oid=relation.relnamespace
      join pg_roles owner on owner.oid=relation.relowner
      where (namespace.nspname='private' and relation.relname in
        ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users'))
         or (namespace.nspname='api' and relation.relname in
        ('notifications_v1','reviews_v1','team_roles_v1','teams_v1','user_profiles_v1',
         'identity_center_processed_events_v1','identity_center_users_v1'))
    ), routine_actual as (
      select namespace.nspname||'.'||routine.proname||'('||oidvectortypes(routine.proargtypes)||')' as object_key,
        md5(concat_ws('|',owner.rolname,language.lanname,routine.prokind::text,
          routine.provolatile::text,routine.prosecdef::text,routine.proisstrict::text,
          routine.proparallel::text,routine.proleakproof::text,routine.procost::text,routine.prorows::text,
          coalesce(routine.proconfig::text,''),pg_get_function_result(routine.oid),
          pg_get_functiondef(routine.oid),coalesce(obj_description(routine.oid,'pg_proc'),''),
          coalesce((select string_agg(
            (case when acl.grantee=0 then 'PUBLIC' else grantee.rolname end)||':'||
            acl.privilege_type||':'||acl.is_grantable::text||':'||
            (case when acl.grantor=0 then 'PUBLIC' else grantor.rolname end),
            '|' order by case when acl.grantee=0 then 'PUBLIC' else grantee.rolname end,
            acl.privilege_type,acl.is_grantable,
            case when acl.grantor=0 then 'PUBLIC' else grantor.rolname end)
            from aclexplode(coalesce(routine.proacl,acldefault('f',routine.proowner))) acl
            left join pg_roles grantee on grantee.oid=acl.grantee
            left join pg_roles grantor on grantor.oid=acl.grantor),''),
          coalesce((select string_agg(
            pg_describe_object(dependency.refclassid,dependency.refobjid,dependency.refobjsubid)||':'||dependency.deptype::text,
            '|' order by pg_describe_object(dependency.refclassid,dependency.refobjid,dependency.refobjsubid),dependency.deptype::text)
            from pg_depend dependency
            where dependency.classid='pg_proc'::regclass and dependency.objid=routine.oid),''))) as fingerprint
      from pg_proc routine
      join pg_namespace namespace on namespace.oid=routine.pronamespace
      join pg_language language on language.oid=routine.prolang
      join pg_roles owner on owner.oid=routine.proowner
      where namespace.nspname='private' and routine.proname in
        ('review_append_scope_snapshot_v1','review_revision_fingerprint_v1',
         'review_scope_all_reference_ids_v1','review_scope_checksum_v1',
         'review_scope_current_items_v1','review_scope_current_reference_ids_v1',
         'review_scope_current_snapshot_v1','review_validate_scope_history_v1')
    ), actual as (select * from relation_actual union all select * from routine_actual)
    (select object_key,fingerprint from expected except select object_key,fingerprint from actual)
    union all
    (select object_key,fingerprint from actual except select object_key,fingerprint from expected)
  ) then
    raise exception 'Issue #355 rollback refuses target fingerprint drift';
  end if;

  if to_regclass('public.comments') is null
     or to_regclass('public.identity_center_users') is null
     or to_regclass('public.reviews') is null
     or to_regprocedure('public.review_scope_checksum_v1(jsonb)') is null
     or to_regprocedure('public.review_validate_scope_history_v1(uuid,jsonb)') is null then
    raise exception 'Issue #355 rollback source contract is incomplete';
  end if;
end
$preflight$;

drop view if exists api.identity_center_processed_events_v1;
drop view if exists api.identity_center_users_v1;
drop view if exists api.notifications_v1;
drop view if exists api.reviews_v1;
drop view if exists api.team_roles_v1;
drop view if exists api.teams_v1;
drop view if exists api.user_profiles_v1;

drop view if exists private.comments;
drop view if exists private.identity_center_processed_events;
drop view if exists private.identity_center_users;
drop view if exists private.notifications;
drop view if exists private.reviews;
drop view if exists private.roles;
drop view if exists private.teams;
drop view if exists private.users;

drop function if exists private.review_append_scope_snapshot_v1(uuid,text,text,jsonb,uuid);
drop function if exists private.review_revision_fingerprint_v1(text,jsonb);
drop function if exists private.review_scope_all_reference_ids_v1(jsonb);
drop function if exists private.review_scope_checksum_v1(jsonb);
drop function if exists private.review_scope_current_items_v1(jsonb);
drop function if exists private.review_scope_current_reference_ids_v1(jsonb);
drop function if exists private.review_scope_current_snapshot_v1(jsonb);
drop function if exists private.review_validate_scope_history_v1(uuid,jsonb);

commit;
