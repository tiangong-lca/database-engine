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
      ('api.identity_center_processed_events_v1','9c9658b4629342cf504b0ff5bc305450'),
      ('api.identity_center_users_v1','2237fc113ddcc791ab0a4f07082887d9'),
      ('api.notifications_v1','d337071f23a863c87b6fe63b8408f38c'),
      ('api.reviews_v1','2812e4fac6d72c77b3fbcf4aaa554a42'),
      ('api.team_roles_v1','0b90da951c7396f584bc4a736b71caa5'),
      ('api.teams_v1','2bc89a22a7c9693827821fe450dc8d11'),
      ('api.user_profiles_v1','8100f44a5690696a4fcec4c195e61cca'),
      ('private.comments','1f5c87041522c134d27db17da9fcaf91'),
      ('private.identity_center_processed_events','11a3b755ce928d66826db079fedc2799'),
      ('private.identity_center_users','a9fd61b56cab162f063220d67bd25854'),
      ('private.notifications','98d99e8d88f5bff4c0192b9c443105b4'),
      ('private.reviews','1e336a0bb0463c6a6f16298bd983bcd1'),
      ('private.roles','ca5a46da736d530d2b417d316da86c86'),
      ('private.teams','20148baef45638d04597c2ced6ccded2'),
      ('private.users','93d5d99161f2b2a12d0beb5c0d8fa31d'),
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
          coalesce((select string_agg(
            attribute.attname||':'||
            (case when acl.grantee=0 then 'PUBLIC' else grantee.rolname end)||':'||
            acl.privilege_type||':'||acl.is_grantable::text||':'||
            (case when acl.grantor=0 then 'PUBLIC' else grantor.rolname end),
            '|' order by attribute.attname,
            case when acl.grantee=0 then 'PUBLIC' else grantee.rolname end,
            acl.privilege_type,acl.is_grantable,
            case when acl.grantor=0 then 'PUBLIC' else grantor.rolname end)
            from pg_attribute attribute
            cross join lateral aclexplode(attribute.attacl) acl
            left join pg_roles grantee on grantee.oid=acl.grantee
            left join pg_roles grantor on grantor.oid=acl.grantor
            where attribute.attrelid=relation.oid and attribute.attnum>0
              and not attribute.attisdropped),''),
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
