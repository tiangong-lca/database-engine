-- Issue #355: identity and collaboration Expand boundary.
--
-- Tables deliberately remain the one physical public source during Expand.
-- Moving them while retaining browser relation ACLs on private relations would
-- violate the Issue #339 zero-internal-CRUD invariant.  The explicit api and
-- private projections establish cutover targets without dual writes.  The
-- physical table move is a later Contract action gated by consumer-zero proof.

begin;
set local lock_timeout = '5s';
set local statement_timeout = '60s';

-- Refuse to derive adapter authority from a drifted live source.  These hashes
-- bind the eight exact signatures to the reviewed predecessor definitions and
-- all execution-relevant properties using role names rather than role OIDs.
do $routine_predecessor$
begin
  if exists (
    with expected(signature,fingerprint) as (values
      ('review_append_scope_snapshot_v1(uuid, text, text, jsonb, uuid)','175ca0e89e01ce55c26d9d799a2c861b'),
      ('review_revision_fingerprint_v1(text, jsonb)','93ba379684cff3fad1dcb5721850563c'),
      ('review_scope_all_reference_ids_v1(jsonb)','5d1a9153045b21ec9438f17bad6ea16c'),
      ('review_scope_checksum_v1(jsonb)','7994415c18fca9cc48b18b2db723d225'),
      ('review_scope_current_items_v1(jsonb)','729370bc24a2ecf8efca388654b0f03c'),
      ('review_scope_current_reference_ids_v1(jsonb)','601641764b3d2acfde78fae4e590ba6b'),
      ('review_scope_current_snapshot_v1(jsonb)','28abac7d508e68687c2c2f0252cd8359'),
      ('review_validate_scope_history_v1(uuid, jsonb)','fb646860c0e176bd849be7a98459a70b')
    ), actual as (
      select p.proname||'('||oidvectortypes(p.proargtypes)||')' as signature,
        md5(concat_ws('|',owner.rolname,language.lanname,p.prokind::text,
          p.provolatile::text,p.prosecdef::text,p.proisstrict::text,p.proparallel::text,
          p.proleakproof::text,p.procost::text,p.prorows::text,coalesce(p.proconfig::text,''),
          pg_get_function_result(p.oid),pg_get_functiondef(p.oid),
          coalesce((select string_agg(
            (case when acl.grantee=0 then 'PUBLIC' else grantee.rolname end)||':'||
            acl.privilege_type||':'||acl.is_grantable::text||':'||
            (case when acl.grantor=0 then 'PUBLIC' else grantor.rolname end),
            '|' order by case when acl.grantee=0 then 'PUBLIC' else grantee.rolname end,
            acl.privilege_type,acl.is_grantable,
            case when acl.grantor=0 then 'PUBLIC' else grantor.rolname end)
            from aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) acl
            left join pg_roles grantee on grantee.oid=acl.grantee
            left join pg_roles grantor on grantor.oid=acl.grantor),''))) as fingerprint
      from pg_proc p
      join pg_namespace namespace on namespace.oid=p.pronamespace
      join pg_language language on language.oid=p.prolang
      join pg_roles owner on owner.oid=p.proowner
      where namespace.nspname='public' and p.proname in
        ('review_append_scope_snapshot_v1','review_revision_fingerprint_v1',
         'review_scope_all_reference_ids_v1','review_scope_checksum_v1',
         'review_scope_current_items_v1','review_scope_current_reference_ids_v1',
         'review_scope_current_snapshot_v1','review_validate_scope_history_v1')
    )
    (select signature,fingerprint from expected except select signature,fingerprint from actual)
    union all
    (select signature,fingerprint from actual except select signature,fingerprint from expected)
  ) then
    raise exception 'Issue #355 reviewed predecessor routine signature/definition/property/ACL fingerprint mismatch';
  end if;
end
$routine_predecessor$;

lock table
  public.comments, public.identity_center_processed_events,
  public.identity_center_users, public.notifications, public.reviews,
  public.roles, public.teams, public.users
in access share mode;

create temporary table issue355_relation_baseline on commit drop as
select c.oid,c.relowner,c.relkind,c.relrowsecurity,c.relforcerowsecurity,c.relacl,
  (select md5(coalesce(string_agg(
    policy.polname||':'||policy.polcmd::text||':'||policy.polpermissive::text||':'||
    coalesce((select string_agg(case when role_oid=0 then 'PUBLIC' else role_name.rolname end,
      ',' order by case when role_oid=0 then 'PUBLIC' else role_name.rolname end)
      from unnest(policy.polroles) role_oid
      left join pg_roles role_name on role_name.oid=role_oid),'')||':'||
    coalesce(pg_get_expr(policy.polqual,policy.polrelid),'')||':'||
    coalesce(pg_get_expr(policy.polwithcheck,policy.polrelid),''),
    '|' order by policy.polname),''))
   from pg_policy policy where policy.polrelid=c.oid) as policy_hash
from pg_class c join pg_namespace n on n.oid=c.relnamespace
where n.nspname='public' and c.relname in
  ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users');

do $preflight$
declare
  v_missing text[];
  v_drift text[];
begin
  select array_agg(object_key order by object_key)
    into v_missing
  from (
    values
      ('public.comments', to_regclass('public.comments') is not null),
      ('public.identity_center_processed_events', to_regclass('public.identity_center_processed_events') is not null),
      ('public.identity_center_users', to_regclass('public.identity_center_users') is not null),
      ('public.notifications', to_regclass('public.notifications') is not null),
      ('public.reviews', to_regclass('public.reviews') is not null),
      ('public.roles', to_regclass('public.roles') is not null),
      ('public.teams', to_regclass('public.teams') is not null),
      ('public.users', to_regclass('public.users') is not null),
      ('public.review_append_scope_snapshot_v1(uuid,text,text,jsonb,uuid)', to_regprocedure('public.review_append_scope_snapshot_v1(uuid,text,text,jsonb,uuid)') is not null),
      ('public.review_revision_fingerprint_v1(text,jsonb)', to_regprocedure('public.review_revision_fingerprint_v1(text,jsonb)') is not null),
      ('public.review_scope_all_reference_ids_v1(jsonb)', to_regprocedure('public.review_scope_all_reference_ids_v1(jsonb)') is not null),
      ('public.review_scope_checksum_v1(jsonb)', to_regprocedure('public.review_scope_checksum_v1(jsonb)') is not null),
      ('public.review_scope_current_items_v1(jsonb)', to_regprocedure('public.review_scope_current_items_v1(jsonb)') is not null),
      ('public.review_scope_current_reference_ids_v1(jsonb)', to_regprocedure('public.review_scope_current_reference_ids_v1(jsonb)') is not null),
      ('public.review_scope_current_snapshot_v1(jsonb)', to_regprocedure('public.review_scope_current_snapshot_v1(jsonb)') is not null),
      ('public.review_validate_scope_history_v1(uuid,jsonb)', to_regprocedure('public.review_validate_scope_history_v1(uuid,jsonb)') is not null)
  ) as expected(object_key, present)
  where not present;

  if v_missing is not null then
    raise exception 'Issue #355 preflight missing exact objects: %', v_missing;
  end if;

  -- public.users has two independently observed predecessor contracts. The
  -- repo/blank replay variant and persistent Dev/Production legacy variant are
  -- both source-only inputs to Expand. Expand preserves either exact hash; it
  -- does not endorse or narrow the legacy global-owner clause. Retirement
  -- requires the separately governed #753/#358 consumer-zero and owner review.
  with expected(relname, acl_hash, policy_hashes) as (values
    ('comments','01325751d110098b0c41305bc21ee772',array['380f1a0c3b1970af16ec71425b65f2f0']),
    ('identity_center_processed_events','3a6eef6333e35dd973f1d4b8cef7e564',array['d41d8cd98f00b204e9800998ecf8427e']),
    ('identity_center_users','3a6eef6333e35dd973f1d4b8cef7e564',array['d41d8cd98f00b204e9800998ecf8427e']),
    ('notifications','80b5dc3f8df5de03126ed3ef949dcd26',array['f08c612310fbeecf938d31e0a5e4806a']),
    ('reviews','01325751d110098b0c41305bc21ee772',array['7f1955b6ee25bf7afd937f1ec32f941b']),
    ('roles','01325751d110098b0c41305bc21ee772',array['0be7e710b6b38a985dafe00754395c74']),
    ('teams','01325751d110098b0c41305bc21ee772',array['dc47a8e9365d969154020c196bbfa23e']),
    ('users','01325751d110098b0c41305bc21ee772',array[
      '57fd9c26617c29dc6edc92d231bbec85',
      '6ab74729e7e0ec6e9378542059d17cd0'
    ])
  ), actual as (
    select c.relname, c.relkind, c.relrowsecurity, c.relforcerowsecurity,
      md5(string_agg(coalesce(grantee_role.rolname,'PUBLIC')||':'||acl.privilege_type||':'||acl.is_grantable::text,
        '|' order by coalesce(grantee_role.rolname,'PUBLIC'),acl.privilege_type,acl.is_grantable)) as acl_hash,
      (select md5(coalesce(string_agg(
        policy.polname||':'||policy.polcmd::text||':'||policy.polpermissive::text||':'||
        coalesce((select string_agg(case when role_oid=0 then 'PUBLIC' else role_name.rolname end,
          ',' order by case when role_oid=0 then 'PUBLIC' else role_name.rolname end)
          from unnest(policy.polroles) role_oid
          left join pg_roles role_name on role_name.oid=role_oid),'')||':'||
        coalesce(pg_get_expr(policy.polqual,policy.polrelid),'')||':'||
        coalesce(pg_get_expr(policy.polwithcheck,policy.polrelid),''),
        '|' order by policy.polname),''))
       from pg_policy policy where policy.polrelid=c.oid) as policy_hash
    from pg_class c
    join pg_namespace n on n.oid=c.relnamespace
    cross join lateral aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) acl
    left join pg_roles grantee_role on grantee_role.oid=acl.grantee
    where n.nspname='public' and c.relname in (select relname from expected)
    group by c.oid,c.relname,c.relkind,c.relrowsecurity,c.relforcerowsecurity
  )
  select array_agg(expected.relname order by expected.relname) into v_drift
  from expected join actual using (relname)
  where actual.relkind <> 'r' or not actual.relrowsecurity or actual.relforcerowsecurity
     or actual.acl_hash <> expected.acl_hash
     or not (actual.policy_hash = any(expected.policy_hashes));
  if v_drift is not null then
    raise exception 'Issue #355 source relation relkind/RLS/ACL/policy preflight drift: %', v_drift;
  end if;

  if exists (
    select 1
    from pg_class c join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'public'
      and c.relname in ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users')
      and pg_get_userbyid(c.relowner) <> 'postgres'
  ) or exists (
    select 1
    from pg_proc p join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'public'
      and p.proname in ('review_append_scope_snapshot_v1','review_revision_fingerprint_v1','review_scope_all_reference_ids_v1','review_scope_checksum_v1','review_scope_current_items_v1','review_scope_current_reference_ids_v1','review_scope_current_snapshot_v1','review_validate_scope_history_v1')
      and pg_get_userbyid(p.proowner) <> 'postgres'
  ) then
    raise exception 'Issue #355 owner preflight requires postgres-owned source objects';
  end if;
end
$preflight$;

create temporary table issue355_routine_baseline on commit drop as
select
  p.oid,
  p.proname,
  p.proargtypes,
  p.proowner,
  p.provolatile,
  p.prosecdef,
  p.proisstrict,
  p.proparallel,
  p.proconfig,
  p.proacl,
  pg_get_function_result(p.oid) as function_result,
  pg_get_functiondef(p.oid) as function_definition
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where n.nspname = 'public'
  and p.proname in (
    'review_append_scope_snapshot_v1','review_revision_fingerprint_v1',
    'review_scope_all_reference_ids_v1','review_scope_checksum_v1',
    'review_scope_current_items_v1','review_scope_current_reference_ids_v1',
    'review_scope_current_snapshot_v1','review_validate_scope_history_v1'
  );

-- Internal projections.  Only the already-approved service role can read them;
-- browser roles receive neither private USAGE nor relation privileges.
create or replace view private.comments with (security_invoker = true) as
select review_id, reviewer_id, "json", created_at, modified_at, state_code
from public.comments;

create or replace view private.identity_center_processed_events with (security_invoker = true) as
select event_id, event_type, processed_at
from public.identity_center_processed_events;

create or replace view private.identity_center_users with (security_invoker = true) as
select keycloak_sub, user_id, status, desired_role, metadata, created_at, modified_at
from public.identity_center_users;

create or replace view private.notifications with (security_invoker = true) as
select id, recipient_user_id, sender_user_id, type, dataset_type, dataset_id,
       dataset_version, "json", created_at, modified_at
from public.notifications;

create or replace view private.reviews with (security_invoker = true) as
select id, data_id, created_at, modified_at, state_code, data_version,
       reviewer_id, "json", deadline, review_kind, target_table,
       submitted_revision_checksum, approved_revision_checksum,
       target_owner_id, target_team_id, scope_schema_version, scope_history,
       current_reference_review_ids, all_reference_review_ids
from public.reviews;

create or replace view private.roles with (security_invoker = true) as
select user_id, team_id, role, created_at, modified_at
from public.roles;

create or replace view private.teams with (security_invoker = true) as
select id, "json", created_at, modified_at, rank, is_public
from public.teams;

create or replace view private.users with (security_invoker = true) as
select id, raw_user_meta_data, contact
from public.users;

alter view private.comments owner to postgres;
alter view private.identity_center_processed_events owner to postgres;
alter view private.identity_center_users owner to postgres;
alter view private.notifications owner to postgres;
alter view private.reviews owner to postgres;
alter view private.roles owner to postgres;
alter view private.teams owner to postgres;
alter view private.users owner to postgres;

do $converge_private_view_acl$
declare target_oid oid; grantee_name text; attribute_acl record;
begin
  for target_oid in
    select c.oid from pg_class c join pg_namespace n on n.oid=c.relnamespace
    where n.nspname='private' and c.relname in
      ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users')
  loop
    -- CASCADE is scoped by PostgreSQL to privileges derived from this exact
    -- target relation.  It is required for multi-level grant-option chains and
    -- cannot revoke the same roles' independent grants on other objects.
    -- ACL role OIDs cannot dangle: PostgreSQL's shared dependencies reject a
    -- role drop while its grant survives; grantee OID zero is PUBLIC.
    execute format('revoke all privileges on table %s from public cascade',target_oid::regclass);
    for grantee_name in
      select role.rolname from pg_class c
      cross join lateral aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) with ordinality acl
      join pg_roles role on role.oid=acl.grantee where c.oid=target_oid and role.rolname<>'postgres'
      group by role.rolname
      order by min(acl.ordinality)
    loop
      -- The cursor can retain rows already removed by an earlier CASCADE;
      -- repeating a target-scoped revoke is stable and leaves no privilege.
      execute format('revoke all privileges on table %s from %I cascade',target_oid::regclass,grantee_name);
    end loop;
    -- Relation ACL convergence does not remove column-only grants.  Enumerate
    -- every physical attacl entry so group, inherited, PUBLIC, and quoted-role
    -- grants all converge to the reviewed no-column-ACL posture.
    for attribute_acl in
      select attribute.attname, acl.grantee, grantee.rolname
      from pg_attribute attribute
      cross join lateral aclexplode(attribute.attacl) with ordinality acl
      left join pg_roles grantee on grantee.oid=acl.grantee
      where attribute.attrelid=target_oid and attribute.attnum>0 and not attribute.attisdropped
      group by attribute.attname,acl.grantee,grantee.rolname
      order by min(acl.ordinality)
    loop
      execute format('revoke all privileges (%I) on table %s from %s cascade',
        attribute_acl.attname,target_oid::regclass,
        case when attribute_acl.grantee=0 then 'PUBLIC' else format('%I',attribute_acl.rolname) end);
    end loop;
  end loop;
end
$converge_private_view_acl$;

revoke all on
  private.comments,
  private.identity_center_processed_events,
  private.identity_center_users,
  private.notifications,
  private.reviews,
  private.roles,
  private.teams,
  private.users
from public, anon, authenticated, service_role, api_internal_executor;

grant select on
  private.comments,
  private.identity_center_processed_events,
  private.identity_center_users,
  private.notifications,
  private.reviews,
  private.roles,
  private.teams,
  private.users
to service_role;

-- Browser cutover projections.  They are read-only grants over the existing
-- RLS-protected public physical source; mutations continue through reviewed
-- command RPCs and existing compatibility paths during Expand.
create or replace view api.notifications_v1 with (security_invoker = true) as
select id, recipient_user_id, sender_user_id, type, dataset_type, dataset_id,
       dataset_version, "json", created_at, modified_at
from public.notifications;

create or replace view api.reviews_v1 with (security_invoker = true) as
select id, data_id, created_at, modified_at, state_code, data_version,
       reviewer_id, "json", deadline, review_kind, target_table,
       submitted_revision_checksum, approved_revision_checksum,
       target_owner_id, target_team_id, scope_schema_version, scope_history,
       current_reference_review_ids, all_reference_review_ids
from public.reviews;

create or replace view api.team_roles_v1 with (security_invoker = true) as
select user_id, team_id, role, created_at, modified_at
from public.roles;

create or replace view api.teams_v1 with (security_invoker = true) as
select id, "json", created_at, modified_at, rank, is_public
from public.teams;

create or replace view api.user_profiles_v1 with (security_invoker = true) as
select id, contact, raw_user_meta_data ->> 'email' as email,
       raw_user_meta_data ->> 'display_name' as display_name
from public.users;

create or replace view api.identity_center_processed_events_v1 with (security_invoker = true) as
select event_id, event_type, processed_at
from public.identity_center_processed_events;

create or replace view api.identity_center_users_v1 with (security_invoker = true) as
select keycloak_sub, user_id, status, desired_role, metadata, created_at, modified_at
from public.identity_center_users;

alter view api.notifications_v1 owner to postgres;
alter view api.reviews_v1 owner to postgres;
alter view api.team_roles_v1 owner to postgres;
alter view api.teams_v1 owner to postgres;
alter view api.user_profiles_v1 owner to postgres;
alter view api.identity_center_processed_events_v1 owner to postgres;
alter view api.identity_center_users_v1 owner to postgres;

do $converge_api_view_acl$
declare target_oid oid; grantee_name text; attribute_acl record;
begin
  for target_oid in
    select c.oid from pg_class c join pg_namespace n on n.oid=c.relnamespace
    where n.nspname='api' and c.relname in
      ('notifications_v1','reviews_v1','team_roles_v1','teams_v1','user_profiles_v1',
       'identity_center_processed_events_v1','identity_center_users_v1')
  loop
    execute format('revoke all privileges on table %s from public cascade',target_oid::regclass);
    for grantee_name in
      select role.rolname from pg_class c
      cross join lateral aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) with ordinality acl
      join pg_roles role on role.oid=acl.grantee where c.oid=target_oid and role.rolname<>'postgres'
      group by role.rolname
      order by min(acl.ordinality)
    loop
      execute format('revoke all privileges on table %s from %I cascade',target_oid::regclass,grantee_name);
    end loop;
    for attribute_acl in
      select attribute.attname, acl.grantee, grantee.rolname
      from pg_attribute attribute
      cross join lateral aclexplode(attribute.attacl) with ordinality acl
      left join pg_roles grantee on grantee.oid=acl.grantee
      where attribute.attrelid=target_oid and attribute.attnum>0 and not attribute.attisdropped
      group by attribute.attname,acl.grantee,grantee.rolname
      order by min(acl.ordinality)
    loop
      execute format('revoke all privileges (%I) on table %s from %s cascade',
        attribute_acl.attname,target_oid::regclass,
        case when attribute_acl.grantee=0 then 'PUBLIC' else format('%I',attribute_acl.rolname) end);
    end loop;
  end loop;
end
$converge_api_view_acl$;

revoke all on
  api.notifications_v1, api.reviews_v1,
  api.team_roles_v1, api.teams_v1, api.user_profiles_v1,
  api.identity_center_processed_events_v1, api.identity_center_users_v1
from public, anon, authenticated, service_role, api_internal_executor;

grant select on api.reviews_v1,
  api.team_roles_v1, api.teams_v1, api.user_profiles_v1
to authenticated, service_role;

grant select on api.notifications_v1,
  api.identity_center_processed_events_v1, api.identity_center_users_v1
to service_role;

-- A normalized empty per-column ACL is part of the target contract.  Table
-- grants above are intentional; no column-specific privilege survives.
do $column_acl_postcondition$
declare normalized_column_acl text;
begin
  select string_agg(
    namespace.nspname||'.'||relation.relname||':'||attribute.attname||':'||
    case when acl.grantee=0 then 'PUBLIC' else grantee.rolname end||':'||
    acl.privilege_type||':'||acl.is_grantable::text||':'||
    case when acl.grantor=0 then 'PUBLIC' else grantor.rolname end,
    '|' order by namespace.nspname,relation.relname,attribute.attname,
      case when acl.grantee=0 then 'PUBLIC' else grantee.rolname end,
      acl.privilege_type,acl.is_grantable,
      case when acl.grantor=0 then 'PUBLIC' else grantor.rolname end)
  into normalized_column_acl
  from pg_class relation
  join pg_namespace namespace on namespace.oid=relation.relnamespace
  join pg_attribute attribute on attribute.attrelid=relation.oid
    and attribute.attnum>0 and not attribute.attisdropped
  cross join lateral aclexplode(attribute.attacl) acl
  left join pg_roles grantee on grantee.oid=acl.grantee
  left join pg_roles grantor on grantor.oid=acl.grantor
  where (namespace.nspname='private' and relation.relname in
    ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users'))
     or (namespace.nspname='api' and relation.relname in
    ('notifications_v1','reviews_v1','team_roles_v1','teams_v1','user_profiles_v1',
     'identity_center_processed_events_v1','identity_center_users_v1'));

  if coalesce(normalized_column_acl,'')<>'' then
    raise exception 'Issue #355 target column ACL postcondition failed: %',normalized_column_acl;
  end if;
end
$column_acl_postcondition$;

-- Keep the audited SECURITY DEFINER routines physically public during Expand.
-- Private invoker adapters create the internal cutover target without adding a
-- second privileged routine or changing the #333 public audit population.
create or replace function private.review_append_scope_snapshot_v1(
  p_root_review_id uuid, p_scope_basis text, p_root_revision_checksum text,
  p_items jsonb, p_created_by uuid
) returns jsonb
language sql volatile security invoker
set search_path = pg_catalog, pg_temp
as $wrapper$
  select public.review_append_scope_snapshot_v1(
    p_root_review_id, p_scope_basis, p_root_revision_checksum, p_items, p_created_by
  )
$wrapper$;

create or replace function private.review_revision_fingerprint_v1(p_target_table text, p_target_row jsonb)
returns text language sql immutable strict parallel safe
set search_path = pg_catalog, pg_temp
as $wrapper$ select public.review_revision_fingerprint_v1(p_target_table, p_target_row) $wrapper$;

create or replace function private.review_scope_all_reference_ids_v1(p_scope_history jsonb)
returns uuid[] language sql immutable parallel safe
set search_path = pg_catalog, pg_temp
as $wrapper$ select public.review_scope_all_reference_ids_v1(p_scope_history) $wrapper$;

create or replace function private.review_scope_checksum_v1(p_items jsonb)
returns text language sql immutable strict parallel safe
set search_path = pg_catalog, pg_temp
as $wrapper$ select public.review_scope_checksum_v1(p_items) $wrapper$;

create or replace function private.review_scope_current_items_v1(p_scope_history jsonb)
returns table(
  item_kind text, target_table text, data_id uuid, data_version text,
  submitted_revision_checksum text, reference_review_id uuid,
  target_owner_id uuid, target_team_id uuid, relation_type text,
  relation_path text, introduced_by text, introduced_field_path text
)
language sql immutable parallel safe
set search_path = pg_catalog, pg_temp
as $wrapper$
  select item_kind, target_table, data_id, data_version,
         submitted_revision_checksum, reference_review_id,
         target_owner_id, target_team_id, relation_type, relation_path,
         introduced_by, introduced_field_path
  from public.review_scope_current_items_v1(p_scope_history)
$wrapper$;

create or replace function private.review_scope_current_reference_ids_v1(p_scope_history jsonb)
returns uuid[] language sql immutable parallel safe
set search_path = pg_catalog, pg_temp
as $wrapper$ select public.review_scope_current_reference_ids_v1(p_scope_history) $wrapper$;

create or replace function private.review_scope_current_snapshot_v1(p_scope_history jsonb)
returns jsonb language sql immutable parallel safe
set search_path = pg_catalog, pg_temp
as $wrapper$ select public.review_scope_current_snapshot_v1(p_scope_history) $wrapper$;

create or replace function private.review_validate_scope_history_v1(p_root_review_id uuid, p_scope_history jsonb)
returns void language sql volatile
security invoker
set search_path = pg_catalog, pg_temp
as $wrapper$ select public.review_validate_scope_history_v1(p_root_review_id, p_scope_history) $wrapper$;

alter function private.review_append_scope_snapshot_v1(uuid,text,text,jsonb,uuid) owner to postgres;
alter function private.review_revision_fingerprint_v1(text,jsonb) owner to postgres;
alter function private.review_scope_all_reference_ids_v1(jsonb) owner to postgres;
alter function private.review_scope_checksum_v1(jsonb) owner to postgres;
alter function private.review_scope_current_items_v1(jsonb) owner to postgres;
alter function private.review_scope_current_reference_ids_v1(jsonb) owner to postgres;
alter function private.review_scope_current_snapshot_v1(jsonb) owner to postgres;
alter function private.review_validate_scope_history_v1(uuid,jsonb) owner to postgres;

-- Exact source signatures own the adapter callable matrix.  Remove every
-- current grantee first, including custom roles left by an earlier replay, then
-- copy non-owner grants and grant options from the audited public source.
do $converge_adapter_acl$
declare
  mapping record;
  grantee_record record;
begin
  for mapping in
    select source_oid, target_oid from (values
      (to_regprocedure('public.review_append_scope_snapshot_v1(uuid,text,text,jsonb,uuid)'),to_regprocedure('private.review_append_scope_snapshot_v1(uuid,text,text,jsonb,uuid)')),
      (to_regprocedure('public.review_revision_fingerprint_v1(text,jsonb)'),to_regprocedure('private.review_revision_fingerprint_v1(text,jsonb)')),
      (to_regprocedure('public.review_scope_all_reference_ids_v1(jsonb)'),to_regprocedure('private.review_scope_all_reference_ids_v1(jsonb)')),
      (to_regprocedure('public.review_scope_checksum_v1(jsonb)'),to_regprocedure('private.review_scope_checksum_v1(jsonb)')),
      (to_regprocedure('public.review_scope_current_items_v1(jsonb)'),to_regprocedure('private.review_scope_current_items_v1(jsonb)')),
      (to_regprocedure('public.review_scope_current_reference_ids_v1(jsonb)'),to_regprocedure('private.review_scope_current_reference_ids_v1(jsonb)')),
      (to_regprocedure('public.review_scope_current_snapshot_v1(jsonb)'),to_regprocedure('private.review_scope_current_snapshot_v1(jsonb)')),
      (to_regprocedure('public.review_validate_scope_history_v1(uuid,jsonb)'),to_regprocedure('private.review_validate_scope_history_v1(uuid,jsonb)'))
    ) pair(source_oid,target_oid)
  loop
    -- As with relation and column ACLs above, CASCADE is limited to the exact
    -- target routine.  This removes delegated EXECUTE chains without touching
    -- the same roles' independent grants on any other routine.
    execute format('revoke all privileges on function %s from public cascade',mapping.target_oid::regprocedure);
    for grantee_record in
      select role.rolname from pg_proc p
      cross join lateral aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) with ordinality acl
      join pg_roles role on role.oid=acl.grantee
      where p.oid=mapping.target_oid and role.rolname<>'postgres'
      group by role.rolname
      order by min(acl.ordinality)
    loop
      -- The loop snapshot can still contain a downstream grantee removed by a
      -- preceding CASCADE.  Repeating the exact-routine revoke is idempotent.
      execute format('revoke all privileges on function %s from %I cascade',mapping.target_oid::regprocedure,grantee_record.rolname);
    end loop;
    for grantee_record in
      select case when acl.grantee=0 then 'PUBLIC' else role.rolname end as rolname,
        acl.is_grantable
      from pg_proc p
      cross join lateral aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) acl
      left join pg_roles role on role.oid=acl.grantee
      where p.oid=mapping.source_oid and acl.privilege_type='EXECUTE'
        and acl.grantee<>p.proowner
    loop
      execute format('grant execute on function %s to %I%s',mapping.target_oid::regprocedure,
        grantee_record.rolname,case when grantee_record.is_grantable then ' with grant option' else '' end);
    end loop;
  end loop;
end
$converge_adapter_acl$;

do $parity$
begin
  if exists (
    select 1 from issue355_relation_baseline baseline
    left join pg_class current on current.oid=baseline.oid
    where current.relowner is distinct from baseline.relowner
       or current.relkind is distinct from baseline.relkind
       or current.relrowsecurity is distinct from baseline.relrowsecurity
       or current.relforcerowsecurity is distinct from baseline.relforcerowsecurity
       or current.relacl is distinct from baseline.relacl
       or (select md5(coalesce(string_agg(
         policy.polname||':'||policy.polcmd::text||':'||policy.polpermissive::text||':'||
         coalesce((select string_agg(case when role_oid=0 then 'PUBLIC' else role_name.rolname end,
           ',' order by case when role_oid=0 then 'PUBLIC' else role_name.rolname end)
           from unnest(policy.polroles) role_oid
           left join pg_roles role_name on role_name.oid=role_oid),'')||':'||
         coalesce(pg_get_expr(policy.polqual,policy.polrelid),'')||':'||
         coalesce(pg_get_expr(policy.polwithcheck,policy.polrelid),''),
         '|' order by policy.polname),''))
         from pg_policy policy where policy.polrelid=current.oid) is distinct from baseline.policy_hash
  ) then
    raise exception 'Issue #355 source relation owner/relkind/RLS/ACL/policy changed during migration';
  end if;

  if exists (
    select 1
    from issue355_routine_baseline b
    left join pg_proc p on p.oid = b.oid
    left join pg_namespace n on n.oid = p.pronamespace
    where n.nspname is distinct from 'public'
       or p.proowner is distinct from b.proowner
       or p.provolatile is distinct from b.provolatile
       or p.prosecdef is distinct from b.prosecdef
       or p.proisstrict is distinct from b.proisstrict
       or p.proparallel is distinct from b.proparallel
       or p.proconfig is distinct from b.proconfig
       or p.proacl is distinct from b.proacl
       or pg_get_function_result(p.oid) is distinct from b.function_result
       or pg_get_functiondef(p.oid) is distinct from b.function_definition
  ) then
    raise exception 'Issue #355 audited public routine OID/property/ACL parity failed';
  end if;

  if exists (
    select 1
    from issue355_routine_baseline b
    join pg_proc source on source.oid=b.oid
    join pg_proc adapter on adapter.proname=source.proname and adapter.proargtypes=source.proargtypes
    join pg_namespace adapter_namespace on adapter_namespace.oid=adapter.pronamespace
    where adapter_namespace.nspname='private'
      and (adapter.proowner<>source.proowner or adapter.provolatile<>source.provolatile
        or adapter.prosecdef or adapter.proisstrict<>source.proisstrict
        or adapter.proparallel<>source.proparallel
        or adapter.proconfig is distinct from array['search_path=pg_catalog, pg_temp']::text[]
        or adapter.proleakproof<>source.proleakproof or adapter.procost<>source.procost
        or adapter.prorows<>source.prorows
        or exists (
          (select case when acl.grantee=0 then 'PUBLIC' else role.rolname end,acl.privilege_type,acl.is_grantable
           from aclexplode(coalesce(source.proacl,acldefault('f',source.proowner))) acl
           left join pg_roles role on role.oid=acl.grantee where acl.grantee<>source.proowner
           except
           select case when acl.grantee=0 then 'PUBLIC' else role.rolname end,acl.privilege_type,acl.is_grantable
           from aclexplode(coalesce(adapter.proacl,acldefault('f',adapter.proowner))) acl
           left join pg_roles role on role.oid=acl.grantee where acl.grantee<>adapter.proowner)
          union all
          (select case when acl.grantee=0 then 'PUBLIC' else role.rolname end,acl.privilege_type,acl.is_grantable
           from aclexplode(coalesce(adapter.proacl,acldefault('f',adapter.proowner))) acl
           left join pg_roles role on role.oid=acl.grantee where acl.grantee<>adapter.proowner
           except
           select case when acl.grantee=0 then 'PUBLIC' else role.rolname end,acl.privilege_type,acl.is_grantable
           from aclexplode(coalesce(source.proacl,acldefault('f',source.proowner))) acl
           left join pg_roles role on role.oid=acl.grantee where acl.grantee<>source.proowner)
        )
      )
  ) then
    raise exception 'Issue #355 private invoker adapter owner/property/ACL parity failed';
  end if;

end
$parity$;

comment on view api.identity_center_users_v1 is 'Issue #355 service cutover projection; identity state is retained pending owner/runtime-zero evidence.';

commit;
