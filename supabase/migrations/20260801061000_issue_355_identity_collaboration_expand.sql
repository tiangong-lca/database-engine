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

do $preflight$
declare
  v_missing text[];
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

lock table
  public.comments, public.identity_center_processed_events,
  public.identity_center_users, public.notifications, public.reviews,
  public.roles, public.teams, public.users
in access exclusive mode;

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
create view private.comments with (security_invoker = true) as
select review_id, reviewer_id, "json", created_at, modified_at, state_code
from public.comments;

create view private.identity_center_processed_events with (security_invoker = true) as
select event_id, event_type, processed_at
from public.identity_center_processed_events;

create view private.identity_center_users with (security_invoker = true) as
select keycloak_sub, user_id, status, desired_role, metadata, created_at, modified_at
from public.identity_center_users;

create view private.notifications with (security_invoker = true) as
select id, recipient_user_id, sender_user_id, type, dataset_type, dataset_id,
       dataset_version, "json", created_at, modified_at
from public.notifications;

create view private.reviews with (security_invoker = true) as
select id, data_id, created_at, modified_at, state_code, data_version,
       reviewer_id, "json", deadline, review_kind, target_table,
       submitted_revision_checksum, approved_revision_checksum,
       target_owner_id, target_team_id, scope_schema_version, scope_history,
       current_reference_review_ids, all_reference_review_ids
from public.reviews;

create view private.roles with (security_invoker = true) as
select user_id, team_id, role, created_at, modified_at
from public.roles;

create view private.teams with (security_invoker = true) as
select id, "json", created_at, modified_at, rank, is_public
from public.teams;

create view private.users with (security_invoker = true) as
select id, raw_user_meta_data, contact
from public.users;

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
create view api.notifications_v1 with (security_invoker = true) as
select id, recipient_user_id, sender_user_id, type, dataset_type, dataset_id,
       dataset_version, "json", created_at, modified_at
from public.notifications;

create view api.reviews_v1 with (security_invoker = true) as
select id, data_id, created_at, modified_at, state_code, data_version,
       reviewer_id, "json", deadline, review_kind, target_table,
       submitted_revision_checksum, approved_revision_checksum,
       target_owner_id, target_team_id, scope_schema_version, scope_history,
       current_reference_review_ids, all_reference_review_ids
from public.reviews;

create view api.team_roles_v1 with (security_invoker = true) as
select user_id, team_id, role, created_at, modified_at
from public.roles;

create view api.teams_v1 with (security_invoker = true) as
select id, "json", created_at, modified_at, rank, is_public
from public.teams;

create view api.user_profiles_v1 with (security_invoker = true) as
select id, contact, raw_user_meta_data ->> 'email' as email,
       raw_user_meta_data ->> 'display_name' as display_name
from public.users;

create view api.identity_center_processed_events_v1 with (security_invoker = true) as
select event_id, event_type, processed_at
from public.identity_center_processed_events;

create view api.identity_center_users_v1 with (security_invoker = true) as
select keycloak_sub, user_id, status, desired_role, metadata, created_at, modified_at
from public.identity_center_users;

revoke all on
  api.notifications_v1, api.reviews_v1,
  api.team_roles_v1, api.teams_v1, api.user_profiles_v1,
  api.identity_center_processed_events_v1, api.identity_center_users_v1
from public, anon, authenticated, service_role, api_internal_executor;

grant select on api.reviews_v1,
  api.team_roles_v1, api.teams_v1, api.user_profiles_v1
to anon, authenticated, service_role;

grant select on api.notifications_v1,
  api.identity_center_processed_events_v1, api.identity_center_users_v1
to service_role;

-- Keep the audited SECURITY DEFINER routines physically public during Expand.
-- Private invoker adapters create the internal cutover target without adding a
-- second privileged routine or changing the #333 public audit population.
create function private.review_append_scope_snapshot_v1(
  p_root_review_id uuid, p_scope_basis text, p_root_revision_checksum text,
  p_items jsonb, p_created_by uuid
) returns jsonb
language sql volatile
set search_path = ''
as $wrapper$
  select public.review_append_scope_snapshot_v1(
    p_root_review_id, p_scope_basis, p_root_revision_checksum, p_items, p_created_by
  )
$wrapper$;

create function private.review_revision_fingerprint_v1(p_target_table text, p_target_row jsonb)
returns text language sql immutable strict parallel safe
set search_path = ''
as $wrapper$ select public.review_revision_fingerprint_v1(p_target_table, p_target_row) $wrapper$;

create function private.review_scope_all_reference_ids_v1(p_scope_history jsonb)
returns uuid[] language sql immutable parallel safe
set search_path = ''
as $wrapper$ select public.review_scope_all_reference_ids_v1(p_scope_history) $wrapper$;

create function private.review_scope_checksum_v1(p_items jsonb)
returns text language sql immutable strict parallel safe
set search_path = ''
as $wrapper$ select public.review_scope_checksum_v1(p_items) $wrapper$;

create function private.review_scope_current_items_v1(p_scope_history jsonb)
returns table(
  item_kind text, target_table text, data_id uuid, data_version text,
  submitted_revision_checksum text, reference_review_id uuid,
  target_owner_id uuid, target_team_id uuid, relation_type text,
  relation_path text, introduced_by text, introduced_field_path text
)
language sql immutable parallel safe
set search_path = ''
as $wrapper$
  select item_kind, target_table, data_id, data_version,
         submitted_revision_checksum, reference_review_id,
         target_owner_id, target_team_id, relation_type, relation_path,
         introduced_by, introduced_field_path
  from public.review_scope_current_items_v1(p_scope_history)
$wrapper$;

create function private.review_scope_current_reference_ids_v1(p_scope_history jsonb)
returns uuid[] language sql immutable parallel safe
set search_path = ''
as $wrapper$ select public.review_scope_current_reference_ids_v1(p_scope_history) $wrapper$;

create function private.review_scope_current_snapshot_v1(p_scope_history jsonb)
returns jsonb language sql immutable parallel safe
set search_path = ''
as $wrapper$ select public.review_scope_current_snapshot_v1(p_scope_history) $wrapper$;

create function private.review_validate_scope_history_v1(p_root_review_id uuid, p_scope_history jsonb)
returns void language sql volatile
set search_path = ''
as $wrapper$ select public.review_validate_scope_history_v1(p_root_review_id, p_scope_history) $wrapper$;

revoke all on function private.review_append_scope_snapshot_v1(uuid,text,text,jsonb,uuid) from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.review_revision_fingerprint_v1(text,jsonb) from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.review_scope_all_reference_ids_v1(jsonb) from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.review_scope_checksum_v1(jsonb) from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.review_scope_current_items_v1(jsonb) from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.review_scope_current_reference_ids_v1(jsonb) from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.review_scope_current_snapshot_v1(jsonb) from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.review_validate_scope_history_v1(uuid,jsonb) from public, anon, authenticated, service_role, api_internal_executor;

grant execute on function private.review_append_scope_snapshot_v1(uuid,text,text,jsonb,uuid) to api_internal_executor;
grant execute on function private.review_revision_fingerprint_v1(text,jsonb) to service_role, api_internal_executor;
grant execute on function private.review_scope_all_reference_ids_v1(jsonb) to api_internal_executor;
grant execute on function private.review_scope_checksum_v1(jsonb) to api_internal_executor;
grant execute on function private.review_scope_current_items_v1(jsonb) to api_internal_executor;
grant execute on function private.review_scope_current_reference_ids_v1(jsonb) to api_internal_executor;
grant execute on function private.review_scope_current_snapshot_v1(jsonb) to api_internal_executor;
grant execute on function private.review_validate_scope_history_v1(uuid,jsonb) to api_internal_executor;

do $parity$
begin
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

end
$parity$;

comment on view api.identity_center_users_v1 is 'Issue #355 service cutover projection; identity state is retained pending owner/runtime-zero evidence.';

commit;
