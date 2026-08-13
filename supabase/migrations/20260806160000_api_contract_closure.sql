begin;

set local lock_timeout = '10s';
set local statement_timeout = '10min';

-- Issue #422 contract closure.  The cutover migration intentionally preserved
-- object ACLs by OID; close the inherited PostgreSQL PUBLIC execute default and
-- make every newly introduced facade opt-in by caller role.
revoke execute on all functions in schema api from public, anon, authenticated, service_role;
alter default privileges for role postgres revoke execute on functions from public;

-- Eight constrained search facades are owned by this non-login executor.
-- REVOKE uses the current grantor, so close its inherited PUBLIC grants while
-- executing as the owning role as well.
grant api_internal_executor to postgres;
grant create on schema api to api_internal_executor;
set local role api_internal_executor;
do $revoke_executor_public$
declare
  routine regprocedure;
begin
  for routine in
    select proc.oid::regprocedure
    from pg_proc as proc
    join pg_namespace as namespace on namespace.oid = proc.pronamespace
    where namespace.nspname = 'api'
      and proc.proowner = current_user::regrole
  loop
    execute format(
      'revoke execute on function %s from public, anon, authenticated, service_role',
      routine
    );
  end loop;
end
$revoke_executor_public$;
reset role;

-- These four simple-search facades call the non-public helper under the
-- constrained, RLS-bound executor. Keeping them SECURITY INVOKER would make
-- the now-closed helper ACL break otherwise valid anon/authenticated calls.
alter function api.search_contacts_latest(text, jsonb, bigint, bigint, text, text, uuid, integer)
  security definer;
alter function api.search_contacts_latest(text, jsonb, bigint, bigint, text, text, uuid, integer)
  set search_path = '';
alter function api.search_contacts_latest(text, jsonb, bigint, bigint, text, text, uuid, integer)
  owner to api_internal_executor;

alter function api.search_flowproperties_latest(text, jsonb, bigint, bigint, text, text, uuid, integer)
  security definer;
alter function api.search_flowproperties_latest(text, jsonb, bigint, bigint, text, text, uuid, integer)
  set search_path = '';
alter function api.search_flowproperties_latest(text, jsonb, bigint, bigint, text, text, uuid, integer)
  owner to api_internal_executor;

alter function api.search_sources_latest(text, jsonb, bigint, bigint, text, text, uuid, integer)
  security definer;
alter function api.search_sources_latest(text, jsonb, bigint, bigint, text, text, uuid, integer)
  set search_path = '';
alter function api.search_sources_latest(text, jsonb, bigint, bigint, text, text, uuid, integer)
  owner to api_internal_executor;

alter function api.search_unitgroups_latest(text, jsonb, bigint, bigint, text, text, uuid, integer)
  security definer;
alter function api.search_unitgroups_latest(text, jsonb, bigint, bigint, text, text, uuid, integer)
  set search_path = '';
alter function api.search_unitgroups_latest(text, jsonb, bigint, bigint, text, text, uuid, integer)
  owner to api_internal_executor;

-- This legacy helper is an implementation detail used by the guarded review
-- command.  It must never be a directly callable anonymous RPC.
revoke execute on function api.cmd_review_submit_without_gate(text, uuid, text, jsonb)
  from public, anon, authenticated, service_role;

-- ---------------------------------------------------------------------------
-- Authenticated, actor-derived team / membership / identity read contracts.
-- ---------------------------------------------------------------------------

create or replace function api.qry_membership_get_mine()
returns table (
  user_id uuid,
  team_id uuid,
  role text,
  created_at timestamptz,
  modified_at timestamptz
)
language sql
stable
security definer
set search_path = ''
as $function$
  select
    membership.user_id,
    membership.team_id,
    membership.role::text,
    membership.created_at,
    membership.modified_at
  from private.roles as membership
  where auth.uid() is not null
    and membership.user_id = auth.uid()
  order by
    membership.modified_at desc nulls last,
    membership.created_at desc nulls last,
    membership.team_id,
    membership.role
$function$;

create or replace function api.qry_team_list(
  p_mode text,
  p_keyword text default null,
  p_page integer default 1,
  p_page_size integer default 10
)
returns table (
  id uuid,
  "json" jsonb,
  rank integer,
  is_public boolean,
  created_at timestamptz,
  modified_at timestamptz,
  owner_user_id uuid,
  owner_email text,
  total_count bigint
)
language plpgsql
stable
security definer
set search_path = ''
as $function$
declare
  v_actor uuid := auth.uid();
  v_mode text := lower(btrim(coalesce(p_mode, '')));
  v_keyword text := nullif(btrim(coalesce(p_keyword, '')), '');
  v_page integer := greatest(coalesce(p_page, 1), 1);
  v_page_size integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_pattern text;
  v_system_manager boolean;
begin
  if v_actor is null then
    raise exception using errcode = '42501', message = 'AUTH_REQUIRED';
  end if;

  if v_mode not in ('ranked', 'public', 'unranked') then
    raise exception using errcode = '22023', message = 'INVALID_TEAM_LIST_MODE';
  end if;

  if v_keyword is not null and length(v_keyword) > 128 then
    raise exception using errcode = '22023', message = 'TEAM_KEYWORD_TOO_LONG';
  end if;

  select exists (
    select 1
    from private.roles as membership
    where membership.user_id = v_actor
      and membership.team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and membership.role in ('owner', 'admin', 'member')
  ) into v_system_manager;

  if v_mode = 'unranked' and not v_system_manager then
    raise exception using errcode = '42501', message = 'SYSTEM_MANAGER_REQUIRED';
  end if;

  -- Treat PostgREST filter metacharacters as data, not as query syntax.
  v_pattern := case when v_keyword is null then null else
    '%' || replace(replace(replace(v_keyword, E'\\', E'\\\\'), '%', E'\\%'), '_', E'\\_') || '%'
  end;

  return query
  with visible as (
    select team.*
    from private.teams as team
    where case v_mode
      when 'ranked' then coalesce(team.rank, -1) > 0
      when 'public' then coalesce(team.is_public, false)
      when 'unranked' then coalesce(team.rank, -1) = 0
    end
      and (
        v_pattern is null
        or coalesce(team.json #>> '{title,0,#text}', '') ilike v_pattern escape E'\\'
        or coalesce(team.json #>> '{title,1,#text}', '') ilike v_pattern escape E'\\'
      )
  ), enriched as (
    select
      team.id,
      team.json,
      team.rank,
      team.is_public,
      team.created_at,
      team.modified_at,
      owner_membership.user_id as owner_user_id,
      coalesce(
        nullif(btrim(auth_owner.email), ''),
        nullif(btrim(owner_profile.raw_user_meta_data ->> 'email'), '')
      ) as owner_email,
      count(*) over () as total_count
    from visible as team
    left join lateral (
      select membership.user_id
      from private.roles as membership
      where membership.team_id = team.id
        and membership.role = 'owner'
      order by membership.created_at, membership.user_id
      limit 1
    ) as owner_membership on true
    left join auth.users as auth_owner on auth_owner.id = owner_membership.user_id
    left join private.users as owner_profile on owner_profile.id = owner_membership.user_id
  )
  select enriched.*
  from enriched
  order by
    case when v_mode = 'unranked' then enriched.created_at end desc nulls last,
    case when v_mode <> 'unranked' then enriched.rank end asc nulls last,
    enriched.id
  offset (v_page - 1) * v_page_size
  limit v_page_size;
end
$function$;

create or replace function api.qry_team_get(p_team_id uuid)
returns table (
  id uuid,
  "json" jsonb,
  rank integer,
  is_public boolean,
  created_at timestamptz,
  modified_at timestamptz
)
language sql
stable
security definer
set search_path = ''
as $function$
  select
    team.id,
    team.json,
    team.rank,
    team.is_public,
    team.created_at,
    team.modified_at
  from private.teams as team
  where auth.uid() is not null
    and team.id = p_team_id
    and (
      coalesce(team.is_public, false)
      or coalesce(team.rank, -1) > 0
      or exists (
        select 1
        from private.roles as membership
        where membership.user_id = auth.uid()
          and (
            membership.team_id = team.id
            or (
              membership.team_id = '00000000-0000-0000-0000-000000000000'::uuid
              and membership.role in ('owner', 'admin', 'member')
            )
          )
      )
    )
$function$;

create or replace function api.qry_identity_get_mine()
returns table (
  id uuid,
  email text,
  display_name text,
  contact jsonb
)
language sql
stable
security definer
set search_path = ''
as $function$
  select
    profile.id,
    coalesce(
      nullif(btrim(auth_user.email), ''),
      nullif(btrim(profile.raw_user_meta_data ->> 'email'), '')
    ),
    coalesce(
      nullif(btrim(profile.raw_user_meta_data ->> 'display_name'), ''),
      nullif(btrim(profile.raw_user_meta_data ->> 'name'), ''),
      nullif(btrim(auth_user.email), '')
    ),
    profile.contact
  from private.users as profile
  left join auth.users as auth_user on auth_user.id = profile.id
  where auth.uid() is not null
    and profile.id = auth.uid()
$function$;

create or replace function api.qry_identity_get_visible_users(p_user_ids uuid[])
returns table (
  id uuid,
  email text,
  display_name text
)
language plpgsql
stable
security definer
set search_path = ''
as $function$
declare
  v_actor uuid := auth.uid();
  v_requested_count integer := coalesce(cardinality(p_user_ids), 0);
begin
  if v_actor is null then
    raise exception using errcode = '42501', message = 'AUTH_REQUIRED';
  end if;

  if v_requested_count > 100 then
    raise exception using errcode = '22023', message = 'TOO_MANY_USER_IDS';
  end if;

  return query
  with requested as (
    select distinct requested_id
    from unnest(coalesce(p_user_ids, '{}'::uuid[])) as requested_id
    where requested_id is not null
  ), visible as (
    select requested.requested_id
    from requested
    where requested.requested_id = v_actor
      or exists (
        select 1
        from private.roles as actor_membership
        join private.roles as target_membership
          on target_membership.team_id = actor_membership.team_id
        where actor_membership.user_id = v_actor
          and target_membership.user_id = requested.requested_id
          and actor_membership.team_id <> '00000000-0000-0000-0000-000000000000'::uuid
          and actor_membership.role not in ('rejected', 'is_invited')
          and target_membership.role not in ('rejected', 'is_invited')
      )
      or exists (
        select 1
        from private.reviews as review
        where api.policy_review_can_read(review.id, v_actor)
          and (
            review.target_owner_id = requested.requested_id
            or review.reviewer_id ? requested.requested_id::text
            or review.json -> 'user' ->> 'id' = requested.requested_id::text
          )
      )
      or exists (
        select 1
        from private.teams as team
        join private.roles as owner_membership
          on owner_membership.team_id = team.id
         and owner_membership.role = 'owner'
        where coalesce(team.is_public, false)
          and owner_membership.user_id = requested.requested_id
      )
  )
  select
    profile.id,
    coalesce(
      nullif(btrim(auth_user.email), ''),
      nullif(btrim(profile.raw_user_meta_data ->> 'email'), '')
    ),
    coalesce(
      nullif(btrim(profile.raw_user_meta_data ->> 'display_name'), ''),
      nullif(btrim(profile.raw_user_meta_data ->> 'name'), ''),
      nullif(btrim(auth_user.email), '')
    )
  from visible
  join private.users as profile on profile.id = visible.requested_id
  left join auth.users as auth_user on auth_user.id = profile.id
  order by profile.id;
end
$function$;

create or replace function api.qry_system_find_member_candidate_by_email(p_email text)
returns table (id uuid, email text, display_name text)
language plpgsql
stable
security definer
set search_path = ''
as $function$
declare
  v_actor uuid := auth.uid();
  v_email text := lower(btrim(coalesce(p_email, '')));
begin
  if v_actor is null then
    raise exception using errcode = '42501', message = 'AUTH_REQUIRED';
  end if;
  if v_email = '' or length(v_email) > 320 then
    raise exception using errcode = '22023', message = 'INVALID_EMAIL';
  end if;
  if not exists (
    select 1 from private.roles
    where user_id = v_actor
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role in ('owner', 'admin', 'member')
  ) then
    raise exception using errcode = '42501', message = 'SYSTEM_MANAGER_REQUIRED';
  end if;

  return query
  select
    profile.id,
    coalesce(nullif(btrim(auth_user.email), ''), nullif(btrim(profile.raw_user_meta_data ->> 'email'), '')),
    coalesce(
      nullif(btrim(profile.raw_user_meta_data ->> 'display_name'), ''),
      nullif(btrim(profile.raw_user_meta_data ->> 'name'), ''),
      nullif(btrim(auth_user.email), '')
    )
  from private.users as profile
  left join auth.users as auth_user on auth_user.id = profile.id
  where lower(btrim(coalesce(auth_user.email, profile.raw_user_meta_data ->> 'email', ''))) = v_email
  order by auth_user.created_at desc nulls last, profile.id
  limit 1;
end
$function$;

create or replace function api.qry_review_find_member_candidate_by_email(p_email text)
returns table (id uuid, email text, display_name text, contact jsonb)
language plpgsql
stable
security definer
set search_path = ''
as $function$
declare
  v_actor uuid := auth.uid();
  v_email text := lower(btrim(coalesce(p_email, '')));
begin
  if v_actor is null then
    raise exception using errcode = '42501', message = 'AUTH_REQUIRED';
  end if;
  if v_email = '' or length(v_email) > 320 then
    raise exception using errcode = '22023', message = 'INVALID_EMAIL';
  end if;
  if not exists (
    select 1 from private.roles
    where user_id = v_actor
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role = 'review-admin'
  ) then
    raise exception using errcode = '42501', message = 'REVIEW_ADMIN_REQUIRED';
  end if;

  return query
  select
    profile.id,
    coalesce(nullif(btrim(auth_user.email), ''), nullif(btrim(profile.raw_user_meta_data ->> 'email'), '')),
    coalesce(
      nullif(btrim(profile.raw_user_meta_data ->> 'display_name'), ''),
      nullif(btrim(profile.raw_user_meta_data ->> 'name'), ''),
      nullif(btrim(auth_user.email), '')
    ),
    profile.contact
  from private.users as profile
  left join auth.users as auth_user on auth_user.id = profile.id
  where lower(btrim(coalesce(auth_user.email, profile.raw_user_meta_data ->> 'email', ''))) = v_email
  order by auth_user.created_at desc nulls last, profile.id
  limit 1;
end
$function$;

-- ---------------------------------------------------------------------------
-- Service-only thin facades over already-reviewed transactional routines.
-- ---------------------------------------------------------------------------

create or replace function api.svc_dataset_review_submit_job_claim(
  p_qty integer default 10,
  p_stale_submitting_seconds integer default 300
)
returns jsonb language sql volatile security definer set search_path = ''
as $function$
  select private.cmd_dataset_review_submit_job_claim(p_qty, p_stale_submitting_seconds)
$function$;

create or replace function api.svc_dataset_review_submit_job_record_result(
  p_job_id uuid,
  p_status text,
  p_gate_run_id uuid default null,
  p_result jsonb default null,
  p_error_code text default null,
  p_error_message text default null,
  p_error_details jsonb default null,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb language sql volatile security definer set search_path = ''
as $function$
  select private.cmd_dataset_review_submit_job_record_result(
    p_job_id, p_status, p_gate_run_id, p_result, p_error_code,
    p_error_message, p_error_details, p_audit
  )
$function$;

create or replace function api.svc_review_submit_from_job(
  p_job_id uuid,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb language sql volatile security definer set search_path = ''
as $function$
  select private.cmd_review_submit_from_job(p_job_id, p_audit)
$function$;

create or replace function api.svc_worker_enqueue_job(
  p_job_kind text,
  p_payload_json jsonb default '{}'::jsonb,
  p_payload_schema_version text default null,
  p_subject_type text default null,
  p_subject_id uuid default null,
  p_subject_version text default null,
  p_requested_by uuid default null,
  p_requester_type text default 'user',
  p_team_id uuid default null,
  p_idempotency_key text default null,
  p_request_hash text default null,
  p_concurrency_key text default null,
  p_priority integer default null,
  p_queue_key text default null,
  p_run_after timestamptz default null,
  p_visibility text default null,
  p_max_attempts integer default null,
  p_timeout_at timestamptz default null,
  p_payload_ref jsonb default null,
  p_parent_job_id uuid default null,
  p_root_job_id uuid default null
)
returns jsonb language sql volatile security definer set search_path = ''
as $function$
  select private.worker_enqueue_job(
    p_job_kind => p_job_kind,
    p_payload_json => p_payload_json,
    p_payload_schema_version => p_payload_schema_version,
    p_subject_type => p_subject_type,
    p_subject_id => p_subject_id,
    p_subject_version => p_subject_version,
    p_requested_by => p_requested_by,
    p_requester_type => p_requester_type,
    p_team_id => p_team_id,
    p_idempotency_key => p_idempotency_key,
    p_request_hash => p_request_hash,
    p_concurrency_key => p_concurrency_key,
    p_priority => p_priority,
    p_queue_key => p_queue_key,
    p_run_after => p_run_after,
    p_visibility => p_visibility,
    p_max_attempts => p_max_attempts,
    p_timeout_at => p_timeout_at,
    p_payload_ref => p_payload_ref,
    p_parent_job_id => p_parent_job_id,
    p_root_job_id => p_root_job_id
  )
$function$;

create or replace function api.svc_worker_read_job(
  p_job_id uuid,
  p_include_internal boolean default false
)
returns jsonb language sql stable security definer set search_path = ''
as $function$
  select private.worker_read_job(p_job_id, p_include_internal)
$function$;

create or replace function api.svc_worker_list_jobs(
  p_requested_by uuid default null,
  p_subject_type text default null,
  p_subject_id uuid default null,
  p_statuses text[] default null,
  p_visibility text default null,
  p_limit integer default 50,
  p_include_internal boolean default false
)
returns jsonb language sql stable security definer set search_path = ''
as $function$
  select private.worker_list_jobs(
    p_requested_by, p_subject_type, p_subject_id, p_statuses,
    p_visibility, p_limit, p_include_internal
  )
$function$;

create or replace function api.svc_worker_cancel_job(
  p_job_id uuid,
  p_cancelled_by uuid default null,
  p_reason text default null
)
returns jsonb language sql volatile security definer set search_path = ''
as $function$
  select private.worker_cancel_job(p_job_id, p_cancelled_by, p_reason)
$function$;

create or replace function api.svc_lca_read_job_projection(
  p_requested_by uuid,
  p_worker_job_id uuid default null,
  p_legacy_job_id uuid default null,
  p_include_internal boolean default false
)
returns jsonb language sql stable security definer set search_path = ''
as $function$
  select private.lca_read_job_projection(
    p_requested_by => p_requested_by,
    p_worker_job_id => p_worker_job_id,
    p_legacy_job_id => p_legacy_job_id,
    p_include_internal => p_include_internal
  )
$function$;

create or replace function api.svc_lca_read_result_projection(
  p_requested_by uuid,
  p_result_id uuid,
  p_required_artifact_format text default null,
  p_include_internal boolean default false
)
returns jsonb language sql stable security definer set search_path = ''
as $function$
  select private.lca_read_result_projection(
    p_requested_by, p_result_id, p_required_artifact_format, p_include_internal
  )
$function$;

create or replace function api.svc_lca_read_latest_single_solve_result(
  p_requested_by uuid,
  p_snapshot_id uuid,
  p_process_index integer
)
returns jsonb language sql stable security definer set search_path = ''
as $function$
  select private.lca_read_latest_single_solve_result(
    p_requested_by, p_snapshot_id, p_process_index
  )
$function$;

-- Identity Center owns durable desired state and its managed-role projection;
-- GoTrue admin side effects remain outside the database transaction in Edge.
create or replace function api.svc_identity_event_claim(
  p_event_id text,
  p_event_type text
)
returns boolean language plpgsql volatile security definer set search_path = ''
as $function$
declare
  v_inserted integer;
begin
  if nullif(pg_catalog.btrim(p_event_id), '') is null then
    raise exception using errcode = '22023', message = 'IDENTITY_EVENT_ID_REQUIRED';
  end if;
  insert into private.identity_center_processed_events (event_id, event_type, processed_at)
  values (
    p_event_id,
    coalesce(nullif(pg_catalog.btrim(p_event_type), ''), 'unknown'),
    pg_catalog.now()
  )
  on conflict (event_id) do nothing;
  get diagnostics v_inserted = row_count;
  return v_inserted = 1;
end
$function$;

create or replace function api.svc_identity_event_release(p_event_id text)
returns boolean language plpgsql volatile security definer set search_path = ''
as $function$
declare
  v_deleted integer;
begin
  if nullif(pg_catalog.btrim(p_event_id), '') is null then
    raise exception using errcode = '22023', message = 'IDENTITY_EVENT_ID_REQUIRED';
  end if;
  delete from private.identity_center_processed_events where event_id = p_event_id;
  get diagnostics v_deleted = row_count;
  return v_deleted = 1;
end
$function$;

create or replace function api.svc_identity_desired_state_upsert(
  p_keycloak_sub text,
  p_status text default null,
  p_role_code text default null,
  p_role_operation text default 'preserve',
  p_metadata jsonb default null
)
returns jsonb language plpgsql volatile security definer set search_path = ''
as $function$
declare
  v_operation text := lower(coalesce(nullif(pg_catalog.btrim(p_role_operation), ''), 'preserve'));
  v_state private.identity_center_users%rowtype;
begin
  if nullif(pg_catalog.btrim(p_keycloak_sub), '') is null then
    raise exception using errcode = '22023', message = 'KEYCLOAK_SUB_REQUIRED';
  end if;
  if p_status is not null and p_status not in ('active', 'disabled', 'revoked', 'deleted') then
    raise exception using errcode = '22023', message = 'INVALID_IDENTITY_STATUS';
  end if;
  if v_operation not in ('preserve', 'set', 'revoke') then
    raise exception using errcode = '22023', message = 'INVALID_ROLE_OPERATION';
  end if;
  if v_operation = 'set' and nullif(pg_catalog.btrim(p_role_code), '') is null then
    raise exception using errcode = '22023', message = 'ROLE_CODE_REQUIRED';
  end if;

  insert into private.identity_center_users as current_state (
    keycloak_sub, status, desired_role, metadata, created_at, modified_at
  ) values (
    p_keycloak_sub,
    coalesce(p_status, 'active'),
    case when v_operation = 'set' then p_role_code else null end,
    coalesce(p_metadata, '{}'::jsonb),
    pg_catalog.now(),
    pg_catalog.now()
  )
  on conflict (keycloak_sub) do update set
    status = coalesce(p_status, current_state.status),
    desired_role = case v_operation
      when 'preserve' then current_state.desired_role
      when 'set' then p_role_code
      when 'revoke' then case
        when p_role_code is null or current_state.desired_role = p_role_code then null
        else current_state.desired_role
      end
    end,
    metadata = coalesce(p_metadata, current_state.metadata),
    modified_at = pg_catalog.now()
  returning * into v_state;

  return pg_catalog.to_jsonb(v_state);
end
$function$;

create or replace function api.svc_identity_desired_state_read(p_keycloak_sub text)
returns jsonb language plpgsql stable security definer set search_path = ''
as $function$
declare
  v_state private.identity_center_users%rowtype;
begin
  if nullif(pg_catalog.btrim(p_keycloak_sub), '') is null then
    raise exception using errcode = '22023', message = 'KEYCLOAK_SUB_REQUIRED';
  end if;
  select identity_state.* into v_state
  from private.identity_center_users as identity_state
  where identity_state.keycloak_sub = p_keycloak_sub;
  if not found then return null; end if;
  return pg_catalog.to_jsonb(v_state);
end
$function$;

create or replace function api.svc_identity_login_bind(
  p_keycloak_sub text,
  p_user_id uuid
)
returns jsonb language plpgsql volatile security definer set search_path = ''
as $function$
declare
  v_state private.identity_center_users%rowtype;
begin
  if nullif(pg_catalog.btrim(p_keycloak_sub), '') is null or p_user_id is null then
    raise exception using errcode = '22023', message = 'IDENTITY_BINDING_REQUIRED';
  end if;
  begin
    insert into private.identity_center_users as current_state (
      keycloak_sub, user_id, status, metadata, created_at, modified_at
    ) values (
      p_keycloak_sub, p_user_id, 'active', '{}'::jsonb, pg_catalog.now(), pg_catalog.now()
    )
    on conflict (keycloak_sub) do update set
      user_id = excluded.user_id,
      status = 'active',
      modified_at = pg_catalog.now()
    where current_state.user_id is null or current_state.user_id = excluded.user_id
    returning * into v_state;
  exception when unique_violation then
    raise exception using errcode = '23505', message = 'IDENTITY_USER_ALREADY_BOUND';
  end;
  if v_state.keycloak_sub is null then
    raise exception using errcode = '23505', message = 'IDENTITY_SUBJECT_ALREADY_BOUND';
  end if;
  return pg_catalog.to_jsonb(v_state);
end
$function$;

create or replace function api.svc_identity_managed_role_materialize(
  p_keycloak_sub text,
  p_user_id uuid
)
returns jsonb language plpgsql volatile security definer set search_path = ''
as $function$
declare
  v_system_team_id constant uuid := '00000000-0000-0000-0000-000000000000'::uuid;
  v_managed_roles constant text[] := array['admin', 'review-admin', 'review-member'];
  v_desired_role text;
  v_previous_role text;
  v_effective_role text;
  v_changed boolean := false;
  v_reason text;
begin
  if nullif(pg_catalog.btrim(p_keycloak_sub), '') is null or p_user_id is null then
    raise exception using errcode = '22023', message = 'IDENTITY_BINDING_REQUIRED';
  end if;
  select desired_role::text into v_desired_role
  from private.identity_center_users
  where keycloak_sub = p_keycloak_sub and user_id = p_user_id
  for update;
  if not found then
    raise exception using errcode = 'P0001', message = 'IDENTITY_BINDING_MISMATCH';
  end if;

  select role::text into v_previous_role
  from private.roles
  where user_id = p_user_id and team_id = v_system_team_id
  for update;
  v_effective_role := v_previous_role;

  if v_desired_role = any(v_managed_roles) then
    if v_previous_role is null then
      insert into private.roles as existing_role (user_id, team_id, role, created_at, modified_at)
      values (p_user_id, v_system_team_id, v_desired_role, pg_catalog.now(), pg_catalog.now())
      on conflict (user_id, team_id) do update set
        role = excluded.role,
        modified_at = pg_catalog.now()
      where existing_role.role::text = any(v_managed_roles)
        and existing_role.role::text is distinct from excluded.role::text
      returning role::text into v_effective_role;
      if found then
        v_changed := true;
        v_reason := 'managed_role_materialized';
      else
        select role::text into v_effective_role from private.roles
        where user_id = p_user_id and team_id = v_system_team_id;
        v_reason := case when v_effective_role = v_desired_role
          then 'already_materialized' else 'non_managed_role_preserved' end;
      end if;
    elsif v_previous_role = any(v_managed_roles) and v_previous_role is distinct from v_desired_role then
      update private.roles set role = v_desired_role, modified_at = pg_catalog.now()
      where user_id = p_user_id and team_id = v_system_team_id
        and role::text = any(v_managed_roles)
      returning role::text into v_effective_role;
      v_changed := found;
      v_reason := case when v_changed then 'managed_role_updated' else 'managed_role_changed_concurrently' end;
    else
      v_reason := case when v_previous_role = v_desired_role
        then 'already_materialized' else 'non_managed_role_preserved' end;
    end if;
  elsif v_desired_role is null and v_previous_role = any(v_managed_roles) then
    update private.roles set role = 'member', modified_at = pg_catalog.now()
    where user_id = p_user_id and team_id = v_system_team_id
      and role::text = any(v_managed_roles)
    returning role::text into v_effective_role;
    v_changed := found;
    v_reason := 'managed_role_revoked';
  elsif v_desired_role is null then
    v_reason := case when v_previous_role is null
      then 'no_desired_or_current_role' else 'non_managed_role_preserved' end;
  else
    v_reason := 'unsupported_desired_role';
  end if;

  return pg_catalog.jsonb_build_object(
    'keycloak_sub', p_keycloak_sub,
    'user_id', p_user_id,
    'team_id', v_system_team_id,
    'desired_role', v_desired_role,
    'previous_role', v_previous_role,
    'effective_role', v_effective_role,
    'changed', v_changed,
    'reason', v_reason
  );
end
$function$;

create or replace function api.svc_schema_contract_status()
returns jsonb
language sql
stable
security definer
set search_path = ''
as $function$
  select jsonb_build_object(
    'migrationHead', (
      select max(version) from supabase_migrations.schema_migrations
    ),
    'publicCoreTables', (
      select count(*)
      from pg_catalog.pg_class as class
      join pg_catalog.pg_namespace as namespace on namespace.oid = class.relnamespace
      where namespace.nspname = 'public' and class.relkind in ('r', 'p')
    ),
    'publicRoutines', (
      select count(*)
      from pg_catalog.pg_proc as routine
      join pg_catalog.pg_namespace as namespace on namespace.oid = routine.pronamespace
      where namespace.nspname = 'public'
    ),
    'apiRoutines', (
      select count(*)
      from pg_catalog.pg_proc as routine
      join pg_catalog.pg_namespace as namespace on namespace.oid = routine.pronamespace
      where namespace.nspname = 'api' and routine.prokind = 'f'
    )
  )
$function$;

-- Keep caller identity inside the same transaction as lifecycle mutations.
create or replace function api.cmd_lifecycle_model_bundle_save(p_plan jsonb)
returns jsonb
language plpgsql
volatile
security definer
set search_path = ''
as $function$
declare
  v_actor uuid := auth.uid();
  v_plan jsonb := coalesce(p_plan, '{}'::jsonb);
begin
  if v_actor is null then
    raise exception using errcode = '42501', message = 'AUTH_REQUIRED';
  end if;
  v_plan := jsonb_set(v_plan, '{actorUserId}', to_jsonb(v_actor::text), true);
  return private.save_lifecycle_model_bundle(v_plan);
end
$function$;

create or replace function api.cmd_lifecycle_model_bundle_delete(
  p_model_id uuid,
  p_version text
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = ''
as $function$
declare
  v_actor uuid := auth.uid();
  v_owner uuid;
begin
  if v_actor is null then
    raise exception using errcode = '42501', message = 'AUTH_REQUIRED';
  end if;

  select model.user_id
  into v_owner
  from public.lifecyclemodels as model
  where model.id = p_model_id
    and model.version = p_version
  for update;

  if v_owner is null then
    raise exception using errcode = 'P0002', message = 'MODEL_NOT_FOUND';
  end if;

  if v_owner <> v_actor and not exists (
    select 1
    from private.roles as membership
    where membership.user_id = v_actor
      and membership.team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and membership.role = 'review-admin'
  ) then
    raise exception using errcode = '42501', message = 'FORBIDDEN';
  end if;

  return private.delete_lifecycle_model_bundle(p_model_id, p_version);
end
$function$;

-- Every facade is denied by default, then exact caller contracts are granted.
revoke execute on function api.qry_membership_get_mine() from public, anon, service_role;
revoke execute on function api.qry_team_list(text, text, integer, integer) from public, anon, service_role;
revoke execute on function api.qry_team_get(uuid) from public, anon, service_role;
revoke execute on function api.qry_identity_get_mine() from public, anon, service_role;
revoke execute on function api.qry_identity_get_visible_users(uuid[]) from public, anon, service_role;
revoke execute on function api.qry_system_find_member_candidate_by_email(text) from public, anon, service_role;
revoke execute on function api.qry_review_find_member_candidate_by_email(text) from public, anon, service_role;
revoke execute on function api.cmd_lifecycle_model_bundle_save(jsonb) from public, anon, service_role;
revoke execute on function api.cmd_lifecycle_model_bundle_delete(uuid, text) from public, anon, service_role;

grant execute on function api.qry_membership_get_mine() to authenticated;
grant execute on function api.qry_team_list(text, text, integer, integer) to authenticated;
grant execute on function api.qry_team_get(uuid) to authenticated;
grant execute on function api.qry_identity_get_mine() to authenticated;
grant execute on function api.qry_identity_get_visible_users(uuid[]) to authenticated;
grant execute on function api.qry_system_find_member_candidate_by_email(text) to authenticated;
grant execute on function api.qry_review_find_member_candidate_by_email(text) to authenticated;
grant execute on function api.cmd_lifecycle_model_bundle_save(jsonb) to authenticated;
grant execute on function api.cmd_lifecycle_model_bundle_delete(uuid, text) to authenticated;

revoke execute on function api.svc_dataset_review_submit_job_claim(integer, integer) from public, anon, authenticated;
revoke execute on function api.svc_dataset_review_submit_job_record_result(uuid, text, uuid, jsonb, text, text, jsonb, jsonb) from public, anon, authenticated;
revoke execute on function api.svc_review_submit_from_job(uuid, jsonb) from public, anon, authenticated;
revoke execute on function api.svc_worker_enqueue_job(text, jsonb, text, text, uuid, text, uuid, text, uuid, text, text, text, integer, text, timestamptz, text, integer, timestamptz, jsonb, uuid, uuid) from public, anon, authenticated;
revoke execute on function api.svc_worker_read_job(uuid, boolean) from public, anon, authenticated;
revoke execute on function api.svc_worker_list_jobs(uuid, text, uuid, text[], text, integer, boolean) from public, anon, authenticated;
revoke execute on function api.svc_worker_cancel_job(uuid, uuid, text) from public, anon, authenticated;
revoke execute on function api.svc_lca_read_job_projection(uuid, uuid, uuid, boolean) from public, anon, authenticated;
revoke execute on function api.svc_lca_read_result_projection(uuid, uuid, text, boolean) from public, anon, authenticated;
revoke execute on function api.svc_lca_read_latest_single_solve_result(uuid, uuid, integer) from public, anon, authenticated;
revoke execute on function api.svc_identity_event_claim(text, text) from public, anon, authenticated;
revoke execute on function api.svc_identity_event_release(text) from public, anon, authenticated;
revoke execute on function api.svc_identity_desired_state_upsert(text, text, text, text, jsonb) from public, anon, authenticated;
revoke execute on function api.svc_identity_desired_state_read(text) from public, anon, authenticated;
revoke execute on function api.svc_identity_login_bind(text, uuid) from public, anon, authenticated;
revoke execute on function api.svc_identity_managed_role_materialize(text, uuid) from public, anon, authenticated;
revoke execute on function api.svc_schema_contract_status() from public, anon, authenticated;

grant execute on function api.svc_dataset_review_submit_job_claim(integer, integer) to service_role;
grant execute on function api.svc_dataset_review_submit_job_record_result(uuid, text, uuid, jsonb, text, text, jsonb, jsonb) to service_role;
grant execute on function api.svc_review_submit_from_job(uuid, jsonb) to service_role;
grant execute on function api.svc_worker_enqueue_job(text, jsonb, text, text, uuid, text, uuid, text, uuid, text, text, text, integer, text, timestamptz, text, integer, timestamptz, jsonb, uuid, uuid) to service_role;
grant execute on function api.svc_worker_read_job(uuid, boolean) to service_role;
grant execute on function api.svc_worker_list_jobs(uuid, text, uuid, text[], text, integer, boolean) to service_role;
grant execute on function api.svc_worker_cancel_job(uuid, uuid, text) to service_role;
grant execute on function api.svc_lca_read_job_projection(uuid, uuid, uuid, boolean) to service_role;
grant execute on function api.svc_lca_read_result_projection(uuid, uuid, text, boolean) to service_role;
grant execute on function api.svc_lca_read_latest_single_solve_result(uuid, uuid, integer) to service_role;
grant execute on function api.svc_identity_event_claim(text, text) to service_role;
grant execute on function api.svc_identity_event_release(text) to service_role;
grant execute on function api.svc_identity_desired_state_upsert(text, text, text, text, jsonb) to service_role;
grant execute on function api.svc_identity_desired_state_read(text) to service_role;
grant execute on function api.svc_identity_login_bind(text, uuid) to service_role;
grant execute on function api.svc_identity_managed_role_materialize(text, uuid) to service_role;
grant execute on function api.svc_schema_contract_status() to service_role;

-- Durable exact grant manifest.  Function names are admitted only when they
-- correspond to a frozen consumer capability; every overload present at this
-- migration head is recorded by its exact regprocedure identity.  Future
-- routines inherit no Data API EXECUTE privilege and therefore require an
-- explicit migration plus capability mapping.
create table if not exists private.api_capability_grants (
  routine_identity text primary key,
  capability_id text not null,
  allow_anon boolean not null default false,
  allow_authenticated boolean not null default false,
  allow_service_role boolean not null default false,
  constraint api_capability_grants_has_role_check check (
    allow_anon or allow_authenticated or allow_service_role
  )
);
revoke all on table private.api_capability_grants from public, anon, authenticated, service_role;

insert into private.api_capability_grants (
  routine_identity, capability_id, allow_anon, allow_authenticated, allow_service_role
)
with admitted_name(name, capability_id, allow_anon, allow_authenticated, allow_service_role) as (
  values
    ('get_latest_contact_versions', 'NX-CORE-02', true, true, false),
    ('get_latest_flow_versions', 'NX-CORE-02', true, true, false),
    ('get_latest_flowproperty_versions', 'NX-CORE-02', true, true, false),
    ('get_latest_lifecyclemodel_versions', 'NX-CORE-02', true, true, false),
    ('get_latest_process_versions', 'NX-CORE-02', true, true, false),
    ('get_latest_source_versions', 'NX-CORE-02', true, true, false),
    ('get_latest_unitgroup_versions', 'NX-CORE-02', true, true, false),
    ('search_contacts_latest', 'NX-CORE-02', true, true, false),
    ('search_dataset_json_uuid_mentions', 'NX-CORE-02', true, true, false),
    ('search_flowproperties_latest', 'NX-CORE-02', true, true, false),
    ('search_flows_latest', 'NX-CORE-02', true, true, false),
    ('search_lifecyclemodels_latest', 'NX-CORE-02', true, true, false),
    ('search_processes_latest_v2', 'NX-CORE-02', true, true, false),
    ('search_sources_latest', 'NX-CORE-02', true, true, false),
    ('search_unitgroups_latest', 'NX-CORE-02', true, true, false),
    ('hybrid_search_contacts_v2', 'NX-CORE-02', true, true, false),
    ('hybrid_search_flowproperties_v2', 'NX-CORE-02', true, true, false),
    ('hybrid_search_flows_v2', 'NX-CORE-02', true, true, false),
    ('hybrid_search_lifecyclemodels_v2', 'NX-CORE-02', true, true, false),
    ('hybrid_search_processes_v2', 'NX-CORE-02', true, true, false),
    ('hybrid_search_sources_v2', 'NX-CORE-02', true, true, false),
    ('hybrid_search_unitgroups_v2', 'NX-CORE-02', true, true, false),
    ('qry_notification_get_my_data_count', 'NX-NOTIF-01', false, true, false),
    ('qry_notification_get_my_data_items', 'NX-NOTIF-01', false, true, false),
    ('qry_notification_get_my_issue_count', 'NX-NOTIF-01', false, true, false),
    ('qry_notification_get_my_issue_items', 'NX-NOTIF-01', false, true, false),
    ('qry_notification_get_my_team_count', 'NX-NOTIF-01', false, true, false),
    ('qry_notification_get_my_team_items', 'NX-NOTIF-01', false, true, false),
    ('qry_review_get_admin_root_queue_items_v2', 'NX-REV-01', false, true, false),
    ('qry_review_get_comment_items', 'NX-REV-01', false, true, false),
    ('qry_review_get_items', 'NX-REV-01', false, true, false),
    ('qry_review_get_member_list', 'NX-REV-01', false, true, false),
    ('qry_review_get_member_queue_items', 'NX-REV-01', false, true, false),
    ('qry_review_get_member_root_queue_items_v2', 'NX-REV-01', false, true, false),
    ('qry_review_get_member_workload', 'NX-REV-01', false, true, false),
    ('qry_root_review_reference_progress_v2', 'NX-REV-01', false, true, false),
    ('qry_system_get_member_list', 'NX-MEM-01', false, true, false),
    ('qry_team_find_invitable_user_by_email', 'NX-MEM-01', false, true, false),
    ('qry_team_get_member_list', 'NX-MEM-01', false, true, false),
    ('cmd_dataset_alias_execution_admit_guarded', 'CLI-ALIAS-01', false, true, false),
    ('cmd_dataset_alias_execution_gate_guarded', 'CLI-ALIAS-01', false, true, false),
    ('cmd_dataset_alias_execution_preflight_guarded', 'CLI-ALIAS-01', false, true, false),
    ('cmd_dataset_alias_execution_read', 'CLI-ALIAS-01', false, true, false),
    ('cmd_dataset_assign_team', 'CLI-RPC-01', false, true, false),
    ('cmd_dataset_create', 'CLI-RPC-01', false, true, false),
    ('cmd_dataset_create_version', 'CLI-RPC-01', false, true, false),
    ('cmd_dataset_delete', 'CLI-RPC-01', false, true, false),
    ('cmd_dataset_derivative_rebuild_plan_guarded', 'CLI-RPC-01', false, true, false),
    ('cmd_dataset_derivative_rebuild_read', 'CLI-RPC-01', false, true, false),
    ('cmd_dataset_derivative_rebuild_snapshot', 'CLI-RPC-01', false, true, false),
    ('cmd_dataset_extraction_ack', 'EDGE-EXTRACT-01', false, false, true),
    ('cmd_dataset_extraction_claim', 'EDGE-EXTRACT-01', false, false, true),
    ('cmd_dataset_extraction_record_failure', 'EDGE-EXTRACT-01', false, false, true),
    ('cmd_dataset_flow_identity_capture_attest_guarded', 'CLI-RPC-01', false, true, false),
    ('cmd_dataset_flow_identity_process_rewrite_guarded', 'CLI-RPC-01', false, true, false),
    ('cmd_dataset_flow_identity_scope_finalize_guarded', 'CLI-RPC-01', false, true, false),
    ('cmd_dataset_flow_identity_scope_lookup', 'CLI-RPC-01', false, true, false),
    ('cmd_dataset_flow_identity_scope_preflight_guarded', 'CLI-RPC-01', false, true, false),
    ('cmd_dataset_flow_identity_scope_read', 'CLI-RPC-01', false, true, false),
    ('cmd_dataset_flow_identity_scope_recover_guarded', 'CLI-RPC-01', false, true, false),
    ('cmd_dataset_publish', 'CLI-RPC-01', false, true, false),
    ('cmd_dataset_review_submit_gate', 'EDGE-ACTOR-01', false, true, false),
    ('cmd_dataset_review_submit_job_enqueue', 'EDGE-ACTOR-01', false, true, false),
    ('cmd_dataset_review_submit_job_read', 'EDGE-ACTOR-01', false, true, false),
    ('cmd_dataset_review_submit_job_read_latest', 'EDGE-ACTOR-01', false, true, false),
    ('cmd_dataset_save_draft', 'CLI-RPC-01', false, true, false),
    ('cmd_dataset_withdraw', 'CLI-RPC-01', false, true, false),
    ('cmd_lca_release_approve', 'EDGE-REL-01', false, true, false),
    ('cmd_lca_release_artifacts_finalize_service', 'EDGE-REL-01', false, false, true),
    ('cmd_lca_release_prepare', 'EDGE-REL-01', false, true, false),
    ('cmd_lca_release_publish', 'EDGE-REL-01', false, true, false),
    ('cmd_lca_release_readback_verify', 'EDGE-REL-01', false, true, false),
    ('cmd_lca_release_unpublish', 'EDGE-REL-01', false, true, false),
    ('cmd_lcia_result_build_request', 'EDGE-ACTOR-01', false, true, false),
    ('cmd_lcia_result_build_request_v2', 'EDGE-ACTOR-01', false, true, false),
    ('cmd_lcia_result_package_publish', 'EDGE-ACTOR-01', false, true, false),
    ('cmd_lcia_result_publication_unpublish', 'EDGE-ACTOR-01', false, true, false),
    ('cmd_lcia_scope_closure_check_request_v2', 'EDGE-ACTOR-01', false, true, false),
    ('cmd_notification_send_validation_issue', 'NX-NOTIF-01', false, true, false),
    ('cmd_review_assign_reviewers', 'NX-CMD-01', false, true, false),
    ('cmd_review_change_member_role', 'NX-CMD-01', false, true, false),
    ('cmd_review_finalize_approve', 'NX-CMD-01', false, true, false),
    ('cmd_review_finalize_reject', 'NX-CMD-01', false, true, false),
    ('cmd_review_revoke_reviewer', 'NX-CMD-01', false, true, false),
    ('cmd_review_save_assignment_draft', 'NX-CMD-01', false, true, false),
    ('cmd_review_save_comment_draft', 'NX-CMD-01', false, true, false),
    ('cmd_review_submit_comment', 'NX-CMD-01', false, true, false),
    ('cmd_review_submit_v2', 'NX-CMD-01', false, true, false),
    ('cmd_simple_review_submit_decision', 'NX-CMD-01', false, true, false),
    ('cmd_system_change_member_role', 'NX-CMD-01', false, true, false),
    ('cmd_team_accept_invitation', 'NX-CMD-01', false, true, false),
    ('cmd_team_change_member_role', 'NX-CMD-01', false, true, false),
    ('cmd_team_create', 'NX-CMD-01', false, true, false),
    ('cmd_team_reinvite_member', 'NX-CMD-01', false, true, false),
    ('cmd_team_reject_invitation', 'NX-CMD-01', false, true, false),
    ('cmd_team_set_rank', 'NX-CMD-01', false, true, false),
    ('cmd_team_update_profile', 'NX-CMD-01', false, true, false),
    ('cmd_user_update_contact', 'NX-CMD-01', false, true, false),
    ('get_current_lca_release', 'EDGE-REL-01', true, true, false),
    ('get_current_lca_release_process', 'EDGE-REL-01', true, true, false),
    ('get_lca_release_artifact_download', 'EDGE-REL-01', false, true, false),
    ('get_lca_release_run', 'EDGE-REL-01', false, true, false),
    ('get_lcia_result_calculation_bundle', 'EDGE-ACTOR-01', false, true, false),
    ('get_lcia_result_package_preview', 'EDGE-ACTOR-01', false, true, false),
    ('get_lcia_scope_closure_check', 'EDGE-ACTOR-01', false, true, false),
    ('get_lcia_scope_closure_report_download', 'EDGE-ACTOR-01', false, true, false),
    ('get_published_lcia_result_package', 'EDGE-REL-01', true, true, false),
    ('get_task_summary_v2_feed', 'EDGE-ACTOR-01', false, true, false),
    ('list_lcia_scope_closure_issues', 'EDGE-ACTOR-01', false, true, false),
    ('qry_membership_get_mine', 'NX-MEM-02', false, true, false),
    ('qry_team_list', 'NX-TEAM-01', false, true, false),
    ('qry_team_get', 'NX-TEAM-01', false, true, false),
    ('qry_identity_get_mine', 'NX-ID-01', false, true, false),
    ('qry_identity_get_visible_users', 'NX-ID-02', false, true, false),
    ('qry_system_find_member_candidate_by_email', 'NX-MEM-01', false, true, false),
    ('qry_review_find_member_candidate_by_email', 'NX-MEM-01', false, true, false),
    ('cmd_lifecycle_model_bundle_save', 'EDGE-BUNDLE-01', false, true, false),
    ('cmd_lifecycle_model_bundle_delete', 'EDGE-BUNDLE-01', false, true, false)
), admitted as (
  select
    format(
      '%I.%I(%s)', namespace.nspname, routine.proname,
      pg_catalog.oidvectortypes(routine.proargtypes)
    ) as routine_identity,
    admitted_name.capability_id,
    admitted_name.allow_anon,
    admitted_name.allow_authenticated,
    admitted_name.allow_service_role
  from admitted_name
  join pg_catalog.pg_proc as routine on routine.proname = admitted_name.name
  join pg_catalog.pg_namespace as namespace on namespace.oid = routine.pronamespace
  where namespace.nspname = 'api'
)
select * from admitted
on conflict (routine_identity) do update set
  capability_id = excluded.capability_id,
  allow_anon = excluded.allow_anon,
  allow_authenticated = excluded.allow_authenticated,
  allow_service_role = excluded.allow_service_role;

insert into private.api_capability_grants (
  routine_identity, capability_id, allow_anon, allow_authenticated, allow_service_role
)
select
  format(
    '%I.%I(%s)', namespace.nspname, routine.proname,
    pg_catalog.oidvectortypes(routine.proargtypes)
  ),
  case
    when routine.proname like 'svc_identity_%' then 'EDGE-ID-01'
    when routine.proname like 'svc_dataset_review_%' or routine.proname like 'svc_review_%' then 'EDGE-REVIEW-01'
    when routine.proname like 'svc_worker_%' then 'EDGE-WJOB-01'
    when routine.proname like 'svc_tidas_%' then 'EDGE-PKG-01'
    when routine.proname like 'svc_lca_release_%' or routine.proname like 'svc_lcia_%' then 'EDGE-REL-01'
    when routine.proname like 'svc_lca_%' then 'EDGE-LCA-01'
    when routine.proname = 'svc_schema_contract_status' then 'DB-VERIFY-01'
    else 'EDGE-SERVICE-LEGACY-01'
  end,
  false,
  false,
  true
from pg_catalog.pg_proc as routine
join pg_catalog.pg_namespace as namespace on namespace.oid = routine.pronamespace
where namespace.nspname = 'api'
  and routine.proname like 'svc_%'
on conflict (routine_identity) do update set
  capability_id = excluded.capability_id,
  allow_anon = excluded.allow_anon,
  allow_authenticated = excluded.allow_authenticated,
  allow_service_role = excluded.allow_service_role;

do $apply_api_capability_grants$
declare
  grant_row record;
begin
  for grant_row in select * from private.api_capability_grants order by routine_identity
  loop
    if to_regprocedure(grant_row.routine_identity) is null then
      raise exception 'capability grant routine is missing: %', grant_row.routine_identity;
    end if;
    if (
      select routine.proowner
      from pg_catalog.pg_proc as routine
      where routine.oid = to_regprocedure(grant_row.routine_identity)
    ) = 'api_internal_executor'::regrole then
      continue;
    end if;
    if grant_row.allow_anon then
      execute format('grant execute on function %s to anon', grant_row.routine_identity);
    end if;
    if grant_row.allow_authenticated then
      execute format('grant execute on function %s to authenticated', grant_row.routine_identity);
    end if;
    if grant_row.allow_service_role then
      execute format('grant execute on function %s to service_role', grant_row.routine_identity);
    end if;
  end loop;
end
$apply_api_capability_grants$;

-- The constrained search facades deliberately have a non-login owner. Apply
-- their grants as that owner, then remove the temporary role membership.
grant usage on schema private to api_internal_executor;
grant select on table private.api_capability_grants to api_internal_executor;
set local role api_internal_executor;
do $apply_executor_capability_grants$
declare
  grant_row record;
begin
  for grant_row in
    select manifest.*
    from private.api_capability_grants as manifest
    join pg_catalog.pg_proc as routine
      on routine.oid = to_regprocedure(manifest.routine_identity)
    where routine.proowner = current_user::regrole
    order by manifest.routine_identity
  loop
    if grant_row.allow_anon then
      execute format('grant execute on function %s to anon', grant_row.routine_identity);
    end if;
    if grant_row.allow_authenticated then
      execute format('grant execute on function %s to authenticated', grant_row.routine_identity);
    end if;
    if grant_row.allow_service_role then
      execute format('grant execute on function %s to service_role', grant_row.routine_identity);
    end if;
  end loop;
end
$apply_executor_capability_grants$;
reset role;
revoke select on table private.api_capability_grants from api_internal_executor;
revoke usage on schema private from api_internal_executor;
revoke create on schema api from api_internal_executor;
revoke api_internal_executor from postgres;

notify pgrst, 'reload schema';

commit;
