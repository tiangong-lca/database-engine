begin;

-- OAuth clients are public identifiers, not credentials.  Supabase Auth keeps
-- client secrets and refresh tokens; this registry owns only revocation and
-- the database capabilities a client may exercise with a user's JWT.
create table private.oauth_client_registry (
  client_id text primary key,
  client_kind text not null,
  enabled boolean not null default true,
  created_at timestamptz not null default statement_timestamp(),
  updated_at timestamptz not null default statement_timestamp(),
  disabled_at timestamptz,
  constraint oauth_client_registry_client_id_check check (
    client_id = btrim(client_id)
    and length(client_id) between 1 and 256
  ),
  constraint oauth_client_registry_client_kind_check check (
    client_kind ~ '^[a-z][a-z0-9_-]{1,63}$'
  ),
  constraint oauth_client_registry_disabled_at_check check (
    (enabled and disabled_at is null)
    or (not enabled and disabled_at is not null)
  )
);

create table private.oauth_client_capability_grants (
  client_id text not null
    references private.oauth_client_registry(client_id) on delete cascade,
  capability_id text not null,
  allowed boolean not null default true,
  granted_at timestamptz not null default statement_timestamp(),
  primary key (client_id, capability_id),
  constraint oauth_client_capability_grants_capability_check check (
    capability_id = btrim(capability_id)
    and length(capability_id) between 1 and 128
  )
);

create table private.oauth_relation_capability_grants (
  relation_schema name not null,
  relation_name name not null,
  command text not null,
  capability_id text not null,
  primary key (relation_schema, relation_name, command),
  constraint oauth_relation_capability_grants_command_check check (
    command in ('select', 'insert', 'update', 'delete')
  ),
  constraint oauth_relation_capability_grants_capability_check check (
    capability_id = btrim(capability_id)
    and length(capability_id) between 1 and 128
  )
);

create table private.oauth_client_registry_audit (
  id bigint generated always as identity primary key,
  client_id text not null,
  action text not null,
  actor_role text,
  actor_sub uuid,
  before_state jsonb,
  after_state jsonb not null,
  changed_at timestamptz not null default statement_timestamp(),
  constraint oauth_client_registry_audit_action_check check (
    action in ('create', 'replace', 'disable', 'enable')
  )
);

alter table private.oauth_client_registry enable row level security;
alter table private.oauth_client_registry force row level security;
alter table private.oauth_client_capability_grants enable row level security;
alter table private.oauth_client_capability_grants force row level security;
alter table private.oauth_relation_capability_grants enable row level security;
alter table private.oauth_relation_capability_grants force row level security;
alter table private.oauth_client_registry_audit enable row level security;
alter table private.oauth_client_registry_audit force row level security;

revoke all on table
  private.oauth_client_registry,
  private.oauth_client_capability_grants,
  private.oauth_relation_capability_grants,
  private.oauth_client_registry_audit
from public, anon, authenticated, service_role;
revoke all on sequence private.oauth_client_registry_audit_id_seq
from public, anon, authenticated, service_role;

-- Browser Data API access is intentionally read-only at this boundary.  The
-- existing table policies still decide which rows a user can see; this
-- restrictive layer additionally requires an admitted OAuth client.
insert into private.oauth_relation_capability_grants (
  relation_schema,
  relation_name,
  command,
  capability_id
)
select
  'public'::name,
  relation_name::name,
  'select',
  'DB-CORE-READ-01'
from unnest(array[
  'contacts',
  'flowproperties',
  'flows',
  'ilcd',
  'lciamethods',
  'lifecyclemodels',
  'processes',
  'sources',
  'unitgroups'
]) as admitted(relation_name);

create or replace function private.oauth_client_has_capability(
  p_capability_id text
)
returns boolean
language sql
stable
security definer
set search_path = ''
as $function$
  select case
    -- First-party Supabase sessions have no OAuth client_id and retain the
    -- existing auth.uid()-based RLS behavior.
    when nullif(pg_catalog.btrim(coalesce(auth.jwt() ->> 'client_id', '')), '') is null
      then true
    when p_capability_id is null or pg_catalog.btrim(p_capability_id) = ''
      then false
    else exists (
      select 1
      from private.oauth_client_registry as client
      join private.oauth_client_capability_grants as grant_row
        on grant_row.client_id = client.client_id
      where client.client_id = auth.jwt() ->> 'client_id'
        and client.enabled
        and grant_row.capability_id = p_capability_id
        and grant_row.allowed
    )
  end;
$function$;

revoke all on function private.oauth_client_has_capability(text)
from public, anon, authenticated, service_role;
grant execute on function private.oauth_client_has_capability(text)
to authenticated;

do $oauth_relation_guards$
declare
  grant_row record;
begin
  for grant_row in
    select *
    from private.oauth_relation_capability_grants
    where command = 'select'
    order by relation_schema, relation_name
  loop
    if not exists (
      select 1
      from pg_catalog.pg_class as relation
      join pg_catalog.pg_namespace as namespace
        on namespace.oid = relation.relnamespace
      where namespace.nspname = grant_row.relation_schema
        and relation.relname = grant_row.relation_name
        and relation.relkind in ('r', 'p')
        and relation.relrowsecurity
    ) then
      raise exception 'OAuth relation guard target is missing RLS: %.%',
        grant_row.relation_schema,
        grant_row.relation_name;
    end if;

    execute format(
      'drop policy if exists %I on %I.%I',
      'oauth_client_select_capability_guard',
      grant_row.relation_schema,
      grant_row.relation_name
    );
    execute format(
      'create policy %I on %I.%I as restrictive for select to authenticated using ((select private.oauth_client_has_capability(%L)))',
      'oauth_client_select_capability_guard',
      grant_row.relation_schema,
      grant_row.relation_name,
      grant_row.capability_id
    );
  end loop;
end
$oauth_relation_guards$;

-- PostgREST exposes overloaded routines under one /rpc/<name> route.  The
-- capability manifest must therefore map every overload of a route name to
-- exactly one capability before the pre-request hook can enforce it.
do $oauth_rpc_route_invariants$
declare
  ambiguous_route record;
begin
  select
    namespace.nspname as routine_schema,
    routine.proname as routine_name,
    array_agg(distinct manifest.capability_id order by manifest.capability_id) as capabilities
  into ambiguous_route
  from private.api_capability_grants as manifest
  join pg_catalog.pg_proc as routine
    on routine.oid = pg_catalog.to_regprocedure(manifest.routine_identity)
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = routine.pronamespace
  where namespace.nspname = 'api'
  group by namespace.nspname, routine.proname
  having count(distinct manifest.capability_id) <> 1
  limit 1;

  if found then
    raise exception 'OAuth RPC route has ambiguous capability mapping: %.% -> %',
      ambiguous_route.routine_schema,
      ambiguous_route.routine_name,
      ambiguous_route.capabilities;
  end if;
end
$oauth_rpc_route_invariants$;

create or replace function api.oauth_client_pre_request()
returns void
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_claims jsonb := coalesce(
    nullif(pg_catalog.current_setting('request.jwt.claims', true), '')::jsonb,
    '{}'::jsonb
  );
  v_client_id text := nullif(pg_catalog.btrim(coalesce(v_claims ->> 'client_id', '')), '');
  v_headers jsonb := coalesce(
    nullif(pg_catalog.current_setting('request.headers', true), '')::jsonb,
    '{}'::jsonb
  );
  v_method text := upper(coalesce(
    nullif(pg_catalog.current_setting('request.method', true), ''),
    'GET'
  ));
  v_path text := coalesce(
    nullif(pg_catalog.current_setting('request.path', true), ''),
    '/'
  );
  v_profile text;
  v_route_name text;
  v_command text;
  v_capability_id text;
  v_capability_count integer;
begin
  if v_client_id is null then
    return;
  end if;

  if v_path like '/rpc/%' then
    v_route_name := split_part(v_path, '/', 3);

    select
      min(manifest.capability_id),
      count(distinct manifest.capability_id)::integer
    into v_capability_id, v_capability_count
    from private.api_capability_grants as manifest
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(manifest.routine_identity)
    join pg_catalog.pg_namespace as namespace
      on namespace.oid = routine.pronamespace
    where namespace.nspname = 'api'
      and routine.proname = v_route_name
      and manifest.allow_authenticated;
  else
    v_profile := coalesce(
      case
        when v_method in ('GET', 'HEAD') then v_headers ->> 'accept-profile'
        else v_headers ->> 'content-profile'
      end,
      v_headers ->> 'accept-profile',
      'public'
    );
    v_route_name := ltrim(v_path, '/');
    v_command := case v_method
      when 'GET' then 'select'
      when 'HEAD' then 'select'
      when 'POST' then 'insert'
      when 'PUT' then 'update'
      when 'PATCH' then 'update'
      when 'DELETE' then 'delete'
      else null
    end;

    select relation_grant.capability_id, 1
    into v_capability_id, v_capability_count
    from private.oauth_relation_capability_grants as relation_grant
    where relation_grant.relation_schema = v_profile
      and relation_grant.relation_name = v_route_name
      and relation_grant.command = v_command;
  end if;

  if coalesce(v_capability_count, 0) <> 1
     or not private.oauth_client_has_capability(v_capability_id) then
    raise insufficient_privilege using
      message = 'OAuth client is not authorized for this API route';
  end if;
end;
$function$;

create or replace function api.svc_oauth_client_configure(
  p_client_id text,
  p_client_kind text,
  p_enabled boolean,
  p_capability_ids text[] default array[]::text[]
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_client_id text := pg_catalog.btrim(coalesce(p_client_id, ''));
  v_client_kind text := pg_catalog.btrim(coalesce(p_client_kind, ''));
  v_capability_ids text[];
  v_unknown_capabilities text[];
  v_before jsonb;
  v_after jsonb;
  v_action text;
begin
  if length(v_client_id) not between 1 and 256 then
    raise invalid_parameter_value using message = 'OAuth client_id is invalid';
  end if;
  if v_client_kind !~ '^[a-z][a-z0-9_-]{1,63}$' then
    raise invalid_parameter_value using message = 'OAuth client kind is invalid';
  end if;
  if p_enabled is null then
    raise invalid_parameter_value using message = 'OAuth client enabled state is required';
  end if;

  select coalesce(array_agg(capability_id order by capability_id), array[]::text[])
  into v_capability_ids
  from (
    select distinct pg_catalog.btrim(capability_id) as capability_id
    from unnest(coalesce(p_capability_ids, array[]::text[])) as requested(capability_id)
    where nullif(pg_catalog.btrim(capability_id), '') is not null
  ) as normalized;

  select array_agg(requested.capability_id order by requested.capability_id)
  into v_unknown_capabilities
  from unnest(v_capability_ids) as requested(capability_id)
  where not exists (
    select 1
    from private.api_capability_grants as rpc_grant
    where rpc_grant.capability_id = requested.capability_id
    union all
    select 1
    from private.oauth_relation_capability_grants as relation_grant
    where relation_grant.capability_id = requested.capability_id
  );

  if v_unknown_capabilities is not null then
    raise invalid_parameter_value using
      message = 'OAuth client capability list contains unknown values',
      detail = array_to_string(v_unknown_capabilities, ',');
  end if;

  select jsonb_build_object(
    'clientId', client.client_id,
    'clientKind', client.client_kind,
    'enabled', client.enabled,
    'capabilities', coalesce((
      select jsonb_agg(grant_row.capability_id order by grant_row.capability_id)
      from private.oauth_client_capability_grants as grant_row
      where grant_row.client_id = client.client_id
        and grant_row.allowed
    ), '[]'::jsonb)
  )
  into v_before
  from private.oauth_client_registry as client
  where client.client_id = v_client_id;

  insert into private.oauth_client_registry (
    client_id,
    client_kind,
    enabled,
    disabled_at
  ) values (
    v_client_id,
    v_client_kind,
    p_enabled,
    case when p_enabled then null else statement_timestamp() end
  )
  on conflict (client_id) do update set
    client_kind = excluded.client_kind,
    enabled = excluded.enabled,
    updated_at = statement_timestamp(),
    disabled_at = excluded.disabled_at;

  delete from private.oauth_client_capability_grants
  where client_id = v_client_id;

  insert into private.oauth_client_capability_grants (
    client_id,
    capability_id
  )
  select v_client_id, capability_id
  from unnest(v_capability_ids) as admitted(capability_id);

  select jsonb_build_object(
    'clientId', client.client_id,
    'clientKind', client.client_kind,
    'enabled', client.enabled,
    'capabilities', coalesce((
      select jsonb_agg(grant_row.capability_id order by grant_row.capability_id)
      from private.oauth_client_capability_grants as grant_row
      where grant_row.client_id = client.client_id
        and grant_row.allowed
    ), '[]'::jsonb)
  )
  into v_after
  from private.oauth_client_registry as client
  where client.client_id = v_client_id;

  v_action := case
    when v_before is null then 'create'
    when not p_enabled then 'disable'
    when coalesce((v_before ->> 'enabled')::boolean, false) then 'replace'
    else 'enable'
  end;

  insert into private.oauth_client_registry_audit (
    client_id,
    action,
    actor_role,
    actor_sub,
    before_state,
    after_state
  ) values (
    v_client_id,
    v_action,
    auth.jwt() ->> 'role',
    auth.uid(),
    v_before,
    v_after
  );

  return jsonb_build_object('ok', true, 'data', v_after);
end;
$function$;

revoke all on function api.oauth_client_pre_request()
from public, anon, authenticated, service_role;
grant execute on function api.oauth_client_pre_request()
to anon, authenticated, service_role;

revoke all on function api.svc_oauth_client_configure(text, text, boolean, text[])
from public, anon, authenticated, service_role;
grant execute on function api.svc_oauth_client_configure(text, text, boolean, text[])
to service_role;

insert into private.api_capability_grants (
  routine_identity,
  capability_id,
  allow_anon,
  allow_authenticated,
  allow_service_role
) values
  (
    'api.oauth_client_pre_request()',
    'DB-OAUTH-GATE-01',
    true,
    true,
    true
  ),
  (
    'api.svc_oauth_client_configure(text, text, boolean, text[])',
    'DB-OAUTH-ADMIN-01',
    false,
    false,
    true
  )
on conflict (routine_identity) do update set
  capability_id = excluded.capability_id,
  allow_anon = excluded.allow_anon,
  allow_authenticated = excluded.allow_authenticated,
  allow_service_role = excluded.allow_service_role;

-- PostgREST reads this database role setting on config reload.  The hook is
-- deliberately additive to existing auth.uid()-based policies and bypasses
-- first-party sessions that have no OAuth client_id claim.
alter role authenticator set pgrst.db_pre_request = 'api.oauth_client_pre_request';
notify pgrst, 'reload config';
notify pgrst, 'reload schema';

commit;
