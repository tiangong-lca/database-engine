CREATE OR REPLACE FUNCTION "api"."svc_oauth_client_configure"("p_client_id" "text", "p_client_kind" "text", "p_enabled" boolean, "p_capability_ids" "text"[] DEFAULT ARRAY[]::"text"[]) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
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
$_$;

ALTER FUNCTION "api"."svc_oauth_client_configure"("p_client_id" "text", "p_client_kind" "text", "p_enabled" boolean, "p_capability_ids" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_oauth_client_configure"("p_client_id" "text", "p_client_kind" "text", "p_enabled" boolean, "p_capability_ids" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_oauth_client_configure"("p_client_id" "text", "p_client_kind" "text", "p_enabled" boolean, "p_capability_ids" "text"[]) TO "service_role";
