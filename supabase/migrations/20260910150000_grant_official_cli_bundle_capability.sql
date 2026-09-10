begin;

-- The official CLI client is environment-specific runtime state, so identify
-- it by the exact least-privilege class deployed before EDGE-BUNDLE-01 rather
-- than hardcoding a Production client UUID. Preview/local environments without
-- that client are valid no-ops; ambiguity fails closed.
do $issue_638_official_cli_bundle_capability$
declare
  v_expected_before constant text[] := array[
    'CLI-RPC-01',
    'DB-CORE-READ-01',
    'DB-CORE-WRITE-01',
    'NX-CORE-02'
  ];
  v_expected_after constant text[] := array[
    'CLI-RPC-01',
    'DB-CORE-READ-01',
    'DB-CORE-WRITE-01',
    'EDGE-BUNDLE-01',
    'NX-CORE-02'
  ];
  v_matching_client_ids text[];
  v_client_id text;
  v_actual_capabilities text[];
  v_result jsonb;
  v_audit_id_before bigint;
begin
  select coalesce(array_agg(client.client_id order by client.client_id), array[]::text[])
  into v_matching_client_ids
  from private.oauth_client_registry as client
  where client.client_kind = 'cli'
    and client.enabled
    and coalesce((
      select array_agg(grant_row.capability_id order by grant_row.capability_id)
        filter (where grant_row.allowed)
      from private.oauth_client_capability_grants as grant_row
      where grant_row.client_id = client.client_id
    ), array[]::text[]) = v_expected_before;

  if cardinality(v_matching_client_ids) = 0 then
    return;
  end if;

  if cardinality(v_matching_client_ids) <> 1 then
    raise exception
      'Issue #638 expected at most one matching enabled CLI client, found %',
      cardinality(v_matching_client_ids);
  end if;

  v_client_id := v_matching_client_ids[1];

  perform 1
  from private.oauth_client_registry as client
  where client.client_id = v_client_id
    and client.client_kind = 'cli'
    and client.enabled
  for update;

  select coalesce(
    array_agg(grant_row.capability_id order by grant_row.capability_id)
      filter (where grant_row.allowed),
    array[]::text[]
  )
  into v_actual_capabilities
  from private.oauth_client_capability_grants as grant_row
  where grant_row.client_id = v_client_id;

  if v_actual_capabilities is distinct from v_expected_before then
    raise exception
      'Issue #638 CLI capability state changed after selection';
  end if;

  if (
    select count(*)
    from private.api_capability_grants as manifest
    where pg_catalog.to_regprocedure(manifest.routine_identity) = any (array[
      'api.cmd_lifecycle_model_bundle_save(jsonb)'::regprocedure,
      'api.cmd_lifecycle_model_bundle_delete(uuid,text)'::regprocedure
    ])
  ) <> 2 or (
    select count(*)
    from private.api_capability_grants as manifest
    where pg_catalog.to_regprocedure(manifest.routine_identity) = any (array[
      'api.cmd_lifecycle_model_bundle_save(jsonb)'::regprocedure,
      'api.cmd_lifecycle_model_bundle_delete(uuid,text)'::regprocedure
    ])
      and manifest.capability_id = 'EDGE-BUNDLE-01'
      and manifest.allow_authenticated
      and not manifest.allow_anon
      and not manifest.allow_service_role
  ) <> 2 then
    raise exception
      'Issue #638 EDGE-BUNDLE-01 route manifest is not exact';
  end if;

  select coalesce(max(audit.id), 0)
  into v_audit_id_before
  from private.oauth_client_registry_audit as audit
  where audit.client_id = v_client_id;

  select api.svc_oauth_client_configure(
    v_client_id,
    'cli',
    true,
    v_expected_after
  )
  into v_result;

  select coalesce(
    array_agg(grant_row.capability_id order by grant_row.capability_id)
      filter (where grant_row.allowed),
    array[]::text[]
  )
  into v_actual_capabilities
  from private.oauth_client_capability_grants as grant_row
  where grant_row.client_id = v_client_id;

  if v_actual_capabilities is distinct from v_expected_after
     or v_result #> '{data,capabilities}' is distinct from to_jsonb(v_expected_after) then
    raise exception
      'Issue #638 CLI capability repair produced an unexpected after-state';
  end if;

  if (
    select count(*)
    from private.oauth_client_registry_audit as audit
    where audit.client_id = v_client_id
      and audit.id > v_audit_id_before
      and audit.action = 'replace'
      and audit.before_state -> 'capabilities' = to_jsonb(v_expected_before)
      and audit.after_state -> 'capabilities' = to_jsonb(v_expected_after)
  ) <> 1 then
    raise exception
      'Issue #638 CLI capability repair did not record one exact audit event';
  end if;
end
$issue_638_official_cli_bundle_capability$;

commit;
