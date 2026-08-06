CREATE OR REPLACE FUNCTION "api"."cmd_dataset_alias_execution_admit_guarded"("p_request" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "lock_timeout" TO '5s'
    SET "statement_timeout" TO '10s'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_schema_version constant text := 'dataset-alias-execution-admit.v1';
  v_request_id uuid;
  v_preflight util.dataset_alias_execution_preflights%rowtype;
  v_token text;
  v_gate_results jsonb;
  v_gate jsonb;
  v_gate_receipt util.dataset_alias_execution_gate_receipts%rowtype;
  v_gate_name text;
  v_expectation_name text;
  v_captured_at timestamp with time zone;
  v_now timestamp with time zone;
  v_gate_results_sha256 text;
  v_admission_request_sha256 text;
  v_nonce text;
  v_nonce_sha256 text;
  v_service_key text;
  v_net_request_id bigint;
  v_dispatch_error jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_request is not null and pg_column_size(p_request) > 65536 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_ADMISSION_TOO_LARGE',
      'status', 413,
      'message', 'Protected admission request exceeds 64 KiB'
    );
  end if;

  if jsonb_typeof(p_request) is distinct from 'object'
    or not (p_request ?& array[
      'schema_version',
      'request_id',
      'preflight_token',
      'preflight_proof_sha256',
      'gate_results'
    ])
    or exists (
      select 1
      from jsonb_object_keys(p_request) as request_key(key)
      where request_key.key <> all (array[
        'schema_version',
        'request_id',
        'preflight_token',
        'preflight_proof_sha256',
        'gate_results'
      ])
    )
    or p_request->>'schema_version' is distinct from v_schema_version
    or jsonb_typeof(p_request->'request_id') is distinct from 'string'
    or (p_request->>'request_id') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or jsonb_typeof(p_request->'preflight_token') is distinct from 'string'
    or (p_request->>'preflight_token') !~ '^[a-f0-9]{64}$'
    or jsonb_typeof(p_request->'preflight_proof_sha256') is distinct from 'string'
    or (p_request->>'preflight_proof_sha256') !~ '^[a-f0-9]{64}$'
    or jsonb_typeof(p_request->'gate_results') is distinct from 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_ADMISSION_INVALID_REQUEST',
      'status', 400,
      'message', 'Admission request must match dataset-alias-execution-admit.v1 exactly'
    );
  end if;

  v_request_id := (p_request->>'request_id')::uuid;
  v_token := p_request->>'preflight_token';
  v_gate_results := p_request->'gate_results';

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(v_actor::text || ':' || v_request_id::text, 0)
  );

  select preflight.*
  into v_preflight
  from util.dataset_alias_execution_preflights as preflight
  where preflight.id = v_request_id
    and preflight.actor_user_id = v_actor
  for update;

  if v_preflight.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_NOT_FOUND',
      'status', 404,
      'message', 'No actor-owned protected preflight exists for this request ID'
    );
  end if;

  v_now := pg_catalog.clock_timestamp();

  if v_preflight.consumed_at is not null
    or exists (
      select 1
      from util.dataset_alias_execution_requests as request
      where request.id = v_request_id
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_ATTEMPT_ALREADY_CONSUMED',
      'status', 409,
      'message', 'The protected attempt was already consumed; use status/readback only'
    );
  end if;

  if v_now > v_preflight.expires_at then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_EXPIRED',
      'status', 409,
      'message', 'The 180-second server preflight window expired before admission'
    );
  end if;

  if util.dataset_alias_execution_sha256(v_token)
      is distinct from v_preflight.token_sha256
    or p_request->>'preflight_proof_sha256'
      is distinct from v_preflight.preflight_proof_sha256 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_PROOF_MISMATCH',
      'status', 409,
      'message', 'Preflight token or proof does not match the durable server record'
    );
  end if;

  if not (v_gate_results ?& array[
      'primary_support_plan',
      'execution_unused',
      'derivative_quiescence'
    ])
    or exists (
      select 1
      from jsonb_object_keys(v_gate_results) as gate_key(key)
      where gate_key.key <> all (array[
        'primary_support_plan',
        'execution_unused',
        'derivative_quiescence'
      ])
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_GATE_SET_MISMATCH',
      'status', 400,
      'message', 'Exactly three protected gate results are required'
    );
  end if;

  for v_gate_name, v_expectation_name in
    select *
    from (values
      ('primary_support_plan'::text, 'primary_support_plan_sha256'::text),
      ('execution_unused'::text, 'execution_unused_sha256'::text),
      ('derivative_quiescence'::text, 'derivative_quiescence_sha256'::text)
    ) as gate_map(gate_name, expectation_name)
  loop
    v_gate := v_gate_results->v_gate_name;
    v_gate_receipt := null;

    select receipt.*
    into v_gate_receipt
    from util.dataset_alias_execution_gate_receipts as receipt
    where receipt.preflight_id = v_request_id
      and receipt.actor_user_id = v_actor
      and receipt.gate_name = v_gate_name;

    if jsonb_typeof(v_gate) is distinct from 'object'
      or not (v_gate ?& array[
        'expected_sha256',
        'observed_sha256',
        'status',
        'captured_at'
      ])
      or exists (
        select 1
        from jsonb_object_keys(v_gate) as gate_field(key)
        where gate_field.key <> all (array[
          'expected_sha256',
          'observed_sha256',
          'status',
          'captured_at'
        ])
      )
      or v_gate_receipt.preflight_id is null
      or v_gate->>'expected_sha256'
        is distinct from v_gate_receipt.expected_sha256
      or v_gate->>'observed_sha256'
        is distinct from v_gate_receipt.observed_sha256
      or v_gate->>'status' is distinct from v_gate_receipt.status
      or v_gate_receipt.expected_sha256
        is distinct from v_preflight.gate_expectations->>v_expectation_name
      or jsonb_typeof(v_gate->'captured_at') is distinct from 'string' then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_EXECUTION_GATE_FAILED',
        'status', 409,
        'message', 'A protected gate is missing, failed, or bound to the wrong digest',
        'gate', v_gate_name
      );
    end if;

    begin
      v_captured_at := (v_gate->>'captured_at')::timestamp with time zone;
    exception
      when others then
        return jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_EXECUTION_GATE_TIMESTAMP_INVALID',
          'status', 400,
          'message', 'A protected gate timestamp is invalid',
          'gate', v_gate_name
        );
    end;

    if v_captured_at is distinct from v_gate_receipt.captured_at
      or v_captured_at < v_preflight.completed_at
      or v_captured_at > v_now + interval '5 seconds'
      or v_captured_at > v_preflight.expires_at then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_EXECUTION_GATE_OUTSIDE_WINDOW',
        'status', 409,
        'message', 'All gate evidence must be captured inside the server preflight window',
        'gate', v_gate_name
      );
    end if;
  end loop;

  if pg_catalog.clock_timestamp() > v_preflight.expires_at then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_PREFLIGHT_EXPIRED',
      'status', 409,
      'message', 'The 180-second server preflight window expired during admission validation'
    );
  end if;

  v_gate_results_sha256 :=
    util.dataset_alias_execution_sha256(v_gate_results::text);
  v_admission_request_sha256 :=
    util.dataset_alias_execution_sha256(p_request::text);
  v_nonce := pg_catalog.encode(extensions.gen_random_bytes(32), 'hex');
  v_nonce_sha256 := util.dataset_alias_execution_sha256(v_nonce);

  insert into util.dataset_alias_execution_requests (
    id,
    actor_user_id,
    plan_sha256,
    operation_id,
    plan_request_sha256,
    freeze_sha256,
    approval_identity_sha256,
    approval_text_sha256,
    derivative_target_set_sha256,
    preflight_proof_sha256,
    admission_request_sha256,
    gate_results,
    gate_results_sha256,
    nonce_sha256,
    attempt_count,
    dispatch_count,
    status,
    admitted_at
  ) values (
    v_request_id,
    v_actor,
    v_preflight.plan_sha256,
    v_preflight.operation_id,
    v_preflight.plan_request_sha256,
    v_preflight.bindings->>'freeze_sha256',
    v_preflight.bindings->>'approval_identity_sha256',
    v_preflight.bindings->>'approval_text_sha256',
    v_preflight.bindings->>'derivative_target_set_sha256',
    v_preflight.preflight_proof_sha256,
    v_admission_request_sha256,
    v_gate_results,
    v_gate_results_sha256,
    v_nonce_sha256,
    1,
    0,
    'dispatching',
    v_now
  );

  update util.dataset_alias_execution_preflights
  set consumed_at = v_now
  where id = v_request_id;

  begin
    v_service_key := util.project_secret_key();
    v_net_request_id := net.http_post(
      url => util.project_url()
        || '/rest/v1/rpc/cmd_dataset_alias_execution_execute',
      headers => jsonb_build_object(
        'Content-Type', 'application/json',
        'Authorization', 'Bearer ' || v_service_key,
        'apikey', v_service_key
      ),
      body => jsonb_build_object(
        'p_request_id', v_request_id,
        'p_nonce', v_nonce
      ),
      timeout_milliseconds => 70000
    );

    if v_net_request_id is null then
      raise exception using
        errcode = 'P0001',
        message = 'pg_net returned no request ID';
    end if;

    update util.dataset_alias_execution_requests
    set
      dispatch_count = 1,
      net_request_id = v_net_request_id,
      status = 'dispatched',
      dispatched_at = pg_catalog.clock_timestamp(),
      updated_at = pg_catalog.clock_timestamp()
    where id = v_request_id;
  exception
    when others then
      v_dispatch_error := jsonb_build_object(
        'phase', 'dispatch',
        'code', 'ALIAS_EXECUTION_DISPATCH_FAILED',
        'sqlstate', sqlstate,
        'message', sqlerrm
      );

      update util.dataset_alias_execution_requests
      set
        status = 'failed',
        terminal_at = pg_catalog.clock_timestamp(),
        last_error = v_dispatch_error,
        updated_at = pg_catalog.clock_timestamp()
      where id = v_request_id;
  end;

  if v_dispatch_error is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_DISPATCH_FAILED',
      'status', 'failed',
      'request_id', v_request_id,
      'attempt_count', 1,
      'dispatch_count', 0,
      'attempt_consumed', true,
      'retry_allowed', false,
      'preflight_proof_sha256', v_preflight.preflight_proof_sha256,
      'admission_request_sha256', v_admission_request_sha256,
      'gate_results_sha256', v_gate_results_sha256
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'command', 'cmd_dataset_alias_execution_admit_guarded',
    'schema_version', v_schema_version,
    'request_id', v_request_id,
    'plan_sha256', v_preflight.plan_sha256,
    'operation_id', v_preflight.operation_id,
    'plan_request_sha256', v_preflight.plan_request_sha256,
    'preflight_proof_sha256', v_preflight.preflight_proof_sha256,
    'admission_request_sha256', v_admission_request_sha256,
    'gate_results_sha256', v_gate_results_sha256,
    'attempt_count', 1,
    'dispatch_count', 1,
    'net_request_id', v_net_request_id::text,
    'status', 'dispatched',
    'attempt_consumed', true,
    'retry_allowed', false
  );
exception
  when lock_not_available then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_ADMISSION_LOCK_BUSY',
      'status', 409,
      'message', 'Protected admission could not acquire its bounded lock'
    );
  when unique_violation then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_EXECUTION_ATTEMPT_ALREADY_CONSUMED',
      'status', 409,
      'message', 'The request or sealed approval identity already consumed its only attempt'
    );
end;
$_$;

ALTER FUNCTION "api"."cmd_dataset_alias_execution_admit_guarded"("p_request" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_dataset_alias_execution_admit_guarded"("p_request" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_dataset_alias_execution_admit_guarded"("p_request" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_dataset_alias_execution_admit_guarded"("p_request" "jsonb") TO "authenticated";
