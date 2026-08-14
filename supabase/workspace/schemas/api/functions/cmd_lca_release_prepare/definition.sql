CREATE OR REPLACE FUNCTION "api"."cmd_lca_release_prepare"("p_release_run_id" "uuid", "p_release_version" "text", "p_selection_manifest_hash" "text", "p_input_manifest_hash" "text", "p_calculation_bundle_ref" "jsonb", "p_calculation_bundle_hash" "text", "p_profile_lock_hash" "text", "p_publish_plan" "jsonb", "p_publish_plan_hash" "text", "p_idempotency_key" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_existing private.lca_release_runs%rowtype;
  v_request_hash text;
  v_artifact_set_hash text;
  v_pair_count integer;
begin
  if v_actor is null then
    return private.lca_release_error('auth_required', 401, 'Authentication required');
  end if;
  if not private.lca_release_is_manager() then
    return private.lca_release_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;
  if p_release_run_id is null then
    return private.lca_release_error('release_run_id_required', 400, 'releaseRunId is required');
  end if;
  if coalesce(p_release_version, '') !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$' then
    return private.lca_release_error('invalid_release_version', 400, 'releaseVersion must use NN.NN.NNN');
  end if;
  if coalesce(p_selection_manifest_hash, '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_input_manifest_hash, '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_calculation_bundle_hash, '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_profile_lock_hash, '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_publish_plan_hash, '') !~ '^[0-9a-f]{64}$' then
    return private.lca_release_error('invalid_hash', 400, 'Release hashes must be lowercase SHA-256 values');
  end if;
  if jsonb_typeof(coalesce(p_calculation_bundle_ref, 'null'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(p_publish_plan, 'null'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(p_audit, 'null'::jsonb)) <> 'object' then
    return private.lca_release_error('invalid_payload', 400, 'Bundle ref, publish plan, and audit must be JSON objects');
  end if;
  if nullif(trim(coalesce(p_idempotency_key, '')), '') is null then
    return private.lca_release_error('idempotency_key_required', 400, 'idempotencyKey is required');
  end if;
  if p_publish_plan->>'schemaVersion' is distinct from 'tiangong.release.publish-plan.v1'
     or p_publish_plan->>'releaseRunId' is distinct from p_release_run_id::text
     or p_publish_plan->>'releaseVersion' is distinct from p_release_version
     or p_publish_plan->>'calculationBundleHash' is distinct from p_calculation_bundle_hash
     or p_publish_plan->>'profileLockHash' is distinct from p_profile_lock_hash
     or p_publish_plan->>'planHash' is distinct from p_publish_plan_hash then
    return private.lca_release_error('publish_plan_mismatch', 400, 'Publish plan identity or exact hashes do not match the request');
  end if;
  v_artifact_set_hash := p_publish_plan->>'artifactSetHash';
  if coalesce(v_artifact_set_hash, '') !~ '^[0-9a-f]{64}$' then
    return private.lca_release_error('invalid_artifact_set_hash', 400, 'Publish plan artifactSetHash must be SHA-256');
  end if;
  if jsonb_typeof(p_publish_plan->'datasets') is distinct from 'array'
     or jsonb_array_length(p_publish_plan->'datasets') = 0
     or jsonb_typeof(p_publish_plan->'packages') is distinct from 'array'
     or jsonb_array_length(p_publish_plan->'packages') <> 4 then
    return private.lca_release_error('publish_plan_incomplete', 400, 'Publish plan must contain datasets and exactly four packages');
  end if;

  select count(distinct (package.value->>'profileId') || ':' || (package.value->>'format'))
    into v_pair_count
  from jsonb_array_elements(p_publish_plan->'packages') as package(value)
  where package.value->>'profileId' in (
          'unit-process-full-closure.v1',
          'standalone-lifecyclemodel-result-full-closure.v1'
        )
    and package.value->>'format' in ('tidas', 'ilcd')
    and package.value->>'sha256' ~ '^[0-9a-f]{64}$';

  if v_pair_count <> 4 then
    return private.lca_release_error('publish_plan_profiles_invalid', 400, 'Publish plan must contain both profiles in TIDAS and ILCD formats');
  end if;

  v_request_hash := encode(
    extensions.digest(
      convert_to(
        concat_ws(
          '|', p_release_run_id::text, p_release_version,
          p_selection_manifest_hash, p_input_manifest_hash,
          p_calculation_bundle_hash, p_profile_lock_hash,
          p_publish_plan_hash, v_artifact_set_hash
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  );

  select * into v_existing
  from private.lca_release_runs
  where id = p_release_run_id
     or (created_by = v_actor and idempotency_key = trim(p_idempotency_key))
  order by (id = p_release_run_id) desc
  limit 1
  for update;

  if v_existing.id is not null then
    if v_existing.id = p_release_run_id
       and v_existing.created_by = v_actor
       and v_existing.request_hash = v_request_hash
       and v_existing.calculation_bundle_ref = p_calculation_bundle_ref
       and v_existing.publish_plan = p_publish_plan then
      return jsonb_build_object(
        'ok', true,
        'reused', true,
        'data', jsonb_build_object(
          'releaseRunId', v_existing.id,
          'releaseVersion', v_existing.release_version,
          'status', v_existing.status,
          'publishPlanHash', v_existing.publish_plan_hash
        )
      );
    end if;
    return private.lca_release_error('release_prepare_conflict', 409, 'Release run id or idempotency key is already bound to different content');
  end if;

  insert into private.lca_release_runs (
    id, release_version, scope_mode, selection_manifest_hash,
    input_manifest_hash, calculation_bundle_hash, calculation_bundle_ref,
    profile_lock_hash, publish_plan_hash, publish_plan, artifact_set_hash,
    status, idempotency_key, request_hash, created_by
  ) values (
    p_release_run_id, p_release_version, 'global_eligible', p_selection_manifest_hash,
    p_input_manifest_hash, p_calculation_bundle_hash, p_calculation_bundle_ref,
    p_profile_lock_hash, p_publish_plan_hash, p_publish_plan, v_artifact_set_hash,
    'prepared', trim(p_idempotency_key), v_request_hash, v_actor
  );

  insert into private.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    'cmd_lca_release_prepare', v_actor, 'lca_release_runs', p_release_run_id,
    p_release_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'publishPlanHash', p_publish_plan_hash,
      'calculationBundleHash', p_calculation_bundle_hash,
      'inputManifestHash', p_input_manifest_hash,
      'artifactSetHash', v_artifact_set_hash
    )
  );

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', jsonb_build_object(
      'releaseRunId', p_release_run_id,
      'releaseVersion', p_release_version,
      'status', 'prepared',
      'publishPlanHash', p_publish_plan_hash
    )
  );
exception
  when unique_violation then
    return private.lca_release_error('release_prepare_conflict', 409, 'Release version, run id, or idempotency key already exists');
end;
$_$;

ALTER FUNCTION "api"."cmd_lca_release_prepare"("p_release_run_id" "uuid", "p_release_version" "text", "p_selection_manifest_hash" "text", "p_input_manifest_hash" "text", "p_calculation_bundle_ref" "jsonb", "p_calculation_bundle_hash" "text", "p_profile_lock_hash" "text", "p_publish_plan" "jsonb", "p_publish_plan_hash" "text", "p_idempotency_key" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_lca_release_prepare"("p_release_run_id" "uuid", "p_release_version" "text", "p_selection_manifest_hash" "text", "p_input_manifest_hash" "text", "p_calculation_bundle_ref" "jsonb", "p_calculation_bundle_hash" "text", "p_profile_lock_hash" "text", "p_publish_plan" "jsonb", "p_publish_plan_hash" "text", "p_idempotency_key" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_lca_release_prepare"("p_release_run_id" "uuid", "p_release_version" "text", "p_selection_manifest_hash" "text", "p_input_manifest_hash" "text", "p_calculation_bundle_ref" "jsonb", "p_calculation_bundle_hash" "text", "p_profile_lock_hash" "text", "p_publish_plan" "jsonb", "p_publish_plan_hash" "text", "p_idempotency_key" "text", "p_audit" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_lca_release_prepare"("p_release_run_id" "uuid", "p_release_version" "text", "p_selection_manifest_hash" "text", "p_input_manifest_hash" "text", "p_calculation_bundle_ref" "jsonb", "p_calculation_bundle_hash" "text", "p_profile_lock_hash" "text", "p_publish_plan" "jsonb", "p_publish_plan_hash" "text", "p_idempotency_key" "text", "p_audit" "jsonb") TO "authenticated";
