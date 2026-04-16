CREATE OR REPLACE FUNCTION "public"."cmd_dataset_create"("p_table" "text", "p_id" "uuid", "p_json_ordered" "jsonb", "p_model_id" "uuid" DEFAULT NULL::"uuid", "p_rule_verification" boolean DEFAULT NULL::boolean, "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_created_row jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table = 'lifecyclemodels' then
    return jsonb_build_object(
      'ok', false,
      'code', 'LIFECYCLEMODEL_BUNDLE_REQUIRED',
      'status', 400,
      'message', 'Lifecycle models must use bundle create and delete commands'
    );
  end if;

  if p_table not in (
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Unsupported dataset table'
    );
  end if;

  if p_json_ordered is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'JSON_ORDERED_REQUIRED',
      'status', 400,
      'message', 'jsonOrdered is required'
    );
  end if;

  if p_table <> 'processes' and p_model_id is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'MODEL_ID_NOT_ALLOWED',
      'status', 400,
      'message', 'modelId is only allowed for process dataset creation'
    );
  end if;

  begin
    if p_table = 'processes' then
      execute format(
        'insert into public.%I as t (id, json_ordered, model_id, rule_verification)
         values ($1, $2::json, $3, $4)
         returning to_jsonb(t)',
        p_table
      )
        into v_created_row
        using p_id, p_json_ordered, p_model_id, p_rule_verification;
    else
      execute format(
        'insert into public.%I as t (id, json_ordered, rule_verification)
         values ($1, $2::json, $3)
         returning to_jsonb(t)',
        p_table
      )
        into v_created_row
        using p_id, p_json_ordered, p_rule_verification;
    end if;
  exception
    when unique_violation then
      return jsonb_build_object(
        'ok', false,
        'code', '23505',
        'status', 409,
        'message', 'Dataset with the same id and version already exists'
      );
    when not_null_violation then
      return jsonb_build_object(
        'ok', false,
        'code', '23502',
        'status', 400,
        'message', 'Dataset creation requires a valid id, version, and jsonOrdered payload'
      );
    when check_violation then
      return jsonb_build_object(
        'ok', false,
        'code', sqlstate,
        'status', 400,
        'message', sqlerrm
      );
  end;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_create',
    v_actor,
    p_table,
    p_id,
    nullif(v_created_row->>'version', ''),
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_created_row
  );
end;
$_$;

ALTER FUNCTION "public"."cmd_dataset_create"("p_table" "text", "p_id" "uuid", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_create"("p_table" "text", "p_id" "uuid", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_create"("p_table" "text", "p_id" "uuid", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_dataset_create"("p_table" "text", "p_id" "uuid", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_dataset_create"("p_table" "text", "p_id" "uuid", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb") TO "service_role";
