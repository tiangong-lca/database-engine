CREATE OR REPLACE FUNCTION "api"."cmd_review_get_root_table"("p_review_json" "jsonb", "p_data_id" "uuid", "p_data_version" "text") RETURNS "text"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_explicit text := lower(nullif(p_review_json#>>'{data,table}', ''));
  v_process_row jsonb;
  v_model_row jsonb;
  v_expected_name jsonb := coalesce(p_review_json#>'{data,name}', '{}'::jsonb);
begin
  if v_explicit in ('processes', 'lifecyclemodels') then
    return v_explicit;
  end if;

  v_process_row := api.cmd_review_get_dataset_row('processes', p_data_id, p_data_version, false);
  v_model_row := api.cmd_review_get_dataset_row(
    'lifecyclemodels',
    p_data_id,
    p_data_version,
    false
  );

  if v_model_row is not null
     and api.cmd_review_get_dataset_name('lifecyclemodels', v_model_row) = v_expected_name then
    return 'lifecyclemodels';
  end if;

  if v_process_row is not null
     and api.cmd_review_get_dataset_name('processes', v_process_row) = v_expected_name then
    return 'processes';
  end if;

  if v_model_row is not null and v_process_row is null then
    return 'lifecyclemodels';
  end if;

  if v_process_row is not null then
    return 'processes';
  end if;

  return null;
end;
$$;

ALTER FUNCTION "api"."cmd_review_get_root_table"("p_review_json" "jsonb", "p_data_id" "uuid", "p_data_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_review_get_root_table"("p_review_json" "jsonb", "p_data_id" "uuid", "p_data_version" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_review_get_root_table"("p_review_json" "jsonb", "p_data_id" "uuid", "p_data_version" "text") TO "api_internal_executor";
