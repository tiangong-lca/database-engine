CREATE OR REPLACE FUNCTION "private"."portal_lcia_v3_package_publish_prepare_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_result jsonb;
  v_package private.lcia_result_packages%rowtype;
  v_projection_id uuid;
begin
  v_result :=
    private.portal_lcia_v3_publish_prepare_unchecked_v1(
      p_package_id, p_display_default_impact_category
    );
  if coalesce((v_result ->> 'ok')::boolean, false) is not true then
    return v_result;
  end if;
  begin
    v_projection_id := nullif(
      v_result #>> '{data,projection,id}', ''
    )::uuid;
  exception when invalid_text_representation then
    return api.lcia_result_error(
      'projection_package_binding_invalid', 409,
      'Portal LCIA package binding is no longer authoritative'
    );
  end;
  select package.* into v_package
  from private.lcia_result_packages as package
  where package.id = p_package_id;
  if v_package.id is null
     or v_result #>> '{data,package,id}' is distinct from v_package.id::text
     or private.portal_lcia_projection_package_binding_valid_v1(
       v_package.id, v_package.build_worker_job_id, v_projection_id
     ) is not true then
    return api.lcia_result_error(
      'projection_package_binding_invalid', 409,
      'Portal LCIA package binding is no longer authoritative'
    );
  end if;
  return v_result;
end
$$;

ALTER FUNCTION "private"."portal_lcia_v3_package_publish_prepare_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_v3_package_publish_prepare_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text") FROM PUBLIC;
