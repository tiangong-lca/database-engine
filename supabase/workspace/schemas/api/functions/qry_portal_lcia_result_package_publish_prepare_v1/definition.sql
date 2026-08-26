CREATE OR REPLACE FUNCTION "api"."qry_portal_lcia_result_package_publish_prepare_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
begin
  if auth.uid() is null then
    return api.lcia_result_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not api.lcia_result_is_manager() then
    return api.lcia_result_error(
      'not_data_product_manager', 403,
      'Data product manager role is required'
    );
  end if;
  return private.portal_lcia_v3_package_publish_prepare_v1(
    p_package_id, p_display_default_impact_category
  );
end
$$;

ALTER FUNCTION "api"."qry_portal_lcia_result_package_publish_prepare_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_portal_lcia_result_package_publish_prepare_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_portal_lcia_result_package_publish_prepare_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text") TO "authenticated";
