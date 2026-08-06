CREATE OR REPLACE FUNCTION "api"."cmd_review_extract_refs"("p_json" "jsonb") RETURNS TABLE("ref_type" "text", "ref_object_id" "uuid", "ref_version" "text")
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select distinct
    role_ref.ref_type,
    role_ref.ref_object_id,
    role_ref.ref_version
  from private.cmd_review_reference_roles(
    case
      when coalesce(p_json, '{}'::jsonb) ? 'processDataSet' then 'processes'
      when coalesce(p_json, '{}'::jsonb) ? 'lifeCycleModelDataSet'
        then 'lifecyclemodels'
      when coalesce(p_json, '{}'::jsonb) ? 'flowDataSet' then 'flows'
      when coalesce(p_json, '{}'::jsonb) ? 'flowPropertyDataSet'
        then 'flowproperties'
      when coalesce(p_json, '{}'::jsonb) ? 'unitGroupDataSet'
        then 'unitgroups'
      when coalesce(p_json, '{}'::jsonb) ? 'sourceDataSet' then 'sources'
      when coalesce(p_json, '{}'::jsonb) ? 'contactDataSet' then 'contacts'
      else 'comments'
    end,
    'json',
    p_json
  ) as role_ref
  where role_ref.lifecycle_role in ('RequiredSupport', 'ModelComposition')
$$;

ALTER FUNCTION "api"."cmd_review_extract_refs"("p_json" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_review_extract_refs"("p_json" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_review_extract_refs"("p_json" "jsonb") TO "api_internal_executor";
