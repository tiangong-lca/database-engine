CREATE OR REPLACE FUNCTION "public"."cmd_review_ref_type_to_table"("p_ref_type" "text") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select case lower(trim(coalesce(p_ref_type, '')))
    when 'contact data set' then 'contacts'
    when 'source data set' then 'sources'
    when 'unit group data set' then 'unitgroups'
    when 'flow property data set' then 'flowproperties'
    when 'flow data set' then 'flows'
    when 'process data set' then 'processes'
    when 'lifecyclemodel data set' then 'lifecyclemodels'
    when 'lifecycle model data set' then 'lifecyclemodels'
    when 'lifecyclemodel dataset' then 'lifecyclemodels'
    else null
  end
$$;

ALTER FUNCTION "public"."cmd_review_ref_type_to_table"("p_ref_type" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_ref_type_to_table"("p_ref_type" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_ref_type_to_table"("p_ref_type" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_ref_type_to_table"("p_ref_type" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_ref_type_to_table"("p_ref_type" "text") TO "service_role";
