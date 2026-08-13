CREATE OR REPLACE FUNCTION "api"."lifecyclemodels_embedding_ft_input"("proc" "public"."lifecyclemodels") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions'
    AS $$
begin
  return proc.extracted_md;
end;
$$;

ALTER FUNCTION "api"."lifecyclemodels_embedding_ft_input"("proc" "public"."lifecyclemodels") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."lifecyclemodels_embedding_ft_input"("proc" "public"."lifecyclemodels") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."lifecyclemodels_embedding_ft_input"("proc" "public"."lifecyclemodels") TO "api_internal_executor";
