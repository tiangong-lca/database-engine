CREATE OR REPLACE FUNCTION "api"."processes_embedding_ft_input"("proc" "public"."processes") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $$
begin
  return proc.extracted_md;
end;
$$;

ALTER FUNCTION "api"."processes_embedding_ft_input"("proc" "public"."processes") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."processes_embedding_ft_input"("proc" "public"."processes") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."processes_embedding_ft_input"("proc" "public"."processes") TO "api_internal_executor";
