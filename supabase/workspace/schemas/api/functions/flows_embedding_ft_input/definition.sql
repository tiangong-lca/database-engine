CREATE OR REPLACE FUNCTION "api"."flows_embedding_ft_input"("proc" "public"."flows") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions'
    AS $$
begin
  return proc.extracted_md;
end;
$$;

ALTER FUNCTION "api"."flows_embedding_ft_input"("proc" "public"."flows") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."flows_embedding_ft_input"("proc" "public"."flows") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."flows_embedding_ft_input"("proc" "public"."flows") TO "api_internal_executor";
