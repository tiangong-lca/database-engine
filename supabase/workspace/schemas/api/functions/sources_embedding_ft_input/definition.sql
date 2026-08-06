CREATE OR REPLACE FUNCTION "api"."sources_embedding_ft_input"("proc" "public"."sources") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE STRICT
    SET "search_path" TO ''
    AS $$
  select proc.extracted_md;
$$;

ALTER FUNCTION "api"."sources_embedding_ft_input"("proc" "public"."sources") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."sources_embedding_ft_input"("proc" "public"."sources") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."sources_embedding_ft_input"("proc" "public"."sources") TO "api_internal_executor";
