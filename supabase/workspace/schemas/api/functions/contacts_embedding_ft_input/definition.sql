CREATE OR REPLACE FUNCTION "api"."contacts_embedding_ft_input"("proc" "public"."contacts") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE STRICT
    SET "search_path" TO ''
    AS $$
  select proc.extracted_md;
$$;

ALTER FUNCTION "api"."contacts_embedding_ft_input"("proc" "public"."contacts") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."contacts_embedding_ft_input"("proc" "public"."contacts") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."contacts_embedding_ft_input"("proc" "public"."contacts") TO "api_internal_executor";
