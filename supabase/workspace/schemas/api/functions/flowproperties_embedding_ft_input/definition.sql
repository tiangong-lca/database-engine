CREATE OR REPLACE FUNCTION "api"."flowproperties_embedding_ft_input"("proc" "public"."flowproperties") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE STRICT
    SET "search_path" TO ''
    AS $$
  select proc.extracted_md;
$$;

ALTER FUNCTION "api"."flowproperties_embedding_ft_input"("proc" "public"."flowproperties") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."flowproperties_embedding_ft_input"("proc" "public"."flowproperties") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."flowproperties_embedding_ft_input"("proc" "public"."flowproperties") TO "api_internal_executor";
