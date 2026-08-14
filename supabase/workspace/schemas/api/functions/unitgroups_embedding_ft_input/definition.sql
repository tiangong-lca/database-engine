CREATE OR REPLACE FUNCTION "api"."unitgroups_embedding_ft_input"("proc" "public"."unitgroups") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE STRICT
    SET "search_path" TO ''
    AS $$
  select proc.extracted_md;
$$;

ALTER FUNCTION "api"."unitgroups_embedding_ft_input"("proc" "public"."unitgroups") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."unitgroups_embedding_ft_input"("proc" "public"."unitgroups") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."unitgroups_embedding_ft_input"("proc" "public"."unitgroups") TO "api_internal_executor";
