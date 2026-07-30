CREATE OR REPLACE FUNCTION "public"."sources_embedding_ft_input"("proc" "public"."sources") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE STRICT
    SET "search_path" TO ''
    AS $$
  select proc.extracted_md;
$$;

ALTER FUNCTION "public"."sources_embedding_ft_input"("proc" "public"."sources") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."sources_embedding_ft_input"("proc" "public"."sources") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."sources_embedding_ft_input"("proc" "public"."sources") TO "anon";

GRANT ALL ON FUNCTION "public"."sources_embedding_ft_input"("proc" "public"."sources") TO "authenticated";

GRANT ALL ON FUNCTION "public"."sources_embedding_ft_input"("proc" "public"."sources") TO "service_role";
