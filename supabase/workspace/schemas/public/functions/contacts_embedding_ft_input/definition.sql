CREATE OR REPLACE FUNCTION "public"."contacts_embedding_ft_input"("proc" "public"."contacts") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE STRICT
    SET "search_path" TO ''
    AS $$
  select proc.extracted_md;
$$;

ALTER FUNCTION "public"."contacts_embedding_ft_input"("proc" "public"."contacts") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."contacts_embedding_ft_input"("proc" "public"."contacts") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."contacts_embedding_ft_input"("proc" "public"."contacts") TO "anon";

GRANT ALL ON FUNCTION "public"."contacts_embedding_ft_input"("proc" "public"."contacts") TO "authenticated";

GRANT ALL ON FUNCTION "public"."contacts_embedding_ft_input"("proc" "public"."contacts") TO "service_role";
