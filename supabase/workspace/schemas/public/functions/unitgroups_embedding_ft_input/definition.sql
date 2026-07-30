CREATE OR REPLACE FUNCTION "public"."unitgroups_embedding_ft_input"("proc" "public"."unitgroups") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE STRICT
    SET "search_path" TO ''
    AS $$
  select proc.extracted_md;
$$;

ALTER FUNCTION "public"."unitgroups_embedding_ft_input"("proc" "public"."unitgroups") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."unitgroups_embedding_ft_input"("proc" "public"."unitgroups") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."unitgroups_embedding_ft_input"("proc" "public"."unitgroups") TO "anon";

GRANT ALL ON FUNCTION "public"."unitgroups_embedding_ft_input"("proc" "public"."unitgroups") TO "authenticated";

GRANT ALL ON FUNCTION "public"."unitgroups_embedding_ft_input"("proc" "public"."unitgroups") TO "service_role";
