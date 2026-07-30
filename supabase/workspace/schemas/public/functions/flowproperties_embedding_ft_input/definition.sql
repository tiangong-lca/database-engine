CREATE OR REPLACE FUNCTION "public"."flowproperties_embedding_ft_input"("proc" "public"."flowproperties") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE STRICT
    SET "search_path" TO ''
    AS $$
  select proc.extracted_md;
$$;

ALTER FUNCTION "public"."flowproperties_embedding_ft_input"("proc" "public"."flowproperties") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."flowproperties_embedding_ft_input"("proc" "public"."flowproperties") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."flowproperties_embedding_ft_input"("proc" "public"."flowproperties") TO "anon";

GRANT ALL ON FUNCTION "public"."flowproperties_embedding_ft_input"("proc" "public"."flowproperties") TO "authenticated";

GRANT ALL ON FUNCTION "public"."flowproperties_embedding_ft_input"("proc" "public"."flowproperties") TO "service_role";
