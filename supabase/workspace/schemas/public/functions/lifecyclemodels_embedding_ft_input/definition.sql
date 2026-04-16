CREATE OR REPLACE FUNCTION "public"."lifecyclemodels_embedding_ft_input"("proc" "public"."lifecyclemodels") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public'
    AS $$
begin
  return proc.extracted_md;
end;
$$;

ALTER FUNCTION "public"."lifecyclemodels_embedding_ft_input"("proc" "public"."lifecyclemodels") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."lifecyclemodels_embedding_ft_input"("proc" "public"."lifecyclemodels") TO "anon";

GRANT ALL ON FUNCTION "public"."lifecyclemodels_embedding_ft_input"("proc" "public"."lifecyclemodels") TO "authenticated";

GRANT ALL ON FUNCTION "public"."lifecyclemodels_embedding_ft_input"("proc" "public"."lifecyclemodels") TO "service_role";
