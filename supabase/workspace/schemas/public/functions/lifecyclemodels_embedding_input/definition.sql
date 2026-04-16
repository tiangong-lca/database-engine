CREATE OR REPLACE FUNCTION "public"."lifecyclemodels_embedding_input"("models" "public"."lifecyclemodels") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public'
    AS $$
begin
  return models.extracted_text;
end;
$$;

ALTER FUNCTION "public"."lifecyclemodels_embedding_input"("models" "public"."lifecyclemodels") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."lifecyclemodels_embedding_input"("models" "public"."lifecyclemodels") TO "anon";

GRANT ALL ON FUNCTION "public"."lifecyclemodels_embedding_input"("models" "public"."lifecyclemodels") TO "authenticated";

GRANT ALL ON FUNCTION "public"."lifecyclemodels_embedding_input"("models" "public"."lifecyclemodels") TO "service_role";
