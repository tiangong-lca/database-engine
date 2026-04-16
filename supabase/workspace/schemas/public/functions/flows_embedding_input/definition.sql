CREATE OR REPLACE FUNCTION "public"."flows_embedding_input"("flow" "public"."flows") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public'
    AS $$
begin
  return flow.extracted_text;
end;
$$;

ALTER FUNCTION "public"."flows_embedding_input"("flow" "public"."flows") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."flows_embedding_input"("flow" "public"."flows") TO "anon";

GRANT ALL ON FUNCTION "public"."flows_embedding_input"("flow" "public"."flows") TO "authenticated";

GRANT ALL ON FUNCTION "public"."flows_embedding_input"("flow" "public"."flows") TO "service_role";
