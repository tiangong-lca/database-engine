CREATE OR REPLACE FUNCTION "public"."processes_embedding_input"("proc" "public"."processes") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO ''
    AS $$
begin
  return proc.extracted_text;
end;
$$;

ALTER FUNCTION "public"."processes_embedding_input"("proc" "public"."processes") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."processes_embedding_input"("proc" "public"."processes") TO "anon";

GRANT ALL ON FUNCTION "public"."processes_embedding_input"("proc" "public"."processes") TO "authenticated";

GRANT ALL ON FUNCTION "public"."processes_embedding_input"("proc" "public"."processes") TO "service_role";
