CREATE OR REPLACE FUNCTION "public"."processes_embedding_ft_input"("proc" "public"."processes") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
begin
  return proc.extracted_md;
end;
$$;

ALTER FUNCTION "public"."processes_embedding_ft_input"("proc" "public"."processes") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."processes_embedding_ft_input"("proc" "public"."processes") TO "anon";

GRANT ALL ON FUNCTION "public"."processes_embedding_ft_input"("proc" "public"."processes") TO "authenticated";

GRANT ALL ON FUNCTION "public"."processes_embedding_ft_input"("proc" "public"."processes") TO "service_role";
