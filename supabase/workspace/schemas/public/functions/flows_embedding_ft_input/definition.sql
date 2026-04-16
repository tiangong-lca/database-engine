CREATE OR REPLACE FUNCTION "public"."flows_embedding_ft_input"("proc" "public"."flows") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public'
    AS $$
begin
  return proc.extracted_md;
end;
$$;

ALTER FUNCTION "public"."flows_embedding_ft_input"("proc" "public"."flows") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."flows_embedding_ft_input"("proc" "public"."flows") TO "anon";

GRANT ALL ON FUNCTION "public"."flows_embedding_ft_input"("proc" "public"."flows") TO "authenticated";

GRANT ALL ON FUNCTION "public"."flows_embedding_ft_input"("proc" "public"."flows") TO "service_role";
