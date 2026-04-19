CREATE OR REPLACE FUNCTION "public"."cmd_membership_resolve_sort_direction"("p_sort_order" "text") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
begin
  case lower(coalesce(p_sort_order, ''))
    when 'asc' then
      return 'asc';
    when 'ascend' then
      return 'asc';
    else
      return 'desc';
  end case;
end;
$$;

ALTER FUNCTION "public"."cmd_membership_resolve_sort_direction"("p_sort_order" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_membership_resolve_sort_direction"("p_sort_order" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_membership_resolve_sort_direction"("p_sort_order" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_membership_resolve_sort_direction"("p_sort_order" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_membership_resolve_sort_direction"("p_sort_order" "text") TO "service_role";
