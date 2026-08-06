CREATE OR REPLACE FUNCTION "api"."cmd_membership_resolve_sort_direction"("p_sort_order" "text") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
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

ALTER FUNCTION "api"."cmd_membership_resolve_sort_direction"("p_sort_order" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_membership_resolve_sort_direction"("p_sort_order" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_membership_resolve_sort_direction"("p_sort_order" "text") TO "api_internal_executor";
