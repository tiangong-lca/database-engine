CREATE OR REPLACE FUNCTION "api"."qry_membership_get_mine"() RETURNS TABLE("user_id" "uuid", "team_id" "uuid", "role" "text", "created_at" timestamp with time zone, "modified_at" timestamp with time zone)
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select
    membership.user_id,
    membership.team_id,
    membership.role::text,
    membership.created_at,
    membership.modified_at
  from private.roles as membership
  where auth.uid() is not null
    and membership.user_id = auth.uid()
  order by
    membership.modified_at desc nulls last,
    membership.created_at desc nulls last,
    membership.team_id,
    membership.role
$$;

ALTER FUNCTION "api"."qry_membership_get_mine"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_membership_get_mine"() FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_membership_get_mine"() TO "authenticated";
