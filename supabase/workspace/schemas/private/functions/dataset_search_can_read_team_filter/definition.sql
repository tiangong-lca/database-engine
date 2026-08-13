CREATE OR REPLACE FUNCTION "private"."dataset_search_can_read_team_filter"("p_team_id" "uuid", "p_actor_id" "uuid") RETURNS boolean
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $$
declare
  v_request_role text := nullif(current_setting('request.jwt.claim.role', true), '');
begin
  if p_team_id is null then
    return false;
  end if;

  if coalesce(v_request_role, '') not in ('anon', 'authenticated') then
    return true;
  end if;

  if p_actor_id is null then
    return false;
  end if;

  return exists (
    select 1
    from private.roles r
    where r.team_id = p_team_id
      and r.user_id = p_actor_id
      and r.role::text in ('admin', 'member', 'owner')
  );
end;
$$;

ALTER FUNCTION "private"."dataset_search_can_read_team_filter"("p_team_id" "uuid", "p_actor_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_search_can_read_team_filter"("p_team_id" "uuid", "p_actor_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_search_can_read_team_filter"("p_team_id" "uuid", "p_actor_id" "uuid") TO "api_internal_executor";
