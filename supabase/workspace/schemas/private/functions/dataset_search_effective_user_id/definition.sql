CREATE OR REPLACE FUNCTION "private"."dataset_search_effective_user_id"("p_this_user_id" "text") RETURNS "uuid"
    LANGUAGE "plpgsql" STABLE
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $_$
declare
  v_actor_id uuid := auth.uid();
  v_request_role text := nullif(current_setting('request.jwt.claim.role', true), '');
  v_param_user_id uuid;
begin
  if v_actor_id is not null then
    return v_actor_id;
  end if;

  if coalesce(v_request_role, '') in ('anon', 'authenticated') then
    return null::uuid;
  end if;

  v_param_user_id := case
    when coalesce(btrim(p_this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      then btrim(p_this_user_id)::uuid
    else null::uuid
  end;

  return v_param_user_id;
end;
$_$;

ALTER FUNCTION "private"."dataset_search_effective_user_id"("p_this_user_id" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_search_effective_user_id"("p_this_user_id" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_search_effective_user_id"("p_this_user_id" "text") TO "api_internal_executor";
