CREATE OR REPLACE FUNCTION "api"."cmd_review_get_actor_meta"("p_actor" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_meta jsonb;
  v_display_name text;
  v_email text;
begin
  select u.raw_user_meta_data
    into v_meta
  from private.users as u
  where u.id = p_actor;

  v_display_name := coalesce(nullif(v_meta->>'display_name', ''), nullif(v_meta->>'email', ''));
  v_email := nullif(v_meta->>'email', '');

  return jsonb_strip_nulls(
    jsonb_build_object(
      'id', p_actor,
      'display_name', v_display_name,
      'email', v_email
    )
  );
end;
$$;

ALTER FUNCTION "api"."cmd_review_get_actor_meta"("p_actor" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_review_get_actor_meta"("p_actor" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_review_get_actor_meta"("p_actor" "uuid") TO "api_internal_executor";
