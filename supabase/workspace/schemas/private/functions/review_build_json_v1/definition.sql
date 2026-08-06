CREATE OR REPLACE FUNCTION "private"."review_build_json_v1"("p_target_table" "text", "p_target_row" "jsonb", "p_owner_id" "uuid", "p_action" "text", "p_actor" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_team_name jsonb;
  v_owner_meta jsonb;
  v_actor_meta jsonb;
begin
  select coalesce(team_row.json->'title', team_row.json->'name')
  into v_team_name
  from private.teams as team_row
  where team_row.id = nullif(p_target_row->>'team_id', '')::uuid;

  select user_row.raw_user_meta_data
  into v_owner_meta
  from private.users as user_row
  where user_row.id = p_owner_id;

  select user_row.raw_user_meta_data
  into v_actor_meta
  from private.users as user_row
  where user_row.id = p_actor;

  return jsonb_build_object(
    'review_kind', case
      when p_action = 'submit_reference_review' then 'reference'
      else 'root'
    end,
    'data', jsonb_build_object(
      'id', p_target_row->>'id',
      'version', p_target_row->>'version',
      'table', p_target_table,
      'name', api.cmd_review_get_dataset_name(
        p_target_table,
        p_target_row
      )
    ),
    'team', jsonb_build_object(
      'id', p_target_row->>'team_id',
      'name', v_team_name
    ),
    'user', jsonb_build_object(
      'id', p_owner_id,
      'name', coalesce(
        nullif(v_owner_meta->>'display_name', ''),
        nullif(v_owner_meta->>'email', '')
      ),
      'email', nullif(v_owner_meta->>'email', '')
    ),
    'comment', jsonb_build_object('message', ''),
    'logs', jsonb_build_array(jsonb_build_object(
      'action', p_action,
      'time', to_jsonb(now()),
      'user', jsonb_build_object(
        'id', p_actor,
        'display_name', coalesce(
          nullif(v_actor_meta->>'display_name', ''),
          nullif(v_actor_meta->>'email', '')
        )
      )
    ))
  );
end;
$$;

ALTER FUNCTION "private"."review_build_json_v1"("p_target_table" "text", "p_target_row" "jsonb", "p_owner_id" "uuid", "p_action" "text", "p_actor" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_build_json_v1"("p_target_table" "text", "p_target_row" "jsonb", "p_owner_id" "uuid", "p_action" "text", "p_actor" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_build_json_v1"("p_target_table" "text", "p_target_row" "jsonb", "p_owner_id" "uuid", "p_action" "text", "p_actor" "uuid") TO "api_internal_executor";
