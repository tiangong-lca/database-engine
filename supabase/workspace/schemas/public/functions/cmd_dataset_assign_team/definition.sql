CREATE OR REPLACE FUNCTION "public"."cmd_dataset_assign_team"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_team_id" "uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_current_row jsonb;
  v_owner_id uuid;
  v_state_code integer;
  v_updated_row jsonb;
  v_actor_has_team_role boolean;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table not in (
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes',
    'lifecyclemodels'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Unsupported dataset table'
    );
  end if;

  execute format(
    'select to_jsonb(t) from public.%I as t where t.id = $1 and t.version = $2 for update of t',
    p_table
  )
    into v_current_row
    using p_id, p_version;

  if v_current_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  v_owner_id := nullif(v_current_row->>'user_id', '')::uuid;
  v_state_code := coalesce((v_current_row->>'state_code')::integer, 0);

  if v_owner_id is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the dataset owner can change dataset team ownership'
    );
  end if;

  if v_state_code >= 100 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_ALREADY_PUBLISHED',
      'status', 403,
      'message', 'Published data cannot be reassigned to another team',
      'details', jsonb_build_object(
        'state_code', v_state_code
      )
    );
  end if;

  if v_state_code >= 20 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_UNDER_REVIEW',
      'status', 403,
      'message', 'Data is under review and cannot be reassigned',
      'details', jsonb_build_object(
        'state_code', 20,
        'review_state_code', v_state_code
      )
    );
  end if;

  select exists (
    select 1
    from public.roles r
    where r.user_id = v_actor
      and r.team_id = p_team_id
      and r.role not in ('is_invited', 'rejected')
  )
    into v_actor_has_team_role;

  if not v_actor_has_team_role then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_MEMBERSHIP_REQUIRED',
      'status', 403,
      'message', 'You must belong to the target team before assigning dataset ownership'
    );
  end if;

  execute format(
    'update public.%I as t
        set team_id = $1,
            modified_at = now()
      where t.id = $2
        and t.version = $3
    returning to_jsonb(t)',
    p_table
  )
    into v_updated_row
    using p_team_id, p_id, p_version;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_assign_team',
    v_actor,
    p_table,
    p_id,
    p_version,
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_updated_row
  );
end;
$_$;

ALTER FUNCTION "public"."cmd_dataset_assign_team"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_team_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_assign_team"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_team_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_assign_team"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_team_id" "uuid", "p_audit" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_dataset_assign_team"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_team_id" "uuid", "p_audit" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_dataset_assign_team"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_team_id" "uuid", "p_audit" "jsonb") TO "service_role";
