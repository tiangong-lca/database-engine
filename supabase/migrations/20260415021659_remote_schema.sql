set check_function_bodies = off;

CREATE OR REPLACE FUNCTION public._navicat_temp_stored_proc(query_text text, query_embedding extensions.vector, filter_condition text DEFAULT ''::text, match_threshold double precision DEFAULT 0.5, match_count integer DEFAULT 20, full_text_weight numeric DEFAULT 0.3, extracted_text_weight numeric DEFAULT 0.2, semantic_weight numeric DEFAULT 0.5, rrf_k integer DEFAULT 10, data_source text DEFAULT 'tg'::text, this_user_id text DEFAULT ''::text, page_size integer DEFAULT 10, page_current integer DEFAULT 1)
 RETURNS TABLE(id uuid, "json" jsonb)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$ BEGIN
		RETURN QUERY WITH 
		full_text AS (
			SELECT
				ps.RANK AS ps_rank,
				ps.ID AS ps_id,
				ps.JSON AS ps_json 
			FROM
				pgroonga_search_processes ( query_text, filter_condition, 20, -- page_size: 获取足够多候选
					1, -- page_current: 第1页
				data_source, this_user_id ) ps 
		),
		ex_text AS (
    SELECT
      ex.rank AS ex_rank,
      ex.id   AS ex_id,
      p.json  AS ex_json
    FROM pgroonga_search_processes_text(
           query_text,
           20,          -- page_size
           1,      -- page_current
           data_source,
           this_user_id
         ) ex
    JOIN public.processes p ON p.id = ex.id
  ),
		semantic AS (
			SELECT
				ss.RANK AS ss_rank,
				ss.ID AS ss_id,
				ss.JSON AS ss_json 
			FROM
				semantic_search_processes ( query_embedding, filter_condition, match_threshold, match_count, data_source, this_user_id ) ss 
		) SELECT COALESCE
		( full_text.ps_id, semantic.ss_id, ex_text.ex_id ) AS ID,
		COALESCE ( full_text.ps_json, semantic.ss_json, ex_text.ex_json) AS JSON, 
		COALESCE(1.0 / (rrf_k + full_text.ps_rank), 0.0) * full_text_weight
      + COALESCE(1.0 / (rrf_k + ex_text.ex_rank), 0.0) * text_weight
      + COALESCE(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight
      AS score
		FROM
			full_text
			FULL OUTER JOIN semantic ON full_text.ps_id = semantic.ss_id
			FULL OUTER JOIN ex_text ON ex_text.ex_id = COALESCE(full_text.ps_id, semantic.ss_id) 
		ORDER BY
			score DESC 
			LIMIT page_size OFFSET ( page_current - 1 ) * page_size;
		
	END;
$function$
;

CREATE OR REPLACE FUNCTION public._navicat_temp_stored_proc(query_text text, query_embedding text, filter_condition text DEFAULT ''::text, match_threshold double precision DEFAULT 0.5, match_count integer DEFAULT 20, full_text_weight numeric DEFAULT 0.3, extracted_text_weight numeric DEFAULT 0.2, semantic_weight numeric DEFAULT 0.5, rrf_k integer DEFAULT 10, data_source text DEFAULT 'tg'::text, this_user_id text DEFAULT ''::text, page_size integer DEFAULT 10, page_current integer DEFAULT 1)
 RETURNS TABLE(id uuid, "json" jsonb)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$ BEGIN
		RETURN QUERY WITH 
		full_text AS (
			SELECT
				ps.RANK AS ps_rank,
				ps.ID AS ps_id,
				ps.JSON AS ps_json 
			FROM
				pgroonga_search_processes ( query_text, filter_condition, 20, -- page_size: 获取足够多候选
					1, -- page_current: 第1页
				data_source, this_user_id ) ps 
		),
		ex_text AS (
    SELECT
      ex.rank AS ex_rank,
      ex.id   AS ex_id,
      p.json  AS ex_json
    FROM pgroonga_search_processes_text(
           query_text,
           20,          -- page_size
           1,      -- page_current
           data_source,
           this_user_id
         ) ex
    JOIN public.processes p ON p.id = ex.id
  ),
		semantic AS (
			SELECT
				ss.RANK AS ss_rank,
				ss.ID AS ss_id,
				ss.JSON AS ss_json 
			FROM
				semantic_search_processes ( query_embedding, filter_condition, match_threshold, match_count, data_source, this_user_id ) ss 
		) SELECT COALESCE
		( full_text.ps_id, semantic.ss_id, ex_text.ex_id ) AS ID,
		COALESCE ( full_text.ps_json, semantic.ss_json, ex_text.ex_json) AS JSON, 
		COALESCE(1.0 / (rrf_k + full_text.ps_rank), 0.0) * full_text_weight
      + COALESCE(1.0 / (rrf_k + ex_text.ex_rank), 0.0) * text_weight
      + COALESCE(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight
      AS score
		FROM
			full_text
			FULL OUTER JOIN semantic ON full_text.ps_id = semantic.ss_id
			FULL OUTER JOIN ex_text ON ex_text.ex_id = COALESCE(full_text.ps_id, semantic.ss_id) 
		ORDER BY
			score DESC 
			LIMIT page_size OFFSET ( page_current - 1 ) * page_size;
		
	END;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_dataset_assign_team(p_table text, p_id uuid, p_version text, p_team_id uuid, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
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
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_dataset_create(p_table text, p_id uuid, p_json_ordered jsonb, p_model_id uuid DEFAULT NULL::uuid, p_rule_verification boolean DEFAULT NULL::boolean, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_created_row jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table = 'lifecyclemodels' then
    return jsonb_build_object(
      'ok', false,
      'code', 'LIFECYCLEMODEL_BUNDLE_REQUIRED',
      'status', 400,
      'message', 'Lifecycle models must use bundle create and delete commands'
    );
  end if;

  if p_table not in (
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Unsupported dataset table'
    );
  end if;

  if p_json_ordered is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'JSON_ORDERED_REQUIRED',
      'status', 400,
      'message', 'jsonOrdered is required'
    );
  end if;

  if p_table <> 'processes' and p_model_id is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'MODEL_ID_NOT_ALLOWED',
      'status', 400,
      'message', 'modelId is only allowed for process dataset creation'
    );
  end if;

  begin
    if p_table = 'processes' then
      execute format(
        'insert into public.%I as t (id, json_ordered, model_id, rule_verification)
         values ($1, $2::json, $3, $4)
         returning to_jsonb(t)',
        p_table
      )
        into v_created_row
        using p_id, p_json_ordered, p_model_id, p_rule_verification;
    else
      execute format(
        'insert into public.%I as t (id, json_ordered, rule_verification)
         values ($1, $2::json, $3)
         returning to_jsonb(t)',
        p_table
      )
        into v_created_row
        using p_id, p_json_ordered, p_rule_verification;
    end if;
  exception
    when unique_violation then
      return jsonb_build_object(
        'ok', false,
        'code', '23505',
        'status', 409,
        'message', 'Dataset with the same id and version already exists'
      );
    when not_null_violation then
      return jsonb_build_object(
        'ok', false,
        'code', '23502',
        'status', 400,
        'message', 'Dataset creation requires a valid id, version, and jsonOrdered payload'
      );
    when check_violation then
      return jsonb_build_object(
        'ok', false,
        'code', sqlstate,
        'status', 400,
        'message', sqlerrm
      );
  end;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_create',
    v_actor,
    p_table,
    p_id,
    nullif(v_created_row->>'version', ''),
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_created_row
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_dataset_delete(p_table text, p_id uuid, p_version text, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_current_row jsonb;
  v_deleted_row jsonb;
  v_owner_id uuid;
  v_state_code integer;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table = 'lifecyclemodels' then
    return jsonb_build_object(
      'ok', false,
      'code', 'LIFECYCLEMODEL_BUNDLE_REQUIRED',
      'status', 400,
      'message', 'Lifecycle models must use bundle create and delete commands'
    );
  end if;

  if p_table not in (
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes'
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
      'message', 'Only the dataset owner can delete this dataset'
    );
  end if;

  if v_state_code <> 0 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_DELETE_REQUIRES_DRAFT',
      'status', 403,
      'message', 'Only draft datasets can be deleted',
      'details', jsonb_build_object(
        'state_code', v_state_code
      )
    );
  end if;

  execute format(
    'delete from public.%I as t
      where t.id = $1
        and t.version = $2
      returning to_jsonb(t)',
    p_table
  )
    into v_deleted_row
    using p_id, p_version;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_delete',
    v_actor,
    p_table,
    p_id,
    p_version,
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_deleted_row
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_dataset_publish(p_table text, p_id uuid, p_version text, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_current_row jsonb;
  v_owner_id uuid;
  v_state_code integer;
  v_updated_row jsonb;
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
      'message', 'Only the dataset owner can publish the dataset'
    );
  end if;

  if v_state_code >= 100 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_ALREADY_PUBLISHED',
      'status', 403,
      'message', 'Dataset is already published',
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
      'message', 'Data is under review and cannot be published directly',
      'details', jsonb_build_object(
        'state_code', 20,
        'review_state_code', v_state_code
      )
    );
  end if;

  execute format(
    'update public.%I as t
        set state_code = 100,
            modified_at = now()
      where t.id = $1
        and t.version = $2
    returning to_jsonb(t)',
    p_table
  )
    into v_updated_row
    using p_id, p_version;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_publish',
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
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_dataset_save_draft(p_table text, p_id uuid, p_version text, p_json_ordered jsonb, p_model_id uuid DEFAULT NULL::uuid, p_rule_verification boolean DEFAULT NULL::boolean, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_current_row jsonb;
  v_owner_id uuid;
  v_state_code integer;
  v_updated_row jsonb;
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

  if p_json_ordered is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'JSON_ORDERED_REQUIRED',
      'status', 400,
      'message', 'jsonOrdered is required'
    );
  end if;

  if p_table <> 'processes' and p_model_id is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'MODEL_ID_NOT_ALLOWED',
      'status', 400,
      'message', 'modelId is only allowed for process dataset drafts'
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
      'message', 'Only the dataset owner can save draft changes'
    );
  end if;

  if v_state_code >= 100 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_ALREADY_PUBLISHED',
      'status', 403,
      'message', 'Published data cannot be edited through draft save',
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
      'message', 'Data is under review and cannot be modified',
      'details', jsonb_build_object(
        'state_code', 20,
        'review_state_code', v_state_code
      )
    );
  end if;

  if p_table = 'processes' then
    execute format(
      'update public.%I as t
          set json_ordered = $1::json,
              model_id = coalesce($2, t.model_id),
              rule_verification = $3,
              modified_at = now()
        where t.id = $4
          and t.version = $5
      returning to_jsonb(t)',
      p_table
    )
      into v_updated_row
      using p_json_ordered, p_model_id, p_rule_verification, p_id, p_version;
  else
    execute format(
      'update public.%I as t
          set json_ordered = $1::json,
              rule_verification = $2,
              modified_at = now()
        where t.id = $3
          and t.version = $4
      returning to_jsonb(t)',
      p_table
    )
      into v_updated_row
      using p_json_ordered, p_rule_verification, p_id, p_version;
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_save_draft',
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
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_membership_is_review_admin(p_actor uuid DEFAULT auth.uid())
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select exists (
    select 1
    from public.roles
    where user_id = coalesce(p_actor, auth.uid())
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role = 'review-admin'
  )
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_membership_is_system_manager(p_actor uuid DEFAULT auth.uid())
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select exists (
    select 1
    from public.roles
    where user_id = coalesce(p_actor, auth.uid())
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role in ('owner', 'admin', 'member')
  )
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_membership_is_system_owner(p_actor uuid DEFAULT auth.uid())
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select exists (
    select 1
    from public.roles
    where user_id = coalesce(p_actor, auth.uid())
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role = 'owner'
  )
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_membership_is_team_manager(p_actor uuid, p_team_id uuid)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select exists (
    select 1
    from public.roles
    where user_id = p_actor
      and team_id = p_team_id
      and role in ('owner', 'admin')
  )
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_membership_is_team_owner(p_actor uuid, p_team_id uuid)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select exists (
    select 1
    from public.roles
    where user_id = p_actor
      and team_id = p_team_id
      and role = 'owner'
  )
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_membership_resolve_member_order_by(p_sort_by text, p_allow_workload boolean DEFAULT false)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
begin
  case lower(coalesce(p_sort_by, ''))
    when 'role' then
      return 'm.role';
    when 'email' then
      return 'm.email';
    when 'display_name' then
      return 'm.display_name';
    when 'modified_at' then
      return 'm.modified_at';
    when 'pendingcount' then
      if p_allow_workload then
        return 'm.pending_count';
      end if;
    when 'pending_count' then
      if p_allow_workload then
        return 'm.pending_count';
      end if;
    when 'reviewedcount' then
      if p_allow_workload then
        return 'm.reviewed_count';
      end if;
    when 'reviewed_count' then
      if p_allow_workload then
        return 'm.reviewed_count';
      end if;
    else
      return 'm.created_at';
  end case;

  return 'm.created_at';
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_membership_resolve_sort_direction(p_sort_order text)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
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
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_notification_normalize_text_array(p_values text[])
 RETURNS text[]
 LANGUAGE sql
 IMMUTABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
  with normalized as (
    select
      min(item.ordinality) as first_ordinality,
      nullif(btrim(item.value), '') as normalized_value
    from unnest(coalesce(p_values, array[]::text[])) with ordinality as item(value, ordinality)
    group by nullif(btrim(item.value), '')
  )
  select coalesce(
    array(
      select normalized_value
      from normalized
      where normalized_value is not null
      order by first_ordinality
    ),
    array[]::text[]
  );
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_notification_send_validation_issue(p_recipient_user_id uuid, p_dataset_type text, p_dataset_id uuid, p_dataset_version text, p_link text DEFAULT NULL::text, p_issue_codes text[] DEFAULT ARRAY[]::text[], p_tab_names text[] DEFAULT ARRAY[]::text[], p_issue_count integer DEFAULT 0, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_dataset_type text := nullif(btrim(coalesce(p_dataset_type, '')), '');
  v_dataset_version text := nullif(btrim(coalesce(p_dataset_version, '')), '');
  v_target_table text;
  v_target_row jsonb;
  v_issue_codes text[];
  v_tab_names text[];
  v_sender_name text;
  v_notification_row jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_recipient_user_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'RECIPIENT_REQUIRED',
      'status', 400,
      'message', 'recipientUserId is required'
    );
  end if;

  if v_actor = p_recipient_user_id then
    return jsonb_build_object(
      'ok', false,
      'code', 'NOTIFICATION_SELF_TARGET',
      'status', 409,
      'message', 'The recipient must differ from the actor'
    );
  end if;

  if v_dataset_type is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_TYPE_REQUIRED',
      'status', 400,
      'message', 'datasetType is required'
    );
  end if;

  if p_dataset_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_ID_REQUIRED',
      'status', 400,
      'message', 'datasetId is required'
    );
  end if;

  if v_dataset_version is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_VERSION_REQUIRED',
      'status', 400,
      'message', 'datasetVersion is required'
    );
  end if;

  if not exists (
    select 1
    from public.users
    where id = p_recipient_user_id
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'RECIPIENT_NOT_FOUND',
      'status', 404,
      'message', 'The recipient user does not exist'
    );
  end if;

  v_target_table := public.cmd_review_ref_type_to_table(v_dataset_type);
  if v_target_table is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_TYPE_INVALID',
      'status', 400,
      'message', 'datasetType is not supported'
    );
  end if;

  v_target_row := public.cmd_review_get_dataset_row(
    v_target_table,
    p_dataset_id,
    v_dataset_version,
    false
  );
  if v_target_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'The target dataset does not exist'
    );
  end if;

  if ((v_target_row ->> 'user_id')::uuid is distinct from p_recipient_user_id) then
    return jsonb_build_object(
      'ok', false,
      'code', 'RECIPIENT_NOT_TARGET_OWNER',
      'status', 403,
      'message', 'The recipient must own the target dataset'
    );
  end if;

  v_issue_codes := public.cmd_notification_normalize_text_array(p_issue_codes);
  v_tab_names := public.cmd_notification_normalize_text_array(p_tab_names);

  select coalesce(
    nullif(btrim(u.raw_user_meta_data ->> 'display_name'), ''),
    nullif(btrim(u.raw_user_meta_data ->> 'name'), ''),
    nullif(btrim(u.raw_user_meta_data ->> 'email'), ''),
    '-'
  )
  into v_sender_name
  from public.users as u
  where u.id = v_actor;

  v_sender_name := coalesce(v_sender_name, '-');

  insert into public.notifications (
    recipient_user_id,
    sender_user_id,
    type,
    dataset_type,
    dataset_id,
    dataset_version,
    json,
    modified_at
  )
  values (
    p_recipient_user_id,
    v_actor,
    'validation_issue',
    v_dataset_type,
    p_dataset_id,
    v_dataset_version,
    jsonb_build_object(
      'issueCodes', to_jsonb(v_issue_codes),
      'issueCount', greatest(coalesce(p_issue_count, 0), 0),
      'link', nullif(btrim(coalesce(p_link, '')), ''),
      'senderName', v_sender_name,
      'tabNames', to_jsonb(v_tab_names)
    ),
    now()
  )
  on conflict (
    recipient_user_id,
    sender_user_id,
    type,
    dataset_type,
    dataset_id,
    dataset_version
  ) do update
  set json = excluded.json,
      modified_at = excluded.modified_at
  returning to_jsonb(notifications.*)
    into v_notification_row;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_notification_send_validation_issue',
    v_actor,
    'notifications',
    (v_notification_row ->> 'id')::uuid,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'recipientUserId', p_recipient_user_id,
      'datasetType', v_dataset_type,
      'datasetId', p_dataset_id,
      'datasetVersion', v_dataset_version,
      'issueCount', greatest(coalesce(p_issue_count, 0), 0)
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_notification_row
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_append_log(p_review_json jsonb, p_action text, p_actor uuid, p_extra jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_review_json jsonb := coalesce(p_review_json, '{}'::jsonb);
  v_logs jsonb := public.cmd_review_json_array(v_review_json->'logs');
  v_actor_meta jsonb := public.cmd_review_get_actor_meta(p_actor);
  v_log_entry jsonb;
begin
  v_log_entry := jsonb_build_object(
    'action', p_action,
    'time', to_jsonb(now()),
    'user', v_actor_meta
  ) || coalesce(p_extra, '{}'::jsonb);

  return jsonb_set(
    v_review_json,
    '{logs}',
    v_logs || jsonb_build_array(v_log_entry),
    true
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_append_review_ref(p_existing_reviews jsonb, p_review_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_reviews jsonb := case
    when jsonb_typeof(p_existing_reviews) = 'array' then p_existing_reviews
    else '[]'::jsonb
  end;
begin
  if exists (
    select 1
    from jsonb_array_elements(v_reviews) as review_item(value)
    where review_item.value->>'id' = p_review_id::text
  ) then
    return v_reviews;
  end if;

  return v_reviews || jsonb_build_array(
    jsonb_build_object(
      'key', jsonb_array_length(v_reviews),
      'id', p_review_id
    )
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_apply_model_validation_to_process_json(p_process_json jsonb, p_model_json jsonb, p_comment_review jsonb DEFAULT '[]'::jsonb, p_comment_compliance jsonb DEFAULT '[]'::jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
  with base as (
    select
      coalesce(p_process_json, '{}'::jsonb) as process_json,
      public.cmd_review_json_array(
        coalesce(
          p_process_json #> '{processDataSet,modellingAndValidation,validation,review}',
          '[]'::jsonb
        )
      ) as existing_review_items,
      public.cmd_review_json_array(
        coalesce(
          p_process_json #> '{processDataSet,modellingAndValidation,complianceDeclarations,compliance}',
          '[]'::jsonb
        )
      ) as existing_compliance_items,
      public.cmd_review_json_array(coalesce(p_comment_review, '[]'::jsonb)) as comment_review_items,
      public.cmd_review_json_array(coalesce(p_comment_compliance, '[]'::jsonb))
        as comment_compliance_items
  ),
  prepared as (
    select
      jsonb_set(
        jsonb_set(
          jsonb_set(
            jsonb_set(
              base.process_json,
              '{processDataSet}',
              case
                when jsonb_typeof(base.process_json->'processDataSet') = 'object'
                  then base.process_json->'processDataSet'
                else '{}'::jsonb
              end,
              true
            ),
            '{processDataSet,modellingAndValidation}',
            case
              when jsonb_typeof(
                base.process_json #> '{processDataSet,modellingAndValidation}'
              ) = 'object'
                then base.process_json #> '{processDataSet,modellingAndValidation}'
              else '{}'::jsonb
            end,
            true
          ),
          '{processDataSet,modellingAndValidation,validation}',
          case
            when jsonb_typeof(
              base.process_json #> '{processDataSet,modellingAndValidation,validation}'
            ) = 'object'
              then base.process_json #> '{processDataSet,modellingAndValidation,validation}'
            else '{}'::jsonb
          end,
          true
        ),
        '{processDataSet,modellingAndValidation,complianceDeclarations}',
        case
          when jsonb_typeof(
            base.process_json #> '{processDataSet,modellingAndValidation,complianceDeclarations}'
          ) = 'object'
            then base.process_json #> '{processDataSet,modellingAndValidation,complianceDeclarations}'
          else '{}'::jsonb
        end,
        true
      ) as prepared_process_json,
      base.existing_review_items,
      base.existing_compliance_items,
      base.comment_review_items,
      base.comment_compliance_items
    from base
  )
  select jsonb_set(
    jsonb_set(
      prepared.prepared_process_json,
      '{processDataSet,modellingAndValidation,validation,review}',
      prepared.existing_review_items || prepared.comment_review_items,
      true
    ),
    '{processDataSet,modellingAndValidation,complianceDeclarations,compliance}',
    prepared.existing_compliance_items || prepared.comment_compliance_items,
    true
  )
  from prepared
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_apply_mv_payload(p_table text, p_id uuid, p_version text, p_review_items jsonb DEFAULT '[]'::jsonb, p_compliance_items jsonb DEFAULT '[]'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_row jsonb;
  v_doc jsonb;
  v_dataset_path text[];
  v_mv_path text[];
  v_validation_object_path text[];
  v_compliance_object_path text[];
  v_review_path text[];
  v_compliance_path text[];
  v_review_items jsonb := coalesce(p_review_items, '[]'::jsonb);
  v_compliance_items jsonb := coalesce(p_compliance_items, '[]'::jsonb);
begin
  if p_table not in ('processes', 'lifecyclemodels') then
    return public.cmd_review_get_dataset_row(p_table, p_id, p_version, false);
  end if;

  v_row := public.cmd_review_get_dataset_row(p_table, p_id, p_version, true);

  if v_row is null then
    return null;
  end if;

  if p_table = 'processes' then
    v_dataset_path := array['processDataSet'];
    v_mv_path := array['processDataSet', 'modellingAndValidation'];
    v_validation_object_path := array[
      'processDataSet',
      'modellingAndValidation',
      'validation'
    ];
    v_compliance_object_path := array[
      'processDataSet',
      'modellingAndValidation',
      'complianceDeclarations'
    ];
    v_review_path := array['processDataSet', 'modellingAndValidation', 'validation', 'review'];
    v_compliance_path := array[
      'processDataSet',
      'modellingAndValidation',
      'complianceDeclarations',
      'compliance'
    ];
  else
    v_dataset_path := array['lifeCycleModelDataSet'];
    v_mv_path := array['lifeCycleModelDataSet', 'modellingAndValidation'];
    v_validation_object_path := array[
      'lifeCycleModelDataSet',
      'modellingAndValidation',
      'validation'
    ];
    v_compliance_object_path := array[
      'lifeCycleModelDataSet',
      'modellingAndValidation',
      'complianceDeclarations'
    ];
    v_review_path := array[
      'lifeCycleModelDataSet',
      'modellingAndValidation',
      'validation',
      'review'
    ];
    v_compliance_path := array[
      'lifeCycleModelDataSet',
      'modellingAndValidation',
      'complianceDeclarations',
      'compliance'
    ];
  end if;

  v_doc := coalesce(v_row->'json_ordered', v_row->'json', '{}'::jsonb);
  v_doc := jsonb_set(
    v_doc,
    v_dataset_path,
    case
      when jsonb_typeof(v_doc #> v_dataset_path) = 'object'
        then v_doc #> v_dataset_path
      else '{}'::jsonb
    end,
    true
  );
  v_doc := jsonb_set(
    v_doc,
    v_mv_path,
    case
      when jsonb_typeof(v_doc #> v_mv_path) = 'object'
        then v_doc #> v_mv_path
      else '{}'::jsonb
    end,
    true
  );
  v_doc := jsonb_set(
    v_doc,
    v_validation_object_path,
    case
      when jsonb_typeof(v_doc #> v_validation_object_path) = 'object'
        then v_doc #> v_validation_object_path
      else '{}'::jsonb
    end,
    true
  );
  v_doc := jsonb_set(
    v_doc,
    v_compliance_object_path,
    case
      when jsonb_typeof(v_doc #> v_compliance_object_path) = 'object'
        then v_doc #> v_compliance_object_path
      else '{}'::jsonb
    end,
    true
  );

  if jsonb_array_length(v_review_items) > 0 then
    v_doc := jsonb_set(
      v_doc,
      v_review_path,
      public.cmd_review_json_array(v_doc #> v_review_path) || v_review_items,
      true
    );
  end if;

  if jsonb_array_length(v_compliance_items) > 0 then
    v_doc := jsonb_set(
      v_doc,
      v_compliance_path,
      public.cmd_review_json_array(v_doc #> v_compliance_path) || v_compliance_items,
      true
    );
  end if;

  execute format(
    'update public.%I
        set json_ordered = $1::json,
            json = $1::jsonb,
            modified_at = now()
      where id = $2
        and version = $3',
    p_table
  )
    using v_doc, p_id, p_version;

  return public.cmd_review_get_dataset_row(p_table, p_id, p_version, false);
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_approve(p_table text, p_review_id uuid, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_root_table text;
  v_root_targets jsonb;
  v_comment_ref_roots jsonb := '[]'::jsonb;
  v_target record;
  v_comment_ref record;
  v_review_items jsonb := '[]'::jsonb;
  v_compliance_items jsonb := '[]'::jsonb;
  v_root_row jsonb;
  v_updated_root_row jsonb;
  v_submodel_ids uuid[] := array[]::uuid[];
  v_submodel_id uuid;
  v_submodel_doc jsonb;
  v_affected_datasets jsonb := '[]'::jsonb;
  v_review_json jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if not public.cmd_review_is_review_admin(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_ADMIN_REQUIRED',
      'status', 403,
      'message', 'Only review admins can approve reviews'
    );
  end if;

  v_root_table := lower(coalesce(p_table, ''));
  if v_root_table not in ('processes', 'lifecyclemodels') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_TABLE',
      'status', 400,
      'message', 'table must be processes or lifecyclemodels'
    );
  end if;

  select *
    into v_review
  from public.reviews
  where id = p_review_id
  for update;

  if not found then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_NOT_FOUND',
      'status', 404,
      'message', 'Review not found'
    );
  end if;

  if v_review.state_code <> 1 then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409,
      'message', 'Only assigned reviews can be approved',
      'details', jsonb_build_object(
        'state_code', v_review.state_code
      )
    );
  end if;

  v_root_targets := jsonb_build_array(
    jsonb_build_object(
      'table', v_root_table,
      'id', v_review.data_id,
      'version', v_review.data_version,
      'is_root', true
    )
  );

  create temporary table if not exists cmd_review_approve_targets (
    table_name text not null,
    dataset_id uuid not null,
    dataset_version text not null,
    state_code integer not null,
    reviews jsonb,
    dataset_row jsonb not null,
    is_root boolean not null default false,
    primary key (table_name, dataset_id, dataset_version)
  ) on commit drop;

  truncate table cmd_review_approve_targets;

  insert into cmd_review_approve_targets (
    table_name,
    dataset_id,
    dataset_version,
    state_code,
    reviews,
    dataset_row,
    is_root
  )
  select
    table_name,
    dataset_id,
    dataset_version,
    state_code,
    reviews,
    dataset_row,
    is_root
  from public.cmd_review_collect_dataset_targets(v_root_targets, true);

  select coalesce(
    jsonb_agg(review_items.value),
    '[]'::jsonb
  )
    into v_review_items
  from public.comments as c
  cross join lateral jsonb_array_elements(
    public.cmd_review_json_array(to_jsonb(c.json)#>'{modellingAndValidation,validation,review}')
  ) as review_items(value)
  where c.review_id = p_review_id
    and c.state_code = 1;

  select coalesce(
    jsonb_agg(compliance_items.value),
    '[]'::jsonb
  )
    into v_compliance_items
  from public.comments as c
  cross join lateral jsonb_array_elements(
    public.cmd_review_json_array(
      to_jsonb(c.json)#>'{modellingAndValidation,complianceDeclarations,compliance}'
    )
  ) as compliance_items(value)
  where c.review_id = p_review_id
    and c.state_code = 1;

  for v_comment_ref in
    select distinct
      ref.ref_type,
      ref.ref_object_id,
      ref.ref_version
    from public.comments as c
    cross join lateral public.cmd_review_extract_refs(coalesce(to_jsonb(c.json), '{}'::jsonb)) as ref
    where c.review_id = p_review_id
      and c.state_code = 1
  loop
    v_comment_ref_roots := v_comment_ref_roots || jsonb_build_array(
      jsonb_build_object(
        'table', public.cmd_review_ref_type_to_table(v_comment_ref.ref_type),
        'id', v_comment_ref.ref_object_id,
        'version', v_comment_ref.ref_version,
        'is_root', false
      )
    );
  end loop;

  insert into cmd_review_approve_targets (
    table_name,
    dataset_id,
    dataset_version,
    state_code,
    reviews,
    dataset_row,
    is_root
  )
  select
    table_name,
    dataset_id,
    dataset_version,
    state_code,
    reviews,
    dataset_row,
    is_root
  from public.cmd_review_collect_dataset_targets(v_comment_ref_roots, true)
  on conflict (table_name, dataset_id, dataset_version) do nothing;

  select dataset_row
    into v_root_row
  from cmd_review_approve_targets
  where is_root
    and table_name = v_root_table
    and dataset_id = v_review.data_id
    and dataset_version = v_review.data_version
  limit 1;

  if v_root_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_TARGET_NOT_FOUND',
      'status', 404,
      'message', 'Review target dataset not found'
    );
  end if;

  if v_root_table = 'processes' then
    v_updated_root_row := public.cmd_review_apply_mv_payload(
      'processes',
      v_review.data_id,
      v_review.data_version,
      v_review_items,
      v_compliance_items
    );
  elsif v_root_table = 'lifecyclemodels' then
    select coalesce(
      array_agg((submodel.value->>'id')::uuid),
      array[]::uuid[]
    )
      into v_submodel_ids
    from jsonb_array_elements(coalesce(v_root_row->'json_tg'->'submodels', '[]'::jsonb))
         as submodel(value)
    where lower(coalesce(submodel.value->>'type', '')) = 'secondary'
      and (submodel.value->>'id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

    foreach v_submodel_id in array v_submodel_ids
    loop
      if not exists (
        select 1
        from cmd_review_approve_targets as t
        where t.table_name = 'processes'
          and t.dataset_id = v_submodel_id
          and t.dataset_version = v_review.data_version
      ) then
        return jsonb_build_object(
          'ok', false,
          'code', 'INVALID_PAYLOAD',
          'status', 400,
          'message', format(
            'Missing current process snapshot for submodel %s',
            v_submodel_id
          )
        );
      end if;
    end loop;

    v_updated_root_row := public.cmd_review_apply_mv_payload(
      'lifecyclemodels',
      v_review.data_id,
      v_review.data_version,
      v_review_items,
      v_compliance_items
    );

    foreach v_submodel_id in array v_submodel_ids
    loop
      select public.cmd_review_apply_model_validation_to_process_json(
        coalesce(t.dataset_row->'json_ordered', t.dataset_row->'json', '{}'::jsonb),
        coalesce(v_updated_root_row->'json_ordered', v_updated_root_row->'json', '{}'::jsonb),
        v_review_items,
        v_compliance_items
      )
        into v_submodel_doc
      from cmd_review_approve_targets as t
      where t.table_name = 'processes'
        and t.dataset_id = v_submodel_id
        and t.dataset_version = v_review.data_version
      limit 1;

      update public.processes
        set json_ordered = v_submodel_doc::json,
            json = v_submodel_doc,
            modified_at = now()
      where id = v_submodel_id
        and version = v_review.data_version;
    end loop;

    for v_target in
      select *
      from cmd_review_approve_targets
      where table_name = 'processes'
        and not (dataset_id = any(v_submodel_ids))
      order by dataset_id, dataset_version
    loop
      perform public.cmd_review_apply_mv_payload(
        'processes',
        v_target.dataset_id,
        v_target.dataset_version,
        v_review_items,
        v_compliance_items
      );
    end loop;
  end if;

  for v_target in
    select *
    from cmd_review_approve_targets
    where state_code < 100
      and state_code <> 200
    order by table_name, dataset_id, dataset_version
  loop
    execute format(
      'update public.%I
          set state_code = 100,
              modified_at = now()
        where id = $1
          and version = $2',
      v_target.table_name
    )
      using v_target.dataset_id, v_target.dataset_version;
  end loop;

  update public.comments
    set state_code = 2,
        modified_at = now()
  where review_id = p_review_id
    and state_code <> -2;

  v_review_json := public.cmd_review_append_log(
    coalesce(v_review.json, '{}'::jsonb),
    'approved',
    v_actor
  );

  update public.reviews
    set state_code = 2,
        json = v_review_json,
        modified_at = now()
  where id = p_review_id
  returning *
    into v_review;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'table', table_name,
        'id', dataset_id,
        'version', dataset_version,
        'state_code', 100
      )
      order by table_name, dataset_id, dataset_version
    ),
    '[]'::jsonb
  )
    into v_affected_datasets
  from cmd_review_approve_targets
  where state_code < 100
    and state_code <> 200;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_review_approve',
    v_actor,
    'reviews',
    p_review_id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'root_table', v_root_table,
      'affected_datasets', v_affected_datasets
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'review', to_jsonb(v_review),
      'affected_datasets', v_affected_datasets
    )
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_assign_reviewers(p_review_id uuid, p_reviewer_ids jsonb, p_deadline timestamp with time zone DEFAULT NULL::timestamp with time zone, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_reviewer_ids jsonb;
  v_review_json jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if not public.cmd_review_is_review_admin(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_ADMIN_REQUIRED',
      'status', 403,
      'message', 'Only review admins can assign reviewers'
    );
  end if;

  if coalesce(jsonb_typeof(p_reviewer_ids), 'null') <> 'array' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEWER_IDS',
      'status', 400,
      'message', 'reviewerIds must be an array of UUID strings'
    );
  end if;

  if exists (
    select 1
    from jsonb_array_elements_text(p_reviewer_ids) as reviewer_ids(value)
    where reviewer_ids.value !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEWER_IDS',
      'status', 400,
      'message', 'reviewerIds must contain valid UUID strings only'
    );
  end if;

  v_reviewer_ids := public.cmd_review_normalize_reviewer_ids(p_reviewer_ids);

  if jsonb_array_length(v_reviewer_ids) = 0 then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEWER_REQUIRED',
      'status', 400,
      'message', 'At least one reviewer is required'
    );
  end if;

  select *
    into v_review
  from public.reviews
  where id = p_review_id
  for update;

  if not found then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_NOT_FOUND',
      'status', 404,
      'message', 'Review not found'
    );
  end if;

  if v_review.state_code not in (-1, 0, 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409,
      'message', 'Reviewers can only be assigned for pending or rejected reviews',
      'details', jsonb_build_object(
        'state_code', v_review.state_code
      )
    );
  end if;

  create temporary table if not exists cmd_review_assignment_active_reviewers (
    reviewer_id uuid primary key
  ) on commit drop;

  truncate table cmd_review_assignment_active_reviewers;

  insert into cmd_review_assignment_active_reviewers (reviewer_id)
  select value::uuid
  from jsonb_array_elements_text(v_reviewer_ids) as reviewer_ids(value);

  update public.comments
    set state_code = -2,
        modified_at = now()
  where review_id = p_review_id
    and reviewer_id not in (
      select reviewer_id
      from cmd_review_assignment_active_reviewers
    )
    and state_code = 0;

  insert into public.comments (
    review_id,
    reviewer_id,
    state_code
  )
  select
    p_review_id,
    reviewer_id,
    0
  from cmd_review_assignment_active_reviewers
  on conflict (review_id, reviewer_id) do update
    set state_code = case
      when public.comments.state_code in (-2, -1) then 0
      else public.comments.state_code
    end,
        modified_at = now();

  v_review_json := public.cmd_review_append_log(
    coalesce(v_review.json, '{}'::jsonb),
    'assign_reviewers',
    v_actor,
    jsonb_strip_nulls(
      jsonb_build_object(
        'reviewer_ids', v_reviewer_ids,
        'deadline', case
          when p_deadline is null then null
          else to_jsonb(p_deadline)
        end
      )
    )
  );

  update public.reviews
    set reviewer_id = v_reviewer_ids,
        state_code = 1,
        deadline = p_deadline,
        json = v_review_json,
        modified_at = now()
  where id = p_review_id
  returning *
    into v_review;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_review_assign_reviewers',
    v_actor,
    'reviews',
    p_review_id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_strip_nulls(
      jsonb_build_object(
        'reviewer_ids', v_reviewer_ids,
        'deadline', case
          when p_deadline is null then null
          else to_jsonb(p_deadline)
        end
      )
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'review', to_jsonb(v_review)
    )
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_change_member_role(p_user_id uuid, p_role text DEFAULT NULL::text, p_action text DEFAULT 'set'::text, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_action text := lower(coalesce(p_action, 'set'));
  v_team_id uuid := '00000000-0000-0000-0000-000000000000'::uuid;
  v_existing_role text;
  v_role_row jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_user_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'USER_ID_REQUIRED',
      'status', 400,
      'message', 'userId is required'
    );
  end if;

  if not public.cmd_membership_is_review_admin(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'Only a review admin can manage review members'
    );
  end if;

  select role
    into v_existing_role
  from public.roles
  where user_id = p_user_id
    and team_id = v_team_id
  for update;

  if v_action = 'remove' then
    if v_existing_role is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'ROLE_NOT_FOUND',
        'status', 404,
        'message', 'Role not found'
      );
    end if;

    if p_user_id = v_actor or v_existing_role <> 'review-member' then
      return jsonb_build_object(
        'ok', false,
        'code', 'FORBIDDEN',
        'status', 403,
        'message', 'Only review-member rows can be removed'
      );
    end if;

    delete from public.roles
    where user_id = p_user_id
      and team_id = v_team_id;

    insert into public.command_audit_log (
      command,
      actor_user_id,
      target_table,
      target_id,
      target_version,
      payload
    )
    values (
      'cmd_review_change_member_role',
      v_actor,
      'roles',
      p_user_id,
      v_team_id::text,
      coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
        'action', 'remove'
      )
    );

    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'removed', true,
        'user_id', p_user_id,
        'team_id', v_team_id
      )
    );
  end if;

  if v_action <> 'set' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_ACTION',
      'status', 400,
      'message', 'Unsupported action'
    );
  end if;

  if p_role not in ('review-member', 'review-admin') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_ROLE',
      'status', 400,
      'message', 'Unsupported review role transition'
    );
  end if;

  if v_existing_role is null then
    if p_role <> 'review-member' then
      return jsonb_build_object(
        'ok', false,
        'code', 'INVALID_ROLE_STATE',
        'status', 409,
        'message', 'A new review member must start as review-member'
      );
    end if;

    insert into public.roles (
      user_id,
      team_id,
      role,
      modified_at
    )
    values (
      p_user_id,
      v_team_id,
      p_role,
      now()
    )
    returning to_jsonb(roles.*)
      into v_role_row;
  elsif v_existing_role in ('review-member', 'review-admin') then
    update public.roles
      set role = p_role,
          modified_at = now()
    where user_id = p_user_id
      and team_id = v_team_id
    returning to_jsonb(roles.*)
      into v_role_row;
  else
    return jsonb_build_object(
      'ok', false,
      'code', 'ROLE_CONFLICT',
      'status', 409,
      'message', 'The existing zero-team role belongs to another scope'
    );
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_review_change_member_role',
    v_actor,
    'roles',
    p_user_id,
    v_team_id::text,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'action', 'set',
      'role', p_role
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_role_row
  );
exception
  when unique_violation then
    return jsonb_build_object(
      'ok', false,
      'code', 'ROLE_CONFLICT',
      'status', 409,
      'message', 'The existing zero-team role belongs to another scope'
    );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_collect_dataset_targets(p_roots jsonb, p_lock boolean DEFAULT false)
 RETURNS TABLE(table_name text, dataset_id uuid, dataset_version text, state_code integer, reviews jsonb, dataset_row jsonb, is_root boolean)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_root jsonb;
  v_current record;
  v_current_row jsonb;
  v_current_state_code integer;
  v_ref record;
  v_ref_table text;
  v_submodel jsonb;
  v_paired_model_exists boolean;
  v_paired_process_exists boolean;
begin
  create temporary table if not exists cmd_review_collect_queue (
    table_name text not null,
    dataset_id uuid not null,
    dataset_version text not null,
    is_root boolean not null default false,
    primary key (table_name, dataset_id, dataset_version)
  ) on commit drop;

  create temporary table if not exists cmd_review_collect_targets (
    table_name text not null,
    dataset_id uuid not null,
    dataset_version text not null,
    state_code integer not null,
    reviews jsonb,
    dataset_row jsonb not null,
    is_root boolean not null default false,
    primary key (table_name, dataset_id, dataset_version)
  ) on commit drop;

  truncate table cmd_review_collect_queue;
  truncate table cmd_review_collect_targets;

  if jsonb_typeof(p_roots) <> 'array' then
    return;
  end if;

  for v_root in
    select value
    from jsonb_array_elements(p_roots)
  loop
    if lower(coalesce(v_root->>'table', '')) not in (
      'contacts',
      'sources',
      'unitgroups',
      'flowproperties',
      'flows',
      'processes',
      'lifecyclemodels'
    ) then
      continue;
    end if;

    if not (coalesce(v_root->>'id', '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') then
      continue;
    end if;

    if nullif(v_root->>'version', '') is null then
      continue;
    end if;

    insert into cmd_review_collect_queue (
      table_name,
      dataset_id,
      dataset_version,
      is_root
    )
    values (
      lower(v_root->>'table'),
      (v_root->>'id')::uuid,
      v_root->>'version',
      coalesce((v_root->>'is_root')::boolean, false)
    )
    on conflict do nothing;
  end loop;

  while exists (select 1 from cmd_review_collect_queue) loop
    select
      q.table_name,
      q.dataset_id,
      q.dataset_version,
      q.is_root
    into v_current
    from cmd_review_collect_queue as q
    order by q.is_root desc, q.table_name, q.dataset_id, q.dataset_version
    limit 1;

    delete from cmd_review_collect_queue as q
    where q.table_name = v_current.table_name
      and q.dataset_id = v_current.dataset_id
      and q.dataset_version = v_current.dataset_version;

    if exists (
      select 1
      from cmd_review_collect_targets as t
      where t.table_name = v_current.table_name
        and t.dataset_id = v_current.dataset_id
        and t.dataset_version = v_current.dataset_version
    ) then
      continue;
    end if;

    v_current_row := public.cmd_review_get_dataset_row(
      v_current.table_name,
      v_current.dataset_id,
      v_current.dataset_version,
      p_lock
    );

    if v_current_row is null then
      continue;
    end if;

    v_current_state_code := coalesce((v_current_row->>'state_code')::integer, 0);

    insert into cmd_review_collect_targets (
      table_name,
      dataset_id,
      dataset_version,
      state_code,
      reviews,
      dataset_row,
      is_root
    )
    values (
      v_current.table_name,
      v_current.dataset_id,
      v_current.dataset_version,
      v_current_state_code,
      v_current_row->'reviews',
      v_current_row,
      v_current.is_root
    )
    on conflict do nothing;

    if v_current_state_code >= 100 and not v_current.is_root then
      if v_current.table_name = 'processes' then
        v_paired_model_exists := public.cmd_review_get_dataset_row(
          'lifecyclemodels',
          v_current.dataset_id,
          v_current.dataset_version,
          false
        ) is not null;

        if v_paired_model_exists then
          insert into cmd_review_collect_queue (
            table_name,
            dataset_id,
            dataset_version,
            is_root
          )
          values (
            'lifecyclemodels',
            v_current.dataset_id,
            v_current.dataset_version,
            false
          )
          on conflict do nothing;
        end if;
      end if;

      continue;
    end if;

    for v_ref in (
      select *
      from public.cmd_review_extract_refs(coalesce(v_current_row->'json_ordered', '{}'::jsonb))
      union
      select *
      from public.cmd_review_extract_refs(coalesce(v_current_row->'json', '{}'::jsonb))
      union
      select *
      from public.cmd_review_extract_refs(coalesce(v_current_row->'json_tg', '{}'::jsonb))
    ) loop
      v_ref_table := public.cmd_review_ref_type_to_table(v_ref.ref_type);

      if v_ref_table is null then
        continue;
      end if;

      if v_ref_table = v_current.table_name
         and v_ref.ref_object_id = v_current.dataset_id
         and v_ref.ref_version = v_current.dataset_version then
        continue;
      end if;

      insert into cmd_review_collect_queue (
        table_name,
        dataset_id,
        dataset_version,
        is_root
      )
      values (
        v_ref_table,
        v_ref.ref_object_id,
        v_ref.ref_version,
        false
      )
      on conflict do nothing;
    end loop;

    if v_current.table_name = 'processes' and not v_current.is_root then
      v_paired_model_exists := public.cmd_review_get_dataset_row(
        'lifecyclemodels',
        v_current.dataset_id,
        v_current.dataset_version,
        false
      ) is not null;

      if v_paired_model_exists then
        insert into cmd_review_collect_queue (
          table_name,
          dataset_id,
          dataset_version,
          is_root
        )
        values (
          'lifecyclemodels',
          v_current.dataset_id,
          v_current.dataset_version,
          false
        )
        on conflict do nothing;
      end if;
    end if;

    if v_current.table_name = 'lifecyclemodels' then
      if v_current.is_root then
        v_paired_process_exists := public.cmd_review_get_dataset_row(
          'processes',
          v_current.dataset_id,
          v_current.dataset_version,
          false
        ) is not null;

        if v_paired_process_exists then
          insert into cmd_review_collect_queue (
            table_name,
            dataset_id,
            dataset_version,
            is_root
          )
          values (
            'processes',
            v_current.dataset_id,
            v_current.dataset_version,
            false
          )
          on conflict do nothing;
        end if;
      end if;

      for v_submodel in
        select value
        from jsonb_array_elements(coalesce(v_current_row->'json_tg'->'submodels', '[]'::jsonb))
      loop
        if lower(coalesce(v_submodel->>'type', '')) <> 'secondary' then
          continue;
        end if;

        if not ((v_submodel->>'id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') then
          continue;
        end if;

        insert into cmd_review_collect_queue (
          table_name,
          dataset_id,
          dataset_version,
          is_root
        )
        values (
          'processes',
          (v_submodel->>'id')::uuid,
          coalesce(nullif(v_submodel->>'version', ''), v_current.dataset_version),
          false
        )
        on conflict do nothing;
      end loop;
    end if;
  end loop;

  return query
  select
    t.table_name,
    t.dataset_id,
    t.dataset_version,
    t.state_code,
    t.reviews,
    t.dataset_row,
    t.is_root
  from cmd_review_collect_targets as t
  order by t.is_root desc, t.table_name, t.dataset_id, t.dataset_version;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_extract_refs(p_json jsonb)
 RETURNS TABLE(ref_type text, ref_object_id uuid, ref_version text)
 LANGUAGE sql
 STABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
  with recursive walk(value) as (
    select coalesce(p_json, '{}'::jsonb)
    union all
    select child.value
    from walk
    cross join lateral (
      select object_values.value
      from jsonb_each(
        case
          when jsonb_typeof(walk.value) = 'object' then walk.value
          else '{}'::jsonb
        end
      ) as object_values(key, value)
      union all
      select array_values.value
      from jsonb_array_elements(
        case
          when jsonb_typeof(walk.value) = 'array' then walk.value
          else '[]'::jsonb
        end
      ) as array_values(value)
    ) as child
  )
  select distinct
    lower(trim(value->>'@type')) as ref_type,
    (value->>'@refObjectId')::uuid as ref_object_id,
    value->>'@version' as ref_version
  from walk
  where jsonb_typeof(value) = 'object'
    and value ? '@refObjectId'
    and value ? '@version'
    and value ? '@type'
    and (value->>'@refObjectId') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    and nullif(value->>'@version', '') is not null
    and public.cmd_review_ref_type_to_table(value->>'@type') is not null
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_get_actor_meta(p_actor uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_meta jsonb;
  v_display_name text;
  v_email text;
begin
  select u.raw_user_meta_data
    into v_meta
  from public.users as u
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
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_get_dataset_name(p_table text, p_row jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select case p_table
    when 'contacts' then coalesce(
      p_row#>'{json,contactDataSet,contactInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,contactDataSet,contactInformation,dataSetInformation,name}',
      '{}'::jsonb
    )
    when 'sources' then coalesce(
      p_row#>'{json,sourceDataSet,sourceInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,sourceDataSet,sourceInformation,dataSetInformation,name}',
      '{}'::jsonb
    )
    when 'unitgroups' then coalesce(
      p_row#>'{json,unitGroupDataSet,unitGroupInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,unitGroupDataSet,unitGroupInformation,dataSetInformation,name}',
      '{}'::jsonb
    )
    when 'flowproperties' then coalesce(
      p_row#>'{json,flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,name}',
      '{}'::jsonb
    )
    when 'flows' then coalesce(
      p_row#>'{json,flowDataSet,flowInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,flowDataSet,flowInformation,dataSetInformation,name}',
      '{}'::jsonb
    )
    when 'processes' then coalesce(
      p_row#>'{json,processDataSet,processInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,processDataSet,processInformation,dataSetInformation,name}',
      '{}'::jsonb
    )
    when 'lifecyclemodels' then coalesce(
      p_row#>'{json,lifeCycleModelDataSet,lifeCycleModelInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,lifeCycleModelDataSet,lifeCycleModelInformation,dataSetInformation,name}',
      '{}'::jsonb
    )
    else '{}'::jsonb
  end
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_get_dataset_row(p_table text, p_id uuid, p_version text, p_lock boolean DEFAULT false)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_row jsonb;
begin
  if p_table not in (
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes',
    'lifecyclemodels'
  ) then
    return null;
  end if;

  execute format(
    'select to_jsonb(t) from public.%I as t where t.id = $1 and t.version = $2 %s',
    p_table,
    case when p_lock then 'for update of t' else '' end
  )
    into v_row
    using p_id, p_version;

  return v_row;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_get_root_table(p_review_json jsonb, p_data_id uuid, p_data_version text)
 RETURNS text
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_explicit text := lower(nullif(p_review_json#>>'{data,table}', ''));
  v_process_row jsonb;
  v_model_row jsonb;
  v_expected_name jsonb := coalesce(p_review_json#>'{data,name}', '{}'::jsonb);
begin
  if v_explicit in ('processes', 'lifecyclemodels') then
    return v_explicit;
  end if;

  v_process_row := public.cmd_review_get_dataset_row('processes', p_data_id, p_data_version, false);
  v_model_row := public.cmd_review_get_dataset_row(
    'lifecyclemodels',
    p_data_id,
    p_data_version,
    false
  );

  if v_model_row is not null
     and public.cmd_review_get_dataset_name('lifecyclemodels', v_model_row) = v_expected_name then
    return 'lifecyclemodels';
  end if;

  if v_process_row is not null
     and public.cmd_review_get_dataset_name('processes', v_process_row) = v_expected_name then
    return 'processes';
  end if;

  if v_model_row is not null and v_process_row is null then
    return 'lifecyclemodels';
  end if;

  if v_process_row is not null then
    return 'processes';
  end if;

  return null;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_is_review_admin(p_actor uuid DEFAULT auth.uid())
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select exists (
    select 1
    from public.roles
    where user_id = coalesce(p_actor, auth.uid())
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role = 'review-admin'
  )
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_json_array(p_value jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select case jsonb_typeof(p_value)
    when 'array' then coalesce(p_value, '[]'::jsonb)
    when 'object' then jsonb_build_array(p_value)
    when 'string' then jsonb_build_array(p_value)
    when 'number' then jsonb_build_array(p_value)
    when 'boolean' then jsonb_build_array(p_value)
    else '[]'::jsonb
  end
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_merge_compliance_declarations(p_existing jsonb, p_additions jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
  with normalized as (
    select
      case
        when jsonb_typeof(p_existing) = 'object' then p_existing
        else '{}'::jsonb
      end as existing_obj,
      case
        when jsonb_typeof(p_additions) = 'object' then p_additions
        else '{}'::jsonb
      end as additions_obj
  ),
  merged as (
    select
      existing_obj,
      additions_obj,
      existing_obj || (additions_obj - 'compliance') as base_obj
    from normalized
  )
  select case
    when additions_obj ? 'compliance' then
      jsonb_set(
        base_obj,
        '{compliance}',
        public.cmd_review_merge_json_collection(
          existing_obj->'compliance',
          additions_obj->'compliance'
        ),
        true
      )
    else base_obj
  end
  from merged
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_merge_json_collection(p_existing jsonb, p_additions jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select public.cmd_review_json_array(p_existing) || public.cmd_review_json_array(p_additions)
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_merge_validation(p_existing jsonb, p_additions jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
  with normalized as (
    select
      case
        when jsonb_typeof(p_existing) = 'object' then p_existing
        else '{}'::jsonb
      end as existing_obj,
      case
        when jsonb_typeof(p_additions) = 'object' then p_additions
        else '{}'::jsonb
      end as additions_obj
  ),
  merged as (
    select
      existing_obj,
      additions_obj,
      existing_obj || (additions_obj - 'review') as base_obj
    from normalized
  )
  select case
    when additions_obj ? 'review' then
      jsonb_set(
        base_obj,
        '{review}',
        public.cmd_review_merge_json_collection(existing_obj->'review', additions_obj->'review'),
        true
      )
    else base_obj
  end
  from merged
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_normalize_reviewer_ids(p_reviewer_ids jsonb)
 RETURNS jsonb
 LANGUAGE sql
 IMMUTABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
  with normalized as (
    select
      value,
      min(ordinality) as ordinality
    from jsonb_array_elements_text(
      case
        when jsonb_typeof(p_reviewer_ids) = 'array' then p_reviewer_ids
        else '[]'::jsonb
      end
    ) with ordinality as reviewer_ids(value, ordinality)
    where value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    group by value
  )
  select coalesce(
    jsonb_agg(to_jsonb(value) order by ordinality),
    '[]'::jsonb
  )
  from normalized
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_ref_type_to_table(p_ref_type text)
 RETURNS text
 LANGUAGE sql
 IMMUTABLE
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select case lower(trim(coalesce(p_ref_type, '')))
    when 'contact data set' then 'contacts'
    when 'source data set' then 'sources'
    when 'unit group data set' then 'unitgroups'
    when 'flow property data set' then 'flowproperties'
    when 'flow data set' then 'flows'
    when 'process data set' then 'processes'
    when 'lifecyclemodel data set' then 'lifecyclemodels'
    when 'lifecycle model data set' then 'lifecyclemodels'
    when 'lifecyclemodel dataset' then 'lifecyclemodels'
    else null
  end
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_reject(p_table text, p_review_id uuid, p_reason text, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_root_table text;
  v_root_targets jsonb;
  v_comment_ref_roots jsonb := '[]'::jsonb;
  v_target record;
  v_comment_ref record;
  v_review_json jsonb;
  v_affected_datasets jsonb := '[]'::jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if not public.cmd_review_is_review_admin(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_ADMIN_REQUIRED',
      'status', 403,
      'message', 'Only review admins can reject reviews'
    );
  end if;

  v_root_table := lower(coalesce(p_table, ''));
  if v_root_table not in ('processes', 'lifecyclemodels') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_TABLE',
      'status', 400,
      'message', 'table must be processes or lifecyclemodels'
    );
  end if;

  select *
    into v_review
  from public.reviews
  where id = p_review_id
  for update;

  if not found then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_NOT_FOUND',
      'status', 404,
      'message', 'Review not found'
    );
  end if;

  if v_review.state_code not in (-1, 0, 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409,
      'message', 'Only pending, assigned, or rejected reviews can be rejected',
      'details', jsonb_build_object(
        'state_code', v_review.state_code
      )
    );
  end if;

  v_root_targets := jsonb_build_array(
    jsonb_build_object(
      'table', v_root_table,
      'id', v_review.data_id,
      'version', v_review.data_version,
      'is_root', true
    )
  );

  create temporary table if not exists cmd_review_reject_targets (
    table_name text not null,
    dataset_id uuid not null,
    dataset_version text not null,
    state_code integer not null,
    reviews jsonb,
    dataset_row jsonb not null,
    is_root boolean not null default false,
    primary key (table_name, dataset_id, dataset_version)
  ) on commit drop;

  truncate table cmd_review_reject_targets;

  insert into cmd_review_reject_targets (
    table_name,
    dataset_id,
    dataset_version,
    state_code,
    reviews,
    dataset_row,
    is_root
  )
  select
    table_name,
    dataset_id,
    dataset_version,
    state_code,
    reviews,
    dataset_row,
    is_root
  from public.cmd_review_collect_dataset_targets(v_root_targets, true);

  for v_comment_ref in
    select distinct
      ref.ref_type,
      ref.ref_object_id,
      ref.ref_version
    from public.comments as c
    cross join lateral public.cmd_review_extract_refs(coalesce(to_jsonb(c.json), '{}'::jsonb)) as ref
    where c.review_id = p_review_id
      and c.state_code <> -2
  loop
    v_comment_ref_roots := v_comment_ref_roots || jsonb_build_array(
      jsonb_build_object(
        'table', public.cmd_review_ref_type_to_table(v_comment_ref.ref_type),
        'id', v_comment_ref.ref_object_id,
        'version', v_comment_ref.ref_version,
        'is_root', false
      )
    );
  end loop;

  insert into cmd_review_reject_targets (
    table_name,
    dataset_id,
    dataset_version,
    state_code,
    reviews,
    dataset_row,
    is_root
  )
  select
    table_name,
    dataset_id,
    dataset_version,
    state_code,
    reviews,
    dataset_row,
    is_root
  from public.cmd_review_collect_dataset_targets(v_comment_ref_roots, true)
  on conflict (table_name, dataset_id, dataset_version) do nothing;

  for v_target in
    select *
    from cmd_review_reject_targets
    where state_code >= 20
      and state_code < 100
    order by table_name, dataset_id, dataset_version
  loop
    execute format(
      'update public.%I
          set state_code = 0,
              modified_at = now()
        where id = $1
          and version = $2',
      v_target.table_name
    )
      using v_target.dataset_id, v_target.dataset_version;
  end loop;

  update public.comments
    set state_code = -1,
        modified_at = now()
  where review_id = p_review_id
    and state_code <> -2;

  v_review_json := coalesce(v_review.json, '{}'::jsonb);
  v_review_json := jsonb_set(
    v_review_json,
    '{comment}',
    coalesce(v_review_json->'comment', '{}'::jsonb) || jsonb_build_object(
      'message', coalesce(p_reason, '')
    ),
    true
  );
  v_review_json := public.cmd_review_append_log(
    v_review_json,
    'rejected',
    v_actor,
    jsonb_build_object(
      'reason', coalesce(p_reason, '')
    )
  );

  update public.reviews
    set state_code = -1,
        json = v_review_json,
        modified_at = now()
  where id = p_review_id
  returning *
    into v_review;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'table', table_name,
        'id', dataset_id,
        'version', dataset_version,
        'state_code', 0
      )
      order by table_name, dataset_id, dataset_version
    ),
    '[]'::jsonb
  )
    into v_affected_datasets
  from cmd_review_reject_targets
  where state_code >= 20
    and state_code < 100;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_review_reject',
    v_actor,
    'reviews',
    p_review_id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'root_table', v_root_table,
      'reason', coalesce(p_reason, ''),
      'affected_datasets', v_affected_datasets
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'review', to_jsonb(v_review),
      'affected_datasets', v_affected_datasets
    )
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_revoke_reviewer(p_review_id uuid, p_reviewer_id uuid, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_comment public.comments%rowtype;
  v_remaining_reviewer_ids jsonb;
  v_review_json jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if not public.cmd_review_is_review_admin(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_ADMIN_REQUIRED',
      'status', 403,
      'message', 'Only review admins can revoke reviewers'
    );
  end if;

  select *
    into v_review
  from public.reviews
  where id = p_review_id
  for update;

  if not found then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_NOT_FOUND',
      'status', 404,
      'message', 'Review not found'
    );
  end if;

  if v_review.state_code <> 1 then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409,
      'message', 'Reviewers can only be revoked from assigned reviews',
      'details', jsonb_build_object(
        'state_code', v_review.state_code
      )
    );
  end if;

  if not public.cmd_review_json_array(v_review.reviewer_id) @> jsonb_build_array(to_jsonb(p_reviewer_id::text)) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEWER_NOT_ASSIGNED',
      'status', 409,
      'message', 'Reviewer is not currently assigned to this review'
    );
  end if;

  select *
    into v_comment
  from public.comments
  where review_id = p_review_id
    and reviewer_id = p_reviewer_id
  for update;

  if found and v_comment.state_code <> 0 then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEWER_ALREADY_RESPONDED',
      'status', 409,
      'message', 'Only pending reviewers can be revoked',
      'details', jsonb_build_object(
        'comment_state_code', v_comment.state_code
      )
    );
  end if;

  select coalesce(
    jsonb_agg(to_jsonb(value) order by ordinality),
    '[]'::jsonb
  )
    into v_remaining_reviewer_ids
  from jsonb_array_elements_text(public.cmd_review_json_array(v_review.reviewer_id))
       with ordinality as reviewer_ids(value, ordinality)
  where reviewer_ids.value <> p_reviewer_id::text;

  v_review_json := public.cmd_review_append_log(
    coalesce(v_review.json, '{}'::jsonb),
    'revoke_reviewer',
    v_actor,
    jsonb_build_object(
      'reviewer_id', p_reviewer_id
    )
  );

  update public.reviews
    set reviewer_id = v_remaining_reviewer_ids,
        state_code = case
          when jsonb_array_length(v_remaining_reviewer_ids) = 0 then 0
          else 1
        end,
        json = v_review_json,
        modified_at = now()
  where id = p_review_id
  returning *
    into v_review;

  update public.comments
    set state_code = -2,
        modified_at = now()
  where review_id = p_review_id
    and reviewer_id = p_reviewer_id;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_review_revoke_reviewer',
    v_actor,
    'reviews',
    p_review_id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'reviewer_id', p_reviewer_id,
      'remaining_reviewer_ids', v_remaining_reviewer_ids
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'review', to_jsonb(v_review)
    )
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_save_assignment_draft(p_review_id uuid, p_reviewer_ids jsonb, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_reviewer_ids jsonb;
  v_review_json jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if not public.cmd_review_is_review_admin(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_ADMIN_REQUIRED',
      'status', 403,
      'message', 'Only review admins can manage reviewer assignments'
    );
  end if;

  if coalesce(jsonb_typeof(p_reviewer_ids), 'null') not in ('null', 'array') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEWER_IDS',
      'status', 400,
      'message', 'reviewerIds must be an array of UUID strings'
    );
  end if;

  if exists (
    select 1
    from jsonb_array_elements_text(coalesce(p_reviewer_ids, '[]'::jsonb)) as reviewer_ids(value)
    where reviewer_ids.value !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEWER_IDS',
      'status', 400,
      'message', 'reviewerIds must contain valid UUID strings only'
    );
  end if;

  v_reviewer_ids := public.cmd_review_normalize_reviewer_ids(coalesce(p_reviewer_ids, '[]'::jsonb));

  select *
    into v_review
  from public.reviews
  where id = p_review_id
  for update;

  if not found then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_NOT_FOUND',
      'status', 404,
      'message', 'Review not found'
    );
  end if;

  if v_review.state_code not in (-1, 0, 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409,
      'message', 'Reviewer assignments can only be changed for unassigned or active reviews',
      'details', jsonb_build_object(
        'state_code', v_review.state_code
      )
    );
  end if;

  v_review_json := public.cmd_review_append_log(
    coalesce(v_review.json, '{}'::jsonb),
    'assign_reviewers_temporary',
    v_actor,
    jsonb_build_object(
      'reviewer_ids', v_reviewer_ids
    )
  );

  update public.reviews
    set reviewer_id = v_reviewer_ids,
        json = v_review_json,
        modified_at = now()
  where id = p_review_id
  returning *
    into v_review;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_review_save_assignment_draft',
    v_actor,
    'reviews',
    p_review_id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'reviewer_ids', v_reviewer_ids
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'review', to_jsonb(v_review)
    )
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_save_comment_draft(p_review_id uuid, p_json jsonb, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_comment public.comments%rowtype;
  v_comment_json jsonb := coalesce(p_json, '{}'::jsonb);
  v_review_json jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if coalesce(jsonb_typeof(v_comment_json), 'null') <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_COMMENT_JSON',
      'status', 400,
      'message', 'comment json must be an object'
    );
  end if;

  select *
    into v_review
  from public.reviews
  where id = p_review_id
  for update;

  if not found then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_NOT_FOUND',
      'status', 404,
      'message', 'Review not found'
    );
  end if;

  if v_review.state_code not in (-1, 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409,
      'message', 'Review comments can only be edited for assigned or rejected reviews',
      'details', jsonb_build_object(
        'state_code', v_review.state_code
      )
    );
  end if;

  if not public.cmd_review_json_array(v_review.reviewer_id) @> jsonb_build_array(to_jsonb(v_actor::text)) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEWER_REQUIRED',
      'status', 403,
      'message', 'Only assigned reviewers can edit review comments'
    );
  end if;

  select *
    into v_comment
  from public.comments
  where review_id = p_review_id
    and reviewer_id = v_actor
  for update;

  if found and v_comment.state_code in (-2, 2) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_COMMENT_STATE',
      'status', 409,
      'message', 'This reviewer comment can no longer be edited',
      'details', jsonb_build_object(
        'state_code', v_comment.state_code
      )
    );
  end if;

  if not found then
    insert into public.comments (
      review_id,
      reviewer_id,
      json,
      state_code
    )
    values (
      p_review_id,
      v_actor,
      v_comment_json::json,
      case
        when v_review.state_code = -1 then -1
        else 0
      end
    )
    returning *
      into v_comment;
  else
    update public.comments
      set json = v_comment_json::json,
          modified_at = now()
    where review_id = p_review_id
      and reviewer_id = v_actor
    returning *
      into v_comment;
  end if;

  v_review_json := public.cmd_review_append_log(
    coalesce(v_review.json, '{}'::jsonb),
    'submit_comments_temporary',
    v_actor,
    jsonb_build_object(
      'reviewer_id', v_actor
    )
  );

  update public.reviews
    set json = v_review_json,
        modified_at = now()
  where id = p_review_id
  returning *
    into v_review;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_review_save_comment_draft',
    v_actor,
    'reviews',
    p_review_id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'reviewer_id', v_actor,
      'comment_state_code', v_comment.state_code
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'review', to_jsonb(v_review),
      'comment', to_jsonb(v_comment)
    )
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_submit(p_table text, p_id uuid, p_version text, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_current record;
  v_current_row jsonb;
  v_current_state_code integer;
  v_conflicting_version text;
  v_conflicting_state integer;
  v_root_row jsonb;
  v_root_owner_id uuid;
  v_review_id uuid := gen_random_uuid();
  v_review_record public.reviews%rowtype;
  v_review_json jsonb;
  v_review_row jsonb;
  v_team_name jsonb;
  v_user_meta jsonb;
  v_ref record;
  v_ref_table text;
  v_submodel jsonb;
  v_paired_process_exists boolean;
  v_paired_model_exists boolean;
  v_affected_datasets jsonb := '[]'::jsonb;
  v_updated_reviews jsonb;
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
    'processes',
    'lifecyclemodels'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Unsupported dataset table for review submission'
    );
  end if;

  create temporary table if not exists cmd_review_submit_queue (
    table_name text not null,
    dataset_id uuid not null,
    dataset_version text not null,
    is_root boolean not null default false,
    primary key (table_name, dataset_id, dataset_version)
  ) on commit drop;

  create temporary table if not exists cmd_review_submit_targets (
    table_name text not null,
    dataset_id uuid not null,
    dataset_version text not null,
    state_code integer not null,
    reviews jsonb,
    primary key (table_name, dataset_id, dataset_version)
  ) on commit drop;

  truncate table cmd_review_submit_queue;
  truncate table cmd_review_submit_targets;

  insert into cmd_review_submit_queue (
    table_name,
    dataset_id,
    dataset_version,
    is_root
  )
  values (
    p_table,
    p_id,
    p_version,
    true
  )
  on conflict do nothing;

  while exists (select 1 from cmd_review_submit_queue) loop
    select
      table_name,
      dataset_id,
      dataset_version,
      is_root
    into v_current
    from cmd_review_submit_queue
    order by is_root desc, table_name, dataset_id, dataset_version
    limit 1;

    delete from cmd_review_submit_queue
    where table_name = v_current.table_name
      and dataset_id = v_current.dataset_id
      and dataset_version = v_current.dataset_version;

    if exists (
      select 1
      from cmd_review_submit_targets
      where table_name = v_current.table_name
        and dataset_id = v_current.dataset_id
        and dataset_version = v_current.dataset_version
    ) then
      continue;
    end if;

    v_current_row := public.cmd_review_get_dataset_row(
      v_current.table_name,
      v_current.dataset_id,
      v_current.dataset_version,
      true
    );

    if v_current_row is null then
      if v_current.is_root then
        return jsonb_build_object(
          'ok', false,
          'code', 'DATASET_NOT_FOUND',
          'status', 404,
          'message', 'Dataset not found'
        );
      end if;

      return jsonb_build_object(
        'ok', false,
        'code', 'REFERENCED_DATASET_NOT_FOUND',
        'status', 409,
        'message', 'Referenced dataset not found',
        'details', jsonb_build_object(
          'table', v_current.table_name,
          'id', v_current.dataset_id,
          'version', v_current.dataset_version
        )
      );
    end if;

    v_current_state_code := coalesce((v_current_row->>'state_code')::integer, 0);

    if v_current.is_root then
      v_root_row := v_current_row;
      v_root_owner_id := nullif(v_current_row->>'user_id', '')::uuid;

      if v_root_owner_id is distinct from v_actor then
        return jsonb_build_object(
          'ok', false,
          'code', 'DATASET_OWNER_REQUIRED',
          'status', 403,
          'message', 'Only the dataset owner can submit review'
        );
      end if;

      if v_current_state_code >= 100 then
        return jsonb_build_object(
          'ok', false,
          'code', 'DATA_ALREADY_PUBLISHED',
          'status', 403,
          'message', 'Published data cannot be submitted for review',
          'details', jsonb_build_object(
            'state_code', v_current_state_code
          )
        );
      end if;

      if v_current_state_code >= 20 then
        return jsonb_build_object(
          'ok', false,
          'code', 'DATA_UNDER_REVIEW',
          'status', 403,
          'message', 'Data is already under review',
          'details', jsonb_build_object(
            'state_code', 20,
            'review_state_code', v_current_state_code
          )
        );
      end if;
    else
      if v_current_state_code >= 20 and v_current_state_code < 100 then
        return jsonb_build_object(
          'ok', false,
          'code', 'REFERENCED_DATA_UNDER_REVIEW',
          'status', 409,
          'message', 'Referenced data is already under review',
          'details', jsonb_build_object(
            'table', v_current.table_name,
            'id', v_current.dataset_id,
            'version', v_current.dataset_version,
            'state_code', 20,
            'review_state_code', v_current_state_code
          )
        );
      end if;

      if v_current_state_code >= 100 then
        if v_current.table_name = 'processes' then
          v_paired_model_exists := public.cmd_review_get_dataset_row(
            'lifecyclemodels',
            v_current.dataset_id,
            v_current.dataset_version,
            false
          ) is not null;

          if v_paired_model_exists then
            insert into cmd_review_submit_queue (
              table_name,
              dataset_id,
              dataset_version,
              is_root
            )
            values (
              'lifecyclemodels',
              v_current.dataset_id,
              v_current.dataset_version,
              false
            )
            on conflict do nothing;
          end if;
        end if;

        continue;
      end if;
    end if;

    execute format(
      'select version, state_code
         from public.%I
        where id = $1
          and version <> $2
          and state_code >= 20
          and state_code < 100
        order by version desc
        limit 1',
      v_current.table_name
    )
      into v_conflicting_version, v_conflicting_state
      using v_current.dataset_id, v_current.dataset_version;

    if v_conflicting_version is not null then
      return jsonb_build_object(
        'ok', false,
        'code', case
          when v_current.is_root then 'DATASET_VERSION_UNDER_REVIEW'
          else 'REFERENCED_VERSION_UNDER_REVIEW'
        end,
        'status', case
          when v_current.is_root then 403
          else 409
        end,
        'message', case
          when v_current.is_root then 'Another version of this dataset is already under review'
          else 'Another version of a referenced dataset is already under review'
        end,
        'details', jsonb_build_object(
          'table', v_current.table_name,
          'id', v_current.dataset_id,
          'version', v_current.dataset_version,
          'under_review_version', v_conflicting_version,
          'state_code', 20,
          'review_state_code', v_conflicting_state
        )
      );
    end if;

    execute format(
      'select version
         from public.%I
        where id = $1
          and version > $2
          and state_code = 100
        order by version desc
        limit 1',
      v_current.table_name
    )
      into v_conflicting_version
      using v_current.dataset_id, v_current.dataset_version;

    if v_conflicting_version is not null then
      return jsonb_build_object(
        'ok', false,
        'code', case
          when v_current.is_root then 'DATASET_VERSION_ALREADY_PUBLISHED'
          else 'REFERENCED_VERSION_ALREADY_PUBLISHED'
        end,
        'status', case
          when v_current.is_root then 403
          else 409
        end,
        'message', case
          when v_current.is_root then 'A newer published version of this dataset already exists'
          else 'A newer published version of a referenced dataset already exists'
        end,
        'details', jsonb_build_object(
          'table', v_current.table_name,
          'id', v_current.dataset_id,
          'version', v_current.dataset_version,
          'published_version', v_conflicting_version,
          'state_code', 100
        )
      );
    end if;

    insert into cmd_review_submit_targets (
      table_name,
      dataset_id,
      dataset_version,
      state_code,
      reviews
    )
    values (
      v_current.table_name,
      v_current.dataset_id,
      v_current.dataset_version,
      v_current_state_code,
      v_current_row->'reviews'
    )
    on conflict do nothing;

    for v_ref in (
      select *
      from public.cmd_review_extract_refs(coalesce(v_current_row->'json_ordered', '{}'::jsonb))
      union
      select *
      from public.cmd_review_extract_refs(coalesce(v_current_row->'json', '{}'::jsonb))
      union
      select *
      from public.cmd_review_extract_refs(coalesce(v_current_row->'json_tg', '{}'::jsonb))
    ) loop
      v_ref_table := public.cmd_review_ref_type_to_table(v_ref.ref_type);

      if v_ref_table is null then
        continue;
      end if;

      if v_ref_table = v_current.table_name
        and v_ref.ref_object_id = v_current.dataset_id
        and v_ref.ref_version = v_current.dataset_version then
        continue;
      end if;

      insert into cmd_review_submit_queue (
        table_name,
        dataset_id,
        dataset_version,
        is_root
      )
      values (
        v_ref_table,
        v_ref.ref_object_id,
        v_ref.ref_version,
        false
      )
      on conflict do nothing;
    end loop;

    if v_current.table_name = 'processes' and not v_current.is_root then
      v_paired_model_exists := public.cmd_review_get_dataset_row(
        'lifecyclemodels',
        v_current.dataset_id,
        v_current.dataset_version,
        false
      ) is not null;

      if v_paired_model_exists then
        insert into cmd_review_submit_queue (
          table_name,
          dataset_id,
          dataset_version,
          is_root
        )
        values (
          'lifecyclemodels',
          v_current.dataset_id,
          v_current.dataset_version,
          false
        )
        on conflict do nothing;
      end if;
    end if;

    if v_current.table_name = 'lifecyclemodels' then
      if v_current.is_root then
        v_paired_process_exists := public.cmd_review_get_dataset_row(
          'processes',
          v_current.dataset_id,
          v_current.dataset_version,
          false
        ) is not null;

        if v_paired_process_exists then
          insert into cmd_review_submit_queue (
            table_name,
            dataset_id,
            dataset_version,
            is_root
          )
          values (
            'processes',
            v_current.dataset_id,
            v_current.dataset_version,
            false
          )
          on conflict do nothing;
        end if;
      end if;

      for v_submodel in
        select value
        from jsonb_array_elements(coalesce(v_current_row->'json_tg'->'submodels', '[]'::jsonb))
      loop
        if coalesce(v_submodel->>'type', '') <> 'secondary' then
          continue;
        end if;

        if not ((v_submodel->>'id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$') then
          continue;
        end if;

        insert into cmd_review_submit_queue (
          table_name,
          dataset_id,
          dataset_version,
          is_root
        )
        values (
          'processes',
          (v_submodel->>'id')::uuid,
          coalesce(nullif(v_submodel->>'version', ''), v_current.dataset_version),
          false
        )
        on conflict do nothing;
      end loop;
    end if;
  end loop;

  select coalesce(t.json->'title', t.json->'name')
    into v_team_name
  from public.teams as t
  where t.id = nullif(v_root_row->>'team_id', '')::uuid;

  select u.raw_user_meta_data
    into v_user_meta
  from public.users as u
  where u.id = v_actor;

  v_review_json := jsonb_build_object(
    'data', jsonb_build_object(
      'id', p_id,
      'version', p_version,
      'name', public.cmd_review_get_dataset_name(p_table, v_root_row)
    ),
    'team', jsonb_build_object(
      'id', nullif(v_root_row->>'team_id', ''),
      'name', v_team_name
    ),
    'user', jsonb_build_object(
      'id', v_actor,
      'name', coalesce(nullif(v_user_meta->>'display_name', ''), nullif(v_user_meta->>'email', '')),
      'email', nullif(v_user_meta->>'email', '')
    ),
    'comment', jsonb_build_object(
      'message', ''
    ),
    'logs', jsonb_build_array(
      jsonb_build_object(
        'action', 'submit_review',
        'time', to_jsonb(now()),
        'user', jsonb_build_object(
          'id', v_actor,
          'display_name', coalesce(nullif(v_user_meta->>'display_name', ''), nullif(v_user_meta->>'email', ''))
        )
      )
    )
  );

  insert into public.reviews (
    id,
    data_id,
    data_version,
    state_code,
    reviewer_id,
    json
  )
  values (
    v_review_id,
    p_id,
    p_version,
    0,
    '[]'::jsonb,
    v_review_json
  )
  returning *
    into v_review_record;

  for v_current in
    select
      table_name,
      dataset_id,
      dataset_version,
      reviews
    from cmd_review_submit_targets
    order by table_name, dataset_id, dataset_version
  loop
    v_updated_reviews := public.cmd_review_append_review_ref(v_current.reviews, v_review_id);

    execute format(
      'update public.%I
          set state_code = 20,
              reviews = $1
        where id = $2
          and version = $3',
      v_current.table_name
    )
      using v_updated_reviews, v_current.dataset_id, v_current.dataset_version;
  end loop;

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'table', table_name,
        'id', dataset_id,
        'version', dataset_version,
        'state_code', 20
      )
      order by table_name, dataset_id, dataset_version
    ),
    '[]'::jsonb
  )
    into v_affected_datasets
  from cmd_review_submit_targets;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_review_submit',
    v_actor,
    p_table,
    p_id,
    p_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'review_id', v_review_id,
      'affected_datasets', v_affected_datasets
    )
  );

  v_review_row := to_jsonb(v_review_record);

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'review', v_review_row,
      'affected_datasets', v_affected_datasets
    )
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_submit_comment(p_review_id uuid, p_json jsonb, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select public.cmd_review_submit_comment(
    p_review_id,
    p_json,
    1,
    p_audit
  )
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_review_submit_comment(p_review_id uuid, p_json jsonb, p_comment_state integer DEFAULT 1, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_comment public.comments%rowtype;
  v_comment_json jsonb := coalesce(p_json, '{}'::jsonb);
  v_review_json jsonb;
  v_ref record;
  v_ref_table text;
  v_ref_roots jsonb := '[]'::jsonb;
  v_target record;
  v_affected_datasets jsonb := '[]'::jsonb;
  v_action text;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_comment_state not in (-3, 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_COMMENT_STATE',
      'status', 400,
      'message', 'commentState must be 1 or -3'
    );
  end if;

  if coalesce(jsonb_typeof(v_comment_json), 'null') <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_COMMENT_JSON',
      'status', 400,
      'message', 'comment json must be an object'
    );
  end if;

  select *
    into v_review
  from public.reviews
  where id = p_review_id
  for update;

  if not found then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_NOT_FOUND',
      'status', 404,
      'message', 'Review not found'
    );
  end if;

  if v_review.state_code not in (-1, 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409,
      'message', 'Review comments can only be submitted for assigned or rejected reviews',
      'details', jsonb_build_object(
        'state_code', v_review.state_code
      )
    );
  end if;

  if not public.cmd_review_json_array(v_review.reviewer_id) @> jsonb_build_array(to_jsonb(v_actor::text)) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEWER_REQUIRED',
      'status', 403,
      'message', 'Only assigned reviewers can submit review comments'
    );
  end if;

  select *
    into v_comment
  from public.comments
  where review_id = p_review_id
    and reviewer_id = v_actor
  for update;

  if found and v_comment.state_code in (-2, 2) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_COMMENT_STATE',
      'status', 409,
      'message', 'This reviewer comment can no longer be submitted',
      'details', jsonb_build_object(
        'state_code', v_comment.state_code
      )
    );
  end if;

  if not found then
    insert into public.comments (
      review_id,
      reviewer_id,
      state_code
    )
    values (
      p_review_id,
      v_actor,
      case
        when v_review.state_code = -1 then -1
        else 0
      end
    )
    returning *
      into v_comment;
  end if;

  if p_comment_state = 1 then
    for v_ref in
      select *
      from public.cmd_review_extract_refs(v_comment_json)
    loop
      v_ref_table := public.cmd_review_ref_type_to_table(v_ref.ref_type);

      if v_ref_table is null then
        continue;
      end if;

      v_ref_roots := v_ref_roots || jsonb_build_array(
        jsonb_build_object(
          'table', v_ref_table,
          'id', v_ref.ref_object_id,
          'version', v_ref.ref_version,
          'is_root', false
        )
      );
    end loop;

    create temporary table if not exists cmd_review_submit_comment_targets (
      table_name text not null,
      dataset_id uuid not null,
      dataset_version text not null,
      state_code integer not null,
      reviews jsonb,
      dataset_row jsonb not null,
      is_root boolean not null default false,
      primary key (table_name, dataset_id, dataset_version)
    ) on commit drop;

    truncate table cmd_review_submit_comment_targets;

    insert into cmd_review_submit_comment_targets (
      table_name,
      dataset_id,
      dataset_version,
      state_code,
      reviews,
      dataset_row,
      is_root
    )
    select
      table_name,
      dataset_id,
      dataset_version,
      state_code,
      reviews,
      dataset_row,
      is_root
    from public.cmd_review_collect_dataset_targets(v_ref_roots, true);

    for v_target in
      select *
      from cmd_review_submit_comment_targets
      where state_code >= 20
        and state_code < 100
      order by table_name, dataset_id, dataset_version
    loop
      return jsonb_build_object(
        'ok', false,
        'code', 'REFERENCED_DATA_UNDER_REVIEW',
        'status', 409,
        'message', 'Referenced data is already under review',
        'details', jsonb_build_object(
          'table', v_target.table_name,
          'id', v_target.dataset_id,
          'version', v_target.dataset_version,
          'state_code', 20,
          'review_state_code', v_target.state_code
        )
      );
    end loop;

    for v_target in
      select *
      from cmd_review_submit_comment_targets
      where state_code < 20
      order by table_name, dataset_id, dataset_version
    loop
      execute format(
        'update public.%I
            set state_code = 20,
                reviews = $1,
                modified_at = now()
          where id = $2
            and version = $3',
        v_target.table_name
      )
        using public.cmd_review_append_review_ref(v_target.reviews, p_review_id),
              v_target.dataset_id,
              v_target.dataset_version;
    end loop;

    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'table', table_name,
          'id', dataset_id,
          'version', dataset_version,
          'state_code', 20
        )
        order by table_name, dataset_id, dataset_version
      ),
      '[]'::jsonb
    )
      into v_affected_datasets
    from cmd_review_submit_comment_targets
    where state_code < 20;
  end if;

  update public.comments
    set json = v_comment_json::json,
        state_code = p_comment_state,
        modified_at = now()
  where review_id = p_review_id
    and reviewer_id = v_actor
  returning *
    into v_comment;

  v_action := case
    when p_comment_state = -3 then 'reviewer_rejected'
    else 'submit_comments'
  end;

  v_review_json := public.cmd_review_append_log(
    coalesce(v_review.json, '{}'::jsonb),
    v_action,
    v_actor,
    jsonb_build_object(
      'reviewer_id', v_actor,
      'comment_state_code', p_comment_state
    )
  );

  update public.reviews
    set state_code = case
          when p_comment_state = 1 and state_code = -1 then 1
          else state_code
        end,
        json = v_review_json,
        modified_at = now()
  where id = p_review_id
  returning *
    into v_review;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_review_submit_comment',
    v_actor,
    'reviews',
    p_review_id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'reviewer_id', v_actor,
      'comment_state_code', p_comment_state,
      'affected_datasets', v_affected_datasets
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'review', to_jsonb(v_review),
      'comment', to_jsonb(v_comment),
      'affected_datasets', v_affected_datasets
    )
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_system_change_member_role(p_user_id uuid, p_role text DEFAULT NULL::text, p_action text DEFAULT 'set'::text, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_action text := lower(coalesce(p_action, 'set'));
  v_team_id uuid := '00000000-0000-0000-0000-000000000000'::uuid;
  v_actor_is_owner boolean;
  v_actor_is_manager boolean;
  v_existing_role text;
  v_role_row jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_user_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'USER_ID_REQUIRED',
      'status', 400,
      'message', 'userId is required'
    );
  end if;

  v_actor_is_owner := public.cmd_membership_is_system_owner(v_actor);
  v_actor_is_manager := public.cmd_membership_is_system_manager(v_actor);

  if not v_actor_is_manager then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'The actor cannot manage system members'
    );
  end if;

  select role
    into v_existing_role
  from public.roles
  where user_id = p_user_id
    and team_id = v_team_id
  for update;

  if v_action = 'remove' then
    if v_existing_role is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'ROLE_NOT_FOUND',
        'status', 404,
        'message', 'Role not found'
      );
    end if;

    if p_user_id = v_actor or v_existing_role = 'owner' then
      return jsonb_build_object(
        'ok', false,
        'code', 'FORBIDDEN',
        'status', 403,
        'message', 'The actor cannot remove this system member'
      );
    end if;

    delete from public.roles
    where user_id = p_user_id
      and team_id = v_team_id;

    insert into public.command_audit_log (
      command,
      actor_user_id,
      target_table,
      target_id,
      target_version,
      payload
    )
    values (
      'cmd_system_change_member_role',
      v_actor,
      'roles',
      p_user_id,
      v_team_id::text,
      coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
        'action', 'remove'
      )
    );

    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'removed', true,
        'user_id', p_user_id,
        'team_id', v_team_id
      )
    );
  end if;

  if v_action <> 'set' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_ACTION',
      'status', 400,
      'message', 'Unsupported action'
    );
  end if;

  if p_role not in ('member', 'admin') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_ROLE',
      'status', 400,
      'message', 'Unsupported system role transition'
    );
  end if;

  if p_role = 'admin' and not v_actor_is_owner then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'Only the system owner can assign admin roles'
    );
  end if;

  if p_role = 'member' and v_existing_role = 'admin' and not v_actor_is_owner then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'Only the system owner can demote an admin'
    );
  end if;

  if v_existing_role is null then
    insert into public.roles (
      user_id,
      team_id,
      role,
      modified_at
    )
    values (
      p_user_id,
      v_team_id,
      p_role,
      now()
    )
    returning to_jsonb(roles.*)
      into v_role_row;
  elsif v_existing_role in ('owner', 'admin', 'member') then
    if v_existing_role = 'owner' then
      return jsonb_build_object(
        'ok', false,
        'code', 'FORBIDDEN',
        'status', 403,
        'message', 'The owner role cannot be modified'
      );
    end if;

    update public.roles
      set role = p_role,
          modified_at = now()
    where user_id = p_user_id
      and team_id = v_team_id
    returning to_jsonb(roles.*)
      into v_role_row;
  else
    return jsonb_build_object(
      'ok', false,
      'code', 'ROLE_CONFLICT',
      'status', 409,
      'message', 'The existing zero-team role belongs to another scope'
    );
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_system_change_member_role',
    v_actor,
    'roles',
    p_user_id,
    v_team_id::text,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'action', 'set',
      'role', p_role
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_role_row
  );
exception
  when unique_violation then
    return jsonb_build_object(
      'ok', false,
      'code', 'ROLE_CONFLICT',
      'status', 409,
      'message', 'The existing zero-team role belongs to another scope'
    );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_team_accept_invitation(p_team_id uuid, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_role_row jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if not public.policy_roles_update(v_actor, p_team_id, 'member') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVITATION_NOT_FOUND',
      'status', 404,
      'message', 'No matching invitation was found for the actor'
    );
  end if;

  update public.roles
    set role = 'member',
        modified_at = now()
  where user_id = v_actor
    and team_id = p_team_id
    and role = 'is_invited'
  returning to_jsonb(roles.*)
    into v_role_row;

  if v_role_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVITATION_NOT_FOUND',
      'status', 404,
      'message', 'No matching invitation was found for the actor'
    );
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_team_accept_invitation',
    v_actor,
    'roles',
    v_actor,
    p_team_id::text,
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_role_row
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_team_change_member_role(p_team_id uuid, p_user_id uuid, p_role text DEFAULT NULL::text, p_action text DEFAULT 'set'::text, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_action text := lower(coalesce(p_action, 'set'));
  v_existing_role text;
  v_role_row jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_team_id is null or p_user_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_PAYLOAD',
      'status', 400,
      'message', 'teamId and userId are required'
    );
  end if;

  if p_team_id = '00000000-0000-0000-0000-000000000000'::uuid then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_TEAM_SCOPE',
      'status', 400,
      'message', 'Use system or review member commands for the zero team scope'
    );
  end if;

  select role
    into v_existing_role
  from public.roles
  where user_id = p_user_id
    and team_id = p_team_id
  for update;

  if v_action = 'remove' then
    if v_existing_role is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'ROLE_NOT_FOUND',
        'status', 404,
        'message', 'Role not found'
      );
    end if;

    if not public.policy_roles_delete(p_user_id, p_team_id, v_existing_role) then
      return jsonb_build_object(
        'ok', false,
        'code', 'FORBIDDEN',
        'status', 403,
        'message', 'The actor cannot remove this team member'
      );
    end if;

    delete from public.roles
    where user_id = p_user_id
      and team_id = p_team_id;

    insert into public.command_audit_log (
      command,
      actor_user_id,
      target_table,
      target_id,
      target_version,
      payload
    )
    values (
      'cmd_team_change_member_role',
      v_actor,
      'roles',
      p_user_id,
      p_team_id::text,
      coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
        'action', 'remove'
      )
    );

    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'removed', true,
        'user_id', p_user_id,
        'team_id', p_team_id
      )
    );
  end if;

  if v_action <> 'set' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_ACTION',
      'status', 400,
      'message', 'Unsupported action'
    );
  end if;

  if p_role = 'is_invited' then
    if v_existing_role = 'rejected' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REINVITE_REQUIRED',
        'status', 409,
        'message', 'Use the reinvite command for rejected members'
      );
    end if;

    if v_existing_role is not null then
      return jsonb_build_object(
        'ok', false,
        'code', 'TEAM_MEMBER_ALREADY_EXISTS',
        'status', 409,
        'message', 'The team membership already exists'
      );
    end if;

    if not public.policy_roles_insert(p_user_id, p_team_id, 'is_invited') then
      return jsonb_build_object(
        'ok', false,
        'code', 'FORBIDDEN',
        'status', 403,
        'message', 'The actor cannot invite this user to the team'
      );
    end if;

    insert into public.roles (
      user_id,
      team_id,
      role,
      modified_at
    )
    values (
      p_user_id,
      p_team_id,
      'is_invited',
      now()
    )
    returning to_jsonb(roles.*)
      into v_role_row;
  elsif p_role in ('admin', 'member') then
    if not public.cmd_membership_is_team_owner(v_actor, p_team_id) then
      return jsonb_build_object(
        'ok', false,
        'code', 'FORBIDDEN',
        'status', 403,
        'message', 'Only the team owner can change active member roles'
      );
    end if;

    if v_existing_role not in ('admin', 'member') then
      return jsonb_build_object(
        'ok', false,
        'code', 'INVALID_ROLE_STATE',
        'status', 409,
        'message', 'Only active team members can be promoted or demoted'
      );
    end if;

    update public.roles
      set role = p_role,
          modified_at = now()
    where user_id = p_user_id
      and team_id = p_team_id
    returning to_jsonb(roles.*)
      into v_role_row;
  else
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_ROLE',
      'status', 400,
      'message', 'Unsupported team role transition'
    );
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_team_change_member_role',
    v_actor,
    'roles',
    p_user_id,
    p_team_id::text,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'action', 'set',
      'role', p_role
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_role_row
  );
exception
  when unique_violation then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_MEMBER_ALREADY_EXISTS',
      'status', 409,
      'message', 'The team membership already exists'
    );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_team_create(p_team_id uuid, p_json jsonb, p_rank integer, p_is_public boolean, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_team_row jsonb;
  v_role_row jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_team_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_ID_REQUIRED',
      'status', 400,
      'message', 'teamId is required'
    );
  end if;

  if p_json is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_JSON_REQUIRED',
      'status', 400,
      'message', 'json is required'
    );
  end if;

  if public.policy_user_has_team(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_ALREADY_ASSIGNED',
      'status', 409,
      'message', 'The actor already belongs to a team'
    );
  end if;

  if not public.policy_roles_insert(v_actor, p_team_id, 'owner') then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'The actor is not allowed to create this team'
    );
  end if;

  delete from public.roles
  where user_id = v_actor
    and role = 'rejected'
    and team_id <> '00000000-0000-0000-0000-000000000000'::uuid;

  insert into public.teams (
    id,
    json,
    rank,
    is_public,
    modified_at
  )
  values (
    p_team_id,
    p_json,
    coalesce(p_rank, -1),
    coalesce(p_is_public, false),
    now()
  )
  returning to_jsonb(teams.*)
    into v_team_row;

  insert into public.roles (
    user_id,
    team_id,
    role,
    modified_at
  )
  values (
    v_actor,
    p_team_id,
    'owner',
    now()
  )
  returning to_jsonb(roles.*)
    into v_role_row;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_team_create',
    v_actor,
    'teams',
    p_team_id,
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'team', v_team_row,
      'owner_role', v_role_row
    )
  );
exception
  when unique_violation then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_ALREADY_EXISTS',
      'status', 409,
      'message', 'The team already exists'
    );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_team_reinvite_member(p_team_id uuid, p_user_id uuid, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_existing_role text;
  v_role_row jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  select role
    into v_existing_role
  from public.roles
  where user_id = p_user_id
    and team_id = p_team_id
  for update;

  if v_existing_role is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'ROLE_NOT_FOUND',
      'status', 404,
      'message', 'Role not found'
    );
  end if;

  if not public.policy_roles_update(p_user_id, p_team_id, 'is_invited') then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'The actor cannot reinvite this member'
    );
  end if;

  update public.roles
    set role = 'is_invited',
        modified_at = now()
  where user_id = p_user_id
    and team_id = p_team_id
  returning to_jsonb(roles.*)
    into v_role_row;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_team_reinvite_member',
    v_actor,
    'roles',
    p_user_id,
    p_team_id::text,
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_role_row
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_team_reject_invitation(p_team_id uuid, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_role_row jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if not public.policy_roles_update(v_actor, p_team_id, 'rejected') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVITATION_NOT_FOUND',
      'status', 404,
      'message', 'No matching invitation was found for the actor'
    );
  end if;

  update public.roles
    set role = 'rejected',
        modified_at = now()
  where user_id = v_actor
    and team_id = p_team_id
    and role = 'is_invited'
  returning to_jsonb(roles.*)
    into v_role_row;

  if v_role_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVITATION_NOT_FOUND',
      'status', 404,
      'message', 'No matching invitation was found for the actor'
    );
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_team_reject_invitation',
    v_actor,
    'roles',
    v_actor,
    p_team_id::text,
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_role_row
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_team_set_rank(p_team_id uuid, p_rank integer, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_team_row jsonb;
  v_can_manage boolean;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_team_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_ID_REQUIRED',
      'status', 400,
      'message', 'teamId is required'
    );
  end if;

  if p_rank is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'RANK_REQUIRED',
      'status', 400,
      'message', 'rank is required'
    );
  end if;

  select
    public.cmd_membership_is_team_manager(v_actor, p_team_id) or
    public.cmd_membership_is_system_manager(v_actor)
  into v_can_manage;

  if not coalesce(v_can_manage, false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'The actor cannot update this team rank'
    );
  end if;

  update public.teams
    set rank = p_rank,
        modified_at = now()
  where id = p_team_id
  returning to_jsonb(teams.*)
    into v_team_row;

  if v_team_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_NOT_FOUND',
      'status', 404,
      'message', 'Team not found'
    );
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_team_set_rank',
    v_actor,
    'teams',
    p_team_id,
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_team_row
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_team_update_profile(p_team_id uuid, p_json jsonb, p_is_public boolean, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_team_row jsonb;
  v_can_manage boolean;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_team_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_ID_REQUIRED',
      'status', 400,
      'message', 'teamId is required'
    );
  end if;

  if p_json is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_JSON_REQUIRED',
      'status', 400,
      'message', 'json is required'
    );
  end if;

  select
    public.cmd_membership_is_team_manager(v_actor, p_team_id) or
    public.cmd_membership_is_system_manager(v_actor)
  into v_can_manage;

  if not coalesce(v_can_manage, false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'The actor cannot update this team profile'
    );
  end if;

  update public.teams
    set json = p_json,
        is_public = coalesce(p_is_public, false),
        modified_at = now()
  where id = p_team_id
  returning to_jsonb(teams.*)
    into v_team_row;

  if v_team_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_NOT_FOUND',
      'status', 404,
      'message', 'Team not found'
    );
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_team_update_profile',
    v_actor,
    'teams',
    p_team_id,
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_team_row
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.cmd_user_update_contact(p_user_id uuid, p_contact jsonb, p_audit jsonb DEFAULT '{}'::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_user_row jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_user_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'USER_ID_REQUIRED',
      'status', 400,
      'message', 'userId is required'
    );
  end if;

  if v_actor <> p_user_id and not public.cmd_membership_is_review_admin(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'The actor cannot update this contact'
    );
  end if;

  update public.users
    set contact = p_contact
  where id = p_user_id
  returning to_jsonb(users.*)
    into v_user_row;

  if v_user_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'USER_NOT_FOUND',
      'status', 404,
      'message', 'User not found'
    );
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_user_update_contact',
    v_actor,
    'users',
    p_user_id,
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_user_row
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.contacts_sync_jsonb_version()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public', 'pg_temp'
AS $function$
BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb THEN
        NEW.json := NEW.json_ordered;
        NEW.version := COALESCE(
            NEW.json->'contactDataSet'->'administrativeInformation'->'publicationAndOwnership'->>'common:dataSetVersion', 
            ''
        );
    END IF;
    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.delete_lifecycle_model_bundle(p_model_id uuid, p_version text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
    v_model_row lifecyclemodels%rowtype;
    v_submodel jsonb;
    v_submodel_version text;
    v_rows_affected integer;
begin
    if p_model_id is null or nullif(btrim(coalesce(p_version, '')), '') is null then
        raise exception 'INVALID_PLAN';
    end if;

    select *
      into v_model_row
      from lifecyclemodels
     where id = p_model_id
       and version = p_version
     for update;

    if not found then
        raise exception 'MODEL_NOT_FOUND';
    end if;

    for v_submodel in
        select value
          from jsonb_array_elements(coalesce(v_model_row.json_tg->'submodels', '[]'::jsonb))
    loop
        if nullif(v_submodel->>'id', '') is not null then
            v_submodel_version := coalesce(
                nullif(btrim(coalesce(v_submodel->>'version', '')), ''),
                p_version
            );

            -- Treat bundle deletion as idempotent for child processes so partially
            -- cleaned-up bundles do not block removal of the parent model row.
            execute 'del' || 'ete from processes where id = $1 and version = $2 and model_id = $3'
               using (v_submodel->>'id')::uuid, v_submodel_version, p_model_id;
        end if;
    end loop;

    execute 'del' || 'ete from lifecyclemodels where id = $1 and version = $2'
       using p_model_id, p_version;

    get diagnostics v_rows_affected = row_count;
    if v_rows_affected = 0 then
        raise exception 'MODEL_NOT_FOUND';
    end if;

    return jsonb_build_object(
        'model_id', p_model_id,
        'version', p_version
    );
end;
$function$
;

CREATE OR REPLACE FUNCTION public.flowproperties_sync_jsonb_version()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public', 'pg_temp'
AS $function$
BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb THEN
        NEW.json := NEW.json_ordered;
        NEW.version := COALESCE( NEW.json->'flowPropertyDataSet'->'administrativeInformation'->'publicationAndOwnership'->>'common:dataSetVersion',
					''
        );
    END IF;
    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.flows_embedding_ft_input(proc public.flows)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'public'
AS $function$
begin
  return proc.extracted_md;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.flows_embedding_input(flow public.flows)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'public'
AS $function$
begin
  return flow.extracted_text;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.flows_sync_jsonb_version()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public', 'pg_temp'
AS $function$
BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb THEN
        NEW.json := NEW.json_ordered;
		NEW.version := COALESCE( NEW.json->'flowDataSet'->'administrativeInformation'->'publicationAndOwnership'->>'common:dataSetVersion',
					''
        );
    END IF;
    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.generate_flow_embedding()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
  request_url text;
  legacy_x_key text;
BEGIN
  request_url := util.project_url();
  legacy_x_key := util.project_x_key();

  SELECT embedding, extracted_text INTO NEW.embedding, NEW.extracted_text
  FROM supabase_functions.http_request(
    request_url || '/functions/v1/flow_embedding',
    'POST',
    jsonb_build_object(
      'Content-Type', 'application/json',
      'x_key', legacy_x_key,
      'x_region', 'us-east-1'
    )::text,
    to_json(NEW.json_ordered)::text,
    '1000'
  );
  RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.hybrid_search_flows(query_text text, query_embedding text, filter_condition text DEFAULT ''::text, match_threshold double precision DEFAULT 0.5, match_count integer DEFAULT 20, full_text_weight double precision DEFAULT 0.3, extracted_text_weight double precision DEFAULT 0.2, semantic_weight double precision DEFAULT 0.5, rrf_k integer DEFAULT 10, data_source text DEFAULT 'tg'::text, page_size integer DEFAULT 10, page_current integer DEFAULT 1)
 RETURNS TABLE(id uuid, "json" jsonb, version character, modified_at timestamp with time zone)
 LANGUAGE plpgsql
 SET statement_timeout TO '60s'
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
 BEGIN
		RETURN QUERY WITH full_text AS (
			SELECT
				ps.RANK AS ps_rank,
				ps.ID AS ps_id,
				ps.JSON AS ps_json 
			FROM
				pgroonga_search_flows_v1 ( query_text, filter_condition, '', 20, -- page_size: 获取足够多候选
					1, -- page_current: 第1页
				data_source ) ps 
		),
		ex_text AS (
			SELECT
				ex.RANK AS ex_rank,
				ex.ID AS ex_id,
				P.JSON AS ex_json 
			FROM
				pgroonga_search_flows_text_v1 ( query_text, 20, -- page_size
					1, -- page_current
				data_source ) ex
				JOIN PUBLIC.flows P ON P.ID = ex.ID 
		),
		semantic AS (
			SELECT
				ss.RANK AS ss_rank,
				ss.ID AS ss_id,
				ss.JSON AS ss_json 
			FROM
				semantic_search_flows_v1 ( query_embedding, filter_condition, match_threshold, match_count, data_source ) ss 
		), 
		fused_raw as (
		SELECT 
			COALESCE ( full_text.ps_id, semantic.ss_id, ex_text.ex_id ) AS ID,
			COALESCE ( full_text.ps_json, semantic.ss_json, ex_text.ex_json ) AS JSON,
			COALESCE ( 1.0 / ( rrf_k + full_text.ps_rank ), 0.0 ) * full_text_weight
			+ COALESCE ( 1.0 / ( rrf_k + ex_text.ex_rank ), 0.0 ) * extracted_text_weight
			+ COALESCE ( 1.0 / ( rrf_k + semantic.ss_rank ), 0.0 ) * semantic_weight AS score 
		FROM
			full_text
			FULL OUTER JOIN semantic ON full_text.ps_id = semantic.ss_id
			FULL OUTER JOIN ex_text ON ex_text.ex_id = COALESCE ( full_text.ps_id, semantic.ss_id ) 
		),
		fused AS (
			SELECT
				fr.id AS fid,
				SUM(fr.score) AS score
			FROM fused_raw fr
			WHERE fr.id IS NOT NULL
			GROUP BY fr.id
		)
		SELECT
			f.fid AS id,
			fl.json,
			fl.version,
			fl.modified_at
		FROM fused f
		JOIN LATERAL (
			SELECT fl.json, fl.version, fl.modified_at
			FROM public.flows fl
			WHERE fl.id = f.fid
			ORDER BY fl.modified_at DESC
			LIMIT 1
		) fl ON true
		ORDER BY f.score DESC
		LIMIT page_size OFFSET ( page_current - 1 ) * page_size;
		
	END;
$function$
;

CREATE OR REPLACE FUNCTION public.hybrid_search_lifecyclemodels(query_text text, query_embedding text, filter_condition text DEFAULT ''::text, match_threshold double precision DEFAULT 0.5, match_count integer DEFAULT 20, full_text_weight double precision DEFAULT 0.3, extracted_text_weight double precision DEFAULT 0.2, semantic_weight double precision DEFAULT 0.5, rrf_k integer DEFAULT 10, data_source text DEFAULT 'tg'::text, page_size integer DEFAULT 10, page_current integer DEFAULT 1)
 RETURNS TABLE(id uuid, "json" jsonb, version character, modified_at timestamp with time zone)
 LANGUAGE plpgsql
 SET statement_timeout TO '60s'
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
 BEGIN
		RETURN QUERY WITH full_text AS (
			SELECT
				ps.RANK AS ps_rank,
				ps.ID AS ps_id,
				ps.JSON AS ps_json
			FROM
			 	-- page_size: 获取足够多候选， page_current: 第1页
				pgroonga_search_lifecyclemodels_v1 ( query_text, filter_condition, '', 20, 1, data_source ) ps 
		),
		ex_text AS (
			SELECT
				ex.RANK AS ex_rank,
				ex.ID AS ex_id,
				P.JSON AS ex_json
			FROM
				pgroonga_search_lifecyclemodels_text_v1( query_text, 20, 1, data_source ) ex
				JOIN PUBLIC.lifecyclemodels P ON P.ID = ex.ID 
		),
		semantic AS (
			SELECT
				ss.RANK AS ss_rank,
				ss.ID AS ss_id,
				ss.JSON AS ss_json
			FROM
				semantic_search_lifecyclemodels_v1 ( query_embedding, filter_condition, match_threshold, match_count, data_source ) ss 
		), 
		fused_raw as (
		SELECT 
			COALESCE ( full_text.ps_id, semantic.ss_id, ex_text.ex_id ) AS ID,
			COALESCE ( full_text.ps_json, semantic.ss_json, ex_text.ex_json ) AS JSON,
			COALESCE ( 1.0 / ( rrf_k + full_text.ps_rank ), 0.0 ) * full_text_weight + COALESCE ( 1.0 / ( rrf_k + ex_text.ex_rank ), 0.0 ) * extracted_text_weight + COALESCE ( 1.0 / ( rrf_k + semantic.ss_rank ), 0.0 ) * semantic_weight AS score 
		FROM
			full_text
			FULL OUTER JOIN semantic ON full_text.ps_id = semantic.ss_id
			FULL OUTER JOIN ex_text ON ex_text.ex_id = COALESCE ( full_text.ps_id, semantic.ss_id ) 
		),
		fused AS (
			SELECT
				fr.id AS pid,
				SUM(fr.score) AS score
				-- 如果你不希望“多路径叠加加分”，把 SUM 改成 MAX
			FROM fused_raw fr
			WHERE fr.id IS NOT NULL
			GROUP BY fr.id
		)
		SELECT
			f.pid AS id,
			p.json,
			p.version,
			p.modified_at
		FROM fused f
		JOIN LATERAL (
			SELECT p.json, p.version, p.modified_at
			FROM public.lifecyclemodels p
			WHERE p.id = f.pid
			ORDER BY p.modified_at DESC
			LIMIT 1
		) p ON true
		ORDER BY f.score DESC
		LIMIT page_size OFFSET ( page_current - 1 ) * page_size;
		
	END;
$function$
;

CREATE OR REPLACE FUNCTION public.hybrid_search_processes(query_text text, query_embedding text, filter_condition text DEFAULT ''::text, match_threshold double precision DEFAULT 0.5, match_count integer DEFAULT 20, full_text_weight double precision DEFAULT 0.3, extracted_text_weight double precision DEFAULT 0.2, semantic_weight double precision DEFAULT 0.5, rrf_k integer DEFAULT 10, data_source text DEFAULT 'tg'::text, page_size integer DEFAULT 10, page_current integer DEFAULT 1)
 RETURNS TABLE(id uuid, "json" jsonb, version character, modified_at timestamp with time zone, model_id uuid)
 LANGUAGE plpgsql
 SET statement_timeout TO '60s'
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
 BEGIN
		RETURN QUERY WITH full_text AS (
			SELECT
				ps.RANK AS ps_rank,
				ps.ID AS ps_id,
				ps.JSON AS ps_json
			FROM
			 	-- page_size: 获取足够多候选， page_current: 第1页
				pgroonga_search_processes_v1 ( query_text, filter_condition, '', 20, 1, data_source ) ps 
		),
		ex_text AS (
			SELECT
				ex.RANK AS ex_rank,
				ex.ID AS ex_id,
				P.JSON AS ex_json
			FROM
				pgroonga_search_processes_text_v1( query_text, 20, 1, data_source ) ex
				JOIN PUBLIC.processes P ON P.ID = ex.ID 
		),
		semantic AS (
			SELECT
				ss.RANK AS ss_rank,
				ss.ID AS ss_id,
				ss.JSON AS ss_json
			FROM
				semantic_search_processes_v1 ( query_embedding, filter_condition, match_threshold, match_count, data_source ) ss 
		), 
		fused_raw as (
		SELECT 
			COALESCE ( full_text.ps_id, semantic.ss_id, ex_text.ex_id ) AS ID,
			COALESCE ( full_text.ps_json, semantic.ss_json, ex_text.ex_json ) AS JSON,
			COALESCE ( 1.0 / ( rrf_k + full_text.ps_rank ), 0.0 ) * full_text_weight + COALESCE ( 1.0 / ( rrf_k + ex_text.ex_rank ), 0.0 ) * extracted_text_weight + COALESCE ( 1.0 / ( rrf_k + semantic.ss_rank ), 0.0 ) * semantic_weight AS score 
		FROM
			full_text
			FULL OUTER JOIN semantic ON full_text.ps_id = semantic.ss_id
			FULL OUTER JOIN ex_text ON ex_text.ex_id = COALESCE ( full_text.ps_id, semantic.ss_id ) 
		),
		fused AS (
			SELECT
				fr.id AS pid,
				SUM(fr.score) AS score
				-- 如果你不希望“多路径叠加加分”，把 SUM 改成 MAX
			FROM fused_raw fr
			WHERE fr.id IS NOT NULL
			GROUP BY fr.id
		)
		SELECT
			f.pid AS id,
			p.json,
			p.version,
			p.modified_at,
			p.model_id
		FROM fused f
		JOIN LATERAL (
			SELECT p.json, p.version, p.modified_at, p.model_id
			FROM public.processes p
			WHERE p.id = f.pid
			ORDER BY p.modified_at DESC
			LIMIT 1
		) p ON true
		ORDER BY f.score DESC
		LIMIT page_size OFFSET ( page_current - 1 ) * page_size;
		
	END;
$function$
;

CREATE OR REPLACE FUNCTION public.ilcd_classification_get(this_file_name text, category_type text, get_values text[])
 RETURNS SETOF jsonb
 LANGUAGE plpgsql
 SET search_path TO 'public', 'pg_temp'
AS $function$
BEGIN
  RETURN QUERY
  SELECT cgs2.cg
  FROM (
  select 
		cgs1.file_name,
	  cgs1.cg->>'@dataType' as cg_type,
      jsonb_array_elements(cgs1.cg -> 'category') AS cg
from
(
    SELECT
      ilcd.file_name,
      jsonb_array_elements(ilcd.json -> 'CategorySystem' -> 'categories') AS cg
    FROM
      ilcd
    WHERE ilcd.file_name = this_file_name
	) as cgs1
	where cgs1.cg->>'@dataType' = category_type
	  ) as cgs2
	  WHERE cgs2.cg->>'@name' = ANY(get_values) or cgs2.cg->>'@id' = ANY(get_values) or 'all' = ANY(get_values)
	;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.ilcd_flow_categorization_get(this_file_name text, get_values text[])
 RETURNS SETOF jsonb
 LANGUAGE plpgsql
 SET search_path TO 'public', 'pg_temp'
AS $function$
BEGIN
  RETURN QUERY
  SELECT cg
  FROM (
    SELECT
      ilcd.file_name,
      jsonb_array_elements(ilcd.json -> 'CategorySystem' -> 'categories' -> 'category') AS cg
    FROM
      ilcd
    WHERE ilcd.file_name = this_file_name
  ) AS cgs
  WHERE cgs.cg->>'@name' = ANY(get_values)  or cgs.cg->>'@id' = ANY(get_values) or 'all' = ANY(get_values);
END;
$function$
;

CREATE OR REPLACE FUNCTION public.ilcd_location_get(this_file_name text, get_values text[])
 RETURNS SETOF jsonb
 LANGUAGE plpgsql
 SET search_path TO 'public', 'pg_temp'
AS $function$
BEGIN
  RETURN QUERY
  SELECT lc
  FROM (
    SELECT
      ilcd.file_name,
      jsonb_array_elements(ilcd.json -> 'ILCDLocations' -> 'location') AS lc
    FROM
      ilcd
    WHERE ilcd.file_name = this_file_name
  ) AS lcs
  WHERE lcs.lc->>'@value' = ANY(get_values);
END;
$function$
;

CREATE OR REPLACE FUNCTION public.lca_enqueue_job(p_queue_name text, p_message jsonb)
 RETURNS bigint
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pgmq'
AS $function$
DECLARE
    v_msg_id bigint;
BEGIN
    IF p_queue_name IS NULL OR btrim(p_queue_name) = '' THEN
        RAISE EXCEPTION 'queue name is required';
    END IF;

    SELECT pgmq.send(p_queue_name, p_message)
      INTO v_msg_id;

    RETURN v_msg_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.lca_package_enqueue_job(p_message jsonb)
 RETURNS bigint
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pgmq'
AS $function$
DECLARE
    v_msg_id bigint;
BEGIN
    SELECT pgmq.send('lca_package_jobs', p_message)
      INTO v_msg_id;

    RETURN v_msg_id;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.lciamethods_sync_jsonb_version()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public', 'pg_temp'
AS $function$
BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb THEN
        NEW.json := NEW.json_ordered;
        NEW.version := NEW.json->'LCIAMethodDataSet'->'administrativeInformation'->'publicationAndOwnership'->>'common:dataSetVersion';
    END IF;
    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.lifecyclemodels_embedding_ft_input(proc public.lifecyclemodels)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'public'
AS $function$
begin
  return proc.extracted_md;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.lifecyclemodels_embedding_input(models public.lifecyclemodels)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'public'
AS $function$
begin
  return models.extracted_text;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.lifecyclemodels_sync_jsonb_version()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public', 'pg_temp'
AS $function$
BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb THEN
        NEW.json := NEW.json_ordered;
        NEW.version := NEW.json->'lifeCycleModelDataSet'->'administrativeInformation'->'publicationAndOwnership'->>'common:dataSetVersion';
    END IF;
    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.pgroonga_search(query_text text)
 RETURNS TABLE(rank bigint, id uuid, "json" jsonb)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
BEGIN
  RETURN QUERY
		SELECT 
			RANK () OVER (ORDER BY pgroonga_score(f.tableoid, f.ctid) DESC) AS rank, 
			f.id, 
			f.json
		FROM flows f
		WHERE f.extracted_text &@~ query_text
		ORDER BY pgroonga_score(tableoid, ctid) DESC;
END;$function$
;

CREATE OR REPLACE FUNCTION public.pgroonga_search_contacts(query_text text, filter_condition text DEFAULT ''::text, page_size bigint DEFAULT 10, page_current bigint DEFAULT 1, data_source text DEFAULT 'tg'::text, this_user_id text DEFAULT ''::text)
 RETURNS TABLE(rank bigint, id uuid, "json" jsonb, version character, modified_at timestamp with time zone, total_count bigint)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
    filter_condition_jsonb JSONB;
BEGIN
	filter_condition_jsonb := filter_condition::JSONB;
  RETURN QUERY
		SELECT 
			RANK () OVER (ORDER BY pgroonga_score(f.tableoid, f.ctid) DESC) AS rank, 
			f.id, 
			f.json,
			f.version,
			f.modified_at,
			COUNT(*) OVER() AS total_count
		FROM contacts f
		WHERE f.json @> filter_condition_jsonb AND f.json &@~ query_text AND ((data_source = 'tg' AND state_code = 100) or (data_source = 'my' AND user_id::text = this_user_id))
		ORDER BY pgroonga_score(tableoid, ctid) DESC
		LIMIT page_size
		OFFSET (page_current -1) * page_size;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.pgroonga_search_flowproperties(query_text text, filter_condition text DEFAULT ''::text, page_size bigint DEFAULT 10, page_current bigint DEFAULT 1, data_source text DEFAULT 'tg'::text, this_user_id text DEFAULT ''::text)
 RETURNS TABLE(rank bigint, id uuid, "json" jsonb, version character, modified_at timestamp with time zone, total_count bigint)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
    filter_condition_jsonb JSONB;
BEGIN
	filter_condition_jsonb := filter_condition::JSONB;
  RETURN QUERY
		SELECT 
			RANK () OVER (ORDER BY pgroonga_score(f.tableoid, f.ctid) DESC) AS rank, 
			f.id, 
			f.json,
			f.version,
			f.modified_at,
			COUNT(*) OVER() AS total_count
		FROM flowproperties f
		WHERE f.json @> filter_condition_jsonb AND f.json &@~ query_text AND ((data_source = 'tg' AND state_code = 100) or (data_source = 'my' AND user_id::text = this_user_id))
		ORDER BY pgroonga_score(tableoid, ctid) DESC
		LIMIT page_size
		OFFSET (page_current -1) * page_size;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.pgroonga_search_flows_text_v1(query_text text, page_size integer DEFAULT 10, page_current integer DEFAULT 1, data_source text DEFAULT 'tg'::text)
 RETURNS TABLE(rank bigint, id uuid, extracted_text text, version character, modified_at timestamp with time zone, total_count bigint)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  RETURN QUERY
		SELECT 
			RANK () OVER (ORDER BY pgroonga_score(f.tableoid, f.ctid) DESC) AS rank, 
			f.id, 
			f.extracted_text,
			f.version,
			f.modified_at,
			COUNT(*) OVER() AS total_count
		FROM public.flows AS f
		WHERE f.extracted_text &@~ query_text AND ((data_source = 'tg' AND state_code = 100) or (data_source = 'co' AND state_code = 200) or (data_source = 'my' AND user_id = auth.uid())
																			  or (data_source = 'te' and
		EXISTS ( 
						SELECT 1
						FROM roles r
						WHERE r.user_id = auth.uid() and r.team_id =  f.team_id
						AND r.role::text IN ('admin', 'member', 'owner') 
				)
			)
		)
		ORDER BY pgroonga_score(tableoid, ctid) DESC
		LIMIT page_size
		OFFSET (page_current -1) * page_size;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.pgroonga_search_flows_v1(query_text text, filter_condition text DEFAULT ''::text, order_by text DEFAULT ''::text, page_size bigint DEFAULT 10, page_current bigint DEFAULT 1, data_source text DEFAULT 'tg'::text)
 RETURNS TABLE(rank bigint, id uuid, "json" jsonb, version character, modified_at timestamp with time zone, total_count bigint)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
 
DECLARE 
	filter_condition_jsonb JSONB;
	flowType TEXT;
	flowTypeArray TEXT[];
	asInput BOOLEAN;
	use_base_name_order boolean := false;
	use_common_category_order boolean := false;
	use_zh_icu_order boolean := false;
	order_by_jsonb jsonb;
	order_key text;
	order_lang text;
	order_dir text;
	order_lang_norm text;
BEGIN
	-- order_by 输入格式（标准 JSON）：{"key":"baseName","lang":"zh","order":"asc"} 或 {"key":"common:category","order":"asc"}

	filter_condition_jsonb := COALESCE(NULLIF(btrim(filter_condition), ''), '{}')::JSONB;

	flowType := NULLIF(btrim(filter_condition_jsonb->>'flowType'), '');
	IF flowType IS NOT NULL THEN
		flowTypeArray := string_to_array(flowType, ',');
	ELSE
		flowTypeArray := NULL;
	END IF;
	filter_condition_jsonb := filter_condition_jsonb - 'flowType';

	IF filter_condition_jsonb ? 'asInput' THEN
		asInput := NULLIF(btrim(filter_condition_jsonb->>'asInput'), '')::BOOLEAN;
	ELSE
		asInput := NULL;
	END IF;
	filter_condition_jsonb := filter_condition_jsonb - 'asInput';

	-- order_by 解析
	IF order_by IS NOT NULL AND btrim(order_by) <> '' THEN
		order_by_jsonb := order_by::jsonb;

		order_key := lower(COALESCE(NULLIF(btrim(order_by_jsonb->>'key'), ''), ''));
		order_lang := COALESCE(NULLIF(btrim(order_by_jsonb->>'lang'), ''), 'en');
		order_dir := lower(COALESCE(NULLIF(btrim(order_by_jsonb->>'order'), ''), 'asc'));
		IF order_dir NOT IN ('asc', 'desc') THEN
			order_dir := 'asc';
		END IF;

		use_base_name_order := (order_key = 'basename');
		use_common_category_order := (order_key = 'common:category');
	ELSE
		use_base_name_order := false;
		use_common_category_order := false;
		order_lang := 'en';
		order_dir := 'asc';
	END IF;

	order_lang_norm := lower(COALESCE(NULLIF(btrim(order_lang), ''), 'en'));
	use_zh_icu_order := (order_lang_norm LIKE 'zh%');

	RETURN QUERY
		WITH filtered AS (
			SELECT
				f.id,
				f.json,
				f.version,
				f.modified_at,
				pgroonga_score(f.tableoid, f.ctid) AS score,
				bn.base_name,
				cat.category_name,
				CASE
					WHEN use_base_name_order THEN bn.base_name
					WHEN use_common_category_order THEN cat.category_name
				END AS order_value
			FROM flows f
			CROSS JOIN LATERAL (
				SELECT
					CASE
						WHEN use_base_name_order THEN COALESCE(
							(
								SELECT bn_item->>'#text'
								FROM jsonb_array_elements(
									CASE jsonb_typeof(
										f.json
											-> 'flowDataSet'
											-> 'flowInformation'
											-> 'dataSetInformation'
											-> 'name'
											-> 'baseName'
									)
										WHEN 'array' THEN (
											f.json
												-> 'flowDataSet'
												-> 'flowInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										WHEN 'object' THEN jsonb_build_array(
											f.json
												-> 'flowDataSet'
												-> 'flowInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										ELSE '[]'::jsonb
									END
								) AS bn_item
								WHERE bn_item->>'@xml:lang' = order_lang
								LIMIT 1
							),
							(
								SELECT bn_item->>'#text'
								FROM jsonb_array_elements(
									CASE jsonb_typeof(
										f.json
											-> 'flowDataSet'
											-> 'flowInformation'
											-> 'dataSetInformation'
											-> 'name'
											-> 'baseName'
									)
										WHEN 'array' THEN (
											f.json
												-> 'flowDataSet'
												-> 'flowInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										WHEN 'object' THEN jsonb_build_array(
											f.json
												-> 'flowDataSet'
												-> 'flowInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										ELSE '[]'::jsonb
									END
								) AS bn_item
								WHERE bn_item->>'@xml:lang' = 'en'
								LIMIT 1
							),
							COALESCE(
								f.json #>> '{flowDataSet,flowInformation,dataSetInformation,name,baseName,0,#text}',
								f.json #>> '{flowDataSet,flowInformation,dataSetInformation,name,baseName,#text}'
							),
							''
						)
					END AS base_name
			) bn
			CROSS JOIN LATERAL (
				SELECT
					CASE
						WHEN use_common_category_order THEN COALESCE(
							(
								SELECT string_agg(cat_item->>'#text', ' / ' ORDER BY cat_level ASC)
								FROM (
									SELECT
										cat_item,
										CASE
											WHEN (cat_item->>'@level') ~ '^\\d+$' THEN (cat_item->>'@level')::int
											ELSE 2147483647
										END AS cat_level
									FROM jsonb_array_elements(
										CASE jsonb_typeof(
											f.json
												-> 'flowDataSet'
												-> 'flowInformation'
												-> 'dataSetInformation'
												-> 'classificationInformation'
												-> 'common:elementaryFlowCategorization'
												-> 'common:category'
										)
											WHEN 'array' THEN (
												f.json
													-> 'flowDataSet'
													-> 'flowInformation'
													-> 'dataSetInformation'
													-> 'classificationInformation'
													-> 'common:elementaryFlowCategorization'
													-> 'common:category'
										)
											WHEN 'object' THEN jsonb_build_array(
												f.json
													-> 'flowDataSet'
													-> 'flowInformation'
													-> 'dataSetInformation'
													-> 'classificationInformation'
													-> 'common:elementaryFlowCategorization'
													-> 'common:category'
										)
											ELSE '[]'::jsonb
										END
									) AS cat_item
								) ordered_cat
							),
							''
						)
					END AS category_name
			) cat
			WHERE f.json @> filter_condition_jsonb
				AND f.json &@~ query_text
				AND (
					(data_source = 'tg' AND state_code = 100)
					OR (data_source = 'co' AND state_code = 200)
					OR (data_source = 'my' AND user_id = auth.uid())
					OR (
						data_source = 'te'
						AND EXISTS (
							SELECT 1
							FROM roles r
							WHERE r.user_id = auth.uid()
								AND r.team_id = f.team_id
								AND r.role::text IN ('admin', 'member', 'owner')
						)
					)
				)
				AND (
					flowType IS NULL
					OR flowType = ''
					OR (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = ANY(flowTypeArray)
				)
				AND (
					asInput IS NULL
					OR asInput = false
					OR NOT(
						f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text": "Emissions", "@level": "0"}]}}}}}}'
					)
				)
		)
		SELECT
			ROW_NUMBER() OVER (
				ORDER BY
					(CASE WHEN (use_base_name_order OR use_common_category_order) AND use_zh_icu_order AND order_dir = 'asc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" ASC NULLS LAST,
					(CASE WHEN (use_base_name_order OR use_common_category_order) AND use_zh_icu_order AND order_dir = 'desc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" DESC NULLS LAST,
					CASE WHEN (use_base_name_order OR use_common_category_order) AND NOT use_zh_icu_order AND order_dir = 'asc' THEN lower(f2.order_value) END ASC NULLS LAST,
					CASE WHEN (use_base_name_order OR use_common_category_order) AND NOT use_zh_icu_order AND order_dir = 'desc' THEN lower(f2.order_value) END DESC NULLS LAST,
					f2.score DESC,
					f2.modified_at DESC,
					f2.id
			) AS rank,
			f2.id,
			f2.json,
			f2.version,
			f2.modified_at,
			COUNT(*) OVER() AS total_count
		FROM filtered f2
		ORDER BY
			(CASE WHEN (use_base_name_order OR use_common_category_order) AND use_zh_icu_order AND order_dir = 'asc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" ASC NULLS LAST,
			(CASE WHEN (use_base_name_order OR use_common_category_order) AND use_zh_icu_order AND order_dir = 'desc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" DESC NULLS LAST,
			CASE WHEN (use_base_name_order OR use_common_category_order) AND NOT use_zh_icu_order AND order_dir = 'asc' THEN lower(f2.order_value) END ASC NULLS LAST,
			CASE WHEN (use_base_name_order OR use_common_category_order) AND NOT use_zh_icu_order AND order_dir = 'desc' THEN lower(f2.order_value) END DESC NULLS LAST,
			f2.score DESC,
			f2.modified_at DESC,
			f2.id
		LIMIT page_size
		OFFSET (page_current - 1) * page_size;
	END; 
	
$function$
;

CREATE OR REPLACE FUNCTION public.pgroonga_search_lifecyclemodels_text_v1(query_text text, page_size integer DEFAULT 10, page_current integer DEFAULT 1, data_source text DEFAULT 'tg'::text)
 RETURNS TABLE(rank bigint, id uuid, extracted_text text, version character, modified_at timestamp with time zone, total_count bigint)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  RETURN QUERY
		SELECT 
			RANK () OVER (ORDER BY pgroonga_score(f.tableoid, f.ctid) DESC) AS rank, 
			f.id, 
			f.extracted_text,
			f.version,
			f.modified_at,
			COUNT(*) OVER() AS total_count
		FROM public.lifecyclemodels AS f
		WHERE f.extracted_text &@~ query_text AND ((data_source = 'tg' AND state_code = 100) or (data_source = 'co' AND state_code = 200) or (data_source = 'my' AND user_id = auth.uid())
																			  or (data_source = 'te' and
		EXISTS ( 
						SELECT 1
						FROM roles r
						WHERE r.user_id = auth.uid() and r.team_id =  f.team_id
						AND r.role::text IN ('admin', 'member', 'owner') 
				)
			)
		)
		ORDER BY pgroonga_score(tableoid, ctid) DESC
		LIMIT page_size
		OFFSET (page_current -1) * page_size;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.pgroonga_search_lifecyclemodels_v1(query_text text, filter_condition text DEFAULT ''::text, order_by text DEFAULT ''::text, page_size bigint DEFAULT 10, page_current bigint DEFAULT 1, data_source text DEFAULT 'tg'::text)
 RETURNS TABLE(rank bigint, id uuid, "json" jsonb, version character, modified_at timestamp with time zone, total_count bigint)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  filter_condition_jsonb JSONB;
  use_base_name_order boolean := false;
  use_common_class_order boolean := false;
  use_zh_icu_order boolean := false;
  order_by_jsonb jsonb;
  order_key text;
  order_lang text;
  order_dir text;
  order_lang_norm text;
BEGIN
  -- order_by 输入格式（标准 JSON）：{"key":"baseName","lang":"zh","order":"asc"} 或 {"key":"common:class","order":"asc"}

  filter_condition_jsonb := COALESCE(NULLIF(btrim(filter_condition), ''), '{}')::JSONB;

  IF order_by IS NOT NULL AND btrim(order_by) <> '' THEN
    order_by_jsonb := order_by::jsonb;

    order_key := lower(COALESCE(NULLIF(btrim(order_by_jsonb->>'key'), ''), ''));
    order_lang := COALESCE(NULLIF(btrim(order_by_jsonb->>'lang'), ''), 'en');
    order_dir := lower(COALESCE(NULLIF(btrim(order_by_jsonb->>'order'), ''), 'asc'));
    IF order_dir NOT IN ('asc', 'desc') THEN
      order_dir := 'asc';
    END IF;

    use_base_name_order := (order_key = 'basename');
    use_common_class_order := (order_key = 'common:class');
  ELSE
    use_base_name_order := false;
    use_common_class_order := false;
    order_lang := 'en';
    order_dir := 'asc';
  END IF;

  order_lang_norm := lower(COALESCE(NULLIF(btrim(order_lang), ''), 'en'));
  use_zh_icu_order := (order_lang_norm LIKE 'zh%');

  RETURN QUERY
    WITH filtered AS (
      SELECT
        f.id,
        f.json,
        f.version,
        f.modified_at,
        pgroonga_score(f.tableoid, f.ctid) AS score,
        bn.base_name,
        cls.class_name,
        CASE
          WHEN use_base_name_order THEN bn.base_name
          WHEN use_common_class_order THEN cls.class_name
        END AS order_value
      FROM lifecyclemodels f
      CROSS JOIN LATERAL (
        SELECT
          CASE
            WHEN use_base_name_order THEN COALESCE(
              (
                SELECT bn_item->>'#text'
                FROM jsonb_array_elements(
                  CASE jsonb_typeof(
                    f.json
                      -> 'lifeCycleModelDataSet'
                      -> 'lifeCycleModelInformation'
                      -> 'dataSetInformation'
                      -> 'name'
                      -> 'baseName'
                  )
                    WHEN 'array' THEN (
                      f.json
                        -> 'lifeCycleModelDataSet'
                        -> 'lifeCycleModelInformation'
                        -> 'dataSetInformation'
                        -> 'name'
                        -> 'baseName'
                    )
                    WHEN 'object' THEN jsonb_build_array(
                      f.json
                        -> 'lifeCycleModelDataSet'
                        -> 'lifeCycleModelInformation'
                        -> 'dataSetInformation'
                        -> 'name'
                        -> 'baseName'
                    )
                    ELSE '[]'::jsonb
                  END
                ) AS bn_item
                WHERE bn_item->>'@xml:lang' = order_lang
                LIMIT 1
              ),
              (
                SELECT bn_item->>'#text'
                FROM jsonb_array_elements(
                  CASE jsonb_typeof(
                    f.json
                      -> 'lifeCycleModelDataSet'
                      -> 'lifeCycleModelInformation'
                      -> 'dataSetInformation'
                      -> 'name'
                      -> 'baseName'
                  )
                    WHEN 'array' THEN (
                      f.json
                        -> 'lifeCycleModelDataSet'
                        -> 'lifeCycleModelInformation'
                        -> 'dataSetInformation'
                        -> 'name'
                        -> 'baseName'
                    )
                    WHEN 'object' THEN jsonb_build_array(
                      f.json
                        -> 'lifeCycleModelDataSet'
                        -> 'lifeCycleModelInformation'
                        -> 'dataSetInformation'
                        -> 'name'
                        -> 'baseName'
                    )
                    ELSE '[]'::jsonb
                  END
                ) AS bn_item
                WHERE bn_item->>'@xml:lang' = 'en'
                LIMIT 1
              ),
              COALESCE(
                f.json #>> '{lifeCycleModelDataSet,lifeCycleModelInformation,dataSetInformation,name,baseName,0,#text}',
                f.json #>> '{lifeCycleModelDataSet,lifeCycleModelInformation,dataSetInformation,name,baseName,#text}'
              ),
              ''
            )
          END AS base_name
      ) bn
      CROSS JOIN LATERAL (
        SELECT
          CASE
            WHEN use_common_class_order THEN COALESCE(
              (
                SELECT string_agg(cls_item->>'#text', ' / ' ORDER BY cls_level ASC)
                FROM (
                  SELECT
                    cls_item,
                    CASE
                      WHEN (cls_item->>'@level') ~ '^\\d+$' THEN (cls_item->>'@level')::int
                      ELSE 2147483647
                    END AS cls_level
                  FROM jsonb_array_elements(
                    CASE jsonb_typeof(
                      f.json
                        -> 'lifeCycleModelDataSet'
                        -> 'lifeCycleModelInformation'
                        -> 'dataSetInformation'
                        -> 'classificationInformation'
                        -> 'common:classification'
                        -> 'common:class'
                    )
                      WHEN 'array' THEN (
                        f.json
                          -> 'lifeCycleModelDataSet'
                          -> 'lifeCycleModelInformation'
                          -> 'dataSetInformation'
                          -> 'classificationInformation'
                          -> 'common:classification'
                          -> 'common:class'
                    )
                      WHEN 'object' THEN jsonb_build_array(
                        f.json
                          -> 'lifeCycleModelDataSet'
                          -> 'lifeCycleModelInformation'
                          -> 'dataSetInformation'
                          -> 'classificationInformation'
                          -> 'common:classification'
                          -> 'common:class'
                    )
                      ELSE '[]'::jsonb
                    END
                  ) AS cls_item
                ) ordered_cls
              ),
              ''
            )
          END AS class_name
      ) cls
      WHERE f.json @> filter_condition_jsonb
        AND f.json &@~ query_text
        AND (
          (data_source = 'tg' AND state_code = 100)
          OR (data_source = 'co' AND state_code = 200)
          OR (data_source = 'my' AND user_id = auth.uid())
          OR (
            data_source = 'te'
            AND EXISTS (
              SELECT 1
              FROM roles r
              WHERE r.user_id = auth.uid()
                AND r.team_id = f.team_id
                AND r.role::text IN ('admin', 'member', 'owner')
            )
          )
        )
    )
    SELECT
      ROW_NUMBER() OVER (
        ORDER BY
          (CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'asc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" ASC NULLS LAST,
          (CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'desc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" DESC NULLS LAST,
          CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'asc' THEN lower(f2.order_value) END ASC NULLS LAST,
          CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'desc' THEN lower(f2.order_value) END DESC NULLS LAST,
          f2.score DESC,
          f2.modified_at DESC,
          f2.id
      ) AS rank,
      f2.id,
      f2.json,
      f2.version,
      f2.modified_at,
      COUNT(*) OVER() AS total_count
    FROM filtered f2
    ORDER BY
      (CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'asc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" ASC NULLS LAST,
      (CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'desc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" DESC NULLS LAST,
      CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'asc' THEN lower(f2.order_value) END ASC NULLS LAST,
      CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'desc' THEN lower(f2.order_value) END DESC NULLS LAST,
      f2.score DESC,
      f2.modified_at DESC,
      f2.id
    LIMIT page_size
    OFFSET (page_current - 1) * page_size;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.pgroonga_search_processes_text_v1(query_text text, page_size integer DEFAULT 10, page_current integer DEFAULT 1, data_source text DEFAULT 'tg'::text)
 RETURNS TABLE(rank bigint, id uuid, extracted_text text, version character, modified_at timestamp with time zone, total_count bigint)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  RETURN QUERY
		SELECT 
			RANK () OVER (ORDER BY pgroonga_score(f.tableoid, f.ctid) DESC) AS rank, 
			f.id, 
			f.extracted_text,
			f.version,
			f.modified_at,
			COUNT(*) OVER() AS total_count
		FROM public.processes AS f
		WHERE f.extracted_text &@~ query_text AND ((data_source = 'tg' AND state_code = 100) or (data_source = 'co' AND state_code = 200) or (data_source = 'my' AND user_id = auth.uid() )
																			  or (data_source = 'te' and
		EXISTS ( 
						SELECT 1
						FROM roles r
						WHERE r.user_id = auth.uid()  and r.team_id =  f.team_id
						AND r.role::text IN ('admin', 'member', 'owner') 
				)
			)
		)
		ORDER BY pgroonga_score(tableoid, ctid) DESC
		LIMIT page_size
		OFFSET (page_current -1) * page_size;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.pgroonga_search_processes_v1(query_text text, filter_condition text DEFAULT ''::text, order_by text DEFAULT ''::text, page_size bigint DEFAULT 10, page_current bigint DEFAULT 1, data_source text DEFAULT 'tg'::text)
 RETURNS TABLE(rank bigint, id uuid, "json" jsonb, version character, modified_at timestamp with time zone, model_id uuid, total_count bigint)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
    filter_condition_jsonb JSONB;
    use_base_name_order boolean := false;
	use_common_class_order boolean := false;
	use_zh_icu_order boolean := false;
    order_by_jsonb jsonb;
    order_key text;
    order_lang text;
    order_dir text;
	order_lang_norm text;
BEGIN
	filter_condition_jsonb := COALESCE(NULLIF(btrim(filter_condition), ''), '{}')::JSONB;

	-- order_by 输入格式（标准 JSON）：{"key":"baseName","lang":"zh","order":"asc"} 或 {"key":"common:class","order":"asc"}
	IF order_by IS NOT NULL AND btrim(order_by) <> '' THEN
		order_by_jsonb := order_by::jsonb;

		order_key := lower(COALESCE(NULLIF(btrim(order_by_jsonb->>'key'), ''), ''));
		order_lang := COALESCE(NULLIF(btrim(order_by_jsonb->>'lang'), ''), 'en');
		order_dir := lower(COALESCE(NULLIF(btrim(order_by_jsonb->>'order'), ''), 'asc'));
		IF order_dir NOT IN ('asc', 'desc') THEN
			order_dir := 'asc';
		END IF;

		use_base_name_order := (order_key = 'basename');
		use_common_class_order := (order_key = 'common:class');
	ELSE
		use_base_name_order := false;
		use_common_class_order := false;
		order_lang := 'en';
		order_dir := 'asc';
	END IF;

	order_lang_norm := lower(COALESCE(NULLIF(btrim(order_lang), ''), 'en'));
	use_zh_icu_order := (order_lang_norm LIKE 'zh%');

  RETURN QUERY
		WITH filtered AS (
			SELECT
				f.id,
				f.json,
				f.version,
				f.modified_at,
				f.model_id,
				pgroonga_score(f.tableoid, f.ctid) AS score,
				bn.base_name,
				cls.class_name,
				CASE
					WHEN use_base_name_order THEN bn.base_name
					WHEN use_common_class_order THEN cls.class_name
				END AS order_value
			FROM processes f
			CROSS JOIN LATERAL (
				SELECT
					CASE
						WHEN use_base_name_order THEN COALESCE(
							(
								SELECT bn_item->>'#text'
								FROM jsonb_array_elements(
									CASE jsonb_typeof(
										f.json
											-> 'processDataSet'
											-> 'processInformation'
											-> 'dataSetInformation'
											-> 'name'
											-> 'baseName'
									)
										WHEN 'array' THEN (
											f.json
												-> 'processDataSet'
												-> 'processInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										WHEN 'object' THEN jsonb_build_array(
											f.json
												-> 'processDataSet'
												-> 'processInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										ELSE '[]'::jsonb
									END
								) AS bn_item
								WHERE bn_item->>'@xml:lang' = order_lang
								LIMIT 1
							),
							(
								SELECT bn_item->>'#text'
								FROM jsonb_array_elements(
									CASE jsonb_typeof(
										f.json
											-> 'processDataSet'
											-> 'processInformation'
											-> 'dataSetInformation'
											-> 'name'
											-> 'baseName'
									)
										WHEN 'array' THEN (
											f.json
												-> 'processDataSet'
												-> 'processInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										WHEN 'object' THEN jsonb_build_array(
											f.json
												-> 'processDataSet'
												-> 'processInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										ELSE '[]'::jsonb
									END
								) AS bn_item
								WHERE bn_item->>'@xml:lang' = 'en'
								LIMIT 1
							),
							COALESCE(
								f.json #>> '{processDataSet,processInformation,dataSetInformation,name,baseName,0,#text}',
								f.json #>> '{processDataSet,processInformation,dataSetInformation,name,baseName,#text}'
							),
							''
						)
					END AS base_name
			) bn
			CROSS JOIN LATERAL (
				SELECT
					CASE
						WHEN use_common_class_order THEN COALESCE(
							(
								SELECT string_agg(cls_item->>'#text', ' / ' ORDER BY cls_level ASC)
								FROM (
									SELECT
										cls_item,
										CASE
											WHEN (cls_item->>'@level') ~ '^\\d+$' THEN (cls_item->>'@level')::int
											ELSE 2147483647
										END AS cls_level
									FROM jsonb_array_elements(
										CASE jsonb_typeof(
											f.json
												-> 'processDataSet'
												-> 'processInformation'
												-> 'dataSetInformation'
												-> 'classificationInformation'
												-> 'common:classification'
												-> 'common:class'
										)
											WHEN 'array' THEN (
												f.json
													-> 'processDataSet'
													-> 'processInformation'
													-> 'dataSetInformation'
													-> 'classificationInformation'
													-> 'common:classification'
													-> 'common:class'
											)
											WHEN 'object' THEN jsonb_build_array(
												f.json
													-> 'processDataSet'
													-> 'processInformation'
													-> 'dataSetInformation'
													-> 'classificationInformation'
													-> 'common:classification'
													-> 'common:class'
											)
											ELSE '[]'::jsonb
										END
									) AS cls_item
								) ordered_cls
							),
							''
						)
					END AS class_name
			) cls
			WHERE f.json @> filter_condition_jsonb
				AND f.json &@~ query_text
				AND (
					(data_source = 'tg' AND state_code = 100)
					OR (data_source = 'co' AND state_code = 200)
					OR (data_source = 'my' AND user_id = auth.uid())
					OR (
						data_source = 'te'
						AND EXISTS (
							SELECT 1
							FROM roles r
							WHERE r.user_id = auth.uid()
								AND r.team_id = f.team_id
								AND r.role::text IN ('admin', 'member', 'owner')
						)
					)
				)
		)
		SELECT
			ROW_NUMBER() OVER (
				ORDER BY
					(CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'asc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" ASC NULLS LAST,
					(CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'desc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" DESC NULLS LAST,
					CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'asc' THEN lower(f2.order_value) END ASC NULLS LAST,
					CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'desc' THEN lower(f2.order_value) END DESC NULLS LAST,
					f2.score DESC,
					f2.modified_at DESC,
					f2.id
			) AS rank,
			f2.id,
			f2.json,
			f2.version,
			f2.modified_at,
			f2.model_id,
			COUNT(*) OVER() AS total_count
		FROM filtered f2
		ORDER BY
			(CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'asc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" ASC NULLS LAST,
			(CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'desc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" DESC NULLS LAST,
			CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'asc' THEN lower(f2.order_value) END ASC NULLS LAST,
			CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'desc' THEN lower(f2.order_value) END DESC NULLS LAST,
			f2.score DESC,
			f2.modified_at DESC,
			f2.id
		LIMIT page_size
		OFFSET (page_current - 1) * page_size;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.pgroonga_search_sources(query_text text, filter_condition text DEFAULT ''::text, page_size bigint DEFAULT 10, page_current bigint DEFAULT 1, data_source text DEFAULT 'tg'::text, this_user_id text DEFAULT ''::text)
 RETURNS TABLE(rank bigint, id uuid, "json" jsonb, version character, modified_at timestamp with time zone, total_count bigint)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
    filter_condition_jsonb JSONB;
BEGIN
	filter_condition_jsonb := filter_condition::JSONB;
  RETURN QUERY
		SELECT 
			RANK () OVER (ORDER BY pgroonga_score(f.tableoid, f.ctid) DESC) AS rank, 
			f.id, 
			f.json,
			f.version,
			f.modified_at,
			COUNT(*) OVER() AS total_count
		FROM sources f
		WHERE f.json @> filter_condition_jsonb AND f.json &@~ query_text AND ((data_source = 'tg' AND state_code = 100) or (data_source = 'my' AND user_id::text = this_user_id))
		ORDER BY pgroonga_score(tableoid, ctid) DESC
		LIMIT page_size
		OFFSET (page_current -1) * page_size;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.pgroonga_search_unitgroups(query_text text, filter_condition text DEFAULT ''::text, page_size bigint DEFAULT 10, page_current bigint DEFAULT 1, data_source text DEFAULT 'tg'::text, this_user_id text DEFAULT ''::text)
 RETURNS TABLE(rank bigint, id uuid, "json" jsonb, version character, modified_at timestamp with time zone, total_count bigint)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
    filter_condition_jsonb JSONB;
BEGIN
 filter_condition_jsonb := filter_condition::JSONB;
  RETURN QUERY
  SELECT 
   RANK () OVER (ORDER BY extensions.pgroonga_score(f.tableoid, f.ctid) DESC) AS rank, 
   f.id, 
   f.json,
   f.version,
   f.modified_at,
   COUNT(*) OVER() AS total_count
  FROM public.unitgroups f
  WHERE f.json @> filter_condition_jsonb 
    AND f.json &@~ query_text 
    AND (
         (data_source = 'tg' AND f.state_code = 100)
         OR 
         (data_source = 'my' AND f.user_id::text = this_user_id)
        )
  ORDER BY extensions.pgroonga_score(f.tableoid, f.ctid) DESC
  LIMIT page_size
  OFFSET (page_current -1) * page_size;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.policy_is_current_user_in_roles(p_team_id uuid, p_roles_to_check text[])
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
	-- 增加空数组判断：空数组直接返回 false
    SELECT CASE 
        WHEN cardinality(p_roles_to_check) = 0 THEN false  -- cardinality() 获取数组长度
		-- 核心逻辑：用 EXISTS 判断是否存在匹配记录，效率更高（无需聚合，找到即返回）
        ELSE EXISTS (
        SELECT 1
        FROM public.roles r
        WHERE r.user_id = auth.uid()                  -- 匹配当前登录用户
          AND r.team_id = p_team_id                   -- 匹配目标团队
          AND r.role <> 'rejected'::text              -- 排除无效的「拒绝」角色
          AND r.role = ANY(p_roles_to_check)          -- 关键：判断用户角色是否在输入的角色数组中（任意一个匹配即可）
     )
	 END;
$function$
;

CREATE OR REPLACE FUNCTION public.policy_is_team_id_used(_team_id uuid)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT EXISTS (
    SELECT 1
    FROM public.teams t
    WHERE t.id = _team_id);
$function$
;

CREATE OR REPLACE FUNCTION public.policy_is_team_public(_team_id uuid)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT EXISTS (
    SELECT 1
    FROM public.teams t
    WHERE t.id = _team_id
      AND t.is_public);
$function$
;

CREATE OR REPLACE FUNCTION public.policy_roles_delete(_user_id uuid, _team_id uuid, _role text)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT (
	-- 验证当前用户是否为团队管理员或拥有者，被删除用户角色不能为owner角色，自己不能删除自己
	(
		_role <> 'owner' AND _user_id <> auth.uid() AND
		EXISTS (
			SELECT 1
			FROM public.roles r
			WHERE r.user_id = auth.uid() AND r.team_id = _team_id AND (r.role = 'admin' OR r.role = 'owner' OR r.role = 'review-admin'))
	)
  );
$function$
;

CREATE OR REPLACE FUNCTION public.policy_roles_insert(_user_id uuid, _team_id uuid, _role text)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT (
    ((
        -- 验证用户是否已经有团队角色，且角色不为rejected
        EXISTS (
            SELECT 1
            FROM public.roles r
            WHERE r.user_id = _user_id
            AND r.role <> 'rejected'
            and r.team_id <> '00000000-0000-0000-0000-000000000000')
        ) = false

    AND
    (
        -- 验证当前用户创建团队时，是否为自己分配owner角色，且团队ID未被使用
        ((
            (_user_id = auth.uid() AND _role = 'owner' AND 
            EXISTS (
                SELECT 1
                FROM public.roles r
                WHERE r.team_id = _team_id) = false)
        ))

        OR
        -- 验证当前用户是否为团队管理员或拥有者，邀请的用户角色是否为is_invited角色
        ((
            _role = 'is_invited' AND 
            EXISTS (
                SELECT 1
                FROM public.roles r
                WHERE r.user_id = auth.uid() AND r.team_id = _team_id AND (r.role = 'admin' OR r.role = 'owner'))
        ))
    ))

    OR
    (
        -- 验证用户是否已经有审核团队角色
        EXISTS (
            SELECT 1
            FROM public.roles r
            WHERE r.user_id = _user_id
            AND r.role like 'review-%'
            AND r.team_id = '00000000-0000-0000-0000-000000000000') = false

        AND
        -- 验证当前用户是否为审核管理员，邀请的用户角色是否为review-member角色
        (
        _role = 'review-member' AND _team_id = '00000000-0000-0000-0000-000000000000'::uuid AND
        EXISTS (
            SELECT 1
            FROM public.roles r
            WHERE r.user_id = auth.uid() AND r.team_id = _team_id AND r.role = 'review-admin')
        )
    )

    OR
    (
        -- 验证用户是否已经有系统团队角色
        EXISTS (
            SELECT 1
            FROM public.roles r
            WHERE r.user_id = _user_id
            AND (r.role = 'admin' OR r.role = 'member')
            AND r.team_id = '00000000-0000-0000-0000-000000000000') = false

        AND
        -- 验证当前用户是否为系统管理员，邀请的用户角色是否为member角色
        (
        _role = 'member' AND _team_id = '00000000-0000-0000-0000-000000000000'::uuid AND
        EXISTS (
            SELECT 1
            FROM public.roles r
            WHERE r.user_id = auth.uid() AND r.team_id = _team_id AND r.role = 'admin')
        )
    )

    );
$function$
;

CREATE OR REPLACE FUNCTION public.policy_roles_select(_team_id uuid, _role text)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT (
  	-- 验证当前用户是否为团队成员（非拒绝状态）
    EXISTS (
		select 1 from public.roles r0
		where r0.user_id = auth.uid() and r0.team_id = _team_id and r0.role <> 'rejected')

    OR
    -- 验证当前用户是否为审核团队/系统管理团队成员
    (_team_id = '00000000-0000-0000-0000-000000000000'::uuid and
    EXISTS (
		select 1 from public.roles r0
		where r0.user_id = auth.uid() and r0.team_id = _team_id))

    OR
    -- 验证当前团队是否为公开团队的拥有者，用于展示加入团队的联系信息
    _role = 'owner' AND
    EXISTS (
        SELECT 1 FROM public.teams t
        WHERE t.id = _team_id AND t.is_public)
	);
$function$
;

CREATE OR REPLACE FUNCTION public.policy_roles_update(_user_id uuid, _team_id uuid, _role text)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT (
  	-- 验证当前用户是否为团队拥有者或管理员
	(
  	EXISTS (
		select 1 from public.roles r0
		where r0.user_id = auth.uid() and r0.team_id = _team_id and (r0.role ='admin' or r0.role='owner'))
	and
	(
	-- 切换admin和member
	((_role = 'admin' or _role = 'member') and 
	  EXISTS (
		SELECT 1
		FROM public.roles r1
		WHERE r1.user_id = _user_id and r1.team_id = _team_id and (r1.role = 'admin' or r1.role = 'member')))
	or 
	-- 重新邀请已经拒绝的用户
	(_role = 'is_invited' and 
		EXISTS (
			SELECT 1
			FROM public.roles r2
			WHERE r2.user_id = _user_id and r2.team_id = _team_id and r2.role = 'rejected'))
	))
	or
	-- 验证当前用户，接受邀请或拒绝邀请
	((_role = 'member' or _role = 'rejected') and _user_id = auth.uid() and
	EXISTS (
		select 1 from public.roles r3
		where r3.user_id = _user_id and r3.team_id = _team_id and r3.role ='is_invited'))
	);
$function$
;

CREATE OR REPLACE FUNCTION public.policy_user_has_team(_user_id uuid)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  SELECT EXISTS (
    SELECT 1
    FROM public.roles r
    WHERE r.user_id = _user_id
      AND r.role <> 'rejected'
	  and r.team_id <> '00000000-0000-0000-0000-000000000000');
$function$
;

CREATE OR REPLACE FUNCTION public.processes_embedding_ft_input(proc public.processes)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
begin
  return proc.extracted_md;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.processes_embedding_input(proc public.processes)
 RETURNS text
 LANGUAGE plpgsql
 IMMUTABLE
 SET search_path TO ''
AS $function$
begin
  return proc.extracted_text;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.processes_sync_jsonb_version()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public', 'pg_temp'
AS $function$
BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb THEN
        NEW.json := NEW.json_ordered;
        NEW.version := COALESCE(NEW.json->'processDataSet'->'administrativeInformation'->'publicationAndOwnership'->>'common:dataSetVersion',
					''
        );
    END IF;
    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.qry_notification_get_my_data_count(p_days integer DEFAULT 3, p_last_view_at timestamp with time zone DEFAULT NULL::timestamp with time zone)
 RETURNS integer
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select count(*)::integer
  from public.reviews as r
  where coalesce(r.json -> 'user' ->> 'id', '') = auth.uid()::text
    and r.state_code in (1, -1, 2)
    and (
      (p_last_view_at is not null and r.modified_at > p_last_view_at) or
      (p_last_view_at is null and (
        coalesce(p_days, 3) <= 0 or
        r.modified_at >= now() - make_interval(days => greatest(coalesce(p_days, 3), 0))
      ))
    );
$function$
;

CREATE OR REPLACE FUNCTION public.qry_notification_get_my_data_items(p_page integer DEFAULT 1, p_page_size integer DEFAULT 10, p_days integer DEFAULT 3)
 RETURNS TABLE(id uuid, state_code integer, "json" jsonb, modified_at timestamp with time zone, total_count integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select
    r.id,
    r.state_code,
    coalesce(r.json, '{}'::jsonb) as json,
    r.modified_at,
    count(*) over ()::integer as total_count
  from public.reviews as r
  where coalesce(r.json -> 'user' ->> 'id', '') = auth.uid()::text
    and r.state_code in (1, -1, 2)
    and (
      coalesce(p_days, 3) <= 0 or
      r.modified_at >= now() - make_interval(days => greatest(coalesce(p_days, 3), 0))
    )
  order by r.modified_at desc
  offset greatest(coalesce(p_page, 1) - 1, 0) * greatest(coalesce(p_page_size, 10), 1)
  limit greatest(coalesce(p_page_size, 10), 1);
$function$
;

CREATE OR REPLACE FUNCTION public.qry_notification_get_my_issue_count(p_days integer DEFAULT 3, p_last_view_at timestamp with time zone DEFAULT NULL::timestamp with time zone)
 RETURNS integer
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select count(*)::integer
  from public.notifications as n
  where n.recipient_user_id = auth.uid()
    and n.type = 'validation_issue'
    and (
      (p_last_view_at is not null and n.modified_at > p_last_view_at) or
      (p_last_view_at is null and (
        coalesce(p_days, 3) <= 0 or
        n.modified_at >= now() - make_interval(days => greatest(coalesce(p_days, 3), 0))
      ))
    );
$function$
;

CREATE OR REPLACE FUNCTION public.qry_notification_get_my_issue_items(p_page integer DEFAULT 1, p_page_size integer DEFAULT 10, p_days integer DEFAULT 3)
 RETURNS TABLE(id uuid, type text, dataset_type text, dataset_id uuid, dataset_version text, "json" jsonb, modified_at timestamp with time zone, total_count integer)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select
    n.id,
    n.type,
    n.dataset_type,
    n.dataset_id,
    n.dataset_version,
    n.json,
    n.modified_at,
    count(*) over ()::integer as total_count
  from public.notifications as n
  where n.recipient_user_id = auth.uid()
    and n.type = 'validation_issue'
    and (
      coalesce(p_days, 3) <= 0 or
      n.modified_at >= now() - make_interval(days => greatest(coalesce(p_days, 3), 0))
    )
  order by n.modified_at desc
  offset greatest(coalesce(p_page, 1) - 1, 0) * greatest(coalesce(p_page_size, 10), 1)
  limit greatest(coalesce(p_page_size, 10), 1);
$function$
;

CREATE OR REPLACE FUNCTION public.qry_notification_get_my_team_count(p_days integer DEFAULT 3, p_last_view_at timestamp with time zone DEFAULT NULL::timestamp with time zone)
 RETURNS integer
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select count(*)::integer
  from public.roles as r
  where r.user_id = auth.uid()
    and r.role = 'is_invited'
    and r.team_id <> '00000000-0000-0000-0000-000000000000'::uuid
    and (
      (p_last_view_at is not null and r.modified_at > p_last_view_at) or
      (p_last_view_at is null and (
        coalesce(p_days, 3) <= 0 or
        r.modified_at >= now() - make_interval(days => greatest(coalesce(p_days, 3), 0))
      ))
    );
$function$
;

CREATE OR REPLACE FUNCTION public.qry_notification_get_my_team_items(p_days integer DEFAULT 3)
 RETURNS TABLE(team_id uuid, user_id uuid, role text, team_title jsonb, modified_at timestamp with time zone)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
  select
    r.team_id,
    r.user_id,
    r.role,
    coalesce(t.json -> 'title', '[]'::jsonb) as team_title,
    r.modified_at
  from public.roles as r
  join public.teams as t
    on t.id = r.team_id
  where r.user_id = auth.uid()
    and r.team_id <> '00000000-0000-0000-0000-000000000000'::uuid
    and (
      coalesce(p_days, 3) <= 0 or
      r.modified_at >= now() - make_interval(days => greatest(coalesce(p_days, 3), 0))
    )
  order by r.modified_at desc;
$function$
;

CREATE OR REPLACE FUNCTION public.qry_review_get_member_list(p_page integer DEFAULT 1, p_page_size integer DEFAULT 10, p_sort_by text DEFAULT 'created_at'::text, p_sort_order text DEFAULT 'desc'::text, p_role text DEFAULT NULL::text)
 RETURNS TABLE(user_id uuid, team_id uuid, role text, email text, display_name text, created_at timestamp with time zone, modified_at timestamp with time zone, total_count bigint)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_team_id uuid := '00000000-0000-0000-0000-000000000000'::uuid;
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_order_by text := public.cmd_membership_resolve_member_order_by(p_sort_by, false);
  v_order_dir text := public.cmd_membership_resolve_sort_direction(p_sort_order);
begin
  if v_actor is null then
    return;
  end if;

  if not public.cmd_membership_is_review_admin(v_actor) then
    return;
  end if;

  return query execute format(
    $sql$
      with members as (
        select
          r.user_id,
          r.team_id,
          r.role::text as role,
          coalesce(u.raw_user_meta_data->>'email', '') as email,
          coalesce(
            nullif(u.raw_user_meta_data->>'display_name', ''),
            u.raw_user_meta_data->>'email',
            '-'
          ) as display_name,
          r.created_at,
          r.modified_at
        from public.roles as r
        left join public.users as u
          on u.id = r.user_id
        where r.team_id = $1
          and r.role in ('review-admin', 'review-member')
          and ($4::text is null or r.role = $4::text)
      )
      select
        m.user_id,
        m.team_id,
        m.role,
        m.email,
        m.display_name,
        m.created_at,
        m.modified_at,
        count(*) over() as total_count
      from members as m
      order by %s %s nulls last, m.user_id asc
      limit $2
      offset $3
    $sql$,
    v_order_by,
    v_order_dir
  )
  using v_team_id, v_limit, v_offset, p_role;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.qry_review_get_member_workload(p_page integer DEFAULT 1, p_page_size integer DEFAULT 10, p_sort_by text DEFAULT 'created_at'::text, p_sort_order text DEFAULT 'desc'::text, p_role text DEFAULT NULL::text)
 RETURNS TABLE(user_id uuid, team_id uuid, role text, email text, display_name text, pending_count bigint, reviewed_count bigint, created_at timestamp with time zone, modified_at timestamp with time zone, total_count bigint)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_team_id uuid := '00000000-0000-0000-0000-000000000000'::uuid;
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_order_by text := public.cmd_membership_resolve_member_order_by(p_sort_by, true);
  v_order_dir text := public.cmd_membership_resolve_sort_direction(p_sort_order);
begin
  if v_actor is null then
    return;
  end if;

  if not public.cmd_membership_is_review_admin(v_actor) then
    return;
  end if;

  return query execute format(
    $sql$
      with members as (
        select
          r.user_id,
          r.team_id,
          r.role::text as role,
          coalesce(u.raw_user_meta_data->>'email', '') as email,
          coalesce(
            nullif(u.raw_user_meta_data->>'display_name', ''),
            u.raw_user_meta_data->>'email',
            '-'
          ) as display_name,
          coalesce(w.pending_count, 0) as pending_count,
          coalesce(w.reviewed_count, 0) as reviewed_count,
          r.created_at,
          r.modified_at
        from public.roles as r
        left join public.users as u
          on u.id = r.user_id
        left join lateral (
          select
            count(*) filter (
              where c.state_code = 0
                and rv.state_code > 0
            ) as pending_count,
            count(*) filter (
              where c.state_code in (1, 2)
                and rv.state_code > 0
            ) as reviewed_count
          from public.comments as c
          join public.reviews as rv
            on rv.id = c.review_id
          where c.reviewer_id = r.user_id
            and c.state_code in (0, 1, 2)
        ) as w on true
        where r.team_id = $1
          and r.role in ('review-admin', 'review-member')
          and ($4::text is null or r.role = $4::text)
      )
      select
        m.user_id,
        m.team_id,
        m.role,
        m.email,
        m.display_name,
        m.pending_count,
        m.reviewed_count,
        m.created_at,
        m.modified_at,
        count(*) over() as total_count
      from members as m
      order by %s %s nulls last, m.user_id asc
      limit $2
      offset $3
    $sql$,
    v_order_by,
    v_order_dir
  )
  using v_team_id, v_limit, v_offset, p_role;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.qry_system_get_member_list(p_page integer DEFAULT 1, p_page_size integer DEFAULT 10, p_sort_by text DEFAULT 'created_at'::text, p_sort_order text DEFAULT 'desc'::text)
 RETURNS TABLE(user_id uuid, team_id uuid, role text, email text, display_name text, created_at timestamp with time zone, modified_at timestamp with time zone, total_count bigint)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_team_id uuid := '00000000-0000-0000-0000-000000000000'::uuid;
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_order_by text := public.cmd_membership_resolve_member_order_by(p_sort_by, false);
  v_order_dir text := public.cmd_membership_resolve_sort_direction(p_sort_order);
begin
  if v_actor is null then
    return;
  end if;

  if not public.cmd_membership_is_system_manager(v_actor) then
    return;
  end if;

  return query execute format(
    $sql$
      with members as (
        select
          r.user_id,
          r.team_id,
          r.role::text as role,
          coalesce(u.raw_user_meta_data->>'email', '') as email,
          coalesce(
            nullif(u.raw_user_meta_data->>'display_name', ''),
            u.raw_user_meta_data->>'email',
            '-'
          ) as display_name,
          r.created_at,
          r.modified_at
        from public.roles as r
        left join public.users as u
          on u.id = r.user_id
        where r.team_id = $1
          and r.role in ('owner', 'admin', 'member')
      )
      select
        m.user_id,
        m.team_id,
        m.role,
        m.email,
        m.display_name,
        m.created_at,
        m.modified_at,
        count(*) over() as total_count
      from members as m
      order by %s %s nulls last, m.user_id asc
      limit $2
      offset $3
    $sql$,
    v_order_by,
    v_order_dir
  )
  using v_team_id, v_limit, v_offset;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.qry_team_get_member_list(p_team_id uuid, p_page integer DEFAULT 1, p_page_size integer DEFAULT 10, p_sort_by text DEFAULT 'created_at'::text, p_sort_order text DEFAULT 'desc'::text)
 RETURNS TABLE(user_id uuid, team_id uuid, role text, email text, display_name text, created_at timestamp with time zone, modified_at timestamp with time zone, total_count bigint)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_order_by text := public.cmd_membership_resolve_member_order_by(p_sort_by, false);
  v_order_dir text := public.cmd_membership_resolve_sort_direction(p_sort_order);
begin
  if v_actor is null then
    return;
  end if;

  if not public.cmd_membership_is_team_manager(v_actor, p_team_id) then
    return;
  end if;

  return query execute format(
    $sql$
      with members as (
        select
          r.user_id,
          r.team_id,
          r.role::text as role,
          coalesce(u.raw_user_meta_data->>'email', '') as email,
          coalesce(
            nullif(u.raw_user_meta_data->>'display_name', ''),
            u.raw_user_meta_data->>'email',
            '-'
          ) as display_name,
          r.created_at,
          r.modified_at
        from public.roles as r
        left join public.users as u
          on u.id = r.user_id
        where r.team_id = $1
      )
      select
        m.user_id,
        m.team_id,
        m.role,
        m.email,
        m.display_name,
        m.created_at,
        m.modified_at,
        count(*) over() as total_count
      from members as m
      order by %s %s nulls last, m.user_id asc
      limit $2
      offset $3
    $sql$,
    v_order_by,
    v_order_dir
  )
  using p_team_id, v_limit, v_offset;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.save_lifecycle_model_bundle(p_plan jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
DECLARE
    v_mode text := coalesce(p_plan->>'mode', '');
    v_model_id uuid := nullif(p_plan->>'modelId', '')::uuid;
    v_expected_version text := nullif(btrim(coalesce(p_plan->>'version', '')), '');
    v_actor_user_id uuid := nullif(p_plan->>'actorUserId', '')::uuid;
    v_parent jsonb := coalesce(p_plan->'parent', '{}'::jsonb);
    v_parent_json_ordered json := (v_parent->'jsonOrdered')::json;
    v_parent_json_tg jsonb := coalesce(v_parent->'jsonTg', '{}'::jsonb);
    v_parent_rule_verification boolean := coalesce((v_parent->>'ruleVerification')::boolean, true);
    v_process_mutations jsonb := coalesce(p_plan->'processMutations', '[]'::jsonb);
    v_mutation jsonb;
    v_child_id uuid;
    v_child_version text;
    v_child_json_ordered json;
    v_child_rule_verification boolean;
    v_result_row lifecyclemodels%ROWTYPE;
    v_rows_affected integer;
BEGIN
    IF v_mode NOT IN ('create', 'update') THEN
        RAISE EXCEPTION 'INVALID_PLAN';
    END IF;

    IF v_model_id IS NULL OR v_parent_json_ordered IS NULL THEN
        RAISE EXCEPTION 'INVALID_PLAN';
    END IF;

    IF v_actor_user_id IS NULL THEN
        RAISE EXCEPTION 'INVALID_PLAN';
    END IF;

    IF jsonb_typeof(v_process_mutations) <> 'array' THEN
        RAISE EXCEPTION 'INVALID_PLAN';
    END IF;

    IF v_mode = 'update' THEN
        IF v_expected_version IS NULL THEN
            RAISE EXCEPTION 'INVALID_PLAN';
        END IF;

        PERFORM 1
          FROM lifecyclemodels
         WHERE id = v_model_id
           AND version = v_expected_version
         FOR UPDATE;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'MODEL_NOT_FOUND';
        END IF;
    END IF;

    FOR v_mutation IN
        SELECT value
          FROM jsonb_array_elements(v_process_mutations)
    LOOP
        CASE coalesce(v_mutation->>'op', '')
            WHEN 'delete' THEN
                v_child_id := nullif(v_mutation->>'id', '')::uuid;
                v_child_version := nullif(btrim(coalesce(v_mutation->>'version', '')), '');

                IF v_child_id IS NULL OR v_child_version IS NULL THEN
                    RAISE EXCEPTION 'INVALID_PLAN';
                END IF;

                EXECUTE 'del' || 'ete from processes where id = $1 and version = $2 and model_id = $3'
                   USING v_child_id, v_child_version, v_model_id;

                GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
                IF v_rows_affected = 0 THEN
                    RAISE EXCEPTION 'PROCESS_NOT_FOUND';
                END IF;
            WHEN 'create' THEN
                v_child_id := nullif(v_mutation->>'id', '')::uuid;
                v_child_json_ordered := (v_mutation->'jsonOrdered')::json;
                v_child_rule_verification := coalesce(
                    (v_mutation->>'ruleVerification')::boolean,
                    true
                );

                IF v_child_id IS NULL OR v_child_json_ordered IS NULL THEN
                    RAISE EXCEPTION 'INVALID_PLAN';
                END IF;

                BEGIN
                    INSERT INTO processes (
                        id,
                        json_ordered,
                        model_id,
                        user_id,
                        rule_verification
                    )
                    VALUES (
                        v_child_id,
                        v_child_json_ordered,
                        v_model_id,
                        v_actor_user_id,
                        v_child_rule_verification
                    );
                EXCEPTION
                    WHEN unique_violation THEN
                        RAISE EXCEPTION 'VERSION_CONFLICT';
                END;
            WHEN 'update' THEN
                v_child_id := nullif(v_mutation->>'id', '')::uuid;
                v_child_version := nullif(btrim(coalesce(v_mutation->>'version', '')), '');
                v_child_json_ordered := (v_mutation->'jsonOrdered')::json;
                v_child_rule_verification := coalesce(
                    (v_mutation->>'ruleVerification')::boolean,
                    true
                );

                IF v_child_id IS NULL OR v_child_version IS NULL OR v_child_json_ordered IS NULL THEN
                    RAISE EXCEPTION 'INVALID_PLAN';
                END IF;

                UPDATE processes
                   SET json_ordered = v_child_json_ordered,
                       model_id = v_model_id,
                       rule_verification = v_child_rule_verification
                 WHERE id = v_child_id
                   AND version = v_child_version
                   AND model_id = v_model_id;

                IF NOT FOUND THEN
                    RAISE EXCEPTION 'PROCESS_NOT_FOUND';
                END IF;
            ELSE
                RAISE EXCEPTION 'INVALID_PLAN';
        END CASE;
    END LOOP;

    IF v_mode = 'create' THEN
        BEGIN
            INSERT INTO lifecyclemodels (
                id,
                json_ordered,
                json_tg,
                user_id,
                rule_verification
            )
            VALUES (
                v_model_id,
                v_parent_json_ordered,
                v_parent_json_tg,
                v_actor_user_id,
                v_parent_rule_verification
            )
            RETURNING *
                 INTO v_result_row;
        EXCEPTION
            WHEN unique_violation THEN
                RAISE EXCEPTION 'VERSION_CONFLICT';
        END;
    ELSE
        UPDATE lifecyclemodels
           SET json_ordered = v_parent_json_ordered,
               json_tg = v_parent_json_tg,
               rule_verification = v_parent_rule_verification
         WHERE id = v_model_id
           AND version = v_expected_version
        RETURNING *
             INTO v_result_row;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'MODEL_NOT_FOUND';
        END IF;
    END IF;

    RETURN jsonb_build_object(
        'model_id', v_result_row.id,
        'version', v_result_row.version,
        'lifecycle_model', to_jsonb(v_result_row)
    );
END;
$function$
;

CREATE OR REPLACE FUNCTION public.semantic_search(query_embedding text, match_threshold double precision DEFAULT 0.5, match_count integer DEFAULT 20)
 RETURNS TABLE(rank bigint, id uuid, "json" jsonb)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
    query_embedding_vector vector(384);
BEGIN
    -- Convert the input TEXT to vector(1536) once
    query_embedding_vector := query_embedding::vector(384);

    RETURN QUERY
    SELECT
        RANK () OVER (ORDER BY f.embedding <=> query_embedding_vector) AS rank,
        f.id,
        f.json
    FROM flows f
    WHERE f.embedding <=> query_embedding_vector < 1 - match_threshold
    ORDER BY f.embedding <=> query_embedding_vector
    LIMIT match_count;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.semantic_search_flows_v1(query_embedding text, filter_condition text DEFAULT ''::text, match_threshold double precision DEFAULT 0.5, match_count integer DEFAULT 20, data_source text DEFAULT 'tg'::text)
 RETURNS TABLE(rank bigint, id uuid, "json" jsonb, version character, modified_at timestamp with time zone, total_count bigint)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  query_embedding_vector  vector(1024);
  filter_condition_jsonb  jsonb;
  flowType                text;
  flowTypeArray           text[];
  asInput                 boolean;
  candidate_size          int := GREATEST(match_count * 10, 200);
BEGIN
  -- 1) 向量转 halfvec(384)
  query_embedding_vector := query_embedding::vector(1024);

  -- 2) 解析 filter_condition
  filter_condition_jsonb := filter_condition::jsonb;
  flowType               := filter_condition_jsonb->>'flowType';
  flowTypeArray          := string_to_array(flowType, ',');
  filter_condition_jsonb := filter_condition_jsonb - 'flowType';

  asInput                := (filter_condition_jsonb->'asInput')::boolean;
  filter_condition_jsonb := filter_condition_jsonb - 'asInput';

  -- 3) 两阶段：先 HNSW 候选，再业务过滤
  RETURN QUERY
  WITH cand AS (
    SELECT
      f.id,
      f.json,
      f.version,
      f.modified_at,
      f.embedding_ft,
      f.state_code,
      f.user_id,
      f.team_id
    FROM public.flows f
    ORDER BY f.embedding_ft <=> query_embedding_vector   
    LIMIT candidate_size
  ),
  final AS (
    SELECT
      c.*,
      (c.embedding_ft <=> query_embedding_vector) AS dist
    FROM cand c
    WHERE
      (c.embedding_ft <=> query_embedding_vector) < 1 - match_threshold
      AND c.json @> filter_condition_jsonb
      AND (
           (data_source = 'tg' AND c.state_code = 100)
        OR (data_source = 'my' AND c.user_id = auth.uid())
      )
      AND (
        flowType IS NULL
        OR flowType = ''
        OR (c.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = ANY(flowTypeArray)
      )
      AND (
        asInput IS NULL
        OR asInput = false
        OR NOT (
          c.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
        )
      )
  )
  SELECT
  RANK() OVER (ORDER BY f2.dist) AS "rank",
  f2.id,
  f2.json,
  f2.version,
  f2.modified_at,
  COUNT(*) OVER()               AS total_count
FROM final AS f2
ORDER BY f2.dist
LIMIT match_count;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.semantic_search_lifecyclemodels_v1(query_embedding text, filter_condition text DEFAULT ''::text, match_threshold double precision DEFAULT 0.5, match_count integer DEFAULT 20, data_source text DEFAULT 'tg'::text)
 RETURNS TABLE(rank bigint, id uuid, "json" jsonb, version character, modified_at timestamp with time zone, total_count bigint)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  query_embedding_vector  vector(1024);   
  filter_condition_jsonb  jsonb;
  candidate_size          int := GREATEST(match_count * 10, 200);
BEGIN
  -- 1) 向量入参 -> vector(384)
  query_embedding_vector := query_embedding::vector(1024);

  -- 2) 解析 filter_condition
  filter_condition_jsonb := filter_condition::jsonb;

  -- 3) 两阶段：先用向量索引取候选，再应用阈值/过滤/权限，最后排序分页
  RETURN QUERY
  WITH cand AS (
    SELECT
      m.id,
      m.json,
      m.version,
      m.modified_at,
      m.embedding_ft,
      m.state_code,
      m.user_id
    FROM public.lifecyclemodels AS m
    ORDER BY m.embedding_ft <=> query_embedding_vector      
    LIMIT candidate_size
  ),
  final AS (
    SELECT
      c.*,
      (c.embedding_ft <=> query_embedding_vector) AS dist
    FROM cand AS c
    WHERE
      -- 向量阈值（在候选集上应用）
      (c.embedding_ft <=> query_embedding_vector) < 1 - match_threshold
      -- JSON 过滤
      AND c.json @> filter_condition_jsonb
      -- data_source 访问控制（与原逻辑一致）
      AND (
           (data_source = 'tg' AND c.state_code = 100)
        OR (data_source = 'my' AND c.user_id = auth.uid())
      )
  )
  SELECT
    RANK() OVER (ORDER BY f2.dist) AS "rank",
    f2.id,
    f2.json,
    f2.version,
    f2.modified_at,
    COUNT(*) OVER()               AS total_count
  FROM final AS f2
  ORDER BY f2.dist
  LIMIT match_count;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.semantic_search_processes_v1(query_embedding text, filter_condition text DEFAULT ''::text, match_threshold double precision DEFAULT 0.5, match_count integer DEFAULT 20, data_source text DEFAULT 'tg'::text)
 RETURNS TABLE(rank bigint, id uuid, "json" jsonb, version character, modified_at timestamp with time zone, total_count bigint)
 LANGUAGE plpgsql
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  query_embedding_vector  vector(1024);   -- 若列为 halfvec(384)，这里改成 halfvec(384)
  filter_condition_jsonb  jsonb;
  candidate_size          int := GREATEST(match_count * 10, 200);
BEGIN
  -- 1) 向量入参转 vector(384)（或 halfvec(384)）
  query_embedding_vector := query_embedding::vector(1024);

  -- 2) 解析 filter_condition
  filter_condition_jsonb := filter_condition::jsonb;

  -- 3) 两阶段：先按相似度取候选（命中向量索引），再在候选上施加全部业务过滤/阈值
  RETURN QUERY
  WITH cand AS (
    SELECT
      p.id,
      p.json,
      p.version,
      p.modified_at,
      p.embedding_ft,
      p.state_code,
      p.user_id
    FROM public.processes AS p
    ORDER BY p.embedding_ft <=> query_embedding_vector      
  ),
  final AS (
    SELECT
      c.*,
      (c.embedding_ft <=> query_embedding_vector) AS dist
    FROM cand AS c
    WHERE
      -- 向量阈值（在候选集上应用）
      (c.embedding_ft <=> query_embedding_vector) < 1 - match_threshold
      -- JSON 过滤
      AND c.json @> filter_condition_jsonb
      -- data_source 访问控制（保持你原逻辑）
      AND (
           (data_source = 'tg' AND c.state_code = 100)
        OR (data_source = 'my' AND c.user_id = auth.uid())
      )
  )
  SELECT
    RANK() OVER (ORDER BY f2.dist) AS "rank",
    f2.id,
    f2.json,
    f2.version,
    f2.modified_at,
    COUNT(*) OVER()               AS total_count
  FROM final AS f2
  ORDER BY f2.dist
  LIMIT match_count;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.sources_sync_jsonb_version()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public', 'pg_temp'
AS $function$
BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb THEN
        NEW.json := NEW.json_ordered;
        NEW.version := COALESCE(NEW.json->'sourceDataSet'->'administrativeInformation'->'publicationAndOwnership'->>'common:dataSetVersion',
					''
        );
    END IF;
    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.sync_auth_users_to_public_users()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
BEGIN
    -- 处理插入操作
    IF TG_OP = 'INSERT' THEN
        INSERT INTO public.users (id, raw_user_meta_data)
        VALUES (NEW.id, NEW.raw_user_meta_data);
    -- 处理更新操作
    ELSIF TG_OP = 'UPDATE' THEN
		IF NEW.raw_user_meta_data != OLD.raw_user_meta_data THEN
			UPDATE public.users
			SET raw_user_meta_data = NEW.raw_user_meta_data
			WHERE id = NEW.id;
    	END IF;
    -- 处理删除操作
    ELSIF TG_OP = 'DELETE' THEN
        DELETE FROM public.users
        WHERE id = OLD.id;
    END IF;
    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.sync_json_to_jsonb()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public', 'pg_temp'
AS $function$BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb
    THEN
        NEW.json := NEW.json_ordered;
    END IF;
    RETURN NEW;
END;$function$
;

CREATE OR REPLACE FUNCTION public.unitgroups_sync_jsonb_version()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO 'public', 'pg_temp'
AS $function$
BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb THEN
        NEW.json := NEW.json_ordered;
        NEW.version :=  COALESCE(NEW.json->'unitGroupDataSet'->'administrativeInformation'->'publicationAndOwnership'->>'common:dataSetVersion'
		,
					''
        );
    END IF;
    RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.update_modified_at()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
begin
  new.modified_at = now();
  return new;
end;
$function$
;

CREATE OR REPLACE FUNCTION util.clear_column()
 RETURNS trigger
 LANGUAGE plpgsql
 SET search_path TO ''
AS $function$
declare
    clear_column text := TG_ARGV[0];
begin
    NEW := NEW #= public.hstore(clear_column, NULL);
    return NEW;
end;
$function$
;

CREATE OR REPLACE FUNCTION util.invoke_edge_function(name text, body jsonb, timeout_milliseconds integer DEFAULT ((5 * 60) * 1000))
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
AS $function$
declare
  service_key text;
begin
  service_key := util.project_secret_key();

  perform net.http_post(
    url => util.project_url() || '/functions/v1/' || name,
    headers => jsonb_build_object(
      'Content-Type', 'application/json',
      'apikey', service_key,
      'x_region', 'us-east-1'
    ),
    body => body,
    timeout_milliseconds => timeout_milliseconds
  );
end;
$function$
;

CREATE OR REPLACE FUNCTION util.invoke_edge_webhook()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
AS $function$
declare
  edge_function text := TG_ARGV[0];
  timeout_milliseconds integer := coalesce(nullif(TG_ARGV[1], '')::integer, 1000);
  payload jsonb;
begin
  if edge_function is null or edge_function = '' then
    raise exception 'Missing webhook edge function name';
  end if;

  payload := jsonb_build_object(
    'type', TG_OP,
    'schema', TG_TABLE_SCHEMA,
    'table', TG_TABLE_NAME,
    'record', case when TG_OP = 'DELETE' then to_jsonb(OLD) else to_jsonb(NEW) end,
    'old_record', case when TG_OP = 'INSERT' then null else to_jsonb(OLD) end
  );

  perform util.invoke_edge_function(
    name => edge_function,
    body => payload,
    timeout_milliseconds => timeout_milliseconds
  );

  if TG_OP = 'DELETE' then
    return OLD;
  end if;

  return NEW;
end;
$function$
;

CREATE OR REPLACE FUNCTION util.process_embeddings(batch_size integer DEFAULT 10, max_requests integer DEFAULT 10, timeout_milliseconds integer DEFAULT ((5 * 60) * 1000))
 RETURNS void
 LANGUAGE plpgsql
 SET search_path TO ''
AS $function$
declare
  job_batches jsonb[];
  batch jsonb;
	edge_fn text;
begin
  with
    -- First get jobs and assign batch numbers
    numbered_jobs as (
      select
        message || jsonb_build_object('jobId', msg_id) as job_info,
        (row_number() over (order by 1) - 1) / batch_size as batch_num
      from pgmq.read(
        queue_name => 'embedding_jobs',
        vt => timeout_milliseconds / 1000,
        qty => max_requests * batch_size
      )
    ),
    -- Then group jobs into batches
    batched_jobs as (
      select
        jsonb_agg(job_info) as batch_array,
        batch_num
      from numbered_jobs
      group by batch_num, job_info->>'edgeFunction'
    )
  -- Finally aggregate all batches into array
  select array_agg(batch_array)
  from batched_jobs
  into job_batches;
	
	if job_batches is null then
    return;
  end if;

  -- Invoke the embed edge function for each batch
  foreach batch in array job_batches loop
    -- 使用 batch 中第一条 job 的 edgeFunction
    edge_fn := batch->0->>'edgeFunction';

    perform util.invoke_edge_function(
      name => edge_fn,
      body => batch,
      timeout_milliseconds => timeout_milliseconds
    );
  end loop;
end;
$function$
;

CREATE OR REPLACE FUNCTION util.process_webhook_jobs(batch_size integer DEFAULT 3, max_batches integer DEFAULT 10, timeout_milliseconds integer DEFAULT ((5 * 60) * 1000))
 RETURNS void
 LANGUAGE plpgsql
 SET search_path TO ''
AS $function$
declare
  rec record;

  -- 当前批
  cur_batch jsonb[] := array[]::jsonb[];
  cur_batch_msg_ids bigint[] := array[]::bigint[];
  batch_count int := 0;

  -- flush 用
  payload jsonb;
  msg_id bigint;
  i int;
begin
  -- 一次性从队列读取（最多 max_batches * batch_size 条）
  for rec in
    select *
    from pgmq.read(
      queue_name => 'webhook_jobs',
      vt => timeout_milliseconds / 1000,
      qty => batch_size * max_batches
    )
  loop
    -- 累加当前批
    cur_batch := cur_batch || (rec.message)::jsonb;
    cur_batch_msg_ids := cur_batch_msg_ids || rec.msg_id;

    -- 满一批就 flush
    if array_length(cur_batch, 1) >= batch_size then
      payload := to_jsonb(cur_batch);

      begin
        perform util.invoke_edge_function(
          name => 'webhook_flow_embedding_ft',
          body => payload,
          timeout_milliseconds => timeout_milliseconds
        );
      exception when others then
        -- ===== 重试逻辑（新增）=====
        for i in 1 .. array_length(cur_batch, 1) loop
          if (cur_batch[i]->'meta'->>'retry')::int < (cur_batch[i]->'meta'->>'max_retry')::int then
            perform pgmq.send(
              queue_name => 'webhook_jobs',
              msg => jsonb_set(
                cur_batch[i],
                '{meta,retry}',
                to_jsonb((cur_batch[i]->'meta'->>'retry')::int + 1),
                true
              )
            );
          end if;
        end loop;

        -- 删除原消息（避免无限重试）
        foreach msg_id in array cur_batch_msg_ids loop
          perform pgmq.delete('webhook_jobs', msg_id);
        end loop;

        -- 清空批缓存
        cur_batch := array[]::jsonb[];
        cur_batch_msg_ids := array[]::bigint[];
        batch_count := batch_count + 1;

        if batch_count >= max_batches then
          return;
        end if;

        continue;
      end;

      -- 调用成功：删除本批消息（原逻辑）
      foreach msg_id in array cur_batch_msg_ids loop
        perform pgmq.delete('webhook_jobs', msg_id);
      end loop;

      cur_batch := array[]::jsonb[];
      cur_batch_msg_ids := array[]::bigint[];
      batch_count := batch_count + 1;

      if batch_count >= max_batches then
        return;
      end if;
    end if;
  end loop;

  -- 处理最后不满一批的
  if array_length(cur_batch, 1) is not null then
    payload := to_jsonb(cur_batch);

    begin
      perform util.invoke_edge_function(
        name => 'webhook_flow_embedding_ft',
        body => payload,
        timeout_milliseconds => timeout_milliseconds
      );
    exception when others then
      -- ===== 重试逻辑（TAIL，新增）=====
      for i in 1 .. array_length(cur_batch, 1) loop
        if (cur_batch[i]->'meta'->>'retry')::int < (cur_batch[i]->'meta'->>'max_retry')::int then
          perform pgmq.send(
            queue_name => 'webhook_jobs',
            msg => jsonb_set(
              cur_batch[i],
              '{meta,retry}',
              to_jsonb((cur_batch[i]->'meta'->>'retry')::int + 1),
              true
            )
          );
        end if;
      end loop;

      foreach msg_id in array cur_batch_msg_ids loop
        perform pgmq.delete('webhook_jobs', msg_id);
      end loop;

      return;
    end;

    foreach msg_id in array cur_batch_msg_ids loop
      perform pgmq.delete('webhook_jobs', msg_id);
    end loop;
  end if;
end;
$function$
;

CREATE OR REPLACE FUNCTION util.project_secret_key()
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
AS $function$
declare
  secret_value text;
begin
  select ds.decrypted_secret
    into secret_value
  from vault.decrypted_secrets ds
  where ds.name = 'project_secret_key';

  if secret_value is null or secret_value = '' then
    raise exception 'Missing vault secret: project_secret_key';
  end if;

  return secret_value;
end;
$function$
;

CREATE OR REPLACE FUNCTION util.project_url()
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
AS $function$
declare
  secret_value text;
begin
  -- Retrieve the project URL from Vault
  select ds.decrypted_secret
    into secret_value
  from vault.decrypted_secrets ds
  where ds.name = 'project_url';

  if secret_value is null or secret_value = '' then
    raise exception 'Missing vault secret: project_url';
  end if;

  return secret_value;
end;
$function$
;

CREATE OR REPLACE FUNCTION util.project_x_key()
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
AS $function$
declare
  secret_value text;
begin
  select ds.decrypted_secret
    into secret_value
  from vault.decrypted_secrets ds
  where ds.name = 'project_x_key';

  if secret_value is null or secret_value = '' then
    raise exception 'Missing vault secret: project_x_key';
  end if;

  return secret_value;
end;
$function$
;

CREATE OR REPLACE FUNCTION util.queue_embedding_webhook()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
AS $function$
begin
  perform pgmq.send(
    queue_name => 'webhook_jobs',
    msg => jsonb_build_object(
			'meta', jsonb_build_object(
        'retry', 0,
        'max_retry', 3,
        'first_seen_at', now(),
        'source', TG_TABLE_NAME
      ),
      'type', TG_OP,                -- 'UPDATE'
      'schema', TG_TABLE_SCHEMA,
      'table', TG_TABLE_NAME,       -- 'flows'
      'record', jsonb_build_object( -- 只放必要列，避免超大
        'id', NEW.id,
        'version', NEW.version,
        'json_ordered', NEW.json_ordered    
      ),
      'old_record', jsonb_build_object(
        'id', OLD.id,
        'version', OLD.version,
        'json_ordered', OLD.json_ordered
      )
    )
  );
  return NEW;
end;
$function$
;

CREATE OR REPLACE FUNCTION util.queue_embeddings()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
AS $function$
declare
  content_function text = TG_ARGV[0];
  embedding_column text = TG_ARGV[1];
	edge_function text := coalesce(TG_ARGV[2], 'embedding');
begin
  perform pgmq.send(
    queue_name => 'embedding_jobs',
    msg => jsonb_build_object(
      'id', NEW.id,
			'version', NEW.version,
      'schema', TG_TABLE_SCHEMA,
      'table', TG_TABLE_NAME,
      'contentFunction', content_function,
      'embeddingColumn', embedding_column,
			'edgeFunction', edge_function
    )
  );
  return NEW;
end;
$function$
;


