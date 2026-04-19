


SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


CREATE SCHEMA IF NOT EXISTS "public";


ALTER SCHEMA "public" OWNER TO "pg_database_owner";


COMMENT ON SCHEMA "public" IS 'standard public schema';



CREATE TYPE "public"."filtered_row" AS (
	"id" "uuid",
	"embedding" "extensions"."vector"(1536)
);


ALTER TYPE "public"."filtered_row" OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "extensions"."vector", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "full_text_weight" numeric DEFAULT 0.3, "extracted_text_weight" numeric DEFAULT 0.2, "semantic_weight" numeric DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1) RETURNS TABLE("id" "uuid", "json" "jsonb")
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$ BEGIN
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
$$;


ALTER FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "extensions"."vector", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" numeric, "extracted_text_weight" numeric, "semantic_weight" numeric, "rrf_k" integer, "data_source" "text", "this_user_id" "text", "page_size" integer, "page_current" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "full_text_weight" numeric DEFAULT 0.3, "extracted_text_weight" numeric DEFAULT 0.2, "semantic_weight" numeric DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1) RETURNS TABLE("id" "uuid", "json" "jsonb")
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$ BEGIN
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
$$;


ALTER FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" numeric, "extracted_text_weight" numeric, "semantic_weight" numeric, "rrf_k" integer, "data_source" "text", "this_user_id" "text", "page_size" integer, "page_current" integer) OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_create"("p_table" "text", "p_id" "uuid", "p_json_ordered" "jsonb", "p_model_id" "uuid" DEFAULT NULL::"uuid", "p_rule_verification" boolean DEFAULT NULL::boolean, "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."cmd_dataset_create"("p_table" "text", "p_id" "uuid", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_delete"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."cmd_dataset_delete"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_publish"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."cmd_dataset_publish"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_save_draft"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_json_ordered" "jsonb", "p_model_id" "uuid" DEFAULT NULL::"uuid", "p_rule_verification" boolean DEFAULT NULL::boolean, "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."cmd_dataset_save_draft"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_membership_is_review_admin"("p_actor" "uuid" DEFAULT "auth"."uid"()) RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select exists (
    select 1
    from public.roles
    where user_id = coalesce(p_actor, auth.uid())
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role = 'review-admin'
  )
$$;


ALTER FUNCTION "public"."cmd_membership_is_review_admin"("p_actor" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_membership_is_system_manager"("p_actor" "uuid" DEFAULT "auth"."uid"()) RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select exists (
    select 1
    from public.roles
    where user_id = coalesce(p_actor, auth.uid())
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role in ('owner', 'admin', 'member')
  )
$$;


ALTER FUNCTION "public"."cmd_membership_is_system_manager"("p_actor" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_membership_is_system_owner"("p_actor" "uuid" DEFAULT "auth"."uid"()) RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select exists (
    select 1
    from public.roles
    where user_id = coalesce(p_actor, auth.uid())
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role = 'owner'
  )
$$;


ALTER FUNCTION "public"."cmd_membership_is_system_owner"("p_actor" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_membership_is_team_manager"("p_actor" "uuid", "p_team_id" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select exists (
    select 1
    from public.roles
    where user_id = p_actor
      and team_id = p_team_id
      and role in ('owner', 'admin')
  )
$$;


ALTER FUNCTION "public"."cmd_membership_is_team_manager"("p_actor" "uuid", "p_team_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_membership_is_team_owner"("p_actor" "uuid", "p_team_id" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select exists (
    select 1
    from public.roles
    where user_id = p_actor
      and team_id = p_team_id
      and role = 'owner'
  )
$$;


ALTER FUNCTION "public"."cmd_membership_is_team_owner"("p_actor" "uuid", "p_team_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_membership_resolve_member_order_by"("p_sort_by" "text", "p_allow_workload" boolean DEFAULT false) RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_membership_resolve_member_order_by"("p_sort_by" "text", "p_allow_workload" boolean) OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."cmd_notification_normalize_text_array"("p_values" "text"[]) RETURNS "text"[]
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_notification_normalize_text_array"("p_values" "text"[]) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_notification_send_validation_issue"("p_recipient_user_id" "uuid", "p_dataset_type" "text", "p_dataset_id" "uuid", "p_dataset_version" "text", "p_link" "text" DEFAULT NULL::"text", "p_issue_codes" "text"[] DEFAULT ARRAY[]::"text"[], "p_tab_names" "text"[] DEFAULT ARRAY[]::"text"[], "p_issue_count" integer DEFAULT 0, "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_notification_send_validation_issue"("p_recipient_user_id" "uuid", "p_dataset_type" "text", "p_dataset_id" "uuid", "p_dataset_version" "text", "p_link" "text", "p_issue_codes" "text"[], "p_tab_names" "text"[], "p_issue_count" integer, "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_append_log"("p_review_json" "jsonb", "p_action" "text", "p_actor" "uuid", "p_extra" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_review_append_log"("p_review_json" "jsonb", "p_action" "text", "p_actor" "uuid", "p_extra" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_append_review_ref"("p_existing_reviews" "jsonb", "p_review_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_review_append_review_ref"("p_existing_reviews" "jsonb", "p_review_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_apply_model_validation_to_process_json"("p_process_json" "jsonb", "p_model_json" "jsonb", "p_comment_review" "jsonb" DEFAULT '[]'::"jsonb", "p_comment_compliance" "jsonb" DEFAULT '[]'::"jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_review_apply_model_validation_to_process_json"("p_process_json" "jsonb", "p_model_json" "jsonb", "p_comment_review" "jsonb", "p_comment_compliance" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_apply_mv_payload"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_review_items" "jsonb" DEFAULT '[]'::"jsonb", "p_compliance_items" "jsonb" DEFAULT '[]'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."cmd_review_apply_mv_payload"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_review_items" "jsonb", "p_compliance_items" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_approve"("p_table" "text", "p_review_id" "uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."cmd_review_approve"("p_table" "text", "p_review_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone DEFAULT NULL::timestamp with time zone, "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone, "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_change_member_role"("p_user_id" "uuid", "p_role" "text" DEFAULT NULL::"text", "p_action" "text" DEFAULT 'set'::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_review_change_member_role"("p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_collect_dataset_targets"("p_roots" "jsonb", "p_lock" boolean DEFAULT false) RETURNS TABLE("table_name" "text", "dataset_id" "uuid", "dataset_version" "text", "state_code" integer, "reviews" "jsonb", "dataset_row" "jsonb", "is_root" boolean)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."cmd_review_collect_dataset_targets"("p_roots" "jsonb", "p_lock" boolean) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_extract_refs"("p_json" "jsonb") RETURNS TABLE("ref_type" "text", "ref_object_id" "uuid", "ref_version" "text")
    LANGUAGE "sql" STABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."cmd_review_extract_refs"("p_json" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_get_actor_meta"("p_actor" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_review_get_actor_meta"("p_actor" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_get_dataset_name"("p_table" "text", "p_row" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_review_get_dataset_name"("p_table" "text", "p_row" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_get_dataset_row"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_lock" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."cmd_review_get_dataset_row"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_lock" boolean) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_get_root_table"("p_review_json" "jsonb", "p_data_id" "uuid", "p_data_version" "text") RETURNS "text"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_review_get_root_table"("p_review_json" "jsonb", "p_data_id" "uuid", "p_data_version" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_is_review_admin"("p_actor" "uuid" DEFAULT "auth"."uid"()) RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select exists (
    select 1
    from public.roles
    where user_id = coalesce(p_actor, auth.uid())
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role = 'review-admin'
  )
$$;


ALTER FUNCTION "public"."cmd_review_is_review_admin"("p_actor" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_is_review_member"("p_actor" "uuid" DEFAULT "auth"."uid"()) RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select exists (
    select 1
    from public.roles
    where user_id = coalesce(p_actor, auth.uid())
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role = 'review-member'
  )
$$;


ALTER FUNCTION "public"."cmd_review_is_review_member"("p_actor" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_json_array"("p_value" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select case jsonb_typeof(p_value)
    when 'array' then coalesce(p_value, '[]'::jsonb)
    when 'object' then jsonb_build_array(p_value)
    when 'string' then jsonb_build_array(p_value)
    when 'number' then jsonb_build_array(p_value)
    when 'boolean' then jsonb_build_array(p_value)
    else '[]'::jsonb
  end
$$;


ALTER FUNCTION "public"."cmd_review_json_array"("p_value" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_merge_compliance_declarations"("p_existing" "jsonb", "p_additions" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_review_merge_compliance_declarations"("p_existing" "jsonb", "p_additions" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_merge_json_collection"("p_existing" "jsonb", "p_additions" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select public.cmd_review_json_array(p_existing) || public.cmd_review_json_array(p_additions)
$$;


ALTER FUNCTION "public"."cmd_review_merge_json_collection"("p_existing" "jsonb", "p_additions" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_merge_validation"("p_existing" "jsonb", "p_additions" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_review_merge_validation"("p_existing" "jsonb", "p_additions" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_normalize_reviewer_ids"("p_reviewer_ids" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."cmd_review_normalize_reviewer_ids"("p_reviewer_ids" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_ref_type_to_table"("p_ref_type" "text") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_review_ref_type_to_table"("p_ref_type" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_reject"("p_table" "text", "p_review_id" "uuid", "p_reason" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."cmd_review_reject"("p_table" "text", "p_review_id" "uuid", "p_reason" "text", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_resolve_queue_order_by"("p_sort_by" "text", "p_allow_comment_modified" boolean DEFAULT false) RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
begin
  case lower(coalesce(p_sort_by, ''))
    when 'created_at' then
      return 'q.created_at';
    when 'createat' then
      return 'q.created_at';
    when 'deadline' then
      return 'q.deadline';
    when 'state_code' then
      return 'q.state_code';
    when 'statecode' then
      return 'q.state_code';
    when 'comment_modified_at' then
      if p_allow_comment_modified then
        return 'q.comment_modified_at';
      end if;
    when 'commentmodifiedat' then
      if p_allow_comment_modified then
        return 'q.comment_modified_at';
      end if;
    else
      return 'q.modified_at';
  end case;

  return 'q.modified_at';
end;
$$;


ALTER FUNCTION "public"."cmd_review_resolve_queue_order_by"("p_sort_by" "text", "p_allow_comment_modified" boolean) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_revoke_reviewer"("p_review_id" "uuid", "p_reviewer_id" "uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_review_revoke_reviewer"("p_review_id" "uuid", "p_reviewer_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_save_assignment_draft"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."cmd_review_save_assignment_draft"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_save_comment_draft"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_review_save_comment_draft"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select public.cmd_review_submit_comment(
    p_review_id,
    p_json,
    1,
    p_audit
  )
$$;


ALTER FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer DEFAULT 1, "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_system_change_member_role"("p_user_id" "uuid", "p_role" "text" DEFAULT NULL::"text", "p_action" "text" DEFAULT 'set'::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_system_change_member_role"("p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_team_accept_invitation"("p_team_id" "uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_team_accept_invitation"("p_team_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_team_change_member_role"("p_team_id" "uuid", "p_user_id" "uuid", "p_role" "text" DEFAULT NULL::"text", "p_action" "text" DEFAULT 'set'::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_team_change_member_role"("p_team_id" "uuid", "p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_team_create"("p_team_id" "uuid", "p_json" "jsonb", "p_rank" integer, "p_is_public" boolean, "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_team_create"("p_team_id" "uuid", "p_json" "jsonb", "p_rank" integer, "p_is_public" boolean, "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_team_reinvite_member"("p_team_id" "uuid", "p_user_id" "uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_team_reinvite_member"("p_team_id" "uuid", "p_user_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_team_reject_invitation"("p_team_id" "uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_team_reject_invitation"("p_team_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_team_set_rank"("p_team_id" "uuid", "p_rank" integer, "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_team_set_rank"("p_team_id" "uuid", "p_rank" integer, "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_team_update_profile"("p_team_id" "uuid", "p_json" "jsonb", "p_is_public" boolean, "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_team_update_profile"("p_team_id" "uuid", "p_json" "jsonb", "p_is_public" boolean, "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_user_update_contact"("p_user_id" "uuid", "p_contact" "jsonb", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."cmd_user_update_contact"("p_user_id" "uuid", "p_contact" "jsonb", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."contacts_sync_jsonb_version"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."contacts_sync_jsonb_version"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."delete_lifecycle_model_bundle"("p_model_id" "uuid", "p_version" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."delete_lifecycle_model_bundle"("p_model_id" "uuid", "p_version" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."flowproperties_sync_jsonb_version"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb THEN
        NEW.json := NEW.json_ordered;
        NEW.version := COALESCE( NEW.json->'flowPropertyDataSet'->'administrativeInformation'->'publicationAndOwnership'->>'common:dataSetVersion',
					''
        );
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."flowproperties_sync_jsonb_version"() OWNER TO "postgres";

SET default_tablespace = '';

SET default_table_access_method = "heap";


CREATE TABLE IF NOT EXISTS "public"."flows" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "embedding_at" timestamp(6) with time zone DEFAULT NULL::timestamp with time zone,
    "extracted_text" "text",
    "team_id" "uuid",
    "review_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    "embedding_flag" smallint,
    "embedding_ft_at" timestamp with time zone,
    "extracted_md" "text",
    "embedding_ft" "extensions"."vector"(1024),
    CONSTRAINT "flows_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100, 200])))
);


ALTER TABLE "public"."flows" OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."flows_embedding_ft_input"("proc" "public"."flows") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public'
    AS $$
begin
  return proc.extracted_md;
end;
$$;


ALTER FUNCTION "public"."flows_embedding_ft_input"("proc" "public"."flows") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."flows_embedding_input"("flow" "public"."flows") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public'
    AS $$
begin
  return flow.extracted_text;
end;
$$;


ALTER FUNCTION "public"."flows_embedding_input"("flow" "public"."flows") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."flows_sync_jsonb_version"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb THEN
        NEW.json := NEW.json_ordered;
		NEW.version := COALESCE( NEW.json->'flowDataSet'->'administrativeInformation'->'publicationAndOwnership'->>'common:dataSetVersion',
					''
        );
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."flows_sync_jsonb_version"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."generate_flow_embedding"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."generate_flow_embedding"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."hybrid_search_flows"("query_text" "text", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "full_text_weight" double precision DEFAULT 0.3, "extracted_text_weight" double precision DEFAULT 0.2, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1) RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone)
    LANGUAGE "plpgsql"
    SET "statement_timeout" TO '60s'
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."hybrid_search_flows"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."hybrid_search_lifecyclemodels"("query_text" "text", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "full_text_weight" double precision DEFAULT 0.3, "extracted_text_weight" double precision DEFAULT 0.2, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1) RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone)
    LANGUAGE "plpgsql"
    SET "statement_timeout" TO '60s'
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."hybrid_search_lifecyclemodels"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."hybrid_search_processes"("query_text" "text", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "full_text_weight" double precision DEFAULT 0.3, "extracted_text_weight" double precision DEFAULT 0.2, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1) RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "model_id" "uuid")
    LANGUAGE "plpgsql"
    SET "statement_timeout" TO '60s'
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."hybrid_search_processes"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."ilcd_classification_get"("this_file_name" "text", "category_type" "text", "get_values" "text"[]) RETURNS SETOF "jsonb"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."ilcd_classification_get"("this_file_name" "text", "category_type" "text", "get_values" "text"[]) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."ilcd_flow_categorization_get"("this_file_name" "text", "get_values" "text"[]) RETURNS SETOF "jsonb"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."ilcd_flow_categorization_get"("this_file_name" "text", "get_values" "text"[]) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."ilcd_location_get"("this_file_name" "text", "get_values" "text"[]) RETURNS SETOF "jsonb"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."ilcd_location_get"("this_file_name" "text", "get_values" "text"[]) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") RETURNS bigint
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pgmq'
    AS $$
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
$$;


ALTER FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."lca_package_enqueue_job"("p_message" "jsonb") RETURNS bigint
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pgmq'
    AS $$
DECLARE
    v_msg_id bigint;
BEGIN
    SELECT pgmq.send('lca_package_jobs', p_message)
      INTO v_msg_id;

    RETURN v_msg_id;
END;
$$;


ALTER FUNCTION "public"."lca_package_enqueue_job"("p_message" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."lciamethods_sync_jsonb_version"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb THEN
        NEW.json := NEW.json_ordered;
        NEW.version := NEW.json->'LCIAMethodDataSet'->'administrativeInformation'->'publicationAndOwnership'->>'common:dataSetVersion';
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."lciamethods_sync_jsonb_version"() OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."lifecyclemodels" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp(6) with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "json_tg" "jsonb",
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "team_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    "extracted_text" "text",
    "embedding_at" timestamp with time zone,
    "embedding_flag" smallint,
    "extracted_md" "text",
    "embedding_ft_at" timestamp with time zone,
    "embedding_ft" "extensions"."vector"(1024),
    CONSTRAINT "lifecyclemodels_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100])))
);


ALTER TABLE "public"."lifecyclemodels" OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."lifecyclemodels_embedding_ft_input"("proc" "public"."lifecyclemodels") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public'
    AS $$
begin
  return proc.extracted_md;
end;
$$;


ALTER FUNCTION "public"."lifecyclemodels_embedding_ft_input"("proc" "public"."lifecyclemodels") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."lifecyclemodels_embedding_input"("models" "public"."lifecyclemodels") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public'
    AS $$
begin
  return models.extracted_text;
end;
$$;


ALTER FUNCTION "public"."lifecyclemodels_embedding_input"("models" "public"."lifecyclemodels") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."lifecyclemodels_sync_jsonb_version"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb THEN
        NEW.json := NEW.json_ordered;
        NEW.version := NEW.json->'lifeCycleModelDataSet'->'administrativeInformation'->'publicationAndOwnership'->>'common:dataSetVersion';
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."lifecyclemodels_sync_jsonb_version"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."pgroonga_search"("query_text" "text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb")
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
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
END;$$;


ALTER FUNCTION "public"."pgroonga_search"("query_text" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_contacts"("query_text" "text", "filter_condition" "text" DEFAULT ''::"text", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."pgroonga_search_contacts"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_flowproperties"("query_text" "text", "filter_condition" "text" DEFAULT ''::"text", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."pgroonga_search_flowproperties"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_flows_text_v1"("query_text" "text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "extracted_text" "text", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."pgroonga_search_flows_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text" DEFAULT ''::"text", "order_by" "text" DEFAULT ''::"text", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $_$
 
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
	
$_$;


ALTER FUNCTION "public"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_lifecyclemodels_text_v1"("query_text" "text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "extracted_text" "text", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."pgroonga_search_lifecyclemodels_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_lifecyclemodels_v1"("query_text" "text", "filter_condition" "text" DEFAULT ''::"text", "order_by" "text" DEFAULT ''::"text", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."pgroonga_search_lifecyclemodels_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_processes_text_v1"("query_text" "text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "extracted_text" "text", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."pgroonga_search_processes_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text" DEFAULT ''::"text", "order_by" "text" DEFAULT ''::"text", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "model_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_sources"("query_text" "text", "filter_condition" "text" DEFAULT ''::"text", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."pgroonga_search_sources"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_unitgroups"("query_text" "text", "filter_condition" "text" DEFAULT ''::"text", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."pgroonga_search_unitgroups"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."policy_is_current_user_in_roles"("p_team_id" "uuid", "p_roles_to_check" "text"[]) RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."policy_is_current_user_in_roles"("p_team_id" "uuid", "p_roles_to_check" "text"[]) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."policy_is_team_id_used"("_team_id" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT EXISTS (
    SELECT 1
    FROM public.teams t
    WHERE t.id = _team_id);
$$;


ALTER FUNCTION "public"."policy_is_team_id_used"("_team_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."policy_is_team_public"("_team_id" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT EXISTS (
    SELECT 1
    FROM public.teams t
    WHERE t.id = _team_id
      AND t.is_public);
$$;


ALTER FUNCTION "public"."policy_is_team_public"("_team_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."policy_review_can_read"("p_review_id" "uuid", "p_actor" "uuid" DEFAULT "auth"."uid"()) RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select exists (
    select 1
    from public.reviews as r
    where r.id = p_review_id
      and coalesce(p_actor, auth.uid()) is not null
      and (
        public.cmd_review_is_review_admin(coalesce(p_actor, auth.uid()))
        or ((r.json -> 'user' ->> 'id')::uuid = coalesce(p_actor, auth.uid()))
        or (
          public.cmd_review_is_review_member(coalesce(p_actor, auth.uid()))
          and (
            coalesce(r.reviewer_id, '[]'::jsonb) ? coalesce(p_actor, auth.uid())::text
            or exists (
              select 1
              from public.comments as c
              where c.review_id = r.id
                and c.reviewer_id = coalesce(p_actor, auth.uid())
            )
          )
        )
      )
  )
$$;


ALTER FUNCTION "public"."policy_review_can_read"("p_review_id" "uuid", "p_actor" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."policy_roles_delete"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
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
$$;


ALTER FUNCTION "public"."policy_roles_delete"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."policy_roles_insert"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
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
$$;


ALTER FUNCTION "public"."policy_roles_insert"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."policy_roles_select"("_team_id" "uuid", "_role" "text") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
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
$$;


ALTER FUNCTION "public"."policy_roles_select"("_team_id" "uuid", "_role" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."policy_roles_update"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
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
$$;


ALTER FUNCTION "public"."policy_roles_update"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."policy_user_has_team"("_user_id" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT EXISTS (
    SELECT 1
    FROM public.roles r
    WHERE r.user_id = _user_id
      AND r.role <> 'rejected'
	  and r.team_id <> '00000000-0000-0000-0000-000000000000');
$$;


ALTER FUNCTION "public"."policy_user_has_team"("_user_id" "uuid") OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."processes" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "team_id" "uuid",
    "extracted_text" "text",
    "embedding_at" timestamp with time zone,
    "review_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    "embedding_flag" smallint,
    "model_id" "uuid",
    "embedding_ft_at" timestamp with time zone,
    "embedding_ft" "extensions"."vector"(1024),
    "extracted_md" "text",
    CONSTRAINT "processes_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100, 200])))
);


ALTER TABLE "public"."processes" OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."processes_embedding_ft_input"("proc" "public"."processes") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
begin
  return proc.extracted_md;
end;
$$;


ALTER FUNCTION "public"."processes_embedding_ft_input"("proc" "public"."processes") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."processes_embedding_input"("proc" "public"."processes") RETURNS "text"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO ''
    AS $$
begin
  return proc.extracted_text;
end;
$$;


ALTER FUNCTION "public"."processes_embedding_input"("proc" "public"."processes") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."processes_sync_jsonb_version"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb THEN
        NEW.json := NEW.json_ordered;
        NEW.version := COALESCE(NEW.json->'processDataSet'->'administrativeInformation'->'publicationAndOwnership'->>'common:dataSetVersion',
					''
        );
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."processes_sync_jsonb_version"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."qry_notification_get_my_data_count"("p_days" integer DEFAULT 3, "p_last_view_at" timestamp with time zone DEFAULT NULL::timestamp with time zone) RETURNS integer
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."qry_notification_get_my_data_count"("p_days" integer, "p_last_view_at" timestamp with time zone) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."qry_notification_get_my_data_items"("p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_days" integer DEFAULT 3) RETURNS TABLE("id" "uuid", "state_code" integer, "json" "jsonb", "modified_at" timestamp with time zone, "total_count" integer)
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."qry_notification_get_my_data_items"("p_page" integer, "p_page_size" integer, "p_days" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."qry_notification_get_my_issue_count"("p_days" integer DEFAULT 3, "p_last_view_at" timestamp with time zone DEFAULT NULL::timestamp with time zone) RETURNS integer
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."qry_notification_get_my_issue_count"("p_days" integer, "p_last_view_at" timestamp with time zone) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."qry_notification_get_my_issue_items"("p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_days" integer DEFAULT 3) RETURNS TABLE("id" "uuid", "type" "text", "dataset_type" "text", "dataset_id" "uuid", "dataset_version" "text", "json" "jsonb", "modified_at" timestamp with time zone, "total_count" integer)
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."qry_notification_get_my_issue_items"("p_page" integer, "p_page_size" integer, "p_days" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."qry_notification_get_my_team_count"("p_days" integer DEFAULT 3, "p_last_view_at" timestamp with time zone DEFAULT NULL::timestamp with time zone) RETURNS integer
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."qry_notification_get_my_team_count"("p_days" integer, "p_last_view_at" timestamp with time zone) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."qry_notification_get_my_team_items"("p_days" integer DEFAULT 3) RETURNS TABLE("team_id" "uuid", "user_id" "uuid", "role" "text", "team_title" "jsonb", "modified_at" timestamp with time zone)
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."qry_notification_get_my_team_items"("p_days" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."qry_review_get_admin_queue_items"("p_status" "text" DEFAULT NULL::"text", "p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_sort_by" "text" DEFAULT 'modified_at'::"text", "p_sort_order" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "data_id" "uuid", "data_version" "text", "state_code" integer, "reviewer_id" "jsonb", "json" "jsonb", "deadline" timestamp with time zone, "created_at" timestamp with time zone, "modified_at" timestamp with time zone, "comment_state_codes" "jsonb", "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_order_by text := public.cmd_review_resolve_queue_order_by(p_sort_by, false);
  v_order_dir text := public.cmd_membership_resolve_sort_direction(p_sort_order);
  v_status text := lower(coalesce(p_status, ''));
  v_state_code integer;
begin
  if v_actor is null then
    return;
  end if;

  if not public.cmd_review_is_review_admin(v_actor) then
    return;
  end if;

  case v_status
    when '', 'all' then
      v_state_code := null;
    when 'unassigned' then
      v_state_code := 0;
    when 'assigned' then
      v_state_code := 1;
    when 'admin-rejected' then
      v_state_code := -1;
    else
      return;
  end case;

  return query execute format(
    $sql$
      with q as (
        select
          r.id,
          r.data_id,
          r.data_version::text as data_version,
          r.state_code,
          coalesce(r.reviewer_id, '[]'::jsonb) as reviewer_id,
          coalesce(r.json, '{}'::jsonb) as json,
          r.deadline,
          r.created_at,
          r.modified_at,
          coalesce(
            jsonb_agg(to_jsonb(c.state_code) order by c.created_at asc, c.reviewer_id asc)
              filter (where c.reviewer_id is not null),
            '[]'::jsonb
          ) as comment_state_codes
        from public.reviews as r
        left join public.comments as c
          on c.review_id = r.id
        where ($1::integer is null or r.state_code = $1::integer)
        group by
          r.id,
          r.data_id,
          r.data_version,
          r.state_code,
          r.reviewer_id,
          r.json,
          r.deadline,
          r.created_at,
          r.modified_at
      )
      select
        q.id,
        q.data_id,
        q.data_version,
        q.state_code,
        q.reviewer_id,
        q.json,
        q.deadline,
        q.created_at,
        q.modified_at,
        q.comment_state_codes,
        count(*) over() as total_count
      from q
      order by %s %s nulls last, q.id asc
      limit $2
      offset $3
    $sql$,
    v_order_by,
    v_order_dir
  )
  using v_state_code, v_limit, v_offset;
end;
$_$;


ALTER FUNCTION "public"."qry_review_get_admin_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."qry_review_get_comment_items"("p_review_id" "uuid", "p_scope" "text" DEFAULT 'auto'::"text") RETURNS TABLE("review_id" "uuid", "reviewer_id" "uuid", "state_code" integer, "json" "jsonb", "created_at" timestamp with time zone, "modified_at" timestamp with time zone)
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  with actor as (
    select
      auth.uid() as actor_id,
      public.cmd_review_is_review_admin(auth.uid()) as is_review_admin,
      exists (
        select 1
        from public.reviews as r
        where r.id = p_review_id
          and ((r.json -> 'user' ->> 'id')::uuid = auth.uid())
      ) as is_owner
  )
  select
    c.review_id,
    c.reviewer_id,
    c.state_code,
    coalesce(c.json::jsonb, '{}'::jsonb) as json,
    c.created_at,
    c.modified_at
  from public.comments as c
  cross join actor as a
  where c.review_id = p_review_id
    and public.policy_review_can_read(p_review_id, a.actor_id)
    and (
      a.is_review_admin
      or a.is_owner
      or c.reviewer_id = a.actor_id
    )
    and (
      lower(coalesce(p_scope, 'auto')) not in ('mine', 'self')
      or c.reviewer_id = a.actor_id
    )
  order by c.created_at asc, c.reviewer_id asc
$$;


ALTER FUNCTION "public"."qry_review_get_comment_items"("p_review_id" "uuid", "p_scope" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."qry_review_get_items"("p_review_ids" "uuid"[] DEFAULT NULL::"uuid"[], "p_data_id" "uuid" DEFAULT NULL::"uuid", "p_data_version" "text" DEFAULT NULL::"text", "p_state_codes" integer[] DEFAULT NULL::integer[]) RETURNS TABLE("id" "uuid", "data_id" "uuid", "data_version" "text", "state_code" integer, "reviewer_id" "jsonb", "json" "jsonb", "deadline" timestamp with time zone, "created_at" timestamp with time zone, "modified_at" timestamp with time zone)
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select
    r.id,
    r.data_id,
    r.data_version::text as data_version,
    r.state_code,
    coalesce(r.reviewer_id, '[]'::jsonb) as reviewer_id,
    coalesce(r.json, '{}'::jsonb) as json,
    r.deadline,
    r.created_at,
    r.modified_at
  from public.reviews as r
  where (p_review_ids is null or r.id = any (p_review_ids))
    and (
      p_data_id is null
      or r.data_id = p_data_id
      or coalesce(r.json -> 'data' ->> 'id', '') = p_data_id::text
    )
    and (
      p_data_version is null
      or r.data_version = p_data_version
      or coalesce(r.json -> 'data' ->> 'version', '') = p_data_version
    )
    and (p_state_codes is null or r.state_code = any (p_state_codes))
    and public.policy_review_can_read(r.id, auth.uid())
  order by r.modified_at desc, r.id desc
$$;


ALTER FUNCTION "public"."qry_review_get_items"("p_review_ids" "uuid"[], "p_data_id" "uuid", "p_data_version" "text", "p_state_codes" integer[]) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."qry_review_get_member_list"("p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_sort_by" "text" DEFAULT 'created_at'::"text", "p_sort_order" "text" DEFAULT 'desc'::"text", "p_role" "text" DEFAULT NULL::"text") RETURNS TABLE("user_id" "uuid", "team_id" "uuid", "role" "text", "email" "text", "display_name" "text", "created_at" timestamp with time zone, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."qry_review_get_member_list"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_role" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."qry_review_get_member_queue_items"("p_status" "text" DEFAULT 'pending'::"text", "p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_sort_by" "text" DEFAULT 'modified_at'::"text", "p_sort_order" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "data_id" "uuid", "data_version" "text", "review_state_code" integer, "reviewer_id" "jsonb", "json" "jsonb", "deadline" timestamp with time zone, "created_at" timestamp with time zone, "modified_at" timestamp with time zone, "comment_state_code" integer, "comment_json" "jsonb", "comment_created_at" timestamp with time zone, "comment_modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_order_by text := public.cmd_review_resolve_queue_order_by(p_sort_by, true);
  v_order_dir text := public.cmd_membership_resolve_sort_direction(p_sort_order);
  v_status text := lower(coalesce(p_status, 'pending'));
begin
  if v_actor is null then
    return;
  end if;

  if not public.cmd_review_is_review_member(v_actor) then
    return;
  end if;

  if v_status not in ('pending', 'reviewed', 'reviewer-rejected') then
    return;
  end if;

  return query execute format(
    $sql$
      with q as (
        select
          r.id,
          r.data_id,
          r.data_version::text as data_version,
          r.state_code as review_state_code,
          coalesce(r.reviewer_id, '[]'::jsonb) as reviewer_id,
          coalesce(r.json, '{}'::jsonb) as json,
          r.deadline,
          r.created_at,
          r.modified_at,
          c.state_code as comment_state_code,
          coalesce(c.json::jsonb, '{}'::jsonb) as comment_json,
          c.created_at as comment_created_at,
          c.modified_at as comment_modified_at
        from public.comments as c
        join public.reviews as r
          on r.id = c.review_id
        where c.reviewer_id = $1
          and public.policy_review_can_read(r.id, $1)
          and (
            ($4::text = 'pending' and c.state_code = 0 and r.state_code > 0)
            or ($4::text = 'reviewed' and c.state_code = any (array[1, 2, -3]) and r.state_code > 0)
            or ($4::text = 'reviewer-rejected' and c.state_code = -1 and r.state_code = -1)
          )
      )
      select
        q.id,
        q.data_id,
        q.data_version,
        q.review_state_code,
        q.reviewer_id,
        q.json,
        q.deadline,
        q.created_at,
        q.modified_at,
        q.comment_state_code,
        q.comment_json,
        q.comment_created_at,
        q.comment_modified_at,
        count(*) over() as total_count
      from q
      order by %s %s nulls last, q.id asc
      limit $2
      offset $3
    $sql$,
    v_order_by,
    v_order_dir
  )
  using v_actor, v_limit, v_offset, v_status;
end;
$_$;


ALTER FUNCTION "public"."qry_review_get_member_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."qry_review_get_member_workload"("p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_sort_by" "text" DEFAULT 'created_at'::"text", "p_sort_order" "text" DEFAULT 'desc'::"text", "p_role" "text" DEFAULT NULL::"text") RETURNS TABLE("user_id" "uuid", "team_id" "uuid", "role" "text", "email" "text", "display_name" "text", "pending_count" bigint, "reviewed_count" bigint, "created_at" timestamp with time zone, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."qry_review_get_member_workload"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_role" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."qry_system_get_member_list"("p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_sort_by" "text" DEFAULT 'created_at'::"text", "p_sort_order" "text" DEFAULT 'desc'::"text") RETURNS TABLE("user_id" "uuid", "team_id" "uuid", "role" "text", "email" "text", "display_name" "text", "created_at" timestamp with time zone, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."qry_system_get_member_list"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."qry_team_get_member_list"("p_team_id" "uuid", "p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_sort_by" "text" DEFAULT 'created_at'::"text", "p_sort_order" "text" DEFAULT 'desc'::"text") RETURNS TABLE("user_id" "uuid", "team_id" "uuid", "role" "text", "email" "text", "display_name" "text", "created_at" timestamp with time zone, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."qry_team_get_member_list"("p_team_id" "uuid", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."save_lifecycle_model_bundle"("p_plan" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
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
$_$;


ALTER FUNCTION "public"."save_lifecycle_model_bundle"("p_plan" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."semantic_search"("query_embedding" "text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb")
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."semantic_search"("query_embedding" "text", "match_threshold" double precision, "match_count" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."semantic_search_flows_v1"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."semantic_search_flows_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."semantic_search_lifecyclemodels_v1"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."semantic_search_lifecyclemodels_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."semantic_search_processes_v1"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."semantic_search_processes_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."sources_sync_jsonb_version"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb THEN
        NEW.json := NEW.json_ordered;
        NEW.version := COALESCE(NEW.json->'sourceDataSet'->'administrativeInformation'->'publicationAndOwnership'->>'common:dataSetVersion',
					''
        );
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."sources_sync_jsonb_version"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."sync_auth_users_to_public_users"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."sync_auth_users_to_public_users"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."sync_json_to_jsonb"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$BEGIN
    IF NEW.json_ordered::jsonb IS DISTINCT FROM OLD.json_ordered::jsonb
    THEN
        NEW.json := NEW.json_ordered;
    END IF;
    RETURN NEW;
END;$$;


ALTER FUNCTION "public"."sync_json_to_jsonb"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."unitgroups_sync_jsonb_version"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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
$$;


ALTER FUNCTION "public"."unitgroups_sync_jsonb_version"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_modified_at"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
begin
  new.modified_at = now();
  return new;
end;
$$;


ALTER FUNCTION "public"."update_modified_at"() OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."command_audit_log" (
    "id" bigint NOT NULL,
    "command" "text" NOT NULL,
    "actor_user_id" "uuid" NOT NULL,
    "target_table" "text",
    "target_id" "uuid",
    "target_version" "text",
    "payload" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."command_audit_log" OWNER TO "postgres";


COMMENT ON TABLE "public"."command_audit_log" IS 'Validation-only migration for database-engine preview branch cutover on 2026-04-14.';



ALTER TABLE "public"."command_audit_log" ALTER COLUMN "id" ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME "public"."command_audit_log_id_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);



CREATE TABLE IF NOT EXISTS "public"."comments" (
    "review_id" "uuid" NOT NULL,
    "reviewer_id" "uuid" DEFAULT "auth"."uid"() NOT NULL,
    "json" json,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "state_code" integer DEFAULT 0,
    CONSTRAINT "comments_state_code_check" CHECK (("state_code" = ANY (ARRAY['-3'::integer, '-2'::integer, '-1'::integer, 0, 1, 2])))
);


ALTER TABLE "public"."comments" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."contacts" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "json_ordered" json,
    "embedding" "extensions"."vector"(1536),
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "team_id" "uuid",
    "review_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    CONSTRAINT "contacts_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 3, 20, 100])))
);


ALTER TABLE "public"."contacts" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."flowproperties" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "json_ordered" json,
    "embedding" "extensions"."vector"(1536),
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "team_id" "uuid",
    "review_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    CONSTRAINT "flowproperties_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100, 200])))
);


ALTER TABLE "public"."flowproperties" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."ilcd" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "file_name" character varying(255),
    "json" "jsonb",
    "created_at" timestamp(6) with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "modified_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."ilcd" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."lca_active_snapshots" (
    "scope" "text" NOT NULL,
    "snapshot_id" "uuid" NOT NULL,
    "source_hash" "text" NOT NULL,
    "activated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "activated_by" "uuid",
    "note" "text"
);


ALTER TABLE "public"."lca_active_snapshots" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."lca_factorization_registry" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "scope" "text" DEFAULT 'prod'::"text" NOT NULL,
    "snapshot_id" "uuid" NOT NULL,
    "backend" "text" DEFAULT 'umfpack'::"text" NOT NULL,
    "numeric_options_hash" "text" NOT NULL,
    "status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "owner_worker_id" "text",
    "lease_until" timestamp with time zone,
    "prepared_job_id" "uuid",
    "diagnostics" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "prepared_at" timestamp with time zone,
    "last_used_at" timestamp with time zone,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_factorization_registry_backend_chk" CHECK (("backend" = ANY (ARRAY['umfpack'::"text", 'cholmod'::"text", 'spqr'::"text"]))),
    CONSTRAINT "lca_factorization_registry_status_chk" CHECK (("status" = ANY (ARRAY['pending'::"text", 'building'::"text", 'ready'::"text", 'failed'::"text", 'stale'::"text"])))
);


ALTER TABLE "public"."lca_factorization_registry" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."lca_jobs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_type" "text" NOT NULL,
    "snapshot_id" "uuid" NOT NULL,
    "status" "text" DEFAULT 'queued'::"text" NOT NULL,
    "payload" "jsonb",
    "diagnostics" "jsonb",
    "attempt" integer DEFAULT 0 NOT NULL,
    "max_attempt" integer DEFAULT 3 NOT NULL,
    "requested_by" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "started_at" timestamp with time zone,
    "finished_at" timestamp with time zone,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "request_key" "text",
    "idempotency_key" "text",
    CONSTRAINT "lca_jobs_attempt_chk" CHECK ((("attempt" >= 0) AND ("max_attempt" >= 0) AND ("attempt" <= "max_attempt"))),
    CONSTRAINT "lca_jobs_status_chk" CHECK (("status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'ready'::"text", 'completed'::"text", 'failed'::"text", 'stale'::"text"]))),
    CONSTRAINT "lca_jobs_type_chk" CHECK (("job_type" = ANY (ARRAY['prepare_factorization'::"text", 'solve_one'::"text", 'solve_batch'::"text", 'solve_all_unit'::"text", 'invalidate_factorization'::"text", 'rebuild_factorization'::"text", 'build_snapshot'::"text", 'analyze_contribution_path'::"text"])))
);


ALTER TABLE "public"."lca_jobs" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."lca_latest_all_unit_results" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "snapshot_id" "uuid" NOT NULL,
    "job_id" "uuid" NOT NULL,
    "result_id" "uuid" NOT NULL,
    "query_artifact_url" "text" NOT NULL,
    "query_artifact_sha256" "text" NOT NULL,
    "query_artifact_byte_size" bigint NOT NULL,
    "query_artifact_format" "text" NOT NULL,
    "status" "text" DEFAULT 'ready'::"text" NOT NULL,
    "computed_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_latest_all_unit_results_size_chk" CHECK (("query_artifact_byte_size" >= 0)),
    CONSTRAINT "lca_latest_all_unit_results_status_chk" CHECK (("status" = ANY (ARRAY['ready'::"text", 'stale'::"text", 'failed'::"text"])))
);


ALTER TABLE "public"."lca_latest_all_unit_results" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."lca_network_snapshots" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "scope" "text" DEFAULT 'full_library'::"text" NOT NULL,
    "process_filter" "jsonb",
    "lcia_method_id" "uuid",
    "lcia_method_version" character(9),
    "provider_matching_rule" "text" DEFAULT 'split_by_evidence_hybrid'::"text" NOT NULL,
    "source_hash" "text",
    "status" "text" DEFAULT 'draft'::"text" NOT NULL,
    "created_by" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_network_snapshots_provider_rule_chk" CHECK (("provider_matching_rule" = ANY (ARRAY['strict_unique_provider'::"text", 'best_provider_strict'::"text", 'split_by_evidence'::"text", 'split_by_evidence_hybrid'::"text", 'split_equal'::"text", 'equal_split_multi_provider'::"text", 'custom_weighted_provider'::"text"]))),
    CONSTRAINT "lca_network_snapshots_scope_chk" CHECK (("scope" = 'full_library'::"text")),
    CONSTRAINT "lca_network_snapshots_status_chk" CHECK (("status" = ANY (ARRAY['draft'::"text", 'ready'::"text", 'stale'::"text", 'failed'::"text"])))
);


ALTER TABLE "public"."lca_network_snapshots" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."lca_package_artifacts" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_id" "uuid" NOT NULL,
    "artifact_kind" "text" NOT NULL,
    "status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "artifact_url" "text" NOT NULL,
    "artifact_sha256" "text",
    "artifact_byte_size" bigint,
    "artifact_format" "text" NOT NULL,
    "content_type" "text" NOT NULL,
    "metadata" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "expires_at" timestamp with time zone,
    "is_pinned" boolean DEFAULT false NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_package_artifacts_format_chk" CHECK (("artifact_format" = ANY (ARRAY['tidas-package-zip:v1'::"text", 'tidas-package-export-report:v1'::"text", 'tidas-package-import-report:v1'::"text"]))),
    CONSTRAINT "lca_package_artifacts_kind_chk" CHECK (("artifact_kind" = ANY (ARRAY['import_source'::"text", 'export_zip'::"text", 'export_report'::"text", 'import_report'::"text"]))),
    CONSTRAINT "lca_package_artifacts_size_chk" CHECK ((("artifact_byte_size" IS NULL) OR ("artifact_byte_size" >= 0))),
    CONSTRAINT "lca_package_artifacts_status_chk" CHECK (("status" = ANY (ARRAY['pending'::"text", 'ready'::"text", 'failed'::"text", 'deleted'::"text"]))),
    CONSTRAINT "lca_package_artifacts_url_chk" CHECK (("length"("btrim"("artifact_url")) > 0))
);


ALTER TABLE "public"."lca_package_artifacts" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."lca_package_export_items" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_id" "uuid" NOT NULL,
    "table_name" "text" NOT NULL,
    "dataset_id" "uuid" NOT NULL,
    "version" "text" NOT NULL,
    "is_seed" boolean DEFAULT false NOT NULL,
    "refs_done" boolean DEFAULT false NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_package_export_items_table_chk" CHECK (("table_name" = ANY (ARRAY['contacts'::"text", 'sources'::"text", 'unitgroups'::"text", 'flowproperties'::"text", 'flows'::"text", 'processes'::"text", 'lifecyclemodels'::"text"]))),
    CONSTRAINT "lca_package_export_items_version_chk" CHECK (("length"("btrim"("version")) > 0))
);


ALTER TABLE "public"."lca_package_export_items" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."lca_package_jobs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_type" "text" NOT NULL,
    "status" "text" DEFAULT 'queued'::"text" NOT NULL,
    "payload" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "diagnostics" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "attempt" integer DEFAULT 0 NOT NULL,
    "max_attempt" integer DEFAULT 3 NOT NULL,
    "requested_by" "uuid" NOT NULL,
    "scope" "text",
    "root_count" integer DEFAULT 0 NOT NULL,
    "request_key" "text",
    "idempotency_key" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "started_at" timestamp with time zone,
    "finished_at" timestamp with time zone,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_package_jobs_attempt_chk" CHECK ((("attempt" >= 0) AND ("max_attempt" >= 0) AND ("attempt" <= "max_attempt"))),
    CONSTRAINT "lca_package_jobs_idempotency_key_chk" CHECK ((("idempotency_key" IS NULL) OR ("length"("btrim"("idempotency_key")) > 0))),
    CONSTRAINT "lca_package_jobs_request_key_chk" CHECK ((("request_key" IS NULL) OR ("length"("btrim"("request_key")) > 0))),
    CONSTRAINT "lca_package_jobs_root_count_chk" CHECK (("root_count" >= 0)),
    CONSTRAINT "lca_package_jobs_status_chk" CHECK (("status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'ready'::"text", 'completed'::"text", 'failed'::"text", 'stale'::"text"]))),
    CONSTRAINT "lca_package_jobs_type_chk" CHECK (("job_type" = ANY (ARRAY['export_package'::"text", 'import_package'::"text"])))
);


ALTER TABLE "public"."lca_package_jobs" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."lca_package_request_cache" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "requested_by" "uuid" NOT NULL,
    "operation" "text" NOT NULL,
    "request_key" "text" NOT NULL,
    "request_payload" "jsonb" NOT NULL,
    "status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "job_id" "uuid",
    "export_artifact_id" "uuid",
    "report_artifact_id" "uuid",
    "error_code" "text",
    "error_message" "text",
    "hit_count" bigint DEFAULT 0 NOT NULL,
    "last_accessed_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_package_request_cache_hit_count_chk" CHECK (("hit_count" >= 0)),
    CONSTRAINT "lca_package_request_cache_operation_chk" CHECK (("operation" = ANY (ARRAY['export_package'::"text", 'import_package'::"text"]))),
    CONSTRAINT "lca_package_request_cache_request_key_chk" CHECK (("length"("btrim"("request_key")) > 0)),
    CONSTRAINT "lca_package_request_cache_status_chk" CHECK (("status" = ANY (ARRAY['pending'::"text", 'running'::"text", 'ready'::"text", 'failed'::"text", 'stale'::"text"])))
);


ALTER TABLE "public"."lca_package_request_cache" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."lca_result_cache" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "scope" "text" DEFAULT 'prod'::"text" NOT NULL,
    "snapshot_id" "uuid" NOT NULL,
    "request_key" "text" NOT NULL,
    "request_payload" "jsonb" NOT NULL,
    "status" "text" DEFAULT 'pending'::"text" NOT NULL,
    "job_id" "uuid",
    "result_id" "uuid",
    "error_code" "text",
    "error_message" "text",
    "hit_count" bigint DEFAULT 0 NOT NULL,
    "last_accessed_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_result_cache_hit_count_chk" CHECK (("hit_count" >= 0)),
    CONSTRAINT "lca_result_cache_request_key_chk" CHECK (("length"("request_key") > 0)),
    CONSTRAINT "lca_result_cache_status_chk" CHECK (("status" = ANY (ARRAY['pending'::"text", 'running'::"text", 'ready'::"text", 'failed'::"text", 'stale'::"text"])))
);


ALTER TABLE "public"."lca_result_cache" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."lca_results" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_id" "uuid" NOT NULL,
    "snapshot_id" "uuid" NOT NULL,
    "payload" "jsonb",
    "diagnostics" "jsonb",
    "artifact_url" "text",
    "artifact_sha256" "text",
    "artifact_byte_size" bigint,
    "artifact_format" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_results_artifact_size_chk" CHECK ((("artifact_byte_size" IS NULL) OR ("artifact_byte_size" >= 0)))
);


ALTER TABLE "public"."lca_results" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."lca_snapshot_artifacts" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "snapshot_id" "uuid" NOT NULL,
    "artifact_url" "text" NOT NULL,
    "artifact_sha256" "text" NOT NULL,
    "artifact_byte_size" bigint NOT NULL,
    "artifact_format" "text" NOT NULL,
    "process_count" integer NOT NULL,
    "flow_count" integer NOT NULL,
    "impact_count" integer NOT NULL,
    "a_nnz" bigint NOT NULL,
    "b_nnz" bigint NOT NULL,
    "c_nnz" bigint NOT NULL,
    "coverage" "jsonb",
    "status" "text" DEFAULT 'ready'::"text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_snapshot_artifacts_counts_chk" CHECK ((("process_count" >= 0) AND ("flow_count" >= 0) AND ("impact_count" >= 0) AND ("a_nnz" >= 0) AND ("b_nnz" >= 0) AND ("c_nnz" >= 0))),
    CONSTRAINT "lca_snapshot_artifacts_size_chk" CHECK (("artifact_byte_size" >= 0)),
    CONSTRAINT "lca_snapshot_artifacts_status_chk" CHECK (("status" = ANY (ARRAY['ready'::"text", 'stale'::"text", 'failed'::"text"])))
);


ALTER TABLE "public"."lca_snapshot_artifacts" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."lciamethods" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp(6) with time zone DEFAULT "now"(),
    "json_ordered" json,
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."lciamethods" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."notifications" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "recipient_user_id" "uuid" NOT NULL,
    "sender_user_id" "uuid" NOT NULL,
    "type" "text" NOT NULL,
    "dataset_type" "text" NOT NULL,
    "dataset_id" "uuid" NOT NULL,
    "dataset_version" "text" NOT NULL,
    "json" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."notifications" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."reviews" (
    "id" "uuid" NOT NULL,
    "data_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "state_code" integer DEFAULT 0,
    "data_version" character(9),
    "reviewer_id" "jsonb",
    "json" "jsonb",
    "deadline" timestamp with time zone,
    CONSTRAINT "reviews_state_code_check" CHECK (("state_code" = ANY (ARRAY['-1'::integer, 0, 1, 2])))
);


ALTER TABLE "public"."reviews" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."roles" (
    "user_id" "uuid" NOT NULL,
    "team_id" "uuid" NOT NULL,
    "role" character varying(255) NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "modified_at" timestamp with time zone,
    CONSTRAINT "roles_role_check" CHECK ((("role")::"text" = ANY ((ARRAY['owner'::character varying, 'admin'::character varying, 'member'::character varying, 'is_invited'::character varying, 'rejected'::character varying, 'review-admin'::character varying, 'review-member'::character varying])::"text"[])))
);


ALTER TABLE "public"."roles" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."sources" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "json_ordered" json,
    "embedding" "extensions"."vector"(1536),
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "team_id" "uuid",
    "review_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    CONSTRAINT "sources_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100])))
);


ALTER TABLE "public"."sources" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."teams" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "modified_at" timestamp with time zone,
    "rank" integer DEFAULT '-1'::integer,
    "is_public" boolean DEFAULT false
);


ALTER TABLE "public"."teams" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."unitgroups" (
    "id" "uuid" NOT NULL,
    "json" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "json_ordered" json,
    "embedding" "extensions"."vector"(1536),
    "user_id" "uuid" DEFAULT "auth"."uid"(),
    "state_code" integer DEFAULT 0,
    "version" character(9) NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "team_id" "uuid",
    "review_id" "uuid",
    "rule_verification" boolean,
    "reviews" "jsonb",
    CONSTRAINT "unitgroups_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 100, 200])))
);


ALTER TABLE "public"."unitgroups" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."users" (
    "id" "uuid" NOT NULL,
    "raw_user_meta_data" "jsonb",
    "contact" "jsonb"
);


ALTER TABLE "public"."users" OWNER TO "postgres";


ALTER TABLE ONLY "public"."command_audit_log"
    ADD CONSTRAINT "command_audit_log_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."comments"
    ADD CONSTRAINT "comments_pkey" PRIMARY KEY ("review_id", "reviewer_id");



ALTER TABLE ONLY "public"."contacts"
    ADD CONSTRAINT "contacts_pkey" PRIMARY KEY ("id", "version");



ALTER TABLE ONLY "public"."flowproperties"
    ADD CONSTRAINT "flowproperties_pkey" PRIMARY KEY ("id", "version");



ALTER TABLE ONLY "public"."flows"
    ADD CONSTRAINT "flows_pkey" PRIMARY KEY ("id", "version");



ALTER TABLE ONLY "public"."ilcd"
    ADD CONSTRAINT "ilcd_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."lca_active_snapshots"
    ADD CONSTRAINT "lca_active_snapshots_pkey" PRIMARY KEY ("scope");



ALTER TABLE ONLY "public"."lca_factorization_registry"
    ADD CONSTRAINT "lca_factorization_registry_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."lca_factorization_registry"
    ADD CONSTRAINT "lca_factorization_registry_scope_snapshot_backend_opts_uk" UNIQUE ("scope", "snapshot_id", "backend", "numeric_options_hash");



ALTER TABLE ONLY "public"."lca_jobs"
    ADD CONSTRAINT "lca_jobs_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."lca_latest_all_unit_results"
    ADD CONSTRAINT "lca_latest_all_unit_results_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."lca_latest_all_unit_results"
    ADD CONSTRAINT "lca_latest_all_unit_results_snapshot_uk" UNIQUE ("snapshot_id");



ALTER TABLE ONLY "public"."lca_network_snapshots"
    ADD CONSTRAINT "lca_network_snapshots_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."lca_package_artifacts"
    ADD CONSTRAINT "lca_package_artifacts_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."lca_package_export_items"
    ADD CONSTRAINT "lca_package_export_items_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."lca_package_jobs"
    ADD CONSTRAINT "lca_package_jobs_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."lca_package_request_cache"
    ADD CONSTRAINT "lca_package_request_cache_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."lca_package_request_cache"
    ADD CONSTRAINT "lca_package_request_cache_user_op_request_uk" UNIQUE ("requested_by", "operation", "request_key");



ALTER TABLE ONLY "public"."lca_result_cache"
    ADD CONSTRAINT "lca_result_cache_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."lca_result_cache"
    ADD CONSTRAINT "lca_result_cache_scope_snapshot_request_key_uk" UNIQUE ("scope", "snapshot_id", "request_key");



ALTER TABLE ONLY "public"."lca_results"
    ADD CONSTRAINT "lca_results_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."lca_snapshot_artifacts"
    ADD CONSTRAINT "lca_snapshot_artifacts_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."lciamethods"
    ADD CONSTRAINT "lciamethods_pkey" PRIMARY KEY ("id", "version");



ALTER TABLE ONLY "public"."lifecyclemodels"
    ADD CONSTRAINT "lifecyclemodels_pkey" PRIMARY KEY ("id", "version");



ALTER TABLE ONLY "public"."notifications"
    ADD CONSTRAINT "notifications_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."processes"
    ADD CONSTRAINT "processes_pkey" PRIMARY KEY ("id", "version");



ALTER TABLE ONLY "public"."reviews"
    ADD CONSTRAINT "reviews_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."roles"
    ADD CONSTRAINT "roles_pkey" PRIMARY KEY ("user_id", "team_id");



ALTER TABLE ONLY "public"."sources"
    ADD CONSTRAINT "sources_pkey" PRIMARY KEY ("id", "version");



ALTER TABLE ONLY "public"."teams"
    ADD CONSTRAINT "teams_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."unitgroups"
    ADD CONSTRAINT "unitgroups_pkey" PRIMARY KEY ("id", "version");



ALTER TABLE ONLY "public"."users"
    ADD CONSTRAINT "users_pkey" PRIMARY KEY ("id");



CREATE INDEX "contacts_created_at_idx" ON "public"."contacts" USING "btree" ("created_at" DESC);



CREATE INDEX "contacts_json_dataversion" ON "public"."contacts" USING "btree" (((((("json" -> 'contactDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "contacts_json_email" ON "public"."contacts" USING "btree" (((((("json" -> 'contactDataSet'::"text") -> 'contactInformation'::"text") -> 'dataSetInformation'::"text") ->> 'email'::"text")));



CREATE INDEX "contacts_json_idx" ON "public"."contacts" USING "gin" ("json");



CREATE INDEX "contacts_json_ordered_vector" ON "public"."contacts" USING "hnsw" ("embedding" "extensions"."vector_cosine_ops");



CREATE INDEX "contacts_json_pgroonga" ON "public"."contacts" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2");



CREATE INDEX "contacts_user_id_created_at_idx" ON "public"."contacts" USING "btree" ("user_id", "created_at" DESC);



CREATE UNIQUE INDEX "file_name_index" ON "public"."ilcd" USING "btree" ("file_name");



CREATE INDEX "flowproperties_created_at_idx" ON "public"."flowproperties" USING "btree" ("created_at" DESC);



CREATE INDEX "flowproperties_json_dataversion" ON "public"."flowproperties" USING "btree" (((((("json" -> 'flowPropertyDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "flowproperties_json_idx" ON "public"."flowproperties" USING "gin" ("json");



CREATE INDEX "flowproperties_json_ordered_vector" ON "public"."flowproperties" USING "hnsw" ("embedding" "extensions"."vector_cosine_ops");



CREATE INDEX "flowproperties_json_pgroonga" ON "public"."flowproperties" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2");



CREATE INDEX "flowproperties_json_refobjectid" ON "public"."flowproperties" USING "btree" ((((((("json" -> 'flowPropertyDataSet'::"text") -> 'flowPropertiesInformation'::"text") -> 'quantitativeReference'::"text") -> 'referenceToReferenceUnitGroup'::"text") ->> '@refObjectId'::"text")));



CREATE INDEX "flowproperties_modified_at_idx" ON "public"."flowproperties" USING "btree" ("modified_at");



CREATE INDEX "flowproperties_user_id_created_at_idx" ON "public"."flowproperties" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "flows_composite_idx" ON "public"."flows" USING "btree" (((((("json" -> 'flowDataSet'::"text") -> 'modellingAndValidation'::"text") -> 'LCIMethod'::"text") ->> 'typeOfDataSet'::"text")), "state_code", "modified_at" DESC);



CREATE INDEX "flows_created_at_idx" ON "public"."flows" USING "btree" ("created_at" DESC);



CREATE INDEX "flows_embedding_ft_hnsw_idx" ON "public"."flows" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops");



CREATE INDEX "flows_json_casnumber" ON "public"."flows" USING "btree" (((((("json" -> 'flowDataSet'::"text") -> 'flowInformation'::"text") -> 'dataSetInformation'::"text") ->> 'CASNumber'::"text")));



CREATE INDEX "flows_json_dataversion" ON "public"."flows" USING "btree" (((((("json" -> 'flowDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "flows_json_locationofsupply" ON "public"."flows" USING "btree" (((((("json" -> 'flowDataSet'::"text") -> 'flowInformation'::"text") -> 'geography'::"text") ->> 'locationOfSupply'::"text")));



CREATE INDEX "flows_json_pgroonga" ON "public"."flows" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2");



CREATE INDEX "flows_json_typeofdataset" ON "public"."flows" USING "btree" (((((("json" -> 'flowDataSet'::"text") -> 'modellingAndValidation'::"text") -> 'LCIMethod'::"text") ->> 'typeOfDataSet'::"text")));



CREATE INDEX "flows_modified_at_idx" ON "public"."flows" USING "btree" ("modified_at");



CREATE INDEX "flows_not_emissions_idx" ON "public"."flows" USING "btree" ("state_code", "modified_at" DESC) WHERE (NOT ("json" @> '{"flowDataSet": {"flowInformation": {"dataSetInformation": {"classificationInformation": {"common:elementaryFlowCategorization": {"common:category": [{"#text": "Emissions", "@level": "0"}]}}}}}}'::"jsonb"));



CREATE INDEX "flows_review_id_idx" ON "public"."flows" USING "btree" ("review_id");



CREATE INDEX "flows_state_code_idx" ON "public"."flows" USING "btree" ("state_code");



CREATE INDEX "flows_team_id_idx" ON "public"."flows" USING "btree" ("team_id");



CREATE INDEX "flows_text_pgroonga" ON "public"."flows" USING "pgroonga" ("extracted_text");



CREATE INDEX "flows_user_id_created_at_idx" ON "public"."flows" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "ilcd_created_at_idx" ON "public"."ilcd" USING "btree" ("created_at" DESC);



CREATE INDEX "ilcd_json_idx" ON "public"."ilcd" USING "gin" ("json");



CREATE INDEX "ilcd_modified_at_idx" ON "public"."ilcd" USING "btree" ("modified_at");



CREATE INDEX "ilcd_user_id_created_at_idx" ON "public"."ilcd" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "lca_active_snapshots_snapshot_idx" ON "public"."lca_active_snapshots" USING "btree" ("snapshot_id");



CREATE INDEX "lca_factorization_registry_snapshot_status_idx" ON "public"."lca_factorization_registry" USING "btree" ("snapshot_id", "status", "updated_at" DESC);



CREATE INDEX "lca_factorization_registry_status_lease_idx" ON "public"."lca_factorization_registry" USING "btree" ("status", "lease_until");



CREATE UNIQUE INDEX "lca_jobs_idempotency_key_uidx" ON "public"."lca_jobs" USING "btree" ("idempotency_key") WHERE ("idempotency_key" IS NOT NULL);



CREATE INDEX "lca_jobs_snapshot_created_idx" ON "public"."lca_jobs" USING "btree" ("snapshot_id", "created_at" DESC);



CREATE INDEX "lca_jobs_snapshot_type_status_created_idx" ON "public"."lca_jobs" USING "btree" ("snapshot_id", "job_type", "status", "created_at" DESC);



CREATE INDEX "lca_jobs_status_created_idx" ON "public"."lca_jobs" USING "btree" ("status", "created_at");



CREATE INDEX "lca_latest_all_unit_results_computed_idx" ON "public"."lca_latest_all_unit_results" USING "btree" ("computed_at" DESC);



CREATE INDEX "lca_latest_all_unit_results_result_idx" ON "public"."lca_latest_all_unit_results" USING "btree" ("result_id");



CREATE INDEX "lca_network_snapshots_status_created_idx" ON "public"."lca_network_snapshots" USING "btree" ("status", "created_at" DESC);



CREATE INDEX "lca_network_snapshots_updated_idx" ON "public"."lca_network_snapshots" USING "btree" ("updated_at" DESC);



CREATE INDEX "lca_package_artifacts_job_created_idx" ON "public"."lca_package_artifacts" USING "btree" ("job_id", "created_at" DESC);



CREATE UNIQUE INDEX "lca_package_artifacts_job_kind_uidx" ON "public"."lca_package_artifacts" USING "btree" ("job_id", "artifact_kind");



CREATE INDEX "lca_package_artifacts_status_created_idx" ON "public"."lca_package_artifacts" USING "btree" ("status", "created_at" DESC);



CREATE UNIQUE INDEX "lca_package_export_items_job_dataset_uidx" ON "public"."lca_package_export_items" USING "btree" ("job_id", "table_name", "dataset_id", "version");



CREATE INDEX "lca_package_export_items_job_refs_idx" ON "public"."lca_package_export_items" USING "btree" ("job_id", "refs_done", "created_at", "table_name");



CREATE INDEX "lca_package_export_items_job_seed_idx" ON "public"."lca_package_export_items" USING "btree" ("job_id", "is_seed", "created_at");



CREATE UNIQUE INDEX "lca_package_jobs_idempotency_key_uidx" ON "public"."lca_package_jobs" USING "btree" ("idempotency_key") WHERE ("idempotency_key" IS NOT NULL);



CREATE INDEX "lca_package_jobs_requested_by_created_idx" ON "public"."lca_package_jobs" USING "btree" ("requested_by", "created_at" DESC);



CREATE INDEX "lca_package_jobs_status_created_idx" ON "public"."lca_package_jobs" USING "btree" ("status", "created_at");



CREATE INDEX "lca_package_jobs_type_status_created_idx" ON "public"."lca_package_jobs" USING "btree" ("job_type", "status", "created_at" DESC);



CREATE UNIQUE INDEX "lca_package_request_cache_job_uidx" ON "public"."lca_package_request_cache" USING "btree" ("job_id") WHERE ("job_id" IS NOT NULL);



CREATE INDEX "lca_package_request_cache_last_accessed_idx" ON "public"."lca_package_request_cache" USING "btree" ("last_accessed_at" DESC);



CREATE INDEX "lca_package_request_cache_lookup_idx" ON "public"."lca_package_request_cache" USING "btree" ("requested_by", "operation", "status", "updated_at" DESC);



CREATE UNIQUE INDEX "lca_result_cache_job_uidx" ON "public"."lca_result_cache" USING "btree" ("job_id") WHERE ("job_id" IS NOT NULL);



CREATE INDEX "lca_result_cache_last_accessed_idx" ON "public"."lca_result_cache" USING "btree" ("last_accessed_at" DESC);



CREATE INDEX "lca_result_cache_lookup_idx" ON "public"."lca_result_cache" USING "btree" ("scope", "snapshot_id", "status", "updated_at" DESC);



CREATE UNIQUE INDEX "lca_result_cache_result_uidx" ON "public"."lca_result_cache" USING "btree" ("result_id") WHERE ("result_id" IS NOT NULL);



CREATE INDEX "lca_results_job_idx" ON "public"."lca_results" USING "btree" ("job_id");



CREATE INDEX "lca_results_snapshot_created_idx" ON "public"."lca_results" USING "btree" ("snapshot_id", "created_at" DESC);



CREATE INDEX "lca_snapshot_artifacts_created_idx" ON "public"."lca_snapshot_artifacts" USING "btree" ("created_at" DESC);



CREATE UNIQUE INDEX "lca_snapshot_artifacts_snapshot_format_key" ON "public"."lca_snapshot_artifacts" USING "btree" ("snapshot_id", "artifact_format");



CREATE INDEX "lca_snapshot_artifacts_snapshot_status_idx" ON "public"."lca_snapshot_artifacts" USING "btree" ("snapshot_id", "status", "created_at" DESC);



CREATE INDEX "lciamethods_created_at_idx" ON "public"."lciamethods" USING "btree" ("created_at" DESC);



CREATE INDEX "lciamethods_json_dataversion" ON "public"."lciamethods" USING "btree" (((((("json" -> 'LCIAMethodDataSetDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "lciamethods_json_idx" ON "public"."lciamethods" USING "gin" ("json");



CREATE INDEX "lciamethods_json_pgroonga" ON "public"."lciamethods" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2");



CREATE INDEX "lciamethods_modified_at_idx" ON "public"."lciamethods" USING "btree" ("modified_at");



CREATE INDEX "lciamethods_user_id_created_at_idx" ON "public"."lciamethods" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "lifecyclemodels_created_at_idx" ON "public"."lifecyclemodels" USING "btree" ("created_at" DESC);



CREATE INDEX "lifecyclemodels_embedding_ft_hnsw_idx" ON "public"."lifecyclemodels" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops");



CREATE INDEX "lifecyclemodels_json_dataversion" ON "public"."lifecyclemodels" USING "btree" (((((("json" -> 'lifeCycleModelDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "lifecyclemodels_json_idx" ON "public"."lifecyclemodels" USING "gin" ("json");



CREATE INDEX "lifecyclemodels_json_pgroonga" ON "public"."lifecyclemodels" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2");



CREATE INDEX "lifecyclemodels_json_tg_idx" ON "public"."lifecyclemodels" USING "gin" ("json_tg");



CREATE INDEX "lifecyclemodels_modified_at_idx" ON "public"."lifecyclemodels" USING "btree" ("modified_at");



CREATE INDEX "lifecyclemodels_text_pgroonga" ON "public"."lifecyclemodels" USING "pgroonga" ("extracted_text");



CREATE INDEX "lifecyclemodels_user_id_created_at_idx" ON "public"."lifecyclemodels" USING "btree" ("user_id", "created_at" DESC);



CREATE UNIQUE INDEX "notifications_recipient_sender_type_dataset_uq" ON "public"."notifications" USING "btree" ("recipient_user_id", "sender_user_id", "type", "dataset_type", "dataset_id", "dataset_version");



CREATE INDEX "notifications_recipient_type_modified_idx" ON "public"."notifications" USING "btree" ("recipient_user_id", "type", "modified_at" DESC);



CREATE INDEX "processes_created_at_idx" ON "public"."processes" USING "btree" ("created_at" DESC);



CREATE INDEX "processes_embedding_ft_hnsw_idx" ON "public"."processes" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops");



CREATE INDEX "processes_json_dataversion" ON "public"."processes" USING "btree" (((((("json" -> 'processDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "processes_json_exchange_gin_idx" ON "public"."processes" USING "gin" ((((("json" -> 'processDataSet'::"text") -> 'exchanges'::"text") -> 'exchange'::"text")));



CREATE INDEX "processes_json_location" ON "public"."processes" USING "btree" ((((((("json" -> 'processDataSet'::"text") -> 'processInformation'::"text") -> 'geography'::"text") -> 'locationOfOperationSupplyOrProduction'::"text") ->> '@location'::"text")));



CREATE INDEX "processes_json_pgroonga" ON "public"."processes" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2");



CREATE INDEX "processes_json_referenceyear" ON "public"."processes" USING "btree" (((((("json" -> 'processDataSet'::"text") -> 'processInformation'::"text") -> 'time'::"text") ->> 'common:referenceYear'::"text")));



CREATE INDEX "processes_modified_at_idx" ON "public"."processes" USING "btree" ("modified_at");



CREATE INDEX "processes_review_id_idx" ON "public"."processes" USING "btree" ("review_id");



CREATE INDEX "processes_rule_verification_idx" ON "public"."processes" USING "btree" ("rule_verification");



CREATE INDEX "processes_state_code_idx" ON "public"."processes" USING "btree" ("state_code");



CREATE INDEX "processes_team_id_idx" ON "public"."processes" USING "btree" ("team_id");



CREATE INDEX "processes_text_pgroonga" ON "public"."processes" USING "pgroonga" ("extracted_text");



CREATE INDEX "processes_user_id_created_at_idx" ON "public"."processes" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "reviews_data_id_data_version_idx" ON "public"."reviews" USING "btree" ("data_id", "data_version");



CREATE INDEX "roles_role_idx" ON "public"."roles" USING "btree" ("role");



CREATE INDEX "roles_team_id_user_id_role_idx" ON "public"."roles" USING "btree" ("team_id", "user_id", "role");



CREATE INDEX "sources_created_at_idx" ON "public"."sources" USING "btree" ("created_at" DESC);



CREATE INDEX "sources_json_dataversion" ON "public"."sources" USING "btree" (((((("json" -> 'sourceDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "sources_json_idx" ON "public"."sources" USING "gin" ("json");



CREATE INDEX "sources_json_ordered_vector" ON "public"."sources" USING "hnsw" ("embedding" "extensions"."vector_cosine_ops");



CREATE INDEX "sources_json_pgroonga" ON "public"."sources" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2");



CREATE INDEX "sources_json_publicationtype" ON "public"."sources" USING "btree" (((((("json" -> 'sourceDataSet'::"text") -> 'sourceInformation'::"text") -> 'dataSetInformation'::"text") ->> 'publicationType'::"text")));



CREATE INDEX "sources_json_sourcecitation" ON "public"."sources" USING "btree" (((((("json" -> 'sourceDataSet'::"text") -> 'sourceInformation'::"text") -> 'dataSetInformation'::"text") ->> 'sourceCitation'::"text")));



CREATE INDEX "sources_modified_at_idx" ON "public"."sources" USING "btree" ("modified_at");



CREATE INDEX "sources_user_id_created_at_idx" ON "public"."sources" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "unitgroups_created_at_idx" ON "public"."unitgroups" USING "btree" ("created_at" DESC);



CREATE INDEX "unitgroups_json_dataversion" ON "public"."unitgroups" USING "btree" (((((("json" -> 'unitGroupDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "unitgroups_json_idx" ON "public"."unitgroups" USING "gin" ("json");



CREATE INDEX "unitgroups_json_ordered_vector" ON "public"."unitgroups" USING "hnsw" ("embedding" "extensions"."vector_cosine_ops");



CREATE INDEX "unitgroups_json_pgroonga" ON "public"."unitgroups" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2");



CREATE INDEX "unitgroups_json_referencetoreferenceunit" ON "public"."unitgroups" USING "btree" (((((("json" -> 'unitGroupDataSet'::"text") -> 'unitGroupInformation'::"text") -> 'quantitativeReference'::"text") ->> 'referenceToReferenceUnit'::"text")));



CREATE INDEX "unitgroups_modified_at_idx" ON "public"."unitgroups" USING "btree" ("modified_at");



CREATE INDEX "unitgroups_user_id_created_at_idx" ON "public"."unitgroups" USING "btree" ("user_id", "created_at" DESC);



CREATE OR REPLACE TRIGGER "contacts_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."contacts" FOR EACH ROW EXECUTE FUNCTION "public"."contacts_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "contacts_set_modified_at_trigger" BEFORE UPDATE ON "public"."contacts" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "flow_embedding_ft_on_extract_md_update" AFTER UPDATE OF "extracted_md" ON "public"."flows" FOR EACH ROW WHEN (("old"."extracted_md" IS DISTINCT FROM "new"."extracted_md")) EXECUTE FUNCTION "util"."queue_embeddings"('flows_embedding_ft_input', 'embedding_ft', 'embedding_ft');



CREATE OR REPLACE TRIGGER "flow_extract_md_trigger_insert" AFTER INSERT ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_flow_embedding_ft', '1000');



CREATE OR REPLACE TRIGGER "flow_extract_md_trigger_update" AFTER UPDATE OF "json" ON "public"."flows" FOR EACH ROW WHEN (("new"."json" IS DISTINCT FROM "old"."json")) EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_flow_embedding_ft', '1000');



CREATE OR REPLACE TRIGGER "flow_extract_md_trigger_update_flag" AFTER UPDATE OF "embedding_flag" ON "public"."flows" FOR EACH ROW WHEN (("new"."embedding_flag" IS DISTINCT FROM "old"."embedding_flag")) EXECUTE FUNCTION "util"."queue_embedding_webhook"();



CREATE OR REPLACE TRIGGER "flow_extract_text_trigger_insert" AFTER INSERT ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_flow_embedding', '1000');



CREATE OR REPLACE TRIGGER "flow_extract_text_trigger_update" AFTER UPDATE OF "json" ON "public"."flows" FOR EACH ROW WHEN (("new"."json" IS DISTINCT FROM "old"."json")) EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_flow_embedding', '1000');



CREATE OR REPLACE TRIGGER "flowproperties_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."flowproperties" FOR EACH ROW EXECUTE FUNCTION "public"."flowproperties_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "flowproperties_set_modified_at_trigger" BEFORE UPDATE ON "public"."flowproperties" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "flows_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "public"."flows_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "flows_set_modified_at_trigger" BEFORE UPDATE ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "ilcd_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."ilcd" FOR EACH ROW EXECUTE FUNCTION "public"."sync_json_to_jsonb"();



CREATE OR REPLACE TRIGGER "ilcd_set_modified_at_trigger" BEFORE UPDATE ON "public"."ilcd" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "lciamethods_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."lciamethods" FOR EACH ROW EXECUTE FUNCTION "public"."lciamethods_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "lciamethods_set_modified_at_trigger" BEFORE UPDATE ON "public"."lciamethods" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "lifecyclemodel_embedding_ft_on_extract_md_update" AFTER UPDATE OF "extracted_md" ON "public"."lifecyclemodels" FOR EACH ROW WHEN (("old"."extracted_md" IS DISTINCT FROM "new"."extracted_md")) EXECUTE FUNCTION "util"."queue_embeddings"('lifecyclemodels_embedding_ft_input', 'embedding_ft', 'embedding_ft');



CREATE OR REPLACE TRIGGER "lifecyclemodel_extract_md_trigger_insert" AFTER INSERT ON "public"."lifecyclemodels" FOR EACH ROW EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_model_embedding_ft', '1000');



CREATE OR REPLACE TRIGGER "lifecyclemodel_extract_md_trigger_update" AFTER UPDATE OF "json" ON "public"."lifecyclemodels" FOR EACH ROW WHEN (("new"."json" IS DISTINCT FROM "old"."json")) EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_model_embedding_ft', '1000');



CREATE OR REPLACE TRIGGER "lifecyclemodel_extract_md_trigger_update_flag" AFTER UPDATE OF "embedding_flag" ON "public"."lifecyclemodels" FOR EACH ROW WHEN (("new"."embedding_flag" IS DISTINCT FROM "old"."embedding_flag")) EXECUTE FUNCTION "util"."queue_embedding_webhook"();



CREATE OR REPLACE TRIGGER "lifecyclemodels_extract_text_trigger_insert" AFTER INSERT ON "public"."lifecyclemodels" FOR EACH ROW EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_model_embedding', '1000');



CREATE OR REPLACE TRIGGER "lifecyclemodels_extract_text_trigger_update" AFTER UPDATE OF "json" ON "public"."lifecyclemodels" FOR EACH ROW WHEN (("new"."json" IS DISTINCT FROM "old"."json")) EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_model_embedding', '1000');



CREATE OR REPLACE TRIGGER "lifecyclemodels_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."lifecyclemodels" FOR EACH ROW EXECUTE FUNCTION "public"."lifecyclemodels_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "lifecyclemodels_set_modified_at_trigger" BEFORE UPDATE ON "public"."lifecyclemodels" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "notifications_set_modified_at_trigger" BEFORE UPDATE ON "public"."notifications" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "process_embedding_ft_on_extract_md_update" AFTER UPDATE OF "extracted_md" ON "public"."processes" FOR EACH ROW WHEN (("old"."extracted_md" IS DISTINCT FROM "new"."extracted_md")) EXECUTE FUNCTION "util"."queue_embeddings"('processes_embedding_ft_input', 'embedding_ft', 'embedding_ft');



CREATE OR REPLACE TRIGGER "process_extract_md_trigger_insert" AFTER INSERT ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_process_embedding_ft', '1000');



CREATE OR REPLACE TRIGGER "process_extract_md_trigger_update" AFTER UPDATE OF "json" ON "public"."processes" FOR EACH ROW WHEN (("new"."json" IS DISTINCT FROM "old"."json")) EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_process_embedding_ft', '1000');



CREATE OR REPLACE TRIGGER "process_extract_md_trigger_update_flag" AFTER UPDATE OF "embedding_flag" ON "public"."processes" FOR EACH ROW WHEN (("new"."embedding_flag" IS DISTINCT FROM "old"."embedding_flag")) EXECUTE FUNCTION "util"."queue_embedding_webhook"();



CREATE OR REPLACE TRIGGER "process_extract_text_trigger_insert" AFTER INSERT ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_process_embedding', '1000');



CREATE OR REPLACE TRIGGER "process_extract_text_trigger_update" AFTER UPDATE OF "json" ON "public"."processes" FOR EACH ROW WHEN (("new"."json" IS DISTINCT FROM "old"."json")) EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_process_embedding', '1000');



CREATE OR REPLACE TRIGGER "processes_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "public"."processes_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "processes_set_modified_at_trigger" BEFORE UPDATE ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "roles_set_modified_at_trigger" BEFORE UPDATE ON "public"."roles" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "sources_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."sources" FOR EACH ROW EXECUTE FUNCTION "public"."sources_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "sources_set_modified_at_trigger" BEFORE UPDATE ON "public"."sources" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "teams_set_modified_at_trigger" BEFORE UPDATE ON "public"."teams" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "unitgroups_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."unitgroups" FOR EACH ROW EXECUTE FUNCTION "public"."unitgroups_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "unitgroups_set_modified_at_trigger" BEFORE UPDATE ON "public"."unitgroups" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



ALTER TABLE ONLY "public"."comments"
    ADD CONSTRAINT "comments_review_id_fkey" FOREIGN KEY ("review_id") REFERENCES "public"."reviews"("id");



ALTER TABLE ONLY "public"."lca_active_snapshots"
    ADD CONSTRAINT "lca_active_snapshots_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE RESTRICT;



ALTER TABLE ONLY "public"."lca_factorization_registry"
    ADD CONSTRAINT "lca_factorization_registry_prepared_job_fk" FOREIGN KEY ("prepared_job_id") REFERENCES "public"."lca_jobs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_factorization_registry"
    ADD CONSTRAINT "lca_factorization_registry_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."lca_jobs"
    ADD CONSTRAINT "lca_jobs_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."lca_latest_all_unit_results"
    ADD CONSTRAINT "lca_latest_all_unit_results_job_fk" FOREIGN KEY ("job_id") REFERENCES "public"."lca_jobs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."lca_latest_all_unit_results"
    ADD CONSTRAINT "lca_latest_all_unit_results_result_fk" FOREIGN KEY ("result_id") REFERENCES "public"."lca_results"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."lca_latest_all_unit_results"
    ADD CONSTRAINT "lca_latest_all_unit_results_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."lca_network_snapshots"
    ADD CONSTRAINT "lca_network_snapshots_lcia_fk" FOREIGN KEY ("lcia_method_id", "lcia_method_version") REFERENCES "public"."lciamethods"("id", "version") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_package_artifacts"
    ADD CONSTRAINT "lca_package_artifacts_job_fk" FOREIGN KEY ("job_id") REFERENCES "public"."lca_package_jobs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."lca_package_export_items"
    ADD CONSTRAINT "lca_package_export_items_job_fk" FOREIGN KEY ("job_id") REFERENCES "public"."lca_package_jobs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."lca_package_request_cache"
    ADD CONSTRAINT "lca_package_request_cache_export_artifact_fk" FOREIGN KEY ("export_artifact_id") REFERENCES "public"."lca_package_artifacts"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_package_request_cache"
    ADD CONSTRAINT "lca_package_request_cache_job_fk" FOREIGN KEY ("job_id") REFERENCES "public"."lca_package_jobs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_package_request_cache"
    ADD CONSTRAINT "lca_package_request_cache_report_artifact_fk" FOREIGN KEY ("report_artifact_id") REFERENCES "public"."lca_package_artifacts"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_result_cache"
    ADD CONSTRAINT "lca_result_cache_job_fk" FOREIGN KEY ("job_id") REFERENCES "public"."lca_jobs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_result_cache"
    ADD CONSTRAINT "lca_result_cache_result_fk" FOREIGN KEY ("result_id") REFERENCES "public"."lca_results"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_result_cache"
    ADD CONSTRAINT "lca_result_cache_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."lca_results"
    ADD CONSTRAINT "lca_results_job_fk" FOREIGN KEY ("job_id") REFERENCES "public"."lca_jobs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."lca_results"
    ADD CONSTRAINT "lca_results_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."lca_snapshot_artifacts"
    ADD CONSTRAINT "lca_snapshot_artifacts_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."notifications"
    ADD CONSTRAINT "notifications_recipient_user_id_fkey" FOREIGN KEY ("recipient_user_id") REFERENCES "auth"."users"("id");



ALTER TABLE ONLY "public"."notifications"
    ADD CONSTRAINT "notifications_sender_user_id_fkey" FOREIGN KEY ("sender_user_id") REFERENCES "auth"."users"("id");



ALTER TABLE ONLY "public"."roles"
    ADD CONSTRAINT "roles_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id");



CREATE POLICY "Enable delete for users based on user_id" ON "public"."contacts" FOR DELETE TO "authenticated" USING ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable delete for users based on user_id" ON "public"."flowproperties" FOR DELETE TO "authenticated" USING ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable delete for users based on user_id" ON "public"."flows" FOR DELETE TO "authenticated" USING ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable delete for users based on user_id" ON "public"."lifecyclemodels" FOR DELETE TO "authenticated" USING ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable delete for users based on user_id" ON "public"."processes" FOR DELETE TO "authenticated" USING ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable delete for users based on user_id" ON "public"."sources" FOR DELETE TO "authenticated" USING ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable delete for users based on user_id" ON "public"."unitgroups" FOR DELETE TO "authenticated" USING ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable insert data access for self" ON "public"."reviews" FOR INSERT TO "authenticated" WITH CHECK ((("auth"."uid"() IS NOT NULL) AND (((("json" -> 'user'::"text") ->> 'id'::"text"))::"uuid" = ( SELECT "auth"."uid"() AS "uid"))));



CREATE POLICY "Enable insert for authenticated users only" ON "public"."contacts" FOR INSERT TO "authenticated" WITH CHECK ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable insert for authenticated users only" ON "public"."flowproperties" FOR INSERT TO "authenticated" WITH CHECK ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable insert for authenticated users only" ON "public"."flows" FOR INSERT TO "authenticated" WITH CHECK ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable insert for authenticated users only" ON "public"."lifecyclemodels" FOR INSERT TO "authenticated" WITH CHECK ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable insert for authenticated users only" ON "public"."processes" FOR INSERT TO "authenticated" WITH CHECK ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable insert for authenticated users only" ON "public"."sources" FOR INSERT TO "authenticated" WITH CHECK ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable insert for authenticated users only" ON "public"."unitgroups" FOR INSERT TO "authenticated" WITH CHECK ((("state_code" = 0) AND (( SELECT "auth"."uid"() AS "uid") = "user_id")));



CREATE POLICY "Enable insert for review-admin" ON "public"."comments" FOR INSERT TO "authenticated" WITH CHECK ((EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid")) AND (("roles"."role")::"text" = 'review-admin'::"text")))));



CREATE POLICY "Enable read access for all users" ON "public"."ilcd" FOR SELECT TO "authenticated" USING (true);



CREATE POLICY "Enable read access for all users" ON "public"."lciamethods" FOR SELECT TO "authenticated" USING (true);



CREATE POLICY "Enable read access for authenticated users" ON "public"."contacts" FOR SELECT USING ((("state_code" >= 100) OR (( SELECT "auth"."uid"() AS "uid") = "user_id") OR (EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."team_id" = "contacts"."team_id") AND (("roles"."role")::"text" = ANY (ARRAY[('admin'::character varying)::"text", ('member'::character varying)::"text", ('owner'::character varying)::"text"])) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (("state_code" = 20) AND ((EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid") AND (("roles"."role")::"text" = 'review-admin'::"text") AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."state_code" > 0) AND (((("r"."json" -> 'data'::"text") ->> 'id'::"text"))::"uuid" = "contacts"."id") AND ((("r"."json" -> 'data'::"text") ->> 'version'::"text") = ("contacts"."version")::"text") AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."id" IN ( SELECT (("review_item"."value" ->> 'id'::"text"))::"uuid" AS "uuid"
           FROM "jsonb_array_elements"("contacts"."reviews") "review_item"("value"))) AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text")))))))));



CREATE POLICY "Enable read access for authenticated users" ON "public"."flowproperties" FOR SELECT TO "authenticated" USING ((("state_code" >= 100) OR (( SELECT "auth"."uid"() AS "uid") = "user_id") OR (EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."team_id" = "flowproperties"."team_id") AND (("roles"."role")::"text" = ANY (ARRAY[('admin'::character varying)::"text", ('member'::character varying)::"text", ('owner'::character varying)::"text"])) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (("state_code" = 20) AND ((EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid") AND (("roles"."role")::"text" = 'review-admin'::"text") AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."state_code" > 0) AND (((("r"."json" -> 'data'::"text") ->> 'id'::"text"))::"uuid" = "flowproperties"."id") AND ((("r"."json" -> 'data'::"text") ->> 'version'::"text") = ("flowproperties"."version")::"text") AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."id" IN ( SELECT (("review_item"."value" ->> 'id'::"text"))::"uuid" AS "uuid"
           FROM "jsonb_array_elements"("flowproperties"."reviews") "review_item"("value"))) AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text")))))))));



CREATE POLICY "Enable read access for authenticated users" ON "public"."flows" FOR SELECT TO "authenticated" USING ((("state_code" >= 100) OR (( SELECT "auth"."uid"() AS "uid") = "user_id") OR (EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."team_id" = "flows"."team_id") AND (("roles"."role")::"text" = ANY (ARRAY[('admin'::character varying)::"text", ('member'::character varying)::"text", ('owner'::character varying)::"text"])) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (("state_code" = 20) AND ((EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid") AND (("roles"."role")::"text" = 'review-admin'::"text") AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."state_code" > 0) AND (((("r"."json" -> 'data'::"text") ->> 'id'::"text"))::"uuid" = "flows"."id") AND ((("r"."json" -> 'data'::"text") ->> 'version'::"text") = ("flows"."version")::"text") AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."id" IN ( SELECT (("review_item"."value" ->> 'id'::"text"))::"uuid" AS "uuid"
           FROM "jsonb_array_elements"("flows"."reviews") "review_item"("value"))) AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text")))))))));



CREATE POLICY "Enable read access for authenticated users" ON "public"."lifecyclemodels" FOR SELECT TO "authenticated" USING ((("state_code" >= 100) OR (( SELECT "auth"."uid"() AS "uid") = "user_id") OR (EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."team_id" = "lifecyclemodels"."team_id") AND (("roles"."role")::"text" = ANY (ARRAY[('admin'::character varying)::"text", ('member'::character varying)::"text", ('owner'::character varying)::"text"])) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (("state_code" = 20) AND ((EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid") AND (("roles"."role")::"text" = 'review-admin'::"text") AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."state_code" > 0) AND (((("r"."json" -> 'data'::"text") ->> 'id'::"text"))::"uuid" = "lifecyclemodels"."id") AND ((("r"."json" -> 'data'::"text") ->> 'version'::"text") = ("lifecyclemodels"."version")::"text") AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."id" IN ( SELECT (("review_item"."value" ->> 'id'::"text"))::"uuid" AS "uuid"
           FROM "jsonb_array_elements"("lifecyclemodels"."reviews") "review_item"("value"))) AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text")))))))));



CREATE POLICY "Enable read access for authenticated users" ON "public"."processes" FOR SELECT TO "authenticated" USING ((("state_code" >= 100) OR (( SELECT "auth"."uid"() AS "uid") = "user_id") OR (EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."team_id" = "processes"."team_id") AND (("roles"."role")::"text" = ANY (ARRAY[('admin'::character varying)::"text", ('member'::character varying)::"text", ('owner'::character varying)::"text"])) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR ((EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid") AND (("roles"."role")::"text" = 'review-admin'::"text") AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."state_code" > 0) AND (((("r"."json" -> 'data'::"text") ->> 'id'::"text"))::"uuid" = "processes"."id") AND ((("r"."json" -> 'data'::"text") ->> 'version'::"text") = ("processes"."version")::"text") AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."id" IN ( SELECT (("review_item"."value" ->> 'id'::"text"))::"uuid" AS "uuid"
           FROM "jsonb_array_elements"("processes"."reviews") "review_item"("value"))) AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))))));



CREATE POLICY "Enable read access for authenticated users" ON "public"."sources" FOR SELECT TO "authenticated" USING ((("state_code" >= 100) OR (( SELECT "auth"."uid"() AS "uid") = "user_id") OR (EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."team_id" = "sources"."team_id") AND (("roles"."role")::"text" = ANY (ARRAY[('admin'::character varying)::"text", ('member'::character varying)::"text", ('owner'::character varying)::"text"])) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (("state_code" = 20) AND ((EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid") AND (("roles"."role")::"text" = 'review-admin'::"text") AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."state_code" > 0) AND (((("r"."json" -> 'data'::"text") ->> 'id'::"text"))::"uuid" = "sources"."id") AND ((("r"."json" -> 'data'::"text") ->> 'version'::"text") = ("sources"."version")::"text") AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."id" IN ( SELECT (("review_item"."value" ->> 'id'::"text"))::"uuid" AS "uuid"
           FROM "jsonb_array_elements"("sources"."reviews") "review_item"("value"))) AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text")))))))));



CREATE POLICY "Enable read access for authenticated users" ON "public"."unitgroups" FOR SELECT TO "authenticated" USING ((("state_code" >= 100) OR (( SELECT "auth"."uid"() AS "uid") = "user_id") OR (EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."team_id" = "unitgroups"."team_id") AND (("roles"."role")::"text" = ANY (ARRAY[('admin'::character varying)::"text", ('member'::character varying)::"text", ('owner'::character varying)::"text"])) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (("state_code" = 20) AND ((EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid") AND (("roles"."role")::"text" = 'review-admin'::"text") AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."state_code" > 0) AND (((("r"."json" -> 'data'::"text") ->> 'id'::"text"))::"uuid" = "unitgroups"."id") AND ((("r"."json" -> 'data'::"text") ->> 'version'::"text") = ("unitgroups"."version")::"text") AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."id" IN ( SELECT (("review_item"."value" ->> 'id'::"text"))::"uuid" AS "uuid"
           FROM "jsonb_array_elements"("unitgroups"."reviews") "review_item"("value"))) AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text")))))))));



ALTER TABLE "public"."comments" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "comments select by review participants" ON "public"."comments" FOR SELECT TO "authenticated" USING ((("auth"."uid"() IS NOT NULL) AND "public"."policy_review_can_read"("review_id", "auth"."uid"()) AND ("public"."cmd_review_is_review_admin"("auth"."uid"()) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."id" = "comments"."review_id") AND (((("r"."json" -> 'user'::"text") ->> 'id'::"text"))::"uuid" = "auth"."uid"())))) OR ("reviewer_id" = "auth"."uid"()))));



CREATE POLICY "comments update by review-admin" ON "public"."comments" FOR UPDATE TO "authenticated" USING ("public"."policy_is_current_user_in_roles"('00000000-0000-0000-0000-000000000000'::"uuid", ARRAY['review-admin'::"text"])) WITH CHECK ("public"."policy_is_current_user_in_roles"('00000000-0000-0000-0000-000000000000'::"uuid", ARRAY['review-admin'::"text"]));



CREATE POLICY "comments update by reviewer self" ON "public"."comments" FOR UPDATE TO "authenticated" USING (("reviewer_id" = "auth"."uid"())) WITH CHECK (("reviewer_id" = "auth"."uid"()));



ALTER TABLE "public"."contacts" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "delete by owner and admin" ON "public"."roles" FOR DELETE TO "authenticated" USING ("public"."policy_roles_delete"("user_id", "team_id", ("role")::"text"));



ALTER TABLE "public"."flowproperties" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."flows" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."ilcd" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "insert by authenticated" ON "public"."roles" FOR INSERT TO "authenticated" WITH CHECK ("public"."policy_roles_insert"("user_id", "team_id", ("role")::"text"));



CREATE POLICY "insert by authenticated" ON "public"."teams" FOR INSERT TO "authenticated" WITH CHECK ((( SELECT "count"(1) AS "count"
   FROM "public"."roles"
  WHERE (("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid")) AND (("roles"."role")::"text" <> 'rejected'::"text") AND ("roles"."team_id" <> '00000000-0000-0000-0000-000000000000'::"uuid"))) = 0));



ALTER TABLE "public"."lca_active_snapshots" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_active_snapshots_service_role_all" ON "public"."lca_active_snapshots" TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."lca_factorization_registry" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_factorization_registry_service_role_all" ON "public"."lca_factorization_registry" TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."lca_jobs" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_jobs_select_own" ON "public"."lca_jobs" FOR SELECT TO "authenticated" USING (("requested_by" = ( SELECT "auth"."uid"() AS "uid")));



ALTER TABLE "public"."lca_latest_all_unit_results" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_latest_all_unit_results_service_role_all" ON "public"."lca_latest_all_unit_results" TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."lca_network_snapshots" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_network_snapshots_service_role_all" ON "public"."lca_network_snapshots" TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."lca_package_artifacts" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_package_artifacts_select_own" ON "public"."lca_package_artifacts" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "public"."lca_package_jobs" "j"
  WHERE (("j"."id" = "lca_package_artifacts"."job_id") AND ("j"."requested_by" = "auth"."uid"())))));



ALTER TABLE "public"."lca_package_export_items" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."lca_package_jobs" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_package_jobs_select_own" ON "public"."lca_package_jobs" FOR SELECT TO "authenticated" USING (("requested_by" = "auth"."uid"()));



ALTER TABLE "public"."lca_package_request_cache" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_package_request_cache_select_own" ON "public"."lca_package_request_cache" FOR SELECT TO "authenticated" USING (("requested_by" = "auth"."uid"()));



ALTER TABLE "public"."lca_result_cache" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_result_cache_service_role_all" ON "public"."lca_result_cache" TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."lca_results" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_results_select_own" ON "public"."lca_results" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "public"."lca_jobs" "j"
  WHERE (("j"."id" = "lca_results"."job_id") AND ("j"."requested_by" = ( SELECT "auth"."uid"() AS "uid"))))));



ALTER TABLE "public"."lca_snapshot_artifacts" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_snapshot_artifacts_service_role_all" ON "public"."lca_snapshot_artifacts" TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."lciamethods" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."lifecyclemodels" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."notifications" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "notifications_delete_recipient_only" ON "public"."notifications" FOR DELETE TO "authenticated" USING (("auth"."uid"() = "recipient_user_id"));



CREATE POLICY "notifications_insert_sender" ON "public"."notifications" FOR INSERT TO "authenticated" WITH CHECK (("auth"."uid"() = "sender_user_id"));



CREATE POLICY "notifications_select_sender_or_recipient" ON "public"."notifications" FOR SELECT TO "authenticated" USING ((("auth"."uid"() = "sender_user_id") OR ("auth"."uid"() = "recipient_user_id")));



CREATE POLICY "notifications_update_sender" ON "public"."notifications" FOR UPDATE TO "authenticated" USING (("auth"."uid"() = "sender_user_id")) WITH CHECK (("auth"."uid"() = "sender_user_id"));



ALTER TABLE "public"."processes" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."reviews" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "reviews select by review participants" ON "public"."reviews" FOR SELECT TO "authenticated" USING ((("auth"."uid"() IS NOT NULL) AND "public"."policy_review_can_read"("id", "auth"."uid"())));



ALTER TABLE "public"."roles" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "select by owner or public teams" ON "public"."teams" FOR SELECT TO "authenticated" USING (("is_public" OR ("rank" > 0) OR (EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE ((("roles"."team_id" = "teams"."id") OR ("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid")) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid")) AND (("roles"."role")::"text" <> 'rejected'::"text"))))));



CREATE POLICY "select by self and team" ON "public"."roles" FOR SELECT TO "authenticated" USING ("public"."policy_roles_select"("team_id", ("role")::"text"));



CREATE POLICY "select by self and team and admin" ON "public"."users" FOR SELECT TO "authenticated" USING ((("id" = ( SELECT "auth"."uid"() AS "uid")) OR ("id" IN ( SELECT "r"."user_id"
   FROM "public"."roles" "r"
  WHERE ((("r"."role")::"text" = 'owner'::"text") AND ("public"."policy_is_team_public"("r"."team_id") = true)))) OR ("id" IN ( SELECT "r0"."user_id"
   FROM "public"."roles" "r0"
  WHERE ("r0"."team_id" IN ( SELECT "r"."team_id"
           FROM "public"."roles" "r"
          WHERE (("r"."user_id" = ( SELECT "auth"."uid"() AS "uid")) AND (("r"."role")::"text" <> 'rejected'::"text")))))) OR "public"."policy_is_current_user_in_roles"('00000000-0000-0000-0000-000000000000'::"uuid", ARRAY['admin'::"text", 'review-admin'::"text", 'review-member'::"text"])));



ALTER TABLE "public"."sources" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."teams" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "transitional_reviews_update_submitter_only" ON "public"."reviews" FOR UPDATE TO "authenticated" USING ((("auth"."uid"() IS NOT NULL) AND (((("json" -> 'user'::"text") ->> 'id'::"text"))::"uuid" = "auth"."uid"()))) WITH CHECK ((("auth"."uid"() IS NOT NULL) AND (((("json" -> 'user'::"text") ->> 'id'::"text"))::"uuid" = "auth"."uid"())));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."contacts" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = "auth"."uid"()))) WITH CHECK ((("state_code" = 0) AND ("user_id" = "auth"."uid"())));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."flowproperties" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = "auth"."uid"()))) WITH CHECK ((("state_code" = 0) AND ("user_id" = "auth"."uid"())));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."flows" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = "auth"."uid"()))) WITH CHECK ((("state_code" = 0) AND ("user_id" = "auth"."uid"())));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."lifecyclemodels" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = "auth"."uid"()))) WITH CHECK ((("state_code" = 0) AND ("user_id" = "auth"."uid"())));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."processes" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = "auth"."uid"()))) WITH CHECK ((("state_code" = 0) AND ("user_id" = "auth"."uid"())));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."sources" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = "auth"."uid"()))) WITH CHECK ((("state_code" = 0) AND ("user_id" = "auth"."uid"())));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."unitgroups" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = "auth"."uid"()))) WITH CHECK ((("state_code" = 0) AND ("user_id" = "auth"."uid"())));



ALTER TABLE "public"."unitgroups" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "update by admin or owner or self" ON "public"."roles" FOR UPDATE TO "authenticated" USING ("public"."policy_roles_update"("user_id", "team_id", ("role")::"text"));



CREATE POLICY "update by owner and admin" ON "public"."teams" FOR UPDATE TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "public"."roles" "r"
  WHERE (("r"."user_id" = "auth"."uid"()) AND ("r"."team_id" = "teams"."id") AND (("r"."role")::"text" = ANY ((ARRAY['owner'::character varying, 'admin'::character varying])::"text"[])))))) WITH CHECK ((EXISTS ( SELECT 1
   FROM "public"."roles" "r"
  WHERE (("r"."user_id" = "auth"."uid"()) AND ("r"."team_id" = "teams"."id") AND (("r"."role")::"text" = ANY ((ARRAY['owner'::character varying, 'admin'::character varying])::"text"[]))))));



ALTER TABLE "public"."users" ENABLE ROW LEVEL SECURITY;


GRANT USAGE ON SCHEMA "public" TO "postgres";
GRANT USAGE ON SCHEMA "public" TO "anon";
GRANT USAGE ON SCHEMA "public" TO "authenticated";
GRANT USAGE ON SCHEMA "public" TO "service_role";



GRANT ALL ON FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "extensions"."vector", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" numeric, "extracted_text_weight" numeric, "semantic_weight" numeric, "rrf_k" integer, "data_source" "text", "this_user_id" "text", "page_size" integer, "page_current" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "extensions"."vector", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" numeric, "extracted_text_weight" numeric, "semantic_weight" numeric, "rrf_k" integer, "data_source" "text", "this_user_id" "text", "page_size" integer, "page_current" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "extensions"."vector", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" numeric, "extracted_text_weight" numeric, "semantic_weight" numeric, "rrf_k" integer, "data_source" "text", "this_user_id" "text", "page_size" integer, "page_current" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" numeric, "extracted_text_weight" numeric, "semantic_weight" numeric, "rrf_k" integer, "data_source" "text", "this_user_id" "text", "page_size" integer, "page_current" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" numeric, "extracted_text_weight" numeric, "semantic_weight" numeric, "rrf_k" integer, "data_source" "text", "this_user_id" "text", "page_size" integer, "page_current" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" numeric, "extracted_text_weight" numeric, "semantic_weight" numeric, "rrf_k" integer, "data_source" "text", "this_user_id" "text", "page_size" integer, "page_current" integer) TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_assign_team"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_team_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_assign_team"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_team_id" "uuid", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_dataset_assign_team"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_team_id" "uuid", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_dataset_assign_team"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_team_id" "uuid", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_create"("p_table" "text", "p_id" "uuid", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_create"("p_table" "text", "p_id" "uuid", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_dataset_create"("p_table" "text", "p_id" "uuid", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_dataset_create"("p_table" "text", "p_id" "uuid", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_delete"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_delete"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_dataset_delete"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_dataset_delete"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_publish"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_publish"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_dataset_publish"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_dataset_publish"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_save_draft"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_save_draft"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_dataset_save_draft"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_dataset_save_draft"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_json_ordered" "jsonb", "p_model_id" "uuid", "p_rule_verification" boolean, "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_membership_is_review_admin"("p_actor" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_membership_is_review_admin"("p_actor" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_membership_is_review_admin"("p_actor" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_membership_is_review_admin"("p_actor" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_membership_is_system_manager"("p_actor" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_membership_is_system_manager"("p_actor" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_membership_is_system_manager"("p_actor" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_membership_is_system_manager"("p_actor" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_membership_is_system_owner"("p_actor" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_membership_is_system_owner"("p_actor" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_membership_is_system_owner"("p_actor" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_membership_is_system_owner"("p_actor" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_membership_is_team_manager"("p_actor" "uuid", "p_team_id" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_membership_is_team_manager"("p_actor" "uuid", "p_team_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_membership_is_team_manager"("p_actor" "uuid", "p_team_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_membership_is_team_manager"("p_actor" "uuid", "p_team_id" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_membership_is_team_owner"("p_actor" "uuid", "p_team_id" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_membership_is_team_owner"("p_actor" "uuid", "p_team_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_membership_is_team_owner"("p_actor" "uuid", "p_team_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_membership_is_team_owner"("p_actor" "uuid", "p_team_id" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_membership_resolve_member_order_by"("p_sort_by" "text", "p_allow_workload" boolean) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_membership_resolve_member_order_by"("p_sort_by" "text", "p_allow_workload" boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_membership_resolve_member_order_by"("p_sort_by" "text", "p_allow_workload" boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_membership_resolve_member_order_by"("p_sort_by" "text", "p_allow_workload" boolean) TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_membership_resolve_sort_direction"("p_sort_order" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_membership_resolve_sort_direction"("p_sort_order" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_membership_resolve_sort_direction"("p_sort_order" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_membership_resolve_sort_direction"("p_sort_order" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_notification_normalize_text_array"("p_values" "text"[]) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_notification_normalize_text_array"("p_values" "text"[]) TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_notification_normalize_text_array"("p_values" "text"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_notification_normalize_text_array"("p_values" "text"[]) TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_notification_send_validation_issue"("p_recipient_user_id" "uuid", "p_dataset_type" "text", "p_dataset_id" "uuid", "p_dataset_version" "text", "p_link" "text", "p_issue_codes" "text"[], "p_tab_names" "text"[], "p_issue_count" integer, "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_notification_send_validation_issue"("p_recipient_user_id" "uuid", "p_dataset_type" "text", "p_dataset_id" "uuid", "p_dataset_version" "text", "p_link" "text", "p_issue_codes" "text"[], "p_tab_names" "text"[], "p_issue_count" integer, "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_notification_send_validation_issue"("p_recipient_user_id" "uuid", "p_dataset_type" "text", "p_dataset_id" "uuid", "p_dataset_version" "text", "p_link" "text", "p_issue_codes" "text"[], "p_tab_names" "text"[], "p_issue_count" integer, "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_notification_send_validation_issue"("p_recipient_user_id" "uuid", "p_dataset_type" "text", "p_dataset_id" "uuid", "p_dataset_version" "text", "p_link" "text", "p_issue_codes" "text"[], "p_tab_names" "text"[], "p_issue_count" integer, "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_append_log"("p_review_json" "jsonb", "p_action" "text", "p_actor" "uuid", "p_extra" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_append_log"("p_review_json" "jsonb", "p_action" "text", "p_actor" "uuid", "p_extra" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_append_log"("p_review_json" "jsonb", "p_action" "text", "p_actor" "uuid", "p_extra" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_append_log"("p_review_json" "jsonb", "p_action" "text", "p_actor" "uuid", "p_extra" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_append_review_ref"("p_existing_reviews" "jsonb", "p_review_id" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_append_review_ref"("p_existing_reviews" "jsonb", "p_review_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_append_review_ref"("p_existing_reviews" "jsonb", "p_review_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_append_review_ref"("p_existing_reviews" "jsonb", "p_review_id" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_apply_model_validation_to_process_json"("p_process_json" "jsonb", "p_model_json" "jsonb", "p_comment_review" "jsonb", "p_comment_compliance" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_apply_model_validation_to_process_json"("p_process_json" "jsonb", "p_model_json" "jsonb", "p_comment_review" "jsonb", "p_comment_compliance" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_apply_model_validation_to_process_json"("p_process_json" "jsonb", "p_model_json" "jsonb", "p_comment_review" "jsonb", "p_comment_compliance" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_apply_model_validation_to_process_json"("p_process_json" "jsonb", "p_model_json" "jsonb", "p_comment_review" "jsonb", "p_comment_compliance" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_apply_mv_payload"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_review_items" "jsonb", "p_compliance_items" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_apply_mv_payload"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_review_items" "jsonb", "p_compliance_items" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_apply_mv_payload"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_review_items" "jsonb", "p_compliance_items" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_apply_mv_payload"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_review_items" "jsonb", "p_compliance_items" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_approve"("p_table" "text", "p_review_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_approve"("p_table" "text", "p_review_id" "uuid", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_approve"("p_table" "text", "p_review_id" "uuid", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_approve"("p_table" "text", "p_review_id" "uuid", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone, "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone, "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone, "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone, "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_change_member_role"("p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_change_member_role"("p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_change_member_role"("p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_change_member_role"("p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_collect_dataset_targets"("p_roots" "jsonb", "p_lock" boolean) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_collect_dataset_targets"("p_roots" "jsonb", "p_lock" boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_collect_dataset_targets"("p_roots" "jsonb", "p_lock" boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_collect_dataset_targets"("p_roots" "jsonb", "p_lock" boolean) TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_extract_refs"("p_json" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_extract_refs"("p_json" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_extract_refs"("p_json" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_extract_refs"("p_json" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_get_actor_meta"("p_actor" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_get_actor_meta"("p_actor" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_get_actor_meta"("p_actor" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_get_actor_meta"("p_actor" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_get_dataset_name"("p_table" "text", "p_row" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_get_dataset_name"("p_table" "text", "p_row" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_get_dataset_name"("p_table" "text", "p_row" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_get_dataset_name"("p_table" "text", "p_row" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_get_dataset_row"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_lock" boolean) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_get_dataset_row"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_lock" boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_get_dataset_row"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_lock" boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_get_dataset_row"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_lock" boolean) TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_get_root_table"("p_review_json" "jsonb", "p_data_id" "uuid", "p_data_version" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_get_root_table"("p_review_json" "jsonb", "p_data_id" "uuid", "p_data_version" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_get_root_table"("p_review_json" "jsonb", "p_data_id" "uuid", "p_data_version" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_get_root_table"("p_review_json" "jsonb", "p_data_id" "uuid", "p_data_version" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_is_review_admin"("p_actor" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_is_review_admin"("p_actor" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_is_review_admin"("p_actor" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_is_review_admin"("p_actor" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_is_review_member"("p_actor" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_is_review_member"("p_actor" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_is_review_member"("p_actor" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_is_review_member"("p_actor" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_json_array"("p_value" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_json_array"("p_value" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_json_array"("p_value" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_json_array"("p_value" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_merge_compliance_declarations"("p_existing" "jsonb", "p_additions" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_merge_compliance_declarations"("p_existing" "jsonb", "p_additions" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_merge_compliance_declarations"("p_existing" "jsonb", "p_additions" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_merge_compliance_declarations"("p_existing" "jsonb", "p_additions" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_merge_json_collection"("p_existing" "jsonb", "p_additions" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_merge_json_collection"("p_existing" "jsonb", "p_additions" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_merge_json_collection"("p_existing" "jsonb", "p_additions" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_merge_json_collection"("p_existing" "jsonb", "p_additions" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_merge_validation"("p_existing" "jsonb", "p_additions" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_merge_validation"("p_existing" "jsonb", "p_additions" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_merge_validation"("p_existing" "jsonb", "p_additions" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_merge_validation"("p_existing" "jsonb", "p_additions" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_normalize_reviewer_ids"("p_reviewer_ids" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_normalize_reviewer_ids"("p_reviewer_ids" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_normalize_reviewer_ids"("p_reviewer_ids" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_normalize_reviewer_ids"("p_reviewer_ids" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_ref_type_to_table"("p_ref_type" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_ref_type_to_table"("p_ref_type" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_ref_type_to_table"("p_ref_type" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_ref_type_to_table"("p_ref_type" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_reject"("p_table" "text", "p_review_id" "uuid", "p_reason" "text", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_reject"("p_table" "text", "p_review_id" "uuid", "p_reason" "text", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_reject"("p_table" "text", "p_review_id" "uuid", "p_reason" "text", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_reject"("p_table" "text", "p_review_id" "uuid", "p_reason" "text", "p_audit" "jsonb") TO "service_role";



GRANT ALL ON FUNCTION "public"."cmd_review_resolve_queue_order_by"("p_sort_by" "text", "p_allow_comment_modified" boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_resolve_queue_order_by"("p_sort_by" "text", "p_allow_comment_modified" boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_resolve_queue_order_by"("p_sort_by" "text", "p_allow_comment_modified" boolean) TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_revoke_reviewer"("p_review_id" "uuid", "p_reviewer_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_revoke_reviewer"("p_review_id" "uuid", "p_reviewer_id" "uuid", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_revoke_reviewer"("p_review_id" "uuid", "p_reviewer_id" "uuid", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_revoke_reviewer"("p_review_id" "uuid", "p_reviewer_id" "uuid", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_save_assignment_draft"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_save_assignment_draft"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_save_assignment_draft"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_save_assignment_draft"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_save_comment_draft"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_save_comment_draft"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_save_comment_draft"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_save_comment_draft"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_system_change_member_role"("p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_system_change_member_role"("p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_system_change_member_role"("p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_system_change_member_role"("p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_team_accept_invitation"("p_team_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_team_accept_invitation"("p_team_id" "uuid", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_team_accept_invitation"("p_team_id" "uuid", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_team_accept_invitation"("p_team_id" "uuid", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_team_change_member_role"("p_team_id" "uuid", "p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_team_change_member_role"("p_team_id" "uuid", "p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_team_change_member_role"("p_team_id" "uuid", "p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_team_change_member_role"("p_team_id" "uuid", "p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_team_create"("p_team_id" "uuid", "p_json" "jsonb", "p_rank" integer, "p_is_public" boolean, "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_team_create"("p_team_id" "uuid", "p_json" "jsonb", "p_rank" integer, "p_is_public" boolean, "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_team_create"("p_team_id" "uuid", "p_json" "jsonb", "p_rank" integer, "p_is_public" boolean, "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_team_create"("p_team_id" "uuid", "p_json" "jsonb", "p_rank" integer, "p_is_public" boolean, "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_team_reinvite_member"("p_team_id" "uuid", "p_user_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_team_reinvite_member"("p_team_id" "uuid", "p_user_id" "uuid", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_team_reinvite_member"("p_team_id" "uuid", "p_user_id" "uuid", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_team_reinvite_member"("p_team_id" "uuid", "p_user_id" "uuid", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_team_reject_invitation"("p_team_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_team_reject_invitation"("p_team_id" "uuid", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_team_reject_invitation"("p_team_id" "uuid", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_team_reject_invitation"("p_team_id" "uuid", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_team_set_rank"("p_team_id" "uuid", "p_rank" integer, "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_team_set_rank"("p_team_id" "uuid", "p_rank" integer, "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_team_set_rank"("p_team_id" "uuid", "p_rank" integer, "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_team_set_rank"("p_team_id" "uuid", "p_rank" integer, "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_team_update_profile"("p_team_id" "uuid", "p_json" "jsonb", "p_is_public" boolean, "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_team_update_profile"("p_team_id" "uuid", "p_json" "jsonb", "p_is_public" boolean, "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_team_update_profile"("p_team_id" "uuid", "p_json" "jsonb", "p_is_public" boolean, "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_team_update_profile"("p_team_id" "uuid", "p_json" "jsonb", "p_is_public" boolean, "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_user_update_contact"("p_user_id" "uuid", "p_contact" "jsonb", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_user_update_contact"("p_user_id" "uuid", "p_contact" "jsonb", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_user_update_contact"("p_user_id" "uuid", "p_contact" "jsonb", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_user_update_contact"("p_user_id" "uuid", "p_contact" "jsonb", "p_audit" "jsonb") TO "service_role";



GRANT ALL ON FUNCTION "public"."contacts_sync_jsonb_version"() TO "anon";
GRANT ALL ON FUNCTION "public"."contacts_sync_jsonb_version"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."contacts_sync_jsonb_version"() TO "service_role";



REVOKE ALL ON FUNCTION "public"."delete_lifecycle_model_bundle"("p_model_id" "uuid", "p_version" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."delete_lifecycle_model_bundle"("p_model_id" "uuid", "p_version" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."delete_lifecycle_model_bundle"("p_model_id" "uuid", "p_version" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."delete_lifecycle_model_bundle"("p_model_id" "uuid", "p_version" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."flowproperties_sync_jsonb_version"() TO "anon";
GRANT ALL ON FUNCTION "public"."flowproperties_sync_jsonb_version"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."flowproperties_sync_jsonb_version"() TO "service_role";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."flows" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."flows" TO "authenticated";
GRANT ALL ON TABLE "public"."flows" TO "service_role";



GRANT ALL ON FUNCTION "public"."flows_embedding_ft_input"("proc" "public"."flows") TO "anon";
GRANT ALL ON FUNCTION "public"."flows_embedding_ft_input"("proc" "public"."flows") TO "authenticated";
GRANT ALL ON FUNCTION "public"."flows_embedding_ft_input"("proc" "public"."flows") TO "service_role";



GRANT ALL ON FUNCTION "public"."flows_embedding_input"("flow" "public"."flows") TO "anon";
GRANT ALL ON FUNCTION "public"."flows_embedding_input"("flow" "public"."flows") TO "authenticated";
GRANT ALL ON FUNCTION "public"."flows_embedding_input"("flow" "public"."flows") TO "service_role";



GRANT ALL ON FUNCTION "public"."flows_sync_jsonb_version"() TO "anon";
GRANT ALL ON FUNCTION "public"."flows_sync_jsonb_version"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."flows_sync_jsonb_version"() TO "service_role";



GRANT ALL ON FUNCTION "public"."generate_flow_embedding"() TO "anon";
GRANT ALL ON FUNCTION "public"."generate_flow_embedding"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."generate_flow_embedding"() TO "service_role";



GRANT ALL ON FUNCTION "public"."hybrid_search_flows"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."hybrid_search_flows"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."hybrid_search_flows"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."hybrid_search_lifecyclemodels"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."hybrid_search_lifecyclemodels"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."hybrid_search_lifecyclemodels"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."hybrid_search_processes"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."hybrid_search_processes"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."hybrid_search_processes"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."ilcd_classification_get"("this_file_name" "text", "category_type" "text", "get_values" "text"[]) TO "anon";
GRANT ALL ON FUNCTION "public"."ilcd_classification_get"("this_file_name" "text", "category_type" "text", "get_values" "text"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."ilcd_classification_get"("this_file_name" "text", "category_type" "text", "get_values" "text"[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."ilcd_flow_categorization_get"("this_file_name" "text", "get_values" "text"[]) TO "anon";
GRANT ALL ON FUNCTION "public"."ilcd_flow_categorization_get"("this_file_name" "text", "get_values" "text"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."ilcd_flow_categorization_get"("this_file_name" "text", "get_values" "text"[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."ilcd_location_get"("this_file_name" "text", "get_values" "text"[]) TO "anon";
GRANT ALL ON FUNCTION "public"."ilcd_location_get"("this_file_name" "text", "get_values" "text"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."ilcd_location_get"("this_file_name" "text", "get_values" "text"[]) TO "service_role";



REVOKE ALL ON FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."lca_package_enqueue_job"("p_message" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."lca_package_enqueue_job"("p_message" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."lca_package_enqueue_job"("p_message" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."lca_package_enqueue_job"("p_message" "jsonb") TO "service_role";



GRANT ALL ON FUNCTION "public"."lciamethods_sync_jsonb_version"() TO "anon";
GRANT ALL ON FUNCTION "public"."lciamethods_sync_jsonb_version"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."lciamethods_sync_jsonb_version"() TO "service_role";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."lifecyclemodels" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."lifecyclemodels" TO "authenticated";
GRANT ALL ON TABLE "public"."lifecyclemodels" TO "service_role";



GRANT ALL ON FUNCTION "public"."lifecyclemodels_embedding_ft_input"("proc" "public"."lifecyclemodels") TO "anon";
GRANT ALL ON FUNCTION "public"."lifecyclemodels_embedding_ft_input"("proc" "public"."lifecyclemodels") TO "authenticated";
GRANT ALL ON FUNCTION "public"."lifecyclemodels_embedding_ft_input"("proc" "public"."lifecyclemodels") TO "service_role";



GRANT ALL ON FUNCTION "public"."lifecyclemodels_embedding_input"("models" "public"."lifecyclemodels") TO "anon";
GRANT ALL ON FUNCTION "public"."lifecyclemodels_embedding_input"("models" "public"."lifecyclemodels") TO "authenticated";
GRANT ALL ON FUNCTION "public"."lifecyclemodels_embedding_input"("models" "public"."lifecyclemodels") TO "service_role";



GRANT ALL ON FUNCTION "public"."lifecyclemodels_sync_jsonb_version"() TO "anon";
GRANT ALL ON FUNCTION "public"."lifecyclemodels_sync_jsonb_version"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."lifecyclemodels_sync_jsonb_version"() TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search"("query_text" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search"("query_text" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search"("query_text" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_contacts"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_contacts"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_contacts"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_flowproperties"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flowproperties"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flowproperties"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_sources"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_sources"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_sources"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_unitgroups"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_unitgroups"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_unitgroups"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."policy_is_current_user_in_roles"("p_team_id" "uuid", "p_roles_to_check" "text"[]) TO "anon";
GRANT ALL ON FUNCTION "public"."policy_is_current_user_in_roles"("p_team_id" "uuid", "p_roles_to_check" "text"[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."policy_is_current_user_in_roles"("p_team_id" "uuid", "p_roles_to_check" "text"[]) TO "service_role";



GRANT ALL ON FUNCTION "public"."policy_is_team_id_used"("_team_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."policy_is_team_id_used"("_team_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."policy_is_team_id_used"("_team_id" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."policy_is_team_public"("_team_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."policy_is_team_public"("_team_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."policy_is_team_public"("_team_id" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."policy_review_can_read"("p_review_id" "uuid", "p_actor" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."policy_review_can_read"("p_review_id" "uuid", "p_actor" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."policy_review_can_read"("p_review_id" "uuid", "p_actor" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."policy_review_can_read"("p_review_id" "uuid", "p_actor" "uuid") TO "service_role";



GRANT ALL ON FUNCTION "public"."policy_roles_delete"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."policy_roles_delete"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."policy_roles_delete"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."policy_roles_insert"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."policy_roles_insert"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."policy_roles_insert"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."policy_roles_select"("_team_id" "uuid", "_role" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."policy_roles_select"("_team_id" "uuid", "_role" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."policy_roles_select"("_team_id" "uuid", "_role" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."policy_roles_update"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."policy_roles_update"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."policy_roles_update"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."policy_user_has_team"("_user_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."policy_user_has_team"("_user_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."policy_user_has_team"("_user_id" "uuid") TO "service_role";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."processes" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."processes" TO "authenticated";
GRANT ALL ON TABLE "public"."processes" TO "service_role";



GRANT ALL ON FUNCTION "public"."processes_embedding_ft_input"("proc" "public"."processes") TO "anon";
GRANT ALL ON FUNCTION "public"."processes_embedding_ft_input"("proc" "public"."processes") TO "authenticated";
GRANT ALL ON FUNCTION "public"."processes_embedding_ft_input"("proc" "public"."processes") TO "service_role";



GRANT ALL ON FUNCTION "public"."processes_embedding_input"("proc" "public"."processes") TO "anon";
GRANT ALL ON FUNCTION "public"."processes_embedding_input"("proc" "public"."processes") TO "authenticated";
GRANT ALL ON FUNCTION "public"."processes_embedding_input"("proc" "public"."processes") TO "service_role";



GRANT ALL ON FUNCTION "public"."processes_sync_jsonb_version"() TO "anon";
GRANT ALL ON FUNCTION "public"."processes_sync_jsonb_version"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."processes_sync_jsonb_version"() TO "service_role";



REVOKE ALL ON FUNCTION "public"."qry_notification_get_my_data_count"("p_days" integer, "p_last_view_at" timestamp with time zone) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_data_count"("p_days" integer, "p_last_view_at" timestamp with time zone) TO "anon";
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_data_count"("p_days" integer, "p_last_view_at" timestamp with time zone) TO "authenticated";
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_data_count"("p_days" integer, "p_last_view_at" timestamp with time zone) TO "service_role";



REVOKE ALL ON FUNCTION "public"."qry_notification_get_my_data_items"("p_page" integer, "p_page_size" integer, "p_days" integer) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_data_items"("p_page" integer, "p_page_size" integer, "p_days" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_data_items"("p_page" integer, "p_page_size" integer, "p_days" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_data_items"("p_page" integer, "p_page_size" integer, "p_days" integer) TO "service_role";



REVOKE ALL ON FUNCTION "public"."qry_notification_get_my_issue_count"("p_days" integer, "p_last_view_at" timestamp with time zone) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_issue_count"("p_days" integer, "p_last_view_at" timestamp with time zone) TO "anon";
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_issue_count"("p_days" integer, "p_last_view_at" timestamp with time zone) TO "authenticated";
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_issue_count"("p_days" integer, "p_last_view_at" timestamp with time zone) TO "service_role";



REVOKE ALL ON FUNCTION "public"."qry_notification_get_my_issue_items"("p_page" integer, "p_page_size" integer, "p_days" integer) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_issue_items"("p_page" integer, "p_page_size" integer, "p_days" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_issue_items"("p_page" integer, "p_page_size" integer, "p_days" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_issue_items"("p_page" integer, "p_page_size" integer, "p_days" integer) TO "service_role";



REVOKE ALL ON FUNCTION "public"."qry_notification_get_my_team_count"("p_days" integer, "p_last_view_at" timestamp with time zone) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_team_count"("p_days" integer, "p_last_view_at" timestamp with time zone) TO "anon";
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_team_count"("p_days" integer, "p_last_view_at" timestamp with time zone) TO "authenticated";
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_team_count"("p_days" integer, "p_last_view_at" timestamp with time zone) TO "service_role";



REVOKE ALL ON FUNCTION "public"."qry_notification_get_my_team_items"("p_days" integer) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_team_items"("p_days" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_team_items"("p_days" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."qry_notification_get_my_team_items"("p_days" integer) TO "service_role";



REVOKE ALL ON FUNCTION "public"."qry_review_get_admin_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."qry_review_get_admin_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."qry_review_get_admin_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."qry_review_get_admin_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."qry_review_get_comment_items"("p_review_id" "uuid", "p_scope" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."qry_review_get_comment_items"("p_review_id" "uuid", "p_scope" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."qry_review_get_comment_items"("p_review_id" "uuid", "p_scope" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."qry_review_get_comment_items"("p_review_id" "uuid", "p_scope" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."qry_review_get_items"("p_review_ids" "uuid"[], "p_data_id" "uuid", "p_data_version" "text", "p_state_codes" integer[]) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."qry_review_get_items"("p_review_ids" "uuid"[], "p_data_id" "uuid", "p_data_version" "text", "p_state_codes" integer[]) TO "anon";
GRANT ALL ON FUNCTION "public"."qry_review_get_items"("p_review_ids" "uuid"[], "p_data_id" "uuid", "p_data_version" "text", "p_state_codes" integer[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."qry_review_get_items"("p_review_ids" "uuid"[], "p_data_id" "uuid", "p_data_version" "text", "p_state_codes" integer[]) TO "service_role";



REVOKE ALL ON FUNCTION "public"."qry_review_get_member_list"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_role" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."qry_review_get_member_list"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_role" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."qry_review_get_member_list"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_role" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."qry_review_get_member_list"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_role" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."qry_review_get_member_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."qry_review_get_member_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."qry_review_get_member_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."qry_review_get_member_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."qry_review_get_member_workload"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_role" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."qry_review_get_member_workload"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_role" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."qry_review_get_member_workload"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_role" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."qry_review_get_member_workload"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_role" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."qry_system_get_member_list"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."qry_system_get_member_list"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."qry_system_get_member_list"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."qry_system_get_member_list"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."qry_team_get_member_list"("p_team_id" "uuid", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."qry_team_get_member_list"("p_team_id" "uuid", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."qry_team_get_member_list"("p_team_id" "uuid", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."qry_team_get_member_list"("p_team_id" "uuid", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."save_lifecycle_model_bundle"("p_plan" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."save_lifecycle_model_bundle"("p_plan" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."save_lifecycle_model_bundle"("p_plan" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."save_lifecycle_model_bundle"("p_plan" "jsonb") TO "service_role";



GRANT ALL ON FUNCTION "public"."semantic_search"("query_embedding" "text", "match_threshold" double precision, "match_count" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."semantic_search"("query_embedding" "text", "match_threshold" double precision, "match_count" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."semantic_search"("query_embedding" "text", "match_threshold" double precision, "match_count" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."semantic_search_flows_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."semantic_search_flows_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."semantic_search_flows_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."semantic_search_lifecyclemodels_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."semantic_search_lifecyclemodels_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."semantic_search_lifecyclemodels_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."semantic_search_processes_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."semantic_search_processes_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."semantic_search_processes_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."sources_sync_jsonb_version"() TO "anon";
GRANT ALL ON FUNCTION "public"."sources_sync_jsonb_version"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."sources_sync_jsonb_version"() TO "service_role";



GRANT ALL ON FUNCTION "public"."sync_auth_users_to_public_users"() TO "anon";
GRANT ALL ON FUNCTION "public"."sync_auth_users_to_public_users"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."sync_auth_users_to_public_users"() TO "service_role";



GRANT ALL ON FUNCTION "public"."sync_json_to_jsonb"() TO "anon";
GRANT ALL ON FUNCTION "public"."sync_json_to_jsonb"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."sync_json_to_jsonb"() TO "service_role";



GRANT ALL ON FUNCTION "public"."unitgroups_sync_jsonb_version"() TO "anon";
GRANT ALL ON FUNCTION "public"."unitgroups_sync_jsonb_version"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."unitgroups_sync_jsonb_version"() TO "service_role";



GRANT ALL ON FUNCTION "public"."update_modified_at"() TO "anon";
GRANT ALL ON FUNCTION "public"."update_modified_at"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_modified_at"() TO "service_role";



GRANT ALL ON TABLE "public"."command_audit_log" TO "service_role";



GRANT ALL ON SEQUENCE "public"."command_audit_log_id_seq" TO "service_role";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."comments" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."comments" TO "authenticated";
GRANT ALL ON TABLE "public"."comments" TO "service_role";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."contacts" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."contacts" TO "authenticated";
GRANT ALL ON TABLE "public"."contacts" TO "service_role";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."flowproperties" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."flowproperties" TO "authenticated";
GRANT ALL ON TABLE "public"."flowproperties" TO "service_role";



GRANT ALL ON TABLE "public"."ilcd" TO "anon";
GRANT ALL ON TABLE "public"."ilcd" TO "authenticated";
GRANT ALL ON TABLE "public"."ilcd" TO "service_role";



GRANT ALL ON TABLE "public"."lca_active_snapshots" TO "anon";
GRANT ALL ON TABLE "public"."lca_active_snapshots" TO "authenticated";
GRANT ALL ON TABLE "public"."lca_active_snapshots" TO "service_role";



GRANT ALL ON TABLE "public"."lca_factorization_registry" TO "anon";
GRANT ALL ON TABLE "public"."lca_factorization_registry" TO "authenticated";
GRANT ALL ON TABLE "public"."lca_factorization_registry" TO "service_role";



GRANT ALL ON TABLE "public"."lca_jobs" TO "anon";
GRANT ALL ON TABLE "public"."lca_jobs" TO "authenticated";
GRANT ALL ON TABLE "public"."lca_jobs" TO "service_role";



GRANT ALL ON TABLE "public"."lca_latest_all_unit_results" TO "anon";
GRANT ALL ON TABLE "public"."lca_latest_all_unit_results" TO "authenticated";
GRANT ALL ON TABLE "public"."lca_latest_all_unit_results" TO "service_role";



GRANT ALL ON TABLE "public"."lca_network_snapshots" TO "anon";
GRANT ALL ON TABLE "public"."lca_network_snapshots" TO "authenticated";
GRANT ALL ON TABLE "public"."lca_network_snapshots" TO "service_role";



GRANT ALL ON TABLE "public"."lca_package_artifacts" TO "anon";
GRANT ALL ON TABLE "public"."lca_package_artifacts" TO "authenticated";
GRANT ALL ON TABLE "public"."lca_package_artifacts" TO "service_role";



GRANT ALL ON TABLE "public"."lca_package_export_items" TO "anon";
GRANT ALL ON TABLE "public"."lca_package_export_items" TO "authenticated";
GRANT ALL ON TABLE "public"."lca_package_export_items" TO "service_role";



GRANT ALL ON TABLE "public"."lca_package_jobs" TO "anon";
GRANT ALL ON TABLE "public"."lca_package_jobs" TO "authenticated";
GRANT ALL ON TABLE "public"."lca_package_jobs" TO "service_role";



GRANT ALL ON TABLE "public"."lca_package_request_cache" TO "anon";
GRANT ALL ON TABLE "public"."lca_package_request_cache" TO "authenticated";
GRANT ALL ON TABLE "public"."lca_package_request_cache" TO "service_role";



GRANT ALL ON TABLE "public"."lca_result_cache" TO "anon";
GRANT ALL ON TABLE "public"."lca_result_cache" TO "authenticated";
GRANT ALL ON TABLE "public"."lca_result_cache" TO "service_role";



GRANT ALL ON TABLE "public"."lca_results" TO "anon";
GRANT ALL ON TABLE "public"."lca_results" TO "authenticated";
GRANT ALL ON TABLE "public"."lca_results" TO "service_role";



GRANT ALL ON TABLE "public"."lca_snapshot_artifacts" TO "anon";
GRANT ALL ON TABLE "public"."lca_snapshot_artifacts" TO "authenticated";
GRANT ALL ON TABLE "public"."lca_snapshot_artifacts" TO "service_role";



GRANT ALL ON TABLE "public"."lciamethods" TO "anon";
GRANT ALL ON TABLE "public"."lciamethods" TO "authenticated";
GRANT ALL ON TABLE "public"."lciamethods" TO "service_role";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."notifications" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."notifications" TO "authenticated";
GRANT ALL ON TABLE "public"."notifications" TO "service_role";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."reviews" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."reviews" TO "authenticated";
GRANT ALL ON TABLE "public"."reviews" TO "service_role";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."roles" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."roles" TO "authenticated";
GRANT ALL ON TABLE "public"."roles" TO "service_role";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."sources" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."sources" TO "authenticated";
GRANT ALL ON TABLE "public"."sources" TO "service_role";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."teams" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."teams" TO "authenticated";
GRANT ALL ON TABLE "public"."teams" TO "service_role";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."unitgroups" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."unitgroups" TO "authenticated";
GRANT ALL ON TABLE "public"."unitgroups" TO "service_role";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."users" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."users" TO "authenticated";
GRANT ALL ON TABLE "public"."users" TO "service_role";



ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON SEQUENCES TO "service_role";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON FUNCTIONS TO "service_role";






ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "postgres";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "anon";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "authenticated";
ALTER DEFAULT PRIVILEGES FOR ROLE "postgres" IN SCHEMA "public" GRANT ALL ON TABLES TO "service_role";







