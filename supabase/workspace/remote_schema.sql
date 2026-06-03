


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


CREATE OR REPLACE FUNCTION "public"."_search_simple_dataset_latest"("p_table" "regclass", "query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $_$
declare
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_this_user_id uuid;
  exact_query_id uuid;
  filter_condition_jsonb jsonb;
  json_filter_clause text;
  v_sql text;
begin
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_this_user_id := case
    when coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      then btrim(this_user_id)::uuid
    else null::uuid
  end;
  exact_query_id := case
    when coalesce(btrim(query_text) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      then btrim(query_text)::uuid
    else null::uuid
  end;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);
  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and d.json @> $8'
  end;

  if exact_query_id is not null then
    v_sql := format($sql$
      with matched_ids as (
        select d.id, 1.0::double precision as search_score
        from %1$s d
        where d.id = $1
          and (
            ($4 = 'tg' and d.state_code = 100 and ($6 is null or d.team_id = $6))
            or ($4 = 'co' and d.state_code = 200 and ($6 is null or d.team_id = $6))
            or ($4 = 'my' and $5 is not null and d.user_id = $5 and ($7 is null or d.state_code = $7))
            or ($4 = 'te' and $6 is not null and d.team_id = $6 and ($7 is null or d.state_code = $7))
          )
          %2$s
        group by d.id
      ),
      latest_rows as (
        select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        from matched_ids
        join lateral (
          select d2.json, d2.version, d2.modified_at, d2.team_id
          from %1$s d2
          where d2.id = matched_ids.id
            and (
              ($4 = 'tg' and d2.state_code = 100 and ($6 is null or d2.team_id = $6))
              or ($4 = 'co' and d2.state_code = 200 and ($6 is null or d2.team_id = $6))
              or ($4 = 'my' and $5 is not null and d2.user_id = $5 and ($7 is null or d2.state_code = $7))
              or ($4 = 'te' and $6 is not null and d2.team_id = $6 and ($7 is null or d2.state_code = $7))
            )
          order by d2.version desc, d2.modified_at desc
          limit 1
        ) latest_row on true
      ),
      counted_rows as (
        select latest_rows.*, count(*) over()::bigint as total_count
        from latest_rows
      )
      select 1::bigint as rank, counted_rows.id, counted_rows.json, counted_rows.version, counted_rows.modified_at, counted_rows.team_id, counted_rows.total_count
      from counted_rows
      order by rank, counted_rows.id
      limit $2
      offset ($3 - 1) * $2
    $sql$, p_table, json_filter_clause);

    return query execute v_sql
      using exact_query_id, normalized_page_size, normalized_page_current,
            data_source, normalized_this_user_id, team_id_filter, state_code_filter,
            filter_condition_jsonb;
    return;
  end if;

  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and d.json @> $2'
  end;

  v_sql := format($sql$
    with text_matches as materialized (
      select d.id,
             d.json,
             d.state_code,
             d.team_id,
             d.user_id,
             pgroonga_score(d.tableoid, d.ctid) as search_score
      from %1$s d
      where d.extracted_text &@~ $1
    ),
    matched_ids as (
      select d.id, max(d.search_score) as search_score
      from text_matches d
      where (
          ($5 = 'tg' and d.state_code = 100 and ($7 is null or d.team_id = $7))
          or ($5 = 'co' and d.state_code = 200 and ($7 is null or d.team_id = $7))
          or ($5 = 'my' and $6 is not null and d.user_id = $6 and ($8 is null or d.state_code = $8))
          or ($5 = 'te' and $7 is not null and d.team_id = $7 and ($8 is null or d.state_code = $8))
        )
        %2$s
      group by d.id
    ),
    latest_rows as (
      select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
      from matched_ids
      join lateral (
        select d2.json, d2.version, d2.modified_at, d2.team_id
        from %1$s d2
        where d2.id = matched_ids.id
          and (
            ($5 = 'tg' and d2.state_code = 100 and ($7 is null or d2.team_id = $7))
            or ($5 = 'co' and d2.state_code = 200 and ($7 is null or d2.team_id = $7))
            or ($5 = 'my' and $6 is not null and d2.user_id = $6 and ($8 is null or d2.state_code = $8))
            or ($5 = 'te' and $7 is not null and d2.team_id = $7 and ($8 is null or d2.state_code = $8))
          )
        order by d2.version desc, d2.modified_at desc
        limit 1
      ) latest_row on true
    ),
    counted_rows as (
      select latest_rows.*, count(*) over()::bigint as total_count
      from latest_rows
    ),
    ranked_rows as (
      select rank() over (order by counted_rows.search_score desc, counted_rows.modified_at desc, counted_rows.id)::bigint as rank,
             counted_rows.*
      from counted_rows
    )
    select ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
    from ranked_rows
    order by ranked_rows.rank, ranked_rows.id
    limit $3
    offset ($4 - 1) * $3
  $sql$, p_table, json_filter_clause);

  return query execute v_sql
    using query_text, filter_condition_jsonb, normalized_page_size, normalized_page_current,
          data_source, normalized_this_user_id, team_id_filter, state_code_filter;
end;
$_$;


ALTER FUNCTION "public"."_search_simple_dataset_latest"("p_table" "regclass", "query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_assert_review_submit_gate_passed"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_gate_run_id" "uuid" DEFAULT NULL::"uuid", "p_revision_checksum" "text" DEFAULT NULL::"text", "p_policy_profile" "text" DEFAULT 'review_submit_fast.v1'::"text", "p_report_schema_version" "text" DEFAULT 'review_submit_gate_report.v1'::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_dataset_row jsonb;
  v_owner_id uuid;
  v_run public.dataset_review_submit_gate_runs%rowtype;
  v_worker_job public.worker_jobs%rowtype;
  v_payload jsonb;
  v_result_checksum text;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table <> 'processes' then
    return jsonb_build_object('ok', true);
  end if;

  v_dataset_row := public.cmd_review_get_dataset_row(p_table, p_id, p_version, false);

  if v_dataset_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  v_owner_id := nullif(v_dataset_row->>'user_id', '')::uuid;

  if v_owner_id is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the dataset owner can submit review'
    );
  end if;

  if p_gate_run_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_REQUIRED',
      'status', 400,
      'message', 'A passed review-submit gate run is required before process review submission'
    );
  end if;

  if coalesce(p_revision_checksum, '') !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVISION_CHECKSUM_REQUIRED',
      'status', 400,
      'message', 'revisionChecksum must be a lowercase SHA-256 hex digest'
    );
  end if;

  select *
    into v_run
  from public.dataset_review_submit_gate_runs
  where id = p_gate_run_id;

  if v_run.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit gate run not found'
    );
  end if;

  v_payload := public.cmd_dataset_review_submit_gate_payload(v_run);

  if v_run.dataset_table <> p_table
    or v_run.dataset_id <> p_id
    or v_run.dataset_version <> p_version then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
      'status', 409,
      'message', 'Review-submit gate run belongs to a different dataset revision'
    );
  end if;

  if v_run.revision_checksum <> p_revision_checksum then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_STALE',
      'status', 409,
      'message', 'Review-submit gate run is stale for the submitted dataset revision',
      'details', public.cmd_dataset_review_submit_gate_payload(v_run, 'stale')
    );
  end if;

  if v_run.policy_profile <> coalesce(p_policy_profile, '') then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_POLICY_MISMATCH',
      'status', 409,
      'message', 'Review-submit gate run used a different policy profile',
      'details', jsonb_build_object(
        'expected', p_policy_profile,
        'actual', v_run.policy_profile
      )
    );
  end if;

  if v_run.report_schema_version <> coalesce(p_report_schema_version, '') then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_SCHEMA_MISMATCH',
      'status', 409,
      'message', 'Review-submit gate run used a different report schema version',
      'details', jsonb_build_object(
        'expected', p_report_schema_version,
        'actual', v_run.report_schema_version
      )
    );
  end if;

  if v_run.worker_job_id is not null then
    select *
      into v_worker_job
    from public.worker_jobs
    where id = v_run.worker_job_id;

    if v_worker_job.id is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate worker job not found',
        'details', v_payload
      );
    end if;

    if v_worker_job.job_kind <> 'review_submit.gate'
      or v_worker_job.subject_type <> v_run.dataset_table
      or v_worker_job.subject_id <> v_run.dataset_id
      or v_worker_job.subject_version <> v_run.dataset_version
      or v_worker_job.requested_by is distinct from v_run.requested_by
      or v_worker_job.payload_json #>> '{datasetRevision,revisionChecksum}' is distinct from v_run.revision_checksum
      or v_worker_job.payload_json #>> '{policy,profile}' is distinct from v_run.policy_profile
      or v_worker_job.payload_json #>> '{policy,reportSchemaVersion}' is distinct from v_run.report_schema_version then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
        'status', 409,
        'message', 'Review-submit gate worker job does not match this gate run',
        'details', v_payload
      );
    end if;

    if v_worker_job.status in ('queued', 'running', 'waiting', 'stale') then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_READY',
        'status', 409,
        'message', 'Review-submit gate has not passed yet',
        'details', v_payload
      );
    end if;

    if v_worker_job.status = 'blocked' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_BLOCKED',
        'status', 409,
        'message', 'Review-submit gate blocked this dataset revision',
        'details', v_payload
      );
    end if;

    if v_worker_job.status <> 'completed' then
      return jsonb_build_object(
        'ok', false,
        'code', coalesce(v_worker_job.error_code, 'REVIEW_SUBMIT_GATE_ERROR'),
        'status', 502,
        'message', coalesce(v_worker_job.error_message, 'Review-submit gate failed before review submission'),
        'details', v_payload
      );
    end if;

    if coalesce(v_worker_job.result_json->>'status', '') <> 'passed' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_ERROR',
        'status', 502,
        'message', 'Review-submit gate worker job completed without a passed result',
        'details', v_payload
      );
    end if;

    v_result_checksum := v_worker_job.result_json #>> '{datasetRevision,revisionChecksum}';
    if v_result_checksum is not null and v_result_checksum <> p_revision_checksum then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_STALE',
        'status', 409,
        'message', 'Review-submit gate worker result is stale for the submitted dataset revision',
        'details', public.cmd_dataset_review_submit_gate_payload(v_run, 'stale')
      );
    end if;

    return jsonb_build_object(
      'ok', true,
      'data', v_payload
    );
  end if;

  case v_run.status
    when 'passed' then
      return jsonb_build_object(
        'ok', true,
        'data', v_payload
      );
    when 'blocked' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_BLOCKED',
        'status', 409,
        'message', 'Review-submit gate blocked this dataset revision',
        'details', v_payload
      );
    when 'queued', 'running' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_READY',
        'status', 409,
        'message', 'Review-submit gate has not passed yet',
        'details', v_payload
      );
    when 'error' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_ERROR',
        'status', 502,
        'message', 'Review-submit gate failed before review submission',
        'details', v_payload
      );
    else
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_STALE',
        'status', 409,
        'message', 'Review-submit gate run is stale for the submitted dataset revision',
        'details', public.cmd_dataset_review_submit_gate_payload(v_run, 'stale')
      );
  end case;
end;
$_$;


ALTER FUNCTION "public"."cmd_dataset_assert_review_submit_gate_passed"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_gate_run_id" "uuid", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text") OWNER TO "postgres";


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

  if p_table = 'flows' then
    perform set_config('lock_timeout', '2s', true);
    perform set_config('statement_timeout', '8s', true);
  end if;

  begin
    if p_table = 'processes' then
      execute format(
        'insert into public.%I as t (id, json_ordered, model_id, rule_verification)
         values ($1, $2::json, $3, $4)
         returning jsonb_build_object(
           ''id'', t.id,
           ''version'', t.version,
           ''state_code'', t.state_code,
           ''user_id'', t.user_id,
           ''team_id'', t.team_id,
           ''model_id'', t.model_id,
           ''rule_verification'', t.rule_verification
         )',
        p_table
      )
        into v_created_row
        using p_id, p_json_ordered, p_model_id, p_rule_verification;
    else
      execute format(
        'insert into public.%I as t (id, json_ordered, rule_verification)
         values ($1, $2::json, $3)
         returning jsonb_build_object(
           ''id'', t.id,
           ''version'', t.version,
           ''state_code'', t.state_code,
           ''user_id'', t.user_id,
           ''team_id'', t.team_id,
           ''model_id'', null,
           ''rule_verification'', t.rule_verification
         )',
        p_table
      )
        into v_created_row
        using p_id, p_json_ordered, p_rule_verification;
    end if;
  exception
    when lock_not_available then
      return jsonb_build_object(
        'ok', false,
        'code', 'DATASET_CREATE_LOCK_TIMEOUT',
        'status', 503,
        'message', 'Dataset creation is temporarily blocked by concurrent database work'
      );
    when query_canceled then
      return jsonb_build_object(
        'ok', false,
        'code', 'DATASET_CREATE_TIMEOUT',
        'status', 503,
        'message', 'Dataset creation exceeded the database timeout'
      );
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


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_extracted_text_backfill"("p_table" "text", "p_batch_size" integer DEFAULT 1000, "p_after_id" "uuid" DEFAULT NULL::"uuid", "p_after_version" "text" DEFAULT NULL::"text", "p_mode" "text" DEFAULT 'empty'::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_table text := lower(btrim(coalesce(p_table, '')));
  v_mode text := lower(btrim(coalesce(p_mode, 'empty')));
  v_batch_size integer := least(greatest(coalesce(p_batch_size, 1000), 1), 5000);
  v_scanned_count integer := 0;
  v_updated_count integer := 0;
  v_last_id uuid;
  v_last_version text;
begin
  if v_table not in (
    'flows',
    'processes',
    'lifecyclemodels',
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'UNSUPPORTED_DATASET_TABLE',
      'message', format('Unsupported dataset table: %s', coalesce(p_table, '<null>'))
    );
  end if;

  if v_mode not in ('empty', 'stale', 'noisy') then
    return jsonb_build_object(
      'ok', false,
      'code', 'UNSUPPORTED_BACKFILL_MODE',
      'message', format('Unsupported extracted_text backfill mode: %s', coalesce(p_mode, '<null>'))
    );
  end if;

  execute format(
    $sql$
      with page as (
        select id, version
          from public.%1$I
         where json is not null
           and ($4 <> 'empty' or coalesce(extracted_text, '') = '')
           and (
             $4 <> 'noisy'
             or extracted_text like '%%../%%'
             or extracted_text like '%%schemas/%%'
           )
           and ($1 is null or (id, version) > ($1, coalesce($2, '')::character(9)))
         order by id, version
         limit $3
         for update skip locked
      ),
      computed as (
        select dataset.id,
               dataset.version,
               util.dataset_json_search_text($5, dataset.json) as next_extracted_text
          from public.%1$I as dataset
          join page on page.id = dataset.id
                   and page.version = dataset.version
      ),
      updated as (
        update public.%1$I as dataset
           set extracted_text = computed.next_extracted_text
          from computed
         where dataset.id = computed.id
           and dataset.version = computed.version
         returning 1
      )
      select
        (select count(*)::integer from page),
        (select count(*)::integer from updated),
        (select id from page order by id desc, version desc limit 1),
        (select version::text from page order by id desc, version desc limit 1)
    $sql$,
    v_table
  )
  using p_after_id, p_after_version, v_batch_size, v_mode, v_table
  into v_scanned_count, v_updated_count, v_last_id, v_last_version;

  return jsonb_build_object(
    'ok', true,
    'table', v_table,
    'mode', v_mode,
    'scanned_count', v_scanned_count,
    'updated_count', v_updated_count,
    'last_id', v_last_id,
    'last_version', v_last_version,
    'has_more', v_scanned_count = v_batch_size
  );
end;
$_$;


ALTER FUNCTION "public"."cmd_dataset_extracted_text_backfill"("p_table" "text", "p_batch_size" integer, "p_after_id" "uuid", "p_after_version" "text", "p_mode" "text") OWNER TO "postgres";


COMMENT ON FUNCTION "public"."cmd_dataset_extracted_text_backfill"("p_table" "text", "p_batch_size" integer, "p_after_id" "uuid", "p_after_version" "text", "p_mode" "text") IS 'Service-role RPC for bounded historical extracted_text backfill. Modes: empty repairs empty rows, noisy rewrites rows with schema/path metadata noise, stale rewrites every selected row.';



CREATE OR REPLACE FUNCTION "public"."cmd_dataset_extraction_ack"("p_msg_ids" bigint[]) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_deleted jsonb := '[]'::jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required'
    );
  end if;

  if coalesce(array_length(p_msg_ids, 1), 0) = 0 then
    return jsonb_build_object('ok', true, 'data', jsonb_build_object('deleted_msg_ids', v_deleted));
  end if;

  select coalesce(jsonb_agg(deleted_msg_id order by deleted_msg_id), '[]'::jsonb)
  into v_deleted
  from pgmq.delete('dataset_extraction_jobs', p_msg_ids) as deleted_msg_id;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object('deleted_msg_ids', v_deleted)
  );
end;
$$;


ALTER FUNCTION "public"."cmd_dataset_extraction_ack"("p_msg_ids" bigint[]) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_extraction_claim"("p_qty" integer DEFAULT 10, "p_vt_seconds" integer DEFAULT 300, "p_max_read_count" integer DEFAULT 5) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_qty integer;
  v_vt_seconds integer;
  v_max_read_count integer;
  v_jobs jsonb := '[]'::jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required'
    );
  end if;

  v_qty := least(greatest(coalesce(p_qty, 10), 1), 50);
  v_vt_seconds := least(greatest(coalesce(p_vt_seconds, 300), 1), 3600);
  v_max_read_count := least(greatest(coalesce(p_max_read_count, 5), 1), 100);

  with expired_jobs as (
    select
      q.msg_id,
      q.message,
      q.read_ct
    from pgmq.q_dataset_extraction_jobs q
    where q.vt <= clock_timestamp()
      and q.read_ct >= v_max_read_count
    order by q.msg_id
    limit greatest(v_qty, 100)
  ),
  recorded_failures as (
    insert into util.dataset_extraction_job_failures (
      queue_name,
      msg_id,
      read_count,
      reason,
      message
    )
    select
      'dataset_extraction_jobs',
      e.msg_id,
      e.read_ct,
      format('read_ct reached retry cap %s', v_max_read_count),
      e.message
    from expired_jobs e
    on conflict (queue_name, msg_id) do update
    set
      read_count = excluded.read_count,
      reason = excluded.reason,
      message = excluded.message,
      created_at = now()
    returning msg_id
  )
  delete from pgmq.q_dataset_extraction_jobs q
  using recorded_failures f
  where q.msg_id = f.msg_id;

  with claimed_jobs as (
    select *
    from pgmq.read('dataset_extraction_jobs', v_vt_seconds, v_qty)
  )
  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'msg_id', msg_id,
        'read_ct', read_ct,
        'enqueued_at', enqueued_at,
        'vt', vt,
        'message', message
      )
      order by msg_id
    ),
    '[]'::jsonb
  )
  into v_jobs
  from claimed_jobs;

  return jsonb_build_object(
    'ok', true,
    'data', v_jobs
  );
end;
$$;


ALTER FUNCTION "public"."cmd_dataset_extraction_claim"("p_qty" integer, "p_vt_seconds" integer, "p_max_read_count" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_extraction_record_failure"("p_msg_id" bigint, "p_read_count" integer, "p_reason" "text", "p_message" "jsonb", "p_last_error" "text" DEFAULT NULL::"text", "p_delete" boolean DEFAULT true) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required'
    );
  end if;

  insert into util.dataset_extraction_job_failures (
    queue_name,
    msg_id,
    read_count,
    reason,
    message,
    last_error
  )
  values (
    'dataset_extraction_jobs',
    p_msg_id,
    coalesce(p_read_count, 0),
    coalesce(nullif(p_reason, ''), 'worker failure'),
    coalesce(p_message, '{}'::jsonb),
    p_last_error
  )
  on conflict (queue_name, msg_id) do update
  set
    read_count = excluded.read_count,
    reason = excluded.reason,
    message = excluded.message,
    last_error = excluded.last_error,
    created_at = now();

  if coalesce(p_delete, true) then
    perform pgmq.delete('dataset_extraction_jobs', p_msg_id);
  end if;

  return jsonb_build_object('ok', true);
end;
$$;


ALTER FUNCTION "public"."cmd_dataset_extraction_record_failure"("p_msg_id" bigint, "p_read_count" integer, "p_reason" "text", "p_message" "jsonb", "p_last_error" "text", "p_delete" boolean) OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text" DEFAULT 'review_submit_fast.v1'::"text", "p_report_schema_version" "text" DEFAULT 'review_submit_gate_report.v1'::"text", "p_action" "text" DEFAULT 'ensure'::"text", "p_gate_run_id" "uuid" DEFAULT NULL::"uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_dataset_row jsonb;
  v_owner_id uuid;
  v_action text := lower(trim(coalesce(p_action, 'ensure')));
  v_run public.dataset_review_submit_gate_runs%rowtype;
  v_existing public.dataset_review_submit_gate_runs%rowtype;
  v_supersedes uuid;
  v_link_result jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table not in ('processes', 'lifecyclemodels') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Unsupported dataset table for review-submit gate'
    );
  end if;

  if coalesce(p_revision_checksum, '') !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVISION_CHECKSUM_REQUIRED',
      'status', 400,
      'message', 'revisionChecksum must be a lowercase SHA-256 hex digest'
    );
  end if;

  if coalesce(p_policy_profile, '') <> 'review_submit_fast.v1' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_POLICY_UNSUPPORTED',
      'status', 400,
      'message', 'Unsupported review-submit gate policy profile',
      'details', jsonb_build_object('policy_profile', p_policy_profile)
    );
  end if;

  if coalesce(p_report_schema_version, '') <> 'review_submit_gate_report.v1' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_SCHEMA_UNSUPPORTED',
      'status', 400,
      'message', 'Unsupported review-submit gate report schema version',
      'details', jsonb_build_object('report_schema_version', p_report_schema_version)
    );
  end if;

  if v_action not in ('ensure', 'read', 'rerun') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_ACTION',
      'status', 400,
      'message', 'action must be ensure, read, or rerun'
    );
  end if;

  v_dataset_row := public.cmd_review_get_dataset_row(p_table, p_id, p_version, false);

  if v_dataset_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  v_owner_id := nullif(v_dataset_row->>'user_id', '')::uuid;

  if v_owner_id is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the dataset owner can run or read the review-submit gate'
    );
  end if;

  if p_gate_run_id is not null then
    select *
      into v_existing
    from public.dataset_review_submit_gate_runs
    where id = p_gate_run_id
    for update;

    if v_existing.id is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate run not found'
      );
    end if;

    if v_existing.dataset_table <> p_table
      or v_existing.dataset_id <> p_id
      or v_existing.dataset_version <> p_version then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
        'status', 409,
        'message', 'Review-submit gate run belongs to a different dataset revision'
      );
    end if;

    if v_action = 'read' then
      return jsonb_build_object(
        'ok', true,
        'data', public.cmd_dataset_review_submit_gate_payload(
          v_existing,
          case
            when v_existing.revision_checksum <> p_revision_checksum then 'stale'
            else null
          end
        )
      );
    end if;

    if v_action = 'ensure' and v_existing.revision_checksum = p_revision_checksum then
      return public.cmd_dataset_review_submit_gate_link_worker_job(v_existing.id, 'ensure');
    end if;

    v_supersedes := v_existing.id;
  end if;

  perform pg_advisory_xact_lock(
    hashtextextended(
      concat_ws(
        ':',
        'review_submit_gate',
        p_table,
        p_id::text,
        p_version,
        p_revision_checksum,
        p_policy_profile,
        p_report_schema_version,
        v_actor::text
      ),
      0
    )
  );

  if v_action in ('ensure', 'read') and p_gate_run_id is null then
    select *
      into v_run
    from public.dataset_review_submit_gate_runs
    where dataset_table = p_table
      and dataset_id = p_id
      and dataset_version = p_version
      and revision_checksum = p_revision_checksum
      and policy_profile = p_policy_profile
      and report_schema_version = p_report_schema_version
      and requested_by = v_actor
    order by created_at desc
    limit 1
    for update;

    if v_run.id is not null then
      if v_action = 'ensure' then
        return public.cmd_dataset_review_submit_gate_link_worker_job(v_run.id, 'ensure');
      end if;

      return jsonb_build_object(
        'ok', true,
        'data', public.cmd_dataset_review_submit_gate_payload(v_run)
      );
    end if;

    if v_action = 'read' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate run not found'
      );
    end if;
  end if;

  if v_action = 'rerun' and p_gate_run_id is null then
    select id
      into v_supersedes
    from public.dataset_review_submit_gate_runs
    where dataset_table = p_table
      and dataset_id = p_id
      and dataset_version = p_version
      and policy_profile = p_policy_profile
      and report_schema_version = p_report_schema_version
      and requested_by = v_actor
    order by created_at desc
    limit 1;
  end if;

  insert into public.dataset_review_submit_gate_runs (
    dataset_table,
    dataset_id,
    dataset_version,
    revision_checksum,
    policy_profile,
    report_schema_version,
    status,
    requested_by,
    supersedes_gate_run_id
  )
  values (
    p_table,
    p_id,
    p_version,
    p_revision_checksum,
    p_policy_profile,
    p_report_schema_version,
    'queued',
    v_actor,
    v_supersedes
  )
  returning *
    into v_run;

  v_link_result := public.cmd_dataset_review_submit_gate_link_worker_job(v_run.id, v_action);

  if coalesce((v_link_result->>'ok')::boolean, false) is false then
    return v_link_result;
  end if;

  select *
    into v_run
  from public.dataset_review_submit_gate_runs
  where id = v_run.id;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_review_submit_gate',
    v_actor,
    p_table,
    p_id,
    p_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'action', v_action,
      'gate_run_id', v_run.id,
      'worker_job_id', v_run.worker_job_id,
      'revision_checksum', p_revision_checksum,
      'policy_profile', p_policy_profile,
      'report_schema_version', p_report_schema_version,
      'supersedes_gate_run_id', v_supersedes
    )
  );

  return v_link_result;
end;
$_$;


ALTER FUNCTION "public"."cmd_dataset_review_submit_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_action" "text", "p_gate_run_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";


COMMENT ON FUNCTION "public"."cmd_dataset_review_submit_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_action" "text", "p_gate_run_id" "uuid", "p_audit" "jsonb") IS 'User-facing review-submit gate RPC. Standalone ensure/rerun now creates or reuses review_submit.gate worker_jobs and links them to retained gate history rows.';



CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_gate_enqueue_worker_job"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_requested_by" "uuid", "p_gate_run_id" "uuid" DEFAULT NULL::"uuid", "p_action" "text" DEFAULT 'ensure'::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_action text := lower(trim(coalesce(p_action, 'ensure')));
  v_kind public.worker_job_kinds%rowtype;
  v_worker_existing public.worker_jobs%rowtype;
  v_worker_job public.worker_jobs%rowtype;
  v_worker_payload jsonb;
  v_idempotency_key text;
  v_concurrency_key text;
begin
  if p_requested_by is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_REQUESTED_BY_REQUIRED',
      'status', 400,
      'message', 'requestedBy is required for review-submit gate worker jobs'
    );
  end if;

  select *
    into v_kind
  from public.worker_job_kinds
  where job_kind = 'review_submit.gate';

  if v_kind.job_kind is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_KIND_UNSUPPORTED',
      'status', 500,
      'message', 'review_submit.gate worker job kind is not registered'
    );
  end if;

  v_worker_payload := jsonb_strip_nulls(
    jsonb_build_object(
      'datasetRevision', jsonb_build_object(
        'table', p_table,
        'id', p_id,
        'version', p_version,
        'revisionChecksum', p_revision_checksum
      ),
      'policy', jsonb_build_object(
        'profile', p_policy_profile,
        'reportSchemaVersion', p_report_schema_version
      ),
      'requestedBy', p_requested_by,
      'gateRunId', p_gate_run_id
    )
  );

  v_idempotency_key := case
    when v_action = 'rerun' and p_gate_run_id is not null then concat_ws(
      ':',
      'review_submit.gate.rerun',
      p_gate_run_id::text
    )
    else concat_ws(
      ':',
      'review_submit.gate',
      p_table,
      p_id::text,
      p_version,
      p_revision_checksum,
      p_policy_profile,
      p_report_schema_version,
      p_requested_by::text
    )
  end;

  v_concurrency_key := concat_ws(
    ':',
    'review_submit.gate',
    p_table,
    p_id::text,
    p_version,
    p_requested_by::text
  );

  if v_action = 'rerun' then
    select *
      into v_worker_existing
    from public.worker_jobs
    where worker_runtime = v_kind.worker_runtime
      and job_kind = v_kind.job_kind
      and requested_by is not distinct from p_requested_by
      and idempotency_key = v_idempotency_key
      and status in ('queued', 'running', 'waiting', 'stale')
    order by created_at desc
    limit 1
    for update;
  else
    select *
      into v_worker_existing
    from public.worker_jobs
    where worker_runtime = v_kind.worker_runtime
      and job_kind = v_kind.job_kind
      and requested_by is not distinct from p_requested_by
      and idempotency_key = v_idempotency_key
      and status in ('queued', 'running', 'waiting', 'stale', 'blocked', 'completed')
    order by created_at desc
    limit 1
    for update;
  end if;

  if v_worker_existing.id is not null then
    return jsonb_build_object(
      'ok', true,
      'data', public.worker_job_payload(v_worker_existing, true),
      'reused', true
    );
  end if;

  select *
    into v_worker_existing
  from public.worker_jobs
  where worker_runtime = v_kind.worker_runtime
    and worker_queue = v_kind.worker_queue
    and concurrency_key = v_concurrency_key
    and status in ('queued', 'running', 'waiting', 'stale')
  order by created_at desc
  limit 1
  for update;

  if v_worker_existing.id is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_CONCURRENCY_CONFLICT',
      'status', 409,
      'message', 'A conflicting review-submit gate job is already active',
      'details', public.worker_job_payload(v_worker_existing, false)
    );
  end if;

  insert into public.worker_jobs (
    job_kind,
    worker_runtime,
    worker_queue,
    priority,
    subject_type,
    subject_id,
    subject_version,
    requester_type,
    requested_by,
    idempotency_key,
    concurrency_key,
    visibility,
    max_attempts,
    payload_schema_version,
    payload_json,
    result_schema_version
  ) values (
    v_kind.job_kind,
    v_kind.worker_runtime,
    v_kind.worker_queue,
    v_kind.default_priority,
    p_table,
    p_id,
    p_version,
    'user',
    p_requested_by,
    v_idempotency_key,
    v_concurrency_key,
    v_kind.default_visibility,
    greatest(1, coalesce(v_kind.default_max_attempts, 3)),
    v_kind.payload_schema_version,
    v_worker_payload,
    v_kind.result_schema_version
  )
  returning *
    into v_worker_job;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    details
  ) values (
    v_worker_job.id,
    'enqueued',
    v_worker_job.status,
    jsonb_build_object(
      'jobKind', v_worker_job.job_kind,
      'workerQueue', v_worker_job.worker_queue,
      'idempotencyKey', v_worker_job.idempotency_key,
      'concurrencyKey', v_worker_job.concurrency_key,
      'gateRunId', p_gate_run_id,
      'source', 'cmd_dataset_review_submit_gate'
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.worker_job_payload(v_worker_job, true),
    'reused', false
  );
end;
$$;


ALTER FUNCTION "public"."cmd_dataset_review_submit_gate_enqueue_worker_job"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_requested_by" "uuid", "p_gate_run_id" "uuid", "p_action" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_gate_link_worker_job"("p_gate_run_id" "uuid", "p_action" "text" DEFAULT 'ensure'::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_run public.dataset_review_submit_gate_runs%rowtype;
  v_worker_result jsonb;
  v_worker_job public.worker_jobs%rowtype;
  v_worker_job_id uuid;
  v_status text;
  v_calculator_report jsonb;
  v_blocking_reasons jsonb;
begin
  select *
    into v_run
  from public.dataset_review_submit_gate_runs
  where id = p_gate_run_id
  for update;

  if v_run.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit gate run not found'
    );
  end if;

  if v_run.worker_job_id is null and v_run.status in ('queued', 'running') then
    v_worker_result := public.cmd_dataset_review_submit_gate_enqueue_worker_job(
      v_run.dataset_table,
      v_run.dataset_id,
      v_run.dataset_version,
      v_run.revision_checksum,
      v_run.policy_profile,
      v_run.report_schema_version,
      v_run.requested_by,
      v_run.id,
      p_action
    );

    if coalesce((v_worker_result->>'ok')::boolean, false) is false then
      return v_worker_result;
    end if;

    v_worker_job_id := nullif(v_worker_result->'data'->>'id', '')::uuid;

    select *
      into v_worker_job
    from public.worker_jobs
    where id = v_worker_job_id;

    v_status := case
      when v_worker_job.status in ('queued', 'waiting', 'stale') then 'queued'
      when v_worker_job.status = 'running' then 'running'
      when v_worker_job.status = 'blocked' then 'blocked'
      when v_worker_job.status = 'completed'
        and coalesce(v_worker_job.result_json->>'status', '') = 'passed'
        then 'passed'
      when v_worker_job.status = 'completed'
        and coalesce(v_worker_job.result_json->>'status', '') = 'blocked'
        then 'blocked'
      when v_worker_job.status in ('completed', 'failed', 'cancelled') then 'error'
      else v_run.status
    end;

    v_calculator_report := case
      when jsonb_typeof(v_worker_job.result_json->'calculatorReport') = 'object'
        then v_worker_job.result_json->'calculatorReport'
      else v_run.calculator_report
    end;

    v_blocking_reasons := coalesce(
      case
        when jsonb_typeof(v_worker_job.result_json->'blockingReasons') = 'array'
          then v_worker_job.result_json->'blockingReasons'
        else null::jsonb
      end,
      v_run.blocking_reasons,
      '[]'::jsonb
    );

    update public.dataset_review_submit_gate_runs
      set worker_job_id = v_worker_job.id,
          status = v_status,
          calculator_report = v_calculator_report,
          blocking_reasons = v_blocking_reasons,
          modified_at = now(),
          completed_at = case
            when v_status in ('passed', 'blocked', 'error') then coalesce(v_worker_job.finished_at, now())
            else completed_at
          end
    where id = v_run.id
    returning *
      into v_run;
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_gate_payload(v_run)
  );
end;
$$;


ALTER FUNCTION "public"."cmd_dataset_review_submit_gate_link_worker_job"("p_gate_run_id" "uuid", "p_action" "text") OWNER TO "postgres";

SET default_tablespace = '';

SET default_table_access_method = "heap";


CREATE TABLE IF NOT EXISTS "public"."dataset_review_submit_gate_runs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "dataset_table" "text" NOT NULL,
    "dataset_id" "uuid" NOT NULL,
    "dataset_version" "text" NOT NULL,
    "revision_checksum" "text" NOT NULL,
    "policy_profile" "text" DEFAULT 'review_submit_fast.v1'::"text" NOT NULL,
    "report_schema_version" "text" DEFAULT 'review_submit_gate_report.v1'::"text" NOT NULL,
    "status" "text" DEFAULT 'queued'::"text" NOT NULL,
    "requested_by" "uuid" NOT NULL,
    "supersedes_gate_run_id" "uuid",
    "calculator_report" "jsonb",
    "blocking_reasons" "jsonb" DEFAULT '[]'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "completed_at" timestamp with time zone,
    "worker_job_id" "uuid",
    CONSTRAINT "dataset_review_submit_gate_runs_blocking_reasons_check" CHECK (("jsonb_typeof"("blocking_reasons") = 'array'::"text")),
    CONSTRAINT "dataset_review_submit_gate_runs_calculator_report_check" CHECK ((("calculator_report" IS NULL) OR ("jsonb_typeof"("calculator_report") = 'object'::"text"))),
    CONSTRAINT "dataset_review_submit_gate_runs_checksum_check" CHECK (("revision_checksum" ~ '^[a-f0-9]{64}$'::"text")),
    CONSTRAINT "dataset_review_submit_gate_runs_status_check" CHECK (("status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'passed'::"text", 'blocked'::"text", 'error'::"text", 'stale'::"text"]))),
    CONSTRAINT "dataset_review_submit_gate_runs_table_check" CHECK (("dataset_table" = ANY (ARRAY['processes'::"text", 'lifecyclemodels'::"text"])))
);


ALTER TABLE "public"."dataset_review_submit_gate_runs" OWNER TO "postgres";


COMMENT ON TABLE "public"."dataset_review_submit_gate_runs" IS 'Review-submit gate report/history table retained for compatibility. New gate execution lifecycle is public.worker_jobs.';



COMMENT ON COLUMN "public"."dataset_review_submit_gate_runs"."worker_job_id" IS 'Canonical review_submit.gate worker_jobs execution that produced this retained gate report row.';



CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_gate_payload"("p_run" "public"."dataset_review_submit_gate_runs", "p_status_override" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "sql" STABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  with worker as (
    select w.*
    from public.worker_jobs as w
    where w.id = (p_run).worker_job_id
    limit 1
  ),
  shaped as (
    select
      worker.id as worker_job_id,
      case
        when p_status_override is not null then p_status_override
        when worker.id is null then (p_run).status
        when worker.status in ('queued', 'waiting', 'stale') then 'queued'
        when worker.status = 'running' then 'running'
        when worker.status = 'blocked' then 'blocked'
        when worker.status = 'completed'
          and coalesce(worker.result_json->>'status', '') = 'passed'
          then 'passed'
        when worker.status = 'completed'
          and coalesce(worker.result_json->>'status', '') = 'blocked'
          then 'blocked'
        when worker.status in ('completed', 'failed', 'cancelled') then 'error'
        else (p_run).status
      end as effective_status,
      case
        when (p_run).calculator_report is not null then
          jsonb_build_object(
            'schemaVersion',
            coalesce((p_run).report_schema_version, worker.result_schema_version)
          ) || (p_run).calculator_report
        when worker.id is not null
          and jsonb_typeof(worker.result_json->'calculatorReport') = 'object'
          then jsonb_build_object(
            'schemaVersion',
            coalesce(
              worker.result_json #>> '{calculatorReport,schemaVersion}',
              (p_run).report_schema_version,
              worker.result_schema_version
            )
          ) || (worker.result_json->'calculatorReport')
        else null::jsonb
      end as effective_calculator_report,
      coalesce(
        case
          when jsonb_typeof(worker.result_json->'blockingReasons') = 'array'
            then worker.result_json->'blockingReasons'
          else null::jsonb
        end,
        case
          when cardinality(worker.blocker_codes) > 0 then (
            select jsonb_agg(jsonb_build_object('code', code) order by code)
            from unnest(worker.blocker_codes) as code
          )
          else null::jsonb
        end,
        (p_run).blocking_reasons,
        '[]'::jsonb
      ) as effective_blocking_reasons,
      greatest(
        (p_run).modified_at,
        coalesce(worker.updated_at, (p_run).modified_at)
      ) as effective_modified_at,
      coalesce(
        (p_run).completed_at,
        case
          when worker.status in ('completed', 'blocked', 'failed', 'cancelled')
            then worker.finished_at
          else null::timestamptz
        end
      ) as effective_completed_at
    from (select 1) as seed
    left join worker on true
  )
  select jsonb_strip_nulls(
    jsonb_build_object(
      'status', shaped.effective_status,
      'gateRunId', (p_run).id,
      'workerJobId', shaped.worker_job_id,
      'workerJob',
        case
          when shaped.worker_job_id is null then null
          else (
            select public.worker_job_payload(worker, false)
            from worker
          )
        end,
      'datasetRevision', jsonb_build_object(
        'table', (p_run).dataset_table,
        'id', (p_run).dataset_id,
        'version', (p_run).dataset_version,
        'revisionChecksum', (p_run).revision_checksum
      ),
      'policy', jsonb_build_object(
        'profile', (p_run).policy_profile,
        'reportSchemaVersion', (p_run).report_schema_version
      ),
      'calculatorReport', shaped.effective_calculator_report,
      'blockingReasons', shaped.effective_blocking_reasons,
      'createdAt', to_jsonb((p_run).created_at),
      'modifiedAt', to_jsonb(shaped.effective_modified_at),
      'completedAt', to_jsonb(shaped.effective_completed_at)
    )
  )
  from shaped
$$;


ALTER FUNCTION "public"."cmd_dataset_review_submit_gate_payload"("p_run" "public"."dataset_review_submit_gate_runs", "p_status_override" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_gate_record_result"("p_gate_run_id" "uuid", "p_status" "text", "p_calculator_report" "jsonb" DEFAULT NULL::"jsonb", "p_blocking_reasons" "jsonb" DEFAULT '[]'::"jsonb", "p_report_schema_version" "text" DEFAULT 'review_submit_gate_report.v1'::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_status text := lower(trim(coalesce(p_status, '')));
  v_run public.dataset_review_submit_gate_runs%rowtype;
  v_worker_status text;
  v_worker_result jsonb;
  v_blocker_codes text[];
begin
  if v_status not in ('passed', 'blocked', 'error') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_RESULT_STATUS',
      'status', 400,
      'message', 'result status must be passed, blocked, or error'
    );
  end if;

  if coalesce(p_report_schema_version, '') <> 'review_submit_gate_report.v1' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_SCHEMA_UNSUPPORTED',
      'status', 400,
      'message', 'Unsupported review-submit gate report schema version',
      'details', jsonb_build_object('report_schema_version', p_report_schema_version)
    );
  end if;

  if p_calculator_report is not null and jsonb_typeof(p_calculator_report) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_REPORT',
      'status', 400,
      'message', 'calculator report must be a JSON object'
    );
  end if;

  if jsonb_typeof(coalesce(p_blocking_reasons, '[]'::jsonb)) <> 'array' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_BLOCKING_REASONS',
      'status', 400,
      'message', 'blockingReasons must be a JSON array'
    );
  end if;

  if v_status = 'passed' and jsonb_array_length(coalesce(p_blocking_reasons, '[]'::jsonb)) > 0 then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_RESULT',
      'status', 400,
      'message', 'passed gate results cannot include blockingReasons'
    );
  end if;

  select *
    into v_run
  from public.dataset_review_submit_gate_runs
  where id = p_gate_run_id
  for update;

  if v_run.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit gate run not found'
    );
  end if;

  update public.dataset_review_submit_gate_runs
    set status = v_status,
        calculator_report = p_calculator_report,
        blocking_reasons = coalesce(p_blocking_reasons, '[]'::jsonb),
        report_schema_version = p_report_schema_version,
        modified_at = now(),
        completed_at = now()
  where id = p_gate_run_id
  returning *
    into v_run;

  if v_run.worker_job_id is not null then
    v_worker_status := case
      when v_status = 'passed' then 'completed'
      when v_status = 'blocked' then 'blocked'
      else 'failed'
    end;

    select coalesce(
      array_agg(distinct nullif(reason->>'code', '')) filter (where nullif(reason->>'code', '') is not null),
      '{}'::text[]
    )
      into v_blocker_codes
    from jsonb_array_elements(coalesce(p_blocking_reasons, '[]'::jsonb)) as reason;

    if v_worker_status = 'blocked' and cardinality(v_blocker_codes) = 0 then
      v_blocker_codes := array['review_submit_gate_blocked'];
    end if;

    v_worker_result := jsonb_strip_nulls(
      jsonb_build_object(
        'status', v_status,
        'datasetRevision', jsonb_build_object(
          'table', v_run.dataset_table,
          'id', v_run.dataset_id,
          'version', v_run.dataset_version,
          'revisionChecksum', v_run.revision_checksum
        ),
        'policy', jsonb_build_object(
          'profile', v_run.policy_profile,
          'reportSchemaVersion', p_report_schema_version
        ),
        'calculatorReport', p_calculator_report,
        'blockingReasons', coalesce(p_blocking_reasons, '[]'::jsonb),
        'gateRunId', v_run.id,
        'recordedBy', 'cmd_dataset_review_submit_gate_record_result'
      )
    );

    update public.worker_jobs
      set status = v_worker_status,
          result_json = case
            when v_worker_status in ('completed', 'blocked') then v_worker_result
            else result_json
          end,
          result_schema_version = coalesce(result_schema_version, 'review_submit.gate.result.v1'),
          error_code = case
            when v_worker_status = 'failed' then 'REVIEW_SUBMIT_GATE_ERROR'
            else null
          end,
          error_message = case
            when v_worker_status = 'failed' then 'Review-submit gate failed before review submission'
            else null
          end,
          blocker_codes = case
            when v_worker_status = 'blocked' then v_blocker_codes
            else '{}'::text[]
          end,
          resolution_scope = case
            when v_worker_status = 'blocked' then 'user'
            else null
          end,
          retryable = case
            when v_worker_status = 'failed' then true
            when v_worker_status in ('completed', 'blocked') then false
            else retryable
          end,
          updated_at = now(),
          finished_at = now()
    where id = v_run.worker_job_id;

    insert into public.worker_job_events (
      job_id,
      event_type,
      status,
      details
    ) values (
      v_run.worker_job_id,
      'legacy_gate_result_recorded',
      v_worker_status,
      jsonb_build_object(
        'gateRunId', v_run.id,
        'gateStatus', v_status,
        'source', 'cmd_dataset_review_submit_gate_record_result'
      )
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
    'cmd_dataset_review_submit_gate_record_result',
    coalesce(v_actor, v_run.requested_by),
    v_run.dataset_table,
    v_run.dataset_id,
    v_run.dataset_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'gate_run_id', v_run.id,
      'worker_job_id', v_run.worker_job_id,
      'status', v_status,
      'report_schema_version', p_report_schema_version
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_gate_payload(v_run)
  );
end;
$$;


ALTER FUNCTION "public"."cmd_dataset_review_submit_gate_record_result"("p_gate_run_id" "uuid", "p_status" "text", "p_calculator_report" "jsonb", "p_blocking_reasons" "jsonb", "p_report_schema_version" "text", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_job_claim"("p_qty" integer DEFAULT 10, "p_stale_submitting_seconds" integer DEFAULT 300) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_qty integer := greatest(1, least(coalesce(p_qty, 10), 50));
  v_stale_seconds integer := greatest(30, coalesce(p_stale_submitting_seconds, 300));
  v_jobs jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to claim review-submit jobs'
    );
  end if;

  with claimed as (
    select id
    from public.dataset_review_submit_requests
    where status in ('queued', 'waiting_gate')
      or (
        status = 'submitting'
        and modified_at < now() - make_interval(secs => v_stale_seconds)
      )
    order by
      case status
        when 'queued' then 0
        when 'waiting_gate' then 1
        else 2
      end,
      created_at asc
    for update skip locked
    limit v_qty
  ),
  updated as (
    update public.dataset_review_submit_requests as request
      set status = 'submitting',
          attempt_count = request.attempt_count + 1,
          modified_at = now()
    from claimed
    where request.id = claimed.id
    returning request.*
  )
  select coalesce(jsonb_agg(public.cmd_dataset_review_submit_job_payload(updated)), '[]'::jsonb)
    into v_jobs
  from updated;

  return jsonb_build_object(
    'ok', true,
    'data', v_jobs
  );
end;
$$;


ALTER FUNCTION "public"."cmd_dataset_review_submit_job_claim"("p_qty" integer, "p_stale_submitting_seconds" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_job_enqueue"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text" DEFAULT 'review_submit_fast.v1'::"text", "p_report_schema_version" "text" DEFAULT 'review_submit_gate_report.v1'::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_dataset_row jsonb;
  v_owner_id uuid;
  v_state_code integer;
  v_kind public.worker_job_kinds%rowtype;
  v_worker_existing public.worker_jobs%rowtype;
  v_worker_job public.worker_jobs%rowtype;
  v_worker_payload jsonb;
  v_idempotency_key text;
  v_concurrency_key text;
  v_worker_status text;
  v_job_status text := 'waiting_gate';
  v_last_error_code text;
  v_last_error_message text;
  v_last_error_details jsonb;
  v_completed_at timestamptz;
  v_existing public.dataset_review_submit_requests%rowtype;
  v_job public.dataset_review_submit_requests%rowtype;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table <> 'processes' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Review-submit jobs currently support process datasets only'
    );
  end if;

  if coalesce(p_revision_checksum, '') !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVISION_CHECKSUM_REQUIRED',
      'status', 400,
      'message', 'revisionChecksum must be a lowercase SHA-256 hex digest'
    );
  end if;

  if coalesce(p_policy_profile, '') <> 'review_submit_fast.v1' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_POLICY_UNSUPPORTED',
      'status', 400,
      'message', 'Unsupported review-submit gate policy profile',
      'details', jsonb_build_object('policy_profile', p_policy_profile)
    );
  end if;

  if coalesce(p_report_schema_version, '') <> 'review_submit_gate_report.v1' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_SCHEMA_UNSUPPORTED',
      'status', 400,
      'message', 'Unsupported review-submit gate report schema version',
      'details', jsonb_build_object('report_schema_version', p_report_schema_version)
    );
  end if;

  v_dataset_row := public.cmd_review_get_dataset_row(p_table, p_id, p_version, false);

  if v_dataset_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  v_owner_id := nullif(v_dataset_row->>'user_id', '')::uuid;
  v_state_code := coalesce(nullif(v_dataset_row->>'state_code', '')::integer, 0);

  if v_owner_id is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the dataset owner can enqueue review submission'
    );
  end if;

  if v_state_code >= 100 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_ALREADY_PUBLISHED',
      'status', 409,
      'message', 'Published datasets cannot be submitted for review again'
    );
  end if;

  if v_state_code >= 20 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_UNDER_REVIEW',
      'status', 409,
      'message', 'Dataset is already under review'
    );
  end if;

  perform pg_advisory_xact_lock(
    hashtextextended(
      concat_ws(
        ':',
        'review_submit_request',
        p_table,
        p_id::text,
        p_version,
        p_revision_checksum,
        p_policy_profile,
        p_report_schema_version,
        v_actor::text
      ),
      0
    )
  );

  select *
    into v_existing
  from public.dataset_review_submit_requests
  where dataset_table = p_table
    and dataset_id = p_id
    and dataset_version = p_version
    and revision_checksum = p_revision_checksum
    and policy_profile = p_policy_profile
    and report_schema_version = p_report_schema_version
    and requested_by = v_actor
    and status in ('queued', 'waiting_gate', 'submitting', 'submitted')
  order by created_at desc
  limit 1
  for update;

  if v_existing.id is not null then
    return jsonb_build_object(
      'ok', true,
      'data', public.cmd_dataset_review_submit_job_payload(v_existing)
    );
  end if;

  select *
    into v_kind
  from public.worker_job_kinds
  where job_kind = 'review_submit.gate';

  if v_kind.job_kind is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_KIND_UNSUPPORTED',
      'status', 500,
      'message', 'review_submit.gate worker job kind is not registered'
    );
  end if;

  v_worker_payload := jsonb_build_object(
    'datasetRevision', jsonb_build_object(
      'table', p_table,
      'id', p_id,
      'version', p_version,
      'revisionChecksum', p_revision_checksum
    ),
    'policy', jsonb_build_object(
      'profile', p_policy_profile,
      'reportSchemaVersion', p_report_schema_version
    ),
    'requestedBy', v_actor
  );
  v_idempotency_key := concat_ws(
    ':',
    'review_submit.gate',
    p_table,
    p_id::text,
    p_version,
    p_revision_checksum,
    p_policy_profile,
    p_report_schema_version,
    v_actor::text
  );
  v_concurrency_key := concat_ws(
    ':',
    'review_submit.gate',
    p_table,
    p_id::text,
    p_version,
    v_actor::text
  );

  select *
    into v_worker_existing
  from public.worker_jobs
  where worker_runtime = 'calculator'
    and job_kind = 'review_submit.gate'
    and requested_by is not distinct from v_actor
    and idempotency_key = v_idempotency_key
    and status in ('queued', 'running', 'waiting', 'stale', 'blocked')
  order by created_at desc
  limit 1
  for update;

  if v_worker_existing.id is not null then
    v_worker_job := v_worker_existing;
  else
    select *
      into v_worker_existing
    from public.worker_jobs
    where worker_runtime = 'calculator'
      and worker_queue = 'review_submit_gate'
      and concurrency_key = v_concurrency_key
      and status in ('queued', 'running', 'waiting', 'stale')
    order by created_at desc
    limit 1
    for update;

    if v_worker_existing.id is not null then
      return jsonb_build_object(
        'ok', false,
        'code', 'WORKER_JOB_CONCURRENCY_CONFLICT',
        'status', 409,
        'message', 'A conflicting review-submit gate job is already active',
        'details', public.worker_job_payload(v_worker_existing, false)
      );
    end if;

    insert into public.worker_jobs (
      job_kind,
      worker_runtime,
      worker_queue,
      priority,
      subject_type,
      subject_id,
      subject_version,
      requester_type,
      requested_by,
      idempotency_key,
      concurrency_key,
      visibility,
      max_attempts,
      payload_schema_version,
      payload_json,
      result_schema_version
    ) values (
      v_kind.job_kind,
      v_kind.worker_runtime,
      v_kind.worker_queue,
      v_kind.default_priority,
      p_table,
      p_id,
      p_version,
      'user',
      v_actor,
      v_idempotency_key,
      v_concurrency_key,
      v_kind.default_visibility,
      v_kind.default_max_attempts,
      v_kind.payload_schema_version,
      v_worker_payload,
      v_kind.result_schema_version
    )
    returning *
      into v_worker_job;

    insert into public.worker_job_events (
      job_id,
      event_type,
      status,
      details
    ) values (
      v_worker_job.id,
      'enqueued',
      v_worker_job.status,
      jsonb_build_object(
        'jobKind', v_worker_job.job_kind,
        'workerQueue', v_worker_job.worker_queue,
        'idempotencyKey', v_worker_job.idempotency_key,
        'concurrencyKey', v_worker_job.concurrency_key,
        'source', 'cmd_dataset_review_submit_job_enqueue'
      )
    );
  end if;

  v_worker_status := v_worker_job.status;
  v_job_status := case
    when v_worker_status = 'completed' then 'queued'
    when v_worker_status in ('queued', 'running', 'waiting', 'stale') then 'waiting_gate'
    when v_worker_status = 'blocked' then 'blocked'
    when v_worker_status = 'cancelled' then 'cancelled'
    else 'error'
  end;

  if v_job_status = 'blocked' then
    v_last_error_code := 'REVIEW_SUBMIT_GATE_BLOCKED';
    v_last_error_message := 'Review-submit gate blocked this dataset revision';
    v_last_error_details := public.worker_job_payload(v_worker_job, false);
  elsif v_job_status = 'stale' then
    v_last_error_code := 'REVIEW_SUBMIT_GATE_STALE';
    v_last_error_message := 'Review-submit gate run is stale for the submitted dataset revision';
    v_last_error_details := public.worker_job_payload(v_worker_job, false);
  elsif v_job_status = 'error' then
    v_last_error_code := coalesce(v_worker_job.error_code, 'REVIEW_SUBMIT_GATE_ERROR');
    v_last_error_message := coalesce(v_worker_job.error_message, 'Review-submit gate failed before review submission');
    v_last_error_details := public.worker_job_payload(v_worker_job, false);
  elsif v_job_status = 'cancelled' then
    v_last_error_code := 'REVIEW_SUBMIT_JOB_CANCELLED';
    v_last_error_message := 'Review-submit job was cancelled';
    v_last_error_details := public.worker_job_payload(v_worker_job, false);
  end if;

  if v_job_status in ('submitted', 'blocked', 'stale', 'error', 'cancelled') then
    v_completed_at := now();
  end if;

  insert into public.dataset_review_submit_requests (
    dataset_table,
    dataset_id,
    dataset_version,
    revision_checksum,
    policy_profile,
    report_schema_version,
    status,
    requested_by,
    gate_worker_job_id,
    last_error_code,
    last_error_message,
    last_error_details,
    completed_at
  )
  values (
    p_table,
    p_id,
    p_version,
    p_revision_checksum,
    p_policy_profile,
    p_report_schema_version,
    v_job_status,
    v_actor,
    v_worker_job.id,
    v_last_error_code,
    v_last_error_message,
    v_last_error_details,
    v_completed_at
  )
  returning *
    into v_job;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_review_submit_job_enqueue',
    v_actor,
    p_table,
    p_id,
    p_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'review_submit_job_id', v_job.id,
      'review_submit_request_id', v_job.id,
      'gate_worker_job_id', v_worker_job.id,
      'gate_worker_job_status', v_worker_status,
      'revision_checksum', p_revision_checksum,
      'policy_profile', p_policy_profile,
      'report_schema_version', p_report_schema_version
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_job_payload(v_job)
  );
end;
$_$;


ALTER FUNCTION "public"."cmd_dataset_review_submit_job_enqueue"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_job_payload"("p_job" "anyelement") RETURNS "jsonb"
    LANGUAGE "sql" STABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  with job as (
    select to_jsonb(p_job) as row_json
  )
  select jsonb_strip_nulls(
    jsonb_build_object(
      'status', row_json->>'status',
      'reviewSubmitJobId', row_json->'id',
      'submitWorkerJobId', row_json->'submit_worker_job_id',
      'gateRunId', row_json->'gate_run_id',
      'gateWorkerJobId', row_json->'gate_worker_job_id',
      'datasetRevision', jsonb_build_object(
        'table', row_json->>'dataset_table',
        'id', row_json->'dataset_id',
        'version', row_json->>'dataset_version',
        'revisionChecksum', row_json->>'revision_checksum'
      ),
      'policy', jsonb_build_object(
        'profile', row_json->>'policy_profile',
        'reportSchemaVersion', row_json->>'report_schema_version'
      ),
      'requestedBy', row_json->'requested_by',
      'attemptCount', row_json->'attempt_count',
      'error',
        case
          when row_json->>'last_error_code' is null
            and row_json->>'last_error_message' is null
            and row_json->'last_error_details' is null then null
          else jsonb_strip_nulls(
            jsonb_build_object(
              'code', row_json->>'last_error_code',
              'message', row_json->>'last_error_message',
              'details', row_json->'last_error_details'
            )
          )
        end,
      'result', row_json->'result',
      'submitWorkerJob',
        (
          select public.worker_job_payload(w, false)
          from public.worker_jobs as w
          where w.id = nullif(row_json->>'submit_worker_job_id', '')::uuid
        ),
      'gate',
        (
          select public.cmd_dataset_review_submit_gate_payload(g)
          from public.dataset_review_submit_gate_runs as g
          where g.id = nullif(row_json->>'gate_run_id', '')::uuid
        ),
      'gateWorkerJob',
        (
          select public.worker_job_payload(w, false)
          from public.worker_jobs as w
          where w.id = nullif(row_json->>'gate_worker_job_id', '')::uuid
        ),
      'createdAt', row_json->'created_at',
      'modifiedAt', row_json->'modified_at',
      'completedAt', row_json->'completed_at'
    )
  )
  from job
$$;


ALTER FUNCTION "public"."cmd_dataset_review_submit_job_payload"("p_job" "anyelement") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_job_read"("p_job_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_is_service boolean := coalesce(util.is_service_request(), false);
  v_job public.dataset_review_submit_requests%rowtype;
begin
  if p_job_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_ID_REQUIRED',
      'status', 400,
      'message', 'reviewSubmitJobId is required'
    );
  end if;

  if v_actor is null and not v_is_service then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  select *
    into v_job
  from public.dataset_review_submit_requests
  where id = p_job_id;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit job not found'
    );
  end if;

  if not v_is_service and v_job.requested_by is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the job requester can read this review-submit job'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_job_payload(v_job)
  );
end;
$$;


ALTER FUNCTION "public"."cmd_dataset_review_submit_job_read"("p_job_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_job_read_latest"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_dataset_row jsonb;
  v_owner_id uuid;
  v_job public.dataset_review_submit_requests%rowtype;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table <> 'processes' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Review-submit jobs currently support process datasets only'
    );
  end if;

  if p_revision_checksum is not null
    and p_revision_checksum !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVISION_CHECKSUM_REQUIRED',
      'status', 400,
      'message', 'revisionChecksum must be a lowercase SHA-256 hex digest'
    );
  end if;

  v_dataset_row := public.cmd_review_get_dataset_row(p_table, p_id, p_version, false);

  if v_dataset_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  v_owner_id := nullif(v_dataset_row->>'user_id', '')::uuid;

  if v_owner_id is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the dataset owner can read review-submit jobs'
    );
  end if;

  select *
    into v_job
  from public.dataset_review_submit_requests
  where dataset_table = p_table
    and dataset_id = p_id
    and dataset_version = p_version
    and requested_by = v_actor
    and (p_revision_checksum is null or revision_checksum = p_revision_checksum)
  order by created_at desc
  limit 1;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit job not found'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_job_payload(v_job)
  );
end;
$_$;


ALTER FUNCTION "public"."cmd_dataset_review_submit_job_read_latest"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_job_record_result"("p_job_id" "uuid", "p_status" "text", "p_gate_run_id" "uuid" DEFAULT NULL::"uuid", "p_result" "jsonb" DEFAULT NULL::"jsonb", "p_error_code" "text" DEFAULT NULL::"text", "p_error_message" "text" DEFAULT NULL::"text", "p_error_details" "jsonb" DEFAULT NULL::"jsonb", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_status text := lower(trim(coalesce(p_status, '')));
  v_job public.dataset_review_submit_requests%rowtype;
  v_gate public.dataset_review_submit_gate_runs%rowtype;
  v_completed_at timestamptz;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to record review-submit job results'
    );
  end if;

  if v_status not in ('waiting_gate', 'blocked', 'stale', 'error', 'cancelled') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_JOB_STATUS',
      'status', 400,
      'message', 'status must be waiting_gate, blocked, stale, error, or cancelled'
    );
  end if;

  if p_result is not null and jsonb_typeof(p_result) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_JOB_RESULT',
      'status', 400,
      'message', 'result must be a JSON object'
    );
  end if;

  if p_error_details is not null and jsonb_typeof(p_error_details) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_JOB_ERROR_DETAILS',
      'status', 400,
      'message', 'error details must be a JSON object'
    );
  end if;

  select *
    into v_job
  from public.dataset_review_submit_requests
  where id = p_job_id
  for update;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit job not found'
    );
  end if;

  if p_gate_run_id is not null then
    select *
      into v_gate
    from public.dataset_review_submit_gate_runs
    where id = p_gate_run_id;

    if v_gate.id is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate run not found'
      );
    end if;

    if v_gate.dataset_table <> v_job.dataset_table
      or v_gate.dataset_id <> v_job.dataset_id
      or v_gate.dataset_version <> v_job.dataset_version
      or v_gate.revision_checksum <> v_job.revision_checksum
      or v_gate.policy_profile <> v_job.policy_profile
      or v_gate.report_schema_version <> v_job.report_schema_version then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
        'status', 409,
        'message', 'Review-submit gate run does not match this review-submit job'
      );
    end if;
  end if;

  if v_status in ('blocked', 'stale', 'error', 'cancelled') then
    v_completed_at := now();
  end if;

  update public.dataset_review_submit_requests
    set status = v_status,
        gate_run_id = coalesce(p_gate_run_id, gate_run_id),
        result = p_result,
        last_error_code =
          case when v_status = 'waiting_gate' then null else p_error_code end,
        last_error_message =
          case when v_status = 'waiting_gate' then null else p_error_message end,
        last_error_details =
          case when v_status = 'waiting_gate' then null else p_error_details end,
        modified_at = now(),
        completed_at = v_completed_at
  where id = v_job.id
  returning *
    into v_job;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_review_submit_job_record_result',
    v_job.requested_by,
    v_job.dataset_table,
    v_job.dataset_id,
    v_job.dataset_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'review_submit_job_id', v_job.id,
      'review_submit_request_id', v_job.id,
      'gate_run_id', v_job.gate_run_id,
      'status', v_status,
      'error_code', p_error_code
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_job_payload(v_job)
  );
end;
$$;


ALTER FUNCTION "public"."cmd_dataset_review_submit_job_record_result"("p_job_id" "uuid", "p_status" "text", "p_gate_run_id" "uuid", "p_result" "jsonb", "p_error_code" "text", "p_error_message" "text", "p_error_details" "jsonb", "p_audit" "jsonb") OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb", "p_review_submit_gate_run_id" "uuid" DEFAULT NULL::"uuid", "p_review_submit_revision_checksum" "text" DEFAULT NULL::"text", "p_review_submit_policy_profile" "text" DEFAULT 'review_submit_fast.v1'::"text", "p_review_submit_report_schema_version" "text" DEFAULT 'review_submit_gate_report.v1'::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_gate_assertion jsonb;
begin
  v_gate_assertion := public.cmd_dataset_assert_review_submit_gate_passed(
    p_table,
    p_id,
    p_version,
    p_review_submit_gate_run_id,
    p_review_submit_revision_checksum,
    p_review_submit_policy_profile,
    p_review_submit_report_schema_version
  );

  if coalesce((v_gate_assertion->>'ok')::boolean, false) is false then
    return v_gate_assertion;
  end if;

  return public.cmd_review_submit_without_gate(
    p_table,
    p_id,
    p_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'review_submit_gate_run_id', p_review_submit_gate_run_id,
      'review_submit_revision_checksum', p_review_submit_revision_checksum,
      'review_submit_policy_profile', p_review_submit_policy_profile,
      'review_submit_report_schema_version', p_review_submit_report_schema_version
    )
  );
end;
$$;


ALTER FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb", "p_review_submit_gate_run_id" "uuid", "p_review_submit_revision_checksum" "text", "p_review_submit_policy_profile" "text", "p_review_submit_report_schema_version" "text") OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."cmd_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_job public.dataset_review_submit_requests%rowtype;
  v_worker_job public.worker_jobs%rowtype;
  v_worker_result_checksum text;
  v_dataset_found boolean;
  v_owner_id uuid;
  v_state_code integer;
  v_modified_at timestamptz;
  v_submit_result jsonb;
  v_error_code text;
  v_error_status integer;
  v_error_message text;
  v_job_status text;
  v_prev_sub text;
  v_prev_role text;
  v_prev_claims text;
  v_submit_audit jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to submit review from a job'
    );
  end if;

  select *
    into v_job
  from public.dataset_review_submit_requests
  where id = p_job_id
  for update;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit job not found'
    );
  end if;

  if v_job.status = 'submitted' then
    return jsonb_build_object(
      'ok', true,
      'data', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_job.status in ('blocked', 'stale', 'error', 'cancelled') then
    return jsonb_build_object(
      'ok', false,
      'code', coalesce(v_job.last_error_code, 'REVIEW_SUBMIT_JOB_NOT_ACTIVE'),
      'status', 409,
      'message', coalesce(v_job.last_error_message, 'Review-submit job is not active'),
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_job.gate_worker_job_id is null and v_job.gate_run_id is null then
    update public.dataset_review_submit_requests
      set status = 'error',
          last_error_code = 'REVIEW_SUBMIT_JOB_GATE_REQUIRED',
          last_error_message = 'Review-submit job is missing a gate job',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_GATE_REQUIRED',
      'status', 409,
      'message', 'Review-submit job is missing a gate job',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_job.gate_worker_job_id is not null then
    select *
      into v_worker_job
    from public.worker_jobs
    where id = v_job.gate_worker_job_id
    for update;

    if v_worker_job.id is null then
      update public.dataset_review_submit_requests
        set status = 'error',
            last_error_code = 'REVIEW_SUBMIT_GATE_NOT_FOUND',
            last_error_message = 'Review-submit gate worker job not found',
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate worker job not found',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.job_kind <> 'review_submit.gate'
      or v_worker_job.subject_type <> v_job.dataset_table
      or v_worker_job.subject_id <> v_job.dataset_id
      or v_worker_job.subject_version <> v_job.dataset_version
      or v_worker_job.requested_by is distinct from v_job.requested_by
      or v_worker_job.payload_json #>> '{policy,profile}' is distinct from v_job.policy_profile
      or v_worker_job.payload_json #>> '{policy,reportSchemaVersion}' is distinct from v_job.report_schema_version then
      update public.dataset_review_submit_requests
        set status = 'stale',
            last_error_code = 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
            last_error_message = 'Review-submit gate worker job does not match this review-submit job',
            last_error_details = jsonb_build_object(
              'gateWorkerJobId', v_worker_job.id,
              'jobKind', v_worker_job.job_kind,
              'subjectType', v_worker_job.subject_type,
              'subjectId', v_worker_job.subject_id,
              'subjectVersion', v_worker_job.subject_version,
              'requestedBy', v_worker_job.requested_by,
              'policyProfile', v_worker_job.payload_json #>> '{policy,profile}',
              'reportSchemaVersion', v_worker_job.payload_json #>> '{policy,reportSchemaVersion}'
            ),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
        'status', 409,
        'message', 'Review-submit gate worker job does not match this review-submit job',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.status in ('queued', 'running', 'waiting', 'stale') then
      update public.dataset_review_submit_requests
        set status = 'waiting_gate',
            last_error_code = null,
            last_error_message = null,
            last_error_details = null,
            modified_at = now(),
            completed_at = null
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_READY',
        'status', 409,
        'message', 'Review-submit gate has not passed yet',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.status = 'blocked' then
      update public.dataset_review_submit_requests
        set status = 'blocked',
            last_error_code = 'REVIEW_SUBMIT_GATE_BLOCKED',
            last_error_message = 'Review-submit gate blocked this dataset revision',
            last_error_details = public.worker_job_payload(v_worker_job, false),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_BLOCKED',
        'status', 409,
        'message', 'Review-submit gate blocked this dataset revision',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.status = 'cancelled' then
      update public.dataset_review_submit_requests
        set status = 'cancelled',
            last_error_code = 'REVIEW_SUBMIT_JOB_CANCELLED',
            last_error_message = 'Review-submit gate worker job was cancelled',
            last_error_details = public.worker_job_payload(v_worker_job, false),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_JOB_CANCELLED',
        'status', 409,
        'message', 'Review-submit gate worker job was cancelled',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.status <> 'completed' then
      update public.dataset_review_submit_requests
        set status = 'error',
            last_error_code = coalesce(v_worker_job.error_code, 'REVIEW_SUBMIT_GATE_ERROR'),
            last_error_message = coalesce(v_worker_job.error_message, 'Review-submit gate failed before review submission'),
            last_error_details = public.worker_job_payload(v_worker_job, false),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', coalesce(v_worker_job.error_code, 'REVIEW_SUBMIT_GATE_ERROR'),
        'status', 502,
        'message', coalesce(v_worker_job.error_message, 'Review-submit gate failed before review submission'),
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if coalesce(v_worker_job.result_json->>'status', '') <> 'passed' then
      update public.dataset_review_submit_requests
        set status = 'error',
            last_error_code = 'REVIEW_SUBMIT_GATE_ERROR',
            last_error_message = 'Review-submit gate worker job completed without a passed result',
            last_error_details = public.worker_job_payload(v_worker_job, false),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_ERROR',
        'status', 502,
        'message', 'Review-submit gate worker job completed without a passed result',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.result_json #>> '{datasetRevision,table}' is distinct from v_job.dataset_table
      or v_worker_job.result_json #>> '{datasetRevision,id}' is distinct from v_job.dataset_id::text
      or v_worker_job.result_json #>> '{datasetRevision,version}' is distinct from v_job.dataset_version then
      update public.dataset_review_submit_requests
        set status = 'stale',
            last_error_code = 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
            last_error_message = 'Review-submit gate worker result does not match this review-submit job',
            last_error_details = jsonb_build_object(
              'gateWorkerJobId', v_worker_job.id,
              'resultDatasetRevision', v_worker_job.result_json->'datasetRevision'
            ),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
        'status', 409,
        'message', 'Review-submit gate worker result does not match this review-submit job',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    v_worker_result_checksum := v_worker_job.result_json #>> '{datasetRevision,revisionChecksum}';

    if v_worker_result_checksum is distinct from v_job.revision_checksum then
      update public.dataset_review_submit_requests
        set status = 'stale',
            last_error_code = 'REVIEW_SUBMIT_GATE_STALE',
            last_error_message = 'Review-submit gate worker job is stale for the submitted dataset revision',
            last_error_details = jsonb_build_object(
              'gateWorkerJobId', v_worker_job.id,
              'expectedRevisionChecksum', v_job.revision_checksum,
              'actualRevisionChecksum', v_worker_result_checksum
            ),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_STALE',
        'status', 409,
        'message', 'Review-submit gate worker job is stale for the submitted dataset revision',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;
  end if;

  execute format(
    'select true, user_id, state_code, modified_at from public.%I where id = $1 and version = $2',
    v_job.dataset_table
  )
    into v_dataset_found, v_owner_id, v_state_code, v_modified_at
    using v_job.dataset_id, v_job.dataset_version;

  if coalesce(v_dataset_found, false) is false then
    update public.dataset_review_submit_requests
      set status = 'error',
          last_error_code = 'DATASET_NOT_FOUND',
          last_error_message = 'Dataset not found',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_owner_id is distinct from v_job.requested_by then
    update public.dataset_review_submit_requests
      set status = 'error',
          last_error_code = 'DATASET_OWNER_REQUIRED',
          last_error_message = 'Only the job requester can submit this dataset for review',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the job requester can submit this dataset for review',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if coalesce(v_state_code, 0) >= 100 then
    update public.dataset_review_submit_requests
      set status = 'error',
          last_error_code = 'DATA_ALREADY_PUBLISHED',
          last_error_message = 'Published datasets cannot be submitted for review again',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_ALREADY_PUBLISHED',
      'status', 409,
      'message', 'Published datasets cannot be submitted for review again',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if coalesce(v_state_code, 0) >= 20 then
    update public.dataset_review_submit_requests
      set status = 'error',
          last_error_code = 'DATA_UNDER_REVIEW',
          last_error_message = 'Dataset is already under review',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_UNDER_REVIEW',
      'status', 409,
      'message', 'Dataset is already under review',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_modified_at > v_job.created_at then
    update public.dataset_review_submit_requests
      set status = 'stale',
          last_error_code = 'REVIEW_SUBMIT_JOB_STALE',
          last_error_message = 'Dataset changed after this review-submit job was created',
          last_error_details = jsonb_build_object(
            'jobCreatedAt', to_jsonb(v_job.created_at),
            'datasetModifiedAt', to_jsonb(v_modified_at)
          ),
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_STALE',
      'status', 409,
      'message', 'Dataset changed after this review-submit job was created',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  v_prev_sub := current_setting('request.jwt.claim.sub', true);
  v_prev_role := current_setting('request.jwt.claim.role', true);
  v_prev_claims := current_setting('request.jwt.claims', true);

  perform set_config('request.jwt.claim.sub', v_job.requested_by::text, true);
  perform set_config('request.jwt.claim.role', 'authenticated', true);
  perform set_config(
    'request.jwt.claims',
    jsonb_build_object(
      'sub', v_job.requested_by::text,
      'role', 'authenticated'
    )::text,
    true
  );

  v_submit_audit := coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
    'source', 'cmd_review_submit_from_job',
    'review_submit_job_id', v_job.id,
    'review_submit_request_id', v_job.id,
    'review_submit_gate_worker_job_id', v_job.gate_worker_job_id
  );

  if v_job.gate_worker_job_id is not null then
    v_submit_result := public.cmd_review_submit_without_gate(
      v_job.dataset_table,
      v_job.dataset_id,
      v_job.dataset_version,
      v_submit_audit || jsonb_build_object(
        'review_submit_revision_checksum', v_job.revision_checksum,
        'review_submit_policy_profile', v_job.policy_profile,
        'review_submit_report_schema_version', v_job.report_schema_version
      )
    );
  else
    v_submit_result := public.cmd_review_submit(
      p_table => v_job.dataset_table,
      p_id => v_job.dataset_id,
      p_version => v_job.dataset_version,
      p_audit => v_submit_audit,
      p_review_submit_gate_run_id => v_job.gate_run_id,
      p_review_submit_revision_checksum => v_job.revision_checksum,
      p_review_submit_policy_profile => v_job.policy_profile,
      p_review_submit_report_schema_version => v_job.report_schema_version
    );
  end if;

  perform set_config('request.jwt.claim.sub', coalesce(v_prev_sub, ''), true);
  perform set_config('request.jwt.claim.role', coalesce(v_prev_role, ''), true);
  perform set_config('request.jwt.claims', coalesce(v_prev_claims, ''), true);

  if coalesce((v_submit_result->>'ok')::boolean, false) then
    update public.dataset_review_submit_requests
      set status = 'submitted',
          result = v_submit_result->'data',
          last_error_code = null,
          last_error_message = null,
          last_error_details = null,
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    insert into public.command_audit_log (
      command,
      actor_user_id,
      target_table,
      target_id,
      target_version,
      payload
    )
    values (
      'cmd_review_submit_from_job',
      v_job.requested_by,
      v_job.dataset_table,
      v_job.dataset_id,
      v_job.dataset_version,
      coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
        'review_submit_job_id', v_job.id,
        'review_submit_request_id', v_job.id,
        'gate_run_id', v_job.gate_run_id,
        'gate_worker_job_id', v_job.gate_worker_job_id
      )
    );

    return jsonb_build_object(
      'ok', true,
      'data', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  v_error_code := coalesce(v_submit_result->>'code', 'REVIEW_SUBMIT_JOB_ERROR');
  v_error_status := coalesce(nullif(v_submit_result->>'status', '')::integer, 500);
  v_error_message := coalesce(v_submit_result->>'message', 'Review-submit job failed');
  v_job_status := case
    when v_error_code = 'REVIEW_SUBMIT_GATE_NOT_READY' then 'waiting_gate'
    when v_error_code = 'REVIEW_SUBMIT_GATE_BLOCKED' then 'blocked'
    when v_error_code in ('REVIEW_SUBMIT_GATE_STALE', 'REVIEW_SUBMIT_JOB_STALE') then 'stale'
    else 'error'
  end;

  update public.dataset_review_submit_requests
    set status = v_job_status,
        last_error_code = case when v_job_status = 'waiting_gate' then null else v_error_code end,
        last_error_message = case when v_job_status = 'waiting_gate' then null else v_error_message end,
        last_error_details = case
          when v_job_status = 'waiting_gate' then null
          else jsonb_build_object('submitResult', v_submit_result)
        end,
        modified_at = now(),
        completed_at = case
          when v_job_status = 'waiting_gate' then null
          else now()
        end
  where id = v_job.id
  returning *
    into v_job;

  return jsonb_build_object(
    'ok', false,
    'code', v_error_code,
    'status', v_error_status,
    'message', v_error_message,
    'details', public.cmd_dataset_review_submit_job_payload(v_job)
  );
exception
  when others then
    perform set_config('request.jwt.claim.sub', coalesce(v_prev_sub, ''), true);
    perform set_config('request.jwt.claim.role', coalesce(v_prev_role, ''), true);
    perform set_config('request.jwt.claims', coalesce(v_prev_claims, ''), true);
    raise;
end;
$_$;


ALTER FUNCTION "public"."cmd_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."cmd_review_submit_without_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
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


ALTER FUNCTION "public"."cmd_review_submit_without_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."dataset_review_submit_requests_assign_submit_worker_job"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_kind public.worker_job_kinds%rowtype;
  v_existing public.worker_jobs%rowtype;
  v_worker_job public.worker_jobs%rowtype;
  v_payload jsonb;
  v_idempotency_key text;
  v_concurrency_key text;
begin
  if new.submit_worker_job_id is not null then
    return new;
  end if;

  select *
    into v_kind
  from public.worker_job_kinds
  where job_kind = 'review_submit.submit';

  if v_kind.job_kind is null then
    raise exception
      using
        errcode = 'P0001',
        message = 'review_submit.submit worker job kind is not registered';
  end if;

  v_payload := jsonb_build_object(
    'datasetRevision', jsonb_build_object(
      'table', new.dataset_table,
      'id', new.dataset_id,
      'version', new.dataset_version,
      'revisionChecksum', new.revision_checksum
    ),
    'policy', jsonb_build_object(
      'profile', new.policy_profile,
      'reportSchemaVersion', new.report_schema_version
    ),
    'requestedBy', new.requested_by,
    'reviewSubmitJobId', new.id
  );
  v_idempotency_key := concat_ws(
    ':',
    'review_submit.submit',
    new.dataset_table,
    new.dataset_id::text,
    new.dataset_version,
    new.revision_checksum,
    new.policy_profile,
    new.report_schema_version,
    new.requested_by::text
  );
  v_concurrency_key := concat_ws(
    ':',
    'review_submit.submit',
    new.dataset_table,
    new.dataset_id::text,
    new.dataset_version,
    new.requested_by::text
  );

  select *
    into v_existing
  from public.worker_jobs
  where worker_runtime = v_kind.worker_runtime
    and job_kind = v_kind.job_kind
    and requested_by is not distinct from new.requested_by
    and idempotency_key = v_idempotency_key
    and status in ('queued', 'running', 'waiting', 'stale', 'blocked')
  order by created_at desc
  limit 1
  for update;

  if v_existing.id is not null then
    new.submit_worker_job_id := v_existing.id;
    return new;
  end if;

  select *
    into v_existing
  from public.worker_jobs
  where worker_runtime = v_kind.worker_runtime
    and worker_queue = v_kind.worker_queue
    and concurrency_key = v_concurrency_key
    and status in ('queued', 'running', 'waiting', 'stale')
  order by created_at desc
  limit 1
  for update;

  if v_existing.id is not null then
    raise exception
      using
        errcode = 'P0001',
        message = 'conflicting active review-submit root worker job exists',
        detail = jsonb_build_object(
          'existingWorkerJobId', v_existing.id,
          'concurrencyKey', v_concurrency_key
        )::text;
  end if;

  insert into public.worker_jobs (
    job_kind,
    worker_runtime,
    worker_queue,
    priority,
    subject_type,
    subject_id,
    subject_version,
    requester_type,
    requested_by,
    idempotency_key,
    request_hash,
    concurrency_key,
    status,
    phase,
    progress,
    visibility,
    max_attempts,
    payload_schema_version,
    payload_json,
    result_schema_version,
    result_ref
  ) values (
    v_kind.job_kind,
    v_kind.worker_runtime,
    v_kind.worker_queue,
    v_kind.default_priority,
    new.dataset_table,
    new.dataset_id,
    new.dataset_version,
    'user',
    new.requested_by,
    v_idempotency_key,
    new.revision_checksum,
    v_concurrency_key,
    'waiting',
    new.status,
    0,
    v_kind.default_visibility,
    greatest(1, v_kind.default_max_attempts),
    v_kind.payload_schema_version,
    v_payload,
    v_kind.result_schema_version,
    jsonb_build_object(
      'domainSource', 'dataset_review_submit_requests',
      'reviewSubmitJobId', new.id
    )
  )
  returning *
    into v_worker_job;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    phase,
    progress,
    details
  ) values (
    v_worker_job.id,
    'enqueued',
    v_worker_job.status,
    v_worker_job.phase,
    v_worker_job.progress,
    jsonb_build_object(
      'jobKind', v_worker_job.job_kind,
      'workerQueue', v_worker_job.worker_queue,
      'idempotencyKey', v_worker_job.idempotency_key,
      'concurrencyKey', v_worker_job.concurrency_key,
      'source', 'dataset_review_submit_requests_assign_submit_worker_job'
    )
  );

  new.submit_worker_job_id := v_worker_job.id;
  return new;
end;
$$;


ALTER FUNCTION "public"."dataset_review_submit_requests_assign_submit_worker_job"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."dataset_review_submit_requests_sync_submit_worker_job"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_worker_status text;
  v_progress numeric;
  v_result_json jsonb;
  v_result_ref jsonb;
  v_error_code text;
  v_error_message text;
  v_error_details jsonb;
  v_blocker_codes text[];
  v_resolution_scope text;
begin
  if new.submit_worker_job_id is null then
    return new;
  end if;

  v_worker_status := case
    when new.status in ('queued', 'waiting_gate', 'submitting') then 'waiting'
    when new.status = 'submitted' then 'completed'
    when new.status = 'blocked' then 'blocked'
    when new.status = 'stale' then 'stale'
    when new.status = 'cancelled' then 'cancelled'
    else 'failed'
  end;

  v_progress := case
    when new.status = 'queued' then 0
    when new.status = 'waiting_gate' then 0.25
    when new.status = 'submitting' then 0.75
    when new.status = 'submitted' then 1
    else null
  end;

  v_result_ref := jsonb_build_object(
    'domainSource', 'dataset_review_submit_requests',
    'reviewSubmitJobId', new.id
  );

  v_result_json := case
    when new.status = 'submitted' then jsonb_strip_nulls(
      jsonb_build_object(
        'status', 'submitted',
        'reviewSubmitJobId', new.id,
        'datasetRevision', jsonb_build_object(
          'table', new.dataset_table,
          'id', new.dataset_id,
          'version', new.dataset_version,
          'revisionChecksum', new.revision_checksum
        ),
        'result', new.result
      )
    )
    else null
  end;

  v_error_code := case
    when v_worker_status in ('blocked', 'stale', 'failed', 'cancelled') then
      coalesce(new.last_error_code, 'REVIEW_SUBMIT_JOB_' || upper(new.status))
    else null
  end;
  v_error_message := case
    when v_worker_status in ('blocked', 'stale', 'failed', 'cancelled') then
      coalesce(new.last_error_message, 'Review-submit job status is ' || new.status)
    else null
  end;
  v_error_details := case
    when v_worker_status in ('blocked', 'stale', 'failed', 'cancelled') then
      coalesce(new.last_error_details, '{}'::jsonb)
    else null
  end;
  v_blocker_codes := case
    when v_worker_status = 'blocked' then array[coalesce(new.last_error_code, 'REVIEW_SUBMIT_GATE_BLOCKED')]
    else '{}'::text[]
  end;
  v_resolution_scope := case
    when v_worker_status = 'blocked' then 'user'
    else null
  end;

  update public.worker_jobs
    set status = v_worker_status,
        phase = new.status,
        progress = v_progress,
        result_json = case
          when v_worker_status = 'completed' then v_result_json
          else result_json
        end,
        result_ref = coalesce(result_ref, '{}'::jsonb) || v_result_ref,
        error_code = v_error_code,
        error_message = v_error_message,
        error_details = v_error_details,
        blocker_codes = v_blocker_codes,
        resolution_scope = v_resolution_scope,
        retryable = case
          when v_worker_status in ('failed', 'stale') then true
          when v_worker_status in ('completed', 'blocked', 'cancelled') then false
          else null
        end,
        updated_at = now(),
        finished_at = case
          when v_worker_status in ('completed', 'blocked', 'stale', 'failed', 'cancelled') then coalesce(finished_at, now())
          else null
        end,
        cancelled_at = case
          when v_worker_status = 'cancelled' then coalesce(cancelled_at, now())
          else cancelled_at
        end
  where id = new.submit_worker_job_id;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    phase,
    progress,
    message,
    details
  )
  select
    new.submit_worker_job_id,
    'review_submit_status_synced',
    v_worker_status,
    new.status,
    v_progress,
    v_error_message,
    jsonb_strip_nulls(
      jsonb_build_object(
        'reviewSubmitJobId', new.id,
        'errorCode', v_error_code,
        'blockerCodes', to_jsonb(v_blocker_codes),
        'resolutionScope', v_resolution_scope
      )
    );

  return new;
end;
$$;


ALTER FUNCTION "public"."dataset_review_submit_requests_sync_submit_worker_job"() OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."get_latest_contact_versions"("page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "sort_by" "text" DEFAULT 'modified_at'::"text", "sort_direction" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_sort_by text;
  normalized_sort_direction text;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_sort_by := lower(coalesce(sort_by, 'modified_at'));
  normalized_sort_direction := lower(coalesce(sort_direction, 'desc'));

  RETURN QUERY
    WITH visible_rows AS (
      SELECT f.*
      FROM public.contacts f
      WHERE
        (
          (data_source = 'tg' AND f.state_code = 100)
          OR (data_source = 'co' AND f.state_code = 200)
          OR (data_source = 'my' AND f.user_id::text = this_user_id)
          OR (data_source = 'te' AND team_id_filter IS NOT NULL AND f.team_id = team_id_filter)
        )
        AND (team_id_filter IS NULL OR data_source NOT IN ('tg', 'co') OR f.team_id = team_id_filter)
        AND (state_code_filter IS NULL OR data_source NOT IN ('my', 'te') OR f.state_code = state_code_filter)
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.created_at,
        visible_rows.modified_at,
        visible_rows.team_id
      FROM visible_rows
      ORDER BY visible_rows.id, visible_rows.version DESC, visible_rows.modified_at DESC
    ),
    counted_rows AS (
      SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
      FROM latest_rows
    )
    SELECT
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.team_id,
      counted_rows.total_count
    FROM counted_rows
    ORDER BY
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN counted_rows.version
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN counted_rows.version
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN counted_rows.created_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.created_at
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN counted_rows.modified_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.modified_at
      END DESC NULLS LAST,
      counted_rows.id
    LIMIT normalized_page_size
    OFFSET (normalized_page_current - 1) * normalized_page_size;
END;
$$;


ALTER FUNCTION "public"."get_latest_contact_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_latest_flow_versions"("page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "sort_by" "text" DEFAULT 'modified_at'::"text", "sort_direction" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $_$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_sort_by text;
  normalized_sort_direction text;
  normalized_this_user_id uuid;
  filter_condition_jsonb jsonb;
  flow_type text;
  flow_type_array text[];
  as_input boolean;
  classification_filter jsonb;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_sort_by := lower(coalesce(sort_by, 'modified_at'));
  normalized_sort_direction := lower(coalesce(sort_direction, 'desc'));
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);

  flow_type := nullif(btrim(filter_condition_jsonb->>'flowType'), '');
  IF flow_type IS NOT NULL THEN
    flow_type_array := string_to_array(flow_type, ',');
  ELSE
    flow_type_array := NULL;
  END IF;
  filter_condition_jsonb := filter_condition_jsonb - 'flowType';

  IF filter_condition_jsonb ? 'asInput' THEN
    as_input := nullif(btrim(filter_condition_jsonb->>'asInput'), '')::boolean;
  ELSE
    as_input := NULL;
  END IF;
  filter_condition_jsonb := filter_condition_jsonb - 'asInput';

  IF jsonb_typeof(filter_condition_jsonb->'classification') = 'array' THEN
    classification_filter := filter_condition_jsonb->'classification';
  ELSE
    classification_filter := '[]'::jsonb;
  END IF;
  filter_condition_jsonb := filter_condition_jsonb - 'classification';

  IF filter_condition_jsonb = '{}'::jsonb
    AND flow_type IS NULL
    AND as_input IS NULL
    AND jsonb_array_length(classification_filter) = 0
  THEN
    RETURN QUERY
      WITH visible_keys AS (
        SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
        FROM public.flows f
        WHERE data_source = 'tg'
          AND f.state_code = 100
          AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
        UNION ALL
        SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
        FROM public.flows f
        WHERE data_source = 'co'
          AND f.state_code = 200
          AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
        UNION ALL
        SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
        FROM public.flows f
        WHERE data_source = 'my'
          AND normalized_this_user_id IS NOT NULL
          AND f.user_id = normalized_this_user_id
          AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
        UNION ALL
        SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
        FROM public.flows f
        WHERE data_source = 'te'
          AND team_id_filter IS NOT NULL
          AND f.team_id = team_id_filter
          AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
      ),
      latest_keys AS (
        SELECT DISTINCT ON (visible_keys.id)
          visible_keys.id,
          visible_keys.version,
          visible_keys.created_at,
          visible_keys.modified_at,
          visible_keys.team_id
        FROM visible_keys
        ORDER BY visible_keys.id, visible_keys.version DESC, visible_keys.modified_at DESC
      ),
      counted_keys AS (
        SELECT latest_keys.*, count(*) OVER()::bigint AS total_count
        FROM latest_keys
      ),
      paged_keys AS (
        SELECT counted_keys.*
        FROM counted_keys
        ORDER BY
          CASE
            WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN counted_keys.version
          END ASC NULLS LAST,
          CASE
            WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN counted_keys.version
          END DESC NULLS LAST,
          CASE
            WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN counted_keys.created_at
          END ASC NULLS LAST,
          CASE
            WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN counted_keys.created_at
          END DESC NULLS LAST,
          CASE
            WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN counted_keys.modified_at
          END ASC NULLS LAST,
          CASE
            WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN counted_keys.modified_at
          END DESC NULLS LAST,
          counted_keys.id
        LIMIT normalized_page_size
        OFFSET (normalized_page_current - 1) * normalized_page_size
      )
      SELECT
        payload.id,
        payload.json,
        payload.version,
        payload.modified_at,
        payload.team_id,
        paged_keys.total_count
      FROM paged_keys
      JOIN public.flows payload
        ON payload.id = paged_keys.id
       AND payload.version = paged_keys.version
      ORDER BY
        CASE
          WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN paged_keys.version
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN paged_keys.version
        END DESC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN paged_keys.created_at
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN paged_keys.created_at
        END DESC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN paged_keys.modified_at
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN paged_keys.modified_at
        END DESC NULLS LAST,
        paged_keys.id;
    RETURN;
  END IF;

  RETURN QUERY
    WITH visible_rows AS (
      SELECT f.*
      FROM public.flows f
      WHERE data_source = 'tg'
        AND f.state_code = 100
        AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
      UNION ALL
      SELECT f.*
      FROM public.flows f
      WHERE data_source = 'co'
        AND f.state_code = 200
        AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
      UNION ALL
      SELECT f.*
      FROM public.flows f
      WHERE data_source = 'my'
        AND normalized_this_user_id IS NOT NULL
        AND f.user_id = normalized_this_user_id
        AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
      UNION ALL
      SELECT f.*
      FROM public.flows f
      WHERE data_source = 'te'
        AND team_id_filter IS NOT NULL
        AND f.team_id = team_id_filter
        AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
    ),
    matched_ids AS (
      SELECT DISTINCT visible_rows.id
      FROM visible_rows
      WHERE visible_rows.json @> filter_condition_jsonb
        AND (
          flow_type IS NULL
          OR (visible_rows.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}') = ANY(flow_type_array)
        )
        AND (
          as_input IS NULL
          OR as_input = false
          OR NOT (
            visible_rows.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
          )
        )
        AND (
          jsonb_array_length(classification_filter) = 0
          OR EXISTS (
            SELECT 1
            FROM jsonb_array_elements(classification_filter) AS selected_class(item)
            WHERE
              (
                selected_class.item->>'scope' = 'elementary'
                AND EXISTS (
                  SELECT 1
                  FROM jsonb_array_elements(
                    CASE jsonb_typeof(visible_rows.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                      WHEN 'array' THEN visible_rows.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}'
                      WHEN 'object' THEN jsonb_build_array(visible_rows.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                      ELSE '[]'::jsonb
                    END
                  ) AS category(item)
                  WHERE category.item->>'@catId' = selected_class.item->>'code'
                )
              )
              OR (
                selected_class.item->>'scope' = 'classification'
                AND EXISTS (
                  SELECT 1
                  FROM jsonb_array_elements(
                    CASE jsonb_typeof(visible_rows.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                      WHEN 'array' THEN visible_rows.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}'
                      WHEN 'object' THEN jsonb_build_array(visible_rows.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                      ELSE '[]'::jsonb
                    END
                  ) AS class_item(item)
                  WHERE class_item.item->>'@classId' = selected_class.item->>'code'
                )
              )
          )
        )
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.created_at,
        visible_rows.modified_at,
        visible_rows.team_id
      FROM visible_rows
      JOIN matched_ids ON matched_ids.id = visible_rows.id
      ORDER BY visible_rows.id, visible_rows.version DESC, visible_rows.modified_at DESC
    ),
    counted_rows AS (
      SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
      FROM latest_rows
    )
    SELECT
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.team_id,
      counted_rows.total_count
    FROM counted_rows
    ORDER BY
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN counted_rows.version
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN counted_rows.version
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN counted_rows.created_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.created_at
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN counted_rows.modified_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.modified_at
      END DESC NULLS LAST,
      counted_rows.id
    LIMIT normalized_page_size
    OFFSET (normalized_page_current - 1) * normalized_page_size;
END;
$_$;


ALTER FUNCTION "public"."get_latest_flow_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "filter_condition" "jsonb", "sort_by" "text", "sort_direction" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_latest_flowproperty_versions"("page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "sort_by" "text" DEFAULT 'modified_at'::"text", "sort_direction" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_sort_by text;
  normalized_sort_direction text;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_sort_by := lower(coalesce(sort_by, 'modified_at'));
  normalized_sort_direction := lower(coalesce(sort_direction, 'desc'));

  RETURN QUERY
    WITH visible_rows AS (
      SELECT f.*
      FROM public.flowproperties f
      WHERE
        (
          (data_source = 'tg' AND f.state_code = 100)
          OR (data_source = 'co' AND f.state_code = 200)
          OR (data_source = 'my' AND f.user_id::text = this_user_id)
          OR (data_source = 'te' AND team_id_filter IS NOT NULL AND f.team_id = team_id_filter)
        )
        AND (team_id_filter IS NULL OR data_source NOT IN ('tg', 'co') OR f.team_id = team_id_filter)
        AND (state_code_filter IS NULL OR data_source NOT IN ('my', 'te') OR f.state_code = state_code_filter)
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.created_at,
        visible_rows.modified_at,
        visible_rows.team_id
      FROM visible_rows
      ORDER BY visible_rows.id, visible_rows.version DESC, visible_rows.modified_at DESC
    ),
    counted_rows AS (
      SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
      FROM latest_rows
    )
    SELECT
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.team_id,
      counted_rows.total_count
    FROM counted_rows
    ORDER BY
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN counted_rows.version
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN counted_rows.version
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN counted_rows.created_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.created_at
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN counted_rows.modified_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.modified_at
      END DESC NULLS LAST,
      counted_rows.id
    LIMIT normalized_page_size
    OFFSET (normalized_page_current - 1) * normalized_page_size;
END;
$$;


ALTER FUNCTION "public"."get_latest_flowproperty_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_latest_lifecyclemodel_versions"("page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "sort_by" "text" DEFAULT 'modified_at'::"text", "sort_direction" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $_$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_sort_by text;
  normalized_sort_direction text;
  normalized_this_user_id uuid;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_sort_by := lower(coalesce(sort_by, 'modified_at'));
  normalized_sort_direction := lower(coalesce(sort_direction, 'desc'));
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;

  RETURN QUERY
    WITH visible_rows AS (
      SELECT l.*
      FROM public.lifecyclemodels l
      WHERE data_source = 'tg'
        AND l.state_code = 100
        AND (team_id_filter IS NULL OR l.team_id = team_id_filter)
      UNION ALL
      SELECT l.*
      FROM public.lifecyclemodels l
      WHERE data_source = 'co'
        AND l.state_code = 200
        AND (team_id_filter IS NULL OR l.team_id = team_id_filter)
      UNION ALL
      SELECT l.*
      FROM public.lifecyclemodels l
      WHERE data_source = 'my'
        AND normalized_this_user_id IS NOT NULL
        AND l.user_id = normalized_this_user_id
        AND (state_code_filter IS NULL OR l.state_code = state_code_filter)
      UNION ALL
      SELECT l.*
      FROM public.lifecyclemodels l
      WHERE data_source = 'te'
        AND team_id_filter IS NOT NULL
        AND l.team_id = team_id_filter
        AND (state_code_filter IS NULL OR l.state_code = state_code_filter)
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.created_at,
        visible_rows.modified_at,
        visible_rows.team_id
      FROM visible_rows
      ORDER BY visible_rows.id, visible_rows.version DESC, visible_rows.modified_at DESC
    ),
    counted_rows AS (
      SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
      FROM latest_rows
    )
    SELECT
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.team_id,
      counted_rows.total_count
    FROM counted_rows
    ORDER BY
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN counted_rows.version
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN counted_rows.version
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN counted_rows.created_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.created_at
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN counted_rows.modified_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.modified_at
      END DESC NULLS LAST,
      counted_rows.id
    LIMIT normalized_page_size
    OFFSET (normalized_page_current - 1) * normalized_page_size;
END;
$_$;


ALTER FUNCTION "public"."get_latest_lifecyclemodel_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_latest_process_versions"("page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "type_of_data_set_filter" "text" DEFAULT 'all'::"text", "sort_by" "text" DEFAULT 'modified_at'::"text", "sort_direction" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "model_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $_$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_sort_by text;
  normalized_sort_direction text;
  normalized_this_user_id uuid;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_sort_by := lower(coalesce(sort_by, 'modified_at'));
  normalized_sort_direction := lower(coalesce(sort_direction, 'desc'));
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;

  RETURN QUERY
    WITH visible_rows AS (
      SELECT p.*
      FROM public.processes p
      WHERE data_source = 'tg'
        AND p.state_code = 100
        AND (team_id_filter IS NULL OR p.team_id = team_id_filter)
      UNION ALL
      SELECT p.*
      FROM public.processes p
      WHERE data_source = 'co'
        AND p.state_code = 200
        AND (team_id_filter IS NULL OR p.team_id = team_id_filter)
      UNION ALL
      SELECT p.*
      FROM public.processes p
      WHERE data_source = 'my'
        AND normalized_this_user_id IS NOT NULL
        AND p.user_id = normalized_this_user_id
        AND (state_code_filter IS NULL OR p.state_code = state_code_filter)
      UNION ALL
      SELECT p.*
      FROM public.processes p
      WHERE data_source = 'te'
        AND team_id_filter IS NOT NULL
        AND p.team_id = team_id_filter
        AND (state_code_filter IS NULL OR p.state_code = state_code_filter)
    ),
    matched_ids AS (
      SELECT DISTINCT visible_rows.id
      FROM visible_rows
      WHERE
        coalesce(type_of_data_set_filter, 'all') = 'all'
        OR visible_rows.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = type_of_data_set_filter
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.created_at,
        visible_rows.modified_at,
        visible_rows.team_id,
        visible_rows.model_id
      FROM visible_rows
      JOIN matched_ids ON matched_ids.id = visible_rows.id
      ORDER BY visible_rows.id, visible_rows.version DESC, visible_rows.modified_at DESC
    ),
    counted_rows AS (
      SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
      FROM latest_rows
    )
    SELECT
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.team_id,
      counted_rows.model_id,
      counted_rows.total_count
    FROM counted_rows
    ORDER BY
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN counted_rows.version
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN counted_rows.version
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN counted_rows.created_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.created_at
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN counted_rows.modified_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.modified_at
      END DESC NULLS LAST,
      counted_rows.id
    LIMIT normalized_page_size
    OFFSET (normalized_page_current - 1) * normalized_page_size;
END;
$_$;


ALTER FUNCTION "public"."get_latest_process_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "sort_by" "text", "sort_direction" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_latest_source_versions"("page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "sort_by" "text" DEFAULT 'modified_at'::"text", "sort_direction" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $_$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_sort_by text;
  normalized_sort_direction text;
  normalized_this_user_id uuid;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_sort_by := lower(coalesce(sort_by, 'modified_at'));
  normalized_sort_direction := lower(coalesce(sort_direction, 'desc'));
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;

  RETURN QUERY
    WITH visible_keys AS (
      SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
      FROM public.sources f
      WHERE data_source = 'tg'
        AND f.state_code = 100
        AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
      UNION ALL
      SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
      FROM public.sources f
      WHERE data_source = 'co'
        AND f.state_code = 200
        AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
      UNION ALL
      SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
      FROM public.sources f
      WHERE data_source = 'my'
        AND normalized_this_user_id IS NOT NULL
        AND f.user_id = normalized_this_user_id
        AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
      UNION ALL
      SELECT f.id, f.version, f.created_at, f.modified_at, f.team_id
      FROM public.sources f
      WHERE data_source = 'te'
        AND team_id_filter IS NOT NULL
        AND f.team_id = team_id_filter
        AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
    ),
    latest_keys AS (
      SELECT DISTINCT ON (visible_keys.id)
        visible_keys.id,
        visible_keys.version,
        visible_keys.created_at,
        visible_keys.modified_at,
        visible_keys.team_id
      FROM visible_keys
      ORDER BY visible_keys.id, visible_keys.version DESC, visible_keys.modified_at DESC
    ),
    counted_keys AS (
      SELECT latest_keys.*, count(*) OVER()::bigint AS total_count
      FROM latest_keys
    ),
    paged_keys AS (
      SELECT counted_keys.*
      FROM counted_keys
      ORDER BY
        CASE
          WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN counted_keys.version
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN counted_keys.version
        END DESC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN counted_keys.created_at
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN counted_keys.created_at
        END DESC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN counted_keys.modified_at
        END ASC NULLS LAST,
        CASE
          WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN counted_keys.modified_at
        END DESC NULLS LAST,
        counted_keys.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size
    )
    SELECT
      payload.id,
      payload.json,
      payload.version,
      payload.modified_at,
      payload.team_id,
      paged_keys.total_count
    FROM paged_keys
    JOIN public.sources payload
      ON payload.id = paged_keys.id
     AND payload.version = paged_keys.version
    ORDER BY
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN paged_keys.version
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN paged_keys.version
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN paged_keys.created_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN paged_keys.created_at
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN paged_keys.modified_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN paged_keys.modified_at
      END DESC NULLS LAST,
      paged_keys.id;
END;
$_$;


ALTER FUNCTION "public"."get_latest_source_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_latest_unitgroup_versions"("page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "sort_by" "text" DEFAULT 'modified_at'::"text", "sort_direction" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_sort_by text;
  normalized_sort_direction text;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_sort_by := lower(coalesce(sort_by, 'modified_at'));
  normalized_sort_direction := lower(coalesce(sort_direction, 'desc'));

  RETURN QUERY
    WITH visible_rows AS (
      SELECT f.*
      FROM public.unitgroups f
      WHERE
        (
          (data_source = 'tg' AND f.state_code = 100)
          OR (data_source = 'co' AND f.state_code = 200)
          OR (data_source = 'my' AND f.user_id::text = this_user_id)
          OR (data_source = 'te' AND team_id_filter IS NOT NULL AND f.team_id = team_id_filter)
        )
        AND (team_id_filter IS NULL OR data_source NOT IN ('tg', 'co') OR f.team_id = team_id_filter)
        AND (state_code_filter IS NULL OR data_source NOT IN ('my', 'te') OR f.state_code = state_code_filter)
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.created_at,
        visible_rows.modified_at,
        visible_rows.team_id
      FROM visible_rows
      ORDER BY visible_rows.id, visible_rows.version DESC, visible_rows.modified_at DESC
    ),
    counted_rows AS (
      SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
      FROM latest_rows
    )
    SELECT
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.team_id,
      counted_rows.total_count
    FROM counted_rows
    ORDER BY
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction = 'asc' THEN counted_rows.version
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'version' AND normalized_sort_direction <> 'asc' THEN counted_rows.version
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction = 'asc' THEN counted_rows.created_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'created_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.created_at
      END DESC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction = 'asc' THEN counted_rows.modified_at
      END ASC NULLS LAST,
      CASE
        WHEN normalized_sort_by = 'modified_at' AND normalized_sort_direction <> 'asc' THEN counted_rows.modified_at
      END DESC NULLS LAST,
      counted_rows.id
    LIMIT normalized_page_size
    OFFSET (normalized_page_current - 1) * normalized_page_size;
END;
$$;


ALTER FUNCTION "public"."get_latest_unitgroup_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."hybrid_search_flows"("query_text" "text", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "full_text_weight" double precision DEFAULT 0.3, "extracted_text_weight" double precision DEFAULT 0.2, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1) RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "statement_timeout" TO '60s'
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
declare
  candidate_limit integer;
  semantic_match_count integer;
  filter_condition_jsonb jsonb;
  text_weight double precision;
begin
  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;
  semantic_match_count := greatest(coalesce(match_count, 20), coalesce(page_size, 10));
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  text_weight := coalesce(full_text_weight, 0) + coalesce(extracted_text_weight, 0);

  return query
    with text_matches as (
      select ts.rank as text_rank, ts.id as text_id
      from public.search_flows_latest(
        query_text,
        filter_condition_jsonb,
        '{}'::jsonb,
        candidate_limit,
        1,
        data_source,
        '',
        null::uuid,
        null::integer
      ) ts
    ),
    semantic as (
      select ss.rank as ss_rank, ss.id as ss_id
      from private.semantic_flow_candidates(
        query_embedding,
        filter_condition,
        match_threshold,
        semantic_match_count,
        data_source
      ) ss
    ),
    fused_raw as (
      select
        coalesce(text_matches.text_id, semantic.ss_id) as id,
        coalesce(1.0 / (rrf_k + text_matches.text_rank), 0.0) * text_weight
          + coalesce(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight as score
      from text_matches
      full outer join semantic on text_matches.text_id = semantic.ss_id
    ),
    fused as (
      select fused_raw.id, sum(fused_raw.score) as score
      from fused_raw
      where fused_raw.id is not null
      group by fused_raw.id
    ),
    visible_rows as (
      select f.*
      from public.flows f
      join fused on fused.id = f.id
      where (
        (data_source = 'tg' and f.state_code = 100)
        or (data_source = 'co' and f.state_code = 200)
        or (data_source = 'my' and f.user_id = auth.uid())
        or (
          data_source = 'te'
          and exists (
            select 1
            from public.roles r
            where r.user_id = auth.uid()
              and r.team_id = f.team_id
              and r.role::text in ('admin', 'member', 'owner')
          )
        )
      )
    ),
    latest_rows as (
      select distinct on (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.team_id,
        fused.score
      from visible_rows
      join fused on fused.id = visible_rows.id
      order by visible_rows.id, visible_rows.version desc, visible_rows.modified_at desc
    ),
    counted_rows as (
      select latest_rows.*, count(*) over()::bigint as total_count
      from latest_rows
    )
    select
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.team_id,
      counted_rows.total_count
    from counted_rows
    order by counted_rows.score desc, counted_rows.modified_at desc, counted_rows.id
    limit greatest(coalesce(page_size, 10), 1)
    offset (greatest(coalesce(page_current, 1), 1) - 1) * greatest(coalesce(page_size, 10), 1);
end;
$$;


ALTER FUNCTION "public"."hybrid_search_flows"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."hybrid_search_lifecyclemodels"("query_text" "text", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "full_text_weight" double precision DEFAULT 0.3, "extracted_text_weight" double precision DEFAULT 0.2, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1) RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "statement_timeout" TO '60s'
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
declare
  candidate_limit integer;
  semantic_match_count integer;
  filter_condition_jsonb jsonb;
  text_weight double precision;
begin
  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;
  semantic_match_count := greatest(coalesce(match_count, 20), coalesce(page_size, 10));
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  text_weight := coalesce(full_text_weight, 0) + coalesce(extracted_text_weight, 0);

  return query
    with text_matches as (
      select ts.rank as text_rank, ts.id as text_id
      from public.search_lifecyclemodels_latest(
        query_text,
        filter_condition_jsonb,
        '{}'::jsonb,
        candidate_limit,
        1,
        data_source,
        '',
        null::uuid,
        null::integer
      ) ts
    ),
    semantic as (
      select ss.rank as ss_rank, ss.id as ss_id
      from private.semantic_lifecyclemodel_candidates(
        query_embedding,
        filter_condition,
        match_threshold,
        semantic_match_count,
        data_source
      ) ss
    ),
    fused_raw as (
      select
        coalesce(text_matches.text_id, semantic.ss_id) as id,
        coalesce(1.0 / (rrf_k + text_matches.text_rank), 0.0) * text_weight
          + coalesce(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight as score
      from text_matches
      full outer join semantic on text_matches.text_id = semantic.ss_id
    ),
    fused as (
      select fused_raw.id, sum(fused_raw.score) as score
      from fused_raw
      where fused_raw.id is not null
      group by fused_raw.id
    ),
    visible_rows as (
      select l.*
      from public.lifecyclemodels l
      join fused on fused.id = l.id
      where (
        (data_source = 'tg' and l.state_code = 100)
        or (data_source = 'co' and l.state_code = 200)
        or (data_source = 'my' and l.user_id = auth.uid())
        or (
          data_source = 'te'
          and exists (
            select 1
            from public.roles r
            where r.user_id = auth.uid()
              and r.team_id = l.team_id
              and r.role::text in ('admin', 'member', 'owner')
          )
        )
      )
    ),
    latest_rows as (
      select distinct on (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.team_id,
        fused.score
      from visible_rows
      join fused on fused.id = visible_rows.id
      order by visible_rows.id, visible_rows.version desc, visible_rows.modified_at desc
    ),
    counted_rows as (
      select latest_rows.*, count(*) over()::bigint as total_count
      from latest_rows
    )
    select
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.team_id,
      counted_rows.total_count
    from counted_rows
    order by counted_rows.score desc, counted_rows.modified_at desc, counted_rows.id
    limit greatest(coalesce(page_size, 10), 1)
    offset (greatest(coalesce(page_current, 1), 1) - 1) * greatest(coalesce(page_size, 10), 1);
end;
$$;


ALTER FUNCTION "public"."hybrid_search_lifecyclemodels"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."hybrid_search_processes"("query_text" "text", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "full_text_weight" double precision DEFAULT 0.3, "extracted_text_weight" double precision DEFAULT 0.2, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1) RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "model_id" "uuid", "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "statement_timeout" TO '60s'
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
declare
  candidate_limit integer;
  semantic_match_count integer;
  filter_condition_jsonb jsonb;
  text_weight double precision;
begin
  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;
  semantic_match_count := greatest(coalesce(match_count, 20), coalesce(page_size, 10));
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  text_weight := coalesce(full_text_weight, 0) + coalesce(extracted_text_weight, 0);

  return query
    with text_matches as (
      select ts.rank as text_rank, ts.id as text_id
      from public.search_processes_latest(
        query_text,
        filter_condition_jsonb,
        '{}'::jsonb,
        candidate_limit,
        1,
        data_source,
        '',
        null::uuid,
        null::integer,
        'all'
      ) ts
    ),
    semantic as (
      select ss.rank as ss_rank, ss.id as ss_id
      from private.semantic_process_candidates(
        query_embedding,
        filter_condition,
        match_threshold,
        semantic_match_count,
        data_source
      ) ss
    ),
    fused_raw as (
      select
        coalesce(text_matches.text_id, semantic.ss_id) as id,
        coalesce(1.0 / (rrf_k + text_matches.text_rank), 0.0) * text_weight
          + coalesce(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight as score
      from text_matches
      full outer join semantic on text_matches.text_id = semantic.ss_id
    ),
    fused as (
      select fused_raw.id, sum(fused_raw.score) as score
      from fused_raw
      where fused_raw.id is not null
      group by fused_raw.id
    ),
    visible_rows as (
      select p.*
      from public.processes p
      join fused on fused.id = p.id
      where (
        (data_source = 'tg' and p.state_code = 100)
        or (data_source = 'co' and p.state_code = 200)
        or (data_source = 'my' and p.user_id = auth.uid())
        or (
          data_source = 'te'
          and exists (
            select 1
            from public.roles r
            where r.user_id = auth.uid()
              and r.team_id = p.team_id
              and r.role::text in ('admin', 'member', 'owner')
          )
        )
      )
    ),
    latest_rows as (
      select distinct on (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.model_id,
        visible_rows.team_id,
        fused.score
      from visible_rows
      join fused on fused.id = visible_rows.id
      order by visible_rows.id, visible_rows.version desc, visible_rows.modified_at desc
    ),
    counted_rows as (
      select latest_rows.*, count(*) over()::bigint as total_count
      from latest_rows
    )
    select
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.model_id,
      counted_rows.team_id,
      counted_rows.total_count
    from counted_rows
    order by counted_rows.score desc, counted_rows.modified_at desc, counted_rows.id
    limit greatest(coalesce(page_size, 10), 1)
    offset (greatest(coalesce(page_current, 1), 1) - 1) * greatest(coalesce(page_size, 10), 1);
end;
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
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
begin
  raise exception
    using
      errcode = 'P0001',
      message = 'legacy lca pgmq enqueue is disabled after worker_jobs cutover',
      detail = jsonb_build_object(
        'queueName', p_queue_name,
        'messageKeys', coalesce(
          (
            select jsonb_agg(key order by key)
            from jsonb_object_keys(coalesce(p_message, '{}'::jsonb)) as keys(key)
          ),
          '[]'::jsonb
        )
      )::text,
      hint = 'Use public.worker_enqueue_job with an lca.* job_kind.';
end;
$$;


ALTER FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") OWNER TO "postgres";


COMMENT ON FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") IS 'Disabled legacy LCA pgmq delivery entrypoint. Use public.worker_enqueue_job for new worker jobs.';



CREATE OR REPLACE FUNCTION "public"."lca_legacy_job_type"("p_job_kind" "text") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select case lower(trim(coalesce(p_job_kind, '')))
    when 'lca.solve_one' then 'solve_one'
    when 'lca.solve_batch' then 'solve_batch'
    when 'lca.solve_all_unit' then 'solve_all_unit'
    when 'lca.build_snapshot' then 'build_snapshot'
    when 'lca.contribution_path' then 'analyze_contribution_path'
    when 'lca.factorization_prepare' then 'prepare_factorization'
    when 'lca.snapshot_gc' then 'snapshot_gc'
    when 'lca.result_gc' then 'result_gc'
    else null
  end
$$;


ALTER FUNCTION "public"."lca_legacy_job_type"("p_job_kind" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."lca_package_enqueue_job"("p_message" "jsonb") RETURNS bigint
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
begin
  raise exception
    using
      errcode = 'P0001',
      message = 'legacy lca package pgmq enqueue is disabled after worker_jobs cutover',
      detail = jsonb_build_object(
        'messageKeys', coalesce(
          (
            select jsonb_agg(key order by key)
            from jsonb_object_keys(coalesce(p_message, '{}'::jsonb)) as keys(key)
          ),
          '[]'::jsonb
        )
      )::text,
      hint = 'Use public.worker_enqueue_job with a tidas.* job_kind.';
end;
$$;


ALTER FUNCTION "public"."lca_package_enqueue_job"("p_message" "jsonb") OWNER TO "postgres";


COMMENT ON FUNCTION "public"."lca_package_enqueue_job"("p_message" "jsonb") IS 'Disabled legacy TIDAS package pgmq delivery entrypoint. Use public.worker_enqueue_job for new worker jobs.';



CREATE OR REPLACE FUNCTION "public"."lca_read_job_projection"("p_requested_by" "uuid", "p_worker_job_id" "uuid" DEFAULT NULL::"uuid", "p_legacy_job_id" "uuid" DEFAULT NULL::"uuid", "p_include_internal" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_job public.worker_jobs%rowtype;
  v_result public.lca_results%rowtype;
  v_legacy_job_id uuid;
  v_snapshot_id text;
begin
  -- Authorization is enforced by EXECUTE grants below; runtime service clients can
  -- call this RPC even when request-header GUCs are not populated by PostgREST.

  if p_worker_job_id is null and p_legacy_job_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_LCA_JOB_LOOKUP',
      'status', 400,
      'message', 'p_worker_job_id or p_legacy_job_id is required'
    );
  end if;

  select *
    into v_job
  from public.worker_jobs as worker_job
  where worker_job.job_kind = any(array[
      'lca.solve_one',
      'lca.solve_batch',
      'lca.solve_all_unit',
      'lca.build_snapshot',
      'lca.contribution_path',
      'lca.factorization_prepare'
    ])
    and (p_requested_by is null or worker_job.requested_by = p_requested_by)
    and (
      (p_worker_job_id is not null and worker_job.id = p_worker_job_id)
      or (
        p_legacy_job_id is not null
        and worker_job.subject_type = 'lca_job'
        and worker_job.subject_id = p_legacy_job_id
      )
      or (
        p_legacy_job_id is not null
        and worker_job.payload_json->>'job_id' = p_legacy_job_id::text
      )
      or (
        p_legacy_job_id is not null
        and worker_job.payload_json->>'lcaJobId' = p_legacy_job_id::text
      )
    )
  order by worker_job.updated_at desc, worker_job.created_at desc
  limit 1;

  if v_job.id is null then
    return jsonb_build_object('ok', true, 'data', null);
  end if;

  v_legacy_job_id := coalesce(
    p_legacy_job_id,
    case when v_job.subject_type = 'lca_job' then v_job.subject_id else null end,
    nullif(v_job.payload_json->>'job_id', '')::uuid,
    nullif(v_job.payload_json->>'lcaJobId', '')::uuid
  );

  select *
    into v_result
  from public.lca_results as result_row
  where result_row.worker_job_id = v_job.id
     or (v_legacy_job_id is not null and result_row.job_id = v_legacy_job_id)
  order by result_row.created_at desc
  limit 1;

  v_snapshot_id := coalesce(
    nullif(v_result.snapshot_id::text, ''),
    nullif(v_job.subject_version, ''),
    nullif(v_job.payload_json->>'snapshot_id', ''),
    nullif(v_job.payload_json->>'snapshotId', '')
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_strip_nulls(
      jsonb_build_object(
        'job', jsonb_strip_nulls(
          jsonb_build_object(
            'workerJobId', v_job.id,
            'legacyJobId', v_legacy_job_id,
            'snapshotId', v_snapshot_id,
            'jobKind', v_job.job_kind,
            'jobType', public.lca_legacy_job_type(v_job.job_kind),
            'status', v_job.status,
            'phase', v_job.phase,
            'progress', v_job.progress,
            'payload', case when p_include_internal then v_job.payload_json else null end,
            'diagnostics', case when p_include_internal then v_job.diagnostics else null end,
            'timestamps', jsonb_strip_nulls(
              jsonb_build_object(
                'createdAt', v_job.created_at,
                'startedAt', v_job.started_at,
                'finishedAt', v_job.finished_at,
                'updatedAt', v_job.updated_at
              )
            )
          )
        ),
        'workerJob', public.worker_job_payload(v_job, p_include_internal),
        'result', case
          when v_result.id is null then null
          else jsonb_strip_nulls(
            jsonb_build_object(
              'resultId', v_result.id,
              'legacyJobId', v_result.job_id,
              'workerJobId', v_result.worker_job_id,
              'snapshotId', v_result.snapshot_id,
              'createdAt', v_result.created_at,
              'diagnostics', v_result.diagnostics,
              'artifact', jsonb_strip_nulls(
                jsonb_build_object(
                  'artifactUrl', v_result.artifact_url,
                  'artifactFormat', v_result.artifact_format,
                  'artifactByteSize', v_result.artifact_byte_size,
                  'artifactSha256', v_result.artifact_sha256
                )
              )
            )
          )
        end
      )
    )
  );
end;
$$;


ALTER FUNCTION "public"."lca_read_job_projection"("p_requested_by" "uuid", "p_worker_job_id" "uuid", "p_legacy_job_id" "uuid", "p_include_internal" boolean) OWNER TO "postgres";


COMMENT ON FUNCTION "public"."lca_read_job_projection"("p_requested_by" "uuid", "p_worker_job_id" "uuid", "p_legacy_job_id" "uuid", "p_include_internal" boolean) IS 'Service-role LCA job/status/result projection backed by worker_jobs and retained result metadata, not public.lca_jobs. Authorization is enforced by EXECUTE grants.';



CREATE OR REPLACE FUNCTION "public"."lca_read_latest_single_solve_result"("p_requested_by" "uuid", "p_snapshot_id" "uuid", "p_process_index" integer) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_cache public.lca_result_cache%rowtype;
  v_job public.worker_jobs%rowtype;
  v_result public.lca_results%rowtype;
  v_amount numeric;
begin
  -- Authorization is enforced by EXECUTE grants below; runtime service clients can
  -- call this RPC even when request-header GUCs are not populated by PostgREST.

  if p_snapshot_id is null or p_process_index is null or p_process_index < 0 then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_LCA_SOLVE_LOOKUP',
      'status', 400,
      'message', 'p_snapshot_id and a non-negative p_process_index are required'
    );
  end if;

  select cache_row.*
    into v_cache
  from public.lca_result_cache as cache_row
  join public.worker_jobs as worker_job
    on worker_job.id = cache_row.worker_job_id
  join public.lca_results as result_row
    on result_row.id = cache_row.result_id
  where cache_row.snapshot_id = p_snapshot_id
    and cache_row.status = 'ready'
    and cache_row.result_id is not null
    and worker_job.job_kind = 'lca.solve_one'
    and (p_requested_by is null or worker_job.requested_by = p_requested_by)
    and cache_row.request_payload->>'demand_mode' = 'single'
    and cache_row.request_payload#>>'{demand,process_index}' ~ '^[0-9]+$'
    and (cache_row.request_payload#>>'{demand,process_index}')::integer = p_process_index
  order by cache_row.updated_at desc, cache_row.created_at desc
  limit 1;

  if v_cache.id is null then
    return jsonb_build_object('ok', true, 'data', null);
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = v_cache.worker_job_id;

  select *
    into v_result
  from public.lca_results
  where id = v_cache.result_id;

  v_amount := case
    when jsonb_typeof(v_cache.request_payload#>'{demand,amount}') = 'number'
      then (v_cache.request_payload#>>'{demand,amount}')::numeric
    else 1
  end;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_strip_nulls(
      jsonb_build_object(
        'snapshotId', v_cache.snapshot_id,
        'processIndex', p_process_index,
        'amount', v_amount,
        'cache', jsonb_strip_nulls(
          jsonb_build_object(
            'cacheId', v_cache.id,
            'requestKey', v_cache.request_key,
            'status', v_cache.status,
            'createdAt', v_cache.created_at,
            'updatedAt', v_cache.updated_at
          )
        ),
        'result', jsonb_strip_nulls(
          jsonb_build_object(
            'resultId', v_result.id,
            'legacyJobId', v_result.job_id,
            'workerJobId', v_result.worker_job_id,
            'snapshotId', v_result.snapshot_id,
            'createdAt', v_result.created_at,
            'artifact', jsonb_strip_nulls(
              jsonb_build_object(
                'artifactUrl', v_result.artifact_url,
                'artifactFormat', v_result.artifact_format,
                'artifactByteSize', v_result.artifact_byte_size,
                'artifactSha256', v_result.artifact_sha256
              )
            )
          )
        ),
        'workerJob', public.worker_job_payload(v_job, false)
      )
    )
  );
end;
$_$;


ALTER FUNCTION "public"."lca_read_latest_single_solve_result"("p_requested_by" "uuid", "p_snapshot_id" "uuid", "p_process_index" integer) OWNER TO "postgres";


COMMENT ON FUNCTION "public"."lca_read_latest_single_solve_result"("p_requested_by" "uuid", "p_snapshot_id" "uuid", "p_process_index" integer) IS 'Service-role latest solve_one result projection from worker_jobs-backed lca_result_cache, replacing lca_jobs payload scans. Authorization is enforced by EXECUTE grants.';



CREATE OR REPLACE FUNCTION "public"."lca_read_result_projection"("p_requested_by" "uuid", "p_result_id" "uuid", "p_required_artifact_format" "text" DEFAULT NULL::"text", "p_include_internal" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_result public.lca_results%rowtype;
  v_job public.worker_jobs%rowtype;
  v_required_format text := nullif(trim(p_required_artifact_format), '');
begin
  -- Authorization is enforced by EXECUTE grants below; runtime service clients can
  -- call this RPC even when request-header GUCs are not populated by PostgREST.

  if p_result_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_LCA_RESULT_LOOKUP',
      'status', 400,
      'message', 'p_result_id is required'
    );
  end if;

  select result_row.*
    into v_result
  from public.lca_results as result_row
  join public.worker_jobs as worker_job
    on worker_job.id = result_row.worker_job_id
  where result_row.id = p_result_id
    and worker_job.job_kind = any(array[
      'lca.solve_one',
      'lca.solve_batch',
      'lca.solve_all_unit',
      'lca.contribution_path'
    ])
    and (p_requested_by is null or worker_job.requested_by = p_requested_by)
  limit 1;

  if v_result.id is null then
    return jsonb_build_object('ok', true, 'data', null);
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = v_result.worker_job_id;

  if v_required_format is not null
    and coalesce(v_result.artifact_format, '') <> v_required_format then
    return jsonb_build_object(
      'ok', false,
      'code', 'UNSUPPORTED_LCA_RESULT_ARTIFACT_FORMAT',
      'status', 409,
      'message', 'LCA result artifact format is not supported for this read path',
      'details', jsonb_build_object(
        'resultId', v_result.id,
        'expectedArtifactFormat', v_required_format,
        'actualArtifactFormat', v_result.artifact_format
      )
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_strip_nulls(
      jsonb_build_object(
        'result', jsonb_strip_nulls(
          jsonb_build_object(
            'resultId', v_result.id,
            'legacyJobId', v_result.job_id,
            'workerJobId', v_result.worker_job_id,
            'snapshotId', v_result.snapshot_id,
            'createdAt', v_result.created_at,
            'diagnostics', v_result.diagnostics,
            'artifact', jsonb_strip_nulls(
              jsonb_build_object(
                'artifactUrl', v_result.artifact_url,
                'artifactFormat', v_result.artifact_format,
                'artifactByteSize', v_result.artifact_byte_size,
                'artifactSha256', v_result.artifact_sha256
              )
            )
          )
        ),
        'job', jsonb_strip_nulls(
          jsonb_build_object(
            'workerJobId', v_job.id,
            'legacyJobId', v_result.job_id,
            'snapshotId', v_result.snapshot_id,
            'jobKind', v_job.job_kind,
            'jobType', public.lca_legacy_job_type(v_job.job_kind),
            'status', v_job.status,
            'phase', v_job.phase,
            'progress', v_job.progress,
            'timestamps', jsonb_strip_nulls(
              jsonb_build_object(
                'createdAt', v_job.created_at,
                'startedAt', v_job.started_at,
                'finishedAt', v_job.finished_at,
                'updatedAt', v_job.updated_at
              )
            )
          )
        ),
        'workerJob', public.worker_job_payload(v_job, p_include_internal)
      )
    )
  );
end;
$$;


ALTER FUNCTION "public"."lca_read_result_projection"("p_requested_by" "uuid", "p_result_id" "uuid", "p_required_artifact_format" "text", "p_include_internal" boolean) OWNER TO "postgres";


COMMENT ON FUNCTION "public"."lca_read_result_projection"("p_requested_by" "uuid", "p_result_id" "uuid", "p_required_artifact_format" "text", "p_include_internal" boolean) IS 'Service-role LCA result projection with worker_jobs ownership, replacing Edge reads through public.lca_jobs. Authorization is enforced by EXECUTE grants.';



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


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_contacts_latest"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
begin
  return query select * from public.search_contacts_latest(query_text, filter_condition, page_size, page_current, data_source, this_user_id, team_id_filter, state_code_filter);
end;
$$;


ALTER FUNCTION "public"."pgroonga_search_contacts_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_flowproperties_latest"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
begin
  return query select * from public.search_flowproperties_latest(query_text, filter_condition, page_size, page_current, data_source, this_user_id, team_id_filter, state_code_filter);
end;
$$;


ALTER FUNCTION "public"."pgroonga_search_flowproperties_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_flows_latest"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "order_by" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
begin
  return query select * from public.search_flows_latest(query_text, filter_condition, order_by, page_size, page_current, data_source, this_user_id, team_id_filter, state_code_filter);
end;
$$;


ALTER FUNCTION "public"."pgroonga_search_flows_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "order_by" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
begin
  return query select * from public.search_lifecyclemodels_latest(query_text, filter_condition, order_by, page_size, page_current, data_source, this_user_id, team_id_filter, state_code_filter);
end;
$$;


ALTER FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_processes_latest"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "order_by" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "type_of_data_set_filter" "text" DEFAULT 'all'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "model_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
begin
  return query select * from public.search_processes_latest(query_text, filter_condition, order_by, page_size, page_current, data_source, this_user_id, team_id_filter, state_code_filter, type_of_data_set_filter);
end;
$$;


ALTER FUNCTION "public"."pgroonga_search_processes_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text") OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_sources_latest"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
begin
  return query select * from public.search_sources_latest(query_text, filter_condition, page_size, page_current, data_source, this_user_id, team_id_filter, state_code_filter);
end;
$$;


ALTER FUNCTION "public"."pgroonga_search_sources_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."pgroonga_search_unitgroups_latest"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
begin
  return query select * from public.search_unitgroups_latest(query_text, filter_condition, page_size, page_current, data_source, this_user_id, team_id_filter, state_code_filter);
end;
$$;


ALTER FUNCTION "public"."pgroonga_search_unitgroups_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."qry_team_find_invitable_user_by_email"("p_team_id" "uuid", "p_email" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_email text := lower(btrim(coalesce(p_email, '')));
  v_user_id uuid;
  v_user_email text;
  v_user_meta jsonb := '{}'::jsonb;
  v_requested_team_role text;
  v_other_team_role text;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_team_id is null or v_email = '' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_PAYLOAD',
      'status', 400,
      'message', 'teamId and email are required'
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

  if not public.cmd_membership_is_team_manager(v_actor, p_team_id) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'Only team owners and admins can look up invitees'
    );
  end if;

  select
    au.id,
    coalesce(nullif(btrim(au.email), ''), nullif(btrim(u.raw_user_meta_data ->> 'email'), '')),
    coalesce(u.raw_user_meta_data, '{}'::jsonb)
  into v_user_id, v_user_email, v_user_meta
  from auth.users as au
  left join public.users as u
    on u.id = au.id
  where lower(btrim(coalesce(au.email, u.raw_user_meta_data ->> 'email', ''))) = v_email
  order by au.created_at desc, au.id
  limit 1;

  if v_user_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'USER_NOT_FOUND',
      'status', 404,
      'message', 'No registered user was found for this email'
    );
  end if;

  select role
    into v_requested_team_role
  from public.roles
  where user_id = v_user_id
    and team_id = p_team_id;

  if v_requested_team_role = 'rejected' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REINVITE_REQUIRED',
      'status', 409,
      'message', 'Use the reinvite command for rejected members',
      'data', jsonb_build_object(
        'id', v_user_id,
        'user_id', v_user_id,
        'email', coalesce(v_user_email, v_email)
      )
    );
  end if;

  if v_requested_team_role is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_MEMBER_ALREADY_EXISTS',
      'status', 409,
      'message', 'The team membership already exists',
      'data', jsonb_build_object(
        'id', v_user_id,
        'user_id', v_user_id,
        'email', coalesce(v_user_email, v_email)
      )
    );
  end if;

  select role
    into v_other_team_role
  from public.roles
  where user_id = v_user_id
    and team_id <> '00000000-0000-0000-0000-000000000000'::uuid
    and team_id <> p_team_id
    and role <> 'rejected'
  order by
    case when role = 'is_invited' then 0 else 1 end,
    modified_at desc nulls last,
    created_at desc nulls last
  limit 1;

  if v_other_team_role = 'is_invited' then
    return jsonb_build_object(
      'ok', false,
      'code', 'USER_ALREADY_INVITED_TO_TEAM',
      'status', 409,
      'message', 'This user already has a pending invitation to another team'
    );
  end if;

  if v_other_team_role is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'USER_ALREADY_IN_TEAM',
      'status', 409,
      'message', 'This user already belongs to another team'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'id', v_user_id,
      'user_id', v_user_id,
      'email', coalesce(v_user_email, v_email),
      'display_name', coalesce(
        nullif(btrim(v_user_meta ->> 'display_name'), ''),
        nullif(btrim(v_user_meta ->> 'name'), ''),
        nullif(btrim(v_user_email), ''),
        v_email
      )
    )
  );
end;
$$;


ALTER FUNCTION "public"."qry_team_find_invitable_user_by_email"("p_team_id" "uuid", "p_email" "text") OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."search_contacts_latest"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
begin
  return query
    select *
    from public._search_simple_dataset_latest(
      'public.contacts'::regclass,
      query_text,
      filter_condition,
      page_size,
      page_current,
      data_source,
      this_user_id,
      team_id_filter,
      state_code_filter
    );
end;
$$;


ALTER FUNCTION "public"."search_contacts_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."search_dataset_json_uuid_mentions"("p_uuid" "uuid", "p_source_entity_kinds" "text"[] DEFAULT NULL::"text"[], "p_data_source" "text" DEFAULT 'tg'::"text", "p_this_user_id" "text" DEFAULT ''::"text", "p_team_id_filter" "uuid" DEFAULT NULL::"uuid", "p_state_code_filter" integer DEFAULT NULL::integer, "p_limit" integer DEFAULT 20) RETURNS TABLE("rank" bigint, "source_entity_kind" "text", "source_id" "uuid", "source_version" character, "source_name" "text", "source_modified_at" timestamp with time zone, "source_team_id" "uuid", "source_json" "jsonb", "matched_by" "text", "matched_entity_table" "text")
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '20s'
    AS $$
begin
  return query
    select *
    from private.search_dataset_json_uuid_mentions_impl(
      p_uuid,
      p_source_entity_kinds,
      p_data_source,
      p_this_user_id,
      p_team_id_filter,
      p_state_code_filter,
      p_limit
    );
end;
$$;


ALTER FUNCTION "public"."search_dataset_json_uuid_mentions"("p_uuid" "uuid", "p_source_entity_kinds" "text"[], "p_data_source" "text", "p_this_user_id" "text", "p_team_id_filter" "uuid", "p_state_code_filter" integer, "p_limit" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."search_flowproperties_latest"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
begin
  return query
    select *
    from public._search_simple_dataset_latest(
      'public.flowproperties'::regclass,
      query_text,
      filter_condition,
      page_size,
      page_current,
      data_source,
      this_user_id,
      team_id_filter,
      state_code_filter
    );
end;
$$;


ALTER FUNCTION "public"."search_flowproperties_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."search_flows_latest"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "order_by" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
begin
  return query
    select *
    from private.search_flows_latest_impl(
      query_text,
      filter_condition,
      page_size,
      page_current,
      data_source,
      this_user_id,
      team_id_filter,
      state_code_filter
    );
end;
$$;


ALTER FUNCTION "public"."search_flows_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."search_lifecyclemodels_latest"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "order_by" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
begin
  return query
    select *
    from private.search_lifecyclemodels_latest_impl(
      query_text,
      filter_condition,
      page_size,
      page_current,
      data_source,
      this_user_id,
      team_id_filter,
      state_code_filter
    );
end;
$$;


ALTER FUNCTION "public"."search_lifecyclemodels_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."search_processes_latest"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "order_by" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "type_of_data_set_filter" "text" DEFAULT 'all'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "model_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
begin
  return query
    select *
    from private.search_processes_latest_impl(
      query_text,
      filter_condition,
      page_size,
      page_current,
      data_source,
      this_user_id,
      team_id_filter,
      state_code_filter,
      type_of_data_set_filter
    );
end;
$$;


ALTER FUNCTION "public"."search_processes_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."search_sources_latest"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
begin
  return query
    select *
    from public._search_simple_dataset_latest(
      'public.sources'::regclass,
      query_text,
      filter_condition,
      page_size,
      page_current,
      data_source,
      this_user_id,
      team_id_filter,
      state_code_filter
    );
end;
$$;


ALTER FUNCTION "public"."search_sources_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."search_unitgroups_latest"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
begin
  return query
    select *
    from public._search_simple_dataset_latest(
      'public.unitgroups'::regclass,
      query_text,
      filter_condition,
      page_size,
      page_current,
      data_source,
      this_user_id,
      team_id_filter,
      state_code_filter
    );
end;
$$;


ALTER FUNCTION "public"."search_unitgroups_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."semantic_search_flows"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "sql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $_$
  select * from public.semantic_search_flows_v1($1, $2, $3, $4, $5);
$_$;


ALTER FUNCTION "public"."semantic_search_flows"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."semantic_search_lifecyclemodels"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "sql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $_$
  select * from public.semantic_search_lifecyclemodels_v1($1, $2, $3, $4, $5);
$_$;


ALTER FUNCTION "public"."semantic_search_lifecyclemodels"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "postgres";


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


CREATE OR REPLACE FUNCTION "public"."semantic_search_processes"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "sql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $_$
  select * from public.semantic_search_processes_v1($1, $2, $3, $4, $5);
$_$;


ALTER FUNCTION "public"."semantic_search_processes"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "postgres";


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
    SET "search_path" TO ''
    AS $$
begin
  new.modified_at = now();
  return new;
end;
$$;


ALTER FUNCTION "public"."update_modified_at"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."worker_cancel_job"("p_job_id" "uuid", "p_cancelled_by" "uuid" DEFAULT NULL::"uuid", "p_reason" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_job public.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to cancel worker jobs'
    );
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = p_job_id
  for update;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Worker job not found'
    );
  end if;

  if v_job.status in ('completed', 'blocked', 'failed') then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_TERMINAL',
      'status', 409,
      'message', 'Completed, blocked, and failed worker jobs cannot be cancelled'
    );
  end if;

  if v_job.status = 'cancelled' then
    return jsonb_build_object(
      'ok', true,
      'data', public.worker_job_payload(v_job, true)
    );
  end if;

  update public.worker_jobs
    set status = 'cancelled',
        cancelled_at = now(),
        cancelled_by = p_cancelled_by,
        leased_by = null,
        lease_token = null,
        lease_expires_at = null,
        error_code = 'worker_job_cancelled',
        error_message = coalesce(nullif(trim(p_reason), ''), 'Worker job was cancelled'),
        updated_at = now(),
        finished_at = now()
  where id = v_job.id
  returning *
    into v_job;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    message,
    details
  ) values (
    v_job.id,
    'cancelled',
    v_job.status,
    p_reason,
    jsonb_build_object('cancelledBy', p_cancelled_by)
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.worker_job_payload(v_job, true)
  );
end;
$$;


ALTER FUNCTION "public"."worker_cancel_job"("p_job_id" "uuid", "p_cancelled_by" "uuid", "p_reason" "text") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."worker_claim_jobs"("p_worker_queue" "text", "p_worker_id" "text" DEFAULT NULL::"text", "p_limit" integer DEFAULT 10, "p_lease_seconds" integer DEFAULT NULL::integer) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_worker_queue text := lower(trim(coalesce(p_worker_queue, '')));
  v_worker_id text := nullif(trim(p_worker_id), '');
  v_limit integer := greatest(1, least(coalesce(p_limit, 10), 50));
  v_lease_seconds integer := greatest(1, least(coalesce(p_lease_seconds, 300), 86400));
  v_jobs jsonb := '[]'::jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to claim worker jobs'
    );
  end if;

  if v_worker_queue not in ('solver', 'review_submit', 'review_submit_gate', 'package', 'maintenance') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_QUEUE',
      'status', 400,
      'message', 'workerQueue must be solver, review_submit, review_submit_gate, package, or maintenance'
    );
  end if;

  with expired as (
    update public.worker_jobs as j
      set status = 'failed',
          error_code = coalesce(j.error_code, 'lease_expired_max_attempts'),
          error_message = coalesce(j.error_message, 'Worker job lease expired after the maximum attempt count'),
          error_details = coalesce(j.error_details, '{}'::jsonb) || jsonb_build_object(
            'leasedBy', j.leased_by,
            'leaseExpiresAt', j.lease_expires_at,
            'attemptCount', j.attempt_count,
            'maxAttempts', j.max_attempts
          ),
          leased_by = null,
          lease_token = null,
          lease_expires_at = null,
          heartbeat_at = coalesce(j.heartbeat_at, now()),
          updated_at = now(),
          finished_at = now()
    where j.worker_runtime = 'calculator'
      and j.worker_queue = v_worker_queue
      and j.status = 'running'
      and j.lease_expires_at < now()
      and j.attempt_count >= j.max_attempts
    returning j.*
  ),
  expired_events as (
    insert into public.worker_job_events (
      job_id,
      event_type,
      status,
      worker_id,
      message,
      details
    )
    select
      expired.id,
      'failed',
      expired.status,
      expired.leased_by,
      'Worker job lease expired after the maximum attempt count',
      jsonb_build_object(
        'errorCode', expired.error_code,
        'attemptCount', expired.attempt_count,
        'maxAttempts', expired.max_attempts
      )
    from expired
    returning id
  ),
  candidate as (
    select id
    from public.worker_jobs
    where worker_runtime = 'calculator'
      and worker_queue = v_worker_queue
      and run_after <= now()
      and attempt_count < max_attempts
      and (
        status in ('queued', 'stale')
        or (status = 'running' and lease_expires_at < now())
      )
    order by priority desc, run_after asc, created_at asc
    limit v_limit
    for update skip locked
  ),
  updated as (
    update public.worker_jobs as j
      set status = 'running',
          attempt_count = j.attempt_count + 1,
          leased_by = v_worker_id,
          lease_token = gen_random_uuid(),
          lease_expires_at = now() + make_interval(secs => v_lease_seconds),
          heartbeat_at = now(),
          started_at = coalesce(j.started_at, now()),
          updated_at = now(),
          error_code = null,
          error_message = null,
          error_details = null
    from candidate
    where j.id = candidate.id
    returning j.*
  ),
  claim_events as (
    insert into public.worker_job_events (
      job_id,
      event_type,
      status,
      phase,
      progress,
      worker_id,
      lease_token,
      details
    )
    select
      updated.id,
      'claimed',
      updated.status,
      updated.phase,
      updated.progress,
      updated.leased_by,
      updated.lease_token,
      jsonb_build_object(
        'attemptCount', updated.attempt_count,
        'leaseExpiresAt', updated.lease_expires_at
      )
    from updated
    returning id
  )
  select coalesce(jsonb_agg(public.worker_job_payload(updated, true)), '[]'::jsonb)
    into v_jobs
  from updated;

  return jsonb_build_object(
    'ok', true,
    'data', v_jobs
  );
end;
$$;


ALTER FUNCTION "public"."worker_claim_jobs"("p_worker_queue" "text", "p_worker_id" "text", "p_limit" integer, "p_lease_seconds" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."worker_enqueue_job"("p_job_kind" "text", "p_payload_json" "jsonb" DEFAULT '{}'::"jsonb", "p_payload_schema_version" "text" DEFAULT NULL::"text", "p_subject_type" "text" DEFAULT NULL::"text", "p_subject_id" "uuid" DEFAULT NULL::"uuid", "p_subject_version" "text" DEFAULT NULL::"text", "p_requested_by" "uuid" DEFAULT NULL::"uuid", "p_requester_type" "text" DEFAULT 'user'::"text", "p_team_id" "uuid" DEFAULT NULL::"uuid", "p_idempotency_key" "text" DEFAULT NULL::"text", "p_request_hash" "text" DEFAULT NULL::"text", "p_concurrency_key" "text" DEFAULT NULL::"text", "p_priority" integer DEFAULT NULL::integer, "p_queue_key" "text" DEFAULT NULL::"text", "p_run_after" timestamp with time zone DEFAULT NULL::timestamp with time zone, "p_visibility" "text" DEFAULT NULL::"text", "p_max_attempts" integer DEFAULT NULL::integer, "p_timeout_at" timestamp with time zone DEFAULT NULL::timestamp with time zone, "p_payload_ref" "jsonb" DEFAULT NULL::"jsonb", "p_parent_job_id" "uuid" DEFAULT NULL::"uuid", "p_root_job_id" "uuid" DEFAULT NULL::"uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_job_kind text := lower(trim(coalesce(p_job_kind, '')));
  v_requester_type text := lower(trim(coalesce(p_requester_type, 'user')));
  v_kind public.worker_job_kinds%rowtype;
  v_existing public.worker_jobs%rowtype;
  v_job public.worker_jobs%rowtype;
  v_payload jsonb := coalesce(p_payload_json, '{}'::jsonb);
  v_payload_ref jsonb := p_payload_ref;
  v_visibility text;
  v_payload_schema_version text;
  v_priority integer;
  v_max_attempts integer;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to enqueue worker jobs'
    );
  end if;

  select *
    into v_kind
  from public.worker_job_kinds
  where job_kind = v_job_kind;

  if v_kind.job_kind is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_KIND_UNSUPPORTED',
      'status', 400,
      'message', 'Unsupported worker job kind'
    );
  end if;

  if jsonb_typeof(v_payload) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_PAYLOAD',
      'status', 400,
      'message', 'worker job payload must be a JSON object'
    );
  end if;

  if v_payload_ref is not null and jsonb_typeof(v_payload_ref) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_PAYLOAD_REF',
      'status', 400,
      'message', 'worker job payloadRef must be a JSON object'
    );
  end if;

  if v_requester_type not in ('user', 'system', 'service', 'operator') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_REQUESTER_TYPE',
      'status', 400,
      'message', 'requesterType must be user, system, service, or operator'
    );
  end if;

  if v_requester_type = 'user' and p_requested_by is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_REQUESTED_BY_REQUIRED',
      'status', 400,
      'message', 'requestedBy is required for user-requested worker jobs'
    );
  end if;

  v_visibility := lower(trim(coalesce(p_visibility, v_kind.default_visibility)));
  if v_visibility not in ('user', 'operator', 'system') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_VISIBILITY',
      'status', 400,
      'message', 'visibility must be user, operator, or system'
    );
  end if;

  v_payload_schema_version := coalesce(nullif(trim(p_payload_schema_version), ''), v_kind.payload_schema_version);
  v_priority := coalesce(p_priority, v_kind.default_priority);
  v_max_attempts := greatest(1, coalesce(p_max_attempts, v_kind.default_max_attempts, 3));

  if p_idempotency_key is not null then
    select *
      into v_existing
    from public.worker_jobs
    where worker_runtime = v_kind.worker_runtime
      and job_kind = v_kind.job_kind
      and requested_by is not distinct from p_requested_by
      and idempotency_key = p_idempotency_key
      and status in ('queued', 'running', 'waiting', 'stale', 'blocked')
    order by created_at desc
    limit 1;

    if v_existing.id is not null then
      return jsonb_build_object(
        'ok', true,
        'data', public.worker_job_payload(v_existing, true),
        'reused', true
      );
    end if;
  end if;

  if p_concurrency_key is not null then
    select *
      into v_existing
    from public.worker_jobs
    where worker_runtime = v_kind.worker_runtime
      and worker_queue = v_kind.worker_queue
      and concurrency_key = p_concurrency_key
      and status in ('queued', 'running', 'waiting', 'stale')
    order by created_at desc
    limit 1;

    if v_existing.id is not null then
      return jsonb_build_object(
        'ok', false,
        'code', 'WORKER_JOB_CONCURRENCY_CONFLICT',
        'status', 409,
        'message', 'A conflicting worker job is already active',
        'details', public.worker_job_payload(v_existing, false)
      );
    end if;
  end if;

  insert into public.worker_jobs (
    job_kind,
    worker_runtime,
    worker_queue,
    priority,
    queue_key,
    root_job_id,
    parent_job_id,
    subject_type,
    subject_id,
    subject_version,
    requester_type,
    requested_by,
    team_id,
    idempotency_key,
    request_hash,
    concurrency_key,
    visibility,
    run_after,
    max_attempts,
    timeout_at,
    payload_schema_version,
    payload_json,
    payload_ref,
    result_schema_version
  ) values (
    v_kind.job_kind,
    v_kind.worker_runtime,
    v_kind.worker_queue,
    v_priority,
    nullif(trim(p_queue_key), ''),
    p_root_job_id,
    p_parent_job_id,
    nullif(trim(p_subject_type), ''),
    p_subject_id,
    nullif(trim(p_subject_version), ''),
    v_requester_type,
    p_requested_by,
    p_team_id,
    nullif(trim(p_idempotency_key), ''),
    nullif(trim(p_request_hash), ''),
    nullif(trim(p_concurrency_key), ''),
    v_visibility,
    coalesce(p_run_after, now()),
    v_max_attempts,
    p_timeout_at,
    v_payload_schema_version,
    v_payload,
    v_payload_ref,
    v_kind.result_schema_version
  )
  returning *
    into v_job;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    details
  ) values (
    v_job.id,
    'enqueued',
    v_job.status,
    jsonb_build_object(
      'jobKind', v_job.job_kind,
      'workerQueue', v_job.worker_queue,
      'idempotencyKey', v_job.idempotency_key,
      'concurrencyKey', v_job.concurrency_key
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.worker_job_payload(v_job, true),
    'reused', false
  );
exception
  when unique_violation then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_UNIQUE_CONFLICT',
      'status', 409,
      'message', 'A worker job with the same idempotency or concurrency key already exists'
    );
end;
$$;


ALTER FUNCTION "public"."worker_enqueue_job"("p_job_kind" "text", "p_payload_json" "jsonb", "p_payload_schema_version" "text", "p_subject_type" "text", "p_subject_id" "uuid", "p_subject_version" "text", "p_requested_by" "uuid", "p_requester_type" "text", "p_team_id" "uuid", "p_idempotency_key" "text", "p_request_hash" "text", "p_concurrency_key" "text", "p_priority" integer, "p_queue_key" "text", "p_run_after" timestamp with time zone, "p_visibility" "text", "p_max_attempts" integer, "p_timeout_at" timestamp with time zone, "p_payload_ref" "jsonb", "p_parent_job_id" "uuid", "p_root_job_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."worker_heartbeat_job"("p_job_id" "uuid", "p_lease_token" "uuid", "p_phase" "text" DEFAULT NULL::"text", "p_progress" numeric DEFAULT NULL::numeric, "p_diagnostics" "jsonb" DEFAULT NULL::"jsonb", "p_lease_seconds" integer DEFAULT 300) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_job public.worker_jobs%rowtype;
  v_lease_seconds integer := greatest(1, least(coalesce(p_lease_seconds, 300), 86400));
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to heartbeat worker jobs'
    );
  end if;

  if p_progress is not null and (p_progress < 0 or p_progress > 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_PROGRESS',
      'status', 400,
      'message', 'progress must be between 0 and 1'
    );
  end if;

  if p_diagnostics is not null and jsonb_typeof(p_diagnostics) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_DIAGNOSTICS',
      'status', 400,
      'message', 'diagnostics must be a JSON object'
    );
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = p_job_id
  for update;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Worker job not found'
    );
  end if;

  if v_job.status <> 'running' then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_RUNNING',
      'status', 409,
      'message', 'Worker job is not running'
    );
  end if;

  if v_job.lease_token is distinct from p_lease_token then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_LEASE_TOKEN_MISMATCH',
      'status', 409,
      'message', 'Worker job lease token does not match'
    );
  end if;

  if v_job.lease_expires_at < now() then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_LEASE_EXPIRED',
      'status', 409,
      'message', 'Worker job lease has expired'
    );
  end if;

  update public.worker_jobs
    set phase = coalesce(nullif(trim(p_phase), ''), phase),
        progress = coalesce(p_progress, progress),
        diagnostics = diagnostics || coalesce(p_diagnostics, '{}'::jsonb),
        heartbeat_at = now(),
        lease_expires_at = now() + make_interval(secs => v_lease_seconds),
        updated_at = now()
  where id = v_job.id
  returning *
    into v_job;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    phase,
    progress,
    worker_id,
    lease_token,
    details
  ) values (
    v_job.id,
    'heartbeat',
    v_job.status,
    v_job.phase,
    v_job.progress,
    v_job.leased_by,
    v_job.lease_token,
    jsonb_build_object(
      'leaseExpiresAt', v_job.lease_expires_at,
      'diagnostics', coalesce(p_diagnostics, '{}'::jsonb)
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.worker_job_payload(v_job, true)
  );
end;
$$;


ALTER FUNCTION "public"."worker_heartbeat_job"("p_job_id" "uuid", "p_lease_token" "uuid", "p_phase" "text", "p_progress" numeric, "p_diagnostics" "jsonb", "p_lease_seconds" integer) OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."worker_jobs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_kind" "text" NOT NULL,
    "worker_runtime" "text" DEFAULT 'calculator'::"text" NOT NULL,
    "worker_queue" "text" NOT NULL,
    "priority" integer DEFAULT 0 NOT NULL,
    "queue_key" "text",
    "root_job_id" "uuid",
    "parent_job_id" "uuid",
    "subject_type" "text",
    "subject_id" "uuid",
    "subject_version" "text",
    "requester_type" "text" DEFAULT 'user'::"text" NOT NULL,
    "requested_by" "uuid",
    "team_id" "uuid",
    "idempotency_key" "text",
    "request_hash" "text",
    "concurrency_key" "text",
    "status" "text" DEFAULT 'queued'::"text" NOT NULL,
    "phase" "text",
    "progress" numeric,
    "visibility" "text" DEFAULT 'user'::"text" NOT NULL,
    "run_after" timestamp with time zone DEFAULT "now"() NOT NULL,
    "attempt_count" integer DEFAULT 0 NOT NULL,
    "max_attempts" integer DEFAULT 3 NOT NULL,
    "leased_by" "text",
    "lease_token" "uuid",
    "lease_expires_at" timestamp with time zone,
    "heartbeat_at" timestamp with time zone,
    "timeout_at" timestamp with time zone,
    "payload_schema_version" "text" NOT NULL,
    "payload_json" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "payload_ref" "jsonb",
    "result_schema_version" "text",
    "result_json" "jsonb",
    "result_ref" "jsonb",
    "diagnostics" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "error_code" "text",
    "error_message" "text",
    "error_details" "jsonb",
    "blocker_codes" "text"[] DEFAULT '{}'::"text"[] NOT NULL,
    "resolution_scope" "text",
    "retryable" boolean,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "started_at" timestamp with time zone,
    "finished_at" timestamp with time zone,
    "expires_at" timestamp with time zone,
    "cancelled_at" timestamp with time zone,
    "cancelled_by" "uuid",
    CONSTRAINT "worker_jobs_attempt_check" CHECK ((("attempt_count" >= 0) AND ("max_attempts" >= 0) AND ("attempt_count" <= "max_attempts"))),
    CONSTRAINT "worker_jobs_blocked_explanation_check" CHECK ((("status" <> 'blocked'::"text") OR (("cardinality"("blocker_codes") > 0) AND ("resolution_scope" IS NOT NULL)))),
    CONSTRAINT "worker_jobs_diagnostics_object_check" CHECK (("jsonb_typeof"("diagnostics") = 'object'::"text")),
    CONSTRAINT "worker_jobs_error_details_object_check" CHECK ((("error_details" IS NULL) OR ("jsonb_typeof"("error_details") = 'object'::"text"))),
    CONSTRAINT "worker_jobs_payload_object_check" CHECK (("jsonb_typeof"("payload_json") = 'object'::"text")),
    CONSTRAINT "worker_jobs_payload_ref_object_check" CHECK ((("payload_ref" IS NULL) OR ("jsonb_typeof"("payload_ref") = 'object'::"text"))),
    CONSTRAINT "worker_jobs_progress_check" CHECK ((("progress" IS NULL) OR (("progress" >= (0)::numeric) AND ("progress" <= (1)::numeric)))),
    CONSTRAINT "worker_jobs_queue_check" CHECK (("worker_queue" = ANY (ARRAY['solver'::"text", 'review_submit'::"text", 'review_submit_gate'::"text", 'package'::"text", 'maintenance'::"text"]))),
    CONSTRAINT "worker_jobs_requester_check" CHECK (((("requester_type" = 'user'::"text") AND ("requested_by" IS NOT NULL)) OR ("requester_type" = ANY (ARRAY['system'::"text", 'service'::"text", 'operator'::"text"])))),
    CONSTRAINT "worker_jobs_resolution_scope_check" CHECK ((("resolution_scope" IS NULL) OR ("resolution_scope" = ANY (ARRAY['user'::"text", 'operator'::"text", 'system'::"text"])))),
    CONSTRAINT "worker_jobs_result_object_check" CHECK ((("result_json" IS NULL) OR ("jsonb_typeof"("result_json") = 'object'::"text"))),
    CONSTRAINT "worker_jobs_result_ref_object_check" CHECK ((("result_ref" IS NULL) OR ("jsonb_typeof"("result_ref") = 'object'::"text"))),
    CONSTRAINT "worker_jobs_runtime_check" CHECK (("worker_runtime" = 'calculator'::"text")),
    CONSTRAINT "worker_jobs_status_check" CHECK (("status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'waiting'::"text", 'completed'::"text", 'blocked'::"text", 'stale'::"text", 'failed'::"text", 'cancelled'::"text"]))),
    CONSTRAINT "worker_jobs_visibility_check" CHECK (("visibility" = ANY (ARRAY['user'::"text", 'operator'::"text", 'system'::"text"])))
);


ALTER TABLE "public"."worker_jobs" OWNER TO "postgres";


COMMENT ON TABLE "public"."worker_jobs" IS 'Canonical task fact table for work executed or coordinated by tiangong-lca-worker. Legacy job tables may remain only as domain artifact/cache/history compatibility surfaces.';



COMMENT ON COLUMN "public"."worker_jobs"."worker_runtime" IS 'Compatibility runtime discriminator; calculator is the existing compute-runtime key, not the repository identity.';



COMMENT ON COLUMN "public"."worker_jobs"."root_job_id" IS 'Optional root worker job for multi-step flows such as review_submit.submit -> review_submit.gate.';



COMMENT ON COLUMN "public"."worker_jobs"."parent_job_id" IS 'Immediate parent worker job for child execution records.';



COMMENT ON COLUMN "public"."worker_jobs"."subject_type" IS 'Domain subject table or logical entity name used for latest/read/list projections.';



COMMENT ON COLUMN "public"."worker_jobs"."subject_id" IS 'Domain subject UUID used with subject_type and subject_version.';



COMMENT ON COLUMN "public"."worker_jobs"."subject_version" IS 'Domain subject version used with subject_type and subject_id.';



COMMENT ON COLUMN "public"."worker_jobs"."payload_ref" IS 'Internal reference to request payload artifacts or legacy compatibility rows. Exposed only through internal worker projections.';



COMMENT ON COLUMN "public"."worker_jobs"."result_ref" IS 'Internal reference to result/artifact/cache rows. Exposed only through internal worker projections.';



CREATE OR REPLACE FUNCTION "public"."worker_job_payload"("p_job" "public"."worker_jobs", "p_include_internal" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "sql" STABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select jsonb_strip_nulls(
    jsonb_build_object(
      'id', (p_job).id,
      'jobKind', (p_job).job_kind,
      'workerRuntime', (p_job).worker_runtime,
      'workerQueue', (p_job).worker_queue,
      'priority', (p_job).priority,
      'queueKey', (p_job).queue_key,
      'rootJobId', (p_job).root_job_id,
      'parentJobId', (p_job).parent_job_id,
      'subjectType', (p_job).subject_type,
      'subjectId', (p_job).subject_id,
      'subjectVersion', (p_job).subject_version,
      'requesterType', (p_job).requester_type,
      'requestedBy', (p_job).requested_by,
      'teamId', (p_job).team_id,
      'idempotencyKey', (p_job).idempotency_key,
      'requestHash', (p_job).request_hash,
      'concurrencyKey', (p_job).concurrency_key,
      'status', (p_job).status,
      'phase', (p_job).phase,
      'progress', (p_job).progress,
      'visibility', (p_job).visibility,
      'runAfter', to_jsonb((p_job).run_after),
      'attemptCount', (p_job).attempt_count,
      'maxAttempts', (p_job).max_attempts,
      'leasedBy', case when p_include_internal then (p_job).leased_by else null end,
      'leaseToken', case when p_include_internal then (p_job).lease_token else null end,
      'leaseExpiresAt', case when p_include_internal then to_jsonb((p_job).lease_expires_at) else null end,
      'heartbeatAt', to_jsonb((p_job).heartbeat_at),
      'timeoutAt', to_jsonb((p_job).timeout_at),
      'payloadSchemaVersion', (p_job).payload_schema_version,
      'payload', case when p_include_internal then (p_job).payload_json else null end,
      'payloadRef', case when p_include_internal then (p_job).payload_ref else null end,
      'resultSchemaVersion', (p_job).result_schema_version,
      'result', (p_job).result_json,
      'resultRef', case when p_include_internal then (p_job).result_ref else null end,
      'diagnostics', case when p_include_internal then (p_job).diagnostics else null end,
      'errorCode', (p_job).error_code,
      'errorMessage', (p_job).error_message,
      'errorDetails', case when p_include_internal then (p_job).error_details else null end,
      'blockerCodes', to_jsonb((p_job).blocker_codes),
      'resolutionScope', (p_job).resolution_scope,
      'retryable', (p_job).retryable,
      'createdAt', to_jsonb((p_job).created_at),
      'updatedAt', to_jsonb((p_job).updated_at),
      'startedAt', to_jsonb((p_job).started_at),
      'finishedAt', to_jsonb((p_job).finished_at),
      'expiresAt', to_jsonb((p_job).expires_at),
      'cancelledAt', to_jsonb((p_job).cancelled_at),
      'cancelledBy', (p_job).cancelled_by
    )
  )
$$;


ALTER FUNCTION "public"."worker_job_payload"("p_job" "public"."worker_jobs", "p_include_internal" boolean) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."worker_list_jobs"("p_requested_by" "uuid" DEFAULT NULL::"uuid", "p_subject_type" "text" DEFAULT NULL::"text", "p_subject_id" "uuid" DEFAULT NULL::"uuid", "p_statuses" "text"[] DEFAULT NULL::"text"[], "p_visibility" "text" DEFAULT NULL::"text", "p_limit" integer DEFAULT 50, "p_include_internal" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_limit integer := greatest(1, least(coalesce(p_limit, 50), 200));
  v_jobs jsonb := '[]'::jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to list worker jobs'
    );
  end if;

  if p_visibility is not null and p_visibility not in ('user', 'operator', 'system') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_VISIBILITY',
      'status', 400,
      'message', 'visibility must be user, operator, or system'
    );
  end if;

  select coalesce(jsonb_agg(public.worker_job_payload(j, p_include_internal) order by j.updated_at desc), '[]'::jsonb)
    into v_jobs
  from (
    select *
    from public.worker_jobs
    where (p_requested_by is null or requested_by = p_requested_by)
      and (p_subject_type is null or subject_type = p_subject_type)
      and (p_subject_id is null or subject_id = p_subject_id)
      and (p_visibility is null or visibility = p_visibility)
      and (p_statuses is null or status = any(p_statuses))
    order by updated_at desc
    limit v_limit
  ) as j;

  return jsonb_build_object(
    'ok', true,
    'data', v_jobs
  );
end;
$$;


ALTER FUNCTION "public"."worker_list_jobs"("p_requested_by" "uuid", "p_subject_type" "text", "p_subject_id" "uuid", "p_statuses" "text"[], "p_visibility" "text", "p_limit" integer, "p_include_internal" boolean) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."worker_read_job"("p_job_id" "uuid", "p_include_internal" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_job public.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to read worker jobs'
    );
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = p_job_id;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Worker job not found'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', public.worker_job_payload(v_job, p_include_internal)
  );
end;
$$;


ALTER FUNCTION "public"."worker_read_job"("p_job_id" "uuid", "p_include_internal" boolean) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."worker_read_latest_job"("p_requested_by" "uuid" DEFAULT NULL::"uuid", "p_subject_type" "text" DEFAULT NULL::"text", "p_subject_id" "uuid" DEFAULT NULL::"uuid", "p_subject_version" "text" DEFAULT NULL::"text", "p_job_kind" "text" DEFAULT NULL::"text", "p_statuses" "text"[] DEFAULT NULL::"text"[], "p_include_internal" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_subject_type text := nullif(trim(p_subject_type), '');
  v_subject_version text := nullif(trim(p_subject_version), '');
  v_job_kind text := nullif(lower(trim(p_job_kind)), '');
  v_job public.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to read worker jobs'
    );
  end if;

  if p_statuses is not null
    and exists (
      select 1
      from unnest(p_statuses) as status_value
      where status_value not in (
        'queued',
        'running',
        'waiting',
        'completed',
        'blocked',
        'stale',
        'failed',
        'cancelled'
      )
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_STATUS',
      'status', 400,
      'message', 'statuses contains an unsupported worker job status'
    );
  end if;

  select *
    into v_job
  from public.worker_jobs
  where (p_requested_by is null or requested_by = p_requested_by)
    and (v_subject_type is null or subject_type = v_subject_type)
    and (p_subject_id is null or subject_id = p_subject_id)
    and (v_subject_version is null or subject_version = v_subject_version)
    and (v_job_kind is null or job_kind = v_job_kind)
    and (p_statuses is null or status = any(p_statuses))
  order by updated_at desc, created_at desc
  limit 1;

  return jsonb_build_object(
    'ok', true,
    'data', case
      when v_job.id is null then null
      else public.worker_job_payload(v_job, p_include_internal)
    end
  );
end;
$$;


ALTER FUNCTION "public"."worker_read_latest_job"("p_requested_by" "uuid", "p_subject_type" "text", "p_subject_id" "uuid", "p_subject_version" "text", "p_job_kind" "text", "p_statuses" "text"[], "p_include_internal" boolean) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."worker_record_job_result"("p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_result_json" "jsonb" DEFAULT NULL::"jsonb", "p_result_schema_version" "text" DEFAULT NULL::"text", "p_result_ref" "jsonb" DEFAULT NULL::"jsonb", "p_diagnostics" "jsonb" DEFAULT NULL::"jsonb", "p_error_code" "text" DEFAULT NULL::"text", "p_error_message" "text" DEFAULT NULL::"text", "p_error_details" "jsonb" DEFAULT NULL::"jsonb", "p_blocker_codes" "text"[] DEFAULT NULL::"text"[], "p_resolution_scope" "text" DEFAULT NULL::"text", "p_retryable" boolean DEFAULT NULL::boolean) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_status text := lower(trim(coalesce(p_status, '')));
  v_resolution_scope text := lower(trim(coalesce(p_resolution_scope, '')));
  v_blocker_codes text[] := coalesce(p_blocker_codes, '{}'::text[]);
  v_job public.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to record worker job results'
    );
  end if;

  if v_status not in ('completed', 'blocked', 'failed', 'waiting') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_RESULT_STATUS',
      'status', 400,
      'message', 'status must be completed, blocked, failed, or waiting'
    );
  end if;

  if p_result_json is not null and jsonb_typeof(p_result_json) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_RESULT',
      'status', 400,
      'message', 'result must be a JSON object'
    );
  end if;

  if p_result_ref is not null and jsonb_typeof(p_result_ref) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_RESULT_REF',
      'status', 400,
      'message', 'resultRef must be a JSON object'
    );
  end if;

  if p_diagnostics is not null and jsonb_typeof(p_diagnostics) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_DIAGNOSTICS',
      'status', 400,
      'message', 'diagnostics must be a JSON object'
    );
  end if;

  if p_error_details is not null and jsonb_typeof(p_error_details) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_ERROR_DETAILS',
      'status', 400,
      'message', 'error details must be a JSON object'
    );
  end if;

  if v_status = 'blocked' and (cardinality(v_blocker_codes) = 0 or v_resolution_scope = '') then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_BLOCKER_DETAILS_REQUIRED',
      'status', 400,
      'message', 'blocked worker jobs require blockerCodes and resolutionScope'
    );
  end if;

  if v_status = 'blocked' and v_resolution_scope not in ('user', 'operator', 'system') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_RESOLUTION_SCOPE',
      'status', 400,
      'message', 'resolutionScope must be user, operator, or system'
    );
  end if;

  if v_status = 'failed' and nullif(trim(p_error_code), '') is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_ERROR_CODE_REQUIRED',
      'status', 400,
      'message', 'failed worker jobs require an errorCode'
    );
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = p_job_id
  for update;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Worker job not found'
    );
  end if;

  if v_job.status <> 'running' then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_RUNNING',
      'status', 409,
      'message', 'Worker job is not running'
    );
  end if;

  if v_job.lease_token is distinct from p_lease_token then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_LEASE_TOKEN_MISMATCH',
      'status', 409,
      'message', 'Worker job lease token does not match'
    );
  end if;

  if v_job.lease_expires_at < now() then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_LEASE_EXPIRED',
      'status', 409,
      'message', 'Worker job lease has expired'
    );
  end if;

  update public.worker_jobs
    set status = v_status,
        result_schema_version = coalesce(nullif(trim(p_result_schema_version), ''), result_schema_version),
        result_json = p_result_json,
        result_ref = p_result_ref,
        diagnostics = diagnostics || coalesce(p_diagnostics, '{}'::jsonb),
        error_code = nullif(trim(p_error_code), ''),
        error_message = nullif(trim(p_error_message), ''),
        error_details = p_error_details,
        blocker_codes = case
          when v_status = 'blocked' then v_blocker_codes
          else '{}'::text[]
        end,
        resolution_scope = case
          when v_status = 'blocked' then v_resolution_scope
          else null
        end,
        retryable = p_retryable,
        leased_by = null,
        lease_token = null,
        lease_expires_at = null,
        heartbeat_at = coalesce(heartbeat_at, now()),
        updated_at = now(),
        finished_at = case
          when v_status in ('completed', 'blocked', 'failed') then now()
          else null
        end
  where id = v_job.id
  returning *
    into v_job;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    phase,
    progress,
    worker_id,
    lease_token,
    message,
    details
  ) values (
    v_job.id,
    v_status,
    v_job.status,
    v_job.phase,
    v_job.progress,
    null,
    p_lease_token,
    coalesce(p_error_message, null),
    jsonb_strip_nulls(
      jsonb_build_object(
        'errorCode', v_job.error_code,
        'blockerCodes', to_jsonb(v_job.blocker_codes),
        'resolutionScope', v_job.resolution_scope,
        'retryable', v_job.retryable
      )
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.worker_job_payload(v_job, true)
  );
end;
$$;


ALTER FUNCTION "public"."worker_record_job_result"("p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_result_json" "jsonb", "p_result_schema_version" "text", "p_result_ref" "jsonb", "p_diagnostics" "jsonb", "p_error_code" "text", "p_error_message" "text", "p_error_details" "jsonb", "p_blocker_codes" "text"[], "p_resolution_scope" "text", "p_retryable" boolean) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."worker_retry_job"("p_job_id" "uuid", "p_run_after" timestamp with time zone DEFAULT NULL::timestamp with time zone, "p_max_attempts" integer DEFAULT NULL::integer, "p_reason" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_job public.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to retry worker jobs'
    );
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = p_job_id
  for update;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Worker job not found'
    );
  end if;

  if v_job.status not in ('failed', 'blocked', 'stale', 'waiting', 'cancelled') then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_RETRYABLE',
      'status', 409,
      'message', 'Worker job is not in a retryable status'
    );
  end if;

  update public.worker_jobs
    set status = 'queued',
        run_after = coalesce(p_run_after, now()),
        attempt_count = 0,
        max_attempts = greatest(1, coalesce(p_max_attempts, max_attempts)),
        leased_by = null,
        lease_token = null,
        lease_expires_at = null,
        error_code = null,
        error_message = null,
        error_details = null,
        blocker_codes = '{}'::text[],
        resolution_scope = null,
        retryable = null,
        cancelled_at = null,
        cancelled_by = null,
        finished_at = null,
        updated_at = now()
  where id = v_job.id
  returning *
    into v_job;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    message,
    details
  ) values (
    v_job.id,
    'retried',
    v_job.status,
    p_reason,
    jsonb_build_object('runAfter', v_job.run_after)
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.worker_job_payload(v_job, true)
  );
end;
$$;


ALTER FUNCTION "public"."worker_retry_job"("p_job_id" "uuid", "p_run_after" timestamp with time zone, "p_max_attempts" integer, "p_reason" "text") OWNER TO "postgres";


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
    "extracted_text" "text",
    CONSTRAINT "contacts_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 3, 20, 100])))
);


ALTER TABLE "public"."contacts" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."dataset_review_submit_requests" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "dataset_table" "text" NOT NULL,
    "dataset_id" "uuid" NOT NULL,
    "dataset_version" "text" NOT NULL,
    "revision_checksum" "text" NOT NULL,
    "policy_profile" "text" DEFAULT 'review_submit_fast.v1'::"text" NOT NULL,
    "report_schema_version" "text" DEFAULT 'review_submit_gate_report.v1'::"text" NOT NULL,
    "status" "text" DEFAULT 'queued'::"text" NOT NULL,
    "requested_by" "uuid" NOT NULL,
    "gate_run_id" "uuid",
    "gate_worker_job_id" "uuid",
    "submit_worker_job_id" "uuid",
    "attempt_count" integer DEFAULT 0 NOT NULL,
    "last_error_code" "text",
    "last_error_message" "text",
    "last_error_details" "jsonb",
    "result" "jsonb",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "completed_at" timestamp with time zone,
    CONSTRAINT "dataset_review_submit_requests_attempt_count_check" CHECK (("attempt_count" >= 0)),
    CONSTRAINT "dataset_review_submit_requests_checksum_check" CHECK (("revision_checksum" ~ '^[a-f0-9]{64}$'::"text")),
    CONSTRAINT "dataset_review_submit_requests_last_error_details_check" CHECK ((("last_error_details" IS NULL) OR ("jsonb_typeof"("last_error_details") = 'object'::"text"))),
    CONSTRAINT "dataset_review_submit_requests_result_check" CHECK ((("result" IS NULL) OR ("jsonb_typeof"("result") = 'object'::"text"))),
    CONSTRAINT "dataset_review_submit_requests_status_check" CHECK (("status" = ANY (ARRAY['queued'::"text", 'waiting_gate'::"text", 'submitting'::"text", 'submitted'::"text", 'blocked'::"text", 'stale'::"text", 'error'::"text", 'cancelled'::"text"]))),
    CONSTRAINT "dataset_review_submit_requests_table_check" CHECK (("dataset_table" = 'processes'::"text"))
);


ALTER TABLE "public"."dataset_review_submit_requests" OWNER TO "postgres";


COMMENT ON TABLE "public"."dataset_review_submit_requests" IS 'Durable review-submit request/coordinator state. This replaces dataset_review_submit_jobs as the active coordinator table while worker_jobs remains the canonical lifecycle fact.';



COMMENT ON COLUMN "public"."dataset_review_submit_requests"."gate_worker_job_id" IS 'Canonical review_submit.gate worker_jobs task that verifies this review-submit request before final submission.';



COMMENT ON COLUMN "public"."dataset_review_submit_requests"."submit_worker_job_id" IS 'Canonical root review_submit.submit worker_jobs task for this review-submit request.';



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
    "extracted_text" "text",
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
    "prepared_worker_job_id" "uuid",
    CONSTRAINT "lca_factorization_registry_backend_chk" CHECK (("backend" = ANY (ARRAY['umfpack'::"text", 'cholmod'::"text", 'spqr'::"text"]))),
    CONSTRAINT "lca_factorization_registry_status_chk" CHECK (("status" = ANY (ARRAY['pending'::"text", 'building'::"text", 'ready'::"text", 'failed'::"text", 'stale'::"text"])))
);


ALTER TABLE "public"."lca_factorization_registry" OWNER TO "postgres";


COMMENT ON COLUMN "public"."lca_factorization_registry"."prepared_job_id" IS 'Historical legacy LCA preparation job identifier retained for compatibility. New canonical task identity is prepared_worker_job_id.';



COMMENT ON COLUMN "public"."lca_factorization_registry"."prepared_worker_job_id" IS 'Canonical worker_jobs task that prepared this factorization artifact.';



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
    "worker_job_id" "uuid",
    CONSTRAINT "lca_latest_all_unit_results_size_chk" CHECK (("query_artifact_byte_size" >= 0)),
    CONSTRAINT "lca_latest_all_unit_results_status_chk" CHECK (("status" = ANY (ARRAY['ready'::"text", 'stale'::"text", 'failed'::"text"])))
);


ALTER TABLE "public"."lca_latest_all_unit_results" OWNER TO "postgres";


COMMENT ON COLUMN "public"."lca_latest_all_unit_results"."job_id" IS 'Historical legacy LCA job identifier retained for compatibility. New canonical task identity is worker_job_id.';



COMMENT ON COLUMN "public"."lca_latest_all_unit_results"."worker_job_id" IS 'Canonical worker_jobs task that produced the latest all-unit result artifact.';



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
    CONSTRAINT "lca_network_snapshots_provider_rule_chk" CHECK (("provider_matching_rule" = ANY (ARRAY['strict_unique_provider'::"text", 'best_provider_strict'::"text", 'split_by_evidence'::"text", 'split_by_evidence_hybrid'::"text", 'split_equal'::"text", 'equal_split_multi_provider'::"text", 'custom_weighted_provider'::"text", 'split_by_process_volume'::"text"]))),
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
    "worker_job_id" "uuid",
    CONSTRAINT "lca_package_artifacts_format_chk" CHECK (("artifact_format" = ANY (ARRAY['tidas-package-zip:v1'::"text", 'tidas-package-export-report:v1'::"text", 'tidas-package-import-report:v1'::"text"]))),
    CONSTRAINT "lca_package_artifacts_kind_chk" CHECK (("artifact_kind" = ANY (ARRAY['import_source'::"text", 'export_zip'::"text", 'export_report'::"text", 'import_report'::"text"]))),
    CONSTRAINT "lca_package_artifacts_size_chk" CHECK ((("artifact_byte_size" IS NULL) OR ("artifact_byte_size" >= 0))),
    CONSTRAINT "lca_package_artifacts_status_chk" CHECK (("status" = ANY (ARRAY['pending'::"text", 'ready'::"text", 'failed'::"text", 'deleted'::"text"]))),
    CONSTRAINT "lca_package_artifacts_url_chk" CHECK (("length"("btrim"("artifact_url")) > 0))
);


ALTER TABLE "public"."lca_package_artifacts" OWNER TO "postgres";


COMMENT ON COLUMN "public"."lca_package_artifacts"."job_id" IS 'Historical legacy package job identifier retained for compatibility. New canonical task identity is worker_job_id.';



COMMENT ON COLUMN "public"."lca_package_artifacts"."worker_job_id" IS 'Canonical worker_jobs task that produced this TIDAS package artifact.';



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
    "worker_job_id" "uuid",
    CONSTRAINT "lca_package_export_items_table_chk" CHECK (("table_name" = ANY (ARRAY['contacts'::"text", 'sources'::"text", 'unitgroups'::"text", 'flowproperties'::"text", 'flows'::"text", 'processes'::"text", 'lifecyclemodels'::"text"]))),
    CONSTRAINT "lca_package_export_items_version_chk" CHECK (("length"("btrim"("version")) > 0))
);


ALTER TABLE "public"."lca_package_export_items" OWNER TO "postgres";


COMMENT ON COLUMN "public"."lca_package_export_items"."job_id" IS 'Historical legacy package job identifier retained for compatibility. New canonical task identity is worker_job_id.';



COMMENT ON COLUMN "public"."lca_package_export_items"."worker_job_id" IS 'Canonical worker_jobs task that discovered or exported this package item.';



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
    "worker_job_id" "uuid",
    CONSTRAINT "lca_package_request_cache_hit_count_chk" CHECK (("hit_count" >= 0)),
    CONSTRAINT "lca_package_request_cache_operation_chk" CHECK (("operation" = ANY (ARRAY['export_package'::"text", 'import_package'::"text"]))),
    CONSTRAINT "lca_package_request_cache_request_key_chk" CHECK (("length"("btrim"("request_key")) > 0)),
    CONSTRAINT "lca_package_request_cache_status_chk" CHECK (("status" = ANY (ARRAY['pending'::"text", 'running'::"text", 'ready'::"text", 'failed'::"text", 'stale'::"text"])))
);


ALTER TABLE "public"."lca_package_request_cache" OWNER TO "postgres";


COMMENT ON COLUMN "public"."lca_package_request_cache"."job_id" IS 'Historical legacy package job identifier retained for compatibility. New canonical task identity is worker_job_id.';



COMMENT ON COLUMN "public"."lca_package_request_cache"."worker_job_id" IS 'Canonical worker_jobs task currently backing this package request cache row.';



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
    "worker_job_id" "uuid",
    CONSTRAINT "lca_result_cache_hit_count_chk" CHECK (("hit_count" >= 0)),
    CONSTRAINT "lca_result_cache_request_key_chk" CHECK (("length"("request_key") > 0)),
    CONSTRAINT "lca_result_cache_status_chk" CHECK (("status" = ANY (ARRAY['pending'::"text", 'running'::"text", 'ready'::"text", 'failed'::"text", 'stale'::"text"])))
);


ALTER TABLE "public"."lca_result_cache" OWNER TO "postgres";


COMMENT ON COLUMN "public"."lca_result_cache"."job_id" IS 'Historical legacy LCA job identifier retained for compatibility. New canonical task identity is worker_job_id.';



COMMENT ON COLUMN "public"."lca_result_cache"."worker_job_id" IS 'Canonical worker_jobs task currently backing this LCA result cache row.';



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
    "worker_job_id" "uuid",
    "expires_at" timestamp with time zone DEFAULT ("now"() + '30 days'::interval) NOT NULL,
    "is_pinned" boolean DEFAULT false NOT NULL,
    CONSTRAINT "lca_results_artifact_size_chk" CHECK ((("artifact_byte_size" IS NULL) OR ("artifact_byte_size" >= 0)))
);


ALTER TABLE "public"."lca_results" OWNER TO "postgres";


COMMENT ON COLUMN "public"."lca_results"."job_id" IS 'Historical legacy LCA job identifier retained for compatibility. New canonical task identity is worker_job_id.';



COMMENT ON COLUMN "public"."lca_results"."worker_job_id" IS 'Canonical worker_jobs task that produced this LCA result artifact.';



COMMENT ON COLUMN "public"."lca_results"."expires_at" IS 'Result artifact retention deadline used by tiangong-lca-worker lca.result_gc; existing rows created before this contract receive a 30 day migration grace period.';



COMMENT ON COLUMN "public"."lca_results"."is_pinned" IS 'When true, protects the LCA result artifact metadata row from automatic result GC.';



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


CREATE TABLE IF NOT EXISTS "public"."lca_snapshot_gc_run_items" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "run_id" "uuid" NOT NULL,
    "candidate_type" "text" NOT NULL,
    "snapshot_id" "uuid",
    "bucket_id" "text" NOT NULL,
    "object_name" "text" NOT NULL,
    "storage_bytes" bigint DEFAULT 0 NOT NULL,
    "reason" "text" NOT NULL,
    "delete_db_snapshot" boolean DEFAULT false NOT NULL,
    "action_status" "text" DEFAULT 'planned'::"text" NOT NULL,
    "error_message" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_snapshot_gc_run_items_action_status_chk" CHECK (("action_status" = ANY (ARRAY['planned'::"text", 'dry_run'::"text", 'storage_deleted'::"text", 'storage_missing'::"text", 'storage_failed'::"text", 'db_deleted'::"text", 'skipped'::"text"]))),
    CONSTRAINT "lca_snapshot_gc_run_items_candidate_type_chk" CHECK (("candidate_type" = ANY (ARRAY['snapshot_directory'::"text", 'orphan_storage_directory'::"text"]))),
    CONSTRAINT "lca_snapshot_gc_run_items_storage_bytes_chk" CHECK (("storage_bytes" >= 0))
);


ALTER TABLE "public"."lca_snapshot_gc_run_items" OWNER TO "postgres";


COMMENT ON TABLE "public"."lca_snapshot_gc_run_items" IS 'Per-object audit items for worker-driven lca-results/snapshots object-aware garbage collection runs.';



CREATE TABLE IF NOT EXISTS "public"."lca_snapshot_gc_runs" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "mode" "text" NOT NULL,
    "status" "text" DEFAULT 'running'::"text" NOT NULL,
    "started_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "finished_at" timestamp with time zone,
    "as_of" timestamp with time zone DEFAULT "now"() NOT NULL,
    "snapshot_retention_window" interval DEFAULT '30 days'::interval NOT NULL,
    "orphan_retention_window" interval DEFAULT '30 days'::interval NOT NULL,
    "max_snapshots" integer DEFAULT 100 NOT NULL,
    "max_orphan_dirs" integer DEFAULT 200 NOT NULL,
    "max_bytes" bigint DEFAULT '2147483648'::bigint NOT NULL,
    "candidate_snapshot_count" integer DEFAULT 0 NOT NULL,
    "candidate_orphan_dir_count" integer DEFAULT 0 NOT NULL,
    "candidate_object_count" integer DEFAULT 0 NOT NULL,
    "candidate_storage_bytes" bigint DEFAULT 0 NOT NULL,
    "storage_deleted_count" integer DEFAULT 0 NOT NULL,
    "storage_failed_count" integer DEFAULT 0 NOT NULL,
    "db_snapshot_deleted_count" integer DEFAULT 0 NOT NULL,
    "diagnostics" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_snapshot_gc_runs_caps_chk" CHECK ((("max_snapshots" > 0) AND ("max_orphan_dirs" > 0) AND ("max_bytes" > 0))),
    CONSTRAINT "lca_snapshot_gc_runs_counts_chk" CHECK ((("candidate_snapshot_count" >= 0) AND ("candidate_orphan_dir_count" >= 0) AND ("candidate_object_count" >= 0) AND ("candidate_storage_bytes" >= 0) AND ("storage_deleted_count" >= 0) AND ("storage_failed_count" >= 0) AND ("db_snapshot_deleted_count" >= 0))),
    CONSTRAINT "lca_snapshot_gc_runs_mode_chk" CHECK (("mode" = ANY (ARRAY['dry_run'::"text", 'execute'::"text"]))),
    CONSTRAINT "lca_snapshot_gc_runs_status_chk" CHECK (("status" = ANY (ARRAY['running'::"text", 'succeeded'::"text", 'failed'::"text", 'skipped'::"text"]))),
    CONSTRAINT "lca_snapshot_gc_runs_windows_chk" CHECK ((("snapshot_retention_window" >= '1 day'::interval) AND ("orphan_retention_window" >= '1 day'::interval)))
);


ALTER TABLE "public"."lca_snapshot_gc_runs" OWNER TO "postgres";


COMMENT ON TABLE "public"."lca_snapshot_gc_runs" IS 'Audit header for worker-driven lca-results/snapshots object-aware garbage collection runs.';



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
    "extracted_text" "text",
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
    "extracted_text" "text",
    CONSTRAINT "unitgroups_state_code_check" CHECK (("state_code" = ANY (ARRAY[0, 20, 100, 200])))
);


ALTER TABLE "public"."unitgroups" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."users" (
    "id" "uuid" NOT NULL,
    "raw_user_meta_data" "jsonb",
    "contact" "jsonb"
);


ALTER TABLE "public"."users" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."worker_job_artifacts" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_id" "uuid" NOT NULL,
    "artifact_type" "text" NOT NULL,
    "storage_bucket" "text",
    "storage_path" "text",
    "content_type" "text",
    "byte_size" bigint,
    "checksum_sha256" "text",
    "metadata" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "visibility" "text" DEFAULT 'operator'::"text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "expires_at" timestamp with time zone,
    CONSTRAINT "worker_job_artifacts_byte_size_check" CHECK ((("byte_size" IS NULL) OR ("byte_size" >= 0))),
    CONSTRAINT "worker_job_artifacts_checksum_check" CHECK ((("checksum_sha256" IS NULL) OR ("checksum_sha256" ~ '^[a-f0-9]{64}$'::"text"))),
    CONSTRAINT "worker_job_artifacts_metadata_object_check" CHECK (("jsonb_typeof"("metadata") = 'object'::"text")),
    CONSTRAINT "worker_job_artifacts_visibility_check" CHECK (("visibility" = ANY (ARRAY['user'::"text", 'operator'::"text", 'system'::"text"])))
);


ALTER TABLE "public"."worker_job_artifacts" OWNER TO "postgres";


CREATE OR REPLACE VIEW "public"."worker_job_domain_refs" WITH ("security_invoker"='true') AS
 SELECT "lca_results"."worker_job_id",
    'lca_results'::"text" AS "domain_source",
    "lca_results"."id" AS "domain_id",
    'lca_result_artifact'::"text" AS "domain_role",
    "lca_results"."job_id" AS "legacy_job_id",
    NULL::"text" AS "status",
    "lca_results"."created_at",
    "lca_results"."created_at" AS "updated_at"
   FROM "public"."lca_results"
  WHERE ("lca_results"."worker_job_id" IS NOT NULL)
UNION ALL
 SELECT "lca_result_cache"."worker_job_id",
    'lca_result_cache'::"text" AS "domain_source",
    "lca_result_cache"."id" AS "domain_id",
    'lca_result_cache'::"text" AS "domain_role",
    "lca_result_cache"."job_id" AS "legacy_job_id",
    "lca_result_cache"."status",
    "lca_result_cache"."created_at",
    "lca_result_cache"."updated_at"
   FROM "public"."lca_result_cache"
  WHERE ("lca_result_cache"."worker_job_id" IS NOT NULL)
UNION ALL
 SELECT "lca_latest_all_unit_results"."worker_job_id",
    'lca_latest_all_unit_results'::"text" AS "domain_source",
    "lca_latest_all_unit_results"."id" AS "domain_id",
    'lca_latest_all_unit_result'::"text" AS "domain_role",
    "lca_latest_all_unit_results"."job_id" AS "legacy_job_id",
    "lca_latest_all_unit_results"."status",
    "lca_latest_all_unit_results"."created_at",
    "lca_latest_all_unit_results"."updated_at"
   FROM "public"."lca_latest_all_unit_results"
  WHERE ("lca_latest_all_unit_results"."worker_job_id" IS NOT NULL)
UNION ALL
 SELECT "lca_factorization_registry"."prepared_worker_job_id" AS "worker_job_id",
    'lca_factorization_registry'::"text" AS "domain_source",
    "lca_factorization_registry"."id" AS "domain_id",
    'lca_factorization_artifact'::"text" AS "domain_role",
    "lca_factorization_registry"."prepared_job_id" AS "legacy_job_id",
    "lca_factorization_registry"."status",
    "lca_factorization_registry"."created_at",
    "lca_factorization_registry"."updated_at"
   FROM "public"."lca_factorization_registry"
  WHERE ("lca_factorization_registry"."prepared_worker_job_id" IS NOT NULL)
UNION ALL
 SELECT "lca_package_artifacts"."worker_job_id",
    'lca_package_artifacts'::"text" AS "domain_source",
    "lca_package_artifacts"."id" AS "domain_id",
    'package_artifact'::"text" AS "domain_role",
    "lca_package_artifacts"."job_id" AS "legacy_job_id",
    "lca_package_artifacts"."status",
    "lca_package_artifacts"."created_at",
    "lca_package_artifacts"."updated_at"
   FROM "public"."lca_package_artifacts"
  WHERE ("lca_package_artifacts"."worker_job_id" IS NOT NULL)
UNION ALL
 SELECT "lca_package_export_items"."worker_job_id",
    'lca_package_export_items'::"text" AS "domain_source",
    "lca_package_export_items"."id" AS "domain_id",
    'package_export_item'::"text" AS "domain_role",
    "lca_package_export_items"."job_id" AS "legacy_job_id",
    NULL::"text" AS "status",
    "lca_package_export_items"."created_at",
    "lca_package_export_items"."updated_at"
   FROM "public"."lca_package_export_items"
  WHERE ("lca_package_export_items"."worker_job_id" IS NOT NULL)
UNION ALL
 SELECT "lca_package_request_cache"."worker_job_id",
    'lca_package_request_cache'::"text" AS "domain_source",
    "lca_package_request_cache"."id" AS "domain_id",
    'package_request_cache'::"text" AS "domain_role",
    "lca_package_request_cache"."job_id" AS "legacy_job_id",
    "lca_package_request_cache"."status",
    "lca_package_request_cache"."created_at",
    "lca_package_request_cache"."updated_at"
   FROM "public"."lca_package_request_cache"
  WHERE ("lca_package_request_cache"."worker_job_id" IS NOT NULL)
UNION ALL
 SELECT "dataset_review_submit_requests"."submit_worker_job_id" AS "worker_job_id",
    'dataset_review_submit_requests'::"text" AS "domain_source",
    "dataset_review_submit_requests"."id" AS "domain_id",
    'review_submit_coordinator'::"text" AS "domain_role",
    NULL::"uuid" AS "legacy_job_id",
    "dataset_review_submit_requests"."status",
    "dataset_review_submit_requests"."created_at",
    "dataset_review_submit_requests"."modified_at" AS "updated_at"
   FROM "public"."dataset_review_submit_requests"
  WHERE ("dataset_review_submit_requests"."submit_worker_job_id" IS NOT NULL)
UNION ALL
 SELECT "dataset_review_submit_gate_runs"."worker_job_id",
    'dataset_review_submit_gate_runs'::"text" AS "domain_source",
    "dataset_review_submit_gate_runs"."id" AS "domain_id",
    'review_submit_gate_report'::"text" AS "domain_role",
    NULL::"uuid" AS "legacy_job_id",
    "dataset_review_submit_gate_runs"."status",
    "dataset_review_submit_gate_runs"."created_at",
    "dataset_review_submit_gate_runs"."modified_at" AS "updated_at"
   FROM "public"."dataset_review_submit_gate_runs"
  WHERE ("dataset_review_submit_gate_runs"."worker_job_id" IS NOT NULL);


ALTER VIEW "public"."worker_job_domain_refs" OWNER TO "postgres";


COMMENT ON VIEW "public"."worker_job_domain_refs" IS 'Service-role projection from canonical worker_jobs to retained non-legacy domain artifact/cache/history/coordinator rows. Legacy job tables are intentionally excluded so they can be retired with DROP RESTRICT after runtime cutover.';



CREATE TABLE IF NOT EXISTS "public"."worker_job_events" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "job_id" "uuid" NOT NULL,
    "event_type" "text" NOT NULL,
    "status" "text",
    "phase" "text",
    "progress" numeric,
    "worker_id" "text",
    "lease_token" "uuid",
    "message" "text",
    "details" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "worker_job_events_details_object_check" CHECK (("jsonb_typeof"("details") = 'object'::"text")),
    CONSTRAINT "worker_job_events_progress_check" CHECK ((("progress" IS NULL) OR (("progress" >= (0)::numeric) AND ("progress" <= (1)::numeric)))),
    CONSTRAINT "worker_job_events_status_check" CHECK ((("status" IS NULL) OR ("status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'waiting'::"text", 'completed'::"text", 'blocked'::"text", 'stale'::"text", 'failed'::"text", 'cancelled'::"text"]))))
);


ALTER TABLE "public"."worker_job_events" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."worker_job_kinds" (
    "job_kind" "text" NOT NULL,
    "worker_runtime" "text" DEFAULT 'calculator'::"text" NOT NULL,
    "worker_queue" "text" NOT NULL,
    "default_visibility" "text" DEFAULT 'user'::"text" NOT NULL,
    "default_priority" integer DEFAULT 0 NOT NULL,
    "default_max_attempts" integer DEFAULT 3 NOT NULL,
    "default_lease_seconds" integer DEFAULT 300 NOT NULL,
    "payload_schema_version" "text" NOT NULL,
    "result_schema_version" "text",
    "user_visible" boolean DEFAULT true NOT NULL,
    "description" "text",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "worker_job_kinds_default_attempts_check" CHECK (("default_max_attempts" >= 0)),
    CONSTRAINT "worker_job_kinds_default_lease_check" CHECK ((("default_lease_seconds" >= 1) AND ("default_lease_seconds" <= 86400))),
    CONSTRAINT "worker_job_kinds_queue_check" CHECK (("worker_queue" = ANY (ARRAY['solver'::"text", 'review_submit'::"text", 'review_submit_gate'::"text", 'package'::"text", 'maintenance'::"text"]))),
    CONSTRAINT "worker_job_kinds_runtime_check" CHECK (("worker_runtime" = 'calculator'::"text")),
    CONSTRAINT "worker_job_kinds_visibility_check" CHECK (("default_visibility" = ANY (ARRAY['user'::"text", 'operator'::"text", 'system'::"text"])))
);


ALTER TABLE "public"."worker_job_kinds" OWNER TO "postgres";


COMMENT ON COLUMN "public"."worker_job_kinds"."worker_runtime" IS 'Compatibility runtime discriminator; calculator is the existing compute-runtime key, not the repository identity.';



CREATE OR REPLACE VIEW "public"."worker_legacy_lifecycle_audit" WITH ("security_invoker"='true') AS
 SELECT 'worker_jobs'::"text" AS "legacy_source",
    "worker_jobs"."job_kind" AS "task_family",
    "worker_jobs"."status" AS "legacy_status",
    "count"(*) AS "row_count",
    "count"(*) FILTER (WHERE ("worker_jobs"."status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'waiting'::"text", 'stale'::"text", 'blocked'::"text"]))) AS "active_count",
    "min"("worker_jobs"."created_at") AS "oldest_created_at",
    "max"("worker_jobs"."created_at") AS "newest_created_at",
    "max"("worker_jobs"."updated_at") AS "latest_updated_at"
   FROM "public"."worker_jobs"
  WHERE (("worker_jobs"."worker_runtime" = 'calculator'::"text") AND ("worker_jobs"."job_kind" = ANY (ARRAY['lca.solve_one'::"text", 'lca.solve_batch'::"text", 'lca.solve_all_unit'::"text", 'lca.build_snapshot'::"text", 'lca.contribution_path'::"text", 'lca.factorization_prepare'::"text", 'lca.snapshot_gc'::"text", 'lca.result_gc'::"text", 'tidas.package_artifact_gc'::"text", 'tidas.export_package'::"text", 'tidas.import_package'::"text", 'review_submit.submit'::"text", 'review_submit.gate'::"text"])))
  GROUP BY "worker_jobs"."job_kind", "worker_jobs"."status"
UNION ALL
 SELECT 'dataset_review_submit_gate_runs'::"text" AS "legacy_source",
    'review_submit.gate'::"text" AS "task_family",
    "dataset_review_submit_gate_runs"."status" AS "legacy_status",
    "count"(*) AS "row_count",
    "count"(*) FILTER (WHERE ("dataset_review_submit_gate_runs"."status" = ANY (ARRAY['queued'::"text", 'running'::"text"]))) AS "active_count",
    "min"("dataset_review_submit_gate_runs"."created_at") AS "oldest_created_at",
    "max"("dataset_review_submit_gate_runs"."created_at") AS "newest_created_at",
    "max"("dataset_review_submit_gate_runs"."modified_at") AS "latest_updated_at"
   FROM "public"."dataset_review_submit_gate_runs"
  GROUP BY "dataset_review_submit_gate_runs"."status";


ALTER VIEW "public"."worker_legacy_lifecycle_audit" OWNER TO "postgres";


COMMENT ON VIEW "public"."worker_legacy_lifecycle_audit" IS 'Service-role lifecycle audit for canonical worker_jobs and retained gate reports. Legacy job tables are intentionally excluded so they can be retired with DROP RESTRICT after runtime cutover.';



CREATE OR REPLACE VIEW "public"."worker_legacy_table_retirement_blockers" WITH ("security_invoker"='true') AS
 WITH "legacy_targets" AS (
         SELECT "target_namespace"."nspname" AS "legacy_schema",
            "target_class"."relname" AS "legacy_table",
            "target_class"."oid" AS "table_oid",
            "target_class"."reltype" AS "row_type_oid"
           FROM ((( VALUES ('public'::"name",'lca_jobs'::"name"), ('public'::"name",'lca_package_jobs'::"name"), ('public'::"name",'dataset_review_submit_jobs'::"name")) "targets"("schema_name", "table_name")
             JOIN "pg_namespace" "target_namespace" ON (("target_namespace"."nspname" = "targets"."schema_name")))
             JOIN "pg_class" "target_class" ON ((("target_class"."relnamespace" = "target_namespace"."oid") AND ("target_class"."relname" = "targets"."table_name") AND ("target_class"."relkind" = ANY (ARRAY['r'::"char", 'p'::"char"])))))
        ), "foreign_key_blockers" AS (
         SELECT "concat"("legacy_targets"."legacy_schema", '.', "legacy_targets"."legacy_table") AS "legacy_table",
            'foreign_key'::"text" AS "blocker_type",
            ("dependent_namespace"."nspname")::"text" AS "blocker_schema",
            ("dependent_class"."relname")::"text" AS "blocker_name",
            ("constraint_record"."conname")::"text" AS "blocker_identity",
            true AS "is_drop_restrict_blocker",
            "jsonb_build_object"('constraintName', "constraint_record"."conname", 'dependentTable', "concat"("dependent_namespace"."nspname", '.', "dependent_class"."relname"), 'dependentColumns', ( SELECT "jsonb_agg"("dependent_attribute"."attname" ORDER BY "dependent_attribute"."attnum") AS "jsonb_agg"
                   FROM ("unnest"("constraint_record"."conkey") "constraint_column"("attnum")
                     JOIN "pg_attribute" "dependent_attribute" ON ((("dependent_attribute"."attrelid" = "constraint_record"."conrelid") AND ("dependent_attribute"."attnum" = "constraint_column"."attnum"))))), 'referencedColumns', ( SELECT "jsonb_agg"("referenced_attribute"."attname" ORDER BY "referenced_attribute"."attnum") AS "jsonb_agg"
                   FROM ("unnest"("constraint_record"."confkey") "referenced_column"("attnum")
                     JOIN "pg_attribute" "referenced_attribute" ON ((("referenced_attribute"."attrelid" = "constraint_record"."confrelid") AND ("referenced_attribute"."attnum" = "referenced_column"."attnum"))))), 'onDelete', "constraint_record"."confdeltype") AS "details"
           FROM ((("legacy_targets"
             JOIN "pg_constraint" "constraint_record" ON ((("constraint_record"."confrelid" = "legacy_targets"."table_oid") AND ("constraint_record"."contype" = 'f'::"char"))))
             JOIN "pg_class" "dependent_class" ON (("dependent_class"."oid" = "constraint_record"."conrelid")))
             JOIN "pg_namespace" "dependent_namespace" ON (("dependent_namespace"."oid" = "dependent_class"."relnamespace")))
          WHERE ("constraint_record"."conrelid" <> "legacy_targets"."table_oid")
        ), "view_blockers" AS (
         SELECT DISTINCT "concat"("legacy_targets"."legacy_schema", '.', "legacy_targets"."legacy_table") AS "legacy_table",
                CASE "dependent_class"."relkind"
                    WHEN 'm'::"char" THEN 'dependent_materialized_view'::"text"
                    ELSE 'dependent_view'::"text"
                END AS "blocker_type",
            ("dependent_namespace"."nspname")::"text" AS "blocker_schema",
            ("dependent_class"."relname")::"text" AS "blocker_name",
            "concat"("dependent_namespace"."nspname", '.', "dependent_class"."relname") AS "blocker_identity",
            true AS "is_drop_restrict_blocker",
            "jsonb_build_object"('dependentView', "concat"("dependent_namespace"."nspname", '.', "dependent_class"."relname"), 'relkind', "dependent_class"."relkind") AS "details"
           FROM (((("legacy_targets"
             JOIN "pg_depend" "dependency" ON (("dependency"."refobjid" = "legacy_targets"."table_oid")))
             JOIN "pg_rewrite" "rewrite_rule" ON (("rewrite_rule"."oid" = "dependency"."objid")))
             JOIN "pg_class" "dependent_class" ON (("dependent_class"."oid" = "rewrite_rule"."ev_class")))
             JOIN "pg_namespace" "dependent_namespace" ON (("dependent_namespace"."oid" = "dependent_class"."relnamespace")))
          WHERE (("dependent_class"."oid" <> "legacy_targets"."table_oid") AND ("dependent_class"."relkind" = ANY (ARRAY['v'::"char", 'm'::"char"])))
        ), "policy_blockers" AS (
         SELECT DISTINCT "concat"("legacy_targets"."legacy_schema", '.', "legacy_targets"."legacy_table") AS "legacy_table",
            'policy'::"text" AS "blocker_type",
            ("dependent_namespace"."nspname")::"text" AS "blocker_schema",
            ("dependent_class"."relname")::"text" AS "blocker_name",
            "concat"("dependent_namespace"."nspname", '.', "dependent_class"."relname", '.', "policy_record"."polname") AS "blocker_identity",
            true AS "is_drop_restrict_blocker",
            "jsonb_build_object"('policyName', "policy_record"."polname", 'dependentTable', "concat"("dependent_namespace"."nspname", '.', "dependent_class"."relname"), 'command', "policy_record"."polcmd") AS "details"
           FROM (((("legacy_targets"
             JOIN "pg_depend" "dependency" ON (("dependency"."refobjid" = "legacy_targets"."table_oid")))
             JOIN "pg_policy" "policy_record" ON (("policy_record"."oid" = "dependency"."objid")))
             JOIN "pg_class" "dependent_class" ON (("dependent_class"."oid" = "policy_record"."polrelid")))
             JOIN "pg_namespace" "dependent_namespace" ON (("dependent_namespace"."oid" = "dependent_class"."relnamespace")))
          WHERE ("dependent_class"."oid" <> "legacy_targets"."table_oid")
        ), "function_signature_blockers" AS (
         SELECT DISTINCT "concat"("legacy_targets"."legacy_schema", '.', "legacy_targets"."legacy_table") AS "legacy_table",
            'function_signature'::"text" AS "blocker_type",
            ("function_namespace"."nspname")::"text" AS "blocker_schema",
            ("function_record"."proname")::"text" AS "blocker_name",
            "concat"("function_namespace"."nspname", '.', "function_record"."proname", '(', "pg_get_function_identity_arguments"("function_record"."oid"), ')') AS "blocker_identity",
            true AS "is_drop_restrict_blocker",
            "jsonb_build_object"('arguments', "pg_get_function_arguments"("function_record"."oid"), 'result', "pg_get_function_result"("function_record"."oid")) AS "details"
           FROM (("legacy_targets"
             JOIN ( SELECT "pg_proc"."oid",
                    "pg_proc"."proname",
                    "pg_proc"."pronamespace",
                    "pg_proc"."proowner",
                    "pg_proc"."prolang",
                    "pg_proc"."procost",
                    "pg_proc"."prorows",
                    "pg_proc"."provariadic",
                    "pg_proc"."prosupport",
                    "pg_proc"."prokind",
                    "pg_proc"."prosecdef",
                    "pg_proc"."proleakproof",
                    "pg_proc"."proisstrict",
                    "pg_proc"."proretset",
                    "pg_proc"."provolatile",
                    "pg_proc"."proparallel",
                    "pg_proc"."pronargs",
                    "pg_proc"."pronargdefaults",
                    "pg_proc"."prorettype",
                    "pg_proc"."proargtypes",
                    "pg_proc"."proallargtypes",
                    "pg_proc"."proargmodes",
                    "pg_proc"."proargnames",
                    "pg_proc"."proargdefaults",
                    "pg_proc"."protrftypes",
                    "pg_proc"."prosrc",
                    "pg_proc"."probin",
                    "pg_proc"."prosqlbody",
                    "pg_proc"."proconfig",
                    "pg_proc"."proacl"
                   FROM "pg_proc"
                  WHERE ("pg_proc"."prokind" = ANY (ARRAY['f'::"char", 'p'::"char", 'w'::"char"]))) "function_record" ON ((("lower"("pg_get_function_arguments"("function_record"."oid")) ~~ (('%'::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%'::"text")) OR ("lower"("pg_get_function_result"("function_record"."oid")) ~~ (('%'::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%'::"text")))))
             JOIN "pg_namespace" "function_namespace" ON (("function_namespace"."oid" = "function_record"."pronamespace")))
        ), "function_source_references" AS (
         SELECT DISTINCT "concat"("legacy_targets"."legacy_schema", '.', "legacy_targets"."legacy_table") AS "legacy_table",
            'function_source_reference'::"text" AS "blocker_type",
            ("function_namespace"."nspname")::"text" AS "blocker_schema",
            ("function_record"."proname")::"text" AS "blocker_name",
            "concat"("function_namespace"."nspname", '.', "function_record"."proname", '(', "pg_get_function_identity_arguments"("function_record"."oid"), ')') AS "blocker_identity",
            false AS "is_drop_restrict_blocker",
            "jsonb_build_object"('reason', 'Function body references the legacy table name; this may not block DROP TABLE RESTRICT, but it is a runtime migration blocker.', 'arguments', "pg_get_function_arguments"("function_record"."oid"), 'result', "pg_get_function_result"("function_record"."oid")) AS "details"
           FROM (("legacy_targets"
             JOIN ( SELECT "pg_proc"."oid",
                    "pg_proc"."proname",
                    "pg_proc"."pronamespace",
                    "pg_proc"."proowner",
                    "pg_proc"."prolang",
                    "pg_proc"."procost",
                    "pg_proc"."prorows",
                    "pg_proc"."provariadic",
                    "pg_proc"."prosupport",
                    "pg_proc"."prokind",
                    "pg_proc"."prosecdef",
                    "pg_proc"."proleakproof",
                    "pg_proc"."proisstrict",
                    "pg_proc"."proretset",
                    "pg_proc"."provolatile",
                    "pg_proc"."proparallel",
                    "pg_proc"."pronargs",
                    "pg_proc"."pronargdefaults",
                    "pg_proc"."prorettype",
                    "pg_proc"."proargtypes",
                    "pg_proc"."proallargtypes",
                    "pg_proc"."proargmodes",
                    "pg_proc"."proargnames",
                    "pg_proc"."proargdefaults",
                    "pg_proc"."protrftypes",
                    "pg_proc"."prosrc",
                    "pg_proc"."probin",
                    "pg_proc"."prosqlbody",
                    "pg_proc"."proconfig",
                    "pg_proc"."proacl"
                   FROM "pg_proc"
                  WHERE ("pg_proc"."prokind" = ANY (ARRAY['f'::"char", 'p'::"char", 'w'::"char"]))) "function_record" ON ((("lower"("function_record"."prosrc") ~~ (('%public.'::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%'::"text")) OR ("lower"("function_record"."prosrc") ~~ (('%from '::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%'::"text")) OR ("lower"("function_record"."prosrc") ~~ (('%join '::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%'::"text")) OR ("lower"("function_record"."prosrc") ~~ (('%update '::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%'::"text")) OR ("lower"("function_record"."prosrc") ~~ (('%insert into '::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%'::"text")) OR ("lower"("function_record"."prosrc") ~~ (('%delete from '::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%'::"text")) OR ("lower"("function_record"."prosrc") ~~ (('%'::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%rowtype%'::"text")))))
             JOIN "pg_namespace" "function_namespace" ON (("function_namespace"."oid" = "function_record"."pronamespace")))
          WHERE ("function_namespace"."nspname" <> ALL (ARRAY['pg_catalog'::"name", 'information_schema'::"name"]))
        )
 SELECT "foreign_key_blockers"."legacy_table",
    "foreign_key_blockers"."blocker_type",
    "foreign_key_blockers"."blocker_schema",
    "foreign_key_blockers"."blocker_name",
    "foreign_key_blockers"."blocker_identity",
    "foreign_key_blockers"."is_drop_restrict_blocker",
    "foreign_key_blockers"."details"
   FROM "foreign_key_blockers"
UNION ALL
 SELECT "view_blockers"."legacy_table",
    "view_blockers"."blocker_type",
    "view_blockers"."blocker_schema",
    "view_blockers"."blocker_name",
    "view_blockers"."blocker_identity",
    "view_blockers"."is_drop_restrict_blocker",
    "view_blockers"."details"
   FROM "view_blockers"
UNION ALL
 SELECT "policy_blockers"."legacy_table",
    "policy_blockers"."blocker_type",
    "policy_blockers"."blocker_schema",
    "policy_blockers"."blocker_name",
    "policy_blockers"."blocker_identity",
    "policy_blockers"."is_drop_restrict_blocker",
    "policy_blockers"."details"
   FROM "policy_blockers"
UNION ALL
 SELECT "function_signature_blockers"."legacy_table",
    "function_signature_blockers"."blocker_type",
    "function_signature_blockers"."blocker_schema",
    "function_signature_blockers"."blocker_name",
    "function_signature_blockers"."blocker_identity",
    "function_signature_blockers"."is_drop_restrict_blocker",
    "function_signature_blockers"."details"
   FROM "function_signature_blockers"
UNION ALL
 SELECT "function_source_references"."legacy_table",
    "function_source_references"."blocker_type",
    "function_source_references"."blocker_schema",
    "function_source_references"."blocker_name",
    "function_source_references"."blocker_identity",
    "function_source_references"."is_drop_restrict_blocker",
    "function_source_references"."details"
   FROM "function_source_references";


ALTER VIEW "public"."worker_legacy_table_retirement_blockers" OWNER TO "postgres";


COMMENT ON VIEW "public"."worker_legacy_table_retirement_blockers" IS 'Service-role audit view for DROP TABLE RESTRICT blockers. It returns no target rows after public.lca_jobs, public.lca_package_jobs, and public.dataset_review_submit_jobs are physically retired.';



ALTER TABLE ONLY "public"."command_audit_log"
    ADD CONSTRAINT "command_audit_log_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."comments"
    ADD CONSTRAINT "comments_pkey" PRIMARY KEY ("review_id", "reviewer_id");



ALTER TABLE ONLY "public"."contacts"
    ADD CONSTRAINT "contacts_pkey" PRIMARY KEY ("id", "version");



ALTER TABLE ONLY "public"."dataset_review_submit_gate_runs"
    ADD CONSTRAINT "dataset_review_submit_gate_runs_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."dataset_review_submit_requests"
    ADD CONSTRAINT "dataset_review_submit_requests_pkey" PRIMARY KEY ("id");



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



ALTER TABLE ONLY "public"."lca_snapshot_gc_run_items"
    ADD CONSTRAINT "lca_snapshot_gc_run_items_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."lca_snapshot_gc_runs"
    ADD CONSTRAINT "lca_snapshot_gc_runs_pkey" PRIMARY KEY ("id");



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



ALTER TABLE ONLY "public"."worker_job_artifacts"
    ADD CONSTRAINT "worker_job_artifacts_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."worker_job_events"
    ADD CONSTRAINT "worker_job_events_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."worker_job_kinds"
    ADD CONSTRAINT "worker_job_kinds_pkey" PRIMARY KEY ("job_kind");



ALTER TABLE ONLY "public"."worker_jobs"
    ADD CONSTRAINT "worker_jobs_pkey" PRIMARY KEY ("id");



CREATE INDEX "contacts_created_at_idx" ON "public"."contacts" USING "btree" ("created_at" DESC);



CREATE INDEX "contacts_json_dataversion" ON "public"."contacts" USING "btree" (((((("json" -> 'contactDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "contacts_json_email" ON "public"."contacts" USING "btree" (((((("json" -> 'contactDataSet'::"text") -> 'contactInformation'::"text") -> 'dataSetInformation'::"text") ->> 'email'::"text")));



CREATE INDEX "contacts_json_idx" ON "public"."contacts" USING "gin" ("json");



CREATE INDEX "contacts_json_ordered_vector" ON "public"."contacts" USING "hnsw" ("embedding" "extensions"."vector_cosine_ops");



CREATE INDEX "contacts_state_code_id_version_modified_at_idx" ON "public"."contacts" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "contacts_team_id_state_code_id_version_modified_at_idx" ON "public"."contacts" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "contacts_text_pgroonga" ON "public"."contacts" USING "pgroonga" ("extracted_text");



CREATE INDEX "contacts_user_id_created_at_idx" ON "public"."contacts" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "contacts_user_id_state_code_id_version_modified_at_idx" ON "public"."contacts" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "dataset_review_submit_gate_runs_requested_by_idx" ON "public"."dataset_review_submit_gate_runs" USING "btree" ("requested_by", "created_at" DESC);



CREATE INDEX "dataset_review_submit_gate_runs_revision_idx" ON "public"."dataset_review_submit_gate_runs" USING "btree" ("dataset_table", "dataset_id", "dataset_version", "revision_checksum", "policy_profile", "report_schema_version", "created_at" DESC);



CREATE INDEX "dataset_review_submit_gate_runs_status_idx" ON "public"."dataset_review_submit_gate_runs" USING "btree" ("status", "modified_at" DESC);



CREATE INDEX "dataset_review_submit_gate_runs_worker_job_idx" ON "public"."dataset_review_submit_gate_runs" USING "btree" ("worker_job_id") WHERE ("worker_job_id" IS NOT NULL);



CREATE UNIQUE INDEX "dataset_review_submit_requests_active_revision_uidx" ON "public"."dataset_review_submit_requests" USING "btree" ("dataset_table", "dataset_id", "dataset_version", "revision_checksum", "policy_profile", "report_schema_version", "requested_by") WHERE ("status" = ANY (ARRAY['queued'::"text", 'waiting_gate'::"text", 'submitting'::"text"]));



CREATE INDEX "dataset_review_submit_requests_gate_run_idx" ON "public"."dataset_review_submit_requests" USING "btree" ("gate_run_id") WHERE ("gate_run_id" IS NOT NULL);



CREATE INDEX "dataset_review_submit_requests_gate_worker_job_idx" ON "public"."dataset_review_submit_requests" USING "btree" ("gate_worker_job_id") WHERE ("gate_worker_job_id" IS NOT NULL);



CREATE INDEX "dataset_review_submit_requests_requested_by_idx" ON "public"."dataset_review_submit_requests" USING "btree" ("requested_by", "created_at" DESC);



CREATE INDEX "dataset_review_submit_requests_status_idx" ON "public"."dataset_review_submit_requests" USING "btree" ("status", "modified_at", "created_at");



CREATE INDEX "dataset_review_submit_requests_submit_worker_job_idx" ON "public"."dataset_review_submit_requests" USING "btree" ("submit_worker_job_id") WHERE ("submit_worker_job_id" IS NOT NULL);



CREATE UNIQUE INDEX "file_name_index" ON "public"."ilcd" USING "btree" ("file_name");



CREATE INDEX "flowproperties_created_at_idx" ON "public"."flowproperties" USING "btree" ("created_at" DESC);



CREATE INDEX "flowproperties_json_dataversion" ON "public"."flowproperties" USING "btree" (((((("json" -> 'flowPropertyDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "flowproperties_json_idx" ON "public"."flowproperties" USING "gin" ("json");



CREATE INDEX "flowproperties_json_ordered_vector" ON "public"."flowproperties" USING "hnsw" ("embedding" "extensions"."vector_cosine_ops");



CREATE INDEX "flowproperties_json_refobjectid" ON "public"."flowproperties" USING "btree" ((((((("json" -> 'flowPropertyDataSet'::"text") -> 'flowPropertiesInformation'::"text") -> 'quantitativeReference'::"text") -> 'referenceToReferenceUnitGroup'::"text") ->> '@refObjectId'::"text")));



CREATE INDEX "flowproperties_modified_at_idx" ON "public"."flowproperties" USING "btree" ("modified_at");



CREATE INDEX "flowproperties_state_code_id_version_modified_at_idx" ON "public"."flowproperties" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "flowproperties_team_id_state_code_id_version_modified_at_idx" ON "public"."flowproperties" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "flowproperties_text_pgroonga" ON "public"."flowproperties" USING "pgroonga" ("extracted_text");



CREATE INDEX "flowproperties_user_id_created_at_idx" ON "public"."flowproperties" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "flowproperties_user_id_state_code_id_version_modified_at_idx" ON "public"."flowproperties" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "flows_composite_idx" ON "public"."flows" USING "btree" (((((("json" -> 'flowDataSet'::"text") -> 'modellingAndValidation'::"text") -> 'LCIMethod'::"text") ->> 'typeOfDataSet'::"text")), "state_code", "modified_at" DESC);



CREATE INDEX "flows_created_at_idx" ON "public"."flows" USING "btree" ("created_at" DESC);



CREATE INDEX "flows_embedding_ft_hnsw_idx" ON "public"."flows" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops");



CREATE INDEX "flows_json_casnumber" ON "public"."flows" USING "btree" (((((("json" -> 'flowDataSet'::"text") -> 'flowInformation'::"text") -> 'dataSetInformation'::"text") ->> 'CASNumber'::"text")));



CREATE INDEX "flows_json_dataversion" ON "public"."flows" USING "btree" (((((("json" -> 'flowDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "flows_json_locationofsupply" ON "public"."flows" USING "btree" (((((("json" -> 'flowDataSet'::"text") -> 'flowInformation'::"text") -> 'geography'::"text") ->> 'locationOfSupply'::"text")));



CREATE INDEX "flows_json_typeofdataset" ON "public"."flows" USING "btree" (((((("json" -> 'flowDataSet'::"text") -> 'modellingAndValidation'::"text") -> 'LCIMethod'::"text") ->> 'typeOfDataSet'::"text")));



CREATE INDEX "flows_modified_at_idx" ON "public"."flows" USING "btree" ("modified_at");



CREATE INDEX "flows_not_emissions_idx" ON "public"."flows" USING "btree" ("state_code", "modified_at" DESC) WHERE (NOT ("json" @> '{"flowDataSet": {"flowInformation": {"dataSetInformation": {"classificationInformation": {"common:elementaryFlowCategorization": {"common:category": [{"#text": "Emissions", "@level": "0"}]}}}}}}'::"jsonb"));



CREATE INDEX "flows_public_latest_keys_cover_idx" ON "public"."flows" USING "btree" ("id", "version" DESC, "modified_at" DESC) INCLUDE ("created_at", "team_id") WHERE ("state_code" = 100);



CREATE INDEX "flows_review_id_idx" ON "public"."flows" USING "btree" ("review_id");



CREATE INDEX "flows_state_code_id_version_modified_at_idx" ON "public"."flows" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "flows_state_code_idx" ON "public"."flows" USING "btree" ("state_code");



CREATE INDEX "flows_team_id_idx" ON "public"."flows" USING "btree" ("team_id");



CREATE INDEX "flows_team_id_state_code_id_version_modified_at_idx" ON "public"."flows" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "flows_text_pgroonga" ON "public"."flows" USING "pgroonga" ("extracted_text");



CREATE INDEX "flows_user_id_created_at_idx" ON "public"."flows" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "flows_user_id_state_code_id_version_modified_at_idx" ON "public"."flows" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "ilcd_created_at_idx" ON "public"."ilcd" USING "btree" ("created_at" DESC);



CREATE INDEX "ilcd_json_idx" ON "public"."ilcd" USING "gin" ("json");



CREATE INDEX "ilcd_modified_at_idx" ON "public"."ilcd" USING "btree" ("modified_at");



CREATE INDEX "ilcd_user_id_created_at_idx" ON "public"."ilcd" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "lca_active_snapshots_snapshot_idx" ON "public"."lca_active_snapshots" USING "btree" ("snapshot_id");



CREATE INDEX "lca_factorization_registry_prepared_worker_job_idx" ON "public"."lca_factorization_registry" USING "btree" ("prepared_worker_job_id") WHERE ("prepared_worker_job_id" IS NOT NULL);



CREATE INDEX "lca_factorization_registry_snapshot_status_idx" ON "public"."lca_factorization_registry" USING "btree" ("snapshot_id", "status", "updated_at" DESC);



CREATE INDEX "lca_factorization_registry_status_lease_idx" ON "public"."lca_factorization_registry" USING "btree" ("status", "lease_until");



CREATE INDEX "lca_latest_all_unit_results_computed_idx" ON "public"."lca_latest_all_unit_results" USING "btree" ("computed_at" DESC);



CREATE INDEX "lca_latest_all_unit_results_result_idx" ON "public"."lca_latest_all_unit_results" USING "btree" ("result_id");



CREATE INDEX "lca_latest_all_unit_results_worker_job_idx" ON "public"."lca_latest_all_unit_results" USING "btree" ("worker_job_id") WHERE ("worker_job_id" IS NOT NULL);



CREATE INDEX "lca_network_snapshots_status_created_idx" ON "public"."lca_network_snapshots" USING "btree" ("status", "created_at" DESC);



CREATE INDEX "lca_network_snapshots_updated_idx" ON "public"."lca_network_snapshots" USING "btree" ("updated_at" DESC);



CREATE INDEX "lca_package_artifacts_job_created_idx" ON "public"."lca_package_artifacts" USING "btree" ("job_id", "created_at" DESC);



CREATE UNIQUE INDEX "lca_package_artifacts_job_kind_uidx" ON "public"."lca_package_artifacts" USING "btree" ("job_id", "artifact_kind");



CREATE INDEX "lca_package_artifacts_status_created_idx" ON "public"."lca_package_artifacts" USING "btree" ("status", "created_at" DESC);



CREATE INDEX "lca_package_artifacts_worker_job_idx" ON "public"."lca_package_artifacts" USING "btree" ("worker_job_id") WHERE ("worker_job_id" IS NOT NULL);



CREATE UNIQUE INDEX "lca_package_export_items_job_dataset_uidx" ON "public"."lca_package_export_items" USING "btree" ("job_id", "table_name", "dataset_id", "version");



CREATE INDEX "lca_package_export_items_job_refs_idx" ON "public"."lca_package_export_items" USING "btree" ("job_id", "refs_done", "created_at", "table_name");



CREATE INDEX "lca_package_export_items_job_seed_idx" ON "public"."lca_package_export_items" USING "btree" ("job_id", "is_seed", "created_at");



CREATE INDEX "lca_package_export_items_worker_job_idx" ON "public"."lca_package_export_items" USING "btree" ("worker_job_id") WHERE ("worker_job_id" IS NOT NULL);



CREATE UNIQUE INDEX "lca_package_request_cache_job_uidx" ON "public"."lca_package_request_cache" USING "btree" ("job_id") WHERE ("job_id" IS NOT NULL);



CREATE INDEX "lca_package_request_cache_last_accessed_idx" ON "public"."lca_package_request_cache" USING "btree" ("last_accessed_at" DESC);



CREATE INDEX "lca_package_request_cache_lookup_idx" ON "public"."lca_package_request_cache" USING "btree" ("requested_by", "operation", "status", "updated_at" DESC);



CREATE INDEX "lca_package_request_cache_worker_job_idx" ON "public"."lca_package_request_cache" USING "btree" ("worker_job_id") WHERE ("worker_job_id" IS NOT NULL);



CREATE UNIQUE INDEX "lca_result_cache_job_uidx" ON "public"."lca_result_cache" USING "btree" ("job_id") WHERE ("job_id" IS NOT NULL);



CREATE INDEX "lca_result_cache_last_accessed_idx" ON "public"."lca_result_cache" USING "btree" ("last_accessed_at" DESC);



CREATE INDEX "lca_result_cache_lookup_idx" ON "public"."lca_result_cache" USING "btree" ("scope", "snapshot_id", "status", "updated_at" DESC);



CREATE UNIQUE INDEX "lca_result_cache_result_uidx" ON "public"."lca_result_cache" USING "btree" ("result_id") WHERE ("result_id" IS NOT NULL);



CREATE INDEX "lca_result_cache_worker_job_idx" ON "public"."lca_result_cache" USING "btree" ("worker_job_id") WHERE ("worker_job_id" IS NOT NULL);



CREATE INDEX "lca_results_created_desc_idx" ON "public"."lca_results" USING "btree" ("created_at" DESC);



CREATE INDEX "lca_results_expires_at_idx" ON "public"."lca_results" USING "btree" ("expires_at", "created_at") WHERE ("is_pinned" = false);



CREATE INDEX "lca_results_job_idx" ON "public"."lca_results" USING "btree" ("job_id");



CREATE INDEX "lca_results_snapshot_created_idx" ON "public"."lca_results" USING "btree" ("snapshot_id", "created_at" DESC);



CREATE INDEX "lca_results_worker_job_idx" ON "public"."lca_results" USING "btree" ("worker_job_id") WHERE ("worker_job_id" IS NOT NULL);



CREATE INDEX "lca_snapshot_artifacts_created_idx" ON "public"."lca_snapshot_artifacts" USING "btree" ("created_at" DESC);



CREATE UNIQUE INDEX "lca_snapshot_artifacts_snapshot_format_key" ON "public"."lca_snapshot_artifacts" USING "btree" ("snapshot_id", "artifact_format");



CREATE INDEX "lca_snapshot_artifacts_snapshot_status_idx" ON "public"."lca_snapshot_artifacts" USING "btree" ("snapshot_id", "status", "created_at" DESC);



CREATE INDEX "lca_snapshot_gc_run_items_run_idx" ON "public"."lca_snapshot_gc_run_items" USING "btree" ("run_id");



CREATE INDEX "lca_snapshot_gc_run_items_snapshot_idx" ON "public"."lca_snapshot_gc_run_items" USING "btree" ("snapshot_id") WHERE ("snapshot_id" IS NOT NULL);



CREATE INDEX "lca_snapshot_gc_run_items_status_idx" ON "public"."lca_snapshot_gc_run_items" USING "btree" ("action_status", "created_at" DESC);



CREATE INDEX "lca_snapshot_gc_runs_started_idx" ON "public"."lca_snapshot_gc_runs" USING "btree" ("started_at" DESC);



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



CREATE INDEX "lifecyclemodels_json_tg_idx" ON "public"."lifecyclemodels" USING "gin" ("json_tg");



CREATE INDEX "lifecyclemodels_modified_at_idx" ON "public"."lifecyclemodels" USING "btree" ("modified_at");



CREATE INDEX "lifecyclemodels_public_latest_keys_cover_idx" ON "public"."lifecyclemodels" USING "btree" ("id", "version" DESC, "modified_at" DESC) INCLUDE ("created_at", "team_id") WHERE ("state_code" = 100);



CREATE INDEX "lifecyclemodels_state_code_id_version_modified_at_idx" ON "public"."lifecyclemodels" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "lifecyclemodels_team_id_state_code_id_version_modified_at_idx" ON "public"."lifecyclemodels" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "lifecyclemodels_text_pgroonga" ON "public"."lifecyclemodels" USING "pgroonga" ("extracted_text");



CREATE INDEX "lifecyclemodels_user_id_created_at_idx" ON "public"."lifecyclemodels" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "lifecyclemodels_user_id_state_code_id_version_modified_at_idx" ON "public"."lifecyclemodels" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE UNIQUE INDEX "notifications_recipient_sender_type_dataset_uq" ON "public"."notifications" USING "btree" ("recipient_user_id", "sender_user_id", "type", "dataset_type", "dataset_id", "dataset_version");



CREATE INDEX "notifications_recipient_type_modified_idx" ON "public"."notifications" USING "btree" ("recipient_user_id", "type", "modified_at" DESC);



CREATE INDEX "processes_created_at_idx" ON "public"."processes" USING "btree" ("created_at" DESC);



CREATE INDEX "processes_embedding_ft_hnsw_idx" ON "public"."processes" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops");



CREATE INDEX "processes_json_dataversion" ON "public"."processes" USING "btree" (((((("json" -> 'processDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "processes_json_exchange_gin_idx" ON "public"."processes" USING "gin" ((((("json" -> 'processDataSet'::"text") -> 'exchanges'::"text") -> 'exchange'::"text")));



CREATE INDEX "processes_json_location" ON "public"."processes" USING "btree" ((((((("json" -> 'processDataSet'::"text") -> 'processInformation'::"text") -> 'geography'::"text") -> 'locationOfOperationSupplyOrProduction'::"text") ->> '@location'::"text")));



CREATE INDEX "processes_json_referenceyear" ON "public"."processes" USING "btree" (((((("json" -> 'processDataSet'::"text") -> 'processInformation'::"text") -> 'time'::"text") ->> 'common:referenceYear'::"text")));



CREATE INDEX "processes_modified_at_idx" ON "public"."processes" USING "btree" ("modified_at");



CREATE INDEX "processes_public_latest_keys_cover_idx" ON "public"."processes" USING "btree" ("id", "version" DESC, "modified_at" DESC) INCLUDE ("created_at", "team_id", "model_id") WHERE ("state_code" = 100);



CREATE INDEX "processes_review_id_idx" ON "public"."processes" USING "btree" ("review_id");



CREATE INDEX "processes_rule_verification_idx" ON "public"."processes" USING "btree" ("rule_verification");



CREATE INDEX "processes_state_code_id_version_modified_at_idx" ON "public"."processes" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "processes_state_code_idx" ON "public"."processes" USING "btree" ("state_code");



CREATE INDEX "processes_team_id_idx" ON "public"."processes" USING "btree" ("team_id");



CREATE INDEX "processes_team_id_state_code_id_version_modified_at_idx" ON "public"."processes" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "processes_text_pgroonga" ON "public"."processes" USING "pgroonga" ("extracted_text");



CREATE INDEX "processes_user_id_created_at_idx" ON "public"."processes" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "processes_user_id_state_code_id_version_modified_at_idx" ON "public"."processes" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "reviews_data_id_data_version_idx" ON "public"."reviews" USING "btree" ("data_id", "data_version");



CREATE INDEX "roles_role_idx" ON "public"."roles" USING "btree" ("role");



CREATE INDEX "roles_team_id_user_id_role_idx" ON "public"."roles" USING "btree" ("team_id", "user_id", "role");



CREATE INDEX "sources_created_at_idx" ON "public"."sources" USING "btree" ("created_at" DESC);



CREATE INDEX "sources_json_dataversion" ON "public"."sources" USING "btree" (((((("json" -> 'sourceDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "sources_json_idx" ON "public"."sources" USING "gin" ("json");



CREATE INDEX "sources_json_ordered_vector" ON "public"."sources" USING "hnsw" ("embedding" "extensions"."vector_cosine_ops");



CREATE INDEX "sources_json_publicationtype" ON "public"."sources" USING "btree" (((((("json" -> 'sourceDataSet'::"text") -> 'sourceInformation'::"text") -> 'dataSetInformation'::"text") ->> 'publicationType'::"text")));



CREATE INDEX "sources_json_sourcecitation" ON "public"."sources" USING "btree" (((((("json" -> 'sourceDataSet'::"text") -> 'sourceInformation'::"text") -> 'dataSetInformation'::"text") ->> 'sourceCitation'::"text")));



CREATE INDEX "sources_modified_at_idx" ON "public"."sources" USING "btree" ("modified_at");



CREATE INDEX "sources_state_code_id_version_modified_at_idx" ON "public"."sources" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "sources_team_id_state_code_id_version_modified_at_idx" ON "public"."sources" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "sources_text_pgroonga" ON "public"."sources" USING "pgroonga" ("extracted_text");



CREATE INDEX "sources_user_id_created_at_idx" ON "public"."sources" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "sources_user_id_id_version_modified_at_latest_idx" ON "public"."sources" USING "btree" ("user_id", "id", "version" DESC, "modified_at" DESC) INCLUDE ("created_at", "team_id", "state_code");



CREATE INDEX "sources_user_id_state_code_id_version_modified_at_idx" ON "public"."sources" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "unitgroups_created_at_idx" ON "public"."unitgroups" USING "btree" ("created_at" DESC);



CREATE INDEX "unitgroups_json_dataversion" ON "public"."unitgroups" USING "btree" (((((("json" -> 'unitGroupDataSet'::"text") -> 'administrativeInformation'::"text") -> 'publicationAndOwnership'::"text") ->> 'common:dataSetVersion'::"text")));



CREATE INDEX "unitgroups_json_idx" ON "public"."unitgroups" USING "gin" ("json");



CREATE INDEX "unitgroups_json_ordered_vector" ON "public"."unitgroups" USING "hnsw" ("embedding" "extensions"."vector_cosine_ops");



CREATE INDEX "unitgroups_json_referencetoreferenceunit" ON "public"."unitgroups" USING "btree" (((((("json" -> 'unitGroupDataSet'::"text") -> 'unitGroupInformation'::"text") -> 'quantitativeReference'::"text") ->> 'referenceToReferenceUnit'::"text")));



CREATE INDEX "unitgroups_modified_at_idx" ON "public"."unitgroups" USING "btree" ("modified_at");



CREATE INDEX "unitgroups_state_code_id_version_modified_at_idx" ON "public"."unitgroups" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "unitgroups_team_id_state_code_id_version_modified_at_idx" ON "public"."unitgroups" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "unitgroups_text_pgroonga" ON "public"."unitgroups" USING "pgroonga" ("extracted_text");



CREATE INDEX "unitgroups_user_id_created_at_idx" ON "public"."unitgroups" USING "btree" ("user_id", "created_at" DESC);



CREATE INDEX "unitgroups_user_id_state_code_id_version_modified_at_idx" ON "public"."unitgroups" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);



CREATE INDEX "worker_job_artifacts_job_created_idx" ON "public"."worker_job_artifacts" USING "btree" ("job_id", "created_at" DESC);



CREATE INDEX "worker_job_events_job_created_idx" ON "public"."worker_job_events" USING "btree" ("job_id", "created_at" DESC);



CREATE INDEX "worker_jobs_claim_idx" ON "public"."worker_jobs" USING "btree" ("worker_runtime", "worker_queue", "priority" DESC, "run_after", "created_at") WHERE ("status" = ANY (ARRAY['queued'::"text", 'stale'::"text"]));



CREATE UNIQUE INDEX "worker_jobs_concurrency_active_uidx" ON "public"."worker_jobs" USING "btree" ("worker_runtime", "worker_queue", "concurrency_key") WHERE (("concurrency_key" IS NOT NULL) AND ("status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'waiting'::"text", 'stale'::"text"])));



CREATE INDEX "worker_jobs_expired_running_idx" ON "public"."worker_jobs" USING "btree" ("worker_runtime", "worker_queue", "lease_expires_at") WHERE ("status" = 'running'::"text");



CREATE UNIQUE INDEX "worker_jobs_idempotency_active_uidx" ON "public"."worker_jobs" USING "btree" ("worker_runtime", "job_kind", COALESCE("requested_by", '00000000-0000-0000-0000-000000000000'::"uuid"), "idempotency_key") WHERE (("idempotency_key" IS NOT NULL) AND ("status" = ANY (ARRAY['queued'::"text", 'running'::"text", 'waiting'::"text", 'stale'::"text", 'blocked'::"text"])));



CREATE INDEX "worker_jobs_parent_idx" ON "public"."worker_jobs" USING "btree" ("parent_job_id") WHERE ("parent_job_id" IS NOT NULL);



CREATE INDEX "worker_jobs_requested_by_updated_idx" ON "public"."worker_jobs" USING "btree" ("requested_by", "updated_at" DESC) WHERE (("visibility" = 'user'::"text") AND ("requested_by" IS NOT NULL));



CREATE INDEX "worker_jobs_requested_kind_updated_idx" ON "public"."worker_jobs" USING "btree" ("requested_by", "job_kind", "updated_at" DESC) WHERE ("requested_by" IS NOT NULL);



CREATE INDEX "worker_jobs_root_idx" ON "public"."worker_jobs" USING "btree" ("root_job_id") WHERE ("root_job_id" IS NOT NULL);



CREATE INDEX "worker_jobs_subject_kind_updated_idx" ON "public"."worker_jobs" USING "btree" ("subject_type", "subject_id", "subject_version", "job_kind", "updated_at" DESC) WHERE (("subject_type" IS NOT NULL) AND ("subject_id" IS NOT NULL));



CREATE INDEX "worker_jobs_subject_updated_idx" ON "public"."worker_jobs" USING "btree" ("subject_type", "subject_id", "subject_version", "updated_at" DESC) WHERE (("subject_type" IS NOT NULL) AND ("subject_id" IS NOT NULL));



CREATE OR REPLACE TRIGGER "contacts_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."contacts" FOR EACH ROW EXECUTE FUNCTION "public"."contacts_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "contacts_set_modified_at_trigger" BEFORE UPDATE ON "public"."contacts" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "dataset_review_submit_requests_assign_submit_worker_job_trigger" BEFORE INSERT OR UPDATE OF "submit_worker_job_id" ON "public"."dataset_review_submit_requests" FOR EACH ROW EXECUTE FUNCTION "public"."dataset_review_submit_requests_assign_submit_worker_job"();



CREATE OR REPLACE TRIGGER "dataset_review_submit_requests_sync_submit_worker_job_trigger" AFTER INSERT OR UPDATE OF "status", "last_error_code", "last_error_message", "last_error_details", "result", "submit_worker_job_id" ON "public"."dataset_review_submit_requests" FOR EACH ROW EXECUTE FUNCTION "public"."dataset_review_submit_requests_sync_submit_worker_job"();



CREATE OR REPLACE TRIGGER "flow_dataset_extraction_trigger_insert" AFTER INSERT ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "util"."queue_dataset_extraction_jobs"();



CREATE OR REPLACE TRIGGER "flow_embedding_ft_on_extract_md_update" AFTER UPDATE OF "extracted_md" ON "public"."flows" FOR EACH ROW WHEN (("old"."extracted_md" IS DISTINCT FROM "new"."extracted_md")) EXECUTE FUNCTION "util"."queue_embeddings"('flows_embedding_ft_input', 'embedding_ft', 'embedding_ft');



CREATE OR REPLACE TRIGGER "flow_extract_md_trigger_update" AFTER UPDATE OF "json" ON "public"."flows" FOR EACH ROW WHEN (("new"."json" IS DISTINCT FROM "old"."json")) EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_flow_embedding_ft', '1000');



CREATE OR REPLACE TRIGGER "flowproperties_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."flowproperties" FOR EACH ROW EXECUTE FUNCTION "public"."flowproperties_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "flowproperties_set_modified_at_trigger" BEFORE UPDATE ON "public"."flowproperties" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "flows_json_sync_trigger" BEFORE INSERT OR UPDATE OF "json_ordered" ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "public"."flows_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "flows_set_modified_at_trigger" BEFORE UPDATE OF "json", "json_ordered", "user_id", "state_code", "version", "team_id", "review_id", "rule_verification", "reviews", "embedding_flag" ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "ilcd_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."ilcd" FOR EACH ROW EXECUTE FUNCTION "public"."sync_json_to_jsonb"();



CREATE OR REPLACE TRIGGER "ilcd_set_modified_at_trigger" BEFORE UPDATE ON "public"."ilcd" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "lciamethods_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."lciamethods" FOR EACH ROW EXECUTE FUNCTION "public"."lciamethods_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "lciamethods_set_modified_at_trigger" BEFORE UPDATE ON "public"."lciamethods" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "lifecyclemodel_embedding_ft_on_extract_md_update" AFTER UPDATE OF "extracted_md" ON "public"."lifecyclemodels" FOR EACH ROW WHEN (("old"."extracted_md" IS DISTINCT FROM "new"."extracted_md")) EXECUTE FUNCTION "util"."queue_embeddings"('lifecyclemodels_embedding_ft_input', 'embedding_ft', 'embedding_ft');



CREATE OR REPLACE TRIGGER "lifecyclemodel_extract_md_trigger_insert" AFTER INSERT ON "public"."lifecyclemodels" FOR EACH ROW EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_model_embedding_ft', '1000');



CREATE OR REPLACE TRIGGER "lifecyclemodel_extract_md_trigger_update" AFTER UPDATE OF "json" ON "public"."lifecyclemodels" FOR EACH ROW WHEN (("new"."json" IS DISTINCT FROM "old"."json")) EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_model_embedding_ft', '1000');



CREATE OR REPLACE TRIGGER "lifecyclemodels_json_sync_trigger" BEFORE INSERT OR UPDATE OF "json_ordered" ON "public"."lifecyclemodels" FOR EACH ROW EXECUTE FUNCTION "public"."lifecyclemodels_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "lifecyclemodels_set_modified_at_trigger" BEFORE UPDATE OF "json", "json_ordered", "user_id", "state_code", "version", "json_tg", "team_id", "rule_verification", "reviews", "embedding_flag" ON "public"."lifecyclemodels" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "notifications_set_modified_at_trigger" BEFORE UPDATE ON "public"."notifications" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "process_embedding_ft_on_extract_md_update" AFTER UPDATE OF "extracted_md" ON "public"."processes" FOR EACH ROW WHEN (("old"."extracted_md" IS DISTINCT FROM "new"."extracted_md")) EXECUTE FUNCTION "util"."queue_embeddings"('processes_embedding_ft_input', 'embedding_ft', 'embedding_ft');



CREATE OR REPLACE TRIGGER "process_extract_md_trigger_insert" AFTER INSERT ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_process_embedding_ft', '1000');



CREATE OR REPLACE TRIGGER "process_extract_md_trigger_update" AFTER UPDATE OF "json" ON "public"."processes" FOR EACH ROW WHEN (("new"."json" IS DISTINCT FROM "old"."json")) EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_process_embedding_ft', '1000');



CREATE OR REPLACE TRIGGER "processes_json_sync_trigger" BEFORE INSERT OR UPDATE OF "json_ordered" ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "public"."processes_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "processes_set_modified_at_trigger" BEFORE UPDATE OF "json", "json_ordered", "user_id", "state_code", "version", "team_id", "review_id", "rule_verification", "reviews", "embedding_flag", "model_id" ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "roles_set_modified_at_trigger" BEFORE UPDATE ON "public"."roles" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "sources_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."sources" FOR EACH ROW EXECUTE FUNCTION "public"."sources_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "sources_set_modified_at_trigger" BEFORE UPDATE ON "public"."sources" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "teams_set_modified_at_trigger" BEFORE UPDATE ON "public"."teams" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "unitgroups_json_sync_trigger" BEFORE INSERT OR UPDATE ON "public"."unitgroups" FOR EACH ROW EXECUTE FUNCTION "public"."unitgroups_sync_jsonb_version"();



CREATE OR REPLACE TRIGGER "unitgroups_set_modified_at_trigger" BEFORE UPDATE ON "public"."unitgroups" FOR EACH ROW EXECUTE FUNCTION "public"."update_modified_at"();



CREATE OR REPLACE TRIGGER "zz_contacts_extracted_text_sync_trigger" BEFORE INSERT OR UPDATE OF "json", "json_ordered" ON "public"."contacts" FOR EACH ROW EXECUTE FUNCTION "util"."set_dataset_extracted_text_from_json"();



CREATE OR REPLACE TRIGGER "zz_flowproperties_extracted_text_sync_trigger" BEFORE INSERT OR UPDATE OF "json", "json_ordered" ON "public"."flowproperties" FOR EACH ROW EXECUTE FUNCTION "util"."set_dataset_extracted_text_from_json"();



CREATE OR REPLACE TRIGGER "zz_flows_extracted_text_sync_trigger" BEFORE INSERT OR UPDATE OF "json", "json_ordered" ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "util"."set_dataset_extracted_text_from_json"();



CREATE OR REPLACE TRIGGER "zz_lifecyclemodels_extracted_text_sync_trigger" BEFORE INSERT OR UPDATE OF "json", "json_ordered" ON "public"."lifecyclemodels" FOR EACH ROW EXECUTE FUNCTION "util"."set_dataset_extracted_text_from_json"();



CREATE OR REPLACE TRIGGER "zz_processes_extracted_text_sync_trigger" BEFORE INSERT OR UPDATE OF "json", "json_ordered" ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "util"."set_dataset_extracted_text_from_json"();



CREATE OR REPLACE TRIGGER "zz_sources_extracted_text_sync_trigger" BEFORE INSERT OR UPDATE OF "json", "json_ordered" ON "public"."sources" FOR EACH ROW EXECUTE FUNCTION "util"."set_dataset_extracted_text_from_json"();



CREATE OR REPLACE TRIGGER "zz_unitgroups_extracted_text_sync_trigger" BEFORE INSERT OR UPDATE OF "json", "json_ordered" ON "public"."unitgroups" FOR EACH ROW EXECUTE FUNCTION "util"."set_dataset_extracted_text_from_json"();



ALTER TABLE ONLY "public"."comments"
    ADD CONSTRAINT "comments_review_id_fkey" FOREIGN KEY ("review_id") REFERENCES "public"."reviews"("id");



ALTER TABLE ONLY "public"."dataset_review_submit_gate_runs"
    ADD CONSTRAINT "dataset_review_submit_gate_runs_supersedes_gate_run_id_fkey" FOREIGN KEY ("supersedes_gate_run_id") REFERENCES "public"."dataset_review_submit_gate_runs"("id");



ALTER TABLE ONLY "public"."dataset_review_submit_gate_runs"
    ADD CONSTRAINT "dataset_review_submit_gate_runs_worker_job_id_fkey" FOREIGN KEY ("worker_job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."dataset_review_submit_requests"
    ADD CONSTRAINT "dataset_review_submit_requests_gate_run_id_fkey" FOREIGN KEY ("gate_run_id") REFERENCES "public"."dataset_review_submit_gate_runs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."dataset_review_submit_requests"
    ADD CONSTRAINT "dataset_review_submit_requests_gate_worker_job_id_fkey" FOREIGN KEY ("gate_worker_job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."dataset_review_submit_requests"
    ADD CONSTRAINT "dataset_review_submit_requests_submit_worker_job_id_fkey" FOREIGN KEY ("submit_worker_job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_active_snapshots"
    ADD CONSTRAINT "lca_active_snapshots_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE RESTRICT;



ALTER TABLE ONLY "public"."lca_factorization_registry"
    ADD CONSTRAINT "lca_factorization_registry_prepared_worker_job_id_fkey" FOREIGN KEY ("prepared_worker_job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_factorization_registry"
    ADD CONSTRAINT "lca_factorization_registry_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."lca_latest_all_unit_results"
    ADD CONSTRAINT "lca_latest_all_unit_results_result_fk" FOREIGN KEY ("result_id") REFERENCES "public"."lca_results"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."lca_latest_all_unit_results"
    ADD CONSTRAINT "lca_latest_all_unit_results_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."lca_latest_all_unit_results"
    ADD CONSTRAINT "lca_latest_all_unit_results_worker_job_id_fkey" FOREIGN KEY ("worker_job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_network_snapshots"
    ADD CONSTRAINT "lca_network_snapshots_lcia_fk" FOREIGN KEY ("lcia_method_id", "lcia_method_version") REFERENCES "public"."lciamethods"("id", "version") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_package_artifacts"
    ADD CONSTRAINT "lca_package_artifacts_worker_job_id_fkey" FOREIGN KEY ("worker_job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_package_export_items"
    ADD CONSTRAINT "lca_package_export_items_worker_job_id_fkey" FOREIGN KEY ("worker_job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_package_request_cache"
    ADD CONSTRAINT "lca_package_request_cache_export_artifact_fk" FOREIGN KEY ("export_artifact_id") REFERENCES "public"."lca_package_artifacts"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_package_request_cache"
    ADD CONSTRAINT "lca_package_request_cache_report_artifact_fk" FOREIGN KEY ("report_artifact_id") REFERENCES "public"."lca_package_artifacts"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_package_request_cache"
    ADD CONSTRAINT "lca_package_request_cache_worker_job_id_fkey" FOREIGN KEY ("worker_job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_result_cache"
    ADD CONSTRAINT "lca_result_cache_result_fk" FOREIGN KEY ("result_id") REFERENCES "public"."lca_results"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_result_cache"
    ADD CONSTRAINT "lca_result_cache_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."lca_result_cache"
    ADD CONSTRAINT "lca_result_cache_worker_job_id_fkey" FOREIGN KEY ("worker_job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_results"
    ADD CONSTRAINT "lca_results_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."lca_results"
    ADD CONSTRAINT "lca_results_worker_job_id_fkey" FOREIGN KEY ("worker_job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE SET NULL;



ALTER TABLE ONLY "public"."lca_snapshot_artifacts"
    ADD CONSTRAINT "lca_snapshot_artifacts_snapshot_fk" FOREIGN KEY ("snapshot_id") REFERENCES "public"."lca_network_snapshots"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."lca_snapshot_gc_run_items"
    ADD CONSTRAINT "lca_snapshot_gc_run_items_run_id_fkey" FOREIGN KEY ("run_id") REFERENCES "public"."lca_snapshot_gc_runs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."notifications"
    ADD CONSTRAINT "notifications_recipient_user_id_fkey" FOREIGN KEY ("recipient_user_id") REFERENCES "auth"."users"("id");



ALTER TABLE ONLY "public"."notifications"
    ADD CONSTRAINT "notifications_sender_user_id_fkey" FOREIGN KEY ("sender_user_id") REFERENCES "auth"."users"("id");



ALTER TABLE ONLY "public"."roles"
    ADD CONSTRAINT "roles_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id");



ALTER TABLE ONLY "public"."worker_job_artifacts"
    ADD CONSTRAINT "worker_job_artifacts_job_id_fkey" FOREIGN KEY ("job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."worker_job_events"
    ADD CONSTRAINT "worker_job_events_job_id_fkey" FOREIGN KEY ("job_id") REFERENCES "public"."worker_jobs"("id") ON DELETE CASCADE;



ALTER TABLE ONLY "public"."worker_jobs"
    ADD CONSTRAINT "worker_jobs_job_kind_fkey" FOREIGN KEY ("job_kind") REFERENCES "public"."worker_job_kinds"("job_kind");



ALTER TABLE ONLY "public"."worker_jobs"
    ADD CONSTRAINT "worker_jobs_parent_job_id_fkey" FOREIGN KEY ("parent_job_id") REFERENCES "public"."worker_jobs"("id");



ALTER TABLE ONLY "public"."worker_jobs"
    ADD CONSTRAINT "worker_jobs_root_job_id_fkey" FOREIGN KEY ("root_job_id") REFERENCES "public"."worker_jobs"("id");



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


CREATE POLICY "comments select by review participants" ON "public"."comments" FOR SELECT TO "authenticated" USING (((( SELECT "auth"."uid"() AS "uid") IS NOT NULL) AND "public"."policy_review_can_read"("review_id", ( SELECT "auth"."uid"() AS "uid")) AND ("public"."cmd_review_is_review_admin"(( SELECT "auth"."uid"() AS "uid")) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."id" = "comments"."review_id") AND (((("r"."json" -> 'user'::"text") ->> 'id'::"text"))::"uuid" = ( SELECT "auth"."uid"() AS "uid"))))) OR ("reviewer_id" = ( SELECT "auth"."uid"() AS "uid")))));



CREATE POLICY "comments update by reviewer or review-admin" ON "public"."comments" FOR UPDATE TO "authenticated" USING (("public"."policy_is_current_user_in_roles"('00000000-0000-0000-0000-000000000000'::"uuid", ARRAY['review-admin'::"text"]) OR ("reviewer_id" = ( SELECT "auth"."uid"() AS "uid")))) WITH CHECK (("public"."policy_is_current_user_in_roles"('00000000-0000-0000-0000-000000000000'::"uuid", ARRAY['review-admin'::"text"]) OR ("reviewer_id" = ( SELECT "auth"."uid"() AS "uid"))));



ALTER TABLE "public"."contacts" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."dataset_review_submit_gate_runs" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."dataset_review_submit_requests" ENABLE ROW LEVEL SECURITY;


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



ALTER TABLE "public"."lca_latest_all_unit_results" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_latest_all_unit_results_service_role_all" ON "public"."lca_latest_all_unit_results" TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."lca_network_snapshots" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_network_snapshots_service_role_all" ON "public"."lca_network_snapshots" TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."lca_package_artifacts" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_package_artifacts_select_own" ON "public"."lca_package_artifacts" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "public"."worker_jobs" "worker_job"
  WHERE (("worker_job"."id" = "lca_package_artifacts"."worker_job_id") AND ("worker_job"."requested_by" = ( SELECT "auth"."uid"() AS "uid"))))));



ALTER TABLE "public"."lca_package_export_items" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."lca_package_request_cache" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_package_request_cache_select_own" ON "public"."lca_package_request_cache" FOR SELECT TO "authenticated" USING (("requested_by" = ( SELECT "auth"."uid"() AS "uid")));



ALTER TABLE "public"."lca_result_cache" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_result_cache_service_role_all" ON "public"."lca_result_cache" TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."lca_results" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_results_select_own" ON "public"."lca_results" FOR SELECT TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "public"."worker_jobs" "worker_job"
  WHERE (("worker_job"."id" = "lca_results"."worker_job_id") AND ("worker_job"."requested_by" = ( SELECT "auth"."uid"() AS "uid"))))));



ALTER TABLE "public"."lca_snapshot_artifacts" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_snapshot_artifacts_service_role_all" ON "public"."lca_snapshot_artifacts" TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."lca_snapshot_gc_run_items" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_snapshot_gc_run_items_service_role_all" ON "public"."lca_snapshot_gc_run_items" TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."lca_snapshot_gc_runs" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "lca_snapshot_gc_runs_service_role_all" ON "public"."lca_snapshot_gc_runs" TO "service_role" USING (true) WITH CHECK (true);



ALTER TABLE "public"."lciamethods" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."lifecyclemodels" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."notifications" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "notifications_delete_recipient_only" ON "public"."notifications" FOR DELETE TO "authenticated" USING ((( SELECT "auth"."uid"() AS "uid") = "recipient_user_id"));



CREATE POLICY "notifications_insert_sender" ON "public"."notifications" FOR INSERT TO "authenticated" WITH CHECK ((( SELECT "auth"."uid"() AS "uid") = "sender_user_id"));



CREATE POLICY "notifications_select_sender_or_recipient" ON "public"."notifications" FOR SELECT TO "authenticated" USING (((( SELECT "auth"."uid"() AS "uid") = "sender_user_id") OR (( SELECT "auth"."uid"() AS "uid") = "recipient_user_id")));



CREATE POLICY "notifications_update_sender" ON "public"."notifications" FOR UPDATE TO "authenticated" USING ((( SELECT "auth"."uid"() AS "uid") = "sender_user_id")) WITH CHECK ((( SELECT "auth"."uid"() AS "uid") = "sender_user_id"));



ALTER TABLE "public"."processes" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."reviews" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "reviews select by review participants" ON "public"."reviews" FOR SELECT TO "authenticated" USING (((( SELECT "auth"."uid"() AS "uid") IS NOT NULL) AND "public"."policy_review_can_read"("id", ( SELECT "auth"."uid"() AS "uid"))));



ALTER TABLE "public"."roles" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "select by owner or public teams" ON "public"."teams" FOR SELECT TO "authenticated" USING (("is_public" OR ("rank" > 0) OR (EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE ((("roles"."team_id" = "teams"."id") OR ("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid")) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid")) AND (("roles"."role")::"text" <> 'rejected'::"text"))))));



CREATE POLICY "select by self and team" ON "public"."roles" FOR SELECT TO "authenticated" USING ("public"."policy_roles_select"("team_id", ("role")::"text"));



CREATE POLICY "select by self and team and admin" ON "public"."users" FOR SELECT TO "authenticated" USING ((("id" = ( SELECT "auth"."uid"() AS "uid")) OR ("id" IN ( SELECT "r"."user_id"
   FROM "public"."roles" "r"
  WHERE ((("r"."role")::"text" = 'owner'::"text") AND ("public"."policy_is_team_public"("r"."team_id") = true)))) OR (EXISTS ( SELECT 1
   FROM "public"."roles" "r"
  WHERE ((("r"."role")::"text" = 'owner'::"text") AND ("r"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR ("id" IN ( SELECT "r0"."user_id"
   FROM "public"."roles" "r0"
  WHERE ("r0"."team_id" IN ( SELECT "r"."team_id"
           FROM "public"."roles" "r"
          WHERE (("r"."user_id" = ( SELECT "auth"."uid"() AS "uid")) AND (("r"."role")::"text" <> 'rejected'::"text")))))) OR "public"."policy_is_current_user_in_roles"('00000000-0000-0000-0000-000000000000'::"uuid", ARRAY['admin'::"text", 'review-admin'::"text", 'review-member'::"text"])));



ALTER TABLE "public"."sources" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."teams" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "transitional_reviews_update_submitter_only" ON "public"."reviews" FOR UPDATE TO "authenticated" USING (((( SELECT "auth"."uid"() AS "uid") IS NOT NULL) AND (((("json" -> 'user'::"text") ->> 'id'::"text"))::"uuid" = ( SELECT "auth"."uid"() AS "uid")))) WITH CHECK (((( SELECT "auth"."uid"() AS "uid") IS NOT NULL) AND (((("json" -> 'user'::"text") ->> 'id'::"text"))::"uuid" = ( SELECT "auth"."uid"() AS "uid"))));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."contacts" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid")))) WITH CHECK ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid"))));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."flowproperties" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid")))) WITH CHECK ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid"))));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."flows" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid")))) WITH CHECK ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid"))));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."lifecyclemodels" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid")))) WITH CHECK ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid"))));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."processes" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid")))) WITH CHECK ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid"))));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."sources" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid")))) WITH CHECK ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid"))));



CREATE POLICY "transitional_update_owner_draft_only" ON "public"."unitgroups" FOR UPDATE TO "authenticated" USING ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid")))) WITH CHECK ((("state_code" = 0) AND ("user_id" = ( SELECT "auth"."uid"() AS "uid"))));



ALTER TABLE "public"."unitgroups" ENABLE ROW LEVEL SECURITY;


CREATE POLICY "update by admin or owner or self" ON "public"."roles" FOR UPDATE TO "authenticated" USING ("public"."policy_roles_update"("user_id", "team_id", ("role")::"text"));



CREATE POLICY "update by owner and admin" ON "public"."teams" FOR UPDATE TO "authenticated" USING ((EXISTS ( SELECT 1
   FROM "public"."roles" "r"
  WHERE (("r"."user_id" = ( SELECT "auth"."uid"() AS "uid")) AND ("r"."team_id" = "teams"."id") AND (("r"."role")::"text" = ANY (ARRAY['owner'::"text", 'admin'::"text"])))))) WITH CHECK ((EXISTS ( SELECT 1
   FROM "public"."roles" "r"
  WHERE (("r"."user_id" = ( SELECT "auth"."uid"() AS "uid")) AND ("r"."team_id" = "teams"."id") AND (("r"."role")::"text" = ANY (ARRAY['owner'::"text", 'admin'::"text"]))))));



ALTER TABLE "public"."users" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."worker_job_artifacts" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."worker_job_events" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."worker_job_kinds" ENABLE ROW LEVEL SECURITY;


ALTER TABLE "public"."worker_jobs" ENABLE ROW LEVEL SECURITY;


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



GRANT ALL ON FUNCTION "public"."_search_simple_dataset_latest"("p_table" "regclass", "query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."_search_simple_dataset_latest"("p_table" "regclass", "query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."_search_simple_dataset_latest"("p_table" "regclass", "query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_assert_review_submit_gate_passed"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_gate_run_id" "uuid", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_assert_review_submit_gate_passed"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_gate_run_id" "uuid", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text") TO "service_role";
GRANT ALL ON FUNCTION "public"."cmd_dataset_assert_review_submit_gate_passed"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_gate_run_id" "uuid", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text") TO "authenticated";



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



REVOKE ALL ON FUNCTION "public"."cmd_dataset_extracted_text_backfill"("p_table" "text", "p_batch_size" integer, "p_after_id" "uuid", "p_after_version" "text", "p_mode" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_extracted_text_backfill"("p_table" "text", "p_batch_size" integer, "p_after_id" "uuid", "p_after_version" "text", "p_mode" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_extraction_ack"("p_msg_ids" bigint[]) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_ack"("p_msg_ids" bigint[]) TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_ack"("p_msg_ids" bigint[]) TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_ack"("p_msg_ids" bigint[]) TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_extraction_claim"("p_qty" integer, "p_vt_seconds" integer, "p_max_read_count" integer) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_claim"("p_qty" integer, "p_vt_seconds" integer, "p_max_read_count" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_claim"("p_qty" integer, "p_vt_seconds" integer, "p_max_read_count" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_claim"("p_qty" integer, "p_vt_seconds" integer, "p_max_read_count" integer) TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_extraction_record_failure"("p_msg_id" bigint, "p_read_count" integer, "p_reason" "text", "p_message" "jsonb", "p_last_error" "text", "p_delete" boolean) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_record_failure"("p_msg_id" bigint, "p_read_count" integer, "p_reason" "text", "p_message" "jsonb", "p_last_error" "text", "p_delete" boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_record_failure"("p_msg_id" bigint, "p_read_count" integer, "p_reason" "text", "p_message" "jsonb", "p_last_error" "text", "p_delete" boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_dataset_extraction_record_failure"("p_msg_id" bigint, "p_read_count" integer, "p_reason" "text", "p_message" "jsonb", "p_last_error" "text", "p_delete" boolean) TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_publish"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_publish"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_dataset_publish"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_dataset_publish"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_action" "text", "p_gate_run_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_action" "text", "p_gate_run_id" "uuid", "p_audit" "jsonb") TO "service_role";
GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_action" "text", "p_gate_run_id" "uuid", "p_audit" "jsonb") TO "authenticated";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate_enqueue_worker_job"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_requested_by" "uuid", "p_gate_run_id" "uuid", "p_action" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate_enqueue_worker_job"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_requested_by" "uuid", "p_gate_run_id" "uuid", "p_action" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate_link_worker_job"("p_gate_run_id" "uuid", "p_action" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate_link_worker_job"("p_gate_run_id" "uuid", "p_action" "text") TO "service_role";



GRANT ALL ON TABLE "public"."dataset_review_submit_gate_runs" TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate_payload"("p_run" "public"."dataset_review_submit_gate_runs", "p_status_override" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate_payload"("p_run" "public"."dataset_review_submit_gate_runs", "p_status_override" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate_record_result"("p_gate_run_id" "uuid", "p_status" "text", "p_calculator_report" "jsonb", "p_blocking_reasons" "jsonb", "p_report_schema_version" "text", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate_record_result"("p_gate_run_id" "uuid", "p_status" "text", "p_calculator_report" "jsonb", "p_blocking_reasons" "jsonb", "p_report_schema_version" "text", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_claim"("p_qty" integer, "p_stale_submitting_seconds" integer) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_claim"("p_qty" integer, "p_stale_submitting_seconds" integer) TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_enqueue"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_enqueue"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_audit" "jsonb") TO "service_role";
GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_enqueue"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_audit" "jsonb") TO "authenticated";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_payload"("p_job" "anyelement") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_payload"("p_job" "anyelement") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_read"("p_job_id" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_read"("p_job_id" "uuid") TO "service_role";
GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_read"("p_job_id" "uuid") TO "authenticated";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_read_latest"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_read_latest"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text") TO "service_role";
GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_read_latest"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text") TO "authenticated";



REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_record_result"("p_job_id" "uuid", "p_status" "text", "p_gate_run_id" "uuid", "p_result" "jsonb", "p_error_code" "text", "p_error_message" "text", "p_error_details" "jsonb", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_record_result"("p_job_id" "uuid", "p_status" "text", "p_gate_run_id" "uuid", "p_result" "jsonb", "p_error_code" "text", "p_error_message" "text", "p_error_details" "jsonb", "p_audit" "jsonb") TO "service_role";



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



REVOKE ALL ON FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb", "p_review_submit_gate_run_id" "uuid", "p_review_submit_revision_checksum" "text", "p_review_submit_policy_profile" "text", "p_review_submit_report_schema_version" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb", "p_review_submit_gate_run_id" "uuid", "p_review_submit_revision_checksum" "text", "p_review_submit_policy_profile" "text", "p_review_submit_report_schema_version" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb", "p_review_submit_gate_run_id" "uuid", "p_review_submit_revision_checksum" "text", "p_review_submit_policy_profile" "text", "p_review_submit_report_schema_version" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_submit"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb", "p_review_submit_gate_run_id" "uuid", "p_review_submit_revision_checksum" "text", "p_review_submit_policy_profile" "text", "p_review_submit_report_schema_version" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."cmd_review_submit_comment"("p_review_id" "uuid", "p_json" "jsonb", "p_comment_state" integer, "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."cmd_review_submit_without_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."cmd_review_submit_without_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_audit" "jsonb") TO "anon";



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



REVOKE ALL ON FUNCTION "public"."dataset_review_submit_requests_assign_submit_worker_job"() FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."dataset_review_submit_requests_assign_submit_worker_job"() TO "service_role";



REVOKE ALL ON FUNCTION "public"."dataset_review_submit_requests_sync_submit_worker_job"() FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."dataset_review_submit_requests_sync_submit_worker_job"() TO "service_role";



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



GRANT ALL ON FUNCTION "public"."get_latest_contact_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."get_latest_contact_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_latest_contact_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_latest_flow_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "filter_condition" "jsonb", "sort_by" "text", "sort_direction" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."get_latest_flow_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "filter_condition" "jsonb", "sort_by" "text", "sort_direction" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_latest_flow_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "filter_condition" "jsonb", "sort_by" "text", "sort_direction" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_latest_flowproperty_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."get_latest_flowproperty_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_latest_flowproperty_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_latest_lifecyclemodel_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."get_latest_lifecyclemodel_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_latest_lifecyclemodel_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_latest_process_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "sort_by" "text", "sort_direction" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."get_latest_process_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "sort_by" "text", "sort_direction" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_latest_process_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "sort_by" "text", "sort_direction" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_latest_source_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."get_latest_source_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_latest_source_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."get_latest_unitgroup_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."get_latest_unitgroup_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_latest_unitgroup_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "service_role";



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
GRANT ALL ON FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."lca_legacy_job_type"("p_job_kind" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."lca_legacy_job_type"("p_job_kind" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."lca_package_enqueue_job"("p_message" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."lca_package_enqueue_job"("p_message" "jsonb") TO "service_role";



REVOKE ALL ON FUNCTION "public"."lca_read_job_projection"("p_requested_by" "uuid", "p_worker_job_id" "uuid", "p_legacy_job_id" "uuid", "p_include_internal" boolean) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."lca_read_job_projection"("p_requested_by" "uuid", "p_worker_job_id" "uuid", "p_legacy_job_id" "uuid", "p_include_internal" boolean) TO "service_role";



REVOKE ALL ON FUNCTION "public"."lca_read_latest_single_solve_result"("p_requested_by" "uuid", "p_snapshot_id" "uuid", "p_process_index" integer) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."lca_read_latest_single_solve_result"("p_requested_by" "uuid", "p_snapshot_id" "uuid", "p_process_index" integer) TO "service_role";



REVOKE ALL ON FUNCTION "public"."lca_read_result_projection"("p_requested_by" "uuid", "p_result_id" "uuid", "p_required_artifact_format" "text", "p_include_internal" boolean) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."lca_read_result_projection"("p_requested_by" "uuid", "p_result_id" "uuid", "p_required_artifact_format" "text", "p_include_internal" boolean) TO "service_role";



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



GRANT ALL ON FUNCTION "public"."pgroonga_search_contacts_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_contacts_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_contacts_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_flowproperties"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flowproperties"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flowproperties"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_flowproperties_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flowproperties_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flowproperties_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_text_v1"("query_text" "text", "page_size" integer, "page_current" integer, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_sources"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_sources"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_sources"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_sources_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_sources_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_sources_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_unitgroups"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_unitgroups"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_unitgroups"("query_text" "text", "filter_condition" "text", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."pgroonga_search_unitgroups_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_unitgroups_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_unitgroups_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "service_role";



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



REVOKE ALL ON FUNCTION "public"."qry_team_find_invitable_user_by_email"("p_team_id" "uuid", "p_email" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."qry_team_find_invitable_user_by_email"("p_team_id" "uuid", "p_email" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."qry_team_find_invitable_user_by_email"("p_team_id" "uuid", "p_email" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."qry_team_find_invitable_user_by_email"("p_team_id" "uuid", "p_email" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."qry_team_get_member_list"("p_team_id" "uuid", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."qry_team_get_member_list"("p_team_id" "uuid", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."qry_team_get_member_list"("p_team_id" "uuid", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."qry_team_get_member_list"("p_team_id" "uuid", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."save_lifecycle_model_bundle"("p_plan" "jsonb") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."save_lifecycle_model_bundle"("p_plan" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."save_lifecycle_model_bundle"("p_plan" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."save_lifecycle_model_bundle"("p_plan" "jsonb") TO "service_role";



GRANT ALL ON FUNCTION "public"."search_contacts_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."search_contacts_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."search_contacts_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."search_dataset_json_uuid_mentions"("p_uuid" "uuid", "p_source_entity_kinds" "text"[], "p_data_source" "text", "p_this_user_id" "text", "p_team_id_filter" "uuid", "p_state_code_filter" integer, "p_limit" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."search_dataset_json_uuid_mentions"("p_uuid" "uuid", "p_source_entity_kinds" "text"[], "p_data_source" "text", "p_this_user_id" "text", "p_team_id_filter" "uuid", "p_state_code_filter" integer, "p_limit" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."search_dataset_json_uuid_mentions"("p_uuid" "uuid", "p_source_entity_kinds" "text"[], "p_data_source" "text", "p_this_user_id" "text", "p_team_id_filter" "uuid", "p_state_code_filter" integer, "p_limit" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."search_flowproperties_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."search_flowproperties_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."search_flowproperties_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."search_flows_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."search_flows_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."search_flows_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."search_lifecyclemodels_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."search_lifecyclemodels_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."search_lifecyclemodels_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."search_processes_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."search_processes_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."search_processes_latest"("query_text" "text", "filter_condition" "jsonb", "order_by" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."search_sources_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."search_sources_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."search_sources_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."search_unitgroups_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."search_unitgroups_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."search_unitgroups_latest"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."semantic_search"("query_embedding" "text", "match_threshold" double precision, "match_count" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."semantic_search"("query_embedding" "text", "match_threshold" double precision, "match_count" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."semantic_search"("query_embedding" "text", "match_threshold" double precision, "match_count" integer) TO "service_role";



GRANT ALL ON FUNCTION "public"."semantic_search_flows"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."semantic_search_flows"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."semantic_search_flows"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."semantic_search_flows_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."semantic_search_flows_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."semantic_search_flows_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."semantic_search_lifecyclemodels"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."semantic_search_lifecyclemodels"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."semantic_search_lifecyclemodels"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."semantic_search_lifecyclemodels_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."semantic_search_lifecyclemodels_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."semantic_search_lifecyclemodels_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "service_role";



GRANT ALL ON FUNCTION "public"."semantic_search_processes"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "anon";
GRANT ALL ON FUNCTION "public"."semantic_search_processes"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "authenticated";
GRANT ALL ON FUNCTION "public"."semantic_search_processes"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "service_role";



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



REVOKE ALL ON FUNCTION "public"."worker_cancel_job"("p_job_id" "uuid", "p_cancelled_by" "uuid", "p_reason" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."worker_cancel_job"("p_job_id" "uuid", "p_cancelled_by" "uuid", "p_reason" "text") TO "service_role";



REVOKE ALL ON FUNCTION "public"."worker_claim_jobs"("p_worker_queue" "text", "p_worker_id" "text", "p_limit" integer, "p_lease_seconds" integer) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."worker_claim_jobs"("p_worker_queue" "text", "p_worker_id" "text", "p_limit" integer, "p_lease_seconds" integer) TO "service_role";



REVOKE ALL ON FUNCTION "public"."worker_enqueue_job"("p_job_kind" "text", "p_payload_json" "jsonb", "p_payload_schema_version" "text", "p_subject_type" "text", "p_subject_id" "uuid", "p_subject_version" "text", "p_requested_by" "uuid", "p_requester_type" "text", "p_team_id" "uuid", "p_idempotency_key" "text", "p_request_hash" "text", "p_concurrency_key" "text", "p_priority" integer, "p_queue_key" "text", "p_run_after" timestamp with time zone, "p_visibility" "text", "p_max_attempts" integer, "p_timeout_at" timestamp with time zone, "p_payload_ref" "jsonb", "p_parent_job_id" "uuid", "p_root_job_id" "uuid") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."worker_enqueue_job"("p_job_kind" "text", "p_payload_json" "jsonb", "p_payload_schema_version" "text", "p_subject_type" "text", "p_subject_id" "uuid", "p_subject_version" "text", "p_requested_by" "uuid", "p_requester_type" "text", "p_team_id" "uuid", "p_idempotency_key" "text", "p_request_hash" "text", "p_concurrency_key" "text", "p_priority" integer, "p_queue_key" "text", "p_run_after" timestamp with time zone, "p_visibility" "text", "p_max_attempts" integer, "p_timeout_at" timestamp with time zone, "p_payload_ref" "jsonb", "p_parent_job_id" "uuid", "p_root_job_id" "uuid") TO "service_role";



REVOKE ALL ON FUNCTION "public"."worker_heartbeat_job"("p_job_id" "uuid", "p_lease_token" "uuid", "p_phase" "text", "p_progress" numeric, "p_diagnostics" "jsonb", "p_lease_seconds" integer) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."worker_heartbeat_job"("p_job_id" "uuid", "p_lease_token" "uuid", "p_phase" "text", "p_progress" numeric, "p_diagnostics" "jsonb", "p_lease_seconds" integer) TO "service_role";



GRANT ALL ON TABLE "public"."worker_jobs" TO "service_role";



REVOKE ALL ON FUNCTION "public"."worker_job_payload"("p_job" "public"."worker_jobs", "p_include_internal" boolean) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."worker_job_payload"("p_job" "public"."worker_jobs", "p_include_internal" boolean) TO "service_role";



REVOKE ALL ON FUNCTION "public"."worker_list_jobs"("p_requested_by" "uuid", "p_subject_type" "text", "p_subject_id" "uuid", "p_statuses" "text"[], "p_visibility" "text", "p_limit" integer, "p_include_internal" boolean) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."worker_list_jobs"("p_requested_by" "uuid", "p_subject_type" "text", "p_subject_id" "uuid", "p_statuses" "text"[], "p_visibility" "text", "p_limit" integer, "p_include_internal" boolean) TO "service_role";



REVOKE ALL ON FUNCTION "public"."worker_read_job"("p_job_id" "uuid", "p_include_internal" boolean) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."worker_read_job"("p_job_id" "uuid", "p_include_internal" boolean) TO "service_role";



REVOKE ALL ON FUNCTION "public"."worker_read_latest_job"("p_requested_by" "uuid", "p_subject_type" "text", "p_subject_id" "uuid", "p_subject_version" "text", "p_job_kind" "text", "p_statuses" "text"[], "p_include_internal" boolean) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."worker_read_latest_job"("p_requested_by" "uuid", "p_subject_type" "text", "p_subject_id" "uuid", "p_subject_version" "text", "p_job_kind" "text", "p_statuses" "text"[], "p_include_internal" boolean) TO "service_role";



REVOKE ALL ON FUNCTION "public"."worker_record_job_result"("p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_result_json" "jsonb", "p_result_schema_version" "text", "p_result_ref" "jsonb", "p_diagnostics" "jsonb", "p_error_code" "text", "p_error_message" "text", "p_error_details" "jsonb", "p_blocker_codes" "text"[], "p_resolution_scope" "text", "p_retryable" boolean) FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."worker_record_job_result"("p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_result_json" "jsonb", "p_result_schema_version" "text", "p_result_ref" "jsonb", "p_diagnostics" "jsonb", "p_error_code" "text", "p_error_message" "text", "p_error_details" "jsonb", "p_blocker_codes" "text"[], "p_resolution_scope" "text", "p_retryable" boolean) TO "service_role";



REVOKE ALL ON FUNCTION "public"."worker_retry_job"("p_job_id" "uuid", "p_run_after" timestamp with time zone, "p_max_attempts" integer, "p_reason" "text") FROM PUBLIC;
GRANT ALL ON FUNCTION "public"."worker_retry_job"("p_job_id" "uuid", "p_run_after" timestamp with time zone, "p_max_attempts" integer, "p_reason" "text") TO "service_role";



GRANT ALL ON TABLE "public"."command_audit_log" TO "service_role";



GRANT ALL ON SEQUENCE "public"."command_audit_log_id_seq" TO "service_role";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."comments" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."comments" TO "authenticated";
GRANT ALL ON TABLE "public"."comments" TO "service_role";



GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."contacts" TO "anon";
GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."contacts" TO "authenticated";
GRANT ALL ON TABLE "public"."contacts" TO "service_role";



GRANT ALL ON TABLE "public"."dataset_review_submit_requests" TO "service_role";



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



GRANT ALL ON TABLE "public"."lca_snapshot_gc_run_items" TO "anon";
GRANT ALL ON TABLE "public"."lca_snapshot_gc_run_items" TO "authenticated";
GRANT ALL ON TABLE "public"."lca_snapshot_gc_run_items" TO "service_role";



GRANT ALL ON TABLE "public"."lca_snapshot_gc_runs" TO "anon";
GRANT ALL ON TABLE "public"."lca_snapshot_gc_runs" TO "authenticated";
GRANT ALL ON TABLE "public"."lca_snapshot_gc_runs" TO "service_role";



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



GRANT ALL ON TABLE "public"."worker_job_artifacts" TO "service_role";



GRANT ALL ON TABLE "public"."worker_job_domain_refs" TO "service_role";



GRANT ALL ON TABLE "public"."worker_job_events" TO "service_role";



GRANT ALL ON TABLE "public"."worker_job_kinds" TO "service_role";



GRANT ALL ON TABLE "public"."worker_legacy_lifecycle_audit" TO "service_role";



GRANT ALL ON TABLE "public"."worker_legacy_table_retirement_blockers" TO "service_role";



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







