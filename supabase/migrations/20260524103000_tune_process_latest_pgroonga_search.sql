CREATE INDEX IF NOT EXISTS "processes_public_json_pgroonga_idx"
ON "public"."processes" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2")
WHERE "state_code" = 100;

CREATE INDEX IF NOT EXISTS "processes_co_json_pgroonga_idx"
ON "public"."processes" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2")
WHERE "state_code" = 200;

CREATE OR REPLACE FUNCTION "public"."pgroonga_search_processes_latest"(
  "query_text" text,
  "filter_condition" jsonb DEFAULT '{}'::jsonb,
  "order_by" jsonb DEFAULT '{}'::jsonb,
  "page_size" bigint DEFAULT 10,
  "page_current" bigint DEFAULT 1,
  "data_source" text DEFAULT 'tg'::text,
  "this_user_id" text DEFAULT ''::text,
  "team_id_filter" uuid DEFAULT NULL::uuid,
  "state_code_filter" integer DEFAULT NULL::integer,
  "type_of_data_set_filter" text DEFAULT 'all'::text
) RETURNS TABLE(
  "rank" bigint,
  "id" uuid,
  "json" jsonb,
  "version" character(9),
  "modified_at" timestamp with time zone,
  "team_id" uuid,
  "model_id" uuid,
  "total_count" bigint
)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_this_user_id uuid;
  filter_condition_jsonb jsonb;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);

  IF data_source = 'tg' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT
          p.id,
          max(pgroonga_score(p.tableoid, p.ctid)) AS search_score
        FROM public.processes p
        WHERE p.state_code = 100
          AND (team_id_filter IS NULL OR p.team_id = team_id_filter)
          AND p.json @> filter_condition_jsonb
          AND p.json &@~ query_text
          AND (
            coalesce(type_of_data_set_filter, 'all') = 'all'
            OR p.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = type_of_data_set_filter
          )
        GROUP BY p.id
      ),
      latest_rows AS (
        SELECT
          matched_ids.id,
          latest_row.json,
          latest_row.version,
          latest_row.modified_at,
          latest_row.team_id,
          latest_row.model_id,
          matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT
            p2.json,
            p2.version,
            p2.modified_at,
            p2.team_id,
            p2.model_id
          FROM public.processes p2
          WHERE p2.state_code = 100
            AND p2.id = matched_ids.id
            AND (team_id_filter IS NULL OR p2.team_id = team_id_filter)
          ORDER BY p2.version DESC, p2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT
          rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank,
          counted_rows.*
        FROM counted_rows
      )
      SELECT
        ranked_rows.rank,
        ranked_rows.id,
        ranked_rows.json,
        ranked_rows.version,
        ranked_rows.modified_at,
        ranked_rows.team_id,
        ranked_rows.model_id,
        ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'co' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT
          p.id,
          max(pgroonga_score(p.tableoid, p.ctid)) AS search_score
        FROM public.processes p
        WHERE p.state_code = 200
          AND (team_id_filter IS NULL OR p.team_id = team_id_filter)
          AND p.json @> filter_condition_jsonb
          AND p.json &@~ query_text
          AND (
            coalesce(type_of_data_set_filter, 'all') = 'all'
            OR p.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = type_of_data_set_filter
          )
        GROUP BY p.id
      ),
      latest_rows AS (
        SELECT
          matched_ids.id,
          latest_row.json,
          latest_row.version,
          latest_row.modified_at,
          latest_row.team_id,
          latest_row.model_id,
          matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT
            p2.json,
            p2.version,
            p2.modified_at,
            p2.team_id,
            p2.model_id
          FROM public.processes p2
          WHERE p2.state_code = 200
            AND p2.id = matched_ids.id
            AND (team_id_filter IS NULL OR p2.team_id = team_id_filter)
          ORDER BY p2.version DESC, p2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT
          rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank,
          counted_rows.*
        FROM counted_rows
      )
      SELECT
        ranked_rows.rank,
        ranked_rows.id,
        ranked_rows.json,
        ranked_rows.version,
        ranked_rows.modified_at,
        ranked_rows.team_id,
        ranked_rows.model_id,
        ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'my' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT
          p.id,
          max(pgroonga_score(p.tableoid, p.ctid)) AS search_score
        FROM public.processes p
        WHERE normalized_this_user_id IS NOT NULL
          AND p.user_id = normalized_this_user_id
          AND (state_code_filter IS NULL OR p.state_code = state_code_filter)
          AND p.json @> filter_condition_jsonb
          AND p.json &@~ query_text
          AND (
            coalesce(type_of_data_set_filter, 'all') = 'all'
            OR p.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = type_of_data_set_filter
          )
        GROUP BY p.id
      ),
      latest_rows AS (
        SELECT
          matched_ids.id,
          latest_row.json,
          latest_row.version,
          latest_row.modified_at,
          latest_row.team_id,
          latest_row.model_id,
          matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT
            p2.json,
            p2.version,
            p2.modified_at,
            p2.team_id,
            p2.model_id
          FROM public.processes p2
          WHERE normalized_this_user_id IS NOT NULL
            AND p2.user_id = normalized_this_user_id
            AND p2.id = matched_ids.id
            AND (state_code_filter IS NULL OR p2.state_code = state_code_filter)
          ORDER BY p2.version DESC, p2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT
          rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank,
          counted_rows.*
        FROM counted_rows
      )
      SELECT
        ranked_rows.rank,
        ranked_rows.id,
        ranked_rows.json,
        ranked_rows.version,
        ranked_rows.modified_at,
        ranked_rows.team_id,
        ranked_rows.model_id,
        ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'te' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT
          p.id,
          max(pgroonga_score(p.tableoid, p.ctid)) AS search_score
        FROM public.processes p
        WHERE team_id_filter IS NOT NULL
          AND p.team_id = team_id_filter
          AND (state_code_filter IS NULL OR p.state_code = state_code_filter)
          AND p.json @> filter_condition_jsonb
          AND p.json &@~ query_text
          AND (
            coalesce(type_of_data_set_filter, 'all') = 'all'
            OR p.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = type_of_data_set_filter
          )
        GROUP BY p.id
      ),
      latest_rows AS (
        SELECT
          matched_ids.id,
          latest_row.json,
          latest_row.version,
          latest_row.modified_at,
          latest_row.team_id,
          latest_row.model_id,
          matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT
            p2.json,
            p2.version,
            p2.modified_at,
            p2.team_id,
            p2.model_id
          FROM public.processes p2
          WHERE team_id_filter IS NOT NULL
            AND p2.team_id = team_id_filter
            AND p2.id = matched_ids.id
            AND (state_code_filter IS NULL OR p2.state_code = state_code_filter)
          ORDER BY p2.version DESC, p2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT
          rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank,
          counted_rows.*
        FROM counted_rows
      )
      SELECT
        ranked_rows.rank,
        ranked_rows.id,
        ranked_rows.json,
        ranked_rows.version,
        ranked_rows.modified_at,
        ranked_rows.team_id,
        ranked_rows.model_id,
        ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;
END;
$$;

ALTER FUNCTION "public"."pgroonga_search_processes_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text) TO "service_role";
