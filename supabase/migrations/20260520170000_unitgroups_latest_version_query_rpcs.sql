CREATE INDEX IF NOT EXISTS "unitgroups_state_code_id_version_modified_at_idx"
ON "public"."unitgroups" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);

CREATE INDEX IF NOT EXISTS "unitgroups_user_id_state_code_id_version_modified_at_idx"
ON "public"."unitgroups" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);

CREATE INDEX IF NOT EXISTS "unitgroups_team_id_state_code_id_version_modified_at_idx"
ON "public"."unitgroups" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);

CREATE OR REPLACE FUNCTION "public"."get_latest_unitgroup_versions"(
  "page_size" bigint DEFAULT 10,
  "page_current" bigint DEFAULT 1,
  "data_source" text DEFAULT 'tg'::text,
  "this_user_id" text DEFAULT ''::text,
  "team_id_filter" uuid DEFAULT NULL::uuid,
  "state_code_filter" integer DEFAULT NULL::integer,
  "sort_by" text DEFAULT 'modified_at'::text,
  "sort_direction" text DEFAULT 'desc'::text
) RETURNS TABLE(
  "id" uuid,
  "json" jsonb,
  "version" character(9),
  "modified_at" timestamp with time zone,
  "team_id" uuid,
  "version_count" bigint,
  "total_count" bigint
)
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
    version_counts AS (
      SELECT visible_rows.id, count(*)::bigint AS version_count
      FROM visible_rows
      GROUP BY visible_rows.id
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.created_at,
        visible_rows.modified_at,
        visible_rows.team_id,
        version_counts.version_count
      FROM visible_rows
      JOIN version_counts ON version_counts.id = visible_rows.id
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
      counted_rows.version_count,
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

ALTER FUNCTION "public"."get_latest_unitgroup_versions"(
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer,
  "sort_by" text,
  "sort_direction" text
) OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."get_latest_unitgroup_versions"(
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer,
  "sort_by" text,
  "sort_direction" text
) TO "anon";

GRANT ALL ON FUNCTION "public"."get_latest_unitgroup_versions"(
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer,
  "sort_by" text,
  "sort_direction" text
) TO "authenticated";

GRANT ALL ON FUNCTION "public"."get_latest_unitgroup_versions"(
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer,
  "sort_by" text,
  "sort_direction" text
) TO "service_role";

CREATE OR REPLACE FUNCTION "public"."pgroonga_search_unitgroups_latest"(
  "query_text" text,
  "filter_condition" jsonb DEFAULT '{}'::jsonb,
  "page_size" bigint DEFAULT 10,
  "page_current" bigint DEFAULT 1,
  "data_source" text DEFAULT 'tg'::text,
  "this_user_id" text DEFAULT ''::text,
  "team_id_filter" uuid DEFAULT NULL::uuid,
  "state_code_filter" integer DEFAULT NULL::integer
) RETURNS TABLE(
  "rank" bigint,
  "id" uuid,
  "json" jsonb,
  "version" character(9),
  "modified_at" timestamp with time zone,
  "team_id" uuid,
  "version_count" bigint,
  "total_count" bigint
)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);

  RETURN QUERY
    WITH visible_rows AS (
      SELECT f.*, f.tableoid AS source_tableoid, f.ctid AS source_ctid
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
    matched_ids AS (
      SELECT
        visible_rows.id,
        max(pgroonga_score(visible_rows.source_tableoid, visible_rows.source_ctid)) AS search_score
      FROM visible_rows
      WHERE visible_rows.json @> coalesce(filter_condition, '{}'::jsonb)
        AND visible_rows.json &@~ query_text
      GROUP BY visible_rows.id
    ),
    version_counts AS (
      SELECT visible_rows.id, count(*)::bigint AS version_count
      FROM visible_rows
      GROUP BY visible_rows.id
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.team_id,
        version_counts.version_count,
        matched_ids.search_score
      FROM visible_rows
      JOIN matched_ids ON matched_ids.id = visible_rows.id
      JOIN version_counts ON version_counts.id = visible_rows.id
      ORDER BY visible_rows.id, visible_rows.version DESC, visible_rows.modified_at DESC
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
      ranked_rows.version_count,
      ranked_rows.total_count
    FROM ranked_rows
    ORDER BY ranked_rows.rank, ranked_rows.id
    LIMIT normalized_page_size
    OFFSET (normalized_page_current - 1) * normalized_page_size;
END;
$$;

ALTER FUNCTION "public"."pgroonga_search_unitgroups_latest"(
  "query_text" text,
  "filter_condition" jsonb,
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer
) OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."pgroonga_search_unitgroups_latest"(
  "query_text" text,
  "filter_condition" jsonb,
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer
) TO "anon";

GRANT ALL ON FUNCTION "public"."pgroonga_search_unitgroups_latest"(
  "query_text" text,
  "filter_condition" jsonb,
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer
) TO "authenticated";

GRANT ALL ON FUNCTION "public"."pgroonga_search_unitgroups_latest"(
  "query_text" text,
  "filter_condition" jsonb,
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer
) TO "service_role";
