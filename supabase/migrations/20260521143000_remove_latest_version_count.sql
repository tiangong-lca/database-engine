-- Remove realtime version totals from latest-version list/search RPC contracts.
-- The frontend now opens all versions by UUID directly, so list/search calls do not need this aggregate.

DROP FUNCTION IF EXISTS "public"."get_latest_source_versions"(bigint, bigint, text, text, uuid, integer, text, text);
CREATE OR REPLACE FUNCTION "public"."get_latest_source_versions"(
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
      FROM public.sources f
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

ALTER FUNCTION "public"."get_latest_source_versions"(
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer,
  "sort_by" text,
  "sort_direction" text
) OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."get_latest_source_versions"(
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer,
  "sort_by" text,
  "sort_direction" text
) TO "anon";

GRANT ALL ON FUNCTION "public"."get_latest_source_versions"(
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer,
  "sort_by" text,
  "sort_direction" text
) TO "authenticated";

GRANT ALL ON FUNCTION "public"."get_latest_source_versions"(
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer,
  "sort_by" text,
  "sort_direction" text
) TO "service_role";

DROP FUNCTION IF EXISTS "public"."pgroonga_search_sources_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer);
CREATE OR REPLACE FUNCTION "public"."pgroonga_search_sources_latest"(
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
      FROM public.sources f
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
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.team_id,
        matched_ids.search_score
      FROM visible_rows
      JOIN matched_ids ON matched_ids.id = visible_rows.id
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
      ranked_rows.total_count
    FROM ranked_rows
    ORDER BY ranked_rows.rank, ranked_rows.id
    LIMIT normalized_page_size
    OFFSET (normalized_page_current - 1) * normalized_page_size;
END;
$$;

ALTER FUNCTION "public"."pgroonga_search_sources_latest"(
  "query_text" text,
  "filter_condition" jsonb,
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer
) OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."pgroonga_search_sources_latest"(
  "query_text" text,
  "filter_condition" jsonb,
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer
) TO "anon";

GRANT ALL ON FUNCTION "public"."pgroonga_search_sources_latest"(
  "query_text" text,
  "filter_condition" jsonb,
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer
) TO "authenticated";

GRANT ALL ON FUNCTION "public"."pgroonga_search_sources_latest"(
  "query_text" text,
  "filter_condition" jsonb,
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer
) TO "service_role";

DROP FUNCTION IF EXISTS "public"."get_latest_contact_versions"(bigint, bigint, text, text, uuid, integer, text, text);
CREATE OR REPLACE FUNCTION "public"."get_latest_contact_versions"(
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

ALTER FUNCTION "public"."get_latest_contact_versions"(
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer,
  "sort_by" text,
  "sort_direction" text
) OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."get_latest_contact_versions"(
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer,
  "sort_by" text,
  "sort_direction" text
) TO "anon";

GRANT ALL ON FUNCTION "public"."get_latest_contact_versions"(
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer,
  "sort_by" text,
  "sort_direction" text
) TO "authenticated";

GRANT ALL ON FUNCTION "public"."get_latest_contact_versions"(
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer,
  "sort_by" text,
  "sort_direction" text
) TO "service_role";

DROP FUNCTION IF EXISTS "public"."pgroonga_search_contacts_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer);
CREATE OR REPLACE FUNCTION "public"."pgroonga_search_contacts_latest"(
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
    matched_ids AS (
      SELECT
        visible_rows.id,
        max(pgroonga_score(visible_rows.source_tableoid, visible_rows.source_ctid)) AS search_score
      FROM visible_rows
      WHERE visible_rows.json @> coalesce(filter_condition, '{}'::jsonb)
        AND visible_rows.json &@~ query_text
      GROUP BY visible_rows.id
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.team_id,
        matched_ids.search_score
      FROM visible_rows
      JOIN matched_ids ON matched_ids.id = visible_rows.id
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
      ranked_rows.total_count
    FROM ranked_rows
    ORDER BY ranked_rows.rank, ranked_rows.id
    LIMIT normalized_page_size
    OFFSET (normalized_page_current - 1) * normalized_page_size;
END;
$$;

ALTER FUNCTION "public"."pgroonga_search_contacts_latest"(
  "query_text" text,
  "filter_condition" jsonb,
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer
) OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."pgroonga_search_contacts_latest"(
  "query_text" text,
  "filter_condition" jsonb,
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer
) TO "anon";

GRANT ALL ON FUNCTION "public"."pgroonga_search_contacts_latest"(
  "query_text" text,
  "filter_condition" jsonb,
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer
) TO "authenticated";

GRANT ALL ON FUNCTION "public"."pgroonga_search_contacts_latest"(
  "query_text" text,
  "filter_condition" jsonb,
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer
) TO "service_role";

DROP FUNCTION IF EXISTS "public"."get_latest_unitgroup_versions"(bigint, bigint, text, text, uuid, integer, text, text);
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

DROP FUNCTION IF EXISTS "public"."pgroonga_search_unitgroups_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer);
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
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.team_id,
        matched_ids.search_score
      FROM visible_rows
      JOIN matched_ids ON matched_ids.id = visible_rows.id
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

DROP FUNCTION IF EXISTS "public"."get_latest_flowproperty_versions"(bigint, bigint, text, text, uuid, integer, text, text);
CREATE OR REPLACE FUNCTION "public"."get_latest_flowproperty_versions"(
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

ALTER FUNCTION "public"."get_latest_flowproperty_versions"(
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer,
  "sort_by" text,
  "sort_direction" text
) OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."get_latest_flowproperty_versions"(
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer,
  "sort_by" text,
  "sort_direction" text
) TO "anon";

GRANT ALL ON FUNCTION "public"."get_latest_flowproperty_versions"(
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer,
  "sort_by" text,
  "sort_direction" text
) TO "authenticated";

GRANT ALL ON FUNCTION "public"."get_latest_flowproperty_versions"(
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer,
  "sort_by" text,
  "sort_direction" text
) TO "service_role";

DROP FUNCTION IF EXISTS "public"."pgroonga_search_flowproperties_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer);
CREATE OR REPLACE FUNCTION "public"."pgroonga_search_flowproperties_latest"(
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
    matched_ids AS (
      SELECT
        visible_rows.id,
        max(pgroonga_score(visible_rows.source_tableoid, visible_rows.source_ctid)) AS search_score
      FROM visible_rows
      WHERE visible_rows.json @> coalesce(filter_condition, '{}'::jsonb)
        AND visible_rows.json &@~ query_text
      GROUP BY visible_rows.id
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.team_id,
        matched_ids.search_score
      FROM visible_rows
      JOIN matched_ids ON matched_ids.id = visible_rows.id
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
      ranked_rows.total_count
    FROM ranked_rows
    ORDER BY ranked_rows.rank, ranked_rows.id
    LIMIT normalized_page_size
    OFFSET (normalized_page_current - 1) * normalized_page_size;
END;
$$;

ALTER FUNCTION "public"."pgroonga_search_flowproperties_latest"(
  "query_text" text,
  "filter_condition" jsonb,
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer
) OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."pgroonga_search_flowproperties_latest"(
  "query_text" text,
  "filter_condition" jsonb,
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer
) TO "anon";

GRANT ALL ON FUNCTION "public"."pgroonga_search_flowproperties_latest"(
  "query_text" text,
  "filter_condition" jsonb,
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer
) TO "authenticated";

GRANT ALL ON FUNCTION "public"."pgroonga_search_flowproperties_latest"(
  "query_text" text,
  "filter_condition" jsonb,
  "page_size" bigint,
  "page_current" bigint,
  "data_source" text,
  "this_user_id" text,
  "team_id_filter" uuid,
  "state_code_filter" integer
) TO "service_role";

DROP FUNCTION IF EXISTS "public"."get_latest_flow_versions"(bigint, bigint, text, text, uuid, integer, jsonb, text, text);
CREATE OR REPLACE FUNCTION "public"."get_latest_flow_versions"(
  "page_size" bigint DEFAULT 10,
  "page_current" bigint DEFAULT 1,
  "data_source" text DEFAULT 'tg'::text,
  "this_user_id" text DEFAULT ''::text,
  "team_id_filter" uuid DEFAULT NULL::uuid,
  "state_code_filter" integer DEFAULT NULL::integer,
  "filter_condition" jsonb DEFAULT '{}'::jsonb,
  "sort_by" text DEFAULT 'modified_at'::text,
  "sort_direction" text DEFAULT 'desc'::text
) RETURNS TABLE(
  "id" uuid,
  "json" jsonb,
  "version" character(9),
  "modified_at" timestamp with time zone,
  "team_id" uuid,
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
$$;

ALTER FUNCTION "public"."get_latest_flow_versions"(bigint, bigint, text, text, uuid, integer, jsonb, text, text) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."get_latest_flow_versions"(bigint, bigint, text, text, uuid, integer, jsonb, text, text) TO "anon";
GRANT ALL ON FUNCTION "public"."get_latest_flow_versions"(bigint, bigint, text, text, uuid, integer, jsonb, text, text) TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_latest_flow_versions"(bigint, bigint, text, text, uuid, integer, jsonb, text, text) TO "service_role";

DROP FUNCTION IF EXISTS "public"."get_latest_process_versions"(bigint, bigint, text, text, uuid, integer, text, text, text);
CREATE OR REPLACE FUNCTION "public"."get_latest_process_versions"(
  "page_size" bigint DEFAULT 10,
  "page_current" bigint DEFAULT 1,
  "data_source" text DEFAULT 'tg'::text,
  "this_user_id" text DEFAULT ''::text,
  "team_id_filter" uuid DEFAULT NULL::uuid,
  "state_code_filter" integer DEFAULT NULL::integer,
  "type_of_data_set_filter" text DEFAULT 'all'::text,
  "sort_by" text DEFAULT 'modified_at'::text,
  "sort_direction" text DEFAULT 'desc'::text
) RETURNS TABLE(
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
    AS $$
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
$$;

ALTER FUNCTION "public"."get_latest_process_versions"(bigint, bigint, text, text, uuid, integer, text, text, text) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."get_latest_process_versions"(bigint, bigint, text, text, uuid, integer, text, text, text) TO "anon";
GRANT ALL ON FUNCTION "public"."get_latest_process_versions"(bigint, bigint, text, text, uuid, integer, text, text, text) TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_latest_process_versions"(bigint, bigint, text, text, uuid, integer, text, text, text) TO "service_role";

DROP FUNCTION IF EXISTS "public"."get_latest_lifecyclemodel_versions"(bigint, bigint, text, text, uuid, integer, text, text);
CREATE OR REPLACE FUNCTION "public"."get_latest_lifecyclemodel_versions"(
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
$$;

ALTER FUNCTION "public"."get_latest_lifecyclemodel_versions"(bigint, bigint, text, text, uuid, integer, text, text) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."get_latest_lifecyclemodel_versions"(bigint, bigint, text, text, uuid, integer, text, text) TO "anon";
GRANT ALL ON FUNCTION "public"."get_latest_lifecyclemodel_versions"(bigint, bigint, text, text, uuid, integer, text, text) TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_latest_lifecyclemodel_versions"(bigint, bigint, text, text, uuid, integer, text, text) TO "service_role";

DROP FUNCTION IF EXISTS "public"."pgroonga_search_flows_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer);
CREATE OR REPLACE FUNCTION "public"."pgroonga_search_flows_latest"(
  "query_text" text,
  "filter_condition" jsonb DEFAULT '{}'::jsonb,
  "order_by" jsonb DEFAULT '{}'::jsonb,
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
  "total_count" bigint
)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_this_user_id uuid;
  filter_condition_jsonb jsonb;
  flow_type text;
  flow_type_array text[];
  as_input boolean;
  classification_filter jsonb;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
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

  RETURN QUERY
    WITH visible_rows AS (
      SELECT f.*, f.tableoid AS source_tableoid, f.ctid AS source_ctid
      FROM public.flows f
      WHERE data_source = 'tg'
        AND f.state_code = 100
        AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
      UNION ALL
      SELECT f.*, f.tableoid AS source_tableoid, f.ctid AS source_ctid
      FROM public.flows f
      WHERE data_source = 'co'
        AND f.state_code = 200
        AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
      UNION ALL
      SELECT f.*, f.tableoid AS source_tableoid, f.ctid AS source_ctid
      FROM public.flows f
      WHERE data_source = 'my'
        AND normalized_this_user_id IS NOT NULL
        AND f.user_id = normalized_this_user_id
        AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
      UNION ALL
      SELECT f.*, f.tableoid AS source_tableoid, f.ctid AS source_ctid
      FROM public.flows f
      WHERE data_source = 'te'
        AND team_id_filter IS NOT NULL
        AND f.team_id = team_id_filter
        AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
    ),
    matched_ids AS (
      SELECT
        visible_rows.id,
        max(pgroonga_score(visible_rows.source_tableoid, visible_rows.source_ctid)) AS search_score
      FROM visible_rows
      WHERE visible_rows.json @> filter_condition_jsonb
        AND visible_rows.json &@~ query_text
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
      GROUP BY visible_rows.id
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.team_id,
        matched_ids.search_score
      FROM visible_rows
      JOIN matched_ids ON matched_ids.id = visible_rows.id
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
      ranked_rows.total_count
    FROM ranked_rows
    ORDER BY ranked_rows.rank, ranked_rows.id
    LIMIT normalized_page_size
    OFFSET (normalized_page_current - 1) * normalized_page_size;
END;
$$;

ALTER FUNCTION "public"."pgroonga_search_flows_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "service_role";

DROP FUNCTION IF EXISTS "public"."pgroonga_search_processes_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text);
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

  RETURN QUERY
    WITH visible_rows AS (
      SELECT p.*, p.tableoid AS source_tableoid, p.ctid AS source_ctid
      FROM public.processes p
      WHERE data_source = 'tg'
        AND p.state_code = 100
        AND (team_id_filter IS NULL OR p.team_id = team_id_filter)
      UNION ALL
      SELECT p.*, p.tableoid AS source_tableoid, p.ctid AS source_ctid
      FROM public.processes p
      WHERE data_source = 'co'
        AND p.state_code = 200
        AND (team_id_filter IS NULL OR p.team_id = team_id_filter)
      UNION ALL
      SELECT p.*, p.tableoid AS source_tableoid, p.ctid AS source_ctid
      FROM public.processes p
      WHERE data_source = 'my'
        AND normalized_this_user_id IS NOT NULL
        AND p.user_id = normalized_this_user_id
        AND (state_code_filter IS NULL OR p.state_code = state_code_filter)
      UNION ALL
      SELECT p.*, p.tableoid AS source_tableoid, p.ctid AS source_ctid
      FROM public.processes p
      WHERE data_source = 'te'
        AND team_id_filter IS NOT NULL
        AND p.team_id = team_id_filter
        AND (state_code_filter IS NULL OR p.state_code = state_code_filter)
    ),
    matched_ids AS (
      SELECT
        visible_rows.id,
        max(pgroonga_score(visible_rows.source_tableoid, visible_rows.source_ctid)) AS search_score
      FROM visible_rows
      WHERE visible_rows.json @> filter_condition_jsonb
        AND visible_rows.json &@~ query_text
        AND (
          coalesce(type_of_data_set_filter, 'all') = 'all'
          OR visible_rows.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = type_of_data_set_filter
        )
      GROUP BY visible_rows.id
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.team_id,
        visible_rows.model_id,
        matched_ids.search_score
      FROM visible_rows
      JOIN matched_ids ON matched_ids.id = visible_rows.id
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
      ranked_rows.model_id,
      ranked_rows.total_count
    FROM ranked_rows
    ORDER BY ranked_rows.rank, ranked_rows.id
    LIMIT normalized_page_size
    OFFSET (normalized_page_current - 1) * normalized_page_size;
END;
$$;

ALTER FUNCTION "public"."pgroonga_search_processes_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text) TO "service_role";

DROP FUNCTION IF EXISTS "public"."pgroonga_search_lifecyclemodels_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer);
CREATE OR REPLACE FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"(
  "query_text" text,
  "filter_condition" jsonb DEFAULT '{}'::jsonb,
  "order_by" jsonb DEFAULT '{}'::jsonb,
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
  "total_count" bigint
)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
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

  RETURN QUERY
    WITH visible_rows AS (
      SELECT l.*, l.tableoid AS source_tableoid, l.ctid AS source_ctid
      FROM public.lifecyclemodels l
      WHERE data_source = 'tg'
        AND l.state_code = 100
        AND (team_id_filter IS NULL OR l.team_id = team_id_filter)
      UNION ALL
      SELECT l.*, l.tableoid AS source_tableoid, l.ctid AS source_ctid
      FROM public.lifecyclemodels l
      WHERE data_source = 'co'
        AND l.state_code = 200
        AND (team_id_filter IS NULL OR l.team_id = team_id_filter)
      UNION ALL
      SELECT l.*, l.tableoid AS source_tableoid, l.ctid AS source_ctid
      FROM public.lifecyclemodels l
      WHERE data_source = 'my'
        AND normalized_this_user_id IS NOT NULL
        AND l.user_id = normalized_this_user_id
        AND (state_code_filter IS NULL OR l.state_code = state_code_filter)
      UNION ALL
      SELECT l.*, l.tableoid AS source_tableoid, l.ctid AS source_ctid
      FROM public.lifecyclemodels l
      WHERE data_source = 'te'
        AND team_id_filter IS NOT NULL
        AND l.team_id = team_id_filter
        AND (state_code_filter IS NULL OR l.state_code = state_code_filter)
    ),
    matched_ids AS (
      SELECT
        visible_rows.id,
        max(pgroonga_score(visible_rows.source_tableoid, visible_rows.source_ctid)) AS search_score
      FROM visible_rows
      WHERE visible_rows.json @> filter_condition_jsonb
        AND visible_rows.json &@~ query_text
      GROUP BY visible_rows.id
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.team_id,
        matched_ids.search_score
      FROM visible_rows
      JOIN matched_ids ON matched_ids.id = visible_rows.id
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
      ranked_rows.total_count
    FROM ranked_rows
    ORDER BY ranked_rows.rank, ranked_rows.id
    LIMIT normalized_page_size
    OFFSET (normalized_page_current - 1) * normalized_page_size;
END;
$$;

ALTER FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "service_role";

DROP FUNCTION IF EXISTS "public"."hybrid_search_flows"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer);
CREATE OR REPLACE FUNCTION "public"."hybrid_search_flows"(
  "query_text" text,
  "query_embedding" text,
  "filter_condition" text DEFAULT ''::text,
  "match_threshold" double precision DEFAULT 0.5,
  "match_count" integer DEFAULT 20,
  "full_text_weight" double precision DEFAULT 0.3,
  "extracted_text_weight" double precision DEFAULT 0.2,
  "semantic_weight" double precision DEFAULT 0.5,
  "rrf_k" integer DEFAULT 10,
  "data_source" text DEFAULT 'tg'::text,
  "page_size" integer DEFAULT 10,
  "page_current" integer DEFAULT 1
) RETURNS TABLE(
  "id" uuid,
  "json" jsonb,
  "version" character(9),
  "modified_at" timestamp with time zone,
  "team_id" uuid,
  "total_count" bigint
)
    LANGUAGE "plpgsql"
    SET "statement_timeout" TO '60s'
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
DECLARE
  candidate_limit integer;
  filter_condition_jsonb jsonb;
BEGIN
  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;

  RETURN QUERY
    WITH full_text AS (
      SELECT ps.rank AS ps_rank, ps.id AS ps_id
      FROM public.pgroonga_search_flows_v1(
        query_text,
        filter_condition,
        '',
        candidate_limit,
        1,
        data_source
      ) ps
    ),
    ex_text AS (
      SELECT ex.rank AS ex_rank, ex.id AS ex_id
      FROM public.pgroonga_search_flows_text_v1(
        query_text,
        candidate_limit,
        1,
        data_source
      ) ex
    ),
    semantic AS (
      SELECT ss.rank AS ss_rank, ss.id AS ss_id
      FROM public.semantic_search_flows_v1(
        query_embedding,
        filter_condition,
        match_threshold,
        candidate_limit,
        data_source
      ) ss
    ),
    fused_raw AS (
      SELECT
        coalesce(full_text.ps_id, semantic.ss_id, ex_text.ex_id) AS id,
        coalesce(1.0 / (rrf_k + full_text.ps_rank), 0.0) * full_text_weight
          + coalesce(1.0 / (rrf_k + ex_text.ex_rank), 0.0) * extracted_text_weight
          + coalesce(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight AS score
      FROM full_text
      FULL OUTER JOIN semantic ON full_text.ps_id = semantic.ss_id
      FULL OUTER JOIN ex_text ON ex_text.ex_id = coalesce(full_text.ps_id, semantic.ss_id)
    ),
    fused AS (
      SELECT fused_raw.id, sum(fused_raw.score) AS score
      FROM fused_raw
      WHERE fused_raw.id IS NOT NULL
      GROUP BY fused_raw.id
    ),
    visible_rows AS (
      SELECT f.*
      FROM public.flows f
      JOIN fused ON fused.id = f.id
      WHERE
        (
          (data_source = 'tg' AND f.state_code = 100)
          OR (data_source = 'co' AND f.state_code = 200)
          OR (data_source = 'my' AND f.user_id = auth.uid())
          OR (
            data_source = 'te'
            AND EXISTS (
              SELECT 1
              FROM public.roles r
              WHERE r.user_id = auth.uid()
                AND r.team_id = f.team_id
                AND r.role::text IN ('admin', 'member', 'owner')
            )
          )
        )
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.team_id,
        fused.score
      FROM visible_rows
      JOIN fused ON fused.id = visible_rows.id
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
    ORDER BY counted_rows.score DESC, counted_rows.modified_at DESC, counted_rows.id
    LIMIT greatest(coalesce(page_size, 10), 1)
    OFFSET (greatest(coalesce(page_current, 1), 1) - 1) * greatest(coalesce(page_size, 10), 1);
END;
$$;

ALTER FUNCTION "public"."hybrid_search_flows"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."hybrid_search_flows"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."hybrid_search_flows"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."hybrid_search_flows"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) TO "service_role";

DROP FUNCTION IF EXISTS "public"."hybrid_search_processes"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer);
CREATE OR REPLACE FUNCTION "public"."hybrid_search_processes"(
  "query_text" text,
  "query_embedding" text,
  "filter_condition" text DEFAULT ''::text,
  "match_threshold" double precision DEFAULT 0.5,
  "match_count" integer DEFAULT 20,
  "full_text_weight" double precision DEFAULT 0.3,
  "extracted_text_weight" double precision DEFAULT 0.2,
  "semantic_weight" double precision DEFAULT 0.5,
  "rrf_k" integer DEFAULT 10,
  "data_source" text DEFAULT 'tg'::text,
  "page_size" integer DEFAULT 10,
  "page_current" integer DEFAULT 1
) RETURNS TABLE(
  "id" uuid,
  "json" jsonb,
  "version" character(9),
  "modified_at" timestamp with time zone,
  "model_id" uuid,
  "team_id" uuid,
  "total_count" bigint
)
    LANGUAGE "plpgsql"
    SET "statement_timeout" TO '60s'
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
DECLARE
  candidate_limit integer;
BEGIN
  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;

  RETURN QUERY
    WITH full_text AS (
      SELECT ps.rank AS ps_rank, ps.id AS ps_id
      FROM public.pgroonga_search_processes_v1(
        query_text,
        filter_condition,
        '',
        candidate_limit,
        1,
        data_source
      ) ps
    ),
    ex_text AS (
      SELECT ex.rank AS ex_rank, ex.id AS ex_id
      FROM public.pgroonga_search_processes_text_v1(
        query_text,
        candidate_limit,
        1,
        data_source
      ) ex
    ),
    semantic AS (
      SELECT ss.rank AS ss_rank, ss.id AS ss_id
      FROM public.semantic_search_processes_v1(
        query_embedding,
        filter_condition,
        match_threshold,
        candidate_limit,
        data_source
      ) ss
    ),
    fused_raw AS (
      SELECT
        coalesce(full_text.ps_id, semantic.ss_id, ex_text.ex_id) AS id,
        coalesce(1.0 / (rrf_k + full_text.ps_rank), 0.0) * full_text_weight
          + coalesce(1.0 / (rrf_k + ex_text.ex_rank), 0.0) * extracted_text_weight
          + coalesce(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight AS score
      FROM full_text
      FULL OUTER JOIN semantic ON full_text.ps_id = semantic.ss_id
      FULL OUTER JOIN ex_text ON ex_text.ex_id = coalesce(full_text.ps_id, semantic.ss_id)
    ),
    fused AS (
      SELECT fused_raw.id, sum(fused_raw.score) AS score
      FROM fused_raw
      WHERE fused_raw.id IS NOT NULL
      GROUP BY fused_raw.id
    ),
    visible_rows AS (
      SELECT p.*
      FROM public.processes p
      JOIN fused ON fused.id = p.id
      WHERE
        (
          (data_source = 'tg' AND p.state_code = 100)
          OR (data_source = 'co' AND p.state_code = 200)
          OR (data_source = 'my' AND p.user_id = auth.uid())
          OR (
            data_source = 'te'
            AND EXISTS (
              SELECT 1
              FROM public.roles r
              WHERE r.user_id = auth.uid()
                AND r.team_id = p.team_id
                AND r.role::text IN ('admin', 'member', 'owner')
            )
          )
        )
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.model_id,
        visible_rows.team_id,
        fused.score
      FROM visible_rows
      JOIN fused ON fused.id = visible_rows.id
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
      counted_rows.model_id,
      counted_rows.team_id,
      counted_rows.total_count
    FROM counted_rows
    ORDER BY counted_rows.score DESC, counted_rows.modified_at DESC, counted_rows.id
    LIMIT greatest(coalesce(page_size, 10), 1)
    OFFSET (greatest(coalesce(page_current, 1), 1) - 1) * greatest(coalesce(page_size, 10), 1);
END;
$$;

ALTER FUNCTION "public"."hybrid_search_processes"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."hybrid_search_processes"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."hybrid_search_processes"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."hybrid_search_processes"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) TO "service_role";

DROP FUNCTION IF EXISTS "public"."hybrid_search_lifecyclemodels"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer);
CREATE OR REPLACE FUNCTION "public"."hybrid_search_lifecyclemodels"(
  "query_text" text,
  "query_embedding" text,
  "filter_condition" text DEFAULT ''::text,
  "match_threshold" double precision DEFAULT 0.5,
  "match_count" integer DEFAULT 20,
  "full_text_weight" double precision DEFAULT 0.3,
  "extracted_text_weight" double precision DEFAULT 0.2,
  "semantic_weight" double precision DEFAULT 0.5,
  "rrf_k" integer DEFAULT 10,
  "data_source" text DEFAULT 'tg'::text,
  "page_size" integer DEFAULT 10,
  "page_current" integer DEFAULT 1
) RETURNS TABLE(
  "id" uuid,
  "json" jsonb,
  "version" character(9),
  "modified_at" timestamp with time zone,
  "team_id" uuid,
  "total_count" bigint
)
    LANGUAGE "plpgsql"
    SET "statement_timeout" TO '60s'
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
DECLARE
  candidate_limit integer;
BEGIN
  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;

  RETURN QUERY
    WITH full_text AS (
      SELECT ps.rank AS ps_rank, ps.id AS ps_id
      FROM public.pgroonga_search_lifecyclemodels_v1(
        query_text,
        filter_condition,
        '',
        candidate_limit,
        1,
        data_source
      ) ps
    ),
    ex_text AS (
      SELECT ex.rank AS ex_rank, ex.id AS ex_id
      FROM public.pgroonga_search_lifecyclemodels_text_v1(
        query_text,
        candidate_limit,
        1,
        data_source
      ) ex
    ),
    semantic AS (
      SELECT ss.rank AS ss_rank, ss.id AS ss_id
      FROM public.semantic_search_lifecyclemodels_v1(
        query_embedding,
        filter_condition,
        match_threshold,
        candidate_limit,
        data_source
      ) ss
    ),
    fused_raw AS (
      SELECT
        coalesce(full_text.ps_id, semantic.ss_id, ex_text.ex_id) AS id,
        coalesce(1.0 / (rrf_k + full_text.ps_rank), 0.0) * full_text_weight
          + coalesce(1.0 / (rrf_k + ex_text.ex_rank), 0.0) * extracted_text_weight
          + coalesce(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight AS score
      FROM full_text
      FULL OUTER JOIN semantic ON full_text.ps_id = semantic.ss_id
      FULL OUTER JOIN ex_text ON ex_text.ex_id = coalesce(full_text.ps_id, semantic.ss_id)
    ),
    fused AS (
      SELECT fused_raw.id, sum(fused_raw.score) AS score
      FROM fused_raw
      WHERE fused_raw.id IS NOT NULL
      GROUP BY fused_raw.id
    ),
    visible_rows AS (
      SELECT l.*
      FROM public.lifecyclemodels l
      JOIN fused ON fused.id = l.id
      WHERE
        (
          (data_source = 'tg' AND l.state_code = 100)
          OR (data_source = 'co' AND l.state_code = 200)
          OR (data_source = 'my' AND l.user_id = auth.uid())
          OR (
            data_source = 'te'
            AND EXISTS (
              SELECT 1
              FROM public.roles r
              WHERE r.user_id = auth.uid()
                AND r.team_id = l.team_id
                AND r.role::text IN ('admin', 'member', 'owner')
            )
          )
        )
    ),
    latest_rows AS (
      SELECT DISTINCT ON (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.team_id,
        fused.score
      FROM visible_rows
      JOIN fused ON fused.id = visible_rows.id
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
    ORDER BY counted_rows.score DESC, counted_rows.modified_at DESC, counted_rows.id
    LIMIT greatest(coalesce(page_size, 10), 1)
    OFFSET (greatest(coalesce(page_current, 1), 1) - 1) * greatest(coalesce(page_size, 10), 1);
END;
$$;

ALTER FUNCTION "public"."hybrid_search_lifecyclemodels"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."hybrid_search_lifecyclemodels"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."hybrid_search_lifecyclemodels"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."hybrid_search_lifecyclemodels"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) TO "service_role";

ALTER FUNCTION "public"."get_latest_flow_versions"(bigint, bigint, text, text, uuid, integer, jsonb, text, text)
  SET "statement_timeout" TO '60s';

ALTER FUNCTION "public"."get_latest_process_versions"(bigint, bigint, text, text, uuid, integer, text, text, text)
  SET "statement_timeout" TO '60s';

ALTER FUNCTION "public"."get_latest_lifecyclemodel_versions"(bigint, bigint, text, text, uuid, integer, text, text)
  SET "statement_timeout" TO '60s';

ALTER FUNCTION "public"."pgroonga_search_flows_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer)
  SET "statement_timeout" TO '60s';

ALTER FUNCTION "public"."pgroonga_search_processes_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text)
  SET "statement_timeout" TO '60s';

ALTER FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer)
  SET "statement_timeout" TO '60s';

ALTER FUNCTION "public"."hybrid_search_flows"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer)
  SET "statement_timeout" TO '60s';

ALTER FUNCTION "public"."hybrid_search_processes"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer)
  SET "statement_timeout" TO '60s';

ALTER FUNCTION "public"."hybrid_search_lifecyclemodels"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer)
  SET "statement_timeout" TO '60s';
