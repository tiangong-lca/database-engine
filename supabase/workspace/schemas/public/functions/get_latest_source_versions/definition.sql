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

GRANT ALL ON FUNCTION "public"."get_latest_source_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."get_latest_source_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."get_latest_source_versions"("page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "sort_by" "text", "sort_direction" "text") TO "service_role";
