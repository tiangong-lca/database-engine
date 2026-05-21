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
        page_version_counts.version_count,
        paged_keys.total_count
      FROM paged_keys
      JOIN public.flows payload
        ON payload.id = paged_keys.id
       AND payload.version = paged_keys.version
      JOIN LATERAL (
        SELECT count(*)::bigint AS version_count
        FROM visible_keys
        WHERE visible_keys.id = paged_keys.id
      ) page_version_counts ON true
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
      JOIN matched_ids ON matched_ids.id = visible_rows.id
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

ALTER FUNCTION "public"."get_latest_flow_versions"(bigint, bigint, text, text, uuid, integer, jsonb, text, text) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."get_latest_flow_versions"(bigint, bigint, text, text, uuid, integer, jsonb, text, text) TO "anon";
GRANT ALL ON FUNCTION "public"."get_latest_flow_versions"(bigint, bigint, text, text, uuid, integer, jsonb, text, text) TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_latest_flow_versions"(bigint, bigint, text, text, uuid, integer, jsonb, text, text) TO "service_role";

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
        visible_rows.model_id,
        version_counts.version_count
      FROM visible_rows
      JOIN matched_ids ON matched_ids.id = visible_rows.id
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
      counted_rows.model_id,
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

ALTER FUNCTION "public"."get_latest_process_versions"(bigint, bigint, text, text, uuid, integer, text, text, text) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."get_latest_process_versions"(bigint, bigint, text, text, uuid, integer, text, text, text) TO "anon";
GRANT ALL ON FUNCTION "public"."get_latest_process_versions"(bigint, bigint, text, text, uuid, integer, text, text, text) TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_latest_process_versions"(bigint, bigint, text, text, uuid, integer, text, text, text) TO "service_role";

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

ALTER FUNCTION "public"."get_latest_lifecyclemodel_versions"(bigint, bigint, text, text, uuid, integer, text, text) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."get_latest_lifecyclemodel_versions"(bigint, bigint, text, text, uuid, integer, text, text) TO "anon";
GRANT ALL ON FUNCTION "public"."get_latest_lifecyclemodel_versions"(bigint, bigint, text, text, uuid, integer, text, text) TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_latest_lifecyclemodel_versions"(bigint, bigint, text, text, uuid, integer, text, text) TO "service_role";

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
  "version_count" bigint,
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

ALTER FUNCTION "public"."pgroonga_search_flows_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "service_role";

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
  "version_count" bigint,
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
        visible_rows.model_id,
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
      ranked_rows.model_id,
      ranked_rows.version_count,
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
  "version_count" bigint,
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

ALTER FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "service_role";
