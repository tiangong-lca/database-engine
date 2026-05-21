CREATE INDEX IF NOT EXISTS "flows_state_code_id_version_modified_at_idx"
ON "public"."flows" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);

CREATE INDEX IF NOT EXISTS "flows_user_id_state_code_id_version_modified_at_idx"
ON "public"."flows" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);

CREATE INDEX IF NOT EXISTS "flows_team_id_state_code_id_version_modified_at_idx"
ON "public"."flows" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);

CREATE INDEX IF NOT EXISTS "processes_state_code_id_version_modified_at_idx"
ON "public"."processes" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);

CREATE INDEX IF NOT EXISTS "processes_user_id_state_code_id_version_modified_at_idx"
ON "public"."processes" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);

CREATE INDEX IF NOT EXISTS "processes_team_id_state_code_id_version_modified_at_idx"
ON "public"."processes" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);

CREATE INDEX IF NOT EXISTS "lifecyclemodels_state_code_id_version_modified_at_idx"
ON "public"."lifecyclemodels" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);

CREATE INDEX IF NOT EXISTS "lifecyclemodels_user_id_state_code_id_version_modified_at_idx"
ON "public"."lifecyclemodels" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);

CREATE INDEX IF NOT EXISTS "lifecyclemodels_team_id_state_code_id_version_modified_at_idx"
ON "public"."lifecyclemodels" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);

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
      SELECT f.*
      FROM public.flows f
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
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_sort_by := lower(coalesce(sort_by, 'modified_at'));
  normalized_sort_direction := lower(coalesce(sort_direction, 'desc'));

  RETURN QUERY
    WITH visible_rows AS (
      SELECT p.*
      FROM public.processes p
      WHERE
        (
          (data_source = 'tg' AND p.state_code = 100)
          OR (data_source = 'co' AND p.state_code = 200)
          OR (data_source = 'my' AND p.user_id::text = this_user_id)
          OR (data_source = 'te' AND team_id_filter IS NOT NULL AND p.team_id = team_id_filter)
        )
        AND (team_id_filter IS NULL OR data_source NOT IN ('tg', 'co') OR p.team_id = team_id_filter)
        AND (state_code_filter IS NULL OR data_source NOT IN ('my', 'te') OR p.state_code = state_code_filter)
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
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_sort_by := lower(coalesce(sort_by, 'modified_at'));
  normalized_sort_direction := lower(coalesce(sort_direction, 'desc'));

  RETURN QUERY
    WITH visible_rows AS (
      SELECT l.*
      FROM public.lifecyclemodels l
      WHERE
        (
          (data_source = 'tg' AND l.state_code = 100)
          OR (data_source = 'co' AND l.state_code = 200)
          OR (data_source = 'my' AND l.user_id::text = this_user_id)
          OR (data_source = 'te' AND team_id_filter IS NOT NULL AND l.team_id = team_id_filter)
        )
        AND (team_id_filter IS NULL OR data_source NOT IN ('tg', 'co') OR l.team_id = team_id_filter)
        AND (state_code_filter IS NULL OR data_source NOT IN ('my', 'te') OR l.state_code = state_code_filter)
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
  filter_condition_jsonb jsonb;
  flow_type text;
  flow_type_array text[];
  as_input boolean;
  classification_filter jsonb;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
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
  filter_condition_jsonb jsonb;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);

  RETURN QUERY
    WITH visible_rows AS (
      SELECT p.*, p.tableoid AS source_tableoid, p.ctid AS source_ctid
      FROM public.processes p
      WHERE
        (
          (data_source = 'tg' AND p.state_code = 100)
          OR (data_source = 'co' AND p.state_code = 200)
          OR (data_source = 'my' AND p.user_id::text = this_user_id)
          OR (data_source = 'te' AND team_id_filter IS NOT NULL AND p.team_id = team_id_filter)
        )
        AND (team_id_filter IS NULL OR data_source NOT IN ('tg', 'co') OR p.team_id = team_id_filter)
        AND (state_code_filter IS NULL OR data_source NOT IN ('my', 'te') OR p.state_code = state_code_filter)
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
  filter_condition_jsonb jsonb;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);

  RETURN QUERY
    WITH visible_rows AS (
      SELECT l.*, l.tableoid AS source_tableoid, l.ctid AS source_ctid
      FROM public.lifecyclemodels l
      WHERE
        (
          (data_source = 'tg' AND l.state_code = 100)
          OR (data_source = 'co' AND l.state_code = 200)
          OR (data_source = 'my' AND l.user_id::text = this_user_id)
          OR (data_source = 'te' AND team_id_filter IS NOT NULL AND l.team_id = team_id_filter)
        )
        AND (team_id_filter IS NULL OR data_source NOT IN ('tg', 'co') OR l.team_id = team_id_filter)
        AND (state_code_filter IS NULL OR data_source NOT IN ('my', 'te') OR l.state_code = state_code_filter)
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
  "version_count" bigint,
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
        fused.score
      FROM visible_rows
      JOIN fused ON fused.id = visible_rows.id
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
  "version_count" bigint,
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
        visible_rows.model_id,
        visible_rows.team_id,
        version_counts.version_count,
        fused.score
      FROM visible_rows
      JOIN fused ON fused.id = visible_rows.id
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
      counted_rows.model_id,
      counted_rows.team_id,
      counted_rows.version_count,
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
  "version_count" bigint,
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
        fused.score
      FROM visible_rows
      JOIN fused ON fused.id = visible_rows.id
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
    ORDER BY counted_rows.score DESC, counted_rows.modified_at DESC, counted_rows.id
    LIMIT greatest(coalesce(page_size, 10), 1)
    OFFSET (greatest(coalesce(page_current, 1), 1) - 1) * greatest(coalesce(page_size, 10), 1);
END;
$$;

ALTER FUNCTION "public"."hybrid_search_lifecyclemodels"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."hybrid_search_lifecyclemodels"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."hybrid_search_lifecyclemodels"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."hybrid_search_lifecyclemodels"(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) TO "service_role";
