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

GRANT ALL ON FUNCTION "public"."semantic_search_flows_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."semantic_search_flows_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."semantic_search_flows_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "service_role";
