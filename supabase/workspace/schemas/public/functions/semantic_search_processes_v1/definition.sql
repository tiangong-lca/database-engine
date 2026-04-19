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

GRANT ALL ON FUNCTION "public"."semantic_search_processes_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."semantic_search_processes_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."semantic_search_processes_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "service_role";
