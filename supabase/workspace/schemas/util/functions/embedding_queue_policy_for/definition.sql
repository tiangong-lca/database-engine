CREATE OR REPLACE FUNCTION "util"."embedding_queue_policy_for"("p_schema_name" "text", "p_table_name" "text", "p_edge_function" "text", "p_embedding_column" "text") RETURNS TABLE("mode" "text", "max_in_flight" integer, "max_read_count" integer, "retry_backoff_seconds" integer)
    LANGUAGE "sql" STABLE
    SET "search_path" TO ''
    AS $$
  select
    p.mode,
    p.max_in_flight,
    p.max_read_count,
    p.retry_backoff_seconds
  from util.embedding_queue_policy p
  where p.scope_schema in ('*', coalesce(p_schema_name, ''))
    and p.scope_table in ('*', coalesce(p_table_name, ''))
    and p.scope_edge_function in ('*', coalesce(p_edge_function, 'embedding'))
    and p.scope_embedding_column in ('*', coalesce(p_embedding_column, ''))
  order by
    (
      (p.scope_schema <> '*')::integer
      + (p.scope_table <> '*')::integer
      + (p.scope_edge_function <> '*')::integer
      + (p.scope_embedding_column <> '*')::integer
    ) desc,
    p.updated_at desc,
    p.id desc
  limit 1
$$;

ALTER FUNCTION "util"."embedding_queue_policy_for"("p_schema_name" "text", "p_table_name" "text", "p_edge_function" "text", "p_embedding_column" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."embedding_queue_policy_for"("p_schema_name" "text", "p_table_name" "text", "p_edge_function" "text", "p_embedding_column" "text") FROM PUBLIC;
