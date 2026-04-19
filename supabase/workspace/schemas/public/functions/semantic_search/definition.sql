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

GRANT ALL ON FUNCTION "public"."semantic_search"("query_embedding" "text", "match_threshold" double precision, "match_count" integer) TO "anon";

GRANT ALL ON FUNCTION "public"."semantic_search"("query_embedding" "text", "match_threshold" double precision, "match_count" integer) TO "authenticated";

GRANT ALL ON FUNCTION "public"."semantic_search"("query_embedding" "text", "match_threshold" double precision, "match_count" integer) TO "service_role";
