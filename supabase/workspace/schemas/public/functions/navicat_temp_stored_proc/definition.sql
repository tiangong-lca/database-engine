CREATE OR REPLACE FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "extensions"."vector", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "full_text_weight" numeric DEFAULT 0.3, "extracted_text_weight" numeric DEFAULT 0.2, "semantic_weight" numeric DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1) RETURNS TABLE("id" "uuid", "json" "jsonb")
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$ BEGIN
		RETURN QUERY WITH 
		full_text AS (
			SELECT
				ps.RANK AS ps_rank,
				ps.ID AS ps_id,
				ps.JSON AS ps_json 
			FROM
				pgroonga_search_processes ( query_text, filter_condition, 20, -- page_size: 获取足够多候选
					1, -- page_current: 第1页
				data_source, this_user_id ) ps 
		),
		ex_text AS (
    SELECT
      ex.rank AS ex_rank,
      ex.id   AS ex_id,
      p.json  AS ex_json
    FROM pgroonga_search_processes_text(
           query_text,
           20,          -- page_size
           1,      -- page_current
           data_source,
           this_user_id
         ) ex
    JOIN public.processes p ON p.id = ex.id
  ),
		semantic AS (
			SELECT
				ss.RANK AS ss_rank,
				ss.ID AS ss_id,
				ss.JSON AS ss_json 
			FROM
				semantic_search_processes ( query_embedding, filter_condition, match_threshold, match_count, data_source, this_user_id ) ss 
		) SELECT COALESCE
		( full_text.ps_id, semantic.ss_id, ex_text.ex_id ) AS ID,
		COALESCE ( full_text.ps_json, semantic.ss_json, ex_text.ex_json) AS JSON, 
		COALESCE(1.0 / (rrf_k + full_text.ps_rank), 0.0) * full_text_weight
      + COALESCE(1.0 / (rrf_k + ex_text.ex_rank), 0.0) * text_weight
      + COALESCE(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight
      AS score
		FROM
			full_text
			FULL OUTER JOIN semantic ON full_text.ps_id = semantic.ss_id
			FULL OUTER JOIN ex_text ON ex_text.ex_id = COALESCE(full_text.ps_id, semantic.ss_id) 
		ORDER BY
			score DESC 
			LIMIT page_size OFFSET ( page_current - 1 ) * page_size;
		
	END;
$$;

ALTER FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "extensions"."vector", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" numeric, "extracted_text_weight" numeric, "semantic_weight" numeric, "rrf_k" integer, "data_source" "text", "this_user_id" "text", "page_size" integer, "page_current" integer) OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "full_text_weight" numeric DEFAULT 0.3, "extracted_text_weight" numeric DEFAULT 0.2, "semantic_weight" numeric DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1) RETURNS TABLE("id" "uuid", "json" "jsonb")
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$ BEGIN
		RETURN QUERY WITH 
		full_text AS (
			SELECT
				ps.RANK AS ps_rank,
				ps.ID AS ps_id,
				ps.JSON AS ps_json 
			FROM
				pgroonga_search_processes ( query_text, filter_condition, 20, -- page_size: 获取足够多候选
					1, -- page_current: 第1页
				data_source, this_user_id ) ps 
		),
		ex_text AS (
    SELECT
      ex.rank AS ex_rank,
      ex.id   AS ex_id,
      p.json  AS ex_json
    FROM pgroonga_search_processes_text(
           query_text,
           20,          -- page_size
           1,      -- page_current
           data_source,
           this_user_id
         ) ex
    JOIN public.processes p ON p.id = ex.id
  ),
		semantic AS (
			SELECT
				ss.RANK AS ss_rank,
				ss.ID AS ss_id,
				ss.JSON AS ss_json 
			FROM
				semantic_search_processes ( query_embedding, filter_condition, match_threshold, match_count, data_source, this_user_id ) ss 
		) SELECT COALESCE
		( full_text.ps_id, semantic.ss_id, ex_text.ex_id ) AS ID,
		COALESCE ( full_text.ps_json, semantic.ss_json, ex_text.ex_json) AS JSON, 
		COALESCE(1.0 / (rrf_k + full_text.ps_rank), 0.0) * full_text_weight
      + COALESCE(1.0 / (rrf_k + ex_text.ex_rank), 0.0) * text_weight
      + COALESCE(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight
      AS score
		FROM
			full_text
			FULL OUTER JOIN semantic ON full_text.ps_id = semantic.ss_id
			FULL OUTER JOIN ex_text ON ex_text.ex_id = COALESCE(full_text.ps_id, semantic.ss_id) 
		ORDER BY
			score DESC 
			LIMIT page_size OFFSET ( page_current - 1 ) * page_size;
		
	END;
$$;

ALTER FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" numeric, "extracted_text_weight" numeric, "semantic_weight" numeric, "rrf_k" integer, "data_source" "text", "this_user_id" "text", "page_size" integer, "page_current" integer) OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "extensions"."vector", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" numeric, "extracted_text_weight" numeric, "semantic_weight" numeric, "rrf_k" integer, "data_source" "text", "this_user_id" "text", "page_size" integer, "page_current" integer) TO "anon";

GRANT ALL ON FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "extensions"."vector", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" numeric, "extracted_text_weight" numeric, "semantic_weight" numeric, "rrf_k" integer, "data_source" "text", "this_user_id" "text", "page_size" integer, "page_current" integer) TO "authenticated";

GRANT ALL ON FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "extensions"."vector", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" numeric, "extracted_text_weight" numeric, "semantic_weight" numeric, "rrf_k" integer, "data_source" "text", "this_user_id" "text", "page_size" integer, "page_current" integer) TO "service_role";

GRANT ALL ON FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" numeric, "extracted_text_weight" numeric, "semantic_weight" numeric, "rrf_k" integer, "data_source" "text", "this_user_id" "text", "page_size" integer, "page_current" integer) TO "anon";

GRANT ALL ON FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" numeric, "extracted_text_weight" numeric, "semantic_weight" numeric, "rrf_k" integer, "data_source" "text", "this_user_id" "text", "page_size" integer, "page_current" integer) TO "authenticated";

GRANT ALL ON FUNCTION "public"."_navicat_temp_stored_proc"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" numeric, "extracted_text_weight" numeric, "semantic_weight" numeric, "rrf_k" integer, "data_source" "text", "this_user_id" "text", "page_size" integer, "page_current" integer) TO "service_role";
