CREATE OR REPLACE FUNCTION "public"."hybrid_search_flows"("query_text" "text", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "full_text_weight" double precision DEFAULT 0.3, "extracted_text_weight" double precision DEFAULT 0.2, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1) RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone)
    LANGUAGE "plpgsql"
    SET "statement_timeout" TO '60s'
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $$
 BEGIN
		RETURN QUERY WITH full_text AS (
			SELECT
				ps.RANK AS ps_rank,
				ps.ID AS ps_id,
				ps.JSON AS ps_json 
			FROM
				pgroonga_search_flows_v1 ( query_text, filter_condition, '', 20, -- page_size: 获取足够多候选
					1, -- page_current: 第1页
				data_source ) ps 
		),
		ex_text AS (
			SELECT
				ex.RANK AS ex_rank,
				ex.ID AS ex_id,
				P.JSON AS ex_json 
			FROM
				pgroonga_search_flows_text_v1 ( query_text, 20, -- page_size
					1, -- page_current
				data_source ) ex
				JOIN PUBLIC.flows P ON P.ID = ex.ID 
		),
		semantic AS (
			SELECT
				ss.RANK AS ss_rank,
				ss.ID AS ss_id,
				ss.JSON AS ss_json 
			FROM
				semantic_search_flows_v1 ( query_embedding, filter_condition, match_threshold, match_count, data_source ) ss 
		), 
		fused_raw as (
		SELECT 
			COALESCE ( full_text.ps_id, semantic.ss_id, ex_text.ex_id ) AS ID,
			COALESCE ( full_text.ps_json, semantic.ss_json, ex_text.ex_json ) AS JSON,
			COALESCE ( 1.0 / ( rrf_k + full_text.ps_rank ), 0.0 ) * full_text_weight
			+ COALESCE ( 1.0 / ( rrf_k + ex_text.ex_rank ), 0.0 ) * extracted_text_weight
			+ COALESCE ( 1.0 / ( rrf_k + semantic.ss_rank ), 0.0 ) * semantic_weight AS score 
		FROM
			full_text
			FULL OUTER JOIN semantic ON full_text.ps_id = semantic.ss_id
			FULL OUTER JOIN ex_text ON ex_text.ex_id = COALESCE ( full_text.ps_id, semantic.ss_id ) 
		),
		fused AS (
			SELECT
				fr.id AS fid,
				SUM(fr.score) AS score
			FROM fused_raw fr
			WHERE fr.id IS NOT NULL
			GROUP BY fr.id
		)
		SELECT
			f.fid AS id,
			fl.json,
			fl.version,
			fl.modified_at
		FROM fused f
		JOIN LATERAL (
			SELECT fl.json, fl.version, fl.modified_at
			FROM public.flows fl
			WHERE fl.id = f.fid
			ORDER BY fl.modified_at DESC
			LIMIT 1
		) fl ON true
		ORDER BY f.score DESC
		LIMIT page_size OFFSET ( page_current - 1 ) * page_size;
		
	END;
$$;

ALTER FUNCTION "public"."hybrid_search_flows"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."hybrid_search_flows"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) TO "anon";

GRANT ALL ON FUNCTION "public"."hybrid_search_flows"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) TO "authenticated";

GRANT ALL ON FUNCTION "public"."hybrid_search_flows"("query_text" "text", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "full_text_weight" double precision, "extracted_text_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer) TO "service_role";
