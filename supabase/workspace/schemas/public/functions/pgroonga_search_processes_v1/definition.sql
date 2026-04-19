CREATE OR REPLACE FUNCTION "public"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text" DEFAULT ''::"text", "order_by" "text" DEFAULT ''::"text", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "model_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $_$
DECLARE
    filter_condition_jsonb JSONB;
    use_base_name_order boolean := false;
	use_common_class_order boolean := false;
	use_zh_icu_order boolean := false;
    order_by_jsonb jsonb;
    order_key text;
    order_lang text;
    order_dir text;
	order_lang_norm text;
BEGIN
	filter_condition_jsonb := COALESCE(NULLIF(btrim(filter_condition), ''), '{}')::JSONB;

	-- order_by 输入格式（标准 JSON）：{"key":"baseName","lang":"zh","order":"asc"} 或 {"key":"common:class","order":"asc"}
	IF order_by IS NOT NULL AND btrim(order_by) <> '' THEN
		order_by_jsonb := order_by::jsonb;

		order_key := lower(COALESCE(NULLIF(btrim(order_by_jsonb->>'key'), ''), ''));
		order_lang := COALESCE(NULLIF(btrim(order_by_jsonb->>'lang'), ''), 'en');
		order_dir := lower(COALESCE(NULLIF(btrim(order_by_jsonb->>'order'), ''), 'asc'));
		IF order_dir NOT IN ('asc', 'desc') THEN
			order_dir := 'asc';
		END IF;

		use_base_name_order := (order_key = 'basename');
		use_common_class_order := (order_key = 'common:class');
	ELSE
		use_base_name_order := false;
		use_common_class_order := false;
		order_lang := 'en';
		order_dir := 'asc';
	END IF;

	order_lang_norm := lower(COALESCE(NULLIF(btrim(order_lang), ''), 'en'));
	use_zh_icu_order := (order_lang_norm LIKE 'zh%');

  RETURN QUERY
		WITH filtered AS (
			SELECT
				f.id,
				f.json,
				f.version,
				f.modified_at,
				f.model_id,
				pgroonga_score(f.tableoid, f.ctid) AS score,
				bn.base_name,
				cls.class_name,
				CASE
					WHEN use_base_name_order THEN bn.base_name
					WHEN use_common_class_order THEN cls.class_name
				END AS order_value
			FROM processes f
			CROSS JOIN LATERAL (
				SELECT
					CASE
						WHEN use_base_name_order THEN COALESCE(
							(
								SELECT bn_item->>'#text'
								FROM jsonb_array_elements(
									CASE jsonb_typeof(
										f.json
											-> 'processDataSet'
											-> 'processInformation'
											-> 'dataSetInformation'
											-> 'name'
											-> 'baseName'
									)
										WHEN 'array' THEN (
											f.json
												-> 'processDataSet'
												-> 'processInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										WHEN 'object' THEN jsonb_build_array(
											f.json
												-> 'processDataSet'
												-> 'processInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										ELSE '[]'::jsonb
									END
								) AS bn_item
								WHERE bn_item->>'@xml:lang' = order_lang
								LIMIT 1
							),
							(
								SELECT bn_item->>'#text'
								FROM jsonb_array_elements(
									CASE jsonb_typeof(
										f.json
											-> 'processDataSet'
											-> 'processInformation'
											-> 'dataSetInformation'
											-> 'name'
											-> 'baseName'
									)
										WHEN 'array' THEN (
											f.json
												-> 'processDataSet'
												-> 'processInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										WHEN 'object' THEN jsonb_build_array(
											f.json
												-> 'processDataSet'
												-> 'processInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										ELSE '[]'::jsonb
									END
								) AS bn_item
								WHERE bn_item->>'@xml:lang' = 'en'
								LIMIT 1
							),
							COALESCE(
								f.json #>> '{processDataSet,processInformation,dataSetInformation,name,baseName,0,#text}',
								f.json #>> '{processDataSet,processInformation,dataSetInformation,name,baseName,#text}'
							),
							''
						)
					END AS base_name
			) bn
			CROSS JOIN LATERAL (
				SELECT
					CASE
						WHEN use_common_class_order THEN COALESCE(
							(
								SELECT string_agg(cls_item->>'#text', ' / ' ORDER BY cls_level ASC)
								FROM (
									SELECT
										cls_item,
										CASE
											WHEN (cls_item->>'@level') ~ '^\\d+$' THEN (cls_item->>'@level')::int
											ELSE 2147483647
										END AS cls_level
									FROM jsonb_array_elements(
										CASE jsonb_typeof(
											f.json
												-> 'processDataSet'
												-> 'processInformation'
												-> 'dataSetInformation'
												-> 'classificationInformation'
												-> 'common:classification'
												-> 'common:class'
										)
											WHEN 'array' THEN (
												f.json
													-> 'processDataSet'
													-> 'processInformation'
													-> 'dataSetInformation'
													-> 'classificationInformation'
													-> 'common:classification'
													-> 'common:class'
											)
											WHEN 'object' THEN jsonb_build_array(
												f.json
													-> 'processDataSet'
													-> 'processInformation'
													-> 'dataSetInformation'
													-> 'classificationInformation'
													-> 'common:classification'
													-> 'common:class'
											)
											ELSE '[]'::jsonb
										END
									) AS cls_item
								) ordered_cls
							),
							''
						)
					END AS class_name
			) cls
			WHERE f.json @> filter_condition_jsonb
				AND f.json &@~ query_text
				AND (
					(data_source = 'tg' AND state_code = 100)
					OR (data_source = 'co' AND state_code = 200)
					OR (data_source = 'my' AND user_id = auth.uid())
					OR (
						data_source = 'te'
						AND EXISTS (
							SELECT 1
							FROM roles r
							WHERE r.user_id = auth.uid()
								AND r.team_id = f.team_id
								AND r.role::text IN ('admin', 'member', 'owner')
						)
					)
				)
		)
		SELECT
			ROW_NUMBER() OVER (
				ORDER BY
					(CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'asc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" ASC NULLS LAST,
					(CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'desc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" DESC NULLS LAST,
					CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'asc' THEN lower(f2.order_value) END ASC NULLS LAST,
					CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'desc' THEN lower(f2.order_value) END DESC NULLS LAST,
					f2.score DESC,
					f2.modified_at DESC,
					f2.id
			) AS rank,
			f2.id,
			f2.json,
			f2.version,
			f2.modified_at,
			f2.model_id,
			COUNT(*) OVER() AS total_count
		FROM filtered f2
		ORDER BY
			(CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'asc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" ASC NULLS LAST,
			(CASE WHEN (use_base_name_order OR use_common_class_order) AND use_zh_icu_order AND order_dir = 'desc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" DESC NULLS LAST,
			CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'asc' THEN lower(f2.order_value) END ASC NULLS LAST,
			CASE WHEN (use_base_name_order OR use_common_class_order) AND NOT use_zh_icu_order AND order_dir = 'desc' THEN lower(f2.order_value) END DESC NULLS LAST,
			f2.score DESC,
			f2.modified_at DESC,
			f2.id
		LIMIT page_size
		OFFSET (page_current - 1) * page_size;
END;
$_$;

ALTER FUNCTION "public"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."pgroonga_search_processes_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "service_role";
