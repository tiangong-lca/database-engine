CREATE OR REPLACE FUNCTION "public"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text" DEFAULT ''::"text", "order_by" "text" DEFAULT ''::"text", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    AS $_$
 
DECLARE 
	filter_condition_jsonb JSONB;
	flowType TEXT;
	flowTypeArray TEXT[];
	asInput BOOLEAN;
	use_base_name_order boolean := false;
	use_common_category_order boolean := false;
	use_zh_icu_order boolean := false;
	order_by_jsonb jsonb;
	order_key text;
	order_lang text;
	order_dir text;
	order_lang_norm text;
BEGIN
	-- order_by 输入格式（标准 JSON）：{"key":"baseName","lang":"zh","order":"asc"} 或 {"key":"common:category","order":"asc"}

	filter_condition_jsonb := COALESCE(NULLIF(btrim(filter_condition), ''), '{}')::JSONB;

	flowType := NULLIF(btrim(filter_condition_jsonb->>'flowType'), '');
	IF flowType IS NOT NULL THEN
		flowTypeArray := string_to_array(flowType, ',');
	ELSE
		flowTypeArray := NULL;
	END IF;
	filter_condition_jsonb := filter_condition_jsonb - 'flowType';

	IF filter_condition_jsonb ? 'asInput' THEN
		asInput := NULLIF(btrim(filter_condition_jsonb->>'asInput'), '')::BOOLEAN;
	ELSE
		asInput := NULL;
	END IF;
	filter_condition_jsonb := filter_condition_jsonb - 'asInput';

	-- order_by 解析
	IF order_by IS NOT NULL AND btrim(order_by) <> '' THEN
		order_by_jsonb := order_by::jsonb;

		order_key := lower(COALESCE(NULLIF(btrim(order_by_jsonb->>'key'), ''), ''));
		order_lang := COALESCE(NULLIF(btrim(order_by_jsonb->>'lang'), ''), 'en');
		order_dir := lower(COALESCE(NULLIF(btrim(order_by_jsonb->>'order'), ''), 'asc'));
		IF order_dir NOT IN ('asc', 'desc') THEN
			order_dir := 'asc';
		END IF;

		use_base_name_order := (order_key = 'basename');
		use_common_category_order := (order_key = 'common:category');
	ELSE
		use_base_name_order := false;
		use_common_category_order := false;
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
				pgroonga_score(f.tableoid, f.ctid) AS score,
				bn.base_name,
				cat.category_name,
				CASE
					WHEN use_base_name_order THEN bn.base_name
					WHEN use_common_category_order THEN cat.category_name
				END AS order_value
			FROM flows f
			CROSS JOIN LATERAL (
				SELECT
					CASE
						WHEN use_base_name_order THEN COALESCE(
							(
								SELECT bn_item->>'#text'
								FROM jsonb_array_elements(
									CASE jsonb_typeof(
										f.json
											-> 'flowDataSet'
											-> 'flowInformation'
											-> 'dataSetInformation'
											-> 'name'
											-> 'baseName'
									)
										WHEN 'array' THEN (
											f.json
												-> 'flowDataSet'
												-> 'flowInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										WHEN 'object' THEN jsonb_build_array(
											f.json
												-> 'flowDataSet'
												-> 'flowInformation'
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
											-> 'flowDataSet'
											-> 'flowInformation'
											-> 'dataSetInformation'
											-> 'name'
											-> 'baseName'
									)
										WHEN 'array' THEN (
											f.json
												-> 'flowDataSet'
												-> 'flowInformation'
												-> 'dataSetInformation'
												-> 'name'
												-> 'baseName'
										)
										WHEN 'object' THEN jsonb_build_array(
											f.json
												-> 'flowDataSet'
												-> 'flowInformation'
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
								f.json #>> '{flowDataSet,flowInformation,dataSetInformation,name,baseName,0,#text}',
								f.json #>> '{flowDataSet,flowInformation,dataSetInformation,name,baseName,#text}'
							),
							''
						)
					END AS base_name
			) bn
			CROSS JOIN LATERAL (
				SELECT
					CASE
						WHEN use_common_category_order THEN COALESCE(
							(
								SELECT string_agg(cat_item->>'#text', ' / ' ORDER BY cat_level ASC)
								FROM (
									SELECT
										cat_item,
										CASE
											WHEN (cat_item->>'@level') ~ '^\\d+$' THEN (cat_item->>'@level')::int
											ELSE 2147483647
										END AS cat_level
									FROM jsonb_array_elements(
										CASE jsonb_typeof(
											f.json
												-> 'flowDataSet'
												-> 'flowInformation'
												-> 'dataSetInformation'
												-> 'classificationInformation'
												-> 'common:elementaryFlowCategorization'
												-> 'common:category'
										)
											WHEN 'array' THEN (
												f.json
													-> 'flowDataSet'
													-> 'flowInformation'
													-> 'dataSetInformation'
													-> 'classificationInformation'
													-> 'common:elementaryFlowCategorization'
													-> 'common:category'
										)
											WHEN 'object' THEN jsonb_build_array(
												f.json
													-> 'flowDataSet'
													-> 'flowInformation'
													-> 'dataSetInformation'
													-> 'classificationInformation'
													-> 'common:elementaryFlowCategorization'
													-> 'common:category'
										)
											ELSE '[]'::jsonb
										END
									) AS cat_item
								) ordered_cat
							),
							''
						)
					END AS category_name
			) cat
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
				AND (
					flowType IS NULL
					OR flowType = ''
					OR (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = ANY(flowTypeArray)
				)
				AND (
					asInput IS NULL
					OR asInput = false
					OR NOT(
						f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text": "Emissions", "@level": "0"}]}}}}}}'
					)
				)
		)
		SELECT
			ROW_NUMBER() OVER (
				ORDER BY
					(CASE WHEN (use_base_name_order OR use_common_category_order) AND use_zh_icu_order AND order_dir = 'asc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" ASC NULLS LAST,
					(CASE WHEN (use_base_name_order OR use_common_category_order) AND use_zh_icu_order AND order_dir = 'desc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" DESC NULLS LAST,
					CASE WHEN (use_base_name_order OR use_common_category_order) AND NOT use_zh_icu_order AND order_dir = 'asc' THEN lower(f2.order_value) END ASC NULLS LAST,
					CASE WHEN (use_base_name_order OR use_common_category_order) AND NOT use_zh_icu_order AND order_dir = 'desc' THEN lower(f2.order_value) END DESC NULLS LAST,
					f2.score DESC,
					f2.modified_at DESC,
					f2.id
			) AS rank,
			f2.id,
			f2.json,
			f2.version,
			f2.modified_at,
			COUNT(*) OVER() AS total_count
		FROM filtered f2
		ORDER BY
			(CASE WHEN (use_base_name_order OR use_common_category_order) AND use_zh_icu_order AND order_dir = 'asc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" ASC NULLS LAST,
			(CASE WHEN (use_base_name_order OR use_common_category_order) AND use_zh_icu_order AND order_dir = 'desc' THEN f2.order_value END) COLLATE "zh-Hans-CN-x-icu" DESC NULLS LAST,
			CASE WHEN (use_base_name_order OR use_common_category_order) AND NOT use_zh_icu_order AND order_dir = 'asc' THEN lower(f2.order_value) END ASC NULLS LAST,
			CASE WHEN (use_base_name_order OR use_common_category_order) AND NOT use_zh_icu_order AND order_dir = 'desc' THEN lower(f2.order_value) END DESC NULLS LAST,
			f2.score DESC,
			f2.modified_at DESC,
			f2.id
		LIMIT page_size
		OFFSET (page_current - 1) * page_size;
	END; 
	
$_$;

ALTER FUNCTION "public"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_v1"("query_text" "text", "filter_condition" "text", "order_by" "text", "page_size" bigint, "page_current" bigint, "data_source" "text") TO "service_role";
