CREATE OR REPLACE FUNCTION "public"."ilcd_classification_get"("this_file_name" "text", "category_type" "text", "get_values" "text"[]) RETURNS SETOF "jsonb"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
BEGIN
  RETURN QUERY
  SELECT cgs2.cg
  FROM (
  select 
		cgs1.file_name,
	  cgs1.cg->>'@dataType' as cg_type,
      jsonb_array_elements(cgs1.cg -> 'category') AS cg
from
(
    SELECT
      ilcd.file_name,
      jsonb_array_elements(ilcd.json -> 'CategorySystem' -> 'categories') AS cg
    FROM
      ilcd
    WHERE ilcd.file_name = this_file_name
	) as cgs1
	where cgs1.cg->>'@dataType' = category_type
	  ) as cgs2
	  WHERE cgs2.cg->>'@name' = ANY(get_values) or cgs2.cg->>'@id' = ANY(get_values) or 'all' = ANY(get_values)
	;
END;
$$;

ALTER FUNCTION "public"."ilcd_classification_get"("this_file_name" "text", "category_type" "text", "get_values" "text"[]) OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."ilcd_classification_get"("this_file_name" "text", "category_type" "text", "get_values" "text"[]) TO "anon";

GRANT ALL ON FUNCTION "public"."ilcd_classification_get"("this_file_name" "text", "category_type" "text", "get_values" "text"[]) TO "authenticated";

GRANT ALL ON FUNCTION "public"."ilcd_classification_get"("this_file_name" "text", "category_type" "text", "get_values" "text"[]) TO "service_role";
