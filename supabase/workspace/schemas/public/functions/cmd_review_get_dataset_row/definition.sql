CREATE OR REPLACE FUNCTION "public"."cmd_review_get_dataset_row"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_lock" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_row jsonb;
begin
  if p_table not in (
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes',
    'lifecyclemodels'
  ) then
    return null;
  end if;

  execute format(
    'select to_jsonb(t) from public.%I as t where t.id = $1 and t.version = $2 %s',
    p_table,
    case when p_lock then 'for update of t' else '' end
  )
    into v_row
    using p_id, p_version;

  return v_row;
end;
$_$;

ALTER FUNCTION "public"."cmd_review_get_dataset_row"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_lock" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_get_dataset_row"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_lock" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_get_dataset_row"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_lock" boolean) TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_get_dataset_row"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_lock" boolean) TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_get_dataset_row"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_lock" boolean) TO "service_role";
