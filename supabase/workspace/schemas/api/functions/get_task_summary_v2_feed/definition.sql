CREATE OR REPLACE FUNCTION "api"."get_task_summary_v2_feed"("p_category" "text" DEFAULT NULL::"text", "p_job_kinds" "text"[] DEFAULT NULL::"text"[], "p_statuses" "text"[] DEFAULT NULL::"text"[], "p_updated_since" timestamp with time zone DEFAULT NULL::timestamp with time zone, "p_cursor_updated_at" timestamp with time zone DEFAULT NULL::timestamp with time zone, "p_cursor_job_id" "uuid" DEFAULT NULL::"uuid", "p_limit" integer DEFAULT 50, "p_root_only" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare v jsonb;
begin
  v:=private.get_task_summary_v2_feed_unversioned(p_category,p_job_kinds,p_statuses,p_updated_since,p_cursor_updated_at,p_cursor_job_id,p_limit,p_root_only);
  if coalesce((v->>'ok')::boolean,false) then
    v:=jsonb_set(v,'{data,items}',coalesce((select jsonb_agg(item||jsonb_build_object('schemaVersion','task-summary.v2')) from jsonb_array_elements(coalesce(v->'data'->'items','[]'::jsonb)) item),'[]'::jsonb));
  end if;
  return v;
end;
$$;

ALTER FUNCTION "api"."get_task_summary_v2_feed"("p_category" "text", "p_job_kinds" "text"[], "p_statuses" "text"[], "p_updated_since" timestamp with time zone, "p_cursor_updated_at" timestamp with time zone, "p_cursor_job_id" "uuid", "p_limit" integer, "p_root_only" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."get_task_summary_v2_feed"("p_category" "text", "p_job_kinds" "text"[], "p_statuses" "text"[], "p_updated_since" timestamp with time zone, "p_cursor_updated_at" timestamp with time zone, "p_cursor_job_id" "uuid", "p_limit" integer, "p_root_only" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."get_task_summary_v2_feed"("p_category" "text", "p_job_kinds" "text"[], "p_statuses" "text"[], "p_updated_since" timestamp with time zone, "p_cursor_updated_at" timestamp with time zone, "p_cursor_job_id" "uuid", "p_limit" integer, "p_root_only" boolean) TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."get_task_summary_v2_feed"("p_category" "text", "p_job_kinds" "text"[], "p_statuses" "text"[], "p_updated_since" timestamp with time zone, "p_cursor_updated_at" timestamp with time zone, "p_cursor_job_id" "uuid", "p_limit" integer, "p_root_only" boolean) TO "authenticated";
