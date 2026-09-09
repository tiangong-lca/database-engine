CREATE OR REPLACE FUNCTION "api"."svc_tidas_package_read_v2"("p_requested_by" "uuid", "p_lookup_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare v_result jsonb; v_worker uuid; v_progress jsonb;
begin
  v_result := api.svc_tidas_package_read(p_requested_by, p_lookup_id);
  v_worker := (v_result #>> '{data,workerJobId}')::uuid;
  if v_worker is null or v_result #>> '{data,payload,import_policy}' is distinct from 'root_closure_v2' then
    return v_result;
  end if;
  with identities as (
    select item ->> 'table' as tab, item ->> 'id' as id, item ->> 'version' as ver,
      bool_or(item ->> 'disposition' = 'inserted') as inserted
    from private.tidas_import_groups_v2 g
    cross join lateral jsonb_array_elements(g.receipt -> 'items') item
    where g.worker_job_id = v_worker
    group by 1,2,3
  )
  select jsonb_build_object('imported_count', count(*) filter (where inserted),
    'existing_count', count(*) filter (where not inserted),
    'successful_root_count', (select count(*) from private.tidas_import_groups_v2 where worker_job_id = v_worker),
    'source', 'committed_receipts') into v_progress from identities;
  return jsonb_set(v_result, '{data,importProgress}', v_progress);
end $$;

ALTER FUNCTION "api"."svc_tidas_package_read_v2"("p_requested_by" "uuid", "p_lookup_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_tidas_package_read_v2"("p_requested_by" "uuid", "p_lookup_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_tidas_package_read_v2"("p_requested_by" "uuid", "p_lookup_id" "uuid") TO "service_role";
