CREATE OR REPLACE FUNCTION "api"."svc_data_product_worker_metadata"("p_worker_job_ids" "uuid"[]) RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_ids uuid[] := coalesce(p_worker_job_ids, '{}'::uuid[]);
  v_worker_rows jsonb;
  v_package_rows jsonb;
begin
  if cardinality(v_ids) > 200 then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_LIMIT_EXCEEDED',
      'status', 400
    );
  end if;

  select coalesce(
    jsonb_agg(
      jsonb_build_object('id', worker.id, 'payload_json', worker.payload_json)
      order by array_position(v_ids, worker.id)
    ),
    '[]'::jsonb
  )
  into v_worker_rows
  from private.worker_jobs as worker
  where worker.id = any(v_ids)
    and worker.job_kind = 'lcia_result.package_build';

  select coalesce(
    jsonb_agg(
      jsonb_build_object(
        'build_worker_job_id', package.build_worker_job_id,
        'id', package.id,
        'package_version', package.package_version,
        'status', package.status,
        'eligible_input_count', package.eligible_input_count,
        'included_input_count', package.included_input_count
      )
      order by array_position(v_ids, package.build_worker_job_id)
    ),
    '[]'::jsonb
  )
  into v_package_rows
  from private.lcia_result_packages as package
  where package.build_worker_job_id = any(v_ids);

  return jsonb_build_object(
    'ok', true,
    'worker_rows', v_worker_rows,
    'package_rows', v_package_rows
  );
end
$$;

ALTER FUNCTION "api"."svc_data_product_worker_metadata"("p_worker_job_ids" "uuid"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_data_product_worker_metadata"("p_worker_job_ids" "uuid"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_data_product_worker_metadata"("p_worker_job_ids" "uuid"[]) TO "service_role";
