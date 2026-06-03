CREATE OR REPLACE FUNCTION "public"."lca_read_latest_single_solve_result"("p_requested_by" "uuid", "p_snapshot_id" "uuid", "p_process_index" integer) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_cache public.lca_result_cache%rowtype;
  v_job public.worker_jobs%rowtype;
  v_result public.lca_results%rowtype;
  v_amount numeric;
begin
  -- Authorization is enforced by EXECUTE grants below; runtime service clients can
  -- call this RPC even when request-header GUCs are not populated by PostgREST.

  if p_snapshot_id is null or p_process_index is null or p_process_index < 0 then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_LCA_SOLVE_LOOKUP',
      'status', 400,
      'message', 'p_snapshot_id and a non-negative p_process_index are required'
    );
  end if;

  select cache_row.*
    into v_cache
  from public.lca_result_cache as cache_row
  join public.worker_jobs as worker_job
    on worker_job.id = cache_row.worker_job_id
  join public.lca_results as result_row
    on result_row.id = cache_row.result_id
  where cache_row.snapshot_id = p_snapshot_id
    and cache_row.status = 'ready'
    and cache_row.result_id is not null
    and worker_job.job_kind = 'lca.solve_one'
    and (p_requested_by is null or worker_job.requested_by = p_requested_by)
    and cache_row.request_payload->>'demand_mode' = 'single'
    and cache_row.request_payload#>>'{demand,process_index}' ~ '^[0-9]+$'
    and (cache_row.request_payload#>>'{demand,process_index}')::integer = p_process_index
  order by cache_row.updated_at desc, cache_row.created_at desc
  limit 1;

  if v_cache.id is null then
    return jsonb_build_object('ok', true, 'data', null);
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = v_cache.worker_job_id;

  select *
    into v_result
  from public.lca_results
  where id = v_cache.result_id;

  v_amount := case
    when jsonb_typeof(v_cache.request_payload#>'{demand,amount}') = 'number'
      then (v_cache.request_payload#>>'{demand,amount}')::numeric
    else 1
  end;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_strip_nulls(
      jsonb_build_object(
        'snapshotId', v_cache.snapshot_id,
        'processIndex', p_process_index,
        'amount', v_amount,
        'cache', jsonb_strip_nulls(
          jsonb_build_object(
            'cacheId', v_cache.id,
            'requestKey', v_cache.request_key,
            'status', v_cache.status,
            'createdAt', v_cache.created_at,
            'updatedAt', v_cache.updated_at
          )
        ),
        'result', jsonb_strip_nulls(
          jsonb_build_object(
            'resultId', v_result.id,
            'legacyJobId', v_result.job_id,
            'workerJobId', v_result.worker_job_id,
            'snapshotId', v_result.snapshot_id,
            'createdAt', v_result.created_at,
            'artifact', jsonb_strip_nulls(
              jsonb_build_object(
                'artifactUrl', v_result.artifact_url,
                'artifactFormat', v_result.artifact_format,
                'artifactByteSize', v_result.artifact_byte_size,
                'artifactSha256', v_result.artifact_sha256
              )
            )
          )
        ),
        'workerJob', public.worker_job_payload(v_job, false)
      )
    )
  );
end;
$_$;

ALTER FUNCTION "public"."lca_read_latest_single_solve_result"("p_requested_by" "uuid", "p_snapshot_id" "uuid", "p_process_index" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lca_read_latest_single_solve_result"("p_requested_by" "uuid", "p_snapshot_id" "uuid", "p_process_index" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."lca_read_latest_single_solve_result"("p_requested_by" "uuid", "p_snapshot_id" "uuid", "p_process_index" integer) TO "service_role";
