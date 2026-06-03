CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_job_read_latest"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_dataset_row jsonb;
  v_owner_id uuid;
  v_job public.dataset_review_submit_requests%rowtype;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table <> 'processes' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Review-submit jobs currently support process datasets only'
    );
  end if;

  if p_revision_checksum is not null
    and p_revision_checksum !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVISION_CHECKSUM_REQUIRED',
      'status', 400,
      'message', 'revisionChecksum must be a lowercase SHA-256 hex digest'
    );
  end if;

  v_dataset_row := public.cmd_review_get_dataset_row(p_table, p_id, p_version, false);

  if v_dataset_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  v_owner_id := nullif(v_dataset_row->>'user_id', '')::uuid;

  if v_owner_id is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the dataset owner can read review-submit jobs'
    );
  end if;

  select *
    into v_job
  from public.dataset_review_submit_requests
  where dataset_table = p_table
    and dataset_id = p_id
    and dataset_version = p_version
    and requested_by = v_actor
    and (p_revision_checksum is null or revision_checksum = p_revision_checksum)
  order by created_at desc
  limit 1;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit job not found'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_job_payload(v_job)
  );
end;
$_$;

ALTER FUNCTION "public"."cmd_dataset_review_submit_job_read_latest"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_read_latest"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_read_latest"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text") TO "service_role";

GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_read_latest"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text") TO "authenticated";
