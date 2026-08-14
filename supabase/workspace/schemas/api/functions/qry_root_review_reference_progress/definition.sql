CREATE OR REPLACE FUNCTION "api"."qry_root_review_reference_progress"("p_root_review_id" "uuid") RETURNS TABLE("reference_review_id" "uuid", "target_table" "text", "data_id" "uuid", "data_version" "text", "submitted_revision_checksum" "text", "state_code" integer, "reviewer_count" integer, "completed_reviewer_count" integer, "relation_paths" "jsonb")
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
begin
  if v_actor is null or not api.cmd_review_is_review_admin(v_actor) then
    raise exception using errcode = '42501', message = 'REVIEW_ADMIN_REQUIRED';
  end if;

  return query
  select
    progress.reference_review_id,
    progress.target_table,
    progress.data_id,
    progress.data_version,
    progress.submitted_revision_checksum,
    progress.state_code,
    progress.reviewer_count,
    progress.completed_reviewer_count,
    '[]'::jsonb
  from api.qry_root_review_reference_progress_v2(p_root_review_id) as progress;
end;
$$;

ALTER FUNCTION "api"."qry_root_review_reference_progress"("p_root_review_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_root_review_reference_progress"("p_root_review_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_root_review_reference_progress"("p_root_review_id" "uuid") TO "api_internal_executor";
