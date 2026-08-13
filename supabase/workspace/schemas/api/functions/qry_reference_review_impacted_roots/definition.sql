CREATE OR REPLACE FUNCTION "api"."qry_reference_review_impacted_roots"("p_reference_review_id" "uuid", "p_include_history" boolean DEFAULT false) RETURNS TABLE("root_review_id" "uuid", "target_table" "text", "data_id" "uuid", "data_version" "text", "state_code" integer, "is_current" boolean)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_reference private.reviews%rowtype;
  v_candidate_ids uuid[];
begin
  if v_actor is null or not api.cmd_review_is_review_admin(v_actor) then
    raise exception using errcode = '42501', message = 'REVIEW_ADMIN_REQUIRED';
  end if;

  select review_row.*
  into v_reference
  from private.reviews as review_row
  where review_row.id = p_reference_review_id
    and review_row.review_kind = 'reference';

  if not found then
    return;
  end if;

  select coalesce(
    pg_catalog.array_agg(candidate.root_review_id order by candidate.root_review_id),
    array[]::uuid[]
  )
  into v_candidate_ids
  from private.review_candidate_root_ids_v1(
    v_reference.target_table,
    v_reference.data_id,
    pg_catalog.btrim(v_reference.data_version::text)
  ) as candidate;

  return query
  select
    root_review.id,
    root_review.target_table,
    root_review.data_id,
    pg_catalog.btrim(root_review.data_version::text),
    root_review.state_code,
    true
  from private.review_resolve_current_reference_targets_v1(
    v_candidate_ids
  ) as target
  join private.reviews as root_review
    on root_review.id = target.root_review_id
    and root_review.review_kind = 'root'
  where target.target_table = v_reference.target_table
    and target.data_id = v_reference.data_id
    and target.data_version = pg_catalog.btrim(v_reference.data_version::text)
    and target.revision_checksum = v_reference.submitted_revision_checksum
  order by root_review.modified_at desc, root_review.id;
end;
$$;

ALTER FUNCTION "api"."qry_reference_review_impacted_roots"("p_reference_review_id" "uuid", "p_include_history" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_reference_review_impacted_roots"("p_reference_review_id" "uuid", "p_include_history" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_reference_review_impacted_roots"("p_reference_review_id" "uuid", "p_include_history" boolean) TO "api_internal_executor";
