CREATE OR REPLACE FUNCTION "private"."review_derive_current_references_v1"("p_root_review_ids" "uuid"[]) RETURNS TABLE("root_review_id" "uuid", "reference_review_id" "uuid", "target_table" "text", "data_id" "uuid", "data_version" "text", "submitted_revision_checksum" "text", "state_code" integer)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
begin
  if exists (
    select 1
    from private.review_resolve_current_reference_targets_v1(
      p_root_review_ids
    ) as target
    join private.reviews as root_review
      on root_review.id = target.root_review_id
      and root_review.state_code in (0, 1)
    where not exists (
      select 1
      from private.reviews as candidate
      where candidate.review_kind = 'reference'
        and candidate.target_table = target.target_table
        and candidate.data_id = target.data_id
        and pg_catalog.btrim(candidate.data_version::text) = target.data_version
        and candidate.submitted_revision_checksum = target.revision_checksum
        and candidate.state_code in (-1, 0, 1, 2)
    )
  ) then
    raise exception using
      errcode = '55000',
      message = 'MISSING_CURRENT_REFERENCE_REVIEW';
  end if;

  return query
  select
    target.root_review_id,
    reference_review.id,
    target.target_table,
    target.data_id,
    target.data_version,
    reference_review.submitted_revision_checksum,
    reference_review.state_code
  from private.review_resolve_current_reference_targets_v1(
    p_root_review_ids
  ) as target
  join lateral (
    select candidate.*
    from private.reviews as candidate
    where candidate.review_kind = 'reference'
      and candidate.target_table = target.target_table
      and candidate.data_id = target.data_id
      and pg_catalog.btrim(candidate.data_version::text) = target.data_version
      and candidate.submitted_revision_checksum = target.revision_checksum
      and candidate.state_code in (-1, 0, 1, 2)
    order by
      case when candidate.state_code in (0, 1, 2) then 0 else 1 end,
      candidate.modified_at desc,
      candidate.id
    limit 1
  ) as reference_review on true
  order by target.root_review_id, target.target_table,
    target.data_id, target.data_version;
end;
$$;

ALTER FUNCTION "private"."review_derive_current_references_v1"("p_root_review_ids" "uuid"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_derive_current_references_v1"("p_root_review_ids" "uuid"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_derive_current_references_v1"("p_root_review_ids" "uuid"[]) TO "api_internal_executor";
