CREATE OR REPLACE FUNCTION "api"."qry_reference_review_impacted_roots"("p_reference_review_id" "uuid", "p_include_history" boolean DEFAULT false) RETURNS TABLE("root_review_id" "uuid", "target_table" "text", "data_id" "uuid", "data_version" "text", "state_code" integer, "is_current" boolean)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
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
    root_review.id,
    root_review.target_table,
    root_review.data_id,
    btrim(root_review.data_version::text),
    root_review.state_code,
    root_review.current_reference_review_ids
      @> array[p_reference_review_id]::uuid[]
  from private.reviews as root_review
  where root_review.review_kind = 'root'
    and (
      root_review.current_reference_review_ids
        @> array[p_reference_review_id]::uuid[]
      or (
        coalesce(p_include_history, false)
        and root_review.all_reference_review_ids
          @> array[p_reference_review_id]::uuid[]
      )
    )
  order by root_review.modified_at desc, root_review.id;
end;
$$;

ALTER FUNCTION "api"."qry_reference_review_impacted_roots"("p_reference_review_id" "uuid", "p_include_history" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_reference_review_impacted_roots"("p_reference_review_id" "uuid", "p_include_history" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_reference_review_impacted_roots"("p_reference_review_id" "uuid", "p_include_history" boolean) TO "api_internal_executor";
