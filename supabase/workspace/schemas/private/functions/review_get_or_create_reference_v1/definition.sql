CREATE OR REPLACE FUNCTION "private"."review_get_or_create_reference_v1"("p_target_table" "text", "p_target_row" "jsonb", "p_checksum" "text", "p_actor" "uuid") RETURNS "private"."reviews"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_reference private.reviews%rowtype;
  v_owner_id uuid := nullif(p_target_row->>'user_id', '')::uuid;
  v_team_id uuid := nullif(p_target_row->>'team_id', '')::uuid;
  v_state integer := coalesce((p_target_row->>'state_code')::integer, 0);
begin
  if v_owner_id is null then
    raise exception using
      errcode = '23502',
      message = 'REFERENCE_OWNER_UNRESOLVED';
  end if;


  select reference_row.*
  into v_reference
  from private.reviews as reference_row
  where reference_row.review_kind = 'reference'
    and reference_row.target_table = p_target_table
    and reference_row.data_id = (p_target_row->>'id')::uuid
    and btrim(reference_row.data_version::text) = p_target_row->>'version'
    and reference_row.submitted_revision_checksum = p_checksum
    and reference_row.state_code in (0, 1, 2)
  order by reference_row.state_code desc, reference_row.created_at
  limit 1
  for update;

  if found then
    return v_reference;
  end if;

  begin
    insert into private.reviews (
      id,
      data_id,
      data_version,
      state_code,
      reviewer_id,
      json,
      review_kind,
      target_table,
      submitted_revision_checksum,
      approved_revision_checksum,
      target_owner_id,
      target_team_id
    )
    values (
      gen_random_uuid(),
      (p_target_row->>'id')::uuid,
      p_target_row->>'version',
      case when v_state >= 100 then 2 else 0 end,
      '[]'::jsonb,
      private.review_build_json_v1(
        p_target_table,
        p_target_row,
        v_owner_id,
        'submit_reference_review',
        p_actor
      ),
      'reference',
      p_target_table,
      p_checksum,
      case when v_state >= 100 then p_checksum else null end,
      v_owner_id,
      v_team_id
    )
    returning * into v_reference;
  exception
    when unique_violation then
      select reference_row.*
      into strict v_reference
      from private.reviews as reference_row
      where reference_row.review_kind = 'reference'
        and reference_row.target_table = p_target_table
        and reference_row.data_id = (p_target_row->>'id')::uuid
        and btrim(reference_row.data_version::text) = p_target_row->>'version'
        and reference_row.submitted_revision_checksum = p_checksum
        and reference_row.state_code in (0, 1, 2)
      order by reference_row.state_code desc, reference_row.created_at
      limit 1
      for update;
  end;

  return v_reference;
end;
$$;

ALTER FUNCTION "private"."review_get_or_create_reference_v1"("p_target_table" "text", "p_target_row" "jsonb", "p_checksum" "text", "p_actor" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_get_or_create_reference_v1"("p_target_table" "text", "p_target_row" "jsonb", "p_checksum" "text", "p_actor" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_get_or_create_reference_v1"("p_target_table" "text", "p_target_row" "jsonb", "p_checksum" "text", "p_actor" "uuid") TO "api_internal_executor";
