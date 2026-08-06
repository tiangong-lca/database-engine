CREATE OR REPLACE FUNCTION "private"."review_validate_scope_history_v1"("p_root_review_id" "uuid", "p_scope_history" "jsonb") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_snapshot jsonb;
  v_item jsonb;
  v_expected_version integer := 1;
  v_reference private.reviews%rowtype;
begin
  if pg_catalog.jsonb_typeof(p_scope_history) <> 'object'
    or p_scope_history->>'schema_version' <> 'review_scope.v1'
    or pg_catalog.jsonb_typeof(p_scope_history->'snapshots') <> 'array'
    or pg_catalog.jsonb_array_length(p_scope_history->'snapshots') = 0 then
    raise exception using errcode = '22023', message = 'REVIEW_SCOPE_INVALID';
  end if;

  if pg_catalog.jsonb_array_length(p_scope_history->'snapshots') > 100 then
    raise exception using errcode = '54000', message = 'REVIEW_SCOPE_SNAPSHOT_LIMIT';
  end if;

  if pg_catalog.pg_column_size(p_scope_history) > 8388608 then
    raise exception using errcode = '54000', message = 'REVIEW_SCOPE_SIZE_LIMIT';
  end if;

  for v_snapshot in
    select value
    from pg_catalog.jsonb_array_elements(p_scope_history->'snapshots')
      with ordinality as snapshot(value, ordinality)
    order by snapshot.ordinality
  loop
    if coalesce((v_snapshot->>'version_no')::integer, 0) <> v_expected_version
      or v_snapshot->>'scope_basis' not in (
        'submitted',
        'review_metadata',
        'approved',
        'reference_repair',
        'migration'
      )
      or coalesce(v_snapshot->>'root_revision_checksum', '')
        !~ '^[a-f0-9]{64}$'
      or coalesce(v_snapshot->>'scope_checksum', '')
        !~ '^[a-f0-9]{64}$'
      or pg_catalog.jsonb_typeof(v_snapshot->'items') <> 'array'
      or pg_catalog.jsonb_array_length(v_snapshot->'items') = 0
      or pg_catalog.jsonb_array_length(v_snapshot->'items') > 5000 then
      raise exception using errcode = '22023', message = 'REVIEW_SCOPE_INVALID';
    end if;

    if v_snapshot->>'scope_checksum'
      <> private.review_scope_checksum_v1(v_snapshot->'items') then
      raise exception using errcode = '22023', message = 'REVIEW_SCOPE_CHECKSUM_MISMATCH';
    end if;

    if (
      select count(*)
      from pg_catalog.jsonb_array_elements(v_snapshot->'items') as item(value)
    ) <> (
      select count(*)
      from (
        select distinct
          item.value->>'target_table',
          item.value->>'data_id',
          item.value->>'data_version',
          item.value->>'submitted_revision_checksum'
        from pg_catalog.jsonb_array_elements(v_snapshot->'items') as item(value)
      ) as distinct_items
    ) then
      raise exception using errcode = '22023', message = 'REVIEW_SCOPE_ITEM_DUPLICATE';
    end if;

    for v_item in
      select value
      from pg_catalog.jsonb_array_elements(v_snapshot->'items')
    loop
      if v_item->>'item_kind' not in ('root', 'reference')
        or v_item->>'target_table' not in (
          'contacts',
          'sources',
          'unitgroups',
          'flowproperties',
          'flows',
          'processes',
          'lifecyclemodels'
        )
        or coalesce(v_item->>'data_id', '')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or nullif(v_item->>'data_version', '') is null
        or coalesce(v_item->>'submitted_revision_checksum', '')
          !~ '^[a-f0-9]{64}$'
        or coalesce(v_item->>'target_owner_id', '')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or (
          nullif(v_item->>'target_team_id', '') is not null
          and v_item->>'target_team_id'
            !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        ) then
        raise exception using errcode = '22023', message = 'REVIEW_SCOPE_ITEM_INVALID';
      end if;

      if v_item->>'item_kind' = 'root' then
        if nullif(v_item->>'reference_review_id', '') is not null then
          raise exception using errcode = '22023', message = 'REVIEW_SCOPE_ROOT_REFERENCE_INVALID';
        end if;
      else
        if coalesce(v_item->>'reference_review_id', '')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
          raise exception using errcode = '22023', message = 'REVIEW_SCOPE_REFERENCE_REQUIRED';
        end if;

        select review_row.*
        into v_reference
        from private.reviews as review_row
        where review_row.id = (v_item->>'reference_review_id')::uuid;

        if not found
          or v_reference.review_kind <> 'reference'
          or v_reference.target_table <> v_item->>'target_table'
          or v_reference.data_id <> (v_item->>'data_id')::uuid
          or btrim(v_reference.data_version::text) <> v_item->>'data_version'
          or v_reference.submitted_revision_checksum
            <> v_item->>'submitted_revision_checksum' then
          raise exception using errcode = '23503', message = 'REVIEW_SCOPE_REFERENCE_MISMATCH';
        end if;
      end if;
    end loop;

    v_expected_version := v_expected_version + 1;
  end loop;

  if coalesce((p_scope_history->>'current_version')::integer, 0)
    <> v_expected_version - 1 then
    raise exception using errcode = '22023', message = 'REVIEW_SCOPE_CURRENT_VERSION_INVALID';
  end if;

  if p_root_review_id is not null and not exists (
    select 1
    from private.reviews as root_review
    where root_review.id = p_root_review_id
      and root_review.review_kind = 'root'
  ) then
    raise exception using errcode = '23503', message = 'ROOT_REVIEW_NOT_FOUND';
  end if;
end;
$_$;

ALTER FUNCTION "private"."review_validate_scope_history_v1"("p_root_review_id" "uuid", "p_scope_history" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_validate_scope_history_v1"("p_root_review_id" "uuid", "p_scope_history" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_validate_scope_history_v1"("p_root_review_id" "uuid", "p_scope_history" "jsonb") TO "api_internal_executor";
