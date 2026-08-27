CREATE OR REPLACE FUNCTION "api"."portal_get_published_lcia_values_v1"("p_mode" "text", "p_process_refs" "jsonb", "p_impact_ref" "text", "p_cursor" "text", "p_limit" integer) RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    AS $_$
declare
  v_mode text := btrim(coalesce(p_mode, ''));
  v_impact_ref text := nullif(btrim(coalesce(p_impact_ref, '')), '');
  v_limit integer := coalesce(p_limit, 50);
  v_ref_count integer;
  v_distinct_ref_count integer;
  v_impact_match_count integer;
  v_query_hash text;
  v_query_fields text[];
  v_cursor jsonb;
  v_cursor_request_order integer;
  v_cursor_ordinal bigint;
  v_cursor_sort_value text;
  v_cursor_sort_numeric numeric;
  v_binding record;
  v_projection record;
  v_rows jsonb := '[]'::jsonb;
  v_next_cursor text;
begin
  if v_mode not in (
       'process_all_impacts',
       'processes_one_impact',
       'ranked_processes_one_impact'
     )
     or v_limit not between 1 and 50
     or jsonb_typeof(p_process_refs) is distinct from 'array' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_ref_count := jsonb_array_length(p_process_refs);
  if v_ref_count not between 1 and 50
     or (v_mode = 'process_all_impacts' and v_ref_count <> 1)
     or (v_mode = 'process_all_impacts' and v_impact_ref is not null)
     or (v_mode <> 'process_all_impacts'
         and (v_impact_ref is null or length(v_impact_ref) > 512))
     or exists (
       select 1
       from jsonb_array_elements(p_process_refs) as item(value)
       where private.portal_lcia_json_object_has_keys_v1(
         item.value, array['id', 'version']
       ) is not true
         or jsonb_typeof(item.value -> 'id') <> 'string'
         or jsonb_typeof(item.value -> 'version') <> 'string'
         or coalesce(item.value ->> 'id', '')
              !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         or coalesce(item.value ->> 'version', '')
              !~ '^\d{2}\.\d{2}\.\d{3}$'
     ) then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  select count(distinct (item.value ->> 'id', item.value ->> 'version'))
  into v_distinct_ref_count
  from jsonb_array_elements(p_process_refs) as item(value);
  if v_distinct_ref_count <> v_ref_count then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;

  select
    binding.id,
    binding.projection_id,
    binding.lcia_result_publication_id,
    binding.package_id,
    binding.package_version,
    binding.projection_content_hash,
    binding.evidence_hash,
    binding.source_published_at,
    binding.status,
    binding.revoked_at
  into v_binding
  from private.portal_lcia_projection_publications as binding
  where binding.status = 'finalized'
  order by binding.source_published_at desc, binding.id
  limit 1;
  if v_binding.id is null then
    return null;
  end if;
  select
    projection.id,
    projection.status,
    projection.process_count,
    projection.impact_count,
    projection.expected_value_count,
    projection.content_hash
  into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = v_binding.projection_id;
  if v_projection.id is null then
    return null;
  end if;
  if v_mode <> 'process_all_impacts' then
    select count(*) into v_impact_match_count
    from private.portal_lcia_projection_impact_axis as impact_row
    where impact_row.projection_id = v_projection.id
      and impact_row.impact_id = v_impact_ref;
    if v_impact_match_count > 1 then
      raise exception using errcode = 'P0001', message = 'portal lcia unavailable';
    end if;
  end if;

  select array[
    'portal.published-lcia-query.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    v_binding.lcia_result_publication_id::text,
    v_binding.projection_content_hash,
    v_mode,
    coalesce(v_impact_ref, ''),
    v_ref_count::text
  ] || array_agg(field.value order by ref.ordinality, field.position)
  into v_query_fields
  from jsonb_array_elements(p_process_refs)
    with ordinality as ref(value, ordinality)
  cross join lateral (
    values (1, ref.value ->> 'id'), (2, ref.value ->> 'version')
  ) as field(position, value);
  v_query_hash := private.portal_lcia_projection_sha256_fields_v1(
    variadic v_query_fields
  );

  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
       or (select count(*) from jsonb_object_keys(v_cursor)) <> 8
       or v_cursor ->> 'v' <> '1'
       or v_cursor ->> 'publicationId'
            <> v_binding.lcia_result_publication_id::text
       or v_cursor ->> 'contentHash' <> v_binding.projection_content_hash
       or v_cursor ->> 'mode' <> v_mode
       or v_cursor ->> 'queryHash' <> v_query_hash
       or coalesce(v_cursor ->> 'requestOrder', '') !~ '^\d+$'
       or coalesce(v_cursor ->> 'ordinal', '') !~ '^\d+$'
       or jsonb_typeof(v_cursor -> 'sortValue') <> 'string' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
    begin
      v_cursor_request_order := (v_cursor ->> 'requestOrder')::integer;
      v_cursor_ordinal := (v_cursor ->> 'ordinal')::bigint;
    exception when others then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end;
    v_cursor_sort_value := v_cursor ->> 'sortValue';
    if v_mode = 'ranked_processes_one_impact' then
      if private.portal_canonical_decimal_v1(v_cursor_sort_value)
           is distinct from v_cursor_sort_value then
        raise exception using errcode = '22023', message = 'invalid portal request';
      end if;
      v_cursor_sort_numeric := v_cursor_sort_value::numeric;
    elsif v_cursor_sort_value <> '' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end if;

  with refs as materialized (
    select
      ref.ordinality::integer as request_order,
      (ref.value ->> 'id')::uuid as process_id,
      ref.value ->> 'version' as process_version
    from jsonb_array_elements(p_process_refs)
      with ordinality as ref(value, ordinality)
  ), eligible as materialized (
    select
      refs.request_order,
      process_row.process_index,
      impact_row.impact_index,
      value_row.ordinal,
      value_row.value_text,
      value_row.value_numeric,
      process_row.process_id,
      process_row.process_version,
      process_row.functional_unit_amount,
      process_row.functional_unit_unit,
      process_row.functional_unit_description,
      process_row.geography_code,
      process_row.geography_precision,
      process_row.reference_year,
      impact_row.method_id,
      impact_row.method_version,
      impact_row.impact_id,
      impact_row.impact_name,
      impact_row.unit
    from refs
    join private.portal_lcia_projection_process_axis as process_row
      on process_row.projection_id = v_projection.id
     and process_row.process_id = refs.process_id
     and process_row.process_version = refs.process_version
    join public.processes as public_process
      on public_process.id = process_row.process_id
     and public_process.version::text = process_row.process_version
     and public_process.state_code = 100
     and (
       private.portal_capabilities_v1(
         'process', public_process.state_code, public_process.json
       ) ->> 'exchangesVisible'
     )::boolean
    join private.portal_lcia_projection_values as value_row
      on value_row.projection_id = process_row.projection_id
     and value_row.process_index = process_row.process_index
    join private.portal_lcia_projection_impact_axis as impact_row
      on impact_row.projection_id = value_row.projection_id
     and impact_row.impact_index = value_row.impact_index
    where v_mode = 'process_all_impacts'
       or impact_row.impact_id = v_impact_ref
  ), after_cursor as materialized (
    select eligible.*
    from eligible
    where v_cursor is null
       or (
         v_mode = 'process_all_impacts'
         and eligible.ordinal > v_cursor_ordinal
       )
       or (
         v_mode = 'processes_one_impact'
         and (eligible.request_order, eligible.ordinal)
               > (v_cursor_request_order, v_cursor_ordinal)
       )
       or (
         v_mode = 'ranked_processes_one_impact'
         and (
           eligible.value_numeric < v_cursor_sort_numeric
           or (
             eligible.value_numeric = v_cursor_sort_numeric
             and eligible.ordinal > v_cursor_ordinal
           )
         )
       )
  ), ordered as materialized (
    select after_cursor.*,
      row_number() over (
        order by
          case when v_mode = 'ranked_processes_one_impact'
            then after_cursor.value_numeric end desc nulls last,
          case when v_mode = 'processes_one_impact'
            then after_cursor.request_order end asc nulls last,
          after_cursor.ordinal asc
      ) as page_rank
    from after_cursor
    order by
      case when v_mode = 'ranked_processes_one_impact'
        then after_cursor.value_numeric end desc nulls last,
      case when v_mode = 'processes_one_impact'
        then after_cursor.request_order end asc nulls last,
      after_cursor.ordinal asc
    limit v_limit + 1
  )
  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'process', jsonb_build_object(
            'id', ordered.process_id::text,
            'version', ordered.process_version
          ),
          'functionalUnit', jsonb_build_object(
            'amount', ordered.functional_unit_amount,
            'unit', ordered.functional_unit_unit,
            'description', ordered.functional_unit_description
          ),
          'geography', jsonb_build_object(
            'code', ordered.geography_code,
            'precision', ordered.geography_precision
          ),
          'referenceYear', ordered.reference_year,
          'method', jsonb_build_object(
            'id', ordered.method_id::text,
            'version', ordered.method_version
          ),
          'impact', jsonb_build_object(
            'id', ordered.impact_id,
            'name', ordered.impact_name
          ),
          'value', ordered.value_text,
          'unit', ordered.unit,
          'evidenceStatus', 'verified'
        )
        order by ordered.page_rank
      ) filter (where ordered.page_rank <= v_limit),
      '[]'::jsonb
    ),
    case
      when max(ordered.page_rank) > v_limit then
        private.portal_cursor_encode_v1(
          (
            jsonb_agg(
              jsonb_build_object(
                'v', 1,
                'publicationId', v_binding.lcia_result_publication_id::text,
                'contentHash', v_binding.projection_content_hash,
                'mode', v_mode,
                'queryHash', v_query_hash,
                'requestOrder', ordered.request_order::text,
                'ordinal', ordered.ordinal::text,
                'sortValue', case
                  when v_mode = 'ranked_processes_one_impact'
                    then ordered.value_text
                  else ''
                end
              ) order by ordered.page_rank
            ) filter (where ordered.page_rank = v_limit)
          ) -> 0
        )
      else null
    end
  into v_rows, v_next_cursor
  from ordered;

  return jsonb_build_object(
    'schemaVersion', 'portal.published-lcia-page.v1',
    'mode', v_mode,
    'publication', jsonb_build_object(
      'publicationId', v_binding.lcia_result_publication_id::text,
      'packageId', v_binding.package_id::text,
      'packageVersion', v_binding.package_version,
      'publishedAt', private.portal_timestamp_v1(v_binding.source_published_at),
      'evidenceHash', v_binding.evidence_hash
    ),
    'rows', v_rows,
    'nextCursor', v_next_cursor
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal lcia unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal lcia unavailable';
end
$_$;

ALTER FUNCTION "api"."portal_get_published_lcia_values_v1"("p_mode" "text", "p_process_refs" "jsonb", "p_impact_ref" "text", "p_cursor" "text", "p_limit" integer) OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "api"."portal_get_published_lcia_values_v1"("p_mode" "text", "p_process_refs" "jsonb", "p_impact_ref" "text", "p_cursor" "text", "p_limit" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."portal_get_published_lcia_values_v1"("p_mode" "text", "p_process_refs" "jsonb", "p_impact_ref" "text", "p_cursor" "text", "p_limit" integer) TO "anon";

GRANT ALL ON FUNCTION "api"."portal_get_published_lcia_values_v1"("p_mode" "text", "p_process_refs" "jsonb", "p_impact_ref" "text", "p_cursor" "text", "p_limit" integer) TO "authenticated";
