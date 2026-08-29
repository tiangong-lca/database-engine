CREATE OR REPLACE FUNCTION "private"."catalog_portal_single_character_search_v1_impl"("p_kind" "text", "p_query" "text", "p_cursor_rank" "text", "p_cursor_id" "uuid", "p_cursor_version" "text", "p_limit" integer, "p_query_fingerprint" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    SET "work_mem" TO '32MB'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "jit" TO 'off'
    SET "row_security" TO 'on'
    AS $$
declare
  v_items jsonb;
  v_next_cursor_payload jsonb;
begin
  perform private.assert_portal_catalog_character_contract_v1();

  if p_kind not in ('process', 'flow')
     or pg_catalog.char_length(p_query) <> 1
     or p_limit not between 1 and 50 then
    raise exception 'invalid Portal character Search'
      using errcode = '22023';
  end if;

  with latest as materialized (
    select distinct on (character_row.id)
      character_row.id,
      character_row.version,
      character_row.state_code,
      character_row.modified_at,
      character_row.document_characters,
      character_row.name_characters,
      character_row.name_exact_characters,
      character_row.classification_characters,
      character_row.classification_exact_characters
    from private.portal_catalog_character_rows_v1 as character_row
    where character_row.dataset_kind = p_kind
    order by character_row.id,
      character_row.version desc,
      character_row.modified_at desc,
      character_row.state_code desc
  ), scored as materialized (
    select latest.*,
      case
        when pg_catalog.strpos(
          latest.name_exact_characters, p_query
        ) > 0 then 0.95::numeric
        when pg_catalog.strpos(
          latest.classification_exact_characters, p_query
        ) > 0 then 0.92::numeric
        else 0.70::numeric
      end as score,
      case
        when pg_catalog.strpos(latest.name_characters, p_query) > 0
          then pg_catalog.jsonb_build_array('name')
        when pg_catalog.strpos(
          latest.classification_characters, p_query
        ) > 0 then pg_catalog.jsonb_build_array('classification')
        else pg_catalog.jsonb_build_array('full_text')
      end as reason_codes
    from latest
    where pg_catalog.strpos(latest.document_characters, p_query) > 0
  ), after_cursor as materialized (
    select scored.*
    from scored
    where p_cursor_rank is null
      or scored.score < p_cursor_rank::numeric
      or (
        scored.score = p_cursor_rank::numeric
        and (
          scored.id > p_cursor_id
          or (
            scored.id = p_cursor_id
            and scored.version < p_cursor_version
          )
        )
      )
  ), ordered as materialized (
    select after_cursor.*,
      pg_catalog.row_number() over (
        order by after_cursor.score desc,
          after_cursor.id asc,
          after_cursor.version desc
      ) as page_rank
    from after_cursor
    order by after_cursor.score desc,
      after_cursor.id asc,
      after_cursor.version desc
    limit p_limit + 1
  ), hydrated as materialized (
    select ordered.*,
      projection.card
    from ordered
    join private.portal_catalog_search_rows_v1 as projection
      on projection.dataset_kind = p_kind
     and projection.id = ordered.id
     and projection.version = ordered.version
  )
  select coalesce(
      pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
          'key', pg_catalog.jsonb_build_object(
            'kind', p_kind,
            'id', hydrated.id::text,
            'version', hydrated.version
          ),
          'accessLevel', hydrated.card -> 'accessLevel',
          'capabilities', hydrated.card -> 'capabilities',
          'names', hydrated.card -> 'names',
          'summary', hydrated.card -> 'summary',
          'geography', hydrated.card -> 'geography',
          'referenceYear', hydrated.card -> 'referenceYear',
          'modifiedAt', pg_catalog.to_char(
            hydrated.modified_at at time zone 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
          ),
          'match', pg_catalog.jsonb_build_object(
            'kind', 'lexical',
            'score', hydrated.score,
            'reasonCodes', hydrated.reason_codes
          )
        )
        order by hydrated.page_rank
      ) filter (where hydrated.page_rank <= p_limit),
      '[]'::jsonb
    ),
    case
      when pg_catalog.max(hydrated.page_rank) > p_limit then
        (
          pg_catalog.jsonb_agg(
            pg_catalog.jsonb_build_object(
              'v', 1,
              'fp', p_query_fingerprint,
              'rankKey', hydrated.score::text,
              'kind', p_kind,
              'id', hydrated.id::text,
              'version', hydrated.version
            )
            order by hydrated.page_rank
          ) filter (where hydrated.page_rank = p_limit)
        ) -> 0
      else null
    end
  into v_items, v_next_cursor_payload
  from hydrated;

  return pg_catalog.jsonb_build_object(
    'items', v_items,
    'nextCursorPayload', v_next_cursor_payload
  );
end
$$;

ALTER FUNCTION "private"."catalog_portal_single_character_search_v1_impl"("p_kind" "text", "p_query" "text", "p_cursor_rank" "text", "p_cursor_id" "uuid", "p_cursor_version" "text", "p_limit" integer, "p_query_fingerprint" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."catalog_portal_single_character_search_v1_impl"("p_kind" "text", "p_query" "text", "p_cursor_rank" "text", "p_cursor_id" "uuid", "p_cursor_version" "text", "p_limit" integer, "p_query_fingerprint" "text") FROM PUBLIC;
