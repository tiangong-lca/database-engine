CREATE OR REPLACE FUNCTION "private"."portal_search_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE PARALLEL RESTRICTED
    SET "search_path" TO ''
    AS $_$
declare
  v_query text;
  v_filters jsonb;
  v_sort text;
  v_limit integer := coalesce(p_limit, 20);
  v_fingerprint text;
  v_cursor jsonb;
  v_cursor_rank text;
  v_cursor_id uuid;
  v_cursor_version text;
  v_kernel jsonb;
  v_next_cursor_payload jsonb;
begin
  perform private.assert_portal_catalog_projection_contract_v1();

  perform private.portal_validate_search_v1(
    p_kind,
    coalesce(p_query, ''),
    coalesce(p_filters, '{}'::jsonb),
    coalesce(p_sort, 'relevance'),
    v_limit
  );
  v_query := pg_catalog.lower(pg_catalog.btrim(coalesce(p_query, '')));
  v_filters := private.portal_normalize_filters_v1(p_filters);
  v_sort := pg_catalog.lower(pg_catalog.btrim(coalesce(p_sort, 'relevance')));
  v_fingerprint := private.portal_query_fingerprint_v1(
    p_kind,
    v_query,
    v_filters,
    v_sort
  );
  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
       or (select count(*) from pg_catalog.jsonb_object_keys(v_cursor)) <> 6
       or v_cursor ->> 'v' <> '1'
       or v_cursor ->> 'fp' <> v_fingerprint
       or v_cursor ->> 'kind' <> p_kind
       or coalesce(v_cursor ->> 'rankKey', '') = ''
       or coalesce(v_cursor ->> 'id', '')
         !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or coalesce(v_cursor ->> 'version', '') !~ '^\d{2}\.\d{2}\.\d{3}$' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
    v_cursor_rank := v_cursor ->> 'rankKey';
    v_cursor_id := (v_cursor ->> 'id')::uuid;
    v_cursor_version := v_cursor ->> 'version';
    if v_sort = 'relevance'
       and v_cursor_rank !~ '^(0(\.\d+)?|1(\.0+)?)$' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    elsif v_sort = 'modified_desc'
       and private.portal_datetime_v1(v_cursor_rank) is null then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end if;

  if pg_catalog.char_length(v_query) = 1
     and v_filters = '{}'::jsonb
     and v_sort = 'relevance' then
    v_kernel := private.catalog_portal_single_character_search_v1_impl(
      p_kind,
      v_query,
      v_cursor_rank,
      v_cursor_id,
      v_cursor_version,
      v_limit,
      v_fingerprint
    );
  else
    v_kernel := private.catalog_portal_search_v1_impl(
      p_kind,
      v_query,
      v_filters,
      v_sort,
      v_cursor_rank,
      v_cursor_id,
      v_cursor_version,
      v_limit,
      v_fingerprint
    );
  end if;

  v_next_cursor_payload := nullif(
    v_kernel -> 'nextCursorPayload',
    'null'::jsonb
  );

  return pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-search-page.v1',
    'kind', p_kind,
    'queryFingerprint', v_fingerprint,
    'items', coalesce(v_kernel -> 'items', '[]'::jsonb),
    'nextCursor', case when v_next_cursor_payload is null then null
      else private.portal_cursor_encode_v1(v_next_cursor_payload)
    end
  );
end
$_$;

ALTER FUNCTION "private"."portal_search_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_search_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text", "p_cursor" "text", "p_limit" integer) FROM PUBLIC;
