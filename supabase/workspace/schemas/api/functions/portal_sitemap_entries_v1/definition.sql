CREATE OR REPLACE FUNCTION "api"."portal_sitemap_entries_v1"("p_kind" "text", "p_cursor" "text" DEFAULT NULL::"text", "p_limit" integer DEFAULT 1000) RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    AS $_$
declare
  v_filter_kind text;
  v_cursor jsonb;
  v_cursor_kind text;
  v_cursor_id uuid;
  v_items jsonb;
  v_next_cursor text;
begin
  if pg_catalog.octet_length(coalesce(p_kind, '')) > 32 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_filter_kind := lower(btrim(coalesce(p_kind, '')));
  if v_filter_kind not in ('process', 'flow', 'all')
     or p_limit is null
     or p_limit not between 1 and 1000 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
       or (select count(*) from jsonb_object_keys(v_cursor)) <> 5
       or v_cursor ->> 'v' <> '1'
       or v_cursor ->> 'filterKind' <> v_filter_kind
       or v_cursor ->> 'kind' not in ('process', 'flow')
       or (v_filter_kind <> 'all' and v_cursor ->> 'kind' <> v_filter_kind)
       or coalesce(v_cursor ->> 'id', '') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or coalesce(v_cursor ->> 'version', '') !~ '^\d{2}\.\d{2}\.\d{3}$' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
    v_cursor_kind := v_cursor ->> 'kind';
    v_cursor_id := (v_cursor ->> 'id')::uuid;
  end if;

  with source_rows as materialized (
    select kinds.kind, source.*
    from (values ('process'::text), ('flow'::text)) as kinds(kind)
    cross join lateral private.portal_catalog_rows_v1(kinds.kind) as source
    where v_filter_kind = 'all' or kinds.kind = v_filter_kind
  ), latest as materialized (
    select candidate.*
    from (
      select source_rows.*,
        row_number() over (
          partition by source_rows.kind, source_rows.id
          order by source_rows.version desc
        ) as version_rank
      from source_rows
    ) as candidate
    where candidate.version_rank = 1
  ), ordered as materialized (
    select latest.*,
      row_number() over (order by latest.kind, latest.id) as page_rank
    from latest
    where v_cursor is null or (latest.kind, latest.id) > (v_cursor_kind, v_cursor_id)
    order by latest.kind, latest.id
    limit p_limit + 1
  )
  select
    coalesce(jsonb_agg(jsonb_build_object(
      'key', jsonb_build_object(
        'kind', ordered.kind,
        'id', ordered.id::text,
        'version', ordered.version
      ),
      'modifiedAt', private.portal_timestamp_v1(ordered.modified_at)
    ) order by ordered.page_rank) filter (where ordered.page_rank <= p_limit), '[]'::jsonb),
    case when max(ordered.page_rank) > p_limit then private.portal_cursor_encode_v1(
      (jsonb_agg(jsonb_build_object(
        'v', 1,
        'filterKind', v_filter_kind,
        'kind', ordered.kind,
        'id', ordered.id::text,
        'version', ordered.version
      ) order by ordered.page_rank) filter (where ordered.page_rank = p_limit)) -> 0
    ) else null end
  into v_items, v_next_cursor
  from ordered;

  return jsonb_build_object(
    'schemaVersion', 'portal.public-sitemap-page.v1',
    'items', v_items,
    'nextCursor', v_next_cursor
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$_$;

ALTER FUNCTION "api"."portal_sitemap_entries_v1"("p_kind" "text", "p_cursor" "text", "p_limit" integer) OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "api"."portal_sitemap_entries_v1"("p_kind" "text", "p_cursor" "text", "p_limit" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."portal_sitemap_entries_v1"("p_kind" "text", "p_cursor" "text", "p_limit" integer) TO "anon";

GRANT ALL ON FUNCTION "api"."portal_sitemap_entries_v1"("p_kind" "text", "p_cursor" "text", "p_limit" integer) TO "authenticated";
