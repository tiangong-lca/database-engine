CREATE OR REPLACE FUNCTION "api"."portal_list_versions_v1"("p_kind" "text", "p_id" "uuid", "p_cursor" "text" DEFAULT NULL::"text", "p_limit" integer DEFAULT 20) RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    AS $_$
declare
  v_cursor jsonb;
  v_cursor_version text;
  v_items jsonb;
  v_next_cursor text;
begin
  if p_kind not in ('process', 'flow')
     or p_id is null
     or p_limit is null
     or p_limit not between 1 and 50 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
       or (select count(*) from jsonb_object_keys(v_cursor)) <> 4
       or v_cursor ->> 'v' <> '1'
       or v_cursor ->> 'kind' <> p_kind
       or v_cursor ->> 'id' <> p_id::text
       or coalesce(v_cursor ->> 'version', '') !~ '^\d{2}\.\d{2}\.\d{3}$' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
    v_cursor_version := v_cursor ->> 'version';
  end if;

  with all_versions as materialized (
    select source.*,
      row_number() over (order by source.version desc) = 1 as is_latest,
      private.portal_capabilities_v1(
        p_kind, source.state_code, source.json_data
      ) as capabilities
    from private.portal_dataset_rows_v1(p_kind, p_id) as source
  ), ordered as materialized (
    select all_versions.*,
      row_number() over (order by all_versions.version desc) as page_rank
    from all_versions
    where v_cursor_version is null or all_versions.version < v_cursor_version
    order by all_versions.version desc
    limit p_limit + 1
  )
  select
    coalesce(jsonb_agg(jsonb_build_object(
      'key', jsonb_build_object(
        'kind', p_kind,
        'id', ordered.id::text,
        'version', ordered.version
      ),
      'accessLevel', case
        when (ordered.capabilities ->> 'exchangesVisible')::boolean
          then 'open'
        else 'metadata_only'
      end,
      'capabilities', ordered.capabilities,
      'modifiedAt', private.portal_timestamp_v1(ordered.modified_at),
      'isLatest', ordered.is_latest
    ) order by ordered.page_rank)
      filter (where ordered.page_rank <= p_limit), '[]'::jsonb),
    case when max(ordered.page_rank) > p_limit
      then private.portal_cursor_encode_v1(
        (jsonb_agg(jsonb_build_object(
          'v', 1,
          'kind', p_kind,
          'id', p_id::text,
          'version', ordered.version
        ) order by ordered.page_rank)
          filter (where ordered.page_rank = p_limit)) -> 0
      )
      else null
    end
  into v_items, v_next_cursor
  from ordered;

  return private.portal_lcia_decorate_item_page_v1(
    jsonb_build_object(
      'schemaVersion', 'portal.public-version-page.v1',
      'dataset', jsonb_build_object('kind', p_kind, 'id', p_id::text),
      'items', v_items,
      'nextCursor', v_next_cursor
    )
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

ALTER FUNCTION "api"."portal_list_versions_v1"("p_kind" "text", "p_id" "uuid", "p_cursor" "text", "p_limit" integer) OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "api"."portal_list_versions_v1"("p_kind" "text", "p_id" "uuid", "p_cursor" "text", "p_limit" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."portal_list_versions_v1"("p_kind" "text", "p_id" "uuid", "p_cursor" "text", "p_limit" integer) TO "anon";

GRANT ALL ON FUNCTION "api"."portal_list_versions_v1"("p_kind" "text", "p_id" "uuid", "p_cursor" "text", "p_limit" integer) TO "authenticated";
