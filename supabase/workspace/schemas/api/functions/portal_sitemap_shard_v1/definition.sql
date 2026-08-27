CREATE OR REPLACE FUNCTION "api"."portal_sitemap_shard_v1"("p_shard_cursor" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '4s'
    SET "work_mem" TO '8MB'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "max_parallel_workers_per_gather" TO '0'
    SET "jit" TO 'off'
    SET "row_security" TO 'on'
    AS $_$
declare
  v_cursor jsonb;
  v_bucket integer;
  v_items jsonb;
  v_result jsonb;
begin
  if p_shard_cursor is null
     or pg_catalog.octet_length(p_shard_cursor) not between 1 and 4096
     or p_shard_cursor ~ '[[:space:]]' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;

  v_cursor := private.portal_cursor_decode_v1(p_shard_cursor);
  if v_cursor is null
     or pg_catalog.jsonb_typeof(v_cursor) <> 'object'
     or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(v_cursor)) <> 4
     or v_cursor ->> 'v' <> '1'
     or v_cursor ->> 'scope' <> 'sitemap-shard'
     or v_cursor ->> 'shardCount' <> '64'
     or coalesce(v_cursor ->> 'bucket', '') !~ '^([0-9]|[1-5][0-9]|6[0-3])$'
     or private.portal_cursor_encode_v1(v_cursor) <> p_shard_cursor then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_bucket := (v_cursor ->> 'bucket')::integer;

  perform private.assert_portal_catalog_projection_contract_v1();
  perform private.assert_portal_catalog_facet_contract_v1();
  perform private.assert_portal_sitemap_projection_v1();

  with latest as materialized (
    select projection.dataset_kind,
      projection.id,
      projection.version,
      projection.modified_at
    from private.portal_sitemap_latest_rows_v1 as projection
    where projection.shard_no = v_bucket
      and projection.contract_version = 1
    order by projection.dataset_kind,
      projection.id
    limit 4097
  )
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'key', pg_catalog.jsonb_build_object(
      'kind', latest.dataset_kind,
      'id', latest.id::text,
      'version', latest.version
    ),
    'modifiedAt', private.portal_timestamp_v1(latest.modified_at)
  ) order by latest.dataset_kind, latest.id), '[]'::jsonb)
  into v_items
  from latest;

  if pg_catalog.jsonb_array_length(v_items) > 4096 then
    raise exception using
      errcode = 'P0001',
      message = 'portal sitemap unavailable';
  end if;

  v_result := pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-sitemap-shard.v1',
    'shardCursor', p_shard_cursor,
    'items', v_items
  );
  if pg_catalog.octet_length(v_result::text) > 2 * 1024 * 1024 then
    raise exception using
      errcode = '54000',
      message = 'portal sitemap response exceeded its budget';
  end if;
  return v_result;
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using
      errcode = 'P0001',
      message = 'portal sitemap unavailable';
  when others then
    raise exception using
      errcode = 'P0001',
      message = 'portal sitemap unavailable';
end
$_$;

ALTER FUNCTION "api"."portal_sitemap_shard_v1"("p_shard_cursor" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "api"."portal_sitemap_shard_v1"("p_shard_cursor" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."portal_sitemap_shard_v1"("p_shard_cursor" "text") TO "anon";

GRANT ALL ON FUNCTION "api"."portal_sitemap_shard_v1"("p_shard_cursor" "text") TO "authenticated";
