CREATE OR REPLACE FUNCTION "api"."portal_sitemap_manifest_v1"() RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '2s'
    SET "max_parallel_workers_per_gather" TO '0'
    SET "jit" TO 'off'
    SET "row_security" TO 'on'
    AS $$
declare
  v_shards jsonb;
begin
  perform private.assert_portal_catalog_projection_contract_v1();
  perform private.assert_portal_catalog_facet_contract_v1();
  perform private.assert_portal_sitemap_projection_v1();

  select pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object(
      'shardCursor',
      private.portal_cursor_encode_v1(pg_catalog.jsonb_build_object(
        'v', 1,
        'scope', 'sitemap-shard',
        'bucket', shard.bucket,
        'shardCount', 64
      )),
      'maxItems', 4096
    )
    order by shard.bucket
  )
  into v_shards
  from pg_catalog.generate_series(0, 63) as shard(bucket);

  return pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-sitemap-manifest.v1',
    'shards', v_shards
  );
exception
  when query_canceled then
    raise exception using
      errcode = 'P0001',
      message = 'portal sitemap unavailable';
  when others then
    raise exception using
      errcode = 'P0001',
      message = 'portal sitemap unavailable';
end
$$;

ALTER FUNCTION "api"."portal_sitemap_manifest_v1"() OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "api"."portal_sitemap_manifest_v1"() FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."portal_sitemap_manifest_v1"() TO "anon";

GRANT ALL ON FUNCTION "api"."portal_sitemap_manifest_v1"() TO "authenticated";
