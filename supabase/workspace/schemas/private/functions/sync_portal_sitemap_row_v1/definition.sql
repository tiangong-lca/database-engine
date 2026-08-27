CREATE OR REPLACE FUNCTION "private"."sync_portal_sitemap_row_v1"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "row_security" TO 'on'
    AS $$
begin
  insert into private.portal_sitemap_rows_v1 (
    dataset_kind,
    id,
    version,
    modified_at,
    shard_no,
    contract_version
  ) values (
    new.dataset_kind,
    new.id,
    new.version,
    new.modified_at,
    (
      pg_catalog.get_byte(
        pg_catalog.decode(
          pg_catalog.md5(
            new.dataset_kind || ':'::text || new.id::text
          ),
          'hex'::text
        ),
        0
      ) / 4
    )::smallint,
    1
  )
  on conflict (dataset_kind, id, version) do update
  set modified_at = excluded.modified_at,
      shard_no = excluded.shard_no,
      contract_version = excluded.contract_version
  where (
    portal_sitemap_rows_v1.modified_at,
    portal_sitemap_rows_v1.shard_no,
    portal_sitemap_rows_v1.contract_version
  ) is distinct from (
    excluded.modified_at,
    excluded.shard_no,
    excluded.contract_version
  );
  return null;
end
$$;

ALTER FUNCTION "private"."sync_portal_sitemap_row_v1"() OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."sync_portal_sitemap_row_v1"() FROM PUBLIC;
