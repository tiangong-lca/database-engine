CREATE OR REPLACE FUNCTION "private"."sync_portal_sitemap_latest_row_v1"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "row_security" TO 'on'
    AS $$
begin
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      new.dataset_kind || ':'::text || new.id::text,
      539
    )
  );

  insert into private.portal_sitemap_latest_rows_v1 (
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
  on conflict (dataset_kind, id) do update
  set version = excluded.version,
      modified_at = excluded.modified_at,
      shard_no = excluded.shard_no,
      contract_version = excluded.contract_version
  where excluded.version > portal_sitemap_latest_rows_v1.version
     or (
       excluded.version = portal_sitemap_latest_rows_v1.version
       and (
         portal_sitemap_latest_rows_v1.modified_at,
         portal_sitemap_latest_rows_v1.shard_no,
         portal_sitemap_latest_rows_v1.contract_version
       ) is distinct from (
         excluded.modified_at,
         excluded.shard_no,
         excluded.contract_version
       )
     );
  return null;
end
$$;

ALTER FUNCTION "private"."sync_portal_sitemap_latest_row_v1"() OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."sync_portal_sitemap_latest_row_v1"() FROM PUBLIC;
