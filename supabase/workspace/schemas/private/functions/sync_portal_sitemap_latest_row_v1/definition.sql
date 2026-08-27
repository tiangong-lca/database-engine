CREATE OR REPLACE FUNCTION "private"."sync_portal_sitemap_latest_row_v1"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "row_security" TO 'on'
    AS $$
declare
  v_fallback record;
begin
  if tg_op in ('INSERT', 'UPDATE') then
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
  else
    select facet.version,
      facet.modified_at
    into v_fallback
    from private.portal_catalog_facet_rows_v1 as facet
    where facet.dataset_kind = old.dataset_kind
      and facet.id = old.id
      and facet.state_code in (100, 200)
      and facet.facet_contract_version = 1
    order by facet.version desc,
      facet.modified_at desc,
      facet.state_code desc
    limit 1;

    if found then
      update private.portal_sitemap_latest_rows_v1 as latest
      set version = v_fallback.version,
          modified_at = v_fallback.modified_at
      where latest.dataset_kind = old.dataset_kind
        and latest.id = old.id
        and latest.version = old.version;
    else
      delete from private.portal_sitemap_latest_rows_v1 as latest
      where latest.dataset_kind = old.dataset_kind
        and latest.id = old.id
        and latest.version = old.version;
    end if;
  end if;
  return null;
end
$$;

ALTER FUNCTION "private"."sync_portal_sitemap_latest_row_v1"() OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."sync_portal_sitemap_latest_row_v1"() FROM PUBLIC;
