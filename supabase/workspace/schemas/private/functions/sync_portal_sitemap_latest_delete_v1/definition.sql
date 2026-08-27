CREATE OR REPLACE FUNCTION "private"."sync_portal_sitemap_latest_delete_v1"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "row_security" TO 'on'
    AS $$
declare
  v_current_version text;
  v_fallback record;
begin
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      old.dataset_kind || ':'::text || old.id::text,
      539
    )
  );

  select latest.version
  into v_current_version
  from private.portal_sitemap_latest_rows_v1 as latest
  where latest.dataset_kind = old.dataset_kind
    and latest.id = old.id
  for update;

  if not found or v_current_version <> old.version then
    return old;
  end if;

  select facet.version,
    facet.modified_at
  into v_fallback
  from private.portal_catalog_facet_rows_v1 as facet
  where facet.dataset_kind = old.dataset_kind
    and facet.id = old.id
    and facet.version <> old.version
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
  return old;
end
$$;

ALTER FUNCTION "private"."sync_portal_sitemap_latest_delete_v1"() OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."sync_portal_sitemap_latest_delete_v1"() FROM PUBLIC;
