CREATE OR REPLACE FUNCTION "private"."sync_portal_catalog_facet_row_v1"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "row_security" TO 'on'
    AS $$
declare
  v_facts record;
begin
  select facts.*
  into strict v_facts
  from private.portal_catalog_facet_facts_v1(
    new.dataset_kind,
    new.card
  ) as facts;

  insert into private.portal_catalog_facet_rows_v1 (
    dataset_kind,
    id,
    version,
    state_code,
    modified_at,
    facet_access_level,
    facet_geography,
    facet_reference_year,
    facet_process_subtype,
    facet_source,
    facet_contract_version
  ) values (
    new.dataset_kind,
    new.id,
    new.version,
    new.state_code,
    new.modified_at,
    v_facts.facet_access_level,
    v_facts.facet_geography,
    v_facts.facet_reference_year,
    v_facts.facet_process_subtype,
    v_facts.facet_source,
    1
  )
  on conflict (dataset_kind, id, version) do update
  set state_code = excluded.state_code,
      modified_at = excluded.modified_at,
      facet_access_level = excluded.facet_access_level,
      facet_geography = excluded.facet_geography,
      facet_reference_year = excluded.facet_reference_year,
      facet_process_subtype = excluded.facet_process_subtype,
      facet_source = excluded.facet_source,
      facet_contract_version = excluded.facet_contract_version;

  return new;
end
$$;

ALTER FUNCTION "private"."sync_portal_catalog_facet_row_v1"() OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."sync_portal_catalog_facet_row_v1"() FROM PUBLIC;
