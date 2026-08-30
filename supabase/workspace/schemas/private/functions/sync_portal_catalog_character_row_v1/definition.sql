CREATE OR REPLACE FUNCTION "private"."sync_portal_catalog_character_row_v1"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "row_security" TO 'on'
    AS $$
begin
  insert into private.portal_catalog_character_rows_v1 (
    dataset_kind,
    id,
    version,
    state_code,
    modified_at,
    document_characters,
    name_characters,
    name_exact_characters,
    classification_characters,
    classification_exact_characters,
    character_contract_version
  ) values (
    new.dataset_kind,
    new.id,
    new.version,
    new.state_code,
    new.modified_at,
    private.portal_catalog_character_set_v1(new.document),
    private.portal_catalog_character_field_set_v1(
      new.card -> 'names', 'value', false
    ),
    private.portal_catalog_character_field_set_v1(
      new.card -> 'names', 'value', true
    ),
    private.portal_catalog_character_field_set_v1(
      new.card -> 'classifications', 'code', false
    ),
    private.portal_catalog_character_field_set_v1(
      new.card -> 'classifications', 'code', true
    ),
    1
  )
  on conflict (dataset_kind, id, version) do update
  set state_code = excluded.state_code,
      modified_at = excluded.modified_at,
      document_characters = excluded.document_characters,
      name_characters = excluded.name_characters,
      name_exact_characters = excluded.name_exact_characters,
      classification_characters = excluded.classification_characters,
      classification_exact_characters =
        excluded.classification_exact_characters,
      character_contract_version = excluded.character_contract_version;
  return new;
end
$$;

ALTER FUNCTION "private"."sync_portal_catalog_character_row_v1"() OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."sync_portal_catalog_character_row_v1"() FROM PUBLIC;
