CREATE OR REPLACE FUNCTION "private"."assert_portal_process_keyword_rank_contract_cn1"() RETURNS "void"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "row_security" TO 'on'
    AS $$
declare
  v_expected_digest constant text :=
    '9686f14020c08ebfdd7a7cd0b049d8e0ea8b6acf9710d16fbe0bf3ca45510821';
  v_expected_index constant text :=
    'CREATE INDEX portal_catalog_search_process_exact_rank_v2_gin ON private.portal_catalog_search_rows_v2 USING gin (private.portal_process_rank_name_keys_v1(card), private.portal_process_rank_classification_keys_v1(card)) WHERE (dataset_kind = ''process''::text)';
begin
  perform private.assert_portal_catalog_projection_contract_cn1();
  if private.portal_process_keyword_rank_manifest_sha256_cn1()
       is distinct from v_expected_digest
     or pg_catalog.to_regclass(
       'private.portal_catalog_search_process_exact_rank_v2_gin'
     ) is null
     or (
       select not index_catalog.indisvalid
         or not index_catalog.indisready
         or not index_catalog.indislive
         or index_catalog.indisunique
         or access_method.amname <> 'gin'
         or pg_catalog.pg_get_indexdef(index_relation.oid)
           <> v_expected_index
       from pg_catalog.pg_class as index_relation
       join pg_catalog.pg_index as index_catalog
         on index_catalog.indexrelid = index_relation.oid
       join pg_catalog.pg_am as access_method
         on access_method.oid = index_relation.relam
       where index_relation.oid =
         'private.portal_catalog_search_process_exact_rank_v2_gin'::regclass
     ) is not false then
    raise exception using
      errcode = '55000',
      message = 'Portal Process keyword rank contract drifted';
  end if;
end
$$;

ALTER FUNCTION "private"."assert_portal_process_keyword_rank_contract_cn1"() OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."assert_portal_process_keyword_rank_contract_cn1"() FROM PUBLIC;
