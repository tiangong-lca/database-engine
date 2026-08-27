-- Issue #539: accept the standalone concurrent sitemap shard index only when
-- its exact narrow facet-projection boundary is present and healthy.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '30s';

grant api_internal_executor to postgres;
set role api_internal_executor;
select private.assert_portal_catalog_projection_contract_v1();
select private.assert_portal_catalog_facet_contract_v1();
reset role;
revoke api_internal_executor from postgres;

do $portal_sitemap_shard_index_guard$
declare
  v_index regclass :=
    pg_catalog.to_regclass('private.portal_sitemap_shard_v1_idx');
  v_expected_expression constant text :=
    $$(get_byte(decode(md5(((dataset_kind || ':'::text) || (id)::text)), 'hex'::text), 0) / 4)$$;
begin
  if v_index is null
     or not exists (
       select 1
       from pg_catalog.pg_index as index_catalog
       join pg_catalog.pg_class as index_relation
         on index_relation.oid = index_catalog.indexrelid
       join pg_catalog.pg_namespace as index_namespace
         on index_namespace.oid = index_relation.relnamespace
       join pg_catalog.pg_am as access_method
         on access_method.oid = index_relation.relam
       where index_catalog.indexrelid = v_index
         and index_namespace.nspname = 'private'
         and index_relation.relname = 'portal_sitemap_shard_v1_idx'
         and index_relation.relowner = 'postgres'::regrole
         and access_method.amname = 'btree'
         and index_catalog.indrelid =
           'private.portal_catalog_facet_rows_v1'::regclass
         and index_catalog.indisvalid
         and index_catalog.indisready
         and index_catalog.indislive
         and not index_catalog.indisunique
         and not index_catalog.indisprimary
         and not index_catalog.indisexclusion
         and index_catalog.indimmediate
         and index_catalog.indnkeyatts = 7
         and index_catalog.indnatts = 7
         and index_catalog.indpred is null
         and index_catalog.indexprs is not null
         and pg_catalog.pg_get_expr(
           index_catalog.indexprs,
           index_catalog.indrelid
         ) = v_expected_expression
         and pg_catalog.string_to_array(
           index_catalog.indkey::text,
           ' '
         )::smallint[] = array[
           0::smallint,
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = index_catalog.indrelid
               and attribute.attname = 'dataset_kind'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = index_catalog.indrelid
               and attribute.attname = 'id'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = index_catalog.indrelid
               and attribute.attname = 'version'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = index_catalog.indrelid
               and attribute.attname = 'modified_at'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = index_catalog.indrelid
               and attribute.attname = 'state_code'
           ),
           (
             select attribute.attnum
             from pg_catalog.pg_attribute as attribute
             where attribute.attrelid = index_catalog.indrelid
               and attribute.attname = 'facet_contract_version'
           )
         ]::smallint[]
         and array(
           select operator_class.opcname
           from pg_catalog.unnest(index_catalog.indclass::oid[])
             with ordinality as class_oid(oid, ordinality)
           join pg_catalog.pg_opclass as operator_class
             on operator_class.oid = class_oid.oid
           order by class_oid.ordinality
         ) = array[
           'int4_ops',
           'text_ops',
           'uuid_ops',
           'text_ops',
           'timestamptz_ops',
           'int4_ops',
           'int2_ops'
         ]::name[]
         and index_relation.reloptions is null
     ) then
    raise exception 'Portal sitemap shard index drifted'
      using errcode = '55000';
  end if;
end
$portal_sitemap_shard_index_guard$;

comment on index private.portal_sitemap_shard_v1_idx is
  'Stable 64-bucket membership and latest-version order for bounded Portal sitemap shards; stores no card, document, or private value.';

commit;
