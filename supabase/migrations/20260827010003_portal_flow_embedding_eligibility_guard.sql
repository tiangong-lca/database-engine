begin;

set local lock_timeout = '5s';
set local statement_timeout = '8s';

do $portal_flow_embedding_eligibility_guard$
declare
  v_index regclass := pg_catalog.to_regclass(
    'public.flows_portal_embedding_eligible_v1_idx'
  );
begin
  if v_index is null
     or not exists (
       select 1
       from pg_catalog.pg_index as index_catalog
       join pg_catalog.pg_class as index_relation
         on index_relation.oid = index_catalog.indexrelid
       join pg_catalog.pg_class as source_relation
         on source_relation.oid = index_catalog.indrelid
       join pg_catalog.pg_namespace as source_namespace
         on source_namespace.oid = source_relation.relnamespace
       join pg_catalog.pg_am as access_method
         on access_method.oid = index_relation.relam
       join pg_catalog.pg_attribute as first_key
         on first_key.attrelid = source_relation.oid
        and first_key.attnum = index_catalog.indkey[0]
       join pg_catalog.pg_attribute as second_key
         on second_key.attrelid = source_relation.oid
        and second_key.attnum = index_catalog.indkey[1]
       join pg_catalog.pg_opclass as first_opclass
         on first_opclass.oid = index_catalog.indclass[0]
       join pg_catalog.pg_opclass as second_opclass
         on second_opclass.oid = index_catalog.indclass[1]
       where index_catalog.indexrelid = v_index
         and source_namespace.nspname = 'public'
         and source_relation.relname = 'flows'
         and access_method.amname = 'btree'
         and index_catalog.indisvalid
         and index_catalog.indisready
         and index_catalog.indislive
         and not index_catalog.indisunique
         and index_catalog.indnkeyatts = 2
         and index_catalog.indnatts = 2
         and index_catalog.indexprs is null
         and first_key.attname = 'id'
         and second_key.attname = 'version'
         and first_opclass.opcname = 'uuid_ops'
         and second_opclass.opcname = 'bpchar_ops'
         and pg_catalog.regexp_replace(
           pg_catalog.lower(pg_catalog.pg_get_expr(
             index_catalog.indpred,
             index_catalog.indrelid
           )),
           '[[:space:]]',
           '',
           'g'
         ) = '((state_code=any(array[100,200]))and(embedding_ftisnotnull))'
         and index_relation.relowner = source_relation.relowner
         and index_relation.reloptions is null
         and index_relation.relacl is null
     ) then
    raise exception using
      errcode = '55000',
      message = 'Portal Flow embedding eligibility index drifted';
  end if;
end
$portal_flow_embedding_eligibility_guard$;

commit;
