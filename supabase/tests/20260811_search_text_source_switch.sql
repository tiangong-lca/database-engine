begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, private, api, auth;

select extensions.plan(25);

create temporary table issue_460_expected_indexes (
  table_name text primary key,
  index_name text not null unique,
  old_index_name text not null unique
) on commit drop;

insert into issue_460_expected_indexes (table_name, index_name, old_index_name)
values
  ('contacts', 'contacts_search_text_pgroonga', 'contacts_extracted_md_pgroonga'),
  ('flowproperties', 'flowproperties_search_text_pgroonga', 'flowproperties_extracted_md_pgroonga'),
  ('flows', 'flows_search_text_pgroonga', 'flows_extracted_md_pgroonga'),
  ('lifecyclemodels', 'lifecyclemodels_search_text_pgroonga', 'lifecyclemodels_extracted_md_pgroonga'),
  ('processes', 'processes_search_text_pgroonga', 'processes_extracted_md_pgroonga'),
  ('sources', 'sources_search_text_pgroonga', 'sources_extracted_md_pgroonga'),
  ('unitgroups', 'unitgroups_search_text_pgroonga', 'unitgroups_extracted_md_pgroonga');

select extensions.is(
  (
    select count(*)
    from issue_460_expected_indexes expected
    join pg_class index_relation
      on index_relation.relname = expected.index_name
    join pg_namespace index_namespace
      on index_namespace.oid = index_relation.relnamespace
    join pg_index index_catalog
      on index_catalog.indexrelid = index_relation.oid
     and index_catalog.indrelid = format('public.%I', expected.table_name)::regclass
    join pg_am access_method
      on access_method.oid = index_relation.relam
    where index_namespace.nspname = 'public'
      and access_method.amname = 'pgroonga'
      and index_catalog.indisvalid
      and index_catalog.indisready
      and index_catalog.indislive
  ),
  7::bigint,
  'all seven search_text PGroonga indexes are valid, ready, and live'
);

select extensions.is(
  (
    select count(*)
    from issue_460_expected_indexes expected
    join pg_class index_relation
      on index_relation.relname = expected.index_name
    join pg_namespace index_namespace
      on index_namespace.oid = index_relation.relnamespace
    join pg_index index_catalog
      on index_catalog.indexrelid = index_relation.oid
     and index_catalog.indrelid = format('public.%I', expected.table_name)::regclass
    join pg_attribute indexed_attribute
      on indexed_attribute.attrelid = index_catalog.indrelid
     and indexed_attribute.attnum = index_catalog.indkey[0]
    join pg_opclass operator_class
      on operator_class.oid = index_catalog.indclass[0]
    where index_namespace.nspname = 'public'
      and index_catalog.indnatts = 1
      and index_catalog.indpred is null
      and index_catalog.indexprs is null
      and indexed_attribute.attname = 'search_text'
      and operator_class.opcname = 'pgroonga_text_array_full_text_search_ops_v2'
  ),
  7::bigint,
  'each new index is a single search_text column with the explicit array opclass'
);

select extensions.is(
  (
    select count(*)
    from issue_460_expected_indexes expected
    join pg_class index_relation on index_relation.relname = expected.index_name
    join pg_namespace index_namespace on index_namespace.oid = index_relation.relnamespace
    where index_namespace.nspname = 'public'
      and 'tokenizer=TokenBigram' = any(coalesce(index_relation.reloptions, '{}'::text[]))
      and 'normalizer=NormalizerAuto' = any(coalesce(index_relation.reloptions, '{}'::text[]))
  ),
  7::bigint,
  'all new indexes use TokenBigram and NormalizerAuto'
);

select extensions.is(
  (
    select count(*)
    from issue_460_expected_indexes expected
    join pg_class index_relation on index_relation.relname = expected.old_index_name
    join pg_namespace index_namespace on index_namespace.oid = index_relation.relnamespace
    where index_namespace.nspname = 'public'
  ),
  7::bigint,
  'the seven extracted_md indexes remain in place for rollback and staged release'
);

select extensions.ok(
  (private.search_text_cutover_gate_v1()->>'fresh_database')::boolean
    and (private.search_text_cutover_gate_v1()->>'ready')::boolean
    and (private.search_text_cutover_gate_v1()->>'total_rows')::bigint = 0,
  'the cutover gate allows an empty new or reset database'
);

select extensions.ok(
  not has_function_privilege(
    'anon',
    'private.search_text_cutover_gate_v1()'::regprocedure,
    'execute'
  )
  and not has_function_privilege(
    'authenticated',
    'private.search_text_cutover_gate_v1()'::regprocedure,
    'execute'
  )
  and not has_function_privilege(
    'service_role',
    'private.search_text_cutover_gate_v1()'::regprocedure,
    'execute'
  ),
  'the cutover gate is private and grant-free rather than a new RPC'
);

alter table public.contacts disable trigger user;
insert into public.contacts (id, version, json, json_ordered)
values (
  '46000000-0000-4000-8000-000000000001'::uuid,
  '01.00.000',
  '{}'::jsonb,
  '{}'::json
);
alter table public.contacts enable trigger user;

select extensions.ok(
  not (private.search_text_cutover_gate_v1()->>'ready')::boolean,
  'the cutover gate blocks an existing environment with incomplete coverage'
);

select extensions.is(
  (private.search_text_cutover_gate_v1()->>'missing_rows')::bigint,
  1::bigint,
  'the cutover gate reports the incomplete search_text row'
);

delete from public.contacts
where id = '46000000-0000-4000-8000-000000000001'::uuid;

create temporary table issue_460_core_sources (
  routine_signature text primary key
) on commit drop;

insert into issue_460_core_sources (routine_signature)
values
  ('api._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)'),
  ('private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'),
  ('private.search_processes_latest_v2_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[],boolean)'),
  ('private.search_lifecyclemodels_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'),
  ('private.hybrid_search_simple_dataset_v2(regclass,text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)');

select extensions.is(
  (
    select count(*)
    from issue_460_core_sources expected
    where pg_get_functiondef(expected.routine_signature::regprocedure) like '%search_text%'
  ),
  5::bigint,
  'all direct lexical candidates and the foundation hybrid lexical branch use search_text'
);

select extensions.is(
  (
    select count(*)
    from issue_460_core_sources expected
    where pg_get_functiondef(expected.routine_signature::regprocedure) like '%extracted_md%'
  ),
  0::bigint,
  'no direct lexical candidate or foundation hybrid branch retains extracted_md'
);

select extensions.is(
  (
    select count(*)
    from (values
      ('private.hybrid_search_flows_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])', 'api.search_flows'),
      ('private.hybrid_search_processes_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])', 'api.search_processes'),
      ('private.hybrid_search_lifecyclemodels_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])', 'api.search_lifecyclemodels')
    ) expected(routine_signature, lexical_facade)
    where pg_get_functiondef(expected.routine_signature::regprocedure) like '%' || expected.lexical_facade || '%'
  ),
  3::bigint,
  'specialized hybrid branches retain their existing lexical facade path to search_text'
);

create temporary table issue_460_wrapper_contract (
  wrapper_signature text primary key,
  implementation_token text not null
) on commit drop;

insert into issue_460_wrapper_contract (wrapper_signature, implementation_token)
values
  ('api.search_contacts(text,jsonb,integer,integer,text,text,uuid,integer)', 'api._search_simple_dataset_latest'),
  ('api.search_contacts_latest(text,jsonb,bigint,bigint,text,text,uuid,integer)', 'api._search_simple_dataset_latest'),
  ('api.search_flowproperties(text,jsonb,integer,integer,text,text,uuid,integer)', 'api._search_simple_dataset_latest'),
  ('api.search_flowproperties_latest(text,jsonb,bigint,bigint,text,text,uuid,integer)', 'api._search_simple_dataset_latest'),
  ('api.search_flows(text,jsonb,integer,integer,text,text,uuid,integer,text[])', 'private.search_flows_latest_impl'),
  ('api.search_flows_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text[])', 'private.search_flows_latest_impl'),
  ('api.search_lifecyclemodels(text,jsonb,integer,integer,text,text,uuid,integer,text[])', 'private.search_lifecyclemodels_latest_impl'),
  ('api.search_lifecyclemodels_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text[])', 'private.search_lifecyclemodels_latest_impl'),
  ('api.search_processes(text,jsonb,integer,integer,text,text,uuid,integer,text,text[],boolean)', 'private.search_processes_latest_v2_impl'),
  ('api.search_processes_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text,text[])', 'private.search_processes_latest_v2_impl'),
  ('api.search_processes_latest_v2(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text,text[],boolean)', 'private.search_processes_latest_v2_impl'),
  ('api.search_sources(text,jsonb,integer,integer,text,text,uuid,integer)', 'api._search_simple_dataset_latest'),
  ('api.search_sources_latest(text,jsonb,bigint,bigint,text,text,uuid,integer)', 'api._search_simple_dataset_latest'),
  ('api.search_unitgroups(text,jsonb,integer,integer,text,text,uuid,integer)', 'api._search_simple_dataset_latest'),
  ('api.search_unitgroups_latest(text,jsonb,bigint,bigint,text,text,uuid,integer)', 'api._search_simple_dataset_latest'),
  ('api.hybrid_search_contacts(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)', 'private.hybrid_search_simple_dataset_v2'),
  ('api.hybrid_search_contacts_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)', 'private.hybrid_search_simple_dataset_v2'),
  ('api.hybrid_search_flowproperties(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)', 'private.hybrid_search_simple_dataset_v2'),
  ('api.hybrid_search_flowproperties_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)', 'private.hybrid_search_simple_dataset_v2'),
  ('api.hybrid_search_flows(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])', 'private.hybrid_search_flows_v2_impl'),
  ('api.hybrid_search_flows_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])', 'private.hybrid_search_flows_v2_impl'),
  ('api.hybrid_search_lifecyclemodels(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])', 'private.hybrid_search_lifecyclemodels_v2_impl'),
  ('api.hybrid_search_lifecyclemodels_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])', 'private.hybrid_search_lifecyclemodels_v2_impl'),
  ('api.hybrid_search_processes(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])', 'private.hybrid_search_processes_v2_impl'),
  ('api.hybrid_search_processes_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])', 'private.hybrid_search_processes_v2_impl'),
  ('api.hybrid_search_sources(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)', 'private.hybrid_search_simple_dataset_v2'),
  ('api.hybrid_search_sources_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)', 'private.hybrid_search_simple_dataset_v2'),
  ('api.hybrid_search_unitgroups(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)', 'private.hybrid_search_simple_dataset_v2'),
  ('api.hybrid_search_unitgroups_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)', 'private.hybrid_search_simple_dataset_v2');

select extensions.is(
  (
    select count(*)
    from issue_460_wrapper_contract expected
    where pg_get_functiondef(expected.wrapper_signature::regprocedure)
      like '%' || expected.implementation_token || '%'
  ),
  29::bigint,
  'all formal, latest, and v2 wrappers retain their existing private implementation mapping'
);

select extensions.is(
  (
    select count(*)
    from issue_460_wrapper_contract expected
    where pg_get_functiondef(expected.wrapper_signature::regprocedure) like '%extracted_md%'
  ),
  0::bigint,
  'formal and compatibility wrappers expose no extracted_md lexical source'
);

select extensions.ok(
  not exists (
    select 1
    from issue_460_core_sources expected
    where pg_get_functiondef(expected.routine_signature::regprocedure)
      ~ 'search_text[[:space:]]*\['
  ),
  'direct candidates do not use array position or element order as a ranking weight'
);

create temporary table issue_460_array_probe (
  id integer primary key,
  search_text text[]
) on commit drop;

create index issue_460_array_probe_search_text_pgroonga
  on issue_460_array_probe using pgroonga (
    search_text pgroonga_text_array_full_text_search_ops_v2
  ) with (tokenizer='TokenBigram', normalizer='NormalizerAuto');

insert into issue_460_array_probe (id, search_text)
values
  (1, array['甲乙', '丙丁']),
  (2, array['乙丙']),
  (3, array[E'甲乙\n丙丁']),
  (4, array['电力生产', 'Stromerzeugung', 'uuid-460-0001']);

select extensions.is(
  (select count(*) from issue_460_array_probe where search_text &@~ '乙丙'),
  1::bigint,
  'array elements do not bridge the 乙/丙 boundary'
);

select extensions.is(
  (select id from issue_460_array_probe where search_text &@~ '乙丙'),
  2,
  'a true in-element lexical match remains searchable'
);

select extensions.is(
  (select count(*) from issue_460_array_probe where id = 1 and search_text &@~ '甲乙'),
  1::bigint,
  'the first array element is searchable without special delimiters'
);

select extensions.is(
  (select count(*) from issue_460_array_probe where id = 1 and search_text &@~ '丙丁'),
  1::bigint,
  'the second array element is searchable without positional ranking'
);

select extensions.ok(
  (select search_text[1] = E'甲乙\n丙丁' from issue_460_array_probe where id = 3),
  'the newline text counterexample remains one preserved array element'
);

select extensions.is(
  (select count(*) from issue_460_array_probe where id = 3 and search_text &@~ '乙丙'),
  0::bigint,
  'a newline inside one element is not treated as a custom cross-element delimiter'
);

select extensions.is(
  (select count(*) from issue_460_array_probe where id = 4 and search_text &@~ '电力生产'),
  1::bigint,
  'CJK multilingual search_text content is searchable'
);

select extensions.is(
  (select count(*) from issue_460_array_probe where id = 4 and search_text &@~ 'Stromerzeugung'),
  1::bigint,
  'Latin multilingual search_text content is searchable'
);

select extensions.is(
  (select count(*) from issue_460_array_probe where id = 4 and search_text &@~ 'uuid-460-0001'),
  1::bigint,
  'UUID-like search_text content is searchable'
);

create temporary table issue_460_explain_probe (
  id integer primary key,
  search_text text[]
) on commit drop;

create index issue_460_explain_probe_search_text_pgroonga
  on issue_460_explain_probe using pgroonga (
    search_text pgroonga_text_array_full_text_search_ops_v2
  ) with (tokenizer='TokenBigram', normalizer='NormalizerAuto');

insert into issue_460_explain_probe (id, search_text)
select id, array[
  case when id = 460 then 'needle460' else format('noise460_%s', id) end
]
from generate_series(1, 20000) as rows(id);

analyze issue_460_explain_probe;

create or replace function pg_temp.issue_460_explain_plan()
returns text
language plpgsql
as $function$
declare
  plan_line text;
  plan_text text := '';
begin
  for plan_line in execute $$
    explain (analyze, buffers, format text)
    select id
    from issue_460_explain_probe
    where search_text &@~ 'needle460'
  $$ loop
    plan_text := plan_text || E'\n' || plan_line;
  end loop;
  return plan_text;
end
$function$;

select extensions.ok(
  pg_temp.issue_460_explain_plan() like '%issue_460_explain_probe_search_text_pgroonga%',
  'actual EXPLAIN on a selective dataset uses the new PGroonga index'
);

select extensions.is(
  (select count(*) from issue_460_explain_probe where search_text &@~ 'needle460'),
  1::bigint,
  'the selective indexed query executes with the expected single result'
);

select * from extensions.finish();

rollback;
