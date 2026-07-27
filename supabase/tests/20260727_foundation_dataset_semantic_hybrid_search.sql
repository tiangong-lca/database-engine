begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(46);

create or replace function pg_temp.trigger_update_columns(
  p_table regclass,
  p_trigger_name text
) returns text
language sql
stable
as $$
  select coalesce(
    string_agg(attribute.attname, ', ' order by attribute.attnum),
    '<all update columns>'
  )
  from pg_trigger trigger_catalog
  left join lateral unnest(trigger_catalog.tgattr::smallint[]) trigger_column(attnum) on true
  left join pg_attribute attribute
    on attribute.attrelid = trigger_catalog.tgrelid
   and attribute.attnum = trigger_column.attnum
  where trigger_catalog.tgrelid = p_table
    and trigger_catalog.tgname = p_trigger_name
    and not trigger_catalog.tgisinternal
  group by trigger_catalog.oid;
$$;

select is(
  (
    select count(*)::integer
    from information_schema.columns
    where table_schema = 'public'
      and table_name in ('contacts', 'flowproperties', 'sources', 'unitgroups')
      and column_name = 'extracted_md'
      and data_type = 'text'
  ),
  4,
  'all four foundation datasets have extracted_md'
);

select is(
  (
    select count(*)::integer
    from pg_attribute attribute
    join pg_class relation on relation.oid = attribute.attrelid
    join pg_namespace namespace on namespace.oid = relation.relnamespace
    join pg_type column_type on column_type.oid = attribute.atttypid
    where namespace.nspname = 'public'
      and relation.relname in ('contacts', 'flowproperties', 'sources', 'unitgroups')
      and attribute.attname = 'embedding_ft'
      and not attribute.attisdropped
      and column_type.typname = 'vector'
      and attribute.atttypmod = 1024
  ),
  4,
  'all four foundation datasets use 1024-dimensional embedding_ft vectors'
);

select is(
  (
    select count(*)::integer
    from information_schema.columns
    where table_schema = 'public'
      and table_name in ('contacts', 'flowproperties', 'sources', 'unitgroups')
      and column_name = 'embedding_ft_at'
      and data_type = 'timestamp with time zone'
  ),
  4,
  'all four foundation datasets timestamp their current embeddings'
);

select is(
  (
    select count(*)::integer
    from information_schema.columns
    where table_schema = 'public'
      and table_name in ('contacts', 'flowproperties', 'sources', 'unitgroups')
      and column_name = 'embedding'
  ),
  0,
  'the unused 1536-dimensional embedding columns are retired'
);

select is(
  (
    select count(*)::integer
    from pg_class relation
    join pg_namespace namespace on namespace.oid = relation.relnamespace
    where namespace.nspname = 'public'
      and relation.relname in (
        'contacts_json_ordered_vector',
        'flowproperties_json_ordered_vector',
        'sources_json_ordered_vector',
        'unitgroups_json_ordered_vector'
      )
  ),
  0,
  'the four empty legacy HNSW indexes are retired'
);

select is(
  (
    select count(*)::integer
    from pg_class index_relation
    join pg_namespace namespace on namespace.oid = index_relation.relnamespace
    where namespace.nspname = 'public'
      and index_relation.relname in (
        'contacts_embedding_ft_hnsw_idx',
        'flowproperties_embedding_ft_hnsw_idx',
        'sources_embedding_ft_hnsw_idx',
        'unitgroups_embedding_ft_hnsw_idx'
      )
  ),
  4,
  'all four current embedding columns have HNSW indexes'
);

select is(
  (
    select count(*)::integer
    from pg_index index_catalog
    join pg_class index_relation on index_relation.oid = index_catalog.indexrelid
    join pg_am access_method on access_method.oid = index_relation.relam
    where index_relation.relname in (
        'contacts_embedding_ft_hnsw_idx',
        'flowproperties_embedding_ft_hnsw_idx',
        'sources_embedding_ft_hnsw_idx',
        'unitgroups_embedding_ft_hnsw_idx'
      )
      and access_method.amname = 'hnsw'
      and index_catalog.indisvalid
      and index_catalog.indisready
      and index_catalog.indislive
  ),
  4,
  'the four HNSW indexes are valid, ready, and live'
);

select is(
  (
    select count(*)::integer
    from pg_proc routine
    join pg_namespace namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'public'
      and routine.proname in (
        'contacts_embedding_ft_input',
        'flowproperties_embedding_ft_input',
        'sources_embedding_ft_input',
        'unitgroups_embedding_ft_input'
      )
  ),
  4,
  'all four embedding workers have an extracted_md input function'
);

select is(
  (
    select count(*)::integer
    from util.embedding_queue_policy policy
    where policy.scope_schema = 'public'
      and policy.scope_table in ('contacts', 'flowproperties', 'sources', 'unitgroups')
      and policy.scope_edge_function = 'embedding_ft'
      and policy.scope_embedding_column = 'embedding_ft'
      and policy.mode = 'normal'
      and policy.max_in_flight = 2
  ),
  4,
  'all four embedding streams have conservative scoped backpressure'
);

select is(
  (
    select count(*)::integer
    from pg_trigger trigger_catalog
    where trigger_catalog.tgrelid in (
        'public.contacts'::regclass,
        'public.flowproperties'::regclass,
        'public.sources'::regclass,
        'public.unitgroups'::regclass
      )
      and trigger_catalog.tgname like '%_dataset_extraction_trigger_%'
      and not trigger_catalog.tgisinternal
  ),
  8,
  'each foundation dataset queues extraction on insert and authored JSON changes'
);

select is(
  (
    select count(*)::integer
    from pg_trigger trigger_catalog
    where trigger_catalog.tgrelid in (
        'public.contacts'::regclass,
        'public.flowproperties'::regclass,
        'public.sources'::regclass,
        'public.unitgroups'::regclass
      )
      and trigger_catalog.tgname like '%_embedding_ft_on_extract_md_update'
      and not trigger_catalog.tgisinternal
  ),
  4,
  'each foundation dataset queues embedding only after Markdown changes'
);

select is(
  (
    select count(*)::integer
    from (values
      ('public.contacts'::regclass, 'contacts_json_sync_trigger'),
      ('public.flowproperties'::regclass, 'flowproperties_json_sync_trigger'),
      ('public.sources'::regclass, 'sources_json_sync_trigger'),
      ('public.unitgroups'::regclass, 'unitgroups_json_sync_trigger')
    ) expected(table_oid, trigger_name)
    where pg_temp.trigger_update_columns(expected.table_oid, expected.trigger_name) = 'json_ordered'
  ),
  4,
  'foundation JSON sync triggers run only for json_ordered updates'
);

select is(
  (
    select count(*)::integer
    from (values
      ('public.contacts'::regclass, 'contacts_set_modified_at_trigger'),
      ('public.flowproperties'::regclass, 'flowproperties_set_modified_at_trigger'),
      ('public.sources'::regclass, 'sources_set_modified_at_trigger'),
      ('public.unitgroups'::regclass, 'unitgroups_set_modified_at_trigger')
    ) expected(table_oid, trigger_name)
    where pg_temp.trigger_update_columns(expected.table_oid, expected.trigger_name) <> '<all update columns>'
  ),
  4,
  'foundation modified_at triggers are column-scoped'
);

select is(
  (
    select count(*)::integer
    from (values
      ('public.contacts'::regclass, 'contacts_set_modified_at_trigger'),
      ('public.flowproperties'::regclass, 'flowproperties_set_modified_at_trigger'),
      ('public.sources'::regclass, 'sources_set_modified_at_trigger'),
      ('public.unitgroups'::regclass, 'unitgroups_set_modified_at_trigger')
    ) expected(table_oid, trigger_name)
    where pg_temp.trigger_update_columns(expected.table_oid, expected.trigger_name) not like '%extracted_text%'
      and pg_temp.trigger_update_columns(expected.table_oid, expected.trigger_name) not like '%extracted_md%'
      and pg_temp.trigger_update_columns(expected.table_oid, expected.trigger_name) not like '%embedding_ft%'
      and pg_temp.trigger_update_columns(expected.table_oid, expected.trigger_name) not like '%embedding_ft_at%'
  ),
  4,
  'foundation modified_at triggers exclude all derived search columns'
);

select is(
  (
    select count(*)::integer
    from (values
      ('public.contacts'::regclass, 'contacts_set_modified_at_trigger'),
      ('public.flowproperties'::regclass, 'flowproperties_set_modified_at_trigger'),
      ('public.sources'::regclass, 'sources_set_modified_at_trigger'),
      ('public.unitgroups'::regclass, 'unitgroups_set_modified_at_trigger')
    ) expected(table_oid, trigger_name)
    where pg_temp.trigger_update_columns(expected.table_oid, expected.trigger_name) like '%json%'
      and pg_temp.trigger_update_columns(expected.table_oid, expected.trigger_name) like '%state_code%'
      and pg_temp.trigger_update_columns(expected.table_oid, expected.trigger_name) like '%reviews%'
  ),
  4,
  'foundation modified_at triggers retain authored business columns'
);

select ok(
  strpos(pg_get_functiondef('util.queue_dataset_extraction_jobs()'::regprocedure), $needle$when 'contacts' then 'contact'$needle$) > 0
    and strpos(pg_get_functiondef('util.queue_dataset_extraction_jobs()'::regprocedure), $needle$when 'flowproperties' then 'flowproperty'$needle$) > 0
    and strpos(pg_get_functiondef('util.queue_dataset_extraction_jobs()'::regprocedure), $needle$when 'sources' then 'source'$needle$) > 0
    and strpos(pg_get_functiondef('util.queue_dataset_extraction_jobs()'::regprocedure), $needle$when 'unitgroups' then 'unitgroup'$needle$) > 0,
  'the compact extraction queue maps every new entity kind explicitly'
);

select ok(
  (
    select routine.prosecdef
    from pg_proc routine
    where routine.oid = 'private.semantic_simple_dataset_candidates(regclass,text,text,double precision,integer,text,integer,uuid)'::regprocedure
  ),
  'the allow-listed semantic candidate helper is security definer'
);

select ok(
  (
    select routine.proconfig @> array[
      'plan_cache_mode=force_custom_plan',
      'hnsw.iterative_scan=strict_order'
    ]
    from pg_proc routine
    where routine.oid = 'private.semantic_simple_dataset_candidates(regclass,text,text,double precision,integer,text,integer,uuid)'::regprocedure
  ),
  'semantic candidates use custom plans and strict iterative HNSW scans'
);

select is(
  (
    select count(*)::integer
    from pg_proc routine
    join pg_namespace namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'public'
      and routine.proname in (
        'semantic_search_contacts_v1',
        'semantic_search_flowproperties_v1',
        'semantic_search_sources_v1',
        'semantic_search_unitgroups_v1'
      )
  ),
  4,
  'four public Semantic Search RPCs exist'
);

select is(
  (
    select count(*)::integer
    from pg_proc routine
    join pg_namespace namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'public'
      and routine.proname in (
        'hybrid_search_contacts',
        'hybrid_search_flowproperties',
        'hybrid_search_sources',
        'hybrid_search_unitgroups'
      )
  ),
  4,
  'four public Hybrid Search RPCs exist'
);

select ok(
  not has_function_privilege(
    'authenticated',
    'public.cmd_dataset_semantic_backfill(text,integer,uuid,text,boolean)',
    'execute'
  ),
  'historical semantic backfill is not callable by authenticated users'
);

select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);
select set_config('request.headers', '{}', true);

select is(
  public.cmd_dataset_semantic_backfill('sources', 10, null, null, false)->>'code',
  'SERVICE_ROLE_REQUIRED',
  'historical semantic backfill rejects non-service request context'
);

insert into auth.users (
  instance_id,
  id,
  aud,
  role,
  email,
  encrypted_password,
  email_confirmed_at,
  raw_app_meta_data,
  raw_user_meta_data,
  created_at,
  updated_at,
  is_sso_user,
  is_anonymous
)
values
  (
    '00000000-0000-0000-0000-000000000000',
    'a1000000-0000-0000-0000-000000000297',
    'authenticated',
    'authenticated',
    'foundation-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"a1000000-0000-0000-0000-000000000297","email":"foundation-owner@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    'b2000000-0000-0000-0000-000000000297',
    'authenticated',
    'authenticated',
    'foundation-outsider@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"b2000000-0000-0000-0000-000000000297","email":"foundation-outsider@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.users (id, raw_user_meta_data, contact)
values
  ('a1000000-0000-0000-0000-000000000297', '{"email":"foundation-owner@example.com"}'::jsonb, null),
  ('b2000000-0000-0000-0000-000000000297', '{"email":"foundation-outsider@example.com"}'::jsonb, null);

insert into public.teams (id, json, rank, is_public)
values ('c3000000-0000-0000-0000-000000000297', '{"name":"Foundation Search Team"}'::jsonb, 1, false);

insert into public.roles (user_id, team_id, role)
values ('a1000000-0000-0000-0000-000000000297', 'c3000000-0000-0000-0000-000000000297', 'owner');

with test_vector(value) as (
  select ('[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']')::vector(1024)
)
insert into public.contacts (
  id, version, json, json_ordered, user_id, state_code, team_id,
  extracted_text, extracted_md, embedding_ft, embedding_ft_at, modified_at
)
select * from (
  select 'ca000000-0000-0000-0000-000000000101'::uuid, '01.00.000'::character(9), '{"name":"public-contact-token"}'::jsonb, null::json, null::uuid, 100, null::uuid, 'public-contact-token'::text, '# Contact public-contact-token'::text, test_vector.value, now(), now() from test_vector
  union all
  select 'ca000000-0000-0000-0000-000000000201'::uuid, '01.00.000'::character(9), '{"name":"owner-contact-token"}'::jsonb, null::json, 'a1000000-0000-0000-0000-000000000297'::uuid, 0, null::uuid, 'owner-contact-token'::text, '# Contact owner-contact-token'::text, test_vector.value, now(), now() from test_vector
  union all
  select 'ca000000-0000-0000-0000-000000000202'::uuid, '01.00.000'::character(9), '{"name":"team-contact-token"}'::jsonb, null::json, 'b2000000-0000-0000-0000-000000000297'::uuid, 0, 'c3000000-0000-0000-0000-000000000297'::uuid, 'team-contact-token'::text, '# Contact team-contact-token'::text, test_vector.value, now(), now() from test_vector
  union all
  select 'ca000000-0000-0000-0000-000000000203'::uuid, '01.00.000'::character(9), '{"name":"outsider-contact-token"}'::jsonb, null::json, 'b2000000-0000-0000-0000-000000000297'::uuid, 0, null::uuid, 'outsider-contact-token'::text, '# Contact outsider-contact-token'::text, test_vector.value, now(), now() from test_vector
) rows;

with test_vector(value) as (
  select ('[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']')::vector(1024)
)
insert into public.flowproperties (
  id, version, json, json_ordered, user_id, state_code, team_id,
  extracted_text, extracted_md, embedding_ft, embedding_ft_at, modified_at
)
select 'fb000000-0000-0000-0000-000000000101', '01.00.000', '{"name":"public-flowproperty-token"}', null, null, 100, null,
       'public-flowproperty-token', '# Flow property public-flowproperty-token', test_vector.value, now(), now()
from test_vector;

with test_vector(value) as (
  select ('[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']')::vector(1024)
)
insert into public.sources (
  id, version, json, json_ordered, user_id, state_code, team_id,
  extracted_text, extracted_md, embedding_ft, embedding_ft_at, modified_at
)
select '5a000000-0000-0000-0000-000000000101', '01.00.000', '{"name":"public-source-token"}', null, null, 100, null,
       'public-source-token', '# Source public-source-token', test_vector.value, now(), now()
from test_vector;

with test_vector(value) as (
  select ('[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']')::vector(1024)
)
insert into public.unitgroups (
  id, version, json, json_ordered, user_id, state_code, team_id,
  extracted_text, extracted_md, embedding_ft, embedding_ft_at, modified_at
)
select 'a1000000-0000-0000-0000-000000000101', '01.00.000', '{"name":"public-unitgroup-token"}', null, null, 100, null,
       'public-unitgroup-token', '# Unit group public-unitgroup-token', test_vector.value, now(), now()
from test_vector;

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select id::text from public.semantic_search_contacts_v1((select value from query_vector), '{}', 0.5, 20, 'tg') limit 1),
  'ca000000-0000-0000-0000-000000000101',
  'contact Semantic Search returns the visible public row'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select id::text from public.semantic_search_flowproperties_v1((select value from query_vector), '{}', 0.5, 20, 'tg') limit 1),
  'fb000000-0000-0000-0000-000000000101',
  'flow-property Semantic Search returns the visible public row'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select id::text from public.semantic_search_sources_v1((select value from query_vector), '{}', 0.5, 20, 'tg') limit 1),
  '5a000000-0000-0000-0000-000000000101',
  'source Semantic Search returns the visible public row'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select id::text from public.semantic_search_unitgroups_v1((select value from query_vector), '{}', 0.5, 20, 'tg') limit 1),
  'a1000000-0000-0000-0000-000000000101',
  'unit-group Semantic Search returns the visible public row'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select id::text from public.hybrid_search_contacts('public-contact-token', (select value from query_vector), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'tg', 10, 1, array['public-contact-token']) limit 1),
  'ca000000-0000-0000-0000-000000000101',
  'contact Hybrid Search fuses text and semantic candidates'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select id::text from public.hybrid_search_flowproperties('public-flowproperty-token', (select value from query_vector), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'tg', 10, 1, array['public-flowproperty-token']) limit 1),
  'fb000000-0000-0000-0000-000000000101',
  'flow-property Hybrid Search fuses text and semantic candidates'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select id::text from public.hybrid_search_sources('public-source-token', (select value from query_vector), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'tg', 10, 1, array['public-source-token']) limit 1),
  '5a000000-0000-0000-0000-000000000101',
  'source Hybrid Search fuses text and semantic candidates'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select id::text from public.hybrid_search_unitgroups('public-unitgroup-token', (select value from query_vector), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'tg', 10, 1, array['public-unitgroup-token']) limit 1),
  'a1000000-0000-0000-0000-000000000101',
  'unit-group Hybrid Search fuses text and semantic candidates'
);

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claim.sub', 'a1000000-0000-0000-0000-000000000297', true);
select set_config('request.jwt.claims', '{"role":"authenticated","sub":"a1000000-0000-0000-0000-000000000297"}', true);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select array_agg(id::text order by id) from public.semantic_search_contacts_v1((select value from query_vector), '{}', 0.5, 20, 'my')),
  array['ca000000-0000-0000-0000-000000000201']::text[],
  'my Semantic Search returns only rows owned by the authenticated actor'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select array_agg(id::text order by id) from public.semantic_search_contacts_v1((select value from query_vector), '{}', 0.5, 20, 'te', null, 'c3000000-0000-0000-0000-000000000297')),
  array['ca000000-0000-0000-0000-000000000202']::text[],
  'team Semantic Search returns only rows from the selected team the actor belongs to'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select id::text from public.hybrid_search_contacts('outsider-contact-token', (select value from query_vector), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'my', 10, 1, array['outsider-contact-token']) limit 1),
  'ca000000-0000-0000-0000-000000000201',
  'my Hybrid Search does not leak an outsider text or semantic candidate'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select count(*)::integer from public.hybrid_search_contacts('owner-contact-token', (select value from query_vector), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'my', 10, 1, array['owner-contact-token'], 1, null)),
  0,
  'my Hybrid Search preserves the explicit state filter'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select id::text from public.hybrid_search_contacts('team-contact-token', (select value from query_vector), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'te', 10, 1, array['team-contact-token'], 0, 'c3000000-0000-0000-0000-000000000297') limit 1),
  'ca000000-0000-0000-0000-000000000202',
  'team Hybrid Search preserves the selected team and state filters'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select count(*)::integer from public.hybrid_search_contacts('team-contact-token', (select value from query_vector), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'te', 10, 1, array['team-contact-token'], 0, null)),
  0,
  'team Hybrid Search fails closed without an explicit team filter'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select count(*)::integer from public.hybrid_search_contacts('public-contact-token', (select value from query_vector), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'tg', 10, 1, array['public-contact-token'], null, 'c3000000-0000-0000-0000-000000000297')),
  0,
  'public Hybrid Search preserves an explicit team scope'
);

reset role;
delete from pgmq.q_dataset_extraction_jobs;
delete from pgmq.q_embedding_jobs;

insert into public.unitgroups (
  id, version, json, json_ordered, user_id, state_code, modified_at
)
values (
  'a1000000-0000-0000-0000-000000000301',
  '01.00.000',
  '{"name":"queued-unitgroup-v1"}',
  null,
  null,
  0,
  now()
);

select is(
  (
    select count(*)::integer
    from pgmq.q_dataset_extraction_jobs queued
    where queued.message @> jsonb_build_object(
      'schema', 'public',
      'table', 'unitgroups',
      'id', 'a1000000-0000-0000-0000-000000000301',
      'version', '01.00.000',
      'entity_kind', 'unitgroup',
      'extraction_kind', 'extracted_md'
    )
  ),
  1,
  'a new unit group enqueues one compact Markdown extraction job'
);

update public.unitgroups
set json = '{"name":"queued-unitgroup-v2"}'::jsonb
where id = 'a1000000-0000-0000-0000-000000000301'
  and version = '01.00.000';

select is(
  (
    select count(*)::integer
    from pgmq.q_dataset_extraction_jobs queued
    where queued.message->>'table' = 'unitgroups'
      and queued.message->>'id' = 'a1000000-0000-0000-0000-000000000301'
  ),
  2,
  'an authored JSON change enqueues a fresh Markdown extraction job'
);

create temporary table unitgroup_modified_at_before (
  value timestamp with time zone not null
) on commit drop;

insert into unitgroup_modified_at_before (value)
select modified_at
from public.unitgroups
where id = 'a1000000-0000-0000-0000-000000000301'
  and version = '01.00.000';

update public.unitgroups
set extracted_md = '# Unit group\nqueued-unitgroup-v2'
where id = 'a1000000-0000-0000-0000-000000000301'
  and version = '01.00.000';

select is(
  (
    select count(*)::integer
    from pgmq.q_embedding_jobs queued
    where queued.message @> jsonb_build_object(
      'schema', 'public',
      'table', 'unitgroups',
      'id', 'a1000000-0000-0000-0000-000000000301',
      'version', '01.00.000',
      'contentFunction', 'unitgroups_embedding_ft_input',
      'embeddingColumn', 'embedding_ft',
      'edgeFunction', 'embedding_ft'
    )
  ),
  1,
  'a Markdown change enqueues the current embedding_ft worker contract'
);

select is(
  (
    select modified_at
    from public.unitgroups
    where id = 'a1000000-0000-0000-0000-000000000301'
      and version = '01.00.000'
  ),
  (select value from unitgroup_modified_at_before),
  'derived Markdown writes do not change the authored modified_at timestamp'
);

set local role service_role;
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

select is(
  public.cmd_dataset_semantic_backfill('unitgroups', 10, null, null, false)->>'already_queued_count',
  '1',
  'bounded backfill recognizes already queued extraction work'
);

reset role;
insert into public.sources (
  id, version, json, json_ordered, user_id, state_code, extracted_md, modified_at
)
values (
  'fa000000-0000-0000-0000-000000000301',
  '01.00.000',
  '{"name":"embedding-only-backfill-source"}',
  null,
  null,
  0,
  '# Source\nembedding-only-backfill-source',
  now()
);

delete from pgmq.q_dataset_extraction_jobs queued
where queued.message->>'table' = 'sources'
  and queued.message->>'id' = 'fa000000-0000-0000-0000-000000000301';

set local role service_role;
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

select is(
  public.cmd_dataset_semantic_backfill('sources', 10, 'f9000000-0000-0000-0000-000000000000', null, false)->>'embedding_enqueued_count',
  '1',
  'bounded backfill can resume a row that has Markdown but lacks embedding_ft'
);

select is(
  public.cmd_dataset_semantic_backfill('not_a_dataset', 10, null, null, false)->>'code',
  'UNSUPPORTED_DATASET_TABLE',
  'bounded backfill rejects tables outside the four-table allow-list'
);

reset role;
insert into public.flowproperties (
  id, version, json, json_ordered, user_id, state_code, modified_at
)
values (
  'ff000000-0000-0000-0000-000000000301',
  '',
  '{"name":"invalid-blank-version-backfill-row"}',
  null,
  null,
  0,
  now()
);

delete from pgmq.q_dataset_extraction_jobs queued
where queued.message->>'table' = 'flowproperties'
  and queued.message->>'id' = 'ff000000-0000-0000-0000-000000000301';

set local role service_role;
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

select is(
  public.cmd_dataset_semantic_backfill(
    'flowproperties',
    10,
    'fe000000-0000-0000-0000-000000000000',
    null,
    false
  )->>'scanned_count',
  '0',
  'bounded backfill excludes a row without a stable nonblank version identity'
);

reset role;

select is(
  (
    select count(*)::integer
    from pgmq.q_dataset_extraction_jobs queued
    where queued.message->>'table' = 'flowproperties'
      and queued.message->>'id' = 'ff000000-0000-0000-0000-000000000301'
  ),
  0,
  'bounded backfill does not enqueue a terminally invalid blank-version job'
);

select * from finish();

rollback;
