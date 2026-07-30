begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(14);

select is(
  (
    select count(*)::integer
    from information_schema.columns as column_info
    where column_info.table_schema in ('public', 'util')
      and column_info.column_name in (
        'extracted_text',
        'expected_extracted_text_sha256',
        'embedding_flag',
        'embedding_at'
      )
  ),
  0,
  'retired extracted-text and legacy embedding columns are absent'
);

select is(
  (
    select count(*)::integer
    from information_schema.columns as column_info
    where column_info.table_schema = 'public'
      and column_info.table_name in (
        'contacts', 'flowproperties', 'flows', 'lifecyclemodels',
        'processes', 'sources', 'unitgroups'
      )
      and column_info.column_name in (
        'extracted_md', 'embedding_ft', 'embedding_ft_at'
      )
  ),
  21,
  'all seven datasets retain extracted_md, embedding_ft, and embedding_ft_at'
);

select is(
  (
    select count(*)::integer
    from pg_indexes as index_row
    where index_row.schemaname = 'public'
      and index_row.indexname in (
        'contacts_text_pgroonga',
        'flowproperties_text_pgroonga',
        'flows_text_pgroonga',
        'lifecyclemodels_text_pgroonga',
        'processes_text_pgroonga',
        'sources_text_pgroonga',
        'unitgroups_text_pgroonga'
      )
  ),
  0,
  'all seven retired text indexes are absent'
);

select is(
  (
    select count(*)::integer
    from pg_indexes as index_row
    where index_row.schemaname = 'public'
      and index_row.tablename in (
        'contacts', 'flowproperties', 'flows', 'lifecyclemodels',
        'processes', 'sources', 'unitgroups'
      )
      and index_row.indexdef ~ '\mextracted_md\M'
      and index_row.indexdef ilike '%using pgroonga%'
  ),
  7,
  'all seven extracted_md PGroonga indexes remain'
);

select cmp_ok(
  (
    select count(*)::integer
    from pg_indexes as index_row
    where index_row.schemaname = 'public'
      and index_row.tablename in (
        'contacts', 'flowproperties', 'flows', 'lifecyclemodels',
        'processes', 'sources', 'unitgroups'
      )
      and index_row.indexdef ~ '\membedding_ft\M'
      and index_row.indexdef ilike '%using hnsw%'
  ),
  '>=',
  7,
  'the supported embedding_ft HNSW index set remains'
);

select is(
  (
    select count(*)::integer
    from pg_trigger as trigger_row
    join pg_class as target on target.oid = trigger_row.tgrelid
    join pg_namespace as target_schema on target_schema.oid = target.relnamespace
    where not trigger_row.tgisinternal
      and target_schema.nspname = 'public'
      and trigger_row.tgname in (
        'zz_contacts_extracted_text_sync_trigger',
        'zz_flowproperties_extracted_text_sync_trigger',
        'zz_flows_extracted_text_sync_trigger',
        'zz_lifecyclemodels_extracted_text_sync_trigger',
        'zz_processes_extracted_text_sync_trigger',
        'zz_sources_extracted_text_sync_trigger',
        'zz_unitgroups_extracted_text_sync_trigger'
      )
  ),
  0,
  'all seven retired synchronous text triggers are absent'
);

select is(
  (
    select count(*)::integer
    from pg_trigger as trigger_row
    where not trigger_row.tgisinternal
      and (
        pg_get_triggerdef(trigger_row.oid, true) ~ '\mextracted_text\M'
        or pg_get_triggerdef(trigger_row.oid, true) ~ '\membedding_flag\M'
        or pg_get_triggerdef(trigger_row.oid, true) ~ '\membedding_at\M'
      )
  ),
  0,
  'no active trigger definition names a retired column'
);

select is(
  (
    select count(*)::integer
    from (values
      ('public.hybrid_search_flows(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[])'),
      ('public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[])'),
      ('public.hybrid_search_lifecyclemodels(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[])'),
      ('public.hybrid_search_contacts(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('public.hybrid_search_flowproperties(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('public.hybrid_search_sources(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('public.hybrid_search_unitgroups(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('private.hybrid_search_simple_dataset(regclass,text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('public.cmd_dataset_extracted_text_backfill(text,integer,uuid,text,text)'),
      ('util.set_dataset_extracted_text_from_json()'),
      ('util.dataset_json_search_text(jsonb)'),
      ('util.dataset_json_search_text(text,jsonb)'),
      ('util.dataset_json_search_text_allowed_prefixes(text)'),
      ('util.dataset_json_search_text_is_noise(text,text)'),
      ('public.flows_embedding_input(public.flows)'),
      ('public.processes_embedding_input(public.processes)'),
      ('public.lifecyclemodels_embedding_input(public.lifecyclemodels)'),
      ('public.generate_flow_embedding()'),
      ('public.pgroonga_search(text)'),
      ('public.pgroonga_search_flows_text_v1(text,integer,integer,text)'),
      ('public.pgroonga_search_processes_text_v1(text,integer,integer,text)'),
      ('public.pgroonga_search_lifecyclemodels_text_v1(text,integer,integer,text)')
    ) retired(signature)
    where to_regprocedure(retired.signature) is not null
  ),
  0,
  'retired compatibility, extraction, embedding, and text-search functions are absent'
);

select is(
  (
    select count(*)::integer
    from pg_proc as routine
    join pg_namespace as routine_schema on routine_schema.oid = routine.pronamespace
    where routine_schema.nspname in ('public', 'private', 'util')
      and (
        routine.prosrc ~ '\mextracted_text\M'
        or routine.prosrc ~ '\membedding_flag\M'
        or routine.prosrc ~ '\membedding_at\M'
      )
  ),
  0,
  'no retained database routine reads a retired column'
);

select is(
  (
    select count(*)::integer
    from pg_proc as routine
    join pg_namespace as routine_schema on routine_schema.oid = routine.pronamespace
    where routine_schema.nspname in ('public', 'private', 'util')
      and coalesce(routine.proargnames, '{}'::text[])
        && array['full_text_weight', 'extracted_text_weight']::text[]
  ),
  0,
  'no retained database routine exposes either compatibility lexical weight'
);

select is(
  (
    select count(*)::integer
    from (values
      ('private.hybrid_search_flows_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
      ('private.hybrid_search_processes_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
      ('private.hybrid_search_lifecyclemodels_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
      ('private.hybrid_search_simple_dataset_v2(regclass,text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)')
    ) retained(signature)
    join pg_proc as routine on routine.oid = to_regprocedure(retained.signature)
    where 'lexical_weight' = any(routine.proargnames)
      and not (
        coalesce(routine.proargnames, '{}'::text[])
          && array['full_text_weight', 'extracted_text_weight']::text[]
      )
  ),
  4,
  'all four private Hybrid v2 implementations expose one lexical weight'
);

select is(
  (
    select count(*)::integer
    from pg_proc as routine
    join pg_namespace as routine_schema on routine_schema.oid = routine.pronamespace
    where routine_schema.nspname = 'public'
      and routine.proname like 'hybrid_search_%_v2'
      and routine.prosrc like '%private.hybrid_search_%'
      and routine.prosrc not like '%0.0::double precision%'
  ),
  7,
  'all seven public Hybrid v2 RPCs call lexical-only private implementations'
);

select is(
  (
    select count(*)::integer
    from util.pending_embedding_jobs as pending
    where pending.status = 'pending'
      and pending.embedding_column <> 'embedding_ft'
  ),
  0,
  'no pending non-FT embedding work remains'
);

select is(
  (
    select count(*)::integer
    from pgmq.q_embedding_jobs as queued
    where coalesce(queued.message->>'embeddingColumn', '') <> 'embedding_ft'
  ),
  0,
  'no active non-FT embedding queue message remains'
);

select * from finish();

rollback;
