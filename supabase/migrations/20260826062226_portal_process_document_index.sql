-- Concurrent, Portal-scope-only Process lexical candidate index.  Keep this
-- as the sole statement so Supabase CLI executes it outside pipeline mode.
create index concurrently if not exists processes_portal_catalog_document_pgroonga
  on public.processes using pgroonga (
    (private.catalog_portal_document_v1('process', json))
      extensions.pgroonga_text_full_text_search_ops_v2
  ) with (tokenizer='TokenBigram', normalizer='NormalizerAuto')
  where state_code in (100, 200)
    and modified_at is not null
    and pg_catalog.jsonb_typeof(json) = 'object'
    and pg_catalog.jsonb_typeof(json -> 'processDataSet') = 'object';
