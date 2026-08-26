-- Concurrent, Portal-scope-only Flow lexical candidate index.  Keep this as
-- the sole statement so Supabase CLI executes it outside pipeline mode.
create index concurrently if not exists flows_portal_catalog_document_pgroonga
  on public.flows using pgroonga (
    (private.catalog_portal_document_v1('flow', json))
      extensions.pgroonga_text_full_text_search_ops_v2
  ) with (tokenizer='TokenBigram', normalizer='NormalizerAuto')
  where state_code in (100, 200)
    and modified_at is not null
    and pg_catalog.jsonb_typeof(json) = 'object'
    and pg_catalog.jsonb_typeof(json -> 'flowDataSet') = 'object';
