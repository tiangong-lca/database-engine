-- Intentionally non-transactional: a retry first removes any INVALID or
-- unrecorded same-name build left by an interrupted concurrent migration.
drop index concurrently if exists
  private.portal_catalog_search_process_document_v1_pgroonga;

create index concurrently portal_catalog_search_process_document_v1_pgroonga
  on private.portal_catalog_search_rows_v1 using pgroonga (
    document extensions.pgroonga_text_full_text_search_ops_v2
  ) with (tokenizer='TokenBigram', normalizer='NormalizerAuto')
  where dataset_kind = 'process';
