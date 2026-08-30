create index concurrently portal_catalog_search_process_exact_rank_v1_gin
on private.portal_catalog_search_rows_v1 using gin (
  private.portal_process_rank_name_keys_v1(card),
  private.portal_process_rank_classification_keys_v1(card)
)
where dataset_kind = 'process';
