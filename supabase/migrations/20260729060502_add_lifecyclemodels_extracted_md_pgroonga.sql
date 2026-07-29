create index concurrently if not exists lifecyclemodels_extracted_md_pgroonga
  on public.lifecyclemodels using pgroonga (extracted_md);
