create index concurrently if not exists sources_extracted_md_pgroonga
  on public.sources using pgroonga (extracted_md);
