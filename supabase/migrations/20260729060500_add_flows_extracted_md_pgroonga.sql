create index concurrently if not exists flows_extracted_md_pgroonga
  on public.flows using pgroonga (extracted_md);
