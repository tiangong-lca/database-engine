create index concurrently if not exists unitgroups_extracted_md_pgroonga
  on public.unitgroups using pgroonga (extracted_md);
