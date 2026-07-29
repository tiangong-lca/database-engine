create index concurrently if not exists flowproperties_extracted_md_pgroonga
  on public.flowproperties using pgroonga (extracted_md);
