create index concurrently if not exists processes_extracted_md_pgroonga
  on public.processes using pgroonga (extracted_md);
