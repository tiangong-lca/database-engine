create index concurrently if not exists contacts_extracted_md_pgroonga
  on public.contacts using pgroonga (extracted_md);
