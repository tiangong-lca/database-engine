-- Database B: one concurrent single-column PGroonga index per search_text array.
-- TokenBigram and NormalizerAuto are the production defaults; array elements
-- remain independent indexed documents.

create index concurrently if not exists contacts_search_text_pgroonga
  on public.contacts using pgroonga (
    search_text pgroonga_text_array_full_text_search_ops_v2
  ) with (tokenizer='TokenBigram', normalizer='NormalizerAuto');
