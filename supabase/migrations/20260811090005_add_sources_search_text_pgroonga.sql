-- Database B: one concurrent single-column PGroonga index per search_text array.

create index concurrently if not exists sources_search_text_pgroonga
  on public.sources using pgroonga (
    search_text pgroonga_text_array_full_text_search_ops_v2
  ) with (tokenizer='TokenBigram', normalizer='NormalizerAuto');
