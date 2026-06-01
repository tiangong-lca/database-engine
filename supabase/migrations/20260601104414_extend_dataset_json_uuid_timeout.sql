-- Extend the bounded timeout for UUID mention lookups. Large flow JSON payloads
-- can exceed the original 8s budget while still being acceptable for this
-- explicit, low-frequency lookup path.

alter function private.search_dataset_json_uuid_mentions_impl(uuid, text[], text, text, uuid, integer, integer)
  set statement_timeout to '20s';

alter function public.search_dataset_json_uuid_mentions(uuid, text[], text, text, uuid, integer, integer)
  set statement_timeout to '20s';
