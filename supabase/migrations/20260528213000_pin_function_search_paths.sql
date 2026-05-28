-- Pin search_path for the remaining functions flagged by the Supabase
-- function_search_path_mutable advisor. The function bodies already qualify
-- non-catalog references, so an empty path preserves behavior while removing
-- caller-dependent name resolution.

alter function public.update_modified_at()
  set search_path to '';

alter function util.dataset_json_search_text(jsonb)
  set search_path to '';

alter function util.dataset_json_search_text(text, jsonb)
  set search_path to '';

alter function util.dataset_json_search_text_allowed_prefixes(text)
  set search_path to '';

alter function util.dataset_json_search_text_is_noise(text, text)
  set search_path to '';
