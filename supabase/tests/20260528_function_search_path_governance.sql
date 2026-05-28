begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create or replace function pg_temp.has_empty_search_path(p_signature text)
returns boolean
language sql
stable
as $$
  select exists (
    select 1
    from pg_proc p
    cross join lateral unnest(coalesce(p.proconfig, array[]::text[])) as config(setting)
    where p.oid = p_signature::regprocedure
      and config.setting in ('search_path=', 'search_path=""')
  );
$$;

select plan(5);

select ok(
  pg_temp.has_empty_search_path('public.update_modified_at()'),
  'public.update_modified_at() pins an empty search_path'
);

select ok(
  pg_temp.has_empty_search_path('util.dataset_json_search_text(jsonb)'),
  'util.dataset_json_search_text(jsonb) pins an empty search_path'
);

select ok(
  pg_temp.has_empty_search_path('util.dataset_json_search_text(text,jsonb)'),
  'util.dataset_json_search_text(text,jsonb) pins an empty search_path'
);

select ok(
  pg_temp.has_empty_search_path('util.dataset_json_search_text_allowed_prefixes(text)'),
  'util.dataset_json_search_text_allowed_prefixes(text) pins an empty search_path'
);

select ok(
  pg_temp.has_empty_search_path('util.dataset_json_search_text_is_noise(text,text)'),
  'util.dataset_json_search_text_is_noise(text,text) pins an empty search_path'
);

select * from finish();

rollback;
