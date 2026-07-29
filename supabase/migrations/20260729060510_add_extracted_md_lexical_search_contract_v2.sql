-- Expand phase for the extracted_text retirement.  The old Hybrid Search RPCs
-- remain callable while Edge and Next move to the v2 lexical_weight contract.
-- Search execution itself moves to extracted_md only after the seven indexes
-- above this migration have been built.

create or replace function pg_temp.required_replace(
  source_text text,
  old_text text,
  new_text text,
  replacement_label text
) returns text
language plpgsql
as $$
declare
  replaced_text text;
begin
  replaced_text := replace(source_text, old_text, new_text);
  if replaced_text = source_text then
    raise exception 'required replacement did not apply: %', replacement_label;
  end if;
  return replaced_text;
end;
$$;

do $$
declare
  simple_latest constant regprocedure := 'public._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure;
  flow_latest constant regprocedure := 'private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure;
  process_latest constant regprocedure := 'private.search_processes_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[])'::regprocedure;
  lifecyclemodel_latest constant regprocedure := 'private.search_lifecyclemodels_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure;
  simple_hybrid constant regprocedure := 'private.hybrid_search_simple_dataset(regclass,text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'::regprocedure;
  fn text;
begin
  fn := pg_get_functiondef(simple_latest);
  fn := pg_temp.required_replace(
    fn,
    'd.extracted_text &@~',
    'd.extracted_md &@~',
    'simple latest lexical source'
  );
  execute fn;

  fn := pg_get_functiondef(flow_latest);
  fn := pg_temp.required_replace(
    fn,
    'f.extracted_text &@~|',
    'f.extracted_md &@~|',
    'flow latest lexical source'
  );
  execute fn;

  fn := pg_get_functiondef(process_latest);
  fn := pg_temp.required_replace(
    fn,
    'p.extracted_text &@~|',
    'p.extracted_md &@~|',
    'process latest lexical source'
  );
  execute fn;

  fn := pg_temp.required_replace(
    fn,
    'CREATE OR REPLACE FUNCTION private.search_processes_latest_impl(',
    'CREATE OR REPLACE FUNCTION private.search_processes_latest_v2_impl(',
    'process latest v2 function name'
  );
  fn := pg_temp.required_replace(
    fn,
    'query_terms text[] DEFAULT NULL::text[])',
    'query_terms text[] DEFAULT NULL::text[], owner_draft_only boolean DEFAULT false)',
    'process latest v2 signature'
  );
  fn := pg_temp.required_replace(
    fn,
    '  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''''), ''tg'');',
    '  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''''), ''tg'');
  if owner_draft_only and normalized_data_source <> ''my'' then
    return;
  end if;',
    'process latest v2 owner-draft source guard'
  );
  fn := pg_temp.required_replace(
    fn,
    '             p.model_id,
             pgroonga_score',
    '             p.model_id,
             p.review_id,
             pgroonga_score',
    'process latest v2 lexical review identity'
  );
  fn := pg_temp.required_replace(
    fn,
    'or (normalized_data_source = ''my'' and effective_user_id is not null and p.user_id = effective_user_id and (state_code_filter is null or p.state_code = state_code_filter))',
    'or (normalized_data_source = ''my'' and effective_user_id is not null and p.user_id = effective_user_id and (state_code_filter is null or p.state_code = state_code_filter) and (not owner_draft_only or (p.state_code = 0 and p.team_id is null and p.review_id is null)))',
    'process latest v2 exact owner-draft match'
  );
  fn := pg_temp.required_replace(
    fn,
    'or (normalized_data_source = ''my'' and effective_user_id is not null and p2.user_id = effective_user_id and (state_code_filter is null or p2.state_code = state_code_filter))',
    'or (normalized_data_source = ''my'' and effective_user_id is not null and p2.user_id = effective_user_id and (state_code_filter is null or p2.state_code = state_code_filter) and (not owner_draft_only or (p2.state_code = 0 and p2.team_id is null and p2.review_id is null)))',
    'process latest v2 exact latest-row owner-draft match'
  );
  fn := pg_temp.required_replace(
    fn,
    'or ($5 = ''my'' and $6 is not null and p.user_id = $6 and ($8 is null or p.state_code = $8))',
    'or ($5 = ''my'' and $6 is not null and p.user_id = $6 and ($8 is null or p.state_code = $8) and (not $12 or (p.state_code = 0 and p.team_id is null and p.review_id is null)))',
    'process latest v2 lexical owner-draft match'
  );
  fn := pg_temp.required_replace(
    fn,
    'or ($5 = ''my'' and $6 is not null and p2.user_id = $6 and ($8 is null or p2.state_code = $8))',
    'or ($5 = ''my'' and $6 is not null and p2.user_id = $6 and ($8 is null or p2.state_code = $8) and (not $12 or (p2.state_code = 0 and p2.team_id is null and p2.review_id is null)))',
    'process latest v2 lexical latest-row owner-draft match'
  );
  fn := pg_temp.required_replace(
    fn,
    '          can_read_team_filter, type_of_data_set_filter, escaped_query_terms;',
    '          can_read_team_filter, type_of_data_set_filter, escaped_query_terms,
          owner_draft_only;',
    'process latest v2 owner-draft execute argument'
  );
  execute fn;

  fn := pg_get_functiondef(lifecyclemodel_latest);
  fn := pg_temp.required_replace(
    fn,
    'l.extracted_text &@~|',
    'l.extracted_md &@~|',
    'lifecycle model latest lexical source'
  );
  execute fn;

  fn := pg_get_functiondef(simple_hybrid);
  fn := pg_temp.required_replace(
    fn,
    'd.extracted_text &@~|',
    'd.extracted_md &@~|',
    'simple Hybrid Search lexical source'
  );
  execute fn;
end;
$$;

create or replace function public.search_processes_latest_v2(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  order_by jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer,
  type_of_data_set_filter text default 'all'::text,
  query_terms text[] default null::text[],
  owner_draft_only boolean default false
) returns table(
  rank bigint,
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  team_id uuid,
  model_id uuid,
  total_count bigint
)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
  select *
  from private.search_processes_latest_v2_impl(
    query_text,
    filter_condition,
    page_size,
    page_current,
    data_source,
    this_user_id,
    team_id_filter,
    state_code_filter,
    type_of_data_set_filter,
    query_terms,
    owner_draft_only
  );
$$;

alter function private.search_processes_latest_v2_impl(text, jsonb, bigint, bigint, text, text, uuid, integer, text, text[], boolean) owner to postgres;
alter function public.search_processes_latest_v2(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text, text[], boolean) owner to postgres;

revoke all on function private.search_processes_latest_v2_impl(text, jsonb, bigint, bigint, text, text, uuid, integer, text, text[], boolean) from public;
revoke all on function public.search_processes_latest_v2(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text, text[], boolean) from public;

grant execute on function private.search_processes_latest_v2_impl(text, jsonb, bigint, bigint, text, text, uuid, integer, text, text[], boolean) to anon, authenticated, service_role;
grant execute on function public.search_processes_latest_v2(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text, text[], boolean) to anon, authenticated, service_role;

comment on function public.search_processes_latest_v2(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text, text[], boolean) is
  'Indexed extracted_md process search with latest-version visibility and an optional strict owner-draft scope for LCA analysis.';

create or replace function public.hybrid_search_flows_v2(
  query_text text,
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  lexical_weight double precision default 0.5,
  semantic_weight double precision default 0.5,
  rrf_k integer default 10,
  data_source text default 'tg'::text,
  page_size integer default 10,
  page_current integer default 1,
  query_terms text[] default null::text[]
) returns table(
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  team_id uuid,
  total_count bigint
)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
  select *
  from public.hybrid_search_flows(
    query_text,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    lexical_weight,
    0.0::double precision,
    semantic_weight,
    rrf_k,
    data_source,
    page_size,
    page_current,
    query_terms
  );
$$;

create or replace function public.hybrid_search_processes_v2(
  query_text text,
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  lexical_weight double precision default 0.5,
  semantic_weight double precision default 0.5,
  rrf_k integer default 10,
  data_source text default 'tg'::text,
  page_size integer default 10,
  page_current integer default 1,
  query_terms text[] default null::text[]
) returns table(
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  model_id uuid,
  team_id uuid,
  total_count bigint
)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
  select *
  from public.hybrid_search_processes(
    query_text,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    lexical_weight,
    0.0::double precision,
    semantic_weight,
    rrf_k,
    data_source,
    page_size,
    page_current,
    query_terms
  );
$$;

create or replace function public.hybrid_search_lifecyclemodels_v2(
  query_text text,
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  lexical_weight double precision default 0.5,
  semantic_weight double precision default 0.5,
  rrf_k integer default 10,
  data_source text default 'tg'::text,
  page_size integer default 10,
  page_current integer default 1,
  query_terms text[] default null::text[]
) returns table(
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  team_id uuid,
  total_count bigint
)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
  select *
  from public.hybrid_search_lifecyclemodels(
    query_text,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    lexical_weight,
    0.0::double precision,
    semantic_weight,
    rrf_k,
    data_source,
    page_size,
    page_current,
    query_terms
  );
$$;

create or replace function public.hybrid_search_contacts_v2(
  query_text text,
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  lexical_weight double precision default 0.5,
  semantic_weight double precision default 0.5,
  rrf_k integer default 10,
  data_source text default 'tg'::text,
  page_size integer default 10,
  page_current integer default 1,
  query_terms text[] default null::text[],
  state_code_filter integer default null::integer,
  team_id_filter uuid default null::uuid
) returns table(
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  team_id uuid,
  total_count bigint
)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
  select *
  from public.hybrid_search_contacts(
    query_text,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    lexical_weight,
    0.0::double precision,
    semantic_weight,
    rrf_k,
    data_source,
    page_size,
    page_current,
    query_terms,
    state_code_filter,
    team_id_filter
  );
$$;

create or replace function public.hybrid_search_flowproperties_v2(
  query_text text,
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  lexical_weight double precision default 0.5,
  semantic_weight double precision default 0.5,
  rrf_k integer default 10,
  data_source text default 'tg'::text,
  page_size integer default 10,
  page_current integer default 1,
  query_terms text[] default null::text[],
  state_code_filter integer default null::integer,
  team_id_filter uuid default null::uuid
) returns table(
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  team_id uuid,
  total_count bigint
)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
  select *
  from public.hybrid_search_flowproperties(
    query_text,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    lexical_weight,
    0.0::double precision,
    semantic_weight,
    rrf_k,
    data_source,
    page_size,
    page_current,
    query_terms,
    state_code_filter,
    team_id_filter
  );
$$;

create or replace function public.hybrid_search_sources_v2(
  query_text text,
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  lexical_weight double precision default 0.5,
  semantic_weight double precision default 0.5,
  rrf_k integer default 10,
  data_source text default 'tg'::text,
  page_size integer default 10,
  page_current integer default 1,
  query_terms text[] default null::text[],
  state_code_filter integer default null::integer,
  team_id_filter uuid default null::uuid
) returns table(
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  team_id uuid,
  total_count bigint
)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
  select *
  from public.hybrid_search_sources(
    query_text,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    lexical_weight,
    0.0::double precision,
    semantic_weight,
    rrf_k,
    data_source,
    page_size,
    page_current,
    query_terms,
    state_code_filter,
    team_id_filter
  );
$$;

create or replace function public.hybrid_search_unitgroups_v2(
  query_text text,
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  lexical_weight double precision default 0.5,
  semantic_weight double precision default 0.5,
  rrf_k integer default 10,
  data_source text default 'tg'::text,
  page_size integer default 10,
  page_current integer default 1,
  query_terms text[] default null::text[],
  state_code_filter integer default null::integer,
  team_id_filter uuid default null::uuid
) returns table(
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  team_id uuid,
  total_count bigint
)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
  select *
  from public.hybrid_search_unitgroups(
    query_text,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    lexical_weight,
    0.0::double precision,
    semantic_weight,
    rrf_k,
    data_source,
    page_size,
    page_current,
    query_terms,
    state_code_filter,
    team_id_filter
  );
$$;

alter function public.hybrid_search_flows_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[]) owner to postgres;
alter function public.hybrid_search_processes_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[]) owner to postgres;
alter function public.hybrid_search_lifecyclemodels_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[]) owner to postgres;
alter function public.hybrid_search_contacts_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid) owner to postgres;
alter function public.hybrid_search_flowproperties_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid) owner to postgres;
alter function public.hybrid_search_sources_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid) owner to postgres;
alter function public.hybrid_search_unitgroups_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid) owner to postgres;

revoke all on function public.hybrid_search_flows_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[]) from public;
revoke all on function public.hybrid_search_processes_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[]) from public;
revoke all on function public.hybrid_search_lifecyclemodels_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[]) from public;
revoke all on function public.hybrid_search_contacts_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid) from public;
revoke all on function public.hybrid_search_flowproperties_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid) from public;
revoke all on function public.hybrid_search_sources_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid) from public;
revoke all on function public.hybrid_search_unitgroups_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid) from public;

grant execute on function public.hybrid_search_flows_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[]) to anon, authenticated, service_role;
grant execute on function public.hybrid_search_processes_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[]) to anon, authenticated, service_role;
grant execute on function public.hybrid_search_lifecyclemodels_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[]) to anon, authenticated, service_role;
grant execute on function public.hybrid_search_contacts_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid) to anon, authenticated, service_role;
grant execute on function public.hybrid_search_flowproperties_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid) to anon, authenticated, service_role;
grant execute on function public.hybrid_search_sources_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid) to anon, authenticated, service_role;
grant execute on function public.hybrid_search_unitgroups_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid) to anon, authenticated, service_role;

comment on function public.hybrid_search_flows_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[]) is
  'Hybrid Search v2: extracted_md lexical candidates plus embedding_ft semantic candidates, fused with lexical_weight and semantic_weight.';
comment on function public.hybrid_search_processes_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[]) is
  'Hybrid Search v2: extracted_md lexical candidates plus embedding_ft semantic candidates, fused with lexical_weight and semantic_weight.';
comment on function public.hybrid_search_lifecyclemodels_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[]) is
  'Hybrid Search v2: extracted_md lexical candidates plus embedding_ft semantic candidates, fused with lexical_weight and semantic_weight.';
comment on function public.hybrid_search_contacts_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid) is
  'Hybrid Search v2: extracted_md lexical candidates plus embedding_ft semantic candidates, fused with lexical_weight and semantic_weight.';
comment on function public.hybrid_search_flowproperties_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid) is
  'Hybrid Search v2: extracted_md lexical candidates plus embedding_ft semantic candidates, fused with lexical_weight and semantic_weight.';
comment on function public.hybrid_search_sources_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid) is
  'Hybrid Search v2: extracted_md lexical candidates plus embedding_ft semantic candidates, fused with lexical_weight and semantic_weight.';
comment on function public.hybrid_search_unitgroups_v2(text, text, text, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid) is
  'Hybrid Search v2: extracted_md lexical candidates plus embedding_ft semantic candidates, fused with lexical_weight and semantic_weight.';
