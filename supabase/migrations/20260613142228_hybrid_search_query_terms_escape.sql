create or replace function private.pgroonga_escape_query_terms(query_terms text[])
returns text[]
language sql
immutable
set search_path to 'extensions', 'pg_temp'
as $$
  select coalesce(
    array_agg(extensions.pgroonga_query_escape(normalized_term) order by ord),
    '{}'::text[]
  )
  from (
    select regexp_replace(btrim(raw_term.term), '[[:space:]]+', ' ', 'g') as normalized_term,
           raw_term.ord
    from unnest(coalesce(query_terms, '{}'::text[])) with ordinality as raw_term(term, ord)
    where raw_term.term is not null
      and btrim(raw_term.term) <> ''
  ) terms;
$$;

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
  flow_private constant regprocedure := 'private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure;
  process_private constant regprocedure := 'private.search_processes_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text)'::regprocedure;
  lifecyclemodel_private constant regprocedure := 'private.search_lifecyclemodels_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure;
  flow_public constant regprocedure := 'public.search_flows_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure;
  process_public constant regprocedure := 'public.search_processes_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text)'::regprocedure;
  lifecyclemodel_public constant regprocedure := 'public.search_lifecyclemodels_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure;
  flow_hybrid constant regprocedure := 'public.hybrid_search_flows(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure;
  process_hybrid constant regprocedure := 'public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure;
  lifecyclemodel_hybrid constant regprocedure := 'public.hybrid_search_lifecyclemodels(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure;
  fn text;
begin
  fn := pg_get_functiondef(lifecyclemodel_private);
  fn := pg_temp.required_replace(fn, 'state_code_filter integer DEFAULT NULL::integer)', 'state_code_filter integer DEFAULT NULL::integer, query_terms text[] DEFAULT NULL::text[])', 'lifecyclemodel private signature');
  fn := pg_temp.required_replace(fn, '  v_sql text;', '  v_sql text;
  escaped_query_terms text[];
  text_match_clause text;', 'lifecyclemodel private declarations');
  fn := pg_temp.required_replace(fn, '  filter_condition_jsonb := coalesce(filter_condition, ''{}''::jsonb);', '  filter_condition_jsonb := coalesce(filter_condition, ''{}''::jsonb);
  escaped_query_terms := private.pgroonga_escape_query_terms(query_terms);
  if cardinality(escaped_query_terms) = 0 then
    escaped_query_terms := private.pgroonga_escape_query_terms(array[query_text]);
  end if;
  text_match_clause := ''where l.extracted_text &@~| $10'';', 'lifecyclemodel private escaped terms');
  fn := pg_temp.required_replace(fn, '      where l.extracted_text &@~ $1', '      %s', 'lifecyclemodel private text match clause');
  fn := pg_temp.required_replace(fn, '$sql$, json_filter_clause);', '$sql$, text_match_clause, json_filter_clause);', 'lifecyclemodel private format args');
  fn := pg_temp.required_replace(fn, '          normalized_data_source, effective_user_id, team_id_filter, state_code_filter, can_read_team_filter;', '          normalized_data_source, effective_user_id, team_id_filter, state_code_filter,
          can_read_team_filter, escaped_query_terms;', 'lifecyclemodel private execute args');
  execute fn;

  fn := pg_get_functiondef(process_private);
  fn := pg_temp.required_replace(fn, 'type_of_data_set_filter text DEFAULT ''all''::text)', 'type_of_data_set_filter text DEFAULT ''all''::text, query_terms text[] DEFAULT NULL::text[])', 'process private signature');
  fn := pg_temp.required_replace(fn, '  v_sql text;', '  v_sql text;
  escaped_query_terms text[];
  text_match_clause text;', 'process private declarations');
  fn := pg_temp.required_replace(fn, '  filter_condition_jsonb := coalesce(filter_condition, ''{}''::jsonb);', '  filter_condition_jsonb := coalesce(filter_condition, ''{}''::jsonb);
  escaped_query_terms := private.pgroonga_escape_query_terms(query_terms);
  if cardinality(escaped_query_terms) = 0 then
    escaped_query_terms := private.pgroonga_escape_query_terms(array[query_text]);
  end if;
  text_match_clause := ''where p.extracted_text &@~| $11'';', 'process private escaped terms');
  fn := pg_temp.required_replace(fn, '      where p.extracted_text &@~ $1', '      %s', 'process private text match clause');
  fn := pg_temp.required_replace(fn, '$sql$, json_filter_clause);', '$sql$, text_match_clause, json_filter_clause);', 'process private format args');
  fn := pg_temp.required_replace(fn, '          can_read_team_filter, type_of_data_set_filter;', '          can_read_team_filter, type_of_data_set_filter, escaped_query_terms;', 'process private execute args');
  execute fn;

  fn := pg_get_functiondef(flow_private);
  fn := pg_temp.required_replace(fn, 'state_code_filter integer DEFAULT NULL::integer)', 'state_code_filter integer DEFAULT NULL::integer, query_terms text[] DEFAULT NULL::text[])', 'flow private signature');
  fn := pg_temp.required_replace(fn, '  v_sql text;', '  v_sql text;
  escaped_query_terms text[];
  text_match_clause text;', 'flow private declarations');
  fn := pg_temp.required_replace(fn, '  filter_condition_jsonb := coalesce(filter_condition, ''{}''::jsonb);', '  filter_condition_jsonb := coalesce(filter_condition, ''{}''::jsonb);
  escaped_query_terms := private.pgroonga_escape_query_terms(query_terms);
  if cardinality(escaped_query_terms) = 0 then
    escaped_query_terms := private.pgroonga_escape_query_terms(array[query_text]);
  end if;
  text_match_clause := ''where f.extracted_text &@~| $14'';', 'flow private escaped terms');
  fn := pg_temp.required_replace(fn, '      where f.extracted_text &@~ $1', '      %s', 'flow private text match clause');
  fn := pg_temp.required_replace(fn, '$sql$, json_filter_clause);', '$sql$, text_match_clause, json_filter_clause);', 'flow private format args');
  fn := pg_temp.required_replace(fn, '          can_read_team_filter, flow_type, flow_type_array, as_input, classification_filter;', '          can_read_team_filter, flow_type, flow_type_array, as_input, classification_filter,
          escaped_query_terms;', 'flow private execute args');
  execute fn;

  fn := pg_get_functiondef(lifecyclemodel_public);
  fn := pg_temp.required_replace(fn, 'state_code_filter integer DEFAULT NULL::integer)', 'state_code_filter integer DEFAULT NULL::integer, query_terms text[] DEFAULT NULL::text[])', 'lifecyclemodel public signature');
  fn := pg_temp.required_replace(fn, '      state_code_filter
    );', '      state_code_filter,
      query_terms
    );', 'lifecyclemodel public query_terms passthrough');
  execute fn;

  fn := pg_get_functiondef(process_public);
  fn := pg_temp.required_replace(fn, 'type_of_data_set_filter text DEFAULT ''all''::text)', 'type_of_data_set_filter text DEFAULT ''all''::text, query_terms text[] DEFAULT NULL::text[])', 'process public signature');
  fn := pg_temp.required_replace(fn, '      type_of_data_set_filter
    );', '      type_of_data_set_filter,
      query_terms
    );', 'process public query_terms passthrough');
  execute fn;

  fn := pg_get_functiondef(flow_public);
  fn := pg_temp.required_replace(fn, 'state_code_filter integer DEFAULT NULL::integer)', 'state_code_filter integer DEFAULT NULL::integer, query_terms text[] DEFAULT NULL::text[])', 'flow public signature');
  fn := pg_temp.required_replace(fn, '      state_code_filter
    );', '      state_code_filter,
      query_terms
    );', 'flow public query_terms passthrough');
  execute fn;

  fn := pg_get_functiondef(lifecyclemodel_hybrid);
  fn := pg_temp.required_replace(fn, 'page_current integer DEFAULT 1)', 'page_current integer DEFAULT 1, query_terms text[] DEFAULT NULL::text[])', 'lifecyclemodel hybrid signature');
  fn := pg_temp.required_replace(fn, '        null::integer
      ) ts', '        null::integer,
        query_terms
      ) ts', 'lifecyclemodel hybrid query_terms passthrough');
  execute fn;

  fn := pg_get_functiondef(process_hybrid);
  fn := pg_temp.required_replace(fn, 'page_current integer DEFAULT 1)', 'page_current integer DEFAULT 1, query_terms text[] DEFAULT NULL::text[])', 'process hybrid signature');
  fn := pg_temp.required_replace(fn, '        ''all''
      ) ts', '        ''all'',
        query_terms
      ) ts', 'process hybrid query_terms passthrough');
  execute fn;

  fn := pg_get_functiondef(flow_hybrid);
  fn := pg_temp.required_replace(fn, 'page_current integer DEFAULT 1)', 'page_current integer DEFAULT 1, query_terms text[] DEFAULT NULL::text[])', 'flow hybrid signature');
  fn := pg_temp.required_replace(fn, '        null::integer
      ) ts', '        null::integer,
        query_terms
      ) ts', 'flow hybrid query_terms passthrough');
  execute fn;
end;
$$;

drop function public.hybrid_search_flows(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer);
drop function public.hybrid_search_processes(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer);
drop function public.hybrid_search_lifecyclemodels(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer);

drop function public.search_flows_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer);
drop function public.search_processes_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text);
drop function public.search_lifecyclemodels_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer);

drop function private.search_flows_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer);
drop function private.search_processes_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer, text);
drop function private.search_lifecyclemodels_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer);

alter function private.pgroonga_escape_query_terms(text[]) owner to postgres;
alter function private.search_lifecyclemodels_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer, text[]) owner to postgres;
alter function private.search_processes_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer, text, text[]) owner to postgres;
alter function private.search_flows_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer, text[]) owner to postgres;
alter function public.search_lifecyclemodels_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text[]) owner to postgres;
alter function public.search_processes_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text, text[]) owner to postgres;
alter function public.search_flows_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text[]) owner to postgres;
alter function public.hybrid_search_lifecyclemodels(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) owner to postgres;
alter function public.hybrid_search_processes(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) owner to postgres;
alter function public.hybrid_search_flows(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) owner to postgres;

revoke all on function private.pgroonga_escape_query_terms(text[]) from public;
revoke all on function private.search_lifecyclemodels_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer, text[]) from public;
revoke all on function private.search_processes_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer, text, text[]) from public;
revoke all on function private.search_flows_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer, text[]) from public;

grant execute on function private.pgroonga_escape_query_terms(text[]) to anon, authenticated, service_role;
grant execute on function private.search_lifecyclemodels_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer, text[]) to anon, authenticated, service_role;
grant execute on function private.search_processes_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer, text, text[]) to anon, authenticated, service_role;
grant execute on function private.search_flows_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer, text[]) to anon, authenticated, service_role;

grant all on function public.search_lifecyclemodels_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text[]) to anon, authenticated, service_role;
grant all on function public.search_processes_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text, text[]) to anon, authenticated, service_role;
grant all on function public.search_flows_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text[]) to anon, authenticated, service_role;
grant all on function public.hybrid_search_lifecyclemodels(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) to anon, authenticated, service_role;
grant all on function public.hybrid_search_processes(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) to anon, authenticated, service_role;
grant all on function public.hybrid_search_flows(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) to anon, authenticated, service_role;
