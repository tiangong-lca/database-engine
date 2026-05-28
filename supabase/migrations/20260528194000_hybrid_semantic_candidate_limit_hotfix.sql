do $$
declare
  flow_signature constant text := 'public.hybrid_search_flows(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)';
  process_signature constant text := 'public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)';
  lifecyclemodel_signature constant text := 'public.hybrid_search_lifecyclemodels(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)';
  fn text;
begin
  foreach fn in array array[
    pg_get_functiondef(flow_signature::regprocedure),
    pg_get_functiondef(process_signature::regprocedure),
    pg_get_functiondef(lifecyclemodel_signature::regprocedure)
  ]
  loop
    fn := replace(
      fn,
      '  candidate_limit integer;
  filter_condition_jsonb jsonb;',
      '  candidate_limit integer;
  semantic_match_count integer;
  filter_condition_jsonb jsonb;'
    );

    fn := replace(
      fn,
      '  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''''), ''{}'')::jsonb;',
      '  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;
  semantic_match_count := greatest(coalesce(match_count, 20), coalesce(page_size, 10));
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''''), ''{}'')::jsonb;'
    );

    fn := replace(
      fn,
      '        match_threshold,
        candidate_limit,
        data_source',
      '        match_threshold,
        semantic_match_count,
        data_source'
    );

    execute fn;
  end loop;

  if strpos(pg_get_functiondef(flow_signature::regprocedure), 'semantic_match_count') = 0
    or strpos(pg_get_functiondef(process_signature::regprocedure), 'semantic_match_count') = 0
    or strpos(pg_get_functiondef(lifecyclemodel_signature::regprocedure), 'semantic_match_count') = 0
  then
    raise exception 'hybrid semantic candidate limit hotfix did not apply';
  end if;
end;
$$;
