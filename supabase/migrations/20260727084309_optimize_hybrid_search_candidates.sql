-- Staging plans showed that the always-true `json @> '{}'` predicate caused
-- severe row-estimate errors and kept the flow semantic path off its HNSW
-- index. Keep non-empty JSON containment semantics, but let a custom plan
-- fold the empty predicate away. Filtered HNSW scans use pgvector's strict
-- iterative mode so post-index filters do not silently truncate candidates.

create index concurrently if not exists processes_embedding_ft_tg_hnsw_idx
  on public.processes using hnsw (embedding_ft extensions.vector_cosine_ops)
  where state_code = 100
    and embedding_ft is not null;

do $migration$
declare
  target_function regprocedure;
  function_definition text;
  source_predicate text;
  replacement_predicate text;
  replacement_count integer;
begin
  if not coalesce(
    (
      select index_catalog.indisvalid
        and index_catalog.indisready
        and index_catalog.indislive
      from pg_index index_catalog
      where index_catalog.indexrelid =
        to_regclass('public.processes_embedding_ft_tg_hnsw_idx')
    ),
    false
  ) then
    raise exception
      'processes_embedding_ft_tg_hnsw_idx is not valid, ready, and live';
  end if;

  foreach target_function in array array[
    'private.semantic_flow_candidates(text,text,double precision,integer,text)'::regprocedure,
    'private.semantic_process_candidates(text,text,double precision,integer,text)'::regprocedure
  ]
  loop
    function_definition := pg_get_functiondef(target_function);

    if strpos(function_definition, 'SET plan_cache_mode TO ''force_custom_plan''') > 0
      or strpos(function_definition, 'SET hnsw.iterative_scan TO ''strict_order''') > 0 then
      raise exception '% already contains issue #292 planner settings', target_function;
    end if;

    if strpos(function_definition, ' SET statement_timeout TO ''60s''') = 0 then
      raise exception '% is missing the expected statement timeout anchor', target_function;
    end if;

    function_definition := replace(
      function_definition,
      $old_config$ SET statement_timeout TO '60s'
$old_config$,
      $new_config$ SET statement_timeout TO '60s'
 SET plan_cache_mode TO 'force_custom_plan'
 SET hnsw.iterative_scan TO 'strict_order'
$new_config$
    );

    if target_function =
      'private.semantic_flow_candidates(text,text,double precision,integer,text)'::regprocedure then
      source_predicate :=
        'and f.json @> filter_condition_jsonb';
      replacement_predicate :=
        'and (filter_condition_jsonb = ''{}''::jsonb or f.json @> filter_condition_jsonb)';
    else
      source_predicate :=
        'and p.json @> filter_condition_jsonb';
      replacement_predicate :=
        'and (filter_condition_jsonb = ''{}''::jsonb or p.json @> filter_condition_jsonb)';
    end if;

    replacement_count := (
      length(function_definition)
        - length(replace(function_definition, source_predicate, ''))
    ) / length(source_predicate);

    if replacement_count <> 4 then
      raise exception
        '% expected four JSON containment predicates, found %',
        target_function,
        replacement_count;
    end if;

    function_definition := replace(
      function_definition,
      source_predicate,
      replacement_predicate
    );

    execute function_definition;

    if not coalesce(
      (
        select p.proconfig @> array[
          'plan_cache_mode=force_custom_plan',
          'hnsw.iterative_scan=strict_order'
        ]
        from pg_proc p
        where p.oid = target_function
      ),
      false
    ) then
      raise exception '% planner settings were not installed', target_function;
    end if;

    function_definition := pg_get_functiondef(target_function);
    replacement_count := (
      length(function_definition)
        - length(replace(function_definition, replacement_predicate, ''))
    ) / length(replacement_predicate);

    if replacement_count <> 4
      or strpos(function_definition, source_predicate) > 0 then
      raise exception '% JSON predicate rewrite did not apply exactly', target_function;
    end if;
  end loop;
end;
$migration$;
