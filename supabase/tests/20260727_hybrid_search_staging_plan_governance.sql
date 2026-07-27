begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(18);

select is(
  (
    select am.amname
    from pg_class index_relation
    join pg_am am on am.oid = index_relation.relam
    where index_relation.oid =
      to_regclass('public.processes_embedding_ft_tg_hnsw_idx')
  ),
  'hnsw',
  'the public-process partial vector index uses HNSW'
);

select ok(
  (
    select pg_get_expr(index_catalog.indpred, index_catalog.indrelid)
      like '%state_code = 100%'
      and pg_get_expr(index_catalog.indpred, index_catalog.indrelid)
        like '%embedding_ft IS NOT NULL%'
    from pg_index index_catalog
    where index_catalog.indexrelid =
      'public.processes_embedding_ft_tg_hnsw_idx'::regclass
  ),
  'the process HNSW index is limited to embedded public tg rows'
);

select ok(
  (
    select index_catalog.indisvalid
      and index_catalog.indisready
      and index_catalog.indislive
    from pg_index index_catalog
    where index_catalog.indexrelid =
      'public.processes_embedding_ft_tg_hnsw_idx'::regclass
  ),
  'the process public HNSW index is valid, ready, and live'
);

select ok(
  (
    select routine.proconfig @> array[
      'plan_cache_mode=force_custom_plan',
      'hnsw.iterative_scan=strict_order'
    ]
    from pg_proc routine
    where routine.oid =
      'private.semantic_process_candidates(text,text,double precision,integer,text)'::regprocedure
  ),
  'process semantic candidates use custom plans and strict iterative HNSW scans'
);

select ok(
  (
    select routine.proconfig @> array[
      'plan_cache_mode=force_custom_plan',
      'hnsw.iterative_scan=strict_order'
    ]
    from pg_proc routine
    where routine.oid =
      'private.semantic_flow_candidates(text,text,double precision,integer,text)'::regprocedure
  ),
  'flow semantic candidates use custom plans and strict iterative HNSW scans'
);

select is(
  (
    select (
      length(routine.prosrc) - length(replace(
        routine.prosrc,
        'filter_condition_jsonb = ''{}''::jsonb or p.json @> filter_condition_jsonb',
        ''
      ))
    ) / length(
      'filter_condition_jsonb = ''{}''::jsonb or p.json @> filter_condition_jsonb'
    )
    from pg_proc routine
    where routine.oid =
      'private.semantic_process_candidates(text,text,double precision,integer,text)'::regprocedure
  ),
  4,
  'all four process visibility branches can fold an empty JSON filter away'
);

select is(
  (
    select (
      length(routine.prosrc) - length(replace(
        routine.prosrc,
        'filter_condition_jsonb = ''{}''::jsonb or f.json @> filter_condition_jsonb',
        ''
      ))
    ) / length(
      'filter_condition_jsonb = ''{}''::jsonb or f.json @> filter_condition_jsonb'
    )
    from pg_proc routine
    where routine.oid =
      'private.semantic_flow_candidates(text,text,double precision,integer,text)'::regprocedure
  ),
  4,
  'all four flow visibility branches can fold an empty JSON filter away'
);

select is(
  (
    select strpos(
      routine.prosrc,
      'and p.json @> filter_condition_jsonb'
    )
    from pg_proc routine
    where routine.oid =
      'private.semantic_process_candidates(text,text,double precision,integer,text)'::regprocedure
  ),
  0,
  'process candidates have no unconditional JSON containment predicate'
);

select is(
  (
    select strpos(
      routine.prosrc,
      'and f.json @> filter_condition_jsonb'
    )
    from pg_proc routine
    where routine.oid =
      'private.semantic_flow_candidates(text,text,double precision,integer,text)'::regprocedure
  ),
  0,
  'flow candidates have no unconditional JSON containment predicate'
);

select ok(
  strpos(
    pg_get_functiondef(
      'private.semantic_process_candidates(text,text,double precision,integer,text)'::regprocedure
    ),
    'candidate_size := greatest(normalized_match_count * 10, 200);'
  ) > 0,
  'process semantic candidate growth remains one 10x bound with a 200-row floor'
);

select ok(
  strpos(
    pg_get_functiondef(
      'private.semantic_flow_candidates(text,text,double precision,integer,text)'::regprocedure
    ),
    'candidate_size := greatest(normalized_match_count * 10, 200);'
  ) > 0,
  'flow semantic candidate growth remains one 10x bound with a 200-row floor'
);

select ok(
  strpos(
    pg_get_functiondef(
      'public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[])'::regprocedure
    ),
    'match_threshold,
        semantic_match_count,
        data_source'
  ) > 0,
  'process hybrid search still passes the unexpanded semantic match count'
);

select ok(
  strpos(
    pg_get_functiondef(
      'public.hybrid_search_flows(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[])'::regprocedure
    ),
    'match_threshold,
        semantic_match_count,
        data_source'
  ) > 0,
  'flow hybrid search still passes the unexpanded semantic match count'
);

select ok(
  strpos(
    pg_get_functiondef(
      'public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[])'::regprocedure
    ),
    'order by counted_rows.score desc, counted_rows.modified_at desc, counted_rows.id'
  ) > 0
  and strpos(
    pg_get_functiondef(
      'public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[])'::regprocedure
    ),
    'offset (greatest(coalesce(page_current, 1), 1) - 1) * greatest(coalesce(page_size, 10), 1)'
  ) > 0,
  'process result ordering and page offset remain compatible'
);

select ok(
  strpos(
    pg_get_functiondef(
      'public.hybrid_search_flows(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[])'::regprocedure
    ),
    'order by counted_rows.score desc, counted_rows.modified_at desc, counted_rows.id'
  ) > 0
  and strpos(
    pg_get_functiondef(
      'public.hybrid_search_flows(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[])'::regprocedure
    ),
    'offset (greatest(coalesce(page_current, 1), 1) - 1) * greatest(coalesce(page_size, 10), 1)'
  ) > 0,
  'flow result ordering and page offset remain compatible'
);

select ok(
  to_regclass('public.processes_embedding_ft_hnsw_idx') is not null,
  'the global process HNSW index remains available for owner and team searches'
);

select ok(
  to_regclass('public.flows_embedding_ft_hnsw_idx') is not null,
  'the global flow HNSW index remains available'
);

select ok(
  not coalesce(
    (
      select routine.proconfig @> array[
        'plan_cache_mode=force_custom_plan',
        'hnsw.iterative_scan=strict_order'
      ]
      from pg_proc routine
      where routine.oid =
        'private.semantic_lifecyclemodel_candidates(text,text,double precision,integer,text)'::regprocedure
    ),
    false
  ),
  'unmeasured lifecycle-model semantic search is unchanged'
);

select * from finish();

rollback;
