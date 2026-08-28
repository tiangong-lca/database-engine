-- Issue #543 final example repair: exact CAS lookup is sub-millisecond, but a
-- cold public Flow response for a high-cardinality CAS can still spend the
-- eight-second budget hydrating every exhaustive source context. Preserve
-- complete Search semantics and advertise only a CAS that occurs exactly once
-- across retained projection history and still matches its latest-visible row.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '30s';

do $portal_catalog_summary_selective_cas_prerequisite_guard$
declare
  v_summary regprocedure := pg_catalog.to_regprocedure(
    'api.portal_catalog_summary_v1()'
  );
  v_definition_sha256 text;
begin
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.pg_get_functiondef(v_summary),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  into v_definition_sha256;

  if v_summary is null
     or v_definition_sha256 <>
       'caec24d4051e784c2d44b785d864416f2cfcfc0d7bc2637f7610faeb965d22d2'
     or pg_catalog.to_regprocedure(
       'private.portal_catalog_summary_label_v1(jsonb)'
     ) is null
     or pg_catalog.to_regprocedure(
       'private.portal_catalog_summary_valid_cas_v1(text)'
     ) is null
     or pg_catalog.to_regclass(
       'private.portal_catalog_summary_eligibility_v1_idx'
     ) is null
     or pg_catalog.to_regclass(
       'private.portal_catalog_search_flow_cas_v1_idx'
     ) is null
     or (
       select pg_catalog.pg_get_expr(
         index_record.indpred,
         index_record.indrelid,
         true
       ) !~ 'length.*casNumber.*7.*12'
       from pg_catalog.pg_index as index_record
       where index_record.indexrelid =
         'private.portal_catalog_search_flow_cas_v1_idx'::regclass
     )
     or not exists (
       select 1
       from pg_catalog.pg_proc as routine
       where routine.oid = v_summary
         and routine.proowner = 'portal_public_executor'::regrole
         and routine.prosecdef
         and routine.provolatile = 's'
     ) then
    raise exception 'Portal catalog summary selective-CAS prerequisites drifted'
      using errcode = '55000';
  end if;
end
$portal_catalog_summary_selective_cas_prerequisite_guard$;

grant portal_public_executor to postgres;
grant create on schema api to portal_public_executor;
set role portal_public_executor;

select private.assert_portal_catalog_projection_contract_v1();
select private.assert_portal_catalog_facet_contract_v1();

create or replace function api.portal_catalog_summary_v1()
returns jsonb
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '2s'
set work_mem = '32MB'
set plan_cache_mode = 'force_custom_plan'
set max_parallel_workers_per_gather = '0'
set jit = 'off'
set row_security = 'on'
as $function$
declare
  v_counts jsonb;
  v_latest_modified_at text;
  v_uuid_example jsonb;
  v_cas_example jsonb;
  v_classification_example jsonb;
  v_result jsonb;
begin
  perform private.assert_portal_catalog_projection_contract_v1();
  perform private.assert_portal_catalog_facet_contract_v1();

  with latest as materialized (
    select distinct on (facet.dataset_kind, facet.id)
      facet.dataset_kind,
      facet.id,
      facet.version,
      facet.modified_at,
      facet.state_code
    from private.portal_catalog_facet_rows_v1 as facet
    where facet.facet_contract_version = 1
    order by facet.dataset_kind,
      facet.id,
      facet.version desc,
      facet.modified_at desc,
      facet.state_code desc
  ), counts as (
    select pg_catalog.jsonb_build_object(
        'process', pg_catalog.count(*) filter (
          where latest.dataset_kind = 'process'
        ),
        'flow', pg_catalog.count(*) filter (
          where latest.dataset_kind = 'flow'
        ),
        'total', pg_catalog.count(*)
      ) as value,
      private.portal_timestamp_v1(
        pg_catalog.max(latest.modified_at)
      ) as latest_modified_at
    from latest
  ), uuid_candidates as materialized (
    (
      select 0 as preference,
        candidate.dataset_kind,
        candidate.id,
        candidate.version,
        private.portal_catalog_summary_label_v1(candidate.card) as label
      from private.portal_catalog_search_rows_v1 as candidate
      join latest
        on latest.dataset_kind = candidate.dataset_kind
       and latest.id = candidate.id
       and latest.version = candidate.version
      where candidate.dataset_kind = 'process'
        and pg_catalog.jsonb_array_length(
          private.portal_catalog_summary_label_v1(candidate.card)
        ) > 0
      order by candidate.id,
        candidate.version desc,
        candidate.modified_at desc,
        candidate.state_code desc
      limit 1
    )
    union all
    (
      select 1 as preference,
        candidate.dataset_kind,
        candidate.id,
        candidate.version,
        private.portal_catalog_summary_label_v1(candidate.card) as label
      from private.portal_catalog_search_rows_v1 as candidate
      join latest
        on latest.dataset_kind = candidate.dataset_kind
       and latest.id = candidate.id
       and latest.version = candidate.version
      where candidate.dataset_kind = 'flow'
        and pg_catalog.jsonb_array_length(
          private.portal_catalog_summary_label_v1(candidate.card)
        ) > 0
      order by candidate.id,
        candidate.version desc,
        candidate.modified_at desc,
        candidate.state_code desc
      limit 1
    )
  ), uuid_example as (
    select pg_catalog.jsonb_build_object(
      'queryKind', 'uuid',
      'datasetKind', candidate.dataset_kind,
      'query', candidate.id::text,
      'label', candidate.label
    ) as value
    from uuid_candidates as candidate
    order by candidate.preference
    limit 1
  ), cas_unique_values as materialized (
    select candidate.card ->> 'casNumber' as cas_number,
      pg_catalog.min(candidate.id::text)::uuid as id
    from private.portal_catalog_search_rows_v1 as candidate
    where candidate.dataset_kind = 'flow'
      and pg_catalog.jsonb_typeof(candidate.card -> 'casNumber') = 'string'
      and candidate.card ->> 'casNumber' ~
        '^[0-9]{2,7}-[0-9]{2}-[0-9]$'
      and pg_catalog.length(
        candidate.card ->> 'casNumber'
      ) between 7 and 12
      and private.portal_catalog_summary_valid_cas_v1(
        candidate.card ->> 'casNumber'
      )
    group by candidate.card ->> 'casNumber'
    having pg_catalog.count(*) = 1
    order by candidate.card ->> 'casNumber'
    limit 64
  ), cas_candidates as materialized (
    select candidate.dataset_kind,
      candidate.id,
      candidate.version,
      candidate.modified_at,
      candidate.state_code,
      unique_cas.cas_number,
      private.portal_catalog_summary_label_v1(candidate.card) as label
    from cas_unique_values as unique_cas
    join private.portal_catalog_search_rows_v1 as candidate
      on candidate.dataset_kind = 'flow'
     and candidate.id = unique_cas.id
     and candidate.card ->> 'casNumber' = unique_cas.cas_number
    join latest
      on latest.dataset_kind = candidate.dataset_kind
     and latest.id = candidate.id
     and latest.version = candidate.version
    where pg_catalog.jsonb_array_length(
      private.portal_catalog_summary_label_v1(candidate.card)
    ) > 0
    order by unique_cas.cas_number,
      candidate.id,
      candidate.version desc,
      candidate.modified_at desc,
      candidate.state_code desc
    limit 1
  ), cas_example as (
    select pg_catalog.jsonb_build_object(
      'queryKind', 'cas',
      'datasetKind', 'flow',
      'query', candidate.cas_number,
      'label', candidate.label
    ) as value
    from cas_candidates as candidate
    order by candidate.id,
      candidate.version desc,
      candidate.modified_at desc,
      candidate.state_code desc
    limit 1
  ), classification_candidates as materialized (
    (
      select 0 as preference,
        candidate.dataset_kind,
        candidate.id,
        candidate.version,
        candidate.modified_at,
        candidate.state_code,
        classification.ordinality,
        pg_catalog.btrim(classification.value ->> 'code') as code,
        private.portal_catalog_summary_label_v1(candidate.card) as label
      from private.portal_catalog_search_rows_v1 as candidate
      join latest
        on latest.dataset_kind = candidate.dataset_kind
       and latest.id = candidate.id
       and latest.version = candidate.version
      cross join lateral pg_catalog.jsonb_array_elements(
        candidate.card -> 'classifications'
      ) with ordinality as classification(value, ordinality)
      where candidate.dataset_kind = 'process'
        and pg_catalog.jsonb_typeof(
          candidate.card -> 'classifications'
        ) = 'array'
        and pg_catalog.jsonb_array_length(
          candidate.card -> 'classifications'
        ) > 0
        and pg_catalog.jsonb_typeof(classification.value) = 'object'
        and pg_catalog.jsonb_typeof(classification.value -> 'code') = 'string'
        and pg_catalog.length(
          pg_catalog.btrim(classification.value ->> 'code')
        ) between 4 and 128
        and pg_catalog.octet_length(
          pg_catalog.btrim(classification.value ->> 'code')
        ) <= 512
        and pg_catalog.btrim(
          classification.value ->> 'code'
        ) !~ '[[:cntrl:]]'
        and pg_catalog.jsonb_array_length(
          private.portal_catalog_summary_label_v1(candidate.card)
        ) > 0
      order by candidate.id,
        candidate.version desc,
        candidate.modified_at desc,
        candidate.state_code desc,
        classification.ordinality,
        pg_catalog.btrim(
          classification.value ->> 'code'
        ) collate pg_catalog."C"
      limit 1
    )
    union all
    (
      select 1 as preference,
        candidate.dataset_kind,
        candidate.id,
        candidate.version,
        candidate.modified_at,
        candidate.state_code,
        classification.ordinality,
        pg_catalog.btrim(classification.value ->> 'code') as code,
        private.portal_catalog_summary_label_v1(candidate.card) as label
      from private.portal_catalog_search_rows_v1 as candidate
      join latest
        on latest.dataset_kind = candidate.dataset_kind
       and latest.id = candidate.id
       and latest.version = candidate.version
      cross join lateral pg_catalog.jsonb_array_elements(
        candidate.card -> 'classifications'
      ) with ordinality as classification(value, ordinality)
      where candidate.dataset_kind = 'flow'
        and pg_catalog.jsonb_typeof(
          candidate.card -> 'classifications'
        ) = 'array'
        and pg_catalog.jsonb_array_length(
          candidate.card -> 'classifications'
        ) > 0
        and pg_catalog.jsonb_typeof(classification.value) = 'object'
        and pg_catalog.jsonb_typeof(classification.value -> 'code') = 'string'
        and pg_catalog.length(
          pg_catalog.btrim(classification.value ->> 'code')
        ) between 4 and 128
        and pg_catalog.octet_length(
          pg_catalog.btrim(classification.value ->> 'code')
        ) <= 512
        and pg_catalog.btrim(
          classification.value ->> 'code'
        ) !~ '[[:cntrl:]]'
        and pg_catalog.jsonb_array_length(
          private.portal_catalog_summary_label_v1(candidate.card)
        ) > 0
      order by candidate.id,
        candidate.version desc,
        candidate.modified_at desc,
        candidate.state_code desc,
        classification.ordinality,
        pg_catalog.btrim(
          classification.value ->> 'code'
        ) collate pg_catalog."C"
      limit 1
    )
  ), classification_example as (
    select pg_catalog.jsonb_build_object(
      'queryKind', 'classification',
      'datasetKind', candidate.dataset_kind,
      'query', candidate.code,
      'label', candidate.label
    ) as value
    from classification_candidates as candidate
    order by candidate.preference
    limit 1
  )
  select counts.value,
    counts.latest_modified_at,
    uuid_example.value,
    cas_example.value,
    classification_example.value
  into v_counts,
    v_latest_modified_at,
    v_uuid_example,
    v_cas_example,
    v_classification_example
  from counts
  left join uuid_example on true
  left join cas_example on true
  left join classification_example on true;

  select pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-catalog-summary.v1',
    'counts', v_counts,
    'latestModifiedAt', v_latest_modified_at,
    'examples', coalesce(pg_catalog.jsonb_agg(
      example.value order by example.ordinality
    ) filter (where example.value is not null), '[]'::jsonb)
  )
  into v_result
  from (values
    (1, v_uuid_example),
    (2, v_cas_example),
    (3, v_classification_example)
  ) as example(ordinality, value);

  if pg_catalog.octet_length(v_result::text) > 16384 then
    raise exception using
      errcode = '54000',
      message = 'Portal catalog summary exceeded its response budget';
  end if;

  return v_result;
exception
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$;

comment on function api.portal_catalog_summary_v1() is
  'Bounded latest-visible public Process/Flow counts, timestamp, and deterministic executable R1 examples; classification prefers non-broad Process evidence and CAS examples are unique across retained projection history.';

reset role;
revoke create on schema api from portal_public_executor;
set role portal_public_executor;

do $verify_portal_catalog_summary_selective_cas$
declare
  v_routine regprocedure := 'api.portal_catalog_summary_v1()'::regprocedure;
  v_summary jsonb;
  v_classification jsonb;
  v_cas jsonb;
  v_search jsonb;
begin
  if not exists (
       select 1
       from pg_catalog.pg_proc as routine
       where routine.oid = v_routine
         and routine.proowner = 'portal_public_executor'::regrole
         and routine.prosecdef
         and routine.provolatile = 's'
         and routine.proparallel = 'r'
         and coalesce(routine.proconfig, '{}'::text[]) @> array[
           'search_path=""',
           'statement_timeout=2s',
           'work_mem=32MB',
           'plan_cache_mode=force_custom_plan',
           'max_parallel_workers_per_gather=0',
           'jit=off',
           'row_security=on'
         ]::text[]
         and routine.prosrc ~ 'between 4 and 128'
         and routine.prosrc ~ 'dataset_kind = ''process'''
         and routine.prosrc ~ 'cas_unique_values as materialized'
         and routine.prosrc ~ 'limit 64'
         and routine.prosrc ~ 'having pg_catalog.count\(\*\) = 1'
     )
     or not pg_catalog.has_function_privilege(
       'anon', v_routine, 'EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'authenticated', v_routine, 'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role', v_routine, 'EXECUTE'
     ) then
    raise exception 'Portal catalog summary selective-CAS contract drifted'
      using errcode = '55000';
  end if;

  v_summary := api.portal_catalog_summary_v1();
  select example.value
  into v_classification
  from pg_catalog.jsonb_array_elements(v_summary -> 'examples') as example(value)
  where example.value ->> 'queryKind' = 'classification';

  select example.value
  into v_cas
  from pg_catalog.jsonb_array_elements(v_summary -> 'examples') as example(value)
  where example.value ->> 'queryKind' = 'cas';

  if v_cas is null
     and exists (
       select 1
       from private.portal_catalog_search_rows_v1 as candidate
       where candidate.dataset_kind = 'flow'
         and pg_catalog.jsonb_typeof(
           candidate.card -> 'casNumber'
         ) = 'string'
         and candidate.card ->> 'casNumber' ~
           '^[0-9]{2,7}-[0-9]{2}-[0-9]$'
         and private.portal_catalog_summary_valid_cas_v1(
           candidate.card ->> 'casNumber'
         )
     ) then
    raise exception 'Portal summary did not find a unique bounded CAS example'
      using errcode = '55000';
  end if;

  if v_classification is not null then
    if pg_catalog.length(v_classification ->> 'query') < 4 then
      raise exception 'Portal summary emitted a broad classification example'
        using errcode = '55000';
    end if;
    if v_classification ->> 'datasetKind' = 'process' then
      v_search := api.portal_search_processes_v1(
        v_classification ->> 'query', '{}'::jsonb, 'relevance', null, 20
      );
    else
      v_search := api.portal_search_flows_v1(
        v_classification ->> 'query', '{}'::jsonb, 'relevance', null, 20
      );
    end if;
    if pg_catalog.jsonb_array_length(v_search -> 'items') = 0 then
      raise exception 'Portal summary classification example is not executable'
        using errcode = '55000';
    end if;
  end if;

  if v_cas is not null then
    v_search := api.portal_search_flows_v1(
      v_cas ->> 'query', '{}'::jsonb, 'relevance', null, 20
    );
    if pg_catalog.jsonb_array_length(v_search -> 'items') <> 1 then
      raise exception 'Portal summary CAS example is not selectively executable'
        using errcode = '55000';
    end if;
  end if;
end
$verify_portal_catalog_summary_selective_cas$;

reset role;
revoke portal_public_executor from postgres;

commit;
