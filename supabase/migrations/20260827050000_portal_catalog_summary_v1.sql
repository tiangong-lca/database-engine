-- Issue #533: expose one bounded anonymous catalog summary from the
-- synchronized Portal projections. The function returns only latest-visible
-- counts, the latest public projection timestamp, and at most one executable
-- UUID/CAS/classification example. It never scans raw dataset tables.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '30s';

do $portal_catalog_summary_prerequisite_guard$
begin
  if pg_catalog.to_regprocedure('api.portal_catalog_summary_v1()') is not null
     or pg_catalog.to_regprocedure(
       'private.assert_portal_catalog_projection_contract_v1()'
     ) is null
     or pg_catalog.to_regprocedure(
       'private.assert_portal_catalog_facet_contract_v1()'
     ) is null
     or pg_catalog.to_regclass(
       'private.portal_catalog_search_rows_v1'
     ) is null
     or pg_catalog.to_regclass(
       'private.portal_catalog_facet_rows_v1'
     ) is null
     or not exists (
       select 1
       from pg_catalog.pg_roles as role
       where role.rolname = 'portal_public_executor'
         and not role.rolcanlogin
         and not role.rolinherit
         and not role.rolbypassrls
         and not role.rolsuper
     ) then
    raise exception 'Portal catalog summary prerequisites are unsafe'
      using errcode = '55000';
  end if;
end
$portal_catalog_summary_prerequisite_guard$;

grant portal_public_executor to postgres;
grant create on schema api to portal_public_executor;
set role portal_public_executor;

select private.assert_portal_catalog_projection_contract_v1();
select private.assert_portal_catalog_facet_contract_v1();

create function api.portal_catalog_summary_v1()
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
  v_result jsonb;
begin
  perform private.assert_portal_catalog_projection_contract_v1();
  perform private.assert_portal_catalog_facet_contract_v1();

  with latest_counts as materialized (
    select distinct on (facet.dataset_kind, facet.id)
      facet.dataset_kind,
      facet.id,
      facet.version,
      facet.state_code,
      facet.modified_at
    from private.portal_catalog_facet_rows_v1 as facet
    where facet.facet_contract_version = 1
    order by facet.dataset_kind,
      facet.id,
      facet.version desc,
      facet.modified_at desc,
      facet.state_code desc
  ), catalog_counts as materialized (
    select
      pg_catalog.count(*) filter (
        where latest_counts.dataset_kind = 'process'
      ) as process_count,
      pg_catalog.count(*) filter (
        where latest_counts.dataset_kind = 'flow'
      ) as flow_count,
      pg_catalog.count(*) as total_count,
      pg_catalog.max(latest_counts.modified_at) as latest_modified_at
    from latest_counts
  ), uuid_example as materialized (
    select pg_catalog.jsonb_build_object(
      'queryKind', 'uuid',
      'datasetKind', latest.dataset_kind,
      'query', latest.id::text,
      'label', bounded_label.value
    ) as value
    from (
      select preferred.dataset_kind,
        preferred.id,
        preferred.version
      from (
        (
          select 0 as preference,
            facet.dataset_kind,
            facet.id,
            facet.version
          from private.portal_catalog_facet_rows_v1 as facet
          where facet.facet_contract_version = 1
            and facet.dataset_kind = 'process'
          order by facet.id,
            facet.version desc,
            facet.modified_at desc,
            facet.state_code desc
          limit 1
        )
        union all
        (
          select 1 as preference,
            facet.dataset_kind,
            facet.id,
            facet.version
          from private.portal_catalog_facet_rows_v1 as facet
          where facet.facet_contract_version = 1
            and facet.dataset_kind = 'flow'
          order by facet.id,
            facet.version desc,
            facet.modified_at desc,
            facet.state_code desc
          limit 1
        )
      ) as preferred
      order by preferred.preference
      limit 1
    ) as latest
    join private.portal_catalog_search_rows_v1 as projection
      on projection.dataset_kind = latest.dataset_kind
     and projection.id = latest.id
     and projection.version = latest.version
    cross join lateral (
      select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'language', label_item.language,
        'value', label_item.label_value
      ) order by label_item.preference,
          pg_catalog.lower(label_item.language) collate pg_catalog."C",
          label_item.label_value collate pg_catalog."C",
          label_item.ordinality), '[]'::jsonb) as value
      from (
        select
          pg_catalog.btrim(item.value ->> 'language') as language,
          pg_catalog.btrim(item.value ->> 'value') as label_value,
          item.ordinality,
          case pg_catalog.lower(pg_catalog.btrim(item.value ->> 'language'))
            when 'zh-cn' then 0
            when 'en' then 1
            else 2
          end as preference
        from pg_catalog.jsonb_array_elements(
          case pg_catalog.jsonb_typeof(projection.card -> 'names')
            when 'array' then projection.card -> 'names'
            else '[]'::jsonb
          end
        ) with ordinality as item(value, ordinality)
        where pg_catalog.jsonb_typeof(item.value) = 'object'
          and pg_catalog.jsonb_typeof(item.value -> 'language') = 'string'
          and pg_catalog.jsonb_typeof(item.value -> 'value') = 'string'
          and pg_catalog.btrim(item.value ->> 'language') ~
            '^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$'
          and pg_catalog.length(
            pg_catalog.btrim(item.value ->> 'language')
          ) <= 35
          and nullif(pg_catalog.btrim(item.value ->> 'value'), '') is not null
          and pg_catalog.length(
            pg_catalog.btrim(item.value ->> 'value')
          ) <= 160
          and pg_catalog.octet_length(
            pg_catalog.btrim(item.value ->> 'value')
          ) <= 640
        order by preference,
          pg_catalog.lower(
            pg_catalog.btrim(item.value ->> 'language')
          ) collate pg_catalog."C",
          pg_catalog.btrim(item.value ->> 'value') collate pg_catalog."C",
          item.ordinality
        limit 2
      ) as label_item
    ) as bounded_label
    where pg_catalog.jsonb_array_length(bounded_label.value) > 0
    limit 1
  ), cas_example as materialized (
    select pg_catalog.jsonb_build_object(
      'queryKind', 'cas',
      'datasetKind', 'flow',
      'query', projection.card ->> 'casNumber',
      'label', bounded_label.value
    ) as value
    from (
      select distinct on (facet.id)
        facet.dataset_kind,
        facet.id,
        facet.version
      from private.portal_catalog_facet_rows_v1 as facet
      where facet.facet_contract_version = 1
        and facet.dataset_kind = 'flow'
      order by facet.id,
        facet.version desc,
        facet.modified_at desc,
        facet.state_code desc
    ) as latest
    join private.portal_catalog_search_rows_v1 as projection
      on projection.dataset_kind = latest.dataset_kind
     and projection.id = latest.id
     and projection.version = latest.version
    cross join lateral (
      select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'language', label_item.language,
        'value', label_item.label_value
      ) order by label_item.preference,
          pg_catalog.lower(label_item.language) collate pg_catalog."C",
          label_item.label_value collate pg_catalog."C",
          label_item.ordinality), '[]'::jsonb) as value
      from (
        select
          pg_catalog.btrim(item.value ->> 'language') as language,
          pg_catalog.btrim(item.value ->> 'value') as label_value,
          item.ordinality,
          case pg_catalog.lower(pg_catalog.btrim(item.value ->> 'language'))
            when 'zh-cn' then 0
            when 'en' then 1
            else 2
          end as preference
        from pg_catalog.jsonb_array_elements(
          case pg_catalog.jsonb_typeof(projection.card -> 'names')
            when 'array' then projection.card -> 'names'
            else '[]'::jsonb
          end
        ) with ordinality as item(value, ordinality)
        where pg_catalog.jsonb_typeof(item.value) = 'object'
          and pg_catalog.jsonb_typeof(item.value -> 'language') = 'string'
          and pg_catalog.jsonb_typeof(item.value -> 'value') = 'string'
          and pg_catalog.btrim(item.value ->> 'language') ~
            '^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$'
          and pg_catalog.length(
            pg_catalog.btrim(item.value ->> 'language')
          ) <= 35
          and nullif(pg_catalog.btrim(item.value ->> 'value'), '') is not null
          and pg_catalog.length(
            pg_catalog.btrim(item.value ->> 'value')
          ) <= 160
          and pg_catalog.octet_length(
            pg_catalog.btrim(item.value ->> 'value')
          ) <= 640
        order by preference,
          pg_catalog.lower(
            pg_catalog.btrim(item.value ->> 'language')
          ) collate pg_catalog."C",
          pg_catalog.btrim(item.value ->> 'value') collate pg_catalog."C",
          item.ordinality
        limit 2
      ) as label_item
    ) as bounded_label
    where latest.dataset_kind = 'flow'
      and projection.card ->> 'casNumber' ~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$'
      and pg_catalog.right(projection.card ->> 'casNumber', 1)::integer = (
        select pg_catalog.mod(
          pg_catalog.sum(
            digit.value::integer * digit.ordinality::integer
          ),
          10
        )
        from pg_catalog.regexp_split_to_table(
          pg_catalog.reverse(pg_catalog.replace(
            pg_catalog.left(
              projection.card ->> 'casNumber',
              pg_catalog.length(projection.card ->> 'casNumber') - 2
            ),
            '-',
            ''
          )),
          ''
        ) with ordinality as digit(value, ordinality)
      )
      and pg_catalog.jsonb_array_length(bounded_label.value) > 0
    order by latest.id
    limit 1
  ), classification_example as materialized (
    select pg_catalog.jsonb_build_object(
      'queryKind', 'classification',
      'datasetKind', latest.dataset_kind,
      'query', pg_catalog.btrim(classification.value ->> 'code'),
      'label', bounded_label.value
    ) as value
    from (
      select distinct on (facet.dataset_kind, facet.id)
        facet.dataset_kind,
        facet.id,
        facet.version
      from private.portal_catalog_facet_rows_v1 as facet
      where facet.facet_contract_version = 1
      order by facet.dataset_kind,
        facet.id,
        facet.version desc,
        facet.modified_at desc,
        facet.state_code desc
    ) as latest
    join private.portal_catalog_search_rows_v1 as projection
      on projection.dataset_kind = latest.dataset_kind
     and projection.id = latest.id
     and projection.version = latest.version
    cross join lateral pg_catalog.jsonb_array_elements(
      case pg_catalog.jsonb_typeof(projection.card -> 'classifications')
        when 'array' then projection.card -> 'classifications'
        else '[]'::jsonb
      end
    ) with ordinality as classification(value, ordinality)
    cross join lateral (
      select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'language', label_item.language,
        'value', label_item.label_value
      ) order by label_item.preference,
          pg_catalog.lower(label_item.language) collate pg_catalog."C",
          label_item.label_value collate pg_catalog."C",
          label_item.ordinality), '[]'::jsonb) as value
      from (
        select
          pg_catalog.btrim(item.value ->> 'language') as language,
          pg_catalog.btrim(item.value ->> 'value') as label_value,
          item.ordinality,
          case pg_catalog.lower(pg_catalog.btrim(item.value ->> 'language'))
            when 'zh-cn' then 0
            when 'en' then 1
            else 2
          end as preference
        from pg_catalog.jsonb_array_elements(
          case pg_catalog.jsonb_typeof(projection.card -> 'names')
            when 'array' then projection.card -> 'names'
            else '[]'::jsonb
          end
        ) with ordinality as item(value, ordinality)
        where pg_catalog.jsonb_typeof(item.value) = 'object'
          and pg_catalog.jsonb_typeof(item.value -> 'language') = 'string'
          and pg_catalog.jsonb_typeof(item.value -> 'value') = 'string'
          and pg_catalog.btrim(item.value ->> 'language') ~
            '^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$'
          and pg_catalog.length(
            pg_catalog.btrim(item.value ->> 'language')
          ) <= 35
          and nullif(pg_catalog.btrim(item.value ->> 'value'), '') is not null
          and pg_catalog.length(
            pg_catalog.btrim(item.value ->> 'value')
          ) <= 160
          and pg_catalog.octet_length(
            pg_catalog.btrim(item.value ->> 'value')
          ) <= 640
        order by preference,
          pg_catalog.lower(
            pg_catalog.btrim(item.value ->> 'language')
          ) collate pg_catalog."C",
          pg_catalog.btrim(item.value ->> 'value') collate pg_catalog."C",
          item.ordinality
        limit 2
      ) as label_item
    ) as bounded_label
    where pg_catalog.jsonb_typeof(classification.value) = 'object'
      and pg_catalog.jsonb_typeof(classification.value -> 'code') = 'string'
      and nullif(
        pg_catalog.btrim(classification.value ->> 'code'), ''
      ) is not null
      and pg_catalog.length(
        pg_catalog.btrim(classification.value ->> 'code')
      ) <= 128
      and pg_catalog.octet_length(
        pg_catalog.btrim(classification.value ->> 'code')
      ) <= 512
      and pg_catalog.btrim(classification.value ->> 'code') !~ '[[:cntrl:]]'
      and pg_catalog.jsonb_array_length(bounded_label.value) > 0
    order by latest.dataset_kind collate pg_catalog."C",
      latest.id,
      classification.ordinality,
      pg_catalog.btrim(
        classification.value ->> 'code'
      ) collate pg_catalog."C"
    limit 1
  ), ordered_examples as materialized (
    select example.ordinality, example.value
    from (values
      (1, (select uuid_example.value from uuid_example)),
      (2, (select cas_example.value from cas_example)),
      (3, (select classification_example.value from classification_example))
    ) as example(ordinality, value)
    where example.value is not null
  )
  select pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-catalog-summary.v1',
    'counts', pg_catalog.jsonb_build_object(
      'process', catalog_counts.process_count,
      'flow', catalog_counts.flow_count,
      'total', catalog_counts.total_count
    ),
    'latestModifiedAt', private.portal_timestamp_v1(
      catalog_counts.latest_modified_at
    ),
    'examples', coalesce((
      select pg_catalog.jsonb_agg(
        ordered_examples.value order by ordered_examples.ordinality
      )
      from ordered_examples
    ), '[]'::jsonb)
  )
  into v_result
  from catalog_counts;

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
  'Bounded latest-visible public Process/Flow counts, timestamp, and deterministic executable R1 examples from synchronized Portal projections.';

revoke all on function api.portal_catalog_summary_v1()
  from public, anon, authenticated, service_role;
grant execute on function api.portal_catalog_summary_v1()
  to anon, authenticated;

reset role;
revoke create on schema api from portal_public_executor;
revoke portal_public_executor from postgres;

insert into private.api_capability_grants (
  routine_identity,
  capability_id,
  allow_anon,
  allow_authenticated,
  allow_service_role
)
values (
  'api.portal_catalog_summary_v1()',
  'PORTAL-CATALOG-01',
  true,
  true,
  false
);

do $verify_portal_catalog_summary_v1$
declare
  v_routine regprocedure := 'api.portal_catalog_summary_v1()'::regprocedure;
begin
  if (
    select not (
      routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and routine.provolatile = 's'
      and routine.proparallel = 'r'
      and routine.prorettype = 'pg_catalog.jsonb'::regtype
      and coalesce(routine.proconfig, '{}'::text[]) @> array[
        'search_path=""',
        'statement_timeout=2s',
        'work_mem=32MB',
        'plan_cache_mode=force_custom_plan',
        'max_parallel_workers_per_gather=0',
        'jit=off',
        'row_security=on'
      ]::text[]
    )
    from pg_catalog.pg_proc as routine
    where routine.oid = v_routine
  ) is not false
  or (
    select count(*)
    from private.api_capability_grants as manifest
    where manifest.routine_identity = 'api.portal_catalog_summary_v1()'
      and manifest.capability_id = 'PORTAL-CATALOG-01'
      and manifest.allow_anon
      and manifest.allow_authenticated
      and not manifest.allow_service_role
  ) <> 1
  or not pg_catalog.has_function_privilege('anon', v_routine, 'EXECUTE')
  or not pg_catalog.has_function_privilege(
    'authenticated', v_routine, 'EXECUTE'
  )
  or pg_catalog.has_function_privilege('service_role', v_routine, 'EXECUTE')
  or exists (
    select 1
    from pg_catalog.aclexplode(
      coalesce(
        (select routine.proacl from pg_catalog.pg_proc as routine
         where routine.oid = v_routine),
        pg_catalog.acldefault(
          'f',
          (select routine.proowner from pg_catalog.pg_proc as routine
           where routine.oid = v_routine)
        )
      )
    ) as acl
    where acl.grantee = 0
      and acl.privilege_type = 'EXECUTE'
  )
  or (
    select routine.prosrc !~ 'portal_catalog_facet_rows_v1'
      or routine.prosrc !~ 'portal_catalog_search_rows_v1'
      or routine.prosrc ~
        'public\.(processes|flows)|search_text|extracted_md|embedding_ft|team_id|user_id|review_id|privateLocator|objectLocator'
    from pg_catalog.pg_proc as routine
    where routine.oid = v_routine
  ) then
    raise exception 'Portal catalog summary contract drifted'
      using errcode = '55000';
  end if;
end
$verify_portal_catalog_summary_v1$;

commit;
