-- Issue #532: hydrate the final bounded Search/Hybrid page by exact source
-- identity. The candidate projection, lexical/vector indexes, Facets, source
-- triggers, ordering, cursors, ranks, limits, and legacy Hybrid routines are
-- unchanged.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

grant api_internal_executor to postgres;
set role api_internal_executor;
select private.assert_portal_catalog_projection_contract_v1();
select private.assert_portal_catalog_facet_contract_v1();
reset role;
revoke api_internal_executor from postgres;

do $portal_card_context_role_guard$
begin
  if not exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'portal_public_executor'
      and not rolcanlogin
      and not rolinherit
      and not rolbypassrls
      and not rolsuper
      and not rolreplication
  ) then
    raise exception 'Portal card context executor prerequisite is unsafe'
      using errcode = '42501';
  end if;
end
$portal_card_context_role_guard$;

create temporary table portal_card_context_api_before (
  routine_identity text primary key,
  owner_name text not null,
  language_name text not null,
  volatility "char" not null,
  parallel_safety "char" not null,
  security_definer boolean not null,
  result_type text not null,
  config text[] not null,
  acl_text text not null,
  source text not null
) on commit drop;

insert into portal_card_context_api_before
select expected.routine_identity,
  pg_catalog.pg_get_userbyid(routine.proowner),
  language.lanname,
  routine.provolatile,
  routine.proparallel,
  routine.prosecdef,
  pg_catalog.pg_get_function_result(routine.oid),
  coalesce(routine.proconfig, '{}'::text[]),
  coalesce(routine.proacl::text, ''),
  routine.prosrc
from (
  values
    ('api.portal_search_processes_v1(text,jsonb,text,text,integer)'::text),
    ('api.portal_search_flows_v1(text,jsonb,text,text,integer)'),
    ('api.portal_hybrid_search_v1(text,text[],text,jsonb,integer)')
) as expected(routine_identity)
join pg_catalog.pg_proc as routine
  on routine.oid = pg_catalog.to_regprocedure(expected.routine_identity)
join pg_catalog.pg_language as language
  on language.oid = routine.prolang;

do $portal_card_context_api_guard$
begin
  if (select count(*) from portal_card_context_api_before) <> 3
     or exists (
       select 1
       from portal_card_context_api_before
       where owner_name <> 'portal_public_executor'
          or language_name <> 'plpgsql'
          or volatility <> 's'
          or parallel_safety <> case
            when routine_identity =
              'api.portal_hybrid_search_v1(text,text[],text,jsonb,integer)'
              then 'r'::"char"
            else 'u'::"char"
          end
          or not security_definer
          or result_type <> 'jsonb'
          or not config @> array[
            'search_path=""',
            'statement_timeout=8s'
          ]::text[]
          or source ~ 'portal_decorate_card_context_v1'
     )
     or not exists (
       select 1
       from portal_card_context_api_before
       where routine_identity =
           'api.portal_search_processes_v1(text,jsonb,text,text,integer)'
         and source ~ 'portal_lcia_decorate_item_page_v1'
         and source ~ 'portal_search_v1'
     )
     or not exists (
       select 1
       from portal_card_context_api_before
       where routine_identity =
           'api.portal_search_flows_v1(text,jsonb,text,text,integer)'
         and source ~ 'portal_search_v1'
     )
     or not exists (
       select 1
       from portal_card_context_api_before
       where routine_identity =
           'api.portal_hybrid_search_v1(text,text[],text,jsonb,integer)'
         and source ~ 'portal_projection_hybrid_search_v1_impl'
         and source ~ 'portal_lcia_decorate_item_page_v1'
     ) then
    raise exception 'Portal Search/Hybrid wrapper prerequisite drifted'
      using errcode = '55000';
  end if;
end
$portal_card_context_api_guard$;

create temporary table portal_card_context_writer_before (
  object_identity text primary key,
  definition text not null,
  owner_name text,
  config text[],
  acl_text text
) on commit drop;

insert into portal_card_context_writer_before
select
  'function:private.sync_portal_catalog_search_row_v1()',
  pg_catalog.pg_get_functiondef(routine.oid),
  pg_catalog.pg_get_userbyid(routine.proowner),
  coalesce(routine.proconfig, '{}'::text[]),
  coalesce(routine.proacl::text, '')
from pg_catalog.pg_proc as routine
where routine.oid =
  'private.sync_portal_catalog_search_row_v1()'::regprocedure;

insert into portal_card_context_writer_before
select
  'trigger:' || trigger.tgrelid::regclass::text || ':' || trigger.tgname,
  pg_catalog.pg_get_triggerdef(trigger.oid),
  null,
  null,
  null
from pg_catalog.pg_trigger as trigger
where not trigger.tgisinternal
  and (
    (
      trigger.tgrelid in (
        'public.processes'::regclass,
        'public.flows'::regclass
      )
      and trigger.tgname = 'portal_catalog_projection_content_sync_v1'
    )
    or (
      trigger.tgrelid =
        'private.portal_catalog_search_rows_v1'::regclass
      and trigger.tgname = 'portal_catalog_facet_sync_v1'
    )
  );

do $portal_card_context_writer_snapshot_guard$
begin
  if (select count(*) from portal_card_context_writer_before) <> 4 then
    raise exception 'Portal source writer snapshot is incomplete'
      using errcode = '55000';
  end if;
end
$portal_card_context_writer_snapshot_guard$;

grant portal_public_executor to postgres;
grant create on schema private, api to portal_public_executor;
set role portal_public_executor;

create function private.portal_card_context_v1(
  p_kind text,
  p_state_code integer,
  p_json jsonb
)
returns jsonb
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set row_security = 'on'
as $function$
declare
  v_information jsonb;
  v_modelling jsonb;
  v_reference_name jsonb := '[]'::jsonb;
  v_functional_unit jsonb := 'null'::jsonb;
  v_technology jsonb := '[]'::jsonb;
  v_source jsonb;
  v_review_status jsonb := 'null'::jsonb;
  v_flow_property jsonb;
begin
  if p_state_code not in (100, 200)
     or pg_catalog.jsonb_typeof(p_json) <> 'object' then
    return null;
  end if;

  if p_kind = 'process'
     and pg_catalog.jsonb_typeof(p_json -> 'processDataSet') = 'object' then
    v_information := p_json #> '{processDataSet,processInformation}';
    v_modelling := p_json #> '{processDataSet,modellingAndValidation}';
    v_reference_name := private.portal_process_reference_product_v1(p_json);

    -- Functional-unit amount/unit are public metadata, not permission to read
    -- Exchanges. Reuse the exact open support-chain validator for both public
    -- Process states, then emit null unless the evidence is complete.
    v_functional_unit := private.portal_process_functional_unit_v1(100, p_json);
    if pg_catalog.jsonb_typeof(v_functional_unit) <> 'object'
       or pg_catalog.jsonb_typeof(
         v_functional_unit -> 'amount'
       ) <> 'string'
       or pg_catalog.jsonb_typeof(
         v_functional_unit -> 'unit'
       ) <> 'string' then
      v_functional_unit := 'null'::jsonb;
    end if;

    v_technology := private.portal_localized_text_v1(
      v_information #> '{technology,technologyDescriptionAndIncludedProcesses}'
    ) || private.portal_localized_text_v1(
      v_information #> '{technology,technologicalApplicability}'
    );
    select coalesce(
      pg_catalog.to_jsonb(nullif(
        private.portal_scalar_text_v1(review_item -> '@type'),
        ''
      )),
      'null'::jsonb
    )
    into v_review_status
    from private.portal_json_items_v1(
      v_modelling #> '{validation,review}'
    ) as review_item
    limit 1;
    v_review_status := coalesce(v_review_status, 'null'::jsonb);
  elsif p_kind = 'flow'
     and pg_catalog.jsonb_typeof(p_json -> 'flowDataSet') = 'object' then
    v_flow_property := private.portal_reference_flowproperty_v1(p_json);
    v_reference_name := coalesce(
      v_flow_property -> 'name',
      '[]'::jsonb
    );
  else
    return null;
  end if;

  v_source := private.portal_source_v1(p_kind, p_json);
  if pg_catalog.jsonb_typeof(v_reference_name) <> 'array'
     or pg_catalog.jsonb_typeof(v_technology) <> 'array'
     or pg_catalog.jsonb_typeof(v_source) <> 'object' then
    return null;
  end if;

  return pg_catalog.jsonb_build_object(
    'reference', pg_catalog.jsonb_build_object(
      'kind', case p_kind
        when 'process' then 'reference_product'
        else 'reference_flow_property'
      end,
      'name', v_reference_name
    ),
    'functionalUnit', v_functional_unit,
    'technology', v_technology,
    'source', v_source,
    'quality', pg_catalog.jsonb_build_object(
      'reviewStatus', v_review_status
    )
  );
end
$function$;

create function private.portal_decorate_card_context_v1(p_page jsonb)
returns jsonb
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
set row_security = 'on'
as $function$
declare
  v_schema_version text := p_page ->> 'schemaVersion';
  v_kind text := p_page ->> 'kind';
  v_expected integer;
  v_actual integer;
  v_items jsonb;
begin
  perform private.assert_portal_card_context_contract_v1();

  if pg_catalog.jsonb_typeof(p_page) <> 'object'
     or pg_catalog.jsonb_typeof(p_page -> 'items') <> 'array'
     or v_schema_version not in (
       'portal.public-search-page.v1',
       'portal.public-hybrid-candidate-page.v1'
     )
     or v_kind not in ('process', 'flow') then
    raise exception 'Portal card context page is invalid'
      using errcode = '55000';
  end if;
  v_expected := pg_catalog.jsonb_array_length(p_page -> 'items');
  if v_expected > (
    case
      when v_schema_version = 'portal.public-search-page.v1' then 50
      else 20
    end
  ) then
    raise exception 'Portal card context page exceeds its fixed bound'
      using errcode = '54000';
  end if;

  if v_kind = 'process' then
    with input as materialized (
      select item.value, item.ordinality,
        case
          when item.value #>> '{key,id}'
            ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            then (item.value #>> '{key,id}')::uuid
          else null
        end as id,
        item.value #>> '{key,version}' as version
      from pg_catalog.jsonb_array_elements(p_page -> 'items')
        with ordinality as item(value, ordinality)
      where pg_catalog.jsonb_typeof(item.value) = 'object'
        and item.value #>> '{key,kind}' = 'process'
        and item.value #>> '{key,version}' ~ '^\d{2}\.\d{2}\.\d{3}$'
    ), hydrated as materialized (
      select input.value, input.ordinality,
        private.portal_card_context_v1(
          'process', source.state_code, source.json
        ) as card_context
      from input
      join public.processes as source
        on source.id = input.id
       and source.version::text = input.version
       and source.state_code in (100, 200)
    )
    select count(*), coalesce(
      pg_catalog.jsonb_agg(
        hydrated.value || pg_catalog.jsonb_build_object(
          'context', hydrated.card_context
        ) order by hydrated.ordinality
      ),
      '[]'::jsonb
    )
    into v_actual, v_items
    from hydrated
    where pg_catalog.jsonb_typeof(hydrated.card_context) = 'object';
  else
    with input as materialized (
      select item.value, item.ordinality,
        case
          when item.value #>> '{key,id}'
            ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            then (item.value #>> '{key,id}')::uuid
          else null
        end as id,
        item.value #>> '{key,version}' as version
      from pg_catalog.jsonb_array_elements(p_page -> 'items')
        with ordinality as item(value, ordinality)
      where pg_catalog.jsonb_typeof(item.value) = 'object'
        and item.value #>> '{key,kind}' = 'flow'
        and item.value #>> '{key,version}' ~ '^\d{2}\.\d{2}\.\d{3}$'
    ), hydrated as materialized (
      select input.value, input.ordinality,
        private.portal_card_context_v1(
          'flow', source.state_code, source.json
        ) as card_context
      from input
      join public.flows as source
        on source.id = input.id
       and source.version::text = input.version
       and source.state_code in (100, 200)
    )
    select count(*), coalesce(
      pg_catalog.jsonb_agg(
        hydrated.value || pg_catalog.jsonb_build_object(
          'context', hydrated.card_context
        ) order by hydrated.ordinality
      ),
      '[]'::jsonb
    )
    into v_actual, v_items
    from hydrated
    where pg_catalog.jsonb_typeof(hydrated.card_context) = 'object';
  end if;

  if v_actual is distinct from v_expected then
    raise exception 'Portal card context exact-key hydration failed'
      using errcode = '55000';
  end if;
  return pg_catalog.jsonb_set(p_page, '{items}', v_items, false);
end
$function$;

create function private.portal_card_context_manifest_sha256_v1()
returns text
language sql
stable
parallel restricted
security definer
set search_path = ''
set row_security = 'on'
as $function$
  with expected(identity) as (
    values
      ('private.portal_card_context_v1(text,integer,jsonb)'::text),
      ('private.portal_decorate_card_context_v1(jsonb)'),
      ('private.portal_process_reference_product_v1(jsonb)'),
      ('private.portal_process_functional_unit_v1(integer,jsonb)'),
      ('private.portal_exchange_support_v1(integer,jsonb,jsonb)'),
      ('private.portal_reference_flowproperty_v1(jsonb)'),
      ('private.portal_localized_text_v1(jsonb)'),
      ('private.portal_scalar_text_v1(jsonb)'),
      ('private.portal_json_items_v1(jsonb)'),
      ('private.portal_source_v1(text,jsonb)'),
      ('private.portal_capabilities_v1(text,integer,jsonb)'),
      ('private.portal_canonical_decimal_v1(text)'),
      ('private.portal_flow_kind_v1(text)'),
      ('private.portal_support_capabilities_v1(text,integer)'),
      ('private.portal_classifications_v1(jsonb)'),
      ('private.portal_publication_root_v1(text,jsonb)'),
      ('private.portal_access_restrictions_open_v1(jsonb)')
  ), manifest_entries as (
    select expected.identity,
      pg_catalog.jsonb_build_object(
        'identity', expected.identity,
        'definition', pg_catalog.pg_get_functiondef(routine.oid),
        'owner', pg_catalog.pg_get_userbyid(routine.proowner),
        'language', language.lanname,
        'volatility', routine.provolatile,
        'parallel', routine.proparallel,
        'securityDefiner', routine.prosecdef,
        'config', coalesce(
          pg_catalog.to_jsonb(routine.proconfig),
          'null'::jsonb
        )
      )::text as entry
    from expected
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(expected.identity)
    join pg_catalog.pg_language as language
      on language.oid = routine.prolang
  )
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.string_agg(
          manifest_entries.entry,
          E'\n'
          order by manifest_entries.identity
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  from manifest_entries
$function$;

create function private.assert_portal_card_context_contract_v1()
returns void
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set row_security = 'on'
as $function$
declare
  v_expected_digest constant text :=
    'e0516d5f3a641d26221a5c44b92a2e7a87cab125e9145e8141074d9bc2af39fa';
begin
  perform private.assert_portal_catalog_projection_contract_v1();
  if private.portal_card_context_manifest_sha256_v1()
       is distinct from v_expected_digest then
    raise exception using
      errcode = '55000',
      message = 'Portal card context derivation contract drifted';
  end if;
end
$function$;

comment on function private.portal_card_context_v1(text, integer, jsonb) is
  'Builds the exact five-field public-safe Search/Hybrid context from one selected source row and reviewed public-dataset allowlist helpers.';
comment on function private.portal_decorate_card_context_v1(jsonb) is
  'Hydrates at most 50 Search or 20 Hybrid items by exact public source identity after candidate ordering and limit.';
comment on function private.portal_card_context_manifest_sha256_v1() is
  'Live SHA-256 for the exact context/decorator and transitive allowlist helper closure.';
comment on function private.assert_portal_card_context_contract_v1() is
  'Fails closed before card-context hydration when its live derivation closure drifts.';

revoke all on function private.portal_card_context_v1(text, integer, jsonb)
from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.portal_decorate_card_context_v1(jsonb)
from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.portal_card_context_manifest_sha256_v1()
from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.assert_portal_card_context_contract_v1()
from public, anon, authenticated, service_role, api_internal_executor;

create or replace function api.portal_search_processes_v1(
  p_query text,
  p_filters jsonb default '{}'::jsonb,
  p_sort text default 'relevance',
  p_cursor text default null,
  p_limit integer default 20
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
set statement_timeout = '8s'
as $function$
begin
  return private.portal_decorate_card_context_v1(
    private.portal_lcia_decorate_item_page_v1(
      private.portal_search_v1(
        'process', p_query, p_filters, p_sort, p_cursor, p_limit
      )
    )
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$;

create or replace function api.portal_search_flows_v1(
  p_query text,
  p_filters jsonb default '{}'::jsonb,
  p_sort text default 'relevance',
  p_cursor text default null,
  p_limit integer default 20
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
set statement_timeout = '8s'
as $function$
begin
  return private.portal_decorate_card_context_v1(
    private.portal_search_v1(
      'flow', p_query, p_filters, p_sort, p_cursor, p_limit
    )
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$;

create or replace function api.portal_hybrid_search_v1(
  p_kind text,
  p_query_terms text[],
  p_query_embedding text,
  p_filters jsonb,
  p_limit integer
)
returns jsonb
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
as $function$
declare
  v_input jsonb;
  v_page jsonb;
begin
  v_input := private.portal_public_hybrid_input_v1(
    p_kind,
    p_query_terms,
    p_query_embedding,
    p_filters,
    p_limit
  );
  v_page := private.portal_decorate_card_context_v1(
    private.portal_lcia_decorate_item_page_v1(
      private.portal_projection_hybrid_search_v1_impl(
        v_input ->> 'kind',
        array(
          select term.value
          from pg_catalog.jsonb_array_elements_text(v_input -> 'queryTerms')
            with ordinality as term(value, ordinality)
          order by term.ordinality
        ),
        (v_input ->> 'queryEmbedding')::extensions.vector(1024),
        v_input -> 'filters',
        (v_input ->> 'limit')::integer,
        v_input ->> 'queryFingerprint'
      )
    )
  );
  if v_page is null
     or pg_catalog.octet_length(
       pg_catalog.convert_to(v_page::text, 'UTF8')
     ) > 524288 then
    raise exception using
      errcode = '54000',
      message = 'portal hybrid response too large';
  end if;
  return v_page;
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal hybrid unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal hybrid unavailable';
end
$function$;

reset role;
revoke create on schema private, api from portal_public_executor;
revoke portal_public_executor from postgres;

do $verify_portal_card_context_cutover$
begin
  if (
    select count(*)
    from pg_catalog.pg_proc as routine
    where routine.oid in (
      'private.portal_card_context_v1(text,integer,jsonb)'::regprocedure,
      'private.portal_decorate_card_context_v1(jsonb)'::regprocedure,
      'private.portal_card_context_manifest_sha256_v1()'::regprocedure,
      'private.assert_portal_card_context_contract_v1()'::regprocedure
    )
      and routine.proowner = 'portal_public_executor'::regrole
      and not pg_catalog.has_function_privilege(
        'anon', routine.oid, 'EXECUTE'
      )
      and not pg_catalog.has_function_privilege(
        'authenticated', routine.oid, 'EXECUTE'
      )
      and not pg_catalog.has_function_privilege(
        'service_role', routine.oid, 'EXECUTE'
      )
  ) <> 4
     or exists (
       select 1
       from portal_card_context_api_before as before
       join pg_catalog.pg_proc as routine
         on routine.oid =
           pg_catalog.to_regprocedure(before.routine_identity)
       join pg_catalog.pg_language as language
         on language.oid = routine.prolang
       where before.owner_name <>
             pg_catalog.pg_get_userbyid(routine.proowner)
          or before.language_name <> language.lanname
          or before.volatility <> routine.provolatile
          or before.parallel_safety <> routine.proparallel
          or before.security_definer <> routine.prosecdef
          or before.result_type <>
             pg_catalog.pg_get_function_result(routine.oid)
          or before.config <>
             coalesce(routine.proconfig, '{}'::text[])
          or before.acl_text <>
             coalesce(routine.proacl::text, '')
          or routine.prosrc !~ 'portal_decorate_card_context_v1'
     )
     or (
       select count(*)
       from portal_card_context_writer_before as before
       join lateral (
         select pg_catalog.pg_get_functiondef(routine.oid) as definition,
           pg_catalog.pg_get_userbyid(routine.proowner) as owner_name,
           coalesce(routine.proconfig, '{}'::text[]) as config,
           coalesce(routine.proacl::text, '') as acl_text
         from pg_catalog.pg_proc as routine
         where before.object_identity =
           'function:private.sync_portal_catalog_search_row_v1()'
           and routine.oid =
             'private.sync_portal_catalog_search_row_v1()'::regprocedure
         union all
         select pg_catalog.pg_get_triggerdef(trigger.oid),
           null,
           null,
           null
         from pg_catalog.pg_trigger as trigger
         where before.object_identity =
             'trigger:' || trigger.tgrelid::regclass::text || ':' ||
               trigger.tgname
           and not trigger.tgisinternal
       ) as after on true
       where before.definition = after.definition
         and before.owner_name is not distinct from after.owner_name
         and before.config is not distinct from after.config
         and before.acl_text is not distinct from after.acl_text
     ) <> 4
     or exists (
       select 1
       from pg_catalog.pg_trigger as trigger
       join pg_catalog.pg_class as relation
         on relation.oid = trigger.tgrelid
       join pg_catalog.pg_namespace as namespace
         on namespace.oid = relation.relnamespace
       where not trigger.tgisinternal
         and trigger.tgname like 'portal_catalog%context%'
     ) then
    raise exception 'Portal card context cutover contract drifted'
      using errcode = '55000';
  end if;
end
$verify_portal_card_context_cutover$;

grant portal_public_executor to postgres;
set role portal_public_executor;
select private.assert_portal_card_context_contract_v1();
reset role;
revoke portal_public_executor from postgres;

commit;
