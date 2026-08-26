-- Issue #531: cut Portal catalog and Hybrid kernels over only after projection reconciliation and indexes.
--
-- Persistent Dev has production-shaped public cardinality but almost no
-- search_text/embedding_ft derivatives, so the existing derivative indexes
-- cannot be the completeness boundary for anonymous Portal reads.  Build one
-- narrow, Portal-only lexical expression per public dataset table from the
-- already-reviewed allowlist, then hydrate the full public card only after
-- indexed candidate and latest-visible-version reduction.  No external
-- signature, DTO, ACL, RLS policy, owner, function timeout, or legacy Hybrid
-- routine changes in this migration.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '15min';

create temporary table portal_candidate_external_before (
  routine_identity text primary key,
  definition text not null,
  owner_name text not null,
  security_definer boolean not null,
  proconfig text[] not null,
  acl_text text not null
) on commit preserve rows;

insert into portal_candidate_external_before (
  routine_identity,
  definition,
  owner_name,
  security_definer,
  proconfig,
  acl_text
)
with expected(routine_identity) as (
  values
    ('api.portal_search_processes_v1(text,jsonb,text,text,integer)'),
    ('api.portal_search_flows_v1(text,jsonb,text,text,integer)'),
    ('api.portal_hybrid_search_v1(text,text[],text,jsonb,integer)')
)
select
  expected.routine_identity,
  pg_catalog.pg_get_functiondef(routine.oid),
  owner_role.rolname,
  routine.prosecdef,
  coalesce(routine.proconfig, '{}'::text[]),
  coalesce(routine.proacl::text, '')
from expected
join pg_catalog.pg_proc as routine
  on routine.oid = pg_catalog.to_regprocedure(expected.routine_identity)
join pg_catalog.pg_roles as owner_role
  on owner_role.oid = routine.proowner;

create temporary table portal_candidate_legacy_before (
  routine_identity text primary key,
  definition text not null,
  owner_name text not null,
  security_definer boolean not null,
  proconfig text[] not null,
  acl_text text not null
) on commit preserve rows;

insert into portal_candidate_legacy_before (
  routine_identity,
  definition,
  owner_name,
  security_definer,
  proconfig,
  acl_text
)
with expected(routine_identity) as (
  values
    ('api.hybrid_search_processes(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('api.hybrid_search_flows(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('api.hybrid_search_processes_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('api.hybrid_search_flows_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('private.hybrid_search_processes_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('private.hybrid_search_flows_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('private.semantic_process_candidates(text,text,double precision,integer,text)'),
    ('private.semantic_flow_candidates(text,text,double precision,integer,text)')
)
select
  expected.routine_identity,
  pg_catalog.pg_get_functiondef(routine.oid),
  owner_role.rolname,
  routine.prosecdef,
  coalesce(routine.proconfig, '{}'::text[]),
  coalesce(routine.proacl::text, '')
from expected
join pg_catalog.pg_proc as routine
  on routine.oid = pg_catalog.to_regprocedure(expected.routine_identity)
join pg_catalog.pg_roles as owner_role
  on owner_role.oid = routine.proowner;

do $portal_candidate_snapshot_guard$
begin
  if (select count(*) from portal_candidate_external_before) <> 3
     or (select count(*) from portal_candidate_legacy_before) <> 8 then
    raise exception 'Portal candidate-first routine snapshot is incomplete';
  end if;
  if not exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'portal_public_executor'
      and not rolcanlogin
      and not rolbypassrls
      and not rolsuper
      and not rolreplication
  ) or not exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'api_internal_executor'
      and not rolcanlogin
      and not rolbypassrls
      and not rolsuper
      and not rolreplication
  ) then
    raise exception 'Portal candidate-first executor role is missing or unsafe'
      using errcode = '42501';
  end if;
end
$portal_candidate_snapshot_guard$;

-- The document helper was installed once before the two concurrent index
-- migrations.  Acquire the constrained owner only to add the separate facts
-- helper; never replace an immutable expression function after its index build.
grant portal_public_executor to postgres;
grant create on schema private to portal_public_executor;
set role portal_public_executor;

-- Exact score/filter facts are materially cheaper than a complete return
-- card: they omit summary/geography labels/capability envelopes and retain
-- only values needed before cursor/order/limit.  The final page is still
-- hydrated through the original frozen card projector.
create or replace function private.catalog_portal_facts_v1(
  p_kind text,
  p_state_code integer,
  p_json jsonb,
  p_filters jsonb,
  p_query text
)
returns jsonb
language plpgsql
immutable
parallel restricted
security definer
set search_path = ''
as $function$
declare
  v_information jsonb;
  v_modelling jsonb;
  v_location jsonb;
  v_name_root jsonb;
  v_name_item jsonb;
  v_name_text text;
  v_name_key text;
  v_name_exact boolean := false;
  v_name_contains boolean := false;
  v_classification_root jsonb;
  v_classification jsonb;
  v_class jsonb;
  v_category jsonb;
  v_class_code text;
  v_class_exact boolean := false;
  v_class_contains boolean := false;
  v_class_filter_match boolean := false;
  v_geography_code text;
  v_reference_year integer;
  v_process_subtype text;
  v_cas text;
  v_source_metadata jsonb;
  v_source text;
  v_capabilities jsonb;
begin
  if p_kind = 'process' then
    v_information := p_json #> '{processDataSet,processInformation}';
    v_modelling := p_json #> '{processDataSet,modellingAndValidation}';
    v_location := v_information
      #> '{geography,locationOfOperationSupplyOrProduction}';
    v_name_root := v_information #> '{dataSetInformation,name,baseName}';
    v_classification_root := v_information
      #> '{dataSetInformation,classificationInformation}';
    if p_filters ? 'geography' then
      v_geography_code := nullif(
        private.portal_scalar_text_v1(v_location -> '@location'),
        ''
      );
    end if;
    if p_filters ? 'referenceYearFrom' or p_filters ? 'referenceYearTo' then
      v_reference_year := private.portal_safe_year_v1(
        v_information #>> '{time,common:referenceYear}'
      );
    end if;
    if p_filters ? 'processSubtype' then
      v_process_subtype := nullif(private.portal_scalar_text_v1(
        v_modelling #> '{LCIMethodAndAllocation,typeOfDataSet}'
      ), '');
    end if;
  elsif p_kind = 'flow' then
    v_information := p_json #> '{flowDataSet,flowInformation}';
    v_location := v_information -> 'geography';
    v_name_root := v_information #> '{dataSetInformation,name,baseName}';
    v_classification_root := v_information
      #> '{dataSetInformation,classificationInformation}';
    if p_filters ? 'geography' then
      v_geography_code := case pg_catalog.jsonb_typeof(
        v_location -> 'locationOfSupply'
      )
        when 'string' then nullif(
          private.portal_scalar_text_v1(v_location -> 'locationOfSupply'),
          ''
        )
        when 'object' then nullif(
          private.portal_scalar_text_v1(
            v_location #> '{locationOfSupply,@location}'
          ),
          ''
        )
        else null
      end;
    end if;
    v_cas := nullif(pg_catalog.btrim(coalesce(
      v_information #>> '{dataSetInformation,CASNumber}',
      v_information #>> '{dataSetInformation,common:CASNumber}'
    )), '');
    if v_cas !~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$' then
      v_cas := null;
    end if;
  else
    return null;
  end if;

  for v_name_item in
    select item.value
    from pg_catalog.jsonb_array_elements(
      case pg_catalog.jsonb_typeof(v_name_root)
        when 'array' then v_name_root
        when 'null' then '[]'::jsonb
        else pg_catalog.jsonb_build_array(v_name_root)
      end
    ) with ordinality as item(value, ordinality)
    order by item.ordinality
  loop
    v_name_text := case
      when pg_catalog.jsonb_typeof(v_name_item) = 'object'
        and pg_catalog.jsonb_typeof(v_name_item -> '#text') = 'string'
        then pg_catalog.btrim(v_name_item #>> '{#text}')
      when pg_catalog.jsonb_typeof(v_name_item) = 'string'
        then pg_catalog.btrim(v_name_item #>> '{}')
      else null
    end;
    if nullif(v_name_text, '') is not null then
      v_name_key := coalesce(v_name_key, v_name_text);
      if p_query <> '' then
        v_name_exact := v_name_exact
          or pg_catalog.lower(pg_catalog.btrim(v_name_text)) = p_query;
        v_name_contains := v_name_contains
          or pg_catalog.strpos(pg_catalog.lower(v_name_text), p_query) > 0;
      end if;
    end if;
  end loop;

  for v_classification in
    select private.portal_json_items_v1(
      v_classification_root -> 'common:classification'
    )
  loop
    for v_class in
      select private.portal_json_items_v1(
        v_classification -> 'common:class'
      )
    loop
      v_class_code := coalesce(
        nullif(private.portal_scalar_text_v1(v_class -> '@classId'), ''),
        nullif(private.portal_scalar_text_v1(v_class -> '#text'), '')
      );
      if v_class_code is not null then
        v_class_exact := v_class_exact
          or (
            p_query <> ''
            and pg_catalog.lower(pg_catalog.btrim(v_class_code)) = p_query
          );
        v_class_contains := v_class_contains
          or (
            p_query <> ''
            and pg_catalog.strpos(pg_catalog.lower(v_class_code), p_query) > 0
          );
        v_class_filter_match := v_class_filter_match
          or (
            p_filters ? 'classification'
            and pg_catalog.lower(pg_catalog.btrim(v_class_code))
              = p_filters ->> 'classification'
          );
      end if;
    end loop;
  end loop;

  for v_category in
    select private.portal_json_items_v1(
      v_classification_root
        #> '{common:elementaryFlowCategorization,common:category}'
    )
  loop
    v_class_code := coalesce(
      nullif(private.portal_scalar_text_v1(v_category -> '@catId'), ''),
      nullif(private.portal_scalar_text_v1(v_category -> '@classId'), ''),
      nullif(private.portal_scalar_text_v1(v_category -> '#text'), '')
    );
    if v_class_code is not null then
      v_class_exact := v_class_exact
        or (
          p_query <> ''
          and pg_catalog.lower(pg_catalog.btrim(v_class_code)) = p_query
        );
      v_class_contains := v_class_contains
        or (
          p_query <> ''
          and pg_catalog.strpos(pg_catalog.lower(v_class_code), p_query) > 0
        );
      v_class_filter_match := v_class_filter_match
        or (
          p_filters ? 'classification'
          and pg_catalog.lower(pg_catalog.btrim(v_class_code))
            = p_filters ->> 'classification'
        );
    end if;
  end loop;

  if p_filters ? 'source' then
    v_source_metadata := private.portal_source_v1(p_kind, p_json);
    select pg_catalog.string_agg(item ->> 'value', ' ' order by item ->> 'language')
    into v_source
    from pg_catalog.jsonb_array_elements(
      v_source_metadata -> 'providerName'
    ) as localized(item);
  end if;
  if p_filters ? 'accessLevel' then
    v_capabilities := private.portal_capabilities_v1(
      p_kind,
      p_state_code,
      p_json
    );
  end if;

  return pg_catalog.jsonb_build_object(
    'accessLevel', case
      when v_capabilities is null then null
      when (v_capabilities ->> 'exchangesVisible')::boolean then 'open'
      else 'metadata_only'
    end,
    'nameKey', v_name_key,
    'nameExact', v_name_exact,
    'nameContains', v_name_contains,
    'classificationExact', v_class_exact,
    'classificationContains', v_class_contains,
    'classificationFilterMatch', v_class_filter_match,
    'geographyCode', v_geography_code,
    'referenceYear', v_reference_year,
    'processSubtype', v_process_subtype,
    'source', v_source,
    'casNumber', v_cas
  );
end
$function$;

comment on function private.catalog_portal_facts_v1(text, integer, jsonb, jsonb, text) is
  'Exact immutable pre-page facts for Portal score/filter/rank; full DTO cards remain projected only after limit.';

revoke all on function private.catalog_portal_facts_v1(text, integer, jsonb, jsonb, text)
  from public, anon, authenticated, service_role;
grant execute on function private.catalog_portal_facts_v1(text, integer, jsonb, jsonb, text)
  to api_internal_executor;

create or replace function private.catalog_portal_card_facts_v1(
  p_card jsonb,
  p_filters jsonb,
  p_query text
)
returns jsonb
language sql
immutable
parallel safe
security definer
set search_path = ''
as $function$
  select pg_catalog.jsonb_build_object(
    'accessLevel', p_card -> 'accessLevel',
    'nameKey', p_card #> '{names,0,value}',
    'nameExact', pg_catalog.to_jsonb(exists (
      select 1
      from pg_catalog.jsonb_array_elements(
        coalesce(p_card -> 'names', '[]'::jsonb)
      ) as name(item)
      where pg_catalog.lower(pg_catalog.btrim(name.item ->> 'value')) = p_query
    )),
    'nameContains', pg_catalog.to_jsonb(exists (
      select 1
      from pg_catalog.jsonb_array_elements(
        coalesce(p_card -> 'names', '[]'::jsonb)
      ) as name(item)
      where p_query <> ''
        and pg_catalog.strpos(
          pg_catalog.lower(name.item ->> 'value'),
          p_query
        ) > 0
    )),
    'classificationExact', pg_catalog.to_jsonb(exists (
      select 1
      from pg_catalog.jsonb_array_elements(
        coalesce(p_card -> 'classifications', '[]'::jsonb)
      ) as classification(item)
      where pg_catalog.lower(pg_catalog.btrim(classification.item ->> 'code'))
        = p_query
    )),
    'classificationContains', pg_catalog.to_jsonb(exists (
      select 1
      from pg_catalog.jsonb_array_elements(
        coalesce(p_card -> 'classifications', '[]'::jsonb)
      ) as classification(item)
      where p_query <> ''
        and pg_catalog.strpos(
          pg_catalog.lower(classification.item ->> 'code'),
          p_query
        ) > 0
    )),
    'classificationFilterMatch', pg_catalog.to_jsonb(exists (
      select 1
      from pg_catalog.jsonb_array_elements(
        coalesce(p_card -> 'classifications', '[]'::jsonb)
      ) as classification(item)
      where p_filters ? 'classification'
        and pg_catalog.lower(pg_catalog.btrim(classification.item ->> 'code'))
          = p_filters ->> 'classification'
    )),
    'geographyCode', p_card #> '{geography,code}',
    'referenceYear', p_card -> 'referenceYear',
    'processSubtype', p_card -> 'processSubtype',
    'source', p_card -> 'source',
    'casNumber', p_card -> 'casNumber'
  )
$function$;

revoke all on function private.catalog_portal_card_facts_v1(jsonb, jsonb, text)
  from public, anon, authenticated, service_role;
grant execute on function private.catalog_portal_card_facts_v1(jsonb, jsonb, text)
  to api_internal_executor;

-- PGroonga's LIKE planner needs a Const pattern to select the expression
-- index.  These two helpers use fixed SQL templates: relation, columns,
-- function, predicates, and ESCAPE are constants; only the already escaped
-- pattern is rendered with %L.  There is no identifier interpolation.
create or replace function private.catalog_portal_process_pattern_versions_v1(
  p_like_pattern text
)
returns table(id uuid, version text)
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
set row_security = 'on'
as $function$
begin
  return query execute pg_catalog.format($sql$
    select projection.id,
      projection.version
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'process'
      and projection.document like %L escape E'\\'
  $sql$, p_like_pattern);
end
$function$;

create or replace function private.catalog_portal_flow_pattern_versions_v1(
  p_like_pattern text
)
returns table(id uuid, version text)
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
set row_security = 'on'
as $function$
begin
  return query execute pg_catalog.format($sql$
    select projection.id,
      projection.version
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'flow'
      and projection.document like %L escape E'\\'
  $sql$, p_like_pattern);
end
$function$;

revoke all on function private.catalog_portal_process_pattern_versions_v1(text)
  from public, anon, authenticated, service_role, api_internal_executor;
revoke all on function private.catalog_portal_flow_pattern_versions_v1(text)
  from public, anon, authenticated, service_role, api_internal_executor;

create or replace function private.catalog_portal_hybrid_pattern_matches_v1(
  p_kind text,
  p_query_terms text[]
)
returns table(id uuid, version text, term_ordinal integer)
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
  v_ordinal integer;
  v_pattern text;
begin
  for v_ordinal in 1..pg_catalog.cardinality(p_query_terms)
  loop
    v_pattern := '%' || pg_catalog.replace(
      pg_catalog.replace(
        pg_catalog.replace(
          p_query_terms[v_ordinal],
          pg_catalog.chr(92),
          pg_catalog.chr(92) || pg_catalog.chr(92)
        ),
        '%',
        pg_catalog.chr(92) || '%'
      ),
      '_',
      pg_catalog.chr(92) || '_'
    ) || '%';
    if p_kind = 'process' then
      return query
      select pattern.id,
        pattern.version,
        v_ordinal
      from private.catalog_portal_process_pattern_versions_v1(
        v_pattern
      ) as pattern;
    elsif p_kind = 'flow' then
      return query
      select pattern.id,
        pattern.version,
        v_ordinal
      from private.catalog_portal_flow_pattern_versions_v1(
        v_pattern
      ) as pattern;
    end if;
  end loop;
end
$function$;

revoke all on function private.catalog_portal_hybrid_pattern_matches_v1(
  text, text[]
) from public, anon, authenticated, service_role;
grant execute on function private.catalog_portal_hybrid_pattern_matches_v1(
  text, text[]
) to api_internal_executor;

reset role;
revoke create on schema private from portal_public_executor;
revoke portal_public_executor from postgres;

-- The preceding single-statement migrations build both partial indexes
-- concurrently.  Reject a same-name drift instead of trusting IF NOT EXISTS.
do $portal_candidate_index_guard$
declare
  v_index regclass;
  v_definition text;
begin
  foreach v_index in array array[
    'private.portal_catalog_search_process_document_v1_pgroonga'::regclass,
    'private.portal_catalog_search_flow_document_v1_pgroonga'::regclass,
    'private.portal_catalog_search_process_embedding_v1_hnsw'::regclass,
    'private.portal_catalog_search_flow_embedding_v1_hnsw'::regclass
  ]
  loop
    if not (
      select index_catalog.indisvalid
        and index_catalog.indisready
        and index_catalog.indislive
      from pg_catalog.pg_index as index_catalog
      where index_catalog.indexrelid = v_index
    ) then
      raise exception 'Portal candidate index % is not valid, ready, and live', v_index;
    end if;
    v_definition := pg_catalog.pg_get_indexdef(v_index);
    if v_definition !~ 'portal_catalog_search_rows_v1'
       or (
         v_definition !~ 'USING pgroonga'
         and v_definition !~ 'USING hnsw'
       )
       or v_definition !~ 'dataset_kind' then
      raise exception 'Portal candidate index % contract drifted: %', v_index, v_definition;
    end if;
  end loop;
end
$portal_candidate_index_guard$;

grant api_internal_executor to postgres;
grant create on schema private to api_internal_executor;
set role api_internal_executor;

create or replace function private.catalog_portal_candidate_rows_v1(
  p_kind text,
  p_query text,
  p_exact_id uuid,
  p_like_pattern text
)
returns table(
  id uuid,
  version text,
  card jsonb,
  state_code integer,
  modified_at timestamptz
)
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
set row_security = 'on'
as $function$
begin
  if p_kind = 'process' and p_query = '' then
    return query
    select distinct on (projection.id)
      projection.id,
      projection.version,
      projection.card,
      projection.state_code,
      projection.modified_at
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'process'
    order by projection.id,
      projection.version desc,
      projection.modified_at desc,
      projection.state_code desc;
    return;
  end if;

  if p_kind = 'process' and p_exact_id is not null then
    return query
    with matched as materialized (
      select pattern.id,
        pattern.version,
        false as exact_id
      from private.catalog_portal_process_pattern_versions_v1(
        p_like_pattern
      ) as pattern
      union
      select projection.id,
        projection.version,
        true
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = 'process'
        and projection.id = p_exact_id
    ), candidate_ids as materialized (
      select matched.id,
        pg_catalog.bool_or(matched.exact_id) as exact_id
      from matched
      group by matched.id
    )
    select latest.id,
      latest.version,
      latest.card,
      latest.state_code,
      latest.modified_at
    from candidate_ids
    cross join lateral (
      select projection.id,
        projection.version,
        projection.card,
        projection.state_code,
        projection.modified_at
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = 'process'
        and projection.id = candidate_ids.id
      order by projection.version desc,
        projection.modified_at desc,
        projection.state_code desc
      limit 1
    ) as latest
    where candidate_ids.exact_id
       or exists (
         select 1
         from matched
         where matched.id = latest.id
           and matched.version = latest.version
       );
    return;
  end if;

  if p_kind = 'process' then
    return query
    with matched as materialized (
      select pattern.id,
        pattern.version
      from private.catalog_portal_process_pattern_versions_v1(
        p_like_pattern
      ) as pattern
    ), candidate_ids as materialized (
      select distinct matched.id
      from matched
    )
    select latest.id,
      latest.version,
      latest.card,
      latest.state_code,
      latest.modified_at
    from candidate_ids
    cross join lateral (
      select projection.id,
        projection.version,
        projection.card,
        projection.state_code,
        projection.modified_at
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = 'process'
        and projection.id = candidate_ids.id
      order by projection.version desc,
        projection.modified_at desc,
        projection.state_code desc
      limit 1
    ) as latest
    where exists (
      select 1
      from matched
      where matched.id = latest.id
        and matched.version = latest.version
    );
    return;
  end if;

  if p_kind = 'flow' and p_query = '' then
    return query
    select distinct on (projection.id)
      projection.id,
      projection.version,
      projection.card,
      projection.state_code,
      projection.modified_at
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'flow'
    order by projection.id,
      projection.version desc,
      projection.modified_at desc,
      projection.state_code desc;
    return;
  end if;

  if p_kind = 'flow' and p_exact_id is not null then
    return query
    with matched as materialized (
      select pattern.id,
        pattern.version,
        false as exact_id
      from private.catalog_portal_flow_pattern_versions_v1(
        p_like_pattern
      ) as pattern
      union
      select projection.id,
        projection.version,
        true
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = 'flow'
        and projection.id = p_exact_id
    ), candidate_ids as materialized (
      select matched.id,
        pg_catalog.bool_or(matched.exact_id) as exact_id
      from matched
      group by matched.id
    )
    select latest.id,
      latest.version,
      latest.card,
      latest.state_code,
      latest.modified_at
    from candidate_ids
    cross join lateral (
      select projection.id,
        projection.version,
        projection.card,
        projection.state_code,
        projection.modified_at
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = 'flow'
        and projection.id = candidate_ids.id
      order by projection.version desc,
        projection.modified_at desc,
        projection.state_code desc
      limit 1
    ) as latest
    where candidate_ids.exact_id
       or exists (
         select 1
         from matched
         where matched.id = latest.id
           and matched.version = latest.version
       );
    return;
  end if;

  if p_kind = 'flow' then
    return query
    with matched as materialized (
      select pattern.id,
        pattern.version
      from private.catalog_portal_flow_pattern_versions_v1(
        p_like_pattern
      ) as pattern
    ), candidate_ids as materialized (
      select distinct matched.id
      from matched
    )
    select latest.id,
      latest.version,
      latest.card,
      latest.state_code,
      latest.modified_at
    from candidate_ids
    cross join lateral (
      select projection.id,
        projection.version,
        projection.card,
        projection.state_code,
        projection.modified_at
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = 'flow'
        and projection.id = candidate_ids.id
      order by projection.version desc,
        projection.modified_at desc,
        projection.state_code desc
      limit 1
    ) as latest
    where exists (
      select 1
      from matched
      where matched.id = latest.id
        and matched.version = latest.version
    );
  end if;
end
$function$;

comment on function private.catalog_portal_candidate_rows_v1(
  text, text, uuid, text
) is
  'Six static kind-by-empty/UUID/lexical branches: indexed public-document IDs, then exact latest-visible recheck without pre-limit.';

revoke all on function private.catalog_portal_candidate_rows_v1(
  text, text, uuid, text
) from public, anon, authenticated, service_role, portal_public_executor;

reset role;
grant portal_public_executor to postgres;
grant create on schema private to portal_public_executor;
alter function private.catalog_portal_candidate_rows_v1(
  text, text, uuid, text
) owner to portal_public_executor;
set role portal_public_executor;
revoke all on function private.catalog_portal_candidate_rows_v1(
  text, text, uuid, text
) from public, anon, authenticated, service_role, api_internal_executor;
grant execute on function private.catalog_portal_candidate_rows_v1(
  text, text, uuid, text
) to api_internal_executor;
reset role;
revoke create on schema private from portal_public_executor;
revoke portal_public_executor from postgres;
set role api_internal_executor;

create or replace function private.catalog_portal_search_v1_impl(
  p_kind text,
  p_query text,
  p_filters jsonb,
  p_sort text,
  p_cursor_rank text,
  p_cursor_id uuid,
  p_cursor_version text,
  p_limit integer,
  p_query_fingerprint text
)
returns jsonb
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
as $function$
declare
  v_items jsonb;
  v_next_cursor_payload jsonb;
  v_exact_id uuid;
  v_like_pattern text;
begin
  if p_query ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    v_exact_id := p_query::uuid;
  end if;
  if p_query <> '' then
    v_like_pattern := '%' || pg_catalog.replace(
      pg_catalog.replace(
        pg_catalog.replace(
          p_query,
          pg_catalog.chr(92),
          pg_catalog.chr(92) || pg_catalog.chr(92)
        ),
        '%',
        pg_catalog.chr(92) || '%'
      ),
      '_',
      pg_catalog.chr(92) || '_'
    ) || '%';
  end if;
  -- Empty unfiltered browse pages do not require search facts for the whole
  -- catalog.  Order/latest/cursor reduction happens before at most limit+1
  -- cards are hydrated.
  if p_query = ''
     and p_filters = '{}'::jsonb
     and p_sort in ('relevance', 'modified_desc') then
    with portal_prefilter as materialized (
      select p_kind as dataset_kind,
        candidate.*
      from private.catalog_portal_candidate_rows_v1(
        p_kind,
        p_query,
        v_exact_id,
        v_like_pattern
      ) as candidate
    ), portal_after_cursor as materialized (
      select portal_prefilter.*
      from portal_prefilter
      where p_cursor_rank is null
        or case p_sort
          when 'relevance' then
            0::numeric < p_cursor_rank::numeric
            or (
              0::numeric = p_cursor_rank::numeric
              and (
                portal_prefilter.id > p_cursor_id
                or (
                  portal_prefilter.id = p_cursor_id
                  and portal_prefilter.version < p_cursor_version
                )
              )
            )
          else
            portal_prefilter.modified_at < p_cursor_rank::timestamptz
            or (
              portal_prefilter.modified_at = p_cursor_rank::timestamptz
              and (
                portal_prefilter.id > p_cursor_id
                or (
                  portal_prefilter.id = p_cursor_id
                  and portal_prefilter.version < p_cursor_version
                )
              )
            )
        end
    ), portal_ordered as materialized (
      select portal_after_cursor.*,
        pg_catalog.row_number() over (
          order by
            case when p_sort = 'modified_desc'
              then portal_after_cursor.modified_at end desc,
            portal_after_cursor.id asc,
            portal_after_cursor.version desc
        ) as page_rank
      from portal_after_cursor
      order by
        case when p_sort = 'modified_desc'
          then portal_after_cursor.modified_at end desc,
        portal_after_cursor.id asc,
        portal_after_cursor.version desc
      limit p_limit + 1
    ), portal_decorated as materialized (
      select portal_ordered.*
      from portal_ordered
    )
    select
      coalesce(pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
          'key', pg_catalog.jsonb_build_object(
            'kind', p_kind,
            'id', portal_decorated.id::text,
            'version', portal_decorated.version
          ),
          'accessLevel', portal_decorated.card -> 'accessLevel',
          'capabilities', portal_decorated.card -> 'capabilities',
          'names', portal_decorated.card -> 'names',
          'summary', portal_decorated.card -> 'summary',
          'geography', portal_decorated.card -> 'geography',
          'referenceYear', portal_decorated.card -> 'referenceYear',
          'modifiedAt', pg_catalog.to_char(
            portal_decorated.modified_at at time zone 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
          ),
          'match', pg_catalog.jsonb_build_object(
            'kind', 'lexical',
            'score', 0::numeric,
            'reasonCodes', '[]'::jsonb
          )
        ) order by portal_decorated.page_rank
      ) filter (where portal_decorated.page_rank <= p_limit), '[]'::jsonb),
      case when max(portal_decorated.page_rank) > p_limit then
        (pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'v', 1,
          'fp', p_query_fingerprint,
          'rankKey', case p_sort
            when 'relevance' then '0'
            else pg_catalog.to_char(
              portal_decorated.modified_at at time zone 'UTC',
              'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
            )
          end,
          'kind', p_kind,
          'id', portal_decorated.id::text,
          'version', portal_decorated.version
        ) order by portal_decorated.page_rank)
          filter (where portal_decorated.page_rank = p_limit)) -> 0
      else null end
    into v_items, v_next_cursor_payload
    from portal_decorated;

    return pg_catalog.jsonb_build_object(
      'items', v_items,
      'nextCursorPayload', v_next_cursor_payload
    );
  end if;

  with portal_prefilter as materialized (
    select p_kind as dataset_kind,
      candidate.*
    from private.catalog_portal_candidate_rows_v1(
      p_kind,
      p_query,
      v_exact_id,
      v_like_pattern
    ) as candidate
  ), portal_facts as materialized (
    select portal_prefilter.*,
      private.catalog_portal_card_facts_v1(
        portal_prefilter.card,
        p_filters,
        p_query
      ) as facts
    from portal_prefilter
  ), portal_scored as materialized (
    select portal_facts.*,
      case
        when nullif(portal_facts.facts ->> 'nameKey', '') is not null
          and pg_catalog.length(portal_facts.facts ->> 'nameKey') <= 500
          and pg_catalog.octet_length(portal_facts.facts ->> 'nameKey') <= 2000
          and portal_facts.facts ->> 'nameKey' !~ '[[:cntrl:]]'
          then portal_facts.facts ->> 'nameKey'
        else '~unnamed:' || portal_facts.id::text
      end as name_key,
      case
        when p_query = '' then 0::numeric
        when pg_catalog.lower(portal_facts.id::text) = p_query then 1::numeric
        when pg_catalog.lower(coalesce(portal_facts.facts ->> 'casNumber', '')) = p_query
          then 0.98::numeric
        when (portal_facts.facts ->> 'nameExact')::boolean then 0.95::numeric
        when (portal_facts.facts ->> 'classificationExact')::boolean
          then 0.92::numeric
        when p_query <> '' then 0.70::numeric
        else 0::numeric
      end as score,
      case
        when pg_catalog.lower(portal_facts.id::text) = p_query
          then pg_catalog.jsonb_build_array('exact_id')
        when pg_catalog.lower(coalesce(portal_facts.facts ->> 'casNumber', '')) = p_query
          then pg_catalog.jsonb_build_array('cas')
        when (portal_facts.facts ->> 'nameExact')::boolean
          or (portal_facts.facts ->> 'nameContains')::boolean
          then pg_catalog.jsonb_build_array('name')
        when (portal_facts.facts ->> 'classificationExact')::boolean
          or (portal_facts.facts ->> 'classificationContains')::boolean
          then pg_catalog.jsonb_build_array('classification')
        when p_query <> '' then pg_catalog.jsonb_build_array('full_text')
        else '[]'::jsonb
      end as reason_codes
    from portal_facts
  ), portal_filtered as materialized (
    select portal_scored.*,
      case p_sort
        when 'relevance' then portal_scored.score::text
        when 'modified_desc' then pg_catalog.to_char(
          portal_scored.modified_at at time zone 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        )
        else pg_catalog.lower(portal_scored.name_key)
      end as rank_key
    from portal_scored
    where (p_query = '' or portal_scored.score > 0)
      and (
        not (p_filters ? 'accessLevel')
        or portal_scored.facts ->> 'accessLevel' = p_filters ->> 'accessLevel'
      )
      and (
        not (p_filters ? 'geography')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_scored.facts ->> 'geographyCode',
          ''
        ))) = p_filters ->> 'geography'
      )
      and (
        not (p_filters ? 'classification')
        or (portal_scored.facts ->> 'classificationFilterMatch')::boolean
      )
      and (
        not (p_filters ? 'referenceYearFrom')
        or (portal_scored.facts ->> 'referenceYear')::integer
          >= (p_filters ->> 'referenceYearFrom')::integer
      )
      and (
        not (p_filters ? 'referenceYearTo')
        or (portal_scored.facts ->> 'referenceYear')::integer
          <= (p_filters ->> 'referenceYearTo')::integer
      )
      and (
        not (p_filters ? 'processSubtype')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_scored.facts ->> 'processSubtype',
          ''
        ))) = p_filters ->> 'processSubtype'
      )
      and (
        not (p_filters ? 'source')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_scored.facts ->> 'source',
          ''
        ))) = p_filters ->> 'source'
      )
  ), portal_after_cursor as materialized (
    select portal_filtered.*
    from portal_filtered
    where p_cursor_rank is null
      or case p_sort
        when 'relevance' then
          portal_filtered.score < p_cursor_rank::numeric
          or (
            portal_filtered.score = p_cursor_rank::numeric
            and (
              portal_filtered.id > p_cursor_id
              or (
                portal_filtered.id = p_cursor_id
                and portal_filtered.version < p_cursor_version
              )
            )
          )
        when 'modified_desc' then
          portal_filtered.modified_at < p_cursor_rank::timestamptz
          or (
            portal_filtered.modified_at = p_cursor_rank::timestamptz
            and (
              portal_filtered.id > p_cursor_id
              or (
                portal_filtered.id = p_cursor_id
                and portal_filtered.version < p_cursor_version
              )
            )
          )
        else
          pg_catalog.lower(portal_filtered.name_key) > pg_catalog.lower(p_cursor_rank)
          or (
            pg_catalog.lower(portal_filtered.name_key) = pg_catalog.lower(p_cursor_rank)
            and (
              portal_filtered.id > p_cursor_id
              or (
                portal_filtered.id = p_cursor_id
                and portal_filtered.version < p_cursor_version
              )
            )
          )
      end
  ), portal_ordered as materialized (
    select portal_after_cursor.*,
      pg_catalog.row_number() over (
        order by
          case when p_sort = 'relevance' then portal_after_cursor.score end desc,
          case when p_sort = 'modified_desc' then portal_after_cursor.modified_at end desc,
          case when p_sort = 'name_asc'
            then pg_catalog.lower(portal_after_cursor.name_key) end asc,
          portal_after_cursor.id asc,
          portal_after_cursor.version desc
      ) as page_rank
    from portal_after_cursor
    order by
      case when p_sort = 'relevance' then portal_after_cursor.score end desc,
      case when p_sort = 'modified_desc' then portal_after_cursor.modified_at end desc,
      case when p_sort = 'name_asc'
        then pg_catalog.lower(portal_after_cursor.name_key) end asc,
      portal_after_cursor.id asc,
      portal_after_cursor.version desc
    limit p_limit + 1
  ), portal_hydrated as materialized (
    select portal_ordered.*
    from portal_ordered
  )
  select
    coalesce(pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'key', pg_catalog.jsonb_build_object(
          'kind', p_kind,
          'id', portal_hydrated.id::text,
          'version', portal_hydrated.version
        ),
        'accessLevel', portal_hydrated.card -> 'accessLevel',
        'capabilities', portal_hydrated.card -> 'capabilities',
        'names', portal_hydrated.card -> 'names',
        'summary', portal_hydrated.card -> 'summary',
        'geography', portal_hydrated.card -> 'geography',
        'referenceYear', portal_hydrated.card -> 'referenceYear',
        'modifiedAt', pg_catalog.to_char(
          portal_hydrated.modified_at at time zone 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        ),
        'match', pg_catalog.jsonb_build_object(
          'kind', case when portal_hydrated.reason_codes
            ?| array['exact_id', 'cas', 'classification']
            then 'identifier' else 'lexical' end,
          'score', portal_hydrated.score,
          'reasonCodes', portal_hydrated.reason_codes
        )
      ) order by portal_hydrated.page_rank
    ) filter (where portal_hydrated.page_rank <= p_limit), '[]'::jsonb),
    case when max(portal_hydrated.page_rank) > p_limit then
      (pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'v', 1,
        'fp', p_query_fingerprint,
        'rankKey', portal_hydrated.rank_key,
        'kind', p_kind,
        'id', portal_hydrated.id::text,
        'version', portal_hydrated.version
      ) order by portal_hydrated.page_rank)
        filter (where portal_hydrated.page_rank = p_limit)) -> 0
    else null end
  into v_items, v_next_cursor_payload
  from portal_hydrated;

  return pg_catalog.jsonb_build_object(
    'items', v_items,
    'nextCursorPayload', v_next_cursor_payload
  );
end
$function$;

comment on function private.catalog_portal_search_v1_impl(
  text, text, jsonb, text, text, uuid, text, integer, text
) is
  'Candidate-first Portal catalog kernel: fixed public-document PGroonga candidates, exact latest-visible recheck, then public-card hydration.';

revoke all on function private.catalog_portal_search_v1_impl(
  text, text, jsonb, text, text, uuid, text, integer, text
) from public, anon, authenticated, service_role;
grant execute on function private.catalog_portal_search_v1_impl(
  text, text, jsonb, text, text, uuid, text, integer, text
) to portal_public_executor;

-- Rebuild only the Portal Hybrid kernel.  The eight historical raw/login
-- Hybrid routines remain untouched and are byte-verified below.
create or replace function private.portal_public_hybrid_search_v1_impl(
  p_kind text,
  p_query_terms text[],
  p_query_embedding extensions.vector(1024),
  p_filters jsonb,
  p_limit integer,
  p_query_fingerprint text
)
returns jsonb
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
set hnsw.iterative_scan = 'strict_order'
as $function$
declare
  v_items jsonb;
  v_result jsonb;
begin
  with portal_lexical_prefilter as materialized (
    select
      process.id,
      process.version::text as version,
      process.json as json_data,
      process.state_code,
      process.modified_at
    from public.processes as process
    where p_kind = 'process'
      and process.state_code in (100, 200)
      and process.modified_at is not null
      and pg_catalog.jsonb_typeof(process.json) = 'object'
      and pg_catalog.jsonb_typeof(process.json -> 'processDataSet') = 'object'
      and private.catalog_portal_document_v1('process', process.json)
        operator(extensions.&@|) p_query_terms
      and not exists (
        select 1
        from public.processes as newer
        where newer.id = process.id
          and newer.state_code in (100, 200)
          and newer.modified_at is not null
          and pg_catalog.jsonb_typeof(newer.json) = 'object'
          and pg_catalog.jsonb_typeof(newer.json -> 'processDataSet') = 'object'
          and (
            newer.version::text > process.version::text
            or (
              newer.version::text = process.version::text
              and newer.modified_at > process.modified_at
            )
            or (
              newer.version::text = process.version::text
              and newer.modified_at = process.modified_at
              and newer.state_code > process.state_code
            )
          )
      )
    union all
    select
      flow.id,
      flow.version::text,
      flow.json,
      flow.state_code,
      flow.modified_at
    from public.flows as flow
    where p_kind = 'flow'
      and flow.state_code in (100, 200)
      and flow.modified_at is not null
      and pg_catalog.jsonb_typeof(flow.json) = 'object'
      and pg_catalog.jsonb_typeof(flow.json -> 'flowDataSet') = 'object'
      and private.catalog_portal_document_v1('flow', flow.json)
        operator(extensions.&@|) p_query_terms
      and not exists (
        select 1
        from public.flows as newer
        where newer.id = flow.id
          and newer.state_code in (100, 200)
          and newer.modified_at is not null
          and pg_catalog.jsonb_typeof(newer.json) = 'object'
          and pg_catalog.jsonb_typeof(newer.json -> 'flowDataSet') = 'object'
          and (
            newer.version::text > flow.version::text
            or (
              newer.version::text = flow.version::text
              and newer.modified_at > flow.modified_at
            )
            or (
              newer.version::text = flow.version::text
              and newer.modified_at = flow.modified_at
              and newer.state_code > flow.state_code
            )
          )
      )
  ), portal_lexical_documents as materialized (
    select portal_lexical_prefilter.*,
      private.catalog_portal_document_v1(
        p_kind,
        portal_lexical_prefilter.json_data
      ) as public_document
    from portal_lexical_prefilter
  ), portal_lexical_counts as materialized (
    select portal_lexical_documents.*,
      (
        select count(*)::integer
        from pg_catalog.unnest(p_query_terms) as query_term(term)
        where pg_catalog.strpos(
          pg_catalog.lower(coalesce(portal_lexical_documents.public_document, '')),
          query_term.term
        ) > 0
      ) as lexical_hit_count
    from portal_lexical_documents
  ), portal_lexical_candidates as materialized (
    select portal_lexical_counts.*
    from portal_lexical_counts
    where portal_lexical_counts.lexical_hit_count > 0
    order by portal_lexical_counts.lexical_hit_count desc,
      portal_lexical_counts.id asc,
      portal_lexical_counts.version desc
    limit 200
  ), portal_lexical_ranked as materialized (
    select portal_lexical_candidates.*,
      pg_catalog.row_number() over (
        order by portal_lexical_candidates.lexical_hit_count desc,
          portal_lexical_candidates.id asc,
          portal_lexical_candidates.version desc
      )::integer as lexical_rank
    from portal_lexical_candidates
  ), portal_semantic_process_pool as materialized (
    select candidate.id,
      candidate.version,
      candidate.semantic_distance
    from (
      select process.id,
        process.version::text as version,
        process.embedding_ft operator(extensions.<=>) p_query_embedding
          as semantic_distance
      from public.processes as process
      where p_kind = 'process'
        and process.state_code in (100, 200)
        and process.modified_at is not null
        and process.embedding_ft is not null
        and pg_catalog.jsonb_typeof(process.json) = 'object'
        and pg_catalog.jsonb_typeof(process.json -> 'processDataSet') = 'object'
        and not exists (
          select 1
          from public.processes as newer
          where newer.id = process.id
            and newer.state_code in (100, 200)
            and newer.modified_at is not null
            and pg_catalog.jsonb_typeof(newer.json) = 'object'
            and pg_catalog.jsonb_typeof(newer.json -> 'processDataSet') = 'object'
            and (
              newer.version::text > process.version::text
              or (
                newer.version::text = process.version::text
                and newer.modified_at > process.modified_at
              )
              or (
                newer.version::text = process.version::text
                and newer.modified_at = process.modified_at
                and newer.state_code > process.state_code
              )
            )
        )
      order by process.embedding_ft operator(extensions.<=>) p_query_embedding
      limit 200
    ) as candidate
    where candidate.semantic_distance is not null
      and candidate.semantic_distance >= 0::double precision
      and candidate.semantic_distance <= 0.5::double precision
  ), portal_semantic_flow_pool as materialized (
    select candidate.id,
      candidate.version,
      candidate.semantic_distance
    from (
      select flow.id,
        flow.version::text as version,
        flow.embedding_ft operator(extensions.<=>) p_query_embedding
          as semantic_distance
      from public.flows as flow
      where p_kind = 'flow'
        and flow.state_code in (100, 200)
        and flow.modified_at is not null
        and flow.embedding_ft is not null
        and pg_catalog.jsonb_typeof(flow.json) = 'object'
        and pg_catalog.jsonb_typeof(flow.json -> 'flowDataSet') = 'object'
        and not exists (
          select 1
          from public.flows as newer
          where newer.id = flow.id
            and newer.state_code in (100, 200)
            and newer.modified_at is not null
            and pg_catalog.jsonb_typeof(newer.json) = 'object'
            and pg_catalog.jsonb_typeof(newer.json -> 'flowDataSet') = 'object'
            and (
              newer.version::text > flow.version::text
              or (
                newer.version::text = flow.version::text
                and newer.modified_at > flow.modified_at
              )
              or (
                newer.version::text = flow.version::text
                and newer.modified_at = flow.modified_at
                and newer.state_code > flow.state_code
              )
            )
        )
      order by flow.embedding_ft operator(extensions.<=>) p_query_embedding
      limit 200
    ) as candidate
    where candidate.semantic_distance is not null
      and candidate.semantic_distance >= 0::double precision
      and candidate.semantic_distance <= 0.5::double precision
  ), portal_semantic_candidates as materialized (
    select * from portal_semantic_process_pool
    union all
    select * from portal_semantic_flow_pool
  ), portal_semantic_ranked as materialized (
    select portal_semantic_candidates.*,
      pg_catalog.row_number() over (
        order by portal_semantic_candidates.semantic_distance asc,
          portal_semantic_candidates.id asc,
          portal_semantic_candidates.version desc
      )::integer as semantic_rank
    from portal_semantic_candidates
  ), portal_fused as materialized (
    select
      coalesce(portal_lexical_ranked.id, portal_semantic_ranked.id) as id,
      coalesce(portal_lexical_ranked.version, portal_semantic_ranked.version) as version,
      portal_lexical_ranked.lexical_rank,
      portal_semantic_ranked.semantic_rank,
      portal_semantic_ranked.semantic_distance,
      pg_catalog.round(
        least(
          1::numeric,
          greatest(
            0::numeric,
            (
              coalesce(
                0.5::numeric / (60 + portal_lexical_ranked.lexical_rank),
                0::numeric
              )
              + coalesce(
                0.5::numeric / (60 + portal_semantic_ranked.semantic_rank),
                0::numeric
              )
            ) * 61::numeric
          )
        ),
        12
      ) as normalized_score
    from portal_lexical_ranked
    full outer join portal_semantic_ranked
      on portal_semantic_ranked.id = portal_lexical_ranked.id
     and portal_semantic_ranked.version = portal_lexical_ranked.version
  ), portal_fused_rows as materialized (
    select
      portal_fused.*,
      process.json as json_data,
      process.state_code,
      process.modified_at
    from portal_fused
    join public.processes as process
      on p_kind = 'process'
     and process.id = portal_fused.id
     and process.version::text = portal_fused.version
    union all
    select
      portal_fused.*,
      flow.json,
      flow.state_code,
      flow.modified_at
    from portal_fused
    join public.flows as flow
      on p_kind = 'flow'
     and flow.id = portal_fused.id
     and flow.version::text = portal_fused.version
  ), portal_fused_decorated as materialized (
    select portal_fused_rows.*,
      private.portal_public_hybrid_card_v1(
        p_kind,
        portal_fused_rows.state_code,
        portal_fused_rows.json_data
      ) as card
    from portal_fused_rows
  ), portal_filtered as materialized (
    select portal_fused_decorated.*
    from portal_fused_decorated
    where (
        not (p_filters ? 'accessLevel')
        or portal_fused_decorated.card ->> 'accessLevel'
          = p_filters ->> 'accessLevel'
      )
      and (
        not (p_filters ? 'geography')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_fused_decorated.card #>> '{geography,code}',
          ''
        ))) = p_filters ->> 'geography'
      )
      and (
        not (p_filters ? 'classification')
        or exists (
          select 1
          from pg_catalog.jsonb_array_elements(coalesce(
            portal_fused_decorated.card -> 'classifications',
            '[]'::jsonb
          )) as classification(item)
          where pg_catalog.lower(pg_catalog.btrim(classification.item ->> 'code'))
            = p_filters ->> 'classification'
        )
      )
      and (
        not (p_filters ? 'referenceYearFrom')
        or (portal_fused_decorated.card ->> 'referenceYear')::integer
          >= (p_filters ->> 'referenceYearFrom')::integer
      )
      and (
        not (p_filters ? 'referenceYearTo')
        or (portal_fused_decorated.card ->> 'referenceYear')::integer
          <= (p_filters ->> 'referenceYearTo')::integer
      )
      and (
        not (p_filters ? 'processSubtype')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_fused_decorated.card ->> 'processSubtype',
          ''
        ))) = p_filters ->> 'processSubtype'
      )
      and (
        not (p_filters ? 'source')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_fused_decorated.card ->> 'source',
          ''
        ))) = p_filters ->> 'source'
      )
  ), portal_ordered as materialized (
    select portal_filtered.*
    from portal_filtered
    order by portal_filtered.normalized_score desc,
      portal_filtered.id asc,
      portal_filtered.version desc
    limit p_limit
  )
  select coalesce(
    pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'key', pg_catalog.jsonb_build_object(
          'kind', p_kind,
          'id', portal_ordered.id::text,
          'version', portal_ordered.version
        ),
        'accessLevel', portal_ordered.card -> 'accessLevel',
        'capabilities', portal_ordered.card -> 'capabilities',
        'names', portal_ordered.card -> 'names',
        'summary', portal_ordered.card -> 'summary',
        'geography', portal_ordered.card -> 'geography',
        'referenceYear', portal_ordered.card -> 'referenceYear',
        'modifiedAt', pg_catalog.to_char(
          portal_ordered.modified_at at time zone 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        ),
        'match', pg_catalog.jsonb_build_object(
          'kind', 'hybrid',
          'algorithmVersion', 'portal-hybrid-rank-v1',
          'score', portal_ordered.normalized_score,
          'reasonCodes', pg_catalog.to_jsonb(pg_catalog.array_remove(array[
            case when portal_ordered.lexical_rank is not null
              then 'lexical_public_projection'::text end,
            case when portal_ordered.semantic_rank is not null
              then 'semantic_public_projection'::text end
          ], null)),
          'evidence', pg_catalog.jsonb_build_object(
            'lexicalRank', portal_ordered.lexical_rank,
            'semanticRank', portal_ordered.semantic_rank,
            'semanticDistance', case
              when portal_ordered.semantic_distance is null then null
              else pg_catalog.trim_scale(
                portal_ordered.semantic_distance::numeric
              )::text
            end
          )
        )
      )
      order by portal_ordered.normalized_score desc,
        portal_ordered.id asc,
        portal_ordered.version desc
    ),
    '[]'::jsonb
  )
  into v_items
  from portal_ordered;

  v_result := pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-hybrid-candidate-page.v1',
    'kind', p_kind,
    'queryFingerprint', p_query_fingerprint,
    'items', v_items
  );
  if pg_catalog.octet_length(pg_catalog.convert_to(v_result::text, 'UTF8')) > 524288 then
    raise exception using errcode = '54000', message = 'portal hybrid response too large';
  end if;
  return v_result;
end
$function$;

comment on function private.portal_public_hybrid_search_v1_impl(
  text, text[], extensions.vector, jsonb, integer, text
) is
  'Candidate-first portal-hybrid-rank-v1 kernel over exact latest visible rows; lexical and semantic pools are bounded before final public-card hydration.';

revoke all on function private.portal_public_hybrid_search_v1_impl(
  text, text[], extensions.vector, jsonb, integer, text
) from public, anon, authenticated, service_role;
grant execute on function private.portal_public_hybrid_search_v1_impl(
  text, text[], extensions.vector, jsonb, integer, text
) to portal_public_executor;

-- Authoritative projection-backed definition. The earlier body is retained in
-- this additive migration only as upgrade evidence until local review; this
-- final CREATE OR REPLACE is the installed kernel verified below.
create or replace function private.portal_public_hybrid_search_v1_impl(
  p_kind text,
  p_query_terms text[],
  p_query_embedding extensions.vector(1024),
  p_filters jsonb,
  p_limit integer,
  p_query_fingerprint text
)
returns jsonb
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
set hnsw.iterative_scan = 'strict_order'
set row_security = 'on'
as $function$
declare
  v_items jsonb;
  v_result jsonb;
begin
  with portal_lexical_matches as materialized (
    select match.id,
      match.version,
      match.term_ordinal
    from private.catalog_portal_hybrid_pattern_matches_v1(
      p_kind,
      p_query_terms
    ) as match
  ), portal_latest_keys as materialized (
    select distinct on (projection.id)
      projection.id,
      projection.version
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = p_kind
    order by projection.id,
      projection.version desc,
      projection.modified_at desc,
      projection.state_code desc
  ), portal_lexical_counts as materialized (
    select portal_lexical_matches.id,
      portal_lexical_matches.version,
      pg_catalog.count(distinct portal_lexical_matches.term_ordinal)::integer
        as lexical_hit_count
    from portal_lexical_matches
    join portal_latest_keys
      on portal_latest_keys.id = portal_lexical_matches.id
     and portal_latest_keys.version = portal_lexical_matches.version
    group by portal_lexical_matches.id,
      portal_lexical_matches.version
  ), portal_lexical_candidates as materialized (
    select portal_lexical_counts.*
    from portal_lexical_counts
    where portal_lexical_counts.lexical_hit_count > 0
    order by portal_lexical_counts.lexical_hit_count desc,
      portal_lexical_counts.id asc,
      portal_lexical_counts.version desc
    limit 200
  ), portal_lexical_ranked as materialized (
    select portal_lexical_candidates.*,
      pg_catalog.row_number() over (
        order by portal_lexical_candidates.lexical_hit_count desc,
          portal_lexical_candidates.id asc,
          portal_lexical_candidates.version desc
      )::integer as lexical_rank
    from portal_lexical_candidates
  ), portal_semantic_process_pool as materialized (
    select candidate.id,
      candidate.version,
      candidate.semantic_distance
    from (
      select projection.id,
        projection.version,
        projection.embedding_ft operator(extensions.<=>) p_query_embedding
          as semantic_distance
      from private.portal_catalog_search_rows_v1 as projection
      where p_kind = 'process'
        and projection.dataset_kind = 'process'
        and projection.embedding_ft is not null
        and not exists (
          select 1
          from private.portal_catalog_search_rows_v1 as newer
          where newer.dataset_kind = projection.dataset_kind
            and newer.id = projection.id
            and (
              newer.version > projection.version
              or (
                newer.version = projection.version
                and newer.modified_at > projection.modified_at
              )
              or (
                newer.version = projection.version
                and newer.modified_at = projection.modified_at
                and newer.state_code > projection.state_code
              )
            )
        )
      order by projection.embedding_ft
        operator(extensions.<=>) p_query_embedding
      limit 200
    ) as candidate
    where candidate.semantic_distance is not null
      and candidate.semantic_distance >= 0::double precision
      and candidate.semantic_distance <= 0.5::double precision
  ), portal_semantic_flow_pool as materialized (
    select candidate.id,
      candidate.version,
      candidate.semantic_distance
    from (
      select projection.id,
        projection.version,
        projection.embedding_ft operator(extensions.<=>) p_query_embedding
          as semantic_distance
      from private.portal_catalog_search_rows_v1 as projection
      where p_kind = 'flow'
        and projection.dataset_kind = 'flow'
        and projection.embedding_ft is not null
        and not exists (
          select 1
          from private.portal_catalog_search_rows_v1 as newer
          where newer.dataset_kind = projection.dataset_kind
            and newer.id = projection.id
            and (
              newer.version > projection.version
              or (
                newer.version = projection.version
                and newer.modified_at > projection.modified_at
              )
              or (
                newer.version = projection.version
                and newer.modified_at = projection.modified_at
                and newer.state_code > projection.state_code
              )
            )
        )
      order by projection.embedding_ft
        operator(extensions.<=>) p_query_embedding
      limit 200
    ) as candidate
    where candidate.semantic_distance is not null
      and candidate.semantic_distance >= 0::double precision
      and candidate.semantic_distance <= 0.5::double precision
  ), portal_semantic_candidates as materialized (
    select * from portal_semantic_process_pool
    union all
    select * from portal_semantic_flow_pool
  ), portal_semantic_ranked as materialized (
    select portal_semantic_candidates.*,
      pg_catalog.row_number() over (
        order by portal_semantic_candidates.semantic_distance asc,
          portal_semantic_candidates.id asc,
          portal_semantic_candidates.version desc
      )::integer as semantic_rank
    from portal_semantic_candidates
  ), portal_fused as materialized (
    select
      coalesce(portal_lexical_ranked.id, portal_semantic_ranked.id) as id,
      coalesce(portal_lexical_ranked.version, portal_semantic_ranked.version)
        as version,
      portal_lexical_ranked.lexical_rank,
      portal_semantic_ranked.semantic_rank,
      portal_semantic_ranked.semantic_distance,
      pg_catalog.round(
        least(
          1::numeric,
          greatest(
            0::numeric,
            (
              coalesce(
                0.5::numeric / (60 + portal_lexical_ranked.lexical_rank),
                0::numeric
              )
              + coalesce(
                0.5::numeric / (60 + portal_semantic_ranked.semantic_rank),
                0::numeric
              )
            ) * 61::numeric
          )
        ),
        12
      ) as normalized_score
    from portal_lexical_ranked
    full outer join portal_semantic_ranked
      on portal_semantic_ranked.id = portal_lexical_ranked.id
     and portal_semantic_ranked.version = portal_lexical_ranked.version
  ), portal_fused_decorated as materialized (
    select portal_fused.*,
      projection.card,
      projection.state_code,
      projection.modified_at
    from portal_fused
    join private.portal_catalog_search_rows_v1 as projection
      on projection.dataset_kind = p_kind
     and projection.id = portal_fused.id
     and projection.version = portal_fused.version
  ), portal_filtered as materialized (
    select portal_fused_decorated.*
    from portal_fused_decorated
    where (
        not (p_filters ? 'accessLevel')
        or portal_fused_decorated.card ->> 'accessLevel'
          = p_filters ->> 'accessLevel'
      )
      and (
        not (p_filters ? 'geography')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_fused_decorated.card #>> '{geography,code}',
          ''
        ))) = p_filters ->> 'geography'
      )
      and (
        not (p_filters ? 'classification')
        or exists (
          select 1
          from pg_catalog.jsonb_array_elements(coalesce(
            portal_fused_decorated.card -> 'classifications',
            '[]'::jsonb
          )) as classification(item)
          where pg_catalog.lower(pg_catalog.btrim(classification.item ->> 'code'))
            = p_filters ->> 'classification'
        )
      )
      and (
        not (p_filters ? 'referenceYearFrom')
        or (portal_fused_decorated.card ->> 'referenceYear')::integer
          >= (p_filters ->> 'referenceYearFrom')::integer
      )
      and (
        not (p_filters ? 'referenceYearTo')
        or (portal_fused_decorated.card ->> 'referenceYear')::integer
          <= (p_filters ->> 'referenceYearTo')::integer
      )
      and (
        not (p_filters ? 'processSubtype')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_fused_decorated.card ->> 'processSubtype',
          ''
        ))) = p_filters ->> 'processSubtype'
      )
      and (
        not (p_filters ? 'source')
        or pg_catalog.lower(pg_catalog.btrim(coalesce(
          portal_fused_decorated.card ->> 'source',
          ''
        ))) = p_filters ->> 'source'
      )
  ), portal_ordered as materialized (
    select portal_filtered.*
    from portal_filtered
    order by portal_filtered.normalized_score desc,
      portal_filtered.id asc,
      portal_filtered.version desc
    limit p_limit
  )
  select coalesce(
    pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'key', pg_catalog.jsonb_build_object(
          'kind', p_kind,
          'id', portal_ordered.id::text,
          'version', portal_ordered.version
        ),
        'accessLevel', portal_ordered.card -> 'accessLevel',
        'capabilities', portal_ordered.card -> 'capabilities',
        'names', portal_ordered.card -> 'names',
        'summary', portal_ordered.card -> 'summary',
        'geography', portal_ordered.card -> 'geography',
        'referenceYear', portal_ordered.card -> 'referenceYear',
        'modifiedAt', pg_catalog.to_char(
          portal_ordered.modified_at at time zone 'UTC',
          'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
        ),
        'match', pg_catalog.jsonb_build_object(
          'kind', 'hybrid',
          'algorithmVersion', 'portal-hybrid-rank-v1',
          'score', portal_ordered.normalized_score,
          'reasonCodes', pg_catalog.to_jsonb(pg_catalog.array_remove(array[
            case when portal_ordered.lexical_rank is not null
              then 'lexical_public_projection'::text end,
            case when portal_ordered.semantic_rank is not null
              then 'semantic_public_projection'::text end
          ], null)),
          'evidence', pg_catalog.jsonb_build_object(
            'lexicalRank', portal_ordered.lexical_rank,
            'semanticRank', portal_ordered.semantic_rank,
            'semanticDistance', case
              when portal_ordered.semantic_distance is null then null
              else pg_catalog.trim_scale(
                portal_ordered.semantic_distance::numeric
              )::text
            end
          )
        )
      )
      order by portal_ordered.normalized_score desc,
        portal_ordered.id asc,
        portal_ordered.version desc
    ),
    '[]'::jsonb
  )
  into v_items
  from portal_ordered;

  v_result := pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-hybrid-candidate-page.v1',
    'kind', p_kind,
    'queryFingerprint', p_query_fingerprint,
    'items', v_items
  );
  if pg_catalog.octet_length(
    pg_catalog.convert_to(v_result::text, 'UTF8')
  ) > 524288 then
    raise exception using
      errcode = '54000',
      message = 'portal hybrid response too large';
  end if;
  return v_result;
end
$function$;

comment on function private.portal_public_hybrid_search_v1_impl(
  text, text[], extensions.vector, jsonb, integer, text
) is
  'Projection-backed portal-hybrid-rank-v1: exact literal lexical matches and latest-only semantic candidates use private PGroonga/HNSW indexes before fusion/card filters.';

reset role;
revoke create on schema private from api_internal_executor;
revoke api_internal_executor from postgres;

grant portal_public_executor to postgres;
grant create on schema private to portal_public_executor;
set role portal_public_executor;

create or replace function private.portal_search_v1(
  p_kind text,
  p_query text,
  p_filters jsonb,
  p_sort text,
  p_cursor text,
  p_limit integer
)
returns jsonb
language plpgsql
stable
parallel restricted
set search_path = ''
as $function$
declare
  v_query text;
  v_filters jsonb;
  v_sort text;
  v_limit integer := coalesce(p_limit, 20);
  v_fingerprint text;
  v_cursor jsonb;
  v_cursor_rank text;
  v_cursor_id uuid;
  v_cursor_version text;
  v_kernel jsonb;
  v_next_cursor_payload jsonb;
begin
  perform private.portal_validate_search_v1(
    p_kind,
    coalesce(p_query, ''),
    coalesce(p_filters, '{}'::jsonb),
    coalesce(p_sort, 'relevance'),
    v_limit
  );
  v_query := pg_catalog.lower(pg_catalog.btrim(coalesce(p_query, '')));
  v_filters := private.portal_normalize_filters_v1(p_filters);
  v_sort := pg_catalog.lower(pg_catalog.btrim(coalesce(p_sort, 'relevance')));
  v_fingerprint := private.portal_query_fingerprint_v1(
    p_kind,
    v_query,
    v_filters,
    v_sort
  );
  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
       or (select count(*) from pg_catalog.jsonb_object_keys(v_cursor)) <> 6
       or v_cursor ->> 'v' <> '1'
       or v_cursor ->> 'fp' <> v_fingerprint
       or v_cursor ->> 'kind' <> p_kind
       or coalesce(v_cursor ->> 'rankKey', '') = ''
       or coalesce(v_cursor ->> 'id', '')
         !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or coalesce(v_cursor ->> 'version', '') !~ '^\d{2}\.\d{2}\.\d{3}$' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
    v_cursor_rank := v_cursor ->> 'rankKey';
    v_cursor_id := (v_cursor ->> 'id')::uuid;
    v_cursor_version := v_cursor ->> 'version';
    if v_sort = 'relevance'
       and v_cursor_rank !~ '^(0(\.\d+)?|1(\.0+)?)$' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    elsif v_sort = 'modified_desc'
       and private.portal_datetime_v1(v_cursor_rank) is null then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end if;

  v_kernel := private.catalog_portal_search_v1_impl(
    p_kind,
    v_query,
    v_filters,
    v_sort,
    v_cursor_rank,
    v_cursor_id,
    v_cursor_version,
    v_limit,
    v_fingerprint
  );
  v_next_cursor_payload := nullif(
    v_kernel -> 'nextCursorPayload',
    'null'::jsonb
  );

  return pg_catalog.jsonb_build_object(
    'schemaVersion', 'portal.public-search-page.v1',
    'kind', p_kind,
    'queryFingerprint', v_fingerprint,
    'items', coalesce(v_kernel -> 'items', '[]'::jsonb),
    'nextCursor', case when v_next_cursor_payload is null then null
      else private.portal_cursor_encode_v1(v_next_cursor_payload)
    end
  );
end
$function$;

reset role;
revoke create on schema private from portal_public_executor;
revoke portal_public_executor from postgres;

do $verify_portal_candidate_contract$
declare
  v_catalog regprocedure := pg_catalog.to_regprocedure(
    'private.catalog_portal_search_v1_impl(text,text,jsonb,text,text,uuid,text,integer,text)'
  );
  v_document regprocedure := pg_catalog.to_regprocedure(
    'private.catalog_portal_document_v1(text,jsonb)'
  );
  v_facts regprocedure := pg_catalog.to_regprocedure(
    'private.catalog_portal_facts_v1(text,integer,jsonb,jsonb,text)'
  );
  v_card_facts regprocedure := pg_catalog.to_regprocedure(
    'private.catalog_portal_card_facts_v1(jsonb,jsonb,text)'
  );
  v_candidates regprocedure := pg_catalog.to_regprocedure(
    'private.catalog_portal_candidate_rows_v1(text,text,uuid,text)'
  );
  v_process_pattern regprocedure := pg_catalog.to_regprocedure(
    'private.catalog_portal_process_pattern_versions_v1(text)'
  );
  v_flow_pattern regprocedure := pg_catalog.to_regprocedure(
    'private.catalog_portal_flow_pattern_versions_v1(text)'
  );
  v_hybrid_pattern regprocedure := pg_catalog.to_regprocedure(
    'private.catalog_portal_hybrid_pattern_matches_v1(text,text[])'
  );
  v_search regprocedure := pg_catalog.to_regprocedure(
    'private.portal_search_v1(text,text,jsonb,text,text,integer)'
  );
  v_hybrid regprocedure := pg_catalog.to_regprocedure(
    'private.portal_public_hybrid_search_v1_impl(text,text[],extensions.vector,jsonb,integer,text)'
  );
begin
  if v_catalog is null
     or v_document is null
     or v_facts is null
     or v_card_facts is null
     or v_candidates is null
     or v_process_pattern is null
     or v_flow_pattern is null
     or v_hybrid_pattern is null
     or v_search is null
     or v_hybrid is null then
    raise exception 'Portal candidate-first installation is incomplete';
  end if;
  if (
    select routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and routine.provolatile = 'i'
      and coalesce(routine.proconfig, '{}'::text[])
        @> array['search_path=""']::text[]
      and pg_catalog.md5(pg_catalog.pg_get_functiondef(routine.oid))
        = 'cc17549d0fc9686036c35457490585c3'
      and coalesce(routine.proacl::text, '')
        = '{portal_public_executor=X/portal_public_executor,postgres=X/portal_public_executor}'
    from pg_catalog.pg_proc as routine
    where routine.oid = v_document
  ) is not true then
    raise exception 'Portal candidate document owner/config mismatch';
  end if;
  if (
    select routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and routine.provolatile = 'i'
      and coalesce(routine.proconfig, '{}'::text[])
        @> array['search_path=""']::text[]
      and coalesce(routine.proacl::text, '')
        = '{portal_public_executor=X/portal_public_executor,api_internal_executor=X/portal_public_executor}'
    from pg_catalog.pg_proc as routine
    where routine.oid = v_facts
  ) is not true then
    raise exception 'Portal candidate facts owner/config/ACL mismatch';
  end if;
  if (
    select routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and routine.provolatile = 'i'
      and coalesce(routine.proconfig, '{}'::text[])
        @> array['search_path=""']::text[]
      and coalesce(routine.proacl::text, '')
        = '{portal_public_executor=X/portal_public_executor,api_internal_executor=X/portal_public_executor}'
    from pg_catalog.pg_proc as routine
    where routine.oid = v_card_facts
  ) is not true then
    raise exception 'Portal stored-card facts owner/config/ACL mismatch';
  end if;
  if (
    select routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and coalesce(routine.proconfig, '{}'::text[])
        @> array[
          'search_path=""',
          'statement_timeout=8s',
          'plan_cache_mode=force_custom_plan',
          'row_security=on'
        ]::text[]
      and coalesce(routine.proacl::text, '')
        = '{portal_public_executor=X/portal_public_executor,api_internal_executor=X/portal_public_executor}'
    from pg_catalog.pg_proc as routine
    where routine.oid = v_candidates
  ) is not true then
    raise exception 'Portal candidate rows owner/config/ACL mismatch';
  end if;
  if exists (
    select 1
    from pg_catalog.pg_proc as routine
    where routine.oid = any (array[v_process_pattern::oid, v_flow_pattern::oid])
      and not (
        routine.proowner = 'portal_public_executor'::regrole
        and routine.prosecdef
        and coalesce(routine.proconfig, '{}'::text[])
          @> array[
            'search_path=""',
            'statement_timeout=8s',
            'plan_cache_mode=force_custom_plan',
            'row_security=on'
          ]::text[]
        and coalesce(routine.proacl::text, '')
          = '{portal_public_executor=X/portal_public_executor}'
        and pg_catalog.strpos(routine.prosrc, '%L') > 0
        and pg_catalog.strpos(routine.prosrc, '%I') = 0
      )
  ) then
    raise exception 'Portal pattern helper owner/config/ACL mismatch';
  end if;
  if (
    select routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and coalesce(routine.proconfig, '{}'::text[])
        @> array[
          'search_path=""',
          'statement_timeout=8s',
          'plan_cache_mode=force_custom_plan',
          'row_security=on'
        ]::text[]
      and coalesce(routine.proacl::text, '')
        = '{portal_public_executor=X/portal_public_executor,api_internal_executor=X/portal_public_executor}'
    from pg_catalog.pg_proc as routine
    where routine.oid = v_hybrid_pattern
  ) is not true then
    raise exception 'Portal Hybrid pattern helper owner/config/ACL mismatch';
  end if;
  if (
    select routine.proowner = 'api_internal_executor'::regrole
      and routine.prosecdef
      and coalesce(routine.proconfig, '{}'::text[])
        @> array[
          'search_path=""',
          'statement_timeout=8s',
          'plan_cache_mode=force_custom_plan'
        ]::text[]
    from pg_catalog.pg_proc as routine
    where routine.oid = v_catalog
  ) is not true then
    raise exception 'Portal candidate catalog kernel owner/config mismatch';
  end if;
  if (
    select routine.proowner = 'portal_public_executor'::regrole
      and not routine.prosecdef
      and coalesce(routine.proconfig, '{}'::text[])
        @> array['search_path=""']::text[]
    from pg_catalog.pg_proc as routine
    where routine.oid = v_search
  ) is not true then
    raise exception 'Portal search coordinator owner/config mismatch';
  end if;
  if (
    select routine.proowner = 'api_internal_executor'::regrole
      and routine.prosecdef
      and coalesce(routine.proconfig, '{}'::text[])
        @> array[
          'search_path=""',
          'statement_timeout=8s',
          'plan_cache_mode=force_custom_plan',
          'hnsw.iterative_scan=strict_order',
          'row_security=on'
        ]::text[]
    from pg_catalog.pg_proc as routine
    where routine.oid = v_hybrid
  ) is not true then
    raise exception 'Portal Hybrid candidate kernel owner/config mismatch';
  end if;
  if (
    select relation.relrowsecurity
      and relation.relforcerowsecurity
      and relation.relowner = 'postgres'::regrole
    from pg_catalog.pg_class as relation
    where relation.oid = 'private.portal_catalog_search_rows_v1'::regclass
  ) is not true
     or pg_catalog.has_table_privilege(
       'anon', 'private.portal_catalog_search_rows_v1', 'SELECT'
     )
     or pg_catalog.has_table_privilege(
       'authenticated', 'private.portal_catalog_search_rows_v1', 'SELECT'
     )
     or pg_catalog.has_table_privilege(
       'service_role', 'private.portal_catalog_search_rows_v1', 'SELECT'
     )
     or not pg_catalog.has_table_privilege(
       'api_internal_executor',
       'private.portal_catalog_search_rows_v1',
       'SELECT,INSERT,UPDATE,DELETE'
     ) then
    raise exception 'Portal projection relation RLS/ACL mismatch';
  end if;
  if (
    select count(*)
    from pg_catalog.pg_policies
    where schemaname = 'private'
      and tablename = 'portal_catalog_search_rows_v1'
      and policyname in (
        'portal_catalog_search_rows_portal_select_v1',
        'portal_catalog_search_rows_internal_all_v1'
      )
  ) <> 2 then
    raise exception 'Portal projection policy set mismatch';
  end if;
  if (
    select routine.proowner = 'api_internal_executor'::regrole
      and routine.prosecdef
      and coalesce(routine.proconfig, '{}'::text[])
        @> array['search_path=""', 'row_security=on']::text[]
    from pg_catalog.pg_proc as routine
    where routine.oid = 'private.sync_portal_catalog_search_row_v1()'::regprocedure
  ) is not true
     or pg_catalog.has_function_privilege(
       'anon', 'private.sync_portal_catalog_search_row_v1()', 'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated', 'private.sync_portal_catalog_search_row_v1()', 'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role', 'private.sync_portal_catalog_search_row_v1()', 'EXECUTE'
     ) then
    raise exception 'Portal projection trigger writer mismatch';
  end if;
  if (
    select count(*)
    from pg_catalog.pg_trigger as trigger
    where trigger.tgrelid in (
        'public.processes'::regclass,
        'public.flows'::regclass
      )
      and trigger.tgname in (
        'portal_catalog_projection_content_sync_v1',
        'portal_catalog_projection_embedding_sync_v1'
      )
      and not trigger.tgisinternal
      and trigger.tgenabled = 'O'
  ) <> 4 then
    raise exception 'Portal projection trigger set mismatch';
  end if;
  if (
    select routine.prosrc ~ 'public\.processes|public\.flows'
    from pg_catalog.pg_proc as routine
    where routine.oid = v_catalog
  ) or exists (
    select 1
    from pg_catalog.pg_proc as routine
    where routine.oid = any (array[
        v_candidates::oid,
        v_process_pattern::oid,
        v_flow_pattern::oid,
        v_hybrid::oid
      ])
      and (
        routine.prosrc ~ 'public\.processes|public\.flows'
        or routine.prosrc !~ 'portal_catalog_search_rows_v1'
      )
  ) then
    raise exception 'Portal cutover kernel still reads raw source tables';
  end if;
  if not pg_catalog.has_function_privilege(
       'portal_public_executor', v_catalog, 'EXECUTE'
     )
     or pg_catalog.has_function_privilege('anon', v_catalog, 'EXECUTE')
     or pg_catalog.has_function_privilege('authenticated', v_catalog, 'EXECUTE')
     or pg_catalog.has_function_privilege('service_role', v_catalog, 'EXECUTE')
     or not pg_catalog.has_function_privilege(
       'postgres', v_document, 'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'api_internal_executor', v_document, 'EXECUTE'
     )
     or pg_catalog.has_function_privilege('anon', v_document, 'EXECUTE')
     or pg_catalog.has_function_privilege('authenticated', v_document, 'EXECUTE')
     or pg_catalog.has_function_privilege(
       'service_role', v_document, 'EXECUTE'
     ) then
    raise exception 'Portal candidate-first private ACL mismatch';
  end if;
end
$verify_portal_candidate_contract$;

do $verify_portal_candidate_preservation$
begin
  if exists (
    select 1
    from portal_candidate_external_before as before
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(before.routine_identity)
    join pg_catalog.pg_roles as owner_role
      on owner_role.oid = routine.proowner
    where before.definition <> pg_catalog.pg_get_functiondef(routine.oid)
       or before.owner_name <> owner_role.rolname
       or before.security_definer <> routine.prosecdef
       or before.proconfig <> coalesce(routine.proconfig, '{}'::text[])
       or before.acl_text <> coalesce(routine.proacl::text, '')
  ) or (
    select count(*)
    from portal_candidate_external_before as before
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(before.routine_identity)
  ) <> 3 then
    raise exception 'Portal external wrapper contract changed';
  end if;

  if exists (
    select 1
    from portal_candidate_legacy_before as before
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(before.routine_identity)
    join pg_catalog.pg_roles as owner_role
      on owner_role.oid = routine.proowner
    where before.definition <> pg_catalog.pg_get_functiondef(routine.oid)
       or before.owner_name <> owner_role.rolname
       or before.security_definer <> routine.prosecdef
       or before.proconfig <> coalesce(routine.proconfig, '{}'::text[])
       or before.acl_text <> coalesce(routine.proacl::text, '')
  ) or (
    select count(*)
    from portal_candidate_legacy_before as before
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(before.routine_identity)
  ) <> 8 then
    raise exception 'Existing raw Hybrid routine contract changed';
  end if;
end
$verify_portal_candidate_preservation$;

commit;
