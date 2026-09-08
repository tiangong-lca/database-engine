-- Database #628: additive composite-name projection. Frozen v1 remains intact.
begin;
set local lock_timeout = '5s';
set local statement_timeout = '120s';
set local check_function_bodies = off;
select extensions.vector_dims('[1]'::extensions.vector);
grant portal_public_executor, api_internal_executor to postgres;
grant create on schema private to portal_public_executor, api_internal_executor;
create function private.portal_process_names_v1(p_json jsonb) returns jsonb
language sql immutable parallel safe set search_path = '' as $names$
  with parts as (
    select part.ordinality as part_order, item.ordinality as item_order,
      item.value ->> 'language' as language,
      pg_catalog.lower(item.value ->> 'language') as language_key,
      item.value ->> 'value' as value
    from pg_catalog.unnest(array[
      'baseName', 'treatmentStandardsRoutes', 'mixAndLocationTypes',
      'functionalUnitFlowProperties'
    ]) with ordinality as part(field, ordinality)
    cross join lateral pg_catalog.jsonb_array_elements(
      private.portal_localized_text_v1(
        p_json #> array['processDataSet','processInformation','dataSetInformation','name',part.field]
      )
    ) with ordinality as item(value, ordinality)
  ), first_values as (
    select distinct on (part_order, language_key) * from parts
    order by part_order, language_key, item_order
  ), names as (
    select base.language, base.item_order,
      pg_catalog.string_agg(part.value, '; ' order by part.part_order) as value
    from first_values as base
    join first_values as part on part.language_key = base.language_key
    where base.part_order = 1
    group by base.language, base.item_order
  )
  select coalesce(pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_object('language', language, 'value', value)
    order by item_order
  ), '[]'::jsonb) from names
$names$;
alter function private.portal_process_names_v1(jsonb) owner to portal_public_executor;
revoke all on function private.portal_process_names_v1(jsonb) from public;
CREATE OR REPLACE FUNCTION private.assert_portal_catalog_character_contract_cn1()
 RETURNS void
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET row_security TO 'on'
AS $function$
begin
  if (
       select not relation.relrowsecurity
         or not relation.relforcerowsecurity
         or relation.relowner <> 'postgres'::regrole
       from pg_catalog.pg_class as relation
       where relation.oid =
         'private.portal_catalog_character_rows_v2'::regclass
     ) is not false
     or (
       select count(*)
       from pg_catalog.pg_attribute as attribute
       where attribute.attrelid =
           'private.portal_catalog_character_rows_v2'::regclass
         and attribute.attnum > 0
         and not attribute.attisdropped
         and attribute.attname in (
           'dataset_kind',
           'id',
           'version',
           'state_code',
           'modified_at',
           'document_characters',
           'name_characters',
           'name_exact_characters',
           'classification_characters',
           'classification_exact_characters',
           'character_contract_version'
         )
     ) <> 11
     or not exists (
       select 1
       from pg_catalog.pg_constraint as parent_fk
       where parent_fk.conrelid =
           'private.portal_catalog_character_rows_v2'::regclass
         and parent_fk.confrelid =
           'private.portal_catalog_search_rows_v2'::regclass
         and parent_fk.conname = 'portal_catalog_character_parent_v2_fk'
         and parent_fk.contype = 'f'
         and parent_fk.convalidated
         and parent_fk.confupdtype = 'r'
         and parent_fk.confdeltype = 'c'
     )
     or (
       select not index_record.indisvalid
         or not index_record.indisready
         or not index_record.indislive
       from pg_catalog.pg_index as index_record
       where index_record.indexrelid =
         'private.portal_catalog_character_rows_latest_v2_idx'::regclass
     ) is not false
     or not exists (
       select 1
       from pg_catalog.pg_trigger as trigger
       where trigger.tgrelid =
           'private.portal_catalog_search_rows_v2'::regclass
         and trigger.tgname = 'portal_catalog_character_sync_v2'
         and not trigger.tgisinternal
         and pg_catalog.pg_get_triggerdef(trigger.oid) ~
           'AFTER INSERT OR UPDATE'
     )
     or (
       select count(*)
       from pg_catalog.pg_policies as policy
       where policy.schemaname = 'private'
         and policy.tablename = 'portal_catalog_character_rows_v2'
         and policy.policyname =
           'portal_catalog_character_rows_portal_select_v2'
         and policy.roles = array['portal_public_executor']::name[]
         and policy.cmd = 'SELECT'
         and policy.qual = 'true'
         and policy.with_check is null
     ) <> 1
     or (
       select count(*)
       from pg_catalog.pg_proc as routine
       where routine.oid in (
           'private.portal_catalog_character_set_v1(text)'::regprocedure,
           'private.portal_catalog_character_field_set_v1(jsonb,text,boolean)'::regprocedure
         )
         and routine.proowner = 'portal_public_executor'::regrole
         and routine.provolatile = 'i'
         and routine.proparallel = 's'
         and routine.prosecdef
         and routine.proconfig @> array['search_path=""']::text[]
     ) <> 2
     or (
       select routine.proowner <> 'api_internal_executor'::regrole
         or not routine.prosecdef
         or not (coalesce(routine.proconfig, '{}'::text[]) @> array[
           'search_path=""',
           'row_security=on'
         ]::text[])
       from pg_catalog.pg_proc as routine
       where routine.oid =
         'private.sync_portal_catalog_character_row_cn1()'::regprocedure
     ) is not false then
    raise exception using
      errcode = '55000',
      message = 'Portal character projection contract drifted';
  end if;
end
$function$
;
alter function private.assert_portal_catalog_character_contract_cn1() owner to portal_public_executor;
revoke all on function private.assert_portal_catalog_character_contract_cn1() from public;
grant execute on function private.assert_portal_catalog_character_contract_cn1() to api_internal_executor;

CREATE OR REPLACE FUNCTION private.assert_portal_catalog_projection_contract_cn1()
 RETURNS void
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET row_security TO 'on'
AS $function$
declare
  v_expected_identities constant text[] := array[
    'private.catalog_portal_projection_payload_cn1(text,integer,jsonb)',
    'private.portal_catalog_card_cn1(text,integer,jsonb)',
    'private.portal_capabilities_v1(text,integer,jsonb)',
    'private.portal_publication_root_v1(text,jsonb)',
    'private.portal_access_restrictions_open_v1(jsonb)',
    'private.portal_scalar_text_v1(jsonb)',
    'private.portal_localized_text_v1(jsonb)',
    'private.portal_json_items_v1(jsonb)',
    'private.portal_classifications_v1(jsonb)',
    'private.portal_safe_year_v1(text)',
    'private.portal_source_v1(text,jsonb)',
    'private.portal_process_names_v1(jsonb)'
  ]::text[];
  v_expected_digest constant text :=
    '5260ed0b5662bf6b4bdae5250d0971fca369d8f36766f32207df47acd68e3500';
  v_live_digest text;
begin
  perform private.assert_portal_catalog_projection_contract_v1();
  -- Flow remains on frozen V1. Pin the zero-storage routing definitions and
  -- invoker security so the mixed-kind readers cannot bypass either contract.
  if (select count(*) from pg_catalog.pg_class c where
      c.relowner='postgres'::regrole and c.relkind='v'
      and c.reloptions @> array['security_invoker=true']::text[]
      and ((c.oid='private.portal_catalog_search_current_v2'::regclass
            and pg_catalog.md5(pg_catalog.pg_get_viewdef(c.oid,true))='1c2b797978f82e0c3db4975b57426e5e')
        or (c.oid='private.portal_catalog_character_current_v2'::regclass
            and pg_catalog.md5(pg_catalog.pg_get_viewdef(c.oid,true))='af8d2dfd90e14b2e06d7d95548f0d97d'))) <> 2 then
    raise exception using errcode='55000', message='Portal name routing contract drifted';
  end if;
  select private.portal_catalog_projection_manifest_sha256_cn1()
  into v_live_digest;

  if v_live_digest is distinct from v_expected_digest
     or (
       select count(*)
       from private.portal_catalog_projection_contract_v2 as contract
       where contract.contract_version = 2
         and contract.manifest_schema =
           'portal.catalog-projection-function-manifest.v2'
         and contract.function_identities = v_expected_identities
         and contract.manifest_sha256 = v_expected_digest
         and contract.created_by_migration = '20260908090000'
     ) <> 1
     or (
       select count(*)
       from private.portal_catalog_projection_contract_v2
     ) <> 1
     or (
       select not relation.relrowsecurity
         or not relation.relforcerowsecurity
         or relation.relowner <> 'postgres'::regrole
       from pg_catalog.pg_class as relation
       where relation.oid =
         'private.portal_catalog_projection_contract_v2'::regclass
     ) is not false
     or (
       select not relation.relrowsecurity
         or not relation.relforcerowsecurity
         or relation.relowner <> 'postgres'::regrole
       from pg_catalog.pg_class as relation
       where relation.oid =
         'private.portal_catalog_search_rows_v2'::regclass
     ) is not false
     or not exists (
       select 1
       from pg_catalog.pg_constraint as state_check
       where state_check.conrelid =
           'private.portal_catalog_search_rows_v2'::regclass
         and state_check.conname =
           'portal_catalog_search_rows_v1_state_code_check'
         and state_check.contype = 'c'
         and state_check.convalidated
         and pg_catalog.regexp_replace(
           pg_catalog.pg_get_expr(
             state_check.conbin,
             state_check.conrelid
           ),
           '[[:space:]]',
           '',
           'g'
         ) = '(state_code=ANY(ARRAY[100,200]))'
     )
     or (
       select count(*)
       from pg_catalog.pg_policies as policy
       where policy.schemaname = 'private'
         and policy.tablename = 'portal_catalog_search_rows_v2'
         and policy.policyname =
           'portal_catalog_search_rows_portal_select_v2'
         and policy.permissive = 'PERMISSIVE'
         and policy.roles = array['portal_public_executor']::name[]
         and policy.cmd = 'SELECT'
         and policy.qual = 'true'
         and policy.with_check is null
     ) <> 1
     or (
       select count(*)
       from pg_catalog.pg_policies as policy
       where policy.schemaname = 'private'
         and policy.tablename = 'portal_catalog_search_rows_v2'
         and policy.roles @> array['portal_public_executor']::name[]
     ) <> 1
     or not exists (
       select 1
       from pg_catalog.pg_attribute as attribute
       where attribute.attrelid =
         'private.portal_catalog_search_rows_v2'::regclass
         and attribute.attname = 'projection_contract_version'
         and attribute.atttypid = 'pg_catalog.int2'::regtype
         and attribute.attnotnull
         and not attribute.atthasdef
         and not attribute.attisdropped
     )
     or not exists (
       select 1
       from pg_catalog.pg_constraint as contract_check
       where contract_check.conrelid =
           'private.portal_catalog_search_rows_v2'::regclass
         and contract_check.conname =
           'portal_catalog_search_rows_contract_version_v2_chk'
         and contract_check.contype = 'c'
         and contract_check.convalidated
         and pg_catalog.regexp_replace(
           pg_catalog.pg_get_expr(
             contract_check.conbin,
             contract_check.conrelid
           ),
           '[[:space:]]',
           '',
           'g'
         ) = '(projection_contract_version=2)'
     )
     or not exists (
       select 1
       from pg_catalog.pg_constraint as contract_fk
       where contract_fk.conrelid =
           'private.portal_catalog_search_rows_v2'::regclass
         and contract_fk.confrelid =
           'private.portal_catalog_projection_contract_v2'::regclass
         and contract_fk.conname =
           'portal_catalog_search_rows_contract_version_v2_fk'
         and contract_fk.contype = 'f'
         and contract_fk.convalidated
         and contract_fk.confupdtype = 'r'
         and contract_fk.confdeltype = 'r'
         and contract_fk.conkey = array[(
           select attribute.attnum
           from pg_catalog.pg_attribute as attribute
           where attribute.attrelid = contract_fk.conrelid
             and attribute.attname = 'projection_contract_version'
         )]::smallint[]
         and contract_fk.confkey = array[(
           select attribute.attnum
           from pg_catalog.pg_attribute as attribute
           where attribute.attrelid = contract_fk.confrelid
             and attribute.attname = 'contract_version'
         )]::smallint[]
     ) then
    raise exception using
      errcode = '55000',
      message = 'Portal projection derivation contract drifted';
  end if;
end
$function$
;
alter function private.assert_portal_catalog_projection_contract_cn1() owner to api_internal_executor;
revoke all on function private.assert_portal_catalog_projection_contract_cn1() from public;
grant execute on function private.assert_portal_catalog_projection_contract_cn1() to portal_public_executor;

CREATE OR REPLACE FUNCTION private.assert_portal_process_keyword_rank_contract_cn1()
 RETURNS void
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET row_security TO 'on'
AS $function$
declare
  v_expected_digest constant text :=
    '9686f14020c08ebfdd7a7cd0b049d8e0ea8b6acf9710d16fbe0bf3ca45510821';
  v_expected_index constant text :=
    'CREATE INDEX portal_catalog_search_process_exact_rank_v2_gin ON private.portal_catalog_search_rows_v2 USING gin (private.portal_process_rank_name_keys_v1(card), private.portal_process_rank_classification_keys_v1(card)) WHERE (dataset_kind = ''process''::text)';
begin
  perform private.assert_portal_catalog_projection_contract_cn1();
  if private.portal_process_keyword_rank_manifest_sha256_cn1()
       is distinct from v_expected_digest
     or pg_catalog.to_regclass(
       'private.portal_catalog_search_process_exact_rank_v2_gin'
     ) is null
     or (
       select not index_catalog.indisvalid
         or not index_catalog.indisready
         or not index_catalog.indislive
         or index_catalog.indisunique
         or access_method.amname <> 'gin'
         or pg_catalog.pg_get_indexdef(index_relation.oid)
           <> v_expected_index
       from pg_catalog.pg_class as index_relation
       join pg_catalog.pg_index as index_catalog
         on index_catalog.indexrelid = index_relation.oid
       join pg_catalog.pg_am as access_method
         on access_method.oid = index_relation.relam
       where index_relation.oid =
         'private.portal_catalog_search_process_exact_rank_v2_gin'::regclass
     ) is not false then
    raise exception using
      errcode = '55000',
      message = 'Portal Process keyword rank contract drifted';
  end if;
end
$function$
;
alter function private.assert_portal_process_keyword_rank_contract_cn1() owner to portal_public_executor;
revoke all on function private.assert_portal_process_keyword_rank_contract_cn1() from public;

CREATE OR REPLACE FUNCTION private.catalog_portal_process_keyword_keys_cn1(p_query text, p_cursor_rank text, p_cursor_id uuid, p_cursor_version text, p_limit integer)
 RETURNS TABLE(id uuid, version text, score numeric)
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
 SET plan_cache_mode TO 'force_custom_plan'
 SET row_security TO 'on'
AS $function$
declare
  v_like_pattern text;
begin
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

  return query
  with matched_versions as materialized (
    select matched.id, matched.version
    from private.catalog_portal_process_pattern_versions_v1(
      v_like_pattern
    ) as matched
  ), candidate_ids as materialized (
    select distinct matched.id
    from matched_versions as matched
  ), latest_keys as materialized (
    select distinct on (projection.id)
      projection.id,
      projection.version
    from private.portal_catalog_search_rows_v2 as projection
    join candidate_ids using (id)
    where projection.dataset_kind = 'process'
    order by projection.id,
      projection.version desc,
      projection.modified_at desc,
      projection.state_code desc
  ), eligible_keys as materialized (
    select latest.id, latest.version
    from latest_keys as latest
    join matched_versions as matched
      on matched.id = latest.id
     and matched.version = latest.version
  ), exact_source as materialized (
    select projection.id,
      projection.version,
      case
        when private.portal_process_rank_name_keys_v1(projection.card)
          @> array[p_query] then 0.95::numeric
        else 0.92::numeric
      end as score
    from private.portal_catalog_search_rows_v2 as projection
    where projection.dataset_kind = 'process'
      and (
        private.portal_process_rank_name_keys_v1(projection.card)
          @> array[p_query]
        or private.portal_process_rank_classification_keys_v1(
          projection.card
        ) @> array[p_query]
      )
  ), exact_keys as materialized (
    select exact_source.*
    from exact_source
    join eligible_keys using (id, version)
    where p_cursor_rank is null
      or exact_source.score < p_cursor_rank::numeric
      or (
        exact_source.score = p_cursor_rank::numeric
        and (
          exact_source.id > p_cursor_id
          or (
            exact_source.id = p_cursor_id
            and exact_source.version < p_cursor_version
          )
        )
      )
  ), general_keys as materialized (
    select eligible.id, eligible.version, 0.70::numeric as score
    from eligible_keys as eligible
    left join exact_source using (id, version)
    where exact_source.id is null
      and (
        p_cursor_rank is null
        or 0.70::numeric < p_cursor_rank::numeric
        or (
          0.70::numeric = p_cursor_rank::numeric
          and (
            eligible.id > p_cursor_id
            or (
              eligible.id = p_cursor_id
              and eligible.version < p_cursor_version
            )
          )
        )
      )
    order by eligible.id, eligible.version desc
    limit p_limit + 1
  ), combined as (
    select exact_keys.* from exact_keys
    union all
    select general_keys.* from general_keys
  )
  select combined.id, combined.version, combined.score
  from combined
  order by combined.score desc, combined.id, combined.version desc
  limit p_limit + 1;
end
$function$
;
alter function private.catalog_portal_process_keyword_keys_cn1(text,text,uuid,text,integer) owner to portal_public_executor;
revoke all on function private.catalog_portal_process_keyword_keys_cn1(text,text,uuid,text,integer) from public;

CREATE OR REPLACE FUNCTION private.catalog_portal_process_keyword_relevance_cn1_impl(p_query text, p_cursor_rank text, p_cursor_id uuid, p_cursor_version text, p_limit integer, p_query_fingerprint text)
 RETURNS jsonb
 LANGUAGE sql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '8s'
 SET plan_cache_mode TO 'force_custom_plan'
 SET row_security TO 'on'
AS $function$
  with selected_keys as materialized (
    select selected.id, selected.version, selected.score,
      pg_catalog.row_number() over (
        order by selected.score desc, selected.id, selected.version desc
      ) as page_rank
    from private.catalog_portal_process_keyword_keys_cn1(
      p_query,
      p_cursor_rank,
      p_cursor_id,
      p_cursor_version,
      p_limit
    ) as selected
  ), hydrated as materialized (
    select selected.page_rank,
      projection.id,
      projection.version,
      projection.modified_at,
      projection.card,
      selected.score,
      private.catalog_portal_card_facts_v1(
        projection.card,
        '{}'::jsonb,
        p_query
      ) as facts
    from selected_keys as selected
    join private.portal_catalog_search_rows_v2 as projection
      on projection.dataset_kind = 'process'
     and projection.id = selected.id
     and projection.version = selected.version
  ), result as (
    select coalesce(
      pg_catalog.jsonb_agg(
        pg_catalog.jsonb_build_object(
          'key', pg_catalog.jsonb_build_object(
            'kind', 'process',
            'id', hydrated.id::text,
            'version', hydrated.version
          ),
          'accessLevel', hydrated.card -> 'accessLevel',
          'capabilities', hydrated.card -> 'capabilities',
          'names', hydrated.card -> 'names',
          'summary', hydrated.card -> 'summary',
          'geography', hydrated.card -> 'geography',
          'referenceYear', hydrated.card -> 'referenceYear',
          'modifiedAt', pg_catalog.to_char(
            hydrated.modified_at at time zone 'UTC',
            'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
          ),
          'match', pg_catalog.jsonb_build_object(
            'kind', case
              when (hydrated.facts ->> 'nameExact')::boolean
                or (hydrated.facts ->> 'nameContains')::boolean
                then 'lexical'
              when (hydrated.facts ->> 'classificationExact')::boolean
                or (hydrated.facts ->> 'classificationContains')::boolean
                then 'identifier'
              else 'lexical'
            end,
            'score', hydrated.score,
            'reasonCodes', case
              when (hydrated.facts ->> 'nameExact')::boolean
                or (hydrated.facts ->> 'nameContains')::boolean
                then pg_catalog.jsonb_build_array('name')
              when (hydrated.facts ->> 'classificationExact')::boolean
                or (hydrated.facts ->> 'classificationContains')::boolean
                then pg_catalog.jsonb_build_array('classification')
              else pg_catalog.jsonb_build_array('full_text')
            end
          )
        ) order by hydrated.page_rank
      ) filter (where hydrated.page_rank <= p_limit),
      '[]'::jsonb
    ) as items,
    case when pg_catalog.max(hydrated.page_rank) > p_limit then
      (
        pg_catalog.jsonb_agg(
          pg_catalog.jsonb_build_object(
            'v', 1,
            'fp', p_query_fingerprint,
            'rankKey', hydrated.score::text,
            'kind', 'process',
            'id', hydrated.id::text,
            'version', hydrated.version
          ) order by hydrated.page_rank
        ) filter (where hydrated.page_rank = p_limit)
      ) -> 0
    else null end as next_cursor_payload
    from hydrated
  )
  select pg_catalog.jsonb_build_object(
    'items', result.items,
    'nextCursorPayload', result.next_cursor_payload
  )
  from result
$function$
;
alter function private.catalog_portal_process_keyword_relevance_cn1_impl(text,text,uuid,text,integer,text) owner to portal_public_executor;
revoke all on function private.catalog_portal_process_keyword_relevance_cn1_impl(text,text,uuid,text,integer,text) from public;

CREATE OR REPLACE FUNCTION private.catalog_portal_projection_payload_cn1(p_kind text, p_state_code integer, p_json jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
AS $function$
declare
  v_card jsonb;
begin
  v_card := private.portal_catalog_card_cn1(
    p_kind,
    p_state_code,
    p_json
  );
  if pg_catalog.jsonb_typeof(v_card) <> 'object' then
    return null;
  end if;
  return pg_catalog.jsonb_build_object(
    'card', v_card,
    'document', coalesce(v_card ->> 'document', '')
  );
end
$function$
;
alter function private.catalog_portal_projection_payload_cn1(text,integer,jsonb) owner to portal_public_executor;
revoke all on function private.catalog_portal_projection_payload_cn1(text,integer,jsonb) from public;
grant execute on function private.catalog_portal_projection_payload_cn1(text,integer,jsonb) to api_internal_executor;

CREATE OR REPLACE FUNCTION private.portal_catalog_card_cn1(p_kind text, p_state_code integer, p_json jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE PARALLEL RESTRICTED
 SET search_path TO ''
AS $function$
declare
  v_capabilities jsonb := private.portal_capabilities_v1(p_kind, p_state_code, p_json);
  v_information jsonb;
  v_modelling jsonb;
  v_location jsonb;
  v_names jsonb := '[]'::jsonb;
  v_synonyms jsonb := '[]'::jsonb;
  v_summary jsonb := '[]'::jsonb;
  v_technology jsonb := '[]'::jsonb;
  v_geography jsonb;
  v_classifications jsonb := '[]'::jsonb;
  v_reference_year integer;
  v_process_subtype text;
  v_cas text;
  v_source_metadata jsonb;
  v_source text;
  v_document text;
begin
  if p_kind = 'process' then
    v_information := p_json #> '{processDataSet,processInformation}';
    v_modelling := p_json #> '{processDataSet,modellingAndValidation}';
    v_location := v_information #> '{geography,locationOfOperationSupplyOrProduction}';
    v_names := private.portal_process_names_v1(p_json);
    v_summary := private.portal_localized_text_v1(
      v_information #> '{dataSetInformation,common:generalComment}'
    );
    v_technology := private.portal_localized_text_v1(
      v_information #> '{technology,technologyDescriptionAndIncludedProcesses}'
    ) || private.portal_localized_text_v1(
      v_information #> '{technology,technologicalApplicability}'
    );
    v_classifications := private.portal_classifications_v1(
      v_information #> '{dataSetInformation,classificationInformation}'
    );
    v_reference_year := private.portal_safe_year_v1(
      v_information #>> '{time,common:referenceYear}'
    );
    v_process_subtype := nullif(private.portal_scalar_text_v1(
      v_modelling #> '{LCIMethodAndAllocation,typeOfDataSet}'
    ), '');
    v_geography := jsonb_build_object(
      'code', nullif(private.portal_scalar_text_v1(v_location -> '@location'), ''),
      'label', private.portal_localized_text_v1(v_location -> 'descriptionOfRestrictions'),
      'precision', 'unknown'
    );
  elsif p_kind = 'flow' then
    v_information := p_json #> '{flowDataSet,flowInformation}';
    v_location := v_information -> 'geography';
    v_names := private.portal_localized_text_v1(
      v_information #> '{dataSetInformation,name,baseName}'
    );
    v_synonyms := private.portal_localized_text_v1(
      v_information #> '{dataSetInformation,common:synonyms}'
    );
    v_summary := private.portal_localized_text_v1(
      v_information #> '{dataSetInformation,common:generalComment}'
    );
    v_classifications := private.portal_classifications_v1(
      v_information #> '{dataSetInformation,classificationInformation}'
    );
    v_cas := nullif(btrim(coalesce(
      v_information #>> '{dataSetInformation,CASNumber}',
      v_information #>> '{dataSetInformation,common:CASNumber}'
    )), '');
    if v_cas !~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$' then
      v_cas := null;
    end if;
    v_geography := jsonb_build_object(
      'code', case jsonb_typeof(v_location -> 'locationOfSupply')
        when 'string' then nullif(
          private.portal_scalar_text_v1(v_location -> 'locationOfSupply'),
          ''
        )
        when 'object' then nullif(
          private.portal_scalar_text_v1(v_location #> '{locationOfSupply,@location}'),
          ''
        )
        else null
      end,
      'label', private.portal_localized_text_v1(
        v_location #> '{locationOfSupply,descriptionOfRestrictions}'
      ),
      'precision', 'unknown'
    );
  else
    return null;
  end if;

  v_source_metadata := private.portal_source_v1(p_kind, p_json);
  select string_agg(item ->> 'value', ' ' order by item ->> 'language')
  into v_source
  from jsonb_array_elements(v_source_metadata -> 'providerName') as localized(item);
  select lower(concat_ws(' ',
    (select string_agg(item ->> 'value', ' ') from jsonb_array_elements(v_names) as localized(item)),
    (select string_agg(item ->> 'value', ' ') from jsonb_array_elements(v_synonyms) as localized(item)),
    (select string_agg(item ->> 'value', ' ') from jsonb_array_elements(v_summary) as localized(item)),
    (select string_agg(item ->> 'code', ' ') from jsonb_array_elements(v_classifications) as classification(item)),
    (select string_agg(item ->> 'value', ' ') from jsonb_array_elements(v_technology) as localized(item)),
    v_geography ->> 'code',
    v_reference_year::text,
    v_process_subtype,
    v_cas,
    v_source
  )) into v_document;
  return jsonb_build_object(
    'accessLevel', case when (v_capabilities ->> 'exchangesVisible')::boolean then 'open' else 'metadata_only' end,
    'capabilities', v_capabilities,
    'names', v_names,
    'summary', v_summary,
    'geography', v_geography,
    'referenceYear', to_jsonb(v_reference_year),
    'processSubtype', to_jsonb(v_process_subtype),
    'source', to_jsonb(v_source),
    'classifications', v_classifications,
    'casNumber', to_jsonb(v_cas),
    'document', to_jsonb(coalesce(v_document, ''))
  );
end
$function$
;
alter function private.portal_catalog_card_cn1(text,integer,jsonb) owner to portal_public_executor;
revoke all on function private.portal_catalog_card_cn1(text,integer,jsonb) from public;

CREATE OR REPLACE FUNCTION private.portal_catalog_projection_manifest_sha256_cn1()
 RETURNS text
 LANGUAGE sql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET row_security TO 'on'
AS $function$
  with expected(identity) as (
    values
      ('private.catalog_portal_projection_payload_cn1(text,integer,jsonb)'::text),
      ('private.portal_catalog_card_cn1(text,integer,jsonb)'),
      ('private.portal_capabilities_v1(text,integer,jsonb)'),
      ('private.portal_publication_root_v1(text,jsonb)'),
      ('private.portal_access_restrictions_open_v1(jsonb)'),
      ('private.portal_scalar_text_v1(jsonb)'),
      ('private.portal_localized_text_v1(jsonb)'),
      ('private.portal_json_items_v1(jsonb)'),
      ('private.portal_classifications_v1(jsonb)'),
      ('private.portal_safe_year_v1(text)'),
      ('private.portal_source_v1(text,jsonb)'),
      ('private.portal_process_names_v1(jsonb)')
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
$function$
;
alter function private.portal_catalog_projection_manifest_sha256_cn1() owner to api_internal_executor;
revoke all on function private.portal_catalog_projection_manifest_sha256_cn1() from public;

CREATE OR REPLACE FUNCTION private.portal_process_keyword_rank_manifest_sha256_cn1()
 RETURNS text
 LANGUAGE sql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET row_security TO 'on'
AS $function$
  with expected(identity) as (
    values
      ('private.portal_process_rank_name_keys_v1(jsonb)'::text),
      ('private.portal_process_rank_classification_keys_v1(jsonb)'),
      ('private.catalog_portal_process_keyword_keys_cn1(text,text,uuid,text,integer)'),
      ('private.catalog_portal_process_keyword_relevance_cn1_impl(text,text,uuid,text,integer,text)')
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
$function$
;
alter function private.portal_process_keyword_rank_manifest_sha256_cn1() owner to portal_public_executor;
revoke all on function private.portal_process_keyword_rank_manifest_sha256_cn1() from public;

CREATE OR REPLACE FUNCTION private.portal_projection_semantic_process_exact_cn1(p_query_embedding vector)
 RETURNS TABLE(id uuid, version text, semantic_distance double precision)
 LANGUAGE sql
 STABLE PARALLEL RESTRICTED SECURITY DEFINER
 SET search_path TO ''
 SET statement_timeout TO '20s'
 SET plan_cache_mode TO 'force_custom_plan'
 SET work_mem TO '32MB'
 SET enable_hashjoin TO 'on'
 SET enable_nestloop TO 'off'
 SET enable_mergejoin TO 'off'
 SET enable_sort TO 'on'
 SET max_parallel_workers_per_gather TO '0'
 SET jit TO 'off'
 SET row_security TO 'on'
AS $function$
  with latest_keys as materialized (
    select distinct on (projection.id)
      projection.id,
      projection.version
    from private.portal_catalog_search_rows_v2 as projection
    where projection.dataset_kind = 'process'
    order by projection.id,
      projection.version desc,
      projection.modified_at desc,
      projection.state_code desc
  ), eligible as materialized (
    select process.id,
      process.version::text as version,
      process.embedding_ft operator(extensions.<=>) p_query_embedding
        as semantic_distance
    from public.processes as process
    join latest_keys as latest
      on latest.id = process.id
     and process.version = latest.version::character(9)
    where process.state_code in (100, 200)
      and process.embedding_ft is not null
  )
  select eligible.id,
    eligible.version,
    eligible.semantic_distance
  from eligible
  where eligible.semantic_distance is not null
    and eligible.semantic_distance >= 0::double precision
    and eligible.semantic_distance <= 0.5::double precision
  order by eligible.semantic_distance,
    eligible.id,
    eligible.version desc
  limit 200
$function$
;
alter function private.portal_projection_semantic_process_exact_cn1(vector) owner to api_internal_executor;
revoke all on function private.portal_projection_semantic_process_exact_cn1(vector) from public;

CREATE OR REPLACE FUNCTION private.sync_portal_catalog_character_row_cn1()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
 SET row_security TO 'on'
AS $function$
begin
  insert into private.portal_catalog_character_rows_v2 (
    dataset_kind,
    id,
    version,
    state_code,
    modified_at,
    document_characters,
    name_characters,
    name_exact_characters,
    classification_characters,
    classification_exact_characters,
    character_contract_version
  ) values (
    new.dataset_kind,
    new.id,
    new.version,
    new.state_code,
    new.modified_at,
    private.portal_catalog_character_set_v1(new.document),
    private.portal_catalog_character_field_set_v1(
      new.card -> 'names', 'value', false
    ),
    private.portal_catalog_character_field_set_v1(
      new.card -> 'names', 'value', true
    ),
    private.portal_catalog_character_field_set_v1(
      new.card -> 'classifications', 'code', false
    ),
    private.portal_catalog_character_field_set_v1(
      new.card -> 'classifications', 'code', true
    ),
    1
  )
  on conflict (dataset_kind, id, version) do update
  set state_code = excluded.state_code,
      modified_at = excluded.modified_at,
      document_characters = excluded.document_characters,
      name_characters = excluded.name_characters,
      name_exact_characters = excluded.name_exact_characters,
      classification_characters = excluded.classification_characters,
      classification_exact_characters =
        excluded.classification_exact_characters,
      character_contract_version = excluded.character_contract_version;
  return new;
end
$function$
;
alter function private.sync_portal_catalog_character_row_cn1() owner to api_internal_executor;
revoke all on function private.sync_portal_catalog_character_row_cn1() from public;

CREATE TABLE IF NOT EXISTS "private"."portal_catalog_projection_contract_v2" (
    "contract_version" smallint NOT NULL,
    "manifest_schema" "text" NOT NULL,
    "function_identities" "text"[] NOT NULL,
    "manifest_sha256" "text" NOT NULL,
    "created_by_migration" "text" NOT NULL,
    CONSTRAINT "portal_catalog_projection_contract_v1_contract_version_check" CHECK (("contract_version" = 2)),
    CONSTRAINT "portal_catalog_projection_contract_v1_function_identities_check" CHECK (("cardinality"("function_identities") = 12)),
    CONSTRAINT "portal_catalog_projection_contract_v1_manifest_schema_check" CHECK (("manifest_schema" = 'portal.catalog-projection-function-manifest.v2'::"text")),
    CONSTRAINT "portal_catalog_projection_contract_v1_manifest_sha256_check" CHECK (("manifest_sha256" = '5260ed0b5662bf6b4bdae5250d0971fca369d8f36766f32207df47acd68e3500'::"text")),
    CONSTRAINT "portal_catalog_projection_contract_v2_created_by_migration_check" CHECK (("created_by_migration" = '20260908090000'::"text"))
);

ALTER TABLE ONLY "private"."portal_catalog_projection_contract_v2" FORCE ROW LEVEL SECURITY;

ALTER TABLE "private"."portal_catalog_projection_contract_v2" OWNER TO "postgres";

ALTER TABLE ONLY "private"."portal_catalog_projection_contract_v2"
    ADD CONSTRAINT "portal_catalog_projection_contract_v2_pkey" PRIMARY KEY ("contract_version");

ALTER TABLE "private"."portal_catalog_projection_contract_v2" ENABLE ROW LEVEL SECURITY;

GRANT SELECT ON TABLE "private"."portal_catalog_projection_contract_v2" TO "api_internal_executor";

CREATE POLICY "portal_catalog_projection_contract_internal_select_v2" ON "private"."portal_catalog_projection_contract_v2" FOR SELECT TO "api_internal_executor" USING (("contract_version" = 2));

CREATE TABLE IF NOT EXISTS "private"."portal_catalog_search_rows_v2" (
    "dataset_kind" "text" NOT NULL,
    "id" "uuid" NOT NULL,
    "version" "text" NOT NULL,
    "state_code" integer NOT NULL,
    "modified_at" timestamp with time zone NOT NULL,
    "card" "jsonb" NOT NULL,
    "document" "text" NOT NULL,
    "projection_contract_version" smallint NOT NULL,
    CONSTRAINT "portal_catalog_search_rows_contract_version_v2_chk" CHECK (("projection_contract_version" = 2)),
    CONSTRAINT "portal_catalog_search_rows_v1_card_check" CHECK (("jsonb_typeof"("card") = 'object'::"text")),
    CONSTRAINT "portal_catalog_search_rows_v2_check" CHECK ((COALESCE(("card" ->> 'document'::"text"), ''::"text") = "document")),
    CONSTRAINT "portal_catalog_search_rows_v1_dataset_kind_check" CHECK (("dataset_kind" = 'process'::"text")),
    CONSTRAINT "portal_catalog_search_rows_v1_state_code_check" CHECK (("state_code" = ANY (ARRAY[100, 200]))),
    CONSTRAINT "portal_catalog_search_rows_v1_version_check" CHECK (("version" ~ '^\d{2}\.\d{2}\.\d{3}$'::"text"))
);

ALTER TABLE ONLY "private"."portal_catalog_search_rows_v2" FORCE ROW LEVEL SECURITY;

ALTER TABLE "private"."portal_catalog_search_rows_v2" OWNER TO "postgres";

COMMENT ON TABLE "private"."portal_catalog_search_rows_v2" IS 'Private synchronized, public-safe Portal card/document projection. Source embeddings and HNSW indexes remain authoritative and are not duplicated.';

ALTER TABLE ONLY "private"."portal_catalog_search_rows_v2"
    ADD CONSTRAINT "portal_catalog_search_rows_v2_pkey" PRIMARY KEY ("dataset_kind", "id", "version");

ALTER TABLE ONLY "private"."portal_catalog_search_rows_v2"
    ADD CONSTRAINT "portal_catalog_search_rows_contract_version_v2_fk" FOREIGN KEY ("projection_contract_version") REFERENCES "private"."portal_catalog_projection_contract_v2"("contract_version") ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE "private"."portal_catalog_search_rows_v2" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "private"."portal_catalog_search_rows_v2" TO "api_internal_executor";

GRANT SELECT("dataset_kind") ON TABLE "private"."portal_catalog_search_rows_v2" TO "portal_public_executor";

GRANT SELECT("id") ON TABLE "private"."portal_catalog_search_rows_v2" TO "portal_public_executor";

GRANT SELECT("version") ON TABLE "private"."portal_catalog_search_rows_v2" TO "portal_public_executor";

GRANT SELECT("state_code") ON TABLE "private"."portal_catalog_search_rows_v2" TO "portal_public_executor";

GRANT SELECT("modified_at") ON TABLE "private"."portal_catalog_search_rows_v2" TO "portal_public_executor";

GRANT SELECT("card") ON TABLE "private"."portal_catalog_search_rows_v2" TO "portal_public_executor";

GRANT SELECT("document") ON TABLE "private"."portal_catalog_search_rows_v2" TO "portal_public_executor";

CREATE POLICY "portal_catalog_search_rows_internal_all_v2" ON "private"."portal_catalog_search_rows_v2" TO "api_internal_executor" USING (("state_code" = ANY (ARRAY[100, 200]))) WITH CHECK (("state_code" = ANY (ARRAY[100, 200])));

CREATE POLICY "portal_catalog_search_rows_portal_select_v2" ON "private"."portal_catalog_search_rows_v2" FOR SELECT TO "portal_public_executor" USING (true);

CREATE TABLE IF NOT EXISTS "private"."portal_catalog_character_rows_v2" (
    "dataset_kind" "text" NOT NULL,
    "id" "uuid" NOT NULL,
    "version" "text" NOT NULL,
    "state_code" integer NOT NULL,
    "modified_at" timestamp with time zone NOT NULL,
    "document_characters" "text" NOT NULL,
    "name_characters" "text" NOT NULL,
    "name_exact_characters" "text" NOT NULL,
    "classification_characters" "text" NOT NULL,
    "classification_exact_characters" "text" NOT NULL,
    "character_contract_version" smallint DEFAULT 1 NOT NULL,
    CONSTRAINT "portal_catalog_character_rows__character_contract_version_check" CHECK (("character_contract_version" = 1)),
    CONSTRAINT "portal_catalog_character_rows_v1_dataset_kind_check" CHECK (("dataset_kind" = 'process'::"text")),
    CONSTRAINT "portal_catalog_character_rows_v1_state_code_check" CHECK (("state_code" = ANY (ARRAY[100, 200]))),
    CONSTRAINT "portal_catalog_character_rows_v1_version_check" CHECK (("version" ~ '^\d{2}\.\d{2}\.\d{3}$'::"text"))
);

ALTER TABLE ONLY "private"."portal_catalog_character_rows_v2" FORCE ROW LEVEL SECURITY;

ALTER TABLE "private"."portal_catalog_character_rows_v2" OWNER TO "postgres";

COMMENT ON TABLE "private"."portal_catalog_character_rows_v2" IS 'Narrow exact-version public character sets for bounded one-code-point Search pre-limit; parent FK and INSERT/UPDATE trigger keep it synchronized.';

ALTER TABLE ONLY "private"."portal_catalog_character_rows_v2"
    ADD CONSTRAINT "portal_catalog_character_rows_v2_pkey" PRIMARY KEY ("dataset_kind", "id", "version");

ALTER TABLE ONLY "private"."portal_catalog_character_rows_v2"
    ADD CONSTRAINT "portal_catalog_character_parent_v2_fk" FOREIGN KEY ("dataset_kind", "id", "version") REFERENCES "private"."portal_catalog_search_rows_v2"("dataset_kind", "id", "version") ON UPDATE RESTRICT ON DELETE CASCADE;

ALTER TABLE "private"."portal_catalog_character_rows_v2" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,INSERT,DELETE,UPDATE ON TABLE "private"."portal_catalog_character_rows_v2" TO "api_internal_executor";

GRANT SELECT("dataset_kind") ON TABLE "private"."portal_catalog_character_rows_v2" TO "portal_public_executor";

GRANT SELECT("id") ON TABLE "private"."portal_catalog_character_rows_v2" TO "portal_public_executor";

GRANT SELECT("version") ON TABLE "private"."portal_catalog_character_rows_v2" TO "portal_public_executor";

GRANT SELECT("state_code") ON TABLE "private"."portal_catalog_character_rows_v2" TO "portal_public_executor";

GRANT SELECT("modified_at") ON TABLE "private"."portal_catalog_character_rows_v2" TO "portal_public_executor";

GRANT SELECT("document_characters") ON TABLE "private"."portal_catalog_character_rows_v2" TO "portal_public_executor";

GRANT SELECT("name_characters") ON TABLE "private"."portal_catalog_character_rows_v2" TO "portal_public_executor";

GRANT SELECT("name_exact_characters") ON TABLE "private"."portal_catalog_character_rows_v2" TO "portal_public_executor";

GRANT SELECT("classification_characters") ON TABLE "private"."portal_catalog_character_rows_v2" TO "portal_public_executor";

GRANT SELECT("classification_exact_characters") ON TABLE "private"."portal_catalog_character_rows_v2" TO "portal_public_executor";

CREATE POLICY "portal_catalog_character_rows_internal_all_v2" ON "private"."portal_catalog_character_rows_v2" TO "api_internal_executor" USING (true) WITH CHECK (true);

CREATE POLICY "portal_catalog_character_rows_portal_select_v2" ON "private"."portal_catalog_character_rows_v2" FOR SELECT TO "portal_public_executor" USING (true);

insert into private.portal_catalog_projection_contract_v2 values (2, 'portal.catalog-projection-function-manifest.v2', array['private.catalog_portal_projection_payload_cn1(text,integer,jsonb)','private.portal_catalog_card_cn1(text,integer,jsonb)','private.portal_capabilities_v1(text,integer,jsonb)','private.portal_publication_root_v1(text,jsonb)','private.portal_access_restrictions_open_v1(jsonb)','private.portal_scalar_text_v1(jsonb)','private.portal_localized_text_v1(jsonb)','private.portal_json_items_v1(jsonb)','private.portal_classifications_v1(jsonb)','private.portal_safe_year_v1(text)','private.portal_source_v1(text,jsonb)','private.portal_process_names_v1(jsonb)'], '5260ed0b5662bf6b4bdae5250d0971fca369d8f36766f32207df47acd68e3500', '20260908090000');
CREATE INDEX "portal_catalog_character_rows_latest_v2_idx" ON "private"."portal_catalog_character_rows_v2" USING "btree" ("dataset_kind", "id", "version" DESC, "modified_at" DESC, "state_code" DESC);
CREATE OR REPLACE TRIGGER "portal_catalog_character_sync_v2" AFTER INSERT OR UPDATE OF "dataset_kind", "id", "version", "state_code", "modified_at", "card", "document" ON "private"."portal_catalog_search_rows_v2" FOR EACH ROW EXECUTE FUNCTION "private"."sync_portal_catalog_character_row_cn1"();
CREATE OR REPLACE FUNCTION private.sync_portal_catalog_search_row_v2()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO ''
 SET row_security TO 'on'
AS $function$
declare
  v_kind text := case tg_table_name
    when 'processes' then 'process'
    when 'flows' then 'flow'
    else null
  end;
  v_root_key text := case v_kind
    when 'process' then 'processDataSet'
    when 'flow' then 'flowDataSet'
    else null
  end;
  v_payload jsonb;
begin
  if v_kind is null then
    raise exception 'unsupported Portal projection trigger source'
      using errcode = '55000';
  end if;

  if tg_op = 'DELETE' then
    delete from private.portal_catalog_search_rows_v2 as projection
    where projection.dataset_kind = v_kind
      and projection.id = old.id
      and projection.version = old.version::text;
    return old;
  end if;

  if tg_op = 'UPDATE'
     and (old.id, old.version::text) is distinct from (new.id, new.version::text) then
    delete from private.portal_catalog_search_rows_v2 as projection
    where projection.dataset_kind = v_kind
      and projection.id = old.id
      and projection.version = old.version::text;
  end if;


  if new.state_code in (100, 200)
     and new.modified_at is not null
     and pg_catalog.jsonb_typeof(new.json) = 'object'
     and pg_catalog.jsonb_typeof(new.json -> v_root_key) = 'object' then
    v_payload := private.catalog_portal_projection_payload_cn1(
      v_kind,
      new.state_code,
      new.json
    );
    if pg_catalog.jsonb_typeof(v_payload) <> 'object' then
      raise exception 'Portal projection payload is invalid'
        using errcode = '55000';
    end if;
    insert into private.portal_catalog_search_rows_v2 (
      dataset_kind,
      id,
      version,
      state_code,
      modified_at,
      card,
      document,
      projection_contract_version
    ) values (
      v_kind,
      new.id,
      new.version::text,
      new.state_code,
      new.modified_at,
      v_payload -> 'card',
      v_payload ->> 'document',
      2
    )
    on conflict (dataset_kind, id, version) do update
    set state_code = excluded.state_code,
        modified_at = excluded.modified_at,
        card = excluded.card,
        document = excluded.document,
        projection_contract_version =
          excluded.projection_contract_version;
  else
    delete from private.portal_catalog_search_rows_v2 as projection
    where projection.dataset_kind = v_kind
      and projection.id = new.id
      and projection.version = new.version::text;
  end if;
  return new;
end
$function$
;
alter function private.sync_portal_catalog_search_row_v2() owner to api_internal_executor;
revoke all on function private.sync_portal_catalog_search_row_v2() from public;
CREATE OR REPLACE TRIGGER "portal_catalog_projection_content_sync_v2" AFTER INSERT OR DELETE OR UPDATE OF "id", "version", "json", "json_ordered", "state_code", "modified_at" ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "private"."sync_portal_catalog_search_row_v2"('content');


create table private.portal_names_backfill_v2 (
  shard smallint primary key check (shard between 0 and 3),
  process_count bigint not null,
  completed_at timestamptz not null default pg_catalog.clock_timestamp()
);
alter table private.portal_names_backfill_v2 enable row level security;
alter table private.portal_names_backfill_v2 force row level security;
revoke all on private.portal_names_backfill_v2 from public, anon, authenticated, service_role;

create view private.portal_catalog_search_current_v2 with (security_invoker=true) as
select dataset_kind,id,version,state_code,modified_at,card,document from private.portal_catalog_search_rows_v2 where dataset_kind='process'
union all
select dataset_kind,id,version,state_code,modified_at,card,document from private.portal_catalog_search_rows_v1 where dataset_kind='flow';
alter view private.portal_catalog_search_current_v2 owner to postgres;
revoke all on private.portal_catalog_search_current_v2 from public,anon,authenticated,service_role;
grant select on private.portal_catalog_search_current_v2 to api_internal_executor;
grant select (dataset_kind,id,version,state_code,modified_at,card,document) on private.portal_catalog_search_current_v2 to portal_public_executor;
create view private.portal_catalog_character_current_v2 with (security_invoker=true) as
select dataset_kind,id,version,state_code,modified_at,document_characters,name_characters,name_exact_characters,classification_characters,classification_exact_characters from private.portal_catalog_character_rows_v2 where dataset_kind='process'
union all
select dataset_kind,id,version,state_code,modified_at,document_characters,name_characters,name_exact_characters,classification_characters,classification_exact_characters from private.portal_catalog_character_rows_v1 where dataset_kind='flow';
alter view private.portal_catalog_character_current_v2 owner to postgres;
revoke all on private.portal_catalog_character_current_v2 from public,anon,authenticated,service_role;
grant select on private.portal_catalog_character_current_v2 to api_internal_executor;
grant select (dataset_kind,id,version,state_code,modified_at,document_characters,name_characters,name_exact_characters,classification_characters,classification_exact_characters) on private.portal_catalog_character_current_v2 to portal_public_executor;

revoke create on schema private from portal_public_executor, api_internal_executor;
revoke portal_public_executor, api_internal_executor from postgres;
commit;
