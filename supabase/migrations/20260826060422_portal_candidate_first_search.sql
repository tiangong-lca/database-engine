-- Issue #531 prerequisite: install the immutable Portal-only lexical
-- allowlist expression before the two concurrent index migrations.

set lock_timeout = '5s';
set statement_timeout = '120s';

do $portal_candidate_document_role_guard$
begin
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
    raise exception 'Portal candidate document executor role is missing or unsafe'
      using errcode = '42501';
  end if;
end
$portal_candidate_document_role_guard$;

grant portal_public_executor to postgres;
grant create on schema private to portal_public_executor;
set role portal_public_executor;

create or replace function private.catalog_portal_document_v1(
  p_kind text,
  p_json jsonb
)
returns text
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
  v_names jsonb := '[]'::jsonb;
  v_synonyms jsonb := '[]'::jsonb;
  v_summary jsonb := '[]'::jsonb;
  v_technology jsonb := '[]'::jsonb;
  v_geography_code text;
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
    v_names := private.portal_localized_text_v1(
      v_information #> '{dataSetInformation,name,baseName}'
    );
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
    v_geography_code := nullif(
      private.portal_scalar_text_v1(v_location -> '@location'),
      ''
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
    v_cas := nullif(pg_catalog.btrim(coalesce(
      v_information #>> '{dataSetInformation,CASNumber}',
      v_information #>> '{dataSetInformation,common:CASNumber}'
    )), '');
    if v_cas !~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$' then
      v_cas := null;
    end if;
    v_geography_code := case pg_catalog.jsonb_typeof(v_location -> 'locationOfSupply')
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
  else
    return '';
  end if;

  v_source_metadata := private.portal_source_v1(p_kind, p_json);
  select pg_catalog.string_agg(item ->> 'value', ' ' order by item ->> 'language')
  into v_source
  from pg_catalog.jsonb_array_elements(
    v_source_metadata -> 'providerName'
  ) as localized(item);

  select pg_catalog.lower(pg_catalog.concat_ws(' ',
    (select pg_catalog.string_agg(item ->> 'value', ' ')
     from pg_catalog.jsonb_array_elements(v_names) as localized(item)),
    (select pg_catalog.string_agg(item ->> 'value', ' ')
     from pg_catalog.jsonb_array_elements(v_synonyms) as localized(item)),
    (select pg_catalog.string_agg(item ->> 'value', ' ')
     from pg_catalog.jsonb_array_elements(v_summary) as localized(item)),
    (select pg_catalog.string_agg(item ->> 'code', ' ')
     from pg_catalog.jsonb_array_elements(v_classifications) as classification(item)),
    (select pg_catalog.string_agg(item ->> 'value', ' ')
     from pg_catalog.jsonb_array_elements(v_technology) as localized(item)),
    v_geography_code,
    v_reference_year::text,
    v_process_subtype,
    v_cas,
    v_source
  )) into v_document;

  return coalesce(v_document, '');
end
$function$;

comment on function private.catalog_portal_document_v1(text, jsonb) is
  'Immutable Portal-only lexical allowlist expression kept byte-equivalent to portal_catalog_card_v1.document; used only for candidate indexes and exact recheck.';

revoke all on function private.catalog_portal_document_v1(text, jsonb)
  from public, anon, authenticated, service_role;
grant execute on function private.catalog_portal_document_v1(text, jsonb)
  to postgres, service_role, api_internal_executor;

reset role;
revoke create on schema private from portal_public_executor;
revoke portal_public_executor from postgres;

reset statement_timeout;
reset lock_timeout;
