begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

create temporary table portal_legacy_api_before (
  routine_identity text primary key,
  definition text not null,
  owner_name text not null,
  security_definer boolean not null,
  proconfig text[] not null,
  acl_text text not null
) on commit drop;

insert into portal_legacy_api_before (
  routine_identity, definition, owner_name, security_definer, proconfig, acl_text
)
with expected(routine_identity) as (
  values
    ('api.search_contacts(text, jsonb, integer, integer, text, text, uuid, integer)'),
    ('api.search_flowproperties(text, jsonb, integer, integer, text, text, uuid, integer)'),
    ('api.search_flows(text, jsonb, integer, integer, text, text, uuid, integer, text[])'),
    ('api.search_lifecyclemodels(text, jsonb, integer, integer, text, text, uuid, integer, text[])'),
    ('api.search_processes(text, jsonb, integer, integer, text, text, uuid, integer, text, text[], boolean)'),
    ('api.search_sources(text, jsonb, integer, integer, text, text, uuid, integer)'),
    ('api.search_unitgroups(text, jsonb, integer, integer, text, text, uuid, integer)'),
    ('api.hybrid_search_contacts(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid)'),
    ('api.hybrid_search_flowproperties(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid)'),
    ('api.hybrid_search_flows(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[])'),
    ('api.hybrid_search_lifecyclemodels(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[])'),
    ('api.hybrid_search_processes(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[])'),
    ('api.hybrid_search_sources(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid)'),
    ('api.hybrid_search_unitgroups(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[], integer, uuid)')
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

do $portal_legacy_api_snapshot_guard$
begin
  if (select count(*) from portal_legacy_api_before) <> 14 then
    raise exception 'Portal legacy API snapshot is incomplete';
  end if;
end
$portal_legacy_api_snapshot_guard$;

-- Portal reads run as a deliberately constrained database principal.  The
-- browser roles receive EXECUTE on the seven api facades only; they never
-- receive table access and no anon policy is added to the core tables.
do $portal_executor_role$
begin
  if not exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'portal_public_executor'
  ) then
    create role portal_public_executor
      nologin
      noinherit
      nobypassrls
      nocreatedb
      nocreaterole;
  end if;
end
$portal_executor_role$;

alter role portal_public_executor
  nologin
  noinherit
  nobypassrls
  nocreatedb
  nocreaterole;

do $portal_executor_attribute_guard$
begin
  if exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'portal_public_executor'
      and (rolsuper or rolreplication)
  ) then
    raise exception 'portal_public_executor has unsafe privileged attributes'
      using errcode = '42501';
  end if;
end
$portal_executor_attribute_guard$;

grant usage on schema public, private, api, extensions to portal_public_executor;

grant select (id, json, state_code, version, modified_at)
  on table public.processes to portal_public_executor;
grant select (id, json, state_code, version, modified_at)
  on table public.flows to portal_public_executor;
grant select (id, json, state_code, version, modified_at)
  on table public.flowproperties to portal_public_executor;
grant select (id, json, state_code, version, modified_at)
  on table public.unitgroups to portal_public_executor;

create policy portal_public_executor_select_processes_v1
on public.processes
for select
to portal_public_executor
using (state_code in (100, 200));

create policy portal_public_executor_select_flows_v1
on public.flows
for select
to portal_public_executor
using (state_code in (100, 200));

create policy portal_public_executor_select_flowproperties_v1
on public.flowproperties
for select
to portal_public_executor
using (state_code in (100, 200));

create policy portal_public_executor_select_unitgroups_v1
on public.unitgroups
for select
to portal_public_executor
using (state_code in (100, 200));

comment on role portal_public_executor is
  'NOLOGIN, NOINHERIT, non-BYPASSRLS owner for the locator-free Portal public catalog facades.';
comment on policy portal_public_executor_select_processes_v1 on public.processes is
  'Portal-only fixed public candidate scope. This is intentionally not an anon/authenticated policy.';
comment on policy portal_public_executor_select_flows_v1 on public.flows is
  'Portal-only fixed public candidate scope. This is intentionally not an anon/authenticated policy.';
comment on policy portal_public_executor_select_flowproperties_v1 on public.flowproperties is
  'Portal-only support-chain scope for public Exchange projection.';
comment on policy portal_public_executor_select_unitgroups_v1 on public.unitgroups is
  'Portal-only support-chain scope for public Exchange projection.';

do $portal_source_table_guard$
begin
  if (
    select count(*)
    from pg_catalog.pg_class as relation
    join pg_catalog.pg_namespace as namespace
      on namespace.oid = relation.relnamespace
    where namespace.nspname = 'public'
      and relation.relname in ('processes', 'flows', 'flowproperties', 'unitgroups')
      and relation.relkind = 'r'
      and relation.relrowsecurity
      and relation.relowner <> 'portal_public_executor'::regrole
  ) <> 4 then
    raise exception 'Portal source-table RLS or ownership guard failed'
      using errcode = '42501';
  end if;
end
$portal_source_table_guard$;

-- Create every helper and facade as the constrained owner.  CREATE is granted
-- only for the duration of this transaction and revoked before commit.
grant portal_public_executor to postgres;
grant create on schema private, api to portal_public_executor;
set local role portal_public_executor;

create function private.portal_scalar_text_v1(p_value jsonb)
returns text
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select case
    when jsonb_typeof(p_value) = 'string' then btrim(p_value #>> '{}')
    else null
  end
$function$;

create function private.portal_localized_text_v1(p_value jsonb)
returns jsonb
language sql
immutable
parallel safe
set search_path = ''
as $function$
  with items as (
    select item.value, item.ordinality
    from jsonb_array_elements(
      case jsonb_typeof(p_value)
        when 'array' then p_value
        when 'null' then '[]'::jsonb
        else jsonb_build_array(p_value)
      end
    ) with ordinality as item(value, ordinality)
  ), normalized as (
    select
      case
        when jsonb_typeof(value) = 'object'
          and btrim(coalesce(value ->> '@xml:lang', '')) ~ '^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$'
          and length(btrim(value ->> '@xml:lang')) <= 35
          then btrim(value ->> '@xml:lang')
        else 'und'
      end as language,
      case
        when jsonb_typeof(value) = 'object'
          then private.portal_scalar_text_v1(value -> '#text')
        when jsonb_typeof(value) = 'string'
          then private.portal_scalar_text_v1(value)
        else null
      end as text_value,
      ordinality
    from items
  )
  select coalesce(
    jsonb_agg(
      jsonb_build_object('language', language, 'value', btrim(text_value))
      order by ordinality
    ) filter (where nullif(btrim(text_value), '') is not null),
    '[]'::jsonb
  )
  from normalized
$function$;

create function private.portal_json_items_v1(p_value jsonb)
returns setof jsonb
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select item.value
  from jsonb_array_elements(
    case jsonb_typeof(p_value)
      when 'array' then p_value
      when 'object' then jsonb_build_array(p_value)
      else '[]'::jsonb
    end
  ) as item(value)
$function$;

create function private.portal_canonical_decimal_v1(p_value text)
returns text
language plpgsql
immutable
parallel safe
set search_path = ''
as $function$
declare
  v_input text := btrim(p_value);
  v_match text[];
  v_exponent integer;
  v_number numeric;
  v_output text;
  v_digits text;
begin
  if p_value is null
     or length(v_input) = 0
     or length(v_input) > 128 then
    return null;
  end if;

  v_match := regexp_match(
    v_input,
    '^([+-]?)([0-9]*)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?$'
  );
  if v_match is null
     or coalesce(length(v_match[2]), 0) + coalesce(length(v_match[3]), 0) = 0 then
    return null;
  end if;

  if v_match[4] is not null then
    if length(ltrim(v_match[4], '+-')) > 4 then
      return null;
    end if;
    v_exponent := v_match[4]::integer;
    if abs(v_exponent) > 1000 then
      return null;
    end if;
  end if;

  begin
    v_number := v_input::numeric;
    v_output := trim_scale(v_number)::text;
  exception
    when others then
      return null;
  end;

  if v_output ~ '[eE+]' or length(v_output) > 2048 then
    return null;
  end if;
  if v_number = 0 then
    return '0';
  end if;

  v_digits := regexp_replace(v_output, '[^0-9]', '', 'g');
  if length(v_digits) not between 1 and 38 then
    return null;
  end if;

  return v_output;
end
$function$;

create function private.portal_dataset_rows_v1(p_kind text, p_id uuid)
returns table(
  id uuid,
  version text,
  json_data jsonb,
  state_code integer,
  modified_at timestamptz,
  lexical_text text
)
language plpgsql
stable
parallel restricted
set search_path = ''
as $function$
begin
  if p_kind = 'process' then
    return query
    select row.id, row.version::text, row.json, row.state_code, row.modified_at,
      ''::text
    from public.processes as row
    where row.id = p_id
      and row.state_code in (100, 200)
      and jsonb_typeof(row.json) = 'object'
      and jsonb_typeof(row.json -> 'processDataSet') = 'object'
      and row.modified_at is not null;
  elsif p_kind = 'flow' then
    return query
    select row.id, row.version::text, row.json, row.state_code, row.modified_at,
      ''::text
    from public.flows as row
    where row.id = p_id
      and row.state_code in (100, 200)
      and jsonb_typeof(row.json) = 'object'
      and jsonb_typeof(row.json -> 'flowDataSet') = 'object'
      and row.modified_at is not null;
  end if;
end
$function$;

create function private.portal_access_restrictions_open_v1(p_value jsonb)
returns boolean
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select case
    when p_value is null or p_value = 'null'::jsonb then true
    when jsonb_typeof(p_value) not in ('array', 'object', 'string') then false
    else not exists (
      select 1
      from jsonb_array_elements(
        case jsonb_typeof(p_value)
          when 'array' then p_value
          else jsonb_build_array(p_value)
        end
      ) as restriction(value)
      where case jsonb_typeof(restriction.value)
        when 'object' then case
          when restriction.value ? '#text'
            and jsonb_typeof(restriction.value -> '#text') = 'string'
            then lower(private.portal_scalar_text_v1(restriction.value -> '#text'))
          else '__invalid__'
        end
        when 'string' then lower(private.portal_scalar_text_v1(restriction.value))
        else '__invalid__'
      end not in ('', 'none')
    )
  end
$function$;

create function private.portal_publication_root_v1(p_kind text, p_json jsonb)
returns jsonb
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select case p_kind
    when 'process' then p_json #> '{processDataSet,administrativeInformation,publicationAndOwnership}'
    when 'flow' then p_json #> '{flowDataSet,administrativeInformation,publicationAndOwnership}'
    when 'flowproperty' then p_json #> '{flowPropertyDataSet,administrativeInformation,publicationAndOwnership}'
    when 'unitgroup' then p_json #> '{unitGroupDataSet,administrativeInformation,publicationAndOwnership}'
    else null
  end
$function$;

create function private.portal_capabilities_v1(
  p_kind text,
  p_state_code integer,
  p_json jsonb
)
returns jsonb
language plpgsql
stable
parallel safe
set search_path = ''
as $function$
declare
  v_publication jsonb := private.portal_publication_root_v1(p_kind, p_json);
  v_license text := private.portal_scalar_text_v1(v_publication -> 'common:licenseType');
  v_exclusive jsonb := v_publication -> 'common:referenceToEntitiesWithExclusiveAccess';
  v_restrictions jsonb := v_publication -> 'common:accessRestrictions';
  v_exclusive_missing boolean;
  v_restrictions_open boolean;
  v_open boolean;
  v_reasons jsonb := '[]'::jsonb;
begin
  v_exclusive_missing := v_exclusive is null
    or v_exclusive = 'null'::jsonb;
  v_restrictions_open := private.portal_access_restrictions_open_v1(v_restrictions);
  v_open := coalesce(p_state_code = 100
    and v_license = 'Free of charge for all users and uses'
    and v_exclusive_missing
    and v_restrictions_open, false);

  if p_state_code = 200 then
    v_reasons := v_reasons || '"state_200_metadata_only"'::jsonb;
  elsif p_state_code <> 100 then
    v_reasons := v_reasons || '"state_not_public"'::jsonb;
  end if;
  if v_license is distinct from 'Free of charge for all users and uses' then
    v_reasons := v_reasons || '"license_not_fully_open"'::jsonb;
  end if;
  if not v_exclusive_missing then
    v_reasons := v_reasons || '"exclusive_access_declared"'::jsonb;
  end if;
  if not v_restrictions_open then
    v_reasons := v_reasons || '"access_restrictions_present"'::jsonb;
  end if;
  if v_open then
    v_reasons := '[]'::jsonb || '"public_license_confirmed"'::jsonb;
  end if;

  return jsonb_build_object(
    'metadataVisible', p_state_code in (100, 200),
    'exchangesVisible', v_open,
    'lciaVisible', false,
    'publicArtifactVisible', false,
    'citationVisible', p_state_code in (100, 200),
    'policyVersion', 'portal-capability-policy.v1',
    'reasonCodes', v_reasons
  );
end
$function$;

create function private.portal_cursor_encode_v1(p_payload jsonb)
returns text
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select rtrim(
    translate(
      replace(
        replace(encode(convert_to(p_payload::text, 'UTF8'), 'base64'), E'\n', ''),
        E'\r',
        ''
      ),
      '+/',
      '-_'
    ),
    '='
  )
$function$;

create function private.portal_support_capabilities_v1(
  p_kind text,
  p_state_code integer
)
returns jsonb
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select jsonb_build_object(
    'exchangesVisible', p_kind in ('flow', 'flowproperty', 'unitgroup') and p_state_code = 100,
    'policyVersion', 'portal-capability-policy.v1',
    'reasonCodes', case
      when p_kind not in ('flow', 'flowproperty', 'unitgroup')
        then jsonb_build_array('unsupported_support_kind')
      when p_state_code = 100
        then jsonb_build_array('support_state_100_public')
      when p_state_code = 200
        then jsonb_build_array('support_state_200_metadata_only')
      else jsonb_build_array('support_state_not_public')
    end
  )
$function$;

create function private.portal_cursor_decode_v1(p_cursor text)
returns jsonb
language plpgsql
stable
parallel safe
set search_path = ''
as $function$
declare
  v_base64 text;
  v_result jsonb;
begin
  if p_cursor is null
     or length(p_cursor) = 0
     or length(p_cursor) > 4096
     or p_cursor !~ '^[A-Za-z0-9_-]+$' then
    return null;
  end if;
  v_base64 := translate(p_cursor, '-_', '+/');
  v_base64 := v_base64 || repeat('=', (4 - length(v_base64) % 4) % 4);
  begin
    v_result := convert_from(decode(v_base64, 'base64'), 'UTF8')::jsonb;
  exception
    when others then
      return null;
  end;
  if jsonb_typeof(v_result) <> 'object' then
    return null;
  end if;
  return v_result;
end
$function$;

create function private.portal_query_fingerprint_v1(
  p_kind text,
  p_query text,
  p_filters jsonb,
  p_sort text
)
returns text
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select encode(
    extensions.digest(
      convert_to(
        jsonb_build_object(
          'kind', p_kind,
          'query', p_query,
          'filters', p_filters,
          'sort', p_sort
        )::text,
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
$function$;

create function private.portal_normalize_filters_v1(p_filters jsonb)
returns jsonb
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select coalesce(
    jsonb_object_agg(
      filter.key,
      case
        when filter.key in (
          'accessLevel', 'geography', 'classification', 'processSubtype', 'source'
        ) and jsonb_typeof(filter.value) = 'string'
          then to_jsonb(lower(btrim(filter.value #>> '{}')))
        else filter.value
      end
      order by filter.key
    ),
    '{}'::jsonb
  )
  from jsonb_each(coalesce(p_filters, '{}'::jsonb)) as filter(key, value)
$function$;

create function private.portal_timestamp_v1(p_value timestamptz)
returns text
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select to_char(p_value at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
$function$;

create function private.portal_safe_year_v1(p_value text)
returns integer
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select case
    when btrim(coalesce(p_value, '')) ~ '^[0-9]{4}$'
      then btrim(p_value)::integer
    else null
  end
$function$;

create function private.portal_geography_precision_v1(p_code text)
returns text
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select 'unknown'::text
$function$;

revoke all on function private.portal_scalar_text_v1(jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_localized_text_v1(jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_json_items_v1(jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_canonical_decimal_v1(text) from public, anon, authenticated, service_role;
revoke all on function private.portal_access_restrictions_open_v1(jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_publication_root_v1(text, jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_capabilities_v1(text, integer, jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_support_capabilities_v1(text, integer) from public, anon, authenticated, service_role;
revoke all on function private.portal_cursor_encode_v1(jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_cursor_decode_v1(text) from public, anon, authenticated, service_role;
revoke all on function private.portal_query_fingerprint_v1(text, text, jsonb, text) from public, anon, authenticated, service_role;
revoke all on function private.portal_normalize_filters_v1(jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_timestamp_v1(timestamptz) from public, anon, authenticated, service_role;
revoke all on function private.portal_safe_year_v1(text) from public, anon, authenticated, service_role;
revoke all on function private.portal_geography_precision_v1(text) from public, anon, authenticated, service_role;

create function private.portal_datetime_v1(p_value text)
returns text
language plpgsql
stable
parallel safe
set search_path = ''
as $function$
declare
  v_timestamp timestamptz;
begin
  if nullif(btrim(coalesce(p_value, '')), '') is null
     or length(p_value) > 64
     or p_value !~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$' then
    return null;
  end if;
  begin
    v_timestamp := p_value::timestamptz;
  exception
    when others then
      return null;
  end;
  return private.portal_timestamp_v1(v_timestamp);
end
$function$;

create function private.portal_named_reference_v1(p_reference jsonb)
returns jsonb
language plpgsql
stable
parallel safe
set search_path = ''
as $function$
declare
  v_reference jsonb;
  v_id text;
  v_version text;
  v_name jsonb;
begin
  v_reference := case
    when jsonb_typeof(p_reference) = 'object' then p_reference
    when jsonb_typeof(p_reference) = 'array'
      and jsonb_array_length(p_reference) = 1 then p_reference -> 0
    else null
  end;
  v_id := nullif(lower(private.portal_scalar_text_v1(v_reference -> '@refObjectId')), '');
  v_version := nullif(private.portal_scalar_text_v1(v_reference -> '@version'), '');
  v_name := private.portal_localized_text_v1(v_reference -> 'common:shortDescription');
  if coalesce(
       v_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
       false
     ) is not true
     or coalesce(v_version ~ '^\d{2}\.\d{2}\.\d{3}$', false) is not true then
    v_id := null;
    v_version := null;
  end if;
  return jsonb_build_object('id', v_id, 'version', v_version, 'name', v_name);
end
$function$;

create function private.portal_classifications_v1(p_information jsonb)
returns jsonb
language plpgsql
stable
parallel safe
set search_path = ''
as $function$
declare
  v_result jsonb := '[]'::jsonb;
  v_classification jsonb;
  v_class jsonb;
  v_category jsonb;
  v_system text;
  v_code text;
begin
  for v_classification in
    select private.portal_json_items_v1(p_information -> 'common:classification')
  loop
    v_system := coalesce(
      nullif(private.portal_scalar_text_v1(v_classification -> '@name'), ''),
      'ILCD'
    );
    for v_class in
      select private.portal_json_items_v1(v_classification -> 'common:class')
    loop
      v_code := coalesce(
        nullif(private.portal_scalar_text_v1(v_class -> '@classId'), ''),
        nullif(private.portal_scalar_text_v1(v_class -> '#text'), '')
      );
      if v_code is not null then
        v_result := v_result || jsonb_build_array(jsonb_build_object(
          'system', v_system,
          'code', v_code,
          'label', private.portal_localized_text_v1(v_class)
        ));
      end if;
    end loop;
  end loop;

  for v_category in
    select private.portal_json_items_v1(
      p_information #> '{common:elementaryFlowCategorization,common:category}'
    )
  loop
    v_code := coalesce(
      nullif(private.portal_scalar_text_v1(v_category -> '@catId'), ''),
      nullif(private.portal_scalar_text_v1(v_category -> '@classId'), ''),
      nullif(private.portal_scalar_text_v1(v_category -> '#text'), '')
    );
    if v_code is not null then
      v_result := v_result || jsonb_build_array(jsonb_build_object(
        'system', 'elementary-flow',
        'code', v_code,
        'label', private.portal_localized_text_v1(v_category)
      ));
    end if;
  end loop;
  return v_result;
end
$function$;

create function private.portal_compliance_v1(p_kind text, p_json jsonb)
returns jsonb
language plpgsql
stable
parallel safe
set search_path = ''
as $function$
declare
  v_value jsonb;
  v_item jsonb;
  v_result jsonb := '[]'::jsonb;
begin
  v_value := case p_kind
    when 'process' then p_json #> '{processDataSet,modellingAndValidation,complianceDeclarations,compliance}'
    when 'flow' then p_json #> '{flowDataSet,modellingAndValidation,complianceDeclarations,compliance}'
    else null
  end;
  for v_item in select private.portal_json_items_v1(v_value)
  loop
    v_result := v_result || jsonb_build_array(jsonb_build_object(
      'system', private.portal_named_reference_v1(v_item -> 'common:referenceToComplianceSystem'),
      'overall', nullif(private.portal_scalar_text_v1(v_item -> 'common:approvalOfOverallCompliance'), ''),
      'nomenclature', nullif(private.portal_scalar_text_v1(v_item -> 'common:nomenclatureCompliance'), ''),
      'methodological', nullif(private.portal_scalar_text_v1(v_item -> 'common:methodologicalCompliance'), ''),
      'review', nullif(private.portal_scalar_text_v1(v_item -> 'common:reviewCompliance'), ''),
      'documentation', nullif(private.portal_scalar_text_v1(v_item -> 'common:documentationCompliance'), ''),
      'quality', nullif(private.portal_scalar_text_v1(v_item -> 'common:qualityCompliance'), '')
    ));
  end loop;
  return v_result;
end
$function$;

create function private.portal_administration_v1(p_kind text, p_json jsonb)
returns jsonb
language plpgsql
stable
parallel safe
set search_path = ''
as $function$
declare
  v_admin jsonb;
  v_publication jsonb;
  v_commissioner jsonb;
  v_data_generator jsonb;
  v_data_entry jsonb;
  v_copyright_text text;
  v_copyright boolean;
  v_permalink text;
begin
  v_admin := case p_kind
    when 'process' then p_json #> '{processDataSet,administrativeInformation}'
    when 'flow' then p_json #> '{flowDataSet,administrativeInformation}'
    else null
  end;
  v_publication := v_admin -> 'publicationAndOwnership';
  v_commissioner := v_admin #> '{common:commissionerAndGoal,common:referenceToCommissioner}';
  v_data_generator := v_admin #> '{dataGenerator,common:referenceToPersonOrEntityGeneratingTheDataSet}';
  v_data_entry := v_admin #> '{dataEntryBy,common:referenceToPersonOrEntityEnteringTheData}';
  v_copyright_text := lower(coalesce(
    private.portal_scalar_text_v1(v_publication -> 'common:copyright'),
    ''
  ));
  v_copyright := case
    when v_copyright_text in ('true', 'yes', '1') then true
    when v_copyright_text in ('false', 'no', '0') then false
    else null
  end;
  -- No public-origin allowlist exists in v1. A syntactically valid HTTPS URL
  -- is not proof that an authored URI is public rather than a private object
  -- or service locator, so this field stays closed until such a contract lands.
  v_permalink := null;

  return jsonb_build_object(
    'workflowStatus', nullif(private.portal_scalar_text_v1(v_publication -> 'common:workflowAndPublicationStatus'), ''),
    'copyright', v_copyright,
    'owner', private.portal_named_reference_v1(v_publication -> 'common:referenceToOwnershipOfDataSet'),
    'commissioner', private.portal_named_reference_v1(v_commissioner),
    'dataGenerator', private.portal_named_reference_v1(v_data_generator),
    'dataEntryBy', private.portal_named_reference_v1(v_data_entry),
    'project', private.portal_localized_text_v1(v_admin #> '{common:commissionerAndGoal,common:project}'),
    'intendedApplications', private.portal_localized_text_v1(v_admin #> '{common:commissionerAndGoal,common:intendedApplications}'),
    'accessRestrictions', private.portal_localized_text_v1(v_publication -> 'common:accessRestrictions'),
    'licenseType', nullif(private.portal_scalar_text_v1(v_publication -> 'common:licenseType'), ''),
    'registrationNumber', nullif(private.portal_scalar_text_v1(v_publication -> 'common:registrationNumber'), ''),
    'lastRevisionAt', private.portal_datetime_v1(v_publication ->> 'common:dateOfLastRevision'),
    'permanentDataSetUri', v_permalink,
    'precedingVersion', private.portal_named_reference_v1(v_publication -> 'common:referenceToPrecedingDataSetVersion')
  );
end
$function$;

create function private.portal_source_v1(p_kind text, p_json jsonb)
returns jsonb
language plpgsql
stable
parallel safe
set search_path = ''
as $function$
declare
  v_publication jsonb := private.portal_publication_root_v1(p_kind, p_json);
  v_database jsonb := v_publication -> 'common:referenceToUnchangedRepublication';
  v_source jsonb;
begin
  v_source := case p_kind
    when 'process' then p_json #> '{processDataSet,modellingAndValidation,dataSourcesTreatmentAndRepresentativeness,referenceToDataSource}'
    when 'flow' then p_json #> '{flowDataSet,modellingAndValidation,dataSourcesTreatmentAndRepresentativeness,referenceToDataSource}'
    else null
  end;
  if jsonb_typeof(v_source) = 'array' and jsonb_array_length(v_source) = 1 then
    v_source := v_source -> 0;
  elsif jsonb_typeof(v_source) <> 'object' then
    v_source := null;
  end if;
  return jsonb_build_object(
    'databaseId', case
      when lower(coalesce(v_database ->> '@refObjectId', '')) ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then lower(v_database ->> '@refObjectId')
      else null
    end,
    'databaseVersion', case
      when coalesce(v_database ->> '@version', '') ~ '^\d{2}\.\d{2}\.\d{3}$'
        then v_database ->> '@version'
      else null
    end,
    'sourceRecordId', case
      when lower(coalesce(v_source ->> '@refObjectId', '')) ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then lower(v_source ->> '@refObjectId')
      else null
    end,
    'providerName', private.portal_localized_text_v1(
      v_publication #> '{common:referenceToOwnershipOfDataSet,common:shortDescription}'
    ),
    'licenseId', nullif(private.portal_scalar_text_v1(v_publication -> 'common:licenseType'), ''),
    'licenseUrl', null
  );
end
$function$;

revoke all on function private.portal_datetime_v1(text) from public, anon, authenticated, service_role;
revoke all on function private.portal_named_reference_v1(jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_classifications_v1(jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_compliance_v1(text, jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_administration_v1(text, jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_source_v1(text, jsonb) from public, anon, authenticated, service_role;

create function private.portal_first_text_v1(p_value jsonb)
returns text
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select item ->> 'value'
  from jsonb_array_elements(private.portal_localized_text_v1(p_value)) as localized(item)
  order by case item ->> 'language' when 'en' then 0 when 'zh' then 1 else 2 end
  limit 1
$function$;

create function private.portal_flow_kind_v1(p_type text)
returns text
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select case lower(btrim(coalesce(p_type, '')))
    when 'elementary flow' then 'elementary'
    when 'waste flow' then 'waste'
    when 'product flow' then 'product'
    when 'other flow' then 'other'
    else 'unknown'
  end
$function$;

create function private.portal_reference_flowproperty_v1(p_flow_json jsonb)
returns jsonb
language plpgsql
stable
parallel safe
set search_path = ''
as $function$
declare
  v_internal_id text := p_flow_json #>> '{flowDataSet,flowInformation,quantitativeReference,referenceToReferenceFlowProperty}';
  v_flow_property jsonb;
  v_reference jsonb;
  v_id_text text;
  v_version text;
  v_id uuid;
  v_row_json jsonb;
  v_match_count bigint;
begin
  if v_internal_id !~ '^(0|[1-9][0-9]{0,4})$' then
    return null;
  end if;
  select count(*), (jsonb_agg(item) -> 0)
  into v_match_count, v_flow_property
  from private.portal_json_items_v1(p_flow_json #> '{flowDataSet,flowProperties,flowProperty}') as item
  where item ->> '@dataSetInternalID' = v_internal_id
    and item ->> '@dataSetInternalID' ~ '^(0|[1-9][0-9]{0,4})$';
  if v_match_count <> 1 then
    return null;
  end if;
  v_reference := v_flow_property -> 'referenceToFlowPropertyDataSet';
  v_id_text := lower(btrim(coalesce(v_reference ->> '@refObjectId', '')));
  v_version := btrim(coalesce(v_reference ->> '@version', ''));
  if v_id_text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or v_version !~ '^\d{2}\.\d{2}\.\d{3}$' then
    return null;
  end if;
  v_id := v_id_text::uuid;
  select row.json
  into v_row_json
  from public.flowproperties as row
  where row.id = v_id
    and row.version::text = v_version
    and row.state_code in (100, 200)
    and jsonb_typeof(row.json) = 'object'
    and jsonb_typeof(row.json -> 'flowPropertyDataSet') = 'object'
  limit 1;
  if v_row_json is null then
    return null;
  end if;
  return jsonb_build_object(
    'id', v_id_text,
    'version', v_version,
    'name', private.portal_localized_text_v1(
      v_row_json #> '{flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:name}'
    )
  );
end
$function$;

create function private.portal_exchange_support_v1(
  p_process_state integer,
  p_process_json jsonb,
  p_exchange jsonb
)
returns jsonb
language plpgsql
stable
parallel restricted
set search_path = ''
as $function$
declare
  v_process_capabilities jsonb;
  v_internal_id text := btrim(coalesce(p_exchange ->> '@dataSetInternalID', ''));
  v_amount text;
  v_direction text;
  v_flow_reference jsonb := p_exchange -> 'referenceToFlowDataSet';
  v_flow_id_text text;
  v_flow_version text;
  v_flow_id uuid;
  v_flow_json jsonb;
  v_flow_state integer;
  v_flow_type text;
  v_exchange_kind text;
  v_flow_property_internal text;
  v_flow_property_item jsonb;
  v_flow_property_reference jsonb;
  v_flow_property_id_text text;
  v_flow_property_version text;
  v_flow_property_id uuid;
  v_flow_property_json jsonb;
  v_flow_property_state integer;
  v_unit_group_reference jsonb;
  v_unit_group_id_text text;
  v_unit_group_version text;
  v_unit_group_id uuid;
  v_unit_group_json jsonb;
  v_unit_group_state integer;
  v_unit_internal text;
  v_unit_item jsonb;
  v_unit text;
  v_unit_factor text;
  v_flow_factor text;
  v_classifications jsonb;
  v_uncertainty_type text;
  v_minimum text;
  v_maximum text;
  v_row jsonb;
  v_match_count integer;
begin
  v_process_capabilities := private.portal_capabilities_v1('process', p_process_state, p_process_json);
  if coalesce((v_process_capabilities ->> 'exchangesVisible')::boolean, false) is not true
     or v_internal_id !~ '^(0|[1-9][0-9]{0,5})$' then
    return null;
  end if;

  v_amount := private.portal_canonical_decimal_v1(
    coalesce(p_exchange ->> 'resultingAmount', p_exchange ->> 'meanAmount')
  );
  v_direction := case lower(btrim(coalesce(p_exchange ->> 'exchangeDirection', '')))
    when 'input' then 'input'
    when 'output' then 'output'
    else null
  end;
  v_flow_id_text := lower(btrim(coalesce(v_flow_reference ->> '@refObjectId', '')));
  v_flow_version := btrim(coalesce(v_flow_reference ->> '@version', ''));
  if v_amount is null
     or v_direction is null
     or v_flow_id_text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or v_flow_version !~ '^\d{2}\.\d{2}\.\d{3}$' then
    return null;
  end if;
  v_flow_id := v_flow_id_text::uuid;

  select row.json, row.state_code
  into v_flow_json, v_flow_state
  from public.flows as row
  where row.id = v_flow_id
    and row.version::text = v_flow_version
    and row.state_code in (100, 200)
    and jsonb_typeof(row.json) = 'object'
    and jsonb_typeof(row.json -> 'flowDataSet') = 'object'
  limit 1;
  if v_flow_json is null
     or coalesce((private.portal_support_capabilities_v1('flow', v_flow_state) ->> 'exchangesVisible')::boolean, false) is not true then
    return null;
  end if;

  v_flow_type := private.portal_flow_kind_v1(
    private.portal_scalar_text_v1(
      v_flow_json #> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}'
    )
  );
  v_exchange_kind := case v_flow_type
    when 'product' then 'technosphere'
    when 'elementary' then 'elementary'
    when 'waste' then 'waste'
    else null
  end;
  if v_exchange_kind is null then
    return null;
  end if;

  v_flow_property_internal := v_flow_json #>> '{flowDataSet,flowInformation,quantitativeReference,referenceToReferenceFlowProperty}';
  if v_flow_property_internal !~ '^(0|[1-9][0-9]{0,4})$' then
    return null;
  end if;
  select count(*), (jsonb_agg(item) -> 0)
  into v_match_count, v_flow_property_item
  from private.portal_json_items_v1(v_flow_json #> '{flowDataSet,flowProperties,flowProperty}') as item
  where item ->> '@dataSetInternalID' = v_flow_property_internal
    and item ->> '@dataSetInternalID' ~ '^(0|[1-9][0-9]{0,4})$';
  if v_match_count <> 1 then
    return null;
  end if;
  v_flow_factor := private.portal_canonical_decimal_v1(v_flow_property_item ->> 'meanValue');
  v_flow_property_reference := v_flow_property_item -> 'referenceToFlowPropertyDataSet';
  v_flow_property_id_text := lower(btrim(coalesce(v_flow_property_reference ->> '@refObjectId', '')));
  v_flow_property_version := btrim(coalesce(v_flow_property_reference ->> '@version', ''));
  if v_flow_factor is distinct from '1'
     or v_flow_property_id_text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or v_flow_property_version !~ '^\d{2}\.\d{2}\.\d{3}$' then
    return null;
  end if;
  v_flow_property_id := v_flow_property_id_text::uuid;

  select row.json, row.state_code
  into v_flow_property_json, v_flow_property_state
  from public.flowproperties as row
  where row.id = v_flow_property_id
    and row.version::text = v_flow_property_version
    and row.state_code in (100, 200)
    and jsonb_typeof(row.json) = 'object'
    and jsonb_typeof(row.json -> 'flowPropertyDataSet') = 'object'
  limit 1;
  if v_flow_property_json is null
     or coalesce((private.portal_support_capabilities_v1('flowproperty', v_flow_property_state) ->> 'exchangesVisible')::boolean, false) is not true then
    return null;
  end if;

  v_unit_group_reference := v_flow_property_json #> '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup}';
  v_unit_group_id_text := lower(btrim(coalesce(v_unit_group_reference ->> '@refObjectId', '')));
  v_unit_group_version := btrim(coalesce(v_unit_group_reference ->> '@version', ''));
  if v_unit_group_id_text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or v_unit_group_version !~ '^\d{2}\.\d{2}\.\d{3}$' then
    return null;
  end if;
  v_unit_group_id := v_unit_group_id_text::uuid;

  select row.json, row.state_code
  into v_unit_group_json, v_unit_group_state
  from public.unitgroups as row
  where row.id = v_unit_group_id
    and row.version::text = v_unit_group_version
    and row.state_code in (100, 200)
    and jsonb_typeof(row.json) = 'object'
    and jsonb_typeof(row.json -> 'unitGroupDataSet') = 'object'
  limit 1;
  if v_unit_group_json is null
     or coalesce((private.portal_support_capabilities_v1('unitgroup', v_unit_group_state) ->> 'exchangesVisible')::boolean, false) is not true then
    return null;
  end if;

  v_unit_internal := v_unit_group_json #>> '{unitGroupDataSet,unitGroupInformation,quantitativeReference,referenceToReferenceUnit}';
  if v_unit_internal !~ '^(0|[1-9][0-9]{0,4})$' then
    return null;
  end if;
  select count(*), (jsonb_agg(item) -> 0)
  into v_match_count, v_unit_item
  from private.portal_json_items_v1(v_unit_group_json #> '{unitGroupDataSet,units,unit}') as item
  where item ->> '@dataSetInternalID' = v_unit_internal
    and item ->> '@dataSetInternalID' ~ '^(0|[1-9][0-9]{0,4})$';
  if v_match_count <> 1 then
    return null;
  end if;
  v_unit := nullif(private.portal_scalar_text_v1(v_unit_item -> 'name'), '');
  v_unit_factor := private.portal_canonical_decimal_v1(v_unit_item ->> 'meanValue');
  if v_unit is null or v_unit_factor is distinct from '1' then
    return null;
  end if;

  v_classifications := private.portal_classifications_v1(
    v_flow_json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation}'
  );
  v_uncertainty_type := nullif(
    private.portal_scalar_text_v1(p_exchange -> 'uncertaintyDistributionType'),
    ''
  );
  v_minimum := private.portal_canonical_decimal_v1(p_exchange ->> 'minimumAmount');
  if v_minimum is null then
    v_minimum := private.portal_canonical_decimal_v1(p_exchange ->> 'minimumValue');
  end if;
  v_maximum := private.portal_canonical_decimal_v1(p_exchange ->> 'maximumAmount');
  if v_maximum is null then
    v_maximum := private.portal_canonical_decimal_v1(p_exchange ->> 'maximumValue');
  end if;

  v_row := jsonb_build_object(
    'internalId', v_internal_id,
    'kind', v_exchange_kind,
    'direction', v_direction,
    'flow', jsonb_build_object(
      'id', v_flow_id_text,
      'version', v_flow_version,
      'name', private.portal_localized_text_v1(
        v_flow_json #> '{flowDataSet,flowInformation,dataSetInformation,name,baseName}'
      )
    ),
    'classification', case when jsonb_array_length(v_classifications) > 0 then v_classifications -> 0 else null end,
    'amount', v_amount,
    'unit', v_unit,
    'isQuantitativeReference', v_internal_id = (
      p_process_json #>> '{processDataSet,processInformation,quantitativeReference,referenceToReferenceFlow}'
    ),
    'uncertainty', case when v_uncertainty_type is null then null else jsonb_build_object(
      'type', v_uncertainty_type,
      'minimum', v_minimum,
      'maximum', v_maximum
    ) end,
    'origin', '[]'::jsonb
  );
  return jsonb_build_object(
    'row', v_row,
    'functionalUnit', jsonb_build_object(
      'amount', v_amount,
      'unit', v_unit,
      'description', private.portal_localized_text_v1(
        p_process_json #> '{processDataSet,processInformation,quantitativeReference,functionalUnitOrOther}'
      )
    )
  );
end
$function$;

create function private.portal_process_functional_unit_v1(
  p_state_code integer,
  p_json jsonb
)
returns jsonb
language plpgsql
stable
parallel restricted
set search_path = ''
as $function$
declare
  v_reference_internal text := p_json #>> '{processDataSet,processInformation,quantitativeReference,referenceToReferenceFlow}';
  v_exchange jsonb;
  v_support jsonb;
  v_match_count integer;
begin
  select count(*), jsonb_agg(item) -> 0
  into v_match_count, v_exchange
  from private.portal_json_items_v1(p_json #> '{processDataSet,exchanges,exchange}') as item
  where item ->> '@dataSetInternalID' = v_reference_internal;
  if v_match_count <> 1 then
    return jsonb_build_object(
      'amount', null,
      'unit', null,
      'description', private.portal_localized_text_v1(
        p_json #> '{processDataSet,processInformation,quantitativeReference,functionalUnitOrOther}'
      )
    );
  end if;
  v_support := private.portal_exchange_support_v1(p_state_code, p_json, v_exchange);
  return coalesce(
    v_support -> 'functionalUnit',
    jsonb_build_object(
      'amount', null,
      'unit', null,
      'description', private.portal_localized_text_v1(
        p_json #> '{processDataSet,processInformation,quantitativeReference,functionalUnitOrOther}'
      )
    )
  );
end
$function$;

create function private.portal_process_reference_product_v1(p_json jsonb)
returns jsonb
language plpgsql
stable
parallel restricted
set search_path = ''
as $function$
declare
  v_reference_internal text := p_json #>> '{processDataSet,processInformation,quantitativeReference,referenceToReferenceFlow}';
  v_exchange jsonb;
  v_reference jsonb;
  v_id_text text;
  v_version text;
  v_id uuid;
  v_flow_json jsonb;
  v_match_count integer;
begin
  select count(*), jsonb_agg(item) -> 0
  into v_match_count, v_exchange
  from private.portal_json_items_v1(p_json #> '{processDataSet,exchanges,exchange}') as item
  where item ->> '@dataSetInternalID' = v_reference_internal;
  if v_match_count <> 1 then
    return '[]'::jsonb;
  end if;
  v_reference := v_exchange -> 'referenceToFlowDataSet';
  v_id_text := lower(btrim(coalesce(v_reference ->> '@refObjectId', '')));
  v_version := btrim(coalesce(v_reference ->> '@version', ''));
  if v_id_text ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     and v_version ~ '^\d{2}\.\d{2}\.\d{3}$' then
    v_id := v_id_text::uuid;
    select row.json
    into v_flow_json
    from public.flows as row
    where row.id = v_id
      and row.version::text = v_version
      and row.state_code in (100, 200)
      and jsonb_typeof(row.json) = 'object'
      and jsonb_typeof(row.json -> 'flowDataSet') = 'object'
    limit 1;
  end if;
  return coalesce(
    private.portal_localized_text_v1(
      v_flow_json #> '{flowDataSet,flowInformation,dataSetInformation,name,baseName}'
    ),
    '[]'::jsonb
  );
end
$function$;

revoke all on function private.portal_first_text_v1(jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_flow_kind_v1(text) from public, anon, authenticated, service_role;
revoke all on function private.portal_reference_flowproperty_v1(jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_exchange_support_v1(integer, jsonb, jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_process_functional_unit_v1(integer, jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_process_reference_product_v1(jsonb) from public, anon, authenticated, service_role;

create function private.portal_dataset_metadata_v1(
  p_kind text,
  p_state_code integer,
  p_json jsonb
)
returns jsonb
language plpgsql
stable
parallel restricted
set search_path = ''
as $function$
declare
  v_information jsonb;
  v_modelling jsonb;
  v_location jsonb;
  v_location_code text;
  v_cas text;
begin
  if p_kind = 'process' then
    v_information := p_json #> '{processDataSet,processInformation}';
    v_modelling := p_json #> '{processDataSet,modellingAndValidation}';
    v_location := v_information #> '{geography,locationOfOperationSupplyOrProduction}';
    v_location_code := nullif(
      private.portal_scalar_text_v1(v_location -> '@location'),
      ''
    );
    return jsonb_build_object(
      'kind', 'process',
      'names', private.portal_localized_text_v1(v_information #> '{dataSetInformation,name,baseName}'),
      'generalComment', private.portal_localized_text_v1(v_information #> '{dataSetInformation,common:generalComment}'),
      'referenceProduct', private.portal_process_reference_product_v1(p_json),
      'functionalUnit', private.portal_process_functional_unit_v1(p_state_code, p_json),
      'classifications', private.portal_classifications_v1(v_information #> '{dataSetInformation,classificationInformation}'),
      'geography', jsonb_build_object(
        'code', v_location_code,
        'label', private.portal_localized_text_v1(v_location -> 'descriptionOfRestrictions'),
        'precision', private.portal_geography_precision_v1(v_location_code)
      ),
      'referenceYear', private.portal_safe_year_v1(v_information #>> '{time,common:referenceYear}'),
      'validUntilYear', private.portal_safe_year_v1(v_information #>> '{time,common:dataSetValidUntil}'),
      'technology',
        private.portal_localized_text_v1(
          v_information #> '{technology,technologyDescriptionAndIncludedProcesses}'
        ) || private.portal_localized_text_v1(
          v_information #> '{technology,technologicalApplicability}'
        ),
      'dataSetType', nullif(private.portal_scalar_text_v1(
        v_modelling #> '{LCIMethodAndAllocation,typeOfDataSet}'
      ), ''),
      'allocationAndModeling',
        private.portal_localized_text_v1(
          v_modelling #> '{LCIMethodAndAllocation,deviationsFromLCIMethodPrinciple}'
        ) || private.portal_localized_text_v1(
          v_modelling #> '{LCIMethodAndAllocation,deviationsFromModellingConstants}'
        ),
      'cutoffRules', private.portal_localized_text_v1(
        v_modelling #> '{dataSourcesTreatmentAndRepresentativeness,deviationsFromCutOffAndCompletenessPrinciples}'
      ),
      'quality', jsonb_build_object(
        'reviewStatus', (
          select nullif(private.portal_scalar_text_v1(review_item -> '@type'), '')
          from private.portal_json_items_v1(v_modelling #> '{validation,review}') as review_item
          limit 1
        ),
        'timeRepresentativeness', private.portal_first_text_v1(
          v_information #> '{time,common:timeRepresentativenessDescription}'
        ),
        'geographyRepresentativeness', private.portal_first_text_v1(
          v_modelling #> '{dataSourcesTreatmentAndRepresentativeness,geographicalRepresentativenessDescription}'
        ),
        'technologyRepresentativeness', private.portal_first_text_v1(
          v_modelling #> '{dataSourcesTreatmentAndRepresentativeness,technologicalRepresentativenessDescription}'
        ),
        'completeness', private.portal_first_text_v1(
          v_modelling #> '{completeness,completenessOtherProblemField}'
        ),
        'uncertainty', private.portal_first_text_v1(
          v_modelling #> '{dataSourcesTreatmentAndRepresentativeness,uncertaintyAdjustments}'
        )
      ),
      'source', private.portal_source_v1('process', p_json),
      'compliance', private.portal_compliance_v1('process', p_json),
      'administration', private.portal_administration_v1('process', p_json)
    );
  elsif p_kind = 'flow' then
    v_information := p_json #> '{flowDataSet,flowInformation}';
    v_modelling := p_json #> '{flowDataSet,modellingAndValidation}';
    v_location := v_information -> 'geography';
    v_location_code := case jsonb_typeof(v_location -> 'locationOfSupply')
      when 'string' then nullif(
        private.portal_scalar_text_v1(v_location -> 'locationOfSupply'),
        ''
      )
      when 'object' then nullif(
        private.portal_scalar_text_v1(v_location #> '{locationOfSupply,@location}'),
        ''
      )
      else null
    end;
    v_cas := nullif(btrim(coalesce(
      v_information #>> '{dataSetInformation,CASNumber}',
      v_information #>> '{dataSetInformation,common:CASNumber}'
    )), '');
    if v_cas !~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$' then
      v_cas := null;
    end if;
    return jsonb_build_object(
      'kind', 'flow',
      'names', private.portal_localized_text_v1(v_information #> '{dataSetInformation,name,baseName}'),
      'synonyms', private.portal_localized_text_v1(v_information #> '{dataSetInformation,common:synonyms}'),
      'generalComment', private.portal_localized_text_v1(v_information #> '{dataSetInformation,common:generalComment}'),
      'casNumber', v_cas,
      'flowType', private.portal_flow_kind_v1(private.portal_scalar_text_v1(
        v_modelling #> '{LCIMethod,typeOfDataSet}'
      )),
      'classifications', private.portal_classifications_v1(v_information #> '{dataSetInformation,classificationInformation}'),
      'locationOfSupply', jsonb_build_object(
        'code', v_location_code,
        'label', private.portal_localized_text_v1(v_location #> '{locationOfSupply,descriptionOfRestrictions}')
      ),
      'referenceFlowProperty', private.portal_reference_flowproperty_v1(p_json),
      'source', private.portal_source_v1('flow', p_json),
      'compliance', private.portal_compliance_v1('flow', p_json),
      'administration', private.portal_administration_v1('flow', p_json)
    );
  end if;
  return null;
end
$function$;

create function private.portal_dataset_projection_v1(
  p_kind text,
  p_id uuid,
  p_version text
)
returns jsonb
language plpgsql
stable
parallel restricted
set search_path = ''
as $function$
declare
  v_json jsonb;
  v_state_code integer;
  v_modified_at timestamptz;
  v_capabilities jsonb;
begin
  if p_kind not in ('process', 'flow')
     or p_version !~ '^\d{2}\.\d{2}\.\d{3}$' then
    return null;
  end if;
  if p_kind = 'process' then
    select row.json, row.state_code, row.modified_at
    into v_json, v_state_code, v_modified_at
    from public.processes as row
    where row.id = p_id
      and row.version::text = p_version
      and row.state_code in (100, 200)
      and jsonb_typeof(row.json) = 'object'
      and jsonb_typeof(row.json -> 'processDataSet') = 'object'
    limit 1;
  else
    select row.json, row.state_code, row.modified_at
    into v_json, v_state_code, v_modified_at
    from public.flows as row
    where row.id = p_id
      and row.version::text = p_version
      and row.state_code in (100, 200)
      and jsonb_typeof(row.json) = 'object'
      and jsonb_typeof(row.json -> 'flowDataSet') = 'object'
    limit 1;
  end if;
  if v_json is null or v_modified_at is null then
    return null;
  end if;
  v_capabilities := private.portal_capabilities_v1(p_kind, v_state_code, v_json);
  return jsonb_build_object(
    'schemaVersion', 'portal.public-dataset.v1',
    'key', jsonb_build_object('kind', p_kind, 'id', p_id::text, 'version', p_version),
    'accessLevel', case when (v_capabilities ->> 'exchangesVisible')::boolean then 'open' else 'metadata_only' end,
    'capabilities', v_capabilities,
    'metadata', private.portal_dataset_metadata_v1(p_kind, v_state_code, v_json),
    'provenance', jsonb_build_object(
      'importBatchId', null,
      'normalizationRuleVersion', null,
      'fieldOrigins', '[]'::jsonb
    ),
    'publication', null,
    'modifiedAt', private.portal_timestamp_v1(v_modified_at)
  );
end
$function$;

create function private.portal_catalog_card_v1(
  p_kind text,
  p_state_code integer,
  p_json jsonb
)
returns jsonb
language plpgsql
stable
parallel restricted
set search_path = ''
as $function$
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
$function$;

create function private.portal_catalog_rows_v1(p_kind text)
returns table(
  id uuid,
  version text,
  json_data jsonb,
  state_code integer,
  modified_at timestamptz,
  lexical_text text
)
language plpgsql
stable
parallel restricted
set search_path = ''
as $function$
begin
  if p_kind = 'process' then
    return query
    select row.id, row.version::text, row.json, row.state_code, row.modified_at,
      ''::text
    from public.processes as row
    where row.state_code in (100, 200)
      and jsonb_typeof(row.json) = 'object'
      and jsonb_typeof(row.json -> 'processDataSet') = 'object'
      and row.modified_at is not null;
  elsif p_kind = 'flow' then
    return query
    select row.id, row.version::text, row.json, row.state_code, row.modified_at,
      ''::text
    from public.flows as row
    where row.state_code in (100, 200)
      and jsonb_typeof(row.json) = 'object'
      and jsonb_typeof(row.json -> 'flowDataSet') = 'object'
      and row.modified_at is not null;
  end if;
end
$function$;

revoke all on function private.portal_dataset_metadata_v1(text, integer, jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_dataset_projection_v1(text, uuid, text) from public, anon, authenticated, service_role;
revoke all on function private.portal_catalog_card_v1(text, integer, jsonb) from public, anon, authenticated, service_role;
revoke all on function private.portal_catalog_rows_v1(text) from public, anon, authenticated, service_role;
revoke all on function private.portal_dataset_rows_v1(text, uuid) from public, anon, authenticated, service_role;

create function private.portal_validate_search_v1(
  p_kind text,
  p_query text,
  p_filters jsonb,
  p_sort text,
  p_limit integer
)
returns void
language plpgsql
stable
parallel safe
set search_path = ''
as $function$
declare
  v_key text;
  v_allowed text[] := array[
    'accessLevel', 'geography', 'classification', 'referenceYearFrom',
    'referenceYearTo', 'source'
  ];
begin
  if p_kind not in ('process', 'flow', 'all')
     or p_query is null
     or length(p_query) > 512
     or pg_catalog.octet_length(p_query) > 2048
     or p_query ~ '[[:cntrl:]]'
     or p_sort is null
     or length(p_sort) > 32
     or pg_catalog.octet_length(p_sort) > 64
     or lower(btrim(p_sort)) not in ('relevance', 'modified_desc', 'name_asc')
     or p_limit is null
     or p_limit < 1
     or p_limit > 50
     or p_filters is null
     or jsonb_typeof(p_filters) <> 'object'
     or pg_catalog.pg_column_size(p_filters) > 4096
     or (select count(*) from jsonb_object_keys(p_filters)) > 7 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  if p_kind in ('process', 'all') then
    v_allowed := pg_catalog.array_append(v_allowed, 'processSubtype');
  end if;
  for v_key in select jsonb_object_keys(p_filters)
  loop
    if not (v_key = any(v_allowed)) then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end loop;
  if p_filters ? 'accessLevel'
     and (
       jsonb_typeof(p_filters -> 'accessLevel') <> 'string'
       or p_filters ->> 'accessLevel' not in ('open', 'metadata_only')
     ) then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  foreach v_key in array array['geography', 'classification', 'processSubtype', 'source']
  loop
    if p_filters ? v_key
       and (
         jsonb_typeof(p_filters -> v_key) <> 'string'
         or length(btrim(p_filters ->> v_key)) not between 1 and 128
       ) then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end loop;
  foreach v_key in array array['referenceYearFrom', 'referenceYearTo']
  loop
    if p_filters ? v_key
       and (
         jsonb_typeof(p_filters -> v_key) <> 'number'
         or (p_filters ->> v_key) !~ '^[0-9]{1,4}$'
       ) then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end loop;
  if p_filters ? 'referenceYearFrom'
     and p_filters ? 'referenceYearTo'
     and (p_filters ->> 'referenceYearFrom')::integer > (p_filters ->> 'referenceYearTo')::integer then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
end
$function$;

create function private.portal_search_v1(
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
  v_items jsonb;
  v_next_cursor text;
begin
  perform private.portal_validate_search_v1(
    p_kind,
    coalesce(p_query, ''),
    coalesce(p_filters, '{}'::jsonb),
    coalesce(p_sort, 'relevance'),
    v_limit
  );
  v_query := lower(btrim(coalesce(p_query, '')));
  v_filters := private.portal_normalize_filters_v1(p_filters);
  v_sort := lower(btrim(coalesce(p_sort, 'relevance')));
  v_fingerprint := private.portal_query_fingerprint_v1(p_kind, v_query, v_filters, v_sort);
  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
       or (select count(*) from jsonb_object_keys(v_cursor)) <> 6
       or v_cursor ->> 'v' <> '1'
       or v_cursor ->> 'fp' <> v_fingerprint
       or v_cursor ->> 'kind' <> p_kind
       or coalesce(v_cursor ->> 'rankKey', '') = ''
       or coalesce(v_cursor ->> 'id', '') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or coalesce(v_cursor ->> 'version', '') !~ '^\d{2}\.\d{2}\.\d{3}$' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
    v_cursor_rank := v_cursor ->> 'rankKey';
    v_cursor_id := (v_cursor ->> 'id')::uuid;
    v_cursor_version := v_cursor ->> 'version';
    if v_sort = 'relevance'
       and (v_cursor_rank !~ '^(0(\.\d+)?|1(\.0+)?)$') then
      raise exception using errcode = '22023', message = 'invalid portal request';
    elsif v_sort = 'modified_desc'
       and private.portal_datetime_v1(v_cursor_rank) is null then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end if;

  with latest as materialized (
    select candidate.*
    from (
      select source.*,
        row_number() over (partition by source.id order by source.version desc) as version_rank
      from private.portal_catalog_rows_v1(p_kind) as source
    ) as candidate
    where candidate.version_rank = 1
  ), decorated as materialized (
    select latest.*,
      private.portal_catalog_card_v1(p_kind, latest.state_code, latest.json_data) as card
    from latest
  ), scored as materialized (
    select decorated.*,
      case
        when nullif(decorated.card #>> '{names,0,value}', '') is not null
          and length(decorated.card #>> '{names,0,value}') <= 500
          and pg_catalog.octet_length(decorated.card #>> '{names,0,value}') <= 2000
          and decorated.card #>> '{names,0,value}' !~ '[[:cntrl:]]'
          then decorated.card #>> '{names,0,value}'
        else '~unnamed:' || decorated.id::text
      end as name_key,
      case
        when v_query = '' then 0::numeric
        when lower(decorated.id::text) = v_query then 1::numeric
        when lower(coalesce(decorated.card ->> 'casNumber', '')) = v_query then 0.98::numeric
        when exists (
          select 1 from jsonb_array_elements(decorated.card -> 'names') as name(item)
          where lower(btrim(item ->> 'value')) = v_query
        ) then 0.95::numeric
        when exists (
          select 1 from jsonb_array_elements(decorated.card -> 'classifications') as classification(item)
          where lower(btrim(item ->> 'code')) = v_query
        ) then 0.92::numeric
        when strpos(lower(concat_ws(' ', decorated.card ->> 'document', decorated.lexical_text)), v_query) > 0
          then 0.70::numeric
        else 0::numeric
      end as score,
      case
        when lower(decorated.id::text) = v_query then jsonb_build_array('exact_id')
        when lower(coalesce(decorated.card ->> 'casNumber', '')) = v_query then jsonb_build_array('cas')
        when exists (
          select 1 from jsonb_array_elements(decorated.card -> 'names') as name(item)
          where lower(btrim(item ->> 'value')) = v_query
             or strpos(lower(item ->> 'value'), v_query) > 0
        ) then jsonb_build_array('name')
        when exists (
          select 1 from jsonb_array_elements(decorated.card -> 'classifications') as classification(item)
          where lower(btrim(item ->> 'code')) = v_query
             or strpos(lower(item ->> 'code'), v_query) > 0
        ) then jsonb_build_array('classification')
        when v_query <> '' then jsonb_build_array('full_text')
        else '[]'::jsonb
      end as reason_codes
    from decorated
  ), filtered as materialized (
    select scored.*,
      case v_sort
        when 'relevance' then scored.score::text
        when 'modified_desc' then private.portal_timestamp_v1(scored.modified_at)
        else lower(scored.name_key)
      end as rank_key
    from scored
    where (v_query = '' or scored.score > 0)
      and (not (v_filters ? 'accessLevel') or scored.card ->> 'accessLevel' = v_filters ->> 'accessLevel')
      and (not (v_filters ? 'geography') or lower(btrim(coalesce(scored.card #>> '{geography,code}', ''))) = v_filters ->> 'geography')
      and (not (v_filters ? 'classification') or exists (
        select 1 from jsonb_array_elements(scored.card -> 'classifications') as classification(item)
        where lower(btrim(item ->> 'code')) = v_filters ->> 'classification'
      ))
      and (not (v_filters ? 'referenceYearFrom') or (scored.card ->> 'referenceYear')::integer >= (v_filters ->> 'referenceYearFrom')::integer)
      and (not (v_filters ? 'referenceYearTo') or (scored.card ->> 'referenceYear')::integer <= (v_filters ->> 'referenceYearTo')::integer)
      and (not (v_filters ? 'processSubtype') or lower(btrim(coalesce(scored.card ->> 'processSubtype', ''))) = v_filters ->> 'processSubtype')
      and (not (v_filters ? 'source') or lower(btrim(coalesce(scored.card ->> 'source', ''))) = v_filters ->> 'source')
  ), after_cursor as materialized (
    select filtered.*
    from filtered
    where v_cursor is null
      or case v_sort
        when 'relevance' then
          filtered.score < v_cursor_rank::numeric
          or (
            filtered.score = v_cursor_rank::numeric
            and (
              filtered.id > v_cursor_id
              or (filtered.id = v_cursor_id and filtered.version < v_cursor_version)
            )
          )
        when 'modified_desc' then
          filtered.modified_at < v_cursor_rank::timestamptz
          or (
            filtered.modified_at = v_cursor_rank::timestamptz
            and (
              filtered.id > v_cursor_id
              or (filtered.id = v_cursor_id and filtered.version < v_cursor_version)
            )
          )
        else
          lower(filtered.name_key) > lower(v_cursor_rank)
          or (
            lower(filtered.name_key) = lower(v_cursor_rank)
            and (
              filtered.id > v_cursor_id
              or (filtered.id = v_cursor_id and filtered.version < v_cursor_version)
            )
          )
      end
  ), ordered as materialized (
    select after_cursor.*,
      row_number() over (
        order by
          case when v_sort = 'relevance' then after_cursor.score end desc,
          case when v_sort = 'modified_desc' then after_cursor.modified_at end desc,
          case when v_sort = 'name_asc' then lower(after_cursor.name_key) end asc,
          after_cursor.id asc,
          after_cursor.version desc
      ) as page_rank
    from after_cursor
    order by
      case when v_sort = 'relevance' then after_cursor.score end desc,
      case when v_sort = 'modified_desc' then after_cursor.modified_at end desc,
      case when v_sort = 'name_asc' then lower(after_cursor.name_key) end asc,
      after_cursor.id asc,
      after_cursor.version desc
    limit v_limit + 1
  )
  select
    coalesce(jsonb_agg(
      jsonb_build_object(
        'key', jsonb_build_object('kind', p_kind, 'id', ordered.id::text, 'version', ordered.version),
        'accessLevel', ordered.card -> 'accessLevel',
        'capabilities', ordered.card -> 'capabilities',
        'names', ordered.card -> 'names',
        'summary', ordered.card -> 'summary',
        'geography', ordered.card -> 'geography',
        'referenceYear', ordered.card -> 'referenceYear',
        'modifiedAt', private.portal_timestamp_v1(ordered.modified_at),
        'match', jsonb_build_object(
          'kind', case when ordered.reason_codes ?| array['exact_id', 'cas', 'classification'] then 'identifier' else 'lexical' end,
          'score', ordered.score,
          'reasonCodes', ordered.reason_codes
        )
      ) order by ordered.page_rank
    ) filter (where ordered.page_rank <= v_limit), '[]'::jsonb),
    case when max(ordered.page_rank) > v_limit then private.portal_cursor_encode_v1(
      (jsonb_agg(jsonb_build_object(
        'v', 1,
        'fp', v_fingerprint,
        'rankKey', ordered.rank_key,
        'kind', p_kind,
        'id', ordered.id::text,
        'version', ordered.version
      ) order by ordered.page_rank) filter (where ordered.page_rank = v_limit)) -> 0
    ) else null end
  into v_items, v_next_cursor
  from ordered;

  return jsonb_build_object(
    'schemaVersion', 'portal.public-search-page.v1',
    'kind', p_kind,
    'queryFingerprint', v_fingerprint,
    'items', v_items,
    'nextCursor', v_next_cursor
  );
end
$function$;

revoke all on function private.portal_validate_search_v1(text, text, jsonb, text, integer) from public, anon, authenticated, service_role;
revoke all on function private.portal_search_v1(text, text, jsonb, text, text, integer) from public, anon, authenticated, service_role;

create function api.portal_search_processes_v1(
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
  return private.portal_search_v1('process', p_query, p_filters, p_sort, p_cursor, p_limit);
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$;

create function api.portal_search_flows_v1(
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
  return private.portal_search_v1('flow', p_query, p_filters, p_sort, p_cursor, p_limit);
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$;

create function api.portal_get_dataset_v1(
  p_kind text,
  p_id uuid,
  p_version text
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
set statement_timeout = '8s'
as $function$
begin
  if p_kind not in ('process', 'flow')
     or p_id is null
     or p_version is null
     or p_version !~ '^\d{2}\.\d{2}\.\d{3}$' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  return private.portal_dataset_projection_v1(p_kind, p_id, p_version);
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal catalog unavailable';
end
$function$;

create function api.portal_list_versions_v1(
  p_kind text,
  p_id uuid,
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
declare
  v_cursor jsonb;
  v_cursor_version text;
  v_items jsonb;
  v_next_cursor text;
begin
  if p_kind not in ('process', 'flow')
     or p_id is null
     or p_limit is null
     or p_limit not between 1 and 50 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
       or (select count(*) from jsonb_object_keys(v_cursor)) <> 4
       or v_cursor ->> 'v' <> '1'
       or v_cursor ->> 'kind' <> p_kind
       or v_cursor ->> 'id' <> p_id::text
       or coalesce(v_cursor ->> 'version', '') !~ '^\d{2}\.\d{2}\.\d{3}$' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
    v_cursor_version := v_cursor ->> 'version';
  end if;

  with all_versions as materialized (
    select source.*,
      row_number() over (order by source.version desc) = 1 as is_latest,
      private.portal_capabilities_v1(p_kind, source.state_code, source.json_data) as capabilities
    from private.portal_dataset_rows_v1(p_kind, p_id) as source
  ), ordered as materialized (
    select all_versions.*,
      row_number() over (order by all_versions.version desc) as page_rank
    from all_versions
    where v_cursor_version is null or all_versions.version < v_cursor_version
    order by all_versions.version desc
    limit p_limit + 1
  )
  select
    coalesce(jsonb_agg(jsonb_build_object(
      'key', jsonb_build_object('kind', p_kind, 'id', ordered.id::text, 'version', ordered.version),
      'accessLevel', case when (ordered.capabilities ->> 'exchangesVisible')::boolean then 'open' else 'metadata_only' end,
      'capabilities', ordered.capabilities,
      'modifiedAt', private.portal_timestamp_v1(ordered.modified_at),
      'isLatest', ordered.is_latest
    ) order by ordered.page_rank) filter (where ordered.page_rank <= p_limit), '[]'::jsonb),
    case when max(ordered.page_rank) > p_limit then private.portal_cursor_encode_v1(
      (jsonb_agg(jsonb_build_object(
        'v', 1, 'kind', p_kind, 'id', p_id::text, 'version', ordered.version
      ) order by ordered.page_rank) filter (where ordered.page_rank = p_limit)) -> 0
    ) else null end
  into v_items, v_next_cursor
  from ordered;

  return jsonb_build_object(
    'schemaVersion', 'portal.public-version-page.v1',
    'dataset', jsonb_build_object('kind', p_kind, 'id', p_id::text),
    'items', v_items,
    'nextCursor', v_next_cursor
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

create function api.portal_list_process_exchanges_v1(
  p_process_id uuid,
  p_process_version text,
  p_exchange_kind text default 'all',
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
declare
  v_kind text;
  v_process_json jsonb;
  v_process_state integer;
  v_functional_unit jsonb;
  v_cursor jsonb;
  v_cursor_internal integer;
  v_cursor_internal_text text;
  v_cursor_kind text;
  v_rows jsonb;
  v_next_cursor text;
begin
  if pg_catalog.octet_length(coalesce(p_exchange_kind, '')) > 32 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_kind := lower(btrim(coalesce(p_exchange_kind, 'all')));
  if p_process_id is null
     or p_process_version is null
     or p_process_version !~ '^\d{2}\.\d{2}\.\d{3}$'
     or v_kind not in ('all', 'technosphere', 'elementary', 'waste')
     or p_limit is null
     or p_limit not between 1 and 50 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  select row.json, row.state_code
  into v_process_json, v_process_state
  from public.processes as row
  where row.id = p_process_id
    and row.version::text = p_process_version
    and row.state_code in (100, 200)
    and jsonb_typeof(row.json) = 'object'
    and jsonb_typeof(row.json -> 'processDataSet') = 'object'
  limit 1;
  if v_process_json is null then
    return null;
  end if;
  v_functional_unit := private.portal_process_functional_unit_v1(v_process_state, v_process_json);

  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
       or (select count(*) from jsonb_object_keys(v_cursor)) <> 6
       or v_cursor ->> 'v' <> '1'
       or v_cursor ->> 'processId' <> p_process_id::text
       or v_cursor ->> 'processVersion' <> p_process_version
       or v_cursor ->> 'filterKind' <> v_kind
       or coalesce(v_cursor ->> 'internalId', '') !~ '^(0|[1-9][0-9]{0,5})$'
       or v_cursor ->> 'kind' not in ('technosphere', 'elementary', 'waste') then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
    v_cursor_internal_text := v_cursor ->> 'internalId';
    v_cursor_internal := v_cursor_internal_text::integer;
    v_cursor_kind := v_cursor ->> 'kind';
  end if;

  with raw_exchanges as materialized (
    select exchange.item,
      exchange.item ->> '@dataSetInternalID' as internal_id,
      count(*) over (partition by exchange.item ->> '@dataSetInternalID') as identity_count
    from private.portal_json_items_v1(v_process_json #> '{processDataSet,exchanges,exchange}') as exchange(item)
  ), supported as materialized (
    select support -> 'row' as row_data
    from raw_exchanges
    cross join lateral private.portal_exchange_support_v1(v_process_state, v_process_json, raw_exchanges.item) as support
    where raw_exchanges.identity_count = 1
      and nullif(v_functional_unit ->> 'amount', '') is not null
      and nullif(v_functional_unit ->> 'unit', '') is not null
      and support is not null
  ), filtered as materialized (
    select supported.row_data,
      (supported.row_data ->> 'internalId')::integer as internal_number,
      supported.row_data ->> 'internalId' as internal_text,
      supported.row_data ->> 'kind' as row_kind
    from supported
    where v_kind = 'all' or supported.row_data ->> 'kind' = v_kind
  ), ordered as materialized (
    select filtered.*,
      row_number() over (order by filtered.internal_number, filtered.internal_text, filtered.row_kind) as page_rank
    from filtered
    where v_cursor is null
      or (filtered.internal_number, filtered.internal_text, filtered.row_kind) >
         (v_cursor_internal, v_cursor_internal_text, v_cursor_kind)
    order by filtered.internal_number, filtered.internal_text, filtered.row_kind
    limit p_limit + 1
  )
  select
    coalesce(jsonb_agg(ordered.row_data order by ordered.page_rank)
      filter (where ordered.page_rank <= p_limit), '[]'::jsonb),
    case when max(ordered.page_rank) > p_limit then private.portal_cursor_encode_v1(
      (jsonb_agg(jsonb_build_object(
        'v', 1,
        'processId', p_process_id::text,
        'processVersion', p_process_version,
        'filterKind', v_kind,
        'internalId', ordered.internal_text,
        'kind', ordered.row_kind
      ) order by ordered.page_rank) filter (where ordered.page_rank = p_limit)) -> 0
    ) else null end
  into v_rows, v_next_cursor
  from ordered;

  return jsonb_build_object(
    'schemaVersion', 'portal.public-exchange-page.v1',
    'process', jsonb_build_object('id', p_process_id::text, 'version', p_process_version),
    'processContext', jsonb_build_object(
      'functionalUnit', v_functional_unit,
      'capabilityPolicyVersion', 'portal-capability-policy.v1'
    ),
    'rows', v_rows,
    'nextCursor', v_next_cursor
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

create function api.portal_facets_v1(
  p_kind text,
  p_query text,
  p_filters jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
set statement_timeout = '8s'
as $function$
declare
  v_kind text;
  v_query text;
  v_filters jsonb;
  v_fingerprint text;
  v_groups jsonb;
begin
  if pg_catalog.octet_length(coalesce(p_kind, '')) > 32 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_kind := lower(btrim(coalesce(p_kind, '')));
  perform private.portal_validate_search_v1(
    v_kind,
    coalesce(p_query, ''),
    coalesce(p_filters, '{}'::jsonb),
    'relevance',
    1
  );
  v_query := lower(btrim(coalesce(p_query, '')));
  v_filters := private.portal_normalize_filters_v1(p_filters);
  v_fingerprint := private.portal_query_fingerprint_v1(v_kind, v_query, v_filters, 'relevance');

  with source_rows as materialized (
    select kinds.kind as dataset_kind, source.*
    from (values ('process'::text), ('flow'::text)) as kinds(kind)
    cross join lateral private.portal_catalog_rows_v1(kinds.kind) as source
    where v_kind = 'all' or kinds.kind = v_kind
  ), latest as materialized (
    select candidate.*
    from (
      select source_rows.*,
        row_number() over (
          partition by source_rows.dataset_kind, source_rows.id
          order by source_rows.version desc
        ) as version_rank
      from source_rows
    ) as candidate
    where candidate.version_rank = 1
  ), decorated as materialized (
    select latest.*,
      private.portal_catalog_card_v1(latest.dataset_kind, latest.state_code, latest.json_data) as card
    from latest
  ), matched as materialized (
    select decorated.*
    from decorated
    where (
      v_query = ''
      or lower(decorated.id::text) = v_query
      or lower(coalesce(decorated.card ->> 'casNumber', '')) = v_query
      or strpos(lower(concat_ws(' ', decorated.card ->> 'document', decorated.lexical_text)), v_query) > 0
    )
      and (not (v_filters ? 'accessLevel') or decorated.card ->> 'accessLevel' = v_filters ->> 'accessLevel')
      and (not (v_filters ? 'geography') or lower(btrim(coalesce(decorated.card #>> '{geography,code}', ''))) = v_filters ->> 'geography')
      and (not (v_filters ? 'classification') or exists (
        select 1 from jsonb_array_elements(decorated.card -> 'classifications') as classification(item)
        where lower(btrim(item ->> 'code')) = v_filters ->> 'classification'
      ))
      and (not (v_filters ? 'referenceYearFrom') or (decorated.card ->> 'referenceYear')::integer >= (v_filters ->> 'referenceYearFrom')::integer)
      and (not (v_filters ? 'referenceYearTo') or (decorated.card ->> 'referenceYear')::integer <= (v_filters ->> 'referenceYearTo')::integer)
      and (not (v_filters ? 'processSubtype') or lower(btrim(coalesce(decorated.card ->> 'processSubtype', ''))) = v_filters ->> 'processSubtype')
      and (not (v_filters ? 'source') or lower(btrim(coalesce(decorated.card ->> 'source', ''))) = v_filters ->> 'source')
  ), facet_values as materialized (
    select 'kind'::text as group_id, 1 as group_order,
      matched.dataset_kind as value, matched.dataset_kind as label
    from matched
    union all
    select 'accessLevel', 2,
      matched.card ->> 'accessLevel' as value, matched.card ->> 'accessLevel' as label
    from matched
    union all
    select 'geography', 3,
      lower(btrim(matched.card #>> '{geography,code}')),
      matched.card #>> '{geography,code}'
    from matched
    union all
    select 'referenceYear', 4,
      btrim(matched.card ->> 'referenceYear'),
      btrim(matched.card ->> 'referenceYear')
    from matched
    union all
    select 'processSubtype', 5,
      lower(btrim(matched.card ->> 'processSubtype')),
      matched.card ->> 'processSubtype'
    from matched where matched.dataset_kind = 'process'
    union all
    select 'source', 6,
      lower(btrim(matched.card ->> 'source')),
      matched.card ->> 'source'
    from matched
  ), counts as materialized (
    select group_id, group_order, value, min(value) as label, count(*) as value_count
    from facet_values
    where nullif(btrim(value), '') is not null
      and length(value) <= 128
      and pg_catalog.octet_length(value) <= 512
    group by group_id, group_order, value
  ), ranked_counts as materialized (
    select counts.*,
      row_number() over (
        partition by counts.group_id
        order by counts.value
      ) as value_rank
    from counts
  ), grouped as materialized (
    select ranked_counts.group_id, ranked_counts.group_order,
      jsonb_agg(jsonb_build_object(
        'value', ranked_counts.value,
        'label', jsonb_build_array(jsonb_build_object('language', 'und', 'value', ranked_counts.label)),
        'count', ranked_counts.value_count
      ) order by ranked_counts.value) filter (where ranked_counts.value_rank <= 100) as values_json,
      bool_or(ranked_counts.value_rank > 100) as has_more
    from ranked_counts
    group by ranked_counts.group_id, ranked_counts.group_order
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'id', grouped.group_id,
    'label', jsonb_build_array(
      jsonb_build_object('language', 'en', 'value', case grouped.group_id
        when 'kind' then 'Object type'
        when 'accessLevel' then 'Access level'
        when 'geography' then 'Geography'
        when 'referenceYear' then 'Reference year'
        when 'processSubtype' then 'Process subtype'
        else 'Source'
      end),
      jsonb_build_object('language', 'zh-CN', 'value', case grouped.group_id
        when 'kind' then '对象类型'
        when 'accessLevel' then '访问级别'
        when 'geography' then '地区'
        when 'referenceYear' then '参考年'
        when 'processSubtype' then '过程类型'
        else '数据源'
      end)
    ),
    'values', grouped.values_json,
    'hasMore', grouped.has_more
  ) order by grouped.group_order), '[]'::jsonb)
  into v_groups
  from grouped;

  return jsonb_build_object(
    'schemaVersion', 'portal.public-facets.v1',
    'kind', v_kind,
    'queryFingerprint', v_fingerprint,
    'groups', v_groups
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

create function api.portal_sitemap_entries_v1(
  p_kind text,
  p_cursor text default null,
  p_limit integer default 1000
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
set statement_timeout = '8s'
as $function$
declare
  v_filter_kind text;
  v_cursor jsonb;
  v_cursor_kind text;
  v_cursor_id uuid;
  v_items jsonb;
  v_next_cursor text;
begin
  if pg_catalog.octet_length(coalesce(p_kind, '')) > 32 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_filter_kind := lower(btrim(coalesce(p_kind, '')));
  if v_filter_kind not in ('process', 'flow', 'all')
     or p_limit is null
     or p_limit not between 1 and 1000 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
       or (select count(*) from jsonb_object_keys(v_cursor)) <> 5
       or v_cursor ->> 'v' <> '1'
       or v_cursor ->> 'filterKind' <> v_filter_kind
       or v_cursor ->> 'kind' not in ('process', 'flow')
       or (v_filter_kind <> 'all' and v_cursor ->> 'kind' <> v_filter_kind)
       or coalesce(v_cursor ->> 'id', '') !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or coalesce(v_cursor ->> 'version', '') !~ '^\d{2}\.\d{2}\.\d{3}$' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
    v_cursor_kind := v_cursor ->> 'kind';
    v_cursor_id := (v_cursor ->> 'id')::uuid;
  end if;

  with source_rows as materialized (
    select kinds.kind, source.*
    from (values ('process'::text), ('flow'::text)) as kinds(kind)
    cross join lateral private.portal_catalog_rows_v1(kinds.kind) as source
    where v_filter_kind = 'all' or kinds.kind = v_filter_kind
  ), latest as materialized (
    select candidate.*
    from (
      select source_rows.*,
        row_number() over (
          partition by source_rows.kind, source_rows.id
          order by source_rows.version desc
        ) as version_rank
      from source_rows
    ) as candidate
    where candidate.version_rank = 1
  ), ordered as materialized (
    select latest.*,
      row_number() over (order by latest.kind, latest.id) as page_rank
    from latest
    where v_cursor is null or (latest.kind, latest.id) > (v_cursor_kind, v_cursor_id)
    order by latest.kind, latest.id
    limit p_limit + 1
  )
  select
    coalesce(jsonb_agg(jsonb_build_object(
      'key', jsonb_build_object(
        'kind', ordered.kind,
        'id', ordered.id::text,
        'version', ordered.version
      ),
      'modifiedAt', private.portal_timestamp_v1(ordered.modified_at)
    ) order by ordered.page_rank) filter (where ordered.page_rank <= p_limit), '[]'::jsonb),
    case when max(ordered.page_rank) > p_limit then private.portal_cursor_encode_v1(
      (jsonb_agg(jsonb_build_object(
        'v', 1,
        'filterKind', v_filter_kind,
        'kind', ordered.kind,
        'id', ordered.id::text,
        'version', ordered.version
      ) order by ordered.page_rank) filter (where ordered.page_rank = p_limit)) -> 0
    ) else null end
  into v_items, v_next_cursor
  from ordered;

  return jsonb_build_object(
    'schemaVersion', 'portal.public-sitemap-page.v1',
    'items', v_items,
    'nextCursor', v_next_cursor
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

comment on function api.portal_search_processes_v1(text, jsonb, text, text, integer) is
  'Locator-free public Process lexical/identifier search over one fixed 100/200 scope with query-bound keyset cursors.';
comment on function api.portal_search_flows_v1(text, jsonb, text, text, integer) is
  'Locator-free public Flow lexical/identifier search over one fixed 100/200 scope with query-bound keyset cursors.';
comment on function api.portal_get_dataset_v1(text, uuid, text) is
  'Exact locator-free public Process/Flow metadata envelope. Missing and non-public identities return SQL NULL.';
comment on function api.portal_list_versions_v1(text, uuid, text, integer) is
  'Keyset page of exact visible versions for one public Process/Flow identity.';
comment on function api.portal_list_process_exchanges_v1(uuid, text, text, text, integer) is
  'Locator-free Process Exchange page. Numeric rows require an exact state-100 Process/Flow/FlowProperty/UnitGroup support chain.';
comment on function api.portal_facets_v1(text, text, jsonb) is
  'Full-result public catalog facet counts for process, flow, or all; never derived from a result-page sample.';
comment on function api.portal_sitemap_entries_v1(text, text, integer) is
  'Bounded keyset page of the latest visible exact Process/Flow identities for Portal sitemap generation.';

revoke all on function api.portal_search_processes_v1(text, jsonb, text, text, integer)
  from public, anon, authenticated, service_role;
revoke all on function api.portal_search_flows_v1(text, jsonb, text, text, integer)
  from public, anon, authenticated, service_role;
revoke all on function api.portal_get_dataset_v1(text, uuid, text)
  from public, anon, authenticated, service_role;
revoke all on function api.portal_list_versions_v1(text, uuid, text, integer)
  from public, anon, authenticated, service_role;
revoke all on function api.portal_list_process_exchanges_v1(uuid, text, text, text, integer)
  from public, anon, authenticated, service_role;
revoke all on function api.portal_facets_v1(text, text, jsonb)
  from public, anon, authenticated, service_role;
revoke all on function api.portal_sitemap_entries_v1(text, text, integer)
  from public, anon, authenticated, service_role;

grant execute on function api.portal_search_processes_v1(text, jsonb, text, text, integer)
  to anon, authenticated;
grant execute on function api.portal_search_flows_v1(text, jsonb, text, text, integer)
  to anon, authenticated;
grant execute on function api.portal_get_dataset_v1(text, uuid, text)
  to anon, authenticated;
grant execute on function api.portal_list_versions_v1(text, uuid, text, integer)
  to anon, authenticated;
grant execute on function api.portal_list_process_exchanges_v1(uuid, text, text, text, integer)
  to anon, authenticated;
grant execute on function api.portal_facets_v1(text, text, jsonb)
  to anon, authenticated;
grant execute on function api.portal_sitemap_entries_v1(text, text, integer)
  to anon, authenticated;

reset role;
revoke create on schema private, api from portal_public_executor;
revoke portal_public_executor from postgres;

do $portal_executor_membership_guard$
begin
  if (
    select count(*)
    from pg_catalog.pg_auth_members as membership
    where membership.member = 'portal_public_executor'::regrole
       or membership.roleid = 'portal_public_executor'::regrole
  ) <> 1
  or not exists (
    select 1
    from pg_catalog.pg_auth_members as membership
    where membership.roleid = 'portal_public_executor'::regrole
      and membership.member = 'postgres'::regrole
      and membership.admin_option
      and not membership.inherit_option
      and not membership.set_option
  ) then
    raise exception 'portal_public_executor has unexpected role membership'
      using errcode = '42501';
  end if;
end
$portal_executor_membership_guard$;

insert into private.api_capability_grants (
  routine_identity,
  capability_id,
  allow_anon,
  allow_authenticated,
  allow_service_role
)
values
  ('api.portal_search_processes_v1(text, jsonb, text, text, integer)', 'PORTAL-CATALOG-01', true, true, false),
  ('api.portal_search_flows_v1(text, jsonb, text, text, integer)', 'PORTAL-CATALOG-01', true, true, false),
  ('api.portal_get_dataset_v1(text, uuid, text)', 'PORTAL-CATALOG-01', true, true, false),
  ('api.portal_list_versions_v1(text, uuid, text, integer)', 'PORTAL-CATALOG-01', true, true, false),
  ('api.portal_list_process_exchanges_v1(uuid, text, text, text, integer)', 'PORTAL-CATALOG-01', true, true, false),
  ('api.portal_facets_v1(text, text, jsonb)', 'PORTAL-CATALOG-01', true, true, false),
  ('api.portal_sitemap_entries_v1(text, text, integer)', 'PORTAL-CATALOG-01', true, true, false)
on conflict (routine_identity) do update
set capability_id = excluded.capability_id,
    allow_anon = excluded.allow_anon,
    allow_authenticated = excluded.allow_authenticated,
    allow_service_role = excluded.allow_service_role;

do $verify_portal_catalog_contract$
declare
  v_identity text;
  v_routine regprocedure;
begin
  foreach v_identity in array array[
    'api.portal_search_processes_v1(text, jsonb, text, text, integer)',
    'api.portal_search_flows_v1(text, jsonb, text, text, integer)',
    'api.portal_get_dataset_v1(text, uuid, text)',
    'api.portal_list_versions_v1(text, uuid, text, integer)',
    'api.portal_list_process_exchanges_v1(uuid, text, text, text, integer)',
    'api.portal_facets_v1(text, text, jsonb)',
    'api.portal_sitemap_entries_v1(text, text, integer)'
  ]
  loop
    v_routine := pg_catalog.to_regprocedure(v_identity);
    if v_routine is null then
      raise exception 'portal catalog routine missing';
    end if;
    if (
      select routine.proowner
      from pg_catalog.pg_proc as routine
      where routine.oid = v_routine
    ) <> 'portal_public_executor'::regrole then
      raise exception 'portal catalog routine owner mismatch';
    end if;
    if not exists (
      select 1
      from private.api_capability_grants as manifest
      where manifest.routine_identity = v_identity
        and manifest.capability_id = 'PORTAL-CATALOG-01'
        and manifest.allow_anon
        and manifest.allow_authenticated
        and not manifest.allow_service_role
    ) then
      raise exception 'portal catalog capability manifest mismatch';
    end if;
    if (
      select count(*)
      from pg_catalog.pg_proc as routine
      cross join lateral pg_catalog.aclexplode(
        coalesce(routine.proacl, pg_catalog.acldefault('f', routine.proowner))
      ) as acl
      where routine.oid = v_routine
    ) <> 3
    or exists (
      select 1
      from pg_catalog.pg_proc as routine
      cross join lateral pg_catalog.aclexplode(
        coalesce(routine.proacl, pg_catalog.acldefault('f', routine.proowner))
      ) as acl
      where routine.oid = v_routine
        and not (
          (
            acl.grantee = routine.proowner
            and acl.privilege_type = 'EXECUTE'
            and not acl.is_grantable
          )
          or (
            acl.grantee in ('anon'::regrole, 'authenticated'::regrole)
            and acl.privilege_type = 'EXECUTE'
            and not acl.is_grantable
          )
        )
    ) then
      raise exception 'portal catalog routine ACL mismatch';
    end if;
  end loop;

  if (
    select count(*)
    from pg_catalog.pg_proc as routine
    join pg_catalog.pg_namespace as namespace
      on namespace.oid = routine.pronamespace
    where namespace.nspname = 'api'
      and routine.proname like 'portal\_%\_v1' escape '\'
  ) <> 7 then
    raise exception 'unexpected Portal v1 API routine';
  end if;

  if (
    select count(*)
    from private.api_capability_grants as manifest
    where manifest.capability_id = 'PORTAL-CATALOG-01'
  ) <> 7 then
    raise exception 'unexpected PORTAL-CATALOG-01 manifest entry';
  end if;
end
$verify_portal_catalog_contract$;

do $verify_portal_legacy_api_unchanged$
begin
  if exists (
    with after_state as (
      select
        before_state.routine_identity,
        pg_catalog.pg_get_functiondef(routine.oid) as definition,
        owner_role.rolname as owner_name,
        routine.prosecdef as security_definer,
        coalesce(routine.proconfig, '{}'::text[]) as proconfig,
        coalesce(routine.proacl::text, '') as acl_text
      from portal_legacy_api_before as before_state
      join pg_catalog.pg_proc as routine
        on routine.oid = pg_catalog.to_regprocedure(before_state.routine_identity)
      join pg_catalog.pg_roles as owner_role
        on owner_role.oid = routine.proowner
    )
    (select * from portal_legacy_api_before except select * from after_state)
    union all
    (select * from after_state except select * from portal_legacy_api_before)
  ) then
    raise exception 'Portal migration changed a legacy Search/Hybrid contract';
  end if;
end
$verify_portal_legacy_api_unchanged$;

notify pgrst, 'reload schema';

commit;
