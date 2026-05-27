create or replace function util.dataset_json_search_text_allowed_prefixes(p_table text)
returns text[]
language sql
immutable
parallel safe
as $$
  select case lower(btrim(coalesce(p_table, '')))
    when 'flows' then array[
      'flowDataSet.flowInformation.dataSetInformation.name',
      'flowDataSet.flowInformation.dataSetInformation.classificationInformation',
      'flowDataSet.flowInformation.dataSetInformation.CASNumber',
      'flowDataSet.flowInformation.dataSetInformation.common:other',
      'flowDataSet.flowInformation.dataSetInformation.common:synonyms',
      'flowDataSet.flowInformation.dataSetInformation.common:generalComment',
      'flowDataSet.modellingAndValidation.LCIMethod.typeOfDataSet',
      'flowDataSet.flowProperties.flowProperty.referenceToFlowPropertyDataSet.common:shortDescription'
    ]
    when 'processes' then array[
      'processDataSet.processInformation.dataSetInformation.name',
      'processDataSet.processInformation.dataSetInformation.classificationInformation',
      'processDataSet.processInformation.dataSetInformation.common:generalComment',
      'processDataSet.processInformation.geography.locationOfOperationSupplyOrProduction',
      'processDataSet.processInformation.time',
      'processDataSet.processInformation.technology',
      'processDataSet.processInformation.quantitativeReference.referenceToReferenceFlow.common:shortDescription',
      'processDataSet.modellingAndValidation.LCIMethod.typeOfDataSet'
    ]
    when 'lifecyclemodels' then array[
      'lifeCycleModelDataSet.lifeCycleModelInformation.dataSetInformation.name',
      'lifeCycleModelDataSet.lifeCycleModelInformation.dataSetInformation.classificationInformation',
      'lifeCycleModelDataSet.lifeCycleModelInformation.dataSetInformation.common:generalComment',
      'lifeCycleModelDataSet.lifeCycleModelInformation.technology.processes.processInstance.referenceToProcess.common:shortDescription'
    ]
    when 'contacts' then array[
      'contactDataSet.contactInformation.dataSetInformation.common:name',
      'contactDataSet.contactInformation.dataSetInformation.common:shortName',
      'contactDataSet.contactInformation.dataSetInformation.name',
      'contactDataSet.contactInformation.dataSetInformation.shortName',
      'contactDataSet.contactInformation.dataSetInformation.email',
      'contactDataSet.contactInformation.dataSetInformation.telephone',
      'contactDataSet.contactInformation.dataSetInformation.wwwAddress',
      'contactDataSet.contactInformation.dataSetInformation.classificationInformation'
    ]
    when 'sources' then array[
      'sourceDataSet.sourceInformation.dataSetInformation.common:name',
      'sourceDataSet.sourceInformation.dataSetInformation.common:shortName',
      'sourceDataSet.sourceInformation.dataSetInformation.classificationInformation',
      'sourceDataSet.sourceInformation.dataSetInformation.sourceDescriptionOrComment'
    ]
    when 'unitgroups' then array[
      'unitGroupDataSet.unitGroupInformation.dataSetInformation.common:name',
      'unitGroupDataSet.unitGroupInformation.dataSetInformation.common:shortName',
      'unitGroupDataSet.unitGroupInformation.dataSetInformation.common:synonyms',
      'unitGroupDataSet.unitGroupInformation.dataSetInformation.common:generalComment',
      'unitGroupDataSet.unitGroupInformation.dataSetInformation.classificationInformation',
      'unitGroupDataSet.units.unit.name'
    ]
    when 'flowproperties' then array[
      'flowPropertyDataSet.flowPropertiesInformation.dataSetInformation.common:name',
      'flowPropertyDataSet.flowPropertiesInformation.dataSetInformation.common:shortName',
      'flowPropertyDataSet.flowPropertiesInformation.dataSetInformation.common:synonyms',
      'flowPropertyDataSet.flowPropertiesInformation.dataSetInformation.common:generalComment',
      'flowPropertyDataSet.flowPropertiesInformation.dataSetInformation.classificationInformation',
      'flowPropertyDataSet.flowPropertiesInformation.quantitativeReference.referenceToReferenceUnitGroup.common:shortDescription'
    ]
    else array[
      'name',
      'common:name',
      'shortName',
      'common:shortName',
      'classificationInformation',
      'common:synonyms',
      'common:generalComment',
      'sourceDescriptionOrComment',
      'technology',
      'time',
      'geography'
    ]
  end;
$$;

create or replace function util.dataset_json_search_text_is_noise(
  p_text text,
  p_leaf_key text
) returns boolean
language sql
immutable
parallel safe
as $$
  select coalesce(
    nullif(btrim(p_text), '') is null
    or coalesce(p_leaf_key, '') like '@%'
    or coalesce(p_leaf_key, '') in (
      'common:UUID',
      'common:dataSetVersion',
      'common:permanentDataSetURI',
      'common:timeStamp',
      'meanValue',
      'resultingAmount',
      'referenceToReferenceUnit'
    )
    or btrim(p_text) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or lower(btrim(p_text)) like 'http://%'
    or lower(btrim(p_text)) like 'https://%'
    or btrim(p_text) like '../%'
    or btrim(p_text) like '/%'
    or lower(btrim(p_text)) like '%.xsd%'
    or lower(btrim(p_text)) like '%.xml%'
    or lower(btrim(p_text)) like '%/schemas/%'
    or btrim(p_text) ~* '^[0-9]{4}-[0-9]{2}-[0-9]{2}t[0-9]{2}:[0-9]{2}:[0-9]{2}',
    true
  );
$$;

create or replace function util.dataset_json_search_text(
  p_table text,
  p_json jsonb
) returns text
language sql
immutable
parallel safe
as $$
  with recursive walk(path, value) as (
    select array[]::text[], p_json
    where p_json is not null

    union all

    select w.path || child.key, child.value
    from walk w
    cross join lateral (
      select elem.ordinality::text as key, elem.value
      from jsonb_array_elements(
        case when jsonb_typeof(w.value) = 'array' then w.value else '[]'::jsonb end
      ) with ordinality as elem(value, ordinality)

      union all

      select obj.key, obj.value
      from jsonb_each(
        case when jsonb_typeof(w.value) = 'object' then w.value else '{}'::jsonb end
      ) as obj(key, value)
    ) child
  ),
  scalar_text as (
    select distinct
           nullif(btrim(value #>> '{}'), '') as text_value,
           path[cardinality(path)] as leaf_key,
           array_to_string(path, '.') as path_text
    from walk
    where jsonb_typeof(value) in ('string', 'number', 'boolean')
  )
  select coalesce(
    string_agg(text_value, ' ' order by lower(text_value), text_value),
    ''
  )
  from scalar_text
  where text_value is not null
    and exists (
      select 1
      from unnest(util.dataset_json_search_text_allowed_prefixes(p_table)) as allowed(prefix)
      where scalar_text.path_text = allowed.prefix
         or scalar_text.path_text like allowed.prefix || '.%'
    )
    and not util.dataset_json_search_text_is_noise(text_value, leaf_key);
$$;

create or replace function util.dataset_json_search_text(p_json jsonb)
returns text
language sql
immutable
parallel safe
as $$
  select util.dataset_json_search_text('generic', p_json);
$$;

alter function util.dataset_json_search_text_allowed_prefixes(text) owner to postgres;
alter function util.dataset_json_search_text_is_noise(text, text) owner to postgres;
alter function util.dataset_json_search_text(text, jsonb) owner to postgres;
alter function util.dataset_json_search_text(jsonb) owner to postgres;

revoke all on function util.dataset_json_search_text_allowed_prefixes(text) from public;
revoke all on function util.dataset_json_search_text_is_noise(text, text) from public;
revoke all on function util.dataset_json_search_text(text, jsonb) from public;
revoke all on function util.dataset_json_search_text(jsonb) from public;

grant execute on function util.dataset_json_search_text(text, jsonb) to service_role;
grant execute on function util.dataset_json_search_text(jsonb) to service_role;

create or replace function util.set_dataset_extracted_text_from_json()
returns trigger
language plpgsql
set search_path to 'public', 'util', 'pg_temp'
as $$
begin
  new.extracted_text := util.dataset_json_search_text(TG_TABLE_NAME, new.json);
  return new;
end;
$$;

alter function util.set_dataset_extracted_text_from_json() owner to postgres;
revoke all on function util.set_dataset_extracted_text_from_json() from public;
grant execute on function util.set_dataset_extracted_text_from_json() to service_role;

create or replace function public.cmd_dataset_extracted_text_backfill(
  p_table text,
  p_batch_size integer default 1000,
  p_after_id uuid default null,
  p_after_version text default null,
  p_mode text default 'empty'
) returns jsonb
language plpgsql
security definer
set search_path to ''
as $$
declare
  v_table text := lower(btrim(coalesce(p_table, '')));
  v_mode text := lower(btrim(coalesce(p_mode, 'empty')));
  v_batch_size integer := least(greatest(coalesce(p_batch_size, 1000), 1), 5000);
  v_scanned_count integer := 0;
  v_updated_count integer := 0;
  v_last_id uuid;
  v_last_version text;
begin
  if v_table not in (
    'flows',
    'processes',
    'lifecyclemodels',
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'UNSUPPORTED_DATASET_TABLE',
      'message', format('Unsupported dataset table: %s', coalesce(p_table, '<null>'))
    );
  end if;

  if v_mode not in ('empty', 'stale') then
    return jsonb_build_object(
      'ok', false,
      'code', 'UNSUPPORTED_BACKFILL_MODE',
      'message', format('Unsupported extracted_text backfill mode: %s', coalesce(p_mode, '<null>'))
    );
  end if;

  execute format(
    $sql$
      with page as (
        select id, version
          from public.%1$I
         where json is not null
           and ($4 <> 'empty' or coalesce(extracted_text, '') = '')
           and ($1 is null or (id, version) > ($1, coalesce($2, '')::character(9)))
         order by id, version
         limit $3
         for update skip locked
      ),
      computed as (
        select dataset.id,
               dataset.version,
               util.dataset_json_search_text($5, dataset.json) as next_extracted_text
          from public.%1$I as dataset
          join page on page.id = dataset.id
                   and page.version = dataset.version
      ),
      updated as (
        update public.%1$I as dataset
           set extracted_text = computed.next_extracted_text
          from computed
         where dataset.id = computed.id
           and dataset.version = computed.version
           and (
             $4 = 'empty'
             or dataset.extracted_text is distinct from computed.next_extracted_text
           )
         returning 1
      )
      select
        (select count(*)::integer from page),
        (select count(*)::integer from updated),
        (select id from page order by id desc, version desc limit 1),
        (select version::text from page order by id desc, version desc limit 1)
    $sql$,
    v_table
  )
  using p_after_id, p_after_version, v_batch_size, v_mode, v_table
  into v_scanned_count, v_updated_count, v_last_id, v_last_version;

  return jsonb_build_object(
    'ok', true,
    'table', v_table,
    'mode', v_mode,
    'scanned_count', v_scanned_count,
    'updated_count', v_updated_count,
    'last_id', v_last_id,
    'last_version', v_last_version,
    'has_more', v_scanned_count = v_batch_size
  );
end;
$$;

alter function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text, text) owner to postgres;
revoke all on function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text, text) from public;
revoke all on function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text, text) from anon;
revoke all on function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text, text) from authenticated;
grant execute on function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text, text) to service_role;

comment on function util.dataset_json_search_text(text, jsonb) is
  'Builds deterministic dataset full-text search input from entity-specific core JSON paths, preserving all authored languages while excluding reference/schema metadata noise.';

comment on function util.dataset_json_search_text(jsonb) is
  'Compatibility wrapper for deterministic generic JSON search text extraction; table-aware callers should use util.dataset_json_search_text(text, jsonb).';

comment on function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text, text) is
  'Service-role RPC for bounded historical extracted_text backfill. Default mode repairs empty rows only; mode=stale performs full consistency repair using table-aware core-field extraction.';
