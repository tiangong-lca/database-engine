CREATE OR REPLACE FUNCTION "private"."cmd_review_reference_roles"("p_table" "text", "p_source" "text", "p_json" "jsonb") RETURNS TABLE("reference_path" "text", "lifecycle_role" "text", "ref_type" "text", "ref_table" "text", "ref_object_id" "uuid", "ref_version" "text")
    LANGUAGE "sql" STABLE
    SET "search_path" TO ''
    AS $_$
  with recursive walk(value, path) as (
    select coalesce(p_json, '{}'::jsonb), array[]::text[]
    union all
    select child.value, walk.path || child.path_item
    from walk
    cross join lateral (
      select object_item.value, object_item.key as path_item
      from pg_catalog.jsonb_each(
        case
          when pg_catalog.jsonb_typeof(walk.value) = 'object' then walk.value
          else '{}'::jsonb
        end
      ) as object_item(key, value)
      union all
      select array_item.value, (array_item.ordinality - 1)::text as path_item
      from pg_catalog.jsonb_array_elements(
        case
          when pg_catalog.jsonb_typeof(walk.value) = 'array' then walk.value
          else '[]'::jsonb
        end
      ) with ordinality as array_item(value, ordinality)
    ) as child
  ),
  candidates as (
    select
      value,
      path,
      coalesce((
        select pg_catalog.array_agg(path_item.item order by path_item.ordinality)
        from pg_catalog.unnest(path)
          with ordinality as path_item(item, ordinality)
        where not (
          path_item.item ~ '^[0-9]+$'
          and path_item.ordinality > 1
          and path[path_item.ordinality - 1] = any(array[
            'exchange',
            'referenceToFlowDataSet',
            'referencesToDataSource',
            'referenceToDataSource',
            'processInstance',
            'referenceToProcess',
            'referenceToResultingProcess',
            'review',
            'common:referenceToReviewDetails',
            'common:referenceToPrecedingDataSetVersion',
            'referenceToIncludedProcesses'
          ])
        )
      ), array[]::text[]) as semantic_path
    from walk
    where pg_catalog.jsonb_typeof(value) = 'object'
  ),
  refs as (
    select
      candidates.value,
      candidates.path,
      candidates.semantic_path,
      case
        when candidates.semantic_path in (
          array[
            'processDataSet',
            'processInformation',
            'technology',
            'referenceToIncludedProcesses'
          ],
          array[
            'lifeCycleModelDataSet',
            'lifeCycleModelInformation',
            'technology',
            'processes',
            'processInstance',
            'referenceToProcess'
          ]
        ) then 'processes'
        else api.cmd_review_ref_type_to_table(candidates.value->>'@type')
      end as mapped_table,
      (
        candidates.value->>'@refObjectId'
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        and candidates.value->>'@version'
          ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
        and (
          not candidates.value ? '@type'
          or api.cmd_review_ref_type_to_table(
            candidates.value->>'@type'
          ) = 'processes'
        )
      ) as valid_process_composition
    from candidates
    where (
      candidates.value ? '@refObjectId'
      and candidates.value ? '@version'
      and candidates.value ? '@type'
      and candidates.value->>'@refObjectId'
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and nullif(candidates.value->>'@version', '') is not null
      and api.cmd_review_ref_type_to_table(
        candidates.value->>'@type'
      ) is not null
    ) or candidates.semantic_path in (
      array[
        'processDataSet',
        'processInformation',
        'technology',
        'referenceToIncludedProcesses'
      ],
      array[
        'lifeCycleModelDataSet',
        'lifeCycleModelInformation',
        'technology',
        'processes',
        'processInstance',
        'referenceToProcess'
      ]
    )
  )
  select
    coalesce(p_source, 'json') || coalesce((
      select pg_catalog.string_agg(
        case
          when path_item.item ~ '^[0-9]+$' then '[' || path_item.item || ']'
          else '.' || path_item.item
        end,
        ''
        order by path_item.ordinality
      )
      from pg_catalog.unnest(refs.path)
        with ordinality as path_item(item, ordinality)
    ), '') as reference_path,
    case
      when refs.semantic_path = array[
        case lower(coalesce(p_table, ''))
          when 'contacts' then 'contactDataSet'
          when 'sources' then 'sourceDataSet'
          when 'unitgroups' then 'unitGroupDataSet'
          when 'flowproperties' then 'flowPropertyDataSet'
          when 'flows' then 'flowDataSet'
          when 'processes' then 'processDataSet'
          when 'lifecyclemodels' then 'lifeCycleModelDataSet'
          else null
        end,
        'administrativeInformation',
        'publicationAndOwnership',
        'common:referenceToPrecedingDataSetVersion'
      ]
        then 'Lineage'
      when lower(coalesce(p_table, '')) = 'processes'
        and refs.semantic_path = array[
          'processDataSet',
          'exchanges',
          'exchange',
          'referenceToFlowDataSet'
        ]
        then 'RequiredSupport'
      when lower(coalesce(p_table, '')) = 'processes'
        and refs.semantic_path = array[
          'processDataSet',
          'exchanges',
          'exchange',
          'referencesToDataSource',
          'referenceToDataSource'
        ]
        then 'RequiredSupport'
      when lower(coalesce(p_table, '')) = 'processes'
        and refs.semantic_path = array[
          'processDataSet',
          'processInformation',
          'quantitativeReference',
          'referenceToReferenceFlow'
        ]
        then 'RequiredSupport'
      when lower(coalesce(p_table, '')) = 'processes'
        and refs.semantic_path = array[
          'processDataSet',
          'processInformation',
          'technology',
          'referenceToIncludedProcesses'
        ]
        then case
          when refs.valid_process_composition then 'ModelComposition'
          else 'PolicyGap'
        end
      when lower(coalesce(p_table, '')) = 'lifecyclemodels'
        and refs.semantic_path = array[
          'lifeCycleModelDataSet',
          'lifeCycleModelInformation',
          'technology',
          'processes',
          'processInstance',
          'referenceToProcess'
        ]
        then case
          when refs.valid_process_composition then 'ModelComposition'
          else 'PolicyGap'
        end
      when lower(coalesce(p_table, '')) = 'lifecyclemodels'
        and refs.semantic_path = array[
          'lifeCycleModelDataSet',
          'lifeCycleModelInformation',
          'dataSetInformation',
          'referenceToResultingProcess'
        ]
        then 'Descriptive'
      when lower(coalesce(p_table, '')) = 'comments'
        and refs.semantic_path = array[
          'modellingAndValidation',
          'validation',
          'review',
          'common:referenceToReviewDetails'
        ]
        then 'RequiredSupport'
      when refs.mapped_table in ('processes', 'flows')
        then 'PolicyGap'
      else 'RequiredSupport'
    end as lifecycle_role,
    case
      when not refs.value ? '@type'
        and refs.mapped_table = 'processes'
        then 'process data set'
      else lower(trim(refs.value->>'@type'))
    end as ref_type,
    refs.mapped_table as ref_table,
    case
      when refs.value->>'@refObjectId'
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then (refs.value->>'@refObjectId')::uuid
      else null
    end as ref_object_id,
    refs.value->>'@version' as ref_version
  from refs
$_$;

ALTER FUNCTION "private"."cmd_review_reference_roles"("p_table" "text", "p_source" "text", "p_json" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."cmd_review_reference_roles"("p_table" "text", "p_source" "text", "p_json" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."cmd_review_reference_roles"("p_table" "text", "p_source" "text", "p_json" "jsonb") TO "api_internal_executor";
