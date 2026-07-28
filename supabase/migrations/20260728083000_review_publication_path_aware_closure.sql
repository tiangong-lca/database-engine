create or replace function public.cmd_review_reference_roles(
  p_table text,
  p_source text,
  p_json jsonb
)
returns table (
  reference_path text,
  lifecycle_role text,
  ref_type text,
  ref_table text,
  ref_object_id uuid,
  ref_version text
)
language sql
stable
set search_path = ''
as $$
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
  refs as (
    select
      value,
      path,
      public.cmd_review_ref_type_to_table(value->>'@type') as mapped_table
    from walk
    where pg_catalog.jsonb_typeof(value) = 'object'
      and value ? '@refObjectId'
      and value ? '@version'
      and value ? '@type'
      and (value->>'@refObjectId')
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and nullif(value->>'@version', '') is not null
      and public.cmd_review_ref_type_to_table(value->>'@type') is not null
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
      when refs.path[array_length(refs.path, 1)]
        = 'referenceToPrecedingDataSetVersion'
        then 'Lineage'
      when lower(coalesce(p_table, '')) = 'processes'
        and array_length(refs.path, 1) = 5
        and refs.path[1:3] = array['processDataSet', 'exchanges', 'exchange']
        and refs.path[4] ~ '^[0-9]+$'
        and refs.path[5] = 'referenceToFlowDataSet'
        then 'RequiredSupport'
      when lower(coalesce(p_table, '')) = 'processes'
        and array_length(refs.path, 1) = 6
        and refs.path[1:3] = array['processDataSet', 'exchanges', 'exchange']
        and refs.path[4] ~ '^[0-9]+$'
        and refs.path[5:6]
          = array['referencesToDataSource', 'referenceToDataSource']
        then 'RequiredSupport'
      when lower(coalesce(p_table, '')) = 'processes'
        and refs.path = array[
          'processDataSet',
          'processInformation',
          'quantitativeReference',
          'referenceToReferenceFlow'
        ]
        then 'RequiredSupport'
      when lower(coalesce(p_table, '')) = 'lifecyclemodels'
        and array_length(refs.path, 1) = 7
        and refs.path[1:5] = array[
          'lifeCycleModelDataSet',
          'lifeCycleModelInformation',
          'technology',
          'processes',
          'processInstance'
        ]
        and refs.path[6] ~ '^[0-9]+$'
        and refs.path[7] = 'referenceToProcess'
        then 'ModelComposition'
      when lower(coalesce(p_table, '')) = 'lifecyclemodels'
        and refs.path = array[
          'lifeCycleModelDataSet',
          'lifeCycleModelInformation',
          'dataSetInformation',
          'referenceToResultingProcess'
        ]
        then 'Descriptive'
      when lower(coalesce(p_table, '')) = 'comments'
        and array_length(refs.path, 1) = 5
        and refs.path[1:3]
          = array['modellingAndValidation', 'validation', 'review']
        and refs.path[4] ~ '^[0-9]+$'
        and refs.path[5] = 'common:referenceToReviewDetails'
        then 'RequiredSupport'
      when refs.mapped_table in ('processes', 'flows')
        then 'PolicyGap'
      else 'RequiredSupport'
    end as lifecycle_role,
    lower(trim(refs.value->>'@type')) as ref_type,
    refs.mapped_table as ref_table,
    (refs.value->>'@refObjectId')::uuid as ref_object_id,
    refs.value->>'@version' as ref_version
  from refs
$$;

create or replace function public.cmd_review_lifecycle_error(
  p_code text,
  p_path text,
  p_ref_table text default null,
  p_ref_id uuid default null,
  p_ref_version text default null
)
returns jsonb
language sql
immutable
set search_path = ''
as $$
  select jsonb_build_object(
    'ok', false,
    'code', p_code,
    'status', 409,
    'message', case p_code
      when 'MODEL_DEPENDENCY_NOT_PUBLIC'
        then 'Model dependency is not public'
      when 'MODEL_COMPOSITION_POLICY_GAP'
        then 'Model composition sources do not agree'
      else 'Reference role is not configured'
    end,
    'details', jsonb_build_object(
      'path', p_path,
      'reference', jsonb_strip_nulls(jsonb_build_object(
        'table', p_ref_table,
        'id', p_ref_id,
        'version', p_ref_version
      ))
    )
  )
$$;

create or replace function public.cmd_review_assert_lifecycle_closure(
  p_roots jsonb,
  p_action text,
  p_actor uuid
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_root jsonb;
  v_current record;
  v_row jsonb;
  v_doc jsonb;
  v_root_owner uuid;
  v_ref record;
  v_submodel record;
  v_dependency jsonb;
  v_paired jsonb;
  v_has_mismatch boolean;
  v_json_tg_submodels jsonb;
  v_publish_root_table text;
  v_publish_root_id uuid;
  v_publish_root_version text;
begin
  create temporary table if not exists pg_temp.cmd_review_policy_queue (
    table_name text not null,
    dataset_id uuid not null,
    dataset_version text not null,
    is_root boolean not null default false,
    dependency_role text not null,
    reference_path text not null,
    primary key (table_name, dataset_id, dataset_version)
  ) on commit drop;

  create temporary table if not exists pg_temp.cmd_review_policy_seen (
    table_name text not null,
    dataset_id uuid not null,
    dataset_version text not null,
    primary key (table_name, dataset_id, dataset_version)
  ) on commit drop;

  create temporary table if not exists pg_temp.cmd_review_policy_ilcd (
    dataset_id uuid not null,
    dataset_version text not null,
    reference_path text not null,
    primary key (dataset_id, dataset_version)
  ) on commit drop;

  create temporary table if not exists pg_temp.cmd_review_policy_tg (
    dataset_id uuid not null,
    dataset_version text not null,
    reference_path text not null,
    primary key (dataset_id, dataset_version)
  ) on commit drop;

  truncate table pg_temp.cmd_review_policy_queue;
  truncate table pg_temp.cmd_review_policy_seen;
  truncate table pg_temp.cmd_review_policy_ilcd;
  truncate table pg_temp.cmd_review_policy_tg;

  if p_actor is null
    or pg_catalog.jsonb_typeof(p_roots) <> 'array'
    or p_action not in ('submit', 'approve', 'publish') then
    return public.cmd_review_lifecycle_error(
      'REFERENCE_ROLE_POLICY_GAP',
      'request'
    );
  end if;

  for v_root in
    select root_item.value
    from pg_catalog.jsonb_array_elements(p_roots) as root_item(value)
  loop
    if lower(coalesce(v_root->>'table', ''))
      not in ('processes', 'lifecyclemodels')
      or coalesce(v_root->>'id', '')
        !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or nullif(v_root->>'version', '') is null then
      return public.cmd_review_lifecycle_error(
        'REFERENCE_ROLE_POLICY_GAP',
        'request.root'
      );
    end if;

    insert into pg_temp.cmd_review_policy_queue (
      table_name,
      dataset_id,
      dataset_version,
      is_root,
      dependency_role,
      reference_path
    )
    values (
      lower(v_root->>'table'),
      (v_root->>'id')::uuid,
      v_root->>'version',
      true,
      'Root',
      'request.root'
    )
    on conflict do nothing;

    if v_publish_root_table is null then
      v_publish_root_table := lower(v_root->>'table');
      v_publish_root_id := (v_root->>'id')::uuid;
      v_publish_root_version := v_root->>'version';
    end if;
  end loop;

  while exists (select 1 from pg_temp.cmd_review_policy_queue) loop
    select
      queue.table_name,
      queue.dataset_id,
      queue.dataset_version,
      queue.is_root,
      queue.dependency_role,
      queue.reference_path
    into v_current
    from pg_temp.cmd_review_policy_queue as queue
    order by
      queue.is_root desc,
      queue.table_name,
      queue.dataset_id,
      queue.dataset_version
    limit 1;

    delete from pg_temp.cmd_review_policy_queue as queue
    where queue.table_name = v_current.table_name
      and queue.dataset_id = v_current.dataset_id
      and queue.dataset_version = v_current.dataset_version;

    if exists (
      select 1
      from pg_temp.cmd_review_policy_seen as seen
      where seen.table_name = v_current.table_name
        and seen.dataset_id = v_current.dataset_id
        and seen.dataset_version = v_current.dataset_version
    ) then
      continue;
    end if;

    v_row := public.cmd_review_get_dataset_row(
      v_current.table_name,
      v_current.dataset_id,
      v_current.dataset_version,
      true
    );

    if v_row is null then
      if v_current.dependency_role in ('ModelComposition', 'Paired') then
        return public.cmd_review_lifecycle_error(
          'MODEL_DEPENDENCY_NOT_PUBLIC',
          v_current.reference_path,
          v_current.table_name,
          v_current.dataset_id,
          v_current.dataset_version
        );
      end if;
      continue;
    end if;

    if v_current.is_root then
      v_root_owner := nullif(v_row->>'user_id', '')::uuid;
    elsif v_current.dependency_role in ('ModelComposition', 'Paired') then
      if p_action = 'publish' then
        if coalesce((v_row->>'state_code')::integer, 0) < 100
          and not (
            v_current.table_name = v_publish_root_table
            and v_current.dataset_id = v_publish_root_id
            and v_current.dataset_version = v_publish_root_version
          ) then
          return public.cmd_review_lifecycle_error(
            'MODEL_DEPENDENCY_NOT_PUBLIC',
            v_current.reference_path,
            v_current.table_name,
            v_current.dataset_id,
            v_current.dataset_version
          );
        end if;
      elsif coalesce((v_row->>'state_code')::integer, 0) < 100
        and nullif(v_row->>'user_id', '')::uuid is distinct from v_root_owner then
        return public.cmd_review_lifecycle_error(
          'MODEL_DEPENDENCY_NOT_PUBLIC',
          v_current.reference_path,
          v_current.table_name,
          v_current.dataset_id,
          v_current.dataset_version
        );
      end if;
    end if;

    insert into pg_temp.cmd_review_policy_seen (
      table_name,
      dataset_id,
      dataset_version
    )
    values (
      v_current.table_name,
      v_current.dataset_id,
      v_current.dataset_version
    );

    v_doc := case
      when coalesce(v_row->'json_ordered', 'null'::jsonb)
        not in ('null'::jsonb, '{}'::jsonb)
        then v_row->'json_ordered'
      else coalesce(v_row->'json', '{}'::jsonb)
    end;

    for v_ref in
      select *
      from public.cmd_review_reference_roles(
        v_current.table_name,
        'json',
        v_doc
      )
      order by reference_path, ref_table, ref_object_id, ref_version
    loop
      if v_ref.lifecycle_role = 'PolicyGap' then
        return public.cmd_review_lifecycle_error(
          'REFERENCE_ROLE_POLICY_GAP',
          v_ref.reference_path,
          v_ref.ref_table,
          v_ref.ref_object_id,
          v_ref.ref_version
        );
      elsif v_ref.lifecycle_role = 'RequiredSupport'
        and p_action in ('submit', 'approve') then
        insert into pg_temp.cmd_review_policy_queue (
          table_name,
          dataset_id,
          dataset_version,
          is_root,
          dependency_role,
          reference_path
        )
        values (
          v_ref.ref_table,
          v_ref.ref_object_id,
          v_ref.ref_version,
          false,
          'RequiredSupport',
          v_ref.reference_path
        )
        on conflict do nothing;
      end if;
    end loop;

    for v_ref in
      select *
      from public.cmd_review_reference_roles(
        v_current.table_name,
        'json_tg',
        coalesce(v_row->'json_tg', '{}'::jsonb)
      )
      order by reference_path, ref_table, ref_object_id, ref_version
    loop
      if v_ref.lifecycle_role = 'PolicyGap' then
        return public.cmd_review_lifecycle_error(
          'REFERENCE_ROLE_POLICY_GAP',
          v_ref.reference_path,
          v_ref.ref_table,
          v_ref.ref_object_id,
          v_ref.ref_version
        );
      elsif v_ref.lifecycle_role = 'RequiredSupport'
        and p_action in ('submit', 'approve') then
        insert into pg_temp.cmd_review_policy_queue (
          table_name,
          dataset_id,
          dataset_version,
          is_root,
          dependency_role,
          reference_path
        )
        values (
          v_ref.ref_table,
          v_ref.ref_object_id,
          v_ref.ref_version,
          false,
          'RequiredSupport',
          v_ref.reference_path
        )
        on conflict do nothing;
      end if;
    end loop;

    if v_current.table_name = 'lifecyclemodels' then
      truncate table pg_temp.cmd_review_policy_ilcd;
      truncate table pg_temp.cmd_review_policy_tg;

      insert into pg_temp.cmd_review_policy_ilcd (
        dataset_id,
        dataset_version,
        reference_path
      )
      select
        role_ref.ref_object_id,
        role_ref.ref_version,
        role_ref.reference_path
      from public.cmd_review_reference_roles(
        'lifecyclemodels',
        'json',
        v_doc
      ) as role_ref
      where role_ref.lifecycle_role = 'ModelComposition'
      on conflict do nothing;

      v_json_tg_submodels := coalesce(v_row#>'{json_tg,submodels}', '[]'::jsonb);
      if pg_catalog.jsonb_typeof(v_json_tg_submodels) <> 'array' then
        return public.cmd_review_lifecycle_error(
          'MODEL_COMPOSITION_POLICY_GAP',
          'json_tg.submodels'
        );
      end if;

      for v_submodel in
        select
          submodel.value,
          submodel.ordinality - 1 as item_index
        from pg_catalog.jsonb_array_elements(v_json_tg_submodels)
          with ordinality as submodel(value, ordinality)
        where lower(coalesce(submodel.value->>'type', '')) = 'secondary'
        order by submodel.ordinality
      loop
        if coalesce(v_submodel.value->>'id', '')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then
          return public.cmd_review_lifecycle_error(
            'MODEL_COMPOSITION_POLICY_GAP',
            'json_tg.submodels[' || v_submodel.item_index || ']'
          );
        end if;

        insert into pg_temp.cmd_review_policy_tg (
          dataset_id,
          dataset_version,
          reference_path
        )
        values (
          (v_submodel.value->>'id')::uuid,
          coalesce(
            nullif(v_submodel.value->>'version', ''),
            v_current.dataset_version
          ),
          'json_tg.submodels[' || v_submodel.item_index || ']'
        )
        on conflict do nothing;
      end loop;

      select exists (
        (
          select ilcd.dataset_id, ilcd.dataset_version
          from pg_temp.cmd_review_policy_ilcd as ilcd
          except
          select tg.dataset_id, tg.dataset_version
          from pg_temp.cmd_review_policy_tg as tg
        )
        union all
        (
          select tg.dataset_id, tg.dataset_version
          from pg_temp.cmd_review_policy_tg as tg
          except
          select ilcd.dataset_id, ilcd.dataset_version
          from pg_temp.cmd_review_policy_ilcd as ilcd
        )
      )
      into v_has_mismatch;

      if v_has_mismatch then
        return public.cmd_review_lifecycle_error(
          'MODEL_COMPOSITION_POLICY_GAP',
          'json_tg.submodels'
        );
      end if;

      for v_submodel in
        select
          tg.dataset_id,
          tg.dataset_version,
          tg.reference_path
        from pg_temp.cmd_review_policy_tg as tg
        order by tg.dataset_id, tg.dataset_version
      loop
        v_dependency := public.cmd_review_get_dataset_row(
          'processes',
          v_submodel.dataset_id,
          v_submodel.dataset_version,
          true
        );

        if v_dependency is null
          or (
            p_action = 'publish'
            and coalesce((v_dependency->>'state_code')::integer, 0) < 100
            and not (
              v_publish_root_table = 'processes'
              and v_submodel.dataset_id = v_publish_root_id
              and v_submodel.dataset_version = v_publish_root_version
            )
          )
          or (
            p_action in ('submit', 'approve')
            and coalesce((v_dependency->>'state_code')::integer, 0) < 100
            and nullif(v_dependency->>'user_id', '')::uuid
              is distinct from v_root_owner
          ) then
          return public.cmd_review_lifecycle_error(
            'MODEL_DEPENDENCY_NOT_PUBLIC',
            v_submodel.reference_path,
            'processes',
            v_submodel.dataset_id,
            v_submodel.dataset_version
          );
        end if;

        if p_action in ('submit', 'approve') then
          insert into pg_temp.cmd_review_policy_queue (
            table_name,
            dataset_id,
            dataset_version,
            is_root,
            dependency_role,
            reference_path
          )
          values (
            'processes',
            v_submodel.dataset_id,
            v_submodel.dataset_version,
            false,
            'ModelComposition',
            v_submodel.reference_path
          )
          on conflict do nothing;
        end if;
      end loop;
    end if;

    if v_current.table_name = 'processes'
      and (
        p_action = 'publish'
        or (p_action in ('submit', 'approve') and not v_current.is_root)
      ) then
      v_paired := public.cmd_review_get_dataset_row(
        'lifecyclemodels',
        v_current.dataset_id,
        v_current.dataset_version,
        true
      );

      if v_paired is not null then
        if (
          p_action = 'publish'
          and coalesce((v_paired->>'state_code')::integer, 0) < 100
        ) or (
          p_action in ('submit', 'approve')
          and coalesce((v_paired->>'state_code')::integer, 0) < 100
          and nullif(v_paired->>'user_id', '')::uuid is distinct from v_root_owner
        ) then
          return public.cmd_review_lifecycle_error(
            'MODEL_DEPENDENCY_NOT_PUBLIC',
            'pairedLifecycleModel',
            'lifecyclemodels',
            v_current.dataset_id,
            v_current.dataset_version
          );
        end if;

        insert into pg_temp.cmd_review_policy_queue (
          table_name,
          dataset_id,
          dataset_version,
          is_root,
          dependency_role,
          reference_path
        )
        values (
          'lifecyclemodels',
          v_current.dataset_id,
          v_current.dataset_version,
          false,
          'Paired',
          'pairedLifecycleModel'
        )
        on conflict do nothing;
      end if;
    elsif v_current.table_name = 'lifecyclemodels'
      and v_current.is_root then
      v_paired := public.cmd_review_get_dataset_row(
        'processes',
        v_current.dataset_id,
        v_current.dataset_version,
        true
      );

      if v_paired is not null then
        if (
          p_action = 'publish'
          and coalesce((v_paired->>'state_code')::integer, 0) < 100
        ) or (
          p_action in ('submit', 'approve')
          and coalesce((v_paired->>'state_code')::integer, 0) < 100
          and nullif(v_paired->>'user_id', '')::uuid is distinct from v_root_owner
        ) then
          return public.cmd_review_lifecycle_error(
            'MODEL_DEPENDENCY_NOT_PUBLIC',
            'pairedProcess',
            'processes',
            v_current.dataset_id,
            v_current.dataset_version
          );
        end if;

        if p_action in ('submit', 'approve') then
          insert into pg_temp.cmd_review_policy_queue (
            table_name,
            dataset_id,
            dataset_version,
            is_root,
            dependency_role,
            reference_path
          )
          values (
            'processes',
            v_current.dataset_id,
            v_current.dataset_version,
            false,
            'Paired',
            'pairedProcess'
          )
          on conflict do nothing;
        end if;
      end if;
    end if;
  end loop;

  truncate table pg_temp.cmd_review_policy_queue;
  truncate table pg_temp.cmd_review_policy_seen;
  truncate table pg_temp.cmd_review_policy_ilcd;
  truncate table pg_temp.cmd_review_policy_tg;
  return null;
end;
$$;

create or replace function public.cmd_review_extract_refs(p_json jsonb)
returns table (
  ref_type text,
  ref_object_id uuid,
  ref_version text
)
language sql
stable
set search_path = ''
as $$
  select distinct
    role_ref.ref_type,
    role_ref.ref_object_id,
    role_ref.ref_version
  from public.cmd_review_reference_roles(
    case
      when coalesce(p_json, '{}'::jsonb) ? 'processDataSet' then 'processes'
      when coalesce(p_json, '{}'::jsonb) ? 'lifeCycleModelDataSet'
        then 'lifecyclemodels'
      when coalesce(p_json, '{}'::jsonb) ? 'flowDataSet' then 'flows'
      when coalesce(p_json, '{}'::jsonb) ? 'flowPropertyDataSet'
        then 'flowproperties'
      when coalesce(p_json, '{}'::jsonb) ? 'unitGroupDataSet'
        then 'unitgroups'
      when coalesce(p_json, '{}'::jsonb) ? 'sourceDataSet' then 'sources'
      when coalesce(p_json, '{}'::jsonb) ? 'contactDataSet' then 'contacts'
      else 'comments'
    end,
    'json',
    p_json
  ) as role_ref
  where role_ref.lifecycle_role in ('RequiredSupport', 'ModelComposition')
$$;

alter function public.cmd_review_submit_without_gate(text, uuid, text, jsonb)
  rename to cmd_review_submit_without_gate_issue304_legacy;

revoke all on function
  public.cmd_review_submit_without_gate_issue304_legacy(text, uuid, text, jsonb)
  from public, anon, authenticated, service_role;

create function public.cmd_review_submit_without_gate(
  p_table text,
  p_id uuid,
  p_version text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_root jsonb;
  v_assertion jsonb;
begin
  if v_actor is null
    or p_table not in ('processes', 'lifecyclemodels') then
    return public.cmd_review_submit_without_gate_issue304_legacy(
      p_table,
      p_id,
      p_version,
      p_audit
    );
  end if;

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended('review-publication-lifecycle-closure', 0)
  );

  v_root := public.cmd_review_get_dataset_row(
    p_table,
    p_id,
    p_version,
    true
  );

  if v_root is null
    or nullif(v_root->>'user_id', '')::uuid is distinct from v_actor
    or coalesce((v_root->>'state_code')::integer, 0) >= 20 then
    return public.cmd_review_submit_without_gate_issue304_legacy(
      p_table,
      p_id,
      p_version,
      p_audit
    );
  end if;

  v_assertion := public.cmd_review_assert_lifecycle_closure(
    jsonb_build_array(jsonb_build_object(
      'table', p_table,
      'id', p_id,
      'version', p_version
    )),
    'submit',
    v_actor
  );

  if v_assertion is not null then
    return v_assertion;
  end if;

  return public.cmd_review_submit_without_gate_issue304_legacy(
    p_table,
    p_id,
    p_version,
    p_audit
  );
end;
$$;

alter function public.cmd_review_approve(text, uuid, jsonb)
  rename to cmd_review_approve_issue304_legacy;

revoke all on function
  public.cmd_review_approve_issue304_legacy(text, uuid, jsonb)
  from public, anon, authenticated, service_role;

create function public.cmd_review_approve(
  p_table text,
  p_review_id uuid,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_root jsonb;
  v_assertion jsonb;
  v_comment_ref record;
begin
  if v_actor is null
    or not public.cmd_review_is_review_admin(v_actor)
    or lower(coalesce(p_table, '')) not in ('processes', 'lifecyclemodels') then
    return public.cmd_review_approve_issue304_legacy(
      p_table,
      p_review_id,
      p_audit
    );
  end if;

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended('review-publication-lifecycle-closure', 0)
  );

  select review_row.*
  into v_review
  from public.reviews as review_row
  where review_row.id = p_review_id
  for update;

  if not found or v_review.state_code <> 1 then
    return public.cmd_review_approve_issue304_legacy(
      p_table,
      p_review_id,
      p_audit
    );
  end if;

  v_root := public.cmd_review_get_dataset_row(
    lower(p_table),
    v_review.data_id,
    v_review.data_version,
    true
  );

  if v_root is null then
    return public.cmd_review_approve_issue304_legacy(
      p_table,
      p_review_id,
      p_audit
    );
  end if;

  perform 1
  from public.comments as comment_row
  where comment_row.review_id = p_review_id
  order by comment_row.reviewer_id, comment_row.created_at
  for update;

  for v_comment_ref in
    select role_ref.*
    from public.comments as comment_row
    cross join lateral public.cmd_review_reference_roles(
      'comments',
      'comment',
      coalesce(to_jsonb(comment_row.json), '{}'::jsonb)
    ) as role_ref
    where comment_row.review_id = p_review_id
      and comment_row.state_code = 1
    order by
      role_ref.reference_path,
      role_ref.ref_table,
      role_ref.ref_object_id,
      role_ref.ref_version
  loop
    if v_comment_ref.lifecycle_role = 'PolicyGap' then
      return public.cmd_review_lifecycle_error(
        'REFERENCE_ROLE_POLICY_GAP',
        v_comment_ref.reference_path,
        v_comment_ref.ref_table,
        v_comment_ref.ref_object_id,
        v_comment_ref.ref_version
      );
    end if;
  end loop;

  v_assertion := public.cmd_review_assert_lifecycle_closure(
    jsonb_build_array(jsonb_build_object(
      'table', lower(p_table),
      'id', v_review.data_id,
      'version', v_review.data_version
    )),
    'approve',
    v_actor
  );

  if v_assertion is not null then
    return v_assertion;
  end if;

  return public.cmd_review_approve_issue304_legacy(
    p_table,
    p_review_id,
    p_audit
  );
end;
$$;

alter function public.cmd_dataset_publish(text, uuid, text, jsonb)
  rename to cmd_dataset_publish_issue304_legacy;

revoke all on function
  public.cmd_dataset_publish_issue304_legacy(text, uuid, text, jsonb)
  from public, anon, authenticated, service_role;

create function public.cmd_dataset_publish(
  p_table text,
  p_id uuid,
  p_version text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_root jsonb;
  v_assertion jsonb;
begin
  if v_actor is null
    or p_table not in ('processes', 'lifecyclemodels') then
    return public.cmd_dataset_publish_issue304_legacy(
      p_table,
      p_id,
      p_version,
      p_audit
    );
  end if;

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended('review-publication-lifecycle-closure', 0)
  );

  v_root := public.cmd_review_get_dataset_row(
    p_table,
    p_id,
    p_version,
    true
  );

  if v_root is null
    or nullif(v_root->>'user_id', '')::uuid is distinct from v_actor
    or coalesce((v_root->>'state_code')::integer, 0) >= 100 then
    return public.cmd_dataset_publish_issue304_legacy(
      p_table,
      p_id,
      p_version,
      p_audit
    );
  end if;

  if p_table = 'processes' then
    lock table public.lifecyclemodels in share row exclusive mode;
  end if;

  v_assertion := public.cmd_review_assert_lifecycle_closure(
    jsonb_build_array(jsonb_build_object(
      'table', p_table,
      'id', p_id,
      'version', p_version
    )),
    'publish',
    v_actor
  );

  if v_assertion is not null then
    return v_assertion;
  end if;

  return public.cmd_dataset_publish_issue304_legacy(
    p_table,
    p_id,
    p_version,
    p_audit
  );
end;
$$;

alter function public.cmd_review_reference_roles(text, text, jsonb)
  owner to postgres;
alter function public.cmd_review_lifecycle_error(text, text, text, uuid, text)
  owner to postgres;
alter function public.cmd_review_assert_lifecycle_closure(jsonb, text, uuid)
  owner to postgres;
alter function public.cmd_review_extract_refs(jsonb)
  owner to postgres;
alter function public.cmd_review_submit_without_gate(text, uuid, text, jsonb)
  owner to postgres;
alter function public.cmd_review_approve(text, uuid, jsonb)
  owner to postgres;
alter function public.cmd_dataset_publish(text, uuid, text, jsonb)
  owner to postgres;

revoke all on function public.cmd_review_reference_roles(text, text, jsonb)
  from public, anon, authenticated, service_role;
revoke all on function
  public.cmd_review_lifecycle_error(text, text, text, uuid, text)
  from public, anon, authenticated, service_role;
revoke all on function
  public.cmd_review_assert_lifecycle_closure(jsonb, text, uuid)
  from public, anon, authenticated, service_role;

revoke all on function public.cmd_review_extract_refs(jsonb) from public;
grant execute on function public.cmd_review_extract_refs(jsonb)
  to anon, authenticated, service_role;

revoke all on function
  public.cmd_review_submit_without_gate(text, uuid, text, jsonb)
  from public, authenticated, service_role;
grant execute on function
  public.cmd_review_submit_without_gate(text, uuid, text, jsonb)
  to anon;

revoke all on function public.cmd_review_approve(text, uuid, jsonb)
  from public;
grant execute on function public.cmd_review_approve(text, uuid, jsonb)
  to anon, authenticated, service_role;

revoke all on function public.cmd_dataset_publish(text, uuid, text, jsonb)
  from public;
grant execute on function public.cmd_dataset_publish(text, uuid, text, jsonb)
  to anon, authenticated, service_role;
