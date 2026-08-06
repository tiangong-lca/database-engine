CREATE OR REPLACE FUNCTION "private"."cmd_review_assert_lifecycle_closure"("p_roots" "jsonb", "p_action" "text", "p_actor" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
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
    return private.cmd_review_lifecycle_error(
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
      return private.cmd_review_lifecycle_error(
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

    v_row := api.cmd_review_get_dataset_row(
      v_current.table_name,
      v_current.dataset_id,
      v_current.dataset_version,
      true
    );

    if v_row is null then
      if v_current.dependency_role in ('ModelComposition', 'Paired') then
        return private.cmd_review_lifecycle_error(
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
          return private.cmd_review_lifecycle_error(
            'MODEL_DEPENDENCY_NOT_PUBLIC',
            v_current.reference_path,
            v_current.table_name,
            v_current.dataset_id,
            v_current.dataset_version
          );
        end if;
      elsif coalesce((v_row->>'state_code')::integer, 0) < 100
        and nullif(v_row->>'user_id', '')::uuid is distinct from v_root_owner then
        return private.cmd_review_lifecycle_error(
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
      from private.cmd_review_reference_roles(
        v_current.table_name,
        'json',
        v_doc
      )
      order by reference_path, ref_table, ref_object_id, ref_version
    loop
      if v_ref.lifecycle_role = 'PolicyGap' then
        return private.cmd_review_lifecycle_error(
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
      elsif v_ref.lifecycle_role = 'ModelComposition'
        and v_current.table_name <> 'lifecyclemodels' then
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
          'ModelComposition',
          v_ref.reference_path
        )
        on conflict do nothing;
      end if;
    end loop;

    for v_ref in
      select *
      from private.cmd_review_reference_roles(
        v_current.table_name,
        'json_tg',
        coalesce(v_row->'json_tg', '{}'::jsonb)
      )
      order by reference_path, ref_table, ref_object_id, ref_version
    loop
      if v_ref.lifecycle_role = 'PolicyGap' then
        return private.cmd_review_lifecycle_error(
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
      elsif v_ref.lifecycle_role = 'ModelComposition'
        and v_current.table_name <> 'lifecyclemodels' then
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
          'ModelComposition',
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
      from private.cmd_review_reference_roles(
        'lifecyclemodels',
        'json',
        v_doc
      ) as role_ref
      where role_ref.lifecycle_role = 'ModelComposition'
      on conflict do nothing;

      v_json_tg_submodels := coalesce(v_row#>'{json_tg,submodels}', '[]'::jsonb);
      if pg_catalog.jsonb_typeof(v_json_tg_submodels) <> 'array' then
        return private.cmd_review_lifecycle_error(
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
          return private.cmd_review_lifecycle_error(
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
        return private.cmd_review_lifecycle_error(
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
        v_dependency := api.cmd_review_get_dataset_row(
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
          return private.cmd_review_lifecycle_error(
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

    if v_current.table_name = 'processes' then
      v_paired := api.cmd_review_get_dataset_row(
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
          and (
            v_current.is_root
            or nullif(v_paired->>'user_id', '')::uuid
              is distinct from v_root_owner
          )
        ) then
          return private.cmd_review_lifecycle_error(
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
      v_paired := api.cmd_review_get_dataset_row(
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
          return private.cmd_review_lifecycle_error(
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
$_$;

ALTER FUNCTION "private"."cmd_review_assert_lifecycle_closure"("p_roots" "jsonb", "p_action" "text", "p_actor" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."cmd_review_assert_lifecycle_closure"("p_roots" "jsonb", "p_action" "text", "p_actor" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."cmd_review_assert_lifecycle_closure"("p_roots" "jsonb", "p_action" "text", "p_actor" "uuid") TO "api_internal_executor";
