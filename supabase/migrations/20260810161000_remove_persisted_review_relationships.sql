-- Issue #446: stop persisting Root Review -> Reference Review relationships.
-- This migration deliberately retains business-table reviews[].id entries as
-- append-only Root Review candidate hints. Every consumer must revalidate them
-- against current dataset JSON and non-revoked Reviewer comments.

create function private.review_migration_replace_between_v1(
  p_definition text,
  p_start_marker text,
  p_end_marker text,
  p_replacement text
)
returns text
language plpgsql
set search_path = ''
as $$
declare
  v_start integer;
  v_tail_start integer;
  v_end integer;
begin
  v_start := pg_catalog.strpos(p_definition, p_start_marker);
  if v_start = 0 then
    raise exception 'migration start marker not found: %', p_start_marker;
  end if;

  v_tail_start := v_start + pg_catalog.length(p_start_marker);
  v_end := pg_catalog.strpos(
    pg_catalog.substr(p_definition, v_tail_start),
    p_end_marker
  );
  if v_end = 0 then
    raise exception 'migration end marker not found: %', p_end_marker;
  end if;

  v_end := v_tail_start + v_end - 1;
  return pg_catalog.substr(p_definition, 1, v_start - 1)
    || p_replacement
    || pg_catalog.substr(p_definition, v_end);
end;
$$;

create or replace function private.review_root_currently_references_target_v1(
  p_root_review_id uuid,
  p_target_table text,
  p_target_id uuid,
  p_target_version text
)
returns boolean
language plpgsql
security definer
set search_path = 'pg_temp'
as $$
declare
  v_root private.reviews%rowtype;
  v_comment_roots jsonb;
begin
  select review_row.*
  into v_root
  from private.reviews as review_row
  where review_row.id = p_root_review_id
    and review_row.review_kind = 'root';

  if not found then
    return false;
  end if;

  if exists (
    select 1
    from api.cmd_review_collect_dataset_targets(
      pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(
        'table', v_root.target_table,
        'id', v_root.data_id,
        'version', pg_catalog.btrim(v_root.data_version::text),
        'is_root', true
      )),
      false
    ) as target
    where not target.is_root
      and target.table_name = p_target_table
      and target.dataset_id = p_target_id
      and target.dataset_version = pg_catalog.btrim(p_target_version)
  ) then
    return true;
  end if;

  select coalesce(
    pg_catalog.jsonb_agg(distinct pg_catalog.jsonb_build_object(
      'table', api.cmd_review_ref_type_to_table(ref.ref_type),
      'id', ref.ref_object_id,
      'version', ref.ref_version,
      'is_root', false
    )),
    '[]'::jsonb
  )
  into v_comment_roots
  from private.comments as comment_row
  cross join lateral api.cmd_review_extract_refs(
    coalesce(comment_row.json::jsonb, '{}'::jsonb)
  ) as ref
  where comment_row.review_id = p_root_review_id
    and comment_row.state_code <> -2
    and api.cmd_review_ref_type_to_table(ref.ref_type) is not null;

  return exists (
    select 1
    from api.cmd_review_collect_dataset_targets(v_comment_roots, false) as target
    where target.table_name = p_target_table
      and target.dataset_id = p_target_id
      and target.dataset_version = pg_catalog.btrim(p_target_version)
  );
end;
$$;

revoke all on function private.review_root_currently_references_target_v1(uuid, text, uuid, text)
  from public, anon, authenticated, service_role;
grant execute on function private.review_root_currently_references_target_v1(uuid, text, uuid, text)
  to api_internal_executor;

do $$
declare
  v_definition text;
begin
  select pg_catalog.pg_get_functiondef(
    'api.cmd_review_submit_v2(text,uuid,text,jsonb,jsonb)'::pg_catalog.regprocedure
  ) into v_definition;

  v_definition := pg_catalog.replace(v_definition, '  v_item jsonb;' || chr(10), '');
  v_definition := pg_catalog.replace(v_definition, '  v_items jsonb := ''[]''::jsonb;' || chr(10), '');
  v_definition := pg_catalog.replace(v_definition, '  v_scope_history jsonb;' || chr(10), '');
  v_definition := pg_catalog.replace(v_definition, '  v_reference_ids uuid[] := array[]::uuid[];' || chr(10), '');
  v_definition := pg_catalog.replace(v_definition, '  v_old_reference_ids uuid[] := array[]::uuid[];' || chr(10), '');
  v_definition := pg_catalog.replace(v_definition, '  v_impacted_root private.reviews%rowtype;' || chr(10), '');
  v_definition := pg_catalog.replace(v_definition, '  v_current_snapshot jsonb;' || chr(10), '');
  v_definition := pg_catalog.replace(v_definition, '  v_repair_items jsonb;' || chr(10), '');

  v_definition := private.review_migration_replace_between_v1(
    v_definition,
    '  select coalesce(array_agg(rejected.id order by rejected.id), array[]::uuid[])',
    '  if exists (' || chr(10) || '    select 1' || chr(10) || '    from private.reviews as active_root',
    $replacement$  if exists (
    select 1
    from private.review_candidate_root_ids_v1(
      v_table,
      p_target_id,
      p_target_version
    ) as candidate
    where private.review_root_currently_references_target_v1(
      candidate.root_review_id,
      v_table,
      p_target_id,
      p_target_version
    )
  ) then
    v_reference := private.review_get_or_create_reference_v1(
      v_table,
      v_root_row,
      v_checksum,
      v_actor
    );

    perform set_config('app.review_controlled_write', 'on', true);
    execute format(
      'update public.%I
          set state_code = 20,
              modified_at = now()
        where id = $1
          and version = $2
          and state_code = 0',
      v_table
    ) using p_target_id, p_target_version;
    perform set_config('app.review_controlled_write', 'off', true);

    insert into private.command_audit_log (
      command, actor_user_id, target_table, target_id, target_version, payload
    ) values (
      'cmd_review_submit_v2', v_actor, v_table, p_target_id, p_target_version,
      coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
        'review_id', v_reference.id,
        'review_kind', 'reference',
        'submission_mode', 'reference_repair'
      )
    );

    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'reviewId', v_reference.id,
        'reviewKind', 'reference',
        'submissionMode', 'reference_repair'
      )
    );
  end if;

$replacement$
  );

  v_definition := private.review_migration_replace_between_v1(
    v_definition,
    '    if v_target.is_root then' || chr(10) || '      v_item := jsonb_build_object(',
    '  end loop;' || chr(10),
    $replacement$    if not v_target.is_root then
      v_reference := private.review_get_or_create_reference_v1(
        v_target.table_name,
        v_target.dataset_row,
        v_target_checksum,
        v_actor
      );
    end if;
$replacement$
  );

  v_definition := private.review_migration_replace_between_v1(
    v_definition,
    '  v_scope_history := jsonb_build_object(',
    '  insert into private.reviews (' || chr(10),
    ''
  );

  v_definition := pg_catalog.replace(
    v_definition,
    ',' || chr(10) || '    target_team_id,' || chr(10) || '    scope_schema_version,' || chr(10) || '    scope_history',
    ',' || chr(10) || '    target_team_id'
  );
  v_definition := pg_catalog.replace(
    v_definition,
    ',' || chr(10) || '    nullif(v_root_row->>''team_id'', '''')::uuid,' || chr(10) || '    ''review_scope.v1'',' || chr(10) || '    v_scope_history',
    ',' || chr(10) || '    nullif(v_root_row->>''team_id'', '''')::uuid'
  );
  v_definition := private.review_migration_replace_between_v1(
    v_definition,
    '  perform private.review_validate_scope_history_v1(',
    '  perform set_config(''app.review_controlled_write'', ''on'', true);' || chr(10),
    ''
  );
  v_definition := private.review_migration_replace_between_v1(
    v_definition,
    '        (' || chr(10) || '          select item.reference_review_id',
    '        nullif(v_target.dataset_row->>''user_id'', '''')::uuid,',
    $replacement$        (
          select reference_review.id
          from private.reviews as reference_review
          where reference_review.review_kind = 'reference'
            and reference_review.target_table = v_target.table_name
            and reference_review.data_id = v_target.dataset_id
            and btrim(reference_review.data_version::text) = v_target.dataset_version
            and reference_review.submitted_revision_checksum =
              private.review_revision_fingerprint_v1(
                v_target.table_name,
                v_target.dataset_row
              )
            and reference_review.state_code in (-1, 0, 1, 2)
          order by case when reference_review.state_code in (0, 1, 2) then 0 else 1 end,
                   reference_review.modified_at desc,
                   reference_review.id
          limit 1
        ),
$replacement$
  );
  v_definition := pg_catalog.replace(
    v_definition,
    '          (v_review.scope_history->>''current_version'')::integer + 1,',
    '          null,'
  );
  v_definition := pg_catalog.replace(
    v_definition,
    ',' || chr(10) || '      ''scope_version'', 1,' || chr(10) || '      ''reference_review_ids'', to_jsonb(v_reference_ids)',
    ''
  );
  v_definition := pg_catalog.replace(
    v_definition,
    '        v_root_review_id,' || chr(10) || '        1,' || chr(10) || '        null',
    '        null,' || chr(10) || '        null,' || chr(10) || '        null'
  );

  execute v_definition;
end;
$$;

do $$
declare
  v_definition text;
begin
  select pg_catalog.pg_get_functiondef(
    'api.cmd_review_submit_comment(uuid,jsonb,integer,jsonb)'::pg_catalog.regprocedure
  ) into v_definition;

  v_definition := pg_catalog.replace(v_definition, '  v_current_snapshot jsonb;' || chr(10), '');
  v_definition := pg_catalog.replace(v_definition, '  v_items jsonb;' || chr(10), '');
  v_definition := pg_catalog.replace(v_definition, '  v_new_items jsonb := ''[]''::jsonb;' || chr(10), '');
  v_definition := pg_catalog.replace(v_definition, '  v_item jsonb;' || chr(10), '');
  v_definition := private.review_migration_replace_between_v1(
    v_definition,
    '    v_current_snapshot := private.review_scope_current_snapshot_v1(',
    '    for v_target in' || chr(10),
    ''
  );
  v_definition := private.review_migration_replace_between_v1(
    v_definition,
    '      if not exists (' || chr(10) || '        select 1' || chr(10) || '        from private.review_scope_current_items_v1',
    '      perform set_config(''app.review_controlled_write'', ''on'', true);' || chr(10),
    ''
  );
  v_definition := pg_catalog.replace(
    v_definition,
    '          (v_review.scope_history->>''current_version'')::integer + 1,',
    '          null,'
  );
  v_definition := private.review_migration_replace_between_v1(
    v_definition,
    '    if jsonb_array_length(v_new_items) > 0 then',
    '  end if;' || chr(10) || chr(10) || '  insert into private.comments (',
    ''
  );
  v_definition := pg_catalog.replace(
    v_definition,
    '          p_review_id,' || chr(10) || '          null,' || chr(10) || '          null',
    '          null,' || chr(10) || '          null,' || chr(10) || '          null'
  );
  v_definition := pg_catalog.replace(
    v_definition,
    '      ''comment_state_code'', p_comment_state,' || chr(10)
      || '      ''affected_datasets'', v_affected',
    '      ''comment_state_code'', p_comment_state,' || chr(10)
      || '      ''affected_datasets'', (' || chr(10)
      || '        select coalesce(' || chr(10)
      || '          jsonb_agg(affected.value - ''reference_review_id''),' || chr(10)
      || '          ''[]''::jsonb' || chr(10)
      || '        )' || chr(10)
      || '        from jsonb_array_elements(v_affected) as affected(value)' || chr(10)
      || '      )'
  );

  execute v_definition;
end;
$$;

do $$
declare
  v_definition text;
begin
  select pg_catalog.pg_get_functiondef(
    'api.cmd_review_finalize_approve(uuid,jsonb)'::pg_catalog.regprocedure
  ) into v_definition;
  v_definition := pg_catalog.replace(v_definition, '  v_current_snapshot jsonb;' || chr(10), '');
  v_definition := pg_catalog.replace(v_definition, '  v_approved_items jsonb;' || chr(10), '');
  v_definition := private.review_migration_replace_between_v1(
    v_definition,
    '  if v_review.review_kind = ''reference'' then' || chr(10) || '    v_approved_checksum := v_review.submitted_revision_checksum;',
    '  execute format(' || chr(10),
    $replacement$  if v_review.review_kind = 'reference' then
    v_approved_checksum := v_review.submitted_revision_checksum;
  end if;

$replacement$
  );
  execute v_definition;
end;
$$;

do $$
declare
  v_definition text;
begin
  select pg_catalog.pg_get_functiondef(
    'api.cmd_review_finalize_reject(uuid,text,jsonb)'::pg_catalog.regprocedure
  ) into v_definition;
  v_definition := pg_catalog.replace(
    v_definition,
    '    case when v_review.review_kind = ''root''' || chr(10)
      || '      then (v_review.scope_history->>''current_version'')::integer' || chr(10)
      || '      else null' || chr(10)
      || '    end,',
    '    null,'
  );
  execute v_definition;
end;
$$;

create or replace function private.review_notify_impacted_roots_v1(
  p_reference_review private.reviews,
  p_event_type text,
  p_actor uuid,
  p_reason_code text default null
)
returns void
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_root record;
begin
  for v_root in
    select impacted.root_review_id, root_review.target_owner_id
    from api.qry_reference_review_impacted_roots(
      p_reference_review.id,
      false
    ) as impacted
    join private.reviews as root_review
      on root_review.id = impacted.root_review_id
    order by impacted.root_review_id
  loop
    perform private.review_notify_event_v1(
      p_event_type,
      p_reference_review.id,
      v_root.target_owner_id,
      p_actor,
      p_reference_review.target_table,
      p_reference_review.data_id,
      pg_catalog.btrim(p_reference_review.data_version::text),
      null,
      null,
      p_reason_code
    );
  end loop;
end;
$$;

create or replace function api.qry_root_review_reference_progress(
  p_root_review_id uuid
)
returns table (
  reference_review_id uuid,
  target_table text,
  data_id uuid,
  data_version text,
  submitted_revision_checksum text,
  state_code integer,
  reviewer_count integer,
  completed_reviewer_count integer,
  relation_paths jsonb
)
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
begin
  if v_actor is null or not api.cmd_review_is_review_admin(v_actor) then
    raise exception using errcode = '42501', message = 'REVIEW_ADMIN_REQUIRED';
  end if;

  return query
  select
    progress.reference_review_id,
    progress.target_table,
    progress.data_id,
    progress.data_version,
    progress.submitted_revision_checksum,
    progress.state_code,
    progress.reviewer_count,
    progress.completed_reviewer_count,
    '[]'::jsonb
  from api.qry_root_review_reference_progress_v2(p_root_review_id) as progress;
end;
$$;

create or replace function api.qry_review_admin_queue_items_v2(
  p_status text default null,
  p_page integer default 1,
  p_page_size integer default 20
)
returns table (
  id uuid,
  review_kind text,
  target_table text,
  data_id uuid,
  data_version text,
  state_code integer,
  target_owner_id uuid,
  target_team_id uuid,
  submitted_revision_checksum text,
  reviewer_id jsonb,
  deadline timestamptz,
  reference_count integer,
  completed_reviewer_count integer,
  modified_at timestamptz,
  total_count bigint
)
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
begin
  if v_actor is null or not api.cmd_review_is_review_admin(v_actor) then
    raise exception using errcode = '42501', message = 'REVIEW_ADMIN_REQUIRED';
  end if;

  return query
  with review_rows as materialized (
    select review_row.*
    from private.reviews as review_row
    where review_row.review_kind in ('root', 'reference')
      and (
        p_status is null
        or (p_status = 'unassigned' and review_row.state_code = 0)
        or (p_status = 'assigned' and review_row.state_code = 1)
        or (p_status = 'approved' and review_row.state_code = 2)
        or (p_status = 'rejected' and review_row.state_code = -1)
      )
  )
  select
    review_row.id,
    review_row.review_kind,
    review_row.target_table,
    review_row.data_id,
    pg_catalog.btrim(review_row.data_version::text),
    review_row.state_code,
    review_row.target_owner_id,
    review_row.target_team_id,
    review_row.submitted_revision_checksum,
    coalesce(review_row.reviewer_id, '[]'::jsonb),
    review_row.deadline,
    case when review_row.review_kind = 'root' then (
      select pg_catalog.count(*)::integer
      from private.review_derive_current_references_v1(array[review_row.id])
    ) else 0 end,
    count(comment_row.reviewer_id)
      filter (where comment_row.state_code in (1, -3, 2))::integer,
    review_row.modified_at,
    pg_catalog.count(*) over ()
  from review_rows as review_row
  left join private.comments as comment_row
    on comment_row.review_id = review_row.id
    and comment_row.state_code <> -2
  group by review_row.id, review_row.review_kind, review_row.target_table,
    review_row.data_id, review_row.data_version, review_row.state_code,
    review_row.target_owner_id, review_row.target_team_id,
    review_row.submitted_revision_checksum, review_row.reviewer_id,
    review_row.deadline, review_row.modified_at
  order by review_row.modified_at desc, review_row.id
  offset greatest(coalesce(p_page, 1) - 1, 0)
    * greatest(coalesce(p_page_size, 20), 1)
  limit least(greatest(coalesce(p_page_size, 20), 1), 100);
end;
$$;

drop function private.cmd_review_migrate_legacy_v2(uuid, uuid, jsonb);

drop trigger if exists reviews_scope_history_guard_v1 on private.reviews;
alter table private.reviews drop constraint if exists reviews_kind_scope_v2_chk;
drop index if exists private.reviews_root_current_reference_ids_gin_idx;
alter table private.reviews
  drop column current_reference_review_ids,
  drop column all_reference_review_ids,
  drop column scope_schema_version,
  drop column scope_history;

alter table private.reviews
  add constraint reviews_kind_target_v3_chk check (
    review_kind is null
    or (
      target_table is not null
      and data_id is not null
      and data_version is not null
      and submitted_revision_checksum is not null
    )
  );

drop function if exists private.review_scope_history_guard_v1();
drop function if exists private.review_append_scope_snapshot_v1(uuid, text, text, jsonb, uuid);
drop function if exists private.review_validate_scope_history_v1(uuid, jsonb);
drop function if exists private.review_scope_current_items_v1(jsonb);
drop function if exists private.review_scope_current_snapshot_v1(jsonb);
drop function if exists private.review_scope_current_reference_ids_v1(jsonb);
drop function if exists private.review_scope_all_reference_ids_v1(jsonb);
drop function if exists private.review_scope_checksum_v1(jsonb);
drop function if exists private.review_replace_reference_item_v1(jsonb, text, uuid, text, text, uuid, uuid, uuid);
drop function if exists private.review_rebind_active_roots_to_reference_v1(text, jsonb, text, uuid, uuid);

drop function private.review_migration_replace_between_v1(text, text, text, text);

-- Reparse routines that were rewritten before the reviews columns were
-- dropped. This refreshes PL/pgSQL composite descriptors against the final
-- private.reviews row type and avoids retaining a pre-drop tuple descriptor.
do $$
declare
  v_routine pg_catalog.regprocedure;
  v_definition text;
begin
  foreach v_routine in array array[
    'api.cmd_review_submit_v2(text,uuid,text,jsonb,jsonb)'::pg_catalog.regprocedure,
    'api.cmd_review_submit_comment(uuid,jsonb,integer,jsonb)'::pg_catalog.regprocedure,
    'api.cmd_review_finalize_approve(uuid,jsonb)'::pg_catalog.regprocedure,
    'api.cmd_review_finalize_reject(uuid,text,jsonb)'::pg_catalog.regprocedure,
    'private.review_root_currently_references_target_v1(uuid,text,uuid,text)'::pg_catalog.regprocedure,
    'private.review_notify_impacted_roots_v1(private.reviews,text,uuid,text)'::pg_catalog.regprocedure
  ]
  loop
    select pg_catalog.pg_get_functiondef(v_routine::pg_catalog.oid)
    into v_definition;
    execute v_definition;
  end loop;
end;
$$;

comment on constraint reviews_kind_target_v3_chk on private.reviews is
  'Root and Reference Reviews require target identity and checksum; no Root/Reference relationship fields are persisted.';
