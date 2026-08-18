begin;

create table private.lcia_result_sets (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  created_at timestamptz not null default now(),
  constraint lcia_result_sets_name_check check (length(btrim(name)) > 0)
);

alter table private.lcia_result_sets enable row level security;

revoke all on table private.lcia_result_sets
  from public, anon, authenticated, service_role;

alter table private.lcia_scope_closure_checks
  add column result_set_id uuid;

alter table private.lcia_scope_closure_checks
  add constraint lcia_scope_closure_checks_result_set_id_fkey
  foreign key (result_set_id)
  references private.lcia_result_sets(id)
  on delete restrict;

create index lcia_scope_closure_checks_result_set_idx
  on private.lcia_scope_closure_checks (result_set_id)
  where result_set_id is not null;

create index lcia_result_sets_created_idx
  on private.lcia_result_sets (created_at desc, id desc);

create or replace function api.cmd_lcia_result_set_create(p_name text)
returns jsonb
language plpgsql
security definer
set search_path = api, private, public, util, extensions, pg_temp
as $function$
declare
  v_actor uuid := auth.uid();
  v_result_set private.lcia_result_sets%rowtype;
begin
  if v_actor is null then
    return api.lcia_scope_closure_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not api.lcia_scope_closure_is_manager() then
    return api.lcia_scope_closure_error(
      'not_data_product_manager', 403,
      'Data product manager role is required'
    );
  end if;
  if coalesce(length(btrim(p_name)), 0) = 0 then
    return api.lcia_scope_closure_error(
      'invalid_result_set_name', 400, 'Result set name is required'
    );
  end if;

  insert into private.lcia_result_sets (name)
  values (btrim(p_name))
  returning * into v_result_set;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'schemaVersion', 'lcia.result-set.v1',
      'resultSetId', v_result_set.id,
      'name', v_result_set.name,
      'createdAt', v_result_set.created_at
    )
  );
end;
$function$;

create or replace function api.list_lcia_result_sets(
  p_limit integer default 100
)
returns jsonb
language plpgsql
security definer
set search_path = api, private, public, util, extensions, pg_temp
as $function$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_limit, 100), 200));
begin
  if v_actor is null then
    return api.lcia_scope_closure_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not api.lcia_scope_closure_is_manager() then
    return api.lcia_scope_closure_error(
      'not_data_product_manager', 403,
      'Data product manager role is required'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'items', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'schemaVersion', 'lcia.result-set.v1',
            'resultSetId', result_set.id,
            'name', result_set.name,
            'createdAt', result_set.created_at
          )
          order by result_set.created_at desc, result_set.id desc
        )
        from (
          select id, name, created_at
          from private.lcia_result_sets
          order by created_at desc, id desc
          limit v_limit
        ) result_set
      ), '[]'::jsonb)
    )
  );
end;
$function$;

create or replace function api.get_lcia_result_set(p_result_set_id uuid)
returns jsonb
language plpgsql
security definer
set search_path = api, private, public, util, extensions, pg_temp
as $function$
declare
  v_actor uuid := auth.uid();
  v_result_set private.lcia_result_sets%rowtype;
begin
  if v_actor is null then
    return api.lcia_scope_closure_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not api.lcia_scope_closure_is_manager() then
    return api.lcia_scope_closure_error(
      'result_set_not_found', 404, 'Result set not found'
    );
  end if;

  select *
  into v_result_set
  from private.lcia_result_sets
  where id = p_result_set_id;

  if v_result_set.id is null then
    return api.lcia_scope_closure_error(
      'result_set_not_found', 404, 'Result set not found'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'schemaVersion', 'lcia.result-set.v1',
      'resultSetId', v_result_set.id,
      'name', v_result_set.name,
      'createdAt', v_result_set.created_at
    )
  );
end;
$function$;

create or replace function api.cmd_lcia_scope_closure_check_request_v3(
  p_result_set_id uuid,
  p_requested_scope jsonb,
  p_request_idempotency_token text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = api, private, public, util, extensions, pg_temp
set statement_timeout = '60s'
as $function$
declare
  v_actor uuid := auth.uid();
  v_result jsonb;
  v_closure_check_id uuid;
begin
  if v_actor is null then
    return api.lcia_scope_closure_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not api.lcia_scope_closure_is_manager() then
    return api.lcia_scope_closure_error(
      'not_data_product_manager', 403,
      'Data product manager role is required'
    );
  end if;
  if not exists (
    select 1 from private.lcia_result_sets where id = p_result_set_id
  ) then
    return api.lcia_scope_closure_error(
      'result_set_not_found', 404, 'Result set not found'
    );
  end if;

  v_result := api.cmd_lcia_scope_closure_check_request_v2(
    p_requested_scope,
    p_request_idempotency_token,
    coalesce(p_audit, '{}'::jsonb)
      || jsonb_build_object('resultSetId', p_result_set_id)
  );
  if coalesce((v_result->>'ok')::boolean, false) is not true then
    return v_result;
  end if;

  v_closure_check_id := nullif(
    v_result->'data'->>'closureCheckId', ''
  )::uuid;

  update private.lcia_scope_closure_checks
  set result_set_id = p_result_set_id,
      updated_at = now()
  where id = v_closure_check_id
    and requested_by = v_actor
    and (result_set_id is null or result_set_id = p_result_set_id);

  if not found then
    return api.lcia_scope_closure_error(
      'closure_check_result_set_conflict', 409,
      'Closure check is already bound to another result set'
    );
  end if;

  return jsonb_set(
    v_result,
    '{data,resultSetId}',
    to_jsonb(p_result_set_id),
    true
  );
end;
$function$;

create or replace function private.get_task_summary_v2_feed_unversioned(
  p_category text default null,
  p_job_kinds text[] default null,
  p_statuses text[] default null,
  p_updated_since timestamptz default null,
  p_cursor_updated_at timestamptz default null,
  p_cursor_job_id uuid default null,
  p_limit integer default 50,
  p_root_only boolean default false
)
returns jsonb
language plpgsql
security definer
set search_path = private, api, public, util, extensions, pg_temp
as $function$
declare
  a uuid := auth.uid();
  lim integer := greatest(1, least(coalesce(p_limit, 50), 200));
  mgr boolean;
begin
  if a is null then
    return api.lcia_scope_closure_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if (p_cursor_updated_at is null) <> (p_cursor_job_id is null) then
    return api.lcia_scope_closure_error(
      'invalid_task_cursor', 400,
      'Task cursor fields must be supplied together'
    );
  end if;
  mgr := api.lcia_scope_closure_is_manager();

  return jsonb_build_object(
    'ok', true,
    'serverTime', now(),
    'data', coalesce((
      with x as (
        select
          j.*,
          k.task_center_category,
          p.id as package_id,
          coalesce(cd.id, cp.id, ck.id) as closure_id,
          coalesce(cd.status, cp.status, ck.status) as closure_status,
          coalesce(
            cd.certificate_status,
            cp.certificate_status,
            ck.certificate_status
          ) as certificate_status,
          result_set.id as result_set_id,
          result_set.name as result_set_name,
          greatest(
            j.updated_at,
            coalesce(cd.updated_at, '-infinity'::timestamptz),
            coalesce(cp.updated_at, '-infinity'::timestamptz),
            coalesce(ck.updated_at, '-infinity'::timestamptz),
            coalesce(p.updated_at, '-infinity'::timestamptz)
          ) as pu
        from private.worker_jobs j
        join private.worker_job_kinds k on k.job_kind = j.job_kind
        left join private.lcia_scope_closure_checks cd
          on cd.worker_job_id = j.id
        left join private.lcia_result_packages p
          on p.build_worker_job_id = j.id
        left join private.lcia_scope_closure_checks cp
          on cp.id = case
            when (j.payload_json->>'closure_check_id')
              ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
              then (j.payload_json->>'closure_check_id')::uuid
            else null
          end
        left join private.lcia_scope_closure_checks ck
          on ck.id = p.closure_check_id
        left join private.lcia_result_sets result_set
          on result_set.id = coalesce(
            cd.result_set_id,
            cp.result_set_id,
            ck.result_set_id
          )
        where j.requested_by = a
          and (
            j.visibility = 'user'
            or (
              mgr
              and j.visibility = 'operator'
              and j.job_kind = any(array[
                'lcia.scope_closure_check',
                'lcia_result.package_build'
              ])
            )
          )
          and (p_category is null or k.task_center_category = p_category)
          and (p_job_kinds is null or j.job_kind = any(p_job_kinds))
          and (p_statuses is null or j.status = any(p_statuses))
          and (
            p_updated_since is null
            or greatest(
              j.updated_at,
              coalesce(cd.updated_at, '-infinity'::timestamptz),
              coalesce(cp.updated_at, '-infinity'::timestamptz),
              coalesce(ck.updated_at, '-infinity'::timestamptz),
              coalesce(p.updated_at, '-infinity'::timestamptz)
            ) >= p_updated_since
          )
          and (
            not p_root_only
            or j.root_job_id is null
            or j.root_job_id = j.id
          )
      ), pg as (
        select *
        from x
        where p_cursor_updated_at is null
          or (pu, id) < (p_cursor_updated_at, p_cursor_job_id)
        order by pu desc, id desc
        limit lim + 1
      ), sh as (
        select * from pg order by pu desc, id desc limit lim
      )
      select jsonb_build_object(
        'items', coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
          'jobId', id,
          'jobKind', job_kind,
          'category', task_center_category,
          'requestedBy', requested_by,
          'workerStatus', status,
          'phase', phase,
          'progressFraction', case
            when progress is null then null
            else greatest(0::numeric, least(progress, 1::numeric))
          end,
          'progressCounters', diagnostics->'progressCounters',
          'domainStatus', coalesce(closure_status, result_json->>'status'),
          'domainValidity', certificate_status,
          'projectionUpdatedAt', pu,
          'title', coalesce(result_set_name, payload_json->>'name', job_kind),
          'blockerCodes', blocker_codes,
          'errorSummary', error_code,
          'capabilities', jsonb_build_object(
            'canCancel', status in ('queued', 'running', 'waiting'),
            'canDownloadReport', closure_id is not null
              and closure_status in ('passed', 'blocked'),
            'canOpenWorkbench', task_center_category = 'data_product',
            'canPreviewResult', package_id is not null
          ),
          'deepLink', case
            when package_id is not null then jsonb_build_object(
              'routeKey', 'data_product.package',
              'params', jsonb_strip_nulls(jsonb_build_object(
                'packageId', package_id,
                'closureCheckId', closure_id,
                'resultSetId', result_set_id
              ))
            )
            when closure_id is not null then jsonb_build_object(
              'routeKey', 'data_product.closure_check',
              'params', jsonb_strip_nulls(jsonb_build_object(
                'closureCheckId', closure_id,
                'resultSetId', result_set_id
              ))
            )
          end,
          'closureCheckId', closure_id,
          'resultPackageId', package_id,
          'resultSetId', result_set_id,
          'resultSetName', result_set_name
        )) order by pu desc, id desc), '[]'::jsonb),
        'nextCursor', case
          when exists(select 1 from pg offset lim) then (
            select jsonb_build_object('updatedAt', pu, 'jobId', id)
            from sh
            order by pu asc, id asc
            limit 1
          )
          else null
        end
      )
      from sh
    ), jsonb_build_object('items', '[]'::jsonb, 'nextCursor', null))
  );
end;
$function$;

alter function api.cmd_lcia_result_set_create(text) owner to postgres;
alter function api.list_lcia_result_sets(integer) owner to postgres;
alter function api.get_lcia_result_set(uuid) owner to postgres;
alter function api.cmd_lcia_scope_closure_check_request_v3(
  uuid, jsonb, text, jsonb
) owner to postgres;
alter function private.get_task_summary_v2_feed_unversioned(
  text, text[], text[], timestamptz, timestamptz, uuid, integer, boolean
) owner to postgres;

revoke all on function api.cmd_lcia_result_set_create(text) from public;
revoke all on function api.list_lcia_result_sets(integer) from public;
revoke all on function api.get_lcia_result_set(uuid) from public;
revoke all on function api.cmd_lcia_scope_closure_check_request_v3(
  uuid, jsonb, text, jsonb
) from public;
revoke all on function private.get_task_summary_v2_feed_unversioned(
  text, text[], text[], timestamptz, timestamptz, uuid, integer, boolean
) from public, anon, authenticated, service_role;

grant execute on function api.cmd_lcia_result_set_create(text)
  to api_internal_executor, authenticated;
grant execute on function api.list_lcia_result_sets(integer)
  to api_internal_executor, authenticated;
grant execute on function api.get_lcia_result_set(uuid)
  to api_internal_executor, authenticated;
grant execute on function api.cmd_lcia_scope_closure_check_request_v3(
  uuid, jsonb, text, jsonb
) to api_internal_executor, authenticated;
grant execute on function private.get_task_summary_v2_feed_unversioned(
  text, text[], text[], timestamptz, timestamptz, uuid, integer, boolean
) to api_internal_executor;

insert into private.api_capability_grants (
  routine_identity,
  capability_id,
  allow_anon,
  allow_authenticated,
  allow_service_role
)
values
  (
    'api.cmd_lcia_result_set_create(text)',
    'EDGE-DATA-PRODUCT-01', false, true, false
  ),
  (
    'api.list_lcia_result_sets(integer)',
    'EDGE-DATA-PRODUCT-01', false, true, false
  ),
  (
    'api.get_lcia_result_set(uuid)',
    'EDGE-DATA-PRODUCT-01', false, true, false
  ),
  (
    'api.cmd_lcia_scope_closure_check_request_v3(uuid, jsonb, text, jsonb)',
    'EDGE-DATA-PRODUCT-01', false, true, false
  )
on conflict (routine_identity) do update
set capability_id = excluded.capability_id,
    allow_anon = excluded.allow_anon,
    allow_authenticated = excluded.allow_authenticated,
    allow_service_role = excluded.allow_service_role;

comment on table private.lcia_result_sets is
  'Named persistent containers for resuming the Data Processing workflow.';
comment on column private.lcia_scope_closure_checks.result_set_id is
  'Optional ResultSet container binding. NULL preserves legacy closure checks.';
comment on function api.cmd_lcia_scope_closure_check_request_v3(
  uuid, jsonb, text, jsonb
) is
  'Creates or reuses a Scope Closure check and atomically binds it to one persistent ResultSet.';

notify pgrst, 'reload schema';

commit;
