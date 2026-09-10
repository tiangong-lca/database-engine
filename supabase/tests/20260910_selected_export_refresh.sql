begin;
create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, auth;
select no_plan();

insert into auth.users (id) values ('64000000-0000-4000-8000-000000000001');
insert into private.roles (user_id, team_id, role) values (
  '64000000-0000-4000-8000-000000000001',
  '00000000-0000-0000-0000-000000000000', 'admin'
);
insert into public.contacts (id, version, user_id, state_code, json) values
  ('64020000-0000-4000-8000-000000000001', '01.01.000',
   '64000000-0000-4000-8000-000000000001', 0, '{"name":"before"}'),
  ('64020000-0000-4000-8000-000000000002', '01.01.000',
   '64000000-0000-4000-8000-000000000001', 0, '{"name":"dependency"}');

-- Exercise the same lifecycle for one/multiple explicit roots and all bulk scopes.
-- Only the Worker completion/artifact writes are simulated; admission and readback
-- execute the real service facades, including canonical job and owner checks.
create function pg_temp.check_export_refresh(p_label text, p_scope text, p_roots jsonb)
returns setof text language plpgsql as $$
declare
  actor uuid := '64000000-0000-4000-8000-000000000001';
  first_job uuid := gen_random_uuid();
  second_job uuid := gen_random_uuid();
  artifact uuid := gen_random_uuid();
  first_result jsonb;
  second_result jsonb;
  duplicate_result jsonb;
  first_worker uuid;
  state text;
  cache_state text;
  key text := 'issue640:' || p_label;
begin
  first_result := api.svc_tidas_package_export_enqueue(
    actor, p_scope, p_roots, key, '{}', first_job, key
  );
  first_worker := (first_result ->> 'worker_job_id')::uuid;
  return next is(first_result ->> 'mode', 'queued', p_label || ': first export queues');

  foreach state in array array['queued','running','waiting','stale','blocked'] loop
    update private.worker_jobs set status = state,
      blocker_codes = case when state = 'blocked' then array['fixture_blocked'] else '{}'::text[] end,
      resolution_scope = case when state = 'blocked' then 'user' else null end
    where id = first_worker;
    duplicate_result := api.svc_tidas_package_export_enqueue(
      actor, p_scope, p_roots, key, '{}', gen_random_uuid(), key
    );
    return next is(duplicate_result ->> 'mode',
      case when state = 'blocked' then 'blocked' else 'in_progress' end,
      p_label || ': ' || state || ' preserves existing admission policy');
    return next is(duplicate_result ->> 'worker_job_id', first_worker::text,
      p_label || ': ' || state || ' preserves canonical work');
    return next is(duplicate_result ->> 'job_id', first_job::text,
      p_label || ': ' || state || ' preserves package identity');
  end loop;

  insert into private.lca_package_artifacts (
    id, job_id, worker_job_id, artifact_kind, status, artifact_url,
    artifact_sha256, artifact_byte_size, artifact_format, content_type
  ) values (
    artifact, first_job, first_worker, 'export_zip', 'ready',
    's3://packages/issue640/' || first_job || '.zip', repeat('a',64), 640,
    'tidas-package-zip:v1', 'application/zip'
  );
  update private.worker_jobs set status = 'completed', progress = 1,
    finished_at = now(), blocker_codes = '{}', resolution_scope = null
  where id = first_worker;

  -- Test both completed-worker fallback and ready-cache fast paths. A cache
  -- lagging the Worker must behave identically to one already marked ready.
  foreach cache_state in array array['running','ready'] loop
    update private.lca_package_request_cache set status = cache_state,
      job_id = first_job, worker_job_id = first_worker, export_artifact_id = artifact
    where requested_by = actor and operation = 'export_package' and request_key = key;
    second_job := gen_random_uuid();
    second_result := api.svc_tidas_package_export_enqueue(
      actor, p_scope, p_roots, key, '{}', second_job, key
    );
    return next is(second_result ->> 'mode', 'queued',
      p_label || ': completed Worker with ' || cache_state || ' cache queues fresh work');
    return next is(second_result ->> 'job_id', second_job::text,
      p_label || ': refreshed package uses new identity');
    return next isnt(second_result ->> 'worker_job_id', first_worker::text,
      p_label || ': refreshed Worker uses new identity');
    return next is((select job_id from private.lca_package_request_cache
      where requested_by = actor and operation = 'export_package' and request_key = key),
      second_job, p_label || ': cache advances to new package');
    return next is(api.svc_tidas_package_read(actor, second_job) #> '{data,artifacts}',
      '[]'::jsonb, p_label || ': pending job never exposes previous ZIP');
    return next is(api.svc_tidas_package_read(actor, first_job) #>> '{data,artifacts,0,id}',
      artifact::text, p_label || ': historical job still resolves original ZIP');
    return next is(api.svc_tidas_package_read(actor, first_worker) #>> '{data,artifacts,0,id}',
      artifact::text, p_label || ': historical Worker still resolves original ZIP');
    duplicate_result := api.svc_tidas_package_export_enqueue(
      actor, p_scope, p_roots, key, '{}', gen_random_uuid(), key
    );
    return next is(duplicate_result ->> 'job_id', second_job::text,
      p_label || ': active refreshed request deduplicates');
    update private.worker_jobs set status = 'completed', finished_at = now()
      where id = (second_result ->> 'worker_job_id')::uuid;
  end loop;
end;
$$;

grant execute on function pg_temp.check_export_refresh(text,text,jsonb) to service_role;

set local role service_role;
select set_config('request.jwt.claims', '{"role":"service_role"}', true);
select * from pg_temp.check_export_refresh('single', 'selected_roots',
  '[{"table":"contacts","id":"64020000-0000-4000-8000-000000000001","version":"01.01.000"}]');
select * from pg_temp.check_export_refresh('multiple', 'selected_roots',
  '[{"table":"contacts","id":"64020000-0000-4000-8000-000000000001","version":"01.01.000"},{"table":"contacts","id":"64020000-0000-4000-8000-000000000002","version":"01.01.000"}]');
select * from pg_temp.check_export_refresh('mine', 'current_user', '[]');
select * from pg_temp.check_export_refresh('open', 'open_data', '[]');
select * from pg_temp.check_export_refresh('combined', 'current_user_and_open_data', '[]');
reset role;

select ok(has_function_privilege('service_role',
  'api.svc_tidas_package_export_enqueue(uuid,text,jsonb,text,jsonb,uuid,text)', 'execute')
  and not has_function_privilege('authenticated',
  'api.svc_tidas_package_export_enqueue(uuid,text,jsonb,text,jsonb,uuid,text)', 'execute')
  and not has_function_privilege('anon',
  'api.svc_tidas_package_export_enqueue(uuid,text,jsonb,text,jsonb,uuid,text)', 'execute'),
  'forward replacement preserves the service-only boundary');
select * from finish();
rollback;
