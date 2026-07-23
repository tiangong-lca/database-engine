-- Task feeds are keyset-paginated on projectionUpdatedAt.  `now()` is fixed
-- for the enclosing transaction, so a certificate event or ready package
-- created after a completed job could otherwise fail to move its projection
-- forward in a batched service transaction.  These are domain events, not
-- historical timestamps, so use the wall clock for the feed touch.

create or replace function public.svc_lcia_scope_closure_certificate_event(
  p_closure_check_id uuid, p_certificate_status text, p_reason text
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_check public.lcia_scope_closure_checks%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error('service_role_required', 403, 'Service role is required');
  end if;
  if p_certificate_status not in ('stale', 'revoked')
     or coalesce(nullif(trim(p_reason), ''), '') = '' then
    return public.lcia_scope_closure_error('invalid_certificate_event', 400, 'Certificate event status and reason are required');
  end if;

  select *
    into v_check
  from public.lcia_scope_closure_checks
  where id = p_closure_check_id
  for update;

  if v_check.id is null then
    return public.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found');
  end if;
  if not (
    (v_check.certificate_status = 'valid' and p_certificate_status in ('stale', 'revoked'))
    or (v_check.certificate_status = 'stale' and p_certificate_status = 'revoked')
  ) then
    return public.lcia_scope_closure_error('invalid_certificate_transition', 409, 'Certificate validity transition is not allowed');
  end if;

  insert into public.lcia_scope_closure_certificate_events (
    closure_check_id,
    certificate_status,
    reason
  ) values (
    v_check.id,
    p_certificate_status,
    trim(p_reason)
  );

  update public.lcia_scope_closure_checks
  set certificate_status = p_certificate_status,
      updated_at = clock_timestamp()
  where id = v_check.id;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'closureCheckId', v_check.id,
      'certificateStatus', p_certificate_status
    )
  );
end;
$$;

create or replace function public.lcia_result_package_touch_task_projection()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  update public.worker_jobs
  set updated_at = clock_timestamp()
  where id = new.build_worker_job_id;
  return new;
end;
$$;

revoke all on function public.lcia_result_package_touch_task_projection() from public, anon, authenticated, service_role;

drop trigger if exists lcia_result_packages_touch_task_projection
  on public.lcia_result_packages;

create trigger lcia_result_packages_touch_task_projection
after insert on public.lcia_result_packages
for each row
execute function public.lcia_result_package_touch_task_projection();
