-- Ensure stale publication reconciliation returns the state persisted by its
-- claim. This additive redefinition is required for preview/branch databases
-- that already recorded 20260729060609 before the snapshot fix landed.

create or replace function public.svc_lcia_scope_closure_artifact_write_set_reconcile(
  p_limit integer default 100,
  p_lease_seconds integer default 300
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_token uuid := gen_random_uuid();
  v_lease_expires_at timestamptz :=
    now() + make_interval(secs => p_lease_seconds);
  v_claimed_ids uuid[] := array[]::uuid[];
  v_items jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if p_limit is null
     or p_limit not between 1 and 500
     or p_lease_seconds is null
     or p_lease_seconds not between 1 and 3600 then
    return public.lcia_scope_closure_error(
      'artifact_write_set_reconcile_invalid', 400, 'Invalid reconcile bounds'
    );
  end if;
  with candidates as (
    select id
    from public.lcia_scope_closure_artifact_write_sets
    where (
      status = 'cleanup_pending'
      or (status = 'staging' and staging_expires_at <= now())
    )
      and (
        reconcile_token is null
        or reconcile_expires_at is null
        or reconcile_expires_at <= now()
      )
    order by staging_expires_at, created_at, id
    for update skip locked
    limit p_limit
  ), claimed as (
    update public.lcia_scope_closure_artifact_write_sets write_set
    set status = 'cleanup_pending',
        reconcile_token = v_token,
        reconcile_claimed_at = now(),
        reconcile_expires_at = v_lease_expires_at,
        updated_at = now()
    from candidates
    where write_set.id = candidates.id
    returning write_set.id
  )
  select coalesce(array_agg(claimed.id order by claimed.id), array[]::uuid[])
  into v_claimed_ids
  from claimed;

  -- A projection evaluated inside the data-modifying CTE statement observes
  -- its pre-update command snapshot. Render from a second statement so the
  -- returned status, fence token, and lease match the persisted claim.
  select coalesce(jsonb_agg(
    public.lcia_scope_closure_artifact_write_set_json(claimed_id)
    order by claimed_id
  ), '[]'::jsonb)
  into v_items
  from unnest(v_claimed_ids) claimed_id;

  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'reconcileToken', v_token,
    'leaseExpiresAt', v_lease_expires_at,
    'writeSets', v_items
  ));
end;
$$;

revoke all on function public.svc_lcia_scope_closure_artifact_write_set_reconcile(
  integer, integer
) from public, anon, authenticated, service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_write_set_reconcile(
  integer, integer
) to service_role;

comment on function public.svc_lcia_scope_closure_artifact_write_set_reconcile(
  integer, integer
) is
  'Service-only fenced stale-publication claim. Returns cleanup_pending write-set projections from a post-update statement so status, reconcileToken, and reconcileLeaseExpiresAt exactly match persisted claim state.';
