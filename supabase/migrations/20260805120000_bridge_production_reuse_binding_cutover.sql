begin;

set local lock_timeout = '10s';
set local statement_timeout = '2min';

-- Issue #422 production already recorded the later Issue #453 hotfix before
-- the full schema cutover reached main. That hotfix added one reviewed helper
-- to the pre-cutover public catalog, so the original 333-routine cutover guard
-- correctly stopped at 334. Move that exact helper and its five textual callers
-- into a standalone-valid bridge state before the cutover runs.
--
-- Fresh databases reach this migration before the later hotfix and therefore
-- no-op at the canonical 333-routine state. Already-cut-over databases also
-- no-op at zero public routines when this older version is back-merged to dev.
do $bridge_production_reuse_binding_cutover$
declare
  public_helper_oid oid := pg_catalog.to_regprocedure(
    'public.lcia_scope_closure_bundle_binding_matches(public.lcia_scope_closure_checks,public.worker_job_artifacts)'
  );
  private_pre_cutover_helper_oid oid := pg_catalog.to_regprocedure(
    'private.lcia_scope_closure_bundle_binding_matches(public.lcia_scope_closure_checks,public.worker_job_artifacts)'
  );
  public_function_count bigint;
  actual_callers text[];
  expected_callers constant text[] := array[
    'cmd_lcia_result_build_request_v2_without_expiry',
    'lcia_result_package_bind_closure_certificate',
    'lcia_scope_closure_certificate_validity_guard',
    'lcia_scope_closure_evidence_usable',
    'svc_lcia_scope_closure_build_binding_without_expiry'
  ];
  caller record;
  rewritten record;
  definition text;
  rewritten_count integer := 0;
begin
  select count(*)
  into public_function_count
  from pg_catalog.pg_proc as routine
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = routine.pronamespace
  where namespace.nspname = 'public'
    and routine.prokind = 'f';

  if public_helper_oid is null then
    if pg_catalog.to_regclass('public.worker_jobs') is not null then
      if public_function_count <> 333 then
        raise exception
          'Issue #422 bridge expected the canonical 333-function pre-cutover catalog, found %',
          public_function_count;
      end if;
    else
      if pg_catalog.to_regclass('private.worker_jobs') is null
         or public_function_count <> 0 then
        raise exception
          'Issue #422 bridge found neither a canonical pre-cutover nor post-cutover catalog (public functions=%)',
          public_function_count;
      end if;
    end if;

    return;
  end if;

  if pg_catalog.to_regclass('public.worker_jobs') is null
     or pg_catalog.to_regclass('public.lcia_scope_closure_checks') is null
     or pg_catalog.to_regclass('public.worker_job_artifacts') is null
     or pg_catalog.to_regclass('private.worker_jobs') is not null then
    raise exception
      'Issue #422 bridge helper exists outside the reviewed pre-cutover relation state';
  end if;

  if private_pre_cutover_helper_oid is not null then
    raise exception
      'Issue #422 bridge found both public and private pre-cutover helper identities';
  end if;

  if public_function_count <> 334 then
    raise exception
      'Issue #422 bridge expected 334 public functions with the production helper, found %',
      public_function_count;
  end if;

  actual_callers := array(
    select routine.proname::text
    from pg_catalog.pg_proc as routine
    join pg_catalog.pg_namespace as namespace
      on namespace.oid = routine.pronamespace
    where namespace.nspname = 'public'
      and routine.oid <> public_helper_oid
      and routine.prosrc
        like '%public.lcia_scope_closure_bundle_binding_matches%'
    order by routine.proname
  );

  if actual_callers is distinct from expected_callers then
    raise exception
      'Issue #422 bridge caller manifest drifted: expected %, found %',
      expected_callers,
      actual_callers;
  end if;

  execute pg_catalog.format(
    'alter function %s set schema private',
    public_helper_oid::pg_catalog.regprocedure
  );

  for caller in
    select
      routine.oid,
      routine.proowner,
      routine.proacl,
      routine.prosecdef,
      routine.provolatile,
      routine.proparallel,
      routine.proleakproof,
      routine.proisstrict,
      routine.proconfig
    from pg_catalog.pg_proc as routine
    where routine.proname::text = any(expected_callers)
      and routine.pronamespace = 'public'::pg_catalog.regnamespace
      and routine.prosrc
        like '%public.lcia_scope_closure_bundle_binding_matches%'
    order by routine.oid
  loop
    definition := pg_catalog.pg_get_functiondef(caller.oid);
    definition := pg_catalog.replace(
      definition,
      'public.lcia_scope_closure_bundle_binding_matches',
      'private.lcia_scope_closure_bundle_binding_matches'
    );
    definition := pg_catalog.replace(
      definition,
      '"public"."lcia_scope_closure_bundle_binding_matches"',
      '"private"."lcia_scope_closure_bundle_binding_matches"'
    );

    if definition like '%public.lcia_scope_closure_bundle_binding_matches%'
       or definition like '%"public"."lcia_scope_closure_bundle_binding_matches"%' then
      raise exception
        'Issue #422 bridge could not rewrite caller %',
        caller.oid::pg_catalog.regprocedure;
    end if;

    execute definition;

    select
      routine.oid,
      routine.proowner,
      routine.proacl,
      routine.prosecdef,
      routine.provolatile,
      routine.proparallel,
      routine.proleakproof,
      routine.proisstrict,
      routine.proconfig
    into rewritten
    from pg_catalog.pg_proc as routine
    where routine.oid = caller.oid;

    if rewritten.oid is null
       or rewritten.proowner is distinct from caller.proowner
       or rewritten.proacl is distinct from caller.proacl
       or rewritten.prosecdef is distinct from caller.prosecdef
       or rewritten.provolatile is distinct from caller.provolatile
       or rewritten.proparallel is distinct from caller.proparallel
       or rewritten.proleakproof is distinct from caller.proleakproof
       or rewritten.proisstrict is distinct from caller.proisstrict
       or rewritten.proconfig is distinct from caller.proconfig then
      raise exception
        'Issue #422 bridge changed caller identity or execution attributes for OID %',
        caller.oid;
    end if;

    rewritten_count := rewritten_count + 1;
  end loop;

  if rewritten_count <> 5 then
    raise exception
      'Issue #422 bridge expected to rewrite five callers, rewrote %',
      rewritten_count;
  end if;

  if pg_catalog.to_regprocedure(
       'private.lcia_scope_closure_bundle_binding_matches(public.lcia_scope_closure_checks,public.worker_job_artifacts)'
     ) is distinct from public_helper_oid
     or pg_catalog.to_regprocedure(
       'public.lcia_scope_closure_bundle_binding_matches(public.lcia_scope_closure_checks,public.worker_job_artifacts)'
     ) is not null then
    raise exception 'Issue #422 bridge did not preserve the helper OID in private';
  end if;

  if exists (
    select 1
    from pg_catalog.pg_proc as routine
    join pg_catalog.pg_namespace as namespace
      on namespace.oid = routine.pronamespace
    where namespace.nspname = 'public'
      and routine.prosrc
        like '%public.lcia_scope_closure_bundle_binding_matches%'
  ) then
    raise exception 'Issue #422 bridge left a public helper reference behind';
  end if;

  select count(*)
  into public_function_count
  from pg_catalog.pg_proc as routine
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = routine.pronamespace
  where namespace.nspname = 'public'
    and routine.prokind = 'f';

  if public_function_count <> 333 then
    raise exception
      'Issue #422 bridge did not restore the 333-function cutover manifest, found %',
      public_function_count;
  end if;
end
$bridge_production_reuse_binding_cutover$;

commit;
