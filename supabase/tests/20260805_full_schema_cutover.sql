begin;

create extension if not exists pgtap with schema extensions;

select plan(23);

select is(
  (
    select array_agg(class.relname::text order by class.relname)
    from pg_class class
    join pg_namespace namespace on namespace.oid = class.relnamespace
    where namespace.nspname = 'public'
      and class.relkind in ('r', 'p')
  ),
  array[
    'contacts', 'flowproperties', 'flows', 'ilcd', 'lciamethods',
    'lifecyclemodels', 'processes', 'sources', 'unitgroups'
  ]::text[],
  'public contains exactly the nine core entity tables'
);

select is(
  (
    select count(*)
    from pg_class class
    join pg_namespace namespace on namespace.oid = class.relnamespace
    where namespace.nspname = 'public'
      and class.relkind in ('v', 'm')
  ),
  0::bigint,
  'public contains no views'
);

select is(
  (
    select count(*)
    from pg_proc routine
    join pg_namespace namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'public'
  ),
  0::bigint,
  'public contains no routines'
);

select is(
  (
    select count(*)
    from pg_class class
    join pg_namespace namespace on namespace.oid = class.relnamespace
    where namespace.nspname = 'public'
      and class.relkind = 'S'
  ),
  0::bigint,
  'public contains no standalone sequences'
);

select ok(
  to_regtype('public.filtered_row') is null
    and to_regtype('api.filtered_row') is not null,
  'standalone API composite type moved from public to api'
);

select is(
  (
    select count(*)
    from pg_proc routine
    join pg_namespace namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'api'
      and routine.prokind = 'f'
  ),
  239::bigint,
  'api contains the cutover functions plus all reviewed consumer facades'
);

select is(
  (
    select count(*)
    from pg_proc routine
    join pg_namespace namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'private'
      and routine.prokind = 'f'
  ),
  196::bigint,
  'private contains existing helpers plus all reviewed internal functions'
);

select ok(
  to_regclass('private.users') is not null
    and to_regclass('private.reviews') is not null
    and to_regclass('private.worker_jobs') is not null
    and to_regclass('private.lcia_scope_closure_checks') is not null,
  'representative non-core state tables moved to private'
);

select ok(
  to_regclass('api.worker_job_domain_refs') is not null
    and to_regclass('private.worker_domain_traceability_cutoffs') is not null
    and to_regclass('util.worker_domain_traceability_violations') is not null
    and to_regclass('util.worker_legacy_lifecycle_audit') is not null
    and to_regclass('util.worker_legacy_table_retirement_blockers') is not null,
  'all five public views moved to their reviewed schemas'
);

select ok(
  to_regclass('private.lcia_scope_closure_publication_epoch_seq') is not null,
  'standalone publication epoch sequence moved to private'
);

select is(
  (
    select count(*)
    from pg_trigger trigger_record
    where not trigger_record.tgisinternal
  ),
  106::bigint,
  'all application triggers remain present'
);

select is(
  (
    select count(*)
    from pg_policy
  ),
  61::bigint,
  'all RLS policies remain present'
);

select is(
  (
    select count(*)
    from pg_constraint constraint_record
    where constraint_record.connamespace in (
      'public'::regnamespace,
      'api'::regnamespace,
      'private'::regnamespace,
      'util'::regnamespace
    )
  ),
  446::bigint,
  'all application constraints remain present'
);

select is(
  (
    select count(*)
    from pg_constraint constraint_record
    where constraint_record.connamespace in (
      'public'::regnamespace,
      'api'::regnamespace,
      'private'::regnamespace,
      'util'::regnamespace
    )
      and not constraint_record.convalidated
  ),
  3::bigint,
  'migration does not introduce additional unvalidated constraints'
);

select is(
  (
    select count(*)
    from pg_class class
    join pg_namespace namespace on namespace.oid = class.relnamespace
    where namespace.nspname in ('public', 'private')
      and class.relkind in ('r', 'p')
      and class.relrowsecurity
  ),
  55::bigint,
  'RLS enablement is preserved across moved tables'
);

select ok(
  not has_schema_privilege('anon', 'private', 'USAGE')
    and not has_schema_privilege('anon', 'util', 'USAGE')
    and not has_schema_privilege('authenticated', 'util', 'USAGE')
    and not has_schema_privilege('anon', 'archive', 'USAGE')
    and not has_schema_privilege('authenticated', 'archive', 'USAGE'),
  'browser roles cannot enter non-RLS internal schemas'
);

select ok(
  has_schema_privilege('anon', 'api', 'USAGE')
    and has_schema_privilege('authenticated', 'api', 'USAGE')
    and has_schema_privilege('service_role', 'api', 'USAGE'),
  'Data API roles can enter the api schema'
);

select ok(
  has_schema_privilege('authenticated', 'private', 'USAGE')
    and has_table_privilege('authenticated', 'private.roles', 'SELECT')
    and has_table_privilege('authenticated', 'private.reviews', 'SELECT')
    and not has_table_privilege('authenticated', 'private.roles', 'INSERT')
    and not has_table_privilege('authenticated', 'private.reviews', 'UPDATE'),
  'authenticated has only the private reads required by public core-table RLS'
);

select is(
  (
    select count(*)
    from pg_proc routine
    join pg_namespace namespace on namespace.oid = routine.pronamespace
    join pg_roles owner_role on owner_role.oid = routine.proowner
    where namespace.nspname = 'api'
      and routine.prosecdef
      and owner_role.rolname = 'api_internal_executor'
  ),
  20::bigint,
  'private RLS helper facades use the constrained executor'
);

select ok(
  exists (
    select 1
    from pg_roles
    where rolname = 'api_internal_executor'
      and not rolcanlogin
      and rolinherit
      and not rolbypassrls
  ),
  'API internal executor cannot log in or bypass RLS'
);

select is(
  (
    select count(*)
    from pg_proc routine
    join pg_namespace namespace on namespace.oid = routine.pronamespace
    where namespace.nspname in ('api', 'private', 'util')
      and routine.prokind = 'f'
      and pg_get_functiondef(routine.oid) ~
        'public[.](command_audit_log|comments|dataset_review_submit_|identity_center_|lca_|lcia_|notifications|reviews|roles|teams|users|worker_)'
  ),
  0::bigint,
  'stored functions contain no stale explicit references to moved public objects'
);

select ok(
  has_function_privilege(
    'authenticated',
    'api.search_flows_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text[])',
    'EXECUTE'
  ),
  'authenticated retains execute access to an API search facade'
);

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  '00000000-0000-0000-0000-000000000001',
  true
);

select lives_ok(
  $sql$
    select *
    from api.search_flows_latest(
      '',
      '{}'::jsonb,
      '{}'::jsonb,
      10,
      1,
      'all',
      null,
      null,
      null,
      array[]::text[]
    )
  $sql$,
  'authenticated API facade can traverse the private helper boundary'
);

reset role;

select * from finish();

rollback;
