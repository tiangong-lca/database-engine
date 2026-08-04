begin;

create extension if not exists pgtap with schema extensions;

select plan(43);

select has_table('private', 'lca_snapshot_gc_runs',
  'snapshot GC run audit canonical table is private');
select has_table('private', 'lca_snapshot_gc_run_items',
  'snapshot GC item audit canonical table is private');
select has_view('public', 'lca_snapshot_gc_runs',
  'snapshot GC runs retain a public compatibility view');
select has_view('public', 'lca_snapshot_gc_run_items',
  'snapshot GC items retain a public compatibility view');

select is(
  (select reloptions from pg_catalog.pg_class
   where oid = 'public.lca_snapshot_gc_runs'::pg_catalog.regclass),
  array['security_invoker=true']::text[],
  'runs compatibility view is security invoker'
);
select is(
  (select reloptions from pg_catalog.pg_class
   where oid = 'public.lca_snapshot_gc_run_items'::pg_catalog.regclass),
  array['security_invoker=true']::text[],
  'items compatibility view is security invoker'
);
select is(
  (select is_updatable from information_schema.views
   where table_schema = 'public' and table_name = 'lca_snapshot_gc_runs'),
  'YES',
  'runs compatibility view is automatically updatable'
);
select is(
  (select is_insertable_into from information_schema.views
   where table_schema = 'public' and table_name = 'lca_snapshot_gc_runs'),
  'YES',
  'runs compatibility view is automatically insertable'
);
select is(
  (select is_updatable from information_schema.views
   where table_schema = 'public'
     and table_name = 'lca_snapshot_gc_run_items'),
  'YES',
  'items compatibility view is automatically updatable'
);
select is(
  (select is_insertable_into from information_schema.views
   where table_schema = 'public'
     and table_name = 'lca_snapshot_gc_run_items'),
  'YES',
  'items compatibility view is automatically insertable'
);

select is(
  (select count(*)::integer from pg_catalog.pg_class relation
   join pg_catalog.pg_namespace namespace
     on namespace.oid = relation.relnamespace
   where namespace.nspname = 'public'
     and relation.relname in (
       'lca_snapshot_gc_runs', 'lca_snapshot_gc_run_items'
     ) and relation.relkind = 'r'),
  0,
  'no public physical snapshot GC audit table remains'
);

select ok(
  exists (
    select 1 from pg_catalog.pg_constraint constraint_row
    where constraint_row.conrelid =
        'private.lca_snapshot_gc_run_items'::pg_catalog.regclass
      and constraint_row.conname = 'lca_snapshot_gc_run_items_run_id_fkey'
      and constraint_row.confrelid =
        'private.lca_snapshot_gc_runs'::pg_catalog.regclass
      and constraint_row.confdeltype = 'c'
  ),
  'items FK targets private runs with ON DELETE CASCADE'
);
select is(
  (select count(*)::integer from pg_catalog.pg_index
   where indrelid =
     'private.lca_snapshot_gc_run_items'::pg_catalog.regclass),
  4,
  'items retain all four indexes'
);
select is(
  (select count(*)::integer from pg_catalog.pg_index
   where indrelid = 'private.lca_snapshot_gc_runs'::pg_catalog.regclass),
  2,
  'runs retain both indexes'
);
select ok(
  (select relrowsecurity and not relforcerowsecurity
   from pg_catalog.pg_class
   where oid = 'private.lca_snapshot_gc_runs'::pg_catalog.regclass),
  'runs retain enabled non-forced RLS'
);
select ok(
  (select relrowsecurity and not relforcerowsecurity
   from pg_catalog.pg_class
   where oid = 'private.lca_snapshot_gc_run_items'::pg_catalog.regclass),
  'items retain enabled non-forced RLS'
);
select is(
  (select count(*)::integer from pg_catalog.pg_policy policy
   where policy.polrelid in (
     'private.lca_snapshot_gc_runs'::pg_catalog.regclass,
     'private.lca_snapshot_gc_run_items'::pg_catalog.regclass
   ) and policy.polname like '%service_role_all'),
  2,
  'both original service-role policies remain'
);
select is(
  (select count(*)::integer from pg_catalog.pg_policy policy
   where policy.polrelid in (
     'private.lca_snapshot_gc_runs'::pg_catalog.regclass,
     'private.lca_snapshot_gc_run_items'::pg_catalog.regclass
   ) and policy.polname like '%worker_runtime_all'),
  2,
  'both dedicated Worker RLS policies exist'
);

select ok(
  pg_catalog.has_table_privilege(
    'lca_worker_runtime', 'private.lca_snapshot_gc_runs',
    'SELECT,INSERT,UPDATE'
  ),
  'Worker role has exact run-table DML needed by snapshot retention'
);
select ok(
  pg_catalog.has_table_privilege(
    'lca_worker_runtime', 'private.lca_snapshot_gc_run_items',
    'SELECT,INSERT,UPDATE'
  ),
  'Worker role has exact item-table DML needed by snapshot retention'
);
select ok(
  not pg_catalog.has_table_privilege(
    'lca_worker_runtime', 'private.lca_snapshot_gc_runs', 'DELETE'
  ) and not pg_catalog.has_table_privilege(
    'lca_worker_runtime', 'private.lca_snapshot_gc_run_items', 'DELETE'
  ),
  'Worker role cannot delete audit rows'
);
select ok(
  not pg_catalog.has_schema_privilege('anon', 'private', 'USAGE'),
  'anonymous role has no private schema usage'
);
select ok(
  not pg_catalog.has_schema_privilege('authenticated', 'private', 'USAGE'),
  'authenticated role has no private schema usage'
);
select ok(
  not pg_catalog.has_table_privilege(
    'anon', 'public.lca_snapshot_gc_runs', 'SELECT'
  ) and not pg_catalog.has_table_privilege(
    'anon', 'public.lca_snapshot_gc_run_items', 'SELECT'
  ),
  'anonymous role cannot read compatibility views'
);
select ok(
  not pg_catalog.has_table_privilege(
    'authenticated', 'public.lca_snapshot_gc_runs', 'SELECT'
  ) and not pg_catalog.has_table_privilege(
    'authenticated', 'public.lca_snapshot_gc_run_items', 'SELECT'
  ),
  'authenticated role cannot read compatibility views'
);
select ok(
  pg_catalog.has_table_privilege(
    'service_role', 'public.lca_snapshot_gc_runs', 'SELECT,INSERT,UPDATE,DELETE'
  ) and pg_catalog.has_table_privilege(
    'service_role', 'public.lca_snapshot_gc_run_items',
    'SELECT,INSERT,UPDATE,DELETE'
  ),
  'service role retains writable compatibility access'
);
select ok(
  pg_catalog.has_table_privilege(
    'api_internal_executor', 'public.lca_snapshot_gc_runs', 'SELECT'
  ) and not pg_catalog.has_table_privilege(
    'api_internal_executor', 'public.lca_snapshot_gc_runs', 'INSERT'
  ),
  'API executor retains read-only compatibility access'
);

select ok(
  pg_catalog.pg_get_functiondef(
    'util.preview_lca_snapshot_retention(interval,interval,timestamp with time zone)'::pg_catalog.regprocedure
  ) like '%private.lca_active_snapshots%'
  and pg_catalog.pg_get_functiondef(
    'util.preview_lca_snapshot_retention(interval,interval,timestamp with time zone)'::pg_catalog.regprocedure
  ) like '%private.lca_network_snapshots%'
  and pg_catalog.pg_get_functiondef(
    'util.preview_lca_snapshot_retention(interval,interval,timestamp with time zone)'::pg_catalog.regprocedure
  ) like '%private.lca_snapshot_artifacts%',
  'retention preview uses private snapshot canonical tables'
);
select ok(
  pg_catalog.pg_get_functiondef(
    'util.list_lca_snapshot_gc_candidates_without_closure_protection(interval,interval,timestamp with time zone,integer,integer,bigint)'::pg_catalog.regprocedure
  ) like '%private.lca_active_snapshots%'
  and pg_catalog.pg_get_functiondef(
    'util.list_lca_snapshot_gc_candidates_without_closure_protection(interval,interval,timestamp with time zone,integer,integer,bigint)'::pg_catalog.regprocedure
  ) like '%private.lca_network_snapshots%'
  and pg_catalog.pg_get_functiondef(
    'util.list_lca_snapshot_gc_candidates_without_closure_protection(interval,interval,timestamp with time zone,integer,integer,bigint)'::pg_catalog.regprocedure
  ) like '%private.lca_snapshot_artifacts%',
  'candidate helper uses private snapshot canonical tables'
);
select ok(
  pg_catalog.pg_get_functiondef(
    'util.preview_lca_snapshot_retention(interval,interval,timestamp with time zone)'::pg_catalog.regprocedure
  ) not like '%public.lca_active_snapshots%'
  and pg_catalog.pg_get_functiondef(
    'util.list_lca_snapshot_gc_candidates_without_closure_protection(interval,interval,timestamp with time zone,integer,integer,bigint)'::pg_catalog.regprocedure
  ) not like '%public.lca_active_snapshots%',
  'retention helpers no longer depend on public snapshot compatibility'
);

select lives_ok($sql$
  insert into public.lca_snapshot_gc_runs (
    id, mode, status, as_of, diagnostics
  ) values (
    '41400000-0000-4000-8000-000000000001',
    'dry_run', 'running', '2026-08-04 00:00:00+00', '{"issue":414}'
  )
$sql$, 'old Worker path can insert a run through the public view');
select is(
  (select count(*)::integer from private.lca_snapshot_gc_runs
   where id = '41400000-0000-4000-8000-000000000001'),
  1,
  'public insert writes the one private fact row'
);
select lives_ok($sql$
  insert into public.lca_snapshot_gc_run_items (
    id, run_id, candidate_type, bucket_id, object_name, reason
  ) values (
    '41400000-0000-4000-8000-000000000002',
    '41400000-0000-4000-8000-000000000001',
    'orphan_storage_directory', 'issue-414', 'fixture/object', 'fixture'
  )
$sql$, 'old Worker path can insert an item through the public view');
select lives_ok($sql$
  update public.lca_snapshot_gc_run_items
  set action_status = 'dry_run', updated_at = '2026-08-04 00:01:00+00'
  where id = '41400000-0000-4000-8000-000000000002'
$sql$, 'old Worker path can update an item through the public view');
select lives_ok($sql$
  update public.lca_snapshot_gc_runs
  set status = 'succeeded', finished_at = '2026-08-04 00:02:00+00'
  where id = '41400000-0000-4000-8000-000000000001'
$sql$, 'old Worker path can finalize a run through the public view');
select lives_ok($sql$
  delete from public.lca_snapshot_gc_runs
  where id = '41400000-0000-4000-8000-000000000001'
$sql$, 'service compatibility path can delete the run');
select is(
  (select count(*)::integer from private.lca_snapshot_gc_run_items
   where run_id = '41400000-0000-4000-8000-000000000001'),
  0,
  'private FK cascade removes child audit items'
);
select is(
  (select count(*) from public.lca_snapshot_gc_runs),
  (select count(*) from private.lca_snapshot_gc_runs),
  'public runs view and private table have exact row parity'
);
select is(
  (select count(*) from public.lca_snapshot_gc_run_items),
  (select count(*) from private.lca_snapshot_gc_run_items),
  'public items view and private table have exact row parity'
);
select ok(
  pg_catalog.pg_get_viewdef(
    'public.lca_snapshot_gc_runs'::pg_catalog.regclass, true
  ) not like '%*%'
  and pg_catalog.pg_get_viewdef(
    'public.lca_snapshot_gc_run_items'::pg_catalog.regclass, true
  ) not like '%*%',
  'compatibility views use explicit columns'
);
select is(
  pg_catalog.obj_description(
    'public.lca_snapshot_gc_runs'::pg_catalog.regclass, 'pg_class'
  ),
  'Issue #414 Expand compatibility view; canonical=private.lca_snapshot_gc_runs; fallback=none; remove only after family runtime/static/owner zero, burn-in, and Contract approval.',
  'runs compatibility removal gate is documented'
);
select is(
  pg_catalog.obj_description(
    'public.lca_snapshot_gc_run_items'::pg_catalog.regclass, 'pg_class'
  ),
  'Issue #414 Expand compatibility view; canonical=private.lca_snapshot_gc_run_items; fallback=none; remove only after family runtime/static/owner zero, burn-in, and Contract approval.',
  'items compatibility removal gate is documented'
);
select is(
  (select count(*)::integer
   from pg_catalog.pg_publication publication
   left join pg_catalog.pg_publication_rel publication_relation
     on publication_relation.prpubid = publication.oid
    and publication_relation.prrelid in (
      'private.lca_snapshot_gc_runs'::pg_catalog.regclass,
      'private.lca_snapshot_gc_run_items'::pg_catalog.regclass
    )
   where publication.puballtables
      or publication_relation.prrelid is not null),
  0,
  'snapshot GC audit tables remain outside all publications'
);

select * from finish();
rollback;
