begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, pg_catalog, public;
select no_plan();

select is((select count(*)::integer from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='private' and c.relkind='r' and c.relname in
    ('lca_active_snapshots','lca_network_snapshots','lca_snapshot_artifacts')), 3,
  'three LCA snapshot relations are private physical tables');
select is((select count(*)::integer from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind='v' and coalesce(c.reloptions,'{}') @> array['security_invoker=true']
    and c.relname in ('lca_active_snapshots','lca_network_snapshots','lca_snapshot_artifacts')), 3,
  'three public compatibility relations are security-invoker views');
select is((select count(*)::integer from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relkind in ('r','p') and c.relname in
    ('lca_active_snapshots','lca_network_snapshots','lca_snapshot_artifacts')), 0,
  'public contains no LCA snapshot physical relation');

select is(
  (select jsonb_agg(jsonb_build_array(c.relname,a.attname,a.attnum,
      format_type(a.atttypid,a.atttypmod)) order by c.relname,a.attnum)
   from pg_class c join pg_namespace n on n.oid=c.relnamespace
   join pg_attribute a on a.attrelid=c.oid and a.attnum>0 and not a.attisdropped
   where n.nspname='private' and c.relname in
     ('lca_active_snapshots','lca_network_snapshots','lca_snapshot_artifacts')),
  (select jsonb_agg(jsonb_build_array(c.relname,a.attname,a.attnum,
      format_type(a.atttypid,a.atttypmod)) order by c.relname,a.attnum)
   from pg_class c join pg_namespace n on n.oid=c.relnamespace
   join pg_attribute a on a.attrelid=c.oid and a.attnum>0 and not a.attisdropped
   where n.nspname='public' and c.relname in
     ('lca_active_snapshots','lca_network_snapshots','lca_snapshot_artifacts')),
  'public compatibility columns exactly preserve private names, order, and types');

select is((select count(*)::integer from pg_constraint con
  join pg_class c on c.oid=con.conrelid join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='private' and c.relname in
    ('lca_active_snapshots','lca_network_snapshots','lca_snapshot_artifacts')), 13,
  'all owned snapshot constraints remain attached to private physical OIDs');
select is((select count(*)::integer from pg_index i join pg_class c on c.oid=i.indrelid
  join pg_namespace n on n.oid=c.relnamespace where n.nspname='private' and c.relname in
    ('lca_active_snapshots','lca_network_snapshots','lca_snapshot_artifacts')), 11,
  'all snapshot indexes remain attached to private physical OIDs');
select is((select count(*)::integer from pg_trigger t join pg_class c on c.oid=t.tgrelid
  join pg_namespace n on n.oid=c.relnamespace where not t.tgisinternal and n.nspname='private'
    and c.relname in ('lca_active_snapshots','lca_network_snapshots','lca_snapshot_artifacts')), 2,
  'both closure guards remain attached to private physical OIDs');
select is((select count(*)::integer from pg_policy p join pg_class c on c.oid=p.polrelid
  join pg_namespace n on n.oid=c.relnamespace where n.nspname='private' and c.relname in
    ('lca_active_snapshots','lca_network_snapshots','lca_snapshot_artifacts')), 3,
  'three service-role RLS policies remain on private canonical tables');
select is((select count(*)::integer from pg_class c join pg_namespace n on n.oid=c.relnamespace
  where n.nspname='private' and c.relname in
    ('lca_active_snapshots','lca_network_snapshots','lca_snapshot_artifacts')
    and c.relrowsecurity and not c.relforcerowsecurity), 3,
  'private canonical tables preserve enabled non-forced RLS');
select is((select count(*)::integer from pg_publication_tables
  where schemaname in ('public','private') and tablename in
    ('lca_active_snapshots','lca_network_snapshots','lca_snapshot_artifacts')), 0,
  'snapshot family preserves the no-Realtime-publication baseline');

select ok(not has_schema_privilege('anon','private','USAGE')
  and not has_schema_privilege('authenticated','private','USAGE'),
  'browser roles cannot resolve private');
select ok(has_table_privilege('service_role','private.lca_active_snapshots','SELECT,INSERT,UPDATE,DELETE')
  and has_table_privilege('service_role','private.lca_network_snapshots','SELECT,INSERT,UPDATE,DELETE')
  and has_table_privilege('service_role','private.lca_snapshot_artifacts','SELECT,INSERT,UPDATE,DELETE'),
  'service role retains canonical snapshot DML');
select ok(not has_table_privilege('anon','public.lca_network_snapshots','SELECT')
  and not has_table_privilege('authenticated','public.lca_network_snapshots','SELECT'),
  'browser roles cannot use public compatibility');
select ok(has_table_privilege('service_role','public.lca_active_snapshots','SELECT,INSERT,UPDATE,DELETE')
  and has_table_privilege('service_role','public.lca_network_snapshots','SELECT,INSERT,UPDATE,DELETE')
  and has_table_privilege('service_role','public.lca_snapshot_artifacts','SELECT,INSERT,UPDATE,DELETE'),
  'legacy service paths retain compatibility DML');

select has_function('api','lca_snapshot_active_read_v1',array['text'],
  'active read facade exists');
select has_function('api','lca_snapshot_scope_read_v1',array['uuid'],
  'exact snapshot scope read facade exists');
select has_function('api','lca_snapshot_resolve_v1',array['text','jsonb'],
  'scoped resolve facade exists');
select has_function('api','lca_snapshot_artifact_read_v1',array['uuid'],
  'artifact readback facade exists');
select has_function('api','lca_snapshot_artifact_latest_v1',array[]::text[],
  'latest artifact facade exists');
select has_function('api','cmd_lca_snapshot_create_v1',array['uuid','text','jsonb','uuid'],
  'snapshot create command exists');
select is((select count(*)::integer from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='api' and p.proname in ('lca_snapshot_active_read_v1','lca_snapshot_resolve_v1',
    'lca_snapshot_scope_read_v1','lca_snapshot_artifact_read_v1',
    'lca_snapshot_artifact_latest_v1','cmd_lca_snapshot_create_v1')
    and not p.prosecdef and coalesce(p.proconfig,'{}') @> array['search_path=""']), 6,
  'all six API surfaces are fixed-search-path security invokers');
select is((select count(*)::integer from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='api' and p.proname in ('lca_snapshot_active_read_v1','lca_snapshot_resolve_v1',
    'lca_snapshot_scope_read_v1','lca_snapshot_artifact_read_v1',
    'lca_snapshot_artifact_latest_v1','cmd_lca_snapshot_create_v1')
    and has_function_privilege('service_role',p.oid,'EXECUTE')), 6,
  'service role can execute all six snapshot API surfaces');
select is((select count(*)::integer from pg_proc p join pg_namespace n on n.oid=p.pronamespace
  where n.nspname='api' and p.proname in ('lca_snapshot_active_read_v1','lca_snapshot_resolve_v1',
    'lca_snapshot_scope_read_v1','lca_snapshot_artifact_read_v1',
    'lca_snapshot_artifact_latest_v1','cmd_lca_snapshot_create_v1')
    and (has_function_privilege('anon',p.oid,'EXECUTE')
      or has_function_privilege('authenticated',p.oid,'EXECUTE'))), 0,
  'browser roles cannot execute snapshot API surfaces');

set local role service_role;

insert into public.lca_network_snapshots (
  id, scope, process_filter, source_hash, status, created_by
) values (
  '37600000-0000-4000-8000-000000000001', 'full_library',
  '{"scope_manifest":{"data_scope":"all"}}', 'source-a', 'ready',
  '37600000-0000-4000-8000-000000000101'
);

insert into public.lca_snapshot_artifacts (
  snapshot_id, artifact_url, artifact_sha256, artifact_byte_size, artifact_format,
  process_count, flow_count, impact_count, a_nnz, b_nnz, c_nnz, status
) values (
  '37600000-0000-4000-8000-000000000001', 's3://issue376/a', repeat('a',64),
  10, 'snapshot-hdf5:v1', 1, 2, 3, 4, 5, 6, 'ready'
);

insert into public.lca_active_snapshots(scope,snapshot_id,source_hash)
values ('full_library','37600000-0000-4000-8000-000000000001','source-a')
on conflict(scope) do update set snapshot_id=excluded.snapshot_id;

insert into public.lca_network_snapshots (
  id, scope, process_filter, source_hash, status, created_by
) values (
  '37600000-0000-4000-8000-000000000001', 'full_library',
  '{"scope_manifest":{"data_scope":"all"},"retry":true}', 'source-b', 'ready',
  '37600000-0000-4000-8000-000000000101'
) on conflict(id) do update set
  process_filter=excluded.process_filter,
  source_hash=excluded.source_hash,
  updated_at=now();

insert into public.lca_snapshot_artifacts (
  snapshot_id, artifact_url, artifact_sha256, artifact_byte_size, artifact_format,
  process_count, flow_count, impact_count, a_nnz, b_nnz, c_nnz, status
) values (
  '37600000-0000-4000-8000-000000000001', 's3://issue376/retry', repeat('b',64),
  11, 'snapshot-hdf5:v1', 2, 3, 4, 5, 6, 7, 'ready'
) on conflict(snapshot_id,artifact_format) do update set
  artifact_url=excluded.artifact_url,
  artifact_sha256=excluded.artifact_sha256,
  artifact_byte_size=excluded.artifact_byte_size,
  process_count=excluded.process_count,
  updated_at=now();

select is((select count(*)::integer from private.lca_network_snapshots
  where id='37600000-0000-4000-8000-000000000001'), 1,
  'public legacy insert writes the private single source');
select is((select snapshot_id from api.lca_snapshot_active_read_v1('full_library')),
  '37600000-0000-4000-8000-000000000001'::uuid,
  'active read facade resolves the legacy write');
select is((select process_filter->>'retry' from api.lca_snapshot_scope_read_v1(
  '37600000-0000-4000-8000-000000000001')), 'true',
  'exact-id scope read preserves draft/ready-independent Edge semantics');
select is((select source_hash from private.lca_network_snapshots
  where id='37600000-0000-4000-8000-000000000001'), 'source-b',
  'public network compatibility preserves ON CONFLICT(id) update semantics');
select is((select artifact_url from private.lca_snapshot_artifacts
  where snapshot_id='37600000-0000-4000-8000-000000000001'), 's3://issue376/retry',
  'public artifact compatibility preserves composite ON CONFLICT update semantics');
select is((select count(*)::integer from api.lca_snapshot_resolve_v1(
  'full_library','{"scope_manifest":{"data_scope":"all"}}'::jsonb)), 1,
  'scoped resolve returns the ready matching snapshot');
select is((select count(*)::integer from api.lca_snapshot_resolve_v1(
  'prod','{"scope_manifest":{"data_scope":"all"}}'::jsonb)), 1,
  'scoped resolve preserves the Edge default prod scope');
select is((select count(*)::integer from api.lca_snapshot_resolve_v1(
  'tenant-custom','{"scope_manifest":{"data_scope":"all"}}'::jsonb)), 1,
  'scoped resolve preserves arbitrary existing custom scope semantics');
select throws_ok(
  $$select * from api.lca_snapshot_resolve_v1('', '{}'::jsonb)$$,
  '22023', 'invalid_snapshot_resolve_request',
  'scoped resolve rejects only an empty scope');
select is((select artifact_url from api.lca_snapshot_artifact_read_v1(
  '37600000-0000-4000-8000-000000000001')),
  's3://issue376/retry', 'artifact readback returns the latest ready artifact');
select is((select snapshot_id from api.lca_snapshot_artifact_latest_v1()),
  '37600000-0000-4000-8000-000000000001'::uuid,
  'latest artifact resolve returns the ready snapshot');

select is(
  api.cmd_lca_snapshot_create_v1(
    '37600000-0000-4000-8000-000000000002','full_library',
    '{"scope_manifest":{"data_scope":"all"}}'::jsonb,
    '37600000-0000-4000-8000-000000000101'
  )->>'created', 'true', 'create command inserts once');
select is(
  api.cmd_lca_snapshot_create_v1(
    '37600000-0000-4000-8000-000000000002','full_library',
    '{"scope_manifest":{"data_scope":"all"}}'::jsonb,
    '37600000-0000-4000-8000-000000000101'
  )->>'created', 'false', 'create command retry is idempotent');
select is((select count(*)::integer from private.lca_network_snapshots
  where id='37600000-0000-4000-8000-000000000002'), 1,
  'create retry leaves one canonical row');

reset role;
select * from finish();
rollback;
