begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, util;

select plan(17);

select has_table('util', 'app_runtime_config', 'runtime config is owned by the util schema');
select col_is_pk('util', 'app_runtime_config', 'config_key', 'config key is the primary key');

select ok(
  not has_table_privilege('anon', 'util.app_runtime_config', 'SELECT')
    and not has_table_privilege('authenticated', 'util.app_runtime_config', 'SELECT')
    and not has_table_privilege('service_role', 'util.app_runtime_config', 'SELECT'),
  'external roles cannot read the runtime config table directly'
);

select ok(
  not has_table_privilege('anon', 'util.app_runtime_config', 'UPDATE')
    and not has_table_privilege('authenticated', 'util.app_runtime_config', 'UPDATE')
    and not has_table_privilege('service_role', 'util.app_runtime_config', 'UPDATE'),
  'external roles cannot mutate the runtime config table directly'
);

select has_function('api', 'qry_system_status', array[]::text[], 'system status facade exists');

select is(
  (
    select routine.prosecdef
    from pg_proc as routine
    where routine.oid = 'api.qry_system_status()'::regprocedure
  ),
  true,
  'system status facade is SECURITY DEFINER'
);

select is(
  (
    select routine.proconfig
    from pg_proc as routine
    where routine.oid = 'api.qry_system_status()'::regprocedure
  ),
  array['search_path=""']::text[],
  'system status facade has an empty fixed search path'
);

select ok(
  has_function_privilege('anon', 'api.qry_system_status()', 'EXECUTE'),
  'anonymous browser startup can read system status'
);

select ok(
  has_function_privilege('authenticated', 'api.qry_system_status()', 'EXECUTE'),
  'authenticated browser startup can read system status'
);

select ok(
  not has_function_privilege('service_role', 'api.qry_system_status()', 'EXECUTE'),
  'service role receives no unnecessary system status grant'
);

select results_eq(
  $$
    select capability_id, allow_anon, allow_authenticated, allow_service_role
    from private.api_capability_grants
    where routine_identity = 'api.qry_system_status()'
  $$,
  $$values ('NX-RUNTIME-01'::text, true, true, false)$$,
  'the capability manifest exactly describes the facade grants'
);

select is(
  api.qry_system_status() ->> 'phase',
  'normal',
  'the seeded production status is normal'
);

set local role anon;
select is(
  api.qry_system_status() ->> 'schemaVersion',
  '1',
  'anonymous callers receive the versioned public payload'
);
reset role;

update util.app_runtime_config
set config_value = jsonb_build_object(
      'schemaVersion', 1,
      'phase', 'maintenance',
      'reason', 'release_upgrade',
      'targetVersion', '0.0.71',
      'estimatedEndAt', '2026-08-14T10:30:00+08:00',
      'releaseId', 'release-20260814'
    ),
    updated_at = '2026-08-14T09:00:00+08:00'::timestamptz
where config_key = 'tiangong-lca-next.production.system-status';

select is(
  api.qry_system_status() ->> 'targetVersion',
  '0.0.71',
  'the facade returns the active release target'
);

select is(
  api.qry_system_status() ->> 'updatedAt',
  to_jsonb('2026-08-14T09:00:00+08:00'::timestamptz) #>> '{}',
  'the facade appends the authoritative update timestamp'
);

select throws_ok(
  $$
    update util.app_runtime_config
    set config_value = '{"schemaVersion":1,"phase":"broken"}'::jsonb
    where config_key = 'tiangong-lca-next.production.system-status'
  $$,
  '23514',
  null,
  'an unsupported phase cannot be persisted'
);

select throws_ok(
  $$
    insert into util.app_runtime_config (config_key, config_value)
    values ('missing-required-fields', '{}'::jsonb)
  $$,
  '23514',
  null,
  'required schema version and phase fields cannot be omitted'
);

select * from finish();
rollback;
