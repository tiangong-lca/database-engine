-- Browser-visible system maintenance state is stored outside exposed schemas
-- and projected through one fixed, read-only API facade.

set lock_timeout = '5s';
set statement_timeout = '120s';

create table util.app_runtime_config (
  config_key text primary key,
  config_value jsonb not null,
  updated_at timestamptz not null default statement_timestamp(),
  constraint app_runtime_config_key_not_blank_check check (btrim(config_key) <> ''),
  constraint app_runtime_config_value_object_check check (
    jsonb_typeof(config_value) = 'object'
  ),
  constraint app_runtime_config_schema_version_check check (
    coalesce(config_value -> 'schemaVersion' = '1'::jsonb, false)
  ),
  constraint app_runtime_config_phase_check check (
    coalesce(config_value ->> 'phase' in ('normal', 'maintenance', 'verifying'), false)
  ),
  constraint app_runtime_config_reason_check check (
    not (config_value ? 'reason')
    or config_value -> 'reason' = 'null'::jsonb
    or config_value ->> 'reason' in ('release_upgrade', 'emergency')
  ),
  constraint app_runtime_config_target_version_check check (
    not (config_value ? 'targetVersion')
    or config_value -> 'targetVersion' = 'null'::jsonb
    or jsonb_typeof(config_value -> 'targetVersion') = 'string'
  ),
  constraint app_runtime_config_estimated_end_at_check check (
    not (config_value ? 'estimatedEndAt')
    or config_value -> 'estimatedEndAt' = 'null'::jsonb
    or (
      jsonb_typeof(config_value -> 'estimatedEndAt') = 'string'
      and config_value ->> 'estimatedEndAt'
        ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}([.]\d+)?(Z|[+-]\d{2}:\d{2})$'
    )
  ),
  constraint app_runtime_config_release_id_check check (
    not (config_value ? 'releaseId')
    or config_value -> 'releaseId' = 'null'::jsonb
    or jsonb_typeof(config_value -> 'releaseId') = 'string'
  ),
  constraint app_runtime_config_active_reason_check check (
    coalesce(
      config_value ->> 'phase' = 'normal'
      or config_value ->> 'reason' in ('release_upgrade', 'emergency'),
      false
    )
  )
);

alter table util.app_runtime_config owner to postgres;
revoke all on table util.app_runtime_config from public, anon, authenticated, service_role;

comment on table util.app_runtime_config is
  'Operational runtime configuration. Values are not Data API relations and are exposed only by fixed api facades.';
comment on column util.app_runtime_config.config_key is
  'Stable application/environment/config identity.';
comment on column util.app_runtime_config.config_value is
  'Versioned JSON payload validated by database constraints.';

insert into util.app_runtime_config (config_key, config_value)
values (
  'tiangong-lca-next.production.system-status',
  jsonb_build_object(
    'schemaVersion', 1,
    'phase', 'normal',
    'reason', null,
    'targetVersion', null,
    'estimatedEndAt', null,
    'releaseId', null
  )
)
on conflict (config_key) do nothing;

create or replace function api.qry_system_status()
returns jsonb
language sql
stable
security definer
set search_path = ''
as $function$
  select coalesce(
    (
      select config.config_value || jsonb_build_object('updatedAt', config.updated_at)
      from util.app_runtime_config as config
      where config.config_key = 'tiangong-lca-next.production.system-status'
    ),
    jsonb_build_object(
      'schemaVersion', 1,
      'phase', 'normal',
      'reason', null,
      'targetVersion', null,
      'estimatedEndAt', null,
      'releaseId', null,
      'updatedAt', null
    )
  );
$function$;

alter function api.qry_system_status() owner to postgres;
revoke all on function api.qry_system_status() from public, anon, authenticated, service_role;
grant execute on function api.qry_system_status() to anon, authenticated;

comment on function api.qry_system_status() is
  'Returns the public browser startup status for tiangong-lca-next without exposing the operational config table.';

insert into private.api_capability_grants (
  routine_identity,
  capability_id,
  allow_anon,
  allow_authenticated,
  allow_service_role
)
values (
  'api.qry_system_status()',
  'NX-RUNTIME-01',
  true,
  true,
  false
)
on conflict (routine_identity) do update
set capability_id = excluded.capability_id,
    allow_anon = excluded.allow_anon,
    allow_authenticated = excluded.allow_authenticated,
    allow_service_role = excluded.allow_service_role;
