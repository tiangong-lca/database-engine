begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

-- This migration is additive.  Keep the established Data Product, LCIA
-- package, and publication routines byte-for-byte and ACL-for-ACL stable.
create temporary table portal_lcia_legacy_routines_before (
  routine_identity text primary key,
  definition text not null,
  owner_name text not null,
  security_definer boolean not null,
  proconfig text[] not null,
  acl_text text not null
) on commit drop;

insert into portal_lcia_legacy_routines_before (
  routine_identity,
  definition,
  owner_name,
  security_definer,
  proconfig,
  acl_text
)
with expected(routine_identity) as (
  values
    ('api.cmd_lcia_result_build_request(text, jsonb, text, text, jsonb, text, jsonb)'),
    ('api.cmd_lcia_result_build_request_v2(text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb)'),
    ('private.cmd_lcia_result_package_mark_ready(uuid, text, uuid, uuid, uuid, jsonb, jsonb, jsonb, jsonb, text, text, jsonb)'),
    ('api.cmd_lcia_result_package_publish(uuid, text, text, jsonb)'),
    ('api.cmd_lcia_result_publication_unpublish(uuid, text, jsonb)'),
    ('api.get_lcia_result_package_preview(uuid)'),
    ('api.get_published_lcia_result_package(uuid, text, text)'),
    ('api.cmd_lca_release_prepare(uuid, text, text, text, jsonb, text, text, jsonb, text, text, jsonb)'),
    ('private.cmd_lca_release_artifacts_finalize_service(uuid, text, jsonb, text, jsonb, jsonb)'),
    ('api.cmd_lca_release_approve(uuid, text, timestamp with time zone, text, jsonb)'),
    ('api.cmd_lca_release_publish(uuid, uuid, text, text, text, text, text, jsonb)'),
    ('api.cmd_lca_release_readback_verify(uuid, text, jsonb, jsonb)'),
    ('api.cmd_lca_release_unpublish(uuid, text, jsonb)'),
    ('api.get_current_lca_release()')
)
select
  expected.routine_identity,
  pg_catalog.pg_get_functiondef(routine.oid),
  owner_role.rolname,
  routine.prosecdef,
  coalesce(routine.proconfig, '{}'::text[]),
  coalesce(routine.proacl::text, '')
from expected
join pg_catalog.pg_proc as routine
  on routine.oid = pg_catalog.to_regprocedure(expected.routine_identity)
join pg_catalog.pg_roles as owner_role
  on owner_role.oid = routine.proowner;

do $portal_lcia_legacy_snapshot_guard$
begin
  if (select count(*) from portal_lcia_legacy_routines_before) <> 14 then
    raise exception 'Portal LCIA legacy routine snapshot is incomplete';
  end if;
end
$portal_lcia_legacy_snapshot_guard$;

-- Catalog scalar helpers are intentionally owned by the constrained public
-- executor with PUBLIC revoked.  The new postgres-owned service/actor
-- definers need only these two exact pure helpers; grant no browser or
-- service-role access and restore the catalog role membership immediately.
grant portal_public_executor to postgres;
set local role portal_public_executor;
grant execute on function private.portal_canonical_decimal_v1(text)
  to postgres;
grant execute on function private.portal_timestamp_v1(timestamptz)
  to postgres;
reset role;
revoke portal_public_executor from postgres;

-- Hash contract shared with Worker.  Every field is UTF-8, prefixed by one
-- signed network-order int32 byte length; NULL is framed with -1.  Call sites
-- freeze the field order and include a record/domain marker as field zero.
create function private.portal_lcia_projection_frame_v1(variadic p_fields text[])
returns bytea
language plpgsql
stable
parallel safe
set search_path = ''
as $function$
declare
  v_field text;
  v_bytes bytea;
  v_result bytea := ''::bytea;
begin
  foreach v_field in array p_fields loop
    if v_field is null then
      v_result := v_result || pg_catalog.int4send(-1);
    else
      v_bytes := pg_catalog.convert_to(v_field, 'UTF8');
      v_result := v_result
        || pg_catalog.int4send(pg_catalog.octet_length(v_bytes))
        || v_bytes;
    end if;
  end loop;
  return v_result;
end
$function$;

create function private.portal_lcia_safe_audit_v1(p_value jsonb)
returns boolean
language sql
immutable
parallel safe
set search_path = ''
as $function$
  with recursive nodes(key_name, value) as (
    select null::text, p_value
    union all
    select child.key_name, child.value
    from nodes as parent
    cross join lateral (
      select member.key as key_name, member.value
      from jsonb_each(
        case jsonb_typeof(parent.value)
          when 'object' then parent.value
          else '{}'::jsonb
        end
      ) as member(key, value)
      union all
      select null::text, member.value
      from jsonb_array_elements(
        case jsonb_typeof(parent.value)
          when 'array' then parent.value
          else '[]'::jsonb
        end
      ) as member(value)
    ) as child
  )
  select coalesce(jsonb_typeof(p_value) = 'object', false)
    and pg_catalog.pg_column_size(p_value) <= 16384
    and not exists (
      select 1
      from nodes
      where coalesce(lower(key_name), '') ~
              '(url|uri|bucket|objectpath|storagepath|locator|credential|secret|token|authorization|cookie|password|api.?key)'
         or (
           jsonb_typeof(value) = 'string'
           and lower(value #>> '{}') ~ '(https?://|s3://|gs://)'
         )
    )
$function$;

create function private.portal_lcia_projection_sha256_fields_v1(
  variadic p_fields text[]
)
returns text
language sql
stable
parallel safe
set search_path = ''
as $function$
  select pg_catalog.encode(
    extensions.digest(
      private.portal_lcia_projection_frame_v1(variadic p_fields),
      'sha256'
    ),
    'hex'
  )
$function$;

create function private.portal_lcia_json_object_has_keys_v1(
  p_value jsonb,
  p_keys text[]
)
returns boolean
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select jsonb_typeof(p_value) = 'object'
    and (select count(*) from jsonb_object_keys(p_value)) = cardinality(p_keys)
    and not exists (
      select 1
      from jsonb_object_keys(p_value) as key(value)
      where not (key.value = any (p_keys))
    )
$function$;

create function private.portal_lcia_public_text_valid_v1(
  p_value text,
  p_max_length integer
)
returns boolean
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select p_value is not null
    and p_value = btrim(p_value)
    and length(p_value) between 1 and p_max_length
    and p_value !~ '[[:cntrl:]]'
    and lower(p_value) !~ '(https?://|s3://|gs://|file://)'
    and p_value !~ '(^|[/\\])\.\.([/\\]|$)'
$function$;

create function private.portal_lcia_localized_text_valid_v1(p_value jsonb)
returns boolean
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select jsonb_typeof(p_value) = 'array'
    and jsonb_array_length(p_value) between 1 and 64
    and (
      select count(*) = count(distinct item.value ->> 'language')
        and array_agg(item.value ->> 'language' order by item.ordinality)
          = array_agg(item.value ->> 'language'
              order by item.value ->> 'language')
      from jsonb_array_elements(p_value)
        with ordinality as item(value, ordinality)
    )
    and not exists (
      select 1
      from jsonb_array_elements(p_value) as item(value)
      where jsonb_typeof(item.value) <> 'object'
        or (select count(*) from jsonb_object_keys(item.value)) <> 2
        or not (item.value ? 'language' and item.value ? 'value')
        or jsonb_typeof(item.value -> 'language') <> 'string'
        or jsonb_typeof(item.value -> 'value') <> 'string'
        or item.value ->> 'language'
             !~ '^[a-z]{2,3}(-[a-z0-9]{2,8})*$'
        or length(item.value ->> 'language') > 35
        or item.value ->> 'value' <> btrim(item.value ->> 'value')
        or length(item.value ->> 'value') not between 1 and 4096
        or lower(item.value ->> 'value') ~ '(https?://|s3://|gs://|file://)'
    )
$function$;

create function private.portal_lcia_localized_text_frame_hex_v1(p_value jsonb)
returns text
language sql
stable
parallel safe
set search_path = ''
as $function$
  select pg_catalog.encode(
    private.portal_lcia_projection_frame_v1(
      variadic (
        array[jsonb_array_length(p_value)::text]
        || coalesce(
          (
            select array_agg(field.value order by item.ordinality, field.position)
            from jsonb_array_elements(p_value)
              with ordinality as item(value, ordinality)
            cross join lateral (
              values
                (1, item.value ->> 'language'),
                (2, item.value ->> 'value')
            ) as field(position, value)
          ),
          '{}'::text[]
        )
      )
    ),
    'hex'
  )
$function$;

create table private.portal_lcia_projection_headers (
  id uuid primary key default gen_random_uuid(),
  build_worker_job_id uuid not null
    references private.worker_jobs(id) on delete restrict,
  stage_lease_token uuid not null,
  projection_contract_version text not null,
  status text not null default 'staging',
  process_count integer not null,
  impact_count integer not null,
  expected_value_count bigint generated always as (
    process_count::bigint * impact_count::bigint
  ) stored,
  input_manifest_hash text not null,
  closure_certificate_hash text not null,
  snapshot_hash text not null,
  closure_bundle_hash text not null,
  snapshot_index_sha256 text not null,
  snapshot_build_contract_hash text not null,
  bundle_content_hash text not null,
  bundle_manifest_sha256 text not null,
  lcia_chunk_set_sha256 text not null,
  result_artifact_sha256 text not null,
  query_artifact_sha256 text not null,
  process_axis_hash text,
  impact_axis_hash text,
  value_grid_hash text,
  relation_hash text,
  content_hash text,
  failure_code text,
  failure_message text,
  created_at timestamptz not null default clock_timestamp(),
  prepared_at timestamptz,
  failed_at timestamptz,
  constraint portal_lcia_projection_headers_job_lease_uidx
    unique (build_worker_job_id, stage_lease_token),
  constraint portal_lcia_projection_headers_contract_chk check (
    projection_contract_version = 'portal.lcia-projection.v1'
  ),
  constraint portal_lcia_projection_headers_status_chk check (
    status in ('staging', 'prepared', 'failed')
  ),
  constraint portal_lcia_projection_headers_counts_chk check (
    process_count between 1 and 1000000
    and impact_count between 1 and 10000
    and process_count::bigint * impact_count::bigint <= 100000000
  ),
  constraint portal_lcia_projection_headers_hashes_chk check (
    input_manifest_hash ~ '^[0-9a-f]{64}$'
    and closure_certificate_hash ~ '^[0-9a-f]{64}$'
    and snapshot_hash ~ '^[0-9a-f]{64}$'
    and closure_bundle_hash ~ '^[0-9a-f]{64}$'
    and snapshot_index_sha256 ~ '^[0-9a-f]{64}$'
    and snapshot_build_contract_hash ~ '^[0-9a-f]{64}$'
    and bundle_content_hash ~ '^[0-9a-f]{64}$'
    and bundle_manifest_sha256 ~ '^[0-9a-f]{64}$'
    and lcia_chunk_set_sha256 ~ '^[0-9a-f]{64}$'
    and result_artifact_sha256 ~ '^[0-9a-f]{64}$'
    and query_artifact_sha256 ~ '^[0-9a-f]{64}$'
    and (process_axis_hash is null or process_axis_hash ~ '^[0-9a-f]{64}$')
    and (impact_axis_hash is null or impact_axis_hash ~ '^[0-9a-f]{64}$')
    and (value_grid_hash is null or value_grid_hash ~ '^[0-9a-f]{64}$')
    and (relation_hash is null or relation_hash ~ '^[0-9a-f]{64}$')
    and (content_hash is null or content_hash ~ '^[0-9a-f]{64}$')
  ),
  constraint portal_lcia_projection_headers_terminal_shape_chk check (
    (
      status = 'staging'
      and process_axis_hash is null
      and impact_axis_hash is null
      and value_grid_hash is null
      and relation_hash is null
      and content_hash is null
      and prepared_at is null
      and failed_at is null
      and failure_code is null
      and failure_message is null
    )
    or (
      status = 'prepared'
      and process_axis_hash is not null
      and impact_axis_hash is not null
      and value_grid_hash is not null
      and relation_hash is not null
      and content_hash is not null
      and prepared_at is not null
      and failed_at is null
      and failure_code is null
      and failure_message is null
    )
    or (
      status = 'failed'
      and failed_at is not null
      and nullif(btrim(failure_code), '') is not null
      and prepared_at is null
      and process_axis_hash is null
      and impact_axis_hash is null
      and value_grid_hash is null
      and relation_hash is null
      and content_hash is null
    )
  )
);

create table private.portal_lcia_projection_process_axis (
  projection_id uuid not null
    references private.portal_lcia_projection_headers(id) on delete restrict,
  process_index integer not null,
  process_id uuid not null,
  process_version text not null,
  reference_flow_id uuid not null,
  reference_flow_version text not null,
  reference_exchange_internal_id text not null,
  reference_flow_amount text not null,
  reference_flow_direction text not null,
  functional_unit_amount text not null,
  functional_unit_unit text not null,
  functional_unit_description jsonb not null,
  geography_code text not null,
  geography_precision text not null,
  reference_year integer not null,
  process_document_sha256 text not null,
  record_hash text not null,
  primary key (projection_id, process_index),
  constraint portal_lcia_projection_process_identity_uidx
    unique (projection_id, process_id, process_version),
  constraint portal_lcia_projection_process_index_chk check (process_index >= 0),
  constraint portal_lcia_projection_process_version_chk check (
    process_version ~ '^\d{2}\.\d{2}\.\d{3}$'
    and reference_flow_version ~ '^\d{2}\.\d{2}\.\d{3}$'
  ),
  constraint portal_lcia_projection_process_reference_chk check (
    reference_exchange_internal_id ~ '^(0|[1-9]\d{0,5})$'
    and reference_flow_amount =
      private.portal_canonical_decimal_v1(reference_flow_amount)
    and reference_flow_direction in ('input', 'output')
  ),
  constraint portal_lcia_projection_process_decimal_chk check (
    functional_unit_amount =
      private.portal_canonical_decimal_v1(functional_unit_amount)
  ),
  constraint portal_lcia_projection_process_unit_chk check (
    private.portal_lcia_public_text_valid_v1(functional_unit_unit, 128)
  ),
  constraint portal_lcia_projection_process_description_chk check (
    private.portal_lcia_localized_text_valid_v1(functional_unit_description)
  ),
  constraint portal_lcia_projection_process_geography_chk check (
    private.portal_lcia_public_text_valid_v1(geography_code, 128)
    and geography_precision in ('country', 'province', 'city', 'other', 'unknown')
  ),
  constraint portal_lcia_projection_process_year_chk check (
    reference_year between 0 and 9999
  ),
  constraint portal_lcia_projection_process_hash_chk check (
    process_document_sha256 ~ '^[0-9a-f]{64}$'
    and record_hash ~ '^[0-9a-f]{64}$'
  )
);

create table private.portal_lcia_projection_impact_axis (
  projection_id uuid not null
    references private.portal_lcia_projection_headers(id) on delete restrict,
  impact_index integer not null,
  method_id uuid not null,
  method_version text not null,
  impact_id text not null,
  impact_name jsonb not null,
  unit text not null,
  method_document_sha256 text not null,
  record_hash text not null,
  primary key (projection_id, impact_index),
  constraint portal_lcia_projection_impact_identity_uidx
    unique (projection_id, method_id, method_version, impact_id),
  constraint portal_lcia_projection_impact_index_chk check (impact_index >= 0),
  constraint portal_lcia_projection_method_version_chk check (
    method_version ~ '^\d{2}\.\d{2}\.\d{3}$'
  ),
  constraint portal_lcia_projection_impact_id_chk check (
    private.portal_lcia_public_text_valid_v1(impact_id, 512)
  ),
  constraint portal_lcia_projection_impact_name_chk check (
    private.portal_lcia_localized_text_valid_v1(impact_name)
  ),
  constraint portal_lcia_projection_impact_unit_chk check (
    private.portal_lcia_public_text_valid_v1(unit, 128)
  ),
  constraint portal_lcia_projection_impact_hash_chk check (
    method_document_sha256 ~ '^[0-9a-f]{64}$'
    and record_hash ~ '^[0-9a-f]{64}$'
  )
);

create table private.portal_lcia_projection_values (
  projection_id uuid not null
    references private.portal_lcia_projection_headers(id) on delete restrict,
  ordinal bigint not null,
  process_index integer not null,
  impact_index integer not null,
  value_text text not null,
  value_numeric numeric not null,
  record_hash text not null,
  primary key (projection_id, ordinal),
  constraint portal_lcia_projection_value_cell_uidx
    unique (projection_id, process_index, impact_index),
  constraint portal_lcia_projection_value_indexes_chk check (
    ordinal > 0 and process_index >= 0 and impact_index >= 0
  ),
  constraint portal_lcia_projection_value_decimal_chk check (
    value_text = private.portal_canonical_decimal_v1(value_text)
    and value_numeric = value_text::numeric
  ),
  constraint portal_lcia_projection_value_hash_chk check (
    record_hash ~ '^[0-9a-f]{64}$'
  ),
  constraint portal_lcia_projection_value_process_fk
    foreign key (projection_id, process_index)
    references private.portal_lcia_projection_process_axis(
      projection_id, process_index
    ) on delete restrict,
  constraint portal_lcia_projection_value_impact_fk
    foreign key (projection_id, impact_index)
    references private.portal_lcia_projection_impact_axis(
      projection_id, impact_index
    ) on delete restrict
);

create table private.portal_lcia_projection_publications (
  id uuid primary key default gen_random_uuid(),
  projection_id uuid not null
    references private.portal_lcia_projection_headers(id) on delete restrict,
  lcia_result_publication_id uuid not null unique
    references private.lcia_result_publications(id) on delete restrict,
  package_id uuid not null
    references private.lcia_result_packages(id) on delete restrict,
  package_version text not null,
  package_result_hash text not null,
  projection_content_hash text not null,
  evidence_hash text not null,
  source_published_at timestamptz not null,
  idempotency_key text not null,
  status text not null default 'finalized',
  finalized_by uuid not null,
  finalized_at timestamptz not null,
  revoked_by uuid,
  revoked_at timestamptz,
  revoke_reason text,
  constraint portal_lcia_projection_publications_status_chk check (
    status in ('finalized', 'revoked')
  ),
  constraint portal_lcia_projection_publications_hashes_chk check (
    package_result_hash ~ '^[0-9a-f]{64}$'
    and projection_content_hash ~ '^[0-9a-f]{64}$'
    and evidence_hash ~ '^[0-9a-f]{64}$'
  ),
  constraint portal_lcia_projection_publications_idempotency_chk check (
    length(btrim(idempotency_key)) between 1 and 256
  ),
  constraint portal_lcia_projection_publications_terminal_chk check (
    (
      status = 'finalized'
      and revoked_by is null
      and revoked_at is null
      and revoke_reason is null
    )
    or (
      status = 'revoked'
      and revoked_by is not null
      and revoked_at is not null
      and nullif(btrim(revoke_reason), '') is not null
    )
  )
);

create index portal_lcia_projection_publications_visibility_idx
  on private.portal_lcia_projection_publications (
    status,
    lcia_result_publication_id,
    projection_id
  )
  where status = 'finalized';

create index portal_lcia_projection_values_impact_rank_idx
  on private.portal_lcia_projection_values (
    projection_id,
    impact_index,
    value_numeric desc,
    ordinal
  );

alter table private.portal_lcia_projection_headers enable row level security;
alter table private.portal_lcia_projection_process_axis enable row level security;
alter table private.portal_lcia_projection_impact_axis enable row level security;
alter table private.portal_lcia_projection_values enable row level security;
alter table private.portal_lcia_projection_publications enable row level security;

revoke all on table private.portal_lcia_projection_headers
  from public, anon, authenticated, service_role;
revoke all on table private.portal_lcia_projection_process_axis
  from public, anon, authenticated, service_role;
revoke all on table private.portal_lcia_projection_impact_axis
  from public, anon, authenticated, service_role;
revoke all on table private.portal_lcia_projection_values
  from public, anon, authenticated, service_role;
revoke all on table private.portal_lcia_projection_publications
  from public, anon, authenticated, service_role;

create function private.portal_lcia_projection_header_guard_v1()
returns trigger
language plpgsql
set search_path = ''
as $function$
begin
  if tg_op = 'DELETE' then
    raise exception 'portal_lcia_projection_header_immutable'
      using errcode = '55000';
  end if;
  if old.status <> 'staging'
     or new.id is distinct from old.id
     or new.build_worker_job_id is distinct from old.build_worker_job_id
     or new.stage_lease_token is distinct from old.stage_lease_token
     or new.projection_contract_version is distinct from old.projection_contract_version
     or new.process_count is distinct from old.process_count
     or new.impact_count is distinct from old.impact_count
     or new.input_manifest_hash is distinct from old.input_manifest_hash
     or new.closure_certificate_hash is distinct from old.closure_certificate_hash
     or new.snapshot_hash is distinct from old.snapshot_hash
     or new.closure_bundle_hash is distinct from old.closure_bundle_hash
     or new.snapshot_index_sha256 is distinct from old.snapshot_index_sha256
     or new.snapshot_build_contract_hash is distinct from old.snapshot_build_contract_hash
     or new.bundle_content_hash is distinct from old.bundle_content_hash
     or new.bundle_manifest_sha256 is distinct from old.bundle_manifest_sha256
     or new.lcia_chunk_set_sha256 is distinct from old.lcia_chunk_set_sha256
     or new.result_artifact_sha256 is distinct from old.result_artifact_sha256
     or new.query_artifact_sha256 is distinct from old.query_artifact_sha256
     or new.created_at is distinct from old.created_at
     or new.status not in ('prepared', 'failed') then
    raise exception 'portal_lcia_projection_header_immutable'
      using errcode = '55000';
  end if;
  return new;
end
$function$;

create trigger portal_lcia_projection_header_guard_v1
before update or delete on private.portal_lcia_projection_headers
for each row execute function private.portal_lcia_projection_header_guard_v1();

create function private.portal_lcia_projection_row_guard_v1()
returns trigger
language plpgsql
set search_path = ''
as $function$
begin
  raise exception 'portal_lcia_projection_row_immutable'
    using errcode = '55000';
end
$function$;

create trigger portal_lcia_projection_process_row_guard_v1
before update or delete on private.portal_lcia_projection_process_axis
for each row execute function private.portal_lcia_projection_row_guard_v1();
create trigger portal_lcia_projection_impact_row_guard_v1
before update or delete on private.portal_lcia_projection_impact_axis
for each row execute function private.portal_lcia_projection_row_guard_v1();
create trigger portal_lcia_projection_value_row_guard_v1
before update or delete on private.portal_lcia_projection_values
for each row execute function private.portal_lcia_projection_row_guard_v1();

create function private.portal_lcia_projection_publication_guard_v1()
returns trigger
language plpgsql
set search_path = ''
as $function$
begin
  if tg_op = 'DELETE' then
    raise exception 'portal_lcia_projection_publication_immutable'
      using errcode = '55000';
  end if;
  if old.status <> 'finalized'
     or new.status <> 'revoked'
     or new.id is distinct from old.id
     or new.projection_id is distinct from old.projection_id
     or new.lcia_result_publication_id is distinct from old.lcia_result_publication_id
     or new.package_id is distinct from old.package_id
     or new.package_version is distinct from old.package_version
     or new.package_result_hash is distinct from old.package_result_hash
     or new.projection_content_hash is distinct from old.projection_content_hash
     or new.evidence_hash is distinct from old.evidence_hash
     or new.source_published_at is distinct from old.source_published_at
     or new.idempotency_key is distinct from old.idempotency_key
     or new.finalized_by is distinct from old.finalized_by
     or new.finalized_at is distinct from old.finalized_at then
    raise exception 'portal_lcia_projection_publication_immutable'
      using errcode = '55000';
  end if;
  return new;
end
$function$;

create trigger portal_lcia_projection_publication_guard_v1
before update or delete on private.portal_lcia_projection_publications
for each row execute function private.portal_lcia_projection_publication_guard_v1();

comment on table private.portal_lcia_projection_headers is
  'Lease-fenced, locator-free typed Portal LCIA projection preparation header. Each Worker attempt has its own immutable identity.';
comment on table private.portal_lcia_projection_process_axis is
  'Exact Process identity and complete public numeric context for one prepared Portal LCIA projection.';
comment on table private.portal_lcia_projection_impact_axis is
  'Exact LCIA method/impact identity and unit context for one prepared Portal LCIA projection.';
comment on table private.portal_lcia_projection_values is
  'Dense canonical-decimal Process-by-impact value grid. Explicit zero rows are retained.';
comment on table private.portal_lcia_projection_publications is
  'Finalized or revoked binding to an existing current LCIA result publication; rows are never deleted.';

create function private.svc_portal_lcia_projection_stage_begin_v1(
  p_build_worker_job_id uuid,
  p_stage_lease_token uuid,
  p_process_count integer,
  p_impact_count integer,
  p_source jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_job private.worker_jobs%rowtype;
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_expected_process_count integer;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false, 'code', 'service_role_required', 'status', 403
    );
  end if;

  if p_build_worker_job_id is null
     or p_stage_lease_token is null
     or p_process_count is null
     or p_impact_count is null
     or p_process_count not between 1 and 1000000
     or p_impact_count not between 1 and 10000
     or p_process_count::bigint * p_impact_count::bigint > 100000000
     or private.portal_lcia_json_object_has_keys_v1(
       p_source,
       array[
         'schemaVersion',
         'bundleContentHash',
         'bundleManifestSha256',
         'lciaChunkSetSha256',
         'resultArtifactSha256',
         'queryArtifactSha256'
       ]
     ) is not true
     or jsonb_typeof(p_source -> 'schemaVersion') <> 'string'
     or jsonb_typeof(p_source -> 'bundleContentHash') <> 'string'
     or jsonb_typeof(p_source -> 'bundleManifestSha256') <> 'string'
     or jsonb_typeof(p_source -> 'lciaChunkSetSha256') <> 'string'
     or jsonb_typeof(p_source -> 'resultArtifactSha256') <> 'string'
     or jsonb_typeof(p_source -> 'queryArtifactSha256') <> 'string'
     or p_source ->> 'schemaVersion' <> 'portal.lcia-projection.source.v1'
     or coalesce(p_source ->> 'bundleContentHash', '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_source ->> 'bundleManifestSha256', '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_source ->> 'lciaChunkSetSha256', '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_source ->> 'resultArtifactSha256', '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_source ->> 'queryArtifactSha256', '') !~ '^[0-9a-f]{64}$' then
    return jsonb_build_object(
      'ok', false, 'code', 'invalid_projection_request', 'status', 400
    );
  end if;

  select job.*
  into v_job
  from private.worker_jobs as job
  where job.id = p_build_worker_job_id
  for update;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_job_not_found', 'status', 404
    );
  end if;
  if v_job.job_kind <> 'lcia_result.package_build'
     or v_job.payload_schema_version <> 'lcia_result.package_build.request.v3'
     or v_job.payload_json ->> 'portalProjectionContractVersion'
          <> 'portal.lcia-projection.v1'
     or jsonb_typeof(v_job.payload_json -> 'input_manifest' -> 'processes')
          <> 'array'
     or jsonb_typeof(v_job.payload_json -> 'lcia_method_set') <> 'array'
     or coalesce(v_job.payload_json ->> 'input_manifest_hash', '')
          !~ '^[0-9a-f]{64}$'
     or coalesce(v_job.payload_json ->> 'closure_certificate_hash', '')
          !~ '^[0-9a-f]{64}$'
     or coalesce(v_job.payload_json ->> 'snapshot_hash', '')
          !~ '^[0-9a-f]{64}$'
     or coalesce(v_job.payload_json ->> 'closure_bundle_hash', '')
          !~ '^[0-9a-f]{64}$'
     or coalesce(v_job.payload_json ->> 'snapshot_index_sha256', '')
          !~ '^[0-9a-f]{64}$'
     or coalesce(v_job.payload_json ->> 'snapshot_build_contract_hash', '')
          !~ '^[0-9a-f]{64}$' then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_job_contract_invalid', 'status', 409
    );
  end if;

  if v_job.status <> 'running'
     or v_job.lease_token is distinct from p_stage_lease_token
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at <= clock_timestamp() then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_lease_invalid', 'status', 409
    );
  end if;

  v_expected_process_count := jsonb_array_length(
    v_job.payload_json -> 'input_manifest' -> 'processes'
  );
  if v_expected_process_count <> p_process_count then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_process_count_mismatch', 'status', 409
    );
  end if;
  if jsonb_array_length(v_job.payload_json -> 'lcia_method_set')
       <> p_impact_count then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_impact_count_mismatch', 'status', 409
    );
  end if;

  select projection.*
  into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.build_worker_job_id = p_build_worker_job_id
    and projection.stage_lease_token = p_stage_lease_token
  for update;

  if v_projection.id is not null then
    if v_projection.process_count <> p_process_count
       or v_projection.impact_count <> p_impact_count
       or v_projection.bundle_content_hash
            <> p_source ->> 'bundleContentHash'
       or v_projection.bundle_manifest_sha256
            <> p_source ->> 'bundleManifestSha256'
       or v_projection.lcia_chunk_set_sha256
            <> p_source ->> 'lciaChunkSetSha256'
       or v_projection.result_artifact_sha256
            <> p_source ->> 'resultArtifactSha256'
       or v_projection.query_artifact_sha256
            <> p_source ->> 'queryArtifactSha256' then
      return jsonb_build_object(
        'ok', false, 'code', 'projection_conflict', 'status', 409
      );
    end if;
    return jsonb_build_object(
      'ok', true,
      'idempotentReplay', true,
      'data', jsonb_build_object(
        'projectionId', v_projection.id,
        'buildWorkerJobId', v_projection.build_worker_job_id,
        'status', v_projection.status,
        'processCount', v_projection.process_count,
        'impactCount', v_projection.impact_count,
        'expectedValueCount', v_projection.expected_value_count,
        'hashContractVersion', 'portal.lcia-projection.int32be-frame-sha256.v1'
      )
    );
  end if;

  insert into private.portal_lcia_projection_headers (
    build_worker_job_id,
    stage_lease_token,
    projection_contract_version,
    process_count,
    impact_count,
    input_manifest_hash,
    closure_certificate_hash,
    snapshot_hash,
    closure_bundle_hash,
    snapshot_index_sha256,
    snapshot_build_contract_hash,
    bundle_content_hash,
    bundle_manifest_sha256,
    lcia_chunk_set_sha256,
    result_artifact_sha256,
    query_artifact_sha256
  ) values (
    v_job.id,
    p_stage_lease_token,
    'portal.lcia-projection.v1',
    p_process_count,
    p_impact_count,
    v_job.payload_json ->> 'input_manifest_hash',
    v_job.payload_json ->> 'closure_certificate_hash',
    v_job.payload_json ->> 'snapshot_hash',
    v_job.payload_json ->> 'closure_bundle_hash',
    v_job.payload_json ->> 'snapshot_index_sha256',
    v_job.payload_json ->> 'snapshot_build_contract_hash',
    p_source ->> 'bundleContentHash',
    p_source ->> 'bundleManifestSha256',
    p_source ->> 'lciaChunkSetSha256',
    p_source ->> 'resultArtifactSha256',
    p_source ->> 'queryArtifactSha256'
  )
  returning * into v_projection;

  return jsonb_build_object(
    'ok', true,
    'idempotentReplay', false,
    'data', jsonb_build_object(
      'projectionId', v_projection.id,
      'buildWorkerJobId', v_projection.build_worker_job_id,
      'status', v_projection.status,
      'processCount', v_projection.process_count,
      'impactCount', v_projection.impact_count,
      'expectedValueCount', v_projection.expected_value_count,
      'hashContractVersion', 'portal.lcia-projection.int32be-frame-sha256.v1'
    )
  );
exception
  when unique_violation then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_conflict', 'status', 409
    );
end
$function$;

create function private.svc_portal_lcia_projection_stage_register_batch_v1(
  p_projection_id uuid,
  p_stage_lease_token uuid,
  p_batch jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_job private.worker_jobs%rowtype;
  v_record jsonb;
  v_expected_process jsonb;
  v_expected_method jsonb;
  v_process private.portal_lcia_projection_process_axis%rowtype;
  v_impact private.portal_lcia_projection_impact_axis%rowtype;
  v_value private.portal_lcia_projection_values%rowtype;
  v_process_index integer;
  v_impact_index integer;
  v_ordinal bigint;
  v_decimal text;
  v_record_hash text;
  v_inserted integer := 0;
  v_row_count integer := 0;
  v_batch_count integer;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false, 'code', 'service_role_required', 'status', 403
    );
  end if;

  if p_projection_id is null
     or p_stage_lease_token is null
     or pg_catalog.octet_length(
       pg_catalog.convert_to(p_batch::text, 'UTF8')
     ) > 1048576
     or private.portal_lcia_json_object_has_keys_v1(
       p_batch,
       array['schemaVersion', 'processes', 'impacts', 'values']
     ) is not true
     or p_batch ->> 'schemaVersion' <> 'portal.lcia-projection.batch.v1'
     or jsonb_typeof(p_batch -> 'processes') <> 'array'
     or jsonb_typeof(p_batch -> 'impacts') <> 'array'
     or jsonb_typeof(p_batch -> 'values') <> 'array' then
    return jsonb_build_object(
      'ok', false, 'code', 'invalid_projection_batch', 'status', 400
    );
  end if;

  v_batch_count := jsonb_array_length(p_batch -> 'processes')
    + jsonb_array_length(p_batch -> 'impacts')
    + jsonb_array_length(p_batch -> 'values');
  if v_batch_count < 1 or v_batch_count > 500
     or exists (
       select 1
       from jsonb_array_elements(p_batch -> 'processes') as item(value)
       group by item.value ->> 'processIndex'
       having count(*) > 1
     )
     or exists (
       select 1
       from jsonb_array_elements(p_batch -> 'impacts') as item(value)
       group by item.value ->> 'impactIndex'
       having count(*) > 1
     )
     or exists (
       select 1
       from jsonb_array_elements(p_batch -> 'values') as item(value)
       group by item.value ->> 'ordinal'
       having count(*) > 1
     ) then
    return jsonb_build_object(
      'ok', false, 'code', 'invalid_projection_batch', 'status', 400
    );
  end if;

  select projection.*
  into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = p_projection_id
  for update;
  if v_projection.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_found', 'status', 404
    );
  end if;

  select job.*
  into v_job
  from private.worker_jobs as job
  where job.id = v_projection.build_worker_job_id
  for share;
  if v_projection.stage_lease_token is distinct from p_stage_lease_token
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from p_stage_lease_token
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at <= clock_timestamp() then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_lease_invalid', 'status', 409
    );
  end if;
  if v_projection.status <> 'staging' then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_staging', 'status', 409
    );
  end if;

  begin
    for v_record in
      select item.value
      from jsonb_array_elements(p_batch -> 'processes') as item(value)
    loop
      if private.portal_lcia_json_object_has_keys_v1(
        v_record,
        array[
          'processIndex', 'processId', 'processVersion',
          'referenceFlowId', 'referenceFlowVersion',
          'referenceExchangeInternalId', 'referenceFlowAmount',
          'referenceFlowDirection', 'functionalUnitAmount',
          'functionalUnitUnit', 'functionalUnitDescription',
          'geographyCode', 'geographyPrecision', 'referenceYear',
          'processDocumentSha256'
        ]
      ) is not true then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;
      if jsonb_typeof(v_record -> 'processIndex') <> 'number'
         or jsonb_typeof(v_record -> 'referenceYear') <> 'number'
         or jsonb_typeof(v_record -> 'processId') <> 'string'
         or jsonb_typeof(v_record -> 'processVersion') <> 'string'
         or jsonb_typeof(v_record -> 'referenceFlowId') <> 'string'
         or jsonb_typeof(v_record -> 'referenceFlowVersion') <> 'string'
         or jsonb_typeof(v_record -> 'referenceExchangeInternalId') <> 'string'
         or jsonb_typeof(v_record -> 'referenceFlowAmount') <> 'string'
         or jsonb_typeof(v_record -> 'referenceFlowDirection') <> 'string'
         or jsonb_typeof(v_record -> 'functionalUnitAmount') <> 'string'
         or jsonb_typeof(v_record -> 'functionalUnitUnit') <> 'string'
         or jsonb_typeof(v_record -> 'geographyCode') <> 'string'
         or jsonb_typeof(v_record -> 'geographyPrecision') <> 'string'
         or jsonb_typeof(v_record -> 'processDocumentSha256') <> 'string' then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;
      begin
        v_process_index := (v_record ->> 'processIndex')::integer;
        v_process.process_id := (v_record ->> 'processId')::uuid;
        v_process.reference_flow_id := (v_record ->> 'referenceFlowId')::uuid;
        v_process.reference_year := (v_record ->> 'referenceYear')::integer;
      exception when others then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end;
      v_decimal := private.portal_canonical_decimal_v1(
        v_record ->> 'functionalUnitAmount'
      );
      v_expected_process := v_job.payload_json
        -> 'input_manifest' -> 'processes' -> v_process_index;
      if v_record ->> 'processIndex' !~ '^(0|[1-9]\d*)$'
         or v_record ->> 'referenceYear' !~ '^(0|[1-9]\d*)$'
         or v_process_index not between 0 and v_projection.process_count - 1
         or v_decimal is distinct from v_record ->> 'functionalUnitAmount'
         or private.portal_canonical_decimal_v1(
              v_record ->> 'referenceFlowAmount'
            ) is distinct from v_record ->> 'referenceFlowAmount'
         or coalesce(v_record ->> 'processVersion', '')
              !~ '^\d{2}\.\d{2}\.\d{3}$'
         or coalesce(v_record ->> 'referenceFlowVersion', '')
              !~ '^\d{2}\.\d{2}\.\d{3}$'
         or coalesce(v_record ->> 'referenceExchangeInternalId', '')
              !~ '^(0|[1-9]\d{0,5})$'
         or v_record ->> 'referenceFlowDirection' not in ('input', 'output')
         or private.portal_lcia_public_text_valid_v1(
              v_record ->> 'functionalUnitUnit', 128
            ) is not true
         or private.portal_lcia_localized_text_valid_v1(
              v_record -> 'functionalUnitDescription'
            ) is not true
         or private.portal_lcia_public_text_valid_v1(
              v_record ->> 'geographyCode', 128
            ) is not true
         or v_record ->> 'geographyPrecision'
              not in ('country', 'province', 'city', 'other', 'unknown')
         or v_process.reference_year not between 0 and 9999
         or coalesce(v_record ->> 'processDocumentSha256', '')
              !~ '^[0-9a-f]{64}$'
         or jsonb_typeof(v_expected_process) <> 'object'
         or v_expected_process ->> 'id'
              is distinct from v_process.process_id::text
         or v_expected_process ->> 'version'
              is distinct from v_record ->> 'processVersion' then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;

      v_record_hash := private.portal_lcia_projection_sha256_fields_v1(
        'portal.lcia-projection.process.v1',
        'portal.lcia-projection.int32be-frame-sha256.v1',
        v_process_index::text,
        v_process.process_id::text,
        v_record ->> 'processVersion',
        v_process.reference_flow_id::text,
        v_record ->> 'referenceFlowVersion',
        v_record ->> 'referenceExchangeInternalId',
        private.portal_canonical_decimal_v1(v_record ->> 'referenceFlowAmount'),
        v_record ->> 'referenceFlowDirection',
        v_decimal,
        btrim(v_record ->> 'functionalUnitUnit'),
        private.portal_lcia_localized_text_frame_hex_v1(
          v_record -> 'functionalUnitDescription'
        ),
        btrim(v_record ->> 'geographyCode'),
        v_record ->> 'geographyPrecision',
        v_process.reference_year::text,
        v_record ->> 'processDocumentSha256'
      );

      insert into private.portal_lcia_projection_process_axis (
        projection_id, process_index, process_id, process_version,
        reference_flow_id, reference_flow_version,
        reference_exchange_internal_id, reference_flow_amount,
        reference_flow_direction, functional_unit_amount,
        functional_unit_unit, functional_unit_description,
        geography_code, geography_precision, reference_year,
        process_document_sha256, record_hash
      ) values (
        v_projection.id, v_process_index, v_process.process_id,
        v_record ->> 'processVersion', v_process.reference_flow_id,
        v_record ->> 'referenceFlowVersion',
        v_record ->> 'referenceExchangeInternalId',
        private.portal_canonical_decimal_v1(v_record ->> 'referenceFlowAmount'),
        v_record ->> 'referenceFlowDirection', v_decimal,
        btrim(v_record ->> 'functionalUnitUnit'),
        v_record -> 'functionalUnitDescription',
        btrim(v_record ->> 'geographyCode'),
        v_record ->> 'geographyPrecision', v_process.reference_year,
        v_record ->> 'processDocumentSha256', v_record_hash
      ) on conflict do nothing;
      get diagnostics v_row_count = row_count;
      v_inserted := v_inserted + v_row_count;

      select row.* into v_process
      from private.portal_lcia_projection_process_axis as row
      where row.projection_id = v_projection.id
        and row.process_index = v_process_index;
      if v_process.record_hash is distinct from v_record_hash then
        raise exception using errcode = 'P2102', message = 'projection batch conflict';
      end if;
    end loop;

    for v_record in
      select item.value
      from jsonb_array_elements(p_batch -> 'impacts') as item(value)
    loop
      if private.portal_lcia_json_object_has_keys_v1(
        v_record,
        array[
          'impactIndex', 'methodId', 'methodVersion', 'impactId',
          'impactName', 'unit', 'methodDocumentSha256'
        ]
      ) is not true then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;
      if jsonb_typeof(v_record -> 'impactIndex') <> 'number'
         or jsonb_typeof(v_record -> 'methodId') <> 'string'
         or jsonb_typeof(v_record -> 'methodVersion') <> 'string'
         or jsonb_typeof(v_record -> 'impactId') <> 'string'
         or jsonb_typeof(v_record -> 'unit') <> 'string'
         or jsonb_typeof(v_record -> 'methodDocumentSha256') <> 'string' then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;
      begin
        v_impact_index := (v_record ->> 'impactIndex')::integer;
        v_impact.method_id := (v_record ->> 'methodId')::uuid;
      exception when others then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end;
      v_expected_method := v_job.payload_json
        -> 'lcia_method_set' -> v_impact_index;
      if v_record ->> 'impactIndex' !~ '^(0|[1-9]\d*)$'
         or v_impact_index not between 0 and v_projection.impact_count - 1
         or coalesce(v_record ->> 'methodVersion', '')
              !~ '^\d{2}\.\d{2}\.\d{3}$'
         or private.portal_lcia_public_text_valid_v1(
              v_record ->> 'impactId', 512
            ) is not true
         or private.portal_lcia_localized_text_valid_v1(
              v_record -> 'impactName'
            ) is not true
         or private.portal_lcia_public_text_valid_v1(
              v_record ->> 'unit', 128
            ) is not true
         or coalesce(v_record ->> 'methodDocumentSha256', '')
              !~ '^[0-9a-f]{64}$'
         or jsonb_typeof(v_expected_method) <> 'object'
         or v_expected_method ->> 'id'
              is distinct from v_impact.method_id::text
         or v_expected_method ->> 'version'
              is distinct from v_record ->> 'methodVersion' then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;

      v_record_hash := private.portal_lcia_projection_sha256_fields_v1(
        'portal.lcia-projection.impact.v1',
        'portal.lcia-projection.int32be-frame-sha256.v1',
        v_impact_index::text,
        v_impact.method_id::text,
        v_record ->> 'methodVersion',
        btrim(v_record ->> 'impactId'),
        private.portal_lcia_localized_text_frame_hex_v1(
          v_record -> 'impactName'
        ),
        btrim(v_record ->> 'unit'),
        v_record ->> 'methodDocumentSha256'
      );
      insert into private.portal_lcia_projection_impact_axis (
        projection_id, impact_index, method_id, method_version,
        impact_id, impact_name, unit, method_document_sha256, record_hash
      ) values (
        v_projection.id, v_impact_index, v_impact.method_id,
        v_record ->> 'methodVersion', btrim(v_record ->> 'impactId'),
        v_record -> 'impactName', btrim(v_record ->> 'unit'),
        v_record ->> 'methodDocumentSha256', v_record_hash
      ) on conflict do nothing;
      get diagnostics v_row_count = row_count;
      v_inserted := v_inserted + v_row_count;

      select row.* into v_impact
      from private.portal_lcia_projection_impact_axis as row
      where row.projection_id = v_projection.id
        and row.impact_index = v_impact_index;
      if v_impact.record_hash is distinct from v_record_hash then
        raise exception using errcode = 'P2102', message = 'projection batch conflict';
      end if;
    end loop;

    for v_record in
      select item.value
      from jsonb_array_elements(p_batch -> 'values') as item(value)
    loop
      if private.portal_lcia_json_object_has_keys_v1(
        v_record,
        array['ordinal', 'processIndex', 'impactIndex', 'value']
      ) is not true then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;
      if jsonb_typeof(v_record -> 'ordinal') <> 'number'
         or jsonb_typeof(v_record -> 'processIndex') <> 'number'
         or jsonb_typeof(v_record -> 'impactIndex') <> 'number'
         or jsonb_typeof(v_record -> 'value') <> 'string' then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;
      begin
        v_ordinal := (v_record ->> 'ordinal')::bigint;
        v_process_index := (v_record ->> 'processIndex')::integer;
        v_impact_index := (v_record ->> 'impactIndex')::integer;
      exception when others then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end;
      v_decimal := private.portal_canonical_decimal_v1(v_record ->> 'value');
      if v_record ->> 'ordinal' !~ '^[1-9]\d*$'
         or v_record ->> 'processIndex' !~ '^(0|[1-9]\d*)$'
         or v_record ->> 'impactIndex' !~ '^(0|[1-9]\d*)$'
         or v_process_index not between 0 and v_projection.process_count - 1
         or v_impact_index not between 0 and v_projection.impact_count - 1
         or v_ordinal < 1
         or v_ordinal <>
              v_process_index::bigint * v_projection.impact_count::bigint
              + v_impact_index::bigint + 1
         or v_decimal is distinct from v_record ->> 'value' then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;

      v_record_hash := private.portal_lcia_projection_sha256_fields_v1(
        'portal.lcia-projection.value.v1',
        'portal.lcia-projection.int32be-frame-sha256.v1',
        v_ordinal::text,
        v_process_index::text,
        v_impact_index::text,
        v_decimal
      );
      insert into private.portal_lcia_projection_values (
        projection_id, ordinal, process_index, impact_index,
        value_text, value_numeric, record_hash
      ) values (
        v_projection.id, v_ordinal, v_process_index, v_impact_index,
        v_decimal, v_decimal::numeric, v_record_hash
      ) on conflict do nothing;
      get diagnostics v_row_count = row_count;
      v_inserted := v_inserted + v_row_count;

      select row.* into v_value
      from private.portal_lcia_projection_values as row
      where row.projection_id = v_projection.id
        and row.ordinal = v_ordinal;
      if v_value.record_hash is distinct from v_record_hash then
        raise exception using errcode = 'P2102', message = 'projection batch conflict';
      end if;
    end loop;
  exception
    when sqlstate 'P2101' or invalid_text_representation
      or numeric_value_out_of_range or check_violation
      or foreign_key_violation or not_null_violation then
      return jsonb_build_object(
        'ok', false, 'code', 'invalid_projection_batch', 'status', 400
      );
    when sqlstate 'P2102' or unique_violation then
      return jsonb_build_object(
        'ok', false, 'code', 'projection_batch_conflict', 'status', 409
      );
  end;

  return jsonb_build_object(
    'ok', true,
    'idempotentReplay', v_inserted = 0,
    'data', jsonb_build_object(
      'projectionId', v_projection.id,
      'acceptedRecordCount', v_batch_count,
      'insertedRecordCount', v_inserted
    )
  );
end
$function$;

create function private.svc_portal_lcia_projection_stage_status_v1(
  p_projection_id uuid,
  p_stage_lease_token uuid
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_job private.worker_jobs%rowtype;
  v_process_count bigint;
  v_impact_count bigint;
  v_value_count bigint;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false, 'code', 'service_role_required', 'status', 403
    );
  end if;
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = p_projection_id;
  if v_projection.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_found', 'status', 404
    );
  end if;
  select job.* into v_job
  from private.worker_jobs as job
  where job.id = v_projection.build_worker_job_id;
  if v_projection.stage_lease_token is distinct from p_stage_lease_token
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from p_stage_lease_token
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at <= clock_timestamp() then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_lease_invalid', 'status', 409
    );
  end if;

  select count(*) into v_process_count
  from private.portal_lcia_projection_process_axis
  where projection_id = v_projection.id;
  select count(*) into v_impact_count
  from private.portal_lcia_projection_impact_axis
  where projection_id = v_projection.id;
  select count(*) into v_value_count
  from private.portal_lcia_projection_values
  where projection_id = v_projection.id;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_strip_nulls(jsonb_build_object(
      'projectionId', v_projection.id,
      'buildWorkerJobId', v_projection.build_worker_job_id,
      'status', v_projection.status,
      'processCount', v_process_count,
      'expectedProcessCount', v_projection.process_count,
      'impactCount', v_impact_count,
      'expectedImpactCount', v_projection.impact_count,
      'valueCount', v_value_count,
      'expectedValueCount', v_projection.expected_value_count,
      'hashContractVersion', 'portal.lcia-projection.int32be-frame-sha256.v1',
      'processAxisHash', v_projection.process_axis_hash,
      'impactAxisHash', v_projection.impact_axis_hash,
      'valueGridHash', v_projection.value_grid_hash,
      'relationHash', v_projection.relation_hash,
      'contentHash', v_projection.content_hash,
      'failureCode', v_projection.failure_code
    ))
  );
end
$function$;

create function private.svc_portal_lcia_projection_stage_seal_v1(
  p_projection_id uuid,
  p_stage_lease_token uuid
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_job private.worker_jobs%rowtype;
  v_process_count bigint;
  v_impact_count bigint;
  v_value_count bigint;
  v_bad_count bigint;
  v_process_axis_hash text;
  v_impact_axis_hash text;
  v_value_grid_hash text;
  v_relation_hash text;
  v_content_hash text;
  v_fields text[];
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false, 'code', 'service_role_required', 'status', 403
    );
  end if;
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = p_projection_id
  for update;
  if v_projection.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_found', 'status', 404
    );
  end if;
  select job.* into v_job
  from private.worker_jobs as job
  where job.id = v_projection.build_worker_job_id
  for share;
  if v_projection.stage_lease_token is distinct from p_stage_lease_token
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from p_stage_lease_token
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at <= clock_timestamp() then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_lease_invalid', 'status', 409
    );
  end if;
  if v_projection.status = 'prepared' then
    return jsonb_build_object(
      'ok', true,
      'idempotentReplay', true,
      'data', jsonb_build_object(
        'projectionId', v_projection.id,
        'status', v_projection.status,
        'processCount', v_projection.process_count,
        'impactCount', v_projection.impact_count,
        'valueCount', v_projection.expected_value_count,
        'processAxisHash', v_projection.process_axis_hash,
        'impactAxisHash', v_projection.impact_axis_hash,
        'valueGridHash', v_projection.value_grid_hash,
        'relationHash', v_projection.relation_hash,
        'contentHash', v_projection.content_hash,
        'hashContractVersion', 'portal.lcia-projection.int32be-frame-sha256.v1'
      )
    );
  end if;
  if v_projection.status <> 'staging' then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_staging', 'status', 409
    );
  end if;

  select count(*),
         count(*) filter (where process_index between 0 and v_projection.process_count - 1)
  into v_process_count, v_bad_count
  from private.portal_lcia_projection_process_axis
  where projection_id = v_projection.id;
  if v_process_count <> v_projection.process_count
     or v_bad_count <> v_projection.process_count then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_incomplete', 'status', 409
    );
  end if;

  select count(*),
         count(*) filter (where impact_index between 0 and v_projection.impact_count - 1)
  into v_impact_count, v_bad_count
  from private.portal_lcia_projection_impact_axis
  where projection_id = v_projection.id;
  if v_impact_count <> v_projection.impact_count
     or v_bad_count <> v_projection.impact_count then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_incomplete', 'status', 409
    );
  end if;

  select count(*),
         count(*) filter (
           where ordinal = process_index::bigint * v_projection.impact_count::bigint
             + impact_index::bigint + 1
             and ordinal between 1 and v_projection.expected_value_count
         )
  into v_value_count, v_bad_count
  from private.portal_lcia_projection_values
  where projection_id = v_projection.id;
  if v_value_count <> v_projection.expected_value_count
     or v_bad_count <> v_projection.expected_value_count then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_incomplete', 'status', 409
    );
  end if;

  if jsonb_typeof(v_job.payload_json -> 'input_manifest' -> 'processes')
       <> 'array'
     or jsonb_array_length(v_job.payload_json -> 'input_manifest' -> 'processes')
          <> v_projection.process_count
     or jsonb_typeof(v_job.payload_json -> 'lcia_method_set') <> 'array'
     or jsonb_array_length(v_job.payload_json -> 'lcia_method_set')
          <> v_projection.impact_count then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select count(*) into v_bad_count
  from private.portal_lcia_projection_process_axis as row
  where row.projection_id = v_projection.id
    and (
      v_job.payload_json -> 'input_manifest' -> 'processes' -> row.process_index
        ->> 'id' is distinct from row.process_id::text
      or v_job.payload_json -> 'input_manifest' -> 'processes'
        -> row.process_index ->> 'version' is distinct from row.process_version
    );
  if v_bad_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select count(*) into v_bad_count
  from private.portal_lcia_projection_impact_axis as row
  where row.projection_id = v_projection.id
    and (
      v_job.payload_json -> 'lcia_method_set' -> row.impact_index
        ->> 'id' is distinct from row.method_id::text
      or v_job.payload_json -> 'lcia_method_set' -> row.impact_index
        ->> 'version' is distinct from row.method_version
    );
  if v_bad_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select count(*) into v_bad_count
  from private.portal_lcia_projection_process_axis as row
  where row.projection_id = v_projection.id
    and row.record_hash is distinct from
      private.portal_lcia_projection_sha256_fields_v1(
        'portal.lcia-projection.process.v1',
        'portal.lcia-projection.int32be-frame-sha256.v1',
        row.process_index::text,
        row.process_id::text,
        row.process_version,
        row.reference_flow_id::text,
        row.reference_flow_version,
        row.reference_exchange_internal_id,
        row.reference_flow_amount,
        row.reference_flow_direction,
        row.functional_unit_amount,
        row.functional_unit_unit,
        private.portal_lcia_localized_text_frame_hex_v1(
          row.functional_unit_description
        ),
        row.geography_code,
        row.geography_precision,
        row.reference_year::text,
        row.process_document_sha256
      );
  if v_bad_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select count(*) into v_bad_count
  from private.portal_lcia_projection_impact_axis as row
  where row.projection_id = v_projection.id
    and row.record_hash is distinct from
      private.portal_lcia_projection_sha256_fields_v1(
        'portal.lcia-projection.impact.v1',
        'portal.lcia-projection.int32be-frame-sha256.v1',
        row.impact_index::text,
        row.method_id::text,
        row.method_version,
        row.impact_id,
        private.portal_lcia_localized_text_frame_hex_v1(row.impact_name),
        row.unit,
        row.method_document_sha256
      );
  if v_bad_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select count(*) into v_bad_count
  from private.portal_lcia_projection_values as row
  where row.projection_id = v_projection.id
    and row.record_hash is distinct from
      private.portal_lcia_projection_sha256_fields_v1(
        'portal.lcia-projection.value.v1',
        'portal.lcia-projection.int32be-frame-sha256.v1',
        row.ordinal::text,
        row.process_index::text,
        row.impact_index::text,
        row.value_text
      );
  if v_bad_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select array[
    'portal.lcia-projection.relation.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    'process-axis',
    v_projection.process_count::text
  ] || coalesce(
    array_agg(field.value order by row.process_index, field.position),
    '{}'::text[]
  )
  into v_fields
  from private.portal_lcia_projection_process_axis as row
  cross join lateral (
    values (1, (row.process_index + 1)::text), (2, row.record_hash)
  ) as field(position, value)
  where row.projection_id = v_projection.id;
  v_process_axis_hash := private.portal_lcia_projection_sha256_fields_v1(
    variadic v_fields
  );

  select array[
    'portal.lcia-projection.relation.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    'impact-axis',
    v_projection.impact_count::text
  ] || coalesce(
    array_agg(field.value order by row.impact_index, field.position),
    '{}'::text[]
  )
  into v_fields
  from private.portal_lcia_projection_impact_axis as row
  cross join lateral (
    values (1, (row.impact_index + 1)::text), (2, row.record_hash)
  ) as field(position, value)
  where row.projection_id = v_projection.id;
  v_impact_axis_hash := private.portal_lcia_projection_sha256_fields_v1(
    variadic v_fields
  );

  select array[
    'portal.lcia-projection.relation.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    'value-grid',
    v_projection.expected_value_count::text
  ] || coalesce(
    array_agg(field.value order by row.ordinal, field.position),
    '{}'::text[]
  )
  into v_fields
  from private.portal_lcia_projection_values as row
  cross join lateral (
    values (1, row.ordinal::text), (2, row.record_hash)
  ) as field(position, value)
  where row.projection_id = v_projection.id;
  v_value_grid_hash := private.portal_lcia_projection_sha256_fields_v1(
    variadic v_fields
  );

  v_relation_hash := private.portal_lcia_projection_sha256_fields_v1(
    'portal.lcia-projection.grid-relation.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    v_projection.process_count::text,
    v_projection.impact_count::text,
    v_projection.expected_value_count::text,
    'ordinal=processIndex*impactCount+impactIndex+1',
    v_process_axis_hash,
    v_impact_axis_hash,
    v_value_grid_hash
  );
  v_content_hash := private.portal_lcia_projection_sha256_fields_v1(
    'portal.lcia-projection.content.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    v_projection.projection_contract_version,
    v_projection.input_manifest_hash,
    v_projection.closure_certificate_hash,
    v_projection.snapshot_hash,
    v_projection.closure_bundle_hash,
    v_projection.snapshot_index_sha256,
    v_projection.snapshot_build_contract_hash,
    v_projection.bundle_content_hash,
    v_projection.bundle_manifest_sha256,
    v_projection.lcia_chunk_set_sha256,
    v_projection.result_artifact_sha256,
    v_projection.query_artifact_sha256,
    v_projection.process_count::text,
    v_projection.impact_count::text,
    v_projection.expected_value_count::text,
    v_process_axis_hash,
    v_impact_axis_hash,
    v_value_grid_hash,
    v_relation_hash
  );

  update private.portal_lcia_projection_headers
  set status = 'prepared',
      process_axis_hash = v_process_axis_hash,
      impact_axis_hash = v_impact_axis_hash,
      value_grid_hash = v_value_grid_hash,
      relation_hash = v_relation_hash,
      content_hash = v_content_hash,
      prepared_at = clock_timestamp()
  where id = v_projection.id
  returning * into v_projection;

  return jsonb_build_object(
    'ok', true,
    'idempotentReplay', false,
    'data', jsonb_build_object(
      'projectionId', v_projection.id,
      'status', v_projection.status,
      'processCount', v_projection.process_count,
      'impactCount', v_projection.impact_count,
      'valueCount', v_projection.expected_value_count,
      'processAxisHash', v_projection.process_axis_hash,
      'impactAxisHash', v_projection.impact_axis_hash,
      'valueGridHash', v_projection.value_grid_hash,
      'relationHash', v_projection.relation_hash,
      'contentHash', v_projection.content_hash,
      'hashContractVersion', 'portal.lcia-projection.int32be-frame-sha256.v1'
    )
  );
end
$function$;

create function private.svc_portal_lcia_projection_stage_fail_v1(
  p_projection_id uuid,
  p_stage_lease_token uuid,
  p_code text,
  p_message text,
  p_audit jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_job private.worker_jobs%rowtype;
  v_code text := btrim(coalesce(p_code, ''));
  v_message text := nullif(btrim(coalesce(p_message, '')), '');
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false, 'code', 'service_role_required', 'status', 403
    );
  end if;
  if length(v_code) not between 1 and 128
     or v_code !~ '^[a-z0-9_]+$'
     or (
       v_message is not null
       and private.portal_lcia_public_text_valid_v1(v_message, 2000)
             is not true
     )
     or private.portal_lcia_safe_audit_v1(p_audit) is not true then
    return jsonb_build_object(
      'ok', false, 'code', 'invalid_projection_request', 'status', 400
    );
  end if;
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = p_projection_id
  for update;
  if v_projection.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_found', 'status', 404
    );
  end if;
  select job.* into v_job
  from private.worker_jobs as job
  where job.id = v_projection.build_worker_job_id
  for share;
  if v_projection.stage_lease_token is distinct from p_stage_lease_token
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from p_stage_lease_token
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at <= clock_timestamp() then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_lease_invalid', 'status', 409
    );
  end if;
  if v_projection.status = 'failed'
     and v_projection.failure_code = v_code
     and v_projection.failure_message is not distinct from v_message then
    return jsonb_build_object(
      'ok', true, 'idempotentReplay', true,
      'data', jsonb_build_object(
        'projectionId', v_projection.id, 'status', v_projection.status
      )
    );
  end if;
  if v_projection.status <> 'staging' then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_staging', 'status', 409
    );
  end if;

  update private.portal_lcia_projection_headers
  set status = 'failed',
      failure_code = v_code,
      failure_message = v_message,
      failed_at = clock_timestamp()
  where id = v_projection.id
  returning * into v_projection;

  insert into private.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    'svc_portal_lcia_projection_stage_fail_v1',
    v_job.requested_by,
    'portal_lcia_projection_headers',
    v_projection.id,
    v_projection.projection_contract_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'buildWorkerJobId', v_projection.build_worker_job_id,
      'failureCode', v_code,
      'failureMessage', v_message
    )
  );

  return jsonb_build_object(
    'ok', true, 'idempotentReplay', false,
    'data', jsonb_build_object(
      'projectionId', v_projection.id, 'status', v_projection.status
    )
  );
end
$function$;

create function private.portal_lcia_projection_v3_job_binding_valid_v1(
  p_build_worker_job_id uuid,
  p_lease_token uuid
)
returns boolean
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_job private.worker_jobs%rowtype;
  v_check private.lcia_scope_closure_checks%rowtype;
  v_closure_check_id uuid;
begin
  select job.* into v_job
  from private.worker_jobs as job
  where job.id = p_build_worker_job_id;
  if v_job.id is null
     or v_job.job_kind <> 'lcia_result.package_build'
     or v_job.payload_schema_version <> 'lcia_result.package_build.request.v3'
     or v_job.payload_json ->> 'portalProjectionContractVersion'
          <> 'portal.lcia-projection.v1'
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from p_lease_token
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at <= clock_timestamp() then
    return false;
  end if;
  begin
    v_closure_check_id := nullif(
      v_job.payload_json ->> 'closure_check_id', ''
    )::uuid;
  exception when invalid_text_representation then
    return false;
  end;
  select closure_check.* into v_check
  from private.lcia_scope_closure_checks as closure_check
  where closure_check.id = v_closure_check_id;
  if v_check.id is null
     or v_check.requested_by <> v_job.requested_by
     or not private.lcia_scope_closure_evidence_usable(v_check)
     or v_job.payload_json ->> 'closure_certificate_hash'
          is distinct from v_check.certificate_hash
     or v_job.payload_json ->> 'requested_scope_hash'
          is distinct from v_check.requested_scope_hash
     or v_job.payload_json ->> 'policy_fingerprint'
          is distinct from v_check.policy_fingerprint
     or v_job.payload_json ->> 'effective_scope_hash'
          is distinct from v_check.effective_scope_hash
     or v_job.payload_json ->> 'data_snapshot_token'
          is distinct from v_check.data_snapshot_token
     or v_job.payload_json ->> 'snapshot_id'
          is distinct from v_check.snapshot_id::text
     or v_job.payload_json ->> 'snapshot_hash'
          is distinct from v_check.snapshot_hash
     or v_job.payload_json ->> 'closure_bundle_artifact_id'
          is distinct from v_check.closure_bundle_artifact_id::text
     or v_job.payload_json ->> 'closure_bundle_hash'
          is distinct from v_check.closure_bundle_hash
     or v_job.payload_json ->> 'report_artifact_manifest_hash'
          is distinct from v_check.report_artifact_manifest_hash
     or v_job.payload_json ->> 'snapshot_artifact_id'
          is distinct from v_check.snapshot_artifact_id::text
     or v_job.payload_json ->> 'snapshot_index_sha256'
          is distinct from v_check.snapshot_index_sha256
     or v_job.payload_json ->> 'snapshot_build_contract_hash'
          is distinct from v_check.snapshot_build_contract_hash then
    return false;
  end if;
  if v_check.requested_scope_manifest ->> 'certificateFreshnessPolicy'
       = 'current-membership-required-v1'
     and not private.lcia_scope_closure_current_release_matches(
       v_check.data_snapshot_token
     ) then
    return false;
  end if;
  return true;
end
$function$;

create function api.cmd_lcia_result_build_request_v3(
  p_name text,
  p_processes jsonb,
  p_coverage_mode text,
  p_default_impact_category text,
  p_lcia_method_set jsonb,
  p_idempotency_key text,
  p_closure_check_id uuid,
  p_requested_scope_hash text,
  p_policy_fingerprint text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_actor uuid := auth.uid();
  v_idempotency_key text;
  v_request jsonb;
  v_result jsonb;
  v_job private.worker_jobs%rowtype;
  v_job_id uuid;
  v_actual_idempotency_key text;
begin
  if v_actor is null then
    return api.lcia_result_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if private.portal_lcia_safe_audit_v1(p_audit) is not true
     or nullif(btrim(coalesce(p_idempotency_key, '')), '') is null
     or length(btrim(p_idempotency_key)) > 220 then
    return api.lcia_result_error(
      'invalid_projection_request', 400, 'Invalid Portal LCIA V3 request'
    );
  end if;
  v_idempotency_key := 'portal-lcia-v3:' || btrim(p_idempotency_key);
  v_request := jsonb_build_object(
    'name', p_name,
    'processes', p_processes,
    'coverageMode', p_coverage_mode,
    'defaultImpactCategory', p_default_impact_category,
    'lciaMethodSet', p_lcia_method_set,
    'closureCheckId', p_closure_check_id,
    'requestedScopeHash', p_requested_scope_hash,
    'policyFingerprint', p_policy_fingerprint
  );

  select job.* into v_job
  from private.worker_jobs as job
  where job.job_kind = 'lcia_result.package_build'
    and job.requested_by = v_actor
    and job.payload_schema_version = 'lcia_result.package_build.request.v3'
    and job.payload_json ->> 'portalProjectionIdempotencyKey'
          = v_idempotency_key
  order by job.created_at desc, job.id
  limit 1
  for update;
  if v_job.id is not null then
    if v_job.payload_json -> 'portalProjectionRequest' is distinct from v_request
       or v_job.payload_json ->> 'portalProjectionContractVersion'
            <> 'portal.lcia-projection.v1' then
      return api.lcia_result_error(
        'build_enqueue_conflict', 409,
        'Existing V3 build is bound to different content'
      );
    end if;
    return jsonb_build_object(
      'ok', true,
      'reused', true,
      'data', jsonb_build_object(
        'buildId', v_job.subject_id,
        'workerJobId', v_job.id,
        'workerJob', private.worker_job_payload(v_job, false),
        'projectionContractVersion', 'portal.lcia-projection.v1'
      )
    );
  end if;

  v_result := api.cmd_lcia_result_build_request_v2(
    p_name,
    p_processes,
    p_coverage_mode,
    p_default_impact_category,
    p_lcia_method_set,
    v_idempotency_key,
    p_closure_check_id,
    p_requested_scope_hash,
    p_policy_fingerprint,
    p_audit
  );
  if coalesce((v_result ->> 'ok')::boolean, false) is not true then
    return v_result;
  end if;
  begin
    v_job_id := nullif(v_result -> 'data' ->> 'workerJobId', '')::uuid;
    v_actual_idempotency_key := nullif(
      v_result -> 'data' -> 'workerJob' ->> 'idempotencyKey', ''
    );
  exception when invalid_text_representation then
    return api.lcia_result_error(
      'build_enqueue_unavailable', 503,
      'V2 admission did not return a valid Worker job identity'
    );
  end;

  select job.* into v_job
  from private.worker_jobs as job
  where job.id = v_job_id
  for update;
  if v_job.payload_schema_version = 'lcia_result.package_build.request.v3' then
    if v_job.requested_by = v_actor
       and v_job.idempotency_key is not distinct from v_actual_idempotency_key
       and v_job.payload_json ->> 'portalProjectionIdempotencyKey'
            = v_idempotency_key
       and v_job.payload_json -> 'portalProjectionRequest' = v_request
       and v_job.payload_json ->> 'portalProjectionContractVersion'
            = 'portal.lcia-projection.v1' then
      return jsonb_build_object(
        'ok', true,
        'reused', true,
        'data', jsonb_build_object(
          'buildId', v_job.subject_id,
          'workerJobId', v_job.id,
          'workerJob', private.worker_job_payload(v_job, false),
          'projectionContractVersion', 'portal.lcia-projection.v1'
        )
      );
    end if;
    return api.lcia_result_error(
      'build_enqueue_conflict', 409,
      'Existing V3 build is bound to different content'
    );
  end if;
  if v_job.id is null
     or v_job.requested_by <> v_actor
     or v_actual_idempotency_key is null
     or v_job.idempotency_key is distinct from v_actual_idempotency_key
     or v_job.payload_schema_version <> 'lcia_result.package_build.request.v2'
     or v_job.status not in ('queued', 'running', 'waiting', 'stale', 'blocked') then
    return api.lcia_result_error(
      'build_enqueue_conflict', 409,
      'V2 admission did not create the reserved convertible Worker job'
    );
  end if;

  update private.worker_jobs
  set payload_schema_version = 'lcia_result.package_build.request.v3',
      payload_json = payload_json || jsonb_build_object(
        'portalProjectionContractVersion', 'portal.lcia-projection.v1',
        'portalProjectionHashContractVersion',
          'portal.lcia-projection.int32be-frame-sha256.v1',
        'portalProjectionIdempotencyKey', v_idempotency_key,
        'portalProjectionRequest', v_request
      ),
      updated_at = clock_timestamp()
  where id = v_job.id
  returning * into v_job;

  insert into private.worker_job_events (
    job_id, event_type, status, details
  ) values (
    v_job.id,
    'portal_projection_v3_admitted',
    v_job.status,
    jsonb_build_object(
      'projectionContractVersion', 'portal.lcia-projection.v1',
      'hashContractVersion',
        'portal.lcia-projection.int32be-frame-sha256.v1'
    )
  );

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', jsonb_build_object(
      'buildId', v_job.subject_id,
      'workerJobId', v_job.id,
      'workerJob', private.worker_job_payload(v_job, false),
      'projectionContractVersion', 'portal.lcia-projection.v1'
    )
  );
exception
  when unique_violation then
    return api.lcia_result_error(
      'build_enqueue_conflict', 409,
      'A conflicting Portal LCIA V3 build already exists'
    );
end
$function$;

create function private.svc_portal_lcia_projection_worker_input_v1(
  p_build_worker_job_id uuid,
  p_lease_token uuid
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_job private.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false, 'code', 'service_role_required', 'status', 403
    );
  end if;
  select job.* into v_job
  from private.worker_jobs as job
  where job.id = p_build_worker_job_id;
  if v_job.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_job_not_found', 'status', 404
    );
  end if;
  if private.portal_lcia_projection_v3_job_binding_valid_v1(
    p_build_worker_job_id, p_lease_token
  ) is not true then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_lease_invalid', 'status', 409
    );
  end if;
  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'buildWorkerJobId', v_job.id,
      'buildId', v_job.subject_id,
      'payloadSchemaVersion', v_job.payload_schema_version,
      'projectionContractVersion', 'portal.lcia-projection.v1',
      'hashContractVersion',
        'portal.lcia-projection.int32be-frame-sha256.v1',
      'payload', v_job.payload_json,
      'payloadRef', v_job.payload_ref
    )
  );
end
$function$;

create function private.svc_portal_lcia_projection_package_mark_ready_v1(
  p_projection_id uuid,
  p_build_worker_job_id uuid,
  p_lease_token uuid,
  p_package_version text,
  p_snapshot_id uuid,
  p_result_id uuid,
  p_latest_all_unit_result_id uuid default null::uuid,
  p_result_artifact_ref jsonb default '{}'::jsonb,
  p_query_artifact_ref jsonb default '{}'::jsonb,
  p_artifact_manifest jsonb default '{}'::jsonb,
  p_available_impact_categories jsonb default '[]'::jsonb,
  p_default_impact_category text default null::text,
  p_package_result_hash text default null::text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_job private.worker_jobs%rowtype;
  v_result jsonb;
  v_package private.lcia_result_packages%rowtype;
  v_package_id uuid;
  v_projection_impacts jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false, 'code', 'service_role_required', 'status', 403
    );
  end if;
  if private.portal_lcia_safe_audit_v1(p_audit) is not true
     or private.portal_lcia_public_text_valid_v1(p_package_version, 256)
          is not true
     or jsonb_typeof(p_result_artifact_ref) <> 'object'
     or jsonb_typeof(p_query_artifact_ref) <> 'object'
     or jsonb_typeof(p_artifact_manifest) <> 'object'
     or p_result_artifact_ref ->> 'artifactSha256'
          !~ '^[0-9a-f]{64}$'
     or p_query_artifact_ref ->> 'artifactSha256'
          !~ '^[0-9a-f]{64}$'
     or p_artifact_manifest ->> 'bundleContentHash'
          !~ '^[0-9a-f]{64}$'
     or p_artifact_manifest ->> 'bundleManifestSha256'
          !~ '^[0-9a-f]{64}$'
     or p_artifact_manifest ->> 'lciaChunkSetSha256'
          !~ '^[0-9a-f]{64}$'
     or p_artifact_manifest ->> 'portalProjectionId'
          !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or p_artifact_manifest ->> 'portalProjectionContentHash'
          !~ '^[0-9a-f]{64}$'
     or jsonb_typeof(p_available_impact_categories) <> 'array'
     or (
       p_default_impact_category is not null
       and private.portal_lcia_public_text_valid_v1(
         p_default_impact_category, 512
       ) is not true
     )
     or coalesce(p_package_result_hash, '') !~ '^[0-9a-f]{64}$' then
    return jsonb_build_object(
      'ok', false, 'code', 'invalid_projection_request', 'status', 400
    );
  end if;

  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = p_projection_id
    and projection.build_worker_job_id = p_build_worker_job_id
  for share;
  if v_projection.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_found', 'status', 404
    );
  end if;
  if v_projection.status <> 'prepared' then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_prepared', 'status', 409
    );
  end if;
  if v_projection.stage_lease_token is distinct from p_lease_token
     or private.portal_lcia_projection_v3_job_binding_valid_v1(
       p_build_worker_job_id, p_lease_token
     ) is not true then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_lease_invalid', 'status', 409
    );
  end if;
  if v_projection.result_artifact_sha256
       <> p_result_artifact_ref ->> 'artifactSha256'
     or v_projection.query_artifact_sha256
       <> p_query_artifact_ref ->> 'artifactSha256'
     or v_projection.bundle_content_hash
       <> p_artifact_manifest ->> 'bundleContentHash'
     or v_projection.bundle_manifest_sha256
       <> p_artifact_manifest ->> 'bundleManifestSha256'
     or v_projection.lcia_chunk_set_sha256
       <> p_artifact_manifest ->> 'lciaChunkSetSha256'
     or v_projection.id::text
       <> p_artifact_manifest ->> 'portalProjectionId'
     or v_projection.content_hash
       <> p_artifact_manifest ->> 'portalProjectionContentHash' then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;
  select coalesce(
    jsonb_agg(to_jsonb(impact.method_id::text) order by impact.impact_index),
    '[]'::jsonb
  ) into v_projection_impacts
  from private.portal_lcia_projection_impact_axis as impact
  where impact.projection_id = v_projection.id;
  if jsonb_typeof(p_available_impact_categories) <> 'array'
     or p_available_impact_categories is distinct from v_projection_impacts then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select job.* into v_job
  from private.worker_jobs as job
  where job.id = p_build_worker_job_id
  for update;
  if v_job.payload_json ->> 'snapshot_id' is distinct from p_snapshot_id::text then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  -- The established insert trigger applies its exact certificate binding only
  -- to request.v2.  A row lock and transaction-local compatibility value let
  -- this V3-only wrapper reuse that unchanged trigger and legacy insert helper;
  -- no observer can see the temporary value and V1/V2 definitions/ACLs remain
  -- byte-stable.
  update private.worker_jobs
  set payload_schema_version = 'lcia_result.package_build.request.v2'
  where id = v_job.id;
  begin
    v_result := private.cmd_lcia_result_package_mark_ready_without_closure_recheck(
      p_build_worker_job_id,
      p_package_version,
      p_snapshot_id,
      p_result_id,
      p_latest_all_unit_result_id,
      p_result_artifact_ref,
      p_query_artifact_ref,
      p_artifact_manifest,
      p_available_impact_categories,
      p_default_impact_category,
      p_package_result_hash,
      p_audit
    );
  exception when others then
    update private.worker_jobs
    set payload_schema_version = 'lcia_result.package_build.request.v3'
    where id = v_job.id;
    raise;
  end;
  update private.worker_jobs
  set payload_schema_version = 'lcia_result.package_build.request.v3'
  where id = v_job.id;

  if coalesce((v_result ->> 'ok')::boolean, false) is not true then
    return v_result;
  end if;
  begin
    v_package_id := nullif(v_result -> 'data' ->> 'packageId', '')::uuid;
  exception when invalid_text_representation then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_package_binding_invalid', 'status', 409
    );
  end;
  select package.* into v_package
  from private.lcia_result_packages as package
  where package.id = v_package_id;
  if v_package.id is null
     or v_package.build_worker_job_id <> v_job.id
     or v_package.package_version <> p_package_version
     or v_package.package_result_hash <> p_package_result_hash
     or v_package.closure_certificate_hash
          <> v_projection.closure_certificate_hash
     or v_package.closure_snapshot_hash <> v_projection.snapshot_hash
     or v_package.artifact_manifest ->> 'portalProjectionId'
          <> v_projection.id::text
     or v_package.artifact_manifest ->> 'portalProjectionContentHash'
          <> v_projection.content_hash then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_package_binding_invalid', 'status', 409
    );
  end if;

  return jsonb_set(
    v_result,
    '{data,projection}',
    jsonb_build_object(
      'projectionId', v_projection.id,
      'contentHash', v_projection.content_hash,
      'hashContractVersion',
        'portal.lcia-projection.int32be-frame-sha256.v1'
    ),
    true
  );
end
$function$;

create function private.portal_lcia_projection_recompute_evidence_v1(
  p_projection_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $function$
declare
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_count bigint;
  v_valid_count bigint;
  v_bad_count bigint;
  v_process_axis_hash text;
  v_impact_axis_hash text;
  v_value_grid_hash text;
  v_relation_hash text;
  v_content_hash text;
  v_fields text[];
begin
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = p_projection_id;
  if v_projection.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_found', 'status', 404
    );
  end if;

  select count(*), count(*) filter (
    where process_index between 0 and v_projection.process_count - 1
  ) into v_count, v_valid_count
  from private.portal_lcia_projection_process_axis
  where projection_id = v_projection.id;
  if v_count <> v_projection.process_count
     or v_valid_count <> v_projection.process_count then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_incomplete', 'status', 409
    );
  end if;

  select count(*), count(*) filter (
    where impact_index between 0 and v_projection.impact_count - 1
  ) into v_count, v_valid_count
  from private.portal_lcia_projection_impact_axis
  where projection_id = v_projection.id;
  if v_count <> v_projection.impact_count
     or v_valid_count <> v_projection.impact_count then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_incomplete', 'status', 409
    );
  end if;

  select count(*), count(*) filter (
    where ordinal = process_index::bigint * v_projection.impact_count::bigint
      + impact_index::bigint + 1
      and ordinal between 1 and v_projection.expected_value_count
  ) into v_count, v_valid_count
  from private.portal_lcia_projection_values
  where projection_id = v_projection.id;
  if v_count <> v_projection.expected_value_count
     or v_valid_count <> v_projection.expected_value_count then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_incomplete', 'status', 409
    );
  end if;

  select count(*) into v_bad_count
  from private.portal_lcia_projection_process_axis as row
  where row.projection_id = v_projection.id
    and row.record_hash is distinct from
      private.portal_lcia_projection_sha256_fields_v1(
        'portal.lcia-projection.process.v1',
        'portal.lcia-projection.int32be-frame-sha256.v1',
        row.process_index::text,
        row.process_id::text, row.process_version,
        row.reference_flow_id::text, row.reference_flow_version,
        row.reference_exchange_internal_id, row.reference_flow_amount,
        row.reference_flow_direction, row.functional_unit_amount,
        row.functional_unit_unit,
        private.portal_lcia_localized_text_frame_hex_v1(
          row.functional_unit_description
        ),
        row.geography_code, row.geography_precision,
        row.reference_year::text, row.process_document_sha256
      );
  if v_bad_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select count(*) into v_bad_count
  from private.portal_lcia_projection_impact_axis as row
  where row.projection_id = v_projection.id
    and row.record_hash is distinct from
      private.portal_lcia_projection_sha256_fields_v1(
        'portal.lcia-projection.impact.v1',
        'portal.lcia-projection.int32be-frame-sha256.v1',
        row.impact_index::text,
        row.method_id::text, row.method_version, row.impact_id,
        private.portal_lcia_localized_text_frame_hex_v1(row.impact_name),
        row.unit, row.method_document_sha256
      );
  if v_bad_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select count(*) into v_bad_count
  from private.portal_lcia_projection_values as row
  where row.projection_id = v_projection.id
    and row.record_hash is distinct from
      private.portal_lcia_projection_sha256_fields_v1(
        'portal.lcia-projection.value.v1',
        'portal.lcia-projection.int32be-frame-sha256.v1',
        row.ordinal::text,
        row.process_index::text, row.impact_index::text, row.value_text
      );
  if v_bad_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select array[
    'portal.lcia-projection.relation.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    'process-axis', v_projection.process_count::text
  ] || coalesce(
    array_agg(field.value order by row.process_index, field.position),
    '{}'::text[]
  ) into v_fields
  from private.portal_lcia_projection_process_axis as row
  cross join lateral (
    values (1, (row.process_index + 1)::text), (2, row.record_hash)
  ) as field(position, value)
  where row.projection_id = v_projection.id;
  v_process_axis_hash := private.portal_lcia_projection_sha256_fields_v1(
    variadic v_fields
  );

  select array[
    'portal.lcia-projection.relation.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    'impact-axis', v_projection.impact_count::text
  ] || coalesce(
    array_agg(field.value order by row.impact_index, field.position),
    '{}'::text[]
  ) into v_fields
  from private.portal_lcia_projection_impact_axis as row
  cross join lateral (
    values (1, (row.impact_index + 1)::text), (2, row.record_hash)
  ) as field(position, value)
  where row.projection_id = v_projection.id;
  v_impact_axis_hash := private.portal_lcia_projection_sha256_fields_v1(
    variadic v_fields
  );

  select array[
    'portal.lcia-projection.relation.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    'value-grid', v_projection.expected_value_count::text
  ] || coalesce(
    array_agg(field.value order by row.ordinal, field.position),
    '{}'::text[]
  ) into v_fields
  from private.portal_lcia_projection_values as row
  cross join lateral (
    values (1, row.ordinal::text), (2, row.record_hash)
  ) as field(position, value)
  where row.projection_id = v_projection.id;
  v_value_grid_hash := private.portal_lcia_projection_sha256_fields_v1(
    variadic v_fields
  );

  v_relation_hash := private.portal_lcia_projection_sha256_fields_v1(
    'portal.lcia-projection.grid-relation.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    v_projection.process_count::text,
    v_projection.impact_count::text,
    v_projection.expected_value_count::text,
    'ordinal=processIndex*impactCount+impactIndex+1',
    v_process_axis_hash, v_impact_axis_hash, v_value_grid_hash
  );
  v_content_hash := private.portal_lcia_projection_sha256_fields_v1(
    'portal.lcia-projection.content.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    v_projection.projection_contract_version,
    v_projection.input_manifest_hash,
    v_projection.closure_certificate_hash,
    v_projection.snapshot_hash,
    v_projection.closure_bundle_hash,
    v_projection.snapshot_index_sha256,
    v_projection.snapshot_build_contract_hash,
    v_projection.bundle_content_hash,
    v_projection.bundle_manifest_sha256,
    v_projection.lcia_chunk_set_sha256,
    v_projection.result_artifact_sha256,
    v_projection.query_artifact_sha256,
    v_projection.process_count::text,
    v_projection.impact_count::text,
    v_projection.expected_value_count::text,
    v_process_axis_hash, v_impact_axis_hash, v_value_grid_hash,
    v_relation_hash
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'processAxisHash', v_process_axis_hash,
      'impactAxisHash', v_impact_axis_hash,
      'valueGridHash', v_value_grid_hash,
      'relationHash', v_relation_hash,
      'contentHash', v_content_hash,
      'processCount', v_projection.process_count,
      'impactCount', v_projection.impact_count,
      'valueCount', v_projection.expected_value_count
    )
  );
end
$function$;

revoke all on function private.portal_lcia_projection_recompute_evidence_v1(uuid)
  from public, anon, authenticated, service_role;

create function api.qry_portal_lcia_projection_prepare_v1(
  p_package_id uuid,
  p_lcia_result_publication_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $function$
declare
  v_actor uuid := auth.uid();
  v_job private.worker_jobs%rowtype;
  v_package private.lcia_result_packages%rowtype;
  v_publication private.lcia_result_publications%rowtype;
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_match_count integer;
  v_projection_methods jsonb;
begin
  if v_actor is null then
    return api.lcia_result_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not api.lcia_result_is_manager() then
    return api.lcia_result_error(
      'not_data_product_manager', 403,
      'Data product manager role is required'
    );
  end if;
  if p_package_id is null or p_lcia_result_publication_id is null then
    return api.lcia_result_error(
      'invalid_projection_request', 400,
      'Package and LCIA result publication identities are required'
    );
  end if;

  select package.* into v_package
  from private.lcia_result_packages as package
  where package.id = p_package_id;
  select publication.* into v_publication
  from private.lcia_result_publications as publication
  where publication.id = p_lcia_result_publication_id;
  if v_package.id is null
     or v_publication.id is null
     or v_publication.package_id <> v_package.id then
    return api.lcia_result_error(
      'projection_package_not_found', 404,
      'Matching LCIA package and publication were not found'
    );
  end if;
  if not v_publication.is_current
     or v_publication.status <> 'current'
     or v_publication.publication_series_key <> 'global'
     or v_publication.publication_channel <> 'public'
     or v_publication.visibility_scope <> 'public'
     or v_publication.published_at is null then
    return api.lcia_result_error(
      'publication_not_current', 409,
      'Only the exact current public LCIA result publication can prepare'
    );
  end if;
  select job.* into v_job
  from private.worker_jobs as job
  where job.id = v_package.build_worker_job_id;
  if v_job.job_kind <> 'lcia_result.package_build'
     or v_job.payload_schema_version <> 'lcia_result.package_build.request.v3'
     or v_job.payload_json ->> 'portalProjectionContractVersion'
          <> 'portal.lcia-projection.v1'
     or v_package.status <> 'preview_ready'
     or coalesce(v_package.package_result_hash, '') !~ '^[0-9a-f]{64}$' then
    return api.lcia_result_error(
      'projection_package_not_ready', 409,
      'Package is not a ready Portal LCIA V3 package'
    );
  end if;

  select count(*)
  into v_match_count
  from private.portal_lcia_projection_headers as projection
  where projection.build_worker_job_id = v_job.id
    and projection.status = 'prepared'
    and projection.input_manifest_hash = v_package.input_manifest_hash
    and projection.closure_certificate_hash = v_package.closure_certificate_hash
    and projection.snapshot_hash = v_package.closure_snapshot_hash
    and projection.result_artifact_sha256
          = v_package.result_artifact_ref ->> 'artifactSha256'
    and projection.query_artifact_sha256
          = v_package.query_artifact_ref ->> 'artifactSha256'
    and projection.bundle_content_hash
          = v_package.artifact_manifest ->> 'bundleContentHash'
    and projection.bundle_manifest_sha256
          = v_package.artifact_manifest ->> 'bundleManifestSha256'
    and projection.lcia_chunk_set_sha256
          = v_package.artifact_manifest ->> 'lciaChunkSetSha256'
    and projection.id::text
          = v_package.artifact_manifest ->> 'portalProjectionId'
    and projection.content_hash
          = v_package.artifact_manifest ->> 'portalProjectionContentHash'
    and projection.process_count = v_package.included_input_count
    and projection.impact_count = jsonb_array_length(
      v_package.available_impact_categories
    );

  if v_match_count = 0 then
    return api.lcia_result_error(
      'projection_not_prepared', 409,
      'No prepared projection exactly matches the package evidence'
    );
  end if;
  if v_match_count > 1 then
    return api.lcia_result_error(
      'projection_conflict', 409,
      'More than one prepared projection matches the package evidence'
    );
  end if;
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.build_worker_job_id = v_job.id
    and projection.status = 'prepared'
    and projection.input_manifest_hash = v_package.input_manifest_hash
    and projection.closure_certificate_hash = v_package.closure_certificate_hash
    and projection.snapshot_hash = v_package.closure_snapshot_hash
    and projection.result_artifact_sha256
          = v_package.result_artifact_ref ->> 'artifactSha256'
    and projection.query_artifact_sha256
          = v_package.query_artifact_ref ->> 'artifactSha256'
    and projection.bundle_content_hash
          = v_package.artifact_manifest ->> 'bundleContentHash'
    and projection.bundle_manifest_sha256
          = v_package.artifact_manifest ->> 'bundleManifestSha256'
    and projection.lcia_chunk_set_sha256
          = v_package.artifact_manifest ->> 'lciaChunkSetSha256'
    and projection.id::text
          = v_package.artifact_manifest ->> 'portalProjectionId'
    and projection.content_hash
          = v_package.artifact_manifest ->> 'portalProjectionContentHash'
    and projection.process_count = v_package.included_input_count
    and projection.impact_count = jsonb_array_length(
      v_package.available_impact_categories
    );
  select coalesce(
    jsonb_agg(to_jsonb(impact.method_id::text) order by impact.impact_index),
    '[]'::jsonb
  ) into v_projection_methods
  from private.portal_lcia_projection_impact_axis as impact
  where impact.projection_id = v_projection.id;
  if v_package.available_impact_categories is distinct from v_projection_methods
     or exists (
       select 1
       from private.portal_lcia_projection_process_axis as process_row
       where process_row.projection_id = v_projection.id
         and (
           v_package.input_manifest -> 'processes' -> process_row.process_index
             ->> 'id' is distinct from process_row.process_id::text
           or v_package.input_manifest -> 'processes' -> process_row.process_index
             ->> 'version' is distinct from process_row.process_version
         )
     ) then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection axes do not match the exact package manifest identities'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'projectionId', v_projection.id,
      'buildWorkerJobId', v_projection.build_worker_job_id,
      'packageId', v_package.id,
      'lciaResultPublicationId', v_publication.id,
      'packageVersion', v_package.package_version,
      'packageResultHash', v_package.package_result_hash,
      'status', v_projection.status,
      'projectionContractVersion', v_projection.projection_contract_version,
      'hashContractVersion',
        'portal.lcia-projection.int32be-frame-sha256.v1',
      'processCount', v_projection.process_count,
      'impactCount', v_projection.impact_count,
      'valueCount', v_projection.expected_value_count,
      'processAxisHash', v_projection.process_axis_hash,
      'impactAxisHash', v_projection.impact_axis_hash,
      'valueGridHash', v_projection.value_grid_hash,
      'relationHash', v_projection.relation_hash,
      'contentHash', v_projection.content_hash,
      'publishedAt', private.portal_timestamp_v1(v_publication.published_at)
    )
  );
end
$function$;

create function api.cmd_portal_lcia_projection_finalize_publication_v1(
  p_projection_id uuid,
  p_lcia_result_publication_id uuid,
  p_package_version text,
  p_package_result_hash text,
  p_projection_content_hash text,
  p_idempotency_key text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_actor uuid := auth.uid();
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_publication private.lcia_result_publications%rowtype;
  v_package private.lcia_result_packages%rowtype;
  v_job private.worker_jobs%rowtype;
  v_existing private.portal_lcia_projection_publications%rowtype;
  v_binding private.portal_lcia_projection_publications%rowtype;
  v_now timestamptz := clock_timestamp();
  v_evidence_hash text;
  v_recomputed jsonb;
  v_projection_impacts jsonb;
  v_idempotency_key text := btrim(coalesce(p_idempotency_key, ''));
begin
  if v_actor is null then
    return api.lcia_result_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not api.lcia_result_is_manager() then
    return api.lcia_result_error(
      'not_data_product_manager', 403,
      'Data product manager role is required'
    );
  end if;
  if p_projection_id is null
     or p_lcia_result_publication_id is null
     or coalesce(p_package_result_hash, '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_projection_content_hash, '') !~ '^[0-9a-f]{64}$'
     or private.portal_lcia_public_text_valid_v1(p_package_version, 256)
          is not true
     or length(v_idempotency_key) not between 1 and 256
     or private.portal_lcia_safe_audit_v1(p_audit) is not true then
    return api.lcia_result_error(
      'invalid_projection_request', 400,
      'Invalid Portal LCIA projection finalization request'
    );
  end if;

  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = p_projection_id
  for share;
  if v_projection.id is null then
    return api.lcia_result_error(
      'projection_not_found', 404, 'Projection was not found'
    );
  end if;
  if v_projection.status <> 'prepared'
     or v_projection.content_hash <> p_projection_content_hash then
    return api.lcia_result_error(
      'projection_not_prepared', 409,
      'Projection is not prepared with the requested content hash'
    );
  end if;

  select publication.* into v_publication
  from private.lcia_result_publications as publication
  where publication.id = p_lcia_result_publication_id
  for update;
  if v_publication.id is null then
    return api.lcia_result_error(
      'publication_not_found', 404, 'LCIA result publication was not found'
    );
  end if;
  if not v_publication.is_current
     or v_publication.status <> 'current'
     or v_publication.publication_series_key <> 'global'
     or v_publication.publication_channel <> 'public'
     or v_publication.visibility_scope <> 'public'
     or v_publication.published_at is null then
    return api.lcia_result_error(
      'publication_not_current', 409,
      'Only the exact current public LCIA result publication can finalize'
    );
  end if;

  select package.* into v_package
  from private.lcia_result_packages as package
  where package.id = v_publication.package_id
  for share;
  if v_package.id is null
     or v_package.status <> 'preview_ready'
     or v_package.package_version <> p_package_version
     or v_package.package_result_hash <> p_package_result_hash
     or v_package.build_worker_job_id <> v_projection.build_worker_job_id
     or v_package.input_manifest_hash <> v_projection.input_manifest_hash
     or v_package.closure_certificate_hash
          <> v_projection.closure_certificate_hash
     or v_package.closure_snapshot_hash <> v_projection.snapshot_hash
     or v_package.result_artifact_ref ->> 'artifactSha256'
          <> v_projection.result_artifact_sha256
     or v_package.query_artifact_ref ->> 'artifactSha256'
          <> v_projection.query_artifact_sha256
     or v_package.artifact_manifest ->> 'bundleContentHash'
          <> v_projection.bundle_content_hash
     or v_package.artifact_manifest ->> 'bundleManifestSha256'
          <> v_projection.bundle_manifest_sha256
     or v_package.artifact_manifest ->> 'lciaChunkSetSha256'
          <> v_projection.lcia_chunk_set_sha256
     or v_package.artifact_manifest ->> 'portalProjectionId'
          <> v_projection.id::text
     or v_package.artifact_manifest ->> 'portalProjectionContentHash'
          <> v_projection.content_hash
     or v_package.included_input_count <> v_projection.process_count
     or jsonb_array_length(v_package.available_impact_categories)
          <> v_projection.impact_count then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection and package evidence do not exactly match'
    );
  end if;
  select coalesce(
    jsonb_agg(to_jsonb(impact.method_id::text) order by impact.impact_index),
    '[]'::jsonb
  ) into v_projection_impacts
  from private.portal_lcia_projection_impact_axis as impact
  where impact.projection_id = v_projection.id;
  if v_package.available_impact_categories is distinct from v_projection_impacts
     or exists (
       select 1
       from private.portal_lcia_projection_process_axis as process_row
       where process_row.projection_id = v_projection.id
         and (
           v_package.input_manifest -> 'processes' -> process_row.process_index
             ->> 'id' is distinct from process_row.process_id::text
           or v_package.input_manifest -> 'processes' -> process_row.process_index
             ->> 'version' is distinct from process_row.process_version
         )
     ) then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection axes do not match the exact package manifest identities'
    );
  end if;

  select job.* into v_job
  from private.worker_jobs as job
  where job.id = v_projection.build_worker_job_id;
  if v_job.id is null
     or v_job.job_kind <> 'lcia_result.package_build'
     or v_job.payload_schema_version <> 'lcia_result.package_build.request.v3'
     or v_job.payload_json ->> 'portalProjectionContractVersion'
          <> 'portal.lcia-projection.v1'
     or v_job.payload_json ->> 'input_manifest_hash'
          <> v_projection.input_manifest_hash
     or v_job.payload_json ->> 'closure_certificate_hash'
          <> v_projection.closure_certificate_hash
     or v_job.payload_json ->> 'snapshot_hash'
          <> v_projection.snapshot_hash
     or v_job.payload_json ->> 'closure_bundle_hash'
          <> v_projection.closure_bundle_hash
     or v_job.payload_json ->> 'snapshot_index_sha256'
          <> v_projection.snapshot_index_sha256
     or v_job.payload_json ->> 'snapshot_build_contract_hash'
          <> v_projection.snapshot_build_contract_hash then
    return api.lcia_result_error(
      'projection_job_contract_invalid', 409,
      'Projection source job is not the exact Portal LCIA V3 contract'
    );
  end if;
  if jsonb_typeof(v_job.payload_json -> 'lcia_method_set') <> 'array'
     or jsonb_array_length(v_job.payload_json -> 'lcia_method_set')
          <> v_projection.impact_count
     or exists (
       select 1
       from private.portal_lcia_projection_impact_axis as impact_row
       where impact_row.projection_id = v_projection.id
         and (
           v_job.payload_json -> 'lcia_method_set' -> impact_row.impact_index
             ->> 'id' is distinct from impact_row.method_id::text
           or v_job.payload_json -> 'lcia_method_set' -> impact_row.impact_index
             ->> 'version' is distinct from impact_row.method_version
         )
     ) then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection Method axis does not match the certified V3 request'
    );
  end if;

  if (select count(*) from private.portal_lcia_projection_process_axis
      where projection_id = v_projection.id) <> v_projection.process_count
     or (select count(*) from private.portal_lcia_projection_impact_axis
         where projection_id = v_projection.id) <> v_projection.impact_count
     or (select count(*) from private.portal_lcia_projection_values
         where projection_id = v_projection.id)
          <> v_projection.expected_value_count then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection record counts changed after preparation'
    );
  end if;
  v_recomputed := private.portal_lcia_projection_recompute_evidence_v1(
    v_projection.id
  );
  if coalesce((v_recomputed ->> 'ok')::boolean, false) is not true
     or v_recomputed -> 'data' ->> 'processAxisHash'
          is distinct from v_projection.process_axis_hash
     or v_recomputed -> 'data' ->> 'impactAxisHash'
          is distinct from v_projection.impact_axis_hash
     or v_recomputed -> 'data' ->> 'valueGridHash'
          is distinct from v_projection.value_grid_hash
     or v_recomputed -> 'data' ->> 'relationHash'
          is distinct from v_projection.relation_hash
     or v_recomputed -> 'data' ->> 'contentHash'
          is distinct from v_projection.content_hash then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection hashes no longer match the typed persisted rows'
    );
  end if;

  select binding.* into v_existing
  from private.portal_lcia_projection_publications as binding
  where binding.lcia_result_publication_id = v_publication.id
  for update;
  if v_existing.id is not null then
    if v_existing.status = 'finalized'
       and v_existing.revoked_at is null
       and v_existing.projection_id = v_projection.id
       and v_existing.package_id = v_package.id
       and v_existing.package_version = p_package_version
       and v_existing.package_result_hash = p_package_result_hash
       and v_existing.projection_content_hash = p_projection_content_hash
       and v_existing.idempotency_key = v_idempotency_key then
      return jsonb_build_object(
        'ok', true,
        'reused', true,
        'data', jsonb_build_object(
          'projectionPublicationId', v_existing.id,
          'projectionId', v_existing.projection_id,
          'lciaResultPublicationId', v_existing.lcia_result_publication_id,
          'packageId', v_existing.package_id,
          'status', v_existing.status,
          'contentHash', v_existing.projection_content_hash,
          'evidenceHash', v_existing.evidence_hash,
          'finalizedAt', private.portal_timestamp_v1(v_existing.finalized_at)
        )
      );
    end if;
    return api.lcia_result_error(
      'projection_conflict', 409,
      'LCIA result publication is bound to different projection content'
    );
  end if;

  v_evidence_hash := private.portal_lcia_projection_sha256_fields_v1(
    'portal.lcia-projection.publication-evidence.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    v_projection.id::text,
    v_projection.content_hash,
    v_publication.id::text,
    v_package.id::text,
    v_package.package_version,
    v_package.package_result_hash,
    private.portal_timestamp_v1(v_publication.published_at),
    v_projection.input_manifest_hash,
    v_projection.closure_certificate_hash,
    v_projection.snapshot_hash,
    v_projection.closure_bundle_hash,
    v_projection.bundle_content_hash,
    v_projection.bundle_manifest_sha256,
    v_projection.lcia_chunk_set_sha256,
    v_projection.result_artifact_sha256,
    v_projection.query_artifact_sha256,
    v_projection.process_count::text,
    v_projection.impact_count::text,
    v_projection.expected_value_count::text
  );

  insert into private.portal_lcia_projection_publications (
    projection_id,
    lcia_result_publication_id,
    package_id,
    package_version,
    package_result_hash,
    projection_content_hash,
    evidence_hash,
    source_published_at,
    idempotency_key,
    status,
    finalized_by,
    finalized_at
  ) values (
    v_projection.id,
    v_publication.id,
    v_package.id,
    v_package.package_version,
    v_package.package_result_hash,
    v_projection.content_hash,
    v_evidence_hash,
    v_publication.published_at,
    v_idempotency_key,
    'finalized',
    v_actor,
    v_now
  ) returning * into v_binding;

  insert into private.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    'cmd_portal_lcia_projection_finalize_publication_v1',
    v_actor,
    'portal_lcia_projection_publications',
    v_binding.id,
    v_binding.package_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'projectionId', v_projection.id,
      'lciaResultPublicationId', v_publication.id,
      'packageId', v_package.id,
      'contentHash', v_projection.content_hash,
      'evidenceHash', v_evidence_hash
    )
  );

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', jsonb_build_object(
      'projectionPublicationId', v_binding.id,
      'projectionId', v_binding.projection_id,
      'lciaResultPublicationId', v_binding.lcia_result_publication_id,
      'packageId', v_binding.package_id,
      'status', v_binding.status,
      'contentHash', v_binding.projection_content_hash,
      'evidenceHash', v_binding.evidence_hash,
      'finalizedAt', private.portal_timestamp_v1(v_binding.finalized_at)
    )
  );
exception
  when unique_violation then
    return api.lcia_result_error(
      'projection_conflict', 409,
      'A conflicting projection publication binding already exists'
    );
end
$function$;

create function api.qry_portal_lcia_projection_publication_readback_v1(
  p_lcia_result_publication_id uuid,
  p_projection_content_hash text
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $function$
declare
  v_actor uuid := auth.uid();
  v_binding record;
  v_projection record;
  v_recomputed jsonb;
  v_publicly_visible boolean;
begin
  if v_actor is null then
    return api.lcia_result_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not api.lcia_result_is_manager() then
    return api.lcia_result_error(
      'not_data_product_manager', 403,
      'Data product manager role is required'
    );
  end if;
  if p_lcia_result_publication_id is null
     or coalesce(p_projection_content_hash, '') !~ '^[0-9a-f]{64}$' then
    return api.lcia_result_error(
      'invalid_projection_request', 400, 'Invalid projection readback request'
    );
  end if;

  select binding.* into v_binding
  from private.portal_lcia_projection_publications as binding
  where binding.lcia_result_publication_id = p_lcia_result_publication_id;
  if v_binding.id is null then
    return api.lcia_result_error(
      'projection_publication_not_found', 404,
      'Projection publication binding was not found'
    );
  end if;
  if v_binding.projection_content_hash <> p_projection_content_hash then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection content hash does not match the binding'
    );
  end if;
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = v_binding.projection_id;
  v_recomputed := private.portal_lcia_projection_recompute_evidence_v1(
    v_projection.id
  );
  if coalesce((v_recomputed ->> 'ok')::boolean, false) is not true
     or v_recomputed -> 'data' ->> 'processAxisHash'
          is distinct from v_projection.process_axis_hash
     or v_recomputed -> 'data' ->> 'impactAxisHash'
          is distinct from v_projection.impact_axis_hash
     or v_recomputed -> 'data' ->> 'valueGridHash'
          is distinct from v_projection.value_grid_hash
     or v_recomputed -> 'data' ->> 'relationHash'
          is distinct from v_projection.relation_hash
     or v_recomputed -> 'data' ->> 'contentHash'
          is distinct from v_projection.content_hash
     or v_projection.content_hash is distinct from v_binding.projection_content_hash then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection readback does not match the typed persisted rows'
    );
  end if;
  v_publicly_visible := private.portal_lcia_projection_is_public_v1(
    v_projection.id
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'projectionPublicationId', v_binding.id,
      'projectionId', v_binding.projection_id,
      'lciaResultPublicationId', v_binding.lcia_result_publication_id,
      'packageId', v_binding.package_id,
      'packageVersion', v_binding.package_version,
      'status', v_binding.status,
      'isCurrent', coalesce(v_publicly_visible, false),
      'isPubliclyVisible', coalesce(v_publicly_visible, false),
      'contentHash', v_binding.projection_content_hash,
      'evidenceHash', v_binding.evidence_hash,
      'processCount', v_projection.process_count,
      'impactCount', v_projection.impact_count,
      'valueCount', v_projection.expected_value_count,
      'finalizedAt', private.portal_timestamp_v1(v_binding.finalized_at),
      'revokedAt', case when v_binding.revoked_at is null then null
        else private.portal_timestamp_v1(v_binding.revoked_at) end
    )
  );
end
$function$;

create function api.cmd_portal_lcia_projection_revoke_publication_v1(
  p_lcia_result_publication_id uuid,
  p_projection_content_hash text,
  p_reason text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_actor uuid := auth.uid();
  v_binding private.portal_lcia_projection_publications%rowtype;
  v_reason text := nullif(btrim(coalesce(p_reason, '')), '');
  v_now timestamptz := clock_timestamp();
begin
  if v_actor is null then
    return api.lcia_result_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not api.lcia_result_is_manager() then
    return api.lcia_result_error(
      'not_data_product_manager', 403,
      'Data product manager role is required'
    );
  end if;
  if p_lcia_result_publication_id is null
     or coalesce(p_projection_content_hash, '') !~ '^[0-9a-f]{64}$'
     or private.portal_lcia_public_text_valid_v1(v_reason, 2000) is not true
     or private.portal_lcia_safe_audit_v1(p_audit) is not true then
    return api.lcia_result_error(
      'invalid_projection_request', 400, 'Invalid projection revoke request'
    );
  end if;

  select binding.* into v_binding
  from private.portal_lcia_projection_publications as binding
  where binding.lcia_result_publication_id = p_lcia_result_publication_id
  for update;
  if v_binding.id is null then
    return api.lcia_result_error(
      'projection_publication_not_found', 404,
      'Projection publication binding was not found'
    );
  end if;
  if v_binding.projection_content_hash <> p_projection_content_hash then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection content hash does not match the binding'
    );
  end if;
  if v_binding.status = 'revoked' then
    return jsonb_build_object(
      'ok', true,
      'reused', true,
      'data', jsonb_build_object(
        'projectionPublicationId', v_binding.id,
        'lciaResultPublicationId', v_binding.lcia_result_publication_id,
        'status', v_binding.status,
        'revokedAt', private.portal_timestamp_v1(v_binding.revoked_at)
      )
    );
  end if;

  update private.portal_lcia_projection_publications
  set status = 'revoked',
      revoked_by = v_actor,
      revoked_at = v_now,
      revoke_reason = v_reason
  where id = v_binding.id
  returning * into v_binding;

  insert into private.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    'cmd_portal_lcia_projection_revoke_publication_v1',
    v_actor,
    'portal_lcia_projection_publications',
    v_binding.id,
    v_binding.package_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'lciaResultPublicationId', v_binding.lcia_result_publication_id,
      'contentHash', v_binding.projection_content_hash,
      'reason', v_reason
    )
  );

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', jsonb_build_object(
      'projectionPublicationId', v_binding.id,
      'lciaResultPublicationId', v_binding.lcia_result_publication_id,
      'status', v_binding.status,
      'revokedAt', private.portal_timestamp_v1(v_binding.revoked_at)
    )
  );
end
$function$;

create function private.portal_lcia_projection_is_public_v1(
  p_projection_id uuid
)
returns boolean
language sql
stable
security definer
set search_path = ''
as $function$
  select exists (
    select 1
    from private.portal_lcia_projection_publications as binding
    join private.portal_lcia_projection_headers as projection
      on projection.id = binding.projection_id
    join private.lcia_result_publications as publication
      on publication.id = binding.lcia_result_publication_id
    join private.lcia_result_packages as package
      on package.id = binding.package_id
     and package.id = publication.package_id
    join private.worker_jobs as job
      on job.id = projection.build_worker_job_id
     and job.id = package.build_worker_job_id
    where binding.projection_id = p_projection_id
      and binding.status = 'finalized'
      and binding.revoked_at is null
      and projection.status = 'prepared'
      and projection.content_hash = binding.projection_content_hash
      and publication.is_current
      and publication.status = 'current'
      and publication.publication_series_key = 'global'
      and publication.publication_channel = 'public'
      and publication.visibility_scope = 'public'
      and publication.published_at = binding.source_published_at
      and package.status = 'preview_ready'
      and package.package_version = binding.package_version
      and package.package_result_hash = binding.package_result_hash
      and package.artifact_manifest ->> 'portalProjectionId'
            = projection.id::text
      and package.artifact_manifest ->> 'portalProjectionContentHash'
            = projection.content_hash
      and job.job_kind = 'lcia_result.package_build'
      and job.payload_schema_version = 'lcia_result.package_build.request.v3'
      and job.payload_json ->> 'portalProjectionContractVersion'
            = 'portal.lcia-projection.v1'
  )
$function$;

revoke all on function private.portal_lcia_projection_is_public_v1(uuid)
  from public, anon, authenticated, service_role;
grant execute on function private.portal_lcia_projection_is_public_v1(uuid)
  to portal_public_executor;

create policy portal_public_executor_select_lcia_projection_headers_v1
on private.portal_lcia_projection_headers
for select
to portal_public_executor
using (private.portal_lcia_projection_is_public_v1(id));

create policy portal_public_executor_select_lcia_projection_process_axis_v1
on private.portal_lcia_projection_process_axis
for select
to portal_public_executor
using (private.portal_lcia_projection_is_public_v1(projection_id));

create policy portal_public_executor_select_lcia_projection_impact_axis_v1
on private.portal_lcia_projection_impact_axis
for select
to portal_public_executor
using (private.portal_lcia_projection_is_public_v1(projection_id));

create policy portal_public_executor_select_lcia_projection_values_v1
on private.portal_lcia_projection_values
for select
to portal_public_executor
using (private.portal_lcia_projection_is_public_v1(projection_id));

create policy portal_public_executor_select_lcia_projection_publications_v1
on private.portal_lcia_projection_publications
for select
to portal_public_executor
using (private.portal_lcia_projection_is_public_v1(projection_id));

grant select (
  id, status, process_count, impact_count, expected_value_count, content_hash
) on private.portal_lcia_projection_headers to portal_public_executor;
grant select (
  projection_id, process_index, process_id, process_version,
  functional_unit_amount, functional_unit_unit,
  functional_unit_description, geography_code, geography_precision,
  reference_year
) on private.portal_lcia_projection_process_axis to portal_public_executor;
grant select (
  projection_id, impact_index, method_id, method_version,
  impact_id, impact_name, unit
) on private.portal_lcia_projection_impact_axis to portal_public_executor;
grant select (
  projection_id, ordinal, process_index, impact_index,
  value_text, value_numeric
) on private.portal_lcia_projection_values to portal_public_executor;
grant select (
  id, projection_id, lcia_result_publication_id, package_id,
  package_version, projection_content_hash, evidence_hash,
  source_published_at, status, revoked_at
) on private.portal_lcia_projection_publications to portal_public_executor;

create function api.portal_get_published_lcia_values_v1(
  p_mode text,
  p_process_refs jsonb,
  p_impact_ref text,
  p_cursor text,
  p_limit integer
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
set statement_timeout = '8s'
as $function$
declare
  v_mode text := btrim(coalesce(p_mode, ''));
  v_impact_ref text := nullif(btrim(coalesce(p_impact_ref, '')), '');
  v_limit integer := coalesce(p_limit, 50);
  v_ref_count integer;
  v_distinct_ref_count integer;
  v_impact_match_count integer;
  v_query_hash text;
  v_query_fields text[];
  v_cursor jsonb;
  v_cursor_request_order integer;
  v_cursor_ordinal bigint;
  v_cursor_sort_value text;
  v_cursor_sort_numeric numeric;
  v_binding record;
  v_projection record;
  v_rows jsonb := '[]'::jsonb;
  v_next_cursor text;
begin
  if v_mode not in (
       'process_all_impacts',
       'processes_one_impact',
       'ranked_processes_one_impact'
     )
     or v_limit not between 1 and 50
     or jsonb_typeof(p_process_refs) is distinct from 'array' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  v_ref_count := jsonb_array_length(p_process_refs);
  if v_ref_count not between 1 and 50
     or (v_mode = 'process_all_impacts' and v_ref_count <> 1)
     or (v_mode = 'process_all_impacts' and v_impact_ref is not null)
     or (v_mode <> 'process_all_impacts'
         and (v_impact_ref is null or length(v_impact_ref) > 512))
     or exists (
       select 1
       from jsonb_array_elements(p_process_refs) as item(value)
       where private.portal_lcia_json_object_has_keys_v1(
         item.value, array['id', 'version']
       ) is not true
         or jsonb_typeof(item.value -> 'id') <> 'string'
         or jsonb_typeof(item.value -> 'version') <> 'string'
         or coalesce(item.value ->> 'id', '')
              !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         or coalesce(item.value ->> 'version', '')
              !~ '^\d{2}\.\d{2}\.\d{3}$'
     ) then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  select count(distinct (item.value ->> 'id', item.value ->> 'version'))
  into v_distinct_ref_count
  from jsonb_array_elements(p_process_refs) as item(value);
  if v_distinct_ref_count <> v_ref_count then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;

  select
    binding.id,
    binding.projection_id,
    binding.lcia_result_publication_id,
    binding.package_id,
    binding.package_version,
    binding.projection_content_hash,
    binding.evidence_hash,
    binding.source_published_at,
    binding.status,
    binding.revoked_at
  into v_binding
  from private.portal_lcia_projection_publications as binding
  where binding.status = 'finalized'
  order by binding.source_published_at desc, binding.id
  limit 1;
  if v_binding.id is null then
    return null;
  end if;
  select
    projection.id,
    projection.status,
    projection.process_count,
    projection.impact_count,
    projection.expected_value_count,
    projection.content_hash
  into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = v_binding.projection_id;
  if v_projection.id is null then
    return null;
  end if;
  if v_mode <> 'process_all_impacts' then
    select count(*) into v_impact_match_count
    from private.portal_lcia_projection_impact_axis as impact_row
    where impact_row.projection_id = v_projection.id
      and impact_row.impact_id = v_impact_ref;
    if v_impact_match_count > 1 then
      raise exception using errcode = 'P0001', message = 'portal lcia unavailable';
    end if;
  end if;

  select array[
    'portal.published-lcia-query.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    v_binding.lcia_result_publication_id::text,
    v_binding.projection_content_hash,
    v_mode,
    coalesce(v_impact_ref, ''),
    v_ref_count::text
  ] || array_agg(field.value order by ref.ordinality, field.position)
  into v_query_fields
  from jsonb_array_elements(p_process_refs)
    with ordinality as ref(value, ordinality)
  cross join lateral (
    values (1, ref.value ->> 'id'), (2, ref.value ->> 'version')
  ) as field(position, value);
  v_query_hash := private.portal_lcia_projection_sha256_fields_v1(
    variadic v_query_fields
  );

  if p_cursor is not null then
    v_cursor := private.portal_cursor_decode_v1(p_cursor);
    if v_cursor is null
       or (select count(*) from jsonb_object_keys(v_cursor)) <> 8
       or v_cursor ->> 'v' <> '1'
       or v_cursor ->> 'publicationId'
            <> v_binding.lcia_result_publication_id::text
       or v_cursor ->> 'contentHash' <> v_binding.projection_content_hash
       or v_cursor ->> 'mode' <> v_mode
       or v_cursor ->> 'queryHash' <> v_query_hash
       or coalesce(v_cursor ->> 'requestOrder', '') !~ '^\d+$'
       or coalesce(v_cursor ->> 'ordinal', '') !~ '^\d+$'
       or jsonb_typeof(v_cursor -> 'sortValue') <> 'string' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
    begin
      v_cursor_request_order := (v_cursor ->> 'requestOrder')::integer;
      v_cursor_ordinal := (v_cursor ->> 'ordinal')::bigint;
    exception when others then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end;
    v_cursor_sort_value := v_cursor ->> 'sortValue';
    if v_mode = 'ranked_processes_one_impact' then
      if private.portal_canonical_decimal_v1(v_cursor_sort_value)
           is distinct from v_cursor_sort_value then
        raise exception using errcode = '22023', message = 'invalid portal request';
      end if;
      v_cursor_sort_numeric := v_cursor_sort_value::numeric;
    elsif v_cursor_sort_value <> '' then
      raise exception using errcode = '22023', message = 'invalid portal request';
    end if;
  end if;

  with refs as materialized (
    select
      ref.ordinality::integer as request_order,
      (ref.value ->> 'id')::uuid as process_id,
      ref.value ->> 'version' as process_version
    from jsonb_array_elements(p_process_refs)
      with ordinality as ref(value, ordinality)
  ), eligible as materialized (
    select
      refs.request_order,
      process_row.process_index,
      impact_row.impact_index,
      value_row.ordinal,
      value_row.value_text,
      value_row.value_numeric,
      process_row.process_id,
      process_row.process_version,
      process_row.functional_unit_amount,
      process_row.functional_unit_unit,
      process_row.functional_unit_description,
      process_row.geography_code,
      process_row.geography_precision,
      process_row.reference_year,
      impact_row.method_id,
      impact_row.method_version,
      impact_row.impact_id,
      impact_row.impact_name,
      impact_row.unit
    from refs
    join private.portal_lcia_projection_process_axis as process_row
      on process_row.projection_id = v_projection.id
     and process_row.process_id = refs.process_id
     and process_row.process_version = refs.process_version
    join public.processes as public_process
      on public_process.id = process_row.process_id
     and public_process.version::text = process_row.process_version
     and public_process.state_code = 100
     and (
       private.portal_capabilities_v1(
         'process', public_process.state_code, public_process.json
       ) ->> 'exchangesVisible'
     )::boolean
    join private.portal_lcia_projection_values as value_row
      on value_row.projection_id = process_row.projection_id
     and value_row.process_index = process_row.process_index
    join private.portal_lcia_projection_impact_axis as impact_row
      on impact_row.projection_id = value_row.projection_id
     and impact_row.impact_index = value_row.impact_index
    where v_mode = 'process_all_impacts'
       or impact_row.impact_id = v_impact_ref
  ), after_cursor as materialized (
    select eligible.*
    from eligible
    where v_cursor is null
       or (
         v_mode = 'process_all_impacts'
         and eligible.ordinal > v_cursor_ordinal
       )
       or (
         v_mode = 'processes_one_impact'
         and (eligible.request_order, eligible.ordinal)
               > (v_cursor_request_order, v_cursor_ordinal)
       )
       or (
         v_mode = 'ranked_processes_one_impact'
         and (
           eligible.value_numeric < v_cursor_sort_numeric
           or (
             eligible.value_numeric = v_cursor_sort_numeric
             and eligible.ordinal > v_cursor_ordinal
           )
         )
       )
  ), ordered as materialized (
    select after_cursor.*,
      row_number() over (
        order by
          case when v_mode = 'ranked_processes_one_impact'
            then after_cursor.value_numeric end desc nulls last,
          case when v_mode = 'processes_one_impact'
            then after_cursor.request_order end asc nulls last,
          after_cursor.ordinal asc
      ) as page_rank
    from after_cursor
    order by
      case when v_mode = 'ranked_processes_one_impact'
        then after_cursor.value_numeric end desc nulls last,
      case when v_mode = 'processes_one_impact'
        then after_cursor.request_order end asc nulls last,
      after_cursor.ordinal asc
    limit v_limit + 1
  )
  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'process', jsonb_build_object(
            'id', ordered.process_id::text,
            'version', ordered.process_version
          ),
          'functionalUnit', jsonb_build_object(
            'amount', ordered.functional_unit_amount,
            'unit', ordered.functional_unit_unit,
            'description', ordered.functional_unit_description
          ),
          'geography', jsonb_build_object(
            'code', ordered.geography_code,
            'precision', ordered.geography_precision
          ),
          'referenceYear', ordered.reference_year,
          'method', jsonb_build_object(
            'id', ordered.method_id::text,
            'version', ordered.method_version
          ),
          'impact', jsonb_build_object(
            'id', ordered.impact_id,
            'name', ordered.impact_name
          ),
          'value', ordered.value_text,
          'unit', ordered.unit,
          'evidenceStatus', 'verified'
        )
        order by ordered.page_rank
      ) filter (where ordered.page_rank <= v_limit),
      '[]'::jsonb
    ),
    case
      when max(ordered.page_rank) > v_limit then
        private.portal_cursor_encode_v1(
          (
            jsonb_agg(
              jsonb_build_object(
                'v', 1,
                'publicationId', v_binding.lcia_result_publication_id::text,
                'contentHash', v_binding.projection_content_hash,
                'mode', v_mode,
                'queryHash', v_query_hash,
                'requestOrder', ordered.request_order::text,
                'ordinal', ordered.ordinal::text,
                'sortValue', case
                  when v_mode = 'ranked_processes_one_impact'
                    then ordered.value_text
                  else ''
                end
              ) order by ordered.page_rank
            ) filter (where ordered.page_rank = v_limit)
          ) -> 0
        )
      else null
    end
  into v_rows, v_next_cursor
  from ordered;

  return jsonb_build_object(
    'schemaVersion', 'portal.published-lcia-page.v1',
    'mode', v_mode,
    'publication', jsonb_build_object(
      'publicationId', v_binding.lcia_result_publication_id::text,
      'packageId', v_binding.package_id::text,
      'packageVersion', v_binding.package_version,
      'publishedAt', private.portal_timestamp_v1(v_binding.source_published_at),
      'evidenceHash', v_binding.evidence_hash
    ),
    'rows', v_rows,
    'nextCursor', v_next_cursor
  );
exception
  when sqlstate '22023' then
    raise exception using errcode = '22023', message = 'invalid portal request';
  when query_canceled then
    raise exception using errcode = 'P0001', message = 'portal lcia unavailable';
  when others then
    raise exception using errcode = 'P0001', message = 'portal lcia unavailable';
end
$function$;

comment on function api.portal_get_published_lcia_values_v1(
  text, jsonb, text, text, integer
) is
  'Bounded, query-bound-keyset, locator-free public LCIA rows from only the exact current finalized V3 projection. Missing publication or unavailable rows never synthesize zero.';

revoke all on function private.portal_lcia_projection_frame_v1(text[])
  from public, anon, authenticated, service_role;
revoke all on function private.portal_lcia_projection_sha256_fields_v1(text[])
  from public, anon, authenticated, service_role;
revoke all on function private.portal_lcia_safe_audit_v1(jsonb)
  from public, anon, authenticated, service_role;
revoke all on function private.portal_lcia_json_object_has_keys_v1(jsonb, text[])
  from public, anon, authenticated, service_role;
revoke all on function private.portal_lcia_public_text_valid_v1(text, integer)
  from public, anon, authenticated, service_role;
revoke all on function private.portal_lcia_localized_text_valid_v1(jsonb)
  from public, anon, authenticated, service_role;
revoke all on function private.portal_lcia_localized_text_frame_hex_v1(jsonb)
  from public, anon, authenticated, service_role;
revoke all on function private.portal_lcia_projection_header_guard_v1()
  from public, anon, authenticated, service_role;
revoke all on function private.portal_lcia_projection_row_guard_v1()
  from public, anon, authenticated, service_role;
revoke all on function private.portal_lcia_projection_publication_guard_v1()
  from public, anon, authenticated, service_role;
revoke all on function private.portal_lcia_projection_v3_job_binding_valid_v1(
  uuid, uuid
) from public, anon, authenticated, service_role;

grant execute on function private.portal_lcia_projection_frame_v1(text[])
  to portal_public_executor;
grant execute on function private.portal_lcia_projection_sha256_fields_v1(text[])
  to portal_public_executor;
grant execute on function private.portal_lcia_json_object_has_keys_v1(jsonb, text[])
  to portal_public_executor;

revoke all on function private.svc_portal_lcia_projection_stage_begin_v1(
  uuid, uuid, integer, integer, jsonb
) from public, anon, authenticated, service_role;
revoke all on function private.svc_portal_lcia_projection_stage_register_batch_v1(
  uuid, uuid, jsonb
) from public, anon, authenticated, service_role;
revoke all on function private.svc_portal_lcia_projection_stage_status_v1(
  uuid, uuid
) from public, anon, authenticated, service_role;
revoke all on function private.svc_portal_lcia_projection_stage_seal_v1(
  uuid, uuid
) from public, anon, authenticated, service_role;
revoke all on function private.svc_portal_lcia_projection_stage_fail_v1(
  uuid, uuid, text, text, jsonb
) from public, anon, authenticated, service_role;
revoke all on function private.svc_portal_lcia_projection_worker_input_v1(
  uuid, uuid
) from public, anon, authenticated, service_role;
revoke all on function private.svc_portal_lcia_projection_package_mark_ready_v1(
  uuid, uuid, uuid, text, uuid, uuid, uuid, jsonb, jsonb, jsonb, jsonb,
  text, text, jsonb
) from public, anon, authenticated, service_role;

grant execute on function private.svc_portal_lcia_projection_stage_begin_v1(
  uuid, uuid, integer, integer, jsonb
) to service_role;
grant execute on function private.svc_portal_lcia_projection_stage_register_batch_v1(
  uuid, uuid, jsonb
) to service_role;
grant execute on function private.svc_portal_lcia_projection_stage_status_v1(
  uuid, uuid
) to service_role;
grant execute on function private.svc_portal_lcia_projection_stage_seal_v1(
  uuid, uuid
) to service_role;
grant execute on function private.svc_portal_lcia_projection_stage_fail_v1(
  uuid, uuid, text, text, jsonb
) to service_role;
grant execute on function private.svc_portal_lcia_projection_worker_input_v1(
  uuid, uuid
) to service_role;
grant execute on function private.svc_portal_lcia_projection_package_mark_ready_v1(
  uuid, uuid, uuid, text, uuid, uuid, uuid, jsonb, jsonb, jsonb, jsonb,
  text, text, jsonb
) to service_role;

revoke all on function api.cmd_lcia_result_build_request_v3(
  text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb
) from public, anon, authenticated, service_role;
revoke all on function api.qry_portal_lcia_projection_prepare_v1(uuid, uuid)
  from public, anon, authenticated, service_role;
revoke all on function api.cmd_portal_lcia_projection_finalize_publication_v1(
  uuid, uuid, text, text, text, text, jsonb
) from public, anon, authenticated, service_role;
revoke all on function api.qry_portal_lcia_projection_publication_readback_v1(
  uuid, text
) from public, anon, authenticated, service_role;
revoke all on function api.cmd_portal_lcia_projection_revoke_publication_v1(
  uuid, text, text, jsonb
) from public, anon, authenticated, service_role;
revoke all on function api.portal_get_published_lcia_values_v1(
  text, jsonb, text, text, integer
) from public, anon, authenticated, service_role;

grant execute on function api.cmd_lcia_result_build_request_v3(
  text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb
) to authenticated;
grant execute on function api.qry_portal_lcia_projection_prepare_v1(uuid, uuid)
  to authenticated;
grant execute on function api.cmd_portal_lcia_projection_finalize_publication_v1(
  uuid, uuid, text, text, text, text, jsonb
) to authenticated;
grant execute on function api.qry_portal_lcia_projection_publication_readback_v1(
  uuid, text
) to authenticated;
grant execute on function api.cmd_portal_lcia_projection_revoke_publication_v1(
  uuid, text, text, jsonb
) to authenticated;
grant execute on function api.portal_get_published_lcia_values_v1(
  text, jsonb, text, text, integer
) to anon, authenticated;

-- The catalog migration deliberately leaves postgres without SET OPTION on
-- the constrained executor.  Re-enable it only for this transactional owner
-- handoff after all owner-only COMMENT/ACL work, then restore the reviewed
-- membership and schema privilege state before commit.
grant portal_public_executor to postgres;
grant create on schema api to portal_public_executor;
alter function api.portal_get_published_lcia_values_v1(
  text, jsonb, text, text, integer
) owner to portal_public_executor;
revoke create on schema api from portal_public_executor;
revoke portal_public_executor from postgres;

insert into private.api_capability_grants (
  routine_identity,
  capability_id,
  allow_anon,
  allow_authenticated,
  allow_service_role
)
values
  (
    'api.cmd_lcia_result_build_request_v3(text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb)',
    'EDGE-ACTOR-01', false, true, false
  ),
  (
    'api.qry_portal_lcia_projection_prepare_v1(uuid, uuid)',
    'PORTAL-LCIA-ADMIN-01', false, true, false
  ),
  (
    'api.cmd_portal_lcia_projection_finalize_publication_v1(uuid, uuid, text, text, text, text, jsonb)',
    'PORTAL-LCIA-ADMIN-01', false, true, false
  ),
  (
    'api.qry_portal_lcia_projection_publication_readback_v1(uuid, text)',
    'PORTAL-LCIA-ADMIN-01', false, true, false
  ),
  (
    'api.cmd_portal_lcia_projection_revoke_publication_v1(uuid, text, text, jsonb)',
    'PORTAL-LCIA-ADMIN-01', false, true, false
  ),
  (
    'api.portal_get_published_lcia_values_v1(text, jsonb, text, text, integer)',
    'PORTAL-LCIA-01', true, true, false
  )
on conflict (routine_identity) do update
set capability_id = excluded.capability_id,
    allow_anon = excluded.allow_anon,
    allow_authenticated = excluded.allow_authenticated,
    allow_service_role = excluded.allow_service_role;

comment on function private.svc_portal_lcia_projection_stage_begin_v1(
  uuid, uuid, integer, integer, jsonb
) is
  'Begins one V3 Worker lease-fenced, locator-free Portal LCIA projection attempt.';
comment on function private.svc_portal_lcia_projection_stage_register_batch_v1(
  uuid, uuid, jsonb
) is
  'Registers at most 500 typed records and 1 MiB per exact-replay-safe batch.';
comment on function private.svc_portal_lcia_projection_stage_seal_v1(
  uuid, uuid
) is
  'Rejects holes and recomputes every int32be-framed record, relation, and content hash before preparing a projection.';
comment on function api.cmd_lcia_result_build_request_v3(
  text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb
) is
  'Additive V3 producer path: reuses unchanged V2 admission and converts only the reserved newly admitted job to the Portal projection contract.';

do $verify_portal_lcia_acl_and_manifest$
declare
  v_identity text;
  v_routine regprocedure;
begin
  foreach v_identity in array array[
    'api.cmd_lcia_result_build_request_v3(text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb)',
    'api.qry_portal_lcia_projection_prepare_v1(uuid, uuid)',
    'api.cmd_portal_lcia_projection_finalize_publication_v1(uuid, uuid, text, text, text, text, jsonb)',
    'api.qry_portal_lcia_projection_publication_readback_v1(uuid, text)',
    'api.cmd_portal_lcia_projection_revoke_publication_v1(uuid, text, text, jsonb)'
  ] loop
    v_routine := pg_catalog.to_regprocedure(v_identity);
    if v_routine is null
       or pg_catalog.has_function_privilege('anon', v_routine, 'EXECUTE')
       or not pg_catalog.has_function_privilege(
         'authenticated', v_routine, 'EXECUTE'
       )
       or pg_catalog.has_function_privilege(
         'service_role', v_routine, 'EXECUTE'
       ) then
      raise exception 'Portal LCIA actor ACL mismatch: %', v_identity;
    end if;
  end loop;

  foreach v_identity in array array[
    'private.svc_portal_lcia_projection_stage_begin_v1(uuid, uuid, integer, integer, jsonb)',
    'private.svc_portal_lcia_projection_stage_register_batch_v1(uuid, uuid, jsonb)',
    'private.svc_portal_lcia_projection_stage_status_v1(uuid, uuid)',
    'private.svc_portal_lcia_projection_stage_seal_v1(uuid, uuid)',
    'private.svc_portal_lcia_projection_stage_fail_v1(uuid, uuid, text, text, jsonb)',
    'private.svc_portal_lcia_projection_worker_input_v1(uuid, uuid)',
    'private.svc_portal_lcia_projection_package_mark_ready_v1(uuid, uuid, uuid, text, uuid, uuid, uuid, jsonb, jsonb, jsonb, jsonb, text, text, jsonb)'
  ] loop
    v_routine := pg_catalog.to_regprocedure(v_identity);
    if v_routine is null
       or pg_catalog.has_function_privilege('anon', v_routine, 'EXECUTE')
       or pg_catalog.has_function_privilege(
         'authenticated', v_routine, 'EXECUTE'
       )
       or not pg_catalog.has_function_privilege(
         'service_role', v_routine, 'EXECUTE'
       ) then
      raise exception 'Portal LCIA Worker ACL mismatch: %', v_identity;
    end if;
  end loop;

  v_routine := pg_catalog.to_regprocedure(
    'api.portal_get_published_lcia_values_v1(text, jsonb, text, text, integer)'
  );
  if v_routine is null
     or (select routine.proowner from pg_catalog.pg_proc as routine
         where routine.oid = v_routine) <> 'portal_public_executor'::regrole
     or not pg_catalog.has_function_privilege('anon', v_routine, 'EXECUTE')
     or not pg_catalog.has_function_privilege(
       'authenticated', v_routine, 'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role', v_routine, 'EXECUTE'
     ) then
    raise exception 'Portal LCIA public reader ACL or owner mismatch';
  end if;

  if not exists (
    select 1 from private.api_capability_grants
    where routine_identity =
      'api.portal_get_published_lcia_values_v1(text, jsonb, text, text, integer)'
      and capability_id = 'PORTAL-LCIA-01'
      and allow_anon
      and allow_authenticated
      and not allow_service_role
  ) then
    raise exception 'PORTAL-LCIA-01 manifest mismatch';
  end if;

  if (
    select count(*)
    from private.api_capability_grants
    where capability_id = 'PORTAL-LCIA-ADMIN-01'
      and not allow_anon
      and allow_authenticated
      and not allow_service_role
  ) <> 4
  or not exists (
    select 1
    from private.api_capability_grants
    where routine_identity =
      'api.cmd_lcia_result_build_request_v3(text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb)'
      and capability_id = 'EDGE-ACTOR-01'
      and not allow_anon
      and allow_authenticated
      and not allow_service_role
  ) then
    raise exception 'Portal LCIA actor capability manifest mismatch';
  end if;

  if exists (
    select 1
    from pg_catalog.pg_class as relation
    join pg_catalog.pg_namespace as namespace
      on namespace.oid = relation.relnamespace
    where namespace.nspname = 'private'
      and relation.relname like 'portal_lcia_projection_%'
      and relation.relkind = 'r'
      and not relation.relrowsecurity
  ) then
    raise exception 'Portal LCIA projection table without RLS';
  end if;

  if exists (
    select 1
    from information_schema.columns as column_info
    where column_info.table_schema = 'private'
      and column_info.table_name like 'portal_lcia_projection_%'
      and lower(column_info.column_name) ~
        '(url|uri|bucket|path|locator|credential|secret|token)'
      and column_info.column_name <> 'stage_lease_token'
  ) then
    raise exception 'Portal LCIA projection table contains locator-like column';
  end if;
end
$verify_portal_lcia_acl_and_manifest$;

do $verify_portal_lcia_legacy_routines_unchanged$
begin
  if exists (
    with after_state as (
      select
        before_state.routine_identity,
        pg_catalog.pg_get_functiondef(routine.oid) as definition,
        owner_role.rolname as owner_name,
        routine.prosecdef as security_definer,
        coalesce(routine.proconfig, '{}'::text[]) as proconfig,
        coalesce(routine.proacl::text, '') as acl_text
      from portal_lcia_legacy_routines_before as before_state
      join pg_catalog.pg_proc as routine
        on routine.oid = pg_catalog.to_regprocedure(
          before_state.routine_identity
        )
      join pg_catalog.pg_roles as owner_role
        on owner_role.oid = routine.proowner
    )
    (select * from portal_lcia_legacy_routines_before
     except select * from after_state)
    union all
    (select * from after_state
     except select * from portal_lcia_legacy_routines_before)
  ) then
    raise exception 'Portal LCIA migration changed a legacy Data Product/package/publication routine';
  end if;
end
$verify_portal_lcia_legacy_routines_unchanged$;

notify pgrst, 'reload schema';

commit;
