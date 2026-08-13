begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;
select no_plan();

select has_table(
  'private',
  'lcia_scope_closure_artifact_write_set_batches',
  'v2 bounded descriptor batch receipts exist'
);
select has_function(
  'private',
  'svc_lcia_scope_closure_artifact_write_set_create_v2',
  array[
    'uuid', 'uuid', 'uuid', 'uuid', 'text',
    'integer', 'text', 'jsonb', 'integer', 'uuid'
  ],
  'versioned v2 header create exists'
);
select has_function(
  'private',
  'svc_lcia_scope_closure_artifact_write_set_register_batch_v2',
  array['uuid', 'uuid', 'uuid', 'uuid', 'uuid', 'jsonb'],
  'bounded v2 batch registration exists'
);
select has_function(
  'private',
  'svc_lcia_scope_closure_artifact_write_set_status_v2',
  array['uuid', 'uuid', 'uuid', 'uuid'],
  'locator-free v2 ambiguous-response status exists'
);
select has_function(
  'private',
  'svc_lcia_scope_closure_artifact_write_set_seal_v2',
  array['uuid', 'uuid', 'uuid', 'uuid'],
  'atomic v2 seal exists'
);
select has_function(
  'private',
  'svc_lcia_scope_closure_artifact_write_set_finalize_v2',
  array['uuid', 'uuid', 'uuid', 'uuid'],
  'lease-fenced v2 finalize exists'
);
select has_function(
  'private',
  'svc_lcia_scope_closure_artifact_write_set_fail_v2',
  array['uuid', 'uuid', 'uuid', 'uuid', 'text'],
  'lease-fenced v2 failure transition exists'
);

select ok(
  has_function_privilege(
    'service_role',
    'private.svc_lcia_scope_closure_artifact_write_set_create_v2(uuid,uuid,uuid,uuid,text,integer,text,jsonb,integer,uuid)',
    'execute'
  )
  and not has_function_privilege(
    'authenticated',
    'private.svc_lcia_scope_closure_artifact_write_set_create_v2(uuid,uuid,uuid,uuid,text,integer,text,jsonb,integer,uuid)',
    'execute'
  )
  and not has_function_privilege(
    'anon',
    'private.svc_lcia_scope_closure_artifact_write_set_create_v2(uuid,uuid,uuid,uuid,text,integer,text,jsonb,integer,uuid)',
    'execute'
  ),
  'v2 staged publication is service-role only'
);
select ok(
  not has_table_privilege(
    'service_role',
    'private.lcia_scope_closure_artifact_write_set_batches',
    'select'
  )
  and not has_table_privilege(
    'service_role',
    'private.lcia_scope_closure_artifact_write_set_items',
    'insert'
  ),
  'service callers cannot bypass RPC fences through registry tables'
);

insert into auth.users (
  instance_id,
  id,
  aud,
  role,
  email,
  encrypted_password,
  email_confirmed_at,
  raw_app_meta_data,
  raw_user_meta_data,
  created_at,
  updated_at,
  is_sso_user,
  is_anonymous
) values (
  '00000000-0000-0000-0000-000000000000',
  '31600000-0000-4000-8000-000000000001',
  'authenticated',
  'authenticated',
  'issue-316-owner@example.com',
  'x',
  now(),
  '{}',
  '{}',
  now(),
  now(),
  false,
  false
);
insert into private.users (id, raw_user_meta_data, contact)
values ('31600000-0000-4000-8000-000000000001', '{}', null);

select set_config('request.jwt.claim.role', 'service_role', true);

create temporary table issue_316_context (
  label text primary key,
  job_id uuid not null,
  lease_token uuid not null,
  check_id uuid not null,
  request_id uuid not null,
  write_set_id uuid,
  write_token uuid,
  descriptor_count integer not null,
  descriptors jsonb not null,
  descriptor_set_sha256 text not null,
  create_result jsonb not null
);

create temporary table issue_316_scale_metrics (
  label text primary key,
  descriptor_count integer not null,
  descriptor_bytes bigint not null,
  batch_size integer not null,
  request_count integer not null,
  registered_row_count integer not null,
  registration_ms numeric not null,
  maximum_statement_ms numeric not null,
  seal_ms numeric not null,
  wall_ms numeric not null
);

create or replace function pg_temp.issue_316_required_roles(
  p_mode text
) returns jsonb
language sql
immutable
as $$
  select case p_mode
    when 'fresh' then jsonb_build_array(
      jsonb_build_object(
        'artifactRole', 'closure_report',
        'artifactType', 'closure_report_xlsx',
        'mediaType',
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'exactCount', 1
      ),
      jsonb_build_object(
        'artifactRole', 'complete_machine_result',
        'artifactType', 'closure_complete_machine_result',
        'mediaType',
          'application/vnd.tiangong.scope-closure-manifest+json',
        'exactCount', 1
      ),
      jsonb_build_object(
        'artifactRole', 'closure_bundle',
        'artifactType', 'closure_bundle',
        'mediaType', 'application/json',
        'exactCount', 1
      )
    )
    when 'reused' then jsonb_build_array(
      jsonb_build_object(
        'artifactRole', 'closure_report',
        'artifactType', 'closure_report_xlsx',
        'mediaType',
          'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'exactCount', 1
      )
    )
  end
$$;

create or replace function pg_temp.issue_316_descriptors(
  p_check_id uuid,
  p_count integer,
  p_mode text default 'fresh'
) returns jsonb
language sql
stable
as $$
  with descriptor_names as (
    select
      ordinal,
      case
        when p_mode = 'reused' then 'report.xlsx'
        when ordinal = 1 then 'bundle.json'
        when ordinal = 2 then 'report.xlsx'
        when ordinal = p_count then 'manifest.json'
        else 'issues/part-'
          || lpad((ordinal - 3)::text, 6, '0')
          || '.ndjson.zst'
      end as client_key
    from generate_series(1, p_count) ordinal
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'ordinal', ordinal,
    'clientKey', client_key,
    'artifactType', case
      when p_mode = 'reused' or ordinal = 2 then 'closure_report_xlsx'
      when ordinal = 1 then 'closure_bundle'
      else 'closure_complete_machine_result'
    end,
    'artifactRole', case
      when p_mode = 'reused' or ordinal = 2 then 'closure_report'
      when ordinal = 1 then 'closure_bundle'
      else 'complete_machine_result'
    end,
    'bucket', 'scope-closure-artifacts',
    'objectPath',
      'scope-closure/' || p_check_id::text
        || '/31600000-0000-4000-8000-000000009999/' || client_key,
    'mediaType', case
      when p_mode = 'reused' or ordinal = 2 then
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
      when ordinal = 1 then 'application/json'
      when ordinal = p_count then
        'application/vnd.tiangong.scope-closure-manifest+json'
      else 'application/x-ndjson+zstd'
    end,
    'size', ordinal * 101,
    'checksumSha256', encode(
      extensions.digest(convert_to(
        p_check_id::text || ':' || ordinal::text,
        'UTF8'
      ), 'sha256'),
      'hex'
    ),
    'metadata', jsonb_build_object(
      'schemaVersion', 'lcia.scope-closure-artifact.v2',
      'closureCheckId', p_check_id,
      'fileName', client_key,
      'artifactRole', case
        when p_mode = 'reused' or ordinal = 2 then 'closure_report'
        when ordinal = 1 then 'closure_bundle'
        else 'complete_machine_result'
      end,
      'retentionSeconds', 604800,
      'contentArtifactManifestHash', repeat('a', 64)
    ) || case
      when p_mode = 'fresh' and ordinal = 1 then jsonb_build_object(
        'completeMachineResultClientKey', 'manifest.json'
      )
      else '{}'::jsonb
    end
  ) order by ordinal), '[]'::jsonb)
  from descriptor_names
$$;

create or replace function pg_temp.issue_316_descriptor_digest(
  p_descriptors jsonb
) returns text
language sql
stable
as $$
  select private.lcia_scope_closure_worker_canonical_sha256(
    jsonb_build_object(
      'contractVersion',
        'lcia.scope-closure-artifact-write-set.v2',
      'descriptors', p_descriptors
    )
  )
$$;

create or replace function pg_temp.issue_316_create_context(
  p_label text,
  p_count integer,
  p_mode text default 'fresh',
  p_reused_from_check_id uuid default null,
  p_descriptors jsonb default null,
  p_digest_override text default null
) returns jsonb
language plpgsql
as $$
declare
  v_job_id uuid := gen_random_uuid();
  v_lease_token uuid := gen_random_uuid();
  v_check_id uuid := gen_random_uuid();
  v_request_id uuid := gen_random_uuid();
  v_descriptors jsonb;
  v_digest text;
  v_result jsonb;
begin
  insert into private.worker_jobs (
    id,
    job_kind,
    worker_runtime,
    worker_queue,
    requester_type,
    requested_by,
    visibility,
    payload_schema_version,
    payload_json,
    status,
    leased_by,
    lease_token,
    lease_expires_at,
    heartbeat_at
  ) values (
    v_job_id,
    'lcia.scope_closure_check',
    'calculator',
    'solver',
    'operator',
    '31600000-0000-4000-8000-000000000001',
    'operator',
    'lcia.scope_closure_check.request.v1',
    '{}',
    'running',
    'issue-316-pgtap',
    v_lease_token,
    now() + interval '2 hours',
    now()
  );

  insert into private.lcia_scope_closure_checks (
    id,
    worker_job_id,
    requested_by,
    request_idempotency_token,
    request_key,
    request_fingerprint,
    requested_scope_hash,
    policy_fingerprint,
    data_snapshot_token,
    expected_validator_scanner_fingerprint,
    status,
    certificate_status
  ) values (
    v_check_id,
    v_job_id,
    '31600000-0000-4000-8000-000000000001',
    p_label || '-token',
    p_label || '-key',
    repeat('1', 64),
    repeat('2', 64),
    repeat('3', 64),
    'issue-316-snapshot',
    'scope-closure-validator-scanner.v1',
    'running',
    'pending'
  );

  v_descriptors := coalesce(
    p_descriptors,
    pg_temp.issue_316_descriptors(v_check_id, greatest(p_count, 0), p_mode)
  );
  -- Custom descriptor templates use a sentinel closure UUID so tests can
  -- vary role/locator shape while retaining exact target binding.
  v_descriptors := replace(
    v_descriptors::text,
    '00000000-0000-4000-8000-000000003160',
    v_check_id::text
  )::jsonb;
  v_digest := coalesce(
    p_digest_override,
    pg_temp.issue_316_descriptor_digest(v_descriptors)
  );

  v_result :=
    private.svc_lcia_scope_closure_artifact_write_set_create_v2(
      v_check_id,
      v_job_id,
      v_lease_token,
      v_request_id,
      'lcia.scope-closure-artifact-write-set.v2',
      p_count,
      v_digest,
      pg_temp.issue_316_required_roles(p_mode),
      3600,
      p_reused_from_check_id
    );

  insert into issue_316_context (
    label,
    job_id,
    lease_token,
    check_id,
    request_id,
    write_set_id,
    write_token,
    descriptor_count,
    descriptors,
    descriptor_set_sha256,
    create_result
  ) values (
    p_label,
    v_job_id,
    v_lease_token,
    v_check_id,
    v_request_id,
    (v_result #>> '{data,writeSetId}')::uuid,
    (v_result #>> '{data,writeToken}')::uuid,
    p_count,
    v_descriptors,
    v_digest,
    v_result
  );
  return v_result;
end;
$$;

create or replace function pg_temp.issue_316_batch(
  p_label text,
  p_batch_id uuid,
  p_first_ordinal integer,
  p_last_ordinal integer,
  p_override jsonb default null
) returns jsonb
language plpgsql
as $$
declare
  v_context issue_316_context%rowtype;
  v_items jsonb;
begin
  select * into v_context
  from issue_316_context
  where label = p_label;
  v_items := coalesce(p_override, (
    select jsonb_agg(item.value order by item.ordinality)
    from jsonb_array_elements(v_context.descriptors)
      with ordinality item(value, ordinality)
    where (item.value->>'ordinal')::integer
      between p_first_ordinal and p_last_ordinal
  ));
  return private.svc_lcia_scope_closure_artifact_write_set_register_batch_v2(
    v_context.write_set_id,
    v_context.write_token,
    v_context.job_id,
    v_context.lease_token,
    p_batch_id,
    v_items
  );
end;
$$;

create or replace function pg_temp.issue_316_seal(
  p_label text
) returns jsonb
language sql
as $$
  select private.svc_lcia_scope_closure_artifact_write_set_seal_v2(
    context.write_set_id,
    context.write_token,
    context.job_id,
    context.lease_token
  )
  from issue_316_context context
  where context.label = p_label
$$;

create or replace function pg_temp.issue_316_run_scale(
  p_label text,
  p_count integer,
  p_batch_size integer
) returns jsonb
language plpgsql
as $$
declare
  v_create jsonb;
  v_register jsonb;
  v_seal jsonb;
  v_context issue_316_context%rowtype;
  v_batch_start integer := 1;
  v_batch_end integer;
  v_requests integer := 0;
  v_statement_start timestamptz;
  v_registration_start timestamptz;
  v_seal_start timestamptz;
  v_wall_start timestamptz := clock_timestamp();
  v_statement_ms numeric;
  v_maximum_statement_ms numeric := 0;
  v_registration_ms numeric;
  v_seal_ms numeric;
  v_registered_rows integer;
begin
  v_create := pg_temp.issue_316_create_context(p_label, p_count);
  if not coalesce((v_create->>'ok')::boolean, false) then
    raise exception 'scale header failed: %', v_create->>'code';
  end if;
  select * into v_context
  from issue_316_context
  where label = p_label;

  v_registration_start := clock_timestamp();
  while v_batch_start <= p_count loop
    v_batch_end := least(v_batch_start + p_batch_size - 1, p_count);
    v_statement_start := clock_timestamp();
    v_register := pg_temp.issue_316_batch(
      p_label,
      gen_random_uuid(),
      v_batch_start,
      v_batch_end
    );
    v_statement_ms :=
      extract(epoch from clock_timestamp() - v_statement_start) * 1000;
    v_maximum_statement_ms := greatest(
      v_maximum_statement_ms,
      v_statement_ms
    );
    if not coalesce((v_register->>'ok')::boolean, false) then
      raise exception 'scale registration failed: %', v_register->>'code';
    end if;
    v_requests := v_requests + 1;
    v_batch_start := v_batch_end + 1;
  end loop;
  v_registration_ms :=
    extract(epoch from clock_timestamp() - v_registration_start) * 1000;

  select count(*) into v_registered_rows
  from private.lcia_scope_closure_artifact_write_set_items
  where write_set_id = v_context.write_set_id;

  v_seal_start := clock_timestamp();
  v_seal := pg_temp.issue_316_seal(p_label);
  v_seal_ms :=
    extract(epoch from clock_timestamp() - v_seal_start) * 1000;

  insert into issue_316_scale_metrics (
    label,
    descriptor_count,
    descriptor_bytes,
    batch_size,
    request_count,
    registered_row_count,
    registration_ms,
    maximum_statement_ms,
    seal_ms,
    wall_ms
  ) values (
    p_label,
    p_count,
    octet_length(convert_to(v_context.descriptors::text, 'UTF8')),
    p_batch_size,
    v_requests,
    v_registered_rows,
    v_registration_ms,
    v_maximum_statement_ms,
    v_seal_ms,
    extract(epoch from clock_timestamp() - v_wall_start) * 1000
  );
  return v_seal;
end;
$$;

select is(
  pg_temp.issue_316_create_context('count-0', 0) #>> '{code}',
  'artifact_write_set_v2_invalid',
  'zero descriptors are rejected before a header becomes active'
);
select is(
  pg_temp.issue_316_create_context('fresh-count-1', 1) #>> '{code}',
  'artifact_write_set_v2_invalid',
  'one descriptor cannot weaken the fresh primary-role contract'
);

create temporary table issue_316_scale_results (
  label text primary key,
  result jsonb not null
);
insert into issue_316_scale_results values
  ('scale-500', pg_temp.issue_316_run_scale('scale-500', 500, 500)),
  ('scale-501', pg_temp.issue_316_run_scale('scale-501', 501, 251)),
  ('scale-596', pg_temp.issue_316_run_scale('scale-596', 596, 500)),
  ('scale-1501', pg_temp.issue_316_run_scale('scale-1501', 1501, 400));

select is(
  (select result #>> '{data,status}' from issue_316_scale_results
   where label = 'scale-500'),
  'staging',
  '500 descriptors seal after one bounded request'
);
select is(
  (select request_count from issue_316_scale_metrics
   where label = 'scale-500'),
  1,
  '500-descriptor case uses exactly one 500-item request'
);
select is(
  (select request_count from issue_316_scale_metrics
   where label = 'scale-501'),
  2,
  '501 descriptors cross the one-shot boundary in two requests'
);
select is(
  (select request_count from issue_316_scale_metrics
   where label = 'scale-596'),
  2,
  '596 production-shaped descriptors use 500 + 96 requests'
);
select is(
  (select request_count from issue_316_scale_metrics
   where label = 'scale-1501'),
  4,
  '1501 descriptors use four bounded 400-item requests'
);
select ok(
  (
    select bool_and(
      registered_row_count = descriptor_count
      and request_count > 1
      and batch_size <= 500
      and registration_ms >= 0
      and maximum_statement_ms >= 0
      and seal_ms >= 0
      and wall_ms >= seal_ms
    )
    from issue_316_scale_metrics
    where descriptor_count in (501, 596, 1501)
  ),
  'scale evidence records exact rows, bounded requests, wall/statement/seal time'
);

select is(
  pg_temp.issue_316_create_context('replay-status', 3)
    #>> '{data,status}',
  'registration_open',
  'v2 header is not upload-eligible before seal'
);
select is(
  (
    select private.svc_lcia_scope_closure_artifact_write_set_create_v2(
      context.check_id,
      context.job_id,
      context.lease_token,
      context.request_id,
      'lcia.scope-closure-artifact-write-set.v2',
      context.descriptor_count,
      context.descriptor_set_sha256,
      pg_temp.issue_316_required_roles('fresh'),
      3600,
      null
    ) #>> '{reused}'
    from issue_316_context context
    where context.label = 'replay-status'
  ),
  'true',
  'byte-identical header replay recovers an ambiguous create response'
);
select is(
  (
    select private.svc_lcia_scope_closure_artifact_write_set_create_v2(
      context.check_id,
      context.job_id,
      context.lease_token,
      context.request_id,
      'lcia.scope-closure-artifact-write-set.v2',
      context.descriptor_count,
      repeat('f', 64),
      pg_temp.issue_316_required_roles('fresh'),
      3600,
      null
    ) #>> '{code}'
    from issue_316_context context
    where context.label = 'replay-status'
  ),
  'artifact_write_set_v2_request_conflict',
  'same request UUID with a different header fails closed'
);
create temporary table issue_316_replay (
  batch_id uuid not null,
  first_result jsonb,
  replay_result jsonb
);
insert into issue_316_replay (batch_id)
values (gen_random_uuid());
update issue_316_replay
set first_result = pg_temp.issue_316_batch(
  'replay-status',
  batch_id,
  1,
  3
);
update issue_316_replay
set replay_result = pg_temp.issue_316_batch(
  'replay-status',
  batch_id,
  1,
  3
);
select ok(
  (select first_result #>> '{ok}' = 'true'
      and replay_result #>> '{ok}' = 'true'
      and replay_result #>> '{reused}' = 'true'
   from issue_316_replay),
  'byte-identical batch replay is idempotent'
);
select is(
  (
    select pg_temp.issue_316_batch(
      'replay-status',
      replay.batch_id,
      1,
      3,
      jsonb_set(
        context.descriptors,
        '{0,checksumSha256}',
        to_jsonb(repeat('e', 64))
      )
    ) #>> '{code}'
    from issue_316_replay replay
    cross join issue_316_context context
    where context.label = 'replay-status'
  ),
  'artifact_write_set_v2_batch_conflict',
  'same batch UUID with different canonical bytes fails closed'
);
select is(
  (
    select count(*)
    from private.lcia_scope_closure_artifact_write_set_items item
    join issue_316_context context
      on context.write_set_id = item.write_set_id
    where context.label = 'replay-status'
  ),
  3::bigint,
  'exact replay inserts no duplicate descriptors'
);

create temporary table issue_316_status_before as
select private.svc_lcia_scope_closure_artifact_write_set_status_v2(
  context.check_id,
  context.job_id,
  context.lease_token,
  context.request_id
) value
from issue_316_context context
where label = 'replay-status';
select is(
  (select value #>> '{data,registeredDescriptorCount}'
   from issue_316_status_before),
  '3',
  'status/readback recovers registered descriptor count after ambiguity'
);
select is(
  (select value #>> '{data,uploadEligible}'
   from issue_316_status_before),
  'false',
  'status/readback does not authorize upload before seal'
);
select is(
  (select value #> '{data,artifactMap}'
   from issue_316_status_before),
  '{}'::jsonb,
  'pre-seal status withholds artifact IDs'
);
select ok(
  (
    select not (value->'data' ?| array[
      'requestedBy',
      'ownerId',
      'bucket',
      'objectPath',
      'storageBucket',
      'storagePath',
      'items',
      'workerLeaseToken',
      'workerLeaseTokenSha256',
      'serviceRole',
      'serviceKey',
      'reconcileToken'
    ])
    from issue_316_status_before
  ),
  'status/readback leaks no owner, locator, raw lease, or service authority'
);

select is(
  pg_temp.issue_316_seal('replay-status') #>> '{data,status}',
  'staging',
  'atomic seal is the sole transition to upload eligibility'
);
select is(
  (
    select private.svc_lcia_scope_closure_artifact_write_set_status_v2(
      context.check_id,
      context.job_id,
      context.lease_token,
      context.request_id
    ) #>> '{data,uploadEligible}'
    from issue_316_context context
    where label = 'replay-status'
  ),
  'true',
  'ambiguous seal response is recoverable through locator-free status'
);
select is(
  (
    select count(*)::integer
    from issue_316_context context
    cross join lateral jsonb_object_keys(
      private.svc_lcia_scope_closure_artifact_write_set_status_v2(
        context.check_id,
        context.job_id,
        context.lease_token,
        context.request_id
      ) #> '{data,artifactMap}'
    ) artifact_key
    where context.label = 'replay-status'
  ),
  3,
  'post-seal status exposes only the clientKey-to-artifactId map'
);
select is(
  (
    select pg_temp.issue_316_batch(
      'replay-status',
      batch_id,
      1,
      3
    ) #>> '{reused}'
    from issue_316_replay
  ),
  'true',
  'post-seal exact replay remains a no-op success'
);
select is(
  pg_temp.issue_316_batch(
    'replay-status',
    gen_random_uuid(),
    1,
    1
  ) #>> '{code}',
  'artifact_write_set_v2_registration_closed',
  'post-seal mutation fails closed'
);
select is(
  (
    select private.svc_lcia_scope_closure_artifact_write_set_inspect(
      write_set_id
    ) #>> '{code}'
    from issue_316_context
    where label = 'replay-status'
  ),
  'artifact_write_set_v2_status_required',
  'legacy locator-bearing inspect cannot read v2 rows'
);
select is(
  (
    select private.svc_lcia_scope_closure_artifact_write_set_finalize(
      write_set_id,
      write_token
    ) #>> '{code}'
    from issue_316_context
    where label = 'replay-status'
  ),
  'artifact_write_set_v2_fence_required',
  'legacy finalize cannot bypass the v2 Worker lease fence'
);
select throws_ok(
  format(
    'update private.lcia_scope_closure_artifact_write_set_items set byte_size = byte_size + 1 where write_set_id = %L::uuid',
    (select write_set_id::text from issue_316_context
     where label = 'replay-status')
  ),
  '23514',
  'artifact_write_set_v2_items_are_immutable',
  'sealed v2 descriptor rows are immutable'
);

select pg_temp.issue_316_create_context('reordered', 3);
select is(
  pg_temp.issue_316_batch(
    'reordered',
    gen_random_uuid(),
    1,
    3,
    (
      select jsonb_build_array(
        descriptors->1,
        descriptors->0,
        descriptors->2
      )
      from issue_316_context
      where label = 'reordered'
    )
  ) #>> '{code}',
  'artifact_write_set_v2_invalid',
  'reordered batch ordinals are rejected'
);
select is(
  (
    select count(*)
    from private.lcia_scope_closure_artifact_write_set_items item
    join issue_316_context context
      on context.write_set_id = item.write_set_id
    where context.label = 'reordered'
  ),
  0::bigint,
  'reordered batch rejection writes no partial rows'
);

select pg_temp.issue_316_create_context('duplicate-cross-batch', 3);
select pg_temp.issue_316_batch(
  'duplicate-cross-batch',
  gen_random_uuid(),
  1,
  1
);
select is(
  pg_temp.issue_316_batch(
    'duplicate-cross-batch',
    gen_random_uuid(),
    1,
    1
  ) #>> '{code}',
  'artifact_write_set_v2_descriptor_conflict',
  'duplicate ordinal/clientKey/locator across batch IDs fails closed'
);
select ok(
  (
    select
      (select count(*)
       from private.lcia_scope_closure_artifact_write_set_items item
       where item.write_set_id = context.write_set_id) = 1
      and
      (select count(*)
       from private.lcia_scope_closure_artifact_write_set_batches batch
       where batch.write_set_id = context.write_set_id) = 1
    from issue_316_context context
    where context.label = 'duplicate-cross-batch'
  ),
  'conflicting batch rolls back both descriptors and receipt'
);

select pg_temp.issue_316_create_context('missing-ordinal', 4);
select pg_temp.issue_316_batch(
  'missing-ordinal',
  gen_random_uuid(),
  1,
  2
);
select pg_temp.issue_316_batch(
  'missing-ordinal',
  gen_random_uuid(),
  4,
  4
);
select is(
  pg_temp.issue_316_seal('missing-ordinal') #>> '{code}',
  'artifact_write_set_v2_incomplete',
  'missing ordinal fails atomic seal'
);
select ok(
  (
    select status = 'registration_open'
      and not exists (
        select 1
        from private.worker_job_artifacts artifact
        where artifact.job_id = context.job_id
      )
    from issue_316_context context
    join private.lcia_scope_closure_artifact_write_sets write_set
      on write_set.id = context.write_set_id
    where context.label = 'missing-ordinal'
  ),
  'missing ordinal produces no staging or partial ready state'
);

select pg_temp.issue_316_create_context(
  'wrong-digest',
  3,
  'fresh',
  null,
  null,
  repeat('f', 64)
);
select pg_temp.issue_316_batch(
  'wrong-digest',
  gen_random_uuid(),
  1,
  3
);
select is(
  pg_temp.issue_316_seal('wrong-digest') #>> '{code}',
  'artifact_write_set_v2_digest_mismatch',
  'wrong descriptor-set digest fails atomic seal'
);
select is(
  (
    select status
    from private.lcia_scope_closure_artifact_write_sets write_set
    join issue_316_context context
      on context.write_set_id = write_set.id
    where context.label = 'wrong-digest'
  ),
  'registration_open',
  'digest mismatch leaves the set upload-ineligible'
);

create temporary table issue_316_role_invalid_descriptors as
select jsonb_set(
  pg_temp.issue_316_descriptors(
    '00000000-0000-4000-8000-000000003160',
    4
  ),
  '{2,mediaType}',
  to_jsonb(
    'application/vnd.tiangong.scope-closure-manifest+json'::text
  )
) value;
select pg_temp.issue_316_create_context(
  'wrong-primary-role',
  4,
  'fresh',
  null,
  (select value from issue_316_role_invalid_descriptors)
);
select pg_temp.issue_316_batch(
  'wrong-primary-role',
  gen_random_uuid(),
  1,
  4
);
select is(
  pg_temp.issue_316_seal('wrong-primary-role') #>> '{code}',
  'artifact_write_set_v2_primary_roles_invalid',
  'duplicate manifest role/media shape fails atomic seal'
);

create temporary table issue_316_locator_conflict_descriptors as
select jsonb_set(
  pg_temp.issue_316_descriptors(
    '00000000-0000-4000-8000-000000003160',
    4
  ),
  '{2,objectPath}',
  pg_temp.issue_316_descriptors(
    '00000000-0000-4000-8000-000000003160',
    4
  ) #> '{1,objectPath}'
) value;
select pg_temp.issue_316_create_context(
  'locator-conflict',
  4,
  'fresh',
  null,
  (select value from issue_316_locator_conflict_descriptors)
);
select is(
  pg_temp.issue_316_batch(
    'locator-conflict',
    gen_random_uuid(),
    1,
    4
  ) #>> '{code}',
  'artifact_write_set_v2_descriptor_conflict',
  'duplicate object locator fails the whole batch'
);
select ok(
  (
    select
      (select count(*)
       from private.lcia_scope_closure_artifact_write_set_items item
       where item.write_set_id = context.write_set_id) = 0
      and
      (select count(*)
       from private.lcia_scope_closure_artifact_write_set_batches batch
       where batch.write_set_id = context.write_set_id) = 0
    from issue_316_context context
    where context.label = 'locator-conflict'
  ),
  'locator conflict rolls back every row and receipt'
);

select pg_temp.issue_316_create_context('out-of-range', 3);
select is(
  pg_temp.issue_316_batch(
    'out-of-range',
    gen_random_uuid(),
    1,
    1,
    (
      select jsonb_build_array(jsonb_set(
        descriptors->0,
        '{ordinal}',
        '4'::jsonb
      ))
      from issue_316_context
      where label = 'out-of-range'
    )
  ) #>> '{code}',
  'artifact_write_set_v2_invalid',
  'out-of-range ordinal is rejected'
);

select pg_temp.issue_316_create_context('stale-fence', 3);
update private.worker_jobs
set lease_token = gen_random_uuid(),
    lease_expires_at = now() + interval '1 hour'
where id = (
  select job_id from issue_316_context where label = 'stale-fence'
);
select is(
  pg_temp.issue_316_batch(
    'stale-fence',
    gen_random_uuid(),
    1,
    3
  ) #>> '{code}',
  'worker_job_lease_invalid',
  'stale Worker lease generation cannot register'
);

select pg_temp.issue_316_create_context('foreign-job', 3);
insert into private.worker_jobs (
  id,
  job_kind,
  worker_runtime,
  worker_queue,
  requester_type,
  requested_by,
  visibility,
  payload_schema_version,
  payload_json,
  status,
  leased_by,
  lease_token,
  lease_expires_at
) values (
  '31600000-0000-4000-8000-000000000099',
  'lcia.scope_closure_check',
  'calculator',
  'solver',
  'operator',
  '31600000-0000-4000-8000-000000000001',
  'operator',
  'lcia.scope_closure_check.request.v1',
  '{}',
  'running',
  'foreign-worker',
  '31600000-0000-4000-8000-000000000098',
  now() + interval '1 hour'
);
select is(
  (
    select private.svc_lcia_scope_closure_artifact_write_set_register_batch_v2(
      context.write_set_id,
      context.write_token,
      '31600000-0000-4000-8000-000000000099',
      '31600000-0000-4000-8000-000000000098',
      gen_random_uuid(),
      context.descriptors
    ) #>> '{code}'
    from issue_316_context context
    where label = 'foreign-job'
  ),
  'worker_job_lease_invalid',
  'foreign valid Worker job cannot cross the closure/write-set binding'
);

select is(
  (
    select private.svc_lcia_scope_closure_artifact_write_set_finalize_v2(
      context.write_set_id,
      context.write_token,
      context.job_id,
      context.lease_token
    ) #>> '{code}'
    from issue_316_context context
    where label = 'foreign-job'
  ),
  'artifact_write_set_not_finalizable',
  'finalize before seal cannot create ready rows'
);

create temporary table issue_316_v2_finalize as
select private.svc_lcia_scope_closure_artifact_write_set_finalize_v2(
  context.write_set_id,
  context.write_token,
  context.job_id,
  context.lease_token
) value
from issue_316_context context
where label = 'replay-status';
select is(
  (select value #>> '{data,status}' from issue_316_v2_finalize),
  'ready',
  'v2 finalize atomically publishes a sealed write-set'
);
select ok(
  (
    select
      (select count(*)
       from private.worker_job_artifacts artifact
       where artifact.job_id = context.job_id
         and artifact.lifecycle_state = 'ready') =
        context.descriptor_count
      and check_row.report_artifact_id is not null
      and check_row.complete_machine_result_artifact_id is not null
      and check_row.closure_bundle_artifact_id is not null
    from issue_316_context context
    join private.lcia_scope_closure_checks check_row
      on check_row.id = context.check_id
    where context.label = 'replay-status'
  ),
  'finalize exposes all rows and primary projections together'
);
select is(
  (
    select private.svc_lcia_scope_closure_artifact_write_set_finalize_v2(
      context.write_set_id,
      context.write_token,
      context.job_id,
      context.lease_token
    ) #>> '{reused}'
    from issue_316_context context
    where label = 'replay-status'
  ),
  'true',
  'exact finalize replay is idempotent'
);

update private.lcia_scope_closure_checks
set status = 'passed',
    scan_completeness = 'complete'
where id = (
  select check_id from issue_316_context where label = 'replay-status'
);
select is(
  pg_temp.issue_316_create_context(
    'reused-count-1',
    1,
    'reused',
    (
      select check_id
      from issue_316_context
      where label = 'replay-status'
    )
  ) #>> '{data,status}',
  'registration_open',
  'one-descriptor reused report header is accepted'
);
select pg_temp.issue_316_batch(
  'reused-count-1',
  gen_random_uuid(),
  1,
  1
);
select is(
  pg_temp.issue_316_seal('reused-count-1') #>> '{data,status}',
  'staging',
  'one-descriptor reused report seals under the exact reused role fixture'
);
select is(
  (
    select private.svc_lcia_scope_closure_artifact_write_set_finalize_v2(
      context.write_set_id,
      context.write_token,
      context.job_id,
      context.lease_token
    ) #>> '{data,status}'
    from issue_316_context context
    where label = 'reused-count-1'
  ),
  'ready',
  'one-descriptor reused report finalizes through v2'
);

select pg_temp.issue_316_create_context('abandoned-open', 3);
select pg_temp.issue_316_batch(
  'abandoned-open',
  gen_random_uuid(),
  1,
  1
);
update private.lcia_scope_closure_artifact_write_sets
set created_at = now() - interval '2 seconds',
    staging_expires_at = now() - interval '1 second'
where id = (
  select write_set_id
  from issue_316_context
  where label = 'abandoned-open'
);
create temporary table issue_316_reconcile as
select private.svc_lcia_scope_closure_artifact_write_set_reconcile(100, 300)
  value;
select ok(
  (
    select exists (
      select 1
      from jsonb_array_elements(value #> '{data,writeSets}') write_set
      where write_set->>'writeSetId' = context.write_set_id::text
        and write_set->>'status' = 'cleanup_pending'
    )
    from issue_316_reconcile
    cross join issue_316_context context
    where context.label = 'abandoned-open'
  ),
  'abandoned registration_open rows enter existing fenced reconciliation'
);

-- The retained one-shot adapter must still accept and finalize <=500 items.
select pg_temp.issue_316_create_context('legacy-seed-only', 3);
update private.lcia_scope_closure_artifact_write_sets
set status = 'cleaned',
    cleaned_at = now(),
    updated_at = now()
where id = (
  select write_set_id
  from issue_316_context
  where label = 'legacy-seed-only'
);
create temporary table issue_316_legacy as
select
  context.job_id,
  context.check_id,
  private.svc_lcia_scope_closure_artifact_write_set_create(
    context.check_id,
    'issue-316-legacy-one-shot',
    (
      select jsonb_agg(item.value - 'ordinal' order by item.ordinality)
      from jsonb_array_elements(context.descriptors)
        with ordinality item(value, ordinality)
    ),
    3600,
    null
  ) value
from issue_316_context context
where context.label = 'legacy-seed-only';
select is(
  (select value #>> '{data,status}' from issue_316_legacy),
  'staging',
  'legacy one-shot create remains compatible during expand/migrate'
);
select is(
  (
    select private.svc_lcia_scope_closure_artifact_write_set_finalize(
      (value #>> '{data,writeSetId}')::uuid,
      (value #>> '{data,writeToken}')::uuid
    ) #>> '{data,status}'
    from issue_316_legacy
  ),
  'ready',
  'legacy one-shot finalize remains compatible'
);

select diag(
  'issue-316 scale metrics: ' || coalesce(jsonb_agg(jsonb_build_object(
    'scenario', label,
    'descriptorCount', descriptor_count,
    'descriptorBytes', descriptor_bytes,
    'batchSize', batch_size,
    'requestCount', request_count,
    'rowCount', registered_row_count,
    'registrationMs', round(registration_ms, 3),
    'maximumStatementMs', round(maximum_statement_ms, 3),
    'sealMs', round(seal_ms, 3),
    'wallMs', round(wall_ms, 3)
  ) order by descriptor_count)::text, '[]')
)
from issue_316_scale_metrics;

select * from finish();
rollback;
