begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;
select no_plan();

create temporary table candidate_webhook_calls (
  edge_function text not null,
  body jsonb not null,
  timeout_milliseconds integer not null
) on commit drop;

create or replace function util.invoke_edge_function(
  name text,
  body jsonb,
  timeout_milliseconds integer default ((5 * 60) * 1000)
) returns void
language plpgsql
security definer
set search_path = ''
as $$
begin
  insert into pg_temp.candidate_webhook_calls(
    edge_function,
    body,
    timeout_milliseconds
  ) values (name, body, timeout_milliseconds);
end;
$$;

select is(
  private.lcia_scope_closure_worker_canonical_sha256(
    '{"z":1,"a":[3,{"b":true}]}'::jsonb
  ),
  '95f7be01f2a50d2296efb7eeba30ac587ef545c9501c9d47740490f143f6b593',
  'candidate document hashing matches the Worker canonical JSON golden vector'
);
select is(
  (
    select count(*)
    from private.lcia_scope_closure_reviewed_lcia_methods
  ),
  25::bigint,
  'candidate LCIA allowlist matches the deployed Worker reviewed method count'
);

insert into auth.users (
  instance_id, id, aud, role, email, encrypted_password, email_confirmed_at,
  raw_app_meta_data, raw_user_meta_data, created_at, updated_at,
  is_sso_user, is_anonymous
) values (
  '00000000-0000-0000-0000-000000000000',
  'c7240000-0000-4000-8000-000000000001',
  'authenticated',
  'authenticated',
  'initial-candidate-closure@example.com',
  'x',
  now(),
  '{}',
  '{}',
  now(),
  now(),
  false,
  false
);
insert into public.users(id, raw_user_meta_data, contact)
values ('c7240000-0000-4000-8000-000000000001', '{}', null);
insert into public.teams(id, json, rank, is_public)
values (
  '00000000-0000-0000-0000-000000000000',
  '{"name":"System"}',
  0,
  false
)
on conflict (id) do nothing;
insert into public.roles(user_id, team_id, role)
values (
  'c7240000-0000-4000-8000-000000000001',
  '00000000-0000-0000-0000-000000000000',
  'data_product_manager'
);

insert into public.processes(
  id, version, state_code, json, json_ordered, user_id
) values
(
  'c7240000-0000-4000-8000-000000000010',
  '01.00.000',
  100,
  '{"processDataSet":{"processInformation":{"dataSetInformation":{"common:UUID":"c7240000-0000-4000-8000-000000000010"}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000"}}}}',
  '{"processDataSet":{"processInformation":{"dataSetInformation":{"common:UUID":"c7240000-0000-4000-8000-000000000010"}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000"}}}}',
  'c7240000-0000-4000-8000-000000000001'
),
(
  'c7240000-0000-4000-8000-000000000010',
  '02.00.000',
  100,
  '{"processDataSet":{"processInformation":{"dataSetInformation":{"common:UUID":"c7240000-0000-4000-8000-000000000010"}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"02.00.000"}},"versionMarker":"latest"}}',
  '{"processDataSet":{"processInformation":{"dataSetInformation":{"common:UUID":"c7240000-0000-4000-8000-000000000010"}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"02.00.000"}},"versionMarker":"latest"}}',
  'c7240000-0000-4000-8000-000000000001'
),
(
  'c7240000-0000-4000-8000-000000000011',
  '01.00.000',
  0,
  '{"processDataSet":{"processInformation":{"dataSetInformation":{"common:UUID":"c7240000-0000-4000-8000-000000000011"}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000"}}}}',
  '{"processDataSet":{"processInformation":{"dataSetInformation":{"common:UUID":"c7240000-0000-4000-8000-000000000011"}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000"}}}}',
  'c7240000-0000-4000-8000-000000000001'
);

insert into public.lciamethods(
  id, version, state_code, json, json_ordered, user_id
) values (
  '9ec743ea-6b00-400d-a53b-61547a3fc03c',
  '01.01.000',
  0,
  '{"LCIAMethodDataSet":{"LCIAMethodInformation":{"dataSetInformation":{"common:UUID":"503699e0-eca9-4089-8bf8-e0f49c93e578"}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.01.000"}}}}',
  '{"LCIAMethodDataSet":{"LCIAMethodInformation":{"dataSetInformation":{"common:UUID":"503699e0-eca9-4089-8bf8-e0f49c93e578"}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.01.000"}}}}',
  'c7240000-0000-4000-8000-000000000001'
)
on conflict (id, version) do update
set state_code = excluded.state_code,
    json = excluded.json,
    json_ordered = excluded.json_ordered,
    user_id = excluded.user_id;

select is(
  (
    select count(*)
    from private.lcia_scope_closure_candidate_document_hashes
    where dataset_type = 'processes'
      and dataset_id = 'c7240000-0000-4000-8000-000000000010'
  ),
  2::bigint,
  'candidate hash triggers cache every eligible exact process version'
);
select is(
  (
    select count(*)
    from private.lcia_scope_closure_candidate_document_hashes
    where dataset_type = 'lciamethods'
      and dataset_id = '503699e0-eca9-4089-8bf8-e0f49c93e578'
      and source_locator_id = '9ec743ea-6b00-400d-a53b-61547a3fc03c'
  ),
  1::bigint,
  'candidate hash triggers cache reviewed state-code-zero LCIA methods under canonical identity'
);

select ok(
  not exists (
    select 1
    from public.lca_release_publications
    where is_current = true and status = 'current'
  ),
  'fixture has no current formal release'
);

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  'c7240000-0000-4000-8000-000000000001',
  true
);

create temporary table candidate_closure_responses(
  idempotency_token text primary key,
  result jsonb not null
);
insert into candidate_closure_responses values
(
  'candidate-subset-a',
  public.cmd_lcia_scope_closure_check_request_v2(
    '{
      "coverageMode":"subset",
      "processes":[{
        "id":"c7240000-0000-4000-8000-000000000010",
        "version":"01.00.000"
      }],
      "lciaMethods":[{
        "id":"503699e0-eca9-4089-8bf8-e0f49c93e578",
        "version":"01.01.000"
      }]
    }'::jsonb,
    'candidate-subset-a',
    '{}'
  )
),
(
  'candidate-subset-b',
  public.cmd_lcia_scope_closure_check_request_v2(
    '{
      "coverageMode":"subset",
      "processes":[{
        "id":"c7240000-0000-4000-8000-000000000010",
        "version":"01.00.000"
      }],
      "lciaMethods":[{
        "id":"503699e0-eca9-4089-8bf8-e0f49c93e578",
        "version":"01.01.000"
      }]
    }'::jsonb,
    'candidate-subset-b',
    '{}'
  )
);

select is(
  (
    select count(*)
    from candidate_closure_responses
    where result->>'ok' = 'true'
  ),
  2::bigint,
  'zero-release subset preflight requests are accepted'
);

reset role;

select is(
  (
    select count(distinct data_snapshot_token)
    from public.lcia_scope_closure_checks
    where request_idempotency_token in ('candidate-subset-a', 'candidate-subset-b')
  ),
  1::bigint,
  'logically identical candidate requests freeze one deterministic data snapshot'
);
select is(
  (
    select count(distinct scan_execution_id)
    from public.lcia_scope_closure_checks
    where request_idempotency_token in ('candidate-subset-a', 'candidate-subset-b')
  ),
  1::bigint,
  'logically identical candidate requests share one scan execution'
);
select is(
  (
    select requested_scope_manifest->'processes'->0->>'version'
    from public.lcia_scope_closure_checks
    where request_idempotency_token = 'candidate-subset-a'
  ),
  '01.00.000',
  'subset normalization preserves the exact requested process version'
);
select is(
  (
    select requested_scope_manifest->'lciaMethods'->0->>'id'
    from public.lcia_scope_closure_checks
    where request_idempotency_token = 'candidate-subset-a'
  ),
  '503699e0-eca9-4089-8bf8-e0f49c93e578',
  'LCIA normalization preserves canonical method identity rather than its artifact locator'
);
select is(
  (
    select snapshot.root_manifest->'candidateData'->>'sourceKind'
    from public.lcia_scope_closure_checks closure_check
    join public.lcia_scope_closure_data_snapshots snapshot
      using (data_snapshot_token)
    where closure_check.request_idempotency_token = 'candidate-subset-a'
  ),
  'candidate-public-state',
  'snapshot explicitly identifies candidate public state as its source'
);
select is(
  (
    select snapshot.root_manifest
      ->'currentPublicRelease'->>'releaseRunId'
    from public.lcia_scope_closure_checks closure_check
    join public.lcia_scope_closure_data_snapshots snapshot
      using (data_snapshot_token)
    where closure_check.request_idempotency_token = 'candidate-subset-a'
  ),
  '00000000-0000-0000-0000-000000000000',
  'Worker v2 compatibility projection cannot be mistaken for a real release identity'
);
select is(
  (
    select count(*)
    from public.lcia_scope_closure_checks closure_check
    join public.lcia_scope_closure_data_snapshots snapshot
      using (data_snapshot_token)
    cross join lateral jsonb_array_elements(
      snapshot.root_manifest->'datasets'
    ) dataset(value)
    where closure_check.request_idempotency_token = 'candidate-subset-a'
      and dataset.value->>'datasetType' = 'processes'
      and dataset.value->>'datasetId' =
        'c7240000-0000-4000-8000-000000000010'
      and dataset.value->>'datasetVersion' in ('01.00.000', '02.00.000')
      and dataset.value->>'canonicalContentHash' ~ '^[0-9a-f]{64}$'
  ),
  2::bigint,
  'candidate allowlist freezes every eligible exact process version with Worker hashes'
);
select is(
  (
    select count(*)
    from public.lcia_scope_closure_checks closure_check
    join public.lcia_scope_closure_data_snapshots snapshot
      using (data_snapshot_token)
    cross join lateral jsonb_array_elements(
      snapshot.root_manifest->'datasets'
    ) dataset(value)
    where closure_check.request_idempotency_token = 'candidate-subset-a'
      and dataset.value->>'datasetType' = 'lciamethods'
      and dataset.value->>'datasetId' =
        '503699e0-eca9-4089-8bf8-e0f49c93e578'
      and dataset.value->>'datasetVersion' = '01.01.000'
  ),
  1::bigint,
  'candidate allowlist uses the canonical LCIA identity while hashing the locator document'
);

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  'c7240000-0000-4000-8000-000000000001',
  true
);

select is(
  public.cmd_lcia_scope_closure_check_request_v2(
    '{
      "coverageMode":"global_eligible",
      "processes":[],
      "lciaMethods":[{
        "id":"503699e0-eca9-4089-8bf8-e0f49c93e578",
        "version":"01.01.000"
      }]
    }'::jsonb,
    'candidate-global',
    '{}'
  )->>'ok',
  'true',
  'zero-release global eligible preflight is accepted'
);

reset role;

select is(
  (
    select requested_scope_manifest->'processes'->0->>'version'
    from public.lcia_scope_closure_checks
    where request_idempotency_token = 'candidate-global'
  ),
  '02.00.000',
  'global candidate roots select the latest eligible version per process UUID'
);

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claim.sub',
  'c7240000-0000-4000-8000-000000000001',
  true
);

select is(
  public.cmd_lcia_scope_closure_check_request_v2(
    '{
      "coverageMode":"subset",
      "processes":[{
        "id":"c7240000-0000-4000-8000-000000000011",
        "version":"01.00.000"
      }],
      "lciaMethods":[{
        "id":"503699e0-eca9-4089-8bf8-e0f49c93e578",
        "version":"01.01.000"
      }]
    }'::jsonb,
    'candidate-ineligible',
    '{}'
  )->>'code',
  'invalid_closure_scope',
  'non-eligible exact process identities remain rejected'
);
select is(
  public.cmd_lcia_scope_closure_check_request_v2(
    '{
      "coverageMode":"subset",
      "processes":[{
        "id":"c7240000-0000-4000-8000-000000000010",
        "version":"01.00.000"
      }],
      "lciaMethods":[{
        "id":"503699e0-eca9-4089-8bf8-e0f49c93e578",
        "version":"01.01.000"
      }],
      "certificateFreshnessPolicy":"current-membership-required-v1"
    }'::jsonb,
    'candidate-current-membership',
    '{}'
  )->>'code',
  'current_release_required',
  'explicit current-membership freshness still requires a formal release'
);

reset role;

select * from finish();
rollback;
