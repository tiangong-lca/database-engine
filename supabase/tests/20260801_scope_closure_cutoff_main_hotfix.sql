begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(3);

-- Keep entity-trigger dispatch inside this rolled-back database test.
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
  null;
end;
$$;

insert into public.processes(
  id, version, state_code, json, json_ordered, user_id
) values (
  'c8010000-0000-4000-8000-000000000010',
  '01.00.000',
  100,
  '{"processDataSet":{"processInformation":{"dataSetInformation":{"common:UUID":"c8010000-0000-4000-8000-000000000010"}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000"}}}}',
  '{"processDataSet":{"processInformation":{"dataSetInformation":{"common:UUID":"c8010000-0000-4000-8000-000000000010"}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000"}}}}',
  null
)
on conflict (id, version) do update
set state_code = excluded.state_code,
    json = excluded.json,
    json_ordered = excluded.json_ordered;

insert into public.lciamethods(
  id, version, state_code, json, json_ordered, user_id
) values (
  '9ec743ea-6b00-400d-a53b-61547a3fc03c',
  '01.01.000',
  0,
  '{"LCIAMethodDataSet":{"LCIAMethodInformation":{"dataSetInformation":{"common:UUID":"503699e0-eca9-4089-8bf8-e0f49c93e578"}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.01.000"}}}}',
  '{"LCIAMethodDataSet":{"LCIAMethodInformation":{"dataSetInformation":{"common:UUID":"503699e0-eca9-4089-8bf8-e0f49c93e578"}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.01.000"}}}}',
  null
)
on conflict (id, version) do update
set state_code = excluded.state_code,
    json = excluded.json,
    json_ordered = excluded.json_ordered;

-- The hotfix runs before the schema cutover on a clean history and no-ops when
-- back-merged into an already-cut-over dev database. Test whichever owner is
-- active so the regression remains valid on both long-lived branches.
create or replace function pg_temp.scope_closure_normalize_request(
  p_requested_scope jsonb
) returns jsonb
language plpgsql
set search_path = ''
as $$
declare
  v_result jsonb;
begin
  if to_regprocedure(
    'private.lcia_scope_closure_normalize_request(jsonb)'
  ) is not null then
    execute
      'select private.lcia_scope_closure_normalize_request($1)'
      into v_result
      using p_requested_scope;
  elsif to_regprocedure(
    'public.lcia_scope_closure_normalize_request(jsonb)'
  ) is not null then
    execute
      'select public.lcia_scope_closure_normalize_request($1)'
      into v_result
      using p_requested_scope;
  else
    raise exception 'scope_closure_normalizer_missing';
  end if;
  return v_result;
end;
$$;

create temporary table scope_boundary_normalizations on commit drop as
select input.label,
  pg_temp.scope_closure_normalize_request(
    jsonb_build_object(
      'coverageMode', 'subset',
      'processes', jsonb_build_array(jsonb_build_object(
        'id', 'c8010000-0000-4000-8000-000000000010',
        'version', '01.00.000'
      )),
      'lciaMethods', jsonb_build_array(jsonb_build_object(
        'id', '503699e0-eca9-4089-8bf8-e0f49c93e578',
        'version', '01.01.000'
      )),
      'linkPolicy', case
        when input.boundary_policy is null then '{}'::jsonb
        else jsonb_build_object(
          'technosphereBoundaryPolicy', input.boundary_policy
        )
      end
    )
  ) as normalized_scope
from (values
  ('omitted', null::text),
  ('closed', 'closed'),
  ('open', 'open'),
  ('cutoff', 'cutoff')
) as input(label, boundary_policy);

select is(
  (
    select count(*)
    from scope_boundary_normalizations
    where normalized_scope->'linkPolicy'->>'technosphereBoundaryPolicy'
      = 'cutoff'
  ),
  4::bigint,
  'all supported legacy boundary inputs normalize to cutoff'
);

select is(
  (
    select count(distinct normalized_scope)
    from scope_boundary_normalizations
  ),
  1::bigint,
  'legacy boundary inputs produce one canonical requested scope and hash input'
);

select throws_ok(
  $sql$
    select pg_temp.scope_closure_normalize_request(
      '{
        "coverageMode":"subset",
        "processes":[{
          "id":"c8010000-0000-4000-8000-000000000010",
          "version":"01.00.000"
        }],
        "lciaMethods":[{
          "id":"503699e0-eca9-4089-8bf8-e0f49c93e578",
          "version":"01.01.000"
        }],
        "linkPolicy":{"technosphereBoundaryPolicy":"unknown"}
      }'::jsonb
    )
  $sql$,
  '22023',
  'invalid_closure_link_policy',
  'unknown boundary policies remain invalid'
);

select * from finish();
rollback;
