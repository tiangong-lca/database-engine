-- Database #624: Next Process/Flow Hybrid V2 correctness, routing, and ACL regression.
-- Local fixtures only; all writes roll back.
begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;
select extensions.no_plan();

create function pg_temp.next_v2_vector(a real, b real)
returns text
language sql
immutable
as $$
  select '[' || a::text || ',' || b::text || ',' ||
    array_to_string(array_fill('0'::text, array[1022]), ',') || ']';
$$;

insert into auth.users(
  id, instance_id, aud, role, email, raw_app_meta_data,
  raw_user_meta_data, created_at, updated_at
) values
  (
    '62000000-0000-4000-8000-000000000901',
    '00000000-0000-0000-0000-000000000000',
    'authenticated', 'authenticated', 'next-v2-owner@example.invalid',
    '{"provider":"email","providers":["email"]}', '{}', now(), now()
  ),
  (
    '62000000-0000-4000-8000-000000000902',
    '00000000-0000-0000-0000-000000000000',
    'authenticated', 'authenticated', 'next-v2-other@example.invalid',
    '{"provider":"email","providers":["email"]}', '{}', now(), now()
  );

insert into private.users(id, raw_user_meta_data, contact) values
  ('62000000-0000-4000-8000-000000000901', '{}', null),
  ('62000000-0000-4000-8000-000000000902', '{}', null);

insert into private.teams(id, json, rank, is_public) values
  ('62000000-0000-4000-8000-000000000903', '{"name":"Selected Team"}', 1, false),
  ('62000000-0000-4000-8000-000000000904', '{"name":"Foreign Team"}', 1, false);

insert into private.roles(user_id, team_id, role) values
  (
    '62000000-0000-4000-8000-000000000901',
    '62000000-0000-4000-8000-000000000903',
    'member'
  );

alter table public.processes disable trigger user;
alter table public.flows disable trigger user;
alter table public.processes enable trigger zz_next_hybrid_public_process_candidate_v2;
alter table public.flows enable trigger zz_next_hybrid_public_flow_candidate_v2;

create function pg_temp.next_v2_fixture(
  p_kind text,
  p_id uuid,
  p_version text,
  p_state integer,
  p_label text,
  p_a real,
  p_b real,
  p_actor uuid default '62000000-0000-4000-8000-000000000902',
  p_team uuid default null,
  p_process_type text default 'LCI result',
  p_flow_type text default 'Product flow',
  p_classification text default 'C-DEFAULT',
  p_elementary text default 'E-DEFAULT',
  p_top_category text default 'Resources',
  p_region text default 'CN'
) returns void
language plpgsql
as $$
declare
  v_document jsonb;
  v_table text;
begin
  if p_kind = 'process' then
    v_table := 'processes';
    v_document := jsonb_build_object(
      'testName', p_label,
      'region', p_region,
      'processDataSet', jsonb_build_object(
        'modellingAndValidation', jsonb_build_object(
          'LCIMethodAndAllocation', jsonb_build_object(
            'typeOfDataSet', p_process_type
          )
        )
      )
    );
  elsif p_kind = 'flow' then
    v_table := 'flows';
    v_document := jsonb_build_object(
      'testName', p_label,
      'region', p_region,
      'flowDataSet', jsonb_build_object(
        'modellingAndValidation', jsonb_build_object(
          'LCIMethod', jsonb_build_object('typeOfDataSet', p_flow_type)
        ),
        'flowInformation', jsonb_build_object(
          'dataSetInformation', jsonb_build_object(
            'classificationInformation', jsonb_build_object(
              'common:classification', jsonb_build_object(
                'common:class', jsonb_build_array(
                  jsonb_build_object('@classId', p_classification)
                )
              ),
              'common:elementaryFlowCategorization', jsonb_build_object(
                'common:category', jsonb_build_array(
                  jsonb_build_object(
                    '@catId', p_elementary,
                    '#text', p_top_category,
                    '@level', '0'
                  )
                )
              )
            )
          )
        )
      )
    );
  else
    raise exception 'invalid fixture kind';
  end if;

  execute format(
    'insert into public.%I(
       id, version, state_code, json, json_ordered, user_id, team_id,
       modified_at, extracted_md, search_text, embedding_ft,
       rule_verification
     ) values (
       $1, $2, $3, $4, $4::json, $5, $6,
       ''2026-09-05 17:00:00+00''::timestamptz,
       $7, $8, $9::extensions.vector(1024), true
     )',
    v_table
  ) using
    p_id, p_version, p_state, v_document, p_actor, p_team,
    'LOCAL NEXT V2 EMBEDDING FIXTURE',
    array[p_label, 'water ' || p_label],
    pg_temp.next_v2_vector(p_a, p_b);
end;
$$;

do $$
begin
  perform pg_temp.next_v2_fixture(
    'process', '62000000-0000-4000-8000-000000000101', '01.00.000',
    100, 'ProcessNeedle', 1, 0
  );
  perform pg_temp.next_v2_fixture(
    'process', '62000000-0000-4000-8000-000000000101', '01.00.001',
    100, 'ProcessNeedleNew', 0.98, 0.02
  );
  perform pg_temp.next_v2_fixture(
    'process', '62000000-0000-4000-8000-000000000102', '01.00.000',
    100, 'BlackBoxNeedle', 1, 0,
    '62000000-0000-4000-8000-000000000902',
    '62000000-0000-4000-8000-000000000903',
    'Unit process, black box'
  );
  perform pg_temp.next_v2_fixture(
    'process', '62000000-0000-4000-8000-000000000103', '01.00.000',
    200, 'CommercialNeedle', 1, 0
  );
  perform pg_temp.next_v2_fixture(
    'process', '62000000-0000-4000-8000-000000000104', '01.00.000',
    0, 'PrivateNeedle', 1, 0,
    '62000000-0000-4000-8000-000000000901'
  );
  perform pg_temp.next_v2_fixture(
    'process', '62000000-0000-4000-8000-000000000105', '01.00.000',
    20, 'SelectedTeamNeedle', 1, 0,
    '62000000-0000-4000-8000-000000000902',
    '62000000-0000-4000-8000-000000000903'
  );
  perform pg_temp.next_v2_fixture(
    'process', '62000000-0000-4000-8000-000000000106', '01.00.000',
    20, 'ForeignTeamNeedle', 1, 0,
    '62000000-0000-4000-8000-000000000902',
    '62000000-0000-4000-8000-000000000904'
  );

  perform pg_temp.next_v2_fixture(
    'flow', '62000000-0000-4000-8000-000000000201', '01.00.000',
    100, 'FlowClassNeedle', 1, 0,
    '62000000-0000-4000-8000-000000000902', null,
    'LCI result', 'Product flow', 'C-ONE', 'E-ONE', 'Resources'
  );
  perform pg_temp.next_v2_fixture(
    'flow', '62000000-0000-4000-8000-000000000202', '01.00.000',
    100, 'FlowOtherNeedle', 0.9, 0.1,
    '62000000-0000-4000-8000-000000000902',
    '62000000-0000-4000-8000-000000000903',
    'LCI result', 'Product flow', 'C-TWO', 'E-TWO', 'Resources'
  );
  perform pg_temp.next_v2_fixture(
    'flow', '62000000-0000-4000-8000-000000000203', '01.00.000',
    100, 'EmissionNeedle', 1, 0,
    '62000000-0000-4000-8000-000000000902', null,
    'LCI result', 'Elementary flow', 'C-THREE', 'E-EMISSION', 'Emissions'
  );
  perform pg_temp.next_v2_fixture(
    'flow', '62000000-0000-4000-8000-000000000204', '01.00.000',
    0, 'PrivateFlowNeedle', 1, 0,
    '62000000-0000-4000-8000-000000000901'
  );
  perform pg_temp.next_v2_fixture(
    'flow', '62000000-0000-4000-8000-000000000205', '01.00.000',
    20, 'SelectedTeamFlowNeedle', 1, 0,
    '62000000-0000-4000-8000-000000000902',
    '62000000-0000-4000-8000-000000000903'
  );
  perform pg_temp.next_v2_fixture(
    'flow', '62000000-0000-4000-8000-000000000206', '01.00.000',
    20, 'ForeignTeamFlowNeedle', 1, 0,
    '62000000-0000-4000-8000-000000000902',
    '62000000-0000-4000-8000-000000000904'
  );
end;
$$;

grant execute on function pg_temp.next_v2_vector(real, real)
  to authenticated, api_internal_executor, next_public_search_executor;

select extensions.ok(
  exists (
    select 1
    from pg_catalog.pg_roles
    where rolname = 'next_public_search_executor'
      and not rolsuper and not rolinherit and not rolcanlogin
      and not rolbypassrls and not rolcreaterole and not rolcreatedb
  ),
  'public candidate executor is a fixed NOLOGIN, NOBYPASSRLS role'
);

select extensions.ok(
  not pg_catalog.has_table_privilege(
    'next_public_search_executor', 'public.flows', 'SELECT'
  ) and not pg_catalog.has_table_privilege(
    'next_public_search_executor', 'public.processes', 'SELECT'
  ),
  'public candidate executor has no whole-table SELECT grant'
);

select extensions.ok(
  not pg_catalog.has_table_privilege(
    'next_public_search_executor',
    'private.next_hybrid_public_candidates_v2',
    'SELECT'
  ) and pg_catalog.has_column_privilege(
    'next_public_search_executor',
    'private.next_hybrid_public_candidates_v2',
    'classification_codes',
    'SELECT'
  ),
  'public candidate executor receives only sidecar search-key columns'
);

select extensions.ok(
  pg_catalog.has_column_privilege(
    'next_public_search_executor', 'public.flows', 'search_text', 'SELECT'
  ) and pg_catalog.has_column_privilege(
    'next_public_search_executor', 'public.processes', 'embedding_ft', 'SELECT'
  ),
  'public candidate executor has only the columns needed for search'
);

select extensions.ok(
  not pg_catalog.has_table_privilege(
    'next_public_search_executor', 'public.flows',
    'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
  ) and not pg_catalog.has_table_privilege(
    'next_public_search_executor', 'public.processes',
    'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
  ),
  'public candidate executor cannot mutate either dataset table'
);

select extensions.is(
  (
    select pg_catalog.array_agg(code order by code)
    from pg_catalog.unnest(
      private.next_hybrid_json_codes_v2(
        '[{"@classId":"B"},{"@classId":"A"},{"@classId":"A"}]',
        '@classId'
      )
    ) as code
  ),
  array['A', 'B']::text[],
  'classification extractor normalizes arrays and removes duplicates'
);

select extensions.is(
  private.next_hybrid_json_codes_v2('{"@catId":"E-ONE"}', '@catId'),
  array['E-ONE']::text[],
  'classification extractor normalizes singleton objects'
);

select extensions.ok(
  (
    select pg_catalog.bool_and(indisvalid and indisready and indislive)
    from pg_catalog.pg_index
    where indexrelid in (
      'private.next_hybrid_public_candidate_type_v2_idx'::regclass,
      'private.next_hybrid_public_candidate_team_v2_idx'::regclass,
      'private.next_hybrid_public_candidate_emission_v2_idx'::regclass,
      'private.next_hybrid_public_candidate_classification_v2_idx'::regclass,
      'private.next_hybrid_public_candidate_elementary_v2_idx'::regclass
    )
  ),
  'all five V2 sidecar candidate indexes are live, ready, and valid'
);

select extensions.ok(
  not exists (
    select 1
    from pg_catalog.pg_attribute as attribute
    where attribute.attrelid =
      'private.next_hybrid_public_candidates_v2'::regclass
      and attribute.attnum > 0
      and not attribute.attisdropped
      and attribute.atttypid = 'extensions.vector'::regtype
  ),
  'public candidate projection contains no embeddings'
);

select extensions.is(
  (
    select count(*)
    from private.next_hybrid_public_candidates_v2
    where dataset_kind = 'process'
  ),
  4::bigint,
  'sidecar trigger projects only public-state embedded Process versions'
);

select extensions.is(
  (
    select count(*)
    from private.next_hybrid_public_candidates_v2
    where dataset_kind = 'flow'
  ),
  3::bigint,
  'sidecar trigger projects only public-state embedded Flow versions'
);

select extensions.ok(
  not pg_catalog.has_function_privilege(
    'anon',
    'api.hybrid_search_process_versions_v2(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid,text)',
    'EXECUTE'
  ) and pg_catalog.has_function_privilege(
    'authenticated',
    'api.hybrid_search_process_versions_v2(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid,text)',
    'EXECUTE'
  ),
  'Process V2 API is authenticated-only'
);

select extensions.ok(
  not pg_catalog.has_function_privilege(
    'anon',
    'api.hybrid_search_flow_versions_v2(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)',
    'EXECUTE'
  ) and pg_catalog.has_function_privilege(
    'authenticated',
    'api.hybrid_search_flow_versions_v2(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)',
    'EXECUTE'
  ),
  'Flow V2 API is authenticated-only'
);

select extensions.ok(
  not pg_catalog.has_function_privilege(
    'authenticated',
    'private.next_semantic_version_candidates_v2(text,extensions.vector,jsonb,text,text[],boolean,text[],text[],text,integer,uuid)',
    'EXECUTE'
  ),
  'candidate helpers are not directly callable by authenticated users'
);

grant api_internal_executor to postgres;
set local role api_internal_executor;

select extensions.is(
  (
    select semantic_route
    from private.next_public_semantic_version_candidates_v2(
      'process', pg_temp.next_v2_vector(1, 0)::extensions.vector,
      '{}', 'LCI result', '{}', false, '{}', '{}', 'tg', null
    )
    limit 1
  ),
  'exact',
  'selective Process type uses bounded exact cosine routing'
);

select extensions.is(
  (
    select candidate_population
    from private.next_public_semantic_version_candidates_v2(
      'process', pg_temp.next_v2_vector(1, 0)::extensions.vector,
      '{}', 'LCI result', '{}', false, '{}', '{}', 'tg', null
    )
    limit 1
  ),
  2,
  'Process exact route reports the complete filtered population'
);

select extensions.is(
  (
    select pg_catalog.array_agg(id::text || '@' || version order by rank)
    from private.next_public_semantic_version_candidates_v2(
      'process', pg_temp.next_v2_vector(1, 0)::extensions.vector,
      '{}', 'LCI result', '{}', false, '{}', '{}', 'tg', null
    )
  ),
  array[
    '62000000-0000-4000-8000-000000000101@01.00.000',
    '62000000-0000-4000-8000-000000000101@01.00.001'
  ]::text[],
  'Process exact route preserves and ranks exact versions'
);

select extensions.is(
  (
    select semantic_route
    from private.next_public_semantic_version_candidates_v2(
      'flow', pg_temp.next_v2_vector(1, 0)::extensions.vector,
      '{}', null, '{}', false, array['C-ONE'], '{}', 'tg', null
    )
  ),
  'exact',
  'Flow classification uses bounded exact cosine routing'
);

select extensions.is(
  (
    select id
    from private.next_public_semantic_version_candidates_v2(
      'flow', pg_temp.next_v2_vector(1, 0)::extensions.vector,
      '{}', null, '{}', false, array['C-ONE'], '{}', 'tg', null
    )
  ),
  '62000000-0000-4000-8000-000000000201'::uuid,
  'Flow classification filtering is applied before candidate ranking'
);

select extensions.is(
  (
    select id
    from private.next_public_semantic_version_candidates_v2(
      'flow', pg_temp.next_v2_vector(1, 0)::extensions.vector,
      '{}', null, '{}', false, '{}', array['E-EMISSION'], 'tg', null
    )
  ),
  '62000000-0000-4000-8000-000000000203'::uuid,
  'Flow elementary category filtering is applied before candidate ranking'
);

select extensions.is(
  (
    select semantic_route
    from private.next_public_semantic_version_candidates_v2(
      'flow', pg_temp.next_v2_vector(1, 0)::extensions.vector,
      '{}', null, '{}', false, '{}', '{}', 'tg', null
    )
    limit 1
  ),
  'hnsw',
  'unfiltered Flow search retains strict iterative HNSW routing'
);

select extensions.is(
  (
    select candidate_population
    from private.next_public_semantic_version_candidates_v2(
      'flow', pg_temp.next_v2_vector(1, 0)::extensions.vector,
      '{}', null, '{}', false, '{}', '{}', 'tg', null
    )
    limit 1
  ),
  null::integer,
  'unfiltered HNSW route does not perform an exact population probe'
);

select extensions.is(
  (
    select count(*)
    from private.next_public_semantic_version_candidates_v2(
      'process', pg_temp.next_v2_vector(1, 0)::extensions.vector,
      '{}', 'Avoided product system', '{}', false, '{}', '{}', 'tg', null
    )
  ),
  0::bigint,
  'known-empty indexed Process filter completes without a second search call'
);

-- Simulate a stale or damaged projection entry. Candidate discovery may now
-- over-select this row, but the public source hydration below must still apply
-- the canonical classification filter before returning any result.
update private.next_hybrid_public_candidates_v2
set classification_codes = array['C-ONE']
where dataset_kind = 'flow'
  and id = '62000000-0000-4000-8000-000000000202'
  and version = '01.00.000';

reset role;
select set_config(
  'request.jwt.claim.sub',
  '62000000-0000-4000-8000-000000000901',
  true
);
select set_config('request.jwt.claim.role', 'authenticated', true);
set local role authenticated;

select extensions.is(
  (
    select id
    from api.hybrid_search_flow_versions_v2(
      'FlowClassNeedle', pg_temp.next_v2_vector(1, 0),
      '{"classification":[{"scope":"classification","code":"C-ONE"}]}',
      0.5, 200, 0, 1, 10, 'tg', 10, 1,
      array['FlowClassNeedle'], 100, null
    )
  ),
  '62000000-0000-4000-8000-000000000201'::uuid,
  'Flow V2 API fixes canonical classification filtering end to end'
);

select extensions.is(
  (
    select pg_catalog.array_agg(id order by id)
    from api.hybrid_search_flow_versions_v2(
      'FlowClassNeedle', pg_temp.next_v2_vector(1, 0),
      '{"classification":[{"scope":"classification","code":"C-ONE"}]}',
      0.5, 200, 0, 1, 10, 'tg', 10, 1,
      array['FlowClassNeedle'], 100, null
    )
  ),
  array['62000000-0000-4000-8000-000000000201'::uuid],
  'source hydration rechecks Flow filters when the candidate projection drifts'
);

select extensions.is(
  (
    select id
    from api.hybrid_search_flow_versions_v2(
      'FlowClassNeedle', pg_temp.next_v2_vector(1, 0),
      '{"classification":[{"scope":"classification","code":" C-ONE "}]}',
      0.5, 200, 0, 1, 10, 'tg', 10, 1,
      array['FlowClassNeedle'], 100, null
    )
  ),
  '62000000-0000-4000-8000-000000000201'::uuid,
  'Flow V2 API trims canonical classification codes before matching'
);

select extensions.is(
  (
    select id
    from api.hybrid_search_flow_versions_v2(
      'FlowClassNeedle', pg_temp.next_v2_vector(1, 0),
      '{"asInput":true}', 0.5, 200, 0, 1, 10,
      'tg', 10, 1, array['FlowClassNeedle'], 100, null
    )
    where id = '62000000-0000-4000-8000-000000000203'
  ),
  null::uuid,
  'Flow as-input filtering excludes elementary emissions'
);

select extensions.is(
  (
    select pg_catalog.bool_and(semantic_fallback_used)
    from api.hybrid_search_process_versions_v2(
      'ProcessNeedle', pg_temp.next_v2_vector(1, 0), '{}',
      1, 200, 0, 1, 10, 'tg', 10, 1,
      array['ProcessNeedle'], 100, null, 'LCI result'
    )
  ),
  true,
  'threshold-empty semantic results fall back inside the same V2 RPC'
);

select extensions.is(
  (
    select pg_catalog.bool_and(not semantic_fallback_used)
    from api.hybrid_search_process_versions_v2(
      'ProcessNeedle', pg_temp.next_v2_vector(1, 0), '{}',
      0.5, 200, 0, 1, 10, 'tg', 10, 1,
      array['ProcessNeedle'], 100, null, 'LCI result'
    )
  ),
  true,
  'primary semantic matches do not report threshold fallback'
);

select extensions.is(
  (
    select pg_catalog.array_agg(version::text order by version::text)
    from api.hybrid_search_process_versions_v2(
      'ProcessNeedle', pg_temp.next_v2_vector(1, 0), '{}',
      0.5, 200, 0, 1, 10, 'tg', 10, 1,
      array['ProcessNeedle'], 100, null, 'LCI result'
    )
  ),
  array['01.00.000', '01.00.001']::text[],
  'Process V2 API retains two exact versions of the same identity'
);

select extensions.is(
  (
    select count(*)
    from api.hybrid_search_process_versions_v2(
      'CommercialNeedle', pg_temp.next_v2_vector(1, 0), '{}',
      0.5, 200, 1, 0, 10, 'tg', 10, 1,
      array['CommercialNeedle'], 200, null, null
    )
  ),
  0::bigint,
  'public source and state filter cannot be combined to broaden visibility'
);

select extensions.is(
  (
    select id
    from api.hybrid_search_process_versions_v2(
      'PrivateNeedle', pg_temp.next_v2_vector(1, 0), '{}',
      0.5, 200, 1, 0, 10, 'my', 10, 1,
      array['PrivateNeedle'], 0, null, null
    )
  ),
  '62000000-0000-4000-8000-000000000104'::uuid,
  'my scope remains bound to the JWT actor and requested state'
);

select extensions.is(
  (
    select id
    from api.hybrid_search_process_versions_v2(
      'SelectedTeamNeedle', pg_temp.next_v2_vector(1, 0), '{}',
      0.5, 200, 1, 0, 10, 'te', 10, 1,
      array['SelectedTeamNeedle'], 20,
      '62000000-0000-4000-8000-000000000903', null
    )
  ),
  '62000000-0000-4000-8000-000000000105'::uuid,
  'te scope returns the explicitly selected readable team only'
);

select extensions.is(
  (
    select count(*)
    from api.hybrid_search_process_versions_v2(
      'SelectedTeamNeedle', pg_temp.next_v2_vector(1, 0), '{}',
      0.5, 200, 1, 0, 10, 'te', 10, 1,
      array['SelectedTeamNeedle'], 20, null, null
    )
  ),
  0::bigint,
  'te scope fails closed when selected team context is absent'
);

select extensions.is(
  (
    select count(*)
    from api.hybrid_search_flow_versions_v2(
      'ForeignTeamFlowNeedle', pg_temp.next_v2_vector(1, 0), '{}',
      0.5, 200, 1, 0, 10, 'te', 10, 1,
      array['ForeignTeamFlowNeedle'], 20,
      '62000000-0000-4000-8000-000000000904'
    )
  ),
  0::bigint,
  'te scope rejects a selected team outside the JWT actor membership'
);

select extensions.throws_ok(
  $$
    select *
    from api.hybrid_search_flow_versions_v2(
      'bad', pg_temp.next_v2_vector(1, 0),
      '{"classification":[{"scope":"wrong","code":"C-ONE"}]}',
      0.5, 200, 0.5, 0.5, 10, 'tg', 10, 1,
      array['bad'], 100, null
    )
  $$,
  '22023',
  'invalid Next Flow Hybrid V2 request',
  'malformed Flow classification contract fails closed'
);

select extensions.throws_ok(
  $$
    select *
    from api.hybrid_search_flow_versions_v2(
      'bad', pg_temp.next_v2_vector(1, 0),
      '{"flowType":"  "}', 0.5, 200, 0.5, 0.5, 10,
      'tg', 10, 1, array['bad'], 100, null
    )
  $$,
  '22023',
  'invalid Next Flow Hybrid V2 request',
  'blank provided Flow type fails closed instead of broadening scope'
);

select extensions.throws_ok(
  $$
    select *
    from api.hybrid_search_process_versions_v2(
      'bad', pg_temp.next_v2_vector(1, 0), '{}',
      0.5, 200, 0.5, 0.5, 10, 'tg', 10, 1,
      array['bad'], 100, null, 'foreground'
    )
  $$,
  '22023',
  'invalid Next Process Hybrid V2 request',
  'unsupported Process type contract fails closed'
);

select extensions.is(
  (
    select id
    from api.hybrid_search_process_versions_v2(
      'BlackBoxNeedle', pg_temp.next_v2_vector(1, 0), '{}',
      0.5, 200, 0, 1, 10, 'tg', 10, 1,
      array['BlackBoxNeedle'], 100,
      '62000000-0000-4000-8000-000000000903',
      'Unit process, black box'
    )
  ),
  '62000000-0000-4000-8000-000000000102'::uuid,
  'public institution team context remains an optional narrowing filter'
);

reset role;
select * from extensions.finish();
rollback;
