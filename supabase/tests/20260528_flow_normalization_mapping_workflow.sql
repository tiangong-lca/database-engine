begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create or replace function pg_temp.disable_trigger_if_exists(p_table regclass, p_trigger name)
returns void
language plpgsql
as $$
begin
  if exists (
    select 1
    from pg_trigger
    where tgrelid = p_table
      and tgname = p_trigger
      and not tgisinternal
  ) then
    execute format('alter table %s disable trigger %I', p_table, p_trigger);
  end if;
end;
$$;

select plan(16);

select ok(to_regclass('public.flow_normalization_canonical_flows') is not null, 'canonical flow table exists');
select ok(to_regclass('public.flow_normalization_clusters') is not null, 'flow normalization cluster table exists');
select ok(to_regclass('public.flow_normalization_cluster_members') is not null, 'cluster member table exists');
select ok(to_regclass('public.flow_normalization_mappings') is not null, 'historical-to-canonical mapping table exists');
select ok(to_regprocedure('public.flow_normalization_flow_identity_rows(integer[])') is not null, 'flow identity row function exists');
select ok(to_regprocedure('public.qry_flow_normalization_candidate_clusters(integer, integer[], integer)') is not null, 'candidate cluster query function exists');

alter table public.flows disable trigger "flows_json_sync_trigger";
select pg_temp.disable_trigger_if_exists('public.flows'::regclass, 'flow_extract_md_trigger_insert');
select pg_temp.disable_trigger_if_exists('public.flows'::regclass, 'flow_extract_text_trigger_insert');
select pg_temp.disable_trigger_if_exists('public.flows'::regclass, 'flow_dataset_extraction_trigger_insert');

insert into public.flows (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  team_id,
  rule_verification,
  created_at,
  modified_at
)
values
  (
    '10600000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Recovered aluminum scrap"}]},"CASNumber":"7429-90-5","classificationInformation":{"common:classification":{"common:class":[{"@classId":"metal-scrap","#text":"Metal scrap"}]}}},"geography":{"locationOfSupply":"GLO"}},"modellingAndValidation":{"LCIMethod":{"typeOfDataSet":"Product flow"}},"flowProperties":{"flowProperty":[{"@dataSetInternalID":"0","referenceToFlowPropertyDataSet":{"@refObjectId":"93a60a56-a3c8-11da-a746-0800200b9a66","common:shortDescription":"Mass"}}]}}}'::jsonb,
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Recovered aluminum scrap"}]},"CASNumber":"7429-90-5","classificationInformation":{"common:classification":{"common:class":[{"@classId":"metal-scrap","#text":"Metal scrap"}]}}},"geography":{"locationOfSupply":"GLO"}},"modellingAndValidation":{"LCIMethod":{"typeOfDataSet":"Product flow"}},"flowProperties":{"flowProperty":[{"@dataSetInternalID":"0","referenceToFlowPropertyDataSet":{"@refObjectId":"93a60a56-a3c8-11da-a746-0800200b9a66","common:shortDescription":"Mass"}}]}}}'::json,
    '10600000-0000-0000-0000-000000000101',
    100,
    null,
    true,
    now() - interval '2 days',
    now() - interval '2 days'
  ),
  (
    '10600000-0000-0000-0000-000000000002',
    '01.00.000',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Recovered aluminum scrap"}]},"CASNumber":"7429-90-5","classificationInformation":{"common:classification":{"common:class":[{"@classId":"metal-scrap","#text":"Metal scrap"}]}}},"geography":{"locationOfSupply":"GLO"}},"modellingAndValidation":{"LCIMethod":{"typeOfDataSet":"Product flow"}},"flowProperties":{"flowProperty":[{"@dataSetInternalID":"0","referenceToFlowPropertyDataSet":{"@refObjectId":"93a60a56-a3c8-11da-a746-0800200b9a66","common:shortDescription":"Mass"}}]}}}'::jsonb,
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Recovered aluminum scrap"}]},"CASNumber":"7429-90-5","classificationInformation":{"common:classification":{"common:class":[{"@classId":"metal-scrap","#text":"Metal scrap"}]}}},"geography":{"locationOfSupply":"GLO"}},"modellingAndValidation":{"LCIMethod":{"typeOfDataSet":"Product flow"}},"flowProperties":{"flowProperty":[{"@dataSetInternalID":"0","referenceToFlowPropertyDataSet":{"@refObjectId":"93a60a56-a3c8-11da-a746-0800200b9a66","common:shortDescription":"Mass"}}]}}}'::json,
    '10600000-0000-0000-0000-000000000102',
    100,
    null,
    true,
    now() - interval '1 day',
    now() - interval '1 day'
  ),
  (
    '10600000-0000-0000-0000-000000000003',
    '01.00.000',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Recovered copper scrap"}]},"CASNumber":"7440-50-8","classificationInformation":{"common:classification":{"common:class":[{"@classId":"metal-scrap","#text":"Metal scrap"}]}}},"geography":{"locationOfSupply":"GLO"}},"modellingAndValidation":{"LCIMethod":{"typeOfDataSet":"Product flow"}},"flowProperties":{"flowProperty":[{"@dataSetInternalID":"0","referenceToFlowPropertyDataSet":{"@refObjectId":"93a60a56-a3c8-11da-a746-0800200b9a66","common:shortDescription":"Mass"}}]}}}'::jsonb,
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Recovered copper scrap"}]},"CASNumber":"7440-50-8","classificationInformation":{"common:classification":{"common:class":[{"@classId":"metal-scrap","#text":"Metal scrap"}]}}},"geography":{"locationOfSupply":"GLO"}},"modellingAndValidation":{"LCIMethod":{"typeOfDataSet":"Product flow"}},"flowProperties":{"flowProperty":[{"@dataSetInternalID":"0","referenceToFlowPropertyDataSet":{"@refObjectId":"93a60a56-a3c8-11da-a746-0800200b9a66","common:shortDescription":"Mass"}}]}}}'::json,
    '10600000-0000-0000-0000-000000000103',
    100,
    null,
    true,
    now(),
    now()
  );

select is(
  (
    select count(*)::integer
    from public.qry_flow_normalization_candidate_clusters(2, array[100], 20)
    where normalized_name = 'recovered aluminum scrap'
  ),
  1,
  'candidate query groups same-name same-property duplicate flows'
);

select is(
  (
    select flow_count::integer
    from public.qry_flow_normalization_candidate_clusters(2, array[100], 20)
    where normalized_name = 'recovered aluminum scrap'
  ),
  2,
  'candidate query reports both duplicate members'
);

select is(
  (
    select jsonb_array_length(members)
    from public.qry_flow_normalization_candidate_clusters(2, array[100], 20)
    where normalized_name = 'recovered aluminum scrap'
  ),
  2,
  'candidate query preserves member evidence payloads'
);

insert into public.flow_normalization_canonical_flows (
  canonical_flow_uuid,
  flow_id,
  flow_version,
  canonical_status,
  canonical_name,
  canonical_basis,
  lca_semantics_preservation,
  evidence
)
values (
  '10610000-0000-0000-0000-000000000001',
  '10600000-0000-0000-0000-000000000001',
  '01.00.000',
  'confirmed',
  'Recovered aluminum scrap',
  'manual expert review',
  'Same product flow, mass property, CAS number, classification, and global supply location; no calculation-relevant distinction is lost.',
  '[{"type":"fixture","note":"same flow identity signals"}]'::jsonb
);

insert into public.flow_normalization_clusters (
  cluster_id,
  cluster_status,
  cluster_basis,
  normalized_identity_key,
  normalized_name,
  type_of_data_set,
  flow_property_ref_id,
  flow_property_name,
  cas_number,
  classification_key,
  location_of_supply,
  candidate_signature,
  canonical_flow_uuid,
  lca_semantics_preservation,
  evidence
)
select
  '10620000-0000-0000-0000-000000000001',
  'confirmed',
  'identity_signature',
  identity_key,
  normalized_name,
  type_of_data_set,
  flow_property_ref_id,
  flow_property_name,
  cas_number,
  classification_key,
  location_of_supply,
  candidate_signature,
  '10610000-0000-0000-0000-000000000001',
  'Confirmed cluster preserves product-flow identity and mass-basis calculation semantics.',
  '[{"type":"fixture","note":"duplicate candidate accepted"}]'::jsonb
from public.qry_flow_normalization_candidate_clusters(2, array[100], 20)
where normalized_name = 'recovered aluminum scrap';

insert into public.flow_normalization_cluster_members (
  cluster_id,
  flow_id,
  flow_version,
  member_role,
  match_basis,
  similarity_score,
  evidence
)
values
  (
    '10620000-0000-0000-0000-000000000001',
    '10600000-0000-0000-0000-000000000001',
    '01.00.000',
    'canonical',
    'identity_signature',
    1.0,
    '[{"type":"fixture","note":"canonical member"}]'::jsonb
  ),
  (
    '10620000-0000-0000-0000-000000000001',
    '10600000-0000-0000-0000-000000000002',
    '01.00.000',
    'historical',
    'identity_signature',
    1.0,
    '[{"type":"fixture","note":"historical duplicate"}]'::jsonb
  );

insert into public.flow_normalization_mappings (
  mapping_id,
  cluster_id,
  historical_flow_id,
  historical_flow_version,
  canonical_flow_uuid,
  canonical_flow_id,
  canonical_flow_version,
  mapping_status,
  mapping_reason,
  lca_semantics_preservation,
  calculation_semantics_preserved,
  evidence
)
values (
  '10630000-0000-0000-0000-000000000001',
  '10620000-0000-0000-0000-000000000001',
  '10600000-0000-0000-0000-000000000002',
  '01.00.000',
  '10610000-0000-0000-0000-000000000001',
  '10600000-0000-0000-0000-000000000001',
  '01.00.000',
  'confirmed',
  'Historical flow is a duplicate split only by record identity.',
  'Both flows share the same material, product-flow type, mass property, CAS number, classification, and location; provider matching can use the canonical flow.',
  true,
  '[{"type":"fixture","note":"confirmed mapping evidence"}]'::jsonb
);

select ok(
  exists (
    select 1
    from public.flow_normalization_mappings
    where historical_flow_id = '10600000-0000-0000-0000-000000000002'
      and historical_flow_version = '01.00.000'
      and canonical_flow_id = '10600000-0000-0000-0000-000000000001'
      and mapping_status = 'confirmed'
  ),
  'confirmed mapping records historical flow to canonical flow'
);

select ok(
  exists (
    select 1
    from public.flow_normalization_cluster_members
    where cluster_id = '10620000-0000-0000-0000-000000000001'
      and member_role = 'canonical'
  ),
  'cluster records exactly one canonical member'
);

create or replace function pg_temp.confirmed_mapping_without_semantics_rejected()
returns boolean
language plpgsql
as $$
begin
  insert into public.flow_normalization_mappings (
    cluster_id,
    historical_flow_id,
    historical_flow_version,
    canonical_flow_uuid,
    canonical_flow_id,
    canonical_flow_version,
    mapping_status,
    mapping_reason,
    lca_semantics_preservation,
    calculation_semantics_preserved,
    evidence
  )
  values (
    '10620000-0000-0000-0000-000000000001',
    '10600000-0000-0000-0000-000000000003',
    '01.00.000',
    '10610000-0000-0000-0000-000000000001',
    '10600000-0000-0000-0000-000000000001',
    '01.00.000',
    'confirmed',
    'Unsafe fixture mapping should be rejected.',
    'This row intentionally does not mark calculation semantics preserved.',
    false,
    '[{"type":"fixture","note":"must fail"}]'::jsonb
  );

  return false;
exception
  when check_violation then
    return true;
end;
$$;

select ok(
  pg_temp.confirmed_mapping_without_semantics_rejected(),
  'confirmed mappings require preserved calculation semantics'
);

select ok(
  not has_table_privilege('authenticated', 'public.flow_normalization_mappings', 'insert'),
  'authenticated users cannot directly insert flow normalization mappings'
);

select ok(
  has_table_privilege('service_role', 'public.flow_normalization_mappings', 'insert'),
  'service role can manage flow normalization mappings'
);

select ok(
  has_function_privilege('authenticated', 'public.qry_flow_normalization_candidate_clusters(integer, integer[], integer)', 'execute'),
  'authenticated users can run the read-only candidate query under normal RLS'
);

select ok(
  not has_function_privilege('anon', 'public.qry_flow_normalization_candidate_clusters(integer, integer[], integer)', 'execute'),
  'anonymous users cannot run the flow normalization candidate query'
);

select * from finish();

rollback;
