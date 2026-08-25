begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select extensions.no_plan();

-- Portal LCIA is deliberately additive.  Keep the existing Data Product and
-- Release control-plane entry points frozen while the projection gets its own
-- actor, service, and anonymous-read boundaries.
create temporary table portal_lcia_expected_routines (
  boundary text not null,
  routine_identity text primary key
) on commit drop;

insert into portal_lcia_expected_routines (boundary, routine_identity)
values
  ('producer', 'api.cmd_lcia_result_build_request_v3(text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb)'),
  ('actor', 'api.qry_portal_lcia_result_package_publish_prepare_v1(uuid, text)'),
  ('actor', 'api.cmd_portal_lcia_result_package_publish_v1(uuid, text, text, text, jsonb)'),
  ('actor', 'api.qry_portal_lcia_projection_prepare_v1(uuid, uuid)'),
  ('actor', 'api.cmd_portal_lcia_projection_finalize_publication_v1(uuid, uuid, text, text, text, text, jsonb)'),
  ('actor', 'api.qry_portal_lcia_projection_publication_readback_v1(uuid, text)'),
  ('actor', 'api.cmd_portal_lcia_projection_revoke_publication_v1(uuid, text, text, jsonb)'),
  ('service', 'private.svc_portal_lcia_projection_stage_begin_v1(uuid, uuid, integer, integer, jsonb)'),
  ('service', 'private.svc_portal_lcia_projection_stage_register_batch_v1(uuid, uuid, jsonb)'),
  ('service', 'private.svc_portal_lcia_projection_stage_status_v1(uuid, uuid)'),
  ('service', 'private.svc_portal_lcia_projection_stage_seal_v1(uuid, uuid)'),
  ('service', 'private.svc_portal_lcia_projection_stage_fail_v1(uuid, uuid, text, text, jsonb)'),
  ('service', 'private.svc_portal_lcia_projection_worker_input_v1(uuid, uuid)'),
  ('service', 'private.svc_portal_lcia_projection_package_mark_ready_v1(uuid, uuid, uuid, text, uuid, uuid, uuid, jsonb, jsonb, jsonb, jsonb, text, text, jsonb)'),
  ('public', 'api.portal_get_published_lcia_values_v1(text, jsonb, text, text, integer)');

create temporary table portal_lcia_legacy_routines (
  routine_identity text primary key
) on commit drop;

insert into portal_lcia_legacy_routines (routine_identity)
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
  ('api.get_current_lca_release()');

select extensions.is(
  (
    select count(*)
    from portal_lcia_expected_routines as expected
    where pg_catalog.to_regprocedure(expected.routine_identity) is not null
  ),
  15::bigint,
  'the Portal LCIA surface has all fifteen exact frozen producer, actor, service, and public signatures'
);

select extensions.is(
  (
    select count(*)
    from portal_lcia_expected_routines as expected
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(expected.routine_identity)
    where expected.boundary in ('actor', 'service')
      and routine.prosecdef
      and routine.proconfig @> array['search_path=""']::text[]
      and routine.proowner = 'postgres'::regrole
  ),
  13::bigint,
  'all Portal LCIA actor and service routines are postgres-owned security definers with an empty search_path'
);

select extensions.ok(
  (
    select routine.prosecdef
      and routine.proconfig @> array['search_path=""']::text[]
      and routine.proowner = 'portal_public_executor'::regrole
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'api.portal_get_published_lcia_values_v1(text,jsonb,text,text,integer)'::regprocedure
  ),
  'the anonymous reader uses the constrained portal_public_executor rather than postgres ownership'
);

select extensions.ok(
  (
    select routine.prosecdef
      and routine.proowner = 'postgres'::regrole
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'api.cmd_lcia_result_build_request_v3(text,jsonb,text,text,jsonb,text,uuid,text,text,jsonb)'::regprocedure
  ),
  'the V3 build producer retains the postgres-owned actor command boundary'
);

select extensions.is(
  (
    select count(*)
    from pg_catalog.pg_proc as routine
    join pg_catalog.pg_namespace as namespace
      on namespace.oid = routine.pronamespace
    where (
      namespace.nspname = 'api'
      and routine.proname in (
        'cmd_lcia_result_build_request_v3',
        'qry_portal_lcia_result_package_publish_prepare_v1',
        'cmd_portal_lcia_result_package_publish_v1',
        'qry_portal_lcia_projection_prepare_v1',
        'cmd_portal_lcia_projection_finalize_publication_v1',
        'qry_portal_lcia_projection_publication_readback_v1',
        'cmd_portal_lcia_projection_revoke_publication_v1',
        'portal_get_published_lcia_values_v1'
      )
    ) or (
      namespace.nspname = 'private'
      and routine.proname in (
        'svc_portal_lcia_projection_stage_begin_v1',
        'svc_portal_lcia_projection_stage_register_batch_v1',
        'svc_portal_lcia_projection_stage_status_v1',
        'svc_portal_lcia_projection_stage_seal_v1',
        'svc_portal_lcia_projection_stage_fail_v1',
        'svc_portal_lcia_projection_worker_input_v1',
        'svc_portal_lcia_projection_package_mark_ready_v1'
      )
    )
  ),
  15::bigint,
  'no overload broadens any frozen Portal LCIA routine name'
);

select extensions.is(
  (
    select count(*)
    from portal_lcia_legacy_routines as expected
    where pg_catalog.to_regprocedure(expected.routine_identity) is not null
  ),
  14::bigint,
  'all fourteen frozen legacy Data Product and Release routine identities remain present'
);

select extensions.is(
  (
    select count(*)
    from pg_catalog.pg_proc as routine
    where routine.oid in (
      'api.cmd_lcia_result_build_request(text,jsonb,text,text,jsonb,text,jsonb)'::regprocedure,
      'api.cmd_lcia_result_build_request_v2(text,jsonb,text,text,jsonb,text,uuid,text,text,jsonb)'::regprocedure
    )
      and routine.prosrc !~ 'portal_lcia_projection|portalProjectionContractVersion'
  ),
  2::bigint,
  'V1 and V2 producers remain byte-isolated from the V3 Portal projection contract'
);

select extensions.ok(
  (
    select routine.prosrc ~ 'portalProjectionContractVersion'
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'api.cmd_lcia_result_build_request_v3(text,jsonb,text,text,jsonb,text,uuid,text,text,jsonb)'::regprocedure
  ),
  'only the additive V3 producer writes the Portal projection contract marker'
);

create temporary table portal_lcia_expected_acl (
  routine_identity text not null,
  grantee text not null,
  privilege_type text not null default 'EXECUTE',
  is_grantable boolean not null default false,
  primary key (routine_identity, grantee, privilege_type)
) on commit drop;

insert into portal_lcia_expected_acl (routine_identity, grantee)
select expected.routine_identity, grant_row.grantee
from portal_lcia_expected_routines as expected
cross join lateral (
  select case
    when expected.boundary = 'public' then 'portal_public_executor'
    else 'postgres'
  end::text as grantee
  union all
  select 'authenticated'
  where expected.boundary in ('producer', 'actor', 'public')
  union all
  select 'anon'
  where expected.boundary = 'public'
  union all
  select 'service_role'
  where expected.boundary = 'service'
) as grant_row;

select extensions.is(
  (
    with actual as (
      select
        expected.routine_identity,
        coalesce(grantee_role.rolname, 'PUBLIC') as grantee,
        acl.privilege_type,
        acl.is_grantable
      from portal_lcia_expected_routines as expected
      join pg_catalog.pg_proc as routine
        on routine.oid = pg_catalog.to_regprocedure(expected.routine_identity)
      cross join lateral pg_catalog.aclexplode(
        coalesce(routine.proacl, pg_catalog.acldefault('f', routine.proowner))
      ) as acl
      left join pg_catalog.pg_roles as grantee_role
        on grantee_role.oid = acl.grantee
    )
    select count(*)
    from (
      (select * from actual except select * from portal_lcia_expected_acl)
      union all
      (select * from portal_lcia_expected_acl except select * from actual)
    ) as symmetric_difference
  ),
  0::bigint,
  'Portal LCIA ACLs contain only owner plus the exact actor, worker, or browser EXECUTE grants'
);

select extensions.is(
  (
    select count(*)
    from private.api_capability_grants as manifest
    join portal_lcia_expected_routines as expected
      on expected.routine_identity = manifest.routine_identity
    where manifest.capability_id = 'PORTAL-LCIA-ADMIN-01'
      and expected.boundary = 'actor'
      and not manifest.allow_anon
      and manifest.allow_authenticated
      and not manifest.allow_service_role
  ),
  6::bigint,
  'PORTAL-LCIA-ADMIN-01 contains the six authenticated manager routines only'
);

select extensions.is(
  (
    select count(*)
    from private.api_capability_grants as manifest
    where manifest.capability_id = 'PORTAL-LCIA-ADMIN-01'
  ),
  6::bigint,
  'the Portal LCIA admin manifest has no extra signature'
);

select extensions.is(
  (
    select count(*)
    from private.api_capability_grants as manifest
    where manifest.capability_id = 'EDGE-ACTOR-01'
      and manifest.routine_identity =
        'api.cmd_lcia_result_build_request_v3(text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb)'
      and not manifest.allow_anon
      and manifest.allow_authenticated
      and not manifest.allow_service_role
  ),
  1::bigint,
  'the V3 producer is registered under the existing authenticated EDGE-ACTOR-01 capability'
);

create temporary table portal_lcia_expected_relations (
  relation_name name primary key
) on commit drop;

insert into portal_lcia_expected_relations (relation_name)
values
  ('portal_lcia_projection_headers'),
  ('portal_lcia_projection_process_axis'),
  ('portal_lcia_projection_impact_axis'),
  ('portal_lcia_projection_values'),
  ('portal_lcia_projection_publications');

select extensions.is(
  (
    select count(*)
    from pg_catalog.pg_class as relation
    join pg_catalog.pg_namespace as namespace
      on namespace.oid = relation.relnamespace
    join portal_lcia_expected_relations as expected
      on expected.relation_name = relation.relname
    where namespace.nspname = 'private'
      and relation.relkind = 'r'
      and relation.relowner = 'postgres'::regrole
      and relation.relrowsecurity
  ),
  5::bigint,
  'the five postgres-owned private projection relations exist with RLS enabled'
);

select extensions.is(
  (
    select count(*)
    from information_schema.table_privileges as privilege
    join portal_lcia_expected_relations as expected
      on expected.relation_name::text = privilege.table_name
    where privilege.table_schema = 'private'
      and privilege.grantee in ('PUBLIC', 'anon', 'authenticated', 'service_role')
  ),
  0::bigint,
  'PUBLIC, browser roles, and service_role have no raw projection table privilege'
);

select extensions.is(
  (
    select count(*)
    from pg_catalog.pg_policies as policy
    join portal_lcia_expected_relations as expected
      on expected.relation_name::text = policy.tablename
    where policy.schemaname = 'private'
      and (
        'public' = any (policy.roles)
        or 'anon' = any (policy.roles)
        or 'authenticated' = any (policy.roles)
        or 'service_role' = any (policy.roles)
      )
  ),
  0::bigint,
  'projection RLS has no direct role policy that bypasses the reviewed routines'
);

select extensions.ok(
  exists (
    select 1
    from pg_catalog.pg_constraint
    where conrelid = 'private.portal_lcia_projection_headers'::regclass
      and conname = 'portal_lcia_projection_headers_status_chk'
      and pg_catalog.pg_get_constraintdef(oid) like '%staging%prepared%failed%'
  )
  and exists (
    select 1
    from pg_catalog.pg_constraint
    where conrelid = 'private.portal_lcia_projection_publications'::regclass
      and conname = 'portal_lcia_projection_publications_status_chk'
      and pg_catalog.pg_get_constraintdef(oid) like '%finalized%revoked%'
  ),
  'header and publication state machines are closed to their frozen status sets'
);

select extensions.ok(
  exists (
    select 1
    from pg_catalog.pg_trigger
    where tgrelid = 'private.portal_lcia_projection_headers'::regclass
      and tgname = 'portal_lcia_projection_header_guard_v1'
      and tgenabled <> 'D'
  )
  and exists (
    select 1
    from pg_catalog.pg_trigger
    where tgrelid = 'private.portal_lcia_projection_process_axis'::regclass
      and tgname = 'portal_lcia_projection_process_row_guard_v1'
      and tgenabled <> 'D'
  )
  and exists (
    select 1
    from pg_catalog.pg_trigger
    where tgrelid = 'private.portal_lcia_projection_impact_axis'::regclass
      and tgname = 'portal_lcia_projection_impact_row_guard_v1'
      and tgenabled <> 'D'
  )
  and exists (
    select 1
    from pg_catalog.pg_trigger
    where tgrelid = 'private.portal_lcia_projection_values'::regclass
      and tgname = 'portal_lcia_projection_value_row_guard_v1'
      and tgenabled <> 'D'
  )
  and exists (
    select 1
    from pg_catalog.pg_trigger
    where tgrelid = 'private.portal_lcia_projection_publications'::regclass
      and tgname = 'portal_lcia_projection_publication_guard_v1'
      and tgenabled <> 'D'
  ),
  'all projection rows and terminal bindings are protected by enabled immutability guards'
);

select extensions.is(
  private.portal_lcia_projection_sha256_fields_v1('A', 'é', null, ''),
  '5a01047a86055adc7954e7411667d0ef91c64f0c9ff4550dce738aa4d2f4a6ea',
  'int32be framing uses UTF-8 byte lengths and distinguishes NULL from empty text across languages'
);

select extensions.is(
  (
    select count(*)
    from private.api_capability_grants as manifest
    where manifest.capability_id = 'PORTAL-LCIA-01'
      and manifest.routine_identity =
        'api.portal_get_published_lcia_values_v1(text, jsonb, text, text, integer)'
      and manifest.allow_anon
      and manifest.allow_authenticated
      and not manifest.allow_service_role
  ),
  1::bigint,
  'the capability manifest contains exactly the frozen anonymous Portal LCIA RPC grant'
);

select extensions.ok(
  pg_catalog.has_function_privilege(
    'anon',
    'api.portal_get_published_lcia_values_v1(text,jsonb,text,text,integer)',
    'EXECUTE'
  )
  and pg_catalog.has_function_privilege(
    'authenticated',
    'api.portal_get_published_lcia_values_v1(text,jsonb,text,text,integer)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'service_role',
    'api.portal_get_published_lcia_values_v1(text,jsonb,text,text,integer)',
    'EXECUTE'
  ),
  'the public reader is executable by browser roles but not service_role'
);

select extensions.is(
  (
    select count(*)
    from pg_catalog.pg_policies
    where schemaname = 'private'
      and tablename like 'portal_lcia_projection%'
      and ('anon' = any (roles) or 'authenticated' = any (roles) or 'public' = any (roles))
  ),
  0::bigint,
  'no raw Portal LCIA projection relation gains an anon, authenticated, or PUBLIC policy'
);

select extensions.ok(
  not pg_catalog.has_table_privilege('anon', 'private.lcia_result_packages', 'SELECT')
  and not pg_catalog.has_table_privilege('authenticated', 'private.lcia_result_packages', 'SELECT')
  and not pg_catalog.has_table_privilege('anon', 'private.lcia_result_publications', 'SELECT')
  and not pg_catalog.has_table_privilege('authenticated', 'private.lcia_result_publications', 'SELECT'),
  'legacy package and publication relations remain inaccessible directly to browser roles'
);

-- Stable identities keep the fixture readable while every row remains inside
-- this test transaction.  Values intentionally cover open, state-200, and
-- non-public Process records plus two exact Method/Impact identities.
create temporary table portal_lcia_ids (
  label text primary key,
  id uuid not null
) on commit drop;

insert into portal_lcia_ids (label, id)
values
  ('manager', '52710000-0000-4000-8000-000000000001'),
  ('ordinary_user', '52710000-0000-4000-8000-000000000002'),
  ('process_open_a', '52710000-0000-4000-8000-000000000101'),
  ('process_open_b', '52710000-0000-4000-8000-000000000102'),
  ('process_state_200', '52710000-0000-4000-8000-000000000103'),
  ('process_draft', '52710000-0000-4000-8000-000000000104'),
  ('method_a', '52710000-0000-4000-8000-000000000201'),
  ('method_b', '52710000-0000-4000-8000-000000000202'),
  ('impact_a', '52710000-0000-4000-8000-000000000301'),
  ('impact_b', '52710000-0000-4000-8000-000000000302'),
  ('build_v3', '52710000-0000-4000-8000-000000000401'),
  ('worker_job_v3', '52710000-0000-4000-8000-000000000402'),
  ('snapshot', '52710000-0000-4000-8000-000000000403'),
  ('result', '52710000-0000-4000-8000-000000000404'),
  ('latest_all_unit', '52710000-0000-4000-8000-000000000405'),
  ('package', '52710000-0000-4000-8000-000000000406'),
  ('publication', '52710000-0000-4000-8000-000000000407'),
  ('stage', '52710000-0000-4000-8000-000000000408'),
  ('closure_job', '52710000-0000-4000-8000-000000000410'),
  ('closure_check', '52710000-0000-4000-8000-000000000411'),
  ('closure_report', '52710000-0000-4000-8000-000000000412'),
  ('closure_machine_result', '52710000-0000-4000-8000-000000000413'),
  ('closure_bundle', '52710000-0000-4000-8000-000000000414'),
  ('snapshot_artifact', '52710000-0000-4000-8000-000000000415'),
  ('batch_job', '52710000-0000-4000-8000-000000000421'),
  ('batch_projection', '52710000-0000-4000-8000-000000000423'),
  ('failed_job', '52710000-0000-4000-8000-000000000424'),
  ('failed_projection', '52710000-0000-4000-8000-000000000426'),
  ('legacy_package', '52710000-0000-4000-8000-000000000427'),
  ('legacy_publication', '52710000-0000-4000-8000-000000000428'),
  ('v3_result', '52710000-0000-4000-8000-000000000430'),
  ('v3_latest_all_unit', '52710000-0000-4000-8000-000000000405'),
  ('legacy_v1_job', '52710000-0000-4000-8000-000000000432');

insert into auth.users (
  instance_id, id, aud, role, email, encrypted_password, email_confirmed_at,
  raw_app_meta_data, raw_user_meta_data, created_at, updated_at,
  is_sso_user, is_anonymous
)
values
  (
    '00000000-0000-0000-0000-000000000000',
    '52710000-0000-4000-8000-000000000001',
    'authenticated', 'authenticated', 'portal-lcia-manager@example.com',
    'test-password-hash', pg_catalog.now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"52710000-0000-4000-8000-000000000001","email":"portal-lcia-manager@example.com"}'::jsonb,
    pg_catalog.now(), pg_catalog.now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '52710000-0000-4000-8000-000000000002',
    'authenticated', 'authenticated', 'portal-lcia-reader@example.com',
    'test-password-hash', pg_catalog.now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"52710000-0000-4000-8000-000000000002","email":"portal-lcia-reader@example.com"}'::jsonb,
    pg_catalog.now(), pg_catalog.now(), false, false
  );

insert into private.users (id, raw_user_meta_data, contact)
values
  ('52710000-0000-4000-8000-000000000001', '{"email":"portal-lcia-manager@example.com"}'::jsonb, null),
  ('52710000-0000-4000-8000-000000000002', '{"email":"portal-lcia-reader@example.com"}'::jsonb, null);

insert into private.teams (id, json, rank, is_public)
values (
  '00000000-0000-0000-0000-000000000000',
  '{"name":"System Team"}'::jsonb,
  0,
  false
)
on conflict (id) do nothing;

insert into private.roles (user_id, team_id, role)
values (
  '52710000-0000-4000-8000-000000000001',
  '00000000-0000-0000-0000-000000000000',
  'data_product_manager'
);

-- The row payloads use the same exact-version public Process source as the
-- Portal catalogue tests.  Open data has explicit full-free license evidence;
-- state 200 and draft fixtures prove that a caller cannot bypass visibility by
-- naming an exact publication.
alter table public.processes disable trigger user;

insert into public.processes (id, version, json, user_id, state_code)
values
  (
    '52710000-0000-4000-8000-000000000101', '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Open process A"}]},"classificationInformation":{"common:classification":[{"common:class":{"@level":"0","@classId":"CPC-A","#text":"A"}}]}},"geography":{"locationOfOperationSupplyOrProduction":{"@location":"CN","@latitudeAndLongitude":"35.0;105.0"}},"time":{"common:referenceYear":"2025"},"quantitativeReference":{"referenceToReferenceFlow":"1","functionalUnitOrOther":[{"@xml:lang":"en","#text":"one kilogram"}]}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000","common:licenseType":"Free of charge for all users and uses"}}}}'::jsonb,
    '52710000-0000-4000-8000-000000000001', 100
  ),
  (
    '52710000-0000-4000-8000-000000000102', '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Open process B"}]}},"geography":{"locationOfOperationSupplyOrProduction":{"@location":"GLO"}},"time":{"common:referenceYear":"2024"},"quantitativeReference":{"referenceToReferenceFlow":"1","functionalUnitOrOther":[{"@xml:lang":"en","#text":"one kilogram"}]}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000","common:licenseType":"Free of charge for all users and uses"}}}}'::jsonb,
    '52710000-0000-4000-8000-000000000001', 100
  ),
  (
    '52710000-0000-4000-8000-000000000104', '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Draft process"}]}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000","common:licenseType":"Free of charge for all users and uses"}}}}}'::jsonb,
    '52710000-0000-4000-8000-000000000001', 20
  );

alter table public.processes enable trigger user;

-- Build one valid, frozen certificate directly from the existing authoritative
-- relations.  The fixture deliberately uses ready retained artifacts and a
-- database-owned numerical snapshot; no private locator is ever copied into a
-- Portal projection row.
insert into private.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, subject_type, subject_id,
  requester_type, requested_by, idempotency_key, request_hash, visibility,
  payload_schema_version, payload_json
)
values (
  '52710000-0000-4000-8000-000000000410',
  'lcia.scope_closure_check', 'calculator', 'solver',
  'lcia_scope_closure_check', '52710000-0000-4000-8000-000000000411',
  'operator', '52710000-0000-4000-8000-000000000001',
  'portal-lcia-closure-check', repeat('0', 64), 'operator',
  'lcia.scope_closure_check.request.v1', '{}'::jsonb
);

insert into private.worker_job_artifacts (
  id, job_id, artifact_type, storage_bucket, storage_path, content_type,
  byte_size, checksum_sha256, metadata
)
values
  (
    '52710000-0000-4000-8000-000000000412',
    '52710000-0000-4000-8000-000000000410',
    'closure_report_xlsx', 'portal-lcia-test', 'closure/report.xlsx',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    101, repeat('5', 64), '{}'::jsonb
  ),
  (
    '52710000-0000-4000-8000-000000000413',
    '52710000-0000-4000-8000-000000000410',
    'closure_complete_machine_result', 'portal-lcia-test', 'closure/result.json',
    'application/vnd.tiangong.scope-closure-manifest+json',
    102, repeat('8', 64),
    '{"closureCheckId":"52710000-0000-4000-8000-000000000411"}'::jsonb
  ),
  (
    '52710000-0000-4000-8000-000000000414',
    '52710000-0000-4000-8000-000000000410',
    'closure_bundle', 'portal-lcia-test', 'closure/bundle.json',
    'application/json', 103, repeat('6', 64),
    '{"closureCheckId":"52710000-0000-4000-8000-000000000411","completeMachineResultArtifactId":"52710000-0000-4000-8000-000000000413"}'::jsonb
  );

insert into private.lca_network_snapshots (
  id, scope, process_filter, source_hash, status, created_by,
  provider_matching_rule
)
values (
  '52710000-0000-4000-8000-000000000403',
  'data_product', '{"portal":"lcia-v1"}'::jsonb,
  'portal-lcia-builder-source-v1', 'ready',
  '52710000-0000-4000-8000-000000000001',
  'split_by_process_volume'
);

insert into private.lca_snapshot_artifacts (
  id, snapshot_id, artifact_url, artifact_sha256, artifact_byte_size,
  artifact_format, process_count, flow_count, impact_count,
  a_nnz, b_nnz, c_nnz, status, snapshot_index_sha256,
  snapshot_build_contract_hash, effective_scope_hash,
  data_snapshot_token, closure_bundle_hash
)
values (
  '52710000-0000-4000-8000-000000000415',
  '52710000-0000-4000-8000-000000000403',
  's3://portal-lcia-test/private/snapshot.h5', repeat('7', 64), 104,
  'snapshot-hdf5:v1', 2, 2, 2, 2, 2, 4, 'ready', repeat('9', 64),
  repeat('a', 64), repeat('3', 64), 'portal-lcia-snapshot-v1', repeat('6', 64)
);

insert into private.lcia_scope_closure_checks (
  id, worker_job_id, requested_by, request_idempotency_token, request_key,
  request_fingerprint, requested_scope_hash, effective_scope_hash,
  policy_fingerprint, data_snapshot_token,
  expected_validator_scanner_fingerprint, status, scan_completeness,
  certificate_status, certificate_hash, report_artifact_id, result_summary,
  blocker_codes, finished_at, requested_scope_manifest,
  effective_scope_manifest, certificate_schema_version, source_fingerprint,
  resolution_map_hash, closure_bundle_hash, snapshot_id, snapshot_hash,
  report_artifact_manifest_hash, evidence_hash, closure_bundle_artifact_id,
  snapshot_artifact_id, snapshot_index_sha256,
  snapshot_build_contract_hash, complete_machine_result_artifact_id,
  valid_until
)
select
  '52710000-0000-4000-8000-000000000411',
  '52710000-0000-4000-8000-000000000410',
  '52710000-0000-4000-8000-000000000001',
  'portal-lcia-closure-check', 'portal-lcia-closure-key', repeat('f', 64),
  repeat('1', 64), repeat('3', 64), repeat('2', 64),
  'portal-lcia-snapshot-v1',
  (select expected_validator_scanner_fingerprint
   from private.lcia_scope_closure_config where singleton),
  'passed', 'complete', 'valid', repeat('4', 64),
  '52710000-0000-4000-8000-000000000412', '{}'::jsonb, '{}'::text[],
  pg_catalog.clock_timestamp(),
  '{"certificateFreshnessPolicy":"frozen-artifact-reusable-v1"}'::jsonb,
  jsonb_build_object(
    'coverageMode', 'global_eligible',
    'eligibilityPredicateVersion', 'portal-lcia-test.v1',
    'processes', jsonb_build_array(
      jsonb_build_object(
        'id', '52710000-0000-4000-8000-000000000101',
        'version', '01.00.000'
      ),
      jsonb_build_object(
        'id', '52710000-0000-4000-8000-000000000102',
        'version', '01.00.000'
      )
    ),
    'lciaMethods', jsonb_build_array(
      jsonb_build_object(
        'id', '52710000-0000-4000-8000-000000000201',
        'version', '01.00.000'
      ),
      jsonb_build_object(
        'id', '52710000-0000-4000-8000-000000000202',
        'version', '01.00.000'
      )
    )
  ),
  'lcia.scope-closure-certificate.v2', 'portal-lcia-source-v1',
  repeat('5', 64), repeat('6', 64),
  '52710000-0000-4000-8000-000000000403', repeat('7', 64),
  private.lcia_scope_closure_sha256(jsonb_build_object(
    'artifactId', '52710000-0000-4000-8000-000000000412'::uuid,
    'bucket', 'portal-lcia-test',
    'objectPath', 'closure/report.xlsx',
    'mediaType',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'byteSize', 101,
    'checksumSha256', repeat('5', 64)
  )),
  repeat('8', 64),
  '52710000-0000-4000-8000-000000000414',
  '52710000-0000-4000-8000-000000000415', repeat('9', 64),
  repeat('a', 64), '52710000-0000-4000-8000-000000000413',
  pg_catalog.clock_timestamp() + interval '1 day';

-- V1 and V2 remain valid producer APIs, but neither may opt a job into the
-- Portal projection.  Snapshot their exact generated jobs before exercising
-- V3 so response loss/retry logic cannot mutate historical payloads.
select pg_catalog.set_config(
  'request.jwt.claim.sub', '52710000-0000-4000-8000-000000000001', true
);
select pg_catalog.set_config('request.jwt.claim.role', 'authenticated', true);
select pg_catalog.set_config(
  'request.jwt.claims',
  '{"role":"authenticated","sub":"52710000-0000-4000-8000-000000000001"}',
  true
);

create temporary table portal_lcia_build_responses (
  producer text primary key,
  response jsonb not null
) on commit drop;

insert into portal_lcia_build_responses (producer, response)
values
  (
    'v1',
    api.cmd_lcia_result_build_request(
      'Portal LCIA legacy V1', null, 'global_eligible', null, '[]'::jsonb,
      'portal-lcia-build-v1', '{}'::jsonb
    )
  ),
  (
    'v2',
    api.cmd_lcia_result_build_request_v2(
      'Portal LCIA legacy V2', null, 'global_eligible', null, '[]'::jsonb,
      'portal-lcia-build-v2',
      '52710000-0000-4000-8000-000000000411',
      repeat('1', 64), repeat('2', 64), '{}'::jsonb
    )
  );

select extensions.ok(
  (select (response->>'ok')::boolean from portal_lcia_build_responses where producer = 'v1')
  and (select (response->>'ok')::boolean from portal_lcia_build_responses where producer = 'v2'),
  'legacy V1 and V2 producers still accept their established valid requests'
);

insert into private.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, subject_type, subject_id,
  requester_type, requested_by, idempotency_key, request_hash, visibility,
  payload_schema_version, payload_json
)
select
  '52710000-0000-4000-8000-000000000432',
  response #>> '{data,workerJob,jobKind}',
  'calculator', 'solver',
  response #>> '{data,workerJob,subjectType}',
  (response #>> '{data,workerJob,subjectId}')::uuid,
  response #>> '{data,workerJob,requesterType}',
  (response #>> '{data,workerJob,requestedBy}')::uuid,
  response #>> '{data,workerJob,idempotencyKey}',
  response #>> '{data,workerJob,requestHash}',
  response #>> '{data,workerJob,visibility}',
  response #>> '{data,workerJob,payloadSchemaVersion}',
  response #> '{data,workerJob,payload}'
from portal_lcia_build_responses
where producer = 'v1';

create temporary table portal_lcia_legacy_jobs_before (
  id uuid primary key,
  payload_schema_version text not null,
  payload_json jsonb not null,
  payload_ref jsonb
) on commit drop;

insert into portal_lcia_legacy_jobs_before (
  id, payload_schema_version, payload_json, payload_ref
)
select job.id, job.payload_schema_version, job.payload_json, job.payload_ref
from private.worker_jobs as job
where job.id in (
  select id from portal_lcia_ids where label = 'legacy_v1_job'
  union all
  select (response->'data'->>'workerJobId')::uuid
  from portal_lcia_build_responses where producer = 'v2'
);

select extensions.is(
  (
    select count(*)
    from portal_lcia_legacy_jobs_before
    where not (payload_json ? 'portalProjectionContractVersion')
      and payload_json::text !~ 'portal_lcia_projection'
  ),
  2::bigint,
  'V1 and V2 jobs contain no Portal projection opt-in marker'
);

insert into portal_lcia_build_responses (producer, response)
values (
  'v3',
  api.cmd_lcia_result_build_request_v3(
    'Portal LCIA V3', null, 'global_eligible', null, '[]'::jsonb,
    'portal-lcia-build-v3',
    '52710000-0000-4000-8000-000000000411',
    repeat('1', 64), repeat('2', 64), '{}'::jsonb
  )
);

update portal_lcia_ids
set id = (
  select (response->'data'->>'buildId')::uuid
  from portal_lcia_build_responses where producer = 'v3'
)
where label = 'build_v3';

update portal_lcia_ids
set id = (
  select (response->'data'->>'workerJobId')::uuid
  from portal_lcia_build_responses where producer = 'v3'
)
where label = 'worker_job_v3';

select extensions.ok(
  (select (response->>'ok')::boolean
   from portal_lcia_build_responses where producer = 'v3'),
  'the additive V3 producer creates a certificate-bound build'
);

select extensions.ok(
  (
    select job.payload_json->>'portalProjectionContractVersion' =
             'portal.lcia-projection.v1'
      and job.payload_schema_version = 'lcia_result.package_build.request.v3'
    from private.worker_jobs as job
    where job.id = (select id from portal_lcia_ids where label = 'worker_job_v3')
  ),
  'the V3 job is the only producer branch carrying the frozen projection marker'
);

select extensions.is(
  (
    select count(*)
    from private.worker_jobs
    where requested_by = '52710000-0000-4000-8000-000000000001'
      and payload_json->>'portalProjectionIdempotencyKey' =
            'portal-lcia-v3:portal-lcia-build-v3'
      and payload_json->>'portalProjectionContractVersion' =
            'portal.lcia-projection.v1'
  ),
  1::bigint,
  'one V3 request creates exactly one V3 worker job'
);

select extensions.is(
  api.cmd_lcia_result_build_request_v3(
    'Portal LCIA V3', null, 'global_eligible', null, '[]'::jsonb,
    'portal-lcia-build-v3',
    '52710000-0000-4000-8000-000000000411',
    repeat('1', 64), repeat('2', 64), '{}'::jsonb
  ) ->'data'->>'workerJobId',
  (select id::text from portal_lcia_ids where label = 'worker_job_v3'),
  'an exact V3 retry returns the same worker job after response loss'
);

select extensions.is(
  (
    select count(*)
    from portal_lcia_legacy_jobs_before as before_state
    join private.worker_jobs as after_state using (id)
    where before_state.payload_schema_version = after_state.payload_schema_version
      and before_state.payload_json = after_state.payload_json
      and before_state.payload_ref is not distinct from after_state.payload_ref
  ),
  2::bigint,
  'creating and retrying V3 leaves pre-existing V1/V2 jobs byte-stable'
);

select extensions.is(
  (select count(*) from private.portal_lcia_projection_headers),
  0::bigint,
  'V1, V2, and V3 build creation alone materialize zero projection rows'
);

reset role;

update private.worker_jobs
set status = 'running',
    lease_token = '52710000-0000-4000-8000-000000000420',
    lease_expires_at = pg_catalog.clock_timestamp() + interval '10 minutes',
    started_at = coalesce(started_at, pg_catalog.clock_timestamp())
where id = (select id from portal_lcia_ids where label = 'worker_job_v3');

insert into private.lca_results (
  id, job_id, snapshot_id, payload, diagnostics, artifact_url,
  artifact_sha256, artifact_byte_size, artifact_format, worker_job_id,
  is_pinned
)
values (
  '52710000-0000-4000-8000-000000000404',
  (select (response->'data'->>'workerJobId')::uuid
   from portal_lcia_build_responses where producer = 'v2'),
  '52710000-0000-4000-8000-000000000403', '{}'::jsonb, '{}'::jsonb,
  's3://portal-lcia-test/private/result.json', repeat('b', 64), 201,
  'application/json',
  (select (response->'data'->>'workerJobId')::uuid
   from portal_lcia_build_responses where producer = 'v2'), false
);

insert into private.lca_latest_all_unit_results (
  id, snapshot_id, job_id, result_id, query_artifact_url,
  query_artifact_sha256, query_artifact_byte_size, query_artifact_format,
  status, worker_job_id
)
values (
  '52710000-0000-4000-8000-000000000405',
  '52710000-0000-4000-8000-000000000403',
  (select (response->'data'->>'workerJobId')::uuid
   from portal_lcia_build_responses where producer = 'v2'),
  '52710000-0000-4000-8000-000000000404',
  's3://portal-lcia-test/private/query.json', repeat('c', 64), 202,
  'application/json', 'ready',
  (select (response->'data'->>'workerJobId')::uuid
   from portal_lcia_build_responses where producer = 'v2')
);

update private.worker_jobs
set status = 'running',
    lease_token = '52710000-0000-4000-8000-000000000419',
    lease_expires_at = pg_catalog.clock_timestamp() + interval '10 minutes',
    started_at = coalesce(started_at, pg_catalog.clock_timestamp())
where id = (
  select (response->'data'->>'workerJobId')::uuid
  from portal_lcia_build_responses where producer = 'v2'
);

select pg_catalog.set_config('request.jwt.claim.role', 'service_role', true);
select pg_catalog.set_config(
  'request.jwt.claims', '{"role":"service_role"}', true
);

create temporary table portal_lcia_package_response (response jsonb not null)
on commit drop;

insert into portal_lcia_package_response (response)
values (
  private.cmd_lcia_result_package_mark_ready(
    p_build_worker_job_id =>
      (select (response->'data'->>'workerJobId')::uuid
       from portal_lcia_build_responses where producer = 'v2'),
    p_package_version => 'portal-lcia-legacy-package-v2',
    p_snapshot_id => '52710000-0000-4000-8000-000000000403',
    p_result_id => '52710000-0000-4000-8000-000000000404',
    p_latest_all_unit_result_id =>
      '52710000-0000-4000-8000-000000000405',
    p_available_impact_categories => jsonb_build_array(
      '52710000-0000-4000-8000-000000000301',
      '52710000-0000-4000-8000-000000000302'
    ),
    p_artifact_manifest => jsonb_build_object(
      'schemaVersion', 'portal-lcia-test-artifacts.v1',
      'resultSha256', repeat('b', 64),
      'querySha256', repeat('c', 64)
    ),
    p_audit => '{}'::jsonb
  )
);

update portal_lcia_ids
set id = (
  select (response->'data'->>'packageId')::uuid
  from portal_lcia_package_response
)
where label = 'legacy_package';

select extensions.ok(
  (select (response->>'ok')::boolean from portal_lcia_package_response)
  and exists (
    select 1
    from private.lcia_result_packages
    where id = (select id from portal_lcia_ids where label = 'legacy_package')
      and status = 'preview_ready'
      and build_worker_job_id = (
        select (response->'data'->>'workerJobId')::uuid
        from portal_lcia_build_responses where producer = 'v2'
      )
  ),
  'the existing Worker package-ready boundary creates an authoritative legacy package fixture'
);

reset role;
select pg_catalog.set_config(
  'request.jwt.claim.sub', '52710000-0000-4000-8000-000000000001', true
);
select pg_catalog.set_config('request.jwt.claim.role', 'authenticated', true);
select pg_catalog.set_config(
  'request.jwt.claims',
  '{"role":"authenticated","sub":"52710000-0000-4000-8000-000000000001"}',
  true
);

insert into private.lcia_result_publications (
  id, package_id, publication_series_key, publication_channel,
  visibility_scope, is_current, status,
  display_default_impact_category, published_by, published_at, reason
)
values (
  (select id from portal_lcia_ids where label = 'legacy_publication'),
  (select id from portal_lcia_ids where label = 'legacy_package'),
  'global', 'public', 'public', true, 'current',
  '52710000-0000-4000-8000-000000000301',
  '52710000-0000-4000-8000-000000000001',
  pg_catalog.clock_timestamp(),
  'pre-existing publication fixture'
);

select extensions.ok(
  exists (
    select 1
    from private.lcia_result_publications
    where id = (select id from portal_lcia_ids where label = 'legacy_publication')
      and package_id = (select id from portal_lcia_ids where label = 'legacy_package')
      and status = 'current'
      and is_current
  ),
  'a pre-existing authoritative current LCIA result publication fixture is available for supersession tests'
);

reset role;

-- A missing public projection is not a numerical zero.  Exercise all three
-- public modes before any stage has been finalized: SQL NULL is the frozen
-- unavailable signal and cannot be confused with an explicit decimal zero.
set local role anon;
select pg_catalog.set_config('request.jwt.claim.role', 'anon', true);
select pg_catalog.set_config('request.jwt.claims', '{"role":"anon"}', true);

select extensions.is(
  api.portal_get_published_lcia_values_v1(
    'process_all_impacts',
    '[{"id":"52710000-0000-4000-8000-000000000101","version":"01.00.000"}]'::jsonb,
    null,
    null,
    50
  ),
  null::jsonb,
  'process_all_impacts returns SQL NULL with no synthesized row'
);

select extensions.is(
  api.portal_get_published_lcia_values_v1(
    'processes_one_impact',
    '[{"id":"52710000-0000-4000-8000-000000000101","version":"01.00.000"}]'::jsonb,
    '52710000-0000-4000-8000-000000000301',
    null,
    50
  ),
  null::jsonb,
  'processes_one_impact returns SQL NULL instead of synthesizing a zero'
);

select extensions.is(
  api.portal_get_published_lcia_values_v1(
    'ranked_processes_one_impact',
    '[{"id":"52710000-0000-4000-8000-000000000101","version":"01.00.000"}]'::jsonb,
    '52710000-0000-4000-8000-000000000301',
    null,
    50
  ),
  null::jsonb,
  'ranked_processes_one_impact returns SQL NULL instead of synthesizing a zero'
);

reset role;

create or replace function pg_temp.portal_lcia_process_record(
  p_process_index integer,
  p_process_id uuid,
  p_amount text default '1'
)
returns jsonb
language sql
immutable
set search_path = ''
as $function$
  select jsonb_build_object(
    'processIndex', p_process_index,
    'processId', p_process_id,
    'processVersion', '01.00.000',
    'referenceFlowId', '52710000-0000-4000-8000-000000000501',
    'referenceFlowVersion', '01.00.000',
    'referenceExchangeInternalId', '1',
    'referenceFlowAmount', '1',
    'referenceFlowDirection', 'output',
    'functionalUnitAmount', p_amount,
    'functionalUnitUnit', 'kg',
    'functionalUnitDescription',
      jsonb_build_array(jsonb_build_object(
        'language', 'en', 'value', 'one kilogram'
      )),
    'geographyCode', case when p_process_index = 0 then 'CN' else 'GLO' end,
    'geographyPrecision', case when p_process_index = 0 then 'country' else 'other' end,
    'referenceYear', case when p_process_index = 0 then 2025 else 2024 end,
    'processDocumentSha256', repeat('d', 64)
  )
$function$;

create or replace function pg_temp.portal_lcia_impact_record(
  p_impact_index integer,
  p_impact_id text
)
returns jsonb
language sql
immutable
set search_path = ''
as $function$
  select jsonb_build_object(
    'impactIndex', p_impact_index,
    'methodId', case
      when p_impact_index = 0 then '52710000-0000-4000-8000-000000000201'
      when p_impact_index = 1 then '52710000-0000-4000-8000-000000000202'
      else '52710000-0000-4000-8000-' ||
        lpad((p_impact_index + 1000)::text, 12, '0')
    end,
    'methodVersion', '01.00.000',
    'impactId', p_impact_id,
    'impactName', jsonb_build_array(jsonb_build_object(
      'language', 'en', 'value', 'Impact ' || p_impact_index::text
    )),
    'unit', case when p_impact_index = 0 then 'kg CO2-eq' else 'mol H+-eq' end,
    'methodDocumentSha256', repeat('e', 64)
  )
$function$;

create or replace function pg_temp.portal_lcia_value_record(
  p_process_index integer,
  p_impact_index integer,
  p_impact_count integer,
  p_value text
)
returns jsonb
language sql
immutable
set search_path = ''
as $function$
  select jsonb_build_object(
    'ordinal', p_process_index::bigint * p_impact_count::bigint
      + p_impact_index::bigint + 1,
    'processIndex', p_process_index,
    'impactIndex', p_impact_index,
    'value', p_value
  )
$function$;

create or replace function pg_temp.portal_lcia_source(
  p_suffix text default 'd'
)
returns jsonb
language sql
immutable
set search_path = ''
as $function$
  select jsonb_build_object(
    'schemaVersion', 'portal.lcia-projection.source.v1',
    'bundleContentHash', repeat(substr(p_suffix, 1, 1), 64),
    'bundleManifestSha256', repeat('e', 64),
    'lciaChunkSetSha256', repeat('f', 64),
    'resultArtifactSha256', repeat('b', 64),
    'queryArtifactSha256', repeat('c', 64)
  )
$function$;

create temporary table portal_lcia_stage_clock (
  label text primary key,
  observed_at timestamptz not null
) on commit drop;

insert into portal_lcia_stage_clock values ('before_begin', clock_timestamp());

select pg_catalog.set_config('request.jwt.claim.role', 'authenticated', true);
select pg_catalog.set_config(
  'request.jwt.claims', '{"role":"authenticated"}', true
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_begin_v1(
    (select id from portal_lcia_ids where label = 'worker_job_v3'),
    '52710000-0000-4000-8000-000000000420', 2, 2,
    pg_temp.portal_lcia_source()
  ) ->> 'code',
  'service_role_required',
  'a non-service request cannot begin projection materialization'
);

select extensions.is(
  private.svc_portal_lcia_projection_worker_input_v1(
    (select id from portal_lcia_ids where label = 'worker_job_v3'),
    '52710000-0000-4000-8000-000000000420'
  ) ->> 'code',
  'service_role_required',
  'the V3 Worker-input helper is not available to an actor request'
);

select pg_catalog.set_config('request.jwt.claim.role', 'service_role', true);
select pg_catalog.set_config(
  'request.jwt.claims', '{"role":"service_role"}', true
);

select extensions.ok(
  private.svc_portal_lcia_projection_worker_input_v1(
    (select id from portal_lcia_ids where label = 'worker_job_v3'),
    '52710000-0000-4000-8000-000000000420'
  ) #>> '{data,payloadSchemaVersion}' =
       'lcia_result.package_build.request.v3'
  and private.svc_portal_lcia_projection_worker_input_v1(
    (select id from portal_lcia_ids where label = 'worker_job_v3'),
    '52710000-0000-4000-8000-000000000420'
  ) #>> '{data,projectionContractVersion}' =
       'portal.lcia-projection.v1',
  'the V3-only Worker-input helper returns the exact lease-bound projection contract'
);

select extensions.is(
  private.svc_portal_lcia_projection_worker_input_v1(
    (select id from portal_lcia_ids where label = 'worker_job_v3'),
    '52710000-0000-4000-8000-000000009999'
  ) ->> 'code',
  'projection_lease_invalid',
  'the V3 Worker input cannot be read with a stale or different lease'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_begin_v1(
    '52710000-0000-4000-8000-000000009999',
    '52710000-0000-4000-8000-000000000420', 2, 2,
    pg_temp.portal_lcia_source()
  ) ->> 'code',
  'projection_job_not_found',
  'stage begin rejects an unknown worker job'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_begin_v1(
    (select id from portal_lcia_legacy_jobs_before limit 1),
    '52710000-0000-4000-8000-000000000420', 2, 2,
    pg_temp.portal_lcia_source()
  ) ->> 'code',
  'projection_job_contract_invalid',
  'stage begin rejects a legacy V1/V2 job with no V3 opt-in marker'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_begin_v1(
    (select id from portal_lcia_ids where label = 'worker_job_v3'),
    '52710000-0000-4000-8000-000000009999', 2, 2,
    pg_temp.portal_lcia_source()
  ) ->> 'code',
  'projection_lease_invalid',
  'stage begin is fenced by the exact active Worker lease token'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_begin_v1(
    (select id from portal_lcia_ids where label = 'worker_job_v3'),
    '52710000-0000-4000-8000-000000000420', 1, 2,
    pg_temp.portal_lcia_source()
  ) ->> 'code',
  'projection_process_count_mismatch',
  'stage begin binds the declared Process count to the V3 input manifest'
);

create temporary table portal_lcia_begin_response (response jsonb not null)
on commit drop;

insert into portal_lcia_begin_response (response)
values (
  private.svc_portal_lcia_projection_stage_begin_v1(
    (select id from portal_lcia_ids where label = 'worker_job_v3'),
    '52710000-0000-4000-8000-000000000420', 2, 2,
    pg_temp.portal_lcia_source()
  )
);

update portal_lcia_ids
set id = (
  select (response->'data'->>'projectionId')::uuid
  from portal_lcia_begin_response
)
where label = 'stage';

select extensions.ok(
  (select (response->>'ok')::boolean from portal_lcia_begin_response)
  and not (select (response->>'idempotentReplay')::boolean
           from portal_lcia_begin_response)
  and exists (
    select 1
    from private.portal_lcia_projection_headers
    where id = (select id from portal_lcia_ids where label = 'stage')
      and status = 'staging'
      and process_count = 2
      and impact_count = 2
      and expected_value_count = 4
      and created_at >= (
        select observed_at from portal_lcia_stage_clock where label = 'before_begin'
      )
      and prepared_at is null
  ),
  'stage begin creates a database-timestamped 2x2 staging header'
);

select extensions.ok(
  (
    private.svc_portal_lcia_projection_stage_begin_v1(
      (select id from portal_lcia_ids where label = 'worker_job_v3'),
      '52710000-0000-4000-8000-000000000420', 2, 2,
      pg_temp.portal_lcia_source()
    ) ->> 'idempotentReplay'
  )::boolean,
  'an exact stage-begin retry recovers the existing projection after response loss'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_begin_v1(
    (select id from portal_lcia_ids where label = 'worker_job_v3'),
    '52710000-0000-4000-8000-000000000420', 2, 2,
    pg_temp.portal_lcia_source('a')
  ) ->> 'code',
  'projection_conflict',
  'a stage-begin retry with different immutable evidence conflicts'
);

select extensions.ok(
  private.svc_portal_lcia_projection_stage_status_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420'
  ) #>> '{data,processCount}' = '0'
  and private.svc_portal_lcia_projection_stage_status_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420'
  ) #>> '{data,impactCount}' = '0'
  and private.svc_portal_lcia_projection_stage_status_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420'
  ) #>> '{data,valueCount}' = '0',
  'stage status makes a lost response recoverable before any batch is accepted'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    null::jsonb
  ) ->> 'code',
  'invalid_projection_batch',
  'SQL NULL batch input is rejected explicitly rather than passing three-valued validation'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    '{"schemaVersion":"portal.lcia-projection.batch.v1","processes":[],"impacts":[],"values":[]}'::jsonb
  ) ->> 'code',
  'invalid_projection_batch',
  'a zero-record batch is rejected at the lower boundary'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', '[]'::jsonb,
      'impacts', '[]'::jsonb,
      'values', '[]'::jsonb,
      'privateLocator', 's3://must-never-enter-projection'
    )
  ) ->> 'code',
  'invalid_projection_batch',
  'an unknown locator-like root field is rejected fail closed'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', jsonb_build_array(
        jsonb_set(
          pg_temp.portal_lcia_process_record(
            1, '52710000-0000-4000-8000-000000000102',
            '1.23'
          ),
          '{functionalUnitDescription,0,locator}',
          '"s3://hidden"'::jsonb,
          true
        )
      ),
      'impacts', '[]'::jsonb,
      'values', '[]'::jsonb
    )
  ) ->> 'code',
  'invalid_projection_batch',
  'a recursively nested locator or unknown localized-text field is rejected'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', '[]'::jsonb,
      'impacts', jsonb_build_array(
        pg_temp.portal_lcia_impact_record(
          0, '52710000-0000-4000-8000-000000000301'
        ),
        pg_temp.portal_lcia_impact_record(
          0, '52710000-0000-4000-8000-000000000301'
        )
      ),
      'values', '[]'::jsonb
    )
  ) ->> 'code',
  'invalid_projection_batch',
  'duplicate indexes inside one batch are rejected before any insert'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', jsonb_build_array(
        pg_temp.portal_lcia_process_record(
          0, '52710000-0000-4000-8000-000000000102', '1'
        )
      ),
      'impacts', '[]'::jsonb,
      'values', '[]'::jsonb
    )
  ) ->> 'code',
  'invalid_projection_batch',
  'a Process identity substituted into another manifest position is rejected'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', '[]'::jsonb,
      'impacts', jsonb_build_array(
        jsonb_set(
          pg_temp.portal_lcia_impact_record(
            0, '52710000-0000-4000-8000-000000000301'
          ),
          '{methodId}',
          '"52710000-0000-4000-8000-000000000202"'::jsonb
        )
      ),
      'values', '[]'::jsonb
    )
  ) ->> 'code',
  'invalid_projection_batch',
  'an Impact Method substituted or swapped from another certified position is rejected'
);

create temporary table portal_lcia_first_batch (response jsonb not null)
on commit drop;

insert into portal_lcia_first_batch (response)
values (
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', jsonb_build_array(
        pg_temp.portal_lcia_process_record(
          1, '52710000-0000-4000-8000-000000000102',
          '1.23'
        )
      ),
      'impacts', '[]'::jsonb,
      'values', '[]'::jsonb
    )
  )
);

select extensions.ok(
  (select (response->>'ok')::boolean from portal_lcia_first_batch)
  and (select response #>> '{data,acceptedRecordCount}' = '1'
       from portal_lcia_first_batch)
  and exists (
    select 1
    from private.portal_lcia_projection_process_axis
    where projection_id = (select id from portal_lcia_ids where label = 'stage')
      and process_index = 1
      and functional_unit_amount = '1.23'
      and record_hash ~ '^[0-9a-f]{64}$'
  ),
  'a one-record batch is accepted when its typed decimal is already canonical'
);

select extensions.ok(
  (
    private.svc_portal_lcia_projection_stage_register_batch_v1(
      (select id from portal_lcia_ids where label = 'stage'),
      '52710000-0000-4000-8000-000000000420',
      jsonb_build_object(
        'schemaVersion', 'portal.lcia-projection.batch.v1',
        'processes', jsonb_build_array(
          pg_temp.portal_lcia_process_record(
            1, '52710000-0000-4000-8000-000000000102',
            '1.23'
          )
        ),
        'impacts', '[]'::jsonb,
        'values', '[]'::jsonb
      )
    ) ->> 'idempotentReplay'
  )::boolean,
  'the same batch can be replayed exactly after a lost response'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', jsonb_build_array(
        pg_temp.portal_lcia_process_record(
          1, '52710000-0000-4000-8000-000000000102', '2'
        )
      ),
      'impacts', '[]'::jsonb,
      'values', '[]'::jsonb
    )
  ) ->> 'code',
  'projection_batch_conflict',
  'reusing a Process index with different content is a deterministic conflict'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_seal_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420'
  ) ->> 'code',
  'projection_incomplete',
  'seal rejects a projection with missing axes and grid cells'
);

create temporary table portal_lcia_complete_batch (response jsonb not null)
on commit drop;

insert into portal_lcia_complete_batch (response)
values (
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      -- Explicit indexes make out-of-order delivery safe and hash-stable.
      'processes', jsonb_build_array(
        pg_temp.portal_lcia_process_record(
          0, '52710000-0000-4000-8000-000000000101', '1'
        )
      ),
      'impacts', jsonb_build_array(
        pg_temp.portal_lcia_impact_record(
          1, '52710000-0000-4000-8000-000000000302'
        ),
        pg_temp.portal_lcia_impact_record(
          0, '52710000-0000-4000-8000-000000000301'
        )
      ),
      'values', jsonb_build_array(
        pg_temp.portal_lcia_value_record(1, 1, 2, '3.14159'),
        pg_temp.portal_lcia_value_record(0, 0, 2, '0'),
        pg_temp.portal_lcia_value_record(1, 0, 2, '-25'),
        pg_temp.portal_lcia_value_record(0, 1, 2, '1.23')
      )
    )
  )
);

select extensions.ok(
  (select (response->>'ok')::boolean from portal_lcia_complete_batch)
  and exists (
    select 1
    from private.portal_lcia_projection_values
    where projection_id = (select id from portal_lcia_ids where label = 'stage')
      and ordinal = 1
      and value_text = '0'
      and value_numeric = 0
  )
  and not exists (
    select 1
    from private.portal_lcia_projection_values
    where projection_id = (select id from portal_lcia_ids where label = 'stage')
      and ordinal not between 1 and 4
  ),
  'out-of-order batches complete the dense grid and preserve explicit zero as a real row'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', '[]'::jsonb,
      'impacts', '[]'::jsonb,
      'values', jsonb_build_array(
        jsonb_build_object(
          'ordinal', 99, 'processIndex', 0, 'impactIndex', 0, 'value', '1'
        )
      )
    )
  ) ->> 'code',
  'invalid_projection_batch',
  'an out-of-range or formula-violating ordinal is rejected'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', '[]'::jsonb,
      'impacts', '[]'::jsonb,
      'values', jsonb_build_array(
        jsonb_build_object(
          'ordinal', 1, 'processIndex', 0, 'impactIndex', 0, 'value', 'NaN'
        )
      )
    )
  ) ->> 'code',
  'invalid_projection_batch',
  'a non-finite decimal is rejected instead of entering the numeric grid'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', '[]'::jsonb,
      'impacts', '[]'::jsonb,
      'values', jsonb_build_array(
        jsonb_build_object(
          'ordinal', 1, 'processIndex', 0, 'impactIndex', 0, 'value', '1.0'
        )
      )
    )
  ) ->> 'code',
  'invalid_projection_batch',
  'a noncanonical decimal spelling is rejected rather than silently normalized'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', '[]'::jsonb,
      'impacts', '[]'::jsonb,
      'values', jsonb_build_array(
        jsonb_build_object(
          'ordinal', 1, 'processIndex', 0, 'impactIndex', 0, 'value', '1e3'
        )
      )
    )
  ) ->> 'code',
  'invalid_projection_batch',
  'exponent-form decimal text is rejected at the canonical projection boundary'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', jsonb_build_array(
        jsonb_set(
          pg_temp.portal_lcia_process_record(
            0, '52710000-0000-4000-8000-000000000101', '1'
          ),
          '{processIndex}', '0.5'::jsonb
        )
      ),
      'impacts', '[]'::jsonb,
      'values', '[]'::jsonb
    )
  ) ->> 'code',
  'invalid_projection_batch',
  'fractional JSON Process indexes are rejected rather than rounded or cast'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', jsonb_build_array(
        jsonb_set(
          pg_temp.portal_lcia_process_record(
            0, '52710000-0000-4000-8000-000000000101', '1'
          ),
          '{referenceYear}', '2025.5'::jsonb
        )
      ),
      'impacts', '[]'::jsonb,
      'values', '[]'::jsonb
    )
  ) ->> 'code',
  'invalid_projection_batch',
  'fractional JSON reference years are rejected rather than rounded or cast'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', '[]'::jsonb,
      'impacts', '[]'::jsonb,
      'values', jsonb_build_array(jsonb_build_object(
        'ordinal', 1.5, 'processIndex', 0, 'impactIndex', 0, 'value', '1'
      ))
    )
  ) ->> 'code',
  'invalid_projection_batch',
  'fractional JSON ordinals are rejected rather than rounded or cast'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', '[]'::jsonb,
      'impacts', jsonb_build_array(
        jsonb_set(
          pg_temp.portal_lcia_impact_record(
            0, '52710000-0000-4000-8000-000000000301'
          ),
          '{impactName,0,value}', '"   "'::jsonb
        )
      ),
      'values', '[]'::jsonb
    )
  ) ->> 'code',
  'invalid_projection_batch',
  'empty or whitespace-only localized values are rejected'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', '[]'::jsonb,
      'impacts', jsonb_build_array(jsonb_build_object(
        'impactIndex', 0,
        'methodId', '52710000-0000-4000-8000-000000000201',
        'methodVersion', '01.00.000',
        'impactId', 'oversized',
        'impactName', jsonb_build_array(jsonb_build_object(
          'language', 'en', 'value', repeat('x', 1048576)
        )),
        'unit', 'kg',
        'methodDocumentSha256', repeat('e', 64)
      )),
      'values', '[]'::jsonb
    )
  ) ->> 'code',
  'invalid_projection_batch',
  'the frozen one-MiB serialized batch cap fails closed'
);

insert into portal_lcia_stage_clock values ('before_seal', clock_timestamp());

create temporary table portal_lcia_seal_response (response jsonb not null)
on commit drop;

insert into portal_lcia_seal_response (response)
values (
  private.svc_portal_lcia_projection_stage_seal_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420'
  )
);

select extensions.ok(
  (select (response->>'ok')::boolean from portal_lcia_seal_response)
  and (select response #>> '{data,status}' = 'prepared'
       from portal_lcia_seal_response)
  and (select response #>> '{data,processCount}' = '2'
       from portal_lcia_seal_response)
  and (select response #>> '{data,impactCount}' = '2'
       from portal_lcia_seal_response)
  and (select response #>> '{data,valueCount}' = '4'
       from portal_lcia_seal_response)
  and (select response #>> '{data,processAxisHash}' ~ '^[0-9a-f]{64}$'
       from portal_lcia_seal_response)
  and (select response #>> '{data,impactAxisHash}' ~ '^[0-9a-f]{64}$'
       from portal_lcia_seal_response)
  and (select response #>> '{data,valueGridHash}' ~ '^[0-9a-f]{64}$'
       from portal_lcia_seal_response)
  and (select response #>> '{data,relationHash}' ~ '^[0-9a-f]{64}$'
       from portal_lcia_seal_response)
  and (select response #>> '{data,contentHash}' ~ '^[0-9a-f]{64}$'
       from portal_lcia_seal_response),
  'seal reconciles all three relation counts and emits the frozen grid/content hash set'
);

select extensions.ok(
  exists (
    select 1
    from private.portal_lcia_projection_headers
    where id = (select id from portal_lcia_ids where label = 'stage')
      and status = 'prepared'
      and prepared_at >= (
        select observed_at from portal_lcia_stage_clock where label = 'before_seal'
      )
      and process_axis_hash =
        (select response #>> '{data,processAxisHash}' from portal_lcia_seal_response)
      and impact_axis_hash =
        (select response #>> '{data,impactAxisHash}' from portal_lcia_seal_response)
      and value_grid_hash =
        (select response #>> '{data,valueGridHash}' from portal_lcia_seal_response)
      and relation_hash =
        (select response #>> '{data,relationHash}' from portal_lcia_seal_response)
      and content_hash =
        (select response #>> '{data,contentHash}' from portal_lcia_seal_response)
  ),
  'seal persists only database timestamps and exactly the returned evidence hashes'
);

select extensions.ok(
  (
    private.svc_portal_lcia_projection_stage_seal_v1(
      (select id from portal_lcia_ids where label = 'stage'),
      '52710000-0000-4000-8000-000000000420'
    ) ->> 'idempotentReplay'
  )::boolean,
  'an exact seal retry returns the prepared projection after response loss'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000000420',
    jsonb_build_object(
      'schemaVersion', 'portal.lcia-projection.batch.v1',
      'processes', jsonb_build_array(
        pg_temp.portal_lcia_process_record(
          0, '52710000-0000-4000-8000-000000000101', '1'
        )
      ),
      'impacts', '[]'::jsonb,
      'values', '[]'::jsonb
    )
  ) ->> 'code',
  'projection_not_staging',
  'prepared projection rows cannot be changed by a later Worker batch'
);

-- A separate V3 fixture isolates the exact 500-record admission boundary.
-- The 500-record batch is deliberately partial, so this test measures batch
-- admission without asking seal to accept a missing final grid cell.
create temporary table portal_lcia_boundary_methods (
  methods jsonb not null
) on commit drop;

insert into portal_lcia_boundary_methods (methods)
select jsonb_agg(
  jsonb_build_object(
    'id', case
      when index = 0 then '52710000-0000-4000-8000-000000000201'
      when index = 1 then '52710000-0000-4000-8000-000000000202'
      else '52710000-0000-4000-8000-' ||
        lpad((index + 1000)::text, 12, '0')
    end,
    'version', '01.00.000'
  ) order by index
)
from generate_series(0, 249) as method(index);

insert into private.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, subject_type, subject_id,
  requester_type, requested_by, idempotency_key, request_hash, visibility,
  payload_schema_version, payload_json, status, lease_token,
  lease_expires_at, started_at
)
select
  '52710000-0000-4000-8000-000000000421',
  job.job_kind, job.worker_runtime, job.worker_queue,
  'lcia_result_build', '52710000-0000-4000-8000-000000000422',
  'operator', job.requested_by, 'portal-lcia-batch-500', repeat('a', 64),
  'operator', 'lcia_result.package_build.request.v3',
  jsonb_set(
    jsonb_set(
      jsonb_set(
        job.payload_json,
        '{input_manifest,processes}',
        '[{"id":"52710000-0000-4000-8000-000000000101","version":"01.00.000"}]'::jsonb,
        false
      ),
      '{lcia_method_set}',
      (select methods from portal_lcia_boundary_methods),
      false
    ),
    '{effective_scope,lciaMethods}',
    (select methods from portal_lcia_boundary_methods),
    false
  ) || jsonb_build_object('input_manifest_hash', repeat('a', 64)),
  'running', '52710000-0000-4000-8000-000000000422',
  clock_timestamp() + interval '10 minutes', clock_timestamp()
from private.worker_jobs as job
where job.id = (select id from portal_lcia_ids where label = 'worker_job_v3');

create temporary table portal_lcia_boundary_begin (response jsonb not null)
on commit drop;

insert into portal_lcia_boundary_begin (response)
values (
  private.svc_portal_lcia_projection_stage_begin_v1(
    '52710000-0000-4000-8000-000000000421',
    '52710000-0000-4000-8000-000000000422',
    1, 250, pg_temp.portal_lcia_source('9')
  )
);

update portal_lcia_ids
set id = (
  select (response->'data'->>'projectionId')::uuid
  from portal_lcia_boundary_begin
)
where label = 'batch_projection';

create temporary table portal_lcia_boundary_batches (
  label text primary key,
  payload jsonb not null
) on commit drop;

insert into portal_lcia_boundary_batches (label, payload)
select
  '500',
  jsonb_build_object(
    'schemaVersion', 'portal.lcia-projection.batch.v1',
    'processes', jsonb_build_array(
      pg_temp.portal_lcia_process_record(
        0, '52710000-0000-4000-8000-000000000101', '1'
      )
    ),
    'impacts', (
      select jsonb_agg(
        pg_temp.portal_lcia_impact_record(
          index,
          'boundary-impact-' || lpad(index::text, 3, '0')
        ) order by index
      )
      from generate_series(0, 249) as impact(index)
    ),
    'values', (
      select jsonb_agg(
        pg_temp.portal_lcia_value_record(0, index, 250, index::text)
        order by index
      )
      from generate_series(0, 248) as value(index)
    )
  )
union all
select
  '501',
  jsonb_build_object(
    'schemaVersion', 'portal.lcia-projection.batch.v1',
    'processes', jsonb_build_array(
      pg_temp.portal_lcia_process_record(
        0, '52710000-0000-4000-8000-000000000101', '1'
      )
    ),
    'impacts', (
      select jsonb_agg(
        pg_temp.portal_lcia_impact_record(
          index,
          'boundary-impact-' || lpad(index::text, 3, '0')
        ) order by index
      )
      from generate_series(0, 249) as impact(index)
    ),
    'values', (
      select jsonb_agg(
        pg_temp.portal_lcia_value_record(0, index, 250, index::text)
        order by index
      )
      from generate_series(0, 249) as value(index)
    )
  );

select extensions.ok(
  (
    private.svc_portal_lcia_projection_stage_register_batch_v1(
      (select id from portal_lcia_ids where label = 'batch_projection'),
      '52710000-0000-4000-8000-000000000422',
      (select payload from portal_lcia_boundary_batches where label = '500')
    ) ->> 'ok'
  )::boolean
  and private.svc_portal_lcia_projection_stage_status_v1(
    (select id from portal_lcia_ids where label = 'batch_projection'),
    '52710000-0000-4000-8000-000000000422'
  ) #>> '{data,processCount}' = '1'
  and private.svc_portal_lcia_projection_stage_status_v1(
    (select id from portal_lcia_ids where label = 'batch_projection'),
    '52710000-0000-4000-8000-000000000422'
  ) #>> '{data,impactCount}' = '250'
  and private.svc_portal_lcia_projection_stage_status_v1(
    (select id from portal_lcia_ids where label = 'batch_projection'),
    '52710000-0000-4000-8000-000000000422'
  ) #>> '{data,valueCount}' = '249',
  'a valid 500-record batch is admitted at the exact upper count boundary'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_register_batch_v1(
    (select id from portal_lcia_ids where label = 'batch_projection'),
    '52710000-0000-4000-8000-000000000422',
    (select payload from portal_lcia_boundary_batches where label = '501')
  ) ->> 'code',
  'invalid_projection_batch',
  'a 501-record batch is rejected before any row is changed'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_status_v1(
    (select id from portal_lcia_ids where label = 'batch_projection'),
    '52710000-0000-4000-8000-000000000422'
  ) #>> '{data,valueCount}',
  '249',
  'the rejected 501-record batch leaves the prior partial stage unchanged'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_fail_v1(
    (select id from portal_lcia_ids where label = 'batch_projection'),
    '52710000-0000-4000-8000-000000009999',
    'worker_failed', 'lease mismatch', '{}'::jsonb
  ) ->> 'code',
  'projection_lease_invalid',
  'failure recording is fenced by the exact active Worker lease'
);

insert into portal_lcia_stage_clock values ('before_fail', clock_timestamp());

create temporary table portal_lcia_fail_response (response jsonb not null)
on commit drop;

insert into portal_lcia_fail_response (response)
values (
  private.svc_portal_lcia_projection_stage_fail_v1(
    (select id from portal_lcia_ids where label = 'batch_projection'),
    '52710000-0000-4000-8000-000000000422',
    'worker_failed', 'deterministic fixture failure', '{}'::jsonb
  )
);

select extensions.ok(
  (select (response ->> 'ok')::boolean from portal_lcia_fail_response)
  and exists (
    select 1
    from private.portal_lcia_projection_headers
    where id = (select id from portal_lcia_ids where label = 'batch_projection')
      and status = 'failed'
      and failure_code = 'worker_failed'
      and failure_message = 'deterministic fixture failure'
      and failed_at >= (
        select observed_at from portal_lcia_stage_clock where label = 'before_fail'
      )
  ),
  'a service Worker can terminate staging as failed with a database timestamp'
);

select extensions.ok(
  (
    private.svc_portal_lcia_projection_stage_fail_v1(
      (select id from portal_lcia_ids where label = 'batch_projection'),
      '52710000-0000-4000-8000-000000000422',
      'worker_failed', 'deterministic fixture failure', '{}'::jsonb
    ) ->> 'idempotentReplay'
  )::boolean,
  'an exact failure retry recovers the terminal response'
);

select extensions.is(
  private.svc_portal_lcia_projection_stage_fail_v1(
    (select id from portal_lcia_ids where label = 'batch_projection'),
    '52710000-0000-4000-8000-000000000422',
    'different_failure', 'conflicting retry', '{}'::jsonb
  ) ->> 'code',
  'projection_not_staging',
  'a conflicting failure retry cannot rewrite terminal failure evidence'
);

-- Package readiness for V3 is a separate service-only helper.  It atomically
-- checks the prepared projection and all retained calculation evidence while
-- leaving the established V1/V2 package routines unchanged.
insert into private.lca_results (
  id, job_id, snapshot_id, payload, diagnostics, artifact_url,
  artifact_sha256, artifact_byte_size, artifact_format, worker_job_id,
  is_pinned
)
values (
  '52710000-0000-4000-8000-000000000430',
  (select id from portal_lcia_ids where label = 'worker_job_v3'),
  '52710000-0000-4000-8000-000000000403', '{}'::jsonb, '{}'::jsonb,
  's3://portal-lcia-test/private/v3-result.json', repeat('b', 64), 301,
  'application/json',
  (select id from portal_lcia_ids where label = 'worker_job_v3'), false
);

update private.lca_latest_all_unit_results
set job_id = (select id from portal_lcia_ids where label = 'worker_job_v3'),
    result_id = '52710000-0000-4000-8000-000000000430',
    query_artifact_url = 's3://portal-lcia-test/private/v3-query.json',
    query_artifact_sha256 = repeat('c', 64),
    query_artifact_byte_size = 302,
    query_artifact_format = 'application/json',
    status = 'ready',
    worker_job_id = (select id from portal_lcia_ids where label = 'worker_job_v3')
where snapshot_id = '52710000-0000-4000-8000-000000000403';

select pg_catalog.set_config('request.jwt.claim.role', 'authenticated', true);
select pg_catalog.set_config(
  'request.jwt.claims', '{"role":"authenticated"}', true
);

select extensions.is(
  private.svc_portal_lcia_projection_package_mark_ready_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    (select id from portal_lcia_ids where label = 'worker_job_v3'),
    '52710000-0000-4000-8000-000000000420',
    'portal-lcia-package-v3',
    '52710000-0000-4000-8000-000000000403',
    '52710000-0000-4000-8000-000000000430',
    '52710000-0000-4000-8000-000000000405',
    jsonb_build_object(
      'artifactUrl', 's3://portal-lcia-test/private/v3-result.json',
      'artifactSha256', repeat('b', 64),
      'artifactByteSize', 301,
      'artifactFormat', 'application/json'
    ),
    jsonb_build_object(
      'artifactUrl', 's3://portal-lcia-test/private/v3-query.json',
      'artifactSha256', repeat('c', 64),
      'artifactByteSize', 302,
      'artifactFormat', 'application/json'
    ),
    jsonb_build_object(
      'bundleContentHash', repeat('d', 64),
      'bundleManifestSha256', repeat('e', 64),
      'lciaChunkSetSha256', repeat('f', 64),
      'portalProjectionId',
        (select id::text from portal_lcia_ids where label = 'stage'),
      'portalProjectionContentHash',
        (select response #>> '{data,contentHash}' from portal_lcia_seal_response)
    ),
    jsonb_build_array(
      '52710000-0000-4000-8000-000000000201',
      '52710000-0000-4000-8000-000000000202'
    ),
    '52710000-0000-4000-8000-000000000201',
    repeat('1', 64),
    '{}'::jsonb
  ) ->> 'code',
  'service_role_required',
  'an actor cannot use the V3 package-ready service helper'
);

select pg_catalog.set_config('request.jwt.claim.role', 'service_role', true);
select pg_catalog.set_config(
  'request.jwt.claims', '{"role":"service_role"}', true
);

select extensions.is(
  private.svc_portal_lcia_projection_package_mark_ready_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    (select id from portal_lcia_ids where label = 'worker_job_v3'),
    '52710000-0000-4000-8000-000000000420',
    'portal-lcia-package-v3',
    '52710000-0000-4000-8000-000000000403',
    '52710000-0000-4000-8000-000000000430',
    '52710000-0000-4000-8000-000000000405',
    jsonb_build_object('artifactSha256', repeat('0', 64)),
    jsonb_build_object('artifactSha256', repeat('c', 64)),
    jsonb_build_object(
      'bundleContentHash', repeat('d', 64),
      'bundleManifestSha256', repeat('e', 64),
      'lciaChunkSetSha256', repeat('f', 64),
      'portalProjectionId',
        (select id::text from portal_lcia_ids where label = 'stage'),
      'portalProjectionContentHash',
        (select response #>> '{data,contentHash}' from portal_lcia_seal_response)
    ),
    jsonb_build_array(
      '52710000-0000-4000-8000-000000000201',
      '52710000-0000-4000-8000-000000000202'
    ),
    '52710000-0000-4000-8000-000000000201',
    repeat('1', 64),
    '{}'::jsonb
  ) ->> 'code',
  'projection_evidence_mismatch',
  'V3 package readiness rejects a result hash that drifted after seal'
);

select extensions.is(
  private.svc_portal_lcia_projection_package_mark_ready_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    (select id from portal_lcia_ids where label = 'worker_job_v3'),
    '52710000-0000-4000-8000-000000000420',
    'portal-lcia-package-impact-order-drift',
    '52710000-0000-4000-8000-000000000403',
    '52710000-0000-4000-8000-000000000430',
    '52710000-0000-4000-8000-000000000405',
    jsonb_build_object(
      'artifactUrl', 's3://portal-lcia-test/private/v3-result.json',
      'artifactSha256', repeat('b', 64),
      'artifactByteSize', 301,
      'artifactFormat', 'application/json'
    ),
    jsonb_build_object(
      'artifactUrl', 's3://portal-lcia-test/private/v3-query.json',
      'artifactSha256', repeat('c', 64),
      'artifactByteSize', 302,
      'artifactFormat', 'application/json'
    ),
    jsonb_build_object(
      'bundleContentHash', repeat('d', 64),
      'bundleManifestSha256', repeat('e', 64),
      'lciaChunkSetSha256', repeat('f', 64),
      'portalProjectionId',
        (select id::text from portal_lcia_ids where label = 'stage'),
      'portalProjectionContentHash',
        (select response #>> '{data,contentHash}' from portal_lcia_seal_response)
    ),
    jsonb_build_array(
      '52710000-0000-4000-8000-000000000202',
      '52710000-0000-4000-8000-000000000201'
    ),
    '52710000-0000-4000-8000-000000000201',
    repeat('1', 64),
    '{}'::jsonb
  ) ->> 'code',
  'projection_evidence_mismatch',
  'package readiness rejects available-Impact identity or order drift'
);

create temporary table portal_lcia_v3_package_response (response jsonb not null)
on commit drop;

insert into portal_lcia_v3_package_response (response)
values (
  private.svc_portal_lcia_projection_package_mark_ready_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    (select id from portal_lcia_ids where label = 'worker_job_v3'),
    '52710000-0000-4000-8000-000000000420',
    'portal-lcia-package-v3',
    '52710000-0000-4000-8000-000000000403',
    '52710000-0000-4000-8000-000000000430',
    '52710000-0000-4000-8000-000000000405',
    jsonb_build_object(
      'artifactUrl', 's3://portal-lcia-test/private/v3-result.json',
      'artifactSha256', repeat('b', 64),
      'artifactByteSize', 301,
      'artifactFormat', 'application/json'
    ),
    jsonb_build_object(
      'artifactUrl', 's3://portal-lcia-test/private/v3-query.json',
      'artifactSha256', repeat('c', 64),
      'artifactByteSize', 302,
      'artifactFormat', 'application/json'
    ),
    jsonb_build_object(
      'bundleContentHash', repeat('d', 64),
      'bundleManifestSha256', repeat('e', 64),
      'lciaChunkSetSha256', repeat('f', 64),
      'portalProjectionId',
        (select id::text from portal_lcia_ids where label = 'stage'),
      'portalProjectionContentHash',
        (select response #>> '{data,contentHash}' from portal_lcia_seal_response)
    ),
    jsonb_build_array(
      '52710000-0000-4000-8000-000000000201',
      '52710000-0000-4000-8000-000000000202'
    ),
    '52710000-0000-4000-8000-000000000201',
    repeat('1', 64),
    '{}'::jsonb
  )
);

update portal_lcia_ids
set id = (
  select (response->'data'->>'packageId')::uuid
  from portal_lcia_v3_package_response
)
where label = 'package';

select extensions.ok(
  (select (response->>'ok')::boolean from portal_lcia_v3_package_response)
  and (select response #>> '{data,projection,projectionId}' =
                (select id::text from portal_lcia_ids where label = 'stage')
       from portal_lcia_v3_package_response)
  and exists (
    select 1
    from private.lcia_result_packages
    where id = (select id from portal_lcia_ids where label = 'package')
      and build_worker_job_id =
        (select id from portal_lcia_ids where label = 'worker_job_v3')
      and package_version = 'portal-lcia-package-v3'
      and package_result_hash = repeat('1', 64)
      and included_input_count = 2
      and available_impact_categories = jsonb_build_array(
        '52710000-0000-4000-8000-000000000201',
        '52710000-0000-4000-8000-000000000202'
      )
  ),
  'the V3-only service helper creates a package bound to the prepared projection evidence'
);

select extensions.is(
  (
    select payload_schema_version
    from private.worker_jobs
    where id = (select id from portal_lcia_ids where label = 'worker_job_v3')
  ),
  'lcia_result.package_build.request.v3',
  'package readiness restores the V3 payload version before returning'
);

-- The additive V3-only publisher reconciles the certificate Process set and
-- prepared projection without changing the frozen legacy publisher.
select pg_catalog.set_config(
  'request.jwt.claim.sub', '52710000-0000-4000-8000-000000000001', true
);
select pg_catalog.set_config('request.jwt.claim.role', 'authenticated', true);
select pg_catalog.set_config(
  'request.jwt.claims',
  '{"role":"authenticated","sub":"52710000-0000-4000-8000-000000000001"}',
  true
);

create temporary table portal_lcia_v3_publication_response (
  response jsonb not null
) on commit drop;

create temporary table portal_lcia_v3_publish_prepare_response (
  response jsonb not null
) on commit drop;

insert into portal_lcia_v3_publish_prepare_response (response)
values (
  api.qry_portal_lcia_result_package_publish_prepare_v1(
    (select id from portal_lcia_ids where label = 'package'),
    '52710000-0000-4000-8000-000000000201'
  )
);

select extensions.ok(
  (select (response->>'ok')::boolean
   from portal_lcia_v3_publish_prepare_response)
  and (select response #>> '{data,publishPlanHash}' ~ '^[0-9a-f]{64}$'
       from portal_lcia_v3_publish_prepare_response)
  and (select response #>> '{data,projection,id}' =
              (select id::text from portal_lcia_ids where label = 'stage')
       from portal_lcia_v3_publish_prepare_response)
  and (select response #>> '{data,currentPublication,publicationId}' =
              (select id::text from portal_lcia_ids
               where label = 'legacy_publication')
       from portal_lcia_v3_publish_prepare_response),
  'V3 package publish prepare freezes projection evidence, current Process set, and publication precondition'
);

select extensions.is(
  api.cmd_portal_lcia_result_package_publish_v1(
    (select id from portal_lcia_ids where label = 'package'),
    '52710000-0000-4000-8000-000000000201',
    repeat('0', 64),
    'publish Portal LCIA V3 package',
    '{}'::jsonb
  ) ->> 'code',
  'publish_plan_drift',
  'V3 package publication rejects a confirmation hash that does not match fresh database evidence'
);

insert into portal_lcia_v3_publication_response (response)
values (
  api.cmd_portal_lcia_result_package_publish_v1(
    (select id from portal_lcia_ids where label = 'package'),
    '52710000-0000-4000-8000-000000000201',
    (select response #>> '{data,publishPlanHash}'
     from portal_lcia_v3_publish_prepare_response),
    'publish Portal LCIA V3 package',
    '{}'::jsonb
  )
);

update portal_lcia_ids
set id = (
  select (response->'data'->>'publicationId')::uuid
  from portal_lcia_v3_publication_response
)
where label = 'publication';

select extensions.ok(
  (select (response->>'ok')::boolean
   from portal_lcia_v3_publication_response)
  and exists (
    select 1
    from private.lcia_result_publications
    where id = (select id from portal_lcia_ids where label = 'publication')
      and package_id = (select id from portal_lcia_ids where label = 'package')
      and is_current
      and status = 'current'
      and published_at is not null
  )
  and exists (
    select 1
    from private.lcia_result_publications
    where id = (select id from portal_lcia_ids where label = 'legacy_publication')
      and not is_current
      and status = 'superseded'
  ),
  'the V3 package becomes the exact current publication and supersedes the legacy fixture'
);

select extensions.ok(
  (
    api.cmd_portal_lcia_result_package_publish_v1(
      (select id from portal_lcia_ids where label = 'package'),
      '52710000-0000-4000-8000-000000000201',
      (select response #>> '{data,publishPlanHash}'
       from portal_lcia_v3_publish_prepare_response),
      'publish Portal LCIA V3 package',
      '{}'::jsonb
    ) ->> 'reused'
  )::boolean,
  'an exact V3 package publication retry returns the current publication after response loss'
);

-- Prepared data remains invisible until an authenticated manager finalizes
-- the exact current publication binding.
reset role;
set local role anon;
select pg_catalog.set_config('request.jwt.claim.role', 'anon', true);
select pg_catalog.set_config('request.jwt.claims', '{"role":"anon"}', true);

select extensions.is(
  api.portal_get_published_lcia_values_v1(
    'process_all_impacts',
    '[{"id":"52710000-0000-4000-8000-000000000101","version":"01.00.000"}]'::jsonb,
    null, null, 50
  ),
  null::jsonb,
  'prepared rows are invisible before an exact publication binding is finalized'
);

reset role;

-- Actor preparation is a read-only manager gate over the exact current
-- package/publication pair.  It does not accept a caller-supplied Worker job.
select pg_catalog.set_config('request.jwt.claim.sub', '', true);
select pg_catalog.set_config('request.jwt.claim.role', 'anon', true);
select pg_catalog.set_config('request.jwt.claims', '{"role":"anon"}', true);

select extensions.is(
  api.qry_portal_lcia_projection_prepare_v1(
    (select id from portal_lcia_ids where label = 'package'),
    (select id from portal_lcia_ids where label = 'publication')
  ) ->> 'code',
  'auth_required',
  'projection preparation requires an authenticated actor'
);

select extensions.ok(
  api.cmd_portal_lcia_projection_finalize_publication_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    (select id from portal_lcia_ids where label = 'publication'),
    'portal-lcia-package-v3', repeat('1', 64),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response),
    'unauthenticated-finalize', '{}'::jsonb
  ) ->> 'code' = 'auth_required'
  and api.qry_portal_lcia_projection_publication_readback_v1(
    (select id from portal_lcia_ids where label = 'publication'),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response)
  ) ->> 'code' = 'auth_required'
  and api.cmd_portal_lcia_projection_revoke_publication_v1(
    (select id from portal_lcia_ids where label = 'publication'),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response),
    'unauthenticated revoke', '{}'::jsonb
  ) ->> 'code' = 'auth_required',
  'finalize, readback, and revoke also reject an unauthenticated actor'
);

select pg_catalog.set_config(
  'request.jwt.claim.sub', '52710000-0000-4000-8000-000000000002', true
);
select pg_catalog.set_config('request.jwt.claim.role', 'authenticated', true);
select pg_catalog.set_config(
  'request.jwt.claims',
  '{"role":"authenticated","sub":"52710000-0000-4000-8000-000000000002"}',
  true
);

select extensions.is(
  api.qry_portal_lcia_projection_prepare_v1(
    (select id from portal_lcia_ids where label = 'package'),
    (select id from portal_lcia_ids where label = 'publication')
  ) ->> 'code',
  'not_data_product_manager',
  'an ordinary authenticated user cannot prepare a Portal LCIA publication'
);

select extensions.ok(
  api.cmd_portal_lcia_projection_finalize_publication_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    (select id from portal_lcia_ids where label = 'publication'),
    'portal-lcia-package-v3', repeat('1', 64),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response),
    'ordinary-finalize', '{}'::jsonb
  ) ->> 'code' = 'not_data_product_manager'
  and api.qry_portal_lcia_projection_publication_readback_v1(
    (select id from portal_lcia_ids where label = 'publication'),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response)
  ) ->> 'code' = 'not_data_product_manager'
  and api.cmd_portal_lcia_projection_revoke_publication_v1(
    (select id from portal_lcia_ids where label = 'publication'),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response),
    'ordinary revoke', '{}'::jsonb
  ) ->> 'code' = 'not_data_product_manager',
  'ordinary authenticated actors cannot finalize, read back, or revoke a projection publication'
);

select pg_catalog.set_config(
  'request.jwt.claim.sub', '52710000-0000-4000-8000-000000000001', true
);
select pg_catalog.set_config(
  'request.jwt.claims',
  '{"role":"authenticated","sub":"52710000-0000-4000-8000-000000000001"}',
  true
);

select extensions.is(
  api.qry_portal_lcia_projection_prepare_v1(
    (select id from portal_lcia_ids where label = 'legacy_package'),
    (select id from portal_lcia_ids where label = 'legacy_publication')
  ) ->> 'code',
  'publication_not_current',
  'prepare rejects a superseded package/publication pair'
);

select extensions.is(
  api.qry_portal_lcia_projection_prepare_v1(
    (select id from portal_lcia_ids where label = 'legacy_package'),
    (select id from portal_lcia_ids where label = 'publication')
  ) ->> 'code',
  'projection_package_not_found',
  'prepare rejects a current publication paired with a different package'
);

create temporary table portal_lcia_prepare_response (response jsonb not null)
on commit drop;

insert into portal_lcia_prepare_response (response)
values (
  api.qry_portal_lcia_projection_prepare_v1(
    (select id from portal_lcia_ids where label = 'package'),
    (select id from portal_lcia_ids where label = 'publication')
  )
);

select extensions.ok(
  (select (response->>'ok')::boolean from portal_lcia_prepare_response)
  and (select response #>> '{data,projectionId}' =
                (select id::text from portal_lcia_ids where label = 'stage')
       from portal_lcia_prepare_response)
  and (select response #>> '{data,packageId}' =
                (select id::text from portal_lcia_ids where label = 'package')
       from portal_lcia_prepare_response)
  and (select response #>> '{data,lciaResultPublicationId}' =
                (select id::text from portal_lcia_ids where label = 'publication')
       from portal_lcia_prepare_response)
  and (select response #>> '{data,publishedAt}' is not null
       from portal_lcia_prepare_response)
  and (select response #>> '{data,contentHash}' =
                (select response #>> '{data,contentHash}'
                 from portal_lcia_seal_response)
       from portal_lcia_prepare_response),
  'manager prepare returns the exact current package/publication, publishedAt, and prepared hash'
);

select extensions.is(
  api.cmd_portal_lcia_projection_finalize_publication_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    '52710000-0000-4000-8000-000000009999',
    'portal-lcia-package-v3', repeat('1', 64),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response),
    'portal-lcia-finalize-v1', '{}'::jsonb
  ) ->> 'code',
  'publication_not_found',
  'finalize rejects an unknown publication identity'
);

select extensions.is(
  api.cmd_portal_lcia_projection_finalize_publication_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    (select id from portal_lcia_ids where label = 'legacy_publication'),
    'portal-lcia-package-v3', repeat('1', 64),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response),
    'portal-lcia-finalize-v1', '{}'::jsonb
  ) ->> 'code',
  'publication_not_current',
  'finalize rejects a superseded publication even with an exact projection hash'
);

select extensions.is(
  api.cmd_portal_lcia_projection_finalize_publication_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    (select id from portal_lcia_ids where label = 'publication'),
    'portal-lcia-package-drift', repeat('1', 64),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response),
    'portal-lcia-finalize-v1', '{}'::jsonb
  ) ->> 'code',
  'projection_evidence_mismatch',
  'finalize rejects package-version drift'
);

select extensions.is(
  api.cmd_portal_lcia_projection_finalize_publication_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    (select id from portal_lcia_ids where label = 'publication'),
    'portal-lcia-package-v3', repeat('0', 64),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response),
    'portal-lcia-finalize-v1', '{}'::jsonb
  ) ->> 'code',
  'projection_evidence_mismatch',
  'finalize rejects package-result hash drift'
);

select extensions.is(
  api.cmd_portal_lcia_projection_finalize_publication_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    (select id from portal_lcia_ids where label = 'publication'),
    'portal-lcia-package-v3', repeat('1', 64), repeat('0', 64),
    'portal-lcia-finalize-v1', '{}'::jsonb
  ) ->> 'code',
  'projection_not_prepared',
  'finalize rejects projection-content hash drift'
);

insert into portal_lcia_stage_clock values ('before_finalize', clock_timestamp());

create temporary table portal_lcia_finalize_response (response jsonb not null)
on commit drop;

insert into portal_lcia_finalize_response (response)
values (
  api.cmd_portal_lcia_projection_finalize_publication_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    (select id from portal_lcia_ids where label = 'publication'),
    'portal-lcia-package-v3', repeat('1', 64),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response),
    'portal-lcia-finalize-v1', '{}'::jsonb
  )
);

select extensions.ok(
  (select (response->>'ok')::boolean from portal_lcia_finalize_response)
  and not (select (response->>'reused')::boolean
           from portal_lcia_finalize_response)
  and exists (
    select 1
    from private.portal_lcia_projection_publications as binding
    join private.lcia_result_publications as publication
      on publication.id = binding.lcia_result_publication_id
    where binding.lcia_result_publication_id =
          (select id from portal_lcia_ids where label = 'publication')
      and binding.projection_id =
          (select id from portal_lcia_ids where label = 'stage')
      and binding.package_id =
          (select id from portal_lcia_ids where label = 'package')
      and binding.status = 'finalized'
      and binding.source_published_at = publication.published_at
      and binding.finalized_at >= (
        select observed_at from portal_lcia_stage_clock where label = 'before_finalize'
      )
      and binding.evidence_hash ~ '^[0-9a-f]{64}$'
  ),
  'finalize binds exact publication/package evidence with database-owned timestamps'
);

select extensions.ok(
  (
    api.cmd_portal_lcia_projection_finalize_publication_v1(
      (select id from portal_lcia_ids where label = 'stage'),
      (select id from portal_lcia_ids where label = 'publication'),
      'portal-lcia-package-v3', repeat('1', 64),
      (select response #>> '{data,contentHash}' from portal_lcia_seal_response),
      'portal-lcia-finalize-v1', '{}'::jsonb
    ) ->> 'reused'
  )::boolean,
  'an exact finalize retry recovers the binding after response loss'
);

select extensions.is(
  api.cmd_portal_lcia_projection_finalize_publication_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    (select id from portal_lcia_ids where label = 'publication'),
    'portal-lcia-package-v3', repeat('1', 64),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response),
    'portal-lcia-finalize-conflict', '{}'::jsonb
  ) ->> 'code',
  'projection_conflict',
  'a finalize retry with a different idempotency binding conflicts'
);

select extensions.is(
  api.qry_portal_lcia_projection_publication_readback_v1(
    (select id from portal_lcia_ids where label = 'publication'),
    repeat('0', 64)
  ) ->> 'code',
  'projection_evidence_mismatch',
  'readback rejects a caller-supplied content hash mismatch'
);

select extensions.ok(
  api.qry_portal_lcia_projection_publication_readback_v1(
    (select id from portal_lcia_ids where label = 'publication'),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response)
  ) #>> '{data,status}' = 'finalized'
  and api.qry_portal_lcia_projection_publication_readback_v1(
    (select id from portal_lcia_ids where label = 'publication'),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response)
  ) #>> '{data,isCurrent}' = 'true'
  and api.qry_portal_lcia_projection_publication_readback_v1(
    (select id from portal_lcia_ids where label = 'publication'),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response)
  ) #>> '{data,isPubliclyVisible}' = 'true'
  and api.qry_portal_lcia_projection_publication_readback_v1(
    (select id from portal_lcia_ids where label = 'publication'),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response)
  ) #>> '{data,valueCount}' = '4',
  'manager readback uses the anonymous visibility predicate and reconciles the sealed grid count'
);

reset role;

grant select on portal_lcia_ids to anon;
set local role anon;
select pg_catalog.set_config('request.jwt.claim.role', 'anon', true);
select pg_catalog.set_config('request.jwt.claims', '{"role":"anon"}', true);

create temporary table portal_lcia_public_pages (
  label text primary key,
  response jsonb not null
) on commit drop;

insert into portal_lcia_public_pages (label, response)
values
  (
    'all_impacts_a',
    api.portal_get_published_lcia_values_v1(
      'process_all_impacts',
      '[{"id":"52710000-0000-4000-8000-000000000101","version":"01.00.000"}]'::jsonb,
      null, null, 50
    )
  ),
  (
    'one_impact_requested',
    api.portal_get_published_lcia_values_v1(
      'processes_one_impact',
      '[{"id":"52710000-0000-4000-8000-000000000102","version":"01.00.000"},{"id":"52710000-0000-4000-8000-000000000101","version":"01.00.000"}]'::jsonb,
      '52710000-0000-4000-8000-000000000301', null, 50
    )
  ),
  (
    'one_impact_ranked',
    api.portal_get_published_lcia_values_v1(
      'ranked_processes_one_impact',
      '[{"id":"52710000-0000-4000-8000-000000000101","version":"01.00.000"},{"id":"52710000-0000-4000-8000-000000000102","version":"01.00.000"}]'::jsonb,
      '52710000-0000-4000-8000-000000000302', null, 50
    )
  );

select extensions.ok(
  (
    select response->>'schemaVersion' = 'portal.published-lcia-page.v1'
      and response->>'mode' = 'process_all_impacts'
      and response #>> '{publication,publicationId}' =
            (select id::text from portal_lcia_ids where label = 'publication')
      and response #>> '{publication,packageId}' =
            (select id::text from portal_lcia_ids where label = 'package')
      and response #>> '{publication,packageVersion}' =
            'portal-lcia-package-v3'
      and response #>> '{publication,publishedAt}' is not null
      and response #>> '{publication,evidenceHash}' ~ '^[0-9a-f]{64}$'
      and jsonb_array_length(response->'rows') = 2
      and response->'nextCursor' = 'null'::jsonb
    from portal_lcia_public_pages where label = 'all_impacts_a'
  ),
  'anonymous process_all_impacts returns the exact finalized publication context and two rows'
);

select extensions.is(
  (
    select count(*)
    from portal_lcia_public_pages as page
    cross join lateral jsonb_array_elements(page.response->'rows') as row(value)
    where jsonb_typeof(row.value->'value') = 'string'
      and jsonb_typeof(row.value#>'{functionalUnit,amount}') = 'string'
      and row.value->>'evidenceStatus' = 'verified'
      and row.value #>> '{process,id}' ~
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and row.value #>> '{process,version}' = '01.00.000'
      and row.value #>> '{method,id}' ~
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      and row.value #>> '{method,version}' = '01.00.000'
      and nullif(row.value #>> '{impact,id}', '') is not null
      and nullif(row.value->>'unit', '') is not null
      and nullif(row.value #>> '{functionalUnit,unit}', '') is not null
      and nullif(row.value #>> '{geography,code}', '') is not null
      and nullif(row.value #>> '{geography,precision}', '') is not null
      and jsonb_typeof(row.value->'referenceYear') = 'number'
  ),
  6::bigint,
  'every public row carries exact Process, FU, geography/year, Method, Impact, unit, decimal-string, and verified evidence context'
);

select extensions.ok(
  (
    select bool_and(
      case row.value #>> '{impact,id}'
        when '52710000-0000-4000-8000-000000000301' then
          row.value #>> '{method,id}' =
            '52710000-0000-4000-8000-000000000201'
        when '52710000-0000-4000-8000-000000000302' then
          row.value #>> '{method,id}' =
            '52710000-0000-4000-8000-000000000202'
        else false
      end
    )
    from portal_lcia_public_pages as page
    cross join lateral jsonb_array_elements(page.response->'rows') as row(value)
  ),
  'each Impact retains its exact positional reviewed Method identity'
);

select extensions.ok(
  exists (
    select 1
    from portal_lcia_public_pages as page
    cross join lateral jsonb_array_elements(page.response->'rows') as row(value)
    where page.label = 'all_impacts_a'
      and row.value #>> '{impact,id}' =
            '52710000-0000-4000-8000-000000000301'
      and row.value->>'value' = '0'
  )
  and not exists (
    select 1
    from portal_lcia_public_pages
    where response::text ~* 'locator|artifact(url|path)|s3://|user_id|team_id|review_id|service_role'
  ),
  'an explicit zero is preserved while every internal locator and actor field stays absent recursively'
);

select extensions.ok(
  (
    select response #>> '{rows,0,process,id}' =
             '52710000-0000-4000-8000-000000000102'
      and response #>> '{rows,1,process,id}' =
             '52710000-0000-4000-8000-000000000101'
    from portal_lcia_public_pages where label = 'one_impact_requested'
  )
  and (
    select response #>> '{rows,0,process,id}' =
             '52710000-0000-4000-8000-000000000102'
      and response #>> '{rows,0,value}' = '3.14159'
      and response #>> '{rows,1,process,id}' =
             '52710000-0000-4000-8000-000000000101'
      and response #>> '{rows,1,value}' = '1.23'
    from portal_lcia_public_pages where label = 'one_impact_ranked'
  ),
  'requested mode preserves request order while ranked mode uses exact numeric descending order'
);

create temporary table portal_lcia_cursor_page (response jsonb not null)
on commit drop;

insert into portal_lcia_cursor_page (response)
values (
  api.portal_get_published_lcia_values_v1(
    'process_all_impacts',
    '[{"id":"52710000-0000-4000-8000-000000000101","version":"01.00.000"}]'::jsonb,
    null, null, 1
  )
);

select extensions.ok(
  (select jsonb_array_length(response->'rows') = 1
          and nullif(response->>'nextCursor', '') is not null
   from portal_lcia_cursor_page)
  and jsonb_array_length(
    api.portal_get_published_lcia_values_v1(
      'process_all_impacts',
      '[{"id":"52710000-0000-4000-8000-000000000101","version":"01.00.000"}]'::jsonb,
      null,
      (select response->>'nextCursor' from portal_lcia_cursor_page),
      1
    )->'rows'
  ) = 1,
  'bounded keyset pagination returns one row per page with no missing second cell'
);

select extensions.throws_ok(
  $$
    select api.portal_get_published_lcia_values_v1(
      'process_all_impacts',
      '[{"id":"52710000-0000-4000-8000-000000000102","version":"01.00.000"}]'::jsonb,
      null,
      (select response->>'nextCursor' from portal_lcia_cursor_page),
      1
    )
  $$,
  '22023',
  'invalid portal request',
  'a cursor cannot be replayed against a different Process query'
);

select extensions.throws_ok(
  $$
    select api.portal_get_published_lcia_values_v1(
      'process_all_impacts',
      '[{"id":"52710000-0000-4000-8000-000000000101","version":"01.00.000"}]'::jsonb,
      null, null, 51
    )
  $$,
  '22023',
  'invalid portal request',
  'public LCIA rejects a limit above fifty'
);

select extensions.throws_ok(
  $$
    select api.portal_get_published_lcia_values_v1(
      'process_all_impacts',
      '[{"id":"52710000-0000-4000-8000-000000000101","version":"01.00.000","teamId":"52710000-0000-4000-8000-000000000001"}]'::jsonb,
      null, null, 50
    )
  $$,
  '22023',
  'invalid portal request',
  'unknown actor/team widening fields are rejected rather than ignored'
);

-- Exact publication identity never bypasses the public Process capability
-- check.  Reclassify one projected Process temporarily and prove both 200 and
-- draft rows disappear without touching immutable projection evidence.
reset role;
alter table public.processes disable trigger user;
update public.processes
set state_code = 200
where id = '52710000-0000-4000-8000-000000000102'
  and version = '01.00.000';
alter table public.processes enable trigger user;

set local role anon;
select pg_catalog.set_config('request.jwt.claim.role', 'anon', true);
select pg_catalog.set_config('request.jwt.claims', '{"role":"anon"}', true);
select extensions.is(
  api.portal_get_published_lcia_values_v1(
    'process_all_impacts',
    '[{"id":"52710000-0000-4000-8000-000000000102","version":"01.00.000"}]'::jsonb,
    null, null, 50
  ) -> 'rows',
  '[]'::jsonb,
  'state-200 Process numerical rows remain hidden despite an exact finalized publication'
);

reset role;
alter table public.processes disable trigger user;
update public.processes
set state_code = 20
where id = '52710000-0000-4000-8000-000000000102'
  and version = '01.00.000';
alter table public.processes enable trigger user;

set local role anon;
select extensions.is(
  api.portal_get_published_lcia_values_v1(
    'process_all_impacts',
    '[{"id":"52710000-0000-4000-8000-000000000102","version":"01.00.000"}]'::jsonb,
    null, null, 50
  ) -> 'rows',
  '[]'::jsonb,
  'draft Process numerical rows remain hidden despite an exact finalized publication'
);

reset role;
alter table public.processes disable trigger user;
update public.processes
set state_code = 100
where id = '52710000-0000-4000-8000-000000000102'
  and version = '01.00.000';
alter table public.processes enable trigger user;

set local role anon;
select extensions.throws_ok(
  $$select count(*) from private.portal_lcia_projection_values$$,
  '42501',
  null,
  'anon cannot read the raw typed value table'
);

reset role;

-- Supersession, unpublish, and projection revocation are independent public
-- visibility fences.  None deletes the immutable typed evidence rows.
alter table private.lcia_result_publications disable trigger user;
update private.lcia_result_publications
set is_current = false, status = 'superseded'
where id = (select id from portal_lcia_ids where label = 'publication');
alter table private.lcia_result_publications enable trigger user;

set local role anon;
select extensions.is(
  api.portal_get_published_lcia_values_v1(
    'process_all_impacts',
    '[{"id":"52710000-0000-4000-8000-000000000101","version":"01.00.000"}]'::jsonb,
    null, null, 50
  ),
  null::jsonb,
  'superseding the source publication immediately hides the finalized projection'
);

reset role;
select extensions.is(
  (select count(*) from private.portal_lcia_projection_values
   where projection_id = (select id from portal_lcia_ids where label = 'stage')),
  4::bigint,
  'supersession hides without deleting typed projection rows'
);

alter table private.lcia_result_publications disable trigger user;
update private.lcia_result_publications
set is_current = true, status = 'current'
where id = (select id from portal_lcia_ids where label = 'publication');
alter table private.lcia_result_publications enable trigger user;

select pg_catalog.set_config(
  'request.jwt.claim.sub', '52710000-0000-4000-8000-000000000001', true
);
select pg_catalog.set_config('request.jwt.claim.role', 'authenticated', true);
select pg_catalog.set_config(
  'request.jwt.claims',
  '{"role":"authenticated","sub":"52710000-0000-4000-8000-000000000001"}',
  true
);

select extensions.ok(
  (
    api.cmd_lcia_result_publication_unpublish(
      (select id from portal_lcia_ids where label = 'publication'),
      'unpublish finalized Portal LCIA visibility fixture',
      '{}'::jsonb
    ) ->> 'ok'
  )::boolean,
  'the established command unpublishes a source with finalized projection evidence'
);

reset role;
set local role anon;
select pg_catalog.set_config('request.jwt.claim.role', 'anon', true);
select pg_catalog.set_config('request.jwt.claims', '{"role":"anon"}', true);
select extensions.is(
  api.portal_get_published_lcia_values_v1(
    'process_all_impacts',
    '[{"id":"52710000-0000-4000-8000-000000000101","version":"01.00.000"}]'::jsonb,
    null, null, 50
  ),
  null::jsonb,
  'unpublishing the finalized source immediately hides all public LCIA rows'
);

reset role;
select extensions.is(
  (select count(*) from private.portal_lcia_projection_values
   where projection_id = (select id from portal_lcia_ids where label = 'stage')),
  4::bigint,
  'unpublishing a finalized source retains all typed projection evidence'
);

alter table private.lcia_result_publications disable trigger user;
update private.lcia_result_publications
set is_current = true, status = 'current', unpublished_at = null,
    unpublished_by = null, reason = null, updated_at = clock_timestamp()
where id = (select id from portal_lcia_ids where label = 'publication');
alter table private.lcia_result_publications enable trigger user;

select pg_catalog.set_config(
  'request.jwt.claim.sub', '52710000-0000-4000-8000-000000000001', true
);
select pg_catalog.set_config('request.jwt.claim.role', 'authenticated', true);
select pg_catalog.set_config(
  'request.jwt.claims',
  '{"role":"authenticated","sub":"52710000-0000-4000-8000-000000000001"}',
  true
);

select extensions.is(
  api.cmd_portal_lcia_projection_revoke_publication_v1(
    (select id from portal_lcia_ids where label = 'publication'),
    repeat('0', 64), 'hash drift', '{}'::jsonb
  ) ->> 'code',
  'projection_evidence_mismatch',
  'revoke is bound to the exact projection content hash'
);

insert into portal_lcia_stage_clock values ('before_revoke', clock_timestamp());

create temporary table portal_lcia_revoke_response (response jsonb not null)
on commit drop;

insert into portal_lcia_revoke_response (response)
values (
  api.cmd_portal_lcia_projection_revoke_publication_v1(
    (select id from portal_lcia_ids where label = 'publication'),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response),
    'Portal LCIA projection withdrawn', '{}'::jsonb
  )
);

select extensions.ok(
  (select (response->>'ok')::boolean from portal_lcia_revoke_response)
  and not (select (response->>'reused')::boolean
           from portal_lcia_revoke_response)
  and exists (
    select 1
    from private.portal_lcia_projection_publications
    where lcia_result_publication_id =
          (select id from portal_lcia_ids where label = 'publication')
      and status = 'revoked'
      and revoked_by = '52710000-0000-4000-8000-000000000001'
      and revoked_at >= (
        select observed_at from portal_lcia_stage_clock where label = 'before_revoke'
      )
      and revoke_reason = 'Portal LCIA projection withdrawn'
  ),
  'manager revoke records a database timestamp and durable reason without deleting rows'
);

select extensions.ok(
  (
    api.cmd_portal_lcia_projection_revoke_publication_v1(
      (select id from portal_lcia_ids where label = 'publication'),
      (select response #>> '{data,contentHash}' from portal_lcia_seal_response),
      'Portal LCIA projection withdrawn', '{}'::jsonb
    ) ->> 'reused'
  )::boolean,
  'an exact revoke retry recovers the terminal binding after response loss'
);

select extensions.is(
  api.cmd_portal_lcia_projection_finalize_publication_v1(
    (select id from portal_lcia_ids where label = 'stage'),
    (select id from portal_lcia_ids where label = 'publication'),
    'portal-lcia-package-v3', repeat('1', 64),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response),
    'portal-lcia-finalize-v1', '{}'::jsonb
  ) ->> 'code',
  'projection_conflict',
  'a revoked binding is terminal and cannot masquerade as a successful finalize retry'
);

select extensions.ok(
  api.qry_portal_lcia_projection_publication_readback_v1(
    (select id from portal_lcia_ids where label = 'publication'),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response)
  ) #>> '{data,status}' = 'revoked'
  and api.qry_portal_lcia_projection_publication_readback_v1(
    (select id from portal_lcia_ids where label = 'publication'),
    (select response #>> '{data,contentHash}' from portal_lcia_seal_response)
  ) #>> '{data,isCurrent}' = 'false',
  'readback reports a revoked binding as not publicly current'
);

reset role;
set local role anon;
select pg_catalog.set_config('request.jwt.claim.role', 'anon', true);
select pg_catalog.set_config('request.jwt.claims', '{"role":"anon"}', true);
select extensions.is(
  api.portal_get_published_lcia_values_v1(
    'process_all_impacts',
    '[{"id":"52710000-0000-4000-8000-000000000101","version":"01.00.000"}]'::jsonb,
    null, null, 50
  ),
  null::jsonb,
  'revoked projection publication is immediately unavailable to anon'
);

reset role;
select extensions.is(
  (select count(*) from private.portal_lcia_projection_values
   where projection_id = (select id from portal_lcia_ids where label = 'stage')),
  4::bigint,
  'revocation hides values while retaining all four immutable evidence rows'
);

select * from extensions.finish();
rollback;
