begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;
select no_plan();

-- A minimal immutable current release.  The closure request must use this
-- manifest, rather than any live state-code rows, as its identity universe.
insert into auth.users (instance_id,id,aud,role,email,encrypted_password,email_confirmed_at,raw_app_meta_data,raw_user_meta_data,created_at,updated_at,is_sso_user,is_anonymous)
values ('00000000-0000-0000-0000-000000000000','c7220000-0000-4000-8000-000000000001','authenticated','authenticated','closure-e2e@example.com','x',now(),'{}','{}',now(),now(),false,false);
insert into public.users(id,raw_user_meta_data,contact) values ('c7220000-0000-4000-8000-000000000001','{}',null);
insert into public.teams(id,json,rank,is_public) values ('00000000-0000-0000-0000-000000000000','{"name":"System"}',0,false) on conflict(id) do nothing;
insert into public.roles(user_id,team_id,role) values ('c7220000-0000-4000-8000-000000000001','00000000-0000-0000-0000-000000000000','data_product_manager');

insert into public.lca_release_runs(id,release_version,selection_manifest_hash,input_manifest_hash,calculation_bundle_hash,calculation_bundle_ref,profile_lock_hash,publish_plan_hash,publish_plan,artifact_set_hash,release_manifest_hash,release_manifest,status,idempotency_key,request_hash,created_by)
values ('c7220000-0000-4000-8000-000000000010','77.00.001',repeat('a',64),repeat('b',64),repeat('c',64),'{}',repeat('d',64),repeat('e',64),'{}',repeat('f',64),repeat('9',64),'{}','published','closure-e2e-release',repeat('1',64),'c7220000-0000-4000-8000-000000000001');
insert into public.lca_release_approvals(id,release_run_id,publish_plan_hash,approval_hash,approved_by,approved_at,expires_at)
values ('c7220000-0000-4000-8000-000000000011','c7220000-0000-4000-8000-000000000010',repeat('e',64),repeat('2',64),'c7220000-0000-4000-8000-000000000001',now(),now()+interval '1 day');
insert into public.lca_release_publications(id,release_run_id,release_version,approval_id,approval_hash,publish_plan_hash,release_manifest_hash,artifact_set_hash,approved_by,executed_by,credential_fingerprint,idempotency_key,published_at)
values ('c7220000-0000-4000-8000-000000000012','c7220000-0000-4000-8000-000000000010','77.00.001','c7220000-0000-4000-8000-000000000011',repeat('2',64),repeat('e',64),repeat('9',64),repeat('f',64),'c7220000-0000-4000-8000-000000000001','c7220000-0000-4000-8000-000000000001',repeat('3',64),'closure-e2e-publication',now());
insert into public.lca_release_dataset_versions(release_run_id,dataset_type,dataset_role,dataset_uuid,dataset_version,source_process_uuid,source_process_version,version_significant_hash,semantic_hash,canonical_content_hash,artifact_ref)
values
('c7220000-0000-4000-8000-000000000010','process','unit_process','c7220000-0000-4000-8000-000000000020','01.00.000','c7220000-0000-4000-8000-000000000020','01.00.000',repeat('4',64),repeat('5',64),repeat('6',64),'{}'),
('c7220000-0000-4000-8000-000000000010','lciamethod','support','c7220000-0000-4000-8000-000000000021','01.00.000',null,null,repeat('7',64),repeat('8',64),repeat('9',64),'{}');

set local role authenticated;
select set_config('request.jwt.claim.role','authenticated',true);
select set_config('request.jwt.claim.sub','c7220000-0000-4000-8000-000000000001',true);

create temporary table closure_e2e_responses(result jsonb);
insert into closure_e2e_responses values
(public.cmd_lcia_scope_closure_check_request_v2('{"coverageMode":"subset","processes":[{"id":"c7220000-0000-4000-8000-000000000020","version":"01.00.000"}],"lciaMethods":[{"id":"c7220000-0000-4000-8000-000000000021","version":"01.00.000"}]}'::jsonb,'closure-e2e-a','{}')),
(public.cmd_lcia_scope_closure_check_request_v2('{"coverageMode":"subset","processes":[{"id":"c7220000-0000-4000-8000-000000000020","version":"01.00.000"}],"lciaMethods":[{"id":"c7220000-0000-4000-8000-000000000021","version":"01.00.000"}]}'::jsonb,'closure-e2e-b','{}'));

reset role;

select is((select count(*) from closure_e2e_responses where result->>'ok'='true'),2::bigint,'two explicit runs are accepted against the release');
select is((select count(distinct scan_execution_id) from public.lcia_scope_closure_checks where request_idempotency_token in ('closure-e2e-a','closure-e2e-b')),1::bigint,'two runs share one release-bound scan execution');
select is((select count(distinct data_snapshot_token) from public.lcia_scope_closure_checks where request_idempotency_token in ('closure-e2e-a','closure-e2e-b')),1::bigint,'two runs freeze the same exact release snapshot');
select is(public.cmd_lcia_scope_closure_check_request_v2('{"coverageMode":"subset","processes":[{"id":"c7220000-0000-4000-8000-000000000099","version":"01.00.000"}],"lciaMethods":[{"id":"c7220000-0000-4000-8000-000000000021","version":"01.00.000"}]}'::jsonb,'closure-e2e-live-only','{}')->>'code','invalid_closure_scope','live-only process identity is rejected outside the current release');

select * from finish();
rollback;
