begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(39);

select has_table('public', 'lcia_scope_closure_checks', 'closure checks are persisted');
select has_table('public', 'lcia_scope_closure_issues', 'closure issues are persisted');
select has_table('public', 'lcia_scope_closure_config', 'server-owned closure configuration is persisted');
select has_table('public', 'lcia_scope_closure_certificate_events', 'certificate invalidation is append-only');
select has_table('public', 'lcia_scope_closure_issue_occurrences', 'closure issue occurrences preserve source-level provenance');
select has_table('public', 'lcia_scope_closure_issue_roots', 'closure issue roots preserve affected-scope provenance');
select col_is_pk('public', 'lcia_scope_closure_checks', 'id', 'closure check has a primary key');
select col_is_pk('public', 'lcia_scope_closure_issues', 'id', 'closure issue has a primary key');
select ok((select relrowsecurity from pg_class where oid = 'public.lcia_scope_closure_checks'::regclass), 'closure checks enable RLS');
select ok((select relrowsecurity from pg_class where oid = 'public.lcia_scope_closure_issues'::regclass), 'closure issues enable RLS');
select ok((select relrowsecurity from pg_class where oid = 'public.lcia_scope_closure_issue_occurrences'::regclass), 'closure issue occurrences enable RLS');
select ok((select relrowsecurity from pg_class where oid = 'public.lcia_scope_closure_issue_roots'::regclass), 'closure issue roots enable RLS');
select ok(not has_table_privilege('authenticated', 'public.lcia_scope_closure_checks', 'select'), 'authenticated cannot directly read closure checks');
select ok(not has_table_privilege('authenticated', 'public.lcia_scope_closure_issues', 'select'), 'authenticated cannot directly read closure issues');

select has_function('public', 'cmd_lcia_scope_closure_check_request_v2', array['jsonb','text','jsonb'], 'closure request RPC accepts a server-normalized scope intent');
select has_function('public', 'svc_lcia_scope_closure_check_get_worker_input', array['uuid'], 'worker can read the frozen scope through a service-only RPC');
select has_function('public', 'svc_lcia_scope_closure_check_record_result_v2', array['uuid','uuid','uuid','text','text','jsonb','jsonb','jsonb','jsonb','text[]','uuid'], 'lease-fenced completion RPC exists');
select has_function('public', 'get_lcia_scope_closure_check', array['uuid'], 'closure read RPC exists');
select has_function('public', 'list_lcia_scope_closure_issues', array['uuid','uuid','integer'], 'closure issue keyset RPC exists');
select has_function('public', 'get_lcia_scope_closure_report_download', array['uuid'], 'report authorization RPC exists');
select has_function('public', 'svc_lcia_scope_closure_check_record_result', array['uuid','text','text','text','text','jsonb','text[]','uuid'], 'service closure result RPC cannot set certificate state');
select has_function('public', 'svc_lcia_scope_closure_certificate_event', array['uuid','text','text'], 'append-only certificate event RPC exists');
select has_function('public', 'get_task_summary_v2_feed', array['text','text[]','text[]','timestamp with time zone','timestamp with time zone','uuid','integer','boolean'], 'role-aware task feed RPC exists');
select has_function('public', 'cmd_lcia_result_build_request_v2', array['text','jsonb','text','text','jsonb','text','uuid','text','text','jsonb'], 'certificate-bound build RPC exists');

select ok(not has_function_privilege('authenticated', 'public.cmd_lcia_result_build_request_legacy(text,jsonb,text,text,jsonb,text,jsonb)', 'execute'), 'legacy build RPC is not callable by authenticated users');
select ok(has_function_privilege('authenticated', 'public.cmd_lcia_result_build_request_v2(text,jsonb,text,text,jsonb,text,uuid,text,text,jsonb)', 'execute'), 'certificate-bound build RPC is callable by authenticated users');
select ok(not has_function_privilege('service_role', 'public.svc_lcia_scope_closure_check_record_result(uuid,text,text,text,text,jsonb,text[],uuid)', 'execute'), 'unfenced service result RPC is no longer callable');
select ok(has_function_privilege('service_role', 'public.svc_lcia_scope_closure_certificate_event(uuid,text,text)', 'execute'), 'certificate event RPC is service-only');
select ok(not has_function_privilege('authenticated', 'public.cmd_lcia_scope_closure_check_request(text,text,text,jsonb)', 'execute'), 'hash-only closure request is not callable by authenticated users');
select ok(has_function_privilege('authenticated', 'public.cmd_lcia_scope_closure_check_request_v2(jsonb,text,jsonb)', 'execute'), 'normalized closure request is callable by authenticated users');
select ok(has_function_privilege('service_role', 'public.svc_lcia_scope_closure_check_get_worker_input(uuid)', 'execute'), 'frozen worker input is service-only');
select ok(has_function_privilege('service_role', 'public.svc_lcia_scope_closure_check_record_result_v2(uuid,uuid,uuid,text,text,jsonb,jsonb,jsonb,jsonb,text[],uuid)', 'execute'), 'lease-fenced result RPC is service-only');
select ok(has_function_privilege('authenticated', 'public.cmd_lcia_result_build_request(text,jsonb,text,text,jsonb,text,jsonb)', 'execute'), 'v1 build stays callable until the server feature flag requires certificates');

select ok(exists (select 1 from public.worker_job_kinds where job_kind = 'lcia.scope_closure_check' and worker_queue = 'solver' and default_visibility = 'operator' and task_center_category = 'data_product'), 'closure job kind has data product task metadata');
select ok(exists (select 1 from public.worker_job_kinds where job_kind = 'lcia_result.package_build' and task_center_category = 'data_product' and presenter_key = 'data_product.lcia_result_package_build.v1'), 'build kind has presenter metadata');
select ok(exists (select 1 from pg_indexes where schemaname = 'public' and indexname = 'lcia_scope_closure_checks_requested_updated_idx'), 'closure requester feed index exists');
select ok(exists (select 1 from pg_indexes where schemaname = 'public' and indexname = 'lcia_scope_closure_issues_check_id_idx'), 'closure issue keyset index exists');
select ok(exists (select 1 from pg_indexes where schemaname = 'public' and indexname = 'lcia_scope_closure_certificate_events_check_created_idx'), 'certificate event readback index exists');
select ok(exists (select 1 from pg_indexes where schemaname = 'public' and indexname = 'lcia_scope_closure_issue_occurrences_issue_idx'), 'occurrence pagination index exists');

select * from finish();
rollback;
