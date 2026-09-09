begin;
create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, auth;
select no_plan();

-- Synthetic, rollback-only fixtures. Suppress unrelated derivative/authoring
-- triggers while constructing exact-version task relationships; RPC security,
-- constraints, indexes and policies remain active for all assertions.
set local session_replication_role = replica;
insert into auth.users (id, aud, role, email, raw_app_meta_data, raw_user_meta_data)
select ('63200000-0000-4000-8000-' || lpad(n::text,12,'0'))::uuid,
 'authenticated', 'authenticated', 'review-search-' || n || '@example.com', '{}', '{}'
from generate_series(1,4) n;
insert into private.teams (id, json, rank, is_public)
values ('00000000-0000-0000-0000-000000000000','{}',0,false) on conflict do nothing;
insert into private.roles (user_id,team_id,role)
values
 ('63200000-0000-4000-8000-000000000002','00000000-0000-0000-0000-000000000000','review-admin'),
 ('63200000-0000-4000-8000-000000000003','00000000-0000-0000-0000-000000000000','review-member'),
 ('63200000-0000-4000-8000-000000000004','00000000-0000-0000-0000-000000000000','review-member');

do $$
declare t text; s integer; k text; d uuid;
begin
 foreach t in array array['contacts','sources','unitgroups','flowproperties','flows','processes','lifecyclemodels'] loop
  foreach s in array array[-1,0,1,2] loop
   d := md5('review-search-dataset-' || s)::uuid;
   execute format('insert into public.%I(id,version,json,json_ordered,user_id,state_code,search_text) values ($1,$2,$3,$3,$4,20,$5)',t)
    using d,'01.00.000','{}'::jsonb,'63200000-0000-4000-8000-000000000001'::uuid,
      array['变压器 wind 49.5MW', 'technical details', 'literal (sample)'];
   execute format('insert into public.%I(id,version,json,json_ordered,user_id,state_code,search_text) values ($1,$2,$3,$3,$4,0,$5)',t)
    using d,'02.00.000','{}'::jsonb,'63200000-0000-4000-8000-000000000001'::uuid,array['newonly'];
   foreach k in array array['root','reference'] loop
    insert into private.reviews(id,data_id,data_version,state_code,review_kind,target_table,
      submitted_revision_checksum,target_owner_id,reviewer_id,json,created_at,modified_at)
    values(md5(t || s || k)::uuid,d,'01.00.000',s,k,t,repeat('a',64),
      '63200000-0000-4000-8000-000000000001',
      case when s=0 then '[]'::jsonb else '["63200000-0000-4000-8000-000000000003"]'::jsonb end,
      '{"user":{"name":"submitter-only"}}','2026-09-01','2026-09-01');
    if s<>0 then
     insert into private.comments(review_id,reviewer_id,state_code,json)
     values(md5(t || s || k)::uuid,'63200000-0000-4000-8000-000000000003',
       case when s=1 then 0 else s end,'{}');
    end if;
   end loop;
  end loop;
 end loop;
end $$;
set local session_replication_role = origin;

select ok(not has_function_privilege('authenticated','private.review_search_dataset_versions_v1(text,text)','EXECUTE'), 'browser cannot invoke raw lexical helper');
select ok(not has_function_privilege('anon','api.qry_review_get_admin_queue_items_v4(text,integer,integer,text,text,text,text,text)','EXECUTE'), 'anonymous admin RPC denied');
select ok(not has_function_privilege('service_role','api.qry_review_get_member_queue_items_v4(text,integer,integer,text,text,text,text,text)','EXECUTE'), 'no inherited service RPC grant');
select is((select count(*) from private.api_capability_grants where routine_identity like 'api.qry_review_get_%_queue_items_v4(%' and capability_id='NX-REV-01' and allow_authenticated and not allow_anon and not allow_service_role),2::bigint,'both exact v4 capabilities registered');

set local role authenticated;
select set_config('request.jwt.claim.sub','63200000-0000-4000-8000-000000000002',true);
select set_config('request.jwt.claim.role','authenticated',true);
select is((select count(*) from api.qry_review_get_admin_queue_items_v4(p_status=>'unassigned',p_query=>'变压器')),14::bigint,'all seven types and both task kinds match');
select is((select count(*) from api.qry_review_get_admin_queue_items_v4(p_status=>'assigned',p_query=>'wind')),14::bigint,'assigned tasks searchable');
select is((select count(*) from api.qry_review_get_admin_queue_items_v4(p_status=>'admin-rejected',p_query=>'wind')),14::bigint,'rejected tasks searchable');
select is((select count(*) from api.qry_review_get_admin_queue_items_v4(p_status=>'unassigned',p_query=>'newonly')),0::bigint,'latest version cannot lend evidence to reviewed old version');
select is((select count(*) from api.qry_review_get_admin_queue_items_v4(p_status=>'unassigned',p_query=>'submitter-only')),0::bigint,'review metadata is outside dataset search scope');
select is((select count(*) from api.qry_review_get_admin_queue_items_v4(p_status=>'unassigned',p_display_mode=>'model_process',p_query=>'wind')),4::bigint,'model/process filter intersects text matches');
select is((select count(*) from api.qry_review_get_admin_queue_items_v4(p_status=>'unassigned',p_display_mode=>'other',p_query=>'wind')),10::bigint,'foundation/flow filter intersects text matches');
select is((select count(*) from api.qry_review_get_admin_queue_items_v4(p_status=>'unassigned',p_display_mode=>'other',p_target_table=>'processes',p_query=>'wind')),0::bigint,'conflicting valid filters have empty intersection');
select is((select count(*) from api.qry_review_get_admin_queue_items_v4(p_status=>'unassigned',p_target_table=>'processes',p_query=>'wind')),2::bigint,'table identity retained when dataset UUID is shared across tables');
select is((select count(*) from api.qry_review_get_admin_queue_items_v4(p_status=>'unassigned',p_query=>md5('review-search-dataset-0')::uuid::text)),14::bigint,'canonical UUID exact search is handled separately');
select is((select count(*) from api.qry_review_get_admin_queue_items_v4(p_status=>'unassigned',p_query=>'no-match')),0::bigint,'no-match query returns no tasks');
select is((select total_count from api.qry_review_get_admin_queue_items_v4(p_status=>'unassigned',p_query=>'wind',p_page_size=>3,p_page=>2) limit 1),14::bigint,'count is global before pagination');
select is((select count(*) from api.qry_review_get_admin_queue_items_v4(p_status=>'unassigned',p_query=>'wind',p_page_size=>3,p_page=>2)),3::bigint,'page has exact requested size');
select results_eq(
 $$select id from api.qry_review_get_admin_queue_items_v4(p_status=>'unassigned',p_query=>'wind',p_page_size=>3,p_page=>2)$$,
 $$select id from api.qry_review_get_admin_queue_items_v4(p_status=>'unassigned',p_query=>'wind') offset 3 limit 3$$,
 'stable ID tie-break produces consistent pages');
select results_eq(
 $$select * from api.qry_review_get_admin_queue_items_v4(p_status=>'unassigned',p_query=>'   ')$$,
 $$select * from api.qry_review_get_admin_queue_items_v3(p_status=>'unassigned')$$,
 'empty query matches v3 including DTO and total');
select throws_ok($$select * from api.qry_review_get_admin_queue_items_v4(p_query=>repeat('x',1001))$$,'22023','REVIEW_QUERY_TOO_LONG','query length is bounded');
select throws_ok($$select * from api.qry_review_get_admin_queue_items_v4(p_target_table=>'invalid',p_query=>'wind')$$,'22023','INVALID_REVIEW_TARGET_TABLE','unknown type rejected');

select set_config('request.jwt.claim.sub','63200000-0000-4000-8000-000000000003',true);
select is((select count(*) from api.qry_review_get_admin_queue_items_v4(p_query=>'wind')),0::bigint,'member cannot search administrator queue');
select is((select count(*) from api.qry_review_get_member_queue_items_v4(p_status=>'pending',p_query=>'wind')),14::bigint,'member searches assigned pending tasks');
select is((select count(*) from api.qry_review_get_member_queue_items_v4(p_status=>'reviewed',p_query=>'wind')),14::bigint,'member searches reviewed tasks');
select is((select count(*) from api.qry_review_get_member_queue_items_v4(p_status=>'reviewer-rejected',p_query=>'wind')),14::bigint,'member searches rejected tasks');
select results_eq(
 $$select * from api.qry_review_get_member_queue_items_v4(p_status=>'pending',p_query=>null)$$,
 $$select * from api.qry_review_get_member_queue_items_v3(p_status=>'pending')$$,
 'empty member query preserves v3');
select is((select total_count from api.qry_review_get_member_queue_items_v4(p_status=>'pending',p_query=>'wind',p_page_size=>2,p_page=>3) limit 1),14::bigint,'member count precedes pagination');
select set_config('request.jwt.claim.sub','63200000-0000-4000-8000-000000000004',true);
select is((select count(*) from api.qry_review_get_member_queue_items_v4(p_query=>'wind')),0::bigint,'unassigned member sees no matching tasks or count');
select set_config('request.jwt.claim.sub','63200000-0000-4000-8000-000000000001',true);
select is((select count(*) from api.qry_review_get_member_queue_items_v4(p_query=>'wind')),0::bigint,'ordinary data owner is not a reviewer');
reset role;

-- Compare the lexical kernel with the exact existing data-list predicates,
-- including array fragments, whitespace, syntax characters and mixed languages.
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('变压器 wind','contacts') order by data_id,data_version$$, $$select id,version from public.contacts where search_text &@~ '变压器 wind' order by id,version$$, 'contacts data-list lexical parity: 变压器 wind');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('  wind   49.5MW  ','contacts') order by data_id,data_version$$, $$select id,version from public.contacts where search_text &@~ '  wind   49.5MW  ' order by id,version$$, 'contacts data-list lexical parity:   wind   49.5MW  ');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('literal (sample)','contacts') order by data_id,data_version$$, $$select id,version from public.contacts where search_text &@~ 'literal (sample)' order by id,version$$, 'contacts data-list lexical parity: literal (sample)');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('WIND','contacts') order by data_id,data_version$$, $$select id,version from public.contacts where search_text &@~ 'WIND' order by id,version$$, 'contacts data-list lexical parity: WIND');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('wind OR newonly','contacts') order by data_id,data_version$$, $$select id,version from public.contacts where search_text &@~ 'wind OR newonly' order by id,version$$, 'contacts data-list lexical parity: wind OR newonly');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('变压器 wind','sources') order by data_id,data_version$$, $$select id,version from public.sources where search_text &@~ '变压器 wind' order by id,version$$, 'sources data-list lexical parity: 变压器 wind');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('  wind   49.5MW  ','sources') order by data_id,data_version$$, $$select id,version from public.sources where search_text &@~ '  wind   49.5MW  ' order by id,version$$, 'sources data-list lexical parity:   wind   49.5MW  ');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('literal (sample)','sources') order by data_id,data_version$$, $$select id,version from public.sources where search_text &@~ 'literal (sample)' order by id,version$$, 'sources data-list lexical parity: literal (sample)');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('WIND','sources') order by data_id,data_version$$, $$select id,version from public.sources where search_text &@~ 'WIND' order by id,version$$, 'sources data-list lexical parity: WIND');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('wind OR newonly','sources') order by data_id,data_version$$, $$select id,version from public.sources where search_text &@~ 'wind OR newonly' order by id,version$$, 'sources data-list lexical parity: wind OR newonly');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('变压器 wind','unitgroups') order by data_id,data_version$$, $$select id,version from public.unitgroups where search_text &@~ '变压器 wind' order by id,version$$, 'unitgroups data-list lexical parity: 变压器 wind');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('  wind   49.5MW  ','unitgroups') order by data_id,data_version$$, $$select id,version from public.unitgroups where search_text &@~ '  wind   49.5MW  ' order by id,version$$, 'unitgroups data-list lexical parity:   wind   49.5MW  ');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('literal (sample)','unitgroups') order by data_id,data_version$$, $$select id,version from public.unitgroups where search_text &@~ 'literal (sample)' order by id,version$$, 'unitgroups data-list lexical parity: literal (sample)');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('WIND','unitgroups') order by data_id,data_version$$, $$select id,version from public.unitgroups where search_text &@~ 'WIND' order by id,version$$, 'unitgroups data-list lexical parity: WIND');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('wind OR newonly','unitgroups') order by data_id,data_version$$, $$select id,version from public.unitgroups where search_text &@~ 'wind OR newonly' order by id,version$$, 'unitgroups data-list lexical parity: wind OR newonly');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('变压器 wind','flowproperties') order by data_id,data_version$$, $$select id,version from public.flowproperties where search_text &@~ '变压器 wind' order by id,version$$, 'flowproperties data-list lexical parity: 变压器 wind');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('  wind   49.5MW  ','flowproperties') order by data_id,data_version$$, $$select id,version from public.flowproperties where search_text &@~ '  wind   49.5MW  ' order by id,version$$, 'flowproperties data-list lexical parity:   wind   49.5MW  ');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('literal (sample)','flowproperties') order by data_id,data_version$$, $$select id,version from public.flowproperties where search_text &@~ 'literal (sample)' order by id,version$$, 'flowproperties data-list lexical parity: literal (sample)');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('WIND','flowproperties') order by data_id,data_version$$, $$select id,version from public.flowproperties where search_text &@~ 'WIND' order by id,version$$, 'flowproperties data-list lexical parity: WIND');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('wind OR newonly','flowproperties') order by data_id,data_version$$, $$select id,version from public.flowproperties where search_text &@~ 'wind OR newonly' order by id,version$$, 'flowproperties data-list lexical parity: wind OR newonly');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('变压器 wind','flows') order by data_id,data_version$$, $$select id,version from public.flows where search_text &@~| private.pgroonga_escape_query_terms(array['变压器 wind']) order by id,version$$, 'flows data-list lexical parity: 变压器 wind');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('  wind   49.5MW  ','flows') order by data_id,data_version$$, $$select id,version from public.flows where search_text &@~| private.pgroonga_escape_query_terms(array['  wind   49.5MW  ']) order by id,version$$, 'flows data-list lexical parity:   wind   49.5MW  ');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('literal (sample)','flows') order by data_id,data_version$$, $$select id,version from public.flows where search_text &@~| private.pgroonga_escape_query_terms(array['literal (sample)']) order by id,version$$, 'flows data-list lexical parity: literal (sample)');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('WIND','flows') order by data_id,data_version$$, $$select id,version from public.flows where search_text &@~| private.pgroonga_escape_query_terms(array['WIND']) order by id,version$$, 'flows data-list lexical parity: WIND');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('wind OR newonly','flows') order by data_id,data_version$$, $$select id,version from public.flows where search_text &@~| private.pgroonga_escape_query_terms(array['wind OR newonly']) order by id,version$$, 'flows data-list lexical parity: wind OR newonly');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('变压器 wind','processes') order by data_id,data_version$$, $$select id,version from public.processes where search_text &@~| private.pgroonga_escape_query_terms(array['变压器 wind']) order by id,version$$, 'processes data-list lexical parity: 变压器 wind');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('  wind   49.5MW  ','processes') order by data_id,data_version$$, $$select id,version from public.processes where search_text &@~| private.pgroonga_escape_query_terms(array['  wind   49.5MW  ']) order by id,version$$, 'processes data-list lexical parity:   wind   49.5MW  ');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('literal (sample)','processes') order by data_id,data_version$$, $$select id,version from public.processes where search_text &@~| private.pgroonga_escape_query_terms(array['literal (sample)']) order by id,version$$, 'processes data-list lexical parity: literal (sample)');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('WIND','processes') order by data_id,data_version$$, $$select id,version from public.processes where search_text &@~| private.pgroonga_escape_query_terms(array['WIND']) order by id,version$$, 'processes data-list lexical parity: WIND');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('wind OR newonly','processes') order by data_id,data_version$$, $$select id,version from public.processes where search_text &@~| private.pgroonga_escape_query_terms(array['wind OR newonly']) order by id,version$$, 'processes data-list lexical parity: wind OR newonly');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('变压器 wind','lifecyclemodels') order by data_id,data_version$$, $$select id,version from public.lifecyclemodels where search_text &@~| private.pgroonga_escape_query_terms(array['变压器 wind']) order by id,version$$, 'lifecyclemodels data-list lexical parity: 变压器 wind');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('  wind   49.5MW  ','lifecyclemodels') order by data_id,data_version$$, $$select id,version from public.lifecyclemodels where search_text &@~| private.pgroonga_escape_query_terms(array['  wind   49.5MW  ']) order by id,version$$, 'lifecyclemodels data-list lexical parity:   wind   49.5MW  ');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('literal (sample)','lifecyclemodels') order by data_id,data_version$$, $$select id,version from public.lifecyclemodels where search_text &@~| private.pgroonga_escape_query_terms(array['literal (sample)']) order by id,version$$, 'lifecyclemodels data-list lexical parity: literal (sample)');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('WIND','lifecyclemodels') order by data_id,data_version$$, $$select id,version from public.lifecyclemodels where search_text &@~| private.pgroonga_escape_query_terms(array['WIND']) order by id,version$$, 'lifecyclemodels data-list lexical parity: WIND');
select results_eq($$select data_id, data_version from private.review_search_dataset_versions_v1('wind OR newonly','lifecyclemodels') order by data_id,data_version$$, $$select id,version from public.lifecyclemodels where search_text &@~| private.pgroonga_escape_query_terms(array['wind OR newonly']) order by id,version$$, 'lifecyclemodels data-list lexical parity: wind OR newonly');

-- Nullable projections produce no lexical matches, while exact UUID lookup
-- remains available. Only change synthetic rows, with rollback at end.
set local session_replication_role = replica;
update public.flows set search_text=null where version='01.00.000';
set local session_replication_role = origin;
select is((select count(*) from private.review_search_dataset_versions_v1('wind','flows')),0::bigint,'missing projection has no lexical match');
select is((select count(*) from private.review_search_dataset_versions_v1(md5('review-search-dataset-0')::uuid::text,'flows')),2::bigint,'UUID lookup includes exact source versions without projection');
select * from finish();
rollback;
