begin;
create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, auth;


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


set local session_replication_role = replica;
do $$
declare t text;
begin
 foreach t in array array['contacts','sources','unitgroups','flowproperties','flows','processes','lifecyclemodels'] loop
 execute format('insert into public.%I(id,version,json,json_ordered,user_id,state_code,search_text) select md5(''bench'' || n)::uuid,''01.00.000'',''{}'',''{}'',''63200000-0000-4000-8000-000000000001'',0,array[''bulk corpus unrelated'',case when n %% 1000 = 0 then ''needle632'' else ''background632'' end] from generate_series(1,5000) n',t);
 execute format('analyze public.%I',t);
 end loop;
end $$;
set local session_replication_role = origin;
-- Source lexical plans must naturally use the existing PGroonga index.
explain (analyze,buffers) select id,version from public.processes where search_text &@~| private.pgroonga_escape_query_terms(array['needle632']);
explain (analyze,buffers) select id,version from public.contacts where search_text &@~ 'needle632';
set local role authenticated;
select set_config('request.jwt.claim.sub','63200000-0000-4000-8000-000000000002',true);
explain (analyze,buffers) select * from api.qry_review_get_admin_queue_items_v4(p_status=>'unassigned',p_query=>'wind');
reset role;
create temporary table timings(scenario text, milliseconds double precision);
do $$
declare q text; started timestamptz; i integer;
begin
 foreach q in array array['wind','needle632','background632','missing632',''] loop
  for i in 1..21 loop
   started := clock_timestamp();
   perform * from api.qry_review_get_admin_queue_items_v4(p_status=>'unassigned',p_query=>q);
   if i>1 then insert into timings values(q,extract(epoch from clock_timestamp()-started)*1000); end if;
  end loop;
 end loop;
end $$;
select scenario,count(*) as samples,round(percentile_cont(0.95) within group(order by milliseconds)::numeric,2) as p95_ms from timings group by scenario order by scenario;
rollback;
