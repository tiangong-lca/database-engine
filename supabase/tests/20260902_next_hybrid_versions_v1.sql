-- Exact-version Next regression; no provider calls or non-local data.
begin;
create extension if not exists pgtap with schema extensions;
set local search_path=extensions,public,auth;
select extensions.no_plan();

create function pg_temp.version_vector(a real,b real) returns text language sql immutable as $$
  select '[' || a::text || ',' || b::text || ',' || array_to_string(array_fill('0'::text,array[1022]),',') || ']';
$$;

insert into auth.users(id,instance_id,aud,role,email,raw_app_meta_data,raw_user_meta_data,created_at,updated_at)
values
  ('60000000-0000-4000-8000-000000000901','00000000-0000-0000-0000-000000000000','authenticated','authenticated',
    'versions-owner@example.invalid','{"provider":"email","providers":["email"]}','{}',now(),now()),
  ('60000000-0000-4000-8000-000000000902','00000000-0000-0000-0000-000000000000','authenticated','authenticated',
    'versions-other@example.invalid','{"provider":"email","providers":["email"]}','{}',now(),now());
insert into private.users(id,raw_user_meta_data,contact) values
  ('60000000-0000-4000-8000-000000000901','{}',null),
  ('60000000-0000-4000-8000-000000000902','{}',null);
insert into private.teams(id,json,rank,is_public) values
  ('60000000-0000-4000-8000-000000000903','{"name":"Version Team"}',1,false),
  ('60000000-0000-4000-8000-000000000904','{"name":"Foreign Team"}',1,false);
insert into private.roles(user_id,team_id,role) values
  ('60000000-0000-4000-8000-000000000901','60000000-0000-4000-8000-000000000903','member');

alter table public.processes disable trigger user;
alter table public.flows disable trigger user;

create function pg_temp.next_version_fixture(
  k text,identity uuid,v text,s integer,label text,a real,b real,
  actor uuid default '60000000-0000-4000-8000-000000000902',
  team uuid default null,flow_type text default 'Product flow',region text default 'CN',
  category text default 'Resources'
) returns void language plpgsql as $$
declare document jsonb; name text;
begin
  if k='process' then name := 'processes';
  elsif k='flow' then name := 'flows';
  else raise exception 'invalid test kind'; end if;
  document := jsonb_build_object('testName',label,'region',region,
    'flowDataSet',jsonb_build_object(
      'modellingAndValidation',jsonb_build_object('LCIMethod',jsonb_build_object('typeOfDataSet',flow_type)),
      'flowInformation',jsonb_build_object('dataSetInformation',jsonb_build_object(
        'classificationInformation',jsonb_build_object('common:elementaryFlowCategorization',jsonb_build_object(
          'common:category',jsonb_build_array(jsonb_build_object('#text',category,'@level','0'))))))));
  execute format(
    'insert into public.%I(id,version,state_code,json,json_ordered,user_id,team_id,modified_at,
      extracted_md,search_text,embedding_ft,rule_verification)
     values($1,$2,$3,$4,$4::json,$5,$6,$7,$8,$9,$10::extensions.vector(1024),true)',name)
    using identity,v,s,document,actor,team,'2026-09-02 09:00:00+00'::timestamptz,
      'ENGLISH SOURCE EMBEDDING DOCUMENT',array[label,'électricité ' || label,'медь ' || label,'铜 ' || label],
      pg_temp.version_vector(a,b);
end;
$$;

do $$
declare k text; n integer;
begin
  foreach k in array array['process','flow'] loop
    perform pg_temp.next_version_fixture(k,'60000000-0000-4000-8000-000000000011','01.00.000',100,'HistoricNeedle',0,1);
    perform pg_temp.next_version_fixture(k,'60000000-0000-4000-8000-000000000011','01.00.001',100,'DifferentLatestName',1,0);
    perform pg_temp.next_version_fixture(k,'60000000-0000-4000-8000-000000000012','01.00.000',200,'MetadataNeedle',1,0);
    perform pg_temp.next_version_fixture(k,'60000000-0000-4000-8000-000000000013','01.00.000',0,'PrivateNeedle',1,0,
      '60000000-0000-4000-8000-000000000901','60000000-0000-4000-8000-000000000903');
    perform pg_temp.next_version_fixture(k,'60000000-0000-4000-8000-000000000013','01.00.001',20,'PrivateNeedle',1,0,
      '60000000-0000-4000-8000-000000000901','60000000-0000-4000-8000-000000000903');
    perform pg_temp.next_version_fixture(k,'60000000-0000-4000-8000-000000000014','01.00.000',0,'ForeignNeedle',1,0,
      '60000000-0000-4000-8000-000000000902','60000000-0000-4000-8000-000000000904');
    perform pg_temp.next_version_fixture(k,'60000000-0000-4000-8000-000000000015','01.00.000',0,'TeamNeedle',1,0,
      '60000000-0000-4000-8000-000000000902','60000000-0000-4000-8000-000000000903');
    for n in 1..250 loop
      perform pg_temp.next_version_fixture(k,('60000000-0000-4000-9000-' || lpad(n::text,12,'0'))::uuid,
        '01.00.000',100,'BudgetNeedle',1,0,
        '60000000-0000-4000-8000-000000000902',null,'Product flow',
        case when n<=245 then 'US' else 'ZZF' end);
    end loop;
  end loop;
  perform pg_temp.next_version_fixture('flow','60000000-0000-4000-8000-000000000021','01.00.000',100,'FlowFilterNeedle',
    1,0,'60000000-0000-4000-8000-000000000902',null,'Elementary flow','ZZI','Emissions');
  perform pg_temp.next_version_fixture('flow','60000000-0000-4000-8000-000000000022','01.00.000',100,'FlowFilterNeedle',
    1,0,'60000000-0000-4000-8000-000000000902',null,'Product flow','ZZI','Resources');
end $$;

create function pg_temp.next_version_search(
  k text,q text,vector_text text,filters jsonb default '{}'::jsonb,scope text default 'tg',
  size integer default 20,page integer default 1,lw double precision default 0.5,sw double precision default 0.5
) returns jsonb language plpgsql as $$
declare result jsonb;
begin
  if k not in ('process','flow') then raise exception 'invalid test kind'; end if;
  execute format('select coalesce(jsonb_agg(to_jsonb(result)),''[]''::jsonb)
    from api.%I($1,$2,$3,0.5,200,$6,$7,10,$4,$5,$8,array[$1]) as result',
    'hybrid_search_' || k || '_versions_v1')
    into result using q,vector_text,filters,scope,size,lw,sw,page;
  return result;
end $$;
grant execute on function pg_temp.version_vector(real,real) to anon,authenticated,api_internal_executor;
grant execute on function pg_temp.next_version_search(text,text,text,jsonb,text,integer,integer,double precision,double precision)
  to anon,authenticated;

create temporary table next_results(label text primary key,payload jsonb);
grant select,insert on next_results to anon,authenticated;

select extensions.ok(
  not exists(select 1 from pg_proc p,unnest(array['anon','authenticated','service_role']) as caller
    where p.oid in (
      'private.semantic_process_version_candidates_v1(text,text,double precision,integer,text)'::regprocedure,
      'private.semantic_flow_version_candidates_v1(text,text,double precision,integer,text)'::regprocedure,
      'private.lexical_version_candidates_v1(text,text,text[],jsonb,text)'::regprocedure
    ) and has_function_privilege(caller,p.oid,'EXECUTE')),
  'Next version candidate helpers are API-internal only');

grant api_internal_executor to postgres;
set local role api_internal_executor;
select extensions.is(
  (select version from private.lexical_version_candidates_v1(k,'HistoricNeedle',array['HistoricNeedle'],'{}','tg')),
  '01.00.000',k || ' lexical recall keeps the exact old match, not a latest alias'
) from unnest(array['process','flow']) as k;
select extensions.is(
  (select count(*) from private.lexical_version_candidates_v1(k,'BudgetNeedle',array['BudgetNeedle'],'{}','tg')),
  200::bigint,k || ' lexical branch retains at most 200 version candidates'
) from unnest(array['process','flow']) as k;
select extensions.is(
  (select count(*) from private.lexical_version_candidates_v1(k,'BudgetNeedle',array['BudgetNeedle'],'{"region":"ZZF"}','tg')),
  5::bigint,k || ' lexical filter runs before the 200-version cutoff'
) from unnest(array['process','flow']) as k;
select extensions.is(
  (select count(*) from private.semantic_process_version_candidates_v1(pg_temp.version_vector(1,0),'{"region":"ZZF"}',0.5,200,'tg')),
  5::bigint,'Process semantic filter runs before the 200-version cutoff');
select extensions.is(
  (select count(*) from private.semantic_flow_version_candidates_v1(pg_temp.version_vector(1,0),'{"region":"ZZF"}',0.5,200,'tg')),
  5::bigint,'Flow semantic filter runs before the 200-version cutoff');
reset role;

set local role anon;
insert into next_results
select k || '_split',pg_temp.next_version_search(k,'HistoricNeedle',pg_temp.version_vector(1,0),'{"region":"CN"}')
from unnest(array['process','flow']) as k;
select extensions.is(jsonb_array_length(payload),2,label || ' retains both matching versions') from next_results;
select extensions.is(
  (select array_agg(item ->> 'version' order by item ->> 'version') from jsonb_array_elements(payload) as item),
  array['01.00.000','01.00.001']::text[],label || ' fuses only by id AND version'
) from next_results;
select extensions.ok(
  (select bool_and(
    item ->> 'version' = case item #>> '{json,testName}' when 'HistoricNeedle' then '01.00.000' else '01.00.001' end
    and (item ->> 'total_count')::integer=2
  ) from jsonb_array_elements(payload) as item),
  label || ' content, exact identity and version-count pagination agree'
) from next_results;

select extensions.is(
  jsonb_array_length(pg_temp.next_version_search(k,'PrivateNeedle',pg_temp.version_vector(1,0),'{}',scope)),
  0,k || ' anonymous ' || scope || ' scope is empty'
) from unnest(array['process','flow']) as k cross join unnest(array['my','te']) as scope;

select extensions.is(
  pg_temp.next_version_search(k,'MetadataNeedle',pg_temp.version_vector(1,0),'{"region":"CN"}','co') #>> '{0,id}',
  '60000000-0000-4000-8000-000000000012',k || ' commercial public scope remains state 200 only'
) from unnest(array['process','flow']) as k;
select extensions.is(
  jsonb_array_length(pg_temp.next_version_search(k,query,pg_temp.version_vector(0,0),'{}','tg',20,1,1,0)),
  1,k || ' full-text source retains original ' || language || ' fields'
) from unnest(array['process','flow']) as k cross join
  (values('fr','électricité HistoricNeedle'),('ru','медь HistoricNeedle'),('zh','铜 HistoricNeedle')) as languages(language,query);

select extensions.is(
  jsonb_array_length(pg_temp.next_version_search('flow','FlowFilterNeedle',pg_temp.version_vector(1,0),
    '{"region":"ZZI","flowType":"Product flow","asInput":true}')),
  1,'Flow type and input filters still constrain both exact-version branches');
select extensions.is(
  pg_temp.next_version_search('flow','FlowFilterNeedle',pg_temp.version_vector(1,0),
    '{"region":"ZZI","asInput":true}') #>> '{0,id}',
  '60000000-0000-4000-8000-000000000022','Flow input selection continues to exclude emissions');

select extensions.throws_ok(
  $$select * from api.hybrid_search_process_versions_v1('q',pg_temp.version_vector(1,0),'{}',0.5,5000)$$,
  '22023','invalid version search request','the new API cannot silently restore a 5000-candidate pool');

reset role;
select set_config('request.jwt.claim.sub','60000000-0000-4000-8000-000000000901',true);
select set_config('request.jwt.claim.role','authenticated',true);
set local role authenticated;
select extensions.is(
  jsonb_array_length(pg_temp.next_version_search(k,'PrivateNeedle',pg_temp.version_vector(1,0),'{}','my')),
  2,k || ' actor-owned private versions stay visible in my scope'
) from unnest(array['process','flow']) as k;
select extensions.ok(
  not exists(select 1 from jsonb_array_elements(pg_temp.next_version_search(
    k,'ForeignNeedle',pg_temp.version_vector(1,0),'{}','my')) as item
    where item ->> 'id' in ('60000000-0000-4000-8000-000000000014','60000000-0000-4000-8000-000000000015')),
  k || ' my scope never leaks other actors, even within a readable team'
) from unnest(array['process','flow']) as k;
select extensions.is(
  jsonb_array_length(pg_temp.next_version_search(k,'TeamNeedle',pg_temp.version_vector(1,0),'{}','te')),
  3,k || ' semantic team scope retains all exact versions within current membership'
) from unnest(array['process','flow']) as k;
select extensions.ok(
  not exists(select 1 from jsonb_array_elements(pg_temp.next_version_search(
    k,'ForeignNeedle',pg_temp.version_vector(1,0),'{}','te')) as item
    where item ->> 'id'='60000000-0000-4000-8000-000000000014'),
  k || ' team scope continues to exclude foreign teams'
) from unnest(array['process','flow']) as k;

reset role;
select * from extensions.finish();
rollback;
