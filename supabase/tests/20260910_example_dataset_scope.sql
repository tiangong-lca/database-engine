-- Database #636: rollback-only examples across all seven entity kinds.
begin;
create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, auth;
select no_plan();

insert into auth.users(id, aud, role, email, raw_app_meta_data, raw_user_meta_data)
values ('63600000-0000-4000-8000-000000000001','authenticated','authenticated','example-reader@example.invalid','{}','{}'),
       ('63600000-0000-4000-8000-000000000002','authenticated','authenticated','example-owner@example.invalid','{}','{}');

-- Construct search projections directly, as in the version-search regression.
-- Keep constraints/RLS/indexes and the example write guard active for assertions.
do $$
declare t text; v text; s integer; k integer;
begin
  foreach t in array array['contacts','sources','unitgroups','flowproperties','flows','processes','lifecyclemodels'] loop
    execute format('alter table public.%I disable trigger user',t);
    for k in 1..3 loop
      v := '01.00.00' || k;
      s := case when k=3 then 100 else -1 end;
      execute format('insert into public.%I(id,version,state_code,json,json_ordered,user_id,search_text,embedding_ft)
        values ($1,$2,$3,$4,$4::json,$5,$6,$7::extensions.vector)',t)
      using '63600000-0000-4000-8000-000000000011'::uuid,v,s,
        '{"testName":"ExampleNeedle","ref":"63600000-0000-4000-8000-000000000099"}'::jsonb,
        '63600000-0000-4000-8000-000000000002'::uuid,array['ExampleNeedle'],
        '[' || array_to_string(array_prepend('1',array_fill('0'::text,array[1023])),',') || ']';
    end loop;
    execute format('insert into public.%I(id,version,state_code,json,json_ordered,user_id,search_text)
       values ($1,$2,0,$3,$3::json,$4,$5)',t)
      using '63600000-0000-4000-8000-000000000012'::uuid,'01.00.001','{"testName":"ExampleNeedle"}'::jsonb,
        '63600000-0000-4000-8000-000000000002'::uuid,array['ExampleNeedle'];
    execute format('alter table public.%I enable trigger protect_example_dataset_write',t);
  end loop;
end $$;

create function pg_temp.example_scope_assertions() returns setof text language plpgsql as $$
declare t text; list_rpc text; n bigint; v text; total bigint;
begin
  for t,list_rpc in select * from (values
    ('contacts','get_latest_contact_versions'),('sources','get_latest_source_versions'),
    ('unitgroups','get_latest_unitgroup_versions'),('flowproperties','get_latest_flowproperty_versions'),
    ('flows','get_latest_flow_versions'),('processes','get_latest_process_versions'),
    ('lifecyclemodels','get_latest_lifecyclemodel_versions')) names(t,rpc) loop
    execute format('select count(*) from public.%I where state_code=-1',t) into n;
    return next extensions.is(n,2::bigint,t || ': examples readable across owners');
    execute format('select count(*) from public.%I where state_code=0',t) into n;
    return next extensions.is(n,0::bigint,t || ': foreign draft remains private');
    execute format('select version,total_count from api.%I(data_source=>''ex'',page_size=>1)',list_rpc) into v,total;
    return next extensions.is(v,'01.00.002',t || ': latest version selected within example scope');
    return next extensions.is(total,1::bigint,t || ': count excludes other states and versions');
    execute format('select count(*) from api.%I(data_source=>''ex'',page_size=>1,page_current=>2)',list_rpc) into n;
    return next extensions.is(n,0::bigint,t || ': second page is empty');
    execute format('select version from api.%I(data_source=>''tg'')',list_rpc) into v;
    return next extensions.is(v,'01.00.003',t || ': open data unchanged');
    execute format('select version from api.%I(query_text=>''63600000-0000-4000-8000-000000000011'',data_source=>''ex'')','search_'||t) into v;
    return next extensions.is(v,'01.00.002',t || ': exact UUID search uses example version');
    execute format('select version from api.%I(query_text=>''ExampleNeedle'',data_source=>''ex'')','search_'||t) into v;
    return next extensions.is(v,'01.00.002',t || ': lexical search uses example version');
    execute format('select count(*) from api.%I(query_text=>''ExampleNeedle'',query_embedding=>$1,data_source=>''ex'',lexical_weight=>1,semantic_weight=>0)', 'hybrid_search_'||t)
      into n using '[' || array_to_string(array_prepend('1',array_fill('0'::text,array[1023])),',') || ']';
    return next extensions.is(n,1::bigint,t || ': hybrid latest scope');
    execute format('select count(*) from api.%I(query_text=>''unmatched lexical term'',query_embedding=>$1,data_source=>''ex'',lexical_weight=>0,semantic_weight=>1)', 'hybrid_search_'||t)
      into n using '[' || array_to_string(array_prepend('1',array_fill('0'::text,array[1023])),',') || ']';
    return next extensions.is(n,1::bigint,t || ': semantic-only search finds latest example');
  end loop;
end $$;

grant execute on function pg_temp.example_scope_assertions() to authenticated;
set local role authenticated;
select set_config('request.jwt.claims','{"sub":"63600000-0000-4000-8000-000000000001","role":"authenticated"}',true);
select set_config('request.jwt.claim.sub','63600000-0000-4000-8000-000000000001',true);
select * from pg_temp.example_scope_assertions();
select is((select count(*) from api.search_dataset_json_uuid_mentions(
 p_uuid=>'63600000-0000-4000-8000-000000000099',p_data_source=>'ex',p_limit=>50)),7::bigint,
 'UUID mention search spans all seven example types');

select is((select count(*) from api.hybrid_search_process_versions_v2(
 query_text=>'ExampleNeedle',query_embedding=>'['||array_to_string(array_prepend('1',array_fill('0'::text,array[1023])),',')||']',
 data_source=>'ex',state_code_filter=>-1,lexical_weight=>1,semantic_weight=>0)),2::bigint,'Process matched versions stay inside ex');
select is((select count(*) from api.hybrid_search_flow_versions_v2(
 query_text=>'ExampleNeedle',query_embedding=>'['||array_to_string(array_prepend('1',array_fill('0'::text,array[1023])),',')||']',
 data_source=>'ex',state_code_filter=>-1,lexical_weight=>1,semantic_weight=>0)),2::bigint,'Flow matched versions stay inside ex');

reset role;
-- Signed-in owners are denied even through a definer write path.
select set_config('request.jwt.claim.sub','63600000-0000-4000-8000-000000000002',true);
select set_config('request.jwt.claims','{"sub":"63600000-0000-4000-8000-000000000002","role":"authenticated"}',true);
select throws_ok($q$update public.processes set json='{}' where state_code=-1$q$,'42501','EXAMPLE_DATASET_READ_ONLY','example original cannot be overwritten');
select throws_ok($q$delete from public.lifecyclemodels where state_code=-1$q$,'42501','EXAMPLE_DATASET_READ_ONLY','example model cannot be deleted through a bundle');
select is((api.cmd_dataset_save_draft('contacts','63600000-0000-4000-8000-000000000011','01.00.002','{}')->>'status')::integer,403,'owner draft-save refuses example');

select set_config('request.jwt.claim.sub','',true);
select set_config('request.jwt.claims','{"role":"service_role"}',true);
select set_config('request.jwt.claim.role','service_role',true);
select is((api.svc_tidas_package_export_enqueue(
 '63600000-0000-4000-8000-000000000001','selected_roots',
 '[{"table":"processes","id":"63600000-0000-4000-8000-000000000011","version":"01.00.002"}]',
 repeat('e',64),'{}','63600000-0000-4000-8000-000000000088','example-export-636')->>'ok')::boolean,true,
 'ordinary actor can enqueue selected example export through the service facade');
select is(api.svc_tidas_package_export_enqueue(
 '63600000-0000-4000-8000-000000000001','selected_roots',
 '[{"table":"processes","id":"63600000-0000-4000-8000-000000000012","version":"01.00.001"}]',
 repeat('f',64),'{}','63600000-0000-4000-8000-000000000089','private-export-636')->>'code','ROOT_EXPORT_FORBIDDEN',
 'export scope still rejects foreign private drafts');

select set_config('request.jwt.claim.role','anon',true);
set local role anon;
select set_config('request.jwt.claims','{"role":"anon"}',true);
select set_config('request.jwt.claim.sub','',true);
select is((select count(*) from public.processes where state_code=-1),0::bigint,'anonymous table read excludes examples');
select is((select count(*) from api.get_latest_process_versions(data_source=>'ex')),0::bigint,'anonymous example list is empty');
select is((select count(*) from api.search_dataset_json_uuid_mentions(p_uuid=>'63600000-0000-4000-8000-000000000099',p_data_source=>'ex')),0::bigint,'anonymous definer search excludes examples');
reset role;
select * from finish();
rollback;
