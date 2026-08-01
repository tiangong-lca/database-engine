begin;
create extension if not exists pgtap with schema extensions;
select plan(61);

select has_table('public', name, 'Expand retains public physical table ' || name)
from unnest(array['comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users']) as name;

select has_view('private', name, 'private projection exists for ' || name)
from unnest(array['comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users']) as name;

select has_view('api', name, 'versioned api projection exists for ' || name)
from unnest(array['review_comments_v1','identity_center_processed_events_v1','identity_center_users_v1','notifications_v1','reviews_v1','team_roles_v1','teams_v1','user_profiles_v1']) as name;

select ok(
  not exists (
    select 1
    from information_schema.role_table_grants
    where table_schema = 'private'
      and table_name in ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users')
      and grantee in ('PUBLIC','anon','authenticated')
  ),
  'browser roles have zero private projection grants'
);

select ok(
  not has_schema_privilege('anon', 'private', 'USAGE')
  and not has_schema_privilege('authenticated', 'private', 'USAGE'),
  'browser roles have no private schema transport'
);

select ok(
  (select bool_and((c.reloptions @> array['security_invoker=true']) is true)
   from pg_class c join pg_namespace n on n.oid = c.relnamespace
   where n.nspname in ('api','private')
     and c.relname in ('comments','identity_center_processed_events','identity_center_users','notifications','reviews','roles','teams','users','review_comments_v1','identity_center_processed_events_v1','identity_center_users_v1','notifications_v1','reviews_v1','team_roles_v1','teams_v1','user_profiles_v1')),
  'all identity/collaboration projections are security invokers'
);

select ok(
  not exists (
    select 1
    from (values
      ('review_comments_v1','comments'), ('notifications_v1','notifications'),
      ('reviews_v1','reviews'), ('team_roles_v1','roles'), ('teams_v1','teams'),
      ('user_profiles_v1','users'),
      ('identity_center_processed_events_v1','identity_center_processed_events'),
      ('identity_center_users_v1','identity_center_users')
    ) m(view_name, table_name)
    cross join (values ('anon'),('authenticated'),('service_role')) r(role_name)
    where has_table_privilege(r.role_name, format('api.%I',m.view_name), 'select')
      and not has_table_privilege(r.role_name, format('public.%I',m.table_name), 'select')
  ),
  'api projection SELECT grants never exceed source-table SELECT grants'
);

select columns_are(
  'api', 'user_profiles_v1', array['id','contact','email','display_name'],
  'user projection excludes raw_user_meta_data'
);

select ok(
  not has_table_privilege('anon','api.review_comments_v1','select')
  and not has_table_privilege('anon','api.reviews_v1','select')
  and not has_table_privilege('anon','api.team_roles_v1','select')
  and not has_table_privilege('anon','api.teams_v1','select')
  and not has_table_privilege('anon','api.user_profiles_v1','select')
  and not has_table_privilege('anon','api.notifications_v1','select')
  and not has_table_privilege('anon','api.identity_center_processed_events_v1','select')
  and not has_table_privilege('anon','api.identity_center_users_v1','select'),
  'anonymous has no identity/collaboration API projection access'
);

select ok(
  has_table_privilege('authenticated','api.reviews_v1','select')
  and has_table_privilege('authenticated','api.team_roles_v1','select')
  and has_table_privilege('authenticated','api.teams_v1','select')
  and has_table_privilege('authenticated','api.user_profiles_v1','select')
  and not has_table_privilege('authenticated','api.notifications_v1','select')
  and not has_table_privilege('authenticated','api.identity_center_processed_events_v1','select')
  and not has_table_privilege('authenticated','api.identity_center_users_v1','select'),
  'authenticated gets reviewed DTO reads while notifications remain RPC-only and identity remains service-only'
);

select has_function('private','review_append_scope_snapshot_v1',array['uuid','text','text','jsonb','uuid'],'private append routine exists');
select has_function('private','review_revision_fingerprint_v1',array['text','jsonb'],'private fingerprint routine exists');
select has_function('private','review_scope_all_reference_ids_v1',array['jsonb'],'private all-reference routine exists');
select has_function('private','review_scope_checksum_v1',array['jsonb'],'private checksum routine exists');
select has_function('private','review_scope_current_items_v1',array['jsonb'],'private current-items routine exists');
select has_function('private','review_scope_current_reference_ids_v1',array['jsonb'],'private current-reference routine exists');
select has_function('private','review_scope_current_snapshot_v1',array['jsonb'],'private current-snapshot routine exists');
select has_function('private','review_validate_scope_history_v1',array['uuid','jsonb'],'private validate routine exists');

select has_function('public','review_append_scope_snapshot_v1',array['uuid','text','text','jsonb','uuid'],'public append compat exists');
select has_function('public','review_revision_fingerprint_v1',array['text','jsonb'],'public fingerprint compat exists');
select has_function('public','review_scope_all_reference_ids_v1',array['jsonb'],'public all-reference compat exists');
select has_function('public','review_scope_checksum_v1',array['jsonb'],'public checksum compat exists');
select has_function('public','review_scope_current_items_v1',array['jsonb'],'public current-items compat exists');
select has_function('public','review_scope_current_reference_ids_v1',array['jsonb'],'public current-reference compat exists');
select has_function('public','review_scope_current_snapshot_v1',array['jsonb'],'public current-snapshot compat exists');
select has_function('public','review_validate_scope_history_v1',array['uuid','jsonb'],'public validate compat exists');

select ok(
  (select bool_and(actual = expected)
   from (
     select p.proname,
       concat(p.provolatile,':',p.prosecdef,':',p.proisstrict,':',p.proparallel) actual,
       case p.proname
         when 'review_append_scope_snapshot_v1' then 'v:t:f:u'
         when 'review_revision_fingerprint_v1' then 'i:f:t:s'
         when 'review_scope_checksum_v1' then 'i:f:t:s'
         when 'review_validate_scope_history_v1' then 'v:t:f:u'
         else 'i:f:f:s'
       end expected
     from pg_proc p join pg_namespace n on n.oid=p.pronamespace
     where n.nspname='public'
       and p.proname in ('review_append_scope_snapshot_v1','review_revision_fingerprint_v1','review_scope_all_reference_ids_v1','review_scope_checksum_v1','review_scope_current_items_v1','review_scope_current_reference_ids_v1','review_scope_current_snapshot_v1','review_validate_scope_history_v1')
   ) properties),
  'audited public routines retain volatility/SECDEF/strict/parallel properties'
);

select ok(
  (select bool_and(pg_get_function_result(pub.oid) = pg_get_function_result(priv.oid))
   from pg_proc pub join pg_namespace npub on npub.oid=pub.pronamespace and npub.nspname='public'
   join pg_proc priv on priv.proname=pub.proname and priv.proargtypes=pub.proargtypes
   join pg_namespace npriv on npriv.oid=priv.pronamespace and npriv.nspname='private'
   where pub.proname in ('review_append_scope_snapshot_v1','review_revision_fingerprint_v1','review_scope_all_reference_ids_v1','review_scope_checksum_v1','review_scope_current_items_v1','review_scope_current_reference_ids_v1','review_scope_current_snapshot_v1','review_validate_scope_history_v1')),
  'private adapters retain exact public result contracts'
);

select ok(
  (select bool_and(not priv.prosecdef)
   from pg_proc pub join pg_namespace npub on npub.oid=pub.pronamespace and npub.nspname='public'
   join pg_proc priv on priv.proname=pub.proname and priv.proargtypes=pub.proargtypes
   join pg_namespace npriv on npriv.oid=priv.pronamespace and npriv.nspname='private'
   where pub.proname in ('review_append_scope_snapshot_v1','review_revision_fingerprint_v1','review_scope_all_reference_ids_v1','review_scope_checksum_v1','review_scope_current_items_v1','review_scope_current_reference_ids_v1','review_scope_current_snapshot_v1','review_validate_scope_history_v1')),
  'private adapters are invoker-only compatibility aliases'
);

select ok(
  has_function_privilege('api_internal_executor','public.review_scope_checksum_v1(jsonb)','execute')
  and not has_function_privilege('anon','public.review_scope_checksum_v1(jsonb)','execute')
  and not has_function_privilege('authenticated','public.review_scope_checksum_v1(jsonb)','execute')
  and has_function_privilege('service_role','public.review_revision_fingerprint_v1(text,jsonb)','execute')
  and not has_function_privilege('service_role','public.review_scope_checksum_v1(jsonb)','execute'),
  'public routine role matrix remains unchanged'
);

select ok(
  has_function_privilege('api_internal_executor','private.review_append_scope_snapshot_v1(uuid,text,text,jsonb,uuid)','execute')
  and has_function_privilege('api_internal_executor','private.review_scope_checksum_v1(jsonb)','execute')
  and has_function_privilege('service_role','private.review_revision_fingerprint_v1(text,jsonb)','execute')
  and not has_function_privilege('service_role','private.review_scope_checksum_v1(jsonb)','execute')
  and not has_function_privilege('anon','private.review_scope_checksum_v1(jsonb)','execute')
  and not has_function_privilege('authenticated','private.review_scope_checksum_v1(jsonb)','execute'),
  'private adapter effective callable matrix is exact and fail-closed'
);

insert into auth.users (
  instance_id,id,aud,role,email,encrypted_password,email_confirmed_at,
  raw_app_meta_data,raw_user_meta_data,created_at,updated_at,is_sso_user,is_anonymous
) values
  ('00000000-0000-0000-0000-000000000000','83550000-0000-4000-8000-000000000001','authenticated','authenticated','issue355-a@example.com','x',now(),'{}','{"email":"issue355-a@example.com","display_name":"Issue 355 A"}',now(),now(),false,false),
  ('00000000-0000-0000-0000-000000000000','83550000-0000-4000-8000-000000000002','authenticated','authenticated','issue355-b@example.com','x',now(),'{}','{"email":"issue355-b@example.com","display_name":"Issue 355 B"}',now(),now(),false,false);

insert into public.teams(id,"json",rank,is_public)
values
  ('83550000-0000-4000-8000-000000000011','{}',0,false),
  ('83550000-0000-4000-8000-000000000012','{}',0,false);
insert into public.roles(user_id,team_id,role)
values
  ('83550000-0000-4000-8000-000000000001','83550000-0000-4000-8000-000000000011','owner'),
  ('83550000-0000-4000-8000-000000000002','83550000-0000-4000-8000-000000000012','owner');

set local role authenticated;
select set_config('request.jwt.claim.role','authenticated',true);
select set_config('request.jwt.claim.sub','83550000-0000-4000-8000-000000000001',true);
select is((select count(*) from api.teams_v1 where id='83550000-0000-4000-8000-000000000011'),1::bigint,'same-team DTO row is readable');
select is((select count(*) from api.teams_v1 where id='83550000-0000-4000-8000-000000000012'),0::bigint,'cross-team DTO row is hidden');
select is((select count(*) from api.team_roles_v1 where team_id='83550000-0000-4000-8000-000000000011'),1::bigint,'same-team role DTO is readable');
select is((select count(*) from api.team_roles_v1 where team_id='83550000-0000-4000-8000-000000000012'),0::bigint,'cross-team role DTO is hidden');
reset role;

select is(
  public.review_scope_checksum_v1('[{"item_kind":"root"}]'::jsonb),
  private.review_scope_checksum_v1('[{"item_kind":"root"}]'::jsonb),
  'checksum public/private parity'
);
select is(
  public.review_scope_current_snapshot_v1('[]'::jsonb),
  private.review_scope_current_snapshot_v1('[]'::jsonb),
  'snapshot public/private parity'
);
select is(
  public.review_scope_current_reference_ids_v1('[]'::jsonb),
  private.review_scope_current_reference_ids_v1('[]'::jsonb),
  'reference-id public/private parity'
);
select is(
  public.review_scope_all_reference_ids_v1('[]'::jsonb),
  private.review_scope_all_reference_ids_v1('[]'::jsonb),
  'all-reference public/private parity'
);

select ok(
  (select bool_and(
      case n.nspname
        when 'public' then coalesce(array_to_string(p.proconfig, ','), '') = 'search_path=""'
        when 'private' then coalesce(array_to_string(p.proconfig, ','), '') = 'search_path=pg_catalog, pg_temp'
        else false
      end
    )
   from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname in ('public','private')
     and p.proname in ('review_append_scope_snapshot_v1','review_revision_fingerprint_v1','review_scope_all_reference_ids_v1','review_scope_checksum_v1','review_scope_current_items_v1','review_scope_current_reference_ids_v1','review_scope_current_snapshot_v1','review_validate_scope_history_v1')),
  'canonical and compatibility routines pin their reviewed safe search_path'
);

select * from finish();
rollback;
