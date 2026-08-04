begin;
create extension if not exists pgtap with schema extensions;
select extensions.plan(10);

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.prokind='f' and p.prosecdef),
  241::bigint, 'all public SECURITY DEFINER signatures are present');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.prosecdef and pg_get_userbyid(p.proowner)='postgres'),
  225::bigint, '225 SECURITY DEFINER signatures retain postgres ownership');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.prosecdef and pg_get_userbyid(p.proowner)='api_internal_executor'),
  16::bigint, '#339 retains sixteen RLS-bound facade owners');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.prosecdef
     and not exists (select 1 from unnest(coalesce(p.proconfig,'{}')) c where c like 'search_path=%')),
  0::bigint, 'every SECURITY DEFINER signature fixes search_path');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.prosecdef and has_function_privilege('anon',p.oid,'EXECUTE')),
  93::bigint, 'current anon effective EXECUTE count reflects #354 convergence');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.prosecdef and has_function_privilege('authenticated',p.oid,'EXECUTE')),
  140::bigint, 'current authenticated effective EXECUTE count reflects #354 convergence');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.prosecdef and has_function_privilege('public',p.oid,'EXECUTE')),
  17::bigint, 'current PUBLIC effective EXECUTE count is explicit');

select extensions.is(
  (select count(*)::bigint from pg_proc p join pg_namespace n on n.oid=p.pronamespace
   where n.nspname='public' and p.prosecdef and has_function_privilege('service_role',p.oid,'EXECUTE')),
  171::bigint, 'current service_role effective EXECUTE count is explicit');

select extensions.is(
  (select jsonb_array_length(posture->'forbiddenInternalExecute') from util.security_acl_expand_posture),
  0, '#339 internal-schema API-role EXECUTE drift remains closed');

select extensions.is(
  (select jsonb_array_length(posture->'repoOwnerDefaultPrivilegeResidue') from util.security_acl_expand_posture),
  0, '#339 repo-owned default privilege residue remains closed');

select * from extensions.finish();
rollback;
