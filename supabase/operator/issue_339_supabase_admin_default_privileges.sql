\set ON_ERROR_STOP on

begin;

do $guard$
begin
  if session_user <> 'supabase_admin' or current_user <> 'supabase_admin' then
    raise exception using
      errcode = '42501',
      message = 'run this file in a Supabase-authorized supabase_admin owner session';
  end if;
end
$guard$;

alter default privileges for role supabase_admin in schema public, api, private, util, archive
  revoke all on tables from public, anon, authenticated, service_role;
alter default privileges for role supabase_admin in schema public, api, private, util, archive
  revoke all on sequences from public, anon, authenticated, service_role;
alter default privileges for role supabase_admin in schema public, api, private, util, archive
  revoke execute on functions from public, anon, authenticated, service_role;

do $readback$
begin
  if exists (
    select 1
    from pg_default_acl d
    join pg_namespace n on n.oid = d.defaclnamespace
    cross join lateral aclexplode(d.defaclacl) a
    left join pg_roles grantee on grantee.oid = a.grantee
    where d.defaclrole = 'supabase_admin'::regrole
      and n.nspname in ('public', 'api', 'private', 'util', 'archive')
      and (a.grantee = 0 or grantee.rolname in ('anon', 'authenticated', 'service_role'))
  ) then
    raise exception 'supabase_admin default privilege residue remains after revoke';
  end if;
end
$readback$;

commit;
