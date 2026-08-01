\set ON_ERROR_STOP on

-- Emergency restore for the repo-owned postgres future-function ACL layers.
-- It restores the global layer plus public/api/private/util/archive additive
-- layers captured before migration 20260801131918; current routine ACLs and all
-- business data remain untouched.
begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

do $restore$
declare
  snapshot_row record;
  grant_target text;
begin
  if session_user <> 'postgres' or current_user <> 'postgres' then
    raise exception using
      errcode = '42501',
      message = 'global function-default restore requires the postgres owner session';
  end if;

  if to_regclass('archive.security_acl_postgres_global_functions_20260801_snapshot') is null then
    raise exception 'global function-default restore snapshot is missing';
  end if;

  alter default privileges for role postgres
    revoke execute on functions from public, anon, authenticated, service_role;
  alter default privileges for role postgres in schema public, api, private, util, archive
    revoke execute on functions from public, anon, authenticated, service_role;

  if exists (
    select 1
    from archive.security_acl_postgres_global_functions_20260801_snapshot
    where scope_schema not in ('*', 'public', 'api', 'private', 'util', 'archive')
      or privilege_type <> 'EXECUTE'
  ) then
    raise exception 'global function-default restore snapshot contains an invalid ACL layer';
  end if;

  for snapshot_row in
    select scope_schema, grantee, privilege_type, is_grantable
    from archive.security_acl_postgres_global_functions_20260801_snapshot
    order by scope_schema, grantee, privilege_type, is_grantable
  loop
    grant_target := case
      when snapshot_row.grantee = 'PUBLIC' then 'PUBLIC'
      else format('%I', snapshot_row.grantee)
    end;
    if snapshot_row.scope_schema = '*' then
      execute format(
        'ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT %s ON FUNCTIONS TO %s%s',
        snapshot_row.privilege_type,
        grant_target,
        case when snapshot_row.is_grantable then ' WITH GRANT OPTION' else '' end
      );
    else
      execute format(
        'ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA %I GRANT %s ON FUNCTIONS TO %s%s',
        snapshot_row.scope_schema,
        snapshot_row.privilege_type,
        grant_target,
        case when snapshot_row.is_grantable then ' WITH GRANT OPTION' else '' end
      );
    end if;
  end loop;
end
$restore$;

commit;
