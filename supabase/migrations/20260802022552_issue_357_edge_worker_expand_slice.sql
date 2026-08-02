-- Issue #357 E3-B: the first real Edge + Worker vertical Expand slice.
--
-- Edge receives a JWT-preserving api facade for one representative dataset
-- mutation.  The Worker hash helper moves physically to private while the old
-- public identity becomes a compatibility wrapper.  No data is copied and no
-- public path is removed in this migration.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

do $preflight$
declare
  v_save_oid oid := to_regprocedure(
    'public.cmd_dataset_save_draft(text,uuid,text,jsonb,uuid,boolean,jsonb)'
  );
  v_public_hash_oid oid := to_regprocedure(
    'public.lcia_scope_closure_sha256(jsonb)'
  );
  v_private_hash_oid oid := to_regprocedure(
    'private.lcia_scope_closure_sha256(jsonb)'
  );
  v_hash_oid oid;
begin
  if v_save_oid is null then
    raise exception using errcode = '55000',
      message = 'Issue 357 source cmd_dataset_save_draft signature is missing';
  end if;

  if v_private_hash_oid is null and v_public_hash_oid is null then
    raise exception using errcode = '55000',
      message = 'Issue 357 source lcia_scope_closure_sha256 signature is missing';
  end if;

  if v_private_hash_oid is not null and v_public_hash_oid is null then
    raise exception using errcode = '55000',
      message = 'Issue 357 private hash exists without its public compatibility wrapper';
  end if;

  v_hash_oid := coalesce(v_private_hash_oid, v_public_hash_oid);

  if not exists (
    select 1
    from pg_proc routine
    join pg_language language on language.oid = routine.prolang
    where routine.oid = v_hash_oid
      and routine.proowner = 'postgres'::regrole
      and language.lanname = 'sql'
      and routine.prorettype = 'text'::regtype
      and not routine.prosecdef
      and routine.provolatile = 'i'
      and md5(routine.prosrc) = '318e5363a4bc6597c5b0846838d2855b'
  ) then
    raise exception using errcode = '55000',
      message = 'Issue 357 source hash implementation property drift';
  end if;

  if v_private_hash_oid is null and not exists (
    select 1
    from pg_proc routine
    where routine.oid = v_public_hash_oid
      and routine.proconfig = array['search_path=public, pg_temp']::text[]
  ) then
    raise exception using errcode = '55000',
      message = 'Issue 357 public source hash definition drift';
  end if;

  if v_private_hash_oid is not null and (
    not exists (
      select 1
      from pg_proc routine
      where routine.oid = v_private_hash_oid
        and routine.proconfig = array[
          'search_path=pg_catalog, extensions, pg_temp'
        ]::text[]
    )
    or not exists (
      select 1
      from pg_proc routine
      join pg_language language on language.oid = routine.prolang
      where routine.oid = v_public_hash_oid
        and routine.proowner = 'postgres'::regrole
        and language.lanname = 'sql'
        and routine.prorettype = 'text'::regtype
        and not routine.prosecdef
        and routine.provolatile = 'i'
        and routine.proconfig = array[
          'search_path=pg_catalog, pg_temp'
        ]::text[]
        and md5(routine.prosrc) = '16f9bcf747368d76b1aa9daf7c095e9b'
    )
  ) then
    raise exception using errcode = '55000',
      message = 'Issue 357 retry hash definition drift';
  end if;
end
$preflight$;

create temporary table issue_357_expand_source_identity on commit drop as
select
  'public.cmd_dataset_save_draft(text,uuid,text,jsonb,uuid,boolean,jsonb)'::regprocedure::oid
    as save_oid,
  coalesce(
    to_regprocedure('private.lcia_scope_closure_sha256(jsonb)'),
    to_regprocedure('public.lcia_scope_closure_sha256(jsonb)')
  )::oid as hash_oid;

-- Move the real Worker implementation once.  A controlled manual retry sees
-- the private implementation and converges the wrappers/ACLs below.
do $move_hash$
begin
  if to_regprocedure('private.lcia_scope_closure_sha256(jsonb)') is null then
    alter function public.lcia_scope_closure_sha256(jsonb) set schema private;
  end if;
end
$move_hash$;

alter function private.lcia_scope_closure_sha256(jsonb)
  set search_path = pg_catalog, extensions, pg_temp;

revoke all on function private.lcia_scope_closure_sha256(jsonb)
  from public, anon, authenticated, service_role, api_internal_executor;
grant usage on schema private to service_role, api_internal_executor;
grant execute on function private.lcia_scope_closure_sha256(jsonb)
  to service_role, api_internal_executor;

comment on function private.lcia_scope_closure_sha256(jsonb) is
  'Issue #357 private Worker integrity helper; direct PostgreSQL callers must use an explicitly authorized login role.';

-- Existing database callers keep the public identity during Expand.  This is
-- a transparent invoker wrapper, not a second hash implementation.
create or replace function public.lcia_scope_closure_sha256(
  p_document jsonb
) returns text
language sql
immutable
security invoker
set search_path = pg_catalog, pg_temp
as $compat$
  select private.lcia_scope_closure_sha256(p_document)
$compat$;

revoke all on function public.lcia_scope_closure_sha256(jsonb)
  from public, anon, authenticated, service_role, api_internal_executor;
grant execute on function public.lcia_scope_closure_sha256(jsonb)
  to service_role, api_internal_executor;

comment on function public.lcia_scope_closure_sha256(jsonb) is
  'Issue #357 Expand compatibility wrapper over private.lcia_scope_closure_sha256(jsonb).';

-- Edge/Data API surface.  SECURITY INVOKER carries the request JWT into the
-- existing command, whose owner/state checks remain the mutation boundary.
create or replace function api.cmd_dataset_save_draft(
  p_table text,
  p_id uuid,
  p_version text,
  p_json_ordered jsonb,
  p_model_id uuid default null,
  p_rule_verification boolean default null,
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language sql
security invoker
set search_path = pg_catalog, pg_temp
as $api$
  select public.cmd_dataset_save_draft(
    p_table,
    p_id,
    p_version,
    p_json_ordered,
    p_model_id,
    p_rule_verification,
    p_audit
  )
$api$;

revoke all on function api.cmd_dataset_save_draft(
  text, uuid, text, jsonb, uuid, boolean, jsonb
) from public, anon, authenticated, service_role, api_internal_executor;
grant execute on function api.cmd_dataset_save_draft(
  text, uuid, text, jsonb, uuid, boolean, jsonb
) to authenticated, service_role;

comment on function api.cmd_dataset_save_draft(
  text, uuid, text, jsonb, uuid, boolean, jsonb
) is 'Issue #357 JWT Data API facade for the dataset save-draft command.';

do $postflight$
declare
  v_source record;
  v_private_oid oid := to_regprocedure(
    'private.lcia_scope_closure_sha256(jsonb)'
  );
  v_public_oid oid := to_regprocedure(
    'public.lcia_scope_closure_sha256(jsonb)'
  );
  v_api_oid oid := to_regprocedure(
    'api.cmd_dataset_save_draft(text,uuid,text,jsonb,uuid,boolean,jsonb)'
  );
begin
  select * into v_source from issue_357_expand_source_identity;

  if v_private_oid is distinct from v_source.hash_oid
     or 'public.cmd_dataset_save_draft(text,uuid,text,jsonb,uuid,boolean,jsonb)'::regprocedure::oid
        is distinct from v_source.save_oid then
    raise exception using errcode = '55000',
      message = 'Issue 357 physical implementation or Edge source OID drift';
  end if;

  if v_public_oid is null or v_public_oid = v_private_oid or v_api_oid is null then
    raise exception using errcode = '55000',
      message = 'Issue 357 wrapper topology is incomplete';
  end if;

  if not exists (
    select 1
    from pg_proc routine
    join pg_language language on language.oid = routine.prolang
    where routine.oid = v_private_oid
      and routine.proowner = 'postgres'::regrole
      and language.lanname = 'sql'
      and routine.prorettype = 'text'::regtype
      and not routine.prosecdef
      and routine.provolatile = 'i'
      and md5(routine.prosrc) = '318e5363a4bc6597c5b0846838d2855b'
      and routine.proconfig = array[
        'search_path=pg_catalog, extensions, pg_temp'
      ]::text[]
  ) then
    raise exception using errcode = '55000',
      message = 'Issue 357 private hash implementation property drift';
  end if;

  if not exists (
    select 1
    from pg_proc routine
    join pg_language language on language.oid = routine.prolang
    where routine.oid = v_public_oid
      and routine.proowner = 'postgres'::regrole
      and language.lanname = 'sql'
      and routine.prorettype = 'text'::regtype
      and not routine.prosecdef
      and routine.provolatile = 'i'
      and routine.proconfig = array[
        'search_path=pg_catalog, pg_temp'
      ]::text[]
      and md5(routine.prosrc) = '16f9bcf747368d76b1aa9daf7c095e9b'
  ) then
    raise exception using errcode = '55000',
      message = 'Issue 357 public hash wrapper property drift';
  end if;

  if not exists (
    select 1
    from pg_proc routine
    join pg_language language on language.oid = routine.prolang
    where routine.oid = v_api_oid
      and routine.proowner = 'postgres'::regrole
      and language.lanname = 'sql'
      and routine.prorettype = 'jsonb'::regtype
      and not routine.prosecdef
      and routine.provolatile = 'v'
      and routine.proconfig = array[
        'search_path=pg_catalog, pg_temp'
      ]::text[]
      and md5(routine.prosrc) = 'b620aa117cebe7f6e01ed5c4acfe86d6'
  ) then
    raise exception using errcode = '55000',
      message = 'Issue 357 API save-draft wrapper property drift';
  end if;

  if not has_schema_privilege('service_role', 'private', 'USAGE')
     or not has_schema_privilege('api_internal_executor', 'private', 'USAGE')
     or has_schema_privilege('anon', 'private', 'USAGE')
     or has_schema_privilege('authenticated', 'private', 'USAGE')
     or not has_function_privilege('service_role', v_private_oid, 'EXECUTE')
     or not has_function_privilege('api_internal_executor', v_private_oid, 'EXECUTE')
     or has_function_privilege('anon', v_private_oid, 'EXECUTE')
     or has_function_privilege('authenticated', v_private_oid, 'EXECUTE') then
    raise exception using errcode = '42501',
      message = 'Issue 357 private hash ACL contract drift';
  end if;

  if not has_function_privilege('authenticated', v_api_oid, 'EXECUTE')
     or not has_function_privilege('service_role', v_api_oid, 'EXECUTE')
     or has_function_privilege('anon', v_api_oid, 'EXECUTE') then
    raise exception using errcode = '42501',
      message = 'Issue 357 api save-draft ACL contract drift';
  end if;

  if private.lcia_scope_closure_sha256(null)
       is distinct from public.lcia_scope_closure_sha256(null)
     or private.lcia_scope_closure_sha256('{"b":2,"a":1}'::jsonb)
       is distinct from public.lcia_scope_closure_sha256('{"a":1,"b":2}'::jsonb) then
    raise exception using errcode = '55000',
      message = 'Issue 357 hash compatibility parity drift';
  end if;

  if private.lcia_scope_closure_sha256(null)
       is distinct from '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a'
     or private.lcia_scope_closure_sha256('{"a":1,"b":2}'::jsonb)
       is distinct from 'd8497d9d82770a70729261095aa98f7ef5154d7af499f8037b6ca250296785a6'
     or private.lcia_scope_closure_sha256(
       '{"nested":{"items":[1,true,null],"unicode":"生命周期"}}'::jsonb
     ) is distinct from 'f081ed2e734fcfbc916c4926518100fef21c02a17cf464f1a4dbe4bda4fbe2d5'
     or private.lcia_scope_closure_sha256('[1,2,3]'::jsonb)
       is distinct from 'a36b1f2c3f84522dd1005145646617d7054c0851e97c72a039c0bdfac9fa07f3' then
    raise exception using errcode = '55000',
      message = 'Issue 357 hash golden-vector drift';
  end if;
end
$postflight$;

commit;
