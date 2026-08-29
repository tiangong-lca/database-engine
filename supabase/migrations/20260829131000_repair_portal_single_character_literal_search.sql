-- Issue #551 CI follow-up: PGroonga's LIKE index condition tokenizes the
-- literal portions of a pattern. With the current TokenBigram index, a plain
-- one-code-point substring such as %a% can produce no index candidate even
-- though PostgreSQL LIKE itself is true. Heap recheck can remove false
-- positives but cannot recover that false negative. Preserve every existing
-- indexed path and use an exact sequential strpos branch only for the one
-- unescaped code point shape.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '30s';

do $portal_single_character_search_prerequisite_guard$
declare
  v_process_sha256 text;
  v_flow_sha256 text;
begin
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.pg_get_functiondef(
          'private.catalog_portal_process_pattern_versions_v1(text)'::regprocedure
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  into v_process_sha256;

  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.pg_get_functiondef(
          'private.catalog_portal_flow_pattern_versions_v1(text)'::regprocedure
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  into v_flow_sha256;

  if v_process_sha256 <>
       '114236b32b7a8a813f222ea301be9dd92529aba1eaf565301b3ed533d94e2115'
     or v_flow_sha256 <>
       'e36c34a1fec82f769fef1c67ce4505f760025d0b2c0d4b72ed8b55e87abfd6a4'
     or (
       select not relation.relrowsecurity
         or not relation.relforcerowsecurity
         or relation.relowner <> 'postgres'::regrole
       from pg_catalog.pg_class as relation
       where relation.oid =
         'private.portal_catalog_search_rows_v1'::regclass
     ) is not false
     or not exists (
       select 1
       from pg_catalog.pg_constraint as state_check
       where state_check.conrelid =
           'private.portal_catalog_search_rows_v1'::regclass
         and state_check.conname =
           'portal_catalog_search_rows_v1_state_code_check'
         and state_check.contype = 'c'
         and state_check.convalidated
     )
     or (
       select count(*)
       from pg_catalog.pg_policies as policy
       where policy.schemaname = 'private'
         and policy.tablename = 'portal_catalog_search_rows_v1'
         and policy.policyname =
           'portal_catalog_search_rows_portal_select_v1'
         and policy.roles = array['portal_public_executor']::name[]
         and policy.cmd = 'SELECT'
         and policy.qual = 'true'
         and policy.with_check is null
     ) <> 1 then
    raise exception 'Portal single-character Search prerequisites drifted'
      using errcode = '55000';
  end if;
end
$portal_single_character_search_prerequisite_guard$;

grant portal_public_executor to postgres;
grant create on schema private to portal_public_executor;
set role portal_public_executor;

create or replace function private.catalog_portal_process_pattern_versions_v1(
  p_like_pattern text
)
returns table(id uuid, version text)
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
set row_security = 'on'
as $function$
declare
  v_literal text;
begin
  if pg_catalog.char_length(p_like_pattern) = 3
     and pg_catalog.left(p_like_pattern, 1) = '%'
     and pg_catalog.right(p_like_pattern, 1) = '%' then
    v_literal := pg_catalog.substr(p_like_pattern, 2, 1);
    return query
    select projection.id,
      projection.version
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'process'
      and pg_catalog.strpos(projection.document, v_literal) > 0;
    return;
  end if;

  return query execute pg_catalog.format($sql$
    select projection.id,
      projection.version
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'process'
      and projection.document like %L escape E'\\'
  $sql$, p_like_pattern);
end
$function$;

create or replace function private.catalog_portal_flow_pattern_versions_v1(
  p_like_pattern text
)
returns table(id uuid, version text)
language plpgsql
stable
parallel restricted
security definer
set search_path = ''
set statement_timeout = '8s'
set plan_cache_mode = 'force_custom_plan'
set row_security = 'on'
as $function$
declare
  v_literal text;
begin
  if pg_catalog.char_length(p_like_pattern) = 3
     and pg_catalog.left(p_like_pattern, 1) = '%'
     and pg_catalog.right(p_like_pattern, 1) = '%' then
    v_literal := pg_catalog.substr(p_like_pattern, 2, 1);
    return query
    select projection.id,
      projection.version
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'flow'
      and pg_catalog.strpos(projection.document, v_literal) > 0;
    return;
  end if;

  return query execute pg_catalog.format($sql$
    select projection.id,
      projection.version
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'flow'
      and projection.document like %L escape E'\\'
  $sql$, p_like_pattern);
end
$function$;

comment on function private.catalog_portal_process_pattern_versions_v1(text) is
  'Returns Process projection versions through the fixed literal-LIKE template; an unescaped one-code-point substring uses strpos to avoid TokenBigram false negatives.';
comment on function private.catalog_portal_flow_pattern_versions_v1(text) is
  'Returns Flow projection versions through the fixed literal-LIKE template; an unescaped one-code-point substring uses strpos to avoid TokenBigram false negatives.';

reset role;
revoke create on schema private from portal_public_executor;
revoke portal_public_executor from postgres;

do $verify_portal_single_character_search_repair$
begin
  if (
       select count(*)
       from pg_catalog.pg_proc as routine
       where routine.oid in (
           'private.catalog_portal_process_pattern_versions_v1(text)'::regprocedure,
           'private.catalog_portal_flow_pattern_versions_v1(text)'::regprocedure
         )
         and routine.proowner = 'portal_public_executor'::regrole
         and routine.prosecdef
         and routine.provolatile = 's'
         and routine.proparallel = 'r'
         and coalesce(routine.proconfig, '{}'::text[]) = array[
           'search_path=""',
           'statement_timeout=8s',
           'plan_cache_mode=force_custom_plan',
           'row_security=on'
         ]::text[]
         and routine.prosrc ~ 'char_length\(p_like_pattern\) = 3'
         and routine.prosrc ~ 'pg_catalog.strpos'
         and routine.prosrc ~ 'return query execute pg_catalog.format'
         and routine.prosrc !~ '%I'
     ) <> 2
     or pg_catalog.has_function_privilege(
       'anon',
       'private.catalog_portal_process_pattern_versions_v1(text)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated',
       'private.catalog_portal_process_pattern_versions_v1(text)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role',
       'private.catalog_portal_process_pattern_versions_v1(text)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'api_internal_executor',
       'private.catalog_portal_process_pattern_versions_v1(text)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon',
       'private.catalog_portal_flow_pattern_versions_v1(text)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated',
       'private.catalog_portal_flow_pattern_versions_v1(text)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role',
       'private.catalog_portal_flow_pattern_versions_v1(text)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'api_internal_executor',
       'private.catalog_portal_flow_pattern_versions_v1(text)',
       'EXECUTE'
     ) then
    raise exception 'Portal single-character Search repair drifted'
      using errcode = '55000';
  end if;
end
$verify_portal_single_character_search_repair$;

commit;
