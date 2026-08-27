begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select extensions.no_plan();

select extensions.is(
  (
    select count(*)
    from pg_catalog.pg_proc as routine
    join pg_catalog.pg_namespace as namespace
      on namespace.oid = routine.pronamespace
    where namespace.nspname = 'private'
      and routine.proname in (
        'portal_process_open_capability_bridge_v1',
        'portal_current_lcia_publication_for_process_v1',
        'portal_lcia_decorate_item_page_v1',
        'portal_lcia_decorate_dataset_v1'
      )
  ),
  4::bigint,
  'the additive LCIA catalog projection has exactly four private helpers and no overloads'
);

select extensions.ok(
  (
    select routine.prosecdef
      and routine.proowner = 'postgres'::regrole
      and routine.proconfig @> array['search_path=""']::text[]
      and routine.prosrc ~ 'portal_lcia_projection_is_public_v1'
      and routine.prosrc ~ 'binding.status = ''finalized'''
      and routine.prosrc ~ 'binding.revoked_at is null'
      and routine.prosrc ~ 'publication.is_current'
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'private.portal_current_lcia_publication_for_process_v1(uuid,text)'::regprocedure
  ),
  'the publication resolver is a fixed-search-path definer that reuses the authoritative visibility predicate'
);

select extensions.is(
  (
    with actual as (
      select coalesce(grantee_role.rolname, 'PUBLIC') as grantee,
        acl.privilege_type, acl.is_grantable
      from pg_catalog.pg_proc as routine
      cross join lateral pg_catalog.aclexplode(
        coalesce(routine.proacl, pg_catalog.acldefault('f', routine.proowner))
      ) as acl
      left join pg_catalog.pg_roles as grantee_role
        on grantee_role.oid = acl.grantee
      where routine.oid =
        'private.portal_current_lcia_publication_for_process_v1(uuid,text)'::regprocedure
    ), expected(grantee, privilege_type, is_grantable) as (
      values
        ('postgres'::text, 'EXECUTE'::text, false),
        ('portal_public_executor'::text, 'EXECUTE'::text, false)
    )
    select count(*)
    from (
      (select * from actual except select * from expected)
      union all
      (select * from expected except select * from actual)
    ) as difference
  ),
  0::bigint,
  'only postgres and the constrained executor can execute the publication resolver'
);

select extensions.ok(
  (
    select routine.prosecdef
      and routine.proowner = 'portal_public_executor'::regrole
      and routine.proconfig @> array['search_path=""']::text[]
      and routine.prosrc ~ 'portal_capabilities_v1'
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'private.portal_process_open_capability_bridge_v1(integer,jsonb)'::regprocedure
  )
  and pg_catalog.has_function_privilege(
    'postgres',
    'private.portal_process_open_capability_bridge_v1(integer,jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'anon',
    'private.portal_process_open_capability_bridge_v1(integer,jsonb)',
    'EXECUTE'
  ),
  'the boolean capability bridge reuses policy as the executor without widening browser access'
);

select extensions.is(
  (
    select count(*)
    from pg_catalog.pg_proc as routine
    where routine.oid in (
      'private.portal_lcia_decorate_item_page_v1(jsonb)'::regprocedure,
      'private.portal_lcia_decorate_dataset_v1(jsonb)'::regprocedure
    )
      and not routine.prosecdef
      and routine.proowner = 'portal_public_executor'::regrole
      and routine.proconfig @> array['search_path=""']::text[]
      and not pg_catalog.has_function_privilege(
        'anon', routine.oid, 'EXECUTE'
      )
      and not pg_catalog.has_function_privilege(
        'authenticated', routine.oid, 'EXECUTE'
      )
      and not pg_catalog.has_function_privilege(
        'service_role', routine.oid, 'EXECUTE'
      )
  ),
  2::bigint,
  'both decorators are invoker helpers owned only by the constrained executor'
);

select extensions.is(
  (
    select count(*)
    from pg_catalog.pg_proc as routine
    where routine.oid in (
      'api.portal_search_processes_v1(text,jsonb,text,text,integer)'::regprocedure,
      'api.portal_get_dataset_v1(text,uuid,text)'::regprocedure,
      'api.portal_list_versions_v1(text,uuid,text,integer)'::regprocedure
    )
      and routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and routine.proconfig @> array[
        'search_path=""',
        'statement_timeout=8s'
      ]::text[]
      and pg_catalog.has_function_privilege('anon', routine.oid, 'EXECUTE')
      and pg_catalog.has_function_privilege(
        'authenticated', routine.oid, 'EXECUTE'
      )
      and not pg_catalog.has_function_privilege(
        'service_role', routine.oid, 'EXECUTE'
      )
  ),
  3::bigint,
  'all enriched catalog signatures preserve owner, security, budget, and browser ACLs'
);

select extensions.ok(
  (
    select routine.prosrc ~ 'portal_lcia_decorate_item_page_v1'
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'api.portal_search_processes_v1(text,jsonb,text,text,integer)'::regprocedure
  )
  and (
    select routine.prosrc ~ 'portal_lcia_decorate_dataset_v1'
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'api.portal_get_dataset_v1(text,uuid,text)'::regprocedure
  )
  and (
    select routine.prosrc ~ 'portal_lcia_decorate_item_page_v1'
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'api.portal_list_versions_v1(text,uuid,text,integer)'::regprocedure
  ),
  'only the intended Search, Detail, and Versions wrappers invoke LCIA decorators'
);

grant portal_public_executor to postgres;
set local role portal_public_executor;

create temporary table portal_lcia_decorator_fixture (
  original jsonb not null,
  decorated jsonb not null
) on commit drop;

insert into portal_lcia_decorator_fixture (original, decorated)
select fixture,
  private.portal_lcia_decorate_item_page_v1(fixture)
from (values (jsonb_build_object(
  'schemaVersion', 'portal.public-search-page.v1',
  'kind', 'process',
  'queryFingerprint', repeat('a', 64),
  'items', jsonb_build_array(
    jsonb_build_object(
      'key', jsonb_build_object(
        'kind', 'process',
        'id', '52719999-0000-4000-8000-000000000001',
        'version', '01.00.000'
      ),
      'accessLevel', 'open',
      'capabilities', jsonb_build_object(
        'metadataVisible', true,
        'exchangesVisible', true,
        'lciaVisible', false,
        'publicArtifactVisible', false,
        'citationVisible', true,
        'policyVersion', 'portal-capability-policy.v1',
        'reasonCodes', '[]'::jsonb
      )
    ),
    jsonb_build_object(
      'key', jsonb_build_object(
        'kind', 'flow',
        'id', '52719999-0000-4000-8000-000000000002',
        'version', '01.00.000'
      ),
      'accessLevel', 'open',
      'capabilities', jsonb_build_object(
        'metadataVisible', true,
        'exchangesVisible', true,
        'lciaVisible', false,
        'publicArtifactVisible', false,
        'citationVisible', true,
        'policyVersion', 'portal-capability-policy.v1',
        'reasonCodes', '[]'::jsonb
      )
    )
  ),
  'nextCursor', 'cursor-v1'
))) as source(fixture);

select extensions.ok(
  (
    select jsonb_array_length(decorated->'items') = 2
      and decorated #>> '{items,0,key,id}' =
            '52719999-0000-4000-8000-000000000001'
      and decorated #>> '{items,1,key,id}' =
            '52719999-0000-4000-8000-000000000002'
      and decorated->>'nextCursor' = 'cursor-v1'
      and (decorated - 'items') = (original - 'items')
      and not exists (
        select 1
        from jsonb_array_elements(original->'items')
          with ordinality as before_item(value, ordinality)
        join jsonb_array_elements(decorated->'items')
          with ordinality as after_item(value, ordinality)
          using (ordinality)
        where (before_item.value - 'capabilities') <>
              (after_item.value - 'capabilities')
      )
    from portal_lcia_decorator_fixture
  ),
  'item decoration preserves cardinality, order, cursor, and every non-item top-level field'
);

select extensions.ok(
  (
    with flow_envelope as (
      select jsonb_build_object(
        'schemaVersion', 'portal.public-dataset.v1',
        'key', jsonb_build_object(
          'kind', 'flow',
          'id', '52719999-0000-4000-8000-000000000002',
          'version', '01.00.000'
        ),
        'accessLevel', 'open',
        'capabilities', jsonb_build_object('lciaVisible', true),
        'publication', jsonb_build_object('forged', true),
        'marker', 'preserved'
      ) as value
    ), decorated as (
      select value, private.portal_lcia_decorate_dataset_v1(value) as result
      from flow_envelope
    )
    select result #>> '{capabilities,lciaVisible}' = 'false'
      and result->'publication' = 'null'::jsonb
      and result->>'marker' = 'preserved'
      and (result - 'capabilities' - 'publication') =
          (value - 'capabilities' - 'publication')
    from decorated
  ),
  'Flow decoration fails closed to false/null without changing unrelated envelope fields'
);

reset role;
revoke portal_public_executor from postgres;

select extensions.is(
  (
    select count(*)
    from private.api_capability_grants
    where routine_identity in (
      'private.portal_current_lcia_publication_for_process_v1(uuid, text)',
      'private.portal_process_open_capability_bridge_v1(integer, jsonb)',
      'private.portal_lcia_decorate_item_page_v1(jsonb)',
      'private.portal_lcia_decorate_dataset_v1(jsonb)'
    )
  ),
  0::bigint,
  'private LCIA catalog helpers create no exposed API capability entry'
);

select extensions.ok(
  (
    select count(*) = 1
    from pg_catalog.pg_auth_members as membership
    where membership.member = 'portal_public_executor'::regrole
       or membership.roleid = 'portal_public_executor'::regrole
  )
  and exists (
    select 1
    from pg_catalog.pg_auth_members as membership
    where membership.roleid = 'portal_public_executor'::regrole
      and membership.member = 'postgres'::regrole
      and membership.admin_option
      and not membership.inherit_option
      and not membership.set_option
  ),
  'temporary owner handoff preserves the reviewed executor membership edge'
);

select * from extensions.finish();
rollback;
