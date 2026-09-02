begin;

set local lock_timeout = '5s';
set local statement_timeout = '30s';

do $migration$
declare
  v_drift text;
  v_context_digest text;
begin
  with expected(identity) as (
    values
      ('api.portal_hybrid_search_v1(text,text[],text,jsonb,integer)'::text),
      ('private.portal_projection_hybrid_search_v1_impl(text,text[],extensions.vector,jsonb,integer,text)'),
      ('private.catalog_portal_hybrid_pattern_matches_v1(text,text[])'),
      ('private.catalog_portal_process_pattern_versions_v1(text)'),
      ('private.catalog_portal_flow_pattern_versions_v1(text)'),
      ('private.catalog_portal_process_single_character_versions_v1(text)'),
      ('private.catalog_portal_flow_single_character_versions_v1(text)'),
      ('private.portal_projection_semantic_candidates_v1(text,extensions.vector)'),
      ('private.portal_projection_semantic_process_v1(extensions.vector)'),
      ('private.portal_projection_semantic_flow_v1(extensions.vector)'),
      ('private.portal_projection_semantic_process_exact_v1(extensions.vector)'),
      ('private.portal_projection_semantic_flow_exact_v1(extensions.vector)'),
      ('private.portal_decorate_card_context_v1(jsonb)')
  )
  select pg_catalog.string_agg(expected.identity, ', ' order by expected.identity)
  into v_drift
  from expected
  left join pg_catalog.pg_proc as routine
    on routine.oid = pg_catalog.to_regprocedure(expected.identity)
  where routine.oid is null
     or not coalesce(routine.proconfig, '{}'::text[])
       @> array['statement_timeout=8s']::text[];

  if v_drift is not null then
    raise exception using
      errcode = '55000',
      message = 'Portal Hybrid timeout predecessor drifted: ' || v_drift;
  end if;

  with expected(identity) as (
    values
      ('private.portal_card_context_v1(text,integer,jsonb)'::text),
      ('private.portal_decorate_card_context_v1(jsonb)'),
      ('private.portal_process_reference_product_v1(jsonb)'),
      ('private.portal_process_functional_unit_v1(integer,jsonb)'),
      ('private.portal_exchange_support_v1(integer,jsonb,jsonb)'),
      ('private.portal_reference_flowproperty_v1(jsonb)'),
      ('private.portal_localized_text_v1(jsonb)'),
      ('private.portal_scalar_text_v1(jsonb)'),
      ('private.portal_json_items_v1(jsonb)'),
      ('private.portal_source_v1(text,jsonb)'),
      ('private.portal_capabilities_v1(text,integer,jsonb)'),
      ('private.portal_canonical_decimal_v1(text)'),
      ('private.portal_flow_kind_v1(text)'),
      ('private.portal_support_capabilities_v1(text,integer)'),
      ('private.portal_classifications_v1(jsonb)'),
      ('private.portal_publication_root_v1(text,jsonb)'),
      ('private.portal_access_restrictions_open_v1(jsonb)')
  ), manifest_entries as (
    select expected.identity,
      pg_catalog.jsonb_build_object(
        'identity', expected.identity,
        'definition', pg_catalog.pg_get_functiondef(routine.oid),
        'owner', pg_catalog.pg_get_userbyid(routine.proowner),
        'language', language.lanname,
        'volatility', routine.provolatile,
        'parallel', routine.proparallel,
        'securityDefiner', routine.prosecdef,
        'config', coalesce(
          pg_catalog.to_jsonb(routine.proconfig),
          'null'::jsonb
        )
      )::text as entry
    from expected
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(expected.identity)
    join pg_catalog.pg_language as language
      on language.oid = routine.prolang
  )
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.string_agg(
          manifest_entries.entry,
          E'\n'
          order by manifest_entries.identity
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  into v_context_digest
  from manifest_entries;

  if v_context_digest is distinct from
      'e0516d5f3a641d26221a5c44b92a2e7a87cab125e9145e8141074d9bc2af39fa' then
    raise exception using
      errcode = '55000',
      message = 'Portal card context timeout predecessor drifted';
  end if;
end
$migration$;

grant portal_public_executor, api_internal_executor to postgres;
grant create on schema private to portal_public_executor;

set role portal_public_executor;

alter function api.portal_hybrid_search_v1(text, text[], text, jsonb, integer)
  set statement_timeout = '20s';
alter function private.catalog_portal_hybrid_pattern_matches_v1(text, text[])
  set statement_timeout = '20s';
alter function private.catalog_portal_process_pattern_versions_v1(text)
  set statement_timeout = '20s';
alter function private.catalog_portal_flow_pattern_versions_v1(text)
  set statement_timeout = '20s';
alter function private.catalog_portal_process_single_character_versions_v1(text)
  set statement_timeout = '20s';
alter function private.catalog_portal_flow_single_character_versions_v1(text)
  set statement_timeout = '20s';
alter function private.portal_decorate_card_context_v1(jsonb)
  set statement_timeout = '20s';

create or replace function private.assert_portal_card_context_contract_v1()
returns void
language plpgsql
stable
security definer
parallel restricted
set search_path = ''
set row_security = on
as $$
declare
  v_expected_digest constant text :=
    'db78336c8604848af1e068352f8a39d9ee740308c44c59c639b986ed2660c47e';
begin
  perform private.assert_portal_catalog_projection_contract_v1();
  if private.portal_card_context_manifest_sha256_v1()
       is distinct from v_expected_digest then
    raise exception using
      errcode = '55000',
      message = 'Portal card context derivation contract drifted';
  end if;
end
$$;

revoke all on function private.assert_portal_card_context_contract_v1()
  from public;

reset role;
revoke create on schema private from portal_public_executor;
set role api_internal_executor;

alter function private.portal_projection_hybrid_search_v1_impl(
  text, text[], extensions.vector, jsonb, integer, text
) set statement_timeout = '20s';
alter function private.portal_projection_semantic_candidates_v1(
  text, extensions.vector
) set statement_timeout = '20s';
alter function private.portal_projection_semantic_process_v1(extensions.vector)
  set statement_timeout = '20s';
alter function private.portal_projection_semantic_flow_v1(extensions.vector)
  set statement_timeout = '20s';
alter function private.portal_projection_semantic_process_exact_v1(extensions.vector)
  set statement_timeout = '20s';
alter function private.portal_projection_semantic_flow_exact_v1(extensions.vector)
  set statement_timeout = '20s';

reset role;
revoke portal_public_executor, api_internal_executor from postgres;

do $migration$
declare
  v_drift text;
  v_context_digest text;
begin
  with expected(identity) as (
    values
      ('api.portal_hybrid_search_v1(text,text[],text,jsonb,integer)'::text),
      ('private.portal_projection_hybrid_search_v1_impl(text,text[],extensions.vector,jsonb,integer,text)'),
      ('private.catalog_portal_hybrid_pattern_matches_v1(text,text[])'),
      ('private.catalog_portal_process_pattern_versions_v1(text)'),
      ('private.catalog_portal_flow_pattern_versions_v1(text)'),
      ('private.catalog_portal_process_single_character_versions_v1(text)'),
      ('private.catalog_portal_flow_single_character_versions_v1(text)'),
      ('private.portal_projection_semantic_candidates_v1(text,extensions.vector)'),
      ('private.portal_projection_semantic_process_v1(extensions.vector)'),
      ('private.portal_projection_semantic_flow_v1(extensions.vector)'),
      ('private.portal_projection_semantic_process_exact_v1(extensions.vector)'),
      ('private.portal_projection_semantic_flow_exact_v1(extensions.vector)'),
      ('private.portal_decorate_card_context_v1(jsonb)')
  )
  select pg_catalog.string_agg(expected.identity, ', ' order by expected.identity)
  into v_drift
  from expected
  join pg_catalog.pg_proc as routine
    on routine.oid = pg_catalog.to_regprocedure(expected.identity)
  where not coalesce(routine.proconfig, '{}'::text[])
      @> array['statement_timeout=20s']::text[]
     or coalesce(routine.proconfig, '{}'::text[])
      @> array['statement_timeout=8s']::text[];

  if v_drift is not null then
    raise exception using
      errcode = '55000',
      message = 'Portal Hybrid timeout forward contract drifted: ' || v_drift;
  end if;

  with expected(identity) as (
    values
      ('private.portal_card_context_v1(text,integer,jsonb)'::text),
      ('private.portal_decorate_card_context_v1(jsonb)'),
      ('private.portal_process_reference_product_v1(jsonb)'),
      ('private.portal_process_functional_unit_v1(integer,jsonb)'),
      ('private.portal_exchange_support_v1(integer,jsonb,jsonb)'),
      ('private.portal_reference_flowproperty_v1(jsonb)'),
      ('private.portal_localized_text_v1(jsonb)'),
      ('private.portal_scalar_text_v1(jsonb)'),
      ('private.portal_json_items_v1(jsonb)'),
      ('private.portal_source_v1(text,jsonb)'),
      ('private.portal_capabilities_v1(text,integer,jsonb)'),
      ('private.portal_canonical_decimal_v1(text)'),
      ('private.portal_flow_kind_v1(text)'),
      ('private.portal_support_capabilities_v1(text,integer)'),
      ('private.portal_classifications_v1(jsonb)'),
      ('private.portal_publication_root_v1(text,jsonb)'),
      ('private.portal_access_restrictions_open_v1(jsonb)')
  ), manifest_entries as (
    select expected.identity,
      pg_catalog.jsonb_build_object(
        'identity', expected.identity,
        'definition', pg_catalog.pg_get_functiondef(routine.oid),
        'owner', pg_catalog.pg_get_userbyid(routine.proowner),
        'language', language.lanname,
        'volatility', routine.provolatile,
        'parallel', routine.proparallel,
        'securityDefiner', routine.prosecdef,
        'config', coalesce(
          pg_catalog.to_jsonb(routine.proconfig),
          'null'::jsonb
        )
      )::text as entry
    from expected
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(expected.identity)
    join pg_catalog.pg_language as language
      on language.oid = routine.prolang
  )
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.string_agg(
          manifest_entries.entry,
          E'\n'
          order by manifest_entries.identity
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  into v_context_digest
  from manifest_entries;

  if v_context_digest is distinct from
      'db78336c8604848af1e068352f8a39d9ee740308c44c59c639b986ed2660c47e' then
    raise exception using
      errcode = '55000',
      message = 'Portal card context timeout forward contract drifted';
  end if;
end
$migration$;

commit;
