begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select no_plan();

-- The v2 wire domain is the JavaScript safe-integer subset of JSON.  Keep
-- these primitive tests independent of the 305-row lifecycle fixture so a
-- serialization or privilege regression fails quickly and diagnostically.
select is(
  private.dataset_flow_identity_safe_json_v2('9007199254740991'::jsonb),
  '9007199254740991'::jsonb,
  'safe JSON accepts Number.MAX_SAFE_INTEGER'
);
select is(
  private.dataset_flow_identity_safe_json_v2('-9007199254740991'::jsonb),
  '-9007199254740991'::jsonb,
  'safe JSON accepts Number.MIN_SAFE_INTEGER'
);
select is(
  private.dataset_flow_identity_safe_json_v2('9007199254740992'::jsonb),
  null::jsonb,
  'safe JSON rejects the first positive unsafe integer'
);
select is(
  private.dataset_flow_identity_safe_json_v2('-9007199254740992'::jsonb),
  null::jsonb,
  'safe JSON rejects the first negative unsafe integer'
);
select is(
  private.dataset_flow_identity_safe_json_v2('1.5'::jsonb),
  null::jsonb,
  'safe JSON rejects a non-integer number'
);
select is(
  private.dataset_flow_identity_safe_json_v2('1e-1'::jsonb),
  null::jsonb,
  'safe JSON rejects a fractional exponent form'
);
select is(
  private.dataset_flow_identity_safe_json_v2('-0'::jsonb),
  '0'::jsonb,
  'safe JSON canonicalizes negative zero to zero'
);
select is(
  private.dataset_flow_identity_safe_json_v2('[null,true,1]'::jsonb),
  '[null,true,1]'::jsonb,
  'safe JSON preserves JSON null instead of confusing it with SQL null'
);
select is(
  private.dataset_flow_identity_safe_json_v2(
    '{"outer":[1,{"bad":1.5}]}'::jsonb
  ),
  null::jsonb,
  'one unsafe descendant rejects the entire JSON value'
);
select is(
  private.dataset_flow_identity_safe_json_v2(
    '{"outer":[1,{"ok":2}]}'::jsonb
  ),
  '{"outer":[1,{"ok":2}]}'::jsonb,
  'set-based safe JSON recursion preserves valid array order and object shape'
);

select throws_ok(
  $$select E'"\\u0000"'::jsonb$$,
  '22P05',
  'unsupported Unicode escape sequence',
  'PostgreSQL rejects U+0000 before it can enter the v2 JSON domain'
);
select throws_ok(
  $$select E'"\\uD800"'::jsonb$$,
  '22P02',
  'invalid input syntax for type json',
  'PostgreSQL rejects an unpaired high surrogate'
);
select is(
  private.dataset_flow_identity_safe_json_v2(
    E'"\\uD83D\\uDE00"'::jsonb
  ),
  '"😀"'::jsonb,
  'safe JSON accepts a paired non-BMP surrogate as one scalar value'
);
select is(
  util.dataset_flow_identity_restricted_sha256_v2(
    '{"a":[1,-0,9007199254740991],"é":"值","😀":"emoji"}'::jsonb
  ),
  '3caeab8cb9c331ffeca1801150a074d10e7e8d3e23dc9314eeb1985b4babcdd6',
  'restricted hash matches a fixed independent JSON.stringify/UTF-8 vector'
);
select is(
  util.dataset_flow_identity_restricted_sha256_v2(
    '{"unsafe":9007199254740992}'::jsonb
  ),
  null::text,
  'restricted hash refuses values outside the safe JSON domain'
);
select is(
  (select provolatile::text
   from pg_proc
   where oid =
     'private.dataset_flow_identity_safe_json_v2(jsonb)'::regprocedure),
  's',
  'safe JSON volatility is STABLE because its recursive key sort is STABLE'
);
select is(
  (select provolatile::text
   from pg_proc
   where oid =
     'util.dataset_flow_identity_validate_support_snapshot(uuid,jsonb)'::regprocedure),
  's',
  'support snapshot validation is STABLE so read-only guards stay STABLE'
);

create temporary table mapping_index_contract on commit drop as
select mappings,
  private.dataset_flow_identity_mapping_index_v2(mappings) as index
from (values (jsonb_build_array(
  jsonb_build_object(
    'ordinal', 1, 'mapping_id', repeat('a', 64),
    'source', jsonb_build_object(
      'id', '10000000-0000-4000-8000-000000000001',
      'version', '01.00.000'
    ),
    'marker', 'first'
  ),
  jsonb_build_object(
    'ordinal', 2, 'mapping_id', repeat('b', 64),
    'source', jsonb_build_object(
      'id', '10000000-0000-4000-8000-000000000002',
      'version', '01.00.000'
    ),
    'marker', 'second'
  )
))) as fixture(mappings);
select is(
  (select jsonb_agg(key order by key)
   from mapping_index_contract
   cross join lateral jsonb_object_keys(index) as key),
  to_jsonb(array[
    'by_id', 'by_ordinal', 'by_source', 'mapping_count',
    'mapping_guard_set_sha256', 'mappings', 'schema_version'
  ]::text[]),
  'mapping index has the exact ordered-array and three lookup domains'
);
select is(
  (select jsonb_build_object(
    'count', index->'mapping_count',
    'hash_matches', index->>'mapping_guard_set_sha256'
      = util.dataset_flow_identity_restricted_sha256_v2(mappings),
    'ordinal', index #>> '{by_ordinal,2,marker}',
    'id', index #>> array['by_id', repeat('a', 64), 'marker'],
    'source', index #>> array[
      'by_source',
      '10000000-0000-4000-8000-000000000002@01.00.000',
      'marker'
    ]
  ) from mapping_index_contract),
  jsonb_build_object(
    'count', 2, 'hash_matches', true,
    'ordinal', 'second', 'id', 'first', 'source', 'second'
  ),
  'mapping index lookups and retained-array hash are exactly consistent'
);
select throws_ok(
  $$select private.dataset_flow_identity_mapping_index_v2(jsonb_build_array(
    jsonb_build_object(
      'ordinal', 1, 'mapping_id', repeat('a', 64),
      'source', jsonb_build_object(
        'id', '10000000-0000-4000-8000-000000000001',
        'version', '01.00.000'
      )
    ),
    jsonb_build_object(
      'ordinal', 2, 'mapping_id', repeat('b', 64),
      'source', jsonb_build_object(
        'id', '10000000-0000-4000-8000-000000000001',
        'version', '01.00.000'
      )
    )
  ))$$,
  '22023',
  'FLOW_IDENTITY_MAPPING_INDEX_IDENTITY_INVALID',
  'mapping index fails closed on a duplicate source identity'
);
select ok(
  pg_get_functiondef(
    'private.dataset_flow_identity_build_process_v2(uuid,jsonb,jsonb,jsonb,jsonb,text)'::regprocedure
  ) not like '%jsonb_array_elements(p_mappings)%'
  and pg_get_functiondef(
    'util.dataset_flow_identity_dry_validate_process(uuid,jsonb,jsonb,jsonb,jsonb)'::regprocedure
  ) like '%v_mapping_by_source ?%',
  'indexed v2 process construction has no per-rewrite mapping-array scan'
);
select is(
  (select provolatile::text
   from pg_proc
   where oid =
     'private.dataset_flow_identity_mapping_index_v2(jsonb)'::regprocedure),
  's',
  'mapping index volatility is STABLE because its restricted hash is STABLE'
);

-- common:STMultiLang/common:shortDescription accepts only the three exact
-- wire shapes supported by the CLI contract.
select ok(
  private.dataset_flow_identity_short_description_v2('"plain"'::jsonb),
  'STMultiLang accepts a plain JSON string'
);
select ok(
  private.dataset_flow_identity_short_description_v2(
    '{"@xml:lang":"en","#text":"Mass"}'::jsonb
  ),
  'STMultiLang accepts one exact language/text object'
);
select ok(
  private.dataset_flow_identity_short_description_v2(
    (
      '[{"@xml:lang":"en","#text":"Mass"},'
        || '{"@xml:lang":"zh","#text":"质量"}]'
    )::jsonb
  ),
  'STMultiLang accepts a non-empty array of exact language/text objects'
);
select ok(
  not private.dataset_flow_identity_short_description_v2(
    '{"@xml:lang":"en","#text":"Mass","extra":true}'::jsonb
  ),
  'STMultiLang rejects an object with an extra key'
);
select ok(
  not private.dataset_flow_identity_short_description_v2(
    '{"@xml:lang":"en"}'::jsonb
  ),
  'STMultiLang rejects an object missing #text'
);
select ok(
  not private.dataset_flow_identity_short_description_v2('[]'::jsonb),
  'STMultiLang rejects an empty array'
);
select ok(
  not private.dataset_flow_identity_short_description_v2(
    '["en","Mass"]'::jsonb
  ),
  'STMultiLang rejects an array of scalar strings'
);
select ok(
  not private.dataset_flow_identity_short_description_v2(
    '{"@xml:lang":1,"#text":"Mass"}'::jsonb
  ),
  'STMultiLang requires string-valued language and text fields'
);
select ok(
  not private.dataset_flow_identity_short_description_v2('null'::jsonb),
  'STMultiLang rejects JSON null'
);

-- The only public v2 entrypoints are authenticated security-definer RPCs
-- with an empty search_path.  Cancel is a zero-primary-write abandon path;
-- its legacy v1 implementation remains private and non-executable.
select function_privs_are(
  'public', 'cmd_dataset_flow_identity_capture_attest_guarded',
  array['jsonb'], 'authenticated', array['EXECUTE'],
  'capture attestation is executable only through the authenticated role'
);
select function_privs_are(
  'public', 'cmd_dataset_flow_identity_scope_preflight_guarded',
  array['jsonb'], 'authenticated', array['EXECUTE'],
  'v2 preflight is executable through the authenticated role'
);
select function_privs_are(
  'public', 'cmd_dataset_flow_identity_process_rewrite_guarded',
  array['uuid', 'jsonb'], 'authenticated', array['EXECUTE'],
  'v2 process rewrite is executable through the authenticated role'
);
select function_privs_are(
  'public', 'cmd_dataset_flow_identity_scope_read',
  array['uuid'], 'authenticated', array['EXECUTE'],
  'v2 scope read is executable through the authenticated role'
);
select function_privs_are(
  'public', 'cmd_dataset_flow_identity_scope_finalize_guarded',
  array['uuid', 'jsonb'], 'authenticated', array['EXECUTE'],
  'v2 finalize is executable through the authenticated role'
);
select function_privs_are(
  'public', 'cmd_dataset_flow_identity_scope_cancel_guarded',
  array['uuid', 'jsonb'], 'authenticated', array['EXECUTE'],
  'v2 zero-write cancel is executable through the authenticated role'
);
select is(
  (
    select count(*)::integer
    from unnest(array[
      'public.cmd_dataset_flow_identity_capture_attest_guarded(jsonb)'::regprocedure,
      'public.cmd_dataset_flow_identity_scope_preflight_guarded(jsonb)'::regprocedure,
      'public.cmd_dataset_flow_identity_process_rewrite_guarded(uuid,jsonb)'::regprocedure,
      'public.cmd_dataset_flow_identity_scope_read(uuid)'::regprocedure,
      'public.cmd_dataset_flow_identity_scope_finalize_guarded(uuid,jsonb)'::regprocedure,
      'public.cmd_dataset_flow_identity_scope_cancel_guarded(uuid,jsonb)'::regprocedure
    ]) as function_oid(oid)
    where has_function_privilege('anon', function_oid.oid, 'EXECUTE')
       or has_function_privilege('service_role', function_oid.oid, 'EXECUTE')
  ),
  0,
  'anon and service_role have no direct v2 execution grant'
);
select ok(
  (
    select bool_and(
      function.prosecdef
      and function.proconfig @> array['search_path=""']::text[]
    )
    from pg_proc as function
    where function.oid = any(array[
      'public.cmd_dataset_flow_identity_capture_attest_guarded(jsonb)'::regprocedure,
      'public.cmd_dataset_flow_identity_scope_preflight_guarded(jsonb)'::regprocedure,
      'public.cmd_dataset_flow_identity_process_rewrite_guarded(uuid,jsonb)'::regprocedure,
      'public.cmd_dataset_flow_identity_scope_read(uuid)'::regprocedure,
      'public.cmd_dataset_flow_identity_scope_finalize_guarded(uuid,jsonb)'::regprocedure,
      'public.cmd_dataset_flow_identity_scope_cancel_guarded(uuid,jsonb)'::regprocedure
    ]::oid[])
  ),
  'every public v2 RPC is security definer with an empty search_path'
);
select ok(
  not has_function_privilege(
    'authenticated',
    'private.dataset_flow_identity_safe_json_v2(jsonb)',
    'EXECUTE'
  ),
  'authenticated cannot call the private safe-JSON primitive directly'
);
select ok(
  not has_function_privilege(
    'authenticated',
    'private.dataset_flow_identity_short_description_v2(jsonb)',
    'EXECUTE'
  ),
  'authenticated cannot call the private STMultiLang primitive directly'
);
select ok(
  not has_function_privilege(
    'authenticated',
    'private.dataset_flow_identity_scope_cancel_core_v1(uuid,jsonb)',
    'EXECUTE'
  )
  and not has_function_privilege(
    'service_role',
    'private.dataset_flow_identity_scope_cancel_core_v1(uuid,jsonb)',
    'EXECUTE'
  ),
  'the legacy cancel core is private to the v2 database implementation'
);
select ok(
  pg_get_functiondef(
    'public.cmd_dataset_flow_identity_scope_cancel_guarded(uuid,jsonb)'::regprocedure
  ) like '%v_scope.status <> ''sealed''%'
  and pg_get_functiondef(
    'public.cmd_dataset_flow_identity_scope_cancel_guarded(uuid,jsonb)'::regprocedure
  ) like '%v_completed_count <> 0%'
  and pg_get_functiondef(
    'public.cmd_dataset_flow_identity_scope_cancel_guarded(uuid,jsonb)'::regprocedure
  ) like '%v_scope.status = ''cancelled''%'
  and pg_get_functiondef(
    'public.cmd_dataset_flow_identity_scope_cancel_guarded(uuid,jsonb)'::regprocedure
  ) like '%v_audit_count <> 1%',
  'v2 cancel is sealed-zero-write-only and exact replay verifies one audit'
);

-- Receipt rows and their relation-shaped children are immutable, and the
-- active-row fence authorizes primary writes only by consuming a private
-- transaction-bound nonce row.  Caller-forged custom GUCs are not consulted.
select is(
  (
    select count(*)::integer
    from pg_trigger as trigger
    where not trigger.tgisinternal
      and trigger.tgname = any(array[
        'dataset_flow_identity_capture_receipts_immutable',
        'dataset_flow_identity_capture_sources_immutable',
        'dataset_flow_identity_capture_targets_immutable',
        'dataset_flow_identity_capture_support_immutable',
        'dataset_flow_identity_capture_mappings_immutable',
        'dataset_flow_identity_capture_processes_immutable'
      ])
      and trigger.tgenabled = 'O'
      and trigger.tgfoid =
        'private.dataset_flow_identity_receipt_immutable_v2()'::regprocedure
  ),
  6,
  'all six capture receipt relations have enabled immutable triggers'
);
select ok(
  pg_get_functiondef(
    'private.dataset_flow_identity_receipt_immutable_v2()'::regprocedure
  ) like '%FLOW_IDENTITY_CAPTURE_RECEIPT_IMMUTABLE%',
  'receipt immutability trigger always raises the dedicated fail-closed code'
);
select is(
  (
    select count(*)::integer
    from pg_trigger as trigger
    where not trigger.tgisinternal
      and trigger.tgname = any(array[
        'dataset_flow_identity_process_active_fence',
        'dataset_flow_identity_flow_active_fence',
        'dataset_flow_identity_flowproperty_active_fence',
        'dataset_flow_identity_unitgroup_active_fence'
      ])
      and trigger.tgenabled = 'O'
      and trigger.tgfoid =
        'private.dataset_flow_identity_active_fence_v2()'::regprocedure
  ),
  4,
  'all four primary/support tables have the enabled v2 active fence'
);
select ok(
  pg_get_functiondef(
    'private.dataset_flow_identity_active_fence_v2()'::regprocedure
  ) like '%pg_try_advisory_xact_lock%'
  and pg_get_functiondef(
    'private.dataset_flow_identity_active_fence_v2()'::regprocedure
  ) like '%FLOW_IDENTITY_ACTIVE_SCOPE_ACTOR_FENCE_BUSY%'
  and pg_get_functiondef(
    'private.dataset_flow_identity_active_fence_v2()'::regprocedure
  ) not like '%perform pg_advisory_xact_lock(%',
  'row triggers use non-blocking actor locks and fail closed instead of waiting'
);
select ok(
  pg_get_functiondef(
    'private.dataset_flow_identity_active_fence_v2()'::regprocedure
  ) like '%delete from util.dataset_flow_identity_mutation_permits%'
  and pg_get_functiondef(
    'private.dataset_flow_identity_active_fence_v2()'::regprocedure
  ) not like '%current_setting%'
  and pg_get_functiondef(
    'private.dataset_flow_identity_active_fence_v2()'::regprocedure
  ) not like '%app.dataset_flow_identity_v2_process_request_sha256%',
  'active fence consumes a private permit and has no forged-GUC bypass path'
);
select is(
  set_config(
    'app.dataset_flow_identity_v2_process_request_sha256', repeat('f', 64), true
  ),
  repeat('f', 64),
  'a caller can forge the custom GUC value, so it cannot be an authority'
);
select is(
  (
    select count(*)::integer
    from util.dataset_flow_identity_mutation_permits as permit
    where permit.transaction_id = txid_current()
  ),
  0,
  'forging the legacy GUC does not mint a transaction-bound mutation permit'
);
select ok(
  not has_table_privilege(
    'authenticated', 'util.dataset_flow_identity_mutation_permits', 'SELECT'
  )
  and not has_table_privilege(
    'authenticated', 'util.dataset_flow_identity_mutation_permits', 'INSERT'
  )
  and not has_table_privilege(
    'service_role', 'util.dataset_flow_identity_mutation_permits', 'SELECT'
  )
  and not has_table_privilege(
    'service_role', 'util.dataset_flow_identity_mutation_permits', 'INSERT'
  ),
  'authenticated and service_role cannot inspect or mint private mutation permits'
);
select ok(
  not has_table_privilege(
    'authenticated', 'util.dataset_flow_identity_capture_receipts', 'SELECT'
  )
  and not has_table_privilege(
    'service_role', 'util.dataset_flow_identity_capture_receipts', 'SELECT'
  )
  and not has_table_privilege(
    'authenticated', 'util.dataset_flow_identity_capture_process_intents', 'SELECT'
  )
  and not has_table_privilege(
    'service_role', 'util.dataset_flow_identity_capture_process_intents', 'SELECT'
  ),
  'receipt and process-intent proofs are not exposed as directly readable tables'
);

select * from finish();
rollback;
