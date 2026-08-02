begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(29);

select has_function(
  'api',
  'cmd_dataset_save_draft',
  array['text', 'uuid', 'text', 'jsonb', 'uuid', 'boolean', 'jsonb'],
  'Issue 357 creates the api save-draft facade'
);

select has_function(
  'private',
  'lcia_scope_closure_sha256',
  array['jsonb'],
  'Issue 357 moves the Worker hash implementation to private'
);

select has_function(
  'public',
  'lcia_scope_closure_sha256',
  array['jsonb'],
  'Issue 357 retains the public hash compatibility identity'
);

select has_function(
  'public',
  'cmd_dataset_save_draft',
  array['text', 'uuid', 'text', 'jsonb', 'uuid', 'boolean', 'jsonb'],
  'Issue 357 retains the public save-draft compatibility identity'
);

select ok(
  (
    select not routine.prosecdef and routine.provolatile = 'v'
    from pg_proc routine
    where routine.oid = 'api.cmd_dataset_save_draft(text,uuid,text,jsonb,uuid,boolean,jsonb)'::regprocedure
  ),
  'api save-draft facade is a volatile SECURITY INVOKER function'
);

select ok(
  (
    select not routine.prosecdef
      and routine.provolatile = 'i'
      and routine.prosrc ~ 'extensions\.digest'
    from pg_proc routine
    where routine.oid = 'private.lcia_scope_closure_sha256(jsonb)'::regprocedure
  ),
  'private hash is the immutable physical implementation'
);

select ok(
  (
    select not routine.prosecdef
      and routine.provolatile = 'i'
      and routine.prosrc ~ 'private\.lcia_scope_closure_sha256'
    from pg_proc routine
    where routine.oid = 'public.lcia_scope_closure_sha256(jsonb)'::regprocedure
  ),
  'public hash is an immutable SECURITY INVOKER compatibility wrapper'
);

select function_privs_are(
  'api',
  'cmd_dataset_save_draft',
  array['text', 'uuid', 'text', 'jsonb', 'uuid', 'boolean', 'jsonb'],
  'authenticated',
  array['EXECUTE'],
  'authenticated can execute the api save-draft facade'
);

select function_privs_are(
  'api',
  'cmd_dataset_save_draft',
  array['text', 'uuid', 'text', 'jsonb', 'uuid', 'boolean', 'jsonb'],
  'service_role',
  array['EXECUTE'],
  'service_role can execute the api save-draft facade'
);

select function_privs_are(
  'api',
  'cmd_dataset_save_draft',
  array['text', 'uuid', 'text', 'jsonb', 'uuid', 'boolean', 'jsonb'],
  'anon',
  array[]::text[],
  'anon cannot execute the api save-draft facade'
);

select function_privs_are(
  'private',
  'lcia_scope_closure_sha256',
  array['jsonb'],
  'service_role',
  array['EXECUTE'],
  'service role can execute the private Worker hash'
);

select function_privs_are(
  'private',
  'lcia_scope_closure_sha256',
  array['jsonb'],
  'api_internal_executor',
  array['EXECUTE'],
  'internal database callers can execute the private Worker hash'
);

select function_privs_are(
  'private',
  'lcia_scope_closure_sha256',
  array['jsonb'],
  'authenticated',
  array[]::text[],
  'authenticated cannot execute the private Worker hash'
);

select function_privs_are(
  'private',
  'lcia_scope_closure_sha256',
  array['jsonb'],
  'anon',
  array[]::text[],
  'anon cannot execute the private Worker hash'
);

select ok(
  has_schema_privilege('service_role', 'private', 'USAGE')
    and has_schema_privilege('api_internal_executor', 'private', 'USAGE'),
  'reviewed internal roles can resolve private'
);

select ok(
  not has_schema_privilege('authenticated', 'private', 'USAGE')
    and not has_schema_privilege('anon', 'private', 'USAGE'),
  'browser roles cannot resolve private'
);

select is(
  private.lcia_scope_closure_sha256(null),
  public.lcia_scope_closure_sha256(null),
  'NULL hash parity is preserved'
);

select is(
  private.lcia_scope_closure_sha256('{"b":2,"a":1}'::jsonb),
  public.lcia_scope_closure_sha256('{"a":1,"b":2}'::jsonb),
  'JSON object key-order hash parity is preserved'
);

select is(
  private.lcia_scope_closure_sha256(
    '{"nested":{"unicode":"生命周期","items":[1,true,null]}}'::jsonb
  ),
  public.lcia_scope_closure_sha256(
    '{"nested":{"items":[1,true,null],"unicode":"生命周期"}}'::jsonb
  ),
  'nested Unicode and array hash parity is preserved'
);

select isnt(
  private.lcia_scope_closure_sha256('[1,2,3]'::jsonb),
  private.lcia_scope_closure_sha256('[3,2,1]'::jsonb),
  'array order remains hash-significant'
);

select is(
  private.lcia_scope_closure_sha256(null),
  '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a',
  'NULL keeps the origin/dev golden hash'
);

select is(
  private.lcia_scope_closure_sha256('{"a":1,"b":2}'::jsonb),
  'd8497d9d82770a70729261095aa98f7ef5154d7af499f8037b6ca250296785a6',
  'canonical JSON object keeps the origin/dev golden hash'
);

select is(
  private.lcia_scope_closure_sha256(
    '{"nested":{"items":[1,true,null],"unicode":"生命周期"}}'::jsonb
  ),
  'f081ed2e734fcfbc916c4926518100fef21c02a17cf464f1a4dbe4bda4fbe2d5',
  'nested Unicode document keeps the origin/dev golden hash'
);

select is(
  private.lcia_scope_closure_sha256('[1,2,3]'::jsonb),
  'a36b1f2c3f84522dd1005145646617d7054c0851e97c72a039c0bdfac9fa07f3',
  'ordered JSON array keeps the origin/dev golden hash'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '35700000-0000-4000-8000-000000000101', true);
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config(
  'request.jwt.claims',
  '{"sub":"35700000-0000-4000-8000-000000000101","role":"authenticated"}',
  true
);

select is(
  api.cmd_dataset_save_draft(
    'not_a_dataset',
    '35700000-0000-4000-8000-000000000201',
    '01.00.000',
    '{}'::jsonb
  ),
  public.cmd_dataset_save_draft(
    'not_a_dataset',
    '35700000-0000-4000-8000-000000000201',
    '01.00.000',
    '{}'::jsonb
  ),
  'api and public JWT paths preserve invalid-table behavior exactly'
);

select is(
  api.cmd_dataset_save_draft(
    'contacts',
    '35700000-0000-4000-8000-000000000202',
    '01.00.000',
    '{}'::jsonb
  ),
  public.cmd_dataset_save_draft(
    'contacts',
    '35700000-0000-4000-8000-000000000202',
    '01.00.000',
    '{}'::jsonb
  ),
  'api and public JWT paths preserve missing-row behavior exactly'
);

reset role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', '', true);
select set_config('request.jwt.claims', '{}', true);

select is(
  api.cmd_dataset_save_draft(
    'contacts',
    '35700000-0000-4000-8000-000000000202',
    '01.00.000',
    '{}'::jsonb
  )->>'code',
  'AUTH_REQUIRED',
  'api save-draft remains fail-closed without a JWT actor'
);

select is(
  (
    select count(*)::integer
    from pg_proc routine
    join pg_namespace namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'private'
      and routine.proname = 'lcia_scope_closure_sha256'
  ),
  1,
  'exactly one private hash implementation exists'
);

select is(
  (
    select count(*)::integer
    from pg_proc routine
    join pg_namespace namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'api'
      and routine.proname = 'cmd_dataset_save_draft'
  ),
  1,
  'exactly one api save-draft overload exists'
);

select * from finish();

rollback;
