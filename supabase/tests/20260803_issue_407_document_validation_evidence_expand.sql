begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;
select no_plan();

select has_table(
  'public', 'lcia_document_validation_evidence',
  'Phase A retains the public physical evidence table'
);
select ok(
  to_regclass('private.lcia_document_validation_evidence') is null,
  'Phase A does not create a private relation, view, or shadow copy'
);
select has_function(
  'private', 'svc_lcia_document_validation_evidence_lookup', array['jsonb'],
  'private exposes the canonical Worker lookup'
);
select has_function(
  'private', 'svc_lcia_document_validation_evidence_record',
  array['jsonb', 'uuid'],
  'private exposes the canonical Worker record command'
);

select is(
  (select count(*)::integer
   from pg_proc procedure
   join pg_namespace namespace on namespace.oid = procedure.pronamespace
   where namespace.nspname in ('public', 'private')
     and procedure.proname in (
       'svc_lcia_document_validation_evidence_lookup',
       'svc_lcia_document_validation_evidence_record'
     )
     and procedure.proowner = 'postgres'::regrole
     and procedure.prosecdef
     and procedure.proconfig =
       array['search_path=pg_catalog, pg_temp']::text[]),
  4,
  'all canonical routines and wrappers are SECURITY DEFINER with the exact safe search_path'
);

select is(
  (select count(*)::integer
   from pg_proc procedure
   join pg_namespace namespace on namespace.oid = procedure.pronamespace
   join pg_language language on language.oid = procedure.prolang
   where namespace.nspname in ('public', 'private')
     and procedure.proname in (
       'svc_lcia_document_validation_evidence_lookup',
       'svc_lcia_document_validation_evidence_record'
     )
     and language.lanname = 'plpgsql'
     and procedure.prokind = 'f'
     and procedure.prorettype = 'jsonb'::regtype
     and procedure.provolatile = 'v'
     and procedure.proparallel = 'u'
     and not procedure.proretset
     and not procedure.proisstrict
     and not procedure.proleakproof
     and pg_get_function_arguments(procedure.oid) = case
       when procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
         then 'p_cache_keys jsonb'
       else 'p_records jsonb, p_source_worker_job_id uuid DEFAULT NULL::uuid' end
     and obj_description(procedure.oid, 'pg_proc') = case
       when namespace.nspname = 'private'
        and procedure.proname = 'svc_lcia_document_validation_evidence_lookup'
         then 'Issue #407 Phase A canonical Worker lookup. Direct EXECUTE is restricted to lca_worker_runtime; the public relation remains physical until Contract.'
       when namespace.nspname = 'private'
         then 'Issue #407 Phase A canonical Worker idempotent record command. Direct EXECUTE is restricted to lca_worker_runtime; no relation ACL is granted.'
       else 'Issue #407 Phase A compatibility wrapper with caller-category-only LOG telemetry; remove only after attributed consumer-zero evidence.' end),
  4,
  'the exact four-function catalog freezes signature/default/language/runtime metadata/comments'
);

select ok(
  has_schema_privilege('lca_worker_runtime', 'private', 'USAGE')
  and not has_schema_privilege('lca_worker_runtime', 'private', 'CREATE')
  and has_schema_privilege('service_role', 'private', 'USAGE')
  and has_schema_privilege('api_internal_executor', 'private', 'USAGE')
  and not has_schema_privilege('anon', 'private', 'USAGE')
  and not has_schema_privilege('authenticated', 'private', 'USAGE'),
  'private schema USAGE/CREATE role matrix remains exact'
);

select ok(
  exists (
    select 1 from pg_roles role_row
    where role_row.rolname = 'lca_worker_runtime'
      and role_row.rolinherit
      and not role_row.rolsuper and not role_row.rolcreatedb
      and not role_row.rolcreaterole and not role_row.rolcanlogin
      and not role_row.rolbypassrls and not role_row.rolreplication
      and role_row.rolconfig is null
  ),
  'lca_worker_runtime freezes all privileged attributes and role settings'
);

select is(
  (select count(*)::integer
   from pg_auth_members membership
   where membership.member = 'postgres'::regrole
     and membership.roleid = 'lca_worker_runtime'::regrole
     and membership.grantor = 'supabase_admin'::regrole
     and membership.admin_option
     and not membership.inherit_option
     and not membership.set_option),
  1,
  'Supabase bootstrap retains the one exact creator baseline membership edge'
);

select ok(
  position(
    'with ordinality as item(value, ordinal)'
    in lower(pg_get_functiondef(
      'private.svc_lcia_document_validation_evidence_record(jsonb,uuid)'::regprocedure
    ))
  ) > 0,
  'record ordering freezes the nine-key identity with input ordinal as tie-breaker'
);

select is(
  (select count(*)::integer
   from pg_proc procedure
   join pg_namespace namespace on namespace.oid = procedure.pronamespace
   where namespace.nspname = 'private'
     and procedure.proname in (
       'svc_lcia_document_validation_evidence_lookup',
       'svc_lcia_document_validation_evidence_record'
     )
     and has_function_privilege(
       'lca_worker_runtime', procedure.oid, 'EXECUTE'
     )),
  2,
  'lca_worker_runtime can execute exactly both private canonical routines'
);

select is(
  (select count(*)::integer
   from pg_proc procedure
   join pg_namespace namespace on namespace.oid = procedure.pronamespace
   cross join (values
     ('anon'), ('authenticated'), ('service_role'), ('api_internal_executor')
   ) as forbidden(role_name)
   where namespace.nspname = 'private'
     and procedure.proname in (
       'svc_lcia_document_validation_evidence_lookup',
       'svc_lcia_document_validation_evidence_record'
     )
     and has_function_privilege(
       forbidden.role_name, procedure.oid, 'EXECUTE'
     )),
  0,
  'browser, service, and API executor roles cannot call private routines'
);

select ok(
  not exists (
    select 1
    from pg_proc procedure
    join pg_namespace namespace on namespace.oid = procedure.pronamespace
    cross join lateral aclexplode(
      coalesce(procedure.proacl, acldefault('f', procedure.proowner))
    ) privilege
    where namespace.nspname = 'private'
      and procedure.proname in (
        'svc_lcia_document_validation_evidence_lookup',
        'svc_lcia_document_validation_evidence_record'
      )
      and privilege.grantee = 0
      and privilege.privilege_type = 'EXECUTE'
  ),
  'PUBLIC has no direct EXECUTE on either private routine'
);

select ok(
  not has_table_privilege(
    'lca_worker_runtime', 'public.lcia_document_validation_evidence',
    'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
  ),
  'lca_worker_runtime receives no public source-table privilege'
);

select ok(
  has_function_privilege(
    'service_role',
    'public.svc_lcia_document_validation_evidence_lookup(jsonb)', 'EXECUTE'
  )
  and has_function_privilege(
    'service_role',
    'public.svc_lcia_document_validation_evidence_record(jsonb,uuid)', 'EXECUTE'
  )
  and has_function_privilege(
    'api_internal_executor',
    'public.svc_lcia_document_validation_evidence_lookup(jsonb)', 'EXECUTE'
  )
  and has_function_privilege(
    'api_internal_executor',
    'public.svc_lcia_document_validation_evidence_record(jsonb,uuid)', 'EXECUTE'
  )
  and has_function_privilege(
    'postgres',
    'public.svc_lcia_document_validation_evidence_lookup(jsonb)', 'EXECUTE'
  )
  and has_function_privilege(
    'postgres',
    'public.svc_lcia_document_validation_evidence_record(jsonb,uuid)', 'EXECUTE'
  ),
  'public wrappers preserve the service_role, api_internal_executor, and owner compatibility ACL'
);

select ok(
  not has_function_privilege(
    'anon', 'public.svc_lcia_document_validation_evidence_lookup(jsonb)',
    'EXECUTE'
  )
  and not has_function_privilege(
    'authenticated',
    'public.svc_lcia_document_validation_evidence_lookup(jsonb)', 'EXECUTE'
  )
  and not has_function_privilege(
    'lca_worker_runtime',
    'public.svc_lcia_document_validation_evidence_lookup(jsonb)', 'EXECUTE'
  ),
  'public compatibility remains unavailable to browser and direct Worker roles'
);

select ok(
  (select position(
     'util.is_service_request()'
     in pg_get_functiondef(
       'private.svc_lcia_document_validation_evidence_lookup(jsonb)'::regprocedure
     )
   ) = 0)
  and (select position(
     'util.is_service_request()'
     in pg_get_functiondef(
       'private.svc_lcia_document_validation_evidence_record(jsonb,uuid)'::regprocedure
     )
   ) = 0),
  'private authorization does not trust the request custom GUC helper'
);

select ok(
  position(
    'private.svc_lcia_document_validation_evidence_lookup'
    in pg_get_functiondef(
      'public.svc_lcia_document_validation_evidence_lookup(jsonb)'::regprocedure
    )
  ) > 0
  and position(
    'public.lcia_document_validation_evidence'
    in pg_get_functiondef(
      'public.svc_lcia_document_validation_evidence_lookup(jsonb)'::regprocedure
    )
  ) = 0
  and position(
    'private.svc_lcia_document_validation_evidence_record'
    in pg_get_functiondef(
      'public.svc_lcia_document_validation_evidence_record(jsonb,uuid)'::regprocedure
    )
  ) > 0
  and position(
    'insert into public.lcia_document_validation_evidence'
    in lower(pg_get_functiondef(
      'public.svc_lcia_document_validation_evidence_record(jsonb,uuid)'::regprocedure
    ))
  ) = 0,
  'public wrappers have one canonical path and contain no duplicate read/write logic'
);

select ok(
  position(
    'raise log ''issue_407_public_compat function=lookup caller_category=%'''
    in lower(pg_get_functiondef(
      'public.svc_lcia_document_validation_evidence_lookup(jsonb)'::regprocedure
    ))
  ) > 0
  and position(
    'raise log ''issue_407_public_compat function=record caller_category=%'''
    in lower(pg_get_functiondef(
      'public.svc_lcia_document_validation_evidence_record(jsonb,uuid)'::regprocedure
    ))
  ) > 0,
  'both wrappers emit caller-category-only compatibility telemetry'
);

create temporary table issue_407_responses (
  label text primary key,
  response jsonb not null
) on commit drop;

insert into issue_407_responses values
  ('blank_private', private.svc_lcia_document_validation_evidence_lookup('[]'::jsonb));

select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);
select set_config('request.headers', '{}', true);

insert into issue_407_responses values
  ('blank_public', public.svc_lcia_document_validation_evidence_lookup('[]'::jsonb));

select is(
  (select response from issue_407_responses where label = 'blank_public'),
  (select response from issue_407_responses where label = 'blank_private'),
  'blank-cache public and private lookup envelopes are byte-equivalent JSON'
);

insert into issue_407_responses values
  ('invalid_private', private.svc_lcia_document_validation_evidence_lookup(
    '[{"datasetId":"not-a-uuid"}]'::jsonb
  )),
  ('invalid_public', public.svc_lcia_document_validation_evidence_lookup(
    '[{"datasetId":"not-a-uuid"}]'::jsonb
  ));

select is(
  (select response from issue_407_responses where label = 'invalid_public'),
  (select response from issue_407_responses where label = 'invalid_private'),
  'invalid identity preserves the exact public/private error envelope'
);

insert into issue_407_responses values (
  'first_record',
  public.svc_lcia_document_validation_evidence_record(
    '[{
      "datasetType":"Process",
      "datasetId":"40700000-0000-4000-8000-000000000001",
      "datasetVersion":"01.00.000",
      "canonicalContentHash":"issue-407-content",
      "documentValidatorVersion":"issue-407-validator",
      "documentValidationProfile":"issue-407-profile",
      "validationReportSchemaVersion":"v1",
      "validatorEngineFingerprint":"issue-407-engine",
      "tidasSchemaLockSha256":"issue-407-schema",
      "status":"passed",
      "summary":{"issue":407},
      "issueArtifactRef":{"kind":"test"},
      "issueArtifactHash":"issue-407-artifact"
    }]'::jsonb,
    null
  )
);

insert into issue_407_responses values (
  'retry_record',
  private.svc_lcia_document_validation_evidence_record(
    '[{
      "datasetType":"Process",
      "datasetId":"40700000-0000-4000-8000-000000000001",
      "datasetVersion":"01.00.000",
      "canonicalContentHash":"issue-407-content",
      "documentValidatorVersion":"issue-407-validator",
      "documentValidationProfile":"issue-407-profile",
      "validationReportSchemaVersion":"v1",
      "validatorEngineFingerprint":"issue-407-engine",
      "tidasSchemaLockSha256":"issue-407-schema",
      "status":"passed",
      "summary":{"issue":407},
      "issueArtifactRef":{"kind":"test"},
      "issueArtifactHash":"issue-407-artifact"
    }]'::jsonb,
    null
  )
);

select is(
  (select response from issue_407_responses where label = 'first_record'),
  '{"ok":true,"data":{"insertedCount":1}}'::jsonb,
  'the populated public compatibility write inserts once'
);
select is(
  (select response from issue_407_responses where label = 'retry_record'),
  '{"ok":true,"data":{"insertedCount":0}}'::jsonb,
  'the canonical retry converges without replacing prior evidence'
);

insert into issue_407_responses values
  ('populated_private', private.svc_lcia_document_validation_evidence_lookup(
    '[{
      "datasetType":"Process",
      "datasetId":"40700000-0000-4000-8000-000000000001",
      "datasetVersion":"01.00.000",
      "canonicalContentHash":"issue-407-content",
      "documentValidatorVersion":"issue-407-validator",
      "documentValidationProfile":"issue-407-profile",
      "validationReportSchemaVersion":"v1",
      "validatorEngineFingerprint":"issue-407-engine",
      "tidasSchemaLockSha256":"issue-407-schema"
    }]'::jsonb
  )),
  ('populated_public', public.svc_lcia_document_validation_evidence_lookup(
    '[{
      "datasetType":"Process",
      "datasetId":"40700000-0000-4000-8000-000000000001",
      "datasetVersion":"01.00.000",
      "canonicalContentHash":"issue-407-content",
      "documentValidatorVersion":"issue-407-validator",
      "documentValidationProfile":"issue-407-profile",
      "validationReportSchemaVersion":"v1",
      "validatorEngineFingerprint":"issue-407-engine",
      "tidasSchemaLockSha256":"issue-407-schema"
    }]'::jsonb
  ));

select is(
  (select response from issue_407_responses where label = 'populated_public'),
  (select response from issue_407_responses where label = 'populated_private'),
  'populated-cache public and private lookup DTOs remain exactly equal'
);

select is(
  private.svc_lcia_document_validation_evidence_record(
    '[
      {"datasetType":"Process","datasetId":"40700000-0000-4000-8000-000000000011","datasetVersion":"01.00.000","canonicalContentHash":"duplicate-forward","documentValidatorVersion":"issue-407-validator","documentValidationProfile":"issue-407-profile","validationReportSchemaVersion":"v1","validatorEngineFingerprint":"issue-407-engine","tidasSchemaLockSha256":"issue-407-schema","status":"passed","summary":{"winner":"forward-first"},"issueArtifactRef":{}},
      {"datasetType":"Process","datasetId":"40700000-0000-4000-8000-000000000011","datasetVersion":"01.00.000","canonicalContentHash":"duplicate-forward","documentValidatorVersion":"issue-407-validator","documentValidationProfile":"issue-407-profile","validationReportSchemaVersion":"v1","validatorEngineFingerprint":"issue-407-engine","tidasSchemaLockSha256":"issue-407-schema","status":"failed","summary":{"winner":"forward-second"},"issueArtifactRef":{}}
    ]'::jsonb, null
  ),
  '{"ok":true,"data":{"insertedCount":1}}'::jsonb,
  'same-key forward payload inserts exactly the first ordinal'
);
select is(
  (select summary from public.lcia_document_validation_evidence
   where dataset_id = '40700000-0000-4000-8000-000000000011'::uuid),
  '{"winner":"forward-first"}'::jsonb,
  'same-key forward conflict preserves first input payload'
);
select is(
  private.svc_lcia_document_validation_evidence_record(
    '[
      {"datasetType":"Process","datasetId":"40700000-0000-4000-8000-000000000012","datasetVersion":"01.00.000","canonicalContentHash":"duplicate-reverse","documentValidatorVersion":"issue-407-validator","documentValidationProfile":"issue-407-profile","validationReportSchemaVersion":"v1","validatorEngineFingerprint":"issue-407-engine","tidasSchemaLockSha256":"issue-407-schema","status":"failed","summary":{"winner":"reverse-first"},"issueArtifactRef":{}},
      {"datasetType":"Process","datasetId":"40700000-0000-4000-8000-000000000012","datasetVersion":"01.00.000","canonicalContentHash":"duplicate-reverse","documentValidatorVersion":"issue-407-validator","documentValidationProfile":"issue-407-profile","validationReportSchemaVersion":"v1","validatorEngineFingerprint":"issue-407-engine","tidasSchemaLockSha256":"issue-407-schema","status":"passed","summary":{"winner":"reverse-second"},"issueArtifactRef":{}}
    ]'::jsonb, null
  ),
  '{"ok":true,"data":{"insertedCount":1}}'::jsonb,
  'same-key reverse payload inserts exactly the first ordinal'
);
select is(
  (select summary from public.lcia_document_validation_evidence
   where dataset_id = '40700000-0000-4000-8000-000000000012'::uuid),
  '{"winner":"reverse-first"}'::jsonb,
  'same-key reverse conflict preserves the new first input payload'
);

select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select is(
  public.svc_lcia_document_validation_evidence_lookup('[]'::jsonb),
  public.lcia_scope_closure_error(
    'service_role_required', 403, 'Service role is required'
  ),
  'public compatibility keeps the exact unauthorized error envelope'
);

select * from finish();
rollback;
