begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;
select no_plan();

select has_table(
  'private', 'lcia_document_validation_evidence',
  'the canonical evidence cache is a private physical table'
);
select has_view(
  'public', 'lcia_document_validation_evidence',
  'the legacy public identity is an Expand compatibility view'
);

select is(
  (select relation.relkind
   from pg_class relation
   where relation.oid = 'private.lcia_document_validation_evidence'::regclass),
  'r'::"char",
  'the private canonical relation remains an ordinary table'
);
select ok(
  (select relation.relrowsecurity and not relation.relforcerowsecurity
   from pg_class relation
   where relation.oid = 'private.lcia_document_validation_evidence'::regclass),
  'the namespace move preserves enabled non-forced RLS'
);
select is(
  (select count(*)::integer
   from pg_policy
   where polrelid = 'private.lcia_document_validation_evidence'::regclass),
  0,
  'the namespace move does not invent an RLS policy'
);

select is(
  (select array_agg(attribute.attname order by attribute.attnum)
   from pg_attribute attribute
   where attribute.attrelid =
       'private.lcia_document_validation_evidence'::regclass
     and attribute.attnum > 0
     and not attribute.attisdropped),
  array[
    'id', 'dataset_type', 'dataset_id', 'dataset_version',
    'canonical_content_hash', 'document_validator_version',
    'document_validation_profile', 'validation_report_schema_version',
    'validator_engine_fingerprint', 'tidas_schema_lock_sha256', 'status',
    'summary', 'issue_artifact_ref', 'issue_artifact_hash',
    'source_worker_job_id', 'created_at'
  ]::name[],
  'the physical table retains the exact sixteen-column contract'
);
select is(
  (select array_agg(attribute.attname order by attribute.attnum)
   from pg_attribute attribute
   where attribute.attrelid =
       'public.lcia_document_validation_evidence'::regclass
     and attribute.attnum > 0
     and not attribute.attisdropped),
  array[
    'id', 'dataset_type', 'dataset_id', 'dataset_version',
    'canonical_content_hash', 'document_validator_version',
    'document_validation_profile', 'validation_report_schema_version',
    'validator_engine_fingerprint', 'tidas_schema_lock_sha256', 'status',
    'summary', 'issue_artifact_ref', 'issue_artifact_hash',
    'source_worker_job_id', 'created_at'
  ]::name[],
  'the compatibility view exposes only the reviewed sixteen columns'
);

select is(
  (select relation.reloptions
   from pg_class relation
   where relation.oid = 'public.lcia_document_validation_evidence'::regclass),
  array['security_invoker=true']::text[],
  'the public compatibility view evaluates permissions as its caller'
);
select ok(
  (pg_relation_is_updatable(
    'public.lcia_document_validation_evidence'::regclass, true
  ) & 28) = 28,
  'the simple compatibility view preserves insert, update, and delete behavior'
);

select is(
  (select count(*)::integer
   from pg_indexes
   where schemaname = 'private'
     and tablename = 'lcia_document_validation_evidence'),
  4,
  'all four physical indexes move with the table'
);
select ok(
  not exists (
    select 1 from pg_indexes
    where schemaname = 'public'
      and tablename = 'lcia_document_validation_evidence'
  )
  and exists (
    select 1 from pg_indexes
    where schemaname = 'private'
      and indexname = 'lcia_document_validation_evidence_lookup_idx'
  )
  and exists (
    select 1 from pg_indexes
    where schemaname = 'private'
      and indexname =
        'lcia_document_validation_evidence_source_worker_job_idx'
  ),
  'no stale public index identity remains after the physical move'
);
select ok(
  exists (
    select 1
    from pg_constraint constraint_row
    where constraint_row.conrelid =
        'private.lcia_document_validation_evidence'::regclass
      and constraint_row.conname =
        'lcia_document_validation_evidence_source_worker_job_id_fkey'
      and constraint_row.confrelid = 'private.worker_jobs'::regclass
      and constraint_row.confdeltype = 'n'
  ),
  'the Worker job foreign key remains bound to the private physical table'
);

select ok(
  has_table_privilege(
    'service_role', 'private.lcia_document_validation_evidence',
    'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
  )
  and has_table_privilege(
    'api_internal_executor', 'private.lcia_document_validation_evidence',
    'SELECT'
  ),
  'the physical table preserves the predecessor service ACL'
);
select ok(
  has_table_privilege(
    'service_role', 'public.lcia_document_validation_evidence',
    'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
  )
  and has_table_privilege(
    'api_internal_executor', 'public.lcia_document_validation_evidence',
    'SELECT'
  ),
  'the compatibility view preserves the legacy public ACL'
);
select ok(
  not has_schema_privilege('anon', 'private', 'USAGE')
  and not has_schema_privilege('authenticated', 'private', 'USAGE')
  and not has_table_privilege(
    'lca_worker_runtime', 'private.lcia_document_validation_evidence',
    'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
  )
  and not has_table_privilege(
    'lca_worker_runtime', 'public.lcia_document_validation_evidence',
    'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
  ),
  'browser and Worker roles gain no direct relation path'
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
     and pg_get_functiondef(procedure.oid) like
       '%private.lcia_document_validation_evidence%'
     and pg_get_functiondef(procedure.oid) not like
       '%public.lcia_document_validation_evidence%'),
  2,
  'both canonical routines bind only the private physical identity'
);
select ok(
  has_function_privilege(
    'lca_worker_runtime',
    'private.svc_lcia_document_validation_evidence_lookup(jsonb)',
    'EXECUTE'
  )
  and has_function_privilege(
    'lca_worker_runtime',
    'private.svc_lcia_document_validation_evidence_record(jsonb,uuid)',
    'EXECUTE'
  ),
  'the Worker runtime retains only its private routine capability'
);

select is(
  obj_description(
    'private.lcia_document_validation_evidence'::regclass, 'pg_class'
  ),
  'Issue #407 Phase B canonical document-validation evidence cache. Worker access is mediated by the two private routines; the predecessor service ACL is preserved and no Worker relation grant is added.',
  'the canonical table documents its access boundary'
);
select is(
  obj_description(
    'public.lcia_document_validation_evidence'::regclass, 'pg_class'
  ),
  'Issue #407 Expand compatibility view; canonical=private.lcia_document_validation_evidence; fallback=none; remove only after family runtime/static/owner zero, burn-in, and Contract approval.',
  'the compatibility view documents its removal gate'
);

select is(
  (select count(*) from private.lcia_document_validation_evidence),
  (select count(*) from public.lcia_document_validation_evidence),
  'canonical and compatibility reads have exact row-count parity'
);

select * from finish();
rollback;
