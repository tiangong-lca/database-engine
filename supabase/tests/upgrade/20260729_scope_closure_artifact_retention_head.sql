begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;
select no_plan();

select is(
  (
    select certificate_status
    from private.lcia_scope_closure_checks
    where id = '30830000-0000-4000-8000-000000000301'
  ),
  'stale',
  'base-to-head migration atomically downgrades expired historical valid evidence'
);
select is(
  (
    select complete_machine_result_artifact_id
    from private.lcia_scope_closure_checks
    where id = '30830000-0000-4000-8000-000000000301'
  ),
  '30830000-0000-4000-8000-000000000202'::uuid,
  'historical stale certificate retains resolved machine-result audit lineage'
);
select ok(
  (
    select valid_until < now()
    from private.lcia_scope_closure_checks
    where id = '30830000-0000-4000-8000-000000000301'
  ),
  'historical stale certificate retains its elapsed evidence deadline'
);
select is(
  (
    select certificate_status || ':' || reason
    from private.lcia_scope_closure_certificate_events
    where closure_check_id = '30830000-0000-4000-8000-000000000301'
    order by created_at desc, id desc
    limit 1
  ),
  'stale:artifact_retention_migration_evidence_expired_or_incomplete',
  'historical downgrade retains a durable certificate audit event'
);
select is(
  (
    select string_agg(lifecycle_state, ',' order by id)
    from private.worker_job_artifacts
    where job_id = '30830000-0000-4000-8000-000000000101'
  ),
  'expired,expired,expired',
  'all eight-day historical evidence is classified expired'
);
select throws_ok(
  $$
    update private.lcia_scope_closure_checks
    set certificate_status = 'valid'
    where id = '30830000-0000-4000-8000-000000000301'
  $$,
  '23514',
  'closure_certificate_evidence_lifecycle_invalid',
  'past finished_at plus elapsed evidence cannot form or retain a valid certificate'
);
select throws_ok(
  $$
    insert into private.worker_jobs (
      id, job_kind, worker_runtime, worker_queue, requester_type, requested_by,
      visibility, payload_schema_version, payload_json, status
    ) values (
      '30830000-0000-4000-8000-000000000401',
      'lcia_result.package_build', 'calculator', 'solver', 'operator',
      '30830000-0000-4000-8000-000000000001', 'operator',
      'lcia_result.package_build.request.v2',
      '{"closure_check_id":"30830000-0000-4000-8000-000000000301"}',
      'queued'
    )
  $$,
  '23514',
  'closure_certificate_expired_or_unavailable',
  'build admission fails closed after historical certificate downgrade'
);
select is(
  (
    select count(*)
    from supabase_migrations.schema_migrations
    where version in (
      '20260729014734',
      '20260729030109',
      '20260729045326',
      '20260729060609',
      '20260729070000'
    )
  ),
  5::bigint,
  'all five PR migrations committed without partial DDL'
);
select has_table(
  'public', 'lcia_scope_closure_artifact_write_sets',
  'final additive staging contract exists after real base-to-head upgrade'
);
select has_function(
  'public', 'svc_lcia_scope_closure_artifact_gc_renew',
  array['uuid', 'integer'],
  'final additive GC renewal contract exists after real upgrade'
);

select * from finish();
rollback;
