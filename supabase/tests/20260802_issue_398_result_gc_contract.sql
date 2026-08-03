begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;
select no_plan();

select has_function('private', 'worker_lca_result_gc_attest_v1', array['uuid']);
select has_function('private', 'worker_lca_result_gc_preview_v1', array['integer']);
select has_function('private', 'worker_lca_result_gc_claim_v1', array['text', 'integer', 'integer']);
select has_function('private', 'worker_lca_result_gc_renew_v1', array['uuid', 'uuid', 'integer']);
select has_function('private', 'worker_lca_result_gc_fence_v1', array['uuid', 'uuid']);
select has_function('private', 'worker_lca_result_gc_finalize_v1', array['uuid', 'uuid', 'text']);
select has_function('private', 'worker_lca_result_gc_fail_v1', array['uuid', 'uuid', 'text']);

select ok(
  (select rolinherit and not rolsuper and not rolcanlogin and not rolbypassrls
   from pg_roles where rolname = 'lca_result_gc_executor'),
  'the executor is NOLOGIN, non-superuser and cannot bypass RLS'
);
select ok(
  (select rolinherit and not rolsuper and not rolcanlogin and not rolbypassrls
   from pg_roles where rolname = 'lca_worker_runtime'),
  'the runtime group is NOLOGIN, non-superuser and cannot bypass RLS'
);
select is(
  (select count(*)::integer
   from pg_roles member_role
   cross join pg_roles granted_role
   where member_role.rolname in (
     'anon', 'authenticated', 'service_role', 'api_internal_executor',
     'lca_worker_runtime', 'lca_result_gc_executor'
   )
     and granted_role.rolname in (
       'lca_worker_runtime', 'lca_result_gc_executor'
     )
     and member_role.rolname <> granted_role.rolname
     and pg_has_role(member_role.oid, granted_role.oid, 'member')),
  0,
  'protected API/service roles cannot transitively inherit runtime or executor authority'
);
select is(
  (select count(*)::integer
   from pg_auth_members membership
   where membership.member = 'postgres'::regrole
     and membership.roleid in (
       'lca_worker_runtime'::regrole, 'lca_result_gc_executor'::regrole
     )
     and membership.grantor = 'supabase_admin'::regrole
     and membership.admin_option
     and not membership.inherit_option
     and not membership.set_option),
  2,
  'PG17 preserves exactly one non-INHERIT/non-SET creator-admin edge per protected role'
);
select is(
  (select count(*)::integer
   from pg_auth_members membership
   where (
     membership.member in (
       'lca_worker_runtime'::regrole, 'lca_result_gc_executor'::regrole
     )
     or membership.roleid in (
       'lca_worker_runtime'::regrole, 'lca_result_gc_executor'::regrole
     )
   )
     and not (
       membership.member = 'postgres'::regrole
       and membership.roleid in (
         'lca_worker_runtime'::regrole, 'lca_result_gc_executor'::regrole
       )
       and membership.grantor = 'supabase_admin'::regrole
       and membership.admin_option
       and not membership.inherit_option
       and not membership.set_option
     )),
  0,
  'protected roles have no edge beyond the exact PG17 creator baseline'
);
select is(
  (select claims_enabled from private.lca_result_gc_control where singleton),
  false,
  'result GC claims are disabled by default'
);
select has_column(
  'public', 'lca_results', 'retention_partition_key',
  'lca_results exposes the additive nullable GC partition key'
);
select ok(
  exists (
    select 1 from pg_constraint
    where conrelid = 'public.lca_results'::regclass
      and conname = 'lca_results_retention_partition_key_chk'
      and not convalidated
  ),
  'partition hash check is enforced for new writes without scanning history'
);
select ok(
  (select count(*) = 4 and bool_and(relrowsecurity and relforcerowsecurity)
   from pg_class
   where oid in (
     'private.lca_result_gc_control'::regclass,
     'private.lca_result_gc_operations'::regclass,
     'private.lca_result_gc_attest_context'::regclass,
     'private.lca_result_gc_finalize_context'::regclass
   )),
  'all private coordinator and capability tables force RLS'
);
select is(
  (select count(*)::integer
   from pg_proc proc
   join pg_namespace namespace on namespace.oid = proc.pronamespace
   where namespace.nspname = 'private'
     and proc.proname like 'worker_lca_result_gc_%_v1'
     and proc.prosecdef
     and proc.proowner = 'lca_result_gc_executor'::regrole
     and proc.proconfig = array['search_path=pg_catalog, pg_temp']::text[]),
  7,
  'all seven versioned worker routines use the dedicated executor and explicit safe search_path'
);
select is(
  (select count(*)::integer
   from pg_proc procedure
   join pg_namespace namespace on namespace.oid=procedure.pronamespace
   where namespace.nspname='private'
     and procedure.proname in (
       'lca_result_gc_error',
       'lca_result_gc_caller_allowed',
       'lca_result_gc_ineligibility_reason',
       'lca_result_gc_prepare_identity',
       'worker_lca_result_gc_attest_v1',
       'lca_result_gc_guard_result_write',
       'lca_result_gc_assert_reference_allowed',
       'lca_result_gc_guard_cache_reference',
       'lca_result_gc_guard_latest_reference',
       'lca_result_gc_guard_package_reference',
       'worker_lca_result_gc_preview_v1',
       'worker_lca_result_gc_claim_v1',
       'worker_lca_result_gc_renew_v1',
       'worker_lca_result_gc_fence_v1',
       'worker_lca_result_gc_finalize_v1',
       'worker_lca_result_gc_fail_v1'
     )
     and procedure.proconfig =
       array['search_path=pg_catalog, pg_temp']::text[]),
  16,
  'all sixteen Issue 398 functions use the explicit safe search_path in exact order'
);
select is(
  (select count(*)::integer
   from pg_proc proc
   join pg_namespace namespace on namespace.oid = proc.pronamespace
   where namespace.nspname = 'private'
     and proc.proname like 'worker_lca_result_gc_%_v1'
     and has_function_privilege('lca_worker_runtime', proc.oid, 'EXECUTE')),
  7,
  'runtime role can execute exactly the seven result GC routines'
);
select is(
  (select count(*)::integer
   from pg_proc proc
   join pg_namespace namespace on namespace.oid = proc.pronamespace
   where namespace.nspname = 'private'
     and proc.proname like 'worker_lca_result_gc_%_v1'
     and has_function_privilege('service_role', proc.oid, 'EXECUTE')),
  0,
  'service_role cannot execute result GC routines directly'
);
select ok(
  not has_table_privilege(
    'lca_worker_runtime', 'private.lca_result_gc_operations', 'SELECT'
  ),
  'runtime role cannot read coordinator storage directly'
);

grant lca_worker_runtime, lca_result_gc_executor to postgres;

insert into private.lca_network_snapshots (id, status)
values ('39800000-0000-4000-8000-000000000001', 'ready');
insert into private.worker_jobs (
  id, job_kind, worker_queue, requester_type, requested_by,
  request_hash, status, payload_schema_version
) values (
  '39800000-0000-4000-8000-000000000002',
  'lca.result_gc', 'maintenance', 'user',
  '39800000-0000-4000-8000-000000000003',
  'issue-398-request-hash', 'completed', 'v1'
);

insert into public.lca_results (
  id, job_id, snapshot_id, worker_job_id, artifact_url,
  artifact_sha256, artifact_byte_size, artifact_format, created_at,
  expires_at
) values
(
  '39800000-0000-4000-8000-000000000010',
  '39800000-0000-4000-8000-000000000110',
  '39800000-0000-4000-8000-000000000001',
  '39800000-0000-4000-8000-000000000002',
  's3://test/results/39800000-0000-4000-8000-000000000010/result.json',
  repeat('a', 64), 10, 'json', now() - interval '3 days',
  now() - interval '2 days'
),
(
  '39800000-0000-4000-8000-000000000011',
  '39800000-0000-4000-8000-000000000111',
  '39800000-0000-4000-8000-000000000001',
  '39800000-0000-4000-8000-000000000002',
  's3://test/results/39800000-0000-4000-8000-000000000011/result.json',
  repeat('b', 64), 11, 'json', now() - interval '2 days',
  now() - interval '1 day'
),
(
  '39800000-0000-4000-8000-000000000012',
  '39800000-0000-4000-8000-000000000112',
  '39800000-0000-4000-8000-000000000001',
  '39800000-0000-4000-8000-000000000002',
  's3://test/Results/39800000-0000-4000-8000-000000000012/result.json',
  repeat('c', 64), 12, 'json', now() - interval '1 day',
  now() - interval '1 hour'
);

select is(
  (select count(*)::integer from public.lca_results
   where retention_partition_key is null
     and id in (
       '39800000-0000-4000-8000-000000000010',
       '39800000-0000-4000-8000-000000000011',
       '39800000-0000-4000-8000-000000000012'
     )),
  3,
  'ordinary inserts remain unattested and ineligible'
);
select throws_ok(
  $$insert into public.lca_results (
      id, job_id, snapshot_id, retention_partition_key
    ) values (
      '39800000-0000-4000-8000-000000000013',
      '39800000-0000-4000-8000-000000000113',
      '39800000-0000-4000-8000-000000000001', repeat('d', 64)
    )$$,
  '23514', 'lca_result_gc_attestation_required',
  'callers cannot inject a partition key during insert'
);
select is(
  private.worker_lca_result_gc_attest_v1(
    '39800000-0000-4000-8000-000000000012'
  )->>'code',
  'result_gc_attestation_invalid',
  'attestation rejects a case-drifted locator even when it contains the UUID'
);
update public.lca_results
set artifact_url =
  's3://test/object.json?next=/results/' || id::text || '/result.json'
where id = '39800000-0000-4000-8000-000000000012';
select is(
  private.worker_lca_result_gc_attest_v1(
    '39800000-0000-4000-8000-000000000012'
  )->>'code',
  'result_gc_attestation_invalid',
  'attestation rejects a UUID path smuggled through a query string'
);
select is(
  private.worker_lca_result_gc_attest_v1(
    '39800000-0000-4000-8000-000000000010'
  )->>'outcome',
  'attested',
  'older result is explicitly attested'
);
select is(
  private.worker_lca_result_gc_attest_v1(
    '39800000-0000-4000-8000-000000000011'
  )->>'outcome',
  'attested',
  'newer result is explicitly attested'
);
select is(
  (select count(distinct retention_partition_key)::integer
   from public.lca_results
   where id in (
     '39800000-0000-4000-8000-000000000010',
     '39800000-0000-4000-8000-000000000011'
   )),
  1,
  'request identity deterministically groups results into one partition'
);
select is(
  private.worker_lca_result_gc_attest_v1(
    '39800000-0000-4000-8000-000000000010'
  )->>'outcome',
  'replayed',
  'attestation is idempotent'
);
select throws_ok(
  $$update public.lca_results
    set id = '39800000-0000-4000-8000-000000000099'
    where id = '39800000-0000-4000-8000-000000000010'$$,
  '23514', 'lca_result_gc_identity_is_immutable',
  'attestation freezes the result primary identity'
);
select throws_ok(
  $$update public.lca_results
    set job_id = '39800000-0000-4000-8000-000000000199'
    where id = '39800000-0000-4000-8000-000000000010'$$,
  '23514', 'lca_result_gc_identity_is_immutable',
  'attestation freezes the legacy compatibility job identity'
);
select throws_ok(
  $$delete from public.lca_results
    where id = '39800000-0000-4000-8000-000000000011'$$,
  '55000', 'lca_result_gc_attested_delete_requires_finalize',
  'attested results cannot bypass the finalize state machine'
);
select is(
  private.worker_lca_result_gc_claim_v1('issue-398-worker', 10, 300)->>'code',
  'result_gc_disabled',
  'disabled rollout control rejects claims'
);

set local role lca_result_gc_executor;
update private.lca_result_gc_control
set claims_enabled = true,
    enabled_at = now(),
    enabled_by = 'pgTAP',
    reason = 'isolated issue 398 test';
reset role;

select is(
  jsonb_array_length(
    private.worker_lca_result_gc_preview_v1(10) #> '{data,items}'
  ),
  1,
  'preview selects only the expired non-newest attested result'
);

create temporary table issue_398_claims (
  ordinal integer primary key,
  response jsonb not null
) on commit drop;
insert into issue_398_claims values (
  1, private.worker_lca_result_gc_claim_v1('issue-398-worker-a', 10, 300)
);
grant select on issue_398_claims to lca_result_gc_executor;
select is(
  (select jsonb_array_length(response #> '{data,items}')
   from issue_398_claims where ordinal = 1),
  1,
  'claim returns one independent item'
);
select is(
  (select response #>> '{data,items,0,resultId}'
   from issue_398_claims where ordinal = 1),
  '39800000-0000-4000-8000-000000000010',
  'claim chooses the older result and protects the partition newest'
);

set local role lca_result_gc_executor;
update private.lca_result_gc_operations
set lease_expires_at = clock_timestamp() - interval '1 second'
where operation_id = (
  select (response #>> '{data,items,0,operationId}')::uuid
  from issue_398_claims where ordinal = 1
);
reset role;

insert into issue_398_claims values (
  2, private.worker_lca_result_gc_claim_v1('issue-398-worker-b', 10, 300)
);
select is(
  (select response #>> '{data,items,0,phase}'
   from issue_398_claims where ordinal = 2),
  'claim_recovery',
  'expired claimed work is taken over instead of remaining wedged'
);
select is(
  (select (response #>> '{data,items,0,generation}')::integer
   from issue_398_claims where ordinal = 2),
  2,
  'takeover increments the fencing generation'
);
select isnt(
  (select response #>> '{data,items,0,claimToken}'
   from issue_398_claims where ordinal = 1),
  (select response #>> '{data,items,0,claimToken}'
   from issue_398_claims where ordinal = 2),
  'takeover rotates the per-item claim token'
);
select is(
  private.worker_lca_result_gc_fence_v1(
    (select (response #>> '{data,items,0,operationId}')::uuid
     from issue_398_claims where ordinal = 1),
    (select (response #>> '{data,items,0,claimToken}')::uuid
     from issue_398_claims where ordinal = 1)
  )->>'code',
  'result_gc_claim_invalid',
  'the stale token cannot fence after takeover'
);
select is(
  private.worker_lca_result_gc_fence_v1(
    (select (response #>> '{data,items,0,operationId}')::uuid
     from issue_398_claims where ordinal = 2),
    (select (response #>> '{data,items,0,claimToken}')::uuid
     from issue_398_claims where ordinal = 2)
  )->>'outcome',
  'delete_ready',
  'the current token commits the persistent delete fence'
);
select throws_ok(
  $$delete from public.lca_results
    where id = '39800000-0000-4000-8000-000000000010'$$,
  '55000', 'lca_result_gc_active_operation_blocks_delete',
  'ordinary deletion is blocked while fenced'
);
select throws_ok(
  $$update public.lca_results set payload = '{"changed":true}'::jsonb
    where id = '39800000-0000-4000-8000-000000000010'$$,
  '55000', 'lca_result_gc_delete_fence_blocks_update',
  'ordinary updates are blocked while deleting'
);

create temporary table issue_398_finalize as
select private.worker_lca_result_gc_finalize_v1(
  (select (response #>> '{data,items,0,operationId}')::uuid
   from issue_398_claims where ordinal = 2),
  (select (response #>> '{data,items,0,claimToken}')::uuid
   from issue_398_claims where ordinal = 2),
  'deleted'
) as response;
select is(
  (select response->>'outcome' from issue_398_finalize),
  'finalized',
  'finalize atomically removes the exact DB row'
);
select is(
  (select count(*)::integer from public.lca_results
   where id = '39800000-0000-4000-8000-000000000010'),
  0,
  'finalized result is absent'
);
select ok(
  (select state = 'finalized' and live_result_id is null
     and object_outcome = 'deleted'
   from private.lca_result_gc_operations
   where target_result_id = '39800000-0000-4000-8000-000000000010'),
  'terminal audit survives with the live FK cleared'
);
select is(
  (select count(*)::integer
   from private.lca_result_gc_finalize_context),
  0,
  'unforgeable finalize capability is removed before return'
);
select is(
  private.worker_lca_result_gc_finalize_v1(
    (select (response #>> '{data,items,0,operationId}')::uuid
     from issue_398_claims where ordinal = 2),
    (select (response #>> '{data,items,0,claimToken}')::uuid
     from issue_398_claims where ordinal = 2),
    'deleted'
  )->>'outcome',
  'replayed',
  'same-token finalize replay is idempotent'
);
select is(
  (select count(*)::integer from public.lca_results
   where id = '39800000-0000-4000-8000-000000000011'),
  1,
  'partition newest remains present'
);

select * from finish();
rollback;
