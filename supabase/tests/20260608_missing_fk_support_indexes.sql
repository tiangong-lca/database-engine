begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create or replace function pg_temp.has_usable_index_prefix(
  p_schema text,
  p_table text,
  p_columns text[]
) returns boolean
language sql
stable
as $$
  select exists (
    select 1
    from pg_catalog.pg_namespace as namespace
    join pg_catalog.pg_class as relation
      on relation.relnamespace = namespace.oid
    join pg_catalog.pg_index as idx
      on idx.indrelid = relation.oid
    where namespace.nspname = p_schema
      and relation.relname = p_table
      and idx.indisvalid
      and idx.indisready
      and idx.indislive
      and idx.indnkeyatts >= cardinality(p_columns)
      and array(
        select attribute.attname::text
        from unnest(idx.indkey) with ordinality as key(attnum, ordinal)
        join pg_catalog.pg_attribute as attribute
          on attribute.attrelid = relation.oid
         and attribute.attnum = key.attnum
        where key.ordinal <= cardinality(p_columns)
        order by key.ordinal
      ) = p_columns
  );
$$;

select plan(23);

select ok(
  pg_temp.has_usable_index_prefix(
    'public',
    'dataset_review_submit_gate_runs',
    array['supersedes_gate_run_id']
  ),
  'dataset_review_submit_gate_runs has a usable supersedes gate run support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'private',
    'lca_network_snapshots',
    array['lcia_method_id', 'lcia_method_version']
  ),
  'private lca_network_snapshots physical table has a usable LCIA method support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'public',
    'lca_package_request_cache',
    array['export_artifact_id']
  ),
  'lca_package_request_cache has a usable export artifact support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'public',
    'lca_package_request_cache',
    array['report_artifact_id']
  ),
  'lca_package_request_cache has a usable report artifact support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'public',
    'lca_result_cache',
    array['snapshot_id']
  ),
  'lca_result_cache has a usable snapshot support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'public',
    'notifications',
    array['sender_user_id']
  ),
  'notifications has a usable sender user support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'private',
    'worker_jobs',
    array['job_kind']
  ),
  'private worker_jobs physical table has a usable job kind support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'private',
    'lcia_document_validation_evidence',
    array['source_worker_job_id']
  ),
  'private lcia_document_validation_evidence has a usable worker job support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'public',
    'lcia_result_packages',
    array['latest_all_unit_result_id']
  ),
  'lcia_result_packages has a usable latest all-unit result support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'public',
    'lcia_result_packages',
    array['result_id']
  ),
  'lcia_result_packages has a usable result support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'public',
    'lcia_result_packages',
    array['snapshot_id']
  ),
  'lcia_result_packages has a usable snapshot support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'public',
    'lcia_scope_closure_checks',
    array['report_artifact_id']
  ),
  'lcia_scope_closure_checks has a usable report artifact support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'public',
    'lcia_scope_closure_checks',
    array['reused_from_check_id']
  ),
  'lcia_scope_closure_checks has a usable reuse support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'public',
    'lcia_scope_closure_scan_executions',
    array['completed_check_id']
  ),
  'lcia_scope_closure_scan_executions has a usable completed-check support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'public',
    'lcia_scope_closure_scan_executions',
    array['data_snapshot_token']
  ),
  'lcia_scope_closure_scan_executions has a usable snapshot-token support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'public',
    'lcia_scope_closure_scan_executions',
    array['leased_by_job_id']
  ),
  'lcia_scope_closure_scan_executions has a usable lease-job support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'util',
    'dataset_derivative_rebuild_permits',
    array['proposal_id']
  ),
  'dataset_derivative_rebuild_permits has a usable proposal support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'util',
    'dataset_flow_identity_mutation_permits',
    array['scope_id', 'ordinal']
  ),
  'dataset_flow_identity_mutation_permits has a usable scope and ordinal support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'util',
    'dataset_flow_identity_process_ledger',
    array['audit_id']
  ),
  'dataset_flow_identity_process_ledger has a usable audit support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'util',
    'dataset_flow_identity_scopes',
    array['final_wrapper_invocation_id']
  ),
  'dataset_flow_identity_scopes has a usable final invocation support index'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'util',
    'dataset_flow_identity_scopes',
    array['receipt_id']
  ),
  'dataset_flow_identity_scopes has a usable receipt support index'
);

select ok(
  to_regclass('public.lca_release_artifacts_run_idx') is null,
  'the redundant lca_release_artifacts run index is absent'
);

select ok(
  pg_temp.has_usable_index_prefix(
    'public',
    'lca_release_artifacts',
    array['release_run_id', 'profile_id', 'artifact_format']
  )
  and exists (
    select 1
    from pg_catalog.pg_class as index_relation
    join pg_catalog.pg_index as idx
      on idx.indexrelid = index_relation.oid
    where index_relation.oid = to_regclass(
      'public.lca_release_artifacts_profile_format_unique'
    )
      and idx.indisunique
      and idx.indisvalid
      and idx.indisready
      and idx.indislive
  ),
  'the release artifact profile-format unique index remains usable and unique'
);

select * from finish();
rollback;
