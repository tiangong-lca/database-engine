begin;

set local lock_timeout = '10s';
set local statement_timeout = '10min';

-- Issue #422 performs a single, OID-preserving schema cutover.
-- public is intentionally limited to the nine durable domain entity tables.
create schema if not exists api authorization postgres;

comment on schema api is
  'Explicit Supabase Data API surface. RPCs and API-facing projections live here.';
comment on schema private is
  'Internal application state and implementation routines; not exposed by PostgREST.';
comment on schema util is
  'Operational controls, queues, diagnostics, and maintenance helpers; not exposed by PostgREST.';
comment on schema archive is
  'Historical and retired data retained outside runtime API namespaces.';

revoke create on schema api, private, util, archive from public;
grant usage on schema api to anon, authenticated, service_role;
grant usage on schema private, util, archive to service_role;

create temporary table issue_422_relation_targets (
  object_name name primary key,
  target_schema name not null check (target_schema in ('api', 'private', 'util', 'archive'))
) on commit drop;

-- All non-core public tables are implementation state and move to private.
insert into issue_422_relation_targets (object_name, target_schema)
select class.relname, 'private'::name
from pg_class class
join pg_namespace namespace on namespace.oid = class.relnamespace
where namespace.nspname = 'public'
  and class.relkind in ('r', 'p')
  and class.relname <> all (array[
    'processes', 'flows', 'contacts', 'sources', 'unitgroups',
    'flowproperties', 'lciamethods', 'lifecyclemodels', 'ilcd'
  ]::name[]);

-- Existing projections keep their previously reviewed boundary.
insert into issue_422_relation_targets (object_name, target_schema) values
  ('worker_domain_traceability_cutoffs', 'private'),
  ('worker_domain_traceability_violations', 'util'),
  ('worker_job_domain_refs', 'api'),
  ('worker_legacy_lifecycle_audit', 'util'),
  ('worker_legacy_table_retirement_blockers', 'util');

create temporary table issue_422_private_routine_names (
  routine_name name primary key
) on commit drop;

-- Explicitly reviewed internal routines. Every other current public routine is
-- an API facade and moves to api. Classification is by routine name because no
-- overload in the current catalog has conflicting boundary semantics.
insert into issue_422_private_routine_names (routine_name)
select unnest(array[
    'cmd_dataset_alias_batch_guarded',
    'cmd_dataset_alias_execution_execute',
    'cmd_dataset_alias_plan_guarded',
    'cmd_dataset_publish_issue304_legacy',
    'cmd_dataset_review_submit_gate_enqueue_worker_job',
    'cmd_dataset_review_submit_gate_link_worker_job',
    'cmd_dataset_review_submit_gate_payload',
    'cmd_dataset_review_submit_gate_record_result',
    'cmd_dataset_review_submit_job_claim',
    'cmd_dataset_review_submit_job_payload',
    'cmd_dataset_review_submit_job_record_result',
    'cmd_dataset_semantic_backfill',
    'cmd_lca_release_artifacts_finalize_service',
    'cmd_lcia_result_build_request_legacy',
    'cmd_lcia_result_build_request_v2_envelope',
    'cmd_lcia_result_build_request_v2_without_expiry',
    'cmd_lcia_result_package_mark_ready',
    'cmd_lcia_result_package_mark_ready_without_closure_recheck',
    'cmd_lcia_scope_closure_check_request',
    'cmd_lcia_scope_closure_check_request_v2_untracked',
    'cmd_review_approve_issue304_legacy',
    'cmd_review_assert_lifecycle_closure',
    'cmd_review_assign_reviewers_v1_legacy',
    'cmd_review_lifecycle_error',
    'cmd_review_migrate_legacy_v2',
    'cmd_review_reference_roles',
    'cmd_review_submit_comment_pre_v2',
    'cmd_review_submit_from_job',
    'cmd_review_submit_without_gate_issue304_legacy',
    'cmd_review_submit_without_gate_pre_v2',
    'contacts_sync_jsonb_version',
    'dataset_review_submit_requests_assign_submit_worker_job',
    'dataset_review_submit_requests_sync_submit_worker_job',
    'delete_lifecycle_model_bundle',
    'flowproperties_sync_jsonb_version',
    'flows_derivative_rebuild_embedding_input',
    'flows_sync_jsonb_version',
    'get_task_summary_v2_feed_unversioned',
    'lca_enqueue_job',
    'lca_legacy_job_type',
    'lca_package_enqueue_job',
    'lca_read_job_projection',
    'lca_read_latest_single_solve_result',
    'lca_read_result_projection',
    'lca_release_error',
    'lca_release_guard_approval_update',
    'lca_release_guard_artifact_update',
    'lca_release_guard_dataset_update',
    'lca_release_guard_publication_update',
    'lca_release_guard_run_update',
    'lca_release_is_manager',
    'lca_release_is_service_request',
    'lcia_result_package_bind_closure_certificate',
    'lcia_result_package_touch_task_projection',
    'lcia_result_prevent_ready_package_content_update',
    'lcia_scope_closure_artifact_lifecycle_guard',
    'lcia_scope_closure_artifact_lineage_eligible',
    'lcia_scope_closure_artifact_role',
    'lcia_scope_closure_artifact_write_set_json',
    'lcia_scope_closure_build_admission_guard',
    'lcia_scope_closure_certificate_event_immutable',
    'lcia_scope_closure_certificate_validity_guard',
    'lcia_scope_closure_current_release_matches',
    'lcia_scope_closure_evidence_usable',
    'lcia_scope_closure_guard_snapshot_artifact_delete',
    'lcia_scope_closure_guard_snapshot_delete',
    'lcia_scope_closure_normalize_request',
    'lcia_scope_closure_preallocate_numerical_snapshot',
    'lcia_scope_closure_sha256',
    'lcia_scope_closure_sha256_text',
    'lcia_scope_closure_snapshot_refs_immutable',
    'lciamethods_sync_jsonb_version',
    'lifecyclemodels_sync_jsonb_version',
    'processes_derivative_rebuild_embedding_input',
    'processes_sync_jsonb_version',
    'review_append_scope_snapshot_v1',
    'review_revision_fingerprint_v1',
    'review_scope_all_reference_ids_v1',
    'review_scope_checksum_v1',
    'review_scope_current_items_v1',
    'review_scope_current_reference_ids_v1',
    'review_scope_current_snapshot_v1',
    'review_validate_scope_history_v1',
    'save_lifecycle_model_bundle',
    'sources_sync_jsonb_version',
    'svc_lcia_document_validation_evidence_lookup',
    'svc_lcia_document_validation_evidence_record',
    'svc_lcia_result_build_bind_closure',
    'svc_lcia_scope_closure_artifact_gc_claim',
    'svc_lcia_scope_closure_artifact_gc_complete',
    'svc_lcia_scope_closure_artifact_gc_fail',
    'svc_lcia_scope_closure_artifact_gc_preview',
    'svc_lcia_scope_closure_artifact_gc_renew',
    'svc_lcia_scope_closure_artifact_write_set_create',
    'svc_lcia_scope_closure_artifact_write_set_create_v2',
    'svc_lcia_scope_closure_artifact_write_set_fail',
    'svc_lcia_scope_closure_artifact_write_set_fail_v2',
    'svc_lcia_scope_closure_artifact_write_set_finalize',
    'svc_lcia_scope_closure_artifact_write_set_finalize_v2',
    'svc_lcia_scope_closure_artifact_write_set_inspect',
    'svc_lcia_scope_closure_artifact_write_set_reconcile',
    'svc_lcia_scope_closure_artifact_write_set_reconcile_complete',
    'svc_lcia_scope_closure_artifact_write_set_register_batch_v2',
    'svc_lcia_scope_closure_artifact_write_set_seal_v2',
    'svc_lcia_scope_closure_artifact_write_set_status_v2',
    'svc_lcia_scope_closure_build_binding',
    'svc_lcia_scope_closure_build_binding_without_expiry',
    'svc_lcia_scope_closure_certificate_event',
    'svc_lcia_scope_closure_check_get_worker_input',
    'svc_lcia_scope_closure_check_record_result',
    'svc_lcia_scope_closure_check_record_result_v2',
    'svc_lcia_scope_closure_check_record_result_v2_legacy',
    'svc_lcia_scope_closure_check_record_result_v2_untracked',
    'svc_lcia_scope_closure_check_record_result_v3',
    'svc_lcia_scope_closure_claim_scan_execution',
    'svc_lcia_scope_closure_fail_before_scan',
    'svc_lcia_scope_closure_finalize_reused_scan',
    'svc_lcia_scope_closure_reuse_completed_scan',
    'sync_auth_users_to_public_users',
    'sync_json_to_jsonb',
    'unitgroups_sync_jsonb_version',
    'update_modified_at',
    'worker_cancel_job',
    'worker_claim_jobs',
    'worker_enqueue_job',
    'worker_heartbeat_job',
    'worker_job_payload',
    'worker_list_jobs',
    'worker_read_job',
    'worker_read_latest_job',
    'worker_record_job_result',
    'worker_retry_job'
]::name[]);

create temporary table issue_422_routine_targets (
  routine_oid oid primary key,
  source_schema name not null,
  routine_name name not null,
  target_schema name not null check (target_schema in ('api', 'private'))
) on commit drop;

insert into issue_422_routine_targets (
  routine_oid, source_schema, routine_name, target_schema
)
select
  routine.oid,
  namespace.nspname,
  routine.proname,
  case
    when private_name.routine_name is not null then 'private'::name
    else 'api'::name
  end
from pg_proc routine
join pg_namespace namespace on namespace.oid = routine.pronamespace
left join issue_422_private_routine_names private_name
  on private_name.routine_name = routine.proname
where namespace.nspname = 'public'
  and routine.prokind = 'f';

do $preflight$
declare
  actual_public_tables text[];
  expected_public_tables constant text[] := array[
    'contacts', 'flowproperties', 'flows', 'ilcd', 'lciamethods',
    'lifecyclemodels', 'processes', 'sources', 'unitgroups'
  ];
begin
  if session_user <> 'postgres' or current_user <> 'postgres' then
    raise exception using
      errcode = '42501',
      message = 'Issue #422 migration requires the postgres owner session';
  end if;

  select array_agg(class.relname::text order by class.relname)
  into actual_public_tables
  from pg_class class
  join pg_namespace namespace on namespace.oid = class.relnamespace
  where namespace.nspname = 'public'
    and class.relkind in ('r', 'p')
    and not exists (
      select 1
      from issue_422_relation_targets target
      where target.object_name = class.relname
    );

  if actual_public_tables is distinct from expected_public_tables then
    raise exception
      'public core table contract drifted: expected %, found %',
      expected_public_tables, actual_public_tables;
  end if;

  if (select count(*) from issue_422_relation_targets) <> 52 then
    raise exception
      'relation manifest drifted: expected 52 movable tables/views, found %',
      (select count(*) from issue_422_relation_targets);
  end if;

  if (select count(*) from issue_422_routine_targets) <> 333 then
    raise exception
      'routine manifest drifted: expected 333 public functions, found %',
      (select count(*) from issue_422_routine_targets);
  end if;

  if exists (
    select 1
    from issue_422_private_routine_names private_name
    where not exists (
      select 1
      from issue_422_routine_targets target
      where target.routine_name = private_name.routine_name
    )
  ) then
    raise exception 'one or more reviewed private routine names are absent';
  end if;

  if to_regtype('public.filtered_row') is null then
    raise exception 'required standalone public.filtered_row type is absent';
  end if;

  if to_regclass('public.lcia_scope_closure_publication_epoch_seq') is null then
    raise exception
      'required standalone public.lcia_scope_closure_publication_epoch_seq is absent';
  end if;
end
$preflight$;

-- Move table-backed state first. Indexes, constraints, owned sequences, row
-- types, RLS policies, triggers, and foreign-key dependencies follow by OID.
do $move_relations$
declare
  target record;
  relation_kind "char";
begin
  for target in
    select object_name, target_schema
    from issue_422_relation_targets
    order by case target_schema
      when 'private' then 1
      when 'util' then 2
      when 'api' then 3
      else 4
    end, object_name
  loop
    select class.relkind
    into relation_kind
    from pg_class class
    join pg_namespace namespace on namespace.oid = class.relnamespace
    where namespace.nspname = 'public'
      and class.relname = target.object_name;

    if relation_kind in ('r', 'p') then
      execute format(
        'alter table public.%I set schema %I',
        target.object_name, target.target_schema
      );
    elsif relation_kind = 'v' then
      execute format(
        'alter view public.%I set schema %I',
        target.object_name, target.target_schema
      );
    else
      raise exception
        'unexpected or missing relation public.% (kind=%)',
        target.object_name, relation_kind;
    end if;
  end loop;
end
$move_relations$;

-- These are not table-owned objects and therefore require explicit moves.
alter sequence public.lcia_scope_closure_publication_epoch_seq set schema private;
alter type public.filtered_row set schema api;

-- Move routines without dropping them so trigger, policy, view, default, and
-- inter-routine dependencies retain their OIDs.
do $move_routines$
declare
  target record;
  routine_identity text;
begin
  for target in
    select routine_oid, target_schema
    from issue_422_routine_targets
    order by routine_oid
  loop
    select routine.oid::regprocedure::text
    into routine_identity
    from pg_proc routine
    where routine.oid = target.routine_oid;

    execute format(
      'alter function %s set schema %I',
      routine_identity, target.target_schema
    );
  end loop;
end
$move_routines$;

create temporary table issue_422_symbol_targets (
  object_name name primary key,
  target_schema name not null
) on commit drop;

insert into issue_422_symbol_targets (object_name, target_schema)
select object_name, target_schema
from issue_422_relation_targets;

insert into issue_422_symbol_targets (object_name, target_schema) values
  ('lcia_scope_closure_publication_epoch_seq', 'private'),
  ('filtered_row', 'api');

insert into issue_422_symbol_targets (object_name, target_schema)
select routine_name, target_schema
from issue_422_routine_targets
group by routine_name, target_schema
on conflict (object_name) do update
set target_schema = excluded.target_schema;

-- Stored PL/pgSQL and SQL bodies contain source text. Recreate each application
-- function in place after replacing explicit public references; the function
-- OID, owner, ACL, volatility, security mode, and dependency callers remain.
do $rewrite_routines$
declare
  routine record;
  symbol record;
  definition text;
  preferred_path text;
begin
  for routine in
    select proc.oid, namespace.nspname as routine_schema, proc.proconfig
    from pg_proc proc
    join pg_namespace namespace on namespace.oid = proc.pronamespace
    where namespace.nspname in ('api', 'private', 'util')
      and proc.prokind = 'f'
    order by proc.oid
  loop
    definition := pg_get_functiondef(routine.oid);

    for symbol in
      select object_name, target_schema
      from issue_422_symbol_targets
      order by length(object_name::text) desc, object_name
    loop
      definition := replace(
        definition,
        format('public.%I', symbol.object_name),
        format('%I.%I', symbol.target_schema, symbol.object_name)
      );
      definition := replace(
        definition,
        format('"public"."%s"', replace(symbol.object_name::text, '"', '""')),
        format('"%s"."%s"',
          replace(symbol.target_schema::text, '"', '""'),
          replace(symbol.object_name::text, '"', '""')
        )
      );
    end loop;

    -- hstore is installed in extensions on Supabase. One legacy trigger helper
    -- still carried the pre-extension-move qualification.
    definition := replace(definition, 'public.hstore', 'extensions.hstore');

    preferred_path := case routine.routine_schema
      when 'api' then '''api'', ''private'', ''public'', ''util'', ''extensions'''
      when 'private' then '''private'', ''api'', ''public'', ''util'', ''extensions'''
      else '''util'', ''private'', ''api'', ''public'', ''extensions'''
    end;

    -- Preserve intentionally empty search paths. Existing public-first paths
    -- gain their new canonical namespace order and retain extension/pg_temp.
    definition := replace(
      definition,
      'SET search_path TO ''public''',
      'SET search_path TO ' || preferred_path
    );

    execute definition;

    -- Previously caller-dependent routines now receive a deterministic path.
    if routine.proconfig is null then
      execute format(
        'alter function %s set search_path to %I, api, private, public, util, extensions, pg_temp',
        routine.oid::regprocedure,
        routine.routine_schema
      );
    end if;
  end loop;
end
$rewrite_routines$;

-- A small set of security-invoker search facades calls private RLS helpers.
-- Give only those facades a non-login, non-BYPASSRLS owner that inherits the
-- authenticated policies. This keeps caller JWT/RLS semantics without giving
-- browser roles direct access to private.
do $executor_role$
begin
  if not exists (
    select 1 from pg_roles where rolname = 'api_internal_executor'
  ) then
    create role api_internal_executor nologin inherit nobypassrls;
  end if;
end
$executor_role$;

alter role api_internal_executor nologin inherit nobypassrls;
grant authenticated to api_internal_executor;
grant api_internal_executor to postgres;
grant usage on schema api, private, public to api_internal_executor;
grant create on schema api to api_internal_executor;
grant select on all tables in schema public, private to api_internal_executor;
grant execute on all functions in schema api, private to api_internal_executor;

do $secure_private_facades$
declare
  routine record;
  adapted_count integer := 0;
begin
  for routine in
    select proc.oid
    from pg_proc proc
    join pg_namespace namespace on namespace.oid = proc.pronamespace
    where namespace.nspname = 'api'
      and proc.prokind = 'f'
      and not proc.prosecdef
      and pg_get_functiondef(proc.oid) ~ 'private[.]'
    order by proc.oid
  loop
    execute format('alter function %s security definer', routine.oid::regprocedure);
    execute format('alter function %s owner to api_internal_executor', routine.oid::regprocedure);
    adapted_count := adapted_count + 1;
  end loop;

  if adapted_count <> 16 then
    raise exception
      'private facade executor manifest drifted: expected 16, adapted %',
      adapted_count;
  end if;
end
$secure_private_facades$;

revoke create on schema api from api_internal_executor;
revoke api_internal_executor from postgres;

-- Data API roles only use the exposed api/public namespaces. Existing API
-- object ACLs are preserved by OID; internal namespaces are not directly
-- reachable by browser roles.
revoke all on schema api from public;
grant usage on schema api to anon, authenticated, service_role;
revoke all on schema private, util, archive from public, anon, authenticated;
revoke execute on all functions in schema private, util, archive
  from public, anon, authenticated;
revoke all on all tables in schema private, util, archive
  from public, anon, authenticated;
revoke all on all sequences in schema private, util, archive
  from public, anon, authenticated;

-- Public core-table RLS policies retain OID dependencies on the membership
-- and review tables. Give authenticated only the minimum read capability
-- required to evaluate those policies. private remains absent from the
-- PostgREST exposed-schema list, and no private write or routine privilege is
-- restored.
grant usage on schema private to authenticated;
grant select on table private.roles, private.reviews to authenticated;

revoke create on schema public from public, anon, authenticated, service_role;
revoke create on schema api, private, util, archive
  from public, anon, authenticated, service_role;

-- PostgreSQL 17's ALL includes MAINTAIN; API roles never need it.
revoke maintain on all tables in schema public, api, private, util, archive
  from anon, authenticated;

-- Future postgres-owned objects are opt-in. Hosted supabase_admin-owned
-- defaults are verified separately because postgres cannot alter another
-- role's default privileges.
alter default privileges for role postgres in schema public, api, private, util, archive
  revoke all on tables from public, anon, authenticated, service_role;
alter default privileges for role postgres in schema public, api, private, util, archive
  revoke all on sequences from public, anon, authenticated, service_role;
alter default privileges for role postgres in schema public, api, private, util, archive
  revoke execute on functions from public, anon, authenticated, service_role;

do $readback$
declare
  actual_public_tables text[];
begin
  select array_agg(class.relname::text order by class.relname)
  into actual_public_tables
  from pg_class class
  join pg_namespace namespace on namespace.oid = class.relnamespace
  where namespace.nspname = 'public'
    and class.relkind in ('r', 'p');

  if actual_public_tables is distinct from array[
    'contacts', 'flowproperties', 'flows', 'ilcd', 'lciamethods',
    'lifecyclemodels', 'processes', 'sources', 'unitgroups'
  ] then
    raise exception 'public table readback failed: %', actual_public_tables;
  end if;

  if exists (
    select 1
    from pg_class class
    join pg_namespace namespace on namespace.oid = class.relnamespace
    where namespace.nspname = 'public'
      and class.relkind in ('v', 'm', 'S', 'c')
  ) then
    raise exception 'public still contains a view, materialized view, sequence, or composite relation';
  end if;

  if exists (
    select 1
    from pg_proc proc
    join pg_namespace namespace on namespace.oid = proc.pronamespace
    where namespace.nspname = 'public'
  ) then
    raise exception 'public still contains application routines';
  end if;

  if (select count(*) from pg_proc proc
      join pg_namespace namespace on namespace.oid = proc.pronamespace
      where namespace.nspname = 'api') <> 201 then
    raise exception 'api routine count readback failed';
  end if;

  if has_schema_privilege('anon', 'private', 'USAGE')
     or has_schema_privilege('anon', 'util', 'USAGE')
     or has_schema_privilege('authenticated', 'util', 'USAGE')
     or has_schema_privilege('anon', 'archive', 'USAGE')
     or has_schema_privilege('authenticated', 'archive', 'USAGE') then
    raise exception 'browser role retains internal schema usage';
  end if;

  if not has_schema_privilege('authenticated', 'private', 'USAGE')
     or not has_table_privilege('authenticated', 'private.roles', 'SELECT')
     or not has_table_privilege('authenticated', 'private.reviews', 'SELECT')
     or exists (
       select 1
       from pg_class class
       join pg_namespace namespace on namespace.oid = class.relnamespace
       where namespace.nspname = 'private'
         and class.relkind in ('r', 'p', 'v', 'm', 'S')
         and class.relname not in ('roles', 'reviews')
         and (
           has_table_privilege('authenticated', class.oid, 'SELECT')
           or has_table_privilege('authenticated', class.oid, 'INSERT')
           or has_table_privilege('authenticated', class.oid, 'UPDATE')
           or has_table_privilege('authenticated', class.oid, 'DELETE')
         )
     ) then
    raise exception 'authenticated private RLS dependency grant drifted';
  end if;

  if (
    select count(*)
    from pg_proc proc
    join pg_namespace namespace on namespace.oid = proc.pronamespace
    join pg_roles owner_role on owner_role.oid = proc.proowner
    where namespace.nspname = 'api'
      and proc.prosecdef
      and owner_role.rolname = 'api_internal_executor'
  ) <> 16 then
    raise exception 'private facade executor readback failed';
  end if;

  if exists (
    select 1
    from pg_proc proc
    join pg_namespace namespace on namespace.oid = proc.pronamespace
    where namespace.nspname in ('api', 'private', 'util')
      and proc.prokind = 'f'
      and pg_get_functiondef(proc.oid) ~
        'public[.](command_audit_log|comments|dataset_review_submit_|identity_center_|lca_|lcia_|notifications|reviews|roles|teams|users|worker_)'
  ) then
    raise exception 'stored routine definition still contains a moved public reference';
  end if;
end
$readback$;

notify pgrst, 'reload config';
notify pgrst, 'reload schema';

commit;
