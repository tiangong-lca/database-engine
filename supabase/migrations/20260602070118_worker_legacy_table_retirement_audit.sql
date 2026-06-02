create or replace view public.worker_legacy_table_retirement_blockers
with (security_invoker = true)
as
with legacy_targets as (
  select
    target_namespace.nspname as legacy_schema,
    target_class.relname as legacy_table,
    target_class.oid as table_oid,
    target_class.reltype as row_type_oid
  from (
    values
      ('public'::name, 'lca_jobs'::name),
      ('public'::name, 'lca_package_jobs'::name),
      ('public'::name, 'dataset_review_submit_jobs'::name)
  ) as targets(schema_name, table_name)
  join pg_namespace as target_namespace
    on target_namespace.nspname = targets.schema_name
  join pg_class as target_class
    on target_class.relnamespace = target_namespace.oid
   and target_class.relname = targets.table_name
   and target_class.relkind in ('r', 'p')
),
foreign_key_blockers as (
  select
    concat(legacy_schema, '.', legacy_table) as legacy_table,
    'foreign_key'::text as blocker_type,
    dependent_namespace.nspname::text as blocker_schema,
    dependent_class.relname::text as blocker_name,
    constraint_record.conname::text as blocker_identity,
    true as is_drop_restrict_blocker,
    jsonb_build_object(
      'constraintName', constraint_record.conname,
      'dependentTable', concat(dependent_namespace.nspname, '.', dependent_class.relname),
      'dependentColumns',
        (
          select jsonb_agg(dependent_attribute.attname order by dependent_attribute.attnum)
          from unnest(constraint_record.conkey) as constraint_column(attnum)
          join pg_attribute as dependent_attribute
            on dependent_attribute.attrelid = constraint_record.conrelid
           and dependent_attribute.attnum = constraint_column.attnum
        ),
      'referencedColumns',
        (
          select jsonb_agg(referenced_attribute.attname order by referenced_attribute.attnum)
          from unnest(constraint_record.confkey) as referenced_column(attnum)
          join pg_attribute as referenced_attribute
            on referenced_attribute.attrelid = constraint_record.confrelid
           and referenced_attribute.attnum = referenced_column.attnum
        ),
      'onDelete', constraint_record.confdeltype
    ) as details
  from legacy_targets
  join pg_constraint as constraint_record
    on constraint_record.confrelid = legacy_targets.table_oid
   and constraint_record.contype = 'f'
  join pg_class as dependent_class
    on dependent_class.oid = constraint_record.conrelid
  join pg_namespace as dependent_namespace
    on dependent_namespace.oid = dependent_class.relnamespace
  where constraint_record.conrelid <> legacy_targets.table_oid
),
view_blockers as (
  select distinct
    concat(legacy_schema, '.', legacy_table) as legacy_table,
    case dependent_class.relkind
      when 'm' then 'dependent_materialized_view'
      else 'dependent_view'
    end as blocker_type,
    dependent_namespace.nspname::text as blocker_schema,
    dependent_class.relname::text as blocker_name,
    concat(dependent_namespace.nspname, '.', dependent_class.relname)::text as blocker_identity,
    true as is_drop_restrict_blocker,
    jsonb_build_object(
      'dependentView', concat(dependent_namespace.nspname, '.', dependent_class.relname),
      'relkind', dependent_class.relkind
    ) as details
  from legacy_targets
  join pg_depend as dependency
    on dependency.refobjid = legacy_targets.table_oid
  join pg_rewrite as rewrite_rule
    on rewrite_rule.oid = dependency.objid
  join pg_class as dependent_class
    on dependent_class.oid = rewrite_rule.ev_class
  join pg_namespace as dependent_namespace
    on dependent_namespace.oid = dependent_class.relnamespace
  where dependent_class.oid <> legacy_targets.table_oid
    and dependent_class.relkind in ('v', 'm')
),
function_signature_blockers as (
  select distinct
    concat(legacy_schema, '.', legacy_table) as legacy_table,
    'function_signature'::text as blocker_type,
    function_namespace.nspname::text as blocker_schema,
    function_record.proname::text as blocker_name,
    concat(
      function_namespace.nspname,
      '.',
      function_record.proname,
      '(',
      pg_get_function_identity_arguments(function_record.oid),
      ')'
    )::text as blocker_identity,
    true as is_drop_restrict_blocker,
    jsonb_build_object(
      'arguments', pg_get_function_arguments(function_record.oid),
      'result', pg_get_function_result(function_record.oid)
    ) as details
  from legacy_targets
  join (
    select *
    from pg_proc
    where prokind in ('f', 'p', 'w')
  ) as function_record
    on lower(pg_get_function_arguments(function_record.oid)) like '%' || lower(legacy_table) || '%'
    or lower(pg_get_function_result(function_record.oid)) like '%' || lower(legacy_table) || '%'
  join pg_namespace as function_namespace
    on function_namespace.oid = function_record.pronamespace
),
function_source_references as (
  select distinct
    concat(legacy_schema, '.', legacy_table) as legacy_table,
    'function_source_reference'::text as blocker_type,
    function_namespace.nspname::text as blocker_schema,
    function_record.proname::text as blocker_name,
    concat(
      function_namespace.nspname,
      '.',
      function_record.proname,
      '(',
      pg_get_function_identity_arguments(function_record.oid),
      ')'
    )::text as blocker_identity,
    false as is_drop_restrict_blocker,
    jsonb_build_object(
      'reason', 'Function source text references the legacy table name; this may not block DROP TABLE RESTRICT, but it is a runtime migration blocker.',
      'arguments', pg_get_function_arguments(function_record.oid),
      'result', pg_get_function_result(function_record.oid)
    ) as details
  from legacy_targets
  join (
    select *
    from pg_proc
    where prokind in ('f', 'p', 'w')
  ) as function_record
    on lower(pg_get_functiondef(function_record.oid)) like '%' || lower(legacy_table) || '%'
  join pg_namespace as function_namespace
    on function_namespace.oid = function_record.pronamespace
  where function_namespace.nspname not in ('pg_catalog', 'information_schema')
)
select *
from foreign_key_blockers
union all
select *
from view_blockers
union all
select *
from function_signature_blockers
union all
select *
from function_source_references;

revoke all on public.worker_legacy_table_retirement_blockers from public, anon, authenticated;
grant select on public.worker_legacy_table_retirement_blockers to service_role;

comment on view public.worker_legacy_table_retirement_blockers
  is 'Service-role audit view for DROP TABLE RESTRICT blockers and runtime references that must be resolved before retiring legacy worker job tables.';
