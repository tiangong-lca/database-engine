begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

create temporary table issue_354_view_contract (
  source_name text primary key,
  target_schema name not null,
  expected_column_names text not null,
  expected_column_types text not null,
  projection text not null,
  compatibility_comment text not null,
  source_oid oid,
  source_comment text,
  source_column_signature text
) on commit drop;

insert into issue_354_view_contract (
  source_name, target_schema, expected_column_names, expected_column_types,
  projection, compatibility_comment
) values
  (
    'worker_domain_traceability_cutoffs', 'private',
    'domain_source,required_worker_column,cutover_at,traceability_required,contract_note',
    'text,text,timestamp with time zone,boolean,text',
    'domain_source, required_worker_column, cutover_at, traceability_required, contract_note',
    'Compatibility view for private.worker_domain_traceability_cutoffs; phase=expand; owner=database-engine#354; removal gate=consumer-zero+burn-in+Contract approval.'
  ),
  (
    'worker_domain_traceability_violations', 'util',
    'domain_source,domain_id,domain_role,created_at,updated_at,cutover_at,violation_code,details',
    'text,uuid,text,timestamp with time zone,timestamp with time zone,timestamp with time zone,text,jsonb',
    'domain_source, domain_id, domain_role, created_at, updated_at, cutover_at, violation_code, details',
    'Compatibility view for util.worker_domain_traceability_violations; phase=expand; owner=database-engine#354; removal gate=consumer-zero+burn-in+Contract approval.'
  ),
  (
    'worker_job_domain_refs', 'api',
    'worker_job_id,domain_source,domain_id,domain_role,legacy_job_id,status,created_at,updated_at',
    'uuid,text,uuid,text,uuid,text,timestamp with time zone,timestamp with time zone',
    'worker_job_id, domain_source, domain_id, domain_role, legacy_job_id, status, created_at, updated_at',
    'Compatibility view for api.worker_job_domain_refs; phase=expand; owner=database-engine#354; removal gate=consumer-zero+burn-in+Contract approval.'
  ),
  (
    'worker_legacy_lifecycle_audit', 'util',
    'legacy_source,task_family,legacy_status,row_count,active_count,oldest_created_at,newest_created_at,latest_updated_at',
    'text,text,text,bigint,bigint,timestamp with time zone,timestamp with time zone,timestamp with time zone',
    'legacy_source, task_family, legacy_status, row_count, active_count, oldest_created_at, newest_created_at, latest_updated_at',
    'Compatibility view for util.worker_legacy_lifecycle_audit; phase=expand; owner=database-engine#354; removal gate=consumer-zero+burn-in+Contract approval.'
  ),
  (
    'worker_legacy_table_retirement_blockers', 'util',
    'legacy_table,blocker_type,blocker_schema,blocker_name,blocker_identity,is_drop_restrict_blocker,details',
    'text,text,text,text,text,boolean,jsonb',
    'legacy_table, blocker_type, blocker_schema, blocker_name, blocker_identity, is_drop_restrict_blocker, details',
    'Compatibility view for util.worker_legacy_table_retirement_blockers; phase=expand; owner=database-engine#354; removal gate=consumer-zero+burn-in+Contract approval.'
  );

do $preflight$
declare
  contract_row record;
  actual_names text;
  actual_types text;
  actual_signature text;
  source_record record;
  schema_name name;
begin
  if session_user <> 'postgres' or current_user <> 'postgres' then
    raise exception using errcode = '42501', message = 'Issue #354 migration requires the postgres owner session';
  end if;

  foreach schema_name in array array['api'::name, 'private'::name, 'util'::name, 'archive'::name]
  loop
    if not exists (
      select 1
      from pg_namespace namespace
      join pg_roles owner_role on owner_role.oid = namespace.nspowner
      where namespace.nspname = schema_name and owner_role.rolname = 'postgres'
    ) then
      raise exception 'required postgres-owned schema is missing: %', schema_name;
    end if;
    if has_schema_privilege('service_role', schema_name, 'CREATE')
       or has_schema_privilege('anon', schema_name, 'CREATE')
       or has_schema_privilege('authenticated', schema_name, 'CREATE') then
      raise exception 'non-owner CREATE privilege is forbidden on schema %', schema_name;
    end if;
  end loop;

  if not has_schema_privilege('service_role', 'api', 'USAGE')
     or not has_schema_privilege('service_role', 'private', 'USAGE')
     or not has_schema_privilege('service_role', 'util', 'USAGE')
     or not has_schema_privilege('service_role', 'archive', 'USAGE')
     or not has_schema_privilege('anon', 'api', 'USAGE')
     or not has_schema_privilege('authenticated', 'api', 'USAGE')
     or has_schema_privilege('anon', 'private', 'USAGE')
     or has_schema_privilege('authenticated', 'private', 'USAGE')
     or has_schema_privilege('anon', 'util', 'USAGE')
     or has_schema_privilege('authenticated', 'util', 'USAGE')
     or has_schema_privilege('anon', 'archive', 'USAGE')
     or has_schema_privilege('authenticated', 'archive', 'USAGE') then
    raise exception 'Issue #339 schema USAGE posture is not ready';
  end if;

  if to_regclass('util.security_acl_expand_posture') is null
     or not coalesce((select (posture->>'migrationReady')::boolean from util.security_acl_expand_posture), false) then
    raise exception 'Issue #339 migration posture is not ready';
  end if;

  for contract_row in
    select source_name, target_schema, expected_column_names, expected_column_types,
      projection, compatibility_comment
    from issue_354_view_contract order by source_name
  loop
    select class.oid, description.description
    into source_record
    from pg_class class
    join pg_namespace namespace on namespace.oid = class.relnamespace
    join pg_roles owner_role on owner_role.oid = class.relowner
    left join pg_description description
      on description.objoid = class.oid and description.classoid = 'pg_class'::regclass and description.objsubid = 0
    where namespace.nspname = 'public'
      and class.relname = contract_row.source_name
      and class.relkind = 'v'
      and owner_role.rolname = 'postgres'
      and class.reloptions = array['security_invoker=true'];

    if source_record.oid is null then
      raise exception 'source must be a postgres-owned security-invoker view: public.%', contract_row.source_name;
    end if;
    if to_regclass(format('%I.%I', contract_row.target_schema, contract_row.source_name)) is not null then
      raise exception 'target relation already exists: %.%', contract_row.target_schema, contract_row.source_name;
    end if;

    select
      string_agg(attribute.attname, ',' order by attribute.attnum),
      string_agg(format_type(attribute.atttypid, attribute.atttypmod), ',' order by attribute.attnum),
      string_agg(format('%s:%s:%s:%s', attribute.attname,
        format_type(attribute.atttypid, attribute.atttypmod), attribute.atttypmod,
        case when attribute.attcollation = 0 then '-' else attribute.attcollation::regcollation::text end
      ), ',' order by attribute.attnum)
    into actual_names, actual_types, actual_signature
    from pg_attribute attribute
    where attribute.attrelid = source_record.oid
      and attribute.attnum > 0
      and not attribute.attisdropped;

    if actual_names <> contract_row.expected_column_names
       or actual_types <> contract_row.expected_column_types then
      raise exception 'frozen column contract mismatch for public.%: names=%, types=%',
        contract_row.source_name, actual_names, actual_types;
    end if;
    if not has_table_privilege('postgres', source_record.oid,
      'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') then
      raise exception 'source owner privileges drifted for public.%', contract_row.source_name;
    end if;
    if exists (
      with actual as (
        select case when acl.grantee=0 then 'PUBLIC' else grantee_role.rolname end as grantee,
          acl.privilege_type, acl.is_grantable
        from pg_class class
        cross join lateral aclexplode(coalesce(class.relacl, acldefault('r', class.relowner))) acl
        left join pg_roles grantee_role on grantee_role.oid = acl.grantee
        where class.oid = source_record.oid
          and (acl.grantee=0 or grantee_role.rolname <> 'postgres')
      ), expected(grantee, privilege_type, is_grantable) as (values
        ('service_role','SELECT',false), ('service_role','INSERT',false),
        ('service_role','UPDATE',false), ('service_role','DELETE',false),
        ('service_role','TRUNCATE',false), ('service_role','REFERENCES',false),
        ('service_role','TRIGGER',false),
        ('api_internal_executor','SELECT',false)
      ), allowed_optional(grantee, privilege_type, is_grantable) as (values
        ('service_role','MAINTAIN',false)
      )
      (select grantee, privilege_type, is_grantable from actual
       except
       (select grantee, privilege_type, is_grantable from expected
        union all
        select grantee, privilege_type, is_grantable from allowed_optional))
      union all
      (select grantee, privilege_type, is_grantable from expected
       except select grantee, privilege_type, is_grantable from actual)
    ) then
      raise exception 'source normalized ACL drift for public.%', contract_row.source_name;
    end if;
    if source_record.description is null or btrim(source_record.description) = '' then
      raise exception 'source comment is missing for public.%', contract_row.source_name;
    end if;

    update issue_354_view_contract
    set source_oid = source_record.oid,
        source_comment = source_record.description,
        source_column_signature = actual_signature
    where source_name = contract_row.source_name;
  end loop;
end
$preflight$;

-- OID-preserving canonical moves.  The dependent violation view follows its
-- cutoff dependency by OID, so no definition is copied or replaced.
alter view public.worker_domain_traceability_cutoffs set schema private;
alter view public.worker_domain_traceability_violations set schema util;
alter view public.worker_job_domain_refs set schema api;
alter view public.worker_legacy_lifecycle_audit set schema util;
alter view public.worker_legacy_table_retirement_blockers set schema util;

create view public.worker_domain_traceability_cutoffs
with (security_invoker = true)
as select
  domain_source, required_worker_column, cutover_at, traceability_required, contract_note
from private.worker_domain_traceability_cutoffs;

create view public.worker_domain_traceability_violations
with (security_invoker = true)
as select
  domain_source, domain_id, domain_role, created_at, updated_at, cutover_at, violation_code, details
from util.worker_domain_traceability_violations;

create view public.worker_job_domain_refs
with (security_invoker = true)
as select
  worker_job_id, domain_source, domain_id, domain_role, legacy_job_id, status, created_at, updated_at
from api.worker_job_domain_refs;

create view public.worker_legacy_lifecycle_audit
with (security_invoker = true)
as select
  legacy_source, task_family, legacy_status, row_count, active_count,
  oldest_created_at, newest_created_at, latest_updated_at
from util.worker_legacy_lifecycle_audit;

create view public.worker_legacy_table_retirement_blockers
with (security_invoker = true)
as select
  legacy_table, blocker_type, blocker_schema, blocker_name, blocker_identity,
  is_drop_restrict_blocker, details
from util.worker_legacy_table_retirement_blockers;

alter view public.worker_domain_traceability_cutoffs owner to postgres;
alter view public.worker_domain_traceability_violations owner to postgres;
alter view public.worker_job_domain_refs owner to postgres;
alter view public.worker_legacy_lifecycle_audit owner to postgres;
alter view public.worker_legacy_table_retirement_blockers owner to postgres;

-- Both canonical and compatibility views remain service-only in Expand.
revoke all on table
  private.worker_domain_traceability_cutoffs,
  util.worker_domain_traceability_violations,
  api.worker_job_domain_refs,
  util.worker_legacy_lifecycle_audit,
  util.worker_legacy_table_retirement_blockers,
  public.worker_domain_traceability_cutoffs,
  public.worker_domain_traceability_violations,
  public.worker_job_domain_refs,
  public.worker_legacy_lifecycle_audit,
  public.worker_legacy_table_retirement_blockers
from public, anon, authenticated, api_internal_executor, service_role;

grant select on table
  private.worker_domain_traceability_cutoffs,
  util.worker_domain_traceability_violations,
  api.worker_job_domain_refs,
  util.worker_legacy_lifecycle_audit,
  util.worker_legacy_table_retirement_blockers,
  public.worker_domain_traceability_cutoffs,
  public.worker_domain_traceability_violations,
  public.worker_job_domain_refs,
  public.worker_legacy_lifecycle_audit,
  public.worker_legacy_table_retirement_blockers
to service_role;

comment on view public.worker_domain_traceability_cutoffs is
  'Compatibility view for private.worker_domain_traceability_cutoffs; phase=expand; owner=database-engine#354; removal gate=consumer-zero+burn-in+Contract approval.';
comment on view public.worker_domain_traceability_violations is
  'Compatibility view for util.worker_domain_traceability_violations; phase=expand; owner=database-engine#354; removal gate=consumer-zero+burn-in+Contract approval.';
comment on view public.worker_job_domain_refs is
  'Compatibility view for api.worker_job_domain_refs; phase=expand; owner=database-engine#354; removal gate=consumer-zero+burn-in+Contract approval.';
comment on view public.worker_legacy_lifecycle_audit is
  'Compatibility view for util.worker_legacy_lifecycle_audit; phase=expand; owner=database-engine#354; removal gate=consumer-zero+burn-in+Contract approval.';
comment on view public.worker_legacy_table_retirement_blockers is
  'Compatibility view for util.worker_legacy_table_retirement_blockers; phase=expand; owner=database-engine#354; removal gate=consumer-zero+burn-in+Contract approval.';

do $readback$
declare
  contract_row record;
  target_oid oid;
  wrapper_oid oid;
  actual_signature text;
  actual_comment text;
  parity_count bigint;
begin
  for contract_row in
    select source_name, target_schema, projection, compatibility_comment,
      source_oid, source_comment, source_column_signature
    from issue_354_view_contract order by source_name
  loop
    target_oid := to_regclass(format('%I.%I', contract_row.target_schema, contract_row.source_name));
    wrapper_oid := to_regclass(format('public.%I', contract_row.source_name));
    if target_oid is null or wrapper_oid is null
       or target_oid <> contract_row.source_oid or wrapper_oid = contract_row.source_oid then
      raise exception 'OID preservation/readback failed for %', contract_row.source_name;
    end if;

    select string_agg(format('%s:%s:%s:%s', attribute.attname,
      format_type(attribute.atttypid, attribute.atttypmod), attribute.atttypmod,
      case when attribute.attcollation = 0 then '-' else attribute.attcollation::regcollation::text end
    ), ',' order by attribute.attnum)
    into actual_signature
    from pg_attribute attribute
    where attribute.attrelid = target_oid and attribute.attnum > 0 and not attribute.attisdropped;
    if actual_signature <> contract_row.source_column_signature then
      raise exception 'canonical column signature changed for %', contract_row.source_name;
    end if;

    select obj_description(target_oid, 'pg_class') into actual_comment;
    if actual_comment <> contract_row.source_comment then
      raise exception 'canonical comment changed for %', contract_row.source_name;
    end if;

    if not exists (
      select 1 from pg_class class join pg_roles role on role.oid = class.relowner
      where class.oid in (target_oid, wrapper_oid)
        and class.relkind = 'v'
        and class.reloptions = array['security_invoker=true']
        and role.rolname = 'postgres'
      group by role.rolname having count(*) = 2
    ) then
      raise exception 'owner/security_invoker readback failed for %', contract_row.source_name;
    end if;

    if exists (
      with actual as (
        select class.oid::text as object_oid,
          case when acl.grantee=0 then 'PUBLIC' else grantee_role.rolname end as grantee,
          acl.privilege_type, acl.is_grantable
        from pg_class class
        cross join lateral aclexplode(coalesce(class.relacl, acldefault('r', class.relowner))) acl
        left join pg_roles grantee_role on grantee_role.oid = acl.grantee
        where class.oid in (target_oid, wrapper_oid)
          and (acl.grantee=0 or grantee_role.rolname <> 'postgres')
      ), expected(object_oid, grantee, privilege_type, is_grantable) as (values
        (target_oid::text, 'service_role', 'SELECT', false),
        (wrapper_oid::text, 'service_role', 'SELECT', false)
      )
      (select object_oid, grantee, privilege_type, is_grantable from actual
       except select object_oid, grantee, privilege_type, is_grantable from expected)
      union all
      (select object_oid, grantee, privilege_type, is_grantable from expected
       except select object_oid, grantee, privilege_type, is_grantable from actual)
    ) then
      raise exception 'normalized target/compatibility ACL readback failed for %', contract_row.source_name;
    end if;

    if not has_table_privilege('service_role', target_oid, 'SELECT')
       or not has_table_privilege('service_role', wrapper_oid, 'SELECT')
       or has_table_privilege('anon', target_oid, 'SELECT')
       or has_table_privilege('anon', wrapper_oid, 'SELECT')
       or has_table_privilege('authenticated', target_oid, 'SELECT')
       or has_table_privilege('authenticated', wrapper_oid, 'SELECT')
       or has_table_privilege('api_internal_executor', target_oid, 'SELECT')
       or has_table_privilege('api_internal_executor', wrapper_oid, 'SELECT') then
      raise exception 'role ACL readback failed for %', contract_row.source_name;
    end if;

    if obj_description(wrapper_oid, 'pg_class') <> contract_row.compatibility_comment then
      raise exception 'compatibility removal metadata drifted for %', contract_row.source_name;
    end if;
    if not exists (
      select 1
      from pg_rewrite rewrite
      join pg_depend dependency
        on dependency.classid = 'pg_rewrite'::regclass and dependency.objid = rewrite.oid
      where rewrite.ev_class = wrapper_oid and dependency.refobjid = target_oid
    ) then
      raise exception 'compatibility dependency is missing for %', contract_row.source_name;
    end if;

    execute format(
      'select count(*) from ((select %1$s from public.%2$I except all select %1$s from %3$I.%2$I) union all (select %1$s from %3$I.%2$I except all select %1$s from public.%2$I)) parity',
      contract_row.projection, contract_row.source_name, contract_row.target_schema
    ) into parity_count;
    if parity_count <> 0 then
      raise exception 'compatibility rowset parity failed for %: %', contract_row.source_name, parity_count;
    end if;
  end loop;

  if not exists (
    select 1
    from pg_rewrite rewrite
    join pg_depend dependency
      on dependency.classid = 'pg_rewrite'::regclass and dependency.objid = rewrite.oid
    where rewrite.ev_class = 'util.worker_domain_traceability_violations'::regclass
      and dependency.refobjid = 'private.worker_domain_traceability_cutoffs'::regclass
  ) then
    raise exception 'util.worker_domain_traceability_violations lost its private cutoff dependency';
  end if;
end
$readback$;

create or replace view util.schema_boundary_phase
with (security_invoker = true)
as
with core_tables(name) as (
  values ('contacts'), ('flowproperties'), ('flows'), ('ilcd'), ('lciamethods'),
    ('lifecyclemodels'), ('processes'), ('sources'), ('unitgroups')
), missing_core as (
  select name from core_tables where to_regclass(format('public.%I', name)) is null
), moved_views(source_name, target_schema) as (
  values
    ('worker_domain_traceability_cutoffs', 'private'),
    ('worker_domain_traceability_violations', 'util'),
    ('worker_job_domain_refs', 'api'),
    ('worker_legacy_lifecycle_audit', 'util'),
    ('worker_legacy_table_retirement_blockers', 'util')
), missing_views as (
  select source_name, target_schema
  from moved_views
  where to_regclass(format('%I.%I', target_schema, source_name)) is null
     or to_regclass(format('public.%I', source_name)) is null
)
select jsonb_build_object(
  'contractVersion', 'schema-boundary-phase.v1',
  'migrationVersion', '20260801042547',
  'phase', 'expand',
  'expandReady', not exists (select 1 from missing_core) and not exists (select 1 from missing_views),
  'contractReady', false,
  'contractBlocker', 'remaining public application objects, consumer-zero burn-in, Issue #352 hosted owner defaults, and Contract approval are pending',
  'missingCoreTables', (select coalesce(jsonb_agg(name order by name), '[]') from missing_core),
  'missingMovedViews', (select coalesce(jsonb_agg(to_jsonb(x) order by target_schema, source_name), '[]') from missing_views x)
) as posture;

revoke all on util.schema_boundary_phase from public, anon, authenticated, api_internal_executor;
grant select on util.schema_boundary_phase to service_role;

notify pgrst, 'reload schema';

commit;
