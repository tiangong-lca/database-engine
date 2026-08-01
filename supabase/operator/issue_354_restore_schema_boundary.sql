\set ON_ERROR_STOP on

\if :{?source_service_role_maintain}
\else
  \echo 'ERROR: set source_service_role_maintain=true|false from retained pre-deployment ACL evidence'
  \set source_service_role_maintain __missing__
\endif

-- Issue #354 emergency compatibility rollback.  Freeze new consumers and
-- retain redacted telemetry evidence before running this owner-only script.
begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

create temporary table issue_354_rollback_evidence (
  source_service_role_maintain text not null
) on commit drop;

insert into issue_354_rollback_evidence values (:'source_service_role_maintain');

do $preflight$
begin
  if session_user <> 'postgres' or current_user <> 'postgres' then
    raise exception using errcode = '42501', message = 'rollback requires the postgres owner session';
  end if;
  if (select source_service_role_maintain from issue_354_rollback_evidence)
     not in ('true', 'false') then
    raise exception 'source_service_role_maintain must be exactly true or false';
  end if;
  if to_regclass('private.worker_domain_traceability_cutoffs') is null
     or to_regclass('util.worker_domain_traceability_violations') is null
     or to_regclass('api.worker_job_domain_refs') is null
     or to_regclass('util.worker_legacy_lifecycle_audit') is null
     or to_regclass('util.worker_legacy_table_retirement_blockers') is null then
    raise exception 'Issue #354 canonical target views are incomplete';
  end if;
end
$preflight$;

drop view util.schema_boundary_phase restrict;

drop view public.worker_domain_traceability_cutoffs restrict;
drop view public.worker_domain_traceability_violations restrict;
drop view public.worker_job_domain_refs restrict;
drop view public.worker_legacy_lifecycle_audit restrict;
drop view public.worker_legacy_table_retirement_blockers restrict;

-- Reverse the canonical moves without replacing the views or their OIDs.
alter view util.worker_legacy_table_retirement_blockers set schema public;
alter view util.worker_legacy_lifecycle_audit set schema public;
alter view api.worker_job_domain_refs set schema public;
alter view util.worker_domain_traceability_violations set schema public;
alter view private.worker_domain_traceability_cutoffs set schema public;

revoke all on table
  public.worker_domain_traceability_cutoffs,
  public.worker_domain_traceability_violations,
  public.worker_job_domain_refs,
  public.worker_legacy_lifecycle_audit,
  public.worker_legacy_table_retirement_blockers
from public, anon, authenticated, api_internal_executor, service_role;

grant select, insert, update, delete, truncate, references, trigger on table
  public.worker_domain_traceability_cutoffs,
  public.worker_domain_traceability_violations,
  public.worker_job_domain_refs,
  public.worker_legacy_lifecycle_audit,
  public.worker_legacy_table_retirement_blockers
to service_role;

grant select on table
  public.worker_domain_traceability_cutoffs,
  public.worker_domain_traceability_violations,
  public.worker_job_domain_refs,
  public.worker_legacy_lifecycle_audit,
  public.worker_legacy_table_retirement_blockers
to api_internal_executor;

do $restore_optional_acl$
begin
  if (select source_service_role_maintain = 'true' from issue_354_rollback_evidence) then
    grant maintain on table
      public.worker_domain_traceability_cutoffs,
      public.worker_domain_traceability_violations,
      public.worker_job_domain_refs,
      public.worker_legacy_lifecycle_audit,
      public.worker_legacy_table_retirement_blockers
    to service_role;
  end if;
end
$restore_optional_acl$;

comment on view public.worker_domain_traceability_cutoffs is
  'Service-role audit contract listing when new worker-produced domain rows must carry a worker_jobs reference. Historical pre-cutover rows are allowed to remain nullable.';
comment on view public.worker_domain_traceability_violations is
  'Service-role audit view for post-cutover worker-produced domain rows that are not traceable to worker_jobs. Historical pre-cutover nullable rows are intentionally ignored.';
comment on view public.worker_job_domain_refs is
  'Service-role projection from canonical worker_jobs to retained non-legacy domain artifact/cache/history/coordinator rows. Legacy job tables are intentionally excluded so they can be retired with DROP RESTRICT after runtime cutover.';
comment on view public.worker_legacy_lifecycle_audit is
  'Service-role lifecycle audit for canonical worker_jobs and retained gate reports. Legacy job tables are intentionally excluded so they can be retired with DROP RESTRICT after runtime cutover.';
comment on view public.worker_legacy_table_retirement_blockers is
  'Service-role audit view for DROP TABLE RESTRICT blockers. It returns no target rows after public.lca_jobs, public.lca_package_jobs, and public.dataset_review_submit_jobs are physically retired.';

notify pgrst, 'reload schema';

commit;
