-- Issue #574: expose one bounded, administrator-only aggregate snapshot for
-- the National Carbon organization-contribution screen. The response contains
-- process, model, and combined scopes so UI scope changes never fan out into
-- additional database calls.

create or replace function api.qry_national_carbon_organization_contributions(
  p_limit integer default 10
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $function$
declare
  v_actor uuid := auth.uid();
  v_result jsonb;
begin
  if v_actor is null then
    raise exception using
      errcode = '28000',
      message = 'AUTH_REQUIRED';
  end if;

  if not api.cmd_membership_is_system_manager(v_actor) then
    raise exception using
      errcode = '42501',
      message = 'SYSTEM_MANAGER_REQUIRED';
  end if;

  if p_limit is null or p_limit < 1 or p_limit > 50 then
    raise exception using
      errcode = '22023',
      message = 'INVALID_LIMIT';
  end if;

  with
  latest_published_process as materialized (
    select distinct on (process_row.id)
      'process'::text as dataset_kind,
      process_row.id as dataset_id,
      process_row.user_id,
      process_row.modified_at
    from public.processes as process_row
    where process_row.state_code = 100
    order by
      process_row.id,
      process_row.version desc,
      process_row.modified_at desc
  ),
  latest_current_process as materialized (
    select distinct on (process_row.id)
      'process'::text as dataset_kind,
      process_row.id as dataset_id,
      process_row.user_id,
      process_row.state_code,
      process_row.modified_at
    from public.processes as process_row
    order by
      process_row.id,
      process_row.version desc,
      process_row.modified_at desc
  ),
  latest_published_model as materialized (
    select distinct on (model_row.id)
      'model'::text as dataset_kind,
      model_row.id as dataset_id,
      model_row.user_id,
      model_row.modified_at
    from public.lifecyclemodels as model_row
    where model_row.state_code = 100
    order by
      model_row.id,
      model_row.version desc,
      model_row.modified_at desc
  ),
  latest_current_model as materialized (
    select distinct on (model_row.id)
      'model'::text as dataset_kind,
      model_row.id as dataset_id,
      model_row.user_id,
      model_row.state_code,
      model_row.modified_at
    from public.lifecyclemodels as model_row
    order by
      model_row.id,
      model_row.version desc,
      model_row.modified_at desc
  ),
  published_facts as materialized (
    select * from latest_published_process
    union all
    select * from latest_published_model
  ),
  pending_review_facts as materialized (
    select
      latest_row.dataset_kind,
      latest_row.dataset_id,
      latest_row.user_id,
      latest_row.modified_at
    from latest_current_process as latest_row
    where latest_row.state_code = 20
    union all
    select
      latest_row.dataset_kind,
      latest_row.dataset_id,
      latest_row.user_id,
      latest_row.modified_at
    from latest_current_model as latest_row
    where latest_row.state_code = 20
  ),
  relevant_user_ids as (
    select published.user_id
    from published_facts as published
    where published.user_id is not null
    union
    select pending.user_id
    from pending_review_facts as pending
    where pending.user_id is not null
  ),
  user_organization_names as materialized (
    select
      relevant.user_id,
      nullif(
        pg_catalog.regexp_replace(
          pg_catalog.btrim(profile.raw_user_meta_data ->> 'organization'),
          '[[:space:]]+',
          ' ',
          'g'
        ),
        ''
      ) as organization_name
    from relevant_user_ids as relevant
    left join private.users as profile on profile.id = relevant.user_id
      and pg_catalog.jsonb_typeof(profile.raw_user_meta_data -> 'organization') = 'string'
  ),
  user_organizations as materialized (
    select
      named.user_id,
      pg_catalog.lower(named.organization_name) as organization_key,
      named.organization_name
    from user_organization_names as named
  ),
  contribution_facts as (
    select
      published.dataset_kind,
      published.dataset_id,
      published.user_id,
      organization.organization_key,
      organization.organization_name,
      1::integer as published_count,
      0::integer as reviewing_count,
      published.modified_at
    from published_facts as published
    left join user_organizations as organization using (user_id)
    union all
    select
      pending.dataset_kind,
      pending.dataset_id,
      pending.user_id,
      organization.organization_key,
      organization.organization_name,
      0::integer as published_count,
      1::integer as reviewing_count,
      pending.modified_at
    from pending_review_facts as pending
    left join user_organizations as organization using (user_id)
  ),
  scoped_facts as materialized (
    select
      scope.dataset_scope,
      fact.dataset_kind,
      fact.dataset_id,
      fact.user_id,
      fact.organization_key,
      fact.organization_name,
      fact.published_count,
      fact.reviewing_count,
      fact.modified_at
    from contribution_facts as fact
    cross join lateral (
      values (fact.dataset_kind), ('all'::text)
    ) as scope(dataset_scope)
  ),
  scope_catalog(dataset_scope) as (
    values ('process'::text), ('model'::text), ('all'::text)
  ),
  scope_summary_values as materialized (
    select
      scope.dataset_scope,
      coalesce(
        pg_catalog.count(distinct fact.organization_key)
          filter (
            where fact.organization_key is not null
              and fact.published_count = 1
          ),
        0::bigint
      )::bigint as organization_count,
      coalesce(pg_catalog.sum(fact.published_count), 0::bigint)::bigint
        as published_dataset_count,
      coalesce(pg_catalog.sum(fact.reviewing_count), 0::bigint)::bigint
        as pending_review_dataset_count,
      coalesce(
        pg_catalog.sum(fact.published_count)
          filter (
            where fact.modified_at >= pg_catalog.statement_timestamp() - interval '30 days'
          ),
        0::bigint
      )::bigint as published_last_30_days_count
    from scope_catalog as scope
    left join scoped_facts as fact on fact.dataset_scope = scope.dataset_scope
    group by scope.dataset_scope
  ),
  scope_organization_aggregate as materialized (
    select
      fact.dataset_scope,
      fact.organization_key,
      pg_catalog.min(fact.organization_name collate "C") as organization_name,
      pg_catalog.sum(fact.published_count)::bigint as published_dataset_count,
      pg_catalog.sum(fact.reviewing_count)::bigint as reviewing_dataset_count,
      (
        pg_catalog.count(distinct fact.user_id)
          filter (where fact.published_count = 1)
      )::bigint as contributor_count,
      pg_catalog.max(fact.modified_at)
        filter (where fact.published_count = 1) as latest_contributed_at
    from scoped_facts as fact
    where fact.organization_key is not null
    group by fact.dataset_scope, fact.organization_key
  ),
  scope_ranked as (
    select
      aggregate.dataset_scope,
      pg_catalog.row_number() over (
        partition by aggregate.dataset_scope
        order by
          aggregate.published_dataset_count desc,
          aggregate.organization_name collate "C" asc,
          aggregate.organization_key collate "C" asc
      )::integer as rank,
      aggregate.organization_key,
      aggregate.organization_name,
      aggregate.published_dataset_count,
      aggregate.reviewing_dataset_count,
      aggregate.contributor_count,
      case
        when summary.published_dataset_count = 0 then 0::numeric
        else pg_catalog.round(
          aggregate.published_dataset_count::numeric
            / summary.published_dataset_count::numeric,
          6
        )
      end as contribution_share,
      aggregate.latest_contributed_at
    from scope_organization_aggregate as aggregate
    join scope_summary_values as summary using (dataset_scope)
    where aggregate.published_dataset_count > 0
  ),
  scope_ranked_limited as materialized (
    select *
    from scope_ranked
    where rank <= p_limit
  ),
  scope_payloads as materialized (
    select
      summary.dataset_scope,
      pg_catalog.jsonb_build_object(
        'datasetScope', summary.dataset_scope,
        'metric', 'latest_published_dataset_count',
        'summary', pg_catalog.jsonb_build_object(
          'organizationCount', summary.organization_count,
          'publishedDatasetCount', summary.published_dataset_count,
          'pendingReviewDatasetCount', summary.pending_review_dataset_count,
          'publishedLast30DaysCount', summary.published_last_30_days_count
        ),
        'rankings', coalesce(
          (
            select pg_catalog.jsonb_agg(
              pg_catalog.jsonb_build_object(
                'rank', ranked.rank,
                'organizationKey', ranked.organization_key,
                'organizationName', ranked.organization_name,
                'publishedDatasetCount', ranked.published_dataset_count,
                'reviewingDatasetCount', ranked.reviewing_dataset_count,
                'contributorCount', ranked.contributor_count,
                'contributionShare', ranked.contribution_share,
                'latestContributedAt', ranked.latest_contributed_at
              )
              order by ranked.rank
            )
            from scope_ranked_limited as ranked
            where ranked.dataset_scope = summary.dataset_scope
          ),
          '[]'::jsonb
        )
      ) as payload
    from scope_summary_values as summary
  ),
  snapshot_metadata as (
    select coalesce(
      pg_catalog.max(fact.modified_at),
      pg_catalog.statement_timestamp()
    ) as data_as_of
    from contribution_facts as fact
  )
  select pg_catalog.jsonb_build_object(
    'schemaVersion', 'national_carbon_organization_contribution_v1',
    'attributionMode', 'current_user_profile',
    'generatedAt', pg_catalog.statement_timestamp(),
    'dataAsOf', metadata.data_as_of,
    'defaultScope', 'all',
    'scopes', pg_catalog.jsonb_build_object(
      'process', (
        select payload from scope_payloads where dataset_scope = 'process'
      ),
      'model', (
        select payload from scope_payloads where dataset_scope = 'model'
      ),
      'all', (
        select payload from scope_payloads where dataset_scope = 'all'
      )
    )
  )
  into v_result
  from snapshot_metadata as metadata;

  return v_result;
end;
$function$;

revoke all on function api.qry_national_carbon_organization_contributions(integer)
  from public, anon, authenticated, service_role;
grant execute on function api.qry_national_carbon_organization_contributions(integer)
  to authenticated;

insert into private.api_capability_grants (
  routine_identity,
  capability_id,
  allow_anon,
  allow_authenticated,
  allow_service_role
)
values (
  'api.qry_national_carbon_organization_contributions(integer)',
  'NX-DASH-01',
  false,
  true,
  false
)
on conflict (routine_identity) do update set
  capability_id = excluded.capability_id,
  allow_anon = excluded.allow_anon,
  allow_authenticated = excluded.allow_authenticated,
  allow_service_role = excluded.allow_service_role;
