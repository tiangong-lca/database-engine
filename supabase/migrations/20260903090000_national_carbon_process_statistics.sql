-- #604: process-only dashboard. p_limit limits the chart, never the organization table.
-- Existing primary (id, version), published/latest-version, created_at, and active-root
-- review indexes serve the narrow scans. Do not index or aggregate whole json_ordered values.
create or replace function api.qry_national_carbon_organization_contributions(
  p_limit integer default 10
)
returns jsonb
language plpgsql stable security definer
set search_path = ''
as $function$
declare
  v_actor uuid := auth.uid();
  v_result jsonb;
begin
  if v_actor is null then
    raise exception using errcode = '28000', message = 'AUTH_REQUIRED';
  end if;
  if not api.cmd_membership_is_system_manager(v_actor) then
    raise exception using errcode = '42501', message = 'SYSTEM_MANAGER_REQUIRED';
  end if;
  if p_limit is null or p_limit < 1 or p_limit > 50 then
    raise exception using errcode = '22023', message = 'INVALID_LIMIT';
  end if;

  with
  date_bounds as materialized (
    select
      pg_catalog.date_trunc('week', pg_catalog.timezone('Asia/Shanghai',
        pg_catalog.statement_timestamp()))::date - 364 as start_date,
      pg_catalog.timezone('Asia/Shanghai', pg_catalog.statement_timestamp())::date as end_date
  ),
  daily_counts as (
    -- The processes primary key already enforces one row per dataset ID + version.
    -- Date filtering uses created_at directly; status changes do not change creation history.
    select pg_catalog.timezone('Asia/Shanghai', p.created_at)::date as day,
      pg_catalog.count(*)::bigint as process_count
    from public.processes p cross join date_bounds b
    where p.created_at >= pg_catalog.timezone('Asia/Shanghai', b.start_date::timestamp)
      and p.created_at < pg_catalog.timezone('Asia/Shanghai', (b.end_date + 1)::timestamp)
    group by 1
  ),
  daily_payload as (
    select pg_catalog.jsonb_build_object(
      'metric', 'dataset_version_created_count',
      'deduplicationKey', pg_catalog.jsonb_build_array('datasetType', 'datasetId', 'version'),
      'timezone', 'Asia/Shanghai', 'startDate', b.start_date, 'endDate', b.end_date,
      'days', pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'date', d.day::date, 'processCount', coalesce(c.process_count, 0)
      ) order by d.day)
    ) as payload
    from date_bounds b
    cross join lateral pg_catalog.generate_series(b.start_date::timestamp,
      b.end_date::timestamp, interval '1 day') as d(day)
    left join daily_counts c on c.day = d.day::date
    group by b.start_date, b.end_date
  ),
  published_keys as materialized (
    -- Select IDs/versions first, then read geography only for the winning open version.
    select distinct on (p.id) p.id, p.version, p.user_id, p.modified_at
    from public.processes p where p.state_code = 100
    order by p.id, p.version desc, p.modified_at desc
  ),
  latest_current as materialized (
    select distinct on (p.id) p.id, p.version, p.user_id, p.state_code, p.modified_at
    from public.processes p
    order by p.id, p.version desc, p.modified_at desc
  ),
  pending as materialized (
    select p.user_id, p.modified_at,
      case when r.state_code = 1 then 1 else 0 end as assigned_count,
      case when r.state_code = 1 then 0 else 1 end as unassigned_count
    from latest_current p
    left join lateral (
      select review.state_code from private.reviews review
      where review.review_kind = 'root' and review.target_table = 'processes'
        and review.data_id = p.id and review.data_version = p.version
        and review.state_code in (0, 1)
      order by review.state_code desc, review.modified_at desc, review.id
      limit 1
    ) r on true
    where p.state_code = 20
  ),
  profile_names as materialized (
    select u.id as user_id,
      nullif(pg_catalog.regexp_replace(pg_catalog.btrim(u.raw_user_meta_data ->> 'organization'),
        '[[:space:]]+', ' ', 'g'), '') as organization_name
    from private.users u
    where pg_catalog.jsonb_typeof(u.raw_user_meta_data -> 'organization') = 'string'
  ),
  profiles as materialized (
    select user_id, organization_name, pg_catalog.lower(organization_name) as organization_key
    from profile_names where organization_name is not null
  ),
  catalog as (
    select organization_key, pg_catalog.min(organization_name collate "C") as organization_name
    from profiles group by organization_key
  ),
  facts as materialized (
    select user_id, modified_at, 1 as published_count, 0 as assigned_count, 0 as unassigned_count
    from published_keys
    union all
    select user_id, modified_at, 0, assigned_count, unassigned_count from pending
  ),
  aggregates as (
    select p.organization_key, pg_catalog.sum(f.published_count)::bigint as published_count,
      pg_catalog.sum(f.assigned_count)::bigint as assigned_count,
      pg_catalog.sum(f.unassigned_count)::bigint as unassigned_count
    from facts f join profiles p using (user_id)
    group by p.organization_key
  ),
  organizations as materialized (
    select pg_catalog.row_number() over (order by coalesce(a.published_count, 0) desc,
      c.organization_name collate "C", c.organization_key collate "C")::integer as rank,
      c.organization_key, c.organization_name,
      coalesce(a.published_count, 0)::bigint as published_count,
      coalesce(a.assigned_count, 0)::bigint as assigned_count,
      coalesce(a.unassigned_count, 0)::bigint as unassigned_count
    from catalog c left join aggregates a using (organization_key)
  ),
  organization_rows as materialized (
    select rank, published_count, pg_catalog.jsonb_build_object(
      'rank', rank, 'organizationKey', organization_key, 'organizationName', organization_name,
      'publishedDatasetCount', published_count,
      'assignedReviewerDatasetCount', assigned_count,
      'unassignedReviewerDatasetCount', unassigned_count
    ) as payload from organizations
  ),
  locations as (
    select case when pg_catalog.json_typeof(location.value) = 'string'
      then nullif(pg_catalog.upper(pg_catalog.btrim(location.value #>> '{}')), '')
      else null end as location_code
    from published_keys k join public.processes p on p.id = k.id and p.version = k.version
    cross join lateral (select p.json_ordered #>
      '{processDataSet,processInformation,geography,locationOfOperationSupplyOrProduction,@location}'
      as value) location
  ),
  regions as materialized (
    select location_code, pg_catalog.count(*)::bigint as process_count
    from locations group by location_code
  )
  select pg_catalog.jsonb_build_object(
    'schemaVersion', 'national_carbon_organization_contribution_v4',
    'datasetScope', 'process', 'attributionMode', 'current_user_profile',
    'generatedAt', pg_catalog.statement_timestamp(),
    'dataAsOf', coalesce((select pg_catalog.max(modified_at) from facts), pg_catalog.statement_timestamp()),
    'summary', pg_catalog.jsonb_build_object(
      'organizationCount', (select pg_catalog.count(*) from organizations),
      'publishedDatasetCount', (select coalesce(pg_catalog.sum(published_count), 0) from organizations),
      'pendingReviewDatasetCount', (select coalesce(pg_catalog.sum(assigned_count + unassigned_count), 0) from organizations),
      'reviewerCount', (select pg_catalog.count(distinct r.user_id) from private.roles r
        where r.team_id = '00000000-0000-0000-0000-000000000000'::uuid and r.role = 'review-member')
    ),
    'rankings', coalesce((select pg_catalog.jsonb_agg(payload order by rank)
      from organization_rows where published_count > 0 and rank <= p_limit), '[]'::jsonb),
    'organizations', coalesce((select pg_catalog.jsonb_agg(payload order by rank)
      from organization_rows), '[]'::jsonb),
    'regions', pg_catalog.jsonb_build_object(
      'metric', 'latest_open_process_count',
      'totalProcessCount', (select pg_catalog.count(*) from published_keys),
      'items', coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
        'locationCode', location_code, 'processCount', process_count
      ) order by process_count desc, location_code collate "C") from regions
        where location_code is not null and location_code <> 'GLO'), '[]'::jsonb),
      'globalProcessCount', coalesce((select process_count from regions where location_code = 'GLO'), 0),
      'unassignedProcessCount', coalesce((select process_count from regions where location_code is null), 0)
    ),
    'dailyCreation', (select payload from daily_payload)
  ) into v_result;
  return v_result;
end;
$function$;

alter function api.qry_national_carbon_organization_contributions(integer) owner to postgres;
revoke all on function api.qry_national_carbon_organization_contributions(integer) from public, anon, service_role;
grant execute on function api.qry_national_carbon_organization_contributions(integer) to authenticated;
