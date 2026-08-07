CREATE OR REPLACE FUNCTION "api"."svc_data_product_publication_list"("p_limit" integer DEFAULT 50) RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_limit integer := least(greatest(coalesce(p_limit, 50), 1), 200);
  v_rows jsonb;
begin
  if v_actor is null then
    return jsonb_build_object('ok', false, 'code', 'AUTH_REQUIRED', 'status', 401);
  end if;

  if not exists (
    select 1
    from private.roles as role_row
    where role_row.user_id = v_actor
      and role_row.team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role_row.role = 'data_product_manager'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_PRODUCT_MANAGER_REQUIRED',
      'status', 403
    );
  end if;

  select coalesce(
    jsonb_agg(
      candidate.payload
      order by candidate.is_current desc, candidate.published_at desc nulls last,
        candidate.created_at desc, candidate.publication_id
    ),
    '[]'::jsonb
  )
  into v_rows
  from (
    select
      publication.id as publication_id,
      publication.is_current,
      publication.published_at,
      publication.created_at,
      jsonb_strip_nulls(jsonb_build_object(
        'publicationId', publication.id,
        'packageId', publication.package_id,
        'packageName', coalesce(
          worker.payload_json ->> 'name',
          worker.payload_json ->> 'packageName',
          worker.payload_json ->> 'package_name'
        ),
        'packageVersion', package.package_version,
        'status', publication.status,
        'isCurrent', publication.is_current,
        'publicationSeriesKey', publication.publication_series_key,
        'publicationChannel', publication.publication_channel,
        'visibilityScope', publication.visibility_scope,
        'displayDefaultImpactCategory', publication.display_default_impact_category,
        'publishedAt', publication.published_at,
        'unpublishedAt', publication.unpublished_at,
        'reason', publication.reason,
        'eligibleInputCount', package.eligible_input_count,
        'includedInputCount', package.included_input_count,
        'packageStatus', package.status
      )) as payload
    from private.lcia_result_publications as publication
    left join private.lcia_result_packages as package on package.id = publication.package_id
    left join private.worker_jobs as worker on worker.id = package.build_worker_job_id
    order by publication.is_current desc, publication.published_at desc nulls last,
      publication.created_at desc, publication.id
    limit v_limit
  ) as candidate;

  return jsonb_build_object('ok', true, 'data', v_rows);
end
$$;

ALTER FUNCTION "api"."svc_data_product_publication_list"("p_limit" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_data_product_publication_list"("p_limit" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_data_product_publication_list"("p_limit" integer) TO "authenticated";
