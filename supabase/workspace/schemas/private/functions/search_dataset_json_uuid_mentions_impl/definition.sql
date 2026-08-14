CREATE OR REPLACE FUNCTION "private"."search_dataset_json_uuid_mentions_impl"("p_uuid" "uuid", "p_source_entity_kinds" "text"[] DEFAULT NULL::"text"[], "p_data_source" "text" DEFAULT 'tg'::"text", "p_this_user_id" "text" DEFAULT ''::"text", "p_team_id_filter" "uuid" DEFAULT NULL::"uuid", "p_state_code_filter" integer DEFAULT NULL::integer, "p_limit" integer DEFAULT 20) RETURNS TABLE("rank" bigint, "source_entity_kind" "text", "source_id" "uuid", "source_version" character, "source_name" "text", "source_modified_at" timestamp with time zone, "source_team_id" "uuid", "source_json" "jsonb", "matched_by" "text", "matched_entity_table" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '20s'
    AS $_$
declare
  normalized_data_source text;
  effective_user_id uuid;
  can_read_team_filter boolean;
  normalized_limit integer;
  per_entity_limit integer;
  uuid_pattern text;
  normalized_source_entity_kinds text[];
  branches text[] := array[]::text[];
  v_sql text;
begin
  normalized_data_source := coalesce(nullif(lower(btrim(p_data_source)), ''), 'tg');
  effective_user_id := private.dataset_search_effective_user_id(p_this_user_id);
  can_read_team_filter := private.dataset_search_can_read_team_filter(p_team_id_filter, effective_user_id);
  normalized_limit := least(greatest(coalesce(p_limit, 20), 1), 50);
  per_entity_limit := normalized_limit;
  uuid_pattern := '%' || p_uuid::text || '%';

  if p_source_entity_kinds is not null then
    select array_agg(distinct normalized_kind order by normalized_kind)
    into normalized_source_entity_kinds
    from (
      select case lower(btrim(kind))
        when 'flow' then 'flow'
        when 'flows' then 'flow'
        when 'process' then 'process'
        when 'processes' then 'process'
        when 'lifecyclemodel' then 'lifecyclemodel'
        when 'lifecyclemodels' then 'lifecyclemodel'
        when 'model' then 'lifecyclemodel'
        when 'models' then 'lifecyclemodel'
        when 'source' then 'source'
        when 'sources' then 'source'
        when 'contact' then 'contact'
        when 'contacts' then 'contact'
        when 'unitgroup' then 'unitgroup'
        when 'unitgroups' then 'unitgroup'
        when 'flowproperty' then 'flowproperty'
        when 'flowproperties' then 'flowproperty'
        else null
      end as normalized_kind
      from unnest(p_source_entity_kinds) as requested(kind)
    ) normalized
    where normalized_kind is not null;

    if coalesce(array_length(normalized_source_entity_kinds, 1), 0) = 0 then
      return;
    end if;
  end if;

  if normalized_source_entity_kinds is null or 'process' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          10::integer as entity_rank,
          'process'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('process', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.processes'::text as matched_entity_table
        from public.processes d
        where (
            ($1 = 'tg' and d.state_code = 100 and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'flow' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          20::integer as entity_rank,
          'flow'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('flow', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.flows'::text as matched_entity_table
        from public.flows d
        where (
            ($1 = 'tg' and d.state_code = 100 and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'lifecyclemodel' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          30::integer as entity_rank,
          'lifecyclemodel'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('lifecyclemodel', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.lifecyclemodels'::text as matched_entity_table
        from public.lifecyclemodels d
        where (
            ($1 = 'tg' and d.state_code = 100 and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'source' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          40::integer as entity_rank,
          'source'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('source', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.sources'::text as matched_entity_table
        from public.sources d
        where (
            ($1 = 'tg' and d.state_code = 100 and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'contact' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          50::integer as entity_rank,
          'contact'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('contact', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.contacts'::text as matched_entity_table
        from public.contacts d
        where (
            ($1 = 'tg' and d.state_code = 100 and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'unitgroup' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          60::integer as entity_rank,
          'unitgroup'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('unitgroup', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.unitgroups'::text as matched_entity_table
        from public.unitgroups d
        where (
            ($1 = 'tg' and d.state_code = 100 and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'flowproperty' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          70::integer as entity_rank,
          'flowproperty'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('flowproperty', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.flowproperties'::text as matched_entity_table
        from public.flowproperties d
        where (
            ($1 = 'tg' and d.state_code = 100 and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if coalesce(array_length(branches, 1), 0) = 0 then
    return;
  end if;

  v_sql := format($sql$
    with matched_rows as (
      %s
    )
    select
      row_number() over (
        order by entity_rank, source_modified_at desc nulls last, source_entity_kind, source_id
      )::bigint as rank,
      source_entity_kind,
      source_id,
      source_version,
      source_name,
      source_modified_at,
      source_team_id,
      source_json,
      matched_by,
      matched_entity_table
    from matched_rows
    order by entity_rank, source_modified_at desc nulls last, source_entity_kind, source_id
    limit $7
  $sql$, array_to_string(branches, E'\nunion all\n'));

  return query execute v_sql
    using normalized_data_source, effective_user_id, p_team_id_filter, p_state_code_filter,
          can_read_team_filter, uuid_pattern, normalized_limit, per_entity_limit;
end;
$_$;

ALTER FUNCTION "private"."search_dataset_json_uuid_mentions_impl"("p_uuid" "uuid", "p_source_entity_kinds" "text"[], "p_data_source" "text", "p_this_user_id" "text", "p_team_id_filter" "uuid", "p_state_code_filter" integer, "p_limit" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."search_dataset_json_uuid_mentions_impl"("p_uuid" "uuid", "p_source_entity_kinds" "text"[], "p_data_source" "text", "p_this_user_id" "text", "p_team_id_filter" "uuid", "p_state_code_filter" integer, "p_limit" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."search_dataset_json_uuid_mentions_impl"("p_uuid" "uuid", "p_source_entity_kinds" "text"[], "p_data_source" "text", "p_this_user_id" "text", "p_team_id_filter" "uuid", "p_state_code_filter" integer, "p_limit" integer) TO "service_role";

GRANT ALL ON FUNCTION "private"."search_dataset_json_uuid_mentions_impl"("p_uuid" "uuid", "p_source_entity_kinds" "text"[], "p_data_source" "text", "p_this_user_id" "text", "p_team_id_filter" "uuid", "p_state_code_filter" integer, "p_limit" integer) TO "api_internal_executor";
