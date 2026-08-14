CREATE OR REPLACE FUNCTION "api"."svc_dataset_search_text_backfill_enqueue"("p_entity_kind" "text", "p_after_id" "uuid" DEFAULT NULL::"uuid", "p_after_version" "text" DEFAULT NULL::"text", "p_limit" integer DEFAULT 100) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_entity_kind text := lower(btrim(coalesce(p_entity_kind, '')));
  v_table name;
  v_queue_entity_kind text;
  v_limit integer := least(greatest(coalesce(p_limit, 100), 1), 500);
  v_cursor_version character(9);
  v_result jsonb;
begin
  if coalesce(current_setting('request.jwt.claim.role', true), '') <> 'service_role' then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required'
    );
  end if;

  select mapping.table_name, mapping.queue_entity_kind
  into v_table, v_queue_entity_kind
  from (values
    ('contact', 'contacts'::name, 'contact'),
    ('flowproperty', 'flowproperties'::name, 'flowproperty'),
    ('flow', 'flows'::name, 'flow'),
    ('lifecyclemodel', 'lifecyclemodels'::name, 'lifecyclemodel'),
    ('process', 'processes'::name, 'process'),
    ('source', 'sources'::name, 'source'),
    ('unitgroup', 'unitgroups'::name, 'unitgroup')
  ) as mapping(entity_kind, table_name, queue_entity_kind)
  where mapping.entity_kind = v_entity_kind;

  if v_table is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_ENTITY_KIND',
      'status', 400,
      'message', 'entity_kind must be one of contact, flowproperty, flow, lifecyclemodel, process, source, or unitgroup'
    );
  end if;

  if p_after_id is null and p_after_version is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_CURSOR',
      'status', 400,
      'message', 'after_version requires after_id'
    );
  end if;

  v_cursor_version := nullif(btrim(coalesce(p_after_version, '')), '')::character(9);

  execute format($sql$
    with candidates as materialized (
      select row.id, row.version
      from public.%1$I as row
      where row.search_text is null
        and (
          $1::uuid is null
          or (row.id, row.version) > ($1::uuid, coalesce($2::character(9), ''::character(9)))
        )
      order by row.id, row.version
      limit $3
    ), enqueueable as materialized (
      select candidate.id, candidate.version
      from candidates as candidate
      where not exists (
        select 1
        from pgmq.q_dataset_extraction_jobs as queued
        where queued.message ->> 'schema' = 'public'
          and queued.message ->> 'table' = %1$L
          and queued.message ->> 'id' = candidate.id::text
          and queued.message ->> 'version' = candidate.version::text
          and queued.message ->> 'extraction_kind' = 'search_text'
      )
    ), sent as (
      select pgmq.send(
        'dataset_extraction_jobs',
        jsonb_build_object(
          'schema', 'public',
          'table', %1$L,
          'id', enqueueable.id,
          'version', enqueueable.version,
          'entity_kind', %2$L,
          'extraction_kind', 'search_text',
          'created_at', clock_timestamp()
        )
      ) as msg_id
      from enqueueable
    ), summary as (
      select
        (select count(*) from candidates) as scanned,
        (select count(*) from sent) as enqueued,
        (select count(*) from candidates) - (select count(*) from sent) as already_queued,
        (select id from candidates order by id desc, version desc limit 1) as next_after_id,
        (select version from candidates order by id desc, version desc limit 1) as next_after_version
    )
    select jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'entity_kind', %2$L,
        'table', %1$L,
        'limit', $3,
        'scanned', summary.scanned,
        'enqueued', summary.enqueued,
        'already_queued', summary.already_queued,
        'next_after_id', summary.next_after_id,
        'next_after_version', summary.next_after_version
      )
    )
    from summary
  $sql$, v_table, v_queue_entity_kind)
  into v_result
  using p_after_id, v_cursor_version, v_limit;

  return v_result;
end
$_$;

ALTER FUNCTION "api"."svc_dataset_search_text_backfill_enqueue"("p_entity_kind" "text", "p_after_id" "uuid", "p_after_version" "text", "p_limit" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_dataset_search_text_backfill_enqueue"("p_entity_kind" "text", "p_after_id" "uuid", "p_after_version" "text", "p_limit" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_dataset_search_text_backfill_enqueue"("p_entity_kind" "text", "p_after_id" "uuid", "p_after_version" "text", "p_limit" integer) TO "service_role";
