CREATE OR REPLACE FUNCTION "private"."cmd_dataset_semantic_backfill"("p_table" "text", "p_batch_size" integer DEFAULT 100, "p_after_id" "uuid" DEFAULT NULL::"uuid", "p_after_version" "text" DEFAULT NULL::"text", "p_force_extraction" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_table text := lower(btrim(coalesce(p_table, '')));
  v_entity_kind text;
  v_content_function text;
  v_batch_size integer := least(greatest(coalesce(p_batch_size, 100), 1), 500);
  v_row_id uuid;
  v_row_version text;
  v_needs_extraction boolean;
  v_needs_embedding boolean;
  v_scanned_count integer := 0;
  v_extraction_enqueued integer := 0;
  v_embedding_enqueued integer := 0;
  v_already_queued integer := 0;
  v_last_id uuid;
  v_last_version text;
  v_job_exists boolean;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required'
    );
  end if;

  v_entity_kind := case v_table
    when 'contacts' then 'contact'
    when 'flowproperties' then 'flowproperty'
    when 'sources' then 'source'
    when 'unitgroups' then 'unitgroup'
    else null
  end;

  if v_entity_kind is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'UNSUPPORTED_DATASET_TABLE',
      'status', 400,
      'message', format('Unsupported semantic backfill table: %s', coalesce(p_table, '<null>'))
    );
  end if;

  v_content_function := v_table || '_embedding_ft_input';

  for v_row_id, v_row_version, v_needs_extraction, v_needs_embedding in execute format(
    $sql$
      select
        dataset.id,
        btrim(dataset.version::text) as version,
        dataset.extracted_md is null or $4 as needs_extraction,
        dataset.embedding_ft is null as needs_embedding
      from public.%I dataset
      where dataset.json is not null
        and nullif(btrim(dataset.version::text), '') is not null
        and ($4 or dataset.extracted_md is null or dataset.embedding_ft is null)
        and (
          $1 is null
          or (dataset.id, btrim(dataset.version::text)) > ($1, coalesce($2, ''))
        )
      order by dataset.id, btrim(dataset.version::text)
      limit $3
    $sql$,
    v_table
  ) using p_after_id, p_after_version, v_batch_size, coalesce(p_force_extraction, false)
  loop
    v_scanned_count := v_scanned_count + 1;
    v_last_id := v_row_id;
    v_last_version := v_row_version;

    if v_needs_extraction then
      select exists (
        select 1
        from pgmq.q_dataset_extraction_jobs queued
        where queued.message->>'schema' = 'public'
          and queued.message->>'table' = v_table
          and queued.message->>'id' = v_row_id::text
          and queued.message->>'version' = v_row_version
          and queued.message->>'extraction_kind' = 'extracted_md'
      ) into v_job_exists;

      if v_job_exists then
        v_already_queued := v_already_queued + 1;
      else
        perform pgmq.send(
          queue_name => 'dataset_extraction_jobs',
          msg => jsonb_build_object(
            'schema', 'public',
            'table', v_table,
            'id', v_row_id,
            'version', v_row_version,
            'entity_kind', v_entity_kind,
            'extraction_kind', 'extracted_md',
            'created_at', clock_timestamp(),
            'backfill', true
          )
        );
        v_extraction_enqueued := v_extraction_enqueued + 1;
      end if;
    elsif v_needs_embedding then
      select exists (
        select 1
        from pgmq.q_embedding_jobs queued
        where queued.message->>'schema' = 'public'
          and queued.message->>'table' = v_table
          and queued.message->>'id' = v_row_id::text
          and queued.message->>'version' = v_row_version
          and queued.message->>'embeddingColumn' = 'embedding_ft'
          and queued.message->>'edgeFunction' = 'embedding_ft'
      ) or exists (
        select 1
        from util.pending_embedding_jobs pending
        where pending.schema_name = 'public'
          and pending.table_name = v_table
          and pending.record_id = v_row_id::text
          and pending.record_version = v_row_version
          and pending.embedding_column = 'embedding_ft'
          and pending.edge_function = 'embedding_ft'
          and pending.status in ('pending', 'enqueued')
      ) into v_job_exists;

      if v_job_exists then
        v_already_queued := v_already_queued + 1;
      else
        perform pgmq.send(
          queue_name => 'embedding_jobs',
          msg => jsonb_build_object(
            'id', v_row_id,
            'version', v_row_version,
            'schema', 'public',
            'table', v_table,
            'contentFunction', v_content_function,
            'embeddingColumn', 'embedding_ft',
            'edgeFunction', 'embedding_ft',
            'backfill', true
          )
        );
        v_embedding_enqueued := v_embedding_enqueued + 1;
      end if;
    end if;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'table', v_table,
    'scanned_count', v_scanned_count,
    'extraction_enqueued_count', v_extraction_enqueued,
    'embedding_enqueued_count', v_embedding_enqueued,
    'already_queued_count', v_already_queued,
    'last_id', v_last_id,
    'last_version', v_last_version,
    'has_more', v_scanned_count = v_batch_size
  );
end;
$_$;

ALTER FUNCTION "private"."cmd_dataset_semantic_backfill"("p_table" "text", "p_batch_size" integer, "p_after_id" "uuid", "p_after_version" "text", "p_force_extraction" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."cmd_dataset_semantic_backfill"("p_table" "text", "p_batch_size" integer, "p_after_id" "uuid", "p_after_version" "text", "p_force_extraction" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."cmd_dataset_semantic_backfill"("p_table" "text", "p_batch_size" integer, "p_after_id" "uuid", "p_after_version" "text", "p_force_extraction" boolean) TO "service_role";

GRANT ALL ON FUNCTION "private"."cmd_dataset_semantic_backfill"("p_table" "text", "p_batch_size" integer, "p_after_id" "uuid", "p_after_version" "text", "p_force_extraction" boolean) TO "api_internal_executor";
