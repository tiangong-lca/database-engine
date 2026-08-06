CREATE OR REPLACE FUNCTION "util"."queue_dataset_extraction_jobs"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_entity_kind text;
  v_message jsonb;
begin
  if TG_TABLE_SCHEMA <> 'public' then
    raise exception 'dataset extraction jobs only support public schema, got %', TG_TABLE_SCHEMA;
  end if;

  v_entity_kind := case TG_TABLE_NAME
    when 'flows' then 'flow'
    when 'processes' then 'process'
    when 'contacts' then 'contact'
    when 'flowproperties' then 'flowproperty'
    when 'sources' then 'source'
    when 'unitgroups' then 'unitgroup'
    else null
  end;

  if v_entity_kind is null then
    raise exception 'unsupported dataset extraction table %', TG_TABLE_NAME;
  end if;

  v_message := jsonb_build_object(
    'schema', TG_TABLE_SCHEMA,
    'table', TG_TABLE_NAME,
    'id', NEW.id,
    'version', btrim(NEW.version::text),
    'entity_kind', v_entity_kind,
    'extraction_kind', 'extracted_md',
    'created_at', clock_timestamp()
  );

  perform pgmq.send(
    queue_name => 'dataset_extraction_jobs',
    msg => v_message
  );

  return NEW;
end;
$$;

ALTER FUNCTION "util"."queue_dataset_extraction_jobs"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."queue_dataset_extraction_jobs"() FROM PUBLIC;
