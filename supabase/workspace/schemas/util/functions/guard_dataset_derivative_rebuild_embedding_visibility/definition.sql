CREATE OR REPLACE FUNCTION "util"."guard_dataset_derivative_rebuild_embedding_visibility"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
begin
  if exists (
    select 1
    from util.dataset_derivative_rebuild_requests as request
    where request.id::text = new.message->>'requestId'
      and request.target_table = new.message->>'table'
      and request.target_table in ('flows', 'processes')
      and request.target_id::text = new.message->>'id'
      and request.target_version = btrim(new.message->>'version')
      and new.message->>'schema' = 'public'
      and new.message->>'embeddingColumn' = 'embedding_ft'
      and request.status not in ('completed', 'stale', 'failed')
  ) then
    new.vt := greatest(
      new.vt,
      pg_catalog.clock_timestamp() + interval '420 seconds'
    );
  end if;

  return new;
end;
$$;

ALTER FUNCTION "util"."guard_dataset_derivative_rebuild_embedding_visibility"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."guard_dataset_derivative_rebuild_embedding_visibility"() FROM PUBLIC;
