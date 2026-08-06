CREATE OR REPLACE FUNCTION "util"."purge_supabase_functions_hooks"("p_retention_window" interval DEFAULT '14 days'::interval, "p_batch_size" integer DEFAULT 50000) RETURNS bigint
    LANGUAGE "plpgsql"
    SET "search_path" TO ''
    AS $$
declare
  deleted_count bigint;
begin
  if p_retention_window is null or p_retention_window < interval '1 day' then
    raise exception using
      errcode = '22023',
      message = 'supabase functions hooks retention window must be at least 1 day';
  end if;

  if p_batch_size is null or p_batch_size < 1 or p_batch_size > 100000 then
    raise exception using
      errcode = '22023',
      message = 'supabase functions hooks purge batch size must be between 1 and 100000';
  end if;

  if to_regclass('supabase_functions.hooks') is null then
    return 0;
  end if;

  if not pg_catalog.pg_try_advisory_xact_lock(
    pg_catalog.hashtext('util.purge_supabase_functions_hooks')
  ) then
    return 0;
  end if;

  with live_responses as materialized (
    select response.id
    from net._http_response as response
  ), candidates as (
    select hooks.id
    from supabase_functions.hooks as hooks
    left join live_responses on live_responses.id = hooks.request_id
    where hooks.created_at < pg_catalog.now() - p_retention_window
      and live_responses.id is null
    order by hooks.created_at, hooks.id
    limit p_batch_size
    for update of hooks skip locked
  )
  delete from supabase_functions.hooks as hooks
  using candidates
  where hooks.id = candidates.id;

  get diagnostics deleted_count = row_count;
  return deleted_count;
end;
$$;

ALTER FUNCTION "util"."purge_supabase_functions_hooks"("p_retention_window" interval, "p_batch_size" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."purge_supabase_functions_hooks"("p_retention_window" interval, "p_batch_size" integer) FROM PUBLIC;
