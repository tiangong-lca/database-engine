CREATE OR REPLACE FUNCTION "util"."guard_dataset_derivative_rebuild_primary"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_request_id uuid;
begin
  if tg_table_schema <> 'public'
    or tg_table_name not in ('flows', 'processes') then
    raise exception using
      errcode = '22023',
      message = 'Unsupported derivative rebuild fence target';
  end if;

  select request.id
  into v_request_id
  from util.dataset_derivative_rebuild_requests as request
  where request.target_table = tg_table_name
    and request.target_id = old.id
    and request.target_version = btrim(old.version::text)
    and request.status not in ('completed', 'stale', 'failed')
  limit 1;

  if v_request_id is not null then
    raise exception using
      errcode = '55006',
      message = case tg_table_name
        when 'processes'
          then 'Process primary row is fenced by an active derivative rebuild'
        else 'Flow primary row is fenced by an active derivative rebuild'
      end,
      detail = v_request_id::text;
  end if;

  if tg_op = 'DELETE' then
    return old;
  end if;
  return new;
end;
$$;

ALTER FUNCTION "util"."guard_dataset_derivative_rebuild_primary"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."guard_dataset_derivative_rebuild_primary"() FROM PUBLIC;
