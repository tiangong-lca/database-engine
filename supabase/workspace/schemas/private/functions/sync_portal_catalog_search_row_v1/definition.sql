CREATE OR REPLACE FUNCTION "private"."sync_portal_catalog_search_row_v1"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "row_security" TO 'on'
    AS $$
declare
  v_kind text := case tg_table_name
    when 'processes' then 'process'
    when 'flows' then 'flow'
    else null
  end;
  v_root_key text := case v_kind
    when 'process' then 'processDataSet'
    when 'flow' then 'flowDataSet'
    else null
  end;
  v_payload jsonb;
begin
  if v_kind is null then
    raise exception 'unsupported Portal projection trigger source'
      using errcode = '55000';
  end if;

  if tg_op = 'DELETE' then
    delete from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = v_kind
      and projection.id = old.id
      and projection.version = old.version::text;
    return old;
  end if;

  if tg_op = 'UPDATE'
     and (old.id, old.version::text) is distinct from (new.id, new.version::text) then
    delete from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = v_kind
      and projection.id = old.id
      and projection.version = old.version::text;
  end if;


  if new.state_code in (100, 200)
     and new.modified_at is not null
     and pg_catalog.jsonb_typeof(new.json) = 'object'
     and pg_catalog.jsonb_typeof(new.json -> v_root_key) = 'object' then
    v_payload := private.catalog_portal_projection_payload_v1(
      v_kind,
      new.state_code,
      new.json
    );
    if pg_catalog.jsonb_typeof(v_payload) <> 'object' then
      raise exception 'Portal projection payload is invalid'
        using errcode = '55000';
    end if;
    insert into private.portal_catalog_search_rows_v1 (
      dataset_kind,
      id,
      version,
      state_code,
      modified_at,
      card,
      document,
      projection_contract_version
    ) values (
      v_kind,
      new.id,
      new.version::text,
      new.state_code,
      new.modified_at,
      v_payload -> 'card',
      v_payload ->> 'document',
      1
    )
    on conflict (dataset_kind, id, version) do update
    set state_code = excluded.state_code,
        modified_at = excluded.modified_at,
        card = excluded.card,
        document = excluded.document,
        projection_contract_version =
          excluded.projection_contract_version;
  else
    delete from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = v_kind
      and projection.id = new.id
      and projection.version = new.version::text;
  end if;
  return new;
end
$$;

ALTER FUNCTION "private"."sync_portal_catalog_search_row_v1"() OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."sync_portal_catalog_search_row_v1"() FROM PUBLIC;

REVOKE ALL ON FUNCTION "private"."sync_portal_catalog_search_row_v1"() FROM "api_internal_executor";
