CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_artifact_v2_item_guard"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_contract_version text;
  v_status text;
  v_write_set_id uuid := case
    when tg_op = 'DELETE' then old.write_set_id
    else new.write_set_id
  end;
begin
  select write_set.contract_version, write_set.status
  into v_contract_version, v_status
  from private.lcia_scope_closure_artifact_write_sets write_set
  where write_set.id = v_write_set_id;

  if v_contract_version is null then
    return case when tg_op = 'DELETE' then old else new end;
  end if;
  if tg_op = 'INSERT' and v_status = 'registration_open' then
    return new;
  end if;
  raise exception 'artifact_write_set_v2_items_are_immutable'
    using errcode = '23514';
end;
$$;

ALTER FUNCTION "private"."lcia_scope_closure_artifact_v2_item_guard"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_artifact_v2_item_guard"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_artifact_v2_item_guard"() TO "api_internal_executor";
