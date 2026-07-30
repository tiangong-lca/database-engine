CREATE OR REPLACE FUNCTION "public"."lcia_scope_closure_preallocate_numerical_snapshot"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
begin
  if tg_op = 'UPDATE' then
    if new.numerical_snapshot_id is distinct from old.numerical_snapshot_id then
      raise exception 'numerical_snapshot_id_is_immutable' using errcode = '23514';
    end if;
    return new;
  end if;

  new.numerical_snapshot_id := coalesce(new.numerical_snapshot_id, gen_random_uuid());
  insert into public.lca_network_snapshots (
    id, scope, process_filter, provider_matching_rule, status
  ) values (
    new.numerical_snapshot_id,
    'data_product',
    jsonb_build_object(
      'schemaVersion', 'lcia.numerical-snapshot-preallocation.v1',
      'scanExecutionId', new.id,
      'requestedScopeHash', new.requested_scope_hash,
      'dataSnapshotToken', new.data_snapshot_token
    ),
    'split_by_process_volume',
    'draft'
  ) on conflict (id) do nothing;
  return new;
end;
$$;

ALTER FUNCTION "public"."lcia_scope_closure_preallocate_numerical_snapshot"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lcia_scope_closure_preallocate_numerical_snapshot"() FROM PUBLIC;
