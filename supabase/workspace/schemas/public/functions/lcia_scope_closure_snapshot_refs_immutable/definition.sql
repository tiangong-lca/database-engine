CREATE OR REPLACE FUNCTION "public"."lcia_scope_closure_snapshot_refs_immutable"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
begin
  if (old.snapshot_id is not null and new.snapshot_id is distinct from old.snapshot_id)
     or (old.snapshot_artifact_id is not null and new.snapshot_artifact_id is distinct from old.snapshot_artifact_id) then
    raise exception 'lcia_scope_closure_snapshot_reference_is_immutable'
      using errcode = '23514';
  end if;
  return new;
end;
$$;

ALTER FUNCTION "public"."lcia_scope_closure_snapshot_refs_immutable"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lcia_scope_closure_snapshot_refs_immutable"() FROM PUBLIC;
