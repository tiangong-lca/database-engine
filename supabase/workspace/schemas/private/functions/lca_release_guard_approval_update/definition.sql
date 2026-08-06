CREATE OR REPLACE FUNCTION "private"."lca_release_guard_approval_update"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
begin
  if old.id is distinct from new.id
     or old.release_run_id is distinct from new.release_run_id
     or old.publish_plan_hash is distinct from new.publish_plan_hash
     or old.approval_hash is distinct from new.approval_hash
     or old.approved_by is distinct from new.approved_by
     or old.approved_at is distinct from new.approved_at
     or old.expires_at is distinct from new.expires_at
     or old.reason is distinct from new.reason
     or old.audit_correlation is distinct from new.audit_correlation
     or (old.status <> 'approved' and old.status is distinct from new.status)
     or (old.status = 'approved' and new.status not in ('approved', 'consumed', 'expired', 'revoked')) then
    raise exception 'lca_release_approval_immutable'
      using errcode = '23514';
  end if;
  return new;
end;
$$;

ALTER FUNCTION "private"."lca_release_guard_approval_update"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lca_release_guard_approval_update"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lca_release_guard_approval_update"() TO "api_internal_executor";
