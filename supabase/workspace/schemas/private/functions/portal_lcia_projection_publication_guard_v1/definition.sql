CREATE OR REPLACE FUNCTION "private"."portal_lcia_projection_publication_guard_v1"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO ''
    AS $$
begin
  if tg_op = 'DELETE' then
    raise exception 'portal_lcia_projection_publication_immutable'
      using errcode = '55000';
  end if;
  if old.status <> 'finalized'
     or new.status <> 'revoked'
     or new.id is distinct from old.id
     or new.projection_id is distinct from old.projection_id
     or new.lcia_result_publication_id is distinct from old.lcia_result_publication_id
     or new.package_id is distinct from old.package_id
     or new.package_version is distinct from old.package_version
     or new.package_result_hash is distinct from old.package_result_hash
     or new.projection_content_hash is distinct from old.projection_content_hash
     or new.evidence_hash is distinct from old.evidence_hash
     or new.source_published_at is distinct from old.source_published_at
     or new.idempotency_key is distinct from old.idempotency_key
     or new.finalized_by is distinct from old.finalized_by
     or new.finalized_at is distinct from old.finalized_at then
    raise exception 'portal_lcia_projection_publication_immutable'
      using errcode = '55000';
  end if;
  return new;
end
$$;

ALTER FUNCTION "private"."portal_lcia_projection_publication_guard_v1"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_projection_publication_guard_v1"() FROM PUBLIC;
