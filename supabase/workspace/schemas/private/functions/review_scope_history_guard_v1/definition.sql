CREATE OR REPLACE FUNCTION "private"."review_scope_history_guard_v1"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
begin
  if tg_op = 'DELETE' and old.review_kind in ('root', 'reference') then
    raise exception using errcode = '55000', message = 'REVIEW_HISTORY_DELETE_FORBIDDEN';
  end if;

  if tg_op = 'UPDATE' then
    if old.review_kind is null and new.review_kind is not null then
      -- Reserved for the explicit legacy migration.
      if pg_catalog.current_setting('app.review_legacy_migration', true)
        is distinct from 'on' then
        raise exception using errcode = '55000', message = 'LEGACY_REVIEW_READ_ONLY';
      end if;
    elsif old.review_kind is distinct from new.review_kind
      or old.target_table is distinct from new.target_table
      or old.data_id is distinct from new.data_id
      or old.data_version is distinct from new.data_version
      or old.submitted_revision_checksum
        is distinct from new.submitted_revision_checksum
      or old.target_owner_id is distinct from new.target_owner_id
      or old.target_team_id is distinct from new.target_team_id
      or old.scope_schema_version is distinct from new.scope_schema_version then
      raise exception using errcode = '55000', message = 'REVIEW_IDENTITY_IMMUTABLE';
    end if;

    if old.scope_history is distinct from new.scope_history then
      if pg_catalog.current_setting('app.review_scope_write', true)
        is distinct from 'on' then
        raise exception using errcode = '55000', message = 'REVIEW_SCOPE_DIRECT_WRITE_FORBIDDEN';
      end if;

      perform private.review_validate_scope_history_v1(new.id, new.scope_history);

      if old.scope_history is not null and (
        pg_catalog.jsonb_array_length(new.scope_history->'snapshots')
          <> pg_catalog.jsonb_array_length(old.scope_history->'snapshots') + 1
        or (
          select pg_catalog.jsonb_agg(snapshot.value order by snapshot.ordinality)
          from pg_catalog.jsonb_array_elements(new.scope_history->'snapshots')
            with ordinality as snapshot(value, ordinality)
          where snapshot.ordinality
            <= pg_catalog.jsonb_array_length(old.scope_history->'snapshots')
        ) is distinct from old.scope_history->'snapshots'
      ) then
        raise exception using errcode = '55000', message = 'REVIEW_SCOPE_APPEND_ONLY';
      end if;
    end if;
  end if;

  if tg_op = 'DELETE' then
    return old;
  end if;

  return new;
end;
$$;

ALTER FUNCTION "private"."review_scope_history_guard_v1"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_scope_history_guard_v1"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_scope_history_guard_v1"() TO "api_internal_executor";
