CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_refresh_candidate_document_hash"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_document jsonb;
  v_dataset_id uuid;
  v_role text;
  v_is_eligible boolean;
begin
  if tg_op <> 'INSERT' then
    delete from private.lcia_scope_closure_candidate_document_hashes
    where dataset_type = tg_table_name
      and source_locator_id = old.id
      and dataset_version = btrim(old.version::text);
  end if;

  if tg_op = 'DELETE' then
    return old;
  end if;

  if tg_table_name = 'lciamethods' then
    -- LCIA methods are the separately reviewed 25-method static bundle. Their
    -- authoring lifecycle state_code remains 0 in production and is not the
    -- candidate-public-data eligibility predicate used by other datasets.
    v_document := coalesce(new.json, new.json_ordered::jsonb);
    v_is_eligible := v_document is not null;
    v_dataset_id := private.lcia_scope_closure_lcia_method_identity(
      new.id,
      btrim(new.version::text),
      v_document
    );
    v_is_eligible := v_is_eligible and exists (
      select 1
      from private.lcia_scope_closure_reviewed_lcia_methods reviewed
      where reviewed.method_id = v_dataset_id
        and reviewed.method_version = btrim(new.version::text)
        and reviewed.artifact_locator_id = new.id
    );
    v_role := 'support';
  else
    v_document := new.json_ordered::jsonb;
    v_is_eligible :=
      new.state_code between 100 and 199 and v_document is not null;
    v_dataset_id := new.id;
    v_role := case
      when tg_table_name = 'processes' then 'unit_process'
      else 'support'
    end;
  end if;

  if v_is_eligible then
    insert into private.lcia_scope_closure_candidate_document_hashes(
      dataset_type,
      dataset_id,
      dataset_version,
      source_locator_id,
      role,
      canonical_content_hash,
      source_modified_at,
      refreshed_at
    ) values (
      tg_table_name,
      v_dataset_id,
      btrim(new.version::text),
      new.id,
      v_role,
      private.lcia_scope_closure_worker_canonical_sha256(v_document),
      new.modified_at,
      now()
    )
    on conflict (dataset_type, dataset_id, dataset_version)
    do update set
      source_locator_id = excluded.source_locator_id,
      role = excluded.role,
      canonical_content_hash = excluded.canonical_content_hash,
      source_modified_at = excluded.source_modified_at,
      refreshed_at = excluded.refreshed_at;
  end if;

  return new;
end;
$$;

ALTER FUNCTION "private"."lcia_scope_closure_refresh_candidate_document_hash"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_refresh_candidate_document_hash"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_refresh_candidate_document_hash"() TO "api_internal_executor";
