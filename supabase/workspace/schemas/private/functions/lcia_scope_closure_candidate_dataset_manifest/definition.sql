CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_candidate_dataset_manifest"() RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select coalesce(jsonb_agg(
    jsonb_build_object(
      'datasetType', dataset_type,
      'datasetId', dataset_id,
      'datasetVersion', dataset_version,
      'role', role,
      -- Worker v2 requires all three hashes.  Candidate snapshots define each
      -- compatibility field over the exact frozen full document.
      'versionSignificantHash', canonical_content_hash,
      'semanticHash', canonical_content_hash,
      'canonicalContentHash', canonical_content_hash
    )
    order by dataset_type, dataset_id, dataset_version, role
  ), '[]'::jsonb)
  from private.lcia_scope_closure_candidate_document_hashes
$$;

ALTER FUNCTION "private"."lcia_scope_closure_candidate_dataset_manifest"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_candidate_dataset_manifest"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_candidate_dataset_manifest"() TO "api_internal_executor";
