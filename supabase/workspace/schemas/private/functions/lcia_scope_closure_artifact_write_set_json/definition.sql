CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_artifact_write_set_json"("p_write_set_id" "uuid") RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select jsonb_build_object(
    'writeSetId', write_set.id,
    'closureCheckId', write_set.closure_check_id,
    'workerJobId', write_set.worker_job_id,
    'requestedBy', write_set.requested_by,
    'publicationMode', write_set.publication_mode,
    'reusedFromCheckId', write_set.reused_from_check_id,
    'status', write_set.status,
    'writeToken', write_set.write_token,
    'stagingExpiresAt', write_set.staging_expires_at,
    'reconcileToken', write_set.reconcile_token,
    'reconcileLeaseExpiresAt', write_set.reconcile_expires_at,
    'failureReason', write_set.failure_reason,
    'createdAt', write_set.created_at,
    'updatedAt', write_set.updated_at,
    'finalizedAt', write_set.finalized_at,
    'cleanedAt', write_set.cleaned_at,
    'artifactMap', coalesce((
      select jsonb_object_agg(item.client_key, item.id order by item.client_key)
      from private.lcia_scope_closure_artifact_write_set_items item
      where item.write_set_id = write_set.id
    ), '{}'::jsonb),
    'items', coalesce((
      select jsonb_agg(jsonb_build_object(
        'artifactId', item.id,
        'ordinal', item.ordinal,
        'clientKey', item.client_key,
        'artifactType', item.artifact_type,
        'artifactRole', item.artifact_role,
        'bucket', item.storage_bucket,
        'objectPath', item.storage_path,
        'mediaType', item.content_type,
        'size', item.byte_size,
        'checksumSha256', item.checksum_sha256,
        'metadata', item.metadata
      ) order by item.ordinal)
      from private.lcia_scope_closure_artifact_write_set_items item
      where item.write_set_id = write_set.id
    ), '[]'::jsonb)
  )
  from private.lcia_scope_closure_artifact_write_sets write_set
  where write_set.id = p_write_set_id
$$;

ALTER FUNCTION "private"."lcia_scope_closure_artifact_write_set_json"("p_write_set_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_artifact_write_set_json"("p_write_set_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_artifact_write_set_json"("p_write_set_id" "uuid") TO "api_internal_executor";
