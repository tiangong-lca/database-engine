CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_artifact_v2_status_json"("p_write_set_id" "uuid") RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select jsonb_build_object(
    'contractVersion', write_set.contract_version,
    'writeSetId', write_set.id,
    'closureCheckId', write_set.closure_check_id,
    'workerJobId', write_set.worker_job_id,
    'requestId', write_set.request_id,
    'publicationMode', write_set.publication_mode,
    'reusedFromCheckId', write_set.reused_from_check_id,
    'status', write_set.status,
    'uploadEligible', write_set.status = 'staging',
    'writeToken', write_set.write_token,
    'expectedDescriptorCount', write_set.expected_descriptor_count,
    'registeredDescriptorCount', (
      select count(*)
      from private.lcia_scope_closure_artifact_write_set_items item
      where item.write_set_id = write_set.id
    ),
    'registeredBatchCount', (
      select count(*)
      from private.lcia_scope_closure_artifact_write_set_batches batch
      where batch.write_set_id = write_set.id
    ),
    'descriptorSetSha256', write_set.descriptor_set_sha256,
    'requiredPrimaryRoles', write_set.required_primary_roles,
    'stagingExpiresAt', write_set.staging_expires_at,
    'sealedAt', write_set.sealed_at,
    'finalizedAt', write_set.finalized_at,
    'cleanedAt', write_set.cleaned_at,
    'artifactMap', case
      when write_set.sealed_at is null then '{}'::jsonb
      else coalesce((
        select jsonb_object_agg(item.client_key, item.id order by item.client_key)
        from private.lcia_scope_closure_artifact_write_set_items item
        where item.write_set_id = write_set.id
      ), '{}'::jsonb)
    end,
    'batches', coalesce((
      select jsonb_agg(jsonb_build_object(
        'batchId', batch.batch_id,
        'itemCount', batch.item_count,
        'firstOrdinal', batch.first_ordinal,
        'lastOrdinal', batch.last_ordinal
      ) order by batch.first_ordinal, batch.batch_id)
      from private.lcia_scope_closure_artifact_write_set_batches batch
      where batch.write_set_id = write_set.id
    ), '[]'::jsonb)
  )
  from private.lcia_scope_closure_artifact_write_sets write_set
  where write_set.id = p_write_set_id
$$;

ALTER FUNCTION "private"."lcia_scope_closure_artifact_v2_status_json"("p_write_set_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_artifact_v2_status_json"("p_write_set_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_artifact_v2_status_json"("p_write_set_id" "uuid") TO "api_internal_executor";
