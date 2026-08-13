CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_artifact_v2_descriptor_set"("p_write_set_id" "uuid") RETURNS "jsonb"
    LANGUAGE "sql" STABLE
    SET "search_path" TO ''
    AS $$
  select jsonb_build_object(
    'contractVersion', write_set.contract_version,
    'descriptors', coalesce((
      select jsonb_agg(
        jsonb_build_object(
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
        )
        order by item.ordinal
      )
      from private.lcia_scope_closure_artifact_write_set_items item
      where item.write_set_id = write_set.id
    ), '[]'::jsonb)
  )
  from private.lcia_scope_closure_artifact_write_sets write_set
  where write_set.id = p_write_set_id
$$;

ALTER FUNCTION "private"."lcia_scope_closure_artifact_v2_descriptor_set"("p_write_set_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_artifact_v2_descriptor_set"("p_write_set_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_artifact_v2_descriptor_set"("p_write_set_id" "uuid") TO "api_internal_executor";
