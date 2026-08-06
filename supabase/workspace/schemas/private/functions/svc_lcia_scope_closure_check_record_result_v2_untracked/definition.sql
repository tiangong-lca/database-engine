CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_check_record_result_v2_untracked"("p_closure_check_id" "uuid", "p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_scan_completeness" "text", "p_effective_scope_manifest" "jsonb", "p_evidence" "jsonb", "p_result_summary" "jsonb" DEFAULT '{}'::"jsonb", "p_issues" "jsonb" DEFAULT '[]'::"jsonb", "p_blocker_codes" "text"[] DEFAULT '{}'::"text"[], "p_report_artifact_id" "uuid" DEFAULT NULL::"uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_check private.lcia_scope_closure_checks%rowtype; v_job private.worker_jobs%rowtype; v_status text := lower(trim(p_status));
  v_effective_hash text; v_certificate_bindings jsonb; v_certificate_hash text; v_worker_status text; v_issue jsonb; v_worker_result jsonb;
  v_closure_issue private.lcia_scope_closure_issues%rowtype; v_occurrence jsonb; v_root jsonb;
  v_artifact private.worker_job_artifacts%rowtype; v_report_artifact_manifest_hash text; v_worker_record jsonb;
begin
  if not coalesce(util.is_service_request(), false) then return api.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  if v_status not in ('passed','blocked','failed') or p_scan_completeness not in ('complete','incomplete','unknown')
     or jsonb_typeof(coalesce(p_effective_scope_manifest,'null'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(p_evidence,'null'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(p_result_summary,'{}'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(p_issues,'[]'::jsonb)) <> 'array' then return api.lcia_scope_closure_error('invalid_closure_result',400,'Invalid closure result payload'); end if;
  select * into v_check from private.lcia_scope_closure_checks where id = p_closure_check_id for update;
  if v_check.id is null or v_check.worker_job_id <> p_job_id then return api.lcia_scope_closure_error('closure_check_not_found',404,'Closure check not found'); end if;
  if v_check.status not in ('queued','running') then return api.lcia_scope_closure_error('closure_check_already_terminal',409,'Closure check is already terminal'); end if;
  select * into v_job from private.worker_jobs where id = p_job_id for update;
  if v_job.status <> 'running' or v_job.lease_token is distinct from p_lease_token or v_job.lease_expires_at < now() then return api.lcia_scope_closure_error('worker_job_lease_invalid',409,'Worker job lease is no longer valid'); end if;
  select * into v_artifact from private.worker_job_artifacts where id = p_report_artifact_id and job_id = v_job.id;
  if p_report_artifact_id is not null and v_artifact.id is null then return api.lcia_scope_closure_error('closure_report_unavailable',409,'Report artifact does not belong to the closure job'); end if;
  if v_status in ('passed','blocked') and p_report_artifact_id is null then return api.lcia_scope_closure_error('closure_report_unavailable',409,'Completed closure checks require a report artifact'); end if;
  if v_status = 'passed' and (p_scan_completeness <> 'complete' or exists (
    select 1 from jsonb_array_elements(p_issues) issue(value)
    where coalesce((issue.value->>'blocking')::boolean, false)
  )) then return api.lcia_scope_closure_error('closure_check_incomplete',409,'Passed closure checks must be complete and free of blocking issues'); end if;
  if v_status = 'blocked' and (cardinality(coalesce(p_blocker_codes,'{}'::text[])) = 0 or jsonb_array_length(p_issues) = 0) then return api.lcia_scope_closure_error('closure_blocker_details_required',409,'Blocked closure checks require issues and blocker codes'); end if;
  if v_status = 'passed' and not (p_evidence ?& array['schemaVersion','sourceFingerprint','resolutionMapHash','closureBundleHash','snapshotId','snapshotHash','reportArtifactManifestHash','evidenceHash']) then return api.lcia_scope_closure_error('closure_evidence_unavailable',409,'Passed closure checks require complete evidence'); end if;
  if p_report_artifact_id is not null then
    v_report_artifact_manifest_hash := private.lcia_scope_closure_sha256(jsonb_build_object('artifactId',v_artifact.id,'bucket',v_artifact.storage_bucket,'objectPath',v_artifact.storage_path,'mediaType',v_artifact.content_type,'byteSize',v_artifact.byte_size,'checksumSha256',v_artifact.checksum_sha256));
  end if;
  if v_status = 'passed' and coalesce(p_evidence->>'reportArtifactManifestHash','') <> coalesce(v_report_artifact_manifest_hash,'') then return api.lcia_scope_closure_error('closure_report_hash_mismatch',409,'Report artifact manifest hash does not match persisted artifact metadata'); end if;
  v_effective_hash := private.lcia_scope_closure_sha256(p_effective_scope_manifest);
  if v_status = 'passed' then
    v_certificate_bindings := jsonb_build_object('certificateSchemaVersion','lcia.scope-closure-certificate.v1','requestedScopeHash',v_check.requested_scope_hash,'scopeHash',v_effective_hash,'policyFingerprint',v_check.policy_fingerprint,'dataSnapshotToken',v_check.data_snapshot_token,'validatorScannerFingerprint',v_check.expected_validator_scanner_fingerprint,'sourceFingerprint',p_evidence->>'sourceFingerprint','resolutionMapHash',p_evidence->>'resolutionMapHash','closureBundleHash',p_evidence->>'closureBundleHash','snapshotId',p_evidence->>'snapshotId','snapshotHash',p_evidence->>'snapshotHash','reportArtifactManifestHash',p_evidence->>'reportArtifactManifestHash','evidenceHash',p_evidence->>'evidenceHash');
    v_certificate_hash := private.lcia_scope_closure_sha256(v_certificate_bindings);
  end if;
  delete from private.lcia_scope_closure_issues where closure_check_id = v_check.id;
  for v_issue in select value from jsonb_array_elements(p_issues) loop
    insert into private.lcia_scope_closure_issues(closure_check_id,issue_key,severity,blocking,issue_code,source_dataset_type,source_dataset_id,source_dataset_version,json_path,reference_role,requested_target_type,requested_target_id,requested_target_version,message,suggested_action,occurrence_count,affected_root_count,details)
    values(v_check.id,coalesce(nullif(trim(v_issue->>'issueKey'),''),private.lcia_scope_closure_sha256(v_issue)),coalesce(v_issue->>'severity','blocker'),coalesce((v_issue->>'blocking')::boolean,false),coalesce(nullif(trim(v_issue->>'issueCode'),''),'closure_issue'),nullif(v_issue->>'sourceDatasetType',''),nullif(v_issue->>'sourceDatasetId','')::uuid,nullif(v_issue->>'sourceDatasetVersion',''),nullif(v_issue->>'jsonPath',''),nullif(v_issue->>'referenceRole',''),nullif(v_issue->>'requestedTargetType',''),nullif(v_issue->>'requestedTargetId','')::uuid,nullif(v_issue->>'requestedTargetVersion',''),coalesce(nullif(v_issue->>'message',''),'Closure validation issue'),nullif(v_issue->>'suggestedAction',''),greatest(1,coalesce((v_issue->>'occurrenceCount')::integer,1)),greatest(0,coalesce((v_issue->>'affectedRootCount')::integer,0)),coalesce(v_issue->'details','{}'::jsonb)) returning * into v_closure_issue;
    for v_occurrence in select value from jsonb_array_elements(coalesce(v_issue->'occurrences','[]'::jsonb)) loop
      insert into private.lcia_scope_closure_issue_occurrences(closure_issue_id,occurrence_key,source_dataset_type,source_dataset_id,source_dataset_version,json_path,reference_role,details)
      values(v_closure_issue.id,coalesce(nullif(v_occurrence->>'occurrenceKey',''),private.lcia_scope_closure_sha256(v_occurrence)),nullif(v_occurrence->>'sourceDatasetType',''),nullif(v_occurrence->>'sourceDatasetId','')::uuid,nullif(v_occurrence->>'sourceDatasetVersion',''),nullif(v_occurrence->>'jsonPath',''),nullif(v_occurrence->>'referenceRole',''),coalesce(v_occurrence->'details','{}'::jsonb));
    end loop;
    for v_root in select value from jsonb_array_elements(coalesce(v_issue->'affectedRoots','[]'::jsonb)) loop
      insert into private.lcia_scope_closure_issue_roots(closure_issue_id,root_dataset_type,root_dataset_id,root_dataset_version,impact_role,witness_path)
      values(v_closure_issue.id,coalesce(nullif(v_root->>'datasetType',''),'process'),(v_root->>'id')::uuid,coalesce(nullif(v_root->>'version',''),'00.00.000'),coalesce(nullif(v_root->>'impactRole',''),'root'),coalesce(v_root->'witnessPath','[]'::jsonb));
    end loop;
  end loop;
  update private.lcia_scope_closure_checks set status=v_status,scan_completeness=p_scan_completeness,effective_scope_manifest=p_effective_scope_manifest,effective_scope_hash=v_effective_hash,certificate_schema_version=case when v_status='passed' then 'lcia.scope-closure-certificate.v1' else null end,certificate_status=case when v_status='passed' then 'valid' else 'unavailable' end,certificate_hash=v_certificate_hash,source_fingerprint=nullif(p_evidence->>'sourceFingerprint',''),resolution_map_hash=nullif(p_evidence->>'resolutionMapHash',''),closure_bundle_hash=nullif(p_evidence->>'closureBundleHash',''),snapshot_id=nullif(p_evidence->>'snapshotId','')::uuid,snapshot_hash=nullif(p_evidence->>'snapshotHash',''),report_artifact_manifest_hash=nullif(p_evidence->>'reportArtifactManifestHash',''),evidence_hash=nullif(p_evidence->>'evidenceHash',''),result_summary=p_result_summary,blocker_codes=coalesce(p_blocker_codes,'{}'::text[]),report_artifact_id=p_report_artifact_id,updated_at=now(),finished_at=now() where id=v_check.id returning * into v_check;
  v_worker_status := case v_status when 'passed' then 'completed' else v_status end;
  v_worker_result := jsonb_strip_nulls(jsonb_build_object('closureCheckId',v_check.id,'status',v_status,'scanCompleteness',p_scan_completeness,'certificateStatus',v_check.certificate_status,'effectiveScopeHash',v_check.effective_scope_hash,'certificateHash',v_check.certificate_hash));
  select private.worker_record_job_result(v_job.id,p_lease_token,v_worker_status,v_worker_result,'lcia.scope_closure_check.result.v1',null,jsonb_build_object('progressCounters',coalesce(p_result_summary->'progressCounters','{}'::jsonb)),case when v_status='failed' then coalesce(p_result_summary->>'errorCode','closure_check_failed') else null end,case when v_status='failed' then 'Scope closure check failed' else null end,null,case when v_status='blocked' then coalesce(p_blocker_codes,'{}'::text[]) else null end,case when v_status='blocked' then 'operator' else null end,case when v_status='failed' then true else false end) into v_worker_record;
  if coalesce((v_worker_record->>'ok')::boolean,false) is not true then raise exception using errcode='P0001',message='worker_job_result_rejected'; end if;
  return jsonb_build_object('ok',true,'data',jsonb_build_object('closureCheckId',v_check.id,'certificateHash',v_certificate_hash,'workerJobId',v_job.id));
exception when invalid_text_representation then return api.lcia_scope_closure_error('invalid_closure_result',400,'Closure result contains invalid identity values');
end;
$$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_check_record_result_v2_untracked"("p_closure_check_id" "uuid", "p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_scan_completeness" "text", "p_effective_scope_manifest" "jsonb", "p_evidence" "jsonb", "p_result_summary" "jsonb", "p_issues" "jsonb", "p_blocker_codes" "text"[], "p_report_artifact_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_check_record_result_v2_untracked"("p_closure_check_id" "uuid", "p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_scan_completeness" "text", "p_effective_scope_manifest" "jsonb", "p_evidence" "jsonb", "p_result_summary" "jsonb", "p_issues" "jsonb", "p_blocker_codes" "text"[], "p_report_artifact_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_check_record_result_v2_untracked"("p_closure_check_id" "uuid", "p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_scan_completeness" "text", "p_effective_scope_manifest" "jsonb", "p_evidence" "jsonb", "p_result_summary" "jsonb", "p_issues" "jsonb", "p_blocker_codes" "text"[], "p_report_artifact_id" "uuid") TO "api_internal_executor";
