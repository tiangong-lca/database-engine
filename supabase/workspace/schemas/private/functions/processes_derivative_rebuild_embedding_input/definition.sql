CREATE OR REPLACE FUNCTION "private"."processes_derivative_rebuild_embedding_input"("p_process" "public"."processes") RETURNS "text"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select proposal.extracted_md
  from util.dataset_derivative_rebuild_requests as request
  join util.dataset_derivative_rebuild_proposals as proposal
    on proposal.id = request.markdown_proposal_id
   and proposal.request_id = request.id
   and proposal.proposal_kind = 'markdown'
   and proposal.status = 'accepted'
  where request.target_table = 'processes'
    and request.target_id = p_process.id
    and request.target_version = btrim(p_process.version::text)
    and request.status = 'embedding_pending'
    and request.accepted_extracted_md_sha256 = proposal.extracted_md_sha256
  limit 1
$$;

ALTER FUNCTION "private"."processes_derivative_rebuild_embedding_input"("p_process" "public"."processes") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."processes_derivative_rebuild_embedding_input"("p_process" "public"."processes") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."processes_derivative_rebuild_embedding_input"("p_process" "public"."processes") TO "api_internal_executor";
