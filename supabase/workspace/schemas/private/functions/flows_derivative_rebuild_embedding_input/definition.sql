CREATE OR REPLACE FUNCTION "private"."flows_derivative_rebuild_embedding_input"("p_flow" "public"."flows") RETURNS "text"
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
  where request.target_table = 'flows'
    and request.target_id = p_flow.id
    and request.target_version = btrim(p_flow.version::text)
    and request.status = 'embedding_pending'
    and request.accepted_extracted_md_sha256 = proposal.extracted_md_sha256
  limit 1
$$;

ALTER FUNCTION "private"."flows_derivative_rebuild_embedding_input"("p_flow" "public"."flows") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."flows_derivative_rebuild_embedding_input"("p_flow" "public"."flows") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."flows_derivative_rebuild_embedding_input"("p_flow" "public"."flows") TO "api_internal_executor";
