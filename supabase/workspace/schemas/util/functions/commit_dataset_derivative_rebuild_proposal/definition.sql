CREATE OR REPLACE FUNCTION "util"."commit_dataset_derivative_rebuild_proposal"("p_request_id" "uuid", "p_markdown_proposal_id" bigint, "p_embedding_proposal_id" bigint) RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_request util.dataset_derivative_rebuild_requests%rowtype;
  v_markdown util.dataset_derivative_rebuild_proposals%rowtype;
  v_embedding util.dataset_derivative_rebuild_proposals%rowtype;
  v_flow public.flows%rowtype;
  v_process public.processes%rowtype;
  v_primary_matches boolean := false;
begin
  select request.*
  into v_request
  from util.dataset_derivative_rebuild_requests as request
  where request.id = p_request_id
    and request.status not in ('completed', 'stale', 'failed')
  for update;

  if v_request.id is null then
    raise exception using
      errcode = 'P0002',
      message = 'Active derivative rebuild request not found';
  end if;

  select proposal.*
  into v_markdown
  from util.dataset_derivative_rebuild_proposals as proposal
  where proposal.id = p_markdown_proposal_id
    and proposal.request_id = p_request_id
    and proposal.proposal_kind = 'markdown'
    and proposal.status = 'accepted'
  for update;

  select proposal.*
  into v_embedding
  from util.dataset_derivative_rebuild_proposals as proposal
  where proposal.id = p_embedding_proposal_id
    and proposal.request_id = p_request_id
    and proposal.proposal_kind = 'embedding'
    and proposal.status = 'captured'
  for update;

  if v_markdown.id is null or v_embedding.id is null then
    raise exception using
      errcode = 'P0002',
      message = 'Accepted Markdown and captured embedding proposals are required';
  end if;

  if v_request.target_table = 'flows' then
    select flow.*
    into v_flow
    from public.flows as flow
    where flow.id = v_request.target_id
      and btrim(flow.version::text) = v_request.target_version
    for update;
    v_primary_matches := util.dataset_derivative_rebuild_primary_matches(
      v_request,
      v_flow
    );
  elsif v_request.target_table = 'processes' then
    select process.*
    into v_process
    from public.processes as process
    where process.id = v_request.target_id
      and btrim(process.version::text) = v_request.target_version
    for update;
    v_primary_matches := util.dataset_derivative_rebuild_primary_matches(
      v_request,
      v_process
    );
  end if;

  if not v_primary_matches then
    raise exception using
      errcode = '40001',
      message = 'Derivative rebuild primary changed before proposal commit';
  end if;

  if v_request.markdown_proposal_id is distinct from v_markdown.id
    or v_request.accepted_extracted_md_sha256
      is distinct from v_markdown.extracted_md_sha256
    or v_embedding.source_extracted_md_sha256
      is distinct from v_markdown.extracted_md_sha256 then
    raise exception using
      errcode = '40001',
      message = 'Embedding proposal is not bound to the accepted Markdown';
  end if;

  if v_embedding.embedding_ft_at
    <= coalesce(
      v_request.before_embedding_ft_at,
      '-infinity'::timestamp with time zone
    ) then
    raise exception using
      errcode = '22023',
      message = 'Embedding proposal is not newer than the frozen derivative baseline';
  end if;

  insert into util.dataset_derivative_rebuild_permits (
    request_id,
    proposal_id,
    permit_kind,
    backend_pid,
    transaction_id
  ) values
    (
      v_request.id,
      v_markdown.id,
      'markdown',
      pg_catalog.pg_backend_pid(),
      pg_catalog.txid_current()
    ),
    (
      v_request.id,
      v_embedding.id,
      'embedding',
      pg_catalog.pg_backend_pid(),
      pg_catalog.txid_current()
    );

  if v_request.target_table = 'flows' then
    update public.flows as flow
    set
      extracted_md = v_markdown.extracted_md,
      embedding_ft = v_embedding.embedding_ft,
      embedding_ft_at = v_embedding.embedding_ft_at
    where flow.id = v_request.target_id
      and btrim(flow.version::text) = v_request.target_version;
  else
    update public.processes as process
    set
      extracted_md = v_markdown.extracted_md,
      embedding_ft = v_embedding.embedding_ft,
      embedding_ft_at = v_embedding.embedding_ft_at
    where process.id = v_request.target_id
      and btrim(process.version::text) = v_request.target_version;
  end if;

  update util.dataset_derivative_rebuild_requests
  set
    embedding_proposal_id = v_embedding.id,
    updated_at = pg_catalog.clock_timestamp()
  where id = v_request.id;

  delete from util.dataset_derivative_rebuild_permits
  where request_id = v_request.id
    and proposal_id in (v_markdown.id, v_embedding.id);

  update util.dataset_derivative_rebuild_proposals
  set
    status = 'committed',
    committed_at = pg_catalog.clock_timestamp()
  where id in (v_markdown.id, v_embedding.id);
end;
$$;

ALTER FUNCTION "util"."commit_dataset_derivative_rebuild_proposal"("p_request_id" "uuid", "p_markdown_proposal_id" bigint, "p_embedding_proposal_id" bigint) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."commit_dataset_derivative_rebuild_proposal"("p_request_id" "uuid", "p_markdown_proposal_id" bigint, "p_embedding_proposal_id" bigint) FROM PUBLIC;
