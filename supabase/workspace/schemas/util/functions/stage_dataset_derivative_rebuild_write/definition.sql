CREATE OR REPLACE FUNCTION "util"."stage_dataset_derivative_rebuild_write"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_kind text := tg_argv[0];
  v_request util.dataset_derivative_rebuild_requests%rowtype;
  v_proposal util.dataset_derivative_rebuild_proposals%rowtype;
  v_permitted boolean := false;
begin
  if tg_table_schema <> 'public'
    or tg_table_name not in ('flows', 'processes') then
    raise exception using
      errcode = '22023',
      message = 'Unsupported derivative rebuild staging target';
  end if;

  select request.*
  into v_request
  from util.dataset_derivative_rebuild_requests as request
  where request.target_table = tg_table_name
    and request.target_id = old.id
    and request.target_version = btrim(old.version::text)
    and request.status not in ('completed', 'stale', 'failed')
  limit 1;

  if v_request.id is null then
    return new;
  end if;

  if not util.dataset_derivative_rebuild_primary_matches(v_request) then
    raise exception using
      errcode = '40001',
      message = 'Derivative rebuild primary fingerprint drifted';
  end if;

  select proposal.*
  into v_proposal
  from util.dataset_derivative_rebuild_permits as permit
  join util.dataset_derivative_rebuild_proposals as proposal
    on proposal.id = permit.proposal_id
   and proposal.request_id = permit.request_id
  where permit.request_id = v_request.id
    and permit.permit_kind = v_kind
    and permit.backend_pid = pg_catalog.pg_backend_pid()
    and permit.transaction_id = pg_catalog.txid_current()
    and proposal.proposal_kind = v_kind
    and (
      (v_kind = 'markdown' and proposal.status = 'accepted')
      or (v_kind = 'embedding' and proposal.status = 'captured')
    )
  limit 1;

  if v_proposal.id is not null then
    if v_kind = 'markdown' then
      v_permitted := new.extracted_md is not null
        and util.dataset_derivative_rebuild_sha256(new.extracted_md)
          = v_proposal.extracted_md_sha256;
    elsif v_kind = 'embedding' then
      v_permitted := new.embedding_ft is not null
        and new.embedding_ft_at is not null
        and util.dataset_derivative_rebuild_sha256(new.embedding_ft::text)
          = v_proposal.embedding_ft_sha256
        and util.dataset_derivative_rebuild_sha256(new.extracted_md)
          = v_proposal.source_extracted_md_sha256;
    end if;

    if not v_permitted then
      raise exception using
        errcode = '22000',
        message = 'Derivative rebuild permit does not match staged proposal';
    end if;
    return new;
  end if;

  if v_kind = 'markdown' then
    if new.extracted_md is null then
      raise exception using
        errcode = '23502',
        message = 'Fenced Markdown proposal must be non-null';
    end if;

    insert into util.dataset_derivative_rebuild_proposals (
      request_id,
      proposal_kind,
      extracted_md,
      extracted_md_sha256
    ) values (
      v_request.id,
      'markdown',
      new.extracted_md,
      util.dataset_derivative_rebuild_sha256(new.extracted_md)
    );
    new.extracted_md := old.extracted_md;
  elsif v_kind = 'embedding' then
    if new.embedding_ft is null or new.embedding_ft_at is null then
      raise exception using
        errcode = '23502',
        message = 'Fenced embedding proposal must carry vector and timestamp';
    end if;

    insert into util.dataset_derivative_rebuild_proposals (
      request_id,
      proposal_kind,
      embedding_ft,
      embedding_ft_sha256,
      embedding_ft_at,
      source_extracted_md_sha256
    ) values (
      v_request.id,
      'embedding',
      new.embedding_ft,
      util.dataset_derivative_rebuild_sha256(new.embedding_ft::text),
      new.embedding_ft_at,
      coalesce(
        v_request.accepted_extracted_md_sha256,
        util.dataset_derivative_rebuild_sha256(old.extracted_md)
      )
    );
    new.embedding_ft := old.embedding_ft;
    new.embedding_ft_at := old.embedding_ft_at;
  else
    raise exception using
      errcode = '22023',
      message = 'Unsupported derivative staging kind';
  end if;

  return new;
end;
$$;

ALTER FUNCTION "util"."stage_dataset_derivative_rebuild_write"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."stage_dataset_derivative_rebuild_write"() FROM PUBLIC;
