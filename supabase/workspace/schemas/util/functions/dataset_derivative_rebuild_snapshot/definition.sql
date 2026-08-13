CREATE OR REPLACE FUNCTION "util"."dataset_derivative_rebuild_snapshot"("p_flow" "public"."flows") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE
    SET "search_path" TO ''
    AS $$
declare
  v_json_sha256 text;
  v_json_ordered_sha256 text;
  v_extracted_md_sha256 text;
  v_embedding_ft_sha256 text;
  v_snapshot jsonb;
begin
  if p_flow.json is null
    or p_flow.json_ordered is null
    or p_flow.modified_at is null then
    raise exception using
      errcode = '22004',
      message = 'Derivative rebuild primary snapshot fields must be non-null';
  end if;

  v_json_sha256 := util.dataset_derivative_rebuild_sha256(
    p_flow.json::jsonb::text
  );
  v_json_ordered_sha256 := util.dataset_derivative_rebuild_sha256(
    p_flow.json_ordered::jsonb::text
  );
  v_extracted_md_sha256 := util.dataset_derivative_rebuild_sha256(
    p_flow.extracted_md
  );
  v_embedding_ft_sha256 := util.dataset_derivative_rebuild_sha256(
    p_flow.embedding_ft::text
  );

  v_snapshot := jsonb_build_object(
    'schema_version', 'dataset-derivative-snapshot.v1',
    'table', 'flows',
    'id', p_flow.id,
    'version', btrim(p_flow.version::text),
    'user_id', p_flow.user_id,
    'state_code', p_flow.state_code,
    'modified_at', p_flow.modified_at,
    'json_sha256', v_json_sha256,
    'json_ordered_sha256', v_json_ordered_sha256,
    'extracted_md_sha256', v_extracted_md_sha256,
    'embedding_ft_sha256', v_embedding_ft_sha256,
    'embedding_ft_at', p_flow.embedding_ft_at
  );

  return v_snapshot || jsonb_build_object(
    'snapshot_sha256',
    util.dataset_derivative_rebuild_sha256(v_snapshot::text)
  );
end;
$$;

ALTER FUNCTION "util"."dataset_derivative_rebuild_snapshot"("p_flow" "public"."flows") OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "util"."dataset_derivative_rebuild_snapshot"("p_process" "public"."processes") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE
    SET "search_path" TO ''
    AS $$
declare
  v_json_sha256 text;
  v_json_ordered_sha256 text;
  v_extracted_md_sha256 text;
  v_embedding_ft_sha256 text;
  v_snapshot jsonb;
begin
  if p_process.json is null
    or p_process.json_ordered is null
    or p_process.modified_at is null then
    raise exception using
      errcode = '22004',
      message = 'Derivative rebuild primary snapshot fields must be non-null';
  end if;

  v_json_sha256 := util.dataset_derivative_rebuild_sha256(
    p_process.json::jsonb::text
  );
  v_json_ordered_sha256 := util.dataset_derivative_rebuild_sha256(
    p_process.json_ordered::jsonb::text
  );
  v_extracted_md_sha256 := util.dataset_derivative_rebuild_sha256(
    p_process.extracted_md
  );
  v_embedding_ft_sha256 := util.dataset_derivative_rebuild_sha256(
    p_process.embedding_ft::text
  );

  v_snapshot := jsonb_build_object(
    'schema_version', 'dataset-derivative-snapshot.v1',
    'table', 'processes',
    'id', p_process.id,
    'version', btrim(p_process.version::text),
    'user_id', p_process.user_id,
    'state_code', p_process.state_code,
    'modified_at', p_process.modified_at,
    'json_sha256', v_json_sha256,
    'json_ordered_sha256', v_json_ordered_sha256,
    'extracted_md_sha256', v_extracted_md_sha256,
    'embedding_ft_sha256', v_embedding_ft_sha256,
    'embedding_ft_at', p_process.embedding_ft_at
  );

  return v_snapshot || jsonb_build_object(
    'snapshot_sha256',
    util.dataset_derivative_rebuild_sha256(v_snapshot::text)
  );
end;
$$;

ALTER FUNCTION "util"."dataset_derivative_rebuild_snapshot"("p_process" "public"."processes") OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "util"."dataset_derivative_rebuild_snapshot"("p_table" "text", "p_id" "uuid", "p_version" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_flow public.flows%rowtype;
  v_process public.processes%rowtype;
begin
  if p_table = 'flows' then
    select flow.*
    into v_flow
    from public.flows as flow
    where flow.id = p_id
      and btrim(flow.version::text) = p_version;
    if v_flow.id is null then
      return null;
    end if;
    return util.dataset_derivative_rebuild_snapshot(v_flow);
  elsif p_table = 'processes' then
    select process.*
    into v_process
    from public.processes as process
    where process.id = p_id
      and btrim(process.version::text) = p_version;
    if v_process.id is null then
      return null;
    end if;
    return util.dataset_derivative_rebuild_snapshot(v_process);
  end if;

  raise exception using
    errcode = '22023',
    message = 'Derivative rebuild target table must be flows or processes';
end;
$$;

ALTER FUNCTION "util"."dataset_derivative_rebuild_snapshot"("p_table" "text", "p_id" "uuid", "p_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_derivative_rebuild_snapshot"("p_flow" "public"."flows") FROM PUBLIC;

REVOKE ALL ON FUNCTION "util"."dataset_derivative_rebuild_snapshot"("p_process" "public"."processes") FROM PUBLIC;

REVOKE ALL ON FUNCTION "util"."dataset_derivative_rebuild_snapshot"("p_table" "text", "p_id" "uuid", "p_version" "text") FROM PUBLIC;
