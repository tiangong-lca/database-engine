CREATE OR REPLACE FUNCTION "private"."review_resolve_current_reference_targets_v1"("p_root_review_ids" "uuid"[]) RETURNS TABLE("root_review_id" "uuid", "target_table" "text", "data_id" "uuid", "data_version" "text", "revision_checksum" "text", "provenance" "jsonb")
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
  with recursive requested_roots as materialized (
    select review_row.*
    from private.reviews as review_row
    where review_row.review_kind = 'root'
      and review_row.id = any(coalesce(p_root_review_ids, array[]::uuid[]))
  ),
  seed_targets as (
    select
      root_review.id as root_review_id,
      root_review.target_table,
      root_review.data_id,
      pg_catalog.btrim(root_review.data_version::text) as data_version,
      true as is_root,
      api.cmd_review_get_dataset_row(
        root_review.target_table,
        root_review.data_id,
        pg_catalog.btrim(root_review.data_version::text),
        false
      ) as dataset_row
    from requested_roots as root_review

    union

    select
      root_review.id,
      api.cmd_review_ref_type_to_table(ref.ref_type),
      ref.ref_object_id,
      ref.ref_version,
      false,
      api.cmd_review_get_dataset_row(
        api.cmd_review_ref_type_to_table(ref.ref_type),
        ref.ref_object_id,
        ref.ref_version,
        false
      )
    from requested_roots as root_review
    join private.comments as comment_row
      on comment_row.review_id = root_review.id
      and comment_row.state_code <> -2
    cross join lateral api.cmd_review_extract_refs(
      coalesce(comment_row.json::jsonb, '{}'::jsonb)
    ) as ref
    where api.cmd_review_ref_type_to_table(ref.ref_type) is not null
  ),
  closure (
    root_review_id,
    target_table,
    data_id,
    data_version,
    is_root,
    dataset_row
  ) as (
    select
      seed.root_review_id,
      seed.target_table,
      seed.data_id,
      seed.data_version,
      seed.is_root,
      seed.dataset_row
    from seed_targets as seed
    where seed.dataset_row is not null

    union

    select
      current_target.root_review_id,
      neighbour.target_table,
      neighbour.data_id,
      neighbour.data_version,
      false,
      dataset.dataset_row
    from closure as current_target
    cross join lateral (
      select
        api.cmd_review_ref_type_to_table(ref.ref_type) as target_table,
        ref.ref_object_id as data_id,
        ref.ref_version as data_version
      from (
        select * from api.cmd_review_extract_refs(
          coalesce(current_target.dataset_row->'json_ordered', '{}'::jsonb)
        )
        union
        select * from api.cmd_review_extract_refs(
          coalesce(current_target.dataset_row->'json', '{}'::jsonb)
        )
        union
        select * from api.cmd_review_extract_refs(
          coalesce(current_target.dataset_row->'json_tg', '{}'::jsonb)
        )
      ) as ref
      where (
          current_target.is_root
          or coalesce((current_target.dataset_row->>'state_code')::integer, 0) < 100
        )
        and api.cmd_review_ref_type_to_table(ref.ref_type) is not null

      union

      select
        'lifecyclemodels',
        current_target.data_id,
        current_target.data_version
      where current_target.target_table = 'processes'
        and not current_target.is_root

      union

      select
        'processes',
        current_target.data_id,
        current_target.data_version
      where current_target.target_table = 'lifecyclemodels'
        and current_target.is_root

      union

      select
        'processes',
        (submodel.value->>'id')::uuid,
        coalesce(
          nullif(submodel.value->>'version', ''),
          current_target.data_version
        )
      from pg_catalog.jsonb_array_elements(
        case
          when pg_catalog.jsonb_typeof(
            current_target.dataset_row->'json_tg'->'submodels'
          ) = 'array'
            then current_target.dataset_row->'json_tg'->'submodels'
          else '[]'::jsonb
        end
      ) as submodel(value)
      where current_target.target_table = 'lifecyclemodels'
        and (
          current_target.is_root
          or coalesce((current_target.dataset_row->>'state_code')::integer, 0) < 100
        )
        and pg_catalog.lower(coalesce(submodel.value->>'type', '')) = 'secondary'
        and coalesce(submodel.value->>'id', '')
          ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ) as neighbour
    cross join lateral (
      select api.cmd_review_get_dataset_row(
        neighbour.target_table,
        neighbour.data_id,
        neighbour.data_version,
        false
      ) as dataset_row
    ) as dataset
    where dataset.dataset_row is not null
      and not (
        neighbour.target_table = current_target.target_table
        and neighbour.data_id = current_target.data_id
        and neighbour.data_version = current_target.data_version
      )
  )
  select
    current_target.root_review_id,
    current_target.target_table,
    current_target.data_id,
    current_target.data_version,
    private.review_revision_fingerprint_v1(
      current_target.target_table,
      current_target.dataset_row
    ),
    pg_catalog.jsonb_build_array('current_json_or_comment')
  from closure as current_target
  join requested_roots as root_review
    on root_review.id = current_target.root_review_id
  where not current_target.is_root
    and not (
      current_target.target_table = root_review.target_table
      and current_target.data_id = root_review.data_id
      and current_target.data_version = pg_catalog.btrim(root_review.data_version::text)
    )
  group by
    current_target.root_review_id,
    current_target.target_table,
    current_target.data_id,
    current_target.data_version,
    current_target.dataset_row
$_$;

ALTER FUNCTION "private"."review_resolve_current_reference_targets_v1"("p_root_review_ids" "uuid"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_resolve_current_reference_targets_v1"("p_root_review_ids" "uuid"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_resolve_current_reference_targets_v1"("p_root_review_ids" "uuid"[]) TO "api_internal_executor";
