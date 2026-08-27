CREATE OR REPLACE FUNCTION "private"."portal_lcia_decorate_item_page_v1"("p_page" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" STABLE
    SET "search_path" TO ''
    AS $$
  select case
    when jsonb_typeof(p_page) <> 'object'
      or jsonb_typeof(p_page -> 'items') <> 'array'
      then null
    else jsonb_set(
      p_page,
      '{items}',
      coalesce((
        select jsonb_agg(
          item.value || jsonb_build_object(
            'capabilities',
            jsonb_set(
              item.value -> 'capabilities',
              '{lciaVisible}',
              to_jsonb(evidence.publication is not null),
              false
            )
          )
          order by item.ordinality
        )
        from jsonb_array_elements(p_page -> 'items')
          with ordinality as item(value, ordinality)
        cross join lateral (
          select case
            when item.value #>> '{key,kind}' = 'process'
              and item.value ->> 'accessLevel' = 'open'
              then private.portal_current_lcia_publication_for_process_v1(
                (item.value #>> '{key,id}')::uuid,
                item.value #>> '{key,version}'
              )
            else null
          end as publication
        ) as evidence
      ), '[]'::jsonb),
      false
    )
  end
$$;

ALTER FUNCTION "private"."portal_lcia_decorate_item_page_v1"("p_page" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_lcia_decorate_item_page_v1"("p_page" "jsonb") FROM PUBLIC;
