CREATE OR REPLACE FUNCTION "private"."portal_lcia_decorate_dataset_v1"("p_envelope" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" STABLE
    SET "search_path" TO ''
    AS $$
  select case
    when p_envelope is null then null
    when jsonb_typeof(p_envelope) <> 'object'
      or jsonb_typeof(p_envelope -> 'key') <> 'object'
      or jsonb_typeof(p_envelope -> 'capabilities') <> 'object'
      then null
    else jsonb_set(
      jsonb_set(
        p_envelope,
        '{capabilities,lciaVisible}',
        to_jsonb(evidence.publication is not null),
        false
      ),
      '{publication}',
      coalesce(evidence.publication, 'null'::jsonb),
      false
    )
  end
  from lateral (
    select case
      when p_envelope #>> '{key,kind}' = 'process'
        and p_envelope ->> 'accessLevel' = 'open'
        then private.portal_current_lcia_publication_for_process_v1(
          (p_envelope #>> '{key,id}')::uuid,
          p_envelope #>> '{key,version}'
        )
      else null
    end as publication
  ) as evidence
$$;

ALTER FUNCTION "private"."portal_lcia_decorate_dataset_v1"("p_envelope" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_lcia_decorate_dataset_v1"("p_envelope" "jsonb") FROM PUBLIC;
