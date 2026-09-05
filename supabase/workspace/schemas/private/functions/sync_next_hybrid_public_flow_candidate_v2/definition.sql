CREATE OR REPLACE FUNCTION "private"."sync_next_hybrid_public_flow_candidate_v2"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "row_security" TO 'on'
    AS $$
begin
  if tg_op = 'DELETE' then
    delete from private.next_hybrid_public_candidates_v2 as candidate
    where candidate.dataset_kind = 'flow'
      and candidate.id = old.id
      and candidate.version = old.version::text;
    return null;
  end if;

  if tg_op = 'UPDATE'
     and (old.id, old.version::text) is distinct from (new.id, new.version::text) then
    delete from private.next_hybrid_public_candidates_v2 as candidate
    where candidate.dataset_kind = 'flow'
      and candidate.id = old.id
      and candidate.version = old.version::text;
  end if;

  if new.state_code in (100, 200) and new.embedding_ft is not null then
    insert into private.next_hybrid_public_candidates_v2(
      dataset_kind, id, version, state_code, team_id, dataset_type,
      is_emission, classification_codes, elementary_codes, source_modified_at
    ) values (
      'flow', new.id, new.version::text, new.state_code, new.team_id,
      new.json #>>
        '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}',
      new.json @>
        '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}',
      coalesce(private.next_hybrid_json_codes_v2(
        new.json #>
          '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}',
        '@classId'
      ), '{}'),
      coalesce(private.next_hybrid_json_codes_v2(
        new.json #>
          '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}',
        '@catId'
      ), '{}'),
      new.modified_at
    )
    on conflict(dataset_kind, id, version) do update set
      state_code = excluded.state_code,
      team_id = excluded.team_id,
      dataset_type = excluded.dataset_type,
      is_emission = excluded.is_emission,
      classification_codes = excluded.classification_codes,
      elementary_codes = excluded.elementary_codes,
      source_modified_at = excluded.source_modified_at;
  else
    delete from private.next_hybrid_public_candidates_v2 as candidate
    where candidate.dataset_kind = 'flow'
      and candidate.id = new.id
      and candidate.version = new.version::text;
  end if;
  return null;
end;
$$;

ALTER FUNCTION "private"."sync_next_hybrid_public_flow_candidate_v2"() OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."sync_next_hybrid_public_flow_candidate_v2"() FROM PUBLIC;
