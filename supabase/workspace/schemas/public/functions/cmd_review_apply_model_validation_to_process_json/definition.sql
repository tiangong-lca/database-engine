CREATE OR REPLACE FUNCTION "public"."cmd_review_apply_model_validation_to_process_json"("p_process_json" "jsonb", "p_model_json" "jsonb", "p_comment_review" "jsonb" DEFAULT '[]'::"jsonb", "p_comment_compliance" "jsonb" DEFAULT '[]'::"jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  with base as (
    select
      coalesce(p_process_json, '{}'::jsonb) as process_json,
      public.cmd_review_json_array(
        coalesce(
          p_process_json #> '{processDataSet,modellingAndValidation,validation,review}',
          '[]'::jsonb
        )
      ) as existing_review_items,
      public.cmd_review_json_array(
        coalesce(
          p_process_json #> '{processDataSet,modellingAndValidation,complianceDeclarations,compliance}',
          '[]'::jsonb
        )
      ) as existing_compliance_items,
      public.cmd_review_json_array(coalesce(p_comment_review, '[]'::jsonb)) as comment_review_items,
      public.cmd_review_json_array(coalesce(p_comment_compliance, '[]'::jsonb))
        as comment_compliance_items
  ),
  prepared as (
    select
      jsonb_set(
        jsonb_set(
          jsonb_set(
            jsonb_set(
              base.process_json,
              '{processDataSet}',
              case
                when jsonb_typeof(base.process_json->'processDataSet') = 'object'
                  then base.process_json->'processDataSet'
                else '{}'::jsonb
              end,
              true
            ),
            '{processDataSet,modellingAndValidation}',
            case
              when jsonb_typeof(
                base.process_json #> '{processDataSet,modellingAndValidation}'
              ) = 'object'
                then base.process_json #> '{processDataSet,modellingAndValidation}'
              else '{}'::jsonb
            end,
            true
          ),
          '{processDataSet,modellingAndValidation,validation}',
          case
            when jsonb_typeof(
              base.process_json #> '{processDataSet,modellingAndValidation,validation}'
            ) = 'object'
              then base.process_json #> '{processDataSet,modellingAndValidation,validation}'
            else '{}'::jsonb
          end,
          true
        ),
        '{processDataSet,modellingAndValidation,complianceDeclarations}',
        case
          when jsonb_typeof(
            base.process_json #> '{processDataSet,modellingAndValidation,complianceDeclarations}'
          ) = 'object'
            then base.process_json #> '{processDataSet,modellingAndValidation,complianceDeclarations}'
          else '{}'::jsonb
        end,
        true
      ) as prepared_process_json,
      base.existing_review_items,
      base.existing_compliance_items,
      base.comment_review_items,
      base.comment_compliance_items
    from base
  )
  select jsonb_set(
    jsonb_set(
      prepared.prepared_process_json,
      '{processDataSet,modellingAndValidation,validation,review}',
      prepared.existing_review_items || prepared.comment_review_items,
      true
    ),
    '{processDataSet,modellingAndValidation,complianceDeclarations,compliance}',
    prepared.existing_compliance_items || prepared.comment_compliance_items,
    true
  )
  from prepared
$$;

ALTER FUNCTION "public"."cmd_review_apply_model_validation_to_process_json"("p_process_json" "jsonb", "p_model_json" "jsonb", "p_comment_review" "jsonb", "p_comment_compliance" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_apply_model_validation_to_process_json"("p_process_json" "jsonb", "p_model_json" "jsonb", "p_comment_review" "jsonb", "p_comment_compliance" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_apply_model_validation_to_process_json"("p_process_json" "jsonb", "p_model_json" "jsonb", "p_comment_review" "jsonb", "p_comment_compliance" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_apply_model_validation_to_process_json"("p_process_json" "jsonb", "p_model_json" "jsonb", "p_comment_review" "jsonb", "p_comment_compliance" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_apply_model_validation_to_process_json"("p_process_json" "jsonb", "p_model_json" "jsonb", "p_comment_review" "jsonb", "p_comment_compliance" "jsonb") TO "service_role";
