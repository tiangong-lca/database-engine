CREATE OR REPLACE VIEW "public"."worker_legacy_table_retirement_blockers" WITH ("security_invoker"='true') AS
 WITH "legacy_targets" AS (
         SELECT "target_namespace"."nspname" AS "legacy_schema",
            "target_class"."relname" AS "legacy_table",
            "target_class"."oid" AS "table_oid",
            "target_class"."reltype" AS "row_type_oid"
           FROM ((( VALUES ('public'::"name",'lca_jobs'::"name"), ('public'::"name",'lca_package_jobs'::"name"), ('public'::"name",'dataset_review_submit_jobs'::"name")) "targets"("schema_name", "table_name")
             JOIN "pg_namespace" "target_namespace" ON (("target_namespace"."nspname" = "targets"."schema_name")))
             JOIN "pg_class" "target_class" ON ((("target_class"."relnamespace" = "target_namespace"."oid") AND ("target_class"."relname" = "targets"."table_name") AND ("target_class"."relkind" = ANY (ARRAY['r'::"char", 'p'::"char"])))))
        ), "foreign_key_blockers" AS (
         SELECT "concat"("legacy_targets"."legacy_schema", '.', "legacy_targets"."legacy_table") AS "legacy_table",
            'foreign_key'::"text" AS "blocker_type",
            ("dependent_namespace"."nspname")::"text" AS "blocker_schema",
            ("dependent_class"."relname")::"text" AS "blocker_name",
            ("constraint_record"."conname")::"text" AS "blocker_identity",
            true AS "is_drop_restrict_blocker",
            "jsonb_build_object"('constraintName', "constraint_record"."conname", 'dependentTable', "concat"("dependent_namespace"."nspname", '.', "dependent_class"."relname"), 'dependentColumns', ( SELECT "jsonb_agg"("dependent_attribute"."attname" ORDER BY "dependent_attribute"."attnum") AS "jsonb_agg"
                   FROM ("unnest"("constraint_record"."conkey") "constraint_column"("attnum")
                     JOIN "pg_attribute" "dependent_attribute" ON ((("dependent_attribute"."attrelid" = "constraint_record"."conrelid") AND ("dependent_attribute"."attnum" = "constraint_column"."attnum"))))), 'referencedColumns', ( SELECT "jsonb_agg"("referenced_attribute"."attname" ORDER BY "referenced_attribute"."attnum") AS "jsonb_agg"
                   FROM ("unnest"("constraint_record"."confkey") "referenced_column"("attnum")
                     JOIN "pg_attribute" "referenced_attribute" ON ((("referenced_attribute"."attrelid" = "constraint_record"."confrelid") AND ("referenced_attribute"."attnum" = "referenced_column"."attnum"))))), 'onDelete', "constraint_record"."confdeltype") AS "details"
           FROM ((("legacy_targets"
             JOIN "pg_constraint" "constraint_record" ON ((("constraint_record"."confrelid" = "legacy_targets"."table_oid") AND ("constraint_record"."contype" = 'f'::"char"))))
             JOIN "pg_class" "dependent_class" ON (("dependent_class"."oid" = "constraint_record"."conrelid")))
             JOIN "pg_namespace" "dependent_namespace" ON (("dependent_namespace"."oid" = "dependent_class"."relnamespace")))
          WHERE ("constraint_record"."conrelid" <> "legacy_targets"."table_oid")
        ), "view_blockers" AS (
         SELECT DISTINCT "concat"("legacy_targets"."legacy_schema", '.', "legacy_targets"."legacy_table") AS "legacy_table",
                CASE "dependent_class"."relkind"
                    WHEN 'm'::"char" THEN 'dependent_materialized_view'::"text"
                    ELSE 'dependent_view'::"text"
                END AS "blocker_type",
            ("dependent_namespace"."nspname")::"text" AS "blocker_schema",
            ("dependent_class"."relname")::"text" AS "blocker_name",
            "concat"("dependent_namespace"."nspname", '.', "dependent_class"."relname") AS "blocker_identity",
            true AS "is_drop_restrict_blocker",
            "jsonb_build_object"('dependentView', "concat"("dependent_namespace"."nspname", '.', "dependent_class"."relname"), 'relkind', "dependent_class"."relkind") AS "details"
           FROM (((("legacy_targets"
             JOIN "pg_depend" "dependency" ON (("dependency"."refobjid" = "legacy_targets"."table_oid")))
             JOIN "pg_rewrite" "rewrite_rule" ON (("rewrite_rule"."oid" = "dependency"."objid")))
             JOIN "pg_class" "dependent_class" ON (("dependent_class"."oid" = "rewrite_rule"."ev_class")))
             JOIN "pg_namespace" "dependent_namespace" ON (("dependent_namespace"."oid" = "dependent_class"."relnamespace")))
          WHERE (("dependent_class"."oid" <> "legacy_targets"."table_oid") AND ("dependent_class"."relkind" = ANY (ARRAY['v'::"char", 'm'::"char"])))
        ), "policy_blockers" AS (
         SELECT DISTINCT "concat"("legacy_targets"."legacy_schema", '.', "legacy_targets"."legacy_table") AS "legacy_table",
            'policy'::"text" AS "blocker_type",
            ("dependent_namespace"."nspname")::"text" AS "blocker_schema",
            ("dependent_class"."relname")::"text" AS "blocker_name",
            "concat"("dependent_namespace"."nspname", '.', "dependent_class"."relname", '.', "policy_record"."polname") AS "blocker_identity",
            true AS "is_drop_restrict_blocker",
            "jsonb_build_object"('policyName', "policy_record"."polname", 'dependentTable', "concat"("dependent_namespace"."nspname", '.', "dependent_class"."relname"), 'command', "policy_record"."polcmd") AS "details"
           FROM (((("legacy_targets"
             JOIN "pg_depend" "dependency" ON (("dependency"."refobjid" = "legacy_targets"."table_oid")))
             JOIN "pg_policy" "policy_record" ON (("policy_record"."oid" = "dependency"."objid")))
             JOIN "pg_class" "dependent_class" ON (("dependent_class"."oid" = "policy_record"."polrelid")))
             JOIN "pg_namespace" "dependent_namespace" ON (("dependent_namespace"."oid" = "dependent_class"."relnamespace")))
          WHERE ("dependent_class"."oid" <> "legacy_targets"."table_oid")
        ), "function_signature_blockers" AS (
         SELECT DISTINCT "concat"("legacy_targets"."legacy_schema", '.', "legacy_targets"."legacy_table") AS "legacy_table",
            'function_signature'::"text" AS "blocker_type",
            ("function_namespace"."nspname")::"text" AS "blocker_schema",
            ("function_record"."proname")::"text" AS "blocker_name",
            "concat"("function_namespace"."nspname", '.', "function_record"."proname", '(', "pg_get_function_identity_arguments"("function_record"."oid"), ')') AS "blocker_identity",
            true AS "is_drop_restrict_blocker",
            "jsonb_build_object"('arguments', "pg_get_function_arguments"("function_record"."oid"), 'result', "pg_get_function_result"("function_record"."oid")) AS "details"
           FROM (("legacy_targets"
             JOIN ( SELECT "pg_proc"."oid",
                    "pg_proc"."proname",
                    "pg_proc"."pronamespace",
                    "pg_proc"."proowner",
                    "pg_proc"."prolang",
                    "pg_proc"."procost",
                    "pg_proc"."prorows",
                    "pg_proc"."provariadic",
                    "pg_proc"."prosupport",
                    "pg_proc"."prokind",
                    "pg_proc"."prosecdef",
                    "pg_proc"."proleakproof",
                    "pg_proc"."proisstrict",
                    "pg_proc"."proretset",
                    "pg_proc"."provolatile",
                    "pg_proc"."proparallel",
                    "pg_proc"."pronargs",
                    "pg_proc"."pronargdefaults",
                    "pg_proc"."prorettype",
                    "pg_proc"."proargtypes",
                    "pg_proc"."proallargtypes",
                    "pg_proc"."proargmodes",
                    "pg_proc"."proargnames",
                    "pg_proc"."proargdefaults",
                    "pg_proc"."protrftypes",
                    "pg_proc"."prosrc",
                    "pg_proc"."probin",
                    "pg_proc"."prosqlbody",
                    "pg_proc"."proconfig",
                    "pg_proc"."proacl"
                   FROM "pg_proc"
                  WHERE ("pg_proc"."prokind" = ANY (ARRAY['f'::"char", 'p'::"char", 'w'::"char"]))) "function_record" ON ((("lower"("pg_get_function_arguments"("function_record"."oid")) ~~ (('%'::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%'::"text")) OR ("lower"("pg_get_function_result"("function_record"."oid")) ~~ (('%'::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%'::"text")))))
             JOIN "pg_namespace" "function_namespace" ON (("function_namespace"."oid" = "function_record"."pronamespace")))
        ), "function_source_references" AS (
         SELECT DISTINCT "concat"("legacy_targets"."legacy_schema", '.', "legacy_targets"."legacy_table") AS "legacy_table",
            'function_source_reference'::"text" AS "blocker_type",
            ("function_namespace"."nspname")::"text" AS "blocker_schema",
            ("function_record"."proname")::"text" AS "blocker_name",
            "concat"("function_namespace"."nspname", '.', "function_record"."proname", '(', "pg_get_function_identity_arguments"("function_record"."oid"), ')') AS "blocker_identity",
            false AS "is_drop_restrict_blocker",
            "jsonb_build_object"('reason', 'Function body references the legacy table name; this may not block DROP TABLE RESTRICT, but it is a runtime migration blocker.', 'arguments', "pg_get_function_arguments"("function_record"."oid"), 'result', "pg_get_function_result"("function_record"."oid")) AS "details"
           FROM (("legacy_targets"
             JOIN ( SELECT "pg_proc"."oid",
                    "pg_proc"."proname",
                    "pg_proc"."pronamespace",
                    "pg_proc"."proowner",
                    "pg_proc"."prolang",
                    "pg_proc"."procost",
                    "pg_proc"."prorows",
                    "pg_proc"."provariadic",
                    "pg_proc"."prosupport",
                    "pg_proc"."prokind",
                    "pg_proc"."prosecdef",
                    "pg_proc"."proleakproof",
                    "pg_proc"."proisstrict",
                    "pg_proc"."proretset",
                    "pg_proc"."provolatile",
                    "pg_proc"."proparallel",
                    "pg_proc"."pronargs",
                    "pg_proc"."pronargdefaults",
                    "pg_proc"."prorettype",
                    "pg_proc"."proargtypes",
                    "pg_proc"."proallargtypes",
                    "pg_proc"."proargmodes",
                    "pg_proc"."proargnames",
                    "pg_proc"."proargdefaults",
                    "pg_proc"."protrftypes",
                    "pg_proc"."prosrc",
                    "pg_proc"."probin",
                    "pg_proc"."prosqlbody",
                    "pg_proc"."proconfig",
                    "pg_proc"."proacl"
                   FROM "pg_proc"
                  WHERE ("pg_proc"."prokind" = ANY (ARRAY['f'::"char", 'p'::"char", 'w'::"char"]))) "function_record" ON ((("lower"("function_record"."prosrc") ~~ (('%public.'::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%'::"text")) OR ("lower"("function_record"."prosrc") ~~ (('%from '::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%'::"text")) OR ("lower"("function_record"."prosrc") ~~ (('%join '::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%'::"text")) OR ("lower"("function_record"."prosrc") ~~ (('%update '::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%'::"text")) OR ("lower"("function_record"."prosrc") ~~ (('%insert into '::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%'::"text")) OR ("lower"("function_record"."prosrc") ~~ (('%delete from '::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%'::"text")) OR ("lower"("function_record"."prosrc") ~~ (('%'::"text" || "lower"(("legacy_targets"."legacy_table")::"text")) || '%rowtype%'::"text")))))
             JOIN "pg_namespace" "function_namespace" ON (("function_namespace"."oid" = "function_record"."pronamespace")))
          WHERE ("function_namespace"."nspname" <> ALL (ARRAY['pg_catalog'::"name", 'information_schema'::"name"]))
        )
 SELECT "foreign_key_blockers"."legacy_table",
    "foreign_key_blockers"."blocker_type",
    "foreign_key_blockers"."blocker_schema",
    "foreign_key_blockers"."blocker_name",
    "foreign_key_blockers"."blocker_identity",
    "foreign_key_blockers"."is_drop_restrict_blocker",
    "foreign_key_blockers"."details"
   FROM "foreign_key_blockers"
UNION ALL
 SELECT "view_blockers"."legacy_table",
    "view_blockers"."blocker_type",
    "view_blockers"."blocker_schema",
    "view_blockers"."blocker_name",
    "view_blockers"."blocker_identity",
    "view_blockers"."is_drop_restrict_blocker",
    "view_blockers"."details"
   FROM "view_blockers"
UNION ALL
 SELECT "policy_blockers"."legacy_table",
    "policy_blockers"."blocker_type",
    "policy_blockers"."blocker_schema",
    "policy_blockers"."blocker_name",
    "policy_blockers"."blocker_identity",
    "policy_blockers"."is_drop_restrict_blocker",
    "policy_blockers"."details"
   FROM "policy_blockers"
UNION ALL
 SELECT "function_signature_blockers"."legacy_table",
    "function_signature_blockers"."blocker_type",
    "function_signature_blockers"."blocker_schema",
    "function_signature_blockers"."blocker_name",
    "function_signature_blockers"."blocker_identity",
    "function_signature_blockers"."is_drop_restrict_blocker",
    "function_signature_blockers"."details"
   FROM "function_signature_blockers"
UNION ALL
 SELECT "function_source_references"."legacy_table",
    "function_source_references"."blocker_type",
    "function_source_references"."blocker_schema",
    "function_source_references"."blocker_name",
    "function_source_references"."blocker_identity",
    "function_source_references"."is_drop_restrict_blocker",
    "function_source_references"."details"
   FROM "function_source_references";

ALTER VIEW "public"."worker_legacy_table_retirement_blockers" OWNER TO "postgres";
