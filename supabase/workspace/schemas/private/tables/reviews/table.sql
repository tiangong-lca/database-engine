CREATE TABLE IF NOT EXISTS "private"."reviews" (
    "id" "uuid" NOT NULL,
    "data_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "state_code" integer DEFAULT 0,
    "data_version" character(9),
    "reviewer_id" "jsonb",
    "json" "jsonb",
    "deadline" timestamp with time zone,
    "review_kind" "text",
    "target_table" "text",
    "submitted_revision_checksum" "text",
    "approved_revision_checksum" "text",
    "target_owner_id" "uuid",
    "target_team_id" "uuid",
    CONSTRAINT "reviews_approved_checksum_v2_chk" CHECK ((("approved_revision_checksum" IS NULL) OR ("approved_revision_checksum" ~ '^[a-f0-9]{64}$'::"text"))),
    CONSTRAINT "reviews_kind_target_v3_chk" CHECK ((("review_kind" IS NULL) OR (("target_table" IS NOT NULL) AND ("data_id" IS NOT NULL) AND ("data_version" IS NOT NULL) AND ("submitted_revision_checksum" IS NOT NULL)))),
    CONSTRAINT "reviews_review_kind_v2_chk" CHECK ((("review_kind" IS NULL) OR ("review_kind" = ANY (ARRAY['root'::"text", 'reference'::"text"])))),
    CONSTRAINT "reviews_state_code_check" CHECK (("state_code" = ANY (ARRAY['-1'::integer, 0, 1, 2]))),
    CONSTRAINT "reviews_submitted_checksum_v2_chk" CHECK ((("submitted_revision_checksum" IS NULL) OR ("submitted_revision_checksum" ~ '^[a-f0-9]{64}$'::"text"))),
    CONSTRAINT "reviews_target_table_v2_chk" CHECK ((("target_table" IS NULL) OR ("target_table" = ANY (ARRAY['contacts'::"text", 'sources'::"text", 'unitgroups'::"text", 'flowproperties'::"text", 'flows'::"text", 'processes'::"text", 'lifecyclemodels'::"text"]))))
);

ALTER TABLE "private"."reviews" OWNER TO "postgres";

ALTER TABLE ONLY "private"."reviews"
    ADD CONSTRAINT "reviews_pkey" PRIMARY KEY ("id");

ALTER TABLE "private"."reviews" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "private"."reviews" TO "service_role";

GRANT SELECT ON TABLE "private"."reviews" TO "api_internal_executor";

GRANT SELECT ON TABLE "private"."reviews" TO "authenticated";
