CREATE TABLE IF NOT EXISTS "private"."next_hybrid_public_candidates_v2" (
    "dataset_kind" "text" NOT NULL,
    "id" "uuid" NOT NULL,
    "version" "text" NOT NULL,
    "state_code" integer NOT NULL,
    "team_id" "uuid",
    "dataset_type" "text",
    "is_emission" boolean NOT NULL,
    "classification_codes" "text"[] DEFAULT '{}'::"text"[] NOT NULL,
    "elementary_codes" "text"[] DEFAULT '{}'::"text"[] NOT NULL,
    "source_modified_at" timestamp with time zone NOT NULL,
    CONSTRAINT "next_hybrid_public_candidates_v2_dataset_kind_check" CHECK (("dataset_kind" = ANY (ARRAY['process'::"text", 'flow'::"text"]))),
    CONSTRAINT "next_hybrid_public_candidates_v2_state_code_check" CHECK (("state_code" = ANY (ARRAY[100, 200])))
);

ALTER TABLE "private"."next_hybrid_public_candidates_v2" OWNER TO "api_internal_executor";

COMMENT ON TABLE "private"."next_hybrid_public_candidates_v2" IS 'Internal public-state search-key projection for bounded Next Hybrid V2 candidate discovery; source RLS and full canonical filters are rechecked during final source hydration.';

ALTER TABLE ONLY "private"."next_hybrid_public_candidates_v2"
    ADD CONSTRAINT "next_hybrid_public_candidates_v2_pkey" PRIMARY KEY ("dataset_kind", "id", "version");

GRANT SELECT("dataset_kind") ON TABLE "private"."next_hybrid_public_candidates_v2" TO "next_public_search_executor";

GRANT SELECT("id") ON TABLE "private"."next_hybrid_public_candidates_v2" TO "next_public_search_executor";

GRANT SELECT("version") ON TABLE "private"."next_hybrid_public_candidates_v2" TO "next_public_search_executor";

GRANT SELECT("state_code") ON TABLE "private"."next_hybrid_public_candidates_v2" TO "next_public_search_executor";

GRANT SELECT("team_id") ON TABLE "private"."next_hybrid_public_candidates_v2" TO "next_public_search_executor";

GRANT SELECT("dataset_type") ON TABLE "private"."next_hybrid_public_candidates_v2" TO "next_public_search_executor";

GRANT SELECT("is_emission") ON TABLE "private"."next_hybrid_public_candidates_v2" TO "next_public_search_executor";

GRANT SELECT("classification_codes") ON TABLE "private"."next_hybrid_public_candidates_v2" TO "next_public_search_executor";

GRANT SELECT("elementary_codes") ON TABLE "private"."next_hybrid_public_candidates_v2" TO "next_public_search_executor";

GRANT SELECT("source_modified_at") ON TABLE "private"."next_hybrid_public_candidates_v2" TO "next_public_search_executor";
