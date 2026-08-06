CREATE TABLE IF NOT EXISTS "private"."comments" (
    "review_id" "uuid" NOT NULL,
    "reviewer_id" "uuid" DEFAULT "auth"."uid"() NOT NULL,
    "json" json,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "state_code" integer DEFAULT 0,
    CONSTRAINT "comments_state_code_check" CHECK (("state_code" = ANY (ARRAY['-3'::integer, '-2'::integer, '-1'::integer, 0, 1, 2])))
);

ALTER TABLE "private"."comments" OWNER TO "postgres";

ALTER TABLE ONLY "private"."comments"
    ADD CONSTRAINT "comments_pkey" PRIMARY KEY ("review_id", "reviewer_id");

ALTER TABLE ONLY "private"."comments"
    ADD CONSTRAINT "comments_review_id_fkey" FOREIGN KEY ("review_id") REFERENCES "private"."reviews"("id");

ALTER TABLE "private"."comments" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "private"."comments" TO "service_role";

GRANT SELECT ON TABLE "private"."comments" TO "api_internal_executor";
