CREATE TABLE IF NOT EXISTS "public"."comments" (
    "review_id" "uuid" NOT NULL,
    "reviewer_id" "uuid" DEFAULT "auth"."uid"() NOT NULL,
    "json" json,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "state_code" integer DEFAULT 0,
    CONSTRAINT "comments_state_code_check" CHECK (("state_code" = ANY (ARRAY['-3'::integer, '-2'::integer, '-1'::integer, 0, 1, 2])))
);

ALTER TABLE "public"."comments" OWNER TO "postgres";

ALTER TABLE ONLY "public"."comments"
    ADD CONSTRAINT "comments_pkey" PRIMARY KEY ("review_id", "reviewer_id");

ALTER TABLE ONLY "public"."comments"
    ADD CONSTRAINT "comments_review_id_fkey" FOREIGN KEY ("review_id") REFERENCES "public"."reviews"("id");

ALTER TABLE "public"."comments" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."comments" TO "anon";

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."comments" TO "authenticated";

GRANT ALL ON TABLE "public"."comments" TO "service_role";
