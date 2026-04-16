CREATE TABLE IF NOT EXISTS "public"."reviews" (
    "id" "uuid" NOT NULL,
    "data_id" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"(),
    "modified_at" timestamp with time zone DEFAULT "now"(),
    "state_code" integer DEFAULT 0,
    "data_version" character(9),
    "reviewer_id" "jsonb",
    "json" "jsonb",
    "deadline" timestamp with time zone,
    CONSTRAINT "reviews_state_code_check" CHECK (("state_code" = ANY (ARRAY['-1'::integer, 0, 1, 2])))
);

ALTER TABLE "public"."reviews" OWNER TO "postgres";

ALTER TABLE ONLY "public"."reviews"
    ADD CONSTRAINT "reviews_pkey" PRIMARY KEY ("id");

ALTER TABLE "public"."reviews" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."reviews" TO "anon";

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "public"."reviews" TO "authenticated";

GRANT ALL ON TABLE "public"."reviews" TO "service_role";
