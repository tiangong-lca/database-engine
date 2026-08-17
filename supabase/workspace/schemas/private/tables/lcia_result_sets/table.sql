CREATE TABLE IF NOT EXISTS "private"."lcia_result_sets" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "name" "text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lcia_result_sets_name_check" CHECK (("length"("btrim"("name")) > 0))
);

ALTER TABLE "private"."lcia_result_sets" OWNER TO "postgres";

COMMENT ON TABLE "private"."lcia_result_sets" IS 'Named persistent containers for resuming the Data Processing workflow.';

ALTER TABLE ONLY "private"."lcia_result_sets"
    ADD CONSTRAINT "lcia_result_sets_pkey" PRIMARY KEY ("id");

ALTER TABLE "private"."lcia_result_sets" ENABLE ROW LEVEL SECURITY;
