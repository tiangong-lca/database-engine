CREATE TABLE IF NOT EXISTS "private"."portal_names_backfill_v2" (
    "shard" smallint NOT NULL,
    "process_count" bigint NOT NULL,
    "flow_count" bigint NOT NULL,
    "completed_at" timestamp with time zone DEFAULT "clock_timestamp"() NOT NULL,
    CONSTRAINT "portal_names_backfill_v2_shard_check" CHECK ((("shard" >= 0) AND ("shard" <= 15)))
);

ALTER TABLE ONLY "private"."portal_names_backfill_v2" FORCE ROW LEVEL SECURITY;

ALTER TABLE "private"."portal_names_backfill_v2" OWNER TO "postgres";

ALTER TABLE ONLY "private"."portal_names_backfill_v2"
    ADD CONSTRAINT "portal_names_backfill_v2_pkey" PRIMARY KEY ("shard");

ALTER TABLE "private"."portal_names_backfill_v2" ENABLE ROW LEVEL SECURITY;
