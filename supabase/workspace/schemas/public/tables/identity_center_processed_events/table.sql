CREATE TABLE IF NOT EXISTS "public"."identity_center_processed_events" (
    "event_id" "text" NOT NULL,
    "event_type" "text" NOT NULL,
    "processed_at" timestamp with time zone DEFAULT "now"()
);

ALTER TABLE "public"."identity_center_processed_events" OWNER TO "postgres";

COMMENT ON TABLE "public"."identity_center_processed_events" IS 'Identity Center webhook idempotency ledger (dedupe by platform event id). Service-role only: RLS enabled with no policies.';

ALTER TABLE ONLY "public"."identity_center_processed_events"
    ADD CONSTRAINT "identity_center_processed_events_pkey" PRIMARY KEY ("event_id");

ALTER TABLE "public"."identity_center_processed_events" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."identity_center_processed_events" TO "service_role";
