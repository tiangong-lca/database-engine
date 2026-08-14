CREATE TABLE IF NOT EXISTS "private"."identity_center_processed_events" (
    "event_id" "text" NOT NULL,
    "event_type" "text" NOT NULL,
    "processed_at" timestamp with time zone DEFAULT "now"()
);

ALTER TABLE "private"."identity_center_processed_events" OWNER TO "postgres";

ALTER TABLE ONLY "private"."identity_center_processed_events"
    ADD CONSTRAINT "identity_center_processed_events_pkey" PRIMARY KEY ("event_id");

ALTER TABLE "private"."identity_center_processed_events" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "private"."identity_center_processed_events" TO "service_role";

GRANT SELECT ON TABLE "private"."identity_center_processed_events" TO "api_internal_executor";
