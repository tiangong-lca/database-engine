CREATE TABLE IF NOT EXISTS "private"."notifications" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "recipient_user_id" "uuid" NOT NULL,
    "sender_user_id" "uuid" NOT NULL,
    "type" "text" NOT NULL,
    "dataset_type" "text" NOT NULL,
    "dataset_id" "uuid" NOT NULL,
    "dataset_version" "text" NOT NULL,
    "json" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "modified_at" timestamp with time zone DEFAULT "now"() NOT NULL
);

ALTER TABLE "private"."notifications" OWNER TO "postgres";

ALTER TABLE ONLY "private"."notifications"
    ADD CONSTRAINT "notifications_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "private"."notifications"
    ADD CONSTRAINT "notifications_recipient_user_id_fkey" FOREIGN KEY ("recipient_user_id") REFERENCES "auth"."users"("id");

ALTER TABLE ONLY "private"."notifications"
    ADD CONSTRAINT "notifications_sender_user_id_fkey" FOREIGN KEY ("sender_user_id") REFERENCES "auth"."users"("id");

ALTER TABLE "private"."notifications" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "private"."notifications" TO "service_role";

GRANT SELECT ON TABLE "private"."notifications" TO "api_internal_executor";
