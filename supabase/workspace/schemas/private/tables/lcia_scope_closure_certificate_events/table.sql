CREATE TABLE IF NOT EXISTS "private"."lcia_scope_closure_certificate_events" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "closure_check_id" "uuid" NOT NULL,
    "certificate_status" "text" NOT NULL,
    "reason" "text" NOT NULL,
    "created_by" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lcia_scope_closure_certificate_events_certificate_status_check" CHECK (("certificate_status" = ANY (ARRAY['stale'::"text", 'revoked'::"text"])))
);

ALTER TABLE "private"."lcia_scope_closure_certificate_events" OWNER TO "postgres";

ALTER TABLE ONLY "private"."lcia_scope_closure_certificate_events"
    ADD CONSTRAINT "lcia_scope_closure_certificate_events_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "private"."lcia_scope_closure_certificate_events"
    ADD CONSTRAINT "lcia_scope_closure_certificate_events_closure_check_id_fkey" FOREIGN KEY ("closure_check_id") REFERENCES "private"."lcia_scope_closure_checks"("id") ON DELETE CASCADE;

ALTER TABLE "private"."lcia_scope_closure_certificate_events" ENABLE ROW LEVEL SECURITY;

GRANT SELECT,REFERENCES,TRIGGER,TRUNCATE,MAINTAIN ON TABLE "private"."lcia_scope_closure_certificate_events" TO "service_role";

GRANT SELECT ON TABLE "private"."lcia_scope_closure_certificate_events" TO "api_internal_executor";
