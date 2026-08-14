CREATE TABLE IF NOT EXISTS "private"."lcia_scope_closure_artifact_write_set_batches" (
    "write_set_id" "uuid" NOT NULL,
    "batch_id" "uuid" NOT NULL,
    "request_sha256" "text" NOT NULL,
    "item_count" integer NOT NULL,
    "first_ordinal" integer NOT NULL,
    "last_ordinal" integer NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lcia_scope_closure_artifact_write_set_batches_count_check" CHECK ((("item_count" >= 1) AND ("item_count" <= 500))),
    CONSTRAINT "lcia_scope_closure_artifact_write_set_batches_hash_check" CHECK (("request_sha256" ~ '^[a-f0-9]{64}$'::"text")),
    CONSTRAINT "lcia_scope_closure_artifact_write_set_batches_range_check" CHECK ((("first_ordinal" > 0) AND ("last_ordinal" >= "first_ordinal") AND ((("last_ordinal" - "first_ordinal") + 1) = "item_count")))
);

ALTER TABLE "private"."lcia_scope_closure_artifact_write_set_batches" OWNER TO "postgres";

COMMENT ON TABLE "private"."lcia_scope_closure_artifact_write_set_batches" IS 'Idempotency receipts for bounded v2 descriptor registration. A batch UUID is replayable only with the same canonical request bytes.';

ALTER TABLE ONLY "private"."lcia_scope_closure_artifact_write_set_batches"
    ADD CONSTRAINT "lcia_scope_closure_artifact_write_set_batches_pkey" PRIMARY KEY ("write_set_id", "batch_id");

ALTER TABLE ONLY "private"."lcia_scope_closure_artifact_write_set_batches"
    ADD CONSTRAINT "lcia_scope_closure_artifact_write_set_batches_write_set_id_fkey" FOREIGN KEY ("write_set_id") REFERENCES "private"."lcia_scope_closure_artifact_write_sets"("id") ON DELETE RESTRICT;

ALTER TABLE "private"."lcia_scope_closure_artifact_write_set_batches" ENABLE ROW LEVEL SECURITY;

GRANT SELECT ON TABLE "private"."lcia_scope_closure_artifact_write_set_batches" TO "api_internal_executor";
