CREATE TABLE IF NOT EXISTS "public"."lcia_scope_closure_artifact_write_set_items" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "write_set_id" "uuid" NOT NULL,
    "ordinal" integer NOT NULL,
    "client_key" "text" NOT NULL,
    "artifact_type" "text" NOT NULL,
    "artifact_role" "text" NOT NULL,
    "storage_bucket" "text" NOT NULL,
    "storage_path" "text" NOT NULL,
    "content_type" "text" NOT NULL,
    "byte_size" bigint NOT NULL,
    "checksum_sha256" "text" NOT NULL,
    "metadata" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lcia_scope_closure_artifact_write_set_items_checksum_check" CHECK (("checksum_sha256" ~ '^[a-f0-9]{64}$'::"text")),
    CONSTRAINT "lcia_scope_closure_artifact_write_set_items_client_key_check" CHECK ((("length"(TRIM(BOTH FROM "client_key")) >= 1) AND ("length"(TRIM(BOTH FROM "client_key")) <= 500))),
    CONSTRAINT "lcia_scope_closure_artifact_write_set_items_locator_check" CHECK ((("length"(TRIM(BOTH FROM "storage_bucket")) > 0) AND ("length"(TRIM(BOTH FROM "storage_path")) > 0))),
    CONSTRAINT "lcia_scope_closure_artifact_write_set_items_metadata_check" CHECK (("jsonb_typeof"("metadata") = 'object'::"text")),
    CONSTRAINT "lcia_scope_closure_artifact_write_set_items_ordinal_check" CHECK (("ordinal" > 0)),
    CONSTRAINT "lcia_scope_closure_artifact_write_set_items_role_check" CHECK ((NOT ("artifact_role" IS DISTINCT FROM "public"."lcia_scope_closure_artifact_role"("artifact_type")))),
    CONSTRAINT "lcia_scope_closure_artifact_write_set_items_size_check" CHECK (("byte_size" >= 0))
);

ALTER TABLE "public"."lcia_scope_closure_artifact_write_set_items" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lcia_scope_closure_artifact_write_set_items"
    ADD CONSTRAINT "lcia_scope_closure_artifact_write_set_items_client_uidx" UNIQUE ("write_set_id", "client_key");

ALTER TABLE ONLY "public"."lcia_scope_closure_artifact_write_set_items"
    ADD CONSTRAINT "lcia_scope_closure_artifact_write_set_items_locator_uidx" UNIQUE ("write_set_id", "storage_bucket", "storage_path");

ALTER TABLE ONLY "public"."lcia_scope_closure_artifact_write_set_items"
    ADD CONSTRAINT "lcia_scope_closure_artifact_write_set_items_ordinal_uidx" UNIQUE ("write_set_id", "ordinal");

ALTER TABLE ONLY "public"."lcia_scope_closure_artifact_write_set_items"
    ADD CONSTRAINT "lcia_scope_closure_artifact_write_set_items_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lcia_scope_closure_artifact_write_set_items"
    ADD CONSTRAINT "lcia_scope_closure_artifact_write_set_items_write_set_id_fkey" FOREIGN KEY ("write_set_id") REFERENCES "public"."lcia_scope_closure_artifact_write_sets"("id") ON DELETE RESTRICT;

ALTER TABLE "public"."lcia_scope_closure_artifact_write_set_items" ENABLE ROW LEVEL SECURITY;
