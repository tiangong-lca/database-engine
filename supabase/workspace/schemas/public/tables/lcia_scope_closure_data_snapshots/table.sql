CREATE TABLE IF NOT EXISTS "public"."lcia_scope_closure_data_snapshots" (
    "data_snapshot_token" "text" NOT NULL,
    "root_manifest" "jsonb" NOT NULL,
    "root_manifest_hash" "text" NOT NULL,
    "publication_epoch" bigint DEFAULT "nextval"('"public"."lcia_scope_closure_publication_epoch_seq"'::"regclass") NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lcia_scope_closure_data_snapshots_data_snapshot_token_check" CHECK (("length"(TRIM(BOTH FROM "data_snapshot_token")) > 0)),
    CONSTRAINT "lcia_scope_closure_data_snapshots_root_manifest_check" CHECK (("jsonb_typeof"("root_manifest") = 'object'::"text")),
    CONSTRAINT "lcia_scope_closure_data_snapshots_root_manifest_hash_check" CHECK (("length"(TRIM(BOTH FROM "root_manifest_hash")) > 0))
);

ALTER TABLE "public"."lcia_scope_closure_data_snapshots" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lcia_scope_closure_data_snapshots"
    ADD CONSTRAINT "lcia_scope_closure_data_snapshots_pkey" PRIMARY KEY ("data_snapshot_token");

ALTER TABLE "public"."lcia_scope_closure_data_snapshots" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lcia_scope_closure_data_snapshots" TO "service_role";
