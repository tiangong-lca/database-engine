CREATE TABLE IF NOT EXISTS "public"."lca_network_snapshots" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "scope" "text" DEFAULT 'full_library'::"text" NOT NULL,
    "process_filter" "jsonb",
    "lcia_method_id" "uuid",
    "lcia_method_version" character(9),
    "provider_matching_rule" "text" DEFAULT 'split_by_evidence_hybrid'::"text" NOT NULL,
    "source_hash" "text",
    "status" "text" DEFAULT 'draft'::"text" NOT NULL,
    "created_by" "uuid",
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    CONSTRAINT "lca_network_snapshots_provider_rule_chk" CHECK (("provider_matching_rule" = ANY (ARRAY['strict_unique_provider'::"text", 'best_provider_strict'::"text", 'split_by_evidence'::"text", 'split_by_evidence_hybrid'::"text", 'split_equal'::"text", 'equal_split_multi_provider'::"text", 'custom_weighted_provider'::"text"]))),
    CONSTRAINT "lca_network_snapshots_scope_chk" CHECK (("scope" = 'full_library'::"text")),
    CONSTRAINT "lca_network_snapshots_status_chk" CHECK (("status" = ANY (ARRAY['draft'::"text", 'ready'::"text", 'stale'::"text", 'failed'::"text"])))
);

ALTER TABLE "public"."lca_network_snapshots" OWNER TO "postgres";

ALTER TABLE ONLY "public"."lca_network_snapshots"
    ADD CONSTRAINT "lca_network_snapshots_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."lca_network_snapshots"
    ADD CONSTRAINT "lca_network_snapshots_lcia_fk" FOREIGN KEY ("lcia_method_id", "lcia_method_version") REFERENCES "public"."lciamethods"("id", "version") ON DELETE SET NULL;

ALTER TABLE "public"."lca_network_snapshots" ENABLE ROW LEVEL SECURITY;

GRANT ALL ON TABLE "public"."lca_network_snapshots" TO "anon";

GRANT ALL ON TABLE "public"."lca_network_snapshots" TO "authenticated";

GRANT ALL ON TABLE "public"."lca_network_snapshots" TO "service_role";
