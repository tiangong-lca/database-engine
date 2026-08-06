CREATE UNIQUE INDEX "dataset_flow_identity_invocation_scope_active_uidx" ON "util"."dataset_flow_identity_wrapper_invocations" USING "btree" ("scope_id") WHERE ("status" = 'active'::"text");
