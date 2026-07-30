CREATE INDEX "lcia_scope_closure_certificate_events_check_created_idx" ON "public"."lcia_scope_closure_certificate_events" USING "btree" ("closure_check_id", "created_at" DESC, "id" DESC);
