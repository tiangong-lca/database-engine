CREATE UNIQUE INDEX "lcia_scope_closure_checks_request_key_uidx" ON "private"."lcia_scope_closure_checks" USING "btree" ("requested_by", "request_key");
