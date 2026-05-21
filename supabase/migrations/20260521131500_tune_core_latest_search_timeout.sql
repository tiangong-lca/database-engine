CREATE INDEX IF NOT EXISTS "processes_public_latest_keys_cover_idx"
ON "public"."processes" USING "btree" ("id", "version" DESC, "modified_at" DESC)
INCLUDE ("created_at", "team_id", "model_id")
WHERE "state_code" = 100;

CREATE INDEX IF NOT EXISTS "lifecyclemodels_public_latest_keys_cover_idx"
ON "public"."lifecyclemodels" USING "btree" ("id", "version" DESC, "modified_at" DESC)
INCLUDE ("created_at", "team_id")
WHERE "state_code" = 100;

ALTER FUNCTION "public"."pgroonga_search_flows_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer)
  SET "statement_timeout" TO '60s';

ALTER FUNCTION "public"."pgroonga_search_processes_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text)
  SET "statement_timeout" TO '60s';

ALTER FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer)
  SET "statement_timeout" TO '60s';
