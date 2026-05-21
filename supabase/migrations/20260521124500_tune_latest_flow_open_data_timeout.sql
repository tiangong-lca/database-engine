CREATE INDEX IF NOT EXISTS "flows_public_latest_keys_cover_idx"
ON "public"."flows" USING "btree" ("id", "version" DESC, "modified_at" DESC)
INCLUDE ("created_at", "team_id")
WHERE "state_code" = 100;

ALTER FUNCTION "public"."get_latest_flow_versions"(bigint, bigint, text, text, uuid, integer, jsonb, text, text)
  SET "statement_timeout" TO '60s';

ALTER FUNCTION "public"."get_latest_process_versions"(bigint, bigint, text, text, uuid, integer, text, text, text)
  SET "statement_timeout" TO '60s';

ALTER FUNCTION "public"."get_latest_lifecyclemodel_versions"(bigint, bigint, text, text, uuid, integer, text, text)
  SET "statement_timeout" TO '60s';
