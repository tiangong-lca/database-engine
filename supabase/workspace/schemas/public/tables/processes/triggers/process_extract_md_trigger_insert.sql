CREATE OR REPLACE TRIGGER "process_extract_md_trigger_insert" AFTER INSERT ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_process_embedding_ft', '1000');
