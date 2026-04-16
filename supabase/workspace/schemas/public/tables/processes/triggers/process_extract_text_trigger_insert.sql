CREATE OR REPLACE TRIGGER "process_extract_text_trigger_insert" AFTER INSERT ON "public"."processes" FOR EACH ROW EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_process_embedding', '1000');
