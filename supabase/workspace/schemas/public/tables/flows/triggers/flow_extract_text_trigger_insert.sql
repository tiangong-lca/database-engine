CREATE OR REPLACE TRIGGER "flow_extract_text_trigger_insert" AFTER INSERT ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_flow_embedding', '1000');
