CREATE OR REPLACE TRIGGER "flow_extract_md_trigger_insert" AFTER INSERT ON "public"."flows" FOR EACH ROW EXECUTE FUNCTION "util"."invoke_edge_webhook"('webhook_flow_embedding_ft', '1000');
