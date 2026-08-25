CREATE POLICY "portal_public_executor_select_flows_v1" ON "public"."flows" FOR SELECT TO "portal_public_executor" USING (("state_code" = ANY (ARRAY[100, 200])));
