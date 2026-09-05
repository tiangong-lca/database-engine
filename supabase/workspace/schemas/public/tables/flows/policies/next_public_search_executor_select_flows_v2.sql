CREATE POLICY "next_public_search_executor_select_flows_v2" ON "public"."flows" FOR SELECT TO "next_public_search_executor" USING (("state_code" = ANY (ARRAY[100, 200])));
