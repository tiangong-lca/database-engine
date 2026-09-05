CREATE POLICY "next_public_search_executor_select_processes_v2" ON "public"."processes" FOR SELECT TO "next_public_search_executor" USING (("state_code" = ANY (ARRAY[100, 200])));
