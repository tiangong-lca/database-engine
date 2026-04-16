CREATE POLICY "Enable read access for authenticated users" ON "public"."flows" FOR SELECT TO "authenticated" USING ((("state_code" >= 100) OR (( SELECT "auth"."uid"() AS "uid") = "user_id") OR (EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."team_id" = "flows"."team_id") AND (("roles"."role")::"text" = ANY (ARRAY[('admin'::character varying)::"text", ('member'::character varying)::"text", ('owner'::character varying)::"text"])) AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (("state_code" = 20) AND ((EXISTS ( SELECT 1
   FROM "public"."roles"
  WHERE (("roles"."team_id" = '00000000-0000-0000-0000-000000000000'::"uuid") AND (("roles"."role")::"text" = 'review-admin'::"text") AND ("roles"."user_id" = ( SELECT "auth"."uid"() AS "uid"))))) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."state_code" > 0) AND (((("r"."json" -> 'data'::"text") ->> 'id'::"text"))::"uuid" = "flows"."id") AND ((("r"."json" -> 'data'::"text") ->> 'version'::"text") = ("flows"."version")::"text") AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text"))))) OR (EXISTS ( SELECT 1
   FROM "public"."reviews" "r"
  WHERE (("r"."id" IN ( SELECT (("review_item"."value" ->> 'id'::"text"))::"uuid" AS "uuid"
           FROM "jsonb_array_elements"("flows"."reviews") "review_item"("value"))) AND ("r"."reviewer_id" @> "jsonb_build_array"((( SELECT "auth"."uid"() AS "uid"))::"text")))))))));
