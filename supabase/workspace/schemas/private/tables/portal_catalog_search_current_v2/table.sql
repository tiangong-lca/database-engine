GRANT SELECT ON TABLE "private"."portal_catalog_search_current_v2" TO "api_internal_executor";

GRANT SELECT("dataset_kind") ON TABLE "private"."portal_catalog_search_current_v2" TO "portal_public_executor";

GRANT SELECT("id") ON TABLE "private"."portal_catalog_search_current_v2" TO "portal_public_executor";

GRANT SELECT("version") ON TABLE "private"."portal_catalog_search_current_v2" TO "portal_public_executor";

GRANT SELECT("state_code") ON TABLE "private"."portal_catalog_search_current_v2" TO "portal_public_executor";

GRANT SELECT("modified_at") ON TABLE "private"."portal_catalog_search_current_v2" TO "portal_public_executor";

GRANT SELECT("card") ON TABLE "private"."portal_catalog_search_current_v2" TO "portal_public_executor";

GRANT SELECT("document") ON TABLE "private"."portal_catalog_search_current_v2" TO "portal_public_executor";
