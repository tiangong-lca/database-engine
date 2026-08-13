CREATE TYPE "api"."filtered_row" AS (
	"id" "uuid",
	"embedding" "extensions"."vector"(1536)
);

ALTER TYPE "api"."filtered_row" OWNER TO "postgres";
