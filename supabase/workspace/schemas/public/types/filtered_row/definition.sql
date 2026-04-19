CREATE TYPE "public"."filtered_row" AS (
	"id" "uuid",
	"embedding" "extensions"."vector"(1536)
);

ALTER TYPE "public"."filtered_row" OWNER TO "postgres";
