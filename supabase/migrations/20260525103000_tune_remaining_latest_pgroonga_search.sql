CREATE INDEX IF NOT EXISTS "sources_public_json_pgroonga_idx"
ON "public"."sources" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2")
WHERE "state_code" = 100;

CREATE INDEX IF NOT EXISTS "sources_co_json_pgroonga_idx"
ON "public"."sources" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2")
WHERE "state_code" = 200;

CREATE INDEX IF NOT EXISTS "contacts_public_json_pgroonga_idx"
ON "public"."contacts" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2")
WHERE "state_code" = 100;

CREATE INDEX IF NOT EXISTS "contacts_co_json_pgroonga_idx"
ON "public"."contacts" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2")
WHERE "state_code" = 200;

CREATE INDEX IF NOT EXISTS "unitgroups_public_json_pgroonga_idx"
ON "public"."unitgroups" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2")
WHERE "state_code" = 100;

CREATE INDEX IF NOT EXISTS "unitgroups_co_json_pgroonga_idx"
ON "public"."unitgroups" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2")
WHERE "state_code" = 200;

CREATE INDEX IF NOT EXISTS "flowproperties_public_json_pgroonga_idx"
ON "public"."flowproperties" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2")
WHERE "state_code" = 100;

CREATE INDEX IF NOT EXISTS "flowproperties_co_json_pgroonga_idx"
ON "public"."flowproperties" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2")
WHERE "state_code" = 200;

CREATE INDEX IF NOT EXISTS "flows_co_json_pgroonga_idx"
ON "public"."flows" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2")
WHERE "state_code" = 200;

CREATE INDEX IF NOT EXISTS "lifecyclemodels_public_json_pgroonga_idx"
ON "public"."lifecyclemodels" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2")
WHERE "state_code" = 100;

CREATE INDEX IF NOT EXISTS "lifecyclemodels_co_json_pgroonga_idx"
ON "public"."lifecyclemodels" USING "pgroonga" ("json" "extensions"."pgroonga_jsonb_full_text_search_ops_v2")
WHERE "state_code" = 200;

CREATE OR REPLACE FUNCTION "public"."pgroonga_search_sources_latest"(
  "query_text" text,
  "filter_condition" jsonb DEFAULT '{}'::jsonb,
  "page_size" bigint DEFAULT 10,
  "page_current" bigint DEFAULT 1,
  "data_source" text DEFAULT 'tg'::text,
  "this_user_id" text DEFAULT ''::text,
  "team_id_filter" uuid DEFAULT NULL::uuid,
  "state_code_filter" integer DEFAULT NULL::integer
) RETURNS TABLE(
  "rank" bigint,
  "id" uuid,
  "json" jsonb,
  "version" character(9),
  "modified_at" timestamp with time zone,
  "team_id" uuid,
  "total_count" bigint
)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_this_user_id uuid;
  filter_condition_jsonb jsonb;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);

  IF data_source = 'tg' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.sources f
        WHERE f.state_code = 100
          AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.sources f2
          WHERE f2.state_code = 100
            AND f2.id = matched_ids.id
            AND (team_id_filter IS NULL OR f2.team_id = team_id_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'co' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.sources f
        WHERE f.state_code = 200
          AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.sources f2
          WHERE f2.state_code = 200
            AND f2.id = matched_ids.id
            AND (team_id_filter IS NULL OR f2.team_id = team_id_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'my' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.sources f
        WHERE normalized_this_user_id IS NOT NULL
          AND f.user_id = normalized_this_user_id
          AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.sources f2
          WHERE normalized_this_user_id IS NOT NULL
            AND f2.user_id = normalized_this_user_id
            AND f2.id = matched_ids.id
            AND (state_code_filter IS NULL OR f2.state_code = state_code_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'te' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.sources f
        WHERE team_id_filter IS NOT NULL
          AND f.team_id = team_id_filter
          AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.sources f2
          WHERE team_id_filter IS NOT NULL
            AND f2.team_id = team_id_filter
            AND f2.id = matched_ids.id
            AND (state_code_filter IS NULL OR f2.state_code = state_code_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;
END;
$$;

ALTER FUNCTION "public"."pgroonga_search_sources_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."pgroonga_search_sources_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_sources_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_sources_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer) TO "service_role";

CREATE OR REPLACE FUNCTION "public"."pgroonga_search_contacts_latest"(
  "query_text" text,
  "filter_condition" jsonb DEFAULT '{}'::jsonb,
  "page_size" bigint DEFAULT 10,
  "page_current" bigint DEFAULT 1,
  "data_source" text DEFAULT 'tg'::text,
  "this_user_id" text DEFAULT ''::text,
  "team_id_filter" uuid DEFAULT NULL::uuid,
  "state_code_filter" integer DEFAULT NULL::integer
) RETURNS TABLE(
  "rank" bigint,
  "id" uuid,
  "json" jsonb,
  "version" character(9),
  "modified_at" timestamp with time zone,
  "team_id" uuid,
  "total_count" bigint
)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_this_user_id uuid;
  filter_condition_jsonb jsonb;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);

  IF data_source = 'tg' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.contacts f
        WHERE f.state_code = 100
          AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.contacts f2
          WHERE f2.state_code = 100
            AND f2.id = matched_ids.id
            AND (team_id_filter IS NULL OR f2.team_id = team_id_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'co' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.contacts f
        WHERE f.state_code = 200
          AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.contacts f2
          WHERE f2.state_code = 200
            AND f2.id = matched_ids.id
            AND (team_id_filter IS NULL OR f2.team_id = team_id_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'my' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.contacts f
        WHERE normalized_this_user_id IS NOT NULL
          AND f.user_id = normalized_this_user_id
          AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.contacts f2
          WHERE normalized_this_user_id IS NOT NULL
            AND f2.user_id = normalized_this_user_id
            AND f2.id = matched_ids.id
            AND (state_code_filter IS NULL OR f2.state_code = state_code_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'te' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.contacts f
        WHERE team_id_filter IS NOT NULL
          AND f.team_id = team_id_filter
          AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.contacts f2
          WHERE team_id_filter IS NOT NULL
            AND f2.team_id = team_id_filter
            AND f2.id = matched_ids.id
            AND (state_code_filter IS NULL OR f2.state_code = state_code_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;
END;
$$;

ALTER FUNCTION "public"."pgroonga_search_contacts_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."pgroonga_search_contacts_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_contacts_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_contacts_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer) TO "service_role";

CREATE OR REPLACE FUNCTION "public"."pgroonga_search_unitgroups_latest"(
  "query_text" text,
  "filter_condition" jsonb DEFAULT '{}'::jsonb,
  "page_size" bigint DEFAULT 10,
  "page_current" bigint DEFAULT 1,
  "data_source" text DEFAULT 'tg'::text,
  "this_user_id" text DEFAULT ''::text,
  "team_id_filter" uuid DEFAULT NULL::uuid,
  "state_code_filter" integer DEFAULT NULL::integer
) RETURNS TABLE(
  "rank" bigint,
  "id" uuid,
  "json" jsonb,
  "version" character(9),
  "modified_at" timestamp with time zone,
  "team_id" uuid,
  "total_count" bigint
)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_this_user_id uuid;
  filter_condition_jsonb jsonb;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);

  IF data_source = 'tg' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.unitgroups f
        WHERE f.state_code = 100
          AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.unitgroups f2
          WHERE f2.state_code = 100
            AND f2.id = matched_ids.id
            AND (team_id_filter IS NULL OR f2.team_id = team_id_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'co' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.unitgroups f
        WHERE f.state_code = 200
          AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.unitgroups f2
          WHERE f2.state_code = 200
            AND f2.id = matched_ids.id
            AND (team_id_filter IS NULL OR f2.team_id = team_id_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'my' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.unitgroups f
        WHERE normalized_this_user_id IS NOT NULL
          AND f.user_id = normalized_this_user_id
          AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.unitgroups f2
          WHERE normalized_this_user_id IS NOT NULL
            AND f2.user_id = normalized_this_user_id
            AND f2.id = matched_ids.id
            AND (state_code_filter IS NULL OR f2.state_code = state_code_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'te' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.unitgroups f
        WHERE team_id_filter IS NOT NULL
          AND f.team_id = team_id_filter
          AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.unitgroups f2
          WHERE team_id_filter IS NOT NULL
            AND f2.team_id = team_id_filter
            AND f2.id = matched_ids.id
            AND (state_code_filter IS NULL OR f2.state_code = state_code_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;
END;
$$;

ALTER FUNCTION "public"."pgroonga_search_unitgroups_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."pgroonga_search_unitgroups_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_unitgroups_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_unitgroups_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer) TO "service_role";

CREATE OR REPLACE FUNCTION "public"."pgroonga_search_flowproperties_latest"(
  "query_text" text,
  "filter_condition" jsonb DEFAULT '{}'::jsonb,
  "page_size" bigint DEFAULT 10,
  "page_current" bigint DEFAULT 1,
  "data_source" text DEFAULT 'tg'::text,
  "this_user_id" text DEFAULT ''::text,
  "team_id_filter" uuid DEFAULT NULL::uuid,
  "state_code_filter" integer DEFAULT NULL::integer
) RETURNS TABLE(
  "rank" bigint,
  "id" uuid,
  "json" jsonb,
  "version" character(9),
  "modified_at" timestamp with time zone,
  "team_id" uuid,
  "total_count" bigint
)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_this_user_id uuid;
  filter_condition_jsonb jsonb;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);

  IF data_source = 'tg' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.flowproperties f
        WHERE f.state_code = 100
          AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.flowproperties f2
          WHERE f2.state_code = 100
            AND f2.id = matched_ids.id
            AND (team_id_filter IS NULL OR f2.team_id = team_id_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'co' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.flowproperties f
        WHERE f.state_code = 200
          AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.flowproperties f2
          WHERE f2.state_code = 200
            AND f2.id = matched_ids.id
            AND (team_id_filter IS NULL OR f2.team_id = team_id_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'my' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.flowproperties f
        WHERE normalized_this_user_id IS NOT NULL
          AND f.user_id = normalized_this_user_id
          AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.flowproperties f2
          WHERE normalized_this_user_id IS NOT NULL
            AND f2.user_id = normalized_this_user_id
            AND f2.id = matched_ids.id
            AND (state_code_filter IS NULL OR f2.state_code = state_code_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'te' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.flowproperties f
        WHERE team_id_filter IS NOT NULL
          AND f.team_id = team_id_filter
          AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.flowproperties f2
          WHERE team_id_filter IS NOT NULL
            AND f2.team_id = team_id_filter
            AND f2.id = matched_ids.id
            AND (state_code_filter IS NULL OR f2.state_code = state_code_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;
END;
$$;

ALTER FUNCTION "public"."pgroonga_search_flowproperties_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flowproperties_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flowproperties_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flowproperties_latest"(text, jsonb, bigint, bigint, text, text, uuid, integer) TO "service_role";

CREATE OR REPLACE FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"(
  "query_text" text,
  "filter_condition" jsonb DEFAULT '{}'::jsonb,
  "order_by" jsonb DEFAULT '{}'::jsonb,
  "page_size" bigint DEFAULT 10,
  "page_current" bigint DEFAULT 1,
  "data_source" text DEFAULT 'tg'::text,
  "this_user_id" text DEFAULT ''::text,
  "team_id_filter" uuid DEFAULT NULL::uuid,
  "state_code_filter" integer DEFAULT NULL::integer
) RETURNS TABLE(
  "rank" bigint,
  "id" uuid,
  "json" jsonb,
  "version" character(9),
  "modified_at" timestamp with time zone,
  "team_id" uuid,
  "total_count" bigint
)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_this_user_id uuid;
  filter_condition_jsonb jsonb;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);

  IF data_source = 'tg' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT l.id, max(pgroonga_score(l.tableoid, l.ctid)) AS search_score
        FROM public.lifecyclemodels l
        WHERE l.state_code = 100
          AND (team_id_filter IS NULL OR l.team_id = team_id_filter)
          AND l.json @> filter_condition_jsonb
          AND l.json &@~ query_text
        GROUP BY l.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT l2.json, l2.version, l2.modified_at, l2.team_id
          FROM public.lifecyclemodels l2
          WHERE l2.state_code = 100
            AND l2.id = matched_ids.id
            AND (team_id_filter IS NULL OR l2.team_id = team_id_filter)
          ORDER BY l2.version DESC, l2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'co' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT l.id, max(pgroonga_score(l.tableoid, l.ctid)) AS search_score
        FROM public.lifecyclemodels l
        WHERE l.state_code = 200
          AND (team_id_filter IS NULL OR l.team_id = team_id_filter)
          AND l.json @> filter_condition_jsonb
          AND l.json &@~ query_text
        GROUP BY l.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT l2.json, l2.version, l2.modified_at, l2.team_id
          FROM public.lifecyclemodels l2
          WHERE l2.state_code = 200
            AND l2.id = matched_ids.id
            AND (team_id_filter IS NULL OR l2.team_id = team_id_filter)
          ORDER BY l2.version DESC, l2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'my' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT l.id, max(pgroonga_score(l.tableoid, l.ctid)) AS search_score
        FROM public.lifecyclemodels l
        WHERE normalized_this_user_id IS NOT NULL
          AND l.user_id = normalized_this_user_id
          AND (state_code_filter IS NULL OR l.state_code = state_code_filter)
          AND l.json @> filter_condition_jsonb
          AND l.json &@~ query_text
        GROUP BY l.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT l2.json, l2.version, l2.modified_at, l2.team_id
          FROM public.lifecyclemodels l2
          WHERE normalized_this_user_id IS NOT NULL
            AND l2.user_id = normalized_this_user_id
            AND l2.id = matched_ids.id
            AND (state_code_filter IS NULL OR l2.state_code = state_code_filter)
          ORDER BY l2.version DESC, l2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'te' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT l.id, max(pgroonga_score(l.tableoid, l.ctid)) AS search_score
        FROM public.lifecyclemodels l
        WHERE team_id_filter IS NOT NULL
          AND l.team_id = team_id_filter
          AND (state_code_filter IS NULL OR l.state_code = state_code_filter)
          AND l.json @> filter_condition_jsonb
          AND l.json &@~ query_text
        GROUP BY l.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT l2.json, l2.version, l2.modified_at, l2.team_id
          FROM public.lifecyclemodels l2
          WHERE team_id_filter IS NOT NULL
            AND l2.team_id = team_id_filter
            AND l2.id = matched_ids.id
            AND (state_code_filter IS NULL OR l2.state_code = state_code_filter)
          ORDER BY l2.version DESC, l2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;
END;
$$;

ALTER FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_lifecyclemodels_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "service_role";

CREATE OR REPLACE FUNCTION "public"."pgroonga_search_flows_latest"(
  "query_text" text,
  "filter_condition" jsonb DEFAULT '{}'::jsonb,
  "order_by" jsonb DEFAULT '{}'::jsonb,
  "page_size" bigint DEFAULT 10,
  "page_current" bigint DEFAULT 1,
  "data_source" text DEFAULT 'tg'::text,
  "this_user_id" text DEFAULT ''::text,
  "team_id_filter" uuid DEFAULT NULL::uuid,
  "state_code_filter" integer DEFAULT NULL::integer
) RETURNS TABLE(
  "rank" bigint,
  "id" uuid,
  "json" jsonb,
  "version" character(9),
  "modified_at" timestamp with time zone,
  "team_id" uuid,
  "total_count" bigint
)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
DECLARE
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_this_user_id uuid;
  filter_condition_jsonb jsonb;
  flow_type text;
  flow_type_array text[];
  as_input boolean;
  classification_filter jsonb;
BEGIN
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_this_user_id := CASE
    WHEN coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      THEN btrim(this_user_id)::uuid
    ELSE NULL::uuid
  END;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);

  flow_type := nullif(btrim(filter_condition_jsonb->>'flowType'), '');
  IF flow_type IS NOT NULL THEN
    flow_type_array := string_to_array(flow_type, ',');
  ELSE
    flow_type_array := NULL;
  END IF;
  filter_condition_jsonb := filter_condition_jsonb - 'flowType';

  IF filter_condition_jsonb ? 'asInput' THEN
    as_input := nullif(btrim(filter_condition_jsonb->>'asInput'), '')::boolean;
  ELSE
    as_input := NULL;
  END IF;
  filter_condition_jsonb := filter_condition_jsonb - 'asInput';

  IF jsonb_typeof(filter_condition_jsonb->'classification') = 'array' THEN
    classification_filter := filter_condition_jsonb->'classification';
  ELSE
    classification_filter := '[]'::jsonb;
  END IF;
  filter_condition_jsonb := filter_condition_jsonb - 'classification';

  IF data_source = 'tg' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.flows f
        WHERE f.state_code = 100
          AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
          AND (
            flow_type IS NULL
            OR (f.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}') = ANY(flow_type_array)
          )
          AND (
            as_input IS NULL
            OR as_input = false
            OR NOT (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
          AND (
            jsonb_array_length(classification_filter) = 0
            OR EXISTS (
              SELECT 1
              FROM jsonb_array_elements(classification_filter) AS selected_class(item)
              WHERE
                (
                  selected_class.item->>'scope' = 'elementary'
                  AND EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(
                      CASE jsonb_typeof(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                        WHEN 'array' THEN f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}'
                        WHEN 'object' THEN jsonb_build_array(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                        ELSE '[]'::jsonb
                      END
                    ) AS category(item)
                    WHERE category.item->>'@catId' = selected_class.item->>'code'
                  )
                )
                OR (
                  selected_class.item->>'scope' = 'classification'
                  AND EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(
                      CASE jsonb_typeof(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                        WHEN 'array' THEN f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}'
                        WHEN 'object' THEN jsonb_build_array(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                        ELSE '[]'::jsonb
                      END
                    ) AS class_item(item)
                    WHERE class_item.item->>'@classId' = selected_class.item->>'code'
                  )
                )
            )
          )
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.flows f2
          WHERE f2.state_code = 100
            AND f2.id = matched_ids.id
            AND (team_id_filter IS NULL OR f2.team_id = team_id_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'co' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.flows f
        WHERE f.state_code = 200
          AND (team_id_filter IS NULL OR f.team_id = team_id_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
          AND (
            flow_type IS NULL
            OR (f.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}') = ANY(flow_type_array)
          )
          AND (
            as_input IS NULL
            OR as_input = false
            OR NOT (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
          AND (
            jsonb_array_length(classification_filter) = 0
            OR EXISTS (
              SELECT 1
              FROM jsonb_array_elements(classification_filter) AS selected_class(item)
              WHERE
                (
                  selected_class.item->>'scope' = 'elementary'
                  AND EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(
                      CASE jsonb_typeof(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                        WHEN 'array' THEN f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}'
                        WHEN 'object' THEN jsonb_build_array(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                        ELSE '[]'::jsonb
                      END
                    ) AS category(item)
                    WHERE category.item->>'@catId' = selected_class.item->>'code'
                  )
                )
                OR (
                  selected_class.item->>'scope' = 'classification'
                  AND EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(
                      CASE jsonb_typeof(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                        WHEN 'array' THEN f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}'
                        WHEN 'object' THEN jsonb_build_array(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                        ELSE '[]'::jsonb
                      END
                    ) AS class_item(item)
                    WHERE class_item.item->>'@classId' = selected_class.item->>'code'
                  )
                )
            )
          )
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.flows f2
          WHERE f2.state_code = 200
            AND f2.id = matched_ids.id
            AND (team_id_filter IS NULL OR f2.team_id = team_id_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'my' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.flows f
        WHERE normalized_this_user_id IS NOT NULL
          AND f.user_id = normalized_this_user_id
          AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
          AND (
            flow_type IS NULL
            OR (f.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}') = ANY(flow_type_array)
          )
          AND (
            as_input IS NULL
            OR as_input = false
            OR NOT (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
          AND (
            jsonb_array_length(classification_filter) = 0
            OR EXISTS (
              SELECT 1
              FROM jsonb_array_elements(classification_filter) AS selected_class(item)
              WHERE
                (
                  selected_class.item->>'scope' = 'elementary'
                  AND EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(
                      CASE jsonb_typeof(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                        WHEN 'array' THEN f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}'
                        WHEN 'object' THEN jsonb_build_array(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                        ELSE '[]'::jsonb
                      END
                    ) AS category(item)
                    WHERE category.item->>'@catId' = selected_class.item->>'code'
                  )
                )
                OR (
                  selected_class.item->>'scope' = 'classification'
                  AND EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(
                      CASE jsonb_typeof(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                        WHEN 'array' THEN f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}'
                        WHEN 'object' THEN jsonb_build_array(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                        ELSE '[]'::jsonb
                      END
                    ) AS class_item(item)
                    WHERE class_item.item->>'@classId' = selected_class.item->>'code'
                  )
                )
            )
          )
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.flows f2
          WHERE normalized_this_user_id IS NOT NULL
            AND f2.user_id = normalized_this_user_id
            AND f2.id = matched_ids.id
            AND (state_code_filter IS NULL OR f2.state_code = state_code_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;

  IF data_source = 'te' THEN
    RETURN QUERY
      WITH matched_ids AS (
        SELECT f.id, max(pgroonga_score(f.tableoid, f.ctid)) AS search_score
        FROM public.flows f
        WHERE team_id_filter IS NOT NULL
          AND f.team_id = team_id_filter
          AND (state_code_filter IS NULL OR f.state_code = state_code_filter)
          AND f.json @> filter_condition_jsonb
          AND f.json &@~ query_text
          AND (
            flow_type IS NULL
            OR (f.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}') = ANY(flow_type_array)
          )
          AND (
            as_input IS NULL
            OR as_input = false
            OR NOT (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
          AND (
            jsonb_array_length(classification_filter) = 0
            OR EXISTS (
              SELECT 1
              FROM jsonb_array_elements(classification_filter) AS selected_class(item)
              WHERE
                (
                  selected_class.item->>'scope' = 'elementary'
                  AND EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(
                      CASE jsonb_typeof(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                        WHEN 'array' THEN f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}'
                        WHEN 'object' THEN jsonb_build_array(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                        ELSE '[]'::jsonb
                      END
                    ) AS category(item)
                    WHERE category.item->>'@catId' = selected_class.item->>'code'
                  )
                )
                OR (
                  selected_class.item->>'scope' = 'classification'
                  AND EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(
                      CASE jsonb_typeof(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                        WHEN 'array' THEN f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}'
                        WHEN 'object' THEN jsonb_build_array(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                        ELSE '[]'::jsonb
                      END
                    ) AS class_item(item)
                    WHERE class_item.item->>'@classId' = selected_class.item->>'code'
                  )
                )
            )
          )
        GROUP BY f.id
      ),
      latest_rows AS (
        SELECT matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        FROM matched_ids
        JOIN LATERAL (
          SELECT f2.json, f2.version, f2.modified_at, f2.team_id
          FROM public.flows f2
          WHERE team_id_filter IS NOT NULL
            AND f2.team_id = team_id_filter
            AND f2.id = matched_ids.id
            AND (state_code_filter IS NULL OR f2.state_code = state_code_filter)
          ORDER BY f2.version DESC, f2.modified_at DESC
          LIMIT 1
        ) latest_row ON true
      ),
      counted_rows AS (
        SELECT latest_rows.*, count(*) OVER()::bigint AS total_count
        FROM latest_rows
      ),
      ranked_rows AS (
        SELECT rank() OVER (ORDER BY counted_rows.search_score DESC, counted_rows.modified_at DESC, counted_rows.id)::bigint AS rank, counted_rows.*
        FROM counted_rows
      )
      SELECT ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
      FROM ranked_rows
      ORDER BY ranked_rows.rank, ranked_rows.id
      LIMIT normalized_page_size
      OFFSET (normalized_page_current - 1) * normalized_page_size;
    RETURN;
  END IF;
END;
$$;

ALTER FUNCTION "public"."pgroonga_search_flows_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) OWNER TO "postgres";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "anon";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."pgroonga_search_flows_latest"(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) TO "service_role";
