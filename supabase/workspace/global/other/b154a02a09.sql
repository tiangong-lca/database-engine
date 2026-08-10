COMMENT ON CONSTRAINT "reviews_kind_target_v3_chk" ON "private"."reviews" IS 'Root and Reference Reviews require target identity and checksum; no Root/Reference relationship fields are persisted.';
