ALTER TABLE public.lca_network_snapshots
    DROP CONSTRAINT IF EXISTS lca_network_snapshots_provider_rule_chk;

ALTER TABLE public.lca_network_snapshots
    ADD CONSTRAINT lca_network_snapshots_provider_rule_chk
    CHECK (
        provider_matching_rule = ANY (
            ARRAY[
                'strict_unique_provider'::text,
                'best_provider_strict'::text,
                'split_by_evidence'::text,
                'split_by_evidence_hybrid'::text,
                'split_equal'::text,
                'equal_split_multi_provider'::text,
                'custom_weighted_provider'::text,
                'split_by_process_volume'::text
            ]
        )
    );
