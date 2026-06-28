-- =============================================================
-- Audit test: dim_draft — compare_row_counts
-- dim_draft is a 1:1 pass-through of dw.draft, so row counts
-- must match exactly.
-- =============================================================

{{ audit_helper.compare_row_counts(
    a_relation = ref('dim_draft'),
    b_relation = source('dw', 'draft')
) }}
