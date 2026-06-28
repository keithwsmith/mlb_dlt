-- =============================================================
-- Audit test: dim_venue — compare_row_counts
-- 1:1 mapping; row counts should match.
-- =============================================================

{{ audit_helper.compare_row_counts(
    a_relation = ref('dim_venue'),
    b_relation = source('dw', 'venues')
) }}
