-- =============================================================
-- Audit test: dim_season — compare_row_counts
-- 1:1 mapping from source; row counts should match.
-- =============================================================

{{ audit_helper.compare_row_counts(
    a_relation = ref('dim_season'),
    b_relation = source('dw', 'seasons')
) }}
