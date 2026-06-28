-- =============================================================
-- Audit test: dim_games — compare_row_counts
-- dim_games is a 1:1 transformation of dw.games, so row
-- counts must match.
-- =============================================================

{{ audit_helper.compare_row_counts(
    a_relation = ref('dim_games'),
    b_relation = source('dw', 'games')
) }}
