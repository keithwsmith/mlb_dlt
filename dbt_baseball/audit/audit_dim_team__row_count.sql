-- =============================================================
-- Audit test: dim_team — compare_row_counts
-- 1:1 mapping from dw.teams (filtered to non-null id);
-- row counts should match.
-- =============================================================

{{ audit_helper.compare_row_counts(
    a_relation = ref('dim_team'),
    b_relation = source('dw', 'teams')
) }}
