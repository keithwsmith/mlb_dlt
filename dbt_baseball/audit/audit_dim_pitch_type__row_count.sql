-- =============================================================
-- Audit test: dim_pitch_type — compare_row_counts
-- 1:1 mapping from source; row counts should match.
-- =============================================================

{{ audit_helper.compare_row_counts(
    a_relation = ref('dim_pitch_type'),
    b_relation = source('dw', 'pitch_type')
) }}
