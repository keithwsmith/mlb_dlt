-- =============================================================
-- Audit test: dim_player — compare_queries (row count)
-- Validates that the number of current player records matches
-- the number of distinct players in the source.
-- =============================================================

{%- set source_query -%}
    SELECT
        id AS player_id,
        COUNT(*) AS row_count
    FROM {{ source('dw', 'mlbplayers') }}
    GROUP BY id
{%- endset -%}

{%- set model_query -%}
    SELECT
        player_id,
        COUNT(*) AS row_count
    FROM {{ ref('dim_player') }}
    WHERE is_current = 1
    GROUP BY player_id
{%- endset -%}

{{ audit_helper.compare_queries(
    a_query     = model_query,
    b_query     = source_query,
    primary_key = 'player_id'
) }}
