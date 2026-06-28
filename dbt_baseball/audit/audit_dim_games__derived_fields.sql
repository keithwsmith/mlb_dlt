-- =============================================================
-- Audit test: dim_games — compare_queries (derived fields)
-- Validates that derived columns (winning_team_id, run_diff,
-- is_final) are calculated correctly from the base columns.
-- =============================================================

{%- set expected_query -%}
    SELECT
        game_pk,
        CASE
            WHEN home_score > away_score THEN home_team_id
            WHEN away_score > home_score THEN away_team_id
            ELSE NULL
        END AS winning_team_id,
        CASE
            WHEN home_score IS NOT NULL AND away_score IS NOT NULL
            THEN home_score - away_score
            ELSE NULL
        END AS run_diff,
        CASE
            WHEN detailed_state = 'Final' THEN 1
            ELSE 0
        END AS is_final
    FROM {{ ref('dim_games') }}
{%- endset -%}

{%- set model_query -%}
    SELECT
        game_pk,
        winning_team_id,
        run_diff,
        is_final
    FROM {{ ref('dim_games') }}
{%- endset -%}

{{ audit_helper.compare_queries(
    a_query     = model_query,
    b_query     = expected_query,
    primary_key = 'game_pk'
) }}
