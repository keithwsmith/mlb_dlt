-- =============================================================
-- Audit test: dim_position — compare_column_values
-- Validates that position_name in dim_position matches
-- the MAX(position_name) logic applied across the three
-- unioned sources (mlbplayers, player_stats, rosters).
-- =============================================================

{%- set source_query -%}
    WITH all_positions AS (
        SELECT
            primary_position__code      AS position_code,
            primary_position__name      AS position_name
        FROM {{ source('dw', 'mlbplayers') }}
        WHERE primary_position__code IS NOT NULL

        UNION

        SELECT
            position__code,
            position__name
        FROM {{ source('dw', 'player_stats') }}
        WHERE position__code IS NOT NULL

        UNION

        SELECT
            position__code,
            position__name
        FROM {{ source('dw', 'rosters') }}
        WHERE position__code IS NOT NULL
    ),
    deduped AS (
        SELECT
            position_code,
            MAX(position_name) AS position_name
        FROM all_positions
        GROUP BY position_code
    )
    SELECT position_code, position_name
    FROM deduped
{%- endset -%}

{%- set model_query -%}
    SELECT
        position_code,
        position_name
    FROM {{ ref('dim_position') }}
{%- endset -%}

{{ audit_helper.compare_column_values(
    a_query           = model_query,
    b_query           = source_query,
    primary_key       = 'position_code',
    column_to_compare = 'position_name'
) }}
