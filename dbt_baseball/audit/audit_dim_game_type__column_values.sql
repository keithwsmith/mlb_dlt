-- =============================================================
-- Audit test: dim_game_type — compare_column_values
-- Validates that game_type_code values from the source are
-- present and that the CASE-derived game_type_name is correct.
-- =============================================================

{%- set source_query -%}
    SELECT DISTINCT
        game_type AS game_type_code,
        CASE game_type
            WHEN 'R' THEN 'Regular Season'
            WHEN 'S' THEN 'Spring Training'
            WHEN 'P' THEN 'Postseason'
            WHEN 'D' THEN 'Division Series'
            WHEN 'L' THEN 'League Championship Series'
            WHEN 'W' THEN 'World Series'
            WHEN 'F' THEN 'Wild Card'
            WHEN 'A' THEN 'All-Star Game'
            WHEN 'E' THEN 'Exhibition'
            ELSE 'Unknown'
        END AS game_type_name
    FROM {{ source('dw', 'games') }}
    WHERE game_type IS NOT NULL
{%- endset -%}

{%- set model_query -%}
    SELECT
        game_type_code,
        game_type_name
    FROM {{ ref('dim_game_type') }}
{%- endset -%}

{{ audit_helper.compare_column_values(
    a_query           = model_query,
    b_query           = source_query,
    primary_key       = 'game_type_code',
    column_to_compare = 'game_type_name'
) }}
