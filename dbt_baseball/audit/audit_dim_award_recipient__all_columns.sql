-- =============================================================
-- Audit test: dim_award_recipient — compare_all_columns
-- Validates that the most-recent-row-per-player logic produces
-- the correct attribute values.
-- =============================================================

{%- set source_query -%}
    WITH ranked AS (
        SELECT
            player_id,
            player_name,
            position_code,
            position_name,
            position_type,
            position_abbreviation,
            ROW_NUMBER() OVER (
                PARTITION BY player_id
                ORDER BY CAST(season AS INT) DESC, _dlt_load_id DESC
            ) AS rn
        FROM {{ source('dw', 'award_recipients') }}
        WHERE player_id IS NOT NULL
    )
    SELECT
        CAST(player_id             AS BIGINT)       AS player_id,
        CAST(player_name           AS NVARCHAR(500)) AS player_name,
        CAST(position_code         AS NVARCHAR(50))  AS position_code,
        CAST(position_name         AS NVARCHAR(200)) AS position_name,
        CAST(position_type         AS NVARCHAR(200)) AS position_type,
        CAST(position_abbreviation AS NVARCHAR(50))  AS position_abbreviation
    FROM ranked
    WHERE rn = 1
{%- endset -%}

{%- set model_query -%}
    SELECT
        player_id,
        player_name,
        position_code,
        position_name,
        position_type,
        position_abbreviation
    FROM {{ ref('dim_award_recipient') }}
{%- endset -%}

{{ audit_helper.compare_all_columns(
    a_query     = model_query,
    b_query     = source_query,
    primary_key = 'player_id'
) }}
