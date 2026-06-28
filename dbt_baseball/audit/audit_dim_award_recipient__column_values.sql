-- =============================================================
-- Audit test: dim_award_recipient — compare_column_values
-- Spot-check that player_name matches the most recent source
-- record per player.
-- =============================================================

{%- set source_query -%}
    WITH ranked AS (
        SELECT
            player_id,
            player_name,
            ROW_NUMBER() OVER (
                PARTITION BY player_id
                ORDER BY CAST(season AS INT) DESC, _dlt_load_id DESC
            ) AS rn
        FROM {{ source('dw', 'award_recipients') }}
        WHERE player_id IS NOT NULL
    )
    SELECT
        CAST(player_id   AS BIGINT)       AS player_id,
        CAST(player_name AS NVARCHAR(500)) AS player_name
    FROM ranked
    WHERE rn = 1
{%- endset -%}

{%- set model_query -%}
    SELECT
        player_id,
        player_name
    FROM {{ ref('dim_award_recipient') }}
{%- endset -%}

{{ audit_helper.compare_column_values(
    a_query           = model_query,
    b_query           = source_query,
    primary_key       = 'player_id',
    column_to_compare = 'player_name'
) }}
