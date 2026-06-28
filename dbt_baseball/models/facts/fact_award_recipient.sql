-- =============================================================
-- dbt model: fact_award_recipient
-- One row per player + award + season.
-- Joins to dim_award (what) and dim_award_recipient (who) to
-- resolve surrogate keys.
--
-- Materialization: incremental (merge on natural key)
-- =============================================================

{{
    config(
        materialized    = 'incremental',
        alias           = 'fact_award_recipient',
        unique_key      = ['award_id', 'season', 'player_id']
    )
}}

WITH stg AS (
    SELECT
        award_id,
        TRY_CAST(award_date AS DATE)    AS award_date,
        TRY_CAST(season     AS INT)     AS season,
        player_id,
        team_id,
        notes,
        _dlt_load_id
    FROM {{ source('dw', 'award_recipients') }}
    WHERE award_id  IS NOT NULL
      AND player_id IS NOT NULL
),

da AS (
    SELECT award_key, award_id
    FROM {{ ref('dim_award') }}
),

dr AS (
    SELECT player_id
    FROM {{ ref('dim_award_recipient') }}
)

SELECT
    da.award_key,
    s.award_id,
    s.player_id,
    s.season,
    s.award_date,
    s.team_id,
    s.notes   AS notes
FROM stg s
LEFT JOIN da
    ON s.award_id = da.award_id
LEFT JOIN dr
    ON s.player_id = dr.player_id