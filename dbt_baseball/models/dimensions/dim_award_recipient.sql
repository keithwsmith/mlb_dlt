-- =============================================================
-- dbt model: dim_award_recipient
-- One row per unique player who has received an award.
-- Keeps the most recent attribute values (name, position) via
-- ROW_NUMBER on season DESC.
--
-- Materialization: table (full refresh)
-- =============================================================
{{
    config(
        materialized = 'table',
        alias        = 'dim_award_recipient'
    )
}}

WITH ranked AS (
    SELECT
        player_id,
        player_name,
        position_code,
        position_name,
        position_type,
        position_abbreviation,
		COALESCE(NULLIF(LTRIM(RTRIM(team_id)), ''), '0') AS team_id,
        award_id,
        award_name,
        award_date,
        season,
        notes,
        _dlt_load_id,
        _dlt_id,
        ROW_NUMBER() OVER (
            PARTITION BY player_id
            ORDER BY
                CAST(season AS INT) DESC,
                _dlt_load_id DESC
        ) AS rn
    FROM {{ source('dw', 'award_recipients') }}
    WHERE player_id IS NOT NULL
)

SELECT
    CAST(player_id              AS BIGINT)         AS player_id,
    CAST(player_name            AS NVARCHAR(500))  AS player_name,
    CAST(position_code          AS NVARCHAR(50))   AS position_code,
    CAST(position_name          AS NVARCHAR(200))  AS position_name,
    CAST(position_type          AS NVARCHAR(200))  AS position_type,
    CAST(position_abbreviation  AS NVARCHAR(50))   AS position_abbreviation,
    CAST(team_id                AS BIGINT)         AS team_id,
    CAST(award_id               AS NVARCHAR(200))  AS award_id,
    CAST(award_name             AS NVARCHAR(500))  AS award_name,
    CAST(award_date             AS NVARCHAR(50))   AS award_date,
    CAST(season                 AS NVARCHAR(10))   AS season,
    CAST(notes                  AS NVARCHAR(MAX))  AS notes,
    CAST(_dlt_load_id           AS NVARCHAR(200))  AS load_id,
    CAST(_dlt_id                AS NVARCHAR(900))  AS event_id
FROM ranked
WHERE rn = 1