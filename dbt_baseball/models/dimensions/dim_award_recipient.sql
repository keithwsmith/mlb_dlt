-- =============================================================
-- dbt model: dim_award_recipient
-- One row per unique (player, award, season) combination -- a
-- player CAN have multiple rows here if they won different
-- awards in the same season, or won awards across different
-- seasons. Dedup below is scoped to (player_id, award_id,
-- season) ONLY, to collapse re-ingested duplicates of the exact
-- same award record (multiple dlt loads of the same event), not
-- to collapse genuinely distinct awards.
--
-- Surrogate key (recipient_key), generated from the grain columns
-- (player_id, award_id, season), is this model's unique_key for
-- incremental merging.
--
-- Joins to dim_award on award_id to pull award_key -- the proper
-- FK for downstream consumers (e.g. the Cube semantic layer) to
-- join through, rather than joining on raw award_id/award_name
-- text. award_name is pulled FROM dim_award (not the raw source)
-- so every row referencing the same award reports an identical
-- name, rather than whatever text happened to be on that specific
-- source event.
--
-- Materialization: incremental (merge on recipient_key), using
-- _dlt_load_id to detect new/changed source rows -- same pattern
-- as dim_game_details.
--
-- ASSUMPTIONS TO VERIFY:
-- 1. incremental_strategy = 'merge' requires your adapter/dbt
--    version to support MERGE against this warehouse. If merge
--    isn't available, switch to incremental_strategy = 'delete+insert'.
-- 2. _dlt_load_id is treated as sortable via simple string '>'
--    comparison, matching the pattern used elsewhere in this
--    project -- holds as long as dlt's load ids stay in a
--    consistent, monotonically-increasing string format.
-- 3. This incremental filter only picks up rows with a load_id
--    greater than what's already in the table. An out-of-order
--    historical backfill landing with a lower load_id would be
--    missed -- run --full-refresh after any such backfill.
-- 4. LEFT JOIN to dim_award (rather than INNER) is defensive --
--    dim_award is `select distinct award_id from this same source
--    table`, so every award_id here should always have a match by
--    construction, but LEFT JOIN means a mismatch surfaces as a
--    NULL award_key instead of silently dropping the recipient
--    row. Worth adding a not_null test on award_key in schema.yml
--    to catch it if this assumption ever breaks.
-- =============================================================
{{
    config(
        materialized         = 'incremental',
        unique_key            = 'recipient_key',
        incremental_strategy  = 'merge',
        alias                 = 'dim_award_recipient'
    )
}}

WITH source_rows AS (
    SELECT
        player_id,
        player_name,
        position_code,
        position_name,
        position_type,
        position_abbreviation,
        COALESCE(NULLIF(LTRIM(RTRIM(team_id)), ''), '0') AS team_id,
        award_id,
        award_date,
        season,
        notes,
        _dlt_load_id,
        _dlt_id
    FROM {{ source('dw', 'award_recipients') }}
    WHERE player_id IS NOT NULL

    {% if is_incremental() %}
    AND _dlt_load_id > (SELECT MAX(load_id) FROM {{ this }})
    {% endif %}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY player_id, award_id, season
            ORDER BY _dlt_load_id DESC
        ) AS rn
    FROM source_rows
),

deduped AS (
    SELECT *
    FROM ranked
    WHERE rn = 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['deduped.player_id', 'deduped.award_id', 'deduped.season']) }}
                                                         AS recipient_key,
    CAST(deduped.player_id             AS BIGINT)        AS player_id,
    CAST(deduped.player_name           AS NVARCHAR(500)) AS player_name,
    CAST(deduped.position_code         AS NVARCHAR(50))  AS position_code,
    CAST(deduped.position_name         AS NVARCHAR(200)) AS position_name,
    CAST(deduped.position_type         AS NVARCHAR(200)) AS position_type,
    CAST(deduped.position_abbreviation AS NVARCHAR(50))  AS position_abbreviation,
    CAST(deduped.team_id               AS BIGINT)        AS team_id,
    da.award_key,
    CAST(deduped.award_id              AS NVARCHAR(200)) AS award_id,
    CAST(da.award_name                 AS NVARCHAR(500)) AS award_name,
    CAST(deduped.award_date            AS NVARCHAR(50))  AS award_date,
    CAST(deduped.season                AS NVARCHAR(10))  AS season,
    CAST(deduped.notes                 AS NVARCHAR(MAX)) AS notes,
    CAST(deduped._dlt_load_id          AS NVARCHAR(200)) AS load_id,
    CAST(deduped._dlt_id               AS NVARCHAR(900)) AS event_id
FROM deduped
LEFT JOIN {{ ref('dim_award') }} AS da
    ON deduped.award_id = da.award_id