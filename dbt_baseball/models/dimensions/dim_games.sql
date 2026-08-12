{{
    config(
        materialized='table',
        unique_key='game_pk',
        indexes=[
            {'columns': ['season']},
            {'columns': ['game_date']},
            {'columns': ['home_team_id']},
            {'columns': ['away_team_id']}
        ]
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('dw', 'games') }}
	where status__detailed_state NOT IN ('Cancelled','Postponed')
),

typed AS (
    SELECT
        -- PK
        CAST(game_pk AS BIGINT)                    AS game_pk,

        -- Core attributes
        TRY_CAST(season AS INT)                    AS season,
        CAST(game_type AS VARCHAR(10))             AS game_type,

        -- Dates (normalize everything)
        CAST(game_date AS DATETIME2)               AS game_datetime,
        CAST(game_date AS DATE)                    AS game_date,
        TRY_CAST(official_date AS DATE)            AS official_date,

        -- Status
        status__abstract_game_state                AS abstract_game_state,
        status__coded_game_state                   AS coded_game_state,
        status__detailed_state                     AS detailed_state,
        status__status_code                        AS status_code,
        status__start_time_tbd                     AS is_start_time_tbd,

        -- Teams (flatten + normalize naming)
        teams__home__team__id                      AS home_team_id,
		COALESCE(NULLIF(LTRIM(RTRIM(teams__home__team__name)), ''), '') AS home_team_name,
        teams__home__score                         AS home_score,
        teams__home__is_winner                     AS home_is_winner,

        teams__away__team__id                      AS away_team_id,
		COALESCE(NULLIF(LTRIM(RTRIM(teams__away__team__name)), ''), '') AS away_team_name,
        teams__away__score                         AS away_score,
        teams__away__is_winner                     AS away_is_winner,

        -- Venue
		COALESCE(NULLIF(LTRIM(RTRIM(venue__id)), ''), '') AS venue_id,
		COALESCE(NULLIF(LTRIM(RTRIM(venue__name)), ''), '') AS venue_name,
        -- Game metadata
        game_number,
        double_header,
        day_night,
        scheduled_innings,
        games_in_series,
        series_game_number,
        series_description,

        -- Flags
        is_tie,
        public_facing,

        -- Audit
        _dlt_load_id							AS load_id,
        _dlt_id

    FROM source
),

final AS (
    SELECT
        *,

        -- Derived fields (important for analytics)
        CASE
            WHEN home_score > away_score THEN home_team_id
            WHEN away_score > home_score THEN away_team_id
            ELSE NULL
        END AS winning_team_id,

        CASE
            WHEN home_score > away_score THEN away_team_id
            WHEN away_score > home_score THEN home_team_id
            ELSE NULL
        END AS losing_team_id,

        CASE
            WHEN home_score IS NOT NULL AND away_score IS NOT NULL
            THEN home_score - away_score
            ELSE NULL
        END AS run_diff,

        CASE
            WHEN detailed_state = 'Final' THEN 1
            ELSE 0
        END AS is_final,

        CASE
            WHEN game_type IN ('R') THEN 'Regular Season'
            WHEN game_type IN ('S') THEN 'Spring Training'
            WHEN game_type IN ('P') THEN 'Postseason'
            ELSE 'Other'
        END AS game_type_desc

    FROM typed
)

SELECT * FROM final