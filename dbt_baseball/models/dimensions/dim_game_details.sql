{{
    config(
        materialized='incremental',
        unique_key='game_pk'
    )
}}

WITH final AS (
    SELECT
        game_pk,

        -- Weather
        weather_condition,
        weather_temp,
        weather_wind,

        -- Game info
        attendance,
        first_pitch,
        game_duration_minutes,

        -- Decisions
        winning_pitcher_id,
        winning_pitcher_name,
        losing_pitcher_id,
        losing_pitcher_name,
        save_pitcher_id,
        save_pitcher_name,

        -- Home batting
        home_runs,
        home_hits,
        home_doubles,
        home_triples,
        home_home_runs,
        home_rbi,
        home_stolen_bases,
        home_strikeouts,
        home_walks,
        home_left_on_base,

        -- Away batting
        away_runs,
        away_hits,
        away_doubles,
        away_triples,
        away_home_runs,
        away_rbi,
        away_stolen_bases,
        away_strikeouts,
        away_walks,
        away_left_on_base,

        -- Home pitching
        home_pitching_strikeouts,
        home_pitching_walks,
        home_pitching_hits_allowed,
        home_pitching_runs_allowed,
        home_pitching_earned_runs,
        home_pitching_home_runs_allowed,

        -- Away pitching
        away_pitching_strikeouts,
        away_pitching_walks,
        away_pitching_hits_allowed,
        away_pitching_runs_allowed,
        away_pitching_earned_runs,
        away_pitching_home_runs_allowed,

        -- Audit
        _dlt_load_id AS load_id,
        _dlt_id

    FROM {{ source('dw', 'game_details') }}

    {% if is_incremental() %}
        WHERE _dlt_load_id > (SELECT MAX(load_id) FROM {{ this }})
    {% endif %}
)

SELECT * FROM final