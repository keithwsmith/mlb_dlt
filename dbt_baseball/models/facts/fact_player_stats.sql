{{
    config(
        materialized='incremental',
        unique_key='_dlt_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH source_stats AS (
    SELECT
        -- Player and team identification
        player__id AS player_id,
        team__id AS team_id,
        season,
        
        -- Common fields
        stat__age AS age,
        stat__games_played AS games_played,
        stat__games_started AS games_started,
        
        -- Batting stats
        stat__at_bats AS at_bats,
        stat__runs AS runs,
        stat__hits AS hits,
        stat__doubles AS doubles,
        stat__triples AS triples,
        stat__home_runs AS home_runs,
        stat__rbi AS rbi,
        stat__stolen_bases AS stolen_bases,
        stat__caught_stealing AS caught_stealing,
        stat__base_on_balls AS walks,
        stat__strike_outs AS strikeouts,
		TRY_CAST(stat__avg AS DECIMAL(5,3)) AS batting_average,
		TRY_CAST(stat__obp AS DECIMAL(5,3)) AS on_base_percentage,
		TRY_CAST(stat__slg AS DECIMAL(5,3)) AS slugging_percentage,
		TRY_CAST(stat__ops AS DECIMAL(5,3)) AS ops,
        
        -- Pitching stats
        stat__wins AS wins,
        stat__losses AS losses,
		TRY_CAST(stat__era           AS DECIMAL(6,2))  AS era,
		TRY_CAST(stat__hits_per9_inn AS DECIMAL(6,2))  AS hits_per9_inn,
		TRY_CAST(stat__whip          AS DECIMAL(6,3))  AS whip,
		TRY_CAST(stat__fielding      AS DECIMAL(5,3))  AS fielding_percentage,
        stat__games_finished AS games_finished,
        stat__saves AS saves,
        stat__save_opportunities AS save_opportunities,
        stat__holds AS holds,
        stat__innings_pitched AS innings_pitched,
        stat__earned_runs AS earned_runs,
        stat__strike_outs AS strikeouts_pitched,
        CAST(NULL AS BIGINT) AS hits_allowed,
        CAST(NULL AS BIGINT) AS runs_allowed,
        CAST(NULL AS BIGINT) AS home_runs_allowed,
        CAST(NULL AS BIGINT) AS walks_allowed,
        
        -- Fielding stats
        position__code AS position_code,
        stat__games AS games_at_position,
        stat__put_outs AS putouts,
        stat__assists AS assists,
        stat__errors AS errors,
        stat__double_plays AS double_plays,
        
        -- Source fields
        _dlt_load_id,
        _dlt_id
        
    FROM {{ source('dw', 'player_stats') }}
    {% if is_incremental() %}
    WHERE _dlt_load_id > (SELECT MAX(_dlt_load_id) FROM {{ this }})
    {% endif %}
)

SELECT
    -- Dimension keys
    -- FIX: these were previously ps.player_id/ps.team_id/ps.season (raw
    -- natural values) despite being named like resolved surrogate keys,
    -- while the LEFT JOINs to dim_player/dim_teams/dim_season below sat
    -- completely unused. Same bug class as fact_games' home_team_key
    -- issue. Now genuinely selecting each dimension's real surrogate key.
    dp.player_version_key AS player_key,
    dt.team_key AS team_key,
    ds.season_key AS season_key,
    
    -- Common fields
    ps.season,
    ps.age,
    ps.games_played,
    ps.games_started,
    
    -- Batting stats
    ps.at_bats,
    ps.runs,
    ps.hits,
    ps.doubles,
    ps.triples,
    ps.home_runs,
    ps.rbi,
    ps.stolen_bases,
    ps.caught_stealing,
    ps.walks,
    ps.strikeouts,
    ps.batting_average,
    ps.on_base_percentage,
    ps.slugging_percentage,
    ps.ops,
    
    -- Pitching stats
    ps.wins,
    ps.losses,
    ps.era,
    ps.games_finished,
    ps.saves,
    ps.save_opportunities,
    ps.holds,
    ps.innings_pitched,
    ps.hits_per9_inn,
    ps.hits_allowed,
    ps.runs_allowed,
    ps.earned_runs,
    ps.home_runs_allowed,
    ps.walks_allowed,
    ps.strikeouts_pitched,
    ps.whip,
    
    -- Fielding stats
    ps.position_code,
    ps.games_at_position,
    ps.putouts,
    ps.assists,
    ps.errors,
    ps.double_plays,
    ps.fielding_percentage,
    
    -- Advanced metrics (placeholder for future calculation)
    CAST(NULL AS FLOAT) AS war,
    
    -- Source fields
    ps._dlt_load_id,
    ps._dlt_id,
    
    -- Audit
    GETDATE() AS created_at,
    GETDATE() AS updated_at
    
FROM source_stats ps

-- Join to player dimension
LEFT JOIN {{ ref('dim_player') }} dp
    ON ps.player_id = dp.player_id
    AND dp.is_current = 1
    
-- Join to team dimension
LEFT JOIN {{ ref('dim_teams') }} dt
    ON ps.team_id = dt.team_id
    AND ps.season = dt.season
    
    
-- Join to season dimension
LEFT JOIN {{ ref('dim_season') }} ds
    ON ps.season = ds.season