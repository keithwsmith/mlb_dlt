{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    GOLD: one row per player per regular season -- the batting line.

    Two silver sources, each used for what it is best at:
      - int_player_game_stats (official box score per game, regular season,
        final games): G, AB, R, H, 2B, 3B, HR, RBI, BB, SO, SB.
      - mart_batter_game (pitch-by-pitch): HBP and sacrifice flies (not in the
        box score), plate discipline and quality of contact. Filtered here to
        regular-season games so it covers the same games as the box score.

    RATES ARE RECOMPUTED FROM SUMMED COUNTS, never averaged from the per-game
    percentages in the mart: the average of 150 game batting averages is not
    the season batting average.

    PA = AB + BB + HBP + SF. HBP and SF come from pitch data, so they are 0
    for games loaded before mlb_load.py saved plate-appearance results; OBP is
    slightly low for those seasons. tracked_games shows how many of the
    player's games have pitch data.

    is_qualified: PA >= 3.1 per team game (MLB's batting-title rule), using
    the games played by the team the player finished the season with.
    ops_plus is NOT park-adjusted: 100 * (OBP/lgOBP + SLG/lgSLG - 1).
*/

with box as (

    select * from {{ ref('int_player_game_stats') }}
    where batting_at_bats > 0
       or batting_walks   > 0
       or batting_runs    > 0

),

regular_season_games as (

    select game_pk
    from {{ ref('dim_games') }}
    where game_type = 'R'
      and is_final  = 1

),

pitch_level as (

    select
        m.season,
        m.batter_id                                                         as player_id,
        count(*)                                                            as tracked_games,
        sum(m.hit_by_pitch)                                                 as hit_by_pitch,
        sum(m.sac_flies)                                                    as sac_flies,
        sum(m.pitches_seen)                                                 as pitches_seen,
        sum(m.swings)                                                       as swings,
        sum(m.whiffs)                                                       as whiffs,
        sum(m.chases)                                                       as chases,
        sum(m.out_of_zone_pitches)                                          as out_of_zone_pitches,
        sum(m.zone_swings)                                                  as zone_swings,
        sum(m.pitches_in_zone)                                              as pitches_in_zone,
        sum(m.batted_ball_events)                                           as batted_ball_events,
        sum(m.hard_hits)                                                    as hard_hits,
        sum(m.barrels)                                                      as barrels,
        sum(m.ground_balls)                                                 as ground_balls,
        sum(m.line_drives)                                                  as line_drives,
        sum(m.fly_balls)                                                    as fly_balls,
        -- per-game averages weighted by that game's batted balls
        sum(m.avg_exit_velo * m.batted_ball_events)
            / nullif(sum(case when m.avg_exit_velo is not null
                              then m.batted_ball_events end), 0)            as avg_exit_velo,
        max(m.max_exit_velo)                                                as max_exit_velo,
        sum(m.avg_launch_angle * m.batted_ball_events)
            / nullif(sum(case when m.avg_launch_angle is not null
                              then m.batted_ball_events end), 0)            as avg_launch_angle
    from {{ ref('mart_batter_game') }} m
    join regular_season_games g
        on g.game_pk = m.game_pk
    group by m.season, m.batter_id

),

last_team as (

    -- The team the player finished the season with (latest game).
    select season, player_id, team_id, team_name
    from (
        select
            season, player_id, team_id, team_name,
            row_number() over (
                partition by season, player_id
                order by game_date desc, game_pk desc
            ) as rn
        from box
    ) x
    where rn = 1

),

teams_played_for as (

    select season, player_id, count(distinct team_id) as teams_played_for
    from box
    group by season, player_id

),

team_games as (

    select season, team_id, count(*) as team_games
    from {{ ref('int_team_game_results') }}
    group by season, team_id

),

season_box as (

    select
        season,
        player_id,
        max(player_name)                                                    as player_name,
        count(*)                                                            as games,
        sum(batting_at_bats)                                                as at_bats,
        sum(batting_runs)                                                   as runs,
        sum(batting_hits)                                                   as hits,
        sum(batting_doubles)                                                as doubles,
        sum(batting_triples)                                                as triples,
        sum(batting_home_runs)                                              as home_runs,
        sum(batting_rbi)                                                    as rbi,
        sum(batting_walks)                                                  as walks,
        sum(batting_strikeouts)                                             as strikeouts,
        sum(batting_stolen_bases)                                           as stolen_bases,
        sum(batting_left_on_base)                                           as left_on_base
    from box
    group by season, player_id

),

combined as (

    select
        b.*,
        coalesce(p.hit_by_pitch, 0)                                         as hit_by_pitch,
        coalesce(p.sac_flies, 0)                                            as sac_flies,
        b.hits - b.doubles - b.triples - b.home_runs                        as singles,
        b.hits + b.doubles + 2 * b.triples + 3 * b.home_runs                as total_bases,
        b.at_bats + b.walks + coalesce(p.hit_by_pitch, 0)
                  + coalesce(p.sac_flies, 0)                                as plate_appearances,
        coalesce(p.tracked_games, 0)                                        as tracked_games,
        p.pitches_seen, p.swings, p.whiffs, p.chases, p.out_of_zone_pitches,
        p.zone_swings, p.pitches_in_zone, p.batted_ball_events, p.hard_hits,
        p.barrels, p.ground_balls, p.line_drives, p.fly_balls,
        p.avg_exit_velo, p.max_exit_velo, p.avg_launch_angle,
        lt.team_id                                                          as last_team_id,
        lt.team_name                                                        as last_team_name,
        tp.teams_played_for,
        tg.team_games
    from season_box b
    left join pitch_level p
        on p.season = b.season and p.player_id = b.player_id
    left join last_team lt
        on lt.season = b.season and lt.player_id = b.player_id
    left join teams_played_for tp
        on tp.season = b.season and tp.player_id = b.player_id
    left join team_games tg
        on tg.season = b.season and tg.team_id = lt.team_id

),

rates as (

    select
        c.*,
        cast(c.hits as float) / nullif(c.at_bats, 0)                        as avg_raw,
        cast(c.hits + c.walks + c.hit_by_pitch as float)
            / nullif(c.at_bats + c.walks + c.hit_by_pitch + c.sac_flies, 0) as obp_raw,
        cast(c.total_bases as float) / nullif(c.at_bats, 0)                 as slg_raw
    from combined c

)

select
    r.season,
    r.player_id,
    r.player_name,
    r.last_team_id,
    r.last_team_name,
    r.teams_played_for,

    r.games,
    r.plate_appearances,
    r.at_bats,
    r.runs,
    r.hits,
    r.singles,
    r.doubles,
    r.triples,
    r.home_runs,
    r.rbi,
    r.walks,
    r.strikeouts,
    r.hit_by_pitch,
    r.sac_flies,
    r.stolen_bases,
    r.total_bases,
    r.left_on_base,

    round(r.avg_raw, 3)                                                     as batting_avg,
    round(r.obp_raw, 3)                                                     as on_base_pct,
    round(r.slg_raw, 3)                                                     as slugging_pct,
    round(r.obp_raw + r.slg_raw, 3)                                         as ops,
    round(r.slg_raw - r.avg_raw, 3)                                         as isolated_power,
    round(cast(r.hits - r.home_runs as float)
          / nullif(r.at_bats - r.strikeouts - r.home_runs + r.sac_flies, 0), 3) as babip,
    round(cast(r.strikeouts as float) / nullif(r.plate_appearances, 0), 3)  as strikeout_pct,
    round(cast(r.walks as float)      / nullif(r.plate_appearances, 0), 3)  as walk_pct,
    cast(round(100 * (r.obp_raw / nullif(lg.on_base_pct, 0)
                    + r.slg_raw / nullif(lg.slugging_pct, 0) - 1), 0) as int) as ops_plus,

    -- plate discipline and contact quality (pitch-tracked games only)
    r.tracked_games,
    r.pitches_seen,
    round(cast(r.swings      as float) / nullif(r.pitches_seen, 0), 3)        as swing_pct,
    round(cast(r.whiffs      as float) / nullif(r.swings, 0), 3)              as whiff_pct,
    round(cast(r.chases      as float) / nullif(r.out_of_zone_pitches, 0), 3) as chase_pct,
    round(cast(r.zone_swings as float) / nullif(r.pitches_in_zone, 0), 3)     as zone_swing_pct,
    r.batted_ball_events,
    round(r.avg_exit_velo, 1)                                                 as avg_exit_velo,
    r.max_exit_velo,
    round(r.avg_launch_angle, 1)                                              as avg_launch_angle,
    round(cast(r.hard_hits    as float) / nullif(r.batted_ball_events, 0), 3) as hard_hit_pct,
    round(cast(r.barrels      as float) / nullif(r.batted_ball_events, 0), 3) as barrel_pct,
    round(cast(r.ground_balls as float) / nullif(r.batted_ball_events, 0), 3) as gb_pct,
    round(cast(r.line_drives  as float) / nullif(r.batted_ball_events, 0), 3) as ld_pct,
    round(cast(r.fly_balls    as float) / nullif(r.batted_ball_events, 0), 3) as fb_pct,

    r.team_games,
    case when r.plate_appearances >= 3.1 * r.team_games then 1 else 0 end     as is_qualified
from rates r
left join {{ ref('gold_league_season') }} lg
    on lg.season = r.season
