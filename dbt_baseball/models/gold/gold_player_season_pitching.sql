{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    GOLD: one row per pitcher per regular season -- the pitching line.

    Sources:
      - int_player_game_stats (box score, regular season, final games):
        outs, H, R, ER, BB, SO, HR allowed.
      - dim_game_details: W / L / SV decisions (winning/losing/save pitcher).
      - mart_pitcher_game (pitch-by-pitch), filtered to regular season:
        batters faced, hit batters, pitch counts, velocity, whiff / chase / zone.

    Rates are recomputed from summed counts (never averaged per game).
    Innings are shown the baseball way (6.2 = 6 and 2/3) in innings_pitched,
    and as a true number in innings_decimal for arithmetic.

    FIP = (13*HR + 3*(BB+HBP) - 2*K) / IP + league fip_constant
    (from gold_league_season). HBP comes from pitch data, so it is 0 for
    games without plate-appearance results; tracked_games shows coverage.

    csw_pct here is (called strikes + whiffs) / pitches -- the standard CSW%.
    NOTE: mart_pitcher_game.csw_pct divides called strikes only, so it is
    lower than CSW% elsewhere; this model does not use that column.

    is_qualified: at least 1 inning (3 outs) per team game -- MLB's ERA-title
    rule -- using the team the pitcher finished the season with.
*/

with box as (

    select * from {{ ref('int_player_game_stats') }}
    where pitching_outs_recorded     > 0
       or pitching_hits_allowed      > 0
       or pitching_walks             > 0
       or pitching_runs_allowed      > 0

),

regular_season_games as (

    select game_pk
    from {{ ref('dim_games') }}
    where game_type = 'R'
      and is_final  = 1

),

decisions as (

    select pitcher_id, season, sum(w) as wins, sum(l) as losses, sum(sv) as saves
    from (
        select gd.winning_pitcher_id as pitcher_id, g.season, 1 as w, 0 as l, 0 as sv
        from {{ ref('dim_game_details') }} gd
        join {{ ref('dim_games') }} g on g.game_pk = gd.game_pk
        where g.game_type = 'R' and g.is_final = 1 and gd.winning_pitcher_id is not null
        union all
        select gd.losing_pitcher_id, g.season, 0, 1, 0
        from {{ ref('dim_game_details') }} gd
        join {{ ref('dim_games') }} g on g.game_pk = gd.game_pk
        where g.game_type = 'R' and g.is_final = 1 and gd.losing_pitcher_id is not null
        union all
        select gd.save_pitcher_id, g.season, 0, 0, 1
        from {{ ref('dim_game_details') }} gd
        join {{ ref('dim_games') }} g on g.game_pk = gd.game_pk
        where g.game_type = 'R' and g.is_final = 1 and gd.save_pitcher_id is not null
    ) d
    group by pitcher_id, season

),

pitch_level as (

    select
        m.season,
        m.pitcher_id                                                        as player_id,
        count(*)                                                            as tracked_games,
        max(m.pitcher_hand)                                                 as pitcher_hand,
        sum(m.batters_faced)                                                as batters_faced,
        sum(m.hit_batters)                                                  as hit_batters,
        sum(m.strikeouts)                                                   as pa_strikeouts,
        sum(m.walks_allowed)                                                as pa_walks,
        sum(m.total_pitches)                                                as total_pitches,
        sum(m.tracked_pitches)                                              as tracked_pitches,
        sum(m.strikes)                                                      as strikes,
        sum(m.called_strikes)                                               as called_strikes,
        sum(m.swings)                                                       as swings,
        sum(m.whiffs)                                                       as whiffs,
        sum(m.in_zone_pitches)                                              as in_zone_pitches,
        sum(m.out_of_zone_pitches)                                          as out_of_zone_pitches,
        sum(m.chases)                                                       as chases,
        sum(m.first_pitches)                                                as first_pitches,
        sum(m.first_pitch_strikes)                                          as first_pitch_strikes,
        -- per-game averages weighted by that game's tracked pitches
        sum(m.avg_velo * m.tracked_pitches)
            / nullif(sum(case when m.avg_velo is not null
                              then m.tracked_pitches end), 0)               as avg_velo,
        max(m.max_velo)                                                     as max_velo,
        sum(m.avg_spin_rate * m.tracked_pitches)
            / nullif(sum(case when m.avg_spin_rate is not null
                              then m.tracked_pitches end), 0)               as avg_spin_rate
    from {{ ref('mart_pitcher_game') }} m
    join regular_season_games g
        on g.game_pk = m.game_pk
    group by m.season, m.pitcher_id

),

last_team as (

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
        count(distinct team_id)                                             as teams_played_for,
        sum(pitching_outs_recorded)                                         as outs,
        sum(pitching_hits_allowed)                                          as hits_allowed,
        sum(pitching_runs_allowed)                                          as runs_allowed,
        sum(pitching_earned_runs)                                           as earned_runs,
        sum(pitching_walks)                                                 as walks,
        sum(pitching_strikeouts)                                            as strikeouts,
        sum(pitching_home_runs_allowed)                                     as home_runs_allowed
    from box
    group by season, player_id

),

combined as (

    select
        b.*,
        cast(b.outs as float) / 3.0                                         as innings,
        coalesce(d.wins, 0)                                                 as wins,
        coalesce(d.losses, 0)                                               as losses,
        coalesce(d.saves, 0)                                                as saves,
        coalesce(p.tracked_games, 0)                                        as tracked_games,
        coalesce(p.hit_batters, 0)                                          as hit_batters,
        p.pitcher_hand, p.batters_faced, p.pa_strikeouts, p.pa_walks,
        p.total_pitches, p.tracked_pitches, p.strikes, p.called_strikes,
        p.swings, p.whiffs, p.in_zone_pitches, p.out_of_zone_pitches, p.chases,
        p.first_pitches, p.first_pitch_strikes, p.avg_velo, p.max_velo, p.avg_spin_rate,
        lt.team_id                                                          as last_team_id,
        lt.team_name                                                        as last_team_name,
        tg.team_games
    from season_box b
    left join decisions d
        on d.season = b.season and d.pitcher_id = b.player_id
    left join pitch_level p
        on p.season = b.season and p.player_id = b.player_id
    left join last_team lt
        on lt.season = b.season and lt.player_id = b.player_id
    left join team_games tg
        on tg.season = b.season and tg.team_id = lt.team_id

)

select
    c.season,
    c.player_id,
    c.player_name,
    c.pitcher_hand,
    c.last_team_id,
    c.last_team_name,
    c.teams_played_for,

    c.games,
    c.wins,
    c.losses,
    c.saves,
    c.outs                                                                  as outs_recorded,
    cast(c.outs / 3 as varchar(10)) + '.' + cast(c.outs % 3 as varchar(1))  as innings_pitched,
    round(c.innings, 2)                                                     as innings_decimal,
    c.hits_allowed,
    c.runs_allowed,
    c.earned_runs,
    c.walks,
    c.strikeouts,
    c.home_runs_allowed,
    c.hit_batters,

    round(9.0 * c.earned_runs / nullif(c.innings, 0), 2)                    as era,
    round(cast(c.walks + c.hits_allowed as float) / nullif(c.innings, 0), 3) as whip,
    round(9.0 * c.strikeouts        / nullif(c.innings, 0), 2)              as strikeouts_per_9,
    round(9.0 * c.walks             / nullif(c.innings, 0), 2)              as walks_per_9,
    round(9.0 * c.home_runs_allowed / nullif(c.innings, 0), 2)              as home_runs_per_9,
    round(cast(c.strikeouts as float) / nullif(c.walks, 0), 2)              as strikeout_to_walk,
    round((13.0 * c.home_runs_allowed + 3.0 * (c.walks + c.hit_batters)
           - 2.0 * c.strikeouts) / nullif(c.innings, 0)
          + lg.fip_constant, 2)                                             as fip,

    -- pitch-tracked games only
    c.tracked_games,
    c.batters_faced,
    round(cast(c.pa_strikeouts as float) / nullif(c.batters_faced, 0), 3)  as strikeout_pct,
    round(cast(c.pa_walks      as float) / nullif(c.batters_faced, 0), 3)  as walk_pct,
    c.total_pitches,
    round(cast(c.total_pitches as float) / nullif(c.innings, 0), 1)         as pitches_per_inning,
    round(cast(c.strikes as float) / nullif(c.total_pitches, 0), 3)         as strike_pct,
    round(cast(c.called_strikes + c.whiffs as float)
          / nullif(c.total_pitches, 0), 3)                                  as csw_pct,
    round(cast(c.whiffs  as float) / nullif(c.swings, 0), 3)                as whiff_pct,
    round(cast(c.chases  as float) / nullif(c.out_of_zone_pitches, 0), 3)   as chase_pct,
    round(cast(c.in_zone_pitches as float) / nullif(c.tracked_pitches, 0), 3) as zone_pct,
    round(cast(c.first_pitch_strikes as float) / nullif(c.first_pitches, 0), 3) as first_pitch_strike_pct,
    round(c.avg_velo, 1)                                                    as avg_velo,
    c.max_velo,
    round(c.avg_spin_rate, 0)                                               as avg_spin_rate,

    c.team_games,
    case when c.outs >= 3 * c.team_games then 1 else 0 end                  as is_qualified
from combined c
left join {{ ref('gold_league_season') }} lg
    on lg.season = c.season
