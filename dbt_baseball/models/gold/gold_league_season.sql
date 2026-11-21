{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    GOLD: one row per regular season -- MLB-wide averages.

    Context for every other gold model: a .280 hitter or a 3.50 ERA means
    something different in a high-scoring season than a low-scoring one.
    gold_player_season_batting uses on_base_pct / slugging_pct for OPS+,
    and gold_player_season_pitching uses fip_constant for FIP.

    Same sources and same rules as the player models (box score from
    int_player_game_stats, HBP / SF from the pitch-level marts, regular
    season only), so the league line is the sum of the player lines.

    fip_constant = league ERA - (13*HR + 3*(BB+HBP) - 2*K) / IP,
    which makes league-average FIP equal league-average ERA.
*/

with box as (

    select * from {{ ref('int_player_game_stats') }}

),

regular_season_games as (

    select game_pk
    from {{ ref('dim_games') }}
    where game_type = 'R'
      and is_final  = 1

),

batting_box as (

    select
        season,
        sum(batting_at_bats)                                                as at_bats,
        sum(batting_hits)                                                   as hits,
        sum(batting_doubles)                                                as doubles,
        sum(batting_triples)                                                as triples,
        sum(batting_home_runs)                                              as home_runs,
        sum(batting_walks)                                                  as walks,
        sum(batting_strikeouts)                                             as strikeouts
    from box
    group by season

),

batting_pitch as (

    select
        m.season,
        sum(m.hit_by_pitch)                                                 as hit_by_pitch,
        sum(m.sac_flies)                                                    as sac_flies
    from {{ ref('mart_batter_game') }} m
    join regular_season_games g on g.game_pk = m.game_pk
    group by m.season

),

pitching_box as (

    select
        season,
        sum(pitching_outs_recorded)                                         as outs,
        sum(pitching_earned_runs)                                           as earned_runs,
        sum(pitching_runs_allowed)                                          as runs_allowed,
        sum(pitching_hits_allowed)                                          as hits_allowed,
        sum(pitching_walks)                                                 as walks_allowed,
        sum(pitching_strikeouts)                                            as strikeouts_pitched,
        sum(pitching_home_runs_allowed)                                     as home_runs_allowed
    from box
    group by season

),

pitching_pitch as (

    select
        m.season,
        sum(m.hit_batters)                                                  as hit_batters
    from {{ ref('mart_pitcher_game') }} m
    join regular_season_games g on g.game_pk = m.game_pk
    group by m.season

),

team_games as (

    select
        season,
        count(*)                                                            as team_games,
        count(distinct game_pk)                                             as games,
        sum(team_score)                                                     as runs
    from {{ ref('int_team_game_results') }}
    group by season

),

combined as (

    select
        b.season,
        t.games,
        t.team_games,
        t.runs,
        b.at_bats, b.hits, b.doubles, b.triples, b.home_runs, b.walks, b.strikeouts,
        coalesce(bp.hit_by_pitch, 0)                                        as hit_by_pitch,
        coalesce(bp.sac_flies, 0)                                           as sac_flies,
        b.hits + b.doubles + 2 * b.triples + 3 * b.home_runs                as total_bases,
        p.outs, p.earned_runs, p.hits_allowed, p.walks_allowed,
        p.strikeouts_pitched, p.home_runs_allowed,
        coalesce(pp.hit_batters, 0)                                         as hit_batters,
        cast(p.outs as float) / 3.0                                         as innings
    from batting_box b
    left join batting_pitch  bp on bp.season = b.season
    left join pitching_box   p  on p.season  = b.season
    left join pitching_pitch pp on pp.season = b.season
    left join team_games     t  on t.season  = b.season

),

rates as (

    select
        c.*,
        cast(c.hits as float) / nullif(c.at_bats, 0)                        as avg_raw,
        cast(c.hits + c.walks + c.hit_by_pitch as float)
            / nullif(c.at_bats + c.walks + c.hit_by_pitch + c.sac_flies, 0) as obp_raw,
        cast(c.total_bases as float) / nullif(c.at_bats, 0)                 as slg_raw,
        9.0 * c.earned_runs / nullif(c.innings, 0)                          as era_raw
    from combined c

)

select
    season,
    games,
    team_games,
    runs,
    round(cast(runs as float) / nullif(team_games, 0), 2)                   as runs_per_team_game,
    round(cast(home_runs as float) / nullif(team_games, 0), 2)              as home_runs_per_team_game,

    round(avg_raw, 3)                                                       as batting_avg,
    round(obp_raw, 3)                                                       as on_base_pct,
    round(slg_raw, 3)                                                       as slugging_pct,
    round(obp_raw + slg_raw, 3)                                             as ops,
    round(cast(strikeouts as float)
          / nullif(at_bats + walks + hit_by_pitch + sac_flies, 0), 3)       as strikeout_pct,
    round(cast(walks as float)
          / nullif(at_bats + walks + hit_by_pitch + sac_flies, 0), 3)       as walk_pct,

    round(era_raw, 2)                                                       as era,
    round(cast(walks_allowed + hits_allowed as float) / nullif(innings, 0), 3) as whip,
    round(era_raw
          - (13.0 * home_runs_allowed + 3.0 * (walks_allowed + hit_batters)
             - 2.0 * strikeouts_pitched) / nullif(innings, 0), 3)           as fip_constant,

    at_bats,
    hits,
    home_runs,
    walks,
    strikeouts,
    outs                                                                    as outs_recorded,
    earned_runs
from rates
