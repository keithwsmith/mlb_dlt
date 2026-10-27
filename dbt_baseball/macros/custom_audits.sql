{#
    macros/audits/custom_audits.sql
    -----------------------------------------------------------------------
    Content-correctness audits for the MLB warehouse, as dbt macros instead
    of a standalone T-SQL stored procedure. Same destination
    (silver.test_sql — your existing custom-SQL-test log table), but now:

      - reads go through {{ ref() }} instead of hardcoded schema.table,
        so these audits automatically follow your models across
        dev/prod/whatever targets you run against
      - each check is DRY'd through one helper macro (_log_custom_audit)
        that computes the failure count (COUNT(*)) and a 50-row JSON
        sample (TOP 50) in SQL. Checks written as CTEs pass the WITH
        clause separately via `ctes`, since SQL Server doesn't allow WITH
        inside a derived table
      - writes to TWO tables: silver.test_sql (full detail, unchanged)
        AND dbo.transformation_audit_log (a summary row), so every check
        also shows up on ETLMonitor's Model Audit tab alongside your
        audit_helper results. See _log_custom_audit's docstring for why
        the model_name written to transformation_audit_log is
        "<model> :: <test_name>" rather than the plain model name.
      - every audit first checks (via _require_models) that the models it
        reads actually exist in the project. A missing model logs a
        SKIPPED line and moves on, instead of ref() raising a compile
        error that would abort every audit after it.
      - runnable on demand via:
            dbt run-operation run_custom_audits
        a single check:
            dbt run-operation run_custom_audits --args '{"only": "business_logic_shutout_flag_consistency"}'
        or several:
            dbt run-operation run_custom_audits --args '{"only": ["uniqueness_fact_games_game_pk", "integrity_fact_games_dimension_keys"]}'

    DELIBERATELY NOT wired into on-run-end (unlike log_manual_dbt_run):
    these scan full fact tables. Run explicitly, on a schedule, or call it
    from ETLMonitor.py the same way dbt itself gets invoked.

    CHECK GROUPS
      B  fact_games business logic / reconciliation
      C  team results & standings
      D  player-level stats
      E  dimension integrity
      F  pitch / at-bat / batted-ball / mart level

    FIXES TO THE ORIGINAL B1/B2/B4:
      All three compared winning_team_id (a bigint team ID) against
      home_team_key / away_team_key. Those keys are now dbt_utils
      surrogate keys (hash strings), so the comparisons never matched:
        - B1 CAST(winning_team_id AS varchar) = <hash>   -> never true
        - B2/B4 TRY_CAST(<hash> AS INT)                  -> always NULL
      i.e. all three could ONLY ever report 0 failures, whatever the data.
      B1/B2 now test the flag against the box score directly; B4 gets the
      real team IDs from dim_games.

    NOTE: the old A1/A2/A3 pitch-level checks were removed when
    fact_pitches was dropped from the project. The F checks below use
    int_pitches_enriched / fact_at_bats / fact_batted_balls instead.

    CHANGES (2026-09 revision) -- run after rebuilding fact_games,
    dim_player, fact_at_bats (--full-refresh) and dim_date:
      _log_custom_audit  counts failures with COUNT(*) and samples with
                         TOP 50 in SQL instead of pulling every failing row
                         into Jinja. CTE checks pass their WITH clause via
                         the new `ctes` argument.
      B1  matches fact_games' new no-hitter definition (opposing staff
          confirms, 27+ outs); dropped the branch that flagged every 0-hit
          side, which is wrong under the 27-out rule.
      B5  also requires a shutout and 0 runs (schedule feed) for the
          perfect-gamed side.
      B6  denominator uses the same filters as the check.
      B9  RENAMED to reconciliation_game_details_runs_vs_dim_games_score;
          now compares the two independent score feeds. The old check
          compared fact_games to its own source.
      B13 NEW range_fact_games_rare_event_counts -- seasons with
          implausible no-hitter / perfect-game counts.
      D1  only games with an accepted box score.
      D1b NEW integrity_fact_games_rejected_box_scores -- the games D1
          used to fail on, reported once each with player_rows.
      D3  adds an overlapping-ranges check.
      F3  chase_in_zone uses is_in_strike_zone (same zone definition as
          is_chase) instead of Statcast's zone number.
#}


{% macro _require_models(test_name, model_names) %}
    {#
        Returns true if every model/snapshot/seed in `model_names` exists
        in the project graph; otherwise logs a SKIPPED line and returns
        false. Must be called BEFORE any {{ ref() }} for those models is
        rendered, because ref() to a missing model is a hard compile error.
    #}
    {% set available = [] %}
    {% for node in graph.nodes.values() %}
        {% if node.resource_type in ['model', 'snapshot', 'seed'] %}
            {% do available.append(node.name) %}
        {% endif %}
    {% endfor %}
    {% set missing = [] %}
    {% for m in model_names %}
        {% if m not in available %}
            {% do missing.append(m) %}
        {% endif %}
    {% endfor %}
    {% if missing | length > 0 %}
        {{ log("SKIPPED " ~ test_name ~ ": model(s) not in project: " ~ missing | join(', '), info=True) }}
        {{ return(false) }}
    {% endif %}
    {{ return(true) }}
{% endmacro %}


{% macro _log_custom_audit(table_name, model_name, test_name, test_type, failure_query, population_query, ctes='') %}
    {#
        Runs `failure_query` (a SELECT that returns ONLY the failing rows --
        column list can be anything) and logs:

          1. ONE row into silver.test_sql -- full detail: failure_count,
             up to 50 sample failures as JSON, and the check's own SQL
             text, same as your other custom test_sql rows.

          2. ONE row into dbo.transformation_audit_log -- a summary row so
             this check shows up on ETLMonitor's Model Audit tab alongside
             your audit_helper results. That table's summary view does
             ROW_NUMBER() PARTITION BY model_name ORDER BY run_at DESC,
             i.e. "one row per model_name, latest only." Since several of
             these checks share a base model_name (fact_games has many),
             the value written to THIS table is `model_name :: test_name`
             -- distinct per check.

        `population_query` is a scalar `SELECT COUNT(*) AS cnt ...`
        representing the denominator ("rows compared") at the SAME GRAIN
        and with the SAME FILTERS as the failure rows. It drives
        perfect_match_pct, which drives the Model Audit tab's
        red/amber/green coloring (<99% danger, <99.9% warning).

        CHANGED: the failure count and the sample are now computed in SQL
        (COUNT(*) and TOP 50 over the failure query as a derived table)
        instead of pulling every failing row into Jinja and taking
        len(). The pitches audit was pulling ~185k rows into memory just to
        count them. The failure query therefore runs twice, but each run
        returns at most 50 rows.

        CTE-based checks: SQL Server won't allow WITH inside a derived
        table, so pass the WITH ... clause separately as `ctes` and keep
        `failure_query` as the final SELECT only. `ctes` is prepended to
        the count query, the sample query, and the stored SQL text. The
        failure query must not contain ORDER BY (it's used as a derived
        table) and every column must have a name.
    #}

    {% set count_sql %}
{{ ctes }}
SELECT COUNT(*) AS cnt FROM (
{{ failure_query }}
) audit_failures
    {% endset %}
    {% set count_result = run_query(count_sql) %}
    {% set failure_count = count_result.rows[0]['cnt'] if (count_result and count_result.rows | length > 0) else 0 %}

    {% set sample_sql %}
{{ ctes }}
SELECT TOP 50 * FROM (
{{ failure_query }}
) audit_failures
    {% endset %}
    {% set result = run_query(sample_sql) if failure_count > 0 else none %}

    {% set pop_result = run_query(population_query) %}
    {% set total_rows = pop_result.rows[0]['cnt'] if (pop_result and pop_result.rows | length > 0) else 0 %}
    {% set perfect_match_pct = (100.0 * (total_rows - failure_count) / total_rows) if total_rows > 0 else 100.0 %}

    {% set sample_rows = [] %}
    {% if result is not none %}
        {% for row in result.rows %}
            {% set row_dict = {} %}
            {% for col_name in result.column_names %}
                {#
                    Stringify every value. These come back as a mix of agate
                    Decimal/Number/Text/DateTime types, and dbt's tojson()
                    doesn't reliably serialize all of them (Decimal in
                    particular).
                #}
                {% do row_dict.update({col_name: (row[col_name] | string)}) %}
            {% endfor %}
            {% do sample_rows.append(row_dict) %}
        {% endfor %}
    {% endif %}
    {% set sample_json = tojson(sample_rows) %}

    {% set full_sql_text = ((ctes ~ '\n' ~ failure_query) | trim) %}
    {% set safe_sql_text = (full_sql_text | replace("'", "''")) %}
    {% set safe_sample_json = (sample_json | replace("'", "''")) %}

    {% set insert_test_sql %}
        INSERT INTO silver.test_sql
            (table_name, model_name, test_name, test_type, failure_count, sql_statement, sql_result, run_at)
        VALUES (
            '{{ table_name }}', '{{ model_name }}', '{{ test_name }}', '{{ test_type }}',
            {{ failure_count }},
            N'{{ safe_sql_text }}',
            N'{{ safe_sample_json }}',
            SYSUTCDATETIME()
        )
    {% endset %}
    {% do run_query(insert_test_sql) %}

    {% set audit_model_name = (model_name ~ ' :: ' ~ test_name) | replace("'", "''") %}
    {% set baseline_desc = (test_type ~ ' — ' ~ table_name) | replace("'", "''") %}
    {% set insert_transform_audit %}
        INSERT INTO dbo.transformation_audit_log
            (model_name, baseline_description, run_at, rows_compared,
             columns_compared, perfect_match_pct, conflicting_values,
             missing_from_a, missing_from_b, detail_url)
        VALUES (
            '{{ audit_model_name }}',
            '{{ baseline_desc }}',
            SYSUTCDATETIME(),
            {{ total_rows }},
            NULL,
            {{ perfect_match_pct }},
            {{ failure_count }},
            NULL,
            NULL,
            NULL
        )
    {% endset %}
    {% do run_query(insert_transform_audit) %}

    {{ log(test_name ~ ": " ~ failure_count ~ " / " ~ total_rows ~ " failed (" ~ perfect_match_pct | round(2) ~ "% match)", info=True) }}
{% endmacro %}


{# =====================================================================
   B — fact_games business logic / reconciliation
   ===================================================================== #}

{% macro _audit_no_hitter_flag() %}
    {#
        B1 — is_no_hitter must match the official definition used by
        fact_games: a side with 0 hits, CONFIRMED by the opposing staff's
        hits allowed (different players' rows), over at least 27 outs.

          flagged_but_not_confirmed   flag set without all three conditions
          confirmed_but_not_flagged   all three hold in a final game, no flag

        CHANGED: the old "zero_hit_side_not_flagged" branch flagged every
        0-hit side. Under the 27-out rule an 8-inning road loss or a
        shortened game with 0 hits is correctly NOT a no-hitter, so that
        branch would now report false failures.
    #}
    {% set test_name = 'business_logic_no_hitter_flag_consistency' %}
    {% if not _require_models(test_name, ['fact_games']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT f.game_pk, f.is_final, f.is_no_hitter,
       f.home_team_hits, f.away_pitching_hits_allowed, f.away_pitching_outs,
       f.away_team_hits, f.home_pitching_hits_allowed, f.home_pitching_outs,
       CASE WHEN f.is_no_hitter = 1 THEN 'flagged_but_not_confirmed'
            ELSE 'confirmed_but_not_flagged' END AS issue
FROM {{ ref('fact_games') }} f
CROSS APPLY (
    SELECT CASE WHEN (f.home_team_hits = 0 AND f.away_pitching_hits_allowed = 0 AND f.away_pitching_outs >= 27)
                  OR (f.away_team_hits = 0 AND f.home_pitching_hits_allowed = 0 AND f.home_pitching_outs >= 27)
                THEN 1 ELSE 0 END AS confirmed
) c
WHERE (f.is_no_hitter = 1 AND (f.is_final = 0 OR c.confirmed = 0))
   OR (f.is_no_hitter = 0 AND f.is_final = 1 AND c.confirmed = 1)
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('fact_games') }}
    {% endset %}
    {% do _log_custom_audit('fact_games', 'fact_games', test_name, 'custom_business_logic', q, pop_q) %}
{% endmacro %}


{% macro _audit_shutout_flag() %}
    {#
        B2 — is_shutout must agree with the runs columns. fact_games
        computes is_shutout from dim_games.home_score/away_score but
        stores runs from dim_game_details, so this also catches the two
        sources disagreeing.
    #}
    {% set test_name = 'business_logic_shutout_flag_consistency' %}
    {% if not _require_models(test_name, ['fact_games']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT game_pk, is_final, is_shutout, home_team_runs, away_team_runs,
       CASE WHEN is_shutout = 1 THEN 'flagged_but_no_zero_run_side'
            ELSE 'zero_run_side_not_flagged' END AS issue
FROM {{ ref('fact_games') }}
WHERE (is_shutout = 1
       AND NOT (ISNULL(home_team_runs, -1) = 0 OR ISNULL(away_team_runs, -1) = 0))
   OR (is_shutout = 0 AND is_final = 1
       AND (home_team_runs = 0 OR away_team_runs = 0))
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('fact_games') }}
    {% endset %}
    {% do _log_custom_audit('fact_games', 'fact_games', test_name, 'custom_business_logic', q, pop_q) %}
{% endmacro %}


{% macro _audit_run_diff() %}
    {#
        B3 — run_diff must equal the actual run differential. Sign
        convention isn't confirmable from the schema alone, so this
        accepts either and only flags rows where NEITHER holds.
    #}
    {% set test_name = 'business_logic_run_diff_consistency' %}
    {% if not _require_models(test_name, ['fact_games']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT game_pk, home_team_runs, away_team_runs, run_diff
FROM {{ ref('fact_games') }}
WHERE run_diff IS NOT NULL
  AND home_team_runs IS NOT NULL AND away_team_runs IS NOT NULL
  AND run_diff <> (home_team_runs - away_team_runs)
  AND run_diff <> (away_team_runs - home_team_runs)
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('fact_games') }}
WHERE run_diff IS NOT NULL
  AND home_team_runs IS NOT NULL AND away_team_runs IS NOT NULL
    {% endset %}
    {% do _log_custom_audit('fact_games', 'fact_games', test_name, 'custom_business_logic', q, pop_q) %}
{% endmacro %}


{% macro _audit_winning_team() %}
    {#
        B4 — for completed, non-tie games, winning_team_id must be the
        team that scored more runs. Team IDs come from dim_games
        (home_team_id / away_team_id); fact_games only carries hashed
        surrogate keys, which can't be compared to an ID.
    #}
    {% set test_name = 'business_logic_winning_team_consistency' %}
    {% if not _require_models(test_name, ['fact_games', 'dim_games']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT f.game_pk, f.home_team_runs, f.away_team_runs,
       g.home_team_id, g.away_team_id, f.winning_team_id
FROM {{ ref('fact_games') }} f
JOIN {{ ref('dim_games') }} g
    ON g.game_pk = f.game_pk
WHERE f.is_final = 1
  AND ISNULL(f.is_tie, 0) = 0
  AND f.home_team_runs IS NOT NULL AND f.away_team_runs IS NOT NULL
  AND (
        (f.home_team_runs > f.away_team_runs AND ISNULL(f.winning_team_id, -1) <> g.home_team_id)
     OR (f.away_team_runs > f.home_team_runs AND ISNULL(f.winning_team_id, -1) <> g.away_team_id)
      )
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt
FROM {{ ref('fact_games') }} f
JOIN {{ ref('dim_games') }} g
    ON g.game_pk = f.game_pk
WHERE f.is_final = 1
  AND ISNULL(f.is_tie, 0) = 0
  AND f.home_team_runs IS NOT NULL AND f.away_team_runs IS NOT NULL
    {% endset %}
    {% do _log_custom_audit('fact_games', 'fact_games', test_name, 'custom_business_logic', q, pop_q) %}
{% endmacro %}


{% macro _audit_perfect_game_implies_no_hitter() %}
    {#
        B5 — a perfect game must also be a no-hitter AND a shutout, and the
        perfect-gamed side's runs must be 0. Runs come from the schedule
        feed (dim_games), not the player lines, so the last check is
        independent of the box score that drives the flag.

        CHANGED: previously only checked is_no_hitter, which fact_games
        can't violate by construction.
    #}
    {% set test_name = 'business_logic_perfect_game_implies_no_hitter' %}
    {% if not _require_models(test_name, ['fact_games']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT f.game_pk, f.season, x.violations,
       f.home_team_hits, f.home_batting_walks, f.home_team_runs,
       f.away_team_hits, f.away_batting_walks, f.away_team_runs
FROM {{ ref('fact_games') }} f
CROSS APPLY (
    SELECT CONCAT(
        CASE WHEN f.is_no_hitter = 0 THEN 'not_no_hitter;' END,
        CASE WHEN f.is_shutout   = 0 THEN 'not_shutout;' END,
        CASE WHEN NOT (
                   (f.home_team_hits = 0 AND f.home_batting_walks = 0 AND f.home_team_runs = 0)
                OR (f.away_team_hits = 0 AND f.away_batting_walks = 0 AND f.away_team_runs = 0))
             THEN 'perfect_side_scored_per_schedule_feed;' END
    ) AS violations
) x
WHERE f.is_perfect_game = 1
  AND x.violations <> ''
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('fact_games') }} WHERE is_perfect_game = 1
    {% endset %}
    {% do _log_custom_audit('fact_games', 'fact_games', test_name, 'custom_business_logic', q, pop_q) %}
{% endmacro %}


{% macro _audit_fact_games_rare_event_counts() %}
    {#
        B13 (NEW) — independent plausibility check on the rare-event flags.
        One row per season whose counts exceed anything in MLB history:
        the single-season records are 9 no-hitters and 3 perfect games, so
        thresholds of 12 and 3 leave headroom while still catching a flag
        that's firing on bad box-score data (the old flag produced 56
        perfect games in total). Unlike B1/B5 this doesn't re-derive the
        flag from the same columns, so it can catch a wrong definition.
    #}
    {% set test_name = 'range_fact_games_rare_event_counts' %}
    {% if not _require_models(test_name, ['fact_games']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT season,
       SUM(is_no_hitter)    AS no_hitters,
       SUM(is_perfect_game) AS perfect_games
FROM {{ ref('fact_games') }}
GROUP BY season
HAVING SUM(is_no_hitter) > 12 OR SUM(is_perfect_game) > 3
    {% endset %}
    {% set pop_q %}
SELECT COUNT(DISTINCT season) AS cnt FROM {{ ref('fact_games') }}
    {% endset %}
    {% do _log_custom_audit('fact_games', 'fact_games', test_name, 'custom_range', q, pop_q) %}
{% endmacro %}


{% macro _audit_box_score_batting_vs_pitching() %}
    {#
        B6 — one team's batting line must mirror the other team's
        pitching line: home hits = away pitching hits allowed, home runs
        = away pitching runs allowed, and likewise for walks, strikeouts,
        and home runs, in both directions. Only compares values that are
        present on both sides. The `mismatches` column lists which stats
        disagree.

        CHANGED: the denominator now uses the same filters as the check
        (final, 1999+, has_box_score = 1). It previously counted every
        final game, inflating the match %.

        A single-game failure here is most often a player who appeared for
        BOTH teams in one game_pk (a suspended game resumed after a trade).
        int_player_game_stats' unique key (game_pk, player_id) can only
        hold one of those lines. Check with:
            SELECT game_pk, player_id FROM dw.player_game_stats
            GROUP BY game_pk, player_id HAVING COUNT(DISTINCT team_id) > 1

        If EVERY game fails this check, the home_/away_ prefixes on the
        pitching columns mean "the staff facing the home team" rather than
        "the home team's staff" — swap the pairs below.
    #}
    {% set test_name = 'reconciliation_box_score_batting_vs_pitching' %}
    {% if not _require_models(test_name, ['fact_games']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT f.game_pk, x.mismatches,
       f.home_team_hits, f.away_pitching_hits_allowed,
       f.away_team_hits, f.home_pitching_hits_allowed,
       f.home_team_runs, f.away_pitching_runs_allowed,
       f.away_team_runs, f.home_pitching_runs_allowed
FROM {{ ref('fact_games') }} f
CROSS APPLY (
    SELECT CONCAT(
        CASE WHEN f.home_team_hits          <> f.away_pitching_hits_allowed      THEN 'home_hits;' END,
        CASE WHEN f.away_team_hits          <> f.home_pitching_hits_allowed      THEN 'away_hits;' END,
        CASE WHEN f.home_team_runs          <> f.away_pitching_runs_allowed      THEN 'home_runs;' END,
        CASE WHEN f.away_team_runs          <> f.home_pitching_runs_allowed      THEN 'away_runs;' END,
        CASE WHEN f.home_batting_walks      <> f.away_pitching_walks             THEN 'home_walks;' END,
        CASE WHEN f.away_batting_walks      <> f.home_pitching_walks             THEN 'away_walks;' END,
        CASE WHEN f.home_batting_strikeouts <> f.away_pitching_strikeouts        THEN 'home_strikeouts;' END,
        CASE WHEN f.away_batting_strikeouts <> f.home_pitching_strikeouts        THEN 'away_strikeouts;' END,
        CASE WHEN f.home_home_runs          <> f.away_pitching_home_runs_allowed THEN 'home_hr;' END,
        CASE WHEN f.away_home_runs          <> f.home_pitching_home_runs_allowed THEN 'away_hr;' END
    ) AS mismatches
) x
WHERE f.is_final = 1
  AND x.mismatches <> ''
  AND f.season >= 1999
  AND f.has_box_score = 1
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('fact_games') }}
WHERE is_final = 1 AND season >= 1999 AND has_box_score = 1
    {% endset %}
    {% do _log_custom_audit('fact_games', 'fact_games', test_name, 'custom_reconciliation', q, pop_q) %}
{% endmacro %}


{% macro _audit_fact_games_stat_composition() %}
    {#
        B7 — stats that are subsets of other stats can't exceed them:
        extra-base hits <= hits, home runs <= runs, earned runs <= runs
        allowed, home runs allowed <= hits allowed. Checked per side.
    #}
    {% set test_name = 'business_logic_fact_games_stat_composition' %}
    {% if not _require_models(test_name, ['fact_games']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT f.game_pk, x.violations
FROM {{ ref('fact_games') }} f
CROSS APPLY (
    SELECT CONCAT(
        CASE WHEN f.home_doubles + f.home_triples + f.home_home_runs > f.home_team_hits THEN 'home_xbh_gt_hits;' END,
        CASE WHEN f.away_doubles + f.away_triples + f.away_home_runs > f.away_team_hits THEN 'away_xbh_gt_hits;' END,
        CASE WHEN f.home_home_runs > f.home_team_runs THEN 'home_hr_gt_runs;' END,
        CASE WHEN f.away_home_runs > f.away_team_runs THEN 'away_hr_gt_runs;' END,
        CASE WHEN f.home_pitching_earned_runs > f.home_pitching_runs_allowed THEN 'home_er_gt_runs_allowed;' END,
        CASE WHEN f.away_pitching_earned_runs > f.away_pitching_runs_allowed THEN 'away_er_gt_runs_allowed;' END,
        CASE WHEN f.home_pitching_home_runs_allowed > f.home_pitching_hits_allowed THEN 'home_hra_gt_hits_allowed;' END,
        CASE WHEN f.away_pitching_home_runs_allowed > f.away_pitching_hits_allowed THEN 'away_hra_gt_hits_allowed;' END
    ) AS violations
) x
WHERE x.violations <> ''
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('fact_games') }}
    {% endset %}
    {% do _log_custom_audit('fact_games', 'fact_games', test_name, 'custom_business_logic', q, pop_q) %}
{% endmacro %}


{% macro _audit_winner_loser_distinct() %}
    {#
        B8 — winner and loser can't be the same team or the same pitcher;
        a completed non-tie game must have both a winning and losing team;
        a tie must have no winning team.
    #}
    {% set test_name = 'business_logic_winner_loser_distinct' %}
    {% if not _require_models(test_name, ['fact_games']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT f.game_pk, f.is_final, f.is_tie, f.winning_team_id, f.losing_team_id,
       f.winning_pitcher_id, f.losing_pitcher_id, x.violations
FROM {{ ref('fact_games') }} f
CROSS APPLY (
    SELECT CONCAT(
        CASE WHEN f.winning_team_id = f.losing_team_id THEN 'same_winning_losing_team;' END,
        CASE WHEN f.winning_pitcher_id = f.losing_pitcher_id THEN 'same_winning_losing_pitcher;' END,
        CASE WHEN f.is_final = 1 AND ISNULL(f.is_tie, 0) = 0
                  AND (f.winning_team_id IS NULL OR f.losing_team_id IS NULL)
             THEN 'final_game_missing_winner_or_loser;' END,
        CASE WHEN f.is_tie = 1 AND f.winning_team_id IS NOT NULL THEN 'tie_has_winner;' END
    ) AS violations
) x
WHERE x.violations <> ''
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('fact_games') }}
    {% endset %}
    {% do _log_custom_audit('fact_games', 'fact_games', test_name, 'custom_business_logic', q, pop_q) %}
{% endmacro %}


{% macro _audit_fact_games_vs_dim_games_score() %}
    {#
        B9 — the two independent score sources must agree for completed
        games: the schedule feed (dim_games.home_score / away_score) and
        the game-details feed (dim_game_details.home_runs / away_runs).

        CHANGED (and renamed from reconciliation_fact_games_runs_vs_dim_games_score):
        the old version compared fact_games' runs to dim_games' score, but
        fact_games takes its runs FROM dim_games, so it could only fail
        when dim_games had no score at all. This version compares two
        genuinely separate feeds. fact_games already only trusts
        game_details' left-on-base when these agree, so expect some
        failures on the first run -- that's new signal, not a regression.
        Rows missing from game_details entirely are not counted here.
    #}
    {% set test_name = 'reconciliation_game_details_runs_vs_dim_games_score' %}
    {% if not _require_models(test_name, ['dim_games', 'dim_game_details']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT g.game_pk, g.season, g.game_type,
       g.home_score, gd.home_runs AS gd_home_runs,
       g.away_score, gd.away_runs AS gd_away_runs
FROM {{ ref('dim_games') }} g
JOIN {{ ref('dim_game_details') }} gd
    ON gd.game_pk = g.game_pk
WHERE g.is_final = 1
  AND (   ISNULL(g.home_score, -1) <> ISNULL(gd.home_runs, -1)
       OR ISNULL(g.away_score, -1) <> ISNULL(gd.away_runs, -1))
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt
FROM {{ ref('dim_games') }} g
JOIN {{ ref('dim_game_details') }} gd
    ON gd.game_pk = g.game_pk
WHERE g.is_final = 1
    {% endset %}
    {% do _log_custom_audit('dim_game_details', 'dim_game_details', test_name, 'custom_reconciliation', q, pop_q) %}
{% endmacro %}


{% macro _audit_fact_games_dimension_keys() %}
    {#
        B10 — every dimension key on fact_games should resolve. A NULL
        key means the LEFT JOIN to that dimension found nothing — or,
        for an incremental model, that rows loaded before a column was
        added/changed were never rebuilt (needs --full-refresh).
    #}
    {% set test_name = 'integrity_fact_games_dimension_keys' %}
    {% if not _require_models(test_name, ['fact_games']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT f.game_pk, f.season, f.load_id, x.null_keys
FROM {{ ref('fact_games') }} f
CROSS APPLY (
    SELECT CONCAT(
        CASE WHEN f.home_team_key     IS NULL THEN 'home_team_key;' END,
        CASE WHEN f.away_team_key     IS NULL THEN 'away_team_key;' END,
        CASE WHEN f.venue_key         IS NULL THEN 'venue_key;' END,
        CASE WHEN f.season_key        IS NULL THEN 'season_key;' END,
        CASE WHEN f.game_type_key     IS NULL THEN 'game_type_key;' END,
        CASE WHEN f.game_status_key   IS NULL THEN 'game_status_key;' END,
        CASE WHEN f.game_date_key     IS NULL THEN 'game_date_key;' END,
        CASE WHEN f.official_date_key IS NULL THEN 'official_date_key;' END
    ) AS null_keys
) x
WHERE x.null_keys <> ''
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('fact_games') }}
    {% endset %}
    {% do _log_custom_audit('fact_games', 'fact_games', test_name, 'custom_integrity', q, pop_q) %}
{% endmacro %}


{% macro _audit_fact_games_unique_game_pk() %}
    {#
        B11 — fact_games is incremental with unique_key='game_pk'; a
        duplicate means the merge didn't behave (or a join fanned out).
        Also flags NULL game_pk, which a merge can never match.
    #}
    {% set test_name = 'uniqueness_fact_games_game_pk' %}
    {% if not _require_models(test_name, ['fact_games']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT game_pk, COUNT(*) AS row_count
FROM {{ ref('fact_games') }}
GROUP BY game_pk
HAVING COUNT(*) > 1 OR game_pk IS NULL
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt
FROM (SELECT game_pk FROM {{ ref('fact_games') }} GROUP BY game_pk) d
    {% endset %}
    {% do _log_custom_audit('fact_games', 'fact_games', test_name, 'custom_uniqueness', q, pop_q) %}
{% endmacro %}


{% macro _audit_fact_games_ranges() %}
    {#
        B12 — plausibility ranges: no negative counting stats; attendance
        0–120,000; duration of a completed game between 1 and 720 minutes
        (the longest MLB games run a little over 8 hours).
    #}
    {% set test_name = 'range_fact_games_values' %}
    {% if not _require_models(test_name, ['fact_games']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT f.game_pk, x.violations, f.home_team_runs, f.away_team_runs,
       f.home_team_hits, f.away_team_hits, f.attendance, f.game_duration_minutes
FROM {{ ref('fact_games') }} f
CROSS APPLY (
    SELECT CONCAT(
        CASE WHEN f.home_team_runs < 0 OR f.away_team_runs < 0 THEN 'negative_runs;' END,
        CASE WHEN f.home_team_hits < 0 OR f.away_team_hits < 0 THEN 'negative_hits;' END,
        CASE WHEN f.attendance < 0 OR f.attendance > 120000 THEN 'attendance_out_of_range;' END,
        CASE WHEN f.is_final = 1
                  AND (f.game_duration_minutes < 0 OR f.game_duration_minutes > 720)
             THEN 'duration_out_of_range;' END
    ) AS violations
) x
WHERE x.violations <> ''
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('fact_games') }}
    {% endset %}
    {% do _log_custom_audit('fact_games', 'fact_games', test_name, 'custom_range', q, pop_q) %}
{% endmacro %}


{# =====================================================================
   C — team results & standings
   ===================================================================== #}

{% macro _audit_team_standings_two_rows_per_game() %}
    {#
        C1 — dim_team_standings is one row per team per game, so each
        game_pk needs exactly 2 rows: one home (is_home=1), one away.
    #}
    {% set test_name = 'integrity_team_standings_two_rows_per_game' %}
    {% if not _require_models(test_name, ['dim_team_standings']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT game_pk, COUNT(*) AS row_count, SUM(is_home) AS home_rows
FROM {{ ref('dim_team_standings') }}
GROUP BY game_pk
HAVING COUNT(*) <> 2 OR SUM(is_home) <> 1
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt
FROM (SELECT game_pk FROM {{ ref('dim_team_standings') }} GROUP BY game_pk) d
    {% endset %}
    {% do _log_custom_audit('dim_team_standings', 'dim_team_standings', test_name, 'custom_integrity', q, pop_q) %}
{% endmacro %}


{% macro _audit_team_standings_symmetry() %}
    {#
        C2 — the home and away rows of a game must mirror each other
        (my score = your opponent score; my win = your loss; tie on both
        or neither), and each row's is_win/is_loss/is_tie must match its
        own scores. Checked once per game from the home row.
    #}
    {% set test_name = 'business_logic_team_standings_symmetry' %}
    {% if not _require_models(test_name, ['dim_team_standings']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT h.game_pk, h.team_id AS home_team_id, a.team_id AS away_team_id,
       h.team_score AS home_score, a.team_score AS away_score,
       h.is_win AS home_is_win, a.is_win AS away_is_win,
       h.is_tie AS home_is_tie, a.is_tie AS away_is_tie,
       x.violations
FROM {{ ref('dim_team_standings') }} h
JOIN {{ ref('dim_team_standings') }} a
    ON  a.game_pk = h.game_pk
    AND a.is_home = 0
    AND a.team_id = h.opponent_team_id
CROSS APPLY (
    SELECT CONCAT(
        CASE WHEN h.team_score <> a.opponent_score OR h.opponent_score <> a.team_score
             THEN 'scores_not_mirrored;' END,
        CASE WHEN h.is_win <> a.is_loss OR h.is_loss <> a.is_win OR h.is_tie <> a.is_tie
             THEN 'results_not_mirrored;' END,
        CASE WHEN h.is_win + h.is_loss + h.is_tie <> 1 THEN 'home_result_flags_not_exclusive;' END,
        CASE WHEN h.is_win = 1 AND h.team_score <= h.opponent_score THEN 'home_win_without_more_runs;' END,
        CASE WHEN a.is_win = 1 AND a.team_score <= a.opponent_score THEN 'away_win_without_more_runs;' END
    ) AS violations
) x
WHERE h.is_home = 1
  AND x.violations <> ''
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('dim_team_standings') }} WHERE is_home = 1
    {% endset %}
    {% do _log_custom_audit('dim_team_standings', 'dim_team_standings', test_name, 'custom_business_logic', q, pop_q) %}
{% endmacro %}


{% macro _audit_standings_by_date_identity() %}
    {#
        C3 — running totals must add up: wins + losses + ties =
        games played, both for the day and to date, and to-date totals
        can never be smaller than the day's totals.
    #}
    {% set test_name = 'business_logic_standings_by_date_identity' %}
    {% if not _require_models(test_name, ['dim_team_standings_by_date']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT s.team_id, s.season, s.game_date, x.violations,
       s.games_played, s.wins, s.losses, s.ties,
       s.games_played_to_date, s.wins_to_date, s.losses_to_date, s.ties_to_date
FROM {{ ref('dim_team_standings_by_date') }} s
CROSS APPLY (
    SELECT CONCAT(
        CASE WHEN ISNULL(s.wins, 0) + ISNULL(s.losses, 0) + ISNULL(s.ties, 0) <> ISNULL(s.games_played, 0)
             THEN 'daily_wlt_ne_games;' END,
        CASE WHEN ISNULL(s.wins_to_date, 0) + ISNULL(s.losses_to_date, 0) + ISNULL(s.ties_to_date, 0)
                  <> ISNULL(s.games_played_to_date, 0)
             THEN 'to_date_wlt_ne_games;' END,
        CASE WHEN s.games_played_to_date < s.games_played THEN 'to_date_less_than_daily;' END
    ) AS violations
) x
WHERE x.violations <> ''
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('dim_team_standings_by_date') }}
    {% endset %}
    {% do _log_custom_audit('dim_team_standings_by_date', 'dim_team_standings_by_date', test_name, 'custom_business_logic', q, pop_q) %}
{% endmacro %}


{# =====================================================================
   D — player-level stats
   ===================================================================== #}

{% macro _audit_player_batting_vs_team_box() %}
    {#
        D1 — summing players' batting lines BY team_id must reproduce the
        team box score in fact_games (which int_team_game_box sums BY
        is_home). Grain: one row per (game, team), completed games with an
        ACCEPTED box score only.

        CHANGED: now restricted to has_box_score = 1. Games whose box score
        fact_games rejected (player runs didn't match the final score) have
        NULL team stats, so every one of them failed here twice (once per
        team) -- that was the ~506 failures. Those games now have their own
        audit, integrity_fact_games_rejected_box_scores, below. What's left
        here catches player rows whose team_id disagrees with is_home /
        dim_games' home and away team ids.
    #}
    {% set test_name = 'reconciliation_player_batting_vs_team_box' %}
    {% if not _require_models(test_name, ['int_player_game_stats', 'fact_games', 'dim_games']) %}{{ return(none) }}{% endif %}
    {% set ctes %}
WITH player_totals AS (
    SELECT game_pk, team_id,
           SUM(batting_hits)      AS player_hits,
           SUM(batting_runs)      AS player_runs,
           SUM(batting_home_runs) AS player_hr
    FROM {{ ref('int_player_game_stats') }}
    GROUP BY game_pk, team_id
),
team_box AS (
    SELECT f.game_pk, g.home_team_id AS team_id,
           f.home_team_hits AS team_hits, f.home_team_runs AS team_runs, f.home_home_runs AS team_hr
    FROM {{ ref('fact_games') }} f
    JOIN {{ ref('dim_games') }} g ON g.game_pk = f.game_pk
    WHERE f.is_final = 1 AND f.has_box_score = 1
    UNION ALL
    SELECT f.game_pk, g.away_team_id,
           f.away_team_hits, f.away_team_runs, f.away_home_runs
    FROM {{ ref('fact_games') }} f
    JOIN {{ ref('dim_games') }} g ON g.game_pk = f.game_pk
    WHERE f.is_final = 1 AND f.has_box_score = 1
)
    {% endset %}
    {% set q %}
SELECT t.game_pk, t.team_id,
       t.team_hits, p.player_hits,
       t.team_runs, p.player_runs,
       t.team_hr,   p.player_hr
FROM team_box t
JOIN player_totals p
    ON p.game_pk = t.game_pk AND p.team_id = t.team_id
WHERE ISNULL(t.team_hits, -1) <> ISNULL(p.player_hits, -1)
   OR ISNULL(t.team_runs, -1) <> ISNULL(p.player_runs, -1)
   OR ISNULL(t.team_hr,   -1) <> ISNULL(p.player_hr,   -1)
    {% endset %}
    {% set pop_q %}
{{ ctes }}
SELECT COUNT(*) AS cnt
FROM team_box t
JOIN player_totals p
    ON p.game_pk = t.game_pk AND p.team_id = t.team_id
    {% endset %}
    {% do _log_custom_audit('int_player_game_stats', 'int_player_game_stats', test_name, 'custom_reconciliation', q, pop_q, ctes=ctes) %}
{% endmacro %}


{% macro _audit_fact_games_rejected_box_scores() %}
    {#
        D1b (NEW) — completed games that HAVE player lines but whose box
        score fact_games rejected because the player runs don't add up to
        the final score. Each of these games has NULL box-score stats and
        has_box_score = 0 in fact_games.

        player_rows helps with diagnosis: roughly 20-50 is a normal game;
        about double means a duplicate load; well under 20 means missing
        players. Grain: games.
    #}
    {% set test_name = 'integrity_fact_games_rejected_box_scores' %}
    {% if not _require_models(test_name, ['fact_games', 'dim_games', 'int_team_game_box']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT f.game_pk, f.season, bx.player_rows,
       g.home_score, bx.home_runs AS box_home_runs,
       g.away_score, bx.away_runs AS box_away_runs
FROM {{ ref('fact_games') }} f
JOIN {{ ref('dim_games') }} g          ON g.game_pk  = f.game_pk
JOIN {{ ref('int_team_game_box') }} bx ON bx.game_pk = f.game_pk
WHERE f.is_final = 1
  AND f.has_box_score = 0
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt
FROM {{ ref('fact_games') }} f
JOIN {{ ref('int_team_game_box') }} bx ON bx.game_pk = f.game_pk
WHERE f.is_final = 1
    {% endset %}
    {% do _log_custom_audit('fact_games', 'fact_games', test_name, 'custom_integrity', q, pop_q) %}
{% endmacro %}


{% macro _audit_player_game_stat_sanity() %}
    {#
        D2 — impossible player game lines: more hits than at-bats, more
        extra-base hits than hits, more home runs than runs scored (every
        HR scores the batter), earned runs above runs allowed, and
        fielding chances not equal to putouts + assists + errors.
    #}
    {% set test_name = 'business_logic_player_game_stat_sanity' %}
    {% if not _require_models(test_name, ['int_player_game_stats']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT s.game_pk, s.player_id, s.player_name, x.violations
FROM {{ ref('int_player_game_stats') }} s
CROSS APPLY (
    SELECT CONCAT(
        CASE WHEN s.batting_hits > s.batting_at_bats THEN 'hits_gt_at_bats;' END,
        CASE WHEN s.batting_doubles + s.batting_triples + s.batting_home_runs > s.batting_hits
             THEN 'xbh_gt_hits;' END,
        CASE WHEN s.batting_home_runs > s.batting_runs THEN 'hr_gt_runs;' END,
        CASE WHEN s.pitching_earned_runs > s.pitching_runs_allowed THEN 'er_gt_runs_allowed;' END,
        CASE WHEN s.fielding_chances <> s.fielding_putouts + s.fielding_assists + s.fielding_errors
             THEN 'chances_ne_po_a_e;' END,
        CASE WHEN s.batting_at_bats < 0 OR s.batting_hits < 0 OR s.pitching_outs_recorded < 0
             THEN 'negative_values;' END
    ) AS violations
) x
WHERE x.violations <> ''
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('int_player_game_stats') }}
    {% endset %}
    {% do _log_custom_audit('int_player_game_stats', 'int_player_game_stats', test_name, 'custom_business_logic', q, pop_q) %}
{% endmacro %}


{% macro _audit_dim_player_one_current_row() %}
    {#
        D3 — dim_player is SCD2 (version_number, effective/end date,
        is_current). Per player: exactly ONE current version, no version
        ending before it starts, and no version starting on or before the
        previous version's end date (overlap).

        Name kept for continuity of history in test_sql. Before the
        dim_player fix, every failure here was an inverted range --
        lead() guarantees exactly one current row.

        CHANGED: added the overlap check.
    #}
    {% set test_name = 'uniqueness_dim_player_one_current_row' %}
    {% if not _require_models(test_name, ['dim_player']) %}{{ return(none) }}{% endif %}
    {% set ctes %}
WITH v AS (
    SELECT player_id, is_current, effective_date, end_date,
           LAG(end_date) OVER (PARTITION BY player_id ORDER BY version_number) AS prev_end_date
    FROM {{ ref('dim_player') }}
)
    {% endset %}
    {% set q %}
SELECT player_id,
       SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END)               AS current_rows,
       SUM(CASE WHEN end_date < effective_date THEN 1 ELSE 0 END)    AS inverted_ranges,
       SUM(CASE WHEN effective_date <= prev_end_date THEN 1 ELSE 0 END) AS overlapping_ranges
FROM v
GROUP BY player_id
HAVING SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END) <> 1
    OR SUM(CASE WHEN end_date < effective_date THEN 1 ELSE 0 END) > 0
    OR SUM(CASE WHEN effective_date <= prev_end_date THEN 1 ELSE 0 END) > 0
    {% endset %}
    {% set pop_q %}
SELECT COUNT(DISTINCT player_id) AS cnt FROM {{ ref('dim_player') }}
    {% endset %}
    {% do _log_custom_audit('dim_player', 'dim_player', test_name, 'custom_uniqueness', q, pop_q, ctes=ctes) %}
{% endmacro %}


{# =====================================================================
   E — dimension integrity
   ===================================================================== #}

{% macro _audit_dim_teams_unique() %}
    {#
        E1 — fact_games joins dim_teams on (team_id, season). A duplicate
        of that pair fans fact_games out into duplicate games; a
        duplicate team_key breaks every join on the surrogate key.
    #}
    {% set test_name = 'uniqueness_dim_teams_team_id_season' %}
    {% if not _require_models(test_name, ['dim_teams']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT 'duplicate_team_id_season' AS issue,
       CAST(team_id AS varchar(50)) AS team_id, CAST(season AS varchar(10)) AS season,
       CAST(NULL AS varchar(50)) AS team_key, COUNT(*) AS row_count
FROM {{ ref('dim_teams') }}
GROUP BY team_id, season
HAVING COUNT(*) > 1
UNION ALL
SELECT 'duplicate_team_key', NULL, NULL, team_key, COUNT(*)
FROM {{ ref('dim_teams') }}
GROUP BY team_key
HAVING COUNT(*) > 1
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('dim_teams') }}
    {% endset %}
    {% do _log_custom_audit('dim_teams', 'dim_teams', test_name, 'custom_uniqueness', q, pop_q) %}
{% endmacro %}


{% macro _audit_dim_season_date_order() %}
    {#
        E2 — season calendar dates must be in order: the regular season
        starts before it ends, the postseason follows the regular season,
        the All-Star game falls inside the regular season. NULL dates are
        ignored (not every season has every phase).
    #}
    {% set test_name = 'business_logic_dim_season_date_order' %}
    {% if not _require_models(test_name, ['dim_season']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT s.season, x.violations,
       s.regular_season_start_date, s.regular_season_end_date,
       s.post_season_start_date, s.post_season_end_date,
       s.all_star_date, s.season_start_date, s.season_end_date
FROM {{ ref('dim_season') }} s
CROSS APPLY (
    SELECT CONCAT(
        CASE WHEN s.regular_season_start_date > s.regular_season_end_date THEN 'regular_start_after_end;' END,
        CASE WHEN s.post_season_start_date < s.regular_season_end_date THEN 'post_before_regular_end;' END,
        CASE WHEN s.post_season_start_date > s.post_season_end_date THEN 'post_start_after_end;' END,
        CASE WHEN s.season_start_date > s.season_end_date THEN 'season_start_after_end;' END,
        CASE WHEN s.all_star_date < s.regular_season_start_date
               OR s.all_star_date > s.regular_season_end_date THEN 'all_star_outside_regular;' END
    ) AS violations
) x
WHERE x.violations <> ''
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('dim_season') }}
    {% endset %}
    {% do _log_custom_audit('dim_season', 'dim_season', test_name, 'custom_business_logic', q, pop_q) %}
{% endmacro %}


{# =====================================================================
   F — pitch / at-bat / batted-ball / mart level
   ===================================================================== #}

{% macro _audit_fact_at_bats_ranges() %}
    {#
        F1 — at-bat level plausibility: final count within 0–4 balls and
        0–3 strikes, 0–2 outs when the batter came up, at least one
        pitch, max speed >= average speed, and the at-bat ending after it
        started.
    #}
    {% set test_name = 'range_fact_at_bats_values' %}
    {% if not _require_models(test_name, ['fact_at_bats']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT a.game_pk, a.at_bat_index, a.batter_id, a.pitcher_id, x.violations
FROM {{ ref('fact_at_bats') }} a
CROSS APPLY (
    SELECT CONCAT(
        CASE WHEN a.final_balls NOT BETWEEN 0 AND 4 THEN 'final_balls;' END,
        CASE WHEN a.final_strikes NOT BETWEEN 0 AND 3 THEN 'final_strikes;' END,
        CASE WHEN a.outs_when_up NOT BETWEEN 0 AND 2 THEN 'outs_when_up;' END,
        CASE WHEN a.total_pitches < 1 THEN 'no_pitches;' END,
        CASE WHEN a.max_pitch_speed < a.avg_pitch_speed THEN 'max_speed_lt_avg;' END,
        CASE WHEN a.ab_end_time < a.ab_start_time THEN 'ends_before_start;' END
    ) AS violations
) x
WHERE x.violations <> ''
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('fact_at_bats') }}
    {% endset %}
    {% do _log_custom_audit('fact_at_bats', 'fact_at_bats', test_name, 'custom_range', q, pop_q) %}
{% endmacro %}


{% macro _audit_fact_batted_balls_ranges() %}
    {#
        F2 — batted-ball physics: exit velocity 0–125 mph, launch angle
        -90..90 degrees, distance 0–550 ft. Also every barrel must be a
        hard-hit ball (barrels need 98+ mph; hard-hit is 95+).
    #}
    {% set test_name = 'range_fact_batted_balls_values' %}
    {% if not _require_models(test_name, ['fact_batted_balls']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT b.event_id, b.game_pk, b.at_bat_index, b.pitch_number,
       b.exit_velocity, b.launch_angle, b.hit_distance, b.is_barrel, b.is_hard_hit,
       x.violations
FROM {{ ref('fact_batted_balls') }} b
CROSS APPLY (
    SELECT CONCAT(
        CASE WHEN b.exit_velocity NOT BETWEEN 0 AND 125 THEN 'exit_velocity;' END,
        CASE WHEN b.launch_angle NOT BETWEEN -90 AND 90 THEN 'launch_angle;' END,
        CASE WHEN b.hit_distance NOT BETWEEN 0 AND 550 THEN 'hit_distance;' END,
        CASE WHEN b.is_barrel = 1 AND b.is_hard_hit = 0 THEN 'barrel_not_hard_hit;' END
    ) AS violations
) x
WHERE x.violations <> ''
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('fact_batted_balls') }}
    {% endset %}
    {% do _log_custom_audit('fact_batted_balls', 'fact_batted_balls', test_name, 'custom_range', q, pop_q) %}
{% endmacro %}


{% macro _audit_pitches_enriched_consistency() %}
    {#
        F3 — pitch-level physics and flag logic:
          - release speed 30–110 mph; the ball can't speed up on the way
            to the plate (plate_speed <= release_speed); spin 0–4000 rpm
          - count 0–4 balls, 0–3 strikes; 0–2 outs
          - a whiff must be a swing; a called strike can't be a swing;
            a chase is by definition outside the zone

        CHANGED: chase_in_zone now compares is_chase against
        is_in_strike_zone -- the same computed box (plate_x within
        +/-0.83 ft, plate_z between sz_bot and sz_top) that is_chase is
        derived from. It previously used is_in_zone, which comes from
        Statcast's zone number (dim_zone) -- a different zone definition,
        so edge pitches disagreed by construction and inflated this check.
    #}
    {% set test_name = 'business_logic_pitches_enriched_consistency' %}
    {% if not _require_models(test_name, ['int_pitches_enriched']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT p.event_id, p.game_pk, p.at_bat_index, p.pitch_number, x.violations
FROM {{ ref('int_pitches_enriched') }} p
CROSS APPLY (
    SELECT CONCAT(
        CASE WHEN p.release_speed NOT BETWEEN 30 AND 110 THEN 'release_speed;' END,
        CASE WHEN p.plate_speed > p.release_speed THEN 'plate_speed_gt_release;' END,
        CASE WHEN p.spin_rate NOT BETWEEN 0 AND 4000 THEN 'spin_rate;' END,
        CASE WHEN p.balls NOT BETWEEN 0 AND 4 THEN 'balls;' END,
        CASE WHEN p.strikes NOT BETWEEN 0 AND 3 THEN 'strikes;' END,
        CASE WHEN p.outs_when_up NOT BETWEEN 0 AND 2 THEN 'outs_when_up;' END,
        CASE WHEN p.is_whiff = 1 AND p.is_swing = 0 THEN 'whiff_without_swing;' END,
        CASE WHEN p.is_called_strike = 1 AND p.is_swing = 1 THEN 'called_strike_with_swing;' END,
        CASE WHEN p.is_chase = 1 AND p.is_in_strike_zone = 1 THEN 'chase_in_zone;' END
    ) AS violations
) x
WHERE p.is_pitch = 1
  AND x.violations <> ''
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('int_pitches_enriched') }} WHERE is_pitch = 1
    {% endset %}
    {% do _log_custom_audit('int_pitches_enriched', 'int_pitches_enriched', test_name, 'custom_business_logic', q, pop_q) %}
{% endmacro %}


{% macro _audit_pitcher_arsenal_usage_total() %}
    {#
        F4 — a pitcher's pitch mix for a season must add up to 100%.
        Accepts either scale (0–100 or 0–1) within rounding tolerance,
        since usage_pct's scale isn't visible from the schema.
    #}
    {% set test_name = 'business_logic_pitcher_arsenal_usage_total' %}
    {% if not _require_models(test_name, ['mart_pitcher_arsenal']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT season, pitcher_id, SUM(usage_pct) AS total_usage_pct, COUNT(*) AS pitch_types
FROM {{ ref('mart_pitcher_arsenal') }}
GROUP BY season, pitcher_id
HAVING NOT (ABS(SUM(usage_pct) - 100.0) <= 1.0 OR ABS(SUM(usage_pct) - 1.0) <= 0.01)
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt
FROM (SELECT season, pitcher_id FROM {{ ref('mart_pitcher_arsenal') }} GROUP BY season, pitcher_id) d
    {% endset %}
    {% do _log_custom_audit('mart_pitcher_arsenal', 'mart_pitcher_arsenal', test_name, 'custom_business_logic', q, pop_q) %}
{% endmacro %}


{% macro _audit_umpire_challenge_totals() %}
    {#
        F5 — every challenge is either overturned or upheld, and the
        overturn rate must match those counts (either 0–100 or 0–1
        scale, within rounding).
    #}
    {% set test_name = 'business_logic_umpire_challenge_totals' %}
    {% if not _require_models(test_name, ['fact_umpire_performance']) %}{{ return(none) }}{% endif %}
    {% set q %}
SELECT u.game_pk, u.official_id, u.total_challenges, u.overturned_challenges,
       u.upheld_challenges, u.overturn_rate_pct, x.violations
FROM {{ ref('fact_umpire_performance') }} u
CROSS APPLY (
    SELECT CONCAT(
        CASE WHEN ISNULL(u.overturned_challenges, 0) + ISNULL(u.upheld_challenges, 0)
                  <> ISNULL(u.total_challenges, 0)
             THEN 'overturned_plus_upheld_ne_total;' END,
        CASE WHEN u.total_challenges > 0
              AND NOT (
                    ABS(u.overturn_rate_pct - 100.0 * u.overturned_challenges / u.total_challenges) <= 0.1
                 OR ABS(u.overturn_rate_pct - 1.0 * u.overturned_challenges / u.total_challenges) <= 0.001
              )
             THEN 'overturn_rate_mismatch;' END
    ) AS violations
) x
WHERE x.violations <> ''
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('fact_umpire_performance') }}
    {% endset %}
    {% do _log_custom_audit('fact_umpire_performance', 'fact_umpire_performance', test_name, 'custom_business_logic', q, pop_q) %}
{% endmacro %}


{# =====================================================================
   Entry point
   ===================================================================== #}

{% macro run_custom_audits(only=none) %}
    {#
        Run all audits, one by name, or a list of names:

            dbt run-operation run_custom_audits
            dbt run-operation run_custom_audits --args '{"only": "uniqueness_fact_games_game_pk"}'
            dbt run-operation run_custom_audits --args '{"only": ["range_fact_games_values", "uniqueness_dim_teams_team_id_season"]}'

        Each check logs its own rows as it completes — nothing is
        batched/held — so a failure partway through still leaves earlier
        results logged.

        The registry below is still an explicit name -> macro list (no
        dynamic lookup by string), so grep/IDE can trace every check to
        its macro, and an unknown name in `only` fails loudly.
    #}
    {% set registry = [
        ('business_logic_no_hitter_flag_consistency',          _audit_no_hitter_flag),
        ('business_logic_shutout_flag_consistency',            _audit_shutout_flag),
        ('business_logic_run_diff_consistency',                _audit_run_diff),
        ('business_logic_winning_team_consistency',            _audit_winning_team),
        ('business_logic_perfect_game_implies_no_hitter',      _audit_perfect_game_implies_no_hitter),
        ('range_fact_games_rare_event_counts',                 _audit_fact_games_rare_event_counts),
        ('reconciliation_box_score_batting_vs_pitching',       _audit_box_score_batting_vs_pitching),
        ('business_logic_fact_games_stat_composition',         _audit_fact_games_stat_composition),
        ('business_logic_winner_loser_distinct',               _audit_winner_loser_distinct),
        ('reconciliation_game_details_runs_vs_dim_games_score', _audit_fact_games_vs_dim_games_score),
        ('integrity_fact_games_dimension_keys',                _audit_fact_games_dimension_keys),
        ('uniqueness_fact_games_game_pk',                      _audit_fact_games_unique_game_pk),
        ('range_fact_games_values',                            _audit_fact_games_ranges),
        ('integrity_team_standings_two_rows_per_game',         _audit_team_standings_two_rows_per_game),
        ('business_logic_team_standings_symmetry',             _audit_team_standings_symmetry),
        ('business_logic_standings_by_date_identity',          _audit_standings_by_date_identity),
        ('reconciliation_player_batting_vs_team_box',          _audit_player_batting_vs_team_box),
        ('integrity_fact_games_rejected_box_scores',           _audit_fact_games_rejected_box_scores),
        ('business_logic_player_game_stat_sanity',             _audit_player_game_stat_sanity),
        ('uniqueness_dim_player_one_current_row',              _audit_dim_player_one_current_row),
        ('uniqueness_dim_teams_team_id_season',                _audit_dim_teams_unique),
        ('business_logic_dim_season_date_order',               _audit_dim_season_date_order),
        ('range_fact_at_bats_values',                          _audit_fact_at_bats_ranges),
        ('range_fact_batted_balls_values',                     _audit_fact_batted_balls_ranges),
        ('business_logic_pitches_enriched_consistency',        _audit_pitches_enriched_consistency),
        ('business_logic_pitcher_arsenal_usage_total',         _audit_pitcher_arsenal_usage_total),
        ('business_logic_umpire_challenge_totals',             _audit_umpire_challenge_totals),
    ] %}

    {% set valid_names = [] %}
    {% for name, _ in registry %}
        {% do valid_names.append(name) %}
    {% endfor %}

    {% if only is none %}
        {% set selected = valid_names %}
    {% elif only is string %}
        {% set selected = [only] %}
    {% else %}
        {% set selected = only %}
    {% endif %}

    {% for name in selected %}
        {% if name not in valid_names %}
            {{ exceptions.raise_compiler_error(
                "Unknown audit '" ~ name ~ "'. Valid names: " ~ valid_names | join(', ')
            ) }}
        {% endif %}
    {% endfor %}

    {{ log("Running " ~ selected | length ~ " of " ~ valid_names | length ~ " custom MLB warehouse audits...", info=True) }}

    {% for name, audit_macro in registry %}
        {% if name in selected %}
            {% do audit_macro() %}
        {% endif %}
    {% endfor %}

    {{ log("Custom audits complete.", info=True) }}
{% endmacro %}
