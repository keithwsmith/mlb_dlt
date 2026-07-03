{#
    macros/audits/custom_audits.sql
    -----------------------------------------------------------------------
    Content-correctness audits for the MLB warehouse, as dbt macros instead
    of a standalone T-SQL stored procedure. Same 7 checks, same destination
    (silver.test_sql — your existing custom-SQL-test log table), but now:

      - reads go through {{ ref() }} instead of hardcoded schema.table,
        so these audits automatically follow your models across
        dev/prod/whatever targets you run against
      - each check is DRY'd through one helper macro (_log_custom_audit)
        that runs the check ONCE and derives both the failure count and
        a JSON sample from that single result set in Jinja — earlier
        version wrapped the query in a COUNT(*) subquery, which breaks
        for any check written as a CTE (WITH ... AS (...) SELECT ...)
        since SQL Server requires WITH to start a batch, not sit nested
        inside a derived table's parentheses
      - writes to TWO tables: silver.test_sql (full detail, unchanged)
        AND dbo.transformation_audit_log (a summary row), so every check
        also shows up on ETLMonitor's Model Audit tab alongside your
        audit_helper results. See _log_custom_audit's docstring for why
        the model_name written to transformation_audit_log is
        "<model> :: <test_name>" rather than the plain model name —
        that table's summary view is one-row-per-model_name-latest-only,
        and several of these checks share a base model (fact_games x4).
      - runnable on demand via:
            dbt run-operation run_custom_audits
        or a single check via:
            dbt run-operation run_custom_audits --args '{"only": "business_logic_shutout_flag_consistency"}'

    DELIBERATELY NOT wired into on-run-end (unlike log_manual_dbt_run):
    these scan full fact tables (fact_pitches in particular) — cheap for
    log_manual_dbt_run's per-model row count, not cheap to run on every
    single dbt invocation. Run it explicitly, on a schedule, or call it
    from ETLMonitor.py the same way dbt itself gets invoked.
#}


{% macro _log_custom_audit(table_name, model_name, test_name, test_type, failure_query, population_query) %}
    {#
        Runs `failure_query` (a SELECT that returns ONLY the failing rows —
        column list can be anything) and logs:

          1. ONE row into silver.test_sql — full detail: failure_count,
             up to 50 sample failures as JSON, and the check's own SQL
             text, same as your other custom test_sql rows.

          2. ONE row into dbo.transformation_audit_log — a summary row so
             this check shows up on ETLMonitor's Model Audit tab alongside
             your audit_helper results. That table's summary view does
             ROW_NUMBER() PARTITION BY model_name ORDER BY run_at DESC,
             i.e. "one row per model_name, latest only." Since several of
             these checks share a base model_name (fact_games has 4), a
             plain model_name would make 3 of every 4 checks silently
             disappear from the summary list. So the value written to
             THIS table is `model_name :: test_name` — distinct per
             check, so all seven show up as their own rows and each has
             its own independent run history when you drill in.

        `population_query` is a scalar `SELECT COUNT(*) AS cnt ...`
        representing the denominator ("rows compared") — needed to
        compute perfect_match_pct, which is what actually drives the
        Model Audit tab's red/amber/green coloring
        (_transform_audit_status_color: <99% danger, <99.9% warning).

        failure_query is run EXACTLY ONCE, as-is, at the top level —
        NOT wrapped in a subquery. Several checks (A1, A2) are CTEs
        (WITH ... AS (...) SELECT ...), and SQL Server requires WITH to
        be the first thing in a batch/statement; nesting one inside a
        derived table's parentheses is a hard syntax error, not
        something tunable. Count and JSON sample are both derived in
        Jinja from the one result set instead.
    #}

    {% set result = run_query(failure_query) %}
    {% set failure_count = result.rows | length %}

    {% set pop_result = run_query(population_query) %}
    {% set total_rows = pop_result.rows[0]['cnt'] if (pop_result and pop_result.rows | length > 0) else 0 %}
    {% set perfect_match_pct = (100.0 * (total_rows - failure_count) / total_rows) if total_rows > 0 else 100.0 %}

    {% set sample_rows = [] %}
    {% for row in result.rows[:50] %}
        {% set row_dict = {} %}
        {% for col_name in result.column_names %}
            {#
                Stringify every value. These come back as a mix of agate
                Decimal/Number/Text/DateTime types, and dbt's tojson()
                doesn't reliably serialize all of them (Decimal in
                particular) — a plain string is more than sufficient for
                a human-readable audit sample and sidesteps the issue
                entirely rather than special-casing types.
            #}
            {% do row_dict.update({col_name: (row[col_name] | string)}) %}
        {% endfor %}
        {% do sample_rows.append(row_dict) %}
    {% endfor %}
    {% set sample_json = tojson(sample_rows) %}

    {% set safe_sql_text = (failure_query | trim | replace("'", "''")) %}
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


{% macro _audit_reconciliation_pitch_counts() %}
    {#
        A1 — fact_at_bats.total_pitches (summed per game) should equal the
        actual row count in fact_pitches for that game. A mismatch usually
        means one table got a partial/duplicate load the other didn't.
    #}
    {% set q %}
WITH ab_totals AS (
    SELECT game_pk, SUM(total_pitches) AS ab_pitch_total
    FROM {{ ref('fact_at_bats') }}
    GROUP BY game_pk
),
fp_totals AS (
    SELECT game_pk, COUNT(*) AS fp_pitch_total
    FROM {{ ref('fact_pitches') }}
    GROUP BY game_pk
)
SELECT a.game_pk, a.ab_pitch_total, f.fp_pitch_total, (a.ab_pitch_total - f.fp_pitch_total) AS diff
FROM ab_totals a
JOIN fp_totals f ON f.game_pk = a.game_pk
WHERE a.ab_pitch_total <> f.fp_pitch_total
    {% endset %}
    {% set pop_q %}
SELECT COUNT(DISTINCT game_pk) AS cnt FROM {{ ref('fact_at_bats') }}
    {% endset %}
    {% do _log_custom_audit(
        table_name='fact_pitches / fact_at_bats',
        model_name='fact_pitches',
        test_name='reconciliation_pitch_count_at_bats_vs_pitches',
        test_type='custom_reconciliation',
        failure_query=q,
        population_query=pop_q
    ) %}
{% endmacro %}


{% macro _audit_reconciliation_batted_balls() %}
    {#
        A2 — fact_batted_balls count per game must never exceed the count
        of fact_pitches rows flagged is_in_play=1 for that game (a batted
        ball can only come from an in-play pitch).
    #}
    {% set q %}
WITH bb AS (
    SELECT game_pk, COUNT(*) AS batted_ball_count
    FROM {{ ref('fact_batted_balls') }}
    GROUP BY game_pk
),
inplay AS (
    SELECT game_pk, COUNT(*) AS in_play_pitch_count
    FROM {{ ref('fact_pitches') }}
    WHERE is_in_play = 1
    GROUP BY game_pk
)
SELECT b.game_pk, b.batted_ball_count, i.in_play_pitch_count, (b.batted_ball_count - i.in_play_pitch_count) AS excess
FROM bb b
JOIN inplay i ON i.game_pk = b.game_pk
WHERE b.batted_ball_count > i.in_play_pitch_count
    {% endset %}
    {% set pop_q %}
SELECT COUNT(DISTINCT game_pk) AS cnt FROM {{ ref('fact_batted_balls') }}
    {% endset %}
    {% do _log_custom_audit(
        table_name='fact_batted_balls / fact_pitches',
        model_name='fact_batted_balls',
        test_name='reconciliation_batted_balls_exceed_inplay_pitches',
        test_type='custom_reconciliation',
        failure_query=q,
        population_query=pop_q
    ) %}
{% endmacro %}


{% macro _audit_uniqueness_pitches() %}
    {#
        A3 — root-cause helper for A1: the most common cause of the pitch
        count mismatch is a re-run loading the same game's pitches twice.
        (game_pk, at_bat_index, pitch_number) should be unique.
    #}
    {% set q %}
SELECT game_pk, at_bat_index, pitch_number, COUNT(*) AS dup_count
FROM {{ ref('fact_pitches') }}
GROUP BY game_pk, at_bat_index, pitch_number
HAVING COUNT(*) > 1
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('fact_pitches') }}
    {% endset %}
    {% do _log_custom_audit(
        table_name='fact_pitches',
        model_name='fact_pitches',
        test_name='uniqueness_game_atbat_pitch_number',
        test_type='custom_uniqueness',
        failure_query=q,
        population_query=pop_q
    ) %}
{% endmacro %}


{% macro _audit_no_hitter_flag() %}
    {#
        B1 — is_no_hitter=1 requires the losing side to have shown 0 hits.
    #}
    {% set q %}
SELECT game_pk, home_team_key, away_team_key, winning_team_id, home_team_hits, away_team_hits, is_no_hitter
FROM {{ ref('fact_games') }}
WHERE is_no_hitter = 1
  AND (
        (winning_team_id = home_team_key AND ISNULL(away_team_hits, -1) <> 0)
     OR (winning_team_id = away_team_key AND ISNULL(home_team_hits, -1) <> 0)
      )
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('fact_games') }}
    {% endset %}
    {% do _log_custom_audit(
        table_name='fact_games',
        model_name='fact_games',
        test_name='business_logic_no_hitter_flag_consistency',
        test_type='custom_business_logic',
        failure_query=q,
        population_query=pop_q
    ) %}
{% endmacro %}


{% macro _audit_shutout_flag() %}
    {#
        B2 — is_shutout=1 requires the losing side to have shown 0 runs.
    #}
    {% set q %}
SELECT game_pk, home_team_key, away_team_key, winning_team_id, home_team_runs, away_team_runs, is_shutout
FROM {{ ref('fact_games') }}
WHERE is_shutout = 1
  AND (
        (winning_team_id = home_team_key AND ISNULL(away_team_runs, -1) <> 0)
     OR (winning_team_id = away_team_key AND ISNULL(home_team_runs, -1) <> 0)
      )
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('fact_games') }}
    {% endset %}
    {% do _log_custom_audit(
        table_name='fact_games',
        model_name='fact_games',
        test_name='business_logic_shutout_flag_consistency',
        test_type='custom_business_logic',
        failure_query=q,
        population_query=pop_q
    ) %}
{% endmacro %}


{% macro _audit_run_diff() %}
    {#
        B3 — run_diff must equal the actual run differential. Sign
        convention (home-away vs. away-home) isn't confirmable from the
        schema alone, so this accepts either and only flags rows where
        NEITHER holds — i.e. genuinely wrong, not just a sign choice.
    #}
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
    {% endset %}
    {% do _log_custom_audit(
        table_name='fact_games',
        model_name='fact_games',
        test_name='business_logic_run_diff_consistency',
        test_type='custom_business_logic',
        failure_query=q,
        population_query=pop_q
    ) %}
{% endmacro %}


{% macro _audit_winning_team() %}
    {#
        B4 — for completed, non-tie games, winning_team_id must match
        whichever side actually scored more runs.
    #}
    {% set q %}
SELECT game_pk, home_team_runs, away_team_runs, home_team_key, away_team_key, winning_team_id
FROM {{ ref('fact_games') }}
WHERE is_final = 1
  AND ISNULL(is_tie, 0) = 0
  AND home_team_runs IS NOT NULL AND away_team_runs IS NOT NULL
  AND (
        (home_team_runs > away_team_runs AND winning_team_id <> home_team_key)
     OR (away_team_runs > home_team_runs AND winning_team_id <> away_team_key)
      )
    {% endset %}
    {% set pop_q %}
SELECT COUNT(*) AS cnt FROM {{ ref('fact_games') }}
    {% endset %}
    {% do _log_custom_audit(
        table_name='fact_games',
        model_name='fact_games',
        test_name='business_logic_winning_team_consistency',
        test_type='custom_business_logic',
        failure_query=q,
        population_query=pop_q
    ) %}
{% endmacro %}


{% macro run_custom_audits(only=none) %}
    {#
        Entry point. Run all 7 audits, or just one by name:

            dbt run-operation run_custom_audits
            dbt run-operation run_custom_audits --args '{"only": "uniqueness_game_atbat_pitch_number"}'

        Each check logs its own row into silver.test_sql as it completes
        (see _log_custom_audit above) — nothing is batched/held in memory,
        so a failure partway through still leaves earlier results logged.

        Deliberately explicit (if/elif, not a dynamic macro-name lookup)
        so a typo in `only` fails loudly and IDE/grep can actually trace
        which macro backs which check name.
    #}
    {% set valid_names = [
        'reconciliation_pitch_count_at_bats_vs_pitches',
        'reconciliation_batted_balls_exceed_inplay_pitches',
        'uniqueness_game_atbat_pitch_number',
        'business_logic_no_hitter_flag_consistency',
        'business_logic_shutout_flag_consistency',
        'business_logic_run_diff_consistency',
        'business_logic_winning_team_consistency',
    ] %}

    {% if only and only not in valid_names %}
        {{ exceptions.raise_compiler_error(
            "Unknown audit '" ~ only ~ "'. Valid names: " ~ valid_names | join(', ')
        ) }}
    {% endif %}

    {% if only %}
        {{ log("Running single audit: " ~ only, info=True) }}
    {% else %}
        {{ log("Running all " ~ valid_names | length ~ " custom MLB warehouse audits...", info=True) }}
    {% endif %}

    {% if not only or only == 'reconciliation_pitch_count_at_bats_vs_pitches' %}
        {% do _audit_reconciliation_pitch_counts() %}
    {% endif %}
    {% if not only or only == 'reconciliation_batted_balls_exceed_inplay_pitches' %}
        {% do _audit_reconciliation_batted_balls() %}
    {% endif %}
    {% if not only or only == 'uniqueness_game_atbat_pitch_number' %}
        {% do _audit_uniqueness_pitches() %}
    {% endif %}
    {% if not only or only == 'business_logic_no_hitter_flag_consistency' %}
        {% do _audit_no_hitter_flag() %}
    {% endif %}
    {% if not only or only == 'business_logic_shutout_flag_consistency' %}
        {% do _audit_shutout_flag() %}
    {% endif %}
    {% if not only or only == 'business_logic_run_diff_consistency' %}
        {% do _audit_run_diff() %}
    {% endif %}
    {% if not only or only == 'business_logic_winning_team_consistency' %}
        {% do _audit_winning_team() %}
    {% endif %}

    {{ log("Custom audits complete.", info=True) }}
{% endmacro %}