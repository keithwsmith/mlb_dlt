{{
    config(
        materialized='view',
        schema='gold'
    )
}}

/*
    GOLD: one row per model -- is its data fit to use right now?

    Combines the two operations logs into a single traffic light per model:
      - dbo.pipeline (one row per model per pipeline run, written by
        pipeline.py): latest status, row counts, run time, and failures in
        the last 7 days.
      - dbo.dbt_test_log (one row per test per run, written by the
        log_test_results on-run-end hook): the LATEST result of every test.

    A view, not a table, so it always reflects the most recent run and test
    results. It reads the same tables as ETL Monitor; use it from a
    dashboard, Cube, or a query when ETL Monitor isn't running.

    Model names in both logs can be 'package.model' (e.g.
    'baseball_dw.dim_games'); everything after the last '.' is kept.

    health:
      red   -- latest run failed, or a test is failing / erroring
      amber -- tests warn, the latest run left 0 rows, or tests haven't run
               since the latest load
      green -- otherwise

    Every row has a model_name: log rows whose model name is NULL, blank,
    or ends in '.' (nothing left after the package prefix) are dropped
    before anything is grouped or joined.
*/

with pipeline_runs_named as (

    select
        p.*,
        nullif(ltrim(rtrim(
            case
                when charindex('.', p.model_name) > 0
                then right(p.model_name, charindex('.', reverse(p.model_name)) - 1)
                else p.model_name
            end
        )), '')                                                             as short_model_name
    from {{ source('ops', 'pipeline') }} p

),

pipeline_runs as (

    select
        id,
        short_model_name                                                    as model_name,
        model_layer,
        lower(status)                                                       as status,
        start_record_count,
        end_record_count,
        records_added,
        elapsed_seconds,
        start_time,
        error_message
    from pipeline_runs_named
    where short_model_name is not null

),

latest_run as (

    select *
    from (
        select
            *,
            row_number() over (partition by model_name order by id desc)    as rn
        from pipeline_runs
    ) r
    where rn = 1

),

recent_runs as (

    select
        model_name,
        count(*)                                                            as runs_last_7_days,
        sum(case when status in ('fail', 'failed', 'failure', 'error')
                 then 1 else 0 end)                                         as failed_runs_last_7_days
    from pipeline_runs
    where start_time >= dateadd(day, -7, sysutcdatetime())
    group by model_name

),

test_log_named as (

    select
        nullif(ltrim(rtrim(
            case
                when charindex('.', l.model_name) > 0
                then right(l.model_name, charindex('.', reverse(l.model_name)) - 1)
                else l.model_name
            end
        )), '')                                                             as model_name,
        l.test_name,
        lower(l.status)                                                     as status,
        l.failures,
        l.run_started_at
    from {{ source('ops', 'dbt_test_log') }} l

),

latest_tests as (

    select *
    from (
        select
            *,
            -- Partitions on the short name, so 'pkg.model' and 'model'
            -- count as the same model.
            row_number() over (
                partition by model_name, test_name
                order by run_started_at desc
            )                                                               as rn
        from test_log_named
        where model_name is not null
    ) t
    where rn = 1

),

test_summary as (

    select
        model_name,
        count(*)                                                            as tests_total,
        sum(case when status = 'pass' then 1 else 0 end)                    as tests_passed,
        sum(case when status in ('fail', 'error') then 1 else 0 end)        as tests_failed,
        sum(case when status = 'warn' then 1 else 0 end)                    as tests_warned,
        sum(case when status in ('fail', 'error', 'warn')
                 then coalesce(failures, 0) else 0 end)                     as failing_rows,
        max(run_started_at)                                                 as last_tested_at
    from latest_tests
    group by model_name

),

all_models as (

    select model_name from latest_run
    union
    select model_name from test_summary

)

select
    m.model_name,
    r.model_layer,

    r.status                                                                as last_run_status,
    r.start_time                                                            as last_run_at,
    r.start_record_count,
    r.end_record_count,
    r.records_added,
    r.elapsed_seconds,
    r.error_message,
    coalesce(h.runs_last_7_days, 0)                                         as runs_last_7_days,
    coalesce(h.failed_runs_last_7_days, 0)                                  as failed_runs_last_7_days,

    coalesce(t.tests_total, 0)                                              as tests_total,
    coalesce(t.tests_passed, 0)                                             as tests_passed,
    coalesce(t.tests_failed, 0)                                             as tests_failed,
    coalesce(t.tests_warned, 0)                                             as tests_warned,
    coalesce(t.failing_rows, 0)                                             as failing_rows,
    t.last_tested_at,

    case
        when r.status in ('fail', 'failed', 'failure', 'error')
          or coalesce(t.tests_failed, 0) > 0                                then 'red'
        when coalesce(t.tests_warned, 0) > 0
          or r.end_record_count = 0
          or (t.last_tested_at is not null and r.start_time > t.last_tested_at) then 'amber'
        else 'green'
    end                                                                     as health
from all_models m
left join latest_run r
    on r.model_name = m.model_name
left join recent_runs h
    on h.model_name = m.model_name
left join test_summary t
    on t.model_name = m.model_name
where m.model_name is not null
