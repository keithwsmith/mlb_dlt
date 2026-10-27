{{
    config(
        materialized='table',
        unique_key='date_key'
    )
}}

{#
    dim_date

    CHANGES (this revision)
    -----------------------
    1. The spine used to be TOP (24500) days from 1960-01-01, which ends on
       2027-01-28 -- every 2027 spring-training game would have come
       through with a NULL date key. It now runs through Dec 31 two years
       past the current year, so it extends itself on every build.

    2. date_key uses CONVERT(..., 112) instead of FORMAT(), which is far
       slower in SQL Server.

    3. day_of_week / is_weekend no longer depend on the session's
       SET DATEFIRST setting. Same values as before under the US default:
       1 = Sunday ... 7 = Saturday. (1900-01-07 was a Sunday.)
#}

WITH date_spine AS (
    SELECT
        CAST(DATEADD(day, n, '1960-01-01') AS DATE) AS date_actual
    FROM (
        SELECT TOP (DATEDIFF(day, '1960-01-01', DATEFROMPARTS(YEAR(GETDATE()) + 2, 12, 31)) + 1)
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM sys.all_objects a
        CROSS JOIN sys.all_objects b
    ) numbers
),

date_calcs AS (
    SELECT
        date_actual,
        CAST(CONVERT(CHAR(8), date_actual, 112) AS INT)        AS date_key,
        DATEDIFF(day, '1900-01-07', date_actual) % 7 + 1       AS day_of_week,
        DATENAME(WEEKDAY, date_actual)                         AS day_name,
        DATEPART(DAY, date_actual)                             AS day_of_month,
        DATEPART(DAYOFYEAR, date_actual)                       AS day_of_year,
        DATEPART(WEEK, date_actual)                            AS week_of_year,
        DATEPART(MONTH, date_actual)                           AS month_number,
        DATENAME(MONTH, date_actual)                           AS month_name,
        DATEPART(QUARTER, date_actual)                         AS quarter,
        DATEPART(YEAR, date_actual)                            AS year
    FROM date_spine
),

seasons_cte AS (
    SELECT
        season,
        TRY_CAST(spring_start_date         AS DATE) AS spring_start_date,
        TRY_CAST(spring_end_date           AS DATE) AS spring_end_date,
        TRY_CAST(regular_season_start_date AS DATE) AS regular_season_start_date,
        TRY_CAST(regular_season_end_date   AS DATE) AS regular_season_end_date,
        TRY_CAST(post_season_start_date    AS DATE) AS post_season_start_date,
        TRY_CAST(post_season_end_date      AS DATE) AS post_season_end_date,
        TRY_CAST(all_star_date             AS DATE) AS all_star_date
    FROM {{ source('dw', 'seasons') }}
)

SELECT
    dc.date_key,
    dc.date_actual,
    dc.day_of_week,
    dc.day_name,
    dc.day_of_month,
    dc.day_of_year,
    dc.week_of_year,
    dc.month_number,
    dc.month_name,
    dc.quarter,
    dc.year,
    CASE WHEN dc.day_of_week IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
    0 AS is_holiday,  -- can be enhanced with a holiday table

    s.season AS baseball_season,
    CASE WHEN dc.date_actual BETWEEN s.spring_start_date AND s.spring_end_date
         THEN 1 ELSE 0 END AS is_spring_training,
    CASE WHEN dc.date_actual BETWEEN s.regular_season_start_date AND s.regular_season_end_date
         THEN 1 ELSE 0 END AS is_regular_season,
    CASE WHEN dc.date_actual BETWEEN s.post_season_start_date AND s.post_season_end_date
         THEN 1 ELSE 0 END AS is_postseason,
    CASE WHEN dc.date_actual = s.all_star_date
         THEN 1 ELSE 0 END AS is_all_star,

    GETDATE() AS created_at,
    GETDATE() AS updated_at

FROM date_calcs dc
LEFT JOIN seasons_cte s
    ON dc.date_actual BETWEEN s.spring_start_date AND s.post_season_end_date
