{{
    config(
        materialized='table',
        unique_key='date_key'
    )
}}

-- Generate date dimension from 2000 to 2050
WITH date_spine AS (
    SELECT 
        DATEADD(day, n, '1960-01-01') AS date_actual
    FROM (
        SELECT TOP (24500) -- 66 years * 365.25 days
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM sys.all_objects a
        CROSS JOIN sys.all_objects b
    ) numbers
),

date_calcs AS (
    SELECT
        date_actual,
        CAST(FORMAT(date_actual, 'yyyyMMdd') AS INT) AS date_key,
        DATEPART(WEEKDAY, date_actual) AS day_of_week,
        DATENAME(WEEKDAY, date_actual) AS day_name,
        DATEPART(DAY, date_actual) AS day_of_month,
        DATEPART(DAYOFYEAR, date_actual) AS day_of_year,
        DATEPART(WEEK, date_actual) AS week_of_year,
        DATEPART(MONTH, date_actual) AS month_number,
        DATENAME(MONTH, date_actual) AS month_name,
        DATEPART(QUARTER, date_actual) AS quarter,
        DATEPART(YEAR, date_actual) AS year,
        CASE WHEN DATEPART(WEEKDAY, date_actual) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend
    FROM date_spine
),

seasons_cte AS (
    SELECT
        season,
        regular_season_start_date,
        regular_season_end_date,
        spring_start_date,
        spring_end_date,
        post_season_start_date,
        post_season_end_date,
        all_star_date
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
    dc.is_weekend,
    0 AS is_holiday, -- Can be enhanced with holiday table
    
    -- Baseball-specific fields
    s.season AS baseball_season,
    CASE 
        WHEN dc.date_actual BETWEEN TRY_CAST(s.spring_start_date AS DATE) 
            AND TRY_CAST(s.spring_end_date AS DATE) THEN 1 
        ELSE 0 
    END AS is_spring_training,
    CASE 
        WHEN dc.date_actual BETWEEN TRY_CAST(s.regular_season_start_date AS DATE) 
            AND TRY_CAST(s.regular_season_end_date AS DATE) THEN 1 
        ELSE 0 
    END AS is_regular_season,
    CASE 
        WHEN dc.date_actual BETWEEN TRY_CAST(s.post_season_start_date AS DATE) 
            AND TRY_CAST(s.post_season_end_date AS DATE) THEN 1 
        ELSE 0 
    END AS is_postseason,
    CASE 
        WHEN dc.date_actual = TRY_CAST(s.all_star_date AS DATE) THEN 1 
        ELSE 0 
    END AS is_all_star,
    
    GETDATE() AS created_at,
    GETDATE() AS updated_at
    
FROM date_calcs dc
LEFT JOIN seasons_cte s
    ON dc.date_actual BETWEEN TRY_CAST(s.spring_start_date AS DATE) 
        AND TRY_CAST(s.post_season_end_date AS DATE)