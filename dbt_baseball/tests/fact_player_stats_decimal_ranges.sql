-- Fails if any decimal pitching/fielding stat is out of range
-- Returns rows that violate the constraints (test passes when 0 rows returned)

SELECT 'era' AS column_name, CAST(era AS FLOAT) AS value
FROM {{ ref('fact_player_stats') }}
WHERE era IS NOT NULL
  AND CAST(era AS FLOAT) < 0

UNION ALL

SELECT 'hits_per9_inn', CAST(hits_per9_inn AS FLOAT)
FROM {{ ref('fact_player_stats') }}
WHERE hits_per9_inn IS NOT NULL
  AND CAST(hits_per9_inn AS FLOAT) < 0

UNION ALL

SELECT 'whip', CAST(whip AS FLOAT)
FROM {{ ref('fact_player_stats') }}
WHERE whip IS NOT NULL
  AND CAST(whip AS FLOAT) < 0

UNION ALL

SELECT 'fielding_percentage', CAST(fielding_percentage AS FLOAT)
FROM {{ ref('fact_player_stats') }}
WHERE fielding_percentage IS NOT NULL
  AND (
      CAST(fielding_percentage AS FLOAT) < 0
      OR CAST(fielding_percentage AS FLOAT) > 1
  )