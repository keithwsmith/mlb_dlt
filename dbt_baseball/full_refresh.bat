dbt run --select fact_pitches --full-refresh
dbt run --select fact_at_bats --full-refresh
dbt run --select fact_batted_balls --full-refresh
dbt run --select mart_batter_game --full-refresh
dbt run --select mart_matchups --full-refresh
dbt run --select mart_pitcher_arsenal --full-refresh
dbt run --select mart_pitcher_game --full-refresh