dbt run --select int_team_game_results --full-refresh
dbt run --select int_team_game_results_to_date --full-refresh
dbt run --select dim_team_standings --full-refresh
dbt run --select dim_team_standings_by_date --full-refresh
dbt run --select int_player_game_stats --full-refresh
dbt run --select dim_player --full-refresh
dbt run --select dim_player_stats --full-refresh
dbt run --select dim_player_stats_by_date --full-refresh

dbt test --select int_team_game_results 
dbt test --select int_team_game_results_to_date 
dbt test --select dim_team_standings 
dbt test --select dim_team_standings_by_date 
dbt test --select int_player_game_stats 
dbt test --select dim_player 
dbt test --select dim_player_stats 
dbt test --select dim_player_stats_by_date 

