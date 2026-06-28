-- Every player must have exactly one row where is_current = 1.
-- Failing rows indicate broken SCD2 logic.

select
    player_id,
    count(*) as current_count
from {{ ref('dim_players') }}
where is_current = 1
group by player_id
having count(*) != 1
