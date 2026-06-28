-- Consecutive versions for the same player should have no gaps.
-- end_date of version N should be exactly 1 day before
-- effective_date of version N+1.

with versions as (
    select
        player_id,
        version_number,
        effective_date,
        end_date,
        lead(effective_date) over (
            partition by player_id
            order by version_number
        ) as next_effective_date
    from {{ ref('dim_players') }}
)

select *
from versions
where next_effective_date is not null
  and dateadd(day, 1, end_date) != next_effective_date
