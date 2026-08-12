{{
    config(
        materialized = 'table',
    )
}}

/*
    fact_draft
    ----------
    One row per draft pick event.
    Contains ONLY meaningfully aggregatable measures at the pick grain.

    Additive:      pick_value, signing_bonus
    Semi-additive: pick_number, round_pick_number, rank, current_age_at_draft

    Everything else (player names, team, school, position, flags, text)
    lives in dim_draft and is accessed via draft_key.

    Source: dw.draft  →  dim_draft (surrogate key)

    FIX: dim_draft.sql removed its duplicate person__draft_year column
    (kept only person_draft_year, the canonical one) -- the join below
    now targets d.person_draft_year instead. Since d.person_draft_year is
    CLEANED (coalesce/try_cast/trim) while s.person__draft_year here is
    still the RAW source value, the join's left side now applies the
    identical cleaning expression rather than a naive rename -- comparing
    raw against cleaned directly would silently mismatch on whitespace,
    leading zeros, or non-numeric values that the cleaning expression
    normalizes away.

    NOT changed: line ~62 below still outputs this model's OWN
    person__draft_year column from the raw, uncleaned source value under
    the double-underscore name -- the same naming/cleanliness
    inconsistency that was just fixed in dim_draft. Left as-is since that
    wasn't part of what broke; worth deciding separately whether to
    rename/clean it here too for consistency.
*/

with source as (

    select * from {{ source('dw', 'draft') }}

),
deduped as (

    select *
    from (

        select
            *,
            row_number() over (
                partition by
                    person__id,
                    person__draft_year,
                    pick_number,
                    round_pick_number
                order by _dlt_load_id desc
            ) as rn
        from source

    ) x
    where rn = 1

),


dim as (

    select * from {{ ref('dim_draft') }}

),

fact_draft as (

    select

        -- ── Foreign key ─────────────────────────────────────────────────
        d.draft_key,
		s.person__draft_year											as person__draft_year,
		d.person_id														as person_id,

        -- ── Pick position (semi-additive: avg / min / max) ──────────────
        cast(s.pick_number as int)                          as pick_number,
        cast(s.round_pick_number as int)                    as round_pick_number,

        -- ── Rank (semi-additive: avg / min / max) ───────────────────────
        cast(s.rank as int)                                 as rank,

        -- ── Financials (fully additive: sum / avg) ──────────────────────
        try_cast(
            replace(replace(s.pick_value, '$', ''), ',', '')
            as decimal(18,2)
        )                                                   as pick_value,

        try_cast(
            replace(replace(s.signing_bonus, '$', ''), ',', '')
            as decimal(18,2)
        )                                                   as signing_bonus,

        -- ── Age (semi-additive: avg / distribution) ─────────────────────
        cast(s.person__current_age as int)                  as current_age

     from deduped s

    inner join dim d
        on s.person__id         = d.person_id
       and coalesce(nullif(ltrim(rtrim(try_cast(s.person__draft_year as int))), ''), '0') = d.person_draft_year
       and s.pick_number        = d.pick_number
       and s.round_pick_number  = d.round_pick_number

)

select * from fact_draft