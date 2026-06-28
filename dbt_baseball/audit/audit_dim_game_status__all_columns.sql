-- =============================================================
-- Audit test: dim_game_status — compare_all_columns
-- Validates that all distinct game statuses from the source
-- are represented correctly in dim_game_status.
-- =============================================================

{%- set source_query -%}
    SELECT DISTINCT
        status__coded_game_state,
        status__abstract_game_state,
        status__detailed_state,
        status__status_code,
        status__abstract_game_code
    FROM {{ source('dw', 'games') }}
    WHERE status__coded_game_state IS NOT NULL
{%- endset -%}

{%- set model_query -%}
    SELECT
        status__coded_game_state,
        status__abstract_game_state,
        status__detailed_state,
        status__status_code,
        status__abstract_game_code
    FROM {{ ref('dim_game_status') }}
{%- endset -%}

{{ audit_helper.compare_all_columns(
    a_query     = model_query,
    b_query     = source_query,
    primary_key = 'status__coded_game_state'
) }}
