{#
    dim_award.sql is:
        select distinct award_id, award_name
        from dw.award_recipients
        where award_id is not null
    ...plus a generated surrogate key. Reusing log_query_audit (the
    compare_queries-based helper) rather than compare_all_columns, since
    dw.award_recipients almost certainly has more columns than dim_award
    does (it also feeds dim_award_recipient) — compare_all_columns requires
    identical column sets on both sides and would error trying to select
    columns dim_award doesn't have. Selecting exactly the columns we care
    about, like this, sidesteps that entirely.
#}
{% macro audit_dim_award_columns() %}

{% set source_query %}
select
    award_id,
    award_name
from dw.award_recipients
where award_id is not null
{% endset %}

{% set dim_query %}
select
    award_id,
    award_name
from {{ ref('dim_award') }}
{% endset %}

{{ log_query_audit(
    model_name='dim_award',
    baseline_description='vs. dw.award_recipients (award_id + award_name)',
    a_query=source_query,
    b_query=dim_query,
    columns_compared=2
) }}

{% endmacro %}
