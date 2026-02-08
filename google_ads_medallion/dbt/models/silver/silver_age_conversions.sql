{{ config(alias='age_conversions') }}
select 

    account_id,
    account_name,
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,
    age_range,
    conversion_name,
    all_conversions,
    all_conversions_value,
    CAST(date as date) as date,
    load_date
from {{ source('bronze', 'age_conversions') }}