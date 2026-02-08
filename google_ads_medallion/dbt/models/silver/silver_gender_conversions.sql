{{ config(alias='gender_conversions') }}

select
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    campaign_type,
    ad_group_id,
    ad_group_name,
    gender,
    conversion_name,
    all_conversions,
    all_conversions_value,
    cast(date as date) as date,
    load_date
from {{ source('bronze', 'gender_conversions') }}