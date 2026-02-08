{{ config(alias='gender_metrics') }}

select
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    campaign_type,
    ad_group_id,
    ad_group_name,
    gender,
    impressions,
    video_views,
    clicks,
    cost_micros / 1000000.0 as cost,
    all_conversions,
    all_conversions_value,
    cast(date as date) as date,
    load_date
from {{ source('bronze', 'gender_metrics') }}