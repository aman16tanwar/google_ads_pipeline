{{ config(alias='main_metrics') }}

select
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    campaign_type,
    device,
    impressions,
    video_views,
    clicks,
    cost_micros / 1000000.0 as cost,
    all_conversions,
    all_conversions_value,
    currency_code,
    cast(date as date) as date,
    load_date

from {{ source('bronze', 'main_metrics') }}
