 {{ config(alias='main') }}
select
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    campaign_type,
    device,
    cast(date as date) as date,
    impressions,
    clicks,
    cost_micros / 1000000.0 as cost,
    all_conversions,
    all_conversions_value,
    conversion_name

from {{ source('bronze', 'main') }}