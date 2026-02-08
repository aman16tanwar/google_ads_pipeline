{{ config(alias='main_conversions') }}

select
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    campaign_type,
    device,
    conversion_name,
    all_conversions,
    all_conversions_value,
    currency_code,
    cast(date as date) as date,
    load_date

from {{ source('bronze', 'main_conversions') }}
