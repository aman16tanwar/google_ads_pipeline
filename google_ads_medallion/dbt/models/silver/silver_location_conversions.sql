{{ config(alias='location_conversions') }}

select
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    campaign_type,
    ad_group_id,
    ad_group_name,
    targeting_location,
    geo_target_city,
    geo_target_province,
    country_code,
    conversion_name,
    all_conversions,
    all_conversions_value,
    cast(date as date) as date,
    load_date

from {{ source('bronze', 'location_conversions') }}
