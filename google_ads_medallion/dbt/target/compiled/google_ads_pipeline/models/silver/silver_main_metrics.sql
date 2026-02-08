

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

from `generative-ai-418805`.`wr_google_ads_bronze`.`main_metrics`