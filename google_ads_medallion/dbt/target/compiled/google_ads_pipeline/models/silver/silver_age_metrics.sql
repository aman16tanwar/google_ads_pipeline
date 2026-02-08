
select 
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    campaign_type,
    ad_group_id,
    ad_group_name,
    age_range,
    impressions,
    video_views,
    clicks,
    cost_micros / 1000000.0 as cost,
    all_conversions,
    all_conversions_value,
    cast(date as date) as date,
    load_date
    
from `generative-ai-418805`.`wr_google_ads_bronze`.`age_metrics`