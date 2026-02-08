
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
from `generative-ai-418805`.`wr_google_ads_bronze`.`age_conversions`