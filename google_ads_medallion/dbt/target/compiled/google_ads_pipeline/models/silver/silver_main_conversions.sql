

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

from `generative-ai-418805`.`wr_google_ads_bronze`.`main_conversions`