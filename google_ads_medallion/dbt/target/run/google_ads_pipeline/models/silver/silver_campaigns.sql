
  
    

    create or replace table `generative-ai-418805`.`wr_google_ads_silver`.`main`
      
    
    

    
    OPTIONS()
    as (
      
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

from `generative-ai-418805`.`wr_google_ads_bronze`.`main`
    );
  