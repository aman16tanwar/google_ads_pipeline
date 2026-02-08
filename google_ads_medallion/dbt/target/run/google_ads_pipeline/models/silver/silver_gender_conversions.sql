
  
    

    create or replace table `generative-ai-418805`.`wr_google_ads_silver`.`gender_conversions`
      
    
    

    
    OPTIONS()
    as (
      

select
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    campaign_type,
    ad_group_id,
    ad_group_name,
    gender,
    conversion_name,
    all_conversions,
    all_conversions_value,
    cast(date as date) as date,
    load_date
from `generative-ai-418805`.`wr_google_ads_bronze`.`gender_conversions`
    );
  