
  
    

    create or replace table `generative-ai-418805`.`wr_google_ads_silver`.`age`
      
    
    

    
    OPTIONS()
    as (
      
select resource_name, campaign_id, 
campaign_name, campaign_type, ad_group_id, ad_group_name, age_range, impressions, clicks, cost_micros / 1000000.0 as cost, all_conversions, all_conversions_value, 
CAST(date as date) as date,conversion_name,account_id,account_name

from `generative-ai-418805`.`wr_google_ads_bronze`.`age`
    );
  