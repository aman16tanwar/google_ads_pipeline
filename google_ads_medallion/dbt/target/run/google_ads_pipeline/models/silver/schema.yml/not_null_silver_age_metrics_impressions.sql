
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select impressions
from `generative-ai-418805`.`wr_google_ads_silver`.`age_metrics`
where impressions is null



  
  
      
    ) dbt_internal_test