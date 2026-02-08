
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date
from `generative-ai-418805`.`wr_google_ads_silver`.`age_metrics`
where date is null



  
  
      
    ) dbt_internal_test