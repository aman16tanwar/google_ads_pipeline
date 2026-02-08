
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cost
from `generative-ai-418805`.`wr_google_ads_silver`.`location_metrics`
where cost is null



  
  
      
    ) dbt_internal_test