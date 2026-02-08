
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select account_id
from `generative-ai-418805`.`wr_google_ads_silver`.`main_metrics`
where account_id is null



  
  
      
    ) dbt_internal_test