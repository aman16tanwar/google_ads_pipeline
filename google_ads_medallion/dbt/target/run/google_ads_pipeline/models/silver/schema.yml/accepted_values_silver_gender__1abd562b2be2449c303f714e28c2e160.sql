
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        gender as value_field,
        count(*) as n_records

    from `generative-ai-418805`.`wr_google_ads_silver`.`gender_metrics`
    group by gender

)

select *
from all_values
where value_field not in (
    'Male','Female','Unknown'
)



  
  
      
    ) dbt_internal_test