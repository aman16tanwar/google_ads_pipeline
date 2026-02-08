
    
    

with all_values as (

    select
        device as value_field,
        count(*) as n_records

    from `generative-ai-418805`.`wr_google_ads_silver`.`main_metrics`
    group by device

)

select *
from all_values
where value_field not in (
    'DESKTOP','MOBILE','TABLET','CONNECTED_TV','OTHER'
)


