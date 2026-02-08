
    
    

with all_values as (

    select
        age_range as value_field,
        count(*) as n_records

    from `generative-ai-418805`.`wr_google_ads_silver`.`age_metrics`
    group by age_range

)

select *
from all_values
where value_field not in (
    '18-24','25-34','35-44','45-54','55-64','65+','Unknown'
)


