with 
src__google_campaigns as (select * from {{ ref('stg__ab__google_campaigns') }}),
final as (
    select *
    from src__google_campaigns
)
select * from final