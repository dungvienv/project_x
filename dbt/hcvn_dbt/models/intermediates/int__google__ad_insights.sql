with 
src__google_ad as (select * from {{ ref('stg__ab__google_ad_insights') }}),
final as (
    select *
    from src__google_ad
)
select * from final