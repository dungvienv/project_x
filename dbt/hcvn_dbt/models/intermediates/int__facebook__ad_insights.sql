with 
src__facebook_ad as (select * from {{ ref('stg__ab__facebook_ad_insights') }}),
final as (
    select *
    from src__facebook_ad
)
select * from final