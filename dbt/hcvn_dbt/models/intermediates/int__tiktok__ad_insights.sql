with 
src__tiktok_ad as (select * from {{ ref('stg__ab__tiktok_ad_insights') }}),
final as (
    select *
    from src__tiktok_ad
)
select * from final