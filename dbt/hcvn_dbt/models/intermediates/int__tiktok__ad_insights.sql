with 
src__tiktok_ad as (select * from {{ ref('stg__ab__tiktok_ad_insights') }}),
final as (
    select a.*,
    {{ dbt_utils.generate_surrogate_key(['a.ad_id','a.stat_time_day']) }} as dbt_key_id
    from src__tiktok_ad a
)
select * from final