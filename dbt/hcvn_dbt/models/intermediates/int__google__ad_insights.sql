with 
src__google_ad as (select * from {{ ref('stg__ab__google_ad_insights') }}),
final as (
    select a.*,
    {{ dbt_utils.generate_surrogate_key(['a.ad_group_ad_id','a.segments_date']) }} as dbt_key_id
    from src__google_ad a
)
select * from final