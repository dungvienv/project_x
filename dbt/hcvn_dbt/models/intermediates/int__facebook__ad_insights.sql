with 
src__facebook_ad as (select * from {{ ref('stg__ab__facebook_ad_insights') }}),
final as (
    select a.*,
    {{ dbt_utils.generate_surrogate_key(['a.ad_id','a.date_start']) }} as dbt_key_id
    from src__facebook_ad a
)
select * from final