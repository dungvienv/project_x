with 
src__facebook_1020643908514628_ad_raw as (select * from {{ source('AP_PRODUCT','AIRBYTE_RAW_FACEBOOKADS_1020643908514628_ADS_INSIGHTS') }}),
src__facebook_1801919233484199_ad_raw as (select * from {{ source('AP_PRODUCT','AIRBYTE_RAW_FACEBOOKADS_1801919233484199_ADS_INSIGHTS') }}),
src__facebook_1843032105782331_ad_raw as (select * from {{ source('AP_PRODUCT','AIRBYTE_RAW_FACEBOOKADS_1843032105782331_ADS_INSIGHTS') }}),
src__facebook_360061471554632_ad_raw as (select * from {{ source('AP_PRODUCT','AIRBYTE_RAW_FACEBOOKADS_360061471554632_ADS_INSIGHTS') }}),
src__facebook_616509509961059_ad_raw as (select * from {{ source('AP_PRODUCT','AIRBYTE_RAW_FACEBOOKADS_616509509961059_ADS_INSIGHTS') }}),

final as (
SELECT
    json_value("_AIRBYTE_DATA", '$."optimization_goal"') as optimization_goal,
    json_value("_AIRBYTE_DATA", '$."updated_time"') as updated_time,
    json_value("_AIRBYTE_DATA", '$."account_currency"') as account_currency,
    json_value("_AIRBYTE_DATA", '$."reach"') as reach,
    json_value("_AIRBYTE_DATA", '$."adset_name"') as adset_name,
    json_value("_AIRBYTE_DATA", '$."social_spend"') as social_spend,
    json_value("_AIRBYTE_DATA", '$."buying_type"') as buying_type,
    json_value("_AIRBYTE_DATA", '$."frequency"') as frequency,
    json_value("_AIRBYTE_DATA", '$."objective"') as objective,
    json_value("_AIRBYTE_DATA", '$."campaign_name"') as campaign_name,
    json_value("_AIRBYTE_DATA", '$."unique_clicks"') as unique_clicks,
    json_value("_AIRBYTE_DATA", '$."account_name"') as account_name,
    json_value("_AIRBYTE_DATA", '$."spend"') as spend,
    json_value("_AIRBYTE_DATA", '$."wish_bid"') as wish_bid,
    json_value("_AIRBYTE_DATA", '$."date_stop"') as date_stop,
    json_value("_AIRBYTE_DATA", '$."campaign_id"') as campaign_id,
    json_value("_AIRBYTE_DATA", '$."created_time"') as created_time,
    json_value("_AIRBYTE_DATA", '$."quality_ranking"') as quality_ranking,
    json_value("_AIRBYTE_DATA", '$."conversion_rate_ranking"') as conversion_rate_ranking,
    json_value("_AIRBYTE_DATA", '$."ad_name"') as ad_name,
    json_value("_AIRBYTE_DATA", '$."impressions"') as impressions,
    json_value("_AIRBYTE_DATA", '$."ad_id"') as ad_id,
    json_value("_AIRBYTE_DATA", '$."date_start"') as date_start,
    json_value("_AIRBYTE_DATA", '$."account_id"') as account_id,
    json_value("_AIRBYTE_DATA", '$."adset_id"') as adset_id,
    json_value("_AIRBYTE_DATA", '$."clicks"') as clicks,
    json_value("_AIRBYTE_DATA", '$."engagement_rate_ranking"') as engagement_rate_ranking
from src__facebook_1020643908514628_ad_raw

UNION

SELECT
    json_value("_AIRBYTE_DATA", '$."optimization_goal"') as optimization_goal,
    json_value("_AIRBYTE_DATA", '$."updated_time"') as updated_time,
    json_value("_AIRBYTE_DATA", '$."account_currency"') as account_currency,
    json_value("_AIRBYTE_DATA", '$."reach"') as reach,
    json_value("_AIRBYTE_DATA", '$."adset_name"') as adset_name,
    json_value("_AIRBYTE_DATA", '$."social_spend"') as social_spend,
    json_value("_AIRBYTE_DATA", '$."buying_type"') as buying_type,
    json_value("_AIRBYTE_DATA", '$."frequency"') as frequency,
    json_value("_AIRBYTE_DATA", '$."objective"') as objective,
    json_value("_AIRBYTE_DATA", '$."campaign_name"') as campaign_name,
    json_value("_AIRBYTE_DATA", '$."unique_clicks"') as unique_clicks,
    json_value("_AIRBYTE_DATA", '$."account_name"') as account_name,
    json_value("_AIRBYTE_DATA", '$."spend"') as spend,
    json_value("_AIRBYTE_DATA", '$."wish_bid"') as wish_bid,
    json_value("_AIRBYTE_DATA", '$."date_stop"') as date_stop,
    json_value("_AIRBYTE_DATA", '$."campaign_id"') as campaign_id,
    json_value("_AIRBYTE_DATA", '$."created_time"') as created_time,
    json_value("_AIRBYTE_DATA", '$."quality_ranking"') as quality_ranking,
    json_value("_AIRBYTE_DATA", '$."conversion_rate_ranking"') as conversion_rate_ranking,
    json_value("_AIRBYTE_DATA", '$."ad_name"') as ad_name,
    json_value("_AIRBYTE_DATA", '$."impressions"') as impressions,
    json_value("_AIRBYTE_DATA", '$."ad_id"') as ad_id,
    json_value("_AIRBYTE_DATA", '$."date_start"') as date_start,
    json_value("_AIRBYTE_DATA", '$."account_id"') as account_id,
    json_value("_AIRBYTE_DATA", '$."adset_id"') as adset_id,
    json_value("_AIRBYTE_DATA", '$."clicks"') as clicks,
    json_value("_AIRBYTE_DATA", '$."engagement_rate_ranking"') as engagement_rate_ranking
from src__facebook_1801919233484199_ad_raw

UNION

SELECT
    json_value("_AIRBYTE_DATA", '$."optimization_goal"') as optimization_goal,
    json_value("_AIRBYTE_DATA", '$."updated_time"') as updated_time,
    json_value("_AIRBYTE_DATA", '$."account_currency"') as account_currency,
    json_value("_AIRBYTE_DATA", '$."reach"') as reach,
    json_value("_AIRBYTE_DATA", '$."adset_name"') as adset_name,
    json_value("_AIRBYTE_DATA", '$."social_spend"') as social_spend,
    json_value("_AIRBYTE_DATA", '$."buying_type"') as buying_type,
    json_value("_AIRBYTE_DATA", '$."frequency"') as frequency,
    json_value("_AIRBYTE_DATA", '$."objective"') as objective,
    json_value("_AIRBYTE_DATA", '$."campaign_name"') as campaign_name,
    json_value("_AIRBYTE_DATA", '$."unique_clicks"') as unique_clicks,
    json_value("_AIRBYTE_DATA", '$."account_name"') as account_name,
    json_value("_AIRBYTE_DATA", '$."spend"') as spend,
    json_value("_AIRBYTE_DATA", '$."wish_bid"') as wish_bid,
    json_value("_AIRBYTE_DATA", '$."date_stop"') as date_stop,
    json_value("_AIRBYTE_DATA", '$."campaign_id"') as campaign_id,
    json_value("_AIRBYTE_DATA", '$."created_time"') as created_time,
    json_value("_AIRBYTE_DATA", '$."quality_ranking"') as quality_ranking,
    json_value("_AIRBYTE_DATA", '$."conversion_rate_ranking"') as conversion_rate_ranking,
    json_value("_AIRBYTE_DATA", '$."ad_name"') as ad_name,
    json_value("_AIRBYTE_DATA", '$."impressions"') as impressions,
    json_value("_AIRBYTE_DATA", '$."ad_id"') as ad_id,
    json_value("_AIRBYTE_DATA", '$."date_start"') as date_start,
    json_value("_AIRBYTE_DATA", '$."account_id"') as account_id,
    json_value("_AIRBYTE_DATA", '$."adset_id"') as adset_id,
    json_value("_AIRBYTE_DATA", '$."clicks"') as clicks,
    json_value("_AIRBYTE_DATA", '$."engagement_rate_ranking"') as engagement_rate_ranking
from src__facebook_1843032105782331_ad_raw

UNION

SELECT
    json_value("_AIRBYTE_DATA", '$."optimization_goal"') as optimization_goal,
    json_value("_AIRBYTE_DATA", '$."updated_time"') as updated_time,
    json_value("_AIRBYTE_DATA", '$."account_currency"') as account_currency,
    json_value("_AIRBYTE_DATA", '$."reach"') as reach,
    json_value("_AIRBYTE_DATA", '$."adset_name"') as adset_name,
    json_value("_AIRBYTE_DATA", '$."social_spend"') as social_spend,
    json_value("_AIRBYTE_DATA", '$."buying_type"') as buying_type,
    json_value("_AIRBYTE_DATA", '$."frequency"') as frequency,
    json_value("_AIRBYTE_DATA", '$."objective"') as objective,
    json_value("_AIRBYTE_DATA", '$."campaign_name"') as campaign_name,
    json_value("_AIRBYTE_DATA", '$."unique_clicks"') as unique_clicks,
    json_value("_AIRBYTE_DATA", '$."account_name"') as account_name,
    json_value("_AIRBYTE_DATA", '$."spend"') as spend,
    json_value("_AIRBYTE_DATA", '$."wish_bid"') as wish_bid,
    json_value("_AIRBYTE_DATA", '$."date_stop"') as date_stop,
    json_value("_AIRBYTE_DATA", '$."campaign_id"') as campaign_id,
    json_value("_AIRBYTE_DATA", '$."created_time"') as created_time,
    json_value("_AIRBYTE_DATA", '$."quality_ranking"') as quality_ranking,
    json_value("_AIRBYTE_DATA", '$."conversion_rate_ranking"') as conversion_rate_ranking,
    json_value("_AIRBYTE_DATA", '$."ad_name"') as ad_name,
    json_value("_AIRBYTE_DATA", '$."impressions"') as impressions,
    json_value("_AIRBYTE_DATA", '$."ad_id"') as ad_id,
    json_value("_AIRBYTE_DATA", '$."date_start"') as date_start,
    json_value("_AIRBYTE_DATA", '$."account_id"') as account_id,
    json_value("_AIRBYTE_DATA", '$."adset_id"') as adset_id,
    json_value("_AIRBYTE_DATA", '$."clicks"') as clicks,
    json_value("_AIRBYTE_DATA", '$."engagement_rate_ranking"') as engagement_rate_ranking
from src__facebook_360061471554632_ad_raw

UNION

SELECT
    json_value("_AIRBYTE_DATA", '$."optimization_goal"') as optimization_goal,
    json_value("_AIRBYTE_DATA", '$."updated_time"') as updated_time,
    json_value("_AIRBYTE_DATA", '$."account_currency"') as account_currency,
    json_value("_AIRBYTE_DATA", '$."reach"') as reach,
    json_value("_AIRBYTE_DATA", '$."adset_name"') as adset_name,
    json_value("_AIRBYTE_DATA", '$."social_spend"') as social_spend,
    json_value("_AIRBYTE_DATA", '$."buying_type"') as buying_type,
    json_value("_AIRBYTE_DATA", '$."frequency"') as frequency,
    json_value("_AIRBYTE_DATA", '$."objective"') as objective,
    json_value("_AIRBYTE_DATA", '$."campaign_name"') as campaign_name,
    json_value("_AIRBYTE_DATA", '$."unique_clicks"') as unique_clicks,
    json_value("_AIRBYTE_DATA", '$."account_name"') as account_name,
    json_value("_AIRBYTE_DATA", '$."spend"') as spend,
    json_value("_AIRBYTE_DATA", '$."wish_bid"') as wish_bid,
    json_value("_AIRBYTE_DATA", '$."date_stop"') as date_stop,
    json_value("_AIRBYTE_DATA", '$."campaign_id"') as campaign_id,
    json_value("_AIRBYTE_DATA", '$."created_time"') as created_time,
    json_value("_AIRBYTE_DATA", '$."quality_ranking"') as quality_ranking,
    json_value("_AIRBYTE_DATA", '$."conversion_rate_ranking"') as conversion_rate_ranking,
    json_value("_AIRBYTE_DATA", '$."ad_name"') as ad_name,
    json_value("_AIRBYTE_DATA", '$."impressions"') as impressions,
    json_value("_AIRBYTE_DATA", '$."ad_id"') as ad_id,
    json_value("_AIRBYTE_DATA", '$."date_start"') as date_start,
    json_value("_AIRBYTE_DATA", '$."account_id"') as account_id,
    json_value("_AIRBYTE_DATA", '$."adset_id"') as adset_id,
    json_value("_AIRBYTE_DATA", '$."clicks"') as clicks,
    json_value("_AIRBYTE_DATA", '$."engagement_rate_ranking"') as engagement_rate_ranking
from src__facebook_616509509961059_ad_raw
)

select * from final
