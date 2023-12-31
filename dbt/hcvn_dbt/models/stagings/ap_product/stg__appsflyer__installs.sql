with 
src__appsflyer_installs as (select * from {{ source('AP_PRODUCT','CUSTOM_RAW_APPSFLYER_INSTALLS') }}),
final as (
SELECT
    JSON_VALUE(json_data, '$."media_source"') AS media_source,
    JSON_VALUE(json_data, '$."idfa"') AS idfa,
    JSON_VALUE(json_data, '$."idfv"') AS idfv,
    JSON_VALUE(json_data, '$."af_prt"') AS af_prt,
    JSON_VALUE(json_data, '$."af_channel"') AS af_channel,
    JSON_VALUE(json_data, '$."advertising_id"') AS advertising_id,
    JSON_VALUE(json_data, '$."platform"') AS platform,
    JSON_VALUE(json_data, '$."app_id"') AS app_id,
    JSON_VALUE(json_data, '$."af_sub1"') AS af_sub1,
    JSON_VALUE(json_data, '$."af_adset"') AS af_adset,
    JSON_VALUE(json_data, '$."af_ad_type"') AS af_ad_type,
    JSON_VALUE(json_data, '$."is_retargeting"') AS is_retargeting,
    JSON_VALUE(json_data, '$."retargeting_conversion_type"') AS retargeting_conversion_type,
    JSON_VALUE(json_data, '$."is_primary_attribution"') AS is_primary_attribution,
    JSON_VALUE(json_data, '$."af_attribution_lookback"') AS af_attribution_lookback,
    JSON_VALUE(json_data, '$."af_reengagement_window"') AS af_reengagement_window,
    JSON_VALUE(json_data, '$."blocked_reason_value"') AS blocked_reason_value,
    JSON_VALUE(json_data, '$."blocked_reason_rule"') AS blocked_reason_rule,
    JSON_VALUE(json_data, '$."blocked_sub_reason"') AS blocked_sub_reason,
    JSON_VALUE(json_data, '$."media_type"') AS media_type,
    JSON_VALUE(json_data, '$."utm_source"') AS utm_source,
    JSON_VALUE(json_data, '$."utm_medium"') AS utm_medium,
    JSON_VALUE(json_data, '$."utm_term"') AS utm_term,
    JSON_VALUE(json_data, '$."utm_content"') AS utm_content,
    JSON_VALUE(json_data, '$."utm_campaign"') AS utm_campaign,
    JSON_VALUE(json_data, '$."media_channel"') AS media_channel,
    JSON_VALUE(json_data, '$."utm_id"') AS utm_id,
    JSON_VALUE(json_data, '$."conversion_type"') AS conversion_type,
    JSON_VALUE(json_data, '$."campaign_type"') AS campaign_type,
    JSON_VALUE(json_data, '$."validation_reason_value"') AS validation_reason_value,
    JSON_VALUE(json_data, '$."rejected_reason"') AS rejected_reason,
    JSON_VALUE(json_data, '$."fraud_reason"') AS fraud_reason,
    JSON_VALUE(json_data, '$."fraud_sub_reason"') AS fraud_sub_reason,
    JSON_VALUE(json_data, '$."is_organic"') AS is_organic,
    REGEXP_SUBSTR(
        JSON_VALUE(json_data, '$."original_url"'),
        'clickid=([^&]*)',
        1,
        1,
        NULL,
        1
    ) AS click_id,
    JSON_VALUE(json_data, '$."blocked_reason"') AS blocked_reason,
    JSON_VALUE(json_data, '$."event_name"') AS event_name,
    JSON_VALUE(json_data, '$."campaign"') AS campaign,
    JSON_VALUE(json_data, '$."appsflyer_id"') AS appsflyer_id,
    JSON_VALUE(json_data, '$."af_ad"') AS af_ad,
    JSON_VALUE(json_data, '$."customer_user_id"') AS customer_user_id,
    to_timestamp(json_value(json_data,'$."event_time"'),'YYYY-MM-DD HH24:MI:SS') + interval '7' hour as event_time,
    to_timestamp(json_value(json_data,'$."install_time"'),'YYYY-MM-DD HH24:MI:SS') + interval '7' hour as install_time

FROM src__appsflyer_installs
)

select distinct * from final