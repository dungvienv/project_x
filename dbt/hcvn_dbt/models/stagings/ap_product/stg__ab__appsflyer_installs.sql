with 
src__appsflyer_installs as (select * from {{ source('AP_PRODUCT','AIRBYTE_RAW_APPSFLYER_INSTALLS') }}),
final as (
SELECT
    JSON_VALUE("_AIRBYTE_DATA", '$."media_source"') AS media_source,
    JSON_VALUE("_AIRBYTE_DATA", '$."idfa"') AS idfa,
    JSON_VALUE("_AIRBYTE_DATA", '$."af_prt"') AS af_prt,
    JSON_VALUE("_AIRBYTE_DATA", '$."advertising_id"') AS advertising_id,
    JSON_VALUE("_AIRBYTE_DATA", '$."h"') AS h,
    JSON_VALUE("_AIRBYTE_DATA", '$."_ab_source_file_last_modified"') AS ab_source_file_last_modified,
    JSON_VALUE("_AIRBYTE_DATA", '$."platform"') AS platform,
    JSON_VALUE("_AIRBYTE_DATA", '$."_ab_source_file_url"') AS ab_source_file_url,
    JSON_VALUE("_AIRBYTE_DATA", '$."af_sub1"') AS af_sub1,
    JSON_VALUE("_AIRBYTE_DATA", '$."dt"') AS dt,
    JSON_VALUE("_AIRBYTE_DATA", '$."t"') AS t,
    JSON_VALUE("_AIRBYTE_DATA", '$."af_adset"') AS af_adset,
    REGEXP_SUBSTR(
        JSON_VALUE("_AIRBYTE_DATA", '$."original_url"'),
        'clickid=([^&]*)',
        1,
        1,
        NULL,
        1
    ) AS click_id,
    JSON_VALUE("_AIRBYTE_DATA", '$."blocked_reason"') AS blocked_reason,
    JSON_VALUE("_AIRBYTE_DATA", '$."event_name"') AS event_name,
    JSON_VALUE("_AIRBYTE_DATA", '$."campaign"') AS campaign,
    JSON_VALUE("_AIRBYTE_DATA", '$."appsflyer_id"') AS appsflyer_id,
    JSON_VALUE("_AIRBYTE_DATA", '$."af_ad"') AS af_ad,
    JSON_VALUE("_AIRBYTE_DATA", '$."customer_user_id"') AS customer_user_id,
    to_timestamp(json_value("_AIRBYTE_DATA",'$."event_time"'),'YYYY-MM-DD HH24:MI:SS') + interval '7' hour as event_time

FROM src__appsflyer_installs
)

select * from final