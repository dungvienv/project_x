with src__crm_orbp as (
   select *
   from {{ ref('int__crm__orbp') }}
),
src__crm_appl as (
   select *
   from {{ ref('int__crm__appl') }}
),
final as (
   select r.SKF_APPROVAL_PROCESS_HEAD,
      r.apl_bod0_trigger_source,
      r.APL_BOD0_DTM,
      max(a.SKP_CREDIT_CASE) as TRIGGER_SKP_CREDIT_CASE,
      max(
         case
            when a.apl_bod01_channel = 'CLW_ONLINE' then 'Web'
            when a.apl_bod01_channel = 'MOBILE_APP' then 'Mobile App'
            when a.apl_bod01_channel = 'OTHER' then 'Others'
            when a.apl_bod01_channel in ('TLS_ACL', 'TLS_BT', 'TLS') then 'TLS'
            when a.APL_PROD_CODE_GR = 'BNPL' then 'BNPL'
            else a.apl_bod01_channel
         end
      ) as TRIGGER_BOD1_CHANNEL,
      max(a.SKP_CREDIT_TYPE) as TRIGGER_SKP_CREDIT_TYPE,
      max(a.APL_PROD_CODE_GR) as TRIGGER_PROD_CODE_GR,
      max(trunc(a.APL_BOD01_BOD02_DTM)) as TRIGGER_APPLICATION_DATE,
      max(
         CASE
            WHEN a.SKP_CREDIT_TYPE = 1 THEN 'POS'
            WHEN a.SKP_CREDIT_TYPE = 2 THEN 'CASH'
            WHEN a.SKP_CREDIT_TYPE = 3
            AND a.APL_PROD_CODE_GR = 'BNPL' THEN 'BNPL'
            WHEN a.SKP_CREDIT_TYPE = 3 THEN 'CARD'
            WHEN r.apl_bod0_trigger_source LIKE 'AM%' THEN 'AM'
            WHEN r.apl_bod0_trigger_source = 'CLIP' THEN 'CLIP'
            ELSE NULL
         END
      ) AS TRIGGER_PRODUCT
   from src__crm_orbp r
      left join src__crm_appl a on r.APL_CONTRACT_NUM = a.APL_CONTRACT_NUM
   where APL_BOD0_CHANNEL = 'Icash'
   group by r.SKF_APPROVAL_PROCESS_HEAD,
      apl_bod0_trigger_source,
      r.APL_BOD0_DTM
)
select *
from final