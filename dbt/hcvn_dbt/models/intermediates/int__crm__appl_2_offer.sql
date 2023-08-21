with src__appl_2_offer as (
      SELECT *
      FROM {{ref('stg__f_crm_appl_2_offer')}}
),
final as (
      select ID_CA as CA_ID,
            ID_CA_OFF as CA_OFF_ID,
            ID_CA_ORIG_OFFER as CA_ORIG_OFFER_ID,
            DATE_CA_START as CA_START_DT,
            ID_CUID as ID_CUID,
            SKP_CLIENT as SKP_CLIENT,
            SKP_CREDIT_CASE as SKP_CREDIT_CASE,
            DATE_DECISION as DATE_DECISION,
            NUM_RK_ORIG_OFFER_BY_APL as RK_ORIG_OFFER_BY_APL,
            NUM_RK_APL_BY_CA_START_DT as RK_APL_BY_CA_START_DT,
            DECODE(
                  SKF_APPROVAL_PROCESS_HEAD,
                  '-1',
                  null,
                  SKF_APPROVAL_PROCESS_HEAD
            ) as SKF_APPROVAL_PROCESS_HEAD,
            DECODE(
                  DTIME_APL_BOD0,
                  date '3000-01-01',
                  cast(null as date),
                  DTIME_APL_BOD0
            ) as APL_BOD0_DTM,
            DECODE(SKP_OFFER_TYPE, '-1', null, SKP_OFFER_TYPE) as SKP_OFFER_TYPE
      from src__appl_2_offer
)
select *
from final