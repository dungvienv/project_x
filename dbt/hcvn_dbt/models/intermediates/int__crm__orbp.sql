with src__crm__orbp as (
    select *
    from {{ ref('stg__f_crm_orbp') }}
),
final as (
    select --+materialized
    SKF_APPROVAL_PROCESS_HEAD
    ,NUM_GROUP_POSITION_1
    ,APL_BOD0_DTM
    ,decode(ID_CUID, 'XNA', NULL, ID_CUID) as ID_CUID
    ,case
    when CA_ID = 'XNA' then
        NULL
    else
        CA_ID
    end as CA_ID
    ,decode(CA_LIMIT_SUB_TYPE, 'XNA', NULL, CA_LIMIT_SUB_TYPE) as CA_LIMIT_SUB_TYPE
    ,decode(CA_OFF_ID, 'XNA', NULL, CA_OFF_ID) as CA_OFF_ID
    ,case
    when APL_BOD0_VALID_FROM_DT = date '1000-01-01' then
        cast(NULL as date)
    else
        APL_BOD0_VALID_FROM_DT
    end as APL_BOD0_VALID_FROM_DT
    ,case
    when APL_BOD0_VALID_TO_DT = date '1000-01-01' then
        cast(NULL as date)
    else
        APL_BOD0_VALID_TO_DT
    end as APL_BOD0_VALID_TO_DT
    ,decode(APL_BOD0_OFFER_NAME, 'XNA', NULL, APL_BOD0_OFFER_NAME) as APL_BOD0_OFFER_NAME
    ,APL_BOD0_MAX_LIMIT
    ,APL_BOD0_MAX_ANNUITY
    ,decode(APL_BOD0_RBP, 'XNA', NULL, APL_BOD0_RBP) as APL_BOD0_RBP
    ,APL_F_BOD0_0MOB
    ,case
    when apl_bod0_trigger_source = 'LP_CC' then
        'WEB'
    else
        APL_BOD0_TRIGGER_TYPE
    end as APL_BOD0_TRIGGER_TYPE
    ,APL_F_PRI_CAT_SHIFT
    ,APL_F_BOD0_FANTOMAS
    ,APL_BOD0_CHANNEL
    ,APL_BOD0_HC_RES
    ,APL_F_BOD0_LIMIT_ELI
    ,APL_F_BOD0_OTP
    ,APL_F_BOD0_APPROVED
    ,decode(APL_BOD0_INST_PROD, 'XNA', NULL, APL_BOD0_INST_PROD) as APL_BOD0_INST_PROD
    ,APL_F_BOD0_FLAT_SUPER_LOW
    ,case
    when output_limit_type = 'CCW'
            and apl_bod0_dtm >= date '2023-04-21'
            and r.apl_bod0_offer_type = 'CCX' then
        'CC_SC'
    else
        APL_BOD0_OFFER_TYPE
    end as APL_BOD0_OFFER_TYPE
    ,decode(APL_BOD0_TRIGGER_SOURCE, 'XNA', NULL, APL_BOD0_TRIGGER_SOURCE) as APL_BOD0_TRIGGER_SOURCE
    ,decode(APL_BOD0_Actor, 'XNA', NULL, APL_BOD0_Actor) as APL_BOD0_Actor
    ,SKP_SCORING_APPLICATION
    ,decode(APL_BOD0_LIMIT_TYPE, 'XNA', NULL, APL_BOD0_LIMIT_TYPE) as APL_BOD0_LIMIT_TYPE
    ,decode(ID_LIMIT_BULK, 'XNA', NULL, ID_LIMIT_BULK) as ID_LIMIT_BULK
    ,decode(APL_CONTRACT_NUM, 'XNA', NULL, APL_CONTRACT_NUM) as APL_CONTRACT_NUM
    ,LIMIT_SCORE_OFFLINE
    ,decode(APL_BOD0_STRATEGY_NAME, 'XNA', NULL, APL_BOD0_STRATEGY_NAME) as APL_BOD0_STRATEGY_NAME
    ,decode(POST_FINAL_RISKGROUP, 'XNA', NULL, POST_FINAL_RISKGROUP) as POST_FINAL_RISKGROUP
    ,case
    when APL_LAST_ACTIVE_DT = date '1000-01-01' then
        cast(NULL as date)
    else
        APL_LAST_ACTIVE_DT
    end as APL_LAST_ACTIVE_DT
    ,decode(PRE_FINAL_RISKGROUP, 'XNA', NULL, PRE_FINAL_RISKGROUP) as PRE_FINAL_RISKGROUP
    ,decode(REGISTER_FINAL_RISKGROUP, 'XNA', NULL, REGISTER_FINAL_RISKGROUP) as REGISTER_FINAL_RISKGROUP
    ,decode(APL_BOD0_SA_CODE, 'XNA', NULL, APL_BOD0_SA_CODE) as APL_BOD0_SA_CODE
    ,decode(APL_BOD0_PROSPECT_ID, 'XNA', NULL, APL_BOD0_PROSPECT_ID) as APL_BOD0_PROSPECT_ID
    ,SKP_CLIENT
    ,decode(CLI_CONTACT_REGION, 'XNA', NULL, CLI_CONTACT_REGION) as CLI_CONTACT_REGION
    ,decode(OUTPUT_LIMIT_SUB_TYPE, 'XNA', NULL, OUTPUT_LIMIT_SUB_TYPE) as OUTPUT_LIMIT_SUB_TYPE
    ,decode(OUTPUT_LIMIT_TYPE, 'XNA', NULL, OUTPUT_LIMIT_TYPE) as OUTPUT_LIMIT_TYPE
    ,decode(OUTPUT_LIMIT_PILOT_CODE, 'XNA', NULL, OUTPUT_LIMIT_PILOT_CODE) as OUTPUT_LIMIT_PILOT_CODE
    ,case
    when APL_F_BOD0_BASE = 0
            and apl_bod0_trigger_source = 'HOMEX_TW'
            and output_limit_type = 'CD' then
        1
    when apl_bod0_dtm >= date '2023-04-21'
            and output_limit_type in ('CCW', 'CCX', 'BNPL') then
        1
    --   when APL_BOD0_OFFER_NAME = 'CCW_UNI_EMP' then
    --    1
    when r.APL_BOD0_MAX_LIMIT > 0 then
        1
    else
        APL_F_BOD0_BASE
    end as APL_F_BOD0_BASE
    ,decode(TRIGGER_PRODUCT_PROFILE, 'XNA', NULL, TRIGGER_PRODUCT_PROFILE) as TRIGGER_PRODUCT_PROFILE
    ,decode(ORI_LIMIT_TYPE, 'XNA', NULL, ORI_LIMIT_TYPE) ORI_LIMIT_TYPE
    ,decode(ORI_LIMIT_PILOT_CODE, 'XNA', NULL, ORI_LIMIT_PILOT_CODE) ORI_LIMIT_PILOT_CODE
    ,OUTPUT_MIN_DEPOSIT_LIMIT_RATE
    ,decode(OUTPUT_SEGMENT, 'XNA', NULL, OUTPUT_SEGMENT) OUTPUT_SEGMENT
    ,case
    when APL_BOD0_END_DTM = date '1000-01-01' then
        cast(NULL as date)
    else
        APL_BOD0_END_DTM
    end as APL_BOD0_END_DTM
    ,case
    when TEXT_ORI_LIMIT_STATUS <> 'XNA' then
        TEXT_ORI_LIMIT_STATUS
    end as ORI_LIMIT_STATUS
    ,case
    when NFLAG_APL_F_ACTIVE_BNPL <> -1 then
        NFLAG_APL_F_ACTIVE_BNPL
    end as FLAG_APL_F_ACTIVE_BNPL
    from   src__crm__orbp r
    where  (case
            when APL_BOD0_OFFER_NAME like '%BI_IMPORT%'
                and apl_bod0_dtm >= date '2023-04-21' then
            0
            else
            1
        end) = 1 --exclude import cases from 21.04.2023
)
select *
from final