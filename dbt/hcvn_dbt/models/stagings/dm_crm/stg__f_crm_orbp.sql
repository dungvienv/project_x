with
final as (
    select
       PP.SKF_APPROVAL_PROCESS_HEAD,
       PP.NUM_GROUP_POSITION_1,
       PP.DTIME_APL_BOD0 APL_BOD0_DTM,
       PP.ID_CUID,
       PP.ID_CA CA_ID,
       PP.TEXT_CA_LIMIT_SUB_TYPE CA_LIMIT_SUB_TYPE,
       PP.ID_CA_OFF CA_OFF_ID,
       PP.DATE_APL_BOD0_VALID_FROM APL_BOD0_VALID_FROM_DT,
       PP.DATE_APL_BOD0_VALID_TO APL_BOD0_VALID_TO_DT,
       PP.NAME_APL_BOD0_OFFER APL_BOD0_OFFER_NAME,
       PP.AMT_APL_BOD0_LIMIT_MAX APL_BOD0_MAX_LIMIT,
       PP.AMT_APL_BOD0_ANNUITY_MAX APL_BOD0_MAX_ANNUITY,
       PP.TEXT_APL_BOD0_RBP APL_BOD0_RBP,
       PP.NUM_APL_F_BOD0_0MOB APL_F_BOD0_0MOB,
       PP.TEXT_APL_BOD0_TRIGGER_TYPE APL_BOD0_TRIGGER_TYPE,
       PP.TEXT_APL_F_PRI_CAT_SHIFT APL_F_PRI_CAT_SHIFT,
       PP.NUM_APL_F_BOD0_FANTOMAS APL_F_BOD0_FANTOMAS,
       PP.TEXT_APL_BOD0_CHANNEL APL_BOD0_CHANNEL,
       PP.TEXT_APL_BOD0_HC_RES APL_BOD0_HC_RES,
       PP.NUM_APL_F_BOD0_LIMIT_ELI APL_F_BOD0_LIMIT_ELI,
       PP.NUM_APL_F_BOD0_OTP APL_F_BOD0_OTP,
       PP.NUM_APL_F_BOD0_APPROVED APL_F_BOD0_APPROVED,
       PP.TEXT_APL_BOD0_INST_PROD APL_BOD0_INST_PROD,
       PP.NUM_APL_F_BOD0_FLAT_SUPER_LOW APL_F_BOD0_FLAT_SUPER_LOW,
       PP.TEXT_APL_BOD0_OFFER_TYPE APL_BOD0_OFFER_TYPE,
       PP.TEXT_APL_BOD0_TRIGGER_SOURCE APL_BOD0_TRIGGER_SOURCE,
       PP.TEXT_APL_BOD0_ACTOR APL_BOD0_ACTOR,
       PP.SKP_SCORING_APPLICATION,
       PP.TEXT_APL_BOD0_LIMIT_TYPE APL_BOD0_LIMIT_TYPE,
       PP.ID_LIMIT_BULK,
       PP.TEXT_APL_CONTRACT_NUM APL_CONTRACT_NUM,
       PP.NUM_LIMIT_SCORE_OFFLINE LIMIT_SCORE_OFFLINE,
       PP.NAME_APL_BOD0_STRATEGY APL_BOD0_STRATEGY_NAME,
       PP.NAME_POST_FINAL_RISKGROUP POST_FINAL_RISKGROUP,
       PP.DATE_APL_LAST_ACTIVE APL_LAST_ACTIVE_DT,
       PP.NAME_PRE_FINAL_RISKGROUP PRE_FINAL_RISKGROUP,
       PP.TEXT_REGISTER_FINAL_RISKGROUP REGISTER_FINAL_RISKGROUP,
       PP.CODE_APL_BOD0_SA APL_BOD0_SA_CODE,
       PP.ID_APL_BOD0_PROSPECT APL_BOD0_PROSPECT_ID,
       PP.SKP_CLIENT,
       PP.TEXT_CLI_CONTACT_REGION CLI_CONTACT_REGION,
       PP.TEXT_OUTPUT_LIMIT_SUB_TYPE OUTPUT_LIMIT_SUB_TYPE,
       PP.TEXT_OUTPUT_LIMIT_TYPE OUTPUT_LIMIT_TYPE,
       PP.CODE_OUTPUT_LIMIT_PILOT OUTPUT_LIMIT_PILOT_CODE,
       PP.NUM_APL_F_BOD0_BASE APL_F_BOD0_BASE,
       PP.TEXT_TRIGGER_PRODUCT_PROFILE TRIGGER_PRODUCT_PROFILE,
       PP.TEXT_ORI_LIMIT_TYPE ORI_LIMIT_TYPE,
       PP.CODE_ORI_LIMIT_PILOT ORI_LIMIT_PILOT_CODE,
       PP.RATE_OUTPUT_MIN_DEPOSIT_LIMIT OUTPUT_MIN_DEPOSIT_LIMIT_RATE,
       PP.TEXT_OUTPUT_SEGMENT OUTPUT_SEGMENT,
       PP.DTIME_APL_BOD0_END APL_BOD0_END_DTM,
       PP.TEXT_ORI_LIMIT_STATUS TEXT_ORI_LIMIT_STATUS,
       PP.NFLAG_APL_F_ACTIVE_BNPL NFLAG_APL_F_ACTIVE_BNPL
  from {{ source('DM_CRM','FT_CRM_ORBP') }} PP 
  where dtime_updated >= sysdate-90
)

select * from final
