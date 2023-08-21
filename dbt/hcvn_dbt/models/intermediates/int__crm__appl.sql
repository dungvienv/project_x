with src__crm__appl as (
    select
        *
    from
        {{ ref('stg__f_crm_appl') }}
),
final as (
    SELECT
        DATE_DECISION as DATE_DECISION,
        SKP_CREDIT_CASE as SKP_CREDIT_CASE,
        SKP_APPLICATION as SKP_APPLICATION,
        SKP_CONTRACT as SKP_CONTRACT,
        SKP_CREDIT_TYPE as SKP_CREDIT_TYPE,
        SKP_CLIENT as SKP_CLIENT,
        ID_CUID as ID_CUID,
        SKP_PRODUCT as SKP_PRODUCT,
        SKP_PRODUCT_VARIANT as SKP_PRODUCT_VARIANT,
        SKP_SALESROOM_APPL_CREATED as SKP_SALESROOM_APPL_CREATED,
        SKP_SALESROOM as SKP_SALESROOM,
        SKP_CREDIT_STATUS as SKP_CREDIT_STATUS,
        NUM_APL_CONTRACT as APL_CONTRACT_NUM,
        DECODE(
            DTIME_APL_BOD01,
            date '3000-01-01',
            CAST(NULL AS DATE),
            DTIME_APL_BOD01
        ) as APL_BOD01_DTM,
        DECODE(
            DTIME_APL_BOD02,
            date '3000-01-01',
            CAST(NULL AS DATE),
            DTIME_APL_BOD02
        ) as APL_BOD02_DTM,
        DECODE(
            DTIME_APL_BOD01_BOD02,
            date '3000-01-01',
            CAST(NULL AS DATE),
            DTIME_APL_BOD01_BOD02
        ) as APL_BOD01_BOD02_DTM,
        DECODE(
            DTIME_APL_APPROVE,
            date '3000-01-01',
            CAST(NULL AS DATE),
            DTIME_APL_APPROVE
        ) as APL_APPROVE_DTM,
        DECODE(
            DTIME_APL_REJECT,
            date '3000-01-01',
            CAST(NULL AS DATE),
            DTIME_APL_REJECT
        ) as APL_REJECT_DTM,
        DECODE(
            DTIME_APL_SIGN,
            date '3000-01-01',
            CAST(NULL AS DATE),
            DTIME_APL_SIGN
        ) as APL_SIGN_DTM,
        DECODE(
            DATE_APL_PAYOUT,
            date '3000-01-01',
            CAST(NULL AS DATE),
            DATE_APL_PAYOUT
        ) as APL_PAYOUT_DT,
        DECODE(
            DTIME_APL_ACTIVE,
            date '3000-01-01',
            CAST(NULL AS DATE),
            DTIME_APL_ACTIVE
        ) as APL_ACTIVE_DTM,
        DECODE(
            DTIME_APL_CANCEL,
            date '3000-01-01',
            CAST(NULL AS DATE),
            DTIME_APL_CANCEL
        ) as APL_CANCEL_DTM,
        DECODE(
            DTIME_APL_CLOSE,
            date '3000-01-01',
            CAST(NULL AS DATE),
            DTIME_APL_CLOSE
        ) as APL_CLOSE_DTM,
        DECODE(
            DTIME_APL_PAIDOFF,
            date '3000-01-01',
            CAST(NULL AS DATE),
            DTIME_APL_PAIDOFF
        ) as APL_PAIDOFF_DTM,
        DECODE(
            DTIME_APL_WRITTEN_OFF,
            date '3000-01-01',
            CAST(NULL AS DATE),
            DTIME_APL_WRITTEN_OFF
        ) as APL_WRITTEN_OFF_DTM,
        DECODE(CODE_APL_PROD_GR, 'XNA', null, CODE_APL_PROD_GR) as APL_PROD_CODE_GR,
        DECODE(
            CODE_APL_CLX_PROD_GR,
            'XNA',
            null,
            CODE_APL_CLX_PROD_GR
        ) as APL_CLX_PROD_CODE_GR,
        DECODE(CODE_APL_PROD, 'XNA', null, CODE_APL_PROD) as APL_PROD_CODE,
        DECODE(
            TEXT_APL_PROD_NAME,
            'XNA',
            null,
            TEXT_APL_PROD_NAME
        ) as APL_PROD_NAME,
        AMT_APL_VOL as APL_VOL,
        AMT_APL_VOL_WOI as APL_VOL_WOI,
        AMT_APL_IR as APL_IR,
        AMT_APL_IR_CALC as APL_IR_CALC,
        AMT_APL_ANNUITY as APL_ANNUITY,
        AMT_APL_ANNUITY_WO_FEE as APL_ANNUITY_WO_FEE,
        CNT_APL_TENOR as APL_TENOR,
        DECODE(
            TEXT_APL_BOD01_CHANNEL,
            'XNA',
            null,
            TEXT_APL_BOD01_CHANNEL
        ) as APL_BOD01_CHANNEL,
        case
            when SKP_EMPLOYEE_APL_BOD02 = 14847683 then 'MOBILE_APP'
            else DECODE(
                TEXT_APL_BOD02_CHANNEL,
                'XNA',
                null,
                TEXT_APL_BOD02_CHANNEL
            )
        end as APL_BOD02_CHANNEL,
        case
            when SKP_EMPLOYEE_APL_SIGN = 14847683 then 'ESIGN'
            else DECODE(
                TEXT_APL_SIGN_CHANNEL,
                'XNA',
                null,
                TEXT_APL_SIGN_CHANNEL
            )
        end as APL_SIGN_CHANNEL,
        NFLAG_APL_F_ESERVICE as APL_F_ESERVICE,
        DECODE(
            TEXT_APL_DISBURS_CHANNEL,
            'XNA',
            null,
            TEXT_APL_DISBURS_CHANNEL
        ) as APL_DISBURS_CHANNEL,
        DECODE(
            TEXT_APL_BOD01_PROCESS,
            'XNA',
            null,
            TEXT_APL_BOD01_PROCESS
        ) as APL_BOD01_PROCESS,
        case
            when DTIME_APL_BOD02 < date '3000-01-01'
            and NFLAG_APL_F_BOD1_BASE = 0 then 1
            else NFLAG_APL_F_BOD1_BASE
        end as APL_F_BOD1_BASE,
        case
            when DTIME_APL_BOD02 < date '3000-01-01'
            and NFLAG_APL_F_BOD1_APPROVE = 0 then 1
            else NFLAG_APL_F_BOD1_APPROVE
        end as APL_F_BOD1_APPROVE,
        case
            when NFLAG_APL_F_BOD2_APPROVE = 1 then 1
            when CODE_APL_PROD_GR = 'BNPL'
            and DTIME_APL_APPROVE < date '3000-01-01' then 1 --TH FIX BNPL base
            when CODE_APL_PROD_GR = 'BNPL'
            and DTIME_APL_REJECT < date '3000-01-01' then 1 --TH FIX BNPL base
            else NFLAG_APL_F_BOD2_BASE
        end as APL_F_BOD2_BASE --TH fix on 29.04.2023
    ,
        NFLAG_APL_F_BOD2_APPROVE as APL_F_BOD2_APPROVE,
        SKP_EMPLOYEE_APL_BOD01 as APL_BOD01_SKP_EMPLOYEE,
        SKP_EMPLOYEE_APL_BOD02 as APL_BOD02_SKP_EMPLOYEE,
        SKP_EMPLOYEE_APL_SIGN as APL_SIGN_SKP_EMPLOYEE,
        SKP_EMPLOYEE_APL_CANCEL as APL_CANCEL_SKP_EMPLOYEE,
        NFLAG_APL_F_NEW as APL_F_NEW,
        DECODE(
            DATE_APL_EARLY_REPAYMENT,
            date '3000-01-01',
            CAST(NULL AS DATE),
            DATE_APL_EARLY_REPAYMENT
        ) as APL_EARLY_REPAYMENT_DT,
        DECODE(
            CODE_APL_POS_PRICING_TYPE,
            'XNA',
            null,
            CODE_APL_POS_PRICING_TYPE
        ) as APL_POS_PRICING_TYPE,
        DECODE(
            TEXT_APL_SIGN_TECH,
            'XNA',
            null,
            TEXT_APL_SIGN_TECH
        ) as APL_SIGN_TECH,
        SKP_EMPLOYEE_APL_CONSULT as APL_CONSULT_SKP_EMPLOYEE,
        case
            when SKP_EMPLOYEE_APL_CONSULT = 14847683 then 'MOBILE_APP'
            else TEXT_APL_CONSULT_CHANNEL
        end as APL_CONSULT_CHANNEL,
        TEXT_APL_FORM as APL_FORM
    FROM
        src__crm__appl
)

select * from final