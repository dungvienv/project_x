{% set mod_check_query %}

    with validation as (

        SELECT
            *
        FROM DBA_OBJECTS

    ),

    validation_errors as (
        SELECT
            count(*) as exit_code
        FROM validation
        WHERE OBJECT_TYPE = 'PACKAGE'
        AND OWNER = 'AP_CRM'
        AND OBJECT_NAME = 'REP_ACQUISITION_FROM_BOD0'
        AND STATUS = 'VALID'
        AND LAST_DDL_TIME >= date'{{ run_started_at.astimezone(modules.pytz.timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d") }}' - 5
    )

    select *
    from validation_errors 


{% endset %}

{% set mod_result = run_query(mod_check_query) %}
{% if execute %}
    {% if mod_result.rows[0][0] == 0 %}

        SELECT 0 as exit_code FROM dual 

    {% else %}

        SELECT 1 as exit_code FROM dual 

    {% endif %}
    
{% endif %}