{{
    config(
        materialized='incremental',
        schema='data_science'
    )
}}

SELECT DISTINCT ON (transaction_code)user_id,
       amount,
       transaction_code,
	     transaction_date,
       created_at 
FROM raw_sms
WHERE q1.payment_type = 'airtime'

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  AND created_at > (select max(created_at) from {{ this }})

{% endif %}