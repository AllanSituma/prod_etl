{{
    config(
        materialized='incremental',
        schema='data_science',
        post_hook='create index if not exists {{ this.name }}__index_on_user_id on {{ this }} ("user_id")'
    )
}}

SELECT DISTINCT ON (transaction_code)user_id,
       amount,
       transaction_code,
	     transaction_date,
       created_at 
FROM raw_sms
WHERE payment_type = 'airtime'

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  AND created_at > (select max(created_at) from {{ this }})

{% endif %}
