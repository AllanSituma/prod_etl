{{
      config(
            materialized='incremental',
            schema='data_science',
            post_hook='create index if not exists {{ this.name }}__index_on_user_id on {{ this }} ("user_id")'
      )
}}


SELECT DISTINCT ON (transaction_code)  user_id,
       amount,
       transaction_code,
       created_at,
	transaction_date
FROM raw_sms
WHERE payment_type = 'mshwari_loan'

{% if is_incremental() %}

AND created_at > (SELECT MAX(created_at)FROM {{this}})

{% endif %}