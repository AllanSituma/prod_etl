{{
      config(
            materialized='incremental',
            schema='data_science',
            post_hook='create index if not exists {{ this.name }}__index_on_user_id on {{ this }} ("user_id")'
      )
}}


SELECT user_id,
       amount,
       transaction_code,
	 transaction_date,
       created_at
FROM (
SELECT *
FROM raw_sms
)q1
WHERE split_part(sender,' ',1) = 'Lipa'

{% if is_incremental() %}

AND  created_at > (SELECT MAX(created_at)FROM {{this}})

{% endif %}