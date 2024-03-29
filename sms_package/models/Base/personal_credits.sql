{{
    config(
        materialized='incremental',
        schema='data_science',
        post_hook='create index if not exists {{ this.name }}__index_on_user_id on {{ this }} ("user_id")'
    )

}}


SELECT DISTINCT ON (transaction_code) raw_sms.*
FROM raw_sms 
WHERE split_part(sender,' ',3)  LIKE '%07%'
OR split_part(sender,' ',4)  LIKE '%07%'

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
AND created_at > (SELECT MAX(created_at) FROM {{this}})

{% endif %}
