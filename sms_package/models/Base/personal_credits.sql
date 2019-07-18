{{
    config(
        materialized='incremental',
        schema='data_science'
    )

}}


SELECT DISTINCT ON (transaction_code) raw_sms.*
FROM raw_sms 

{% if is_incremental() %}

WHERE created_at > (SELECT MAX(created_at)FROM {{this}})

{% endif %}

AND split_part(sender,' ',3)  LIKE '%07%'
OR split_part(sender,' ',4)  LIKE '%07%'

