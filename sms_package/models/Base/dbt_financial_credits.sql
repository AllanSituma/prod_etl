{{
    config(
        materialized='incremental',
        schema='data_science',
        post_hook='create index if not exists {{ this.name }}__index_on_user_id on {{ this }} ("user_id")'
    )
}}

SELECT DISTINCT ON (transaction_code)
raw_sms.*
FROM raw_sms

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
WHERE created_at > (SELECT MAX(created_at) FROM {{this}})

{% endif %}

AND  sender is not null
AND split_part(sender,' ',1) != 'Lipa'
AND split_part(sender,' ',3) NOT LIKE '%07%'
AND split_part(sender,' ',4) NOT LIKE '%07%'
AND split_part(sender,' ',5) NOT LIKE '%07%'
AND split_part(sender,' ',6) NOT LIKE '%07%'
AND split_part(sender,' ',6) NOT LIKE '%254%'
AND split_part(sender,' ',3) NOT LIKE '%254%'
