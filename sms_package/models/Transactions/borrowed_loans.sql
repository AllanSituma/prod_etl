{{
    config(
        materialized='incremental',
        schema='data_science',
		post_hook='create index if not exists {{ this.name }}__index_on_user_id on {{ this }} ("user_id")'
    )
}}

WITH all_transactions as (
SELECT
    amount,
	sender,
	user_id,
	transaction_code,
	split_part(sender,' ',1) first_part,
	split_part(sender,' ',2) second_part,
	split_part(sender,' ',3) third_part,
	split_part(sender,' ',4) fourth_part,
	split_part(sender,' ',5) fifth_part,
	split_part(sender,' ',6) sixth_part,
	transaction_date,
	created_at
FROM {{ref('dbt_financial_credits')}}
)

SELECT user_id,
	      sender,
	      amount,
	      CASE WHEN third_part = 'TIMIZA' then 'TIMIZA' 
	           else first_part
	      end first_part,
		  transaction_code,
	      transaction_date,
		  created_at
	FROM all_transactions
	WHERE first_part IN (
	  'AERO','AFRI','AFRIKALOAN','ALIQUOT','BAYES',
		'BIDII','BRANCH','EXTEND','FINANCE PLAN','GRANARY',
		'HARAKA','INVENTURE','JISTAWI','KUWAZO','Letshego',
		'LPESA','MKOPO','OKOLEA','OPAL','Paddy','PEOLIMONA','PESAPRO',
		'UTUNZI','PEZESHA','USAWA2','SHIKA','SPIDER','STAWIKA','Tala',
		'SUAVE','Zidisha','WEZA','UBAPESA') 
	
	OR second_part in (
	'AERO','AFRI','AFRIKALOAN','ALIQUOT','BAYES',
		'BIDII','BRANCH','EXTEND','FINANCE PLAN','GRANARY',
		'HARAKA','INVENTURE','JISTAWI','KUWAZO','Letshego',
		'LPESA','MKOPO','OKOLEA','OPAL','Paddy','PEOLIMONA','PESAPRO',
		'UTUNZI','PEZESHA','USAWA2','SHIKA','SPIDER','STAWIKA','Tala',
		'SUAVE','Zidisha','WEZA','UBAPESA')



	OR third_part in (
	'AERO','AFRI','AFRIKALOAN','ALIQUOT','TIMIZA','BAYES',
		'BIDII','BRANCH','EXTEND','FINANCE PLAN','GRANARY',
		'HARAKA','INVENTURE','JISTAWI','KUWAZO','Letshego',
		'LPESA','MKOPO','OKOLEA','OPAL','Paddy','PEOLIMONA','PESAPRO',
		'UTUNZI','PEZESHA','USAWA2','SHIKA','SPIDER','STAWIKA','Tala',
		'SUAVE','Zidisha','WEZA','UBAPESA')


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  AND created_at > (select max(created_at) from {{ this }})

{% endif %}
		

	


