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
	paybill,
	user_id,
	transaction_code,
	split_part(paybill,' ',1) first_part,
	split_part(paybill,' ',2) second_part,
	split_part(paybill,' ',3) third_part,
	split_part(paybill,' ',4) fourth_part,
	split_part(paybill,' ',5) fifth_part,
	split_part(paybill,' ',6) sixth_part,
	transaction_date,
	created_at
FROM {{ref('dbt_financial_debits')}}

)


   SELECT user_id,
	      paybill,
	      amount,
	      first_part,
		  transaction_code,
	      transaction_date,
		  created_at
	FROM all_transactions
	WHERE first_part IN ('1XBET','1XBET 2','BETBOSS','BETBOSS SUPA3','BETIKA',
		   'BETIN',
		   'Betway','Betway.','BETYETU',
		   'DAFABET',
		   'O-PLAY',
		   'Pevans','SAFARIBET','SAFARIBET-','SHABIKI',
		   'SportPesa.','SportyBet','SPOTCASH','SUPER JACKPOT',
		    'GAMEMANIA'	,'CHAPABETS','GAMING','JUSTBET','LOTTO'	,'LOTTO 1','LOTT0 JOTO','mCHEZA','MOZZARTBET','ODIBETS'	)

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
 AND created_at > (select max(created_at) from {{ this }})

{% endif %}