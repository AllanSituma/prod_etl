{{
	config(
		materialized='table',
		schema='data_science',
		post_hook='create index if not exists {{ this.name }}__index_on_user_id on {{ this }} ("user_id")'
	)
}}

SELECT  user_id,
        sum(case when 'BRANCH' = ANY(loan_apps) then 1 else 0 end) Branch,
		sum(case when 'WEZA' = ANY(loan_apps) then 1 else 0 end) Weza,
		sum(case when 'Zidisha' = ANY(loan_apps) then 1 else 0 end) Zidisha,
		sum(case when 'SUAVE' = ANY(loan_apps) then 1 else 0 end) Suave,
		sum(case when 'Tala' = ANY(loan_apps) then 1 else 0 end) Tala,
		sum(case when 'STAWIKA' = ANY(loan_apps) then 1 else 0 end) Stawika,
		sum(case when 'SPIDER' = ANY(loan_apps) then 1 else 0 end) Spider,
		sum(case when 'SHIKA' = ANY(loan_apps) then 1 else 0 end) Shika,
		sum(case when 'USAWA2' = ANY(loan_apps) then 1 else 0 end) Usawa,
		sum(case when 'PEZESHA' = ANY(loan_apps) then 1 else 0 end) Pezesha,
		sum(case when 'UTUNZI' = ANY(loan_apps) then 1 else 0 end) Utunzi,
		sum(case when 'PESAPRO' = ANY(loan_apps) then 1 else 0 end) Pesapro,
		sum(case when 'PEOLIMONA' = ANY(loan_apps) then 1 else 0 end) Peolimona,
		sum(case when 'Paddy' = ANY(loan_apps) then 1 else 0 end) Paddy,
		sum(case when 'OPAL' = ANY(loan_apps) then 1 else 0 end) Opal,
		sum(case when 'OKOLEA' = ANY(loan_apps) then 1 else 0 end) Okolea,
		sum(case when 'MKOPO' = ANY(loan_apps) then 1 else 0 end) Mkopo,
		sum(case when 'LPESA' = ANY(loan_apps) then 1 else 0 end) Lpesa,
		sum(case when 'Letshego' = ANY(loan_apps) then 1 else 0 end) Letshego,
		sum(case when 'KUWAZO' = ANY(loan_apps) then 1 else 0 end) Kuwazo,
		sum(case when 'JISTAWI' = ANY(loan_apps) then 1 else 0 end) Jistawi,
        sum(case when 'INVENTURE' = ANY(loan_apps) then 1 else 0 end) Inventure,
		sum(case when 'HARAKA' = ANY(loan_apps) then 1 else 0 end) Haraka,
		sum(case when 'GRANARY' = ANY(loan_apps) then 1 else 0 end) Granary,
		sum(case when 'FINANCE PLAN' = ANY(loan_apps) then 1 else 0 end) Finance_plan,
		sum(case when 'EXTEND' = ANY(loan_apps) then 1 else 0 end) Extend,
		sum(case when 'BIDII' = ANY(loan_apps) then 1 else 0 end) Bidii,
		sum(case when 'BAYES' = ANY(loan_apps) then 1 else 0 end) Bayes,
		sum(case when 'TIMIZA' = ANY(loan_apps) then 1 else 0 end) Timiza,
		sum(case when 'ALIQUOT' = ANY(loan_apps) then 1 else 0 end) Aliquot,
		sum(case when 'AFRIKALOAN' = ANY(loan_apps) then 1 else 0 end) Afrikaloan,
		sum(case when 'AFRI' = ANY(loan_apps) then 1 else 0 end) Afri,
		sum(case when 'AERO' = ANY(loan_apps) then 1 else 0 end) Aero
FROM {{ref('loanapp_borrowing_ag')}}
GROUP BY user_id