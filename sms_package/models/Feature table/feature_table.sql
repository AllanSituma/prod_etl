{{
    config(
        materialized='table',
        schema='data_science'
    )
}}

WITH airtime_purchase as (
    SELECT *
    FROM public_data_science.airtime_ag_purchase
),

bank_deposit as (
    SELECT *
    FROM public_data_science.bank_deposit_ag
),

bank_withdraw as (
    SELECT *
    FROM public_data_science.bank_withdraw_ag
),

betting_receipts as (
    SELECT *
    FROM public_data_science.betting_receipts_ag
),

betting_spends as (
    SELECT *
    FROM public_data_science.betting_spends_ag
),

lipa_mpesa as (
    SELECT * 
    FROM public_data_science.lipa_transactions_ag
),

loanapp_borrowing as (
    SELECT *
    FROM public_data_science.loanapp_borrowing_ag
),

loanapp_repayment as (
    SELECT *
    FROM public_data_science.loanapp_repayment_ag
),

loanapps_used as (
    SELECT *
    FROM public_data_science.loanapps_used
),

mshwari as (
    SELECT *
    FROM public_data_science.mshwari_receipts_ag
),

bank_payments as (
    SELECT *
    FROM public_data_science.payments_via_bank_ag
),

sacco_savings as (
    SELECT *
    FROM public_data_science.sacco_savings_ag
),

sacco_withdraw as (
    SELECT *
    FROM public_data_science.sacco_withdrawal_ag
),


all_clients as (
    SELECT lf."ClientID" user_id,
           lf."NonPerformingLoans" npl
    FROM loan_first lf
)

SELECT DISTINCT ON (ac.user_id) ac.user_id,
        ac.npl,
        ap.max_purchased_airtime,
	    ap.total_airtime_purchase,
	    ap.num_airtime_purchases,
	    ap.avg_daily_airtime_purchases,
	    ap.avg_daily_airtime_amount,
	    ap.avg_weekly_airtime_purchases,
	    ap.avg_weekly_airtime_amount,
	    ap.avg_monthly_airtime_purchases,
	    ap.avg_monthly_airtime_amount,
        bd.max_send_to_bank,
	    bd.min_send_to_bank,
	    bd.total_send_to_bank,
	    bd.num_bank_send_transactions,
 	    bd.avg_daily_bank_send_transactions,
	    bd.avg_daily_bank_send_amount,
	    bd.avg_weekly_bank_send_transactions,
	    bd.avg_weekly_bank_send_amount,
	    bd.avg_monthly_bank_send_transactions,
	    bd.avg_monthly_bank_send_amount,
        bw.max_withdraw_from_bank,
	   bw.min_withdraw_from_bank,
	   bw.total_withdraw_from_bank,
	   bw.num_bank_withdraw_transactions,
 	   bw.avg_daily_bank_w_transactions,
	   bw.avg_daily_bank_w_amount,
	   bw.avg_weekly_bank_w_transactions,
	   bw.avg_weekly_bank_w_amount,
	   bw.avg_monthly_bank_w_transactions,
	   bw.avg_monthly_bank_w_amount,
       br.max_betting_win_amount,
	   br.min_betting_win_amount,
	   br.total_betting_win_amount,
	   br.num_betting_wins,
 	   br.avg_daily_betting_w_transactions,
	   br.avg_daily_betting_w_amount,
	   br.avg_weekly_betting_w_transactions,
	   br.avg_weekly_betting_w_amount,
	   br.avg_monthly_betting_w_transactions,
	   br.avg_monthly_betting_w_amount,
       bs.max_betting_spend_amount,
	   bs.min_betting_spend_amount,
	   bs.total_betting_spend_amount,
	   bs.num_betting_spends,
 	   bs.avg_daily_betting_s_transactions,
	   bs.avg_daily_betting_s_amount,
	   bs.avg_weekly_betting_s_transactions,
	   bs.avg_weekly_betting_s_amount,
	   bs.avg_monthly_betting_s_transactions,
	   bs.avg_monthly_betting_s_amount,
       lm.max_lipa_mpesa_receipts,
	   lm.min_lipa_mpesa_receipts,
	   lm.total_lipa_mpesa_receipts,
	   lm.num_lipa_mpesa_receipts_transactions,
 	   lm.avg_daily_lipa_mpesa_receipts_transactions,
	   lm.avg_daily_lipa_mpesa_receipts_amount,
	   lm.avg_weekly_lipa_mpesa_receipts_transactions,
	   lm.avg_weekly_lipa_mpesa_receipts_amount,
	   lm.avg_monthly_lipa_mpesa_receipts_transactions,
	   lm.avg_monthly_lipa_mpesa_receipts_amount,
       lb.min_creditline,
	   lb.min_credit_lender,
	   lb.max_creditline,
	   lb.max_creditline_lender,
	   lb.loan_apps_used,
	   lb.total_loanapp_transactions,
	   lb.total_loanapp_borrowing,
	   lb.avg_daily_loanapps_borrowing_frequency,
       lb.avg_daily_loanapps_borrowing_amount,
	   lb.avg_weekly_loanapps_borrowing_frequency,
	   lb.avg_loanapps_borrowing_amount,
	   lb.avg_monthly_loanapps_borrowings_frequency,
	   lb.avg_monthly_loanapps_borrowing_amount,
       lr.max_loanapp_repayment,
	   lr.min_loanapp_repayment,
	   lr.total_loanapp_repayment,
	   lr.num_loanapp_repayment_transactions,
 	   lr.avg_daily_loanapp_repayment_transactions,
	   lr.avg_daily_loanapp_repayment_amount,
	   lr.avg_weekly_loanapp_repayment_transactions,
	   lr.avg_weekly_loanapp_repayment_amount,
	   lr.avg_monthly_loanapp_repayment_transactions,
	   lr.avg_monthly_loanapp_repayment_amount,
       lu.Branch,
	   lu.Weza,
		lu.Zidisha,
		lu.Suave,
		lu.Tala,
		lu.Stawika,
		lu.Spider,
		lu.Shika,
		lu.Usawa,
		lu.Pezesha,
		lu.Utunzi,
		lu.Pesapro,
		lu.Peolimona,
		lu.Paddy,
		lu.Opal,
		lu.Okolea,
		lu.Mkopo,
		lu.Lpesa,
		lu.Letshego,
		lu.Kuwazo,
		lu.Jistawi,
        lu.Inventure,
		lu.Haraka,
		lu.Granary,
		lu.Finance_plan,
		lu.Extend,
		lu.Bidii,
		lu.Bayes,
		lu.Timiza,
		lu.Aliquot,
		lu.Afrikaloan,
		lu.Afri,
		lu.Aero,
        m.max_mshwari_receipts,
	   m.min_mshwari_receipts,
	   m.total_mshwari_receipts,
	   m.num_mshwari_transactions,
 	   m.avg_daily_mshwari_transactions,
	   m.avg_daily_mshwari_amount,
	   m.avg_weekly_mshwari_transactions,
	   m.avg_weekly_mshwari_amount,
	   m.avg_monthly_mshwari_transactions,
	   m.avg_monthly_mshwari_amount,
       bp.max_payments_via_bank,
	   bp.min_payments_via_bank,
	   bp.total_payments_via_bank,
	   bp.num_payments_via_bank_transactions,
 	   bp.avg_daily_payments_via_bank_transactions,
	   bp.avg_daily_payments_via_bank_amount,
	   bp.avg_weekly_payments_via_bank_transactions,
	   bp.avg_weekly_payments_via_bank_amount,
	   bp.avg_monthly_payments_via_bank_transactions,
	   bp.avg_monthly_payments_via_bank_amount,
       ss.max_sacco_savings,
	   ss.min_sacco_savings,
	   ss.total_sacco_savings,
	   ss.num_sacco_savings_transactions,
 	   ss.avg_daily_sacco_savings_transactions,
	   ss.avg_daily_sacco_savings_amount,
	   ss.avg_weekly_sacco_savings_transactions,
	   ss.avg_weekly_sacco_savings_amount,
	   ss.avg_monthly_sacco_savings_transactions,
	   ss.avg_monthly_sacco_savings_amount,
       sw.max_sacco_withdraw,
	   sw.min_sacco_withdraw,
	   sw.total_sacco_withdraw,
	   sw.num_sacco_withdraw_transactions,
 	   sw.avg_daily_sacco_withdraw_transactions,
	   sw.avg_daily_sacco_withdraw_amount,
	   sw.avg_weekly_sacco_withdraw_transactions,
	   sw.avg_weekly_sacco_withdraw_amount,
	   sw.avg_monthly_sacco_withdraw_transactions,
	   sw.avg_monthly_sacco_withdraw_amount

FROM all_clients ac
LEFT JOIN airtime_purchase ap ON ap.user_id = ac.user_id
LEFT JOIN bank_deposit bd ON bd.user_id = ac.user_id
LEFT JOIN bank_withdraw bw ON bw.user_id = ac.user_id
LEFT JOIN betting_receipts br ON br.user_id = ac.user_id
LEFT JOIN betting_spends bs ON bs.user_id = ac.user_id
LEFT JOIN lipa_mpesa lm ON lm.user_id = ac.user_id
LEFT JOIN loanapp_borrowing lb ON lb.user_id = ac.user_id
LEFT JOIN loanapp_repayment lr ON lr.user_id = ac.user_id
LEFT JOIN loanapps_used lu ON lu.user_id = ac.user_id
LEFT JOIN mshwari m ON m.user_id = ac.user_id
LEFT JOIN bank_payments bp ON bp.user_id = ac.user_id
LEFT JOIN sacco_savings ss ON ss.user_id = ac.user_id
LEFT JOIN sacco_withdraw sw ON sw.user_id = ac.user_id
