# user_first_withdrawal_sql

/*
    The user firsts table sql creates a single record of all first withdrawal activites for a user.

    It depends on the transactions.fact_withdrawals table
    
    TODO: move this a streaming processing format - See Apache Beam executed on Google Dataflow.
*/

with first_withdrawal_dt as (
    select user_address, min(date(withdrawal_initiated_ts)) as first_withdrawal_dt 
    from transactions.fact_withdrawals group by user_address
)

select 
    dep.user_address, 
    date(withdrawal_initiated_ts) as first_withdrawal_date,
    array_agg(withdrawal_initiated_epoch) as first_withdrawal_epoch,
    sum(withdrawal_initiated_amt_usd) as first_withdrawal_amount,
    array_agg(deposited_asset)as first_withdrawal_token
from transactions.fact_withdrawals dep 
inner join first_withdrawal_dt fd on fd.user_address = dep.user_address and fd.first_withdrawal_dt = date(dep.withdrawal_initiated_ts)
group by 1, 2
