# user_table_sql_script

declare max_date date;
declare as_of_date_range array<date>;
declare first_deposit_user_address string;
declare first_deposit_dt date;

set max_date = @max_date;
set first_deposit_user_address = @user_address;
set first_deposit_dt = @first_deposit_dt;
set as_of_date_range = generate_date_array(first_deposit_dt, max_date, INTERVAL 1 DAY);

/*****************************
  Make individual user table
*****************************/
create temp table individual_user (user_address STRING, as_of_date DATE) as
    select first_deposit_user_address, dates from unnest(as_of_date_range) as dates;


/*****************************
  Make user daily deposit table
*****************************/

create temp table user_daily_deposits (user_address string, as_of_date date, deposit_initiated_amt_usd float64) as 
    select user_address, date(deposit_initiated_ts) as as_of_date, sum(deposit_initiated_amt_usd) from transactions.fact_deposits group by 1, 2;

create temp table user_daily_withdrawals (user_address string, as_of_date date, withdrawal_initiated_amt_usd float64) as 
    select user_address, date(withdrawal_initiated_ts) as as_of_date, sum(withdrawal_initiated_amt_usd) from transactions.fact_withdrawals group by 1, 2;
    
    select 
        cd.user_address, 
        cd.as_of_date, 
        cd.total_deposits_usd, 
        cw.total_withdrawals_usd, 
        (cd.total_deposits_usd - cw.total_withdrawals_usd) as total_value_locked_USD, 
        cd.first_deposit_date, 
        cd.first_deposit_epoch,
        cd.first_deposit_token, 
        cd.first_deposit_amount, 
        cd.first_withdrawal_date, 
        cd.first_withdrawal_epoch, 
        cd.first_withdrawal_token, 
        cd.first_withdrawal_amount,
        lw.last_withdrawal_date, 
        lw.last_withdrawal_epoch, 
        lw.last_withdrawal_amt, 
        lw.last_withdrawal_token,
        ld.last_deposit_amt, 
        ld.last_deposit_epoch, 
        ld.last_deposit_token, 
        ld.last_deposit_date,
        date_diff(cd.as_of_date, ld.last_deposit_date, DAY) as days_since_last_deposit,
        tvl_d30.tvl_delta_30_day, 
        tvl_d60.tvl_delta_60_day, 
        tvl_d90.tvl_delta_90_day,
        case when (cd.total_deposits_usd - cw.total_withdrawals_usd <= 0) then TRUE else FALSE end as has_churned,
        case when (cd.total_deposits_usd - cw.total_withdrawals_usd <= 0) then cd.as_of_date else NULL end as churn_date 
    from (
        select 
            user.user_address, 
            user.as_of_date, 
            sum(dep.deposit_initiated_amt_usd) OVER (order by user.as_of_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as total_deposits_usd, 
            user_firsts.first_deposit_date,
            user_firsts.first_deposit_epoch,
            user_firsts.first_deposit_token,
            user_firsts.first_deposit_amount,
            user_firsts.first_withdrawal_date,
            user_firsts.first_withdrawal_epoch,
            user_firsts.first_withdrawal_token,
            user_firsts.first_withdrawal_amount
        from individual_user user 
        left join user_daily_deposits dep on user.user_address = dep.user_address and user.as_of_date = dep.as_of_date
        left join users.fact_user_firsts user_firsts on user.user_address = user_firsts.user_address
    ) cd 
    left join (
        select user.user_address, user.as_of_date, coalesce(sum(dep.withdrawal_initiated_amt_usd) OVER (order by user.as_of_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0.0) as total_withdrawals_usd
        from individual_user user left join user_daily_withdrawals dep on user.user_address = dep.user_address and user.as_of_date = dep.as_of_date
    ) cw on cd.user_address = cw.user_address and cd.as_of_date = cw.as_of_date
    left join (
        select 
            lw_date.user_address, 
            lw_date.as_of_date, 
            lw_date.last_withdrawal_date, 
            array_agg(distinct fw.withdrawal_initiated_epoch ignore nulls) as last_withdrawal_epoch, 
            array_agg(distinct fw.deposited_asset ignore nulls) as last_withdrawal_token, 
            sum(fw.withdrawal_initiated_amt_usd) as last_withdrawal_amt 
        from (
            select user.user_address, user.as_of_date, max(date(w.as_of_date)) OVER (order by user.as_of_date ROWS BETWEEN unbounded PRECEDING and current row) as last_withdrawal_date
            from individual_user user left join user_daily_withdrawals w on user.user_address = w.user_address and user.as_of_date = w.as_of_date
        ) lw_date 
        left join transactions.fact_withdrawals fw on fw.user_address = lw_date.user_address and date(fw.withdrawal_initiated_ts) = lw_date.last_withdrawal_date
        group by 1, 2, 3
    ) lw on lw.user_address = cd.user_address and cd.as_of_date = lw.as_of_date
    left join (
        select ld_date.user_address, 
        ld_date.as_of_date, 
        ld_date.last_deposit_date, 
        array_agg(distinct ld_other.deposit_initiated_epoch) as last_deposit_epoch, 
        sum(ld_other.deposit_initiated_amt_usd) as last_deposit_amt, 
        array_agg(distinct ld_other.deposited_asset) as last_deposit_token 
        from (
            select user.user_address, user.as_of_date, max(w.as_of_date) OVER (order by user.as_of_date ROWS BETWEEN unbounded PRECEDING and current row) as last_deposit_date
            from individual_user user left join user_daily_deposits w on user.user_address = w.user_address and user.as_of_date = w.as_of_date
        ) ld_date 
        left join (
            select user_address, deposit_initiated_amt_usd, deposit_initiated_epoch, deposit_initiated_ts, deposited_asset 
            from transactions.fact_deposits where user_address = first_deposit_user_address
        ) ld_other
            on ld_other.user_address = ld_date.user_address and date(ld_other.deposit_initiated_ts) = ld_date.last_deposit_date
        group by 1, 2, 3
    ) ld on ld.user_address = cd.user_address and cd.as_of_date = ld.as_of_date
    left join (    
        select user.user_address, user.as_of_date, sum(dly_dep.deposit_initiated_amt_usd) over (order by user.as_of_date rows between 30 preceding and current row) as tvl_delta_30_day 
        from individual_user user 
        left join user_daily_deposits dly_dep 
            on dly_dep.user_address = user.user_address
            and dly_dep.as_of_date = user.as_of_date
    ) tvl_d30 on tvl_d30.user_address = cd.user_address and tvl_d30.as_of_date = cd.as_of_date
    left join (    
        select user.user_address, user.as_of_date, sum(dly_dep.deposit_initiated_amt_usd) over (order by user.as_of_date rows between 60 preceding and current row) as tvl_delta_60_day 
        from individual_user user 
        left join user_daily_deposits dly_dep 
            on dly_dep.user_address = user.user_address
            and dly_dep.as_of_date = user.as_of_date
    ) tvl_d60 on tvl_d60.user_address = cd.user_address and tvl_d60.as_of_date = cd.as_of_date
    left join (    
        select user.user_address, user.as_of_date, sum(dly_dep.deposit_initiated_amt_usd) over (order by user.as_of_date rows between 90 preceding and current row) as tvl_delta_90_day 
        from individual_user user 
        left join user_daily_deposits dly_dep 
            on dly_dep.user_address = user.user_address
            and dly_dep.as_of_date = user.as_of_date
    ) tvl_d90 on tvl_d90.user_address = cd.user_address and tvl_d90.as_of_date = cd.as_of_date;