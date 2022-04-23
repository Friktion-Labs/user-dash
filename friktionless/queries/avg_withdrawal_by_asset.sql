select
    fw.deposited_asset,
    case
        when fw.deposited_asset = 'tsUSDC' then 'SOL [tsUSDC]' 
        when fw.volt_number = 1 and fw.deposited_asset = 'SOL' and fw.is_high_voltage then 'SOL [high]'
        when fw.volt_number = 1 and fw.deposited_asset = 'SOL' and not fw.is_high_voltage then 'SOL [low]'
        when fw.volt_number = 2 and pc.asset = 'SOL' and fw.is_high_voltage then 'SOL [high]'
        when fw.volt_number = 2 and pc.asset = 'SOL' and not fw.is_high_voltage then 'SOL [low]'
        else pc.asset end as underlying_asset,
    fw.product_name,
    fw.strategy,
    fw.volt_number,
    fw.is_high_voltage,
    case
        when fw.deposited_asset in ('tsUSDC','USDC') then sum(fw.withdrawal_initiated_amt_usd)
        else sum(fw.withdrawal_initiated_amt_token * p2.price_usd)
        end as total_withdrawal_initiated_amt_usd_spot,
    case
        when fw.deposited_asset in ('tsUSDC','USDC') then avg(fw.withdrawal_initiated_amt_usd)
        else avg(fw.withdrawal_initiated_amt_token * p2.price_usd)
        end as avg_withdrawal_initiated_amt_usd_spot
from `transactions.fact_withdrawals` fw
    left join (
        select
            p.asset,
            p.price_usd     
        from analytics.prices p
        where
            p.timestamp in (select max(p2.timestamp) from analytics.prices p2 where p2.asset = p.asset)
    ) p2 on p2.asset = fw.deposited_asset
    left join analytics.product_catalog pc on pc.product_name = fw.product_name
where
    fw.custody_epoch between cast('{0}' as integer) and cast('{1}' as integer) and
    cast(fw.volt_number as string) = '{2}'
group by 1,2,3,4,5,6
order by 2