select
    fd.deposited_asset,
    case
        when fd.deposited_asset = 'tsUSDC' then 'SOL [tsUSDC]' 
        when fd.volt_number = 1 and fd.deposited_asset = 'SOL' and fd.is_high_voltage then 'SOL [high]'
        when fd.volt_number = 1 and fd.deposited_asset = 'SOL' and not fd.is_high_voltage then 'SOL [low]'
        when fd.volt_number = 2 and pc.asset = 'SOL' and fd.is_high_voltage then 'SOL [high]'
        when fd.volt_number = 2 and pc.asset = 'SOL' and not fd.is_high_voltage then 'SOL [low]'
        else pc.asset end as underlying_asset,
    fd.product_name,
    fd.strategy,
    fd.volt_number,
    fd.is_high_voltage,
    case
        when fd.deposited_asset in ('tsUSDC','USDC') then sum(fd.deposit_initiated_amt_usd)
        else sum(fd.deposit_initiated_amt_token * p2.price_usd)
        end as total_deposit_initiated_amt_usd_spot,
    case
        when fd.deposited_asset in ('tsUSDC','USDC') then avg(fd.deposit_initiated_amt_usd)
        else avg(fd.deposit_initiated_amt_token * p2.price_usd)
        end as avg_deposit_initiated_amt_usd_spot
from `transactions.fact_deposits` fd
    left join (
        select
            p.asset,
            p.price_usd     
        from analytics.prices p
        where
            p.timestamp in (select max(p2.timestamp) from analytics.prices p2 where p2.asset = p.asset)
    ) p2 on p2.asset = fd.deposited_asset
    left join analytics.product_catalog pc on pc.product_name = fd.product_name
where
    cast(fd.custody_epoch as string) between '{0}' and '{1}' and
    cast(fd.volt_number as string) = '{2}'
group by 1,2,3,4,5,6
order by 2