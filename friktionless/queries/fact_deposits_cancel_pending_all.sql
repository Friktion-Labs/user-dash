select *
from transactions.fact_deposits_cancel_pending fd
where
    fd.deposit_cancelled_ts between '{0}' and '{1}' and
    (({2} is null or {3} is null) or fd.deposit_cancelled_epoch between ifnull({2},0) and ifnull({3},0)) and
    (({4} is null or {5} is null) or fd.custody_epoch between ifnull({4},0) and ifnull({5},0)) and
    (if('{6}' = 'null', null, '{6}') is null or fd.deposited_asset = ifnull('{6}','null')) and
    (if('{7}' = 'null', null, '{7}') is null or fd.strategy = ifnull('{7}','null')) and
    ({8} is null or fd.volt_number = ifnull({8},0)) and
    ({9} is null or fd.is_high_voltage is {9})