select
    nff.*,
    nff.net_withdrawal_amt * -1 as net_withdrawal_amt_neg
from transactions.net_funds_flow_by_epoch nff
where
    nff.product_name = '{}'