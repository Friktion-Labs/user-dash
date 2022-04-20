# user firsts tables

select
    fd.user_address
    , fd.first_deposit_date
    , fd.first_deposit_epoch
    , fd.first_deposit_token.list as first_deposit_token
    , fd.first_deposit_amount
    , fw.first_withdrawal_date
    , fw.first_withdrawal_epoch
    , fw.first_withdrawal_token.list as first_withdrawal_token
    , fw.first_withdrawal_amount
from `friktion-dev.users.fact_user_first_deposit` fd
left join `friktion-dev.users.fact_user_first_withdrawal` fw
    on fw.user_address = fd.user_address