# user firsts table

/*
    The user firsts table sql creates a single record of all first activites for a user.

    It depends on the users.fact_user_first_deposit and users.fact_user_first_withdrawal tables
    
    TODO: move this a streaming processing format - See Apache Beam executed on Google Dataflow.
*/

select
    fd.user_address
    , fd.first_deposit_date
    , fd.first_deposit_epoch
    , fd.first_deposit_token as first_deposit_token
    , fd.first_deposit_amount
    , fw.first_withdrawal_date
    , fw.first_withdrawal_epoch
    , fw.first_withdrawal_token as first_withdrawal_token
    , fw.first_withdrawal_amount
from users.fact_user_first_deposit fd
left join users.fact_user_first_withdrawal fw
    on fw.user_address = fd.user_address