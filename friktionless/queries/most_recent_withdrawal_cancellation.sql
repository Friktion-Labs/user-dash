-- TODO: Parameterize project-id, dataset, and table

select
    max(wcp.timestamp) as most_recent_txn_ts
from `solana.withdrawals_cancel_pending` wcp