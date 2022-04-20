-- TODO: Parameterize project-id, dataset, and table

select
    max(w.timestamp) as most_recent_txn_ts
from `solana.withdrawals` w