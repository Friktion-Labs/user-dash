import friktionless as fless
from datetime import datetime

# Execute prices table etl
fless.friktion_prices_table('lyrical-amulet-337502')


# Execute core friktion tables etl
lst_user_actions = ['deposit','cancel_pending_deposit','withdrawal','claim_withdrawal','cancel_pending_withdrawal']

for user_action in lst_user_actions:
    
    print(datetime.now(), 'Starting {} etl...'.format(user_action))
    fless.friktion_etl.append_most_recent_transactions('lyrical-amulet-337502', user_action)