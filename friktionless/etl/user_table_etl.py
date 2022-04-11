import logging

# create logger
logger = logging.getLogger('user_etl_logger')
logger.setLevel(logging.DEBUG)

logger.info('importing packages')
import pandas as pd
import datetime

logger.info('creating master user table in pandas')
master_user_table = pd.DataFrame(columns=['user_address', 'as_of_date', 'total_value_locked_USD', 'total_deposited_USD', 'total_withdrawn_USD', \
                                           'tvl_delta_30_days', 'tvl_delta_60_days', 'tvl_delta_90_days', \
                                           'tvl_delta_1_epoch', 'tvl_delta_2_epoch', \
                                           'tvl_delta_3_epoch', 'tvl_delta_4_epoch', 'tvl_delta5_epoch', \
                                           'first_deposit_date', 'first_deposit_epoch', 'first_deposit_amount', 'first_deposit_token', \
                                           'last_deposit_date', 'last_deposit_epoch', 'last_deposit_amount', 'last_deposit_token', \
                                           'days_since_last_deposit', 'epochs_since_last_deposit', \
                                           'first_withdrawal_date', 'first_withdrawal_epoch', 'first_withdrawal_amount', 'first_withdrawal_token', \
                                           'last_withdrawal_date', 'last_withdrawal_epoch', 'last_withdrawal_amount', 'last_withdrawal_token', \
                                           'has_churned', 'churn_date', 'churn_epoch'])

    
logger.info('querying for the dates dataframe')
dates = pd.read_gbq(
    query='select distinct date(deposit_initiated_ts) as date from transactions.fact_deposits',
    project_id='friktion-dev'
    )

logger.info('querying for the users dataframe')
users = pd.read_gbq(
    query='select distinct(user_address) as user_address from transactions.fact_deposits')

logger.info('querying for user first deposit dates dataframe')
user_first_deposits_dates = pd.read_gbq(
    query='select user_address, min(date(deposit_initiated_ts)) as first_deposit_dt from transactions.fact_deposits group by user_address')

logger.info('start looping through all users')
for index in range(user_first_deposits_dates.shape[0]):
    current_user_address = user_first_deposits_dates.iloc[index,:]['user_address']
    user_first_deposit_date = user_first_deposits_dates.iloc[index,:]['first_deposit_dt']
    logger.info(f'processing user {current_user_address} who first deposited on {user_first_deposit_date.isoformat()}')

    user_deposit_table = pd.DataFrame(columns=['user_address', 'as_of_date', 'total_value_locked_USD', 'total_deposited_USD', 'total_withdrawn_USD', \
                                           'tvl_delta_30_days', 'tvl_delta_60_days', 'tvl_delta_90_days', \
                                           'tvl_delta_1_epoch', 'tvl_delta_2_epoch', \
                                           'tvl_delta_3_epoch', 'tvl_delta_4_epoch', 'tvl_delta5_epoch', \
                                           'first_deposit_date', 'first_deposit_epoch', 'first_deposit_amount', 'first_deposit_token', \
                                           'last_deposit_date', 'last_deposit_epoch', 'last_deposit_amount', 'last_deposit_token', \
                                           'days_since_last_deposit', 'epochs_since_last_deposit', \
                                           'first_withdrawal_date', 'first_withdrawal_epoch', 'first_withdrawal_amount', 'first_withdrawal_token', \
                                           'last_withdrawal_date', 'last_withdrawal_epoch', 'last_withdrawal_amount', 'last_withdrawal_token', \
                                           'has_churned', 'churn_date', 'churn_epoch'])
    
    user_deposit_table['as_of_date'] = dates[dates['date'].apply(lambda x: x>=user_first_deposit_date)].sort_values('date').iloc[:,0]
    
    user_deposit_table['user_address'] = current_user_address
    
    # need to add market marking code
    user_deposit_table['total_deposited_USD'] = [pd.read_gbq(query=f"select sum(deposit_initiated_amt) as total_deposited from transactions.fact_deposits where user_address=\'{current_user_address}\' and date(deposit_initiated_ts) <= date(\'{query_date}\')").iloc[0,0] for query_date in user_deposit_table['as_of_date'].tolist()]
    
    # need to add market marking code
    # will need to change to the usd amount from the trans tables in prod
    user_deposit_table['total_withdrawn_USD'] = [pd.read_gbq(query=f"select sum(withdrawal_initiated_amt) as total_withdrawn from transactions.fact_withdrawals where user_address=\'{current_user_address}\' and date(withdrawal_initiated_ts) <= date(\'{query_date}\')").fillna(0).iloc[0,0] for query_date in user_deposit_table['as_of_date'].tolist()]
    
    user_deposit_table['total_value_locked_USD'] = user_deposit_table['total_deposited_USD'] - user_deposit_table['total_withdrawn_USD']
    
    user_deposit_table['first_deposit_date'] =  user_first_deposit_date
    
    user_deposit_table['first_deposit_epoch'] = pd.read_gbq(query=f'select deposit_initiated_epoch from transactions.fact_deposits where date(deposit_initiated_ts) = \'{user_first_deposit_date}\'')
    
    user_deposit_table['first_deposit_amount'] = pd.read_gbq(query=f'select sum(deposit_initiated_amt) from transactions.fact_deposits where user_address = \'{current_user_address}\' and date(deposit_initiated_ts) = \'{user_first_deposit_date.isoformat()}\'').iloc[0,0]
    
    user_deposit_table['first_deposit_token'] = pd.read_gbq(query=f'select deposited_asset from transactions.fact_deposits where user_address = \'{current_user_address}\' and date(deposit_initiated_ts) = \'{user_first_deposit_date.isoformat()}\'').iloc[0,0]
    
    tvl_list = []
    for table_date in user_deposit_table['as_of_date']:
        current_as_of_date = table_date
        # we can also perform this logic with GBQ SQL
        #      DATE_ADD(DATE "2022-01-19", INTERVAL -30 DAY)
        window_end_date = current_as_of_date - datetime.timedelta(30)
        window_delta = pd.read_gbq(query=f'\
            with window_deposits as \
             (select user_address, IFNULL(sum(deposit_initiated_amt), 0.0) as deposits \
             from transactions.fact_deposits \
             where user_address = \'{current_user_address}\' \
             and date(deposit_initiated_ts) between \'{window_end_date}\' and \'{current_as_of_date.isoformat()}\'\
                 group by user_address) \
            , window_withdrawals as \
                (select user_address, sum(withdrawal_initiated_amt) as withdrawals \
                from transactions.fact_withdrawals \
                where user_address = \'{current_user_address}\' \
                and date(withdrawal_initiated_ts) between \'{window_end_date}\' and \'{current_as_of_date.isoformat()}\'\
                group by user_address) \
            select \
                 IFNULL(deposits, 0.0) - IFNULL(withdrawals, 0.0) \
            from window_deposits wd\
            left join window_withdrawals ww on wd.user_address = ww.user_address \
            ')
        if window_delta.empty:
            # print(window_end_date, current_as_of_date)
            # print(table_date, window_delta.empty)
            tvl_list.append(0.0)
        else:
            # print(window_end_date, current_as_of_date)
            # print(window_delta)
            # print(table_date, window_delta.iloc[0,0])
            tvl_list.append(window_delta.iloc[0,0])

    user_deposit_table['tvl_delta_30_days'] = tvl_list
    
    tvl_list = []
    for table_date in user_deposit_table['as_of_date']:
        current_as_of_date = table_date
        # we can also perform this logic with GBQ SQL
        #      DATE_ADD(DATE "2022-01-19", INTERVAL -30 DAY)
        window_end_date = current_as_of_date - datetime.timedelta(60)
        window_delta = pd.read_gbq(query=f'\
            with window_deposits as \
             (select user_address, IFNULL(sum(deposit_initiated_amt), 0.0) as deposits \
             from transactions.fact_deposits \
             where user_address = \'{current_user_address}\' \
             and date(deposit_initiated_ts) between \'{window_end_date}\' and \'{current_as_of_date.isoformat()}\'\
                 group by user_address) \
            , window_withdrawals as \
                (select user_address, sum(withdrawal_initiated_amt) as withdrawals \
                from transactions.fact_withdrawals \
                where user_address = \'{current_user_address}\' \
                and date(withdrawal_initiated_ts) between \'{window_end_date}\' and \'{current_as_of_date.isoformat()}\'\
                group by user_address) \
            select \
                 IFNULL(deposits, 0.0) - IFNULL(withdrawals, 0.0) \
            from window_deposits wd\
            left join window_withdrawals ww on wd.user_address = ww.user_address \
            ')
        if window_delta.empty:
            # print(window_end_date, current_as_of_date)
            # print(table_date, window_delta.empty)
            tvl_list.append(0.0)
        else:
            # print(window_end_date, current_as_of_date)
            # print(window_delta)
            # print(table_date, window_delta.iloc[0,0])
            tvl_list.append(window_delta.iloc[0,0])

    user_deposit_table['tvl_delta_60_days'] = tvl_list
    
    tvl_list = []
    for table_date in user_deposit_table['as_of_date']:
        current_as_of_date = table_date
        # we can also perform this logic with GBQ SQL
        #      DATE_ADD(DATE "2022-01-19", INTERVAL -30 DAY)
        window_end_date = current_as_of_date - datetime.timedelta(90)
        window_delta = pd.read_gbq(query=f'\
            with window_deposits as \
             (select user_address, IFNULL(sum(deposit_initiated_amt), 0.0) as deposits \
             from transactions.fact_deposits \
             where user_address = \'{current_user_address}\' \
             and date(deposit_initiated_ts) between \'{window_end_date}\' and \'{current_as_of_date.isoformat()}\'\
                 group by user_address) \
            , window_withdrawals as \
                (select user_address, sum(withdrawal_initiated_amt) as withdrawals \
                from transactions.fact_withdrawals \
                where user_address = \'{current_user_address}\' \
                and date(withdrawal_initiated_ts) between \'{window_end_date}\' and \'{current_as_of_date.isoformat()}\'\
                group by user_address) \
            select \
                 IFNULL(deposits, 0.0) - IFNULL(withdrawals, 0.0) \
            from window_deposits wd\
            left join window_withdrawals ww on wd.user_address = ww.user_address \
            ')
        if window_delta.empty:
            # print(window_end_date, current_as_of_date)
            # print(table_date, window_delta.empty)
            tvl_list.append(0.0)
        else:
            # print(window_end_date, current_as_of_date)
            # print(window_delta)
            # print(table_date, window_delta.iloc[0,0])
            tvl_list.append(window_delta.iloc[0,0])

    user_deposit_table['tvl_delta_90_days'] = tvl_list
    
    last_deposit_list = []

    for table_date in user_deposit_table['as_of_date']:
        current_as_of_date = table_date
        r = pd.read_gbq(query=f'select max(date(deposit_initiated_ts)) from transactions.fact_deposits where user_address = \'{current_user_address}\' and date(deposit_initiated_ts) <= \'{current_as_of_date}\'')
        last_deposit_list.append(r.iloc[0,0])
    user_deposit_table['last_deposit_date'] = last_deposit_list
    
    last_deposit_epoch_list = []

    for table_date in user_deposit_table['as_of_date']:
        current_as_of_date = table_date
        r = pd.read_gbq(query=f'select max((deposit_initiated_epoch)) from transactions.fact_deposits where date(deposit_initiated_ts) <= \'{current_as_of_date}\'')
        last_deposit_epoch_list.append(r.iloc[0,0])
    user_deposit_table['last_deposit_epoch'] = last_deposit_epoch_list
    
    last_deposit_amount_list = []
last_deposit_token_list = []

for table_date in user_deposit_table['last_deposit_date']:
    r = pd.read_gbq(query=f'select deposited_asset, deposit_initiated_amt from transactions.fact_deposits where user_address = \'{current_user_address}\' and date(deposit_initiated_ts) = \'{table_date}\'')
    
    last_deposit_token_list.append(r.iloc[0,0])
    last_deposit_amount_list.append(r.iloc[0,1])
    user_deposit_table['last_deposit_token'] = last_deposit_token_list
    user_deposit_table['last_deposit_amount'] = last_deposit_amount_list
    
    days_since_last_deposit_list = []
    for as_of_date, last_deposit_date in user_deposit_table[['as_of_date', 'last_deposit_date']].to_records(index=False).tolist():
        days_since_last_deposit_list.append((as_of_date - last_deposit_date).days)
    user_deposit_table['days_since_last_deposit'] = days_since_last_deposit_list
    
    r = pd.read_gbq(query=f'select min(date(withdrawal_initiated_ts)) from transactions.fact_withdrawals where user_address = \'{current_user_address}\'')
    if r.empty:
        pass
    else:
        user_deposit_table['first_withdrawal_date'] = r.iloc[0,0]
        
    first_withdrawal_date = user_deposit_table['first_withdrawal_date'].iloc[0]
    r = pd.read_gbq(query=f'select (withdrawal_initiated_epoch) from transactions.fact_withdrawals where date(withdrawal_initiated_ts) <= \'{first_withdrawal_date}\'')
    user_deposit_table['first_withdrawal_epoch'] = r.iloc[0,0]
    
    first_withdrawal_date = user_deposit_table['first_withdrawal_date'].iloc[0]
    r = pd.read_gbq(query=f'select deposited_asset, withdrawal_initiated_amt from transactions.fact_withdrawals where user_address = \'{current_user_address}\' and date(withdrawal_initiated_ts) = \'{first_withdrawal_date}\'')
    r
    user_deposit_table['first_withdrawal_token'] = r.iloc[0,0]
    user_deposit_table['first_withdrawal_amount'] = r.iloc[0,1]
    
    last_withdrawal_list = []

    for table_date in user_deposit_table['as_of_date']:
        current_as_of_date = table_date
        r = pd.read_gbq(query=f'select max(date(withdrawal_initiated_ts)) from transactions.fact_withdrawals where user_address = \'{current_user_address}\' and date(withdrawal_initiated_ts) <= \'{current_as_of_date}\'')
        last_withdrawal_list.append(r.iloc[0,0])
    user_deposit_table['last_withdrawal_date'] = last_withdrawal_list
    
    last_withdrawal_epoch_list = []

    for table_date in user_deposit_table['last_withdrawal_date']:
        if pd.isnull(table_date):
            last_withdrawal_epoch_list.append(None)
        else:
            r = pd.read_gbq(query=f'select max((withdrawal_initiated_epoch)) from transactions.fact_withdrawals where user_address = \'{current_user_address}\' and date(withdrawal_initiated_ts) = \'{table_date}\'')
            last_withdrawal_epoch_list.append(r.iloc[0,0])
    user_deposit_table['last_withdrawal_epoch'] = last_withdrawal_epoch_list
    
    last_withdrawal_amount_list = []
    last_withdrawal_token_list = []

    for table_date in user_deposit_table['last_withdrawal_date']:
        current_as_of_date = table_date
        # print(current_as_of_date)
        if pd.isnull(current_as_of_date):
            last_withdrawal_token_list.append(None)
            last_withdrawal_amount_list.append(None)
        else:
            r = pd.read_gbq(query=f'select deposited_asset, withdrawal_initiated_amt from transactions.fact_withdrawals where user_address = \'{current_user_address}\' and date(withdrawal_initiated_ts) = \'{current_as_of_date}\'')

            last_withdrawal_token_list.append(r.iloc[0,0])
            last_withdrawal_amount_list.append(r.iloc[0,1])
    user_deposit_table['last_withdrawal_token'] = last_withdrawal_token_list
    user_deposit_table['last_withdrawal_amount'] = last_withdrawal_amount_list
    
    user_deposit_table['has_churned'] = (user_deposit_table['total_deposited_USD'] - user_deposit_table['total_withdrawn_USD']).apply(lambda x: x <= 0)
    
    churn_state = user_deposit_table['has_churned'].tolist()
    if any(churn_state):
        user_deposit_table['churn_date'] = None
    else:
        churn_date_list = []

        for index in range(len(churn_state)-1):
            if index == 0:
                churn_date_list.append(None)
            if (churn_state[index-1] == False ) & (churn_state[index] == True):
                churn_date_list.append(user_deposit_table['as_of_date'].iloc[index])
            else:
                churn_date_list.append(churn_date_list[index-1])

        user_deposit_table['churn_date'] = churn_date_list
        
        master_user_table = pd.concat([master_user_table, user_deposit_table])
        
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "friktion-dev.users.fact_users"

job = client.load_table_from_dataframe(
    master_user_table, table_id
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)
