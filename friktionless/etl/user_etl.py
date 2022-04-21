import pandas as pd
from google.cloud import bigquery
from datetime import date

def write_user_first_deposit_table():
    # create the user first deposit table

    user_first_deposits_query = None

    import os
    fname = 'user_first_deposit.sql'
    this_file = os.path.abspath(__file__)
    this_dir = os.path.dirname(this_file)
    wanted_file = os.path.join(this_dir, fname)
    
    print(f'opening {wanted_file}')
    
    with open(wanted_file, 'r') as f:
        user_first_deposits_query = f.read()
    
    print('loaded query to memory')

    user_first_deposits_df = pd.read_gbq(query=user_first_deposits_query)

    print('loaded dataframe')
    # make sure the output meets our tests
    print('testing output')
    # test 1: the number of rows should match the number of distinct user_address
    # ie. there should be one-to-one relationship between user_address and rows in this table

    assert user_first_deposits_df.user_address.nunique() == user_first_deposits_df.shape[0], \
        f'{user_first_deposits_df.user_address.nunique()} unique user_address does not equal {user_first_deposits_df.shape[0]} rows'
    
    # reformat the output so it's ready for BigQuery ingest
    user_first_deposits_df.first_deposit_date = user_first_deposits_df.first_deposit_date.apply(date.isoformat)

    # write it to json
    print('writing dataframe to gcs')
    user_first_deposits_df.to_json('gs://friktion-users-prod/user-first-deposits.json', orient='records', date_format='iso', lines=True)

    # load it bigquery
    print('loading to bigquery')
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Set table_id to the ID of the table to create.
    table_id = "lyrical-amulet-337502.users.fact_user_first_deposit"

    #configure the job - schema and source format
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("user_address", "STRING"),
            bigquery.SchemaField("first_deposit_date", "DATE"),
            bigquery.SchemaField("first_deposit_epoch", "INT64", "REPEATED"),
            bigquery.SchemaField("first_deposit_token", "STRING", "REPEATED"),
            bigquery.SchemaField("first_deposit_amount", "FLOAT64"),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    uri = "gs://friktion-users-prod/user-first-deposits.json"

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        location="US",  # Must match the destination dataset location.
        job_config=job_config,
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

def write_user_first_withdrawal_table():
    # create the user first withdrawal table

    user_first_withdrawals_query = None

    import os
    fname = 'user_first_withdrawal.sql'
    this_file = os.path.abspath(__file__)
    this_dir = os.path.dirname(this_file)
    wanted_file = os.path.join(this_dir, fname)
    
    print(f'opening {wanted_file}')
    
    with open(wanted_file, 'r') as f:
        user_first_withdrawals_query = f.read()
    
    print('loaded query to memory')

    user_first_withdrawals_df = pd.read_gbq(query=user_first_withdrawals_query)

    print('loaded dataframe')
    # make sure the output meets our tests
    print('testing output')
    # test 1: the number of rows should match the number of distinct user_address
    # ie. there should be one-to-one relationship between user_address and rows in this table

    assert user_first_withdrawals_df.user_address.nunique() == user_first_withdrawals_df.shape[0], \
        f'{user_first_withdrawals_df.user_address.nunique()} unique user_address does not equal {user_first_withdrawals_df.shape[0]} rows'
    
    # reformat the output so it's ready for BigQuery ingest
    user_first_withdrawals_df.first_withdrawal_date = user_first_withdrawals_df.first_withdrawal_date.apply(date.isoformat)

    # write it to json
    print('writing dataframe to gcs')
    user_first_withdrawals_df.to_json('gs://friktion-users-prod/user-first-withdrawals.json', orient='records', date_format='iso', lines=True)

    # load it bigquery
    print('loading to bigquery')
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Set table_id to the ID of the table to create.
    table_id = "lyrical-amulet-337502.users.fact_user_first_withdrawal"

    #configure the job - schema and source format
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("user_address", "STRING"),
            bigquery.SchemaField("first_withdrawal_date", "DATE"),
            bigquery.SchemaField("first_withdrawal_epoch", "INT64", "REPEATED"),
            bigquery.SchemaField("first_withdrawal_token", "STRING", "REPEATED"),
            bigquery.SchemaField("first_withdrawal_amount", "FLOAT64"),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    uri = "gs://friktion-users-prod/user-first-withdrawals.json"

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        location="US",  # Must match the destination dataset location.
        job_config=job_config,
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

def write_user_firsts_table():
    # create the user first table

    user_firsts_query = None

    import os
    fname = 'user_firsts_table.sql'
    this_file = os.path.abspath(__file__)
    this_dir = os.path.dirname(this_file)
    wanted_file = os.path.join(this_dir, fname)
    
    print(f'opening {wanted_file}')
    
    with open(wanted_file, 'r') as f:
        user_firsts_query = f.read()
    
    print('loaded query to memory')

    user_firsts_df = pd.read_gbq(query=user_firsts_query)

    print('loaded dataframe')
    # make sure the output meets our tests
    print('testing output')

    # test 1: the number of rows should match the number of distinct user_address
    # ie. there should be one-to-one relationship between user_address and rows in this table

    assert user_firsts_df.user_address.nunique() == user_firsts_df.shape[0], \
        f'{user_firsts_df.user_address.nunique()} unique user_address does not equal {user_firsts_df.shape[0]} rows'
    
    # reformat the output so it's ready for BigQuery ingest
    print('formatting dates')
    user_firsts_df.first_deposit_date = user_firsts_df.first_deposit_date.apply(date.isoformat)
    user_firsts_df.first_withdrawal_date = user_firsts_df.first_withdrawal_date.apply(date.isoformat)

    # write it to json
    print('writing dataframe to gcs')
    user_firsts_df.to_json('gs://friktion-users-prod/tables/user-firsts.json', orient='records', date_format='iso', lines=True)
    # load it bigquery
    print('loading to bigquery')
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Set table_id to the ID of the table to create.
    table_id = "lyrical-amulet-337502.users.fact_user_firsts"

    #configure the job - schema and source format
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("user_address", "STRING"),
            bigquery.SchemaField("first_deposit_date", "DATE"),
            bigquery.SchemaField("first_deposit_epoch", "INT64", "REPEATED"),
            bigquery.SchemaField("first_deposit_token", "STRING", "REPEATED"),
            bigquery.SchemaField("first_deposit_amount", "FLOAT64"),
            bigquery.SchemaField("first_withdrawal_date", "DATE"),
            bigquery.SchemaField("first_withdrawal_epoch", "INT64", "REPEATED"),
            bigquery.SchemaField("first_withdrawal_token", "STRING", "REPEATED"),
            bigquery.SchemaField("first_withdrawal_amount", "FLOAT64"),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    uri = "gs://friktion-users-prod/tables/user-firsts.json"

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        location="US",  # Must match the destination dataset location.
        job_config=job_config,
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

def divide_and_conquer():
    user_first_deposits_df = pd.read_json(f'gs://friktion-users-prod/tables/user_first_deposits.json', orient='records', lines=True, convert_dates=['first_deposit_date'])

    total_rows = user_first_deposits_df.shape[0]

    iters = user_first_deposits_df.shape[0] // 500

    for iter in range(iters):
        for iter in range(user_first_deposits_df.shape[0] // 500):
            start = (iter*500)
            if start < 12000:
                finish = start+500
            else:
                finish = None
            
        user_first_deposits_df.iloc[start:finish,:].to_json(f'user_first_deposits_part_{iter}.json',\
                orient='records', date_format='iso', lines=True, date_unit='s')
    