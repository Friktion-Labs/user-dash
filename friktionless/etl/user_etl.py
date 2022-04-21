import pandas as pd
from google.cloud import bigquery

def write_user_first_deposit_table():
    # create the user first deposit table

    user_first_deposits_query = None

    with open('user_first_deposit.sql', 'r') as f:
        user_first_deposits_query = f.read()

    user_first_deposits_df = pd.read_gbq(query=user_first_deposits_query)

    # make sure the output meets our tests

    # test 1: the number of rows should match the number of distinct user_address
    # ie. there should be one-to-one relationship between user_address and rows in this table

    assert user_first_deposits_df.user_address.nunique() == user_first_deposits_df.shape[0], \
        f'{user_first_deposits_df.user_address.nunique()} unique user_address does not equal {user_first_deposits_df.shape[0]} rows'

    # write it to json

    user_first_deposits_df.to_json('gs://friktion-users-prod/user-first-deposits.json', orient='records', date_format='iso', lines=True)

    # load it bigquery
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