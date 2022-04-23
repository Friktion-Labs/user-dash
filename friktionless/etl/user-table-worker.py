#! /opt/conda/bin/python

'''
USER TABLE WORKER

The user table worker is script designed to be executed at the command line to enable
fan-out processing of transaction data into individual user table and then write those
to file output in Google Cloud Storage.

You need to run the divide_and_conquer function in the user_etl.py file and etl api before
you use the workers. They are consuming the numbered output files of divide_and_conquer.

Example:
$ python
    > import friktionless
    > friktionless.etl.divide_and_conquer()
$ ./user-dash/friktionless/etl/user-table-worker.py 0 & ./user-dash/friktionless/etl/user-table-worker.py 1 ...

'''

import pandas as pd
from datetime import date
from tqdm import tqdm

def collect_and_write_user_table(user_address, first_deposit_date, max_date, min_date):
    '''
    execute the user_table_script and write the outcome to Google Cloud Storage in a JSON
    '''
    
    
    # TODO: So much of this table creation code is boiler plate. See copy-paste source 
    # in user_table_etl.py. Need to refactor. WET Code
    from google.cloud import bigquery

    _user_table_script_query = None

    import os
    fname = 'user_table_script.sql'
    this_file = os.path.abspath(__file__)
    this_dir = os.path.dirname(this_file)
    wanted_file = os.path.join(this_dir, fname)
    
    with open(wanted_file, 'r') as f:
        _user_table_script_query = f.read()
        
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Run a SQL script.
    sql_script = _user_table_script_query
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_address", "STRING", user_address),
            bigquery.ScalarQueryParameter("first_deposit_dt", "DATE", first_deposit_date),
            bigquery.ScalarQueryParameter("max_date", "DATE", max_date)
        ]
    )

    parent_job = client.query(sql_script, job_config=job_config)

    # Wait for the whole script to finish.
    rows_iterable = parent_job.result()
    # TODO: Convert to logging
    # TODO: Add cloud monitoring and cloud logging to etl pipelines
    # print("Script created {} child jobs.".format(parent_job.num_child_jobs))

    # Fetch result rows for the final sub-job in the script.
    rows = list(rows_iterable)
    
    min_date = min_date.date()
    max_date = date.fromisoformat(max_date)
        
    print_true = (len(rows) > abs((min_date - max_date).days)+1)
    
    if print_true:
        print(
            f'{len(rows)} rows of user data were collected for user {user_address}'
            )


    # Fetch jobs created by the SQL script.
    child_jobs_iterable = client.list_jobs(parent_job=parent_job)
    for child_job in child_jobs_iterable:
        child_rows = list(child_job.result())
        # print(
        #     "Child job with ID {} produced {} row(s).".format(
        #         child_job.job_id, len(child_rows)
        #     )
        # )
        
    user_df = None
    for job in client.list_jobs(parent_job=parent_job):
        user_df = client.get_job(job.job_id).to_dataframe().sort_values('as_of_date')
        break
    
    # reformat the output so it's ready for BigQuery ingest
    print('formatting dates')
    user_df.as_of_date = user_df.as_of_date.apply(date.isoformat)
    user_df.first_deposit_date = user_df.first_deposit_date.apply(date.isoformat)
    user_df.first_withdrawal_date = user_df.first_withdrawal_date.apply(date.isoformat)
    user_df.last_withdrawal_date = user_df.last_withdrawal_date.apply(date.isoformat)
    user_df.last_deposit_date = user_df.last_deposit_date.apply(date.isoformat)
    user_df.churn_date = user_df.churn_date.apply(date.isoformat)
    
    user_df.to_json(f'gcs://friktion-users-prod/user-{user_address}.json', orient='records', date_format='iso', lines=True, date_unit='s')
    if print_true:
        print(f'Written to gcs://friktion-users-prod/user-{user_address}.json')

if __name__ == '__main__':
    import sys

    split = sys.argv[1]
    
    print(split)

    user_first_deposits_df = pd.read_json(f'user_first_deposits_part_{split}.json', orient='records', lines=True, convert_dates=['first_deposit_date'])

    min_date = user_first_deposits_df.first_deposit_date.min()
    max_date = pd.read_json('gs://friktion-users-prod/tables/max_date.json', typ='Series').iloc[0].date().isoformat()

    for row in tqdm(user_first_deposits_df.itertuples()):
        collect_and_write_user_table(row.user_address, row.first_deposit_date.date(), max_date, min_date)