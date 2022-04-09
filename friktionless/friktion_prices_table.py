from google.cloud import bigquery
import json
import pandas as pd
from datetime import datetime, timezone

def create_friktion_prices_table(friktion_gcloud_project):
    '''
    Create a price table in order to look at the value of tokens in USD terms in the supplied google cloud project.

    Example
    ----------
    import friktionless as fless
    fless.friktion_prices_table.create_friktion_prices_table('some_project_name')

    '''
    # Construct a BigQuery client object.
    client = bigquery.Client()


    # Read in JSON mapper for assets to their respective GitHub file locations
    file = open('asset_prices.json')
    asset_prices_mapper = json.load(file)


    # Create DataFrame from asset prices mapper
    df = pd.DataFrame(asset_prices_mapper)


    # Delete existing prices table
    print('Creating BigQuery Client...')
    client.query('drop table if exists analytics.prices', project=friktion_gcloud_project)


    # Loop through assets to recreate price table from the latest dataset in GitHub
    for index, value in df.iterrows():
    
        prices = pd.read_json(value[1])
        prices.rename(columns={0: 'timestamp_ms', 1: 'price_usd'},inplace=True)
        prices['asset'] = value[0]
        prices['timestamp_s'] = prices['timestamp_ms'] / 1000
        prices['timestamp'] = prices['timestamp_s'].apply(lambda x: datetime.fromtimestamp(x, timezone.utc))
        prices['price_usd'] = prices['price_usd'].apply(lambda x: float(x))
        
        final_df = prices[['asset','timestamp','price_usd']]
        
        print(datetime.now(),'Writing {} price data to BigQuery'.format(value[0]))
        final_df.to_gbq('analytics.prices',project_id='lyrical-amulet-337502',if_exists='append')
