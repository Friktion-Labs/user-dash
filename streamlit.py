import friktionless as fless
import streamlit as st
import pandas as pd

### UTILITY FUNCTION - Needs to be put into friktionless ###
def open_file(path):
    with open (path) as query:
        query_string = query.read()
    
    return query_string

st.set_page_config(
    page_title='Friktion Analytics Hub',
    layout='wide',
    initial_sidebar_state='collapsed'
    )

st.title('Friktion Analytics Hub')

# Cumulative and New Users Container
with st.container():
    st.header('Cumulative and New Users')

    col1, col2, col3 = st.columns(3)
    
    with col1:
        strategy = st.multiselect(
            'Strategy:', 
            list(pd.read_parquet('gs://friktion-strategies-prod/strategies.parquet')['strategy']),
            list(pd.read_parquet('gs://friktion-strategies-prod/strategies.parquet')['strategy']))
    with col2:
        volt_number = st.multiselect(
            'Volt Number:', 
            list(pd.read_parquet('gs://friktion-volt-numbers-prod/volt-numbers.parquet')['volt_number']),
            list(pd.read_parquet('gs://friktion-volt-numbers-prod/volt-numbers.parquet')['volt_number']))
    with col3:
        voltage = st.multiselect(
            'Voltage:', 
            list(pd.read_parquet('gs://friktion-voltages-prod/voltages.parquet')['voltage']),
            list(pd.read_parquet('gs://friktion-voltages-prod/voltages.parquet')['voltage']))

    asset = st.multiselect(
        'Asset:', 
        list(pd.read_parquet('gs://friktion-assets-prod/assets.parquet')['asset']),
        list(pd.read_parquet('gs://friktion-assets-prod/assets.parquet')['asset']))

    #st.markdown('#')
    st.altair_chart(
        fless.analytics.charts.create_cumulative_users_chart('lyrical-amulet-337502',strategy,volt_number,asset,voltage),
        use_container_width=True
    )

# Net Funds Flow Container
with st.container():
    st.header('Net Funds Flow')
    product_name = st.selectbox(
        'Product Name:', 
        list(pd.read_parquet('gs://friktion-products-prod/products.parquet')['product_name'])
        )
    
    st.markdown('#')
    st.altair_chart(
        fless.analytics.charts.create_net_funds_flow_chart('lyrical-amulet-337502',product_name),
        use_container_width=True
    )