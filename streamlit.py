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

    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        strategy = st.multiselect('Strategy:', list(pd.read_gbq(query=open_file('friktionless/queries/distinct_strategies.sql'), project_id='friktion-dev')['strategy']),default='All')
    with col2:
        volt_number = st.multiselect('Volt Number:', list(pd.read_gbq(query=open_file('friktionless/queries/distinct_volt_numbers.sql'), project_id='friktion-dev')['volt_number']),default='All')
    with col3:
        asset = st.multiselect('Asset:', list(pd.read_gbq(query=open_file('friktionless/queries/distinct_assets.sql'), project_id='friktion-dev')['asset']),default='All')
    with col4:
        voltage = st.multiselect('Voltage:', list(pd.read_gbq(query=open_file('friktionless/queries/distinct_voltages.sql'), project_id='friktion-dev')['voltage']),default='All')

    st.markdown('#')
    st.altair_chart(
        fless.analytics.charts.create_cumulative_users_chart('friktion-dev',strategy,volt_number,asset,voltage),
        use_container_width=True
    )

# Net Funds Flow Container
with st.container():
    st.header('Net Funds Flow')
    product_name = st.selectbox('Product Name:', list(pd.read_gbq(query=open_file('friktionless/queries/distinct_product_names.sql'), project_id='friktion-dev')['product_name']))
    
    st.markdown('#')
    st.altair_chart(
        fless.analytics.charts.create_net_funds_flow_chart('friktion-dev',product_name),
        use_container_width=True
    )