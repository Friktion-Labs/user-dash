import friktionless as fless
import streamlit as st
import pandas as pd

st.set_page_config(
    page_title='Friktion Analytics Hub',
    layout='wide',
    initial_sidebar_state='collapsed'
    )

st.title('Friktion Analytics Hub')

with st.container():
    st.header('Net Funds Flow')
    with open ('friktionless/queries/distinct_product_names.sql') as query_product_names:
        query_string_product_names = query_product_names.read()
        
    product_name = st.selectbox('Product Name:', list(pd.read_gbq(query=query_string_product_names, project_id='friktion-dev')['product_name']))
    
    st.markdown('#')
    st.altair_chart(
        fless.analytics.charts.create_net_funds_flow_chart('friktion-dev',product_name),
        use_container_width=True
    )