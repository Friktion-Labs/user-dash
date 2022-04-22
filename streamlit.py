import friktionless as fless
import streamlit as st
import pandas as pd


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

    st.markdown('#')
    st.altair_chart(
        fless.analytics.charts.create_cumulative_users_chart('lyrical-amulet-337502',strategy,volt_number,asset,voltage),
        use_container_width=True
    )
    st.markdown('#')

# Average Deposit Amount and Withdrawal Amount by Underlying Asset Container
with st.container():
    st.header('Average Deposit and Withdrawal Amounts')
    volt_number_select = st.selectbox(
        'Volt Number:',
        list(pd.read_parquet('gs://friktion-volt-numbers-prod/volt-numbers.parquet')['volt_number']),
        0
    )
    start_epoch, end_epoch = st.select_slider(
        'Epoch Range:',
        options=list(pd.read_parquet('gs://friktion-epochs-prod/epochs.parquet')['epochs']),
        value=(
            list(pd.read_parquet('gs://friktion-epochs-prod/epochs.parquet')['epochs'])[0],
            list(pd.read_parquet('gs://friktion-epochs-prod/epochs.parquet')['epochs'])[-1]
            )
        )

    st.markdown('#')
    col4, col5 = st.columns(2)

    with col4:
        st.altair_chart(
            fless.analytics.charts.create_avg_deposit_by_underlying_asset_chart('lyrical-amulet-337502', volt_number_select, start_epoch, end_epoch),
            use_container_width=True
        )

    with col5:
        st.altair_chart(
            fless.analytics.charts.create_avg_withdrawal_by_underlying_asset_chart('lyrical-amulet-337502', volt_number_select, start_epoch, end_epoch),
            use_container_width=True
        )
    st.markdown('#')

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