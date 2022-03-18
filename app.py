"""
2022
For Friktion Labs
Done by: MordantBlack (enfamil#3658)
"""

from flask import Flask
import dash
from dash import html
from dash import dcc
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from dash.dependencies import Input, Output, State
import visdcc
from datetime import datetime
import time




#---------VOLT 01 DATA -------------

# Load volt01 unique user data
v1_uu = pd.read_csv('data/volt01_uu.csv', index_col=0, parse_dates=True)
# Get latest total uu of each asset
v1_uu_sum = v1_uu.sort_values('date').groupby('asset').tail(1)
v1_uu_sum = v1_uu_sum['count'].sum()
# Date to Index
v1_uu.index = pd.to_datetime(v1_uu['date'], format = '%Y-%m-%d') # indicate date format from csv
v1_uu.drop_duplicates(keep='first',inplace=True)



# Load volt01 avg transaction size data
v1_ts = pd.read_csv('data/volt01_ts.csv', index_col=0)
v1_ts = v1_ts.fillna(0)
v1_ts = v1_ts.sort_values("asset")
# For Avg tx size
total_v1_size = (v1_ts['count']*v1_ts['tx_size']).sum()
total_v1_count = v1_ts['count'].sum()


# Load volt01 withdrawal data
v1_wd = pd.read_csv('data/volt01_wd.csv', index_col=0)
#v1_wd['date'] = pd.to_datetime(v1_wd['date'], format = '%Y-%m-%d')
v1_wd = v1_wd.sort_values("asset")




#---------VOLT 02 DATA -------------

# Load volt02 unique user data
v2_uu = pd.read_csv('data/volt02_uu.csv', index_col=0, parse_dates=True)
# Get latest total uu of each asset
v2_uu_sum = v2_uu.sort_values('date').groupby('asset').tail(1)
v2_uu_sum = v2_uu_sum['count'].sum()
# Date to Index
v2_uu.index = pd.to_datetime(v2_uu['date'], format = '%Y-%m-%d') # indicate date format from csv
v2_uu.drop_duplicates(keep='first',inplace=True)


# Load volt02 avg transaction size data
v2_ts = pd.read_csv('data/volt02_ts.csv', index_col=0)
v2_ts = v2_ts.fillna(0)
v2_ts = v2_ts.sort_values("asset")
# For Avg tx size
total_v2_size = (v2_ts['count']*v2_ts['tx_size']).sum()
total_v2_count = v2_ts['count'].sum()


# Load volt02 withdrawal data
v2_wd = pd.read_csv('data/volt02_wd.csv', index_col=0)
#v2_wd['date'] = pd.to_datetime(v2_wd['date'], format = '%Y-%m-%d')
v2_wd = v2_wd.sort_values("asset")

# total uu
total_uu = v1_uu_sum + v2_uu_sum
# total avg tx size
total_avg = (total_v1_size + total_v2_size) / (total_v1_count + total_v2_count)


#---------INITIALISE DASH -------------

# external JavaScript files
external_scripts = ['https://code.jquery.com/jquery-3.3.1.min.js']

# Initialize the app
# server = Flask(__name__)
app = dash.Dash(__name__, external_scripts = external_scripts)
server = app.server
app.config.suppress_callback_exceptions = True



# options for the dropdown
def get_options(list_assets):
    dict_list = []
    for i in list_assets:
        dict_list.append({'label': i, 'value': i})

    return dict_list



app.layout = html.Div(
    children=[

        dcc.Store(id='dropdown-cache', data='initial value'),

        html.Div(className='row',
                children=[
                    html.Div(className='four columns div-user-controls',
                            children=[
                                html.H1(total_uu, className='Count'),
                                # jQuery
                                visdcc.Run_js(id = 'counter-users', run = "$('.Count').each(function () {$(this).prop('Counter',0).animate({Counter: $(this).text()},{duration: 1000,easing: 'swing',step: function (now) {$(this).text((Math.ceil(now).toLocaleString('en')));}});});", event = 'null'),
                                html.H2('FRIKTION VOLT TOTAL UNIQUE USERS'),
                                html.P('Time Series of Unique Users since launch (Cumulative).'),
                                html.P('Select Volt & pick one or more asset from the dropdown.'),
                                html.Div([dcc.Tabs(parent_className='custom-tabs',
                                                    className='div-for-dropdown custom-tabs-container', 
                                                    children=[
                                                        dcc.Tab(label='Volt 01',
                                                                value='tab-1',
                                                                style={'backgroundColor': '#1E1E1E'},
                                                                className='custom-tab',
                                                                selected_className='custom-tab--selected',
                                                                children=[
                                                                    dcc.Dropdown(id='volt01_uu_assetselector', options=get_options(v1_uu['asset'].unique()),
                                                                        multi=True, value=v1_uu['asset'].unique().tolist(), # .sort_values()[0]
                                                                        style={'backgroundColor': '#1E1E1E'},
                                                                        className='assetselector'
                                                                        ),
                                                                        ]
                                                                ),
                                                        dcc.Tab(label='Volt 02',
                                                                value='tab-2',
                                                                style={'backgroundColor': '#1E1E1E'},
                                                                className='custom-tab',
                                                                selected_className='custom-tab--selected',
                                                                children=[
                                                                    dcc.Dropdown(id='volt02_uu_assetselector', options=get_options(v2_uu['asset'].unique()),
                                                                        multi=True, value=v2_uu['asset'].unique().tolist(), # [.sort_values()[0]]
                                                                        style={'backgroundColor': '#1E1E1E'},
                                                                        className='assetselector'
                                                                        ),
                                                                        ]
                                                                )
                                                        ],
                                                style={'color': '#1E1E1E'},
                                                id='tabs', 
                                                value='tab-1'),
                                        ]),
                            ]),
                    html.Div(dcc.Loading(id = "loading-icon", 
                                        children=[
                                            dcc.Graph(id='volt_uu_timeseries', config={'displayModeBar': False}, animate=True),
                                        ],
                                        type="default"),
                            className='eight columns div-for-charts bg-grey',
                            )
                        ]),

# new section (Volt01 avg tx size)

        dcc.Store(id='ts_assets'),

        html.Div(className='row',
                 children=[
                    html.Div(className='four columns div-user-controls',
                            children=[
                                html.H1(total_avg, className='Value', id='avg-dollar'),
                                # jQuery
                                visdcc.Run_js(id = 'counter-value', run = "$('.Value').each(function () {$(this).prop('Counter',0).animate({Counter: $(this).text()},{duration: 1000,easing: 'swing',step: function (now) {$(this).text((Math.ceil(now).toLocaleString('en')));}});});", event = 'null'),
                                html.H2('AVERAGE TRANSACTION SIZE'),
                                html.P('Bubble Chart for Avg. Transaction Size (USD)'),
                                html.P('Select Volt:'),
                                html.Div([dcc.Tabs(parent_className='custom-tabs',
                                                    className='div-for-dropdown custom-tabs-container', 
                                                    children=[
                                                        dcc.Tab(label='Volt 01',
                                                                value='tab_ts-1',
                                                                style={'backgroundColor': '#1E1E1E'},
                                                                className='custom-tab',
                                                                selected_className='custom-tab--selected',
                                                                # children=[
                                                                #     dcc.Dropdown(id='volt01_uu_assetselector', options=get_options(v1_uu['asset'].unique()),
                                                                #         multi=True, value=v1_uu['asset'].unique().tolist(), # .sort_values()[0]
                                                                #         style={'backgroundColor': '#1E1E1E'},
                                                                #         className='assetselector'
                                                                #         ),
                                                                #         ]
                                                                ),
                                                        dcc.Tab(label='Volt 02',
                                                                value='tab_ts-2',
                                                                style={'backgroundColor': '#1E1E1E'},
                                                                className='custom-tab',
                                                                selected_className='custom-tab--selected',
                                                                # children=[
                                                                #     dcc.Dropdown(id='volt02_uu_assetselector', options=get_options(v2_uu['asset'].unique()),
                                                                #         multi=True, value=v2_uu['asset'].unique().tolist(), # [.sort_values()[0]]
                                                                #         style={'backgroundColor': '#1E1E1E'},
                                                                #         className='assetselector'
                                                                #         ),
                                                                #         ]
                                                                )
                                                        ],
                                                style={'color': '#1E1E1E'},
                                                id='tabs_ts', 
                                                value='tab_ts-1'),
                                        ])
                            ]),
                    html.Div(dcc.Loading(id = "loading-icon_ts", 
                                        children=[
                                            dcc.Graph(id='volt_ts_bubblechart', config={'displayModeBar': False}, animate=True),
                                        ],
                                        type="default"),
                            className='eight columns div-for-charts bg-grey',
                            )
                        ]),


        
        dcc.Store(id='dropdown-cache-wk'),
        dcc.Store(id='wd_assets'),

        html.Div(className='row',
                children=[
                    html.Div(className='four columns div-user-controls',
                            children=[
                                # html.H1('1000', className='Value', id='wd-dollar'),
                                html.H2('FRIKTION VOLT WITHDRAWALS BY WEEK'),
                                html.P('Bar Chart for individual asset withdrawal size'),
                                html.P('Select Week using the dropdown below.'),
                                html.Div([dcc.Tabs(parent_className='custom-tabs',
                                                    className='div-for-dropdown custom-tabs-container', 
                                                    children=[
                                                        dcc.Tab(label='Volt 01',
                                                                value='tab_wd-1',
                                                                style={'backgroundColor': '#1E1E1E'},
                                                                className='custom-tab',
                                                                selected_className='custom-tab--selected',
                                                                children=[
                                                                    dcc.Dropdown(id='volt01_wd_wkselector', options=[{'label': i, 'value': i} for i in sorted(v1_wd['epoch'].unique())],
                                                                        multi=False, value=v1_wd['epoch'][0], # get_options(v1_wd['epoch'].unique())
                                                                        style={'backgroundColor': '#1E1E1E'},
                                                                        className='assetselector'
                                                                        ),
                                                                        ]
                                                                ),
                                                        dcc.Tab(label='Volt 02',
                                                                value='tab_wd-2',
                                                                style={'backgroundColor': '#1E1E1E'},
                                                                className='custom-tab',
                                                                selected_className='custom-tab--selected',
                                                                children=[
                                                                    dcc.Dropdown(id='volt02_wd_wkselector', options=[{'label': i, 'value': i} for i in sorted(v2_wd['epoch'].unique())],
                                                                        multi=False, value=v2_wd['epoch'][0], # [.sort_values()[0]]
                                                                        style={'backgroundColor': '#1E1E1E'},
                                                                        className='assetselector'
                                                                        ),
                                                                        ]
                                                                )
                                                        ],
                                                style={'color': '#1E1E1E'},
                                                id='tabs_wd', 
                                                value='tab_wd-1'),
                                        ])
                            ]),
                    html.Div(dcc.Loading(id = "loading-icon_wd", 
                                        children=[
                                            dcc.Graph(id='volt_wd_barchart', config={'displayModeBar': False}, animate=True),
                                        ],
                                        type="default"),
                            className='eight columns div-for-charts bg-grey',
                            )
                ])
])


#-------------------- CALLBACKS --------------------------------------------------------------

# Callback for Volt01 / Volt02 unique users dropdown box
@app.callback(Output('dropdown-cache','data'),
                [Input('volt01_uu_assetselector','value'),
                Input('volt02_uu_assetselector','value')],
                [Input('tabs','value')])
def store_dropdown_cache(volt01_dropdown_sel, volt02_dropdown_sel, tab):
    if tab == 'tab-1':
        return volt01_dropdown_sel
    elif tab == 'tab-2':
        return volt02_dropdown_sel



# @app.callback(Output('volt01_uu_assetselector', 'value'),
#               [Input('tabs', 'value')],
#               [State('dropdown-cache', 'data')])
# def synchronize_dropdowns(_, cache):
#     return cache

# @app.callback(Output('volt02_uu_assetselector', 'value'),
#               [Input('tabs', 'value')],
#               [State('dropdown-cache', 'data')])
# def synchronize_dropdowns(_, cache):
#     return cache


# Callback for Tx size x-axis labels
@app.callback(Output('ts_assets','data'),
                [Input('tabs_ts','value')])
def store_ts_assets(tabs_ts):
    if tabs_ts == 'tab_ts-1':
        return v1_ts['asset']
    elif tabs_ts == 'tab_ts-2':
        return v2_ts['asset']


# Callback for Volt01 / Volt02 withdrawals dropdown box
@app.callback(Output('dropdown-cache-wk','data'),
                [Input('volt01_wd_wkselector','value'),
                Input('volt02_wd_wkselector','value')],
                [Input('tabs_wd','value')])
def store_dropdown_cache_wk(volt01_wd_wkselector, volt02_wd_wkselector, tabs_wd):
    if tabs_wd == 'tab_wd-1':
        return volt01_wd_wkselector
    elif tabs_wd == 'tab_wd-2':
        return volt02_wd_wkselector


# Callback for Volt Withdrawal x-axis labels
@app.callback(Output('wd_assets','data'),
                [Input('tabs_wd','value')])
def store_wd_assets(tabs_wd):
    if tabs_wd == 'tab_wd-1':
        return v1_wd['asset']
    elif tabs_wd == 'tab_wd-2':
        return v2_wd['asset']


#------------------------- GRAPHS ------------------------------------------------------------------

#------------------------- Callback for Volt unique users timeseries -------------------------------

@app.callback(Output('volt_uu_timeseries', 'figure'),
                [Input('dropdown-cache','data')],
                [Input('tabs','value')])
def update_graph(dropdown_sel, tab):

    if tab == 'tab-1':
        trace1 = []
        v1_uu_sub = v1_uu
        for asset in dropdown_sel:
            trace1.append(go.Scatter(x=v1_uu_sub[v1_uu_sub['asset'] == asset].index,
                                     y=v1_uu_sub[v1_uu_sub['asset'] == asset]['count'],
                                     mode='lines',
                                     opacity=0.7,
                                     name=asset,
                                     textposition='bottom center'))

        traces = [trace1]
        data = [val for sublist in traces for val in sublist]
        figure = {'data': data,
                  'layout': go.Layout(
                      colorway=["#b97af5", '#FF4F00', '#375CB1', '#FF7400', '#BBFD55', '#FFF400', '#FF0056', '#11E02C', '#F11EED', '#56E6FD'],
                      template='plotly_dark',
                      paper_bgcolor='rgba(0, 0, 0, 0)',
                      plot_bgcolor='rgba(0, 0, 0, 0)',
                      margin={'b': 15},
                      hovermode='x',
                      autosize=True,
                      xaxis={'autorange': True},
                      title={'text': 'Volt #01 Unique Users', 'font': {'color': 'white'}, 'x': 0.5},
                      transition={
                            'duration': 500,
                        }
                    ),
                  }

    elif tab == 'tab-2':
        trace2 = []
        v2_uu_sub = v2_uu
        for asset in dropdown_sel:
            trace2.append(go.Scatter(x=v2_uu_sub[v2_uu_sub['asset'] == asset].index,
                                     y=v2_uu_sub[v2_uu_sub['asset'] == asset]['count'],
                                     mode='lines',
                                     opacity=0.7,
                                     name=asset,
                                     textposition='bottom center'))

        traces = [trace2]
        data = [val for sublist in traces for val in sublist]
        figure = {'data': data,
                  'layout': go.Layout(
                      colorway=["#b97af5", '#FF4F00', '#375CB1', '#FF7400', '#BBFD55', '#FFF400', '#FF0056', '#11E02C', '#F11EED', '#56E6FD'],
                      template='plotly_dark',
                      paper_bgcolor='rgba(0, 0, 0, 0)',
                      plot_bgcolor='rgba(0, 0, 0, 0)',
                      margin={'b': 15},
                      hovermode='x',
                      autosize=True,
                      xaxis={'autorange': True},
                      title={'text': 'Volt #02 Unique Users', 'font': {'color': 'white'}, 'x': 0.5},
                      transition={
                            'duration': 500,
                        }
                  ),
                  }
    return figure


#------------------- Callback for Volt tx size bubble chart -------------------------------------

@app.callback(Output('volt_ts_bubblechart', 'figure'),
              [Input('ts_assets','data')],
              [Input('tabs_ts', 'value')])
def update_graph(ts_assets, tab_ts):

    if tab_ts == 'tab_ts-1':

        # relative bubble to fit graph 
        sizeref = 2.*max(v1_ts['tx_size'])/(100**2)

        # text for each bubble when hover over
        v1_ts_txt = v1_ts.drop(columns=['amount','usd'])
        v1_ts_txt = v1_ts_txt[['asset','count','tx_size']]
        v1_ts_txt = v1_ts_txt.T
        hover_text = []
        for i in range(v1_ts_txt.shape[1]):
            hover_text.append(v1_ts_txt[i][0] + "<br>" + "Unique Users: " + str(v1_ts_txt[i][1]) + "<br>" + "Avg. Tx Size: $" + str(round(v1_ts_txt[i][2],2)))

        data = go.Scatter(
            name='Tx Size',
            x=ts_assets,
            y=v1_ts['count'],
            text=hover_text,
            opacity=1,
            mode='markers',
            marker=dict(color = v1_ts['tx_size'],
                        size = v1_ts['tx_size'],
                        showscale=True,
                        sizemode = 'area',
                        sizeref=sizeref, line_width=2)
            )

        figure = {'data': [data],
                  'layout': go.Layout(
                      colorway=['#b97af5', '#FF4F00', '#375CB1', '#FF7400', '#BBFD55', '#FFF400', '#FF0056', '#11E02C', '#F11EED', '#56E6FD'],
                      template='plotly_dark',
                      paper_bgcolor='rgba(0, 0, 0, 0)',
                      plot_bgcolor='rgba(0, 0, 0, 0)',
                      margin={'b': 15},
                      hovermode='x',
                      autosize=True,
                      yaxis={'title': 'No. of Unique Users','range': [v1_ts['count'].min()-100, v1_ts['count'].max()+100], 'categoryorder': 'total ascending'},
                      title={'text': 'Volt #01 Average Tx Size (USD)', 'font': {'color': 'white'}, 'x': 0.5},
                      transition={
                            'duration': 500,
                        }
                  )
                  }


    elif tab_ts == 'tab_ts-2':

        # relative bubble to fit graph 
        sizeref = 2.*max(v2_ts['tx_size'])/(100**2)

        # text for each bubble when hover over
        v2_ts_txt = v2_ts.drop(columns=['amount'])
        v2_ts_txt = v2_ts_txt[['asset','count','tx_size']]
        v2_ts_txt = v2_ts_txt.T
        hover_text = []
        for i in range(v2_ts_txt.shape[1]):
            hover_text.append(v2_ts_txt[i][0] + "<br>" + "Unique Users: " + str(v2_ts_txt[i][1]) + "<br>" + "Avg. Tx Size: $" + str(round(v2_ts_txt[i][2],2)))

        data = go.Scatter(
            name='Tx Size',
            x=ts_assets,
            y=v2_ts['count'],
            text=hover_text,
            opacity=1,
            mode='markers',
            marker=dict(color = v2_ts['tx_size'],
                        size = v2_ts['tx_size'],
                        showscale=True,
                        sizemode = 'area',
                        sizeref=sizeref, line_width=2)
            )

        figure = {'data': [data],
                  'layout': go.Layout(
                      colorway=['#b97af5', '#FF4F00', '#375CB1', '#FF7400', '#BBFD55', '#FFF400', '#FF0056', '#11E02C', '#F11EED', '#56E6FD'],
                      template='plotly_dark',
                      paper_bgcolor='rgba(0, 0, 0, 0)',
                      plot_bgcolor='rgba(0, 0, 0, 0)',
                      margin={'b': 15},
                      hovermode='x',
                      autosize=True,
                      yaxis={'title': 'No. of Unique Users','range': [v2_ts['count'].min()-100, v2_ts['count'].max()+ 100], 'categoryorder': 'total ascending'},
                      title={'text': 'Volt #02 Average Tx Size (USD)', 'font': {'color': 'white'}, 'x': 0.5},
                      transition={
                            'duration': 500,
                        }
                  )
                  }


    return figure


#--------------------- Callback for Volt withdrawal size bar chart ---------------------------------

@app.callback(Output('volt_wd_barchart', 'figure'),
                [Input('dropdown-cache-wk','data')],
                [Input('tabs_wd','value')])
def update_graph(selected_wk, tabs_wd):

    if tabs_wd == 'tab_wd-1':

        slide_epoch = v1_wd[v1_wd['epoch'] == selected_wk]
        slide_epoch = slide_epoch.sort_values("amt_usd", ascending=False)


        data = go.Bar(
        name='Withdrawal',
        x=slide_epoch["asset"],
        y=slide_epoch["amt_usd"],
        opacity=1,
        marker=dict(color = slide_epoch['amt_usd']) #amt_usd
        )


        figure = {'data': [data],
                'layout': go.Layout(
                      # colorway=['#b97af5', '#FF4F00', '#375CB1', '#FF7400', '#BBFD55', '#FFF400', '#FF0056', '#11E02C', '#F11EED', '#56E6FD'],
                      template='plotly_dark',
                      paper_bgcolor='rgba(0, 0, 0, 0)',
                      plot_bgcolor='rgba(0, 0, 0, 0)',
                      margin={'b': 15},
                      hovermode='x',
                      autosize=False,
                      title={'text': 'Volt #01 Withdrawal (USD) By Epoch', 'font': {'color': 'white'}, 'x': 0.5},
                      xaxis={'range': [-1,10]},
                      yaxis={'range': [slide_epoch['amt_usd'].min(), slide_epoch['amt_usd'].max()]},
                      transition={'duration': 500}
                )
                }


    elif tabs_wd == 'tab_wd-2':

        slide_epoch = v2_wd[v2_wd['epoch'] == selected_wk]
        slide_epoch = slide_epoch.sort_values("amt_usd", ascending=False)


        data = go.Bar(
        name='Withdrawal',
        x=slide_epoch["asset"],
        y=slide_epoch["amt_usd"],
        opacity=1,
        marker=dict(color = slide_epoch['amt_usd'])
        # color=['#b97af5', '#FF4F00', '#375CB1', '#FF7400', '#BBFD55', '#FFF400', '#FF0056', '#11E02C', '#F11EED', '#56E6FD'])
        )


        figure = {'data': [data],
                'layout': go.Layout(
                      template='plotly_dark',
                      paper_bgcolor='rgba(0, 0, 0, 0)',
                      plot_bgcolor='rgba(0, 0, 0, 0)',
                      margin={'b': 15},
                      hovermode='x',
                      autosize=False,
                      title={'text': 'Volt #02 Withdrawal (USD) By Epoch', 'font': {'color': 'white'}, 'x': 0.5},
                      xaxis={'range': [-1,5]},
                      yaxis={'range': [slide_epoch['amt_usd'].min(), slide_epoch['amt_usd'].max()], 'categoryorder': 'total ascending'},
                      transition={'duration': 500}
                )
                }


    return figure


if __name__ == '__main__':
    app.run_server(debug=True)

