from functions import *
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dash import Dash, dcc, html, Input, Output

import pandas as pd
import numpy as np
# For Label Encoding
from sklearn import preprocessing
import dash
import dash_core_components as dcc
import dash_html_components as html
from sqlalchemy import create_engine


def extract_df(filename):
    df = pd.read_csv(filename)
    df.to_csv('/opt/airflow/data/fintech_data_29_52_23411.csv',index=False)
    print('loaded succesfully')
    
    
def load_to_data(filename_df,filename_lookup):
    df = pd.read_csv(filename_df)
    look_up = pd.read_csv(filename_lookup)
    df.to_csv('/opt/airflow/data/fintech_final.csv',index=False)
    look_up.to_csv("/opt/airflow/data/look_up.csv",index=False)
    print("loaded to data directory")

def load_to_postgres(filename_df,filename_lookup): 
    df = pd.read_csv(filename_df)
    lookup = pd.read_csv(filename_lookup)
    
    engine = create_engine('postgresql://root:root@pgdatabase:5432/fintech_etl')
    if(engine.connect()):
        print('connected succesfully')
    else:
        print('failed to connect')
    df.to_sql(name = 'fintech_loans',con = engine,if_exists='replace')
    lookup.to_sql(name = 'lookup_table',con = engine,if_exists='replace')
    print("loaded to postgres")
    

def create_dashboard(filename):
    df=pd.read_csv(filename)
    app = Dash()
    
    app.layout = html.Div([
        html.H1("Web Application Dashboards with Dash", style={'text-align': 'center'}),
        html.Br(),
        html.H1("Fintech dataset", style={'text-align': 'center'}),
        html.Br(),
        html.Div(),
        dcc.Graph(figure=bar_no_of_loans_per_state(df)),
        html.Br(),
        html.Div(),
        dcc.Graph(figure=bar_common_purposes(df)),
        html.Br(),
        html.Div(),
        dcc.Dropdown(
            id="select grade",
            options=[
                {"label": "A", "value": "A"},
                {"label": "B", "value": "B"},
                {"label": "C", "value": "C"},
                {"label": "D", "value": "D"},
                {"label": "E", "value": "E"},
                {"label": "F", "value": "F"},
                {"label": "G", "value": "G"}
            ],
            multi=False,
            value="A",
            style={'width': "40%"}
        ),
        html.Br(),
        html.Div([
            html.Div(
                dcc.Graph(id='loan_status_per_grade', figure={}),
                style={'flex': '2', 'padding': '10px'}  # Increase width (2 parts)
            ),
            html.Div(
                dcc.Graph(id='Home_Mortgage_per_grade', figure={}),
                style={'flex': '1', 'padding': '10px'}  # Default width (1 part)
            )
        ], style={'display': 'flex', 'flex-direction': 'row', 'justify-content': 'space-around'}),
        html.Br(),
        html.Div([
            dcc.Graph(figure=Hist_Term(df)[0]),
            dcc.Graph(figure=Hist_Term(df)[1])
        ], style={'display': 'flex', 'flex-direction': 'row', 'justify-content': 'space-around'})
    ])
    
    @app.callback(
        [Output(component_id='loan_status_per_grade', component_property='figure'),
         Output(component_id='Home_Mortgage_per_grade', component_property='figure')],
        Input(component_id='select grade', component_property='value')
    )
    def update_graph(grade):
        # Filter the data for the current Letter Grade
        filtered_df = df[df["Letter Grade"] == grade]
        fig = loan_status_per_grade(filtered_df, grade)
        fig2 = Home_Mortgage_per_grade(filtered_df, grade)
        return fig, fig2
    print("Dashboard Created....running server")
    app.run_server(host='0.0.0.0',debug=False)
    print("Server opened !!")
# Usage:
# Define or import the required functions and dataset (`df`) before calling this function.
# Example:
# run_fintech_dashboard(df, bar_no_of_loans_per_state, bar_common_purposes, loan_status_per_grade, Home_Mortgage_per_grade, Hist_Term)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': days_ago(2),
    "retries": 1,
}

dag = DAG(
    'fintech_etl_pipeline',
    default_args=default_args,
    description='fintech etl pipeline',
)
with DAG(
    dag_id = 'fintech_etl_pipeline',
    schedule_interval = '@once',
    default_args = default_args,
    tags = ['fintech-pipeline'],
)as dag:
    read_csv_task= PythonOperator(
        task_id = 'read_csv',
        python_callable = extract_df,
        op_kwargs={
            "filename": '/opt/airflow/data/fintech_data_29_52_23411.csv'
        },
    )
    clean_and_transform_task= PythonOperator(
        task_id = 'clean_transform',
        python_callable = clean_df,
        op_kwargs={
            "filename": "/opt/airflow/data/fintech_data_29_52_23411.csv"
        },
    )
    load_to_data_task=PythonOperator(
        task_id = 'load_to_data',
        python_callable = load_to_data,
        op_kwargs={
            "filename_df": "/opt/airflow/data/fintech_clean.csv"
            ,"filename_lookup": "/opt/airflow/data/look_up.csv"
        },
    )
    load_to_postgres_task=PythonOperator(
        task_id = 'load_to_postgres',
        python_callable = load_to_postgres,
        op_kwargs={
            "filename_df": "/opt/airflow/data/fintech_final.csv"
            ,"filename_lookup": "/opt/airflow/data/look_up.csv"

        },
    )
    
    create_dashboard_task= PythonOperator(
        task_id = 'create_dashboard_task',
        python_callable = create_dashboard,
        op_kwargs={
            "filename": "/opt/airflow/data/fintech_After_FeatureEng.csv"
        },
    )
    


    read_csv_task >> clean_and_transform_task >> load_to_data_task >> load_to_postgres_task >> create_dashboard_task

    
    




