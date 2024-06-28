from airflow import DAG
import airflow
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
#https://blog.devgenius.io/etl-process-using-airflow-and-docker-226aa5c7a41a

from datetime import datetime, timedelta
from pendulum import date
import json
import pandas as pd
from pandas import json_normalize
from bs4 import BeautifulSoup
import requests


# Grab current date
current_date = datetime.today().strftime('%Y-%m-%d')

# API_URL = os.getenv('API_URL')
# API_KEY = os.getenv('API_KEY')

# python function to transform data
def _process_data(ti):
    raw_data = ti.xcom_pull(task_ids = 'extract_data')
    data = raw_data['rates']
    processed_data = pd.DataFrame(columns=['rate','symbol'])

    temp_df = dict()

    for symbol, rate in data.items():
        temp_df = json_normalize({
            'rate':float(rate),
            'symbol':symbol
        })

        processed_data= processed_data.append(temp_df)

    # Export DataFrame to CSV
    
    processed_data.to_csv('/tmp/processed_data.csv', index=None, header=False)

def _store_data():
    '''
    This function uses the Postgres Hook to copy users from processed_data.csv
    and into the table
    
    '''
    # Connect to the Postgres connection
    hook = PostgresHook(postgres_conn_id = 'postgres')

    # Insert the data from the CSV file into the postgres database
    hook.copy_expert(
        sql = "COPY rates FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_data.csv'
    )

# API_URL = 'http://data.fixer.io/api/'
# API_KEY = '42bab102608e1c3a5ae0f02a7f01105c'

# Default setting for all the dag in the pipeline
default_args = {
    "owner": "Airflow",
    "start_date": datetime(2023,12,30),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)     
}

with DAG('forex_pipeline',
         default_args=default_args,
         schedule_interval=timedelta(minutes=2),
         catchup=False) as dag:
    
    #Dag #1 - check if APi is available
    is_api_available = HttpSensor(
        task_id = 'is_api_available',
        method='GET',
        http_conn_id="is_api_available",
        endpoint = current_date + '?access_key=42bab102608e1c3a5ae0f02a7f01105c',
        #endpoint= f"{API_URL} + latest +?access_key={API_KEY}"
        response_check= lambda response:'EUR' in response.text,
        poke_interval=5
    )

    #Dag #2 - Create a table
    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'postgres',
        sql="""
            DROP TABLE IF EXISTS rates;

            CREATE TABLE rates(
            rate float not null,
            symbol text not null
            );

        """
    )

    #Dag #3 Extract Data
    extract_data = SimpleHttpOperator(
    task_id='extract_data',
    http_conn_id='is_api_available',
    method='GET',
    endpoint=current_date + '?access_key=42bab102608e1c3a5ae0f02a7f01105c', # Assuming you need to pass an API key in the headers
    response_filter=lambda response: json.loads(response.text),
    log_response=True,
    )


    #Dag #4 Process data
    transform_data = PythonOperator(
        task_id = 'transform_data',
        python_callable=_process_data
    )

    #Dag #5 Load
    load_data = PythonOperator(
        task_id = 'load_data',
        python_callable=_store_data

    )


    # set dependecies
    is_api_available >> create_table >> extract_data >> transform_data >> load_data
    