from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import requests
import pandas as pd
from datetime import datetime
import json
from dotenv import load_dotenv
import os
from discord_webhook import DiscordWebhook
from pathlib import Path

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}


dag = DAG(
    'country_etl_dag',
    default_args=default_args,
    description='ETL DAG for country data',
    schedule_interval='@daily',
)


# File paths for temporary storage
extract_path = '/tmp/countries_data_raw.json'
transform_path = '/tmp/countries_data_transformed.csv'


# Discord webhook URL
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')

# send notifications to Discord
def notify_discord(message):
    webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, content=message)
    webhook.execute()


def extract_data():
    url = 'https://restcountries.com/v3.1/all'
    response = requests.get(url)
    countries_data = response.json()
    with open(extract_path, 'w') as f:
        json.dump(countries_data, f)
    notify_discord("File extracted from website")

    

def transform_data():
    with open(extract_path, 'r') as f:
        countries_data = json.load(f)
        
    transformed_data = []
    for country in countries_data:
        country_info = {
            "country_name": country.get('name', {}).get('common', None),
            "independence": country.get('independent', None),
            "un_member": country.get('unMember', None),
            "start_of_week": country.get('startOfWeek', None),
            "official_country_name": country.get('name', {}).get('official', None),
            "common_native_name": next(iter(country.get('name', {}).get('nativeName', {}).values()), {}).get('common', None),
            "currency_code": next(iter(country.get('currencies', {}).keys()), None),
            "currency_name": next(iter(country.get('currencies', {}).values()), {}).get('name', None),
            "currency_symbol": next(iter(country.get('currencies', {}).values()), {}).get('symbol', None),
            "country_code": f"{country.get('idd', {}).get('root', '')}{next(iter(country.get('idd', {}).get('suffixes', [''])), '')}",
            "capital": next(iter(country.get('capital', [])), None),
            "region": country.get('region', None),
            "sub_region": country.get('subregion', None),
            "languages": ', '.join(country.get('languages', {}).values()),
            "area": country.get('area', None),
            "population": country.get('population', None),
            "continents": ', '.join(country.get('continents', []))
        }
        transformed_data.append(country_info)
    
    df = pd.DataFrame(transformed_data)
    df.to_csv(transform_path, index=False)


def load_data():
    DATABASE_URI = os.getenv('DATABASE_URI')
    engine = create_engine(DATABASE_URI)
    df = pd.read_csv(transform_path)
    df.to_sql('countries', engine, if_exists='replace', index=False)
    notify_discord("File loaded to Postgres completed")



# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)


extract_task >> transform_task >> load_task
