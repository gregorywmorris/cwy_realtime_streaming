from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from kafka import KafkaProducer
import json
import time
import logging


default_args = {
    'owner': 'airscholor',
    'start_date': datetime(2023, 8, 3, 10, 0),
}

def get_data():
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    return res['results'][0]

def format_data(res):
    location = res['location']
    return {
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'gender': res['gender'],
        'address': f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}",
        'postcode': location['postcode'],
        'email': res['email'],
        'username': res['login']['username'],
        'dob': res['dob']['date'],
        'registration_date': res['registered']['date'],
        'phone': res['phone'],
        'picture': res['picture']['medium']
    }

def stream_data():
    
    Producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while not time.time() > curr_time + 60:
        try:
            res = get_data()
            res = format_data(res)
    
            Producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False
         ) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
