from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
import logging
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2025, 3, 4),
}

def get_data() -> dict:
    try:
        res = requests.get("https://randomuser.me/api/", timeout=10)
        res.raise_for_status()
        return res.json()['results'][0]
    except requests.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        return {}

def format_data(res: dict) -> dict:
    if not res:
        return {}

    location = res.get('location', {})
    return {
        'first_name': res.get('name', {}).get('first', ''),
        'last_name': res.get('name', {}).get('last', ''),
        'gender': res.get('gender', ''),
        'address': f"{location.get('street', {}).get('number', '')} {location.get('street', {}).get('name', '')} "
                   f"{location.get('city', '')} {location.get('state', '')} {location.get('country', '')}",
        'email': res.get('email', ''),
        'username': res.get('login', {}).get('username', ''),
        'dob': res.get('dob', {}).get('date', ''),
        'registered_date': res.get('registered', {}).get('date', ''),
        'phone': res.get('phone', ''),
        'picture': res.get('picture', {}).get('medium', ''),
    }

def create_producer():
    for _ in range(5):
        try:
            return KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
        except KafkaError as e:
            logging.error(f"Kafka connection error: {e}")
            time.sleep(5)
    return None

def stream_data():
    producer = create_producer()
    if not producer:
        logging.error("Kafka producer could not be created. Exiting task.")
        return

    start_time = time.time()
    while time.time() - start_time < 60:
        try:
            res = get_data()
            res = format_data(res)
            if res:
                producer.send(topic='users_created', value=json.dumps(res).encode('utf-8'))
                logging.info(f"Sent: {res}")
            else:
                logging.warning("No data fetched, skipping...")
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue

with DAG(
    'user_automation',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
