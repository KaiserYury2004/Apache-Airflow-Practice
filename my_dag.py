from pymongo import MongoClient
import pandas as pd
import os
import re
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
import configparser
import logging
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset
from airflow.providers.mongo.hooks.mongo import MongoHook
config = configparser.ConfigParser()
config.read("config.ini")
#path_to_data=/path/to/your/data/file.csv
path_to_data='/home/kaiser/airflow/dags/tiktok_google_play_reviews.csv'
def loading_data():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info("Подключение:")
    try:
        hook = MongoHook(mongo_conn_id='Mongo_compass')
        client=hook.get_conn()
        # db = client.your_database_name
        # collection = db.your_collection_name
        db = client.admin
        collection = db.TikTokReviews
        df=pd.read_csv(path_to_data)
        data_dictionary = df.to_dict('records')
        collection.insert_one(data_dictionary)
        client.close()
    except Exception as e:
        logger.error("Ошибка: %s", e)
def empty_checker():
    file_path =path_to_data
    if os.path.getsize(file_path) > 0:
        return 'NotEmpty'
    else:
        return 'Empty'
def edition_1():
    df=pd.read_csv(path_to_data)
    df=df.replace('null','-')
    df.to_csv(path_to_data, index=False)
def edition_2():
    df = pd.read_csv(path_to_data)
    df = df.sort_values(by='at')
    df.to_csv(path_to_data, index=False)
def edition_3():
    df = pd.read_csv(path_to_data)
    def clean_text(text):
            return re.sub(r'[^a-zA-Z0-9\s.,!?\'\";:()-]', '', text)
    df['content'] = df['content'].astype(str).apply(clean_text)
    csv_filename = path_to_data
    df.to_csv(csv_filename, index=False)

with DAG("Mongo_DAG_1", start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False) as main_dag:
    sensor_searching = FileSensor(
        task_id='sensor',
        filepath=path_to_data,
        fs_conn_id='fs_snowflake',
        poke_interval=10,
        timeout=300
    )
    task_branch = BranchPythonOperator(
        task_id='empty_checker',
        python_callable=empty_checker,
    )
    with TaskGroup('editon_tasks') as edition_tasks:
        editor_1=PythonOperator(
            task_id='editor_1',
            python_callable=edition_1,
        )
        editor_2=PythonOperator(
            task_id='editor_2',
            python_callable=edition_2,
        )
        editor_3=PythonOperator(
            task_id='editor_3',
            python_callable=edition_3,
            outlets=[Dataset(path_to_data)]
        )
        editor_1>>editor_2>>editor_3
    Empty = BashOperator(
        task_id='Empty',
        bash_command='echo "File is empty!"',
    )
    NotEmpty = BashOperator(
        task_id='NotEmpty',
        bash_command='echo "File is not empty!"'
    )
    sensor_searching >> task_branch
    task_branch >> Empty
    task_branch >> NotEmpty
    NotEmpty>>edition_tasks

with DAG(
    dag_id='Mongo_DAG_2',start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False,
    schedule=[Dataset(path_to_data)]
) as dag2:
    loader = PythonOperator(
        task_id='load',
        python_callable=loading_data,
        provide_context=True
    )