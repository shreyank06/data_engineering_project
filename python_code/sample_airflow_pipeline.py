import sqlite3
import os
import json
from send_to_ihc_api import send_to_ihc_api_and_store_results  # Importing the function from send_to_ihc.py
from channel_reporting_excel import main as channel_reporting_main
from channel_reporting_table import populate_channel_reporting, check_ihc_sum_condition, check_channel_reporting_table_exists, check_table_exists, get_customer_journeys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Set up the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 9),
    'retries': 1,
}

dag = DAG(
    'customer_journey_pipeline',
    default_args=default_args,
    description='A pipeline for processing customer journeys and sending data to IHC API',
    schedule_interval=None,  # Set to None or a schedule as needed
)

def load_customer_journeys(**kwargs):
    save_path = "customer_journeys.json"
    db_path = "../challenge.db"
    
    if os.path.exists(save_path):
        print(f"Customer journeys already exist in {save_path}, proceeding with next steps...")
        with open(save_path, "r") as f:
            journeys = json.load(f)
        kwargs['ti'].xcom_push(key='journeys', value=journeys)  # Push to XCom for next tasks
    else:
        print("Customer journeys not found. Generating journeys first...")
        journeys = get_customer_journeys(db_path, save_path)
        kwargs['ti'].xcom_push(key='journeys', value=journeys)  # Push to XCom for next tasks

def check_channel_reporting_table(db_path, **kwargs):
    if check_channel_reporting_table_exists(db_path):
        print("Channel Reporting table exists, proceeding with CSV file generation...")
        check_ihc_sum_condition(db_path)  # Check the IHC sum condition after populating the channel_reporting table
        return 'prepare_csv_file'
    else:
        if not check_table_exists(db_path, 'attribution_customer_journey'):
            print("Attribution customer journey table does not exist, sending customer data to IHC API...")
            journeys = kwargs['ti'].xcom_pull(key='journeys', task_ids='load_customer_journeys')
            send_to_ihc_api_and_store_results(journeys, db_path, conv_type_id="ihc_challenge")
            populate_channel_reporting(db_path)
            return 'populate_channel_reporting'
        else:
            print("Attribution customer journey table exists, populating channel reporting table...")
            populate_channel_reporting(db_path)
            check_ihc_sum_condition(db_path)
            return 'prepare_csv_file'

def prepare_csv_file(**kwargs):
    print("📝 Continuing with preparing the CSV file...")
    channel_reporting_main()  # Execute the main function from channel_reporting_excel.py  

def populate_channel_reporting_table(**kwargs):
    print("Populating channel reporting table...")
    populate_channel_reporting("../challenge.db")

# Define the tasks
load_journeys_task = PythonOperator(
    task_id='load_customer_journeys',
    python_callable=load_customer_journeys,
    provide_context=True,
    dag=dag,
)

check_channel_reporting_task = PythonOperator(
    task_id='check_channel_reporting_table',
    python_callable=check_channel_reporting_table,
    op_args=["../challenge.db"],
    provide_context=True,
    dag=dag,
)

prepare_csv_task = PythonOperator(
    task_id='prepare_csv_file',
    python_callable=prepare_csv_file,
    provide_context=True,
    dag=dag,
)

populate_channel_reporting_task = PythonOperator(
    task_id='populate_channel_reporting',
    python_callable=populate_channel_reporting_table,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
load_journeys_task >> check_channel_reporting_task
check_channel_reporting_task >> [prepare_csv_task, populate_channel_reporting_task]
