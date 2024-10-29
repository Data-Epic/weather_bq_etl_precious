from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
import pandas as pd
from airflow.exceptions import AirflowFailException
from dags.scripts.fetch_data import fetch_weather_for_cities
from dags.scripts.processing import normalize_weather_data
from dags.scripts.bq_table_config import schemas_dict, table_ids_dict, primary_keys
from dags.scripts.loading import load_all_data
from dotenv import load_dotenv
import os

load_dotenv()

# Default arguments for the DAG
default_args = {
    "owner": "Airlow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def fetch_data_task(**kwargs):
    try:
        api_key = os.getenv("API_KEY")

        current_dir = os.path.dirname(os.path.abspath(__file__))
        cities_file = os.path.join(current_dir, "data", "cities.csv")

        raw_data = fetch_weather_for_cities(cities_file, api_key)

        if raw_data.empty:
            raise ValueError("No data fetched from API.")
        kwargs["ti"].xcom_push(key="raw_data", value=raw_data)
        logging.info("Weather data fetched successfully.")
    except Exception as e:
        logging.error(f"Error in fetch_data_task: {e}", exc_info=True)
        raise AirflowFailException(f"fetch_data_task failed: {e}")


def normalize_data_task(**kwargs):
    try:
        raw_data = kwargs["ti"].xcom_pull(key="raw_data", task_ids="fetch_data")
        if raw_data is None or raw_data.empty:
            raise ValueError("No data received for normalization.")

        normalized_data = normalize_weather_data(pd.DataFrame(raw_data))
        kwargs["ti"].xcom_push(key="normalized_data", value=normalized_data)
        logging.info("Weather data normalized successfully.")
    except Exception as e:
        logging.error(f"Error in normalize_data_task: {e}", exc_info=True)
        raise AirflowFailException(f"normalize_data_task failed: {e}")


def load_data_task(**kwargs):
    try:
        normalized_data = kwargs["ti"].xcom_pull(
            key="normalized_data", task_ids="normalize_data"
        )
        if normalized_data is None:
            raise ValueError("No normalized data available for loading.")

        load_all_data(normalized_data, table_ids_dict, schemas_dict, primary_keys)
        logging.info("All normalized data loaded to BigQuery successfully.")
    except Exception as e:
        logging.error(f"Error in load_data_task: {e}", exc_info=True)
        raise AirflowFailException(f"load_data_task failed: {e}")


with DAG(
    dag_id="weather_etl_dag",
    default_args=default_args,
    description="Fetch, normalize, and load weather data to BigQuery",
    schedule_interval="@hourly",  # Adjust the schedule as needed
    start_date=days_ago(1),
    catchup=False,
) as dag:
    fetch_data = PythonOperator(
        task_id="fetch_data", python_callable=fetch_data_task, provide_context=True
    )

    normalize_data = PythonOperator(
        task_id="normalize_data",
        python_callable=normalize_data_task,
        provide_context=True,
    )

    load_data = PythonOperator(
        task_id="load_data", python_callable=load_data_task, provide_context=True
    )

    fetch_data >> normalize_data >> load_data
