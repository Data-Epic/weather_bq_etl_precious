from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import logging
import json
import os


# Exception for when no data is found for filtering
class NoDataFoundError(Exception):
    """Exception raised when no data is found in a dataframe."""

    pass


def get_countries() -> list:
    """Fetch  from Airflow Variables or log an error if not set"""
    try:
        country_str = Variable.get("COUNTRIES")
        country = [country.strip() for country in country_str.split(",")]
        logging.info(f"Country codes: {country}")
        return country
    except KeyError:
        logging.error(
            "Airflow Variable 'COUNTRIES' is not set. Please set it in the Airflow UI under Admin > Variables. "
            "Example - key: COUNTRIES, value: NG, GH, BR"
        )
        raise
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        raise


# Extraction function
def extract_city_data_from_json(json_file: str) -> pd.DataFrame:
    try:
        logging.info(f"Loading JSON data from {json_file}...")
        with open(json_file, "r", encoding="utf-8") as file:
            city_data = json.load(file)
        processed_data = [
            {
                "city_id": int(city["id"]),
                "city_name": str(city["name"]),
                "country": str(city["country"]),
            }
            for city in city_data
        ]
        logging.info(f"{len(processed_data)} records loaded from {json_file}")
        return pd.DataFrame(processed_data)
    except FileNotFoundError:
        logging.error(f"File {json_file} not found.")
        raise
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from file {json_file}.")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        raise


# Filter function
def filter_cities_by_country(city_df: pd.DataFrame, country_code: str) -> pd.DataFrame:
    try:
        filtered_df = city_df[city_df["country"] == country_code.upper()]
        if filtered_df.empty:
            raise NoDataFoundError(f"No cities found for country code '{country_code}'")
        logging.info(
            f"{len(filtered_df)} cities loaded for country code '{country_code}'"
        )
        return filtered_df
    except NoDataFoundError as e:
        logging.error(f"NoDataFoundError: {e}")
        raise
    except Exception as e:
        logging.error(
            f"An unexpected error occurred while filtering cities: {e}", exc_info=True
        )
        raise


# Save function
def save_city_data_to_csv(city_data: pd.DataFrame, csv_file: str) -> None:
    try:
        city_data.to_csv(csv_file)
        logging.info(
            f"{len(city_data)} city records saved to '{csv_file}' successfully."
        )
    except FileNotFoundError:
        logging.error(f"File {csv_file} not found.")
        raise
    except PermissionError:
        logging.error(f"Permission denied when trying to write to {csv_file}.")
        raise
    except Exception as e:
        logging.error(
            f"An unexpected error occurred while saving city data to CSV: {e}",
            exc_info=True,
        )
        raise


# Main task function
def update_city_csv():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file = os.path.join(current_dir, "data", "cities.csv")
    json_file = os.path.join(current_dir, "data", "city.list.json")

    try:
        # Step 1: Extract all city data
        data = extract_city_data_from_json(json_file)

        # Step 2: Filter cities by the specified countries
        city_data = pd.DataFrame()
        countries = get_countries()

        for country in countries:
            filtered_data = filter_cities_by_country(data, country)
            city_data = (
                pd.concat([city_data, filtered_data])
                if not city_data.empty
                else filtered_data
            )

        # Step 3: Save the combined filtered data to CSV
        save_city_data_to_csv(city_data, csv_file)
    except NoDataFoundError as e:
        logging.error(f"No data found error: {e}")
        raise
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
        raise


# Define DAG
with DAG(
    dag_id="update_city_csv",
    schedule_interval=None,  # Triggered manually
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["city_data"],
) as dag:
    update_city_csv_task = PythonOperator(
        task_id="update_city_csv", python_callable=update_city_csv
    )
