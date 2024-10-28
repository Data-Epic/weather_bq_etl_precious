import pandas as pd
import requests
import logging
import time
from ratelimit import limits, sleep_and_retry
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_RECORDS = 1000
GROUP_SIZE = 20
REQUESTS_PER_MINUTE = 60
RETRY_DELAY = 5


def load_cities_from_csv(csv_file: str) -> pd.DataFrame:
    """
    Loads cities from a CSV file to a DataFrame. If the number of records exceeds MAX_RECORDS,
    only the first MAX_RECORDS records are loaded.
    """
    df = pd.read_csv(csv_file)
    if len(df) > MAX_RECORDS:
        logging.warning(
            f"Number of records ({len(df)}) exceeds the maximum limit of {MAX_RECORDS}. Only the first {MAX_RECORDS} records will be loaded."
        )
        df = df.head(MAX_RECORDS)
    return df


@sleep_and_retry
@limits(calls=REQUESTS_PER_MINUTE, period=60)
def fetch_weather_data(city_group: str, api_key: str) -> dict:
    url = f"https://api.openweathermap.org/data/2.5/group?id={city_group}&units=metric&appid={api_key}"
    for attempt in range(2):
        try:
            response = requests.get(url)
            if response.status_code != 200:
                raise Exception(
                    f"API request failed with status code {response.status_code}, {response.text}"
                )
            return response.json()
        except Exception as e:
            if attempt < 1:
                logging.error(
                    f"Request failed: {e}. Retrying in {RETRY_DELAY} seconds..."
                )
                time.sleep(RETRY_DELAY)
            else:
                logging.error(f"Retry failed: {e}")
    return {}


def fetch_weather_for_cities(csv_file: str, api_key: str) -> pd.DataFrame:
    city_df = load_cities_from_csv(csv_file)
    city_groups = [
        ",".join(map(str, city_df["city_id"][i : i + GROUP_SIZE]))
        for i in range(0, len(city_df), GROUP_SIZE)
    ]

    weather_data = []
    count = 0
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(fetch_weather_data, group, api_key) for group in city_groups
        ]
        for future in as_completed(futures):
            try:
                data = future.result()
                if "list" in data:
                    weather_data.extend(data["list"])
                    count += len(data["list"])
                logging.info(f"Fetched data: {data}")
            except Exception as e:
                logging.error(f"Failed to fetch data: {e}", exc_info=True)
        logging.info(f"Weather data fetched for {count} cities")

    return pd.DataFrame(weather_data)
