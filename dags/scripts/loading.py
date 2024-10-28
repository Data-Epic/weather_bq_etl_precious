from google.cloud import bigquery
import pandas as pd
import logging


def get_bigquery_client():
    """Return a BigQuery client."""
    return bigquery.Client()


def load_df_to_bq(data: pd.DataFrame, table_id: str, schema: list) -> None:
    """Load a DataFrame into a BigQuery table, handling empty data and errors."""
    try:
        client = get_bigquery_client()

        df = pd.DataFrame(data)
        if df.empty:
            logging.info(f"No new data to load to {table_id}")
            return

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV, autodetect=False, schema=schema
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logging.info(f"{len(df)} records loaded to {table_id} successfully.")

    except Exception as e:
        logging.error(
            f"An error occurred while loading data to {table_id}: {e}", exc_info=True
        )
        raise


def deduplicate(data: pd.DataFrame, table_id: str, key: str) -> pd.DataFrame:
    """Remove duplicate records by comparing to existing BigQuery data."""
    try:
        client = get_bigquery_client()

        df = pd.DataFrame(data)
        query = f"SELECT {key} FROM `{table_id}`"
        existing_df = client.query(query).to_dataframe()

        unique_records = df[~df[key].isin(existing_df[key])]
        return unique_records

    except Exception as e:
        logging.error(
            f"An error occurred during deduplication for {table_id}: {e}", exc_info=True
        )
        raise


def load_all_data(
    normalized_data: dict, table_ids: dict, schemas: dict, keys: dict
) -> None:
    """
    Loads all normalized DataFrames to BigQuery.
    Deduplicates each DataFrame based on its primary key by fetching the table and ensuring there are no duplicates.

    :param normalized_data:  A dictionary of normalized DataFrames.
    :param table_ids_dict: Dictionary of table keys and their corresponding table IDs.
    :param schemas_dict: Dictionary of table keys and their corresponding schemas.
    :param primary_keys: Dictionary of table keys and their corresponding primary keys.
    """
    try:
        # Normalize the data
        for table_key, df in normalized_data.items():
            if df is None or df.empty:
                raise ValueError(f"No data available for loading in table {table_key}.")

            table_id = table_ids[table_key]
            schema = schemas[table_key]
            primary_key = keys[table_key]

            # Deduplicate and load data
            new_df = deduplicate(df, table_id, primary_key)
            load_df_to_bq(new_df, table_id, schema)

            logging.info(f"Data for {table_key} loaded to {table_id} successfully.")
        logging.info("All data loaded to tables successfully.")
    except Exception as e:
        logging.error(f"An error occurred while loading data: {e}", exc_info=True)
        raise
