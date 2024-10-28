from google.cloud import bigquery

# Dictionary of schemas
schemas_dict = {
    "cities": [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("lon", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("lat", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        # bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    ],
    "timezone": [
        bigquery.SchemaField("timezone", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("timezone_utc", "STRING", mode="REQUIRED"),
    ],
    "city_tz": [
        bigquery.SchemaField("city_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("tz_id", "STRING", mode="REQUIRED"),
    ],
    "weather_condition": [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("main", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("icon", "STRING", mode="NULLABLE"),
    ],
    "temperature": [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("temp", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("feels_like", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("temp_min", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("temp_max", "FLOAT", mode="NULLABLE"),
    ],
    "pressure": [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pressure", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("sea_level", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("grnd_level", "INTEGER", mode="NULLABLE"),
    ],
    "wind": [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("speed", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("deg", "INTEGER", mode="NULLABLE"),
    ],
    "sun": [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sunrise", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("sunset", "INTEGER", mode="NULLABLE"),
        # bigquery.SchemaField("recorded_at", "TIMESTAMP", mode="NULLABLE"),
    ],
    "weather_record": [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("city_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("weather_condition_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("temp_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pressure_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("wind_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("sun_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("humidity", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("visibility", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("cloud", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("dt", "INTEGER", mode="NULLABLE"),
        # bigquery.SchemaField("recorded_at", "TIMESTAMP", mode="NULLABLE"),
    ],
}

# Dictionary of table IDs
project_id = "weather-pipeline-12"
dataset_id = "weather_data"
table_ids_dict = {
    "cities": f"{project_id}.{dataset_id}.cities",
    "timezone": f"{project_id}.{dataset_id}.timezone",
    "city_tz": f"{project_id}.{dataset_id}.city_tz",
    "weather_condition": f"{project_id}.{dataset_id}.weather_condition",
    "temperature": f"{project_id}.{dataset_id}.temperature",
    "pressure": f"{project_id}.{dataset_id}.pressure",
    "wind": f"{project_id}.{dataset_id}.wind",
    "sun": f"{project_id}.{dataset_id}.sun",
    "weather_record": f"{project_id}.{dataset_id}.weather_record",
}

# Define primary keys for each table
primary_keys = {
    "cities": "id",
    "timezone": "timezone_utc",
    "city_tz": "city_id",
    "weather_condition": "id",
    "temperature": "id",
    "pressure": "id",
    "wind": "id",
    "sun": "id",
    "weather_record": "id",
}
