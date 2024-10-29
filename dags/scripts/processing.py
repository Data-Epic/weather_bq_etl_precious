# transform_data.py
import pandas as pd
import logging


def get_timezone_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a timezone dimension table from the given DataFrame.
    Extracts timezone, and timezone in UTC from the 'sys' column.
    Unique id: timezone

    :param df: DataFrame containing weather data with a 'sys' field.
    :return: DataFrame representing the country dimension table.
    :raises ValueError: If 'sys' field is not present in the DataFrame.
    :raises Exception: For any other issues encountered during processing.
    """
    try:
        if "sys" not in df.columns:
            logging.error("The 'sys' field is missing from the DataFrame.")
            raise ValueError("The 'sys' field is missing from the DataFrame.")

        # Extract relevant fields from the 'sys' column
        timezone_df = df[["sys"]].copy()
        timezone_df["timezone"] = timezone_df["sys"].apply(lambda x: x["timezone"])
        timezone_df["timezone_utc"] = timezone_df["timezone"].apply(
            lambda x: f"UTC{'+' if x >= 0 else '-'}{abs(x) // 3600}"
        )

        # Drop duplicates to create the dimension table
        timezone_df = (
            timezone_df[["timezone", "timezone_utc"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        logging.info(
            f"timezone_df created successfully with {len(timezone_df)} records."
        )
        return timezone_df

    except ValueError as e:
        logging.error(f"ValueError: {e}", exc_info=True)
        raise
    except Exception as e:
        logging.error(
            f"An error occurred while creating timezone df: {e}", exc_info=True
        )
        raise


def get_city_df(data: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a city DataFrame that references the timezone and includes country, city_id, name, lat, lon, and other important data.

    :param data: DataFrame containing weather information.
    :return: DataFrame containing the combined city information.
    """
    try:
        city_df = data[["id", "name", "coord", "sys"]].copy()

        # Extract relevant fields from the 'coord' and 'sys' columns
        city_df["lat"] = city_df["coord"].apply(lambda x: x["lat"])
        city_df["lon"] = city_df["coord"].apply(lambda x: x["lon"])
        city_df["country"] = city_df["sys"].apply(lambda x: x["country"])
        city_df["timezone"] = city_df["sys"].apply(lambda x: x["timezone"])
        city_df["city"] = city_df["name"]

        city_df = (
            city_df[["id", "city", "lon", "lat", "country", "timezone"]]
            .drop_duplicates(subset=["id"])
            .reset_index(drop=True)
        )

        logging.info(
            f"City DataFrame created successfully with {len(city_df)} records."
        )
        return city_df

    except Exception as e:
        logging.error(
            f"An error occurred while creating the city DataFrame: {e}", exc_info=True
        )
        raise


def get_city_tz(city_df: pd.DataFrame, timezone_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges the city dataframe with the timezone dataframe based on the timezone column.

    :param city_df: DataFrame containing city data.
    :param timezone_df: DataFrame containing timezone data.
    :return: DataFrame containing city_id and timezone_id.
    """
    try:
        merged_df = city_df.merge(timezone_df, on="timezone", how="left")
        city_tz_df = merged_df[["id", "timezone_utc"]].rename(
            columns={"id": "city_id", "timezone_utc": "tz_id"}
        )
        logging.info(
            f"City and timezone data merged successfully with {len(city_tz_df)} records."
        )

        # Drop the timezone column from city_df
        city_df.drop(columns=["timezone"], inplace=True)

        return city_tz_df

    except Exception as e:
        logging.error(
            f"An error occurred while merging city and timezone data: {e}",
            exc_info=True,
        )
        raise


def get_weather_condition_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create the dataframe for the weather conditions table from the given DataFrame.
    :param df: DataFrame containing weather data with a 'weather' field.
    :return: DataFrame representing the weather_condition table.
    :raises ValueError: If 'weather' field is not present in the DataFrame.
    :raises Exception: For any other issues encountered during processing.
    """
    try:
        if "weather" not in df.columns:
            logging.error("The 'weather' field is missing from the DataFrame.")
            raise ValueError("The 'weather' field is missing from the DataFrame.")

        #  extract weather column to another dataframe
        weather_df = df[["weather"]].explode("weather")
        weather_df = pd.json_normalize(weather_df["weather"])

        weather_df = weather_df.drop_duplicates(subset=["id"]).reset_index(drop=True)
        logging.info(f"weather_df created successfully with {len(weather_df)} records.")
        return weather_df

    except ValueError as e:
        logging.error(f"ValueError: {e}", exc_info=True)
        raise
    except Exception as e:
        logging.error(
            f"An error occurred while creating weather_df: {e}", exc_info=True
        )
        raise


def get_temperature_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts temperature-related data from the given DataFrame and returns a new DataFrame with relevant fields.
    Use temp_id_dt as unique id for each record.
    Unit of measurement: Celsius

    :param df: DataFrame containing weather data with a 'main' field.
    :return: DataFrame containing the temperature-related data.
    :raises ValueError: If 'main' field is not present in the DataFrame.
    :raises Exception: For any other issues encountered during processing.
    """
    try:
        if "main" not in df.columns:
            logging.error("The 'main' field is missing from the DataFrame.")
            raise ValueError("The 'main' field is missing from the DataFrame.")

        temperature_df = df[["id", "dt", "main"]].copy()

        temperature_df["temp"] = temperature_df["main"].apply(lambda x: x["temp"])
        temperature_df["feels_like"] = temperature_df["main"].apply(
            lambda x: x["feels_like"]
        )
        temperature_df["temp_min"] = temperature_df["main"].apply(
            lambda x: x["temp_min"]
        )
        temperature_df["temp_max"] = temperature_df["main"].apply(
            lambda x: x["temp_max"]
        )

        temperature_df["id"] = temperature_df.apply(
            lambda row: f"temp_{row['id']}_{row['dt']}", axis=1
        )

        # Final Temperature DataFrame
        temperature_df = temperature_df[
            ["id", "temp", "feels_like", "temp_min", "temp_max"]
        ]

        logging.info(
            f"Temperature data extracted successfully with {len(temperature_df)} records."
        )
        return temperature_df

    except ValueError as e:
        logging.error(f"ValueError: {e}", exc_info=True)
        raise
    except Exception as e:
        logging.error(
            f"An error occurred while extracting temperature data: {e}", exc_info=True
        )
        raise


def get_pressure_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts pressure-related data from the given DataFrame and returns a new DataFrame with relevant fields.
    Use p_id_dt as unique id for each record.
    Unit of measurement: hPa

    :param df: DataFrame containing weather data with a 'main' field.
    :return: DataFrame containing the pressure-related data.
    :raises ValueError: If 'main' field is not present in the DataFrame.
    :raises Exception: For any other issues encountered during processing.
    """
    try:
        if "main" not in df.columns:
            logging.error("The 'main' field is missing from the DataFrame.")
            raise ValueError("The 'main' field is missing from the DataFrame.")

        pressure_df = df[["id", "dt", "main"]].copy()

        pressure_df["pressure"] = pressure_df["main"].apply(lambda x: x["pressure"])
        pressure_df["sea_level"] = pressure_df["main"].apply(
            lambda x: x.get("sea_level")
        )
        pressure_df["grnd_level"] = pressure_df["main"].apply(
            lambda x: x.get("grnd_level")
        )

        pressure_df["id"] = pressure_df.apply(
            lambda row: f"p_{row['id']}_{row['dt']}", axis=1
        )

        # Final Pressure DataFrame
        pressure_df = pressure_df[["id", "pressure", "sea_level", "grnd_level"]]

        logging.info(
            f"Pressure data extracted successfully with {len(pressure_df)} records."
        )
        return pressure_df

    except ValueError as e:
        logging.error(f"ValueError: {e}", exc_info=True)
        raise
    except Exception as e:
        logging.error(
            f"An error occurred while extracting pressure data: {e}", exc_info=True
        )
        raise


def get_wind_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts wind-related data from the given DataFrame and returns a new DataFrame with relevant fields.
    Use wind_id_dt as unique id for each record.
    Unit of measurement: speed in meter/sec, direction in degrees.

    :param df: DataFrame containing weather data with a 'wind' field.
    :return: DataFrame containing the wind-related data.
    :raises ValueError: If 'wind' field is not present in the DataFrame.
    :raises Exception: For any other issues encountered during processing.
    """
    try:
        if "wind" not in df.columns:
            logging.error("The 'wind' field is missing from the DataFrame.")
            raise ValueError("The 'wind' field is missing from the DataFrame.")

        wind_df = df[["id", "dt", "wind"]].copy()

        wind_df["speed"] = wind_df["wind"].apply(lambda x: x["speed"])
        wind_df["deg"] = wind_df["wind"].apply(lambda x: x["deg"])

        wind_df["id"] = wind_df.apply(
            lambda row: f"wind_{row['id']}_{row['dt']}", axis=1
        )

        # Final Wind DataFrame
        wind_df = wind_df[["id", "speed", "deg"]]

        logging.info(f"Wind data extracted successfully with {len(wind_df)} records.")
        return wind_df

    except ValueError as e:
        logging.error(f"ValueError: {e}", exc_info=True)
        raise
    except Exception as e:
        logging.error(
            f"An error occurred while extracting wind data: {e}", exc_info=True
        )
        raise


def get_sun_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts sunrise and sunset data from the given DataFrame and returns a new DataFrame with relevant fields.
    Use city_id_sunrise_sunset as unique id
    Unit of measurement: seconds in utc timezone

    :param df: DataFrame containing weather data with a 'sys' field.
    :return:  DataFrame containing the sunrise and sunset data.
    :raises ValueError: If 'sys' field is not present in the DataFrame.
    :raises Exception: For any other issues encountered during processing.
    """

    try:
        if "sys" not in df.columns:
            logging.error("The 'sys' field is missing from the DataFrame.")
            raise ValueError("The 'sys' field is missing from the DataFrame.")

        sun_df = df[["sys"]].copy()

        sun_df["sunrise"] = sun_df["sys"].apply(lambda x: x["sunrise"])
        sun_df["sunset"] = sun_df["sys"].apply(lambda x: x["sunset"])

        sun_df["id"] = sun_df.apply(
            lambda row: f"{row['sunrise']}_{row['sunset']}", axis=1
        )

        sun_df = (
            sun_df[["id", "sunrise", "sunset"]]
            .drop_duplicates(subset=["id"])
            .reset_index(drop=True)
        )

        logging.info(
            f"Sunrise and Sunset data extracted successfully with {len(sun_df)} records."
        )

        return sun_df
    except Exception as e:
        logging.error(
            f"An error occurred while extracting sunrise and sunset data: {e}",
            exc_info=True,
        )
        raise


def get_weather_record(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates a weather record DataFrame from the given DataFrame.
    Contains the ids for main, weather, wind, and sun tables as well as  humidity, visibility, and column for each record
    Use id_dt as unique id for each record

    :param df: DataFrame containing weather data.
    :return: DataFrame representing the weather record table.
    :raises ValueError: If 'id' field is not present in the DataFrame.
    :raises Exception: For any other issues encountered during processing.
    """

    try:
        if "id" not in df.columns:
            logging.error("The 'id' field is missing from the DataFrame.")
            raise ValueError("The 'id' field is missing from the DataFrame.")

        # df['city_id'] = df['id']

        weather_record_df = df.copy()

        weather_record_df["weather_condition_id"] = weather_record_df["weather"].apply(
            lambda x: x[0]["id"]
        )
        weather_record_df["temp_id"] = weather_record_df.apply(
            lambda x: f"temp_{x['id']}_{x['dt']}", axis=1
        )
        weather_record_df["pressure_id"] = weather_record_df.apply(
            lambda x: f"p_{x['id']}_{x['dt']}", axis=1
        )
        weather_record_df["wind_id"] = weather_record_df.apply(
            lambda x: f"wind_{x['id']}_{x['dt']}", axis=1
        )
        weather_record_df["sun_id"] = weather_record_df.apply(
            lambda x: f"{x['sys']['sunrise']}_{x['sys']['sunset']}", axis=1
        )
        weather_record_df["humidity"] = weather_record_df["main"].apply(
            lambda x: x["humidity"]
        )
        weather_record_df["visibility"] = weather_record_df["visibility"]
        weather_record_df["cloud"] = weather_record_df["clouds"].apply(
            lambda x: x["all"]
        )

        weather_record_df["city_id"] = weather_record_df["id"]

        weather_record_df["id"] = weather_record_df.apply(
            lambda x: f"{x['city_id']}_{x['dt']}", axis=1
        )

        weather_record_df = (
            weather_record_df[
                [
                    "id",
                    "city_id",
                    "weather_condition_id",
                    "temp_id",
                    "pressure_id",
                    "wind_id",
                    "sun_id",
                    "humidity",
                    "visibility",
                    "cloud",
                    "dt",
                ]
            ]
            .drop_duplicates(subset=["id"])
            .reset_index(drop=True)
        )

        logging.info(
            f"Weather record data extracted successfully with {len(weather_record_df)} records."
        )

        return weather_record_df
    except Exception as e:
        logging.error(
            f"An error occurred while extracting weather record data: {e}",
            exc_info=True,
        )
        raise


def normalize_weather_data(df: pd.DataFrame) -> dict:
    """
    Normalizes the weather data into multiple dimension tables and fact tables.

    :param df: DataFrame containing raw weather data.
    :return: Dictionary containing the normalized DataFrames.
    """
    try:
        timezone_df = get_timezone_df(df)
        city_df = get_city_df(df)
        city_tz_df = get_city_tz(city_df, timezone_df)
        weather_condition_df = get_weather_condition_df(df)
        temperature_df = get_temperature_df(df)
        pressure_df = get_pressure_df(df)
        wind_df = get_wind_df(df)
        sun_df = get_sun_df(df)
        weather_record_df = get_weather_record(df)

        return {
            "cities": city_df,
            "timezone": timezone_df,
            "city_tz": city_tz_df,
            "weather_condition": weather_condition_df,
            "temperature": temperature_df,
            "pressure": pressure_df,
            "wind": wind_df,
            "sun": sun_df,
            "weather_record": weather_record_df,
        }

    except Exception as e:
        logging.error(
            f"An error occurred while normalizing weather data: {e}", exc_info=True
        )
        raise
