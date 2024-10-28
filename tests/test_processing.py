import pytest
import pandas as pd

from dags.scripts.processing import (
    get_timezone_df,
    get_city_df,
    get_city_tz,
    get_weather_condition_df,
    get_temperature_df,
    get_pressure_df,
    get_wind_df,
    get_sun_df,
    get_weather_record,
    normalize_weather_data,
)


@pytest.fixture
def sample_data():
    return pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["CityA", "CityB"],
            "coord": [{"lat": 10, "lon": 20}, {"lat": 30, "lon": 40}],
            "sys": [
                {
                    "country": "CountryA",
                    "timezone": 3600,
                    "sunrise": 1600000000,
                    "sunset": 1600040000,
                },
                {
                    "country": "CountryB",
                    "timezone": -3600,
                    "sunrise": 1600100000,
                    "sunset": 1600140000,
                },
            ],
            "weather": [
                [
                    {
                        "id": 800,
                        "main": "Clear",
                        "description": "clear sky",
                        "icon": "01d",
                    }
                ],
                [
                    {
                        "id": 801,
                        "main": "Clouds",
                        "description": "few clouds",
                        "icon": "02d",
                    }
                ],
            ],
            "main": [
                {
                    "temp": 300,
                    "feels_like": 305,
                    "temp_min": 295,
                    "temp_max": 310,
                    "pressure": 1013,
                    "humidity": 50,
                },
                {
                    "temp": 280,
                    "feels_like": 285,
                    "temp_min": 275,
                    "temp_max": 290,
                    "pressure": 1020,
                    "humidity": 60,
                },
            ],
            "wind": [{"speed": 5, "deg": 180}, {"speed": 10, "deg": 270}],
            "clouds": [{"all": 0}, {"all": 20}],
            "visibility": [10000, 8000],
            "dt": [1600000000, 1600100000],
        }
    )


def test_get_timezone_df(sample_data):
    result = get_timezone_df(sample_data)
    expected = pd.DataFrame(
        {"timezone": [3600, -3600], "timezone_utc": ["UTC+1", "UTC-1"]}
    )
    pd.testing.assert_frame_equal(result, expected)


def test_get_city_df(sample_data):
    result = get_city_df(sample_data)
    expected = pd.DataFrame(
        {
            "id": [1, 2],
            "city": ["CityA", "CityB"],
            "lon": [20, 40],
            "lat": [10, 30],
            "country": ["CountryA", "CountryB"],
            "timezone": [3600, -3600],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_get_city_tz(sample_data):
    city_df = get_city_df(sample_data)
    timezone_df = get_timezone_df(sample_data)
    result = get_city_tz(city_df, timezone_df)
    expected = pd.DataFrame({"city_id": [1, 2], "tz_id": ["UTC+1", "UTC-1"]})
    pd.testing.assert_frame_equal(result, expected)


def test_get_weather_condition_df(sample_data):
    result = get_weather_condition_df(sample_data)
    expected = pd.DataFrame(
        {
            "id": [800, 801],
            "main": ["Clear", "Clouds"],
            "description": ["clear sky", "few clouds"],
            "icon": ["01d", "02d"],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_get_temperature_df(sample_data):
    result = get_temperature_df(sample_data)
    expected = pd.DataFrame(
        {
            "id": ["temp_1_1600000000", "temp_2_1600100000"],
            "temp": [300, 280],
            "feels_like": [305, 285],
            "temp_min": [295, 275],
            "temp_max": [310, 290],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_get_pressure_df(sample_data):
    result = get_pressure_df(sample_data)
    expected = pd.DataFrame(
        {
            "id": ["p_1_1600000000", "p_2_1600100000"],
            "pressure": [1013, 1020],
            "sea_level": [None, None],
            "grnd_level": [None, None],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_get_wind_df(sample_data):
    result = get_wind_df(sample_data)
    expected = pd.DataFrame(
        {
            "id": ["wind_1_1600000000", "wind_2_1600100000"],
            "speed": [5, 10],
            "deg": [180, 270],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_get_sun_df(sample_data):
    result = get_sun_df(sample_data)
    expected = pd.DataFrame(
        {
            "id": ["1600000000_1600040000", "1600100000_1600140000"],
            "sunrise": [1600000000, 1600100000],
            "sunset": [1600040000, 1600140000],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_get_weather_record(sample_data):
    result = get_weather_record(sample_data)
    expected = pd.DataFrame(
        {
            "id": ["1_1600000000", "2_1600100000"],
            "city_id": [1, 2],
            "weather_condition_id": [800, 801],
            "temp_id": ["temp_1_1600000000", "temp_2_1600100000"],
            "pressure_id": ["p_1_1600000000", "p_2_1600100000"],
            "wind_id": ["wind_1_1600000000", "wind_2_1600100000"],
            "sun_id": ["1600000000_1600040000", "1600100000_1600140000"],
            "humidity": [50, 60],
            "visibility": [10000, 8000],
            "cloud": [0, 20],
            "dt": [1600000000, 1600100000],
        }
    )
    pd.testing.assert_frame_equal(result, expected)


def test_normalize_weather_data(sample_data):
    result = normalize_weather_data(sample_data)
    assert set(result.keys()) == {
        "cities",
        "timezone",
        "city_tz",
        "weather_condition",
        "temperature",
        "pressure",
        "wind",
        "sun",
        "weather_record",
    }

    # Drop the timezone column from the expected city_df before comparison
    expected_city_df = get_city_df(sample_data).drop(columns=["timezone"])

    pd.testing.assert_frame_equal(result["cities"], expected_city_df)
    pd.testing.assert_frame_equal(result["timezone"], get_timezone_df(sample_data))
    pd.testing.assert_frame_equal(
        result["city_tz"],
        get_city_tz(get_city_df(sample_data), get_timezone_df(sample_data)),
    )
    pd.testing.assert_frame_equal(
        result["weather_condition"], get_weather_condition_df(sample_data)
    )
    pd.testing.assert_frame_equal(
        result["temperature"], get_temperature_df(sample_data)
    )
    pd.testing.assert_frame_equal(result["pressure"], get_pressure_df(sample_data))
    pd.testing.assert_frame_equal(result["wind"], get_wind_df(sample_data))
    pd.testing.assert_frame_equal(result["sun"], get_sun_df(sample_data))
    pd.testing.assert_frame_equal(
        result["weather_record"], get_weather_record(sample_data)
    )
