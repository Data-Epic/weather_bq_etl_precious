import pandas as pd
from unittest.mock import patch, MagicMock

# import os
from dags.scripts.fetch_data import (
    load_cities_from_csv,
    fetch_weather_data,
    fetch_weather_for_cities,
)

# API_KEY = os.getenv("API_KEY")


# Test load_cities_from_csv
def test_load_cities_from_csv(tmp_path):
    csv_file = tmp_path / "cities.csv"
    data = {"city_id": range(1050), "city_name": [f"City{i}" for i in range(1050)]}
    df = pd.DataFrame(data)
    df.to_csv(csv_file, index=False)

    result_df = load_cities_from_csv(csv_file)
    assert len(result_df) == 1000
    assert list(result_df.columns) == ["city_id", "city_name"]


# Test fetch_weather_data
@patch("dags.scripts.fetch_data.requests.get")
def test_fetch_weather_data(mock_get):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"list": [{"id": 1, "weather": "sunny"}]}
    mock_get.return_value = mock_response

    city_group = "1,2,3"
    api_key = "fake_api_key"
    result = fetch_weather_data(city_group, api_key)
    assert result == {"list": [{"id": 1, "weather": "sunny"}]}

    mock_get.assert_called_once_with(
        f"https://api.openweathermap.org/data/2.5/group?id={city_group}&units=metric&appid={api_key}"
    )


# Test fetch_weather_for_cities
@patch("dags.scripts.fetch_data.fetch_weather_data")
@patch("dags.scripts.fetch_data.load_cities_from_csv")
def test_fetch_weather_for_cities(mock_load_cities, mock_fetch_weather):
    mock_load_cities.return_value = pd.DataFrame(
        {"city_id": range(40), "city_name": [f"City{i}" for i in range(40)]}
    )
    mock_fetch_weather.return_value = {
        "list": [{"id": i, "weather": "sunny"} for i in range(20)]
    }

    csv_file = "fake_csv_file.csv"
    api_key = "fake_api_key"
    result_df = fetch_weather_for_cities(csv_file, api_key)

    assert len(result_df) == 40
    assert list(result_df.columns) == ["id", "weather"]

    mock_load_cities.assert_called_once_with(csv_file)
    assert mock_fetch_weather.call_count == 2
