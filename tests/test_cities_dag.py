import pytest
from unittest import mock
from airflow.models import DagBag, Variable
from airflow import DAG
import pandas as pd
import json


from dags.cities_dag import (
    get_countries,
    extract_city_data_from_json,
    filter_cities_by_country,
    save_city_data_to_csv,
    NoDataFoundError,
)


@pytest.fixture
def dag():
    """Load the DAG from your DAG file."""
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    return dagbag.dags["update_city_csv"]


def test_dag(dag):
    """Test if the DAG is correctly imported and contains the expected task."""
    assert dag is not None
    assert isinstance(dag, DAG)
    assert dag.dag_id == "update_city_csv"
    assert len(dag.tasks) == 1
    task_ids = [task.task_id for task in dag.tasks]
    assert "update_city_csv" in task_ids


def test_get_countries():
    with mock.patch.object(Variable, "get", return_value="NG, GH, BR"):
        countries = get_countries()
        assert countries == ["NG", "GH", "BR"]

    with mock.patch.object(Variable, "get", side_effect=KeyError):
        with pytest.raises(KeyError):
            get_countries()


def test_extract_city_data_from_json(tmp_path):
    city_data = [
        {"id": 1, "name": "Lagos", "country": "NG"},
        {"id": 2, "name": "Accra", "country": "GH"},
    ]
    json_file = tmp_path / "city.list.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(city_data, f)

    df = extract_city_data_from_json(json_file)
    assert len(df) == 2
    assert df.iloc[0]["city_name"] == "Lagos"
    assert df.iloc[1]["city_name"] == "Accra"


def test_filter_cities_by_country():
    data = {
        "city_id": [1, 2, 3],
        "city_name": ["Lagos", "Accra", "Brasilia"],
        "country": ["NG", "GH", "BR"],
    }
    df = pd.DataFrame(data)

    filtered_df = filter_cities_by_country(df, "NG")
    assert len(filtered_df) == 1
    assert filtered_df.iloc[0]["city_name"] == "Lagos"

    with pytest.raises(NoDataFoundError):
        filter_cities_by_country(df, "US")


def test_save_city_data_to_csv(tmp_path):
    data = {"city_id": [1, 2], "city_name": ["Lagos", "Accra"], "country": ["NG", "GH"]}
    df = pd.DataFrame(data)
    csv_file = tmp_path / "cities.csv"

    save_city_data_to_csv(df, csv_file)
    saved_df = pd.read_csv(csv_file)

    assert len(saved_df) == 2
    assert saved_df.iloc[0]["city_name"] == "Lagos"
    assert saved_df.iloc[1]["city_name"] == "Accra"
