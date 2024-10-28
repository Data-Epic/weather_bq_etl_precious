import pytest
import pandas as pd
from unittest import mock
from airflow.models import DagBag
from airflow import DAG
from dags.weather_dag import fetch_data_task, normalize_data_task, load_data_task
from dags.scripts.bq_table_config import schemas_dict, table_ids_dict, primary_keys
from airflow.exceptions import AirflowFailException


@pytest.fixture
def dag():
    """Load the DAG from your DAG file."""
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    return dagbag.dags["weather_etl_dag"]


def test_dag_import(dag):
    """Test if the DAG is correctly imported."""
    assert dag is not None
    assert isinstance(dag, DAG)
    assert dag.dag_id == "weather_etl_dag"


def test_dag_tasks(dag):
    """Test if the DAG contains the expected tasks."""
    assert len(dag.tasks) == 3
    task_ids = [task.task_id for task in dag.tasks]
    assert "fetch_data" in task_ids
    assert "normalize_data" in task_ids
    assert "load_data" in task_ids


def test_task_dependencies(dag):
    """Test if the task dependencies are correctly set up."""
    fetch_data_task = dag.get_task("fetch_data")
    normalize_data_task = dag.get_task("normalize_data")
    load_data_task = dag.get_task("load_data")

    assert fetch_data_task.downstream_task_ids == {"normalize_data"}
    assert normalize_data_task.downstream_task_ids == {"load_data"}
    assert load_data_task.downstream_task_ids == set()


def test_fetch_data_task():
    with mock.patch(
        "dags.weather_dag.fetch_weather_for_cities"
    ) as mock_fetch, mock.patch(
        "dags.weather_dag.os.getenv", return_value="fake_api_key"
    ), mock.patch("dags.weather_dag.logging") as mock_logging:
        mock_fetch.return_value = pd.DataFrame({"city": ["City1"], "temperature": [20]})
        ti = mock.MagicMock()
        fetch_data_task(ti=ti)

        ti.xcom_push.assert_called_once_with(
            key="raw_data", value=mock_fetch.return_value
        )
        mock_logging.info.assert_called_with("Weather data fetched successfully.")


def test_fetch_data_task_no_data():
    with mock.patch(
        "dags.weather_dag.fetch_weather_for_cities"
    ) as mock_fetch, mock.patch(
        "dags.weather_dag.os.getenv", return_value="fake_api_key"
    ), mock.patch("dags.weather_dag.logging") as mock_logging:
        mock_fetch.return_value = pd.DataFrame()
        ti = mock.MagicMock()

        with pytest.raises(AirflowFailException):
            fetch_data_task(ti=ti)

        mock_logging.error.assert_called()


def test_normalize_data_task():
    with mock.patch(
        "dags.weather_dag.normalize_weather_data"
    ) as mock_normalize, mock.patch("dags.weather_dag.logging") as mock_logging:
        raw_data = pd.DataFrame({"city": ["City1"], "temperature": [20]})
        normalized_data = pd.DataFrame({"city": ["City1"], "temp_celsius": [20]})
        mock_normalize.return_value = normalized_data

        ti = mock.MagicMock()
        ti.xcom_pull.return_value = raw_data

        normalize_data_task(ti=ti)

        ti.xcom_push.assert_called_once_with(
            key="normalized_data", value=normalized_data
        )
        mock_logging.info.assert_called_with("Weather data normalized successfully.")


def test_normalize_data_task_no_data():
    with mock.patch("dags.weather_dag.logging") as mock_logging:
        ti = mock.MagicMock()
        ti.xcom_pull.return_value = pd.DataFrame()

        with pytest.raises(AirflowFailException):
            normalize_data_task(ti=ti)

        mock_logging.error.assert_called()


def test_load_data_task():
    with mock.patch("dags.weather_dag.load_all_data") as mock_load, mock.patch(
        "dags.weather_dag.logging"
    ) as mock_logging:
        normalized_data = pd.DataFrame({"city": ["City1"], "temp_celsius": [20]})

        ti = mock.MagicMock()
        ti.xcom_pull.return_value = normalized_data

        load_data_task(ti=ti)

        mock_load.assert_called_once_with(
            normalized_data, table_ids_dict, schemas_dict, primary_keys
        )
        mock_logging.info.assert_called_with(
            "All normalized data loaded to BigQuery successfully."
        )


def test_load_data_task_no_data():
    with mock.patch("dags.weather_dag.logging") as mock_logging:
        ti = mock.MagicMock()
        ti.xcom_pull.return_value = None

        with pytest.raises(AirflowFailException):
            load_data_task(ti=ti)

        mock_logging.error.assert_called()
