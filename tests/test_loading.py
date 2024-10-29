import pytest
from unittest import mock
import pandas as pd
from dags.scripts.loading import (
    get_bigquery_client,
    load_df_to_bq,
    deduplicate,
    load_all_data,
)
import logging


# Mock the BigQuery client
@pytest.fixture
def mock_bigquery_client():
    with mock.patch("dags.scripts.loading.bigquery.Client") as mock_client:
        yield mock_client


def test_get_bigquery_client(mock_bigquery_client):
    client = get_bigquery_client()
    assert client == mock_bigquery_client.return_value


def test_load_df_to_bq_empty_data(mock_bigquery_client, caplog):
    data = pd.DataFrame()
    table_id = "test_table"
    schema = []

    with caplog.at_level(logging.INFO):
        load_df_to_bq(data, table_id, schema)

    assert "No new data to load to test_table" in caplog.text


def test_load_df_to_bq_success(mock_bigquery_client, caplog):
    data = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    table_id = "test_table"
    schema = []

    mock_load_job = mock.Mock()
    mock_load_job.result.return_value = None
    mock_bigquery_client.return_value.load_table_from_dataframe.return_value = (
        mock_load_job
    )

    with caplog.at_level(logging.INFO):
        load_df_to_bq(data, table_id, schema)

    assert "2 records loaded to test_table successfully." in caplog.text


def test_load_df_to_bq_error(mock_bigquery_client, caplog):
    data = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    table_id = "test_table"
    schema = []

    mock_bigquery_client.return_value.load_table_from_dataframe.side_effect = Exception(
        "Test error"
    )

    with caplog.at_level(logging.ERROR):
        with pytest.raises(Exception, match="Test error"):
            load_df_to_bq(data, table_id, schema)

    assert (
        "An error occurred while loading data to test_table: Test error" in caplog.text
    )


def test_deduplicate(mock_bigquery_client):
    data = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    table_id = "test_table"
    key = "id"

    existing_data = pd.DataFrame({"id": [2]})
    mock_bigquery_client.return_value.query.return_value.to_dataframe.return_value = (
        existing_data
    )

    result = deduplicate(data, table_id, key).reset_index(drop=True)
    expected_result = pd.DataFrame({"id": [1, 3], "value": ["a", "c"]})

    pd.testing.assert_frame_equal(result, expected_result)


def test_load_all_data(mock_bigquery_client, caplog):
    normalized_data = {
        "table1": pd.DataFrame({"id": [1, 2], "value": ["a", "b"]}),
        "table2": pd.DataFrame({"id": [3, 4], "value": ["c", "d"]}),
    }
    table_ids = {"table1": "project.dataset.table1", "table2": "project.dataset.table2"}
    schemas = {"table1": [], "table2": []}
    keys = {"table1": "id", "table2": "id"}

    existing_data = pd.DataFrame({"id": [2, 4]})
    mock_bigquery_client.return_value.query.return_value.to_dataframe.return_value = (
        existing_data
    )

    mock_load_job = mock.Mock()
    mock_load_job.result.return_value = None
    mock_bigquery_client.return_value.load_table_from_dataframe.return_value = (
        mock_load_job
    )

    with caplog.at_level(logging.INFO):
        load_all_data(normalized_data, table_ids, schemas, keys)

    assert (
        "Data for table1 loaded to project.dataset.table1 successfully." in caplog.text
    )
    assert (
        "Data for table2 loaded to project.dataset.table2 successfully." in caplog.text
    )
    assert "All data loaded to tables successfully." in caplog.text
